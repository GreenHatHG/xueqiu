from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Iterator, Optional
from urllib.parse import urlencode

from .constants import (
    BASE_URL,
    DEFAULT_BACKOFF_INITIAL_SEC,
    DEFAULT_BACKOFF_MAX_SEC,
    TALKS_PAGE_SIZE,
    USER_COMMENTS_PAGE_SIZE,
)
from .rate_limit import RateLimiter


class BlockedError(RuntimeError):
    pass


class ChallengeRequiredError(BlockedError):
    def __init__(
        self, message: str, *, url: str, final_url: str, status: int, text_head: str
    ) -> None:
        super().__init__(message)
        self.url = url
        self.final_url = final_url
        self.status = int(status)
        self.text_head = str(text_head)


def _looks_like_html(text: str) -> bool:
    t = text.lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html") or "<head" in t[:200]


def _looks_like_waf_challenge(text: str) -> bool:
    """
    Heuristically detect WAF challenge pages that require manual verification (e.g. slider).

    Observed markers include Aliyun WAF meta tags/ids and Chinese verification prompts.
    """

    head = text[:4000]
    lower = head.lower()

    markers = [
        "aliyun_waf",
        "_waf_",
        "renderdata",
        "alichlgref",
        "md5__1038",
        "为保证您的正常访问",
        "请进行如下验证",
        "验证失败",
        "请刷新重试",
        "滑动",
        "验证码",
        "日志id",
    ]
    return any(m in lower or m in head for m in markers)


@dataclass
class ApiConfig:
    min_delay_sec: float
    jitter_sec: float
    max_retries: int
    max_consecutive_blocks: int


class XueqiuApi:
    def __init__(
        self,
        nav_page: Any,
        cfg: ApiConfig,
        *,
        prefer_page_fetch: bool = False,
    ) -> None:
        self._nav_page = nav_page
        self._cfg = cfg
        self._limiter = RateLimiter(cfg.min_delay_sec, cfg.jitter_sec)
        self._consecutive_blocks = 0
        self._prefer_page_fetch = bool(prefer_page_fetch)

    @staticmethod
    def _looks_like_challenge_url(url: str) -> bool:
        u = (url or "").lower()
        return ("alichlgref=" in u) or ("md5__1038=" in u) or ("_waf_" in u)

    def _fetch_text_once(
        self, url: str, referrer: Optional[str] = None
    ) -> tuple[int, str, str]:
        """
        Fetch an endpoint by real browser navigation inside the current session.
        """
        if self._prefer_page_fetch:
            return self._fetch_text_via_page_fetch_once(url, referrer=referrer)
        return self._fetch_text_via_nav_once(url)

    def _ensure_page_fetch_context(self) -> bool:
        if self._nav_page is None:
            return False
        try:
            current_url = str(self._nav_page.url or "")
        except Exception:
            current_url = ""
        if current_url.startswith(BASE_URL):
            return True
        try:
            self._nav_page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            return False
        try:
            return str(self._nav_page.url or "").startswith(BASE_URL)
        except Exception:
            return False

    def _fetch_text_via_page_fetch_once(
        self, url: str, referrer: Optional[str] = None
    ) -> tuple[int, str, str]:
        """
        Fetch by executing window.fetch() inside a logged-in Xueqiu page.

        This is only enabled for CDP mode, where we already validated that the
        real Chrome session can directly fetch these JSON endpoints.
        """

        self._limiter.sleep_before_next()
        if self._nav_page is None:
            return 0, "", url
        if not self._ensure_page_fetch_context():
            return 0, "", url

        try:
            obj = self._nav_page.evaluate(
                """
                async ({ url, referrer, timeoutMs }) => {
                  const controller = new AbortController();
                  const timer = setTimeout(() => controller.abort(), timeoutMs);
                  try {
                    const resp = await fetch(url, {
                      method: 'GET',
                      credentials: 'include',
                      redirect: 'follow',
                      signal: controller.signal,
                      referrer: referrer || undefined,
                      referrerPolicy: 'strict-origin-when-cross-origin',
                      headers: {
                        'accept': 'application/json, text/plain, */*',
                        'x-requested-with': 'XMLHttpRequest',
                      },
                    });
                    const text = await resp.text();
                    return {
                      status: Number(resp.status || 0),
                      text: String(text || ''),
                      final_url: String(resp.url || url || ''),
                    };
                  } catch (error) {
                    return {
                      status: 0,
                      text: '',
                      final_url: String(url || ''),
                      error: String(error || ''),
                    };
                  } finally {
                    clearTimeout(timer);
                  }
                }
                """,
                {"url": str(url), "referrer": str(referrer or ""), "timeoutMs": 30000},
            )
        except Exception:
            return 0, "", url

        if not isinstance(obj, dict):
            return 0, "", url
        try:
            status = int(obj.get("status") or 0)
        except Exception:
            status = 0
        text = str(obj.get("text") or "")
        final_url = str(obj.get("final_url") or url)
        return status, text, final_url

    def _fetch_text_via_nav_once(
        self, url: str, *, wait_until: str = "domcontentloaded", timeout_ms: int = 30000
    ) -> tuple[int, str, str]:
        """
        Fetch by real browser navigation (address-bar style) and read response body.

        This is often closer to manual browsing behavior under WAF than a plain HTTP client,
        because the challenge/verification flow happens inside the browser session.
        """

        self._limiter.sleep_before_next()
        if self._nav_page is None:
            return 0, "", url
        try:
            resp = self._nav_page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        except Exception:
            return 0, "", url
        if resp is None:
            return 0, "", url
        try:
            status = int(resp.status)
        except Exception:
            status = 0
        try:
            final_url = str(resp.url or url)
        except Exception:
            final_url = url
        try:
            text = resp.text() or ""
        except Exception:
            text = ""
        return status, text, final_url

    @staticmethod
    def _normalize_detail_url(url: Optional[str]) -> str:
        s = str(url or "").strip()
        if not s:
            return ""
        if s.startswith("/"):
            return f"{BASE_URL}{s}"
        if s.startswith("http://") or s.startswith("https://"):
            return s
        return ""

    def _extract_status_display_line_from_page(self, status_id: str) -> Optional[str]:
        if self._nav_page is None:
            return None

        line = ""
        try:
            line = self._nav_page.evaluate(
                """
                (statusId) => {
                  const sid = String(statusId || '').trim();
                  if (!sid) {
                    return '';
                  }

                  const toPlainText = (raw) => {
                    if (!raw) {
                      return '';
                    }
                    const box = document.createElement('div');
                    box.innerHTML = String(raw);
                    box.querySelectorAll('img').forEach((node) => node.remove());
                    return String(box.innerText || box.textContent || '')
                      .split('\\n')
                      .map((part) => part.trim())
                      .filter(Boolean)
                      .join('\\n')
                      .trim();
                  };

                  const authorOf = (obj) => {
                    const user = obj && typeof obj.user === 'object' ? obj.user : null;
                    if (user) {
                      for (const key of ['screen_name', 'screenName', 'name', 'nickname']) {
                        const value = user[key];
                        if (value !== undefined && value !== null && String(value).trim()) {
                          return String(value).trim();
                        }
                      }
                      const userId = user.id ?? user.user_id ?? user.uid;
                      if (userId !== undefined && userId !== null && String(userId).trim()) {
                        return String(userId).trim();
                      }
                    }
                    const fallbackId = obj ? (obj.user_id ?? obj.uid) : '';
                    return fallbackId !== undefined && fallbackId !== null ? String(fallbackId).trim() : '';
                  };

                  const root = window.SNOWMAN_STATUS && typeof window.SNOWMAN_STATUS === 'object'
                    ? window.SNOWMAN_STATUS
                    : null;
                  const candidates = [];
                  if (root) {
                    candidates.push(root);
                  }
                  if (root && root.retweeted_status && typeof root.retweeted_status === 'object') {
                    candidates.push(root.retweeted_status);
                  }

                  let target = candidates.find((obj) => String(obj && (obj.id ?? obj.status_id ?? '')).trim() === sid) || null;
                  if (
                    !target &&
                    root &&
                    String(root.retweet_status_id ?? root.retweeted_status_id ?? '').trim() === sid &&
                    root.retweeted_status &&
                    typeof root.retweeted_status === 'object'
                  ) {
                    target = root.retweeted_status;
                  }
                  if (!target) {
                    return '';
                  }

                  const body = toPlainText(target.text || target.description || '');
                  if (!body) {
                    return '';
                  }
                  const author = authorOf(target);
                  return author ? `${author}：${body}` : body;
                }
                """,
                status_id,
            )
        except Exception:
            line = ""

        line_str = str(line or "").strip()
        if line_str:
            return line_str

        try:
            line = self._nav_page.evaluate(
                """
                (statusId) => {
                  const articles = Array.from(document.querySelectorAll('article'));
                  if (!articles.length) {
                    return '';
                  }

                  const statusHrefNeedle = `/${statusId}`;
                  let article = articles.find((node) =>
                    Array.from(node.querySelectorAll('a[href]')).some((link) =>
                      String(link.getAttribute('href') || '').includes(statusHrefNeedle)
                    )
                  );
                  if (!article) {
                    article = articles.find((node) => (node.innerText || '').trim().length > 80) || articles[0];
                  }
                  if (!article) {
                    return '';
                  }

                  const lines = String(article.innerText || '')
                    .split('\\n')
                    .map((line) => line.trim())
                    .filter(Boolean);
                  if (!lines.length) {
                    return '';
                  }

                  const isMetaLine = (line) =>
                    /^(今天|昨天|前天|\\d{2}-\\d{2}|\\d+分钟前|\\d+小时前|\\d+天前)/.test(line) ||
                    line.includes('来自');
                  const isFooterLine = (line) =>
                    /^(转发|讨论|赞|收藏|投诉|查看对话|分享|编辑|删除|关注|回复)\\b/.test(line) ||
                    /^\\d+$/.test(line);

                  const author = lines[0];
                  let start = 1;
                  if (lines.length > 1 && isMetaLine(lines[1])) {
                    start = 2;
                  }

                  const body = [];
                  for (let i = start; i < lines.length; i += 1) {
                    const line = lines[i];
                    if (isFooterLine(line)) {
                      break;
                    }
                    body.push(line);
                  }

                  const text = body.join('\\n').trim();
                  if (!text) {
                    return '';
                  }
                  return author ? `${author}：${text}` : text;
                }
                """,
                status_id,
            )
        except Exception:
            line = ""

        line_str = str(line or "").strip()
        return line_str or None

    def fetch_status_display_line(
        self,
        status_id: str,
        *,
        source_status_url: Optional[str] = None,
        status_url: Optional[str] = None,
        status_user_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Open a status detail page and extract a readable "author: text" line.

        This is used as a best-effort fallback when timeline/comment payloads only
        contain a truncated preview of the quoted/original status.
        """

        sid = str(status_id or "").strip()
        if not sid or self._nav_page is None:
            return None

        candidate_urls: list[str] = []
        for candidate in (
            self._normalize_detail_url(source_status_url),
            self._normalize_detail_url(status_url),
            f"{BASE_URL}/{str(status_user_id).strip()}/{sid}"
            if str(status_user_id or "").strip()
            else "",
            f"{BASE_URL}/status/{sid}",
        ):
            if candidate and candidate not in candidate_urls:
                candidate_urls.append(candidate)

        for url in candidate_urls:
            self.goto(url)
            try:
                self._nav_page.wait_for_timeout(1200)
            except Exception:
                pass
            line = self._extract_status_display_line_from_page(sid)
            if line:
                return line
        return None

    def fetch_json(self, url: str, referrer: Optional[str] = None) -> Any:
        backoff = DEFAULT_BACKOFF_INITIAL_SEC
        last_exc: Optional[Exception] = None

        for attempt in range(self._cfg.max_retries + 1):
            try:
                status, text, final_url = self._fetch_text_once(url, referrer=referrer)

                looks_html = _looks_like_html(text)
                if status in (401, 403, 429):
                    raise BlockedError(f"blocked or not logged in (status={status})")
                if looks_html:
                    is_waf = bool(
                        self._looks_like_challenge_url(final_url)
                        or _looks_like_waf_challenge(text)
                    )
                    if is_waf:
                        raise ChallengeRequiredError(
                            f"waf challenge required (status={status})",
                            url=url,
                            final_url=final_url,
                            status=int(status),
                            text_head=text[:200],
                        )
                    raise BlockedError(f"blocked or not logged in (status={status})")

                try:
                    obj = json.loads(text)
                except Exception as e:
                    # When JSON parsing fails, decide whether it looks like a WAF challenge.
                    if self._looks_like_challenge_url(
                        final_url
                    ) or _looks_like_waf_challenge(text):
                        raise ChallengeRequiredError(
                            f"waf challenge required (status={status})",
                            url=url,
                            final_url=final_url,
                            status=int(status),
                            text_head=text[:200],
                        ) from e
                    if _looks_like_html(text):
                        raise BlockedError(
                            f"blocked or not logged in (status={status})"
                        ) from e
                    raise

                self._consecutive_blocks = 0
                return obj
            except ChallengeRequiredError:
                # Do not auto-retry/refresh challenge pages in a tight loop.
                raise
            except BlockedError as e:
                self._consecutive_blocks += 1
                last_exc = e
            except Exception as e:
                last_exc = e

            if attempt < self._cfg.max_retries:
                time.sleep(min(backoff, DEFAULT_BACKOFF_MAX_SEC))
                backoff *= 2

            if self._consecutive_blocks >= self._cfg.max_consecutive_blocks:
                raise BlockedError(
                    f"too many blocked responses ({self._consecutive_blocks}), stop to protect account"
                ) from last_exc

        assert last_exc is not None
        raise last_exc

    def goto(
        self, url: str, wait_until: str = "domcontentloaded", timeout_ms: int = 30000
    ) -> None:
        """
        Navigate like a normal browser before calling JSON endpoints.

        For better consistency with "manual browsing", we allow the crawler to land on the
        relevant page (status/profile) first, then open JSON endpoint URLs.
        """
        self._limiter.sleep_before_next()
        # Use navigation only for human-visible/UI flows (e.g. preparing context pages).
        if self._nav_page is None:
            return
        try:
            self._nav_page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        except Exception:
            pass

    @staticmethod
    def build_url(path: str, query: dict[str, Any]) -> str:
        qs = urlencode({k: v for k, v in query.items() if v is not None})
        if path.startswith("http"):
            return f"{path}?{qs}" if qs else path
        return f"{BASE_URL}{path}?{qs}" if qs else f"{BASE_URL}{path}"

    def probe_url_json(
        self, url: str, referrer: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Probe a single URL once (no retries) to see if it returns parseable JSON.
        """

        status, text, final_url = self._fetch_text_once(url, referrer=referrer)
        looks_html = _looks_like_html(text)
        if status in (401, 403, 429):
            return {
                "ok": False,
                "status": status,
                "looks_like_html": bool(looks_html),
                "is_waf_challenge": False,
                "url": url,
                "final_url": final_url,
                "text_head": text[:200],
            }
        if looks_html:
            is_waf = bool(
                self._looks_like_challenge_url(final_url)
                or _looks_like_waf_challenge(text)
            )
            return {
                "ok": False,
                "status": status,
                "looks_like_html": True,
                "is_waf_challenge": bool(is_waf),
                "url": url,
                "final_url": final_url,
                "text_head": text[:200],
            }
        try:
            obj = json.loads(text)
        except Exception:
            # If parsing fails but redirect/markers indicate WAF, surface it clearly.
            is_waf2 = bool(
                self._looks_like_challenge_url(final_url)
                or _looks_like_waf_challenge(text)
            )
            return {
                "ok": False,
                "status": status,
                "looks_like_html": bool(looks_html),
                "is_waf_challenge": bool(is_waf2),
                "url": url,
                "final_url": final_url,
                "text_head": text[:200],
                "error": "json_parse_failed",
            }
        return {
            "ok": True,
            "status": status,
            "url": url,
            "final_url": final_url,
            "type": type(obj).__name__,
        }

    @staticmethod
    def extract_timeline_statuses(obj: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[Any] = []
        for key in ("statuses", "list", "items", "data"):
            if key not in obj:
                continue
            candidates.append(obj.get(key))

        # Common shapes:
        # - {"statuses":[...]}
        # - {"list":[...]}
        # - {"data":{"statuses":[...]}} / {"data":{"list":[...]}}
        for value in candidates:
            if isinstance(value, list):
                return [v for v in value if isinstance(v, dict)]
            if isinstance(value, dict):
                for k in ("statuses", "list", "items"):
                    inner = value.get(k)
                    if isinstance(inner, list):
                        return [v for v in inner if isinstance(v, dict)]
        return []

    def iter_user_comments_pages(
        self, user_id: str, start_max_id: int, max_pages: int
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        max_id = start_max_id
        seen_cursors: set[int] = set()
        for _page_idx in range(1, max_pages + 1):
            if max_id in seen_cursors:
                break
            seen_cursors.add(int(max_id))
            url = self.build_url(
                "/statuses/user/comments.json",
                {"user_id": user_id, "size": USER_COMMENTS_PAGE_SIZE, "max_id": max_id},
            )
            obj = self.fetch_json(url)
            items = obj.get("items") or []
            if not items:
                break
            yield int(obj.get("next_max_id") or -1), list(items)
            next_max_id = obj.get("next_max_id")
            if next_max_id is None:
                break
            max_id = int(next_max_id)
            if max_id == -1:
                break

    def fetch_talks_all_pages(
        self,
        root_status_id: str,
        comment_id: str,
        max_pages: int,
    ) -> dict[str, Any]:
        ref = f"{BASE_URL}/status/{root_status_id}"
        first_url = self.build_url(
            "/statuses/talks.json",
            {
                "id": root_status_id,
                "comment_id": comment_id,
                "page": 1,
                "count": TALKS_PAGE_SIZE,
                "asc": "true",
            },
        )
        first = self.fetch_json(first_url, referrer=ref)
        max_page = int(first.get("maxPage") or 1)
        max_page = min(max_page, max_pages)

        pages: list[dict[str, Any]] = [first]
        for p in range(2, max_page + 1):
            url = self.build_url(
                "/statuses/talks.json",
                {
                    "id": root_status_id,
                    "comment_id": comment_id,
                    "page": p,
                    "count": TALKS_PAGE_SIZE,
                    "asc": "true",
                },
            )
            obj = self.fetch_json(url, referrer=ref)
            pages.append(obj)
            comments = obj.get("comments") or []
            if not comments and p >= max_page:
                break
        return {
            "root_status_id": root_status_id,
            "comment_id": comment_id,
            "max_page": int(first.get("maxPage") or max_page),
            "fetched_pages": len(pages),
            "truncated": bool(int(first.get("maxPage") or max_page) > max_page),
            "pages": pages,
        }

    def fetch_talks_incremental(
        self,
        root_status_id: str,
        comment_id: str,
        max_pages: int,
        existing: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Incrementally extend an existing talks snapshot.

        This supports the common workflow:
        - First run: fetch the first N pages (N is capped).
        - Later runs: increase the cap and continue fetching the remaining pages.
        """

        existing_pages: list[dict[str, Any]] = []
        pages_obj = existing.get("pages") if existing else None
        if isinstance(pages_obj, list):
            existing_pages = [
                page_obj for page_obj in pages_obj if isinstance(page_obj, dict)
            ]

        fetched_page_nums: set[int] = set()
        for page_obj in existing_pages:
            try:
                page_value = page_obj.get("page")
                if page_value is None:
                    continue
                fetched_page_nums.add(int(page_value))
            except Exception:
                continue

        # Fast path: if we cannot determine progress, fall back to full fetch.
        if existing_pages and not fetched_page_nums:
            return self.fetch_talks_all_pages(
                root_status_id=root_status_id,
                comment_id=comment_id,
                max_pages=max_pages,
            )

        start_page = (max(fetched_page_nums) + 1) if fetched_page_nums else 1

        ref = f"{BASE_URL}/status/{root_status_id}"
        first_url = self.build_url(
            "/statuses/talks.json",
            {
                "id": root_status_id,
                "comment_id": comment_id,
                "page": 1,
                "count": TALKS_PAGE_SIZE,
                "asc": "true",
            },
        )
        first = self.fetch_json(first_url, referrer=ref)
        max_page_reported = int(first.get("maxPage") or 1)
        max_page_target = min(max_page_reported, int(max_pages))

        pages_out: list[dict[str, Any]] = list(existing_pages)

        # Ensure page 1 exists when starting from scratch.
        if start_page == 1:
            pages_out = [first]
            start_page = 2

        for page_num in range(start_page, max_page_target + 1):
            if page_num in fetched_page_nums:
                continue
            url = self.build_url(
                "/statuses/talks.json",
                {
                    "id": root_status_id,
                    "comment_id": comment_id,
                    "page": page_num,
                    "count": TALKS_PAGE_SIZE,
                    "asc": "true",
                },
            )
            obj = self.fetch_json(url, referrer=ref)
            pages_out.append(obj)
            try:
                fetched_page_nums.add(int(obj.get("page")))
            except Exception:
                pass

        return {
            "root_status_id": root_status_id,
            "comment_id": comment_id,
            "max_page": int(max_page_reported),
            "fetched_pages": len(pages_out),
            "truncated": bool(int(max_page_reported) > max_page_target),
            "pages": pages_out,
        }


def normalize_root_status_url(status_obj: dict[str, Any]) -> Optional[str]:
    user_id = status_obj.get("user_id")
    status_id = status_obj.get("id")
    if user_id is None or status_id is None:
        return None
    if int(user_id) != -1:
        return f"{BASE_URL}/{user_id}/{status_id}"
    target = status_obj.get("target")
    if isinstance(target, str) and target.startswith("/"):
        return f"{BASE_URL}{target}"
    return None
