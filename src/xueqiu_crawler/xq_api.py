from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urlencode

from .constants import (
    BASE_URL,
    DEFAULT_BACKOFF_INITIAL_SEC,
    DEFAULT_BACKOFF_MAX_SEC,
    TALKS_PAGE_SIZE,
    USER_COMMENTS_PAGE_SIZE,
)
from .rate_limit import RateLimiter
from .text_sanitize import sanitize_xueqiu_text


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


DETAIL_PAGE_INITIAL_WAIT_MS = 1200
DETAIL_WAF_SETTLE_TIMEOUT_SEC = 8.0
DETAIL_WAF_SETTLE_POLL_SEC = 1.0

STATUS_SHOW_JSON_PATH = "/statuses/show.json"


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


def _extract_status_obj_from_show_payload(obj: Any) -> Optional[dict[str, Any]]:
    if not isinstance(obj, dict):
        return None
    # Common error shapes:
    # - {"error_code": "...", "error_description": "..."}
    if str(obj.get("error_code") or "").strip():
        return None
    if str(obj.get("error_description") or "").strip():
        return None

    if obj.get("id") is not None or obj.get("status_id") is not None:
        return obj
    for key in ("status", "data", "item", "result"):
        inner = obj.get(key)
        if isinstance(inner, dict) and (
            inner.get("id") is not None or inner.get("status_id") is not None
        ):
            return inner
    return None


def _status_display_line_from_status_obj(status_obj: dict[str, Any]) -> str:
    raw_text = status_obj.get("text") or status_obj.get("description")
    text = sanitize_xueqiu_text(raw_text) or ""
    text = str(text).strip()
    if not text:
        return ""

    author = ""
    user_obj = status_obj.get("user")
    if isinstance(user_obj, dict):
        for key in ("screen_name", "screenName", "name", "nickname"):
            val = user_obj.get(key)
            if val is None:
                continue
            s = str(val).strip()
            if s:
                author = s
                break
        if not author:
            uid = user_obj.get("id") or user_obj.get("user_id") or user_obj.get("uid")
            if uid not in (None, "", 0, "0"):
                author = str(uid).strip()
    if not author:
        uid = status_obj.get("user_id") or status_obj.get("uid")
        if uid not in (None, "", 0, "0"):
            author = str(uid).strip()

    return f"{author}：{text}" if author else text


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

    def _read_detail_page_state(
        self, status_id: str, *, fallback_url: str
    ) -> tuple[Optional[str], str, str, str]:
        line = self._extract_status_display_line_from_page(status_id)
        try:
            final_url = str(self._nav_page.url or fallback_url or "")
        except Exception:
            final_url = str(fallback_url or "")
        try:
            page_html = str(self._nav_page.content() or "")
        except Exception:
            page_html = ""
        try:
            page_title = str(self._nav_page.title() or "")
        except Exception:
            page_title = ""
        return line, final_url, page_html, page_title

    def _wait_for_detail_page_to_settle(
        self, status_id: str, *, fallback_url: str
    ) -> tuple[Optional[str], str, str, str]:
        line, final_url, page_html, page_title = self._read_detail_page_state(
            status_id, fallback_url=fallback_url
        )
        if line:
            return line, final_url, page_html, page_title

        deadline = time.monotonic() + DETAIL_WAF_SETTLE_TIMEOUT_SEC
        while self._looks_like_challenge_url(final_url) or _looks_like_waf_challenge(
            page_html
        ):
            if time.monotonic() >= deadline:
                break
            try:
                self._nav_page.wait_for_timeout(int(DETAIL_WAF_SETTLE_POLL_SEC * 1000))
            except Exception:
                break
            line, final_url, page_html, page_title = self._read_detail_page_state(
                status_id, fallback_url=fallback_url
            )
            if line:
                return line, final_url, page_html, page_title
        return line, final_url, page_html, page_title

    def _fetch_status_display_line_via_show_json(
        self,
        status_id: str,
        *,
        referrer: str,
        debug: Optional[dict[str, Any]] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        sid = str(status_id or "").strip()
        if not sid:
            return None, "状态ID为空，没法补抓"

        url = self.build_url(STATUS_SHOW_JSON_PATH, {"id": sid})
        if debug is not None:
            debug["show_json_url"] = str(url)

        try:
            obj = self._fetch_json_with_retry(
                url,
                referrer=str(referrer or "").strip() or None,
                request_label=f"status-show id={sid}",
            )
        except ChallengeRequiredError as exc:
            if debug is not None:
                debug["show_json_error"] = {
                    "issue_code": "waf_challenge",
                    "url": str(exc.url or ""),
                    "final_url": str(exc.final_url or ""),
                    "status": int(exc.status),
                    "text_head": str(exc.text_head or ""),
                }
            return None, "遇到风控验证页"
        except BlockedError as exc:
            if debug is not None:
                debug["show_json_error"] = {"issue_code": "blocked", "error": str(exc)}
            return None, f"接口被拦了：{exc}"
        except Exception as exc:
            if debug is not None:
                debug["show_json_error"] = {"issue_code": "error", "error": str(exc)}
            return None, f"接口请求失败：{exc}"

        status_obj = _extract_status_obj_from_show_payload(obj)
        if status_obj is None:
            if debug is not None:
                debug["show_json_error"] = {
                    "issue_code": "bad_payload",
                    "payload_type": type(obj).__name__,
                }
            return None, "接口回包没 status"

        line = _status_display_line_from_status_obj(status_obj)
        if not line:
            if debug is not None:
                debug["show_json_error"] = {"issue_code": "empty_text"}
            return None, "接口回包没正文"

        if debug is not None:
            debug["show_json_success"] = True
        return line, None

    def fetch_status_display_line(
        self,
        status_id: str,
        *,
        source_status_url: Optional[str] = None,
        status_url: Optional[str] = None,
        status_user_id: Optional[str] = None,
        debug: Optional[dict[str, Any]] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Open a status detail page and extract a readable "author: text" line.

        This is used as a best-effort fallback when timeline/comment payloads only
        contain a truncated preview of the quoted/original status.
        """

        sid = str(status_id or "").strip()
        if not sid or self._nav_page is None:
            return None, "状态ID为空，没法补抓"

        candidate_urls: list[str] = []
        for candidate in (
            self._normalize_detail_url(source_status_url),
            self._normalize_detail_url(status_url),
            f"{BASE_URL}/{str(status_user_id).strip()}/{sid}"
            if str(status_user_id or "").strip()
            else "",
        ):
            if candidate and candidate not in candidate_urls:
                candidate_urls.append(candidate)

        if debug is not None:
            debug.clear()
            debug["candidate_urls"] = list(candidate_urls)
            debug["attempts"] = []

        last_reason = "页面没拿到正文"
        for url in candidate_urls:
            backoff = DEFAULT_BACKOFF_INITIAL_SEC
            for attempt in range(self._cfg.max_retries + 1):
                self._limiter.sleep_before_next()
                try:
                    resp = self._nav_page.goto(
                        url, wait_until="domcontentloaded", timeout=30000
                    )
                except Exception:
                    resp = None
                try:
                    self._nav_page.wait_for_timeout(DETAIL_PAGE_INITIAL_WAIT_MS)
                except Exception:
                    pass

                try:
                    status = int(resp.status) if resp is not None else 0
                except Exception:
                    status = 0
                line, final_url, page_html, page_title = (
                    self._wait_for_detail_page_to_settle(
                        sid,
                        fallback_url=str((resp.url if resp is not None else "") or url),
                    )
                )
                if line:
                    if debug is not None:
                        debug["success_url"] = str(final_url or url)
                    return line, None

                issue_code = "empty_page"
                issue_reason = "页面打开了，但没抠到正文"
                if (
                    status == 404
                    or "404_雪球" in page_title
                    or "没有找到这条讨论" in page_html
                ):
                    issue_code = "not_found"
                    issue_reason = (
                        "帖子页 404，可能原帖没了"
                        f" url={str(url).strip()} final_url={str(final_url or '').strip()}"
                    )
                elif self._looks_like_challenge_url(
                    final_url
                ) or _looks_like_waf_challenge(page_html):
                    issue_code = "waf_challenge"
                    issue_reason = "遇到风控验证页"
                elif status in (401, 403, 429):
                    issue_code = "blocked"
                    issue_reason = f"页面被拦了（status={status}）"
                elif status > 0:
                    issue_reason = f"页面打开了，但没抠到正文（status={status}）"

                last_reason = issue_reason
                if debug is not None:
                    attempts = debug.get("attempts")
                    if isinstance(attempts, list):
                        attempts.append(
                            {
                                "url": str(url),
                                "attempt": int(attempt + 1),
                                "status": int(status),
                                "final_url": str(final_url or ""),
                                "page_title": str(page_title or ""),
                                "issue_code": str(issue_code),
                                "issue_reason": str(issue_reason),
                            }
                        )
                if issue_code == "waf_challenge":
                    return None, issue_reason
                if issue_code != "not_found" and attempt < self._cfg.max_retries:
                    print(
                        f"[detail-retry] status_id={sid} 第 {attempt + 1}/{self._cfg.max_retries + 1} 次重试：{issue_reason}",
                        file=sys.stderr,
                    )
                    time.sleep(min(backoff, DEFAULT_BACKOFF_MAX_SEC))
                    backoff *= 2
                    continue
                break

        # Fallback: if page parsing failed, try JSON show endpoint.
        json_referrer = candidate_urls[0] if candidate_urls else BASE_URL
        json_line, json_reason = self._fetch_status_display_line_via_show_json(
            sid, referrer=json_referrer, debug=debug
        )
        if json_line:
            if debug is not None:
                debug["success_url"] = str(debug.get("show_json_url") or "")
            return json_line, None
        if json_reason and ("风控" in str(json_reason) or "验证" in str(json_reason)):
            return None, json_reason
        return None, last_reason

    @staticmethod
    def _describe_collection_payload_issue(
        obj: Any, *, list_key: str, allow_empty: bool
    ) -> Optional[str]:
        if not isinstance(obj, dict):
            return "顶层不是对象"
        rows = obj.get(list_key)
        if not isinstance(rows, list):
            return f"{list_key} 不是列表"
        if (not allow_empty) and (not rows):
            return f"{list_key} 是空列表"
        return None

    @staticmethod
    def _is_terminal_empty_user_comments_page(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        items = obj.get("items")
        if not isinstance(items, list) or items:
            return False
        next_max_id = str(obj.get("next_max_id") or "").strip()
        next_id = str(obj.get("next_id") or "").strip()
        return next_max_id == "-1" and next_id == "-1"

    def _fetch_json_with_retry(
        self,
        url: str,
        *,
        referrer: Optional[str] = None,
        retry_reason: Optional[Callable[[Any], Optional[str]]] = None,
        request_label: Optional[str] = None,
    ) -> Any:
        backoff = DEFAULT_BACKOFF_INITIAL_SEC
        last_exc: Optional[Exception] = None
        label = str(request_label or url)

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

                issue = retry_reason(obj) if retry_reason is not None else None
                if issue is not None:
                    if attempt < self._cfg.max_retries:
                        print(
                            f"[api-retry] {label} 回包不对，第 {attempt + 1}/{self._cfg.max_retries + 1} 次重试：{issue}",
                            file=sys.stderr,
                        )
                        time.sleep(min(backoff, DEFAULT_BACKOFF_MAX_SEC))
                        backoff *= 2
                        continue
                    raise RuntimeError(f"{label} 重试后回包还是不对：{issue}")

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
                if last_exc is not None:
                    print(
                        f"[api-retry] {label} 请求失败，第 {attempt + 1}/{self._cfg.max_retries + 1} 次重试：{last_exc}",
                        file=sys.stderr,
                    )
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
            allow_terminal_empty = _page_idx == 1

            def _retry_reason(payload: Any) -> Optional[str]:
                return self._describe_collection_payload_issue(
                    payload,
                    list_key="items",
                    allow_empty=(_page_idx > 1)
                    or (
                        allow_terminal_empty
                        and self._is_terminal_empty_user_comments_page(payload)
                    ),
                )

            obj = self._fetch_json_with_retry(
                url,
                retry_reason=_retry_reason,
                request_label=f"comments-api user={user_id} max_id={max_id}",
            )
            items = obj.get("items") or []
            if not items:
                if _page_idx == 1 and self._is_terminal_empty_user_comments_page(obj):
                    print(
                        f"[comments-api] user={user_id} 第一批为空且 next_max_id=-1，按无可见回复处理",
                        file=sys.stderr,
                    )
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
        first = self._fetch_json_with_retry(
            first_url,
            referrer=ref,
            retry_reason=lambda payload: self._describe_collection_payload_issue(
                payload, list_key="comments", allow_empty=True
            ),
            request_label=f"talks root={root_status_id} comment={comment_id} page=1",
        )
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
            obj = self._fetch_json_with_retry(
                url,
                referrer=ref,
                retry_reason=lambda payload: self._describe_collection_payload_issue(
                    payload, list_key="comments", allow_empty=True
                ),
                request_label=f"talks root={root_status_id} comment={comment_id} page={p}",
            )
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

        max_pages_i = max(1, int(max_pages))
        existing_pages: list[dict[str, Any]] = []
        pages_obj = existing.get("pages") if existing else None
        if isinstance(pages_obj, list):
            existing_pages = [
                page_obj for page_obj in pages_obj if isinstance(page_obj, dict)
            ]

        existing_pages_by_num: dict[int, dict[str, Any]] = {}
        for page_obj in existing_pages:
            try:
                page_num = int(page_obj.get("page") or 0)
            except Exception:
                continue
            if page_num <= 0:
                continue
            existing_pages_by_num[page_num] = page_obj

        # Fast path: if we cannot determine progress, fall back to full fetch.
        if existing_pages and not existing_pages_by_num:
            return self.fetch_talks_all_pages(
                root_status_id=root_status_id,
                comment_id=comment_id,
                max_pages=max_pages_i,
            )

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
        first = self._fetch_json_with_retry(
            first_url,
            referrer=ref,
            retry_reason=lambda payload: self._describe_collection_payload_issue(
                payload, list_key="comments", allow_empty=True
            ),
            request_label=f"talks root={root_status_id} comment={comment_id} page=1",
        )
        max_page_reported = int(first.get("maxPage") or 1)
        max_page_target = min(max_page_reported, max_pages_i)

        # Merge pages by page number to support:
        # - filling missing pages (previous partial runs)
        # - refreshing the last fetched page (new replies may appear without maxPage increasing)
        pages_by_num: dict[int, dict[str, Any]] = dict(existing_pages_by_num)
        pages_by_num[1] = first

        last_fetched = max(pages_by_num) if pages_by_num else 1
        refresh_page = min(max_page_target, last_fetched)

        need_pages: set[int] = set()
        for p in range(2, max_page_target + 1):
            if p not in pages_by_num:
                need_pages.add(p)
        if refresh_page >= 2:
            need_pages.add(refresh_page)

        for page_num in sorted(need_pages):
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
            obj = self._fetch_json_with_retry(
                url,
                referrer=ref,
                retry_reason=lambda payload: self._describe_collection_payload_issue(
                    payload, list_key="comments", allow_empty=True
                ),
                request_label=f"talks root={root_status_id} comment={comment_id} page={page_num}",
            )
            if isinstance(obj, dict):
                pages_by_num[page_num] = obj

        pages_out = [
            pages_by_num[p] for p in sorted(pages_by_num) if 1 <= p <= max_page_target
        ]

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
