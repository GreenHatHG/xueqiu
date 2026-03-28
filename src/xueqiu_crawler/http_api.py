from __future__ import annotations

import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .constants import BASE_URL, TALKS_PAGE_SIZE, USER_COMMENTS_PAGE_SIZE
from .http_debug import (
    sanitize_url_for_debug,
    single_line_text,
    summarize_payload,
    text_preview,
)
from .rate_limit import RateLimiter
from .text_sanitize import sanitize_xueqiu_text
from .xq_api import (
    ApiConfig,
    BlockedError,
    ChallengeRequiredError,
    _looks_like_html,
    _looks_like_waf_challenge,
)


XUEQIU_COOKIE_ENV = "XUEQIU_COOKIE"
DEFAULT_USER_AGENT = "Xueqiu iPhone 14.15.1"
DEFAULT_TIMEOUT_SEC = 20.0


def _require_cookie_from_env() -> str:
    cookie = str(os.environ.get(XUEQIU_COOKIE_ENV, "") or "").strip()
    if not cookie:
        raise RuntimeError(
            f"Missing {XUEQIU_COOKIE_ENV}. "
            "Set it to the full Cookie header value, e.g. "
            f"export {XUEQIU_COOKIE_ENV}='xq_a_token=...; u=...; ...'"
        )
    return cookie


def _parse_charset(content_type: str) -> str:
    ctype = str(content_type or "")
    for part in ctype.split(";"):
        part = part.strip()
        if part.lower().startswith("charset="):
            value = part.split("=", 1)[1].strip()
            return value or "utf-8"
    return "utf-8"


def _decode_body(body: bytes, *, charset: str) -> str:
    try:
        return body.decode(charset, errors="replace")
    except Exception:
        return body.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class HttpClientConfig:
    cookie: str
    user_agent: str = DEFAULT_USER_AGENT
    timeout_sec: float = DEFAULT_TIMEOUT_SEC


class XueqiuHttpApi:
    """
    A minimal browserless HTTP API client for Xueqiu, authenticated by Cookie.

    This is intended for incremental cloud runs that must not launch a browser.
    """

    def __init__(
        self,
        cfg: ApiConfig,
        http_cfg: HttpClientConfig,
    ) -> None:
        self._cfg = cfg
        self._http_cfg = http_cfg
        self._limiter = RateLimiter(cfg.min_delay_sec, cfg.jitter_sec)
        self._consecutive_blocks = 0

    @classmethod
    def from_env(cls, cfg: ApiConfig) -> "XueqiuHttpApi":
        return cls(cfg, HttpClientConfig(cookie=_require_cookie_from_env()))

    def _http_debug_enabled(self) -> bool:
        return bool(getattr(self._cfg, "http_debug", False))

    def _http_debug_log(self, message: str) -> None:
        if self._http_debug_enabled():
            print(f"[http-debug] {message}", file=sys.stderr)

    def build_url(self, path: str, params: Optional[dict[str, Any]] = None) -> str:
        p = str(path or "").strip()
        if not p.startswith("/"):
            p = f"/{p}" if p else ""
        query = urlencode(params or {})
        return f"{BASE_URL}{p}?{query}" if query else f"{BASE_URL}{p}"

    def _headers(self, *, referrer: Optional[str]) -> dict[str, str]:
        headers: dict[str, str] = {
            "accept": "application/json",
            "cookie": str(self._http_cfg.cookie),
            "user-agent": str(self._http_cfg.user_agent),
            "accept-language": "zh-Hans-CN;q=1",
            # Avoid `br` to keep stdlib-only decoding simple.
            "accept-encoding": "gzip",
            "connection": "keep-alive",
            "x-requested-with": "XMLHttpRequest",
        }
        ref = str(referrer or "").strip()
        if ref:
            headers["referer"] = ref
        return headers

    def _fetch_text_once(
        self, url: str, *, referrer: Optional[str] = None
    ) -> tuple[int, str, str]:
        self._limiter.sleep_before_next()
        target = str(url or "").strip()
        if not target:
            return 0, "", ""

        req = Request(target, headers=self._headers(referrer=referrer), method="GET")
        status = 0
        final_url = target
        body: bytes = b""
        content_type = ""
        content_encoding = ""

        try:
            with urlopen(req, timeout=float(self._http_cfg.timeout_sec)) as resp:
                status = int(getattr(resp, "status", 0) or 0)
                final_url = str(getattr(resp, "geturl", lambda: target)() or target)
                content_type = str(resp.headers.get("content-type") or "")
                content_encoding = str(resp.headers.get("content-encoding") or "")
                body = resp.read() or b""
        except HTTPError as e:
            try:
                status = int(getattr(e, "code", 0) or 0)
            except Exception:
                status = 0
            try:
                final_url = str(getattr(e, "geturl", lambda: target)() or target)
            except Exception:
                final_url = target
            try:
                content_type = str(getattr(e, "headers", {}).get("content-type") or "")
                content_encoding = str(
                    getattr(e, "headers", {}).get("content-encoding") or ""
                )
            except Exception:
                content_type = ""
                content_encoding = ""
            try:
                body = e.read() or b""
            except Exception:
                body = b""
        except URLError as e:
            raise RuntimeError(f"network error: {e}") from e

        if content_encoding.lower().strip() == "gzip" and body:
            try:
                body = gzip.decompress(body)
            except Exception:
                pass

        charset = _parse_charset(content_type)
        text = _decode_body(body, charset=charset)
        return int(status), str(text or ""), str(final_url or target)

    @staticmethod
    def _describe_collection_payload_issue(
        obj: Any, *, list_key: str, allow_empty: bool
    ) -> Optional[str]:
        if not isinstance(obj, dict):
            return "top-level is not an object"
        rows = obj.get(list_key)
        if not isinstance(rows, list):
            return f"{list_key} is not a list"
        if (not allow_empty) and (not rows):
            return f"{list_key} is empty"
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

    @staticmethod
    def _extract_timeline_rows(obj: Any) -> Optional[list[Any]]:
        if not isinstance(obj, dict):
            return None
        candidates: list[Any] = [obj.get("statuses"), obj.get("list"), obj.get("items")]
        data_obj = obj.get("data")
        if isinstance(data_obj, dict):
            candidates.extend(
                [data_obj.get("statuses"), data_obj.get("list"), data_obj.get("items")]
            )
        elif isinstance(data_obj, list):
            candidates.append(data_obj)
        for value in candidates:
            if isinstance(value, list):
                return value
        return None

    @classmethod
    def _describe_timeline_payload_issue(cls, obj: Any) -> Optional[str]:
        if not isinstance(obj, dict):
            return "timeline payload is not an object"
        rows = cls._extract_timeline_rows(obj)
        if rows is None:
            return "timeline payload missing statuses/list/items"
        return None

    def _fetch_json_with_retry(
        self,
        url: str,
        *,
        referrer: Optional[str] = None,
        retry_reason: Optional[Callable[[Any], Optional[str]]] = None,
        request_label: Optional[str] = None,
    ) -> Any:
        backoff = 3.0
        last_exc: Optional[Exception] = None
        label = str(request_label or url)
        total_attempts = int(self._cfg.max_retries) + 1

        for attempt in range(total_attempts):
            attempt_started = time.monotonic()
            attempt_no = int(attempt) + 1
            self._http_debug_log(
                f"{label} attempt={attempt_no}/{total_attempts} request "
                f"url={sanitize_url_for_debug(url)} "
                f"referrer={sanitize_url_for_debug(str(referrer or ''))}"
            )
            try:
                status, text, final_url = self._fetch_text_once(url, referrer=referrer)

                looks_html = _looks_like_html(text)
                elapsed_ms = int((time.monotonic() - attempt_started) * 1000)
                self._http_debug_log(
                    f"{label} attempt={attempt_no}/{total_attempts} response "
                    f"status={int(status)} elapsed_ms={elapsed_ms} body_len={len(text)} "
                    f"looks_html={int(bool(looks_html))} "
                    f"final_url={sanitize_url_for_debug(final_url)}"
                )
                if status in (401, 403, 429):
                    raise BlockedError(f"blocked or not logged in (status={status})")
                if looks_html:
                    is_waf = bool(
                        ("alichlgref=" in final_url.lower())
                        or ("md5__1038=" in final_url.lower())
                        or ("_waf_" in final_url.lower())
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
                    preview, truncated, total_len = text_preview(text)
                    self._http_debug_log(
                        f"{label} attempt={attempt_no}/{total_attempts} json_parse_failed "
                        f"status={int(status)} text_len={total_len} "
                        f"text_head={single_line_text(preview)} truncated={int(truncated)} "
                        f"error={single_line_text(str(e))}"
                    )
                    if (
                        ("alichlgref=" in final_url.lower())
                        or ("md5__1038=" in final_url.lower())
                        or ("_waf_" in final_url.lower())
                        or _looks_like_waf_challenge(text)
                    ):
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
                    payload_summary = summarize_payload(obj)
                    preview, truncated, total_len = text_preview(text)
                    self._http_debug_log(
                        f"{label} attempt={attempt_no}/{total_attempts} bad_payload "
                        f"status={int(status)} issue={single_line_text(str(issue))} "
                        f"{payload_summary} "
                        f"url={sanitize_url_for_debug(url)} "
                        f"final_url={sanitize_url_for_debug(final_url)}"
                    )
                    self._http_debug_log(
                        f"{label} attempt={attempt_no}/{total_attempts} bad_payload "
                        f"text_len={total_len} text_head={single_line_text(preview)} "
                        f"truncated={int(truncated)}"
                    )
                    if attempt < int(self._cfg.max_retries):
                        print(
                            f"[api-retry] {label} bad payload, attempt {attempt + 1}/{int(self._cfg.max_retries) + 1}: {issue}",
                            file=sys.stderr,
                        )
                        time.sleep(min(backoff, 60.0))
                        backoff *= 2
                        continue
                    raise RuntimeError(f"{label} still bad after retries: {issue}")

                self._consecutive_blocks = 0
                return obj
            except ChallengeRequiredError:
                self._http_debug_log(
                    f"{label} attempt={attempt_no}/{total_attempts} challenge_required"
                )
                raise
            except BlockedError as e:
                self._consecutive_blocks += 1
                last_exc = e
                self._http_debug_log(
                    f"{label} attempt={attempt_no}/{total_attempts} blocked "
                    f"error={single_line_text(str(e))}"
                )
            except Exception as e:
                last_exc = e
                self._http_debug_log(
                    f"{label} attempt={attempt_no}/{total_attempts} failed "
                    f"error={single_line_text(str(e))}"
                )

            if attempt < int(self._cfg.max_retries):
                if last_exc is not None:
                    print(
                        f"[api-retry] {label} request failed, attempt {attempt + 1}/{int(self._cfg.max_retries) + 1}: {last_exc}",
                        file=sys.stderr,
                    )
                time.sleep(min(backoff, 60.0))
                backoff *= 2

            if self._consecutive_blocks >= int(self._cfg.max_consecutive_blocks):
                raise BlockedError(
                    f"too many blocked responses ({self._consecutive_blocks}), stop to protect account"
                ) from last_exc

        assert last_exc is not None
        raise last_exc

    def fetch_timeline_first_page(self, user_id: str) -> dict[str, Any]:
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("user_id is empty")
        ref = f"{BASE_URL}/u/{uid}"
        candidates = [
            self.build_url(
                "/statuses/user_timeline.json",
                {"user_id": uid, "page": 1, "count": 20},
            ),
            self.build_url(
                "/v4/statuses/user_timeline.json", {"page": 1, "user_id": uid}
            ),
        ]
        last_exc: Optional[Exception] = None
        for url in candidates:
            try:
                obj = self._fetch_json_with_retry(
                    url,
                    referrer=ref,
                    retry_reason=self._describe_timeline_payload_issue,
                    request_label=f"timeline user={uid} page=1",
                )
                return obj if isinstance(obj, dict) else {"data": obj}
            except (ChallengeRequiredError, BlockedError) as e:
                last_exc = e
                break
            except Exception as e:
                last_exc = e
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("timeline fetch failed")

    def fetch_user_comments_first_page(
        self, user_id: str
    ) -> tuple[int, list[dict[str, Any]]]:
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("user_id is empty")
        url = self.build_url(
            "/statuses/user/comments.json",
            {"user_id": uid, "size": USER_COMMENTS_PAGE_SIZE, "max_id": -1},
        )

        def _retry_reason(payload: Any) -> Optional[str]:
            allow_terminal_empty = True
            return self._describe_collection_payload_issue(
                payload,
                list_key="items",
                allow_empty=allow_terminal_empty
                and self._is_terminal_empty_user_comments_page(payload),
            )

        obj = self._fetch_json_with_retry(
            url,
            referrer=f"{BASE_URL}/u/{uid}#/comments",
            retry_reason=_retry_reason,
            request_label=f"comments user={uid} max_id=-1",
        )
        if not isinstance(obj, dict):
            raise RuntimeError("comments payload is not an object")
        items = obj.get("items") or []
        out = [it for it in items if isinstance(it, dict)]
        try:
            next_max_id = int(obj.get("next_max_id") or -1)
        except Exception:
            next_max_id = -1
        return next_max_id, out

    def fetch_talks_all_pages(
        self,
        *,
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
                payload, list_key="comments", allow_empty=False
            ),
            request_label=f"talks root={root_status_id} comment={comment_id} page=1",
        )
        if not isinstance(first, dict):
            raise RuntimeError("talks payload is not an object")
        max_page = int(first.get("maxPage") or 1)
        max_page = min(max_page, int(max_pages))

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
            if isinstance(obj, dict):
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
        *,
        root_status_id: str,
        comment_id: str,
        max_pages: int,
        existing: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
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

        if existing_pages and not existing_pages_by_num:
            # Fast path: if we cannot determine progress, fall back to full fetch.
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
        if not isinstance(first, dict):
            raise RuntimeError("talks payload is not an object")
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

    @staticmethod
    def _user_label(user_obj: Any) -> str:
        if not isinstance(user_obj, dict):
            return ""
        for key in ("screen_name", "screenName", "name", "nickname"):
            val = user_obj.get(key)
            if val is None:
                continue
            s = str(val).strip()
            if s:
                return s
        uid = user_obj.get("id") or user_obj.get("user_id") or user_obj.get("uid")
        return str(uid).strip() if uid is not None else ""

    @staticmethod
    def _extract_status_obj(obj: Any) -> Optional[dict[str, Any]]:
        if not isinstance(obj, dict):
            return None
        status = obj.get("status")
        if isinstance(status, dict):
            return status
        data = obj.get("data")
        if isinstance(data, dict):
            st = data.get("status")
            if isinstance(st, dict):
                return st
        if ("id" in obj) and ("text" in obj or "description" in obj):
            return obj
        return None

    def fetch_status_display_line(
        self, status_id: str, *, referrer: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        sid = str(status_id or "").strip()
        if not sid:
            return None, "empty status_id"

        candidates = [
            self.build_url("/statuses/show.json", {"id": sid}),
            self.build_url("/v5/statuses/show.json", {"id": sid}),
        ]
        last_exc: Optional[Exception] = None
        for url in candidates:
            try:
                obj = self._fetch_json_with_retry(
                    url,
                    referrer=referrer or BASE_URL,
                    request_label=f"status-show id={sid}",
                )
                st = self._extract_status_obj(obj)
                if not st:
                    return None, "status payload missing"
                raw_text = st.get("text") or st.get("description") or ""
                text = str(sanitize_xueqiu_text(raw_text) or "").strip()
                if not text:
                    return None, "empty status text"
                author = self._user_label(st.get("user"))
                return (f"{author}：{text}" if author else text), None
            except Exception as e:
                last_exc = e
                continue
        return None, f"status fetch failed: {last_exc}"
