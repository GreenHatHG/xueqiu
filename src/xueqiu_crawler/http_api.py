from __future__ import annotations

import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from http.cookiejar import Cookie, CookieJar
from http.cookies import SimpleCookie
from threading import Lock
from typing import Any, Callable, Optional
from urllib.parse import urlsplit
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .constants import BASE_URL, TALKS_PAGE_SIZE, USER_COMMENTS_PAGE_SIZE
from .http_debug import (
    sanitize_url_for_debug,
    single_line_text,
    summarize_payload,
    text_preview,
)
from .rate_limit import RateLimiter
from .text_sanitize import sanitize_xueqiu_text
from .waf_signer import (
    extract_challenge_script_src,
    run_waf_signer,
    strip_md5_query_param,
)
from .xq_api import (
    ApiConfig,
    BlockedError,
    ChallengeRequiredError,
    _looks_like_html,
    _looks_like_waf_challenge,
)


XUEQIU_COOKIE_ENV = "XUEQIU_COOKIE"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SEC = 20.0
HQ_WARMUP_URL = f"{BASE_URL}/hq"
MAX_WAF_SIGN_ROUNDS = 4
DEFAULT_COOKIE_DOMAIN = ".xueqiu.com"
DEFAULT_COOKIE_PATH = "/"
WAF_SIGN_PATHS = frozenset(("/statuses/show.json", "/v5/statuses/show.json"))
WAF_SIGN_CACHE_TTL_SEC = 10 * 60.0
HTTP_TRACE_ID_PREFIX = "xq-http"
HTTP_TRACE_ID_RANDOM_BYTES = 4


def _cookie_from_env() -> str:
    return str(os.environ.get(XUEQIU_COOKIE_ENV, "") or "").strip()


def _build_http_trace_id() -> str:
    timestamp_ms = int(time.time() * 1000)
    random_suffix = os.urandom(HTTP_TRACE_ID_RANDOM_BYTES).hex()
    return f"{HTTP_TRACE_ID_PREFIX}-{timestamp_ms}-{random_suffix}"


@dataclass(frozen=True)
class _WafSignedCacheEntry:
    signed_url: str
    cookie_writes: tuple[str, ...]
    expires_at_monotonic: float


_WAF_SIGN_CACHE: dict[str, _WafSignedCacheEntry] = {}
_WAF_SIGN_CACHE_LOCK = Lock()


def _waf_sign_cache_key(url: str) -> str:
    return strip_md5_query_param(url)


def _get_waf_sign_cache_entry(unsigned_url: str) -> Optional[_WafSignedCacheEntry]:
    key = _waf_sign_cache_key(unsigned_url)
    now_monotonic = time.monotonic()
    with _WAF_SIGN_CACHE_LOCK:
        entry = _WAF_SIGN_CACHE.get(key)
        if entry is None:
            return None
        if entry.expires_at_monotonic > now_monotonic:
            return entry
        _WAF_SIGN_CACHE.pop(key, None)
    return None


def _store_waf_sign_cache_entry(
    unsigned_url: str, signed_url: str, cookie_writes: list[str]
) -> None:
    key = _waf_sign_cache_key(unsigned_url)
    entry = _WafSignedCacheEntry(
        signed_url=str(signed_url),
        cookie_writes=tuple(
            str(item) for item in cookie_writes if str(item or "").strip()
        ),
        expires_at_monotonic=time.monotonic() + WAF_SIGN_CACHE_TTL_SEC,
    )
    with _WAF_SIGN_CACHE_LOCK:
        _WAF_SIGN_CACHE[key] = entry


def _drop_waf_sign_cache_entry(unsigned_url: str) -> None:
    key = _waf_sign_cache_key(unsigned_url)
    with _WAF_SIGN_CACHE_LOCK:
        _WAF_SIGN_CACHE.pop(key, None)


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


def _parse_cookie_header(raw_cookie: str) -> dict[str, str]:
    cookie = str(raw_cookie or "").strip()
    if not cookie:
        return {}

    jar = SimpleCookie()
    try:
        jar.load(cookie)
    except Exception:
        jar = SimpleCookie()
    parsed = {str(key): morsel.value for key, morsel in jar.items()}
    if parsed:
        return parsed

    out: dict[str, str] = {}
    for part in cookie.split(";"):
        item = str(part or "").strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        key_str = str(key or "").strip()
        if key_str:
            out[key_str] = str(value or "")
    return out


@dataclass(frozen=True)
class _CookieSpec:
    name: str
    value: str
    domain: str = DEFAULT_COOKIE_DOMAIN
    path: str = DEFAULT_COOKIE_PATH
    secure: bool = False


def _cookie_spec_from_assignment(raw_value: str) -> Optional[_CookieSpec]:
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return None

    parsed = SimpleCookie()
    try:
        parsed.load(raw_text)
    except Exception:
        parsed = SimpleCookie()
    if parsed:
        first_key = next(iter(parsed.keys()), "")
        if first_key:
            morsel = parsed[first_key]
            return _CookieSpec(
                name=str(first_key).strip(),
                value=str(morsel.value or ""),
                domain=str(morsel["domain"] or DEFAULT_COOKIE_DOMAIN).strip()
                or DEFAULT_COOKIE_DOMAIN,
                path=str(morsel["path"] or DEFAULT_COOKIE_PATH).strip()
                or DEFAULT_COOKIE_PATH,
                secure=bool(str(morsel["secure"] or "").strip()),
            )

    first = raw_text.split(";", 1)[0].strip()
    if "=" not in first:
        return None
    key, value = first.split("=", 1)
    key_str = str(key or "").strip()
    if not key_str:
        return None
    return _CookieSpec(name=key_str, value=str(value or ""))


def _build_cookie(cookie_spec: _CookieSpec) -> Cookie:
    domain = (
        str(cookie_spec.domain or DEFAULT_COOKIE_DOMAIN).strip()
        or DEFAULT_COOKIE_DOMAIN
    )
    path = str(cookie_spec.path or DEFAULT_COOKIE_PATH).strip() or DEFAULT_COOKIE_PATH
    return Cookie(
        version=0,
        name=str(cookie_spec.name),
        value=str(cookie_spec.value),
        port=None,
        port_specified=False,
        domain=domain,
        domain_specified=bool(domain),
        domain_initial_dot=domain.startswith("."),
        path=path,
        path_specified=True,
        secure=bool(cookie_spec.secure),
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={},
        rfc2109=False,
    )


def _extract_set_cookie_specs(headers_obj: Any) -> list[_CookieSpec]:
    if headers_obj is None:
        return []

    values: list[str] = []
    get_all = getattr(headers_obj, "get_all", None)
    if callable(get_all):
        try:
            raw_values = get_all("Set-Cookie") or []
            values.extend(str(item) for item in raw_values if str(item or "").strip())
        except Exception:
            values = []

    if not values:
        try:
            single = headers_obj.get("Set-Cookie")
        except Exception:
            single = None
        if str(single or "").strip():
            values.append(str(single))

    out: list[_CookieSpec] = []
    for item in values:
        cookie_spec = _cookie_spec_from_assignment(item)
        if cookie_spec is not None:
            out.append(cookie_spec)
    return out


@dataclass(frozen=True)
class HttpClientConfig:
    cookie: str = ""
    user_agent: str = DEFAULT_USER_AGENT
    timeout_sec: float = DEFAULT_TIMEOUT_SEC


@dataclass(frozen=True)
class _HttpFetchResult:
    status: int
    text: str
    final_url: str
    content_type: str


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
        self._cookie_jar = CookieJar()
        self._opener = build_opener(HTTPCookieProcessor(self._cookie_jar))
        self._load_initial_cookies(http_cfg.cookie)
        self._warmup_attempted = False
        self._last_waf_signed_url = ""
        self._last_waf_signed_cookie_writes = 0
        self._last_waf_round = 0
        self._last_waf_cache_hit = False
        self._current_trace_id = ""
        self._last_trace_id = ""

    @classmethod
    def from_env(cls, cfg: ApiConfig) -> "XueqiuHttpApi":
        return cls(cfg, HttpClientConfig(cookie=_cookie_from_env()))

    def _http_debug_enabled(self) -> bool:
        return bool(getattr(self._cfg, "http_debug", False))

    def _http_debug_log(self, message: str, *, trace_id: Optional[str] = None) -> None:
        if self._http_debug_enabled():
            active_trace_id = str(trace_id or self._current_trace_id or "").strip()
            prefix = (
                f"[http-debug] trace_id={active_trace_id} "
                if active_trace_id
                else "[http-debug] "
            )
            print(f"{prefix}{message}", file=sys.stderr)

    def _push_trace_id(self, trace_id: Optional[str] = None) -> str:
        previous_trace_id = str(self._current_trace_id or "").strip()
        active_trace_id = (
            str(trace_id or "").strip() or previous_trace_id or _build_http_trace_id()
        )
        self._current_trace_id = active_trace_id
        self._last_trace_id = active_trace_id
        return previous_trace_id

    def _pop_trace_id(self, previous_trace_id: str) -> None:
        self._current_trace_id = str(previous_trace_id or "").strip()

    def build_url(self, path: str, params: Optional[dict[str, Any]] = None) -> str:
        p = str(path or "").strip()
        if not p.startswith("/"):
            p = f"/{p}" if p else ""
        query = urlencode(params or {})
        return f"{BASE_URL}{p}?{query}" if query else f"{BASE_URL}{p}"

    def _load_initial_cookies(self, raw_cookie: str) -> None:
        for key, value in _parse_cookie_header(raw_cookie).items():
            self._set_cookie(_CookieSpec(name=key, value=value))

    def _set_cookie(self, cookie_spec: _CookieSpec) -> None:
        if not str(cookie_spec.name or "").strip():
            return
        self._cookie_jar.set_cookie(_build_cookie(cookie_spec))

    def _reset_last_waf_state(self) -> None:
        self._last_waf_signed_url = ""
        self._last_waf_signed_cookie_writes = 0
        self._last_waf_round = 0
        self._last_waf_cache_hit = False

    def _set_last_waf_state(
        self,
        *,
        signed_url: str,
        cookie_writes_count: int,
        round_no: int,
        cache_hit: bool,
    ) -> None:
        self._last_waf_signed_url = str(signed_url or "")
        self._last_waf_signed_cookie_writes = int(cookie_writes_count)
        self._last_waf_round = int(round_no)
        self._last_waf_cache_hit = bool(cache_hit)

    def _cookie_header_value(self) -> str:
        latest: dict[str, str] = {}
        for item in self._cookie_jar:
            key = str(getattr(item, "name", "") or "").strip()
            if key:
                latest[key] = str(getattr(item, "value", "") or "")
        return "; ".join(f"{key}={value}" for key, value in latest.items())

    def _apply_cookie_writes(self, writes: list[str]) -> None:
        for item in writes:
            cookie_spec = _cookie_spec_from_assignment(item)
            if cookie_spec is not None:
                self._set_cookie(cookie_spec)

    def _update_cookies_from_headers(self, headers_obj: Any) -> None:
        for cookie_spec in _extract_set_cookie_specs(headers_obj):
            self._set_cookie(cookie_spec)

    def _headers(self, *, referrer: Optional[str]) -> dict[str, str]:
        headers: dict[str, str] = {
            "accept": "application/json,text/plain,*/*",
            "user-agent": str(self._http_cfg.user_agent),
            "accept-language": "zh-CN,zh;q=0.9",
            # Avoid `br` to keep stdlib-only decoding simple.
            "accept-encoding": "gzip",
            "connection": "keep-alive",
            "x-requested-with": "XMLHttpRequest",
        }
        ref = str(referrer or "").strip()
        if ref:
            headers["referer"] = ref
        return headers

    def _fetch_raw_once(
        self, url: str, *, referrer: Optional[str] = None
    ) -> _HttpFetchResult:
        target = str(url or "").strip()
        if not target:
            return _HttpFetchResult(0, "", "", "")

        req = Request(target, headers=self._headers(referrer=referrer), method="GET")
        status = 0
        final_url = target
        body: bytes = b""
        content_type = ""
        content_encoding = ""
        response_headers: Any = None

        try:
            with self._opener.open(
                req, timeout=float(self._http_cfg.timeout_sec)
            ) as resp:
                status = int(getattr(resp, "status", 0) or 0)
                final_url = str(getattr(resp, "geturl", lambda: target)() or target)
                response_headers = getattr(resp, "headers", None)
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
                response_headers = getattr(e, "headers", None)
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

        self._update_cookies_from_headers(response_headers)
        charset = _parse_charset(content_type)
        text = _decode_body(body, charset=charset)
        return _HttpFetchResult(
            int(status),
            str(text or ""),
            str(final_url or target),
            str(content_type or ""),
        )

    @staticmethod
    def _looks_like_waf_response(*, text: str, final_url: str) -> bool:
        if not _looks_like_html(text):
            return False
        final_url_lower = str(final_url or "").lower()
        return bool(
            ("alichlgref=" in final_url_lower)
            or ("md5__1038=" in final_url_lower)
            or ("_waf_" in final_url_lower)
            or _looks_like_waf_challenge(text)
        )

    def _ensure_hq_warmup(self) -> None:
        if self._warmup_attempted:
            return
        self._warmup_attempted = True
        try:
            result = self._fetch_raw_once(HQ_WARMUP_URL, referrer=BASE_URL)
        except Exception as exc:
            self._http_debug_log(f"warmup hq failed error={single_line_text(str(exc))}")
            return
        self._http_debug_log(
            "warmup hq "
            f"status={int(result.status)} "
            f"final_url={sanitize_url_for_debug(result.final_url)} "
            f"body_len={len(result.text)}"
        )

    def _fetch_challenge_external_js(self, challenge_html: str) -> str:
        src = extract_challenge_script_src(challenge_html)
        if not src:
            return ""
        target = str(src or "").strip()
        if target.startswith("//"):
            target = f"https:{target}"
        elif target.startswith("/"):
            target = f"{BASE_URL}{target}"
        elif (not target.startswith("http://")) and (not target.startswith("https://")):
            target = f"{BASE_URL}/{target.lstrip('/')}"
        try:
            result = self._fetch_raw_once(target, referrer=BASE_URL)
        except Exception as exc:
            self._http_debug_log(
                "challenge external js fetch failed "
                f"url={sanitize_url_for_debug(target)} "
                f"error={single_line_text(str(exc))}"
            )
            return ""
        if _looks_like_html(result.text):
            self._http_debug_log(
                "challenge external js looks like html "
                f"url={sanitize_url_for_debug(target)} "
                f"final_url={sanitize_url_for_debug(result.final_url)}"
            )
            return ""
        return str(result.text or "")

    def _resolve_waf_challenge(
        self,
        *,
        url: str,
        referrer: Optional[str],
        challenge_html: str,
    ) -> Optional[_HttpFetchResult]:
        self._reset_last_waf_state()
        original_unsigned_url = strip_md5_query_param(url)
        unsigned_url = original_unsigned_url
        html = str(challenge_html or "")
        for round_no in range(1, MAX_WAF_SIGN_ROUNDS + 1):
            external_js = self._fetch_challenge_external_js(html)
            signer_result = run_waf_signer(
                unsigned_url,
                html,
                external_js=external_js,
            )
            if not signer_result.signed_url:
                self._http_debug_log(
                    "waf signer failed "
                    f"round={round_no}/{MAX_WAF_SIGN_ROUNDS} "
                    f"url={sanitize_url_for_debug(unsigned_url)} "
                    f"error={single_line_text(signer_result.error_text)}"
                )
                _drop_waf_sign_cache_entry(original_unsigned_url)
                return None

            self._apply_cookie_writes(signer_result.cookie_writes)
            signed_url = str(signer_result.signed_url)
            self._set_last_waf_state(
                signed_url=signed_url,
                cookie_writes_count=len(signer_result.cookie_writes),
                round_no=round_no,
                cache_hit=False,
            )
            self._http_debug_log(
                "waf signer produced url "
                f"round={round_no}/{MAX_WAF_SIGN_ROUNDS} "
                f"signed_url={sanitize_url_for_debug(signed_url)} "
                f"cookie_writes={len(signer_result.cookie_writes)}"
            )

            result = self._fetch_raw_once(signed_url, referrer=referrer)
            if not self._looks_like_waf_response(
                text=result.text, final_url=result.final_url
            ):
                _store_waf_sign_cache_entry(
                    original_unsigned_url, signed_url, signer_result.cookie_writes
                )
                return result

            html = str(result.text or "")
            unsigned_url = strip_md5_query_param(signed_url)
            self._http_debug_log(
                "waf challenge continues "
                f"round={round_no}/{MAX_WAF_SIGN_ROUNDS} "
                f"status={int(result.status)} "
                f"final_url={sanitize_url_for_debug(result.final_url)}"
            )
        _drop_waf_sign_cache_entry(original_unsigned_url)
        return None

    def _try_fetch_cached_waf_response(
        self, unsigned_url: str, *, referrer: Optional[str]
    ) -> Optional[_HttpFetchResult]:
        entry = _get_waf_sign_cache_entry(unsigned_url)
        if entry is None:
            return None

        self._apply_cookie_writes(list(entry.cookie_writes))
        self._set_last_waf_state(
            signed_url=entry.signed_url,
            cookie_writes_count=len(entry.cookie_writes),
            round_no=0,
            cache_hit=True,
        )
        self._http_debug_log(
            "waf signer cache hit "
            f"url={sanitize_url_for_debug(unsigned_url)} "
            f"signed_url={sanitize_url_for_debug(entry.signed_url)} "
            f"cookie_writes={len(entry.cookie_writes)}"
        )
        result = self._fetch_raw_once(entry.signed_url, referrer=referrer)
        if not self._looks_like_waf_response(
            text=result.text, final_url=result.final_url
        ):
            return result

        _drop_waf_sign_cache_entry(unsigned_url)
        self._reset_last_waf_state()
        self._http_debug_log(
            "waf signer cache stale "
            f"url={sanitize_url_for_debug(unsigned_url)} "
            f"final_url={sanitize_url_for_debug(result.final_url)}"
        )
        return None

    def _fetch_text_once(
        self,
        url: str,
        *,
        referrer: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> tuple[int, str, str]:
        previous_trace_id = self._push_trace_id(trace_id)
        try:
            self._limiter.sleep_before_next()
            self._reset_last_waf_state()
            target = str(url or "").strip()
            if not target:
                return 0, "", ""
            request_target = (
                target if target == HQ_WARMUP_URL else strip_md5_query_param(target)
            )
            if request_target != HQ_WARMUP_URL:
                self._ensure_hq_warmup()

            can_sign = self._should_run_waf_signer(request_target)
            if can_sign:
                cached_result = self._try_fetch_cached_waf_response(
                    request_target, referrer=referrer
                )
                if cached_result is not None:
                    return (
                        int(cached_result.status),
                        str(cached_result.text),
                        str(cached_result.final_url),
                    )

            result = self._fetch_raw_once(request_target, referrer=referrer)
            if can_sign and self._looks_like_waf_response(
                text=result.text, final_url=result.final_url
            ):
                self._http_debug_log(
                    "waf challenge detected "
                    f"url={sanitize_url_for_debug(request_target)} "
                    f"final_url={sanitize_url_for_debug(result.final_url)} "
                    f"status={int(result.status)}"
                )
                resolved = self._resolve_waf_challenge(
                    url=request_target,
                    referrer=referrer,
                    challenge_html=result.text,
                )
                if resolved is not None:
                    return (
                        int(resolved.status),
                        str(resolved.text),
                        str(resolved.final_url),
                    )

            return int(result.status), str(result.text), str(result.final_url)
        finally:
            self._pop_trace_id(previous_trace_id)

    @staticmethod
    def _should_run_waf_signer(url: str) -> bool:
        path = str(urlsplit(str(url or "")).path or "").strip()
        return path in WAF_SIGN_PATHS

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
        previous_trace_id = self._push_trace_id()
        trace_id = str(self._current_trace_id or "").strip()
        try:
            for attempt in range(total_attempts):
                attempt_started = time.monotonic()
                attempt_no = int(attempt) + 1
                self._http_debug_log(
                    f"{label} attempt={attempt_no}/{total_attempts} request "
                    f"url={sanitize_url_for_debug(url)} "
                    f"referrer={sanitize_url_for_debug(str(referrer or ''))}"
                )
                try:
                    status, text, final_url = self._fetch_text_once(
                        url,
                        referrer=referrer,
                        trace_id=trace_id,
                    )

                    looks_html = _looks_like_html(text)
                    elapsed_ms = int((time.monotonic() - attempt_started) * 1000)
                    self._http_debug_log(
                        f"{label} attempt={attempt_no}/{total_attempts} response "
                        f"status={int(status)} elapsed_ms={elapsed_ms} body_len={len(text)} "
                        f"looks_html={int(bool(looks_html))} "
                        f"final_url={sanitize_url_for_debug(final_url)}"
                    )
                    if status in (401, 403, 429):
                        raise BlockedError(
                            f"blocked or not logged in (status={status})"
                        )
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
                        raise BlockedError(
                            f"blocked or not logged in (status={status})"
                        )

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
                                f"[api-retry] trace_id={trace_id} {label} bad payload, "
                                f"attempt {attempt + 1}/{int(self._cfg.max_retries) + 1}: {issue}",
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
                            f"[api-retry] trace_id={trace_id} {label} request failed, "
                            f"attempt {attempt + 1}/{int(self._cfg.max_retries) + 1}: {last_exc}",
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
        finally:
            self._pop_trace_id(previous_trace_id)

    def fetch_timeline_first_page(self, user_id: str) -> dict[str, Any]:
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("user_id is empty")
        ref = f"{BASE_URL}/u/{uid}"
        url = self.build_url(
            "/statuses/user_timeline.json",
            {"user_id": uid, "page": 1, "count": 20},
        )
        obj = self._fetch_json_with_retry(
            url,
            referrer=ref,
            retry_reason=self._describe_timeline_payload_issue,
            request_label=f"timeline user={uid} page=1",
        )
        return obj if isinstance(obj, dict) else {"data": obj}

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
