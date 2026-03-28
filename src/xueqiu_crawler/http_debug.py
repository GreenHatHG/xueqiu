from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .constants import HTTP_DEBUG_MAX_KEYS, HTTP_DEBUG_TEXT_PREVIEW_CHARS


DEBUG_REDACTED_VALUE = "***"
DEBUG_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on", "y", "t"})
DEBUG_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "access_token",
        "auth",
        "authorization",
        "cookie",
        "key",
        "signature",
        "token",
        "xq_a_token",
        "xq_id_token",
        "xq_r_token",
    }
)
DEBUG_LIST_LIKE_KEYS = ("items", "comments", "statuses", "list", "data")


def env_flag_enabled(raw_value: Any) -> bool:
    value = str(raw_value or "").strip().lower()
    return value in DEBUG_TRUTHY_VALUES


def sanitize_url_for_debug(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
        if not parts.query:
            return text
        masked_items: list[tuple[str, str]] = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            k = str(key or "").strip()
            if k.lower() in DEBUG_SENSITIVE_QUERY_KEYS:
                masked_items.append((k, DEBUG_REDACTED_VALUE))
            else:
                masked_items.append((k, str(value or "")))
        query = urlencode(masked_items, doseq=True)
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, query, parts.fragment)
        )
    except Exception:
        return text


def single_line_text(text: str) -> str:
    return str(text or "").replace("\r", "\\r").replace("\n", "\\n")


def text_preview(
    text: str, *, limit: int = HTTP_DEBUG_TEXT_PREVIEW_CHARS
) -> tuple[str, bool, int]:
    raw = str(text or "")
    total_len = len(raw)
    if limit <= 0:
        return "", total_len > 0, total_len
    if total_len <= limit:
        return raw, False, total_len
    return raw[:limit], True, total_len


def summarize_payload(payload: Any, *, max_keys: int = HTTP_DEBUG_MAX_KEYS) -> str:
    if not isinstance(payload, dict):
        return f"payload_type={type(payload).__name__}"

    keys = [str(key) for key in payload.keys()]
    parts: list[str] = ["payload_type=dict", f"keys={keys[: max(1, int(max_keys))]}"]
    for key in DEBUG_LIST_LIKE_KEYS:
        if key not in payload:
            continue
        value = payload.get(key)
        if isinstance(value, list):
            parts.append(f"{key}_type=list")
            parts.append(f"{key}_len={len(value)}")
        else:
            parts.append(f"{key}_type={type(value).__name__}")
    return " ".join(parts)
