from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

from .constants import BASE_URL, BEIJING_TIMEZONE_NAME
from .text_sanitize import sanitize_xueqiu_text


MERGED_TABLE_NAME = "merged_records"
RAW_TABLE_NAME = "raw_records"

KIND_STATUS = "status"
KIND_COMMENT = "comment"
KIND_TALK = "talk"
KIND_ENTRY = "entry"

MERGE_KEY_STATUS_PREFIX = f"{KIND_STATUS}:"
MERGE_KEY_COMMENT_PREFIX = f"{KIND_COMMENT}:"
MERGE_KEY_TALK_PREFIX = f"{KIND_TALK}:"
MERGE_KEY_ENTRY_STATUS_PREFIX = f"{KIND_ENTRY}:status:"
MERGE_KEY_ENTRY_CHAIN_PREFIX = f"{KIND_ENTRY}:chain:"
CRAWL_PROGRESS_TABLE_NAME = "crawl_progress"
TALKS_PROGRESS_TABLE_NAME = "talks_progress"
CRAWL_CHECKPOINTS_TABLE_NAME = "crawl_checkpoints"
USERNAME_COLUMN_NAME = "username"

DEFAULT_TALK_TEXT_MAX_CHARS = 4000
TALK_TEXT_SEPARATOR = "\n\n---\n\n"
EMPTY_TEXT_PLACEHOLDER = "(无正文)"
CONTEXT_JSON_DEFAULT = "{}"
BEIJING_TIMEZONE = ZoneInfo(BEIJING_TIMEZONE_NAME)


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _beijing_iso_now() -> str:
    return dt.datetime.now(tz=BEIJING_TIMEZONE).replace(microsecond=0).isoformat()


def _merge_key_kind(merge_key: Any) -> str:
    key = str(merge_key or "")
    if key.startswith(MERGE_KEY_STATUS_PREFIX):
        return KIND_STATUS
    if key.startswith(MERGE_KEY_COMMENT_PREFIX):
        return KIND_COMMENT
    if key.startswith(MERGE_KEY_TALK_PREFIX):
        return KIND_TALK
    if key.startswith(f"{KIND_ENTRY}:"):
        return KIND_ENTRY
    return ""


def _merge_key_like(prefix: str) -> str:
    return f"{str(prefix)}%"


def _try_load_json_obj(value: Any) -> Optional[dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _extract_text_from_obj(obj: dict[str, Any]) -> str:
    for key in (
        "text",
        "description",
        "title",
        "content",
        "content_text",
        "contentText",
    ):
        val = obj.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    return ""


def _extract_root_status_from_comment_item(
    item_obj: dict[str, Any],
) -> Optional[dict[str, Any]]:
    st = item_obj.get("status")
    return st if isinstance(st, dict) else None


def _extract_retweeted_status_from_status_obj(
    status_obj: dict[str, Any],
) -> Optional[dict[str, Any]]:
    retweeted_status = status_obj.get("retweeted_status")
    return retweeted_status if isinstance(retweeted_status, dict) else None


def _status_url_from_status_obj(status_obj: dict[str, Any]) -> str:
    uid = status_obj.get("user_id")
    sid = status_obj.get("id")
    if uid is not None and sid is not None:
        return f"{BASE_URL}/{uid}/{sid}"
    target = status_obj.get("target")
    if isinstance(target, str) and target.startswith("/"):
        return f"{BASE_URL}{target}"
    return ""


def _status_display_line_from_status_obj(status_obj: dict[str, Any]) -> str:
    raw_text = status_obj.get("text") or status_obj.get("description")
    text = sanitize_xueqiu_text(raw_text) or ""
    text = str(text).strip()
    if not text:
        return ""

    user_obj = status_obj.get("user")
    author = _user_label_from_user_obj(user_obj) if isinstance(user_obj, dict) else ""
    if not author:
        uid = status_obj.get("user_id")
        author = str(uid).strip() if uid is not None else ""
    return f"{author}：{text}" if author else text


def _root_status_display_line_from_comment_record(record: dict[str, Any]) -> str:
    item_obj = _try_load_json_obj(record.get("raw_json"))
    if not item_obj:
        return ""
    status_obj = _extract_root_status_from_comment_item(item_obj)
    if not status_obj:
        return ""

    return _status_display_line_from_status_obj(status_obj)


def _context_json_for_status(record: dict[str, Any]) -> str:
    raw_obj = _try_load_json_obj(record.get("raw_json"))
    status_url = ""
    if raw_obj:
        status_url = _status_url_from_status_obj(raw_obj)
    ctx = {"status_id": record.get("status_id"), "status_url": status_url}
    if raw_obj:
        retweeted_obj = _extract_retweeted_status_from_status_obj(raw_obj)
        retweeted_status_id = raw_obj.get("retweet_status_id")
        if retweeted_obj:
            retweeted_status_id = retweeted_obj.get("id") or retweeted_status_id
            ctx["retweeted_status_user_id"] = retweeted_obj.get("user_id") or ""
            ctx["retweeted_status_url"] = _status_url_from_status_obj(retweeted_obj)
        if retweeted_status_id not in (None, "", 0, "0"):
            ctx["retweeted_status_id"] = str(retweeted_status_id)
    return json.dumps(ctx, ensure_ascii=False)


def _context_json_for_comment(record: dict[str, Any]) -> str:
    ctx = {
        "comment_id": record.get("comment_id"),
        "root_status_url": record.get("root_status_url") or "",
        "root_status_id": record.get("root_status_id")
        or record.get("root_in_reply_to_status_id")
        or "",
        "root_status_user_id": record.get("root_status_user_id") or "",
        "root_status_target": record.get("root_status_target") or "",
        "in_reply_to_comment_id": record.get("in_reply_to_comment_id") or "",
    }
    return json.dumps(ctx, ensure_ascii=False)


def _context_json_for_talk(
    *,
    root_status_id: str,
    comment_id: str,
    root_status_url: str,
    root_status_user_id: str = "",
) -> str:
    ctx = {
        "root_status_id": str(root_status_id),
        "comment_id": str(comment_id),
        "root_status_url": str(root_status_url or ""),
        "root_status_user_id": str(root_status_user_id or ""),
    }
    return json.dumps(ctx, ensure_ascii=False)


def _user_label_from_user_obj(user_obj: dict[str, Any]) -> str:
    for key in ("screen_name", "screenName", "name", "nickname"):
        val = user_obj.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    uid = user_obj.get("id") or user_obj.get("user_id") or user_obj.get("uid")
    if uid is not None:
        return str(uid)
    return ""


def _author_label_from_raw_json(raw_json: Any, fallback_user_id: str) -> str:
    obj = _try_load_json_obj(raw_json)
    if obj:
        user_obj = obj.get("user")
        if isinstance(user_obj, dict):
            label = _user_label_from_user_obj(user_obj)
            if label:
                return label
        uid = obj.get("user_id") or obj.get("uid")
        if uid is not None:
            return str(uid)
    return str(fallback_user_id or "").strip()


def _username_from_record(record: dict[str, Any], fallback_user_id: str) -> str:
    label = _author_label_from_raw_json(record.get("raw_json"), fallback_user_id)
    if label and label != str(fallback_user_id or "").strip():
        return label
    for key in ("screen_name", "screenName", "name", "nickname", "user_name"):
        value = record.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _username_from_talk_obj(obj: dict[str, Any], fallback_user_id: str) -> str:
    pages = obj.get("pages")
    if not isinstance(pages, list):
        return ""
    fallback_user_id_str = str(fallback_user_id or "").strip()
    for page in pages:
        if not isinstance(page, dict):
            continue
        comments = page.get("comments")
        if not isinstance(comments, list):
            continue
        for comment in comments:
            if not isinstance(comment, dict):
                continue
            comment_user_id = comment.get("user_id") or comment.get("uid")
            if fallback_user_id_str and str(comment_user_id or "").strip() not in (
                "",
                fallback_user_id_str,
            ):
                continue
            user_obj = comment.get("user")
            if isinstance(user_obj, dict):
                label = _user_label_from_user_obj(user_obj)
                if label and label != fallback_user_id_str:
                    return label
    return ""


def _username_from_payload(payload: dict[str, Any], fallback_user_id: str) -> str:
    record = payload.get("record")
    if isinstance(record, dict):
        label = _username_from_record(record, fallback_user_id)
        if label:
            return label

    status_payload = payload.get("status")
    if isinstance(status_payload, dict):
        status_record = status_payload.get("record")
        if isinstance(status_record, dict):
            label = _username_from_record(status_record, fallback_user_id)
            if label:
                return label

    comment_payload = payload.get("comment")
    if isinstance(comment_payload, dict):
        comment_record = comment_payload.get("record")
        if isinstance(comment_record, dict):
            label = _username_from_record(comment_record, fallback_user_id)
            if label:
                return label

    talk_obj = payload.get("clean")
    if isinstance(talk_obj, dict):
        label = _username_from_talk_obj(talk_obj, fallback_user_id)
        if label:
            return label

    raw_talk_obj = payload.get("raw")
    if isinstance(raw_talk_obj, dict):
        label = _username_from_talk_obj(raw_talk_obj, fallback_user_id)
        if label:
            return label

    talk_payload = payload.get("talk")
    if isinstance(talk_payload, dict):
        for key in ("clean", "raw"):
            talk_obj = talk_payload.get(key)
            if isinstance(talk_obj, dict):
                label = _username_from_talk_obj(talk_obj, fallback_user_id)
                if label:
                    return label
    return ""


def _status_display_text(record: dict[str, Any]) -> str:
    text = str(record.get("text") or "").strip()
    if not text:
        raw_obj = _try_load_json_obj(record.get("raw_json"))
        if raw_obj:
            text = _extract_text_from_obj(raw_obj)
    if not text:
        text = EMPTY_TEXT_PLACEHOLDER
    author = _author_label_from_raw_json(
        record.get("raw_json"), str(record.get("user_id") or "")
    )
    body_line = f"{author}：{text}" if author else text

    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if not raw_obj:
        return body_line
    retweeted_obj = _extract_retweeted_status_from_status_obj(raw_obj)
    if not retweeted_obj:
        return body_line
    retweeted_line = _status_display_line_from_status_obj(retweeted_obj)
    if not retweeted_line:
        return body_line
    return f"{retweeted_line}{TALK_TEXT_SEPARATOR}{body_line}"


def _comment_root_url(record: dict[str, Any]) -> str:
    url = record.get("root_status_url")
    if url:
        return str(url).strip()
    target = record.get("root_status_target")
    if isinstance(target, str) and target.startswith("/"):
        return f"{BASE_URL}{target}"
    root_status_user_id = record.get("root_status_user_id")
    root_status_id = record.get("root_in_reply_to_status_id") or record.get(
        "root_status_id"
    )
    if root_status_user_id not in (None, "", 0, "0") and root_status_id not in (
        None,
        "",
        0,
        "0",
    ):
        return f"{BASE_URL}/{str(root_status_user_id).strip()}/{str(root_status_id).strip()}"
    return ""


def _comment_display_text(record: dict[str, Any]) -> str:
    root_line = _root_status_display_line_from_comment_record(record)
    if not root_line:
        root_line = "原博文：(缺失)"

    body_line = _comment_body_display_line(record)
    return f"{root_line}{TALK_TEXT_SEPARATOR}{body_line}"


def _comment_body_display_line(record: dict[str, Any]) -> str:
    text = str(record.get("text") or "").strip()
    if not text:
        raw_obj = _try_load_json_obj(record.get("raw_json"))
        if raw_obj:
            text = _extract_text_from_obj(raw_obj)
    if not text:
        text = EMPTY_TEXT_PLACEHOLDER

    author = _author_label_from_raw_json(
        record.get("raw_json"), str(record.get("user_id") or "")
    )
    return f"{author}：{text}" if author else text


def _talk_chain_text_from_clean_obj(clean_obj: dict[str, Any]) -> str:
    """
    Build a readable talk chain string from the 'clean' talks snapshot.

    This is for display only, not for data correctness.
    """

    parts: list[str] = []
    pages = clean_obj.get("pages")
    if not isinstance(pages, list):
        return ""
    for page in pages:
        if not isinstance(page, dict):
            continue
        comments = page.get("comments")
        if not isinstance(comments, list):
            continue
        for c in comments:
            if not isinstance(c, dict):
                continue
            author = ""
            user_obj = c.get("user")
            if isinstance(user_obj, dict):
                author = _user_label_from_user_obj(user_obj)
            if not author:
                uid = c.get("user_id") or c.get("uid")
                author = str(uid).strip() if uid is not None else ""
            t = c.get("text")
            if t is None:
                continue
            s = str(t).strip()
            if not s:
                continue
            parts.append(f"{author}：{s}" if author else s)
    out = TALK_TEXT_SEPARATOR.join(parts)
    if len(out) > DEFAULT_TALK_TEXT_MAX_CHARS:
        out = out[:DEFAULT_TALK_TEXT_MAX_CHARS] + "…"
    return out


def _split_display_text(text: Any) -> list[str]:
    if text is None:
        return []
    parts = [str(part).strip() for part in str(text).split(TALK_TEXT_SEPARATOR)]
    return [part for part in parts if part]


def _join_display_lines(lines: Iterable[str]) -> str:
    parts = [str(line).strip() for line in lines if str(line).strip()]
    if not parts:
        return EMPTY_TEXT_PLACEHOLDER
    return TALK_TEXT_SEPARATOR.join(parts)


def _replace_first_display_line(text: Any, new_first_line: str) -> str:
    parts = _split_display_text(text)
    new_line = str(new_first_line or "").strip()
    if not new_line:
        return _join_display_lines(parts)
    if not parts:
        return _join_display_lines([new_line])
    parts[0] = new_line
    return _join_display_lines(parts)


def _merge_display_text(base_text: Any, chain_text: Any) -> str:
    base_lines = _split_display_text(base_text)
    chain_lines = _split_display_text(chain_text)
    if not base_lines:
        return _join_display_lines(chain_lines)
    if not chain_lines:
        return _join_display_lines(base_lines)

    overlap = 0
    max_overlap = min(len(base_lines), len(chain_lines))
    for size in range(max_overlap, 0, -1):
        if base_lines[-size:] == chain_lines[:size]:
            overlap = size
            break
    return _join_display_lines(base_lines + chain_lines[overlap:])


def _display_lines_contained(shorter: list[str], longer: list[str]) -> bool:
    if not shorter:
        return True
    if len(shorter) > len(longer):
        return False
    max_start = len(longer) - len(shorter)
    for start in range(max_start + 1):
        if longer[start : start + len(shorter)] == shorter:
            return True
    return False


def _looks_like_truncated_display_line(text: Any) -> bool:
    parts = _split_display_text(text)
    if not parts:
        return False
    first = parts[0].strip()
    if first.endswith("..."):
        return True
    # Chinese ellipsis punctuation is often "……" (two chars). Treat it as NOT truncated.
    return first.endswith("…") and (not first.endswith("……"))


def _load_json_text(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _comment_id_from_status_record(record: dict[str, Any]) -> str:
    value = record.get("comment_id")
    if value not in (None, "", 0, "0"):
        return str(value)
    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if not raw_obj:
        return ""
    raw_value = raw_obj.get("commentId") or raw_obj.get("comment_id")
    if raw_value in (None, "", 0, "0"):
        return ""
    return str(raw_value)


def _resolve_status_topic_id(
    status_id: str, statuses_by_id: dict[str, dict[str, Any]]
) -> str:
    current_id = str(status_id or "").strip()
    if not current_id:
        return ""

    seen: set[str] = set()
    topic_id = current_id
    while current_id and current_id not in seen:
        seen.add(current_id)
        source = statuses_by_id.get(current_id)
        if not source:
            break
        record = source.get("record") or {}
        next_id = record.get("retweeted_status_id")
        if next_id in (None, "", 0, "0"):
            break
        topic_id = str(next_id)
        current_id = str(next_id)
    return str(topic_id)


def _resolve_comment_topic_id(
    comment_record: dict[str, Any], statuses_by_id: dict[str, dict[str, Any]]
) -> str:
    root_status_id = comment_record.get("root_status_id") or comment_record.get(
        "root_in_reply_to_status_id"
    )
    if root_status_id in (None, "", 0, "0"):
        return ""
    root_status_id_str = str(root_status_id)
    if root_status_id_str in statuses_by_id:
        return _resolve_status_topic_id(root_status_id_str, statuses_by_id)
    return root_status_id_str


def _base_status_text_for_comment(
    comment_record: dict[str, Any], statuses_by_id: dict[str, dict[str, Any]]
) -> str:
    root_status_id = comment_record.get("root_status_id") or comment_record.get(
        "root_in_reply_to_status_id"
    )
    if root_status_id not in (None, "", 0, "0"):
        source = statuses_by_id.get(str(root_status_id))
        if source:
            text = str(source.get("text") or "").strip()
            if text:
                return text
    return _root_status_display_line_from_comment_record(comment_record)


def _retweet_status_id_from_status_record(record: dict[str, Any]) -> str:
    value = record.get("retweeted_status_id")
    if value not in (None, "", 0, "0"):
        return str(value)
    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if not raw_obj:
        return ""
    raw_value = raw_obj.get("retweet_status_id")
    if raw_value in (None, "", 0, "0"):
        return ""
    return str(raw_value)


def _status_url_from_record(record: dict[str, Any]) -> str:
    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if raw_obj:
        url = _status_url_from_status_obj(raw_obj)
        if url:
            return url
    value = record.get("status_url")
    return str(value or "").strip()


def _retweet_status_url_from_status_record(record: dict[str, Any]) -> str:
    value = record.get("retweeted_status_url")
    if value:
        return str(value).strip()

    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if not raw_obj:
        return ""
    retweeted_obj = _extract_retweeted_status_from_status_obj(raw_obj)
    if not retweeted_obj:
        return ""
    return _status_url_from_status_obj(retweeted_obj)


def _retweet_status_user_id_from_status_record(record: dict[str, Any]) -> str:
    value = record.get("retweeted_status_user_id")
    if value not in (None, "", 0, "0"):
        return str(value)

    raw_obj = _try_load_json_obj(record.get("raw_json"))
    if not raw_obj:
        return ""
    retweeted_obj = _extract_retweeted_status_from_status_obj(raw_obj)
    if not retweeted_obj:
        return ""
    user_id = retweeted_obj.get("user_id")
    if user_id in (None, "", 0, "0"):
        return ""
    return str(user_id)


def _enrich_status_text_with_full_original(
    *,
    status_text: Any,
    status_record: dict[str, Any],
    resolve_status_line: Optional[Callable[[str, str, str, str], Optional[str]]],
) -> str:
    out = str(status_text or "").strip()
    if not out:
        return out
    if resolve_status_line is None:
        return out
    if not _looks_like_truncated_display_line(out):
        return out

    retweeted_status_id = _retweet_status_id_from_status_record(status_record)
    if not retweeted_status_id:
        return out

    status_user_id = _retweet_status_user_id_from_status_record(status_record)
    if not status_user_id:
        status_user_id = str(status_record.get("user_id") or "").strip()

    full_line = resolve_status_line(
        str(retweeted_status_id),
        _status_url_from_record(status_record),
        _retweet_status_url_from_status_record(status_record),
        status_user_id,
    )
    if not full_line:
        return out
    return _replace_first_display_line(out, full_line)


def _build_entry_context(*, entry_type: str, payload: dict[str, Any]) -> str:
    return json.dumps({"entry_type": str(entry_type), **payload}, ensure_ascii=False)


def collapse_user_records_to_entries(
    *,
    db: "SqliteDb",
    user_id: str,
    resolve_status_line: Optional[Callable[[str, str, str, str], Optional[str]]] = None,
) -> int:
    final_entries = _build_user_entries(
        db=db,
        user_id=user_id,
        source_table_name=MERGED_TABLE_NAME,
        resolve_status_line=resolve_status_line,
    )
    if not final_entries:
        return 0

    db.conn.execute(
        f"DELETE FROM {MERGED_TABLE_NAME} WHERE user_id = ? AND merge_key LIKE ?",
        (str(user_id), _merge_key_like(f"{KIND_ENTRY}:")),
    )
    db.conn.executemany(
        f"""
        INSERT INTO {MERGED_TABLE_NAME}(
          merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(item.get("merge_key") or ""),
                str(user_id),
                str(item.get("username") or ""),
                item.get("created_at_bj"),
                item.get("fetched_at_bj") or _beijing_iso_now(),
                str(item.get("text") or EMPTY_TEXT_PLACEHOLDER),
                str(item.get("context_json") or CONTEXT_JSON_DEFAULT),
                str(item.get("payload_json") or "{}"),
            )
            for item in final_entries
            if item.get("merge_key")
        ],
    )
    db.conn.commit()
    return len(final_entries)


def rebuild_user_entries_from_raw_records(
    *,
    db: "SqliteDb",
    user_id: str,
    resolve_status_line: Optional[Callable[[str, str, str, str], Optional[str]]] = None,
) -> int:
    """
    Rebuild `entry:*` rows in merged_records from raw_records.

    Incremental-friendly behavior:
    - Read sources from raw_records
    - Delete only existing `entry:*` rows in merged_records for the user
    """

    final_entries = _build_user_entries(
        db=db,
        user_id=user_id,
        source_table_name=RAW_TABLE_NAME,
        resolve_status_line=resolve_status_line,
    )
    if not final_entries:
        return 0

    db.conn.execute(
        f"DELETE FROM {MERGED_TABLE_NAME} WHERE user_id = ? AND merge_key LIKE ?",
        (
            str(user_id),
            _merge_key_like(f"{KIND_ENTRY}:"),
        ),
    )
    db.conn.executemany(
        f"""
        INSERT INTO {MERGED_TABLE_NAME}(
          merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                str(item.get("merge_key") or ""),
                str(user_id),
                str(item.get("username") or ""),
                item.get("created_at_bj"),
                item.get("fetched_at_bj") or _beijing_iso_now(),
                str(item.get("text") or EMPTY_TEXT_PLACEHOLDER),
                str(item.get("context_json") or CONTEXT_JSON_DEFAULT),
                str(item.get("payload_json") or "{}"),
            )
            for item in final_entries
            if item.get("merge_key")
        ],
    )
    db.conn.commit()
    return len(final_entries)


def _build_user_entries(
    *,
    db: "SqliteDb",
    user_id: str,
    source_table_name: str,
    resolve_status_line: Optional[Callable[[str, str, str, str], Optional[str]]] = None,
) -> list[dict[str, Any]]:
    rows = list(
        db.conn.execute(
            f"""
            SELECT merge_key, created_at_bj, fetched_at_bj, text, context_json, payload_json
            FROM {source_table_name}
            WHERE user_id = ?
              AND (
                merge_key LIKE ?
                OR merge_key LIKE ?
                OR merge_key LIKE ?
              )
            ORDER BY created_at_bj ASC, fetched_at_bj ASC
            """,
            (
                str(user_id),
                _merge_key_like(MERGE_KEY_STATUS_PREFIX),
                _merge_key_like(MERGE_KEY_COMMENT_PREFIX),
                _merge_key_like(MERGE_KEY_TALK_PREFIX),
            ),
        )
    )
    if not rows:
        return []

    statuses_by_id: dict[str, dict[str, Any]] = {}
    comments_by_id: dict[str, dict[str, Any]] = {}
    talks_by_comment_id: dict[str, dict[str, Any]] = {}

    for row in rows:
        payload = _load_json_text(row["payload_json"])
        merge_key = str(row["merge_key"] or "")
        source_kind = _merge_key_kind(merge_key)
        source = {
            "merge_key": merge_key,
            "source_type": source_kind,
            "created_at_bj": row["created_at_bj"],
            "fetched_at_bj": row["fetched_at_bj"],
            "text": str(row["text"] or "").strip(),
            "context": _load_json_text(row["context_json"]),
            "payload": payload,
            "record": payload.get("record")
            if isinstance(payload.get("record"), dict)
            else {},
        }
        if source["source_type"] == KIND_STATUS:
            status_id = source["record"].get("status_id")
            if status_id not in (None, ""):
                statuses_by_id[str(status_id)] = source
        elif source["source_type"] == KIND_COMMENT:
            comment_id = source["record"].get("comment_id")
            if comment_id not in (None, ""):
                comments_by_id[str(comment_id)] = source
        elif source["source_type"] == KIND_TALK:
            comment_id = payload.get("comment_id")
            if comment_id not in (None, ""):
                talks_by_comment_id[str(comment_id)] = source

    chain_candidates: list[dict[str, Any]] = []
    for comment_id, source in comments_by_id.items():
        record = source.get("record") or {}
        topic_status_id = _resolve_comment_topic_id(record, statuses_by_id)
        base_text = _base_status_text_for_comment(record, statuses_by_id)
        # Prefer the root status line embedded in the comment payload, because
        # it can include richer HTML (e.g. <img ...>) than timeline status text.
        root_line = _root_status_display_line_from_comment_record(record)
        if root_line:
            base_text_str = str(base_text or "").strip()
            root_line_str = str(root_line or "").strip()
            if ("<img" in root_line_str.lower()) or (
                len(root_line_str) > len(base_text_str)
            ):
                base_text = root_line_str
        source_status = statuses_by_id.get(
            str(
                record.get("root_status_id")
                or record.get("root_in_reply_to_status_id")
                or ""
            )
        )
        if source_status:
            base_text = _enrich_status_text_with_full_original(
                status_text=base_text,
                status_record=source_status.get("record") or {},
                resolve_status_line=resolve_status_line,
            )

        # Build chain text (comment + replies) without duplicating the root status line.
        talk_source = talks_by_comment_id.get(str(comment_id))
        chain_text = ""
        if talk_source:
            talk_payload = talk_source.get("payload") or {}
            clean_obj = talk_payload.get("clean")
            if isinstance(clean_obj, dict):
                chain_text = _talk_chain_text_from_clean_obj(clean_obj)
        if not chain_text:
            chain_text = _comment_body_display_line(record)

        merged_text = _merge_display_text(base_text, chain_text)
        lines = _split_display_text(merged_text)
        chain_candidates.append(
            {
                "merge_key": f"{MERGE_KEY_ENTRY_CHAIN_PREFIX}{topic_status_id or 'unknown'}:{comment_id}",
                "username": _username_from_payload(
                    source.get("payload") or {}, str(user_id)
                ),
                "created_at_bj": record.get("created_at_bj")
                or source.get("created_at_bj"),
                "fetched_at_bj": source.get("fetched_at_bj") or _beijing_iso_now(),
                "text": merged_text,
                "lines": lines,
                "topic_status_id": str(topic_status_id or ""),
                "comment_id": str(comment_id),
                "context_json": _build_entry_context(
                    entry_type="chain",
                    payload={
                        "comment_id": str(comment_id),
                        "topic_status_id": str(topic_status_id or ""),
                        "root_status_id": str(
                            record.get("root_status_id")
                            or record.get("root_in_reply_to_status_id")
                            or ""
                        ),
                        "root_status_url": _comment_root_url(record),
                        "root_status_user_id": str(
                            record.get("root_status_user_id") or ""
                        ),
                        "root_status_target": str(
                            record.get("root_status_target") or ""
                        ),
                    },
                ),
                "payload_json": json.dumps(
                    {
                        "entry_type": "chain",
                        "comment": source.get("payload") or {},
                        "talk": talk_source.get("payload") if talk_source else {},
                    },
                    ensure_ascii=False,
                ),
            }
        )

    kept_chains: list[dict[str, Any]] = []
    sorted_candidates = sorted(
        chain_candidates,
        key=lambda item: (
            len(item.get("lines") or []),
            len(str(item.get("text") or "")),
            str(item.get("created_at_bj") or ""),
        ),
        reverse=True,
    )
    for candidate in sorted_candidates:
        candidate_lines = candidate.get("lines") or []
        if any(
            candidate.get("topic_status_id") == kept.get("topic_status_id")
            and _display_lines_contained(candidate_lines, kept.get("lines") or [])
            for kept in kept_chains
        ):
            continue
        kept_chains.append(candidate)

    final_entries: list[dict[str, Any]] = list(kept_chains)
    for status_id, source in statuses_by_id.items():
        record = source.get("record") or {}
        comment_id = _comment_id_from_status_record(record)
        topic_status_id = _resolve_status_topic_id(str(status_id), statuses_by_id)
        # Only skip statuses that are proven to be the same comment-thread record.
        if comment_id and comment_id in comments_by_id:
            continue
        enriched_text = _enrich_status_text_with_full_original(
            status_text=source.get("text") or _status_display_text(record),
            status_record=record,
            resolve_status_line=resolve_status_line,
        )

        final_entries.append(
            {
                "merge_key": f"{MERGE_KEY_ENTRY_STATUS_PREFIX}{status_id}",
                "username": _username_from_payload(
                    source.get("payload") or {}, str(user_id)
                ),
                "created_at_bj": record.get("created_at_bj")
                or source.get("created_at_bj"),
                "fetched_at_bj": source.get("fetched_at_bj") or _beijing_iso_now(),
                "text": enriched_text,
                "lines": _split_display_text(enriched_text),
                "topic_status_id": str(topic_status_id or status_id),
                "context_json": _build_entry_context(
                    entry_type="status",
                    payload={
                        "status_id": str(status_id),
                        "topic_status_id": str(topic_status_id or status_id),
                        "comment_id": comment_id,
                        "status_url": _status_url_from_record(record),
                    },
                ),
                "payload_json": json.dumps(
                    {"entry_type": "status", "status": source.get("payload") or {}},
                    ensure_ascii=False,
                ),
            }
        )

    final_entries.sort(
        key=lambda item: (
            str(item.get("created_at_bj") or ""),
            str(item.get("merge_key") or ""),
        )
    )
    return final_entries


class SqliteDb:
    """
    A tiny SQLite wrapper for this project.

    Design goals:
    - Standard library only (no ORM)
    - Deterministic schema creation
    - Avoid JSON/JSONL/CSV file outputs by storing data as tables
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._conn: Optional[sqlite3.Connection] = None

    def __enter__(self) -> "SqliteDb":
        _ensure_parent_dir(self.path)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self.ensure_schema()
        return self

    def __exit__(self, _exc_type, exc, _tb) -> None:
        if self._conn is not None:
            try:
                self._conn.commit()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteDb 未打开（请用 with SqliteDb(...) as db）")
        return self._conn

    def ensure_schema(self) -> None:
        c = self.conn
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS merged_records (
              merge_key TEXT PRIMARY KEY,
              user_id TEXT NOT NULL,
              username TEXT NOT NULL DEFAULT '',
              created_at_bj TEXT,
              fetched_at_bj TEXT NOT NULL,
              text TEXT NOT NULL DEFAULT '',
              context_json TEXT NOT NULL DEFAULT '{}',
              payload_json TEXT NOT NULL
            )
            """
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_merged_records_user_created ON merged_records(user_id, created_at_bj)"
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_records (
              merge_key TEXT PRIMARY KEY,
              user_id TEXT NOT NULL,
              username TEXT NOT NULL DEFAULT '',
              created_at_bj TEXT,
              fetched_at_bj TEXT NOT NULL,
              text TEXT NOT NULL DEFAULT '',
              context_json TEXT NOT NULL DEFAULT '{}',
              payload_json TEXT NOT NULL
            )
            """
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_raw_records_user_created ON raw_records(user_id, created_at_bj)"
        )
        merged_columns = {
            str(row["name"])
            for row in c.execute(f"PRAGMA table_info({MERGED_TABLE_NAME})")
        }
        if USERNAME_COLUMN_NAME not in merged_columns:
            c.execute(
                f"ALTER TABLE {MERGED_TABLE_NAME} ADD COLUMN {USERNAME_COLUMN_NAME} TEXT NOT NULL DEFAULT ''"
            )
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TALKS_PROGRESS_TABLE_NAME} (
              user_id TEXT NOT NULL,
              since_bj_iso TEXT NOT NULL,
              comment_id TEXT NOT NULL DEFAULT '',
              root_status_id TEXT NOT NULL DEFAULT '',
              created_at_bj TEXT NOT NULL DEFAULT '',
              current_index INTEGER NOT NULL DEFAULT 0,
              total_count INTEGER NOT NULL DEFAULT 0,
              updated_at_bj TEXT NOT NULL,
              PRIMARY KEY (user_id, since_bj_iso)
            )
            """
        )
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CRAWL_PROGRESS_TABLE_NAME} (
              user_id TEXT NOT NULL,
              since_bj_iso TEXT NOT NULL,
              stage TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT '',
              cursor_text TEXT NOT NULL DEFAULT '',
              current_index INTEGER NOT NULL DEFAULT 0,
              total_count INTEGER NOT NULL DEFAULT 0,
              detail_json TEXT NOT NULL DEFAULT '{{}}',
              updated_at_bj TEXT NOT NULL,
              PRIMARY KEY (user_id, since_bj_iso, stage)
            )
            """
        )
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CRAWL_CHECKPOINTS_TABLE_NAME} (
              user_id TEXT PRIMARY KEY,
              checkpoint_bj_iso TEXT NOT NULL,
              updated_at_bj TEXT NOT NULL,
              detail_json TEXT NOT NULL DEFAULT '{{}}'
            )
            """
        )
        c.commit()


@dataclass(frozen=True)
class SqliteCrawlCheckpointStore:
    db: SqliteDb

    def get(self, *, user_id: str) -> Optional[dict[str, Any]]:
        row = self.db.conn.execute(
            f"""
            SELECT checkpoint_bj_iso, updated_at_bj, detail_json
            FROM {CRAWL_CHECKPOINTS_TABLE_NAME}
            WHERE user_id = ?
            """,
            (str(user_id),),
        ).fetchone()
        if not row:
            return None
        checkpoint = str(row["checkpoint_bj_iso"] or "").strip()
        if not checkpoint:
            return None
        detail = _try_load_json_obj(row["detail_json"]) or {}
        return {
            "user_id": str(user_id),
            "checkpoint_bj_iso": checkpoint,
            "updated_at_bj": str(row["updated_at_bj"] or ""),
            "detail": detail,
        }

    def upsert(
        self,
        *,
        user_id: str,
        checkpoint_bj_iso: str,
        detail: Optional[dict[str, Any]] = None,
    ) -> None:
        self.db.conn.execute(
            f"""
            INSERT INTO {CRAWL_CHECKPOINTS_TABLE_NAME}(
              user_id, checkpoint_bj_iso, updated_at_bj, detail_json
            ) VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              checkpoint_bj_iso = excluded.checkpoint_bj_iso,
              updated_at_bj = excluded.updated_at_bj,
              detail_json = excluded.detail_json
            """,
            (
                str(user_id),
                str(checkpoint_bj_iso),
                _beijing_iso_now(),
                json.dumps(detail or {}, ensure_ascii=False),
            ),
        )
        self.db.conn.commit()


@dataclass(frozen=True)
class SqliteMergedStatusesStore:
    db: SqliteDb
    user_id: str
    table_name: str = MERGED_TABLE_NAME
    id_field: str = "status_id"

    def append_many(self, records: Iterable[dict[str, Any]]) -> int:
        before = int(self.db.conn.total_changes)
        self.db.conn.executemany(
            f"""
            INSERT OR IGNORE INTO {self.table_name}(
              merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f"{MERGE_KEY_STATUS_PREFIX}{str(r.get('status_id'))}",
                    str(r.get("user_id") or self.user_id),
                    _username_from_record(r, str(r.get("user_id") or self.user_id)),
                    r.get("created_at_bj"),
                    r.get("fetched_at_bj") or _beijing_iso_now(),
                    _status_display_text(r),
                    _context_json_for_status(r),
                    json.dumps({"record": r}, ensure_ascii=False),
                )
                for r in records
                if r.get("status_id")
            ],
        )
        self.db.conn.commit()
        return int(self.db.conn.total_changes) - before


@dataclass(frozen=True)
class SqliteMergedCommentsStore:
    db: SqliteDb
    user_id: str
    table_name: str = MERGED_TABLE_NAME
    id_field: str = "comment_id"

    def append_many(self, records: Iterable[dict[str, Any]]) -> int:
        before = int(self.db.conn.total_changes)
        self.db.conn.executemany(
            f"""
            INSERT OR IGNORE INTO {self.table_name}(
              merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f"{MERGE_KEY_COMMENT_PREFIX}{str(r.get('comment_id'))}",
                    str(r.get("user_id") or self.user_id),
                    _username_from_record(r, str(r.get("user_id") or self.user_id)),
                    r.get("created_at_bj"),
                    r.get("fetched_at_bj") or _beijing_iso_now(),
                    _comment_display_text(r),
                    _context_json_for_comment(r),
                    json.dumps({"record": r}, ensure_ascii=False),
                )
                for r in records
                if r.get("comment_id")
            ],
        )
        self.db.conn.commit()
        return int(self.db.conn.total_changes) - before

    def iter_comment_refs_since(self, *, since_bj_iso: str) -> Iterable[dict[str, Any]]:
        cur = self.db.conn.execute(
            f"""
            SELECT payload_json
            FROM {self.table_name}
            WHERE user_id = ? AND merge_key LIKE ? AND created_at_bj >= ?
            ORDER BY created_at_bj DESC
            """,
            (
                str(self.user_id),
                _merge_key_like(MERGE_KEY_COMMENT_PREFIX),
                str(since_bj_iso),
            ),
        )
        for row in cur:
            try:
                payload = json.loads(str(row["payload_json"]))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            rec = payload.get("record")
            if not isinstance(rec, dict):
                continue
            yield {
                "comment_id": rec.get("comment_id"),
                "root_in_reply_to_status_id": rec.get("root_in_reply_to_status_id"),
                "root_status_id": rec.get("root_status_id"),
                "created_at_bj": rec.get("created_at_bj"),
            }


@dataclass(frozen=True)
class SqliteMergedTalksStore:
    db: SqliteDb
    user_id: str
    table_name: str = MERGED_TABLE_NAME

    def get_existing_obj(
        self, *, root_status_id: str, comment_id: str
    ) -> Optional[dict[str, Any]]:
        merge_key = f"{MERGE_KEY_TALK_PREFIX}{str(root_status_id)}:{str(comment_id)}"
        row = self.db.conn.execute(
            f"SELECT payload_json FROM {self.table_name} WHERE merge_key = ? AND user_id = ?",
            (str(merge_key), str(self.user_id)),
        ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(str(row["payload_json"]))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        raw = payload.get("raw")
        return raw if isinstance(raw, dict) else None

    def get_meta(
        self, *, root_status_id: str, comment_id: str
    ) -> Optional[dict[str, Any]]:
        merge_key = f"{MERGE_KEY_TALK_PREFIX}{str(root_status_id)}:{str(comment_id)}"
        row = self.db.conn.execute(
            f"SELECT payload_json FROM {self.table_name} WHERE merge_key = ? AND user_id = ?",
            (str(merge_key), str(self.user_id)),
        ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(str(row["payload_json"]))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        meta = payload.get("meta")
        return meta if isinstance(meta, dict) else None

    @staticmethod
    def _extract_meta(
        obj: dict[str, Any],
    ) -> tuple[Optional[int], Optional[int], Optional[int]]:
        try:
            fetched_pages = obj.get("fetched_pages")
            if fetched_pages is None and isinstance(obj.get("pages"), list):
                fetched_pages = len(obj.get("pages") or [])
            fetched_pages_i = int(fetched_pages) if fetched_pages is not None else None
        except Exception:
            fetched_pages_i = None
        try:
            max_page_value = obj.get("max_page")
            max_page_i = int(max_page_value) if max_page_value is not None else None
        except Exception:
            max_page_i = None
        try:
            truncated_i = 1 if bool(obj.get("truncated")) else 0
        except Exception:
            truncated_i = 1
        return fetched_pages_i, max_page_i, truncated_i

    def upsert_obj(
        self, *, root_status_id: str, comment_id: str, user_id: str, obj: dict[str, Any]
    ) -> None:
        # Store everything in payload_json. Keep a cleaned copy for readability.
        clean_obj: dict[str, Any]
        try:
            clean_obj = json.loads(json.dumps(obj, ensure_ascii=False))
        except Exception:
            clean_obj = dict(obj)

        try:
            pages = clean_obj.get("pages") if isinstance(clean_obj, dict) else None
            if isinstance(pages, list):
                from .text_sanitize import sanitize_xueqiu_text

                for page in pages:
                    if not isinstance(page, dict):
                        continue
                    comments = page.get("comments")
                    if not isinstance(comments, list):
                        continue
                    for c in comments:
                        if not isinstance(c, dict):
                            continue
                        if "text" in c:
                            c["text"] = sanitize_xueqiu_text(c.get("text"))
        except Exception:
            pass

        fetched_pages_i, max_page_i, truncated_i = self._extract_meta(obj)
        payload = {
            "root_status_id": str(root_status_id),
            "comment_id": str(comment_id),
            "meta": {
                "fetched_pages": fetched_pages_i,
                "max_page": max_page_i,
                "truncated": truncated_i,
            },
            "raw": obj,
            "clean": clean_obj,
        }
        merge_key = f"{MERGE_KEY_TALK_PREFIX}{str(root_status_id)}:{str(comment_id)}"
        comment_merge_key = f"{MERGE_KEY_COMMENT_PREFIX}{str(comment_id)}"
        chain = _talk_chain_text_from_clean_obj(clean_obj)

        root_line = ""
        root_status_url = ""
        root_status_user_id = ""
        try:
            row = self.db.conn.execute(
                f"SELECT payload_json FROM {self.table_name} WHERE merge_key = ? AND user_id = ?",
                (str(comment_merge_key), str(user_id)),
            ).fetchone()
            if row:
                payload2 = json.loads(str(row["payload_json"]))
                if isinstance(payload2, dict):
                    rec = payload2.get("record")
                    if isinstance(rec, dict):
                        root_line = _root_status_display_line_from_comment_record(rec)
                        root_status_url = _comment_root_url(rec) or root_status_url
                        root_status_user_id = str(
                            rec.get("root_status_user_id") or ""
                        ).strip()
        except Exception:
            pass

        if root_line and chain:
            text = f"{root_line}{TALK_TEXT_SEPARATOR}{chain}"
        else:
            text = root_line or chain
        if not text:
            text = "原博文：(缺失)"

        context_json = _context_json_for_talk(
            root_status_id=str(root_status_id),
            comment_id=str(comment_id),
            root_status_url=str(root_status_url),
            root_status_user_id=str(root_status_user_id),
        )
        self.db.conn.execute(
            f"""
            INSERT INTO {self.table_name}(
              merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(merge_key) DO UPDATE SET
              username = excluded.username,
              fetched_at_bj = excluded.fetched_at_bj,
              text = excluded.text,
              context_json = excluded.context_json,
              payload_json = excluded.payload_json
            """,
            (
                str(merge_key),
                str(user_id),
                _username_from_talk_obj(clean_obj, str(user_id)),
                None,
                _beijing_iso_now(),
                str(text or ""),
                str(context_json or CONTEXT_JSON_DEFAULT),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        # Make comment rows readable too: once talks is available, overwrite the corresponding comment.text
        # with the same chain text (so comments do not appear "without context").
        try:
            self.db.conn.execute(
                f"UPDATE {self.table_name} SET text = ? WHERE merge_key = ? AND user_id = ?",
                (str(text or ""), str(comment_merge_key), str(user_id)),
            )
        except Exception:
            pass
        self.db.conn.commit()


@dataclass(frozen=True)
class SqliteTalksProgressStore:
    db: SqliteDb
    user_id: str

    def get(self, *, since_bj_iso: str) -> Optional[dict[str, Any]]:
        row = self.db.conn.execute(
            f"""
            SELECT comment_id, root_status_id, created_at_bj, current_index, total_count, updated_at_bj
            FROM {TALKS_PROGRESS_TABLE_NAME}
            WHERE user_id = ? AND since_bj_iso = ?
            """,
            (str(self.user_id), str(since_bj_iso)),
        ).fetchone()
        if not row:
            return None
        return {
            "comment_id": str(row["comment_id"] or ""),
            "root_status_id": str(row["root_status_id"] or ""),
            "created_at_bj": str(row["created_at_bj"] or ""),
            "current_index": int(row["current_index"] or 0),
            "total_count": int(row["total_count"] or 0),
            "updated_at_bj": str(row["updated_at_bj"] or ""),
        }

    def upsert(
        self,
        *,
        since_bj_iso: str,
        comment_id: str,
        root_status_id: str,
        created_at_bj: str,
        current_index: int,
        total_count: int,
    ) -> None:
        self.db.conn.execute(
            f"""
            INSERT INTO {TALKS_PROGRESS_TABLE_NAME}(
              user_id, since_bj_iso, comment_id, root_status_id, created_at_bj,
              current_index, total_count, updated_at_bj
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, since_bj_iso) DO UPDATE SET
              comment_id = excluded.comment_id,
              root_status_id = excluded.root_status_id,
              created_at_bj = excluded.created_at_bj,
              current_index = excluded.current_index,
              total_count = excluded.total_count,
              updated_at_bj = excluded.updated_at_bj
            """,
            (
                str(self.user_id),
                str(since_bj_iso),
                str(comment_id or ""),
                str(root_status_id or ""),
                str(created_at_bj or ""),
                int(current_index),
                int(total_count),
                _beijing_iso_now(),
            ),
        )
        self.db.conn.commit()

    def clear(self, *, since_bj_iso: str) -> None:
        self.db.conn.execute(
            f"DELETE FROM {TALKS_PROGRESS_TABLE_NAME} WHERE user_id = ? AND since_bj_iso = ?",
            (str(self.user_id), str(since_bj_iso)),
        )
        self.db.conn.commit()


@dataclass(frozen=True)
class SqliteCrawlProgressStore:
    db: SqliteDb
    user_id: str

    def get(self, *, since_bj_iso: str, stage: str) -> Optional[dict[str, Any]]:
        row = self.db.conn.execute(
            f"""
            SELECT status, cursor_text, current_index, total_count, detail_json, updated_at_bj
            FROM {CRAWL_PROGRESS_TABLE_NAME}
            WHERE user_id = ? AND since_bj_iso = ? AND stage = ?
            """,
            (str(self.user_id), str(since_bj_iso), str(stage)),
        ).fetchone()
        if not row:
            return None
        detail = _try_load_json_obj(row["detail_json"]) or {}
        return {
            "status": str(row["status"] or ""),
            "cursor_text": str(row["cursor_text"] or ""),
            "current_index": int(row["current_index"] or 0),
            "total_count": int(row["total_count"] or 0),
            "detail": detail,
            "updated_at_bj": str(row["updated_at_bj"] or ""),
        }

    def upsert(
        self,
        *,
        since_bj_iso: str,
        stage: str,
        status: str,
        cursor_text: str = "",
        current_index: int = 0,
        total_count: int = 0,
        detail: Optional[dict[str, Any]] = None,
    ) -> None:
        self.db.conn.execute(
            f"""
            INSERT INTO {CRAWL_PROGRESS_TABLE_NAME}(
              user_id, since_bj_iso, stage, status, cursor_text,
              current_index, total_count, detail_json, updated_at_bj
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, since_bj_iso, stage) DO UPDATE SET
              status = excluded.status,
              cursor_text = excluded.cursor_text,
              current_index = excluded.current_index,
              total_count = excluded.total_count,
              detail_json = excluded.detail_json,
              updated_at_bj = excluded.updated_at_bj
            """,
            (
                str(self.user_id),
                str(since_bj_iso),
                str(stage),
                str(status or ""),
                str(cursor_text or ""),
                int(current_index),
                int(total_count),
                json.dumps(detail or {}, ensure_ascii=False),
                _beijing_iso_now(),
            ),
        )
        self.db.conn.commit()

    def mark_completed(
        self,
        *,
        since_bj_iso: str,
        stage: str,
        cursor_text: str = "",
        current_index: int = 0,
        total_count: int = 0,
        detail: Optional[dict[str, Any]] = None,
    ) -> None:
        self.upsert(
            since_bj_iso=since_bj_iso,
            stage=stage,
            status="completed",
            cursor_text=cursor_text,
            current_index=current_index,
            total_count=total_count,
            detail=detail,
        )

    def is_completed(self, *, since_bj_iso: str, stage: str) -> bool:
        row = self.get(since_bj_iso=since_bj_iso, stage=stage)
        return bool(row and row.get("status") == "completed")

    def clear(self, *, since_bj_iso: str, stage: str) -> None:
        self.db.conn.execute(
            f"DELETE FROM {CRAWL_PROGRESS_TABLE_NAME} WHERE user_id = ? AND since_bj_iso = ? AND stage = ?",
            (str(self.user_id), str(since_bj_iso), str(stage)),
        )
        self.db.conn.commit()
