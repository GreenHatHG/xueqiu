#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = PROJECT_ROOT / "src"
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

from xueqiu_crawler.constants import (
    BASE_URL,
    DEFAULT_BATCH_DB_BASENAME,
    DEFAULT_OUTPUT_DIR,
)

MERGED_TABLE_NAME = "merged_records"
POSTS_TABLE_NAME = "posts"
ASSERTIONS_TABLE_NAME = "assertions"
STATUS_TABLE_PREFIX = "status:"
BARE_STATUS_PAGE_RE = re.compile(rf"^{re.escape(BASE_URL)}/S/\d+$")
LEGACY_STATUS_FALLBACK_RE = re.compile(rf"^{re.escape(BASE_URL)}/status/\d+$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_posts_assertions_meta",
        description=(
            "Backfill missing `posts.created_at`, `posts.url`, and `assertions.created_at` "
            "using local data in merged_records."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME,
        help="SQLite path.",
    )
    return parser.parse_args()


def _load_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _min_iso(existing: str, candidate: str) -> str:
    existing_text = str(existing or "").strip()
    candidate_text = str(candidate or "").strip()
    if not existing_text:
        return candidate_text
    if not candidate_text:
        return existing_text
    return candidate_text if candidate_text < existing_text else existing_text


def _ensure_tables_exist(conn: sqlite3.Connection) -> None:
    required = {MERGED_TABLE_NAME, POSTS_TABLE_NAME, ASSERTIONS_TABLE_NAME}
    existing = {
        str(row[0]).strip()
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    missing = sorted(required - existing)
    if missing:
        raise SystemExit(f"DB missing tables: {missing}")


def _status_url_from_status_obj(status_obj: dict[str, Any]) -> str:
    user_id = status_obj.get("user_id")
    status_id = status_obj.get("id")
    if user_id not in (None, "", 0, "0") and status_id not in (None, "", 0, "0"):
        return f"{BASE_URL}/{str(user_id).strip()}/{str(status_id).strip()}"
    target = status_obj.get("target")
    if isinstance(target, str) and target.startswith("/"):
        return f"{BASE_URL}{target}"
    return ""


def _status_url_from_status_record(record: dict[str, Any]) -> str:
    direct_url = str(record.get("status_url") or "").strip()
    if direct_url:
        return direct_url
    raw_obj = _load_json_obj(record.get("raw_json"))
    if raw_obj:
        return _status_url_from_status_obj(raw_obj)
    user_id = record.get("user_id")
    status_id = record.get("status_id")
    if user_id not in (None, "", 0, "0") and status_id not in (None, "", 0, "0"):
        return f"{BASE_URL}/{str(user_id).strip()}/{str(status_id).strip()}"
    return ""


def _is_legacy_status_url(url: str) -> bool:
    text = str(url or "").strip()
    if not text:
        return False
    return bool(
        BARE_STATUS_PAGE_RE.fullmatch(text) or LEGACY_STATUS_FALLBACK_RE.fullmatch(text)
    )


def _normalize_chain_root_url(context: dict[str, Any], payload: dict[str, Any]) -> str:
    root_status_url = str(context.get("root_status_url") or "").strip()
    if root_status_url and not _is_legacy_status_url(root_status_url):
        return root_status_url

    comment_payload = payload.get("comment")
    record: dict[str, Any] = {}
    if isinstance(comment_payload, dict):
        record_obj = comment_payload.get("record")
        if isinstance(record_obj, dict):
            record = record_obj

    record_root_url = str(record.get("root_status_url") or "").strip()
    if record_root_url and not _is_legacy_status_url(record_root_url):
        return record_root_url

    target = str(
        context.get("root_status_target") or record.get("root_status_target") or ""
    ).strip()
    if target.startswith("/"):
        return f"{BASE_URL}{target}"

    root_status_user_id = str(
        context.get("root_status_user_id") or record.get("root_status_user_id") or ""
    ).strip()
    root_status_id = str(
        context.get("root_status_id")
        or record.get("root_status_id")
        or record.get("root_in_reply_to_status_id")
        or ""
    ).strip()
    if root_status_user_id and root_status_id:
        return f"{BASE_URL}/{root_status_user_id}/{root_status_id}"
    return ""


def _collect_status_url_mappings(conn: sqlite3.Connection) -> dict[str, str]:
    status_id_to_url: dict[str, str] = {}
    cur = conn.execute(
        f"""
        SELECT merge_key, context_json, payload_json
        FROM {MERGED_TABLE_NAME}
        WHERE merge_key LIKE '{STATUS_TABLE_PREFIX}%'
        ORDER BY merge_key ASC
        """
    )
    for merge_key, context_json, payload_json in cur:
        merge_key_text = str(merge_key or "").strip()
        if not merge_key_text.startswith(STATUS_TABLE_PREFIX):
            continue
        status_id = merge_key_text[len(STATUS_TABLE_PREFIX) :].strip()
        if not status_id:
            continue
        context = _load_json_obj(context_json)
        status_url = str(context.get("status_url") or "").strip()
        if not status_url:
            payload = _load_json_obj(payload_json)
            record: dict[str, Any] = {}
            record_obj = payload.get("record")
            if isinstance(record_obj, dict):
                record = record_obj
            if record:
                status_url = _status_url_from_status_record(record)
        if status_url:
            status_id_to_url.setdefault(status_id, status_url)
    return status_id_to_url


def _collect_entry_mappings(
    conn: sqlite3.Connection,
) -> tuple[
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
    dict[str, str],
]:
    """
    Build local lookup tables from `merged_records` entries.

    - status_id_to_created_at: best-effort timestamp for status nodes
    - topic_id_to_created_at: best-effort timestamp for topic_post nodes
    - comment_id_to_root_url: thread URL for comment nodes
    - talk_reply_id_to_root_url: thread URL for talk_reply nodes
    """
    status_id_to_created_at: dict[str, str] = {}
    topic_id_to_created_at: dict[str, str] = {}
    topic_id_to_root_url: dict[str, str] = {}
    comment_id_to_root_url: dict[str, str] = {}
    talk_reply_id_to_root_url: dict[str, str] = {}

    cur = conn.execute(
        f"""
        SELECT merge_key, created_at_bj, context_json, payload_json
        FROM {MERGED_TABLE_NAME}
        WHERE merge_key LIKE 'entry:%'
        ORDER BY merge_key ASC
        """
    )
    for merge_key, created_at_bj, context_json, payload_json in cur:
        context = _load_json_obj(context_json)
        payload = _load_json_obj(payload_json)
        entry_type = str(context.get("entry_type") or "").strip()
        created_at_text = str(created_at_bj or "").strip()

        if entry_type == "status":
            status_id = str(context.get("status_id") or "").strip()
            topic_status_id = str(context.get("topic_status_id") or "").strip()
            if status_id:
                status_id_to_created_at[status_id] = _min_iso(
                    status_id_to_created_at.get(status_id, ""), created_at_text
                )
            if topic_status_id:
                topic_id_to_created_at[topic_status_id] = _min_iso(
                    topic_id_to_created_at.get(topic_status_id, ""), created_at_text
                )
            continue

        if entry_type != "chain":
            continue

        topic_status_id = str(context.get("topic_status_id") or "").strip()
        root_status_id = str(context.get("root_status_id") or "").strip()
        comment_id = str(context.get("comment_id") or "").strip()
        root_status_url = _normalize_chain_root_url(context, payload)

        if root_status_id:
            status_id_to_created_at[root_status_id] = _min_iso(
                status_id_to_created_at.get(root_status_id, ""), created_at_text
            )
        if topic_status_id:
            topic_id_to_created_at[topic_status_id] = _min_iso(
                topic_id_to_created_at.get(topic_status_id, ""), created_at_text
            )
            if root_status_url:
                topic_id_to_root_url.setdefault(topic_status_id, root_status_url)
        if comment_id and root_status_url:
            comment_id_to_root_url.setdefault(comment_id, root_status_url)

        if not root_status_url:
            continue

        comment_payload = payload.get("comment")
        if isinstance(comment_payload, dict):
            record = comment_payload.get("record")
            if isinstance(record, dict):
                raw_obj = _load_json_obj(record.get("raw_json"))
                reply_comment = raw_obj.get("reply_comment")
                if isinstance(reply_comment, dict):
                    reply_id = str(
                        reply_comment.get("id") or reply_comment.get("comment_id") or ""
                    ).strip()
                    if reply_id:
                        talk_reply_id_to_root_url.setdefault(reply_id, root_status_url)

        talk_payload = payload.get("talk")
        if not isinstance(talk_payload, dict):
            continue
        clean_obj = talk_payload.get("clean")
        if not isinstance(clean_obj, dict):
            continue
        pages = clean_obj.get("pages")
        if not isinstance(pages, list):
            continue
        for page in pages:
            if not isinstance(page, dict):
                continue
            comments = page.get("comments")
            if not isinstance(comments, list):
                continue
            for item in comments:
                if not isinstance(item, dict):
                    continue
                reply_id = str(item.get("id") or item.get("comment_id") or "").strip()
                if reply_id:
                    talk_reply_id_to_root_url.setdefault(reply_id, root_status_url)

    return (
        status_id_to_created_at,
        topic_id_to_created_at,
        topic_id_to_root_url,
        comment_id_to_root_url,
        talk_reply_id_to_root_url,
    )


def _backfill_posts_created_at(
    conn: sqlite3.Connection,
    *,
    status_id_to_created_at: dict[str, str],
    topic_id_to_created_at: dict[str, str],
) -> int:
    cur = conn.execute(
        f"""
        SELECT post_uid, platform_post_id
        FROM {POSTS_TABLE_NAME}
        WHERE created_at = ''
          AND (
            post_uid LIKE 'xueqiu:status:%'
            OR post_uid LIKE 'xueqiu:topic_post:%'
          )
        """
    )

    updates: list[tuple[str, str]] = []
    for post_uid, platform_post_id in cur:
        post_uid_text = str(post_uid or "").strip()
        id_text = str(platform_post_id or "").strip()
        if not post_uid_text or not id_text:
            continue
        if post_uid_text.startswith("xueqiu:status:"):
            created_at = status_id_to_created_at.get(id_text, "")
        else:
            created_at = topic_id_to_created_at.get(id_text, "")
        created_at = str(created_at or "").strip()
        if created_at:
            updates.append((created_at, post_uid_text))

    if updates:
        conn.executemany(
            f"UPDATE {POSTS_TABLE_NAME} SET created_at = ? WHERE post_uid = ?",
            updates,
        )
    return len(updates)


def _backfill_posts_url(
    conn: sqlite3.Connection,
    *,
    status_id_to_url: dict[str, str],
    topic_id_to_root_url: dict[str, str],
    comment_id_to_root_url: dict[str, str],
    talk_reply_id_to_root_url: dict[str, str],
) -> int:
    cur = conn.execute(
        f"""
        SELECT post_uid, platform_post_id
        FROM {POSTS_TABLE_NAME}
        WHERE (
            url = ''
            OR url LIKE '{BASE_URL}/status/%'
            OR (
              url LIKE '{BASE_URL}/S/%'
              AND url NOT LIKE '{BASE_URL}/S/%/%'
            )
          )
          AND (
            post_uid LIKE 'xueqiu:status:%'
            OR post_uid LIKE 'xueqiu:topic_post:%'
            OR post_uid LIKE 'xueqiu:comment:%'
            OR post_uid LIKE 'xueqiu:talk_reply:%'
          )
        """
    )

    updates: list[tuple[str, str]] = []
    for post_uid, platform_post_id in cur:
        post_uid_text = str(post_uid or "").strip()
        id_text = str(platform_post_id or "").strip()
        if not post_uid_text or not id_text:
            continue
        if post_uid_text.startswith("xueqiu:status:"):
            url = status_id_to_url.get(id_text, "")
        elif post_uid_text.startswith("xueqiu:topic_post:"):
            url = topic_id_to_root_url.get(id_text, "")
        elif post_uid_text.startswith("xueqiu:comment:"):
            url = comment_id_to_root_url.get(id_text, "")
        else:
            url = talk_reply_id_to_root_url.get(
                id_text, ""
            ) or comment_id_to_root_url.get(id_text, "")
        url = str(url or "").strip()
        if url:
            updates.append((url, post_uid_text))

    if updates:
        conn.executemany(
            f"UPDATE {POSTS_TABLE_NAME} SET url = ? WHERE post_uid = ?",
            updates,
        )
    return len(updates)


def _backfill_assertions_created_at(conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        f"""
        UPDATE {ASSERTIONS_TABLE_NAME}
        SET created_at = (
          SELECT p.created_at
          FROM {POSTS_TABLE_NAME} p
          WHERE p.post_uid = {ASSERTIONS_TABLE_NAME}.post_uid
        )
        WHERE created_at = ''
          AND EXISTS (
            SELECT 1
            FROM {POSTS_TABLE_NAME} p
            WHERE p.post_uid = {ASSERTIONS_TABLE_NAME}.post_uid
              AND p.created_at != ''
          )
        """
    )
    return int(cursor.rowcount or 0)


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"sqlite file not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_tables_exist(conn)

        (
            status_id_to_created_at,
            topic_id_to_created_at,
            topic_id_to_root_url,
            comment_id_to_root_url,
            talk_reply_id_to_root_url,
        ) = _collect_entry_mappings(conn)
        status_id_to_url = _collect_status_url_mappings(conn)

        posts_created_at_updated = _backfill_posts_created_at(
            conn,
            status_id_to_created_at=status_id_to_created_at,
            topic_id_to_created_at=topic_id_to_created_at,
        )
        posts_url_updated = _backfill_posts_url(
            conn,
            status_id_to_url=status_id_to_url,
            topic_id_to_root_url=topic_id_to_root_url,
            comment_id_to_root_url=comment_id_to_root_url,
            talk_reply_id_to_root_url=talk_reply_id_to_root_url,
        )
        assertions_created_at_updated = _backfill_assertions_created_at(conn)

        conn.commit()
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "db": str(db_path),
                "posts_created_at_updated": posts_created_at_updated,
                "posts_url_updated": posts_url_updated,
                "assertions_created_at_updated": assertions_created_at_updated,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
