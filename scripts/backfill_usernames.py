#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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

from xueqiu_crawler.constants import DEFAULT_BATCH_DB_BASENAME, DEFAULT_OUTPUT_DIR
from xueqiu_crawler.storage import MERGED_TABLE_NAME, SqliteDb

USERNAME_COLUMN_NAME = "username"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_usernames",
        description="Backfill merged_records.username from payload_json.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME,
        help="SQLite path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only update the first N matched rows. 0 means no limit.",
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


def _pick_name_from_user_obj(user_obj: Any) -> str:
    if not isinstance(user_obj, dict):
        return ""
    for key in ("screen_name", "screenName", "name", "nickname", "user_name"):
        value = user_obj.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _pick_name_from_record(record: Any, fallback_user_id: str) -> str:
    if not isinstance(record, dict):
        return ""
    raw_obj = _load_json_obj(record.get("raw_json"))
    label = _pick_name_from_user_obj(raw_obj.get("user"))
    if label and label != fallback_user_id:
        return label
    for key in ("screen_name", "screenName", "name", "nickname", "user_name"):
        value = record.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _pick_name_from_talk_obj(obj: Any, fallback_user_id: str) -> str:
    if not isinstance(obj, dict):
        return ""
    pages = obj.get("pages")
    if not isinstance(pages, list):
        return ""
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
            if fallback_user_id and str(comment_user_id or "").strip() not in (
                "",
                fallback_user_id,
            ):
                continue
            label = _pick_name_from_user_obj(comment.get("user"))
            if label and label != fallback_user_id:
                return label
    return ""


def _extract_username(payload_json: Any, fallback_user_id: str) -> str:
    payload = _load_json_obj(payload_json)
    record = payload.get("record")
    label = _pick_name_from_record(record, fallback_user_id)
    if label:
        return label

    status_payload = payload.get("status")
    if isinstance(status_payload, dict):
        label = _pick_name_from_record(status_payload.get("record"), fallback_user_id)
        if label:
            return label

    comment_payload = payload.get("comment")
    if isinstance(comment_payload, dict):
        label = _pick_name_from_record(comment_payload.get("record"), fallback_user_id)
        if label:
            return label

    for key in ("clean", "raw"):
        label = _pick_name_from_talk_obj(payload.get(key), fallback_user_id)
        if label:
            return label

    talk_payload = payload.get("talk")
    if isinstance(talk_payload, dict):
        for key in ("clean", "raw"):
            label = _pick_name_from_talk_obj(talk_payload.get(key), fallback_user_id)
            if label:
                return label
    return ""


def main() -> int:
    args = _parse_args()
    with SqliteDb(args.db) as db:
        rows = list(
            db.conn.execute(
                f"""
                SELECT merge_key, user_id, payload_json
                FROM {MERGED_TABLE_NAME}
                WHERE {USERNAME_COLUMN_NAME} = ''
                ORDER BY fetched_at_bj ASC, merge_key ASC
                """
            )
        )
        if args.limit > 0:
            rows = rows[: args.limit]

        updates: list[tuple[str, str]] = []
        for row in rows:
            username = _extract_username(row["payload_json"], str(row["user_id"] or ""))
            if not username:
                continue
            updates.append((username, str(row["merge_key"])))

        if updates:
            db.conn.executemany(
                f"""
                UPDATE {MERGED_TABLE_NAME}
                SET {USERNAME_COLUMN_NAME} = ?
                WHERE merge_key = ?
                """,
                updates,
            )
            db.conn.commit()

    print(
        json.dumps(
            {
                "scanned_rows": len(rows),
                "updated_rows": len(updates),
                "db": str(args.db),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
