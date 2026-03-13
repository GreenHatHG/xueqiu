#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

if __package__ in (None, ""):
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = PROJECT_ROOT / "src"
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

from xueqiu_crawler.constants import DEFAULT_BATCH_DB_BASENAME, DEFAULT_OUTPUT_DIR
from xueqiu_crawler import storage as storage_lib
from xueqiu_crawler.storage import (
    MERGED_TABLE_NAME,
    RAW_TABLE_NAME,
    SqliteDb,
    collapse_user_records_to_entries,
)


@dataclass(frozen=True)
class RawRow:
    merge_key: str
    user_id: str
    username: str
    created_at_bj: Optional[str]
    fetched_at_bj: str
    text: str
    context_json: str
    payload_json: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_raw_records",
        description=(
            "Backfill raw_records from merged entry rows and perform full in-memory reconciliation."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME,
        help="SQLite path.",
    )
    parser.add_argument(
        "--no-reconcile",
        action="store_true",
        help="Skip reconciliation (only backfill raw_records).",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional JSON report output path (only written when mismatches exist).",
    )
    parser.add_argument(
        "--print-mismatches",
        type=int,
        default=30,
        help="Print at most N mismatches to stdout (default: 30).",
    )
    return parser.parse_args()


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return "{}"


def _entry_kind(payload: dict[str, Any]) -> str:
    kind = payload.get("entry_type")
    return str(kind).strip() if kind is not None else ""


def _parse_status_id_from_entry_merge_key(merge_key: str) -> str:
    prefix = storage_lib.MERGE_KEY_ENTRY_STATUS_PREFIX
    if not merge_key.startswith(prefix):
        return ""
    return merge_key[len(prefix) :].strip()


def _parse_comment_id_from_entry_merge_key(merge_key: str) -> str:
    prefix = storage_lib.MERGE_KEY_ENTRY_CHAIN_PREFIX
    if not merge_key.startswith(prefix):
        return ""
    parts = merge_key[len(prefix) :].split(":")
    if not parts:
        return ""
    return str(parts[-1]).strip()


def _parse_topic_status_id_from_chain_entry_merge_key(merge_key: str) -> str:
    prefix = storage_lib.MERGE_KEY_ENTRY_CHAIN_PREFIX
    if not merge_key.startswith(prefix):
        return ""
    parts = merge_key[len(prefix) :].split(":")
    if len(parts) < 2:
        return ""
    return str(parts[0]).strip()


@dataclass(frozen=True)
class SyntheticStatusHint:
    user_id: str
    username: str
    root_status_id: str
    topic_status_id: str
    comment_id: str
    fetched_at_bj: str
    base_text: str


def _build_synthetic_status_row(*, hint: SyntheticStatusHint) -> RawRow:
    record: dict[str, Any] = {
        "_synthetic": 1,
        "_synthetic_reason": "topic_status_id_resolution",
        "_synthetic_topic_status_id": hint.topic_status_id,
        "_synthetic_comment_id": hint.comment_id,
        "status_id": hint.root_status_id,
        "user_id": hint.user_id,
        # Make this status skipped as an entry row by linking it to an existing comment row.
        "comment_id": hint.comment_id,
        # Minimal retweet chain info for topic-id resolution.
        "retweeted_status_id": hint.topic_status_id,
        # Keep payload stable and small; raw_json is optional for correctness here.
        "raw_json": "{}",
        "fetched_at_bj": hint.fetched_at_bj,
    }
    payload = {"record": record}
    merge_key = f"{storage_lib.MERGE_KEY_STATUS_PREFIX}{hint.root_status_id}"
    return RawRow(
        merge_key=merge_key,
        user_id=hint.user_id,
        username=hint.username,
        created_at_bj=None,
        fetched_at_bj=hint.fetched_at_bj,
        text=hint.base_text or "",
        context_json=storage_lib._context_json_for_status(record),
        payload_json=_json_dumps(payload),
    )


def _recover_base_text_from_entry_text(*, entry_text: str, chain_text: str) -> str:
    merged_lines = storage_lib._split_display_text(entry_text)
    chain_lines = storage_lib._split_display_text(chain_text)
    if not merged_lines:
        return ""
    if not chain_lines:
        return entry_text

    max_overlap = min(len(chain_lines), len(merged_lines))
    # Prefer the smallest possible base prefix. When reconstructing from the final merged
    # display text, multiple (base, overlap) pairs can yield the same merged_lines.
    # A smaller base is less likely to accidentally duplicate chain content.
    for overlap in range(0, max_overlap + 1):
        suffix = chain_lines[overlap:]
        base_len = len(merged_lines) - len(suffix)
        if base_len < 0:
            continue
        if merged_lines[base_len:] != suffix:
            continue
        base_candidate = merged_lines[:base_len]
        if overlap > 0 and base_candidate[-overlap:] != chain_lines[:overlap]:
            continue
        return storage_lib.TALK_TEXT_SEPARATOR.join(base_candidate).strip()
    return ""


def _reconstruct_raw_rows_from_entry_row(row: Any) -> list[RawRow]:
    merge_key = str(row["merge_key"] or "").strip()
    user_id = str(row["user_id"] or "").strip()
    username = str(row["username"] or "").strip()
    created_at_bj = row["created_at_bj"]
    fetched_at_bj = str(row["fetched_at_bj"] or "").strip()
    entry_text = str(row["text"] or "").strip()

    payload = _load_json_dict(row["payload_json"])
    kind = _entry_kind(payload)
    if not kind:
        return []

    out: list[RawRow] = []
    if kind == "status":
        status_payload = payload.get("status")
        if not isinstance(status_payload, dict):
            return []
        record = status_payload.get("record")
        if not isinstance(record, dict):
            record = {}
        status_id = str(record.get("status_id") or "").strip()
        if not status_id:
            status_id = _parse_status_id_from_entry_merge_key(merge_key)
        if not status_id:
            return []
        raw_merge_key = f"{storage_lib.MERGE_KEY_STATUS_PREFIX}{status_id}"
        raw_username = username or storage_lib._username_from_payload(
            status_payload, user_id
        )
        raw_context_json = storage_lib._context_json_for_status(record)
        out.append(
            RawRow(
                merge_key=raw_merge_key,
                user_id=user_id,
                username=str(raw_username or "").strip(),
                created_at_bj=str(record.get("created_at_bj") or created_at_bj or "")
                or None,
                fetched_at_bj=fetched_at_bj,
                text=entry_text or storage_lib._status_display_text(record),
                context_json=raw_context_json,
                payload_json=_json_dumps(status_payload),
            )
        )
        return out

    if kind != "chain":
        return []

    comment_payload = payload.get("comment")
    if not isinstance(comment_payload, dict):
        return []
    comment_record = comment_payload.get("record")
    if not isinstance(comment_record, dict):
        comment_record = {}

    talk_payload = payload.get("talk")
    talk_payload_dict = talk_payload if isinstance(talk_payload, dict) else {}

    comment_id = str(comment_record.get("comment_id") or "").strip()
    if not comment_id:
        comment_id = _parse_comment_id_from_entry_merge_key(merge_key)
    if not comment_id:
        return []

    comment_merge_key = f"{storage_lib.MERGE_KEY_COMMENT_PREFIX}{comment_id}"
    comment_username = username or storage_lib._username_from_payload(
        comment_payload, user_id
    )
    comment_text = storage_lib._comment_display_text(comment_record)
    talk_text = ""

    if talk_payload_dict:
        root_status_id = str(
            talk_payload_dict.get("root_status_id")
            or comment_record.get("root_status_id")
            or comment_record.get("root_in_reply_to_status_id")
            or ""
        ).strip()
        if root_status_id:
            clean_obj = talk_payload_dict.get("clean")
            clean_obj_dict = clean_obj if isinstance(clean_obj, dict) else {}
            root_line = storage_lib._root_status_display_line_from_comment_record(
                comment_record
            )
            chain = storage_lib._talk_chain_text_from_clean_obj(clean_obj_dict)
            if root_line and chain:
                talk_text = f"{root_line}{storage_lib.TALK_TEXT_SEPARATOR}{chain}"
            else:
                talk_text = root_line or chain
            if not talk_text:
                talk_text = "原博文：(缺失)"

    # The crawler overwrites comment.text once talks is available.
    # To keep reconciliation deterministic, mimic that behavior here.
    if talk_text:
        comment_text = talk_text

    out.append(
        RawRow(
            merge_key=comment_merge_key,
            user_id=user_id,
            username=str(comment_username or "").strip(),
            created_at_bj=str(
                comment_record.get("created_at_bj") or created_at_bj or ""
            )
            or None,
            fetched_at_bj=fetched_at_bj,
            text=comment_text,
            context_json=storage_lib._context_json_for_comment(comment_record),
            payload_json=_json_dumps(comment_payload),
        )
    )

    if not talk_payload_dict:
        return out

    root_status_id = str(
        talk_payload_dict.get("root_status_id")
        or comment_record.get("root_status_id")
        or comment_record.get("root_in_reply_to_status_id")
        or ""
    ).strip()
    if not root_status_id:
        return out

    talk_merge_key = f"{storage_lib.MERGE_KEY_TALK_PREFIX}{root_status_id}:{comment_id}"
    clean_obj = talk_payload_dict.get("clean")
    clean_obj_dict = clean_obj if isinstance(clean_obj, dict) else {}
    talk_username = username or storage_lib._username_from_talk_obj(
        clean_obj_dict, user_id
    )

    if not talk_text:
        root_line = storage_lib._root_status_display_line_from_comment_record(
            comment_record
        )
        chain = storage_lib._talk_chain_text_from_clean_obj(clean_obj_dict)
        if root_line and chain:
            talk_text = f"{root_line}{storage_lib.TALK_TEXT_SEPARATOR}{chain}"
        else:
            talk_text = root_line or chain
        if not talk_text:
            talk_text = "原博文：(缺失)"

    root_status_url = storage_lib._comment_root_url(comment_record) or (
        f"{storage_lib.BASE_URL}/status/{root_status_id}"
    )
    talk_context_json = storage_lib._context_json_for_talk(
        root_status_id=root_status_id,
        comment_id=comment_id,
        root_status_url=root_status_url,
    )
    out.append(
        RawRow(
            merge_key=talk_merge_key,
            user_id=user_id,
            username=str(talk_username or "").strip(),
            created_at_bj=None,
            fetched_at_bj=fetched_at_bj,
            text=talk_text,
            context_json=talk_context_json,
            payload_json=_json_dumps(talk_payload_dict),
        )
    )
    return out


def _iter_entry_rows(db: SqliteDb) -> Iterable[Any]:
    cur = db.conn.execute(
        f"""
        SELECT merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        FROM {MERGED_TABLE_NAME}
        WHERE merge_key LIKE ?
        ORDER BY user_id ASC, created_at_bj ASC, fetched_at_bj ASC, merge_key ASC
        """,
        ("entry:%",),
    )
    yield from cur


def _insert_raw_rows(db: SqliteDb, rows: list[RawRow]) -> int:
    if not rows:
        return 0
    before = int(db.conn.total_changes)
    db.conn.executemany(
        f"""
        INSERT INTO {RAW_TABLE_NAME}(
          merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(merge_key) DO UPDATE SET
          user_id = excluded.user_id,
          username = excluded.username,
          created_at_bj = excluded.created_at_bj,
          fetched_at_bj = excluded.fetched_at_bj,
          text = excluded.text,
          context_json = excluded.context_json,
          payload_json = excluded.payload_json
        WHERE
          user_id != excluded.user_id
          OR username != excluded.username
          OR ifnull(created_at_bj, '') != ifnull(excluded.created_at_bj, '')
          OR fetched_at_bj != excluded.fetched_at_bj
          OR text != excluded.text
          OR context_json != excluded.context_json
          OR payload_json != excluded.payload_json
        """,
        [
            (
                r.merge_key,
                r.user_id,
                r.username,
                r.created_at_bj,
                r.fetched_at_bj,
                r.text,
                r.context_json or storage_lib.CONTEXT_JSON_DEFAULT,
                r.payload_json,
            )
            for r in rows
            if r.merge_key and r.user_id and r.fetched_at_bj
        ],
    )
    db.conn.commit()
    return int(db.conn.total_changes) - before


def _insert_scratch_raw_rows(scratch_db: SqliteDb, rows: list[RawRow]) -> None:
    if not rows:
        return
    scratch_db.conn.executemany(
        f"""
        INSERT OR IGNORE INTO {MERGED_TABLE_NAME}(
          merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.merge_key,
                r.user_id,
                r.username,
                r.created_at_bj,
                r.fetched_at_bj,
                r.text,
                r.context_json or storage_lib.CONTEXT_JSON_DEFAULT,
                r.payload_json,
            )
            for r in rows
            if r.merge_key and r.user_id and r.fetched_at_bj
        ],
    )
    scratch_db.conn.commit()


def _load_entry_map(db: SqliteDb) -> dict[str, dict[str, Any]]:
    cur = db.conn.execute(
        f"""
        SELECT merge_key, user_id, username, created_at_bj, fetched_at_bj, text, context_json, payload_json
        FROM {MERGED_TABLE_NAME}
        WHERE merge_key LIKE ?
        """,
        ("entry:%",),
    )
    out: dict[str, dict[str, Any]] = {}
    for row in cur:
        key = str(row["merge_key"] or "").strip()
        if not key:
            continue
        out[key] = {
            "merge_key": key,
            "user_id": str(row["user_id"] or "").strip(),
            "username": str(row["username"] or "").strip(),
            "created_at_bj": str(row["created_at_bj"] or "").strip(),
            "fetched_at_bj": str(row["fetched_at_bj"] or "").strip(),
            "text": str(row["text"] or "").strip(),
            "context_json": str(row["context_json"] or "").strip(),
            "payload_json": str(row["payload_json"] or "").strip(),
        }
    return out


def _json_semantic_equal(left: str, right: str) -> bool:
    if left == right:
        return True
    try:
        left_obj = json.loads(left)
        right_obj = json.loads(right)
    except Exception:
        return False
    return left_obj == right_obj


def _compare_entry_maps(
    *,
    expected: dict[str, dict[str, Any]],
    actual: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    expected_keys = set(expected.keys())
    actual_keys = set(actual.keys())
    missing_keys = sorted(expected_keys - actual_keys)
    extra_keys = sorted(actual_keys - expected_keys)

    mismatches: list[dict[str, Any]] = []
    for key in sorted(expected_keys & actual_keys):
        exp = expected[key]
        act = actual[key]
        diffs: dict[str, Any] = {}
        for field in (
            "user_id",
            "username",
            "created_at_bj",
            "fetched_at_bj",
            "text",
        ):
            if exp.get(field) != act.get(field):
                diffs[field] = {"expected": exp.get(field), "actual": act.get(field)}
        for field in ("context_json", "payload_json"):
            if not _json_semantic_equal(
                str(exp.get(field) or ""), str(act.get(field) or "")
            ):
                diffs[field] = {
                    "expected": exp.get(field),
                    "actual": act.get(field),
                }
        if diffs:
            mismatches.append({"merge_key": key, "diffs": diffs})

    return {
        "expected_count": len(expected),
        "actual_count": len(actual),
        "missing_keys_count": len(missing_keys),
        "extra_keys_count": len(extra_keys),
        "mismatches_count": len(mismatches),
        "missing_keys": missing_keys,
        "extra_keys": extra_keys,
        "mismatches": mismatches,
    }


def main() -> int:
    args = _parse_args()

    reconstructed: list[RawRow] = []
    entry_rows_scanned = 0
    reconstructed_statuses = 0
    reconstructed_comments = 0
    reconstructed_talks = 0
    synthetic_status_hints: dict[tuple[str, str], SyntheticStatusHint] = {}
    status_ids_by_user: dict[str, set[str]] = {}
    user_ids: set[str] = set()

    with SqliteDb(args.db) as db:
        for entry_row in _iter_entry_rows(db):
            entry_rows_scanned += 1
            raw_rows = _reconstruct_raw_rows_from_entry_row(entry_row)
            if not raw_rows:
                continue
            row_user_id = str(entry_row["user_id"] or "").strip()
            row_username = str(entry_row["username"] or "").strip()
            user_ids.add(row_user_id)
            status_ids_by_user.setdefault(row_user_id, set())
            for r in raw_rows:
                if r.merge_key.startswith(storage_lib.MERGE_KEY_STATUS_PREFIX):
                    reconstructed_statuses += 1
                    status_id = r.merge_key[
                        len(storage_lib.MERGE_KEY_STATUS_PREFIX) :
                    ].strip()
                    if status_id:
                        status_ids_by_user[row_user_id].add(status_id)
                elif r.merge_key.startswith(storage_lib.MERGE_KEY_COMMENT_PREFIX):
                    reconstructed_comments += 1
                elif r.merge_key.startswith(storage_lib.MERGE_KEY_TALK_PREFIX):
                    reconstructed_talks += 1
            reconstructed.extend(raw_rows)

            # Capture topic-id resolution hints for chain entries.
            merge_key = str(entry_row["merge_key"] or "").strip()
            payload = _load_json_dict(entry_row["payload_json"])
            if _entry_kind(payload) != "chain":
                continue
            topic_status_id = _parse_topic_status_id_from_chain_entry_merge_key(
                merge_key
            )
            if not topic_status_id or topic_status_id == "unknown":
                continue
            comment_payload = payload.get("comment")
            if not isinstance(comment_payload, dict):
                continue
            comment_record = comment_payload.get("record")
            if not isinstance(comment_record, dict):
                continue
            comment_id = str(comment_record.get("comment_id") or "").strip()
            if not comment_id:
                continue
            root_status_id = str(
                comment_record.get("root_status_id")
                or comment_record.get("root_in_reply_to_status_id")
                or ""
            ).strip()
            if not root_status_id or root_status_id == topic_status_id:
                continue
            root_line = storage_lib._root_status_display_line_from_comment_record(
                comment_record
            )
            fetched_at_bj = str(entry_row["fetched_at_bj"] or "").strip()
            if not fetched_at_bj:
                continue
            chain_text = ""
            comment_merge_key = f"{storage_lib.MERGE_KEY_COMMENT_PREFIX}{comment_id}"
            for item in raw_rows:
                if item.merge_key == comment_merge_key and item.user_id == row_user_id:
                    chain_text = str(item.text or "").strip()
                    break
            entry_text = str(entry_row["text"] or "").strip()
            base_text = _recover_base_text_from_entry_text(
                entry_text=entry_text,
                chain_text=chain_text,
            )
            if not base_text:
                base_text = root_line
            key = (row_user_id, root_status_id)
            existing = synthetic_status_hints.get(key)
            hint = SyntheticStatusHint(
                user_id=row_user_id,
                username=row_username,
                root_status_id=root_status_id,
                topic_status_id=topic_status_id,
                comment_id=comment_id,
                fetched_at_bj=fetched_at_bj,
                base_text=base_text,
            )
            if existing and existing.topic_status_id != hint.topic_status_id:
                # Keep the first one; report via reconciliation if it matters.
                continue
            synthetic_status_hints[key] = hint

        # Add synthetic status rows for root_status_id values that are not present as status entries.
        synthetic_added = 0
        for (uid, root_status_id), hint in synthetic_status_hints.items():
            if root_status_id in status_ids_by_user.get(uid, set()):
                continue
            reconstructed.append(_build_synthetic_status_row(hint=hint))
            reconstructed_statuses += 1
            synthetic_added += 1

        changed = _insert_raw_rows(db, reconstructed)

    report: dict[str, Any] = {
        "db": str(args.db),
        "entry_rows_scanned": entry_rows_scanned,
        "reconstructed": {
            "total": len(reconstructed),
            "statuses": reconstructed_statuses,
            "comments": reconstructed_comments,
            "talks": reconstructed_talks,
            "users": len([u for u in user_ids if u]),
            "synthetic_statuses": int(synthetic_added),
        },
        "raw_records_changed": changed,
    }

    if args.no_reconcile:
        print(json.dumps({**report, "reconcile": None}, ensure_ascii=False))
        return 0

    # Reconcile in a scratch in-memory SQLite DB.
    with SqliteDb(Path(":memory:")) as scratch_db, SqliteDb(args.db) as prod_db:
        _insert_scratch_raw_rows(scratch_db, reconstructed)
        for uid in sorted(u for u in user_ids if u):
            collapse_user_records_to_entries(db=scratch_db, user_id=str(uid))

        expected_entries = _load_entry_map(prod_db)
        actual_entries = _load_entry_map(scratch_db)
        reconcile = _compare_entry_maps(
            expected=expected_entries, actual=actual_entries
        )

    report["reconcile"] = {
        "expected_entries": reconcile["expected_count"],
        "actual_entries": reconcile["actual_count"],
        "missing_keys_count": reconcile["missing_keys_count"],
        "extra_keys_count": reconcile["extra_keys_count"],
        "mismatches_count": reconcile["mismatches_count"],
    }

    mismatches_count = int(reconcile.get("mismatches_count") or 0)
    missing_keys_count = int(reconcile.get("missing_keys_count") or 0)
    extra_keys_count = int(reconcile.get("extra_keys_count") or 0)
    if mismatches_count or missing_keys_count or extra_keys_count:
        printable = int(args.print_mismatches or 0)
        if printable > 0:
            for item in (reconcile.get("mismatches") or [])[:printable]:
                print(json.dumps(item, ensure_ascii=False))
        if args.report:
            try:
                args.report.write_text(
                    json.dumps(reconcile, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                report["report_path"] = str(args.report)
            except Exception as exc:
                report["report_write_error"] = str(exc)

    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
