from __future__ import annotations

import datetime as dt
import sqlite3
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .constants import BEIJING_TIMEZONE_NAME
from .storage import MERGED_TABLE_NAME, RAW_TABLE_NAME


# Keep it simple: minimal metadata for rate limiting.
MAINTENANCE_META_TABLE_NAME = "maintenance_meta"
META_KEY_LAST_CLEANUP_BJ_ISO = "last_cleanup_bj_iso"

DEFAULT_RETENTION_DAYS = 10
DEFAULT_MIN_INTERVAL_SEC = 24 * 60 * 60

BEIJING_TIMEZONE = ZoneInfo(BEIJING_TIMEZONE_NAME)


def _beijing_now() -> dt.datetime:
    return dt.datetime.now(tz=BEIJING_TIMEZONE).replace(microsecond=0)


def _safe_parse_iso_datetime(value: Any) -> Optional[dt.datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).strip())
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BEIJING_TIMEZONE)
    return parsed


def _ensure_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MAINTENANCE_META_TABLE_NAME} (
          k TEXT PRIMARY KEY,
          v TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _get_meta_value(conn: sqlite3.Connection, *, key: str) -> str:
    row = conn.execute(
        f"SELECT v FROM {MAINTENANCE_META_TABLE_NAME} WHERE k = ?",
        (str(key),),
    ).fetchone()
    if not row:
        return ""
    value = row[0]
    return str(value or "").strip()


def _set_meta_value(conn: sqlite3.Connection, *, key: str, value: str) -> None:
    conn.execute(
        f"INSERT OR REPLACE INTO {MAINTENANCE_META_TABLE_NAME}(k, v) VALUES(?, ?)",
        (str(key), str(value)),
    )
    conn.commit()


def _should_run_by_interval(
    *, now: dt.datetime, last_run: Optional[dt.datetime], min_interval_sec: int
) -> bool:
    if last_run is None:
        return True
    try:
        elapsed = (now - last_run).total_seconds()
    except Exception:
        return True
    return elapsed >= float(min_interval_sec)


def maybe_cleanup_old_data(
    conn: sqlite3.Connection,
    *,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    min_interval_sec: int = DEFAULT_MIN_INTERVAL_SEC,
) -> dict[str, Any]:
    """
    Best-effort SQLite maintenance:
    - Delete old rows from `raw_records` and `merged_records`
    - Truncate WAL (checkpoint)
    - Run PRAGMA optimize

    This function is intentionally conservative:
    - rate limited by a timestamp stored in DB
    - no VACUUM (can require extra free disk space)
    """

    keep_days = max(1, int(retention_days))
    interval_sec = max(0, int(min_interval_sec))
    now = _beijing_now()

    _ensure_meta_table(conn)
    last_run_raw = _get_meta_value(conn, key=META_KEY_LAST_CLEANUP_BJ_ISO)
    last_run = _safe_parse_iso_datetime(last_run_raw)
    if not _should_run_by_interval(
        now=now, last_run=last_run, min_interval_sec=interval_sec
    ):
        return {
            "ran": False,
            "reason": "too_recent",
            "last_run_bj_iso": last_run_raw,
        }

    cutoff = now - dt.timedelta(days=int(keep_days))
    cutoff_iso = cutoff.isoformat()

    before_total = int(conn.total_changes)
    conn.execute(
        f"""
        DELETE FROM {RAW_TABLE_NAME}
        WHERE COALESCE(NULLIF(created_at_bj, ''), NULLIF(fetched_at_bj, '')) < ?
        """,
        (str(cutoff_iso),),
    )
    raw_deleted = int(conn.total_changes) - before_total

    before_total = int(conn.total_changes)
    conn.execute(
        f"""
        DELETE FROM {MERGED_TABLE_NAME}
        WHERE COALESCE(NULLIF(created_at_bj, ''), NULLIF(fetched_at_bj, '')) < ?
        """,
        (str(cutoff_iso),),
    )
    merged_deleted = int(conn.total_changes) - before_total

    conn.commit()

    checkpoint_result = None
    try:
        checkpoint_result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    except Exception:
        checkpoint_result = None

    try:
        conn.execute("PRAGMA optimize")
    except Exception:
        pass

    _set_meta_value(conn, key=META_KEY_LAST_CLEANUP_BJ_ISO, value=now.isoformat())
    return {
        "ran": True,
        "retention_days": int(keep_days),
        "cutoff_bj_iso": str(cutoff_iso),
        "deleted_raw": int(raw_deleted),
        "deleted_merged": int(merged_deleted),
        "wal_checkpoint": checkpoint_result,
    }
