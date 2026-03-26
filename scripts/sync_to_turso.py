#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import shlex
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional


SYNC_META_TABLE = "sync_meta"

TABLE_SYNC_CONFIG: dict[str, dict[str, str]] = {
    "raw_records": {"cursor_col": "fetched_at_bj"},
    "merged_records": {"cursor_col": "fetched_at_bj"},
    "crawl_progress": {"cursor_col": "updated_at_bj"},
    "talks_progress": {"cursor_col": "updated_at_bj"},
    "crawl_checkpoints": {"cursor_col": "updated_at_bj"},
    "posts": {
        "cursor_expr": "COALESCE(NULLIF(processed_at, ''), NULLIF(created_at, ''))"
    },
    "assertions": {"cursor_col": "created_at"},
    "topic_package_run_progress": {"cursor_col": "updated_at"},
}


STANDARD_TABLES = {"posts", "assertions"}

STANDARD_POSTS_COLUMNS = [
    "post_uid",
    "platform",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
    "final_status",
    "invest_score",
    "processed_at",
    "model",
    "prompt_version",
    "archived_at",
]

STANDARD_ASSERTIONS_COLUMNS = [
    "post_uid",
    "idx",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes_json",
    "stock_names_json",
    "industries_json",
    "commodities_json",
    "indices_json",
]

POSTS_STANDARD_SELECT_COLUMNS = [
    "post_uid",
    "platform",
    "platform_post_id",
    "author",
    "created_at",
    "url",
    "raw_text",
    "CASE WHEN status IN ('relevant','irrelevant') THEN status ELSE 'irrelevant' END AS final_status",
    "invest_score",
    "processed_at",
    "model",
    "prompt_version",
    "COALESCE(NULLIF(processed_at, ''), NULLIF(created_at, ''), '') AS archived_at",
]

ASSERTIONS_STANDARD_SELECT_COLUMNS = [
    "post_uid",
    "idx",
    "topic_key",
    "action",
    "action_strength",
    "summary",
    "evidence",
    "confidence",
    "stock_codes_json",
    "stock_names_json",
    "industries_json",
    "commodities_json",
    "indices_json",
]

STANDARD_POSTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
  post_uid TEXT PRIMARY KEY,
  platform TEXT,
  platform_post_id TEXT,
  author TEXT,
  created_at TEXT,
  url TEXT,
  raw_text TEXT,
  final_status TEXT,
  invest_score REAL,
  processed_at TEXT,
  model TEXT,
  prompt_version TEXT,
  archived_at TEXT
)
"""

STANDARD_ASSERTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS assertions (
  post_uid TEXT,
  idx INTEGER,
  topic_key TEXT,
  action TEXT,
  action_strength INTEGER,
  summary TEXT,
  evidence TEXT,
  confidence REAL,
  stock_codes_json TEXT,
  stock_names_json TEXT,
  industries_json TEXT,
  commodities_json TEXT,
  indices_json TEXT,
  UNIQUE(post_uid, idx)
)
"""

STANDARD_SCHEMA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform_post_id)",
    "CREATE INDEX IF NOT EXISTS idx_assertions_topic_key ON assertions(topic_key)",
    "CREATE INDEX IF NOT EXISTS idx_assertions_action ON assertions(action)",
]

DEFAULT_TABLE_ORDER = [
    "raw_records",
    "merged_records",
    "crawl_progress",
    "talks_progress",
    "crawl_checkpoints",
    "posts",
    "assertions",
    "topic_package_run_progress",
]


def _now_iso() -> str:
    return dt.datetime.now().replace(microsecond=0).isoformat()


def _ensure_sync_meta(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SYNC_META_TABLE} (
          table_name TEXT PRIMARY KEY,
          cursor_value TEXT NOT NULL DEFAULT '',
          updated_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.commit()


def _get_meta(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        f"SELECT cursor_value FROM {SYNC_META_TABLE} WHERE table_name = ?",
        (table,),
    ).fetchone()
    if not row:
        return ""
    return str(row[0] or "")


def _set_meta(conn: sqlite3.Connection, table: str, value: str) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {SYNC_META_TABLE}(table_name, cursor_value, updated_at)
        VALUES(?, ?, ?)
        """,
        (table, value, _now_iso()),
    )
    conn.commit()


def _sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'" + bytes(value).hex() + "'"
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


def _build_insert_sql(
    table: str, columns: list[str], rows: Iterable[sqlite3.Row]
) -> str:
    rows_list = list(rows)
    if not rows_list:
        return ""
    col_sql = ", ".join(f'"{col}"' for col in columns)
    values_sql: list[str] = []
    for row in rows_list:
        values_sql.append(
            "(" + ", ".join(_sql_literal(row[col]) for col in columns) + ")"
        )
    return (
        f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES\n  '
        + ",\n  ".join(values_sql)
        + ";\n"
    )


def _inject_if_not_exists(sql: str) -> str:
    stripped = sql.lstrip()
    prefix = sql[: len(sql) - len(stripped)]
    if stripped.startswith("CREATE TABLE "):
        return prefix + stripped.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
    if stripped.startswith("CREATE UNIQUE INDEX "):
        return prefix + stripped.replace(
            "CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1
        )
    if stripped.startswith("CREATE INDEX "):
        return prefix + stripped.replace(
            "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1
        )
    return sql


def _standard_schema_sql() -> list[str]:
    return [
        STANDARD_POSTS_SCHEMA.strip(),
        STANDARD_ASSERTIONS_SCHEMA.strip(),
        *STANDARD_SCHEMA_INDEXES,
    ]


def _is_standard_table_related(row_type: str, name: str, sql: str) -> bool:
    if name in STANDARD_TABLES:
        return True
    if row_type != "index":
        return False
    low = sql.lower()
    for table in STANDARD_TABLES:
        if f" on {table}" in low or f" on \"{table}\"" in low:
            return True
    return False


def _standard_table_columns(table: str) -> Optional[list[str]]:
    if table == "posts":
        return list(STANDARD_POSTS_COLUMNS)
    if table == "assertions":
        return list(STANDARD_ASSERTIONS_COLUMNS)
    return None


def _standard_table_select_sql(table: str, cursor_expr: Optional[str]) -> Optional[str]:
    if table == "posts":
        columns = list(POSTS_STANDARD_SELECT_COLUMNS)
    elif table == "assertions":
        columns = list(ASSERTIONS_STANDARD_SELECT_COLUMNS)
    else:
        return None

    if cursor_expr:
        columns.append(f"{cursor_expr} AS __cursor_value")
    sql = f'SELECT {", ".join(columns)} FROM "{table}"'
    if cursor_expr:
        sql += f" WHERE {cursor_expr} > ? ORDER BY {cursor_expr}"
    return sql


def _load_schema_sql(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY CASE WHEN type = 'table' THEN 0 ELSE 1 END, name
        """
    ).fetchall()
    statements: list[str] = []
    for row in rows:
        row_type = str(row[0] or "").strip().lower()
        name = str(row[1] or "").strip()
        sql = str(row[2] or "").strip()
        if not sql:
            continue
        if _is_standard_table_related(row_type, name, sql):
            continue
        statements.append(_inject_if_not_exists(sql))

    statements.extend(_standard_schema_sql())
    return statements


def _get_user_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0] != SYNC_META_TABLE]


def _get_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    standard = _standard_table_columns(table)
    if standard is not None:
        return standard
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(row[1]) for row in rows]


def _get_cursor_expr(table: str) -> Optional[str]:
    cfg = TABLE_SYNC_CONFIG.get(table)
    if not cfg:
        return None
    return cfg.get("cursor_expr") or cfg.get("cursor_col")


def _count_rows(
    conn: sqlite3.Connection, table: str, *, cursor_expr: Optional[str], last_value: str
) -> int:
    if cursor_expr:
        row = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE {cursor_expr} > ?',
            (last_value,),
        ).fetchone()
    else:
        row = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    return int(row[0] or 0)


def _build_incremental_query(table: str, cursor_expr: str) -> str:
    return (
        f'SELECT *, {cursor_expr} AS __cursor_value FROM "{table}" '
        f"WHERE {cursor_expr} > ? ORDER BY {cursor_expr}"
    )


def _build_max_cursor_query(table: str, cursor_expr: str) -> str:
    return f'SELECT MAX({cursor_expr}) FROM "{table}"'

def _build_select_sql(table: str, cursor_expr: Optional[str], *, incremental: bool) -> str:
    standard_sql = _standard_table_select_sql(table, cursor_expr if incremental else None)
    if standard_sql:
        return standard_sql
    if incremental and cursor_expr:
        return _build_incremental_query(table, cursor_expr)
    return f'SELECT * FROM "{table}"'



def _run_turso(
    sql_text: str,
    *,
    turso_db: str,
    dry_run: bool,
    shell_cmd: Optional[list[str]] = None,
) -> None:
    if not sql_text.strip():
        return
    if dry_run:
        print(sql_text)
        return
    cmd = shell_cmd or ["turso", "db", "shell", turso_db]
    subprocess.run(cmd, input=sql_text, text=True, check=True)


def _sync_table_full(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    select_sql: str,
    batch_size: int,
    turso_db: str,
    dry_run: bool,
    progress: bool,
    shell_cmd: Optional[list[str]],
) -> None:
    total = _count_rows(conn, table, cursor_expr=None, last_value="")
    processed = 0
    cursor = conn.execute(select_sql)
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        sql = "BEGIN;\n" + _build_insert_sql(table, columns, rows) + "COMMIT;\n"
        _run_turso(sql, turso_db=turso_db, dry_run=dry_run, shell_cmd=shell_cmd)
        processed += len(rows)
        if progress:
            print(f"[{table}] {processed}/{total} rows")


def _sync_table_incremental(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    cursor_expr: str,
    select_sql: str,
    batch_size: int,
    turso_db: str,
    dry_run: bool,
    progress: bool,
    shell_cmd: Optional[list[str]],
) -> None:
    last_value = _get_meta(conn, table)
    total = _count_rows(conn, table, cursor_expr=cursor_expr, last_value=last_value)
    processed = 0
    cursor = conn.execute(select_sql, (last_value,))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        sql = "BEGIN;\n" + _build_insert_sql(table, columns, rows) + "COMMIT;\n"
        _run_turso(sql, turso_db=turso_db, dry_run=dry_run, shell_cmd=shell_cmd)
        processed += len(rows)
        if progress:
            print(f"[{table}] {processed}/{total} rows")
    max_row = conn.execute(_build_max_cursor_query(table, cursor_expr)).fetchone()
    max_value = "" if not max_row or max_row[0] is None else str(max_row[0])
    _set_meta(conn, table, max_value)


def _order_tables(tables: list[str]) -> list[str]:
    ordered: list[str] = []
    remaining = set(tables)
    for name in DEFAULT_TABLE_ORDER:
        if name in remaining:
            ordered.append(name)
            remaining.remove(name)
    ordered.extend(sorted(remaining))
    return ordered


def _parse_table_list(values: list[str]) -> set[str]:
    tables: set[str] = set()
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                tables.add(item)
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync local SQLite to Turso via CLI.")
    parser.add_argument(
        "--db",
        default="xueqiu_batch.sqlite3",
        help="Local SQLite path (default: xueqiu_batch.sqlite3).",
    )
    parser.add_argument(
        "--turso-db",
        default=os.environ.get("TURSO_DB", ""),
        help="Turso database name (or set TURSO_DB).",
    )
    parser.add_argument(
        "--turso-shell-cmd",
        default="",
        help='Override shell command, e.g. "turso db shell mydb".',
    )
    parser.add_argument("--full", action="store_true", help="Full sync (all rows).")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental sync using per-table cursor.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per INSERT batch.",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Comma-separated list of tables to include (repeatable).",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Comma-separated list of tables to exclude (repeatable).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only.")
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print per-batch progress.",
    )
    args = parser.parse_args()

    dry_run = args.dry_run

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"sqlite file not found: {db_path}")

    if args.full == args.incremental:
        raise SystemExit("Please choose exactly one of --full or --incremental.")

    turso_db = str(args.turso_db or "").strip()
    if not turso_db:
        raise SystemExit("Missing --turso-db (or set TURSO_DB).")

    shell_cmd = shlex.split(args.turso_shell_cmd) if args.turso_shell_cmd else None

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _ensure_sync_meta(conn)

    tables = _order_tables(_get_user_tables(conn))
    include_tables = _parse_table_list(args.include)
    exclude_tables = _parse_table_list(args.exclude)
    if include_tables:
        tables = [name for name in tables if name in include_tables]
    if exclude_tables:
        tables = [name for name in tables if name not in exclude_tables]

    if args.full:
        schema_sql = _load_schema_sql(conn)
        if schema_sql:
            _run_turso(
                "BEGIN;\n" + ";\n".join(schema_sql) + ";\nCOMMIT;\n",
                turso_db=turso_db,
                dry_run=args.dry_run,
                shell_cmd=shell_cmd,
            )

    for table in tables:
        columns = _get_columns(conn, table)
        if not columns:
            continue
        cursor_expr = _get_cursor_expr(table)
        incremental = bool(args.incremental and cursor_expr)
        select_sql = _build_select_sql(table, cursor_expr, incremental=incremental)
        if incremental:
            _sync_table_incremental(
                conn,
                table=table,
                columns=columns,
                cursor_expr=cursor_expr,
                select_sql=select_sql,
                batch_size=int(args.batch_size),
                turso_db=turso_db,
                dry_run=dry_run,
                progress=args.progress,
                shell_cmd=shell_cmd,
            )
        else:
            _sync_table_full(
                conn,
                table=table,
                columns=columns,
                select_sql=select_sql,
                batch_size=int(args.batch_size),
                turso_db=turso_db,
                dry_run=dry_run,
                progress=args.progress,
                shell_cmd=shell_cmd,
            )
        if cursor_expr and not args.incremental:
            max_row = conn.execute(
                _build_max_cursor_query(table, cursor_expr)
            ).fetchone()
            max_value = "" if not max_row or max_row[0] is None else str(max_row[0])
            _set_meta(conn, table, max_value)

    conn.close()


if __name__ == "__main__":
    main()
