from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path
from typing import Optional

from . import schema as schema_lib
from .session import TursoSession
from .sql_build import build_insert_sql


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


def _build_select_sql(
    table: str, cursor_expr: Optional[str], *, incremental: bool
) -> str:
    standard_sql = schema_lib.standard_table_select_sql(
        table, cursor_expr if incremental else None
    )
    if standard_sql:
        return standard_sql
    if incremental and cursor_expr:
        return _build_incremental_query(table, cursor_expr)
    return f'SELECT * FROM "{table}"'


def _safe_print_sql(sql_text: str) -> None:
    try:
        sys.stdout.write(sql_text)
        if not sql_text.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
    except BrokenPipeError:
        try:
            sys.stdout.close()
        except Exception:
            pass
        raise SystemExit(0) from None


def _run_turso(
    sql_text: str, *, dry_run: bool, session: Optional[TursoSession]
) -> None:
    if not sql_text.strip():
        return
    if dry_run:
        _safe_print_sql(sql_text)
        return
    if session is None:
        raise RuntimeError("missing turso session")
    session.execute(sql_text)


def _sync_table_full(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    select_sql: str,
    batch_size: int,
    dry_run: bool,
    progress: bool,
    session: Optional[TursoSession],
) -> None:
    total = _count_rows(conn, table, cursor_expr=None, last_value="")
    processed = 0
    if progress:
        print(f"[{table}] start total={total} batch={batch_size}", flush=True)
    cursor = conn.execute(select_sql)
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        sql = build_insert_sql(table, columns, rows)
        _run_turso(sql, dry_run=dry_run, session=session)
        processed += len(rows)
        if progress:
            print(f"[{table}] synced {processed}/{total}", flush=True)


def _sync_table_incremental(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    cursor_expr: str,
    select_sql: str,
    batch_size: int,
    dry_run: bool,
    progress: bool,
    session: Optional[TursoSession],
) -> str:
    last_value = _get_meta(conn, table)
    total = _count_rows(conn, table, cursor_expr=cursor_expr, last_value=last_value)
    processed = 0
    if progress:
        print(
            f"[{table}] start last={last_value or '<empty>'} total={total} batch={batch_size}",
            flush=True,
        )
    cursor = conn.execute(select_sql, (last_value,))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        sql = build_insert_sql(table, columns, rows)
        _run_turso(sql, dry_run=dry_run, session=session)
        processed += len(rows)
        if progress:
            print(f"[{table}] synced {processed}/{total}", flush=True)
    max_row = conn.execute(_build_max_cursor_query(table, cursor_expr)).fetchone()
    max_value = "" if not max_row or max_row[0] is None else str(max_row[0])
    return max_value


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


def sync_sqlite_to_turso(
    *,
    db_path: Path,
    full: bool,
    incremental: bool,
    batch_size: int,
    include: list[str],
    exclude: list[str],
    dry_run: bool,
    progress: bool,
    session: Optional[TursoSession],
) -> None:
    if full == incremental:
        raise SystemExit("Please choose exactly one of --full or --incremental.")
    if not db_path.exists():
        raise SystemExit(f"sqlite file not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    pending_meta_updates: dict[str, str] = {}
    try:
        _ensure_sync_meta(conn)

        tables = _order_tables(
            schema_lib.get_user_tables(conn, meta_table=SYNC_META_TABLE)
        )
        include_tables = _parse_table_list(include)
        exclude_tables = _parse_table_list(exclude)
        if include_tables:
            tables = [name for name in tables if name in include_tables]
        if exclude_tables:
            tables = [name for name in tables if name not in exclude_tables]

        def run_all(active_session: Optional[TursoSession]) -> None:
            if full:
                schema_tables = set(tables)
                schema_sql = schema_lib.load_schema_sql_for_tables(
                    conn, tables=schema_tables
                )
                schema_statements = [
                    stmt.strip() for stmt in schema_sql if stmt.strip()
                ]
                if schema_statements:
                    if progress:
                        print(
                            f"[schema] start items={len(schema_statements)}",
                            flush=True,
                        )
                    applied = 0
                    for statement in schema_statements:
                        sql = statement
                        if not sql.rstrip().endswith(";"):
                            sql += ";"
                        _run_turso(
                            sql + "\n",
                            dry_run=dry_run,
                            session=active_session,
                        )
                        applied += 1
                        if progress:
                            print(
                                f"[schema] applied {applied}/{len(schema_statements)}",
                                flush=True,
                            )
                    if progress:
                        print("[schema] done", flush=True)

            for table in tables:
                columns = schema_lib.get_columns(conn, table)
                if not columns:
                    continue
                cursor_expr = _get_cursor_expr(table)
                is_incremental = bool(incremental and cursor_expr)
                select_sql = _build_select_sql(
                    table, cursor_expr, incremental=is_incremental
                )
                if is_incremental:
                    assert cursor_expr is not None
                    max_value = _sync_table_incremental(
                        conn,
                        table=table,
                        columns=columns,
                        cursor_expr=cursor_expr,
                        select_sql=select_sql,
                        batch_size=int(batch_size),
                        dry_run=dry_run,
                        progress=progress,
                        session=active_session,
                    )
                    pending_meta_updates[table] = max_value
                    continue

                _sync_table_full(
                    conn,
                    table=table,
                    columns=columns,
                    select_sql=select_sql,
                    batch_size=int(batch_size),
                    dry_run=dry_run,
                    progress=progress,
                    session=active_session,
                )
                if cursor_expr:
                    max_row = conn.execute(
                        _build_max_cursor_query(table, cursor_expr)
                    ).fetchone()
                    max_value = (
                        "" if not max_row or max_row[0] is None else str(max_row[0])
                    )
                    pending_meta_updates[table] = max_value

        if session is None:
            run_all(None)
        else:
            with session:
                run_all(session)

        for table_name, cursor_value in pending_meta_updates.items():
            _set_meta(conn, table_name, cursor_value)
    finally:
        conn.close()
