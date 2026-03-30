from __future__ import annotations

import sqlite3
from typing import Optional


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
    "id",
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
    "id",
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
  id INTEGER PRIMARY KEY AUTOINCREMENT,
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


def inject_if_not_exists(sql: str) -> str:
    stripped = sql.lstrip()
    prefix = sql[: len(sql) - len(stripped)]
    if stripped.startswith("CREATE TABLE "):
        return prefix + stripped.replace(
            "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1
        )
    if stripped.startswith("CREATE UNIQUE INDEX "):
        return prefix + stripped.replace(
            "CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1
        )
    if stripped.startswith("CREATE INDEX "):
        return prefix + stripped.replace(
            "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1
        )
    return sql


def standard_schema_sql_for_tables(tables: set[str]) -> list[str]:
    statements: list[str] = []
    if "posts" in tables:
        statements.append(STANDARD_POSTS_SCHEMA.strip())
        statements.extend(
            [
                "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform_post_id)",
            ]
        )
    if "assertions" in tables:
        statements.append(STANDARD_ASSERTIONS_SCHEMA.strip())
        statements.extend(
            [
                "CREATE INDEX IF NOT EXISTS idx_assertions_topic_key ON assertions(topic_key)",
                "CREATE INDEX IF NOT EXISTS idx_assertions_action ON assertions(action)",
            ]
        )
    return statements


def is_standard_table_related(row_type: str, name: str, sql: str) -> bool:
    if name in STANDARD_TABLES:
        return True
    if row_type != "index":
        return False
    low = sql.lower()
    for table in STANDARD_TABLES:
        if f" on {table}" in low or f' on "{table}"' in low:
            return True
    return False


def standard_table_columns(table: str) -> Optional[list[str]]:
    if table == "posts":
        return list(STANDARD_POSTS_COLUMNS)
    if table == "assertions":
        return list(STANDARD_ASSERTIONS_COLUMNS)
    return None


def standard_table_select_sql(table: str, cursor_expr: Optional[str]) -> Optional[str]:
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


def load_schema_sql_for_tables(
    conn: sqlite3.Connection, *, tables: set[str]
) -> list[str]:
    if not tables:
        return []
    placeholders = ", ".join("?" for _ in sorted(tables))
    rows = conn.execute(
        f"""
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
          AND (
            (type = 'table' AND name IN ({placeholders}))
            OR (type = 'index' AND tbl_name IN ({placeholders}))
          )
        ORDER BY CASE WHEN type = 'table' THEN 0 ELSE 1 END, name
        """,
        (*sorted(tables), *sorted(tables)),
    ).fetchall()

    statements: list[str] = []
    for row in rows:
        row_type = str(row[0] or "").strip().lower()
        name = str(row[1] or "").strip()
        sql = str(row[3] or "").strip()
        if not sql:
            continue
        if is_standard_table_related(row_type, name, sql):
            continue
        statements.append(inject_if_not_exists(sql))

    statements.extend(standard_schema_sql_for_tables(tables & STANDARD_TABLES))
    return statements


def get_user_tables(conn: sqlite3.Connection, *, meta_table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0] != meta_table]


def get_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    standard = standard_table_columns(table)
    if standard is not None:
        return standard
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(row[1]) for row in rows]
