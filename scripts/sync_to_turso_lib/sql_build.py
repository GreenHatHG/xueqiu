from __future__ import annotations

import sqlite3
from typing import Iterable


def sql_literal(value: object) -> str:
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


def build_insert_sql(
    table: str, columns: list[str], rows: Iterable[sqlite3.Row]
) -> str:
    rows_list = list(rows)
    if not rows_list:
        return ""
    col_sql = ", ".join(f'"{col}"' for col in columns)
    values_sql: list[str] = []
    for row in rows_list:
        values_sql.append(
            "(" + ", ".join(sql_literal(row[col]) for col in columns) + ")"
        )
    return (
        f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES\n  '
        + ",\n  ".join(values_sql)
        + ";\n"
    )
