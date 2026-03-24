#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

if __package__ in (None, ""):
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = PROJECT_ROOT / "src"
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

from xueqiu_crawler.constants import (  # noqa: E402
    DEFAULT_BATCH_DB_BASENAME,
    DEFAULT_OUTPUT_DIR,
)
from xueqiu_crawler.storage import (  # noqa: E402
    SqliteDb,
    rebuild_user_entries_from_raw_records,
)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rebuild_entries_from_raw_records",
        description=(
            "Rebuild merged_records entry:* rows for given users from raw_records.\n"
            "Useful after changing entry building logic (e.g. de-dup root status line in entry:chain)."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME,
        help="SQLite path (default: data/xueqiu_batch.sqlite3).",
    )
    parser.add_argument(
        "--user-id",
        action="append",
        default=[],
        help="Target user_id (can be provided multiple times).",
    )
    parser.add_argument(
        "--user-list-file",
        type=Path,
        default=None,
        help="Optional user list file (one user_id per line).",
    )
    return parser.parse_args(argv)


def _load_user_ids(args: argparse.Namespace) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for raw in getattr(args, "user_id", []) or []:
        uid = str(raw or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)

    user_list_file = getattr(args, "user_list_file", None)
    if user_list_file:
        path = Path(user_list_file)
        for line in path.read_text(encoding="utf-8").splitlines():
            s = str(line or "").strip()
            if (not s) or s.startswith("#"):
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)

    return out


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    user_ids = _load_user_ids(args)
    if not user_ids:
        print("必须提供 --user-id 或 --user-list-file", file=sys.stderr)
        return 2

    db_path = Path(args.db)
    total_entries = 0
    with SqliteDb(db_path) as db:
        for index, user_id in enumerate(user_ids, start=1):
            print(
                f"[rebuild] {index}/{len(user_ids)} user_id={user_id} start",
                file=sys.stderr,
            )
            count = rebuild_user_entries_from_raw_records(db=db, user_id=str(user_id))
            total_entries += int(count)
            print(
                f"[rebuild] {index}/{len(user_ids)} user_id={user_id} entries={int(count)}",
                file=sys.stderr,
            )

    print(
        f"[rebuild] done users={len(user_ids)} total_entries={total_entries}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
