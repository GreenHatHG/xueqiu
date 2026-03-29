#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from sync_to_turso_lib import session as session_lib  # noqa: E402
from sync_to_turso_lib import sync as sync_lib  # noqa: E402


ENV_FILE_BASENAME = ".env"
ENV_KEY_URL_SUFFIX = "TURSO_DATABASE_URL"
ENV_KEY_TOKEN_SUFFIX = "TURSO_AUTH_TOKEN"


def _normalize_env_prefix(raw: str) -> str:
    name = str(raw or "").strip().upper()
    out: list[str] = []
    for ch in name:
        out.append(ch if ch.isalnum() else "_")
    normalized = "".join(out).strip("_")
    return normalized or "TURSO"


def _dotenv_candidate_paths() -> list[Path]:
    paths = [Path.cwd() / ENV_FILE_BASENAME, REPO_ROOT / ENV_FILE_BASENAME]
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        content = path.read_text(encoding="utf-8")
    except Exception:
        return
    for raw_line in content.splitlines():
        line = str(raw_line or "").strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = str(key or "").strip()
        if not key:
            continue
        value = str(value or "").strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        if key not in os.environ:
            os.environ[key] = value


def _load_dotenv() -> None:
    for path in _dotenv_candidate_paths():
        _load_dotenv_file(path)


def _build_env_keys(turso_db: str) -> tuple[str, str]:
    prefix = _normalize_env_prefix(turso_db)
    return f"{prefix}_{ENV_KEY_URL_SUFFIX}", f"{prefix}_{ENV_KEY_TOKEN_SUFFIX}"


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="用 SDK 把本地 SQLite 同步到 Turso。")
    parser.add_argument(
        "--db",
        default="xueqiu_batch.sqlite3",
        help="本地 SQLite 路径（默认：xueqiu_batch.sqlite3）。",
    )
    parser.add_argument(
        "--turso-db",
        default=os.environ.get("TURSO_DB", ""),
        help="Turso 名称（用来拼 .env key；或设 TURSO_DB）。",
    )
    parser.add_argument(
        "--connect-timeout-sec",
        type=int,
        default=session_lib.DEFAULT_CONNECT_TIMEOUT_SEC,
        help="超时秒数（连接/请求）。",
    )
    parser.add_argument(
        "--ack-timeout-sec",
        type=int,
        default=session_lib.DEFAULT_ACK_TIMEOUT_SEC,
        help="超时秒数（兼容旧参数）。",
    )
    parser.add_argument("--full", action="store_true", help="全量同步（所有行）。")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="增量同步（按表 cursor）。",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="每批多少行（INSERT）。",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="要同步哪些表（逗号分隔，可重复）。",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="不要同步哪些表（逗号分隔，可重复）。",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="只打印 SQL，不写 Turso。"
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="显示进度。",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    turso_db = str(args.turso_db or "").strip()
    if not turso_db:
        raise SystemExit("缺少 --turso-db（或环境变量 TURSO_DB）。")

    session: session_lib.TursoSession | None = None
    if not bool(args.dry_run):
        url_key, token_key = _build_env_keys(turso_db)
        url = str(os.environ.get(url_key, "") or "").strip()
        token = str(os.environ.get(token_key, "") or "").strip()
        missing: list[str] = []
        if not url:
            missing.append(url_key)
        if not token:
            missing.append(token_key)
        if missing:
            raise SystemExit("缺少 .env 配置：" + ", ".join(missing))

        timeout_sec = max(int(args.connect_timeout_sec), int(args.ack_timeout_sec))
        session = session_lib.LibsqlSession(
            url=url,
            auth_token=token,
            timeout_sec=timeout_sec,
        )

    sync_lib.sync_sqlite_to_turso(
        db_path=db_path,
        full=bool(args.full),
        incremental=bool(args.incremental),
        batch_size=int(args.batch_size),
        include=list(args.include),
        exclude=list(args.exclude),
        dry_run=bool(args.dry_run),
        progress=bool(args.progress),
        session=session,
    )


if __name__ == "__main__":
    main()
