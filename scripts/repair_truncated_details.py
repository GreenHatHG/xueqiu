#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, cast

if __package__ in (None, ""):
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    SRC_ROOT = PROJECT_ROOT / "src"
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

from xueqiu_crawler.browser import BrowserConfig, BrowserSession
from xueqiu_crawler.cli import _ensure_logged_in_ui
from xueqiu_crawler.constants import (
    BASE_URL,
    DEFAULT_BATCH_DB_BASENAME,
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CONSECUTIVE_BLOCKS,
    DEFAULT_MIN_DELAY_SEC,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_USER_DATA_DIR,
)
from xueqiu_crawler.storage import (
    RAW_TABLE_NAME,
    TALK_TEXT_SEPARATOR,
    SqliteDb,
    rebuild_user_entries_from_raw_records,
)
from xueqiu_crawler.xq_api import ApiConfig, XueqiuApi


SQLITE_IN_CLAUSE_CHUNK_SIZE = 900
DEFAULT_REPAIR_MAX_RETRIES = 1
MERGE_KEY_STATUS_PREFIX = "status:"


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_truncated_details",
        description=(
            "Repair truncated original status text (the [detail] part) by rebuilding entry:* from raw_records "
            "with a logged-in browser session."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME,
        help="SQLite path (default: data/xueqiu_batch.sqlite3).",
    )
    parser.add_argument(
        "--user-list-file",
        type=Path,
        default=None,
        help="Optional user list file (one user_id per line).",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=DEFAULT_MIN_DELAY_SEC,
        help="Minimum delay between requests (seconds).",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=DEFAULT_JITTER_SEC,
        help="Random jitter added to delay (seconds).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_REPAIR_MAX_RETRIES,
        help="Max retries per request.",
    )
    parser.add_argument(
        "--max-consecutive-blocks",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_BLOCKS,
        help="Stop after too many blocked/HTML responses to protect the account.",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        help="Headless mode (not recommended; login/WAF is less reliable).",
    )
    parser.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_USER_DATA_DIR,
        help="Base browser profile directory used for the repair session.",
    )
    parser.add_argument(
        "--chrome-channel",
        default="chrome",
        help="Chrome executable name or full path.",
    )
    parser.add_argument(
        "--reduce-automation-fingerprint",
        action="store_true",
        help="Enable a minimal automation fingerprint mitigation.",
    )
    parser.add_argument(
        "--skip-login-check",
        action="store_true",
        help="Skip login/WAF pre-check (not recommended).",
    )
    parser.add_argument(
        "--login-timeout-sec",
        type=int,
        default=600,
        help="Wait up to N seconds for manual login / WAF verification (default: 600).",
    )
    return parser.parse_args(argv)


def _load_user_ids_from_file(path: Path) -> list[str]:
    file_path = Path(path)
    if not file_path.is_file():
        raise RuntimeError(f"user list file not found: {file_path}")
    lines = file_path.read_text(encoding="utf-8").splitlines()

    user_ids: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line in seen:
            continue
        seen.add(line)
        user_ids.append(line)
    return user_ids


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _looks_like_truncated_first_part(text: Any) -> bool:
    first = str(text or "").split(TALK_TEXT_SEPARATOR, 1)[0].strip()
    if not first:
        return False
    if first.endswith("..."):
        return True
    # Chinese ellipsis punctuation is often "……" (two chars). Treat it as NOT truncated.
    return first.endswith("…") and (not first.endswith("……"))


def _split_display_text(text: Any) -> list[str]:
    if text is None:
        return []
    parts = [str(part).strip() for part in str(text).split(TALK_TEXT_SEPARATOR)]
    return [part for part in parts if part]


def _join_display_lines(lines: list[str]) -> str:
    parts = [str(line).strip() for line in lines if str(line).strip()]
    return TALK_TEXT_SEPARATOR.join(parts)


def _replace_first_display_line(text: Any, new_first_line: str) -> str:
    parts = _split_display_text(text)
    new_line = str(new_first_line or "").strip()
    if not new_line:
        return _join_display_lines(parts)
    if not parts:
        return _join_display_lines([new_line])
    parts[0] = new_line
    return _join_display_lines(parts)


def _load_json_text(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _row_has_retweeted_status_id(record: dict[str, Any]) -> bool:
    if str(record.get("retweeted_status_id") or "").strip():
        return True
    raw_json = record.get("raw_json")
    if not raw_json:
        return False
    try:
        raw_obj = json.loads(str(raw_json))
    except Exception:
        return False
    if not isinstance(raw_obj, dict):
        return False
    return bool(
        str(
            raw_obj.get("retweet_status_id") or raw_obj.get("retweeted_status_id") or ""
        ).strip()
    )


def _row_retweeted_status_id(record: dict[str, Any]) -> str:
    for key in ("retweeted_status_id", "retweet_status_id"):
        value = record.get(key)
        if value not in (None, "", 0, "0"):
            return str(value)

    raw_obj = _load_json_text(record.get("raw_json"))
    for key in ("retweet_status_id", "retweeted_status_id"):
        value = raw_obj.get(key)
        if value not in (None, "", 0, "0"):
            return str(value)

    retweeted_obj = raw_obj.get("retweeted_status")
    if isinstance(retweeted_obj, dict):
        value = retweeted_obj.get("id")
        if value not in (None, "", 0, "0"):
            return str(value)
    return ""


def _strip_optional_str(value: Any) -> str:
    return str(value or "").strip()


def _status_url_from_ctx_or_parts(
    *, ctx: dict[str, Any], user_id: str, status_id: str
) -> str:
    url = _strip_optional_str(ctx.get("status_url"))
    if url:
        return url
    uid = _strip_optional_str(user_id)
    sid = _strip_optional_str(status_id)
    if uid and sid:
        return f"{BASE_URL}/{uid}/{sid}"
    return ""


def _retweeted_status_url_from_ctx_or_parts(
    *,
    ctx: dict[str, Any],
    retweeted_status_user_id: str,
    retweeted_status_id: str,
) -> str:
    url = _strip_optional_str(ctx.get("retweeted_status_url"))
    if url:
        return url
    uid = _strip_optional_str(retweeted_status_user_id)
    sid = _strip_optional_str(retweeted_status_id)
    if uid and sid:
        return f"{BASE_URL}/{uid}/{sid}"
    return ""


def _load_truncated_status_rows_for_user(
    *, db: SqliteDb, user_id: str
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    sql = f"""
    SELECT merge_key, text, context_json, payload_json
    FROM {RAW_TABLE_NAME}
    WHERE user_id = ?
      AND merge_key LIKE 'status:%'
      AND text IS NOT NULL
      AND text != ''
    """
    for row in db.conn.execute(sql, (str(user_id),)):
        merge_key = _strip_optional_str(row["merge_key"])
        if not merge_key.startswith(MERGE_KEY_STATUS_PREFIX):
            continue
        text = str(row["text"] or "").strip()
        if not _looks_like_truncated_first_part(text):
            continue
        payload = _load_json_text(row["payload_json"])
        record_obj = payload.get("record")
        rec = cast(dict[str, Any], record_obj) if isinstance(record_obj, dict) else {}
        if not _row_has_retweeted_status_id(rec):
            continue
        out.append(
            {
                "merge_key": merge_key,
                "text": text,
                "context": _load_json_text(row["context_json"]),
                "record": rec,
            }
        )
    return out


def _collect_users_with_truncated_details(
    *, db: SqliteDb, user_ids: Optional[list[str]]
) -> dict[str, int]:
    counts: dict[str, int] = {}

    def _run_query(user_filter_sql: str, params: tuple[Any, ...]) -> None:
        sql = f"""
        SELECT user_id, text, payload_json
        FROM {RAW_TABLE_NAME}
        WHERE merge_key LIKE 'status:%'
          AND text IS NOT NULL
          AND text != ''
          {user_filter_sql}
        """
        for row in db.conn.execute(sql, params):
            user_id = str(row["user_id"] or "").strip()
            if not user_id:
                continue
            if not _looks_like_truncated_first_part(row["text"]):
                continue
            payload = _load_json_text(row["payload_json"])
            record_obj = payload.get("record")
            rec = (
                cast(dict[str, Any], record_obj) if isinstance(record_obj, dict) else {}
            )
            if not _row_has_retweeted_status_id(rec):
                continue
            counts[user_id] = int(counts.get(user_id, 0)) + 1

    if not user_ids:
        _run_query("", tuple())
        return counts

    for chunk in _chunks(list(user_ids), SQLITE_IN_CLAUSE_CHUNK_SIZE):
        placeholders = ",".join(["?"] * len(chunk))
        _run_query(f"AND user_id IN ({placeholders})", tuple(str(u) for u in chunk))
    return counts


def _wait_for_manual_detail_verification(*, ui_page, url: str) -> None:
    target = str(url or "").strip() or BASE_URL
    try:
        ui_page.goto(target, wait_until="domcontentloaded")
    except Exception:
        try:
            ui_page.goto(BASE_URL, wait_until="domcontentloaded")
        except Exception:
            pass
    print(
        "现在需要你手动处理一下风控验证（UI 标签页里）。处理好后回到终端按回车继续。",
        file=sys.stderr,
    )
    input("按回车继续（Ctrl+C 退出）: ")


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db)

    user_ids: Optional[list[str]] = None
    if args.user_list_file is not None:
        try:
            user_ids = _load_user_ids_from_file(Path(args.user_list_file))
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 2

    api_cfg = ApiConfig(
        min_delay_sec=float(args.min_delay),
        jitter_sec=float(args.jitter),
        max_retries=int(args.max_retries),
        max_consecutive_blocks=int(args.max_consecutive_blocks),
    )

    browser_cfg = BrowserConfig(
        headless=bool(args.headless),
        user_data_dir=Path(args.user_data_dir),
        chrome_channel=str(args.chrome_channel or "chrome"),
        cdp_url=None,
        reduce_automation_fingerprint=bool(args.reduce_automation_fingerprint),
        manage_cdp=True,
    )

    with SqliteDb(db_path) as db:
        users_to_repair = _collect_users_with_truncated_details(
            db=db, user_ids=user_ids
        )
        if not users_to_repair:
            print("没找到需要修复的截断原帖全文（[detail]）。", file=sys.stderr)
            return 0

        total_users = len(users_to_repair)
        total_status_rows = sum(int(v) for v in users_to_repair.values())
        print(
            f"找到 {total_users} 个用户需要修（status 行数={total_status_rows}）。",
            file=sys.stderr,
        )

        with BrowserSession(browser_cfg) as session:
            ui_page = session.ui_page
            if args.skip_login_check:
                ui_page.goto(BASE_URL, wait_until="domcontentloaded")
            else:
                try:
                    _ensure_logged_in_ui(ui_page, int(args.login_timeout_sec))
                except Exception as exc:
                    print(f"Login check failed: {exc}", file=sys.stderr)
                    return 2

            detail_api = XueqiuApi(
                session.page,
                api_cfg,
                prefer_page_fetch=bool(session.prefer_page_fetch),
            )
            detail_cache: dict[str, Optional[str]] = {}
            waited_for_waf = False

            def resolve_status_line(
                status_id: str,
                source_status_url: str = "",
                status_url: str = "",
                status_user_id: str = "",
            ) -> Optional[str]:
                nonlocal waited_for_waf
                sid = str(status_id or "").strip()
                if not sid:
                    return None
                if sid in detail_cache:
                    return detail_cache[sid]

                debug: dict[str, Any] = {}
                print(f"[detail] 尝试补抓原帖全文 status_id={sid}", file=sys.stderr)
                line, failure_reason = detail_api.fetch_status_display_line(
                    sid,
                    source_status_url=str(source_status_url or "").strip(),
                    status_url=str(status_url or "").strip(),
                    status_user_id=str(status_user_id or "").strip(),
                    debug=debug,
                )
                if (
                    (not line)
                    and (not waited_for_waf)
                    and (not bool(args.headless))
                    and failure_reason
                    and ("风控" in str(failure_reason) or "验证" in str(failure_reason))
                ):
                    waited_for_waf = True
                    candidate_urls = debug.get("candidate_urls")
                    blocked_url = (
                        str(candidate_urls[0]).strip()
                        if isinstance(candidate_urls, list) and candidate_urls
                        else ""
                    )
                    _wait_for_manual_detail_verification(
                        ui_page=ui_page,
                        url=blocked_url or BASE_URL,
                    )
                    debug = {}
                    line, failure_reason = detail_api.fetch_status_display_line(
                        sid,
                        source_status_url=str(source_status_url or "").strip(),
                        status_url=str(status_url or "").strip(),
                        status_user_id=str(status_user_id or "").strip(),
                        debug=debug,
                    )

                if line:
                    out = str(line).strip()
                    if out:
                        detail_cache[sid] = out
                        print(
                            f"[detail] 原帖全文补抓成功 status_id={sid}",
                            file=sys.stderr,
                        )
                        return out
                print(
                    f"[detail] 原帖全文补抓失败 status_id={sid}，原因：{failure_reason or '页面没拿到正文'}",
                    file=sys.stderr,
                )
                detail_cache[sid] = None
                return None

            repaired_users = 0
            total_entries = 0
            for idx, (user_id, truncated_count) in enumerate(
                sorted(users_to_repair.items(), key=lambda kv: kv[0]), start=1
            ):
                print(
                    f"[repair] 用户 {idx}/{total_users}: {user_id} 需要修的 status 行数={truncated_count}",
                    file=sys.stderr,
                )
                # 1) Patch raw_records first so the fix persists (raw is the source of truth).
                truncated_rows = _load_truncated_status_rows_for_user(
                    db=db, user_id=str(user_id)
                )
                updated_raw_rows = 0
                for row in truncated_rows:
                    merge_key = _strip_optional_str(row.get("merge_key"))
                    record = cast(dict[str, Any], row.get("record") or {})
                    ctx = cast(dict[str, Any], row.get("context") or {})

                    status_id = _strip_optional_str(
                        merge_key[len(MERGE_KEY_STATUS_PREFIX) :]
                    )
                    retweeted_status_id = _row_retweeted_status_id(record)
                    if not retweeted_status_id:
                        continue

                    retweeted_status_user_id = _strip_optional_str(
                        ctx.get("retweeted_status_user_id")
                        or record.get("retweeted_status_user_id")
                    )
                    if not retweeted_status_user_id:
                        # Best-effort hint: fall back to current row's user_id.
                        retweeted_status_user_id = _strip_optional_str(
                            record.get("user_id")
                        )

                    full_line = resolve_status_line(
                        retweeted_status_id,
                        _status_url_from_ctx_or_parts(
                            ctx=ctx, user_id=str(user_id), status_id=status_id
                        ),
                        _retweeted_status_url_from_ctx_or_parts(
                            ctx=ctx,
                            retweeted_status_user_id=retweeted_status_user_id,
                            retweeted_status_id=retweeted_status_id,
                        ),
                        retweeted_status_user_id,
                    )
                    if not full_line:
                        continue

                    old_text = str(row.get("text") or "").strip()
                    new_text = _replace_first_display_line(old_text, str(full_line))
                    if not new_text or new_text == old_text:
                        continue
                    db.conn.execute(
                        f"UPDATE {RAW_TABLE_NAME} SET text = ? WHERE merge_key = ?",
                        (new_text, merge_key),
                    )
                    updated_raw_rows += 1

                if updated_raw_rows:
                    db.conn.commit()
                print(
                    f"[repair] 已写回 raw_records user_id={user_id} rows={updated_raw_rows}",
                    file=sys.stderr,
                )

                # 2) Rebuild entry:* for the user from (updated) raw_records.
                entry_count = rebuild_user_entries_from_raw_records(
                    db=db, user_id=str(user_id)
                )
                repaired_users += 1
                total_entries += int(entry_count)
                print(
                    f"[repair] 已重建 entry:* user_id={user_id} entries={entry_count}",
                    file=sys.stderr,
                )

            print(
                f"[repair] 完成 repaired_users={repaired_users} rebuilt_entries={total_entries}",
                file=sys.stderr,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
