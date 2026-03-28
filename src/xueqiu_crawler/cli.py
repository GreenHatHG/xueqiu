from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

from .constants import (
    BASE_URL,
    BEIJING_TIMEZONE_NAME,
    DEFAULT_BATCH_DB_BASENAME,
    DEFAULT_BATCH_USER_COOLDOWN_SEC,
    DEFAULT_CORE_MAX_USER_COMMENTS_SCAN_PAGES,
    DEFAULT_CORE_MAX_TALK_PAGES,
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CONSECUTIVE_BLOCKS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MIN_DELAY_SEC,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_USER_DATA_DIR,
    USER_COMMENTS_PAGE_SIZE,
)
from .storage import (
    MERGE_KEY_COMMENT_PREFIX,
    RAW_TABLE_NAME,
    SqliteCrawlCheckpointStore,
    SqliteCrawlProgressStore,
    SqliteDb,
    SqliteMergedCommentsStore,
    SqliteMergedStatusesStore,
    SqliteMergedTalksStore,
    SqliteTalksProgressStore,
    rebuild_user_entries_from_raw_records,
)
from .text_sanitize import sanitize_xueqiu_text
from .xq_api import (
    ApiConfig,
    BlockedError,
    ChallengeRequiredError,
    XueqiuApi,
    normalize_root_status_url,
)
from .rate_limit import RateLimiter


BEIJING_TIMEZONE = ZoneInfo(BEIJING_TIMEZONE_NAME)
UI_PAGINATION_NEXT_SELECTOR = "a.pagination__next"
UI_INTERCEPT_IDLE_SEC = 6.0
UI_INTERCEPT_MAX_IDLE_ROUNDS = 6
UI_INTERCEPT_ROUNDS_PER_BATCH = 30
UI_INTERCEPT_PAGE_TURN_TIMEOUT_SEC = 8.0
UI_INTERCEPT_PAGE_TURN_SETTLE_SEC = 2.0
UI_INTERCEPT_MAX_CONSECUTIVE_EMPTY_TIMELINE_BATCHES = 3
LOGIN_STATE_CHECK_INTERVAL_SEC = 4.0
LOGIN_STATE_CONFIRM_SETTLE_SEC = 2.0
BASE_PROFILE_COPY_SETTLE_SEC = 2.0
LOGIN_HOME_TITLE_TEXT = "我的首页"
LOGIN_POST_BUTTON_TEXT = "发帖"
LOGIN_UI_STRONG_SIGNAL_NAMES = frozenset(
    {
        "post_button",
        "user_name",
        "editor_placeholder",
        "logout_link",
        "settings_link",
    }
)
LOGIN_UI_SIGNAL_LABELS = {
    "home_title": "首页标题",
    "post_button": "发帖按钮",
    "user_name": "用户名字",
    "editor_placeholder": "发帖输入框",
    "logout_link": "退出账号",
    "settings_link": "个人设置",
}
BROWSER_PROFILE_RUNS_DIRNAME = "browser_profiles"
BROWSER_PROFILE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S_%f"
BROWSER_PROFILE_COPY_IGNORE_PATTERNS = (
    "SingletonLock",
    "SingletonSocket",
    "SingletonCookie",
    "DevToolsActivePort",
    "*.lock",
    "*.tmp",
)
PROGRESS_STAGE_TIMELINE = "timeline"
PROGRESS_STAGE_COMMENTS = "comments"
PROGRESS_STAGE_TALKS = "talks"
PROGRESS_STAGE_FINALIZE = "finalize"


def _beijing_iso_now() -> str:
    return dt.datetime.now(tz=BEIJING_TIMEZONE).replace(microsecond=0).isoformat()


def _format_progress_dt(value: Optional[dt.datetime]) -> str:
    if value is None:
        return "-"
    return value.astimezone(BEIJING_TIMEZONE).replace(microsecond=0).isoformat(sep=" ")


def _user_has_entry_rows(*, db: SqliteDb, user_id: str) -> bool:
    row = db.conn.execute(
        """
        SELECT 1
        FROM merged_records
        WHERE user_id = ? AND merge_key LIKE 'entry:%'
        LIMIT 1
        """,
        (str(user_id),),
    ).fetchone()
    return bool(row)


def _user_has_raw_rows(*, db: SqliteDb, user_id: str) -> bool:
    row = db.conn.execute(
        f"""
        SELECT 1
        FROM {RAW_TABLE_NAME}
        WHERE user_id = ?
          AND (
            merge_key LIKE 'status:%'
            OR merge_key LIKE 'comment:%'
            OR merge_key LIKE 'talk:%'
          )
        LIMIT 1
        """,
        (str(user_id),),
    ).fetchone()
    return bool(row)


def _max_raw_created_at_bj_iso(*, db: SqliteDb, user_id: str) -> Optional[str]:
    row = db.conn.execute(
        f"""
        SELECT MAX(created_at_bj) AS max_created_at_bj
        FROM {RAW_TABLE_NAME}
        WHERE user_id = ?
          AND created_at_bj IS NOT NULL
          AND created_at_bj != ''
        """,
        (str(user_id),),
    ).fetchone()
    if not row:
        return None
    value = row["max_created_at_bj"]
    if value in (None, ""):
        return None
    return str(value).strip()


def _resolve_incremental_since_bj(
    *,
    args: argparse.Namespace,
    db: SqliteDb,
    user_id: str,
    tz_name: str,
) -> dt.datetime:
    checkpoint_store = SqliteCrawlCheckpointStore(db=db)
    checkpoint = checkpoint_store.get(user_id=str(user_id))
    if checkpoint:
        return _parse_since_to_beijing(
            str(checkpoint.get("checkpoint_bj_iso") or ""), tz_name=tz_name
        )

    raw_max_bj_iso = _max_raw_created_at_bj_iso(db=db, user_id=str(user_id))
    if raw_max_bj_iso:
        return _parse_since_to_beijing(raw_max_bj_iso, tz_name=tz_name)

    if args.since:
        return _parse_since_to_beijing(args.since, tz_name=tz_name)

    raise ValueError(
        f"--incremental mode requires an existing checkpoint or raw_records history for user {user_id}. "
        "For the first run of a new user, please provide --since."
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="xq-crawl")
    p.add_argument(
        "--user-list-file",
        type=Path,
        required=True,
        help="用户列表文件路径（UTF-8 文本；每行一个用户ID，空行与 # 注释行会忽略）",
    )
    p.add_argument(
        "--start-from-user",
        default="",
        help=(
            "从指定 user_id 开始跑（包含该用户）。"
            "用于某个用户失败退出后，下次重跑不用从第一个用户慢慢走到它。"
        ),
    )
    p.add_argument(
        "--mode",
        default="core",
        choices=["core", "incremental_http"],
        help=(
            "抓取内容范围："
            "core=浏览器会话抓取（发言+回复+查看对话）；"
            "incremental_http=无浏览器增量抓取（需要环境变量 XUEQIU_COOKIE；timeline/comments 只抓一页，但 talks/detail 尽量补齐）。"
        ),
    )
    p.add_argument(
        "--since",
        default=None,
        help=(
            "截止时间（本地时区日期 YYYY-MM-DD 或 ISO 8601 时间）。"
            "非增量 core 模式必填：抓取 >= since 的内容，遇到更早内容则停止。"
            "incremental_http 模式可选：若不提供则不做时间过滤（仍然只抓一页）。"
            "When using --incremental, --since is only needed for the first run of a new user."
        ),
    )
    p.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Enable incremental crawling: use per-user checkpoint (watermark) to fetch only newly published content. "
            "De-dup is handled by SQLite merge_key uniqueness."
        ),
    )
    p.add_argument(
        "--out", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录（默认 data/）"
    )
    p.add_argument(
        "--db",
        type=Path,
        default=None,
        help=(
            f"SQLite 数据库文件路径（默认 {{out}}/{DEFAULT_BATCH_DB_BASENAME}）。"
            "本项目已切换为“只落库”，不再生成 JSONL/CSV/JSON 文件。"
        ),
    )
    p.add_argument(
        "--skip-login-check",
        action="store_true",
        help="跳过登录/风控探测（不推荐；如果接口被拦截会在后续请求中失败）",
    )

    p.add_argument(
        "--max-timeline-pages",
        type=int,
        default=0,
        help="时间线最多抓取批次（UI 拦截模式下等价于滚动触发的 JSON 批次数；默认 0 表示抓到 --since 为止）",
    )
    p.add_argument(
        "--max-comment-pages",
        type=int,
        default=0,
        help="全站回复最多抓取批次（UI 拦截模式下等价于滚动触发的 JSON 批次数；默认 0 表示抓到 --since 为止）",
    )

    p.add_argument(
        "--with-talks",
        action="store_true",
        help="强制开启：为每条回复抓取“查看对话”上下文（core 默认已开启）",
    )
    p.add_argument(
        "--no-talks",
        action="store_true",
        help="不抓取“查看对话”（core 模式默认会尽量抓 talks；如果你只要主干数据可用该开关关闭）",
    )
    p.add_argument(
        "--max-talk-pages",
        type=int,
        default=DEFAULT_CORE_MAX_TALK_PAGES,
        help=f"每条对话链最多抓取页数（默认 {DEFAULT_CORE_MAX_TALK_PAGES}；对话很长时可继续跑补齐）",
    )
    p.add_argument(
        "--tz",
        default=BEIJING_TIMEZONE_NAME,
        help="日期判定用的时区（默认 Asia/Shanghai）。",
    )

    p.add_argument(
        "--min-delay",
        type=float,
        default=DEFAULT_MIN_DELAY_SEC,
        help="每次请求的最小延迟（秒）",
    )
    p.add_argument(
        "--jitter", type=float, default=DEFAULT_JITTER_SEC, help="随机抖动（秒）"
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="单请求最大重试次数；像第一页空列表这种明显不对的回包也会重试",
    )
    p.add_argument(
        "--max-consecutive-blocks",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_BLOCKS,
        help="连续被风控/返回HTML次数阈值，超过则停止",
    )
    p.add_argument(
        "--user-cooldown-sec",
        type=float,
        default=DEFAULT_BATCH_USER_COOLDOWN_SEC,
        help=f"相邻两个用户之间额外等待的秒数（默认 {DEFAULT_BATCH_USER_COOLDOWN_SEC}）",
    )

    p.add_argument(
        "--headless", action="store_true", help="无头模式（不推荐，登录与风控更不稳定）"
    )
    p.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_USER_DATA_DIR,
        help="基础浏览器资料目录。第一次会在这里登录，后面每个用户都会先复制一份新的目录再抓。",
    )
    p.add_argument(
        "--chrome-channel",
        default="chrome",
        help="Chrome 可执行名字或路径（默认 chrome）",
    )
    p.add_argument(
        "--reduce-automation-fingerprint",
        action="store_true",
        help="启用最小的自动化指纹减弱（若出现 alichlgref/md5__1038 无限跳转，建议开启）",
    )
    p.add_argument(
        "--login-timeout-sec",
        type=int,
        default=600,
        help="等待手动登录的最长时间（秒，默认 600）",
    )
    return p.parse_args(argv)


def _get_login_ui_signal_names(page) -> set[str]:
    try:
        signal_names = page.evaluate(
            """
            () => {
              const isVisible = (node) => {
                if (!node) {
                  return false;
                }
                const style = window.getComputedStyle(node);
                if (!style) {
                  return false;
                }
                if (style.display === 'none' || style.visibility === 'hidden') {
                  return false;
                }
                if (Number(style.opacity || '1') === 0) {
                  return false;
                }
                const rect = node.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
              };

              const signalNames = [];
              const title = (document.title || '').trim();
              if (title.includes(%r)) {
                signalNames.push('home_title');
              }

              const postButton = document.querySelector('.user__col-btn-status > button');
              if (
                postButton &&
                isVisible(postButton) &&
                (postButton.textContent || '').trim().includes(%r)
              ) {
                signalNames.push('post_button');
              }

              const userName = document.querySelector('#side_user_name, .user__col--name');
              if (userName && isVisible(userName)) {
                signalNames.push('user_name');
              }

              const editorPlaceholder = document.querySelector('.editor-container .fake-placeholder');
              if (editorPlaceholder && isVisible(editorPlaceholder)) {
                signalNames.push('editor_placeholder');
              }

              const logoutLink = document.querySelector("a[href*='/snowman/logout']");
              if (logoutLink && isVisible(logoutLink)) {
                signalNames.push('logout_link');
              }

              const settingsLink = document.querySelector("a[href='/setting/user']");
              if (settingsLink && isVisible(settingsLink)) {
                signalNames.push('settings_link');
              }

              return signalNames;
            }
            """
            % (LOGIN_HOME_TITLE_TEXT, LOGIN_POST_BUTTON_TEXT)
        )
    except Exception:
        return set()

    names: set[str] = set()
    for name in signal_names or []:
        key = str(name or "").strip()
        if key:
            names.add(key)
    return names


def _is_confirmed_logged_in(*, ui_signal_names: set[str]) -> bool:
    return bool(ui_signal_names.intersection(LOGIN_UI_STRONG_SIGNAL_NAMES))


def _format_login_state(ui_signal_names: set[str]) -> str:
    ui_parts = [
        label
        for name, label in LOGIN_UI_SIGNAL_LABELS.items()
        if name in ui_signal_names
    ]
    ui_parts.extend(
        sorted(name for name in ui_signal_names if name not in LOGIN_UI_SIGNAL_LABELS)
    )
    ui_text = ", ".join(ui_parts) if ui_parts else "-"
    return f"页面: {ui_text}"


def _ensure_logged_in_ui(page, timeout_sec: int) -> None:
    """
    UI 拦截模式下不做“程序侧直连接口探测”，只基于页面痕迹做保守等待。

    说明：登录/滑动验证码很难在不触发更强风控的前提下全自动完成，因此这里只做“自动等待已有登录态”，
    如果未登录则需要你在弹出的浏览器里手动登录一次（之后 persistent context 会复用）。
    """

    try:
        page.goto(BASE_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    ui_signal_names = _get_login_ui_signal_names(page)
    if _is_confirmed_logged_in(ui_signal_names=ui_signal_names):
        return

    print(
        "当前浏览器里还没看出已经登录。请在打开的浏览器窗口里手动登录雪球一次。",
        file=sys.stderr,
    )
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        time.sleep(LOGIN_STATE_CHECK_INTERVAL_SEC)
        ui_signal_names = _get_login_ui_signal_names(page)
        if _is_confirmed_logged_in(ui_signal_names=ui_signal_names):
            time.sleep(LOGIN_STATE_CONFIRM_SETTLE_SEC)
            ui_signal_names = _get_login_ui_signal_names(page)
            if not _is_confirmed_logged_in(ui_signal_names=ui_signal_names):
                continue
            print(
                f"检测到已登录痕迹（{_format_login_state(ui_signal_names)}），继续执行自动化浏览与拦截取数。",
                file=sys.stderr,
            )
            return
    raise RuntimeError(f"等待登录超时（{timeout_sec}s），为保护账号已停止。")


def _wait_for_waf_challenge(
    ui_page,
    api: XueqiuApi,
    user_id: str,
    timeout_sec: int,
    blocked_url: str,
    *,
    navigate_to_blocked_url: bool = True,
) -> None:
    """
    Stop aggressive retries when a WAF challenge page is detected and wait for manual verification.
    """

    if navigate_to_blocked_url:
        try:
            ui_page.goto(blocked_url, wait_until="domcontentloaded")
        except Exception:
            try:
                ui_page.goto(BASE_URL, wait_until="domcontentloaded")
            except Exception:
                pass
    else:
        try:
            current_url = str(ui_page.url or "")
        except Exception:
            current_url = ""
        if not current_url.startswith(BASE_URL):
            try:
                ui_page.goto(BASE_URL, wait_until="domcontentloaded")
            except Exception:
                pass

    print(
        "检测到需要手动验证/滑动的风控挑战。为保护账号，程序将暂停重试并等待你处理。",
        file=sys.stderr,
    )
    print(
        f"请在打开的 UI 标签页里完成验证（必要时刷新），目标接口：{blocked_url}",
        file=sys.stderr,
    )
    print(
        "如果该自动化浏览器一直验证失败，通常需要改用你日常 Chrome Profile："
        "先换一份全新的基础浏览器资料目录，重新登录后再跑。",
        file=sys.stderr,
    )

    # Do NOT auto-refresh the blocked endpoint in a loop.
    # Instead, wait for the user to finish verification and explicitly continue.
    max_confirm_attempts = max(1, int(timeout_sec // 5))
    for _ in range(max_confirm_attempts):
        try:
            input("完成验证后按回车继续（Ctrl+C 退出）: ")
        except KeyboardInterrupt:
            raise
        probe = api.probe_url_json(blocked_url, referrer=BASE_URL)
        if probe.get("ok") is True:
            print("验证通过，继续抓取。", file=sys.stderr)
            return
        print(
            "仍然处于风控挑战/验证状态，尚无法获取 JSON。请在 UI 标签页继续完成验证后再按回车。",
            file=sys.stderr,
        )

    raise RuntimeError(f"等待风控验证超时（{timeout_sec}s），为保护账号已停止。")


def _normalize_timeline_status(status: dict[str, Any], user_id: str) -> dict[str, Any]:
    sid = status.get("id")
    comment_id = status.get("commentId") or status.get("comment_id")
    created_bj = _parse_created_at_to_beijing(status.get("created_at"))
    text = status.get("text") or status.get("description")
    retweeted_status = (
        status.get("retweeted_status")
        if isinstance(status.get("retweeted_status"), dict)
        else None
    )
    return {
        "status_id": str(sid) if sid is not None else None,
        "comment_id": str(comment_id) if comment_id not in (None, "", 0, "0") else None,
        "user_id": str(user_id),
        "created_at": status.get("created_at"),
        "created_at_bj": created_bj.replace(microsecond=0).isoformat()
        if created_bj is not None
        else None,
        "text": sanitize_xueqiu_text(text),
        "retweeted_status_id": (
            str(retweeted_status.get("id"))
            if retweeted_status and retweeted_status.get("id") is not None
            else (
                str(status.get("retweet_status_id"))
                if status.get("retweet_status_id") not in (None, 0, "0")
                else None
            )
        ),
        "retweeted_status_user_id": (
            str(retweeted_status.get("user_id"))
            if retweeted_status and retweeted_status.get("user_id") is not None
            else None
        ),
        "retweeted_status_url": normalize_root_status_url(retweeted_status)
        if retweeted_status
        else None,
        "raw_json": json.dumps(status, ensure_ascii=False),
        "fetched_at_bj": _beijing_iso_now(),
    }


def _normalize_user_comment(item: dict[str, Any], user_id: str) -> dict[str, Any]:
    cid = item.get("id")
    status_obj = item.get("status") or {}
    root_url = (
        normalize_root_status_url(status_obj) if isinstance(status_obj, dict) else None
    )
    created_bj = _parse_created_at_to_beijing(item.get("created_at"))
    text = item.get("text") or item.get("description")
    return {
        "comment_id": str(cid) if cid is not None else None,
        "user_id": str(user_id),
        "created_at": item.get("created_at"),
        "created_at_bj": created_bj.replace(microsecond=0).isoformat()
        if created_bj is not None
        else None,
        "text": sanitize_xueqiu_text(text),
        "in_reply_to_comment_id": item.get("in_reply_to_comment_id"),
        "root_in_reply_to_status_id": item.get("root_in_reply_to_status_id")
        or (status_obj.get("id") if isinstance(status_obj, dict) else None),
        "root_status_url": root_url,
        "root_status_user_id": status_obj.get("user_id")
        if isinstance(status_obj, dict)
        else None,
        "root_status_id": status_obj.get("id")
        if isinstance(status_obj, dict)
        else None,
        "root_status_target": status_obj.get("target")
        if isinstance(status_obj, dict)
        else None,
        "raw_json": json.dumps(item, ensure_ascii=False),
        "fetched_at_bj": _beijing_iso_now(),
    }


def _parse_created_at_to_beijing(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:  # likely ms
            ts /= 1000.0
        try:
            return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone(
                BEIJING_TIMEZONE
            )
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            try:
                return _parse_created_at_to_beijing(int(s))
            except Exception:
                return None
        try:
            parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(BEIJING_TIMEZONE)
        except Exception:
            pass
        # Try RFC2822-like formats.
        try:
            parsed2 = parsedate_to_datetime(s)
            if parsed2.tzinfo is None:
                parsed2 = parsed2.replace(tzinfo=dt.timezone.utc)
            return parsed2.astimezone(BEIJING_TIMEZONE)
        except Exception:
            return None
    return None


def _safe_ts_bj() -> str:
    return dt.datetime.now(tz=BEIJING_TIMEZONE).strftime("%Y%m%dT%H%M%S")


def _write_html_snapshot(
    out_dir: Path, *, user_id: str, kind: str, page
) -> Optional[Path]:
    try:
        html = page.content()
    except Exception:
        return None
    if not html:
        return None
    path = out_dir / "html" / str(user_id) / f"{kind}_{_safe_ts_bj()}.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path


def _profile_url(user_id: str) -> str:
    return f"{BASE_URL}/u/{user_id}"


def _comments_url(user_id: str) -> str:
    return f"{BASE_URL}/u/{user_id}#/comments"


def _scroll_down_once(page) -> None:
    """
    Best-effort scrolling to trigger infinite-load requests.

    雪球页面可能使用不同的滚动容器；这里采用“多策略叠加”的保守方式，提高触发概率。
    """

    try:
        page.mouse.wheel(0, 2400)
    except Exception:
        pass
    try:
        page.keyboard.press("End")
    except Exception:
        pass
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        pass


def _scroll_to_top(page) -> None:
    try:
        page.evaluate("() => window.scrollTo(0, 0)")
    except Exception:
        pass


def _page_signature(page) -> str:
    try:
        obj = page.evaluate(
            """
            () => {
              const active = document.querySelector(
                '.pagination .active, .pagination__item.active, .pagination__item.current, .pagination__page.active, .pagination__current'
              );
              const article = document.querySelector('article');
              let recordHref = '';
              if (article) {
                const links = Array.from(article.querySelectorAll('a[href]'));
                const recordLink = links.find((link) => /\\/\\d+\\/\\d+/.test(String(link.getAttribute('href') || '')));
                recordHref = recordLink ? String(recordLink.getAttribute('href') || '') : '';
              }
              return {
                active_page: active ? String(active.textContent || '').trim() : '',
                first_record_href: recordHref,
                first_article_text: article ? String(article.innerText || '').slice(0, 120) : '',
              };
            }
            """
        )
    except Exception:
        return ""
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(obj or "")


def _active_pagination_page(page) -> int:
    try:
        value = page.evaluate(
            """
            () => {
              const active = document.querySelector(
                '.pagination .active, .pagination__item.active, .pagination__item.current, .pagination__page.active, .pagination__current'
              );
              return active ? String(active.textContent || '').trim() : '';
            }
            """
        )
    except Exception:
        return 0
    try:
        return int(str(value or "").strip())
    except Exception:
        return 0


def _timeline_page_from_url(url: str) -> int:
    try:
        parsed = urlparse(str(url or ""))
        value = parse_qs(parsed.query).get("page", [""])[0]
        return int(str(value or "").strip())
    except Exception:
        return 0


def _wait_for_timeline_page_change(
    page,
    *,
    stats: "_UiInterceptStats",
    previous_signature: str,
    previous_batches: int,
    target_page: int,
) -> bool:
    deadline = time.time() + UI_INTERCEPT_PAGE_TURN_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(0.5)
        current_page = _active_pagination_page(page)
        if current_page > 0:
            stats.current_page_number = int(current_page)
        if int(current_page) == int(target_page):
            _scroll_to_top(page)
            print(
                f"[{stats.kind_name}] 已跳到第 {target_page} 页。",
                file=sys.stderr,
            )
            return True
        if int(stats.captured_batches) > previous_batches:
            current_page = _active_pagination_page(page)
            if current_page > 0:
                stats.current_page_number = int(current_page)
            if int(stats.current_page_number or 0) == int(target_page):
                _scroll_to_top(page)
                print(
                    f"[{stats.kind_name}] 已跳到第 {target_page} 页，并收到新批次。",
                    file=sys.stderr,
                )
                return True
        if _page_signature(page) != previous_signature and int(
            stats.current_page_number or 0
        ) == int(target_page):
            _scroll_to_top(page)
            print(
                f"[{stats.kind_name}] 已跳到第 {target_page} 页。",
                file=sys.stderr,
            )
            return True
    return False


def _jump_to_timeline_page_and_wait(
    page, *, stats: "_UiInterceptStats", target_page: int
) -> bool:
    target = max(1, int(target_page))
    if target <= 1:
        return True
    previous_signature = _page_signature(page)
    previous_batches = int(stats.captured_batches)

    try:
        clicked = bool(
            page.evaluate(
                """
                (pageText) => {
                  const links = Array.from(document.querySelectorAll('.pagination a'));
                  const target = links.find((el) => String(el.textContent || '').trim() === pageText);
                  if (!target) return false;
                  target.scrollIntoView({ block: 'center' });
                  target.click();
                  return true;
                }
                """,
                str(target),
            )
        )
    except Exception:
        clicked = False

    if clicked and _wait_for_timeline_page_change(
        page,
        stats=stats,
        previous_signature=previous_signature,
        previous_batches=previous_batches,
        target_page=target,
    ):
        return True

    try:
        input_locator = page.locator(".pagination input").first
        if input_locator.count() == 0:
            return False
        input_locator.scroll_into_view_if_needed()
        input_locator.click(timeout=3000)
        input_locator.evaluate(
            """
            (el) => {
              el.focus();
              el.value = '';
              try {
                el.setSelectionRange(0, String(el.value || '').length);
              } catch (e) {}
            }
            """
        )
        page.keyboard.type(str(target))
        page.keyboard.press("Enter")
    except Exception:
        return False

    return _wait_for_timeline_page_change(
        page,
        stats=stats,
        previous_signature=previous_signature,
        previous_batches=previous_batches,
        target_page=target,
    )


def _click_next_page_and_wait(page, *, stats: "_UiInterceptStats") -> bool:
    print(
        f"[{stats.kind_name}] 这一页没新数据了，准备翻页；已扫到最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
        file=sys.stderr,
    )
    previous_signature = _page_signature(page)
    previous_batches = int(stats.captured_batches)

    try:
        locator = page.locator(UI_PAGINATION_NEXT_SELECTOR).first
        if locator.count() == 0:
            return False
        class_name = str(locator.get_attribute("class") or "")
        aria_disabled = (
            str(locator.get_attribute("aria-disabled") or "").strip().lower()
        )
        if "disabled" in class_name or aria_disabled == "true":
            return False
        locator.scroll_into_view_if_needed()
        locator.click(timeout=3000)
    except Exception:
        try:
            clicked = bool(
                page.evaluate(
                    """
                    (selector) => {
                      const el = document.querySelector(selector);
                      if (!el) return false;
                      const className = String(el.className || '');
                      const ariaDisabled = String(el.getAttribute('aria-disabled') || '').trim().toLowerCase();
                      if (className.includes('disabled') || ariaDisabled === 'true') {
                        return false;
                      }
                      el.scrollIntoView({ block: 'center' });
                      el.click();
                      return true;
                    }
                    """,
                    UI_PAGINATION_NEXT_SELECTOR,
                )
            )
        except Exception:
            clicked = False
        if not clicked:
            return False

    deadline = time.time() + UI_INTERCEPT_PAGE_TURN_TIMEOUT_SEC
    while time.time() < deadline:
        time.sleep(0.5)
        if int(stats.captured_batches) > previous_batches:
            current_page = _active_pagination_page(page)
            if current_page > 0:
                stats.current_page_number = int(current_page)
            _scroll_to_top(page)
            print(f"[{stats.kind_name}] 下一页已触发并收到新批次。", file=sys.stderr)
            return True
        if _page_signature(page) != previous_signature:
            current_page = _active_pagination_page(page)
            if current_page > 0:
                stats.current_page_number = int(current_page)
            _scroll_to_top(page)
            print(f"[{stats.kind_name}] 下一页已打开。", file=sys.stderr)
            return True

    print(f"[{stats.kind_name}] 下一页未打开或未产生新批次。", file=sys.stderr)
    return False


def _fast_forward_ui_batches(
    page, *, stats: "_UiInterceptStats", target_batches: int
) -> None:
    target = max(0, int(target_batches))
    if target <= 0:
        return
    if int(stats.captured_batches) >= target:
        return
    if str(stats.stage_name or "") == PROGRESS_STAGE_TIMELINE:
        target_page = max(1, int(target))
        print(
            f"[{stats.kind_name}] 发现断点，先跳到第 {target_page} 页附近。",
            file=sys.stderr,
        )
        if _jump_to_timeline_page_and_wait(page, stats=stats, target_page=target_page):
            return
        print(
            f"[{stats.kind_name}] 直接跳页没成功，改走下一页快进。",
            file=sys.stderr,
        )
    else:
        print(
            f"[{stats.kind_name}] 发现断点，先快进到第 {target + 1} 批前面。",
            file=sys.stderr,
        )
    max_steps = max(target * 2 + 4, 8)
    steps = 0
    while int(stats.captured_batches) < target and steps < max_steps:
        steps += 1
        if not _click_next_page_and_wait(page, stats=stats):
            print(
                f"[{stats.kind_name}] 快进没到目标位置，只走到了第 {stats.captured_batches} 批，后面从这里继续。",
                file=sys.stderr,
            )
            return
        time.sleep(UI_INTERCEPT_PAGE_TURN_SETTLE_SEC)
    if int(stats.captured_batches) >= target:
        print(
            f"[{stats.kind_name}] 快进完成，当前已到第 {stats.captured_batches} 批。",
            file=sys.stderr,
        )


@dataclass
class _UiInterceptStats:
    max_batches: int
    since_bj: dt.datetime
    user_id: str
    store: Any
    seen_ids: set[str]
    url_contains: str
    kind_name: str
    kind: Callable[[dict[str, Any], str], dict[str, Any]]
    extract_records: Any
    progress_store: Any = None
    since_bj_iso: str = ""
    stage_name: str = ""
    current_page_number: int = 0

    seen_urls: set[str] = field(default_factory=set)
    wrote: int = 0
    captured_batches: int = 0
    consecutive_old_batches: int = 0
    consecutive_empty_batches: int = 0
    saw_any: bool = False
    last_hit_ts: float = 0.0
    last_batch_oldest: Optional[dt.datetime] = None

    def should_stop(self) -> bool:
        if self.max_batches > 0 and self.captured_batches >= self.max_batches:
            return True
        if (
            str(self.kind_name) == PROGRESS_STAGE_TIMELINE
            and int(self.consecutive_empty_batches)
            >= UI_INTERCEPT_MAX_CONSECUTIVE_EMPTY_TIMELINE_BATCHES
        ):
            return True
        # If we've already observed sufficiently old batches a few times,
        # and no more new batches appear, the scroll loop will stop by idle logic.
        return False


@dataclass(frozen=True)
class _UiCrawlResult:
    wrote: int
    saw_any: bool
    html_path: Optional[Path]
    last_batch_oldest: Optional[dt.datetime]
    captured_batches: int


def _make_ui_response_handler(stats: _UiInterceptStats):
    def _on_response(res) -> None:
        url = ""
        try:
            url = str(res.url or "")
        except Exception:
            return
        if stats.url_contains not in url:
            return
        try:
            if int(res.status) != 200:
                return
        except Exception:
            return
        if url in stats.seen_urls:
            return
        stats.seen_urls.add(url)

        obj: Any
        try:
            obj = res.json()
        except Exception:
            return
        if not isinstance(obj, dict):
            return

        records, batch_oldest = stats.extract_records(obj)
        is_empty_batch = not bool(records)

        stats.saw_any = True
        stats.captured_batches += 1

        if str(stats.kind_name) == PROGRESS_STAGE_TIMELINE:
            if is_empty_batch:
                stats.consecutive_empty_batches += 1
            else:
                stats.consecutive_empty_batches = 0

            if (
                int(stats.consecutive_empty_batches)
                == UI_INTERCEPT_MAX_CONSECUTIVE_EMPTY_TIMELINE_BATCHES
            ):
                print(
                    f"[{stats.kind_name}] 连续 {stats.consecutive_empty_batches} 批为空，停止继续翻页/滚动（可能已到末尾或被拦截）。",
                    file=sys.stderr,
                )

            # Timeline empty batches are not treated as "effective progress" to avoid
            # endless page turns when later pages always return empty lists.
            if not is_empty_batch:
                stats.last_hit_ts = time.time()
                page_num = _timeline_page_from_url(url)
                if page_num > 0:
                    stats.current_page_number = int(page_num)
        else:
            stats.last_hit_ts = time.time()
            page_num = _timeline_page_from_url(url)
            if page_num > 0:
                stats.current_page_number = int(page_num)
        batch_newest: Optional[dt.datetime] = None
        to_write: list[dict[str, Any]] = []
        for raw in records:
            if not isinstance(raw, dict):
                continue
            created_bj = _parse_created_at_to_beijing(raw.get("created_at"))
            if created_bj is None:
                continue
            if batch_newest is None or created_bj > batch_newest:
                batch_newest = created_bj
            if created_bj < stats.since_bj:
                continue
            rec = stats.kind(raw, stats.user_id)
            rid = rec.get(stats.store.id_field)
            if not rid:
                continue
            rid_str = str(rid)
            if rid_str in stats.seen_ids:
                continue
            stats.seen_ids.add(rid_str)
            to_write.append(rec)

        if to_write:
            stats.wrote += stats.store.append_many(to_write)

        if batch_oldest is not None:
            stats.last_batch_oldest = batch_oldest
        if stats.progress_store is not None and stats.since_bj_iso and stats.stage_name:
            if not (str(stats.kind_name) == PROGRESS_STAGE_TIMELINE and is_empty_batch):
                progress_index = int(stats.captured_batches)
                progress_cursor = str(stats.captured_batches)
                if (
                    str(stats.stage_name) == PROGRESS_STAGE_TIMELINE
                    and int(stats.current_page_number or 0) > 0
                ):
                    progress_index = int(stats.current_page_number)
                    progress_cursor = str(stats.current_page_number)
                stats.progress_store.upsert(
                    since_bj_iso=str(stats.since_bj_iso),
                    stage=str(stats.stage_name),
                    status="running",
                    cursor_text=progress_cursor,
                    current_index=progress_index,
                    total_count=int(stats.max_batches)
                    if int(stats.max_batches) > 0
                    else 0,
                    detail={
                        "last_oldest_bj": batch_oldest.isoformat()
                        if batch_oldest
                        else "",
                        "last_newest_bj": batch_newest.isoformat()
                        if batch_newest
                        else "",
                    },
                )
        print(
            f"[{stats.kind_name}] 批次 {stats.captured_batches}: 原始 {len(records)} 条, 新增 {len(to_write)} 条, "
            f"日期 {_format_progress_dt(batch_newest)} -> {_format_progress_dt(batch_oldest)}",
            file=sys.stderr,
        )

        if batch_oldest is not None:
            if batch_oldest < stats.since_bj:
                stats.consecutive_old_batches += 1
            else:
                stats.consecutive_old_batches = 0

    return _on_response


def _extract_timeline_records(
    obj: dict[str, Any],
) -> tuple[list[dict[str, Any]], Optional[dt.datetime]]:
    statuses = XueqiuApi.extract_timeline_statuses(obj)
    oldest: Optional[dt.datetime] = None
    for st in statuses:
        created_bj = _parse_created_at_to_beijing(st.get("created_at"))
        if created_bj is None:
            continue
        if oldest is None or created_bj < oldest:
            oldest = created_bj
    return statuses, oldest


def _extract_comment_records(
    obj: dict[str, Any],
) -> tuple[list[dict[str, Any]], Optional[dt.datetime]]:
    items = obj.get("items") or []
    out = [it for it in items if isinstance(it, dict)]
    oldest: Optional[dt.datetime] = None
    for it in out:
        created_bj = _parse_created_at_to_beijing(it.get("created_at"))
        if created_bj is None:
            continue
        if oldest is None or created_bj < oldest:
            oldest = created_bj
    return out, oldest


def _crawl_via_ui_intercept(
    *,
    page,
    out_dir: Path,
    user_id: str,
    since_bj: dt.datetime,
    url: str,
    url_contains: str,
    max_batches: int,
    store: Any,
    seen_ids: set[str],
    normalize_fn,
    extract_records_fn,
    limiter: RateLimiter,
    kind_name: str,
    progress_store: Any = None,
    since_bj_iso: str = "",
    stage_name: str = "",
) -> _UiCrawlResult:
    """
    全自动页面浏览 + 网络拦截取数。

    返回：UI 拦截阶段的写入结果与进度信息
    """

    stats = _UiInterceptStats(
        max_batches=int(max_batches),
        since_bj=since_bj,
        user_id=str(user_id),
        store=store,
        seen_ids=seen_ids,
        url_contains=str(url_contains),
        kind_name=str(kind_name),
        kind=normalize_fn,
        extract_records=extract_records_fn,
        progress_store=progress_store,
        since_bj_iso=str(since_bj_iso or ""),
        stage_name=str(stage_name or ""),
    )
    handler = _make_ui_response_handler(stats)
    page.on("response", handler)
    try:
        resume_batches = 0
        if progress_store is not None and since_bj_iso and stage_name:
            checkpoint = progress_store.get(
                since_bj_iso=str(since_bj_iso), stage=str(stage_name)
            )
            if checkpoint and checkpoint.get("status") != "completed":
                resume_batches = int(checkpoint.get("current_index") or 0)
        print(
            f"[{kind_name}] 开始抓取：since={_format_progress_dt(since_bj)}",
            file=sys.stderr,
        )
        try:
            page.goto(url, wait_until="domcontentloaded")
        except Exception:
            pass

        idle_rounds = 0

        # Let initial network settle a bit.
        time.sleep(2.0)
        if stats.last_hit_ts <= 0:
            stats.last_hit_ts = time.time()
        current_page = _active_pagination_page(page)
        if current_page > 0:
            stats.current_page_number = int(current_page)
        if resume_batches > 0:
            _fast_forward_ui_batches(page, stats=stats, target_batches=resume_batches)

        # Avoid unbounded scrolling when the UI does not trigger target requests.
        max_rounds = (
            (max(2, int(max_batches)) * UI_INTERCEPT_ROUNDS_PER_BATCH)
            if int(max_batches) > 0
            else 4000
        )

        # Scroll loop: stop by max_batches, or by "already old enough + idle", or by hard cap.
        for _ in range(max_rounds):
            if stats.should_stop():
                break

            limiter.sleep_before_next()
            _scroll_down_once(page)
            time.sleep(1.0)

            if time.time() - stats.last_hit_ts >= UI_INTERCEPT_IDLE_SEC:
                idle_rounds += 1
            else:
                idle_rounds = 0

            if stats.consecutive_old_batches >= 2 and idle_rounds >= 2:
                print(
                    f"[{kind_name}] 已连续命中早于 since 的批次，停止继续滚动；当前最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
                    file=sys.stderr,
                )
                break
            if idle_rounds >= UI_INTERCEPT_MAX_IDLE_ROUNDS:
                if _click_next_page_and_wait(page, stats=stats):
                    idle_rounds = 0
                    stats.last_hit_ts = time.time()
                    time.sleep(UI_INTERCEPT_PAGE_TURN_SETTLE_SEC)
                    continue
                print(
                    f"[{kind_name}] 长时间没拿到数据，停止抓取；当前最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
                    file=sys.stderr,
                )
                break

        html_path: Optional[Path] = None
        if not stats.saw_any:
            html_path = _write_html_snapshot(
                out_dir, user_id=str(user_id), kind=kind_name, page=page
            )
        if progress_store is not None and since_bj_iso and stage_name:
            progress_index = int(stats.captured_batches)
            progress_cursor = str(stats.captured_batches)
            if (
                str(stage_name) == PROGRESS_STAGE_TIMELINE
                and int(stats.current_page_number or 0) > 0
            ):
                progress_index = int(stats.current_page_number)
                progress_cursor = str(stats.current_page_number)
            progress_store.mark_completed(
                since_bj_iso=str(since_bj_iso),
                stage=str(stage_name),
                cursor_text=progress_cursor,
                current_index=progress_index,
                total_count=progress_index,
                detail={
                    "last_oldest_bj": stats.last_batch_oldest.isoformat()
                    if stats.last_batch_oldest
                    else ""
                },
            )
        print(
            f"[{kind_name}] 抓取结束：拦截批次 {stats.captured_batches}，新增写入 {stats.wrote} 条，"
            f"最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
            file=sys.stderr,
        )
        return _UiCrawlResult(
            wrote=int(stats.wrote),
            saw_any=bool(stats.saw_any),
            html_path=html_path,
            last_batch_oldest=stats.last_batch_oldest,
            captured_batches=int(stats.captured_batches),
        )
    finally:
        try:
            page.off("response", handler)
        except Exception:
            pass


def _crawl_comments_via_api(
    *,
    api: XueqiuApi,
    user_id: str,
    since_bj: dt.datetime,
    max_pages: int,
    store: Any,
    seen_ids: set[str],
    progress_store: Any,
    since_bj_iso: str,
    stage_name: str,
) -> tuple[int, Optional[dt.datetime], int]:
    page_limit = (
        int(max_pages)
        if int(max_pages) > 0
        else DEFAULT_CORE_MAX_USER_COMMENTS_SCAN_PAGES
    )
    if page_limit <= 0:
        progress_store.mark_completed(
            since_bj_iso=since_bj_iso,
            stage=stage_name,
            total_count=0,
        )
        return 0, None, 0

    checkpoint = progress_store.get(since_bj_iso=since_bj_iso, stage=stage_name)
    resume_page_count = 0
    start_max_id = -1
    if checkpoint and checkpoint.get("status") != "completed":
        resume_page_count = int(checkpoint.get("current_index") or 0)
        cursor_text = str(checkpoint.get("cursor_text") or "").strip()
        if cursor_text:
            try:
                start_max_id = int(cursor_text)
            except Exception:
                start_max_id = -1
        if resume_page_count > 0:
            print(
                f"[comments-api] 发现断点，从第 {resume_page_count + 1} 批继续。上次 next_max_id={start_max_id}",
                file=sys.stderr,
            )

    remaining_pages = max(0, page_limit - resume_page_count)
    if remaining_pages == 0:
        progress_store.mark_completed(
            since_bj_iso=since_bj_iso,
            stage=stage_name,
            cursor_text=str(start_max_id),
            current_index=resume_page_count,
            total_count=page_limit,
        )
        return 0, None, resume_page_count

    print(
        f"[comments-api] 开始抓取：since={_format_progress_dt(since_bj)}",
        file=sys.stderr,
    )
    wrote = 0
    oldest_seen: Optional[dt.datetime] = None
    page_count = resume_page_count
    for next_max_id, items in api.iter_user_comments_pages(
        user_id=user_id, start_max_id=int(start_max_id), max_pages=remaining_pages
    ):
        page_count += 1
        batch_oldest: Optional[dt.datetime] = None
        batch_newest: Optional[dt.datetime] = None
        to_write: list[dict[str, Any]] = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            created_bj = _parse_created_at_to_beijing(raw.get("created_at"))
            if created_bj is None:
                continue
            if batch_newest is None or created_bj > batch_newest:
                batch_newest = created_bj
            if batch_oldest is None or created_bj < batch_oldest:
                batch_oldest = created_bj
            if oldest_seen is None or created_bj < oldest_seen:
                oldest_seen = created_bj
            if created_bj < since_bj:
                continue
            rec = _normalize_user_comment(raw, str(user_id))
            rid = rec.get(store.id_field)
            if not rid:
                continue
            rid_str = str(rid)
            if rid_str in seen_ids:
                continue
            seen_ids.add(rid_str)
            to_write.append(rec)

        if to_write:
            wrote += store.append_many(to_write)
        print(
            f"[comments-api] 批次 {page_count}: 原始 {len(items)} 条, 新增 {len(to_write)} 条, "
            f"日期 {_format_progress_dt(batch_newest)} -> {_format_progress_dt(batch_oldest)} next_max_id={next_max_id}",
            file=sys.stderr,
        )
        progress_store.upsert(
            since_bj_iso=since_bj_iso,
            stage=stage_name,
            status="running",
            cursor_text=str(next_max_id),
            current_index=page_count,
            total_count=page_limit,
            detail={
                "last_oldest_bj": batch_oldest.isoformat() if batch_oldest else "",
                "last_newest_bj": batch_newest.isoformat() if batch_newest else "",
            },
        )
        if batch_oldest is not None and batch_oldest < since_bj:
            break

    progress_store.mark_completed(
        since_bj_iso=since_bj_iso,
        stage=stage_name,
        cursor_text=str(
            start_max_id if page_count == resume_page_count else next_max_id
        ),
        current_index=page_count,
        total_count=page_limit,
        detail={"oldest_seen_bj": oldest_seen.isoformat() if oldest_seen else ""},
    )
    return wrote, oldest_seen, page_count


def _parse_since_to_beijing(since_str: str, tz_name: str) -> dt.datetime:
    tz = ZoneInfo(tz_name)
    s = str(since_str or "").strip()
    if not s:
        raise ValueError("core 模式必须提供 --since（YYYY-MM-DD 或 ISO 8601 时间）")

    # Date-only: treat as local midnight.
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        day = dt.date.fromisoformat(s)
        local_dt = dt.datetime.combine(day, dt.time.min).replace(tzinfo=tz)
        return local_dt.astimezone(BEIJING_TIMEZONE)

    # Datetime: support "Z" and timezone-less values (assume tz_name).
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception as e:
        raise ValueError(
            f"--since 解析失败：{s!r}（请用 YYYY-MM-DD 或 ISO 8601，例如 2026-03-06T00:00:00+08:00）"
        ) from e
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(BEIJING_TIMEZONE)


def _backfill_talks_since(
    api: XueqiuApi,
    user_id: str,
    since_bj: dt.datetime,
    max_talk_pages: int,
    comments_store: Any,
    talks_store: Any,
    talks_progress_store: Any,
) -> int:
    wrote = 0
    skipped = 0
    since_bj_iso = since_bj.replace(microsecond=0).isoformat()
    refs = list(comments_store.iter_comment_refs_since(since_bj_iso=since_bj_iso))
    total_refs = len(refs)

    def _save_talks_progress(idx: int, ref_obj: dict[str, Any]) -> None:
        progress_root_status_id = str(
            ref_obj.get("root_in_reply_to_status_id")
            or ref_obj.get("root_status_id")
            or ""
        )
        talks_progress_store.upsert(
            since_bj_iso=since_bj_iso,
            comment_id=str(ref_obj.get("comment_id") or ""),
            root_status_id=progress_root_status_id,
            created_at_bj=str(ref_obj.get("created_at_bj") or ""),
            current_index=int(idx),
            total_count=total_refs,
        )

    if refs:
        print(f"[talks] 开始补齐，共 {total_refs} 条评论链待检查。", file=sys.stderr)
    else:
        print("[talks] 没有需要补齐的评论链。", file=sys.stderr)
        talks_progress_store.clear(since_bj_iso=since_bj_iso)
        return 0

    checkpoint = talks_progress_store.get(since_bj_iso=since_bj_iso)
    resume_index = 0
    if checkpoint is not None:
        checkpoint_comment_id = str(checkpoint.get("comment_id") or "")
        checkpoint_root_status_id = str(checkpoint.get("root_status_id") or "")
        for idx, ref in enumerate(refs, start=1):
            ref_comment_id = str(ref.get("comment_id") or "")
            ref_root_status_id = str(
                ref.get("root_in_reply_to_status_id") or ref.get("root_status_id") or ""
            )
            if ref_comment_id != checkpoint_comment_id:
                continue
            if (
                checkpoint_root_status_id
                and ref_root_status_id != checkpoint_root_status_id
            ):
                continue
            resume_index = idx
            break
        if 0 < resume_index < total_refs:
            print(
                f"[talks] 发现断点，从第 {resume_index + 1}/{total_refs} 条继续。上次停在 comment={checkpoint_comment_id} root={checkpoint_root_status_id or '-'}",
                file=sys.stderr,
            )
        elif resume_index >= total_refs:
            print(
                "[talks] 发现断点，但当前这批已经跑完了，这次直接收尾。",
                file=sys.stderr,
            )
        else:
            print(
                "[talks] 发现旧断点，但这次没对上评论链，改为从头开始。",
                file=sys.stderr,
            )
            talks_progress_store.clear(since_bj_iso=since_bj_iso)

    for idx, ref in enumerate(refs[resume_index:], start=resume_index + 1):
        cid = ref.get("comment_id")
        root_status_id = ref.get("root_in_reply_to_status_id") or ref.get(
            "root_status_id"
        )
        if not cid or not root_status_id:
            continue
        print(
            f"[talks] {idx}/{total_refs} comment={cid} root={root_status_id} created_at={ref.get('created_at_bj') or '-'}",
            file=sys.stderr,
        )

        meta = talks_store.get_meta(
            root_status_id=str(root_status_id), comment_id=str(cid)
        )
        if meta is not None:
            try:
                max_page = int(meta.get("max_page") or 0)
                fetched_pages = int(meta.get("fetched_pages") or 0)
                truncated = bool(meta.get("truncated"))
            except Exception:
                max_page = 0
                fetched_pages = 0
                truncated = True

            # Skip already-complete snapshots under current cap.
            if (
                not truncated
                and max_page > 0
                and fetched_pages >= min(max_page, int(max_talk_pages))
            ):
                _save_talks_progress(idx, ref)
                continue
            if not truncated and max_page == 0 and fetched_pages >= int(max_talk_pages):
                _save_talks_progress(idx, ref)
                continue

        existing_obj = (
            talks_store.get_existing_obj(
                root_status_id=str(root_status_id), comment_id=str(cid)
            )
            if meta
            else None
        )
        try:
            obj = api.fetch_talks_incremental(
                root_status_id=str(root_status_id),
                comment_id=str(cid),
                max_pages=int(max_talk_pages),
                existing=existing_obj,
            )
            if isinstance(obj, dict):
                talks_store.upsert_obj(
                    root_status_id=str(root_status_id),
                    comment_id=str(cid),
                    user_id=str(user_id),
                    obj=obj,
                )
                wrote += 1
        except (ChallengeRequiredError, BlockedError):
            raise
        except Exception as e:
            skipped += 1
            print(
                f"[talks-skip] comment={cid} root={root_status_id} 这一条抓失败了，先跳过后面继续：{e}",
                file=sys.stderr,
            )
        _save_talks_progress(idx, ref)

    talks_progress_store.clear(since_bj_iso=since_bj_iso)
    if skipped:
        print(
            f"[talks] 补齐结束，新增/更新 {wrote} 条，跳过失败 {skipped} 条。",
            file=sys.stderr,
        )
    else:
        print(f"[talks] 补齐结束，新增/更新 {wrote} 条。", file=sys.stderr)
    return wrote


def _load_user_ids_from_file(path: Path) -> list[str]:
    file_path = Path(path)
    if not file_path.is_file():
        raise RuntimeError(f"用户列表文件不存在：{file_path}")

    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        raise RuntimeError(f"读取用户列表文件失败：{file_path}，原因：{e}") from e

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

    if not user_ids:
        raise RuntimeError(f"用户列表文件里没有可用的用户ID：{file_path}")
    return user_ids


def _resolve_target_user_ids(args: argparse.Namespace) -> list[str]:
    user_ids = _load_user_ids_from_file(Path(args.user_list_file))
    start_user_id = str(getattr(args, "start_from_user", "") or "").strip()
    if not start_user_id:
        return user_ids

    try:
        start_index = user_ids.index(start_user_id)
    except ValueError as e:
        raise RuntimeError(
            f"--start-from-user 指定的用户不在列表文件里：{start_user_id}"
        ) from e

    if start_index > 0:
        print(
            f"从用户 {start_user_id} 开始继续，跳过前面 {start_index} 个用户。",
            file=sys.stderr,
        )
    return user_ids[start_index:]


def _resolve_db_path(*, args: argparse.Namespace, out_dir: Path) -> Path:
    if args.db is not None:
        return Path(args.db)
    return out_dir / DEFAULT_BATCH_DB_BASENAME


def _build_browser_profiles_root(out_dir: Path) -> Path:
    stamp = dt.datetime.now(tz=BEIJING_TIMEZONE).strftime(
        BROWSER_PROFILE_TIMESTAMP_FORMAT
    )
    path = out_dir / BROWSER_PROFILE_RUNS_DIRNAME / stamp
    path.mkdir(parents=True, exist_ok=True)
    return path


def _build_user_browser_profile_dir(
    *, profiles_root: Path, index: int, user_id: str
) -> Path:
    return profiles_root / f"{int(index):03d}_{str(user_id)}"


def _copy_browser_profile_dir(src: Path, dst: Path) -> None:
    source = Path(src)
    target = Path(dst)
    source.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(
        source,
        target,
        ignore=shutil.ignore_patterns(*BROWSER_PROFILE_COPY_IGNORE_PATTERNS),
    )


def _cleanup_browser_profile_dir(path: Path) -> None:
    shutil.rmtree(path)


def _run_single_user(
    *,
    args: argparse.Namespace,
    db: SqliteDb,
    db_path: Path,
    out_dir: Path,
    session,
    user_id: str,
) -> int:
    incremental = bool(getattr(args, "incremental", False))
    api_cfg = ApiConfig(
        min_delay_sec=args.min_delay,
        jitter_sec=args.jitter,
        max_retries=args.max_retries,
        max_consecutive_blocks=args.max_consecutive_blocks,
    )
    timeline_store = SqliteMergedStatusesStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )
    comment_store = SqliteMergedCommentsStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )
    talks_store = SqliteMergedTalksStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )
    talks_progress_store = SqliteTalksProgressStore(db=db, user_id=str(user_id))
    crawl_progress_store = SqliteCrawlProgressStore(db=db, user_id=str(user_id))

    # Cross-run de-dup relies on SQLite unique key (merge_key). Keep in-run sets only.
    seen_status_ids: set[str] = set()
    seen_comment_ids: set[str] = set()

    ui_page = session.ui_page
    prefer_page_fetch = bool(session.prefer_page_fetch)
    data_page = ui_page if prefer_page_fetch else session.page
    data_api = XueqiuApi(data_page, api_cfg, prefer_page_fetch=prefer_page_fetch)
    # Keep api_page around for talks backfill and future extensions.
    detail_api = XueqiuApi(session.page, api_cfg)
    detail_status_line_cache: dict[tuple[str, str, str, str], Optional[str]] = {}
    user_log_prefix = f"[user {user_id}]"

    def fetch_detail_with_headless_worker(
        *, status_id: str, source_status_url: str, status_url: str, status_user_id: str
    ) -> tuple[Optional[str], Optional[str], dict[str, Any]]:
        command = [
            sys.executable,
            "-m",
            "xueqiu_crawler.detail_retry_worker",
            "--status-id",
            str(status_id),
            "--min-delay",
            str(args.min_delay),
            "--jitter",
            str(args.jitter),
            "--max-retries",
            str(args.max_retries),
            "--max-consecutive-blocks",
            str(args.max_consecutive_blocks),
        ]
        if source_status_url:
            command.extend(["--source-status-url", str(source_status_url)])
        if status_url:
            command.extend(["--status-url", str(status_url)])
        if status_user_id:
            command.extend(["--status-user-id", str(status_user_id)])

        try:
            env = os.environ.copy()
            src_root = str(Path(__file__).resolve().parents[1])
            existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
            env["PYTHONPATH"] = (
                src_root
                if not existing_pythonpath
                else f"{src_root}{os.pathsep}{existing_pythonpath}"
            )
            proc = subprocess.run(
                command,
                cwd=str(Path(__file__).resolve().parents[2]),
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
            stdout_text = str(proc.stdout or "").strip()
            stderr_text = str(proc.stderr or "").strip()
            if stderr_text:
                print(
                    f"[detail] 无头补抓 stderr status_id={status_id}: {stderr_text}",
                    file=sys.stderr,
                )
            if not stdout_text:
                return None, "无头补抓没有返回结果", {}
            try:
                payload = json.loads(stdout_text)
            except Exception:
                return None, f"无头补抓结果解析失败：{stdout_text}", {}

            line = payload.get("line")
            failure_reason = payload.get("failure_reason")
            debug = payload.get("debug")
            debug_dict: dict[str, Any] = debug if isinstance(debug, dict) else {}
            stealth_mode = str(payload.get("stealth_mode") or "").strip()
            if stealth_mode:
                print(
                    f"[detail] 无头补抓 stealth={stealth_mode} status_id={status_id}",
                    file=sys.stderr,
                )
            if proc.returncode != 0 and not line and not failure_reason:
                return None, "无头补抓子进程失败", debug_dict
            return (
                str(line).strip() if line not in (None, "") else None,
                str(failure_reason).strip()
                if failure_reason not in (None, "")
                else None,
                debug_dict,
            )
        except Exception as exc:
            return None, f"无头补抓异常：{exc}", {}

    def _short_debug_value(value: Any, max_len: int = 180) -> str:
        text = str(value or "").replace("\n", " ").strip()
        if len(text) <= max_len:
            return text
        return f"{text[:max_len]}..."

    def _log_detail_debug(*, status_id: str, debug: dict[str, Any]) -> None:
        candidates = debug.get("candidate_urls")
        attempts = debug.get("attempts")
        if not isinstance(candidates, list):
            candidates = []
        if not isinstance(attempts, list):
            attempts = []

        last_by_url: dict[str, dict[str, Any]] = {}
        seen_order: list[str] = []
        for item in attempts:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            if url not in last_by_url:
                seen_order.append(url)
            last_by_url[url] = item

        ordered_urls = [
            str(url).strip() for url in candidates if str(url or "").strip()
        ] or seen_order
        if not ordered_urls:
            return

        print(
            f"[detail] Headless detail fetch attempted URLs status_id={status_id}:",
            file=sys.stderr,
        )
        for url in ordered_urls:
            item = last_by_url.get(url, {})
            status = item.get("status")
            final_url = item.get("final_url")
            page_title = item.get("page_title")
            issue_reason = item.get("issue_reason") or item.get("issue_code") or ""
            print(
                f"[detail]  url={_short_debug_value(url)} status={status} final_url={_short_debug_value(final_url)} title={_short_debug_value(page_title)} reason={_short_debug_value(issue_reason)}",
                file=sys.stderr,
            )

    def resolve_status_line(
        status_id: str,
        source_status_url: str = "",
        status_url: str = "",
        status_user_id: str = "",
    ) -> Optional[str]:
        sid = str(status_id or "").strip()
        if not sid:
            return None
        cache_key = (
            sid,
            str(source_status_url or "").strip(),
            str(status_url or "").strip(),
            str(status_user_id or "").strip(),
        )
        if cache_key in detail_status_line_cache:
            return detail_status_line_cache[cache_key]
        print(f"[detail] 尝试补抓原帖全文 status_id={sid}", file=sys.stderr)
        line, failure_reason, debug = fetch_detail_with_headless_worker(
            status_id=sid,
            source_status_url=str(source_status_url or "").strip(),
            status_url=str(status_url or "").strip(),
            status_user_id=str(status_user_id or "").strip(),
        )
        if line:
            print(f"[detail] 原帖全文补抓成功 status_id={sid}", file=sys.stderr)
        else:
            print(
                f"[detail] 原帖全文补抓失败 status_id={sid}，原因：{failure_reason or '页面没拿到正文'}",
                file=sys.stderr,
            )
            if debug:
                _log_detail_debug(status_id=sid, debug=debug)
        detail_status_line_cache[cache_key] = line
        return line

    if args.skip_login_check:
        ui_page.goto(BASE_URL, wait_until="domcontentloaded")
    else:
        try:
            _ensure_logged_in_ui(ui_page, int(args.login_timeout_sec))
        except Exception as e:
            print(f"登录态等待失败：{e}", file=sys.stderr)
            return 2

    if args.mode != "core":
        print("当前仅支持 core 模式。", file=sys.stderr)
        return 2

    checkpoint_store: Optional[SqliteCrawlCheckpointStore] = None
    checkpoint_existed = False
    if incremental:
        checkpoint_store = SqliteCrawlCheckpointStore(db=db)
        checkpoint_existed = checkpoint_store.get(user_id=str(user_id)) is not None

    tz_name = str(args.tz)
    try:
        if incremental:
            since_bj = _resolve_incremental_since_bj(
                args=args, db=db, user_id=str(user_id), tz_name=tz_name
            )
        else:
            since_bj = _parse_since_to_beijing(args.since, tz_name=tz_name)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 2
    since_bj_iso = since_bj.replace(microsecond=0).isoformat()
    if incremental and checkpoint_store is not None and (not checkpoint_existed):
        checkpoint_store.upsert(
            user_id=str(user_id),
            checkpoint_bj_iso=str(since_bj_iso),
            detail={
                "seeded": True,
                "seed_checkpoint_bj_iso": str(since_bj_iso),
                "seed_reason": "missing_checkpoint",
            },
        )

    # core 模式默认尽量把“查看对话”也补齐；如果你只要主干数据可用 --no-talks 关闭。
    want_talks = bool(args.with_talks) or (not bool(args.no_talks))

    if not incremental:
        if crawl_progress_store.is_completed(
            since_bj_iso=since_bj_iso, stage=PROGRESS_STAGE_FINALIZE
        ):
            print(
                f"{user_log_prefix} 上次已经完整跑完，这次直接跳过。", file=sys.stderr
            )
            return 0
        if _user_has_entry_rows(db=db, user_id=str(user_id)) and not _user_has_raw_rows(
            db=db, user_id=str(user_id)
        ):
            crawl_progress_store.mark_completed(
                since_bj_iso=since_bj_iso,
                stage=PROGRESS_STAGE_FINALIZE,
                detail={"inferred_from_entries": True},
            )
            print(f"{user_log_prefix} 已发现完整结果，这次直接跳过。", file=sys.stderr)
            return 0

    had_blocked = False
    wrote_statuses = 0
    wrote_comments = 0
    wrote_talks = 0
    timeline_stage_ok = True
    comments_stage_ok = True
    talks_stage_ok = True
    limiter = RateLimiter(float(args.min_delay), float(args.jitter))

    if (not incremental) and crawl_progress_store.is_completed(
        since_bj_iso=since_bj_iso, stage=PROGRESS_STAGE_TIMELINE
    ):
        print(f"{user_log_prefix} 时间线上次已经跑完，这次跳过。", file=sys.stderr)
    else:
        timeline_result = _crawl_via_ui_intercept(
            page=ui_page,
            out_dir=out_dir,
            user_id=str(user_id),
            since_bj=since_bj,
            url=_profile_url(str(user_id)),
            url_contains="/v4/statuses/user_timeline.json",
            max_batches=int(args.max_timeline_pages),
            store=timeline_store,
            seen_ids=seen_status_ids,
            normalize_fn=_normalize_timeline_status,
            extract_records_fn=_extract_timeline_records,
            limiter=limiter,
            kind_name="timeline",
            progress_store=crawl_progress_store,
            since_bj_iso=since_bj_iso,
            stage_name=PROGRESS_STAGE_TIMELINE,
        )
        wrote_statuses = int(timeline_result.wrote)
        timeline_stage_ok = bool(timeline_result.saw_any)
        if wrote_statuses:
            print(
                f"{user_log_prefix} 时间线新增写入 {wrote_statuses} 条到 SQLite：{db_path}"
            )
        elif not timeline_result.saw_any and timeline_result.html_path:
            print(
                f"{user_log_prefix} 未拦截到时间线 JSON，已降级保存 HTML 快照：{timeline_result.html_path}",
                file=sys.stderr,
            )
            if incremental:
                print(
                    f"{user_log_prefix} [incremental] Timeline JSON not intercepted; checkpoint will not advance.",
                    file=sys.stderr,
                )

    if (not incremental) and crawl_progress_store.is_completed(
        since_bj_iso=since_bj_iso, stage=PROGRESS_STAGE_COMMENTS
    ):
        print(f"{user_log_prefix} 回复上次已经跑完，这次跳过。", file=sys.stderr)
        comments_pages = 0
    elif prefer_page_fetch:
        comments_fetch_error: Optional[Exception] = None
        try:
            wrote_comments, _comments_oldest, comments_pages = _crawl_comments_via_api(
                api=data_api,
                user_id=str(user_id),
                since_bj=since_bj,
                max_pages=int(args.max_comment_pages),
                store=comment_store,
                seen_ids=seen_comment_ids,
                progress_store=crawl_progress_store,
                since_bj_iso=since_bj_iso,
                stage_name=PROGRESS_STAGE_COMMENTS,
            )
        except ChallengeRequiredError as e:
            try:
                target_url = getattr(e, "final_url", "") or data_api.build_url(
                    "/statuses/user/comments.json",
                    {
                        "user_id": str(user_id),
                        "size": USER_COMMENTS_PAGE_SIZE,
                        "max_id": -1,
                    },
                )
                _wait_for_waf_challenge(
                    ui_page,
                    data_api,
                    user_id,
                    int(args.login_timeout_sec),
                    target_url,
                    navigate_to_blocked_url=not prefer_page_fetch,
                )
                wrote_comments, _comments_oldest, comments_pages = (
                    _crawl_comments_via_api(
                        api=data_api,
                        user_id=str(user_id),
                        since_bj=since_bj,
                        max_pages=int(args.max_comment_pages),
                        store=comment_store,
                        seen_ids=seen_comment_ids,
                        progress_store=crawl_progress_store,
                        since_bj_iso=since_bj_iso,
                        stage_name=PROGRESS_STAGE_COMMENTS,
                    )
                )
            except Exception as e2:
                comments_stage_ok = False
                had_blocked = True
                wrote_comments, _comments_oldest, comments_pages = 0, None, 0
                print(f"{user_log_prefix} 回复抓取被拦截：{e2}", file=sys.stderr)
        except BlockedError as e:
            comments_stage_ok = False
            had_blocked = True
            wrote_comments, _comments_oldest, comments_pages = 0, None, 0
            print(f"{user_log_prefix} 回复接口当前不可用：{e}", file=sys.stderr)
        except Exception as e:
            comments_stage_ok = False
            comments_fetch_error = e
            wrote_comments, _comments_oldest, comments_pages = 0, None, 0
        if wrote_comments:
            print(
                f"{user_log_prefix} 回复新增写入 {wrote_comments} 条到 SQLite：{db_path}"
            )
        elif comments_fetch_error is not None:
            html2 = _write_html_snapshot(
                out_dir,
                user_id=str(user_id),
                kind="comments",
                page=ui_page,
            )
            if html2:
                print(
                    f"{user_log_prefix} 回复接口重试后还是不对：{comments_fetch_error}；已保存 HTML 快照：{html2}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"{user_log_prefix} 回复接口重试后还是不对：{comments_fetch_error}",
                    file=sys.stderr,
                )
        elif comments_pages == 0:
            html2 = _write_html_snapshot(
                out_dir,
                user_id=str(user_id),
                kind="comments",
                page=ui_page,
            )
            if html2:
                print(
                    f"{user_log_prefix} 未获取到回复 JSON，已降级保存 HTML 快照：{html2}",
                    file=sys.stderr,
                )
    else:
        comments_result = _crawl_via_ui_intercept(
            page=ui_page,
            out_dir=out_dir,
            user_id=str(user_id),
            since_bj=since_bj,
            url=_comments_url(str(user_id)),
            url_contains="/statuses/user/comments.json",
            max_batches=int(args.max_comment_pages),
            store=comment_store,
            seen_ids=seen_comment_ids,
            normalize_fn=_normalize_user_comment,
            extract_records_fn=_extract_comment_records,
            limiter=limiter,
            kind_name="comments",
        )
        wrote_comments = int(comments_result.wrote)
        comments_pages = int(comments_result.captured_batches)
        comments_stage_ok = bool(comments_result.saw_any)
        crawl_progress_store.mark_completed(
            since_bj_iso=since_bj_iso,
            stage=PROGRESS_STAGE_COMMENTS,
            current_index=int(comments_result.captured_batches),
            total_count=int(comments_result.captured_batches),
            detail={
                "last_oldest_bj": comments_result.last_batch_oldest.isoformat()
                if comments_result.last_batch_oldest
                else ""
            },
        )
        if wrote_comments:
            print(
                f"{user_log_prefix} 回复新增写入 {wrote_comments} 条到 SQLite：{db_path}"
            )
        elif not comments_result.saw_any and comments_result.html_path:
            print(
                f"{user_log_prefix} 未拦截到回复 JSON，已降级保存 HTML 快照：{comments_result.html_path}",
                file=sys.stderr,
            )
            if incremental:
                print(
                    f"{user_log_prefix} [incremental] Comments JSON not intercepted; checkpoint will not advance.",
                    file=sys.stderr,
                )

    if (
        want_talks
        and (not incremental)
        and crawl_progress_store.is_completed(
            since_bj_iso=since_bj_iso, stage=PROGRESS_STAGE_TALKS
        )
    ):
        print(f"{user_log_prefix} 查看对话上次已经跑完，这次跳过。", file=sys.stderr)
    elif want_talks:
        talks_api = detail_api
        try:
            wrote_talks = _backfill_talks_since(
                api=talks_api,
                user_id=str(user_id),
                since_bj=since_bj,
                max_talk_pages=int(args.max_talk_pages),
                comments_store=comment_store,
                talks_store=talks_store,
                talks_progress_store=talks_progress_store,
            )
            crawl_progress_store.mark_completed(
                since_bj_iso=since_bj_iso,
                stage=PROGRESS_STAGE_TALKS,
            )
        except ChallengeRequiredError as e:
            try:
                _wait_for_waf_challenge(
                    ui_page,
                    talks_api,
                    user_id,
                    int(args.login_timeout_sec),
                    getattr(e, "final_url", e.url),
                )
                wrote_talks = _backfill_talks_since(
                    api=talks_api,
                    user_id=str(user_id),
                    since_bj=since_bj,
                    max_talk_pages=int(args.max_talk_pages),
                    comments_store=comment_store,
                    talks_store=talks_store,
                    talks_progress_store=talks_progress_store,
                )
                crawl_progress_store.mark_completed(
                    since_bj_iso=since_bj_iso,
                    stage=PROGRESS_STAGE_TALKS,
                )
            except Exception as e2:
                talks_stage_ok = False
                had_blocked = True
                print(f"{user_log_prefix} 查看对话抓取被拦截：{e2}", file=sys.stderr)
        except BlockedError as e:
            talks_stage_ok = False
            had_blocked = True
            print(f"{user_log_prefix} 查看对话抓取被拦截：{e}", file=sys.stderr)
        except Exception as e:
            talks_stage_ok = False
            had_blocked = True
            print(f"{user_log_prefix} 查看对话抓取失败：{e}", file=sys.stderr)
        if wrote_talks:
            print(
                f"{user_log_prefix} 查看对话新增/补齐写入 {wrote_talks} 份到 SQLite：{db_path}"
            )
    else:
        crawl_progress_store.mark_completed(
            since_bj_iso=since_bj_iso,
            stage=PROGRESS_STAGE_TALKS,
            detail={"skipped": True},
        )

    wrote_total = wrote_statuses + wrote_comments + wrote_talks
    if incremental:
        wrote_total = wrote_statuses + wrote_comments + wrote_talks
        final_entries = 0
        finalize_detail: dict[str, Any] = {
            "skipped": True,
            "no_new_raw_rows": True,
            "wrote_raw_total": int(wrote_total),
        }
        if wrote_total > 0:
            final_entries = rebuild_user_entries_from_raw_records(
                db=db,
                user_id=str(user_id),
                resolve_status_line=resolve_status_line,
            )
            finalize_detail = {
                "final_entries": int(final_entries),
                "from_raw_records": True,
                "wrote_raw_total": int(wrote_total),
            }
            if final_entries:
                print(
                    f"{user_log_prefix} 最终展示记录写入 {final_entries} 条到 SQLite：{db_path}"
                )
        crawl_progress_store.mark_completed(
            since_bj_iso=since_bj_iso,
            stage=PROGRESS_STAGE_FINALIZE,
            current_index=1,
            total_count=1,
            detail=finalize_detail,
        )
    else:
        already_finalized = crawl_progress_store.is_completed(
            since_bj_iso=since_bj_iso, stage=PROGRESS_STAGE_FINALIZE
        )
        should_finalize = wrote_total > 0 or (not already_finalized)
        if should_finalize:
            final_entries = rebuild_user_entries_from_raw_records(
                db=db,
                user_id=str(user_id),
                resolve_status_line=resolve_status_line,
            )
            crawl_progress_store.mark_completed(
                since_bj_iso=since_bj_iso,
                stage=PROGRESS_STAGE_FINALIZE,
                current_index=1,
                total_count=1,
                detail={
                    "final_entries": int(final_entries),
                    "from_raw_records": True,
                    "wrote_raw_total": int(wrote_total),
                },
            )
            if final_entries:
                print(
                    f"{user_log_prefix} 最终展示记录写入 {final_entries} 条到 SQLite：{db_path}"
                )

    if had_blocked and (wrote_statuses + wrote_comments + wrote_talks == 0):
        print(
            "提示：当前会话下接口暂时不可用。这可能是真正的风控挑战页，也可能是非 JSON/HTML 拦截页。"
            "如果程序明确提示需要手动验证，请在 UI 标签页处理后重试；否则优先检查当前会话是否仍能稳定返回 JSON。",
            file=sys.stderr,
        )
        return 2

    if incremental:
        checkpoint_blockers: list[str] = []
        if had_blocked:
            checkpoint_blockers.append("blocked")
        if not timeline_stage_ok:
            checkpoint_blockers.append("timeline_no_json")
        if not comments_stage_ok:
            checkpoint_blockers.append("comments_failed")
        if want_talks and (not talks_stage_ok):
            checkpoint_blockers.append("talks_failed")

        should_advance_checkpoint = not checkpoint_blockers
        if should_advance_checkpoint:
            checkpoint_bj_iso = _max_raw_created_at_bj_iso(db=db, user_id=str(user_id))
            if checkpoint_bj_iso:
                assert checkpoint_store is not None
                checkpoint_store.upsert(
                    user_id=str(user_id),
                    checkpoint_bj_iso=checkpoint_bj_iso,
                    detail={
                        "since_bj_iso": str(since_bj_iso),
                        "wrote_raw_total": int(
                            wrote_statuses + wrote_comments + wrote_talks
                        ),
                        "advanced": True,
                    },
                )
        else:
            print(
                f"{user_log_prefix} [incremental] Checkpoint not advanced (reasons: {', '.join(checkpoint_blockers)}).",
                file=sys.stderr,
            )
    return 0


def _prepare_base_browser_profile(*, args: argparse.Namespace, browser_cfg) -> None:
    from .browser import BrowserSession

    with BrowserSession(browser_cfg) as session:
        ui_page = session.ui_page
        if args.skip_login_check:
            ui_page.goto(BASE_URL, wait_until="domcontentloaded")
            return
        _ensure_logged_in_ui(ui_page, int(args.login_timeout_sec))
        time.sleep(BASE_PROFILE_COPY_SETTLE_SEC)


def _crawl_timeline_one_page_via_http_api(
    *,
    api,
    user_id: str,
    since_bj: dt.datetime,
    store: Any,
    seen_ids: set[str],
) -> int:
    obj = api.fetch_timeline_first_page(str(user_id))
    statuses = XueqiuApi.extract_timeline_statuses(obj if isinstance(obj, dict) else {})
    batch_oldest: Optional[dt.datetime] = None
    batch_newest: Optional[dt.datetime] = None
    to_write: list[dict[str, Any]] = []
    for raw in statuses:
        if not isinstance(raw, dict):
            continue
        created_bj = _parse_created_at_to_beijing(raw.get("created_at"))
        if created_bj is None:
            continue
        if batch_newest is None or created_bj > batch_newest:
            batch_newest = created_bj
        if batch_oldest is None or created_bj < batch_oldest:
            batch_oldest = created_bj
        if created_bj < since_bj:
            continue
        rec = _normalize_timeline_status(raw, str(user_id))
        rid = rec.get(store.id_field)
        if not rid:
            continue
        rid_str = str(rid)
        if rid_str in seen_ids:
            continue
        seen_ids.add(rid_str)
        to_write.append(rec)
    inserted = store.append_many(to_write) if to_write else 0
    print(
        f"[timeline-http] page=1 原始 {len(statuses)} 条, 待写入 {len(to_write)} 条, 实际写入 {inserted} 条, "
        f"日期 {_format_progress_dt(batch_newest)} -> {_format_progress_dt(batch_oldest)}",
        file=sys.stderr,
    )
    return int(inserted)


def _crawl_comments_one_page_via_http_api(
    *,
    api,
    user_id: str,
    since_bj: dt.datetime,
    store: Any,
    seen_ids: set[str],
) -> tuple[int, list[dict[str, Any]]]:
    next_max_id, items = api.fetch_user_comments_first_page(str(user_id))
    batch_oldest: Optional[dt.datetime] = None
    batch_newest: Optional[dt.datetime] = None
    candidates: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        created_bj = _parse_created_at_to_beijing(raw.get("created_at"))
        if created_bj is None:
            continue
        if batch_newest is None or created_bj > batch_newest:
            batch_newest = created_bj
        if batch_oldest is None or created_bj < batch_oldest:
            batch_oldest = created_bj
        if created_bj < since_bj:
            continue
        rec = _normalize_user_comment(raw, str(user_id))
        rid = rec.get(store.id_field)
        if not rid:
            continue
        candidates.append(rec)

    existing_merge_keys: set[str] = set()
    if candidates:
        merge_keys = [
            f"{MERGE_KEY_COMMENT_PREFIX}{str(r.get('comment_id'))}"
            for r in candidates
            if r.get("comment_id")
        ]
        merge_keys = [mk for mk in merge_keys if mk]
        if merge_keys:
            placeholders = ",".join(["?"] * len(merge_keys))
            try:
                table_name = str(getattr(store, "table_name", "merged_records"))
                cur = store.db.conn.execute(
                    f"""
                    SELECT merge_key
                    FROM {table_name}
                    WHERE merge_key IN ({placeholders})
                    """,
                    merge_keys,
                )
                existing_merge_keys = {str(row[0] or "") for row in cur}
            except Exception:
                existing_merge_keys = set()

    to_write: list[dict[str, Any]] = []
    refs: list[dict[str, Any]] = []
    existing_count = 0
    for rec in candidates:
        rid = rec.get(store.id_field)
        if not rid:
            continue
        rid_str = str(rid)
        if rid_str in seen_ids:
            continue
        seen_ids.add(rid_str)
        mk = f"{MERGE_KEY_COMMENT_PREFIX}{rid_str}"
        refs.append(
            {
                "comment_id": rec.get("comment_id"),
                "root_in_reply_to_status_id": rec.get("root_in_reply_to_status_id"),
                "root_status_id": rec.get("root_status_id"),
                "created_at_bj": rec.get("created_at_bj"),
            }
        )
        if mk in existing_merge_keys:
            existing_count += 1
            continue
        to_write.append(rec)

    inserted = store.append_many(to_write) if to_write else 0
    print(
        f"[comments-http] max_id=-1 next_max_id={next_max_id} 原始 {len(items)} 条, 待写入 {len(to_write)} 条, "
        f"已存在 {existing_count} 条, 实际写入 {inserted} 条, "
        f"日期 {_format_progress_dt(batch_newest)} -> {_format_progress_dt(batch_oldest)}",
        file=sys.stderr,
    )
    return int(inserted), refs


def _backfill_talks_for_comment_refs(
    *,
    api,
    user_id: str,
    refs: list[dict[str, Any]],
    max_talk_pages: int,
    talks_store: Any,
) -> int:
    total = len(refs)
    wrote = 0
    skipped = 0

    def _talks_signature(
        obj: Optional[dict[str, Any]],
    ) -> tuple[int, tuple[tuple[int, tuple[str, ...]], ...]]:
        """
        Compare only stable identifiers (page num + comment ids).
        This avoids rewriting SQLite when the talks snapshot has no new replies.
        """

        if not isinstance(obj, dict):
            return 0, tuple()
        try:
            max_page_i = int(obj.get("max_page") or 0)
        except Exception:
            max_page_i = 0
        pages_obj = obj.get("pages")
        if not isinstance(pages_obj, list):
            return max_page_i, tuple()
        page_sigs: list[tuple[int, tuple[str, ...]]] = []
        for page in pages_obj:
            if not isinstance(page, dict):
                continue
            try:
                page_num = int(page.get("page") or 0)
            except Exception:
                continue
            if page_num <= 0:
                continue
            comments_obj = page.get("comments")
            comment_ids: list[str] = []
            if isinstance(comments_obj, list):
                for c in comments_obj:
                    if not isinstance(c, dict):
                        continue
                    cid = c.get("id") or c.get("comment_id")
                    if cid in (None, "", 0, "0"):
                        continue
                    comment_ids.append(str(cid))
            page_sigs.append((page_num, tuple(comment_ids)))
        page_sigs.sort(key=lambda item: item[0])
        return max_page_i, tuple(page_sigs)

    def _talks_changed(
        existing: Optional[dict[str, Any]], current: Optional[dict[str, Any]]
    ) -> bool:
        if existing is None:
            return True
        return _talks_signature(existing) != _talks_signature(current)

    for idx, ref in enumerate(refs, start=1):
        cid = str(ref.get("comment_id") or "").strip()
        root_status_id = str(
            ref.get("root_in_reply_to_status_id") or ref.get("root_status_id") or ""
        ).strip()
        if not cid or not root_status_id:
            continue

        print(
            f"[talks-http] {idx}/{total} 开始补齐 root_status_id={root_status_id} comment_id={cid}",
            file=sys.stderr,
        )
        started = time.monotonic()
        existing_obj = talks_store.get_existing_obj(
            root_status_id=root_status_id,
            comment_id=cid,
        )
        obj = api.fetch_talks_incremental(
            root_status_id=root_status_id,
            comment_id=cid,
            max_pages=int(max_talk_pages),
            existing=existing_obj,
        )
        if isinstance(obj, dict):
            if not _talks_changed(existing_obj, obj):
                skipped += 1
                elapsed = time.monotonic() - started
                try:
                    fetched_pages2 = int(obj.get("fetched_pages") or 0)
                except Exception:
                    fetched_pages2 = 0
                try:
                    max_page2 = int(obj.get("max_page") or 0)
                except Exception:
                    max_page2 = 0
                print(
                    f"[talks-http] {idx}/{total} 无新增 replies（pages={fetched_pages2} max_page={max_page2}），跳过写入，耗时 {elapsed:.1f}s",
                    file=sys.stderr,
                )
                continue
            talks_store.upsert_obj(
                root_status_id=root_status_id,
                comment_id=cid,
                user_id=str(user_id),
                obj=obj,
            )
            wrote += 1
            elapsed = time.monotonic() - started
            try:
                fetched_pages2 = int(obj.get("fetched_pages") or 0)
            except Exception:
                fetched_pages2 = 0
            try:
                max_page2 = int(obj.get("max_page") or 0)
            except Exception:
                max_page2 = 0
            truncated2 = bool(obj.get("truncated"))
            print(
                f"[talks-http] {idx}/{total} 完成 pages={fetched_pages2} max_page={max_page2} truncated={int(truncated2)} 耗时 {elapsed:.1f}s",
                file=sys.stderr,
            )
        else:
            print(f"[talks-http] {idx}/{total} 返回非对象，跳过写入", file=sys.stderr)
    if total:
        print(
            f"[talks-http] 本轮补齐结束：写入 {wrote} 份，跳过 {skipped} 份（refs={total} max_pages={int(max_talk_pages)}）",
            file=sys.stderr,
        )
    return wrote


def _run_single_user_incremental_http(
    *,
    args: argparse.Namespace,
    db: SqliteDb,
    db_path: Path,
    out_dir: Path,
    user_id: str,
    since_bj: dt.datetime,
) -> int:
    from .http_api import XueqiuHttpApi

    api_cfg = ApiConfig(
        min_delay_sec=args.min_delay,
        jitter_sec=args.jitter,
        max_retries=args.max_retries,
        max_consecutive_blocks=args.max_consecutive_blocks,
        http_debug=bool(getattr(args, "http_debug", False)),
    )
    timeline_store = SqliteMergedStatusesStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )
    comment_store = SqliteMergedCommentsStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )
    talks_store = SqliteMergedTalksStore(
        db=db, user_id=str(user_id), table_name=RAW_TABLE_NAME
    )

    seen_status_ids: set[str] = set()
    seen_comment_ids: set[str] = set()

    user_log_prefix = f"[user {user_id}]"
    want_talks = bool(args.with_talks) or (not bool(args.no_talks))
    user_started = time.monotonic()

    try:
        api = XueqiuHttpApi.from_env(api_cfg)
    except Exception as e:
        print(f"{user_log_prefix} HTTP 模式初始化失败：{e}", file=sys.stderr)
        return 2

    wrote_statuses = 0
    wrote_comments = 0
    wrote_talks = 0
    had_blocked = False
    had_failure = False
    refs: list[dict[str, Any]] = []

    print(f"{user_log_prefix} timeline HTTP 开始抓取 page=1", file=sys.stderr)
    timeline_started = time.monotonic()
    try:
        wrote_statuses = _crawl_timeline_one_page_via_http_api(
            api=api,
            user_id=str(user_id),
            since_bj=since_bj,
            store=timeline_store,
            seen_ids=seen_status_ids,
        )
    except ChallengeRequiredError as e:
        had_blocked = True
        had_failure = True
        print(f"{user_log_prefix} timeline HTTP 被风控拦截：{e}", file=sys.stderr)
    except BlockedError as e:
        had_blocked = True
        had_failure = True
        print(f"{user_log_prefix} timeline HTTP 不可用：{e}", file=sys.stderr)
    except Exception as e:
        had_failure = True
        print(f"{user_log_prefix} timeline HTTP 抓取失败：{e}", file=sys.stderr)
    timeline_elapsed = time.monotonic() - timeline_started
    print(
        f"{user_log_prefix} timeline HTTP 结束（耗时 {timeline_elapsed:.1f}s）",
        file=sys.stderr,
    )

    print(f"{user_log_prefix} comments HTTP 开始抓取 max_id=-1", file=sys.stderr)
    comments_started = time.monotonic()
    try:
        wrote_comments, refs = _crawl_comments_one_page_via_http_api(
            api=api,
            user_id=str(user_id),
            since_bj=since_bj,
            store=comment_store,
            seen_ids=seen_comment_ids,
        )
    except ChallengeRequiredError as e:
        had_blocked = True
        had_failure = True
        print(f"{user_log_prefix} comments HTTP 被风控拦截：{e}", file=sys.stderr)
    except BlockedError as e:
        had_blocked = True
        had_failure = True
        print(f"{user_log_prefix} comments HTTP 不可用：{e}", file=sys.stderr)
    except Exception as e:
        had_failure = True
        print(f"{user_log_prefix} comments HTTP 抓取失败：{e}", file=sys.stderr)
    comments_elapsed = time.monotonic() - comments_started
    print(
        f"{user_log_prefix} comments HTTP 结束（耗时 {comments_elapsed:.1f}s）",
        file=sys.stderr,
    )

    if want_talks:
        if refs:
            print(
                f"{user_log_prefix} talks HTTP 开始补齐（refs={len(refs)} max_pages={int(args.max_talk_pages)}）",
                file=sys.stderr,
            )
            talks_started = time.monotonic()
            try:
                wrote_talks = _backfill_talks_for_comment_refs(
                    api=api,
                    user_id=str(user_id),
                    refs=refs,
                    max_talk_pages=int(args.max_talk_pages),
                    talks_store=talks_store,
                )
            except ChallengeRequiredError as e:
                had_blocked = True
                had_failure = True
                print(f"{user_log_prefix} talks HTTP 被风控拦截：{e}", file=sys.stderr)
            except BlockedError as e:
                had_blocked = True
                had_failure = True
                print(f"{user_log_prefix} talks HTTP 不可用：{e}", file=sys.stderr)
            except Exception as e:
                had_failure = True
                print(f"{user_log_prefix} talks HTTP 抓取失败：{e}", file=sys.stderr)
            talks_elapsed = time.monotonic() - talks_started
            print(
                f"{user_log_prefix} talks HTTP 结束（新增/更新 {wrote_talks} 份，耗时 {talks_elapsed:.1f}s）",
                file=sys.stderr,
            )
        else:
            print(
                f"{user_log_prefix} 本轮 comments 无新增，跳过 talks 补齐。",
                file=sys.stderr,
            )

    did_write = int(wrote_statuses + wrote_comments + wrote_talks)
    if did_write > 0:
        print(
            f"{user_log_prefix} 开始重建 entry（本轮新增写入 status={wrote_statuses} comment={wrote_comments} talk={wrote_talks}）",
            file=sys.stderr,
        )
        finalize_started = time.monotonic()
        detail_cache: dict[str, Optional[str]] = {}

        def resolve_status_line(
            status_id: str,
            source_status_url: str = "",
            status_url: str = "",
            status_user_id: str = "",
        ) -> Optional[str]:
            sid = str(status_id or "").strip()
            if not sid:
                return None
            if sid in detail_cache:
                return detail_cache[sid]
            print(f"[detail-http] 开始补抓原帖全文 status_id={sid}", file=sys.stderr)
            referrer = str(status_url or source_status_url or BASE_URL)
            line, reason = api.fetch_status_display_line(sid, referrer=referrer)
            if not line and reason:
                print(
                    f"[detail-http] status_id={sid} fetch failed: {reason}",
                    file=sys.stderr,
                )
            detail_cache[sid] = line
            return line

        final_entries = rebuild_user_entries_from_raw_records(
            db=db,
            user_id=str(user_id),
            resolve_status_line=resolve_status_line,
        )
        finalize_elapsed = time.monotonic() - finalize_started
        if final_entries:
            print(
                f"{user_log_prefix} 最终展示记录写入 {final_entries} 条到 SQLite：{db_path}（耗时 {finalize_elapsed:.1f}s）",
                file=sys.stderr,
            )
    else:
        print(f"{user_log_prefix} 本轮无新增数据，跳过 entry 更新。", file=sys.stderr)

    total_elapsed = time.monotonic() - user_started
    print(
        f"{user_log_prefix} HTTP 增量模式结束，耗时 {total_elapsed:.1f}s",
        file=sys.stderr,
    )
    if had_failure:
        if had_blocked and did_write == 0:
            print(
                f"{user_log_prefix} 提示：HTTP 模式下接口被拦截/返回非 JSON。"
                "这通常意味着 Cookie 失效或触发了 WAF 挑战页；云上无法手动验证，需要更新 Cookie 或改用浏览器模式。",
                file=sys.stderr,
            )
        else:
            print(
                f"{user_log_prefix} 本轮抓取存在失败，返回失败码以便上层感知。",
                file=sys.stderr,
            )
        return 2
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        target_user_ids = _resolve_target_user_ids(args)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 2

    db_path = _resolve_db_path(args=args, out_dir=out_dir)
    cooldown_sec = max(0.0, float(args.user_cooldown_sec))

    print(
        f"本次共 {len(target_user_ids)} 个用户，统一写入 SQLite：{db_path}",
        file=sys.stderr,
    )

    if args.mode == "incremental_http":
        resume_since_bj: dt.datetime
        if str(args.since or "").strip():
            try:
                resume_since_bj = _parse_since_to_beijing(
                    args.since, tz_name=str(args.tz)
                )
            except Exception as e:
                print(str(e), file=sys.stderr)
                return 2
        else:
            # For incremental_http: `--since` is optional. Use a very old timestamp
            # so no records on the fetched first page are filtered out.
            resume_since_bj = dt.datetime(1970, 1, 1, tzinfo=BEIJING_TIMEZONE)

        with SqliteDb(db_path) as db:
            total = len(target_user_ids)
            for index, user_id in enumerate(target_user_ids, start=1):
                print(
                    f"开始抓取用户 {index}/{total}：{user_id}（HTTP 增量模式）",
                    file=sys.stderr,
                )
                result = _run_single_user_incremental_http(
                    args=args,
                    db=db,
                    db_path=db_path,
                    out_dir=out_dir,
                    user_id=str(user_id),
                    since_bj=resume_since_bj,
                )
                if result != 0:
                    if total > 1:
                        print(
                            f"用户 {user_id} 抓取失败。为保护账号，批量任务已停止，后续用户不会继续。",
                            file=sys.stderr,
                        )
                    return result
                if index < total and cooldown_sec > 0:
                    print(
                        f"用户 {user_id} 已完成，等待 {cooldown_sec:.1f} 秒后继续下一个用户。",
                        file=sys.stderr,
                    )
                    time.sleep(cooldown_sec)
        return 0

    incremental = bool(getattr(args, "incremental", False))
    resume_since_bj_iso = ""
    if (not incremental) and (not str(args.since or "").strip()):
        print(
            "core 模式必须提供 --since（YYYY-MM-DD 或 ISO 8601 时间）", file=sys.stderr
        )
        return 2
    if not incremental:
        try:
            resume_since_bj = _parse_since_to_beijing(args.since, tz_name=str(args.tz))
        except Exception as e:
            print(str(e), file=sys.stderr)
            return 2
        resume_since_bj_iso = resume_since_bj.replace(microsecond=0).isoformat()

    base_user_data_dir = Path(args.user_data_dir)
    browser_profiles_root = _build_browser_profiles_root(out_dir)

    print(f"基础浏览器资料目录：{base_user_data_dir}", file=sys.stderr)
    print(f"本次临时浏览器目录根路径：{browser_profiles_root}", file=sys.stderr)

    # Import Playwright-dependent modules only when we actually need to crawl.
    from .browser import BrowserConfig, BrowserSession

    base_browser_cfg = BrowserConfig(
        headless=bool(args.headless),
        user_data_dir=base_user_data_dir,
        chrome_channel=str(args.chrome_channel or "chrome"),
        cdp_url=None,
        reduce_automation_fingerprint=bool(args.reduce_automation_fingerprint),
        manage_cdp=True,
    )

    try:
        _prepare_base_browser_profile(args=args, browser_cfg=base_browser_cfg)
    except Exception as e:
        print(f"准备基础浏览器资料目录失败：{e}", file=sys.stderr)
        return 2

    with SqliteDb(db_path) as db:
        total = len(target_user_ids)
        for index, user_id in enumerate(target_user_ids, start=1):
            if not incremental:
                crawl_progress_store = SqliteCrawlProgressStore(
                    db=db, user_id=str(user_id)
                )
                if crawl_progress_store.is_completed(
                    since_bj_iso=resume_since_bj_iso, stage=PROGRESS_STAGE_FINALIZE
                ):
                    print(
                        f"开始抓取用户 {index}/{total}：{user_id}（上次已经跑完，这次跳过）",
                        file=sys.stderr,
                    )
                    continue
                if _user_has_entry_rows(
                    db=db, user_id=str(user_id)
                ) and not _user_has_raw_rows(db=db, user_id=str(user_id)):
                    crawl_progress_store.mark_completed(
                        since_bj_iso=resume_since_bj_iso,
                        stage=PROGRESS_STAGE_FINALIZE,
                        detail={"inferred_from_entries": True},
                    )
                    print(
                        f"开始抓取用户 {index}/{total}：{user_id}（已发现完整结果，这次跳过）",
                        file=sys.stderr,
                    )
                    continue
            user_profile_dir = _build_user_browser_profile_dir(
                profiles_root=browser_profiles_root,
                index=index,
                user_id=str(user_id),
            )
            try:
                _copy_browser_profile_dir(base_user_data_dir, user_profile_dir)
            except Exception as e:
                print(
                    f"用户 {user_id} 的浏览器资料目录复制失败：{e}",
                    file=sys.stderr,
                )
                return 2

            browser_cfg = BrowserConfig(
                headless=bool(args.headless),
                user_data_dir=user_profile_dir,
                chrome_channel=str(args.chrome_channel or "chrome"),
                cdp_url=None,
                reduce_automation_fingerprint=bool(args.reduce_automation_fingerprint),
                manage_cdp=True,
            )
            print(f"开始抓取用户 {index}/{total}：{user_id}", file=sys.stderr)
            print(
                f"用户 {user_id} 使用浏览器资料目录：{user_profile_dir}",
                file=sys.stderr,
            )
            try:
                with BrowserSession(browser_cfg) as session:
                    result = _run_single_user(
                        args=args,
                        db=db,
                        db_path=db_path,
                        out_dir=out_dir,
                        session=session,
                        user_id=str(user_id),
                    )
            except Exception as e:
                print(f"用户 {user_id} 抓取时启动浏览器失败：{e}", file=sys.stderr)
                print(
                    f"用户 {user_id} 的浏览器资料目录已保留：{user_profile_dir}",
                    file=sys.stderr,
                )
                if total > 1:
                    print(
                        f"用户 {user_id} 抓取失败。为保护账号，批量任务已停止，后续用户不会继续。",
                        file=sys.stderr,
                    )
                return 2

            if result != 0:
                print(
                    f"用户 {user_id} 的浏览器资料目录已保留：{user_profile_dir}",
                    file=sys.stderr,
                )
                if total > 1:
                    print(
                        f"用户 {user_id} 抓取失败。为保护账号，批量任务已停止，后续用户不会继续。",
                        file=sys.stderr,
                    )
                return result

            try:
                _cleanup_browser_profile_dir(user_profile_dir)
            except Exception as e:
                print(
                    f"用户 {user_id} 的临时浏览器目录删除失败，已保留：{user_profile_dir}，原因：{e}",
                    file=sys.stderr,
                )

            if index < total and cooldown_sec > 0:
                print(
                    f"用户 {user_id} 已完成，等待 {cooldown_sec:.1f} 秒后继续下一个用户。",
                    file=sys.stderr,
                )
                time.sleep(cooldown_sec)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
