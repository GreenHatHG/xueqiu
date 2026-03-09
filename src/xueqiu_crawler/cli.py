from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from urllib.error import URLError
from urllib.request import Request, urlopen

from .constants import (
    BASE_URL,
    BEIJING_TIMEZONE_NAME,
    DEFAULT_CORE_MAX_USER_COMMENTS_SCAN_PAGES,
    DEFAULT_CORE_MAX_TALK_PAGES,
    DEFAULT_DB_BASENAME,
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CONSECUTIVE_BLOCKS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MIN_DELAY_SEC,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_USER_DATA_DIR,
    USER_COMMENTS_PAGE_SIZE,
)
from .storage import (
    SqliteDb,
    SqliteMergedCommentsStore,
    SqliteMergedStatusesStore,
    SqliteMergedTalksStore,
    collapse_user_records_to_entries,
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


def _beijing_iso_now() -> str:
    return dt.datetime.now(tz=BEIJING_TIMEZONE).replace(microsecond=0).isoformat()


def _format_progress_dt(value: Optional[dt.datetime]) -> str:
    if value is None:
        return "-"
    return value.astimezone(BEIJING_TIMEZONE).replace(microsecond=0).isoformat(sep=" ")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="xq-crawl")
    p.add_argument("--user-id", required=True, help="雪球用户ID，例如 9650668145")
    p.add_argument(
        "--mode",
        default="core",
        choices=["core"],
        help="抓取内容范围（当前仅支持 core：发言+回复+查看对话，且只落库 SQLite）。",
    )
    p.add_argument(
        "--since",
        default=None,
        help="截止时间（本地时区日期 YYYY-MM-DD 或 ISO 8601 时间）。core 模式必填：抓取 >= since 的内容，遇到更早内容则停止。",
    )
    p.add_argument(
        "--out", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录（默认 data/）"
    )
    p.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite 数据库文件路径（默认：{out}/xueqiu_{user_id}.sqlite3）。本项目已切换为“只落库”，不再生成 JSONL/CSV/JSON 文件。",
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
        help="单请求最大重试次数",
    )
    p.add_argument(
        "--max-consecutive-blocks",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_BLOCKS,
        help="连续被风控/返回HTML次数阈值，超过则停止",
    )

    p.add_argument(
        "--headless", action="store_true", help="无头模式（不推荐，登录与风控更不稳定）"
    )
    p.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_USER_DATA_DIR,
        help="Playwright 持久化用户数据目录",
    )
    p.add_argument(
        "--chrome-channel", default="chrome", help="Playwright channel（默认 chrome）"
    )
    p.add_argument(
        "--reduce-automation-fingerprint",
        action="store_true",
        help="启用最小的自动化指纹减弱（若出现 alichlgref/md5__1038 无限跳转，建议开启）",
    )
    p.add_argument(
        "--cdp",
        default=None,
        help="连接现有 Chrome 的 CDP 地址，例如 http://127.0.0.1:9222 （可选）",
    )
    p.add_argument(
        "--login-timeout-sec",
        type=int,
        default=600,
        help="等待手动登录的最长时间（秒，默认 600）",
    )
    return p.parse_args(argv)


def _has_login_cookie(page) -> bool:
    try:
        cookies = page.context.cookies(BASE_URL)
    except Exception:
        return False
    names = {c.get("name") for c in cookies if isinstance(c, dict)}
    return bool(names.intersection({"xq_a_token", "xq_r_token", "xq_id_token", "u"}))


def _ensure_logged_in_ui(page, timeout_sec: int) -> None:
    """
    UI 拦截模式下不做“程序侧直连接口探测”，只基于页面与 cookie 做保守等待。

    说明：登录/滑动验证码很难在不触发更强风控的前提下全自动完成，因此这里只做“自动等待已有登录态”，
    如果未登录则需要你在弹出的浏览器里手动登录一次（之后 persistent context 会复用）。
    """

    try:
        page.goto(BASE_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    if _has_login_cookie(page):
        return

    print(
        "当前浏览器上下文未检测到登录 cookie。请在打开的浏览器窗口里手动登录雪球一次。",
        file=sys.stderr,
    )
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        time.sleep(4)
        if _has_login_cookie(page):
            print("检测到登录 cookie，继续执行自动化浏览与拦截取数。", file=sys.stderr)
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
        "用 --cdp 连接一个手工启动的 Chrome（见 docs/使用本地Chrome(CDP)操作指南.md）。",
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


def _click_next_page_and_wait(page, *, stats: "_UiInterceptStats") -> bool:
    print(
        f"[{stats.kind_name}] 当前页空转，尝试翻到下一页；已扫到最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
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
            _scroll_to_top(page)
            print(f"[{stats.kind_name}] 下一页已触发并收到新批次。", file=sys.stderr)
            return True
        if _page_signature(page) != previous_signature:
            _scroll_to_top(page)
            print(f"[{stats.kind_name}] 下一页已打开。", file=sys.stderr)
            return True

    print(f"[{stats.kind_name}] 下一页未打开或未产生新批次。", file=sys.stderr)
    return False


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

    seen_urls: set[str] = field(default_factory=set)
    wrote: int = 0
    captured_batches: int = 0
    consecutive_old_batches: int = 0
    saw_any: bool = False
    last_hit_ts: float = 0.0
    last_batch_oldest: Optional[dt.datetime] = None

    def should_stop(self) -> bool:
        if self.max_batches > 0 and self.captured_batches >= self.max_batches:
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

        stats.saw_any = True
        stats.last_hit_ts = time.time()
        stats.captured_batches += 1

        records, batch_oldest = stats.extract_records(obj)
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

        stats.last_batch_oldest = batch_oldest
        print(
            f"[{stats.kind_name}] 批次 {stats.captured_batches}: 原始 {len(records)} 条, 新增 {len(to_write)} 条, "
            f"日期 {_format_progress_dt(batch_newest)} -> {_format_progress_dt(batch_oldest)}",
            file=sys.stderr,
        )

        if batch_oldest is not None and batch_oldest < stats.since_bj:
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
    )
    handler = _make_ui_response_handler(stats)
    page.on("response", handler)
    try:
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
                    f"[{kind_name}] 已达到空转阈值，且无法继续翻页，停止抓取；当前最旧日期 {_format_progress_dt(stats.last_batch_oldest)}",
                    file=sys.stderr,
                )
                break

        html_path: Optional[Path] = None
        if not stats.saw_any:
            html_path = _write_html_snapshot(
                out_dir, user_id=str(user_id), kind=kind_name, page=page
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
) -> tuple[int, Optional[dt.datetime], int]:
    page_limit = (
        int(max_pages)
        if int(max_pages) > 0
        else DEFAULT_CORE_MAX_USER_COMMENTS_SCAN_PAGES
    )
    if page_limit <= 0:
        return 0, None, 0

    print(
        f"[comments-api] 开始抓取：since={_format_progress_dt(since_bj)}",
        file=sys.stderr,
    )
    wrote = 0
    oldest_seen: Optional[dt.datetime] = None
    page_count = 0
    for next_max_id, items in api.iter_user_comments_pages(
        user_id=user_id, start_max_id=-1, max_pages=page_limit
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
        if batch_oldest is not None and batch_oldest < since_bj:
            break

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
) -> int:
    wrote = 0
    since_bj_iso = since_bj.replace(microsecond=0).isoformat()
    refs = list(comments_store.iter_comment_refs_since(since_bj_iso=since_bj_iso))
    if refs:
        print(f"[talks] 开始补齐，共 {len(refs)} 条评论链待检查。", file=sys.stderr)
    else:
        print("[talks] 没有需要补齐的评论链。", file=sys.stderr)

    for idx, ref in enumerate(refs, start=1):
        cid = ref.get("comment_id")
        root_status_id = ref.get("root_in_reply_to_status_id") or ref.get(
            "root_status_id"
        )
        if not cid or not root_status_id:
            continue
        print(
            f"[talks] {idx}/{len(refs)} comment={cid} root={root_status_id} created_at={ref.get('created_at_bj') or '-'}",
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
                continue
            if not truncated and max_page == 0 and fetched_pages >= int(max_talk_pages):
                continue

        existing_obj = (
            talks_store.get_existing_obj(
                root_status_id=str(root_status_id), comment_id=str(cid)
            )
            if meta
            else None
        )
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

    print(f"[talks] 补齐结束，新增/更新 {wrote} 条。", file=sys.stderr)
    return wrote


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.cdp:
        # Preflight check: provide a clearer error than ECONNREFUSED.
        cdp_base = str(args.cdp).rstrip("/")
        version_url = f"{cdp_base}/json/version"
        try:
            req = Request(version_url, headers={"accept": "application/json"})
            with urlopen(req, timeout=2) as resp:  # nosec - local loopback only
                if resp.status >= 400:
                    raise URLError(f"HTTP {resp.status}")
        except Exception:
            print(
                "无法连接到 CDP 端点。请确认：1) Chrome 已启动并监听该端口；2) 端口仅绑定 127.0.0.1；"
                "3) macOS 上必须用非默认的 --user-data-dir 启动 Chrome（否则不会开启 DevTools 远程调试）。",
                file=sys.stderr,
            )
            print(f"你配置的 CDP 地址：{args.cdp}", file=sys.stderr)
            print(f"建议先在终端验证：curl -s {version_url}", file=sys.stderr)
            return 2

    db_path: Path
    if args.db:
        db_path = Path(args.db)
    else:
        db_path = out_dir / DEFAULT_DB_BASENAME.format(user_id=str(args.user_id))

    with SqliteDb(db_path) as db:
        api_cfg = ApiConfig(
            min_delay_sec=args.min_delay,
            jitter_sec=args.jitter,
            max_retries=args.max_retries,
            max_consecutive_blocks=args.max_consecutive_blocks,
        )

        # Import Playwright-dependent modules only when we actually need to crawl.
        from .browser import BrowserConfig, BrowserSession

        browser_cfg = BrowserConfig(
            headless=bool(args.headless),
            user_data_dir=Path(args.user_data_dir),
            chrome_channel=None if args.cdp else str(args.chrome_channel or "chrome"),
            cdp_url=str(args.cdp) if args.cdp else None,
            reduce_automation_fingerprint=bool(args.reduce_automation_fingerprint),
        )

        timeline_store = SqliteMergedStatusesStore(db=db, user_id=str(args.user_id))
        comment_store = SqliteMergedCommentsStore(db=db, user_id=str(args.user_id))
        talks_store = SqliteMergedTalksStore(db=db, user_id=str(args.user_id))

        # Cross-run de-dup relies on SQLite unique key (merge_key). Keep in-run sets only.
        seen_status_ids: set[str] = set()
        seen_comment_ids: set[str] = set()

        with BrowserSession(browser_cfg) as session:
            ui_page = session.ui_page
            data_page = ui_page if args.cdp else session.page
            data_api = XueqiuApi(data_page, api_cfg, prefer_page_fetch=bool(args.cdp))
            # Keep api_page around for talks backfill and future extensions.
            detail_api = XueqiuApi(session.page, api_cfg)
            detail_status_line_cache: dict[
                tuple[str, str, str, str], Optional[str]
            ] = {}

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
                line = detail_api.fetch_status_display_line(
                    sid,
                    source_status_url=source_status_url,
                    status_url=status_url,
                    status_user_id=status_user_id,
                )
                if line:
                    print(f"[detail] 原帖全文补抓成功 status_id={sid}", file=sys.stderr)
                else:
                    print(f"[detail] 原帖全文补抓失败 status_id={sid}", file=sys.stderr)
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

            if args.mode == "core":
                tz_name = str(args.tz)
                try:
                    since_bj = _parse_since_to_beijing(args.since, tz_name=tz_name)
                except Exception as e:
                    print(str(e), file=sys.stderr)
                    return 2

                # core 模式默认尽量把“查看对话”也补齐；如果你只要主干数据可用 --no-talks 关闭。
                want_talks = bool(args.with_talks) or (not bool(args.no_talks))

                had_blocked = False
                wrote_statuses = 0
                wrote_comments = 0
                wrote_talks = 0
                limiter = RateLimiter(float(args.min_delay), float(args.jitter))

                timeline_result = _crawl_via_ui_intercept(
                    page=ui_page,
                    out_dir=out_dir,
                    user_id=str(args.user_id),
                    since_bj=since_bj,
                    url=_profile_url(str(args.user_id)),
                    url_contains="/v4/statuses/user_timeline.json",
                    max_batches=int(args.max_timeline_pages),
                    store=timeline_store,
                    seen_ids=seen_status_ids,
                    normalize_fn=_normalize_timeline_status,
                    extract_records_fn=_extract_timeline_records,
                    limiter=limiter,
                    kind_name="timeline",
                )
                wrote_statuses = int(timeline_result.wrote)
                if wrote_statuses:
                    print(f"时间线新增写入 {wrote_statuses} 条到 SQLite：{db_path}")
                elif not timeline_result.saw_any and timeline_result.html_path:
                    print(
                        f"未拦截到时间线 JSON，已降级保存 HTML 快照：{timeline_result.html_path}",
                        file=sys.stderr,
                    )

                if args.cdp:
                    try:
                        wrote_comments, _comments_oldest, comments_pages = (
                            _crawl_comments_via_api(
                                api=data_api,
                                user_id=str(args.user_id),
                                since_bj=since_bj,
                                max_pages=int(args.max_comment_pages),
                                store=comment_store,
                                seen_ids=seen_comment_ids,
                            )
                        )
                    except ChallengeRequiredError as e:
                        try:
                            target_url = getattr(
                                e, "final_url", ""
                            ) or data_api.build_url(
                                "/statuses/user/comments.json",
                                {
                                    "user_id": str(args.user_id),
                                    "size": USER_COMMENTS_PAGE_SIZE,
                                    "max_id": -1,
                                },
                            )
                            _wait_for_waf_challenge(
                                ui_page,
                                data_api,
                                args.user_id,
                                int(args.login_timeout_sec),
                                target_url,
                                navigate_to_blocked_url=not bool(args.cdp),
                            )
                            wrote_comments, _comments_oldest, comments_pages = (
                                _crawl_comments_via_api(
                                    api=data_api,
                                    user_id=str(args.user_id),
                                    since_bj=since_bj,
                                    max_pages=int(args.max_comment_pages),
                                    store=comment_store,
                                    seen_ids=seen_comment_ids,
                                )
                            )
                        except Exception as e2:
                            had_blocked = True
                            wrote_comments, _comments_oldest, comments_pages = (
                                0,
                                None,
                                0,
                            )
                            print(f"回复抓取被拦截：{e2}", file=sys.stderr)
                    except BlockedError as e:
                        had_blocked = True
                        wrote_comments, _comments_oldest, comments_pages = 0, None, 0
                        print(f"回复接口当前不可用：{e}", file=sys.stderr)
                    if wrote_comments:
                        print(f"回复新增写入 {wrote_comments} 条到 SQLite：{db_path}")
                    elif comments_pages == 0:
                        html2 = _write_html_snapshot(
                            out_dir,
                            user_id=str(args.user_id),
                            kind="comments",
                            page=ui_page,
                        )
                        if html2:
                            print(
                                f"未获取到回复 JSON，已降级保存 HTML 快照：{html2}",
                                file=sys.stderr,
                            )
                else:
                    comments_result = _crawl_via_ui_intercept(
                        page=ui_page,
                        out_dir=out_dir,
                        user_id=str(args.user_id),
                        since_bj=since_bj,
                        url=_comments_url(str(args.user_id)),
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
                    if wrote_comments:
                        print(f"回复新增写入 {wrote_comments} 条到 SQLite：{db_path}")
                    elif not comments_result.saw_any and comments_result.html_path:
                        print(
                            f"未拦截到回复 JSON，已降级保存 HTML 快照：{comments_result.html_path}",
                            file=sys.stderr,
                        )

                if want_talks:
                    talks_api = detail_api
                    try:
                        wrote_talks = _backfill_talks_since(
                            api=talks_api,
                            user_id=str(args.user_id),
                            since_bj=since_bj,
                            max_talk_pages=int(args.max_talk_pages),
                            comments_store=comment_store,
                            talks_store=talks_store,
                        )
                    except ChallengeRequiredError as e:
                        try:
                            _wait_for_waf_challenge(
                                ui_page,
                                talks_api,
                                args.user_id,
                                int(args.login_timeout_sec),
                                getattr(e, "final_url", e.url),
                            )
                            wrote_talks = _backfill_talks_since(
                                api=talks_api,
                                user_id=str(args.user_id),
                                since_bj=since_bj,
                                max_talk_pages=int(args.max_talk_pages),
                                comments_store=comment_store,
                                talks_store=talks_store,
                            )
                        except Exception as e2:
                            had_blocked = True
                            print(f"查看对话抓取被拦截：{e2}", file=sys.stderr)
                    except BlockedError as e:
                        had_blocked = True
                        print(f"查看对话抓取被拦截：{e}", file=sys.stderr)
                    except Exception as e:
                        had_blocked = True
                        print(f"查看对话抓取失败：{e}", file=sys.stderr)
                    if wrote_talks:
                        print(
                            f"查看对话新增/补齐写入 {wrote_talks} 份到 SQLite：{db_path}"
                        )

                final_entries = collapse_user_records_to_entries(
                    db=db,
                    user_id=str(args.user_id),
                    resolve_status_line=resolve_status_line,
                )
                if final_entries:
                    print(f"最终展示记录写入 {final_entries} 条到 SQLite：{db_path}")

                if had_blocked and (wrote_statuses + wrote_comments + wrote_talks == 0):
                    print(
                        "提示：当前会话下接口暂时不可用。这可能是真正的风控挑战页，也可能是非 JSON/HTML 拦截页。"
                        "如果程序明确提示需要手动验证，请在 UI 标签页处理后重试；否则优先检查当前会话是否仍能稳定返回 JSON。",
                        file=sys.stderr,
                    )
                    return 2
                return 0

            print("当前仅支持 core 模式。", file=sys.stderr)
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
