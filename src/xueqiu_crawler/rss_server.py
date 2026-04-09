from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import threading
from dataclasses import dataclass
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response

from . import cli as cli_lib
from .constants import (
    BASE_URL,
    BEIJING_TIMEZONE_NAME,
    DEFAULT_BATCH_DB_BASENAME,
    DEFAULT_CORE_MAX_TALK_PAGES,
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CONSECUTIVE_BLOCKS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MIN_DELAY_SEC,
    DEFAULT_OUTPUT_DIR,
)
from .http_debug import env_flag_enabled
from .sqlite_maintenance import maybe_cleanup_old_data
from .storage import (
    MERGED_TABLE_NAME,
    MERGE_KEY_ENTRY_CHAIN_PREFIX,
    MERGE_KEY_ENTRY_STATUS_PREFIX,
    TALK_TEXT_SEPARATOR,
    SqliteCrawlProgressStore,
    SqliteDb,
)
from .text_sanitize import (
    strip_reply_wrappers,
    split_reply_chain_for_rss,
)


# Env vars for this service.
XQ_RSS_DB_PATH_ENV = "XQ_RSS_DB_PATH"
XQ_RSS_TTL_SEC_ENV = "XQ_RSS_TTL_SEC"
XQ_RSS_KEY_ENV = "XQ_RSS_KEY"
XQ_HTTP_DEBUG_ENV = "XQ_HTTP_DEBUG"

# Query params.
RSS_KEY_QUERY_PARAM = "key"
RSS_KEY_MASKED_VALUE = "***"

# Defaults: keep them conservative and simple.
DEFAULT_RSS_LIMIT = 20  # "one page" size used by Xueqiu APIs in this repo.
MAX_RSS_LIMIT = 200
DEFAULT_TTL_SEC = 300

RSS_PROGRESS_SINCE_BJ_ISO = "rss"
RSS_PROGRESS_STAGE = "rss_refresh"

BEIJING_TZ = ZoneInfo(BEIJING_TIMEZONE_NAME)

_USER_LOCKS: dict[str, threading.Lock] = {}
_USER_LOCKS_GUARD = threading.Lock()
_DB_MAINTENANCE_LOCK = threading.Lock()

DEFAULT_ROOT_TEXT = "ok\nTry: /u/{user_id}?limit=20&key=YOUR_KEY\n"

POST_UID_PLATFORM = "xueqiu"
POST_UID_KIND_STATUS = "status"
POST_UID_KIND_COMMENT = "comment"


def _get_user_lock(user_id: str) -> threading.Lock:
    uid = str(user_id or "").strip()
    with _USER_LOCKS_GUARD:
        lock = _USER_LOCKS.get(uid)
        if lock is None:
            lock = threading.Lock()
            _USER_LOCKS[uid] = lock
        return lock


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_str(name: str) -> str:
    return str(os.environ.get(name, "") or "").strip()


def _query_first(query: dict[str, list[str]], name: str) -> str:
    values = query.get(name)
    if not isinstance(values, list) or not values:
        return ""
    return str(values[0] or "").strip()


def _mask_key_in_path(raw_path: str) -> str:
    """Mask RSS `key` query param in logs."""
    try:
        parsed = urlparse(str(raw_path or ""))
        query = parse_qs(str(parsed.query or ""))
        if RSS_KEY_QUERY_PARAM not in query:
            return str(raw_path or "")
        query[RSS_KEY_QUERY_PARAM] = [RSS_KEY_MASKED_VALUE]
        masked_query = urlencode(query, doseq=True)
        path = str(parsed.path or "")
        return f"{path}?{masked_query}" if masked_query else path
    except Exception:
        return str(raw_path or "")


def _resolve_db_path(cli_db: Optional[str]) -> Path:
    if cli_db and str(cli_db).strip():
        return Path(str(cli_db).strip())
    env_db = str(os.environ.get(XQ_RSS_DB_PATH_ENV, "") or "").strip()
    if env_db:
        return Path(env_db)
    return DEFAULT_OUTPUT_DIR / DEFAULT_BATCH_DB_BASENAME


def _parse_limit(query: dict[str, list[str]]) -> int:
    raw = ""
    values = query.get("limit")
    if isinstance(values, list) and values:
        raw = str(values[0] or "").strip()
    if not raw:
        return int(DEFAULT_RSS_LIMIT)
    try:
        val = int(raw)
    except Exception as e:
        raise ValueError("limit 必须是整数") from e
    if val <= 0:
        raise ValueError("limit 必须大于 0")
    return min(int(val), int(MAX_RSS_LIMIT))


def _should_refresh(*, now: dt.datetime, last_updated_at_bj: str, ttl_sec: int) -> bool:
    if ttl_sec <= 0:
        return True
    s = str(last_updated_at_bj or "").strip()
    if not s:
        return True
    try:
        last = dt.datetime.fromisoformat(s)
    except Exception:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=BEIJING_TZ)
    age = (now - last).total_seconds()
    return age >= float(ttl_sec)


def _build_incremental_http_args() -> argparse.Namespace:
    # Keep args minimal: only fields used by _run_single_user_incremental_http().
    return argparse.Namespace(
        min_delay=float(DEFAULT_MIN_DELAY_SEC),
        jitter=float(DEFAULT_JITTER_SEC),
        max_retries=int(DEFAULT_MAX_RETRIES),
        max_consecutive_blocks=int(DEFAULT_MAX_CONSECUTIVE_BLOCKS),
        http_debug=env_flag_enabled(_env_str(XQ_HTTP_DEBUG_ENV)),
        with_talks=False,
        no_talks=False,  # want talks by default
        max_talk_pages=int(DEFAULT_CORE_MAX_TALK_PAGES),
    )


@dataclass(frozen=True)
class RssEntry:
    guid: str
    title: str
    link: str
    author: str
    description: str
    pub_date_rfc2822: str


def _pick_title(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return "(无标题)"
    first = s.splitlines()[0].strip() if s.splitlines() else s
    if len(first) > 80:
        return f"{first[:77]}..."
    return first


def _rss_raw_text(text: str) -> str:
    parts = [str(part).strip() for part in str(text or "").split(TALK_TEXT_SEPARATOR)]
    parts = [part for part in parts if part]
    if not parts:
        return ""

    out: list[str] = []
    for part in parts:
        s = str(part or "").strip()
        if not s:
            continue

        if "：" in s:
            speaker, body = s.split("：", 1)
            lines = split_reply_chain_for_rss(speaker=speaker, body=body)
        else:
            lines = split_reply_chain_for_rss(speaker="", body=s)
        lines = lines if lines else [s]

        overlap = 0
        max_overlap = min(len(out), len(lines))
        for size in range(max_overlap, 0, -1):
            if out[-size:] == lines[:size]:
                overlap = size
                break
        out.extend(lines[overlap:])

    return TALK_TEXT_SEPARATOR.join([part for part in out if str(part).strip()])


def _rss_title_text(text: str) -> str:
    """
    Keep title semantics stable: use the current reply wording, not newly promoted
    older quoted lines.
    """

    parts = [str(part).strip() for part in str(text or "").split(TALK_TEXT_SEPARATOR)]
    parts = [part for part in parts if part]
    if not parts:
        return ""

    for part in reversed(parts):
        s = str(part or "").strip()
        if not s:
            continue

        if "：" in s:
            speaker, body = s.split("：", 1)
            speaker_text = str(speaker or "").strip()
            body_text = str(body or "")
            stripped_body = strip_reply_wrappers(body_text)
            final_body = stripped_body if stripped_body else body_text.strip()
            if speaker_text and final_body:
                return f"{speaker_text}：{final_body}"
            if stripped_body or s:
                return stripped_body or s
            continue

        stripped = strip_reply_wrappers(s)
        if stripped or s:
            return stripped if stripped else s

    return ""


def _parse_entry_context(context_json: Any) -> dict[str, Any]:
    if context_json is None:
        return {}
    if isinstance(context_json, dict):
        return context_json
    if not isinstance(context_json, str):
        return {}
    s = context_json.strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _build_post_uid(*, source_kind: str, source_id: str) -> str:
    kind = str(source_kind or "").strip()
    sid = str(source_id or "").strip()
    if not kind or not sid:
        return ""
    return f"{POST_UID_PLATFORM}:{kind}:{sid}"


def _post_uid_from_entry(*, merge_key: str, ctx: dict[str, Any]) -> str:
    entry_type = str(ctx.get("entry_type") or "").strip()

    if entry_type == POST_UID_KIND_STATUS:
        status_id = str(ctx.get("status_id") or "").strip()
        if status_id:
            return _build_post_uid(
                source_kind=POST_UID_KIND_STATUS, source_id=status_id
            )

    if entry_type == "chain":
        comment_id = str(ctx.get("comment_id") or "").strip()
        if comment_id:
            return _build_post_uid(
                source_kind=POST_UID_KIND_COMMENT, source_id=comment_id
            )

    key = str(merge_key or "").strip()
    if key.startswith(MERGE_KEY_ENTRY_STATUS_PREFIX):
        status_id = key[len(MERGE_KEY_ENTRY_STATUS_PREFIX) :].strip()
        if status_id:
            return _build_post_uid(
                source_kind=POST_UID_KIND_STATUS, source_id=status_id
            )

    if key.startswith(MERGE_KEY_ENTRY_CHAIN_PREFIX):
        rest = key[len(MERGE_KEY_ENTRY_CHAIN_PREFIX) :].strip()
        comment_id = rest.rsplit(":", 1)[-1].strip() if rest else ""
        if comment_id:
            return _build_post_uid(
                source_kind=POST_UID_KIND_COMMENT, source_id=comment_id
            )

    return ""


def _build_entry_link(*, user_id: str, ctx: dict[str, Any]) -> str:
    entry_type = str(ctx.get("entry_type") or "").strip()
    if entry_type == "chain":
        root_url = str(ctx.get("root_status_url") or "").strip()
        if root_url:
            return root_url
        root_status_id = str(ctx.get("root_status_id") or "").strip()
        if root_status_id:
            return f"{BASE_URL}/status/{root_status_id}"
        return f"{BASE_URL}/u/{str(user_id).strip()}"
    # status
    status_id = str(ctx.get("status_id") or "").strip()
    if status_id:
        uid = str(user_id or "").strip()
        if uid:
            return f"{BASE_URL}/{uid}/{status_id}"
        return f"{BASE_URL}/status/{status_id}"
    return f"{BASE_URL}/u/{str(user_id).strip()}"


def _to_rfc2822(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=BEIJING_TZ)
        return format_datetime(parsed)
    except Exception:
        return ""


def _query_latest_entries(*, db: SqliteDb, user_id: str, limit: int) -> list[RssEntry]:
    cur = db.conn.execute(
        f"""
        SELECT merge_key, username, created_at_bj, text, context_json
        FROM {MERGED_TABLE_NAME}
        WHERE user_id = ?
          AND merge_key LIKE 'entry:%'
        ORDER BY COALESCE(created_at_bj, '') DESC, fetched_at_bj DESC, merge_key DESC
        LIMIT ?
        """,
        (str(user_id), int(limit)),
    )
    out: list[RssEntry] = []
    for row in cur:
        merge_key = str(row["merge_key"] or "").strip()
        author = str(row["username"] or "").strip()
        raw_text = str(row["text"] or "")
        text = _rss_raw_text(raw_text)
        ctx = _parse_entry_context(row["context_json"])
        title = _pick_title(_rss_title_text(raw_text))
        link = _build_entry_link(user_id=str(user_id), ctx=ctx)
        post_uid = _post_uid_from_entry(merge_key=merge_key, ctx=ctx)
        pub_date = _to_rfc2822(row["created_at_bj"])
        out.append(
            RssEntry(
                guid=post_uid or merge_key or link,
                title=title,
                link=link,
                author=author,
                description=text,
                pub_date_rfc2822=pub_date,
            )
        )
    return out


def _build_rss_xml(*, user_id: str, entries: list[RssEntry]) -> bytes:
    # Plain RSS 2.0, keep it minimal.
    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")

    uid = str(user_id or "").strip()
    channel_title = f"xueqiu u/{uid}" if uid else "xueqiu"
    channel_link = f"{BASE_URL}/u/{uid}" if uid else BASE_URL

    ET.SubElement(channel, "title").text = channel_title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = channel_title
    ET.SubElement(channel, "lastBuildDate").text = format_datetime(
        dt.datetime.now(tz=BEIJING_TZ)
    )

    for e in entries:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = e.title
        ET.SubElement(item, "link").text = e.link
        if e.author:
            ET.SubElement(item, "author").text = e.author
        if e.guid:
            guid_node = ET.SubElement(item, "guid")
            guid_text = str(e.guid)
            if not (
                guid_text.startswith("http://") or guid_text.startswith("https://")
            ):
                guid_node.set("isPermaLink", "false")
            guid_node.text = guid_text
        if e.pub_date_rfc2822:
            ET.SubElement(item, "pubDate").text = e.pub_date_rfc2822
        # Keep the DB `text` as-is (escaped by ElementTree).
        ET.SubElement(item, "description").text = e.description

    return ET.tostring(rss, encoding="utf-8", xml_declaration=True)


def _refresh_user_incremental_http(
    *, db: SqliteDb, db_path: Path, user_id: str
) -> None:
    args = _build_incremental_http_args()
    out_dir = db_path.parent
    since_bj = dt.datetime(1970, 1, 1, tzinfo=BEIJING_TZ)
    result = cli_lib._run_single_user_incremental_http(
        args=args,
        db=db,
        db_path=db_path,
        out_dir=out_dir,
        user_id=str(user_id),
        since_bj=since_bj,
    )
    if int(result) != 0:
        raise RuntimeError(f"incremental_http failed (code={int(result)})")

    SqliteCrawlProgressStore(db=db, user_id=str(user_id)).mark_completed(
        since_bj_iso=RSS_PROGRESS_SINCE_BJ_ISO,
        stage=RSS_PROGRESS_STAGE,
        detail={"kind": "rss_refresh"},
    )


app = FastAPI()
# Keep CLI args in app state so both `python -m ...` and `uvicorn module:app` work.
app.state.cli_db_path = ""


@app.middleware("http")
async def _access_log(request: Request, call_next):  # type: ignore[no-untyped-def]
    # Custom access log to avoid leaking `key=...` in query strings.
    try:
        response = await call_next(request)
    except Exception:
        # Let FastAPI/uvicorn handle stack traces; still print a small log line.
        client_host = "-"
        try:
            client_host = str(request.client.host) if request.client else "-"
        except Exception:
            client_host = "-"
        raw_path = str(request.url.path or "")
        if request.url.query:
            raw_path = f"{raw_path}?{request.url.query}"
        safe_path = _mask_key_in_path(raw_path)
        print(f"[rss] {client_host} {request.method} {safe_path} - 500", flush=True)
        raise

    client_host = "-"
    try:
        client_host = str(request.client.host) if request.client else "-"
    except Exception:
        client_host = "-"
    raw_path = str(request.url.path or "")
    if request.url.query:
        raw_path = f"{raw_path}?{request.url.query}"
    safe_path = _mask_key_in_path(raw_path)
    print(
        f"[rss] {client_host} {request.method} {safe_path} - {response.status_code}",
        flush=True,
    )
    return response


@app.get("/")
def root() -> PlainTextResponse:
    return PlainTextResponse(DEFAULT_ROOT_TEXT, status_code=200)


@app.get("/healthz")
def healthz() -> PlainTextResponse:
    return PlainTextResponse("ok\n", status_code=200)


@app.get("/u/{user_id}")
def user_rss(user_id: str, request: Request) -> Response:
    uid = str(user_id or "").strip()
    if not uid:
        return PlainTextResponse("user_id 为空\n", status_code=400)

    expected_key = _env_str(XQ_RSS_KEY_ENV)
    if not expected_key:
        return PlainTextResponse("服务没配 XQ_RSS_KEY\n", status_code=503)

    try:
        query = parse_qs(str(request.url.query or ""))
        got_key = _query_first(query, RSS_KEY_QUERY_PARAM)
        if not got_key or got_key != expected_key:
            return PlainTextResponse("key 不对\n", status_code=401)
        limit = _parse_limit(query)
    except Exception as e:
        return PlainTextResponse(
            f"bad request: {e}\n", status_code=400, media_type="text/plain"
        )

    ttl_sec = _env_int(XQ_RSS_TTL_SEC_ENV, DEFAULT_TTL_SEC)
    cli_db_path = str(getattr(request.app.state, "cli_db_path", "") or "").strip()
    db_path = _resolve_db_path(cli_db_path)

    # Keep it simple: per-user lock in-process, 1 uvicorn worker by default.
    lock = _get_user_lock(uid)
    with lock:
        try:
            with SqliteDb(db_path) as db:
                with _DB_MAINTENANCE_LOCK:
                    try:
                        result = maybe_cleanup_old_data(db.conn)
                        if bool(result.get("ran")):
                            print(
                                "[db] cleanup ok "
                                f"days={int(result.get('retention_days') or 0)} "
                                f"deleted_raw={int(result.get('deleted_raw') or 0)} "
                                f"deleted_merged={int(result.get('deleted_merged') or 0)} "
                                f"cutoff={str(result.get('cutoff_bj_iso') or '')}",
                                file=sys.stderr,
                                flush=True,
                            )
                    except Exception as e:
                        print(
                            f"[db] cleanup failed: {e}",
                            file=sys.stderr,
                            flush=True,
                        )

                progress_store = SqliteCrawlProgressStore(db=db, user_id=str(uid))
                progress = progress_store.get(
                    since_bj_iso=RSS_PROGRESS_SINCE_BJ_ISO, stage=RSS_PROGRESS_STAGE
                )
                now = dt.datetime.now(tz=BEIJING_TZ)
                last_updated_at = (
                    str(progress.get("updated_at_bj") or "") if progress else ""
                )
                if _should_refresh(
                    now=now,
                    last_updated_at_bj=last_updated_at,
                    ttl_sec=int(ttl_sec),
                ):
                    _refresh_user_incremental_http(
                        db=db, db_path=db_path, user_id=str(uid)
                    )

                entries = _query_latest_entries(
                    db=db, user_id=str(uid), limit=int(limit)
                )
                xml_bytes = _build_rss_xml(user_id=str(uid), entries=entries)
        except Exception as e:
            # Fail closed: if refresh fails, return a non-200 so monitoring can catch it.
            return PlainTextResponse(
                f"upstream failed: {e}\n",
                status_code=502,
                media_type="text/plain; charset=utf-8",
            )

    return Response(content=xml_bytes, media_type="application/rss+xml; charset=utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    import uvicorn

    def _argv_has(raw: list[str], flag: str) -> bool:
        f = str(flag or "").strip()
        if not f:
            return False
        for item in raw:
            s = str(item or "").strip()
            if s == f or s.startswith(f"{f}="):
                return True
        return False

    p = argparse.ArgumentParser(prog="xq-rss")
    p.add_argument("--host", default="0.0.0.0", help="listen host (default 0.0.0.0)")
    p.add_argument("--port", type=int, default=8000, help="listen port (default 8000)")
    p.add_argument(
        "--db",
        default="",
        help=(
            "SQLite 路径；也可用环境变量 XQ_RSS_DB_PATH。"
            f"默认 {DEFAULT_OUTPUT_DIR}/{DEFAULT_BATCH_DB_BASENAME}"
        ),
    )
    args = p.parse_args(argv)

    host = str(args.host or "0.0.0.0").strip() or "0.0.0.0"
    port = int(args.port or 8000)
    raw_argv = sys.argv[1:] if argv is None else list(argv)
    if not _argv_has(raw_argv, "--port"):
        env_port = str(os.environ.get("PORT", "") or "").strip()
        if env_port:
            try:
                port = int(env_port)
            except Exception:
                pass

    app.state.cli_db_path = str(args.db or "").strip()
    print(f"[rss] listen http://{host}:{port}", flush=True)
    uvicorn.run(app, host=host, port=int(port), workers=1, access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
