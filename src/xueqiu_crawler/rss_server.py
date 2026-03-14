from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import threading
from dataclasses import dataclass
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

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
from .storage import MERGED_TABLE_NAME, SqliteCrawlProgressStore, SqliteDb


# Env vars for this service.
XQ_RSS_DB_PATH_ENV = "XQ_RSS_DB_PATH"
XQ_RSS_TTL_SEC_ENV = "XQ_RSS_TTL_SEC"
XQ_RSS_KEY_ENV = "XQ_RSS_KEY"

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
        with_talks=False,
        no_talks=False,  # want talks by default
        max_talk_pages=int(DEFAULT_CORE_MAX_TALK_PAGES),
    )


@dataclass(frozen=True)
class RssEntry:
    guid: str
    title: str
    link: str
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
        SELECT merge_key, created_at_bj, text, context_json
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
        text = str(row["text"] or "")
        ctx = _parse_entry_context(row["context_json"])
        title = _pick_title(text)
        link = _build_entry_link(user_id=str(user_id), ctx=ctx)
        pub_date = _to_rfc2822(row["created_at_bj"])
        out.append(
            RssEntry(
                guid=merge_key or link,
                title=title,
                link=link,
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
        if e.guid:
            guid_node = ET.SubElement(item, "guid")
            guid_node.text = e.guid
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


class _RssHttpServer(ThreadingHTTPServer):
    _db_path: str


class _Handler(BaseHTTPRequestHandler):
    server_version = "xq-rss/0.1"

    def address_string(self) -> str:  # noqa: D401
        # Avoid reverse DNS lookups (slow/unreliable in containers).
        try:
            return str(self.client_address[0])
        except Exception:
            return "-"

    def _send_text(self, code: int, text: str, *, content_type: str) -> None:
        data = (text or "").encode("utf-8", errors="replace")
        self.send_response(int(code))
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_rss(self, xml_bytes: bytes) -> None:
        data = xml_bytes or b""
        self.send_response(200)
        self.send_header("content-type", "application/rss+xml; charset=utf-8")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = str(parsed.path or "").strip()
        if path in ("", "/"):
            self._send_text(
                200,
                "ok\nTry: /u/{user_id}?limit=20&key=YOUR_KEY\n",
                content_type="text/plain; charset=utf-8",
            )
            return
        if path == "/healthz":
            self._send_text(200, "ok\n", content_type="text/plain; charset=utf-8")
            return

        if not path.startswith("/u/"):
            self._send_text(
                404, "not found\n", content_type="text/plain; charset=utf-8"
            )
            return

        user_id = path[len("/u/") :].strip().strip("/")
        if not user_id:
            self._send_text(
                400, "user_id 为空\n", content_type="text/plain; charset=utf-8"
            )
            return

        expected_key = _env_str(XQ_RSS_KEY_ENV)
        if not expected_key:
            self._send_text(
                503,
                "服务没配 XQ_RSS_KEY\n",
                content_type="text/plain; charset=utf-8",
            )
            return

        try:
            query = parse_qs(str(parsed.query or ""))
            got_key = _query_first(query, RSS_KEY_QUERY_PARAM)
            if not got_key or got_key != expected_key:
                self._send_text(
                    401,
                    "key 不对\n",
                    content_type="text/plain; charset=utf-8",
                )
                return
            limit = _parse_limit(query)
        except Exception as e:
            self._send_text(
                400, f"bad request: {e}\n", content_type="text/plain; charset=utf-8"
            )
            return

        ttl_sec = _env_int(XQ_RSS_TTL_SEC_ENV, DEFAULT_TTL_SEC)
        db_path = _resolve_db_path(getattr(self.server, "_db_path", None))

        lock = _get_user_lock(user_id)
        with lock:
            try:
                with SqliteDb(db_path) as db:
                    progress_store = SqliteCrawlProgressStore(
                        db=db, user_id=str(user_id)
                    )
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
                            db=db, db_path=db_path, user_id=str(user_id)
                        )

                    entries = _query_latest_entries(
                        db=db, user_id=str(user_id), limit=int(limit)
                    )
                    xml_bytes = _build_rss_xml(user_id=str(user_id), entries=entries)
            except Exception as e:
                # Fail closed: if refresh fails, return a non-200 so monitoring can catch it.
                self._send_text(
                    502,
                    f"upstream failed: {e}\n",
                    content_type="text/plain; charset=utf-8",
                )
                return

        self._send_rss(xml_bytes)

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        # Keep logs compact and stdlib-only.
        msg = fmt % args if args else fmt
        safe_path = _mask_key_in_path(self.path)
        raw_requestline = str(getattr(self, "requestline", "") or "")
        if raw_requestline and raw_requestline in msg:
            safe_requestline = (
                f"{self.command} {safe_path} {str(getattr(self, 'request_version', '') or '').strip()}"
            ).strip()
            msg = msg.replace(raw_requestline, safe_requestline)
        print(f"[rss] {self.address_string()} {self.command} {safe_path} - {msg}")


def main(argv: Optional[list[str]] = None) -> int:
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

    httpd = _RssHttpServer((host, port), _Handler)
    httpd._db_path = str(args.db or "").strip()
    print(f"[rss] listen http://{host}:{port}", flush=True)
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
