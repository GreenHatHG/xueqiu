from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from xueqiu_crawler import cli
from xueqiu_crawler import rss_server
from xueqiu_crawler.http_api import HttpClientConfig, XueqiuHttpApi
from xueqiu_crawler.storage import SqliteDb
from xueqiu_crawler.xq_api import ApiConfig


class TimelinePayloadValidationTests(unittest.TestCase):
    def test_fetch_timeline_first_page_rejects_error_payload(self) -> None:
        cfg = ApiConfig(
            min_delay_sec=0.0,
            jitter_sec=0.0,
            max_retries=0,
            max_consecutive_blocks=3,
            http_debug=False,
        )
        api = XueqiuHttpApi(
            cfg,
            HttpClientConfig(cookie="xq_a_token=fake", timeout_sec=1.0),
        )
        bad_payload = json.dumps(
            {
                "error_description": "遇到错误，请刷新页面或者重新登录帐号后再试",
                "error_code": "400016",
            }
        )
        api._fetch_text_once = lambda url, referrer=None: (  # type: ignore[assignment,misc]
            400,
            bad_payload,
            str(url),
        )

        with self.assertRaises(RuntimeError):
            api.fetch_timeline_first_page("8790885129")


class IncrementalHttpFailurePolicyTests(unittest.TestCase):
    def _build_args(self) -> argparse.Namespace:
        return argparse.Namespace(
            min_delay=0.0,
            jitter=0.0,
            max_retries=0,
            max_consecutive_blocks=3,
            http_debug=False,
            with_talks=False,
            no_talks=False,
            max_talk_pages=1,
        )

    def _since_bj(self) -> dt.datetime:
        return dt.datetime(1970, 1, 1, tzinfo=cli.BEIJING_TIMEZONE)

    def test_incremental_http_returns_nonzero_when_comments_stage_raises(self) -> None:
        args = self._build_args()
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "batch.sqlite3"
            with SqliteDb(db_path) as db:
                with (
                    patch(
                        "xueqiu_crawler.http_api.XueqiuHttpApi.from_env",
                        return_value=object(),
                    ),
                    patch.object(
                        cli, "_crawl_timeline_one_page_via_http_api", return_value=0
                    ),
                    patch.object(
                        cli,
                        "_crawl_comments_one_page_via_http_api",
                        side_effect=RuntimeError("comments failed"),
                    ),
                    patch.object(
                        cli, "_backfill_talks_for_comment_refs", return_value=0
                    ),
                ):
                    result = cli._run_single_user_incremental_http(
                        args=args,
                        db=db,
                        db_path=db_path,
                        out_dir=db_path.parent,
                        user_id="8790885129",
                        since_bj=self._since_bj(),
                    )

        self.assertEqual(result, 2)

    def test_incremental_http_returns_nonzero_when_talks_stage_raises(self) -> None:
        args = self._build_args()
        refs = [
            {
                "comment_id": "1",
                "root_in_reply_to_status_id": "",
                "root_status_id": "2",
                "created_at_bj": "2026-03-28T00:00:00+08:00",
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "batch.sqlite3"
            with SqliteDb(db_path) as db:
                with (
                    patch(
                        "xueqiu_crawler.http_api.XueqiuHttpApi.from_env",
                        return_value=object(),
                    ),
                    patch.object(
                        cli, "_crawl_timeline_one_page_via_http_api", return_value=0
                    ),
                    patch.object(
                        cli,
                        "_crawl_comments_one_page_via_http_api",
                        return_value=(0, refs),
                    ),
                    patch.object(
                        cli,
                        "_backfill_talks_for_comment_refs",
                        side_effect=RuntimeError("talks failed"),
                    ),
                ):
                    result = cli._run_single_user_incremental_http(
                        args=args,
                        db=db,
                        db_path=db_path,
                        out_dir=db_path.parent,
                        user_id="8790885129",
                        since_bj=self._since_bj(),
                    )

        self.assertEqual(result, 2)


class RssRouteFailurePolicyTests(unittest.TestCase):
    def test_user_rss_returns_502_when_refresh_result_is_nonzero(self) -> None:
        old_key = os.environ.get("XQ_RSS_KEY")
        old_ttl = os.environ.get("XQ_RSS_TTL_SEC")
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "batch.sqlite3"
            with SqliteDb(db_path):
                pass
            rss_server.app.state.cli_db_path = str(db_path)
            os.environ["XQ_RSS_KEY"] = "k"
            os.environ["XQ_RSS_TTL_SEC"] = "0"
            with (
                patch.object(
                    rss_server.cli_lib,
                    "_run_single_user_incremental_http",
                    return_value=2,
                ),
                patch.object(
                    rss_server, "maybe_cleanup_old_data", return_value={"ran": False}
                ),
            ):
                request = SimpleNamespace(
                    url=SimpleNamespace(query="key=k"),
                    app=SimpleNamespace(
                        state=SimpleNamespace(cli_db_path=str(db_path))
                    ),
                )
                resp = rss_server.user_rss("8790885129", cast(Any, request))
            self.assertEqual(resp.status_code, 502)
            body = resp.body
            if isinstance(body, memoryview):
                body = body.tobytes()
            self.assertIn("upstream failed", body.decode("utf-8", errors="replace"))
        if old_key is None:
            os.environ.pop("XQ_RSS_KEY", None)
        else:
            os.environ["XQ_RSS_KEY"] = old_key
        if old_ttl is None:
            os.environ.pop("XQ_RSS_TTL_SEC", None)
        else:
            os.environ["XQ_RSS_TTL_SEC"] = old_ttl


if __name__ == "__main__":
    unittest.main()
