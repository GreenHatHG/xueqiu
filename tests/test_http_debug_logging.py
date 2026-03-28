from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr

from xueqiu_crawler.http_debug import sanitize_url_for_debug
from xueqiu_crawler.http_api import HttpClientConfig, XueqiuHttpApi
from xueqiu_crawler.rss_server import _build_incremental_http_args
from xueqiu_crawler.xq_api import ApiConfig


class HttpDebugLoggingTests(unittest.TestCase):
    def test_rss_args_reads_http_debug_env(self) -> None:
        old = os.environ.get("XQ_HTTP_DEBUG")
        try:
            os.environ["XQ_HTTP_DEBUG"] = "1"
            args = _build_incremental_http_args()
            self.assertTrue(bool(getattr(args, "http_debug", False)))
        finally:
            if old is None:
                os.environ.pop("XQ_HTTP_DEBUG", None)
            else:
                os.environ["XQ_HTTP_DEBUG"] = old

    def test_http_api_logs_text_head_on_bad_payload_when_debug_enabled(self) -> None:
        cfg = ApiConfig(
            min_delay_sec=0.0,
            jitter_sec=0.0,
            max_retries=0,
            max_consecutive_blocks=3,
            http_debug=True,
        )
        api = XueqiuHttpApi(
            cfg,
            HttpClientConfig(cookie="xq_a_token=fake", timeout_sec=1.0),
        )

        bad_payload = json.dumps(
            {
                "items": {"not": "list"},
                "next_max_id": -1,
                "next_id": -1,
                "message": "bad payload for debug preview",
            }
        )
        api._fetch_text_once = lambda url, referrer=None: (  # type: ignore[assignment,misc]
            200,
            bad_payload,
            str(url),
        )

        buffer = io.StringIO()
        with self.assertRaises(RuntimeError), redirect_stderr(buffer):
            api.fetch_user_comments_first_page("123")
        logs = buffer.getvalue()
        self.assertIn("[http-debug]", logs)
        self.assertIn("text_head=", logs)
        self.assertIn("items_type=dict", logs)
        self.assertNotIn("xq_a_token=fake", logs)

    def test_sanitize_url_for_debug_masks_sensitive_query_keys(self) -> None:
        url = "https://xueqiu.com/u/1?key=abc&xq_a_token=secret&foo=bar"
        masked = sanitize_url_for_debug(url)
        self.assertIn("key=%2A%2A%2A", masked)
        self.assertIn("xq_a_token=%2A%2A%2A", masked)
        self.assertIn("foo=bar", masked)
        self.assertNotIn("secret", masked)


if __name__ == "__main__":
    unittest.main()
