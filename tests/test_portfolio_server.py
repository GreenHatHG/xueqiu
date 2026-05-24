from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from xueqiu_crawler.http_api import HttpClientConfig, XueqiuHttpApi
from xueqiu_crawler.portfolio_routes import (
    portfolio_snapshot,
)
from xueqiu_crawler.rss_server import app
from xueqiu_crawler.xq_api import ApiConfig


class PortfolioHttpApiTests(unittest.TestCase):
    def _build_api(self) -> XueqiuHttpApi:
        cfg = ApiConfig(
            min_delay_sec=0.0,
            jitter_sec=0.0,
            max_retries=0,
            max_consecutive_blocks=3,
            http_debug=False,
        )
        return XueqiuHttpApi(
            cfg,
            HttpClientConfig(cookie="xq_a_token=fake", timeout_sec=1.0),
        )

    def test_portfolio_current_and_history_use_cube_endpoints(self) -> None:
        api = self._build_api()
        calls: list[dict[str, Any]] = []

        def _fake_fetch_json(
            path: str,
            params: dict[str, Any] | None = None,
            *,
            referrer: str | None = None,
            retry_reason=None,
            request_label: str | None = None,
        ) -> Any:
            calls.append(
                {
                    "path": path,
                    "params": dict(params or {}),
                    "referrer": referrer,
                    "request_label": request_label,
                }
            )
            return {"ok": True}

        api.fetch_json = _fake_fetch_json  # type: ignore[assignment]

        self.assertEqual(api.fetch_portfolio_current("zh838108"), {"ok": True})
        self.assertEqual(
            api.fetch_portfolio_rebalancing_history("ZH838108", count=20, page=2),
            {"ok": True},
        )

        self.assertEqual(calls[0]["path"], "/cubes/rebalancing/current.json")
        self.assertEqual(calls[0]["params"], {"cube_symbol": "ZH838108"})
        self.assertEqual(calls[0]["referrer"], "https://xueqiu.com/P/ZH838108")
        self.assertEqual(calls[1]["path"], "/cubes/rebalancing/history.json")
        self.assertEqual(
            calls[1]["params"],
            {"cube_symbol": "ZH838108", "count": 20, "page": 2},
        )


class PortfolioRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_key = os.environ.get("XQ_PORTFOLIO_KEY")
        os.environ["XQ_PORTFOLIO_KEY"] = "k"

    def tearDown(self) -> None:
        if self.old_key is None:
            os.environ.pop("XQ_PORTFOLIO_KEY", None)
        else:
            os.environ["XQ_PORTFOLIO_KEY"] = self.old_key

    def test_portfolio_routes_are_mounted_on_rss_app(self) -> None:
        paths = {str(route.path) for route in app.routes if hasattr(route, "path")}
        self.assertIn("/portfolios/{portfolio_symbol}/snapshot", paths)
        self.assertIn("/u/{user_id}", paths)

    def test_portfolio_snapshot_returns_current_and_history(self) -> None:
        calls: list[tuple[str, Any]] = []

        def _fake_current(symbol: str) -> dict[str, Any]:
            calls.append(("current", symbol))
            return {"symbol": symbol, "holdings": []}

        def _fake_history(symbol: str, *, count: int, page: int) -> dict[str, Any]:
            calls.append(("history", {"symbol": symbol, "count": count, "page": page}))
            return {"symbol": symbol, "page": page, "list": [{"id": 1}]}

        fake_api = SimpleNamespace(
            fetch_portfolio_current=_fake_current,
            fetch_portfolio_rebalancing_history=_fake_history,
        )
        request = SimpleNamespace(
            url=SimpleNamespace(query="key=k&count=30&page=2"),
            app=app,
        )
        with patch("xueqiu_crawler.portfolio_routes._build_api", return_value=fake_api):
            resp = portfolio_snapshot("zh838108", cast(Any, request))

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            calls,
            [
                ("current", "ZH838108"),
                ("history", {"symbol": "ZH838108", "count": 30, "page": 2}),
            ],
        )
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        text = body.decode("utf-8", errors="replace")
        self.assertIn('"symbol":"ZH838108"', text)
        self.assertIn('"count":30', text)
        self.assertIn('"page":2', text)
        self.assertIn('"holdings":[]', text)
        self.assertIn('"id":1', text)

    def test_portfolio_snapshot_rejects_bad_symbol(self) -> None:
        request = SimpleNamespace(url=SimpleNamespace(query="key=k"), app=app)
        resp = portfolio_snapshot("SH600000", cast(Any, request))

        self.assertEqual(resp.status_code, 400)
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        self.assertIn(
            "portfolio_symbol is invalid",
            body.decode("utf-8", errors="replace"),
        )


if __name__ == "__main__":
    unittest.main()
