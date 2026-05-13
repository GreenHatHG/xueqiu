from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from xueqiu_crawler.constants import STOCK_BASE_URL
from xueqiu_crawler.http_api import HttpClientConfig, XueqiuHttpApi
from xueqiu_crawler.portfolio_routes import (
    _extract_portfolio_rows,
    user_followed_portfolio_snapshots,
    user_followed_portfolios,
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

    def test_fetch_user_followed_portfolios_uses_stock_endpoint(self) -> None:
        api = self._build_api()
        seen: dict[str, Any] = {}

        def _fake_fetch_json(
            path: str,
            params: dict[str, Any] | None = None,
            *,
            referrer: str | None = None,
            retry_reason=None,
            request_label: str | None = None,
        ) -> Any:
            seen["path"] = path
            seen["params"] = dict(params or {})
            seen["referrer"] = referrer
            seen["request_label"] = request_label
            return {"data": {"stocks": []}}

        api.fetch_json = _fake_fetch_json  # type: ignore[assignment]
        result = api.fetch_user_followed_portfolios("4776750571")

        self.assertEqual(result, {"data": {"stocks": []}})
        self.assertEqual(
            seen["path"],
            f"{STOCK_BASE_URL}/v5/stock/portfolio/stock/list.json",
        )
        self.assertEqual(
            seen["params"],
            {
                "size": 1000,
                "category": 3,
                "uid": "4776750571",
                "pid": -120,
            },
        )
        self.assertEqual(seen["referrer"], "https://xueqiu.com/u/4776750571")

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


class PortfolioExtractionTests(unittest.TestCase):
    def test_extract_portfolio_rows_deduplicates_zh_symbols(self) -> None:
        payload = {
            "data": {
                "stocks": [
                    {"symbol": "SH600000"},
                    {"stock_symbol": "ZH838108"},
                    {"stock": {"symbol": "zh123456"}},
                    {"target": "https://xueqiu.com/P/ZH838108"},
                ]
            }
        }

        rows = _extract_portfolio_rows(payload, max_portfolios=10)

        self.assertEqual([row["symbol"] for row in rows], ["ZH838108", "ZH123456"])
        self.assertEqual(rows[0]["url"], "https://xueqiu.com/P/ZH838108")


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
        self.assertIn("/users/{user_id}/followed-portfolios", paths)
        self.assertIn("/users/{user_id}/followed-portfolios/snapshots", paths)
        self.assertIn("/u/{user_id}", paths)

    def test_user_followed_portfolios_returns_upstream_payload(self) -> None:
        calls: dict[str, Any] = {}

        def _fake_fetch(
            user_id: str,
            *,
            size: int,
            category: int,
            pid: int,
        ) -> dict[str, Any]:
            calls["user_id"] = user_id
            calls["size"] = size
            calls["category"] = category
            calls["pid"] = pid
            return {"data": {"stocks": [{"symbol": "ZH123456"}]}}

        fake_api = SimpleNamespace(fetch_user_followed_portfolios=_fake_fetch)
        request = SimpleNamespace(
            url=SimpleNamespace(query="key=k&size=50"),
            app=app,
        )
        with patch("xueqiu_crawler.portfolio_routes._build_api", return_value=fake_api):
            resp = user_followed_portfolios("4776750571", cast(Any, request))

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            calls,
            {
                "user_id": "4776750571",
                "size": 50,
                "category": 3,
                "pid": -120,
            },
        )
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        text = body.decode("utf-8", errors="replace")
        self.assertIn('"user_id":"4776750571"', text)
        self.assertIn('"symbol":"ZH123456"', text)

    def test_user_followed_portfolios_rejects_bad_size(self) -> None:
        request = SimpleNamespace(url=SimpleNamespace(query="key=k&size=0"), app=app)
        resp = user_followed_portfolios("4776750571", cast(Any, request))
        self.assertEqual(resp.status_code, 400)
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        self.assertIn("size must be >=", body.decode("utf-8", errors="replace"))

    def test_user_followed_portfolio_snapshots_returns_current_and_history(self) -> None:
        calls: list[tuple[str, Any]] = []

        def _fake_followed(
            user_id: str,
            *,
            size: int,
            category: int,
            pid: int,
        ) -> dict[str, Any]:
            calls.append(("followed", {"user_id": user_id, "size": size, "category": category, "pid": pid}))
            return {
                "data": {
                    "stocks": [
                        {"stock_symbol": "ZH838108", "name": "demo"},
                        {"stock": {"symbol": "ZH123456"}},
                    ]
                }
            }

        def _fake_current(symbol: str) -> dict[str, Any]:
            calls.append(("current", symbol))
            return {"symbol": symbol, "holdings": []}

        def _fake_history(symbol: str, *, count: int, page: int) -> dict[str, Any]:
            calls.append(("history", {"symbol": symbol, "count": count, "page": page}))
            return {"symbol": symbol, "page": page, "list": []}

        fake_api = SimpleNamespace(
            fetch_user_followed_portfolios=_fake_followed,
            fetch_portfolio_current=_fake_current,
            fetch_portfolio_rebalancing_history=_fake_history,
        )
        request = SimpleNamespace(
            url=SimpleNamespace(
                query="key=k&max_portfolios=1&history_count=30&history_pages=2"
            ),
            app=app,
        )
        with patch("xueqiu_crawler.portfolio_routes._build_api", return_value=fake_api):
            resp = user_followed_portfolio_snapshots("4776750571", cast(Any, request))

        self.assertEqual(resp.status_code, 200)
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        text = body.decode("utf-8", errors="replace")
        self.assertIn('"count":1', text)
        self.assertIn('"symbol":"ZH838108"', text)
        self.assertIn('"history_count":30', text)
        self.assertNotIn('"symbol":"ZH123456"', text)
        self.assertEqual(
            calls,
            [
                (
                    "followed",
                    {"user_id": "4776750571", "size": 1000, "category": 3, "pid": -120},
                ),
                ("current", "ZH838108"),
                ("history", {"symbol": "ZH838108", "count": 30, "page": 1}),
                ("history", {"symbol": "ZH838108", "count": 30, "page": 2}),
            ],
        )

    def test_user_followed_portfolio_snapshots_keeps_partial_errors(self) -> None:
        def _fake_followed(
            user_id: str,
            *,
            size: int,
            category: int,
            pid: int,
        ) -> dict[str, Any]:
            return {
                "data": {
                    "stocks": [
                        {"symbol": "ZH111111"},
                        {"symbol": "ZH222222"},
                    ]
                }
            }

        def _fake_current(symbol: str) -> dict[str, Any]:
            if symbol == "ZH222222":
                raise RuntimeError("blocked")
            return {"symbol": symbol, "holdings": []}

        def _fake_history(symbol: str, *, count: int, page: int) -> dict[str, Any]:
            return {"symbol": symbol, "page": page, "list": []}

        fake_api = SimpleNamespace(
            fetch_user_followed_portfolios=_fake_followed,
            fetch_portfolio_current=_fake_current,
            fetch_portfolio_rebalancing_history=_fake_history,
        )
        request = SimpleNamespace(url=SimpleNamespace(query="key=k"), app=app)
        with patch("xueqiu_crawler.portfolio_routes._build_api", return_value=fake_api):
            resp = user_followed_portfolio_snapshots("4776750571", cast(Any, request))

        self.assertEqual(resp.status_code, 200)
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        text = body.decode("utf-8", errors="replace")
        self.assertIn('"count":1', text)
        self.assertIn('"symbol":"ZH111111"', text)
        self.assertIn('"symbol":"ZH222222"', text)
        self.assertIn('"error":"blocked"', text)

    def test_user_followed_portfolio_snapshots_can_skip_slow_sections(self) -> None:
        def _fake_followed(
            user_id: str,
            *,
            size: int,
            category: int,
            pid: int,
        ) -> dict[str, Any]:
            return {"data": {"stocks": [{"symbol": "ZH111111"}]}}

        def _unexpected(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("should not fetch skipped sections")

        fake_api = SimpleNamespace(
            fetch_user_followed_portfolios=_fake_followed,
            fetch_portfolio_current=_unexpected,
            fetch_portfolio_rebalancing_history=_unexpected,
        )
        request = SimpleNamespace(
            url=SimpleNamespace(query="key=k&include_current=0&include_history=0"),
            app=app,
        )
        with patch("xueqiu_crawler.portfolio_routes._build_api", return_value=fake_api):
            resp = user_followed_portfolio_snapshots("4776750571", cast(Any, request))

        self.assertEqual(resp.status_code, 200)
        body = resp.body
        if isinstance(body, memoryview):
            body = body.tobytes()
        text = body.decode("utf-8", errors="replace")
        self.assertIn('"include_current":false', text)
        self.assertIn('"include_history":false', text)
        self.assertIn('"symbol":"ZH111111"', text)
        self.assertNotIn('"current"', text)
        self.assertNotIn('"rebalancing_history"', text)


if __name__ == "__main__":
    unittest.main()
