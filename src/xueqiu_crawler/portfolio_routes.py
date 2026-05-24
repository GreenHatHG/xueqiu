from __future__ import annotations

import os
import re
import threading
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from .constants import (
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CONSECUTIVE_BLOCKS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MIN_DELAY_SEC,
)
from .http_api import XueqiuHttpApi
from .http_debug import env_flag_enabled
from .xq_api import ApiConfig


XQ_PORTFOLIO_KEY_ENV = "XQ_PORTFOLIO_KEY"
XQ_HTTP_DEBUG_ENV = "XQ_HTTP_DEBUG"

API_KEY_QUERY_PARAM = "key"

DEFAULT_HISTORY_COUNT = 20
MAX_HISTORY_COUNT = 200
DEFAULT_HISTORY_PAGE = 1
PORTFOLIO_SYMBOL_PATTERN = re.compile(r"\bZH[0-9A-Z]+\b", re.IGNORECASE)

_UPSTREAM_LOCK = threading.Lock()

router = APIRouter()


def _env_str(name: str) -> str:
    return str(os.environ.get(name, "") or "").strip()


def _query_first(query: dict[str, list[str]], name: str) -> str:
    values = query.get(name)
    if not isinstance(values, list) or not values:
        return ""
    return str(values[0] or "").strip()


def _parse_int(
    query: dict[str, list[str]],
    name: str,
    *,
    default: int,
) -> int:
    raw = _query_first(query, name)
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception as e:
        raise ValueError(f"{name} must be an integer") from e


def _parse_positive_int(
    query: dict[str, list[str]],
    name: str,
    *,
    default: int,
    min_value: int = 1,
    max_value: Optional[int] = None,
) -> int:
    value = _parse_int(query, name, default=default)
    if value < int(min_value):
        raise ValueError(f"{name} must be >= {int(min_value)}")
    if max_value is not None:
        value = min(int(value), int(max_value))
    return int(value)


def _require_api_key(query: dict[str, list[str]]) -> Optional[PlainTextResponse]:
    expected_key = _env_str(XQ_PORTFOLIO_KEY_ENV)
    if not expected_key:
        return PlainTextResponse(
            f"service missing {XQ_PORTFOLIO_KEY_ENV}\n", status_code=503
        )
    got_key = _query_first(query, API_KEY_QUERY_PARAM)
    if not got_key or got_key != expected_key:
        return PlainTextResponse("key invalid\n", status_code=401)
    return None


def _build_api() -> XueqiuHttpApi:
    cfg = ApiConfig(
        min_delay_sec=float(DEFAULT_MIN_DELAY_SEC),
        jitter_sec=float(DEFAULT_JITTER_SEC),
        max_retries=int(DEFAULT_MAX_RETRIES),
        max_consecutive_blocks=int(DEFAULT_MAX_CONSECUTIVE_BLOCKS),
        http_debug=env_flag_enabled(_env_str(XQ_HTTP_DEBUG_ENV)),
    )
    return XueqiuHttpApi.from_env(cfg)


def _json_upstream_error(exc: Exception) -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": str(exc)},
        status_code=502,
    )


def _extract_symbol_from_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    matched = PORTFOLIO_SYMBOL_PATTERN.search(text)
    return str(matched.group(0)).upper() if matched else ""


def _simplify_current(raw: dict[str, Any]) -> dict[str, Any]:
    """从上游 current.json 响应中提取调用方关心的字段。"""
    last_rb = raw.get("last_success_rb") or raw.get("last_rb") or {}
    holdings = last_rb.get("holdings")
    return {
        "holdings": [
            {
                "stock_symbol": h.get("stock_symbol"),
                "stock_name": h.get("stock_name"),
                "weight": h.get("weight"),
                "volume": h.get("volume"),
            }
            for h in holdings
        ]
        if holdings
        else [],
        "cash": last_rb.get("cash"),
        "updated_at": last_rb.get("updated_at"),
    }


@router.get("/portfolios/{portfolio_symbol}/snapshot")
def portfolio_snapshot(portfolio_symbol: str, request: Request) -> Response:
    symbol = _extract_symbol_from_text(portfolio_symbol)
    if not symbol:
        return PlainTextResponse("portfolio_symbol is invalid\n", status_code=400)

    try:
        query = parse_qs(str(request.url.query or ""))
        auth_error = _require_api_key(query)
        if auth_error is not None:
            return auth_error
        count = _parse_positive_int(
            query,
            "count",
            default=DEFAULT_HISTORY_COUNT,
            min_value=1,
            max_value=MAX_HISTORY_COUNT,
        )
        page = _parse_positive_int(
            query,
            "page",
            default=DEFAULT_HISTORY_PAGE,
            min_value=1,
        )
    except Exception as e:
        return PlainTextResponse(f"bad request: {e}\n", status_code=400)

    try:
        with _UPSTREAM_LOCK:
            api = _build_api()
            current = api.fetch_portfolio_current(symbol)
            history = api.fetch_portfolio_rebalancing_history(
                symbol,
                count=count,
                page=page,
            )
        return JSONResponse(
            {
                "ok": True,
                "symbol": symbol,
                "count": count,
                "page": page,
                "url": f"https://xueqiu.com/P/{symbol}",
                "current": _simplify_current(current),
                "rebalancing_history": history,
            }
        )
    except Exception as e:
        return _json_upstream_error(e)



