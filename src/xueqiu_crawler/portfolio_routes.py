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

DEFAULT_FOLLOWED_PORTFOLIO_SIZE = 1000
MAX_FOLLOWED_PORTFOLIO_SIZE = 1000
DEFAULT_FOLLOWED_PORTFOLIO_CATEGORY = 3
DEFAULT_FOLLOWED_PORTFOLIO_PID = -120
DEFAULT_HISTORY_COUNT = 20
MAX_HISTORY_COUNT = 200
DEFAULT_HISTORY_PAGES = 1
MAX_HISTORY_PAGES = 10
DEFAULT_HISTORY_PAGE = 1
DEFAULT_MAX_PORTFOLIOS = 10
MAX_PORTFOLIOS = 200
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


def _parse_bool(
    query: dict[str, list[str]],
    name: str,
    *,
    default: bool,
) -> bool:
    raw = _query_first(query, name)
    if not raw:
        return bool(default)
    value = raw.lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


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


def _extract_follow_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    containers: list[dict[str, Any]] = [payload]
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        containers.append(data)

    for container in containers:
        for key in ("stocks", "list", "items", "portfolios"):
            value = container.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _extract_symbol_from_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    matched = PORTFOLIO_SYMBOL_PATTERN.search(text)
    return str(matched.group(0)).upper() if matched else ""


def _portfolio_symbol(item: dict[str, Any]) -> str:
    for key in (
        "symbol",
        "stock_symbol",
        "stockSymbol",
        "cube_symbol",
        "cubeSymbol",
        "code",
    ):
        symbol = _extract_symbol_from_text(item.get(key))
        if symbol:
            return symbol

    for key in ("stock", "portfolio", "cube"):
        nested = item.get(key)
        if not isinstance(nested, dict):
            continue
        for nested_key in (
            "symbol",
            "stock_symbol",
            "stockSymbol",
            "cube_symbol",
            "cubeSymbol",
            "code",
        ):
            symbol = _extract_symbol_from_text(nested.get(nested_key))
            if symbol:
                return symbol

    for value in item.values():
        symbol = _extract_symbol_from_text(value)
        if symbol:
            return symbol
    return ""


def _extract_portfolio_rows(
    payload: Any,
    *,
    max_portfolios: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in _extract_follow_items(payload):
        symbol = _portfolio_symbol(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        rows.append(
            {
                "symbol": symbol,
                "url": f"https://xueqiu.com/P/{symbol}",
                "follow_item": item,
            }
        )
        if len(rows) >= int(max_portfolios):
            break
    return rows


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


@router.get("/users/{user_id}/followed-portfolios")
def user_followed_portfolios(user_id: str, request: Request) -> Response:
    uid = str(user_id or "").strip()
    if not uid:
        return PlainTextResponse("user_id is empty\n", status_code=400)

    try:
        query = parse_qs(str(request.url.query or ""))
        auth_error = _require_api_key(query)
        if auth_error is not None:
            return auth_error
        size = _parse_positive_int(
            query,
            "size",
            default=DEFAULT_FOLLOWED_PORTFOLIO_SIZE,
            min_value=1,
            max_value=MAX_FOLLOWED_PORTFOLIO_SIZE,
        )
        category = _parse_int(
            query,
            "category",
            default=DEFAULT_FOLLOWED_PORTFOLIO_CATEGORY,
        )
        pid = _parse_int(query, "pid", default=DEFAULT_FOLLOWED_PORTFOLIO_PID)
    except Exception as e:
        return PlainTextResponse(f"bad request: {e}\n", status_code=400)

    try:
        with _UPSTREAM_LOCK:
            api = _build_api()
            result = api.fetch_user_followed_portfolios(
                uid,
                size=size,
                category=category,
                pid=pid,
            )
        return JSONResponse(
            {
                "ok": True,
                "user_id": uid,
                "size": size,
                "category": category,
                "pid": pid,
                "result": result,
            }
        )
    except Exception as e:
        return _json_upstream_error(e)


@router.get("/users/{user_id}/followed-portfolios/snapshots")
def user_followed_portfolio_snapshots(user_id: str, request: Request) -> Response:
    uid = str(user_id or "").strip()
    if not uid:
        return PlainTextResponse("user_id is empty\n", status_code=400)

    try:
        query = parse_qs(str(request.url.query or ""))
        auth_error = _require_api_key(query)
        if auth_error is not None:
            return auth_error
        size = _parse_positive_int(
            query,
            "size",
            default=DEFAULT_FOLLOWED_PORTFOLIO_SIZE,
            min_value=1,
            max_value=MAX_FOLLOWED_PORTFOLIO_SIZE,
        )
        category = _parse_int(
            query,
            "category",
            default=DEFAULT_FOLLOWED_PORTFOLIO_CATEGORY,
        )
        pid = _parse_int(query, "pid", default=DEFAULT_FOLLOWED_PORTFOLIO_PID)
        history_count = _parse_positive_int(
            query,
            "history_count",
            default=DEFAULT_HISTORY_COUNT,
            min_value=1,
            max_value=MAX_HISTORY_COUNT,
        )
        history_pages = _parse_positive_int(
            query,
            "history_pages",
            default=DEFAULT_HISTORY_PAGES,
            min_value=1,
            max_value=MAX_HISTORY_PAGES,
        )
        max_portfolios = _parse_positive_int(
            query,
            "max_portfolios",
            default=DEFAULT_MAX_PORTFOLIOS,
            min_value=1,
            max_value=MAX_PORTFOLIOS,
        )
        include_quote = _parse_bool(query, "include_quote", default=False)
        include_current = _parse_bool(query, "include_current", default=True)
        include_history = _parse_bool(query, "include_history", default=True)
    except Exception as e:
        return PlainTextResponse(f"bad request: {e}\n", status_code=400)

    try:
        portfolios: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        with _UPSTREAM_LOCK:
            api = _build_api()
            followed_payload = api.fetch_user_followed_portfolios(
                uid,
                size=size,
                category=category,
                pid=pid,
            )
            followed_rows = _extract_portfolio_rows(
                followed_payload,
                max_portfolios=max_portfolios,
            )

            for row in followed_rows:
                symbol = str(row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                try:
                    current = (
                        api.fetch_portfolio_current(symbol) if include_current else None
                    )
                    history_pages_payload = (
                        [
                            api.fetch_portfolio_rebalancing_history(
                                symbol,
                                count=history_count,
                                page=page,
                            )
                            for page in range(1, history_pages + 1)
                        ]
                        if include_history
                        else []
                    )
                except Exception as e:
                    errors.append(
                        {
                            "symbol": symbol,
                            "url": row.get("url") or f"https://xueqiu.com/P/{symbol}",
                            "stage": "snapshot",
                            "error": str(e),
                        }
                    )
                    continue

                out = dict(row)
                if include_current:
                    out["current"] = _simplify_current(current)
                if include_history:
                    out["rebalancing_history"] = {
                        "count": history_count,
                        "pages": history_pages_payload,
                    }
                if include_quote:
                    try:
                        out["quote"] = api.fetch_portfolio_quote(symbol)
                    except Exception as e:
                        errors.append(
                            {
                                "symbol": symbol,
                                "url": row.get("url") or f"https://xueqiu.com/P/{symbol}",
                                "stage": "quote",
                                "error": str(e),
                            }
                        )
                        out["quote"] = None
                portfolios.append(out)

        return JSONResponse(
            {
                "ok": True,
                "user_id": uid,
                "size": size,
                "category": category,
                "pid": pid,
                "history_count": history_count,
                "history_pages": history_pages,
                "max_portfolios": max_portfolios,
                "include_quote": include_quote,
                "include_current": include_current,
                "include_history": include_history,
                "count": len(portfolios),
                "portfolios": portfolios,
                "errors": errors,
            }
        )
    except Exception as e:
        return _json_upstream_error(e)


@router.get("/portfolios/{portfolio_symbol}/snapshot")
@router.get("/portfolios/{portfolio_symbol}/rebalancing-history")
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


portfolio_rebalancing_history = portfolio_snapshot
