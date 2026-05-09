#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qsl, urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from xueqiu_crawler.constants import BASE_URL
from xueqiu_crawler.http_api import HttpClientConfig, XueqiuHttpApi
from xueqiu_crawler.waf_signer import MD5_QUERY_KEY, strip_md5_query_param
from xueqiu_crawler.xq_api import ApiConfig, _looks_like_html, _looks_like_waf_challenge


DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_BODY_HEAD_CHARS = 500
DEFAULT_OUTPUT_PATH = Path("data") / "md5_sign_flow_response.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="test_md5_sign_flow",
        description=(
            "Test the real Xueqiu request flow: strip old md5__1038, warm /hq, "
            "auto-sign on WAF challenge, and request the final endpoint."
        ),
    )
    parser.add_argument("--url", required=True, help="Target Xueqiu URL to test.")
    parser.add_argument(
        "--referrer",
        default=f"{BASE_URL}/hq",
        help=f"HTTP Referer to use. Default: {BASE_URL}/hq",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=DEFAULT_TIMEOUT_SEC,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT_SEC}",
    )
    parser.add_argument(
        "--save-json",
        type=Path,
        default=None,
        help="Optional file path to save the JSON body when the final response is JSON.",
    )
    parser.add_argument(
        "--body-head-chars",
        type=int,
        default=DEFAULT_BODY_HEAD_CHARS,
        help=f"How many leading characters of the response body to print. Default: {DEFAULT_BODY_HEAD_CHARS}",
    )
    parser.add_argument(
        "--print-cookie-names",
        action="store_true",
        help="Print the current cookie key names after the request.",
    )
    return parser.parse_args()


def _extract_query_value(url: str, key: str) -> str:
    for item_key, item_value in parse_qsl(
        urlsplit(str(url or "")).query, keep_blank_values=True
    ):
        if str(item_key) == str(key):
            return str(item_value or "")
    return ""


def _save_json(path: Path, payload: Any) -> Path:
    out_path = Path(path)
    if not out_path.is_absolute():
        out_path = PROJECT_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_path


def _cookie_key_names(cookie_header_value: str) -> list[str]:
    out: list[str] = []
    for part in str(cookie_header_value or "").split(";"):
        item = str(part or "").strip()
        if "=" not in item:
            continue
        key = str(item.split("=", 1)[0] or "").strip()
        if key:
            out.append(key)
    return out


def main() -> int:
    args = _parse_args()
    cookie = str(os.environ.get("XUEQIU_COOKIE", "") or "").strip()

    url = str(args.url or "").strip()
    if not url:
        raise SystemExit("--url 不能为空")

    unsigned_url = strip_md5_query_param(url)
    cfg = ApiConfig(
        min_delay_sec=0.0,
        jitter_sec=0.0,
        max_retries=0,
        max_consecutive_blocks=3,
        http_debug=bool(os.environ.get("XQ_HTTP_DEBUG")),
    )
    api = XueqiuHttpApi(
        cfg,
        HttpClientConfig(cookie=cookie, timeout_sec=float(args.timeout_sec)),
    )

    status, text, final_url = api._fetch_text_once(
        unsigned_url,
        referrer=str(args.referrer or "").strip() or None,
    )

    trace_id = str(getattr(api, "_last_trace_id", "") or "")
    generated_md5 = _extract_query_value(final_url, MD5_QUERY_KEY)
    last_signed_url = str(getattr(api, "_last_waf_signed_url", "") or "")
    last_generated_md5 = _extract_query_value(last_signed_url, MD5_QUERY_KEY)
    last_waf_round = int(getattr(api, "_last_waf_round", 0) or 0)
    last_waf_cookie_writes = int(getattr(api, "_last_waf_signed_cookie_writes", 0) or 0)
    last_waf_cache_hit = bool(getattr(api, "_last_waf_cache_hit", False))
    original_md5 = _extract_query_value(url, MD5_QUERY_KEY)
    json_ok = False
    payload: Optional[Any] = None
    json_type = ""
    json_keys: list[str] = []
    json_error = ""
    try:
        payload = json.loads(text)
        json_ok = True
        json_type = type(payload).__name__
        if isinstance(payload, dict):
            json_keys = [str(key) for key in list(payload.keys())[:10]]
    except Exception as exc:
        json_error = str(exc)

    save_path = None
    if json_ok:
        target_path = (
            Path(args.save_json) if args.save_json is not None else DEFAULT_OUTPUT_PATH
        )
        save_path = _save_json(target_path, payload)

    body_head = str(text or "")[: max(0, int(args.body_head_chars))].replace("\n", " ")
    print(f"original_url = {url}")
    print(f"unsigned_url = {unsigned_url}")
    print(f"trace_id = {trace_id}")
    print(f"final_url = {final_url}")
    print(f"status = {int(status)}")
    print(f"original_md5 = {original_md5}")
    print(f"generated_md5 = {generated_md5}")
    print(f"last_signed_url = {last_signed_url}")
    print(f"last_generated_md5 = {last_generated_md5}")
    print(f"last_waf_round = {last_waf_round}")
    print(f"last_waf_cookie_writes = {last_waf_cookie_writes}")
    print(f"last_waf_cache_hit = {last_waf_cache_hit}")
    print(f"initial_cookie_supplied = {bool(cookie)}")
    print(f"used_md5 = {bool(generated_md5)}")
    print(f"looks_html = {_looks_like_html(text)}")
    print(
        "looks_waf = "
        f"{_looks_like_waf_challenge(text) or ('md5__1038=' in str(final_url).lower())}"
    )
    print(f"json_ok = {json_ok}")
    print(f"json_type = {json_type}")
    print(f"json_keys = {json_keys}")
    print(f"json_error = {json_error}")
    print(f"saved_json = {str(save_path) if save_path is not None else ''}")
    if args.print_cookie_names:
        print(f"cookie_names = {_cookie_key_names(api._cookie_header_value())}")
    print(f"body_head = {body_head}")
    return 0 if json_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
