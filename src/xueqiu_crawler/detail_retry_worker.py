from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Optional

from playwright.sync_api import sync_playwright

try:
    from playwright_stealth import Stealth
except Exception:
    Stealth = None

try:
    from playwright_stealth import stealth_sync
except Exception:
    stealth_sync = None

from .xq_api import ApiConfig, XueqiuApi


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="detail_retry_worker",
        description="Run one detail retry in a separate process.",
    )
    parser.add_argument("--status-id", required=True)
    parser.add_argument("--source-status-url", default="")
    parser.add_argument("--status-url", default="")
    parser.add_argument("--status-user-id", default="")
    parser.add_argument("--min-delay", type=float, required=True)
    parser.add_argument("--jitter", type=float, required=True)
    parser.add_argument("--max-retries", type=int, required=True)
    parser.add_argument("--max-consecutive-blocks", type=int, required=True)
    return parser.parse_args(argv)


def _apply_stealth(page) -> Optional[str]:
    if Stealth is not None:
        try:
            stealth = Stealth(init_scripts_only=True)
            apply_sync = getattr(stealth, "apply_stealth_sync", None)
            if callable(apply_sync):
                apply_sync(page.context)
                return "Stealth.apply_stealth_sync"
        except Exception:
            pass
    if stealth_sync is not None:
        try:
            stealth_sync(page)
            return "stealth_sync"
        except Exception:
            pass
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    api_cfg = ApiConfig(
        min_delay_sec=float(args.min_delay),
        jitter_sec=float(args.jitter),
        max_retries=int(args.max_retries),
        max_consecutive_blocks=int(args.max_consecutive_blocks),
    )

    result: dict[str, object] = {
        "line": None,
        "failure_reason": None,
        "stealth_mode": None,
        "debug": {},
    }
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            result["stealth_mode"] = _apply_stealth(page) or ""
            api = XueqiuApi(page, api_cfg)
            try:
                debug: dict[str, Any] = {}
                line, failure_reason = api.fetch_status_display_line(
                    str(args.status_id or "").strip(),
                    source_status_url=str(args.source_status_url or "").strip(),
                    status_url=str(args.status_url or "").strip(),
                    status_user_id=str(args.status_user_id or "").strip(),
                    debug=debug,
                )
                result["line"] = line
                result["failure_reason"] = failure_reason
                result["debug"] = debug
            finally:
                browser.close()
    except Exception as exc:
        result["line"] = None
        result["failure_reason"] = f"无头补抓异常：{exc}"
        print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
        return 1

    print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
