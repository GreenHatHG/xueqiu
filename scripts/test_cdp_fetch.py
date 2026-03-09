#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from xueqiu_crawler.browser import BrowserConfig, BrowserSession
from xueqiu_crawler.constants import BASE_URL, DEFAULT_USER_DATA_DIR
from xueqiu_crawler.xq_api import XueqiuApi, _looks_like_html, _looks_like_waf_challenge


DEFAULT_DELAY_SEC = 1.5
DEFAULT_REPEAT = 2
DEFAULT_TIMEOUT_MS = 30000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="test_cdp_fetch",
        description="Probe timeline/comments direct fetch behavior inside an existing Chrome CDP session.",
    )
    parser.add_argument(
        "--cdp", required=True, help="Chrome CDP endpoint, e.g. http://127.0.0.1:9222"
    )
    parser.add_argument(
        "--user-id", required=True, help="Xueqiu user id, e.g. 9650668145"
    )
    parser.add_argument(
        "--timeline-page-a", type=int, default=1, help="First timeline page to probe"
    )
    parser.add_argument(
        "--timeline-page-b", type=int, default=2, help="Second timeline page to probe"
    )
    parser.add_argument(
        "--comment-size", type=int, default=10, help="comments.json size parameter"
    )
    parser.add_argument(
        "--comments-only",
        action="store_true",
        help="Only run the deep comments cursor probe",
    )
    parser.add_argument(
        "--comments-max-pages",
        type=int,
        default=10,
        help="Maximum comments batches to probe in comments-only mode",
    )
    parser.add_argument(
        "--comments-start-max-id",
        type=int,
        default=-1,
        help="Starting max_id for comments cursor probe",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=DEFAULT_REPEAT,
        help="How many times to repeat same-param probe",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SEC,
        help="Delay between probes in seconds",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
        help="Per-request timeout in milliseconds",
    )
    parser.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_USER_DATA_DIR,
        help="Unused in CDP mode; kept only to satisfy BrowserConfig construction.",
    )
    return parser.parse_args()


def _sleep(delay_sec: float) -> None:
    if delay_sec > 0:
        time.sleep(float(delay_sec))


def _fetch_text(page: Any, *, url: str, timeout_ms: int) -> dict[str, Any]:
    return page.evaluate(
        """
        async ({ url, timeoutMs }) => {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), timeoutMs);
          try {
            const resp = await fetch(url, {
              method: 'GET',
              credentials: 'include',
              redirect: 'follow',
              signal: controller.signal,
              referrerPolicy: 'strict-origin-when-cross-origin',
              headers: {
                'accept': 'application/json, text/plain, */*',
                'x-requested-with': 'XMLHttpRequest',
              },
            });
            const text = await resp.text();
            return {
              ok: Boolean(resp.ok),
              status: Number(resp.status || 0),
              final_url: String(resp.url || url || ''),
              content_type: String(resp.headers.get('content-type') || ''),
              text: String(text || ''),
            };
          } catch (error) {
            return {
              ok: false,
              status: 0,
              final_url: String(url || ''),
              content_type: '',
              text: '',
              error: String(error || ''),
            };
          } finally {
            clearTimeout(timer);
          }
        }
        """,
        {"url": str(url), "timeoutMs": int(timeout_ms)},
    )


def _sample_ids(obj: Any) -> list[str]:
    if not isinstance(obj, dict):
        return []
    if isinstance(obj.get("statuses"), list):
        return [
            str(item.get("id"))
            for item in obj["statuses"][:5]
            if isinstance(item, dict) and item.get("id") is not None
        ]
    if isinstance(obj.get("items"), list):
        return [
            str(item.get("id"))
            for item in obj["items"][:5]
            if isinstance(item, dict) and item.get("id") is not None
        ]
    return []


def _record_count(obj: Any) -> int:
    if not isinstance(obj, dict):
        return -1
    if isinstance(obj.get("statuses"), list):
        return len(obj["statuses"])
    if isinstance(obj.get("items"), list):
        return len(obj["items"])
    return -1


def _oldest_created_at(obj: Any) -> Optional[int]:
    if not isinstance(obj, dict):
        return None
    items = obj.get("items")
    if not isinstance(items, list):
        return None
    values: list[int] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        value = item.get("created_at")
        if isinstance(value, (int, float)):
            values.append(int(value))
    return min(values) if values else None


def _semantic_fingerprint(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    payload = {
        "keys": list(obj.keys())[:10],
        "record_count": _record_count(obj),
        "sample_ids": _sample_ids(obj),
        "next_max_id": obj.get("next_max_id"),
        "page": obj.get("page"),
        "maxPage": obj.get("maxPage"),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]


def _analyze_response(label: str, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text") or "")
    final_url = str(payload.get("final_url") or url)
    looks_html = _looks_like_html(text)

    obj: Optional[Any] = None
    json_error = ""
    if text:
        try:
            obj = json.loads(text)
        except Exception as exc:
            json_error = str(exc)

    # Treat WAF as a transport/content failure signal. Once the body is valid JSON,
    # body-text heuristics are too noisy because normal timeline/comment content may
    # contain words like “验证/刷新”.
    looks_waf = bool(
        (obj is None)
        and (
            _looks_like_waf_challenge(text)
            or XueqiuApi._looks_like_challenge_url(final_url)
        )
    )

    return {
        "label": label,
        "url": url,
        "ok": bool(payload.get("ok")),
        "status": int(payload.get("status") or 0),
        "final_url": final_url,
        "content_type": str(payload.get("content_type") or ""),
        "looks_html": bool(looks_html),
        "looks_waf": bool(looks_waf),
        "looks_json": obj is not None,
        "json_type": type(obj).__name__ if obj is not None else "",
        "json_keys": list(obj.keys())[:10] if isinstance(obj, dict) else [],
        "record_count": _record_count(obj),
        "sample_ids": _sample_ids(obj),
        "next_max_id": obj.get("next_max_id") if isinstance(obj, dict) else None,
        "oldest_created_at": _oldest_created_at(obj),
        "text_hash": hashlib.sha256(text.encode("utf-8")).hexdigest()[:16],
        "semantic_hash": _semantic_fingerprint(obj),
        "text_head": text[:160],
        "json_error": json_error,
        "obj": obj,
    }


def _compare_same_param(results: list[dict[str, Any]]) -> dict[str, Any]:
    hashes = [item.get("text_hash") for item in results]
    semantic_hashes = [item.get("semantic_hash") for item in results]
    sample_ids = [tuple(item.get("sample_ids") or []) for item in results]
    return {
        "all_hash_equal": len(set(hashes)) == 1 if hashes else False,
        "all_semantic_hash_equal": len(set(semantic_hashes)) == 1
        if semantic_hashes
        else False,
        "all_sample_ids_equal": len(set(sample_ids)) == 1 if sample_ids else False,
    }


def _compare_diff_param(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {
        "hash_equal": left.get("text_hash") == right.get("text_hash"),
        "semantic_hash_equal": left.get("semantic_hash") == right.get("semantic_hash"),
        "sample_ids_equal": tuple(left.get("sample_ids") or [])
        == tuple(right.get("sample_ids") or []),
    }


def _print_probe(result: dict[str, Any]) -> None:
    print(
        json.dumps(
            {
                "label": result["label"],
                "status": result["status"],
                "looks_json": result["looks_json"],
                "looks_html": result["looks_html"],
                "looks_waf": result["looks_waf"],
                "record_count": result["record_count"],
                "sample_ids": result["sample_ids"],
                "next_max_id": result["next_max_id"],
                "oldest_created_at": result["oldest_created_at"],
                "text_hash": result["text_hash"],
                "semantic_hash": result["semantic_hash"],
            },
            ensure_ascii=False,
        )
    )


def _run_comments_deep_probe(page: Any, args: argparse.Namespace) -> int:
    cursor = int(args.comments_start_max_id)
    page_summaries: list[dict[str, Any]] = []

    for idx in range(1, max(1, int(args.comments_max_pages)) + 1):
        if idx > 1:
            _sleep(float(args.delay))
        url = XueqiuApi.build_url(
            "/statuses/user/comments.json",
            {
                "user_id": str(args.user_id),
                "size": int(args.comment_size),
                "max_id": cursor,
            },
        )
        result = _analyze_response(
            label=f"comments_cursor_batch_{idx}",
            url=url,
            payload=_fetch_text(page, url=url, timeout_ms=int(args.timeout_ms)),
        )
        _print_probe(result)
        if result["looks_html"] or result["looks_waf"] or not result["looks_json"]:
            print(
                json.dumps(
                    {
                        "stopped": True,
                        "reason": "comments_cursor_blocked",
                        "batch": idx,
                        "cursor": cursor,
                    },
                    ensure_ascii=False,
                )
            )
            return 2

        page_summaries.append(result)
        next_max_id = result.get("next_max_id")
        if next_max_id in (None, "", -1, "-1"):
            break
        cursor = int(str(next_max_id))

    comparisons: list[dict[str, Any]] = []
    for idx in range(1, len(page_summaries)):
        prev_result = page_summaries[idx - 1]
        current_result = page_summaries[idx]
        comparisons.append(
            {
                "from": prev_result["label"],
                "to": current_result["label"],
                **_compare_diff_param(prev_result, current_result),
            }
        )

    summary = {
        "comments_only": True,
        "pages_probed": len(page_summaries),
        "all_batches_json": all(item.get("looks_json") for item in page_summaries),
        "sample_heads": [item.get("sample_ids") for item in page_summaries],
        "next_max_ids": [item.get("next_max_id") for item in page_summaries],
        "oldest_created_at": [item.get("oldest_created_at") for item in page_summaries],
        "adjacent_diffs": comparisons,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    args = _parse_args()

    browser_cfg = BrowserConfig(
        headless=False,
        user_data_dir=Path(args.user_data_dir),
        chrome_channel=None,
        cdp_url=str(args.cdp),
        reduce_automation_fingerprint=False,
    )

    timeline_url_a = XueqiuApi.build_url(
        "/v4/statuses/user_timeline.json",
        {"page": int(args.timeline_page_a), "user_id": str(args.user_id)},
    )
    timeline_url_b = XueqiuApi.build_url(
        "/v4/statuses/user_timeline.json",
        {"page": int(args.timeline_page_b), "user_id": str(args.user_id)},
    )
    comments_url_first = XueqiuApi.build_url(
        "/statuses/user/comments.json",
        {"user_id": str(args.user_id), "size": int(args.comment_size), "max_id": -1},
    )

    with BrowserSession(browser_cfg) as session:
        page = session.ui_page
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=args.timeout_ms)
        except Exception:
            pass

        if args.comments_only:
            return _run_comments_deep_probe(page, args)

        timeline_repeats: list[dict[str, Any]] = []
        for idx in range(max(1, int(args.repeat))):
            if idx > 0:
                _sleep(float(args.delay))
            timeline_repeats.append(
                _analyze_response(
                    label=f"timeline_page_{args.timeline_page_a}_repeat_{idx + 1}",
                    url=timeline_url_a,
                    payload=_fetch_text(
                        page, url=timeline_url_a, timeout_ms=int(args.timeout_ms)
                    ),
                )
            )
            _print_probe(timeline_repeats[-1])
            if (
                timeline_repeats[-1]["looks_html"]
                or timeline_repeats[-1]["looks_waf"]
                or not timeline_repeats[-1]["looks_json"]
            ):
                print(
                    json.dumps(
                        {"stopped": True, "reason": "timeline_same_param_blocked"},
                        ensure_ascii=False,
                    )
                )
                return 2

        _sleep(float(args.delay))
        timeline_other = _analyze_response(
            label=f"timeline_page_{args.timeline_page_b}",
            url=timeline_url_b,
            payload=_fetch_text(
                page, url=timeline_url_b, timeout_ms=int(args.timeout_ms)
            ),
        )
        _print_probe(timeline_other)
        if (
            timeline_other["looks_html"]
            or timeline_other["looks_waf"]
            or not timeline_other["looks_json"]
        ):
            print(
                json.dumps(
                    {"stopped": True, "reason": "timeline_diff_param_blocked"},
                    ensure_ascii=False,
                )
            )
            return 2

        comments_repeats: list[dict[str, Any]] = []
        for idx in range(max(1, int(args.repeat))):
            _sleep(float(args.delay))
            comments_repeats.append(
                _analyze_response(
                    label=f"comments_first_batch_repeat_{idx + 1}",
                    url=comments_url_first,
                    payload=_fetch_text(
                        page, url=comments_url_first, timeout_ms=int(args.timeout_ms)
                    ),
                )
            )
            _print_probe(comments_repeats[-1])
            if (
                comments_repeats[-1]["looks_html"]
                or comments_repeats[-1]["looks_waf"]
                or not comments_repeats[-1]["looks_json"]
            ):
                print(
                    json.dumps(
                        {"stopped": True, "reason": "comments_same_param_blocked"},
                        ensure_ascii=False,
                    )
                )
                return 2

        next_max_id = comments_repeats[0].get("next_max_id")
        comments_other: Optional[dict[str, Any]] = None
        if next_max_id not in (None, "", -1, "-1"):
            comments_url_next = XueqiuApi.build_url(
                "/statuses/user/comments.json",
                {
                    "user_id": str(args.user_id),
                    "size": int(args.comment_size),
                    "max_id": next_max_id,
                },
            )
            _sleep(float(args.delay))
            comments_other = _analyze_response(
                label=f"comments_next_batch_{next_max_id}",
                url=comments_url_next,
                payload=_fetch_text(
                    page, url=comments_url_next, timeout_ms=int(args.timeout_ms)
                ),
            )
            _print_probe(comments_other)
            if (
                comments_other["looks_html"]
                or comments_other["looks_waf"]
                or not comments_other["looks_json"]
            ):
                print(
                    json.dumps(
                        {"stopped": True, "reason": "comments_diff_param_blocked"},
                        ensure_ascii=False,
                    )
                )
                return 2

        summary = {
            "timeline_same_param": _compare_same_param(timeline_repeats),
            "timeline_diff_param": _compare_diff_param(
                timeline_repeats[0], timeline_other
            ),
            "comments_same_param": _compare_same_param(comments_repeats),
            "comments_diff_param": _compare_diff_param(
                comments_repeats[0], comments_other
            )
            if comments_other
            else None,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
