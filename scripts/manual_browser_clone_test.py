#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from xueqiu_crawler.cli import _build_browser_profiles_root, _copy_browser_profile_dir
from xueqiu_crawler.constants import BASE_URL, DEFAULT_OUTPUT_DIR, DEFAULT_USER_DATA_DIR

CHROME_ENV_EXECUTABLE = "CHROME_EXECUTABLE"
CHROME_DEFAULT_NAMES = (
    "google-chrome",
    "google-chrome-stable",
    "chrome",
    "chromium",
    "chromium-browser",
)
CHROME_MACOS_PATHS = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
)
CHROME_WINDOWS_PATHS = (
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="manual_browser_clone_test",
        description="Copy a fresh browser profile and open Xueqiu pages for manual checking.",
    )
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="Direct URL to open. You can pass this more than once.",
    )
    parser.add_argument("--status-id", help="Status id to build detail page URLs.")
    parser.add_argument(
        "--status-user-id",
        help="Status owner user id. If given, the script also opens /{user_id}/{status_id}.",
    )
    parser.add_argument(
        "--source-status-url",
        default="",
        help="Optional source status URL to open first.",
    )
    parser.add_argument(
        "--status-url",
        default="",
        help="Optional status URL to open first.",
    )
    parser.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_USER_DATA_DIR,
        help="Base browser profile directory to copy from.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output root used to place copied browser profiles.",
    )
    parser.add_argument(
        "--chrome-channel",
        default="chrome",
        help="Chrome executable name or full path.",
    )
    parser.add_argument(
        "--reduce-automation-fingerprint",
        action="store_true",
        help="Add --disable-blink-features=AutomationControlled when starting Chrome.",
    )
    parser.add_argument(
        "--cleanup-profile",
        action="store_true",
        help="Delete the copied profile directory after Chrome exits.",
    )
    return parser.parse_args()


def _resolve_chrome_executable(channel: str) -> str:
    raw_env = str(os.environ.get(CHROME_ENV_EXECUTABLE, "")).strip()
    if raw_env:
        expanded = Path(raw_env).expanduser()
        if expanded.is_file():
            return str(expanded)
        found = shutil.which(raw_env)
        if found:
            return found

    candidates: list[str] = []
    channel_text = str(channel or "").strip()
    if channel_text:
        candidates.append(channel_text)
    candidates.extend(CHROME_DEFAULT_NAMES)

    for candidate in candidates:
        found = shutil.which(candidate)
        if found:
            return found
        expanded = Path(candidate).expanduser()
        if expanded.is_file():
            return str(expanded)

    system_name = platform.system()
    if system_name == "Darwin":
        for raw_path in CHROME_MACOS_PATHS:
            path = Path(raw_path).expanduser()
            if path.is_file():
                return str(path)
    elif system_name == "Windows":
        for raw_path in CHROME_WINDOWS_PATHS:
            path = Path(raw_path)
            if path.is_file():
                return str(path)

    raise RuntimeError(
        "没找到 Chrome 可执行文件。你可以传 --chrome-channel，或者把环境变量 "
        "CHROME_EXECUTABLE 指到 Chrome 可执行文件。"
    )


def _collect_urls(args: argparse.Namespace) -> list[str]:
    urls: list[str] = []
    for candidate in args.url:
        text = str(candidate or "").strip()
        if text and text not in urls:
            urls.append(text)

    status_id = str(args.status_id or "").strip()
    status_user_id = str(args.status_user_id or "").strip()
    for candidate in (
        str(args.source_status_url or "").strip(),
        str(args.status_url or "").strip(),
        f"{BASE_URL}/{status_user_id}/{status_id}"
        if status_id and status_user_id
        else "",
        f"{BASE_URL}/status/{status_id}" if status_id else "",
    ):
        if candidate and candidate not in urls:
            urls.append(candidate)

    if not urls:
        urls.append(BASE_URL)
    return urls


def _build_manual_profile_dir(out_dir: Path, status_id: str) -> Path:
    profiles_root = _build_browser_profiles_root(Path(out_dir))
    label = f"manual_status_{status_id}" if status_id else "manual_browser_test"
    return profiles_root / label


def main() -> int:
    args = _parse_args()
    urls = _collect_urls(args)
    status_id = str(args.status_id or "").strip()
    source_dir = Path(args.user_data_dir).expanduser().resolve()
    target_dir = _build_manual_profile_dir(Path(args.out_dir).expanduser(), status_id)

    try:
        _copy_browser_profile_dir(source_dir, target_dir)
    except Exception as exc:
        print(f"复制浏览器资料目录失败：{exc}", file=sys.stderr)
        return 2

    try:
        chrome_path = _resolve_chrome_executable(str(args.chrome_channel or ""))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    command = [
        chrome_path,
        f"--user-data-dir={target_dir}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    if args.reduce_automation_fingerprint:
        command.append("--disable-blink-features=AutomationControlled")
    command.extend(urls)

    print(f"母本浏览器目录：{source_dir}")
    print(f"复制后的测试目录：{target_dir}")
    print("这次会打开这些地址：")
    for url in urls:
        print(f"- {url}")
    print("浏览器关掉后，这个脚本才会结束。")

    proc: subprocess.Popen[str] | None = None
    try:
        proc = subprocess.Popen(  # noqa: S603
            command,
            start_new_session=True,
            text=True,
        )
        assert proc is not None
        return int(proc.wait())
    except KeyboardInterrupt:
        print("收到中断，准备关掉这次测试浏览器。", file=sys.stderr)
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=5)
                except Exception:
                    pass
        return 130
    finally:
        if args.cleanup_profile and target_dir.exists():
            try:
                shutil.rmtree(target_dir)
            except Exception as exc:
                print(f"清理测试浏览器目录失败：{exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
