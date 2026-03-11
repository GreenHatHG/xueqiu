from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import time
from typing import Optional
from urllib.request import Request, urlopen

from .constants import BASE_URL

try:
    from playwright.sync_api import (
        Browser,
        BrowserContext,
        Page,
        Playwright,
        sync_playwright,
    )
except ModuleNotFoundError as e:  # pragma: no cover
    raise RuntimeError(
        "未安装 Playwright 依赖，程序无法启动。请先安装依赖并下载浏览器：\n"
        "1) 安装依赖：pip install playwright（或按 README 使用 uv sync）\n"
        "2) 安装浏览器：python -m playwright install chromium"
    ) from e


LOCALHOST_CDP_ADDRESS = "127.0.0.1"
MANAGED_CDP_STARTUP_TIMEOUT_SEC = 15.0
MANAGED_CDP_POLL_INTERVAL_SEC = 0.25
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


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((LOCALHOST_CDP_ADDRESS, 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _resolve_chrome_executable(channel: Optional[str]) -> str:
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
        "没找到 Chrome 可执行文件。你可以先装好 Google Chrome，"
        "或者把环境变量 CHROME_EXECUTABLE 指到 Chrome 的可执行文件。"
    )


def _wait_for_cdp_ready(cdp_url: str, timeout_sec: float) -> None:
    version_url = f"{cdp_url.rstrip('/')}/json/version"
    deadline = time.time() + float(timeout_sec)
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            req = Request(version_url, headers={"accept": "application/json"})
            with urlopen(req, timeout=1.0) as resp:  # nosec - local loopback only
                if int(resp.status) < 400:
                    return
        except Exception as exc:  # pragma: no cover - depends on local Chrome timing
            last_error = exc
        time.sleep(MANAGED_CDP_POLL_INTERVAL_SEC)
    raise RuntimeError(f"等待 Chrome 调试端口超时：{last_error}")


@dataclass
class BrowserConfig:
    headless: bool
    user_data_dir: Path
    chrome_channel: Optional[str]
    cdp_url: Optional[str]
    reduce_automation_fingerprint: bool
    manage_cdp: bool = False


class BrowserSession:
    def __init__(self, cfg: BrowserConfig) -> None:
        self._cfg = cfg
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._ui_page: Optional[Page] = None
        self._api_page: Optional[Page] = None
        self._attached_over_cdp = bool(cfg.cdp_url or cfg.manage_cdp)
        self._chrome_process: Optional[subprocess.Popen[str]] = None
        self._ui_page_owned = False

    def _launch_managed_chrome(self) -> str:
        executable = _resolve_chrome_executable(self._cfg.chrome_channel)
        port = _pick_free_port()
        user_data_dir = Path(self._cfg.user_data_dir)
        user_data_dir.mkdir(parents=True, exist_ok=True)

        command = [
            executable,
            f"--remote-debugging-address={LOCALHOST_CDP_ADDRESS}",
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if self._cfg.headless:
            command.append("--headless=new")
        if self._cfg.reduce_automation_fingerprint:
            command.append("--disable-blink-features=AutomationControlled")

        self._chrome_process = subprocess.Popen(  # noqa: S603
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            text=True,
        )
        cdp_url = f"http://{LOCALHOST_CDP_ADDRESS}:{port}"
        _wait_for_cdp_ready(cdp_url, MANAGED_CDP_STARTUP_TIMEOUT_SEC)
        return cdp_url

    def _stop_managed_chrome(self) -> None:
        if self._chrome_process is None:
            return
        if self._chrome_process.poll() is not None:
            return
        try:
            self._chrome_process.terminate()
            self._chrome_process.wait(timeout=5)
        except Exception:
            try:
                self._chrome_process.kill()
                self._chrome_process.wait(timeout=5)
            except Exception:
                pass

    def __enter__(self) -> "BrowserSession":
        try:
            self._pw = sync_playwright().start()
            chromium = self._pw.chromium

            effective_cdp_url = str(self._cfg.cdp_url or "").strip()
            if self._cfg.manage_cdp and not effective_cdp_url:
                effective_cdp_url = self._launch_managed_chrome()

            if effective_cdp_url:
                # Attach to an existing or managed Chrome started with --remote-debugging-port.
                self._browser = chromium.connect_over_cdp(effective_cdp_url)
                contexts = self._browser.contexts
                self._context = contexts[0] if contexts else self._browser.new_context()
            else:
                args = None
                ignore_default_args = None
                if self._cfg.reduce_automation_fingerprint:
                    # A minimal mitigation to reduce obvious automation markers.
                    # NOTE: This is not a guarantee and should still be used with conservative rate limits.
                    args = ["--disable-blink-features=AutomationControlled"]
                    ignore_default_args = ["--enable-automation"]
                self._context = chromium.launch_persistent_context(
                    user_data_dir=str(self._cfg.user_data_dir),
                    headless=self._cfg.headless,
                    channel=self._cfg.chrome_channel,
                    args=args,
                    ignore_default_args=ignore_default_args,
                )

            if self._cfg.reduce_automation_fingerprint and self._context is not None:
                self._context.add_init_script(
                    """
                    // Remove obvious webdriver flag.
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    """
                )

            if self._context is None:
                raise RuntimeError("failed to create browser context")

            # Keep two tabs:
            # - UI tab: for manual login / WAF challenges.
            # - API tab: for programmatic navigation to JSON endpoints.
            #
            # In CDP attach mode, prefer reusing an existing Xueqiu tab for UI interactions,
            # so manual verification happens in the same stable session the user already sees.
            if self._attached_over_cdp:
                self._ui_page, self._ui_page_owned = self._pick_cdp_ui_page()
                self._api_page = self._context.new_page()
            else:
                self._ui_page = (
                    self._context.pages[0]
                    if self._context.pages
                    else self._context.new_page()
                )
                self._api_page = self._context.new_page()
            return self
        except Exception:
            self._stop_managed_chrome()
            if self._pw is not None:
                try:
                    self._pw.stop()
                except Exception:
                    pass
            raise

    def _pick_cdp_ui_page(self) -> tuple[Page, bool]:
        assert self._context is not None
        pages = list(self._context.pages)
        for page in pages:
            try:
                url = str(page.url or "")
            except Exception:
                url = ""
            if url.startswith(BASE_URL):
                return page, False
        if pages:
            return pages[0], False
        return self._context.new_page(), True

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        # Important: in CDP attach mode, do not close the user's browser/context.
        if not self._attached_over_cdp:
            try:
                if self._context is not None:
                    self._context.close()
            finally:
                if self._browser is not None:
                    self._browser.close()
        else:
            try:
                if self._api_page is not None:
                    self._api_page.close()
                if (
                    self._ui_page_owned
                    and self._ui_page is not None
                    and self._ui_page is not self._api_page
                ):
                    self._ui_page.close()
            except Exception:
                pass
            if self._cfg.manage_cdp:
                try:
                    if self._browser is not None:
                        self._browser.close()
                except Exception:
                    pass
                self._stop_managed_chrome()
        if self._pw is not None:
            self._pw.stop()

    @property
    def page(self) -> Page:
        # Use the API tab as the default page handle.
        assert self._api_page is not None
        return self._api_page

    @property
    def ui_page(self) -> Page:
        assert self._ui_page is not None
        return self._ui_page

    @property
    def prefer_page_fetch(self) -> bool:
        return bool(self._attached_over_cdp)
