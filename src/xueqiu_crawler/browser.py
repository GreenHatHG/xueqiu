from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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


@dataclass
class BrowserConfig:
    headless: bool
    user_data_dir: Path
    chrome_channel: Optional[str]
    cdp_url: Optional[str]
    reduce_automation_fingerprint: bool


class BrowserSession:
    def __init__(self, cfg: BrowserConfig) -> None:
        self._cfg = cfg
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._ui_page: Optional[Page] = None
        self._api_page: Optional[Page] = None
        self._attached_over_cdp = bool(cfg.cdp_url)
        self._ui_page_owned = False

    def __enter__(self) -> "BrowserSession":
        self._pw = sync_playwright().start()
        chromium = self._pw.chromium

        if self._cfg.cdp_url:
            # Attach to an existing Chrome started with --remote-debugging-port.
            self._browser = chromium.connect_over_cdp(self._cfg.cdp_url)
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
