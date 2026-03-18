from __future__ import annotations

from pathlib import Path

from .utils import sha256_text


class BrowserCapture:
    def capture(self, url: str) -> str | None:
        raise NotImplementedError


class NoopBrowserCapture(BrowserCapture):
    def capture(self, url: str) -> str | None:
        return None


class PlaywrightBrowserCapture(BrowserCapture):
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def capture(self, url: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("playwright is required for browser capture support") from exc

        target = self.root / f"{sha256_text(url)}.png"
        with sync_playwright() as playwright:  # pragma: no cover - external browser dependency
            browser = playwright.chromium.launch()
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")
            page.screenshot(path=str(target), full_page=True)
            browser.close()
        return str(target.resolve())
