"""Playwright-based browser client — drop-in replacement for PinchTabClient.

Provides the same interface (navigate, evaluate, screenshot, instance management)
but drives Playwright directly instead of proxying through a PinchTab HTTP server.
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from .config import Settings
from .models import BrowserMode

logger = logging.getLogger(__name__)


@dataclass
class _BrowserInstance:
    """Internal state for a single browser instance."""

    instance_id: str
    browser: Any  # playwright.sync_api.Browser
    context: Any  # playwright.sync_api.BrowserContext
    page: Any  # playwright.sync_api.Page
    mode: BrowserMode
    created_at: float = field(default_factory=time.monotonic)


class PlaywrightClient:
    """Browser automation client backed by Playwright.

    Provides the same public interface as the former PinchTabClient so the
    GoogleImagesBrowserAdapter can use it without modification.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._pw: Any | None = None  # lazy playwright instance
        self._instances: dict[str, _BrowserInstance] = {}

    # ------------------------------------------------------------------
    # Instance lifecycle
    # ------------------------------------------------------------------

    def instance_start(
        self,
        mode: BrowserMode | str | None = None,
        profile_id: str | None = None,
    ) -> dict[str, Any]:
        """Launch a new browser instance and return its metadata."""
        resolved_mode = BrowserMode(mode) if isinstance(mode, str) else (mode or BrowserMode(self.settings.playwright_browser_mode))
        pw = self._get_playwright()
        headless = resolved_mode == BrowserMode.headless
        browser = pw.chromium.launch(
            headless=headless,
            args=self.settings.playwright_launch_args,
        )
        context = browser.new_context(
            user_agent=self.settings.user_agent,
            viewport={"width": 1280, "height": 900},
            device_scale_factor=1,
        )
        page = context.new_page()
        instance_id = str(uuid4())
        inst = _BrowserInstance(
            instance_id=instance_id,
            browser=browser,
            context=context,
            page=page,
            mode=resolved_mode,
        )
        self._instances[instance_id] = inst
        logger.info("Playwright instance %s started (mode=%s)", instance_id, resolved_mode.value)
        return {"instanceId": instance_id, "id": instance_id, "mode": resolved_mode.value}

    def wait_for_instance_ready(self, instance_id: str, timeout_seconds: float | None = None) -> None:
        """No-op for Playwright — instances are ready immediately after launch."""
        if instance_id not in self._instances:
            raise RuntimeError(f"Unknown Playwright instance: {instance_id}")

    def instance_health(self, instance_id: str) -> dict[str, Any]:
        """Check whether an instance's page is still usable."""
        inst = self._get(instance_id)
        try:
            # A lightweight evaluate to confirm the page is alive
            inst.page.evaluate("() => true")
            return {"status": "ok"}
        except Exception:
            return {"status": "dead"}

    def instances(self) -> list[dict[str, Any]]:
        """List all managed instances."""
        result = []
        for inst in self._instances.values():
            result.append({
                "id": inst.instance_id,
                "instanceId": inst.instance_id,
                "mode": inst.mode.value,
            })
        return result

    # ------------------------------------------------------------------
    # Page interaction (matches PinchTabClient interface)
    # ------------------------------------------------------------------

    def navigate(self, instance_id: str, url: str) -> dict[str, Any]:
        """Navigate the instance's page to *url*."""
        inst = self._get(instance_id)
        response = inst.page.goto(url, wait_until="domcontentloaded", timeout=self.settings.playwright_navigation_timeout_ms)
        status = response.status if response else None
        return {"status": status, "url": url}

    def evaluate(self, instance_id: str, expression: str) -> Any:
        """Execute JavaScript in the instance's page and return the result."""
        inst = self._get(instance_id)
        return inst.page.evaluate(expression)

    def screenshot(self, instance_id: str) -> bytes:
        """Capture a full-page screenshot and return PNG bytes."""
        inst = self._get(instance_id)
        return inst.page.screenshot(full_page=True)

    def action(self, instance_id: str, action: str, **kwargs: Any) -> dict[str, Any]:
        """Generic action dispatch — minimal shim for compatibility."""
        if action == "click":
            selector = kwargs.get("selector", "")
            inst = self._get(instance_id)
            inst.page.click(selector)
            return {"action": action, "clicked": True}
        return {"action": action, "status": "unsupported"}

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close_instance(self, instance_id: str) -> None:
        """Close and remove a single instance."""
        inst = self._instances.pop(instance_id, None)
        if inst:
            try:
                inst.context.close()
            except Exception:
                pass
            try:
                inst.browser.close()
            except Exception:
                pass

    def close_all(self) -> None:
        """Close every instance and shut down Playwright."""
        for instance_id in list(self._instances):
            self.close_instance(instance_id)
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def __del__(self) -> None:
        try:
            self.close_all()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, instance_id: str) -> _BrowserInstance:
        inst = self._instances.get(instance_id)
        if not inst:
            raise RuntimeError(f"Unknown Playwright instance: {instance_id}")
        return inst

    def _get_playwright(self) -> Any:
        if self._pw is not None:
            return self._pw
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "playwright is not installed. Run: pip install playwright && playwright install chromium"
            ) from exc
        self._pw = sync_playwright().start()
        return self._pw
