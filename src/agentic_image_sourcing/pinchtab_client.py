from __future__ import annotations

import base64
import time
from typing import Any

import requests

from .config import Settings
from .models import BrowserMode


class PinchTabClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.base_url = settings.pinchtab_base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        if settings.pinchtab_token:
            self.session.headers["Authorization"] = f"Bearer {settings.pinchtab_token}"
        self._instance_urls: dict[str, str] = {}

    def instance_start(self, mode: BrowserMode | str | None = None, profile_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"mode": (mode or self.settings.pinchtab_default_browser_mode)}
        if profile_id:
            payload["profileId"] = profile_id
        if self.settings.pinchtab_start_port:
            payload["port"] = self.settings.pinchtab_start_port
        response = self._request("POST", "/instances/launch", json=payload)
        self._remember_instance(response)
        return response

    def wait_for_instance_ready(self, instance_id: str, timeout_seconds: float | None = None) -> None:
        deadline = time.monotonic() + float(timeout_seconds or self.settings.pinchtab_instance_ready_wait_seconds)
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                health = self.instance_health(instance_id)
                if isinstance(health, dict) and health.get("status") in (None, "ok", "healthy", True):
                    return
            except Exception as exc:  # pragma: no cover - depends on live runtime
                last_error = exc
            time.sleep(1.0)
        if last_error:
            raise RuntimeError(f"PinchTab instance {instance_id} did not become ready") from last_error
        raise RuntimeError(f"PinchTab instance {instance_id} did not become ready")

    def instance_health(self, instance_id: str) -> dict[str, Any]:
        return self._request("GET", "/health", base_url=self.instance_base_url(instance_id))

    def instances(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/instances")
        if isinstance(payload, list):
            for item in payload:
                self._remember_instance(item)
            return payload
        data = self._unwrap(payload)
        if isinstance(data, list):
            for item in data:
                self._remember_instance(item)
            return data
        return []

    def navigate(self, instance_id: str, url: str) -> dict[str, Any]:
        return self._request("POST", "/navigate", json={"url": url}, base_url=self.instance_base_url(instance_id))

    def evaluate(self, instance_id: str, expression: str) -> Any:
        response = self._request(
            "POST",
            "/evaluate",
            json={"expression": expression},
            base_url=self.instance_base_url(instance_id),
        )
        return self._unwrap(response)

    def screenshot(self, instance_id: str) -> bytes:
        response = self._request("GET", "/screenshot", base_url=self.instance_base_url(instance_id), expect_json=False)
        payload = self._unwrap(response)
        if isinstance(payload, dict):
            for key in ("image", "imageBase64", "pngBase64", "base64"):
                if key in payload:
                    payload = payload[key]
                    break
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str) and payload.startswith("\x89PNG"):
            return payload.encode("latin1")
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            try:
                return base64.b64decode(payload)
            except Exception as exc:  # pragma: no cover - depends on live runtime
                raise RuntimeError("PinchTab screenshot payload was not valid base64") from exc
        raise RuntimeError("Unsupported PinchTab screenshot payload")

    def action(self, instance_id: str, action: str, **kwargs: Any) -> dict[str, Any]:
        payload = {"action": action, **kwargs}
        return self._request("POST", "/action", json=payload, base_url=self.instance_base_url(instance_id))

    def instance_base_url(self, instance_id: str) -> str:
        if instance_id in self._instance_urls:
            return self._instance_urls[instance_id]
        for item in self.instances():
            if str(item.get("id")) == instance_id:
                url = self._extract_instance_url(item)
                if url:
                    return url
        raise RuntimeError(f"Unknown PinchTab instance: {instance_id}")

    def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        base_url: str | None = None,
        expect_json: bool = True,
    ) -> Any:
        response = self.session.request(
            method=method,
            url=f"{(base_url or self.base_url).rstrip('/')}{path}",
            json=json,
            timeout=self.settings.request_timeout_seconds,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        if not expect_json:
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                return response.content
        return response.json()

    def _remember_instance(self, payload: dict[str, Any]) -> None:
        unwrapped = self._unwrap(payload)
        instance_id = payload.get("instanceId") or payload.get("instance_id") or payload.get("id")
        if not instance_id and isinstance(unwrapped, dict):
            instance_id = unwrapped.get("instanceId") or unwrapped.get("instance_id") or unwrapped.get("id")
        url = self._extract_instance_url(payload)
        if instance_id and url:
            self._instance_urls[str(instance_id)] = url

    @staticmethod
    def _extract_instance_url(payload: dict[str, Any]) -> str | None:
        for container in (payload, payload.get("data") if isinstance(payload.get("data"), dict) else None):
            if not isinstance(container, dict):
                continue
            if container.get("url"):
                return str(container["url"]).rstrip("/")
            if container.get("port"):
                return f"http://127.0.0.1:{container['port']}"
        return None

    @staticmethod
    def _unwrap(payload: Any) -> Any:
        current = payload
        for key in ("data", "result", "value"):
            if isinstance(current, dict) and key in current:
                current = current[key]
        return current
