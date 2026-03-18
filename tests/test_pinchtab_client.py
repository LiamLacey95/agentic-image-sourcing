from __future__ import annotations

import json

from agentic_image_sourcing.config import Settings
from agentic_image_sourcing.models import BrowserMode
from agentic_image_sourcing.pinchtab_client import PinchTabClient


class DummyResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.content = json.dumps(payload).encode("utf-8")

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class DummySession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, str, dict | None]] = []

    def request(self, method: str, url: str, json: dict | None = None, timeout: float | None = None):
        self.calls.append((method, url, json))
        if url.endswith("/instances/launch"):
            return DummyResponse({"id": "inst_123", "url": "http://127.0.0.1:9988"})
        if url.endswith("/navigate"):
            return DummyResponse({"ok": True})
        if url.endswith("/evaluate"):
            return DummyResponse({"data": {"result": {"items": []}}})
        return DummyResponse({"ok": True})


def test_pinchtab_client_maps_requests_to_expected_endpoints() -> None:
    session = DummySession()
    client = PinchTabClient(Settings(pinchtab_base_url="http://localhost:9867"), session=session)

    start = client.instance_start(mode=BrowserMode.headed, profile_id="profile-main")
    assert start["id"] == "inst_123"
    client.navigate("inst_123", "https://www.google.com/search?tbm=isch&q=robot")
    client.evaluate("inst_123", "(() => true)()")

    assert session.calls[0] == (
        "POST",
        "http://localhost:9867/instances/launch",
        {"mode": "headed", "profileId": "profile-main"},
    )
    assert session.calls[1][1] == "http://127.0.0.1:9988/navigate"
    assert session.calls[2][1] == "http://127.0.0.1:9988/evaluate"
