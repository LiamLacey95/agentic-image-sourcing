from __future__ import annotations

from fastapi.testclient import TestClient

from agentic_image_sourcing.api import create_app
from agentic_image_sourcing.models import GoogleGalleryRequest, GoogleInspectRequest, SearchRequest


def test_search_and_candidate_inspect_routes(service) -> None:
    app = create_app(service=service, settings=service.settings)
    client = TestClient(app)

    response = client.post("/search", json=SearchRequest(query="factory reference", limit=5).model_dump())
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["candidates"]) == 2

    candidate_id = payload["candidates"][0]["candidate_id"]
    candidate_response = client.get(f"/candidates/{candidate_id}")
    assert candidate_response.status_code == 200
    assert candidate_response.json()["image_url"] == "https://images.example.com/one.png"


def test_google_gallery_and_inspect_routes(service) -> None:
    app = create_app(service=service, settings=service.settings)
    client = TestClient(app)

    gallery = client.post("/google/gallery", json=GoogleGalleryRequest(query="robot art").model_dump())
    assert gallery.status_code == 200
    payload = gallery.json()
    assert payload["gallery_id"] == "gallery-123"
    assert len(payload["candidates"]) == 12

    candidate_id = payload["candidates"][0]["candidate_id"]
    inspect = client.post("/google/inspect", json=GoogleInspectRequest(candidate_id=candidate_id).model_dump())
    assert inspect.status_code == 200
    assert inspect.json()["candidate"]["image_url"] == "https://cdn.example.com/full-resolution.jpg"
