from __future__ import annotations

from pathlib import Path

from PIL import Image

from agentic_image_sourcing.adapters.google_browser import GoogleImagesBrowserAdapter
from agentic_image_sourcing.config import Settings
from agentic_image_sourcing.google_gallery import ContactSheetBuilder
from agentic_image_sourcing.models import CandidateRecord, GoogleGalleryRequest, Provenance, ProvenanceStep


class FakePinchTab:
    def __init__(self) -> None:
        self.navigations: list[tuple[str, str]] = []
        self.screenshots: list[str] = []
        self.evaluate_calls: list[tuple[str, str]] = []

    def instance_start(self, mode=None, profile_id=None):  # noqa: ANN001, ANN201
        return {"id": "inst-live", "url": "http://127.0.0.1:9988"}

    def instance_health(self, instance_id: str) -> dict:
        return {"ok": True, "instance_id": instance_id}

    def navigate(self, instance_id: str, url: str) -> dict:
        self.navigations.append((instance_id, url))
        return {"ok": True}

    def screenshot(self, instance_id: str) -> bytes:
        self.screenshots.append(instance_id)
        image = Image.new("RGB", (400, 400), "purple")
        path = Path.cwd() / "_tmp_google_browser_test.png"
        image.save(path, format="PNG")
        data = path.read_bytes()
        path.unlink(missing_ok=True)
        return data

    def evaluate(self, instance_id: str, expression: str):  # noqa: ANN201
        self.evaluate_calls.append((instance_id, expression))
        if "accept all" in expression:
            return False
        if "window.scrollBy" in expression:
            return True
        if "targetIndex" in expression:
            return {"clicked": True, "count": 3}
        if "previewImageUrl" in expression:
            return {
                "previewImageUrl": "https://cdn.example.com/preview.jpg",
                "previewWidth": 800,
                "previewHeight": 600,
                "sourcePageUrl": "https://source.example.com/story",
                "pageTitle": "Preview",
            }
        return {
            "items": [
                {
                    "domIndex": 0,
                    "imageUrl": "https://images.example.com/0.jpg",
                    "thumbnailUrl": "https://thumbs.example.com/0.jpg",
                    "sourcePageUrl": "https://source.example.com/0",
                    "googleResultUrl": None,
                    "altText": "Zero",
                    "nearbyText": "Result Zero",
                    "rect": {"left": 0, "top": 0, "width": 100, "height": 100},
                },
                {
                    "domIndex": 1,
                    "imageUrl": "https://images.example.com/1.jpg",
                    "thumbnailUrl": "https://thumbs.example.com/1.jpg",
                    "sourcePageUrl": "https://source.example.com/1",
                    "googleResultUrl": None,
                    "altText": "One",
                    "nearbyText": "Result One",
                    "rect": {"left": 100, "top": 0, "width": 100, "height": 100},
                },
            ],
            "viewport": {"width": 1200, "height": 800, "dpr": 1},
            "pageTitle": "Google Images",
            "currentUrl": "https://www.google.com/search?q=robot&udm=2",
        }


def test_build_gallery_tracks_dom_index_and_inspect_reopens_tile(tmp_path: Path) -> None:
    settings = Settings(local_cache_dir=tmp_path / "cache", local_object_store_dir=tmp_path / "objects")
    adapter = GoogleImagesBrowserAdapter(
        settings=settings,
        pinchtab=FakePinchTab(),
        sheet_builder=ContactSheetBuilder(settings=settings),
    )

    gallery_id, gallery_path, candidates, instance_id = adapter.build_gallery(
        GoogleGalleryRequest(query="robot", batch_size=2)
    )

    assert gallery_id
    assert Path(gallery_path).exists()
    assert instance_id == "inst-live"
    assert candidates[1].provenance.steps[0].details["dom_index"] == 1

    inspected = adapter.inspect_candidate(candidates[1])
    assert inspected.image_url == "https://cdn.example.com/preview.jpg"
    assert inspected.source_page_url == "https://source.example.com/story"
