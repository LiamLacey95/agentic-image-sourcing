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
        self.started_modes: list[object] = []
        self.instance_counter = 0

    def instance_start(self, mode=None, profile_id=None):  # noqa: ANN001, ANN201
        self.instance_counter += 1
        self.started_modes.append(mode)
        return {"id": f"inst-live-{self.instance_counter}", "url": f"http://127.0.0.1:{9987 + self.instance_counter}"}

    def instance_health(self, instance_id: str) -> dict:
        return {"ok": True, "instance_id": instance_id}

    def wait_for_instance_ready(self, instance_id: str, timeout_seconds: float | None = None) -> None:
        return None

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
        if "unusual traffic" in expression:
            return {"blocked": False, "currentUrl": "https://www.google.com/search?q=robot&udm=2", "pageTitle": "Google Images"}
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
                    "imageUrl": "data:image/jpeg;base64,AAA",
                    "thumbnailUrl": "data:image/jpeg;base64,AAA",
                    "sourcePageUrl": None,
                    "googleResultUrl": None,
                    "altText": "Logo",
                    "nearbyText": "Tiny",
                    "rect": {"left": 0, "top": 0, "width": 40, "height": 40},
                },
                {
                    "domIndex": 1,
                    "imageUrl": "https://images.example.com/1.jpg",
                    "thumbnailUrl": "https://thumbs.example.com/1.jpg",
                    "sourcePageUrl": "https://source.example.com/1",
                    "googleResultUrl": None,
                    "altText": "One wildlife reference image with context",
                    "nearbyText": "Result One in the wild",
                    "rect": {"left": 100, "top": 0, "width": 140, "height": 140},
                },
                {
                    "domIndex": 2,
                    "imageUrl": "https://images.example.com/2.jpg",
                    "thumbnailUrl": "https://thumbs.example.com/2.jpg",
                    "sourcePageUrl": "https://source.example.com/2",
                    "googleResultUrl": None,
                    "altText": "Two wildlife reference image with strong context",
                    "nearbyText": "Result Two with more details",
                    "rect": {"left": 200, "top": 0, "width": 150, "height": 150},
                },
                {
                    "domIndex": 3,
                    "imageUrl": "https://images.example.com/3.jpg",
                    "thumbnailUrl": "https://thumbs.example.com/3.jpg",
                    "sourcePageUrl": "https://source.example.com/3",
                    "googleResultUrl": None,
                    "altText": "Three",
                    "nearbyText": "Result Three",
                    "rect": {"left": 300, "top": 0, "width": 120, "height": 120},
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
    assert instance_id == "inst-live-1"
    assert candidates[1].provenance.steps[0].details["dom_index"] == 1
    assert candidates[0].quality_score is not None
    assert candidates[0].quality_score >= candidates[1].quality_score

    batch_two_gallery_id, _, batch_two_candidates, _ = adapter.build_gallery(
        GoogleGalleryRequest(query="robot", batch_size=2, batch_number=2)
    )
    assert batch_two_gallery_id
    assert batch_two_candidates[0].provenance.steps[0].details["batch_number"] == 2

    inspected = adapter.inspect_candidate(candidates[1])
    assert inspected.image_url == "https://cdn.example.com/preview.jpg"
    assert inspected.source_page_url == "https://source.example.com/story"


class BlockingFakePinchTab(FakePinchTab):
    def evaluate(self, instance_id: str, expression: str):  # noqa: ANN201
        self.evaluate_calls.append((instance_id, expression))
        if "accept all" in expression:
            return False
        if "unusual traffic" in expression:
            blocked = instance_id == "inst-live-1"
            return {
                "blocked": blocked,
                "currentUrl": "https://www.google.com/sorry/index" if blocked else "https://www.google.com/search?q=robot&udm=2",
                "pageTitle": "Sorry" if blocked else "Google Images",
            }
        return super().evaluate(instance_id, expression)


def test_build_gallery_falls_back_to_headed_when_google_blocks_headless(tmp_path: Path) -> None:
    settings = Settings(
        local_cache_dir=tmp_path / "cache",
        local_object_store_dir=tmp_path / "objects",
        pinchtab_default_browser_mode="headless",
    )
    pinchtab = BlockingFakePinchTab()
    adapter = GoogleImagesBrowserAdapter(
        settings=settings,
        pinchtab=pinchtab,
        sheet_builder=ContactSheetBuilder(settings=settings),
    )

    _, _, candidates, instance_id = adapter.build_gallery(GoogleGalleryRequest(query="robot", batch_size=2))

    assert [mode.value for mode in pinchtab.started_modes[:2]] == ["headless", "headed"]
    assert instance_id == "inst-live-2"
    assert candidates[0].provenance.steps[0].details["browser_mode"] == "headed"
