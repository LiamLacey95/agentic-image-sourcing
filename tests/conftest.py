from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys

import pytest
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agentic_image_sourcing.config import Settings
from agentic_image_sourcing.models import CandidateRecord, FetchMode, Provenance, ProvenanceStep, utc_now
from agentic_image_sourcing.repository import build_repository
from agentic_image_sourcing.service import RetrievalService
from agentic_image_sourcing.storage import FileCache, build_object_store


def make_candidate(image_url: str, source: str = "test", thumbnail_url: str | None = None) -> CandidateRecord:
    return CandidateRecord(
        image_url=image_url,
        thumbnail_url=thumbnail_url,
        source_page_url="https://example.com/page",
        source_domain="example.com",
        page_title="Example",
        provenance=Provenance(
            discovery_method=source,
            steps=[ProvenanceStep(stage="discover", source=source)],
        ),
    )


class FakeGoogleAdapter:
    def __init__(self, candidates: list[CandidateRecord]) -> None:
        self._candidates = candidates

    def discover(self, query: str, limit: int, preferred_domains: list[str] | None = None) -> list[CandidateRecord]:
        return self._candidates[:limit]


class FakeExtractor:
    def __init__(self, candidates: list[CandidateRecord]) -> None:
        self._candidates = candidates

    def extract(self, url: str, limit: int, capture_page_screenshot: bool = False) -> list[CandidateRecord]:
        return self._candidates[:limit]


class FakeCrawler:
    def __init__(self, candidates: list[CandidateRecord]) -> None:
        self._candidates = candidates

    def crawl(self, start_url: str, limit: int, max_pages: int) -> list[CandidateRecord]:
        return self._candidates[:limit]


class FakeFetchPayload:
    def __init__(self, url: str, mode: FetchMode, data: bytes) -> None:
        self.source_url = url
        self.mode = mode
        self.data = data
        self.mime_type = "image/png"
        self.width = 4
        self.height = 4
        self.byte_size = len(data)
        self.content_hash = "content-hash"
        self.perceptual_hash = "perceptual-hash"
        self.provenance = Provenance(
            discovery_method="image_fetch",
            steps=[ProvenanceStep(stage="fetch_image", source="fake_fetcher")],
        )
        self.fetch_status = "fetched"


class FakeFetcher:
    def __init__(self) -> None:
        image = Image.new("RGB", (4, 4), "red")
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        self._data = buffer.getvalue()

    def fetch_image(self, url: str, mode: FetchMode) -> FakeFetchPayload:
        return FakeFetchPayload(url, mode, self._data)


class FakeGoogleBrowserAdapter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build_gallery(self, request) -> tuple[str, str, list[CandidateRecord], str]:
        gallery_id = "gallery-123"
        gallery_path = self.settings.local_cache_dir / "google-galleries" / f"{gallery_id}.png"
        gallery_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (512, 768), "blue").save(gallery_path, format="PNG")

        candidates = []
        now = utc_now()
        for index in range(1, request.batch_size + 1):
            candidates.append(
                CandidateRecord(
                    query_text=request.query,
                    image_url=f"https://images.example.com/google-{index}.jpg",
                    thumbnail_url=f"https://thumbs.example.com/google-{index}.jpg",
                    source_page_url=f"https://source.example.com/page-{index}",
                    source_domain="source.example.com",
                    page_title="Google Images",
                    alt_text=f"Result {index}",
                    nearby_text=f"Snippet {index}",
                    crawl_timestamp=now,
                    gallery_id=gallery_id,
                    tile_index=index,
                    google_result_url=f"https://www.google.com/imgres?result={index}",
                    pinchtab_instance_id="inst-managed",
                    provenance=Provenance(
                        discovery_method="google_images_browser",
                        steps=[ProvenanceStep(stage="google_gallery", source="fake_google_browser")],
                    ),
                )
            )
        return gallery_id, str(gallery_path.resolve()), candidates, "inst-managed"

    def inspect_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        return candidate.model_copy(
            update={
                "image_url": "https://cdn.example.com/full-resolution.jpg",
                "source_page_url": "https://source.example.com/inspected",
                "source_domain": "source.example.com",
                "width": 1024,
                "height": 768,
                "provenance": candidate.provenance.model_copy(
                    update={
                        "steps": [
                            *candidate.provenance.steps,
                            ProvenanceStep(stage="google_inspect", source="fake_google_browser"),
                        ]
                    }
                ),
            }
        )


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url=f"sqlite:///{(tmp_path / 'test.db').as_posix()}",
        local_cache_dir=tmp_path / "cache",
        local_object_store_dir=tmp_path / "object-store",
        crawl_respect_robots=False,
        rate_limit_per_domain_seconds=0.0,
    )


@pytest.fixture()
def service(settings: Settings) -> RetrievalService:
    google_candidates = [
        make_candidate("https://images.example.com/one.png", source="google", thumbnail_url="https://thumbs.example.com/one.png"),
        make_candidate("https://images.example.com/two.png", source="google"),
    ]
    extractor_candidates = [
        make_candidate("https://images.example.com/one.png", source="page_extract"),
        make_candidate("https://images.example.com/three.png", source="page_extract"),
    ]
    repository = build_repository(settings.database_url)
    object_store = build_object_store(settings)
    cache = FileCache(settings.local_cache_dir)
    return RetrievalService(
        settings=settings,
        repository=repository,
        object_store=object_store,
        cache=cache,
        google_adapter=FakeGoogleAdapter(google_candidates),
        extractor=FakeExtractor(extractor_candidates),
        crawler=FakeCrawler(extractor_candidates),
        fetcher=FakeFetcher(),
        google_browser_adapter=FakeGoogleBrowserAdapter(settings),
    )
