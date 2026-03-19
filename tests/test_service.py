from __future__ import annotations

from pathlib import Path

from agentic_image_sourcing.models import (
    FetchRequest,
    GoogleDownloadRequest,
    GoogleGalleryRequest,
    GoogleInspectRequest,
    SaveAssetRequest,
    SearchRequest,
)


def test_search_deduplicates_candidates_from_multiple_sources(service) -> None:
    response = service.image_search(
        SearchRequest(query="warehouse reference", limit=10, seed_urls=["https://example.com/page"])
    )

    urls = [candidate.image_url for candidate in response.candidates]
    assert urls == [
        "https://images.example.com/one.png",
        "https://images.example.com/two.png",
        "https://images.example.com/three.png",
    ]


def test_fetch_and_save_asset_updates_candidate_and_writes_files(service) -> None:
    search = service.image_search(SearchRequest(query="warehouse reference", limit=10))
    candidate = search.candidates[0]

    fetched = service.image_fetch(FetchRequest(candidate_id=candidate.candidate_id))
    assert fetched.local_cache_path is not None
    assert Path(fetched.local_cache_path).exists()
    assert fetched.width == 4
    assert fetched.height == 4

    saved = service.asset_save(SaveAssetRequest(candidate_id=candidate.candidate_id, collection="references", tags=["warehouse"]))
    assert saved.candidate.storage_key is not None
    assert saved.asset.object_key.startswith("references/")


def test_google_gallery_inspect_and_download_flow(service) -> None:
    gallery = service.google_image_gallery(GoogleGalleryRequest(query="robot concept art", batch_size=12, batch_number=2))
    assert Path(gallery.gallery_image_path).exists()
    assert len(gallery.candidates) == 12
    assert gallery.candidates[0].tile_index == 1
    assert gallery.batch_number == 2
    assert gallery.next_batch_number == 3
    assert gallery.has_more is True

    inspected = service.google_image_inspect(GoogleInspectRequest(candidate_id=gallery.candidates[0].candidate_id))
    assert inspected.candidate.image_url == "https://cdn.example.com/full-resolution.jpg"
    assert inspected.candidate.pinchtab_instance_id == "inst-managed"
    persisted = service.candidate_inspect(inspected.candidate.candidate_id)
    assert persisted.image_url == "https://cdn.example.com/full-resolution.jpg"

    saved = service.google_image_download(
        GoogleDownloadRequest(candidate_id=gallery.candidates[0].candidate_id, collection="google-picks")
    )
    assert saved.asset.object_key.startswith("google-picks/")
