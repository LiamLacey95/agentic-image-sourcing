from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FetchMode(str, Enum):
    thumbnail = "thumbnail"
    full = "full"


class BrowserMode(str, Enum):
    headed = "headed"
    headless = "headless"


class JobKind(str, Enum):
    search = "search"
    extract = "extract"
    google_gallery = "google_gallery"
    google_inspect = "google_inspect"
    google_download = "google_download"


class FetchStatus(str, Enum):
    pending = "pending"
    fetched = "fetched"
    cached = "cached"
    persisted = "persisted"
    failed = "failed"


class ProvenanceStep(BaseModel):
    stage: str
    source: str
    timestamp: datetime = Field(default_factory=utc_now)
    details: dict[str, Any] = Field(default_factory=dict)


class Provenance(BaseModel):
    discovery_method: str
    discovered_at: datetime = Field(default_factory=utc_now)
    crawl_timestamp: datetime = Field(default_factory=utc_now)
    http_status: int | None = None
    redirect_chain: list[str] = Field(default_factory=list)
    steps: list[ProvenanceStep] = Field(default_factory=list)


class CandidateRecord(BaseModel):
    candidate_id: str = Field(default_factory=lambda: str(uuid4()))
    job_id: str | None = None
    query_text: str | None = None
    image_url: str
    thumbnail_url: str | None = None
    source_page_url: str | None = None
    source_domain: str | None = None
    mime_type: str | None = None
    width: int | None = None
    height: int | None = None
    byte_size: int | None = None
    page_title: str | None = None
    alt_text: str | None = None
    nearby_text: str | None = None
    crawl_timestamp: datetime = Field(default_factory=utc_now)
    fetch_status: FetchStatus = FetchStatus.pending
    storage_key: str | None = None
    local_cache_path: str | None = None
    page_screenshot_path: str | None = None
    content_hash: str | None = None
    perceptual_hash: str | None = None
    last_error: str | None = None
    quality_score: float | None = None
    gallery_id: str | None = None
    tile_index: int | None = None
    google_result_url: str | None = None
    pinchtab_instance_id: str | None = None
    provenance: Provenance


class JobRecord(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    kind: JobKind
    query: str | None = None
    source_url: str | None = None
    status: str = "completed"
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    request_payload: dict[str, Any] = Field(default_factory=dict)


class AssetRecord(BaseModel):
    asset_id: str = Field(default_factory=lambda: str(uuid4()))
    candidate_id: str
    collection: str
    tags: list[str] = Field(default_factory=list)
    object_key: str
    object_uri: str
    created_at: datetime = Field(default_factory=utc_now)


class SearchRequest(BaseModel):
    query: str
    limit: int = 10
    preferred_domains: list[str] = Field(default_factory=list)
    seed_urls: list[str] = Field(default_factory=list)


class ExtractRequest(BaseModel):
    url: str
    limit: int = 20
    crawl: bool = False
    max_pages: int = 3
    capture_page_screenshot: bool = False


class FetchRequest(BaseModel):
    candidate_id: str | None = None
    url: str | None = None
    mode: FetchMode = FetchMode.full


class SaveAssetRequest(BaseModel):
    candidate_id: str
    collection: str
    tags: list[str] = Field(default_factory=list)


class GoogleGalleryRequest(BaseModel):
    query: str
    batch_size: int = 12
    batch_number: int = 1
    offset: int = 0
    browser_mode: BrowserMode | None = None
    profile_id: str | None = None
    instance_id: str | None = None


class GoogleInspectRequest(BaseModel):
    candidate_id: str
    open_source_page: bool = False


class GoogleDownloadRequest(BaseModel):
    candidate_id: str
    collection: str
    tags: list[str] = Field(default_factory=list)


class FetchResult(BaseModel):
    candidate_id: str | None = None
    source_url: str
    mode: FetchMode
    fetch_status: FetchStatus
    mime_type: str | None = None
    width: int | None = None
    height: int | None = None
    byte_size: int | None = None
    local_cache_path: str | None = None
    storage_key: str | None = None
    content_hash: str | None = None
    perceptual_hash: str | None = None
    provenance: Provenance


class CandidateListResponse(BaseModel):
    job: JobRecord
    candidates: list[CandidateRecord]


class AssetSaveResponse(BaseModel):
    candidate: CandidateRecord
    asset: AssetRecord


class GoogleGalleryResponse(BaseModel):
    job: JobRecord
    gallery_id: str
    gallery_image_path: str
    pinchtab_instance_id: str
    batch_number: int
    next_batch_number: int | None = None
    has_more: bool = False
    requested_offset: int = 0
    effective_offset: int = 0
    candidate_pool_size: int = 0
    candidates: list[CandidateRecord]


class GoogleInspectResponse(BaseModel):
    job: JobRecord
    candidate: CandidateRecord
    source_page_candidates: list[CandidateRecord] = Field(default_factory=list)
