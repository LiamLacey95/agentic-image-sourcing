from __future__ import annotations

from pathlib import Path

from .adapters.crawl import SiteCrawlerAdapter
from .adapters.extract import DirectPageImageExtractor
from .adapters.google import GoogleImageDiscoveryAdapter
from .adapters.google_browser import GoogleImagesBrowserAdapter
from .browser_capture import NoopBrowserCapture, PlaywrightBrowserCapture
from .config import Settings, get_settings
from .fetching import ImageFetcher
from .google_gallery import ContactSheetBuilder
from .models import (
    AssetRecord,
    AssetSaveResponse,
    CandidateListResponse,
    CandidateRecord,
    ExtractRequest,
    FetchMode,
    FetchRequest,
    FetchResult,
    FetchStatus,
    GoogleDownloadRequest,
    GoogleGalleryRequest,
    GoogleGalleryResponse,
    GoogleInspectRequest,
    GoogleInspectResponse,
    JobKind,
    JobRecord,
    Provenance,
    ProvenanceStep,
    SaveAssetRequest,
    SearchRequest,
)
from .pinchtab_client import PinchTabClient
from .policies import DomainRateLimiter, RobotsPolicy
from .repository import Repository, build_repository
from .storage import FileCache, ObjectStore, build_object_store
from .utils import sha256_text


class RetrievalService:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        object_store: ObjectStore,
        cache: FileCache,
        google_adapter: GoogleImageDiscoveryAdapter,
        extractor: DirectPageImageExtractor,
        crawler: SiteCrawlerAdapter,
        fetcher: ImageFetcher,
        google_browser_adapter: GoogleImagesBrowserAdapter | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.object_store = object_store
        self.cache = cache
        self.google_adapter = google_adapter
        self.extractor = extractor
        self.crawler = crawler
        self.fetcher = fetcher
        self.google_browser_adapter = google_browser_adapter

    def image_search(self, request: SearchRequest) -> CandidateListResponse:
        job = self.repository.create_job(
            JobRecord(kind=JobKind.search, query=request.query, request_payload=request.model_dump())
        )

        candidates = self.google_adapter.discover(
            query=request.query,
            limit=request.limit,
            preferred_domains=request.preferred_domains,
        )
        for url in request.seed_urls:
            try:
                candidates.extend(self.extractor.extract(url, limit=request.limit))
            except Exception:
                continue

        stored = self._store_candidates(job, candidates, request.limit)
        return CandidateListResponse(job=job, candidates=stored)

    def page_extract_images(self, request: ExtractRequest) -> CandidateListResponse:
        source_url = str(request.url)
        job = self.repository.create_job(
            JobRecord(kind=JobKind.extract, source_url=source_url, request_payload=request.model_dump())
        )
        if request.crawl:
            candidates = self.crawler.crawl(source_url, limit=request.limit, max_pages=request.max_pages)
        else:
            candidates = self.extractor.extract(
                source_url,
                limit=request.limit,
                capture_page_screenshot=request.capture_page_screenshot,
            )
        stored = self._store_candidates(job, candidates, request.limit)
        return CandidateListResponse(job=job, candidates=stored)

    def google_image_gallery(self, request: GoogleGalleryRequest) -> GoogleGalleryResponse:
        adapter = self._require_google_browser_adapter()
        job = self.repository.create_job(
            JobRecord(kind=JobKind.google_gallery, query=request.query, request_payload=request.model_dump())
        )
        gallery_id, gallery_path, candidates, instance_id = adapter.build_gallery(request)
        stored = self._store_candidates(job, candidates, request.batch_size)
        effective_offset = self._google_effective_offset(request)
        has_more = len(stored) == request.batch_size
        return GoogleGalleryResponse(
            job=job,
            gallery_id=gallery_id,
            gallery_image_path=gallery_path,
            pinchtab_instance_id=instance_id,
            batch_number=max(1, request.batch_number),
            next_batch_number=(max(1, request.batch_number) + 1) if has_more else None,
            has_more=has_more,
            requested_offset=request.offset,
            effective_offset=effective_offset,
            candidate_pool_size=effective_offset + len(stored),
            candidates=stored,
        )

    def google_image_inspect(self, request: GoogleInspectRequest) -> GoogleInspectResponse:
        adapter = self._require_google_browser_adapter()
        job = self.repository.create_job(
            JobRecord(kind=JobKind.google_inspect, request_payload=request.model_dump())
        )
        candidate = self.candidate_inspect(request.candidate_id)
        inspected = adapter.inspect_candidate(candidate)

        source_page_candidates: list[CandidateRecord] = []
        inspected, source_page_candidates = self._resolve_source_page_image(inspected)
        inspected = self.repository.upsert_candidate(inspected)

        return GoogleInspectResponse(job=job, candidate=inspected, source_page_candidates=source_page_candidates)

    def google_image_download(self, request: GoogleDownloadRequest) -> AssetSaveResponse:
        inspected = self.google_image_inspect(GoogleInspectRequest(candidate_id=request.candidate_id))
        return self.asset_save(
            SaveAssetRequest(candidate_id=inspected.candidate.candidate_id, collection=request.collection, tags=request.tags)
        )

    def image_fetch(self, request: FetchRequest) -> FetchResult:
        candidate = self._candidate_for_fetch(request)
        target_url = candidate.thumbnail_url if request.mode.value == "thumbnail" and candidate.thumbnail_url else candidate.image_url
        payload = self.fetcher.fetch_image(target_url, request.mode)
        cache_path = self.cache.write(payload.data, key_hint=target_url, mime_type=payload.mime_type)

        if self.repository.get_candidate(candidate.candidate_id):
            candidate = candidate.model_copy(
                update={
                    "mime_type": payload.mime_type,
                    "width": payload.width,
                    "height": payload.height,
                    "byte_size": payload.byte_size,
                    "local_cache_path": cache_path,
                    "content_hash": payload.content_hash,
                    "perceptual_hash": payload.perceptual_hash,
                    "fetch_status": FetchStatus.cached,
                    "provenance": candidate.provenance.model_copy(
                        update={
                            "http_status": payload.provenance.http_status,
                            "redirect_chain": payload.provenance.redirect_chain,
                            "steps": [*candidate.provenance.steps, *payload.provenance.steps],
                        }
                    ),
                }
            )
            self.repository.update_candidate(candidate)

        return FetchResult(
            candidate_id=candidate.candidate_id,
            source_url=payload.source_url,
            mode=request.mode,
            fetch_status=FetchStatus.cached,
            mime_type=payload.mime_type,
            width=payload.width,
            height=payload.height,
            byte_size=payload.byte_size,
            local_cache_path=cache_path,
            content_hash=payload.content_hash,
            perceptual_hash=payload.perceptual_hash,
            provenance=payload.provenance,
        )

    def candidate_inspect(self, candidate_id: str) -> CandidateRecord:
        candidate = self.repository.get_candidate(candidate_id)
        if not candidate:
            raise KeyError(f"Unknown candidate: {candidate_id}")
        return candidate

    def asset_save(self, request: SaveAssetRequest) -> AssetSaveResponse:
        candidate = self.candidate_inspect(request.candidate_id)
        if not candidate.local_cache_path or not Path(candidate.local_cache_path).exists():
            self.image_fetch(FetchRequest(candidate_id=candidate.candidate_id))
            candidate = self.candidate_inspect(request.candidate_id)

        if not candidate.local_cache_path:
            raise RuntimeError("Candidate has no cached file after fetch")

        data = Path(candidate.local_cache_path).read_bytes()
        object_key_hint = f"{request.collection}/{sha256_text(candidate.image_url)}"
        stored = self.object_store.put_bytes(data, key_hint=object_key_hint, mime_type=candidate.mime_type)
        asset = self.repository.save_asset(
            AssetRecord(
                candidate_id=candidate.candidate_id,
                collection=request.collection,
                tags=request.tags,
                object_key=stored.key,
                object_uri=stored.uri,
            )
        )
        updated = candidate.model_copy(update={"storage_key": stored.key, "fetch_status": FetchStatus.persisted})
        self.repository.update_candidate(updated)
        return AssetSaveResponse(candidate=updated, asset=asset)

    def _store_candidates(self, job: JobRecord, candidates: list[CandidateRecord], limit: int) -> list[CandidateRecord]:
        for rank, candidate in enumerate(candidates[:limit], start=1):
            enriched = candidate.model_copy(update={"job_id": job.job_id})
            saved = self.repository.upsert_candidate(enriched)
            self.repository.link_candidate_to_job(job.job_id, saved.candidate_id, rank)
        return self.repository.list_job_candidates(job.job_id, limit)

    def _candidate_for_fetch(self, request: FetchRequest) -> CandidateRecord:
        if request.candidate_id:
            candidate = self.repository.get_candidate(request.candidate_id)
            if not candidate:
                raise KeyError(f"Unknown candidate: {request.candidate_id}")
            return candidate
        if request.url:
            existing = self.repository.get_candidate_by_url(request.url)
            if existing:
                return existing
            return CandidateRecord(
                image_url=request.url,
                provenance=Provenance(discovery_method="direct_fetch"),
            )
        raise ValueError("Either candidate_id or url must be provided")

    def _require_google_browser_adapter(self) -> GoogleImagesBrowserAdapter:
        if not self.google_browser_adapter:
            raise RuntimeError("Google browser automation is not configured")
        return self.google_browser_adapter

    @staticmethod
    def _google_effective_offset(request: GoogleGalleryRequest) -> int:
        if request.offset > 0:
            return request.offset
        return (max(1, request.batch_number) - 1) * request.batch_size

    def _resolve_source_page_image(self, candidate: CandidateRecord) -> tuple[CandidateRecord, list[CandidateRecord]]:
        if not candidate.source_page_url:
            return candidate, []
        try:
            extracted = self.extractor.extract(candidate.source_page_url, limit=8)
        except Exception:
            return candidate, []

        resolved_candidates: list[CandidateRecord] = []
        best_candidate: CandidateRecord | None = None
        best_payload = None
        best_score = float("-inf")
        current_area = (candidate.width or 0) * (candidate.height or 0)

        for extracted_candidate in extracted:
            try:
                payload = self.fetcher.fetch_image(extracted_candidate.image_url, FetchMode.full)
            except Exception:
                continue

            resolved = extracted_candidate.model_copy(
                update={
                    "source_page_url": candidate.source_page_url or extracted_candidate.source_page_url,
                    "source_domain": candidate.source_domain or extracted_candidate.source_domain,
                    "page_title": candidate.page_title or extracted_candidate.page_title,
                    "mime_type": payload.mime_type or extracted_candidate.mime_type,
                    "width": payload.width or extracted_candidate.width,
                    "height": payload.height or extracted_candidate.height,
                    "byte_size": payload.byte_size,
                    "fetch_status": FetchStatus.fetched,
                    "content_hash": payload.content_hash,
                    "perceptual_hash": payload.perceptual_hash,
                    "provenance": extracted_candidate.provenance.model_copy(
                        update={
                            "http_status": payload.provenance.http_status,
                            "redirect_chain": payload.provenance.redirect_chain,
                            "steps": [*extracted_candidate.provenance.steps, *payload.provenance.steps],
                        }
                    ),
                }
            )
            resolved_candidates.append(resolved)

            score = self._source_resolution_score(resolved, preferred_domain=candidate.source_domain)
            if score > best_score:
                best_score = score
                best_candidate = resolved
                best_payload = payload

        if not best_candidate or not best_payload:
            return candidate, resolved_candidates

        best_area = (best_candidate.width or 0) * (best_candidate.height or 0)
        if not self._should_replace_with_source_image(candidate, current_area=current_area, best_area=best_area, best_url=best_candidate.image_url):
            return candidate, resolved_candidates

        cache_path = self.cache.write(best_payload.data, key_hint=best_payload.source_url, mime_type=best_payload.mime_type)
        updated = candidate.model_copy(
            update={
                "image_url": best_payload.source_url,
                "source_page_url": candidate.source_page_url or best_candidate.source_page_url,
                "source_domain": candidate.source_domain or best_candidate.source_domain,
                "page_title": candidate.page_title or best_candidate.page_title,
                "mime_type": best_payload.mime_type or candidate.mime_type,
                "width": best_payload.width or candidate.width,
                "height": best_payload.height or candidate.height,
                "byte_size": best_payload.byte_size,
                "local_cache_path": cache_path,
                "content_hash": best_payload.content_hash,
                "perceptual_hash": best_payload.perceptual_hash,
                "fetch_status": FetchStatus.cached,
                "crawl_timestamp": best_candidate.crawl_timestamp,
                "provenance": candidate.provenance.model_copy(
                    update={
                        "http_status": best_payload.provenance.http_status,
                        "redirect_chain": best_payload.provenance.redirect_chain,
                        "steps": [
                            *candidate.provenance.steps,
                            *best_payload.provenance.steps,
                            ProvenanceStep(
                                stage="source_page_resolve",
                                source="extractor",
                                details={
                                    "source_page_url": candidate.source_page_url,
                                    "resolved_image_url": best_payload.source_url,
                                    "resolved_width": best_payload.width,
                                    "resolved_height": best_payload.height,
                                    "candidate_count": len(resolved_candidates),
                                },
                            ),
                        ],
                    }
                ),
            }
        )
        return updated, resolved_candidates

    @staticmethod
    def _source_resolution_score(candidate: CandidateRecord, preferred_domain: str | None) -> float:
        width = float(candidate.width or 0)
        height = float(candidate.height or 0)
        score = width * height
        if candidate.source_domain and preferred_domain and candidate.source_domain == preferred_domain:
            score += 250_000
        if candidate.provenance.steps:
            source = candidate.provenance.steps[0].source
            if source == "metadata":
                score += 500_000
            elif source == "jsonld":
                score += 350_000
        return score

    @staticmethod
    def _should_replace_with_source_image(
        candidate: CandidateRecord,
        *,
        current_area: int,
        best_area: int,
        best_url: str,
    ) -> bool:
        if best_area <= 0:
            return False
        if candidate.image_url == best_url and not candidate.local_cache_path:
            return True
        if current_area <= 0:
            return True
        if RetrievalService._looks_like_preview_image(candidate.image_url):
            return True
        return best_area > current_area * 1.2

    @staticmethod
    def _looks_like_preview_image(url: str | None) -> bool:
        if not url:
            return True
        lowered = url.lower()
        if lowered.startswith("data:"):
            return True
        preview_markers = (
            "encrypted-tbn",
            "googleusercontent",
            "gstatic.com/images",
            "/images?q=tbn:",
            "imgres",
        )
        return any(marker in lowered for marker in preview_markers)


def build_service(settings: Settings | None = None) -> RetrievalService:
    settings = settings or get_settings()
    repository = build_repository(settings.database_url)
    object_store = build_object_store(settings)
    cache = FileCache(settings.local_cache_dir)
    screenshot_root = settings.local_cache_dir / "screenshots"
    browser_capture = PlaywrightBrowserCapture(screenshot_root) if settings.enable_browser_capture else NoopBrowserCapture()
    robots_policy = RobotsPolicy(settings)
    rate_limiter = DomainRateLimiter(settings.rate_limit_per_domain_seconds)
    fetcher = ImageFetcher(settings, robots_policy=robots_policy, rate_limiter=rate_limiter)
    extractor = DirectPageImageExtractor(settings=settings, fetcher=fetcher, browser_capture=browser_capture)
    crawler = SiteCrawlerAdapter(settings=settings, fetcher=fetcher, extractor=extractor)
    google_adapter = GoogleImageDiscoveryAdapter(settings=settings)
    pinchtab_client = PinchTabClient(settings=settings)
    contact_sheet_builder = ContactSheetBuilder(settings=settings)
    google_browser_adapter = GoogleImagesBrowserAdapter(
        settings=settings,
        pinchtab=pinchtab_client,
        sheet_builder=contact_sheet_builder,
    )
    return RetrievalService(
        settings=settings,
        repository=repository,
        object_store=object_store,
        cache=cache,
        google_adapter=google_adapter,
        extractor=extractor,
        crawler=crawler,
        fetcher=fetcher,
        google_browser_adapter=google_browser_adapter,
    )
