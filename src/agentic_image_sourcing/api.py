from __future__ import annotations

from fastapi import FastAPI, HTTPException

from .config import Settings, get_settings
from .models import (
    AssetSaveResponse,
    CandidateListResponse,
    CandidateRecord,
    ExtractRequest,
    FetchRequest,
    FetchResult,
    GoogleDownloadRequest,
    GoogleGalleryRequest,
    GoogleGalleryResponse,
    GoogleInspectRequest,
    GoogleInspectResponse,
    SaveAssetRequest,
    SearchRequest,
)
from .service import RetrievalService, build_service


def create_app(service: RetrievalService | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    service = service or build_service(settings)
    app = FastAPI(title=settings.app_name)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/search", response_model=CandidateListResponse)
    def search_images(request: SearchRequest) -> CandidateListResponse:
        return service.image_search(request)

    @app.post("/extract", response_model=CandidateListResponse)
    def page_extract_images(request: ExtractRequest) -> CandidateListResponse:
        return service.page_extract_images(request)

    @app.post("/google/gallery", response_model=GoogleGalleryResponse)
    def google_image_gallery(request: GoogleGalleryRequest) -> GoogleGalleryResponse:
        try:
            return service.google_image_gallery(request)
        except (PermissionError, RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/google/inspect", response_model=GoogleInspectResponse)
    def google_image_inspect(request: GoogleInspectRequest) -> GoogleInspectResponse:
        try:
            return service.google_image_inspect(request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (PermissionError, RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/google/download", response_model=AssetSaveResponse)
    def google_image_download(request: GoogleDownloadRequest) -> AssetSaveResponse:
        try:
            return service.google_image_download(request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (PermissionError, RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/fetch", response_model=FetchResult)
    def image_fetch(request: FetchRequest) -> FetchResult:
        try:
            return service.image_fetch(request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (PermissionError, RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/candidates/{candidate_id}", response_model=CandidateRecord)
    def candidate_inspect(candidate_id: str) -> CandidateRecord:
        try:
            return service.candidate_inspect(candidate_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/assets", response_model=AssetSaveResponse)
    def asset_save(request: SaveAssetRequest) -> AssetSaveResponse:
        try:
            return service.asset_save(request)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (PermissionError, RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app
