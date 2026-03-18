from __future__ import annotations

from .models import (
    ExtractRequest,
    FetchRequest,
    GoogleDownloadRequest,
    GoogleGalleryRequest,
    GoogleInspectRequest,
    SaveAssetRequest,
    SearchRequest,
)
from .service import RetrievalService, build_service


def create_mcp_server(service: RetrievalService | None = None):
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("Install the optional 'mcp' dependency to run the MCP server") from exc

    service = service or build_service()
    server = FastMCP("agentic-image-sourcing")

    @server.tool()
    def image_search(query: str, limit: int = 10, preferred_domains: list[str] | None = None) -> dict:
        response = service.image_search(
            SearchRequest(query=query, limit=limit, preferred_domains=preferred_domains or [])
        )
        return response.model_dump(mode="json")

    @server.tool()
    def page_extract_images(url: str, limit: int = 20, crawl: bool = False) -> dict:
        response = service.page_extract_images(ExtractRequest(url=url, limit=limit, crawl=crawl))
        return response.model_dump(mode="json")

    @server.tool()
    def google_image_gallery(
        query: str,
        batch_size: int = 12,
        offset: int = 0,
        browser_mode: str | None = None,
        profile_id: str | None = None,
        instance_id: str | None = None,
    ) -> dict:
        response = service.google_image_gallery(
            GoogleGalleryRequest(
                query=query,
                batch_size=batch_size,
                offset=offset,
                browser_mode=browser_mode,
                profile_id=profile_id,
                instance_id=instance_id,
            )
        )
        return response.model_dump(mode="json")

    @server.tool()
    def google_image_inspect(candidate_id: str, open_source_page: bool = False) -> dict:
        response = service.google_image_inspect(
            GoogleInspectRequest(candidate_id=candidate_id, open_source_page=open_source_page)
        )
        return response.model_dump(mode="json")

    @server.tool()
    def google_image_download(candidate_id: str, collection: str, tags: list[str] | None = None) -> dict:
        response = service.google_image_download(
            GoogleDownloadRequest(candidate_id=candidate_id, collection=collection, tags=tags or [])
        )
        return response.model_dump(mode="json")

    @server.tool()
    def image_fetch(candidate_id: str | None = None, url: str | None = None, mode: str = "full") -> dict:
        response = service.image_fetch(FetchRequest(candidate_id=candidate_id, url=url, mode=mode))
        return response.model_dump(mode="json")

    @server.tool()
    def candidate_inspect(candidate_id: str) -> dict:
        response = service.candidate_inspect(candidate_id)
        return response.model_dump(mode="json")

    @server.tool()
    def asset_save(candidate_id: str, collection: str, tags: list[str] | None = None) -> dict:
        response = service.asset_save(
            SaveAssetRequest(candidate_id=candidate_id, collection=collection, tags=tags or [])
        )
        return response.model_dump(mode="json")

    return server
