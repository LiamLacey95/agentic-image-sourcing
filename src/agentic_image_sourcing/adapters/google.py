from __future__ import annotations

import requests

from ..config import Settings
from ..models import CandidateRecord, Provenance, ProvenanceStep, utc_now
from ..utils import domain_for_url


class GoogleImageDiscoveryAdapter:
    endpoint = "https://customsearch.googleapis.com/customsearch/v1"

    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})

    @property
    def available(self) -> bool:
        return bool(self.settings.google_api_key and self.settings.google_cse_id)

    def discover(self, query: str, limit: int, preferred_domains: list[str] | None = None) -> list[CandidateRecord]:
        if not self.available:
            return []

        params = {
            "key": self.settings.google_api_key,
            "cx": self.settings.google_cse_id,
            "q": query,
            "searchType": "image",
            "num": min(max(limit, 1), 10),
        }
        if preferred_domains:
            params["siteSearch"] = " OR ".join(preferred_domains)

        response = self.session.get(self.endpoint, params=params, timeout=self.settings.request_timeout_seconds)
        response.raise_for_status()
        items = response.json().get("items", [])
        candidates: list[CandidateRecord] = []
        now = utc_now()
        for item in items:
            image_meta = item.get("image", {})
            source_page_url = image_meta.get("contextLink")
            candidates.append(
                CandidateRecord(
                    query_text=query,
                    image_url=item["link"],
                    thumbnail_url=image_meta.get("thumbnailLink"),
                    source_page_url=source_page_url,
                    source_domain=domain_for_url(source_page_url) or item.get("displayLink"),
                    mime_type=item.get("mime"),
                    width=image_meta.get("width"),
                    height=image_meta.get("height"),
                    page_title=item.get("title"),
                    nearby_text=item.get("snippet"),
                    crawl_timestamp=now,
                    provenance=Provenance(
                        discovery_method="google_custom_search",
                        discovered_at=now,
                        crawl_timestamp=now,
                        steps=[
                            ProvenanceStep(
                                stage="discover",
                                source="google_custom_search",
                                details={"title": item.get("title"), "display_link": item.get("displayLink")},
                            )
                        ],
                    ),
                )
            )
        return candidates
