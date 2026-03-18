from __future__ import annotations

import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..browser_capture import BrowserCapture, NoopBrowserCapture
from ..config import Settings
from ..fetching import ImageFetcher
from ..models import CandidateRecord, Provenance, ProvenanceStep, utc_now
from ..utils import domain_for_url


LAZY_IMAGE_ATTRS = [
    "src",
    "data-src",
    "data-lazy-src",
    "data-original",
    "data-image",
    "data-srcset",
]


class DirectPageImageExtractor:
    def __init__(
        self,
        settings: Settings,
        fetcher: ImageFetcher,
        browser_capture: BrowserCapture | None = None,
    ) -> None:
        self.settings = settings
        self.fetcher = fetcher
        self.browser_capture = browser_capture or NoopBrowserCapture()

    def extract(
        self,
        url: str,
        limit: int,
        capture_page_screenshot: bool = False,
    ) -> list[CandidateRecord]:
        html, provenance = self.fetcher.fetch_page(url)
        screenshot_path = self.browser_capture.capture(url) if capture_page_screenshot else None
        return self.extract_from_html(
            source_url=url,
            html=html,
            limit=limit,
            screenshot_path=screenshot_path,
            page_provenance=provenance,
        )

    def extract_from_html(
        self,
        source_url: str,
        html: str,
        limit: int,
        screenshot_path: str | None = None,
        page_provenance: Provenance | None = None,
    ) -> list[CandidateRecord]:
        soup = BeautifulSoup(html, "html.parser")
        page_title = soup.title.string.strip() if soup.title and soup.title.string else None
        now = utc_now()

        discovered: list[CandidateRecord] = []
        seen_urls: set[str] = set()

        def add_candidate(image_url: str | None, alt_text: str | None, nearby_text: str | None, source: str) -> None:
            if not image_url:
                return
            absolute = urljoin(source_url, image_url.strip())
            if absolute in seen_urls:
                return
            seen_urls.add(absolute)
            steps = [ProvenanceStep(stage="extract", source=source, details={"source_url": source_url})]
            if page_provenance:
                steps = [*page_provenance.steps, *steps]
            discovered.append(
                CandidateRecord(
                    image_url=absolute,
                    source_page_url=source_url,
                    source_domain=domain_for_url(source_url),
                    page_title=page_title,
                    alt_text=alt_text,
                    nearby_text=nearby_text,
                    crawl_timestamp=now,
                    page_screenshot_path=screenshot_path,
                    provenance=Provenance(
                        discovery_method="page_extract",
                        discovered_at=now,
                        crawl_timestamp=now,
                        steps=steps,
                    ),
                )
            )

        for tag in soup.select("meta[property='og:image'], meta[name='twitter:image'], link[rel='image_src']"):
            add_candidate(tag.get("content") or tag.get("href"), None, None, "metadata")

        for script in soup.select("script[type='application/ld+json']"):
            try:
                payload = json.loads(script.string or "")
            except json.JSONDecodeError:
                continue
            for image_url in self._images_from_jsonld(payload):
                add_candidate(image_url, None, None, "jsonld")

        for image in soup.find_all("img"):
            candidate_url = None
            for attr in LAZY_IMAGE_ATTRS:
                value = image.get(attr)
                if value:
                    candidate_url = self._best_from_srcset(value) if "srcset" in attr else value
                    break
            if not candidate_url:
                continue
            nearby_text = image.parent.get_text(" ", strip=True)[:240] if image.parent else None
            add_candidate(candidate_url, image.get("alt"), nearby_text, "img")
            if len(discovered) >= limit:
                break

        return discovered[:limit]

    def _images_from_jsonld(self, payload) -> list[str]:
        if isinstance(payload, dict):
            images = payload.get("image")
            if isinstance(images, str):
                return [images]
            if isinstance(images, list):
                return [item for item in images if isinstance(item, str)]
            return []
        if isinstance(payload, list):
            images: list[str] = []
            for item in payload:
                images.extend(self._images_from_jsonld(item))
            return images
        return []

    @staticmethod
    def _best_from_srcset(srcset: str) -> str:
        best = srcset.split(",")[-1].strip()
        return best.split(" ")[0]
