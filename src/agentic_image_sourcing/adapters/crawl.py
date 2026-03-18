from __future__ import annotations

from collections import deque
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..config import Settings
from ..fetching import ImageFetcher
from ..models import CandidateRecord
from ..utils import domain_for_url
from .extract import DirectPageImageExtractor


class SiteCrawlerAdapter:
    def __init__(self, settings: Settings, fetcher: ImageFetcher, extractor: DirectPageImageExtractor) -> None:
        self.settings = settings
        self.fetcher = fetcher
        self.extractor = extractor

    def crawl(self, start_url: str, limit: int, max_pages: int) -> list[CandidateRecord]:
        origin_domain = domain_for_url(start_url)
        queue = deque([start_url])
        visited_pages: set[str] = set()
        discovered: list[CandidateRecord] = []

        while queue and len(visited_pages) < min(max_pages, self.settings.max_crawl_pages):
            current = queue.popleft()
            if current in visited_pages:
                continue
            visited_pages.add(current)
            html, provenance = self.fetcher.fetch_page(current)
            candidates = self.extractor.extract_from_html(
                source_url=current,
                html=html,
                limit=limit - len(discovered),
                page_provenance=provenance,
            )
            discovered.extend(candidates)
            if len(discovered) >= limit:
                break

            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all("a", href=True):
                next_url = urljoin(current, link["href"])
                if urlparse(next_url).scheme not in {"http", "https"}:
                    continue
                if origin_domain and domain_for_url(next_url) != origin_domain:
                    continue
                if next_url not in visited_pages:
                    queue.append(next_url)
        return discovered[:limit]
