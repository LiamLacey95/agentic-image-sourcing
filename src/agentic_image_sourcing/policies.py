from __future__ import annotations

import threading
import time
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

import requests

from .config import Settings
from .utils import domain_for_url


class DomainRateLimiter:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(interval_seconds, 0.0)
        self._lock = threading.Lock()
        self._last_seen: dict[str, float] = {}

    def wait(self, url: str) -> None:
        domain = domain_for_url(url)
        if not domain or self.interval_seconds == 0:
            return
        with self._lock:
            now = time.monotonic()
            last_seen = self._last_seen.get(domain)
            if last_seen is not None:
                sleep_for = self.interval_seconds - (now - last_seen)
                if sleep_for > 0:
                    time.sleep(sleep_for)
            self._last_seen[domain] = time.monotonic()


class RobotsPolicy:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()

    def is_allowed(self, url: str) -> bool:
        domain = domain_for_url(url)
        if not domain:
            return False

        if self.settings.crawl_allow_domains and domain not in self.settings.crawl_allow_domains:
            return False
        if domain in self.settings.crawl_deny_domains:
            return False
        if not self.settings.crawl_respect_robots:
            return True

        parser = self._parser_for_domain(domain)
        return parser.can_fetch(self.settings.user_agent, url)

    def _parser_for_domain(self, domain: str) -> RobotFileParser:
        with self._lock:
            cached = self._cache.get(domain)
            if cached is not None:
                return cached

            robots_url = urljoin(f"https://{domain}", "/robots.txt")
            parser = RobotFileParser()
            try:
                response = self.session.get(
                    robots_url,
                    timeout=self.settings.request_timeout_seconds,
                    headers={"User-Agent": self.settings.user_agent},
                )
                if response.ok:
                    parser.parse(response.text.splitlines())
                else:
                    parser.allow_all = True
            except requests.RequestException:
                parser.allow_all = True
            self._cache[domain] = parser
            return parser
