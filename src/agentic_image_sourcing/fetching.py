from __future__ import annotations

import time
from dataclasses import dataclass
from io import BytesIO

import requests
from PIL import Image

from .config import Settings
from .models import FetchMode, FetchStatus, Provenance, ProvenanceStep, utc_now
from .policies import DomainRateLimiter, RobotsPolicy
from .utils import sha256_bytes


def average_hash(data: bytes) -> str | None:
    try:
        with Image.open(BytesIO(data)) as image:
            grayscale = image.convert("L").resize((8, 8))
            pixels = list(grayscale.getdata())
    except Exception:
        return None

    if not pixels:
        return None
    avg = sum(pixels) / len(pixels)
    bits = "".join("1" if pixel >= avg else "0" for pixel in pixels)
    return f"{int(bits, 2):016x}"


@dataclass
class ImageFetchPayload:
    source_url: str
    mode: FetchMode
    data: bytes
    mime_type: str | None
    width: int | None
    height: int | None
    byte_size: int
    content_hash: str
    perceptual_hash: str | None
    provenance: Provenance
    fetch_status: FetchStatus


class ImageFetcher:
    def __init__(
        self,
        settings: Settings,
        robots_policy: RobotsPolicy,
        rate_limiter: DomainRateLimiter,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings
        self.robots_policy = robots_policy
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})

    def fetch_page(self, url: str) -> tuple[str, Provenance]:
        response = self._request(url)
        provenance = Provenance(
            discovery_method="page_fetch",
            crawl_timestamp=utc_now(),
            http_status=response.status_code,
            redirect_chain=[item.url for item in response.history] + [response.url],
            steps=[
                ProvenanceStep(
                    stage="fetch_page",
                    source="http",
                    details={"content_type": response.headers.get("Content-Type")},
                )
            ],
        )
        return response.text, provenance

    def fetch_image(self, url: str, mode: FetchMode) -> ImageFetchPayload:
        response = self._request(url, stream=True)
        content_type = response.headers.get("Content-Type", "").split(";")[0].strip() or None
        if content_type and content_type not in self.settings.allowed_image_mime_types:
            raise ValueError(f"Unsupported image MIME type: {content_type}")

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > self.settings.max_image_bytes:
            raise ValueError(f"Image exceeds configured size limit: {content_length}")

        data = response.content
        if len(data) > self.settings.max_image_bytes:
            raise ValueError(f"Image exceeds configured size limit: {len(data)}")

        width, height = self._dimensions(data)
        payload = ImageFetchPayload(
            source_url=response.url,
            mode=mode,
            data=data,
            mime_type=content_type,
            width=width,
            height=height,
            byte_size=len(data),
            content_hash=sha256_bytes(data),
            perceptual_hash=average_hash(data),
            provenance=Provenance(
                discovery_method="image_fetch",
                crawl_timestamp=utc_now(),
                http_status=response.status_code,
                redirect_chain=[item.url for item in response.history] + [response.url],
                steps=[
                    ProvenanceStep(
                        stage="fetch_image",
                        source="http",
                        details={
                            "mode": mode.value,
                            "content_type": content_type,
                            "content_length": len(data),
                        },
                    )
                ],
            ),
            fetch_status=FetchStatus.fetched,
        )
        return payload

    def _request(self, url: str, stream: bool = False) -> requests.Response:
        if not self.robots_policy.is_allowed(url):
            raise PermissionError(f"Robots policy denied access to {url}")

        last_error: Exception | None = None
        for attempt in range(self.settings.max_retries + 1):
            try:
                self.rate_limiter.wait(url)
                response = self.session.get(
                    url,
                    timeout=self.settings.request_timeout_seconds,
                    allow_redirects=True,
                    stream=stream,
                )
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.settings.max_retries:
                    break
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"Failed to fetch {url}") from last_error

    @staticmethod
    def _dimensions(data: bytes) -> tuple[int | None, int | None]:
        try:
            with Image.open(BytesIO(data)) as image:
                return image.size
        except Exception:
            return None, None
