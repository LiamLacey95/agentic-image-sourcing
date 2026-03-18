from __future__ import annotations

from agentic_image_sourcing.adapters.extract import DirectPageImageExtractor
from agentic_image_sourcing.config import Settings


class DummyFetcher:
    def fetch_page(self, url: str):
        raise NotImplementedError


def test_extract_from_html_finds_metadata_jsonld_and_lazy_images() -> None:
    extractor = DirectPageImageExtractor(settings=Settings(), fetcher=DummyFetcher())
    html = """
    <html>
      <head>
        <title>Gallery</title>
        <meta property="og:image" content="/meta.jpg" />
        <script type="application/ld+json">
          {"image":["https://cdn.example.com/jsonld.jpg"]}
        </script>
      </head>
      <body>
        <figure>
          <img data-src="/lazy.png" alt="Lazy image" />
          <figcaption>A relevant nearby caption</figcaption>
        </figure>
        <img data-srcset="/small.jpg 320w, /large.jpg 960w" alt="Hero" />
      </body>
    </html>
    """

    candidates = extractor.extract_from_html("https://example.com/gallery", html, limit=10)
    urls = [candidate.image_url for candidate in candidates]

    assert "https://example.com/meta.jpg" in urls
    assert "https://cdn.example.com/jsonld.jpg" in urls
    assert "https://example.com/lazy.png" in urls
    assert "https://example.com/large.jpg" in urls
    assert any(candidate.alt_text == "Lazy image" for candidate in candidates)
