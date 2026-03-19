from __future__ import annotations

import time
from collections.abc import Iterable
from threading import Lock
from urllib.parse import quote_plus, urlparse
from uuid import uuid4

from ..config import Settings
from ..google_gallery import ContactSheetBuilder, RenderableTile
from ..models import BrowserMode, CandidateRecord, GoogleGalleryRequest, Provenance, ProvenanceStep, utc_now
from ..pinchtab_client import PinchTabClient
from ..utils import domain_for_url


GALLERY_EXTRACTION_JS = """
(() => {
  const normalizeText = (value) => ((value || '').replace(/\\s+/g, ' ').trim() || null);
  const readImageSource = (img) =>
    img?.currentSrc || img?.src || img?.getAttribute?.('src') || img?.getAttribute?.('data-src') || null;
  const fromImgres = (href) => {
    try {
      const url = new URL(href, location.origin);
      return {
        imageUrl: url.searchParams.get('imgurl'),
        sourcePageUrl: url.searchParams.get('imgrefurl')
      };
    } catch (err) {
      return { imageUrl: null, sourcePageUrl: null };
    }
  };
  const isMeaningful = (img) => {
    const src = readImageSource(img) || '';
    const width = img.naturalWidth || img.width || 0;
    const height = img.naturalHeight || img.height || 0;
    return Boolean(src) && (width >= 120 || height >= 120);
  };
  const isExternalHref = (href) => {
    try {
      const host = new URL(href, location.origin).hostname.toLowerCase();
      return !host.startsWith('google.') && !host.startsWith('www.google.') && !host.endsWith('.google.com');
    } catch (err) {
      return false;
    }
  };
  const rectFor = (node) => {
    const rect = (node || document.body).getBoundingClientRect();
    return {
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height
    };
  };
  const preferExternalLink = (card) => {
    const links = Array.from(card.querySelectorAll('a[href]'))
      .map((link) => ({
        href: link.href,
        text: normalizeText(link.innerText),
        ariaLabel: normalizeText(link.getAttribute('aria-label')),
        className: link.className || ''
      }))
      .filter((link) => isExternalHref(link.href));
    return (
      links.find((link) => link.className.includes('EZAeBe')) ||
      links.find((link) => Boolean(link.text || link.ariaLabel)) ||
      links[0] ||
      null
    );
  };
  const resultCards = Array.from(document.querySelectorAll('div[data-lpage][data-docid], div[data-lpage], div[data-docid]'));
  if (resultCards.length) {
    const items = [];
    const seen = new Set();
    for (const card of resultCards) {
      const wrapper =
        card.querySelector('div.F0uyec[role="button"], div.F0uyec, [role="button"][jsaction*="J29LQb"]') ||
        card.querySelector('[role="button"], [jsaction*="J29LQb"]') ||
        card;
      const img = wrapper.querySelector('img') || card.querySelector('img');
      if (!img || !isMeaningful(img)) continue;
      const thumb = readImageSource(img);
      const sourceLink = preferExternalLink(card);
      const sourcePageUrl = card.getAttribute('data-lpage') || sourceLink?.href || null;
      const rect = rectFor(wrapper.getBoundingClientRect().width > 1 || wrapper.getBoundingClientRect().height > 1 ? wrapper : img);
      const dedupeKey = [card.getAttribute('data-docid') || '', thumb || '', sourcePageUrl || '', Math.round(rect.top), Math.round(rect.left)].join('|');
      if (!thumb || seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      items.push({
        domIndex: items.length,
        googleResultUrl: null,
        imageUrl: thumb,
        sourcePageUrl,
        thumbnailUrl: thumb,
        altText: normalizeText(img.alt) || sourceLink?.ariaLabel || sourceLink?.text,
        nearbyText: sourceLink?.text || normalizeText(card.innerText),
        docId: card.getAttribute('data-docid') || null,
        rect
      });
    }
    return {
      items,
      viewport: {
        width: window.innerWidth,
        height: window.innerHeight,
        dpr: window.devicePixelRatio || 1
      },
      pageTitle: document.title,
      currentUrl: location.href
    };
  }

  const items = [];
  const seen = new Set();
  const images = Array.from(document.querySelectorAll('img'));
  for (const img of images) {
    if (!isMeaningful(img)) continue;
    const wrapper = img.closest('[jsaction], a, [role="link"], [role="button"]') || img;
    const container = wrapper.closest('div[data-ved], div.isv-r, [data-docid], [data-lpage], [data-tbnid], [jscontroller]') || wrapper.parentElement || wrapper;
    const href = wrapper instanceof HTMLAnchorElement ? wrapper.href : wrapper.getAttribute?.('href') || null;
    const thumb = readImageSource(img);
    const parsed = href && href.includes('/imgres?') ? fromImgres(href) : { imageUrl: null, sourcePageUrl: null };
    const googleResultUrl = href && !isExternalHref(href) ? href : null;
    const sourcePageUrl = parsed.sourcePageUrl || (href && isExternalHref(href) ? href : null);
    const rectNode = wrapper.getBoundingClientRect().width > 1 || wrapper.getBoundingClientRect().height > 1 ? wrapper : img;
    const rect = rectFor(rectNode);
    const dedupeKey = [
      thumb,
      normalizeText(img.alt),
      Math.round(rect.top),
      Math.round(rect.left),
      Math.round(rect.width),
      Math.round(rect.height)
    ].join('|');
    if (!thumb || seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    items.push({
      domIndex: items.length,
      googleResultUrl,
      imageUrl: parsed.imageUrl || thumb,
      sourcePageUrl,
      thumbnailUrl: thumb,
      altText: normalizeText(img.alt) || normalizeText(wrapper.getAttribute?.('aria-label')),
      nearbyText: normalizeText(container?.innerText) || normalizeText(wrapper.innerText),
      rect: {
        left: rect.left,
        top: rect.top,
        width: rect.width,
        height: rect.height
      }
    });
  }
  return {
    items,
    viewport: {
      width: window.innerWidth,
      height: window.innerHeight,
      dpr: window.devicePixelRatio || 1
    },
    pageTitle: document.title,
    currentUrl: location.href
  };
})()
"""

BLOCK_PAGE_JS = """
(() => {
  const bodyText = ((document.body?.innerText || '').replace(/\\s+/g, ' ').trim() || '').toLowerCase();
  const title = document.title || '';
  const href = location.href;
  const blocked =
    href.includes('/sorry/') ||
    title.toLowerCase().includes('sorry') ||
    bodyText.includes('unusual traffic') ||
    bodyText.includes('detected unusual traffic') ||
    bodyText.includes('our systems have detected');
  return {
    blocked,
    currentUrl: href,
    pageTitle: title,
    bodySnippet: bodyText.slice(0, 500)
  };
})()
"""

CONSENT_JS = """
(() => {
  const labels = ['accept all', 'i agree', 'accept', 'allow all'];
  const nodes = Array.from(document.querySelectorAll('button, input[type="button"], div[role="button"]'));
  const target = nodes.find((node) => {
    const text = ((node.innerText || node.value || '') + '').trim().toLowerCase();
    return labels.some((label) => text.includes(label));
  });
  if (target) {
    target.click();
    return true;
  }
  return false;
})()
"""

INSPECT_JS = """
(() => {
  const isExternalHref = (href) => {
    try {
      const host = new URL(href, location.origin).hostname.toLowerCase();
      if (!host) return false;
      if (host.startsWith('google.') || host.startsWith('www.google.')) return false;
      if (host.endsWith('.google.com')) return false;
      if (host.includes('googleusercontent')) return false;
      return true;
    } catch (err) {
      return false;
    }
  };

  const visibleImages = Array.from(document.images)
    .map((img) => ({
      src: img.currentSrc || img.src || null,
      width: img.naturalWidth || img.width || 0,
      height: img.naturalHeight || img.height || 0,
      area: (img.naturalWidth || img.width || 0) * (img.naturalHeight || img.height || 0)
    }))
    .filter((img) => img.src && img.width >= 120 && img.height >= 120)
    .sort((a, b) => b.area - a.area);

  const externalLinks = Array.from(document.querySelectorAll('a[href]'))
    .map((link) => ({
      href: link.href,
      text: (link.innerText || '').replace(/\\s+/g, ' ').trim(),
      className: link.className || '',
      ariaLabel: (link.getAttribute('aria-label') || '').trim()
    }))
    .filter((link) => isExternalHref(link.href))
    .sort((left, right) => {
      const leftPreferred = left.className.includes('EZAeBe') || Boolean(left.text);
      const rightPreferred = right.className.includes('EZAeBe') || Boolean(right.text);
      return Number(rightPreferred) - Number(leftPreferred);
    });

  return {
    previewImageUrl: visibleImages.length ? visibleImages[0].src : null,
    previewWidth: visibleImages.length ? visibleImages[0].width : null,
    previewHeight: visibleImages.length ? visibleImages[0].height : null,
    sourcePageUrl: externalLinks.length ? externalLinks[0].href : null,
    sourceLabel: externalLinks.length ? (externalLinks[0].text || externalLinks[0].ariaLabel || null) : null,
    pageTitle: document.title
  };
})()
"""

CLICK_TILE_JS_TEMPLATE = """
((targetIndex) => {
  const cards = Array.from(document.querySelectorAll('div[data-lpage][data-docid], div[data-lpage], div[data-docid]'))
    .map((card) => ({
      card,
      wrapper:
        card.querySelector('div.F0uyec[role="button"], div.F0uyec, [role="button"][jsaction*="J29LQb"]') ||
        card.querySelector('[role="button"], [jsaction*="J29LQb"]') ||
        null
    }))
    .filter((item) => item.wrapper);
  const cardTarget = cards[targetIndex];
  if (cardTarget) {
    cardTarget.wrapper.scrollIntoView({ block: 'center', inline: 'center' });
    cardTarget.wrapper.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
    cardTarget.wrapper.click();
    return { clicked: true, count: cards.length };
  }

  const isMeaningful = (img) => {
    const src = img.currentSrc || img.src || img.getAttribute('src') || img.getAttribute('data-src') || '';
    const width = img.naturalWidth || img.width || 0;
    const height = img.naturalHeight || img.height || 0;
    return Boolean(src) && (width >= 120 || height >= 120);
  };

  const matches = [];
  const seen = new Set();
  for (const img of Array.from(document.querySelectorAll('img'))) {
    if (!isMeaningful(img)) continue;
    const wrapper = img.closest('[jsaction], a, [role="link"], [role="button"]') || img;
    const rectNode = wrapper.getBoundingClientRect().width > 1 || wrapper.getBoundingClientRect().height > 1 ? wrapper : img;
    const rect = rectNode.getBoundingClientRect();
    const src = img.currentSrc || img.src || img.getAttribute('src') || img.getAttribute('data-src') || '';
    const key = [
      src,
      (img.alt || '').trim(),
      Math.round(rect.top),
      Math.round(rect.left),
      Math.round(rect.width),
      Math.round(rect.height)
    ].join('|');
    if (!src || seen.has(key)) continue;
    seen.add(key);
    matches.push({ img, wrapper });
  }

  const target = matches[targetIndex];
  if (!target) {
    return { clicked: false, count: matches.length };
  }

  const clickTarget = target.wrapper || target.img;
  clickTarget.scrollIntoView({ block: 'center', inline: 'center' });
  clickTarget.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
  clickTarget.click();
  return { clicked: true, count: matches.length };
})(__TARGET_INDEX__)
"""


class GoogleImagesBrowserAdapter:
    def __init__(
        self,
        settings: Settings,
        pinchtab: PinchTabClient,
        sheet_builder: ContactSheetBuilder,
    ) -> None:
        self.settings = settings
        self.pinchtab = pinchtab
        self.sheet_builder = sheet_builder
        self._managed_instances: dict[tuple[str, str | None], str] = {}
        self._lock = Lock()

    def build_gallery(self, request: GoogleGalleryRequest) -> tuple[str, str, list[CandidateRecord], str]:
        browser_mode = request.browser_mode or BrowserMode(self.settings.pinchtab_default_browser_mode)
        profile_id = request.profile_id or self.settings.pinchtab_default_profile_id
        instance_id = self._ensure_instance(
            browser_mode=browser_mode,
            profile_id=profile_id,
            instance_id=request.instance_id,
        )
        query_url = f"https://www.google.com/search?q={quote_plus(request.query)}&udm=2"
        instance_id, browser_mode = self._open_gallery_page(
            query_url=query_url,
            instance_id=instance_id,
            browser_mode=browser_mode,
            profile_id=profile_id,
            managed_instance=request.instance_id is None,
        )

        effective_offset = request.offset if request.offset > 0 else max(request.batch_number - 1, 0) * request.batch_size
        pool_target = max(
            effective_offset + request.batch_size,
            request.batch_size * max(1, int(self.settings.google_gallery_pool_multiplier)),
        )
        raw = self._collect_gallery_payload(instance_id, pool_target)
        ranked_items = self._rank_items(raw.get("items", []))
        selected = ranked_items[effective_offset : effective_offset + request.batch_size]
        if not selected:
            raise RuntimeError(f"No Google Images results available for batch {request.batch_number}")
        screenshot_bytes = None
        if any(not item.get("thumbnailUrl") for item in selected):
            screenshot_bytes = self.pinchtab.screenshot(instance_id)

        gallery_id = str(uuid4())
        now = utc_now()
        candidates: list[CandidateRecord] = []
        tiles: list[RenderableTile] = []
        for index, item in enumerate(selected, start=1):
            rect = self._crop_rect(item.get("rect"), raw["viewport"])
            resolved_image_url = (
                item.get("imageUrl")
                or item.get("thumbnailUrl")
                or item.get("googleResultUrl")
                or f"google://gallery/{gallery_id}/{index}"
            )
            candidate = CandidateRecord(
                query_text=request.query,
                image_url=resolved_image_url,
                thumbnail_url=item.get("thumbnailUrl"),
                source_page_url=item.get("sourcePageUrl"),
                source_domain=domain_for_url(item.get("sourcePageUrl")),
                page_title=raw.get("pageTitle"),
                alt_text=item.get("altText"),
                nearby_text=item.get("nearbyText"),
                crawl_timestamp=now,
                quality_score=item.get("qualityScore"),
                gallery_id=gallery_id,
                tile_index=index,
                google_result_url=item.get("googleResultUrl"),
                pinchtab_instance_id=instance_id,
                provenance=Provenance(
                    discovery_method="google_images_browser",
                    discovered_at=now,
                    crawl_timestamp=now,
                    steps=[
                        ProvenanceStep(
                            stage="google_gallery",
                            source="pinchtab",
                            details={
                                "query_url": raw.get("currentUrl"),
                                "instance_id": instance_id,
                                "batch_number": request.batch_number,
                                "effective_offset": effective_offset,
                                "tile_index": index,
                                "dom_index": item.get("domIndex"),
                                "quality_score": item.get("qualityScore"),
                                "browser_mode": browser_mode.value,
                                "offset": request.offset,
                            },
                        )
                    ],
                ),
            )
            candidates.append(candidate)
            tiles.append(
                RenderableTile(
                    tile_index=index,
                    thumbnail_url=item.get("thumbnailUrl"),
                    crop_rect=rect,
                )
            )

        gallery_path = self.sheet_builder.build(
            gallery_id=gallery_id,
            tiles=tiles,
            output_dir=self.settings.local_cache_dir / "google-galleries",
            screenshot_bytes=screenshot_bytes,
        )
        return gallery_id, gallery_path, candidates, instance_id

    def inspect_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        if not candidate.pinchtab_instance_id:
            raise RuntimeError("Candidate is missing PinchTab instance context")
        if candidate.google_result_url:
            self.pinchtab.navigate(candidate.pinchtab_instance_id, candidate.google_result_url)
            time.sleep(self.settings.pinchtab_scroll_pause_seconds)
        else:
            self._open_candidate_preview(candidate)

        payload = self.pinchtab.evaluate(candidate.pinchtab_instance_id, INSPECT_JS)
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected PinchTab inspect payload")

        image_url = payload.get("previewImageUrl") or candidate.image_url
        source_page_url = payload.get("sourcePageUrl") or candidate.source_page_url
        now = utc_now()
        return candidate.model_copy(
            update={
                "image_url": image_url,
                "source_page_url": source_page_url,
                "source_domain": domain_for_url(source_page_url) or candidate.source_domain,
                "page_title": payload.get("pageTitle") or candidate.page_title,
                "width": payload.get("previewWidth") or candidate.width,
                "height": payload.get("previewHeight") or candidate.height,
                "crawl_timestamp": now,
                "provenance": candidate.provenance.model_copy(
                    update={
                        "crawl_timestamp": now,
                        "steps": [
                            *candidate.provenance.steps,
                            ProvenanceStep(
                                stage="google_inspect",
                                source="pinchtab",
                                details={
                                    "instance_id": candidate.pinchtab_instance_id,
                                    "google_result_url": candidate.google_result_url,
                                    "source_page_url": source_page_url,
                                },
                            ),
                        ],
                    }
                ),
            }
        )

    def _dismiss_consent(self, instance_id: str) -> None:
        try:
            accepted = self.pinchtab.evaluate(instance_id, CONSENT_JS)
        except Exception:
            return
        if accepted:
            time.sleep(self.settings.pinchtab_scroll_pause_seconds)

    def _open_gallery_page(
        self,
        query_url: str,
        instance_id: str,
        browser_mode: BrowserMode,
        profile_id: str | None,
        managed_instance: bool,
    ) -> tuple[str, BrowserMode]:
        self.pinchtab.navigate(instance_id, query_url)
        time.sleep(self.settings.pinchtab_scroll_pause_seconds)
        self._dismiss_consent(instance_id)
        block_state = self._block_state(instance_id)
        if not block_state.get("blocked"):
            return instance_id, browser_mode
        if browser_mode != BrowserMode.headed and managed_instance:
            fallback_instance_id = self._ensure_instance(BrowserMode.headed, profile_id=profile_id, instance_id=None)
            self.pinchtab.navigate(fallback_instance_id, query_url)
            time.sleep(self.settings.pinchtab_scroll_pause_seconds)
            self._dismiss_consent(fallback_instance_id)
            fallback_state = self._block_state(fallback_instance_id)
            if not fallback_state.get("blocked"):
                return fallback_instance_id, BrowserMode.headed
        raise RuntimeError(
            "Google Images blocked the browser session. "
            f"URL={block_state.get('currentUrl')} title={block_state.get('pageTitle')!r}"
        )

    def _collect_gallery_payload(self, instance_id: str, needed: int) -> dict:
        payload = {"items": [], "viewport": {"width": 0, "height": 0, "dpr": 1}}
        attempts = 0
        while len(payload.get("items", [])) < needed and attempts <= self.settings.pinchtab_gallery_scroll_attempts:
            evaluated = self.pinchtab.evaluate(instance_id, GALLERY_EXTRACTION_JS)
            if isinstance(evaluated, dict):
                payload = evaluated
            if len(payload.get("items", [])) >= needed:
                break
            attempts += 1
            self.pinchtab.evaluate(
                instance_id,
                f"(() => {{ window.scrollBy(0, {self.settings.pinchtab_gallery_scroll_step}); return true; }})()",
            )
            time.sleep(self.settings.pinchtab_scroll_pause_seconds)
        return payload

    def _block_state(self, instance_id: str) -> dict:
        payload = self.pinchtab.evaluate(instance_id, BLOCK_PAGE_JS)
        if isinstance(payload, dict):
            return payload
        return {"blocked": False}

    def _rank_items(self, items: Iterable[dict]) -> list[dict]:
        ranked: list[dict] = []
        for position, item in enumerate(items):
            score = self._score_item(item, position)
            ranked.append({**item, "qualityScore": score})
        ranked.sort(key=lambda item: (-float(item.get("qualityScore") or 0.0), int(item.get("domIndex") or 0)))
        return ranked

    def _score_item(self, item: dict, position: int) -> float:
        score = 0.0
        image_url = str(item.get("imageUrl") or "")
        thumbnail_url = str(item.get("thumbnailUrl") or "")
        source_page_url = str(item.get("sourcePageUrl") or "")
        alt_text = str(item.get("altText") or "")
        nearby_text = str(item.get("nearbyText") or "")
        rect = item.get("rect") or {}
        width = float(rect.get("width") or 0)
        height = float(rect.get("height") or 0)
        area = width * height

        if source_page_url:
            score += 20.0
            if not self._looks_google_owned(source_page_url):
                score += 10.0
        if image_url and not image_url.startswith("data:"):
            score += 10.0
        if thumbnail_url and not thumbnail_url.startswith("data:"):
            score += 5.0
        score += min(20.0, area / 4000.0)
        score += min(10.0, len(alt_text) / 20.0)
        score += min(5.0, len(nearby_text) / 80.0)
        score += max(0.0, 5.0 - min(position, 5))

        lowered = f"{alt_text} {nearby_text} {image_url}".lower()
        penalties = ("logo", "icon", "clipart", "vector", "watermark", "favicon")
        for term in penalties:
            if term in lowered:
                score -= 8.0
        if image_url.startswith("data:"):
            score -= 12.0
        return round(score, 2)

    @staticmethod
    def _looks_google_owned(url: str) -> bool:
        try:
            host = str(urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host.startswith("google.") or host.startswith("www.google.") or host.endswith(".google.com") or "googleusercontent" in host

    def _open_candidate_preview(self, candidate: CandidateRecord) -> None:
        query_url = self._gallery_detail(candidate, "query_url")
        dom_index = self._gallery_detail(candidate, "dom_index")
        if query_url:
            self.pinchtab.navigate(candidate.pinchtab_instance_id, str(query_url))
            time.sleep(self.settings.pinchtab_scroll_pause_seconds)
            self._dismiss_consent(candidate.pinchtab_instance_id)
        target_index = int(dom_index) if dom_index is not None else max((candidate.tile_index or 1) - 1, 0)
        payload = self._collect_gallery_payload(candidate.pinchtab_instance_id, target_index + 1)
        if len(payload.get("items", [])) <= target_index:
            raise RuntimeError(f"Unable to locate Google Images tile {target_index}")
        clicked = self.pinchtab.evaluate(
            candidate.pinchtab_instance_id,
            CLICK_TILE_JS_TEMPLATE.replace("__TARGET_INDEX__", str(target_index)),
        )
        if isinstance(clicked, dict) and not clicked.get("clicked"):
            raise RuntimeError(f"Unable to open Google Images tile {target_index}")
        time.sleep(self.settings.pinchtab_scroll_pause_seconds)

    def _ensure_instance(
        self,
        browser_mode: BrowserMode,
        profile_id: str | None,
        instance_id: str | None,
    ) -> str:
        if instance_id:
            return instance_id
        key = (browser_mode.value, profile_id)
        with self._lock:
            cached = self._managed_instances.get(key)
            if cached:
                try:
                    self.pinchtab.instance_health(cached)
                    return cached
                except Exception:
                    self._managed_instances.pop(key, None)
            response = self.pinchtab.instance_start(mode=browser_mode, profile_id=profile_id)
            created = self._extract_instance_id(response)
            self.pinchtab.wait_for_instance_ready(created)
            self._managed_instances[key] = created
            return created

    @staticmethod
    def _extract_instance_id(payload: dict) -> str:
        for candidate in (
            payload.get("instanceId"),
            payload.get("instance_id"),
            payload.get("id"),
            payload.get("data", {}).get("instanceId") if isinstance(payload.get("data"), dict) else None,
            payload.get("data", {}).get("instance_id") if isinstance(payload.get("data"), dict) else None,
            payload.get("data", {}).get("id") if isinstance(payload.get("data"), dict) else None,
        ):
            if candidate:
                return str(candidate)
        raise RuntimeError("Unable to extract instance id from PinchTab response")

    @staticmethod
    def _crop_rect(rect: dict | None, viewport: dict) -> tuple[int, int, int, int] | None:
        if not rect:
            return None
        dpr = max(1.0, float(viewport.get("dpr", 1)))
        left = max(0, int(rect.get("left", 0) * dpr))
        top = max(0, int(rect.get("top", 0) * dpr))
        width = max(1, int(rect.get("width", 1) * dpr))
        height = max(1, int(rect.get("height", 1) * dpr))
        return (left, top, left + width, top + height)

    @staticmethod
    def _gallery_detail(candidate: CandidateRecord, key: str) -> object | None:
        for step in reversed(candidate.provenance.steps):
            if step.stage == "google_gallery" and key in step.details:
                return step.details.get(key)
        return None
