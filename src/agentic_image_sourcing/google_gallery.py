from __future__ import annotations

import base64
import math
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Iterable

import requests
from PIL import Image, ImageDraw, ImageOps

from .config import Settings


@dataclass
class RenderableTile:
    tile_index: int
    thumbnail_url: str | None
    crop_rect: tuple[int, int, int, int] | None = None


class ContactSheetBuilder:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})

    def build(
        self,
        gallery_id: str,
        tiles: Iterable[RenderableTile],
        output_dir: Path,
        screenshot_bytes: bytes | None = None,
    ) -> str:
        renderables = list(tiles)
        if not renderables:
            raise RuntimeError("Cannot build a contact sheet with no tiles")

        tile_size = self.settings.google_gallery_tile_size
        columns = max(1, self.settings.google_gallery_columns)
        rows = math.ceil(len(renderables) / columns)
        canvas = Image.new("RGB", (columns * tile_size, rows * tile_size), color="#111111")
        screenshot = Image.open(BytesIO(screenshot_bytes)).convert("RGB") if screenshot_bytes else None

        for position, tile in enumerate(renderables):
            image = self._tile_image(tile, tile_size, screenshot)
            x = (position % columns) * tile_size
            y = (position // columns) * tile_size
            canvas.paste(image, (x, y))
            overlay = ImageDraw.Draw(canvas)
            overlay.rectangle((x + 8, y + 8, x + 48, y + 40), fill="#000000")
            overlay.text((x + 16, y + 14), str(tile.tile_index), fill="#ffffff")

        output_dir.mkdir(parents=True, exist_ok=True)
        target = output_dir / f"{gallery_id}.png"
        canvas.save(target, format="PNG")
        if screenshot:
            screenshot.close()
        return str(target.resolve())

    def _tile_image(self, tile: RenderableTile, tile_size: int, screenshot: Image.Image | None) -> Image.Image:
        image = self._from_thumbnail(tile.thumbnail_url, tile_size)
        if image is not None:
            return image
        if screenshot and tile.crop_rect:
            left, top, right, bottom = tile.crop_rect
            cropped = screenshot.crop((left, top, right, bottom))
            return ImageOps.fit(cropped.convert("RGB"), (tile_size, tile_size))
        return self._placeholder(tile_size)

    def _from_thumbnail(self, thumbnail_url: str | None, tile_size: int) -> Image.Image | None:
        if not thumbnail_url:
            return None
        try:
            if thumbnail_url.startswith("data:image/"):
                _, encoded = thumbnail_url.split(",", 1)
                data = base64.b64decode(encoded)
            else:
                response = self.session.get(thumbnail_url, timeout=self.settings.request_timeout_seconds)
                response.raise_for_status()
                data = response.content
            with Image.open(BytesIO(data)) as image:
                return ImageOps.fit(image.convert("RGB"), (tile_size, tile_size))
        except Exception:
            return None

    @staticmethod
    def _placeholder(tile_size: int) -> Image.Image:
        image = Image.new("RGB", (tile_size, tile_size), color="#2b2b2b")
        overlay = ImageDraw.Draw(image)
        overlay.rectangle((16, 16, tile_size - 16, tile_size - 16), outline="#666666", width=4)
        overlay.line((16, 16, tile_size - 16, tile_size - 16), fill="#666666", width=4)
        overlay.line((tile_size - 16, 16, 16, tile_size - 16), fill="#666666", width=4)
        return image
