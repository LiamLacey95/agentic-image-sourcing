from __future__ import annotations

from io import BytesIO
from pathlib import Path

from PIL import Image

from agentic_image_sourcing.config import Settings
from agentic_image_sourcing.google_gallery import ContactSheetBuilder, RenderableTile


def test_contact_sheet_builder_uses_screenshot_crop_fallback(tmp_path: Path) -> None:
    settings = Settings(local_cache_dir=tmp_path / "cache", local_object_store_dir=tmp_path / "objects")
    builder = ContactSheetBuilder(settings=settings)

    screenshot = Image.new("RGB", (400, 400), "green")
    buffer = BytesIO()
    screenshot.save(buffer, format="PNG")

    output = builder.build(
        gallery_id="fallback-gallery",
        tiles=[RenderableTile(tile_index=1, thumbnail_url=None, crop_rect=(10, 10, 210, 210))],
        output_dir=tmp_path / "galleries",
        screenshot_bytes=buffer.getvalue(),
    )

    assert Path(output).exists()
