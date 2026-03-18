from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AIS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Agentic Image Sourcing"
    environment: str = "development"

    database_url: str = "sqlite:///./var/agentic_image_sourcing.db"
    local_cache_dir: Path = Path("./var/cache")
    local_object_store_dir: Path = Path("./var/object-store")

    object_store_backend: str = "local"
    s3_bucket: str | None = None
    s3_endpoint_url: str | None = None
    s3_region: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_prefix: str = "assets"

    google_api_key: str | None = None
    google_cse_id: str | None = None
    pinchtab_base_url: str = "http://127.0.0.1:9867"
    pinchtab_token: str | None = None
    pinchtab_default_browser_mode: str = "headed"
    pinchtab_start_port: int | None = None
    pinchtab_gallery_scroll_step: int = 1200
    pinchtab_gallery_scroll_attempts: int = 8
    pinchtab_scroll_pause_seconds: float = 1.0
    google_gallery_tile_size: int = 256
    google_gallery_columns: int = 4

    user_agent: str = "AgenticImageSourcing/0.1"
    request_timeout_seconds: float = 20.0
    page_extract_timeout_seconds: float = 15.0
    max_retries: int = 2
    max_image_bytes: int = 15_000_000
    max_crawl_pages: int = 5
    default_result_limit: int = 10
    rate_limit_per_domain_seconds: float = 1.0
    crawl_respect_robots: bool = True
    enable_browser_capture: bool = False

    crawl_allow_domains: list[str] = Field(default_factory=list)
    crawl_deny_domains: list[str] = Field(default_factory=lambda: ["google.com", "www.google.com"])
    allowed_image_mime_types: list[str] = Field(
        default_factory=lambda: ["image/jpeg", "image/png", "image/webp", "image/gif", "image/bmp"]
    )

    @field_validator("crawl_allow_domains", "crawl_deny_domains", "allowed_image_mime_types", mode="before")
    @classmethod
    def _split_csv(cls, value: str | list[str] | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [item.strip() for item in value.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.local_cache_dir.mkdir(parents=True, exist_ok=True)
    settings.local_object_store_dir.mkdir(parents=True, exist_ok=True)
    if settings.database_url.startswith("sqlite:///"):
        db_path = Path(settings.database_url.replace("sqlite:///", "", 1))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
