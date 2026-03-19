from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Protocol

from .models import AssetRecord, CandidateRecord, JobRecord, Provenance, utc_now
from .utils import parse_json, stable_json

CANDIDATE_EXTRA_COLUMNS = {
    "gallery_id": "TEXT",
    "tile_index": "INTEGER",
    "google_result_url": "TEXT",
    "pinchtab_instance_id": "TEXT",
    "quality_score": "REAL",
}


class Repository(Protocol):
    def init_schema(self) -> None:
        ...

    def create_job(self, job: JobRecord) -> JobRecord:
        ...

    def upsert_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        ...

    def link_candidate_to_job(self, job_id: str, candidate_id: str, rank: int) -> None:
        ...

    def list_job_candidates(self, job_id: str, limit: int) -> list[CandidateRecord]:
        ...

    def get_candidate(self, candidate_id: str) -> CandidateRecord | None:
        ...

    def get_candidate_by_url(self, image_url: str) -> CandidateRecord | None:
        ...

    def update_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        ...

    def save_asset(self, asset: AssetRecord) -> AssetRecord:
        ...


class SQLiteRepository:
    def __init__(self, database_url: str) -> None:
        if not database_url.startswith("sqlite:///"):
            raise RuntimeError("SQLiteRepository requires a sqlite:/// database URL")
        self.db_path = Path(database_url.replace("sqlite:///", "", 1))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    query TEXT,
                    source_url TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    request_payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    job_id TEXT,
                    query_text TEXT,
                    image_url TEXT NOT NULL UNIQUE,
                    thumbnail_url TEXT,
                    source_page_url TEXT,
                    source_domain TEXT,
                    mime_type TEXT,
                    width INTEGER,
                    height INTEGER,
                    byte_size INTEGER,
                    page_title TEXT,
                    alt_text TEXT,
                    nearby_text TEXT,
                    crawl_timestamp TEXT NOT NULL,
                    fetch_status TEXT NOT NULL,
                    storage_key TEXT,
                    local_cache_path TEXT,
                    page_screenshot_path TEXT,
                    content_hash TEXT,
                    perceptual_hash TEXT,
                    last_error TEXT,
                    quality_score REAL,
                    gallery_id TEXT,
                    tile_index INTEGER,
                    google_result_url TEXT,
                    pinchtab_instance_id TEXT,
                    provenance_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS job_candidates (
                    job_id TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    discovered_at TEXT NOT NULL,
                    PRIMARY KEY (job_id, candidate_id)
                );

                CREATE TABLE IF NOT EXISTS assets (
                    asset_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    collection_name TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    object_uri TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._ensure_candidate_columns(conn)

    def create_job(self, job: JobRecord) -> JobRecord:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (job_id, kind, query, source_url, status, created_at, updated_at, request_payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.kind.value,
                    job.query,
                    job.source_url,
                    job.status,
                    job.created_at.isoformat(),
                    job.updated_at.isoformat(),
                    stable_json(job.request_payload),
                ),
            )
        return job

    def upsert_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        current = self.get_candidate(candidate.candidate_id)
        if current:
            if current.image_url == candidate.image_url:
                merged = self._merge_candidates(current, candidate)
                return self.update_candidate(merged)
            url_owner = self.get_candidate_by_url(candidate.image_url)
            if url_owner and url_owner.candidate_id != candidate.candidate_id:
                merged = self._merge_candidates(url_owner, candidate)
                return self.update_candidate(merged)
            return self.update_candidate(candidate)

        existing = self.get_candidate_by_url(candidate.image_url)
        if existing:
            merged = self._merge_candidates(existing, candidate)
            return self.update_candidate(merged)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO candidates (
                    candidate_id, job_id, query_text, image_url, thumbnail_url, source_page_url, source_domain,
                    mime_type, width, height, byte_size, page_title, alt_text, nearby_text, crawl_timestamp,
                    fetch_status, storage_key, local_cache_path, page_screenshot_path, content_hash, perceptual_hash,
                    last_error, quality_score, gallery_id, tile_index, google_result_url, pinchtab_instance_id, provenance_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._candidate_values(candidate),
            )
        return candidate

    def link_candidate_to_job(self, job_id: str, candidate_id: str, rank: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO job_candidates (job_id, candidate_id, rank, discovered_at)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, candidate_id, rank, datetime.utcnow().isoformat()),
            )

    def list_job_candidates(self, job_id: str, limit: int) -> list[CandidateRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.*
                FROM candidates c
                INNER JOIN job_candidates jc ON jc.candidate_id = c.candidate_id
                WHERE jc.job_id = ?
                ORDER BY jc.rank ASC
                LIMIT ?
                """,
                (job_id, limit),
            ).fetchall()
        return [self._row_to_candidate(row) for row in rows]

    def get_candidate(self, candidate_id: str) -> CandidateRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM candidates WHERE candidate_id = ?", (candidate_id,)).fetchone()
        return self._row_to_candidate(row) if row else None

    def get_candidate_by_url(self, image_url: str) -> CandidateRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM candidates WHERE image_url = ?", (image_url,)).fetchone()
        return self._row_to_candidate(row) if row else None

    def update_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE candidates SET
                    job_id = ?, query_text = ?, image_url = ?, thumbnail_url = ?, source_page_url = ?, source_domain = ?,
                    mime_type = ?, width = ?, height = ?, byte_size = ?, page_title = ?, alt_text = ?,
                    nearby_text = ?, crawl_timestamp = ?, fetch_status = ?, storage_key = ?, local_cache_path = ?,
                    page_screenshot_path = ?, content_hash = ?, perceptual_hash = ?, last_error = ?, quality_score = ?,
                    gallery_id = ?, tile_index = ?, google_result_url = ?, pinchtab_instance_id = ?, provenance_json = ?
                WHERE candidate_id = ?
                """,
                (
                    candidate.job_id,
                    candidate.query_text,
                    candidate.image_url,
                    candidate.thumbnail_url,
                    candidate.source_page_url,
                    candidate.source_domain,
                    candidate.mime_type,
                    candidate.width,
                    candidate.height,
                    candidate.byte_size,
                    candidate.page_title,
                    candidate.alt_text,
                    candidate.nearby_text,
                    candidate.crawl_timestamp.isoformat(),
                    candidate.fetch_status.value,
                    candidate.storage_key,
                    candidate.local_cache_path,
                    candidate.page_screenshot_path,
                    candidate.content_hash,
                    candidate.perceptual_hash,
                    candidate.last_error,
                    candidate.quality_score,
                    candidate.gallery_id,
                    candidate.tile_index,
                    candidate.google_result_url,
                    candidate.pinchtab_instance_id,
                    candidate.provenance.model_dump_json(),
                    candidate.candidate_id,
                ),
            )
        return candidate

    def save_asset(self, asset: AssetRecord) -> AssetRecord:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO assets (asset_id, candidate_id, collection_name, tags_json, object_key, object_uri, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset.asset_id,
                    asset.candidate_id,
                    asset.collection,
                    stable_json({"tags": asset.tags}),
                    asset.object_key,
                    asset.object_uri,
                    asset.created_at.isoformat(),
                ),
            )
        return asset

    def _row_to_candidate(self, row: sqlite3.Row) -> CandidateRecord:
        return CandidateRecord.model_validate(
            {
                "candidate_id": row["candidate_id"],
                "job_id": row["job_id"],
                "query_text": row["query_text"],
                "image_url": row["image_url"],
                "thumbnail_url": row["thumbnail_url"],
                "source_page_url": row["source_page_url"],
                "source_domain": row["source_domain"],
                "mime_type": row["mime_type"],
                "width": row["width"],
                "height": row["height"],
                "byte_size": row["byte_size"],
                "page_title": row["page_title"],
                "alt_text": row["alt_text"],
                "nearby_text": row["nearby_text"],
                "crawl_timestamp": row["crawl_timestamp"],
                "fetch_status": row["fetch_status"],
                "storage_key": row["storage_key"],
                "local_cache_path": row["local_cache_path"],
                "page_screenshot_path": row["page_screenshot_path"],
                "content_hash": row["content_hash"],
                "perceptual_hash": row["perceptual_hash"],
                "last_error": row["last_error"],
                "quality_score": row["quality_score"],
                "gallery_id": row["gallery_id"],
                "tile_index": row["tile_index"],
                "google_result_url": row["google_result_url"],
                "pinchtab_instance_id": row["pinchtab_instance_id"],
                "provenance": parse_json(row["provenance_json"], {}),
            }
        )

    @staticmethod
    def _candidate_values(candidate: CandidateRecord) -> tuple:
        return (
            candidate.candidate_id,
            candidate.job_id,
            candidate.query_text,
            candidate.image_url,
            candidate.thumbnail_url,
            candidate.source_page_url,
            candidate.source_domain,
            candidate.mime_type,
            candidate.width,
            candidate.height,
            candidate.byte_size,
            candidate.page_title,
            candidate.alt_text,
            candidate.nearby_text,
            candidate.crawl_timestamp.isoformat(),
            candidate.fetch_status.value,
            candidate.storage_key,
            candidate.local_cache_path,
            candidate.page_screenshot_path,
            candidate.content_hash,
            candidate.perceptual_hash,
            candidate.last_error,
            candidate.quality_score,
            candidate.gallery_id,
            candidate.tile_index,
            candidate.google_result_url,
            candidate.pinchtab_instance_id,
            candidate.provenance.model_dump_json(),
        )

    @staticmethod
    def _merge_candidates(existing: CandidateRecord, incoming: CandidateRecord) -> CandidateRecord:
        return existing.model_copy(
            update={
                "job_id": incoming.job_id or existing.job_id,
                "query_text": incoming.query_text or existing.query_text,
                "thumbnail_url": incoming.thumbnail_url or existing.thumbnail_url,
                "source_page_url": incoming.source_page_url or existing.source_page_url,
                "source_domain": incoming.source_domain or existing.source_domain,
                "mime_type": incoming.mime_type or existing.mime_type,
                "width": incoming.width or existing.width,
                "height": incoming.height or existing.height,
                "byte_size": incoming.byte_size or existing.byte_size,
                "page_title": incoming.page_title or existing.page_title,
                "alt_text": incoming.alt_text or existing.alt_text,
                "nearby_text": incoming.nearby_text or existing.nearby_text,
                "gallery_id": incoming.gallery_id or existing.gallery_id,
                "tile_index": incoming.tile_index if incoming.tile_index is not None else existing.tile_index,
                "google_result_url": incoming.google_result_url or existing.google_result_url,
                "pinchtab_instance_id": incoming.pinchtab_instance_id or existing.pinchtab_instance_id,
                "crawl_timestamp": incoming.crawl_timestamp,
                "page_screenshot_path": incoming.page_screenshot_path or existing.page_screenshot_path,
                "quality_score": incoming.quality_score if incoming.quality_score is not None else existing.quality_score,
                "provenance": existing.provenance.model_copy(
                    update={
                        "crawl_timestamp": incoming.crawl_timestamp,
                        "steps": [*existing.provenance.steps, *incoming.provenance.steps],
                    }
                ),
            }
        )

    @staticmethod
    def _ensure_candidate_columns(conn: sqlite3.Connection) -> None:
        current = {row["name"] for row in conn.execute("PRAGMA table_info(candidates)").fetchall()}
        for column, kind in CANDIDATE_EXTRA_COLUMNS.items():
            if column not in current:
                conn.execute(f"ALTER TABLE candidates ADD COLUMN {column} {kind}")


class PsycopgRepository:
    def __init__(self, database_url: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("Install the optional postgres dependency to use PostgreSQL") from exc

        self.database_url = database_url
        self.psycopg = psycopg
        self.dict_row = dict_row

    @contextmanager
    def _connect(self):
        connection = self.psycopg.connect(self.database_url, row_factory=self.dict_row)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        kind TEXT NOT NULL,
                        query TEXT,
                        source_url TEXT,
                        status TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        request_payload TEXT NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS candidates (
                        candidate_id TEXT PRIMARY KEY,
                        job_id TEXT,
                        query_text TEXT,
                        image_url TEXT NOT NULL UNIQUE,
                        thumbnail_url TEXT,
                        source_page_url TEXT,
                        source_domain TEXT,
                        mime_type TEXT,
                        width INTEGER,
                        height INTEGER,
                        byte_size INTEGER,
                        page_title TEXT,
                        alt_text TEXT,
                        nearby_text TEXT,
                        crawl_timestamp TEXT NOT NULL,
                        fetch_status TEXT NOT NULL,
                        storage_key TEXT,
                        local_cache_path TEXT,
                        page_screenshot_path TEXT,
                        content_hash TEXT,
                        perceptual_hash TEXT,
                        last_error TEXT,
                        quality_score REAL,
                        gallery_id TEXT,
                        tile_index INTEGER,
                        google_result_url TEXT,
                        pinchtab_instance_id TEXT,
                        provenance_json TEXT NOT NULL
                    )
                    """
                )
                for column, kind in CANDIDATE_EXTRA_COLUMNS.items():
                    cursor.execute(f"ALTER TABLE candidates ADD COLUMN IF NOT EXISTS {column} {kind}")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS job_candidates (
                        job_id TEXT NOT NULL,
                        candidate_id TEXT NOT NULL,
                        rank INTEGER NOT NULL,
                        discovered_at TEXT NOT NULL,
                        PRIMARY KEY (job_id, candidate_id)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS assets (
                        asset_id TEXT PRIMARY KEY,
                        candidate_id TEXT NOT NULL,
                        collection_name TEXT NOT NULL,
                        tags_json TEXT NOT NULL,
                        object_key TEXT NOT NULL,
                        object_uri TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )

    def create_job(self, job: JobRecord) -> JobRecord:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO jobs (job_id, kind, query, source_url, status, created_at, updated_at, request_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job.job_id,
                        job.kind.value,
                        job.query,
                        job.source_url,
                        job.status,
                        job.created_at.isoformat(),
                        job.updated_at.isoformat(),
                        stable_json(job.request_payload),
                    ),
                )
        return job

    def upsert_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        current = self.get_candidate(candidate.candidate_id)
        if current:
            if current.image_url == candidate.image_url:
                merged = SQLiteRepository._merge_candidates(current, candidate)
                return self.update_candidate(merged)
            url_owner = self.get_candidate_by_url(candidate.image_url)
            if url_owner and url_owner.candidate_id != candidate.candidate_id:
                merged = SQLiteRepository._merge_candidates(url_owner, candidate)
                return self.update_candidate(merged)
            return self.update_candidate(candidate)

        existing = self.get_candidate_by_url(candidate.image_url)
        if existing:
            merged = SQLiteRepository._merge_candidates(existing, candidate)
            return self.update_candidate(merged)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO candidates (
                        candidate_id, job_id, query_text, image_url, thumbnail_url, source_page_url, source_domain,
                        mime_type, width, height, byte_size, page_title, alt_text, nearby_text, crawl_timestamp,
                        fetch_status, storage_key, local_cache_path, page_screenshot_path, content_hash, perceptual_hash,
                        last_error, quality_score, gallery_id, tile_index, google_result_url, pinchtab_instance_id, provenance_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    SQLiteRepository._candidate_values(candidate),
                )
        return candidate

    def link_candidate_to_job(self, job_id: str, candidate_id: str, rank: int) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO job_candidates (job_id, candidate_id, rank, discovered_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (job_id, candidate_id) DO NOTHING
                    """,
                    (job_id, candidate_id, rank, datetime.utcnow().isoformat()),
                )

    def list_job_candidates(self, job_id: str, limit: int) -> list[CandidateRecord]:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT c.*
                    FROM candidates c
                    INNER JOIN job_candidates jc ON jc.candidate_id = c.candidate_id
                    WHERE jc.job_id = %s
                    ORDER BY jc.rank ASC
                    LIMIT %s
                    """,
                    (job_id, limit),
                )
                rows = cursor.fetchall()
        return [self._row_to_candidate(row) for row in rows]

    def get_candidate(self, candidate_id: str) -> CandidateRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM candidates WHERE candidate_id = %s", (candidate_id,))
                row = cursor.fetchone()
        return self._row_to_candidate(row) if row else None

    def get_candidate_by_url(self, image_url: str) -> CandidateRecord | None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM candidates WHERE image_url = %s", (image_url,))
                row = cursor.fetchone()
        return self._row_to_candidate(row) if row else None

    def update_candidate(self, candidate: CandidateRecord) -> CandidateRecord:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE candidates SET
                        job_id = %s, query_text = %s, image_url = %s, thumbnail_url = %s, source_page_url = %s, source_domain = %s,
                        mime_type = %s, width = %s, height = %s, byte_size = %s, page_title = %s, alt_text = %s,
                        nearby_text = %s, crawl_timestamp = %s, fetch_status = %s, storage_key = %s, local_cache_path = %s,
                        page_screenshot_path = %s, content_hash = %s, perceptual_hash = %s, last_error = %s,
                        quality_score = %s, gallery_id = %s, tile_index = %s, google_result_url = %s, pinchtab_instance_id = %s, provenance_json = %s
                    WHERE candidate_id = %s
                    """,
                    (
                        candidate.job_id,
                        candidate.query_text,
                        candidate.image_url,
                        candidate.thumbnail_url,
                        candidate.source_page_url,
                        candidate.source_domain,
                        candidate.mime_type,
                        candidate.width,
                        candidate.height,
                        candidate.byte_size,
                        candidate.page_title,
                        candidate.alt_text,
                        candidate.nearby_text,
                        candidate.crawl_timestamp.isoformat(),
                        candidate.fetch_status.value,
                        candidate.storage_key,
                        candidate.local_cache_path,
                        candidate.page_screenshot_path,
                        candidate.content_hash,
                        candidate.perceptual_hash,
                        candidate.last_error,
                        candidate.quality_score,
                        candidate.gallery_id,
                        candidate.tile_index,
                        candidate.google_result_url,
                        candidate.pinchtab_instance_id,
                        candidate.provenance.model_dump_json(),
                        candidate.candidate_id,
                    ),
                )
        return candidate

    def save_asset(self, asset: AssetRecord) -> AssetRecord:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO assets (asset_id, candidate_id, collection_name, tags_json, object_key, object_uri, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        asset.asset_id,
                        asset.candidate_id,
                        asset.collection,
                        stable_json({"tags": asset.tags}),
                        asset.object_key,
                        asset.object_uri,
                        asset.created_at.isoformat(),
                    ),
                )
        return asset

    @staticmethod
    def _row_to_candidate(row) -> CandidateRecord:
        return CandidateRecord.model_validate(
            {
                "candidate_id": row["candidate_id"],
                "job_id": row["job_id"],
                "query_text": row["query_text"],
                "image_url": row["image_url"],
                "thumbnail_url": row["thumbnail_url"],
                "source_page_url": row["source_page_url"],
                "source_domain": row["source_domain"],
                "mime_type": row["mime_type"],
                "width": row["width"],
                "height": row["height"],
                "byte_size": row["byte_size"],
                "page_title": row["page_title"],
                "alt_text": row["alt_text"],
                "nearby_text": row["nearby_text"],
                "crawl_timestamp": row["crawl_timestamp"],
                "fetch_status": row["fetch_status"],
                "storage_key": row["storage_key"],
                "local_cache_path": row["local_cache_path"],
                "page_screenshot_path": row["page_screenshot_path"],
                "content_hash": row["content_hash"],
                "perceptual_hash": row["perceptual_hash"],
                "last_error": row["last_error"],
                "quality_score": row["quality_score"],
                "gallery_id": row["gallery_id"],
                "tile_index": row["tile_index"],
                "google_result_url": row["google_result_url"],
                "pinchtab_instance_id": row["pinchtab_instance_id"],
                "provenance": parse_json(row["provenance_json"], {}),
            }
        )


def build_repository(database_url: str) -> Repository:
    if database_url.startswith("sqlite:///"):
        repository = SQLiteRepository(database_url)
        repository.init_schema()
        return repository
    if database_url.startswith("postgresql://") or database_url.startswith("postgres://"):
        repository = PsycopgRepository(database_url)
        repository.init_schema()
        return repository
    raise RuntimeError(f"Unsupported database URL: {database_url}")
