from __future__ import annotations

from pathlib import Path
from typing import Protocol

from .config import Settings
from .utils import sha256_bytes, suffix_for_mime


class StoredObject:
    def __init__(self, key: str, uri: str, local_path: str | None = None) -> None:
        self.key = key
        self.uri = uri
        self.local_path = local_path


class ObjectStore(Protocol):
    def put_bytes(self, data: bytes, key_hint: str, mime_type: str | None = None) -> StoredObject:
        ...


class FileCache:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, data: bytes, key_hint: str, mime_type: str | None = None) -> str:
        digest = sha256_bytes(data)
        suffix = suffix_for_mime(mime_type, key_hint)
        relative = Path(digest[:2]) / f"{digest}{suffix}"
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path.resolve())


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, data: bytes, key_hint: str, mime_type: str | None = None) -> StoredObject:
        suffix = suffix_for_mime(mime_type, key_hint)
        key = f"{key_hint}{suffix}" if not key_hint.endswith(suffix) else key_hint
        target = self.root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return StoredObject(key=key, uri=f"file://{target.resolve()}", local_path=str(target.resolve()))


class S3ObjectStore:
    def __init__(self, settings: Settings) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("boto3 is required for the S3 object store backend") from exc

        if not settings.s3_bucket:
            raise RuntimeError("AIS_S3_BUCKET must be configured for the S3 backend")

        self.bucket = settings.s3_bucket
        self.prefix = settings.s3_prefix.strip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
        )

    def put_bytes(self, data: bytes, key_hint: str, mime_type: str | None = None) -> StoredObject:
        suffix = suffix_for_mime(mime_type, key_hint)
        key_base = f"{self.prefix}/{key_hint}".strip("/")
        key = f"{key_base}{suffix}" if not key_base.endswith(suffix) else key_base
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data}
        if mime_type:
            kwargs["ContentType"] = mime_type
        self.client.put_object(**kwargs)
        return StoredObject(key=key, uri=f"s3://{self.bucket}/{key}")


def build_object_store(settings: Settings) -> ObjectStore:
    if settings.object_store_backend.lower() == "s3":
        return S3ObjectStore(settings)
    return LocalObjectStore(settings.local_object_store_dir)
