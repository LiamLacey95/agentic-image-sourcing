from __future__ import annotations

import hashlib
import json
import mimetypes
from pathlib import Path
from urllib.parse import urlparse


def domain_for_url(url: str | None) -> str | None:
    if not url:
        return None
    return urlparse(url).netloc.lower() or None


def normalize_url(url: str) -> str:
    return url.strip()


def stable_json(data: dict) -> str:
    return json.dumps(data, sort_keys=True, ensure_ascii=True)


def parse_json(value: str | None, default):
    if not value:
        return default
    return json.loads(value)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def suffix_for_mime(mime_type: str | None, url: str | None = None) -> str:
    if mime_type:
        guessed = mimetypes.guess_extension(mime_type.split(";")[0].strip())
        if guessed:
            return guessed
    if url:
        path = urlparse(url).path
        suffix = Path(path).suffix
        if suffix:
            return suffix
    return ".bin"
