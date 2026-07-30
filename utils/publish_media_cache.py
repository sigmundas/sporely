"""Persistent, disposable cache primitives for external publish media."""
from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Callable

from app_identity import app_cache_dir


PUBLISH_MEDIA_CACHE_SCHEMA_VERSION = "1"
_ASSET_DIRECTORIES = {
    "mosaic": "mosaics",
    "annotated": "annotated",
    "variant": "variants",
}


def _canonical_value(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_canonical_value(item) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    if isinstance(value, bytes):
        return {
            "bytes_sha256": hashlib.sha256(value).hexdigest(),
            "bytes_length": len(value),
        }
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Publish-media signatures do not accept non-finite floats")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"Unsupported publish-media signature value: {type(value).__name__}")


def publish_media_signature(
    asset_kind: str,
    renderer_version: str,
    dependencies: dict,
) -> str:
    """Return a stable SHA-256 digest for one derived publish asset."""
    if asset_kind not in _ASSET_DIRECTORIES:
        raise ValueError(f"Unsupported publish-media asset kind: {asset_kind}")
    payload = {
        "cache_schema_version": PUBLISH_MEDIA_CACHE_SCHEMA_VERSION,
        "asset_kind": asset_kind,
        "renderer_version": str(renderer_version),
        "dependencies": _canonical_value(dict(dependencies or {})),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_cached_image(path: Path) -> bool:
    """Return whether *path* is a complete image readable by Pillow."""
    try:
        if not path.is_file() or path.stat().st_size <= 0:
            return False
        from PIL import Image

        with Image.open(path) as image:
            image.verify()
        return True
    except Exception:
        return False


class PublishMediaCache:
    """Signature-addressed cache whose contents are never canonical media."""

    def __init__(self, root: Path | None = None):
        self.root = Path(root) if root is not None else app_cache_dir() / "publish-media"

    @staticmethod
    def _extension(extension: str) -> str:
        cleaned = str(extension or "").strip().lower().lstrip(".")
        if not cleaned or not cleaned.isalnum():
            raise ValueError("Cache extension must contain only letters and digits")
        return cleaned

    @staticmethod
    def _digest(signature: str) -> str:
        digest = str(signature or "").strip().lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("Cache signature must be a SHA-256 hex digest")
        return digest

    def path_for(self, asset_kind: str, signature: str, extension: str) -> Path:
        directory = _ASSET_DIRECTORIES.get(asset_kind)
        if directory is None:
            raise ValueError(f"Unsupported publish-media asset kind: {asset_kind}")
        return self.root / directory / f"{self._digest(signature)}.{self._extension(extension)}"

    def lookup(
        self,
        asset_kind: str,
        signature: str,
        extension: str,
        *,
        validator: Callable[[Path], bool] | None = None,
    ) -> Path | None:
        path = self.path_for(asset_kind, signature, extension)
        check = validator or (lambda candidate: candidate.is_file() and candidate.stat().st_size > 0)
        try:
            if check(path):
                return path
        except Exception:
            pass
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass
        return None

    def store_file(
        self,
        asset_kind: str,
        signature: str,
        extension: str,
        source_path: str | Path,
        *,
        validator: Callable[[Path], bool] | None = None,
    ) -> Path:
        """Atomically copy a completed render into the persistent cache."""
        source = Path(source_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        final_path = self.path_for(asset_kind, signature, extension)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=final_path.parent,
                prefix=f".{final_path.stem}.tmp-",
                suffix=final_path.suffix,
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                with source.open("rb") as source_handle:
                    shutil.copyfileobj(source_handle, handle)
                handle.flush()
                os.fsync(handle.fileno())
            if validator is not None and not validator(temp_path):
                raise ValueError(f"Rendered cache asset is invalid: {source}")
            os.replace(temp_path, final_path)
            temp_path = None
            return final_path
        finally:
            if temp_path is not None and temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass

    def cleanup(
        self,
        *,
        max_age_days: int = 90,
        temporary_max_age_hours: int = 24,
    ) -> dict[str, int]:
        """Remove stale cache entries and abandoned atomic-write files."""
        removed_assets = 0
        removed_temporary = 0
        now = time.time()
        asset_cutoff = now - max(0, int(max_age_days)) * 86400
        temp_cutoff = now - max(0, int(temporary_max_age_hours)) * 3600
        if not self.root.exists():
            return {"assets": 0, "temporary": 0}
        for path in self.root.rglob("*"):
            if not path.is_file():
                continue
            try:
                modified = path.stat().st_mtime
                if ".tmp-" in path.name:
                    if modified < temp_cutoff:
                        path.unlink()
                        removed_temporary += 1
                elif modified < asset_cutoff:
                    path.unlink()
                    removed_assets += 1
            except Exception:
                continue
        return {"assets": removed_assets, "temporary": removed_temporary}
