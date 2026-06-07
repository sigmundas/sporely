"""Pure RAW image detection helpers."""
from __future__ import annotations

from pathlib import Path


SUPPORTED_RAW_SUFFIXES = frozenset(
    {
        ".3fr",
        ".arw",
        ".cr2",
        ".cr3",
        ".dcr",
        ".dng",
        ".erf",
        ".fff",
        ".iiq",
        ".kdc",
        ".mef",
        ".mos",
        ".nef",
        ".nrw",
        ".orf",
        ".pef",
        ".ptx",
        ".raf",
        ".rw2",
        ".srf",
        ".sr2",
        ".x3f",
    }
)

_RAW_MIME_TYPE = "image/x-raw"


def is_raw_image_path(path: str | Path) -> bool:
    """Return True when *path* looks like a RAW file by suffix."""
    try:
        suffix = Path(path).suffix.lower()
    except Exception:
        return False
    return suffix in SUPPORTED_RAW_SUFFIXES


def raw_mime_type_for_path(path: str | Path) -> str:
    """Return the MIME label used for RAW provenance metadata."""
    return _RAW_MIME_TYPE if is_raw_image_path(path) else "application/octet-stream"


__all__ = [
    "SUPPORTED_RAW_SUFFIXES",
    "is_raw_image_path",
    "raw_mime_type_for_path",
]
