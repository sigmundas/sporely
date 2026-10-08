"""Pure cloud-sync helpers shared by every owner.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations


from utils.r2_storage import normalize_media_key


def _normalize_cloud_media_key(value: str | None) -> str:
    """Normalize cloud media references to the stored relative key form."""
    return normalize_media_key(value)


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
