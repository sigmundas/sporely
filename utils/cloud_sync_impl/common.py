"""Pure cloud-sync helpers shared by every owner.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction (Stage S6 added the slug, select-column,
batch-size and size-format helpers).
"""

from __future__ import annotations

import re

from utils.r2_storage import normalize_media_key


def _normalize_cloud_media_key(value: str | None) -> str:
    """Normalize cloud media references to the stored relative key form."""
    return normalize_media_key(value)


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_slug(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _join_select_columns(*columns: str) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for column in columns:
        text = str(column or '').strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ','.join(ordered)


# Batch size for PostgREST `id=in.(...)` fetches (measurements + image metadata).
# IDs are UUIDs (~36 chars), so 100 IDs keep the `in.(...)` clause under ~3.7KB,
# well below any reasonable proxy URL limit (nginx default request line is 8KB),
# while halving the request count vs the previous size of 50 (e.g. 866 image IDs
# go from 18 requests to 9). Kept conservative on purpose; do not raise without
# re-checking the proxy URL limit for the longest realistic ID list.
_CLOUD_SYNC_IN_BATCH_SIZE = 100


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024: return f"{size_bytes} B"
    if size_bytes < 1024 * 1024: return f"{size_bytes/1024:.1f} KB"
    return f"{size_bytes/(1024*1024):.1f} MB"
