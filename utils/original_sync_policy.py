"""Policy helpers for optional full-resolution original sync.

This is the Stage I policy slice only. It classifies which local image rows
could participate in full-resolution original sync later, and it decides when a
future original download would be safe to treat as a recovery/cache action.
It does not upload or download anything by itself.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

from database.models import SettingsDB

SYNC_FULL_RESOLUTION_ORIGINALS_SETTING = "sync_full_resolution_originals"

_ALLOWED_SOURCE_ROLES = {
    "local_canonical",
    "converted_local",
}

_DISALLOWED_SOURCE_ROLES = {
    "cloud_derivative",
    "cloud_recovery_cache",
    "generated_artifact",
}

_ALLOWED_FILE_PURPOSES = {
    "field",
    "microscope",
}

_FUTURE_ORIGINAL_REMOTE_PATH_KEYS = (
    "original_storage_path",
    "original_image_key",
)


def _normalize_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = _normalize_slug(value)
    return text in {"1", "true", "yes", "on", "enabled"}


def _is_readable_file(path_value: Any) -> bool:
    text = str(path_value or "").strip()
    if not text:
        return False
    path = Path(text)
    try:
        return path.exists() and path.is_file() and os.access(path, os.R_OK)
    except Exception:
        return False


def _first_non_empty_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def is_full_resolution_original_sync_enabled() -> bool:
    """Return the opt-in gate for full-resolution original sync.

    The setting defaults to off when absent, which keeps current sync behavior
    unchanged until the later upload/download slice is wired in.
    """
    return _coerce_bool(SettingsDB.get_setting(SYNC_FULL_RESOLUTION_ORIGINALS_SETTING, False))


def is_full_original_sync_candidate(
    image_row: Mapping[str, Any] | None,
    *,
    include_converted_local: bool = False,
    include_original_path: bool = False,
) -> bool:
    """Return True when a local image row is eligible for full-original sync."""
    row = dict(image_row or {})
    source_role = _normalize_slug(row.get("source_role"))
    file_purpose = _normalize_slug(row.get("file_purpose"))

    if not source_role:
        return False
    if source_role in _DISALLOWED_SOURCE_ROLES:
        return False
    if source_role not in _ALLOWED_SOURCE_ROLES:
        return False
    if file_purpose not in _ALLOWED_FILE_PURPOSES:
        return False

    working_path = row.get("filepath")
    original_path = row.get("original_filepath")

    if source_role == "local_canonical":
        return _is_readable_file(working_path)

    if source_role == "converted_local":
        if include_original_path and _is_readable_file(original_path):
            return True
        if include_converted_local and _is_readable_file(working_path):
            return True
        return False

    return False


def should_download_full_original(
    remote_meta: Mapping[str, Any] | None,
    local_row: Mapping[str, Any] | None,
) -> bool:
    """Return True when a future full-resolution original can be recovered safely.

    The current cloud schema does not yet carry a dedicated original-object key,
    so this stays False for today’s cloud derivative rows. When a future original
    key is present, the helper still refuses to overwrite a readable local
    canonical or original path.
    """
    if not is_full_resolution_original_sync_enabled():
        return False

    remote = dict(remote_meta or {})
    original_storage_path = _first_non_empty_text(
        *(remote.get(key) for key in _FUTURE_ORIGINAL_REMOTE_PATH_KEYS)
    )
    if not original_storage_path:
        return False

    local = dict(local_row or {})
    local_source_role = _normalize_slug(local.get("source_role"))

    if local_source_role == "local_canonical":
        if _is_readable_file(local.get("filepath")) or _is_readable_file(local.get("original_filepath")):
            return False
    elif local_source_role == "converted_local":
        if _is_readable_file(local.get("original_filepath")):
            return False
    elif local_source_role in _DISALLOWED_SOURCE_ROLES:
        # Cache and derivative rows are not canonical originals, but they also
        # do not block a future recovery-style download to a separate cache path.
        pass

    return True


__all__ = [
    "SYNC_FULL_RESOLUTION_ORIGINALS_SETTING",
    "is_full_original_sync_candidate",
    "is_full_resolution_original_sync_enabled",
    "should_download_full_original",
]
