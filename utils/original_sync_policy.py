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

FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES = 250 * 1024 * 1024

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


def _readable_path_value(path_value: Any) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    try:
        if path.exists() and path.is_file() and os.access(path, os.R_OK):
            return path
    except Exception:
        return None
    return None


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


def set_full_resolution_original_sync_enabled(enabled: bool) -> None:
    """Persist the opt-in gate for full-resolution original sync."""
    SettingsDB.set_setting(SYNC_FULL_RESOLUTION_ORIGINALS_SETTING, bool(enabled))


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


def resolve_full_original_upload_source(image_row: Mapping[str, Any] | None) -> dict[str, str] | None:
    """Return the readable source path to upload for one eligible image row.

    The helper keeps the source choice explicit so the desktop can report which
    path was used without guessing. For converted local images, an original
    HEIC/HEIF lineage file wins when present and readable; otherwise the
    converted working copy is used when readable.
    """
    row = dict(image_row or {})
    source_role = _normalize_slug(row.get("source_role"))
    file_purpose = _normalize_slug(row.get("file_purpose"))

    if not is_full_original_sync_candidate(
        row,
        include_converted_local=True,
        include_original_path=True,
    ):
        return None

    if source_role == "local_canonical":
        source_path = _readable_path_value(row.get("filepath"))
        if source_path is None:
            return None
        return {
            "source_path": str(source_path),
            "source_kind": "filepath",
            "source_role": source_role,
            "file_purpose": file_purpose,
        }

    if source_role == "converted_local":
        original_path = _readable_path_value(row.get("original_filepath"))
        if original_path is not None:
            return {
                "source_path": str(original_path),
                "source_kind": "original_filepath",
                "source_role": source_role,
                "file_purpose": file_purpose,
            }

        working_path = _readable_path_value(row.get("filepath"))
        if working_path is not None:
            return {
                "source_path": str(working_path),
                "source_kind": "filepath",
                "source_role": source_role,
                "file_purpose": file_purpose,
            }

    return None


def is_full_resolution_original_upload_too_large(path_value: Any) -> bool:
    path = _readable_path_value(path_value)
    if path is None:
        return False
    try:
        return int(path.stat().st_size) > FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES
    except Exception:
        return False


def should_download_full_original(
    remote_meta: Mapping[str, Any] | None,
    local_row: Mapping[str, Any] | None,
) -> bool:
    """Return True when a future full-resolution original can be recovered safely.

    The cloud contract may now carry optional original-object metadata such as
    original_storage_path, but upload/download remains deferred. This helper
    only returns True when the opt-in is enabled, a remote original key is
    present, and the local row does not already hold a readable canonical or
    original path that would be overwritten.
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
    "FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES",
    "SYNC_FULL_RESOLUTION_ORIGINALS_SETTING",
    "is_full_original_sync_candidate",
    "is_full_resolution_original_sync_enabled",
    "is_full_resolution_original_upload_too_large",
    "resolve_full_original_upload_source",
    "set_full_resolution_original_sync_enabled",
    "should_download_full_original",
]
