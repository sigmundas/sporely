"""Bidirectional sync between local SQLite and Sporely cloud (Supabase).

Desktop → Cloud  push observations and selected images not yet synced
Cloud → Desktop  pull observations created on mobile/web (no desktop_id)

One-time Supabase SQL to run in the SQL editor for optimal upsert performance:
    ALTER TABLE public.observations
        ADD CONSTRAINT observations_desktop_id_user_unique UNIQUE (desktop_id, user_id);
    ALTER TABLE public.observation_images
        ADD CONSTRAINT observation_images_desktop_id_user_unique UNIQUE (desktop_id, user_id);
"""
from __future__ import annotations

import base64
import logging
import math
import os
import hashlib
import io
import json
import mimetypes
import re
import random
import sqlite3
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import quote, urlparse

import requests
from PIL import Image, ImageOps, features
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
import time

from app_identity import app_data_dir, runtime_profile_scope, using_isolated_profile
from database.schema import get_connection, get_app_settings, get_images_dir, update_app_settings
from database.models import (
    ObservationDB,
    ImageDB,
    SettingsDB,
    MeasurementDB,
    CalibrationDB,
    mark_observation_sync_dirty,
    update_observation_sync_state,
    _upsert_image_tombstone,
    get_image_tombstones_by_deleted_cloud_id,
    get_image_tombstones_by_local_image_id,
    list_pending_image_tombstones,
    mark_image_tombstone_synced,
)
from utils.heic_converter import guess_local_image_mime_type
from utils.cloud_media_policy import (
    IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE,
    WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE,
    build_cloud_upload_policy,
    build_full_image_webp_quality_attempts,
    normalize_cloud_plan_profile,
    scale_dimensions_to_max_pixels,
)
from utils.original_sync_policy import (
    FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES,
    is_full_resolution_original_sync_enabled,
    is_full_resolution_original_upload_too_large,
    resolve_full_original_upload_source,
    should_download_full_original,
)
from utils.publish_targets import normalize_publish_target
from utils.r2_storage import (
    CloudflareR2Client,
    CloudflareMediaWorkerClient,
    R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE,
    direct_r2_runtime_available,
    media_worker_base_url,
    media_variant_key,
    normalize_media_key,
)
from utils.thumbnail_generator import generate_all_sizes

logger = logging.getLogger(__name__)

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'
SUPABASE_KEY = 'sb_publishable_nZrERVFN3WR4Aqn2yggc7Q_siAG1TCV'
_SUPABASE_AUTH_TIMEOUT = 30
_SUPABASE_REST_TIMEOUT = 60
_SUPABASE_PROFILE_UPLOAD_TIMEOUT = 60
_SUPABASE_REQUEST_MAX_ATTEMPTS = 4
_SUPABASE_REQUEST_BACKOFF_BASE_SECONDS = 0.5
_SUPABASE_REQUEST_BACKOFF_MAX_SECONDS = 8.0
_SUPABASE_TRANSIENT_STATUS_CODES = {500, 502, 503, 504}
_SUPABASE_TRANSIENT_ERROR_HINTS = (
    'bad gateway',
    'connection aborted',
    'connection refused',
    'connection reset',
    'could not connect to server',
    'gateway timeout',
    'postgrest unavailable',
    'schema cache',
    'service unavailable',
    'temporarily unavailable',
    'timed out',
    'timeout',
)
_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE = (
    'Supabase/cloud sync is temporarily unavailable; local data was not overwritten.'
)
_CLOUD_KEYRING_SERVICE = 'Sporely.Cloud'
_CLOUD_LEGACY_KEYRING_SERVICE = 'MycoLog.Cloud'
_profile_suffix = runtime_profile_scope()
_CLOUD_KEYRING_ACCOUNT = f'password:{_profile_suffix}' if _profile_suffix else 'password'

# Cloud contract audit:
# - Synced now: `is_draft`, `location_precision`, `ai_selected_*`, image `measure_color`
#   and `crop_mode`, and spore measurement `gallery_rotation`.
# - Future work: image `scale_bar_*`, spore measurement `notes`, `image_key`,
#   `thumb_key`, and cloud upload metadata/derived keys remain intentionally
#   out of the desktop contract for now.
# - Intentionally blocked / future work for the desktop schema: observation
#   `captured_at`, `gps_altitude`, `gps_accuracy`, and the stored
#   `observation_identifications` table.
# - Avoid user-facing conflicts for harmless reduced cloud media copies.
# Observation columns we push to cloud (excludes local-only fields)
_OBS_PUSH_COLS = [
    'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude',
    'location_public',
    'is_draft', 'location_precision',
    'ai_selected_service', 'ai_selected_taxon_id',
    'ai_selected_scientific_name', 'ai_selected_probability',
    'ai_selected_at',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'spore_data_visibility',
]
# Never push: private_comment, ai_state_json, folder_path, cloud_id, sync_status, synced_at


def _normalize_slug(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _normalize_sharing_scope(value: str | None, fallback: str = 'private') -> str:
    raw = str(value or '').strip().lower()
    if raw == 'draft':
        return 'private'
    if raw in {'private', 'friends', 'public'}:
        return raw
    fallback_raw = str(fallback or 'private').strip().lower()
    if fallback_raw == 'draft':
        return 'private'
    return fallback_raw if fallback_raw in {'private', 'friends', 'public'} else 'private'


def _sharing_scope_to_cloud_visibility(value: str | None, fallback: str = 'private') -> str:
    """Map local desktop sharing scope to the Phase 7 cloud visibility value."""
    normalized = _normalize_sharing_scope(value, fallback=fallback)
    return normalized


def _cloud_visibility_to_sharing_scope(value: str | None, fallback: str = 'private') -> str:
    """Map Phase 7 cloud visibility back to the local desktop sharing scope."""
    return _normalize_sharing_scope(value, fallback=fallback)


_OBSERVATION_BOOL_FIELDS = {
    'location_public',
    'uncertain',
    'unspontaneous',
    'interesting_comment',
    'is_draft',
}
_OBSERVATION_INT_FIELDS = {
    'artsdata_id',
    'artportalen_id',
    'inaturalist_id',
    'mushroomobserver_id',
    'determination_method',
}
_OBSERVATION_FLOAT_FIELDS = {
    'gps_latitude',
    'gps_longitude',
    'ai_selected_probability',
    'auto_threshold',
}
_OBSERVATION_FLOAT_ABS_TOL = 1e-9
_OBSERVATION_FLOAT_REL_TOL = 1e-9


def _normalize_observation_bool_value(value, *, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, float):
        return value != 0.0
    text = str(value or '').strip().lower()
    if not text:
        return default
    if text in {'true', '1', 'yes', 'on'}:
        return True
    if text in {'false', '0', 'no', 'off'}:
        return False
    if text in {'none', 'null'}:
        return default
    return bool(value)


def _normalize_observation_int_value(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _normalize_observation_float_value(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_observation_json_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        normalized = _normalize_snapshot_value(value)
        if normalized in ({}, [], ''):
            return None
        return normalized
    if isinstance(value, (bool, int, float)):
        return value
    text = str(value or '').strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return text
    if data in ({}, [], ''):
        return None
    return _normalize_snapshot_value(data)


def _encode_postgrest_filter_value(value: str | None) -> str:
    """Encode filter values for PostgREST query strings.

    Timestamps may contain '+' in timezone offsets, which must be percent-encoded
    inside a URL query or they can be parsed incorrectly.
    """
    return quote(str(value or '').strip(), safe='')


def _normalize_cloud_media_key(value: str | None) -> str:
    """Normalize cloud media references to the stored relative key form."""
    return normalize_media_key(value)


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


def _direct_r2_unavailable_warning(context: str | None = None) -> str:
    detail = str(context or "").strip()
    if detail:
        return f"{R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE}; {detail}"
    return R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE


def _is_direct_r2_unavailable_error(exc: Exception | str) -> bool:
    return R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE.lower() in str(exc or "").lower()


def _image_storage_timestamp_ms(image_row: dict | None) -> int:
    row = dict(image_row or {})
    for key in ('created_at', 'captured_at', 'synced_at'):
        parsed = _parse_sync_timestamp(row.get(key))
        if parsed is not None:
            return int(parsed.timestamp() * 1000)
        raw = str(row.get(key) or '').strip()
        if raw.isdigit():
            try:
                return int(raw)
            except Exception:
                continue
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _build_worker_storage_path(
    user_id: str,
    obs_cloud_id: str,
    image_row: dict | None,
    upload_path: str,
) -> str:
    path = Path(str(upload_path or '').strip())
    suffix = path.suffix.lower() or _detected_image_extension(path)
    extension = suffix if suffix.startswith('.') else f'.{suffix or "jpg"}'
    sort_order = _safe_int((image_row or {}).get('sort_order'))
    if sort_order < 0:
        sort_order = 0
    timestamp_ms = _image_storage_timestamp_ms(image_row)
    return _normalize_cloud_media_key(
        f'{str(user_id or "").strip()}/{str(obs_cloud_id or "").strip()}/{sort_order}_{timestamp_ms}{extension}'
    )


def _sanitize_original_storage_filename(source_path: str | Path) -> str:
    path = Path(str(source_path or '').strip())
    raw_name = path.name or 'original'
    stem = re.sub(r'[^A-Za-z0-9._-]+', '_', path.stem).strip('._') or 'original'
    suffix = path.suffix.lower()
    if not re.fullmatch(r'\.[A-Za-z0-9]{1,10}', suffix or ''):
        suffix = ''
    sanitized = f'{stem}{suffix}'
    if sanitized:
        return sanitized
    cleaned_name = re.sub(r'[^A-Za-z0-9._-]+', '_', raw_name).strip('._')
    return cleaned_name or 'original'


def _client_uses_default_r2_loader(client: object | None) -> bool:
    if not isinstance(client, SporelyCloudClient):
        return False
    try:
        return client._using_default_r2_loader()
    except Exception:
        return False

_CALIBRATION_SYNC_COLS = [
    'calibration_uuid',
    'objective_key',
    'calibration_date',
    'calibration_image_date',
    'microns_per_pixel',
    'microns_per_pixel_std',
    'confidence_interval_low',
    'confidence_interval_high',
    'num_measurements',
    'measurements_json',
    'camera',
    'megapixels',
    'target_sampling_pct',
    'resample_scale_factor',
    'calibration_image_width',
    'calibration_image_height',
    'notes',
    'is_active',
]

# Float fields can drift by tiny amounts across database versions or JSON
# serialization paths. Treat those as equivalent during conflict detection.
_CALIBRATION_FLOAT_FIELDS = {
    'microns_per_pixel',
    'microns_per_pixel_std',
    'confidence_interval_low',
    'confidence_interval_high',
    'megapixels',
    'target_sampling_pct',
    'resample_scale_factor',
}
_CALIBRATION_FLOAT_ABS_TOL = 1e-9
_CALIBRATION_FLOAT_REL_TOL = 1e-9

_CALIBRATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'created_at',
    'image_storage_path',
    *_CALIBRATION_SYNC_COLS,
)


def _normalize_calibration_uuid(value) -> str | None:
    if isinstance(value, uuid.UUID):
        return str(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return str(uuid.UUID(text))
    except (TypeError, ValueError, AttributeError):
        return None


def _normalize_calibration_text(value) -> str | None:
    text = str(value or '').strip()
    return text or None


def _normalize_calibration_date(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    text = str(value or '').strip()
    if not text:
        return None
    if len(text) >= 10 and re.match(r'^\d{4}-\d{2}-\d{2}', text):
        return text[:10]
    for fmt in (
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
    ):
        try:
            return datetime.strptime(text, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).strftime('%Y-%m-%d')
    except Exception:
        return text[:10] if len(text) >= 10 else text or None


def _normalize_calibration_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_calibration_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _normalize_calibration_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int):
        return value != 0
    if isinstance(value, float):
        return value != 0.0
    text = str(value).strip().lower()
    if not text:
        return False
    if text in {'true', '1', 'yes', 'on'}:
        return True
    if text in {'false', '0', 'no', 'off'}:
        return False
    return bool(value)


def _normalize_calibration_measurements_json(value):
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        normalized = _normalize_snapshot_value(value)
        if normalized in ({}, [], ''):
            return None
        return normalized
    if isinstance(value, (bool, int, float)):
        return value
    text = str(value or '').strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return text
    if data in ({}, [], ''):
        return None
    return _normalize_snapshot_value(data)


def _serialize_calibration_measurements_json(value) -> str | None:
    normalized = _normalize_calibration_measurements_json(value)
    if normalized is None:
        return None
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True)


def _calibration_field_values_match(field: str, local_value, remote_value) -> bool:
    if field in _CALIBRATION_FLOAT_FIELDS:
        local_float = _normalize_calibration_float(local_value)
        remote_float = _normalize_calibration_float(remote_value)
        if local_float is None or remote_float is None:
            return local_float == remote_float
        return math.isclose(
            local_float,
            remote_float,
            rel_tol=_CALIBRATION_FLOAT_REL_TOL,
            abs_tol=_CALIBRATION_FLOAT_ABS_TOL,
        )
    return local_value == remote_value


def _calibration_field_changes(local_row: dict | None, remote_row: dict | None) -> dict[str, tuple[object, object]]:
    local_payload = _calibration_sync_payload(local_row)
    remote_payload = _calibration_sync_payload(remote_row)
    changes: dict[str, tuple[object, object]] = {}
    for field in _CALIBRATION_SYNC_COLS:
        local_value = local_payload.get(field)
        remote_value = remote_payload.get(field)
        if not _calibration_field_values_match(field, local_value, remote_value):
            changes[field] = (local_value, remote_value)
    return changes


def _calibration_sync_payload(row: dict | None) -> dict:
    record = dict(row or {})
    return {
        'calibration_uuid': _normalize_calibration_uuid(record.get('calibration_uuid')),
        'objective_key': _normalize_calibration_text(record.get('objective_key')),
        'calibration_date': _normalize_calibration_date(record.get('calibration_date')),
        'calibration_image_date': _normalize_calibration_date(record.get('calibration_image_date')),
        'microns_per_pixel': _normalize_calibration_float(record.get('microns_per_pixel')),
        'microns_per_pixel_std': _normalize_calibration_float(record.get('microns_per_pixel_std')),
        'confidence_interval_low': _normalize_calibration_float(record.get('confidence_interval_low')),
        'confidence_interval_high': _normalize_calibration_float(record.get('confidence_interval_high')),
        'num_measurements': _normalize_calibration_int(record.get('num_measurements')),
        'measurements_json': _normalize_calibration_measurements_json(record.get('measurements_json')),
        'camera': _normalize_calibration_text(record.get('camera')),
        'megapixels': _normalize_calibration_float(record.get('megapixels')),
        'target_sampling_pct': _normalize_calibration_float(record.get('target_sampling_pct')),
        'resample_scale_factor': _normalize_calibration_float(record.get('resample_scale_factor')),
        'calibration_image_width': _normalize_calibration_int(record.get('calibration_image_width')),
        'calibration_image_height': _normalize_calibration_int(record.get('calibration_image_height')),
        'notes': _normalize_calibration_text(record.get('notes')),
        'is_active': _normalize_calibration_bool(record.get('is_active')),
    }


def _calibration_insert_kwargs(row: dict | None) -> dict:
    payload = _calibration_sync_payload(row)
    return {
        'objective_key': payload['objective_key'],
        'calibration_date': payload['calibration_date'],
        'calibration_image_date': payload['calibration_image_date'],
        'microns_per_pixel': payload['microns_per_pixel'],
        'microns_per_pixel_std': payload['microns_per_pixel_std'],
        'confidence_interval_low': payload['confidence_interval_low'],
        'confidence_interval_high': payload['confidence_interval_high'],
        'num_measurements': payload['num_measurements'],
        'measurements_json': _serialize_calibration_measurements_json(payload['measurements_json']),
        'camera': payload['camera'],
        'megapixels': payload['megapixels'],
        'target_sampling_pct': payload['target_sampling_pct'],
        'resample_scale_factor': payload['resample_scale_factor'],
        'calibration_image_width': payload['calibration_image_width'],
        'calibration_image_height': payload['calibration_image_height'],
        'notes': payload['notes'],
        'set_active': bool(payload['is_active']),
        'calibration_uuid': payload['calibration_uuid'],
    }


def _calibration_payloads_match(local_row: dict | None, remote_row: dict | None) -> bool:
    return not _calibration_field_changes(local_row, remote_row)


def _calibration_diff_fields(local_row: dict | None, remote_row: dict | None) -> list[str]:
    return list(_calibration_field_changes(local_row, remote_row).keys())


def _calibration_local_wins_patch_payload(local_row: dict | None, remote_row: dict | None) -> dict:
    local_payload = _calibration_sync_payload(local_row)
    return {
        field: local_payload.get(field)
        for field in _calibration_diff_fields(local_row, remote_row)
    }


def _calibration_display_name(row: dict | None) -> str:
    record = dict(row or {})
    objective = _normalize_calibration_text(record.get('objective_key')) or '?'
    date_text = _normalize_calibration_date(record.get('calibration_date')) or 'unknown date'
    return f'{objective} • {date_text}'


def _remap_known_local_calibration_path(path: Path) -> Path:
    if path.exists():
        return path
    try:
        from app_identity import app_data_dir, legacy_app_data_dir

        legacy_root = legacy_app_data_dir().resolve()
        current_root = app_data_dir().resolve()
        rel = path.resolve(strict=False).relative_to(legacy_root)
    except Exception:
        return path
    return current_root / rel


def _is_readable_local_file(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        with path.open('rb') as handle:
            handle.read(1)
        return True
    except Exception:
        return False


def _resolve_local_calibration_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
        remapped = _remap_known_local_calibration_path(raw_path)
        if remapped != raw_path:
            candidates.append(remapped)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_readable_local_file(candidate):
            return candidate
    return None


def _resolve_existing_local_calibration_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
        remapped = _remap_known_local_calibration_path(raw_path)
        if remapped != raw_path:
            candidates.append(remapped)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


def _select_representative_calibration_image_path(calibration: dict | None) -> Path | None:
    record = dict(calibration or {})
    local_image = _resolve_local_calibration_asset_path(record.get('image_filepath'))
    if local_image is not None:
        return local_image

    measurements_json = record.get('measurements_json')
    if isinstance(measurements_json, (dict, list, tuple)):
        loaded = measurements_json
    elif measurements_json:
        try:
            loaded = json.loads(str(measurements_json))
        except Exception:
            loaded = None
    else:
        loaded = None

    if not isinstance(loaded, dict):
        return None

    for entry in loaded.get('images', []):
        if not isinstance(entry, dict):
            continue
        path = _resolve_local_calibration_asset_path(entry.get('path'))
        if path is not None:
            return path
    return None


def _calibration_reference_save_format() -> tuple[str, str, dict, str]:
    if features.check('webp'):
        return 'WEBP', 'image/webp', {'quality': 82, 'method': 4}, '.webp'
    return 'JPEG', 'image/jpeg', {'quality': 88, 'optimize': True}, '.jpg'


def _calibration_reference_image_bytes(path: Path) -> tuple[bytes, str, str]:
    with Image.open(path) as image:
        image = ImageOps.exif_transpose(image)
        if image.mode in {'RGBA', 'LA'} or 'transparency' in image.info:
            rgba = image.convert('RGBA')
            background = Image.new('RGB', rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel('A'))
            image = background
        elif image.mode != 'RGB':
            image = image.convert('RGB')
        image.thumbnail((_CALIBRATION_REFERENCE_MAX_EDGE, _CALIBRATION_REFERENCE_MAX_EDGE), Image.Resampling.LANCZOS)

        format_name, mime_type, save_options, extension = _calibration_reference_save_format()
        buffer = io.BytesIO()
        image.save(buffer, format=format_name, **save_options)
        return buffer.getvalue(), mime_type, extension


def _calibration_reference_storage_key(user_id: str, calibration_uuid: str, extension: str) -> str:
    key = f"{str(user_id).strip()}/{str(calibration_uuid).strip()}/reference{extension}"
    return _normalize_cloud_media_key(key)


def _sanitize_calibration_cache_component(value: str | None, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(text).name).strip("._")
    return cleaned or fallback


def _normalize_calibration_cache_extension(value: str | None, fallback: str = ".jpg") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    if not text.startswith("."):
        text = f".{text}"
    cleaned = re.sub(r"[^a-z0-9.]+", "", text)
    if not cleaned:
        return fallback
    if not cleaned.startswith("."):
        cleaned = f".{cleaned.lstrip('.')}"
    return cleaned if cleaned != "." else fallback


def _calibration_recovery_cache_root() -> Path:
    return app_data_dir() / "cloud_cache" / "calibrations"


def _calibration_recovery_cache_path(
    calibration_uuid: str | None,
    cloud_key: str | None = None,
    desired_extension: str | None = None,
) -> Path:
    uuid_value = _normalize_calibration_uuid(calibration_uuid)
    folder_name = uuid_value or _sanitize_calibration_cache_component(calibration_uuid, "unknown_calibration")
    normalized_key = _normalize_cloud_media_key(cloud_key)
    inferred_extension = Path(normalized_key).suffix if normalized_key else ""
    extension = _normalize_calibration_cache_extension(desired_extension or inferred_extension, ".jpg")
    return _calibration_recovery_cache_root() / folder_name / f"reference{extension}"


def _calibration_reference_recovery_state(calibration: dict | None) -> dict[str, object]:
    record = dict(calibration or {})
    image_filepath = str(record.get("image_filepath") or "").strip() or None
    image_storage_path = _normalize_cloud_media_key(record.get("image_storage_path")) or None
    calibration_uuid = _normalize_calibration_uuid(record.get("calibration_uuid"))
    local_original_path = _resolve_existing_local_calibration_asset_path(image_filepath)
    local_original_exists = local_original_path is not None
    return {
        "calibration_uuid": calibration_uuid,
        "image_filepath": image_filepath,
        "image_storage_path": image_storage_path,
        "local_original_exists": local_original_exists,
        "local_original_missing": bool(image_filepath and not local_original_exists),
        "local_original_path": local_original_path,
        "recovery_available": bool(calibration_uuid and image_storage_path and not local_original_exists),
    }


def download_calibration_reference_to_cache(
    client: "SporelyCloudClient",
    calibration: dict | None,
) -> dict[str, object]:
    """Download a cloud calibration reference image into the local cache.

    This helper is intentionally soft-failing and does not mutate the input
    calibration row or any database records.
    """
    state = _calibration_reference_recovery_state(calibration)
    result = dict(state)
    result.update(
        {
            "status": "skipped",
            "warning": None,
            "cache_path": None,
            "downloaded": False,
        }
    )

    calibration_uuid = state.get("calibration_uuid")
    image_storage_path = state.get("image_storage_path")
    if not calibration_uuid:
        result["status"] = "skipped_invalid_calibration_uuid"
        result["warning"] = "calibration ?: skipped recovery because calibration_uuid is missing or invalid"
        return result

    if state.get("local_original_exists"):
        result["status"] = "skipped_local_original_exists"
        return result

    if not image_storage_path:
        result["status"] = "unavailable_missing_storage_path"
        result["warning"] = (
            f"calibration {calibration_uuid}: skipped recovery because image_storage_path is missing"
        )
        return result

    cache_root = _calibration_recovery_cache_root() / str(calibration_uuid)
    cache_root.mkdir(parents=True, exist_ok=True)
    initial_extension = _normalize_calibration_cache_extension(Path(str(image_storage_path)).suffix or ".jpg")
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=cache_root,
            prefix="reference_",
            suffix=initial_extension,
            delete=False,
        ) as tmp:
            temp_path = Path(tmp.name)

        downloaded_path = Path(client.download_image_file(str(image_storage_path), temp_path))
        if not downloaded_path.exists():
            raise RuntimeError("downloaded calibration reference image was not written")

        detected_extension = _detected_image_extension(downloaded_path)
        final_path = _calibration_recovery_cache_path(
            calibration_uuid,
            str(image_storage_path),
            detected_extension,
        )
        if downloaded_path != final_path:
            downloaded_path.replace(final_path)
            downloaded_path = final_path

        result["status"] = "downloaded_to_cache"
        result["downloaded"] = True
        result["cache_path"] = downloaded_path
        return result
    except Exception as exc:
        detail = str(exc or "").strip() or exc.__class__.__name__
        result["status"] = "download_failed"
        result["warning"] = f"calibration {calibration_uuid}: skipped recovery download ({detail})"
        return result
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


def _sanitize_original_recovery_cache_component(value: str | None, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(text).name).strip("._")
    return cleaned or fallback


def _normalize_original_recovery_cache_extension(value: str | None, fallback: str = ".jpg") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    if not text.startswith("."):
        text = f".{text}"
    cleaned = re.sub(r"[^a-z0-9.]+", "", text)
    if not cleaned or cleaned == ".":
        return fallback
    if not cleaned.startswith("."):
        cleaned = f".{cleaned.lstrip('.')}"
    return cleaned if cleaned != "." else fallback


def _original_recovery_cache_root() -> Path:
    return app_data_dir() / "cloud_cache" / "originals"


def _original_recovery_cache_path(
    user_id: str | None,
    observation_cloud_id: str | None,
    image_cloud_id: str | None,
    original_storage_path: str | None,
    *,
    original_filename: str | None = None,
    local_image_id: int | None = None,
) -> Path:
    storage_name = Path(str(original_storage_path or "").strip()).name
    filename_source = str(original_filename or "").strip() or storage_name
    fallback_name = f"original_{_safe_int(local_image_id) or 'image'}"
    base_name = Path(filename_source or fallback_name).name or fallback_name
    stem = _sanitize_original_recovery_cache_component(Path(base_name).stem or base_name, "original")
    suffix = _normalize_original_recovery_cache_extension(Path(base_name).suffix or None, ".jpg")
    filename = f"{stem}{suffix or '.jpg'}"
    user_component = _sanitize_original_recovery_cache_component(user_id, "unknown_user")
    observation_component = _sanitize_original_recovery_cache_component(
        observation_cloud_id,
        "unknown_observation",
    )
    image_component = _sanitize_original_recovery_cache_component(
        image_cloud_id or f"desktop_{_safe_int(local_image_id) or 'unknown'}",
        "unknown_image",
    )
    return _original_recovery_cache_root() / user_component / observation_component / image_component / filename


def _original_recovery_sidecar_path(cache_path: Path) -> Path:
    return cache_path.with_name(f"{cache_path.name}.json")


def _write_original_recovery_sidecar(
    cache_path: Path,
    *,
    local_image: dict | None,
    local_observation: dict | None,
    remote_image: dict | None,
    original_storage_path: str,
    bytes_downloaded: int,
) -> Path:
    payload = {
        "asset_type": "full_original_recovery_cache",
        "source_role": "cloud_recovery_cache",
        "file_purpose": "cache",
        "recovered_from_source_role": str((local_image or {}).get("source_role") or "").strip() or None,
        "recovered_from_file_purpose": str((local_image or {}).get("file_purpose") or "").strip() or None,
        "local_image_id": _safe_int((local_image or {}).get("id")) or None,
        "local_observation_id": _safe_int((local_observation or {}).get("id")) or None,
        "cloud_image_id": str((remote_image or {}).get("id") or "").strip() or None,
        "cloud_observation_id": str((local_observation or {}).get("cloud_id") or "").strip() or None,
        "original_storage_path": str(original_storage_path or "").strip() or None,
        "downloaded_path": str(cache_path),
        "bytes": max(0, int(bytes_downloaded)),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    sidecar_path = _original_recovery_sidecar_path(cache_path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return sidecar_path


def _new_original_upload_summary(enabled: bool) -> dict[str, int | bool]:
    return {
        "enabled": bool(enabled),
        "uploaded": 0,
        "skipped_disabled": 0,
        "skipped_ineligible": 0,
        "skipped_too_large": 0,
        "failed_uploads": 0,
    }


def _format_original_count(count: int, label: str) -> str:
    total = max(0, int(count))
    return f"{total} {label}"


def format_original_upload_summary(original_summary: dict | None) -> str | None:
    summary = dict(original_summary or {})
    if not bool(summary.get("enabled")):
        return None

    uploaded = max(0, int(summary.get("uploaded") or 0))
    skipped_disabled = max(0, int(summary.get("skipped_disabled") or 0))
    skipped_ineligible = max(0, int(summary.get("skipped_ineligible") or 0))
    skipped_too_large = max(0, int(summary.get("skipped_too_large") or 0))
    failed_uploads = max(0, int(summary.get("failed_uploads") or 0))

    skipped_total = skipped_disabled + skipped_ineligible + skipped_too_large
    if not any((uploaded, skipped_total, failed_uploads)):
        return None

    parts: list[str] = []
    if uploaded:
        parts.append(_format_original_count(uploaded, "uploaded"))
    if skipped_total:
        parts.append(_format_original_count(skipped_total, "skipped"))
    if failed_uploads:
        parts.append(_format_original_count(failed_uploads, "failed"))
    return f"Original uploads: {', '.join(parts)}."


def format_original_recovery_summary(recovery_result: dict | None) -> str | None:
    result = dict(recovery_result or {})
    status = str(result.get("status") or "").strip()
    if not status or status == "skipped_disabled":
        return None
    if status == "downloaded_to_cache":
        return "Original recovery: 1 downloaded."
    if status == "download_failed":
        return "Original recovery: 1 failed."
    if status.startswith("skipped_"):
        return "Original recovery: 1 skipped."
    return None


def _local_image_existing_original_path(local_image: dict | None) -> Path | None:
    row = dict(local_image or {})
    source_role = _normalize_slug(row.get("source_role"))
    if source_role == "local_canonical":
        readable_path = _resolve_existing_local_image_asset_path(row.get("filepath"))
        if readable_path is not None:
            return readable_path
        readable_original = _resolve_existing_local_image_asset_path(row.get("original_filepath"))
        if readable_original is not None:
            return readable_original
    elif source_role == "converted_local":
        readable_original = _resolve_existing_local_image_asset_path(row.get("original_filepath"))
        if readable_original is not None:
            return readable_original
    return None


def _remote_image_matches_local_image(remote_image: dict | None, local_image: dict | None) -> bool:
    remote = dict(remote_image or {})
    local = dict(local_image or {})
    remote_cloud_id = str(remote.get("id") or "").strip()
    local_cloud_id = str(local.get("cloud_id") or "").strip()
    if remote_cloud_id and local_cloud_id and remote_cloud_id == local_cloud_id:
        return True
    remote_desktop_id = _safe_int(remote.get("desktop_id"))
    local_image_id = _safe_int(local.get("id"))
    return remote_desktop_id > 0 and local_image_id > 0 and remote_desktop_id == local_image_id


def _load_remote_original_recovery_image_rows(
    client: "SporelyCloudClient",
    observation_cloud_id: str,
    *,
    prefer_snapshot: bool = True,
) -> list[dict]:
    cloud_value = str(observation_cloud_id or "").strip()
    if not cloud_value:
        return []

    rows: list[dict] = []
    if prefer_snapshot:
        snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
        rows = [dict(row or {}) for row in (snapshot.get("images") or [])]
        if rows:
            return rows

    try:
        rows = [dict(row or {}) for row in (client.pull_image_metadata(cloud_value, include_deleted_for_sync=True) or [])]
    except Exception:
        rows = []
    return rows


def _find_remote_original_for_local_image(
    client: "SporelyCloudClient",
    local_image: dict,
    observation_cloud_id: str,
) -> dict | None:
    snapshot_rows = _load_remote_original_recovery_image_rows(client, observation_cloud_id, prefer_snapshot=True)
    snapshot_match: dict | None = None
    for remote_image in snapshot_rows:
        if _remote_image_matches_local_image(remote_image, local_image):
            snapshot_match = remote_image
            if _normalize_cloud_media_key(
                remote_image.get("original_storage_path") or remote_image.get("original_image_key")
            ):
                return remote_image
            break
    live_rows = _load_remote_original_recovery_image_rows(client, observation_cloud_id, prefer_snapshot=False)
    for remote_image in live_rows:
        if _remote_image_matches_local_image(remote_image, local_image):
            return remote_image
    return snapshot_match


def recover_full_original_for_image(
    client: "SporelyCloudClient",
    local_image_id: int | str,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    """Recover a cloud original into the local cache without touching the canonical file."""
    local_id = _safe_int(local_image_id)
    result: dict[str, object] = {
        "status": "skipped",
        "skipped_reason": None,
        "downloaded_path": None,
        "bytes": 0,
        "used_original_storage_path": None,
        "warnings": [],
        "errors": [],
        "cache_sidecar_path": None,
        "local_image_id": local_id,
        "cloud_image_id": None,
        "cloud_observation_id": None,
    }

    profiler = _cloud_sync_current_profiler()
    progress_state = {"done": 0, "total": 1}

    def _add_warning(message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        result["warnings"].append(text)

    def _add_error(message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        result["errors"].append(text)

    if local_id <= 0:
        result["status"] = "skipped_invalid_local_image_id"
        result["skipped_reason"] = "invalid_local_image_id"
        return result

    local_image = ImageDB.get_image(local_id)
    if not local_image:
        result["status"] = "skipped_local_image_missing"
        result["skipped_reason"] = "local_image_missing"
        return result

    local_observation_id = _safe_int(local_image.get("observation_id"))
    local_observation = ObservationDB.get_observation(local_observation_id) if local_observation_id > 0 else None
    cloud_observation_id = str((local_observation or {}).get("cloud_id") or "").strip()
    result["cloud_observation_id"] = cloud_observation_id or None

    if not cloud_observation_id:
        result["status"] = "skipped_missing_cloud_link"
        result["skipped_reason"] = "missing_cloud_link"
        return result

    if not is_full_resolution_original_sync_enabled():
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_disabled()
            except Exception:
                pass
        result["status"] = "skipped_disabled"
        result["skipped_reason"] = "disabled"
        return result

    canonical_original_path = _local_image_existing_original_path(local_image)
    if canonical_original_path is not None:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_existing_local_original()
            except Exception:
                pass
        result["status"] = "skipped_existing_local_original"
        result["skipped_reason"] = "existing_local_original"
        return result

    _emit_progress(
        progress_cb,
        f"Recovering full-resolution original for image {local_id}…",
        progress_state,
    )

    remote_image = _find_remote_original_for_local_image(client, local_image, cloud_observation_id)
    if not remote_image:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_missing_remote_image"
        result["skipped_reason"] = "missing_remote_image"
        return result

    remote_original_storage_path = _normalize_cloud_media_key(
        remote_image.get("original_storage_path") or remote_image.get("original_image_key")
    )
    result["cloud_image_id"] = str(remote_image.get("id") or "").strip() or None
    result["used_original_storage_path"] = remote_original_storage_path or None

    if not remote_original_storage_path:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_missing_key"
        result["skipped_reason"] = "missing_original_storage_path"
        return result

    policy_allows = should_download_full_original(remote_image, local_image)
    if not policy_allows:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_policy_rejected"
        result["skipped_reason"] = "policy_rejected"
        return result

    cache_path = _original_recovery_cache_path(
        client.user_id,
        cloud_observation_id,
        result["cloud_image_id"] or f"desktop_{local_id}",
        remote_original_storage_path,
        original_filename=str(remote_image.get("original_filename") or "").strip() or None,
        local_image_id=local_id,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    existing_cache_path = _resolve_existing_local_image_asset_path(str(cache_path))
    if existing_cache_path is not None:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_existing_cache()
            except Exception:
                pass
        try:
            bytes_downloaded = int(existing_cache_path.stat().st_size)
        except Exception:
            bytes_downloaded = 0
        try:
            sidecar_path = _write_original_recovery_sidecar(
                existing_cache_path,
                local_image=local_image,
                local_observation=local_observation,
                remote_image=remote_image,
                original_storage_path=remote_original_storage_path,
                bytes_downloaded=bytes_downloaded,
            )
        except Exception as exc:
            _add_warning(f"obs {local_id}: could not refresh original recovery sidecar ({exc})")
            sidecar_path = None
        result.update(
            {
                "status": "skipped_existing_cache",
                "skipped_reason": "existing_cache",
                "downloaded_path": existing_cache_path,
                "bytes": bytes_downloaded,
                "cache_sidecar_path": sidecar_path,
            }
        )
        _emit_progress(
            progress_cb,
            f"Recovered original already cached for image {local_id}",
            progress_state,
        )
        return result

    temp_path: Path | None = None
    downloaded_path: Path | None = None
    try:
        cache_suffix = cache_path.suffix or ".bin"
        with tempfile.NamedTemporaryFile(
            dir=cache_path.parent,
            prefix=f"{cache_path.stem}_",
            suffix=cache_suffix,
            delete=False,
        ) as tmp:
            temp_path = Path(tmp.name)

        downloaded_temp = Path(client.download_image_file(remote_original_storage_path, temp_path))
        if not downloaded_temp.exists() or not downloaded_temp.is_file() or not os.access(downloaded_temp, os.R_OK):
            raise CloudSyncError("downloaded original image was not written")

        downloaded_path = cache_path
        if downloaded_temp != cache_path:
            if cache_path.exists():
                cache_path.unlink()
            downloaded_temp.replace(cache_path)
            downloaded_path = cache_path

        if not downloaded_path.exists() or not downloaded_path.is_file() or not os.access(downloaded_path, os.R_OK):
            raise CloudSyncError("downloaded original cache file is not readable")

        bytes_downloaded = 0
        try:
            bytes_downloaded = int(downloaded_path.stat().st_size)
        except Exception:
            bytes_downloaded = 0

        try:
            sidecar_path = _write_original_recovery_sidecar(
                downloaded_path,
                local_image=local_image,
                local_observation=local_observation,
                remote_image=remote_image,
                original_storage_path=remote_original_storage_path,
                bytes_downloaded=bytes_downloaded,
            )
        except Exception as exc:
            _add_warning(f"obs {local_id}: recovered original but could not write sidecar ({exc})")
            sidecar_path = None

        if profiler is not None:
            try:
                profiler.record_original_download_success(bytes_downloaded)
            except Exception:
                pass

        result.update(
            {
                "status": "downloaded_to_cache",
                "skipped_reason": None,
                "downloaded_path": downloaded_path,
                "bytes": bytes_downloaded,
                "cache_sidecar_path": sidecar_path,
            }
        )
        _emit_progress(
            progress_cb,
            f"Recovered full-resolution original for image {local_id}",
            progress_state,
        )
        return result
    except Exception as exc:
        if profiler is not None:
            try:
                profiler.record_original_download_failed()
            except Exception:
                pass
        detail = str(exc or "").strip() or exc.__class__.__name__
        _add_error(f"obs {local_id}: original recovery failed ({detail})")
        result["status"] = "download_failed"
        result["skipped_reason"] = None
        result["downloaded_path"] = None
        result["bytes"] = 0
        return result
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


_IMG_PUSH_COLS = [
    'sort_order', 'image_type', 'micro_category', 'objective_name',
    'calibration_uuid',
    'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'contrast', 'measure_color',
    'crop_mode', 'notes',
    'gps_source', 'storage_path',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h', 'ai_crop_is_custom',
]
_IMG_UPLOAD_META_COLS = [
    'upload_mode',
    'source_width',
    'source_height',
    'stored_width',
    'stored_height',
    'stored_bytes',
]

_MEAS_PUSH_COLS = [
    'length_um', 'width_um', 'measurement_type',
    'gallery_rotation',
    'p1_x', 'p1_y', 'p2_x', 'p2_y',
    'p3_x', 'p3_y', 'p4_x', 'p4_y',
    'measured_at',
]

_SETTING_INCLUDE_ANNOTATIONS = "artsobs_publish_include_annotations"
_SETTING_SHOW_SCALE_BAR = "artsobs_publish_show_scale_bar"
_SETTING_INCLUDE_MEASURE_PLOTS = "artsobs_publish_include_measure_plots"
_SETTING_INCLUDE_THUMBNAIL_GALLERY = "artsobs_publish_include_thumbnail_gallery"
_SETTING_INCLUDE_PLATE = "artsobs_publish_include_plate"
_SETTING_INCLUDE_COPYRIGHT = "artsobs_publish_include_copyright"
_SETTING_IMAGE_LICENSE = "artsobs_publish_image_license"
_SETTING_PROFILE_NAME = "profile_name"
_SETTING_PROFILE_EMAIL = "profile_email"
_SETTING_CLOUD_MEDIA_SIGNATURE = "sporely_cloud_media_signature_v1"
_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX = "sporely_cloud_snapshot_obs_"
_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX = "sporely_cloud_image_file_sig_"
_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX = "sporely_cloud_local_media_sig_obs_"
_SETTING_LINKED_CLOUD_USER_ID = "linked_cloud_user_id"
_CLOUD_LOCAL_MEDIA_RENDER_VERSION = "2"
_REMOTE_SYNC_TIMESTAMP_GRACE_SECONDS = 5.0
_CLOUD_THUMB_MAX_EDGE = 400
_CALIBRATION_REFERENCE_MAX_EDGE = 2048
_LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS = (
    'ai_crop_x1',
    'ai_crop_y1',
    'ai_crop_x2',
    'ai_crop_y2',
    'ai_crop_source_w',
    'ai_crop_source_h',
    'ai_crop_is_custom',
)

_LOCAL_MEDIA_PREP_RENDER_AFFECTING_IMAGE_FIELDS = frozenset({
    'crop_mode',
    'ai_crop_x1',
    'ai_crop_y1',
    'ai_crop_x2',
    'ai_crop_y2',
    'ai_crop_source_w',
    'ai_crop_source_h',
    'ai_crop_is_custom',
})

_LOCAL_MEDIA_PREP_RENDER_AFFECTING_TOP_LEVEL_FIELDS = frozenset({
    'render_version',
    'cloud_media_signature',
    'cloud_image_size_mode',
    'excluded_image_ids_raw',
})

_SNAPSHOT_OBS_FIELDS = [
    'id', 'desktop_id', 'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude', 'location_public',
    'is_draft', 'location_precision',
    'ai_selected_service', 'ai_selected_taxon_id',
    'ai_selected_scientific_name', 'ai_selected_probability',
    'ai_selected_at',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'visibility',
    'spore_data_visibility',
]

_SNAPSHOT_IMG_FIELDS = [
    'id', 'desktop_id', 'sort_order', 'image_type', 'micro_category',
    'calibration_uuid',
    'objective_name', 'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'contrast', 'measure_color',
    'crop_mode', 'notes',
    'gps_source', 'storage_path', 'original_filename',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h', 'ai_crop_is_custom',
    'upload_mode', 'source_width', 'source_height',
    'stored_width', 'stored_height', 'stored_bytes',
]

# Future original-object metadata that we preserve in snapshots when it is
# already present on the cloud row, but do not yet use for sync decisions.
_SNAPSHOT_IMG_PASSIVE_FIELDS = [
    'original_storage_path',
]

_CONFLICT_COMPARE_FIELDS = [
    'date',
    'genus',
    'species',
    'common_name',
    'species_guess',
    'location',
    'gps_latitude',
    'gps_longitude',
    'habitat',
    'notes',
    'open_comment',
    'publish_target',
    'visibility',
    'location_public',
    'is_draft',
    'location_precision',
    'spore_statistics',
]

_CONFLICT_FIELD_LABELS = {
    'date': 'Date',
    'genus': 'Genus',
    'species': 'Species',
    'common_name': 'Common name',
    'species_guess': 'Species guess',
    'location': 'Location',
    'gps_latitude': 'Latitude',
    'gps_longitude': 'Longitude',
    'habitat': 'Habitat',
    'notes': 'Notes',
    'open_comment': 'Public comment',
    'publish_target': 'Publishing target',
    'visibility': 'Visibility',
    'location_public': 'Public GPS',
    'is_draft': 'Draft state',
    'location_precision': 'Location precision',
    'spore_statistics': 'Spore statistics',
}

ProgressCallback = Callable[[str, int, int], None]
PreparedImagesCallback = Callable[[dict, ProgressCallback | None], tuple[list[dict], object | None, list[str]]]

_CLOUD_SYNC_PROFILE_ENV = 'SPORELY_CLOUD_SYNC_PROFILE'
_CLOUD_SYNC_DEBUG_ENV = 'SPORELY_DEBUG_CLOUD_SYNC'
_CLOUD_SYNC_PROFILE_CONTEXT: ContextVar['CloudSyncProfiler | None'] = ContextVar(
    'cloud_sync_profiler',
    default=None,
)
_CLOUD_SYNC_SUMMARY_CONTEXT: ContextVar[dict[str, int] | None] = ContextVar(
    'cloud_sync_summary',
    default=None,
)

# A single sync sub-step taking longer than this is logged so a silent UI pause
# can be traced to the exact calibration / step responsible.
_CLOUD_SYNC_SLOW_STEP_SECONDS = 1.0

# Per-sync progress trace. When set, every progress message emission records its
# monotonic timestamp so a gap between two UI updates (i.e. a backend step that
# produced no progress text) can be logged and traced to whatever was running.
_CLOUD_SYNC_PROGRESS_TRACE_CONTEXT: ContextVar[dict | None] = ContextVar(
    'cloud_sync_progress_trace',
    default=None,
)


def _cloud_sync_progress_trace() -> dict | None:
    try:
        return _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.get()
    except Exception:
        return None


def _cloud_sync_profile_enabled() -> bool:
    return str(os.getenv(_CLOUD_SYNC_PROFILE_ENV) or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _cloud_sync_debug_enabled() -> bool:
    return str(os.getenv(_CLOUD_SYNC_DEBUG_ENV) or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _cloud_sync_current_profiler() -> 'CloudSyncProfiler | None':
    try:
        return _CLOUD_SYNC_PROFILE_CONTEXT.get()
    except Exception:
        return None


def _cloud_sync_current_summary() -> dict[str, int] | None:
    try:
        return _CLOUD_SYNC_SUMMARY_CONTEXT.get()
    except Exception:
        return None


@contextmanager
def _cloud_sync_profile_scope(profiler: 'CloudSyncProfiler'):
    token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
    try:
        yield profiler
    finally:
        try:
            _CLOUD_SYNC_PROFILE_CONTEXT.reset(token)
        except Exception:
            pass


@contextmanager
def _cloud_sync_summary_scope(sync_summary: dict[str, int]):
    token = _CLOUD_SYNC_SUMMARY_CONTEXT.set(sync_summary)
    try:
        yield sync_summary
    finally:
        try:
            _CLOUD_SYNC_SUMMARY_CONTEXT.reset(token)
        except Exception:
            pass


def _cloud_sync_phase_scope(profiler: 'CloudSyncProfiler | None', phase_name: str):
    if profiler is None:
        return nullcontext()
    return profiler.phase(phase_name)


def _cloud_sync_perf_counter() -> float:
    try:
        return time.perf_counter()
    except Exception:
        return 0.0


def _cloud_sync_profile_print(payload: dict) -> None:
    try:
        print(
            f"[cloud_sync_profile] {json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))}",
            flush=True,
        )
    except Exception:
        pass


@dataclass
class CloudSyncProfiler:
    sync_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    started_at: float = field(default_factory=_cloud_sync_perf_counter)
    phase_durations_ms: dict[str, float] = field(default_factory=dict)
    download_image_file_calls: int = 0
    download_image_file_duration_ms: float = 0.0
    download_image_file_bytes: int = 0
    generate_all_sizes_calls: int = 0
    generate_all_sizes_duration_ms: float = 0.0
    pull_bulk_image_metadata_calls: int = 0
    pull_bulk_image_metadata_rows: int = 0
    pull_measurements_for_images_calls: int = 0
    pull_measurements_for_images_rows: int = 0
    store_remote_snapshot_fetch_images_count: int = 0
    store_remote_snapshot_fetch_measurements_count: int = 0
    retry_missing_cloud_media_branch_runs: int = 0
    original_upload_calls: int = 0
    original_upload_bytes: int = 0
    original_upload_skipped_disabled: int = 0
    original_upload_skipped_ineligible: int = 0
    original_upload_skipped_too_large: int = 0
    original_upload_failed_uploads: int = 0
    original_download_calls: int = 0
    original_download_bytes: int = 0
    original_download_skipped_disabled: int = 0
    original_download_skipped_missing_key: int = 0
    original_download_skipped_existing_local_original: int = 0
    original_download_skipped_existing_cache: int = 0
    original_download_failed_downloads: int = 0

    def _emit(self, payload: dict) -> None:
        payload = dict(payload or {})
        payload.setdefault('sync_id', self.sync_id)
        _cloud_sync_profile_print(payload)

    def phase(self, phase_name: str):
        @contextmanager
        def _phase_scope():
            start = _cloud_sync_perf_counter()
            try:
                yield
            finally:
                try:
                    elapsed_ms = max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0)
                    key = str(phase_name or '').strip() or 'unknown'
                    self.phase_durations_ms[key] = self.phase_durations_ms.get(key, 0.0) + elapsed_ms
                    self._emit({
                        'event': 'phase',
                        'phase': key,
                        'duration_ms': round(elapsed_ms, 3),
                    })
                except Exception:
                    pass

        return _phase_scope()

    def record_download_image_file(self, duration_ms: float, bytes_downloaded: int = 0) -> None:
        try:
            self.download_image_file_calls += 1
            self.download_image_file_duration_ms += max(0.0, float(duration_ms))
            self.download_image_file_bytes += max(0, int(bytes_downloaded))
        except Exception:
            pass

    def record_generate_all_sizes(self, duration_ms: float) -> None:
        try:
            self.generate_all_sizes_calls += 1
            self.generate_all_sizes_duration_ms += max(0.0, float(duration_ms))
        except Exception:
            pass

    def record_pull_bulk_image_metadata(self, row_count: int) -> None:
        try:
            self.pull_bulk_image_metadata_calls += 1
            self.pull_bulk_image_metadata_rows += max(0, int(row_count))
        except Exception:
            pass

    def record_pull_measurements_for_images(self, row_count: int) -> None:
        try:
            self.pull_measurements_for_images_calls += 1
            self.pull_measurements_for_images_rows += max(0, int(row_count))
        except Exception:
            pass

    def record_store_remote_snapshot_fetch(self, *, images: bool = False, measurements: bool = False) -> None:
        try:
            if images:
                self.store_remote_snapshot_fetch_images_count += 1
            if measurements:
                self.store_remote_snapshot_fetch_measurements_count += 1
        except Exception:
            pass

    def record_retry_missing_cloud_media_branch(self) -> None:
        try:
            self.retry_missing_cloud_media_branch_runs += 1
        except Exception:
            pass

    def record_original_upload_success(self, bytes_uploaded: int = 0) -> None:
        try:
            self.original_upload_calls += 1
            self.original_upload_bytes += max(0, int(bytes_uploaded))
        except Exception:
            pass

    def record_original_upload_skipped_disabled(self) -> None:
        try:
            self.original_upload_skipped_disabled += 1
        except Exception:
            pass

    def record_original_upload_skipped_ineligible(self) -> None:
        try:
            self.original_upload_skipped_ineligible += 1
        except Exception:
            pass

    def record_original_upload_skipped_too_large(self) -> None:
        try:
            self.original_upload_skipped_too_large += 1
        except Exception:
            pass

    def record_original_upload_failed(self) -> None:
        try:
            self.original_upload_failed_uploads += 1
        except Exception:
            pass

    def record_original_download_success(self, bytes_downloaded: int = 0) -> None:
        try:
            self.original_download_calls += 1
            self.original_download_bytes += max(0, int(bytes_downloaded))
        except Exception:
            pass

    def record_original_download_skipped_disabled(self) -> None:
        try:
            self.original_download_skipped_disabled += 1
        except Exception:
            pass

    def record_original_download_skipped_missing_key(self) -> None:
        try:
            self.original_download_skipped_missing_key += 1
        except Exception:
            pass

    def record_original_download_skipped_existing_local_original(self) -> None:
        try:
            self.original_download_skipped_existing_local_original += 1
        except Exception:
            pass

    def record_original_download_skipped_existing_cache(self) -> None:
        try:
            self.original_download_skipped_existing_cache += 1
        except Exception:
            pass

    def record_original_download_failed(self) -> None:
        try:
            self.original_download_failed_downloads += 1
        except Exception:
            pass

    def summary_payload(self, result: dict | None = None, error: Exception | None = None) -> dict:
        try:
            now = _cloud_sync_perf_counter()
            payload = {
                'event': 'summary',
                'status': 'error' if error else 'ok',
                'duration_ms': round(max(0.0, (now - self.started_at) * 1000.0), 3),
                'phases_ms': {
                    key: round(value, 3)
                    for key, value in sorted(self.phase_durations_ms.items(), key=lambda item: item[0])
                },
                'metrics': {
                    'download_image_file': {
                        'calls': self.download_image_file_calls,
                        'duration_ms': round(self.download_image_file_duration_ms, 3),
                        'bytes': self.download_image_file_bytes,
                    },
                    'generate_all_sizes': {
                        'calls': self.generate_all_sizes_calls,
                        'duration_ms': round(self.generate_all_sizes_duration_ms, 3),
                    },
                    'pull_bulk_image_metadata': {
                        'calls': self.pull_bulk_image_metadata_calls,
                        'rows': self.pull_bulk_image_metadata_rows,
                    },
                    'pull_measurements_for_images': {
                        'calls': self.pull_measurements_for_images_calls,
                        'rows': self.pull_measurements_for_images_rows,
                    },
                    'store_remote_snapshot': {
                        'fetched_images': self.store_remote_snapshot_fetch_images_count,
                        'fetched_measurements': self.store_remote_snapshot_fetch_measurements_count,
                    },
                    'retry_missing_cloud_media': {
                        'branch_runs': self.retry_missing_cloud_media_branch_runs,
                    },
                    'original_upload': {
                        'calls': self.original_upload_calls,
                        'bytes': self.original_upload_bytes,
                        'skipped_disabled': self.original_upload_skipped_disabled,
                        'skipped_ineligible': self.original_upload_skipped_ineligible,
                        'skipped_too_large': self.original_upload_skipped_too_large,
                        'failed_uploads': self.original_upload_failed_uploads,
                    },
                    'original_download': {
                        'calls': self.original_download_calls,
                        'bytes': self.original_download_bytes,
                        'skipped_disabled': self.original_download_skipped_disabled,
                        'skipped_missing_key': self.original_download_skipped_missing_key,
                        'skipped_existing_local_original': self.original_download_skipped_existing_local_original,
                        'skipped_existing_cache': self.original_download_skipped_existing_cache,
                        'failed_downloads': self.original_download_failed_downloads,
                    },
                },
            }
            if result is not None:
                payload['result'] = {
                    'pushed': int(result.get('pushed', 0) or 0),
                    'pulled': int(result.get('pulled', 0) or 0),
                    'calibrations_pushed': int(result.get('calibrations_pushed', 0) or 0),
                    'calibrations_pulled': int(result.get('calibrations_pulled', 0) or 0),
                    'deleted_remote': len(result.get('deleted_remote') or []),
                    'error_count': len(result.get('errors') or []),
                }
                sync_summary = result.get('sync_summary')
                if isinstance(sync_summary, dict):
                    payload['result']['sync_summary'] = {
                        str(key): _safe_int(value)
                        for key, value in sync_summary.items()
                    }
            if error is not None:
                error_text = str(error or '').strip()
                if error_text:
                    payload['error'] = error_text[:300]
                payload['error_type'] = error.__class__.__name__
            return payload
        except Exception:
            return {
                'event': 'summary',
                'status': 'error' if error else 'ok',
                'duration_ms': 0.0,
                'phases_ms': {},
                'metrics': {},
            }

    def finish(self, result: dict | None = None, error: Exception | None = None) -> None:
        try:
            self._emit(self.summary_payload(result=result, error=error))
        except Exception:
            pass

_PUSH_CONFLICT_RE = re.compile(
    r"^obs\s+(?P<local_id>\d+):\s+skipped desktop push because the linked cloud observation changed on the web$"
)
_PULL_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+skipped remote update because local observation\s+(?P<local_id>\d+)\s+has unsynced desktop edits$"
)
_REVIEW_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+needs review before applying remaining cloud changes to local observation\s+(?P<local_id>\d+)(?:\s+\((?P<reason>.*)\))?$"
)


class CloudSyncError(Exception):
    pass


class AccountMismatchError(CloudSyncError):
    pass


class CloudTemporarilyUnavailableError(CloudSyncError):
    pass


ACCOUNT_MISMATCH_MESSAGE = (
    "This local database is permanently linked to another Sporely Cloud account. "
    "Please switch to the correct OS user profile, or use the 'Reset Cloud Sync' "
    "tool in Settings to migrate your data to a new account."
)
PRIVACY_SLOT_LIMIT_USER_MESSAGE = (
    "Free accounts can have up to 20 private or fuzzed-location cloud observations. "
    "Make one public, delete one, or upgrade to Pro."
)
IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE = (
    "Image upload was rejected by the worker."
)
FREE_TIER_PRIVACY_SLOT_LIMIT = 20
_PRIVACY_SLOT_LIMIT_HINTS = (
    "Free Sporely accounts",
    "20 privacy slot",
    "privacy slot observations",
)
_IMAGE_TOO_LARGE_FOR_PLAN_HINTS = (
    "image too large for plan",
    "too large for your plan",
)
_IMAGE_TOO_LARGE_FOR_PLAN_REASONS = {"byte_cap", "pixel_cap", "edge_cap", "unknown"}
_IMAGE_TOO_LARGE_FOR_PLAN_FIRST_LINE_RE = re.compile(
    r"(?i)^\s*(?:Image(?:\s+is)?\s+too\s+large(?:\s+for\s+your\s+plan)?(?:\.\s*Make it smaller or upgrade to Pro\.)?|Image\s+too\s+large\s+for\s+plan)\s*$"
)
_IMAGE_TOO_LARGE_FOR_PLAN_REASON_LINE_RE = re.compile(
    r"(?im)^\s*(?:Worker\s+)?Reason:\s*(?P<reason>[A-Za-z_]+)\s*$"
)


def _normalize_image_too_large_reason(reason: str | None) -> str:
    text = str(reason or "").strip().lower().replace("-", "_")
    return text if text in _IMAGE_TOO_LARGE_FOR_PLAN_REASONS else ""


def _parse_human_size(text: str | None) -> int | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    match = re.search(r"(?i)(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)\b", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2).upper()
    scale = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
    }.get(unit, 1)
    return int(value * scale)


def _parse_dimension_pair(text: str | None) -> tuple[int, int] | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    match = re.search(r"(?i)(\d+)\s*[×x]\s*(\d+)\s*px\b", cleaned)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _parse_int_text(text: str | None) -> int | None:
    cleaned = str(text or "").strip().replace(",", "")
    if not cleaned:
        return None
    match = re.search(r"(-?\d+)", cleaned)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _extract_label_value(text: str, label: str) -> str:
    pattern = re.compile(rf"(?im)^\s*{re.escape(label)}\s*:\s*(?P<value>.+?)\s*$")
    match = pattern.search(str(text or ""))
    return str(match.group("value") or "").strip() if match else ""


def _image_too_large_reason_message(reason: str | None) -> str:
    normalized = _normalize_image_too_large_reason(reason)
    if normalized == "byte_cap":
        return "Image exceeds the byte cap for this upload policy."
    if normalized == "pixel_cap":
        return "Image exceeds the pixel cap for this upload policy."
    if normalized == "edge_cap":
        return "Image exceeds the longest-edge cap for this upload policy."
    return IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE


def _image_too_large_summary_message(reason: str | None) -> str:
    normalized = _normalize_image_too_large_reason(reason)
    if normalized == "byte_cap":
        return "Cloud sync failed while uploading an image that exceeded the byte cap."
    if normalized == "pixel_cap":
        return "Cloud sync failed while uploading an image that exceeded the pixel cap."
    if normalized == "edge_cap":
        return "Cloud sync failed while uploading an image that exceeded the longest-edge cap."
    return "Cloud sync failed while uploading an image that was rejected by the worker."


def _infer_image_too_large_reason_from_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""

    reason_match = _IMAGE_TOO_LARGE_FOR_PLAN_REASON_LINE_RE.search(cleaned)
    if reason_match:
        normalized = _normalize_image_too_large_reason(reason_match.group("reason"))
        if normalized:
            return normalized

    worker_body_bytes = _parse_human_size(_extract_label_value(cleaned, "Worker body size"))
    worker_plan_cap = _parse_human_size(_extract_label_value(cleaned, "Worker plan cap"))
    prepared_bytes = _parse_human_size(_extract_label_value(cleaned, "Prepared upload size"))
    plan_cap = _parse_human_size(_extract_label_value(cleaned, "Plan cap"))
    if worker_body_bytes and worker_plan_cap and worker_body_bytes > worker_plan_cap:
        return "byte_cap"
    if prepared_bytes and plan_cap and prepared_bytes > plan_cap:
        return "byte_cap"

    worker_stored_pixels = _parse_int_text(_extract_label_value(cleaned, "Worker stored pixels"))
    worker_stored_pixel_cap = _parse_int_text(_extract_label_value(cleaned, "Worker stored pixel cap"))
    if worker_stored_pixels and worker_stored_pixel_cap and worker_stored_pixels > worker_stored_pixel_cap:
        return "pixel_cap"

    worker_resize_max_edge = _parse_int_text(_extract_label_value(cleaned, "Worker resize max edge"))
    stored_dimensions = _parse_dimension_pair(
        _extract_label_value(cleaned, "Worker stored dimensions") or _extract_label_value(cleaned, "Prepared dimensions")
    )
    if stored_dimensions and worker_resize_max_edge and max(stored_dimensions) > worker_resize_max_edge:
        return "edge_cap"

    return "unknown"


def infer_image_too_large_for_plan_reason(error) -> str:
    code, texts = _collect_sync_error_details(error)
    haystack = " ".join(dict.fromkeys(texts)).lower()
    if not (
        code.strip().lower() == "image_too_large_for_plan"
        or "image_too_large_for_plan" in haystack
        or "too large for your plan" in haystack
    ):
        return ""

    payload = {}
    if isinstance(error, dict):
        payload = dict(error)
    else:
        for attr in ("payload", "response_payload", "response", "body"):
            try:
                candidate = getattr(error, attr)
            except Exception:
                candidate = None
            if isinstance(candidate, dict):
                payload = dict(candidate)
                break

    details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
    reason = _normalize_image_too_large_reason(
        details.get("reason")
        or payload.get("reason")
        or details.get("errorReason")
        or payload.get("errorReason")
    )
    if reason:
        return reason

    worker_body_bytes = _safe_int(
        details.get("bodyBytes")
        or details.get("body_bytes")
        or payload.get("bodyBytes")
        or payload.get("body_bytes")
    )
    worker_plan_cap = _safe_int(
        details.get("planByteCap")
        or details.get("plan_byte_cap")
        or details.get("planCap")
        or details.get("plan_cap")
        or payload.get("planByteCap")
        or payload.get("plan_byte_cap")
        or payload.get("planCap")
        or payload.get("plan_cap")
    )
    if worker_body_bytes > 0 and worker_plan_cap > 0 and worker_body_bytes > worker_plan_cap:
        return "byte_cap"

    prepared_bytes = _safe_int(
        details.get("preparedBytes")
        or details.get("prepared_bytes")
        or payload.get("preparedBytes")
        or payload.get("prepared_bytes")
    )
    prepared_plan_cap = _safe_int(
        details.get("planCap")
        or details.get("plan_cap")
        or payload.get("planCap")
        or payload.get("plan_cap")
        or worker_plan_cap
    )
    if prepared_bytes > 0 and prepared_plan_cap > 0 and prepared_bytes > prepared_plan_cap:
        return "byte_cap"

    worker_stored_pixels = _safe_int(
        details.get("storedPixels")
        or details.get("stored_pixels")
        or payload.get("storedPixels")
        or payload.get("stored_pixels")
    )
    worker_stored_pixel_cap = _safe_int(
        details.get("storedPixelCap")
        or details.get("stored_pixel_cap")
        or payload.get("storedPixelCap")
        or payload.get("stored_pixel_cap")
    )
    if worker_stored_pixels > 0 and worker_stored_pixel_cap > 0 and worker_stored_pixels > worker_stored_pixel_cap:
        return "pixel_cap"

    worker_stored_width = _safe_int(
        details.get("storedWidth")
        or details.get("stored_width")
        or payload.get("storedWidth")
        or payload.get("stored_width")
        or details.get("preparedWidth")
        or details.get("prepared_width")
        or payload.get("preparedWidth")
        or payload.get("prepared_width")
    )
    worker_stored_height = _safe_int(
        details.get("storedHeight")
        or details.get("stored_height")
        or payload.get("storedHeight")
        or payload.get("stored_height")
        or details.get("preparedHeight")
        or details.get("prepared_height")
        or payload.get("preparedHeight")
        or payload.get("prepared_height")
    )
    worker_resize_max_edge = _safe_int(
        details.get("resizeMaxEdge")
        or details.get("resize_max_edge")
        or payload.get("resizeMaxEdge")
        or payload.get("resize_max_edge")
    )
    if worker_stored_width > 0 and worker_stored_height > 0 and worker_resize_max_edge > 0 and max(worker_stored_width, worker_stored_height) > worker_resize_max_edge:
        return "edge_cap"

    return _infer_image_too_large_reason_from_text("\n".join(dict.fromkeys(texts)))


def format_image_too_large_for_plan_reason(reason_or_error) -> str:
    if isinstance(reason_or_error, str):
        reason = _normalize_image_too_large_reason(reason_or_error)
        if not reason:
            reason = infer_image_too_large_for_plan_reason(reason_or_error)
    else:
        reason = infer_image_too_large_for_plan_reason(reason_or_error)
    return _image_too_large_reason_message(reason)


def summarize_image_too_large_for_plan_error(reason_or_error) -> str:
    if isinstance(reason_or_error, str):
        reason = _normalize_image_too_large_reason(reason_or_error)
        if not reason:
            reason = infer_image_too_large_for_plan_reason(reason_or_error)
    else:
        reason = infer_image_too_large_for_plan_reason(reason_or_error)
    return _image_too_large_summary_message(reason)


def sanitize_image_too_large_for_plan_error_message(error) -> str:
    text = str(error or "").strip()
    if not text or not is_image_too_large_for_plan_error(text):
        return text
    lines = [line.rstrip() for line in text.splitlines()]
    reason_message = format_image_too_large_for_plan_reason(text)
    if not lines:
        return reason_message
    lines[0] = reason_message
    return "\n".join(lines)


def privacy_slot_limit_user_message() -> str:
    return PRIVACY_SLOT_LIMIT_USER_MESSAGE


def cloud_observation_uses_privacy_slot(observation: dict | None) -> bool:
    record = dict(observation or {})
    sharing_scope = _normalize_sharing_scope(
        record.get('visibility') or record.get('sharing_scope'),
        fallback='private',
    )
    location_precision = ObservationDB._normalize_location_precision(record.get('location_precision'))
    return sharing_scope != 'public' or location_precision == 'fuzzed'


def count_cloud_privacy_slots(remote_observations: list[dict] | None) -> int:
    return sum(1 for row in list(remote_observations or []) if cloud_observation_uses_privacy_slot(row))


def _parse_postgrest_content_range_total(content_range: str | None) -> int | None:
    text = str(content_range or '').strip()
    if not text or '/' not in text:
        return None
    total_text = text.rsplit('/', 1)[-1].strip()
    if total_text == '*':
        return None
    try:
        return int(total_text)
    except (TypeError, ValueError):
        return None


def fetch_cloud_usage_summary(client) -> dict:
    profile = normalize_cloud_plan_profile({})
    profile_loaded = False
    privacy_count_loaded = False
    privacy_loaded = False
    privacy_slots_used: int | None = None
    profile_error = ''
    privacy_error = ''
    if client is not None:
        try:
            profile = normalize_cloud_plan_profile(client.fetch_cloud_plan_profile())
            profile_loaded = True
        except Exception as exc:
            profile = normalize_cloud_plan_profile({})
            profile_error = format_cloud_sync_error_details(exc)
            if profile_error:
                logger.warning(
                    "Cloud plan profile lookup failed for user_id=%s: %s",
                    getattr(client, 'user_id', ''),
                    profile_error,
                )
        try:
            count_method = getattr(client, 'count_remote_privacy_slots', None)
            if callable(count_method):
                privacy_slots_used = int(count_method())
                privacy_count_loaded = True
        except Exception as exc:
            privacy_slots_used = None
            privacy_error = format_cloud_sync_error_details(exc)
            if privacy_error:
                logger.warning(
                    "Cloud privacy slot count failed for user_id=%s: %s",
                    getattr(client, 'user_id', ''),
                    privacy_error,
                )
    has_pro_access = bool(profile.get('has_pro_access') or str(profile.get('cloud_plan') or '').strip().lower() == 'pro')
    privacy_slots_limit = None if has_pro_access else FREE_TIER_PRIVACY_SLOT_LIMIT
    privacy_loaded = bool(profile_loaded and privacy_count_loaded)
    if not privacy_loaded:
        privacy_slots_used = None
    privacy_slots_available = None if privacy_slots_limit is None or not privacy_loaded else max(
        0,
        int(privacy_slots_limit) - int(privacy_slots_used),
    )
    error_messages = []
    if profile_error:
        error_messages.append(f"Profile lookup: {profile_error}")
    if privacy_error:
        error_messages.append(f"Private slot count: {privacy_error}")
    summary = dict(profile)
    summary.update({
        'privacy_slots_used': privacy_slots_used,
        'privacySlotsUsed': privacy_slots_used,
        'privacy_slot_count': privacy_slots_used,
        'privacySlotCount': privacy_slots_used,
        'privacy_slots_limit': privacy_slots_limit,
        'privacySlotsLimit': privacy_slots_limit,
        'privacy_slots_available': privacy_slots_available,
        'privacySlotsAvailable': privacy_slots_available,
        'cloud_profile_loaded': profile_loaded,
        'cloudProfileLoaded': profile_loaded,
        'cloud_privacy_usage_loaded': privacy_loaded,
        'cloudPrivacyUsageLoaded': privacy_loaded,
        'cloud_usage_loaded': bool(profile_loaded and privacy_loaded),
        'cloudUsageLoaded': bool(profile_loaded and privacy_loaded),
        'cloud_profile_error': profile_error or None,
        'cloudProfileError': profile_error or None,
        'cloud_privacy_usage_error': privacy_error or None,
        'cloudPrivacyUsageError': privacy_error or None,
        'cloud_usage_error': "\n".join(error_messages),
        'cloudUsageError': "\n".join(error_messages),
        'cloud_usage_error_messages': error_messages,
        'cloudUsageErrorMessages': error_messages,
    })
    return summary


def _collect_sync_error_details(value, seen: set[int] | None = None) -> tuple[str, list[str]]:
    if seen is None:
        seen = set()
    try:
        marker = id(value)
    except Exception:
        marker = None
    if marker is not None and marker in seen:
        return '', []
    if marker is not None:
        seen.add(marker)

    code = ''
    texts: list[str] = []
    if value is None:
        return code, texts

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return code, texts
        texts.append(text)
        if text[:1] in {'{', '['}:
            try:
                parsed = json.loads(text)
            except Exception:
                return code, texts
            parsed_code, parsed_texts = _collect_sync_error_details(parsed, seen)
            if parsed_code and not code:
                code = parsed_code
            texts.extend(parsed_texts)
        if not code:
            lowered = text.lower()
            if '23514' in text:
                code = '23514'
            elif 'check_violation' in lowered:
                code = 'check_violation'
        return code, texts

    if isinstance(value, dict):
        for key in ('code', 'sqlstate', 'status_code', 'statusCode', 'status'):
            raw_code = value.get(key)
            if raw_code not in (None, ''):
                candidate = str(raw_code).strip()
                if candidate and not code:
                    code = candidate
        for key in ('message', 'details', 'hint', 'error', 'body', 'text', 'reason', 'response'):
            if key not in value:
                continue
            sub_code, sub_texts = _collect_sync_error_details(value.get(key), seen)
            if sub_code and not code:
                code = sub_code
            texts.extend(sub_texts)
        return code, texts

    for attr in ('code', 'sqlstate', 'status_code', 'statusCode', 'status'):
        try:
            raw_code = getattr(value, attr)
        except Exception:
            raw_code = None
        if raw_code not in (None, ''):
            candidate = str(raw_code).strip()
            if candidate and not code:
                code = candidate
    for attr in ('message', 'details', 'hint', 'error', 'body', 'text', 'reason', 'response', 'payload', 'response_payload'):
        try:
            raw_value = getattr(value, attr)
        except Exception:
            raw_value = None
        if raw_value is None:
            continue
        sub_code, sub_texts = _collect_sync_error_details(raw_value, seen)
        if sub_code and not code:
            code = sub_code
        texts.extend(sub_texts)
    for attr in ('__cause__', '__context__'):
        try:
            chained_value = getattr(value, attr)
        except Exception:
            chained_value = None
        if chained_value is None:
            continue
        sub_code, sub_texts = _collect_sync_error_details(chained_value, seen)
        if sub_code and not code:
            code = sub_code
        texts.extend(sub_texts)
    text = str(value).strip()
    if text:
        texts.append(text)
        if not code:
            lowered = text.lower()
            if '23514' in text:
                code = '23514'
            elif 'check_violation' in lowered:
                code = 'check_violation'
    return code, texts


def format_cloud_sync_error_details(error) -> str:
    code, texts = _collect_sync_error_details(error)
    parts: list[str] = []
    code_text = str(code or '').strip()
    if code_text:
        parts.append(f"code={code_text}")
    for text in dict.fromkeys(texts):
        cleaned = str(text or '').strip()
        if cleaned and cleaned not in parts:
            parts.append(cleaned)
    if not parts:
        fallback = str(error or '').strip()
        if fallback:
            parts.append(fallback)
    return " | ".join(parts)


def is_privacy_slot_limit_error(error) -> bool:
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    has_privacy_phrase = any(hint.lower() in haystack for hint in _PRIVACY_SLOT_LIMIT_HINTS)
    has_constraint_code = (
        code.strip() == '23514'
        or code.strip().lower() == 'check_violation'
        or '23514' in haystack
        or 'check_violation' in haystack
    )
    return has_privacy_phrase and has_constraint_code


def is_image_too_large_for_plan_error(error) -> bool:
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    has_phrase = any(hint in haystack for hint in _IMAGE_TOO_LARGE_FOR_PLAN_HINTS)
    has_code = (
        code.strip().lower() == 'image_too_large_for_plan'
        or 'image_too_large_for_plan' in haystack
        or 'payload_too_large' in haystack
    )
    return has_phrase or has_code


def is_webp_support_required_for_cloud_media_upload_error(error) -> bool:
    return WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE.lower() in str(error or '').lower()


_CLOUD_AUTH_ERROR_HINTS = (
    'jwt expired',
    'invalid jwt',
    'expired access token',
    'access token expired',
    'auth refresh failed',
    'token expired',
    'session expired',
    'authentication failed',
    'invalid_grant',
    'not logged in',
    'unauthorized',
    'pgrst301',
    'pgrst303',
)


def is_cloud_auth_error(error) -> bool:
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    code_text = str(code or '').strip().lower()
    if code_text in {'401', '403'}:
        return True
    return any(hint in haystack for hint in _CLOUD_AUTH_ERROR_HINTS)


def _sleep_supabase_backoff(attempt: int) -> None:
    delay = min(
        _SUPABASE_REQUEST_BACKOFF_MAX_SECONDS,
        _SUPABASE_REQUEST_BACKOFF_BASE_SECONDS * (2 ** max(0, int(attempt))),
    )
    if delay <= 0:
        return
    time.sleep(random.uniform(0.0, delay))


def _request_exception_is_transient(error: Exception) -> bool:
    if isinstance(error, (requests.Timeout, requests.ConnectionError)):
        return True
    _, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    return any(hint in haystack for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)


def _response_indicates_transient_supabase_error(response: requests.Response) -> bool:
    try:
        status_code = int(getattr(response, 'status_code', 0) or 0)
    except Exception:
        status_code = 0
    if status_code in _SUPABASE_TRANSIENT_STATUS_CODES:
        return True
    try:
        text = str(getattr(response, 'text', '') or '').strip().lower()
    except Exception:
        text = ''
    if not text:
        return False
    return any(hint in text for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)


def _response_indicates_auth_error(response: requests.Response) -> bool:
    try:
        status_code = int(getattr(response, 'status_code', 0) or 0)
    except Exception:
        status_code = 0
    if status_code in {401, 403}:
        return True
    try:
        return is_cloud_auth_error(getattr(response, 'text', ''))
    except Exception:
        return False


def is_cloud_temporary_unavailable_error(error) -> bool:
    if isinstance(error, CloudTemporarilyUnavailableError):
        return True
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    code_text = str(code or '').strip().lower()
    if code_text in {'pgrst000', 'pgrst001', 'pgrst002', 'pgrst003'}:
        return True
    if code_text in {str(status) for status in _SUPABASE_TRANSIENT_STATUS_CODES}:
        return True
    if _CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE.lower() in haystack:
        return True
    return any(hint in haystack for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)


def _request_with_transient_retry(
    request_callable,
    method: str,
    url: str,
    *,
    refresh_on_auth_error: bool = False,
    refresh_callback: Callable[[], bool] | None = None,
    **kwargs,
):
    last_response: requests.Response | None = None
    refreshed = False
    for attempt in range(_SUPABASE_REQUEST_MAX_ATTEMPTS):
        try:
            response = request_callable(method, url, **kwargs)
        except Exception as exc:
            if isinstance(exc, requests.RequestException) and _request_exception_is_transient(exc):
                if attempt < _SUPABASE_REQUEST_MAX_ATTEMPTS - 1:
                    _sleep_supabase_backoff(attempt)
                    continue
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            raise

        last_response = response
        if getattr(response, 'ok', False):
            return response

        if refresh_on_auth_error and not refreshed and _response_indicates_auth_error(response):
            refreshed = True
            try:
                refreshed_ok = bool(refresh_callback()) if callable(refresh_callback) else False
            except CloudTemporarilyUnavailableError:
                raise
            except Exception as exc:
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            if refreshed_ok:
                continue
            raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from CloudSyncError(
                f'{method} {url} status={getattr(response, "status_code", "")}: auth refresh failed'
            )

        if _response_indicates_transient_supabase_error(response):
            if attempt < _SUPABASE_REQUEST_MAX_ATTEMPTS - 1:
                _sleep_supabase_backoff(attempt)
                continue
            raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from CloudSyncError(
                f'{method} {url} status={getattr(response, "status_code", "")}: {getattr(response, "text", "")}'
            )
        return response

    if last_response is not None:
        return last_response
    raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE)


def _normalize_cloud_user_id(value: str | None) -> str:
    return str(value or '').strip()


def _decode_jwt_subject(access_token: str | None) -> str:
    token = str(access_token or '').strip()
    parts = token.split('.')
    if len(parts) < 2:
        return ''
    payload = parts[1]
    padding = '=' * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode((payload + padding).encode('ascii')).decode('utf-8'))
    except Exception:
        return ''
    return _normalize_cloud_user_id(data.get('sub') if isinstance(data, dict) else None)


def _load_linked_cloud_user_id() -> str:
    return _normalize_cloud_user_id(get_app_settings().get(_SETTING_LINKED_CLOUD_USER_ID))


def _save_linked_cloud_user_id(user_id: str) -> None:
    normalized = _normalize_cloud_user_id(user_id)
    if normalized:
        update_app_settings({_SETTING_LINKED_CLOUD_USER_ID: normalized})


def ensure_database_linked_to_cloud_user(client: "SporelyCloudClient") -> str:
    """Bind this local DB to the active cloud account, or reject a mismatch."""
    current_user_id = ""
    token_user_id = ""
    try:
        token_user_id = _normalize_cloud_user_id(_decode_jwt_subject(getattr(client, 'access_token', None)))
    except Exception:
        token_user_id = ""
    if token_user_id:
        current_user_id = token_user_id
        try:
            if getattr(client, 'user_id', '') != token_user_id:
                client.user_id = token_user_id
        except Exception:
            pass
    if not current_user_id:
        current_user_id = _normalize_cloud_user_id(getattr(client, 'user_id', ''))
    if not current_user_id and hasattr(client, 'fetch_current_user_id'):
        current_user_id = _normalize_cloud_user_id(client.fetch_current_user_id())
    if not current_user_id:
        raise CloudSyncError("Could not verify the active Sporely Cloud account before syncing.")
    linked_user_id = _load_linked_cloud_user_id()
    if not linked_user_id:
        _save_linked_cloud_user_id(current_user_id)
        return current_user_id
    if linked_user_id != current_user_id:
        raise AccountMismatchError(ACCOUNT_MISMATCH_MESSAGE)
    return current_user_id


def summarize_sync_issues(errors: list[str] | tuple[str, ...] | None) -> dict:
    conflict_entries: dict[str, dict] = {}
    blocked_errors: list[dict] = []
    retryable_errors: list[dict] = []
    other_errors: list[str] = []

    for raw_error in list(errors or []):
        text = str(raw_error or '').strip()
        if not text:
            continue
        if is_privacy_slot_limit_error(text):
            blocked_errors.append({
                'error': text,
                'message': privacy_slot_limit_user_message(),
            })
            continue
        if is_image_too_large_for_plan_error(text):
            reason = infer_image_too_large_for_plan_reason(text)
            retryable_errors.append({
                'error': text,
                'reason': reason,
                'message': summarize_image_too_large_for_plan_error(reason),
            })
            continue
        push_match = _PUSH_CONFLICT_RE.match(text)
        if push_match:
            local_id = int(push_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['push_skipped'] = True
            continue
        pull_match = _PULL_CONFLICT_RE.match(text)
        if pull_match:
            local_id = int(pull_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(pull_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            continue
        review_match = _REVIEW_CONFLICT_RE.match(text)
        if review_match:
            local_id = int(review_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(review_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            continue
        other_errors.append(text)

    conflicts = sorted(
        conflict_entries.values(),
        key=lambda row: (
            int(row.get('local_id') or 0),
            str(row.get('cloud_id') or ''),
        ),
    )
    return {
        'conflicts': conflicts,
        'conflict_count': len(conflicts),
        'blocked_errors': blocked_errors,
        'blocked_count': len(blocked_errors),
        'retryable_errors': retryable_errors,
        'retryable_count': len(retryable_errors),
        'other_errors': other_errors,
        'other_count': len(other_errors),
        'display_count': len(conflicts) + len(blocked_errors) + len(retryable_errors) + len(other_errors),
    }


def _parse_cloud_observation_snapshot(snapshot: str | None) -> dict:
    text = str(snapshot or '').strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    images = data.get('images')
    if isinstance(images, list):
        data['images'] = [
            dict(row or {})
            for row in images
            if should_pull_cloud_image_to_desktop(row)
        ]
    measurements = data.get('measurements')
    if isinstance(measurements, list):
        data['measurements'] = [dict(row or {}) for row in measurements]
    return data


def _normalize_observation_field_value(field: str, value):
    if field == 'date':
        text = str(value or '').strip()
        if not text:
            return None
        if len(text) >= 10 and re.match(r'^\d{4}-\d{2}-\d{2}', text):
            return text[:10]
        return text
    if field in {'location_public', 'uncertain', 'unspontaneous', 'interesting_comment'}:
        return _normalize_observation_bool_value(value, default=None)
    if field == 'is_draft':
        return _normalize_observation_bool_value(value, default=True)
    if field in _OBSERVATION_FLOAT_FIELDS:
        return _normalize_observation_float_value(value)
    if field in _OBSERVATION_INT_FIELDS:
        return _normalize_observation_int_value(value)
    if field == 'location_precision':
        return ObservationDB._normalize_location_precision(value)
    if field == 'spore_data_visibility':
        raw = str(value or 'public').strip().lower()
        return raw if raw in {'private', 'friends', 'public'} else 'public'
    if field == 'spore_statistics':
        return _normalize_observation_json_value(value)
    return _normalize_snapshot_value(value)


def _observation_field_values_match(field: str, left, right) -> bool:
    if field in _OBSERVATION_FLOAT_FIELDS:
        left_value = _normalize_observation_float_value(left)
        right_value = _normalize_observation_float_value(right)
        if left_value is None or right_value is None:
            return left_value == right_value
        return math.isclose(
            left_value,
            right_value,
            rel_tol=_OBSERVATION_FLOAT_REL_TOL,
            abs_tol=_OBSERVATION_FLOAT_ABS_TOL,
        )
    return left == right


def _observation_push_payload(record: dict | None, *, local: bool) -> dict:
    row = dict(record or {})
    payload = {col: row.get(col) for col in _OBS_PUSH_COLS}
    payload['date'] = _normalize_observation_field_value('date', payload.get('date'))
    scope_source = row.get('sharing_scope') if local else (row.get('visibility') or row.get('sharing_scope'))
    payload['visibility'] = _sharing_scope_to_cloud_visibility(scope_source, fallback='private')
    raw_vis = str(payload.get('spore_data_visibility') or 'public').strip().lower()
    payload['spore_data_visibility'] = raw_vis if raw_vis in {'private', 'friends', 'public'} else 'public'
    payload['location_precision'] = ObservationDB._normalize_location_precision(payload.get('location_precision'))
    payload['is_draft'] = _normalize_observation_bool_value(payload.get('is_draft'), default=True)
    for field in ('location_public', 'uncertain', 'unspontaneous', 'interesting_comment'):
        payload[field] = _normalize_observation_bool_value(payload.get(field), default=None)
    for field in _OBSERVATION_INT_FIELDS:
        payload[field] = _normalize_observation_int_value(payload.get(field))
    for field in _OBSERVATION_FLOAT_FIELDS:
        payload[field] = _normalize_observation_float_value(payload.get(field))
    payload['spore_statistics'] = _normalize_observation_json_value(payload.get('spore_statistics'))
    raw_publish_target = str(payload.get('publish_target') or '').strip()
    if raw_publish_target:
        payload['publish_target'] = normalize_publish_target(raw_publish_target)
    return payload


def _parse_sync_timestamp(value) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _observation_compare_payload(record: dict | None, *, local: bool) -> dict:
    row = dict(record or {})
    payload = _observation_push_payload(row, local=local)
    payload['id'] = _normalize_observation_field_value(
        'id',
        row.get('cloud_id') if local else row.get('id'),
    )
    payload['desktop_id'] = _normalize_observation_field_value(
        'desktop_id',
        row.get('id') if local else row.get('desktop_id'),
    )
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if field not in payload:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    return payload


def _baseline_observation_compare_payload(record: dict | None) -> dict:
    """Normalize a stored snapshot observation for fair comparison with live rows."""
    row = dict(record or {})
    payload = _observation_push_payload(row, local=False)
    payload['id'] = _normalize_observation_field_value('id', row.get('id'))
    payload['desktop_id'] = _normalize_observation_field_value('desktop_id', row.get('desktop_id'))
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if field not in payload:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    return payload


def _observation_push_diff_fields(local_obs: dict | None, remote_obs: dict | None) -> list[str]:
    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    diff_fields: list[str] = []
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if not _observation_field_values_match(field, local_payload.get(field), remote_payload.get(field)):
            diff_fields.append(field)
    return diff_fields


def _local_image_snapshot_payload(image_row: dict | None) -> dict:
    row = dict(image_row or {})
    payload = {
        'id': _normalize_snapshot_value(str(row.get('cloud_id') or '').strip() or None),
        'desktop_id': _normalize_snapshot_value(row.get('id')),
        'sort_order': _normalize_snapshot_value(row.get('sort_order')),
        'image_type': _normalize_snapshot_value(row.get('image_type')),
        'micro_category': _normalize_snapshot_value(row.get('micro_category')),
        'calibration_uuid': _normalize_snapshot_value(_image_calibration_uuid(row)),
        'objective_name': _normalize_snapshot_value(row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
        'stain': _normalize_snapshot_value(row.get('stain')),
        'sample_type': _normalize_snapshot_value(row.get('sample_type')),
        'contrast': _normalize_snapshot_value(row.get('contrast')),
        'measure_color': _normalize_snapshot_value(row.get('measure_color')),
        'crop_mode': _normalize_snapshot_value(row.get('crop_mode')),
        'notes': _normalize_snapshot_value(row.get('notes')),
        'gps_source': _normalize_snapshot_value(
            None if row.get('gps_source') is None else bool(row.get('gps_source'))
        ),
        'storage_path': None,
        'original_filename': _normalize_snapshot_value(
            Path(str(row.get('filepath') or '')).name or None
        ),
        'ai_crop_x1': _normalize_snapshot_value(row.get('ai_crop_x1')),
        'ai_crop_y1': _normalize_snapshot_value(row.get('ai_crop_y1')),
        'ai_crop_x2': _normalize_snapshot_value(row.get('ai_crop_x2')),
        'ai_crop_y2': _normalize_snapshot_value(row.get('ai_crop_y2')),
        'ai_crop_source_w': _normalize_snapshot_value(row.get('ai_crop_source_w')),
        'ai_crop_source_h': _normalize_snapshot_value(row.get('ai_crop_source_h')),
    }
    return payload


def _image_compare_key(image_row: dict | None) -> str:
    row = dict(image_row or {})
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    filename = str(row.get('original_filename') or '').strip()
    image_type = str(row.get('image_type') or '').strip()
    if filename:
        if cloud_id:
            return f'cloud:{cloud_id}'
        if desktop_id:
            return f'desktop:{desktop_id}'
        suffix = f':{image_type}' if image_type else ''
        return f'name:{filename}{suffix}'
    if cloud_id:
        return f'cloud:{cloud_id}'
    if desktop_id:
        return f'desktop:{desktop_id}'
    return json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _image_identity_keys(image_row: dict | None) -> set[str]:
    row = dict(image_row or {})
    keys: set[str] = set()
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    if cloud_id:
        keys.add(f'cloud:{cloud_id}')
    if desktop_id:
        keys.add(f'desktop:{desktop_id}')
    return keys


def _deleted_remote_image_identity_keys(remote_images: list[dict] | None) -> set[str]:
    keys: set[str] = set()
    for row in (remote_images or []):
        if not str((row or {}).get('deleted_at') or '').strip():
            continue
        keys.update(_image_identity_keys(_remote_image_payload(row)))
    return keys


def _image_label(image_row: dict | None) -> str:
    row = dict(image_row or {})
    filename = str(row.get('original_filename') or '').strip()
    image_type = str(row.get('image_type') or '').strip()
    if filename and image_type:
        return f'{filename} ({image_type})'
    if filename:
        return filename
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    if cloud_id:
        return f'cloud image {cloud_id}'
    if desktop_id:
        return f'local image {desktop_id}'
    return 'image'


def _image_metadata_payload(image_row: dict | None) -> dict:
    row = dict(image_row or {})
    hidden_fields = {
        'upload_mode',
        'source_width',
        'source_height',
        'stored_width',
        'stored_height',
        'stored_bytes',
    }
    return {
        field: row.get(field)
        for field in _SNAPSHOT_IMG_FIELDS
        if field not in {'id', 'desktop_id', 'sort_order', 'storage_path', 'original_filename'}
        and field not in hidden_fields
    }


def _format_image_metadata_field_label(field: str) -> str:
    labels = {
        'measure_color': 'measurement color',
        'crop_mode': 'crop mode',
        'ai_crop_x1': 'AI crop left',
        'ai_crop_y1': 'AI crop top',
        'ai_crop_x2': 'AI crop right',
        'ai_crop_y2': 'AI crop bottom',
        'ai_crop_source_w': 'AI crop source width',
        'ai_crop_source_h': 'AI crop source height',
        'ai_crop_is_custom': 'custom crop',
    }
    return labels.get(field, field.replace('_', ' '))


def _summarize_image_changes(
    current_images: list[dict],
    baseline_images: list[dict],
    *,
    ignored_keys: set[str] | None = None,
) -> list[str]:
    current = [dict(row or {}) for row in (current_images or [])]
    baseline = [dict(row or {}) for row in (baseline_images or [])]
    ignored = {str(key or '').strip() for key in (ignored_keys or set()) if str(key or '').strip()}
    current_keys = [_image_compare_key(row) for row in current]
    baseline_keys = [_image_compare_key(row) for row in baseline]
    current_map = {_image_compare_key(row): row for row in current}
    baseline_map = {_image_compare_key(row): row for row in baseline}

    added = [current_map[key] for key in current_keys if key not in baseline_map and key not in ignored]
    removed = [baseline_map[key] for key in baseline_keys if key not in current_map and key not in ignored]
    shared_keys = [key for key in current_keys if key in baseline_map and key not in ignored]

    lines: list[str] = []

    metadata_changes = []
    for key in shared_keys:
        c_meta = _image_metadata_payload(current_map[key])
        b_meta = _image_metadata_payload(baseline_map[key])
        if c_meta != b_meta:
            changed_fields = [k for k, v in c_meta.items() if v != b_meta.get(k)]
            label = _image_label(current_map[key])
            friendly_fields = ', '.join(_format_image_metadata_field_label(field) for field in changed_fields)
            metadata_changes.append(f"{label} changed: {friendly_fields}")

    if added:
        labels = ", ".join(_image_label(row) for row in added[:3])
        if len(added) > 3:
            labels += ", …"
        lines.append(f'Images added since last sync: {labels}')
    if removed:
        labels = ", ".join(_image_label(row) for row in removed[:3])
        if len(removed) > 3:
            labels += ", …"
        lines.append(f'Images removed since last sync: {labels}')
    if metadata_changes:
        for mc in metadata_changes[:5]:
            lines.append(mc)
        if len(metadata_changes) > 5:
            lines.append(f"...and {len(metadata_changes) - 5} more metadata changes")
    if not lines and len(current) != len(baseline) and not ignored:
        lines.append(f'Image count changed since last sync: {len(baseline)} -> {len(current)}')
    return lines


def _normalize_observation_sync_field(field: str) -> str:
    normalized = str(field or '').strip()
    if normalized in {'visibility', 'sharing_scope'}:
        return 'sharing_scope'
    return normalized


def _format_observation_metadata_field_label(field: str) -> str:
    labels = {
        'date': 'date',
        'genus': 'genus',
        'species': 'species',
        'common_name': 'common name',
        'species_guess': 'species guess',
        'uncertain': 'uncertain',
        'unspontaneous': 'unspontaneous',
        'determination_method': 'determination method',
        'location': 'location',
        'gps_latitude': 'latitude',
        'gps_longitude': 'longitude',
        'location_public': 'location public',
        'is_draft': 'draft flag',
        'location_precision': 'location precision',
        'ai_selected_service': 'AI service',
        'ai_selected_taxon_id': 'AI taxon id',
        'ai_selected_scientific_name': 'AI scientific name',
        'ai_selected_probability': 'AI probability',
        'ai_selected_at': 'AI selected at',
        'habitat': 'habitat',
        'habitat_nin2_path': 'NIN2 path',
        'habitat_substrate_path': 'substrate path',
        'habitat_host_genus': 'host genus',
        'habitat_host_species': 'host species',
        'habitat_host_common_name': 'host common name',
        'habitat_nin2_note': 'NIN2 note',
        'habitat_substrate_note': 'substrate note',
        'habitat_grows_on_note': 'grows-on note',
        'notes': 'notes',
        'open_comment': 'open comment',
        'interesting_comment': 'interesting comment',
        'publish_target': 'publish target',
        'artsdata_id': 'Artsobs id',
        'artportalen_id': 'Artportalen id',
        'inaturalist_id': 'iNaturalist id',
        'mushroomobserver_id': 'Mushroom Observer id',
        'spore_data_visibility': 'spore visibility',
        'visibility': 'visibility',
        'sharing_scope': 'sharing scope',
    }
    normalized = str(field or '').strip()
    return labels.get(normalized, normalized.replace('_', ' '))


def _analyze_observation_field_changes(local_obs: dict | None, remote_obs: dict | None, baseline_obs: dict | None) -> dict:
    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    baseline_payload = _baseline_observation_compare_payload(baseline_obs)
    remote_only_fields: list[str] = []
    local_only_fields: list[str] = []
    conflict_fields: list[str] = []
    shared_same_fields: list[str] = []

    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        baseline_value = baseline_payload.get(field)
        local_value = local_payload.get(field)
        remote_value = remote_payload.get(field)
        local_changed = not _observation_field_values_match(field, local_value, baseline_value)
        remote_changed = not _observation_field_values_match(field, remote_value, baseline_value)
        if local_changed and remote_changed:
            if _observation_field_values_match(field, local_value, remote_value):
                shared_same_fields.append(field)
            else:
                conflict_fields.append(field)
        elif local_changed:
            local_only_fields.append(field)
        elif remote_changed:
            remote_only_fields.append(field)

    return {
        'local_payload': local_payload,
        'remote_payload': remote_payload,
        'baseline_payload': baseline_payload,
        'local_only_fields': local_only_fields,
        'remote_only_fields': remote_only_fields,
        'conflict_fields': conflict_fields,
        'shared_same_fields': shared_same_fields,
    }


def _analyze_image_changes(
    current_images: list[dict],
    baseline_images: list[dict],
    *,
    ignored_keys: set[str] | None = None,
) -> dict:
    current = [dict(row or {}) for row in (current_images or [])]
    baseline = [dict(row or {}) for row in (baseline_images or [])]
    ignored = {str(key or '').strip() for key in (ignored_keys or set()) if str(key or '').strip()}
    current_keys = [_image_compare_key(row) for row in current]
    baseline_keys = [_image_compare_key(row) for row in baseline]
    current_map = {_image_compare_key(row): row for row in current}
    baseline_map = {_image_compare_key(row): row for row in baseline}

    added_keys = [key for key in current_keys if key not in baseline_map and key not in ignored]
    removed_keys = [key for key in baseline_keys if key not in current_map and key not in ignored]
    shared_keys = [key for key in current_keys if key in baseline_map and key not in ignored]
    metadata_changed_keys = [
        key
        for key in shared_keys
        if _image_metadata_payload(current_map[key]) != _image_metadata_payload(baseline_map[key])
    ]

    return {
        'added_keys': added_keys,
        'removed_keys': removed_keys,
        'metadata_changed_keys': metadata_changed_keys,
        'order_changed': False,
        'added': [current_map[key] for key in added_keys],
        'removed': [baseline_map[key] for key in removed_keys],
        'changed': bool(added_keys or removed_keys or metadata_changed_keys),
    }


def _remaining_local_changes_after_remote_merge(
    field_changes: dict,
    *,
    local_media_changed: bool,
) -> bool:
    return bool(field_changes.get('local_only_fields') or field_changes.get('conflict_fields') or local_media_changed)


def _format_review_needed_error(local_id: int, cloud_id: str, reasons: list[str] | None = None) -> str:
    reason_text = ', '.join(str(reason or '').strip() for reason in (reasons or []) if str(reason or '').strip())
    base = (
        f"cloud {str(cloud_id or '').strip() or '?'}: needs review before applying remaining "
        f"cloud changes to local observation {int(local_id)}"
    )
    return f'{base} ({reason_text})' if reason_text else base


def _local_has_real_changes_since_snapshot(local_obs: dict, cloud_id: str | None = None) -> bool:
    cloud_value = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not cloud_value:
        return True
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    local_payload = _observation_compare_payload(local_obs, local=True)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if not _observation_field_values_match(field, local_payload.get(field), baseline_obs.get(field)):
            return True

    local_id = _safe_int(local_obs.get('id'))
    if local_id <= 0:
        return False
    stored_media_sig = _load_local_cloud_media_signature(local_id)
    if not stored_media_sig:
        return True
    current_media_sig = _local_cloud_media_signature(local_id)
    if not current_media_sig:
        return False
    if not _local_media_signatures_match(stored_media_sig, current_media_sig):
        return True
    _store_local_media_signature_if_equivalent(local_id, stored_media_sig, current_media_sig)

    try:
        _, local_measurements_by_id = _load_local_measurement_lookup(local_id)
    except Exception:
        return True
    local_measurement_payloads = [
        _local_measurement_snapshot_payload(row)
        for row in (local_measurements_by_id.values() if local_measurements_by_id else [])
    ]
    baseline_measurements = [dict(row or {}) for row in (snapshot.get('measurements') or [])]
    if _analyze_measurement_changes(local_measurement_payloads, baseline_measurements).get('changed'):
        return True
    return False


def _clear_observation_dirty_if_no_real_changes(local_id: int, cloud_id: str) -> bool:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        return False
    if _local_has_real_changes_since_snapshot(local_obs, cloud_id):
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='synced',
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()
    return True


def _progress_done(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('done', 0) or 0))
    except Exception:
        return 0


def _progress_total(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('total', 0) or 0))
    except Exception:
        return 0


def _trace_progress_gap(message: str) -> None:
    """Log when a long backend step elapsed between two UI progress updates.

    The UI only shows the *last* emitted message. If a slow step runs while that
    message stays on screen (e.g. the bar appears frozen on "Checking
    calibration 4/8"), the gap is logged here naming both messages so the pause
    can be traced to the actual backend work, even when that work emits no
    progress text of its own.
    """
    trace = _cloud_sync_progress_trace()
    if not isinstance(trace, dict):
        return
    now = _cloud_sync_perf_counter()
    last_t = trace.get('last_t')
    last_msg = trace.get('last_msg')
    start = trace.get('start', now)
    if last_t is not None:
        gap = now - last_t
        if gap >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
            print(
                f"[cloud_sync] progress gap: {gap * 1000:.0f}ms with no UI update "
                f"(stuck showing \"{last_msg}\") before \"{message}\" "
                f"at +{(now - start):.1f}s into sync",
                flush=True,
            )
    trace['last_t'] = now
    trace['last_msg'] = message


def _emit_progress(
    progress_cb: ProgressCallback | None,
    message: str,
    progress_state: dict | None,
) -> None:
    _trace_progress_gap(message)
    if callable(progress_cb):
        progress_cb(message, _progress_done(progress_state), max(1, _progress_total(progress_state)))


def _advance_progress(
    progress_state: dict | None,
    amount: int = 1,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state) + increment
    state['total'] = _progress_total(state)
    return _progress_done(state), _progress_total(state)


def _extend_progress_total(
    progress_state: dict | None,
    amount: int,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state)
    state['total'] = _progress_total(state) + increment
    return _progress_done(state), _progress_total(state)


_SYNC_SUMMARY_KEYS = (
    'observations_checked',
    'observations_redirtied_pending_local_images',
    'observations_patched',
    'observations_skipped_noop',
    'observations_deleted_remote',
    'images_checked',
    'images_prepared_local',
    'images_uploaded',
    'images_skipped_already_synced',
    'images_cloud_id_repaired',
    'images_deleted_remote',
    'measurements_checked',
    'measurements_patched',
    'measurements_skipped_noop',
    'calibrations_pushed',
    'calibrations_pulled',
    'calibrations_skipped_noop',
    'calibrations_conflicts',
    'calibration_reference_images_uploaded',
    'calibration_remote_lookups',
    'storage_quota_delta_rpc_calls',
    'remote_media_downloads',
    'remote_media_materializations',
)


def _new_sync_summary() -> dict[str, int]:
    return {key: 0 for key in _SYNC_SUMMARY_KEYS}


def _sync_summary_value(sync_summary: dict | None, key: str) -> int:
    try:
        return max(0, int((sync_summary or {}).get(key, 0) or 0))
    except Exception:
        return 0


def _increment_sync_summary(sync_summary: dict | None, key: str, amount: int = 1) -> None:
    if not isinstance(sync_summary, dict):
        return
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    if increment <= 0:
        return
    sync_summary[key] = _sync_summary_value(sync_summary, key) + increment


def format_sync_summary(sync_summary: dict | None) -> str | None:
    summary = dict(sync_summary or {})
    if not summary:
        return None

    lines: list[str] = []

    observation_bits = []
    observations_checked = _sync_summary_value(summary, 'observations_checked')
    if observations_checked:
        observation_bits.append(f'{observations_checked} checked')
    observations_redirtied = _sync_summary_value(summary, 'observations_redirtied_pending_local_images')
    if observations_redirtied:
        observation_bits.append(f'{observations_redirtied} re-dirtied due to pending local images')
    observations_patched = _sync_summary_value(summary, 'observations_patched')
    if observations_patched:
        observation_bits.append(f'{observations_patched} patched')
    observations_noop = _sync_summary_value(summary, 'observations_skipped_noop')
    if observations_noop:
        observation_bits.append(f'{observations_noop} skipped as no-op')
    observations_deleted = _sync_summary_value(summary, 'observations_deleted_remote')
    if observations_deleted:
        observation_bits.append(f'{observations_deleted} deleted remotely')
    if observation_bits:
        lines.append(f"Observations: {'; '.join(observation_bits)}.")

    image_bits = []
    images_checked = _sync_summary_value(summary, 'images_checked')
    if images_checked:
        image_bits.append(f'{images_checked} checked')
    images_prepared = _sync_summary_value(summary, 'images_prepared_local')
    if images_prepared:
        image_bits.append(f'{images_prepared} prepared for upload')
    images_uploaded = _sync_summary_value(summary, 'images_uploaded')
    if images_uploaded:
        image_bits.append(f'{images_uploaded} uploaded')
    images_skipped = _sync_summary_value(summary, 'images_skipped_already_synced')
    if images_skipped:
        image_bits.append(f'{images_skipped} skipped as already synced')
    images_repaired = _sync_summary_value(summary, 'images_cloud_id_repaired')
    if images_repaired:
        image_bits.append(f'{images_repaired} cloud_id associations repaired')
    images_deleted = _sync_summary_value(summary, 'images_deleted_remote')
    if images_deleted:
        image_bits.append(f'{images_deleted} deleted remotely')
    if image_bits:
        lines.append(f"Images: {'; '.join(image_bits)}.")

    measurement_bits = []
    measurements_checked = _sync_summary_value(summary, 'measurements_checked')
    if measurements_checked:
        measurement_bits.append(f'{measurements_checked} checked')
    measurements_patched = _sync_summary_value(summary, 'measurements_patched')
    if measurements_patched:
        measurement_bits.append(f'{measurements_patched} patched')
    measurements_noop = _sync_summary_value(summary, 'measurements_skipped_noop')
    if measurements_noop:
        measurement_bits.append(f'{measurements_noop} skipped as no-op')
    if measurement_bits:
        lines.append(f"Measurements: {'; '.join(measurement_bits)}.")

    calibration_bits = []
    calibrations_pushed = _sync_summary_value(summary, 'calibrations_pushed')
    if calibrations_pushed:
        calibration_bits.append(f'{calibrations_pushed} pushed')
    calibrations_pulled = _sync_summary_value(summary, 'calibrations_pulled')
    if calibrations_pulled:
        calibration_bits.append(f'{calibrations_pulled} pulled')
    calibrations_noop = _sync_summary_value(summary, 'calibrations_skipped_noop')
    if calibrations_noop:
        calibration_bits.append(f'{calibrations_noop} skipped as no-op')
    calibrations_conflicts = _sync_summary_value(summary, 'calibrations_conflicts')
    if calibrations_conflicts:
        calibration_bits.append(f'{calibrations_conflicts} conflicts')
    calibration_reference_uploads = _sync_summary_value(summary, 'calibration_reference_images_uploaded')
    if calibration_reference_uploads:
        calibration_bits.append(f'{calibration_reference_uploads} reference image(s) uploaded')
    if calibration_bits:
        lines.append(f"Calibrations: {'; '.join(calibration_bits)}.")

    storage_quota_delta_calls = _sync_summary_value(summary, 'storage_quota_delta_rpc_calls')
    if storage_quota_delta_calls:
        lines.append(f'Storage quota delta RPC calls: {storage_quota_delta_calls}.')

    remote_downloads = _sync_summary_value(summary, 'remote_media_downloads')
    remote_materializations = _sync_summary_value(summary, 'remote_media_materializations')
    if remote_downloads or remote_materializations:
        lines.append(
            'Remote media downloads/materializations: '
            f'{remote_downloads} downloads; {remote_materializations} materializations.'
        )

    return '\n'.join(lines) if lines else None


def summarize_sync_change_activity(result: dict | None) -> dict:
    """Classify a sync result into real changes vs. checked/no-op/local-only work.

    The push phase walks every observation whose row is dirty or has no cloud_id
    and counts each as "pushed" even when the upsert was a no-op (e.g. an
    observation re-dirtied only because a local image row was re-associated to an
    existing cloud image). The user-facing notification must reflect *real*
    remote-facing or local changes, not the raw dirty-scan count, otherwise a
    no-change sync wrongly reports that an observation was synced.

    Returns a dict with explicit counters plus ``any_real_change``:
      - real remote change: observation/measurement metadata written, image bytes
        uploaded or deleted remotely, calibration pushed / reference image uploaded.
      - real local change: observation/calibration pulled, remote media downloaded
        or materialized locally.
    Local-only cloud_id repairs (``images_cloud_id_repaired``) and pure no-op /
    checked counts are reported but excluded from ``any_real_change``.
    """
    data = dict(result or {})
    summary = data.get('sync_summary') or {}

    def _value(key: str) -> int:
        return _sync_summary_value(summary, key)

    def _result_int(key: str) -> int:
        try:
            return max(0, int(data.get(key, 0) or 0))
        except Exception:
            return 0

    observations_metadata_patched = _value('observations_patched')
    observations_checked = _value('observations_checked')
    observations_checked_noop = _value('observations_skipped_noop')
    observations_deleted_remote = _value('observations_deleted_remote')
    images_uploaded = _value('images_uploaded')
    images_deleted_remote = _value('images_deleted_remote')
    images_repaired_local_only = _value('images_cloud_id_repaired')
    measurements_patched = _value('measurements_patched')
    calibrations_pushed = _value('calibrations_pushed')
    calibration_reference_images_uploaded = _value('calibration_reference_images_uploaded')
    calibrations_pulled = _value('calibrations_pulled')
    remote_media_downloads = _value('remote_media_downloads')
    remote_media_materializations = _value('remote_media_materializations')
    # ``pulled`` is the count of observations pulled into the local DB; fall back
    # to the summary count is not tracked separately, so use the result value.
    observations_pulled = _result_int('pulled')
    deleted_remote_rows = len(data.get('deleted_remote') or [])

    real_remote_change = (
        observations_metadata_patched
        + images_uploaded
        + images_deleted_remote
        + measurements_patched
        + calibrations_pushed
        + calibration_reference_images_uploaded
        + observations_deleted_remote
    )
    real_local_change = (
        observations_pulled
        + calibrations_pulled
        + remote_media_downloads
        + remote_media_materializations
    )
    # ``deleted_remote_rows`` (cloud observations deleted elsewhere, awaiting local
    # review) is surfaced by its own notification branch, so it is reported here
    # but not folded into ``any_real_change``.
    any_real_change = bool(real_remote_change or real_local_change)

    return {
        'observations_metadata_patched': observations_metadata_patched,
        'observations_checked': observations_checked,
        'observations_checked_noop': observations_checked_noop,
        'observations_images_repaired_local_only': images_repaired_local_only,
        'observations_pulled': observations_pulled,
        'images_uploaded': images_uploaded,
        'images_deleted_remote': images_deleted_remote,
        'measurements_patched': measurements_patched,
        'calibrations_pushed': calibrations_pushed,
        'calibrations_pulled': calibrations_pulled,
        'calibration_reference_images_uploaded': calibration_reference_images_uploaded,
        'remote_media_downloads': remote_media_downloads,
        'remote_media_materializations': remote_media_materializations,
        'deleted_remote_rows': deleted_remote_rows,
        'real_remote_change': real_remote_change,
        'real_local_change': real_local_change,
        'any_real_change': any_real_change,
    }


def _observation_display_name(obs: dict | None) -> str:
    record = obs or {}
    parts = [
        str(record.get('genus') or '').strip(),
        str(record.get('species') or '').strip(),
    ]
    name = " ".join(part for part in parts if part).strip()
    if name:
        return name
    species_guess = str(record.get('species_guess') or '').strip()
    if species_guess:
        return species_guess
    location = str(record.get('location') or '').strip()
    if location:
        return location
    obs_id = str(record.get('id') or '').strip()
    return f'observation {obs_id}' if obs_id else 'observation'


def _cloud_media_signature() -> str:
    snapshot = {
        'include_annotations': str(SettingsDB.get_setting(_SETTING_INCLUDE_ANNOTATIONS, '0') or '0').strip(),
        'show_scale_bar': str(SettingsDB.get_setting(_SETTING_SHOW_SCALE_BAR, '0') or '0').strip(),
        'include_measure_plots': str(SettingsDB.get_setting(_SETTING_INCLUDE_MEASURE_PLOTS, '0') or '0').strip(),
        'include_thumbnail_gallery': str(SettingsDB.get_setting(_SETTING_INCLUDE_THUMBNAIL_GALLERY, '0') or '0').strip(),
        'include_plate': str(SettingsDB.get_setting(_SETTING_INCLUDE_PLATE, '0') or '0').strip(),
        'include_copyright': str(SettingsDB.get_setting(_SETTING_INCLUDE_COPYRIGHT, '0') or '0').strip(),
        'image_license': str(SettingsDB.get_setting(_SETTING_IMAGE_LICENSE, '60') or '60').strip(),
        'profile_name': str(SettingsDB.get_setting(_SETTING_PROFILE_NAME, '') or '').strip(),
        'profile_email': str(SettingsDB.get_setting(_SETTING_PROFILE_EMAIL, '') or '').strip(),
    }
    return json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _cloud_observation_snapshot_key(cloud_id: str) -> str:
    return f"{_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX}{str(cloud_id or '').strip()}"


def _cloud_image_file_signature_key(observation_id: int | str, image_id: int | str) -> str:
    return (
        f"{_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX}"
        f"{str(observation_id or '').strip()}_{str(image_id or '').strip()}"
    )


def _cloud_local_media_signature_key(observation_id: int | str) -> str:
    return f"{_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX}{str(observation_id or '').strip()}"


def _normalize_snapshot_value(value):
    if isinstance(value, bool):
        return bool(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): _normalize_snapshot_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_snapshot_value(v) for v in value]
    return str(value)


def _is_generated_cloud_image(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    notes = str(row.get('notes') or '').strip().lower()
    filename = str(row.get('original_filename') or '').strip().lower()
    desktop_id = _safe_int(row.get('desktop_id'))
    if notes.startswith('generated media'):
        return True
    if filename.startswith('cloud_extra_'):
        return True
    if desktop_id < 0:
        return True
    return False


def should_push_local_image_to_cloud(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if _is_generated_cloud_image(row):
        return False
    source_role = str(row.get('source_role') or '').strip().lower()
    file_purpose = str(row.get('file_purpose') or '').strip().lower()
    if source_role == 'cloud_recovery_cache' or file_purpose == 'cache':
        # Cloud-imported rows are cached locally, but they are still the
        # canonical syncable image rows and must remain publishable so the
        # cloud copy can be preserved or restored.
        return True
    return True


def should_pull_cloud_image_to_desktop(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    return not _is_generated_cloud_image(row)


def _cloud_observation_snapshot(
    remote: dict,
    remote_images: list[dict],
    remote_measurements: list[dict] | None = None,
) -> str:
    obs_part = {
        field: _normalize_snapshot_value((remote or {}).get(field))
        for field in _SNAPSHOT_OBS_FIELDS
    }
    images_part = []
    filtered_images = [
        dict(row or {})
        for row in (remote_images or [])
        if should_pull_cloud_image_to_desktop(row)
    ]
    for image in sorted(filtered_images, key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))):
        image_payload = {
            field: _normalize_snapshot_value(image.get(field))
            for field in _SNAPSHOT_IMG_FIELDS
        }
        for field in _SNAPSHOT_IMG_PASSIVE_FIELDS:
            passive_value = _normalize_cloud_media_key(image.get(field))
            if passive_value:
                image_payload[field] = _normalize_snapshot_value(passive_value)
        images_part.append(image_payload)
    measurements_part = []
    filtered_measurements = [dict(row or {}) for row in (remote_measurements or [])]
    for measurement in sorted(
        filtered_measurements,
        key=lambda row: (
            str(row.get('image_id') or ''),
            _safe_int(row.get('desktop_id')),
            str(row.get('id') or ''),
        ),
    ):
        measurements_part.append(
            {
                field: _normalize_snapshot_value(measurement.get(field))
                for field in _SNAPSHOT_MEAS_FIELDS
            }
        )
    payload = {'observation': obs_part, 'images': images_part, 'measurements': measurements_part}
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _load_cloud_observation_snapshot(cloud_id: str) -> str:
    raw = str(SettingsDB.get_setting(_cloud_observation_snapshot_key(cloud_id), '') or '').strip()
    if not raw:
        return ''
    parsed = _parse_cloud_observation_snapshot(raw)
    if not parsed:
        return raw
    return json.dumps(parsed, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _store_cloud_observation_snapshot(cloud_id: str, snapshot: str) -> None:
    if not str(cloud_id or '').strip():
        return
    normalized = str(snapshot or '').strip()
    if normalized:
        parsed = _parse_cloud_observation_snapshot(normalized)
        if parsed:
            normalized = json.dumps(parsed, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    SettingsDB.set_setting(_cloud_observation_snapshot_key(cloud_id), normalized)


def _load_cloud_image_file_signature(observation_id: int | str, image_id: int | str) -> str:
    return str(
        SettingsDB.get_setting(_cloud_image_file_signature_key(observation_id, image_id), '') or ''
    ).strip()


def _store_cloud_image_file_signature(
    observation_id: int | str,
    image_id: int | str,
    signature: str,
) -> None:
    SettingsDB.set_setting(
        _cloud_image_file_signature_key(observation_id, image_id),
        str(signature or '').strip(),
    )


def _clear_cloud_image_file_signature(observation_id: int | str, image_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_image_file_signature_key(observation_id, image_id), '')


def _reconcile_local_image_cloud_id(
    local_image_id: int | str,
    cloud_image_id: str,
    *,
    mark_synced: bool = False,
) -> bool:
    """Persist ``cloud_image_id`` onto the local images row.

    Returns ``True`` when the row was updated because its ``cloud_id`` was
    missing or stale. This makes metadata-only associations of an existing
    remote cloud image persistent: previously-synced local rows whose
    ``cloud_id`` was lost get re-linked instead of being re-dirtied on every
    sync. No image bytes are uploaded here — only the local link is restored.
    """
    try:
        image_id = int(local_image_id)
    except Exception:
        image_id = 0
    cloud_image_id = str(cloud_image_id or '').strip()
    if image_id <= 0 or not cloud_image_id:
        return False
    conn = get_connection()
    try:
        row = conn.execute('SELECT cloud_id FROM images WHERE id = ?', (image_id,)).fetchone()
        if row is None:
            return False
        existing = str((row['cloud_id'] if isinstance(row, sqlite3.Row) else row[0]) or '').strip()
        if existing == cloud_image_id:
            return False
        if mark_synced:
            conn.execute(
                'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                (cloud_image_id, datetime.now(timezone.utc).isoformat(), image_id),
            )
        else:
            conn.execute(
                'UPDATE images SET cloud_id = ? WHERE id = ?',
                (cloud_image_id, image_id),
            )
        conn.commit()
        return True
    finally:
        conn.close()


def _local_tombstoned_cloud_image_ids(cloud_image_ids: list[str] | tuple[str, ...] | set[str] | None = None) -> set[str]:
    tombstones = get_image_tombstones_by_deleted_cloud_id(cloud_image_ids)
    return set(tombstones.keys())


def _local_tombstoned_local_image_ids(local_image_ids: list[int] | tuple[int, ...] | set[int] | None = None) -> set[int]:
    tombstones = get_image_tombstones_by_local_image_id(local_image_ids)
    return set(tombstones.keys())


def _pull_remote_images_for_sync(client: "SporelyCloudClient", cloud_id: str) -> list[dict]:
    """Fetch cloud image rows including deleted ones so tombstones can be recorded."""
    cloud_value = str(cloud_id or '').strip()
    if not cloud_value:
        return []
    return [
        dict(row or {})
        for row in (client.pull_image_metadata(cloud_value, include_deleted_for_sync=True) or [])
    ]


def _record_remote_image_tombstones(
    remote_images,
    *,
    local_observation_id: int | None = None,
    cloud_observation_id: str | None = None,
) -> set[str]:
    # Option A: keep the local active image row visible for now.
    # Recording the tombstone is enough to block reupload/recreation; local
    # hiding/deletion and any explicit confirmation flow stay deferred.
    rows = [dict(row or {}) for row in (remote_images or [])]
    tombstone_rows = [
        row
        for row in rows
        if str(row.get("id") or "").strip() and str(row.get("deleted_at") or "").strip()
    ]
    if not tombstone_rows:
        return set()

    tombstone_cloud_ids = [
        str(row.get("id") or "").strip()
        for row in tombstone_rows
        if str(row.get("id") or "").strip()
    ]
    existing_tombstones = get_image_tombstones_by_deleted_cloud_id(tombstone_cloud_ids)
    new_tombstone_cloud_ids = [
        cloud_id
        for cloud_id in dict.fromkeys(tombstone_cloud_ids)
        if cloud_id not in existing_tombstones
    ]
    _increment_sync_summary(_cloud_sync_current_summary(), 'images_deleted_remote', len(new_tombstone_cloud_ids))
    deleted_cloud_ids: set[str] = set()
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA table_info(images)")
            image_columns = {str(row[1] or "") for row in cursor.fetchall()}
        except Exception:
            image_columns = set()

        has_cloud_id = "cloud_id" in image_columns
        select_columns = ["id"]
        for column in ("observation_id", "image_type", "filepath", "original_filepath"):
            if column in image_columns:
                select_columns.append(column)

        local_image_sql = (
            f"SELECT {', '.join(select_columns)} FROM images WHERE cloud_id = ? LIMIT 1"
            if has_cloud_id
            else None
        )
        local_desktop_image_sql = (
            f"SELECT {', '.join(select_columns)} FROM images WHERE id = ? LIMIT 1"
        )

        for remote_image in tombstone_rows:
            cloud_image_id = str(remote_image.get("id") or "").strip()
            deleted_at = str(remote_image.get("deleted_at") or "").strip()
            resolved_local_observation_id = None
            if local_observation_id is not None:
                local_observation_id_value = _safe_int(local_observation_id)
                if local_observation_id_value > 0:
                    resolved_local_observation_id = local_observation_id_value
            local_image_row = None
            if local_image_sql:
                local_image_row = cursor.execute(local_image_sql, (cloud_image_id,)).fetchone()
            if local_image_row is None:
                desktop_image_id = _safe_int(remote_image.get("desktop_id"))
                if desktop_image_id > 0:
                    local_image_row = cursor.execute(
                        local_desktop_image_sql,
                        (desktop_image_id,),
                    ).fetchone()

            local_image_id = None
            image_type = None
            filepath = None
            original_filepath = None
            if local_image_row:
                local_image_data = dict(local_image_row)
                local_image_id_value = _safe_int(local_image_data.get("id"))
                if local_image_id_value > 0:
                    local_image_id = local_image_id_value
                if resolved_local_observation_id is None and "observation_id" in local_image_data:
                    local_observation_id_value = _safe_int(local_image_data.get("observation_id"))
                    if local_observation_id_value > 0:
                        resolved_local_observation_id = local_observation_id_value
                image_type = str(local_image_data.get("image_type") or "").strip() or None
                filepath = str(local_image_data.get("filepath") or "").strip() or None
                original_filepath = str(local_image_data.get("original_filepath") or "").strip() or None

            _upsert_image_tombstone(
                cursor,
                deleted_cloud_id=cloud_image_id,
                deleted_at=deleted_at,
                deleted_storage_path=_normalize_cloud_media_key(remote_image.get("storage_path")) or None,
                deleted_observation_cloud_id=(
                    str(cloud_observation_id or remote_image.get("observation_id") or "").strip() or None
                ),
                local_observation_id=resolved_local_observation_id,
                local_image_id=local_image_id,
                image_type=image_type,
                filepath=filepath,
                original_filepath=original_filepath,
            )
            deleted_cloud_ids.add(cloud_image_id)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return deleted_cloud_ids


def _tombstoned_cloud_image_warning(local_id: int | None, cloud_image_id: str) -> str:
    return f"obs {int(local_id or 0)}: skipped cloud image {cloud_image_id} because it has a local tombstone"


def _remote_images_missing_locally(local_id: int, remote_images: list[dict] | None) -> list[dict]:
    """Return cloud images that should exist locally but are missing or unreadable."""
    pullable_remote_images = [
        dict(row or {})
        for row in (remote_images or [])
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
        and str(row.get('id') or '').strip()
    ]
    if not pullable_remote_images:
        return []

    local_images = ImageDB.get_images_for_observation(int(local_id))
    local_cloud_map = {
        str(img.get('cloud_id') or '').strip(): img
        for img in local_images
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('cloud_id') or '').strip()
    }
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in pullable_remote_images]
    )

    missing_remote_images: list[dict] = []
    for remote_image in pullable_remote_images:
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id or cloud_image_id in tombstoned_cloud_ids:
            continue
        local_image = local_cloud_map.get(cloud_image_id)
        if local_image is None:
            missing_remote_images.append(remote_image)
            continue
        if _resolve_existing_local_image_asset_path(local_image.get('filepath')) is None:
            missing_remote_images.append(remote_image)
    return missing_remote_images


def _push_pending_image_tombstones(client: "SporelyCloudClient") -> list[str]:
    warnings: list[str] = []
    for tombstone in list_pending_image_tombstones():
        cloud_image_id = str(tombstone.get('deleted_cloud_id') or '').strip()
        if not cloud_image_id:
            continue
        deleted_at = str(tombstone.get('deleted_at') or '').strip() or datetime.now(timezone.utc).isoformat()
        try:
            client.soft_delete_image(cloud_image_id, deleted_at)
        except Exception as exc:
            warning = (
                f"obs {int(tombstone.get('local_observation_id') or 0)}: "
                f"could not sync cloud image tombstone {cloud_image_id}: {exc}"
            )
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        try:
            mark_image_tombstone_synced(cloud_image_id)
        except Exception as exc:
            warning = (
                f"obs {int(tombstone.get('local_observation_id') or 0)}: "
                f"synced cloud image tombstone {cloud_image_id} but could not mark it locally: {exc}"
            )
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
    return warnings


def _load_local_cloud_media_signature(observation_id: int | str) -> str:
    return str(SettingsDB.get_setting(_cloud_local_media_signature_key(observation_id), '') or '').strip()


def _store_local_cloud_media_signature(observation_id: int | str, signature: str) -> None:
    SettingsDB.set_setting(
        _cloud_local_media_signature_key(observation_id),
        str(signature or '').strip(),
    )


def _pull_remote_measurements_for_images(
    client: "SporelyCloudClient",
    image_cloud_ids: list[str],
) -> list[dict]:
    fetcher = getattr(client, 'pull_measurements_for_images', None)
    if not callable(fetcher):
        return []
    rows = fetcher(image_cloud_ids)
    return [dict(row or {}) for row in (rows or [])]


def _group_remote_measurements_by_observation(
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None,
) -> dict[str, list[dict]]:
    image_to_obs: dict[str, str] = {}
    for image_row in (remote_images or []):
        cloud_image_id = str(image_row.get('id') or '').strip()
        cloud_obs_id = str(image_row.get('observation_id') or '').strip()
        if cloud_image_id and cloud_obs_id:
            image_to_obs[cloud_image_id] = cloud_obs_id
    grouped: dict[str, list[dict]] = {}
    for measurement_row in (remote_measurements or []):
        cloud_image_id = str(measurement_row.get('image_id') or '').strip()
        cloud_obs_id = image_to_obs.get(cloud_image_id)
        if not cloud_obs_id:
            continue
        grouped.setdefault(cloud_obs_id, []).append(dict(measurement_row or {}))
    for rows in grouped.values():
        rows.sort(
            key=lambda row: (
                str(row.get('image_id') or ''),
                _safe_int(row.get('desktop_id')),
                str(row.get('id') or ''),
            )
        )
    return grouped


def sync_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    materialize_remote_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
) -> dict:
    """Run a full bidirectional sync: push local changes then pull remote ones."""
    profiler = CloudSyncProfiler() if _cloud_sync_profile_enabled() else None
    profile_token = None
    if profiler is not None:
        profile_token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
    sync_summary = _new_sync_summary()
    summary_token = _CLOUD_SYNC_SUMMARY_CONTEXT.set(sync_summary)
    progress_trace = {'start': _cloud_sync_perf_counter(), 'last_t': None, 'last_msg': None}
    progress_trace_token = _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.set(progress_trace)

    sync_error: Exception | None = None
    result: dict | None = None
    try:
        # Safety check: ensure this DB belongs to the current user
        with _cloud_sync_phase_scope(profiler, 'ensure_database_linked_to_cloud_user'):
            ensure_database_linked_to_cloud_user(client)

        # Initialize a shared progress state to keep the bar moving smoothly across both phases
        progress_state = {'done': 0, 'total': 0}
        _emit_progress(progress_cb, "Connecting to Sporely Cloud...", progress_state)

        # Pre-fetch remote metadata once to reuse in both phases
        _emit_progress(progress_cb, "Loading cloud observations…", progress_state)
        with _cloud_sync_phase_scope(profiler, 'list_remote_observations'):
            remote_obs = client.list_remote_observations()
        _emit_progress(progress_cb, "Loading cloud calibrations…", progress_state)
        with _cloud_sync_phase_scope(profiler, 'list_remote_calibrations'):
            remote_calibrations = client.list_remote_calibrations()

        with _cloud_sync_phase_scope(profiler, 'push_calibrations'):
            calibration_push_result = push_calibrations(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_calibrations=remote_calibrations,
            )

        # Phase 1: Push local edits to the cloud
        with _cloud_sync_phase_scope(profiler, 'push_all'):
            push_result = push_all(
                client,
                progress_cb=progress_cb,
                sync_images=sync_images,
                prepare_images_cb=prepare_images_cb,
                progress_state=progress_state,
                remote_obs=remote_obs,
                sync_calibrations=False,
            )

        # Refresh remote observations after the push phase so pull-side
        # comparisons see the cloud state that now includes any local metadata
        # edits we just pushed. This network round-trip runs before the first
        # pull-side progress update, so announce it and time it.
        _emit_progress(progress_cb, "Loading cloud observations…", progress_state)
        refresh_start = _cloud_sync_perf_counter()
        with _cloud_sync_phase_scope(profiler, 'refresh_remote_observations_after_push'):
            remote_obs = client.list_remote_observations()
        refresh_elapsed = _cloud_sync_perf_counter() - refresh_start
        print(
            f"[cloud_sync] observation preflight: remote observations refreshed "
            f"count={len(remote_obs or [])} duration={refresh_elapsed * 1000:.0f}ms",
            flush=True,
        )

        # Phase 2: Pull cloud edits to the desktop
        with _cloud_sync_phase_scope(profiler, 'pull_all'):
            pull_result = pull_all(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_obs=remote_obs,
                sync_calibrations=False,
                materialize_remote_images=materialize_remote_images,
            )

        with _cloud_sync_phase_scope(profiler, 'pull_calibrations'):
            calibration_pull_result = pull_calibrations(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_calibrations=remote_calibrations,
            )

        # Leave the UI on a neutral phase rather than the last per-calibration
        # message while the worker finishes and the table refreshes.
        _emit_progress(progress_cb, "Finalizing cloud sync…", progress_state)
        print(
            f"[cloud_sync] sync progress complete "
            f"duration={(_cloud_sync_perf_counter() - progress_trace['start']) * 1000:.0f}ms",
            flush=True,
        )

        # Combine results for the UI summary
        result = {
            'pushed': push_result.get('pushed', 0),
            'pulled': pull_result.get('pulled', 0),
            'calibrations_pushed': calibration_push_result.get('pushed', 0),
            'calibrations_pulled': calibration_pull_result.get('pulled', 0),
            'errors': (
                calibration_push_result.get('errors', [])
                + push_result.get('errors', [])
                + pull_result.get('errors', [])
                + calibration_pull_result.get('errors', [])
            ),
            'deleted_remote': pull_result.get('deleted_remote', []),
        }
        original_sync = push_result.get('original_sync')
        if original_sync is not None:
            result['original_sync'] = original_sync
        result['sync_summary'] = dict(sync_summary)
        return result
    except Exception as exc:
        sync_error = exc
        raise
    finally:
        if profiler is not None:
            try:
                profiler.finish(result=result, error=sync_error)
            except Exception:
                pass
            if profile_token is not None:
                try:
                    _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
                except Exception:
                    pass
        try:
            _CLOUD_SYNC_SUMMARY_CONTEXT.reset(summary_token)
        except Exception:
            pass
        try:
            _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.reset(progress_trace_token)
        except Exception:
            pass

def _parsed_local_media_signature(signature: str | None) -> dict:
    text = str(signature or '').strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalized_local_media_signature_payload(
    payload: dict | None,
    *,
    include_measurements: bool = True,
) -> dict:
    normalized = dict(payload or {})
    # Gallery layout state only affects desktop-local generated views.
    normalized['gallery_settings_raw'] = ''
    images = []
    for row in list(normalized.get('images') or []):
        if not isinstance(row, dict):
            continue
        image_payload = dict(row)
        image_payload.pop('sort_order', None)
        for key in _LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS:
            image_payload.setdefault(key, None)
        for path_key in ('filepath', 'original_filepath'):
            path_payload = image_payload.get(path_key)
            if isinstance(path_payload, dict):
                normalized_path = dict(path_payload)
                normalized_path.pop('mtime_ns', None)
                image_payload[path_key] = normalized_path
        images.append(image_payload)
    normalized['images'] = sorted(
        images,
        key=lambda row: (
            str(row.get('desktop_id') or ''),
            str(row.get('id') or ''),
            str(row.get('original_filename') or ''),
            str(row.get('image_type') or ''),
        ),
    )
    if not include_measurements:
        normalized.pop('measurements', None)
    return normalized


def _local_media_signatures_match(
    stored_signature: str | None,
    current_signature: str | None,
    *,
    include_measurements: bool = True,
) -> bool:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    if not stored_text or not current_text:
        return stored_text == current_text
    if stored_text == current_text:
        return True
    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    if not stored_payload or not current_payload:
        return False
    return _normalized_local_media_signature_payload(
        stored_payload,
        include_measurements=include_measurements,
    ) == _normalized_local_media_signature_payload(
        current_payload,
        include_measurements=include_measurements,
    )


def _store_local_media_signature_if_equivalent(
    observation_id: int | str,
    stored_signature: str | None,
    current_signature: str | None,
) -> None:
    current_text = str(current_signature or '').strip()
    if not current_text:
        return
    stored_text = str(stored_signature or '').strip()
    if stored_text == current_text:
        return
    if _local_media_signatures_match(stored_text, current_text):
        _store_local_cloud_media_signature(observation_id, current_text)


def _clear_cloud_observation_snapshot(cloud_id: str) -> None:
    if not str(cloud_id or '').strip():
        return
    SettingsDB.set_setting(_cloud_observation_snapshot_key(cloud_id), '')


def _clear_local_cloud_media_signature(observation_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_local_media_signature_key(observation_id), '')


def _refresh_local_cloud_media_signature(observation_id: int | str) -> str:
    signature = _local_cloud_media_signature(observation_id)
    if str(signature or '').strip():
        _store_local_cloud_media_signature(observation_id, signature)
    return signature


def _resolve_existing_local_image_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_readable_local_file(candidate):
            return candidate
    return None


def _detect_deleted_remote_observations(remote_obs: list[dict] | None) -> list[dict]:
    remote_ids = {
        str(row.get('id') or '').strip()
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT *
            FROM observations
            WHERE cloud_id IS NOT NULL
              AND TRIM(COALESCE(cloud_id, '')) != ''
            ORDER BY date DESC, id DESC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    deleted: list[dict] = []
    for local_obs in rows:
        cloud_id = str(local_obs.get('cloud_id') or '').strip()
        if not cloud_id or cloud_id in remote_ids:
            continue
        deleted.append(
            {
                'local_id': int(local_obs.get('id') or 0),
                'cloud_id': cloud_id,
                'title': _observation_display_name(local_obs),
                'date': local_obs.get('date'),
                'location': local_obs.get('location'),
                'sync_status': str(local_obs.get('sync_status') or '').strip().lower(),
                'observation': dict(local_obs),
            }
        )
    return deleted


def _load_local_calibration_rows() -> list[dict]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM calibrations
            ORDER BY objective_key ASC, calibration_date ASC, id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _load_local_calibration_by_uuid(calibration_uuid: str) -> dict | None:
    uuid_value = _normalize_calibration_uuid(calibration_uuid)
    if not uuid_value:
        return None
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM calibrations WHERE calibration_uuid = ? LIMIT 1",
            (uuid_value,),
        ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def _local_calibration_lookup(rows: list[dict] | None = None) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for row in (rows or _load_local_calibration_rows()):
        uuid_value = _normalize_calibration_uuid((row or {}).get('calibration_uuid'))
        if uuid_value:
            lookup[uuid_value] = dict(row or {})
    return lookup


def _image_calibration_uuid(image_row: dict | None) -> str | None:
    row = dict(image_row or {})
    uuid_value = _normalize_calibration_uuid(row.get('calibration_uuid'))
    if uuid_value:
        return uuid_value

    calibration_id = _safe_int(row.get('calibration_id'))
    if calibration_id <= 0:
        return None

    try:
        calibration = CalibrationDB.get_calibration(calibration_id)
    except Exception:
        return None
    if not calibration:
        return None
    return _normalize_calibration_uuid(calibration.get('calibration_uuid'))


def _local_calibration_id_for_image(image_row: dict | None) -> int | None:
    calibration_uuid = _image_calibration_uuid(image_row)
    if not calibration_uuid:
        return None

    calibration = _load_local_calibration_by_uuid(calibration_uuid)
    if not calibration:
        return None

    calibration_id = _safe_int(calibration.get('id'))
    return calibration_id if calibration_id > 0 else None


def _reconcile_local_image_calibration_links() -> int:
    """Backfill local image calibration_id values from stored cloud snapshots."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        try:
            snapshot_rows = cursor.execute(
                'SELECT key, value FROM settings WHERE key LIKE ?',
                (f'{_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX}%',),
            ).fetchall()
        except sqlite3.OperationalError:
            return 0
        if not snapshot_rows:
            return 0

        calibration_lookup = _local_calibration_lookup()
        try:
            local_image_rows = cursor.execute(
                'SELECT id, cloud_id, calibration_id FROM images WHERE cloud_id IS NOT NULL'
            ).fetchall()
        except sqlite3.OperationalError:
            return 0

        local_images_by_cloud_id = {
            str(row['cloud_id']).strip(): dict(row)
            for row in local_image_rows
            if str(row['cloud_id']).strip()
        }
        updates: list[tuple[int, int]] = []

        for snapshot_row in snapshot_rows:
            snapshot = _parse_cloud_observation_snapshot(snapshot_row['value'])
            for remote_image in snapshot.get('images') or []:
                calibration_uuid = _normalize_calibration_uuid(remote_image.get('calibration_uuid'))
                if not calibration_uuid:
                    continue
                calibration_row = calibration_lookup.get(calibration_uuid)
                if not calibration_row:
                    continue
                local_calibration_id = _safe_int(calibration_row.get('id'))
                if local_calibration_id <= 0:
                    continue

                cloud_image_id = str(remote_image.get('id') or '').strip()
                if not cloud_image_id:
                    continue
                local_image_row = local_images_by_cloud_id.get(cloud_image_id)
                if not local_image_row:
                    continue
                current_calibration_id = _safe_int(local_image_row.get('calibration_id'))
                if current_calibration_id == local_calibration_id:
                    continue
                updates.append((local_calibration_id, _safe_int(local_image_row.get('id'))))

        if not updates:
            return 0

        cursor.executemany(
            'UPDATE images SET calibration_id = ? WHERE id = ?',
            updates,
        )
        conn.commit()
        return len(updates)
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def _calibration_sync_warning(direction: str, local_row: dict | None, remote_row: dict | None, fields: list[str]) -> str:
    calibration_uuid = _normalize_calibration_uuid((local_row or remote_row or {}).get('calibration_uuid')) or '?'
    label = _calibration_display_name(local_row or remote_row)
    field_text = ', '.join(fields[:6]) if fields else 'metadata'
    return (
        f'calibration {calibration_uuid}: skipped {direction} for {label} '
        f'because the same UUID has conflicting metadata ({field_text})'
    )


def push_calibrations(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Push calibration metadata rows that exist only on the desktop."""
    phase_start = _cloud_sync_perf_counter()
    summary = _cloud_sync_current_summary()
    remote_rows = [dict(row or {}) for row in (remote_calibrations or client.list_remote_calibrations())]
    remote_map = {
        _normalize_calibration_uuid(row.get('calibration_uuid')): row
        for row in remote_rows
        if _normalize_calibration_uuid(row.get('calibration_uuid'))
    }
    local_rows = _load_local_calibration_rows()
    total = len(local_rows)
    pushed = 0
    matched_noop = 0
    conflicts = 0
    remote_lookups = 0
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)
    reference_image_uploader = getattr(client, 'push_calibration_reference_image', None)
    print(
        f"[cloud_sync] calibration push: start (local={total}, remote={len(remote_rows)})",
        flush=True,
    )
    if total:
        _emit_progress(progress_cb, "Checking local calibrations…", progress_state)

    for index, local_row in enumerate(local_rows, start=1):
        step_start = _cloud_sync_perf_counter()
        step_kind = 'metadata'
        calibration_uuid = _normalize_calibration_uuid(local_row.get('calibration_uuid'))
        label = _calibration_display_name(local_row)
        _emit_progress(
            progress_cb,
            f"Syncing calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped push because calibration_uuid is missing')
                continue

            remote_row = remote_map.get(calibration_uuid)
            if remote_row is not None:
                if not _calibration_payloads_match(local_row, remote_row):
                    conflicts += 1
                    errors.append(_calibration_sync_warning('push', local_row, remote_row, _calibration_diff_fields(local_row, remote_row)))
                    continue
                matched_noop += 1
                if callable(reference_image_uploader):
                    step_kind = 'reference_image'
                    warning = reference_image_uploader(
                        local_row,
                        cloud_row_id=str(remote_row.get('id') or '').strip() or None,
                        remote_row=remote_row,
                    )
                    if warning:
                        errors.append(warning)
                continue

            # Not in the freshly-listed remote set: double-check the server
            # before inserting so a row created since the list (e.g. another
            # device) is not duplicated. This is an extra remote call per
            # not-yet-synced calibration; tracked so an N+1 shows up in logs.
            step_kind = 'remote_lookup'
            remote_lookups += 1
            current_remote = client.find_remote_calibration(calibration_uuid)
            if current_remote is not None:
                if _calibration_payloads_match(local_row, current_remote):
                    matched_noop += 1
                    if callable(reference_image_uploader):
                        step_kind = 'reference_image'
                        warning = reference_image_uploader(
                            local_row,
                            cloud_row_id=str(current_remote.get('id') or '').strip() or None,
                            remote_row=current_remote,
                        )
                        if warning:
                            errors.append(warning)
                    continue
                conflicts += 1
                errors.append(_calibration_sync_warning('push', local_row, current_remote, _calibration_diff_fields(local_row, current_remote)))
                continue

            step_kind = 'metadata_insert'
            cloud_row_id = client.push_calibration_metadata(local_row)
            pushed += 1
            if callable(reference_image_uploader):
                step_kind = 'reference_image'
                warning = reference_image_uploader(
                    local_row,
                    cloud_row_id=cloud_row_id,
                    remote_row={'id': cloud_row_id, 'image_storage_path': None},
                )
                if warning:
                    errors.append(warning)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            step_elapsed = _cloud_sync_perf_counter() - step_start
            if step_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                print(
                    f"[cloud_sync] calibration push: slow step "
                    f"calibration {index}/{max(1, total)} ({label}) "
                    f"took {step_elapsed * 1000:.0f}ms during {step_kind}",
                    flush=True,
                )
            _advance_progress(progress_state, 1)

    _increment_sync_summary(summary, 'calibrations_pushed', pushed)
    _increment_sync_summary(summary, 'calibrations_skipped_noop', matched_noop)
    _increment_sync_summary(summary, 'calibrations_conflicts', conflicts)
    _increment_sync_summary(summary, 'calibration_remote_lookups', remote_lookups)
    print(
        f"[cloud_sync] calibration push: complete pushed={pushed} matched_noop={matched_noop} "
        f"conflicts={conflicts} remote_lookups={remote_lookups} errors={len(errors)} "
        f"duration={(_cloud_sync_perf_counter() - phase_start) * 1000:.0f}ms",
        flush=True,
    )
    return {
        'pushed': pushed,
        'total': total,
        'matched_noop': matched_noop,
        'conflicts': conflicts,
        'remote_lookups': remote_lookups,
        'errors': errors,
    }


def pull_calibrations(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Pull cloud calibration metadata into local rows keyed by UUID."""
    phase_start = _cloud_sync_perf_counter()
    summary = _cloud_sync_current_summary()
    remote_rows = [dict(row or {}) for row in (remote_calibrations or client.list_remote_calibrations())]
    local_rows = _load_local_calibration_rows()
    local_map = _local_calibration_lookup(local_rows)
    total = len(remote_rows)
    pulled = 0
    matched_noop = 0
    conflicts = 0
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)
    print(
        f"[cloud_sync] calibration pull: start (remote={total}, local={len(local_rows)})",
        flush=True,
    )

    remote_rows_sorted = sorted(
        remote_rows,
        key=lambda row: (
            _normalize_calibration_bool(row.get('is_active')),
            _normalize_calibration_text(row.get('objective_key')) or '',
            _normalize_calibration_date(row.get('calibration_date')) or '',
            str(row.get('id') or ''),
        ),
    )

    for index, remote_row in enumerate(remote_rows_sorted, start=1):
        step_start = _cloud_sync_perf_counter()
        calibration_uuid = _normalize_calibration_uuid(remote_row.get('calibration_uuid'))
        label = _calibration_display_name(remote_row)
        _emit_progress(
            progress_cb,
            f"Checking calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped pull because calibration_uuid is missing')
                continue

            local_row = local_map.get(calibration_uuid)
            if local_row is not None:
                if not _calibration_payloads_match(local_row, remote_row):
                    conflicts += 1
                    errors.append(_calibration_sync_warning('pull', local_row, remote_row, _calibration_diff_fields(local_row, remote_row)))
                else:
                    matched_noop += 1
                continue

            try:
                CalibrationDB.add_calibration(**_calibration_insert_kwargs(remote_row))
                pulled += 1
                local_map[calibration_uuid] = _load_local_calibration_by_uuid(calibration_uuid) or dict(remote_row)
            except sqlite3.IntegrityError:
                current_local = _load_local_calibration_by_uuid(calibration_uuid)
                if current_local and _calibration_payloads_match(current_local, remote_row):
                    matched_noop += 1
                    local_map[calibration_uuid] = current_local
                    continue
                conflicts += 1
                errors.append(_calibration_sync_warning('pull', current_local or remote_row, remote_row, _calibration_diff_fields(current_local or {}, remote_row)))
            except Exception as exc:
                errors.append(f'calibration {calibration_uuid}: {exc}')
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            step_elapsed = _cloud_sync_perf_counter() - step_start
            if step_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                print(
                    f"[cloud_sync] calibration pull: slow step "
                    f"calibration {index}/{max(1, total)} ({label}) took {step_elapsed * 1000:.0f}ms",
                    flush=True,
                )
            _advance_progress(progress_state, 1)

    reconcile_start = _cloud_sync_perf_counter()
    reconciled_links = 0
    try:
        _emit_progress(progress_cb, "Linking calibration images…", progress_state)
        reconciled_links = _reconcile_local_image_calibration_links()
    except Exception as exc:
        errors.append(f'calibration reconciliation: {exc}')
    reconcile_elapsed = _cloud_sync_perf_counter() - reconcile_start
    if reconcile_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
        print(
            f"[cloud_sync] calibration pull: image link reconciliation took "
            f"{reconcile_elapsed * 1000:.0f}ms ({reconciled_links} link(s) updated)",
            flush=True,
        )

    _increment_sync_summary(summary, 'calibrations_pulled', pulled)
    _increment_sync_summary(summary, 'calibrations_skipped_noop', matched_noop)
    _increment_sync_summary(summary, 'calibrations_conflicts', conflicts)
    print(
        f"[cloud_sync] calibration pull: complete pulled={pulled} matched_noop={matched_noop} "
        f"conflicts={conflicts} links_updated={reconciled_links} errors={len(errors)} "
        f"duration={(_cloud_sync_perf_counter() - phase_start) * 1000:.0f}ms",
        flush=True,
    )
    return {
        'pulled': pulled,
        'total': total,
        'matched_noop': matched_noop,
        'conflicts': conflicts,
        'links_updated': reconciled_links,
        'errors': errors,
    }


def list_calibration_conflicts(
    client: SporelyCloudClient,
    calibration_uuids: list[str] | None = None,
    remote_calibrations: list[dict] | None = None,
) -> list[dict]:
    """Return explicit calibration UUID conflicts between the local DB and cloud."""
    remote_source = remote_calibrations if remote_calibrations is not None else client.list_remote_calibrations()
    remote_rows = [dict(row or {}) for row in remote_source]
    remote_map = {
        _normalize_calibration_uuid(row.get('calibration_uuid')): row
        for row in remote_rows
        if _normalize_calibration_uuid(row.get('calibration_uuid'))
    }
    target_uuids = None
    if calibration_uuids is not None:
        target_uuids = {
            _normalize_calibration_uuid(value)
            for value in calibration_uuids
            if _normalize_calibration_uuid(value)
        }
    local_rows = _load_local_calibration_rows()
    if target_uuids is not None:
        local_rows = [
            row
            for row in local_rows
            if _normalize_calibration_uuid(row.get('calibration_uuid')) in target_uuids
        ]

    conflicts: list[dict] = []
    for local_row in local_rows:
        calibration_uuid = _normalize_calibration_uuid(local_row.get('calibration_uuid'))
        if not calibration_uuid:
            continue
        remote_row = remote_map.get(calibration_uuid)
        if remote_row is None:
            continue
        changes = _calibration_field_changes(local_row, remote_row)
        if not changes:
            continue
        conflict = {
            'calibration_uuid': calibration_uuid,
            'cloud_row_id': str(remote_row.get('id') or '').strip() or None,
            'label': _calibration_display_name(local_row),
            'fields': list(changes.keys()),
            'local_row': dict(local_row),
            'remote_row': dict(remote_row),
        }
        if 'measurements_json' in changes:
            conflict['normalized_local_measurements_json'] = _normalize_calibration_measurements_json(
                local_row.get('measurements_json')
            )
            conflict['normalized_remote_measurements_json'] = _normalize_calibration_measurements_json(
                remote_row.get('measurements_json')
            )
        conflicts.append(conflict)
    return conflicts


def repair_calibrations_local_wins(
    client: SporelyCloudClient,
    calibration_uuids: list[str] | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Repair conflicting cloud calibration metadata using the local desktop rows as source of truth."""
    conflicts = list_calibration_conflicts(
        client,
        calibration_uuids=calibration_uuids,
        remote_calibrations=remote_calibrations,
    )
    total = len(conflicts)
    repaired = 0
    repairs: list[dict] = []
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)

    for index, conflict in enumerate(conflicts, start=1):
        calibration_uuid = _normalize_calibration_uuid(conflict.get('calibration_uuid'))
        local_row = dict(conflict.get('local_row') or {})
        remote_row = dict(conflict.get('remote_row') or {})
        label = str(conflict.get('label') or _calibration_display_name(local_row))
        _emit_progress(
            progress_cb,
            f"Repairing calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped repair because calibration_uuid is missing')
                continue

            fields = list(conflict.get('fields') or [])
            if not fields:
                continue

            cloud_row_id = str(conflict.get('cloud_row_id') or remote_row.get('id') or '').strip()
            if not cloud_row_id:
                current_remote = client.find_remote_calibration(calibration_uuid)
                remote_row = dict(current_remote or {})
                cloud_row_id = str(remote_row.get('id') or '').strip()
            if not cloud_row_id:
                errors.append(
                    f'calibration {calibration_uuid}: skipped repair because the cloud row id is unavailable'
                )
                continue

            patch_payload = _calibration_local_wins_patch_payload(local_row, remote_row)
            if not patch_payload:
                continue

            client._patch(
                f'calibrations?user_id=eq.{client.user_id}&id=eq.{cloud_row_id}',
                patch_payload,
            )
            repaired += 1
            fields = list(fields or _calibration_diff_fields(local_row, remote_row))
            repair_entry = {
                'calibration_uuid': calibration_uuid,
                'cloud_row_id': cloud_row_id,
                'fields': fields,
                'message': (
                    f'calibration {calibration_uuid}: repaired local-wins cloud row {cloud_row_id} '
                    f'overwrote fields ({", ".join(fields)})'
                ),
            }
            refreshed_remote = None
            if 'measurements_json' in fields:
                try:
                    refreshed_remote = client.find_remote_calibration(calibration_uuid)
                except Exception as exc:
                    if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                        raise
                    errors.append(
                        f'calibration {calibration_uuid}: could not re-read cloud row after repair ({exc})'
                    )
                else:
                    remaining_changes = _calibration_field_changes(local_row, refreshed_remote or remote_row)
                    if 'measurements_json' in remaining_changes:
                        repair_entry['remaining_fields'] = list(remaining_changes.keys())
                        repair_entry['normalized_local_measurements_json'] = _normalize_calibration_measurements_json(
                            local_row.get('measurements_json')
                        )
                        repair_entry['normalized_remote_measurements_json'] = _normalize_calibration_measurements_json(
                            (refreshed_remote or remote_row).get('measurements_json')
                        )
                        print(
                            '[cloud_sync] '
                            f'calibration {calibration_uuid}: measurements_json still differs after local-wins repair '
                            f'(local={json.dumps(repair_entry["normalized_local_measurements_json"], ensure_ascii=False, sort_keys=True)}, '
                            f'remote={json.dumps(repair_entry["normalized_remote_measurements_json"], ensure_ascii=False, sort_keys=True)})'
                        )
            repairs.append(repair_entry)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            _advance_progress(progress_state, 1)

    return {'repaired': repaired, 'total': total, 'repairs': repairs, 'errors': errors}


def unlink_local_observation_from_cloud(local_id: int) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    image_rows = list(ImageDB.get_images_for_observation(int(local_id)) or [])

    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE observations
            SET cloud_id = NULL,
                sync_status = NULL,
                synced_at = NULL
            WHERE id = ?
            """,
            (int(local_id),),
        )
        conn.execute(
            """
            UPDATE images
            SET cloud_id = NULL,
                synced_at = NULL
            WHERE observation_id = ?
            """,
            (int(local_id),),
        )
        conn.commit()
    finally:
        conn.close()

    if cloud_id:
        _clear_cloud_observation_snapshot(cloud_id)
    _clear_local_cloud_media_signature(int(local_id))
    for image_row in image_rows:
        image_id = _safe_int(image_row.get('id'))
        cloud_image_id = str(image_row.get('cloud_id') or '').strip()
        if image_id > 0:
            _clear_cloud_image_file_signature(int(local_id), image_id)
        if cloud_image_id:
            _clear_cloud_image_file_signature(int(local_id), cloud_image_id)
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def mark_observation_dirty(local_id: int) -> None:
    try:
        obs_id = int(local_id or 0)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return
    conn = get_connection()
    try:
        cursor = conn.cursor()
        mark_observation_sync_dirty(cursor, obs_id)
        conn.commit()
    finally:
        conn.close()


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _observation_sync_species_label(obs: dict | None) -> str:
    record = dict(obs or {})
    parts = [
        str(record.get('genus') or '').strip(),
        str(record.get('species') or '').strip(),
    ]
    label = " ".join(part for part in parts if part).strip()
    if label:
        return label
    common_name = str(record.get('common_name') or '').strip()
    if common_name:
        return common_name
    species_guess = str(record.get('species_guess') or '').strip()
    if species_guess:
        return species_guess
    return ''


def _format_cloud_sync_observation_status(obs: dict | None, message: str) -> str:
    record = dict(obs or {})
    obs_id = _safe_int(record.get('id'))
    label = _observation_sync_species_label(record)
    if obs_id > 0 and label:
        prefix = f'Observation {obs_id} ({label})'
    elif obs_id > 0:
        prefix = f'Observation {obs_id}'
    elif label:
        prefix = label
    else:
        prefix = 'Observation'
    text = str(message or '').strip()
    return f'{prefix}: {text}' if text else prefix


def _file_content_signature(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return ''
    digest = hashlib.sha1()
    with open(file_path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _remote_ai_crop_box(image_row: dict | None) -> tuple[float, float, float, float] | None:
    row = dict(image_row or {})
    values = []
    for key in ('ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2'):
        value = row.get(key)
        if value is None:
            return None
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            return None
    return tuple(values) if len(values) == 4 else None


def _remote_ai_crop_source_size(image_row: dict | None) -> tuple[int, int] | None:
    row = dict(image_row or {})
    width = row.get('ai_crop_source_w')
    height = row.get('ai_crop_source_h')
    if width is None or height is None:
        return None
    try:
        return int(width), int(height)
    except (TypeError, ValueError):
        return None


def _remote_ai_crop_is_custom(image_row: dict | None) -> bool | None:
    row = dict(image_row or {})
    value = row.get('ai_crop_is_custom')
    if value is None:
        return None
    return bool(value)


def _path_stat_signature(path_value: str | None) -> dict:
    path_text = str(path_value or '').strip()
    if not path_text:
        return {'path': '', 'exists': False}
    path = Path(path_text)
    try:
        stat = path.stat()
        return {
            'path': path_text,
            'exists': True,
            'size': int(stat.st_size),
            'mtime_ns': int(getattr(stat, 'st_mtime_ns', int(stat.st_mtime * 1_000_000_000))),
        }
    except Exception:
        return {'path': path_text, 'exists': path.exists()}


def _local_cloud_media_signature(
    observation_id: int | str,
    *,
    include_measurements: bool = True,
) -> str:
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return ''
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                id,
                filepath,
                original_filepath,
                sort_order,
                image_type,
                micro_category,
                objective_name,
                scale_microns_per_pixel,
                resample_scale_factor,
                mount_medium,
                stain,
                sample_type,
                contrast,
                measure_color,
                crop_mode,
                notes,
                gps_source,
                ai_crop_x1,
                ai_crop_y1,
                ai_crop_x2,
                ai_crop_y2,
                ai_crop_source_w,
                ai_crop_source_h,
                ai_crop_is_custom
            FROM images
            WHERE observation_id = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                image_type,
                micro_category,
                created_at,
                id
            ''',
            (obs_id,),
        )
        image_rows = [dict(row) for row in cursor.fetchall()]
        tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
            [str(row.get('cloud_id') or '').strip() for row in image_rows if str(row.get('cloud_id') or '').strip()]
        )
        if tombstoned_cloud_ids:
            image_rows = [
                row
                for row in image_rows
                if str(row.get('cloud_id') or '').strip() not in tombstoned_cloud_ids
            ]
        measurement_rows: list[dict] = []
        if include_measurements:
            cursor.execute(
                '''
                SELECT
                    m.id,
                    m.image_id,
                    m.length_um,
                    m.width_um,
                    m.measurement_type,
                    m.notes,
                    m.p1_x,
                    m.p1_y,
                    m.p2_x,
                    m.p2_y,
                    m.p3_x,
                    m.p3_y,
                    m.p4_x,
                    m.p4_y,
                    m.gallery_rotation
                FROM spore_measurements m
                JOIN images i ON i.id = m.image_id
                WHERE i.observation_id = ?
                ORDER BY m.id
                ''',
                (obs_id,),
            )
            measurement_rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    excluded_raw = str(SettingsDB.get_setting(f"artsobs_publish_excluded_image_ids_{obs_id}", '[]') or '[]')
    gallery_settings_raw = str(SettingsDB.get_setting(f"gallery_settings_{obs_id}", '') or '').strip()
    payload = {
        'render_version': _CLOUD_LOCAL_MEDIA_RENDER_VERSION,
        'cloud_media_signature': _cloud_media_signature(),
        'cloud_image_size_mode': 'full',
        'excluded_image_ids_raw': excluded_raw,
        'gallery_settings_raw': gallery_settings_raw,
        'images': [
            {
                'id': _safe_int(row.get('id')),
                'filepath': _path_stat_signature(row.get('filepath')),
                'original_filepath': _path_stat_signature(row.get('original_filepath')),
                'sort_order': _normalize_snapshot_value(row.get('sort_order')),
                'image_type': _normalize_snapshot_value(row.get('image_type')),
                'micro_category': _normalize_snapshot_value(row.get('micro_category')),
                'objective_name': _normalize_snapshot_value(row.get('objective_name')),
                'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
                'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
                'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
                'stain': _normalize_snapshot_value(row.get('stain')),
                'sample_type': _normalize_snapshot_value(row.get('sample_type')),
                'contrast': _normalize_snapshot_value(row.get('contrast')),
                'measure_color': _normalize_snapshot_value(row.get('measure_color')),
                'crop_mode': _normalize_snapshot_value(row.get('crop_mode')),
                'notes': _normalize_snapshot_value(row.get('notes')),
                'gps_source': _normalize_snapshot_value(row.get('gps_source')),
                'ai_crop_x1': _normalize_snapshot_value(row.get('ai_crop_x1')),
                'ai_crop_y1': _normalize_snapshot_value(row.get('ai_crop_y1')),
                'ai_crop_x2': _normalize_snapshot_value(row.get('ai_crop_x2')),
                'ai_crop_y2': _normalize_snapshot_value(row.get('ai_crop_y2')),
                'ai_crop_source_w': _normalize_snapshot_value(row.get('ai_crop_source_w')),
                'ai_crop_source_h': _normalize_snapshot_value(row.get('ai_crop_source_h')),
            }
            for row in image_rows
        ],
    }
    if include_measurements:
        payload['measurements'] = [
            {
                **_measurement_compare_payload(row, local=False),
                'notes': _normalize_snapshot_value(row.get('notes')),
            }
            for row in measurement_rows
        ]
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _local_cloud_image_media_signature(observation_id: int | str) -> str:
    return _local_cloud_media_signature(observation_id, include_measurements=False)


def _local_media_signatures_match_ignoring_tombstoned_images(
    stored_signature: str | None,
    current_signature: str | None,
) -> bool:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    if not stored_text or not current_text:
        return stored_text == current_text
    if stored_text == current_text:
        return True

    tombstoned_local_ids = _local_tombstoned_local_image_ids()
    if not tombstoned_local_ids:
        return False

    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    if not stored_payload or not current_payload:
        return False

    def _filtered_payload(payload: dict) -> dict:
        filtered = dict(payload or {})
        images: list[dict] = []
        for row in list(filtered.get('images') or []):
            if not isinstance(row, dict):
                continue
            if _safe_int(row.get('id')) in tombstoned_local_ids:
                continue
            images.append(dict(row))
        filtered['images'] = images
        return filtered

    return _normalized_local_media_signature_payload(
        _filtered_payload(stored_payload),
        include_measurements=False,
    ) == _normalized_local_media_signature_payload(
        _filtered_payload(current_payload),
        include_measurements=False,
    )


def _local_media_prep_diagnostics(
    observation_id: int | str,
    stored_signature: str | None,
    current_signature: str | None,
) -> dict:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    image_render_signature_matched = bool(
        stored_text
        and current_text
        and _local_media_signatures_match(
            stored_text,
            current_text,
            include_measurements=False,
        )
    )
    tombstone_aware_signature_matched = bool(
        stored_text
        and current_text
        and _local_media_signatures_match_ignoring_tombstoned_images(
            stored_text,
            current_text,
        )
    )
    measurement_only_matched = bool(
        stored_text
        and current_text
        and image_render_signature_matched
        and not _local_media_signatures_match(stored_text, current_text)
    )

    local_image_rows: list[dict] = []
    obs_id = _safe_int(observation_id)
    if obs_id > 0:
        try:
            local_image_rows = [dict(row or {}) for row in ImageDB.get_images_for_observation(obs_id) or []]
        except Exception:
            local_image_rows = []
    has_local_image_cloud_id_null = any(not str(row.get('cloud_id') or '').strip() for row in local_image_rows)

    changed_keys: list[str] = []
    any_image_file_signature_changed = False
    any_render_affecting_field_changed = False
    only_metadata_fields_changed = False

    def _normalized_path_signature(path_value: object) -> dict:
        if not isinstance(path_value, dict):
            return {'path': '', 'exists': False}
        normalized = dict(path_value)
        normalized.pop('mtime_ns', None)
        return normalized

    if stored_payload and current_payload:
        stored_images = [dict(row or {}) for row in (stored_payload.get('images') or [])]
        current_images = [dict(row or {}) for row in (current_payload.get('images') or [])]
        image_changes = _analyze_image_changes(current_images, stored_images)
        changed_keys.extend(f'+{key}' for key in (image_changes.get('added_keys') or []))
        changed_keys.extend(f'-{key}' for key in (image_changes.get('removed_keys') or []))

        current_map = {_image_compare_key(row): row for row in current_images}
        stored_map = {_image_compare_key(row): row for row in stored_images}
        shared_keys = [key for key in current_map if key in stored_map]
        metadata_changed_field_count = 0

        for key in shared_keys:
            current_row = current_map[key]
            stored_row = stored_map[key]

            for path_key in ('filepath', 'original_filepath'):
                if _normalized_path_signature(current_row.get(path_key)) != _normalized_path_signature(stored_row.get(path_key)):
                    any_image_file_signature_changed = True
                    changed_keys.append(f'{key}:{path_key}')

            current_meta = _image_metadata_payload(current_row)
            stored_meta = _image_metadata_payload(stored_row)
            for field, current_value in current_meta.items():
                if current_value == stored_meta.get(field):
                    continue
                metadata_changed_field_count += 1
                changed_keys.append(f'{key}:{field}')
                if field in _LOCAL_MEDIA_PREP_RENDER_AFFECTING_IMAGE_FIELDS:
                    any_render_affecting_field_changed = True

        for field in _LOCAL_MEDIA_PREP_RENDER_AFFECTING_TOP_LEVEL_FIELDS:
            if current_payload.get(field) != stored_payload.get(field):
                any_render_affecting_field_changed = True
                changed_keys.append(field)

        if image_changes.get('added_keys') or image_changes.get('removed_keys'):
            any_image_file_signature_changed = True

        only_metadata_fields_changed = bool(
            metadata_changed_field_count
            and not any_image_file_signature_changed
            and not any_render_affecting_field_changed
            and not image_changes.get('added_keys')
            and not image_changes.get('removed_keys')
        )

    changed_keys = list(dict.fromkeys(str(key).strip() for key in changed_keys if str(key).strip()))

    return {
        'image_render_signature_matched': image_render_signature_matched,
        'tombstone_aware_signature_matched': tombstone_aware_signature_matched,
        'measurement_only_matched': measurement_only_matched,
        'has_local_image_cloud_id_null': has_local_image_cloud_id_null,
        'any_image_file_signature_changed': any_image_file_signature_changed,
        'any_render_affecting_field_changed': any_render_affecting_field_changed,
        'only_metadata_fields_changed': only_metadata_fields_changed,
        'changed_keys': changed_keys,
    }


def _format_local_media_prep_diagnostic_keys(keys: list[str], *, limit: int = 8) -> str:
    normalized_keys = [str(key).strip() for key in (keys or []) if str(key or '').strip()]
    if not normalized_keys:
        return '[]'
    display_keys = normalized_keys[: max(1, int(limit))]
    suffix = ''
    if len(normalized_keys) > len(display_keys):
        suffix = f', … (+{len(normalized_keys) - len(display_keys)} more)'
    return f"[{', '.join(display_keys)}{suffix}]"


def _prepared_item_remote_payload(
    image_row: dict,
    upload_path: str,
    storage_path: str,
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    normalized_key = _normalize_cloud_media_key(storage_path)
    payload = {
        'id': _normalize_snapshot_value(image_row.get('cloud_id')),
        'desktop_id': _safe_int(image_row.get('id')),
        'sort_order': _normalize_snapshot_value(image_row.get('sort_order')),
        'image_type': _normalize_snapshot_value(image_row.get('image_type')),
        'micro_category': _normalize_snapshot_value(image_row.get('micro_category')),
        'calibration_uuid': _normalize_snapshot_value(_image_calibration_uuid(image_row)),
        'objective_name': _normalize_snapshot_value(image_row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image_row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image_row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image_row.get('mount_medium')),
        'stain': _normalize_snapshot_value(image_row.get('stain')),
        'sample_type': _normalize_snapshot_value(image_row.get('sample_type')),
        'contrast': _normalize_snapshot_value(image_row.get('contrast')),
        'measure_color': _normalize_snapshot_value(image_row.get('measure_color')),
        'crop_mode': _normalize_snapshot_value(image_row.get('crop_mode')),
        'notes': _normalize_snapshot_value(image_row.get('notes')),
        'gps_source': _normalize_snapshot_value(
            None if image_row.get('gps_source') is None else bool(image_row.get('gps_source'))
        ),
        'storage_path': _normalize_snapshot_value(normalized_key or None),
        'original_filename': _normalize_snapshot_value(Path(str(upload_path or '').strip()).name or None),
    }
    if include_ai_crop:
        payload.update({
            'ai_crop_x1': _normalize_snapshot_value(image_row.get('ai_crop_x1')),
            'ai_crop_y1': _normalize_snapshot_value(image_row.get('ai_crop_y1')),
            'ai_crop_x2': _normalize_snapshot_value(image_row.get('ai_crop_x2')),
            'ai_crop_y2': _normalize_snapshot_value(image_row.get('ai_crop_y2')),
            'ai_crop_source_w': _normalize_snapshot_value(image_row.get('ai_crop_source_w')),
            'ai_crop_source_h': _normalize_snapshot_value(image_row.get('ai_crop_source_h')),
            'ai_crop_is_custom': _normalize_snapshot_value(image_row.get('ai_crop_is_custom')),
        })
    if include_upload_meta:
        payload.update({
            'upload_mode': _normalize_snapshot_value(image_row.get('upload_mode')),
            'source_width': _normalize_snapshot_value(image_row.get('source_width')),
            'source_height': _normalize_snapshot_value(image_row.get('source_height')),
            'stored_width': _normalize_snapshot_value(image_row.get('stored_width')),
            'stored_height': _normalize_snapshot_value(image_row.get('stored_height')),
            'stored_bytes': _normalize_snapshot_value(image_row.get('stored_bytes')),
        })
    return payload


def _remote_image_payload(
    remote_image: dict | None,
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    image = remote_image or {}
    payload = {
        'id': _normalize_snapshot_value(image.get('id')),
        'desktop_id': _safe_int(image.get('desktop_id')),
        'sort_order': _normalize_snapshot_value(image.get('sort_order')),
        'image_type': _normalize_snapshot_value(image.get('image_type')),
        'micro_category': _normalize_snapshot_value(image.get('micro_category')),
        'calibration_uuid': _normalize_snapshot_value(image.get('calibration_uuid')),
        'objective_name': _normalize_snapshot_value(image.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image.get('mount_medium')),
        'stain': _normalize_snapshot_value(image.get('stain')),
        'sample_type': _normalize_snapshot_value(image.get('sample_type')),
        'contrast': _normalize_snapshot_value(image.get('contrast')),
        'measure_color': _normalize_snapshot_value(image.get('measure_color')),
        'crop_mode': _normalize_snapshot_value(image.get('crop_mode')),
        'notes': _normalize_snapshot_value(image.get('notes')),
        'gps_source': _normalize_snapshot_value(image.get('gps_source')),
        'storage_path': _normalize_snapshot_value(_normalize_cloud_media_key(image.get('storage_path')) or None),
        'original_filename': _normalize_snapshot_value(image.get('original_filename')),
    }
    if include_ai_crop:
        payload.update({
            'ai_crop_x1': _normalize_snapshot_value(image.get('ai_crop_x1')),
            'ai_crop_y1': _normalize_snapshot_value(image.get('ai_crop_y1')),
            'ai_crop_x2': _normalize_snapshot_value(image.get('ai_crop_x2')),
            'ai_crop_y2': _normalize_snapshot_value(image.get('ai_crop_y2')),
            'ai_crop_source_w': _normalize_snapshot_value(image.get('ai_crop_source_w')),
            'ai_crop_source_h': _normalize_snapshot_value(image.get('ai_crop_source_h')),
            'ai_crop_is_custom': _normalize_snapshot_value(image.get('ai_crop_is_custom')),
        })
    if include_upload_meta:
        payload.update({
            'upload_mode': _normalize_snapshot_value(image.get('upload_mode')),
            'source_width': _normalize_snapshot_value(image.get('source_width')),
            'source_height': _normalize_snapshot_value(image.get('source_height')),
            'stored_width': _normalize_snapshot_value(image.get('stored_width')),
            'stored_height': _normalize_snapshot_value(image.get('stored_height')),
            'stored_bytes': _normalize_snapshot_value(image.get('stored_bytes')),
        })
    return payload


_SNAPSHOT_MEAS_FIELDS = [
    'id', 'desktop_id', 'image_id', 'length_um', 'width_um', 'measurement_type',
    'gallery_rotation', 'p1_x', 'p1_y', 'p2_x', 'p2_y', 'p3_x', 'p3_y',
    'p4_x', 'p4_y', 'measured_at',
]

_OBSERVATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'captured_at',
    'created_at',
    'updated_at',
    *_SNAPSHOT_OBS_FIELDS,
)

_OBSERVATION_IMAGE_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'observation_id',
    'created_at',
    'deleted_at',
    *_SNAPSHOT_IMG_FIELDS,
    *_SNAPSHOT_IMG_PASSIVE_FIELDS,
)

_OBSERVATION_IDENTIFICATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'service',
    'created_at',
    'results',
    'top_scientific_name',
    'top_vernacular_name',
    'top_taxon_id',
    'top_species_url',
    'top_probability',
)

_SPORE_MEASUREMENT_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'image_id',
    'length_um',
    'width_um',
    'measurement_type',
    'gallery_rotation',
    'p1_x',
    'p1_y',
    'p2_x',
    'p2_y',
    'p3_x',
    'p3_y',
    'p4_x',
    'p4_y',
    'measured_at',
    'image_key',
    'thumb_key',
)

# Batch size for PostgREST `id=in.(...)` fetches (measurements + image metadata).
# IDs are UUIDs (~36 chars), so 100 IDs keep the `in.(...)` clause under ~3.7KB,
# well below any reasonable proxy URL limit (nginx default request line is 8KB),
# while halving the request count vs the previous size of 50 (e.g. 866 image IDs
# go from 18 requests to 9). Kept conservative on purpose; do not raise without
# re-checking the proxy URL limit for the longest realistic ID list.
_CLOUD_SYNC_IN_BATCH_SIZE = 100


def _normalize_measurement_type_value(value) -> str:
    text = str(value or 'manual').strip().lower()
    return text or 'manual'


def _normalize_measurement_timestamp_value(value) -> str | None:
    parsed = _parse_sync_timestamp(value)
    if parsed is not None:
        return parsed.isoformat()
    text = str(value or '').strip()
    return text or None


_MEASUREMENT_FLOAT_FIELDS = {
    'length_um',
    'width_um',
    'p1_x',
    'p1_y',
    'p2_x',
    'p2_y',
    'p3_x',
    'p3_y',
    'p4_x',
    'p4_y',
}
_MEASUREMENT_FLOAT_ABS_TOL = 1e-9
_MEASUREMENT_FLOAT_REL_TOL = 1e-9
_MEASUREMENT_SYNC_FIELDS = [
    'desktop_id',
    'image_id',
    'length_um',
    'width_um',
    'measurement_type',
    'gallery_rotation',
    'p1_x',
    'p1_y',
    'p2_x',
    'p2_y',
    'p3_x',
    'p3_y',
    'p4_x',
    'p4_y',
    'measured_at',
]
_MEASUREMENT_SYNC_MEDIA_FIELDS = ['image_key', 'thumb_key']


def _normalize_measurement_identity_value(value) -> str | None:
    text = str(value or '').strip()
    return text or None


def _normalize_measurement_int_value(value, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        try:
            return int(value)
        except Exception:
            return default
    text = str(value or '').strip()
    if not text:
        return default
    try:
        return int(float(text))
    except Exception:
        return default


def _normalize_measurement_float_value(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _measurement_field_values_match(field: str, left, right) -> bool:
    if field in _MEASUREMENT_FLOAT_FIELDS:
        left_float = _normalize_measurement_float_value(left)
        right_float = _normalize_measurement_float_value(right)
        if left_float is None or right_float is None:
            return left_float is None and right_float is None
        return math.isclose(
            left_float,
            right_float,
            rel_tol=_MEASUREMENT_FLOAT_REL_TOL,
            abs_tol=_MEASUREMENT_FLOAT_ABS_TOL,
        )
    if field == 'measurement_type':
        return _normalize_measurement_type_value(left) == _normalize_measurement_type_value(right)
    if field == 'measured_at':
        return _normalize_measurement_timestamp_value(left) == _normalize_measurement_timestamp_value(right)
    if field == 'gallery_rotation':
        return _normalize_measurement_int_value(left, default=0) == _normalize_measurement_int_value(right, default=0)
    if field == 'desktop_id':
        return _normalize_measurement_int_value(left) == _normalize_measurement_int_value(right)
    if field in {'id', 'image_id', 'image_key', 'thumb_key'}:
        return _normalize_measurement_identity_value(left) == _normalize_measurement_identity_value(right)
    return _normalize_snapshot_value(left) == _normalize_snapshot_value(right)


def _measurement_compare_key(measurement_row: dict | None) -> str:
    row = dict(measurement_row or {})
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    image_id = str(row.get('image_id') or '').strip()
    if cloud_id:
        return f'cloud:{cloud_id}'
    if desktop_id:
        return f'desktop:{desktop_id}'
    if image_id:
        return f'image:{image_id}'
    return json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _measurement_compare_payload(
    measurement_row: dict | None,
    *,
    local: bool,
    cloud_image_id: str | None = None,
    include_media_keys: bool = False,
    image_storage_key: str | None = None,
) -> dict:
    row = dict(measurement_row or {})
    payload: dict = {}
    if local:
        payload['id'] = _normalize_measurement_identity_value(
            str(row.get('cloud_id') or '').strip() or row.get('id')
        )
        payload['desktop_id'] = _normalize_measurement_int_value(row.get('id'))
        payload['image_id'] = _normalize_measurement_identity_value(
            cloud_image_id or str(row.get('image_cloud_id') or '').strip() or row.get('image_id')
        )
    else:
        payload['id'] = _normalize_measurement_identity_value(row.get('id'))
        payload['desktop_id'] = _normalize_measurement_int_value(row.get('desktop_id'))
        payload['image_id'] = _normalize_measurement_identity_value(row.get('image_id'))

    payload['length_um'] = _normalize_measurement_float_value(row.get('length_um'))
    payload['width_um'] = _normalize_measurement_float_value(row.get('width_um'))
    payload['measurement_type'] = _normalize_measurement_type_value(row.get('measurement_type'))
    payload['gallery_rotation'] = _normalize_measurement_int_value(row.get('gallery_rotation'), default=0)
    payload['p1_x'] = _normalize_measurement_float_value(row.get('p1_x'))
    payload['p1_y'] = _normalize_measurement_float_value(row.get('p1_y'))
    payload['p2_x'] = _normalize_measurement_float_value(row.get('p2_x'))
    payload['p2_y'] = _normalize_measurement_float_value(row.get('p2_y'))
    payload['p3_x'] = _normalize_measurement_float_value(row.get('p3_x'))
    payload['p3_y'] = _normalize_measurement_float_value(row.get('p3_y'))
    payload['p4_x'] = _normalize_measurement_float_value(row.get('p4_x'))
    payload['p4_y'] = _normalize_measurement_float_value(row.get('p4_y'))
    payload['measured_at'] = _normalize_measurement_timestamp_value(row.get('measured_at'))
    if include_media_keys:
        if local:
            storage_key = _normalize_cloud_media_key(image_storage_key)
            payload['image_key'] = storage_key or None
            payload['thumb_key'] = media_variant_key(storage_key, 'thumb') if storage_key else None
        else:
            payload['image_key'] = _normalize_cloud_media_key(row.get('image_key')) or None
            payload['thumb_key'] = _normalize_cloud_media_key(row.get('thumb_key')) or None
    return payload


def _local_measurement_snapshot_payload(measurement_row: dict | None) -> dict:
    return _measurement_compare_payload(measurement_row, local=True)


def _remote_measurement_snapshot_payload(measurement_row: dict | None) -> dict:
    return _measurement_compare_payload(measurement_row, local=False)


def _baseline_measurement_compare_payload(record: dict | None) -> dict:
    return _measurement_compare_payload(record, local=False)


def _measurement_sync_payload(
    measurement_row: dict | None,
    *,
    local: bool,
    cloud_image_id: str | None = None,
    image_storage_key: str | None = None,
    include_media_keys: bool = False,
) -> dict:
    payload = _measurement_compare_payload(
        measurement_row,
        local=local,
        cloud_image_id=cloud_image_id,
        include_media_keys=include_media_keys,
        image_storage_key=image_storage_key,
    )
    payload.pop('id', None)
    return payload


def _measurement_payloads_match(
    local_row: dict | None,
    remote_row: dict | None,
    *,
    cloud_image_id: str | None = None,
    image_storage_key: str | None = None,
    include_media_keys: bool = False,
) -> bool:
    local_payload = _measurement_sync_payload(
        local_row,
        local=True,
        cloud_image_id=cloud_image_id,
        image_storage_key=image_storage_key,
        include_media_keys=include_media_keys,
    )
    remote_payload = _measurement_sync_payload(
        remote_row,
        local=False,
        include_media_keys=include_media_keys,
    )
    compare_fields = list(_MEASUREMENT_SYNC_FIELDS)
    if include_media_keys:
        compare_fields.extend(_MEASUREMENT_SYNC_MEDIA_FIELDS)
    for field in compare_fields:
        if not _measurement_field_values_match(field, local_payload.get(field), remote_payload.get(field)):
            return False
    return True


def _measurement_push_diff_fields(
    local_row: dict | None,
    remote_row: dict | None,
    *,
    cloud_image_id: str | None = None,
    image_storage_key: str | None = None,
    include_media_keys: bool = False,
) -> list[str]:
    local_payload = _measurement_sync_payload(
        local_row,
        local=True,
        cloud_image_id=cloud_image_id,
        image_storage_key=image_storage_key,
        include_media_keys=include_media_keys,
    )
    remote_payload = _measurement_sync_payload(
        remote_row,
        local=False,
        include_media_keys=include_media_keys,
    )
    diff_fields: list[str] = []
    compare_fields = list(_MEASUREMENT_SYNC_FIELDS)
    if include_media_keys:
        compare_fields.extend(_MEASUREMENT_SYNC_MEDIA_FIELDS)
    for field in compare_fields:
        if not _measurement_field_values_match(field, local_payload.get(field), remote_payload.get(field)):
            diff_fields.append(field)
    return diff_fields


def _analyze_measurement_changes(current_measurements: list[dict], baseline_measurements: list[dict]) -> dict:
    current = [dict(row or {}) for row in (current_measurements or [])]
    baseline = [dict(row or {}) for row in (baseline_measurements or [])]
    current_keys = [_measurement_compare_key(row) for row in current]
    baseline_keys = [_measurement_compare_key(row) for row in baseline]
    current_map = {_measurement_compare_key(row): row for row in current}
    baseline_map = {_measurement_compare_key(row): row for row in baseline}

    added_keys = [key for key in current_keys if key not in baseline_map]
    removed_keys = [key for key in baseline_keys if key not in current_map]
    shared_keys = [key for key in current_keys if key in baseline_map]
    changed_keys: list[str] = []
    for key in shared_keys:
        current_payload = _measurement_compare_payload(current_map[key], local=False)
        baseline_payload = _measurement_compare_payload(baseline_map[key], local=False)
        if any(
            not _measurement_field_values_match(
                field,
                current_payload.get(field),
                baseline_payload.get(field),
            )
            for field in _SNAPSHOT_MEAS_FIELDS
        ):
            changed_keys.append(key)

    return {
        'added_keys': added_keys,
        'removed_keys': removed_keys,
        'changed_keys': changed_keys,
        'added': [current_map[key] for key in added_keys],
        'removed': [baseline_map[key] for key in removed_keys],
        'changed': bool(added_keys or removed_keys or changed_keys),
    }


def _mark_cloud_observations_dirty_for_media_changes() -> None:
    current_signature = _cloud_media_signature()
    previous_signature = str(SettingsDB.get_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, '') or '').strip()
    if previous_signature == current_signature:
        return
    # Background cloud sync currently uploads source images/metadata, not the
    # optional rendered overlays/gallery/plate outputs tied to these settings.
    # Persist the new signature so future comparisons are stable, but don't mark
    # every linked observation dirty just because a global render preference changed.
    SettingsDB.set_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, current_signature)


def _cloud_publish_excluded_image_ids(observation_id: int | None) -> set[int]:
    """Local image ids the user has unchecked from cloud/publish upload.

    Mirrors ``ObservationsTab._publish_excluded_image_ids`` so the backend
    dirty-scan agrees with what the upload path actually mirrors.
    """
    if not observation_id:
        return set()
    key = f"artsobs_publish_excluded_image_ids_{int(observation_id or 0)}"
    raw = SettingsDB.get_setting(key, "[]")
    try:
        loaded = json.loads(raw or "[]")
        if isinstance(loaded, list):
            return {int(value) for value in loaded}
    except Exception:
        pass
    return set()


def _cloud_publish_path_key(path: str | None) -> str:
    if not path:
        return ""
    try:
        return str(Path(path).resolve()).lower()
    except Exception:
        return str(Path(path)).lower()


def _pending_cloud_pushable_image_ids(observation_id: int) -> list[int]:
    """Image ids still missing a cloud_id that cloud sync would actually push.

    This mirrors ``ObservationsTab._collect_cloud_sync_image_rows`` so the
    dirty-scan does not perpetually re-dirty observations over rows that sync
    intentionally skips — publish-excluded images, duplicate file paths and
    (for non cloud-origin rows) missing files. Without this alignment those
    rows keep ``cloud_id IS NULL`` forever and re-trigger the scan on every run.
    """
    try:
        excluded_ids = _cloud_publish_excluded_image_ids(observation_id)
    except Exception:
        excluded_ids = set()

    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        existing_cols = {
            str(info["name"])
            for info in conn.execute("PRAGMA table_info(images)").fetchall()
        }
        if "id" not in existing_cols or "image_type" not in existing_cols:
            return []
        wanted = [
            col
            for col in (
                "id", "image_type", "cloud_id", "filepath", "original_filepath",
                "source_role", "file_purpose", "notes", "sort_order",
            )
            if col in existing_cols
        ]
        order_bits: list[str] = []
        if "sort_order" in existing_cols:
            order_bits.append("CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END")
            order_bits.append("sort_order")
        order_bits.append("id")
        rows = conn.execute(
            f"SELECT {', '.join(wanted)} FROM images WHERE observation_id = ? "
            f"ORDER BY {', '.join(order_bits)}",
            (int(observation_id),),
        ).fetchall()
    finally:
        conn.close()

    pending: list[int] = []
    seen_paths: set[str] = set()
    for image in rows or []:
        row = dict(image)
        image_id = _safe_int(row.get("id"))
        if image_id <= 0 or image_id in excluded_ids:
            continue
        image_type = str(row.get("image_type") or "").strip().lower()
        if image_type not in {"field", "microscope"}:
            continue
        if not should_push_local_image_to_cloud(row):
            continue
        filepath = str(row.get("filepath") or row.get("original_filepath") or "").strip()
        source_role = str(row.get("source_role") or "").strip().lower()
        file_purpose = str(row.get("file_purpose") or "").strip().lower()
        is_cloud_origin = source_role == "cloud_recovery_cache" or file_purpose == "cache"
        if not is_cloud_origin and (not filepath or not Path(filepath).exists()):
            # Non cloud-origin rows are only pushed when their local file exists;
            # otherwise sync skips them and they would re-dirty forever.
            continue
        if filepath:
            path_key = _cloud_publish_path_key(filepath)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
        if not str(row.get("cloud_id") or "").strip():
            pending.append(image_id)
    return pending


def _mark_cloud_observations_dirty_for_pending_local_images() -> None:
    """Mark synced observations dirty when they still have cloud-eligible local images.

    This catches older observations that were left in a synced state after a
    previous sync skipped microscope images or otherwise failed to assign a
    cloud_id to newly added local media.
    """
    dirty_ids: list[int] = []
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT DISTINCT o.id AS observation_id
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            WHERE o.cloud_id IS NOT NULL
              AND i.cloud_id IS NULL
              AND i.image_type IN ('field', 'microscope')
            ORDER BY o.id
            """
        ).fetchall()
        candidate_ids = [
            _safe_int(dict(row or {}).get("observation_id"))
            for row in rows or []
        ]
    except Exception as exc:
        print(f"[cloud_sync] Could not mark observations dirty for pending local images: {exc}")
        candidate_ids = []
    finally:
        conn.close()

    for obs_id in candidate_ids:
        if obs_id <= 0:
            continue
        try:
            pending_ids = _pending_cloud_pushable_image_ids(obs_id)
        except Exception as exc:
            print(
                f"[cloud_sync] Could not evaluate pending local images for observation {obs_id}: {exc}"
            )
            continue
        pending_count = len(pending_ids)
        if pending_count > 0:
            print(
                f"[cloud_sync] Observation {obs_id}: re-dirtied because "
                f"{pending_count} cloud-eligible local image row(s) still have cloud_id IS NULL"
            )
            dirty_ids.append(obs_id)
    if not dirty_ids:
        return

    _increment_sync_summary(
        _cloud_sync_current_summary(),
        'observations_redirtied_pending_local_images',
        len(dirty_ids),
    )

    for obs_id in dirty_ids:
        try:
            _clear_local_cloud_media_signature(obs_id)
        except Exception as exc:
            print(
                f"[cloud_sync] Could not clear local media signature for observation {obs_id}: {exc}"
            )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        for obs_id in dirty_ids:
            mark_observation_sync_dirty(cursor, obs_id)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"[cloud_sync] Could not update dirty state for pending local images: {exc}")
    finally:
        conn.close()


def _has_pending_local_push_work() -> bool:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT 1 FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty' LIMIT 1"
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def _find_local_observation_for_remote(remote: dict) -> dict | None:
    cloud_id = str((remote or {}).get('id') or '').strip()
    desktop_id = (remote or {}).get('desktop_id')
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        if cloud_id:
            cursor.execute('SELECT * FROM observations WHERE cloud_id = ? LIMIT 1', (cloud_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        try:
            local_id = int(desktop_id)
        except (TypeError, ValueError):
            local_id = 0
        if local_id > 0:
            cursor.execute('SELECT * FROM observations WHERE id = ? LIMIT 1', (local_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    finally:
        conn.close()


def _load_local_observation_lookup() -> tuple[dict[str, dict], dict[int, dict]]:
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM observations')
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for row in rows:
        local_id = _safe_int(row.get('id'))
        cloud_id = str(row.get('cloud_id') or '').strip()
        if local_id > 0:
            by_local_id[local_id] = row
        if cloud_id:
            by_cloud_id[cloud_id] = row
    return by_cloud_id, by_local_id


def _load_local_measurement_lookup(observation_id: int) -> tuple[dict[str, dict], dict[int, dict]]:
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                m.*,
                i.cloud_id AS image_cloud_id
            FROM spore_measurements m
            JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ?
            ORDER BY m.id
            ''',
            (int(observation_id),),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for row in rows:
        local_id = _safe_int(row.get('id'))
        cloud_id = str(row.get('cloud_id') or '').strip()
        if local_id > 0:
            by_local_id[local_id] = row
        if cloud_id:
            by_cloud_id[cloud_id] = row
    return by_cloud_id, by_local_id


def _build_remote_measurement_identity_cache(remote_measurements: list[dict] | None) -> dict[str, dict]:
    cache: dict[str, dict] = {}
    for row in remote_measurements or []:
        remote_row = dict(row or {})
        cloud_id = str(remote_row.get('id') or '').strip()
        if cloud_id:
            cache[f'cloud:{cloud_id}'] = remote_row
        desktop_id = _safe_int(remote_row.get('desktop_id'))
        if desktop_id > 0:
            cache[f'desktop:{desktop_id}'] = remote_row
    return cache


def _measurement_push_lookup_keys(measurement_row: dict | None) -> list[str]:
    row = dict(measurement_row or {})
    keys: list[str] = []
    cloud_id = str(row.get('cloud_id') or '').strip()
    if cloud_id:
        keys.append(f'cloud:{cloud_id}')
    desktop_id = _safe_int(row.get('id'))
    if desktop_id > 0:
        keys.append(f'desktop:{desktop_id}')
    return keys


def fetch_remote_measurement_identity_cache(
    client,
    image_cloud_ids: list[str],
) -> dict[str, dict]:
    fetcher = getattr(client, 'pull_measurements_for_images', None)
    if not callable(fetcher):
        return {}
    remote_measurements = fetcher(image_cloud_ids)
    return _build_remote_measurement_identity_cache(remote_measurements)


def _load_local_image_lookup(observation_id: int) -> tuple[dict[str, dict], dict[int, dict]]:
    local_images = ImageDB.get_images_for_observation(int(observation_id))
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for image_row in local_images or []:
        local_image_id = _safe_int(image_row.get('id'))
        if local_image_id > 0:
            by_local_id[local_image_id] = dict(image_row or {})
        cloud_image_id = str(image_row.get('cloud_id') or '').strip()
        if cloud_image_id:
            by_cloud_id[cloud_image_id] = dict(image_row or {})
    return by_cloud_id, by_local_id


def _find_local_observation_for_remote_cached(
    remote: dict,
    by_cloud_id: dict[str, dict],
    by_local_id: dict[int, dict],
) -> dict | None:
    cloud_id = str((remote or {}).get('id') or '').strip()
    if cloud_id and cloud_id in by_cloud_id:
        return dict(by_cloud_id[cloud_id])
    local_id = _safe_int((remote or {}).get('desktop_id'))
    if local_id > 0 and local_id in by_local_id:
        return dict(by_local_id[local_id])
    return None


def _remote_observation_changed_since_last_sync(local_obs: dict | None, remote: dict | None) -> bool:
    if not local_obs:
        return True
    synced_at = _parse_sync_timestamp((local_obs or {}).get('synced_at'))
    remote_changed_at = _parse_sync_timestamp((remote or {}).get('updated_at') or (remote or {}).get('created_at'))
    if synced_at is None or remote_changed_at is None:
        return True
    return (remote_changed_at - synced_at).total_seconds() > _REMOTE_SYNC_TIMESTAMP_GRACE_SECONDS


def _remote_snapshot_has_meaningful_changes(
    remote: dict | None,
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None,
    stored_snapshot: str | None,
) -> bool:
    snapshot = _parse_cloud_observation_snapshot(stored_snapshot)
    if not snapshot:
        return True
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    remote_payload = _observation_compare_payload(remote, local=False)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if not _observation_field_values_match(field, remote_payload.get(field), baseline_obs.get(field)):
            return True
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]
    remote_image_payloads = [_remote_image_payload(img) for img in (remote_images or [])]
    remote_image_changes = _analyze_image_changes(remote_image_payloads, baseline_images)
    baseline_measurements = [dict(row or {}) for row in (snapshot.get('measurements') or [])]
    remote_measurement_payloads = [_remote_measurement_snapshot_payload(row) for row in (remote_measurements or [])]
    remote_measurement_changes = _analyze_measurement_changes(remote_measurement_payloads, baseline_measurements)
    return bool(
        remote_image_changes.get('added_keys')
        or remote_image_changes.get('removed_keys')
        or remote_image_changes.get('metadata_changed_keys')
        or remote_measurement_changes.get('changed')
    )


def _stamp_observation_synced(local_id: int, cloud_id: str) -> None:
    _set_observation_sync_state(int(local_id), str(cloud_id or '').strip(), dirty=False)


def _set_observation_sync_state(local_id: int, cloud_id: str, *, dirty: bool) -> None:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=str(cloud_id or '').strip() or None,
            sync_status='dirty' if dirty else 'synced',
            synced_at=datetime.now(timezone.utc).isoformat(),
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()


def _set_observation_sync_blocked(local_id: int, raw_error: str, blocked_reason: str, *, error_code: str | None = None) -> str:
    code, _ = _collect_sync_error_details(raw_error)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='blocked',
            sync_error_code=error_code or code or None,
            sync_error_message=str(raw_error or '').strip() or None,
            sync_blocked_reason=blocked_reason,
            sync_blocked_at=datetime.now(timezone.utc).isoformat(),
        )
        conn.commit()
    finally:
        conn.close()
    return blocked_reason


def _set_observation_privacy_blocked(local_id: int, raw_error: str) -> str:
    return _set_observation_sync_blocked(
        local_id,
        raw_error,
        privacy_slot_limit_user_message(),
        error_code='privacy_slot_limit',
    )


def _set_observation_plan_image_retryable(local_id: int, raw_error: str) -> str:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='dirty',
            sync_error_code='image_too_large_for_plan',
            sync_error_message=str(raw_error or '').strip() or None,
            sync_blocked_reason=None,
            sync_blocked_at=None,
        )
        conn.commit()
    finally:
        conn.close()
    return summarize_image_too_large_for_plan_error(raw_error)


def _set_observation_plan_image_blocked(local_id: int, raw_error: str) -> str:
    return _set_observation_sync_blocked(
        local_id,
        raw_error,
        IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE,
        error_code='image_too_large_for_plan',
    )


def _remote_observation_update_kwargs(remote: dict) -> dict:
    raw_location_public = remote.get('location_public')
    location_public = _normalize_observation_bool_value(raw_location_public, default=None)
    raw_publish_target = str(remote.get('publish_target') or '').strip()
    return {
        'date': remote.get('date'),
        'genus': remote.get('genus'),
        'species': remote.get('species'),
        'common_name': remote.get('common_name'),
        'species_guess': remote.get('species_guess'),
        'location': remote.get('location'),
        'habitat': remote.get('habitat'),
        'notes': remote.get('notes'),
        'open_comment': remote.get('open_comment'),
        'interesting_comment': _normalize_observation_bool_value(remote.get('interesting_comment'), default=False),
        'sharing_scope': _cloud_visibility_to_sharing_scope(
            remote.get('visibility') or remote.get('sharing_scope'),
            fallback='friends' if location_public else 'private',
        ),
        'location_public': location_public,
        'is_draft': _normalize_observation_bool_value(remote.get('is_draft'), default=True),
        'location_precision': ObservationDB._normalize_location_precision(
            remote.get('location_precision')
        ),
        'ai_selected_service': remote.get('ai_selected_service'),
        'ai_selected_taxon_id': remote.get('ai_selected_taxon_id'),
        'ai_selected_scientific_name': remote.get('ai_selected_scientific_name'),
        'ai_selected_probability': _normalize_observation_float_value(remote.get('ai_selected_probability')),
        'ai_selected_at': remote.get('ai_selected_at'),
        'spore_data_visibility': (lambda v: v if v in {'private', 'friends', 'public'} else 'public')(
            str(remote.get('spore_data_visibility') or 'public').strip().lower()
        ),
        'uncertain': _normalize_observation_bool_value(remote.get('uncertain'), default=False),
        'unspontaneous': _normalize_observation_bool_value(remote.get('unspontaneous'), default=False),
        'gps_latitude': _normalize_observation_float_value(remote.get('gps_latitude')),
        'gps_longitude': _normalize_observation_float_value(remote.get('gps_longitude')),
        'artsdata_id': _normalize_observation_int_value(remote.get('artsdata_id')),
        'artportalen_id': _normalize_observation_int_value(remote.get('artportalen_id')),
        'publish_target': normalize_publish_target(raw_publish_target) if raw_publish_target else None,
        'determination_method': _normalize_observation_int_value(remote.get('determination_method')),
        'habitat_nin2_path': remote.get('habitat_nin2_path'),
        'habitat_substrate_path': remote.get('habitat_substrate_path'),
        'habitat_host_genus': remote.get('habitat_host_genus'),
        'habitat_host_species': remote.get('habitat_host_species'),
        'habitat_host_common_name': remote.get('habitat_host_common_name'),
        'habitat_nin2_note': remote.get('habitat_nin2_note'),
        'habitat_substrate_note': remote.get('habitat_substrate_note'),
        'habitat_grows_on_note': remote.get('habitat_grows_on_note'),
        'allow_nulls': True,
    }


def _remote_observation_extra_values(remote: dict) -> dict:
    raw_spore_stats = remote.get('spore_statistics')
    serialized_spore_stats = _normalize_observation_json_value(raw_spore_stats)
    if serialized_spore_stats is not None and not isinstance(serialized_spore_stats, str):
        serialized_spore_stats = json.dumps(serialized_spore_stats, ensure_ascii=False, sort_keys=True)
    raw_auto_threshold = _normalize_observation_float_value(remote.get('auto_threshold'))
    return {
        'inaturalist_id': _normalize_observation_int_value(remote.get('inaturalist_id')),
        'mushroomobserver_id': _normalize_observation_int_value(remote.get('mushroomobserver_id')),
        'source_type': remote.get('source_type'),
        'citation': remote.get('citation'),
        'data_provider': remote.get('data_provider'),
        'author': remote.get('author'),
        'spore_statistics': serialized_spore_stats,
        'auto_threshold': raw_auto_threshold,
    }


def _merge_cloud_selected_ai_fields(local_obs: dict | None, remote_obs: dict | None) -> dict:
    """Preserve cloud-side selected AI values when the desktop row is still empty.

    Existing desktop observations may have `NULL` in the newly added fields until
    they are re-pulled from cloud. When we push an unrelated desktop edit, we
    don't want those missing local values to wipe the cloud selection.
    """
    merged = dict(local_obs or {})
    remote = dict(remote_obs or {})
    for field in (
        'ai_selected_service',
        'ai_selected_taxon_id',
        'ai_selected_scientific_name',
        'ai_selected_probability',
        'ai_selected_at',
    ):
        local_value = merged.get(field)
        if local_value not in (None, ''):
            continue
        remote_value = remote.get(field)
        if remote_value not in (None, ''):
            merged[field] = remote_value
    return merged


def _normalize_cloud_identification_service(value: object) -> str | None:
    raw = str(value or '').strip().lower()
    if raw in {'artsorakel', 'arts'}:
        return 'artsorakel'
    if raw in {'inat', 'inaturalist'}:
        return 'inat'
    return None


def _cloud_identification_prediction_taxon(prediction: dict, service: str | None = None) -> dict | None:
    pred = dict(prediction or {})
    taxon = dict(pred.get('taxon') or {})
    scientific_name = str(
        pred.get('scientificName')
        or pred.get('scientific_name')
        or pred.get('name')
        or ''
    ).strip()
    vernacular_name = str(
        pred.get('vernacularName')
        or pred.get('vernacular_name')
        or pred.get('commonName')
        or pred.get('common_name')
        or ''
    ).strip()
    taxon_id = pred.get('taxonId') or pred.get('taxon_id')

    if scientific_name:
        taxon.setdefault('scientificName', scientific_name)
        taxon.setdefault('scientific_name', scientific_name)
        taxon.setdefault('name', scientific_name)
    if vernacular_name:
        taxon.setdefault('vernacularName', vernacular_name)
        taxon.setdefault('vernacular_name', vernacular_name)
        taxon.setdefault('preferred_common_name', vernacular_name)
        taxon.setdefault('common_name', vernacular_name)
    if taxon_id not in (None, ''):
        taxon.setdefault('id', taxon_id)
        taxon.setdefault('taxonId', taxon_id)
        taxon.setdefault('taxon_id', taxon_id)
    if service == 'inat' and vernacular_name and not taxon.get('preferred_common_name'):
        taxon['preferred_common_name'] = vernacular_name

    return taxon or None


def _cloud_identification_prediction_display_name(prediction: dict) -> str:
    scientific_name = str(
        prediction.get('scientificName')
        or prediction.get('scientific_name')
        or prediction.get('name')
        or ''
    ).strip()
    vernacular_name = str(
        prediction.get('vernacularName')
        or prediction.get('vernacular_name')
        or prediction.get('commonName')
        or prediction.get('common_name')
        or ''
    ).strip()
    display_name = str(prediction.get('displayName') or prediction.get('display_name') or '').strip()

    if display_name:
        return display_name
    if vernacular_name and scientific_name and vernacular_name.casefold() != scientific_name.casefold():
        return f'{vernacular_name} ({scientific_name})'
    return vernacular_name or scientific_name


def _cloud_identification_prediction_species_url(prediction: dict, service: str | None = None) -> str | None:
    pred = dict(prediction or {})
    taxon = dict(pred.get('taxon') or {})
    if service == 'inat':
        taxon_id = str(
            pred.get('taxonId')
            or pred.get('taxon_id')
            or taxon.get('id')
            or taxon.get('taxonId')
            or taxon.get('taxon_id')
            or ''
        ).strip()
        if taxon_id:
            return f'https://www.inaturalist.org/taxa/{taxon_id}'
        return None

    for source in (pred, taxon):
        for key in (
            'species_url',
            'speciesUrl',
            'adbUrl',
            'url',
            'link',
            'href',
            'uri',
            'infoUrl',
            'infoURL',
            'info_url',
        ):
            value = source.get(key)
            if isinstance(value, str) and value.strip().startswith('http'):
                return value.strip()

    taxon_id = str(
        pred.get('taxonId')
        or pred.get('taxon_id')
        or taxon.get('taxonId')
        or taxon.get('taxon_id')
        or taxon.get('id')
        or ''
    ).strip()
    if taxon_id and taxon_id.isdigit():
        return f'https://artsdatabanken.no/arter/takson/{taxon_id}'
    return None


def _cloud_identification_prediction_matches_observation(prediction: dict, observation: dict | None) -> bool:
    obs = dict(observation or {})
    obs_scientific_name = str(
        obs.get('genus')
        or ''
    ).strip()
    obs_species = str(obs.get('species') or '').strip()
    if obs_scientific_name and obs_species:
        obs_scientific_name = f'{obs_scientific_name} {obs_species}'.strip()
    else:
        obs_scientific_name = str(obs.get('species_guess') or obs_scientific_name or '').strip()
    obs_common_name = str(obs.get('common_name') or '').strip()

    prediction_scientific_name = str(
        prediction.get('scientificName')
        or prediction.get('scientific_name')
        or prediction.get('name')
        or ''
    ).strip()
    taxon = dict(prediction.get('taxon') or {})
    if not prediction_scientific_name:
        prediction_scientific_name = str(
            taxon.get('scientificName')
            or taxon.get('scientific_name')
            or taxon.get('name')
            or ''
        ).strip()
    prediction_common_name = str(
        prediction.get('vernacularName')
        or prediction.get('vernacular_name')
        or prediction.get('commonName')
        or prediction.get('common_name')
        or ''
    ).strip()
    if not prediction_common_name:
        prediction_common_name = str(
            taxon.get('vernacularName')
            or taxon.get('vernacular_name')
            or taxon.get('preferred_common_name')
            or taxon.get('common_name')
            or ''
        ).strip()
    prediction_taxon_id = str(
        prediction.get('taxonId')
        or prediction.get('taxon_id')
        or ''
    ).strip()
    if not prediction_taxon_id:
        prediction_taxon_id = str(
            taxon.get('id')
            or taxon.get('taxonId')
            or taxon.get('taxon_id')
            or ''
        ).strip()
    selected_taxon_id = str(obs.get('ai_selected_taxon_id') or '').strip()
    selected_scientific_name = str(obs.get('ai_selected_scientific_name') or '').strip()

    if selected_taxon_id and prediction_taxon_id and selected_taxon_id == prediction_taxon_id:
        return True
    if selected_scientific_name and prediction_scientific_name and selected_scientific_name == prediction_scientific_name:
        return True
    if obs_scientific_name and prediction_scientific_name and obs_scientific_name == prediction_scientific_name:
        return True
    if obs_common_name and prediction_common_name and obs_common_name == prediction_common_name:
        return True
    return False


def build_cloud_ai_state_from_observation_identifications(
    observation: dict | None,
    identification_rows: list[dict] | None,
    local_images: list[dict] | None = None,
) -> dict | None:
    """Build the desktop AI-state cache from cloud observation_identifications rows.

    The cloud table stays authoritative; the desktop only keeps a derived cache
    so the observation detail dialog can render the same suggestions without a
    separate local AI run.
    """
    obs = dict(observation or {})
    rows = [dict(row or {}) for row in (identification_rows or []) if row]
    if not rows:
        return None

    local_image_rows = [dict(row or {}) for row in (local_images or []) if row]
    index_count = len(local_image_rows) or 1
    indices = list(range(index_count))
    paths = [str(row.get('filepath') or '').strip() for row in local_image_rows]
    image_ids = [_safe_int(row.get('id')) or None for row in local_image_rows]
    selected_service = _normalize_cloud_identification_service(obs.get('ai_selected_service'))

    service_rows: dict[str, dict] = {}
    for row in sorted(
        rows,
        key=lambda item: (
            _parse_sync_timestamp(item.get('created_at')) or datetime.min.replace(tzinfo=timezone.utc),
            _safe_int(item.get('id')) or 0,
        ),
        reverse=True,
    ):
        service = _normalize_cloud_identification_service(row.get('service'))
        if not service or service in service_rows:
            continue
        service_rows[service] = row

    predictions_by_service: dict[str, list[dict]] = {'artsorakel': [], 'inat': []}
    selected_by_service: dict[str, dict] = {}
    for service, row in service_rows.items():
        raw_predictions = [dict(pred or {}) for pred in (row.get('results') or []) if isinstance(pred, dict)]
        normalized_predictions: list[dict] = []
        for prediction in raw_predictions:
            taxon = _cloud_identification_prediction_taxon(prediction, service=service)
            if taxon:
                prediction['taxon'] = taxon
            if not str(prediction.get('scientificName') or '').strip():
                scientific_name = str(
                    prediction.get('scientific_name')
                    or taxon.get('scientificName')
                    or taxon.get('scientific_name')
                    or taxon.get('name')
                    or ''
                ).strip()
                if scientific_name:
                    prediction['scientificName'] = scientific_name
                    prediction['scientific_name'] = scientific_name
            if not str(prediction.get('vernacularName') or '').strip():
                vernacular_name = str(
                    prediction.get('vernacular_name')
                    or taxon.get('vernacularName')
                    or taxon.get('vernacular_name')
                    or taxon.get('preferred_common_name')
                    or taxon.get('common_name')
                    or ''
                ).strip()
                if vernacular_name:
                    prediction['vernacularName'] = vernacular_name
                    prediction['vernacular_name'] = vernacular_name
            if not str(prediction.get('displayName') or '').strip():
                display_name = _cloud_identification_prediction_display_name(prediction)
                if display_name:
                    prediction['displayName'] = display_name
                    prediction['display_name'] = display_name
            species_url = _cloud_identification_prediction_species_url(prediction, service=service)
            if species_url:
                prediction['species_url'] = species_url
                prediction['speciesUrl'] = species_url
                prediction['adbUrl'] = species_url
            normalized_predictions.append(prediction)

        if not normalized_predictions:
            top_scientific_name = str(row.get('top_scientific_name') or '').strip()
            top_vernacular_name = str(row.get('top_vernacular_name') or '').strip()
            top_taxon_id = str(row.get('top_taxon_id') or '').strip()
            top_species_url = str(
                row.get('top_species_url')
                or row.get('top_speciesUrl')
                or row.get('top_adbUrl')
                or ''
            ).strip()
            if top_scientific_name or top_vernacular_name or top_taxon_id:
                synth_prediction = {
                    'service': service,
                    'rank': 1,
                    'scientificName': top_scientific_name or None,
                    'scientific_name': top_scientific_name or None,
                    'vernacularName': top_vernacular_name or None,
                    'vernacular_name': top_vernacular_name or None,
                    'displayName': top_vernacular_name or top_scientific_name or top_taxon_id or 'Unknown',
                    'taxonId': top_taxon_id or None,
                    'taxon_id': top_taxon_id or None,
                    'probability': row.get('top_probability'),
                }
                if top_species_url:
                    synth_prediction['species_url'] = top_species_url
                    synth_prediction['speciesUrl'] = top_species_url
                    synth_prediction['adbUrl'] = top_species_url
                taxon = _cloud_identification_prediction_taxon(synth_prediction, service=service)
                if taxon:
                    synth_prediction['taxon'] = taxon
                normalized_predictions = [synth_prediction]

        if not normalized_predictions:
            continue

        selected_prediction = None
        if selected_service == service:
            selected_prediction = next(
                (
                    prediction
                    for prediction in normalized_predictions
                    if _cloud_identification_prediction_matches_observation(prediction, obs)
                ),
                None,
            )

        predictions_by_service[service] = normalized_predictions
        if selected_prediction:
            selected_by_service[service] = selected_prediction

    if not any(predictions_by_service.values()) and not selected_by_service:
        selected_service = _normalize_cloud_identification_service(obs.get('ai_selected_service'))
        selected_scientific_name = str(obs.get('ai_selected_scientific_name') or '').strip()
        selected_taxon_id = str(obs.get('ai_selected_taxon_id') or '').strip()
        if selected_service and (selected_scientific_name or selected_taxon_id):
            synth_prediction = {
                'service': selected_service,
                'rank': 1,
                'scientificName': selected_scientific_name or None,
                'scientific_name': selected_scientific_name or None,
                'vernacularName': str(obs.get('common_name') or '').strip() or None,
                'vernacular_name': str(obs.get('common_name') or '').strip() or None,
                'displayName': selected_scientific_name or str(obs.get('common_name') or '').strip() or selected_taxon_id or 'Unknown',
                'taxonId': selected_taxon_id or None,
                'taxon_id': selected_taxon_id or None,
                'probability': obs.get('ai_selected_probability'),
            }
            taxon = _cloud_identification_prediction_taxon(synth_prediction, service=selected_service)
            if taxon:
                synth_prediction['taxon'] = taxon
            predictions_by_service[selected_service] = [synth_prediction]
            selected_by_service[selected_service] = synth_prediction

    if not any(predictions_by_service.values()) and not selected_by_service:
        return None

    state: dict = {
        'predictions': {},
        'selected': {},
        'inat_predictions': {},
        'inat_selected': {},
        'selected_index': indices[0] if indices else None,
        'paths': paths,
        'image_ids': image_ids,
    }

    for index in indices:
        if predictions_by_service.get('artsorakel'):
            state['predictions'][index] = [dict(pred or {}) for pred in predictions_by_service['artsorakel']]
            if selected_by_service.get('artsorakel'):
                state['selected'][index] = dict(selected_by_service['artsorakel'])
        if predictions_by_service.get('inat'):
            state['inat_predictions'][index] = [dict(pred or {}) for pred in predictions_by_service['inat']]
            if selected_by_service.get('inat'):
                state['inat_selected'][index] = dict(selected_by_service['inat'])

    return state


def _apply_remote_observation_fields(
    local_id: int,
    remote: dict,
    *,
    fields: set[str] | None = None,
) -> None:
    requested_fields = {
        str(field or '').strip()
        for field in (fields or set(_SNAPSHOT_OBS_FIELDS))
        if str(field or '').strip()
    }
    if not requested_fields:
        return

    normalized_fields = {
        'sharing_scope' if field in {'visibility', 'sharing_scope'} else field
        for field in requested_fields
    }
    update_kwargs = _remote_observation_update_kwargs(remote)
    partial_kwargs = {
        key: value
        for key, value in update_kwargs.items()
        if key == 'allow_nulls' or key in normalized_fields
    }
    if len(partial_kwargs) > 1:
        ObservationDB.update_observation(int(local_id), **partial_kwargs)

    extra_values = _remote_observation_extra_values(remote)
    extra_updates = {
        key: value
        for key, value in extra_values.items()
        if key in normalized_fields
    }
    if not extra_updates:
        return

    conn = get_connection()
    try:
        assignments = [f'{column} = ?' for column in extra_updates]
        values = list(extra_updates.values())
        values.append(int(local_id))
        conn.execute(
            f"UPDATE observations SET {', '.join(assignments)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
    finally:
        conn.close()


def _inject_obs_exif_into_field_image(
    image_path: Path,
    obs_lat: float | None,
    obs_lon: float | None,
    obs_altitude: float | None,
    obs_datetime_str: str | None,
    camera_model: str | None = None,
    iso: int | None = None,
    exposure_time: float | None = None,
    f_number: float | None = None,
    gps_accuracy: float | None = None,
) -> None:
    """Write observation GPS/datetime and camera metadata into an image that has no EXIF.

    Called on cloud-synced field images whose EXIF was stripped by the web
    app's conversion.  Only modifies the file when the image has no
    existing DateTimeOriginal AND the observation has GPS or datetime data.
    Does nothing for unsupported files or on any error.
    """
    if not image_path.exists():
        return
    suffix = image_path.suffix.lower()
    if suffix not in {'.jpg', '.jpeg', '.webp'}:
        return
    has_coords = obs_lat is not None and obs_lon is not None
    has_datetime = bool(obs_datetime_str)
    has_camera_data = any(x is not None for x in (camera_model, iso, exposure_time, f_number))
    if not has_coords and not has_datetime and not has_camera_data:
        return
    try:
        from PIL import Image as _PilImage, ExifTags as _ExifTags
        with _PilImage.open(image_path) as img:
            existing_exif = img.getexif()
            existing_tags = {
                _ExifTags.TAGS.get(k, k): v for k, v in existing_exif.items()
            } if existing_exif else {}
            already_has_dt = any(
                t in existing_tags
                for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
            )
            try:
                already_has_gps = bool(existing_exif.get_ifd(0x8825))
            except Exception:
                already_has_gps = False
                
            already_has_camera = any(
                t in existing_tags
                for t in ('Model', 'Make', 'ISOSpeedRatings', 'ExposureTime', 'FNumber')
            )
            
            if already_has_dt and already_has_gps and already_has_camera:
                return  # nothing to do

            exif = existing_exif if existing_exif is not None else img.getexif()

            if not already_has_dt and has_datetime:
                try:
                    dt_exif = _exif_datetime_from_text(obs_datetime_str)
                    # Tag 306 = DateTime, 36867 = DateTimeOriginal, 36868 = DateTimeDigitized
                    if dt_exif:
                        exif[306] = dt_exif
                        exif[36867] = dt_exif
                        exif[36868] = dt_exif
                except Exception:
                    pass

            if not already_has_gps and has_coords:
                try:
                    def _deg_to_rational(deg_float):
                        d = int(abs(deg_float))
                        m_float = (abs(deg_float) - d) * 60
                        m = int(m_float)
                        s_float = (m_float - m) * 60
                        s_num = int(round(s_float * 1000))
                        return ((d, 1), (m, 1), (s_num, 1000))

                    gps_ifd = {
                        1: 'N' if obs_lat >= 0 else 'S',    # GPSLatitudeRef
                        2: _deg_to_rational(obs_lat),        # GPSLatitude
                        3: 'E' if obs_lon >= 0 else 'W',    # GPSLongitudeRef
                        4: _deg_to_rational(obs_lon),        # GPSLongitude
                    }
                    if obs_altitude is not None:
                        altitude = float(obs_altitude)
                        gps_ifd[5] = 1 if altitude < 0 else 0  # GPSAltitudeRef
                        gps_ifd[6] = (int(round(abs(altitude) * 100)), 100)
                    if gps_accuracy is not None:
                        acc = float(gps_accuracy)
                        if acc >= 0:
                            gps_ifd[31] = (int(round(acc * 100)), 100)  # GPSHPositioningError
                    exif[34853] = gps_ifd  # GPSInfo
                except Exception:
                    pass
                    
            if not already_has_camera:
                try:
                    if camera_model:
                        exif[272] = camera_model  # Model
                    if iso is not None:
                        exif[34855] = int(iso)  # ISOSpeedRatings
                    if exposure_time is not None:
                        try:
                            ex_time = float(exposure_time)
                            if ex_time > 0:
                                if ex_time >= 1:
                                    exif[33434] = (int(round(ex_time * 1000)), 1000)  # ExposureTime
                                else:
                                    exif[33434] = (1, int(round(1 / ex_time)))
                        except Exception:
                            pass
                    if f_number is not None:
                        try:
                            fn = float(f_number)
                            if fn > 0:
                                exif[33437] = (int(round(fn * 10)), 10)  # FNumber
                        except Exception:
                            pass
                except Exception:
                    pass

            mode = img.mode
            if suffix in {'.jpg', '.jpeg'} and mode not in {'RGB', 'L'}:
                img = img.convert('RGB')
            try:
                exif_bytes = exif.tobytes()
                if suffix == '.webp':
                    save_kwargs = {'format': 'WEBP', 'exif': exif_bytes}
                    if mode == 'RGBA':
                        save_kwargs['lossless'] = True
                    else:
                        save_kwargs['quality'] = 96
                else:
                    save_kwargs = {'format': 'JPEG', 'exif': exif_bytes, 'quality': 92}
                img.save(image_path, **save_kwargs)
            except Exception:
                pass
    except Exception as exc:
        print(f'[cloud_sync] Could not inject EXIF into {image_path.name}: {exc}')


def _exif_datetime_from_text(value: str | None) -> str | None:
    """Return EXIF datetime text (YYYY:MM:DD HH:MM:SS) from ISO/date text."""
    text = str(value or '').strip()
    if not text:
        return None
    try:
        normalized = text.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime('%Y:%m:%d %H:%M:%S')
    except Exception:
        pass
    try:
        if 'T' in text:
            date_part, time_part = text.split('T', 1)
        elif ' ' in text:
            date_part, time_part = text.split(' ', 1)
        else:
            date_part, time_part = text, '00:00:00'
        time_part = time_part.split('+', 1)[0].split('-', 1)[0].split('.', 1)[0]
        bits = [part for part in time_part.split(':') if part]
        while len(bits) < 3:
            bits.append('00')
        return f"{date_part.replace('-', ':')} {':'.join(bits[:3])}"
    except Exception:
        return None


def _load_obs_exif_fallback(observation_id: int, fallback_datetime: str | None = None) -> tuple[float | None, float | None, float | None, float | None, str | None]:
    """Return (lat, lon, altitude, gps_accuracy, datetime_str) from local observation data."""
    try:
        obs = ObservationDB.get_observation(observation_id)
        if not obs:
            return None, None, None, None, fallback_datetime
        lat = obs.get('gps_latitude')
        lon = obs.get('gps_longitude')
        altitude = obs.get('gps_altitude')
        accuracy = obs.get('gps_accuracy')
        datetime_str = str(
            obs.get('captured_at')
            or obs.get('date')
            or fallback_datetime
            or ''
        ).strip() or None
        return (float(lat) if lat is not None else None,
                float(lon) if lon is not None else None,
                float(altitude) if altitude is not None else None,
                float(accuracy) if accuracy is not None else None,
                datetime_str)
    except Exception:
        return None, None, None, None, fallback_datetime


def _is_missing_cloud_image_error(exc: Exception | str | None) -> bool:
    text = str(exc or '').strip().lower()
    if not text:
        return False
    return (
        'cloud image file is missing from storage' in text
        or 'nosuchkey' in text
    )


def _cloud_missing_image_warning(local_id: int, remote_image: dict) -> str:
    cloud_image_id = str(remote_image.get('id') or '').strip() or '?'
    filename = Path(str(remote_image.get('original_filename') or '')).name or f'cloud image {cloud_image_id}'
    return (
        f'obs {int(local_id)}: skipped missing cloud image {cloud_image_id}'
        f' ({filename})'
    )


def _cloud_thumb_save_format(path: Path) -> tuple[str, str, dict]:
    if features.check('webp'):
        return 'WEBP', 'image/webp', {'quality': 65, 'method': 4}
    return 'JPEG', 'image/jpeg', {'quality': 72}


def _cloud_upload_policy_from_meta(upload_meta: dict | None) -> dict[str, object]:
    meta = dict(upload_meta or {})
    upload_mode = str(meta.get('upload_mode') or meta.get('uploadMode') or 'full').strip().lower() or 'full'
    cloud_plan = str(meta.get('cloud_plan') or meta.get('cloudPlan') or '').strip().lower()
    quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    profile = {}
    if cloud_plan:
        profile['cloud_plan'] = cloud_plan
    elif quality_profile == 'high':
        profile['is_pro'] = True
    return build_cloud_upload_policy(normalize_cloud_plan_profile(profile), upload_mode=upload_mode)


def _prepare_cloud_image_upload_file(
    source_path: str,
    temp_dir: Path,
    image_id: int,
    upload_meta: dict | None,
) -> tuple[Path, int, int, int, int, str, int | float | None]:
    source = Path(str(source_path or '').strip())
    if not source.exists():
        raise FileNotFoundError(source)

    policy = _cloud_upload_policy_from_meta(upload_meta)
    resize_max_pixels = max(
        1,
        int(
            policy.get('resizeMaxPixels')
            or policy.get('resize_max_pixels')
            or policy.get('maxPixels')
            or 0
        ) or 20_000_000,
    )
    resize_max_edge = policy.get('resizeMaxEdge') or policy.get('resize_max_edge')
    quality_profile = str(policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
    byte_cap = int(policy.get('fullImageByteCap') or 0)
    webp_qualities = list(build_full_image_webp_quality_attempts(quality_profile))

    if not features.check('webp'):
        raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE)

    with Image.open(source) as img:
        img = ImageOps.exif_transpose(img)
        source_width = int(img.width or 0)
        source_height = int(img.height or 0)
        if not source_width or not source_height:
            raise RuntimeError('Could not determine image dimensions')

        if img.mode in ('RGBA', 'LA') or 'transparency' in img.info:
            rgba = img.convert('RGBA')
            background = Image.new('RGB', rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel('A'))
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        target = scale_dimensions_to_max_pixels(source_width, source_height, resize_max_pixels, resize_max_edge)
        if target['resized']:
            img = img.resize((int(target['width']), int(target['height'])), Image.Resampling.LANCZOS)

        for attempt_quality in webp_qualities:
            buffer = io.BytesIO()
            img.save(buffer, 'WEBP', quality=attempt_quality, method=4)
            data = buffer.getvalue()
            if byte_cap and len(data) > byte_cap:
                continue

            out_path = temp_dir / f'cloud_{int(image_id):04d}.webp'
            out_path.write_bytes(data)
            try:
                source_stat = source.stat()
                os.utime(out_path, (source_stat.st_atime, source_stat.st_mtime))
            except Exception:
                pass

            return (
                out_path,
                source_width,
                source_height,
                int(img.width or 0),
                int(img.height or 0),
                'image/webp',
                attempt_quality,
            )

    raise CloudSyncError(IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE)


def _profile_generate_all_sizes(image_path: str, image_id: int):
    profiler = _cloud_sync_current_profiler()
    start = _cloud_sync_perf_counter()
    try:
        return generate_all_sizes(image_path, image_id)
    finally:
        if profiler is not None:
            try:
                profiler.record_generate_all_sizes(max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0))
            except Exception:
                pass


def _content_type_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == '.webp':
        return 'image/webp'
    return mimetypes.guess_type(path.name)[0] or 'image/jpeg'


def _detected_image_extension(path: str | Path) -> str:
    try:
        with Image.open(path) as img:
            fmt = str(img.format or '').strip().upper()
    except Exception:
        return Path(path).suffix.lower() or '.jpg'
    if fmt == 'WEBP':
        return '.webp'
    if fmt == 'AVIF':
        return '.avif'
    if fmt in {'JPEG', 'JPG'}:
        return '.jpg'
    if fmt == 'PNG':
        return '.png'
    if fmt == 'TIFF':
        return '.tif'
    return Path(path).suffix.lower() or '.jpg'


def _rename_to_detected_image_extension(path: Path) -> Path:
    detected_ext = _detected_image_extension(path)
    if not detected_ext or path.suffix.lower() == detected_ext:
        return path
    target = path.with_suffix(detected_ext)
    counter = 1
    while target.exists() and target != path:
        target = path.with_name(f"{path.stem}_{counter}{detected_ext}")
        counter += 1
    path.rename(target)
    return target


def _sync_existing_remote_image_to_local(
    client: "SporelyCloudClient",
    local_image: dict,
    remote_image: dict,
    materialize_remote_images: bool = True,
) -> None:
    if not materialize_remote_images:
        return
    image_id = int(local_image.get('id'))
    existing_path = str(local_image.get('filepath') or '').strip()
    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_image_{image_id}_'))
    try:
        filename = Path(str(remote_image.get('original_filename') or '')).name or f'cloud_{image_id}.jpg'
        temp_path = temp_dir / filename
        client.download_image_file(str(remote_image.get('storage_path') or ''), temp_path)
        temp_path = _rename_to_detected_image_extension(temp_path)
        image_type = str(remote_image.get('image_type') or 'field').strip().lower()
        target_path = Path(existing_path) if existing_path else temp_path

        # Preserve any existing local field image. Cloud field copies are the
        # reduced sync artifact, so metadata can update without replacing the
        # desktop original bytes.
        local_file_exists = bool(existing_path and Path(existing_path).exists())
        local_is_larger = bool(local_file_exists and image_type == 'field')

        if image_type == 'field' and not local_is_larger:
            obs_id = int(local_image.get('observation_id') or 0)
            if obs_id > 0:
                lat, lon, altitude, gps_acc, datetime_str = _load_obs_exif_fallback(
                    obs_id,
                    fallback_datetime=remote_image.get('captured_at'),
                )
                img_lat = remote_image.get('gps_latitude') if remote_image.get('gps_latitude') is not None else lat
                img_lon = remote_image.get('gps_longitude') if remote_image.get('gps_longitude') is not None else lon
                img_alt = remote_image.get('gps_altitude') if remote_image.get('gps_altitude') is not None else altitude
                img_acc = remote_image.get('gps_accuracy') if remote_image.get('gps_accuracy') is not None else gps_acc
                _inject_obs_exif_into_field_image(
                    temp_path, img_lat, img_lon, img_alt, datetime_str,
                    camera_model=remote_image.get('camera_model'),
                    iso=remote_image.get('iso'),
                    exposure_time=remote_image.get('exposure_time'),
                    f_number=remote_image.get('f_number'),
                    gps_accuracy=img_acc,
                )

        if existing_path and not local_is_larger:
            detected_ext = _detected_image_extension(temp_path)
            if detected_ext and target_path.suffix.lower() != detected_ext:
                target_path = target_path.with_suffix(detected_ext)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(temp_path, target_path)
        # If local is larger it is the full-res desktop-imported original — keep it as-is.

        calibration_id = _local_calibration_id_for_image(remote_image)
        update_kwargs = {
            'filepath': str(target_path),
            'image_type': str(remote_image.get('image_type') or 'field'),
            'scale': remote_image.get('scale_microns_per_pixel'),
            'notes': remote_image.get('notes'),
            'micro_category': remote_image.get('micro_category'),
            'objective_name': remote_image.get('objective_name'),
            'measure_color': remote_image.get('measure_color'),
            'mount_medium': remote_image.get('mount_medium'),
            'stain': remote_image.get('stain'),
            'sample_type': remote_image.get('sample_type'),
            'contrast': remote_image.get('contrast'),
            'crop_mode': remote_image.get('crop_mode'),
            'sort_order': remote_image.get('sort_order'),
            'gps_source': remote_image.get('gps_source'),
            'resample_scale_factor': remote_image.get('resample_scale_factor'),
            'ai_crop_box': _remote_ai_crop_box(remote_image),
            'ai_crop_source_size': _remote_ai_crop_source_size(remote_image),
            'ai_crop_is_custom': _remote_ai_crop_is_custom(remote_image),
        }
        if calibration_id is not None:
            update_kwargs['calibration_id'] = calibration_id
        ImageDB.update_image(image_id, **update_kwargs)
        conn = get_connection()
        try:
            conn.execute(
                'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                (str(remote_image.get('id') or '').strip() or None, datetime.now(timezone.utc).isoformat(), image_id),
            )
            conn.commit()
        finally:
            conn.close()
        try:
            _profile_generate_all_sizes(str(target_path), image_id)
        except Exception:
            pass
        try:
            file_sig = _file_content_signature(str(target_path))
            if file_sig:
                _store_cloud_image_file_signature(int(local_image.get('observation_id') or 0), image_id, file_sig)
        except Exception:
            pass
        _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _apply_remote_images_to_local(
    client: "SporelyCloudClient",
    local_id: int,
    remote_images: list[dict],
    *,
    allow_delete: bool = True,
    materialize_remote_images: bool = True,
) -> list[str]:
    warnings: list[str] = []
    if not materialize_remote_images:
        return warnings
    local_images = ImageDB.get_images_for_observation(int(local_id))
    local_cloud_map = {
        str(img.get('cloud_id') or '').strip(): img
        for img in local_images
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('cloud_id') or '').strip()
    }
    remote_map = {
        str(img.get('id') or '').strip(): img
        for img in (remote_images or [])
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('id') or '').strip()
    }
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(remote_map.keys())

    for cloud_image_id, remote_image in remote_map.items():
        if cloud_image_id in tombstoned_cloud_ids:
            warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        local_image = local_cloud_map.get(cloud_image_id)
        if local_image:
            try:
                _sync_existing_remote_image_to_local(
                    client,
                    local_image,
                    remote_image,
                    materialize_remote_images=materialize_remote_images,
                )
            except CloudSyncError as exc:
                if _is_missing_cloud_image_error(exc):
                    print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, remote_image)}')
                    continue
                raise
            try:
                client.set_image_desktop_id(cloud_image_id, int(local_image.get('id')))
                remote_image['desktop_id'] = int(local_image.get('id'))
            except Exception:
                pass
            continue

        storage_path = _normalize_cloud_media_key(remote_image.get('storage_path'))
        if not storage_path:
            continue
        temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
        try:
            filename = Path(str(remote_image.get('original_filename') or '')).name or f'{cloud_image_id}.jpg'
            download_path = temp_dir / filename
            try:
                client.download_image_file(storage_path, download_path)
                download_path = _rename_to_detected_image_extension(download_path)
            except CloudSyncError as exc:
                if _is_missing_cloud_image_error(exc):
                    print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, remote_image)}')
                    continue
                raise
            new_image_type = str(remote_image.get('image_type') or 'field').strip().lower()
            if new_image_type == 'field':
                lat, lon, altitude, gps_acc, datetime_str = _load_obs_exif_fallback(
                    int(local_id),
                    fallback_datetime=remote_image.get('captured_at'),
                )
                img_lat = remote_image.get('gps_latitude') if remote_image.get('gps_latitude') is not None else lat
                img_lon = remote_image.get('gps_longitude') if remote_image.get('gps_longitude') is not None else lon
                img_alt = remote_image.get('gps_altitude') if remote_image.get('gps_altitude') is not None else altitude
                img_acc = remote_image.get('gps_accuracy') if remote_image.get('gps_accuracy') is not None else gps_acc
                _inject_obs_exif_into_field_image(
                    download_path, img_lat, img_lon, img_alt, datetime_str,
                    camera_model=remote_image.get('camera_model'),
                    iso=remote_image.get('iso'),
                    exposure_time=remote_image.get('exposure_time'),
                    f_number=remote_image.get('f_number'),
                    gps_accuracy=img_acc,
                )
            local_image_id = ImageDB.add_image(
                observation_id=int(local_id),
                filepath=str(download_path),
                image_type=str(remote_image.get('image_type') or 'field'),
                scale=remote_image.get('scale_microns_per_pixel'),
                notes=remote_image.get('notes'),
                micro_category=remote_image.get('micro_category'),
                objective_name=remote_image.get('objective_name'),
                measure_color=remote_image.get('measure_color'),
                mount_medium=remote_image.get('mount_medium'),
                stain=remote_image.get('stain'),
                sample_type=remote_image.get('sample_type'),
                contrast=remote_image.get('contrast'),
                crop_mode=remote_image.get('crop_mode'),
                sort_order=remote_image.get('sort_order'),
                gps_source=remote_image.get('gps_source'),
                resample_scale_factor=remote_image.get('resample_scale_factor'),
                calibration_id=_local_calibration_id_for_image(remote_image),
                ai_crop_box=_remote_ai_crop_box(remote_image),
                ai_crop_source_size=_remote_ai_crop_source_size(remote_image),
                ai_crop_is_custom=_remote_ai_crop_is_custom(remote_image),
                captured_at=remote_image.get('captured_at'),
                copy_to_folder=True,
                mark_observation_dirty=False,
                source_role='cloud_recovery_cache',
                file_purpose='cache',
                original_mime_type=None,
                working_mime_type=guess_local_image_mime_type(download_path),
            )
            conn = get_connection()
            try:
                conn.execute(
                    'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                    (cloud_image_id, datetime.now(timezone.utc).isoformat(), int(local_image_id)),
                )
                conn.commit()
            finally:
                conn.close()
            try:
                client.set_image_desktop_id(cloud_image_id, int(local_image_id))
                remote_image['desktop_id'] = int(local_image_id)
            except Exception:
                pass
            try:
                _profile_generate_all_sizes(str(download_path), int(local_image_id))
            except Exception:
                pass
            _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    if allow_delete:
        for cloud_image_id, local_image in local_cloud_map.items():
            if cloud_image_id in remote_map:
                continue
            image_id = int(local_image.get('id') or 0)
            if image_id <= 0:
                continue
            if MeasurementDB.get_measurements_for_image(image_id):
                warnings.append(
                    f"obs {local_id}: kept local image {image_id} because it has measurements, even though the cloud copy was removed"
                )
                continue
            try:
                ImageDB.delete_image(image_id)
            except Exception as exc:
                warnings.append(f"obs {local_id}: could not remove local image {image_id}: {exc}")

    return warnings


def _store_remote_snapshot(
    client: "SporelyCloudClient",
    cloud_id: str,
    remote: dict | None = None,
    remote_images: list[dict] | None = None,
    remote_measurements: list[dict] | None = None,
) -> None:
    cloud_value = str(cloud_id or '').strip()
    if not cloud_value:
        return
    remote_obs = remote or client.get_observation(cloud_value)
    if not remote_obs:
        return
    profiler = _cloud_sync_current_profiler()
    if profiler is not None:
        try:
            profiler.record_store_remote_snapshot_fetch(images=remote_images is None)
            profiler.record_store_remote_snapshot_fetch(measurements=remote_measurements is None)
        except Exception:
            pass
    images = (
        [dict(row or {}) for row in (remote_images or [])]
        if remote_images is not None
        else [dict(row or {}) for row in (client.pull_image_metadata(cloud_value) or [])]
    )
    if remote_measurements is not None:
        measurements = [dict(row or {}) for row in remote_measurements]
    else:
        measurements = list(_pull_remote_measurements_for_images(
            client,
            [str(row.get('id') or '').strip() for row in images if str(row.get('id') or '').strip()],
        ))
    _store_cloud_observation_snapshot(
        cloud_value,
        _cloud_observation_snapshot(remote_obs, images, measurements),
    )


def _prompt_for_deleted_cloud_observations(self, deleted_remote: list[dict]) -> bool:
        """Refined to ensure local files aren't deleted without explicit user choice."""
        entries = [dict(row or {}) for row in (deleted_remote or []) if row]
        if not entries:
            return False
        
        changed = False
        for entry in entries:
            local_id = int(entry.get('local_id') or 0)
            if local_id <= 0: continue

            box = QMessageBox(self)
            box.setIcon(QMessageBox.Question)
            box.setWindowTitle('Cloud Observation Deleted')
            box.setText(f"Observation was deleted from Sporely Cloud.")
            box.setInformativeText(
                self._format_deleted_cloud_observation_label(entry) +
                "\n\nHow would you like to handle the local desktop copy?"
            )
            
            # Action Buttons
            keep_btn = box.addButton('Keep local only (Unlink)', QMessageBox.NoRole)
            delete_btn = box.addButton('Delete local copy', QMessageBox.DestructiveRole)
            box.setDefaultButton(keep_btn)
            
            box.exec()
            clicked = box.clickedButton()
            
            if clicked is delete_btn:
                # Double check for files specifically
                confirm = QMessageBox.warning(
                    self, "Confirm Delete",
                    "This will permanently delete the observation record and associated local image references. Continue?",
                    QMessageBox.Yes | QMessageBox.No
                )
                if confirm == QMessageBox.Yes:
                    ObservationDB.delete_observation(local_id)
                    changed = True
            else:
                # User chose to keep it local but remove the cloud link
                unlink_local_observation_from_cloud(local_id)
                changed = True
        
        # ... (refresh logic) ...
        return changed


def resolve_conflict_keep_local(
    client: "SporelyCloudClient",
    local_id: int,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    remote_obs = None
    cloud_value = str(local_obs.get('cloud_id') or '').strip()
    if cloud_value:
        try:
            remote_obs = client.get_observation(cloud_value)
        except Exception:
            remote_obs = None

    try:
        cloud_id = client.push_observation(
            _merge_cloud_selected_ai_fields(local_obs, remote_obs),
            remote_obs=remote_obs,
        )
    except Exception as exc:
        if not is_privacy_slot_limit_error(exc):
            raise
        blocked_reason = _set_observation_privacy_blocked(int(local_id), str(exc))
        return {
            'local_id': int(local_id),
            'cloud_id': None,
            'blocked': True,
            'blocked_reason': blocked_reason,
            'raw_error': str(exc),
        }
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=cloud_id,
            sync_status='synced',
            synced_at=datetime.now(timezone.utc).isoformat(),
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()

    should_push_images = prepare_images_cb is not None
    stored_local_media_signature = _load_local_cloud_media_signature(int(local_id))
    current_local_media_signature = _local_cloud_media_signature(int(local_id))
    local_media_changed = bool(
        stored_local_media_signature
        and current_local_media_signature
        and not _local_media_signatures_match(
            stored_local_media_signature,
            current_local_media_signature,
        )
    )
    if not local_media_changed:
        _store_local_media_signature_if_equivalent(
            int(local_id),
            stored_local_media_signature,
            current_local_media_signature,
        )

    remote_images_raw = _pull_remote_images_for_sync(client, cloud_id) if cloud_id else []
    if remote_images_raw:
        _record_remote_image_tombstones(
            remote_images_raw,
            local_observation_id=int(local_id),
            cloud_observation_id=cloud_id,
        )
        tombstoned_remote_image_keys = _deleted_remote_image_identity_keys(remote_images_raw)

    if should_push_images and cloud_id:
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id)
        if stored_snapshot:
            baseline_images = [dict(row or {}) for row in (_parse_cloud_observation_snapshot(stored_snapshot).get('images') or [])]
            remote_images = [
                dict(row or {})
                for row in remote_images_raw
                if should_pull_cloud_image_to_desktop(row)
                and not str(row.get('deleted_at') or '').strip()
            ]
            remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
            remote_image_changes = _analyze_image_changes(
                remote_image_payloads,
                baseline_images,
                ignored_keys=tombstoned_remote_image_keys,
            )
            should_push_images = bool(
                local_media_changed
                or remote_image_changes.get('added_keys')
                or remote_image_changes.get('removed_keys')
                or remote_image_changes.get('metadata_changed_keys')
            )

    if should_push_images:
        images_ok = _push_images_for_observation(
            client,
            local_obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            progress_cb=progress_cb,
            progress_state={'done': 0, 'total': 0},
            observation_index=1,
            observation_total=1,
        )
        if not images_ok:
            mark_observation_dirty(int(local_id))
            raise CloudSyncError(f'Could not fully upload images for observation {local_id}')

    _store_remote_snapshot(client, cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def resolve_conflict_keep_cloud(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
    allow_delete: bool = False,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    remote_obs = client.get_observation(resolved_cloud_id)
    if not remote_obs:
        raise CloudSyncError(f'Cloud observation {resolved_cloud_id} not found')
    remote_images_raw = _pull_remote_images_for_sync(client, resolved_cloud_id)
    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=int(local_id),
        cloud_observation_id=resolved_cloud_id,
    )
    remote_images = [
        dict(row or {})
        for row in remote_images_raw
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
    ]

    _apply_remote_observation_fields(int(local_id), remote_obs)
    warnings = _apply_remote_images_to_local(client, int(local_id), remote_images, allow_delete=allow_delete)
    _stamp_observation_synced(int(local_id), resolved_cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    _store_remote_snapshot(client, resolved_cloud_id, remote_obs, remote_images)
    return {'local_id': int(local_id), 'cloud_id': resolved_cloud_id, 'warnings': warnings}


def resolve_conflict_merge(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    # For merge, keep local observation but add any new images from cloud
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    # First, pull any new images from cloud and add to local
    remote_obs = client.get_observation(resolved_cloud_id)
    if remote_obs:
        remote_images_raw = _pull_remote_images_for_sync(client, resolved_cloud_id)
        _record_remote_image_tombstones(
            remote_images_raw,
            local_observation_id=int(local_id),
            cloud_observation_id=resolved_cloud_id,
        )
        remote_images = [
            dict(row or {})
            for row in remote_images_raw
            if should_pull_cloud_image_to_desktop(row)
            and not str(row.get('deleted_at') or '').strip()
        ]
        warnings = _apply_remote_images_to_local(client, int(local_id), remote_images, allow_delete=False)
    else:
        warnings = []

    # Then push the local observation (which now includes merged images)
    try:
        cloud_id = client.push_observation(
            _merge_cloud_selected_ai_fields(local_obs, remote_obs),
            remote_obs=remote_obs,
        )
    except Exception as exc:
        if not is_privacy_slot_limit_error(exc):
            raise
        blocked_reason = _set_observation_privacy_blocked(int(local_id), str(exc))
        return {
            'local_id': int(local_id),
            'cloud_id': None,
            'blocked': True,
            'blocked_reason': blocked_reason,
            'raw_error': str(exc),
            'warnings': warnings,
        }
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=cloud_id,
            sync_status='synced',
            synced_at=datetime.now(timezone.utc).isoformat(),
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()

    # Push images if needed
    should_push_images = prepare_images_cb is not None
    if should_push_images:
        images_ok = _push_images_for_observation(
            client,
            local_obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            progress_cb=progress_cb,
            progress_state={'done': 0, 'total': 0},
            observation_index=1,
            observation_total=1,
        )
        if not images_ok:
            mark_observation_dirty(int(local_id))
            raise CloudSyncError(f'Could not fully upload images for observation {local_id}')

    _store_remote_snapshot(client, cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    return {'local_id': int(local_id), 'cloud_id': cloud_id, 'warnings': warnings}


def _get_keyring_module():
    try:
        import keyring  # type: ignore

        return keyring
    except Exception:
        return None


def load_saved_cloud_password() -> tuple[str, str | None, bool]:
    settings = get_app_settings()
    email = str(settings.get('cloud_user_email') or '').strip()
    keyring = _get_keyring_module()
    if keyring is None:
        return email, None, False
    try:
        password = keyring.get_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
        if password is None and not using_isolated_profile():
            password = keyring.get_password(_CLOUD_LEGACY_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
    except Exception:
        return email, None, False
    return email, password, True


def save_cloud_password(email: str, password: str) -> None:
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure password storage is unavailable on this system.")
    try:
        keyring.set_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT, password)
    except Exception as exc:
        raise RuntimeError(f"Could not securely save password: {exc}") from exc
    update_app_settings({'cloud_user_email': str(email or '').strip()})


def clear_saved_cloud_password() -> None:
    keyring = _get_keyring_module()
    if keyring is None:
        return
    for service_name in (_CLOUD_KEYRING_SERVICE, _CLOUD_LEGACY_KEYRING_SERVICE):
        try:
            keyring.delete_password(service_name, _CLOUD_KEYRING_ACCOUNT)
        except Exception:
            continue


def has_saved_cloud_password() -> bool:
    email, password, _ = load_saved_cloud_password()
    return bool(email and password)


class SporelyCloudClient:
    """Thin wrapper around Supabase REST API."""

    def __init__(self, access_token: str, user_id: str, refresh_token: str | None = None):
        self.access_token = access_token
        self.user_id = user_id
        self.refresh_token = str(refresh_token or '').strip() or None
        self._s = requests.Session()
        self._r2: CloudflareR2Client | None = None
        self._media_worker: CloudflareMediaWorkerClient | None = None
        self._column_support_cache: dict[tuple[str, str], bool] = {}
        self._cloud_image_storage_key_cache: dict[str, str] = {}
        self._s.headers.update({
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        })

    def _get_r2(self) -> CloudflareR2Client:
        if self._r2 is None:
            self._r2 = CloudflareR2Client.from_env()
        return self._r2

    def _get_media_worker(self) -> CloudflareMediaWorkerClient:
        if self._media_worker is None:
            self._media_worker = CloudflareMediaWorkerClient.from_access_token(self.access_token)
        return self._media_worker

    def _using_default_r2_loader(self) -> bool:
        if "_get_r2" in self.__dict__:
            return False
        return type(self)._get_r2 is SporelyCloudClient._get_r2

    def _response_indicates_auth_error(self, response: requests.Response) -> bool:
        return _response_indicates_auth_error(response)

    def _refresh_session_if_possible(self) -> bool:
        refresh_token = str(self.refresh_token or '').strip()
        if not refresh_token:
            return False
        try:
            refreshed = type(self).refresh_login(refresh_token)
        except CloudTemporarilyUnavailableError:
            raise
        except CloudSyncError:
            return False
        self.access_token = refreshed.access_token
        self.user_id = refreshed.user_id
        self.refresh_token = refreshed.refresh_token
        self._s.headers.update({
            'Authorization': f'Bearer {self.access_token}',
        })
        self._media_worker = None
        try:
            self.save_credentials()
        except Exception:
            pass
        return True

    def _request_with_refresh(self, method: str, url: str, *, refresh_on_auth_error: bool = True, **kwargs):
        return _request_with_transient_retry(
            self._s.request,
            method,
            url,
            refresh_on_auth_error=refresh_on_auth_error,
            refresh_callback=self._refresh_session_if_possible if refresh_on_auth_error else None,
            **kwargs,
        )

    def _has_column(self, table_name: str, column_name: str) -> bool:
        cache_key = (str(table_name or '').strip(), str(column_name or '').strip())
        if not all(cache_key):
            return False
        if cache_key in self._column_support_cache:
            return self._column_support_cache[cache_key]
        try:
            self._get(f'{cache_key[0]}?select={cache_key[1]}&limit=1')
            supported = True
        except CloudSyncError as exc:
            text = str(exc or '').lower()
            if (
                ('column' in text and cache_key[1].lower() in text and 'does not exist' in text)
                or 'could not find the' in text
            ):
                supported = False
            else:
                raise
        self._column_support_cache[cache_key] = supported
        return supported

    def _observation_supports_media_keys(self) -> bool:
        return self._has_column('observations', 'image_key') or self._has_column('observations', 'thumb_key')

    def _observation_images_support_ai_crop(self) -> bool:
        return self._has_column('observation_images', 'ai_crop_x1') or self._has_column('observation_images', 'ai_crop_source_w')

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return self._has_column('observation_images', 'ai_crop_is_custom')

    def _observation_images_support_upload_metadata(self) -> bool:
        return self._has_column('observation_images', 'upload_mode') or self._has_column('observation_images', 'stored_bytes')

    def _observation_images_support_original_storage_path(self) -> bool:
        return self._has_column('observation_images', 'original_storage_path')

    def _measurement_supports_media_keys(self) -> bool:
        return self._has_column('spore_measurements', 'image_key') or self._has_column('spore_measurements', 'thumb_key')

    def _set_observation_media_keys(self, obs_cloud_id: str, storage_key: str, sort_order) -> None:
        if not obs_cloud_id:
            return
        if sort_order not in (None, 0, '0'):
            return
        if not self._observation_supports_media_keys():
            return
        normalized_key = _normalize_cloud_media_key(storage_key)
        if not normalized_key:
            return
        payload: dict[str, str] = {}
        if self._has_column('observations', 'image_key'):
            payload['image_key'] = normalized_key
        if self._has_column('observations', 'thumb_key'):
            payload['thumb_key'] = media_variant_key(normalized_key, 'thumb')
        if payload:
            self._patch(f'observations?id=eq.{obs_cloud_id}', payload)

    def _cloud_image_storage_key(self, cloud_image_id: str) -> str:
        normalized_id = str(cloud_image_id or '').strip()
        if not normalized_id:
            return ''
        if normalized_id in self._cloud_image_storage_key_cache:
            return self._cloud_image_storage_key_cache[normalized_id]
        rows = self._get(f'observation_images?id=eq.{normalized_id}&select=storage_path&limit=1')
        storage_key = _normalize_cloud_media_key((rows[0] or {}).get('storage_path') if rows else '')
        self._cloud_image_storage_key_cache[normalized_id] = storage_key
        return storage_key

    # ── Auth ────────────────────────────────────────────────────────────

    @classmethod
    def login(cls, email: str, password: str) -> 'SporelyCloudClient':
        resp = _request_with_transient_retry(
            requests.request,
            'POST',
            f'{SUPABASE_URL}/auth/v1/token?grant_type=password',
            json={'email': email, 'password': password},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=_SUPABASE_AUTH_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'Login failed (status={resp.status_code}): {resp.text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token'),
        )

    @classmethod
    def refresh_login(cls, refresh_token: str) -> 'SporelyCloudClient':
        token = str(refresh_token or '').strip()
        if not token:
            raise CloudSyncError('Missing refresh token')
        resp = _request_with_transient_retry(
            requests.request,
            'POST',
            f'{SUPABASE_URL}/auth/v1/token?grant_type=refresh_token',
            json={'refresh_token': token},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=_SUPABASE_AUTH_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'Refresh failed (status={resp.status_code}): {resp.text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token') or token,
        )

    @classmethod
    def from_stored_credentials(cls) -> 'SporelyCloudClient | None':
        settings = get_app_settings()
        token = settings.get('cloud_access_token')
        user_id = settings.get('cloud_user_id')
        refresh_token = settings.get('cloud_refresh_token')
        token_text = str(token or '').strip()
        user_id_text = _normalize_cloud_user_id(user_id)
        if token_text and user_id_text:
            token_user_id = _decode_jwt_subject(token_text)
            return cls(
                access_token=token_text,
                user_id=token_user_id or user_id_text,
                refresh_token=refresh_token,
            )
        if refresh_token:
            try:
                client = cls.refresh_login(str(refresh_token))
                client.save_credentials()
                return client
            except CloudTemporarilyUnavailableError:
                raise
            except CloudSyncError:
                pass
        email, password, _ = load_saved_cloud_password()
        if email and password:
            try:
                client = cls.login(email, password)
                client.save_credentials(email=email)
                return client
            except CloudTemporarilyUnavailableError:
                raise
            except CloudSyncError:
                return None
        return None

    def fetch_current_user_id(self) -> str:
        """Return the authenticated Supabase user id for the current session."""
        user_info = self.fetch_current_user_info()
        user_id = _normalize_cloud_user_id(user_info.get('id') if isinstance(user_info, dict) else None)
        if user_id:
            if user_id != self.user_id:
                self.user_id = user_id
            return user_id
        raise CloudSyncError('Could not fetch current cloud user.')

    def fetch_current_user_info(self) -> dict:
        """Return the authenticated Supabase user record."""
        resp = self._request_with_refresh('GET', f'{SUPABASE_URL}/auth/v1/user', timeout=_SUPABASE_AUTH_TIMEOUT)
        if resp.ok:
            try:
                data = resp.json()
            except Exception:
                data = {}
            user_id = _normalize_cloud_user_id(data.get('id') if isinstance(data, dict) else None)
            if user_id:
                if user_id != self.user_id:
                    self.user_id = user_id
                return data if isinstance(data, dict) else {'id': user_id}
        if not self._response_indicates_auth_error(resp):
            token_user_id = _decode_jwt_subject(self.access_token)
            if token_user_id:
                if token_user_id != self.user_id:
                    self.user_id = token_user_id
                return {'id': token_user_id}
        raise CloudSyncError(f'Could not fetch current cloud user: {resp.text if resp is not None else ""}')

    def fetch_profile(self) -> dict:
        rows = self._get(
            f'profiles?id=eq.{self.user_id}&select=id,username,display_name,bio,avatar_url&limit=1'
        )
        return dict(rows[0] or {}) if rows else {}

    def fetch_cloud_plan_profile(self) -> dict:
        rows = self._get(
            f'profiles?id=eq.{self.user_id}&select='
            'id,cloud_plan,is_pro,full_res_storage_enabled,storage_quota_bytes,'
            'total_storage_bytes,storage_used_bytes,image_count,is_banned&limit=1'
        )
        return normalize_cloud_plan_profile(rows[0] if rows else {})

    def update_profile(
        self,
        *,
        username: str | None = None,
        display_name: str | None = None,
        bio: str | None = None,
        avatar_url: str | None = None,
    ) -> None:
        payload: dict[str, object] = {}
        if username is not None:
            normalized = str(username or '').strip().lstrip('@')
            payload['username'] = normalized or None
        if display_name is not None:
            normalized = str(display_name or '').strip()
            payload['display_name'] = normalized or None
        if bio is not None:
            normalized = str(bio or '').strip()
            payload['bio'] = normalized or None
        if avatar_url is not None:
            normalized = str(avatar_url or '').strip()
            payload['avatar_url'] = normalized or None
        if not payload:
            return
        self._patch(f'profiles?id=eq.{self.user_id}', payload)

    def upload_profile_avatar(self, jpeg_bytes: bytes) -> str:
        content = bytes(jpeg_bytes or b'')
        if not content:
            raise CloudSyncError('Missing avatar image data.')
        path = f'{self.user_id}/avatar.jpg'
        url = f'{SUPABASE_URL}/storage/v1/object/avatars/{path}'
        headers = {
            'Content-Type': 'image/jpeg',
            'x-upsert': 'true',
        }
        resp = self._request_with_refresh('POST', url, data=content, headers=headers, timeout=_SUPABASE_PROFILE_UPLOAD_TIMEOUT)
        if not resp.ok:
            resp = self._request_with_refresh('PUT', url, data=content, headers=headers, timeout=_SUPABASE_PROFILE_UPLOAD_TIMEOUT)
        if not resp.ok:
            raise CloudSyncError(f'Avatar upload failed: {resp.text}')
        public_url = f'{SUPABASE_URL}/storage/v1/object/public/avatars/{path}'
        self.update_profile(avatar_url=public_url)
        return public_url

    def save_credentials(
        self,
        email: str | None = None,
        password: str | None = None,
        remember_password: bool | None = None,
    ) -> None:
        updates = {
            'cloud_access_token': self.access_token,
            'cloud_user_id': self.user_id,
            'cloud_refresh_token': self.refresh_token,
        }
        if email is not None:
            updates['cloud_user_email'] = str(email or '').strip()
        update_app_settings(updates)
        # Only change the saved password when the caller explicitly asked us to.
        # Auto-login/refresh paths should preserve whatever the user chose earlier.
        if remember_password is True and email and password:
            save_cloud_password(str(email or '').strip(), password)
        elif remember_password is False:
            clear_saved_cloud_password()

    @staticmethod
    def clear_session() -> None:
        """Forget only the cloud tokens so the saved password can survive re-login."""
        update_app_settings({
            'cloud_access_token': None,
            'cloud_user_id': None,
            'cloud_refresh_token': None,
        })

    @staticmethod
    def clear_credentials() -> None:
        clear_saved_cloud_password()
        update_app_settings({
            'cloud_access_token': None,
            'cloud_user_id': None,
            'cloud_refresh_token': None,
            'cloud_user_email': None,
        })

    # ── REST helpers ─────────────────────────────────────────────────────

    def _get(self, path: str) -> list:
        resp = self._request_with_refresh('GET', f'{SUPABASE_URL}/rest/v1/{path}', timeout=_SUPABASE_REST_TIMEOUT)
        if not resp.ok:
            raise CloudSyncError(f'GET {path}: {resp.text}')
        return resp.json()

    def _post(self, path: str, payload: dict) -> list:
        resp = self._request_with_refresh(
            'POST',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=representation'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'POST {path}: {resp.text}')
        return resp.json()

    def _rpc(self, function_name: str, payload: dict | None = None):
        rpc_name = str(function_name or '').strip()
        if not rpc_name:
            raise CloudSyncError('Missing RPC function name')
        resp = self._request_with_refresh(
            'POST',
            f'{SUPABASE_URL}/rest/v1/rpc/{rpc_name}',
            json=dict(payload or {}),
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'RPC {rpc_name}: {resp.text}')
        if not resp.content:
            return None
        return resp.json()

    def _patch(self, path: str, payload: dict) -> None:
        resp = self._request_with_refresh(
            'PATCH',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=minimal'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')

    def _delete(self, path: str) -> None:
        resp = self._request_with_refresh(
            'DELETE',
            f'{SUPABASE_URL}/rest/v1/{path}',
            headers={'Prefer': 'return=minimal'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'DELETE {path}: {resp.text}')

    def _storage_remove(self, storage_paths: list[str]) -> None:
        cleaned = []
        for path in (storage_paths or []):
            path_str = _normalize_cloud_media_key(path)
            if not path_str:
                continue
            cleaned.append(path_str)

            for variant in ('thumb', 'small', 'medium'):
                cleaned.append(media_variant_key(path_str, variant))

        if not cleaned:
            return
        try:
            if direct_r2_runtime_available():
                self._get_r2().delete_objects(cleaned)
            else:
                self._get_media_worker().delete_objects(cleaned)
                _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
        except Exception as exc:
            raise CloudSyncError(f'Media delete failed: {exc}') from exc

    # ── Observation push ─────────────────────────────────────────────────

    def _find_cloud_observation(self, desktop_id: int) -> str | None:
        rows = self._get(
            f'observations?desktop_id=eq.{desktop_id}&user_id=eq.{self.user_id}&select=id'
        )
        return rows[0]['id'] if rows else None

    def get_observation(self, cloud_id: str) -> dict | None:
        cloud_value = str(cloud_id or '').strip()
        if not cloud_value:
            return None
        rows = self._get(
            f'observations?id=eq.{cloud_value}&user_id=eq.{self.user_id}&select={_OBSERVATION_SELECT_COLUMNS}'
        )
        return rows[0] if rows else None

    def list_remote_observations(self) -> list[dict]:
        return self._get(
            f'observations?user_id=eq.{self.user_id}&order=created_at.asc&select={_OBSERVATION_SELECT_COLUMNS}'
        )

    def count_remote_privacy_slots(self) -> int:
        resp = self._request_with_refresh(
            'GET',
            (
                f'{SUPABASE_URL}/rest/v1/observations?user_id=eq.{self.user_id}'
                '&or=(visibility.is.null,visibility.neq.public,location_precision.eq.fuzzed)'
                '&select=id&limit=1'
            ),
            headers={'Prefer': 'count=exact'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'GET observations count failed: {resp.text}')
        total = _parse_postgrest_content_range_total(getattr(resp, 'headers', {}).get('Content-Range'))
        if total is None:
            raise CloudSyncError('Could not determine privacy slot count from cloud response.')
        return total

    def find_remote_calibration(self, calibration_uuid: str) -> dict | None:
        calibration_id = _normalize_calibration_uuid(calibration_uuid)
        if not calibration_id:
            return None
        rows = self._get(
            f'calibrations?user_id=eq.{self.user_id}&calibration_uuid=eq.{calibration_id}&select={_CALIBRATION_SELECT_COLUMNS}'
        )
        return rows[0] if rows else None

    def list_remote_calibrations(self) -> list[dict]:
        return self._get(
            f'calibrations?user_id=eq.{self.user_id}&order=created_at.asc&select={_CALIBRATION_SELECT_COLUMNS}'
        )

    def push_calibration_reference_image(
        self,
        calibration: dict,
        *,
        cloud_row_id: str | None = None,
        remote_row: dict | None = None,
    ) -> str | None:
        """Upload a derivative calibration reference image and patch the cloud row.

        Returns a warning string when the image is missing or could not be uploaded.
        """
        record = dict(calibration or {})
        calibration_uuid = _normalize_calibration_uuid(record.get('calibration_uuid'))
        label = _calibration_display_name(record)
        ref_image_start = _cloud_sync_perf_counter()

        def _log_slow_reference_image(*, had_storage_path: bool, upload_attempted: bool, outcome: str) -> None:
            elapsed = _cloud_sync_perf_counter() - ref_image_start
            if elapsed < _CLOUD_SYNC_SLOW_STEP_SECONDS:
                return
            print(
                f"[cloud_sync] calibration reference image: slow "
                f"calibration {calibration_uuid or '?'} ({label}) "
                f"had_storage_path={had_storage_path} "
                f"upload_attempted={upload_attempted} outcome={outcome} "
                f"took {elapsed * 1000:.0f}ms",
                flush=True,
            )

        remote = dict(remote_row or {})
        if not remote and calibration_uuid:
            remote = dict(self.find_remote_calibration(calibration_uuid) or {})

        existing_storage_path = _normalize_cloud_media_key(remote.get('image_storage_path'))
        if existing_storage_path:
            # Fast no-op once the cloud row already references an image.
            return None

        target_cloud_row_id = str(cloud_row_id or remote.get('id') or '').strip()
        if not target_cloud_row_id:
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because the cloud row id is unavailable'
            )

        local_path = _select_representative_calibration_image_path(record)
        if local_path is None:
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because no readable local calibration image was found'
            )

        try:
            image_bytes, content_type, extension = _calibration_reference_image_bytes(local_path)
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=False, outcome='prepare_failed')
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because the image could not be prepared ({exc})'
            )

        storage_key = _calibration_reference_storage_key(
            self.user_id,
            calibration_uuid or str(remote.get('calibration_uuid') or '').strip() or target_cloud_row_id,
            extension,
        )
        cache_control = 'public, max-age=31536000, immutable'
        try:
            if direct_r2_runtime_available():
                self._get_r2().put_bytes(
                    image_bytes,
                    storage_key,
                    content_type=content_type,
                    cache_control=cache_control,
                    timeout=120,
                )
            else:
                cloud_plan = 'free'
                try:
                    cloud_plan = str(self.fetch_cloud_plan_profile().get('cloud_plan') or cloud_plan).strip() or cloud_plan
                except Exception:
                    pass
                self._get_media_worker().put_bytes(
                    image_bytes,
                    storage_key,
                    content_type=content_type,
                    cache_control=cache_control,
                    upload_meta={
                        'upload_mode': 'full',
                        'quality_profile': 'standard',
                        'encoding_quality': '',
                        'encoding_format': content_type,
                        'source_width': '',
                        'source_height': '',
                        'stored_width': '',
                        'stored_height': '',
                        'stored_bytes': str(len(image_bytes)),
                    },
                    options={
                        'uploadMode': 'full',
                        'uploadVariant': 'full',
                        'cloudPlan': cloud_plan,
                        'qualityProfile': 'standard',
                        'encodingFormat': content_type,
                    },
                    timeout=120,
                )
                _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='r2_upload_failed')
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because R2 upload failed ({exc})'
            )

        try:
            self._patch(
                f'calibrations?user_id=eq.{self.user_id}&id=eq.{target_cloud_row_id}',
                {'image_storage_path': _normalize_cloud_media_key(storage_key)},
            )
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='patch_failed')
            return (
                f'calibration {calibration_uuid or "?"}: uploaded reference image for {label} '
                f'but could not update the cloud row ({exc})'
            )

        _increment_sync_summary(_cloud_sync_current_summary(), 'calibration_reference_images_uploaded')
        _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='uploaded')
        return None

    def push_calibration_metadata(self, calibration: dict) -> str:
        """Upsert calibration metadata row. Returns cloud row id."""
        payload = _calibration_sync_payload(calibration)
        calibration_uuid = str(payload.get('calibration_uuid') or '').strip()
        if not calibration_uuid:
            raise CloudSyncError('Missing calibration UUID')
        if not payload.get('objective_key'):
            raise CloudSyncError('Missing objective key')
        if payload.get('calibration_date') is None:
            raise CloudSyncError('Missing calibration date')
        if payload.get('microns_per_pixel') is None:
            raise CloudSyncError('Missing microns per pixel')

        payload['user_id'] = self.user_id
        payload['calibration_uuid'] = calibration_uuid

        existing_row = self.find_remote_calibration(calibration_uuid)
        if existing_row:
            if _calibration_payloads_match(payload, existing_row):
                return str(existing_row.get('id') or '').strip() or calibration_uuid
            raise CloudSyncError(
                'Skipped cloud update because the same UUID has different metadata'
            )

        try:
            rows = self._post('calibrations', payload)
        except CloudSyncError:
            existing_row = self.find_remote_calibration(calibration_uuid)
            if existing_row and _calibration_payloads_match(payload, existing_row):
                return str(existing_row.get('id') or '').strip() or calibration_uuid
            raise

        return str(rows[0]['id'])

    def push_observation(
        self,
        obs: dict,
        remote_obs: dict | None = None,
        *,
        sync_summary: dict[str, int] | None = None,
    ) -> str:
        """Upsert observation to cloud. Returns cloud UUID."""
        summary = sync_summary or _cloud_sync_current_summary()
        payload = _observation_push_payload(obs, local=True)
        payload['user_id'] = self.user_id
        payload['desktop_id'] = obs['id']

        existing_id = self._find_cloud_observation(obs['id'])
        if existing_id:
            if remote_obs is not None:
                diff_fields = _observation_push_diff_fields(dict(obs or {}), remote_obs)
                if not diff_fields:
                    _increment_sync_summary(summary, 'observations_skipped_noop')
                    return existing_id
            self._patch(f'observations?id=eq.{existing_id}', payload)
            _increment_sync_summary(summary, 'observations_patched')
            return existing_id
        rows = self._post('observations', payload)
        _increment_sync_summary(summary, 'observations_patched')
        return rows[0]['id']

    # ── Image push ───────────────────────────────────────────────────────

    def _find_cloud_image(self, desktop_id: int) -> str | None:
        rows = self._get(
            f'observation_images?desktop_id=eq.{desktop_id}&user_id=eq.{self.user_id}&select=id'
        )
        return rows[0]['id'] if rows else None

    def _build_storage_path(self, obs_cloud_id: str, img_cloud_id: str, local_path: str) -> str:
        import urllib.parse
        path = Path(local_path)
        safe_name = urllib.parse.quote(path.name)
        return f'{self.user_id}/{obs_cloud_id}/{img_cloud_id}_{safe_name}'

    def _build_original_storage_path(self, obs_cloud_id: str, img_cloud_id: str, local_path: str) -> str:
        source_path = Path(str(local_path or '').strip())
        safe_name = _sanitize_original_storage_filename(source_path)
        normalized_image_id = str(img_cloud_id or '').strip()
        if not normalized_image_id:
            digest_source = f'{self.user_id}/{str(obs_cloud_id or "").strip()}/{safe_name}'
            normalized_image_id = hashlib.sha1(digest_source.encode('utf-8')).hexdigest()[:16]
        return _normalize_cloud_media_key(
            f'{self.user_id}/{str(obs_cloud_id or "").strip()}/originals/{normalized_image_id}/{safe_name}'
        )

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        """Upsert image metadata row. Returns cloud UUID."""
        payload = {col: img.get(col) for col in _IMG_PUSH_COLS}
        calibration_uuid = _image_calibration_uuid(img)
        if calibration_uuid:
            payload['calibration_uuid'] = calibration_uuid
        else:
            payload.pop('calibration_uuid', None)
        payload['observation_id']    = obs_cloud_id
        payload['user_id']           = self.user_id
        payload['desktop_id']        = img['id']
        payload['original_filename'] = (
            str(img.get('original_filename') or '').strip()
            or Path(img.get('filepath') or '').name
            or None
        )
        payload['storage_path']      = _normalize_cloud_media_key(storage_path)
        if payload.get('gps_source') is not None:
            payload['gps_source'] = bool(payload['gps_source'])
        if not self._observation_images_support_ai_crop():
            for key in (
                'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
                'ai_crop_source_w', 'ai_crop_source_h',
            ):
                payload.pop(key, None)
        if not self._observation_images_support_ai_crop_custom():
            payload.pop('ai_crop_is_custom', None)
        if self._observation_images_support_upload_metadata():
            for key in _IMG_UPLOAD_META_COLS:
                payload[key] = img.get(key)

        existing_id = self._find_cloud_image(img['id'])
        if existing_id:
            self._patch(f'observation_images?id=eq.{existing_id}', payload)
            cloud_id = existing_id
        else:
            rows = self._post('observation_images', payload)
            cloud_id = rows[0]['id']
        normalized_key = _normalize_cloud_media_key(payload.get('storage_path'))
        if cloud_id and normalized_key:
            self._cloud_image_storage_key_cache[str(cloud_id)] = normalized_key
            self._set_observation_media_keys(obs_cloud_id, normalized_key, img.get('sort_order'))
        return cloud_id

    def set_image_original_storage_path(self, cloud_image_id: str, original_storage_path: str) -> None:
        normalized_id = str(cloud_image_id or '').strip()
        normalized_key = _normalize_cloud_media_key(original_storage_path)
        if not normalized_id or not normalized_key:
            return
        if not self._observation_images_support_original_storage_path():
            return
        self._patch(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}',
            {'original_storage_path': normalized_key},
        )

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        """Upload file to Cloudflare R2. Returns the relative media key or None if missing."""
        path = Path(local_path)
        if not path.exists():
            return None

        storage_path = _normalize_cloud_media_key(
            storage_path or self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)
        )
        cache_control = 'public, max-age=31536000, immutable'
        meta = dict(upload_meta or {})
        print(
            '[cloud_sync] Uploading cloud image request '
            f'obs={_safe_int(meta.get("observation_id")) or obs_cloud_id or "?"} '
            f'image={_safe_int(meta.get("image_id")) or img_cloud_id or "?"} '
            f'storage_path={storage_path}'
        )

        with tempfile.TemporaryDirectory(prefix='sporely_cloud_upload_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            worker_base_url = media_worker_base_url()
            try:
                prepared_path, source_width, source_height, stored_width, stored_height, encoding_format, encoding_quality = _prepare_cloud_image_upload_file(
                    str(path),
                    temp_dir,
                    _safe_int(img_cloud_id, default=0) or 0,
                    meta,
                )
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='full',
                            storage_key=storage_path,
                            content_type='image/webp',
                            prepared_path_suffix='.webp',
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                raise
            mime = _content_type_for_path(prepared_path)
            prepared_path_suffix = prepared_path.suffix.lower() or '.webp'
            upload_policy = _cloud_upload_policy_from_meta(meta)
            quality_profile = str(upload_policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
            upload_mode = str(upload_policy.get('uploadMode') or 'full').strip().lower() or 'full'
            cloud_plan = str(upload_policy.get('cloudPlan') or ('pro' if quality_profile == 'high' else 'free'))
            stored_bytes = prepared_path.stat().st_size
            common_metadata = {
                'user_id': self.user_id,
                'uploaded_at': datetime.now(timezone.utc).isoformat(),
                'uploaded_by': self.user_id,
                'upload_mode': upload_mode,
                'upload_variant': 'full',
                'cloud_plan': cloud_plan,
                'quality_profile': quality_profile,
                'encoding_quality': '' if encoding_quality is None else str(encoding_quality),
                'encoding_format': encoding_format,
                'source_width': str(source_width),
                'source_height': str(source_height),
                'stored_width': str(stored_width),
                'stored_height': str(stored_height),
                'stored_bytes': str(stored_bytes),
            }

            try:
                if direct_r2_runtime_available():
                    r2 = self._get_r2()
                    r2.put_file(
                        prepared_path,
                        storage_path,
                        content_type=mime,
                        cache_control=cache_control,
                        timeout=120,
                        custom_metadata=common_metadata,
                    )
                else:
                    worker = self._get_media_worker()
                    worker_base_url = str(getattr(worker, 'base_url', worker_base_url) or worker_base_url).strip().rstrip('/')
                    upload_response = worker.put_file(
                        prepared_path,
                        storage_path,
                        content_type=mime,
                        cache_control=cache_control,
                        timeout=120,
                        upload_meta=common_metadata,
                        options={
                            'uploadMode': upload_mode,
                            'uploadVariant': 'full',
                            'cloudPlan': cloud_plan,
                            'qualityProfile': quality_profile,
                            'encodingQuality': encoding_quality,
                            'encodingFormat': encoding_format,
                            'sourceWidth': source_width,
                            'sourceHeight': source_height,
                            'storedWidth': stored_width,
                            'storedHeight': stored_height,
                        },
                    )
                    _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                    confirmed_key = _normalize_cloud_media_key(str((upload_response or {}).get('key') or storage_path))
                    if not confirmed_key:
                        raise CloudSyncError('Worker upload did not return a storage key')
                    storage_path = confirmed_key
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='full',
                            storage_key=storage_path,
                            content_type=mime,
                            prepared_path_suffix=prepared_path_suffix,
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise CloudSyncError(f'Media upload failed: {exc}') from exc

            # Generate the single cloud thumbnail variant used by web and desktop.
            try:
                with Image.open(prepared_path) as img:
                    img = ImageOps.exif_transpose(img)
                    if img.mode in ('RGBA', 'LA'):
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'RGBA':
                            background.paste(img, mask=img.split()[3])
                        else:
                            background.paste(img, mask=img.split()[1])
                        img = background
                    elif img.mode != 'RGB':
                        img = img.convert('RGB')

                    orig_w, orig_h = img.size
                    scale = min(1.0, _CLOUD_THUMB_MAX_EDGE / max(orig_w, orig_h))
                    target_w = max(1, int(orig_w * scale))
                    target_h = max(1, int(orig_h * scale))

                    variant_path = media_variant_key(storage_path, 'thumb')
                    thumb_worker_base_url = media_worker_base_url()
                    thumb_prepared_suffix = Path(variant_path).suffix.lower() or '.webp'
                    thumb_mime = mime
                    img_resized = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
                    buffer = io.BytesIO()
                    thumb_format, thumb_mime, thumb_options = _cloud_thumb_save_format(prepared_path)
                    img_resized.save(buffer, format=thumb_format, **thumb_options)
                    thumb_quality = thumb_options.get('quality')
                    thumb_metadata = {
                        'user_id': self.user_id,
                        'uploaded_at': common_metadata['uploaded_at'],
                        'uploaded_by': self.user_id,
                        'upload_mode': upload_mode,
                        'upload_variant': 'thumb',
                        'cloud_plan': cloud_plan,
                        'quality_profile': quality_profile,
                        'encoding_quality': '' if thumb_quality is None else str(thumb_quality),
                        'encoding_format': thumb_mime,
                        'source_width': str(source_width),
                        'source_height': str(source_height),
                        'stored_width': str(target_w),
                        'stored_height': str(target_h),
                        'stored_bytes': str(len(buffer.getvalue())),
                    }
                    thumb_worker_base_url = media_worker_base_url()
                    thumb_prepared_suffix = Path(variant_path).suffix.lower() or '.webp'
                    if direct_r2_runtime_available():
                        self._get_r2().put_bytes(
                            buffer.getvalue(),
                            variant_path,
                            content_type=thumb_mime,
                            cache_control=cache_control,
                            timeout=60,
                            custom_metadata=thumb_metadata,
                        )
                    else:
                        worker = self._get_media_worker()
                        thumb_worker_base_url = str(getattr(worker, 'base_url', media_worker_base_url()) or media_worker_base_url()).strip().rstrip('/')
                        thumb_response = worker.put_bytes(
                            buffer.getvalue(),
                            variant_path,
                            content_type=thumb_mime,
                            cache_control=cache_control,
                            timeout=60,
                            upload_meta=thumb_metadata,
                            options={
                                'uploadMode': upload_mode,
                                'uploadVariant': 'thumb',
                                'cloudPlan': cloud_plan,
                                'qualityProfile': quality_profile,
                                'encodingQuality': thumb_quality,
                                'encodingFormat': thumb_mime,
                                'sourceWidth': source_width,
                                'sourceHeight': source_height,
                                'storedWidth': target_w,
                                'storedHeight': target_h,
                            },
                        )
                        _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                        confirmed_thumb_key = _normalize_cloud_media_key(str((thumb_response or {}).get('key') or variant_path))
                        if confirmed_thumb_key != _normalize_cloud_media_key(variant_path):
                            raise CloudSyncError('Worker thumbnail upload returned an unexpected storage key')
            except Exception as e:
                if is_image_too_large_for_plan_error(e):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            e,
                            upload_variant='thumb',
                            storage_key=variant_path,
                            content_type=thumb_mime,
                            prepared_path_suffix=thumb_prepared_suffix,
                            worker_base_url=thumb_worker_base_url,
                        )
                    ) from e
                raise CloudSyncError(f'Media thumbnail upload failed: {e}') from e

        _increment_sync_summary(_cloud_sync_current_summary(), 'images_uploaded')
        return storage_path

    def upload_original_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        """Upload a full-resolution original file to cloud storage as WebP."""
        path = Path(local_path)
        if not path.exists():
            return None

        storage_path = _normalize_cloud_media_key(
            storage_path or self._build_original_storage_path(obs_cloud_id, img_cloud_id, local_path)
        )
        if not storage_path:
            return None

        cache_control = 'public, max-age=31536000, immutable'
        meta = dict(upload_meta or {})
        print(
            '[cloud_sync] Uploading cloud original image request '
            f'obs={_safe_int(meta.get("observation_id")) or obs_cloud_id or "?"} '
            f'image={_safe_int(meta.get("image_id")) or img_cloud_id or "?"} '
            f'storage_path={storage_path}'
        )
        with tempfile.TemporaryDirectory(prefix='sporely_cloud_upload_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            worker_base_url = media_worker_base_url()
            try:
                prepared_path, source_width, source_height, stored_width, stored_height, encoding_format, encoding_quality = _prepare_cloud_image_upload_file(
                    str(path),
                    temp_dir,
                    _safe_int(img_cloud_id, default=0) or 0,
                    meta,
                )
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='original',
                            storage_key=storage_path,
                            content_type='image/webp',
                            prepared_path_suffix='.webp',
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise

            content_type = _content_type_for_path(prepared_path)
            prepared_path_suffix = prepared_path.suffix.lower() or '.webp'
            upload_policy = _cloud_upload_policy_from_meta(meta)
            quality_profile = str(upload_policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
            cloud_plan = str(upload_policy.get('cloudPlan') or ('pro' if quality_profile == 'high' else 'free'))
            stored_bytes = prepared_path.stat().st_size
            common_metadata = {
                'user_id': self.user_id,
                'uploaded_at': datetime.now(timezone.utc).isoformat(),
                'uploaded_by': self.user_id,
                'upload_mode': 'full',
                'upload_variant': 'original',
                'cloud_plan': cloud_plan,
                'quality_profile': quality_profile,
                'encoding_quality': '' if encoding_quality is None else str(encoding_quality),
                'encoding_format': encoding_format,
                'source_width': str(source_width),
                'source_height': str(source_height),
                'stored_width': str(stored_width),
                'stored_height': str(stored_height),
                'stored_bytes': str(stored_bytes),
                'source_role': str(meta.get('source_role') or '').strip(),
                'source_kind': str(meta.get('source_kind') or '').strip(),
            }

            try:
                if direct_r2_runtime_available():
                    r2 = self._get_r2()
                    r2.put_file(
                        prepared_path,
                        storage_path,
                        content_type=content_type,
                        cache_control=cache_control,
                        timeout=120,
                        custom_metadata=common_metadata,
                    )
                else:
                    worker = self._get_media_worker()
                    worker_base_url = str(getattr(worker, 'base_url', worker_base_url) or worker_base_url).strip().rstrip('/')
                    upload_response = worker.put_file(
                        prepared_path,
                        storage_path,
                        content_type=content_type,
                        cache_control=cache_control,
                        timeout=120,
                        upload_meta=common_metadata,
                        options={
                            'uploadMode': 'full',
                            'uploadVariant': 'original',
                            'cloudPlan': cloud_plan,
                            'qualityProfile': quality_profile,
                            'encodingQuality': encoding_quality,
                            'encodingFormat': encoding_format,
                            'sourceWidth': source_width,
                            'sourceHeight': source_height,
                            'storedWidth': stored_width,
                            'storedHeight': stored_height,
                        },
                    )
                    _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                    confirmed_key = _normalize_cloud_media_key(str((upload_response or {}).get('key') or storage_path))
                    if not confirmed_key:
                        raise CloudSyncError('Worker upload did not return a storage key')
                    storage_path = confirmed_key
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='original',
                            storage_key=storage_path,
                            content_type=content_type,
                            prepared_path_suffix=prepared_path_suffix,
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise CloudSyncError(f'Original media upload failed: {exc}') from exc

        return storage_path

    def _download_public_media_file(self, storage_path: str, dest_path: str | Path, *, timeout: int = 120) -> Path:
        storage_key = _normalize_cloud_media_key(storage_path)
        if not storage_key:
            raise CloudSyncError('Missing storage path')
        public_url = self._get_media_worker().public_url(storage_key)
        if not public_url:
            raise CloudSyncError('Missing public media URL')
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        session = requests.Session()
        response = session.get(public_url, stream=True, timeout=timeout)
        try:
            if not response.ok:
                raise CloudSyncError(f'Public media download failed ({response.status_code})')
            content_type = str(response.headers.get('content-type') or '').strip().lower()
            if content_type and not content_type.startswith('image/') and content_type != 'application/octet-stream':
                raise CloudSyncError(f'Public media download returned non-image content ({content_type})')
            with destination.open('wb') as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
            return destination
        finally:
            try:
                response.close()
            except Exception:
                pass

    # ── Pull new web observations ─────────────────────────────────────────

    def pull_web_observations(self, after_iso: str | None = None) -> list[dict]:
        """Fetch observations created on mobile/web (desktop_id IS NULL)."""
        qs = f'observations?desktop_id=is.null&user_id=eq.{self.user_id}&order=created_at.asc&select={_OBSERVATION_SELECT_COLUMNS}'
        if after_iso:
            qs += f'&created_at=gt.{_encode_postgrest_filter_value(after_iso)}'
        return self._get(qs)

    def set_desktop_id(self, cloud_id: str, desktop_id: int) -> None:
        """Write the local SQLite ID back to the cloud row for future dedup."""
        self._patch(f'observations?id=eq.{cloud_id}', {'desktop_id': desktop_id})

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        cloud_value = str(obs_cloud_id or '').strip()
        if not cloud_value:
            return []

        path = f'observation_images?observation_id=eq.{cloud_value}&user_id=eq.{self.user_id}'
        if not include_deleted_for_sync:
            path += '&deleted_at=is.null'
        path += f'&select={_OBSERVATION_IMAGE_SELECT_COLUMNS}'

        rows = self._get(path)
        image_rows = [dict(row or {}) for row in (rows or [])]
        if include_deleted_for_sync:
            return image_rows
        return [
            row
            for row in image_rows
            if not str(row.get('deleted_at') or '').strip()
        ]

    def pull_observation_identifications(self, obs_cloud_id: str) -> list[dict]:
        cloud_value = str(obs_cloud_id or '').strip()
        if not cloud_value:
            return []
        try:
            return [
                dict(row or {})
                for row in self._get(
                    f'observation_identifications?observation_id=eq.{cloud_value}&user_id=eq.{self.user_id}&order=created_at.desc,id.desc&select={_OBSERVATION_IDENTIFICATION_SELECT_COLUMNS}'
                )
            ]
        except Exception as exc:
            message = str(exc or '').lower()
            if 'observation_identifications' in message and (
                'could not find the table' in message
                or 'does not exist' in message
            ):
                return []
            if 'schema cache' in message or 'pgrst002' in message or 'pgrst003' in message:
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            raise

    def set_measurement_desktop_id(self, cloud_measurement_id: str, desktop_id: int) -> None:
        """Write the local SQLite measurement ID back to the cloud row for future dedup."""
        self._patch(
            f'spore_measurements?id=eq.{cloud_measurement_id}&user_id=eq.{self.user_id}',
            {'desktop_id': desktop_id},
        )

    def pull_measurements_for_images(self, image_cloud_ids: list[str]) -> list[dict]:
        profiler = _cloud_sync_current_profiler()
        image_ids = [str(image_id or '').strip() for image_id in (image_cloud_ids or []) if str(image_id or '').strip()]
        all_rows: list[dict] = []
        success = False
        batch_size = _CLOUD_SYNC_IN_BATCH_SIZE
        request_count = 0
        fetch_start = _cloud_sync_perf_counter()
        try:
            if not image_ids:
                success = True
                return []
            batch_total = (len(image_ids) + batch_size - 1) // batch_size
            print(
                f'[cloud_sync] measurement fetch: start image_ids={len(image_ids)} '
                f'batch_size={batch_size} batches={batch_total}',
                flush=True,
            )
            for batch_index, i in enumerate(range(0, len(image_ids), batch_size)):
                chunk = image_ids[i:i + batch_size]
                ids_str = ','.join(chunk)
                batch_start = _cloud_sync_perf_counter()
                rows = self._get(
                    f'spore_measurements?image_id=in.({ids_str})&user_id=eq.{self.user_id}&order=measured_at.asc,id.asc&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
                )
                request_count += 1
                all_rows.extend(rows)
                batch_elapsed = _cloud_sync_perf_counter() - batch_start
                if batch_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                    print(
                        f'[cloud_sync] measurement fetch: slow batch '
                        f'{batch_index + 1}/{batch_total} ids={len(chunk)} '
                        f'rows={len(rows)} duration={batch_elapsed * 1000:.0f}ms',
                        flush=True,
                    )
            success = True
            return all_rows
        finally:
            print(
                f'[cloud_sync] measurement fetch: complete requests={request_count} '
                f'rows={len(all_rows)} '
                f'duration={(_cloud_sync_perf_counter() - fetch_start) * 1000:.0f}ms',
                flush=True,
            )
            if profiler is not None:
                try:
                    profiler.record_pull_measurements_for_images(len(all_rows) if success else 0)
                except Exception:
                    pass

    def pull_bulk_image_metadata(self, obs_cloud_ids: list[str]) -> list[dict]:
        profiler = _cloud_sync_current_profiler()
        all_images: list[dict] = []
        success = False
        try:
            if not obs_cloud_ids:
                success = True
                return []
            for i in range(0, len(obs_cloud_ids), _CLOUD_SYNC_IN_BATCH_SIZE):
                chunk = obs_cloud_ids[i:i + _CLOUD_SYNC_IN_BATCH_SIZE]
                ids_str = ','.join(chunk)
                rows = self._get(
                    f'observation_images?observation_id=in.({ids_str})&user_id=eq.{self.user_id}&select={_OBSERVATION_IMAGE_SELECT_COLUMNS}'
                )
                all_images.extend(rows)
            success = True
            return all_images
        finally:
            if profiler is not None:
                try:
                    profiler.record_pull_bulk_image_metadata(len(all_images) if success else 0)
                except Exception:
                    pass

    def search_community_spore_datasets(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_community_spore_datasets', payload)
        return rows if isinstance(rows, list) else []

    def get_community_spore_dataset(self, observation_id: int) -> dict | None:
        rows = self._rpc(
            'get_community_spore_dataset',
            {'p_observation_id': int(observation_id)},
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def community_spore_taxon_summary(self, genus: str, species: str) -> dict | None:
        rows = self._rpc(
            'community_spore_taxon_summary',
            {
                'p_genus': str(genus or '').strip(),
                'p_species': str(species or '').strip(),
            },
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def search_public_reference_values(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_public_reference_values', payload)
        return rows if isinstance(rows, list) else []

    def set_image_desktop_id(self, cloud_image_id: str, desktop_id: int) -> None:
        """Write the local SQLite image ID back to the cloud image row."""
        self._patch(
            f'observation_images?id=eq.{cloud_image_id}&user_id=eq.{self.user_id}',
            {'desktop_id': desktop_id},
        )

    def soft_delete_image(self, cloud_image_id: str, deleted_at: str | None) -> None:
        """Mark one cloud image row as deleted without removing storage objects."""
        normalized_id = str(cloud_image_id or '').strip()
        if not normalized_id:
            raise CloudSyncError('Missing cloud image id')
        deleted_at_text = str(deleted_at or '').strip() or datetime.now(timezone.utc).isoformat()
        rows = self._get(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}&select=id,deleted_at&limit=1'
        )
        if not rows:
            raise CloudSyncError(f'Cloud image {normalized_id} not found')
        self._patch(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}',
            {'deleted_at': deleted_at_text},
        )

    def push_measurement(
        self,
        meas: dict,
        cloud_image_id: str,
        *,
        remote_measurement_cache: dict[str, dict] | None = None,
        sync_summary: dict[str, int] | None = None,
    ) -> str:
        """Upsert one spore measurement row. Returns cloud UUID."""
        summary = sync_summary or _cloud_sync_current_summary()
        storage_key = ''
        include_media_keys = False
        if self._measurement_supports_media_keys():
            storage_key = self._cloud_image_storage_key(cloud_image_id)
            include_media_keys = bool(storage_key)

        payload = _measurement_sync_payload(
            meas,
            local=True,
            cloud_image_id=cloud_image_id,
            image_storage_key=storage_key,
            include_media_keys=include_media_keys,
        )
        payload['user_id'] = self.user_id

        if remote_measurement_cache is not None:
            for lookup_key in _measurement_push_lookup_keys(meas):
                cached_row = remote_measurement_cache.get(lookup_key)
                if cached_row is None:
                    continue
                existing_id = str(cached_row.get('id') or '').strip()
                if existing_id:
                    if _measurement_payloads_match(
                        meas,
                        cached_row,
                        cloud_image_id=cloud_image_id,
                        image_storage_key=storage_key,
                        include_media_keys=include_media_keys,
                    ):
                        _increment_sync_summary(summary, 'measurements_skipped_noop')
                        return existing_id
                    diff_fields = _measurement_push_diff_fields(
                        meas,
                        cached_row,
                        cloud_image_id=cloud_image_id,
                        image_storage_key=storage_key,
                        include_media_keys=include_media_keys,
                    )
                    if diff_fields:
                        print(
                            f'[cloud_sync] Measurement {int(meas.get("id") or 0)} '
                            f'push diff fields: {", ".join(diff_fields)}'
                        )
                    self._patch(f'spore_measurements?id=eq.{existing_id}', payload)
                    _increment_sync_summary(summary, 'measurements_patched')
                    return existing_id
            rows = self._post('spore_measurements', payload)
            _increment_sync_summary(summary, 'measurements_patched')
            return rows[0]['id']

        rows = self._get(
            f'spore_measurements?desktop_id=eq.{payload["desktop_id"]}&user_id=eq.{self.user_id}&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
        )
        if rows:
            remote_row = dict(rows[0] or {})
            existing_id = str(remote_row.get('id') or '').strip()
            if existing_id:
                if _measurement_payloads_match(
                    meas,
                    remote_row,
                    cloud_image_id=cloud_image_id,
                    image_storage_key=storage_key,
                    include_media_keys=include_media_keys,
                ):
                    _increment_sync_summary(summary, 'measurements_skipped_noop')
                    return existing_id
                diff_fields = _measurement_push_diff_fields(
                    meas,
                    remote_row,
                    cloud_image_id=cloud_image_id,
                    image_storage_key=storage_key,
                    include_media_keys=include_media_keys,
                )
                if diff_fields:
                    print(
                        f'[cloud_sync] Measurement {int(meas.get("id") or 0)} '
                        f'push diff fields: {", ".join(diff_fields)}'
                    )
                self._patch(f'spore_measurements?id=eq.{existing_id}', payload)
                _increment_sync_summary(summary, 'measurements_patched')
                return existing_id
        rows = self._post('spore_measurements', payload)
        _increment_sync_summary(summary, 'measurements_patched')
        return rows[0]['id']

    def delete_cloud_measurements_for_image(self, cloud_image_id: str) -> None:
        """Delete all cloud spore_measurements rows for one image."""
        self._delete(f'spore_measurements?image_id=eq.{cloud_image_id}&user_id=eq.{self.user_id}')

    def delete_cloud_observation(self, obs_cloud_id: str) -> None:
        """Delete one cloud observation and its associated cloud image rows/files."""
        cloud_id = str(obs_cloud_id or '').strip()
        if not cloud_id:
            raise CloudSyncError('Missing cloud observation id')
        image_rows = self.pull_image_metadata(cloud_id) or []
        storage_paths = [
            _normalize_cloud_media_key(row.get('storage_path'))
            for row in image_rows
            if _normalize_cloud_media_key(row.get('storage_path'))
        ]
        if storage_paths:
            try:
                self._storage_remove(storage_paths)
            except CloudSyncError as exc:
                print(f'[cloud_sync] Warning: could not remove storage files for {cloud_id}: {exc}')
        self._delete(f'observation_images?observation_id=eq.{cloud_id}')
        self._delete(f'observations?id=eq.{cloud_id}')

    def download_image_file(self, storage_path: str, dest_path: str | Path) -> Path:
        """Download one cloud image from Cloudflare R2 into a local path."""
        profiler = _cloud_sync_current_profiler()
        start = _cloud_sync_perf_counter()
        downloaded_path: Path | None = None
        try:
            storage_key = _normalize_cloud_media_key(storage_path)
            if not storage_key:
                raise CloudSyncError('Missing storage path')
            if direct_r2_runtime_available():
                downloaded_path = Path(self._get_r2().download_to_file(storage_key, dest_path, timeout=120))
                return downloaded_path
            try:
                downloaded_path = Path(self._download_public_media_file(storage_key, dest_path, timeout=120))
                return downloaded_path
            except Exception as public_exc:
                try:
                    downloaded_path = Path(self._get_media_worker().download_to_file(storage_key, dest_path, timeout=120))
                    return downloaded_path
                except Exception as worker_exc:
                    detail = str(worker_exc or '').strip() or worker_exc.__class__.__name__
                    if 'nosuchkey' in detail.lower():
                        raise CloudSyncError(
                            f'Cloud image file is missing from storage ({storage_key})'
                        ) from worker_exc
                    public_detail = str(public_exc or '').strip()
                    if public_detail:
                        detail = f'{detail} (public fallback: {public_detail})'
                    raise CloudSyncError(f'Download failed: {detail}') from worker_exc
        except CloudSyncError:
            raise
        except Exception as exc:
            detail = str(exc or '').strip()
            if 'nosuchkey' in detail.lower():
                raise CloudSyncError(
                    f'Cloud image file is missing from storage ({storage_key})'
                ) from exc
            raise CloudSyncError(f'Download failed: {detail or exc.__class__.__name__}') from exc
        finally:
            if profiler is not None:
                try:
                    bytes_downloaded = 0
                    if downloaded_path is not None:
                        try:
                            bytes_downloaded = int(Path(downloaded_path).stat().st_size)
                        except Exception:
                            bytes_downloaded = 0
                    profiler.record_download_image_file(
                        max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0),
                        bytes_downloaded,
                    )
                except Exception:
                    pass
            try:
                if downloaded_path is not None and Path(downloaded_path).exists():
                    _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_downloads')
            except Exception:
                pass

def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024: return f"{size_bytes} B"
    if size_bytes < 1024 * 1024: return f"{size_bytes/1024:.1f} KB"
    return f"{size_bytes/(1024*1024):.1f} MB"


def _worker_base_url_from_request_url(request_url: str | None) -> str:
    text = str(request_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _format_cloud_image_too_large_error(
    local_path: str | Path,
    upload_meta: dict | None = None,
    inner_error: Exception | str | None = None,
    *,
    upload_variant: str | None = None,
    storage_key: str | None = None,
    content_type: str | None = None,
    prepared_path_suffix: str | None = None,
    worker_base_url: str | None = None,
) -> str:
    meta = dict(upload_meta or {})
    path = Path(str(local_path or "").strip())
    upload_policy = _cloud_upload_policy_from_meta(meta)

    details: list[str] = []
    worker_payload: dict[str, object] = {}
    worker_request_url = ""
    worker_request_method = ""
    worker_response_status = 0
    worker_response_text = ""
    worker_request_headers: dict[str, str] = {}
    if isinstance(inner_error, dict):
        worker_payload = dict(inner_error)
    elif inner_error is not None:
        for attr in ('payload', 'response_payload', 'response', 'body'):
            try:
                candidate = getattr(inner_error, attr)
            except Exception:
                candidate = None
            if isinstance(candidate, dict):
                worker_payload = dict(candidate)
                break
        try:
            worker_request_url = str(getattr(inner_error, 'request_url', '') or '').strip()
        except Exception:
            worker_request_url = ""
        try:
            worker_request_method = str(getattr(inner_error, 'request_method', '') or '').strip().upper()
        except Exception:
            worker_request_method = ""
        try:
            worker_response_status = _safe_int(
                getattr(inner_error, 'response_status', None)
                or getattr(inner_error, 'response_status_code', None)
                or getattr(inner_error, 'status_code', None)
            )
        except Exception:
            worker_response_status = 0
        try:
            worker_response_text = str(getattr(inner_error, 'response_text', '') or getattr(inner_error, 'text', '') or '').strip()
        except Exception:
            worker_response_text = ""
        try:
            headers = getattr(inner_error, 'request_headers', None)
        except Exception:
            headers = None
        if isinstance(headers, dict):
            worker_request_headers = {
                str(name): str(value).strip()
                for name, value in headers.items()
                if str(name or "").strip() and str(value or "").strip()
            }

    worker_details = worker_payload.get('details')
    worker_detail_map = worker_details if isinstance(worker_details, dict) else {}
    worker_code = str(
        worker_payload.get('error')
        or worker_payload.get('code')
        or worker_detail_map.get('error')
        or worker_detail_map.get('code')
        or ''
    ).strip()
    worker_message = str(
        worker_payload.get('message')
        or worker_detail_map.get('message')
        or worker_detail_map.get('detail')
        or ''
    ).strip()
    worker_reason = str(worker_payload.get('reason') or worker_detail_map.get('reason') or '').strip()
    if worker_payload and not worker_detail_map:
        details.append("Worker details: missing")

    observation_label = str(meta.get('observation_label') or '').strip()
    observation_id = str(meta.get('observation_id') or '').strip()
    if observation_label and observation_id:
        details.append(f"Observation: {observation_label} (ID {observation_id})")
    elif observation_label:
        details.append(f"Observation: {observation_label}")
    elif observation_id:
        details.append(f"Observation ID: {observation_id}")

    image_label = str(meta.get('image_label') or '').strip()
    image_id = str(meta.get('image_id') or '').strip()
    if image_label and image_id:
        details.append(f"Image: {image_label} (ID {image_id})")
    elif image_label:
        details.append(f"Image: {image_label}")
    elif image_id:
        details.append(f"Image ID: {image_id}")

    source_path = str(meta.get('source_path') or '').strip()
    source_filename = str(meta.get('source_filename') or '').strip()
    if source_path:
        details.append(f"Original file: {source_path}")
    elif source_filename:
        details.append(f"Original file: {source_filename}")

    source_bytes = _safe_int(meta.get('source_bytes'))
    if source_bytes <= 0 and source_path:
        try:
            source_bytes = int(Path(source_path).stat().st_size)
        except Exception:
            source_bytes = 0
    if source_bytes <= 0:
        try:
            source_bytes = int(path.stat().st_size)
        except Exception:
            source_bytes = 0
    if source_bytes > 0:
        details.append(f"Original size: {_format_size(source_bytes)}")

    source_width = _safe_int(meta.get('source_width'))
    source_height = _safe_int(meta.get('source_height'))
    if source_width <= 0 or source_height <= 0:
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
                source_width = int(img.width or 0)
                source_height = int(img.height or 0)
        except Exception:
            source_width = source_width or 0
            source_height = source_height or 0
    if source_width > 0 and source_height > 0:
        details.append(f"Original dimensions: {source_width} × {source_height} px")

    prepared_bytes = _safe_int(meta.get('stored_bytes'))
    if prepared_bytes <= 0:
        prepared_bytes = _safe_int(worker_detail_map.get('bodyBytes') or worker_detail_map.get('body_bytes'))
    if prepared_bytes <= 0:
        prepared_bytes = _safe_int(worker_detail_map.get('storedBytes') or worker_detail_map.get('stored_bytes'))
    if prepared_bytes <= 0:
        try:
            prepared_bytes = int(path.stat().st_size)
        except Exception:
            prepared_bytes = 0
    if prepared_bytes > 0:
        details.append(f"Prepared upload size: {_format_size(prepared_bytes)}")

    prepared_width = _safe_int(meta.get('stored_width'))
    prepared_height = _safe_int(meta.get('stored_height'))
    if (prepared_width <= 0 or prepared_height <= 0) and worker_detail_map:
        prepared_width = prepared_width or _safe_int(
            worker_detail_map.get('storedWidth')
            or worker_detail_map.get('stored_width')
        )
        prepared_height = prepared_height or _safe_int(
            worker_detail_map.get('storedHeight')
            or worker_detail_map.get('stored_height')
        )
    if prepared_width > 0 and prepared_height > 0:
        details.append(f"Prepared dimensions: {prepared_width} × {prepared_height} px")

    prepared_suffix = str(
        prepared_path_suffix
        or meta.get('prepared_path_suffix')
        or meta.get('preparedPathSuffix')
        or ''
    ).strip().lower()
    if not prepared_suffix:
        prepared_suffix = path.suffix.lower() or ''
    if not prepared_suffix and content_type:
        lowered_content_type = str(content_type or '').strip().lower()
        if lowered_content_type == 'image/webp':
            prepared_suffix = '.webp'
        elif lowered_content_type in {'image/jpeg', 'image/jpg'}:
            prepared_suffix = '.jpg'
        elif lowered_content_type == 'image/png':
            prepared_suffix = '.png'
        elif lowered_content_type == 'image/avif':
            prepared_suffix = '.avif'
        elif lowered_content_type == 'image/tiff':
            prepared_suffix = '.tif'
    if prepared_suffix:
        details.append(f"Prepared path suffix: {prepared_suffix}")

    encoding_format = str(meta.get('encoding_format') or meta.get('encodingFormat') or '').strip().lower()
    if not encoding_format and worker_detail_map:
        encoding_format = str(
            worker_detail_map.get('encodingFormat')
            or worker_detail_map.get('encoding_format')
            or ''
        ).strip().lower()
    if encoding_format:
        details.append(f"Encoding format: {encoding_format}")

    byte_cap = _safe_int(
        meta.get('full_image_byte_cap')
        or meta.get('fullImageByteCap')
        or upload_policy.get('fullImageByteCap')
        or upload_policy.get('full_image_byte_cap')
    )
    if byte_cap > 0:
        details.append(f"Plan cap: {_format_size(byte_cap)}")

    local_upload_variant = str(
        upload_variant
        or meta.get('upload_variant')
        or meta.get('uploadVariant')
        or worker_request_headers.get('X-Sporely-Upload-Variant')
        or worker_detail_map.get('uploadVariant')
        or worker_detail_map.get('upload_variant')
        or ''
    ).strip().lower()
    if local_upload_variant:
        details.append(f"Local upload variant: {local_upload_variant}")

    upload_mode = str(
        meta.get('upload_mode')
        or meta.get('uploadMode')
        or worker_request_headers.get('X-Sporely-Upload-Mode')
        or worker_detail_map.get('uploadMode')
        or worker_detail_map.get('upload_mode')
        or ''
    ).strip().lower()
    quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    if not quality_profile and worker_detail_map:
        quality_profile = str(
            worker_detail_map.get('qualityProfile')
            or worker_detail_map.get('quality_profile')
            or ''
        ).strip().lower()
    if upload_mode or quality_profile:
        details.append(
            f"Local upload mode: {upload_mode or 'unknown'} / {quality_profile or 'standard'}"
        )

    local_content_type = str(
        content_type
        or meta.get('content_type')
        or meta.get('contentType')
        or worker_request_headers.get('Content-Type')
        or worker_detail_map.get('contentType')
        or worker_detail_map.get('content_type')
        or ''
    ).strip().lower()
    if local_content_type:
        details.append(f"Content type: {local_content_type}")

    if worker_base_url:
        worker_base = str(worker_base_url or '').strip().rstrip('/')
    else:
        worker_base = _worker_base_url_from_request_url(worker_request_url)
        if not worker_base and worker_payload:
            worker_base = media_worker_base_url()
    if worker_base:
        details.append(f"Worker base URL: {worker_base}")

    storage_key_text = str(storage_key or meta.get('storage_path') or meta.get('storageKey') or '').strip()
    if storage_key_text:
        details.append(f"Storage key: {storage_key_text}")

    desktop_cloud_plan = str(meta.get('cloud_plan') or meta.get('cloudPlan') or '').strip().lower()
    desktop_quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    if desktop_cloud_plan or desktop_quality_profile:
        details.append(
            f"Desktop cloud plan: {desktop_cloud_plan or 'unknown'} / {desktop_quality_profile or 'standard'}"
        )

    worker_cloud_plan = ""
    worker_quality_profile = ""
    worker_plan_cap = 0
    worker_body_bytes = 0
    worker_stored_width = 0
    worker_stored_height = 0
    worker_stored_pixels = 0
    worker_stored_pixel_cap = 0
    worker_resize_max_edge = 0
    worker_upload_mode = ""
    worker_upload_variant = ""
    worker_encoding_format = ""
    worker_content_type = ""
    reason = ""

    if worker_payload:
        worker_cloud_plan = str(
            worker_detail_map.get('cloudPlan')
            or worker_detail_map.get('cloud_plan')
            or worker_payload.get('cloudPlan')
            or worker_payload.get('cloud_plan')
            or ''
        ).strip().lower()
        worker_quality_profile = str(
            worker_detail_map.get('qualityProfile')
            or worker_detail_map.get('quality_profile')
            or worker_payload.get('qualityProfile')
            or worker_payload.get('quality_profile')
            or ''
        ).strip().lower()
        worker_plan_cap = _safe_int(
            worker_detail_map.get('planByteCap')
            or worker_detail_map.get('plan_byte_cap')
            or worker_payload.get('planByteCap')
            or worker_payload.get('plan_byte_cap')
        )
        worker_body_bytes = _safe_int(
            worker_detail_map.get('bodyBytes')
            or worker_detail_map.get('body_bytes')
            or worker_payload.get('bodyBytes')
            or worker_payload.get('body_bytes')
        )
        worker_stored_width = _safe_int(
            worker_detail_map.get('storedWidth')
            or worker_detail_map.get('stored_width')
        )
        worker_stored_height = _safe_int(
            worker_detail_map.get('storedHeight')
            or worker_detail_map.get('stored_height')
        )
        worker_stored_pixels = _safe_int(
            worker_detail_map.get('storedPixels')
            or worker_detail_map.get('stored_pixels')
        )
        worker_stored_pixel_cap = _safe_int(
            worker_detail_map.get('storedPixelCap')
            or worker_detail_map.get('stored_pixel_cap')
        )
        worker_resize_max_edge = _safe_int(
            worker_detail_map.get('resizeMaxEdge')
            or worker_detail_map.get('resize_max_edge')
        )
        worker_upload_mode = str(
            worker_detail_map.get('uploadMode')
            or worker_detail_map.get('upload_mode')
            or worker_request_headers.get('X-Sporely-Upload-Mode')
            or ''
        ).strip().lower()
        worker_upload_variant = str(
            worker_detail_map.get('uploadVariant')
            or worker_detail_map.get('upload_variant')
            or worker_request_headers.get('X-Sporely-Upload-Variant')
            or ''
        ).strip().lower()
        worker_encoding_format = str(
            worker_detail_map.get('encodingFormat')
            or worker_detail_map.get('encoding_format')
            or worker_request_headers.get('X-Sporely-Encoding-Format')
            or ''
        ).strip().lower()
        worker_content_type = str(
            worker_detail_map.get('contentType')
            or worker_detail_map.get('content_type')
            or worker_request_headers.get('Content-Type')
            or ''
        ).strip().lower()

        if worker_code:
            details.append(f"Worker error code: {worker_code}")
        if worker_message:
            details.append(f"Worker message: {worker_message}")
        if worker_reason:
            details.append(f"Worker reason: {worker_reason}")
        if worker_cloud_plan or worker_quality_profile:
            details.append(
                f"Worker cloud plan: {worker_cloud_plan or 'unknown'} / {worker_quality_profile or 'standard'}"
            )
        if worker_plan_cap > 0:
            details.append(f"Worker plan cap: {_format_size(worker_plan_cap)}")
        if worker_body_bytes > 0:
            details.append(f"Worker body size: {_format_size(worker_body_bytes)}")
        if worker_stored_width > 0 and worker_stored_height > 0:
            details.append(f"Worker stored dimensions: {worker_stored_width} × {worker_stored_height} px")
        if worker_stored_pixels > 0:
            details.append(f"Worker stored pixels: {worker_stored_pixels:,}")
        if worker_stored_pixel_cap > 0:
            details.append(f"Worker stored pixel cap: {worker_stored_pixel_cap:,}")
        if worker_resize_max_edge > 0:
            details.append(f"Worker resize max edge: {worker_resize_max_edge}px")
        if worker_upload_mode or worker_upload_variant:
            details.append(
                f"Worker upload mode: {worker_upload_mode or 'unknown'} / {worker_upload_variant or 'unknown'}"
            )
            if worker_encoding_format or worker_content_type:
                details.append(
                    f"Worker encoding/content type: {worker_encoding_format or 'unknown'} / {worker_content_type or 'unknown'}"
                )

    if not reason:
        reason = infer_image_too_large_for_plan_reason(
            {
                'error': worker_code or 'image_too_large_for_plan',
                'message': worker_message,
                'reason': worker_reason,
                'details': {
                    'bodyBytes': worker_body_bytes,
                    'planByteCap': worker_plan_cap,
                    'storedWidth': worker_stored_width,
                    'storedHeight': worker_stored_height,
                    'storedPixels': worker_stored_pixels,
                    'storedPixelCap': worker_stored_pixel_cap,
                    'resizeMaxEdge': worker_resize_max_edge,
                    'preparedBytes': prepared_bytes,
                    'planCap': byte_cap,
                    'preparedWidth': prepared_width,
                    'preparedHeight': prepared_height,
                },
            }
        )

    if inner_error is not None:
        extra = str(inner_error or '').strip()
        if extra and not is_image_too_large_for_plan_error(extra):
            details.append(f"Details: {extra}")

    first_line = format_image_too_large_for_plan_reason(reason)
    if not details:
        return first_line
    return "\n".join([first_line, "", *details])

def get_conflict_detail(client: "SporelyCloudClient", local_id: int, cloud_id: str | None = None) -> dict:
    """Enhanced conflict details with filename-level media differences."""
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    remote_obs = client.get_observation(resolved_cloud_id)

    remote_images_raw = client.pull_image_metadata(resolved_cloud_id, include_deleted_for_sync=True) or []
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(resolved_cloud_id))
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]
    tombstoned_remote_image_keys = _deleted_remote_image_identity_keys(remote_images_raw)
    remote_images = [
        dict(row or {})
        for row in remote_images_raw
        if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
    ]

    # 1. Field Comparisons
    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    field_rows = []
    for field in _CONFLICT_COMPARE_FIELDS:
        l_val, r_val, b_val = local_payload.get(field), remote_payload.get(field), baseline_obs.get(field)
        if _observation_field_values_match(field, l_val, r_val):
            continue
        field_rows.append({
            'field': field,
            'label': _CONFLICT_FIELD_LABELS.get(field, field.replace('_', ' ').title()),
            'baseline': b_val, 'local': l_val, 'remote': r_val,
            'local_changed': not _observation_field_values_match(field, l_val, b_val),
            'remote_changed': not _observation_field_values_match(field, r_val, b_val),
        })

    # 2. Detailed Image Differences
    local_images_raw = ImageDB.get_images_for_observation(int(local_id))
    local_image_payloads = [_local_image_snapshot_payload(img) for img in local_images_raw]
    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]

    image_mismatches = []
    local_map = {_image_compare_key(img): img for img in local_image_payloads}
    remote_map = {_image_compare_key(img): img for img in remote_image_payloads}

    for key in sorted(set(local_map.keys()) | set(remote_map.keys())):
        if key in tombstoned_remote_image_keys:
            continue
        l_img, r_img = local_map.get(key), remote_map.get(key)
        if l_img and r_img:
            continue
        if not l_img and not r_img:
            continue
        image_mismatches.append({
            'filename': _image_label(l_img or r_img),
            'status': 'local_only' if l_img else 'cloud_only',
        })

    return {
        'local_id': int(local_id),
        'cloud_id': resolved_cloud_id,
        'title': _observation_display_name(local_obs),
        'field_rows': field_rows,
        'image_mismatches': image_mismatches,
        'local_image_changes': _summarize_image_changes(
            local_image_payloads,
            baseline_images,
            ignored_keys=tombstoned_remote_image_keys,
        ),
        'remote_image_changes': _summarize_image_changes(
            remote_image_payloads,
            baseline_images,
            ignored_keys=tombstoned_remote_image_keys,
        ),
        'local_measurement_count': len(MeasurementDB.get_measurements_for_observation(int(local_id))),
    }
# ── High-level sync entry points ──────────────────────────────────────────────

def push_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
    sync_calibrations: bool = True,
) -> dict:
    """Push all unsynced / dirty observations (and optionally images) to cloud.

    Returns a summary dict with counts.
    """
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()

    # Observation preflight: the dirty scans below run before any per-observation
    # progress update, so on a no-change sync (0 dirty rows) they were a silent
    # gap that left the UI stuck on the last calibration label. Emit a neutral
    # message first and time each sub-step so a slow scan is named in the log.
    preflight_start = _cloud_sync_perf_counter()
    print("[cloud_sync] observation preflight: start", flush=True)
    _emit_progress(progress_cb, "Checking local observation changes…", progress_state)

    media_scan_start = _cloud_sync_perf_counter()
    _mark_cloud_observations_dirty_for_media_changes()
    media_scan_elapsed = _cloud_sync_perf_counter() - media_scan_start
    print(
        f"[cloud_sync] observation preflight: media dirty scan complete "
        f"duration={media_scan_elapsed * 1000:.0f}ms",
        flush=True,
    )

    if sync_images:
        pending_scan_start = _cloud_sync_perf_counter()
        _mark_cloud_observations_dirty_for_pending_local_images()
        pending_scan_elapsed = _cloud_sync_perf_counter() - pending_scan_start
        redirtied = _sync_summary_value(
            _cloud_sync_current_summary(), 'observations_redirtied_pending_local_images'
        )
        print(
            f"[cloud_sync] observation preflight: pending image dirty scan complete "
            f"re_dirtied={redirtied} duration={pending_scan_elapsed * 1000:.0f}ms",
            flush=True,
        )

    calibration_result = {'pushed': 0, 'total': 0, 'errors': []}
    if sync_calibrations:
        calibration_result = push_calibrations(
            client,
            progress_cb=progress_cb,
            progress_state=progress_state,
        )

    dirty_scan_start = _cloud_sync_perf_counter()
    cursor.execute(
        "SELECT * FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty' ORDER BY date DESC"
    )
    observations = [dict(r) for r in cursor.fetchall()]
    conn.close()
    dirty_scan_elapsed = _cloud_sync_perf_counter() - dirty_scan_start
    print(
        f"[cloud_sync] observation preflight: local dirty scan complete "
        f"count={len(observations)} duration={dirty_scan_elapsed * 1000:.0f}ms",
        flush=True,
    )
    print(
        f"[cloud_sync] observation preflight: complete "
        f"candidates={len(observations)} duration={(_cloud_sync_perf_counter() - preflight_start) * 1000:.0f}ms",
        flush=True,
    )

    total = len(observations)
    pushed = 0
    errors = list(calibration_result.get('errors') or [])
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    progress_state['done'] = _progress_done(progress_state)
    progress_state['total'] = _progress_total(progress_state) + total
    original_upload_summary = _new_original_upload_summary(is_full_resolution_original_sync_enabled())
    remote_lookup = {
        str(row.get('id') or '').strip(): row
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }

    for i, obs in enumerate(observations):
        _increment_sync_summary(_cloud_sync_current_summary(), 'observations_checked')
        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                obs,
                f"Syncing observation {i + 1}/{max(1, total)}…",
            ),
            progress_state,
        )
        try:
            cloud_id = str(obs.get('cloud_id') or '').strip()
            had_existing_cloud = bool(cloud_id)
            stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
            remote = remote_lookup.get(cloud_id) if cloud_id else None
            if cloud_id and remote is None:
                _advance_progress(progress_state, 1)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Cloud copy was deleted for observation {i + 1}/{max(1, total)}",
                    ),
                    progress_state,
                )
                continue
            push_payload = dict(obs)
            if cloud_id and stored_snapshot and remote:
                if sync_images:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Checking cloud media for observation {i + 1}/{max(1, total)}…",
                        ),
                        progress_state,
                    )
                remote_images = client.pull_image_metadata(cloud_id) or []
                remote_measurements = _pull_remote_measurements_for_images(
                    client,
                    [str(row.get('id') or '').strip() for row in remote_images if str(row.get('id') or '').strip()],
                )
                remote_snapshot = _cloud_observation_snapshot(remote, remote_images, remote_measurements)
                if remote_snapshot != stored_snapshot and _clear_observation_dirty_if_no_real_changes(int(obs['id']), cloud_id):
                    _advance_progress(progress_state, 1)
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Skipped stale local change for observation {i + 1}/{max(1, total)}",
                        ),
                        progress_state,
                    )
                    continue
                if remote_snapshot != stored_snapshot:
                    snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                    baseline_obs = _baseline_observation_compare_payload(snapshot_data.get('observation') or {})
                    field_changes = _analyze_observation_field_changes(obs, remote, baseline_obs)
                    remote_update_kwargs = _remote_observation_update_kwargs(remote)
                    for field in {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('remote_only_fields') or [])
                    }:
                        if field in remote_update_kwargs:
                            push_payload[field] = remote_update_kwargs[field]

            cloud_id = client.push_observation(
                _merge_cloud_selected_ai_fields(push_payload, remote),
                remote_obs=remote,
            )

            # Update local record with cloud_id and sync_status
            conn2 = get_connection()
            cursor2 = conn2.cursor()
            update_observation_sync_state(
                cursor2,
                int(obs['id']),
                cloud_id=cloud_id,
                sync_status='synced',
                synced_at=datetime.now(timezone.utc).isoformat(),
                clear_sync_error_state=True,
            )
            conn2.commit()
            conn2.close()

            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Observation {i + 1}/{max(1, total)} synced",
                ),
                progress_state,
            )

            images_synced = True
            if sync_images:
                local_obs_id = _safe_int(obs.get('id'))

                def _push_measurements_for_current_observation() -> None:
                    if local_obs_id <= 0:
                        return
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Syncing measurements for observation {i + 1}/{max(1, total)}…",
                        ),
                        progress_state,
                    )
                    try:
                        _push_measurements_for_observation(client, local_obs_id)
                    except Exception as e:
                        if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                            raise
                        print(f'[cloud_sync] Measurement push failed for obs {local_obs_id}: {e}')
                    else:
                        print(
                            f'[cloud_sync] Observation {obs["id"]}: measurements pushed '
                            f'(local_id={local_obs_id})'
                        )

                stored_local_media_signature = (
                    _load_local_cloud_media_signature(local_obs_id)
                    if had_existing_cloud and local_obs_id > 0
                    else ''
                )
                current_local_image_signature = ''
                if had_existing_cloud and local_obs_id > 0 and stored_local_media_signature:
                    current_local_image_signature = _local_cloud_image_media_signature(local_obs_id)
                image_render_unchanged = (
                    had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_image_signature
                    and _local_media_signatures_match(
                        stored_local_media_signature,
                        current_local_image_signature,
                        include_measurements=False,
                    )
                )
                tombstone_cleanup_only = (
                    not image_render_unchanged
                    and had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_image_signature
                    and _local_media_signatures_match_ignoring_tombstoned_images(
                        stored_local_media_signature,
                        current_local_image_signature,
                    )
                )
                if _cloud_sync_debug_enabled():
                    prep_diagnostics = _local_media_prep_diagnostics(
                        local_obs_id,
                        stored_local_media_signature,
                        current_local_image_signature,
                    )
                    prep_decision = (
                        'skip prep'
                        if image_render_unchanged
                        else 'metadata-only image sync'
                        if tombstone_cleanup_only
                        else 'full image prep'
                    )
                    print(
                        (
                            f'[cloud_sync] Observation {obs["id"]}: image prep diagnostics '
                            f'image_render_signature_matched={prep_diagnostics["image_render_signature_matched"]} '
                            f'tombstone_aware_signature_matched={prep_diagnostics["tombstone_aware_signature_matched"]} '
                            f'measurement_only_matched={prep_diagnostics["measurement_only_matched"]} '
                            f'has_local_image_cloud_id_null={prep_diagnostics["has_local_image_cloud_id_null"]} '
                            f'image_file_signature_changed={prep_diagnostics["any_image_file_signature_changed"]} '
                            f'render_affecting_field_changed={prep_diagnostics["any_render_affecting_field_changed"]} '
                            f'only_metadata_fields_changed={prep_diagnostics["only_metadata_fields_changed"]} '
                            f'decision={prep_decision} '
                            f'changed_keys={_format_local_media_prep_diagnostic_keys(prep_diagnostics["changed_keys"])}'
                        ),
                        flush=True,
                    )
                if image_render_unchanged:
                    measurement_only = bool(
                        stored_local_media_signature
                        and current_local_image_signature
                        and not _local_media_signatures_match(
                            stored_local_media_signature,
                            current_local_image_signature,
                        )
                    )
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Image/render media unchanged; skipping image prep for "
                                f"observation {i + 1}/{max(1, total)} "
                                f"(reason={'measurement_only' if measurement_only else 'unchanged'}, "
                                f"no prepared upload candidates)"
                            ),
                        ),
                        progress_state,
                    )
                    _push_measurements_for_current_observation()
                elif tombstone_cleanup_only:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Image/render media unchanged after tombstone cleanup; "
                                f"skipping image prep for observation {i + 1}/{max(1, total)} "
                                f"(reason=tombstone_only, metadata-only image sync)"
                            ),
                        ),
                        progress_state,
                    )
                    original_upload_warnings = []
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=None,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                        summary_warnings=original_upload_warnings,
                        original_summary=original_upload_summary,
                    )
                    if images_synced and local_obs_id > 0:
                        _push_measurements_for_current_observation()
                else:
                    original_upload_warnings: list[str] = []
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=prepare_images_cb,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                        summary_warnings=original_upload_warnings,
                        original_summary=original_upload_summary,
                    )
                    if images_synced and local_obs_id > 0:
                        _push_measurements_for_current_observation()
                if local_obs_id > 0:
                    if images_synced:
                        _refresh_local_cloud_media_signature(local_obs_id)
                    else:
                        mark_observation_dirty(local_obs_id)
            _store_remote_snapshot(client, cloud_id)

            pushed += 1
        except CloudSyncError as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            raw_error = f"obs {obs['id']}: {e}"
            if is_privacy_slot_limit_error(raw_error):
                _set_observation_privacy_blocked(int(obs['id']), raw_error)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} blocked: "
                            f"{privacy_slot_limit_user_message()}"
                        ),
                    ),
                    progress_state,
                )
            elif is_image_too_large_for_plan_error(raw_error):
                _set_observation_plan_image_retryable(int(obs['id']), raw_error)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} needs retry: "
                            f"{summarize_image_too_large_for_plan_error(raw_error)}"
                        ),
                    ),
                    progress_state,
                )
            elif is_webp_support_required_for_cloud_media_upload_error(raw_error):
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} failed: "
                            f"{WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE}"
                        ),
                    ),
                    progress_state,
                )
            else:
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Observation {i + 1}/{max(1, total)} failed",
                    ),
                    progress_state,
                )
            errors.append(raw_error)
            _advance_progress(progress_state, 1)

    result = {
        'pushed': pushed,
        'total': total,
        'calibrations_pushed': calibration_result.get('pushed', 0),
        'calibrations_total': calibration_result.get('total', 0),
        'errors': errors,
    }
    if format_original_upload_summary(original_upload_summary):
        result['original_sync'] = original_upload_summary
    sync_summary = _cloud_sync_current_summary()
    if sync_summary is not None:
        result['sync_summary'] = dict(sync_summary)
    return result


# Image rows the upload-preparation step should skip because a remote-first
# pass already re-associated them with an existing cloud image. Passed to the
# prepare callback via the observation dict so no temporary WebP candidate is
# encoded for metadata-only associations.
CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY = '_cloud_sync_skip_prepare_image_ids'


def _associate_persisted_cloud_images(
    client: SporelyCloudClient,
    obs: dict,
    existing_rows: list[dict],
) -> tuple[set[int], set[str]]:
    """Re-link orphaned local image rows to existing remote cloud images.

    Targets the repair case described in the sync bug: a local image row whose
    ``cloud_id`` was lost but which still has a matching remote cloud image
    (same ``desktop_id``), an existing remote ``storage_path`` and descriptive
    metadata that already matches. The local ``cloud_id`` is restored without
    uploading any bytes and without encoding a temporary WebP candidate.

    Returns ``(associated_local_image_ids, kept_remote_cloud_ids)``. The first
    set tells the upload-preparation step which rows it can skip; the second
    seeds ``kept_cloud_ids`` so the re-linked remote rows are not treated as
    stale and deleted.
    """
    associated_ids: set[int] = set()
    kept_cloud_ids: set[str] = set()
    try:
        obs_local_id = int(obs.get('id'))
    except Exception:
        return associated_ids, kept_cloud_ids

    existing_by_desktop_id = {
        _safe_int(row.get('desktop_id')): row
        for row in (existing_rows or [])
        if _safe_int(row.get('desktop_id')) != 0
    }
    if not existing_by_desktop_id:
        return associated_ids, kept_cloud_ids

    include_ai_crop = client._observation_images_support_ai_crop()
    include_upload_meta = client._observation_images_support_upload_metadata()
    # Fields that describe the uploaded bytes rather than user metadata. They
    # cannot be known without encoding, so they are excluded from the
    # metadata-only match decision — the remote bytes are unchanged anyway.
    upload_derived_keys = {
        'id',
        'storage_path',
        'original_filename',
        'source_width',
        'source_height',
        'stored_width',
        'stored_height',
        'stored_bytes',
        'upload_mode',
    }

    for image_row in ImageDB.get_images_for_observation(obs_local_id):
        img = dict(image_row or {})
        local_image_id = _safe_int(img.get('id'))
        if local_image_id <= 0 or not should_push_local_image_to_cloud(img):
            continue
        # Only repair orphaned rows. Rows that still hold a cloud_id (correct or
        # conflicting) and never-synced rows flow through the normal path.
        if str(img.get('cloud_id') or '').strip():
            continue
        if not str(img.get('synced_at') or '').strip():
            continue
        remote_row = existing_by_desktop_id.get(local_image_id)
        if not remote_row:
            continue
        remote_cloud_id = str(remote_row.get('id') or '').strip()
        remote_storage_path = _normalize_cloud_media_key(remote_row.get('storage_path'))
        if not remote_cloud_id or not remote_storage_path:
            continue

        expected_payload = _prepared_item_remote_payload(
            img,
            '',
            remote_storage_path,
            include_ai_crop=include_ai_crop,
            include_upload_meta=include_upload_meta,
        )
        remote_payload = _remote_image_payload(
            remote_row,
            include_ai_crop=include_ai_crop,
            include_upload_meta=include_upload_meta,
        )
        if not _image_calibration_uuid(img):
            expected_payload.pop('calibration_uuid', None)
            remote_payload.pop('calibration_uuid', None)
        for key in upload_derived_keys:
            expected_payload.pop(key, None)
            remote_payload.pop(key, None)
        if expected_payload != remote_payload:
            # Descriptive metadata changed while the link was missing — let the
            # normal prepare + metadata-patch path handle it.
            continue

        if _reconcile_local_image_cloud_id(local_image_id, remote_cloud_id, mark_synced=True):
            _increment_sync_summary(_cloud_sync_current_summary(), 'images_cloud_id_repaired')
            print(
                f'[cloud_sync] Observation {obs_local_id}: metadata association for cloud image '
                f'actual_upload=False image_id={local_image_id} cloud_image_id={remote_cloud_id} '
                f'storage_path={remote_storage_path} '
                f'(restored local cloud_id without preparing an upload candidate)'
            )
        associated_ids.add(local_image_id)
        kept_cloud_ids.add(remote_cloud_id)

    return associated_ids, kept_cloud_ids


def _push_images_for_observation(
    client: SporelyCloudClient,
    obs: dict,
    obs_cloud_id: str,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    observation_index: int | None = None,
    observation_total: int | None = None,
    summary_warnings: list[str] | None = None,
    original_summary: dict | None = None,
) -> bool:
    """Push selected observation images for one observation."""
    warnings: list[str] = []
    warnings.extend(_push_pending_image_tombstones(client))
    prepared_items: list[dict] = []
    cleanup = None
    preparation_failed = False
    # Remote-first pass: pull existing cloud image metadata once up front so we
    # can re-associate orphaned local rows (cloud_id lost but already uploaded)
    # before any temporary WebP candidate is encoded for them. Reused by the
    # main upload loop below to avoid a second metadata fetch.
    prepass_existing_rows: list[dict] | None = None
    prepass_kept_cloud_ids: set[str] = set()
    skip_prepare_image_ids: set[int] = set()
    if callable(prepare_images_cb):
        try:
            prepass_existing_rows = client.pull_image_metadata(obs_cloud_id) or []
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(
                f'[cloud_sync] Could not pre-fetch cloud images for observation {obs["id"]}: {e}'
            )
            prepass_existing_rows = None
        if prepass_existing_rows is not None:
            skip_prepare_image_ids, prepass_kept_cloud_ids = _associate_persisted_cloud_images(
                client, obs, prepass_existing_rows
            )
    if callable(prepare_images_cb):
        try:
            def prepare_progress(message: str, _current: int | None = None, _total: int | None = None) -> None:
                _emit_progress(progress_cb, message, progress_state)

            prepare_obs = dict(obs)
            if skip_prepare_image_ids:
                prepare_obs[CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY] = sorted(skip_prepare_image_ids)
            prepared_items, cleanup, prep_warnings = prepare_images_cb(prepare_obs, prepare_progress)
            warnings.extend(prep_warnings or [])
        except Exception as e:
            if is_image_too_large_for_plan_error(e) or is_webp_support_required_for_cloud_media_upload_error(e):
                raise
            print(f'[cloud_sync] Observation {obs["id"]} image preparation failed: {e}')
            prepared_items = []
            cleanup = None
            warnings.append(str(e))
            preparation_failed = True
    else:
        images = ImageDB.get_images_for_observation(obs['id'])
        for img in images:
            if img.get('image_type') == 'microscope' and not img.get('cloud_id'):
                continue
            prepared_items.append({
                'image_row': img,
                'upload_path': img.get('filepath'),
            })

    for warning in warnings:
        print(f'[cloud_sync] Observation {obs["id"]}: {warning}')

    def _record_original_upload_warning(message: str) -> None:
        text = str(message or '').strip()
        if not text:
            return
        print(f'[cloud_sync] Observation {obs["id"]}: {text}')
        if isinstance(summary_warnings, list):
            summary_warnings.append(text)

    def _record_original_summary(metric: str) -> None:
        if not isinstance(original_summary, dict):
            return
        current = _safe_int(original_summary.get(metric))
        original_summary[metric] = current + 1

    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [
            str(dict(item.get('image_row') or {}).get('cloud_id') or '').strip()
            for item in prepared_items
            if str(dict(item.get('image_row') or {}).get('cloud_id') or '').strip()
        ]
    )
    tombstoned_local_image_ids = _local_tombstoned_local_image_ids(
        [
            _safe_int(dict(item.get('image_row') or {}).get('id'))
            for item in prepared_items
            if _safe_int(dict(item.get('image_row') or {}).get('id')) > 0
        ]
    )
    if tombstoned_cloud_ids:
        filtered_items: list[dict] = []
        for item in prepared_items:
            img = dict(item.get('image_row') or {})
            cloud_image_id = str(img.get('cloud_id') or '').strip()
            if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                warning = _tombstoned_cloud_image_warning(obs.get('id'), cloud_image_id)
                warnings.append(warning)
                print(f'[cloud_sync] Warning: {warning}')
                continue
            filtered_items.append(item)
        prepared_items = filtered_items
    if tombstoned_local_image_ids:
        filtered_items = []
        for item in prepared_items:
            img = dict(item.get('image_row') or {})
            local_image_id = _safe_int(img.get('id'))
            if local_image_id > 0 and local_image_id in tombstoned_local_image_ids:
                warning = f"obs {obs.get('id')}: skipped local image {local_image_id} because it has a local tombstone"
                warnings.append(warning)
                print(f'[cloud_sync] Warning: {warning}')
                continue
            filtered_items.append(item)
        prepared_items = filtered_items

    if prepared_items:
        _extend_progress_total(progress_state, len(prepared_items))
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Prepared {len(prepared_items)} image(s) for upload in observation {observation_index}/{max(1, observation_total)}",
                ),
                progress_state,
            )

    if preparation_failed:
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Cloud media preparation failed for observation {observation_index}/{max(1, observation_total)}",
                ),
                progress_state,
            )
        return False

    if prepass_existing_rows is not None:
        existing_rows = prepass_existing_rows
    else:
        try:
            existing_rows = client.pull_image_metadata(obs_cloud_id) or []
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(f'[cloud_sync] Could not fetch existing cloud images for observation {obs["id"]}: {e}')
            existing_rows = []
    existing_by_id = {
        str(row.get('id') or '').strip(): row
        for row in existing_rows
        if str(row.get('id') or '').strip()
    }
    existing_by_desktop_id = {
        _safe_int(row.get('desktop_id')): row
        for row in existing_rows
        if _safe_int(row.get('desktop_id')) != 0
    }

    try:
        processed_items = 0
        total_items = len(prepared_items)
        # Seed with remote rows re-associated by the remote-first pass so they
        # are not deleted as stale even though they were skipped during prepare.
        kept_cloud_ids: set[str] = set(prepass_kept_cloud_ids)
        had_failures = False
        include_ai_crop = client._observation_images_support_ai_crop()
        include_upload_meta = client._observation_images_support_upload_metadata()
        original_sync_enabled = is_full_resolution_original_sync_enabled()
        for item_index, item in enumerate(prepared_items, start=1):
            _increment_sync_summary(_cloud_sync_current_summary(), 'images_checked')
            img = dict(item.get('image_row') or {})
            img.update(dict(item.get('cloud_upload_meta') or {}))
            if observation_index and observation_total:
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Checking cloud image {item_index}/{max(1, total_items)}…",
                    ),
                    progress_state,
                )
            if not img:
                print(f'[cloud_sync] Observation {obs["id"]}: skipped empty cloud image item {item_index}')
                continue
            local_image_id = _safe_int(img.get('id'))
            local_cloud_id = str(img.get('cloud_id') or '').strip()
            remote_row = existing_by_desktop_id.get(local_image_id)
            if remote_row is None and local_cloud_id:
                remote_row = existing_by_id.get(local_cloud_id)
            remote_cloud_id = str((remote_row or {}).get('id') or '').strip()
            if not should_push_local_image_to_cloud(img):
                selected_cloud_id = remote_cloud_id or local_cloud_id
                if selected_cloud_id:
                    kept_cloud_ids.add(selected_cloud_id)
                print(
                    f'[cloud_sync] Observation {obs["id"]}: skipped cloud image '
                    f'{local_image_id or item_index} because it is not eligible for upload'
                )
                continue
            upload_path = str(item.get('upload_path') or img.get('filepath') or '').strip()
            if not upload_path:
                print(
                    f'[cloud_sync] Observation {obs["id"]}: skipped cloud image '
                    f'{local_image_id or item_index} because upload_path is missing'
                )
                continue
            try:
                existing_storage_path = _normalize_cloud_media_key((remote_row or {}).get('storage_path'))
                storage_path = existing_storage_path or _build_worker_storage_path(
                    client.user_id,
                    obs_cloud_id,
                    img,
                    upload_path,
                )
                expected_payload = _prepared_item_remote_payload(
                    img,
                    upload_path,
                    storage_path,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                local_calibration_uuid = _image_calibration_uuid(img)
                if remote_row and remote_row.get('original_filename'):
                    expected_payload['original_filename'] = _normalize_snapshot_value(remote_row.get('original_filename'))
                remote_payload = _remote_image_payload(
                    remote_row,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                if not local_calibration_uuid:
                    expected_payload.pop('calibration_uuid', None)
                    remote_payload.pop('calibration_uuid', None)
                metadata_matches = bool(remote_row) and remote_payload == expected_payload

                current_file_sig = _file_content_signature(upload_path)
                stored_file_sig = _load_cloud_image_file_signature(obs.get('id'), local_image_id)
                file_matches = False
                if remote_row and _normalize_cloud_media_key(remote_row.get('storage_path')) == _normalize_cloud_media_key(storage_path):
                    if stored_file_sig and stored_file_sig == current_file_sig:
                        file_matches = True
                    elif (
                        not stored_file_sig
                        and (
                            local_cloud_id == remote_cloud_id
                            or bool(img.get('synced_at'))
                        )
                        ):
                            file_matches = True

                if file_matches and metadata_matches:
                    _increment_sync_summary(_cloud_sync_current_summary(), 'images_skipped_already_synced')
                    # The remote bytes and metadata already match, but the local
                    # row may have lost its cloud_id (e.g. an earlier overwrite).
                    # Restore the association so this observation is not
                    # re-dirtied forever. This is a metadata-only repair —
                    # actual_upload=False, no bytes are sent.
                    if remote_cloud_id and local_image_id > 0 and local_cloud_id != remote_cloud_id:
                        if _reconcile_local_image_cloud_id(
                            local_image_id, remote_cloud_id, mark_synced=True
                        ):
                            _increment_sync_summary(
                                _cloud_sync_current_summary(), 'images_cloud_id_repaired'
                            )
                            print(
                                f'[cloud_sync] Observation {obs["id"]}: metadata association for cloud image '
                                f'actual_upload=False image_id={local_image_id or item_index} '
                                f'cloud_image_id={remote_cloud_id} storage_path={storage_path} '
                                f'(restored missing local cloud_id)'
                            )
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: skipped already synced cloud image '
                        f'{local_image_id or item_index} (storage_path={storage_path})'
                    )
                    if remote_cloud_id:
                        kept_cloud_ids.add(remote_cloud_id)
                    elif local_cloud_id:
                        kept_cloud_ids.add(local_cloud_id)
                    continue

                if not file_matches:
                    if observation_index and observation_total:
                        _emit_progress(
                            progress_cb,
                            _format_cloud_sync_observation_status(
                                obs,
                                f"Uploading cloud image {item_index}/{max(1, total_items)}…",
                            ),
                            progress_state,
                        )
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: Uploading cloud image request '
                        f'actual_upload=True image_id={local_image_id or item_index} '
                        f'cloud_image_id={local_cloud_id or remote_cloud_id or "new"} '
                        f'storage_path={storage_path} (uploading cloud image bytes)'
                    )
                else:
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: metadata patch for cloud image '
                        f'actual_upload=False image_id={local_image_id or item_index} '
                        f'cloud_image_id={local_cloud_id or remote_cloud_id or "new"} '
                        f'storage_path={storage_path}'
                    )

                img_cloud_id = remote_cloud_id
                if not file_matches:
                    uploaded_key = client.upload_image_file(
                        upload_path,
                        obs_cloud_id,
                        img_cloud_id,
                        storage_path=storage_path,
                        upload_meta=dict(item.get('cloud_upload_meta') or {}),
                    )
                    storage_path = _normalize_cloud_media_key(uploaded_key or storage_path)

                if not img_cloud_id or not metadata_matches:
                    if remote_row and remote_row.get('original_filename'):
                        img['original_filename'] = str(remote_row.get('original_filename') or '').strip()
                    img_cloud_id = client.push_image_metadata(img, obs_cloud_id, storage_path)
                    remote_payload = _prepared_item_remote_payload(
                        img,
                        upload_path,
                        storage_path,
                        include_ai_crop=include_ai_crop,
                        include_upload_meta=include_upload_meta,
                    )
                    if remote_row and remote_row.get('original_filename'):
                        remote_payload['original_filename'] = _normalize_snapshot_value(remote_row.get('original_filename'))
                    metadata_matches = True

                kept_cloud_ids.add(str(img_cloud_id or '').strip())

                try:
                    image_id = int(img['id'])
                except Exception:
                    image_id = 0
                if image_id > 0:
                    conn = get_connection()
                    conn.execute(
                        'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                        (img_cloud_id, datetime.now(timezone.utc).isoformat(), image_id),
                    )
                    conn.commit()
                    conn.close()
                if current_file_sig:
                    _store_cloud_image_file_signature(obs.get('id'), local_image_id, current_file_sig)

                original_upload_source = resolve_full_original_upload_source(img)
                original_storage_path = _normalize_cloud_media_key((remote_row or {}).get('original_storage_path'))
                if not original_storage_path:
                    profiler = _cloud_sync_current_profiler()
                    if not original_sync_enabled:
                        if profiler is not None:
                            try:
                                if original_upload_source is None:
                                    profiler.record_original_upload_skipped_ineligible()
                                else:
                                    profiler.record_original_upload_skipped_disabled()
                            except Exception:
                                pass
                        if original_upload_source is None:
                            _record_original_summary('skipped_ineligible')
                        else:
                            _record_original_summary('skipped_disabled')
                    elif original_upload_source is None:
                        if profiler is not None:
                            try:
                                profiler.record_original_upload_skipped_ineligible()
                            except Exception:
                                pass
                        _record_original_summary('skipped_ineligible')
                    else:
                        source_path = str(original_upload_source.get('source_path') or '').strip()
                        source_kind = str(original_upload_source.get('source_kind') or '').strip() or 'filepath'
                        source_size = 0
                        try:
                            source_size = int(Path(source_path).stat().st_size)
                        except Exception:
                            source_size = 0

                        if is_full_resolution_original_upload_too_large(source_path):
                            if profiler is not None:
                                try:
                                    profiler.record_original_upload_skipped_too_large()
                                except Exception:
                                    pass
                            _record_original_summary('skipped_too_large')
                            _record_original_upload_warning(
                                (
                                    f"skipped original upload for image {img.get('id')} "
                                    f"because {source_kind} is too large "
                                    f"({_format_size(source_size)} > "
                                    f"{_format_size(FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES)})"
                                )
                            )
                        else:
                            original_upload_meta = dict(item.get('cloud_upload_meta') or {})
                            original_upload_meta.update(
                                {
                                    'source_role': original_upload_source.get('source_role'),
                                    'source_kind': source_kind,
                                    'upload_mode': 'full',
                                    'upload_variant': 'original',
                                }
                            )
                            original_storage_path = client._build_original_storage_path(
                                obs_cloud_id,
                                img_cloud_id,
                                source_path,
                            )
                            try:
                                uploaded_original_key = client.upload_original_image_file(
                                    source_path,
                                    obs_cloud_id,
                                    img_cloud_id,
                                    storage_path=original_storage_path,
                                    upload_meta=original_upload_meta,
                                )
                            except CloudSyncError as exc:
                                if profiler is not None:
                                    try:
                                        profiler.record_original_upload_failed()
                                    except Exception:
                                        pass
                                _record_original_summary('failed_uploads')
                                _record_original_upload_warning(
                                    (
                                        f"original upload failed for image {img.get('id')} "
                                        f"from {source_kind}: {exc}"
                                    )
                                )
                            else:
                                original_storage_path = _normalize_cloud_media_key(
                                    uploaded_original_key or original_storage_path
                                )
                                if original_storage_path:
                                    try:
                                        client.set_image_original_storage_path(
                                            img_cloud_id,
                                            original_storage_path,
                                        )
                                    except Exception as exc:
                                        if profiler is not None:
                                            try:
                                                profiler.record_original_upload_failed()
                                            except Exception:
                                                pass
                                        _record_original_summary('failed_uploads')
                                        _record_original_upload_warning(
                                            (
                                                f"original upload succeeded for image {img.get('id')} "
                                                f"from {source_kind}, but patching original_storage_path failed: {exc}"
                                            )
                                        )
                                    else:
                                        if profiler is not None:
                                            try:
                                                profiler.record_original_upload_success(source_size)
                                            except Exception:
                                                pass
                                        _record_original_summary('uploaded')
                                        print(
                                            f'[cloud_sync] Observation {obs["id"]}: original upload source for image {img.get("id")} '
                                            f'was {source_kind}'
                                        )
            except CloudSyncError as e:
                if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                    raise
                if is_image_too_large_for_plan_error(e):
                    raise
                had_failures = True
                print(f'[cloud_sync] Image {img["id"]} push failed: {e}')
            finally:
                processed_items += 1
                _advance_progress(progress_state, 1)
        stale_rows = [
            row for row in existing_rows
            if str(row.get('id') or '').strip() and str(row.get('id') or '').strip() not in kept_cloud_ids
        ]
        for stale_row in stale_rows:
            stale_cloud_id = str(stale_row.get('id') or '').strip()
            stale_storage_path = _normalize_cloud_media_key(stale_row.get('storage_path'))
            print(
                f'[cloud_sync] Observation {obs["id"]}: deleting stale cloud image '
                f'{stale_cloud_id} (storage_path={stale_storage_path})'
            )
            if stale_storage_path:
                try:
                    client._storage_remove([stale_storage_path])
                except Exception as e:
                    if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                        raise
                    print(
                        f'[cloud_sync] Could not remove old cloud storage file for observation {obs["id"]}: {e}'
                    )
            try:
                client._delete(f'observation_images?id=eq.{stale_cloud_id}')
            except Exception as e:
                if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                    raise
                print(f'[cloud_sync] Could not remove old cloud image row for observation {obs["id"]}: {e}')
            stale_desktop_id = _safe_int(stale_row.get('desktop_id'))
            if stale_desktop_id > 0:
                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE images SET cloud_id = NULL, synced_at = NULL WHERE id = ?',
                        (stale_desktop_id,),
                    )
                    conn.commit()
                finally:
                    conn.close()
            _clear_cloud_image_file_signature(obs.get('id'), stale_desktop_id or stale_cloud_id)
        if total_items > processed_items:
            _advance_progress(progress_state, total_items - processed_items)
        return not had_failures
    finally:
        if callable(cleanup):
            try:
                cleanup()
            except Exception:
                pass


def _push_measurements_for_observation(
    client: SporelyCloudClient,
    obs_local_id: int,
) -> None:
    """Push all spore measurements for an observation's microscope images to the cloud.

    Only images that have a cloud_id (i.e. have been synced) are included.
    Measurements are upserted by desktop_id; stale cloud rows for images that
    still exist locally are cleaned up.
    """
    push_start = _cloud_sync_perf_counter()
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT m.id, m.image_id, m.length_um, m.width_um, m.measurement_type,
                   m.p1_x, m.p1_y, m.p2_x, m.p2_y,
                   m.p3_x, m.p3_y, m.p4_x, m.p4_y,
                   m.measured_at, m.cloud_id,
                   i.cloud_id AS image_cloud_id
            FROM spore_measurements m
            JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ?
              AND i.image_type = 'microscope'
              AND i.cloud_id IS NOT NULL
            ORDER BY m.id
            ''',
            (obs_local_id,),
        )
        measurements = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [
            str(row.get('image_cloud_id') or '').strip()
            for row in measurements
            if str(row.get('image_cloud_id') or '').strip()
        ]
    )
    if tombstoned_cloud_ids:
        filtered_measurements: list[dict] = []
        for meas in measurements:
            cloud_image_id = str(meas.get('image_cloud_id') or '').strip()
            if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                print(
                    f'[cloud_sync] Warning: obs {int(obs_local_id)}: skipped cloud measurement '
                    f'{meas["id"]} because cloud image {cloud_image_id} has a local tombstone'
                )
                continue
            filtered_measurements.append(meas)
        measurements = filtered_measurements

    remote_measurement_cache = fetch_remote_measurement_identity_cache(
        client,
        sorted(
            {
                str(row.get('image_cloud_id') or '').strip()
                for row in measurements
                if str(row.get('image_cloud_id') or '').strip()
            }
        ),
    )

    pushed_cloud_ids: set[str] = set()
    for meas in measurements:
        cloud_image_id = str(meas.get('image_cloud_id') or '').strip()
        if not cloud_image_id:
            continue
        try:
            cloud_meas_id = client.push_measurement(
                meas,
                cloud_image_id,
                remote_measurement_cache=remote_measurement_cache,
            )
            pushed_cloud_ids.add(cloud_meas_id)
            if str(meas.get('cloud_id') or '').strip() != cloud_meas_id:
                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE spore_measurements SET cloud_id = ? WHERE id = ?',
                        (cloud_meas_id, int(meas['id'])),
                    )
                    conn.commit()
                finally:
                    conn.close()
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(f'[cloud_sync] Measurement {meas["id"]} push failed: {e}')

    push_elapsed = _cloud_sync_perf_counter() - push_start
    print(
        (
            f'[cloud_sync] Observation {obs_local_id}: measurement push finalized '
            f'measurements={len(measurements)} pushed={len(pushed_cloud_ids)} '
            f'duration={push_elapsed * 1000:.0f}ms'
        ),
        flush=True,
    )


_SETTING_CLOUD_EXIF_BACKFILL_STATE = 'cloud_exif_backfill_checked'


def _exif_file_signature(path: Path) -> str | None:
    """Cheap file fingerprint (mtime + size) used to skip already-checked files.

    A `stat()` is orders of magnitude cheaper than opening + decoding the image,
    so it lets EXIF backfill avoid re-reading every cloud field image on a sync
    where nothing has changed.
    """
    try:
        st = path.stat()
    except Exception:
        return None
    return f'{st.st_mtime_ns}:{st.st_size}'


def _load_exif_backfill_state() -> dict:
    try:
        raw = SettingsDB.get_setting(_SETTING_CLOUD_EXIF_BACKFILL_STATE)
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def _save_exif_backfill_state(state: dict) -> None:
    try:
        SettingsDB.set_setting(
            _SETTING_CLOUD_EXIF_BACKFILL_STATE,
            json.dumps(state, separators=(',', ':')),
        )
    except Exception:
        pass


def _backfill_missing_exif_on_cloud_images() -> dict:
    """Inject observation GPS/datetime into field images whose EXIF was stripped
    by the web app's 2 MP conversion (cloud_id set but no EXIF datetime/GPS).

    Runs at the start of each pull.  To avoid a multi-second hidden tax on every
    no-change sync, files are fingerprinted by mtime+size: a file whose exact
    version was already checked is skipped without opening/decoding it.  Files
    are only opened when they are new or have changed since the last check.

    Returns a counters dict (scanned / skipped_cached / opened / already_complete
    / updated / missing_file) for instrumentation.
    """
    counters = {
        'scanned': 0,
        'skipped_cached': 0,
        'opened': 0,
        'already_complete': 0,
        'updated': 0,
        'missing_file': 0,
    }
    try:
        from PIL import Image as _PilImg, ExifTags as _ET
        conn = get_connection()
        try:
            rows = conn.execute(
                '''
                SELECT i.id, i.filepath, i.observation_id, i.image_type
                FROM images i
                WHERE i.cloud_id IS NOT NULL
                  AND i.image_type != 'microscope'
                  AND i.filepath IS NOT NULL
                '''
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            return counters

        prev_state = _load_exif_backfill_state()
        new_state: dict[str, str] = {}
        for row in rows:
            counters['scanned'] += 1
            image_id = str(row[0])
            filepath = str(row[1] or '').strip()
            if not filepath:
                continue
            p = Path(filepath)
            if p.suffix.lower() not in {'.jpg', '.jpeg', '.webp'}:
                continue
            sig = _exif_file_signature(p)
            if sig is None:
                # File missing/unreadable: don't cache, so it is retried once it
                # reappears (e.g. after media materialization).
                counters['missing_file'] += 1
                continue
            if prev_state.get(image_id) == sig:
                # Same file version already checked — skip the expensive open.
                counters['skipped_cached'] += 1
                new_state[image_id] = sig
                continue

            counters['opened'] += 1
            try:
                with _PilImg.open(p) as img:
                    exif = img.getexif()
                    tags = {_ET.TAGS.get(k, k): v for k, v in exif.items()} if exif else {}
                    already_has_dt = any(
                        t in tags for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
                    )
                    try:
                        already_has_gps = bool(exif.get_ifd(0x8825))
                    except Exception:
                        already_has_gps = False
                if already_has_dt and already_has_gps:
                    counters['already_complete'] += 1
                    new_state[image_id] = sig
                    continue
            except Exception:
                # Leave uncached so a transient read error is retried next sync.
                continue

            obs_id = int(row[2] or 0)
            if obs_id <= 0:
                continue
            lat, lon, altitude, gps_acc, date_str = _load_obs_exif_fallback(obs_id)
            _inject_obs_exif_into_field_image(
                p,
                lat,
                lon,
                altitude,
                date_str,
                gps_accuracy=gps_acc,
            )
            counters['updated'] += 1
            # Injection rewrites the file, so record the post-write signature to
            # skip it next time.
            new_state[image_id] = _exif_file_signature(p) or sig

        if new_state != prev_state:
            _save_exif_backfill_state(new_state)
    except Exception as exc:
        print(f'[cloud_sync] EXIF backfill skipped: {exc}')
    return counters


def pull_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
    sync_calibrations: bool = True,
    materialize_remote_images: bool = True,
) -> dict:
    """Pull new cloud observations and apply remote updates to clean local rows."""
    # Pull preflight: EXIF backfill, candidate build, and the bulk image /
    # measurement fetches all run before the first per-observation progress
    # update. On a no-change sync these were part of the silent gap that left the
    # UI on the last calibration label, so emit messages and time each sub-step.
    pull_preflight_start = _cloud_sync_perf_counter()
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _emit_progress(progress_cb, "Preparing cloud observations…", progress_state)

    _emit_progress(progress_cb, "Checking image EXIF metadata…", progress_state)
    exif_start = _cloud_sync_perf_counter()
    exif_counts = _backfill_missing_exif_on_cloud_images() or {}
    exif_elapsed = _cloud_sync_perf_counter() - exif_start
    print(
        f"[cloud_sync] pull preflight: exif backfill complete "
        f"scanned={exif_counts.get('scanned', 0)} "
        f"skipped_cached={exif_counts.get('skipped_cached', 0)} "
        f"opened={exif_counts.get('opened', 0)} "
        f"updated={exif_counts.get('updated', 0)} "
        f"duration={exif_elapsed * 1000:.0f}ms",
        flush=True,
    )

    remote_obs = list(remote_obs or client.list_remote_observations())
    calibration_result = {'pulled': 0, 'total': 0, 'errors': []}
    if sync_calibrations:
        calibration_result = pull_calibrations(
            client,
            progress_cb=progress_cb,
            progress_state=progress_state,
        )
    pulled = 0
    errors = list(calibration_result.get('errors') or [])
    imported_local_ids: list[int] = []
    candidate_start = _cloud_sync_perf_counter()
    local_by_cloud_id, local_by_id = _load_local_observation_lookup()
    candidates: list[tuple[dict, dict | None, str]] = []
    candidate_cloud_ids: list[str] = []
    for remote in remote_obs:
        cloud_id = str(remote.get('id') or '').strip()
        local_obs = _find_local_observation_for_remote_cached(remote, local_by_cloud_id, local_by_id)
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
        candidates.append((remote, local_obs, stored_snapshot))
        if cloud_id:
            candidate_cloud_ids.append(cloud_id)

    total = len(candidates)
    candidate_elapsed = _cloud_sync_perf_counter() - candidate_start
    print(
        f"[cloud_sync] pull preflight: candidate build complete "
        f"count={total} duration={candidate_elapsed * 1000:.0f}ms",
        flush=True,
    )
    _extend_progress_total(progress_state, total)
    if total:
        _emit_progress(progress_cb, "Loading cloud image metadata…", progress_state)
    bulk_start = _cloud_sync_perf_counter()
    bulk_fetcher = getattr(client, 'pull_bulk_image_metadata', None)
    if callable(bulk_fetcher):
        bulk_images = [dict(row or {}) for row in (bulk_fetcher(candidate_cloud_ids) or [])]
    else:
        bulk_images = []
        for cloud_id in candidate_cloud_ids:
            bulk_images.extend(
                dict(row or {})
                for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])
            )
    bulk_elapsed = _cloud_sync_perf_counter() - bulk_start
    print(
        f"[cloud_sync] pull preflight: cloud image metadata fetched "
        f"images={len(bulk_images)} duration={bulk_elapsed * 1000:.0f}ms",
        flush=True,
    )
    remote_images_by_obs = {}
    for img in bulk_images:
        obs_id = str(img.get('observation_id') or '').strip()
        if obs_id:
            remote_images_by_obs.setdefault(obs_id, []).append(img)
    if total:
        _emit_progress(progress_cb, "Loading cloud measurements…", progress_state)
    measurements_start = _cloud_sync_perf_counter()
    remote_measurements = _pull_remote_measurements_for_images(
        client,
        [str(row.get('id') or '').strip() for row in bulk_images if str(row.get('id') or '').strip()],
    )
    remote_measurements_by_obs = _group_remote_measurements_by_observation(
        bulk_images,
        remote_measurements,
    )
    measurements_elapsed = _cloud_sync_perf_counter() - measurements_start
    print(
        f"[cloud_sync] pull preflight: remote measurements fetched "
        f"count={len(remote_measurements or [])} duration={measurements_elapsed * 1000:.0f}ms",
        flush=True,
    )
    print(
        f"[cloud_sync] pull preflight: complete candidates={total} "
        f"duration={(_cloud_sync_perf_counter() - pull_preflight_start) * 1000:.0f}ms",
        flush=True,
    )

    for i, (remote, local_obs, stored_snapshot) in enumerate(candidates):
        cloud_id = str(remote.get('id') or '').strip()
        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                remote,
                f"Checking cloud observation {i + 1}/{max(1, total)}…",
            ),
            progress_state,
        )

        try:
            # DO NOT filter by should_pull_cloud_image_to_desktop here, otherwise the 
            # conflict logic falsely thinks microscope images were deleted by the cloud!
            remote_images = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])]
            remote_measurements = [
                dict(row or {})
                for row in remote_measurements_by_obs.get(cloud_id, [])
            ]

            if local_obs is None:
                local_id = _create_local_from_remote(
                    remote,
                    progress_cb=progress_cb,
                    progress_state=progress_state,
                    remote_index=i + 1,
                    remote_total=total,
                    remote_images=remote_images,
                    client=client,
                    remote_measurements=remote_measurements,
                    materialize_remote_images=materialize_remote_images,
                )
                if cloud_id:
                    client.set_desktop_id(cloud_id, local_id)
                    # The prefetched remote rows are mutated in-place when
                    # cloud desktop IDs are written back, so the snapshot can
                    # reuse them without another fetch.
                    _store_remote_snapshot(
                        client,
                        cloud_id,
                        remote=remote,
                        remote_images=remote_images,
                        remote_measurements=remote_measurements,
                    )
                _refresh_local_cloud_media_signature(local_id)
                pulled += 1
                imported_local_ids.append(int(local_id))
            else:
                local_id = int(local_obs['id'])
                if cloud_id and int(remote.get('desktop_id') or 0) != local_id:
                    try:
                        client.set_desktop_id(cloud_id, local_id)
                    except Exception as exc:
                        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                            raise
                local_dirty = str(local_obs.get('sync_status') or '').strip().lower() == 'dirty'
                if local_dirty and cloud_id and _clear_observation_dirty_if_no_real_changes(local_id, cloud_id):
                    local_obs = ObservationDB.get_observation(local_id) or local_obs
                    local_dirty = False
                remote_images_raw = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])] if cloud_id else []
                _record_remote_image_tombstones(
                    remote_images_raw,
                    local_observation_id=local_id,
                    cloud_observation_id=cloud_id,
                )
                tombstoned_remote_image_keys = _deleted_remote_image_identity_keys(remote_images_raw)
                remote_images = [
                    dict(row or {})
                    for row in remote_images_raw
                    if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
                ]
                remote_changed = (not stored_snapshot) or _remote_snapshot_has_meaningful_changes(
                    remote,
                    remote_images,
                    remote_measurements,
                    stored_snapshot,
                )
                should_store_snapshot = True
                local_media_changed = False
                if remote_changed and not stored_snapshot:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Applying cloud changes to local observation {local_id}…",
                        ),
                        progress_state,
                    )
                    _apply_remote_observation_fields(local_id, remote)
                    warnings = _apply_remote_images_to_local(
                        client,
                        local_id,
                        remote_images,
                        allow_delete=False,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(warnings)
                    measurement_result = _import_remote_measurements_for_observation(
                        client,
                        local_id,
                        cloud_id,
                        remote_images,
                        remote_measurements,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(measurement_result.get('warnings') or [])
                    if measurement_result.get('conflict'):
                        _set_observation_sync_state(local_id, cloud_id, dirty=True)
                    else:
                        _stamp_observation_synced(local_id, cloud_id)
                    _refresh_local_cloud_media_signature(local_id)
                    pulled += 1
                elif remote_changed:
                    snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                    baseline_obs = _baseline_observation_compare_payload(
                        snapshot_data.get('observation') or {}
                    )
                    baseline_images = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
                    field_changes = _analyze_observation_field_changes(local_obs, remote, baseline_obs)
                    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
                    remote_image_changes = _analyze_image_changes(
                        remote_image_payloads,
                        baseline_images,
                        ignored_keys=tombstoned_remote_image_keys,
                    )
                    remote_raw_map = {_image_compare_key(row): row for row in remote_images}
                    stored_local_media_signature = _load_local_cloud_media_signature(local_id)
                    current_local_media_signature = _local_cloud_media_signature(local_id)
                    local_media_changed = bool(
                        stored_local_media_signature
                        and current_local_media_signature
                        and not _local_media_signatures_match(
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    )
                    if not local_media_changed:
                        _store_local_media_signature_if_equivalent(
                            local_id,
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    if remote_image_changes.get('removed_keys'):
                        errors.append(
                            _format_review_needed_error(
                                local_id,
                                cloud_id,
                                ['cloud removed local image files'],
                            )
                        )
                        should_store_snapshot = False
                        continue
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Applying cloud changes to local observation {local_id}…",
                        ),
                        progress_state,
                    )
                    remote_only_fields = {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('remote_only_fields') or [])
                    }
                    conflict_fields = {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('conflict_fields') or [])
                    }
                    if remote_only_fields:
                        _apply_remote_observation_fields(
                            local_id,
                            remote,
                            fields=remote_only_fields,
                        )
                    if conflict_fields:
                        errors.append(
                            _format_review_needed_error(
                                local_id,
                                cloud_id,
                                [
                                    ', '.join(
                                        _format_observation_metadata_field_label(field)
                                        for field in sorted(conflict_fields)
                                    )
                                ],
                            )
                        )

                    if remote_image_changes.get('changed'):
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            remote_images,
                            allow_delete=False,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(warnings)
                    elif remote_image_changes.get('added_keys'):
                        added_remote_images = [
                            remote_raw_map[key]
                            for key in remote_image_changes.get('added_keys') or []
                            if key in remote_raw_map
                        ]
                        if added_remote_images:
                            warnings = _apply_remote_images_to_local(
                                client,
                                local_id,
                                added_remote_images,
                                allow_delete=False,
                                materialize_remote_images=materialize_remote_images,
                            )
                            errors.extend(warnings)
                    measurement_result = _import_remote_measurements_for_observation(
                        client,
                        local_id,
                        cloud_id,
                        remote_images,
                        remote_measurements,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(measurement_result.get('warnings') or [])
                    remaining_local_changes = _remaining_local_changes_after_remote_merge(
                        field_changes,
                        local_media_changed=local_media_changed,
                    ) or bool(measurement_result.get('conflict'))
                    should_store_snapshot = should_store_snapshot and not bool(conflict_fields)
                    _set_observation_sync_state(local_id, cloud_id, dirty=remaining_local_changes)
                    if not local_media_changed:
                        _refresh_local_cloud_media_signature(local_id)
                    pulled += 1
                # Metadata-only pulls intentionally skip the retry pass because
                # that branch exists solely to re-materialize missing cloud media.
                if stored_snapshot and materialize_remote_images:
                    retry_remote_images = _remote_images_missing_locally(local_id, remote_images)
                    if retry_remote_images:
                        profiler = _cloud_sync_current_profiler()
                        if profiler is not None:
                            try:
                                profiler.record_retry_missing_cloud_media_branch()
                            except Exception:
                                pass
                        _emit_progress(
                            progress_cb,
                            _format_cloud_sync_observation_status(
                                remote,
                                f"Retrying missing cloud media for local observation {local_id}…",
                            ),
                            progress_state,
                        )
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            retry_remote_images,
                            allow_delete=False,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(warnings)
                        measurement_result = _import_remote_measurements_for_observation(
                            client,
                            local_id,
                            cloud_id,
                            remote_images,
                            remote_measurements,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(measurement_result.get('warnings') or [])
                        if measurement_result.get('conflict'):
                            _set_observation_sync_state(local_id, cloud_id, dirty=True)
                        else:
                            _stamp_observation_synced(local_id, cloud_id)
                        if not local_media_changed:
                            _refresh_local_cloud_media_signature(local_id)
                        if not remote_changed:
                            pulled += 1
                if cloud_id and should_store_snapshot:
                    _store_remote_snapshot(
                        client,
                        cloud_id,
                        remote=remote,
                        remote_images=remote_images,
                        remote_measurements=remote_measurements,
                    )
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            errors.append(f"cloud {remote.get('id')}: {e}")
        finally:
            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    remote,
                    f"Processed cloud observation {i + 1}/{max(1, total)}",
                ),
                progress_state,
            )

    updates = {'cloud_last_pull_at': datetime.now(timezone.utc).isoformat()}
    if imported_local_ids:
        updates['cloud_recent_import_local_ids'] = json.dumps(imported_local_ids)
    update_app_settings(updates)
    deleted_remote = _detect_deleted_remote_observations(remote_obs)
    _increment_sync_summary(_cloud_sync_current_summary(), 'observations_deleted_remote', len(deleted_remote))
    return {
        'pulled': pulled,
        'total': total,
        'calibrations_pulled': calibration_result.get('pulled', 0),
        'calibrations_total': calibration_result.get('total', 0),
        'errors': errors,
        'deleted_remote': deleted_remote,
        'sync_summary': dict(_cloud_sync_current_summary() or {}),
    }


def _create_local_from_remote(
    remote: dict,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
    client: SporelyCloudClient | None = None,
    remote_measurements: list[dict] | None = None,
    materialize_remote_images: bool = True,
) -> int:
    """Insert a cloud observation into local SQLite. Returns new local ID."""
    raw_location_public = remote.get('location_public')
    location_public = _normalize_observation_bool_value(raw_location_public, default=None)
    sharing_scope = _cloud_visibility_to_sharing_scope(
        remote.get('visibility') or remote.get('sharing_scope'),
        fallback='friends' if location_public else 'private',
    )
    raw_spore_vis = str(remote.get('spore_data_visibility') or 'public').strip().lower()
    spore_data_visibility = raw_spore_vis if raw_spore_vis in {'private', 'friends', 'public'} else 'public'
    raw_publish_target = str(remote.get('publish_target') or '').strip()

    # Map cloud columns to create_observation kwargs
    remote_captured_at = str(remote.get('captured_at') or '').strip()
    kwargs = dict(
        date=remote_captured_at or remote.get('date') or datetime.now().strftime('%Y-%m-%d'),
        genus=remote.get('genus'),
        species=remote.get('species'),
        common_name=remote.get('common_name'),
        species_guess=remote.get('species_guess'),
        location=remote.get('location'),
        habitat=remote.get('habitat'),
        notes=remote.get('notes'),
        open_comment=remote.get('open_comment'),
        sharing_scope=sharing_scope,
        location_public=location_public,
        spore_data_visibility=spore_data_visibility,
        uncertain=_normalize_observation_bool_value(remote.get('uncertain'), default=False),
        unspontaneous=_normalize_observation_bool_value(remote.get('unspontaneous'), default=False),
        gps_latitude=_normalize_observation_float_value(remote.get('gps_latitude')),
        gps_longitude=_normalize_observation_float_value(remote.get('gps_longitude')),
        is_draft=_normalize_observation_bool_value(remote.get('is_draft'), default=True),
        location_precision=ObservationDB._normalize_location_precision(remote.get('location_precision')),
        ai_selected_service=remote.get('ai_selected_service'),
        ai_selected_taxon_id=remote.get('ai_selected_taxon_id'),
        ai_selected_scientific_name=remote.get('ai_selected_scientific_name'),
        ai_selected_probability=_normalize_observation_float_value(remote.get('ai_selected_probability')),
        ai_selected_at=remote.get('ai_selected_at'),
        source_type=remote.get('source_type') or 'personal',
        author=remote.get('author'),
        habitat_nin2_path=remote.get('habitat_nin2_path'),
        habitat_substrate_path=remote.get('habitat_substrate_path'),
        habitat_host_genus=remote.get('habitat_host_genus'),
        habitat_host_species=remote.get('habitat_host_species'),
        habitat_host_common_name=remote.get('habitat_host_common_name'),
        habitat_nin2_note=remote.get('habitat_nin2_note'),
        habitat_substrate_note=remote.get('habitat_substrate_note'),
        habitat_grows_on_note=remote.get('habitat_grows_on_note'),
        publish_target=normalize_publish_target(raw_publish_target) if raw_publish_target else None,
        interesting_comment=_normalize_observation_bool_value(remote.get('interesting_comment'), default=False),
    )
    local_id = ObservationDB.create_observation(**kwargs)

    # Stamp the cloud_id and sync_status on the newly created row
    conn = get_connection()
    cursor = conn.cursor()
    update_observation_sync_state(
        cursor,
        int(local_id),
        cloud_id=remote['id'],
        sync_status='synced',
        synced_at=datetime.now(timezone.utc).isoformat(),
        clear_sync_error_state=True,
    )
    conn.commit()
    conn.close()

    cloud_id = str(remote.get('id') or '').strip()
    if cloud_id:
        _import_remote_images(
            remote,
            local_id,
            cloud_id,
            progress_cb=progress_cb,
            progress_state=progress_state,
            remote_index=remote_index,
            remote_total=remote_total,
            remote_images=remote_images,
            materialize_remote_images=materialize_remote_images,
        )
        measurement_result = _import_remote_measurements_for_observation(
            client,
            local_id,
            cloud_id,
            remote_images=remote_images,
            remote_measurements=remote_measurements,
            materialize_remote_images=materialize_remote_images,
        )
        if measurement_result.get('warnings'):
            for warning in measurement_result['warnings']:
                print(f'[cloud_sync] Observation {local_id}: {warning}')

    return local_id


def _import_remote_images(
    remote: dict,
    local_id: int,
    cloud_id: str,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
    materialize_remote_images: bool = True,
) -> None:
    """Download and create local image rows for a newly pulled cloud observation."""
    if not materialize_remote_images:
        return
    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        return
    
    remote_images_raw = (
        [dict(row or {}) for row in remote_images]
        if remote_images is not None
        else [dict(row or {}) for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])]
    )
    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=local_id,
        cloud_observation_id=cloud_id,
    )

    # Keep only active rows for the existing image import path.
    images_to_pull = [
        dict(row or {})
        for row in remote_images_raw
        if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
    ]
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in images_to_pull if str(row.get('id') or '').strip()]
    )
    
    if not images_to_pull:
        return
        
    _extend_progress_total(progress_state, len(images_to_pull))
    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
    synced_at = datetime.now(timezone.utc).isoformat()
    try:
        for idx, image_row in enumerate(images_to_pull, start=1):
            try:
                cloud_image_id = str(image_row.get('id') or '').strip()
                if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                    warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
                    print(f'[cloud_sync] Warning: {warning}')
                    continue
                if remote_index and remote_total:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Importing cloud image {idx}/{len(images_to_pull)}…",
                        ),
                        progress_state,
                    )
                
                storage_path = _normalize_cloud_media_key(image_row.get('storage_path'))
                if not storage_path: continue

                image_temp_dir = temp_dir / (str(image_row.get('id') or idx).strip() or str(idx))
                image_temp_dir.mkdir(parents=True, exist_ok=True)
                download_path = image_temp_dir / (Path(str(image_row.get('original_filename') or '')).name or 'img.jpg')
                client.download_image_file(storage_path, download_path)
                download_path = _rename_to_detected_image_extension(download_path)
                new_image_type = str(image_row.get('image_type') or 'field').strip().lower()

                local_image_id = ImageDB.add_image(
                    observation_id=int(local_id),
                    filepath=str(download_path),
                    image_type=str(image_row.get('image_type') or 'field'),
                    scale=image_row.get('scale_microns_per_pixel'),
                    notes=image_row.get('notes'),
                    micro_category=image_row.get('micro_category'),
                    objective_name=image_row.get('objective_name'),
                    measure_color=image_row.get('measure_color'),
                    mount_medium=image_row.get('mount_medium'),
                    stain=image_row.get('stain'),
                    sample_type=image_row.get('sample_type'),
                    contrast=image_row.get('contrast'),
                    sort_order=image_row.get('sort_order'),
                    crop_mode=image_row.get('crop_mode'),
                    gps_source=image_row.get('gps_source'),
                    resample_scale_factor=image_row.get('resample_scale_factor'),
                    calibration_id=_local_calibration_id_for_image(image_row),
                    ai_crop_box=_remote_ai_crop_box(image_row),
                    ai_crop_source_size=_remote_ai_crop_source_size(image_row),
                    ai_crop_is_custom=_remote_ai_crop_is_custom(image_row),
                    captured_at=image_row.get('captured_at'),
                    copy_to_folder=True,
                    mark_observation_dirty=False,
                    source_role='cloud_recovery_cache',
                    file_purpose='cache',
                    original_mime_type=None,
                    working_mime_type=guess_local_image_mime_type(download_path),
                )
                cloud_image_id = str(image_row.get('id') or '').strip()

                # Update sync metadata
                conn = get_connection()
                try:
                    conn.execute('UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?', 
                                 (cloud_image_id or None, synced_at, int(local_image_id)))
                    conn.commit()
                finally:
                    conn.close()

                set_image_desktop_id = getattr(client, 'set_image_desktop_id', None)
                if cloud_image_id and callable(set_image_desktop_id):
                    try:
                        set_image_desktop_id(cloud_image_id, int(local_image_id))
                        image_row['desktop_id'] = int(local_image_id)
                    except Exception as exc:
                        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                            raise

                # Generate thumbnails and signature
                _profile_generate_all_sizes(str(download_path), int(local_image_id))
                file_sig = _file_content_signature(download_path)
                if file_sig:
                    _store_cloud_image_file_signature(local_id, local_image_id, file_sig)
                _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')

            except Exception as e:
                if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                    raise
                print(f'[cloud_sync] Failed image import: {e}')
            finally:
                _advance_progress(progress_state, 1)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _import_remote_measurements_for_observation(
    client: SporelyCloudClient | None,
    local_id: int,
    cloud_id: str,
    remote_images: list[dict] | None = None,
    remote_measurements: list[dict] | None = None,
    materialize_remote_images: bool = True,
) -> dict:
    warnings: list[str] = []
    if not str(cloud_id or '').strip():
        return {'warnings': warnings, 'conflict': False, 'imported': 0}
    if client is None:
        client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        return {'warnings': warnings, 'conflict': False, 'imported': 0}

    remote_images_raw = (
        [dict(row or {}) for row in remote_images]
        if remote_images is not None
        else [dict(row or {}) for row in (client.pull_image_metadata(cloud_id) or [])]
    )
    remote_image_lookup = {
        str(row.get('id') or '').strip(): row
        for row in remote_images_raw
        if str(row.get('id') or '').strip()
    }
    tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(remote_image_lookup.keys())
    measurement_rows_source = (
        [dict(row or {}) for row in (remote_measurements or [])]
        if remote_measurements is not None
        else _pull_remote_measurements_for_images(client, list(remote_image_lookup.keys()))
    )
    remote_measurements_by_obs = _group_remote_measurements_by_observation(remote_images_raw, measurement_rows_source)
    measurement_rows = [dict(row or {}) for row in remote_measurements_by_obs.get(str(cloud_id), [])]
    if not measurement_rows:
        return {'warnings': warnings, 'conflict': False, 'imported': 0}

    def _load_local_images() -> tuple[dict[str, dict], dict[int, dict]]:
        local_images = ImageDB.get_images_for_observation(int(local_id))
        by_cloud_id: dict[str, dict] = {}
        by_local_id: dict[int, dict] = {}
        for image_row in local_images or []:
            local_image_id = _safe_int(image_row.get('id'))
            if local_image_id > 0:
                by_local_id[local_image_id] = dict(image_row or {})
            cloud_image_id = str(image_row.get('cloud_id') or '').strip()
            if cloud_image_id:
                by_cloud_id[cloud_image_id] = dict(image_row or {})
        return by_cloud_id, by_local_id

    def _measurement_write_values(remote_row: dict, local_image_id: int) -> dict:
        return {
            'image_id': int(local_image_id),
            'length_um': remote_row.get('length_um'),
            'width_um': remote_row.get('width_um'),
            'measurement_type': _normalize_measurement_type_value(remote_row.get('measurement_type')),
            'gallery_rotation': _safe_int(remote_row.get('gallery_rotation')),
            'p1_x': remote_row.get('p1_x'),
            'p1_y': remote_row.get('p1_y'),
            'p2_x': remote_row.get('p2_x'),
            'p2_y': remote_row.get('p2_y'),
            'p3_x': remote_row.get('p3_x'),
            'p3_y': remote_row.get('p3_y'),
            'p4_x': remote_row.get('p4_x'),
            'p4_y': remote_row.get('p4_y'),
            'measured_at': (
                str(remote_row.get('measured_at') or '').strip()
                or datetime.now(timezone.utc).isoformat()
            ),
        }

    local_images_by_cloud_id, local_images_by_id = _load_local_images()
    local_measurements_by_cloud_id, local_measurements_by_id = _load_local_measurement_lookup(int(local_id))
    set_measurement_desktop_id = getattr(client, 'set_measurement_desktop_id', None)
    imported = 0
    conflict = False

    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        for remote_row in measurement_rows:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue

            remote_image_id = str(remote_row.get('image_id') or '').strip()
            remote_image = remote_image_lookup.get(remote_image_id)
            if not remote_image:
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because cloud image {remote_image_id or '?'} is unavailable"
                )
                continue
            if not should_pull_cloud_image_to_desktop(remote_image):
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"on excluded image {remote_image_id or '?'}"
                )
                continue
            if remote_image_id in tombstoned_remote_image_ids:
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because cloud image {remote_image_id} has a local tombstone"
                )
                continue

            local_image = local_images_by_cloud_id.get(remote_image_id)
            if local_image is None:
                if not materialize_remote_images:
                    # Measurement import for cloud images without local media is deferred
                    # until those images are materialized on this device.
                    continue
                warnings.extend(
                    _apply_remote_images_to_local(
                        client,
                        int(local_id),
                        [remote_image],
                        allow_delete=False,
                        materialize_remote_images=materialize_remote_images,
                    )
                )
                local_images_by_cloud_id, local_images_by_id = _load_local_images()
                local_image = local_images_by_cloud_id.get(remote_image_id)
            if local_image is None:
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because image {remote_image_id or '?'} could not be materialized"
                )
                continue

            remote_image_type = str(remote_image.get('image_type') or '').strip().lower()
            local_image_type = str(local_image.get('image_type') or '').strip().lower()
            if remote_image_type == 'microscope' or local_image_type == 'microscope':
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"on excluded image {remote_image_id or '?'}"
                )
                continue

            local_image_id = _safe_int(local_image.get('id'))
            if local_image_id <= 0:
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because the local image anchor is missing"
                )
                continue

            local_measurement = local_measurements_by_cloud_id.get(remote_measurement_id)
            if local_measurement is None:
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                if remote_desktop_measurement_id > 0:
                    local_measurement = local_measurements_by_id.get(remote_desktop_measurement_id)

            remote_payload = _measurement_compare_payload(remote_row, local=False)
            if local_measurement is None:
                write_values = _measurement_write_values(remote_row, local_image_id)
                cursor.execute(
                    '''
                    INSERT INTO spore_measurements (
                        image_id, length_um, width_um, measurement_type, gallery_rotation,
                        p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y,
                        measured_at, cloud_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        write_values['image_id'],
                        write_values['length_um'],
                        write_values['width_um'],
                        write_values['measurement_type'],
                        write_values['gallery_rotation'],
                        write_values['p1_x'],
                        write_values['p1_y'],
                        write_values['p2_x'],
                        write_values['p2_y'],
                        write_values['p3_x'],
                        write_values['p3_y'],
                        write_values['p4_x'],
                        write_values['p4_y'],
                        write_values['measured_at'],
                        remote_measurement_id,
                    ),
                )
                new_local_measurement_id = _safe_int(cursor.lastrowid)
                imported += 1
                if callable(set_measurement_desktop_id):
                    remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                    if remote_desktop_measurement_id != new_local_measurement_id:
                        try:
                            set_measurement_desktop_id(remote_measurement_id, new_local_measurement_id)
                            remote_row['desktop_id'] = new_local_measurement_id
                        except Exception as exc:
                            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                                raise
                local_measurements_by_cloud_id[remote_measurement_id] = {
                    'id': new_local_measurement_id,
                    'cloud_id': remote_measurement_id,
                    'image_id': local_image_id,
                    'image_cloud_id': str(local_image.get('cloud_id') or '').strip() or None,
                    **write_values,
                }
                if new_local_measurement_id > 0:
                    local_measurements_by_id[new_local_measurement_id] = dict(local_measurements_by_cloud_id[remote_measurement_id])
                continue

            local_payload = _measurement_compare_payload(local_measurement, local=True)
            for identity_key in ('id', 'desktop_id'):
                local_payload.pop(identity_key, None)
                remote_payload.pop(identity_key, None)
            if local_payload != remote_payload:
                conflict = True
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because the local copy changed"
                )
                continue

            write_values = _measurement_write_values(remote_row, local_image_id)
            cursor.execute(
                '''
                UPDATE spore_measurements
                SET image_id = ?,
                    length_um = ?,
                    width_um = ?,
                    measurement_type = ?,
                    gallery_rotation = ?,
                    p1_x = ?,
                    p1_y = ?,
                    p2_x = ?,
                    p2_y = ?,
                    p3_x = ?,
                    p3_y = ?,
                    p4_x = ?,
                    p4_y = ?,
                    measured_at = ?,
                    cloud_id = ?
                WHERE id = ?
                ''',
                (
                    write_values['image_id'],
                    write_values['length_um'],
                    write_values['width_um'],
                    write_values['measurement_type'],
                    write_values['gallery_rotation'],
                    write_values['p1_x'],
                    write_values['p1_y'],
                    write_values['p2_x'],
                    write_values['p2_y'],
                    write_values['p3_x'],
                    write_values['p3_y'],
                    write_values['p4_x'],
                    write_values['p4_y'],
                    write_values['measured_at'],
                    remote_measurement_id,
                    _safe_int(local_measurement.get('id')),
                ),
            )
            imported += 1
            if callable(set_measurement_desktop_id):
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                local_measurement_id = _safe_int(local_measurement.get('id'))
                if remote_desktop_measurement_id != local_measurement_id:
                    try:
                        set_measurement_desktop_id(remote_measurement_id, local_measurement_id)
                        remote_row['desktop_id'] = local_measurement_id
                    except Exception as exc:
                        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                            raise
    finally:
        conn.commit()
        conn.close()

    return {
        'warnings': warnings,
        'conflict': conflict,
        'imported': imported,
    }


def materialize_cloud_media_for_observation(
    client: SporelyCloudClient | None,
    local_observation_id: int | str,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    summary = {
        'status': 'skipped',
        'reason': None,
        'local_observation_id': _safe_int(local_observation_id),
        'cloud_observation_id': None,
        'remote_images_considered': 0,
        'skipped_already_materialized': 0,
        'downloaded': 0,
        'failed': 0,
        'measurements_imported': 0,
        'warnings': [],
        'errors': [],
        'used_snapshot_data': False,
        'used_live_fallback': False,
    }

    profiler = _cloud_sync_current_profiler()
    owns_profiler = False
    profile_token = None
    if profiler is None and _cloud_sync_profile_enabled():
        profiler = CloudSyncProfiler()
        owns_profiler = True
        try:
            profile_token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
        except Exception:
            profile_token = None

    def _finish(result_summary: dict) -> dict:
        if owns_profiler and profiler is not None:
            try:
                profiler.finish(result=result_summary)
            except Exception:
                pass
        return result_summary

    local_id = _safe_int(local_observation_id)
    if local_id <= 0:
        summary['reason'] = 'invalid_local_observation_id'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    local_obs = ObservationDB.get_observation(local_id)
    if not local_obs:
        summary['reason'] = 'local_observation_not_found'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    summary['cloud_observation_id'] = cloud_id or None
    if not cloud_id:
        summary['reason'] = 'no_cloud_snapshot'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    if client is None:
        client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        summary['status'] = 'error'
        summary['errors'].append('Could not load Sporely Cloud credentials.')
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    snapshot_data = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_id))
    snapshot_has_media = 'images' in snapshot_data and 'measurements' in snapshot_data

    remote = dict(snapshot_data.get('observation') or local_obs or {'id': cloud_id})
    remote_images_raw: list[dict] = []
    remote_measurements_source: list[dict] = []

    if snapshot_has_media:
        summary['used_snapshot_data'] = True
        remote_images_raw = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
        remote_measurements_source = [dict(row or {}) for row in (snapshot_data.get('measurements') or [])]
    else:
        summary['used_live_fallback'] = True
        get_observation = getattr(client, 'get_observation', None)
        if callable(get_observation):
            try:
                live_remote = get_observation(cloud_id)
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['warnings'].append(
                    f'obs {local_id}: could not fetch live cloud observation metadata: {exc}'
                )
            else:
                if isinstance(live_remote, dict) and live_remote:
                    remote = dict(live_remote)
        try:
            remote_images_raw = [
                dict(row or {})
                for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])
            ]
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['errors'].append(f'obs {local_id}: could not fetch cloud image metadata: {exc}')
            remote_images_raw = []
        image_cloud_ids = [
            str(row.get('id') or '').strip()
            for row in remote_images_raw
            if str(row.get('id') or '').strip()
        ]
        try:
            remote_measurements_source = _pull_remote_measurements_for_images(client, image_cloud_ids)
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['errors'].append(f'obs {local_id}: could not fetch cloud measurements: {exc}')
            remote_measurements_source = []

    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=local_id,
        cloud_observation_id=cloud_id,
    )

    active_remote_images = [
        row
        for row in remote_images_raw
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
        and str(row.get('id') or '').strip()
    ]
    active_remote_images.sort(
        key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))
    )
    summary['remote_images_considered'] = len(active_remote_images)

    remote_measurements_by_obs = _group_remote_measurements_by_observation(
        remote_images_raw,
        remote_measurements_source,
    )
    remote_measurements = [dict(row or {}) for row in remote_measurements_by_obs.get(cloud_id, [])]

    local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
    tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in active_remote_images]
    )

    progress_state = {'done': 0, 'total': 0}
    if active_remote_images:
        _extend_progress_total(progress_state, len(active_remote_images))

    observation_folder = str(local_obs.get('folder_path') or '').strip()
    if observation_folder:
        base_folder = Path(observation_folder)
    else:
        try:
            base_folder = ObservationDB._build_observation_folder_path(
                local_obs.get('genus'),
                local_obs.get('species'),
                local_obs.get('date'),
            )
        except Exception:
            base_folder = get_images_dir() / f'observation_{local_id}'

    def _local_image_asset_path(local_image: dict | None) -> Path | None:
        if not local_image:
            return None
        return _resolve_existing_local_image_asset_path(local_image.get('filepath'))

    def _fallback_local_image_path(remote_image: dict) -> Path:
        filename = Path(str(remote_image.get('original_filename') or '')).name
        if not filename:
            filename = f"{str(remote_image.get('id') or local_id).strip() or local_id}.jpg"
        return base_folder / filename

    def _ensure_local_cloud_link(local_image: dict, remote_image: dict) -> None:
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id:
            return
        local_image_id = _safe_int(local_image.get('id'))
        if local_image_id <= 0:
            return

        current_cloud_id = str(local_image.get('cloud_id') or '').strip()
        current_remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
        if current_cloud_id != cloud_image_id:
            conn = get_connection()
            try:
                conn.execute(
                    'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                    (cloud_image_id, datetime.now(timezone.utc).isoformat(), local_image_id),
                )
                conn.commit()
                local_image['cloud_id'] = cloud_image_id
            finally:
                conn.close()

        if current_remote_desktop_id != local_image_id:
            set_image_desktop_id = getattr(client, 'set_image_desktop_id', None)
            if callable(set_image_desktop_id):
                try:
                    set_image_desktop_id(cloud_image_id, local_image_id)
                    remote_image['desktop_id'] = local_image_id
                except Exception as exc:
                    if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                        raise
                    summary['warnings'].append(
                        f'obs {local_id}: could not link cloud image {cloud_image_id} to local image {local_image_id}: {exc}'
                    )

    for idx, remote_image in enumerate(active_remote_images, start=1):
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id:
            continue
        if cloud_image_id in tombstoned_remote_image_ids:
            warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
            summary['warnings'].append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue

        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                remote,
                f"Materializing cloud image {idx}/{len(active_remote_images)}: {cloud_image_id}…",
            ),
            progress_state,
        )

        local_image = local_images_by_cloud_id.get(cloud_image_id)
        if local_image is None:
            remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
            if remote_desktop_id > 0:
                local_image = local_images_by_id.get(remote_desktop_id)

        if local_image is not None:
            existing_asset_path = _local_image_asset_path(local_image)
            if existing_asset_path is not None:
                summary['skipped_already_materialized'] += 1
                _ensure_local_cloud_link(local_image, remote_image)
                _advance_progress(progress_state, 1)
                continue

            repair_local_image = dict(local_image)
            if not str(repair_local_image.get('filepath') or '').strip():
                repair_local_image['filepath'] = str(_fallback_local_image_path(remote_image))
            try:
                _sync_existing_remote_image_to_local(
                    client,
                    repair_local_image,
                    remote_image,
                    materialize_remote_images=True,
                )
            except CloudSyncError as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['failed'] += 1
                summary['errors'].append(
                    f'obs {local_id}: could not repair cloud image {cloud_image_id}: {exc}'
                )
                _advance_progress(progress_state, 1)
                continue
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['failed'] += 1
                summary['errors'].append(
                    f'obs {local_id}: could not repair cloud image {cloud_image_id}: {exc}'
                )
                _advance_progress(progress_state, 1)
                continue

            summary['downloaded'] += 1
            _ensure_local_cloud_link(repair_local_image, remote_image)
            local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
            _advance_progress(progress_state, 1)
            continue

        try:
            warnings = _apply_remote_images_to_local(
                client,
                local_id,
                [remote_image],
                allow_delete=False,
                materialize_remote_images=True,
            )
            if warnings:
                summary['warnings'].extend(warnings)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: could not materialize cloud image {cloud_image_id}: {exc}'
            )
            _advance_progress(progress_state, 1)
            continue
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: could not materialize cloud image {cloud_image_id}: {exc}'
            )
            _advance_progress(progress_state, 1)
            continue

        local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
        created_local_image = local_images_by_cloud_id.get(cloud_image_id)
        if created_local_image is None:
            remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
            if remote_desktop_id > 0:
                created_local_image = local_images_by_id.get(remote_desktop_id)
        if created_local_image is None or _local_image_asset_path(created_local_image) is None:
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: failed to materialize cloud image {cloud_image_id}'
            )
            _advance_progress(progress_state, 1)
            continue

        summary['downloaded'] += 1
        _ensure_local_cloud_link(created_local_image, remote_image)
        _advance_progress(progress_state, 1)

    measurement_result = _import_remote_measurements_for_observation(
        client,
        local_id,
        cloud_id,
        remote_images=remote_images_raw,
        remote_measurements=remote_measurements_source,
        materialize_remote_images=False,
    )
    summary['measurements_imported'] = int(measurement_result.get('imported') or 0)
    try:
        latest_local_measurements_by_cloud_id, _ = _load_local_measurement_lookup(local_id)
        for remote_row in remote_measurements_source:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue
            local_measurement = latest_local_measurements_by_cloud_id.get(remote_measurement_id)
            if not local_measurement:
                continue
            local_measurement_id = _safe_int(local_measurement.get('id'))
            if local_measurement_id > 0:
                remote_row['desktop_id'] = local_measurement_id
    except Exception:
        pass
    if measurement_result.get('warnings'):
        summary['warnings'].extend(str(warning) for warning in measurement_result.get('warnings') or [])
    if measurement_result.get('conflict'):
        summary['warnings'].append(
            f'obs {local_id}: some cloud measurements need review before they can be linked locally'
        )

    try:
        _store_remote_snapshot(
            client,
            cloud_id,
            remote=remote,
            remote_images=remote_images_raw,
            remote_measurements=remote_measurements_source,
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        summary['warnings'].append(f'obs {local_id}: could not refresh cloud snapshot: {exc}')

    try:
        _refresh_local_cloud_media_signature(local_id)
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        summary['warnings'].append(f'obs {local_id}: could not refresh local media signature: {exc}')

    if summary['failed'] > 0 or summary['errors']:
        summary['status'] = 'partial'
    else:
        summary['status'] = 'ok'
    if profile_token is not None:
        try:
            _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
        except Exception:
            pass
    return _finish(summary)


def cloud_media_materialization_state_for_observation(local_observation_id: int | str) -> dict:
    """Inspect whether a cloud-linked observation still needs media materialized locally."""
    summary = {
        'status': 'skipped',
        'reason': None,
        'local_observation_id': _safe_int(local_observation_id),
        'cloud_observation_id': None,
        'snapshot_available': False,
        'snapshot_has_media': False,
        'remote_images_considered': 0,
        'remote_measurements_considered': 0,
        'local_images_total': 0,
        'local_images_ready': 0,
        'local_images_missing_files': 0,
        'local_measurements_total': 0,
        'local_measurements_linked': 0,
        'local_measurements_missing': 0,
        'needs_materialization': False,
        'can_auto_start': False,
        'warnings': [],
    }

    local_id = _safe_int(local_observation_id)
    if local_id <= 0:
        summary['reason'] = 'invalid_local_observation_id'
        return summary

    local_obs = ObservationDB.get_observation(local_id)
    if not local_obs:
        summary['reason'] = 'local_observation_not_found'
        return summary

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    summary['cloud_observation_id'] = cloud_id or None
    if not cloud_id:
        summary['reason'] = 'no_cloud_snapshot'
        return summary

    local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
    local_measurements_by_cloud_id, local_measurements_by_id = _load_local_measurement_lookup(local_id)
    local_image_rows = list(local_images_by_id.values()) or list(local_images_by_cloud_id.values())
    summary['local_images_total'] = len(local_image_rows)
    summary['local_measurements_total'] = len(local_measurements_by_id)

    snapshot_data = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_id))
    snapshot_has_media = 'images' in snapshot_data and 'measurements' in snapshot_data
    summary['snapshot_available'] = bool(snapshot_data)
    summary['snapshot_has_media'] = snapshot_has_media

    if snapshot_has_media:
        remote_images_raw = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
        remote_measurements_source = [dict(row or {}) for row in (snapshot_data.get('measurements') or [])]
        active_remote_images = [
            row
            for row in remote_images_raw
            if should_pull_cloud_image_to_desktop(row)
            and not str(row.get('deleted_at') or '').strip()
            and str(row.get('id') or '').strip()
        ]
        active_remote_images.sort(
            key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))
        )
        summary['remote_images_considered'] = len(active_remote_images)

        remote_measurements_by_obs = _group_remote_measurements_by_observation(
            remote_images_raw,
            remote_measurements_source,
        )
        remote_measurements = [dict(row or {}) for row in remote_measurements_by_obs.get(cloud_id, [])]
        summary['remote_measurements_considered'] = len(remote_measurements)

        tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(
            [str(row.get('id') or '').strip() for row in active_remote_images]
        )
        active_remote_image_ids = {
            str(row.get('id') or '').strip()
            for row in active_remote_images
            if str(row.get('id') or '').strip() and str(row.get('id') or '').strip() not in tombstoned_remote_image_ids
        }

        for remote_image in active_remote_images:
            cloud_image_id = str(remote_image.get('id') or '').strip()
            if not cloud_image_id or cloud_image_id in tombstoned_remote_image_ids:
                continue
            local_image = local_images_by_cloud_id.get(cloud_image_id)
            if local_image is None:
                remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
                if remote_desktop_id > 0:
                    local_image = local_images_by_id.get(remote_desktop_id)
            existing_asset_path = _resolve_existing_local_image_asset_path(
                str((local_image or {}).get('filepath') or '')
            )
            if existing_asset_path is None:
                summary['local_images_missing_files'] += 1
            else:
                summary['local_images_ready'] += 1

        for remote_row in remote_measurements:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue
            local_measurement = local_measurements_by_cloud_id.get(remote_measurement_id)
            remote_image_id = str(remote_row.get('image_id') or '').strip()
            if remote_image_id not in active_remote_image_ids:
                continue
            if local_measurement is None:
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                if remote_desktop_measurement_id > 0:
                    local_measurement = local_measurements_by_id.get(remote_desktop_measurement_id)
            if local_measurement is None:
                summary['local_measurements_missing'] += 1
            else:
                summary['local_measurements_linked'] += 1

        summary['needs_materialization'] = bool(
            summary['remote_images_considered'] and (
                summary['local_images_missing_files'] > 0
                or summary['local_measurements_missing'] > 0
                or summary['local_images_ready'] < summary['remote_images_considered']
            )
        )
        summary['can_auto_start'] = bool(summary['needs_materialization'])
        summary['status'] = 'needs_materialization' if summary['needs_materialization'] else 'already_materialized'
        summary['reason'] = 'missing_local_media' if summary['needs_materialization'] else 'already_materialized'
        return summary

    # No stored snapshot media. Keep the UI conservative: if there are already
    # local files, treat the observation as materialized; otherwise let the UI
    # offer a manual retry path without auto-starting a live fetch.
    for local_image in local_image_rows:
        existing_asset_path = _resolve_existing_local_image_asset_path(
            str((local_image or {}).get('filepath') or '')
        )
        if existing_asset_path is None:
            summary['local_images_missing_files'] += 1
        else:
            summary['local_images_ready'] += 1

    if summary['local_images_total'] > 0 and summary['local_images_missing_files'] == 0:
        summary['status'] = 'already_materialized'
        summary['reason'] = 'snapshot_missing_media_but_local_images_exist'
    else:
        summary['status'] = 'needs_materialization'
        summary['reason'] = 'snapshot_missing_media'
        summary['needs_materialization'] = bool(summary['local_images_total'] == 0 or summary['local_images_missing_files'] > 0)
        summary['can_auto_start'] = False
    return summary
