"""Pure calibration normalizers, payloads and compare helpers.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from datetime import datetime

from utils.cloud_sync_impl.reconciliation.values import _normalize_snapshot_value


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


# is_active is per-device state (which calibration the desktop currently uses).
# The cloud row still stores a value, but a mismatch on it should never emit a
# push/pull conflict — it's noise that blocks real content diffs.
_CALIBRATION_CONFLICT_IGNORED_FIELDS = frozenset({'is_active'})


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
        if field in _CALIBRATION_CONFLICT_IGNORED_FIELDS:
            continue
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


def _calibration_sync_warning(direction: str, local_row: dict | None, remote_row: dict | None, fields: list[str]) -> str:
    calibration_uuid = _normalize_calibration_uuid((local_row or remote_row or {}).get('calibration_uuid')) or '?'
    label = _calibration_display_name(local_row or remote_row)
    field_text = ', '.join(fields[:6]) if fields else 'metadata'
    return (
        f'calibration {calibration_uuid}: skipped {direction} for {label} '
        f'because the same UUID has conflicting metadata ({field_text})'
    )
