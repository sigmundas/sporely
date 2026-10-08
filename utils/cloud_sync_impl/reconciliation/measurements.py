"""Pure measurement normalizers, compare payloads and change analysis.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import math

from utils.r2_storage import media_variant_key

from utils.cloud_sync_impl.common import _normalize_cloud_media_key
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_snapshot_value,
    _parse_sync_timestamp,
    _SNAPSHOT_MEAS_FIELDS,
)


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
