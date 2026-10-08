"""Pure image compare payloads and image change analysis.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json

from utils.cloud_sync_impl.reconciliation.values import _SNAPSHOT_IMG_FIELDS


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
