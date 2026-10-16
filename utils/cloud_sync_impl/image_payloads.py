"""Remote image compare payload.

Stateless, but outside the pure package: it maps sample source through
``sample_source``, whose ``DatabaseTerms`` import pulls in Qt.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from utils.cloud_sync_impl.common import _normalize_cloud_media_key
from utils.cloud_sync_impl.reconciliation.images import _image_identity_keys
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
    _normalize_snapshot_value,
    _keep_remote_null_for_blank_text,
    _normalize_sync_key_value,
)
from utils.cloud_sync_impl.sample_source import _cloud_to_desktop_sample_source


def _deleted_remote_image_identity_keys(remote_images: list[dict] | None) -> set[str]:
    keys: set[str] = set()
    for row in (remote_images or []):
        if not str((row or {}).get('deleted_at') or '').strip():
            continue
        keys.update(_image_identity_keys(_remote_image_payload(row)))
    return keys


def _remote_image_payload(
    remote_image: dict | None,
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    image = remote_image or {}
    payload = {
        'id': _normalize_snapshot_value(image.get('id')),
        'sync_key': _normalize_sync_key_value(image.get('sync_key')),
        'sort_order': _normalize_snapshot_value(image.get('sort_order')),
        'image_type': _normalize_snapshot_value(image.get('image_type')),
        'micro_category': _normalize_snapshot_value(image.get('micro_category')),
        'captured_at': _normalize_image_captured_at_for_cloud(
            image.get('captured_at'), local=False
        ),
        'calibration_uuid': _normalize_snapshot_value(image.get('calibration_uuid')),
        'objective_name': _normalize_snapshot_value(image.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image.get('mount_medium')),
        'stain': _normalize_snapshot_value(image.get('stain')),
        'sample_type': _normalize_snapshot_value(image.get('sample_type')),
        # Cloud canonical is lowercase snake_case (`spore_print`); desktop
        # canonical is Title_Case (`Spore_print`). Normalize inbound values
        # so downstream diff / write paths see the desktop form.
        'sample_source': _normalize_snapshot_value(
            _cloud_to_desktop_sample_source(image.get('sample_source'))
        ),
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


def _preserve_unedited_remote_image_values(payload: dict, img: dict | None, remote_row: dict | None) -> None:
    """Shape an existing-row image PATCH (or its drift compare) in place.

    * A blank local text value never replaces a cloud NULL.
    * A remote-owned recovery copy (``source_role=cloud_recovery_cache`` or
      ``file_purpose=cache``) holds bytes the cloud supplied. Facts the
      desktop derived from that local file -- its name, and the capture time
      the pull injected from the observation datetime -- are not edits and
      never fill a value the cloud row does not hold.
    """
    if not remote_row:
        return
    _keep_remote_null_for_blank_text(payload, remote_row)
    row = dict(img or {})
    source_role = str(row.get('source_role') or '').strip().lower()
    file_purpose = str(row.get('file_purpose') or '').strip().lower()
    if source_role != 'cloud_recovery_cache' and file_purpose != 'cache':
        return
    for key in ('original_filename', 'captured_at'):
        if key in payload and remote_row.get(key) is None:
            payload.pop(key, None)
