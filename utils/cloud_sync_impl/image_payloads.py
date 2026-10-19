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
from utils.cloud_sync_impl.sample_source import (
    _cloud_to_desktop_sample_source,
    _split_legacy_sample_type_into_source,
)


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


def _preserve_unedited_remote_image_values(
    payload: dict,
    img: dict | None,
    remote_row: dict | None,
    *,
    captured_at_pull_injected: bool = False,
) -> None:
    """Shape an existing-row image PATCH (or its drift compare) in place.

    * A blank local text value never replaces a cloud NULL.
    * ``captured_at`` the pull injected (``captured_at_pull_injected``, from
      ``exif._captured_at_is_pull_injected``) never fills a cloud NULL; a
      capture time the user set is sent.
    * A remote-owned recovery copy (``source_role=cloud_recovery_cache`` or
      ``file_purpose=cache``) never fills ``original_filename`` from its
      local file name when the cloud row holds none.
    """
    if not remote_row:
        return
    _keep_remote_null_for_blank_text(payload, remote_row)
    if captured_at_pull_injected and 'captured_at' in payload and remote_row.get('captured_at') is None:
        payload.pop('captured_at', None)
    row = dict(img or {})
    source_role = str(row.get('source_role') or '').strip().lower()
    file_purpose = str(row.get('file_purpose') or '').strip().lower()
    if source_role != 'cloud_recovery_cache' and file_purpose != 'cache':
        return
    if 'original_filename' in payload and remote_row.get('original_filename') is None:
        payload.pop('original_filename', None)


# float8 columns the cloud returns at 15 significant digits (DBL_DIG): a value
# the desktop sent with 16-17 digits comes back as its 15-digit form. Equality
# at that precision is representation equality, not a tolerance: any real edit
# to a scale differs far above 1e-15 relative.
_IMAGE_CLOUD_FLOAT8_FIELDS = frozenset({'scale_microns_per_pixel', 'resample_scale_factor'})


def _cloud_float8_equal(a, b) -> bool:
    if a is None or b is None or isinstance(a, bool) or isinstance(b, bool):
        return a == b
    try:
        return float(f'{float(a):.15g}') == float(f'{float(b):.15g}')
    except (TypeError, ValueError):
        return a == b


def _image_value_equivalent(key: str, local, remote) -> bool:
    """Whether one image field holds the same value on both sides once the
    cloud's own representation is applied (float8 digits, NULL flag columns)."""
    if local == remote:
        return True
    if key in _IMAGE_CLOUD_FLOAT8_FIELDS:
        return _cloud_float8_equal(local, remote)
    if key in ('gps_source', 'ai_crop_is_custom'):
        # Local flag columns default to 0 where the cloud holds NULL.
        return bool(local) == bool(remote)
    return False


def _is_metadata_only_anchor_row(remote_row: dict | None) -> bool:
    row = dict(remote_row or {})
    return (
        str(row.get('image_type') or '').strip().lower() == 'microscope'
        and not _normalize_cloud_media_key(row.get('storage_path'))
    )


def _image_field_same(key: str, a, b) -> bool:
    """Exact equality, except a float8 field compared in the cloud's precision.
    The baselined three-way comparisons use this (and nothing broader)."""
    return a == b or (key in _IMAGE_CLOUD_FLOAT8_FIELDS and _cloud_float8_equal(a, b))


def _image_payloads_same(a: dict, b: dict) -> bool:
    return set(a) == set(b) and all(_image_field_same(k, a[k], b[k]) for k in a)


def _no_baseline_image_values_equivalent(
    key: str, local, remote, *, remote_row: dict | None, local_is_anchor_source: bool = False,
) -> bool:
    """The no-baseline comparison: equal after the push's own normalization.

    Beyond ``_image_value_equivalent``: a metadata-only anchor is created
    without ``captured_at`` (``anchors._METADATA_ONLY_IMG_FIELDS``), so when
    the local image is that anchor's source (bytes not desired,
    ``local_is_anchor_source``) and the cloud row has no bytes, its cloud
    NULL is a fill, not a disagreement. Every other difference stays an
    owner decision. Sample fields are compared after
    ``_apply_pushed_sample_fields`` on BOTH sides (callers do this).
    """
    if _image_value_equivalent(key, local, remote):
        return True
    return (
        key == 'captured_at' and remote is None and local is not None
        and local_is_anchor_source and _is_metadata_only_anchor_row(remote_row)
    )


def _apply_pushed_sample_fields(local_payload: dict, img: dict | None) -> None:
    """Set the sample fields of a compare payload to what the push sends:
    ``'Not_set'`` is NULL and a legacy ``sample_type='Spore_print'`` is a
    ``sample_source`` (``_apply_image_sample_fields_to_push_payload``).

    Applied to the cloud side too: a metadata-only anchor is created with the
    raw local values, so its cloud row can hold ``'Not_set'`` or the legacy
    spelling."""
    row = dict(img or {})
    condition, source = _split_legacy_sample_type_into_source(
        row.get('sample_type'), row.get('sample_source'),
    )
    if 'sample_type' in local_payload:
        local_payload['sample_type'] = _normalize_snapshot_value(condition)
    if 'sample_source' in local_payload:
        local_payload['sample_source'] = _normalize_snapshot_value(_cloud_to_desktop_sample_source(source))
