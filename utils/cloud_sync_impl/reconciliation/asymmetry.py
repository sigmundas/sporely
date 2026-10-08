"""Pure accepted-asymmetry helpers carried by the snapshot baseline.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.reconciliation.measurements import (
    _normalize_measurement_float_value,
    _normalize_measurement_type_value,
)
from utils.cloud_sync_impl.reconciliation.values import _normalize_snapshot_value


# Material-content fields for the accepted-asymmetry image fingerprint.  Turn-B
# fix 3: presentation-only fields (``gallery_rotation``, ``sort_order``) and
# transport/storage/cache metadata are excluded so that a thumbnail rotation
# or gallery/image-order tweak governed by the automatic desktop presentation
# policy does NOT resurface an accepted one-sided media conflict.  Only fields
# that represent genuine user or scientific media content participate.
_ASYMMETRY_MATERIAL_IMAGE_FIELDS = (
    'image_type',
    'micro_category',
    'notes',
    'objective_name',
    'scale_microns_per_pixel',
    'mount_medium',
    'stain',
    'sample_type',
    'sample_source',
    'contrast',
    'crop_mode',
    'microscope_notes',
)


def _asymmetry_fingerprint_local_image(row: dict | None) -> dict:
    """Material content fingerprint for a local-only accepted image.

    Excludes presentation-only fields (``gallery_rotation``, ``sort_order``)
    and every transport/storage/cache/derivative field (``filepath``,
    ``storage_path``, thumbnail keys, ...).
    """
    row = dict(row or {})
    return {
        field: _normalize_snapshot_value(row.get(field))
        for field in _ASYMMETRY_MATERIAL_IMAGE_FIELDS
    }


def _asymmetry_fingerprint_remote_image(row: dict | None) -> dict:
    row = dict(row or {})
    return {
        field: _normalize_snapshot_value(row.get(field))
        for field in _ASYMMETRY_MATERIAL_IMAGE_FIELDS
    }


def _asymmetry_fingerprint_local_measurement(row: dict | None) -> dict:
    """Content fingerprint for a local-only accepted measurement."""
    row = dict(row or {})
    return {
        'length_um': _normalize_measurement_float_value(row.get('length_um')),
        'width_um': _normalize_measurement_float_value(row.get('width_um')),
        'measurement_type': _normalize_measurement_type_value(row.get('measurement_type')),
        'p1_x': _normalize_measurement_float_value(row.get('p1_x')),
        'p1_y': _normalize_measurement_float_value(row.get('p1_y')),
        'p2_x': _normalize_measurement_float_value(row.get('p2_x')),
        'p2_y': _normalize_measurement_float_value(row.get('p2_y')),
        'image_id': _safe_int(row.get('image_id')) or None,
    }


def _asymmetry_fingerprint_remote_measurement(row: dict | None) -> dict:
    row = dict(row or {})
    return {
        'length_um': _normalize_measurement_float_value(row.get('length_um')),
        'width_um': _normalize_measurement_float_value(row.get('width_um')),
        'measurement_type': _normalize_measurement_type_value(row.get('measurement_type')),
        'p1_x': _normalize_measurement_float_value(row.get('p1_x')),
        'p1_y': _normalize_measurement_float_value(row.get('p1_y')),
        'p2_x': _normalize_measurement_float_value(row.get('p2_x')),
        'p2_y': _normalize_measurement_float_value(row.get('p2_y')),
        'image_id': str(row.get('image_id') or '').strip() or None,
    }


def _accepted_asymmetry_key(entry: dict) -> tuple:
    return (
        str(entry.get('side') or ''),
        str(entry.get('kind') or ''),
        _safe_int(entry.get('local_id')) or 0,
        str(entry.get('cloud_id') or '').strip(),
    )


def _identities_referenced_by_plan(items: list[dict]) -> dict:
    """Every stable identity that this plan explicitly touches with a non-keep choice.

    Used to remove prior accepted-asymmetry entries the user is overriding.
    """
    image_identities: set[tuple[int, str]] = set()
    measurement_identities: set[tuple[int, str]] = set()
    for item in items or []:
        kind = item.get('kind')
        choice = item.get('choice')
        # ``keep_*`` choices are handled by _build_accepted_asymmetry_from_plan.
        # Everything else counts as an override that removes any prior keep-only
        # entry for the same stable identity.
        if choice in {'keep_local', 'keep_cloud'}:
            continue
        local_id = _safe_int(item.get('local_id')) or 0
        cloud_id = str(item.get('cloud_id') or '').strip()
        if kind in {'image', 'image_metadata'}:
            image_identities.add((local_id, cloud_id))
        elif kind == 'measurement':
            measurement_identities.add((local_id, cloud_id))
    return {'images': image_identities, 'measurements': measurement_identities}


def _reconcile_accepted_asymmetry(
    previous: dict | None,
    new: dict,
    *,
    plan_items: list[dict],
    current_local_images: list[dict],
    current_remote_images: list[dict],
    current_local_measurements: list[dict],
    current_remote_measurements: list[dict],
    matched_local_image_ids: set[int] | None = None,
    matched_cloud_image_ids: set[str] | None = None,
    matched_local_measurement_ids: set[int] | None = None,
    matched_cloud_measurement_ids: set[str] | None = None,
) -> dict:
    """Merge, prune, and supersede accepted-asymmetry entries.

    Removes an accepted entry when any of the following is now true:

    * its stable identity is referenced by a non-keep plan choice (user
      explicitly changed their mind to upload/download);
    * the counterpart now exists (row is no longer one-sided);
    * the accepted row no longer exists on its side (deletion / relink);
    * for measurements: the row's owning image identity changed;
    * ownership or side changed.

    Newer keep-only entries for the same identity replace older ones so the
    fingerprint stays fresh.  The result contains only entries that are still
    valid.
    """
    result = {
        'local_only_images': [],
        'cloud_only_images': [],
        'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    prev = previous if isinstance(previous, dict) else {}
    override = _identities_referenced_by_plan(plan_items or [])
    override_images = override['images']
    override_measurements = override['measurements']
    matched_local_image_ids = matched_local_image_ids or set()
    matched_cloud_image_ids = matched_cloud_image_ids or set()
    matched_local_measurement_ids = matched_local_measurement_ids or set()
    matched_cloud_measurement_ids = matched_cloud_measurement_ids or set()

    local_image_by_id = {_safe_int(r.get('id')): r for r in current_local_images or []
                         if _safe_int(r.get('id'))}
    remote_image_by_id = {str(r.get('id') or '').strip(): r for r in current_remote_images or []
                          if str(r.get('id') or '').strip()}
    local_meas_by_id = {_safe_int(r.get('id')): r for r in current_local_measurements or []
                        if _safe_int(r.get('id'))}
    remote_meas_by_id = {str(r.get('id') or '').strip(): r for r in current_remote_measurements or []
                         if str(r.get('id') or '').strip()}

    def _still_local_only_image(entry: dict) -> bool:
        local_id = _safe_int(entry.get('local_id'))
        if not local_id or local_id not in local_image_by_id:
            return False  # row is gone
        row = local_image_by_id[local_id]
        # Row now linked to a cloud row → no longer one-sided.
        if str(row.get('cloud_id') or '').strip():
            return False
        # A cloud row now claims this local id → no longer one-sided.
        if local_id in matched_local_image_ids:
            return False
        # User overrode (upload etc.) → drop.
        if (local_id, '') in override_images or any(lid == local_id for lid, _ in override_images):
            return False
        return True

    def _still_cloud_only_image(entry: dict) -> bool:
        cloud_id = str(entry.get('cloud_id') or '').strip()
        if not cloud_id or cloud_id not in remote_image_by_id:
            return False
        row = remote_image_by_id[cloud_id]
        if _safe_int(row.get('desktop_id')):
            return False  # cloud row now points at a local row
        if cloud_id in matched_cloud_image_ids:
            return False
        if any(cid == cloud_id for _, cid in override_images):
            return False
        return True

    def _still_local_only_measurement(entry: dict) -> bool:
        local_id = _safe_int(entry.get('local_id'))
        if not local_id or local_id not in local_meas_by_id:
            return False
        row = local_meas_by_id[local_id]
        if str(row.get('cloud_id') or '').strip():
            return False  # relinked
        # Owning-image identity changed → drop; fresh conflict.
        expected_owner = _safe_int(entry.get('owning_local_image_id')) or None
        current_owner = _safe_int(row.get('image_id')) or None
        if expected_owner is not None and current_owner is not None and expected_owner != current_owner:
            return False
        if local_id in matched_local_measurement_ids:
            return False
        if any(lid == local_id for lid, _ in override_measurements):
            return False
        return True

    def _still_cloud_only_measurement(entry: dict) -> bool:
        cloud_id = str(entry.get('cloud_id') or '').strip()
        if not cloud_id or cloud_id not in remote_meas_by_id:
            return False
        row = remote_meas_by_id[cloud_id]
        if _safe_int(row.get('desktop_id')):
            return False
        expected_owner = str(entry.get('owning_cloud_image_id') or '').strip() or None
        current_owner = str(row.get('image_id') or '').strip() or None
        if expected_owner and current_owner and expected_owner != current_owner:
            return False
        if cloud_id in matched_cloud_measurement_ids:
            return False
        if any(cid == cloud_id for _, cid in override_measurements):
            return False
        return True

    predicates = {
        'local_only_images': _still_local_only_image,
        'cloud_only_images': _still_cloud_only_image,
        'local_only_measurements': _still_local_only_measurement,
        'cloud_only_measurements': _still_cloud_only_measurement,
    }

    for key, predicate in predicates.items():
        seen: dict[tuple, dict] = {}
        # Prior entries first (survivors of pruning).
        for entry in prev.get(key) or []:
            if isinstance(entry, dict) and predicate(entry):
                seen[_accepted_asymmetry_key(entry)] = dict(entry)
        # New entries override / add — they were computed against fresh state
        # so they are always valid.
        for entry in new.get(key) or []:
            if isinstance(entry, dict):
                seen[_accepted_asymmetry_key(entry)] = dict(entry)
        result[key] = list(seen.values())
    return result


def _merge_accepted_asymmetry(previous: dict | None, new: dict) -> dict:
    """Preserved for ordinary sync callers that don't do full reconciliation.

    Kept as a thin wrapper for backward compatibility with helpers that store
    a snapshot without a plan context (e.g. push_all).  A plan resolution
    should call ``_reconcile_accepted_asymmetry`` directly.
    """
    result = {
        'local_only_images': [],
        'cloud_only_images': [],
        'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    prev = previous if isinstance(previous, dict) else {}
    for key in result.keys():
        seen: dict[tuple, dict] = {}
        for entry in prev.get(key) or []:
            if isinstance(entry, dict):
                seen[_accepted_asymmetry_key(entry)] = dict(entry)
        for entry in new.get(key) or []:
            if isinstance(entry, dict):
                seen[_accepted_asymmetry_key(entry)] = dict(entry)
        result[key] = list(seen.values())
    return result


def _filter_accepted_one_sided_images(
    image_pairs: list[dict],
    accepted_asymmetry: dict | None,
    local_images_raw: list[dict],
    remote_images: list[dict],
) -> tuple[list[dict], set[tuple[int, str]]]:
    """Drop one-sided image pairs whose accepted-asymmetry fingerprint is unchanged.

    * Editing the retained item (change to any fingerprint field) resurfaces it.
    * A counterpart appearing on the other side means the pair is no longer
      truly one-sided; it goes back through normal identity matching and the
      acceptance no longer suppresses the pair.
    """
    if not isinstance(accepted_asymmetry, dict):
        return image_pairs, set()
    local_only_accepted = {
        _safe_int(entry.get('local_id')): entry
        for entry in accepted_asymmetry.get('local_only_images') or []
        if isinstance(entry, dict) and _safe_int(entry.get('local_id'))
    }
    cloud_only_accepted = {
        str(entry.get('cloud_id') or '').strip(): entry
        for entry in accepted_asymmetry.get('cloud_only_images') or []
        if isinstance(entry, dict) and str(entry.get('cloud_id') or '').strip()
    }
    local_row_by_id = {_safe_int(r.get('id')): r for r in local_images_raw or []
                       if _safe_int(r.get('id'))}
    remote_row_by_id = {str(r.get('id') or '').strip(): r for r in remote_images or []
                        if str(r.get('id') or '').strip()}
    kept: list[dict] = []
    dropped_ids: set[tuple[int, str]] = set()
    for pair in image_pairs:
        status = pair.get('status')
        if status == 'local_only':
            local_id = _safe_int((pair.get('local') or {}).get('local_id'))
            accepted = local_only_accepted.get(local_id)
            live_row = local_row_by_id.get(local_id)
            if accepted and live_row is not None:
                current_fp = _asymmetry_fingerprint_local_image(live_row)
                if current_fp == accepted.get('fingerprint'):
                    dropped_ids.add((local_id, ''))
                    continue  # unchanged accepted → hide
        elif status == 'cloud_only':
            cloud_id = str((pair.get('remote') or {}).get('cloud_id') or '').strip()
            accepted = cloud_only_accepted.get(cloud_id)
            live_row = remote_row_by_id.get(cloud_id)
            if accepted and live_row is not None:
                current_fp = _asymmetry_fingerprint_remote_image(live_row)
                if current_fp == accepted.get('fingerprint'):
                    dropped_ids.add((0, cloud_id))
                    continue
        kept.append(pair)
    return kept, dropped_ids


def _filter_accepted_one_sided_measurements(
    measurement_pairs: list[dict],
    accepted_asymmetry: dict | None,
    local_measurements_raw: list[dict],
    remote_measurements: list[dict],
) -> tuple[list[dict], set[tuple[int, str]]]:
    if not isinstance(accepted_asymmetry, dict):
        return measurement_pairs, set()
    local_only_accepted = {
        _safe_int(entry.get('local_id')): entry
        for entry in accepted_asymmetry.get('local_only_measurements') or []
        if isinstance(entry, dict) and _safe_int(entry.get('local_id'))
    }
    cloud_only_accepted = {
        str(entry.get('cloud_id') or '').strip(): entry
        for entry in accepted_asymmetry.get('cloud_only_measurements') or []
        if isinstance(entry, dict) and str(entry.get('cloud_id') or '').strip()
    }
    local_row_by_id = {_safe_int(r.get('id')): r for r in local_measurements_raw or []
                       if _safe_int(r.get('id'))}
    remote_row_by_id = {str(r.get('id') or '').strip(): r for r in remote_measurements or []
                        if str(r.get('id') or '').strip()}
    kept: list[dict] = []
    dropped_ids: set[tuple[int, str]] = set()
    for pair in measurement_pairs:
        status = pair.get('status')
        if status == 'local_only':
            local_id = _safe_int(pair.get('local_id'))
            accepted = local_only_accepted.get(local_id)
            live_row = local_row_by_id.get(local_id)
            if accepted and live_row is not None:
                current_fp = _asymmetry_fingerprint_local_measurement(live_row)
                if current_fp == accepted.get('fingerprint'):
                    dropped_ids.add((local_id, ''))
                    continue
        elif status == 'cloud_only':
            cloud_id = str(pair.get('cloud_id') or '').strip()
            accepted = cloud_only_accepted.get(cloud_id)
            live_row = remote_row_by_id.get(cloud_id)
            if accepted and live_row is not None:
                current_fp = _asymmetry_fingerprint_remote_measurement(live_row)
                if current_fp == accepted.get('fingerprint'):
                    dropped_ids.add((0, cloud_id))
                    continue
        kept.append(pair)
    return kept, dropped_ids
