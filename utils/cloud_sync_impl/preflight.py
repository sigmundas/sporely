"""Observation push preflight: three-way conflict analysis against the snapshot.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction, except that ``_load_local_measurement_lookup``
sets ``sqlite3.Row`` instead of ``__import__('sqlite3').Row`` (the same object):
owners may not use dynamic import machinery.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from database.models import CalibrationDB
from database.schema import get_connection
from utils.taxon_identity import TaxonIdentity

from utils.cloud_sync_impl.baseline import (
    _load_cloud_observation_snapshot,
    _parse_cloud_observation_snapshot,
)
from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.image_payloads import (
    _deleted_remote_image_identity_keys,
    _remote_image_payload,
)
from utils.cloud_sync_impl.image_policy import should_pull_cloud_image_to_desktop
from utils.cloud_sync_impl.local_files import _resolve_existing_local_image_asset_path
from utils.cloud_sync_impl.location_precision import _guard_local_location_precision
from utils.cloud_sync_impl.media_signature import (
    _local_cloud_media_signature,
    _local_media_signatures_match,
    _store_local_media_signature_if_equivalent,
)
from utils.cloud_sync_impl.push_payloads import (
    _analyze_observation_field_changes,
    _baseline_observation_compare_payload,
    _observation_compare_payload,
)
from utils.cloud_sync_impl.reconciliation.calibrations import _normalize_calibration_uuid
from utils.cloud_sync_impl.reconciliation.identity import (
    _baseline_identity_key,
    _IDENTITY_BASELINE_UNKNOWN,
    TAXON_IDENTITY_SYNC_FIELD,
)
from utils.cloud_sync_impl.reconciliation.images import (
    _analyze_image_changes,
    _image_identity_keys,
    _image_metadata_payload,
)
from utils.cloud_sync_impl.reconciliation.measurements import (
    _analyze_measurement_changes,
    _local_measurement_snapshot_payload,
    _measurement_payloads_match,
)
from utils.cloud_sync_impl.reconciliation.report import (
    _format_observation_metadata_field_label,
    ObservationPushConflictReport,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
    _normalize_snapshot_value,
    _observation_field_values_match,
    _SNAPSHOT_OBS_FIELDS,
)
from utils.cloud_sync_impl.sync_state import _load_local_cloud_media_signature
from utils.cloud_sync_impl.tombstones import _local_tombstoned_cloud_image_ids


def _observation_push_diff_fields(local_obs: dict | None, remote_obs: dict | None) -> list[str]:
    local_obs = _guard_local_location_precision(local_obs, remote_obs)
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
        'captured_at': _normalize_image_captured_at_for_cloud(
            row.get('captured_at'), local=True
        ),
        'calibration_uuid': _normalize_snapshot_value(_image_calibration_uuid(row)),
        'objective_name': _normalize_snapshot_value(row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
        'stain': _normalize_snapshot_value(row.get('stain')),
        'sample_type': _normalize_snapshot_value(row.get('sample_type')),
        'sample_source': _normalize_snapshot_value(row.get('sample_source')),
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
        'ai_crop_is_custom': _normalize_snapshot_value(
            None if row.get('ai_crop_is_custom') is None else bool(row.get('ai_crop_is_custom'))
        ),
    }
    return payload


def _locally_tombstoned_snapshot_image_identity_keys(
    baseline_images: list[dict] | None,
) -> set[str]:
    baseline_rows = [dict(row or {}) for row in (baseline_images or [])]
    cloud_ids = [str(row.get('id') or '').strip() for row in baseline_rows]
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(cloud_ids)
    keys: set[str] = set()
    for row in baseline_rows:
        if str(row.get('id') or '').strip() in tombstoned_cloud_ids:
            keys.update(_image_identity_keys(row))
    return keys


def _analyze_observation_push_conflicts(
    *,
    local_obs: dict | None,
    local_images: list[dict] | None,
    local_measurements_by_cloud_id: dict[str, dict] | None,
    remote_obs: dict | None,
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None,
    baseline_snapshot: dict | None,
) -> ObservationPushConflictReport:
    """Detect per-observation conflicts before push_all mutates cloud state.

    The three input sides — local rows, fetched remote rows, and the stored
    sync baseline (``_load_cloud_observation_snapshot`` / parsed) — are the
    same primitives pull_all uses. Returned categories align with the review
    reasons pull_all emits so a single review dialog covers both directions:

    * ``observation`` — obs metadata field conflict
      (via :func:`_analyze_observation_field_changes` ``conflict_fields``).
    * ``images`` — a shared image was edited on both sides.
    * ``measurements`` — a shared spore measurement differs local vs remote
      (mirrors :func:`_import_remote_measurements_for_observation` ``conflict``).
    * ``remote_removed_media`` — cloud removed an image still present locally
      (mirrors pull_all's ``removed_keys`` review-needed guard).
    """
    baseline_snapshot = dict(baseline_snapshot or {})
    baseline_obs = _baseline_observation_compare_payload(
        baseline_snapshot.get('observation') or {}
    )
    baseline_images = [dict(row or {}) for row in (baseline_snapshot.get('images') or [])]

    # ---- Observation metadata: three-way conflict fields ----
    field_changes = _analyze_observation_field_changes(local_obs, remote_obs, baseline_obs)
    conflict_fields = list(field_changes.get('conflict_fields') or [])
    if local_measurements_by_cloud_id or remote_measurements:
        # Derived statistics follow the selected scientific measurement set;
        # they are never an independent winner-takes-all field conflict.
        conflict_fields = [field for field in conflict_fields if field != 'spore_statistics']
    field_labels = [
        _format_observation_metadata_field_label(field)
        for field in sorted(set(conflict_fields))
    ]

    # ---- Image removals: mirror pull_all "cloud removed local image files" ----
    remote_images = list(remote_images or [])
    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
    tombstoned_remote_image_keys = (
        _deleted_remote_image_identity_keys(remote_images)
        | _locally_tombstoned_snapshot_image_identity_keys(baseline_images)
    )
    remote_image_changes = _analyze_image_changes(
        remote_image_payloads,
        baseline_images,
        ignored_keys=tombstoned_remote_image_keys,
    )
    remote_removed_image_keys = list(remote_image_changes.get('removed_keys') or [])

    # ---- Image metadata: three-way conflict on shared cloud image rows ----
    # Normalize each side to a canonical snapshot payload before comparing so
    # that representation differences (e.g. cloud lowercase sample_source vs
    # desktop Title_Case, calibration_id vs calibration_uuid, naive local
    # captured_at vs UTC timestamptz) do not produce false conflicts.
    baseline_by_cloud_id = {
        str(row.get('id') or '').strip(): _remote_image_payload(row)
        for row in baseline_images
        if str(row.get('id') or '').strip()
    }
    remote_by_cloud_id = {
        str(row.get('id') or '').strip(): row
        for row in remote_image_payloads
        if str(row.get('id') or '').strip()
    }
    local_by_cloud_id = {
        str((img or {}).get('cloud_id') or '').strip(): _local_image_snapshot_payload(dict(img or {}))
        for img in (local_images or [])
        if str((img or {}).get('cloud_id') or '').strip()
    }
    image_conflict_keys: list[str] = []
    shared_cloud_ids = sorted(
        set(baseline_by_cloud_id) & set(remote_by_cloud_id) & set(local_by_cloud_id)
    )
    for cloud_image_id in shared_cloud_ids:
        baseline_meta = _image_metadata_payload(baseline_by_cloud_id[cloud_image_id])
        remote_meta = _image_metadata_payload(remote_by_cloud_id[cloud_image_id])
        local_meta = _image_metadata_payload(local_by_cloud_id[cloud_image_id])
        local_changed = local_meta != baseline_meta
        remote_changed = remote_meta != baseline_meta
        if local_changed and remote_changed and local_meta != remote_meta:
            image_conflict_keys.append(f'cloud:{cloud_image_id}')

    # ---- Measurements: mirror pull-side conflict detection (no apply) ----
    remote_measurements = list(remote_measurements or [])
    remote_image_lookup = {
        str(row.get('id') or '').strip(): row
        for row in remote_images
        if str(row.get('id') or '').strip()
    }
    tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(list(remote_image_lookup.keys()))
    local_measurements_by_cloud_id = dict(local_measurements_by_cloud_id or {})

    measurement_conflict_ids: list[int] = []
    seen_local_ids: set[int] = set()
    for remote_row in remote_measurements:
        remote_measurement_id = str(remote_row.get('id') or '').strip()
        if not remote_measurement_id:
            continue
        remote_image_id = str(remote_row.get('image_id') or '').strip()
        remote_image = remote_image_lookup.get(remote_image_id)
        if not remote_image:
            continue
        if not _is_spore_measurement_source_image(remote_image):
            continue
        if remote_image_id in tombstoned_remote_image_ids:
            continue
        local_measurement = local_measurements_by_cloud_id.get(remote_measurement_id)
        if local_measurement is None:
            continue
        if not _measurement_payloads_match(
            local_measurement,
            remote_row,
            cloud_image_id=remote_image_id,
        ):
            local_measurement_id = _safe_int(local_measurement.get('id'))
            if local_measurement_id > 0 and local_measurement_id not in seen_local_ids:
                seen_local_ids.add(local_measurement_id)
                measurement_conflict_ids.append(local_measurement_id)

    categories: list[str] = []
    if conflict_fields:
        categories.append('observation')
    if image_conflict_keys:
        categories.append('images')
    if measurement_conflict_ids:
        categories.append('measurements')
    if remote_removed_image_keys:
        categories.append('remote_removed_media')

    return ObservationPushConflictReport(
        has_conflict=bool(categories),
        categories=categories,
        field_labels=field_labels,
        measurement_conflict_ids=measurement_conflict_ids,
        image_conflict_keys=image_conflict_keys,
        remote_removed_image_keys=remote_removed_image_keys,
    )


def _local_has_real_changes_since_snapshot(local_obs: dict, cloud_id: str | None = None) -> bool:
    cloud_value = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not cloud_value:
        return True
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    local_obs = _guard_local_location_precision(local_obs, baseline_obs)
    local_payload = _observation_compare_payload(local_obs, local=True)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if not _observation_field_values_match(field, local_payload.get(field), baseline_obs.get(field)):
            return True
    # A proven identity is the one local identity push can assert (through
    # the RPC); if the baseline does not record it, it still has to go out.
    if TaxonIdentity.from_row(local_obs).is_proven_sporely:
        baseline_identity = _baseline_identity_key(baseline_obs)
        if (
            baseline_identity is _IDENTITY_BASELINE_UNKNOWN
            or baseline_identity != local_payload.get(TAXON_IDENTITY_SYNC_FIELD)
        ):
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


def _is_spore_measurement_source_image(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if not should_pull_cloud_image_to_desktop(row):
        return False
    if str(row.get('image_type') or '').strip().lower() == 'microscope':
        return True
    if _normalize_cloud_media_key(row.get('storage_path')):
        return True
    return _resolve_existing_local_image_asset_path(str(row.get('filepath') or '')) is not None


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


def _load_local_measurement_lookup(observation_id: int) -> tuple[dict[str, dict], dict[int, dict]]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
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
