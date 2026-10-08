"""Conflict-plan model: the reviewed baseline and its fingerprints, drift and
shape checks, plan-identity validation, operation building, retry-state
validation, expected and intended material state, and accepted asymmetry
built from an applied plan.

Read-only model code. Conflict *execution* (``resolve_conflict_*``,
``finalize_sync_candidates``), every observation-completion and
conflict-review marker writer, and every decision to store a snapshot stay in
the facade; they call this model downward.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S7 of the cloud-sync extraction.
"""

from __future__ import annotations

import math

from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.errors import CloudSyncError
from utils.cloud_sync_impl.image_payloads import _remote_image_payload
from utils.cloud_sync_impl.preflight import _local_image_snapshot_payload
from utils.cloud_sync_impl.push_payloads import _observation_compare_payload
from utils.cloud_sync_impl.reconciliation.asymmetry import (
    _asymmetry_fingerprint_local_image,
    _asymmetry_fingerprint_local_measurement,
    _asymmetry_fingerprint_remote_image,
    _asymmetry_fingerprint_remote_measurement,
    _ASYMMETRY_MATERIAL_IMAGE_FIELDS,
)
from utils.cloud_sync_impl.reconciliation.measurements import (
    _local_measurement_snapshot_payload,
    _MEASUREMENT_FLOAT_ABS_TOL,
    _MEASUREMENT_FLOAT_REL_TOL,
    _measurement_payloads_match,
    _normalize_measurement_float_value,
    _normalize_measurement_type_value,
    _remote_measurement_snapshot_payload,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_snapshot_value,
    _observation_field_values_match,
)


def _build_plan_from_automatic_decisions(detail: dict) -> dict:
    """Translate ``detail['automatic_decisions']`` into a resolve_conflict_plan.

    The produced plan uses the exact same primitives ordinary sync would
    otherwise reach for; we route through the plan executor so drift
    protection, identity validation, presentation policy, statistics
    recomputation, and finalization all run consistently.
    """
    detail = detail or {}
    auto = detail.get('automatic_decisions') or {}
    items: list[dict] = []
    for entry in auto.get('fields') or []:
        action = entry.get('action')
        field = entry.get('field')
        if not field:
            continue
        if action == 'push_local':
            items.append({'kind': 'field', 'field': field, 'choice': 'local'})
        elif action == 'pull_cloud':
            items.append({'kind': 'field', 'field': field, 'choice': 'cloud'})
        elif action == 'auto_draft_wins':
            side = entry.get('chosen_side') or 'local'
            items.append({'kind': 'field', 'field': field, 'choice': side})
    for entry in auto.get('media') or []:
        kind = entry.get('kind')
        side = entry.get('side')
        action = entry.get('action') or ''
        local_id = _safe_int(entry.get('local_id')) or None
        cloud_id = str(entry.get('cloud_id') or '').strip() or None
        if kind == 'image' and side == 'local_only':
            items.append({'kind': 'image', 'side': 'local_only',
                          'local_id': local_id, 'choice': 'upload'})
        elif kind == 'image' and side == 'cloud_only':
            items.append({'kind': 'image', 'side': 'cloud_only',
                          'cloud_id': cloud_id, 'choice': 'download'})
        elif kind == 'image_metadata':
            choice = 'local' if action == 'auto_apply_local' else 'cloud'
            items.append({'kind': 'image_metadata',
                          'local_id': local_id, 'cloud_id': cloud_id,
                          'choice': choice})
        elif kind == 'measurement' and side == 'local_only':
            items.append({'kind': 'measurement', 'side': 'local_only',
                          'local_id': local_id, 'choice': 'upload'})
        elif kind == 'measurement' and side == 'cloud_only':
            items.append({'kind': 'measurement', 'side': 'cloud_only',
                          'cloud_id': cloud_id, 'choice': 'download'})
        elif kind == 'measurement' and side == 'matched':
            choice = 'local' if action == 'auto_apply_local' else 'cloud'
            items.append({'kind': 'measurement', 'side': 'matched',
                          'local_id': local_id, 'cloud_id': cloud_id,
                          'choice': choice})
    return {
        'items': items,
        'baseline': detail.get('plan_baseline'),
        'allow_media_deletion': False,
        'derived_statistics': (
            'recompute_from_measurements'
            if detail.get('derived_statistics') else 'unchanged'
        ),
    }


def _conflict_plan_local_observation_fingerprint(record: dict | None) -> dict:
    """Normalized local observation fingerprint used for drift detection."""
    return _observation_compare_payload(record or {}, local=True)


def _conflict_plan_remote_observation_fingerprint(record: dict | None) -> dict:
    return _observation_compare_payload(record or {}, local=False)


def _conflict_plan_local_image_fingerprint(row: dict | None) -> dict:
    payload = _local_image_snapshot_payload(row or {})
    payload['__local_id'] = _safe_int((row or {}).get('id'))
    payload['__cloud_id'] = str((row or {}).get('cloud_id') or '').strip() or None
    return payload


def _conflict_plan_remote_image_fingerprint(row: dict | None) -> dict:
    payload = _remote_image_payload(row or {})
    payload['__cloud_id'] = str((row or {}).get('id') or '').strip() or None
    payload['__desktop_id'] = _safe_int((row or {}).get('desktop_id'))
    return payload


def _conflict_plan_local_measurement_fingerprint(row: dict | None) -> dict:
    payload = _local_measurement_snapshot_payload(row or {})
    payload['__local_id'] = _safe_int((row or {}).get('id'))
    payload['__cloud_id'] = str((row or {}).get('cloud_id') or '').strip() or None
    return payload


def _conflict_plan_remote_measurement_fingerprint(row: dict | None) -> dict:
    payload = _remote_measurement_snapshot_payload(row or {})
    payload['__cloud_id'] = str((row or {}).get('id') or '').strip() or None
    payload['__desktop_id'] = _safe_int((row or {}).get('desktop_id'))
    return payload


def build_conflict_plan_baseline(
    *,
    local_obs: dict | None,
    remote_obs: dict | None,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> dict:
    """Deterministic baseline fingerprint returned inside get_conflict_detail.

    The dialog echoes this back inside the plan under ``baseline`` so that
    ``resolve_conflict_plan`` can prove the reviewed state still matches the
    live state before any write occurs.
    """
    return {
        'schema_version': _CONFLICT_PLAN_BASELINE_SCHEMA_VERSION,
        'local_observation': _conflict_plan_local_observation_fingerprint(local_obs),
        'remote_observation': _conflict_plan_remote_observation_fingerprint(remote_obs),
        'local_images': [
            _conflict_plan_local_image_fingerprint(row) for row in (local_images or [])
        ],
        'remote_images': [
            _conflict_plan_remote_image_fingerprint(row) for row in (remote_images or [])
        ],
        'local_measurements': [
            _conflict_plan_local_measurement_fingerprint(row)
            for row in (local_measurements or [])
        ],
        'remote_measurements': [
            _conflict_plan_remote_measurement_fingerprint(row)
            for row in (remote_measurements or [])
        ],
    }


def _plan_drift_message() -> str:
    return (
        'The observation changed after this comparison was loaded. '
        'Refresh the comparison before applying.'
    )


def _find_local_image_fingerprint(baseline_images: list[dict], *, local_id: int | None,
                                  cloud_id: str | None) -> dict | None:
    if local_id:
        for row in baseline_images:
            if _safe_int(row.get('__local_id')) == int(local_id):
                return row
    if cloud_id:
        for row in baseline_images:
            if str(row.get('__cloud_id') or '') == str(cloud_id):
                return row
    return None


def _find_remote_image_fingerprint(baseline_images: list[dict], *, cloud_id: str | None,
                                   local_id: int | None) -> dict | None:
    if cloud_id:
        for row in baseline_images:
            if str(row.get('__cloud_id') or '') == str(cloud_id):
                return row
    if local_id:
        for row in baseline_images:
            if _safe_int(row.get('__desktop_id')) == int(local_id):
                return row
    return None


def _find_local_measurement_fingerprint(rows: list[dict], *, local_id: int | None,
                                        cloud_id: str | None) -> dict | None:
    if local_id:
        for row in rows:
            if _safe_int(row.get('__local_id')) == int(local_id):
                return row
    if cloud_id:
        for row in rows:
            if str(row.get('__cloud_id') or '') == str(cloud_id):
                return row
    return None


def _find_remote_measurement_fingerprint(rows: list[dict], *, cloud_id: str | None,
                                         local_id: int | None) -> dict | None:
    if cloud_id:
        for row in rows:
            if str(row.get('__cloud_id') or '') == str(cloud_id):
                return row
    if local_id:
        for row in rows:
            if _safe_int(row.get('__desktop_id')) == int(local_id):
                return row
    return None


_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION = 1


_CONFLICT_PLAN_BASELINE_REQUIRED_KEYS = (
    'local_observation',
    'remote_observation',
    'local_images',
    'remote_images',
    'local_measurements',
    'remote_measurements',
)


def _validate_plan_baseline_shape(baseline: dict | None) -> None:
    """Fail closed when the per-item plan lacks a well-formed baseline.

    Turn-A A1: the item-level plan is not backward-compatible with callers
    that omit a baseline.  The old whole-observation resolvers
    (``resolve_conflict_keep_local`` / ``keep_cloud`` / ``merge``) are
    separate APIs that never took a baseline; those remain compatible.
    """
    if not isinstance(baseline, dict) or not baseline:
        raise CloudSyncError(
            'Conflict plan is missing the reviewed baseline. '
            'Refresh the comparison and try again.'
        )
    schema = baseline.get('schema_version')
    if schema != _CONFLICT_PLAN_BASELINE_SCHEMA_VERSION:
        raise CloudSyncError(
            f'Unsupported conflict plan baseline schema {schema!r}. '
            'Refresh the comparison and try again.'
        )
    for key in _CONFLICT_PLAN_BASELINE_REQUIRED_KEYS:
        if key not in baseline:
            raise CloudSyncError(
                f'Malformed conflict plan baseline (missing "{key}"). '
                'Refresh the comparison and try again.'
            )
    # Collection keys must be list-shaped.
    for key in _CONFLICT_PLAN_BASELINE_REQUIRED_KEYS[2:]:
        if not isinstance(baseline.get(key), list):
            raise CloudSyncError(
                f'Malformed conflict plan baseline ("{key}" not a list). '
                'Refresh the comparison and try again.'
            )
    if not isinstance(baseline.get('local_observation'), dict):
        raise CloudSyncError(
            'Malformed conflict plan baseline (local_observation not an object). '
            'Refresh the comparison and try again.'
        )
    if not isinstance(baseline.get('remote_observation'), dict):
        raise CloudSyncError(
            'Malformed conflict plan baseline (remote_observation not an object). '
            'Refresh the comparison and try again.'
        )


def _verify_plan_baseline(
    baseline: dict,
    items: list[dict],
    *,
    local_obs: dict | None,
    remote_obs: dict | None,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> None:
    """Abort before any write if reviewed values or identities changed."""
    has_field_item = any(item.get('kind') == 'field' for item in items)
    if has_field_item:
        current_local = _conflict_plan_local_observation_fingerprint(local_obs)
        if baseline.get('local_observation') != current_local:
            raise CloudSyncError(_plan_drift_message())
        current_remote = _conflict_plan_remote_observation_fingerprint(remote_obs)
        if baseline.get('remote_observation') != current_remote:
            raise CloudSyncError(_plan_drift_message())

    baseline_local_images = list(baseline.get('local_images') or [])
    baseline_remote_images = list(baseline.get('remote_images') or [])
    baseline_local_measurements = list(baseline.get('local_measurements') or [])
    baseline_remote_measurements = list(baseline.get('remote_measurements') or [])

    current_local_image_by_local = {
        _safe_int(row.get('id')): _conflict_plan_local_image_fingerprint(row)
        for row in local_images or []
        if _safe_int(row.get('id'))
    }
    current_local_image_by_cloud = {
        str(row.get('cloud_id') or '').strip(): _conflict_plan_local_image_fingerprint(row)
        for row in local_images or []
        if str(row.get('cloud_id') or '').strip()
    }
    current_remote_image_by_cloud = {
        str(row.get('id') or '').strip(): _conflict_plan_remote_image_fingerprint(row)
        for row in remote_images or []
        if str(row.get('id') or '').strip()
    }
    current_remote_image_by_desktop = {
        _safe_int(row.get('desktop_id')): _conflict_plan_remote_image_fingerprint(row)
        for row in remote_images or []
        if _safe_int(row.get('desktop_id'))
    }
    current_local_measurement_by_local = {
        _safe_int(row.get('id')): _conflict_plan_local_measurement_fingerprint(row)
        for row in local_measurements or []
        if _safe_int(row.get('id'))
    }
    current_local_measurement_by_cloud = {
        str(row.get('cloud_id') or '').strip(): _conflict_plan_local_measurement_fingerprint(row)
        for row in local_measurements or []
        if str(row.get('cloud_id') or '').strip()
    }
    current_remote_measurement_by_cloud = {
        str(row.get('id') or '').strip(): _conflict_plan_remote_measurement_fingerprint(row)
        for row in remote_measurements or []
        if str(row.get('id') or '').strip()
    }
    current_remote_measurement_by_desktop = {
        _safe_int(row.get('desktop_id')): _conflict_plan_remote_measurement_fingerprint(row)
        for row in remote_measurements or []
        if _safe_int(row.get('desktop_id'))
    }

    for item in items:
        kind = item.get('kind')
        local_id = _safe_int(item.get('local_id')) or None
        cloud_id = str(item.get('cloud_id') or '').strip() or None
        if kind in {'image', 'image_metadata'}:
            expected_local = _find_local_image_fingerprint(
                baseline_local_images, local_id=local_id, cloud_id=cloud_id,
            )
            if expected_local is not None:
                current = (
                    current_local_image_by_local.get(local_id) if local_id
                    else current_local_image_by_cloud.get(cloud_id or '')
                )
                if current != expected_local:
                    raise CloudSyncError(_plan_drift_message())
            expected_remote = _find_remote_image_fingerprint(
                baseline_remote_images, cloud_id=cloud_id, local_id=local_id,
            )
            if expected_remote is not None:
                current = (
                    current_remote_image_by_cloud.get(cloud_id or '') if cloud_id
                    else current_remote_image_by_desktop.get(local_id)
                )
                if current != expected_remote:
                    raise CloudSyncError(_plan_drift_message())
        elif kind == 'measurement':
            expected_local = _find_local_measurement_fingerprint(
                baseline_local_measurements, local_id=local_id, cloud_id=cloud_id,
            )
            if expected_local is not None:
                current = (
                    current_local_measurement_by_local.get(local_id) if local_id
                    else current_local_measurement_by_cloud.get(cloud_id or '')
                )
                if current != expected_local:
                    raise CloudSyncError(_plan_drift_message())
            expected_remote = _find_remote_measurement_fingerprint(
                baseline_remote_measurements, cloud_id=cloud_id, local_id=local_id,
            )
            if expected_remote is not None:
                current = (
                    current_remote_measurement_by_cloud.get(cloud_id or '') if cloud_id
                    else current_remote_measurement_by_desktop.get(local_id)
                )
                if current != expected_remote:
                    raise CloudSyncError(_plan_drift_message())


def _validate_plan_identity_state(
    *,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
    items: list[dict],
) -> None:
    """Refuse ambiguous or contradictory identity — resolver-side belt-and-braces."""
    # duplicate local cloud_ids
    local_cloud_id_counts: dict[str, int] = {}
    for row in local_images or []:
        cid = str(row.get('cloud_id') or '').strip()
        if cid:
            local_cloud_id_counts[cid] = local_cloud_id_counts.get(cid, 0) + 1
    for cid, count in local_cloud_id_counts.items():
        if count > 1:
            raise CloudSyncError(
                f'Image identity conflict: multiple local images share cloud ID {cid}'
            )
    # duplicate cloud desktop_ids
    remote_desktop_counts: dict[int, int] = {}
    for row in remote_images or []:
        did = _safe_int(row.get('desktop_id'))
        if did:
            remote_desktop_counts[did] = remote_desktop_counts.get(did, 0) + 1
    for did, count in remote_desktop_counts.items():
        if count > 1:
            raise CloudSyncError(
                f'Image identity conflict: multiple cloud images reference local image {did}'
            )
    # contradictory cross-references for every affected image
    local_by_cloud = {
        str(row.get('cloud_id') or '').strip(): row
        for row in local_images or []
        if str(row.get('cloud_id') or '').strip()
    }
    remote_by_cloud = {
        str(row.get('id') or '').strip(): row
        for row in remote_images or []
        if str(row.get('id') or '').strip()
    }
    remote_by_desktop = {
        _safe_int(row.get('desktop_id')): row
        for row in remote_images or []
        if _safe_int(row.get('desktop_id'))
    }
    for item in items:
        if item.get('kind') not in {'image', 'image_metadata', 'measurement'}:
            continue
        local_id = _safe_int(item.get('local_id')) or None
        cloud_id = str(item.get('cloud_id') or '').strip() or None
        if item.get('kind') in {'image', 'image_metadata'}:
            if cloud_id and cloud_id in remote_by_cloud and local_id:
                remote_row = remote_by_cloud[cloud_id]
                remote_desktop = _safe_int(remote_row.get('desktop_id'))
                if remote_desktop and remote_desktop != local_id:
                    raise CloudSyncError(
                        f'Image identity conflict: cloud image {cloud_id} references '
                        f'local image {remote_desktop}, not {local_id}'
                    )
            if local_id and local_id in remote_by_desktop and cloud_id:
                candidate_cloud = str(remote_by_desktop[local_id].get('id') or '').strip()
                if candidate_cloud and candidate_cloud != cloud_id:
                    raise CloudSyncError(
                        f'Image identity conflict: local image {local_id} is referenced '
                        f'by cloud image {candidate_cloud}, not {cloud_id}'
                    )
    # ── Measurement identity guards (A2) ──────────────────────────────────
    # 1. duplicate local measurement cloud_ids
    local_meas_cloud_counts: dict[str, int] = {}
    for row in local_measurements or []:
        cid = str(row.get('cloud_id') or '').strip()
        if cid:
            local_meas_cloud_counts[cid] = local_meas_cloud_counts.get(cid, 0) + 1
    for cid, count in local_meas_cloud_counts.items():
        if count > 1:
            raise CloudSyncError(
                f'Measurement identity conflict: multiple local measurements share cloud ID {cid}. '
                'Refresh the comparison and repair the link before applying.'
            )
    # 2. duplicate cloud measurement desktop_ids
    remote_meas_desktop_counts: dict[int, int] = {}
    for row in remote_measurements or []:
        did = _safe_int(row.get('desktop_id'))
        if did:
            remote_meas_desktop_counts[did] = remote_meas_desktop_counts.get(did, 0) + 1
    for did, count in remote_meas_desktop_counts.items():
        if count > 1:
            raise CloudSyncError(
                f'Measurement identity conflict: multiple cloud measurements reference local '
                f'measurement {did}. Refresh the comparison and repair the link before applying.'
            )
    # 3. cross-referenced measurement contradiction
    local_meas_by_cloud = {
        str(row.get('cloud_id') or '').strip(): row
        for row in local_measurements or []
        if str(row.get('cloud_id') or '').strip()
    }
    remote_meas_by_cloud = {
        str(row.get('id') or '').strip(): row
        for row in remote_measurements or []
        if str(row.get('id') or '').strip()
    }
    remote_meas_by_desktop = {
        _safe_int(row.get('desktop_id')): row
        for row in remote_measurements or []
        if _safe_int(row.get('desktop_id'))
    }
    local_meas_by_local = {
        _safe_int(row.get('id')): row
        for row in local_measurements or []
        if _safe_int(row.get('id'))
    }
    for item in items:
        if item.get('kind') != 'measurement':
            continue
        local_id = _safe_int(item.get('local_id')) or None
        cloud_id = str(item.get('cloud_id') or '').strip() or None
        side = item.get('side')
        # 4. missing local target
        if side in {'matched', 'local_only'} and local_id and local_id not in local_meas_by_local:
            raise CloudSyncError(
                f'Measurement identity conflict: local measurement {local_id} no longer exists. '
                'Refresh the comparison before applying.'
            )
        # 5. missing cloud target
        if side in {'matched', 'cloud_only'} and cloud_id and cloud_id not in remote_meas_by_cloud:
            raise CloudSyncError(
                f'Measurement identity conflict: cloud measurement {cloud_id} no longer exists. '
                'Refresh the comparison before applying.'
            )
        # 6. cross-referenced contradiction for matched measurements
        if side == 'matched' and local_id and cloud_id:
            remote = remote_meas_by_cloud.get(cloud_id)
            if remote is not None:
                remote_desktop = _safe_int(remote.get('desktop_id'))
                if remote_desktop and remote_desktop != local_id:
                    raise CloudSyncError(
                        f'Measurement identity conflict: cloud measurement {cloud_id} '
                        f'references local measurement {remote_desktop}, not {local_id}. '
                        'Refresh the comparison before applying.'
                    )
            reverse = remote_meas_by_desktop.get(local_id)
            if reverse is not None:
                reverse_cloud = str(reverse.get('id') or '').strip()
                if reverse_cloud and reverse_cloud != cloud_id:
                    raise CloudSyncError(
                        f'Measurement identity conflict: local measurement {local_id} is '
                        f'referenced by cloud measurement {reverse_cloud}, not {cloud_id}. '
                        'Refresh the comparison before applying.'
                    )
        # 7. measurement moved to a different image after review
        if side == 'matched' and local_id:
            local_row = local_meas_by_local.get(local_id)
            reviewed_local_image = _safe_int(item.get('local_image_id')) or None
            if local_row is not None and reviewed_local_image:
                current_image = _safe_int(local_row.get('image_id'))
                if current_image and current_image != reviewed_local_image:
                    raise CloudSyncError(
                        f'Measurement identity conflict: local measurement {local_id} '
                        f'now belongs to image {current_image}, not {reviewed_local_image}. '
                        'Refresh the comparison before applying.'
                    )
        if side == 'matched' and cloud_id:
            remote_row = remote_meas_by_cloud.get(cloud_id)
            reviewed_cloud_image = str(item.get('cloud_image_id') or '').strip() or None
            if remote_row is not None and reviewed_cloud_image:
                current_image = str(remote_row.get('image_id') or '').strip()
                if current_image and current_image != reviewed_cloud_image:
                    raise CloudSyncError(
                        f'Measurement identity conflict: cloud measurement {cloud_id} '
                        f'now belongs to image {current_image}, not {reviewed_cloud_image}. '
                        'Refresh the comparison before applying.'
                    )
        # 8. owning image not among the authoritative paired images
        image_local = _safe_int(item.get('local_image_id')) or None
        image_cloud = str(item.get('cloud_image_id') or '').strip() or None
        if image_local:
            if image_local not in {_safe_int(row.get('id'))
                                   for row in local_images or [] if _safe_int(row.get('id'))}:
                raise CloudSyncError(
                    f'Measurement identity conflict: local image {image_local} no longer exists. '
                    'Refresh the comparison before applying.'
                )
        if image_cloud:
            if image_cloud not in {str(row.get('id') or '').strip()
                                    for row in remote_images or [] if str(row.get('id') or '').strip()}:
                raise CloudSyncError(
                    f'Measurement identity conflict: cloud image {image_cloud} no longer exists. '
                    'Refresh the comparison before applying.'
                )
        # 9. matched measurement whose reviewed owning-image pair is not itself authoritative
        if side == 'matched' and image_local and image_cloud:
            authoritative_pairs = {
                (_safe_int(lrow.get('id')), str(rrow.get('id') or '').strip())
                for lrow in local_images or []
                for rrow in remote_images or []
                if _safe_int(lrow.get('id'))
                and str(rrow.get('id') or '').strip()
                and str(lrow.get('cloud_id') or '').strip() == str(rrow.get('id') or '').strip()
            }
            if (image_local, image_cloud) not in authoritative_pairs:
                raise CloudSyncError(
                    f'Measurement identity conflict: reviewed owning images '
                    f'(local {image_local}, cloud {image_cloud}) are not the authoritative '
                    'paired images. Refresh the comparison and repair the link before applying.'
                )
        # 10. ambiguous plan identity
        if side == 'matched' and not (local_id and cloud_id):
            raise CloudSyncError(
                'Measurement identity conflict: matched-measurement plan item is missing '
                'one of local_id or cloud_id. Refresh the comparison before applying.'
            )


def _build_plan_operations(items: list[dict]) -> list[dict]:
    """Turn the item plan into a spy-friendly, unambiguous operation list.

    Every op is dispatched later by ``resolve_conflict_plan`` from this list;
    no set-comprehension is used to derive execution behavior after this point.
    """
    ops: list[dict] = []
    for item in items:
        kind = item.get('kind')
        choice = item.get('choice')
        local_id = _safe_int(item.get('local_id')) or None
        cloud_id = str(item.get('cloud_id') or '').strip() or None
        side = item.get('side')
        if kind == 'field':
            if choice == 'cloud':
                ops.append({'op': 'pull_field', 'field': item.get('field')})
            elif choice == 'local':
                ops.append({'op': 'push_field', 'field': item.get('field')})
        elif kind == 'image':
            if choice == 'upload':
                ops.append({'op': 'push_image', 'local_id': local_id})
            elif choice == 'download':
                ops.append({'op': 'import_image', 'cloud_id': cloud_id})
            elif choice in {'keep_local', 'keep_cloud'}:
                ops.append({
                    'op': 'keep_asymmetric_image',
                    'side': 'local' if choice == 'keep_local' else 'cloud',
                    'local_id': local_id,
                    'cloud_id': cloud_id,
                })
        elif kind == 'image_metadata':
            if choice == 'local':
                ops.append({
                    'op': 'push_image_metadata',
                    'local_id': local_id,
                    'cloud_id': cloud_id,
                })
            elif choice == 'cloud':
                ops.append({
                    'op': 'apply_image_metadata',
                    'local_id': local_id,
                    'cloud_id': cloud_id,
                })
        elif kind == 'measurement':
            if side == 'matched':
                if choice == 'local':
                    ops.append({
                        'op': 'push_measurement',
                        'local_id': local_id,
                        'cloud_id': cloud_id,
                    })
                elif choice == 'cloud':
                    ops.append({
                        'op': 'import_measurement',
                        'cloud_id': cloud_id,
                        'local_id': local_id,
                    })
            elif side == 'local_only':
                if choice == 'upload':
                    ops.append({
                        'op': 'push_measurement',
                        'local_id': local_id,
                    })
                elif choice == 'keep_local':
                    ops.append({
                        'op': 'keep_asymmetric_measurement',
                        'side': 'local',
                        'local_id': local_id,
                    })
            elif side == 'cloud_only':
                if choice == 'download':
                    ops.append({
                        'op': 'import_measurement',
                        'cloud_id': cloud_id,
                    })
                elif choice == 'keep_cloud':
                    ops.append({
                        'op': 'keep_asymmetric_measurement',
                        'side': 'cloud',
                        'cloud_id': cloud_id,
                    })
    return ops


def _malformed_retry_state(reason: str) -> CloudSyncError:
    """Fail-closed error used when a retry ``prior_result`` op is unusable."""
    return CloudSyncError(
        f'Malformed retry state ({reason}). Refresh the comparison and try again.'
    )


def _reconcile_verification_pending_op(
    op: dict,
    *,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> tuple:
    """Recover from a write-succeeded / read-back-failed op on retry.

    Returns one of:

    * ``('completed', {'local_image_ids': ..., 'cloud_image_ids': ..., ...})``
      when the intended row is present in current state and material matches;
    * ``('drift', <reason>)`` when a candidate row is present but its material
      diverges (unrelated edit) — retry aborts;
    * ``('missing', None)`` when no candidate row exists — treat the write as
      not having gone through, let the plan's normal dispatch rerun it.
    """
    opname = op.get('op')
    stable = op.get('stable_identity') or {}
    intended = op.get('intended_after') or {}
    kind = intended.get('kind')
    explained = {'local_image_ids': set(), 'cloud_image_ids': set(),
                 'local_meas_ids': set(), 'cloud_meas_ids': set()}
    if kind == 'image':
        material_local_intended = (intended.get('material_local') or {}).get('material') or {}
        material_remote_intended = (intended.get('material_remote') or {}).get('material') or {}
        lid = _safe_int(stable.get('local_id')) or None
        cid = str(stable.get('cloud_id') or '').strip() or None
        local_row = None
        remote_row = None
        if lid:
            local_row = next((r for r in local_images if _safe_int(r.get('id')) == lid), None)
        if not local_row and cid:
            local_row = next(
                (r for r in local_images
                 if str(r.get('cloud_id') or '').strip() == cid),
                None,
            )
        if cid:
            remote_row = next(
                (r for r in remote_images
                 if str(r.get('id') or '').strip() == cid),
                None,
            )
        if not remote_row and lid:
            remote_row = next(
                (r for r in remote_images if _safe_int(r.get('desktop_id')) == lid),
                None,
            )
        if not local_row and not remote_row:
            return ('missing', None)
        if material_local_intended and local_row is not None:
            current_local_material = _material_image_expected_state(
                local_row, side='local',
            ).get('material')
            if current_local_material != material_local_intended:
                return ('drift',
                        f'{opname} verification recovered a local image whose material differs')
            explained['local_image_ids'].add(_safe_int(local_row.get('id')))
        if material_remote_intended and remote_row is not None:
            current_remote_material = _material_image_expected_state(
                remote_row, side='remote',
            ).get('material')
            if current_remote_material != material_remote_intended:
                return ('drift',
                        f'{opname} verification recovered a cloud image whose material differs')
            explained['cloud_image_ids'].add(str(remote_row.get('id') or '').strip())
        return ('completed', explained)
    if kind == 'measurement':
        material_local_intended = (intended.get('material_local') or {}).get('material') or {}
        material_remote_intended = (intended.get('material_remote') or {}).get('material') or {}
        lid = _safe_int(stable.get('local_id')) or None
        cid = str(stable.get('cloud_id') or '').strip() or None
        local_row = None
        remote_row = None
        if lid:
            local_row = next(
                (r for r in local_measurements if _safe_int(r.get('id')) == lid),
                None,
            )
        if not local_row and cid:
            local_row = next(
                (r for r in local_measurements
                 if str(r.get('cloud_id') or '').strip() == cid),
                None,
            )
        if cid:
            remote_row = next(
                (r for r in remote_measurements
                 if str(r.get('id') or '').strip() == cid),
                None,
            )
        if not remote_row and lid:
            remote_row = next(
                (r for r in remote_measurements if _safe_int(r.get('desktop_id')) == lid),
                None,
            )
        if not local_row and not remote_row:
            return ('missing', None)
        if material_local_intended and local_row is not None:
            current_local_material = _material_measurement_expected_state(
                local_row, side='local',
            ).get('material')
            if current_local_material != material_local_intended:
                return ('drift',
                        f'{opname} verification recovered a local measurement whose material differs')
            explained['local_meas_ids'].add(_safe_int(local_row.get('id')))
        if material_remote_intended and remote_row is not None:
            current_remote_material = _material_measurement_expected_state(
                remote_row, side='remote',
            ).get('material')
            if current_remote_material != material_remote_intended:
                return ('drift',
                        f'{opname} verification recovered a cloud measurement whose material differs')
            explained['cloud_meas_ids'].add(str(remote_row.get('id') or '').strip())
        return ('completed', explained)
    return ('missing', None)


def _validate_prior_op_expected_after(op: dict) -> None:
    """Reject a prior completed/verification-pending op with missing structure.

    Fails closed rather than accepting identity-only proof.  This runs BEFORE
    any explained-set inclusion, so a malformed op cannot make its records
    silently exclude themselves from drift validation.
    """
    opname = str(op.get('op') or '')
    status = str(op.get('status') or '')
    expected = op.get('expected_after')
    intended = op.get('intended_after')
    if status == 'verification_pending':
        # Own set of requirements — validated in the verification_pending
        # branch below; but we still require write_attempted + stable_identity
        # + intended_after with a kind.
        if not op.get('write_attempted'):
            raise _malformed_retry_state(
                f'{opname} verification_pending without write_attempted flag'
            )
        if not isinstance(op.get('stable_identity'), dict):
            raise _malformed_retry_state(
                f'{opname} verification_pending missing stable_identity'
            )
        if not isinstance(intended, dict):
            raise _malformed_retry_state(
                f'{opname} verification_pending missing intended_after'
            )
        kind = intended.get('kind')
        if kind not in {'image', 'measurement'}:
            raise _malformed_retry_state(
                f'{opname} verification_pending has invalid intended_after.kind'
            )
        return
    if status != 'completed':
        return  # failed / pending / already_complete — validated elsewhere
    if opname in {'push_field', 'pull_field'}:
        if not isinstance(expected, dict):
            raise _malformed_retry_state(f'{opname} missing expected_after')
        if 'value' not in expected:
            raise _malformed_retry_state(f'{opname} missing expected_after.value')
        if not op.get('field'):
            raise _malformed_retry_state(f'{opname} missing field name')
        return
    if opname == 'recompute_spore_statistics':
        if not isinstance(expected, dict) or 'value' not in expected:
            raise _malformed_retry_state(
                'recompute_spore_statistics missing expected_after.value'
            )
        return
    if opname in {'keep_asymmetric_image', 'keep_asymmetric_measurement',
                  'preserve_spore_statistics_no_measurements',
                  'restore_presentation', 'assign_downloaded_order',
                  'store_snapshot'}:
        return  # no material verification needed
    if opname in {'push_image', 'import_image',
                  'push_image_metadata', 'apply_image_metadata'}:
        _require_full_material_expected(op, expected, kind='image')
        return
    if opname in {'push_measurement', 'import_measurement'}:
        _require_full_material_expected(op, expected, kind='measurement')
        return
    raise _malformed_retry_state(f'unknown completed op {opname!r}')


def _require_full_material_expected(op: dict, expected, *, kind: str) -> None:
    opname = op.get('op')

    def _bad(reason: str):
        raise _malformed_retry_state(f'{opname}: {reason}')

    if not isinstance(expected, dict):
        _bad('missing expected_after')
    if expected.get('kind') != kind:
        _bad(f'expected_after.kind should be {kind!r}')
    for side in ('material_local', 'material_remote'):
        side_data = expected.get(side)
        if not isinstance(side_data, dict):
            _bad(f'missing {side}')
        stable = side_data.get('stable')
        material = side_data.get('material')
        if not isinstance(stable, dict):
            _bad(f'{side} missing stable identity')
        if not isinstance(material, dict):
            _bad(f'{side} missing material fingerprint')
        if kind == 'image':
            required_material = set(_ASYMMETRY_MATERIAL_IMAGE_FIELDS)
        else:
            required_material = set(_EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS)
        missing = required_material - set(material.keys())
        if missing:
            _bad(f'{side}.material missing fields: {sorted(missing)}')
    # Op-specific stable-identity requirements.
    local_stable = expected['material_local']['stable']
    remote_stable = expected['material_remote']['stable']
    if opname == 'push_image':
        if not remote_stable.get('cloud_id'):
            _bad('material_remote.stable.cloud_id is required after upload')
        if not local_stable.get('local_id'):
            _bad('material_local.stable.local_id is required (upload source)')
    elif opname == 'import_image':
        if not local_stable.get('local_id'):
            _bad('material_local.stable.local_id is required after import')
        if not remote_stable.get('cloud_id'):
            _bad('material_remote.stable.cloud_id is required (import source)')
    elif opname in {'push_image_metadata', 'apply_image_metadata'}:
        if not local_stable.get('local_id'):
            _bad('material_local.stable.local_id is required')
        if not remote_stable.get('cloud_id'):
            _bad('material_remote.stable.cloud_id is required')
    elif opname in {'push_measurement', 'import_measurement'}:
        if not local_stable.get('local_id'):
            _bad('material_local.stable.local_id is required')
        if not remote_stable.get('cloud_id'):
            _bad('material_remote.stable.cloud_id is required')
        # Owning-image identity must be present (may be null, but the key
        # must exist so we KNOW it was captured, not just omitted).
        if 'owning_local_image_id' not in local_stable:
            _bad('material_local.stable.owning_local_image_id is required')
        if 'owning_cloud_image_id' not in remote_stable:
            _bad('material_remote.stable.owning_cloud_image_id is required')


# ── Full material expected-after fingerprints (Turn-B final Fix 1) ───────────
# Presentation-only fields (``gallery_rotation``, ``sort_order``) and every
# transport/storage/cache field are intentionally excluded so that a purely
# nonmaterial change between attempts does NOT abort a legitimate retry.

_EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS = (
    'length_um', 'width_um', 'measurement_type',
    'p1_x', 'p1_y', 'p2_x', 'p2_y', 'p3_x', 'p3_y', 'p4_x', 'p4_y',
)


def _material_image_expected_state(row: dict | None, *, side: str) -> dict:
    """Full material post-state fingerprint of one image row.

    Includes stable identity + owning-observation link + every material
    content field.  Used both to record the post-write state on each completed
    op and to verify it on retry.
    """
    row = dict(row or {})
    if side == 'local':
        stable = {
            'local_id': _safe_int(row.get('id')) or None,
            'cloud_id': str(row.get('cloud_id') or '').strip() or None,
            'observation_id': _safe_int(row.get('observation_id')) or None,
        }
    else:
        stable = {
            'cloud_id': str(row.get('id') or '').strip() or None,
            'local_id': _safe_int(row.get('desktop_id')) or None,
            'observation_id': str(row.get('observation_id') or '').strip() or None,
        }
    payload = {
        field: _normalize_snapshot_value(row.get(field))
        for field in _ASYMMETRY_MATERIAL_IMAGE_FIELDS
    }
    return {'side': side, 'stable': stable, 'material': payload}


def _material_measurement_expected_state(row: dict | None, *, side: str) -> dict:
    """Full material post-state fingerprint of one measurement row.

    Includes stable identity, owning-image identity, and every scientific
    value.  Non-scientific / transport / presentation fields are excluded.
    """
    row = dict(row or {})
    if side == 'local':
        stable = {
            'local_id': _safe_int(row.get('id')) or None,
            'cloud_id': str(row.get('cloud_id') or '').strip() or None,
            'owning_local_image_id': _safe_int(row.get('image_id')) or None,
            'owning_cloud_image_id': None,
        }
    else:
        stable = {
            'cloud_id': str(row.get('id') or '').strip() or None,
            'local_id': _safe_int(row.get('desktop_id')) or None,
            'owning_local_image_id': None,
            'owning_cloud_image_id': str(row.get('image_id') or '').strip() or None,
        }
    payload: dict = {}
    for field in _EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS:
        if field == 'measurement_type':
            payload[field] = _normalize_measurement_type_value(row.get(field))
        else:
            payload[field] = _normalize_measurement_float_value(row.get(field))
    return {'side': side, 'stable': stable, 'material': payload}


def _intended_after_image_from_local(row: dict | None) -> dict:
    """Build ``intended_after`` for a push whose readback failed.

    Uses the local (source) row's material fields as the intended cloud
    material — since a push copies material fields, the cloud row is expected
    to match once it materializes.
    """
    row = dict(row or {})
    local_state = _material_image_expected_state(row, side='local')
    remote_material = dict(local_state.get('material') or {})
    return {
        'kind': 'image',
        'material_local': local_state,
        'material_remote': {
            'side': 'remote',
            'stable': {
                'cloud_id': str(row.get('cloud_id') or '').strip() or None,
                'local_id': _safe_int(row.get('id')) or None,
                'observation_id': None,
            },
            'material': remote_material,
        },
    }


def _intended_after_image_from_remote(row: dict | None) -> dict:
    row = dict(row or {})
    remote_state = _material_image_expected_state(row, side='remote')
    local_material = dict(remote_state.get('material') or {})
    return {
        'kind': 'image',
        'material_local': {
            'side': 'local',
            'stable': {
                'local_id': _safe_int(row.get('desktop_id')) or None,
                'cloud_id': str(row.get('id') or '').strip() or None,
                'observation_id': None,
            },
            'material': local_material,
        },
        'material_remote': remote_state,
    }


def _intended_after_measurement_from_local(row: dict | None) -> dict:
    row = dict(row or {})
    local_state = _material_measurement_expected_state(row, side='local')
    material = dict(local_state.get('material') or {})
    return {
        'kind': 'measurement',
        'material_local': local_state,
        'material_remote': {
            'side': 'remote',
            'stable': {
                'cloud_id': str(row.get('cloud_id') or '').strip() or None,
                'local_id': _safe_int(row.get('id')) or None,
                'owning_local_image_id': None,
                'owning_cloud_image_id': None,
            },
            'material': material,
        },
    }


def _intended_after_measurement_from_remote(row: dict | None) -> dict:
    row = dict(row or {})
    remote_state = _material_measurement_expected_state(row, side='remote')
    material = dict(remote_state.get('material') or {})
    return {
        'kind': 'measurement',
        'material_local': {
            'side': 'local',
            'stable': {
                'local_id': _safe_int(row.get('desktop_id')) or None,
                'cloud_id': str(row.get('id') or '').strip() or None,
                'owning_local_image_id': None,
                'owning_cloud_image_id': str(row.get('image_id') or '').strip() or None,
            },
            'material': material,
        },
        'material_remote': remote_state,
    }


def _material_image_current_state(current_row: dict | None, *, side: str) -> dict:
    """Alias — the current-side fingerprint uses the same shape."""
    return _material_image_expected_state(current_row, side=side)


def _material_measurement_current_state(current_row: dict | None, *, side: str) -> dict:
    return _material_measurement_expected_state(current_row, side=side)


def _normalize_observation_field_for_baseline(
    obs: dict | None, field: str, *, local: bool
) -> object:
    """Return the same normalized value the baseline fingerprint would carry."""
    payload = _observation_compare_payload(obs or {}, local=local)
    return payload.get(field)


def _verify_completed_ops_and_rebase(
    prior_result: dict,
    original_baseline: dict,
    *,
    local_obs: dict | None,
    remote_obs: dict | None,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> tuple[dict, set[tuple], list[dict]]:
    """Verify each completed op's expected effect, then rebase the baseline.

    Returns ``(rebased_baseline, completed_op_keys, verified_ops)``.  Raises
    :class:`CloudSyncError` if any completed op's expected effect is missing
    from the current state or if any UNRELATED record has drifted.

    The caller treats ``completed_op_keys`` as "do not re-dispatch" and uses
    the rebased baseline for drift-checking the remaining (pending/failed)
    operations.  This is the expected-effect-aware retry algorithm.
    """
    completed_ops = [
        op for op in (prior_result.get('operations') or [])
        if isinstance(op, dict) and op.get('status') in {'completed', 'verification_pending'}
    ]

    explained_field_names: set[str] = set()
    explained_local_image_ids: set[int] = set()
    explained_cloud_image_ids: set[str] = set()
    explained_local_meas_ids: set[int] = set()
    explained_cloud_meas_ids: set[str] = set()

    def _drift_error(reason: str) -> CloudSyncError:
        return CloudSyncError(
            f'{_plan_drift_message()} ({reason})'
        )

    # Ops that couldn't be resolved on retry (row missing) fall out of the
    # explained set so the plan re-dispatches them; they are removed from
    # ``completed_ops`` here so downstream code doesn't add them.
    resolved_verification_pending: list[dict] = []

    for op in list(completed_ops):
        # Fail-closed structure check.  A malformed prior op cannot bypass
        # drift validation via the explained-set — the exception aborts the
        # retry before any explained-set entry is added.
        _validate_prior_op_expected_after(op)
        if op.get('status') == 'verification_pending':
            resolution = _reconcile_verification_pending_op(
                op,
                local_images=local_images, remote_images=remote_images,
                local_measurements=local_measurements,
                remote_measurements=remote_measurements,
            )
            # resolution is one of:
            #   ('completed', explained_ids...) — row discovered and material matches
            #   ('drift', reason)               — row discovered but material differs
            #   ('missing', None)               — row not found; re-dispatch normally
            kind = resolution[0]
            if kind == 'drift':
                raise _drift_error(resolution[1])
            if kind == 'missing':
                # Do NOT add to explained_* — the plan's normal dispatch reruns
                # the op (safe: underlying helpers upsert by stable id).
                completed_ops.remove(op)
                continue
            # kind == 'completed'
            explained = resolution[1]
            explained_local_image_ids.update(explained.get('local_image_ids', set()))
            explained_cloud_image_ids.update(explained.get('cloud_image_ids', set()))
            explained_local_meas_ids.update(explained.get('local_meas_ids', set()))
            explained_cloud_meas_ids.update(explained.get('cloud_meas_ids', set()))
            resolved_verification_pending.append(op)
            continue
        opname = op.get('op')
        expected = op.get('expected_after') or {}
        if opname == 'pull_field':
            field = op.get('field') or ''
            if not field:
                continue
            current = _normalize_observation_field_for_baseline(
                local_obs, field, local=True
            )
            if not _observation_field_values_match(field, current, expected.get('value')):
                raise _drift_error(
                    f'local observation field "{field}" no longer matches the '
                    'expected effect of a completed pull_field'
                )
            explained_field_names.add(field)
        elif opname == 'push_field':
            field = op.get('field') or ''
            if not field:
                continue
            current = _normalize_observation_field_for_baseline(
                remote_obs, field, local=False
            )
            if not _observation_field_values_match(field, current, expected.get('value')):
                raise _drift_error(
                    f'cloud observation field "{field}" no longer matches the '
                    'expected effect of a completed push_field'
                )
            explained_field_names.add(field)
        elif opname in {'push_image', 'import_image',
                        'push_image_metadata', 'apply_image_metadata'}:
            # Full material verification for every image op.  Identity alone is
            # not proof of intactness; the material post-state must match.
            lid = _safe_int(op.get('local_id')) or None
            cid = str(op.get('cloud_id') or '').strip() or None
            expected_local = (expected or {}).get('material_local')
            expected_remote = (expected or {}).get('material_remote')
            if lid and expected_local is not None:
                current_local = next(
                    (r for r in local_images if _safe_int(r.get('id')) == lid),
                    None,
                )
                if current_local is None:
                    raise _drift_error(
                        f'{opname}: local image {lid} no longer exists'
                    )
                current_fp = _material_image_current_state(current_local, side='local')
                if current_fp != expected_local:
                    raise _drift_error(
                        f'{opname}: local image {lid} material content changed since '
                        'the completed operation wrote it'
                    )
                explained_local_image_ids.add(lid)
            if cid and expected_remote is not None:
                current_remote_row = next(
                    (r for r in remote_images if str(r.get('id') or '').strip() == cid),
                    None,
                )
                if current_remote_row is None:
                    raise _drift_error(
                        f'{opname}: cloud image {cid} no longer exists'
                    )
                current_fp_r = _material_image_current_state(current_remote_row, side='remote')
                if current_fp_r != expected_remote:
                    raise _drift_error(
                        f'{opname}: cloud image {cid} material content changed since '
                        'the completed operation wrote it'
                    )
                explained_cloud_image_ids.add(cid)
        elif opname in {'push_measurement', 'import_measurement'}:
            lid = _safe_int(op.get('local_id')) or None
            cid = str(op.get('cloud_id') or '').strip() or None
            expected_local = (expected or {}).get('material_local')
            expected_remote = (expected or {}).get('material_remote')
            if lid and expected_local is not None:
                current_local = next(
                    (r for r in local_measurements if _safe_int(r.get('id')) == lid),
                    None,
                )
                if current_local is None:
                    raise _drift_error(
                        f'{opname}: local measurement {lid} no longer exists'
                    )
                # Owning-image change is caught here: material_local includes
                # ``stable.owning_local_image_id``.
                current_fp = _material_measurement_current_state(current_local, side='local')
                if current_fp != expected_local:
                    raise _drift_error(
                        f'{opname}: local measurement {lid} scientific values or '
                        'owning image changed since the completed operation'
                    )
                explained_local_meas_ids.add(lid)
            if cid and expected_remote is not None:
                current_remote_row = next(
                    (r for r in remote_measurements if str(r.get('id') or '').strip() == cid),
                    None,
                )
                if current_remote_row is None:
                    raise _drift_error(
                        f'{opname}: cloud measurement {cid} no longer exists'
                    )
                current_fp_r = _material_measurement_current_state(current_remote_row, side='remote')
                if current_fp_r != expected_remote:
                    raise _drift_error(
                        f'{opname}: cloud measurement {cid} scientific values or '
                        'owning image changed since the completed operation'
                    )
                explained_cloud_meas_ids.add(cid)
        elif opname == 'recompute_spore_statistics':
            explained_field_names.add('spore_statistics')
            value = expected.get('value') if isinstance(expected, dict) else None
            if value is not None:
                current_local = _normalize_observation_field_for_baseline(
                    local_obs, 'spore_statistics', local=True
                )
                current_remote = _normalize_observation_field_for_baseline(
                    remote_obs, 'spore_statistics', local=False
                )
                if not (
                    _observation_field_values_match('spore_statistics', current_local, value)
                    and _observation_field_values_match(
                        'spore_statistics', current_remote, value
                    )
                ):
                    raise _drift_error(
                        'spore_statistics no longer matches the recomputed value'
                    )
        # keep_asymmetric_*, preserve_spore_statistics_no_measurements,
        # restore_presentation, assign_downloaded_order: no verification needed
        # here (no cloud/local material write happened).

    # ── Now check that every UNRELATED record still matches original baseline ─
    original_local_obs = original_baseline.get('local_observation') or {}
    original_remote_obs = original_baseline.get('remote_observation') or {}
    current_local_obs_fp = _conflict_plan_local_observation_fingerprint(local_obs)
    current_remote_obs_fp = _conflict_plan_remote_observation_fingerprint(remote_obs)
    for field, expected_value in original_local_obs.items():
        if field in explained_field_names:
            continue
        if not _observation_field_values_match(
            field, current_local_obs_fp.get(field), expected_value
        ):
            raise _drift_error(
                f'unrelated local field "{field}" changed since the baseline'
            )
    for field, expected_value in original_remote_obs.items():
        if field in explained_field_names:
            continue
        if not _observation_field_values_match(
            field, current_remote_obs_fp.get(field), expected_value
        ):
            raise _drift_error(
                f'unrelated cloud field "{field}" changed since the baseline'
            )

    def _local_id(row): return _safe_int(row.get('__local_id')) or _safe_int(row.get('local_id'))
    def _cloud_id(row): return str(row.get('__cloud_id') or row.get('cloud_id') or '').strip()

    current_local_images_fp = {
        _safe_int(row.get('id')): _conflict_plan_local_image_fingerprint(row)
        for row in local_images or [] if _safe_int(row.get('id'))
    }
    current_remote_images_fp = {
        str(row.get('id') or '').strip(): _conflict_plan_remote_image_fingerprint(row)
        for row in remote_images or [] if str(row.get('id') or '').strip()
    }
    for row in original_baseline.get('local_images') or []:
        lid = _local_id(row)
        if not lid or lid in explained_local_image_ids:
            continue
        current = current_local_images_fp.get(lid)
        if current != row:
            raise _drift_error(
                f'unrelated local image {lid} changed since the baseline'
            )
    for row in original_baseline.get('remote_images') or []:
        cid = _cloud_id(row)
        if not cid or cid in explained_cloud_image_ids:
            continue
        current = current_remote_images_fp.get(cid)
        if current != row:
            raise _drift_error(
                f'unrelated cloud image {cid} changed since the baseline'
            )

    current_local_meas_fp = {
        _safe_int(row.get('id')): _conflict_plan_local_measurement_fingerprint(row)
        for row in local_measurements or [] if _safe_int(row.get('id'))
    }
    current_remote_meas_fp = {
        str(row.get('id') or '').strip(): _conflict_plan_remote_measurement_fingerprint(row)
        for row in remote_measurements or [] if str(row.get('id') or '').strip()
    }
    for row in original_baseline.get('local_measurements') or []:
        lid = _local_id(row)
        if not lid or lid in explained_local_meas_ids:
            continue
        current = current_local_meas_fp.get(lid)
        if current != row:
            raise _drift_error(
                f'unrelated local measurement {lid} changed since the baseline'
            )
    for row in original_baseline.get('remote_measurements') or []:
        cid = _cloud_id(row)
        if not cid or cid in explained_cloud_meas_ids:
            continue
        current = current_remote_meas_fp.get(cid)
        if current != row:
            raise _drift_error(
                f'unrelated cloud measurement {cid} changed since the baseline'
            )

    # Rebase — build a fresh baseline from the verified current state.
    rebased = build_conflict_plan_baseline(
        local_obs=local_obs,
        remote_obs=remote_obs,
        local_images=list(local_images or []),
        remote_images=list(remote_images or []),
        local_measurements=list(local_measurements or []),
        remote_measurements=list(remote_measurements or []),
    )
    completed_keys = {_stable_op_key(op) for op in completed_ops}
    # Attach the observed ``already_complete`` status to the ops we hand back.
    verified_ops = []
    for op in completed_ops:
        entry = dict(op)
        entry['status'] = 'already_complete'
        verified_ops.append(entry)
    return rebased, completed_keys, verified_ops


def _stable_op_key(op: dict) -> tuple:
    """Identity of an operation across retries.  Used to dedupe completed work."""
    return (
        str(op.get('op') or ''),
        str(op.get('field') or ''),
        _safe_int(op.get('local_id')) or 0,
        str(op.get('cloud_id') or '').strip(),
    )


def _plan_dispatch_keys(op: dict) -> set:
    """Every plan-op key an item with the same identity could produce.

    A completed op may carry the newly-formed cloud id (or the newly-linked
    local id), but the plan-op derived from the user's item won't yet know it.
    Return both variants so the filter matches either.
    """
    opname = str(op.get('op') or '')
    field = str(op.get('field') or '')
    local_id = _safe_int(op.get('local_id')) or 0
    cloud_id = str(op.get('cloud_id') or '').strip()
    keys: set = {(opname, field, local_id, cloud_id)}
    if opname in {'push_image', 'push_measurement'}:
        # Plan-op form: cloud_id not yet known.
        keys.add((opname, field, local_id, ''))
    elif opname in {'import_image', 'import_measurement'}:
        # Plan-op form: local_id not yet known.
        keys.add((opname, field, 0, cloud_id))
    return keys


def _plan_item_matches_completed_op(item: dict, completed_key: tuple) -> bool:
    """Return True when a plan item's dispatch matches an already-completed op key."""
    kind = item.get('kind')
    choice = item.get('choice')
    side = item.get('side')
    local_id = _safe_int(item.get('local_id')) or 0
    cloud_id = str(item.get('cloud_id') or '').strip()
    field = str(item.get('field') or '')
    if kind == 'field':
        if choice == 'cloud':
            return completed_key == ('pull_field', field, 0, '')
        if choice == 'local':
            return completed_key == ('push_field', field, 0, '')
    elif kind == 'image':
        if choice == 'upload':
            return completed_key == ('push_image', '', local_id, '')
        if choice == 'download':
            return completed_key == ('import_image', '', 0, cloud_id)
        if choice in {'keep_local', 'keep_cloud'}:
            return completed_key == (
                'keep_asymmetric_image', '', local_id, cloud_id,
            )
    elif kind == 'image_metadata':
        if choice == 'local':
            return completed_key == ('push_image_metadata', '', local_id, cloud_id)
        if choice == 'cloud':
            return completed_key == ('apply_image_metadata', '', local_id, cloud_id)
    elif kind == 'measurement':
        if side == 'matched':
            if choice == 'local':
                return completed_key == ('push_measurement', '', local_id, cloud_id)
            if choice == 'cloud':
                return completed_key == ('import_measurement', '', local_id, cloud_id)
        elif side == 'local_only':
            if choice == 'upload':
                return completed_key == ('push_measurement', '', local_id, '')
            if choice == 'keep_local':
                return completed_key == ('keep_asymmetric_measurement', '', local_id, '')
        elif side == 'cloud_only':
            if choice == 'download':
                return completed_key == ('import_measurement', '', 0, cloud_id)
            if choice == 'keep_cloud':
                return completed_key == ('keep_asymmetric_measurement', '', 0, cloud_id)
    return False


def _build_accepted_asymmetry_from_plan(
    items: list[dict],
    *,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> dict:
    """Turn each ``keep_*`` item into a durable accepted-asymmetry entry.

    Only items whose stable identity still resolves to a live row on the
    named side are recorded; a keep-only choice against a phantom row is
    ignored so we never invent acceptance.
    """
    accepted = {
        'local_only_images': [],
        'cloud_only_images': [],
        'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    local_image_by_id = {_safe_int(r.get('id')): r for r in local_images
                         if _safe_int(r.get('id'))}
    remote_image_by_id = {str(r.get('id') or '').strip(): r for r in remote_images
                          if str(r.get('id') or '').strip()}
    local_meas_by_id = {_safe_int(r.get('id')): r for r in local_measurements
                        if _safe_int(r.get('id'))}
    remote_meas_by_id = {str(r.get('id') or '').strip(): r for r in remote_measurements
                         if str(r.get('id') or '').strip()}
    for item in items:
        kind = item.get('kind')
        choice = item.get('choice')
        if kind == 'image' and choice == 'keep_local':
            local_id = _safe_int(item.get('local_id'))
            row = local_image_by_id.get(local_id)
            if row is None:
                continue
            accepted['local_only_images'].append({
                'side': 'local_only', 'kind': 'image',
                'local_id': local_id, 'cloud_id': None,
                'owning_local_image_id': None, 'owning_cloud_image_id': None,
                'fingerprint': _asymmetry_fingerprint_local_image(row),
                'accepted_at': _iso_timestamp_now(),
                'choice': 'keep_local',
            })
        elif kind == 'image' and choice == 'keep_cloud':
            cloud_id = str(item.get('cloud_id') or '').strip()
            row = remote_image_by_id.get(cloud_id)
            if row is None:
                continue
            accepted['cloud_only_images'].append({
                'side': 'cloud_only', 'kind': 'image',
                'local_id': None, 'cloud_id': cloud_id,
                'owning_local_image_id': None, 'owning_cloud_image_id': None,
                'fingerprint': _asymmetry_fingerprint_remote_image(row),
                'accepted_at': _iso_timestamp_now(),
                'choice': 'keep_cloud',
            })
        elif kind == 'measurement' and choice == 'keep_local':
            local_id = _safe_int(item.get('local_id'))
            row = local_meas_by_id.get(local_id)
            if row is None:
                continue
            accepted['local_only_measurements'].append({
                'side': 'local_only', 'kind': 'measurement',
                'local_id': local_id, 'cloud_id': None,
                'owning_local_image_id': _safe_int(row.get('image_id')) or None,
                'owning_cloud_image_id': None,
                'fingerprint': _asymmetry_fingerprint_local_measurement(row),
                'accepted_at': _iso_timestamp_now(),
                'choice': 'keep_local',
            })
        elif kind == 'measurement' and choice == 'keep_cloud':
            cloud_id = str(item.get('cloud_id') or '').strip()
            row = remote_meas_by_id.get(cloud_id)
            if row is None:
                continue
            accepted['cloud_only_measurements'].append({
                'side': 'cloud_only', 'kind': 'measurement',
                'local_id': None, 'cloud_id': cloud_id,
                'owning_local_image_id': None,
                'owning_cloud_image_id': str(row.get('image_id') or '').strip() or None,
                'fingerprint': _asymmetry_fingerprint_remote_measurement(row),
                'accepted_at': _iso_timestamp_now(),
                'choice': 'keep_cloud',
            })
    return accepted


def _iso_timestamp_now() -> str:
    """UTC timestamp for accepted_at diagnostics.  Never used as identity."""
    try:
        from datetime import datetime, timezone
        return datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return ''


def _mosaic_render_state_unverified(
    *,
    local_images: list[dict],
    remote_images: list[dict],
    local_measurements: list[dict],
    remote_measurements: list[dict],
) -> bool:
    """True when some mosaic-eligible measurement's geometry, or its owning
    image's render scale or image_type, cannot be proven equal to the
    cloud-approved state.

    Mirrors ``_push_spore_mosaic_for_observation``'s own eligibility query
    (~23100 onward): a cloud-linked microscope image and a cloud-linked
    measurement are both required for a tile. ``merged_asymmetry`` only
    tracks *accepted* local-only/cloud-only entries, so a matched local/cloud
    pair that a plan's automatic items never addressed (a genuine two-sided
    divergence left for manual review) would otherwise be invisible here —
    checking it directly against the reconciled state closes that gap without
    a second renderer or a new persisted eligibility format.
    """
    local_images_by_id = {
        _safe_int(row.get('id')): row
        for row in (local_images or []) if _safe_int(row.get('id'))
    }
    remote_images_by_cloud_id = {
        str(row.get('id') or '').strip(): row
        for row in (remote_images or []) if str(row.get('id') or '').strip()
    }
    remote_measurements_by_cloud_id = {
        str(row.get('id') or '').strip(): row
        for row in (remote_measurements or []) if str(row.get('id') or '').strip()
    }
    for measurement in local_measurements or []:
        measurement_cloud_id = str(measurement.get('cloud_id') or '').strip()
        if not measurement_cloud_id:
            continue
        image = local_images_by_id.get(_safe_int(measurement.get('image_id')))
        if not image or str(image.get('image_type') or '').strip() != 'microscope':
            continue
        image_cloud_id = str(image.get('cloud_id') or '').strip()
        if not image_cloud_id:
            continue
        remote_measurement = remote_measurements_by_cloud_id.get(measurement_cloud_id)
        if remote_measurement is None:
            return True
        if not _measurement_payloads_match(
            measurement, remote_measurement, cloud_image_id=image_cloud_id,
        ):
            return True
        remote_image = remote_images_by_cloud_id.get(image_cloud_id)
        if remote_image is None:
            return True
        if str(remote_image.get('image_type') or '').strip() != 'microscope':
            # The local image is microscope (selected by the query above),
            # but the cloud-approved image_type disagrees. The mosaic SQL
            # (~23190) selects purely on the *local* image_type, so an
            # unresolved local/cloud image_type divergence would otherwise
            # let this measurement's tile render from a classification the
            # cloud side has not agreed to.
            return True
        for field in ('scale_microns_per_pixel', 'resample_scale_factor'):
            local_value = _normalize_measurement_float_value(image.get(field))
            remote_value = _normalize_measurement_float_value(remote_image.get(field))
            if local_value is None or remote_value is None:
                if local_value != remote_value:
                    return True
                continue
            if not math.isclose(
                local_value, remote_value,
                rel_tol=_MEASUREMENT_FLOAT_REL_TOL, abs_tol=_MEASUREMENT_FLOAT_ABS_TOL,
            ):
                return True
    return False
