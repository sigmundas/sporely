"""The snapshot baseline: codec, settings load/store/clear, and storing the
remote snapshot after sync.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json

from database.models import (
    ImageDB,
    MeasurementDB,
    SettingsDB,
)
from database.schema import get_connection

from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.image_policy import should_pull_cloud_image_to_desktop
from utils.cloud_sync_impl.progress import _cloud_sync_current_profiler
from utils.cloud_sync_impl.reconciliation.asymmetry import _reconcile_accepted_asymmetry
from utils.cloud_sync_impl.reconciliation.identity import (
    _remote_identity_claim,
    TAXON_IDENTITY_SYNC_FIELD,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
    _normalize_snapshot_value,
    _SNAPSHOT_IMG_FIELDS,
    _SNAPSHOT_IMG_PASSIVE_FIELDS,
    _SNAPSHOT_MEAS_FIELDS,
    _SNAPSHOT_OBS_FIELDS,
)
from utils.cloud_sync_impl.remote_reads import _pull_remote_measurements_for_images


_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX = "sporely_cloud_snapshot_obs_"


def _parse_cloud_observation_snapshot(snapshot: str | None) -> dict:
    """Parse a snapshot payload.

    Backward-compatible across both schema versions:

    * older snapshots without ``schema_version`` continue to load; no
      accepted-asymmetry is invented for them (the section is absent);
    * schema-2 payloads preserve the ``schema_version`` and
      ``accepted_asymmetry`` sections verbatim.
    """
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
    # Preserve accepted_asymmetry when present.  When absent (pre-B3 snapshots)
    # leave the key unset — do NOT synthesize acceptance.
    asymmetry = data.get('accepted_asymmetry')
    if isinstance(asymmetry, dict):
        data['accepted_asymmetry'] = {
            key: [dict(entry) for entry in asymmetry.get(key) or [] if isinstance(entry, dict)]
            for key in (
                'local_only_images',
                'cloud_only_images',
                'local_only_measurements',
                'cloud_only_measurements',
            )
        }
    return data


def _cloud_observation_snapshot_key(cloud_id: str) -> str:
    return f"{_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX}{str(cloud_id or '').strip()}"


_CLOUD_OBSERVATION_SNAPSHOT_SCHEMA_VERSION = 2


def _cloud_observation_snapshot(
    remote: dict,
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None = None,
    *,
    include_images: bool = True,
    include_measurements: bool = True,
    accepted_asymmetry: dict | None = None,
) -> str:
    """Deterministic snapshot payload.

    Turn-B B3: adds an optional ``accepted_asymmetry`` section that records
    intentionally one-sided items the user chose to keep.  Older callers that
    do not pass it still produce a schema-2 payload with an empty asymmetry
    section (kept backward-compatible on the read side by
    ``_parse_cloud_observation_snapshot``).
    """
    obs_part = {
        field: _normalize_snapshot_value((remote or {}).get(field))
        for field in _SNAPSHOT_OBS_FIELDS
    }
    identity_claim = _remote_identity_claim(remote)
    if identity_claim is not None:
        obs_part[TAXON_IDENTITY_SYNC_FIELD] = identity_claim.key
    payload: dict = {
        'schema_version': _CLOUD_OBSERVATION_SNAPSHOT_SCHEMA_VERSION,
        'observation': obs_part,
    }
    if include_images:
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
            image_payload['captured_at'] = _normalize_image_captured_at_for_cloud(
                image.get('captured_at'), local=False
            )
            for field in _SNAPSHOT_IMG_PASSIVE_FIELDS:
                passive_value = _normalize_cloud_media_key(image.get(field))
                if passive_value:
                    image_payload[field] = _normalize_snapshot_value(passive_value)
            images_part.append(image_payload)
        payload['images'] = images_part
    if include_measurements:
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
        payload['measurements'] = measurements_part
    # ── Accepted-asymmetry section ────────────────────────────────────────
    # Only include the section when meaningful; older readers ignore it.
    if accepted_asymmetry:
        payload['accepted_asymmetry'] = _normalize_accepted_asymmetry_for_snapshot(
            accepted_asymmetry
        )
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _normalize_accepted_asymmetry_for_snapshot(raw: dict | None) -> dict:
    """Canonicalize the accepted-asymmetry section for stable serialization.

    Each collection is a list of entries; each entry always carries:

    * ``side``: ``"local_only"`` or ``"cloud_only"``;
    * ``kind``: ``"image"`` or ``"measurement"``;
    * ``local_id`` and ``cloud_id`` (one of them will be null);
    * ``owning_local_image_id`` / ``owning_cloud_image_id`` for measurements;
    * ``fingerprint``: normalized content fingerprint at acceptance time;
    * ``accepted_at``: ISO timestamp (diagnostic only; never a privacy path);
    * ``choice``: ``"keep_local"`` or ``"keep_cloud"``.

    No secrets, tokens, or filesystem paths are stored.
    """
    if not isinstance(raw, dict):
        return {
            'local_only_images': [],
            'cloud_only_images': [],
            'local_only_measurements': [],
            'cloud_only_measurements': [],
        }
    result = {
        'local_only_images': [],
        'cloud_only_images': [],
        'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    for key in result.keys():
        for entry in raw.get(key) or []:
            if not isinstance(entry, dict):
                continue
            result[key].append(_normalize_accepted_asymmetry_entry(entry))
        # Stable sort so snapshots round-trip byte-for-byte.
        result[key].sort(key=lambda e: (
            str(e.get('local_id') or ''),
            str(e.get('cloud_id') or ''),
        ))
    return result


def _normalize_accepted_asymmetry_entry(entry: dict) -> dict:
    return {
        'side': str(entry.get('side') or ''),
        'kind': str(entry.get('kind') or ''),
        'local_id': _safe_int(entry.get('local_id')) or None,
        'cloud_id': str(entry.get('cloud_id') or '').strip() or None,
        'owning_local_image_id': _safe_int(entry.get('owning_local_image_id')) or None,
        'owning_cloud_image_id': str(entry.get('owning_cloud_image_id') or '').strip() or None,
        'fingerprint': entry.get('fingerprint') if isinstance(entry.get('fingerprint'), dict) else {},
        'accepted_at': str(entry.get('accepted_at') or '').strip() or None,
        'choice': str(entry.get('choice') or '').strip() or None,
    }


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


def _clear_cloud_observation_snapshot(cloud_id: str) -> None:
    if not str(cloud_id or '').strip():
        return
    SettingsDB.set_setting(_cloud_observation_snapshot_key(cloud_id), '')


def _local_observation_id_by_cloud_id(cloud_id: str) -> int:
    """Resolve local observation id from cloud id via SQL, best-effort."""
    text = str(cloud_id or '').strip()
    if not text:
        return 0
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM observations WHERE cloud_id = ? LIMIT 1",
            (text,),
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])
    except Exception:
        return 0
    return 0


def _store_remote_snapshot(
    client: "SporelyCloudClient",
    cloud_id: str,
    remote: dict | None = None,
    remote_images: list[dict] | None = None,
    remote_measurements: list[dict] | None = None,
    *,
    include_images: bool = True,
    include_measurements: bool = True,
    accepted_asymmetry: dict | None = None,
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
            profiler.record_store_remote_snapshot_fetch(images=include_images and remote_images is None)
            profiler.record_store_remote_snapshot_fetch(
                measurements=include_measurements and remote_measurements is None
            )
        except Exception:
            pass
    if include_images:
        images = (
            [dict(row or {}) for row in (remote_images or [])]
            if remote_images is not None
            else [dict(row or {}) for row in (client.pull_image_metadata(cloud_value) or [])]
        )
    else:
        images = []
    if include_measurements:
        if remote_measurements is not None:
            measurements = [dict(row or {}) for row in remote_measurements]
        else:
            measurements = list(_pull_remote_measurements_for_images(
                client,
                [str(row.get('id') or '').strip() for row in images if str(row.get('id') or '').strip()],
            ))
    else:
        measurements = []
    # ── B3 + final Fix 2: reconcile accepted-asymmetry on every schema-2
    # snapshot write.  Ordinary sync callers do NOT supply
    # ``accepted_asymmetry`` — they invoke this helper to persist a fresh
    # baseline.  We must never create new acceptance in that path, but we
    # MUST prune existing entries whose stable identity is no longer one-sided
    # or no longer exists.  Only the plan resolver passes a non-None
    # ``accepted_asymmetry`` (containing new / replacement entries) — those
    # additions come from ``_reconcile_accepted_asymmetry`` upstream and are
    # respected verbatim here.
    previous = _parse_cloud_observation_snapshot(
        _load_cloud_observation_snapshot(cloud_value)
    )
    prev_asym = previous.get('accepted_asymmetry')
    additions_from_caller = accepted_asymmetry if isinstance(accepted_asymmetry, dict) else None
    if additions_from_caller is not None or isinstance(prev_asym, dict):
        # Best-effort resolve local observation id from cloud id so we can
        # apply local-side pruning rules (row deleted / relinked).  If we
        # cannot resolve it, we degrade gracefully to remote-only pruning.
        local_observation_id = _local_observation_id_by_cloud_id(cloud_value)
        current_local_images: list[dict] = []
        current_local_measurements: list[dict] = []
        if local_observation_id:
            try:
                current_local_images = ImageDB.get_images_for_observation(int(local_observation_id))
            except Exception:
                current_local_images = []
            try:
                current_local_measurements = MeasurementDB.get_measurements_for_observation(
                    int(local_observation_id)
                )
            except Exception:
                current_local_measurements = []
        # Determine matched IDs (counterpart appearance) from remote state.
        matched_local_image_ids = {
            _safe_int(row.get('desktop_id'))
            for row in images if _safe_int(row.get('desktop_id'))
        } | {
            _safe_int(row.get('id')) for row in current_local_images
            if str(row.get('cloud_id') or '').strip()
        }
        matched_cloud_image_ids = {
            str(row.get('cloud_id') or '').strip()
            for row in current_local_images
            if str(row.get('cloud_id') or '').strip()
        }
        matched_local_measurement_ids = {
            _safe_int(row.get('desktop_id'))
            for row in measurements if _safe_int(row.get('desktop_id'))
        } | {
            _safe_int(row.get('id')) for row in current_local_measurements
            if str(row.get('cloud_id') or '').strip()
        }
        matched_cloud_measurement_ids = {
            str(row.get('cloud_id') or '').strip()
            for row in current_local_measurements
            if str(row.get('cloud_id') or '').strip()
        }
        # Ordinary sync path: additions_from_caller is None → we only prune.
        # Plan-resolver path: additions_from_caller carries new entries → they
        # are added on top of the pruned survivors.
        new_entries_source = additions_from_caller if additions_from_caller is not None else {
            'local_only_images': [],
            'cloud_only_images': [],
            'local_only_measurements': [],
            'cloud_only_measurements': [],
        }
        reconciled = _reconcile_accepted_asymmetry(
            prev_asym, new_entries_source,
            plan_items=[],  # ordinary sync has no plan; overrides come from additions_from_caller only
            current_local_images=current_local_images,
            current_remote_images=images,
            current_local_measurements=current_local_measurements,
            current_remote_measurements=measurements,
            matched_local_image_ids=matched_local_image_ids,
            matched_cloud_image_ids=matched_cloud_image_ids,
            matched_local_measurement_ids=matched_local_measurement_ids,
            matched_cloud_measurement_ids=matched_cloud_measurement_ids,
        )
        # If reconciliation produced nothing meaningful, drop the section
        # entirely (empty dict) so the snapshot stays lean.
        if any(reconciled.get(key) for key in (
            'local_only_images', 'cloud_only_images',
            'local_only_measurements', 'cloud_only_measurements',
        )):
            accepted_asymmetry = reconciled
        else:
            accepted_asymmetry = None
    _store_cloud_observation_snapshot(
        cloud_value,
        _cloud_observation_snapshot(
            remote_obs,
            images if include_images else None,
            measurements if include_measurements else None,
            include_images=include_images,
            include_measurements=include_measurements,
            accepted_asymmetry=accepted_asymmetry,
        ),
    )
