"""Location-precision confirmation, guard and legacy repair (settings and SQLite).

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from database.models import (
    ObservationDB,
    SettingsDB,
)
from database.schema import get_connection

from utils.cloud_sync_impl.baseline import (
    _load_cloud_observation_snapshot,
    _parse_cloud_observation_snapshot,
)
from utils.cloud_sync_impl.logs import logger
from utils.cloud_sync_impl.push_payloads import _baseline_observation_compare_payload
from utils.cloud_sync_impl.reconciliation.location_precision import _location_precision_rank


def _precision_local_id(value) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _confirmed_location_precision_key(local_id) -> str:
    return f'cloud_location_precision_confirmed:{int(local_id)}'


def record_confirmed_location_precision(local_id, precision: str | None) -> None:
    """Record that the user explicitly chose ``precision`` for this local
    observation (after the publish notice when that applies)."""
    if _precision_local_id(local_id) is None or not precision:
        return
    SettingsDB.set_setting(
        _confirmed_location_precision_key(local_id),
        ObservationDB._normalize_location_precision(precision),
    )


def _confirmed_location_precision(local_id) -> str | None:
    if _precision_local_id(local_id) is None:
        return None
    value = str(SettingsDB.get_setting(_confirmed_location_precision_key(local_id), '') or '').strip()
    return value or None


def consume_confirmed_location_precision(local_id) -> None:
    if _precision_local_id(local_id) is not None:
        SettingsDB.set_setting(_confirmed_location_precision_key(local_id), '')


def _guard_local_location_precision(local_obs: dict | None, *references: dict | None) -> dict:
    """Copy of ``local_obs`` whose precision never exceeds the least precise
    known cloud/baseline value, unless the user confirmed that exact value."""
    obs = dict(local_obs or {})
    known = [
        ref.get('location_precision') for ref in references
        if isinstance(ref, dict) and str(ref.get('location_precision') or '').strip()
    ]
    if not known:
        return obs
    reference = min(known, key=_location_precision_rank)
    local_value = ObservationDB._normalize_location_precision(obs.get('location_precision'))
    if _location_precision_rank(local_value) <= _location_precision_rank(reference):
        return obs
    if _confirmed_location_precision(obs.get('id')) == local_value:
        return obs
    logger.info(
        'Keeping cloud location_precision %r for observation %s: local %r was not confirmed',
        reference, obs.get('id'), local_value,
    )
    obs['location_precision'] = ObservationDB._normalize_location_precision(reference)
    return obs


def _snapshot_baseline_for_cloud_id(cloud_id) -> dict:
    cloud_value = str(cloud_id or '').strip()
    if not cloud_value:
        return {}
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
    return _baseline_observation_compare_payload(snapshot.get('observation') or {})


_LOCATION_PRECISION_REPAIR_DONE_KEY = 'cloud_location_precision_repair_v1_done'


def repair_legacy_location_precision(*, force: bool = False) -> int:
    """Once per database: restore 'hidden'/'region' on synced rows that an
    older build stored as 'exact'. Only rows with a cloud_id, an 'exact' (or
    empty) local value, a 'hidden'/'region' baseline, and no confirmed local
    choice. Local-only write; does not mark the row dirty.

    A marker in this database's settings records completion, so later calls
    (app start, every sync) return at once. New legacy rows cannot appear:
    this build stores pulled hidden/region values as they are. A restored or
    switched database has no marker and is repaired on its first call.
    """
    if not force and str(SettingsDB.get_setting(_LOCATION_PRECISION_REPAIR_DONE_KEY, '') or '') == '1':
        return 0
    conn = get_connection()
    repaired = 0
    try:
        rows = conn.execute(
            "SELECT id, cloud_id, location_precision FROM observations "
            "WHERE cloud_id IS NOT NULL AND TRIM(cloud_id) != '' "
            "AND (location_precision IS NULL OR LOWER(TRIM(location_precision)) IN ('', 'exact'))"
        ).fetchall()
        for row in rows:
            local_id, cloud_id = row[0], row[1]
            baseline = _snapshot_baseline_for_cloud_id(cloud_id)
            target = str(baseline.get('location_precision') or '').strip().lower()
            if target not in {'hidden', 'region'}:
                continue
            if _confirmed_location_precision(local_id) == 'exact':
                continue
            conn.execute(
                "UPDATE observations SET location_precision = ? WHERE id = ?",
                (target, int(local_id)),
            )
            repaired += 1
        if repaired:
            conn.commit()
    finally:
        conn.close()
    SettingsDB.set_setting(_LOCATION_PRECISION_REPAIR_DONE_KEY, '1')
    if repaired:
        logger.info('Restored hidden/region location_precision on %d observation(s)', repaired)
    return repaired
