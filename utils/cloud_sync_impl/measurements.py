"""Measurement push: the remote measurement identity cache, per-observation
measurement push and the remote measurement verification schedule.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction, except that ``_push_measurements_for_observation``
sets ``sqlite3.Row`` instead of ``__import__('sqlite3').Row`` (the same object):
owners may not use dynamic import machinery.
"""

from __future__ import annotations

import sqlite3

from database.schema import get_app_settings, get_connection

from utils.cloud_sync_impl.anchors import (
    _ensure_metadata_anchors_for_public_spore_observation,
)
from utils.cloud_sync_impl.common import (
    _CLOUD_SYNC_IN_BATCH_SIZE,
    _join_select_columns,
    _safe_int,
)
from utils.cloud_sync_impl.errors import (
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.image_identity import _finalize_portable_cloud_identity_guard
from utils.cloud_sync_impl.progress import _cloud_sync_perf_counter
from utils.cloud_sync_impl.tombstones import _local_tombstoned_cloud_image_ids


_CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING = 'cloud_measurement_reconcile_version'


_CLOUD_MEASUREMENT_RECONCILE_VERSION = 1


def _cloud_measurement_remote_verification_due(
    *,
    full_pull: bool,
    child_safety_pull: bool,
    safety_pull_due: bool,
) -> tuple[bool, str]:
    """Select deep stamped-ID verification without weakening local repair."""
    if full_pull:
        return True, 'full_pull'

    settings = get_app_settings()
    stored_version = _safe_int(settings.get(_CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING))
    if stored_version != _CLOUD_MEASUREMENT_RECONCILE_VERSION:
        return True, f'version_{stored_version}_to_{_CLOUD_MEASUREMENT_RECONCILE_VERSION}'

    if child_safety_pull and safety_pull_due:
        return True, 'child_safety_pull'
    return False, 'fresh_watermark'


_SPORE_MEASUREMENT_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'image_id',
    'length_um',
    'width_um',
    'measurement_type',
    'gallery_rotation',
    'p1_x',
    'p1_y',
    'p2_x',
    'p2_y',
    'p3_x',
    'p3_y',
    'p4_x',
    'p4_y',
    'measured_at',
    'image_key',
    'thumb_key',
)


def _build_remote_measurement_identity_cache(remote_measurements: list[dict] | None) -> dict[str, dict]:
    cache: dict[str, dict] = {}
    for row in remote_measurements or []:
        remote_row = dict(row or {})
        cloud_id = str(remote_row.get('id') or '').strip()
        if cloud_id:
            cache[f'cloud:{cloud_id}'] = remote_row
        desktop_id = _safe_int(remote_row.get('desktop_id'))
        if desktop_id > 0:
            cache[f'desktop:{desktop_id}'] = remote_row
    return cache


def _measurement_push_lookup_keys(measurement_row: dict | None) -> list[str]:
    row = dict(measurement_row or {})
    keys: list[str] = []
    cloud_id = str(row.get('cloud_id') or '').strip()
    if cloud_id:
        keys.append(f'cloud:{cloud_id}')
    desktop_id = _safe_int(row.get('id'))
    if desktop_id > 0:
        keys.append(f'desktop:{desktop_id}')
    return keys


def fetch_remote_measurement_identity_cache(
    client,
    image_cloud_ids: list[str],
) -> dict[str, dict]:
    fetcher = getattr(client, 'pull_measurements_for_images', None)
    if not callable(fetcher):
        return {}
    remote_measurements = fetcher(image_cloud_ids)
    return _build_remote_measurement_identity_cache(remote_measurements)


def _push_measurements_for_observation(
    client: SporelyCloudClient,
    obs_local_id: int,
    measurement_ids: set[int] | None = None,
) -> None:
    """Push all spore measurements for an observation's microscope images to the cloud.

    Only images that have a cloud_id (i.e. have been synced) are included.
    Measurements are upserted by desktop_id; stale cloud rows for images that
    still exist locally are cleaned up.
    """
    push_start = _cloud_sync_perf_counter()
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        observation_row = conn.execute(
            'SELECT * FROM observations WHERE id = ?',
            (int(obs_local_id),),
        ).fetchone()
    finally:
        conn.close()
    if observation_row is not None:
        observation = dict(observation_row)
        _ensure_metadata_anchors_for_public_spore_observation(
            client,
            observation,
            int(obs_local_id),
            str(observation.get('cloud_id') or '').strip(),
        )

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT m.id, m.image_id, m.length_um, m.width_um, m.measurement_type,
                   m.gallery_rotation,
                   m.p1_x, m.p1_y, m.p2_x, m.p2_y,
                   m.p3_x, m.p3_y, m.p4_x, m.p4_y,
                   m.measured_at, m.cloud_id,
                   i.cloud_id AS image_cloud_id
            FROM spore_measurements m
            JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ?
              AND i.image_type = 'microscope'
              AND i.cloud_id IS NOT NULL
            ORDER BY m.id
            ''',
            (obs_local_id,),
        )
        measurements = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    if measurement_ids is not None:
        selected_ids = {_safe_int(value) for value in measurement_ids if _safe_int(value) > 0}
        measurements = [
            row for row in measurements if _safe_int(row.get('id')) in selected_ids
        ]

    portable_identity_pending = bool(
        dict(observation_row).get('portable_cloud_identity_pending')
        if observation_row is not None else False
    )
    for measurement in measurements:
        measurement['portable_cloud_identity_pending'] = portable_identity_pending

    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [
            str(row.get('image_cloud_id') or '').strip()
            for row in measurements
            if str(row.get('image_cloud_id') or '').strip()
        ]
    )
    if tombstoned_cloud_ids:
        filtered_measurements: list[dict] = []
        for meas in measurements:
            cloud_image_id = str(meas.get('image_cloud_id') or '').strip()
            if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                print(
                    f'[cloud_sync] Warning: obs {int(obs_local_id)}: skipped cloud measurement '
                    f'{meas["id"]} because cloud image {cloud_image_id} has a local tombstone'
                )
                continue
            filtered_measurements.append(meas)
        measurements = filtered_measurements

    remote_measurement_cache = fetch_remote_measurement_identity_cache(
        client,
        sorted(
            {
                str(row.get('image_cloud_id') or '').strip()
                for row in measurements
                if str(row.get('image_cloud_id') or '').strip()
            }
        ),
    )

    stale_link_candidates = [
        meas
        for meas in measurements
        if str(meas.get('cloud_id') or '').strip()
        and f"cloud:{str(meas.get('cloud_id') or '').strip()}" not in remote_measurement_cache
        and f"desktop:{_safe_int(meas.get('id'))}" not in remote_measurement_cache
    ]
    getter = getattr(client, '_get', None)
    if stale_link_candidates and callable(getter):
        desktop_ids = sorted({
            _safe_int(meas.get('id'))
            for meas in stale_link_candidates
            if _safe_int(meas.get('id')) > 0
        })
        for start in range(0, len(desktop_ids), _CLOUD_SYNC_IN_BATCH_SIZE):
            chunk = desktop_ids[start:start + _CLOUD_SYNC_IN_BATCH_SIZE]
            rows = getter(
                f'spore_measurements?desktop_id=in.({",".join(str(value) for value in chunk)})'
                f'&user_id=eq.{client.user_id}'
                f'&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
            )
            remote_measurement_cache.update(
                _build_remote_measurement_identity_cache(rows)
            )

    stale_local_measurement_ids = [
        _safe_int(meas.get('id'))
        for meas in stale_link_candidates
        if f"cloud:{str(meas.get('cloud_id') or '').strip()}" not in remote_measurement_cache
        and f"desktop:{_safe_int(meas.get('id'))}" not in remote_measurement_cache
        and _safe_int(meas.get('id')) > 0
    ]
    if stale_local_measurement_ids:
        conn = get_connection()
        try:
            placeholders = ','.join('?' for _ in stale_local_measurement_ids)
            conn.execute(
                f'UPDATE spore_measurements SET cloud_id = NULL '
                f'WHERE id IN ({placeholders})',
                stale_local_measurement_ids,
            )
            conn.commit()
        finally:
            conn.close()
        stale_id_set = set(stale_local_measurement_ids)
        for meas in measurements:
            if _safe_int(meas.get('id')) in stale_id_set:
                meas['cloud_id'] = None
        print(
            f'[cloud_sync] Observation {obs_local_id}: cleared '
            f'{len(stale_local_measurement_ids)} stale local measurement cloud id(s)',
            flush=True,
        )

    pushed_cloud_ids: set[str] = set()
    local_id_stamp_failures: list[int] = []
    for meas in measurements:
        cloud_image_id = str(meas.get('image_cloud_id') or '').strip()
        if not cloud_image_id:
            continue
        try:
            cloud_meas_id = client.push_measurement(
                meas,
                cloud_image_id,
                remote_measurement_cache=remote_measurement_cache,
            )
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(f'[cloud_sync] Measurement {meas["id"]} push failed: {e}')
            continue

        pushed_cloud_ids.add(cloud_meas_id)

        # Split the local cloud_id stamp into its own try/except so a
        # local UPDATE failure becomes VISIBLE instead of getting
        # swallowed by the broad "push failed" handler above. Without
        # this, a database lock / stale connection could silently
        # prevent cloud_id from ever being persisted locally, which is
        # exactly the "reconcile keeps pushing the same measurements"
        # failure mode.
        normalized_cloud_meas_id = str(cloud_meas_id or '').strip()
        normalized_local_cloud_id = str(meas.get('cloud_id') or '').strip()
        if normalized_cloud_meas_id and normalized_local_cloud_id != normalized_cloud_meas_id:
            try:
                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE spore_measurements SET cloud_id = ? WHERE id = ?',
                        (normalized_cloud_meas_id, int(meas['id'])),
                    )
                    conn.commit()
                finally:
                    conn.close()
            except Exception as stamp_exc:
                local_id_stamp_failures.append(int(meas.get('id') or 0))
                print(
                    f'[cloud_sync] Measurement {meas["id"]} cloud_id stamp '
                    f'failed (cloud id={cloud_meas_id!r}): {stamp_exc}',
                    flush=True,
                )

    push_elapsed = _cloud_sync_perf_counter() - push_start
    stamp_failure_suffix = (
        f' local_stamp_failures={len(local_id_stamp_failures)}'
        if local_id_stamp_failures else ''
    )
    print(
        (
            f'[cloud_sync] Observation {obs_local_id}: measurement push finalized '
            f'measurements={len(measurements)} pushed={len(pushed_cloud_ids)}'
            f'{stamp_failure_suffix} '
            f'duration={push_elapsed * 1000:.0f}ms'
        ),
        flush=True,
    )
    _finalize_portable_cloud_identity_guard(client, int(obs_local_id))
