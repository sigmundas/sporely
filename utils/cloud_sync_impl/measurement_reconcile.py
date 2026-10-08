"""Reconciliation of spore measurements missing from the cloud.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction, except that ``_reconcile_missing_spore_measurements``
sets ``sqlite3.Row`` instead of ``__import__('sqlite3').Row`` (the same object):
owners may not use dynamic import machinery.
"""

from __future__ import annotations

import sqlite3

from database.models import mark_observation_sync_dirty
from database.schema import get_connection

from utils.cloud_sync_impl.common import _CLOUD_SYNC_IN_BATCH_SIZE
from utils.cloud_sync_impl.errors import (
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.measurements import _push_measurements_for_observation
from utils.cloud_sync_impl.progress import _cloud_sync_perf_counter
from utils.cloud_sync_impl.spore_mosaic import _push_spore_mosaic_for_observation


def _reconcile_missing_spore_measurements(
    client: SporelyCloudClient,
    errors: list[str],
    *,
    verify_stamped_remote: bool = True,
) -> dict[str, int]:
    """Backfill raw `public.spore_measurements` rows for observations
    that were synced pre-Stage-D or otherwise skipped the measurement
    push loop.

    The main ``push_all`` loop only visits observations where
    `cloud_id IS NULL OR sync_status = 'dirty'`. `_push_measurements_
    for_observation` runs inside that loop; an observation that was
    once synced but never re-dirtied can end up with LOCAL measurements
    that never made it to cloud `spore_measurements`. The summary
    writer counts local measurements — so a summary row can advertise
    n_paired = 29 while the cloud raw table only exposes 20 through
    the public spore RPCs, and the two disagree until this pass runs.

    We always target observations with an unstamped local measurement. When
    ``verify_stamped_remote`` is enabled by a version upgrade, child-safety
    pass, or explicit recovery pull, stamped cloud ids are also checked for
    remote deletion. This keeps ordinary sync proportional to local pending
    work without losing periodic recovery from recreated cloud datasets.

    `_push_measurements_for_observation` is per-measurement idempotent:
    each row is either PATCHed (only if its payload differs from the
    remote) or POSTed if missing. Fully-covered observations therefore
    also survive an accidental extra call cheaply, but we still filter
    them out here so the reconciliation pass stays fast in steady
    state.

    Returns a small counter dict for the sync result. Errors from
    individual observations are appended to `errors` per the surrounding
    convention; auth/temporary errors propagate.
    """
    reconcile_start = _cloud_sync_perf_counter()
    counters = {'candidates': 0, 'attempted': 0}
    print('[cloud_sync] spore measurement reconciliation: start', flush=True)

    local_query_start = _cloud_sync_perf_counter()
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        # The reconciliation source query MUST mirror the filter
        # used by `_push_measurements_for_observation` exactly. If we
        # flag observations whose only "missing" measurements are
        # attached to non-microscope images or to tombstoned images,
        # the push helper refuses to push them, `spore_measurements
        # .cloud_id` stays NULL, and we re-select the same observation
        # every sync — the "measurements=22 pushed=22 but reconcile
        # picks it up again next sync" bug from the field log.
        cursor.execute(
            '''
            SELECT o.id AS local_id,
                   o.cloud_id AS observation_cloud_id,
                   i.id AS image_local_id,
                   i.cloud_id AS image_cloud_id,
                   m.id AS measurement_local_id,
                   m.cloud_id AS measurement_cloud_id
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            JOIN spore_measurements m ON m.image_id = i.id
            LEFT JOIN image_tombstones t ON t.deleted_cloud_id = i.cloud_id
            WHERE o.cloud_id IS NOT NULL
              AND trim(o.cloud_id) != ''
              AND i.cloud_id IS NOT NULL
              AND trim(i.cloud_id) != ''
              AND i.image_type = 'microscope'
              AND t.id IS NULL
            ORDER BY o.id, m.id
            '''
        )
        eligible_rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    print(
        f'[cloud_sync] spore measurement reconciliation: local candidate scan complete '
        f'eligible_rows={len(eligible_rows)} '
        f'duration={(_cloud_sync_perf_counter() - local_query_start) * 1000:.0f}ms',
        flush=True,
    )

    candidate_ids = {
        int(row['local_id'])
        for row in eligible_rows
        if not str(row.get('measurement_cloud_id') or '').strip()
    }

    # Verify stamped ids with narrow ID-only requests. Pulling full remote
    # measurement payloads here was the source of the old no-op Refresh
    # slowdown; existence is all reconciliation needs.
    stamped_ids = sorted({
        str(row.get('measurement_cloud_id') or '').strip()
        for row in eligible_rows
        if str(row.get('measurement_cloud_id') or '').strip()
    })
    remote_ids: set[str] = set()
    deleted_image_ids: set[str] = set()
    getter = getattr(client, '_get', None)
    measurement_requests = 0
    measurement_rows = 0
    image_requests = 0
    image_rows_count = 0
    if stamped_ids and callable(getter) and verify_stamped_remote:
        measurement_verify_start = _cloud_sync_perf_counter()
        for start in range(0, len(stamped_ids), _CLOUD_SYNC_IN_BATCH_SIZE):
            chunk = stamped_ids[start:start + _CLOUD_SYNC_IN_BATCH_SIZE]
            rows = getter(
                f'spore_measurements?id=in.({",".join(chunk)})'
                f'&user_id=eq.{client.user_id}&select=id'
            )
            measurement_requests += 1
            measurement_rows += len(rows or [])
            remote_ids.update(
                str(row.get('id') or '').strip()
                for row in rows or []
                if str(row.get('id') or '').strip()
            )
        candidate_ids.update(
            int(row['local_id'])
            for row in eligible_rows
            if str(row.get('measurement_cloud_id') or '').strip() not in remote_ids
        )
        print(
            f'[cloud_sync] spore measurement reconciliation: stamped measurement verification complete '
            f'ids={len(stamped_ids)} requests={measurement_requests} rows={measurement_rows} '
            f'duration={(_cloud_sync_perf_counter() - measurement_verify_start) * 1000:.0f}ms',
            flush=True,
        )

        image_cloud_ids = sorted({
            str(row.get('image_cloud_id') or '').strip()
            for row in eligible_rows
            if str(row.get('image_cloud_id') or '').strip()
        })
        image_verify_start = _cloud_sync_perf_counter()
        for start in range(0, len(image_cloud_ids), _CLOUD_SYNC_IN_BATCH_SIZE):
            chunk = image_cloud_ids[start:start + _CLOUD_SYNC_IN_BATCH_SIZE]
            image_rows = getter(
                f'observation_images?id=in.({",".join(chunk)})'
                f'&user_id=eq.{client.user_id}&select=id,deleted_at,purged_at'
            )
            image_requests += 1
            image_rows_count += len(image_rows or [])
            deleted_image_ids.update(
                str(row.get('id') or '').strip()
                for row in image_rows or []
                if str(row.get('id') or '').strip()
                and str(row.get('deleted_at') or '').strip()
                and not str(row.get('purged_at') or '').strip()
            )
        candidate_ids.update(
            int(row['local_id'])
            for row in eligible_rows
            if str(row.get('image_cloud_id') or '').strip() in deleted_image_ids
        )
        print(
            f'[cloud_sync] spore measurement reconciliation: cloud image verification complete '
            f'ids={len(image_cloud_ids)} requests={image_requests} rows={image_rows_count} '
            f'duration={(_cloud_sync_perf_counter() - image_verify_start) * 1000:.0f}ms',
            flush=True,
        )
    elif stamped_ids and not verify_stamped_remote:
        print(
            f'[cloud_sync] spore measurement reconciliation: stamped remote verification skipped '
            f'ids={len(stamped_ids)} reason=periodic_verification_not_due',
            flush=True,
        )

    candidates = sorted(candidate_ids)
    observation_cloud_ids = {
        int(row['local_id']): str(row.get('observation_cloud_id') or '').strip()
        for row in eligible_rows
        if str(row.get('observation_cloud_id') or '').strip()
    }
    counters['candidates'] = len(candidates)
    if not candidates:
        print(
            f'[cloud_sync] spore measurement reconciliation: complete '
            f'candidates=0 attempted=0 duration='
            f'{(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
            flush=True,
        )
        return counters

    print(
        f'[cloud_sync] Spore measurement reconciliation: '
        f'{len(candidates)} synced observations have missing remote '
        f'measurements; backfilling public.spore_measurements.',
        flush=True,
    )

    for local_id in candidates:
        if local_id <= 0:
            continue
        counters['attempted'] += 1
        try:
            # Restore active local microscope anchors before reconciling their
            # measurements. Existing measurement rows keep their image FKs.
            restore_ids = sorted({
                str(row.get('image_cloud_id') or '').strip()
                for row in eligible_rows
                if int(row['local_id']) == local_id
                and str(row.get('image_cloud_id') or '').strip() in deleted_image_ids
            })
            for image_cloud_id in restore_ids:
                client._patch(
                    f'observation_images?id=eq.{image_cloud_id}'
                    f'&user_id=eq.{client.user_id}',
                    {'deleted_at': None},
                )
            _push_measurements_for_observation(client, local_id)
            cloud_observation_id = observation_cloud_ids.get(local_id, '')
            if cloud_observation_id:
                _push_spore_mosaic_for_observation(
                    client,
                    local_id,
                    cloud_observation_id,
                )
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(
                f'obs {local_id}: measurement reconciliation failed: {exc}'
            )
            try:
                mark_observation_sync_dirty(local_id)
            except Exception:
                pass
    print(
        f'[cloud_sync] spore measurement reconciliation: complete '
        f'candidates={counters["candidates"]} attempted={counters["attempted"]} '
        f'duration={(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
        flush=True,
    )
    return counters
