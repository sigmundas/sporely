"""Remote side of metadata-only microscope anchors: ensure, owner-sync
parents, retirement, tombstone cancellation and anchor promotion helpers.

The local pull-side anchor insert stays with image pull and materialization.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S5 of the cloud-sync extraction.
"""

from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from database.models import ImageDB, get_image_tombstones_by_local_image_id
from database.schema import get_connection
from utils.r2_storage import media_variant_key

from utils.cloud_sync_impl.capabilities import (
    METADATA_PURPOSE_OWNER_SYNC,
    METADATA_PURPOSE_PUBLIC_MICROSCOPY,
    _owner_sync_parents_supported,
    _remote_metadata_purpose,
)
from utils.cloud_sync_impl.common import _normalize_cloud_media_key, _safe_int
from utils.cloud_sync_impl.errors import (
    CloudSyncError,
    ImageIdentityConflictError,
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.image_identity import _reconcile_local_image_cloud_id
from utils.cloud_sync_impl.image_policy import (
    _cloud_explicit_media_upload_selection,
    _cloud_image_storage_excluded_image_ids,
    cloud_image_bytes_desired,
    microscope_image_requires_owner_sync_anchor,
    microscope_image_requires_public_spore_anchor,
)
from utils.cloud_sync_impl.preflight import _image_calibration_uuid
from utils.cloud_sync_impl.reconciliation.values import _normalize_snapshot_value
from utils.cloud_sync_impl.sync_state import (
    _clear_cloud_image_file_signature,
    _clear_pending_image_promotion_key,
    _cloud_metadata_only_image_ids,
    _set_cloud_image_metadata_only_state,
    _store_pending_image_promotion_key,
)


def _reserve_anchor_promotion_key(
    client: 'SporelyCloudClient',
    observation_id: int | str,
    local_image_id: int,
    cloud_image_id: str,
    storage_path: str,
) -> str:
    """Reserve the Worker key on an existing metadata-only anchor row.

    First step of the linked-anchor → byte-backed promotion: the local image
    already has a valid ``cloud_id``, the remote row is owned and live, but
    its ``storage_path`` is NULL. Promotion keeps that cloud identity and
    binds the intended byte key to it — it never creates a second
    ``observation_images`` row.

    The local pending marker is written BEFORE the remote PATCH so an
    interruption anywhere leaves a recoverable trail:

    * crash before the PATCH → the remote row is still a clean NULL anchor;
      the marker is stale but harmless (the next attempt overwrites it).
    * crash after the PATCH → the remote row carries the reserved key with
      no bytes behind it; the marker forces the next sync to treat that
      non-NULL storage_path as unconfirmed and upload the bytes anyway.

    Raises ``CloudSyncError`` when the conditional reservation matched no
    row (the row is no longer a bare anchor — concurrent writer, or remote
    state diverged from the cached rows). The observation stays dirty and
    the next sync re-reads remote state before deciding again.
    """
    normalized_key = _normalize_cloud_media_key(storage_path)
    _store_pending_image_promotion_key(observation_id, local_image_id, normalized_key)
    if not client.reserve_image_storage_path_for_promotion(cloud_image_id, normalized_key):
        _clear_pending_image_promotion_key(observation_id, local_image_id)
        raise CloudSyncError(
            f'anchor promotion for cloud image {cloud_image_id}: reservation '
            f'matched no row (storage_path no longer NULL or row not owned); '
            f'will retry with fresh remote state on the next sync'
        )
    return normalized_key


def _rollback_anchor_promotion(
    client: 'SporelyCloudClient',
    observation_id: int | str,
    local_image_id: int,
    cloud_image_id: str,
    reserved_key: str,
) -> None:
    """Failure path of the anchor promotion: restore the clean anchor.

    Removes any partial derivative/thumb objects, then releases the
    reservation — restoring ``storage_path`` to NULL only when the row
    still carries the key reserved by this attempt. The existing
    metadata-anchor row itself is never deleted. All steps are
    best-effort: the caller re-raises the original upload error either
    way, and when the release cannot be confirmed the pending marker is
    kept so the next sync treats the lingering non-NULL storage_path as
    unconfirmed bytes instead of trusting it.
    """
    try:
        client._storage_remove([
            reserved_key,
            media_variant_key(reserved_key, 'thumb'),
        ])
    except Exception as exc:
        # Orphaned partials are overwritten by the retry (which reuses the
        # reserved key), so a failed delete only costs temporary storage.
        print(
            f'[cloud_sync] anchor promotion rollback: partial object cleanup '
            f'failed for {reserved_key}: {exc}'
        )
    try:
        released = client.release_image_storage_path_reservation(
            cloud_image_id, reserved_key,
        )
    except Exception as exc:
        print(
            f'[cloud_sync] anchor promotion rollback: could not release the '
            f'reservation for cloud image {cloud_image_id}: {exc}; keeping '
            f'the pending marker so the retry re-uploads instead of '
            f'trusting the reserved storage_path'
        )
        return
    if not released:
        print(
            f'[cloud_sync] anchor promotion rollback: cloud image '
            f'{cloud_image_id} no longer carries the reserved key '
            f'{reserved_key}; leaving remote state untouched'
        )
    _clear_pending_image_promotion_key(observation_id, local_image_id)


# Column subset that is safe to send on a metadata-only microscope image
# insert. Excludes storage_path (NULL), desktop_id/observation_id/user_id
# (set explicitly by the helper), and original_filename (derived).
_METADATA_ONLY_IMG_FIELDS = [
    'sort_order', 'image_type', 'micro_category', 'objective_name',
    'calibration_uuid',
    'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'sample_source',
    'contrast', 'measure_color',
    'crop_mode', 'notes',
    'gps_source',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h', 'ai_crop_is_custom',
]


def _metadata_only_microscope_image_payload(
    client: 'SporelyCloudClient',
    obs_cloud_id: str,
    image_row: dict,
) -> dict:
    row = dict(image_row or {})
    local_image_id = _safe_int(row.get('id'))
    payload = {
        field: row.get(field)
        for field in _METADATA_ONLY_IMG_FIELDS
        if field in row
    }
    calibration_uuid = _image_calibration_uuid(row)
    if calibration_uuid:
        payload['calibration_uuid'] = calibration_uuid
    else:
        payload.pop('calibration_uuid', None)
    payload.update({
        'image_type': 'microscope',
        'storage_path': None,
        'observation_id': obs_cloud_id,
        'user_id': client.user_id,
        'desktop_id': local_image_id,
        'original_filename': (
            str(row.get('original_filename') or '').strip()
            or Path(str(row.get('filepath') or '')).name
            or None
        ),
    })
    if payload.get('gps_source') is not None:
        payload['gps_source'] = bool(payload['gps_source'])
    if hasattr(client, '_observation_images_support_ai_crop'):
        try:
            if not client._observation_images_support_ai_crop():
                for key in (
                    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
                    'ai_crop_source_w', 'ai_crop_source_h',
                ):
                    payload.pop(key, None)
        except Exception:
            pass
    if hasattr(client, '_observation_images_support_ai_crop_custom'):
        try:
            if not client._observation_images_support_ai_crop_custom():
                payload.pop('ai_crop_is_custom', None)
        except Exception:
            pass
    return payload


def _cancel_microscope_anchor_tombstones(
    local_image_id: int,
    *cloud_image_ids: str,
) -> None:
    tombstones = get_image_tombstones_by_local_image_id([local_image_id])
    ids = {
        str(value or '').strip()
        for value in cloud_image_ids
        if str(value or '').strip()
    }
    local_tombstone = tombstones.get(int(local_image_id))
    if local_tombstone:
        local_cloud_id = str(local_tombstone.get('deleted_cloud_id') or '').strip()
        if local_cloud_id:
            ids.add(local_cloud_id)
    for cloud_image_id in ids:
        ImageDB.clear_image_tombstone_by_deleted_cloud_id(cloud_image_id)


def _remote_image_row_matches_anchor_payload(
    remote_row: dict,
    payload: dict,
    *,
    metadata_only: bool,
) -> bool:
    remote = dict(remote_row or {})
    for field in _METADATA_ONLY_IMG_FIELDS:
        if field not in payload:
            continue
        remote_value = remote.get(field)
        payload_value = payload.get(field)
        if (
            isinstance(remote_value, (int, float))
            and not isinstance(remote_value, bool)
            and isinstance(payload_value, (int, float))
            and not isinstance(payload_value, bool)
        ):
            if math.isclose(
                float(remote_value),
                float(payload_value),
                rel_tol=1e-9,
                abs_tol=1e-12,
            ):
                continue
        if _normalize_snapshot_value(remote_value) != _normalize_snapshot_value(payload_value):
            return False
    if str(remote.get('image_type') or '').strip().lower() != 'microscope':
        return False
    if _safe_int(remote.get('desktop_id')) != _safe_int(payload.get('desktop_id')):
        return False
    if str(remote.get('deleted_at') or '').strip():
        return False
    if metadata_only:
        if _normalize_cloud_media_key(remote.get('storage_path')):
            return False
        if _normalize_cloud_media_key(remote.get('original_storage_path')):
            return False
    return True


def _ensure_metadata_only_microscope_image_for_public_spores(
    client: 'SporelyCloudClient',
    obs_local_id: int,
    obs_cloud_id: str,
    image_row: dict,
    *,
    remote_images: list[dict] | None = None,
) -> str | None:
    """Create or reuse a metadata-only cloud row for a microscope image.

    The row anchors ``spore_measurements.image_id`` without uploading the
    microscope frame. It has ``storage_path = NULL`` and
    ``image_type = 'microscope'``, which the web migration
    ``20260706100000_add_metadata_only_microscope_images.sql`` explicitly
    allows and hides from public image galleries. Public sporePoints /
    mosaic tile RPCs still see the row because they don't require
    ``storage_path``.

    Contract:
    * Only microscope images qualify. Non-microscope rows are skipped —
      the cloud CHECK constraint would reject them anyway.
    * Only images that have at least one *public-eligible* spore
      measurement locally (``length_um`` and ``width_um`` present, and
      the measurement_type is one of NULL/''/'manual'/'spore'/'spores')
      get an anchor. This avoids polluting the cloud with anchors that
      can never contribute to public sporePoints.
    * Idempotent — local ``cloud_id`` values are validated against the
      current remote rows. A row matching ``desktop_id`` is reused; stale
      local ids are cleared and replaced with a repaired anchor id.
    * Never uploads image bytes. Never calls ``upload_image_file`` or
      the R2 client — the caller can guarantee "no full microscope
      source uploads" simply by using this helper.

    Returns the cloud id (str) on success, or ``None`` when the row was
    skipped. Auth / temporary-unavailable errors propagate so the
    caller can abort the whole backfill cleanly.
    """
    if not obs_cloud_id:
        print(
            f'[cloud_sync] Mosaic image metadata: skip '
            f'local_image=? obs={obs_local_id} reason=no_obs_cloud_id',
            flush=True,
        )
        return None

    row = dict(image_row or {})
    local_image_id = _safe_int(row.get('id'))
    if local_image_id <= 0:
        print(
            f'[cloud_sync] Mosaic image metadata: skip '
            f'local_image=? obs={obs_local_id} reason=no_local_id',
            flush=True,
        )
        return None

    image_type = str(row.get('image_type') or '').strip().lower()
    if image_type != 'microscope':
        print(
            f'[cloud_sync] Mosaic image metadata: skip '
            f'local_image={local_image_id} reason=not_microscope',
            flush=True,
        )
        return None

    # Two separate intents may require a parent: public spore data, and —
    # only on a server with the owner-sync capability — the owner's own
    # cross-device measurements of any type.
    public_required = microscope_image_requires_public_spore_anchor(local_image_id)
    # Probe the server only when this image actually needs an owner-sync
    # parent: observations without such images issue no extra request.
    owner_sync_supported = bool(
        not public_required
        and microscope_image_requires_owner_sync_anchor(local_image_id)
        and not (cloud_image_bytes_desired(obs_local_id, local_image_id, row) and not row.get('cloud_id'))
        and _owner_sync_parents_supported(client)
    )
    if not public_required:
        if not owner_sync_supported:
            print(
                f'[cloud_sync] Mosaic image metadata: skip '
                f'local_image={local_image_id} reason=no_public_spore_measurements',
                flush=True,
            )
            return None
        if not microscope_image_requires_owner_sync_anchor(local_image_id):
            print(
                f'[cloud_sync] Mosaic image metadata: skip '
                f'local_image={local_image_id} reason=no_measurements',
                flush=True,
            )
            return None
        # An image whose bytes the owner keeps in the cloud gets its cloud row
        # from the ordinary upload; a metadata-only parent is for images whose
        # bytes are deliberately excluded (observation 917, image 7305).
        if cloud_image_bytes_desired(obs_local_id, local_image_id, row) and not row.get('cloud_id'):
            print(
                f'[cloud_sync] Mosaic image metadata: skip '
                f'local_image={local_image_id} reason=bytes_upload_provides_parent',
                flush=True,
            )
            return None
    # For an owner-sync parent the capability is already confirmed. For a
    # public-spore parent it is resolved below, once we know whether a
    # no-byte row is involved at all.
    desired_purpose = METADATA_PURPOSE_OWNER_SYNC if owner_sync_supported else None

    # Missing local source file is informational, not a hard skip: we
    # still want the metadata row so the measurement lands in public
    # sporePoints. The mosaic pusher will separately skip the tile.
    filepath = str(row.get('filepath') or '').strip()
    if filepath and not Path(filepath).exists():
        print(
            f'[cloud_sync] Mosaic image metadata: note '
            f'local_image={local_image_id} reason=missing_source_file',
            flush=True,
        )

    existing_local_cloud_id = str(row.get('cloud_id') or '').strip()
    portable_identity_pending = bool(row.get('portable_cloud_identity_pending'))
    remote_rows = [dict(remote or {}) for remote in (remote_images or [])]
    if remote_images is None:
        puller = getattr(client, 'pull_image_metadata', None)
        if callable(puller):
            try:
                remote_rows = [
                    dict(remote or {})
                    for remote in (
                        puller(obs_cloud_id, include_deleted_for_sync=True) or []
                    )
                ]
            except TypeError:
                remote_rows = [
                    dict(remote or {})
                    for remote in (puller(obs_cloud_id) or [])
                ]
        elif not portable_identity_pending and hasattr(client, '_find_cloud_image'):
            try:
                _row = client._find_cloud_image(local_image_id, obs_cloud_id)
                remote_cloud_id = _row['id'] if _row else None
            except ImageIdentityConflictError:
                raise
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                print(
                    f'[cloud_sync] Mosaic image metadata: lookup failed '
                    f'local_image={local_image_id}: {exc}',
                    flush=True,
                )
            else:
                if remote_cloud_id:
                    remote_rows = [{
                        'id': str(remote_cloud_id),
                        'desktop_id': local_image_id,
                        'observation_id': obs_cloud_id,
                        'image_type': 'microscope',
                        'storage_path': None,
                        'deleted_at': None,
                    }]

    remote_by_id = {
        str(remote.get('id') or '').strip(): remote
        for remote in remote_rows
        if str(remote.get('id') or '').strip()
    }
    remote_by_desktop_id = {
        _safe_int(remote.get('desktop_id')): remote
        for remote in remote_rows
        if _safe_int(remote.get('desktop_id')) > 0
    }
    remote_row = None
    if not portable_identity_pending:
        remote_row = remote_by_desktop_id.get(local_image_id)
    if remote_row is None and existing_local_cloud_id:
        candidate = remote_by_id.get(existing_local_cloud_id)
        if (
            candidate
            and str(candidate.get('observation_id') or '').strip() == str(obs_cloud_id)
            and (
                portable_identity_pending
                or _safe_int(candidate.get('desktop_id')) in {0, local_image_id}
            )
        ):
            remote_row = candidate

    payload = _metadata_only_microscope_image_payload(
        client, obs_cloud_id, row,
    )
    if portable_identity_pending:
        payload.pop('desktop_id', None)
    # A public-spore parent needs its `public_microscopy` marker on a server
    # with the capability, or that server treats it as not public (NULL fails
    # closed, sporePoints included). Probe (cached per client) only when a
    # no-byte row is being created or already exists; byte-backed rows ignore
    # the marker.
    if public_required and (
        remote_row is None
        or not _normalize_cloud_media_key(remote_row.get('storage_path'))
    ) and _owner_sync_parents_supported(client):
        desired_purpose = METADATA_PURPOSE_PUBLIC_MICROSCOPY
    if desired_purpose:
        payload['metadata_purpose'] = desired_purpose
    if remote_row:
        remote_cloud_id = str(remote_row.get('id') or '').strip()
        # Keep an existing metadata-only parent's purpose current (a spore
        # added or removed changes it). Bytes present: the marker is
        # irrelevant and left alone. Unchanged: no write.
        if (
            desired_purpose
            and not _normalize_cloud_media_key(remote_row.get('storage_path'))
            and _remote_metadata_purpose(client, remote_row) != desired_purpose
        ):
            client._patch(
                f'observation_images?id=eq.{remote_cloud_id}'
                f'&user_id=eq.{client.user_id}',
                {'metadata_purpose': desired_purpose},
            )
        _cancel_microscope_anchor_tombstones(
            local_image_id, existing_local_cloud_id, remote_cloud_id,
        )
        _reconcile_local_image_cloud_id(
            local_image_id, remote_cloud_id, mark_synced=True,
        )
        _set_cloud_image_metadata_only_state(
            obs_local_id,
            local_image_id,
            not bool(_normalize_cloud_media_key(remote_row.get('storage_path'))),
        )
        if str(remote_row.get('deleted_at') or '').strip():
            client._patch(
                f'observation_images?id=eq.{remote_cloud_id}'
                f'&user_id=eq.{client.user_id}',
                {'deleted_at': None},
            )
        print(
            f'[cloud_sync] Mosaic image metadata: linked '
            f'local_image={local_image_id} cloud_image={remote_cloud_id} (validated)',
            flush=True,
        )
        return remote_cloud_id

    if existing_local_cloud_id:
        conn = get_connection()
        try:
            conn.execute(
                'UPDATE images SET cloud_id = NULL, synced_at = NULL WHERE id = ?',
                (local_image_id,),
            )
            conn.commit()
        finally:
            conn.close()
        _clear_cloud_image_file_signature(obs_local_id, local_image_id)
        _set_cloud_image_metadata_only_state(obs_local_id, local_image_id, False)
        _cancel_microscope_anchor_tombstones(
            local_image_id, existing_local_cloud_id,
        )
        print(
            f'[cloud_sync] Mosaic image metadata: stale local link '
            f'local_image={local_image_id} cloud_image={existing_local_cloud_id}',
            flush=True,
        )

    print(
        f'[cloud_sync] Mosaic image metadata: create '
        f'local_image={local_image_id} obs={obs_local_id} '
        f'cloud_obs={obs_cloud_id} type=microscope storage_path=NULL',
        flush=True,
    )

    rows = client._post('observation_images', payload)
    cloud_image_id = ''
    if isinstance(rows, list) and rows:
        cloud_image_id = str(rows[0].get('id') or '').strip()
    elif isinstance(rows, dict):
        cloud_image_id = str(rows.get('id') or '').strip()

    if not cloud_image_id:
        print(
            f'[cloud_sync] Mosaic image metadata: create returned no id '
            f'local_image={local_image_id}',
            flush=True,
        )
        return None

    _cancel_microscope_anchor_tombstones(
        local_image_id, existing_local_cloud_id, cloud_image_id,
    )
    _reconcile_local_image_cloud_id(local_image_id, cloud_image_id, mark_synced=True)
    _set_cloud_image_metadata_only_state(obs_local_id, local_image_id, True)
    print(
        f'[cloud_sync] Mosaic image metadata: linked '
        f'local_image={local_image_id} cloud_image={cloud_image_id}',
        flush=True,
    )
    return cloud_image_id


def _ensure_metadata_only_microscope_images_for_observation(
    client: 'SporelyCloudClient',
    obs_local_id: int,
    obs_cloud_id: str,
) -> dict:
    """Loop wrapper around ``_ensure_metadata_only_microscope_image_for_public_spores``.

    Walks every local microscope image on the observation and validates or
    establishes the cloud anchor required by its public measurements.
    Non-fatal per-image failures are logged and counted, but do not stop
    the loop. Auth / temporary errors propagate so the backfill aborts.
    Returns a counters dict with:

    * ``considered`` / ``ensured`` / ``skipped`` / ``failed`` — counts.
    * ``cloud_ids`` — every microscope anchor cloud id ensured this cycle.
    * ``metadata_only_cloud_ids`` — existing legitimate metadata-only
      anchors, never rows downgraded because of publication selection.
    """
    counters = {
        'considered': 0,
        'ensured': 0,
        'skipped': 0,
        'failed': 0,
        'failures': [],
        'cloud_ids': [],
        'metadata_only_cloud_ids': [],
    }
    if not obs_cloud_id:
        return counters
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        portable_identity_pending = False
        if 'portable_cloud_identity_pending' in {
            str(info[1]) for info in conn.execute('PRAGMA table_info(observations)')
        }:
            marker_row = conn.execute(
                'SELECT portable_cloud_identity_pending FROM observations WHERE id = ?',
                (obs_local_id,),
            ).fetchone()
            portable_identity_pending = bool(marker_row and marker_row[0])
        cursor = conn.execute(
            """
            SELECT images.*
            FROM images
            WHERE images.observation_id = ?
              AND images.image_type = 'microscope'
            ORDER BY images.id
            """,
            (obs_local_id,),
        )
        rows = [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()

    if not rows:
        return counters

    selected_image_ids = _cloud_explicit_media_upload_selection(obs_local_id)

    puller = getattr(client, 'pull_image_metadata', None)
    remote_images: list[dict] = []
    if callable(puller):
        try:
            remote_images = [
                dict(row or {})
                for row in (
                    puller(obs_cloud_id, include_deleted_for_sync=True) or []
                )
            ]
        except TypeError:
            remote_images = [
                dict(row or {})
                for row in (puller(obs_cloud_id) or [])
            ]

    for image_row in rows:
        image_row['portable_cloud_identity_pending'] = portable_identity_pending
        counters['considered'] += 1
        local_image_id = _safe_int(image_row.get('id'))
        try:
            result = _ensure_metadata_only_microscope_image_for_public_spores(
                client,
                obs_local_id,
                obs_cloud_id,
                image_row,
                remote_images=remote_images,
            )
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            counters['failed'] += 1
            failure_msg = f'local_image={local_image_id}: {type(exc).__name__}: {exc}'
            counters['failures'].append(failure_msg)
            print(
                f'[cloud_sync] Mosaic image metadata: failed '
                f'{failure_msg}',
                flush=True,
            )
            continue
        if result:
            counters['ensured'] += 1
            counters['cloud_ids'].append(str(result))
            remote_match = next(
                (
                    remote for remote in remote_images
                    if str(remote.get('id') or '').strip() == str(result)
                ),
                None,
            )
            if (
                local_image_id not in selected_image_ids
                and remote_match
                and not _normalize_cloud_media_key(remote_match.get('storage_path'))
            ):
                counters['metadata_only_cloud_ids'].append(str(result))
        else:
            counters['skipped'] += 1
            if _retire_unneeded_owner_sync_parent(
                client, obs_local_id, image_row, remote_images,
            ):
                counters.setdefault('retired_cloud_ids', []).append(
                    str(image_row.get('cloud_id') or '')
                )

    return counters


def _observation_has_owner_sync_candidates(obs_local_id: int) -> bool:
    """Local-only check: any measured microscope image whose bytes are
    excluded, or any recorded metadata-only parent (a retirement candidate)."""
    conn = get_connection()
    try:
        image_ids = [
            int(row[0]) for row in conn.execute(
                """
                SELECT DISTINCT i.id FROM images i
                JOIN spore_measurements m ON m.image_id = i.id
                WHERE i.observation_id = ? AND i.image_type = 'microscope'
                """,
                (int(obs_local_id),),
            ).fetchall()
        ]
    except sqlite3.OperationalError:
        # No measurement/image tables (a partial database): nothing to sync.
        image_ids = []
    finally:
        conn.close()
    excluded = _cloud_image_storage_excluded_image_ids(obs_local_id)
    return bool(set(image_ids) & excluded) or bool(_cloud_metadata_only_image_ids(obs_local_id))


def _retire_unneeded_owner_sync_parent(
    client: 'SporelyCloudClient',
    obs_local_id: int,
    image_row: dict,
    remote_images: list[dict],
) -> bool:
    """Queue a cloud-copy tombstone for an owner-sync parent nothing needs.

    Called only for images neither parent intent requires. Retires the
    parent only when it is provably unnecessary everywhere:

    * the server has the capability and marks the row ``owner_sync`` (a
      legacy or public-microscopy parent keeps its existing lifecycle);
    * it is a live metadata-only row (no bytes) and the owner has not asked
      for the image's bytes to be kept in the cloud;
    * the image has no local measurements AND the cloud has none on it — a
      device that simply has not downloaded the measurements yet must never
      retire the parent that carries them.

    It only queues a cloud-copy tombstone; the canonical tombstone push
    (`_push_pending_image_tombstones`) performs the soft delete on the next
    sync that runs it (in the normal chain the tombstone push precedes the
    parent pass, so retirement converges over two syncs). A later
    measurement revives the parent through `_cancel_microscope_anchor_tombstones`.
    """
    local_image_id = _safe_int(image_row.get('id'))
    cloud_image_id = str(image_row.get('cloud_id') or '').strip()
    if local_image_id <= 0 or not cloud_image_id:
        return False
    # Cheap local checks first: no request unless this is a recorded
    # metadata-only parent that nothing local needs any more.
    if local_image_id not in _cloud_metadata_only_image_ids(obs_local_id):
        return False
    if cloud_image_bytes_desired(obs_local_id, local_image_id, image_row):
        return False
    if microscope_image_requires_owner_sync_anchor(local_image_id):
        return False
    remote = next(
        (dict(r) for r in (remote_images or []) if str(r.get('id') or '').strip() == cloud_image_id),
        None,
    )
    if (
        remote is None
        or _normalize_cloud_media_key(remote.get('storage_path'))
        or str(remote.get('deleted_at') or '').strip()
    ):
        return False
    if not _owner_sync_parents_supported(client):
        return False
    if _remote_metadata_purpose(client, remote) != METADATA_PURPOSE_OWNER_SYNC:
        return False
    try:
        remote_measurements = client.pull_measurements_for_images([cloud_image_id]) or []
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        return False
    if remote_measurements:
        return False
    queued = ImageDB.queue_image_tombstone_for_local_image(local_image_id)
    if queued:
        print(
            f'[cloud_sync] Owner-sync parent retired: local_image={local_image_id} '
            f'cloud_image={cloud_image_id} (no measurements on any device)',
            flush=True,
        )
    return bool(queued)


def _ensure_metadata_anchors_for_public_spore_observation(
    client: 'SporelyCloudClient',
    obs: dict,
    obs_local_id: int,
    obs_cloud_id: str,
) -> dict:
    """Public spore pre-step: create metadata-only microscope image anchors.

    Runs immediately before the normal-sync measurement push for any
    observation whose ``spore_data_visibility='public'``. Mirrors the
    backfill path so newly-added measurements on unshared microscope
    frames reach public sporePoints without a manual backfill run.

    Auth / temporary cloud errors propagate — the caller aborts. Other
    per-image errors are logged (already inside the helper) and the
    per-observation wrapper's own catch keeps sync moving.
    """
    empty = {
        'considered': 0,
        'ensured': 0,
        'skipped': 0,
        'failed': 0,
        'failures': [],
        'cloud_ids': [],
        'metadata_only_cloud_ids': [],
    }
    if obs_local_id <= 0 or not obs_cloud_id:
        return empty
    visibility = str(
        (obs or {}).get('spore_data_visibility') or 'public'
    ).strip().lower()
    # Owner cross-device sync does not depend on publication: with the
    # owner-sync capability every observation's measured microscope images
    # get a parent. Without it, only public spore data needs one.
    if visibility != 'public' and not (
        _observation_has_owner_sync_candidates(obs_local_id)
        and _owner_sync_parents_supported(client)
    ):
        return empty
    try:
        result = _ensure_metadata_only_microscope_images_for_observation(
            client, obs_local_id, obs_cloud_id,
        )
        inner_failures = result.get('failures') or []
        if inner_failures:
            return {**result, 'failures': inner_failures}
        return result
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        exc_msg = f'{type(exc).__name__}: {exc}'
        print(
            f'[cloud_sync] Mosaic image metadata: observation failed '
            f'local={obs_local_id} cloud={obs_cloud_id}: {exc}',
            flush=True,
        )
        return {**empty, 'failures': [exc_msg]}
