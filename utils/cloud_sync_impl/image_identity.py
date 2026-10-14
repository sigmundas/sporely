"""Image and observation push identity, mixed into ``SporelyCloudClient``;
the client class stays in the facade.

Owns local image ``cloud_id`` repair (``_reconcile_local_image_cloud_id``),
the portable cloud identity guard, and the client methods that decide which
existing cloud row an observation or image push targets.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S5 of the cloud-sync extraction.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from database.schema import get_connection

from utils.cloud_sync_impl.errors import (
    ImageIdentityConflictError,
    ObservationIdentityConflictError,
)


def _reconcile_local_image_cloud_id(
    local_image_id: int | str,
    cloud_image_id: str,
    *,
    mark_synced: bool = False,
) -> bool:
    """Persist ``cloud_image_id`` onto the local images row.

    Returns ``True`` when the row was updated because its ``cloud_id`` was
    missing or stale. This makes metadata-only associations of an existing
    remote cloud image persistent: previously-synced local rows whose
    ``cloud_id`` was lost get re-linked instead of being re-dirtied on every
    sync. No image bytes are uploaded here — only the local link is restored.
    """
    try:
        image_id = int(local_image_id)
    except Exception:
        image_id = 0
    cloud_image_id = str(cloud_image_id or '').strip()
    if image_id <= 0 or not cloud_image_id:
        return False
    conn = get_connection()
    try:
        row = conn.execute('SELECT cloud_id FROM images WHERE id = ?', (image_id,)).fetchone()
        if row is None:
            return False
        existing = str((row['cloud_id'] if isinstance(row, sqlite3.Row) else row[0]) or '').strip()
        if existing == cloud_image_id:
            return False
        if mark_synced:
            conn.execute(
                'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                (cloud_image_id, datetime.now(timezone.utc).isoformat(), image_id),
            )
        else:
            conn.execute(
                'UPDATE images SET cloud_id = ? WHERE id = ?',
                (cloud_image_id, image_id),
            )
        conn.commit()
        return True
    finally:
        conn.close()


def _portable_cloud_identity_pending_for_observation(observation_id: int) -> bool:
    """Return the persistent portable identity guard without assuming migration state."""
    conn = get_connection()
    try:
        try:
            table_info = list(conn.execute('PRAGMA table_info(observations)'))
        except (TypeError, sqlite3.Error):
            return False
        if 'portable_cloud_identity_pending' not in {str(info[1]) for info in table_info}:
            return False
        row = conn.execute(
            'SELECT portable_cloud_identity_pending FROM observations WHERE id=?',
            (int(observation_id),),
        ).fetchone()
        return bool(row and row[0])
    finally:
        conn.close()


def _finalize_portable_cloud_identity_guard(
    client: SporelyCloudClient,
    obs_local_id: int,
) -> bool:
    """Publish collision-free destination reverse IDs, then retire the guard."""
    connection = get_connection()
    connection.row_factory = sqlite3.Row
    try:
        if 'portable_cloud_identity_pending' not in {
            str(info[1]) for info in connection.execute('PRAGMA table_info(observations)')
        }:
            return False
        observation_row = connection.execute(
            "SELECT id, cloud_id, portable_cloud_identity_pending "
            "FROM observations WHERE id=?",
            (int(obs_local_id),),
        ).fetchone()
        if (
            observation_row is None
            or not bool(observation_row['portable_cloud_identity_pending'])
            or not str(observation_row['cloud_id'] or '').strip()
        ):
            return False
        image_rows = [dict(row) for row in connection.execute(
            """
            SELECT id, cloud_id, image_type FROM images
            WHERE observation_id = ?
              AND COALESCE(source_role, '') != 'cloud_recovery_cache'
              AND COALESCE(file_purpose, '') != 'cache'
            ORDER BY id
            """,
            (int(obs_local_id),),
        )]
        if any(not str(row.get('cloud_id') or '').strip() for row in image_rows):
            return False
        measurement_rows = [dict(row) for row in connection.execute(
            """
            SELECT measurement.id, measurement.cloud_id
            FROM spore_measurements AS measurement
            JOIN images AS image ON image.id = measurement.image_id
            WHERE image.observation_id = ?
              AND COALESCE(image.source_role, '') != 'cloud_recovery_cache'
              AND COALESCE(image.file_purpose, '') != 'cache'
            ORDER BY measurement.id
            """,
            (int(obs_local_id),),
        )]
        if any(not str(row.get('cloud_id') or '').strip() for row in measurement_rows):
            return False
    finally:
        connection.close()

    observation_cloud_id = str(observation_row['cloud_id']).strip()
    observation_match = client._find_cloud_observation(int(obs_local_id))
    if observation_match and str(observation_match) != observation_cloud_id:
        return False

    image_matches: dict[int, str] = {}
    for image in image_rows:
        image_id = int(image['id'])
        image_cloud_id = str(image['cloud_id']).strip()
        candidates = client._get(
            f'observation_images?desktop_id=eq.{image_id}'
            f'&user_id=eq.{client.user_id}&select=id'
        )
        candidate_ids = {
            str(row.get('id') or '').strip()
            for row in (candidates or [])
            if str(row.get('id') or '').strip()
        }
        if candidate_ids and candidate_ids != {image_cloud_id}:
            return False
        image_matches[image_id] = image_cloud_id if candidate_ids else ''

    measurement_matches: dict[int, str] = {}
    for measurement in measurement_rows:
        measurement_id = int(measurement['id'])
        measurement_cloud_id = str(measurement['cloud_id']).strip()
        rows = client._get(
            f'spore_measurements?desktop_id=eq.{measurement_id}'
            f'&user_id=eq.{client.user_id}&select=id'
        )
        candidate_ids = {
            str(row.get('id') or '').strip()
            for row in (rows or [])
            if str(row.get('id') or '').strip()
        }
        if candidate_ids and candidate_ids != {measurement_cloud_id}:
            return False
        measurement_matches[measurement_id] = (
            measurement_cloud_id if candidate_ids else ''
        )

    # The observation's reverse link is never written: desktop_id is retired
    # as an observation identity, and the server refuses it on keyed rows.
    for image in image_rows:
        image_id = int(image['id'])
        if not image_matches[image_id]:
            client.set_image_desktop_id(str(image['cloud_id']).strip(), image_id)
    for measurement in measurement_rows:
        measurement_id = int(measurement['id'])
        if not measurement_matches[measurement_id]:
            client.set_measurement_desktop_id(
                str(measurement['cloud_id']).strip(), measurement_id
            )

    connection = get_connection()
    try:
        cursor = connection.execute(
            "UPDATE observations SET portable_cloud_identity_pending=0 "
            "WHERE id=? AND portable_cloud_identity_pending=1",
            (int(obs_local_id),),
        )
        connection.commit()
        return int(cursor.rowcount or 0) == 1
    finally:
        connection.close()


class CloudSyncPushIdentityMixin:
    """Observation and image push identity methods of ``SporelyCloudClient``."""

    def _resolve_existing_observation_for_push(
        self,
        obs: dict,
        remote_obs: dict | None = None,
    ) -> str | None:
        """Canonical owner of observation push identity resolution.

        The local direct link (``obs['cloud_id']``) is the only push target:
        once it verifies against an existing same-owner cloud row, that row
        is PATCHed by id. ``desktop_id`` is retired as an identity and is
        never looked up here; a lost ``cloud_id`` is recovered by the create
        path through ``sync_key`` (``23505`` adoption) or the one-time legacy
        recovery.

        Returns the cloud observation id to PATCH, or ``None`` when the row
        has no ``cloud_id`` and the caller may create it (L0).

        Raises :class:`ObservationIdentityConflictError` when the row carries
        a ``cloud_id`` that does not verify (deleted remotely, or invisible
        to this user-scoped client because it belongs to another account): a
        cloud-linked row is never POSTed, and never PATCHed blindly.
        """
        local_cloud_id = str(obs.get('cloud_id') or '').strip()
        if not local_cloud_id:
            return None
        if remote_obs is not None and str(remote_obs.get('id') or '').strip() == local_cloud_id:
            # Caller-supplied remote rows come from user-scoped queries
            # (get_observation / list_remote_observations), so a matching
            # id is already existence- and ownership-verified.
            candidate = remote_obs
        else:
            candidate = self.get_observation(local_cloud_id)
        if candidate is not None:
            return str(candidate.get('id') or '').strip() or local_cloud_id
        raise ObservationIdentityConflictError(
            f"observation {obs.get('id')}: linked cloud observation {local_cloud_id} "
            f'was not found; refusing to patch it blindly or create a new row'
        )

    def _find_cloud_image(
        self,
        desktop_id: int,
        obs_cloud_id: str,
        image_type: str | None = None,
    ) -> dict | None:
        """Reverse-link (``desktop_id``) recovery lookup for one image.

        Returns a dict with ``id`` and ``deleted_at`` for the single
        same-owner, same-observation cloud image carrying this
        ``desktop_id``, or ``None``.  Multiple matches mean cloud-side
        duplicates; silently picking one could route a push at the wrong
        row, so that case raises :class:`ImageIdentityConflictError`.
        """
        q = (
            f'observation_images?desktop_id=eq.{desktop_id}'
            f'&user_id=eq.{self.user_id}'
            f'&observation_id=eq.{obs_cloud_id}'
            f'&select=id,deleted_at'
            f'&order=id.asc'
        )
        if image_type:
            q += f'&image_type=eq.{image_type}'
        rows = self._get(q)
        if len(rows) > 1:
            raise ImageIdentityConflictError(
                f'image desktop_id={desktop_id}: {len(rows)} cloud images '
                f'carry this desktop_id in observation {obs_cloud_id} '
                f'({", ".join(str(r.get("id")) for r in rows)}); '
                f'refusing to choose among duplicates'
            )
        return dict(rows[0]) if rows else None

    def _resolve_existing_image_for_push(
        self,
        img: dict,
        obs_cloud_id: str,
        remote_row: dict | None = None,
    ) -> str | None:
        """Canonical owner of image push identity resolution.

        The local direct link (``img['cloud_id']``) is the only push target:
        once it verifies against an existing same-owner row under the same
        cloud observation, that row is PATCHed by id. ``desktop_id`` is
        retired as an identity and is never looked up here; a lost
        ``cloud_id`` is recovered by the create path through ``sync_key``
        (``23505`` adoption) or the one-time legacy recovery.

        Returns the cloud image id to PATCH, or ``None`` when the image has
        no ``cloud_id`` and the caller may create it (L0).

        Raises :class:`ImageIdentityConflictError` when the linked row is
        gone, sits under another observation (never re-parented) or is
        soft-deleted (never resurrected): a cloud-linked row is never POSTed
        and never PATCHed blindly.
        """
        local_cloud_id = str(img.get('cloud_id') or '').strip()
        if not local_cloud_id:
            return None
        candidate: dict | None = None
        if remote_row is not None:
            r_id = str(remote_row.get('id') or '').strip()
            r_obs = str(remote_row.get('observation_id') or '').strip()
            if r_id == local_cloud_id and r_obs == str(obs_cloud_id):
                candidate = dict(remote_row)
        if candidate is None:
            rows = self._get(
                f'observation_images?id=eq.{local_cloud_id}'
                f'&user_id=eq.{self.user_id}'
                f'&select=id,deleted_at,observation_id'
                f'&limit=1'
            )
            candidate = dict(rows[0]) if rows else None
        if candidate is None:
            raise ImageIdentityConflictError(
                f"image {img.get('id')}: linked cloud image {local_cloud_id} was not "
                f'found; refusing to patch it blindly or create a new row'
            )
        if candidate.get('deleted_at'):
            raise ImageIdentityConflictError(
                f'image {local_cloud_id} is soft-deleted in the cloud; not safe to PATCH'
            )
        if str(candidate.get('observation_id') or '') != str(obs_cloud_id):
            raise ImageIdentityConflictError(
                f"local cloud_id {local_cloud_id!r} points to a cloud image in observation "
                f"{candidate.get('observation_id')!r}, not {obs_cloud_id!r}; "
                "refusing to reparent"
            )
        return str(candidate.get('id') or '').strip() or local_cloud_id
