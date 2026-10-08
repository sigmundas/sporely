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
from utils.cloud_sync_impl.sync_state import _explicit_image_restore_source


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

    if not observation_match:
        client.set_desktop_id(observation_cloud_id, int(obs_local_id))
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

        Decides which existing cloud observation a push must target:

        * The local direct link (``obs['cloud_id']``) is the PRIMARY
          identity. Once it verifies against an existing same-owner cloud
          row, that row is the push target — even when the remote
          ``desktop_id`` is NULL. A NULL reverse link is the normal state
          for observations imported by Download from Cloud (pull-only
          performs zero cloud writes, so the reverse link is only healed by
          a later normal push) and must never cause a duplicate POST.
        * The remote reverse link (``desktop_id``) is the RECOVERY
          identity: used when no verified direct link exists, and
          cross-checked for disagreement when both links resolve.

        Returns the cloud observation id to PATCH, or ``None`` when
        creating a new cloud observation is permitted (no verified direct
        target and no unambiguous reverse-link match).

        Raises :class:`ObservationIdentityConflictError` when the two links
        resolve to different rows or the reverse link is ambiguous. Callers
        must leave the observation dirty/retryable and must not POST.
        """
        local_cloud_id = str(obs.get('cloud_id') or '').strip()

        verified_direct_id = ''
        if local_cloud_id:
            candidate = None
            if remote_obs is not None and str(remote_obs.get('id') or '').strip() == local_cloud_id:
                # Caller-supplied remote rows come from user-scoped queries
                # (get_observation / list_remote_observations), so a matching
                # id is already existence- and ownership-verified.
                candidate = remote_obs
            else:
                candidate = self.get_observation(local_cloud_id)
            if candidate is not None:
                verified_direct_id = str(candidate.get('id') or '').strip() or local_cloud_id

        portable_identity_pending = bool(obs.get('portable_cloud_identity_pending'))
        if portable_identity_pending:
            return verified_direct_id or None

        # The reverse-link lookup runs in both branches: as recovery when the
        # direct link is unusable, and as a disagreement cross-check when it
        # verified. Raises on multiple matches.
        recovered_id = self._find_cloud_observation(obs['id'])

        if verified_direct_id:
            if recovered_id and str(recovered_id) != verified_direct_id:
                raise ObservationIdentityConflictError(
                    f"observation {obs['id']}: direct link cloud_id="
                    f'{verified_direct_id} and reverse link desktop_id match='
                    f'{recovered_id} resolve to different cloud observations; '
                    f'refusing to patch either or create a third'
                )
            return verified_direct_id

        if recovered_id:
            return str(recovered_id)

        # No verified direct target and no reverse-link match: creation is
        # allowed by the existing contract. A populated but unverifiable
        # local cloud_id (row deleted remotely, or invisible to this
        # user-scoped client because it belongs to another account) must
        # never be patched blindly.
        return None

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

        Returns the cloud image id to PATCH, or ``None`` when creating a
        new cloud image is permitted.

        Raises :class:`ImageIdentityConflictError` on ambiguity or
        soft-delete conflict without explicit restore intent.
        """
        restore_source_id = _explicit_image_restore_source(img['id'])
        local_cloud_id = str(img.get('cloud_id') or '').strip()

        verified_direct: dict | None = None
        if local_cloud_id:
            candidate: dict | None = None
            if remote_row is not None:
                r_id = str(remote_row.get('id') or '').strip()
                r_obs = str(remote_row.get('observation_id') or '').strip()
                if r_id == local_cloud_id and r_obs == obs_cloud_id:
                    candidate = dict(remote_row)
            if candidate is None:
                rows = self._get(
                    f'observation_images?id=eq.{local_cloud_id}'
                    f'&user_id=eq.{self.user_id}'
                    f'&select=id,desktop_id,deleted_at,observation_id'
                    f'&limit=1'
                )
                candidate = dict(rows[0]) if rows else None
            if candidate is not None:
                if candidate.get('deleted_at') and local_cloud_id != restore_source_id:
                    raise ImageIdentityConflictError(
                        f'image {local_cloud_id} is soft-deleted and no explicit '
                        f'restore intent exists; not safe to PATCH'
                    )
                if str(candidate.get('observation_id') or '') != str(obs_cloud_id) and local_cloud_id != restore_source_id:
                    raise ImageIdentityConflictError(
                        f"local cloud_id {local_cloud_id!r} points to a cloud image in observation "
                        f"{candidate.get('observation_id')!r}, not {obs_cloud_id!r}; "
                        "refusing to reparent"
                    )
                verified_direct = candidate

        if bool(img.get('portable_cloud_identity_pending')):
            return str(verified_direct.get('id') or '').strip() if verified_direct else None

        # Reverse leg — raises on ambiguity.
        recovered_row = self._find_cloud_image(
            img['id'], obs_cloud_id, image_type=img.get('image_type')
        )
        recovered_id = str(recovered_row['id']) if recovered_row else ''
        if recovered_row and recovered_row.get('deleted_at') and recovered_id != restore_source_id:
            raise ImageIdentityConflictError(
                f'image {recovered_id} is soft-deleted and no explicit '
                f'restore intent exists'
            )

        verified_direct_id = str(verified_direct.get('id') or '').strip() if verified_direct else ''

        # Restore release: release old soft-deleted row's desktop identity so
        # a fresh POST can claim it, then treat as new.
        for candidate_id in {verified_direct_id, recovered_id}:
            if candidate_id and restore_source_id and candidate_id == restore_source_id:
                self._patch(
                    f'observation_images?id=eq.{restore_source_id}'
                    f'&user_id=eq.{self.user_id}',
                    {'desktop_id': None},
                )
                if candidate_id == verified_direct_id:
                    verified_direct = None
                    verified_direct_id = ''
                if candidate_id == recovered_id:
                    recovered_row = None
                    recovered_id = ''
                break

        if verified_direct_id and recovered_id and verified_direct_id != recovered_id:
            raise ImageIdentityConflictError(
                f"image {img['id']}: direct link cloud_id={verified_direct_id} "
                f'and reverse link desktop_id match={recovered_id} resolve to '
                f'different cloud images; refusing to patch either or create a third'
            )

        if verified_direct_id:
            return verified_direct_id
        if recovered_id:
            return recovered_id

        # When a restore release was processed above, the desktop_id was just
        # cleared on the cloud row — the global check would find a stale state
        # and incorrectly raise. Skip it; the POST is intentional here.
        if restore_source_id:
            return None

        # Global same-user desktop_id check before permitting POST.
        global_rows = self._get(
            f'observation_images?desktop_id=eq.{img["id"]}&user_id=eq.{self.user_id}'
            f'&select=id,observation_id,image_type,deleted_at&order=id.asc'
        )
        if global_rows:
            if len(global_rows) > 1:
                raise ImageIdentityConflictError(
                    f"desktop_id {img['id']} matches {len(global_rows)} cloud images "
                    f"across observations; ambiguous — refusing to POST duplicate"
                )
            row = global_rows[0]
            if row.get('deleted_at'):
                raise ImageIdentityConflictError(
                    f"desktop_id {img['id']} exists as soft-deleted cloud image "
                    f"{row['id']} in observation {row['observation_id']}; "
                    "not safe to POST duplicate"
                )
            if str(row.get('observation_id') or '') != str(obs_cloud_id):
                raise ImageIdentityConflictError(
                    f"desktop_id {img['id']} belongs to cloud image {row['id']} "
                    f"in observation {row['observation_id']}, not {obs_cloud_id}; "
                    "cannot POST duplicate"
                )
            local_image_type = img.get('image_type')
            if row.get('image_type') and local_image_type and row.get('image_type') != local_image_type:
                raise ImageIdentityConflictError(
                    f"desktop_id {img['id']} exists as {row['image_type']} in cloud "
                    f"but local image_type is {local_image_type}"
                )
            raise ImageIdentityConflictError(
                f"unexpected: global lookup found {row['id']} but obs-scoped reverse was empty"
            )

        return None
