"""Image and observation push identity, mixed into ``SporelyCloudClient``;
the client class stays in the facade.

Owns local image ``cloud_id`` repair (``_reconcile_local_image_cloud_id``)
and the client methods that decide which existing cloud row an observation or
image push targets. Only the verified local ``cloud_id`` is a push target;
``desktop_id`` is never looked up (portable imports mint fresh keys, so their
``portable_cloud_identity_pending`` flag no longer gates anything and stays
only until cleanup).

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
