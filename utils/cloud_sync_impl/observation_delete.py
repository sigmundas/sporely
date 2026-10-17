"""Durable intent for a local observation's cloud delete.

Deleting a linked observation removes the local row first and its cloud copy
afterwards (media, image rows, then the observation row). Without a record of
the intent, a crash or failure between those steps left a partial cloud
observation that the next pull imported again. Contract rules 6 and 8: the
intent is stored before anything is removed, and the deletion stays
retryable.

- :func:`mark_observation_delete_pending` commits the intent before the local
  delete; :func:`clear_observation_delete_pending` removes it once the cloud
  delete completed.
- :func:`retry_pending_observation_deletes` finishes pending deletes at the
  start of a write-enabled push. A delete whose observation is linked to a
  local row again is void and dropped, never sent.
- Pull never imports a cloud observation whose delete is pending
  (:func:`pending_observation_delete_ids`), nor one whose delete the same
  client completed earlier in the sync (the pull may use a listing read
  before the push).

The record holds the owning account's id; a delete is retried only for that
account.
"""

from __future__ import annotations

from database.models import get_connection
from utils.cloud_sync_impl.child_identity import _has_settings_table
from utils.cloud_sync_impl.errors import CloudUploadRefusedError

PENDING_OBSERVATION_DELETE_PREFIX = 'sporely_cloud_observation_delete_pending_'


def mark_observation_delete_pending(cloud_id, cloud_user_id) -> None:
    cloud_id = str(cloud_id or '').strip()
    if not cloud_id:
        return
    connection = get_connection()
    try:
        if not _has_settings_table(connection):
            return
        connection.execute(
            'INSERT INTO settings (key, value) VALUES (?, ?) '
            'ON CONFLICT(key) DO UPDATE SET value = excluded.value',
            (f'{PENDING_OBSERVATION_DELETE_PREFIX}{cloud_id}', str(cloud_user_id or '').strip()),
        )
        connection.commit()
    finally:
        connection.close()


def clear_observation_delete_pending(cloud_id) -> None:
    cloud_id = str(cloud_id or '').strip()
    if not cloud_id:
        return
    connection = get_connection()
    try:
        if not _has_settings_table(connection):
            return
        connection.execute(
            'DELETE FROM settings WHERE key = ?', (f'{PENDING_OBSERVATION_DELETE_PREFIX}{cloud_id}',),
        )
        connection.commit()
    finally:
        connection.close()


def pending_observation_deletes() -> dict[str, str]:
    """``{cloud_id: owning cloud user id}`` for every pending delete."""
    connection = get_connection()
    try:
        if not _has_settings_table(connection):
            return {}
        rows = connection.execute(
            'SELECT key, value FROM settings WHERE key LIKE ?',
            (f'{PENDING_OBSERVATION_DELETE_PREFIX}%',),
        ).fetchall()
    finally:
        connection.close()
    return {
        str(key)[len(PENDING_OBSERVATION_DELETE_PREFIX):]: str(value or '').strip()
        for key, value in rows
    }


_COMPLETED_ATTR = '_completed_observation_deletes'


def pending_observation_delete_ids(client=None) -> set[str]:
    """Cloud ids pull must not import: pending deletes, plus those ``client``
    completed in this sync (a pull may use a listing read before the push)."""
    ids = set(pending_observation_deletes())
    ids.update(getattr(client, _COMPLETED_ATTR, None) or ())
    return ids


def _locally_linked(cloud_id: str) -> bool:
    connection = get_connection()
    try:
        return connection.execute(
            'SELECT 1 FROM observations WHERE cloud_id = ? LIMIT 1', (cloud_id,),
        ).fetchone() is not None
    finally:
        connection.close()


def retry_pending_observation_deletes(client) -> dict:
    """Finish the pending cloud deletes of this account; return a summary.

    A refusal that stops uploads (``CloudUploadRefusedError``) propagates and
    leaves every remaining intent in place.
    """
    from utils.reference_cloud_sync import acknowledge_observation_parent_delete

    user_id = str(getattr(client, 'user_id', '') or '').strip()
    done: list[str] = []
    void: list[str] = []
    errors: list[str] = []
    for cloud_id, owner in sorted(pending_observation_deletes().items()):
        if owner and owner != user_id:
            continue
        if _locally_linked(cloud_id):
            # The local row exists again (the local delete did not complete):
            # the intent is void; the cloud copy is the local row's.
            clear_observation_delete_pending(cloud_id)
            void.append(cloud_id)
            continue
        try:
            client.delete_cloud_observation(cloud_id)
        except CloudUploadRefusedError:
            raise
        except Exception as exc:
            errors.append(f'cloud {cloud_id}: pending delete failed: {exc}')
            continue
        if user_id:
            acknowledge_observation_parent_delete(user_id, cloud_id)
        clear_observation_delete_pending(cloud_id)
        done.append(cloud_id)
    if done:
        completed = getattr(client, _COMPLETED_ATTR, None)
        if not isinstance(completed, set):
            completed = set()
            setattr(client, _COMPLETED_ATTR, completed)
        completed.update(done)
    if done or void or errors:
        print(
            f'[cloud_sync] pending observation deletes: completed={done} void={void} '
            f'failed={len(errors)}',
            flush=True,
        )
    return {'completed': done, 'void': void, 'errors': errors}
