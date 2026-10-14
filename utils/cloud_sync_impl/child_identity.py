"""Image and measurement create identity on the per-row state machine.

A child row (an ``observation_images`` or ``spore_measurements`` row) is
created only under a parent whose ``cloud_id`` is committed locally and equals
the cloud parent it targets; otherwise it is deferred and stays dirty
(:class:`ChildParentNotLinkedError`).

State L0 (key, no ``cloud_id``): POST with the committed local ``sync_key``.
A ``23505`` naming the table's ``*_user_sync_key_uidx`` means this database
already created the row and lost the answer: fetch it by key and owner and
judge it (:func:`judge_child_row`). It links only when it sits under the same
parent, is not deleted in the cloud and holds the local content; otherwise
:class:`ChildAdoptionConflictError`. It is never PATCHed, re-parented or
resurrected. Every other refusal is a plain error.

Rows marked ``legacy_recovery_pending`` first make one read-only lookup for a
keyless row with ``desktop_id`` equal to the local id under the already-linked
parent (:func:`recover_legacy_child`). It never writes to the matched row.

A link made here is never written to the cloud. The caller commits the local
``cloud_id`` (and clears the marker) only once the row's own work is done, so
a crash before that commit repeats the same decision on retry.
"""

from __future__ import annotations

from dataclasses import dataclass

from database.models import mark_observation_sync_dirty
from database.schema import get_connection

from utils.cloud_sync_impl.baseline import remember_judged_child_row
from utils.cloud_sync_impl.errors import (
    ChildAdoptionConflictError,
    ChildParentNotLinkedError,
    CloudSyncError,
    CloudWriteRejectedError,
)
from utils.cloud_sync_impl.fault_injection import sync_fault_point
from utils.cloud_sync_impl.observation_identity import (
    LEGACY_RECOVERY_MARKER_COLUMN,
    is_sync_key_unique_violation,
    local_sync_key,
)
from utils.cloud_sync_impl.progress import _increment_sync_summary


@dataclass(frozen=True)
class ChildTable:
    cloud_table: str
    local_table: str
    parent_column: str
    local_parent_table: str
    label: str
    summary_prefix: str


CHILD_TABLES = {
    'observation_images': ChildTable(
        cloud_table='observation_images',
        local_table='images',
        parent_column='observation_id',
        local_parent_table='observations',
        label='image',
        summary_prefix='images',
    ),
    'spore_measurements': ChildTable(
        cloud_table='spore_measurements',
        local_table='spore_measurements',
        parent_column='image_id',
        local_parent_table='images',
        label='measurement',
        summary_prefix='measurements',
    ),
}


def _spec(table: str) -> ChildTable:
    spec = CHILD_TABLES.get(str(table or ''))
    if spec is None:
        raise ValueError(f'not a child sync table: {table!r}')
    return spec


def committed_parent_cloud_id(table: str, local_row: dict) -> str:
    """The parent's ``cloud_id`` as committed in the local database, or ``''``."""
    spec = _spec(table)
    try:
        parent_id = int((local_row or {}).get(spec.parent_column) or 0)
    except (TypeError, ValueError):
        parent_id = 0
    if parent_id <= 0:
        return ''
    connection = get_connection()
    try:
        row = connection.execute(
            f'SELECT cloud_id FROM {spec.local_parent_table} WHERE id = ?', (parent_id,),
        ).fetchone()
    finally:
        connection.close()
    return str((row[0] if row else '') or '').strip()


def require_committed_parent(table: str, local_row: dict, parent_cloud_id: str) -> None:
    """Defer a child whose parent has no committed ``cloud_id`` equal to the target."""
    spec = _spec(table)
    target = str(parent_cloud_id or '').strip()
    committed = committed_parent_cloud_id(table, local_row)
    if not target or committed != target:
        raise ChildParentNotLinkedError(
            f"{spec.label} {(local_row or {}).get('id')}: deferred until its parent's "
            f'cloud_id is committed locally (committed={committed or "none"}, '
            f'target={target or "none"})'
        )


def clear_legacy_marker(connection, local_table: str, local_id: int) -> None:
    """Clear the one-time marker inside the caller's transaction, if the column exists."""
    columns = {str(row[1]) for row in connection.execute(f'PRAGMA table_info({local_table})')}
    if LEGACY_RECOVERY_MARKER_COLUMN in columns:
        connection.execute(
            f'UPDATE {local_table} SET {LEGACY_RECOVERY_MARKER_COLUMN} = 0 WHERE id = ?',
            (int(local_id),),
        )


def _clear_legacy_marker_now(local_table: str, local_id: int) -> None:
    connection = get_connection()
    try:
        clear_legacy_marker(connection, local_table, local_id)
        connection.commit()
    finally:
        connection.close()


# An image create runs after its observation was already stamped synced, so a
# crash between the image INSERT and its local link would otherwise wait for
# the periodic pending-image scan. This marker, set before the request and
# cleared with the link commit, makes the next push re-dirty the observation.
CHILD_CREATE_PENDING_SETTING_PREFIX = 'sporely_cloud_child_create_pending_obs_'


def _child_create_pending_key(local_observation_id) -> str:
    return f'{CHILD_CREATE_PENDING_SETTING_PREFIX}{int(local_observation_id)}'


def _has_settings_table(connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='settings'"
    ).fetchone() is not None


def mark_child_create_pending(local_observation_id) -> None:
    connection = get_connection()
    try:
        if not _has_settings_table(connection):
            return
        connection.execute(
            'INSERT INTO settings (key, value) VALUES (?, ?) '
            'ON CONFLICT(key) DO UPDATE SET value = excluded.value',
            (_child_create_pending_key(local_observation_id), '1'),
        )
        connection.commit()
    finally:
        connection.close()


def clear_child_create_pending(connection, local_observation_id) -> None:
    """Clear the marker inside the caller's link-commit transaction."""
    if not _has_settings_table(connection):
        return
    connection.execute(
        'DELETE FROM settings WHERE key = ?', (_child_create_pending_key(local_observation_id),),
    )


def redirty_interrupted_child_creates() -> list[int]:
    """Re-dirty every observation whose image create was interrupted.

    One transaction marks each such observation dirty and drops its marker:
    from then on the dirty status carries the retry, which repeats the same
    create and adopts the row it may have left. Returns the observation ids.
    """
    connection = get_connection()
    try:
        if not _has_settings_table(connection):
            return []
        keys = [
            str(row[0]) for row in connection.execute(
                'SELECT key FROM settings WHERE key LIKE ?',
                (f'{CHILD_CREATE_PENDING_SETTING_PREFIX}%',),
            ).fetchall()
        ]
        observation_ids = []
        cursor = connection.cursor()
        for key in keys:
            try:
                observation_id = int(key[len(CHILD_CREATE_PENDING_SETTING_PREFIX):])
            except ValueError:
                observation_id = 0
            if observation_id > 0:
                mark_observation_sync_dirty(cursor, observation_id)
                observation_ids.append(observation_id)
            cursor.execute('DELETE FROM settings WHERE key = ?', (key,))
        connection.commit()
    finally:
        connection.close()
    return observation_ids


def read_child_rows(client, table: str, filters: str, *, limit: int) -> list[dict]:
    """Same-owner rows of ``table`` matching ``filters``, oldest id first."""
    spec = _spec(table)
    if spec.cloud_table == 'observation_images':
        rows = client._read_observation_image_rows(
            client._get,
            lambda select: (
                f'observation_images?{filters}&user_id=eq.{client.user_id}'
                f'&select={select}&order=id.asc&limit={int(limit)}'
            ),
        )
    else:
        rows = client._get(
            f'spore_measurements?{filters}&user_id=eq.{client.user_id}'
            f'&select=*&order=id.asc&limit={int(limit)}'
        )
    return [dict(row or {}) for row in (rows or [])]


def judge_child_row(
    table: str,
    local_row: dict,
    remote: dict,
    *,
    parent_cloud_id: str,
    content_matches,
) -> None:
    """Raise :class:`ChildAdoptionConflictError` unless ``remote`` may be linked."""
    spec = _spec(table)
    local_id = (local_row or {}).get('id')
    cloud_id = str((remote or {}).get('id') or '').strip()
    remote_parent = str((remote or {}).get(spec.parent_column) or '').strip()
    if remote_parent != str(parent_cloud_id or '').strip():
        raise ChildAdoptionConflictError(
            f'{spec.label} {local_id}: its cloud copy {cloud_id} sits under '
            f'{spec.parent_column}={remote_parent or "none"}, not {parent_cloud_id}; '
            f'never re-parented',
            table=spec.cloud_table, cloud_id=cloud_id, reason='parent',
        )
    if str((remote or {}).get('deleted_at') or '').strip():
        raise ChildAdoptionConflictError(
            f'{spec.label} {local_id}: its cloud copy {cloud_id} was deleted in the '
            f'cloud; never recreated',
            table=spec.cloud_table, cloud_id=cloud_id, reason='deleted',
        )
    if not content_matches(dict(remote or {})):
        raise ChildAdoptionConflictError(
            f'{spec.label} {local_id}: its cloud copy {cloud_id} (same sync key) '
            f'differs from the local row; needs review',
            table=spec.cloud_table, cloud_id=cloud_id, reason='content',
        )


def adopt_child_by_sync_key(
    client,
    table: str,
    local_row: dict,
    sync_key: str,
    *,
    parent_cloud_id: str,
    content_matches,
) -> dict:
    """Resolve a create that hit this database's own keyed row; never writes."""
    spec = _spec(table)
    rows = read_child_rows(client, table, f'sync_key=eq.{sync_key}', limit=2)
    if len(rows) != 1 or not str(rows[0].get('id') or '').strip():
        raise CloudSyncError(
            f"{spec.label} {(local_row or {}).get('id')}: create reported an existing "
            f'row for its sync key, but {len(rows)} same-owner rows carry it; left for retry'
        )
    remote = rows[0]
    judge_child_row(
        table, local_row, remote,
        parent_cloud_id=parent_cloud_id, content_matches=content_matches,
    )
    return remote


def recover_legacy_child(
    client,
    table: str,
    local_row: dict,
    *,
    parent_cloud_id: str,
    content_matches,
) -> tuple[dict | None, bool]:
    """One-time lookup for an older desktop's lost upload of a marked child.

    Restricted to the already-linked parent. Returns ``(remote_row, False)``
    for exactly one keyless match that may be linked; nothing is written and
    the caller commits the link with the cleared marker. Otherwise clears the
    marker and returns ``(None, True)``: a possible duplicate, and the caller
    POSTs a new keyed row. A row found here is never PATCHed or deleted.
    """
    if not bool((local_row or {}).get(LEGACY_RECOVERY_MARKER_COLUMN)):
        return None, False
    spec = _spec(table)
    local_id = int(local_row['id'])
    rows = read_child_rows(
        client,
        table,
        f'sync_key=is.null&desktop_id=eq.{local_id}'
        f'&{spec.parent_column}=eq.{parent_cloud_id}',
        limit=2,
    )
    if len(rows) == 1 and str(rows[0].get('id') or '').strip():
        try:
            judge_child_row(
                table, local_row, rows[0],
                parent_cloud_id=parent_cloud_id, content_matches=content_matches,
            )
        except ChildAdoptionConflictError:
            pass
        else:
            return rows[0], False
    _clear_legacy_marker_now(spec.local_table, local_id)
    print(
        f'[cloud_sync] legacy recovery: {spec.label} {local_id} not linked '
        f'(matches={len(rows)}); uploading as a new row, possible duplicate '
        f'of an earlier upload',
        flush=True,
    )
    return None, True


def create_child_row(
    client,
    table: str,
    local_row: dict,
    payload: dict,
    *,
    parent_cloud_id: str,
    content_matches,
    summary: dict | None,
    count_adoption: bool = True,
) -> tuple[str, dict | None]:
    """State L0 for one child row: create it, or find this database's own copy.

    Returns ``(cloud_id, None)`` for a new row, and ``(cloud_id, remote_row)``
    for a row linked without any cloud write (sync-key adoption or legacy
    recovery). ``content_matches(remote_row)`` decides whether a found row
    holds what this create uploads. Nothing local is committed here. A caller
    that may leave a found row for a later step passes ``count_adoption=False``
    and counts the link itself.
    """
    spec = _spec(table)
    local_id = (local_row or {}).get('id')
    sync_key = local_sync_key(local_row)
    if not sync_key:
        raise CloudSyncError(
            f'{spec.label} {local_id}: no committed sync key; refusing to create a cloud row'
        )
    require_committed_parent(table, local_row, parent_cloud_id)
    recovered, possible_duplicate = recover_legacy_child(
        client, table, local_row,
        parent_cloud_id=parent_cloud_id, content_matches=content_matches,
    )
    if recovered is not None:
        cloud_id = str(recovered.get('id') or '').strip()
        remember_judged_child_row(client, spec.cloud_table, recovered)
        print(
            f'[cloud_sync] legacy recovery: {spec.label} {local_id} linked to cloud '
            f'{cloud_id} (no cloud write)',
            flush=True,
        )
        if count_adoption:
            _increment_sync_summary(summary, f'{spec.summary_prefix}_adopted')
        return cloud_id, recovered
    if possible_duplicate:
        _increment_sync_summary(summary, f'{spec.summary_prefix}_possible_duplicate')

    body = dict(payload or {})
    body.pop('desktop_id', None)
    body['sync_key'] = sync_key
    if spec.cloud_table == 'observation_images':
        mark_child_create_pending(local_row.get('observation_id'))
    sync_fault_point('before_request', table=spec.cloud_table)
    try:
        rows = client._post(spec.cloud_table, body)
    except CloudWriteRejectedError as exc:
        if not is_sync_key_unique_violation(exc, spec.cloud_table):
            raise
        remote = adopt_child_by_sync_key(
            client, table, local_row, sync_key,
            parent_cloud_id=parent_cloud_id, content_matches=content_matches,
        )
        cloud_id = str(remote.get('id') or '').strip()
        remember_judged_child_row(client, spec.cloud_table, remote)
        print(
            f'[cloud_sync] {spec.label} {local_id}: create already landed; adopted '
            f'cloud {cloud_id} by sync key (no PATCH)',
            flush=True,
        )
        if count_adoption:
            _increment_sync_summary(summary, f'{spec.summary_prefix}_adopted')
        return cloud_id, remote
    sync_fault_point('after_insert', table=spec.cloud_table)
    if isinstance(rows, list) and rows:
        cloud_id = str((rows[0] or {}).get('id') or '').strip()
    elif isinstance(rows, dict):
        cloud_id = str(rows.get('id') or '').strip()
    else:
        cloud_id = ''
    if not cloud_id:
        raise CloudSyncError(f'{spec.label} {local_id}: create returned no id')
    return cloud_id, None


# The image push reserves its row through ``push_image_metadata``, which
# returns only the id. A row that call linked without a write is recorded
# here so the caller never deletes, re-reserves or PATCHes it.
_ADOPTED_CHILD_ROWS_ATTR = '_adopted_child_rows'


def remember_adopted_child(client, table: str, cloud_id: str, remote: dict) -> None:
    rows = getattr(client, _ADOPTED_CHILD_ROWS_ATTR, None)
    if not isinstance(rows, dict):
        rows = {}
        setattr(client, _ADOPTED_CHILD_ROWS_ATTR, rows)
    rows[(str(table), str(cloud_id))] = dict(remote or {})


def take_adopted_child(client, table: str, cloud_id: str) -> dict | None:
    rows = getattr(client, _ADOPTED_CHILD_ROWS_ATTR, None)
    if not isinstance(rows, dict):
        return None
    return rows.pop((str(table), str(cloud_id)), None)
