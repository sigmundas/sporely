"""Pull identity: a cloud row pairs with a local row by cloud id, else by ``sync_key``.

Integer local ids are never cloud identities. Pull never reads a cloud
``desktop_id`` and never writes an identity onto a cloud row; the local
``cloud_id`` is the link.

Before pull creates a local row for a cloud row it does not know by cloud id,
it asks whether a local row already holds that row's ``sync_key``:

- no holder: the row is new here; pull creates it and stores the cloud key
  (which may be NULL), never minting one;
- a holder with no ``cloud_id``: this database created the cloud row and lost
  the answer (or a copied database did). Linking it is the upload state
  machine's job (``23505`` adoption: parent check, content compare, conflict
  on any difference), so pull leaves it alone and never creates a second
  local row;
- a holder linked to another cloud row: an identity conflict. Pull skips the
  cloud row.
"""

from __future__ import annotations

import sqlite3

from database.schema import get_connection

from utils.cloud_sync_impl.reconciliation.values import _normalize_sync_key_value

KEY_FREE = 'free'
KEY_LINKED = 'linked'
KEY_PENDING_ADOPTION = 'pending_adoption'
KEY_IDENTITY_CONFLICT = 'identity_conflict'

_LOCAL_TABLES = frozenset({'observations', 'images', 'spore_measurements'})


def remote_sync_key(remote_row: dict | None) -> str | None:
    """The cloud row's ``sync_key`` in canonical form, or ``None``."""
    return _normalize_sync_key_value((remote_row or {}).get('sync_key'))


def local_sync_key_holder(local_table: str, sync_key: str | None) -> dict | None:
    """The local row of ``local_table`` holding ``sync_key`` (``id``, ``cloud_id``)."""
    if local_table not in _LOCAL_TABLES:
        raise ValueError(f'not a keyed local table: {local_table}')
    key = _normalize_sync_key_value(sync_key)
    if not key:
        return None
    conn = get_connection()
    try:
        try:
            row = conn.execute(
                f'SELECT id, cloud_id FROM {local_table} WHERE sync_key = ? LIMIT 1',
                (key,),
            ).fetchone()
        except sqlite3.OperationalError:
            # A database without the sync_key column holds no key.
            return None
    finally:
        conn.close()
    if row is None:
        return None
    return {'id': int(row[0]), 'cloud_id': str(row[1] or '').strip()}


def classify_remote_sync_key(
    local_table: str,
    remote_row: dict | None,
    *,
    holder: dict | None = None,
    use_holder: bool = False,
) -> tuple[str, dict | None]:
    """How a cloud row's key relates to this database: one of the ``KEY_*`` outcomes.

    ``holder`` (with ``use_holder=True``) supplies an already-looked-up local
    holder, so in-memory lookups need no query.
    """
    key = remote_sync_key(remote_row)
    if not key:
        return KEY_FREE, None
    if not use_holder:
        holder = local_sync_key_holder(local_table, key)
    if holder is None:
        return KEY_FREE, None
    holder_cloud_id = str(holder.get('cloud_id') or '').strip()
    remote_cloud_id = str((remote_row or {}).get('id') or '').strip()
    if holder_cloud_id and holder_cloud_id == remote_cloud_id:
        return KEY_LINKED, holder
    if not holder_cloud_id:
        return KEY_PENDING_ADOPTION, holder
    return KEY_IDENTITY_CONFLICT, holder


def pull_key_skip_message(kind: str, remote_row: dict | None, outcome: str, holder: dict | None) -> str:
    """The warning pull records when it skips a cloud row its key ties to a local row."""
    cloud_id = str((remote_row or {}).get('id') or '').strip() or '?'
    local_id = (holder or {}).get('id') or '?'
    if outcome == KEY_PENDING_ADOPTION:
        return (
            f'cloud {kind} {cloud_id} carries the sync key of local {kind} {local_id}, '
            f'which is not linked yet; the next upload links it'
        )
    if outcome == KEY_LINKED:
        return (
            f'cloud {kind} {cloud_id} is already linked to local {kind} {local_id} '
            f'under another observation; skipped'
        )
    return (
        f'cloud {kind} {cloud_id} carries the sync key of local {kind} {local_id}, '
        f'which is linked to another cloud row; skipped as an identity conflict'
    )
