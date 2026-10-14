"""Observation create identity: ``sync_key`` adoption and one-time legacy recovery.

The per-row state machine for an observation without a ``cloud_id`` (L0):
POST with its committed local ``sync_key``. A ``23505`` naming
``observations_user_sync_key_uidx`` means this database already created the
row (a crash or timeout lost the answer): fetch it by key and owner, link it
when its content equals the local row, otherwise hand it to conflict review.
Never PATCH it. Every other refusal is a plain error.

Rows that existed before the upgrade and were never confirmed in the cloud
carry ``legacy_recovery_pending``. Before such a row's first POST, one
read-only lookup by ``desktop_id`` with ``sync_key IS NULL`` may link an
older desktop's crashed upload. It never writes to the matched row.

The cloud has no observation tombstone: an observation deleted in the cloud
is gone, so a key match is never a deleted row. A linked row whose cloud copy
is gone goes to the existing "cloud copy deleted" flow, never to a POST.
"""

from __future__ import annotations

import re
import uuid

from database.schema import get_connection
from utils.taxon_identity import TaxonIdentity

from utils.cloud_sync_impl.errors import (
    CloudSyncError,
    CloudWriteRejectedError,
    ObservationAdoptionConflictError,
)
from utils.cloud_sync_impl.push_payloads import (
    _observation_compare_payload,
    _observation_push_payload,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _SNAPSHOT_OBS_FIELDS,
    _normalize_observation_int_value,
    _observation_field_values_match,
)

SYNC_KEY_UNIQUE_INDEXES = {
    'observations': 'observations_user_sync_key_uidx',
    'observation_images': 'observation_images_user_sync_key_uidx',
    'spore_measurements': 'spore_measurements_user_sync_key_uidx',
}
LEGACY_RECOVERY_MARKER_COLUMN = 'legacy_recovery_pending'

# The create payload never carries these; the server or a later step owns them.
_CREATE_COMPARE_EXCLUDED_FIELDS = frozenset({'id', 'desktop_id', 'region_id'})


def is_sync_key_unique_violation(error: Exception, table: str) -> bool:
    """True only for ``23505`` on ``table``'s own ``*_user_sync_key_uidx``."""
    if not isinstance(error, CloudWriteRejectedError) or error.code != '23505':
        return False
    index = SYNC_KEY_UNIQUE_INDEXES.get(str(table or ''))
    if not index:
        return False
    pattern = rf'(?<![A-Za-z0-9_]){re.escape(index)}(?![A-Za-z0-9_])'
    return re.search(pattern, error.message or '') is not None


def local_sync_key(row: dict | None) -> str:
    """The row's committed local key in canonical form, or ``''``."""
    text = str((row or {}).get('sync_key') or '').strip()
    if not text:
        return ''
    try:
        return str(uuid.UUID(text))
    except ValueError:
        return ''


def observation_create_content_matches(obs: dict, remote: dict) -> bool:
    """Whether ``remote`` holds every value a create of ``obs`` uploads.

    A proven local Sporely identity must equal the remote selection, or the
    remote must have none yet: the create sets it in a step after the
    INSERT, so an interrupted create leaves it unset, never different.
    """
    identity = TaxonIdentity.from_row(obs)
    if identity.is_proven_sporely:
        remote_taxon_id = _normalize_observation_int_value(remote.get('selected_sporely_taxon_id'))
        if remote_taxon_id is not None and remote_taxon_id != identity.sporely_taxon_id:
            return False
    create_fields = set(_observation_push_payload(obs, local=True)) - _CREATE_COMPARE_EXCLUDED_FIELDS
    local_payload = _observation_compare_payload(obs, local=True)
    remote_payload = _observation_compare_payload(remote, local=False)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field not in create_fields:
            continue
        if not _observation_field_values_match(field, local_payload.get(field), remote_payload.get(field)):
            return False
    return True


def _read_observations(client, filters: str, *, limit: int) -> list[dict]:
    return client._read_observation_rows(
        client._get,
        lambda select: (
            f'observations?{filters}&user_id=eq.{client.user_id}'
            f'&select={select}&order=id.asc&limit={int(limit)}'
        ),
    )


def adopt_observation_by_sync_key(client, obs: dict, sync_key: str) -> tuple[str, dict]:
    """Resolve a create that hit this database's own row; never writes.

    Returns ``(cloud_id, remote_row)`` when the content equals the local row.
    Raises :class:`ObservationAdoptionConflictError` when it differs, and a
    plain :class:`CloudSyncError` when the keyed row is not visible.
    """
    rows = _read_observations(client, f'sync_key=eq.{sync_key}', limit=2)
    if len(rows) != 1:
        raise CloudSyncError(
            f"observation {obs.get('id')}: create reported an existing row for its "
            f'sync key, but {len(rows)} same-owner rows carry it; left for retry'
        )
    remote = dict(rows[0] or {})
    cloud_id = str(remote.get('id') or '').strip()
    if not cloud_id:
        raise CloudSyncError(f"observation {obs.get('id')}: adopted row has no id")
    if not observation_create_content_matches(obs, remote):
        raise ObservationAdoptionConflictError(
            f"observation {obs.get('id')}: its cloud copy {cloud_id} (same sync key) "
            f'differs from the local row; needs review',
            cloud_id=cloud_id,
            remote=remote,
        )
    return cloud_id, remote


def _commit_legacy_recovery_outcome(local_id: int, cloud_id: str | None) -> None:
    """Clear the one-time marker, linking ``cloud_id`` in the same transaction."""
    connection = get_connection()
    try:
        if cloud_id:
            connection.execute(
                f'UPDATE observations SET cloud_id = ?, {LEGACY_RECOVERY_MARKER_COLUMN} = 0 '
                'WHERE id = ?',
                (str(cloud_id), int(local_id)),
            )
        else:
            connection.execute(
                f'UPDATE observations SET {LEGACY_RECOVERY_MARKER_COLUMN} = 0 WHERE id = ?',
                (int(local_id),),
            )
        connection.commit()
    finally:
        connection.close()


def recover_legacy_observation(client, obs: dict) -> tuple[str | None, bool]:
    """One-time lookup for an older desktop's lost upload of ``obs``.

    Returns ``(cloud_id, False)`` when exactly one keyless same-owner row
    carries this local id and equals the local content: the row is linked
    locally and nothing is written to the cloud. Otherwise clears the marker
    and returns ``(None, possible_duplicate)``; the caller then POSTs a new
    keyed row. A row found here is never PATCHed or deleted.
    """
    if not bool(obs.get(LEGACY_RECOVERY_MARKER_COLUMN)):
        return None, False
    local_id = int(obs['id'])
    rows = _read_observations(
        client, f'sync_key=is.null&desktop_id=eq.{local_id}', limit=2,
    )
    if len(rows) == 1 and observation_create_content_matches(obs, dict(rows[0] or {})):
        cloud_id = str(rows[0].get('id') or '').strip()
        if cloud_id:
            _commit_legacy_recovery_outcome(local_id, cloud_id)
            print(
                f'[cloud_sync] legacy recovery: obs {local_id} linked to cloud {cloud_id} '
                f'(no cloud write)',
                flush=True,
            )
            return cloud_id, False
    _commit_legacy_recovery_outcome(local_id, None)
    possible_duplicate = bool(rows)
    if possible_duplicate:
        print(
            f'[cloud_sync] legacy recovery: obs {local_id} not linked '
            f'(matches={len(rows)}); uploading as a new row, possible duplicate '
            f'of an earlier upload',
            flush=True,
        )
    return None, possible_duplicate
