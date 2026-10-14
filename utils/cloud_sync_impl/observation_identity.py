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
from datetime import datetime

from database.schema import get_connection
from utils.taxon_identity import TaxonIdentity

from utils.cloud_sync_impl.errors import (
    CloudSyncError,
    CloudWriteRejectedError,
    ObservationAdoptionConflictError,
)
from utils.cloud_sync_impl.reconciliation.location_provenance import (
    LOCATION_SOURCE_FIELD,
    LOCATION_STATUS_FIELD,
    location_pair_is_writable,
    provenance_pair,
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


def create_location_pair(client, obs: dict) -> tuple[str | None, str | None]:
    """The location pair a create of ``obs`` sends, as ``_post_new_observation`` does.

    A writable attributed pair travels with the coordinates in the INSERT;
    otherwise neither field is sent and the row is created with NULL/NULL.
    """
    payload = _observation_push_payload(obs, local=True)
    source, status = provenance_pair(obs)
    if (
        not getattr(client, '_observation_location_provenance_unsupported', False)
        and location_pair_is_writable(
            source, status, payload.get('gps_latitude'), payload.get('gps_longitude'),
        )
    ):
        return source, status
    return None, None


def _timestamp(value) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None


def remote_row_never_updated(remote: dict) -> bool:
    """Whether the cloud row is exactly as its INSERT left it.

    The server sets ``created_at`` and ``updated_at`` to the same ``now()`` on
    INSERT and bumps ``updated_at`` on every UPDATE (``trg_observations_updated_at``),
    so equal timestamps prove no write followed the create: not the create's
    own identity step, and no edit anywhere else. Missing values fail closed.
    """
    created = _timestamp(remote.get('created_at'))
    updated = _timestamp(remote.get('updated_at'))
    return created is not None and updated is not None and created == updated


def interrupted_identity_step(obs: dict, remote: dict) -> bool:
    """A proven local selection the create never got to send.

    True only when the remote has no selection and was never updated since
    its INSERT: the crash or timeout fell between the INSERT and the
    identity RPC. A selection that was set and later cleared bumped
    ``updated_at`` and is a difference instead. It only says what the read
    showed: adoption never writes the selection (the RPC takes no
    precondition), it leaves the row dirty for the ordinary push.
    """
    identity = TaxonIdentity.from_row(obs)
    return (
        identity.is_proven_sporely
        and _normalize_observation_int_value(remote.get('selected_sporely_taxon_id')) is None
        and remote_row_never_updated(remote)
    )


def observation_create_content_matches(
    obs: dict,
    remote: dict,
    *,
    location_pair: tuple[str | None, str | None] | None = None,
    allow_interrupted_identity: bool = False,
) -> bool:
    """Whether ``remote`` holds exactly what a create of ``obs`` uploads.

    Fails closed on every difference, including the taxonomy selection: the
    remote must carry the proven local Sporely identity, or none when the
    local row asserts none. With ``allow_interrupted_identity`` (adoption of
    this database's own keyed row), a missing selection on a row never
    updated since its INSERT is the create's unfinished identity step, not a
    difference (see :func:`interrupted_identity_step`); a cleared or changed
    selection still is. ``location_pair`` is the pair the create sent (see
    :func:`create_location_pair`); ``None`` skips the pair, for rows written
    by older desktops that never uploaded it.
    """
    identity = TaxonIdentity.from_row(obs)
    expected_taxon_id = identity.sporely_taxon_id if identity.is_proven_sporely else None
    remote_taxon_id = _normalize_observation_int_value(remote.get('selected_sporely_taxon_id'))
    if remote_taxon_id != expected_taxon_id and not (
        allow_interrupted_identity and interrupted_identity_step(obs, remote)
    ):
        return False
    if location_pair is not None:
        remote_pair = (remote.get(LOCATION_SOURCE_FIELD), remote.get(LOCATION_STATUS_FIELD))
        if tuple(location_pair) != remote_pair:
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
    if not observation_create_content_matches(
        obs, remote,
        location_pair=create_location_pair(client, obs),
        allow_interrupted_identity=True,
    ):
        raise ObservationAdoptionConflictError(
            f"observation {obs.get('id')}: its cloud copy {cloud_id} (same sync key) "
            f'differs from the local row; needs review',
            cloud_id=cloud_id,
            remote=remote,
        )
    return cloud_id, remote


# A link made without a cloud write (adoption, legacy recovery) is committed
# locally in one transaction with its baseline and pending status, by the
# facade's ``_commit_linked_observation``. These record, for the rest of the
# run, that the row's local state is already final: push_all must neither
# restamp it nor replace its baseline with a fresh read.
_ADOPTED_BASES_ATTR = '_adopted_observation_bases'


def remember_adopted_base(client, cloud_id: str, remote: dict) -> None:
    bases = getattr(client, _ADOPTED_BASES_ATTR, None)
    if not isinstance(bases, dict):
        bases = {}
        setattr(client, _ADOPTED_BASES_ATTR, bases)
    bases[str(cloud_id)] = dict(remote or {})


def take_adopted_base(client, cloud_id: str) -> dict | None:
    """The fetched row a committed link was judged on, once; ``None`` otherwise."""
    bases = getattr(client, _ADOPTED_BASES_ATTR, None)
    if not isinstance(bases, dict):
        return None
    return bases.pop(str(cloud_id), None)


def _clear_legacy_recovery_marker(local_id: int) -> None:
    connection = get_connection()
    try:
        connection.execute(
            f'UPDATE observations SET {LEGACY_RECOVERY_MARKER_COLUMN} = 0 WHERE id = ?',
            (int(local_id),),
        )
        connection.commit()
    finally:
        connection.close()


def recover_legacy_observation(client, obs: dict) -> tuple[dict | None, bool]:
    """One-time lookup for an older desktop's lost upload of ``obs``.

    Returns ``(remote_row, False)`` when exactly one keyless same-owner row
    carries this local id and equals the local content. Nothing is written,
    locally or to the cloud: the caller commits the link, its baseline and
    the cleared marker together. Otherwise clears the marker and returns
    ``(None, True)``: a possible duplicate. The caller then POSTs a new keyed
    row. A row found here is never PATCHed or deleted.
    """
    if not bool(obs.get(LEGACY_RECOVERY_MARKER_COLUMN)):
        return None, False
    local_id = int(obs['id'])
    rows = _read_observations(
        client, f'sync_key=is.null&desktop_id=eq.{local_id}', limit=2,
    )
    if len(rows) == 1 and observation_create_content_matches(obs, dict(rows[0] or {})):
        if str(rows[0].get('id') or '').strip():
            return dict(rows[0]), False
    # No match, several matches or different content: an earlier upload may
    # still exist under a row this lookup cannot prove, so every outcome
    # other than a link is reported as a possible duplicate.
    _clear_legacy_recovery_marker(local_id)
    print(
        f'[cloud_sync] legacy recovery: obs {local_id} not linked '
        f'(matches={len(rows)}); uploading as a new row, possible duplicate '
        f'of an earlier upload',
        flush=True,
    )
    return None, True
