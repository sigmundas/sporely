"""Pure taxonomy identity classification for sync.

Structured identity is authoritative; name text never substitutes for it.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from dataclasses import dataclass

from utils.taxon_identity import (
    IDENTITY_COLUMNS as _TAXON_IDENTITY_COLUMNS,
    TaxonIdentity,
)

from utils.cloud_sync_impl.reconciliation.values import _normalize_observation_int_value


def _identification_key(row: dict) -> tuple[str, str]:
    """Case/whitespace-insensitive (genus, species) of an observation row."""
    return tuple(
        ' '.join(str(row.get(field) or '').split()).casefold()
        for field in ('genus', 'species')
    )


def _identification_contradicts_remote(local_row: dict, remote_row: dict) -> bool:
    """Whether the local row's OWN committed identification positively
    contradicts the remote row's, component by component.

    A blank/missing local ``genus`` or ``species`` is unknown, not
    contradictory — the known production shape is a cloud row with a bound
    concept and no local genus/species at all (see sync-integrity follow-up 4
    and the read-only production audit it cites). Only a POPULATED local
    component that differs, case/whitespace-insensitively, from the
    corresponding remote component counts as a contradiction. Compatible
    partial information (blank, or matching) is left to the existing
    adoption path (``cloud_selected_unverified`` etc.) — this function only
    decides whether the Case F block applies, never whether to adopt.
    """
    local_key = _identification_key(local_row)
    remote_key = _identification_key(remote_row)
    return any(
        local_component and local_component != remote_component
        for local_component, remote_component in zip(local_key, remote_key)
    )


#: The virtual observation field under which the taxonomy identity takes part
#: in snapshots, change detection, conflict reporting and field resolution. It
#: is never a cloud column and never pushed: the guarded RPC stays the only
#: desktop → cloud identity channel.
TAXON_IDENTITY_SYNC_FIELD = 'taxon_identity'


#: Cloud identity-provenance columns (sporely-web migration 20260922120000).
#: Selected only when the server has them — see
#: `SporelyCloudClient._observation_select_columns`.
_OBSERVATION_IDENTITY_SELECT_COLUMNS = (
    'taxon_identity_state',
    'taxon_identity_source_system',
    'taxon_identity_namespace',
    'taxon_identity_external_id',
    'taxon_identity_raw_external_id',
)


@dataclass(frozen=True)
class _RemoteIdentityClaim:
    """What one cloud observation row says about its taxonomy identity."""

    kind: str  # 'none' | 'sporely' | 'external'
    sporely_taxon_id: int | None = None
    cloud_state: str | None = None
    source_system: str | None = None
    namespace: str | None = None
    external_id: str | None = None
    raw_external_id: str | None = None

    @property
    def key(self) -> str:
        return _identity_sync_key(
            self.kind, self.sporely_taxon_id,
            self.source_system, self.namespace, self.external_id,
        )


def _identity_sync_key(kind, sporely_taxon_id, source_system, namespace, external_id) -> str:
    """Canonical comparison value shared by local, remote and baseline sides."""
    if kind == 'sporely' and sporely_taxon_id:
        return f'sporely:{int(sporely_taxon_id)}'
    if kind == 'external' and source_system and namespace and external_id:
        return f'external:{source_system}:{namespace}:{external_id}'
    return ''


def _remote_identity_claim(remote: dict | None) -> _RemoteIdentityClaim | None:
    """Read the cloud row's identity, or ``None`` when it carries none at all.

    ``None`` means the row did not include the identity columns (an older
    server, or a partial row) — which is "no information", distinct from a
    row that explicitly has no identity. Callers leave local identity alone.
    """
    row = dict(remote or {})
    if 'selected_sporely_taxon_id' not in row and 'taxon_identity_state' not in row:
        return None
    state = str(row.get('taxon_identity_state') or '').strip() or None
    selected = _normalize_observation_int_value(row.get('selected_sporely_taxon_id'))
    if selected is not None and selected > 0:
        return _RemoteIdentityClaim(kind='sporely', sporely_taxon_id=selected, cloud_state=state)
    tuple_values = tuple(
        str(row.get(column) or '').strip() or None
        for column in (
            'taxon_identity_source_system',
            'taxon_identity_namespace',
            'taxon_identity_external_id',
            'taxon_identity_raw_external_id',
        )
    )
    if state == 'external_unresolved' and all(tuple_values[:3]):
        return _RemoteIdentityClaim(
            kind='external', cloud_state=state,
            source_system=tuple_values[0], namespace=tuple_values[1],
            external_id=tuple_values[2], raw_external_id=tuple_values[3],
        )
    return _RemoteIdentityClaim(kind='none', cloud_state=state)


def _withhold_identity_from_push(push_payload: dict) -> None:
    """Make this push carry no identity, so the RPC gate skips it (never clears)."""
    push_payload['sporely_taxon_id'] = None
    for column in _TAXON_IDENTITY_COLUMNS:
        push_payload[column] = None


def _remote_row_without_identity(remote: dict | None) -> dict:
    """The cloud row minus its identity columns ("no identity information")."""
    return {
        key: value
        for key, value in dict(remote or {}).items()
        if key != 'selected_sporely_taxon_id' and key not in _OBSERVATION_IDENTITY_SELECT_COLUMNS
    }


def _local_identity_sync_key(local_obs: dict | None) -> str:
    """The local row's identity in the same vocabulary as `_RemoteIdentityClaim.key`.

    A Sporely-namespace external identity (a cloud ID the local artifact could
    not confirm) compares as the Sporely ID it preserves, so holding it is not
    a perpetual difference from the cloud. A legacy-unverified integer never
    equals a cloud value: nothing records what it is.
    """
    identity = TaxonIdentity.from_row(local_obs)
    if identity.is_proven_sporely or identity.is_cloud_selected_unverified:
        return _identity_sync_key('sporely', identity.sporely_taxon_id, None, None, None)
    if identity.is_legacy_unverified:
        return f'legacy:{identity.sporely_taxon_id}'
    if identity.state == 'external_unresolved' and identity.has_external_evidence:
        if (identity.source_system, identity.namespace) == ('sporely', 'sporely_taxon_id'):
            sporely_id = _normalize_observation_int_value(identity.external_id)
            return _identity_sync_key('sporely', sporely_id, None, None, None)
        return _identity_sync_key(
            'external', None,
            identity.source_system, identity.namespace, identity.external_id,
        )
    return ''


#: Returned by `_baseline_identity_key` for a snapshot stored before identity
#: joined change detection. Nothing records what the cloud identity was then.
_IDENTITY_BASELINE_UNKNOWN = object()


def _baseline_identity_key(baseline_obs: dict | None):
    row = dict(baseline_obs or {})
    if TAXON_IDENTITY_SYNC_FIELD not in row:
        return _IDENTITY_BASELINE_UNKNOWN
    return str(row.get(TAXON_IDENTITY_SYNC_FIELD) or '')


def _remote_identity_changed_since(remote: dict | None, baseline_obs: dict | None) -> bool:
    """Whether the cloud identity differs from the stored sync baseline.

    With an unknown baseline, any cloud identity counts as a change worth
    reconciling once; after that the snapshot records it.
    """
    claim = _remote_identity_claim(remote)
    if claim is None:
        return False
    baseline = _baseline_identity_key(baseline_obs)
    if baseline is _IDENTITY_BASELINE_UNKNOWN:
        return claim.key != ''
    return claim.key != baseline


def _local_identity_is_claim(local_obs: dict | None) -> bool:
    """A local identity that is the desktop's own evidence, not cloud-derived.

    Proven Sporely identities and preserved non-Sporely external identifiers
    are claims; legacy integers, cloud-selected tokens, unconfirmable cloud
    Sporely IDs, manual text and no identity are not.
    """
    identity = TaxonIdentity.from_row(local_obs)
    if identity.is_proven_sporely:
        return True
    return (
        identity.state == 'external_unresolved'
        and identity.has_external_evidence
        and (identity.source_system, identity.namespace) != ('sporely', 'sporely_taxon_id')
    )


def _classify_identity_sync_change(
    local_obs: dict | None,
    remote_obs: dict | None,
    baseline_obs: dict | None,
    *,
    identification_locally_owned: bool,
) -> str | None:
    """Three-way classification of the taxonomy identity for one observation.

    Returns ``'remote_only'`` (adopt the cloud identity), ``'local_only'``
    (push it — only a proven identity, the one kind the RPC gate accepts),
    ``'conflict'`` (fail closed: review required, nothing applied),
    ``'shared'`` (both sides moved to the same identity) or ``None``.

    ``identification_locally_owned`` is True when genus/species changed
    locally or conflict: identity follows the identification it names, so a
    remote identity change against a locally edited identification is a
    conflict, never a silent adoption.

    See docs/supabase-sync-contract.md "Identity in change detection".
    """
    claim = _remote_identity_claim(remote_obs)
    if claim is None:
        return None
    remote_key = claim.key
    local_key = _local_identity_sync_key(local_obs)
    baseline = _baseline_identity_key(baseline_obs)
    if local_key == remote_key:
        if baseline is not _IDENTITY_BASELINE_UNKNOWN and baseline != remote_key:
            return 'shared'
        return None
    local_is_claim = _local_identity_is_claim(local_obs)
    local_is_proven = TaxonIdentity.from_row(local_obs).is_proven_sporely
    if baseline is _IDENTITY_BASELINE_UNKNOWN:
        # Nothing says who changed. Two different claims disagree: fail
        # closed. A proven local identity with nothing in the cloud is the
        # desktop's own pick awaiting the RPC. A non-claim local takes the
        # cloud identity unless the identification itself is being edited
        # locally.
        if local_is_claim:
            if remote_key:
                return 'conflict'
            return 'local_only' if local_is_proven else None
        if not remote_key or identification_locally_owned:
            return None
        return 'remote_only'
    remote_changed = remote_key != baseline
    local_changed = local_is_claim and local_key != baseline
    if remote_changed and (local_changed or identification_locally_owned):
        return 'conflict'
    if remote_changed:
        return 'remote_only'
    if local_changed and local_is_proven:
        return 'local_only'
    return None


def _remote_name_snapshot(remote: dict) -> tuple[str | None, str | None]:
    genus = str(remote.get('genus') or '').strip()
    species = str(remote.get('species') or '').strip()
    name = ' '.join(part for part in (genus, species) if part) or None
    rank = 'species' if genus and species else ('genus' if genus else None)
    return name, rank
