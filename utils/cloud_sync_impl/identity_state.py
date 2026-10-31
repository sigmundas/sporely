"""Stateful taxonomy identity adapters: applying a remote identity locally.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from database.models import ObservationDB
from utils.taxon_identity import TaxonIdentity

from utils.cloud_sync_impl.reconciliation.identity import (
    _identification_key,
    _local_identity_is_claim,
    _local_identity_sync_key,
    _remote_identity_claim,
    _remote_name_snapshot,
    _RemoteIdentityClaim,
)


def _installed_taxon_concept(sporely_taxon_id: int):
    """The installed taxonomy-v2 artifact's record for one Sporely ID, or None."""
    try:
        from database.taxon_lookup import installed_taxon_concept
        from utils.vernacular_utils import resolve_vernacular_db_path
        return installed_taxon_concept(resolve_vernacular_db_path(), sporely_taxon_id)
    except Exception:
        return None


def _verified_cloud_selection(claim: _RemoteIdentityClaim, concept) -> TaxonIdentity | None:
    """The cloud selection proven by its own recorded provider tuple, or None.

    When the cloud row records the provider identifier that produced its
    selected Sporely ID (a web/Android client resolved ``NBIC:57217``, say)
    and the installed artifact resolves that tuple through its authoritative
    mapping to the very same concept, the desktop has re-derived the identity
    itself: it is kept as ``external_id_resolution``, exactly as a desktop
    copy of that identifier would be. Anything else returns None and the
    selection stays ``cloud_selected_unverified``.

    A selected id that the installed release presents as another concept
    (a name association member) is verified when the tuple resolves to that
    concept: the cloud's own id is kept, never replaced by the primary.
    """
    if not (claim.source_system and claim.namespace and claim.external_id):
        return None
    provider = TaxonIdentity.unresolved_external(
        source_system=claim.source_system,
        namespace=claim.namespace,
        external_id=claim.external_id,
        raw_external_id=claim.raw_external_id,
        scientific_name=concept.scientific_name,
        rank=concept.rank,
    )
    if not provider.has_external_evidence:
        return None
    try:
        from database.taxon_lookup import association_primary_id, resolve_installed_external_identity
        from utils.vernacular_utils import resolve_vernacular_db_path
        db_path = resolve_vernacular_db_path()
        resolved = resolve_installed_external_identity(
            db_path,
            source_system=provider.source_system,
            namespace=provider.namespace,
            external_id=provider.external_id,
        )
        claimed_as = association_primary_id(db_path, claim.sporely_taxon_id) if resolved is not None else None
    except Exception:
        return None
    if resolved is None or int(resolved.sporely_taxon_id) not in {int(claim.sporely_taxon_id), claimed_as}:
        return None
    proven = provider.resolved_to(
        claim.sporely_taxon_id,
        provenance=(
            f"taxonomy_v2:{resolved.release_id}:"
            f"{getattr(resolved, 'resolved_through', None) or 'taxon_external_id_text_min'}; "
            'cloud:observations.taxon_identity_*'
        ),
    )
    return proven if proven.is_proven_sporely else None


def _local_identity_columns_for_remote_claim(
    claim: _RemoteIdentityClaim,
    remote: dict,
) -> dict:
    """The complete local identity (+ name/rank snapshot) a claim maps to.

    * ``sporely`` present in the installed artifact → ``external_id_resolution``
      when the cloud's recorded provider tuple resolves locally to the same
      concept (:func:`_verified_cloud_selection`), else
      ``cloud_selected_unverified``; either way with the artifact's canonical
      name and rank;
    * ``sporely`` absent locally → preserved as an unresolved Sporely-namespace
      external identity, never a bound integer;
    * ``external`` → the cloud's preserved tuple as ``external_unresolved``;
    * ``none`` → no identity.
    """
    remote_name, remote_rank = _remote_name_snapshot(dict(remote or {}))
    if claim.kind == 'sporely':
        concept = _installed_taxon_concept(int(claim.sporely_taxon_id))
        if concept is not None:
            identity = _verified_cloud_selection(claim, concept) or TaxonIdentity.from_cloud_selection(
                claim.sporely_taxon_id,
                local_release_id=concept.release_id,
                scientific_name=concept.scientific_name,
                rank=concept.rank,
                cloud_state=claim.cloud_state,
            )
            name, rank = concept.scientific_name or remote_name, concept.rank or remote_rank
        else:
            identity = TaxonIdentity.unresolved_external(
                source_system='sporely',
                namespace='sporely_taxon_id',
                external_id=str(claim.sporely_taxon_id),
                scientific_name=remote_name,
                rank=remote_rank,
                provenance=(
                    'cloud:observations.selected_sporely_taxon_id; '
                    f"cloud_state={claim.cloud_state or 'null'}; "
                    'absent_from_local_release'
                ),
            )
            name, rank = remote_name, remote_rank
    elif claim.kind == 'external':
        identity = TaxonIdentity.unresolved_external(
            source_system=claim.source_system,
            namespace=claim.namespace,
            external_id=claim.external_id,
            raw_external_id=claim.raw_external_id,
            scientific_name=remote_name,
            rank=remote_rank,
            provenance='cloud:observations.taxon_identity_*',
        )
        name, rank = remote_name, remote_rank
    else:
        identity = TaxonIdentity.none()
        name, rank = None, None
    return {
        **identity.to_row(),
        'scientific_name_snapshot': name if identity.state != 'no_identity_evidence' else None,
        'taxon_rank_snapshot': rank if identity.state != 'no_identity_evidence' else None,
    }


IDENTITY_APPLY_APPLIED = 'applied'


IDENTITY_APPLY_UNCHANGED = 'unchanged'


IDENTITY_APPLY_CONFLICT = 'conflict'


def _apply_remote_identity_to_local(
    local_id: int,
    remote: dict,
    *,
    local_before: dict | None = None,
    fail_closed_on_local_claim: bool = False,
    absence_is_evidence: bool = False,
) -> str:
    """Write the cloud row's identity onto a local observation.

    Returns ``'unchanged'`` without writing when the row carries no identity
    information or nothing would change, ``'applied'`` after a write, and
    ``'conflict'`` without writing when ``fail_closed_on_local_claim`` is set
    and the local row holds its own claim (a proven identity or a preserved
    non-Sporely tuple) that differs from a non-empty cloud identity. Automatic
    applies with no sync baseline pass it: nothing says which side changed,
    so a disagreement is a review, never an overwrite. ``local_before`` is the
    local row as it was before this pull applied any other field. Uses the
    persistence API's one-coherent-transition write.

    ``absence_is_evidence`` is set when three-way reconciliation established
    that the cloud CLEARED its identity since the baseline; otherwise a cloud
    row without identity clears local identity only when genus/species changed.
    """
    claim = _remote_identity_claim(remote)
    if claim is None:
        return IDENTITY_APPLY_UNCHANGED
    # The local row already holds this identity. Rewriting it would replace a
    # stronger local proof (a picker-proven 83668) with the weaker
    # cloud-derived one for the very same concept.
    local_row = dict(local_before or ObservationDB.get_observation(int(local_id)) or {})
    if _local_identity_sync_key(local_row) == claim.key:
        # Same concept. Only a cloud-derived, unverified local identity may
        # still be raised to the proof the cloud's provider tuple now gives.
        if claim.kind == 'sporely' and TaxonIdentity.from_row(local_row).is_cloud_selected_unverified:
            columns = _local_identity_columns_for_remote_claim(claim, remote)
            if TaxonIdentity.from_row(columns).is_proven_sporely:
                ObservationDB.update_observation(int(local_id), allow_nulls=True, **columns)
                return IDENTITY_APPLY_APPLIED
        return IDENTITY_APPLY_UNCHANGED
    if fail_closed_on_local_claim and claim.key and _local_identity_is_claim(local_row):
        return IDENTITY_APPLY_CONFLICT
    # A cloud row with no identity is not evidence that local evidence is
    # wrong while both still name the same taxon — the push rule, mirrored.
    # It clears local identity only when the identification itself changed.
    if (
        claim.kind == 'none'
        and not absence_is_evidence
        and _identification_key(local_row) == _identification_key(dict(remote or {}))
    ):
        return IDENTITY_APPLY_UNCHANGED
    columns = _local_identity_columns_for_remote_claim(claim, remote)
    ObservationDB.update_observation(int(local_id), allow_nulls=True, **columns)
    if claim.kind == 'sporely' and columns.get('sporely_taxon_id') is None:
        print(
            f'[cloud_sync] identity pull: obs {local_id} cloud Sporely '
            f'{claim.sporely_taxon_id} is absent from the installed taxonomy '
            'artifact; preserved unresolved, not bound',
            flush=True,
        )
    return IDENTITY_APPLY_APPLIED
