from __future__ import annotations

from utils.cloud_sync import SporelyCloudClient
from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_SPORELY,
    TaxonIdentity,
)


def _client_with_existing_observation(remote: dict):
    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "1184"
    client._patch = lambda *_args, **_kwargs: None
    calls: list[tuple[str, dict]] = []
    client._rpc = lambda name, payload: calls.append((name, payload))
    return client, calls


def _artifact_proven(sporely_taxon_id: int) -> dict:
    """Row columns for an identity proven by the taxonomy-v2 artifact."""
    return TaxonIdentity.from_taxonomy_v2_artifact(sporely_taxon_id).to_row()


def test_taxonomy_aware_observation_syncs_exact_selected_taxon():
    remote = {
        "id": 1184,
        "selected_sporely_taxon_id": None,
    }
    client, calls = _client_with_existing_observation(remote)

    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            **_artifact_proven(634856),
        },
        remote_obs=remote,
    )

    assert calls == [
        (
            "set_observation_selected_taxon_v2",
            {
                "p_observation_id": 1184,
                "p_sporely_taxon_id": 634856,
            },
        )
    ]


def test_externally_resolved_identity_also_syncs():
    """A resolved namespaced external identifier is equally authoritative."""
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(remote)
    identity = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="53482",
        raw_external_id="NBIC:53482",
    ).resolved_to(7821)
    assert identity.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION

    client.push_observation(
        {"id": 42, "cloud_id": "1184", **identity.to_row()},
        remote_obs=remote,
    )

    assert calls == [(
        "set_observation_selected_taxon_v2",
        {"p_observation_id": 1184, "p_sporely_taxon_id": 7821},
    )]


def test_exact_taxon_sync_skips_noop_and_never_infers_from_name_text():
    matching = {"id": 1184, "selected_sporely_taxon_id": 634856}
    client, calls = _client_with_existing_observation(matching)
    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            "genus": "Amanita",
            "species": "muscaria",
            **_artifact_proven(634856),
        },
        remote_obs=matching,
    )
    assert calls == []

    unresolved = {"id": 1185, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(unresolved)
    client._resolve_existing_observation_for_push = (
        lambda _obs, remote_obs=None: "1185"
    )
    client.push_observation(
        {
            "id": 43,
            "cloud_id": "1185",
            "sporely_taxon_id": None,
            "genus": "Amanita",
            "species": "muscaria",
        },
        remote_obs=unresolved,
    )
    assert calls == []


# ── Stage 2 Part A regression #7: the proof standard is provenance ──────────


def test_sync_skips_unresolved_external_identity_even_when_it_collides():
    """Regression #2 + #7 combined, and the residual risk the server cannot see.

    ``634856`` is simultaneously a real Sporely ID in the active release
    (asserted by the first test in this module) and, here, a NorTaxa external
    identifier. The deployed ``set_observation_selected_taxon_v2`` validates
    active-release membership, so it would ACCEPT this integer — it has no way
    to know the digits came from a different registry. Nothing server-side can
    catch a numeric collision, so the client must refuse to emit it.

    The external evidence is kept, not erased: refusing to resolve is a state,
    not grounds to destroy the source prediction.
    """
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(remote)
    identity = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="634856",
        raw_external_id="NBIC:634856",
    )
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.is_proven_sporely is False

    row = identity.to_row()
    assert row["sporely_taxon_id"] is None
    assert row["taxon_identity_external_id"] == "634856"
    assert row["taxon_identity_raw_external_id"] == "NBIC:634856"

    client.push_observation(
        {"id": 42, "cloud_id": "1184", **row}, remote_obs=remote,
    )
    assert calls == []


def test_sync_skips_a_raw_integer_smuggled_into_the_identity_column():
    """An unproven integer in ``sporely_taxon_id`` is not emitted.

    Covers the case where an external integer reaches the column through a
    path that recorded external provenance beside it. The provenance is what
    decides; the integer's sign and magnitude are not evidence.
    """
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(remote)
    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            "sporely_taxon_id": 634856,
            "taxon_identity_state": STATE_EXTERNAL_UNRESOLVED,
            "taxon_identity_source_system": "nortaxa",
            "taxon_identity_namespace": "nortaxa_taxon_id",
            "taxon_identity_external_id": "634856",
        },
        remote_obs=remote,
    )
    assert calls == []


def test_sync_skips_a_sporely_state_with_no_recorded_proof():
    """A state claiming Sporely identity without a proof token is not trusted."""
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(remote)
    client.push_observation(
        {
            "id": 42,
            "cloud_id": "1184",
            "sporely_taxon_id": 634856,
            "taxon_identity_state": STATE_SPORELY,
            "taxon_identity_proof": None,
        },
        remote_obs=remote,
    )
    assert calls == []


def test_sync_skips_a_legacy_integer_until_it_is_reverified():
    """A pre-Stage-2 row carries no provenance, so it is unverified.

    ``database/migrate_observations_sporely_id.py`` promotes such a row to
    ``PROOF_TAXONOMY_V2_ARTIFACT`` once the integer still verifies against
    ``taxon_min``; until then the client does not assert it as an
    owner-selected Sporely identity. This is the behaviour change that makes
    the gate mean something — grandfathering every pre-existing value would
    enforce nothing.
    """
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client, calls = _client_with_existing_observation(remote)
    legacy_row = {"id": 42, "cloud_id": "1184", "sporely_taxon_id": 634856}
    assert TaxonIdentity.from_row(legacy_row).is_legacy_unverified is True

    client.push_observation(dict(legacy_row), remote_obs=remote)
    assert calls == []

    # After re-verification the same integer is emitted.
    client, calls = _client_with_existing_observation(remote)
    client.push_observation(
        {"id": 42, "cloud_id": "1184", **_artifact_proven(634856)},
        remote_obs=remote,
    )
    assert calls == [(
        "set_observation_selected_taxon_v2",
        {"p_observation_id": 1184, "p_sporely_taxon_id": 634856},
    )]
    assert _artifact_proven(634856)["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT


def test_identity_provenance_columns_are_never_pushed_as_observation_fields():
    """The cloud learns identity only through the guarded RPC.

    Pushing provenance as ordinary observation columns would create a second,
    ungated identity channel alongside
    ``set_observation_selected_taxon_v2``.
    """
    from utils.cloud_sync import _OBS_PUSH_COLS
    from utils.taxon_identity import IDENTITY_COLUMNS

    assert not (set(_OBS_PUSH_COLS) & set(IDENTITY_COLUMNS))
    assert not (set(_OBS_PUSH_COLS) & {"sporely_taxon_id"})
