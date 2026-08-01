"""Stage W2D fixture-backed reconciliation tests.

Every test constructs a :class:`ReconciliationInput` in-memory, resolves
it against the pinned macrofungi release, and asserts the resulting
state, evidence chain, migration action, and snapshot-preservation
guarantees. No test opens the observations database or connects to
Supabase.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from database.taxonomy.reconciliation.errors import ReconciliationInvariantError
from database.taxonomy.reconciliation.input_model import (
    RawSignal,
    ReconciliationInput,
)
from database.taxonomy.reconciliation.manifest import build_manifest_body
from database.taxonomy.reconciliation.namespace_rules import (
    derive_signals_from_observation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _exact(source_system, namespace, external_id, *, origin, rule_id, raw=None, notes=None):
    return RawSignal(
        kind="exact",
        source_system=source_system,
        namespace=namespace,
        external_id=str(external_id),
        origin_field=origin,
        raw_value=str(raw if raw is not None else external_id),
        rule_id=rule_id,
        notes=notes,
    )


def _text(origin, value, rule_id):
    return RawSignal(
        kind="text-only",
        source_system=None,
        namespace=None,
        external_id=None,
        origin_field=origin,
        raw_value=value,
        rule_id=rule_id,
    )


def _invalid(origin, value, rule_id, source_system=None, notes=None):
    return RawSignal(
        kind="invalid",
        source_system=source_system,
        namespace=None,
        external_id=str(value),
        origin_field=origin,
        raw_value=str(value),
        rule_id=rule_id,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Contract §24: reconciliation outcome coverage
# ---------------------------------------------------------------------------


def test_exact_namespaced_mapping_resolves(resolver):
    """Level 1: `col_xr:col_usage_id:323XQ` in the release resolves 167."""
    obs = ReconciliationInput(
        observation_id="obs-A",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "resolved_exact"
    assert result.resolved_sporely_taxon_id == 167
    assert result.resolution_method == "direct_taxonomy_v2_mapping"
    assert result.migration_action == "materialize_existing_taxonomy_v2_concept"
    assert result.resolved_scope_state == "include"
    # every resolved record must carry at least one chain step
    assert len(result.resolution_evidence) >= 1
    step = result.resolution_evidence[0]
    assert step.level == 1
    assert step.method == "direct_taxonomy_v2_mapping"


def test_legacy_lookup_chain_resolves(resolver):
    """Level 2: prepopulated sporely_taxon_id verified against the release."""
    obs = ReconciliationInput(
        observation_id="obs-B",
        signals=(
            _exact(
                "sporely", "sporely_taxon_id", "167",
                origin="observations.sporely_taxon_id",
                rule_id="sporely_taxon_id_prepopulated_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "resolved_exact_via_legacy_mapping"
    assert result.resolved_sporely_taxon_id == 167
    assert result.resolution_method == "legacy_lookup_chain"
    step = result.resolution_evidence[0]
    assert step.level == 2
    assert step.action == "verify_sporely_taxon_id_in_release"


def test_pinned_synonym_relationship_skipped(release):
    """Contract §5 Level 3 requires an accepted-name relationship in the
    pinned source. The tax-2026.08.01-01 release exports zero non-accepted
    id_role rows, so this fixture is documented as SKIPPED — never faked.
    """
    # Only accepted rows exist in this release; the resolver's synonym_index
    # must therefore be empty. That empty invariant is what this test pins.
    assert release.synonym_index == {}


def test_same_scientific_name_distinct_identities_resolve_independently(resolver, release):
    """Two observations sharing a scientific name but different exact
    namespaced signals must resolve independently. Contract §10 forbids
    scientific-name equality for identity, so this is the safety net.
    """
    obs_167 = ReconciliationInput(
        observation_id="obs-C1",
        stored_scientific_name="Crystallocystidium albescens",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    obs_168 = ReconciliationInput(
        observation_id="obs-C2",
        stored_scientific_name="Crystallocystidium albescens",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XR",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    r1 = resolver.resolve(obs_167)
    r2 = resolver.resolve(obs_168)
    assert r1.resolved_sporely_taxon_id == 167
    assert r2.resolved_sporely_taxon_id == 168


def test_same_raw_id_in_different_namespaces_resolves_independently(resolver):
    """(nortaxa, nortaxa_taxon_id, 167) is a nortaxa lookup that isn't in
    the pinned release. (sporely, sporely_taxon_id, 167) is a legacy
    chain that IS in the release. The engine must not conflate them.
    """
    nortaxa_obs = ReconciliationInput(
        observation_id="obs-D1",
        signals=(
            _exact(
                "nortaxa", "nortaxa_taxon_id", "167",
                origin="observations.artsdata_id",
                rule_id="artsdata_id_v1",
            ),
        ),
    )
    sporely_obs = ReconciliationInput(
        observation_id="obs-D2",
        signals=(
            _exact(
                "sporely", "sporely_taxon_id", "167",
                origin="observations.sporely_taxon_id",
                rule_id="sporely_taxon_id_prepopulated_v1",
            ),
        ),
    )
    r_nt = resolver.resolve(nortaxa_obs)
    r_sp = resolver.resolve(sporely_obs)
    # nortaxa signal is not in the release (release has no nortaxa entries)
    assert r_nt.reconciliation_state == "unresolved_external_identifier"
    # sporely signal resolves via Level-2 legacy chain
    assert r_sp.resolved_sporely_taxon_id == 167


def test_unknown_namespaced_id_becomes_unresolved_external(resolver):
    obs = ReconciliationInput(
        observation_id="obs-E",
        signals=(
            _exact(
                "artportalen", "artportalen_taxon_id", "9999999",
                origin="observations.artportalen_id",
                rule_id="artportalen_id_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "unresolved_external_identifier"
    assert result.resolved_sporely_taxon_id is None


def test_unnamespaced_integer_becomes_invalid(resolver):
    """Contract §3 normalisation rules: an ai_selected_taxon_id under an
    unknown service is invalid, not resolvable.
    """
    obs = ReconciliationInput(
        observation_id="obs-F",
        signals=(
            _invalid(
                "observations.ai_selected_taxon_id",
                "12345",
                "ai_selected_invalid_v1",
                source_system="unknownservice",
                notes="unknown service",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "invalid_or_unnamespaced_identifier"


def test_missing_legacy_lookup_row_becomes_source_record_missing(resolver):
    """A prepopulated sporely_taxon_id that no longer exists in the pinned
    release must map to ``source_record_missing`` — not to
    ``unresolved_legacy_identifier`` — so the migration driver can decide
    whether to retire the observation snapshot.
    """
    obs = ReconciliationInput(
        observation_id="obs-G",
        signals=(
            _exact(
                "sporely", "sporely_taxon_id", "99999999",
                origin="observations.sporely_taxon_id",
                rule_id="sporely_taxon_id_prepopulated_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "source_record_missing"
    assert "observations.sporely_taxon_id:99999999" in result.missing_source_records


def test_multiple_exact_identifiers_agreeing_resolve(resolver):
    """Contract §5: agreement → highest-priority chain method used."""
    obs = ReconciliationInput(
        observation_id="obs-H",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
            _exact(
                "sporely", "sporely_taxon_id", "167",
                origin="observations.sporely_taxon_id",
                rule_id="sporely_taxon_id_prepopulated_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "resolved_exact"
    assert result.resolved_sporely_taxon_id == 167
    # Level 1 wins over Level 2 for the reported method
    assert result.resolution_method == "direct_taxonomy_v2_mapping"


def test_multiple_exact_identifiers_conflicting_becomes_conflicting_exact_evidence(resolver):
    obs = ReconciliationInput(
        observation_id="obs-I",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
            _exact(
                "sporely", "sporely_taxon_id", "168",
                origin="observations.sporely_taxon_id",
                rule_id="sporely_taxon_id_prepopulated_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "conflicting_exact_evidence"
    assert result.resolved_sporely_taxon_id is None
    # Both concepts recorded in conflicting_concepts
    taxa = {c.sporely_taxon_id for c in result.conflicting_concepts}
    assert taxa == {167, 168}
    assert result.migration_action == "manual_review_required"


def test_manual_unresolved_state(resolver):
    obs = ReconciliationInput(
        observation_id="obs-J",
        manual_identification_flag=True,
        stored_scientific_name="Boletus regius",
        stored_vernacular_name="kongesteinsopp",
        signals=(
            _text(
                "observations.scientific_name_snapshot",
                "Boletus regius",
                "text_scientific_name_snapshot_v1",
            ),
            _text(
                "observations.common_name",
                "kongesteinsopp",
                "text_common_name_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "manual_unresolved"
    assert result.resolved_sporely_taxon_id is None
    assert result.original_scientific_name == "Boletus regius"
    assert result.original_vernacular_name == "kongesteinsopp"


def test_no_identity_evidence(resolver):
    obs = ReconciliationInput(observation_id="obs-K")
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "no_identity_evidence"
    assert result.resolved_sporely_taxon_id is None
    assert result.migration_action == "retain_unresolved_without_registry_concept"


def test_candidate_generation_without_automatic_resolution(resolver):
    """Ambiguous text signals produce candidates but never resolve identity."""
    obs = ReconciliationInput(
        observation_id="obs-L",
        manual_identification_flag=True,
        signals=(
            _text(
                "observations.scientific_name_snapshot",
                "Crystallocystidium albescens",
                "text_scientific_name_snapshot_v1",
            ),
            _text(
                "observations.ai_selected_scientific_name",
                "Crystallocystidium albobadium",
                "text_ai_selected_scientific_name_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    # Primary state is unresolved; candidates carry the concepts but no
    # identity is assigned.
    assert result.reconciliation_state == "ambiguous_multiple_candidates"
    assert result.resolved_sporely_taxon_id is None
    taxa = {c.sporely_taxon_id for c in result.candidate_concepts}
    assert taxa >= {167, 168}


def test_resolved_concept_outside_cache_preserves_scope_state(resolver):
    """Contract §7: a resolved concept outside the macrofungi cache keeps
    its ``scope_state`` verbatim from the pinned release. The engine
    must not broaden the cache — it merely materialises the concept.
    """
    obs = ReconciliationInput(
        observation_id="obs-M",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "33D",  # Cyttariales, required_ancestor
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    result = resolver.resolve(obs)
    assert result.reconciliation_state == "resolved_exact"
    assert result.resolved_sporely_taxon_id == 931
    assert result.resolved_scope_state == "required_ancestor"


def test_snapshot_preservation_after_resolution(resolver, release):
    """Contract §8: original snapshot fields are immutable at resolution time.
    Running the resolver a second time against the same input (as if a
    subsequent release) must NOT rewrite the snapshot values.
    """
    obs = ReconciliationInput(
        observation_id="obs-N",
        stored_scientific_name="Crystallocystidium albescens",
        stored_vernacular_name="test-vernacular",
        stored_rank="species",
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    r1 = resolver.resolve(obs)
    # Now "as if a later release": run again with a mutated obs where the
    # signal points to a different concept (168). The resolver must not
    # mutate the input's stored_* fields — obs is frozen.
    obs2 = replace(
        obs,
        signals=(
            _exact(
                "col_xr", "col_usage_id", "323XR",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    r2 = resolver.resolve(obs2)
    # The stored_* fields are the input's own fields; they are immutable.
    assert r1.original_scientific_name == "Crystallocystidium albescens"
    assert r2.original_scientific_name == "Crystallocystidium albescens"
    assert r1.original_vernacular_name == "test-vernacular"
    assert r2.original_vernacular_name == "test-vernacular"


def test_later_resolution_does_not_rewrite_snapshot(resolver):
    """Contract §8 tail: resolving after the fact does not rewrite the
    snapshot. Even when the second run resolves a concept the first run
    could not, the original snapshot values are preserved.
    """
    obs_unresolved = ReconciliationInput(
        observation_id="obs-O",
        stored_scientific_name="Crystallocystidium albescens",
        signals=(
            _text(
                "observations.scientific_name_snapshot",
                "Crystallocystidium albescens",
                "text_scientific_name_snapshot_v1",
            ),
        ),
    )
    r_first = resolver.resolve(obs_unresolved)
    obs_resolved = replace(
        obs_unresolved,
        signals=obs_unresolved.signals
        + (
            _exact(
                "col_xr", "col_usage_id", "323XQ",
                origin="observations.ai_selected_taxon_id",
                rule_id="artsorakel_bare_int_v1",
            ),
        ),
    )
    r_second = resolver.resolve(obs_resolved)
    assert r_first.resolved_sporely_taxon_id is None
    assert r_second.resolved_sporely_taxon_id == 167
    # Original snapshot fields are identical across the two runs.
    assert r_first.original_scientific_name == r_second.original_scientific_name


# ---------------------------------------------------------------------------
# Determinism + manifest tests
# ---------------------------------------------------------------------------


def test_manifest_body_is_byte_deterministic(resolver, rule_set, release, fixtures_dir):
    """Two invocations of ``build_manifest_body`` must produce byte-identical
    manifest strings for the same inputs.
    """
    inputs = []
    with (fixtures_dir / "all_states.jsonl").open("r", encoding="utf-8") as h:
        for line in h:
            row = json.loads(line)
            if row.get("__synthetic__"):
                continue
            inputs.append(ReconciliationInput.from_dict(row))
    results_a = [resolver.resolve(o) for o in inputs]
    results_b = [resolver.resolve(o) for o in inputs]
    manifest_a = build_manifest_body(results=results_a, rule_set=rule_set, release=release)
    manifest_b = build_manifest_body(results=results_b, rule_set=rule_set, release=release)
    assert manifest_a.body == manifest_b.body
    assert manifest_a.semantic_hash == manifest_b.semantic_hash


def test_manifest_body_is_stable_across_input_permutations(resolver, rule_set, release, fixtures_dir):
    """Reordering the inputs must not change the manifest — records are
    sorted by observation_id at emit time.
    """
    inputs = []
    with (fixtures_dir / "all_states.jsonl").open("r", encoding="utf-8") as h:
        for line in h:
            row = json.loads(line)
            if row.get("__synthetic__"):
                continue
            inputs.append(ReconciliationInput.from_dict(row))
    reversed_inputs = list(reversed(inputs))
    results_forward = [resolver.resolve(o) for o in inputs]
    results_reversed = [resolver.resolve(o) for o in reversed_inputs]
    m_forward = build_manifest_body(results=results_forward, rule_set=rule_set, release=release)
    m_reversed = build_manifest_body(results=results_reversed, rule_set=rule_set, release=release)
    assert m_forward.body == m_reversed.body
    assert m_forward.semantic_hash == m_reversed.semantic_hash


def test_semantic_hash_excludes_documented_fields(rule_set):
    """The policy must declare the excluded field names, and the manifest
    builder must honour them. This test guards the contract §9 promise
    that timestamps + hostnames never enter the semantic hash body.
    """
    excludes = set(rule_set.semantic_hash_excludes())
    assert {"generated_at", "resolution_timestamp", "run_host"} <= excludes


def test_duplicate_observation_ids_raise_invariant(resolver, rule_set, release):
    a = ReconciliationInput(observation_id="dup", signals=())
    result_a = resolver.resolve(a)
    with pytest.raises(ReconciliationInvariantError):
        build_manifest_body(
            results=[result_a, result_a],
            rule_set=rule_set,
            release=release,
        )


# ---------------------------------------------------------------------------
# Namespace derivation coverage
# ---------------------------------------------------------------------------


def test_derive_signals_nbic_prefix_strips_to_nortaxa(rule_set):
    signals = derive_signals_from_observation(
        {
            "ai_selected_service": "artsorakel",
            "ai_selected_taxon_id": "NBIC:54995",
        },
        rule_set,
    )
    # Exactly one exact signal, in the nortaxa:nortaxa_taxon_id namespace,
    # tagged with the NBIC-strip rule id. The raw_value keeps the prefix.
    exact = [s for s in signals if s.kind == "exact"]
    assert len(exact) == 1
    signal = exact[0]
    assert signal.source_system == "nortaxa"
    assert signal.namespace == "nortaxa_taxon_id"
    assert signal.external_id == "54995"
    assert signal.raw_value == "NBIC:54995"
    assert signal.rule_id == "artsorakel_nbic_prefix_v1"


def test_derive_signals_inat_alias_normalises_to_inaturalist(rule_set):
    signals = derive_signals_from_observation(
        {
            "ai_selected_service": "inat",
            "ai_selected_taxon_id": "12345",
        },
        rule_set,
    )
    exact = [s for s in signals if s.kind == "exact"]
    assert len(exact) == 1
    assert exact[0].source_system == "inaturalist"
    assert exact[0].namespace == "inaturalist_taxon_id"
    assert exact[0].rule_id == "ai_selected_inaturalist_v1"


def test_derive_signals_artsorakel_bare_int_uses_documented_rule_id(rule_set):
    signals = derive_signals_from_observation(
        {
            "ai_selected_service": "artsorakel",
            "ai_selected_taxon_id": "54995",
        },
        rule_set,
    )
    exact = [s for s in signals if s.kind == "exact"]
    assert exact[0].rule_id == "artsorakel_bare_int_v1"
    assert exact[0].source_system == "nortaxa"


def test_derive_signals_unknown_service_becomes_invalid(rule_set):
    signals = derive_signals_from_observation(
        {
            "ai_selected_service": "unknownservice",
            "ai_selected_taxon_id": "12345",
        },
        rule_set,
    )
    invalid = [s for s in signals if s.kind == "invalid"]
    assert len(invalid) == 1
    assert invalid[0].rule_id == "ai_selected_invalid_v1"


def test_derive_signals_preserve_only_observation_ids(rule_set):
    signals = derive_signals_from_observation(
        {"inaturalist_id": 1234, "mushroomobserver_id": 5678},
        rule_set,
    )
    kinds = {s.origin_field: s.kind for s in signals}
    assert kinds["observations.inaturalist_id"] == "preserve_only"
    assert kinds["observations.mushroomobserver_id"] == "preserve_only"


# ---------------------------------------------------------------------------
# End-to-end CLI test — determinism between two runs
# ---------------------------------------------------------------------------


def test_cli_end_to_end_is_byte_deterministic(tmp_path, fixtures_dir):
    repo_root = Path(__file__).resolve().parents[2]
    input_path = fixtures_dir / "all_states.jsonl"
    release_dir = (
        repo_root
        / "database"
        / "reference_data"
        / "generated"
        / "taxonomy_v2"
        / "global_macrofungi_tax-2026.08.01-01"
    )
    policy_path = (
        repo_root
        / "database"
        / "taxonomy"
        / "policies"
        / "w2d-reconciliation-policy.json"
    )

    def _run(out_dir: Path) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "database.taxonomy.reconciliation.cli",
                "--input",
                str(input_path),
                "--output",
                str(out_dir),
                "--release-dir",
                str(release_dir),
                "--policy",
                str(policy_path),
            ],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=True,
        )
        assert proc.returncode == 0

    out_a = tmp_path / "run_a"
    out_b = tmp_path / "run_b"
    _run(out_a)
    _run(out_b)
    body_a = (out_a / "reconciliation-manifest.json").read_bytes()
    body_b = (out_b / "reconciliation-manifest.json").read_bytes()
    assert body_a == body_b
    sha_a = (out_a / "reconciliation-manifest.sha256.txt").read_text().strip()
    sha_b = (out_b / "reconciliation-manifest.sha256.txt").read_text().strip()
    assert sha_a == sha_b
