"""Stage 6l source-of-truth checks across the landed repository slices.

These tests intentionally inspect the sibling repositories.  They are the gate that
prevents independently passing client and database suites from drifting at the wire
boundary.  A checkout without the siblings skips this integration-only module; the
canonical Stage 6l verification runs it with all four paths present.
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
CODE_ROOT = ROOT.parent
WEB = Path(os.environ.get("SPORELY_WEB_REPO", CODE_ROOT / "sporely-web-reference-stage6"))
LANDING = Path(os.environ.get("SPORELY_LANDING_REPO", CODE_ROOT / "sporely-landing-stage6j"))
ADMIN = Path(os.environ.get("SPORELY_ADMIN_REPO", CODE_ROOT / "sporely-admin-reference-stage6"))

EXPECTED_HEADS = {
    WEB: "7d9bc8fd8aa14274932b471ad41e23041f9b9ca4",
    LANDING: "d0860354e0af9bf7bccae13c85fc11c5a9d34ab4",
    ADMIN: "10f923e79433fd59f88fc4584c223f13454bf3ab",
}

PUBLIC_KEYS = {
    "curated_measurement_set_id", "bundle_revision", "status",
    "superseded_by_id", "published_at", "sporely_taxon_id",
    "canonical_scientific_name", "snapshot", "citation", "exports",
}
WITHDRAWN_KEYS = {
    "curated_measurement_set_id", "bundle_revision", "status",
    "withdrawn_at", "superseded_by_id",
}
SEARCH_PARAMETERS = (
    "p_sporely_taxon_id", "p_limit", "p_after_published_at", "p_after_id",
)
EXACT_PARAMETERS = ("p_curated_measurement_set_id", "p_bundle_revision")
SUBMIT_PARAMETERS = (
    "p_source_measurement_set_id", "p_expected_work_revision",
    "p_expected_treatment_revision", "p_expected_measurement_set_revision",
    "p_attestation_version", "p_rights_confirmed",
    "p_curation_consent_confirmed",
)


def _require_repositories() -> None:
    missing = [str(path) for path in EXPECTED_HEADS if not (path / ".git").exists()]
    if missing:
        if os.environ.get("SPORELY_STAGE6L_GATE") == "1":
            pytest.fail(f"Stage 6l sibling worktrees are unavailable: {', '.join(missing)}")
        pytest.skip(f"Stage 6l sibling worktrees are unavailable: {', '.join(missing)}")


def _git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repository, check=True, capture_output=True, text=True,
    ).stdout.strip()


def _tracked_tree_is_clean(repository: Path) -> bool:
    for args in (("diff", "--quiet"), ("diff", "--cached", "--quiet")):
        if subprocess.run(["git", *args], cwd=repository, check=False).returncode != 0:
            return False
    return True


def _method_rpc_keys(source: str, method_name: str) -> tuple[str, ...]:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name:
            for call in ast.walk(node):
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "_rpc" and len(call.args) >= 2
                        and isinstance(call.args[1], ast.Dict)):
                    return tuple(
                        key.value for key in call.args[1].keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    )
    raise AssertionError(f"RPC payload not found for {method_name}")


def _ts_array(source: str, name: str) -> set[str]:
    match = re.search(rf"const {name} = \[(.*?)\] as const", source, re.DOTALL)
    assert match, f"missing {name}"
    return set(re.findall(r"['\"]([^'\"]+)['\"]", match.group(1)))


def _sql_signature(source: str, function_name: str) -> tuple[tuple[str, str], ...]:
    match = re.search(
        rf"CREATE FUNCTION public\.{function_name}\s*\((.*?)\)\s*RETURNS",
        source, re.DOTALL,
    )
    assert match, f"missing SQL function {function_name}"
    return tuple(
        (name, " ".join(declaration.split()))
        for name, declaration in re.findall(
            r"^\s*(p_[a-z0-9_]+)\s+([^,\n]+)", match.group(1), re.MULTILINE,
        )
    )


def test_stage6l_uses_the_landed_stage6_heads() -> None:
    _require_repositories()
    for repository, revision in EXPECTED_HEADS.items():
        assert _git(repository, "rev-parse", "HEAD") == revision
        assert _tracked_tree_is_clean(repository), f"{repository.name} has tracked changes"


def test_public_rpc_names_parameters_limits_and_envelopes_match() -> None:
    _require_repositories()
    desktop = (ROOT / "utils/cloud_sync.py").read_text()
    desktop_model = (ROOT / "database/curated_reference_forks.py").read_text()
    migration = (WEB / "supabase/migrations/20260829220943_add_public_curated_reference_reads.sql").read_text()
    landing_api = (LANDING / "src/lib/publicApi.ts").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()

    assert _method_rpc_keys(desktop, "search_public_curated_reference_sets") == SEARCH_PARAMETERS
    assert _method_rpc_keys(desktop, "get_public_curated_reference_set") == EXACT_PARAMETERS
    assert _sql_signature(migration, "search_public_curated_reference_sets") == (
        ("p_sporely_taxon_id", "integer"),
        ("p_limit", "integer"),
        ("p_after_published_at", "timestamptz"),
        ("p_after_id", "uuid"),
    )
    assert _sql_signature(migration, "get_public_curated_reference_set") == (
        ("p_curated_measurement_set_id", "uuid"),
        ("p_bundle_revision", "integer DEFAULT NULL"),
    )
    for parameter in SEARCH_PARAMETERS + EXACT_PARAMETERS:
        assert re.search(rf"\b{parameter}\s*:", landing_api)

    desktop_keys = set(re.search(
        r"_FULL_KEYS = frozenset\(\{(.*?)\}\)", desktop_model, re.DOTALL,
    ).group(1).replace('"', '').replace("'", '').replace("\n", "").split(","))
    desktop_keys = {key.strip() for key in desktop_keys if key.strip()}
    assert desktop_keys == PUBLIC_KEYS
    assert _ts_array(landing_model, "FULL_KEYS") == PUBLIC_KEYS
    assert _ts_array(landing_model, "WITHDRAWN_KEYS") == WITHDRAWN_KEYS
    assert "p_limit IS NULL OR p_limit < 1 OR p_limit > 50" in migration
    assert "LIMIT p_limit" in migration
    assert "value.length > requestedLimit" in landing_model
    assert "len(response) > limit" in desktop_model

    agent_bound_sql = (WEB / (
        "supabase/migrations/20260830130000_bound_curated_citation_agents.sql"
    )).read_text()
    assert "len(value) > 100" in desktop_model
    assert "value.length > 100" in landing_model
    assert "jsonb_array_length(p_agents) > 100" in agent_bound_sql
    assert "jsonb_array_length(p_agents) <= 100" in agent_bound_sql


def test_exact_taxonomy_lifecycle_and_public_access_are_fail_closed() -> None:
    _require_repositories()
    migration = (WEB / "supabase/migrations/20260829220943_add_public_curated_reference_reads.sql").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()
    desktop_model = (ROOT / "database/curated_reference_forks.py").read_text()

    assert "publication_taxon.sporely_taxon_id = p_sporely_taxon_id" in migration
    assert "concept.rank = 'species'" in migration
    assert "measurement_set.catalogue_status = 'published'" in migration
    assert "v_set.catalogue_status = 'withdrawn'" in migration
    assert "item.sporelyTaxonId === requestedTaxonId" in landing_model
    assert "taxon_id != expected_taxon_id" in desktop_model
    assert "SECURITY DEFINER" in migration and "SET search_path = ''" in migration
    for role in ("anon", "authenticated", "service_role"):
        assert role in migration
    assert "REVOKE ALL ON FUNCTION public.search_public_curated_reference_sets" in migration
    assert "REVOKE ALL ON FUNCTION public.get_public_curated_reference_set" in migration


def test_submission_contract_matches_and_production_policy_stays_dormant() -> None:
    _require_repositories()
    desktop = (ROOT / "utils/cloud_sync.py").read_text()
    intake = (WEB / "supabase/migrations/20260829145939_add_reference_curation_intake.sql").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()

    assert _method_rpc_keys(desktop, "submit_private_reference_for_curation") == SUBMIT_PARAMETERS
    assert _sql_signature(intake, "submit_private_reference_for_curation") == (
        ("p_source_measurement_set_id", "uuid"),
        ("p_expected_work_revision", "integer"),
        ("p_expected_treatment_revision", "integer"),
        ("p_expected_measurement_set_revision", "integer"),
        ("p_attestation_version", "text"),
        ("p_rights_confirmed", "boolean"),
        ("p_curation_consent_confirmed", "boolean"),
    )
    assert "NOT v_policy.submissions_enabled" in intake
    assert "policy_not_configured" in intake
    assert "configuredCuratedReferencePageSize(): number | null" in landing_model
    assert "return null" in landing_model


def test_no_unapproved_references_route_was_activated() -> None:
    _require_repositories()
    app = (LANDING / "src/App.tsx").read_text()
    assert not re.search(r'<Route\s+path=["\']/references(?:/|["\'])', app)


def test_admin_edge_contract_is_bounded_and_keeps_service_authority() -> None:
    _require_repositories()
    admin_api = (ADMIN / "src/referenceCurationApi.js").read_text()
    admin_models = (ADMIN / "src/referenceCurationModels.js").read_text()
    edge_actions = (WEB / "supabase/functions/reference-curation/actions.ts").read_text()
    edge_reads = (WEB / "supabase/functions/reference-curation/reads.ts").read_text()

    admin_actions = set(re.findall(
        r"^\s*[a-zA-Z]+:\s*'([^']+)'", re.search(
            r"REFERENCE_CURATION_ACTIONS = Object\.freeze\(\{(.*?)\}\)",
            admin_api, re.DOTALL,
        ).group(1), re.MULTILINE,
    ))
    edge_mutations = set(re.findall(
        r"'([^']+)'", re.search(
            r"const MUTATION_ACTIONS = new Set\(\[(.*?)\]\)", edge_actions, re.DOTALL,
        ).group(1),
    ))
    edge_lifecycle = set(re.findall(
        r"'([^']+)'", re.search(
            r"const LIFECYCLE_ACTIONS = new Set\(\[(.*?)\]\)", edge_actions, re.DOTALL,
        ).group(1),
    ))
    # Stage 6e intentionally left acceptance at the already-tested Edge/SQL
    # boundary; every browser-exposed mutation must still be understood there.
    assert admin_actions == (edge_mutations - {"accept_to_draft"}) | edge_lifecycle
    assert "headers: { Authorization: `Bearer ${token}` }" in admin_api
    assert "This module never accepts or constructs service credentials" in admin_api
    assert "value.role === null" in admin_models
    assert "raw.items.length > 50" in admin_models
    assert "encodedSize(data) > MAX_RESPONSE_BYTES" in edge_reads
    assert "p_actor_user_id: context.actorUserId" in edge_actions
    assert "p_actor_session_id: context.actorSessionId" in edge_actions
