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

REQUIRED_ANCESTORS = {
    WEB: "7d9bc8fd8aa14274932b471ad41e23041f9b9ca4",
    LANDING: "d0860354e0af9bf7bccae13c85fc11c5a9d34ab4",
}

PUBLIC_KEYS = {
    "contribution_id", "revision", "status", "shared_at", "sporely_taxon_id",
    "canonical_scientific_name", "contributor", "snapshot", "citation", "exports",
}
WITHDRAWN_KEYS = {
    "contribution_id", "revision", "status", "withdrawn_at",
}
SEARCH_PARAMETERS = (
    "p_sporely_taxon_id", "p_limit", "p_after_shared_at", "p_after_id",
)
EXACT_PARAMETERS = ("p_contribution_id", "p_revision")
SUBMIT_PARAMETERS = (
    "p_source_measurement_set_id", "p_sporely_taxon_id",
    "p_expected_work_revision", "p_expected_treatment_revision",
    "p_expected_measurement_set_revision",
)


def _require_repositories() -> None:
    missing = [str(path) for path in REQUIRED_ANCESTORS if not (path / ".git").exists()]
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


def test_model_simplification_builds_on_the_landed_stage6_slices() -> None:
    _require_repositories()
    for repository, revision in REQUIRED_ANCESTORS.items():
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", revision, "HEAD"],
            cwd=repository, check=True,
        )


def test_public_rpc_names_parameters_limits_and_envelopes_match() -> None:
    _require_repositories()
    desktop = (ROOT / "utils/cloud_sync.py").read_text()
    desktop_model = (ROOT / "database/curated_reference_forks.py").read_text()
    migration = (WEB / "supabase/migrations/20260830183210_add_shared_reference_contributions.sql").read_text()
    landing_api = (LANDING / "src/lib/publicApi.ts").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()

    assert _method_rpc_keys(desktop, "search_public_reference_contributions") == SEARCH_PARAMETERS
    assert _method_rpc_keys(desktop, "get_public_reference_contribution") == EXACT_PARAMETERS
    assert _sql_signature(migration, "search_public_reference_contributions") == (
        ("p_sporely_taxon_id", "integer"),
        ("p_limit", "integer"),
        ("p_after_shared_at", "timestamptz"),
        ("p_after_id", "uuid"),
    )
    assert _sql_signature(migration, "get_public_reference_contribution") == (
        ("p_contribution_id", "uuid"),
        ("p_revision", "integer DEFAULT NULL"),
    )
    for parameter in SEARCH_PARAMETERS + EXACT_PARAMETERS:
        assert re.search(rf"\b{parameter}\s*:", landing_api)

    desktop_keys = set(re.search(
        r"_SHARED_KEYS = frozenset\(\{(.*?)\}\)", desktop_model, re.DOTALL,
    ).group(1).replace('"', '').replace("'", '').replace("\n", "").split(","))
    desktop_keys = {key.strip() for key in desktop_keys if key.strip()}
    assert desktop_keys == PUBLIC_KEYS
    assert _ts_array(landing_model, "SHARED_KEYS") == PUBLIC_KEYS
    assert _ts_array(landing_model, "SHARED_WITHDRAWN_KEYS") == WITHDRAWN_KEYS
    assert "p_limit IS NULL OR p_limit < 1 OR p_limit > 50" in migration
    assert "LIMIT p_limit" in migration
    assert "value.length > requestedLimit" in landing_model
    assert "len(response) > limit" in desktop_model

    agent_bound_sql = (WEB / "supabase/migrations/20260830130000_bound_curated_citation_agents.sql").read_text()
    assert "len(value) > 100" in desktop_model
    assert "value.length > 100" in landing_model
    assert "jsonb_array_length(p_agents) > 100" in agent_bound_sql
    assert "jsonb_array_length(p_agents) <= 100" in agent_bound_sql


def test_exact_taxonomy_lifecycle_and_public_access_are_fail_closed() -> None:
    _require_repositories()
    migration = (WEB / "supabase/migrations/20260830183210_add_shared_reference_contributions.sql").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()
    desktop_model = (ROOT / "database/curated_reference_forks.py").read_text()

    assert "c.sporely_taxon_id = p_sporely_taxon_id" in migration
    assert "c.rank = 'species'" in migration
    assert "c.status = 'shared'" in migration
    assert "v_contribution.status = 'withdrawn'" in migration
    assert "item.sporelyTaxonId === requestedTaxonId" in landing_model
    assert "taxon_id != expected_taxon_id" in desktop_model
    assert "SECURITY DEFINER" in migration and "SET search_path = ''" in migration
    for role in ("anon", "authenticated", "service_role"):
        assert role in migration
    assert "REVOKE ALL ON FUNCTION public.search_public_reference_contributions" in migration
    assert "REVOKE ALL ON FUNCTION public.get_public_reference_contribution" in migration


def test_submission_contract_matches_and_production_policy_stays_dormant() -> None:
    _require_repositories()
    desktop = (ROOT / "utils/cloud_sync.py").read_text()
    sharing = (WEB / "supabase/migrations/20260830183210_add_shared_reference_contributions.sql").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()

    assert _method_rpc_keys(desktop, "share_reference_contribution") == SUBMIT_PARAMETERS
    assert _sql_signature(sharing, "share_reference_contribution") == (
        ("p_source_measurement_set_id", "uuid"),
        ("p_sporely_taxon_id", "integer"),
        ("p_expected_work_revision", "integer"),
        ("p_expected_treatment_revision", "integer"),
        ("p_expected_measurement_set_revision", "integer"),
    )
    assert "exact_taxon_use_required" in sharing
    assert "attestation" not in sharing.lower()
    assert "reference_reviewer" not in sharing
    assert "reference_publisher" not in sharing
    assert "configuredCuratedReferencePageSize(): number | null" in landing_model
    assert "return null" in landing_model


def test_no_unapproved_references_route_was_activated() -> None:
    _require_repositories()
    app = (LANDING / "src/App.tsx").read_text()
    assert not re.search(r'<Route\s+path=["\']/references(?:/|["\'])', app)


def test_shared_contribution_public_contract_is_attributed_and_not_curated() -> None:
    _require_repositories()
    sharing = (WEB / "supabase/migrations/20260830183210_add_shared_reference_contributions.sql").read_text()
    landing_api = (LANDING / "src/lib/publicApi.ts").read_text()
    landing_model = (LANDING / "src/lib/publicCuratedReferences.ts").read_text()
    assert "search_public_reference_contributions" in landing_api
    assert "get_public_reference_contribution" in landing_api
    assert "'contributor'" in landing_model
    assert "shared_reference_contributions_owner_source_taxon_key" in sharing
    assert "WHERE owner_id IS NOT NULL AND source_measurement_set_id IS NOT NULL" in sharing
    assert "doi" not in re.search(
        r"CREATE TABLE private\.shared_reference_contributions \((.*?)\);",
        sharing, re.DOTALL,
    ).group(1).lower()
    assert "observation_reference_use_shared_contribution_trg" in sharing
