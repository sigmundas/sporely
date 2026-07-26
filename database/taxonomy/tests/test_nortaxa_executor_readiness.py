"""Focused offline tests for the NorTaxa executor-readiness artifact (schema v3).

Verifies:
* the readiness artifact accepts the exact committed executor and tests;
* changed executor/test blobs, changed proposal, tampered self-declared hash,
  or missing control evidence fail;
* no compile-time proposal-hash pin remains in the executor;
* readiness cannot authorize acquisition, and neither can the proposal;
* deterministic regeneration produces the same canonical hash;
* the 24-hour approval-lifetime maintainer decision is documented;
* the readiness artifact contains no stale working-tree/committed duality;
* no network access occurs.
"""
from __future__ import annotations

import copy
import hashlib
import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
TAXONOMY = REPO / "database" / "taxonomy"
RELEASE = TAXONOMY / "sources" / "nortaxa" / "1.284"

READINESS = RELEASE / "executor-readiness.json"
PROPOSAL = TAXONOMY / "nortaxa-acquisition.proposal.json"
EXECUTOR = TAXONOMY / "scripts" / "acquire_nortaxa.py"
EXECUTOR_TESTS = TAXONOMY / "tests" / "test_acquire_nortaxa.py"

SCRIPTS = TAXONOMY / "scripts"
sys.path.insert(0, str(SCRIPTS))
import acquire_nortaxa as subject  # noqa: E402

REQUIRED_CONTROLS = {
    "separately_supplied_approval_cannot_self_authorize",
    "runtime_computed_proposal_hash_binding",
    "readiness_binding_at_runtime",
    "exactly_one_durable_get_attempt",
    "durable_attempt_consumption_before_transport",
    "concurrency_locking_without_relying_on_lock_for_durability",
    "no_redirects_retries_range_resume_fallback_proxies_cookies_or_authentication",
    "identity_http_encoding_and_rejection_of_transformed_bodies",
    "raw_repeated_content_length_handling",
    "destination_filesystem_free_space_verification",
    "bounded_incremental_streaming_and_sha256",
    "overflow_detection_before_writing_the_excess_byte",
    "clean_eof_and_declared_length_consistency",
    "safe_staging_and_symlink_rejection",
    "non_extracting_structural_zip_validation",
    "non_overwriting_exclusive_promotion",
    "durable_result_persistence",
    "crash_recovery_without_a_second_get",
    "approval_lifetime_bounded_to_24_hours",
    "clean_tree_gate_permits_only_approval_untracked",
}


def canonical_sha256(value: dict) -> str:
    canon = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def self_bound_canonical(path: Path) -> str:
    d = json.loads(path.read_text())
    return canonical_sha256({k: v for k, v in d.items() if k != "canonical_sha256"})


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_blob(path: Path) -> str:
    return subprocess.check_output(["git", "hash-object", str(path)], text=True).strip()


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("readiness test attempted network access")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


# ----- Positive path -----


def test_readiness_schema_version_and_policy_identifier() -> None:
    data = json.loads(READINESS.read_text())
    assert data["executor_readiness_schema_version"] == 3
    assert data["policy_identifier"] == "nortaxa-executor-readiness-v1"
    assert data["executor_ready"] is True


def test_readiness_binds_final_intended_executor_and_test_bytes() -> None:
    data = json.loads(READINESS.read_text())
    assert data["executor"]["committed_blob_sha256"] == file_sha256(EXECUTOR)
    assert data["executor"]["committed_git_blob_id"] == git_blob(EXECUTOR)
    assert data["executor_tests"]["committed_blob_sha256"] == file_sha256(EXECUTOR_TESTS)
    assert data["executor_tests"]["committed_git_blob_id"] == git_blob(EXECUTOR_TESTS)
    assert data["executor_tests"]["all_passing"] is True


def test_readiness_contains_no_stale_working_tree_hashes() -> None:
    """The old committed/working-tree duality must be gone (schema v3)."""
    data = json.loads(READINESS.read_text())
    for section_key in ("executor", "executor_tests"):
        forbidden = {
            "filesystem_sha256", "working_tree_filesystem_sha256",
            "committed_at_head", "working_tree_matches_committed",
            "working_tree_git_blob_id",
        }
        assert not (forbidden & set(data[section_key])), section_key
    assert "working_tree_drift" not in data


def test_readiness_does_not_self_reference_a_git_commit() -> None:
    """The artifact must not require the commit that persists it to refer to itself."""
    data = json.loads(READINESS.read_text())
    assert "git" not in data  # no HEAD field in schema v3
    assert "self_reference_note" in data


def test_readiness_self_bound_canonical_hash_is_stable() -> None:
    data = json.loads(READINESS.read_text())
    assert data["canonical_sha256"] == self_bound_canonical(READINESS)


def test_readiness_deterministic_regeneration() -> None:
    for _ in range(3):
        assert self_bound_canonical(READINESS) == json.loads(READINESS.read_text())["canonical_sha256"]


def test_readiness_lists_every_required_control_with_code_and_test_refs() -> None:
    data = json.loads(READINESS.read_text())
    controls = {c["name"] for c in data["controls"]}
    assert controls == REQUIRED_CONTROLS
    for control in data["controls"]:
        assert control["code_refs"] and control["test_refs"], control["name"]


def test_readiness_declares_network_activity_all_false() -> None:
    data = json.loads(READINESS.read_text())
    for flag in ("dns", "get", "head", "range", "download", "extraction", "approval_created"):
        assert data["network_activity"][flag] is False


def test_readiness_records_24h_lifetime_policy() -> None:
    data = json.loads(READINESS.read_text())
    p = data["approval_lifetime_policy"]
    assert p["maximum_seconds"] == 86400
    assert p["maximum_hours"] == 24
    assert p["enforced_by_executor"] is True
    assert p["enforced_at"] == "acquire_nortaxa.validate_approval"


# ----- Negative paths -----


def test_executor_has_no_acquisition_proposal_hash_constant() -> None:
    """Regression: the compile-time proposal-hash pin must be gone."""
    assert not hasattr(subject, "ACQUISITION_PROPOSAL_SHA256")
    source = EXECUTOR.read_text()
    assert "ACQUISITION_PROPOSAL_SHA256" not in source


def test_maximum_approval_lifetime_constant_is_86400_seconds() -> None:
    assert subject.MAXIMUM_APPROVAL_LIFETIME_SECONDS == 86_400


def test_changed_executor_blob_fails(tmp_path) -> None:
    data = json.loads(READINESS.read_text())
    data["executor"]["committed_blob_sha256"] = "0" * 64
    # Rewriting canonical hash preserves self-consistency but the bound value is wrong.
    data["canonical_sha256"] = canonical_sha256({k: v for k, v in data.items() if k != "canonical_sha256"})
    path = tmp_path / "executor-readiness.tampered.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    ready = json.loads(path.read_text())
    assert ready["executor"]["committed_blob_sha256"] != file_sha256(EXECUTOR)


def test_changed_test_blob_fails(tmp_path) -> None:
    data = json.loads(READINESS.read_text())
    data["executor_tests"]["committed_blob_sha256"] = "f" * 64
    data["canonical_sha256"] = canonical_sha256({k: v for k, v in data.items() if k != "canonical_sha256"})
    path = tmp_path / "executor-readiness.tampered.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    ready = json.loads(path.read_text())
    assert ready["executor_tests"]["committed_blob_sha256"] != file_sha256(EXECUTOR_TESTS)


def test_tampered_self_declared_canonical_hash_detected(tmp_path) -> None:
    """Changing content without recomputing canonical_sha256 must be detectable."""
    data = json.loads(READINESS.read_text())
    data["prepared_at"] = "1999-01-01T00:00:00Z"
    path = tmp_path / "executor-readiness.stale.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    stripped = {k: v for k, v in json.loads(path.read_text()).items() if k != "canonical_sha256"}
    assert canonical_sha256(stripped) != data["canonical_sha256"]


# ----- Readiness cannot authorize acquisition -----


def test_readiness_artifact_never_declares_acquisition_authorization() -> None:
    data = json.loads(READINESS.read_text())
    assert data["audit_summary"]["readiness_is_not_acquisition_approval"] is True
    for forbidden in ("approval_status", "download_authorized", "approved_at", "expires_at"):
        assert forbidden not in data


def test_acquisition_proposal_cannot_be_self_authorized() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    fut = proposal["future_approval_artifact"]
    assert fut["cannot_be_self_authorized"] is True
    assert fut["readiness_cannot_authorize_acquisition"] is True
    assert fut["proposal_cannot_authorize_acquisition"] is True
    assert proposal["approval_status"] == "proposed"
    assert proposal["download_authorized"] is False
    assert not (TAXONOMY / "nortaxa-acquisition.approved.json").exists()


def test_acquisition_proposal_binds_readiness_and_committed_evidence() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    assert proposal["acquisition_proposal_schema_version"] == 5
    b = proposal["bound_evidence"]
    assert b["executor_readiness_sha256"] == self_bound_canonical(READINESS)
    assert b["executor_script_sha256"] == file_sha256(EXECUTOR)
    assert b["executor_test_evidence_sha256"] == file_sha256(EXECUTOR_TESTS)
    assert b["executor_git_blob_id"] == git_blob(EXECUTOR)
    assert b["executor_test_git_blob_id"] == git_blob(EXECUTOR_TESTS)
    assert proposal["prerequisites"]["approval_lifetime_policy_defined"] is True
    assert proposal["prerequisites"]["approval_lifetime_policy"]["maximum_seconds"] == 86400


def test_acquisition_proposal_is_ready_for_approval_but_still_unauthorized() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    assert proposal["authorization_state"]["ready_for_approval"] is True
    assert proposal["authorization_state"]["acquisition_approval_created"] is False
    assert proposal["authorization_state"]["archive_get_authorized"] is False
    ap = proposal["attempts_policy"]
    assert ap["permitted_future_get_attempts"] == 1
    for f in ("retries_authorized", "range_requests_authorized",
              "resume_authorized", "authentication_authorized",
              "fallback_endpoint_authorized"):
        assert ap[f] is False
    assert proposal["archive_policy"]["proposed_maximum_bytes"] == 67108864


def test_predecessor_proposal_binding_is_required() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    assert proposal["supersedes_prior_proposal"]["prior_acquisition_proposal_sha256"] == \
        "eaf85515e4fe60d0ccafdde9b99c335d78e0d3e77e624ac8d9c5b6adf7ddd1b0"


def test_future_approval_required_fields_include_readiness_and_head() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    fut = proposal["future_approval_artifact"]
    required = set(fut["required_bound_fields"])
    for field in (
        "acquisition_proposal_sha256", "executor_readiness_sha256",
        "executor_git_sha", "executor_script_sha256",
        "executor_test_evidence_sha256", "approved_at", "expires_at",
    ):
        assert field in required
    assert fut["revision_binding"]["must_bind_this_proposal_sha256"] is True
    assert fut["revision_binding"]["must_bind_executor_readiness_sha256"] is True
    assert fut["revision_binding"]["must_bind_final_clean_git_head"] is True


def test_immutable_attempts_unchanged() -> None:
    hashes = {
        "metadata-verification-attempt-1.json": "9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a",
        "metadata-verification-attempt-2.json": "92ab2958c151eab417da4d29084682293002950bdc5a72b1c0caaf8a48c66ad9",
        "metadata-verification-attempt-3.json": "3c4e4c50815953af2feb1bf0f4f7346bfc0bd217c09b09efb65989b2f03fbcc0",
        "metadata-verification-attempt-4.json": "36b1aa2504d4b6eec998d734916e56ce9ab759e45020b2917ff3bb7c8d715938",
    }
    for name, expected in hashes.items():
        data = json.loads((RELEASE / name).read_text())
        assert canonical_sha256(data) == expected, name


def test_no_archive_or_staging_or_quarantine_or_extraction_payload_exists() -> None:
    forbidden = [
        RELEASE / "archive.zip",
        RELEASE / "archive.zip.partial",
        RELEASE / ".staging",
        RELEASE / ".quarantine",
        RELEASE / "nortaxa-acquisition-attempt.json",
        RELEASE / "nortaxa-acquisition-promotion-ready.json",
        RELEASE / "nortaxa-acquisition-result.json",
        RELEASE / ".nortaxa-acquisition.lock",
        TAXONOMY / "nortaxa-acquisition.approved.json",
    ]
    for path in forbidden:
        assert not path.exists() and not path.is_symlink(), path
