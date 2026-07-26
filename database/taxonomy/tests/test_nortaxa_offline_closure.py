"""Offline closure tests for NorTaxa Stage 2B metadata verification.

Covers the formalized policy-resolution contract and the unauthorized
acquisition proposal, and proves that ``verify_closure_conditions`` refuses to
close a `parse_failed` attempt for anything other than an explicitly superseded
policy decision with deterministic re-evaluation and sufficient preserved
evidence.

Because attempt 3 did not preserve the raw HEAD headers dict, closure is
NOT emitted: `metadata-verification.json` deliberately does not exist. Attempts
1-3, the proposal, request, and manifest are byte-identical.
"""
from __future__ import annotations

import copy
import hashlib
import json
import socket
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from nortaxa_metadata import (  # noqa: E402
    AcquisitionError,
    OPERATION_NAMES,
    RESOLVABLE_PARSE_FAILURES,
    SUPPLEMENTAL_ATTEMPT_AUTHORIZED_OPERATIONS,
    compose_supplemental_metadata_verification,
    evaluate_archive_get,
    verify_closure_conditions,
)
from refresh_nortaxa import load_proposal, load_request  # noqa: E402

RELEASE = Path(__file__).resolve().parents[1] / "sources" / "nortaxa" / "1.284"
TAXONOMY = Path(__file__).resolve().parents[1]

ATTEMPT_1_SHA = "9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a"
ATTEMPT_2_SHA = "92ab2958c151eab417da4d29084682293002950bdc5a72b1c0caaf8a48c66ad9"
ATTEMPT_3_SHA = "3c4e4c50815953af2feb1bf0f4f7346bfc0bd217c09b09efb65989b2f03fbcc0"
PROPOSAL_SHA = "e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3"
REQUEST_SHA = "38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392"


def canonical_sha256(value: dict) -> str:
    canon = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def file_canonical_sha256(path: Path) -> str:
    return canonical_sha256(json.loads(path.read_text()))


def self_bound_canonical_sha256(path: Path) -> str:
    data = json.loads(path.read_text())
    return canonical_sha256({k: v for k, v in data.items() if k != "canonical_sha256"})


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("offline closure test attempted network access")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


# ----- Immutable evidence bindings -----


def test_attempts_1_2_3_remain_byte_identical() -> None:
    assert file_canonical_sha256(RELEASE / "metadata-verification-attempt-1.json") == ATTEMPT_1_SHA
    assert file_canonical_sha256(RELEASE / "metadata-verification-attempt-2.json") == ATTEMPT_2_SHA
    assert file_canonical_sha256(RELEASE / "metadata-verification-attempt-3.json") == ATTEMPT_3_SHA


def test_proposal_and_request_hashes_unchanged() -> None:
    assert file_canonical_sha256(TAXONOMY / "nortaxa-source-selection.proposal.json") == PROPOSAL_SHA
    request = json.loads((RELEASE / "request.json").read_text())
    assert request["canonical_request_sha256"] == REQUEST_SHA


# ----- Policy-resolution artifact -----


def test_policy_resolution_artifact_exists_and_declares_conditions() -> None:
    path = RELEASE / "policy-resolution.json"
    data = json.loads(path.read_text())
    assert data["policy_resolution_schema_version"] == 2
    assert data["policy_identifier"] == "nortaxa-archive-head-content-length-absent-v1"
    assert data["resolved_offline"] is True
    assert data["acquisition_authorization_status"] == "not_authorized"
    bindings = data["bound_evidence"]
    assert bindings["source_selection_proposal_sha256"] == PROPOSAL_SHA
    assert bindings["request_sha256"] == REQUEST_SHA
    assert bindings["attempt_1_sha256"] == ATTEMPT_1_SHA
    assert bindings["attempt_2_sha256"] == ATTEMPT_2_SHA
    assert bindings["attempt_3_sha256"] == ATTEMPT_3_SHA
    conditions = data["closure_contract"]["conditions"]
    assert conditions == [
        "failure_is_exclusively_a_superseded_policy_decision",
        "resolution_binds_attempt_hash_and_exact_transport_evidence",
        "deterministic_reevaluation_under_replacement_policy_succeeds",
        "all_other_operations_succeeded",
        "new_policy_requires_no_evidence_the_attempt_failed_to_preserve",
        "final_artifact_references_the_resolution",
    ]
    # Attempt-3 transport observation is copied verbatim from the record.
    obs = data["archive_head_observation"]
    assert obs["status_code"] == 200
    assert obs["content_type"] == "application/zip"
    assert obs["redirect_chain"] == []
    assert obs["body_bytes"] == 0 and obs["body_sha256"] is None
    assert obs["raw_headers_recorded"] is False
    assert obs["content_length_presence_recorded"] is False


def test_policy_resolution_self_declared_sha_matches_canonical() -> None:
    path = RELEASE / "policy-resolution.json"
    data = json.loads(path.read_text())
    assert data["canonical_sha256"] == self_bound_canonical_sha256(path)


def test_policy_resolution_reports_insufficient_evidence_for_attempt_3() -> None:
    data = json.loads((RELEASE / "policy-resolution.json").read_text())
    result = data["attempt_3_closure_result"]
    assert result["status"] == "insufficient_transport_evidence"
    assert result["final_metadata_verification_emitted"] is False
    for header in ("content_length", "etag", "last_modified", "accept_ranges", "content_disposition"):
        assert result["unresolved_header_evidence"][header]["status"] == "unavailable_due_to_parse_failure"
    assert data["attempt_3_reclassification_interpretation_only"]["attempt_3_record_modified"] is False


def test_final_metadata_verification_artifact_now_exists_via_composition() -> None:
    """Attempt 3 alone was insufficient; attempt 4 supplied the HEAD evidence."""
    assert (RELEASE / "metadata-verification.json").exists()


# ----- Formalized closure-conditions helper (verify_closure_conditions) -----


def load_policy_and_attempt3():
    pol = json.loads((RELEASE / "policy-resolution.json").read_text())
    att = json.loads((RELEASE / "metadata-verification-attempt-3.json").read_text())
    return pol, att


def always_parse_succeeded(_op):
    return "parse_succeeded"


def test_closure_fails_for_attempt_3_because_evidence_is_not_preserved() -> None:
    pol, att = load_policy_and_attempt3()
    with pytest.raises(AcquisitionError, match="did not preserve required evidence"):
        verify_closure_conditions(
            policy_resolution=pol, attempt_record=att,
            replacement_evaluator=always_parse_succeeded,
        )


def _attempt_with_head_headers_recorded():
    """A hypothetical attempt whose archive_head journal DID preserve raw headers.

    Deliberately synthetic: attempt-3 on disk cannot be modified. We use this
    only to exercise the positive branch of verify_closure_conditions and to
    prove closure is refused whenever any single condition fails.
    """
    att = json.loads((RELEASE / "metadata-verification-attempt-3.json").read_text())
    head = next(op for op in att["operations"] if op["operation"] == "archive_head")
    head["raw_headers"] = {"Content-Type": "application/zip"}
    # Attempt hash changes because the record changes; leave the binding for the
    # caller to update explicitly per test.
    return att, head


def test_closure_succeeds_when_all_six_conditions_hold() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    # Rebind: replace attempt_3 hash in a copy of the policy so we can prove the
    # positive branch without mutating the on-disk policy or attempt 3.
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    out = verify_closure_conditions(
        policy_resolution=pol, attempt_record=att,
        replacement_evaluator=always_parse_succeeded,
    )
    assert out["resolved_status"] == "parse_succeeded"
    assert out["policy_identifier"] == pol["policy_identifier"]


# --- Negative tests: policy resolution must refuse to close disallowed cases ---


def test_closure_refuses_unknown_policy_identifier() -> None:
    pol, att = load_policy_and_attempt3()
    pol = copy.deepcopy(pol)
    pol["policy_identifier"] = "no-such-policy"
    with pytest.raises(AcquisitionError, match="unknown or unregistered policy identifier"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_when_attempt_hash_does_not_match_binding() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    with pytest.raises(AcquisitionError, match="attempt hash does not match"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_transport_failure_because_it_is_not_a_parse_failure() -> None:
    pol, _ = load_policy_and_attempt3()
    att, head = _attempt_with_head_headers_recorded()
    head["status"] = "transport_failed"
    head["error"] = {"type": "AcquisitionError", "phase": "transport",
                     "message": "metadata endpoint returned HTTP 500"}
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not parse_failed"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_unrelated_parser_failure() -> None:
    pol, _ = load_policy_and_attempt3()
    att, head = _attempt_with_head_headers_recorded()
    head["error"] = {"type": "AcquisitionError", "phase": "parse",
                     "message": "malformed EML XML: not well-formed"}
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not among the explicitly superseded set"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_unsafe_content_type() -> None:
    """Unsafe content type would be caught by validate_response BEFORE the parse phase,
    surfacing as a transport-phase failure. `verify_closure_conditions` must not attempt
    to close such a case, even if a policy-writer relabels the phase.
    """
    pol, _ = load_policy_and_attempt3()
    att, head = _attempt_with_head_headers_recorded()
    head["content_type"] = "text/html"
    head["error"] = {"type": "AcquisitionError", "phase": "transport",
                     "message": "unexpected metadata Content-Type: 'text/html'"}
    head["status"] = "transport_failed"
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not parse_failed"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_malformed_response() -> None:
    pol, _ = load_policy_and_attempt3()
    att, head = _attempt_with_head_headers_recorded()
    head["error"] = {"type": "AcquisitionError", "phase": "parse",
                     "message": "archive HEAD is inconsistent with ZIP delivery"}
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not among the explicitly superseded set"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_arbitrary_error_types() -> None:
    pol, _ = load_policy_and_attempt3()
    att, head = _attempt_with_head_headers_recorded()
    head["error"] = {"type": "ValueError", "phase": "parse",
                     "message": "archive HEAD lacks a positive Content-Length"}
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not superseded"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_when_sibling_operation_did_not_reach_parse_succeeded() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    for op in att["operations"]:
        if op["operation"] == "eml":
            op["status"] = "parse_failed"
            op["error"] = {"type": "AcquisitionError", "phase": "parse", "message": "unrelated"}
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="did not reach parse_succeeded"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_when_missing_required_transport_evidence() -> None:
    pol, att = load_policy_and_attempt3()  # attempt-3 as-is: no raw_headers recorded
    with pytest.raises(AcquisitionError, match="did not preserve required evidence"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_when_replacement_evaluator_missing() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="deterministic replacement evaluator is required"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=None)


def test_closure_refuses_when_replacement_evaluator_returns_not_parse_succeeded() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    with pytest.raises(AcquisitionError, match="not 'parse_succeeded'"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=lambda op: "still_ambiguous")


def test_closure_refuses_when_transport_observation_disagrees_with_attempt() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    pol["archive_head_observation"]["status_code"] = 302  # tampered
    with pytest.raises(AcquisitionError, match="does not match the attempt record"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


def test_closure_refuses_when_policy_resolution_omits_transport_field() -> None:
    pol, _ = load_policy_and_attempt3()
    att, _ = _attempt_with_head_headers_recorded()
    pol = copy.deepcopy(pol)
    pol["bound_evidence"]["attempt_3_sha256"] = canonical_sha256(att)
    pol["archive_head_observation"].pop("content_type")
    with pytest.raises(AcquisitionError, match="omits transport field 'content_type'"):
        verify_closure_conditions(policy_resolution=pol, attempt_record=att,
                                  replacement_evaluator=always_parse_succeeded)


# ----- Unauthorized acquisition proposal -----


def test_acquisition_proposal_bound_after_attempt_4_but_blocked_by_executor() -> None:
    path = TAXONOMY / "nortaxa-acquisition.proposal.json"
    data = json.loads(path.read_text())
    assert data["acquisition_proposal_schema_version"] == 3
    assert data["approval_status"] == "proposed"
    assert data["download_authorized"] is False
    prereq = data["prerequisites"]
    assert prereq["metadata_verification_final_artifact_exists"] is True
    assert prereq["executor_ready"] is False
    assert "acquire_nortaxa.py" in prereq["executor_readiness_audit"]["next_prerequisite"]
    b = data["bound_evidence"]
    assert b["source_selection_proposal_sha256"] == PROPOSAL_SHA
    assert b["request_sha256"] == REQUEST_SHA
    assert b["attempt_3_sha256"] == ATTEMPT_3_SHA
    assert b["attempt_4_sha256"] == ATTEMPT_4_SHA
    assert b["policy_resolution_sha256"] == self_bound_canonical_sha256(RELEASE / "policy-resolution.json")
    assert b["metadata_verification_sha256"] == self_bound_canonical_sha256(RELEASE / "metadata-verification.json")


def test_acquisition_proposal_still_cannot_be_created_before_executor_ready() -> None:
    data = json.loads((TAXONOMY / "nortaxa-acquisition.proposal.json").read_text())
    fut = data["future_approval_artifact"]
    assert fut["cannot_be_self_authorized"] is True
    assert any("acquire_nortaxa.py" in item for item in fut["cannot_be_created_before"])
    required = set(fut["required_bound_fields"])
    for field in (
        "acquisition_proposal_sha256", "metadata_verification_sha256",
        "policy_resolution_sha256", "approved_at",
        "executor_script_sha256", "executor_test_evidence_sha256",
    ):
        assert field in required


def test_acquisition_proposal_does_not_change_proposal_ceiling() -> None:
    proposal = json.loads((TAXONOMY / "nortaxa-source-selection.proposal.json").read_text())
    acquisition = json.loads((TAXONOMY / "nortaxa-acquisition.proposal.json").read_text())
    assert (
        acquisition["archive_policy"]["proposed_maximum_bytes"]
        == proposal["archive_policy"]["proposed_maximum_bytes"]
    )
    # And no `nortaxa-acquisition.approved.json` exists on disk.
    assert not (TAXONOMY / "nortaxa-acquisition.approved.json").exists()


# ----- Streaming-overflow enforcement (belt-and-braces on evaluate_archive_get) -----


CEILING = 1_000


def test_streaming_overflow_fails_before_promotion_declared() -> None:
    with pytest.raises(AcquisitionError, match="exceeded the ceiling"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=str(CEILING),
            completed_bytes=CEILING + 1, reached_eof=True,
        )


def test_streaming_overflow_fails_before_promotion_undeclared() -> None:
    with pytest.raises(AcquisitionError, match="exceeded the ceiling"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=None,
            completed_bytes=CEILING + 1, reached_eof=True,
        )


def test_partial_cleanup_is_required_by_acquisition_proposal() -> None:
    proposal = json.loads((TAXONOMY / "nortaxa-acquisition.proposal.json").read_text())
    controls = proposal["archive_policy"]
    assert controls["quarantine_or_delete_interrupted_or_oversized_partial_bytes"] is True
    assert controls["never_promote_partial_file_as_final_archive"] is True
    assert controls["require_non_empty_bytes_before_promotion"] is True
    assert controls["require_complete_structural_validation_before_promotion"] is True


def test_manifest_state_is_still_planned_and_download_unauthorized() -> None:
    manifest = json.loads((RELEASE / "manifest.json").read_text())
    assert manifest["state"] == "planned"
    assert manifest["approval_status"] == "proposed"
    assert manifest["download_authorized"] is False
    assert manifest["execution_attempts"] == []
    assert manifest["download"] is None
    assert manifest["validation"] is None


# ----- Cross-attempt composition (compose_supplemental_metadata_verification) -----


ATTEMPT_4_SHA = "36b1aa2504d4b6eec998d734916e56ce9ab759e45020b2917ff3bb7c8d715938"


def _load_composition_inputs():
    proposal = load_proposal()
    request  = load_request(RELEASE / "request.json")
    attempts = {n: json.loads((RELEASE / f"metadata-verification-attempt-{n}.json").read_text())
                for n in (1, 2, 3, 4)}
    attempt_hashes = {n: canonical_sha256(a) for n, a in attempts.items()}
    provenance = {
        "resource_page": {"attempt_number": 3, "attempt_record_sha256": attempt_hashes[3]},
        "eml":           {"attempt_number": 3, "attempt_record_sha256": attempt_hashes[3]},
        "archive_head":  {"attempt_number": 4, "attempt_record_sha256": attempt_hashes[4]},
    }
    return proposal, request, provenance, attempts, attempt_hashes


def test_composition_succeeds_for_attempt_3_plus_attempt_4() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    result = compose_supplemental_metadata_verification(
        proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
    )
    assert set(result["per_operation_parsed"]) == set(OPERATION_NAMES)
    p = result["per_operation_provenance"]
    assert p["archive_head"]["attempt_number"] == 4
    assert p["resource_page"]["attempt_number"] == 3
    assert p["eml"]["attempt_number"] == 3
    # Attempt-3 archive_head parse_failure is explicitly recorded as superseded.
    superseded = p["archive_head"].get("supersedes")
    assert superseded and any(s["superseded_attempt_number"] == 3 for s in superseded)


def test_composition_requires_every_operation_name() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    provenance = {k: v for k, v in provenance.items() if k != "archive_head"}
    with pytest.raises(AcquisitionError, match="OPERATION_NAMES"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_tampered_attempt_hash() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = "0" * 64
    with pytest.raises(AcquisitionError, match="does not match provenance"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_missing_attempt_record() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = {k: v for k, v in attempts.items() if k != 4}
    with pytest.raises(AcquisitionError, match="is not supplied"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_mismatched_release_endpoint() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    head_op = next(op for op in attempts[4]["operations"] if op["operation"] == "archive_head")
    head_op["final_url"] = "https://ipt.artsdatabanken.no/archive.do?r=other&v=9.999"
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="does not match the pinned"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_mismatched_request_hash() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    attempts[4]["request_sha256"] = "0" * 64
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="canonical request hash"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_mismatched_proposal_hash() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    attempts[4]["proposal_sha256"] = "f" * 64
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="source-selection proposal hash"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_unauthorized_supplemental_operation() -> None:
    """Attempt 4 is HEAD-only; it must not pretend to supply resource_page evidence."""
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    provenance = copy.deepcopy(provenance)
    provenance["resource_page"] = {
        "attempt_number": 4,
        "attempt_record_sha256": canonical_sha256(attempts[4]),
    }
    with pytest.raises(AcquisitionError, match="is not authorized to supply"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_missing_operation_in_attempt() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    attempts[4]["operations"] = [
        op for op in attempts[4]["operations"] if op["operation"] != "archive_head"
    ]
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="unique 'archive_head' operation"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_duplicate_conflicting_operation() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    attempts[4]["operations"] = attempts[4]["operations"] + copy.deepcopy(attempts[4]["operations"])
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="unique 'archive_head' operation"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_operation_marked_parse_failed() -> None:
    """Attempt 3's archive_head is parse_failed; composing it directly must fail."""
    proposal, request, provenance, attempts, hashes = _load_composition_inputs()
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"] = {"attempt_number": 3, "attempt_record_sha256": hashes[3]}
    with pytest.raises(AcquisitionError, match="not parse_succeeded"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_wrong_method_for_operation() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    head_op = next(op for op in attempts[4]["operations"] if op["operation"] == "archive_head")
    head_op["method"] = "GET"
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="method is 'GET'"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_redirect_chain_not_empty() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    head_op = next(op for op in attempts[4]["operations"] if op["operation"] == "archive_head")
    head_op["redirect_chain"] = ["https://ipt.artsdatabanken.no/other"]
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="redirect_chain is not empty"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_head_operation_with_non_zero_body_bytes() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    attempts = copy.deepcopy(attempts)
    head_op = next(op for op in attempts[4]["operations"] if op["operation"] == "archive_head")
    head_op["body_bytes"] = 1
    provenance = copy.deepcopy(provenance)
    provenance["archive_head"]["attempt_record_sha256"] = canonical_sha256(attempts[4])
    with pytest.raises(AcquisitionError, match="HEAD body_bytes is non-zero"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_composition_refuses_missing_provenance_hash() -> None:
    proposal, request, provenance, attempts, _ = _load_composition_inputs()
    provenance = copy.deepcopy(provenance)
    del provenance["archive_head"]["attempt_record_sha256"]
    with pytest.raises(AcquisitionError, match="does not bind an attempt hash"):
        compose_supplemental_metadata_verification(
            proposal=proposal, request=request, provenance=provenance, attempt_records=attempts,
        )


def test_supplemental_operation_registry_is_frozen() -> None:
    assert set(SUPPLEMENTAL_ATTEMPT_AUTHORIZED_OPERATIONS) == {4}
    assert SUPPLEMENTAL_ATTEMPT_AUTHORIZED_OPERATIONS[4] == ("archive_head",)


# ----- Final metadata-verification.json (composed) and revised acquisition proposal -----


def test_final_metadata_verification_exists_and_matches_composer_output() -> None:
    path = RELEASE / "metadata-verification.json"
    assert path.exists()
    data = json.loads(path.read_text())
    assert data["metadata_verification_schema_version"] == 3
    assert data["verdict"] == "passed"
    assert data["archive_acquisition_authorization_status"] == "not_authorized"
    assert data["policy_identifier"] == "nortaxa-archive-head-content-length-absent-v1"
    b = data["bound_evidence"]
    assert b["source_selection_proposal_sha256"] == PROPOSAL_SHA
    assert b["request_sha256"] == REQUEST_SHA
    assert b["attempt_1_sha256"] == ATTEMPT_1_SHA
    assert b["attempt_2_sha256"] == ATTEMPT_2_SHA
    assert b["attempt_3_sha256"] == ATTEMPT_3_SHA
    assert b["attempt_4_sha256"] == ATTEMPT_4_SHA
    assert b["policy_resolution_sha256"] == self_bound_canonical_sha256(RELEASE / "policy-resolution.json")
    # Per-operation provenance identifies attempts.
    p = data["per_operation_provenance"]
    assert p["resource_page"]["attempt_number"] == 3
    assert p["eml"]["attempt_number"] == 3
    assert p["archive_head"]["attempt_number"] == 4
    # Archive-HEAD evidence matches attempt-4's allowlisted headers exactly.
    head = data["observed"]["archive_head"]
    assert head["status_code"] == 200
    assert head["redirect_chain"] == []
    assert head["content_type_raw"] == "application/zip;charset=UTF-8"
    ah = head["allowlisted_headers"]
    assert ah["Content-Length"]["present"] is False
    assert ah["ETag"]["present"] is False
    assert ah["Accept-Ranges"]["present"] is False
    assert ah["Last-Modified"]["raw_values"] == ["Fri, 17 Jul 2026 12:08:23 GMT"]
    assert ah["Content-Disposition"]["raw_values"] == ['filename="dwca-artsnavnebase-v1.284.zip"']
    vm = data["verified_metadata"]
    for field in ("content_length", "etag", "accept_ranges"):
        assert vm[field]["value"] is None
        assert vm[field]["status"] == "not_exposed"
    assert vm["last_modified"]["status"] == "verified"
    assert vm["content_disposition"]["status"] == "verified"
    assert vm["declared_archive_size_bytes"]["status"] == "unavailable"
    assert vm["declared_archive_size_bytes"]["value"] is None


def test_final_metadata_verification_canonical_hash_is_stable() -> None:
    path = RELEASE / "metadata-verification.json"
    data = json.loads(path.read_text())
    assert data["canonical_sha256"] == self_bound_canonical_sha256(path)


def test_revised_acquisition_proposal_binds_attempt_4_and_final_metadata() -> None:
    path = TAXONOMY / "nortaxa-acquisition.proposal.json"
    data = json.loads(path.read_text())
    assert data["acquisition_proposal_schema_version"] == 3
    assert data["approval_status"] == "proposed"
    assert data["download_authorized"] is False
    b = data["bound_evidence"]
    assert b["attempt_4_sha256"] == ATTEMPT_4_SHA
    assert b["metadata_verification_sha256"] == self_bound_canonical_sha256(RELEASE / "metadata-verification.json")
    assert data["supersedes_prior_proposal"]["prior_acquisition_proposal_sha256"] == \
        "8cdc0bd6e8f5701d3f58bcbad062d47baf81c456a5f3174fa094f1552171f226"
    assert data["prerequisites"]["metadata_verification_final_artifact_exists"] is True
    assert data["prerequisites"]["executor_ready"] is False
    assert data["authorization_state"]["ready_for_approval"] is False
    assert data["authorization_state"]["blocked_by"] == "executor readiness"
    # One future bounded GET; no Range/retry/resume/authentication/fallback.
    ap = data["attempts_policy"]
    assert ap["permitted_future_get_attempts"] == 1
    for f in ("retries_authorized", "range_requests_authorized",
              "resume_authorized", "authentication_authorized",
              "fallback_endpoint_authorized"):
        assert ap[f] is False


def test_revised_acquisition_proposal_reports_missing_executor() -> None:
    data = json.loads((TAXONOMY / "nortaxa-acquisition.proposal.json").read_text())
    audit = data["prerequisites"]["executor_readiness_audit"]
    assert audit["expected_downloader_script"] == "database/taxonomy/scripts/acquire_nortaxa.py"
    assert audit["expected_downloader_present"] is False
    assert not (Path("database/taxonomy/scripts/acquire_nortaxa.py").exists())


def test_policy_resolution_unchanged_by_this_task() -> None:
    """policy-resolution.json must remain historical evidence."""
    assert self_bound_canonical_sha256(RELEASE / "policy-resolution.json") == \
        "771d02ca3c656e43eeb9c448838b25faab443947c20a93dc8451dc5f656918fc"


def test_resolvable_parse_failures_registry_is_frozen() -> None:
    assert set(RESOLVABLE_PARSE_FAILURES) == {"nortaxa-archive-head-content-length-absent-v1"}
    spec = RESOLVABLE_PARSE_FAILURES["nortaxa-archive-head-content-length-absent-v1"]
    assert spec["operation"] == "archive_head"
    assert "raw_headers_recorded" in spec["required_transport_evidence"]
    assert "content_length_presence_recorded" in spec["required_transport_evidence"]
