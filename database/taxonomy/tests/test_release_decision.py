"""Validation of release decisions for candidate_build_only artifacts."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from release_decision import (  # noqa: E402
    PENDING_CANDIDATE_ONLY, ReleaseDecisionError, require_release_decision)

ID, SHA = "tax-2026.10.23-01", "a" * 64
GOOD = {"format": "sporely-taxonomy-release-decision-v1", "decision": "release_approved",
        "content_release_id": ID, "sqlite_sha256": SHA, "approved_by": "owner",
        "approved_at": "2026-10-23", "decision_reference": "ref"}


def _check(tmp_path, document, meta=PENDING_CANDIDATE_ONLY):
    path = tmp_path / "decision.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return require_release_decision(meta_value=meta, content_release_id=ID,
                                    sqlite_sha256=SHA, decision_path=path)


def test_bound_decision_is_accepted(tmp_path):
    assert _check(tmp_path, GOOD) == GOOD


def test_no_marker_needs_no_decision():
    assert require_release_decision(meta_value=None, content_release_id=ID,
                                    sqlite_sha256=SHA, decision_path=None) is None


def test_missing_decision_is_refused():
    with pytest.raises(ReleaseDecisionError, match="candidate_build_only"):
        require_release_decision(meta_value=PENDING_CANDIDATE_ONLY, content_release_id=ID,
                                 sqlite_sha256=SHA, decision_path=None)


@pytest.mark.parametrize("document", [[], [GOOD], "x", 1, None])
def test_non_object_is_refused(tmp_path, document):
    with pytest.raises(ReleaseDecisionError, match="JSON object"):
        _check(tmp_path, document)


@pytest.mark.parametrize("field", ["approved_by", "approved_at", "decision_reference",
                                   "decision", "content_release_id", "sqlite_sha256", "format"])
@pytest.mark.parametrize("value", [True, ["owner"], {"x": 1}, 7, "", None])
def test_wrong_types_are_refused(tmp_path, field, value):
    with pytest.raises(ReleaseDecisionError, match="non-empty string"):
        _check(tmp_path, {**GOOD, field: value})


@pytest.mark.parametrize("field,value", [("content_release_id", "tax-2026.10.23-02"),
                                         ("sqlite_sha256", "b" * 64)])
def test_mismatched_artifact_is_refused(tmp_path, field, value):
    with pytest.raises(ReleaseDecisionError, match="different artifact"):
        _check(tmp_path, {**GOOD, field: value})


def test_other_decision_is_refused(tmp_path):
    with pytest.raises(ReleaseDecisionError, match="expected 'release_approved'"):
        _check(tmp_path, {**GOOD, "decision": "rejected"})


def test_unknown_marker_is_refused(tmp_path):
    with pytest.raises(ReleaseDecisionError, match="unknown release_approval"):
        _check(tmp_path, GOOD, meta="pending:other")
