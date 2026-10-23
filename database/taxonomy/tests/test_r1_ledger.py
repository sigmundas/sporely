"""Taxonomy reconciliation R1: the committed supersession records.

R1 is a candidate build. Its records must be exactly the audited inputs
(S1 988, rel3-verified 147, corrected E0/E1 43), carry P10 and an honest
candidate-only approval scope, and form no chain or many-to-one merge.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

_TAXONOMY = Path(__file__).resolve().parents[1]
_SCRIPTS = _TAXONOMY / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from bridge_emission import verify_supersession_manifest_approvals  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "generate_r1_supersessions",
    _TAXONOMY / "evidence" / "taxonomy-r1" / "generate_r1_supersessions.py")
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

_LEDGER = _TAXONOMY / "policies" / "concept_supersessions.yml"


def _ledger() -> dict:
    return json.loads(_LEDGER.read_text(encoding="utf-8"))


def _r1(ledger: dict) -> list[dict]:
    return [s for s in ledger["supersessions"]
            if s["supersession_id"].startswith("taxonomy-r1-")]


def test_ledger_is_what_the_generator_renders() -> None:
    text, files = generator.render()
    assert text == _LEDGER.read_text(encoding="utf-8")
    for path, content in files.items():
        assert path.read_text(encoding="utf-8") == content


def test_r1_records_are_exactly_the_audited_inputs() -> None:
    ledger = _ledger()
    bound = verify_supersession_manifest_approvals(ledger)
    approvals = {a["file_sha256"]: a for a in ledger["approved_manifests"]
                 if a["manifest_id"].startswith("taxonomy-r1-")}
    per_manifest = Counter(approvals[sha]["manifest_id"] for sha in bound.values()
                           if sha in approvals)
    assert per_manifest == {
        "taxonomy-r1-A0-species": 943,
        "taxonomy-r1-A0-infraspecific": 45,
        "taxonomy-r1-rel3-verified": 129,
        "taxonomy-r1-E-E1-class-concept-family-agrees": 12,
        "taxonomy-r1-E-E1-default-class-error": 18,
        "taxonomy-r1-E-E1-verified-author-alias-default-class-error": 6,
        "taxonomy-r1-E-E1-verified-author-alias": 4,
    }
    records = _r1(ledger)
    individual = sorted(s["current_source_usage"]["identifier"] for s in records
                        if "approved_manifest" not in s)
    assert individual == ["458TC", "L8R6K", "TVWMN"]   # E0 pairing corrections
    assert len(records) == 988 + 129 + 40 + 3
    for held in ("nortaxa-54854-", "nortaxa-76921-", "nortaxa-226944-", "nortaxa-75668-"):
        assert not any(held in s["supersession_id"] for s in records)
    for record in records:
        if "approved_manifest" not in record:
            assert "pairing correction" in record["rationale"]
            assert any("replaces mispaired COL" in ref for ref in record["evidence_references"])


def test_r1_scope_is_candidate_only_and_p10() -> None:
    ledger = _ledger()
    for approval in ledger["approved_manifests"]:
        if approval["manifest_id"].startswith("taxonomy-r1-"):
            assert approval["approval_scope"] == "candidate_build_only"
            assert approval["release_approval"] == "pending"
            assert approval["inherited_usages"] == "names_only"
    for record in _r1(ledger):
        assert record["approval_scope"] == "candidate_build_only"
        assert record["release_approval"] == "pending"
        assert record["inherited_usages"] == "names_only"
        assert record["relationship"] == "exact" and record["review_status"] == "approved"


def test_derived_manifest_drops_only_the_held_pairs() -> None:
    derived = json.loads((_TAXONOMY / "evidence" / "taxonomy-r1" / "manifests" /
                          "r1-E-E1-default-class-error.manifest.json").read_text(encoding="utf-8"))
    source = json.loads((_TAXONOMY / "evidence" / "taxonomy-r1" / "manifests" / "source" /
                         "E-E1-default-class-error.manifest.json").read_text(encoding="utf-8"))
    assert derived["pins"] == source["pins"] and derived["columns"] == source["columns"]
    assert {h["nortaxa_taxon_id"] for h in derived["held_out"]} == {"76921", "226944"}
    rel3 = json.loads((_TAXONOMY / "evidence" / "taxonomy-r1" / "manifests" /
                       "r1-rel3-verified.manifest.json").read_text(encoding="utf-8"))
    assert len(rel3["held_out"]) == 18 and rel3["member_count"] == 129
    assert [m for m in source["members"] if m[0] not in ("76921", "226944")] == derived["members"]


def test_no_chain_and_no_survivor_takes_two_retirees() -> None:
    """H2/H3 on the ledger: a survivor never ends up with two NorTaxa accepted ids."""
    from identity_registry import IdentityRegistry

    registry = IdentityRegistry(_TAXONOMY / "registry" / "canonical")
    registry.load()
    retired = {}
    for record in _ledger()["supersessions"]:
        usage = record["current_source_usage"]
        current = registry.lookup(usage["source"], usage["namespace"], usage["identifier"])
        retired[record["superseded_sporely_taxon_id"]] = current.sporely_taxon_id
    assert not set(retired) & set(retired.values())
    r1_survivors = Counter(retired[s["superseded_sporely_taxon_id"]] for s in _r1(_ledger()))
    all_survivors = Counter(retired.values())
    assert all(all_survivors[s] == 1 for s in r1_survivors)
