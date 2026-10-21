"""Validation tests for the pass-3 relationship manifests, run against the REAL,
unmodified validator (read-only import):
database/taxonomy/scripts/bridge_emission.py::verify_supersession_manifest_approvals.

Modelled on ../../../scripts/test_manifests_validator.py. Each test builds a
synthetic ledger in memory ("TEST — NOT AN APPROVAL") citing a generated
manifest and one record per member. Nothing is written to the repository.

  PYTHONDONTWRITEBYTECODE=1 <venv>/bin/pytest -q -p no:cacheprovider --noconftest \
      docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3/scripts/test_manifests_validator_v3.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.dont_write_bytecode = True
V3 = Path(__file__).resolve().parents[1]
REPO = V3.parents[4]
sys.path.insert(0, str(REPO / "database/taxonomy/scripts"))
import bridge_emission as be  # noqa: E402

INDEX = json.loads((V3 / "manifests/index.json").read_text())
PARTS = sorted(INDEX["partitions"])


def _ledger(path: Path, manifest: dict) -> dict:
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    cols = manifest["columns"]
    records = []
    for row in manifest["members"]:
        m = dict(zip(cols, row))
        records.append({
            "supersession_id": f"test-{m['nortaxa_taxon_id']}-{m['col_usage_id']}",
            "superseded_sporely_taxon_id": int(m["nortaxa_sporely_taxon_id"]),
            "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id",
                                     "identifier": str(m["col_usage_id"])},
            "relationship": "exact", "review_status": "approved",
            "approved_manifest": {"file_sha256": sha, "member": {
                k: m[k] for k in be.SUPERSESSION_MEMBER_COLUMNS}},
        })
    return {"approved_manifests": [{
        "path": str(path), "file_sha256": sha, "approved_by": "TEST — NOT AN APPROVAL",
        "approved_at": "1970-01-01", "decision_reference": "audit validation test only",
        "pins": manifest["pins"]}], "supersessions": records}


@pytest.mark.parametrize("part", PARTS)
def test_validator_accepts_generated_manifest(part):
    path = V3 / "manifests" / INDEX["partitions"][part]["path"]
    manifest = json.loads(path.read_text())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == INDEX["partitions"][part]["file_sha256"]
    assert manifest["member_count"] == len(manifest["members"]) == INDEX["partitions"][part]["member_count"]
    if not manifest["members"]:
        pytest.skip("empty partition")
    bound = be.verify_supersession_manifest_approvals(_ledger(path, manifest), repo_root=REPO)
    assert len(bound) == manifest["member_count"]


def test_partitions_are_disjoint_and_one_to_one():
    seen_n, seen_c = {}, {}
    for part, info in INDEX["partitions"].items():
        doc = json.loads((V3 / "manifests" / info["path"]).read_text())
        for row in doc["members"]:
            m = dict(zip(doc["columns"], row))
            assert m["nortaxa_sporely_taxon_id"] not in seen_n, (part, seen_n.get(m["nortaxa_sporely_taxon_id"]))
            assert m["col_sporely_taxon_id"] not in seen_c, (part, seen_c.get(m["col_sporely_taxon_id"]))
            seen_n[m["nortaxa_sporely_taxon_id"]] = seen_c[m["col_sporely_taxon_id"]] = part


def test_no_member_is_in_s1_a0():
    for info in INDEX["partitions"].values():
        doc = json.loads((V3 / "manifests" / info["path"]).read_text())
        assert not any(dict(zip(doc["columns"], r))["s1_package_partition"].startswith("A0") for r in doc["members"])


def test_copied_source_provenance_never_alone_verifies():
    """Every verified member has a non-ChecklistBank verified-eligible witness."""
    rows = [json.loads(l) for l in (V3 / "data/verified.jsonl").open(encoding="utf-8")]
    for r in rows:
        elig = [e for e in r["evidence"] if e.get("verified_eligible")]
        assert elig and all(e["kind"] != "checklistbank_source_record" for e in elig), r["scientific_name"]
    all_ev = [e for f in ("verified", "pragmatic_compatible", "pragmatic_missing_authorship", "unresolved")
              for l in (V3 / f"data/{f}.jsonl").open(encoding="utf-8") for e in json.loads(l)["evidence"]
              if e["kind"] == "checklistbank_source_record"]
    assert all_ev and not any(e["verified_eligible"] or e["independent_corroboration"] for e in all_ev)


def test_not_one_to_one_pairs_are_refused_by_unmodified_validator(tmp_path):
    """Pairs that are not one-to-one stay out of every manifest. Put into one
    honestly (one_to_one False), the real validator refuses them."""
    rows = [json.loads(l) for l in (V3 / "data/unresolved.jsonl").open(encoding="utf-8")]
    bad = [r for r in rows if r["one_to_one"] is not True]
    assert bad
    template = json.loads((V3 / "manifests" / INDEX["partitions"]["rel3-verified"]["path"]).read_text())
    cols = template["columns"]
    members = []
    for r in bad:
        m = {c: None for c in cols}
        m.update({k: r[k] for k in ("nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id", "col_sporely_taxon_id")})
        m["one_to_one"] = False
        members.append([m[c] for c in cols])
    doc = copy.deepcopy(template)
    doc["members"], doc["member_count"] = members, len(members)
    p = tmp_path / "not-one-to-one.manifest.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(be.BridgeEmissionError, match="not one-to-one"):
        be.verify_supersession_manifest_approvals(_ledger(p, doc), repo_root=REPO)


def test_tampered_manifest_is_refused(tmp_path):
    src = V3 / "manifests" / INDEX["partitions"]["rel3-verified"]["path"]
    doc = json.loads(src.read_text())
    ledger = _ledger(src, doc)
    p = tmp_path / "tampered.json"
    p.write_text(src.read_text().replace('"needs_review"', '"needs-review"'))
    ledger["approved_manifests"][0]["path"] = str(p)
    with pytest.raises(be.BridgeEmissionError, match="sha256"):
        be.verify_supersession_manifest_approvals(ledger, repo_root=REPO)


def test_missing_member_record_is_refused():
    src = V3 / "manifests" / INDEX["partitions"]["rel3-verified"]["path"]
    doc = json.loads(src.read_text())
    ledger = _ledger(src, doc)
    ledger["supersessions"].pop()
    with pytest.raises(be.BridgeEmissionError, match="no supersession record"):
        be.verify_supersession_manifest_approvals(ledger, repo_root=REPO)


def test_pins_match_files():
    for name in ("comparator", "relationship_rules", "checklistbank_source_records"):
        pin = INDEX["pins"][name]
        assert hashlib.sha256((REPO / pin["path"]).read_bytes()).hexdigest() == pin["sha256"], name
