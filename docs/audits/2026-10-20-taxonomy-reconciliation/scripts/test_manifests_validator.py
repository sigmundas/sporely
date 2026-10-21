"""Validation tests for the revision-3 candidate manifests, run against the
REAL validator in the repository (read-only import):
database/taxonomy/scripts/bridge_emission.py::verify_supersession_manifest_approvals.

Nothing here weakens or patches the validator. Each test builds a synthetic
ledger document in memory ("TEST — NOT AN APPROVAL") that cites a generated
manifest file and one record per member, exactly as
generate_stage2_supersessions.py would, and asserts the validator's verdict.

Run from the worktree root (no cache or bytecode is written):
  PYTHONDONTWRITEBYTECODE=1 <venv>/bin/pytest -q -p no:cacheprovider \
      docs/audits/2026-10-20-taxonomy-reconciliation/scripts/test_manifests_validator.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.dont_write_bytecode = True
AUDIT = Path(__file__).resolve().parents[1]
REPO = AUDIT.parents[2]
sys.path.insert(0, str(REPO / "database/taxonomy/scripts"))
import bridge_emission as be  # noqa: E402

INDEX = json.loads((AUDIT / "manifests/index.json").read_text())
VALIDATOR_PARTS = sorted(k for k, v in INDEX["partitions"].items() if v["validator_format"])
OTHER_PARTS = sorted(k for k, v in INDEX["partitions"].items() if not v["validator_format"])


def _ledger(path: Path, manifest: dict) -> dict:
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
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


@pytest.mark.parametrize("part", VALIDATOR_PARTS)
def test_validator_accepts_generated_manifest(part):
    path = AUDIT / "manifests" / INDEX["partitions"][part]["path"]
    manifest = json.loads(path.read_text())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == INDEX["partitions"][part]["file_sha256"]
    assert manifest["member_count"] == len(manifest["members"]) == INDEX["partitions"][part]["member_count"]
    bound = be.verify_supersession_manifest_approvals(_ledger(path, manifest), repo_root=REPO)
    assert len(bound) == manifest["member_count"]


def test_groups_are_disjoint():
    seen = {}
    for part, info in INDEX["partitions"].items():
        path = AUDIT / "manifests" / info["path"]
        if info["validator_format"]:
            doc = json.loads(path.read_text())
            ids = [dict(zip(doc["columns"], r))["nortaxa_sporely_taxon_id"] for r in doc["members"]]
        else:
            ids = [json.loads(l)["retired_sporely_taxon_id"] for l in path.open()]
        for i in ids:
            assert i not in seen, f"{i} in {seen.get(i)} and {part}"
            seen[i] = part


def test_author_filtered_pairs_fail_unmodified_validator(tmp_path):
    """The 12 author-filtered pairs are kept OUT of validator manifests. Put
    into one honestly (one_to_one False), the real validator refuses them."""
    path = AUDIT / "manifests" / INDEX["partitions"]["A2-author-filtered"]["path"]
    rows = [json.loads(l) for l in path.open()]
    names = {r["record"]["scientific_name"] for r in rows}
    assert {"Stereum durum", "Uromyces trifolii", "Alectoria asiatica"} <= names
    assert "Helvella crispa" not in names  # now B: 'Scop.: Fr.' vs '(Scop.) Fr.' is structural
    template = json.loads((AUDIT / "manifests" / INDEX["partitions"]["A0-species"]["path"]).read_text())
    cols = template["columns"]
    members = []
    for r in rows:
        rec = r["record"]
        assert rec["one_to_one"] is False
        m = {c: None for c in cols}
        m.update({"nortaxa_taxon_id": rec["nortaxa_taxon_id"],
                  "nortaxa_sporely_taxon_id": rec["nortaxa_sporely_taxon_id"],
                  "col_usage_id": rec["col_usage_id"], "col_sporely_taxon_id": rec["col_sporely_taxon_id"],
                  "one_to_one": False})
        members.append([m[c] for c in cols])
    doc = copy.deepcopy(template)
    doc["members"], doc["member_count"] = members, len(members)
    p = tmp_path / "author-filtered.manifest.json"
    p.write_text(json.dumps(doc))
    with pytest.raises(be.BridgeEmissionError, match="not one-to-one"):
        be.verify_supersession_manifest_approvals(_ledger(p, doc), repo_root=REPO)


def test_tampered_manifest_is_refused(tmp_path):
    src = AUDIT / "manifests" / INDEX["partitions"]["A0-species"]["path"]
    doc = json.loads(src.read_text())
    ledger = _ledger(src, doc)
    p = tmp_path / "tampered.json"
    p.write_text(src.read_text().replace('"needs_review"', '"needs-review"'))
    ledger["approved_manifests"][0]["path"] = str(p)
    with pytest.raises(be.BridgeEmissionError, match="sha256"):
        be.verify_supersession_manifest_approvals(ledger, repo_root=REPO)


def test_missing_member_record_is_refused():
    src = AUDIT / "manifests" / INDEX["partitions"]["A0-species"]["path"]
    doc = json.loads(src.read_text())
    ledger = _ledger(src, doc)
    ledger["supersessions"].pop()
    with pytest.raises(be.BridgeEmissionError, match="no supersession record"):
        be.verify_supersession_manifest_approvals(ledger, repo_root=REPO)


def test_within_source_partition_has_no_validator_format():
    assert "A2-col-internal" in OTHER_PARTS
    assert INDEX["partitions"]["A2-col-internal"]["required_validator_change"]


def test_comparator_selftest_pinned():
    pin = INDEX["pins"]["comparator"]
    assert hashlib.sha256((REPO / pin["path"]).read_bytes()).hexdigest() == pin["sha256"]
    sys.path.insert(0, str(AUDIT / "scripts"))
    import authorship as au
    bad = [c for c in au.CASES if not au._ok(au.compare(c[0], c[1]), c[2])
           or not au._ok(au.compare(c[1], c[0]), c[2])]
    assert not bad
