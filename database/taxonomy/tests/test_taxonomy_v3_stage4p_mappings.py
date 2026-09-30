"""Taxonomy v3 Stage 4P: the batch-approval path for Dyntaxa manifests.

No Dyntaxa manifest is approved. These tests use synthetic approvals of the
real committed manifests to show the dormant path works and fails closed:
the generator yields one record per member of exactly the approved file, the
compiler's verifier accepts those records and refuses any other, and a
manifest-bound Dyntaxa record is consumed by the compiler as a reviewed
bridge. Nothing here writes the real ledger.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_TAXONOMY = Path(__file__).resolve().parents[1]
_REPO = _TAXONOMY.parents[1]
_EVIDENCE = _TAXONOMY / "evidence" / "taxonomy-v3"
for _path in (_TAXONOMY / "scripts", _EVIDENCE):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

_spec = importlib.util.spec_from_file_location(
    "generate_stage4p_mappings", _EVIDENCE / "generate_stage4p_mappings.py")
generator = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = generator
_spec.loader.exec_module(generator)

from bridge_emission import BridgeEmissionError, verify_manifest_approvals  # noqa: E402
from compile_release import compile_release  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402

from test_bridge_emission import _POLICY_PATH, _resolve  # noqa: E402
from test_compile_release import _with_fixture_provenance  # noqa: E402
from test_taxonomy_v3_stage4p import (  # noqa: E402
    _RUGOSA, _candidate, _col_source, _dyntaxa_source, _dyntaxa_usages,
    _fixture_pins, _nortaxa_source,
)

_STAGE4P = "database/taxonomy/evidence/taxonomy-v3/stage4p"
_RECIPROCAL = f"{_STAGE4P}/dyntaxa-reciprocal-accepted-synonymy.manifest.json"
_ONE_DIRECTIONAL = f"{_STAGE4P}/dyntaxa-one-directional-accepted-synonymy.manifest.json"
_PARENT_SHARED = f"{_STAGE4P}/dyntaxa-shared-synonymy.manifest.json"
_LEDGER = _TAXONOMY / "policies" / "manual_mappings.yml"


def _sha(path: str) -> str:
    return hashlib.sha256((_REPO / path).read_bytes()).hexdigest()


def _manifest(path: str) -> dict:
    return json.loads((_REPO / path).read_text(encoding="utf-8"))


def _approval(path: str = _RECIPROCAL, **overrides) -> dict:
    return {"path": path, "file_sha256": _sha(path),
            "pins": _manifest(path)["pins"], "approved_by": "synthetic owner",
            "approved_at": "2099-01-01",
            "decision_reference": "synthetic test decision", **overrides}


@pytest.fixture(scope="module")
def registry() -> IdentityRegistry:
    loaded = IdentityRegistry(_TAXONOMY / "registry" / "canonical")
    loaded.load()
    return loaded


@pytest.fixture(scope="module")
def generated(registry: IdentityRegistry) -> dict:
    document = json.loads(_LEDGER.read_text(encoding="utf-8"))
    return generator.render_document(document, [_approval()], registry=registry)


# ------------------------------------------------------------ generator ---


def test_the_committed_generator_approves_nothing() -> None:
    assert generator.OWNER_APPROVALS == ()


def test_one_record_per_member_of_exactly_the_approved_file(
    generated: dict,
) -> None:
    manifest = _manifest(_RECIPROCAL)
    columns = manifest["columns"]
    members = {(m[columns.index("dyntaxa_taxon_id")],
                m[columns.index("col_usage_id")],
                m[columns.index("sporely_taxon_id")])
               for m in manifest["members"]}
    records = [r for r in generated["mappings"]
               if r["source_usage"]["source"] == "dyntaxa"]
    assert len(records) == manifest["member_count"] == len(members)
    assert {(r["approved_manifest"]["member"]["dyntaxa_taxon_id"],
             r["approved_manifest"]["member"]["col_usage_id"],
             r["approved_manifest"]["member"]["sporely_taxon_id"])
            for r in records} == members
    rugosa = next(r for r in records
                  if r["source_usage"]["identifier"]
                  == "urn:lsid:dyntaxa.se:Taxon:3423")
    assert rugosa["approved_manifest"]["member"]["sporely_taxon_id"] == 83668
    assert rugosa["source_release_range"] == {"first": "2026-09-30",
                                              "last": "2026-09-30"}
    assert rugosa["target"]["source_usage"]["namespace"] == "col_usage_id"
    # The NorTaxa ledger rides along untouched.
    original = json.loads(_LEDGER.read_text(encoding="utf-8"))
    assert [r for r in generated["mappings"]
            if r["source_usage"]["source"] != "dyntaxa"] == original["mappings"]
    assert generated["approved_manifests"][:-1] == original["approved_manifests"]
    assert verify_manifest_approvals(generated, repo_root=_REPO)


@pytest.mark.parametrize("approval,match", [
    (_approval(_ONE_DIRECTIONAL), "batch approval only"),
    (_approval(_PARENT_SHARED), "parent shared_synonymy"),
    (_approval(file_sha256="0" * 64), "Stopping: a different file"),
    (_approval(pins={"release": {}}), "pins differ"),
    ({**_approval(), "approved_by": ""}, "approval carries no approved_by"),
])
def test_generator_refuses(registry: IdentityRegistry, approval: dict,
                           match: str) -> None:
    document = json.loads(_LEDGER.read_text(encoding="utf-8"))
    with pytest.raises(generator.GenerationError, match=match):
        generator.render_document(document, [approval], registry=registry)


def test_generator_refuses_a_manifest_of_another_dyntaxa_export(
    registry: IdentityRegistry, tmp_path: Path,
) -> None:
    acquisition = json.loads(generator.ACQUISITION_MANIFEST.read_text())
    acquisition["download"]["sha256"] = "f" * 64
    later = tmp_path / "manifest.json"
    later.write_text(json.dumps(acquisition), encoding="utf-8")
    document = json.loads(_LEDGER.read_text(encoding="utf-8"))
    with pytest.raises(generator.GenerationError, match="other than the pinned"):
        generator.render_document(document, [_approval()], registry=registry,
                                  acquisition_manifest=later)


# ------------------------------------------------------------- verifier ---


def _record(generated: dict, lsid: str) -> dict:
    return copy.deepcopy(next(r for r in generated["mappings"]
                              if r["source_usage"]["identifier"] == lsid))


def test_verifier_refuses_a_non_member_borrowing_the_approval(
    generated: dict,
) -> None:
    doc = copy.deepcopy(generated)
    # Craterellus tubaeformis grades no_published_cross_reference.
    borrowed = _record(generated, "urn:lsid:dyntaxa.se:Taxon:3423")
    borrowed["mapping_id"] = "borrowed"
    borrowed["source_usage"]["identifier"] = "urn:lsid:dyntaxa.se:Taxon:3217"
    borrowed["target"]["source_usage"]["identifier"] = "Z8TV"
    borrowed["approved_manifest"]["member"] = {
        "dyntaxa_taxon_id": "urn:lsid:dyntaxa.se:Taxon:3217",
        "col_usage_id": "Z8TV", "sporely_taxon_id": 620306}
    doc["mappings"].append(borrowed)
    with pytest.raises(BridgeEmissionError, match="not a member"):
        verify_manifest_approvals(doc, repo_root=_REPO)


def test_verifier_refuses_a_record_outside_the_pinned_release(
    generated: dict,
) -> None:
    doc = copy.deepcopy(generated)
    record = next(r for r in doc["mappings"]
                  if r["source_usage"]["source"] == "dyntaxa")
    record["source_release_range"] = {"first": "2026-09-30", "last": "2027-01-01"}
    with pytest.raises(BridgeEmissionError, match="source_release_range"):
        verify_manifest_approvals(doc, repo_root=_REPO)


def test_verifier_requires_the_dyntaxa_approval_to_restate_its_pins(
    generated: dict,
) -> None:
    doc = copy.deepcopy(generated)
    del doc["approved_manifests"][-1]["pins"]
    with pytest.raises(BridgeEmissionError, match="pins do not equal"):
        verify_manifest_approvals(doc, repo_root=_REPO)


@pytest.mark.parametrize("field", ["sha256", "source_release_id", None])
def test_verifier_requires_complete_col_pins_on_a_dyntaxa_manifest(
    tmp_path: Path, generated: dict, field: str | None,
) -> None:
    """An approval restating incomplete pins, of a file with incomplete pins,
    is refused: the file names no COL archive it was reviewed against."""
    manifest = _manifest(_RECIPROCAL)
    if field is None:
        del manifest["pins"]["source_archives"]["col_xr"]
    else:
        del manifest["pins"]["source_archives"]["col_xr"][field]
    path = tmp_path / "incomplete.manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    doc = copy.deepcopy(generated)
    doc["approved_manifests"][-1].update(
        path=str(path), file_sha256=sha, pins=manifest["pins"])
    for record in doc["mappings"]:
        if record["source_usage"]["source"] == "dyntaxa":
            record["approved_manifest"]["file_sha256"] = sha
    with pytest.raises(BridgeEmissionError, match="does not pin the col_xr"):
        verify_manifest_approvals(doc, repo_root=_REPO)


def test_verifier_refuses_a_namespace_that_disagrees_with_the_manifest(
    generated: dict,
) -> None:
    doc = copy.deepcopy(generated)
    record = next(r for r in doc["mappings"]
                  if r["source_usage"]["source"] == "dyntaxa")
    record["source_usage"]["source"] = "nortaxa"
    with pytest.raises(BridgeEmissionError, match="do not match"):
        verify_manifest_approvals(doc, repo_root=_REPO)


# ------------------------------------------------------------- compiler ---


def test_compiler_consumes_a_manifest_bound_dyntaxa_record(tmp_path: Path) -> None:
    """A synthetic Dyntaxa manifest over the fixture, approved once; the
    record the compiler receives has the generator's shape."""
    columns = ["dyntaxa_taxon_id", "col_usage_id", "sporely_taxon_id"]
    manifest_path = tmp_path / "fixture.manifest.json"

    root = tmp_path / "sources"
    sources = [_col_source(root), _nortaxa_source(root), _dyntaxa_source(root)]
    # The manifest is pinned to the fixture inputs actually compiled.
    pins = {"source_archives": _fixture_pins(root)}

    def compile_with(ledger: dict, name: str, release_id: str) -> Path:
        path = tmp_path / f"{name}.yml"
        path.write_text(json.dumps(ledger), encoding="utf-8")
        out = tmp_path / name
        compile_release(
            normalized_source_dirs=sources, manual_mappings_path=path,
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl", output_dir=out,
            release_id=release_id)
        return out

    first = compile_with({"mappings": []}, "first", "tax-2026.09.30-01")
    host = next(json.loads(line) for line in
                (first / "source_usages.jsonl").read_text().splitlines()
                if json.loads(line)["source_usage"]["identifier"] == "5ZT3G"
                )["sporely_taxon_id"]
    manifest_path.write_text(json.dumps({
        "population": generator.POPULATION, "candidate_kind": "matched",
        "evidence_class": "reciprocal_accepted_synonymy", "pins": pins,
        "columns": columns, "members": [[_RUGOSA, "5ZT3G", host]],
        "member_count": 1}), encoding="utf-8")
    sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    record = {
        "mapping_id": "taxonomy-v3-4p-dyntaxa-1001-to-col-5ZT3G",
        "source_usage": {"source": "dyntaxa", "namespace": "dyntaxa_taxon_id",
                         "identifier": _RUGOSA},
        "target": {"source_usage": {"source": "col_xr",
                                    "namespace": "col_xr_taxon_id",
                                    "identifier": "5ZT3G"}},
        "relationship": "exact", "review_status": "approved",
        "source_release_range": {"first": "2026-09-30", "last": "2026-09-30"},
        "approved_manifest": {"file_sha256": sha, "member": {
            "dyntaxa_taxon_id": _RUGOSA, "col_usage_id": "5ZT3G",
            "sporely_taxon_id": host}},
    }
    ledger = {
        "approved_manifests": [{
            "path": str(manifest_path), "file_sha256": sha, "pins": pins,
            "approved_by": "synthetic owner", "approved_at": "2099-01-01",
            "decision_reference": "synthetic"}],
        "mappings": _with_fixture_provenance([record]),
    }
    release = compile_with(ledger, "second", "tax-2026.09.30-02")
    usage = _dyntaxa_usages(release)[_RUGOSA]
    assert usage["sporely_taxon_id"] == host
    assert usage["alias_reason"] == "manual_approved_exact"
    conn = _candidate(tmp_path, release)
    try:
        assert _resolve(conn, "dyntaxa", "dyntaxa_taxon_id", _RUGOSA) == [host]
    finally:
        conn.close()
