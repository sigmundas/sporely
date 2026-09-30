"""Taxonomy v3 Stage 1A: Group-A bridges from the owner-approved manifest.

The owner approved exactly one Group-A review manifest,
``group-a-shared-synonymy--ordinary`` (``file_sha256`` ``1eda453a…``). These
tests pin what that approval may and may not do:

* the shipped ledger holds one approved exact mapping per member of that file,
  and nothing for any other manifest's members;
* a record claiming the approval is refused unless it is exactly one member of
  the file the approval names, and the file still hashes to that digest;
* in a compiled release an ``ordinary`` member resolves to its concept, while a
  sibling-class member (Craterellus tubaeformis, NorTaxa 56227), a
  ``no_published_cross_reference`` member and a new taxon that only a later
  NorTaxa release would carry all stay unresolved.
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

_TAXONOMY = Path(__file__).resolve().parents[1]
_REPO = _TAXONOMY.parents[1]
_SCRIPTS = _TAXONOMY / "scripts"
_V3 = _TAXONOMY / "evidence" / "taxonomy-v3"
for _path in (_SCRIPTS, _V3, _TAXONOMY):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bridge_emission import (  # noqa: E402
    BridgeEmissionError,
    missing_review_provenance,
    verify_manifest_approvals,
)
from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import CompilerError, compile_release  # noqa: E402
from cross_source_mapping import EVIDENCE_CLASS_CROSS_SOURCE_STRICT  # noqa: E402
import generate_stage1a_mappings as generator  # noqa: E402
from validate_policies import PolicyError, validate  # noqa: E402

from test_bridge_emission import _resolvable_parents, _usages  # noqa: E402
from test_compile_release import _write_normalized_source  # noqa: E402

_POLICY_DIR = _TAXONOMY / "policies"
_LEDGER = _POLICY_DIR / "manual_mappings.yml"
_STAGE0 = _V3 / "stage0"
_ORDINARY = _STAGE0 / "group-a-shared-synonymy--ordinary.manifest.json"
_APPROVED_SHA = (
    "1eda453a7134995b2a596d09e0f10341a72ba7a007e2666a6e5cee9e118cda4d"
)
_COL_RELEASE = {"version": "2026-07-17-XR", "issued_date": "2026-07-17"}
_NORTAXA_RELEASE = {"version": "1.284", "issued_date": "2026-07-17"}


def _ledger() -> dict:
    return json.loads(_LEDGER.read_text(encoding="utf-8"))


def _members(path: Path) -> set[tuple[str, str, int]]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    cols = manifest["columns"]
    i, j, k = (cols.index(c) for c in
               ("nortaxa_taxon_id", "col_usage_id", "sporely_taxon_id"))
    return {(str(r[i]), str(r[j]), int(r[k])) for r in manifest["members"]}


def _bound_records(ledger: dict) -> list[dict]:
    """Stage 1A's records. The ledger also carries Stage 4P's Dyntaxa
    records, bound to their own approvals."""
    return [m for m in ledger["mappings"] if "approved_manifest" in m
            and m["source_usage"]["source"] == "nortaxa"]


# ------------------------------------------------------- the shipped ledger ---


def test_approved_manifest_is_the_owner_confirmed_file() -> None:
    assert hashlib.sha256(_ORDINARY.read_bytes()).hexdigest() == _APPROVED_SHA
    approvals = [a for a in _ledger()["approved_manifests"]
                 if "/stage0/" in a["path"]]
    assert [a["file_sha256"] for a in approvals] == [_APPROVED_SHA]
    assert approvals[0]["pins"] == json.loads(
        _ORDINARY.read_text(encoding="utf-8"))["pins"]


def test_shipped_records_are_exactly_the_approved_members() -> None:
    """One record per member, each tracing to the approved ``file_sha256``."""
    ledger = _ledger()
    bound = {mapping_id: sha
             for mapping_id, sha in verify_manifest_approvals(ledger).items()
             if sha == _APPROVED_SHA}
    records = _bound_records(ledger)
    assert len(records) == len(bound) == 3383
    assert set(bound.values()) == {_APPROVED_SHA}
    keys = {
        (r["source_usage"]["identifier"],
         r["target"]["source_usage"]["identifier"],
         r["approved_manifest"]["member"]["sporely_taxon_id"])
        for r in records
    }
    assert keys == _members(_ORDINARY)
    for record in records:
        assert record["review_status"] == "approved"
        assert record["relationship"] == "exact"
        assert not missing_review_provenance(record)
        assert record["source_release_range"] == {"first": "1.284", "last": "1.284"}
        assert any(_APPROVED_SHA in ref for ref in record["evidence_references"])


def test_no_other_manifest_member_has_a_record() -> None:
    """Siblings, no_published and the parent's remainder stay unreviewed."""
    mapped = {
        m["source_usage"]["identifier"] for m in _ledger()["mappings"]
        if m["source_usage"]["source"] == "nortaxa"
    }
    ordinary = {k[0] for k in _members(_ORDINARY)}
    for path in sorted(_STAGE0.glob("group-a-*.manifest.json")):
        if path == _ORDINARY:
            continue
        others = {k[0] for k in _members(path)} - ordinary
        assert not (others & mapped), path.name
    # Named cases: Craterellus tubaeformis is single_shared_synonym_low_overlap
    # (not approved); NBIC:56449 is the temporary unresolved Group-B fixture.
    assert "56227" not in mapped
    assert "56449" not in mapped


def test_regression_records_are_unchanged() -> None:
    """53482 keeps its individual record; 52369/58722 stay supersession-only."""
    mappings = _ledger()["mappings"]
    by_id = {m["source_usage"]["identifier"]: m for m in mappings}
    assert by_id["53482"]["mapping_id"] == \
        "nortaxa-53482-entoloma-conferendum-to-col-39ZCL"
    assert "approved_manifest" not in by_id["53482"]
    assert "52369" not in by_id and "58722" not in by_id


def test_shipped_ledger_validates_and_generator_is_current() -> None:
    validate()
    assert generator.render() == _LEDGER.read_text(encoding="utf-8")


# ------------------------------------------------------- generator gate ---


def test_generator_stops_on_a_different_manifest_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same members, different bytes: not the approved file."""
    altered = tmp_path / "ordinary.manifest.json"
    altered.write_bytes(_ORDINARY.read_bytes() + b"\n")
    monkeypatch.setattr(generator, "MANIFEST_PATH", str(altered))
    with pytest.raises(generator.GenerationError, match="sha256"):
        generator.render()


def test_generator_stops_when_pins_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pins = copy.deepcopy(generator.APPROVED_PINS)
    pins["source_archives"]["nortaxa"]["source_release_id"] = \
        "nortaxa:1.285:2026-10-01"
    monkeypatch.setattr(generator, "APPROVED_PINS", pins)
    with pytest.raises(generator.GenerationError, match="pins"):
        generator.render()


# ------------------------------------------------- loader fail-closed ---


def _approval(path: Path = _ORDINARY, sha: str = _APPROVED_SHA) -> dict:
    return {"path": str(path), "file_sha256": sha, "approved_by": "owner",
            "approved_at": "2026-09-28", "decision_reference": "plan"}


def _bound(nortaxa_id: str, col_id: str, sporely_id: int,
           sha: str = _APPROVED_SHA, col_namespace: str = "col_usage_id") -> dict:
    return {
        "mapping_id": f"probe-{nortaxa_id}",
        "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                         "identifier": nortaxa_id},
        "target": {"source_usage": {"source": "col_xr",
                                    "namespace": col_namespace,
                                    "identifier": col_id}},
        "relationship": "exact", "review_status": "approved",
        "reviewer": "owner", "rationale": "probe", "evidence_references": ["x"],
        "created_at": "2026-09-28T00:00:00Z",
        "updated_at": "2026-09-28T00:00:00Z",
        "source_release_range": {"first": "1.284", "last": "1.284"},
        "supersedes": None,
        "approved_manifest": {"file_sha256": sha, "member": {
            "nortaxa_taxon_id": nortaxa_id, "col_usage_id": col_id,
            "sporely_taxon_id": sporely_id}},
    }


def test_member_record_is_accepted() -> None:
    doc = {"approved_manifests": [_approval()],
           "mappings": [_bound("53057", "ZDXW", 620390)]}
    assert verify_manifest_approvals(doc) == {"probe-53057": _APPROVED_SHA}


@pytest.mark.parametrize("record,match", [
    # Sibling review class: Craterellus tubaeformis is low-overlap.
    (_bound("56227", "Z8TV", 620306), "not a member"),
    # no_published_cross_reference member.
    (_bound("120245", "YMTP", 619706), "not a member"),
    # A new taxon only a later NorTaxa release would carry.
    (_bound("999001", "NEWCOL", 999001), "not a member"),
    # Right member, wrong concept.
    (_bound("53057", "ZDXW", 1), "not a member"),
    # Cites an approval that was never recorded.
    (_bound("53057", "ZDXW", 620390, sha="0" * 64), "no approved_manifests"),
])
def test_non_member_record_is_refused(record: dict, match: str) -> None:
    doc = {"approved_manifests": [_approval()], "mappings": [record]}
    with pytest.raises(BridgeEmissionError, match=match):
        verify_manifest_approvals(doc)


def test_record_disagreeing_with_its_member_is_refused() -> None:
    record = _bound("53057", "ZDXW", 620390)
    record["target"]["source_usage"]["identifier"] = "Z8TV"
    with pytest.raises(BridgeEmissionError, match="do not match"):
        verify_manifest_approvals(
            {"approved_manifests": [_approval()], "mappings": [record]})


def test_approval_of_a_regenerated_file_is_refused(tmp_path: Path) -> None:
    altered = tmp_path / "ordinary.manifest.json"
    altered.write_bytes(_ORDINARY.read_bytes() + b"\n")
    doc = {"approved_manifests": [_approval(altered)],
           "mappings": [_bound("53057", "ZDXW", 620390)]}
    with pytest.raises(BridgeEmissionError, match="regenerated manifest"):
        verify_manifest_approvals(doc)


def test_compiler_and_validator_refuse_a_non_member(tmp_path: Path) -> None:
    ledger = _ledger()
    ledger["mappings"].append(_bound("56227", "Z8TV", 620306))
    ledger_path = tmp_path / "manual.yml"
    ledger_path.write_text(json.dumps(ledger, ensure_ascii=False),
                           encoding="utf-8")
    with pytest.raises(CompilerError, match="not a member"):
        compile_release(
            normalized_source_dirs=[],
            manual_mappings_path=ledger_path,
            mapping_policy_path=_POLICY_DIR / "mapping_policy.yml",
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / "release",
            release_id="tax-2026.09.28-01",
        )
    policy_dir = tmp_path / "policies"
    shutil.copytree(_POLICY_DIR, policy_dir)
    shutil.copy(ledger_path, policy_dir / "manual_mappings.yml")
    with pytest.raises(PolicyError, match="not a member"):
        validate(policy_dir)


# ------------------------------------------------- compiled emission ---


def _species(taxon_id: str, name: str, authorship: str, parent: str) -> dict:
    return {"core_row_id": f"row-{taxon_id}", "taxon_id": taxon_id,
            "parent": parent, "parent_resolution": "resolved",
            "scientific_name": name, "authorship": authorship,
            "status": "valid"}


#: (NorTaxa taxonID, COL usage, name, authorship). All four are Group-A
#: shaped: equal accepted name and authorship, so the compiler binds them
#: automatically. Only the first is an approved member.
_FIXTURE = [
    ("53057", "ZDXW", "Crepidotus cesatii", "(Rabenh.) Sacc."),      # ordinary
    ("56227", "Z8TV", "Craterellus tubaeformis", "(Fr.) Quél."),     # sibling
    ("120245", "YMTP", "Cortinarius terribilis", "Reumaux"),         # no_published
    ("999001", "NEWCOL", "Mycena stageoneaensis", "Fr."),            # later release
    ("53482", "39ZCL", "Entoloma conferendum", "(Britzelm.) Noordel."),
]


def _sources(root: Path) -> list[Path]:
    col = _resolvable_parents(_write_normalized_source(
        root / "col_xr", source_code="col_xr", source_release=_COL_RELEASE,
        identifier_namespace_prefix="COL:",
        rows=[{"core_row_id": "COL-K", "taxon_id": "COL-K",
               "scientific_name": "Fungi", "rank": "kingdom"}]
        + [{**_species(col_id, name, aut, "COL-K"), "core_row_id": col_id}
           for _, col_id, name, aut in _FIXTURE],
    ), "col_xr")
    nortaxa = _resolvable_parents(_write_normalized_source(
        root / "nortaxa", source_code="nortaxa",
        source_release=_NORTAXA_RELEASE, identifier_namespace_prefix="NBIC:",
        rows=[{"core_row_id": "row-K", "taxon_id": "9999",
               "scientific_name": "Fungi", "rank": "kingdom"}]
        + [_species(nt, name, aut, "9999") for nt, _, name, aut in _FIXTURE],
    ), "nortaxa")
    return [col, nortaxa]


def _fixture_ledger(path: Path) -> Path:
    """The shipped records for the fixture taxa, retargeted to its namespace.

    The shared fixture helper writes COL usages under ``col_xr_taxon_id``
    rather than the real ``col_usage_id``; everything else — the approval, the
    real manifest file and the member reference — is the shipped record.
    """
    shipped = _ledger()
    wanted = {nt for nt, *_ in _FIXTURE}
    mappings = []
    for entry in shipped["mappings"]:
        if entry["source_usage"]["identifier"] in wanted:
            entry = copy.deepcopy(entry)
            entry["target"]["source_usage"]["namespace"] = "col_xr_taxon_id"
            mappings.append(entry)
    assert sorted(m["source_usage"]["identifier"] for m in mappings) \
        == ["53057", "53482"]
    approvals = copy.deepcopy(shipped["approved_manifests"])
    for approval in approvals:
        approval["path"] = str(_REPO / approval["path"])
    path.write_text(json.dumps({"format": shipped["format"], "schema": {},
                                "approved_manifests": approvals,
                                "mappings": mappings}, ensure_ascii=False),
                    encoding="utf-8")
    return path


def test_only_the_approved_member_is_emitted(tmp_path: Path) -> None:
    release = tmp_path / "release"
    registry = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=_sources(tmp_path / "sources"),
        manual_mappings_path=_fixture_ledger(tmp_path / "manual.yml"),
        mapping_policy_path=_POLICY_DIR / "mapping_policy.yml",
        registry_path=registry,
        output_dir=release,
        release_id="tax-2026.09.28-01",
    )
    # Every fixture taxon is bound to its COL concept; the unapproved ones
    # only by the automatic match, which is exactly the Group-A population.
    usages = _usages(release)
    for nortaxa_id, *_ in _FIXTURE:
        usage = usages[("nortaxa", nortaxa_id)]
        assert usage["identity_binding"] == "alias", nortaxa_id
        assert usage["bridge_evidence_class"] == (
            "manual_approved_exact" if nortaxa_id in ("53057", "53482")
            else EVIDENCE_CLASS_CROSS_SOURCE_STRICT), nortaxa_id

    db_path = tmp_path / "candidate.sqlite3"
    summary = build_candidate(release_dir=release, registry_path=registry,
                              output_db=db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    def resolve(source: str, namespace: str, external_id: str) -> list[int]:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT taxon_id FROM taxon_external_id_text_min "
            "WHERE source_system = ? AND namespace = ? AND external_id = ?",
            (source, namespace, external_id))]

    for nortaxa_id, col_id, *_ in _FIXTURE:
        host = resolve("col_xr", "col_xr_taxon_id", col_id)
        assert len(host) == 1, col_id
        resolved = resolve("nortaxa", "nortaxa_taxon_id", nortaxa_id)
        if nortaxa_id in ("53057", "53482"):
            assert resolved == host, nortaxa_id
        else:
            assert resolved == [], nortaxa_id
    assert summary["authoritative_bridge_emission"][
        "emitted_by_evidence_class"] == {"manual_approved_exact": 2}
    conn.close()

    # The emitted bridge traces back to its record and the approved digest.
    records = {
        json.loads(line)["mapping_id"]: json.loads(line)
        for line in (release / "mappings.jsonl").read_text(
            encoding="utf-8").splitlines() if line.strip()
    }
    member = records["taxonomy-v3-1a-nortaxa-53057-to-col-ZDXW"]
    assert member["approved_manifest_file_sha256"] == _APPROVED_SHA
    assert "approved_manifest_file_sha256" not in records[
        "nortaxa-53482-entoloma-conferendum-to-col-39ZCL"]
