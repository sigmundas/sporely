"""Quarterly refresh: acquisition parameterisation, continuity receipt,
compiler hook and prepare stop conditions (offline, synthetic)."""
from __future__ import annotations

import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import acquire_dyntaxa  # noqa: E402
import acquire_nortaxa  # noqa: E402
import carry_forward  # noqa: E402
import prepare_refresh  # noqa: E402
from col_xr_delivery import _exact_public_endpoint  # noqa: E402
from compile_release import (  # noqa: E402
    ManualMapping, continuity_blockers, continuity_inapplicable, load_continuity_receipt,
    CompilerError,
)
from refresh_col_xr import AcquisitionError  # noqa: E402

KEY = "abcdef0123456789abcdef0123456789"


# ----- acquisition -----------------------------------------------------------

def _dwca() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name in acquire_dyntaxa.REQUIRED_MEMBERS:
            z.writestr(name, "<eml><pubDate>2026-10-20</pubDate></eml>" if name == "eml.xml" else "x")
    return buf.getvalue()


class FakeTransport:
    def __init__(self, body: bytes, redirected: bool = False, status: int = 200):
        self.body, self.redirected, self.status, self.urls = body, redirected, status, []

    def open(self, url):
        self.urls.append(url)
        return acquire_dyntaxa.Response(self.status, {"Content-Type": "application/zip",
                                                      "Content-Length": str(len(self.body))},
                                        io.BytesIO(self.body), self.redirected)


def _dyntaxa_release(tmp_path: Path) -> Path:
    release = tmp_path / "sources" / "dyntaxa" / "2026-10-23"
    release.mkdir(parents=True)
    (release / "acquisition-approval.json").write_text(json.dumps({
        **acquire_dyntaxa.REQUIRED_APPROVAL, "version": "2026-10-23", "approved_maximum_bytes": 1 << 20,
        "approved_by": "owner", "approved_at": "2026-10-23", "scope": "test"}))
    return release


def test_dyntaxa_key_is_used_for_the_get_but_never_written(tmp_path):
    release = _dyntaxa_release(tmp_path)
    transport = FakeTransport(_dwca())
    manifest = acquire_dyntaxa.acquire(release, release / "archive.zip", "2026-10-23", KEY, transport)
    assert KEY in transport.urls[0]
    assert manifest["release"]["issued_date"] == "2026-10-20"
    for path in release.iterdir():
        if path.suffix == ".json":
            assert KEY not in path.read_text()


def test_dyntaxa_errors_are_redacted_and_redirects_refused(tmp_path):
    release = _dyntaxa_release(tmp_path)

    class Leaky(FakeTransport):
        def open(self, url):
            raise RuntimeError(f"boom at {url}")
    with pytest.raises(acquire_dyntaxa.DyntaxaAcquisitionError) as info:
        acquire_dyntaxa.acquire(release, release / "archive.zip", "2026-10-23", KEY, Leaky(b""))
    assert KEY not in str(info.value) and "<redacted>" in str(info.value)
    with pytest.raises(acquire_dyntaxa.DyntaxaAcquisitionError, match="redirected"):
        acquire_dyntaxa.acquire(release, release / "archive.zip", "2026-10-23", KEY,
                                FakeTransport(_dwca(), redirected=True))
    assert not (release / "manifest.json").exists()


def test_dyntaxa_requires_approval(tmp_path):
    release = tmp_path / "sources" / "dyntaxa" / "2026-10-23"
    release.mkdir(parents=True)
    with pytest.raises(acquire_dyntaxa.DyntaxaAcquisitionError, match="missing acquisition approval"):
        acquire_dyntaxa.acquire(release, release / "archive.zip", "2026-10-23", KEY, FakeTransport(_dwca()))


def test_nortaxa_new_version_requires_matching_approval(tmp_path):
    paths = acquire_nortaxa.ReleasePaths(taxonomy_root=tmp_path, version="1.292")
    paths.release.mkdir(parents=True)
    (paths.release / "manifest.json").write_text(json.dumps({"execution_attempts": []}))

    class Request:
        version, source_selection_proposal_sha256, request_sha256 = "1.292", "p" * 64, "r" * 64
        archive_endpoint = "https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.292"
    with pytest.raises(AcquisitionError, match="missing acquisition approval"):
        acquire_nortaxa.require_version_approval(paths, Request)
    approval = {**acquire_nortaxa.REQUIRED_APPROVAL_FLAGS, "source_code": "nortaxa", "version": "1.292",
                "source_selection_proposal_sha256": "p" * 64, "request_sha256": "r" * 64,
                "approved_canonical_endpoint": Request.archive_endpoint, "approved_maximum_bytes": 1000,
                "approved_by": "owner", "approved_at": "2026-10-23", "scope": "test"}
    paths.approval.write_text(json.dumps(approval))
    assert acquire_nortaxa.require_version_approval(paths, Request)["version"] == "1.292"
    paths.approval.write_text(json.dumps({**approval, "approved_canonical_endpoint": "https://x/"}))
    with pytest.raises(AcquisitionError, match="approved_canonical_endpoint"):
        acquire_nortaxa.require_version_approval(paths, Request)
    paths.approval.write_text(json.dumps(approval))
    (paths.release / "manifest.json").write_text(json.dumps({"execution_attempts": [{"outcome": "failed"}]}))
    with pytest.raises(AcquisitionError, match="spent"):
        acquire_nortaxa.require_version_approval(paths, Request)
    assert acquire_nortaxa.ReleasePaths(tmp_path).source_proposal.name == "nortaxa-source-selection.proposal.json"


def test_col_endpoint_follows_the_proposal_dataset_key():
    url = "https://api.checklistbank.org/dataset/316691/export.zip?extended=true&format=ColDP"
    _exact_public_endpoint(url, 316691)
    _exact_public_endpoint(url)
    with pytest.raises(AcquisitionError):
        _exact_public_endpoint(url, 315834)


# ----- continuity receipt ----------------------------------------------------

def _row(source, ns, value, name, rank="species", status="accepted", authorship="L.", accepted=None):
    return {"source_code": source, "source_release": {"version": "v", "issued_date": "d"},
            "core_row_id": {"namespace": ns, "value": value}, "taxon_id": {"namespace": ns, "value": value},
            "accepted_name_usage_id": accepted, "parent_name_usage_id": None,
            "scientific_name": f"{name} {authorship}", "authorship": authorship, "rank": rank,
            "taxonomic_status": status, "classification": {"kingdom": "Fungi"}}


def _norm(tmp_path: Path, label: str, rows: list[dict]) -> Path:
    directory = tmp_path / label
    directory.mkdir()
    (directory / "taxa.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    return directory


MAPPINGS = {"mappings": [
    {"mapping_id": f"m-{n}", "review_status": "approved",
     "source_usage": {"source": "dyntaxa", "namespace": "dyntaxa_taxon_id", "identifier": f"T:{n}"},
     "target": {"source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": f"C{n}"}},
     "approved_manifest": {"file_sha256": "f" * 64}} for n in range(1, 6)]
    + [{"mapping_id": "rej", "review_status": "rejected",
        "source_usage": {"source": "dyntaxa", "namespace": "dyntaxa_taxon_id", "identifier": "T:9"},
        "target": {"source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": "C9"}}}]}


def _receipt(tmp_path, holds=None):
    col = _norm(tmp_path, "col", [_row("col_xr", "col_usage_id", f"C{n}", f"Genus sp{n}") for n in range(1, 6)])
    old = _norm(tmp_path, "dyn-old", [
        _row("dyntaxa", "dyntaxa_taxon_id", "T:1", "Genus sp1"),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:2", "Genus sp2", authorship="(Fr.) P.Kumm."),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:3", "Genus sp3"),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:4", "Genus sp4", authorship="Fr."),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:5", "Genus sp5")])
    new = _norm(tmp_path, "dyn-new", [
        _row("dyntaxa", "dyntaxa_taxon_id", "T:1", "Genus sp1"),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:2", "Genus sp2", authorship="(Fr.)  P. Kumm."),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:3", "Genus other"),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:4", "Genus sp4", authorship="Pers."),
        _row("dyntaxa", "dyntaxa_taxon_id", "T:5", "Genus sp5", status="synonym",
             accepted={"namespace": "dyntaxa_taxon_id", "value": "T:1"})])
    pins = lambda d: {"col_xr": {"source_release_id": "col_xr:a:b", "sha256": "c" * 64},
                      "dyntaxa": {"source_release_id": f"dyntaxa:{d}:{d}", "sha256": d[0] * 64}}
    return carry_forward.build_receipt(
        release_id="tax-2026.10.23-02", previous_dirs={"col_xr": col, "dyntaxa": old},
        current_dirs={"col_xr": col, "dyntaxa": new}, previous_pins=pins("1"), current_pins=pins("2"),
        manual_mappings=MAPPINGS, supersessions={"supersessions": []}, anchors={}, holds=holds)


def test_receipt_carries_unchanged_and_typography_and_flags_changes(tmp_path):
    receipt = _receipt(tmp_path)
    records = receipt["records"]
    assert records["m-1"]["status"] == "carried"
    assert records["m-2"]["status"] == "carried"  # typography-only authorship change
    assert any(r.endswith("scientific_name_changed") for r in records["m-3"]["reasons"])
    assert any(r.endswith("authorship_changed") for r in records["m-4"]["reasons"])
    assert {r.rsplit(":", 1)[1] for r in records["m-5"]["reasons"]} >= {"taxonomic_status_changed", "accepted_changed"}
    assert "rej" not in records
    assert receipt["summary"]["dyntaxa_mapping"] == {"carried": 2, "flagged": 3, "held": 0}


def test_missing_and_ambiguous_identifiers_are_flagged():
    snap = carry_forward.Snapshot("A b", "L.", "species", "accepted", "", "", "Fungi")
    assert carry_forward.compare_identifier([snap], []) == ["missing"]
    assert carry_forward.compare_identifier([snap], [snap, snap]) == ["ambiguous"]
    assert carry_forward.compare_identifier([], []) == []
    assert carry_forward.compare_identifier([], [snap]) == ["absent_in_reviewed_release"]


# ----- compiler hook ---------------------------------------------------------

def _mapping(mid: str, rng=("1", "1"), sha="f" * 64) -> ManualMapping:
    return ManualMapping(mapping_id=mid, source_usage=("dyntaxa", "dyntaxa_taxon_id", "T:1"),
                         target_source_usage=("col_xr", "col_usage_id", "C1"), target_sporely_taxon_id=None,
                         relationship="exact", review_status="approved", approved_manifest_file_sha256=sha,
                         source_release_range=rng,
                         reviewed_source_pins=(("col_xr", "col_xr:a:b", "c" * 64),
                                               ("dyntaxa", "dyntaxa:1:1", "1" * 64)))


def test_compiler_accepts_carried_record_and_reports_flagged(tmp_path):
    receipt = _receipt(tmp_path)
    assert continuity_inapplicable(_mapping("m-1"), receipt) is None
    assert continuity_inapplicable(_mapping("m-3"), receipt) == "flagged_by_continuity_receipt"
    assert continuity_inapplicable(_mapping("m-1", sha="0" * 64), receipt) == "continuity_approval_hash_differs"
    assert continuity_inapplicable(_mapping("m-1", rng=("2", "2")), receipt) \
        == "continuity_release_range_is_not_the_reviewed_release"
    assert continuity_inapplicable(_mapping("zz"), receipt) == "continuity_record_absent"
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt))
    compiled = {"col_xr": ("col_xr:a:b", "c" * 64), "dyntaxa": ("dyntaxa:2:2", "2" * 64)}
    assert load_continuity_receipt(path, compiled)["release_id"] == "tax-2026.10.23-02"
    with pytest.raises(CompilerError, match="current_pins"):
        load_continuity_receipt(path, {**compiled, "dyntaxa": ("dyntaxa:3:3", "3" * 64)})


def test_flagged_nortaxa_mapping_or_supersession_blocks():
    receipt = {"records": {"a": {"kind": "nortaxa_mapping", "status": "flagged"},
                           "b": {"kind": "supersession", "status": "carried"},
                           "c": {"kind": "dyntaxa_mapping", "status": "flagged"}}}
    assert continuity_blockers(receipt) == ["a"]


# ----- prepare stop conditions -----------------------------------------------

def test_acquisition_gate_stops_without_approval_or_archive(tmp_path, monkeypatch):
    monkeypatch.setattr(prepare_refresh, "REPO_ROOT", tmp_path)
    release = tmp_path / "database/taxonomy/sources/nortaxa/9.9"
    release.mkdir(parents=True)
    source = {"code": "nortaxa", "release_dir": "database/taxonomy/sources/nortaxa/9.9"}
    with pytest.raises(prepare_refresh.RefreshStop, match="no acquisition approval"):
        prepare_refresh.acquisition_gate(source, tmp_path / "arch", False, sys.executable)
    (release / "acquisition-approval.json").write_text("{}")
    with pytest.raises(prepare_refresh.RefreshStop, match="offline"):
        prepare_refresh.acquisition_gate(source, tmp_path / "arch", False, sys.executable)
    (release / "manifest.json").write_text(json.dumps({"download": {"archive_sha256": "0" * 64}}))
    archive = tmp_path / "arch" / source["release_dir"] / "archive.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"x")
    with pytest.raises(prepare_refresh.RefreshStop, match="not the manifest pin"):
        prepare_refresh.acquisition_gate(source, tmp_path / "arch", False, sys.executable)


def test_identity_comparison_separates_supersession_from_unexplained(tmp_path):
    import sqlite3

    def db(name, live, retired):
        path = tmp_path / name
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER)")
            conn.execute("CREATE TABLE taxon_supersession_min (retired_taxon_id INTEGER, current_taxon_id INTEGER)")
            conn.execute("CREATE TABLE taxonomy_meta (key TEXT, value TEXT)")
            conn.executemany("INSERT INTO taxon_min VALUES (?)", [(i,) for i in live])
            conn.executemany("INSERT INTO taxon_supersession_min VALUES (?, ?)", retired)
        return path
    result = prepare_refresh.identity_comparison(db("a", [1, 2, 3], []), db("b", [1, 4], [(2, 1)]))
    assert result["stopped_by_supersession"] == 1 and result["stopped_unexplained"] == [3]
    assert result["new_live"] == 1


HOLD = {"reason": "nortaxa_species_to_coll_concept_change", "decided_by": "owner", "decided_at": "2026-10-23"}


def test_hold_quarantines_only_the_named_flagged_record(tmp_path):
    receipt = _receipt(tmp_path, holds={"m-3": {"record_id": "m-3", **HOLD}})
    assert receipt["records"]["m-3"]["status"] == "held"
    assert receipt["records"]["m-4"]["status"] == "flagged"  # other flags are untouched


def test_stale_or_unknown_holds_are_refused(tmp_path):
    with pytest.raises(carry_forward.HoldError, match="stale"):
        _receipt(tmp_path, holds={"m-1": {"record_id": "m-1", **HOLD}})
    (tmp_path / "x").mkdir()
    with pytest.raises(carry_forward.HoldError, match="not approved"):
        _receipt(tmp_path / "x", holds={"nope": {"record_id": "nope", **HOLD}})
    path = tmp_path / "holds.json"
    path.write_text(json.dumps({"format": carry_forward.HOLDS_FORMAT, "holds": [{"record_id": "m-3"}]}))
    with pytest.raises(carry_forward.HoldError, match="reason"):
        carry_forward.load_holds(path)


def test_held_nortaxa_usage_is_withheld_but_backbone_is_not():
    from compile_release import continuity_held_usages
    receipt = {"records": {"a": {"status": "held", "kind": "supersession",
                                 "identifiers": ["col_xr/col_usage_id/C1", "nortaxa/nortaxa_taxon_id/52024"]},
                           "b": {"status": "flagged", "kind": "nortaxa_mapping",
                                 "identifiers": ["nortaxa/nortaxa_taxon_id/1"]}}}
    assert continuity_held_usages(receipt) == {("nortaxa", "nortaxa_taxon_id", "52024")}
    assert continuity_blockers(receipt) == ["b"]


# ----- per-release BOM corrections -------------------------------------------

def _bom_row():
    import hashlib
    from col_xr_corrections import BOM
    header = ("ID", "scientificName", "status", "rank", "parentID", "genus")
    raw = b"\t".join([b"VR3R9", BOM + b"Janbinmorpha", b"accepted", b"genus", b"P", BOM + b"Janbinmorpha"]) + b"\n"
    correction = {"line_number": 7, "row_taxon_id": "VR3R9", "raw_row_bytes": len(raw),
                  "raw_row_sha256": hashlib.sha256(raw).hexdigest(), "expected_bom_count": 2,
                  "bom_fields": ["scientificName", "genus"], "expected_columns": 6, "correction_id": "c1"}
    return header, raw, correction


def test_release_bom_correction_removes_only_approved_boms():
    from col_xr_corrections import apply_release_bom_correction
    header, raw, correction = _bom_row()
    applied = apply_release_bom_correction(raw, line_number=7, normalized_header=header, correction=correction,
                                           max_line_bytes=1 << 20, max_field_bytes=1 << 20)
    assert applied.record.semantic_fields[1] == "Janbinmorpha"
    assert applied.record.raw_fields[1].startswith("﻿")  # raw bytes preserved
    with pytest.raises(AcquisitionError, match="fingerprint"):
        apply_release_bom_correction(raw.replace(b"P", b"Q"), line_number=7, normalized_header=header,
                                     correction=correction, max_line_bytes=1 << 20, max_field_bytes=1 << 20)
    with pytest.raises(AcquisitionError, match="approved fields"):
        apply_release_bom_correction(raw, line_number=7, normalized_header=header,
                                     correction={**correction, "bom_fields": ["scientificName", "status"]},
                                     max_line_bytes=1 << 20, max_field_bytes=1 << 20)


def test_release_corrections_bind_archive_and_list_rows():
    from col_xr_corrections import load_release_bom_corrections
    policy = {"correction_policy_schema_version": 2, "approval_status": "approved", "approved_by": "owner",
              "archive_sha256": "a" * 64, "dataset_key": 1, "release_label": "x", "member_sha256": "m",
              "corrections": [{"line_number": 7}]}
    assert set(load_release_bom_corrections(policy, archive_sha256="a" * 64, dataset_key=1, release_label="x")) == {7}
    with pytest.raises(AcquisitionError, match="archive_sha256"):
        load_release_bom_corrections(policy, archive_sha256="b" * 64, dataset_key=1, release_label="x")


def test_usage_holds_are_listed_in_receipt(tmp_path):
    usage = "nortaxa/nortaxa_taxon_id/128204"
    receipt = _receipt(tmp_path, holds={usage: {"source_usage": usage, **HOLD}})
    assert list(receipt["held_alias_conflicts"]) == [usage]
    path = tmp_path / "holds.json"
    path.write_text(json.dumps({"format": carry_forward.HOLDS_FORMAT,
                                "holds": [{"record_id": "m-3", "source_usage": usage, **HOLD}]}))
    with pytest.raises(carry_forward.HoldError, match="both"):
        carry_forward.load_holds(path)
