"""Freezing and promoting a release derived from a frozen base (name associations)."""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import sqlite3
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "database/taxonomy/scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

import name_associations as na  # noqa: E402
import promote_desktop_bundle as promote  # noqa: E402
from build_sqlite_candidate import CREATE_SCHEMA_SQL  # noqa: E402

BASE = "tax-2099.01.01-01"
DERIVED = "tax-2099.01.02-01"
REGISTRY_SHA = "ab" * 32
COMPILER_SHA = "cd" * 32
POLICY = {"format": na.POLICY_FORMAT, "policy_version": "test", "exclusions": []}
# taxon_id, name, canonical source, canonical external id
CONCEPTS = [
    (10, "Russula aeruginea", "col_xr", "C10"),
    (20, "Russula aeruginea", "nortaxa", "500"),
    (11, "Calocybe carnea", "col_xr", "C11"),
    (21, "Rugosomyces carneus", "nortaxa", "501"),
    (12, "Flammulina fennae", "col_xr", "C12"),
]
NORTAXA = [("500", "Russula aeruginea", "valid", None), ("501", "Rugosomyces carneus", "valid", None),
           ("601", "Calocybe carnea", "synonym", "501"), ("502", "Flammulina fennae", "valid", None)]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, data) -> Path:
    path.write_text(json.dumps(data, indent=1), encoding="utf-8")
    return path


def _base_sqlite(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(CREATE_SCHEMA_SQL)
    for tid, name, css, ext in CONCEPTS:
        genus, _, epithet = name.partition(" ")
        conn.execute(
            "INSERT INTO taxon_min (taxon_id, genus, specific_epithet, canonical_scientific_name, taxon_rank, "
            "taxonomic_status, source_system, canonical_source_system, canonical_external_id, norwegian_taxon_id, "
            "sporely_content_release_id) VALUES (?,?,?,?, 'species', ?,?,?,?,?,?)",
            (tid, genus, epithet, name, "valid" if css == "nortaxa" else "accepted", css, css, ext,
             int(ext) if css == "nortaxa" else None, BASE))
        conn.execute("INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, "
                     "is_preferred_name, source) VALUES (?, 'sci', ?, 1, ?)", (tid, name, css))
    conn.execute("INSERT INTO taxon_redlist_min (taxon_id, source_system, source_release, assessment_id, "
                 "assessment_area, assessed_name_source, assessed_name_namespace, assessed_name_id, "
                 "scientific_name_snapshot, category_raw, category_code) VALUES "
                 "(20, 'artsdatabanken_redlist', '2021', 'A1', 'Norge', 'artsdatabanken', "
                 "'artsnavnebase_scientific_name_id', '500', 'Russula aeruginea', 'LC', 'LC'), "
                 "(NULL, 'artsdatabanken_redlist', '2021', 'A2', 'Norge', 'artsdatabanken', "
                 "'artsnavnebase_scientific_name_id', '999', 'Nomen nudum', 'NT', 'NT'), "
                 "(12, 'artsdatabanken_redlist', '2021', 'A3', 'Norge', 'artsdatabanken', "
                 "'artsnavnebase_scientific_name_id', '502', 'Flammulina fennae', 'VU', 'VU')")
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("content_release_id", BASE), ("taxonomy_schema_version", "2"), ("registry_sha256", REGISTRY_SHA),
        ("compiler_manifest_sha256", COMPILER_SHA), ("state", "candidate"), ("publication", "none")])
    conn.commit()
    conn.execute("VACUUM")
    conn.close()


@pytest.fixture
def setup(tmp_path: Path) -> dict:
    """A bundle that publishes the frozen base, its derivation inputs and a validated derivation."""
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    base_sqlite = tmp_path / f"{BASE}.sqlite3"
    _base_sqlite(base_sqlite)
    base_gz = bundle / f"{BASE}.sqlite3.gz"
    promote.write_deterministic_gzip(base_sqlite, base_gz, member_name=base_sqlite.name)
    (bundle / f"{BASE}.evidence.tar.gz").write_bytes(b"base evidence")
    _write_json(bundle / "manifest.json", {
        "manifest_schema_version": 1, "taxonomy_schema_version": 2, "content_release_id": BASE,
        "state": "candidate", "publication": "none", "gz_artifact": base_gz.name, "gz_sha256": _sha(base_gz),
        "gz_bytes": base_gz.stat().st_size, "sqlite_sha256": _sha(base_sqlite),
        "sqlite_bytes": base_sqlite.stat().st_size, "registry_concatenated_sha256": REGISTRY_SHA,
        "compiler_manifest_sha256": COMPILER_SHA, "freeze_artifact": f"{BASE}.freeze.json",
        "install_target_name": "vernacular_multilanguage_v2.sqlite3",
        "redlist_no": {"source_release": "2021", "citation": "Curated", "row_count": 3,
                       "resolved_count": 2, "unresolved_count": 1,
                       "collision_summary": {"unique": 2, "multiple_rows_same_category": 0,
                                             "conflicting_categories": 0, "conflicting_ranks": 0}},
        "compiler_evidence": {"artifact": f"{BASE}.evidence.tar.gz",
                              "sha256": _sha(bundle / f"{BASE}.evidence.tar.gz"), "bytes": 13,
                              "format": promote.COMPILER_EVIDENCE_FORMAT},
    })
    compat = _write_json(tmp_path / "compat.json", {"tested_taxonomy_release": BASE,
                                                    "redlist_no": {"citation_evidence": "prose"}})
    _write_json(bundle / f"{BASE}.freeze.json", {
        "format": "sporely-frozen-publication-v1", "content_release_id": BASE,
        "files": {name: {"sha256": _sha(path), "bytes": path.stat().st_size}
                  for name, path in (("manifest.json", bundle / "manifest.json"), ("compatibility.json", compat),
                                     (base_gz.name, base_gz),
                                     (f"{BASE}.evidence.tar.gz", bundle / f"{BASE}.evidence.tar.gz"))},
        "baseline_manifest_sha256": "00" * 32, "baseline_compatibility_sha256": None})
    registry = _write_json(tmp_path / "registry-manifest.json", {"concatenated_sha256": REGISTRY_SHA})

    norm = tmp_path / "norm"
    for source in ("nortaxa", "dyntaxa", "col_xr"):
        (norm / source).mkdir(parents=True)
        rows = NORTAXA if source == "nortaxa" else []
        (norm / source / "taxa.jsonl").write_text("".join(json.dumps({
            "taxon_id": {"value": i}, "core_row_id": {"value": i}, "scientific_name": name,
            "authorship": "A.", "rank": "species", "taxonomic_status": status, "source_code": source,
            "accepted_name_usage_id": {"value": accepted} if accepted else None,
            "classification": {"kingdom": "Fungi", "family": ""}}) + "\n" for i, name, status, accepted in rows))
    policy = _write_json(tmp_path / "policy.json", POLICY)
    holds = _write_json(tmp_path / "holds.json", {"holds": []})

    derivation = tmp_path / "derivation"
    report = na.build(base_gz=base_gz, base_sqlite_sha256=_sha(base_sqlite), norm_dir=norm, policy_path=policy,
                      holds_path=holds, release_id=DERIVED, out_dir=derivation,
                      base_manifest=bundle / "manifest.json")
    receipt = _write_json(tmp_path / "receipt.json", {
        "validated": True, "sqlite_sha256": report["sqlite_sha256"],
        "association_report_sha256": _sha(derivation / "association-report.json"),
        "expected_associations_total": report["associations_total"]})
    return dict(derivation_dir=derivation, norm_dir=norm, policy_path=policy, holds_path=holds,
                bundle_dir=bundle, registry_manifest_path=registry, compatibility_path=compat,
                validation_receipt_path=receipt)


def _freeze(setup, output: Path) -> dict:
    return promote.freeze_derived(**setup, output_dir=output)


def _decision(setup, tmp_path: Path) -> Path:
    report = json.loads((setup["derivation_dir"] / "association-report.json").read_text())
    return _write_json(tmp_path / "decision.json", {
        "format": "sporely-taxonomy-release-decision-v1", "decision": "release_approved",
        "content_release_id": DERIVED, "sqlite_sha256": report["sqlite_sha256"],
        "approved_by": "test", "approved_at": "2099-01-02", "decision_reference": "test"})


def _promote(setup, frozen: Path, decision: Path | None) -> dict:
    return promote.promote(frozen_dir=frozen, expected_freeze_sha256=_sha(frozen / "freeze.json"),
                           bundle_dir=setup["bundle_dir"], registry_manifest_path=setup["registry_manifest_path"],
                           compatibility_path=setup["compatibility_path"], release_decision_path=decision)


def test_derived_release_freezes_deterministically_and_promotes_the_frozen_bytes(setup, tmp_path):
    report = json.loads((setup["derivation_dir"] / "association-report.json").read_text())
    assert report["associations_total"] > 0
    first = _freeze(setup, tmp_path / "frozen-a")
    second = _freeze(setup, tmp_path / "frozen-b")
    assert _sha(tmp_path / "frozen-a" / "freeze.json") == _sha(tmp_path / "frozen-b" / "freeze.json")
    frozen_dir = tmp_path / "frozen-a"
    base_freeze_sha = _sha(setup["bundle_dir"] / f"{BASE}.freeze.json")
    assert first["derived_from"] == {"content_release_id": BASE, "freeze_sha256": base_freeze_sha} == second["derived_from"]
    # The frozen gzip is the validated derivation's, byte for byte.
    assert _sha(frozen_dir / f"{DERIVED}.sqlite3.gz") == report["gz_sha256"] == \
        _sha(setup["derivation_dir"] / f"{DERIVED}.sqlite3.gz")
    manifest = json.loads((frozen_dir / "manifest.json").read_text())
    assert "compiler_evidence" not in manifest
    assert manifest["derivation_evidence"]["format"] == promote.DERIVED_EVIDENCE_FORMAT
    assert manifest["compiler_manifest_sha256"] == COMPILER_SHA
    block = manifest["name_association"]
    assert block["base_freeze_sha256"] == base_freeze_sha
    assert block["implementation_sha256"] == {n: _sha(SCRIPTS / n) for n in promote.DERIVATION_IMPLEMENTATION}
    with tarfile.open(frozen_dir / manifest["derivation_evidence"]["artifact"]) as archive:
        names = set(archive.getnames())
        inventory = json.load(archive.extractfile("inventory.json"))
    assert {"derivation/association-report.json", "derivation/associations.jsonl", "inputs/base-freeze.json",
            "inputs/name-association-policy.json", "inputs/source-continuity-holds.json",
            "validation/receipt.json", "implementation/name_associations.py"} <= names
    assert inventory["inputs_sha256"] == report["inputs_sha256"]

    # A candidate-only derivation is promoted only with its release decision.
    with pytest.raises(promote.PromotionError, match="candidate_build_only"):
        _promote(setup, frozen_dir, None)
    published = _promote(setup, frozen_dir, _decision(setup, tmp_path))
    bundle = setup["bundle_dir"]
    assert published["content_release_id"] == DERIVED
    for name in ("manifest.json", f"{DERIVED}.sqlite3.gz", f"{DERIVED}.evidence.tar.gz"):
        assert _sha(bundle / name) == _sha(frozen_dir / name)
    assert _sha(bundle / f"{DERIVED}.freeze.json") == _sha(frozen_dir / "freeze.json")
    assert not (bundle / f"{BASE}.sqlite3.gz").exists()
    # The base's freeze descriptor and evidence stay available.
    assert (bundle / f"{BASE}.freeze.json").is_file() and (bundle / f"{BASE}.evidence.tar.gz").is_file()


@pytest.mark.parametrize("breakage, message", [
    (lambda s: (s["bundle_dir"] / f"{BASE}.sqlite3.gz").unlink(), "base artifact missing or altered"),
    (lambda s: (s["bundle_dir"] / f"{BASE}.sqlite3.gz").write_bytes(b"tampered"), "base artifact missing or altered"),
    (lambda s: (s["bundle_dir"] / f"{BASE}.freeze.json").unlink(), "base freeze descriptor missing"),
    (lambda s: (s["bundle_dir"] / "manifest.json").write_text("{}"), "not the frozen base's"),
    (lambda s: (s["norm_dir"] / "nortaxa" / "taxa.jsonl").write_text(""), "derivation input missing or changed"),
    (lambda s: (s["norm_dir"] / "col_xr" / "taxa.jsonl").unlink(), "derivation input missing or changed"),
    (lambda s: _write_json(s["policy_path"], dict(POLICY, policy_version="other")), "policy is not the one"),
    (lambda s: (s["derivation_dir"] / f"{DERIVED}.sqlite3").write_bytes(b"x"), "does not bind its artifacts"),
    (lambda s: (s["derivation_dir"] / "extra.json").write_text("{}"), "inventory mismatch"),
    (lambda s: _write_json(s["validation_receipt_path"], {
        **json.loads(s["validation_receipt_path"].read_text()), "sqlite_sha256": "00" * 32}),
     "receipt does not bind"),
    (lambda s: _write_json(s["validation_receipt_path"], {
        **json.loads(s["validation_receipt_path"].read_text()), "expected_associations_total": 1}),
     "pinned association count disagrees"),
    (lambda s: _write_json(s["validation_receipt_path"], {
        **json.loads(s["validation_receipt_path"].read_text()), "validated": False}), "does not approve"),
    # The holds changed after validation: the reconstruction differs.
    (lambda s: _write_json(s["holds_path"], {"holds": [{"source_usage": "nortaxa/nortaxa_taxon_id/501"}]}),
     "reconstruction"),
])
def test_derived_freeze_refuses_unproven_inputs(setup, tmp_path, breakage, message):
    breakage(setup)
    with pytest.raises(promote.PromotionError, match=message):
        _freeze(setup, tmp_path / "frozen")
    assert not (tmp_path / "frozen").exists()
    assert not list(tmp_path.glob(".freeze-*")) and not list(tmp_path.glob(".derive-*"))


def test_tampered_association_list_fails_reconstruction(setup, tmp_path):
    path = setup["derivation_dir"] / "associations.jsonl"
    path.write_text("".join(path.read_text().splitlines(keepends=True)[1:]))
    with pytest.raises(promote.PromotionError, match="not byte-identical: associations.jsonl"):
        _freeze(setup, tmp_path / "frozen")


_ADD_ID_99 = ("CREATE TEMP TABLE t AS SELECT * FROM taxon_min WHERE taxon_id = 12; "
              "UPDATE t SET taxon_id = 99; INSERT INTO taxon_min SELECT * FROM t;")


def _copy_with_base(setup, tmp_path, mutate, mutate_base=None) -> Path:
    base_sqlite = tmp_path / "base-copy.sqlite3"
    with gzip.open(setup["bundle_dir"] / f"{BASE}.sqlite3.gz", "rb") as src:
        base_sqlite.write_bytes(src.read())
    derived = tmp_path / "derived-copy.sqlite3"
    shutil.copyfile(setup["derivation_dir"] / f"{DERIVED}.sqlite3", derived)
    for path, change in ((derived, mutate), (base_sqlite, mutate_base)):
        if change is not None:
            with sqlite3.connect(path) as conn:
                change(conn)
    return base_sqlite, derived


def test_a_base_id_missing_from_the_derived_release_is_refused(setup, tmp_path):
    base_sqlite, derived = _copy_with_base(setup, tmp_path, None, lambda c: c.executescript(_ADD_ID_99))
    with pytest.raises(promote.PromotionError, match="exactly the base release's Sporely ids"):
        promote._verify_derived_artifact(
            derived, base_sqlite, report=json.loads((setup["derivation_dir"] / "association-report.json").read_text()),
            receipt=json.loads(setup["validation_receipt_path"].read_text()),
            previous=json.loads((setup["bundle_dir"] / "manifest.json").read_text()),
            registry_sha=REGISTRY_SHA, associations_path=setup["derivation_dir"] / "associations.jsonl")


@pytest.mark.parametrize("mutate, message", [
    (lambda c: c.execute("DELETE FROM taxon_min WHERE taxon_id = 12"), "foreign-key check failed"),
    (lambda c: c.executescript(_ADD_ID_99),
     "exactly the base release's Sporely ids"),
    (lambda c: c.execute("UPDATE taxon_min SET canonical_external_id = 'C99' WHERE taxon_id = 11"),
     "canonical identity"),
    (lambda c: c.execute("DELETE FROM taxon_association_min WHERE rowid = (SELECT min(rowid) FROM taxon_association_min)"),
     "association table, list and report disagree"),
    (lambda c: c.execute("UPDATE taxon_redlist_min SET taxon_id = 12 WHERE assessment_id = 'A2'"),
     "red-list resolution"),
    # Same counts, but two assessments now fall on one concept with conflicting categories.
    (lambda c: c.execute("UPDATE taxon_redlist_min SET taxon_id = "
                         "(SELECT taxon_id FROM taxon_redlist_min WHERE assessment_id = 'A1') WHERE assessment_id = 'A3'"),
     "red-list resolution"),
    # Same counts, but a different row is the unresolved one.
    (lambda c: c.executescript("UPDATE taxon_redlist_min SET taxon_id = 11 WHERE assessment_id = 'A2'; "
                               "UPDATE taxon_redlist_min SET taxon_id = NULL WHERE assessment_id = 'A3';"),
     "red-list resolution"),
])
def test_derived_artifact_checks_ids_associations_and_redlist(setup, tmp_path, mutate, message):
    base_sqlite, derived = _copy_with_base(setup, tmp_path, mutate)
    report = json.loads((setup["derivation_dir"] / "association-report.json").read_text())
    with pytest.raises(promote.PromotionError, match=message):
        promote._verify_derived_artifact(
            derived, base_sqlite, report=report,
            receipt=json.loads(setup["validation_receipt_path"].read_text()),
            previous=json.loads((setup["bundle_dir"] / "manifest.json").read_text()),
            registry_sha=REGISTRY_SHA, associations_path=setup["derivation_dir"] / "associations.jsonl")


def _refingerprint(frozen: Path) -> None:
    descriptor = json.loads((frozen / "freeze.json").read_text())
    for name in descriptor["files"]:
        descriptor["files"][name] = {"sha256": _sha(frozen / name), "bytes": (frozen / name).stat().st_size}
    _write_json(frozen / "freeze.json", descriptor)


def _rewrite_archive(frozen: Path, edit) -> None:
    """Rewrite the evidence archive consistently (inventory, manifest, freeze) after ``edit``."""
    manifest = json.loads((frozen / "manifest.json").read_text())
    path = frozen / manifest["derivation_evidence"]["artifact"]
    with tarfile.open(path) as archive:
        files = {m.name: archive.extractfile(m).read() for m in archive.getmembers()}
    inventory = json.loads(files.pop("inventory.json"))
    edit(files, inventory)
    inventory["members"] = {n: {"sha256": hashlib.sha256(b).hexdigest(), "bytes": len(b)} for n, b in sorted(files.items())}
    files["inventory.json"] = json.dumps(inventory).encode()
    with tarfile.open(path, "w:gz") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name); info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    manifest["derivation_evidence"]["sha256"] = _sha(path)
    _write_json(frozen / "manifest.json", manifest)
    _refingerprint(frozen)


def _edit_report(files, inventory, **changes):
    report = json.loads(files["derivation/association-report.json"])
    report.update(changes)
    files["derivation/association-report.json"] = json.dumps(report).encode()


@pytest.mark.parametrize("edit, message", [
    (lambda f, i: f.update({"implementation/name_associations.py": b"# other"}), "overlay implementation"),
    (lambda f, i: f.update({"inputs/base-freeze.json": b"{}"}), "does not bind the frozen base"),
    (lambda f, i: f.update({"inputs/previous-publication-manifest.json": b'{"sqlite_sha256": "x"}'}),
     "does not chain back to the frozen base"),
    (lambda f, i: f.update({"inputs/source-continuity-holds.json": b'{"holds": []}'}), "does not bind its holds"),
    (lambda f, i: f.update({"derivation/associations.jsonl": b""}), "association list is incomplete"),
    (lambda f, i: _edit_report(f, i, sqlite_sha256="00" * 32), "archived derivation report mismatch"),
    (lambda f, i: f.update({"validation/receipt.json": b'{"validated": false}'}), "archived validation receipt"),
])
def test_tampered_derived_evidence_is_refused(setup, tmp_path, edit, message):
    frozen = tmp_path / "frozen"
    _freeze(setup, frozen)
    _rewrite_archive(frozen, edit)
    with pytest.raises(promote.PromotionError, match=message):
        promote.verify_frozen(frozen)


def test_derived_frozen_set_needs_its_expected_descriptor(setup, tmp_path):
    frozen = tmp_path / "frozen"
    _freeze(setup, frozen)
    expected = _sha(frozen / "freeze.json")
    manifest = json.loads((frozen / "manifest.json").read_text())
    manifest["name_association"]["associations_total"] += 1
    _write_json(frozen / "manifest.json", manifest)
    with pytest.raises(promote.PromotionError, match="fingerprint mismatch"):
        promote.promote(frozen_dir=frozen, expected_freeze_sha256=expected, bundle_dir=setup["bundle_dir"],
                        registry_manifest_path=setup["registry_manifest_path"],
                        compatibility_path=setup["compatibility_path"], release_decision_path=_decision(setup, tmp_path))
    assert not (setup["bundle_dir"] / f"{DERIVED}.sqlite3.gz").exists()


def test_a_manifest_naming_both_evidence_kinds_is_refused(setup, tmp_path):
    frozen = tmp_path / "frozen"
    _freeze(setup, frozen)
    manifest = json.loads((frozen / "manifest.json").read_text())
    manifest["compiler_evidence"] = dict(manifest["derivation_evidence"], format=promote.COMPILER_EVIDENCE_FORMAT)
    _write_json(frozen / "manifest.json", manifest)
    _refingerprint(frozen)
    with pytest.raises(promote.PromotionError, match="exactly one evidence archive"):
        promote.verify_frozen(frozen)


def test_a_derived_archive_cannot_pass_as_compiler_evidence(setup, tmp_path):
    frozen = tmp_path / "frozen"
    _freeze(setup, frozen)
    manifest = json.loads((frozen / "manifest.json").read_text())
    manifest["compiler_evidence"] = dict(manifest.pop("derivation_evidence"), format=promote.COMPILER_EVIDENCE_FORMAT)
    _write_json(frozen / "manifest.json", manifest)
    _refingerprint(frozen)
    with pytest.raises((promote.PromotionError, KeyError)):
        promote.verify_frozen(frozen)


def test_cli_freezes_a_derived_release_and_rejects_mixed_arguments(setup, tmp_path):
    common = ["--bundle-dir", setup["bundle_dir"], "--registry-manifest", setup["registry_manifest_path"],
              "--validation-receipt", setup["validation_receipt_path"], "--compatibility", setup["compatibility_path"]]
    derived = ["--derivation-dir", setup["derivation_dir"], "--norm-dir", setup["norm_dir"],
               "--policy", setup["policy_path"], "--holds", setup["holds_path"]]
    run = lambda *args: subprocess.run([sys.executable, str(SCRIPTS / "freeze_release.py"), *map(str, args)],
                                       capture_output=True, text=True)
    mixed = run(*common, *derived, "--sqlite", tmp_path / "x.sqlite3", "--output", tmp_path / "mixed")
    assert mixed.returncode == 2 and "--sqlite" in mixed.stderr
    ok = run(*common, *derived, "--output", tmp_path / "frozen")
    assert ok.returncode == 0, ok.stderr
    assert json.loads(ok.stdout)["derived_from"]["content_release_id"] == BASE
