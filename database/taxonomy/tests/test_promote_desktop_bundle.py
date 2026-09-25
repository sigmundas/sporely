"""promote_desktop_bundle.py: bundle manifest from authoritative build data."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "database/taxonomy/scripts"))

import promote_desktop_bundle as promote  # noqa: E402

RELEASE = "tax-2099.01.01-01"
REGISTRY_SHA = "ab" * 32
WORKBOOK_BYTES = b"synthetic red-list workbook"
WORKBOOK_SHA = hashlib.sha256(WORKBOOK_BYTES).hexdigest()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def build(tmp_path: Path) -> dict:
    """A minimal but internally consistent compiler output + artifact."""
    release_dir = tmp_path / "release"
    release_dir.mkdir()
    compiler_manifest = {
        "content_release_id": RELEASE,
        "registry_sha256": REGISTRY_SHA,
        "redlist_no": {"source_binding": {
            "input_sha256": WORKBOOK_SHA, "source_release": "2021",
            "source_system": "artsdatabanken_redlist",
        }},
    }
    (release_dir / "manifest.json").write_text(json.dumps(compiler_manifest), encoding="utf-8")
    (release_dir / "redlist_no_diagnostics.json").write_text(json.dumps({
        "counts": {"total": 3, "resolved": 1, "unresolved": 2},
        "collisions": {"summary": {"unique": 1, "conflicting_categories": 0}},
    }), encoding="utf-8")
    (release_dir / "redlist_no.jsonl").write_text("".join(json.dumps(r) + "\n" for r in (
        {"unresolved_reason": None},
        {"unresolved_reason": "unresolved_name_id_not_found"},
        {"unresolved_reason": "unresolved_name_id_name_mismatch"},
    )), encoding="utf-8")

    sqlite_path = tmp_path / "artifact.sqlite3"
    conn = sqlite3.connect(sqlite_path)
    conn.execute("CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"), ("content_release_id", RELEASE),
        ("compiler_manifest_sha256", _sha(release_dir / "manifest.json")),
        ("registry_sha256", REGISTRY_SHA), ("state", "candidate"), ("publication", "none"),
    ])
    conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    workbook = tmp_path / "redlist.xlsx"
    workbook.write_bytes(WORKBOOK_BYTES)
    report = tmp_path / "redlist_report.json"
    report.write_text(json.dumps({
        "input_sha256": WORKBOOK_SHA, "input_filename": "redlist.xlsx", "row_count": 3,
        "source_release": "2021", "source_system": "artsdatabanken_redlist",
        "source_url": "https://example.invalid/redlist", "sheet_name": "Vurderinger",
    }), encoding="utf-8")
    registry_manifest = tmp_path / "registry-manifest.json"
    registry_manifest.write_text(json.dumps({"concatenated_sha256": REGISTRY_SHA}), encoding="utf-8")

    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "tax-old.sqlite3.gz").write_bytes(b"old")
    (bundle / "manifest.json").write_text(json.dumps({
        "content_release_id": "tax-old", "gz_artifact": "tax-old.sqlite3.gz",
        "install_target_name": "vernacular_multilanguage_v2.sqlite3",
        "redlist_no": {
            "source_system": "artsdatabanken_redlist", "source_release": "2021",
            "citation": "Curated citation", "license": {"status": "pending"},
            "workbook_sha256": WORKBOOK_SHA, "resolved_count": 999,
        },
    }), encoding="utf-8")
    compat = tmp_path / "compat.json"
    compat.write_text(json.dumps({
        "tested_taxonomy_release": "tax-old",
        "redlist_no": {"citation_evidence": "compat prose", "resolved_count": 999},
    }), encoding="utf-8")
    return dict(sqlite_path=sqlite_path, release_dir=release_dir, redlist_report_path=report,
                redlist_workbook=workbook, bundle_dir=bundle,
                registry_manifest_path=registry_manifest, compatibility_path=compat)


def test_promotion_writes_a_loadable_deterministic_bundle(build, tmp_path):
    manifest = promote.promote(**build)
    bundle = build["bundle_dir"]
    gz = bundle / f"{RELEASE}.sqlite3.gz"
    assert manifest["content_release_id"] == RELEASE
    assert manifest["gz_artifact"] == gz.name
    assert manifest["gz_sha256"] == _sha(gz)
    assert manifest["sqlite_sha256"] == _sha(build["sqlite_path"])
    assert manifest["registry_concatenated_sha256"] == REGISTRY_SHA
    assert manifest["compiler_manifest_sha256"] == _sha(build["release_dir"] / "manifest.json")
    assert manifest["install_target_name"] == "vernacular_multilanguage_v2.sqlite3"
    assert not (bundle / "tax-old.sqlite3.gz").exists(), "superseded bundle gzip removed"
    assert json.loads((bundle / "manifest.json").read_text(encoding="utf-8")) == manifest

    red = manifest["redlist_no"]
    assert red["resolved_count"] == 1 and red["unresolved_count"] == 2, "counts recomputed, not carried"
    assert red["unresolved_reason_counts"] == {
        "name_id_not_found_in_nortaxa": 1, "name_id_name_mismatch": 1, "name_id_ambiguous": 0}
    assert red["citation"] == "Curated citation" and red["license"] == {"status": "pending"}
    assert red["workbook_bytes"] == len(WORKBOOK_BYTES)

    compat = json.loads(build["compatibility_path"].read_text(encoding="utf-8"))
    assert compat["tested_taxonomy_release"] == RELEASE
    assert compat["bundled_gz_sha256"] == manifest["gz_sha256"]
    assert compat["redlist_no"] == {"citation_evidence": "compat prose", "resolved_count": 1}

    # The runtime loader and installer accept the result as-is.
    from utils import taxonomy_v2
    loaded = taxonomy_v2.load_manifest(bundle / "manifest.json")
    installed = taxonomy_v2.ensure_installed(app_data_dir=tmp_path / "appdata", manifest=loaded,
                                             gz_path=gz, force_verify=True)
    assert _sha(installed) == manifest["sqlite_sha256"]

    # Same inputs → same bytes.
    first = gz.read_bytes()
    (bundle / "manifest.json").write_text(json.dumps({**manifest}), encoding="utf-8")
    promote.promote(**build)
    assert gz.read_bytes() == first


@pytest.mark.parametrize("breakage, message", [
    ("compiler_manifest", "not built from this compiler manifest"),
    ("registry", "committed registry"),
    ("workbook", "not the workbook this build consumed"),
    ("previous_workbook", "curated"),
    ("previous_gz_is_manifest", "invalid gzip artifact suffix"),
    ("previous_gz_traversal", "invalid gzip artifact name"),
    ("release_id_traversal", "is not tax-YYYY.MM.DD-NN"),
])
def test_promotion_refuses_unproven_inputs(build, breakage, message):
    if breakage == "compiler_manifest":
        (build["release_dir"] / "manifest.json").write_text(
            json.dumps({"content_release_id": RELEASE, "edited": True}), encoding="utf-8")
    elif breakage == "registry":
        build["registry_manifest_path"].write_text(json.dumps({"concatenated_sha256": "cd" * 32}))
    elif breakage == "workbook":
        build["redlist_workbook"].write_bytes(b"a different workbook")
    elif breakage == "previous_workbook":
        path = build["bundle_dir"] / "manifest.json"
        previous = json.loads(path.read_text(encoding="utf-8"))
        previous["redlist_no"]["workbook_sha256"] = "00" * 32
        path.write_text(json.dumps(previous), encoding="utf-8")
    elif breakage in ("previous_gz_is_manifest", "previous_gz_traversal"):
        path = build["bundle_dir"] / "manifest.json"
        previous = json.loads(path.read_text(encoding="utf-8"))
        previous["gz_artifact"] = "manifest.json" if breakage == "previous_gz_is_manifest" else "..\\evil.sqlite3.gz"
        path.write_text(json.dumps(previous), encoding="utf-8")
    elif breakage == "release_id_traversal":
        conn = sqlite3.connect(build["sqlite_path"])
        conn.execute("UPDATE taxonomy_meta SET value = '../escape' WHERE key = 'content_release_id'")
        conn.commit()
        conn.close()
    before = {p.name: p.read_bytes() for p in build["bundle_dir"].iterdir()}
    with pytest.raises(promote.PromotionError, match=message):
        promote.promote(**build)
    assert {p.name: p.read_bytes() for p in build["bundle_dir"].iterdir()} == before, \
        "a refused promotion writes nothing"
