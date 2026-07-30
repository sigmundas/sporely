"""Focused Stage 3B.1 tests: legacy compatibility enrichment.

Small synthetic fixtures exercise the extraction / compile / build pipeline.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import compile_release  # noqa: E402
from export_legacy_enrichment import export as export_legacy  # noqa: E402


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


def _build_synthetic_bundled_db(path: Path) -> None:
    """Emulate the pre-Stage-3A `vernacular_multilanguage.sqlite3` shape."""
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0,
            source TEXT
        );
        CREATE TABLE taxon_external_id_min (
            external_id_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            source_system TEXT NOT NULL,
            external_id INTEGER NOT NULL,
            id_role TEXT NOT NULL,
            is_preferred INTEGER NOT NULL DEFAULT 0,
            external_name TEXT,
            note TEXT
        );
        """
    )
    # NorTaxa 300190 is Candolleomyces candolleanus (accepted). Represent it
    # with an Artportalen mapping + French/German vernaculars in the bundled
    # DB. Also add a "ghost" taxon 999999 with an Artportalen id but no
    # corresponding NorTaxa row in Stage 3A's Fungi scope — should surface
    # in legacy_enrichment_skips.
    conn.executemany(
        "INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?)",
        [
            (300190, "Candolleomyces", "candolleanus", "Psathyrellaceae",
             "Candolleomyces candolleanus"),
            (999999, "Unknown", "concept", "", "Unknown concept"),
        ],
    )
    conn.executemany(
        "INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, "
        "is_preferred_name, source) VALUES (?, ?, ?, ?, ?)",
        [
            (300190, "fr", "psathyrelle de Candolle", 1, "inat_csv"),
            (300190, "de", "Halbkugeliger Träuschling", 1, "inat_csv"),
            (300190, "no", "hvit sprøsopp", 1, "artsdatabanken"),  # already stage 3A
            (999999, "fr", "unknown-fr", 0, "inat_csv"),
        ],
    )
    conn.executemany(
        "INSERT INTO taxon_external_id_min "
        "(taxon_id, source_system, external_id, id_role, is_preferred, "
        "external_name, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (300190, "artportalen", 222138, "accepted", 1, "Sprosopp", ""),
            (999999, "artportalen", 111111, "accepted", 1, "Ghost", ""),
        ],
    )
    conn.commit()
    conn.close()


def _write_synthetic_release(tmp_path: Path) -> tuple[Path, Path]:
    """Compile a synthetic release with COL + NorTaxa 300190 → known Sporely id."""
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)
    def _write_source(root, source_code, taxa, vernacular=None):
        root.mkdir(parents=True, exist_ok=True)
        if source_code == "col_xr":
            ns = {k: "col_usage_id" for k in
                  ("core_row_id", "taxon_id",
                   "accepted_name_usage_id", "parent_name_usage_id")}
        else:
            ns = {
                "core_row_id": f"{source_code}_dwc_id",
                "taxon_id": f"{source_code}_taxon_id",
                "accepted_name_usage_id": f"{source_code}_accepted_name_usage_id",
                "parent_name_usage_id": f"{source_code}_parent_name_usage_id",
            }
        release = {"version": "v", "issued_date": "d"}
        with (root / "taxa.jsonl").open("w") as h:
            for row in taxa:
                rec = {
                    "source_code": source_code, "source_release": release,
                    "core_row_id": {"value": row["core_row_id"],
                                    "namespace": ns["core_row_id"]},
                    "taxon_id": {"value": row["taxon_id"],
                                 "namespace": ns["taxon_id"]},
                    "accepted_name_usage_id": None,
                    "parent_name_usage_id": None,
                    "parent_reference_resolution": "absent",
                    "identifier_namespace": source_code,
                    "scientific_name": row["scientific_name"],
                    "authorship": row.get("authorship", ""),
                    "rank": row.get("rank", "species"),
                    "taxonomic_status": row.get("status", "accepted"),
                    "external_ids": {},
                    "classification": {
                        "kingdom": "Fungi", "phylum": "", "class": "",
                        "order": "",
                        "family": row.get("family", ""),
                        "genus": row.get("genus", ""),
                        "specific_epithet": row.get("specific_epithet", ""),
                        "infraspecific_epithet": "",
                    },
                    "provenance": {"member": "taxa.tsv", "row_index": 0,
                                   "source_code": source_code,
                                   "source_release": release,
                                   "identifier_namespace": source_code},
                }
                h.write(json.dumps(rec, sort_keys=True) + "\n")
        if vernacular:
            with (root / "vernacular.jsonl").open("w") as h:
                for v in vernacular:
                    rec = {
                        "source_code": source_code, "source_release": release,
                        "core_row_id": {"value": v["core_row_id"],
                                        "namespace": ns["core_row_id"]},
                        "language": v["language"],
                        "vernacular_name": v["name"],
                        "is_preferred": v.get("preferred", False),
                        "provenance": {"member": "vern.tsv", "row_index": 0,
                                       "source_code": source_code,
                                       "source_release": release,
                                       "identifier_namespace": source_code},
                    }
                    h.write(json.dumps(rec, sort_keys=True) + "\n")
        (root / "report.json").write_text(json.dumps({
            "result": "passed",
            "profile_source_code": source_code,
            "profile_source_release": release,
            "record_counts": {"Taxon": len(taxa),
                              "VernacularName": len(vernacular or [])},
            "outputs": {"taxa": "taxa.jsonl"},
            "distribution_imported": False,
            "identifier_namespaces": ns,
            "archive_sha256": "00" * 32,
            "reference_gaps": {"orphan_parent_reference_count": 0,
                               "orphan_accepted_reference_count": 0,
                               "orphan_parent_reference_samples": [],
                               "orphan_accepted_reference_samples": [],
                               "sample_bound": 25},
            "hierarchy_complete": True, "compiler_ready": True,
        }, sort_keys=True) + "\n")
        return root
    col = _write_source(src / "col_xr", "col_xr", taxa=[
        {"core_row_id": "COL-A", "taxon_id": "COL-A",
         "scientific_name": "Candolleomyces candolleanus",
         "authorship": "(Fr.) D. Wächt. & A. Melzer",
         "genus": "Candolleomyces", "specific_epithet": "candolleanus"},
    ])
    nor = _write_source(src / "nortaxa", "nortaxa", taxa=[
        {"core_row_id": "row-A", "taxon_id": "300190",
         "scientific_name": "Candolleomyces candolleanus",
         "authorship": "(Fr.) D. Wächt. & A. Melzer", "status": "valid",
         "genus": "Candolleomyces", "specific_epithet": "candolleanus"},
    ], vernacular=[
        {"core_row_id": "row-A", "language": "nb", "name": "hvit sprøsopp",
         "preferred": True},
    ])
    mappings = tmp_path / "mappings.json"
    mappings.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": [],
    }, sort_keys=True) + "\n")
    return col, nor


# ---------------------- extraction ---------------------------------------


def test_extraction_labels_unknown_provider_as_legacy_sporely(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.sqlite3"
    _build_synthetic_bundled_db(bundled)
    # Corrupt one row to have empty source.
    conn = sqlite3.connect(str(bundled))
    conn.execute("UPDATE vernacular_min SET source='' WHERE language_code='fr'")
    conn.commit(); conn.close()
    out = tmp_path / "legacy.jsonl"
    result = export_legacy(bundled_db=bundled, output_path=out)
    entries = [json.loads(l) for l in out.read_text().splitlines() if l.strip()]
    fr = [e for e in entries if e["kind"] == "vernacular"
          and e["language"] == "fr"]
    assert fr and all(e["provider"] == "legacy_sporely" for e in fr)


# ---------------------- compile integration -------------------------------


def test_compile_routes_legacy_via_nortaxa_to_sporely(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.sqlite3"
    _build_synthetic_bundled_db(bundled)
    legacy = tmp_path / "legacy.jsonl"
    export_legacy(bundled_db=bundled, output_path=legacy)

    col, nor = _write_synthetic_release(tmp_path)
    release_dir = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=tmp_path / "mappings.json",
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release_dir,
        release_id="tax-2026.07.29-01",
        legacy_enrichment_path=legacy,
    )
    diag = json.loads((release_dir / "diagnostics.json").read_text())
    legacy_counts = diag["counts"]["legacy_enrichment"]
    # NorTaxa 300190 is in scope → its Artportalen id + fr + de vernacular are
    # added; the "no" vernacular is skipped as already-in-Stage-3A; the
    # ghost 999999 has no NorTaxa presence → all 2 rows skip.
    assert legacy_counts["vernacular_added"] == 2  # fr + de
    assert legacy_counts["external_id_added"] == 1  # Artportalen 222138
    assert legacy_counts["ignored_reason_already_in_stage3a"] == 1  # 'no' row
    assert legacy_counts["unresolved_nortaxa_taxonid"] == 2  # both 999999 rows

    # Every legacy row resolves to the accepted Sporely id.
    ext_rows = [json.loads(l) for l in
                (release_dir / "legacy_external_ids.jsonl").read_text().splitlines()
                if l.strip()]
    assert len(ext_rows) == 1
    assert ext_rows[0]["source_system"] == "artportalen"
    assert ext_rows[0]["external_id"] == "222138"
    # Vernaculars appended to vernacular.jsonl.
    verns = [json.loads(l) for l in
             (release_dir / "vernacular.jsonl").read_text().splitlines()
             if l.strip()]
    langs = {v["language"] for v in verns}
    assert {"nb", "fr", "de"} <= langs
    # Skips file records the two ghost 999999 rows.
    skips = [json.loads(l) for l in
             (release_dir / "legacy_enrichment_skips.jsonl").read_text().splitlines()
             if l.strip()]
    assert len(skips) == 2
    assert all(s["reason"] == "nortaxa_taxon_id_not_in_registry" for s in skips)


def test_compile_never_allocates_sporely_id_for_legacy(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.sqlite3"
    _build_synthetic_bundled_db(bundled)
    legacy = tmp_path / "legacy.jsonl"
    export_legacy(bundled_db=bundled, output_path=legacy)
    col, nor = _write_synthetic_release(tmp_path)
    release_dir = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=tmp_path / "mappings.json",
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release_dir,
        release_id="tax-2026.07.29-01",
        legacy_enrichment_path=legacy,
    )
    diag = json.loads((release_dir / "diagnostics.json").read_text())
    # Only two canonical Sporely IDs: the COL row and the NorTaxa row that
    # cross-aliases onto it. Legacy input did not add any Sporely identity.
    assert diag["counts"]["compiled_taxa"] == 1  # NorTaxa 300190 folds onto COL
    assert diag["counts"]["registry_anchors"] == 1


# ---------------------- SQLite build integration --------------------------


def test_sqlite_build_ingests_legacy_external_ids(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.sqlite3"
    _build_synthetic_bundled_db(bundled)
    legacy = tmp_path / "legacy.jsonl"
    export_legacy(bundled_db=bundled, output_path=legacy)
    col, nor = _write_synthetic_release(tmp_path)
    release_dir = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=tmp_path / "mappings.json",
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release_dir,
        release_id="tax-2026.07.29-01",
        legacy_enrichment_path=legacy,
    )
    output_db = tmp_path / "candidate.sqlite3"
    build_candidate(
        release_dir=release_dir,
        registry_path=tmp_path / "registry.jsonl",
        output_db=output_db,
    )
    conn = sqlite3.connect(f"file:{output_db}?mode=ro", uri=True)
    sporely_id = conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name=?",
        ("Candolleomyces candolleanus",)).fetchone()[0]
    # Artportalen id lands in taxon_external_id_min.
    rows = list(conn.execute(
        "SELECT taxon_id, external_id, note FROM taxon_external_id_min "
        "WHERE source_system=? AND external_id=?", ("artportalen", 222138)))
    assert rows == [(sporely_id, 222138, "legacy_compat:artportalen")], rows
    # French vernacular lands in vernacular_min.
    rows = list(conn.execute(
        "SELECT taxon_id FROM vernacular_min "
        "WHERE language_code=? AND vernacular_name=?",
        ("fr", "psathyrelle de Candolle")))
    assert rows == [(sporely_id,)]


def test_two_builds_with_legacy_are_deterministic(tmp_path: Path) -> None:
    bundled = tmp_path / "bundled.sqlite3"
    _build_synthetic_bundled_db(bundled)
    legacy = tmp_path / "legacy.jsonl"
    export_legacy(bundled_db=bundled, output_path=legacy)
    col, nor = _write_synthetic_release(tmp_path)

    def _run(prefix: str) -> tuple[bytes, ...]:
        rel = tmp_path / f"release_{prefix}"
        reg = tmp_path / f"registry_{prefix}.jsonl"
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=tmp_path / "mappings.json",
            mapping_policy_path=_POLICY_PATH,
            registry_path=reg, output_dir=rel,
            release_id="tax-2026.07.29-01",
            legacy_enrichment_path=legacy,
        )
        db = tmp_path / f"candidate_{prefix}.sqlite3"
        build_candidate(release_dir=rel, registry_path=reg, output_db=db)
        return (
            (rel / "vernacular.jsonl").read_bytes(),
            (rel / "legacy_external_ids.jsonl").read_bytes(),
            (rel / "legacy_enrichment_skips.jsonl").read_bytes(),
            db.read_bytes(),
        )

    a = _run("a")
    b = _run("b")
    for i in range(4):
        assert hashlib.sha256(a[i]).hexdigest() == hashlib.sha256(b[i]).hexdigest(), i
