"""Focused tests for the Stage 3B SQLite candidate builder.

Small synthetic fixtures — no dependency on the real archives.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_sqlite_candidate import BuildError, build_candidate  # noqa: E402
from compile_release import compile_release  # noqa: E402

_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


def _write_source(root: Path, *, source_code: str, taxa: list[dict],
                  vernacular: list[dict] | None = None,
                  source_release: dict | None = None) -> Path:
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
    release = source_release or {"version": "v", "issued_date": "d"}
    with (root / "taxa.jsonl").open("w", encoding="utf-8") as h:
        for row in taxa:
            record = {
                "source_code": source_code,
                "source_release": release,
                "core_row_id": {"value": row["core_row_id"],
                                "namespace": ns["core_row_id"]},
                "taxon_id": {"value": row["taxon_id"],
                             "namespace": ns["taxon_id"]},
                "accepted_name_usage_id": (
                    {"value": row["accepted"],
                     "namespace": ns["accepted_name_usage_id"]}
                    if row.get("accepted") else None
                ),
                "parent_name_usage_id": (
                    {"value": row["parent"],
                     "namespace": ns["parent_name_usage_id"]}
                    if row.get("parent") else None
                ),
                "parent_reference_resolution": row.get("parent_resolution", "absent"),
                "identifier_namespace": source_code,
                "scientific_name": row["scientific_name"],
                "authorship": row.get("authorship", ""),
                "rank": row.get("rank", "species"),
                "taxonomic_status": row.get("status", "accepted"),
                "external_ids": row.get("external_ids", {}),
                "classification": {
                    "kingdom": row.get("kingdom", "Fungi"),
                    "phylum": "", "class": "", "order": "",
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
            h.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    if vernacular:
        with (root / "vernacular.jsonl").open("w", encoding="utf-8") as h:
            for v in vernacular:
                record = {
                    "source_code": source_code,
                    "source_release": release,
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
                h.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    ns_report = {
        "core_row_id": ns["core_row_id"], "taxon_id": ns["taxon_id"],
        "accepted_name_usage_id": ns["accepted_name_usage_id"],
        "parent_name_usage_id": ns["parent_name_usage_id"],
    }
    (root / "report.json").write_text(json.dumps({
        "result": "passed",
        "profile_source_code": source_code,
        "profile_source_release": release,
        "record_counts": {"Taxon": len(taxa),
                          "VernacularName": len(vernacular or [])},
        "outputs": {"taxa": "taxa.jsonl"},
        "distribution_imported": False,
        "identifier_namespaces": ns_report,
        "archive_sha256": "00" * 32,
        "reference_gaps": {"orphan_parent_reference_count": 0,
                           "orphan_accepted_reference_count": 0,
                           "orphan_parent_reference_samples": [],
                           "orphan_accepted_reference_samples": [],
                           "sample_bound": 25},
        "hierarchy_complete": True,
        "compiler_ready": True,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return root


def _write_mappings(path: Path) -> Path:
    path.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1",
        "schema": {}, "mappings": [],
    }, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _compile(tmp: Path) -> Path:
    col = _write_source(tmp / "src" / "col_xr", source_code="col_xr", taxa=[
        {"core_row_id": "COL-K", "taxon_id": "COL-K",
         "scientific_name": "Fungi", "rank": "kingdom", "kingdom": "Fungi"},
        {"core_row_id": "COL-G", "taxon_id": "COL-G", "parent": "COL-K",
         "parent_resolution": "resolved",
         "scientific_name": "Candolleomyces", "rank": "genus",
         "kingdom": "Fungi", "genus": "Candolleomyces"},
        {"core_row_id": "COL-A", "taxon_id": "COL-A", "parent": "COL-G",
         "parent_resolution": "resolved",
         "scientific_name": "Candolleomyces candolleanus",
         "authorship": "(Fr.) D. Wächt. & A. Melzer",
         "kingdom": "Fungi", "genus": "Candolleomyces",
         "specific_epithet": "candolleanus", "family": "Psathyrellaceae"},
    ])
    nor = _write_source(tmp / "src" / "nortaxa", source_code="nortaxa", taxa=[
        {"core_row_id": "row-A", "taxon_id": "300190",
         "scientific_name": "Candolleomyces candolleanus",
         "authorship": "(Fr.) D. Wächt. & A. Melzer",
         "status": "valid", "kingdom": "Fungi",
         "genus": "Candolleomyces", "specific_epithet": "candolleanus"},
        {"core_row_id": "row-S", "taxon_id": "54995",
         "scientific_name": "Psathyrella candolleana",
         "authorship": "(Fr. : Fr.) Maire",
         "status": "synonym", "accepted": "300190",
         "kingdom": "Fungi",
         "genus": "Psathyrella", "specific_epithet": "candolleana"},
    ], vernacular=[
        {"core_row_id": "row-A", "language": "nb",
         "name": "hvit sprøsopp", "preferred": True},
        {"core_row_id": "row-A", "language": "nn",
         "name": "kvit sprøsopp", "preferred": True},
        {"core_row_id": "row-A", "language": "sma",
         "name": "sample-sma", "preferred": False},
    ])
    out = tmp / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=_write_mappings(tmp / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp / "registry.jsonl",
        output_dir=out,
        release_id="tax-2026.07.29-01",
    )
    return out


def test_build_creates_expected_schema_and_row_counts(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    summary = build_candidate(
        release_dir=release_dir,
        registry_path=tmp_path / "registry.jsonl",
        output_db=output,
    )
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"taxon_min", "vernacular_min", "scientific_name_min",
            "taxon_external_id_min", "taxon_external_id_text_min",
            "taxonomy_meta"} <= tables
    # 3 canonical taxa (COL kingdom, genus, species) — NorTaxa accepted
    # auto-aliases onto COL-A, synonym aliases onto that same identity.
    assert summary["counts"]["taxon_min"] == 3
    # 3 vernaculars preserved distinct languages.
    assert summary["counts"]["vernacular_min"] == 3
    # Metadata written.
    meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
    assert meta["taxonomy_schema_version"] == "2"
    assert meta["content_release_id"] == "tax-2026.07.29-01"
    assert meta["state"] == "candidate"
    assert meta["publication"] == "none"


def test_lookup_by_accepted_scientific_name(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    rows = list(conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE lower(canonical_scientific_name)=lower(?)",
        ("Candolleomyces candolleanus",)))
    assert len(rows) == 1
    sporely_id = rows[0][0]
    # Same Sporely ID resolves from all aliases:
    rows = list(conn.execute(
        "SELECT DISTINCT taxon_id FROM scientific_name_min "
        "WHERE lower(scientific_name)=lower(?)",
        ("Psathyrella candolleana",)))
    assert rows == [(sporely_id,)]
    rows = list(conn.execute(
        "SELECT DISTINCT taxon_id FROM vernacular_min "
        "WHERE lower(vernacular_name)=lower(?)",
        ("hvit sprøsopp",)))
    assert rows == [(sporely_id,)]
    rows = list(conn.execute(
        "SELECT DISTINCT taxon_id FROM vernacular_min "
        "WHERE lower(vernacular_name)=lower(?)",
        ("kvit sprøsopp",)))
    assert rows == [(sporely_id,)]


def test_external_identifier_lookup_54995_via_namespace(tmp_path: Path) -> None:
    """The NorTaxa taxonID 54995 must resolve via its explicit namespace,
    not by treating 54995 as a Sporely ID."""
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    canonical_id = conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE lower(canonical_scientific_name)=lower(?)",
        ("Candolleomyces candolleanus",)).fetchone()[0]
    # Namespaced INTEGER external-ID lookup — id_role='synonym' because
    # 54995 is the NorTaxa synonym record.
    rows = list(conn.execute(
        "SELECT taxon_id, id_role FROM taxon_external_id_min "
        "WHERE source_system=? AND external_id=?",
        ("artsdatabanken", 54995)))
    assert rows == [(canonical_id, "synonym")]
    # 54995 must NOT be a taxon_id.
    row = conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE taxon_id = 54995").fetchone()
    assert row is None


def test_col_usage_id_stored_in_text_table(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    canonical_id = conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE lower(canonical_scientific_name)=lower(?)",
        ("Candolleomyces candolleanus",)).fetchone()[0]
    rows = list(conn.execute(
        "SELECT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system=? AND namespace=? AND external_id=?",
        ("col_xr", "col_usage_id", "COL-A")))
    assert rows == [(canonical_id,)]


def test_hierarchy_only_when_resolved(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    # Every non-root canonical taxon has a resolved parent_taxon_id pointing
    # inside taxon_min.
    orphans = conn.execute(
        "SELECT COUNT(*) FROM taxon_min t "
        "WHERE parent_taxon_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM taxon_min p "
        "                 WHERE p.taxon_id = t.parent_taxon_id)").fetchone()[0]
    assert orphans == 0
    # The kingdom row has no parent.
    row = conn.execute(
        "SELECT parent_taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = 'Fungi'").fetchone()
    assert row == (None,)


def test_integrity_and_foreign_key_checks_pass(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(str(output))
    assert list(conn.execute("PRAGMA integrity_check")) == [("ok",)]
    assert list(conn.execute("PRAGMA foreign_key_check")) == []


def test_two_builds_produce_identical_logical_content(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    a = tmp_path / "a.sqlite3"
    b = tmp_path / "b.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=a)
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=b)

    def logical(path: Path) -> tuple:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        out = []
        for table in ("taxon_min", "scientific_name_min", "vernacular_min",
                      "taxon_external_id_min", "taxon_external_id_text_min",
                      "taxonomy_meta"):
            out.append((table, tuple(conn.execute(
                f"SELECT * FROM {table} ORDER BY rowid"))))
        return tuple(out)

    assert logical(a) == logical(b)


def test_no_dangling_relationships(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release_dir,
                    registry_path=tmp_path / "registry.jsonl",
                    output_db=output)
    conn = sqlite3.connect(f"file:{output}?mode=ro", uri=True)
    for table in ("scientific_name_min", "vernacular_min",
                  "taxon_external_id_min", "taxon_external_id_text_min"):
        orphans = conn.execute(
            f"SELECT COUNT(*) FROM {table} x "
            f"WHERE NOT EXISTS (SELECT 1 FROM taxon_min t "
            f"                   WHERE t.taxon_id = x.taxon_id)").fetchone()[0]
        assert orphans == 0, table


def test_build_refuses_existing_output(tmp_path: Path) -> None:
    release_dir = _compile(tmp_path)
    output = tmp_path / "candidate.sqlite3"
    output.write_bytes(b"")
    with pytest.raises(BuildError, match="already exists"):
        build_candidate(release_dir=release_dir,
                        registry_path=tmp_path / "registry.jsonl",
                        output_db=output)
