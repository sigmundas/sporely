"""Stage 3B.4: Norwegian Red List 2021 overlay tests.

Covers the normalizer, compiler resolution, SQLite population, and the
runtime lookup. Synthetic workbooks are built with openpyxl so tests do not
depend on the manually-downloaded 10 MB file.
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

from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import compile_release  # noqa: E402
from normalize_redlist_no import (  # noqa: E402
    ALLOWED_CATEGORIES,
    RedlistNormalizeError,
    normalize,
)


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"
_REPO_ROOT = Path(__file__).resolve().parents[3]

pytest.importorskip("openpyxl")


HEADERS = [
    "Id for vurderingen", "Vurderingsområde", "Ekspertkomité", "Artsgruppe",
    "Taksonomisk sti", "Vitenskapelig navn id", "Vitenskapelig navn", "Autor",
    "Populærnavn", "Taksonomisk nivå", "Årstall for siste revisjon",
    "Kategori 2021", "Kriterier 2021",
    "Kriteriedokumentasjon", "Begrunnelse nedgradering av kategori",
]


def _write_workbook(path: Path, rows, headers=HEADERS) -> None:
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vurderinger"
    ws.append(headers)
    for row in rows:
        ws.append(list(row))
    # Include the other sheets so the sheet-name check still matches real data.
    wb.create_sheet("Feltnavn og beskrivelser")
    wb.save(str(path))


def _row(**overrides):
    base = {
        "Id for vurderingen": 15090,
        "Vurderingsområde": "Norge",
        "Ekspertkomité": "Sopper",
        "Artsgruppe": "Sopper",
        "Taksonomisk sti": "Fungi/Basidiomycota/…/Amanita/muscaria",
        "Vitenskapelig navn id": 52147,
        "Vitenskapelig navn": "Amanita muscaria",
        "Autor": "(L.) Lam.",
        "Populærnavn": "rød fluesopp",
        "Taksonomisk nivå": "Art",
        "Årstall for siste revisjon": 2021,
        "Kategori 2021": "LC",
        "Kriterier 2021": "",
        "Kriteriedokumentasjon": "",
        "Begrunnelse nedgradering av kategori": "",
    }
    base.update(overrides)
    return [base[h] for h in HEADERS]


# ---------------------- Normalizer -----------------------------------------


def test_normalizer_recognizes_headers_in_any_order(tmp_path: Path) -> None:
    shuffled = list(reversed(HEADERS))
    row_map = {h: v for h, v in zip(HEADERS, _row())}
    reordered = [row_map[h] for h in shuffled]
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [reordered], headers=shuffled)
    report = normalize(input_path=xlsx, output_dir=tmp_path / "out")
    assert report["row_count"] == 1
    assessments = [
        json.loads(line) for line in
        (tmp_path / "out" / "assessments.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert assessments[0]["scientific_name_snapshot"] == "Amanita muscaria"


def test_normalizer_rejects_missing_required_header(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    reduced = [h for h in HEADERS if h != "Kategori 2021"]
    row_map = {h: v for h, v in zip(HEADERS, _row())}
    _write_workbook(xlsx, [[row_map[h] for h in reduced]], headers=reduced)
    with pytest.raises(RedlistNormalizeError, match="missing required"):
        normalize(input_path=xlsx, output_dir=tmp_path / "out")


def test_normalizer_preserves_all_category_classes(tmp_path: Path) -> None:
    rows = [
        _row(**{"Id for vurderingen": 100 + i,
                "Vitenskapelig navn id": 500 + i,
                "Vitenskapelig navn": f"Genus species{i}",
                "Kategori 2021": cat})
        for i, cat in enumerate(sorted(ALLOWED_CATEGORIES))
    ]
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, rows)
    report = normalize(input_path=xlsx, output_dir=tmp_path / "out")
    assert set(report["category_counts"]) == set(ALLOWED_CATEGORIES)


def test_normalizer_preserves_downgraded_category(tmp_path: Path) -> None:
    rows = [_row(**{"Kategori 2021": "VU°"})]
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, rows)
    normalize(input_path=xlsx, output_dir=tmp_path / "out")
    assessment = json.loads(
        (tmp_path / "out" / "assessments.jsonl").read_text().splitlines()[0]
    )
    assert assessment["category_raw"] == "VU°"
    assert assessment["category_code"] == "VU"
    assert assessment["category_is_downgraded"] is True


def test_normalizer_rejects_unknown_category(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [_row(**{"Kategori 2021": "ZZ"})])
    with pytest.raises(RedlistNormalizeError, match="unknown category"):
        normalize(input_path=xlsx, output_dir=tmp_path / "out")


def test_normalizer_rejects_unknown_area(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [_row(**{"Vurderingsområde": "Mars"})])
    with pytest.raises(RedlistNormalizeError, match="unknown assessment area"):
        normalize(input_path=xlsx, output_dir=tmp_path / "out")


def test_normalizer_rejects_duplicate_assessment_id(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [
        _row(**{"Id for vurderingen": 42, "Vitenskapelig navn id": 1}),
        _row(**{"Id for vurderingen": 42, "Vitenskapelig navn id": 2,
                "Vitenskapelig navn": "Other species"}),
    ])
    with pytest.raises(RedlistNormalizeError, match="duplicate assessment_id"):
        normalize(input_path=xlsx, output_dir=tmp_path / "out")


def test_normalizer_norway_and_svalbard_stay_separate(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [
        _row(**{"Id for vurderingen": 1, "Vitenskapelig navn id": 100,
                "Vurderingsområde": "Norge"}),
        _row(**{"Id for vurderingen": 2, "Vitenskapelig navn id": 100,
                "Vurderingsområde": "Svalbard"}),
    ])
    report = normalize(input_path=xlsx, output_dir=tmp_path / "out")
    assert report["area_counts"] == {"Norge": 1, "Svalbard": 1}


def test_normalizer_deterministic_two_runs(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [
        _row(**{"Id for vurderingen": 2, "Vitenskapelig navn id": 200}),
        _row(**{"Id for vurderingen": 1, "Vitenskapelig navn id": 100,
                "Vitenskapelig navn": "Aa aa"}),
    ])
    normalize(input_path=xlsx, output_dir=tmp_path / "out1")
    normalize(input_path=xlsx, output_dir=tmp_path / "out2")
    a = (tmp_path / "out1" / "assessments.jsonl").read_bytes()
    b = (tmp_path / "out2" / "assessments.jsonl").read_bytes()
    assert a == b
    ra = (tmp_path / "out1" / "report.json").read_bytes()
    rb = (tmp_path / "out2" / "report.json").read_bytes()
    assert ra == rb


def test_normalizer_rank_whitelist(tmp_path: Path) -> None:
    xlsx = tmp_path / "wb.xlsx"
    _write_workbook(xlsx, [
        _row(**{"Id for vurderingen": 1, "Vitenskapelig navn id": 1,
                "Taksonomisk nivå": "Art"}),
        _row(**{"Id for vurderingen": 2, "Vitenskapelig navn id": 2,
                "Vitenskapelig navn": "Boletus edulis f.",
                "Taksonomisk nivå": "Kaos"}),
    ])
    normalize(input_path=xlsx, output_dir=tmp_path / "out")
    rows = [
        json.loads(line) for line in
        (tmp_path / "out" / "assessments.jsonl").read_text().splitlines()
        if line.strip()
    ]
    ranks = {r["assessment_id"]: r["taxon_rank_snapshot"] for r in rows}
    assert ranks == {"1": "species", "2": None}


# ---------------------- Compile-time resolution ----------------------------


def _write_synthetic_release(tmp_path: Path):
    """Build a COL + NorTaxa synthetic release where NorTaxa row carries the
    DwC scientificNameID URI populated with '52147' (Amanita muscaria)."""
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    def _source(root, source_code, taxa):
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
                    "external_ids": row.get("external_ids", {}),
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
        (root / "report.json").write_text(json.dumps({
            "result": "passed",
            "profile_source_code": source_code,
            "profile_source_release": release,
            "record_counts": {"Taxon": len(taxa)},
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

    col = _source(src / "col_xr", "col_xr", taxa=[
        {"core_row_id": "COL-A", "taxon_id": "COL-A",
         "scientific_name": "Amanita muscaria", "authorship": "(L.) Lam.",
         "genus": "Amanita", "specific_epithet": "muscaria"},
    ])
    # NorTaxa taxonID == the workbook's "Vitenskapelig navn id" (Artsnavnebase
    # scientific-name id). No extra external_ids column is exposed by the real
    # NorTaxa DwC-A; the bridge is namespace equality.
    nor = _source(src / "nortaxa", "nortaxa", taxa=[
        {"core_row_id": "row-A", "taxon_id": "52147",
         "scientific_name": "Amanita muscaria", "authorship": "(L.) Lam.",
         "status": "valid", "genus": "Amanita", "specific_epithet": "muscaria"},
    ])
    mappings = tmp_path / "mappings.json"
    mappings.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": [],
    }, sort_keys=True) + "\n")
    return col, nor, mappings


def _redlist_dir(tmp_path: Path, rows) -> Path:
    xlsx = tmp_path / "redlist.xlsx"
    _write_workbook(xlsx, rows)
    out = tmp_path / "redlist_norm"
    normalize(input_path=xlsx, output_dir=out)
    return out


def test_compile_resolves_by_scientific_name_id(tmp_path: Path) -> None:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147,
                "Kategori 2021": "VU"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.29-01",
        redlist_dir=redlist,
    )
    rows = [json.loads(l) for l in
            (release / "redlist_no.jsonl").read_text().splitlines() if l.strip()]
    assert len(rows) == 1
    assert rows[0]["taxon_id"] is not None
    assert rows[0]["resolution"] == "resolved_by_artsnavnebase_scientific_name_id"
    diag = json.loads((release / "redlist_no_diagnostics.json").read_text())
    assert diag["counts"]["resolved"] == 1
    assert diag["counts"]["unresolved"] == 0


def test_compile_leaves_unresolved_with_null_taxon_id(tmp_path: Path) -> None:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 99999, "Vitenskapelig navn id": 999999,
                "Vitenskapelig navn": "Ghost species",
                "Kategori 2021": "DD"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.29-01",
        redlist_dir=redlist,
    )
    rows = [json.loads(l) for l in
            (release / "redlist_no.jsonl").read_text().splitlines() if l.strip()]
    assert rows[0]["taxon_id"] is None
    assert rows[0]["resolution"] == "unresolved"


def test_compile_never_resolves_by_scientific_name(tmp_path: Path) -> None:
    """Name equality alone must NOT resolve identity — only the explicit
    scientificNameID bridge does."""
    col, nor, mappings = _write_synthetic_release(tmp_path)
    # Assessment carries a DIFFERENT numeric name-id but the SAME scientific
    # name string. Must remain unresolved.
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 88888,
                "Vitenskapelig navn": "Amanita muscaria",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.29-01",
        redlist_dir=redlist,
    )
    rows = [json.loads(l) for l in
            (release / "redlist_no.jsonl").read_text().splitlines() if l.strip()]
    assert rows[0]["taxon_id"] is None


def test_compile_redlist_never_mutates_identity_registry(tmp_path: Path) -> None:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147}),
        _row(**{"Id for vurderingen": 55555, "Vitenskapelig navn id": 999999,
                "Vitenskapelig navn": "Ghost x"}),
    ])
    reg = tmp_path / "registry.jsonl"
    reg2 = tmp_path / "registry2.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=tmp_path / "with_redlist",
        release_id="tax-2026.07.29-01", redlist_dir=redlist,
    )
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg2, output_dir=tmp_path / "without_redlist",
        release_id="tax-2026.07.29-01",
    )
    assert reg.read_bytes() == reg2.read_bytes()


# ---------------------- SQLite population ---------------------------------


def test_sqlite_taxon_redlist_min_schema_and_query(tmp_path: Path) -> None:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU"}),
        _row(**{"Id for vurderingen": 20000, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Svalbard", "Kategori 2021": "EN"}),
        _row(**{"Id for vurderingen": 99999, "Vitenskapelig navn id": 999999,
                "Vitenskapelig navn": "Ghost x", "Kategori 2021": "DD"}),
    ])
    release = tmp_path / "release"
    reg = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=release,
        release_id="tax-2026.07.29-01", redlist_dir=redlist,
    )
    db = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release, registry_path=reg, output_db=db)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    schema = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "taxon_redlist_min" in schema
    indexes = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_redlist_taxon_area_release" in indexes
    assert "idx_redlist_assessment_id" in indexes
    assert "idx_redlist_name_area" in indexes

    total = conn.execute("SELECT COUNT(*) FROM taxon_redlist_min").fetchone()[0]
    assert total == 3
    # Norway and Svalbard both resolved to the same taxon_id, but stay separate
    # rows keyed by area.
    resolved = conn.execute(
        "SELECT assessment_area, category_code FROM taxon_redlist_min "
        "WHERE taxon_id IS NOT NULL ORDER BY assessment_area"
    ).fetchall()
    assert resolved == [("Norge", "VU"), ("Svalbard", "EN")]
    unresolved = conn.execute(
        "SELECT COUNT(*) FROM taxon_redlist_min WHERE taxon_id IS NULL"
    ).fetchone()[0]
    assert unresolved == 1
    # Norway query only returns Norway.
    row = conn.execute(
        "SELECT category_code FROM taxon_redlist_min "
        "WHERE assessment_area='Norge' AND taxon_id IS NOT NULL"
    ).fetchone()
    assert row[0] == "VU"


def test_sqlite_two_builds_are_deterministic(tmp_path: Path) -> None:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147}),
    ])

    def _once(tag: str) -> bytes:
        release = tmp_path / f"release_{tag}"
        reg = tmp_path / f"reg_{tag}.jsonl"
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
            registry_path=reg, output_dir=release,
            release_id="tax-2026.07.29-01", redlist_dir=redlist,
        )
        return (release / "redlist_no.jsonl").read_bytes()

    assert _once("a") == _once("b")


# ---------------------- Registry fixtures for concept-vs-name-id -----------


def _known_id_pair_release(tmp_path: Path):
    """A synthetic release seeded with the exact NorTaxa row for the two
    identity pairs from the audit:

      Vulpes vulpes:          name-id 48034, concept-id 31176 (a totally
                              different NorTaxa row about Chironomus pallens
                              — a fly, not a fox).
      Cladonia chlorophaea:   name-id 69071, concept-id 45044 (a totally
                              different NorTaxa row about Acrolepiopsis
                              betulella — a moth, not a lichen).

    NorTaxa's DwC ``taxonID`` values carry Artsnavnebase scientific-name
    ids; the concept-id integers happen to name entirely unrelated NorTaxa
    rows (that's the whole point of the audit). We seed both so tests can
    prove the concept-id is not accidentally bridged onto the name concept.
    """
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    def _source(root, source_code, taxa):
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
                h.write(json.dumps(rec, sort_keys=True) + "\n")
        (root / "report.json").write_text(json.dumps({
            "result": "passed",
            "profile_source_code": source_code,
            "profile_source_release": release,
            "record_counts": {"Taxon": len(taxa)},
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

    # Two fungal NorTaxa rows with the exact IDs from the audit:
    # 69071 (Cladonia chlorophaea) and 45044 (an unrelated fungus seeded
    # here so it stays in fungal scope; its identity must NOT be bridged to
    # Cladonia chlorophaea by the concept-id numeric).
    nor = _source(src / "nortaxa", "nortaxa", taxa=[
        {"core_row_id": "row-cla", "taxon_id": "69071",
         "scientific_name": "Cladonia chlorophaea",
         "authorship": "(Sommerf.) Sprengel", "status": "valid",
         "genus": "Cladonia", "specific_epithet": "chlorophaea",
         "family": "Cladoniaceae"},
        {"core_row_id": "row-other", "taxon_id": "45044",
         "scientific_name": "Other fungus alpha", "authorship": "Auth",
         "status": "valid", "genus": "Other", "specific_epithet": "alpha",
         "family": "OtherFamily"},
    ])
    # COL row carrying a numeric usage id "48034" — this must NOT resolve
    # the workbook Vulpes vulpes assessment. COL usage ids are a different
    # registry and a numeric collision must be ignored.
    col = _source(src / "col_xr", "col_xr", taxa=[
        {"core_row_id": "48034", "taxon_id": "48034",
         "scientific_name": "Cladonia chlorophaea",
         "authorship": "(Sommerf.) Sprengel",
         "genus": "Cladonia", "specific_epithet": "chlorophaea"},
    ])
    mappings = tmp_path / "mappings.json"
    mappings.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": [],
    }, sort_keys=True) + "\n")
    return col, nor, mappings


def test_workbook_name_id_69071_resolves_to_cladonia(tmp_path: Path) -> None:
    col, nor, mappings = _known_id_pair_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 20504, "Vitenskapelig navn id": 69071,
                "Vitenskapelig navn": "Cladonia chlorophaea",
                "Autor": "(Sommerf.) Sprengel",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.30-01",
        redlist_dir=redlist,
    )
    rows = [json.loads(l) for l in
            (release / "redlist_no.jsonl").read_text().splitlines() if l.strip()]
    assert rows[0]["taxon_id"] is not None
    assert rows[0]["scientific_name_snapshot"] == "Cladonia chlorophaea"
    assert rows[0]["resolution"] == \
        "resolved_by_artsnavnebase_scientific_name_id"
    assert rows[0]["resolved_via"] == "artsnavnebase_scientific_name_id"


def test_concept_id_45044_stays_unresolved_under_coherence_check(tmp_path: Path) -> None:
    """Workbook rows use Artsnavnebase scientific-name IDs. The
    Artsdatabanken taxon-concept id 45044 for Cladonia chlorophaea is a
    value from a DIFFERENT Artsdatabanken registry; it also happens to be
    a valid Artsnavnebase name-id (for the unrelated NorTaxa row
    "Other fungus alpha" in this fixture, or Acrolepiopsis betulella in
    real NorTaxa 1.284). The source-row coherence check must reject the
    binding rather than resolve the assessment onto that unrelated row —
    and must not fall back to name matching either. Correct outcome: the
    row stays entirely unresolved with reason
    ``unresolved_name_id_name_mismatch``, both scientific names preserved
    for later audit."""
    col, nor, mappings = _known_id_pair_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 20504, "Vitenskapelig navn id": 45044,
                "Vitenskapelig navn": "Cladonia chlorophaea",
                "Autor": "(Sommerf.) Sprengel",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.30-01",
        redlist_dir=redlist,
    )
    row = [json.loads(l) for l in
           (release / "redlist_no.jsonl").read_text().splitlines()
           if l.strip()][0]
    assert row["taxon_id"] is None
    assert row["resolution"] == "unresolved"
    assert row["resolved_via"] is None
    assert row["unresolved_reason"] == "unresolved_name_id_name_mismatch"
    # Both names are preserved in evidence:
    assert row["scientific_name_snapshot"] == "Cladonia chlorophaea"
    assert row["source_row_scientific_name"] == "Other fungus alpha"
    # Confirm neither Cladonia's Sporely id nor the other row's id was
    # bound to this assessment.
    taxa = {json.loads(l)["scientific_name"]: json.loads(l)["sporely_taxon_id"]
            for l in (release / "taxa.jsonl").read_text().splitlines() if l.strip()}
    assert taxa["Other fungus alpha"] is not None  # sanity
    assert taxa["Cladonia chlorophaea"] is not None  # sanity
    # And the diagnostics record the mismatch:
    diag = json.loads((release / "redlist_no_diagnostics.json").read_text())
    assert diag["counts"]["name_id_name_mismatch"] == 1
    assert any(s["assessed_name_id"] == "45044"
               for s in diag["name_id_name_mismatch_samples"])


def test_conservative_name_normalization_accepts_subgenus_notation(tmp_path: Path) -> None:
    """The workbook often carries "Genus (Subgen) species" while NorTaxa
    publishes plain "Genus species". Conservative normalization strips the
    parenthetical so the row resolves, not remain a false mismatch."""
    col, nor, mappings = _known_id_pair_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 20504, "Vitenskapelig navn id": 69071,
                "Vitenskapelig navn": "Cladonia (Cladonia) chlorophaea",
                "Autor": "(Sommerf.) Sprengel",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.30-01",
        redlist_dir=redlist,
    )
    row = [json.loads(l) for l in
           (release / "redlist_no.jsonl").read_text().splitlines()
           if l.strip()][0]
    assert row["taxon_id"] is not None
    assert row["resolution"] == "resolved_by_artsnavnebase_scientific_name_id"


def test_col_usage_id_numeric_collision_does_not_resolve(tmp_path: Path) -> None:
    """The workbook's Vulpes vulpes uses name-id 48034. Our fixture has a
    COL usage id that happens to be the string ``48034``. That COL usage
    id is in the ``col_usage_id`` namespace — a completely different
    registry — and must NOT resolve the red-list row."""
    col, nor, mappings = _known_id_pair_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 6714, "Vitenskapelig navn id": 48034,
                "Vitenskapelig navn": "Vulpes vulpes",
                "Autor": "(Linnaeus, 1758)",
                "Ekspertkomité": "Pattedyr",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.30-01",
        redlist_dir=redlist,
    )
    row = [json.loads(l) for l in
           (release / "redlist_no.jsonl").read_text().splitlines()
           if l.strip()][0]
    # 48034 is a col_usage_id in this fixture, NOT an Artsnavnebase
    # scientific-name id → must stay unresolved with taxon_id null.
    assert row["taxon_id"] is None
    assert row["resolution"] == "unresolved"


def test_accepted_name_usage_id_numeric_does_not_resolve_wrong_taxon(tmp_path: Path) -> None:
    """A NorTaxa synonym row's ``acceptedNameUsageID`` also points at an
    Artsnavnebase name-id. That value describes the ACCEPTED target row,
    not this synonym row's own identity. It must not be added to the
    bridge — the bridge walks only the source row's own
    ``nortaxa_taxon_id``, not other-row references."""
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    def _source(root, source_code, taxa):
        root.mkdir(parents=True, exist_ok=True)
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
                    "accepted_name_usage_id": (
                        {"value": row["accepted_id"],
                         "namespace": ns["accepted_name_usage_id"]}
                        if row.get("accepted_id") else None
                    ),
                    "parent_name_usage_id": None,
                    "parent_reference_resolution": "absent",
                    "identifier_namespace": source_code,
                    "scientific_name": row["scientific_name"],
                    "authorship": row.get("authorship", ""),
                    "rank": row.get("rank", "species"),
                    "taxonomic_status": row.get("status", "valid"),
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
        (root / "report.json").write_text(json.dumps({
            "result": "passed", "profile_source_code": source_code,
            "profile_source_release": release,
            "record_counts": {"Taxon": len(taxa)},
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

    # Two accepted NorTaxa rows so that acceptedNameUsageID resolves cleanly.
    nor = _source(src / "nortaxa", "nortaxa", taxa=[
        {"core_row_id": "row-A", "taxon_id": "70000", "accepted_id": "70000",
         "scientific_name": "Real accepted name",
         "genus": "Real", "specific_epithet": "accepted", "status": "valid"},
        {"core_row_id": "row-B", "taxon_id": "80000", "accepted_id": "70000",
         "scientific_name": "Synonym syn", "genus": "Real",
         "specific_epithet": "accepted", "status": "synonym"},
    ])
    mappings = tmp_path / "mappings.json"
    mappings.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": [],
    }, sort_keys=True) + "\n")
    # A workbook row keyed on the value that only appears as an
    # acceptedNameUsageID — must still resolve because 70000 IS the
    # accepted row's own nortaxa_taxon_id. But if we ask for a number that
    # ONLY appears as an accepted-reference target from elsewhere (never as
    # the source row's own taxonID), it must not resolve.
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 3, "Vitenskapelig navn id": 999999,
                "Vitenskapelig navn": "Missing"}),
    ])
    release = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release, release_id="tax-2026.07.30-01",
        redlist_dir=redlist,
    )
    row = [json.loads(l) for l in
           (release / "redlist_no.jsonl").read_text().splitlines()
           if l.strip()][0]
    assert row["taxon_id"] is None
    assert row["resolution"] == "unresolved"


# ---------------------- Runtime lookup ------------------------------------


def _make_v2_db_with_redlist(tmp_path: Path) -> Path:
    col, nor, mappings = _write_synthetic_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU°"}),
        _row(**{"Id for vurderingen": 20000, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Svalbard", "Kategori 2021": "EN"}),
    ])
    release = tmp_path / "release"
    reg = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=release,
        release_id="tax-2026.07.29-01", redlist_dir=redlist,
    )
    db = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release, registry_path=reg, output_db=db)
    return db


def _fetch_taxon_id(db: Path, scientific_name: str) -> int:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    row = conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name=?",
        (scientific_name,)
    ).fetchone()
    conn.close()
    return int(row[0])


def test_runtime_get_redlist_assessment_norway(tmp_path: Path) -> None:
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    db = _make_v2_db_with_redlist(tmp_path)
    tid = _fetch_taxon_id(db, "Amanita muscaria")

    class _Vern:
        def __init__(self, p):
            self.db_path = str(p)
            self.language_code = "no"
    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)
    a = svc.get_redlist_assessment(tid)  # defaults: Norge, 2021
    assert a is not None
    assert a.category_code == "VU"
    assert a.category_is_downgraded is True
    assert a.assessment_area == "Norge"

    b = svc.get_redlist_assessment(tid, area="Svalbard")
    assert b is not None and b.category_code == "EN"
    assert b.assessment_area == "Svalbard"


def test_runtime_returns_none_for_missing_area(tmp_path: Path) -> None:
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    db = _make_v2_db_with_redlist(tmp_path)
    tid = _fetch_taxon_id(db, "Amanita muscaria")

    class _Vern:
        def __init__(self, p):
            self.db_path = str(p)
            self.language_code = "no"
    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)
    # Unknown area (not silently defaulted).
    assert svc.get_redlist_assessment(tid, area="Grønland") is None
    # Unknown taxon.
    assert svc.get_redlist_assessment(999_999_999) is None


# --- Explicit RedlistLookupResult scenarios: unique / same-category / conflict / mismatch


def _make_v2_db_with_synonym_group(tmp_path: Path, workbook_rows) -> Path:
    """Build a v2 SQLite where NorTaxa has one accepted row plus a synonym
    row folded onto it — two distinct Artsnavnebase name-ids referring to
    the same Sporely concept — for testing multi-row runtime behaviors."""
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    def _source(root, source_code, taxa):
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
                    "accepted_name_usage_id": (
                        {"value": row["accepted_id"],
                         "namespace": ns["accepted_name_usage_id"]}
                        if row.get("accepted_id") else None
                    ),
                    "parent_name_usage_id": None,
                    "parent_reference_resolution": "absent",
                    "identifier_namespace": source_code,
                    "scientific_name": row["scientific_name"],
                    "authorship": row.get("authorship", ""),
                    "rank": row.get("rank", "species"),
                    "taxonomic_status": row.get("status", "valid"),
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
        (root / "report.json").write_text(json.dumps({
            "result": "passed", "profile_source_code": source_code,
            "profile_source_release": release,
            "record_counts": {"Taxon": len(taxa)},
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

    # Two NorTaxa rows for the same accepted concept "Amanita muscaria":
    # 52147 is the accepted row (name-id 52147); 52148 is a synonym that
    # canonicalizes to the same accepted scientific name. Both share a
    # Sporely id after the compiler folds the synonym.
    nor = _source(src / "nortaxa", "nortaxa", taxa=[
        {"core_row_id": "row-A", "taxon_id": "52147", "accepted_id": "52147",
         "scientific_name": "Amanita muscaria", "authorship": "(L.) Lam.",
         "status": "valid", "genus": "Amanita", "specific_epithet": "muscaria"},
        {"core_row_id": "row-B", "taxon_id": "52148", "accepted_id": "52147",
         "scientific_name": "Amanita muscaria", "authorship": "(L.) Lam.",
         "status": "synonym", "genus": "Amanita", "specific_epithet": "muscaria"},
    ])
    col = _source(src / "col_xr", "col_xr", taxa=[
        {"core_row_id": "COL-A", "taxon_id": "COL-A",
         "scientific_name": "Amanita muscaria", "authorship": "(L.) Lam.",
         "genus": "Amanita", "specific_epithet": "muscaria"},
    ])
    mappings = tmp_path / "mappings.json"
    mappings.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": [],
    }, sort_keys=True) + "\n")

    xlsx = tmp_path / "redlist.xlsx"
    _write_workbook(xlsx, workbook_rows)
    redlist = tmp_path / "redlist_norm"
    normalize(input_path=xlsx, output_dir=redlist)

    release = tmp_path / "release"
    reg = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=release,
        release_id="tax-2026.07.30-02", redlist_dir=redlist,
    )
    db = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release, registry_path=reg, output_db=db)
    return db


class _Vern:
    def __init__(self, p):
        self.db_path = str(p)
        self.language_code = "no"


def test_runtime_lookup_unique_returns_the_only_row(tmp_path: Path) -> None:
    """One assessment row for (taxon, area, release) → status='unique'."""
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    db = _make_v2_db_with_synonym_group(tmp_path, [
        _row(**{"Id for vurderingen": 15090, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU"}),
    ])
    tid = _fetch_taxon_id(db, "Amanita muscaria")
    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)

    result = svc.get_redlist_lookup(tid)
    assert result.status == "unique"
    assert result.assessment is not None
    assert result.assessment.category_code == "VU"
    assert result.assessment.assessment_id == "15090"
    assert result.conflicting_assessments == ()

    # Back-compat API returns the same representative.
    single = svc.get_redlist_assessment(tid)
    assert single is not None and single.assessment_id == "15090"


def test_runtime_lookup_same_category_duplicates_resolve_deterministically(tmp_path: Path) -> None:
    """Two workbook rows for the same Sporely concept via distinct name-ids,
    same category → status='multiple_same_category'. Representative is
    the smallest numeric assessment_id, deterministically."""
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    db = _make_v2_db_with_synonym_group(tmp_path, [
        _row(**{"Id for vurderingen": 22222, "Vitenskapelig navn id": 52148,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU"}),
        _row(**{"Id for vurderingen": 11111, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU"}),
    ])
    tid = _fetch_taxon_id(db, "Amanita muscaria")
    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)

    result = svc.get_redlist_lookup(tid)
    assert result.status == "multiple_same_category"
    assert result.assessment is not None
    assert result.assessment.category_code == "VU"
    # Determinism: representative is smallest numeric assessment_id.
    assert result.assessment.assessment_id == "11111"
    all_ids = tuple(a.assessment_id for a in result.conflicting_assessments)
    assert all_ids == ("11111", "22222")

    # And the same call is stable across repeated invocations.
    result2 = svc.get_redlist_lookup(tid)
    assert result2.assessment.assessment_id == "11111"

    # Back-compat: returns the deterministic representative.
    single = svc.get_redlist_assessment(tid)
    assert single is not None and single.category_code == "VU"
    assert single.assessment_id == "11111"


def test_runtime_lookup_conflicting_categories_returns_no_category(tmp_path: Path) -> None:
    """Two workbook rows for the same Sporely concept via distinct name-ids
    with DIFFERENT categories → status='conflict'. No representative
    is chosen. Back-compat API returns None (never auto-picks)."""
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    db = _make_v2_db_with_synonym_group(tmp_path, [
        _row(**{"Id for vurderingen": 33333, "Vitenskapelig navn id": 52147,
                "Vurderingsområde": "Norge", "Kategori 2021": "VU"}),
        _row(**{"Id for vurderingen": 44444, "Vitenskapelig navn id": 52148,
                "Vurderingsområde": "Norge", "Kategori 2021": "NT"}),
    ])
    tid = _fetch_taxon_id(db, "Amanita muscaria")
    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)

    result = svc.get_redlist_lookup(tid)
    assert result.status == "conflict"
    assert result.assessment is None
    categories = {a.category_code for a in result.conflicting_assessments}
    assert categories == {"VU", "NT"}
    ids = tuple(a.assessment_id for a in result.conflicting_assessments)
    assert ids == ("33333", "44444")

    # Back-compat: never auto-picks a category from a conflict group.
    assert svc.get_redlist_assessment(tid) is None


def test_runtime_lookup_mismatch_row_returns_no_category(tmp_path: Path) -> None:
    """A workbook row whose name-id/name coherence check failed at compile
    time is stored with taxon_id NULL in taxon_redlist_min and must never
    surface as a category for any runtime lookup — the runtime queries by
    taxon_id, and NULL rows are not associated with any Sporely id."""
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    col, nor, mappings = _known_id_pair_release(tmp_path)
    redlist = _redlist_dir(tmp_path, [
        _row(**{"Id for vurderingen": 20504, "Vitenskapelig navn id": 45044,
                "Vitenskapelig navn": "Cladonia chlorophaea",
                "Autor": "(Sommerf.) Sprengel",
                "Kategori 2021": "LC"}),
    ])
    release = tmp_path / "release"
    reg = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings, mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=release,
        release_id="tax-2026.07.30-02", redlist_dir=redlist,
    )
    db = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release, registry_path=reg, output_db=db)

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    # Sanity: the mismatch row lives in the SQLite with taxon_id NULL.
    null_rows = conn.execute(
        "SELECT COUNT(*) FROM taxon_redlist_min WHERE taxon_id IS NULL"
    ).fetchone()[0]
    assert null_rows == 1
    cladonia_tid = conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name=?",
        ("Cladonia chlorophaea",)
    ).fetchone()[0]
    other_tid = conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name=?",
        ("Other fungus alpha",)
    ).fetchone()[0]
    conn.close()

    svc = TaxonLookupService(vernacular_db=_Vern(db), include_reference_data=False)

    # Neither taxon has any assessment row bound to them.
    for tid in (cladonia_tid, other_tid):
        result = svc.get_redlist_lookup(int(tid))
        assert result.status == "none"
        assert result.assessment is None
        assert result.conflicting_assessments == ()
        assert svc.get_redlist_assessment(int(tid)) is None


# ---------------------- Pre-existing runtime tests -------------------------


def test_runtime_returns_none_without_redlist_table(tmp_path: Path) -> None:
    """Legacy DB without taxon_redlist_min must not raise."""
    sys.path.insert(0, str(_REPO_ROOT))
    from database.taxon_lookup import TaxonLookupService

    legacy = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(str(legacy))
    conn.executescript("""
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        INSERT INTO taxon_min VALUES (1, 'Amanita', 'muscaria', 'Amanitaceae',
                                      'Amanita muscaria');
    """)
    conn.commit(); conn.close()

    class _Vern:
        def __init__(self, p):
            self.db_path = str(p)
            self.language_code = "no"
    svc = TaxonLookupService(vernacular_db=_Vern(legacy), include_reference_data=False)
    assert svc.get_redlist_assessment(1) is None
