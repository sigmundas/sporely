"""Stage 3A.1 tests: scope closure, cross-source proposer, vernacular join.

Uses tiny synthetic fixtures. Real-data validation lives in the dry-run
report; these tests lock the semantics regardless of upstream data volume.
"""
from __future__ import annotations

import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from compile_release import CompilerError, compile_release  # noqa: E402
from cross_source_mapping import (  # noqa: E402
    BackboneIndex,
    PROPOSAL_AMBIGUOUS,
    PROPOSAL_AUTOMATIC_EXACT,
    PROPOSAL_NATIONAL_ONLY,
    PROPOSAL_REVIEW_PROPOSED,
    classify_bridge_records,
)
from normalize_col_xr import ColNormalizeError, normalize_col_xr  # noqa: E402


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


# ------------- COL synthetic archive builder ------------------------------

_HEADER = "\t".join([
    "col:ID", "col:parentID", "col:status", "col:scientificName",
    "col:authorship", "col:rank", "col:kingdom", "col:genus",
    "col:specificEpithet", "col:family",
])


def _build_synthetic_col_archive(path: Path, rows: list[tuple[str, ...]]) -> None:
    """Rows are tuples in the same order as ``_HEADER``."""
    content = _HEADER + "\n" + "\n".join("\t".join(r) for r in rows) + "\n"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("NameUsage.tsv", content)


def test_col_normalizer_pulls_in_fungi_root_via_ancestor_closure(tmp_path: Path) -> None:
    archive = tmp_path / "archive.zip"
    _build_synthetic_col_archive(archive, [
        # id,parent,status,sci_name,authorship,rank,kingdom,genus,species,family
        ("LIFE", "", "accepted", "Biota", "", "unranked", "", "", "", ""),
        # F is the Fungi root: its OWN kingdom column is empty (matches real COL).
        ("F", "LIFE", "accepted", "Fungi", "", "kingdom", "", "", "", ""),
        ("G", "F", "accepted", "Candolleomyces", "", "genus", "Fungi",
         "Candolleomyces", "", "Psathyrellaceae"),
        ("S", "G", "accepted", "Candolleomyces candolleanus", "(Fr.) …",
         "species", "Fungi", "Candolleomyces", "candolleanus", "Psathyrellaceae"),
        # Sibling non-Fungi kingdom must NOT be pulled in.
        ("ANIMAL", "LIFE", "accepted", "Animalia", "", "kingdom", "", "", "", ""),
        ("A1", "ANIMAL", "accepted", "Homo sapiens", "L.", "species", "Animalia",
         "Homo", "sapiens", "Hominidae"),
    ])
    output = tmp_path / "out"
    report = normalize_col_xr(
        archive_path=archive, output_dir=output,
        source_release={"version": "test", "issued_date": "2026-07-28"},
    )
    ids = {
        json.loads(line)["taxon_id"]["value"]
        for line in (output / "taxa.jsonl").read_text("utf-8").splitlines()
        if line.strip()
    }
    # Fungi root F is included even though kingdom column is empty; ancestor
    # LIFE is included; non-Fungi ANIMAL siblings are NOT.
    assert ids == {"LIFE", "F", "G", "S"}
    # No unresolved parents.
    assert report["reference_gaps"]["orphan_parent_reference_count"] == 0
    assert report["hierarchy_complete"] is True
    assert report["record_counts"]["TaxonAncestor"] == 2  # LIFE, F
    assert report["record_counts"]["TaxonFungi"] == 2     # G, S


def test_col_normalizer_flags_dangling_parents(tmp_path: Path) -> None:
    """A Fungi row pointing at a parent ID that is not present in the archive
    is preserved as an unresolved warning, not invented into an edge."""
    archive = tmp_path / "archive.zip"
    _build_synthetic_col_archive(archive, [
        ("G", "GHOST", "accepted", "Candolleomyces", "", "genus", "Fungi",
         "Candolleomyces", "", ""),
    ])
    report = normalize_col_xr(
        archive_path=archive, output_dir=tmp_path / "out",
        source_release={"version": "test", "issued_date": "2026-07-28"},
    )
    assert report["reference_gaps"]["orphan_parent_reference_count"] == 1
    assert report["hierarchy_complete"] is False


# ------------- Cross-source proposer semantics ----------------------------


def _backbone(records: list[dict]) -> BackboneIndex:
    return BackboneIndex.build(records)


def test_proposer_name_only_never_produces_automatic_exact() -> None:
    backbone = _backbone([
        {"source_code": "col_xr",
         "taxon_id": {"namespace": "col_usage_id", "value": "COL-A"},
         "scientific_name": "Candolleomyces candolleanus", "rank": "species"},
    ])
    proposals = classify_bridge_records(
        bridge_records=[
            {"source_code": "nortaxa",
             "taxon_id": {"namespace": "nortaxa_taxon_id", "value": "taxon:x"},
             "scientific_name": "Candolleomyces candolleanus", "rank": "species"},
        ],
        backbone_index=backbone,
    )
    assert len(proposals) == 1
    assert proposals[0].proposal_class == PROPOSAL_REVIEW_PROPOSED
    assert proposals[0].relationship == "likely_exact"
    assert proposals[0].review_status == "needs_review"
    # Not "automatic_exact" — name equality alone must not merge.
    assert proposals[0].proposal_class != PROPOSAL_AUTOMATIC_EXACT


def test_proposer_flags_homonyms_as_ambiguous() -> None:
    backbone = _backbone([
        {"source_code": "col_xr",
         "taxon_id": {"namespace": "col_usage_id", "value": "COL-1"},
         "scientific_name": "Homonymus fakei", "rank": "species"},
        {"source_code": "col_xr",
         "taxon_id": {"namespace": "col_usage_id", "value": "COL-2"},
         "scientific_name": "Homonymus fakei", "rank": "species"},
    ])
    proposals = classify_bridge_records(
        bridge_records=[
            {"source_code": "nortaxa",
             "taxon_id": {"namespace": "nortaxa_taxon_id", "value": "n:1"},
             "scientific_name": "Homonymus fakei", "rank": "species"},
        ],
        backbone_index=backbone,
    )
    assert proposals[0].proposal_class == PROPOSAL_AMBIGUOUS
    assert proposals[0].target_source_usage is None


def test_proposer_national_only_when_no_backbone_match() -> None:
    proposals = classify_bridge_records(
        bridge_records=[
            {"source_code": "nortaxa",
             "taxon_id": {"namespace": "nortaxa_taxon_id", "value": "n:1"},
             "scientific_name": "Unknownus species", "rank": "species"},
        ],
        backbone_index=_backbone([]),
    )
    assert proposals[0].proposal_class == PROPOSAL_NATIONAL_ONLY


def test_proposer_rank_mismatch_does_not_match() -> None:
    """Same scientific name at different ranks is a homonym-like case and must
    not become a review proposal for a wrong-rank match."""
    backbone = _backbone([
        {"source_code": "col_xr",
         "taxon_id": {"namespace": "col_usage_id", "value": "COL-G"},
         "scientific_name": "Something", "rank": "genus"},
    ])
    proposals = classify_bridge_records(
        bridge_records=[
            {"source_code": "nortaxa",
             "taxon_id": {"namespace": "nortaxa_taxon_id", "value": "n:1"},
             "scientific_name": "Something", "rank": "species"},
        ],
        backbone_index=backbone,
    )
    assert proposals[0].proposal_class == PROPOSAL_NATIONAL_ONLY


# ------------- End-to-end compiler tests ----------------------------------


def _write_source(
    root: Path,
    *,
    source_code: str,
    source_release: dict,
    taxa: list[dict],
    vernacular: list[dict] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    ns_prefix = source_code
    ns = {
        "core_row_id": f"{ns_prefix}_dwc_id",
        "taxon_id": f"{ns_prefix}_taxon_id",
        "accepted_name_usage_id": f"{ns_prefix}_accepted_name_usage_id",
        "parent_name_usage_id": f"{ns_prefix}_parent_name_usage_id",
    } if source_code != "col_xr" else {
        "core_row_id": "col_usage_id", "taxon_id": "col_usage_id",
        "accepted_name_usage_id": "col_usage_id",
        "parent_name_usage_id": "col_usage_id",
    }
    with (root / "taxa.jsonl").open("w", encoding="utf-8") as handle:
        for row in taxa:
            record = {
                "source_code": source_code,
                "source_release": source_release,
                "core_row_id": {"value": row["core_row_id"],
                                "namespace": ns["core_row_id"]},
                "taxon_id": {"value": row["taxon_id"],
                             "namespace": ns["taxon_id"]},
                "accepted_name_usage_id": None,
                "parent_name_usage_id": (
                    {"value": row["parent"], "namespace": ns["parent_name_usage_id"]}
                    if row.get("parent") else None
                ),
                "parent_reference_resolution": row.get("parent_resolution", "absent"),
                "identifier_namespace": ns_prefix,
                "scientific_name": row["scientific_name"],
                "authorship": row.get("authorship", ""),
                "rank": row["rank"],
                "taxonomic_status": row.get("status", "accepted"),
                "external_ids": row.get("external_ids", {}),
                "classification": {
                    "kingdom": row.get("kingdom", "Fungi"),
                    "phylum": "", "class": "", "order": "",
                    "family": row.get("family", ""),
                    "genus": row.get("genus", ""),
                    "specific_epithet": "", "infraspecific_epithet": "",
                },
                "provenance": {"source_code": source_code,
                               "source_release": source_release,
                               "identifier_namespace": ns_prefix,
                               "member": "taxa.tsv", "row_index": 0},
            }
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    if vernacular is not None:
        with (root / "vernacular.jsonl").open("w", encoding="utf-8") as handle:
            for v in vernacular:
                record = {
                    "source_code": source_code,
                    "source_release": source_release,
                    "core_row_id": {"value": v["core_row_id"],
                                    "namespace": ns["core_row_id"]},
                    "language": v["language"],
                    "vernacular_name": v["name"],
                    "is_preferred": v.get("preferred", False),
                    "provenance": {"source_code": source_code,
                                   "source_release": source_release,
                                   "identifier_namespace": ns_prefix,
                                   "member": "vernacular.tsv", "row_index": 0},
                }
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    report = {
        "result": "passed",
        "profile_source_code": source_code,
        "profile_source_release": source_release,
        "record_counts": {
            "Taxon": len(taxa),
            "VernacularName": len(vernacular or []),
        },
        "outputs": {"taxa": "taxa.jsonl"},
        "distribution_imported": False,
        "identifier_namespaces": ns,
        "archive_sha256": hashlib.sha256(
            json.dumps(taxa, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "reference_gaps": {
            "orphan_parent_reference_count": 0,
            "orphan_accepted_reference_count": 0,
            "orphan_parent_reference_samples": [],
            "orphan_accepted_reference_samples": [],
            "sample_bound": 25,
        },
        "hierarchy_complete": True,
        "compiler_ready": True,
    }
    (root / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return root


def _write_mappings(path: Path, mappings: list[dict]) -> Path:
    path.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1", "schema": {},
        "mappings": mappings,
    }, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_compiler_allocates_col_before_nortaxa_even_without_manual_mapping(
        tmp_path: Path) -> None:
    """Explicit source priority: COL row's Sporely IDs come first regardless
    of alphabetical accidents."""
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    nor = _write_source(src / "nortaxa", source_code="nortaxa",
                        source_release={"version": "1.284", "issued_date": "d"},
                        taxa=[{"core_row_id": "row-a", "taxon_id": "n:a",
                               "scientific_name": "Sp x", "rank": "species"}])
    mappings = _write_mappings(tmp_path / "mappings.json", [])
    manifest = compile_release(
        normalized_source_dirs=[nor, col],  # deliberately reverse
        manual_mappings_path=mappings,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "reg.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    rows = [json.loads(line) for line in
            (tmp_path / "release" / "source_usages.jsonl").read_text("utf-8").splitlines()
            if line.strip()]
    col_row = next(r for r in rows if r["source_code"] == "col_xr")
    nor_row = next(r for r in rows if r["source_code"] == "nortaxa")
    # COL always allocated first → smaller sporely_taxon_id.
    assert col_row["sporely_taxon_id"] < nor_row["sporely_taxon_id"]
    # No name-only merging — different IDs.
    assert col_row["sporely_taxon_id"] != nor_row["sporely_taxon_id"]


def test_compiler_emits_cross_source_proposals(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    nor = _write_source(src / "nortaxa", source_code="nortaxa",
                        source_release={"version": "1.284", "issued_date": "d"},
                        taxa=[
                            {"core_row_id": "row-a", "taxon_id": "n:a",
                             "scientific_name": "Sp x", "rank": "species"},
                            {"core_row_id": "row-b", "taxon_id": "n:b",
                             "scientific_name": "Different", "rank": "species"},
                        ])
    mappings = _write_mappings(tmp_path / "mappings.json", [])
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "reg.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    mappings_content = (tmp_path / "release" / "mappings.jsonl").read_text("utf-8")
    proposals = [json.loads(l) for l in mappings_content.splitlines() if l.strip()]
    classes = {p["proposal_class"] for p in proposals if "proposal_class" in p}
    assert PROPOSAL_REVIEW_PROPOSED in classes
    assert PROPOSAL_NATIONAL_ONLY in classes
    # None of them silently applied identity.
    for p in proposals:
        if "identity_applied" in p:
            assert p["identity_applied"] is False


def test_compiler_vernacular_join_and_fail_closed(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    nor = _write_source(
        src / "nortaxa", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "row-a", "taxon_id": "n:a",
               "scientific_name": "Sp x", "rank": "species"}],
        vernacular=[
            {"core_row_id": "row-a", "language": "nb", "name": "test-nb",
             "preferred": True},
            {"core_row_id": "row-a", "language": "nn", "name": "test-nn",
             "preferred": False},
            {"core_row_id": "row-a", "language": "sma", "name": "test-sma",
             "preferred": False},
        ],
    )
    mappings = _write_mappings(tmp_path / "mappings.json", [])
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "reg.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    vern_lines = (tmp_path / "release" / "vernacular.jsonl").read_text("utf-8")
    verns = [json.loads(l) for l in vern_lines.splitlines() if l.strip()]
    assert len(verns) == 3
    langs = {v["language"] for v in verns}
    # Sámi and Bokmål/Nynorsk not collapsed.
    assert {"nb", "nn", "sma"} == langs
    # All resolve to the SAME sporely_taxon_id (NorTaxa row-a).
    assert len({v["sporely_taxon_id"] for v in verns}) == 1


def test_compiler_fails_closed_on_dangling_vernacular(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    nor = _write_source(
        src / "nortaxa", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "row-a", "taxon_id": "n:a",
               "scientific_name": "Sp x", "rank": "species"}],
        vernacular=[
            {"core_row_id": "row-missing", "language": "nb", "name": "orphan",
             "preferred": True},
        ],
    )
    mappings = _write_mappings(tmp_path / "mappings.json", [])
    with pytest.raises(CompilerError, match="does not resolve"):
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=mappings,
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "reg.jsonl",
            output_dir=tmp_path / "release",
            release_id="tax-2026.07.28-01",
        )


def test_compiler_persists_ids_across_registry_reuse(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    mappings = _write_mappings(tmp_path / "mappings.json", [])
    reg = tmp_path / "reg.jsonl"
    compile_release(
        normalized_source_dirs=[col],
        manual_mappings_path=mappings,
        mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=tmp_path / "r1",
        release_id="tax-2026.07.28-01",
    )
    first_ids = {json.loads(l)["source_usage"]["identifier"]:
                 json.loads(l)["sporely_taxon_id"]
                 for l in (tmp_path / "r1" / "source_usages.jsonl").read_text("utf-8").splitlines()
                 if l.strip()}
    # Add NorTaxa in a second run reusing the same registry file.
    nor = _write_source(src / "nortaxa", source_code="nortaxa",
                        source_release={"version": "1.284", "issued_date": "d"},
                        taxa=[{"core_row_id": "row-a", "taxon_id": "n:a",
                               "scientific_name": "Y", "rank": "species"}])
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings,
        mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=tmp_path / "r2",
        release_id="tax-2026.07.29-01",
    )
    second_ids = {json.loads(l)["source_usage"]["identifier"]:
                  json.loads(l)["sporely_taxon_id"]
                  for l in (tmp_path / "r2" / "source_usages.jsonl").read_text("utf-8").splitlines()
                  if l.strip()
                  and json.loads(l)["source_code"] == "col_xr"}
    assert first_ids == second_ids


def test_compiler_deterministic_across_runs_with_vernacular_and_proposals(
        tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _write_source(src / "col_xr", source_code="col_xr",
                        source_release={"version": "v", "issued_date": "d"},
                        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
                               "scientific_name": "Sp x", "rank": "species"}])
    nor = _write_source(
        src / "nortaxa", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "row-a", "taxon_id": "n:a",
               "scientific_name": "Sp x", "rank": "species"}],
        vernacular=[
            {"core_row_id": "row-a", "language": "nb", "name": "nb-1"},
            {"core_row_id": "row-a", "language": "nn", "name": "nn-1"},
        ],
    )
    mappings = _write_mappings(tmp_path / "mappings.json", [])

    def do(run_dir: Path) -> tuple[bytes, ...]:
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=mappings,
            mapping_policy_path=_POLICY_PATH,
            registry_path=run_dir / "reg.jsonl",
            output_dir=run_dir / "release",
            release_id="tax-2026.07.28-01",
        )
        r = run_dir / "release"
        return tuple((r / n).read_bytes() for n in
                     ("taxa.jsonl", "source_usages.jsonl", "mappings.jsonl",
                      "vernacular.jsonl", "diagnostics.json", "manifest.json"))

    assert do(tmp_path / "a") == do(tmp_path / "b")
