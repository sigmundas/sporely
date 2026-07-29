"""Stage 3A.2 tests: fungal scope for bridge sources, conservative exact
rule, canonical-vs-usage output split, vernacular resolves through alias.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from compile_release import CompilerError, compile_release  # noqa: E402
from cross_source_mapping import (  # noqa: E402
    BackboneIndex, PROPOSAL_AMBIGUOUS, PROPOSAL_AUTOMATIC_EXACT,
    PROPOSAL_NATIONAL_ONLY, PROPOSAL_REVIEW_PROPOSED,
    classify_bridge_records,
)


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


def _write_source(
    root: Path,
    *,
    source_code: str,
    source_release: dict,
    taxa: list[dict],
    vernacular: list[dict] | None = None,
) -> Path:
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
    with (root / "taxa.jsonl").open("w", encoding="utf-8") as h:
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
                "identifier_namespace": source_code,
                "scientific_name": row["scientific_name"],
                "authorship": row.get("authorship", ""),
                "rank": row.get("rank", "species"),
                "taxonomic_status": row.get("status", "accepted"),
                "external_ids": row.get("external_ids", {}),
                "classification": {
                    "kingdom": row.get("kingdom", ""),
                    "phylum": "", "class": "", "order": "",
                    "family": "", "genus": "",
                    "specific_epithet": "", "infraspecific_epithet": "",
                },
                "provenance": {"member": "taxa.tsv", "row_index": 0,
                               "source_code": source_code,
                               "source_release": source_release,
                               "identifier_namespace": source_code},
            }
            h.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    if vernacular:
        with (root / "vernacular.jsonl").open("w", encoding="utf-8") as h:
            for v in vernacular:
                record = {
                    "source_code": source_code,
                    "source_release": source_release,
                    "core_row_id": {"value": v["core_row_id"],
                                    "namespace": ns["core_row_id"]},
                    "language": v["language"],
                    "vernacular_name": v["name"],
                    "is_preferred": v.get("preferred", False),
                    "provenance": {"member": "vern.tsv", "row_index": 0,
                                   "source_code": source_code,
                                   "source_release": source_release,
                                   "identifier_namespace": source_code},
                }
                h.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    report = {
        "result": "passed",
        "profile_source_code": source_code,
        "profile_source_release": source_release,
        "record_counts": {"Taxon": len(taxa),
                          "VernacularName": len(vernacular or [])},
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


def _mappings(path: Path) -> Path:
    path.write_text(json.dumps({
        "format": "sporely-taxonomy-manual-mappings-v1",
        "schema": {}, "mappings": [],
    }, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line)
            for line in path.read_text("utf-8").splitlines() if line.strip()]


def _compile(tmp: Path, sources: list[Path], *, release: str = "tax-2026.07.28-01") -> Path:
    out = tmp / "release"
    compile_release(
        normalized_source_dirs=sources,
        manual_mappings_path=_mappings(tmp / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp / "registry.jsonl",
        output_dir=out,
        release_id=release,
    )
    return out


# ---------------------------------------------------------------- SCOPE ----


def test_bridge_scope_drops_non_fungi_kingdoms(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[
            {"core_row_id": "COL-A", "taxon_id": "COL-A",
             "scientific_name": "Sp fungi", "kingdom": "Fungi"},
        ],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-fungi", "taxon_id": "nt:fungi",
             "scientific_name": "Sp fungi", "kingdom": "Fungi"},
            # These MUST be dropped by fungal scope:
            {"core_row_id": "n-plant", "taxon_id": "nt:plant",
             "scientific_name": "Plantus norvegicus", "kingdom": "Plantae"},
            {"core_row_id": "n-anim",  "taxon_id": "nt:anim",
             "scientific_name": "Animalus norvegicus", "kingdom": "Animalia"},
        ],
        vernacular=[
            {"core_row_id": "n-fungi", "language": "nb", "name": "sopp"},
            {"core_row_id": "n-plant", "language": "nb", "name": "plante"},
            {"core_row_id": "n-anim",  "language": "nb", "name": "dyr"},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    usages = _read_jsonl(out / "source_usages.jsonl")
    nortaxa_usages = [u for u in usages if u["source_code"] == "nortaxa"]
    # Only the fungal NorTaxa row survives.
    assert {u["source_usage"]["identifier"] for u in nortaxa_usages} == {"nt:fungi"}
    verns = _read_jsonl(out / "vernacular.jsonl")
    # Plant/animal vernaculars silently dropped as out-of-scope.
    assert {v["vernacular_name"] for v in verns} == {"sopp"}
    diag = json.loads((out / "diagnostics.json").read_text("utf-8"))
    bridges = diag["counts"]["sporely_scope"]["bridges"]["nortaxa"]
    assert bridges["input"] == 3
    assert bridges["kept"] == 1
    assert bridges["dropped_out_of_scope"] == 2
    assert diag["counts"]["vernacular_rows_dropped_out_of_scope"] == 2


def test_bridge_scope_pulls_in_ancestor_of_fungus(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            # Genus row has kingdom Fungi and no explicit parent (root-ish).
            {"core_row_id": "n-g", "taxon_id": "nt:genus",
             "scientific_name": "Candolleomyces", "rank": "genus",
             "kingdom": "Fungi"},
            # Species pulls it in as an ancestor. Species declares kingdom
            # Fungi too.
            {"core_row_id": "n-s", "taxon_id": "nt:sp",
             "parent": "nt:genus", "parent_resolution": "resolved",
             "scientific_name": "Candolleomyces candolleanus",
             "kingdom": "Fungi"},
            # An unrelated Plantae row that MUST NOT be included even though
            # it happens to be in the source.
            {"core_row_id": "n-p", "taxon_id": "nt:plant",
             "scientific_name": "Plantus", "kingdom": "Plantae"},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    diag = json.loads((out / "diagnostics.json").read_text("utf-8"))
    bridges = diag["counts"]["sporely_scope"]["bridges"]["nortaxa"]
    assert bridges["kept"] == 2  # genus + species
    assert bridges["dropped_out_of_scope"] == 1


# --------------------------------------------------------- CONSERVATIVE ----


def test_conservative_exact_requires_authorship(tmp_path: Path) -> None:
    """Same name+rank+kingdom but no authorship → review_proposed, NOT
    auto-alias."""
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Candolleomyces candolleanus",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Candolleomyces candolleanus",
               "kingdom": "Fungi"}],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    # Two canonical taxa (COL + NorTaxa) because auto-merge is refused.
    assert len(taxa) == 2
    proposals = _read_jsonl(out / "mappings.jsonl")
    classes = {p["proposal_class"] for p in proposals}
    assert PROPOSAL_REVIEW_PROPOSED in classes
    assert PROPOSAL_AUTOMATIC_EXACT not in classes


def test_conservative_exact_auto_aliases_when_all_evidence_matches(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Candolleomyces candolleanus",
               "authorship": "(Fr.) D. Wächt. & A. Melzer",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Candolleomyces candolleanus",
               "authorship": "(Fr.) D. Wächt. & A. Melzer",
               "kingdom": "Fungi"}],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    # Automatic-exact folded NorTaxa onto COL: exactly ONE canonical taxon.
    assert len(taxa) == 1
    usages = _read_jsonl(out / "source_usages.jsonl")
    sporely_ids = {u["sporely_taxon_id"] for u in usages}
    assert len(sporely_ids) == 1
    assert {u["source_code"] for u in usages} == {"col_xr", "nortaxa"}
    diag = json.loads((out / "diagnostics.json").read_text("utf-8"))
    assert diag["counts"]["automatic_exact_aliases_applied"] == 1
    assert diag["counts"]["cross_source_proposal_counts"].get(
        PROPOSAL_AUTOMATIC_EXACT) == 1


def test_conservative_exact_rejects_authorship_mismatch(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "Fries 1836",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "Persoon 1801",
               "kingdom": "Fungi"}],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    assert len(taxa) == 2  # not merged
    classes = {p["proposal_class"]
               for p in _read_jsonl(out / "mappings.jsonl")}
    assert PROPOSAL_AUTOMATIC_EXACT not in classes


def test_synonym_with_no_accepted_target_fails_closed(tmp_path: Path) -> None:
    """After Stage 3A.3, a synonym missing acceptedNameUsageID must block
    compilation rather than allocate a Sporely anchor of its own."""
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi", "status": "accepted"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi", "status": "synonym"}],
    )
    with pytest.raises(CompilerError, match="synonym resolution failed"):
        _compile(tmp_path, [col, nor])


def test_homonym_backbone_never_auto_aliases(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[
            {"core_row_id": "COL-1", "taxon_id": "COL-1",
             "scientific_name": "Sp x", "authorship": "A 1900",
             "kingdom": "Fungi"},
            {"core_row_id": "COL-2", "taxon_id": "COL-2",
             "scientific_name": "Sp x", "authorship": "A 1900",
             "kingdom": "Fungi"},
        ],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    out = _compile(tmp_path, [col, nor])
    proposals = _read_jsonl(out / "mappings.jsonl")
    assert any(p["proposal_class"] == PROPOSAL_AMBIGUOUS for p in proposals)
    # Three canonical taxa (two homonymous COL rows + one distinct NorTaxa).
    assert len(_read_jsonl(out / "taxa.jsonl")) == 3


# --------------------------------------------- CANONICAL OUTPUT & VERNS ----


def test_canonical_output_has_no_duplicate_ids_after_alias(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
        vernacular=[{"core_row_id": "n-a", "language": "nb", "name": "test-sopp"}],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    ids = [t["sporely_taxon_id"] for t in taxa]
    assert len(ids) == len(set(ids))  # no duplicates
    assert len(taxa) == 1  # NorTaxa folded onto COL
    vern = _read_jsonl(out / "vernacular.jsonl")
    assert len(vern) == 1
    assert vern[0]["sporely_taxon_id"] == taxa[0]["sporely_taxon_id"]
    # The alias vernacular resolves to the canonical COL identity.
    assert taxa[0]["canonical_source_code"] == "col_xr"


def test_col_ids_unchanged_after_appending_nortaxa(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    registry = tmp_path / "reg.jsonl"
    compile_release(
        normalized_source_dirs=[col],
        manual_mappings_path=_mappings(tmp_path / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry,
        output_dir=tmp_path / "r1",
        release_id="tax-2026.07.28-01",
    )
    r1_taxa = _read_jsonl(tmp_path / "r1" / "taxa.jsonl")
    col_id = next(t["sporely_taxon_id"] for t in r1_taxa
                  if t["canonical_source_code"] == "col_xr")

    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Different", "kingdom": "Fungi"}],
    )
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=_mappings(tmp_path / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry,
        output_dir=tmp_path / "r2",
        release_id="tax-2026.07.29-01",
    )
    r2_taxa = _read_jsonl(tmp_path / "r2" / "taxa.jsonl")
    col_id_2 = next(t["sporely_taxon_id"] for t in r2_taxa
                    if t["canonical_source_code"] == "col_xr")
    assert col_id == col_id_2


def test_output_is_byte_deterministic_across_two_runs(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-a", "taxon_id": "nt:a",
             "scientific_name": "Sp x", "authorship": "A 1900",
             "kingdom": "Fungi"},
            {"core_row_id": "n-b", "taxon_id": "nt:b",
             "scientific_name": "Only nortaxa", "kingdom": "Fungi"},
        ],
        vernacular=[
            {"core_row_id": "n-a", "language": "nb", "name": "sopp"},
            {"core_row_id": "n-b", "language": "sma", "name": "guobmesopp"},
        ],
    )

    def run(dir_: Path) -> tuple[bytes, ...]:
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=_mappings(dir_ / "mappings.json"),
            mapping_policy_path=_POLICY_PATH,
            registry_path=dir_ / "reg.jsonl",
            output_dir=dir_ / "release",
            release_id="tax-2026.07.28-01",
        )
        r = dir_ / "release"
        return tuple((r / n).read_bytes() for n in
                     ("taxa.jsonl", "source_usages.jsonl", "mappings.jsonl",
                      "vernacular.jsonl", "diagnostics.json", "manifest.json"))

    a = tmp_path / "a"; a.mkdir()
    b = tmp_path / "b"; b.mkdir()
    assert run(a) == run(b)


def test_input_order_independence(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )

    def sporely_ids(order: list[Path], dir_: Path) -> dict:
        compile_release(
            normalized_source_dirs=order,
            manual_mappings_path=_mappings(dir_ / "mappings.json"),
            mapping_policy_path=_POLICY_PATH,
            registry_path=dir_ / "reg.jsonl",
            output_dir=dir_ / "release",
            release_id="tax-2026.07.28-01",
        )
        return {u["source_usage"]["identifier"]: u["sporely_taxon_id"]
                for u in _read_jsonl(dir_ / "release" / "source_usages.jsonl")}

    a = tmp_path / "a"; a.mkdir()
    b = tmp_path / "b"; b.mkdir()
    assert sporely_ids([col, nor], a) == sporely_ids([nor, col], b)
