"""Stage 3A.3/3A.4 tests: intra-source synonym resolution + missing-
authorship + classification-agreement automatic-exact rule.
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
    PROPOSAL_AMBIGUOUS, PROPOSAL_AUTOMATIC_EXACT,
    PROPOSAL_NATIONAL_ONLY, PROPOSAL_REVIEW_PROPOSED,
)


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


def _write_source(
    root: Path, *, source_code: str, source_release: dict,
    taxa: list[dict], vernacular: list[dict] | None = None,
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
                "accepted_name_usage_id": (
                    {"value": row["accepted"],
                     "namespace": ns["accepted_name_usage_id"]}
                    if row.get("accepted") else None
                ),
                "parent_name_usage_id": None,
                "parent_reference_resolution": "absent",
                "identifier_namespace": source_code,
                "scientific_name": row["scientific_name"],
                "authorship": row.get("authorship", ""),
                "rank": row.get("rank", "species"),
                "taxonomic_status": row.get("status", "accepted"),
                "external_ids": row.get("external_ids", {}),
                "classification": {
                    "kingdom": row.get("kingdom", "Fungi"),
                    "phylum": row.get("phylum", ""),
                    "class": row.get("class", ""),
                    "order": row.get("order", ""),
                    "family": row.get("family", ""),
                    "genus": row.get("genus", ""),
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


def _compile(tmp: Path, sources: list[Path],
             release: str = "tax-2026.07.28-01") -> Path:
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


# --------------------------------------------- SYNONYM RESOLUTION ---------


def test_synonym_binds_to_accepted_no_new_canonical(tmp_path: Path) -> None:
    """The synonym must NOT get its own canonical taxon; it must alias onto
    the accepted usage's Sporely ID."""
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Different name", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Candolleomyces candolleanus",
             "authorship": "(Fr.) D. Wächt. & A. Melzer",
             "kingdom": "Fungi"},
            {"core_row_id": "n-syn", "taxon_id": "nt:syn",
             "scientific_name": "Psathyrella candolleana",
             "authorship": "(Fr. : Fr.) Maire",
             "status": "synonym", "accepted": "nt:acc",
             "kingdom": "Fungi"},
        ],
        vernacular=[
            {"core_row_id": "n-acc", "language": "nb", "name": "hvit sprøsopp",
             "preferred": True},
            {"core_row_id": "n-syn", "language": "nb", "name": "gammelt navn",
             "preferred": False},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    # 2 canonical: COL-A + NorTaxa accepted. Synonym does NOT add a canonical.
    assert len(taxa) == 2
    usages = _read_jsonl(out / "source_usages.jsonl")
    # 3 usages: COL-A, nt:acc, nt:syn.
    assert len(usages) == 3
    # Synonym and accepted share the same Sporely ID.
    syn = next(u for u in usages if u["source_usage"]["identifier"] == "nt:syn")
    acc = next(u for u in usages if u["source_usage"]["identifier"] == "nt:acc")
    assert syn["sporely_taxon_id"] == acc["sporely_taxon_id"]
    assert syn["identity_binding"] == "alias"
    assert syn["alias_reason"] == "synonym_of_accepted"
    assert syn["accepted_source_usage"]["identifier"] == "nt:acc"
    # Synonym remains a searchable scientific-name alias.
    assert syn["searchable_scientific_name_alias"] == "Psathyrella candolleana"
    assert syn["scientific_name"] == "Psathyrella candolleana"  # original
    assert syn["authorship"] == "(Fr. : Fr.) Maire"  # original
    assert syn["taxonomic_status"] == "synonym"  # original status preserved


def test_vernacular_from_both_synonym_and_accepted_hits_same_sporely_id(
        tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Different", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Accepted name", "kingdom": "Fungi"},
            {"core_row_id": "n-syn", "taxon_id": "nt:syn",
             "scientific_name": "Old name", "status": "synonym",
             "accepted": "nt:acc", "kingdom": "Fungi"},
        ],
        vernacular=[
            {"core_row_id": "n-acc", "language": "nb", "name": "nytt-navn"},
            {"core_row_id": "n-syn", "language": "nb", "name": "gammelt-navn"},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    verns = _read_jsonl(out / "vernacular.jsonl")
    assert len(verns) == 2
    ids = {v["sporely_taxon_id"] for v in verns}
    assert len(ids) == 1  # both point to the same Sporely ID
    names = {v["vernacular_name"] for v in verns}
    assert names == {"nytt-navn", "gammelt-navn"}


def test_synonym_chain_multiple_hops_resolves(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Other", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Accepted", "kingdom": "Fungi"},
            {"core_row_id": "n-m", "taxon_id": "nt:middle",
             "scientific_name": "Middle synonym", "status": "synonym",
             "accepted": "nt:acc", "kingdom": "Fungi"},
            {"core_row_id": "n-s", "taxon_id": "nt:syn",
             "scientific_name": "Outer synonym", "status": "synonym",
             "accepted": "nt:middle", "kingdom": "Fungi"},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    usages = _read_jsonl(out / "source_usages.jsonl")
    ids = {u["source_usage"]["identifier"]: u["sporely_taxon_id"]
           for u in usages if u["source_code"] == "nortaxa"}
    # All three NorTaxa usages resolve to the same Sporely ID (the accepted).
    assert ids["nt:acc"] == ids["nt:middle"] == ids["nt:syn"]


def test_synonym_cycle_fails_closed(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Other", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-a", "taxon_id": "nt:a",
             "scientific_name": "A", "status": "synonym", "accepted": "nt:b",
             "kingdom": "Fungi"},
            {"core_row_id": "n-b", "taxon_id": "nt:b",
             "scientific_name": "B", "status": "synonym", "accepted": "nt:a",
             "kingdom": "Fungi"},
        ],
    )
    with pytest.raises(CompilerError, match="synonym resolution failed"):
        _compile(tmp_path, [col, nor])


def test_synonym_missing_accepted_target_fails_closed(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Other", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-s", "taxon_id": "nt:syn",
               "scientific_name": "Ghost", "status": "synonym",
               "accepted": "nt:ghost", "kingdom": "Fungi"}],
    )
    with pytest.raises(CompilerError, match="synonym resolution failed"):
        _compile(tmp_path, [col, nor])


def test_synonym_self_reference_fails_closed(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Other", "kingdom": "Fungi"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-s", "taxon_id": "nt:syn",
               "scientific_name": "Self", "status": "synonym",
               "accepted": "nt:syn", "kingdom": "Fungi"}],
    )
    with pytest.raises(CompilerError, match="synonym resolution failed"):
        _compile(tmp_path, [col, nor])


def test_synonym_folded_onto_col_via_accepted_alias(tmp_path: Path) -> None:
    """If the accepted NorTaxa row auto-aliases to COL, the synonym must fold
    onto the SAME Sporely identity — one canonical, three usages."""
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
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Candolleomyces candolleanus",
             "authorship": "(Fr.) D. Wächt. & A. Melzer",
             "status": "valid", "kingdom": "Fungi"},
            {"core_row_id": "n-syn", "taxon_id": "nt:syn",
             "scientific_name": "Psathyrella candolleana",
             "status": "synonym", "accepted": "nt:acc",
             "kingdom": "Fungi"},
        ],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    # Exactly ONE canonical (COL-A). NorTaxa accepted and synonym both alias.
    assert len(taxa) == 1
    assert taxa[0]["canonical_source_code"] == "col_xr"
    usages = _read_jsonl(out / "source_usages.jsonl")
    ids = {u["sporely_taxon_id"] for u in usages}
    assert len(ids) == 1
    assert {u["source_code"] for u in usages} == {"col_xr", "nortaxa"}
    # Synonym has both flags.
    syn = next(u for u in usages if u["source_usage"]["identifier"] == "nt:syn")
    assert syn["alias_reason"] == "synonym_of_accepted"


# ---------------- MISSING-AUTHORSHIP + CLASSIFICATION RULE ----------------


def test_missing_authorship_with_classification_agreement_aliases(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp anonymous",
               "kingdom": "Fungi", "family": "Agaricaceae",
               "genus": "Agaricus"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp anonymous",
               "kingdom": "Fungi", "family": "Agaricaceae",
               "genus": "Agaricus"}],
    )
    out = _compile(tmp_path, [col, nor])
    taxa = _read_jsonl(out / "taxa.jsonl")
    assert len(taxa) == 1  # folded via missing-authorship + classification rule
    proposals = _read_jsonl(out / "mappings.jsonl")
    matching = [p for p in proposals
                if p["source_usage"]["identifier"] == "nt:a"]
    assert matching and matching[0]["proposal_class"] == PROPOSAL_AUTOMATIC_EXACT
    assert matching[0]["evidence"]["reason"] == \
        "missing_authorship_classification_rule_satisfied"


def test_missing_authorship_but_classification_disagreement_stays_review(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp anonymous",
               "kingdom": "Fungi", "family": "Agaricaceae"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp anonymous",
               "kingdom": "Fungi", "family": "Russulaceae"}],  # disagrees
    )
    out = _compile(tmp_path, [col, nor])
    assert len(_read_jsonl(out / "taxa.jsonl")) == 2  # no merge
    proposals = _read_jsonl(out / "mappings.jsonl")
    matching = [p for p in proposals
                if p["source_usage"]["identifier"] == "nt:a"]
    assert matching[0]["proposal_class"] == PROPOSAL_REVIEW_PROPOSED


def test_missing_authorship_rule_never_fires_on_authorship_mismatch(tmp_path: Path) -> None:
    """Both sides have authorship but they differ — must NOT auto-alias
    regardless of classification agreement."""
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "Fries 1836",
               "kingdom": "Fungi", "family": "Agaricaceae"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[{"core_row_id": "n-a", "taxon_id": "nt:a",
               "scientific_name": "Sp x", "authorship": "Persoon 1801",
               "kingdom": "Fungi", "family": "Agaricaceae"}],
    )
    out = _compile(tmp_path, [col, nor])
    assert len(_read_jsonl(out / "taxa.jsonl")) == 2  # not merged


def test_deterministic_with_synonyms_and_missing_authorship(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi", "family": "Agaricaceae"}],
    )
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Sp x", "authorship": "A 1900",
             "kingdom": "Fungi", "family": "Agaricaceae"},
            {"core_row_id": "n-syn", "taxon_id": "nt:syn",
             "scientific_name": "Old name", "status": "synonym",
             "accepted": "nt:acc", "kingdom": "Fungi"},
            {"core_row_id": "n-other", "taxon_id": "nt:other",
             "scientific_name": "Missing-auth species",
             "kingdom": "Fungi", "family": "Agaricaceae"},
        ],
        vernacular=[
            {"core_row_id": "n-syn", "language": "nb", "name": "gammel"},
            {"core_row_id": "n-acc", "language": "nn", "name": "ny"},
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


def test_col_ids_unchanged_when_synonym_rich_nortaxa_appended(tmp_path: Path) -> None:
    col = _write_source(
        tmp_path / "col", source_code="col_xr",
        source_release={"version": "v", "issued_date": "d"},
        taxa=[{"core_row_id": "COL-A", "taxon_id": "COL-A",
               "scientific_name": "Sp x", "authorship": "A 1900",
               "kingdom": "Fungi"}],
    )
    reg = tmp_path / "reg.jsonl"
    compile_release(
        normalized_source_dirs=[col],
        manual_mappings_path=_mappings(tmp_path / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=tmp_path / "r1",
        release_id="tax-2026.07.28-01",
    )
    r1 = _read_jsonl(tmp_path / "r1" / "taxa.jsonl")
    col_id = next(t["sporely_taxon_id"] for t in r1
                  if t["canonical_source_code"] == "col_xr")
    # Append NorTaxa with a synonym and a missing-authorship match.
    nor = _write_source(
        tmp_path / "nor", source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "d"},
        taxa=[
            {"core_row_id": "n-acc", "taxon_id": "nt:acc",
             "scientific_name": "Sp x", "authorship": "A 1900",
             "kingdom": "Fungi"},
            {"core_row_id": "n-syn", "taxon_id": "nt:syn",
             "scientific_name": "Old", "status": "synonym",
             "accepted": "nt:acc", "kingdom": "Fungi"},
        ],
    )
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=_mappings(tmp_path / "mappings.json"),
        mapping_policy_path=_POLICY_PATH,
        registry_path=reg, output_dir=tmp_path / "r2",
        release_id="tax-2026.07.29-01",
    )
    r2 = _read_jsonl(tmp_path / "r2" / "taxa.jsonl")
    col_id_2 = next(t["sporely_taxon_id"] for t in r2
                    if t["canonical_source_code"] == "col_xr")
    assert col_id == col_id_2
