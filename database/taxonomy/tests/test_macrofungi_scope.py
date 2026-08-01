from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database.taxonomy.macrofungi_scope import (  # noqa: E402
    ScopeError,
    Taxon,
    evaluate,
    load_policy,
    load_taxa,
    resolve_rules,
)

POLICY = ROOT / "database/taxonomy/policies/global-macrofungi-scope.yml"
SOURCE = Path("/tmp/sporely-tax-w2c-source.sqlite3")
CANDIDATE = ROOT / "database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01"
EVIDENCE = ROOT / "database/taxonomy/evidence/global-macrofungi/desktop-candidate.json"


def test_policy_contract_and_authorized_class_keys() -> None:
    policy = load_policy(POLICY)
    rules = {item["code"]: item for item in policy["rules"]}
    assert rules["exclude_pucciniomycetes"]["col_concept_id"] == "H7"
    assert rules["exclude_ustilaginomycetes"]["col_concept_id"] == "K9"
    assert rules["include_gymnosporangium"]["col_concept_id"] == "4RXL"
    assert rules["include_mycosarcoma_maydis"]["col_concept_id"] == "B24TM"
    assert rules["include_mycosarcoma_maydis"]["searchable_synonyms"][0]["name"] == "Ustilago maydis"


def test_precedence_and_negative_exception_probes_against_pinned_source() -> None:
    if not SOURCE.exists():
        pytest.skip("decompressed pinned source is not present")
    connection = sqlite3.connect(SOURCE)
    taxa, by_col = load_taxa(connection)
    rules = resolve_rules(load_policy(POLICY), by_col)
    result = evaluate(taxa, rules)

    gymnosporangium = by_col["4RXL"]
    gymnosporangium_species = next(item for item in taxa.values() if item.parent_id == gymnosporangium.taxon_id and item.rank == "species")
    other_rust = by_col["78MLW"]  # Puccinia abchazica, pinned accepted species.
    maydis = by_col["B24TM"]
    other_ustilago = by_col["7F2TK"]

    assert result[gymnosporangium.taxon_id]["state"] == "include"
    assert result[gymnosporangium_species.taxon_id]["rule"] == "include_gymnosporangium"
    assert result[other_rust.taxon_id]["rule"] == "exclude_pucciniomycetes"
    assert result[other_rust.taxon_id]["state"] == "exclude"
    assert result[maydis.taxon_id]["rule"] == "include_mycosarcoma_maydis"
    assert result[maydis.taxon_id]["state"] == "include"
    assert result[other_ustilago.taxon_id]["rule"] == "exclude_ustilaginomycetes"
    assert result[other_ustilago.taxon_id]["state"] == "exclude"
    for col_id in ("7C", "622DM", "G4", "BS", "F9"):
        assert result[by_col[col_id].taxon_id]["state"] == "include"
    for col_id in ("JX", "87", "DQ", "J2"):
        assert result[by_col[col_id].taxon_id]["state"] == "review"
    connection.close()


def test_species_and_genus_rules_override_class_rules() -> None:
    taxa = {
        1: Taxon(1, "CLASS", "Class", "class", None, "accepted"),
        2: Taxon(2, "GENUS", "Genus", "genus", 1, "accepted"),
        3: Taxon(3, "SPECIES", "Genus species", "species", 2, "accepted"),
    }
    raw = {
        "format": "sporely-global-macrofungi-policy-v1",
        "source": {},
        "default_state": "exclude",
        "precedence": ["species", "genus", "family", "order", "class", "subphylum"],
        "rules": [
            {"code": "class", "col_concept_id": "CLASS", "name": "Class", "rank": "class", "state": "exclude", "reason": "x", "review_status": "approved", "evidence": "x"},
            {"code": "genus", "col_concept_id": "GENUS", "name": "Genus", "rank": "genus", "state": "include", "reason": "x", "review_status": "approved", "evidence": "x"},
            {"code": "species", "col_concept_id": "SPECIES", "name": "Genus species", "rank": "species", "state": "exclude", "reason": "x", "review_status": "approved", "evidence": "x"},
        ],
    }
    rules = resolve_rules(raw, {item.col_id: item for item in taxa.values()})
    result = evaluate(taxa, rules)
    assert result[1]["rule"] == "class"
    assert result[2]["rule"] == "genus"
    assert result[3]["rule"] == "species"


def test_sequence_cluster_exclusion_overrides_broad_class_include() -> None:
    taxa = {
        1: Taxon(1, "CLASS", "Class", "class", None, "accepted"),
        2: Taxon(2, "SH123.10FU", "SH123.10FU", "unranked", 1, "accepted"),
    }
    raw_rule = {
        "code": "class",
        "col_concept_id": "CLASS",
        "name": "Class",
        "rank": "class",
        "state": "include",
        "reason": "macrofruitbody_class",
        "review_status": "approved",
        "evidence": "x",
    }
    exclusions = [{"code": "sequences", "reason": "environmental_sequence_exclusion", "review_status": "approved", "criteria": {"rank": "unranked", "col_id_prefix": "SH", "name_equals_col_id": True}}]
    rules = resolve_rules({"rules": [raw_rule]}, {item.col_id: item for item in taxa.values()})
    result = evaluate(taxa, rules, exclusions)
    assert result[1]["state"] == "include"
    assert result[2]["state"] == "exclude"
    assert result[2]["rule"] == "sequences"


def test_same_rank_conflict_is_rejected() -> None:
    taxon = Taxon(1, "X", "Example", "genus", None, "accepted")
    policy = {
        "rules": [
            {"code": "one", "col_concept_id": "X", "name": "Example", "rank": "genus", "state": "include", "reason": "x", "review_status": "approved", "evidence": "x"},
            {"code": "two", "col_concept_id": "X", "name": "Example", "rank": "genus", "state": "exclude", "reason": "x", "review_status": "approved", "evidence": "x"},
        ]
    }
    with pytest.raises(ScopeError, match="conflicting same-rank rules"):
        resolve_rules(policy, {"X": taxon})


def test_policy_contains_no_synthetic_subphylum_rules() -> None:
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    assert all(item["rank"] != "subphylum" for item in policy["rules"])
    assert all(item["name"] != "Pucciniomycotina" for item in policy["rules"])
    assert all(item["name"] != "Ustilaginomycotina" for item in policy["rules"])


def test_generated_candidate_preserves_maydis_synonym_without_duplicate_identity() -> None:
    if not CANDIDATE.is_dir():
        pytest.skip("generated Phase-A candidate is not present")
    taxa = [json.loads(line) for line in (CANDIDATE / "taxon.jsonl").read_text(encoding="utf-8").splitlines()]
    names = [json.loads(line) for line in (CANDIDATE / "scientific_name.jsonl").read_text(encoding="utf-8").splitlines()]
    accepted = [row for row in taxa if row["canonical_external_id"] == "B24TM"]
    synonym = [row for row in names if row["scientific_name"] == "Ustilago maydis"]
    assert len(accepted) == 1
    assert len(synonym) == 1
    assert synonym[0]["taxon_id"] == accepted[0]["taxon_id"]
    assert not [row for row in taxa if row["canonical_scientific_name"] == "Ustilago maydis"]


def test_generated_candidate_has_zero_selectable_non_fungi_and_passed_contract() -> None:
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    assert evidence["selectable_validation"] == {"animals": 0, "non_fungi": 0, "plants": 0}
    assert evidence["export_contract_validation"]["status"] == "passed"
    assert evidence["correctness_probes"]["Saccharomyces cerevisiae"]["actual"] == "exclude"
    assert evidence["correctness_probes"]["Aspergillus fumigatus"]["actual"] == "exclude"
    assert evidence["correctness_probes"]["Cladonia rangiferina"]["actual"] == "exclude"
