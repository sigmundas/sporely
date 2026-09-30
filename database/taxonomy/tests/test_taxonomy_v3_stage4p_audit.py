"""Taxonomy v3 Stage 4P evidence audit: candidate pairing and review classes.

The audit only prepares review. These tests pin the parts a reviewer relies
on: a name suggests at most one candidate per basis and never decides a pair,
two Dyntaxa concepts claiming one COL concept are kept out of the matched
manifests, Dyntaxa's misapplied and pro parte usages never count as synonymy,
and Dyntaxa's own status vocabulary reaches the weak-synonym tests.
"""
from __future__ import annotations

import importlib.util
import sys
import zipfile
from pathlib import Path

_TAXONOMY = Path(__file__).resolve().parents[1]
_SCRIPT = _TAXONOMY / "evidence" / "taxonomy-v3" / "audit_stage4p_dyntaxa.py"
_spec = importlib.util.spec_from_file_location("audit_stage4p_dyntaxa", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = audit
_spec.loader.exec_module(audit)

from cross_reference_evidence import read_dyntaxa  # noqa: E402

_T = "urn:lsid:dyntaxa.se:Taxon:"
_N = "urn:lsid:dyntaxa.se:TaxonName:"
_HEADER = ["taxonId", "acceptedNameUsageID", "parentNameUsageID",
           "scientificName", "taxonRank", "scientificNameAuthorship",
           "taxonomicStatus", "nomenclaturalStatus", "taxonRemarks", "kingdom"]


def _archive(path: Path, rows: list[list[str]]) -> Path:
    body = "\n".join("\t".join(r) for r in [_HEADER, *rows]) + "\n"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("Taxon.csv", "﻿" + body)
    return path


def _row(taxon_id, accepted, name, authorship, status, nomenclatural="valid",
         rank="species", kingdom="Fungi"):
    return [taxon_id, accepted, "", name, rank, authorship, status,
            nomenclatural, "", kingdom]


def test_reader_excludes_misapplied_and_pro_parte_usages(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "d.zip", [
        _row(f"{_T}1", f"{_T}1", "Pholiotina rugosa", "(Peck) Singer", "accepted"),
        _row(f"{_N}2", f"{_T}1", "Conocybe rugosa", "(Peck) Watling",
             "homotypicSynonym"),
        _row(f"{_N}3", f"{_T}1", "Pholiotina blattaria", "auct.", "misapplied"),
        _row(f"{_N}4", f"{_T}1", "Pholiota togularis", "(Bull.) P.Kumm.",
             "proParteSynonym"),
        _row(f"{_T}9", f"{_T}9", "Picea abies", "(L.) H.Karst.", "accepted",
             kingdom="Plantae"),
    ])
    concepts = read_dyntaxa(archive, kingdom="Fungi")
    assert set(concepts) == {f"{_T}1"}
    concept = concepts[f"{_T}1"]
    assert concept["accepted"] == ("pholiotina rugosa", "(Peck) Singer")
    assert concept["synonyms"] == {("conocybe rugosa", "(Peck) Watling")}
    assert concept["excluded_usage_count"] == 2


def _concept(name, rank="species", synonyms=()):
    return {"accepted": (name.casefold(), ""), "scientific_name": name,
            "authorship": "", "rank": rank,
            "synonym_usages": {(s.casefold(), ""): [{"rank": rank}]
                               for s in synonyms}}


def test_pairing_prefers_the_accepted_name_and_isolates_competing_claims(
) -> None:
    index = {("conocybe rugosa", "species"): [("5ZT3G", 83668)],
             ("inocybe ambigua", "species"): [("HOM-1", 1), ("HOM-2", 2)],
             ("entoloma conferendum", "species"): [("39ZCL", 7821)]}
    dyntaxa = {
        # No COL concept by its accepted name; found through its synonym.
        f"{_T}1": _concept("Pholiotina rugosa", synonyms=["Conocybe rugosa"]),
        f"{_T}2": _concept("Inocybe ambigua"),
        f"{_T}3": _concept("Entoloma conferendum"),
        # A second Dyntaxa concept that also points at 7821.
        f"{_T}4": _concept("Nolanea conferenda",
                           synonyms=["Entoloma conferendum"]),
        f"{_T}5": _concept("Cortinarius suecicus"),
    }
    partition = audit.pair_concepts(dyntaxa, index)

    assert [(r["dyntaxa_taxon_id"], r["candidate_basis"], r["sporely_taxon_id"])
            for r in partition["matched"]] == [
        (f"{_T}1", audit.BASIS_SYNONYM_NAME, 83668)]
    assert [r["dyntaxa_taxon_id"] for r in partition["ambiguous"]] == [f"{_T}2"]
    assert sorted(r["dyntaxa_taxon_id"] for r in partition["not_one_to_one"]) \
        == [f"{_T}3", f"{_T}4"]
    assert [r["dyntaxa_taxon_id"] for r in partition["unmatched"]] == [f"{_T}5"]


def test_dyntaxa_statuses_reach_the_weak_synonym_tests() -> None:
    bridge = {"accepted": ("agaricus campestris", "L."),
              "synonym_status": {("psalliota edulis", "Vittad."): {
                  ("taxonomicStatus", "synonym"),
                  ("nomenclaturalStatus", "orthographia")}}}
    backbone = {"accepted": ("agaricus campestris", "L."), "synonym_status": {},
                "synonym_usages": {("psalliota edulis", "Vittad."): [
                    {"col_usage_id": "U1", "source_id": "2041", "merged": "",
                     "remarks": "", "name_remarks": ""}]}}
    evidence = audit.synonym_evidence(("psalliota edulis", "Vittad."), bridge, backbone)
    assert audit.classify_synonym(evidence)[0] == "orthographic_variant"

    # An otherwise ordinary synonym COL only has from Dyntaxa itself is not
    # independent corroboration.
    bridge["synonym_status"] = {("psalliota edulis", "Vittad."): {
        ("taxonomicStatus", "heterotypicSynonym"),
        ("nomenclaturalStatus", "valid")}}
    derived = audit.synonym_evidence(("psalliota edulis", "Vittad."), bridge, backbone,
                                     frozenset({"2041"}))
    assert audit.classify_synonym(derived)[0] == "dyntaxa_derived"
    independent = audit.synonym_evidence(("psalliota edulis", "Vittad."), bridge, backbone,
                                         frozenset({"2030"}))
    assert audit.classify_synonym(independent)[0] == audit.ORDINARY


def test_review_classes_name_no_nortaxa_rule() -> None:
    assert "only_dyntaxa_derived" in audit.SHARED_REVIEW_CLASSES
    assert not [c for c in audit.SHARED_REVIEW_CLASSES if "nortaxa" in c]
    assert list(audit.SHARED_REVIEW_CLASSES)[-1] == "ordinary"
    for rule in audit.SYNONYM_KINDS.values():
        assert "NorTaxa" not in rule
