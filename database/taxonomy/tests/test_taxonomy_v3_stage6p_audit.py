"""Taxonomy v3 Stage 6P: the release-delta audit refuses unexplained changes.

A synthetic baseline and candidate release: concept 9 is retired onto
survivor 2 by an approved supersession; concept 3 gains an approved NorTaxa
mapping; concept 4 an approved Dyntaxa one. The correct candidate audits
clean, and each fabricated, lost, misdirected or wrongly inherited row is a
violation.
"""
from __future__ import annotations

import copy
import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = (Path(__file__).resolve().parents[1] / "evidence" / "taxonomy-v3" / "audit_stage6p_release.py")
_spec = importlib.util.spec_from_file_location("audit_stage6p_release", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = audit
_spec.loader.exec_module(audit)

NORTAXA = "nortaxa_taxon_id"
DYNTAXA = "dyntaxa_taxon_id"
REFRESH = "inaturalist_refresh:2026-09-26"

POLICIES = audit.Policies(
    registry={("nortaxa", NORTAXA, "900"): 9, ("nortaxa", NORTAXA, "901"): 9,
              ("nortaxa", NORTAXA, "300"): 3, ("dyntaxa", DYNTAXA, "urn:x:4"): 4},
    approved_usages={("nortaxa", NORTAXA, "300"), ("dyntaxa", DYNTAXA, "urn:x:4")},
    supersessions={9: (2, "sup-9")},
)

TAXON_COLUMNS = ["taxon_id", "canonical_scientific_name", "inaturalist_taxon_id",
                 "sporely_content_release_id", "preferred_scientific_name_no",
                 "preferred_scientific_name_sv"]
PROVENANCE = sorted(audit.NATIONAL_PROVENANCE_COLUMNS)


def _redlist(taxon_id: int, assessment: str = "A1") -> tuple:
    return (taxon_id, "artsdatabanken_redlist", "2021", assessment, "Norge", "nortaxa",
            "artsnavnebase_scientific_name_id", "1", "Gamma nine", None, "species",
            "VU", "VU", 0, None, None, None)


def _baseline() -> dict:
    return {
        "meta": {"content_release_id": "tax-old"},
        "taxa": [
            {"taxon_id": 1, "canonical_scientific_name": "Alpha one", "sporely_content_release_id": "tax-old"},
            {"taxon_id": 2, "canonical_scientific_name": "Beta two", "sporely_content_release_id": "tax-old"},
            {"taxon_id": 3, "canonical_scientific_name": "Epsilon", "sporely_content_release_id": "tax-old"},
            {"taxon_id": 4, "canonical_scientific_name": "Zeta", "sporely_content_release_id": "tax-old"},
            {"taxon_id": 9, "canonical_scientific_name": "Gamma nine", "inaturalist_taxon_id": 555,
             "sporely_content_release_id": "tax-old"},
        ],
        "scientific_name_min": [
            (2, "sci", "Beta two", 1, "col_xr", None),
            (9, "sci", "Gamma nine", 1, "nortaxa", None),
            (9, "sci", "Beta two", 0, "nortaxa", "synonym_of_accepted"),
            (9, "sci", "Delta", 0, "nortaxa", "synonym_of_accepted"),
            (3, "sci", "Epsilon", 0, "nortaxa", "cross_source_automatic_exact"),
        ],
        "vernacular_min": [(9, "nb", "nisse", 1, "nortaxa")],
        "taxon_external_id_min": [
            (9, "artsdatabanken", 900, "accepted", 1, "Gamma nine", None),
            (9, "inaturalist", 555, "accepted", 0, "Gamma nine", REFRESH),
            (3, "artsdatabanken", 300, "accepted", 0, "Epsilon", "cross_source_automatic_exact"),
        ],
        "taxon_redlist_min": [_redlist(9)],
        "taxon_external_id_text_min": [
            (1, "col_xr", "col_usage_id", "C1", "accepted", 1, "Alpha one", None),
            (2, "col_xr", "col_usage_id", "C2", "accepted", 1, "Beta two", None),
        ],
    }


def _candidate() -> dict:
    new = "tax-new"
    return {
        "meta": {"content_release_id": new},
        "taxa": [
            {"taxon_id": 1, "canonical_scientific_name": "Alpha one", "sporely_content_release_id": new},
            {"taxon_id": 2, "canonical_scientific_name": "Beta two", "inaturalist_taxon_id": 555,
             "sporely_content_release_id": new, "preferred_scientific_name_no": "Gamma nine",
             "preferred_scientific_name_no_source_system": "nortaxa",
             "preferred_scientific_name_no_namespace": NORTAXA,
             "preferred_scientific_name_no_external_id": "900"},
            {"taxon_id": 3, "canonical_scientific_name": "Epsilon", "sporely_content_release_id": new,
             "preferred_scientific_name_no": "Epsilon",
             "preferred_scientific_name_no_source_system": "nortaxa",
             "preferred_scientific_name_no_namespace": NORTAXA,
             "preferred_scientific_name_no_external_id": "300"},
            {"taxon_id": 4, "canonical_scientific_name": "Zeta", "sporely_content_release_id": new,
             "preferred_scientific_name_sv": "Zeta",
             "preferred_scientific_name_sv_source_system": "dyntaxa",
             "preferred_scientific_name_sv_namespace": DYNTAXA,
             "preferred_scientific_name_sv_external_id": "urn:x:4"},
        ],
        "scientific_name_min": [
            (2, "sci", "Beta two", 1, "col_xr", None),          # 9's "Beta two" collapses here
            (2, "sci", "Gamma nine", 0, "nortaxa", "reviewed_supersession"),
            (2, "sci", "Delta", 0, "nortaxa", "synonym_of_accepted"),
            (3, "sci", "Epsilon", 0, "nortaxa", "manual_approved_exact"),
            (4, "sci", "Zeta", 0, "dyntaxa", "synonym_of_accepted"),
        ],
        "vernacular_min": [(2, "nb", "nisse", 1, "nortaxa"), (4, "sv", "zetasvamp", 0, "dyntaxa")],
        "taxon_external_id_min": [
            (2, "artsdatabanken", 900, "accepted", 0, "Gamma nine", "reviewed_supersession"),
            (2, "inaturalist", 555, "accepted", 0, "Gamma nine",
             f"{REFRESH};superseded_from:9;supersession:sup-9"),
            (3, "artsdatabanken", 300, "accepted", 0, "Epsilon", "manual_approved_exact"),
        ],
        "taxon_redlist_min": [_redlist(2)],
        "taxon_external_id_text_min": [
            (1, "col_xr", "col_usage_id", "C1", "accepted", 1, "Alpha one", None),
            (2, "col_xr", "col_usage_id", "C2", "accepted", 1, "Beta two", None),
            (2, "nortaxa", NORTAXA, "900", "accepted", 0, "Gamma nine", audit.SUPERSESSION_BRIDGE),
            (2, "nortaxa", NORTAXA, "901", "synonym", 0, "Gamma old", audit.SUPERSESSION_BRIDGE),
            (3, "nortaxa", NORTAXA, "300", "accepted", 0, "Epsilon", audit.MANUAL_BRIDGE),
            (4, "dyntaxa", DYNTAXA, "urn:x:4", "accepted", 0, "Zeta", audit.MANUAL_BRIDGE),
        ],
    }


def _write(path: Path, release: dict, provenance: bool) -> Path:
    columns = TAXON_COLUMNS + (PROVENANCE if provenance else [])
    path.unlink(missing_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE taxonomy_meta (key TEXT, value TEXT)")
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", release["meta"].items())
    conn.execute(f"CREATE TABLE taxon_min ({', '.join(columns)})")
    conn.executemany(f"INSERT INTO taxon_min VALUES ({', '.join('?' for _ in columns)})",
                     [tuple(t.get(c) for c in columns) for t in release["taxa"]])
    tables = dict(audit.CONTENT_COLUMNS, taxon_external_id_text_min=audit.TEXT_ID_COLUMNS)
    for table, cols in tables.items():
        conn.execute(f"CREATE TABLE {table} ({', '.join(cols)})")
        conn.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' for _ in cols)})", release[table])
    conn.commit()
    conn.close()
    return path


def _violations(tmp_path: Path, candidate: dict) -> list[str]:
    base = _write(tmp_path / "base.sqlite3", _baseline(), provenance=False)
    cand = _write(tmp_path / "cand.sqlite3", candidate, provenance=True)
    return audit.audit(base, cand, POLICIES)["violations"]


def _mutated(**changes) -> dict:
    candidate = copy.deepcopy(_candidate())
    for table, change in changes.items():
        candidate[table] = change(candidate[table])
    return candidate


def test_the_correct_candidate_audits_clean(tmp_path: Path) -> None:
    report = audit.audit(_write(tmp_path / "b.sqlite3", _baseline(), False),
                         _write(tmp_path / "c.sqlite3", _candidate(), True), POLICIES)
    assert report["violations"] == []
    names = report["tables"]["scientific_name_min"]
    assert (names["rekeyed_exact"], names["rekeyed_accepted_name"], names["collapsed_into_survivor_name"],
            names["relabelled_automatic_to_approved"]) == (1, 1, 1, 1)
    ids = report["tables"]["taxon_external_id_min"]
    assert (ids["rekeyed_accepted_id"], ids["rekeyed_with_supersession_note"]) == (1, 1)
    assert report["tables"]["vernacular_min"]["national_additions"] == {"dyntaxa/sv": 1}


@pytest.mark.parametrize("label, changes, expected", [
    ("fabricated red-list assessment on the survivor",
     {"taxon_redlist_min": lambda rows: rows + [_redlist(2, "FAKE")]},
     "taxon_redlist_min: added row without an approved relationship"),
    ("retired vernacular lost",
     {"vernacular_min": lambda rows: [r for r in rows if r[2] != "nisse"]},
     "vernacular_min: row of retired concept 9 does not reach its survivor 2"),
    ("retired vernacular misdirected to a kept concept",
     {"vernacular_min": lambda rows: [(1,) + r[1:] if r[2] == "nisse" else r for r in rows]},
     "vernacular_min: row of retired concept 9 does not reach its survivor 2"),
    ("retired red-list row lost",
     {"taxon_redlist_min": lambda rows: []},
     "taxon_redlist_min: row of retired concept 9 does not reach its survivor 2"),
    ("supersession note names another retired concept",
     {"taxon_external_id_min": lambda rows: [
         r[:6] + (f"{REFRESH};superseded_from:8;supersession:sup-9",) if r[1] == "inaturalist" else r
         for r in rows]},
     "taxon_external_id_min: row of retired concept 9 does not reach its survivor 2"),
    ("retired accepted id kept preferred",
     {"taxon_external_id_min": lambda rows: [
         r[:4] + (1,) + r[5:] if r[2] == 900 else r for r in rows]},
     "taxon_external_id_min: row of retired concept 9 does not reach its survivor 2"),
    ("fabricated NorTaxa vernacular on a mapped concept",
     {"vernacular_min": lambda rows: rows + [(3, "nb", "oppdiktet", 0, "nortaxa")]},
     "vernacular_min: added row without an approved relationship"),
    ("Dyntaxa vernacular outside Swedish",
     {"vernacular_min": lambda rows: rows + [(4, "en", "zeta fungus", 0, "dyntaxa")]},
     "vernacular_min: added row without an approved relationship"),
    ("Dyntaxa name on a concept without a Dyntaxa bridge",
     {"scientific_name_min": lambda rows: rows + [(1, "sci", "Alpha dyn", 0, "dyntaxa", None)]},
     "scientific_name_min: added row without an approved relationship"),
    ("kept concept loses a row",
     {"scientific_name_min": lambda rows: [r for r in rows if r[2] != "Epsilon"]},
     "scientific_name_min: removed row on a kept concept"),
    ("bridge without an approved mapping",
     {"taxon_external_id_text_min": lambda rows: rows + [
         (1, "nortaxa", NORTAXA, "100", "accepted", 0, "Alpha one", audit.MANUAL_BRIDGE)]},
     "bridge ('nortaxa', 'nortaxa_taxon_id', '100') has no approved manual mapping"),
    ("supersession bridge on the wrong survivor",
     {"taxon_external_id_text_min": lambda rows: [
         (1,) + r[1:] if r[3] == "901" else r for r in rows]},
     "supersession bridge ('nortaxa', 'nortaxa_taxon_id', '901') on 1"),
])
def test_unexplained_changes_are_violations(tmp_path: Path, label, changes, expected) -> None:
    violations = _violations(tmp_path, _mutated(**changes))
    assert any(v.startswith(expected) for v in violations), (label, violations)


def test_survivor_inherits_only_its_retired_twins_lookup_id(tmp_path: Path) -> None:
    def wrong(taxa):
        return [dict(t, inaturalist_taxon_id=556) if t["taxon_id"] == 2 else t for t in taxa]

    assert "concept 2: inaturalist_taxon_id None -> 556" in _violations(tmp_path, _mutated(taxa=wrong))

    def on_kept(taxa):
        return [dict(t, inaturalist_taxon_id=555) if t["taxon_id"] == 1 else t for t in taxa]

    assert "concept 1: inaturalist_taxon_id None -> 555" in _violations(tmp_path, _mutated(taxa=on_kept))


def test_concept_removed_without_supersession(tmp_path: Path) -> None:
    violations = _violations(tmp_path, _mutated(taxa=lambda taxa: [t for t in taxa if t["taxon_id"] != 1]))
    assert "concept 1 removed without an approved supersession" in violations
