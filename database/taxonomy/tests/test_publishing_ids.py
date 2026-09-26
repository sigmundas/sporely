"""Reviewed publishing ids: Artportalen overlay, iNaturalist refresh, builder use."""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "database/taxonomy/scripts"))

import artportalen_overlay as ao  # noqa: E402
import publishing_ids as pid  # noqa: E402
import refresh_inaturalist_ids as rin  # noqa: E402


def _candidate(path: Path, concepts, artportalen=(), inaturalist=()) -> Path:
    """concepts: (taxon_id, name, rank, genus[, inaturalist_taxon_id])."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, canonical_scientific_name TEXT,
            taxon_rank TEXT, taxonomic_status TEXT, genus TEXT, family TEXT, inaturalist_taxon_id INTEGER);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT, external_id INTEGER);
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO taxonomy_meta VALUES ('content_release_id', 'tax-2099.01.01-01');
    """)
    for c in concepts:
        taxon_id, name, rank, genus = c[:4]
        conn.execute("INSERT INTO taxon_min VALUES (?, ?, ?, 'accepted', ?, NULL, ?)",
                     (taxon_id, name, rank, genus, c[4] if len(c) > 4 else None))
    conn.executemany("INSERT INTO taxon_external_id_min VALUES (?, 'artportalen', ?)", artportalen)
    conn.executemany("INSERT INTO taxon_external_id_min VALUES (?, 'inaturalist', ?)", inaturalist)
    conn.commit()
    conn.close()
    return path


def _legacy(path: Path, names) -> Path:
    """names: (artportalen_id, artportalen scientific name)."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT, "
                 "external_id INTEGER, external_name TEXT)")
    conn.executemany("INSERT INTO taxon_external_id_min VALUES (-1, 'artportalen', ?, ?)", names)
    conn.commit()
    conn.close()
    return path


# ------------------------------------------------------ Artportalen proposals

@pytest.mark.parametrize("artportalen_name, relation", [
    ("Amanita muscaria", "exact"),
    ("Amanita muscaria s.lat.", "variant"),
    ("Amanita muscaria s. str.", "variant"),
    ("Amanita muscaria agg.", "variant"),
    ("Amanita muscaria var. formosa", None),
    ("Amanita muscaria regalis", None),
    ("Amanita muscarioides", None),
])
def test_name_relation(artportalen_name, relation):
    assert ao.name_relation("Amanita muscaria", artportalen_name) == relation


def test_genus_is_not_a_split_of_its_species():
    assert ao.name_relation("Arthonia", "Arthonia radiata") is None


@pytest.fixture
def proposals(tmp_path):
    candidate = _candidate(tmp_path / "c.sqlite3", [
        (1, "Cantharellus cibarius", "species", "Cantharellus"),
        (2, "Amanita muscaria", "species", "Amanita"),
        (3, "Sclerotium denigrans", "species", "Sclerotium"),
        (4, "Sclerotium denigrans", "species", "Sclerotium"),
        (5, "Cytospora leucostoma", "species", "Cytospora"),
        (6, "Leucostoma persoonii", "species", "Leucostoma"),
        (7, "Cryptosphaeria subcutanea", "species", "Cryptosphaeria"),
        (8, "Cryptosphaeria subcutanea", "species", "Cryptosphaeria"),
        (9, "Russula nobilis", "species", "Russula"),
    ], artportalen=[(6, 6380), (8, 4000), (9, 5000)])
    legacy = _legacy(tmp_path / "l.sqlite3", [
        (3213, "Cantharellus cibarius"),
        (2976, "Amanita muscaria s.lat."), (236537, "Amanita muscaria s.str."),
        (6028364, "Sclerotium denigrans"),
        (6380, "Cytospora leucostoma"),
        (4000, "Cryptosphaeria subcutanea"),
    ])
    return ao.propose(candidate=candidate, legacy_db=legacy)


def test_proposals_classify_every_case(proposals):
    by_id = {e["sporely_taxon_id"]: e["classification"] for e in proposals["entries"]}
    assert by_id == {1: "unique_exact", 2: "split", 3: "ambiguous_homonym",
                     4: "ambiguous_homonym", 5: "ambiguous_id_in_use"}
    counts = proposals["counts"]
    assert counts["proposed"] == 5 and counts["unique_exact"] == 1 and counts["needs_review"] == 4
    assert counts["covered_via_same_name_concept"] == 1  # concept 7: its namesake 8 holds 4000
    assert counts["concepts_lacking_artportalen_id"] == 6  # 1-5 and 7; 6, 8, 9 already have one


def test_amanita_muscaria_split_is_never_guessed(proposals):
    entry = next(e for e in proposals["entries"] if e["scientific_name"] == "Amanita muscaria")
    assert entry["classification"] == "split"
    assert [(c["artportalen_taxon_id"], c["relation"]) for c in entry["candidates"]] == [
        (2976, "variant"), (236537, "variant")]
    overlay, added = ao.accept(proposals=proposals, overlay=ao.empty_overlay(), accepted_by="tester",
                               accepted_on="2099-01-01", classification="unique_exact")
    assert added == 1 and [e["sporely_taxon_id"] for e in overlay["entries"]] == [1]
    assert "Amanita muscaria" in ao.render_markdown(proposals)


def test_review_cases_are_accepted_only_one_by_one_and_only_from_their_candidates(proposals):
    with pytest.raises(ao.OverlayError, match="only unique_exact"):
        ao.accept(proposals=proposals, overlay=ao.empty_overlay(), accepted_by="t",
                  accepted_on="2099-01-01", classification="split")
    with pytest.raises(ao.OverlayError, match="not a candidate"):
        ao.accept(proposals=proposals, overlay=ao.empty_overlay(), accepted_by="t",
                  accepted_on="2099-01-01", decisions=[(2, 3213)])
    overlay, _ = ao.accept(proposals=proposals, overlay=ao.empty_overlay(), accepted_by="t",
                           accepted_on="2099-01-01", decisions=[(2, 2976)], note="s.lat. chosen")
    entry = overlay["entries"][0]
    assert entry["decision"] == "accepted_after_review:split" and entry["note"] == "s.lat. chosen"


def test_overlay_validation(tmp_path):
    good = {"sporely_taxon_id": 1, "scientific_name": "A b", "artportalen_taxon_id": 10,
            "artportalen_scientific_name": "A b", "decision": "d", "accepted_by": "t", "accepted_on": "x"}
    for entries, message in (
        ([good, {**good}], "listed twice"),
        ([good, {**good, "sporely_taxon_id": 2}], "mapped to two concepts"),
        ([{**good, "sporely_taxon_id": 3, "artportalen_taxon_id": 11}, good], "sorted"),
        ([{**good, "accepted_by": ""}], "lacks"),
    ):
        path = tmp_path / "overlay.json"
        path.write_text(json.dumps({**ao.empty_overlay(), "entries": entries}))
        with pytest.raises(ao.OverlayError, match=message):
            ao.load_overlay(path)


def test_committed_overlay_is_valid():
    ao.load_overlay(ao.DEFAULT_OVERLAY)


# ------------------------------------------------------ builder application

def _conn(concepts):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, canonical_scientific_name TEXT, "
                 "inaturalist_taxon_id INTEGER)")
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?, ?)", concepts)
    return conn


OVERLAY_ENTRY = {"sporely_taxon_id": 2, "scientific_name": "Amanita muscaria", "artportalen_taxon_id": 2976,
                 "artportalen_scientific_name": "Amanita muscaria s.lat.",
                 "decision": "accepted_after_review:split", "accepted_by": "t", "accepted_on": "x"}


def test_overlay_rows_are_publishing_metadata():
    rows = [(1, "artportalen", 3213, "accepted", 1, "Cantharellus cibarius", None)]
    added = pid.apply_artportalen_overlay(_conn([(1, "Cantharellus cibarius", None), (2, "Amanita muscaria", None)]),
                                          [OVERLAY_ENTRY], rows)
    assert added == 1
    assert rows[-1] == (2, "artportalen", 2976, "publishing", 1, "Amanita muscaria s.lat.",
                        "reviewed_publishing_overlay:accepted_after_review:split")


@pytest.mark.parametrize("concepts, rows, message", [
    ([(2, "Amanita muscaria s.str.", None)], [], "review it again"),
    ([], [], "not in this release"),
    ([(2, "Amanita muscaria", None), (5, "Amanita regalis", None)],
     [(5, "artportalen", 2976, "accepted", 1, None, None)], "already attached"),
    ([(2, "Amanita muscaria", None)], [(2, "artportalen", 1, "accepted", 1, None, None)], "another Artportalen id"),
])
def test_overlay_fails_closed(concepts, rows, message):
    with pytest.raises(pid.PublishingIdError, match=message):
        pid.apply_artportalen_overlay(_conn(concepts), [OVERLAY_ENTRY], list(rows))


def _refresh(entries):
    return pid.InaturalistRefresh("2099-01-01", tuple(pid.RefreshEntry(*e) for e in entries))


def test_refresh_supersedes_contradictory_legacy_rows_and_sets_the_lookup_id():
    conn = _conn([(2, "Amanita muscaria", None), (3, "Amanita gemmata", None), (4, "Boletus edulis", None)])
    rows = [(2, "inaturalist", 48715, "accepted", 1, None, "legacy_compat:inaturalist"),
            (3, "inaturalist", 48715, "accepted", 1, None, "legacy_compat:inaturalist"),
            (4, "inaturalist", 48701, "accepted", 1, None, "legacy_compat:inaturalist")]
    refresh = _refresh([(2, "Amanita muscaria", 48715, "Amanita muscaria"), (4, "Boletus edulis", None, None)])
    kept, counts = pid.apply_inaturalist_refresh_rows(conn, refresh, rows)
    assert counts == {"resolved": 1, "unresolved": 1, "legacy_rows_superseded": 2}
    assert sorted(r[:3] for r in kept) == [(2, "inaturalist", 48715), (4, "inaturalist", 48701)]
    pid.set_refreshed_inaturalist_columns(conn, refresh)
    assert dict(conn.execute("SELECT taxon_id, inaturalist_taxon_id FROM taxon_min")) == {2: 48715, 3: None, 4: None}


def test_refresh_never_reuses_another_concepts_lookup_id():
    conn = _conn([(2, "Amanita muscaria", None), (3, "Amanita gemmata", 48715)])
    with pytest.raises(pid.PublishingIdError, match="already the lookup id"):
        pid.set_refreshed_inaturalist_columns(conn, _refresh([(2, "Amanita muscaria", 48715, "Amanita muscaria")]))


def test_refresh_file_refuses_one_id_for_two_concepts(tmp_path):
    entry = {"status": "resolved", "inaturalist": {"taxon_id": 1, "name": "A b"}}
    path = tmp_path / "r.json"
    path.write_text(json.dumps({"format": pid.INATURALIST_REFRESH_FORMAT, "entries": [
        {**entry, "sporely_taxon_id": 1, "scientific_name": "A b"},
        {**entry, "sporely_taxon_id": 2, "scientific_name": "A c"}]}))
    with pytest.raises(pid.PublishingIdError, match="two concepts"):
        pid.load_inaturalist_refresh(path)


# ------------------------------------------------------ iNaturalist decisions

CONCEPT = {"sporely_taxon_id": 2, "scientific_name": "Amanita muscaria", "taxon_rank": "species",
           "genus": "Amanita", "family": "Amanitaceae"}
MATCH = {"id": 48715, "name": "Amanita muscaria", "rank": "species", "is_active": True}
DETAIL = {"id": 48715, "name": "Amanita muscaria", "rank": "species", "is_active": True,
          "ancestor_ids": [48460, 47170, 47169, 48715],
          "ancestors": [{"id": 47170, "rank": "kingdom", "name": "Fungi"},
                        {"id": 47169, "rank": "family", "name": "Amanitaceae"},
                        {"id": 48460, "rank": "genus", "name": "Amanita"}]}


@pytest.mark.parametrize("exact, detail, lookup, expected", [
    ([MATCH], DETAIL, {}, ("resolved", None)),
    ([], None, {}, ("unresolved", "no_active_exact_name_match")),
    ([MATCH, {**MATCH, "id": 2}], None, {}, ("unresolved", "multiple_active_exact_name_matches")),
    ([{**MATCH, "rank": "variety"}], DETAIL, {}, ("unresolved", "rank_mismatch:variety")),
    ([MATCH], {**DETAIL, "ancestor_ids": [1, 2]}, {}, ("unresolved", "not_in_kingdom_fungi")),
    ([MATCH], {**DETAIL, "ancestors": [{"id": 9, "rank": "genus", "name": "Amanitopsis"}]}, {},
     ("unresolved", "genus_mismatch:Amanitopsis")),
    ([MATCH], DETAIL, {48715: 3}, ("unresolved", "id_already_lookup_id_of:3")),
])
def test_acceptance_rules(exact, detail, lookup, expected):
    assert rin.decide(CONCEPT, exact, detail, lookup) == expected


def test_refresh_records_evidence_and_provenance(tmp_path):
    candidate = _candidate(tmp_path / "c.sqlite3", [
        (2, "Amanita muscaria", "species", "Amanita"), (3, "Amanita gemmata", "species", "Amanita"),
        (4, "Boletus edulis", "species", "Boletus"), (5, "Boletus pinetorum", "species", "Boletus")],
        inaturalist=[(2, 48715), (3, 48715), (4, 48701), (5, 48701)])
    concepts = rin.affected_concepts(candidate)
    assert [c["sporely_taxon_id"] for c in concepts] == [2, 3, 4, 5]
    responses = {
        "q=Amanita+muscaria": {"results": [MATCH, {**MATCH, "id": 321524, "name": "Amanita muscaria guessowii"}]},
        "q=Amanita+gemmata": {"results": [{"id": 63186, "name": "Amanita gemmata", "rank": "species",
                                           "is_active": True}]},
        "q=Boletus+edulis": {"results": [{**MATCH, "id": 48701, "name": "Boletus edulis"},
                                         {**MATCH, "id": 99, "name": "Boletus edulis"}]},
        "q=Boletus+pinetorum": {"results": []},
    }

    def fetch(url):
        for key, body in responses.items():
            if key in url:
                return 200, json.dumps(body).encode()
        return 200, json.dumps({"results": [DETAIL, {**DETAIL, "id": 63186, "name": "Amanita gemmata"}]}).encode()

    entries = {e["scientific_name"]: e for e in rin.refresh(concepts, fetch)}
    assert entries["Amanita muscaria"]["status"] == "resolved"
    assert entries["Amanita muscaria"]["inaturalist"]["taxon_id"] == 48715
    assert entries["Amanita gemmata"]["status"] == "resolved"
    assert entries["Boletus edulis"]["reason"] == "multiple_active_exact_name_matches"
    assert entries["Boletus pinetorum"]["reason"] == "no_active_exact_name_match"
    assert all(r["response_sha256"] and r["http_status"] == 200
               for e in entries.values() for r in e["requests"])
    document = rin.build_document(list(entries.values()), candidate=candidate, acquired_on="2099-01-01")
    assert document["counts"] == {"concepts": 4, "resolved": 2, "unresolved": 2, "unresolved_by_reason": {
        "multiple_active_exact_name_matches": 1, "no_active_exact_name_match": 1}}
    assert [e["sporely_taxon_id"] for e in document["entries"]] == [2, 3, 4, 5]
