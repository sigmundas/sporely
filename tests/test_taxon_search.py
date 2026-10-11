"""Unified taxon search (`TaxonLookupService.search_taxa`) — Stage 1 of
`docs/plans/active/2026-10-11-unified-taxon-search.md`."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from database.taxon_lookup import TaxonLookupService, TaxonSearchResult
from database.vernacular_db import VernacularDB


def _seed(db_path: Path, *, v2: bool = True) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE taxon_min (
                taxon_id INTEGER PRIMARY KEY,
                genus TEXT, specific_epithet TEXT, family TEXT,
                canonical_scientific_name TEXT,
                taxon_rank TEXT, taxonomic_status TEXT,
                canonical_source_system TEXT
            );
            CREATE TABLE scientific_name_min (
                scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE vernacular_min (
                vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL,
                vernacular_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        if v2:
            conn.execute("CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (1, "Cantharellus", "cibarius", "Cantharellaceae",
                 "Cantharellus cibarius", "species", "accepted", "col_xr"),
                (2, "Russula", "emetica", "Russulaceae",
                 "Russula emetica", "species", "accepted", "col_xr"),
                (3, "Craterellus", "tubaeformis", "Cantharellaceae",
                 "Craterellus tubaeformis", "species", "accepted", "col_xr"),
                (4, "Cantharellus", "amethysteus", "Cantharellaceae",
                 "Cantharellus amethysteus", "species", "accepted", "col_xr"),
                # Genus rank is outside the picker whitelist.
                (5, "Cantharellus", None, "Cantharellaceae",
                 "Cantharellus", "genus", "accepted", "col_xr"),
            ],
        )
        conn.executemany(
            "INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, is_preferred_name) "
            "VALUES (?, 'sci', ?, ?)",
            [
                (1, "Cantharellus cibarius", 1),
                (2, "Russula emetica", 1),
                (3, "Craterellus tubaeformis", 1),
                (3, "Cantharellus tubaeformis", 0),
                (4, "Cantharellus amethysteus", 1),
            ],
        )
        conn.executemany(
            "INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name) "
            "VALUES (?, ?, ?, ?)",
            [
                (1, "en", "Chanterelle", 1),
                (1, "nb", "Kantarell", 1),
                (2, "en", "Russula sickener", 1),
                (2, "nb", "Giftkremle", 1),
                (3, "en", "Winter chanterelle", 1),
                (3, "nb", "Traktkantarell", 1),
                (4, "nb", "Kantarellsopp", 0),
                (5, "nb", "Kantareller", 1),
            ],
        )


def _service(tmp_path: Path, *, language: str = "en", v2: bool = True) -> TaxonLookupService:
    db_path = tmp_path / "taxonomy.sqlite"
    _seed(db_path, v2=v2)
    db = VernacularDB(db_path, language_code=language, display_language_code="en")
    return TaxonLookupService(db, reference_db_factory=lambda: None, language_code=language)


def _ids(results: list[TaxonSearchResult]) -> list[int]:
    return [r.sporely_taxon_id for r in results]


def test_vernacular_prefix_hit_carries_committable_row(tmp_path: Path) -> None:
    results = _service(tmp_path).search_taxa("chant")
    assert _ids(results) == [1]
    hit = results[0]
    assert hit.matched_on == "vernacular"
    assert hit.vernacular_name == "Chanterelle"
    assert hit.vernacular_language_code == "en"
    assert hit.scientific_name == "Cantharellus cibarius"
    assert (hit.genus, hit.species, hit.rank) == ("Cantharellus", "cibarius", "species")
    assert hit.family == "Cantharellaceae"
    assert hit.link_kind == "canonical"
    assert hit.display_label == "Chanterelle (Cantharellus cibarius)"
    suggestion = hit.as_scientific_suggestion()
    assert suggestion["sporely_taxon_id"] == 1
    assert suggestion["taxon_rank_snapshot"] == "species"
    assert suggestion["canonical_scientific_name"] == "Cantharellus cibarius"


def test_scientific_prefix_hit_is_case_insensitive_and_labelled_with_vernacular(tmp_path: Path) -> None:
    service = _service(tmp_path)
    for query in ("Cantharellus ci", "cantharellus ci", "CANTHARELLUS CI"):
        results = service.search_taxa(query)
        assert _ids(results) == [1], query
        assert results[0].matched_on == "scientific"
        assert results[0].display_label == "Chanterelle (Cantharellus cibarius)"


def test_scientific_variant_hit_writes_the_variant_name(tmp_path: Path) -> None:
    results = _service(tmp_path).search_taxa("Cantharellus tub")
    assert _ids(results) == [3]
    hit = results[0]
    assert hit.matched_on == "scientific"
    assert hit.scientific_name == "Cantharellus tubaeformis"
    assert hit.canonical_scientific_name == "Craterellus tubaeformis"
    assert hit.link_kind == "synonym_of_accepted"
    assert hit.display_label == "Winter chanterelle (Cantharellus tubaeformis)"


def test_taxon_matching_both_ways_appears_once(tmp_path: Path) -> None:
    results = _service(tmp_path).search_taxa("Russula")
    assert _ids(results) == [2]
    assert results[0].matched_on == "scientific"
    assert results[0].display_label == "Russula sickener (Russula emetica)"


def test_exact_match_ranks_before_prefix_matches(tmp_path: Path) -> None:
    results = _service(tmp_path, language="nb").search_taxa("kantarell")
    # Kantarell (exact) before Kantarellsopp; the genus-rank Kantareller is
    # outside the picker whitelist.
    assert _ids(results) == [1, 4]
    assert results[0].matched_text == "Kantarell"
    assert [r.matched_on for r in results] == ["vernacular", "vernacular"]


def test_fewer_than_two_characters_returns_nothing(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.search_taxa("") == []
    assert service.search_taxa("C") == []
    assert service.search_taxa("  C  ") == []


def test_language_switch_changes_vernacular_hits_and_labels(tmp_path: Path) -> None:
    service = _service(tmp_path, language="en")
    assert service.search_taxa("Kantarell") == []
    assert service.search_taxa("Cantharellus cib")[0].display_label == "Chanterelle (Cantharellus cibarius)"

    service.language_code = "nb"
    assert _ids(service.search_taxa("Kantarell")) == [1, 4]
    assert service.search_taxa("Chant") == []
    hit = service.search_taxa("Cantharellus cib")[0]
    assert hit.display_label == "Kantarell (Cantharellus cibarius)"
    assert hit.vernacular_language_code == "nb"


def test_results_are_cached_until_clear_cache(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.search_taxa("Chant")
    assert first
    with sqlite3.connect(service.vernacular_db.db_path) as conn:
        conn.execute("DELETE FROM vernacular_min")  # a re-query would now miss
    assert service.search_taxa("Chant") == first
    service.clear_cache()
    assert service.search_taxa("Chant") == []


def test_limit_caps_results(tmp_path: Path) -> None:
    assert len(_service(tmp_path).search_taxa("Cantharellus", limit=1)) == 1


def test_without_taxonomy_v2_db_returns_empty_without_raising(tmp_path: Path) -> None:
    assert TaxonLookupService(None, reference_db_factory=lambda: None).search_taxa("Chant") == []
    legacy = _service(tmp_path, v2=False)
    assert legacy.search_taxa("Chant") == []
    assert legacy.search_taxa("Cantharellus ci") == []
    missing = TaxonLookupService(
        VernacularDB(tmp_path / "missing" / "none.sqlite", language_code="en", display_language_code="en"),
        reference_db_factory=lambda: None,
    )
    assert missing.search_taxa("Chant") == []
