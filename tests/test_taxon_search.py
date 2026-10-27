"""Unified taxon search (`database.taxon_search`, `TaxonLookupService.search_taxa`).

A small taxonomy-v2 fixture built to mirror the shapes of the installed
release: cross-source COL/NorTaxa pairs, a same-source homonym, synonyms,
infraspecific names and Norwegian vernaculars. The real release is exercised
in `test_taxon_search_release.py`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from database import taxon_search
from database.taxon_lookup import TaxonLookupService
from database.taxon_search import (
    KIND_SYNONYM,
    KIND_TAXON,
    MATCH_EPITHET,
    MATCH_VERNACULAR,
    MATCH_VERNACULAR_PART,
    TaxonSearchPage,
    search,
)
from database.vernacular_db import VernacularDB

# (taxon_id, genus, epithet, family, canonical name, rank, status, source)
TAXA = [
    (50109, "Flammulina", "", "Physalacriaceae", "Flammulina", "genus", "accepted", "col_xr"),
    (625619, "Flammulina", "", "Physalacriaceae", "Flammulina", "genus", "valid", "nortaxa"),
    (98288, "Flammulina", "velutipes", "Physalacriaceae", "Flammulina velutipes", "species", "accepted", "col_xr"),
    (625621, "Flammulina", "velutipes", "Physalacriaceae", "Flammulina velutipes", "species", "valid", "nortaxa"),
    (98272, "Flammulina", "elastica", "Physalacriaceae", "Flammulina elastica", "species", "accepted", "col_xr"),
    (625622, "Flammulina", "velutipes", "Physalacriaceae", "Flammulina velutipes var. lactea", "variety", "valid", "nortaxa"),
    (80115, "Arrhenia", "velutipes", "Hygrophoraceae", "Arrhenia velutipes", "species", "accepted", "col_xr"),
    (123322, "Russula", "", "Russulaceae", "Russula", "genus", "accepted", "col_xr"),
    (61673, "Russula", "aeruginea", "Russulaceae", "Russula aeruginea", "species", "accepted", "col_xr"),
    (626822, "Russula", "aeruginea", "Russulaceae", "Russula aeruginea", "species", "valid", "nortaxa"),
    (62433, "Russula", "queletii", "Russulaceae", "Russula queletii", "species", "accepted", "col_xr"),
    (626865, "Russula", "queletii", "Russulaceae", "Russula queletii coll.", "species", "valid", "nortaxa"),
    (78915, "Amanita", "muscaria", "Amanitaceae", "Amanita muscaria", "species", "accepted", "col_xr"),
    (700020, "Russula", "variata", "Russulaceae", "Russula variata", "species", "accepted", "col_xr"),
    (700021, "Hygrocybe", "conica", "Hygrophoraceae", "Hygrocybe conica subsp. subconica", "subspecies", "accepted", "col_xr"),
    (92473, "Cortinarius", "albo-ochraceus", "Cortinariaceae", "Cortinarius albo-ochraceus", "species", "accepted", "col_xr"),
    # Same-source homonyms: two COL genera called Aschersonia.
    (980, "Aschersonia", "", "Clavicipitaceae", "Aschersonia", "genus", "accepted", "col_xr"),
    (131914, "Aschersonia", "", "Phanerochaetaceae", "Aschersonia", "genus", "provisionally accepted", "col_xr"),
    # A cross-source pair whose sources disagree on the family.
    (626242, "Cantharellus", "", "Cantharellaceae", "Cantharellus", "genus", "valid", "nortaxa"),
    (129742, "Cantharellus", "", "Hydnaceae", "Cantharellus", "genus", "accepted", "col_xr"),
    # A pair where neither concept has an Artsdatabanken id.
    (700001, "Mycena", "", "Mycenaceae", "Mycena", "genus", "accepted", "col_xr"),
    (700002, "Mycena", "", "Mycenaceae", "Mycena", "genus", "valid", "nortaxa"),
    # Two COL concepts plus a NorTaxa one: no row may offer a choice here.
    (700010, "Inocybe", "rimosa", "Inocybaceae", "Inocybe rimosa", "species", "accepted", "col_xr"),
    (700011, "Inocybe", "rimosa", "Inocybaceae", "Inocybe rimosa", "species", "accepted", "col_xr"),
    (700012, "Inocybe", "rimosa", "Inocybaceae", "Inocybe rimosa", "species", "valid", "nortaxa"),
    # Never offered: higher ranks and placeholders.
    (29515, "", "", "", "Russulales", "order", "accepted", "col_xr"),
    (11, "", "", "", "Incertae sedis", "genus", "accepted", "col_xr"),
    (636240, "", "", "", "BOLD:AEG6437", "unranked", "accepted", "col_xr"),
]

# (taxon_id, alias, is_preferred)
ALIASES = [
    (625621, "Collybia velutipes", 0),
    (98272, "Flammulina velutipes f. longispora", 0),
    (626822, "Russula aeruginosa", 0),
]

# (taxon_id, language, name, preferred)
VERNACULARS = [
    (50109, "nb", "vintersopper", 1), (625619, "nb", "vintersopper", 1),
    (50109, "nn", "vintersoppar", 1), (625619, "nn", "vintersoppar", 1),
    (625621, "nb", "vintersopp", 1),  # only the NorTaxa concept has the name
    (98272, "nb", "langsporet vintersopp", 1),
    (123322, "nb", "kremler", 1), (123322, "nn", "kremler", 1),
    (61673, "nb", "grønnkremle", 1), (626822, "nb", "grønnkremle", 1),
    (62433, "nb", "grantårekremle", 1),
    (626865, "nb", "grantårekremle-gruppen", 1),
    (29515, "nb", "kremleordenen", 1),
    (78915, "nb", "rød fluesopp", 1),
    (78915, "sv", "röd flugsvamp", 1),
    (61673, "sv", "grön kremla", 1),
    (626242, "nb", "kantareller", 1),
]

# (taxon_id, Artsdatabanken id)
ARTSDATABANKEN = [(625619, 54695), (625621, 54701), (626822, 57677), (123322, 57666), (626242, 41000)]


def build_db(path: Path, *, v2: bool = True, extra_taxa: list[tuple] = ()) -> Path:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE taxon_min (
                taxon_id INTEGER PRIMARY KEY, parent_taxon_id INTEGER,
                genus TEXT NOT NULL, specific_epithet TEXT NOT NULL, family TEXT,
                canonical_scientific_name TEXT, taxon_rank TEXT, taxonomic_status TEXT,
                canonical_source_system TEXT NOT NULL, canonical_external_id TEXT NOT NULL DEFAULT '',
                preferred_scientific_name_no TEXT, preferred_scientific_name_sv TEXT
            );
            CREATE INDEX idx_taxon_canonical_name ON taxon_min(canonical_scientific_name);
            CREATE TABLE scientific_name_min (
                scientific_name_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL, scientific_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0, source TEXT, note TEXT
            );
            CREATE INDEX idx_scientific_name_lookup ON scientific_name_min(language_code, scientific_name);
            CREATE TABLE vernacular_min (
                vernacular_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL, vernacular_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0, source TEXT
            );
            CREATE INDEX idx_vern_lang_name ON vernacular_min(language_code, vernacular_name);
            CREATE TABLE taxon_external_id_min (
                external_id_row_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                source_system TEXT NOT NULL, external_id INTEGER NOT NULL, id_role TEXT NOT NULL,
                is_preferred INTEGER NOT NULL DEFAULT 0, external_name TEXT, note TEXT
            );
            """
        )
        if v2:
            conn.execute("CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT)")
        taxa = list(TAXA) + list(extra_taxa)
        conn.executemany(
            "INSERT INTO taxon_min (taxon_id, genus, specific_epithet, family, canonical_scientific_name, "
            "taxon_rank, taxonomic_status, canonical_source_system) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            taxa,
        )
        # Every canonical name is also a preferred scientific_name_min row, as
        # in the release.
        conn.executemany(
            "INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, is_preferred_name) "
            "VALUES (?, 'sci', ?, ?)",
            [(t[0], t[4], 1) for t in taxa] + ALIASES,
        )
        conn.executemany(
            "INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name) "
            "VALUES (?, ?, ?, ?)",
            VERNACULARS,
        )
        conn.executemany(
            "INSERT INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, is_preferred) "
            "VALUES (?, 'artsdatabanken', ?, 'accepted', 1)",
            ARTSDATABANKEN,
        )
    return path


@pytest.fixture
def db_path(tmp_path) -> Path:
    return build_db(tmp_path / "taxonomy.sqlite3")


@pytest.fixture
def conn(db_path):
    connection = sqlite3.connect(db_path)
    yield connection
    connection.close()


def run(conn, query, **kwargs) -> TaxonSearchPage:
    kwargs.setdefault("language_codes", ("nb",))
    return search(conn, query, **kwargs)


def ids(row) -> list[int]:
    return [c.sporely_taxon_id for c in row.concepts]


def row_named(page, name, *, kind=KIND_TAXON):
    matches = [r for r in page.rows if r.name == name and r.kind == kind]
    assert len(matches) == 1, [(r.name, r.kind, ids(r)) for r in page.rows]
    return matches[0]


# --- one row per name, never an identity merge ------------------------------


def test_cross_source_pair_is_one_row_with_both_concepts(conn):
    page = run(conn, "Flammulina")
    genus = page.rows[0]
    assert (genus.name, genus.rank, genus.tier) == ("Flammulina", "genus", taxon_search.TIER_EXACT)
    # Both concepts stay selectable with their own ids; the one with an
    # Artsdatabanken id is the default.
    assert ids(genus) == [625619, 50109]
    assert genus.has_source_choice
    assert [c.source_name for c in genus.concepts] == ["NorTaxa", "COL"]
    assert genus.concepts[0].artsdatabanken_id == 54695
    assert genus.concepts[1].artsdatabanken_id is None


def test_nortaxa_genus_with_valid_status_is_offered(conn):
    # The old branch's "accepted genus only" filter dropped this concept.
    assert 625619 in ids(run(conn, "Flammulina").rows[0])


def test_without_artsdatabanken_ids_col_is_the_default(conn):
    row = run(conn, "Mycena").rows[0]
    assert ids(row) == [700001, 700002]
    assert row.default_concept.canonical_source_system == "col_xr"


def test_same_source_homonyms_get_separate_rows(conn):
    page = run(conn, "Aschersonia")
    rows = [r for r in page.rows if r.name == "Aschersonia"]
    assert sorted(ids(r)[0] for r in rows) == [980, 131914]
    assert all(len(r.concepts) == 1 and r.shares_name for r in rows)
    assert {r.default_concept.family for r in rows} == {"Clavicipitaceae", "Phanerochaetaceae"}


def test_group_with_a_repeated_source_is_split_completely(conn):
    rows = [r for r in run(conn, "Inocybe rimosa").rows if r.name == "Inocybe rimosa"]
    assert sorted(ids(r)[0] for r in rows) == [700010, 700011, 700012]
    assert all(not r.has_source_choice and r.shares_name for r in rows)


def test_cross_source_pair_keeps_each_concepts_family(conn):
    row = run(conn, "Cantharellus").rows[0]
    assert ids(row) == [626242, 129742]
    assert [c.family for c in row.concepts] == ["Cantharellaceae", "Hydnaceae"]


def test_same_name_sibling_is_offered_when_only_one_concept_matched(conn):
    # Only NorTaxa 625621 carries "vintersopp"; the COL concept is still a choice.
    row = run(conn, "vintersopp").rows[0]
    assert row.name == "Flammulina velutipes"
    assert row.match_kind == MATCH_VERNACULAR and row.tier == taxon_search.TIER_EXACT
    assert ids(row) == [625621, 98288]


def test_aggregate_name_is_its_own_row(conn):
    page = run(conn, "Russula queletii")
    assert ids(row_named(page, "Russula queletii")) == [62433]
    aggregate = row_named(page, "Russula queletii coll.")
    assert aggregate.rank == "aggregate" and ids(aggregate) == [626865]


# --- synonyms ---------------------------------------------------------------


def test_synonym_row_names_its_target_and_binds_that_concept(conn):
    page = run(conn, "Collybia")
    row = row_named(page, "Collybia velutipes", kind=KIND_SYNONYM)
    assert ids(row) == [625621]
    assert row.synonym_of == "Flammulina velutipes"
    concept = row.default_concept
    assert concept.scientific_name == "Collybia velutipes"
    assert concept.canonical_scientific_name == "Flammulina velutipes"
    assert concept.link_kind == "synonym_of_accepted"


def test_infraspecific_alias_of_a_species_is_a_linked_row(conn):
    page = run(conn, "velutipes")
    row = row_named(page, "Flammulina velutipes f. longispora", kind=KIND_SYNONYM)
    assert row.rank == "form"
    assert row.default_concept.link_kind == "linked"
    assert row.synonym_of == "Flammulina elastica"


# --- epithets and word parts ------------------------------------------------


def test_epithet_matches_across_genera_and_infraspecific_names(conn):
    page = run(conn, "velutipes")
    names = [r.name for r in page.rows]
    assert {"Arrhenia velutipes", "Flammulina velutipes", "Flammulina velutipes var. lactea"} <= set(names)
    assert "Collybia velutipes" in names
    assert all(r.match_kind == MATCH_EPITHET for r in page.rows)
    assert ids(row_named(page, "Flammulina velutipes")) == [625621, 98288]


def test_rank_markers_are_not_epithets(conn):
    # "var" must not match every variety through its "var." marker.
    assert [r.name for r in run(conn, "var").rows] == ["Russula variata"]
    assert [r.name for r in run(conn, "subsp").rows] == []
    assert [r.name for r in run(conn, "sub").rows] == ["Hygrocybe conica subsp. subconica"]
    assert [r.name for r in run(conn, "lactea").rows] == ["Flammulina velutipes var. lactea"]


def test_evidence_never_lowers_the_epithet_threshold(conn):
    # Two letters do not search epithets, with or without evidence.
    assert run(conn, "mu", evidence_taxon_ids=[78915]).rows == run(conn, "mu").rows
    assert run(conn, "var", evidence_taxon_ids=[625622]).rows == run(conn, "var").rows


def test_hyphenated_epithet_matches_as_typed(conn):
    assert ids(run(conn, "Cortinarius albo-ochraceus").rows[0]) == [92473]
    assert [r.name for r in run(conn, "albo-och").rows] == ["Cortinarius albo-ochraceus"]


def test_exact_vernacular_comes_first(conn):
    page = run(conn, "rød fluesopp")
    assert [r.name for r in page.rows] == ["Amanita muscaria"]
    assert page.rows[0].tier == taxon_search.TIER_EXACT


def test_vernacular_word_part_finds_species(conn):
    page = run(conn, "fluesopp")
    assert row_named(page, "Amanita muscaria").match_kind == MATCH_VERNACULAR_PART


def test_norwegian_group_name_finds_genus_then_member_species(conn):
    page = run(conn, "kremler")
    assert page.rows[0].name == "Russula" and page.rows[0].tier == taxon_search.TIER_EXACT
    green = row_named(page, "Russula aeruginea")
    assert green.match_kind == MATCH_VERNACULAR_PART and green.vernacular_name == "grønnkremle"
    assert ids(green) == [626822, 61673]
    # Higher ranks never appear, even when their names match.
    assert "Russulales" not in [r.name for r in page.rows]


def test_plural_stem_is_norwegian_only(conn):
    # Swedish "grön kremla" contains the stem "kreml", but the Norwegian
    # plural rule does not apply to Swedish searches.
    assert run(conn, "kremler", language_codes=("sv",)).rows == ()
    assert [r.name for r in run(conn, "kremla", language_codes=("sv",)).rows] == ["Russula aeruginea"]


def test_umbrella_norwegian_prefers_bokmal_for_a_row(conn):
    # "vintersopp" prefixes both nb "vintersopper" and nn "vintersoppar"; the
    # umbrella search shows the Bokmål name, which comes first in its order.
    codes = taxon_search.search_language_codes("no")
    assert codes[:2] == ("nb", "no")
    genus = row_named(run(conn, "vintersopp", language_codes=codes), "Flammulina")
    assert genus.vernacular_name == "vintersopper"
    nynorsk = row_named(run(conn, "vintersoppar", language_codes=codes), "Flammulina")
    assert nynorsk.vernacular_name == "vintersoppar"


def test_placeholders_and_unranked_names_are_never_offered(conn):
    assert run(conn, "Incertae").rows == ()
    assert run(conn, "BOLD").rows == ()


# --- evidence and the current concept ---------------------------------------


def test_evidence_sets_the_default_without_dropping_concepts(conn):
    plain = run(conn, "velutipes")
    proven = run(conn, "velutipes", evidence_taxon_ids=[98288])
    assert len(proven.rows) == len(plain.rows)
    first = proven.rows[0]
    assert first.name == "Flammulina velutipes"
    assert ids(first) == [98288, 625621]
    assert first.default_concept.is_evidence


def test_evidence_that_does_not_match_adds_nothing(conn):
    assert run(conn, "Amanita", evidence_taxon_ids=[98288]).rows == run(conn, "Amanita").rows


def test_current_concept_outranks_evidence_as_default(conn):
    row = row_named(
        run(conn, "Flammulina velutipes", evidence_taxon_ids=[625621], current_taxon_id=98288),
        "Flammulina velutipes",
    )
    assert ids(row) == [98288, 625621]
    assert row.default_concept.is_current


# --- commit data ------------------------------------------------------------


def test_concept_carries_the_scientific_pick_shape(conn):
    concept = run(conn, "Russula aeruginea").rows[0].default_concept
    suggestion = concept.as_scientific_suggestion()
    assert suggestion == {
        "sporely_taxon_id": 626822,
        "scientific_name": "Russula aeruginea",
        "taxon_rank_snapshot": "species",
        "is_canonical": True,
        "canonical_scientific_name": "Russula aeruginea",
        "canonical_rank": "species",
        "canonical_taxonomic_status": "valid",
        "family": "Russulaceae",
        "canonical_source_system": "nortaxa",
        "display_scientific_name": "Russula aeruginea",
        "authorship": None,
        "link_kind": "canonical",
    }


def test_genus_row_parses_for_the_snapshot_commit(conn):
    from database.vernacular_db import parse_scientific_name_snapshot

    concept = run(conn, "Russula").rows[0].default_concept
    assert concept.taxon_rank_snapshot == "genus"
    assert parse_scientific_name_snapshot(concept.scientific_name) == ("Russula", None, "genus")


# --- input edges and paging -------------------------------------------------


@pytest.mark.parametrize("query", ["", " ", "a", "R"])
def test_short_queries_return_nothing(conn, query):
    assert run(conn, query) == TaxonSearchPage()


@pytest.mark.parametrize("query", ["%%", "_u", "[a]", "*a", "100%", "a'b", "kr\\e"])
def test_wildcard_and_quote_characters_are_literal(conn, query):
    assert run(conn, query).rows == ()


def test_pages_concatenate_to_the_full_ranking(tmp_path):
    # Letter-only epithets (the name parser rejects digits), with every
    # tenth name also held by a NorTaxa concept so pages must keep pairs.
    epithets = [a + b + "ensis" for a in "abcdefghij" for b in "abcdefghijklmno"]
    extra = [
        (800000 + i, "Russula", ep, "Russulaceae", f"Russula {ep}", "species", "accepted", "col_xr")
        for i, ep in enumerate(epithets)
    ] + [
        (900000 + i, "Russula", ep, "Russulaceae", f"Russula {ep}", "species", "valid", "nortaxa")
        for i, ep in enumerate(epithets) if i % 10 == 0
    ]
    path = build_db(tmp_path / "big.sqlite3", extra_taxa=extra)
    with sqlite3.connect(path) as connection:
        full = search(connection, "Russula", language_codes=("nb",), limit=1000)
        assert not full.has_more
        collected, offset = [], 0
        while True:
            page = search(connection, "Russula", language_codes=("nb",), offset=offset, limit=7)
            collected.extend(page.rows)
            if not page.has_more:
                break
            offset += 7
    assert [ids(r) for r in collected] == [ids(r) for r in full.rows]
    assert len(full.rows) > 150
    assert sum(r.has_source_choice for r in collected) == 15 + 1  # plus Russula aeruginea


# --- service wrapper --------------------------------------------------------


def make_service(path: Path, language: str = "nb") -> TaxonLookupService:
    db = VernacularDB(path, language_code=language, display_language_code="en")
    return TaxonLookupService(db, language_code=language, include_reference_data=False)


def test_service_searches_the_vernacular_language(db_path):
    service = make_service(db_path, "nb")
    assert service.search_taxa("rød fluesopp").rows[0].name == "Amanita muscaria"
    service.language_code = "sv"
    assert service.search_taxa("rød fluesopp").rows == ()
    assert service.search_taxa("röd flugsvamp").rows[0].name == "Amanita muscaria"


def test_service_without_v2_artifact_returns_empty(tmp_path):
    service = make_service(build_db(tmp_path / "legacy.sqlite3", v2=False))
    assert service.search_taxa("Flammulina") == TaxonSearchPage()


def test_service_without_database_returns_empty():
    assert TaxonLookupService(None, include_reference_data=False).search_taxa("Flammulina") == TaxonSearchPage()


def test_service_cache_respects_evidence_and_current(db_path):
    service = make_service(db_path)
    assert ids(service.search_taxa("Flammulina velutipes").rows[0]) == [625621, 98288]
    assert ids(service.search_taxa("Flammulina velutipes", evidence_taxon_ids=[98288]).rows[0]) == [98288, 625621]
    assert ids(service.search_taxa("Flammulina velutipes", current_taxon_id=98288).rows[0]) == [98288, 625621]
