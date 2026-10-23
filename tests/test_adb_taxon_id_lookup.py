"""Artsobservasjoner (ADB/NorTaxa) taxon-id lookup must only yield destination ids.

Synthetic taxonomy SQLite fixtures; no network, no real release.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

import utils.vernacular_utils as vernacular_utils
from database.models import ObservationDB


def _make_db(path: Path, taxa, external=(), *, with_norwegian=True) -> Path:
    conn = sqlite3.connect(path)
    nor_col = ", norwegian_taxon_id INTEGER" if with_norwegian else ""
    conn.execute(
        "CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, genus TEXT, "
        f"specific_epithet TEXT, source_system TEXT{nor_col})"
    )
    conn.execute(
        "CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT, "
        "external_id TEXT, id_role TEXT, is_preferred INTEGER)"
    )
    for row in taxa:
        if with_norwegian:
            conn.execute("INSERT INTO taxon_min VALUES (?,?,?,?,?)", row)
        else:
            conn.execute("INSERT INTO taxon_min VALUES (?,?,?,?)", row[:4])
    for row in external:
        conn.execute("INSERT INTO taxon_external_id_min VALUES (?,?,?,?,1)", row)
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def use_db(monkeypatch):
    def _use(path: Path):
        monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda lang_code=None: path)
    return _use


def test_unmapped_taxon_never_returns_internal_sporely_id(tmp_path, use_db):
    # Sporely taxon_id 1 has no NorTaxa id and no artsdatabanken external id.
    use_db(_make_db(tmp_path / "t.sqlite3", [(1, "Cryptosphaerella", "annonae", "col", None)]))
    assert ObservationDB.resolve_adb_taxon_id("Cryptosphaerella", "annonae") is None


def test_unmapped_taxon_without_norwegian_column_never_returns_internal_id(tmp_path, use_db):
    use_db(_make_db(tmp_path / "t.sqlite3", [(7, "Foo", "bar", "col", None)], with_norwegian=False))
    assert ObservationDB.resolve_adb_taxon_id("Foo", "bar") is None


def test_ambiguous_norwegian_ids_return_none(tmp_path, use_db):
    # Species and var. share genus+epithet but carry different NorTaxa ids.
    use_db(_make_db(tmp_path / "t.sqlite3", [
        (624178, "Hygrocybe", "conica", "nortaxa", 11111),
        (625373, "Hygrocybe", "conica", "nortaxa", 53968),
    ]))
    assert ObservationDB.resolve_adb_taxon_id("Hygrocybe", "conica") is None


def test_ambiguous_norwegian_ids_do_not_fall_through_to_external_id(tmp_path, use_db):
    use_db(_make_db(
        tmp_path / "t.sqlite3",
        [(10, "Amb", "sp", "nortaxa", 101), (11, "Amb", "sp", "nortaxa", 102)],
        external=[(10, "artsdatabanken", "101", "accepted")],
    ))
    assert ObservationDB.resolve_adb_taxon_id("Amb", "sp") is None


def test_ambiguous_artsdatabanken_external_ids_return_none(tmp_path, use_db):
    use_db(_make_db(
        tmp_path / "t.sqlite3",
        [(20, "Ext", "amb", "col", None), (21, "Ext", "amb", "col", None)],
        external=[(20, "artsdatabanken", "201", "accepted"), (21, "artsdatabanken", "202", "accepted")],
    ))
    assert ObservationDB.resolve_adb_taxon_id("Ext", "amb") is None


def test_valid_norwegian_taxon_id_unchanged(tmp_path, use_db):
    use_db(_make_db(tmp_path / "t.sqlite3", [(5, "Amanita", "muscaria", "nortaxa", 9999)]))
    assert ObservationDB.resolve_adb_taxon_id("Amanita", "muscaria") == 9999


def test_unique_artsdatabanken_accepted_id_unchanged(tmp_path, use_db):
    use_db(_make_db(
        tmp_path / "t.sqlite3",
        [(30, "Uniq", "ext", "col", None)],
        external=[(30, "artsdatabanken", "3030", "accepted"), (30, "artsdatabanken", "4040", "synonym")],
    ))
    assert ObservationDB.resolve_adb_taxon_id("Uniq", "ext") == 3030


def _tab_stub(picker_calls, picker_result):
    def _picker(name):
        picker_calls.append(name)
        return picker_result
    return SimpleNamespace(
        _extract_artsobs_taxon_pair=lambda obs: (obs["genus"], obs["species"]),
        _resolve_accepted_taxon_pair=lambda g, s: None,
        _lookup_artsobservasjoner_taxon_id=_picker,
    )


@pytest.mark.parametrize("genus,species", [("Cryptosphaerella", "annonae"), ("Hygrocybe", "conica")])
def test_missing_or_ambiguous_mapping_goes_to_picker_search(tmp_path, use_db, genus, species):
    from ui.observations_tab import ObservationsTab

    use_db(_make_db(tmp_path / "t.sqlite3", [
        (1, "Cryptosphaerella", "annonae", "col", None),
        (624178, "Hygrocybe", "conica", "nortaxa", 11111),
        (625373, "Hygrocybe", "conica", "nortaxa", 53968),
    ]))
    calls: list[str] = []
    stub = _tab_stub(calls, 777)
    result = ObservationsTab._resolve_artsobs_taxon_id(stub, {"genus": genus, "species": species})
    assert result == (777, "artsobservasjoner.picker_search")
    assert calls == [f"{genus} {species}"]


def test_mapped_taxon_does_not_reach_picker_search(tmp_path, use_db):
    from ui.observations_tab import ObservationsTab

    use_db(_make_db(tmp_path / "t.sqlite3", [(5, "Amanita", "muscaria", "nortaxa", 9999)]))
    calls: list[str] = []
    result = ObservationsTab._resolve_artsobs_taxon_id(
        _tab_stub(calls, 777), {"genus": "Amanita", "species": "muscaria"}
    )
    assert result == (9999, "taxonomy_db.artsdatabanken")
    assert calls == []


def test_stored_artsobservasjoner_id_still_wins_when_lookup_is_empty():
    from utils.artsobservasjoner_taxon import select_artsobservasjoner_taxon_id

    resolution = select_artsobservasjoner_taxon_id(
        {"genus": "Foo", "species": "bar", "artsobservasjoner_taxon_id": 4242},
        resolved_taxonomy_taxon_id=None,
    )
    assert resolution.taxon_id == 4242
    assert resolution.source_field == "artsobservasjoner_taxon_id"


# --- taxonomy-v2 releases: concept- and rank-aware lookup (R1 regressions) ---

def _make_v2_db(path: Path) -> Path:
    """The three R1 regressions in miniature, with the real ids."""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, genus TEXT, specific_epithet TEXT,
            canonical_scientific_name TEXT, taxon_rank TEXT, norwegian_taxon_id INTEGER);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT, external_id INTEGER,
            id_role TEXT, is_preferred INTEGER, note TEXT);
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER, source_system TEXT, namespace TEXT,
            external_id TEXT, id_role TEXT, is_preferred INTEGER, note TEXT);
        CREATE TABLE scientific_name_min (taxon_id INTEGER, language_code TEXT, scientific_name TEXT);
        CREATE TABLE taxon_supersession_min (retired_taxon_id INTEGER PRIMARY KEY,
            current_taxon_id INTEGER, supersession_id TEXT, content_release_id TEXT);
        INSERT INTO taxon_min VALUES
            (36762, 'Myxothyrium', 'leptideum', 'Myxothyrium leptideum', 'species', NULL),
            (621785, 'Myxothyrium', 'leptideum', 'leptideum', 'subgenus', 133944),
            (82511, 'Clavaria', 'argillacea', 'Clavaria argillacea', 'species', NULL),
            (161825, 'Clavaria', 'argillacea', 'Clavaria argillacea var. argillacea', 'variety', 99001),
            (166409, 'Verrucaria', 'tristis', 'Lithocia tristis', 'species', NULL),
            (634435, 'Verrucaria', 'tristis', 'Verrucaria tristis', 'species', 82592),
            (70000, 'Two', 'ids', 'Two ids', 'species', NULL),
            (161886, 'Cortinarius', 'aerinus', 'Cortinarius aerinus', 'species', NULL),
            (624786, 'Cortinarius', 'aerinus', 'Cortinarius aerinus nom. dub.', 'species', 52523),
            (634850, 'Morchella', 'conica', 'Morchella conica nom. dub.', 'species', 95926),
            (626344, 'Ramaria', 'flavoides coll.', 'Ramaria flavoides coll.', 'species', 56508),
            (624063, 'Ramaria', 'flavoides coll. 1', 'Ramaria flavoides coll. 1', 'species', 226900),
            (96276, 'Entoloma', 'sericellum', 'Entoloma sericellum', 'species', NULL),
            (625268, 'Entoloma', 'sericellum', 'Entoloma sericellum', 'species', 53666),
            (90001, 'Paxillus', 'cuprinus jargeat', 'Paxillus cuprinus', 'species', 298850);
        INSERT INTO taxon_external_id_min VALUES
            (36762, 'artsdatabanken', 135683, 'accepted', 0, 'reviewed_supersession'),
            (621785, 'artsdatabanken', 133944, 'accepted', 1, NULL),
            (82511, 'artsdatabanken', 52390, 'accepted', 0, 'reviewed_supersession'),
            (161825, 'artsdatabanken', 99001, 'accepted', 1, NULL),
            (166409, 'artsdatabanken', 135403, 'accepted', 0, 'reviewed_supersession'),
            (70000, 'artsdatabanken', 1, 'accepted', 0, NULL),
            (70000, 'artsdatabanken', 2, 'accepted', 0, NULL),
            (96276, 'artsdatabanken', 298970, 'accepted', 0, NULL),
            (625268, 'artsdatabanken', 53666, 'accepted', 1, NULL);
        INSERT INTO taxon_external_id_text_min VALUES
            (36762, 'nortaxa', 'nortaxa_taxon_id', '135683', 'accepted', 0, 'authoritative_bridge:reviewed_supersession');
        INSERT INTO scientific_name_min VALUES
            (166409, 'sci', 'Lithocia tristis'), (166409, 'sci', 'Verrucaria tristis');
        INSERT INTO taxon_supersession_min VALUES
            (621852, 36762, 'r1', 'tax-test'), (621803, 166409, 'r1', 'tax-test');
    """)
    conn.commit()
    conn.close()
    return path


@pytest.mark.parametrize("genus,species,expected", [
    ("Myxothyrium", "leptideum", 135683),   # never the subgenus' 133944
    ("Clavaria", "argillacea", 52390),      # the variety does not make it ambiguous
    ("Lithocia", "tristis", 135403),        # survivor's genus column says Verrucaria
    ("Verrucaria", "tristis", 82592),
    ("Two", "ids", None),                   # ambiguous: refuse
    ("Cortinarius", "aerinus", 52523),      # canonical carries "nom. dub."
    ("Morchella", "conica", 95926),
    ("Ramaria", "flavoides coll.", 56508),  # "coll. 1" is another taxon, not a qualifier
    ("Entoloma", "sericellum", 53666),      # NorTaxa's own id outranks a legacy ADB id
    ("Paxillus", "cuprinus Jargeat", 298850),  # epithet column holds authorship
])
def test_v2_name_lookup_is_rank_and_canonical_name_aware(tmp_path, use_db, genus, species, expected):
    use_db(_make_v2_db(tmp_path / "v2.sqlite3"))
    assert ObservationDB.resolve_adb_taxon_id(genus, species) == expected


@pytest.mark.parametrize("sporely_id,genus,species,expected", [
    (36762, "Myxothyrium", "leptideum", ("found", 135683)),
    (621852, "Myxothyrium", "leptideum", ("found", 135683)),   # retired id follows the map
    (82511, "Clavaria", "argillacea", ("found", 52390)),
    (166409, "Lithocia", "tristis", ("found", 135403)),
    (621803, "Lithocia", "tristis", ("found", 135403)),
    (621785, "Myxothyrium", "leptideum", ("unresolved", None)),  # subgenus: wrong rank
    (70000, "Two", "ids", ("unresolved", None)),                 # two accepted ids
    (82511, "Amanita", "muscaria", ("unknown", None)),           # id does not name this taxon
    (999999, "Clavaria", "argillacea", ("unknown", None)),
    (None, "Clavaria", "argillacea", ("unknown", None)),
])
def test_concept_lookup(tmp_path, use_db, sporely_id, genus, species, expected):
    use_db(_make_v2_db(tmp_path / "v2.sqlite3"))
    assert ObservationDB.resolve_adb_taxon_id_for_concept(sporely_id, genus, species) == expected


def test_publishing_uses_the_concept_and_sends_unresolved_concepts_to_the_picker(tmp_path, use_db):
    from ui.observations_tab import ObservationsTab

    use_db(_make_v2_db(tmp_path / "v2.sqlite3"))
    calls: list[str] = []
    stub = _tab_stub(calls, 777)
    assert ObservationsTab._resolve_artsobs_taxon_id(
        stub, {"genus": "Lithocia", "species": "tristis", "sporely_taxon_id": 621803}
    ) == (135403, "taxonomy_db.artsdatabanken:concept")
    assert ObservationsTab._resolve_artsobs_taxon_id(
        stub, {"genus": "Two", "species": "ids", "sporely_taxon_id": 70000}
    ) == (777, "artsobservasjoner.picker_search")
    assert calls == ["Two ids"]


@pytest.mark.skipif(not __import__("os").environ.get("SPORELY_TAXONOMY_PROBE_DB"),
                    reason="set SPORELY_TAXONOMY_PROBE_DB to a release SQLite")
def test_release_regressions(use_db):
    import os
    use_db(Path(os.environ["SPORELY_TAXONOMY_PROBE_DB"]))
    assert ObservationDB.resolve_adb_taxon_id("Myxothyrium", "leptideum") == 135683
    assert ObservationDB.resolve_adb_taxon_id("Clavaria", "argillacea") == 52390
    assert ObservationDB.resolve_adb_taxon_id("Lithocia", "tristis") == 135403
    assert ObservationDB.resolve_adb_taxon_id("Cortinarius", "aerinus") == 52523
    assert ObservationDB.resolve_adb_taxon_id("Morchella", "conica") == 95926
