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
