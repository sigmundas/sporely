"""Artsobservasjoner destination confirmation of the publishing taxon id.

Renamed species: Artsobservasjoner keeps the original id while NorTaxa gives
the accepted name a new one.  Publishing must use the id Artsobservasjoner's
own species search returns for the exact accepted name.  Picker HTTP is
mocked; the taxonomy lookup runs against a real synthetic SQLite file.
"""
from __future__ import annotations

import copy
import hashlib
import json
import logging
from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

import ui.observations_tab as observations_tab
from ui.observations_tab import ObservationsTab
from utils.artsobservasjoner_taxon import (
    DESTINATION_CONFIRMED_SOURCE,
    AmbiguousArtsobservasjonerTaxonIdError,
    confirm_destination_taxon_id,
    log_artsobservasjoner_taxon_diagnostic,
)
from tests.test_adb_taxon_id_lookup import _make_db

LOCAL = [
    (1, "Paralepista", "flaccida", "nortaxa", 186418),
    (2, "Imleria", "badia", "nortaxa", 204334),
    (3, "Apioperdon", "pyriforme", "nortaxa", 228333),
    (4, "Russula", "integra", "nortaxa", 57772),
]


def _item(taxon_id, name):
    payload = json.dumps({"taxonid": str(taxon_id), "taxonname": "x",
                          "scientificname": f"<i>{name}</i>", "leaf": "true"})
    escaped = payload.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<li><span class="itemjson">{escaped}</span></li>'


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    import utils.vernacular_utils as vernacular_utils
    path = _make_db(tmp_path / "taxonomy.sqlite3", LOCAL)
    monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda lang_code=None: path)
    return path


def _tab(monkeypatch, responder):
    calls = []

    def fake_get(url, params=None, headers=None, cookies=None, timeout=None):
        calls.append(params["search"])
        return responder(params["search"])

    monkeypatch.setattr(observations_tab.requests, "get", fake_get)
    tab = SimpleNamespace(
        _artsobs_lookup_cookies={"c": "1"},
        _normalize_taxon_text=MethodType(ObservationsTab._normalize_taxon_text, SimpleNamespace()),
        _resolve_accepted_taxon_pair=lambda g, s: None,
    )
    for name in ("_observation_taxon_fields", "_extract_artsobs_taxon_pair",
                 "_lookup_artsobservasjoner_taxon_id", "_resolve_artsobs_taxon_id",
                 "_artsobs_accepted_scientific_name", "_confirm_artsobs_destination_taxon_id"):
        setattr(tab, name, MethodType(getattr(ObservationsTab, name), tab))
    return tab, calls


def _ok(body):
    return SimpleNamespace(status_code=200, text=body, raise_for_status=lambda: None)


def _resolve(tab, obs):
    return ObservationsTab._resolve_artsobs_taxon_resolution(tab, obs)


@pytest.mark.parametrize("genus,species,local_id,dest_id", [
    ("Paralepista", "flaccida", 186418, 55448),
    ("Imleria", "badia", 204334, 56023),
    ("Apioperdon", "pyriforme", 228333, 54338),
])
def test_destination_confirmed_id_overrides_local_nortaxa_id(
        db_path, monkeypatch, caplog, genus, species, local_id, dest_id):
    tab, calls = _tab(monkeypatch, lambda s: _ok(_item(dest_id, s)))
    obs = {"id": 7, "genus": genus, "species": species}
    before = copy.deepcopy(obs)
    db_hash = hashlib.sha256(Path(db_path).read_bytes()).hexdigest()

    res = _resolve(tab, obs)

    assert res.taxon_id == dest_id
    assert res.source_field == DESTINATION_CONFIRMED_SOURCE
    assert res.local_taxon_id == local_id
    assert res.local_source_field == "taxonomy_db.artsdatabanken"
    assert res.override_reason
    assert calls == [f"{genus} {species}"]
    # Publishing only: no observation or taxonomy identity writes.
    assert obs == before
    assert hashlib.sha256(Path(db_path).read_bytes()).hexdigest() == db_hash
    with caplog.at_level(logging.WARNING):
        log_artsobservasjoner_taxon_diagnostic(obs, res)
    assert f"final_taxon_id={dest_id}" in caplog.text
    assert f"local_taxon_id={local_id}" in caplog.text


def test_agreeing_destination_keeps_local_source(db_path, monkeypatch):
    tab, _ = _tab(monkeypatch, lambda s: _ok(_item(57772, s)))
    res = _resolve(tab, {"genus": "Russula", "species": "integra"})
    assert (res.taxon_id, res.source_field, res.local_taxon_id) == (57772, "taxonomy_db.artsdatabanken", None)


@pytest.mark.parametrize("body", [
    _item(55448, "Paralepista flaccida") + _item(99999, "Paralepista flaccida"),
    _item(55448, "Paralepista gilva"),
    _item(55448, "Paralepista") + _item(1, "Lepista flaccida"),
    "",
    "<html>not a picker</html>",
])
def test_missing_or_ambiguous_search_keeps_local_id(db_path, monkeypatch, body):
    tab, _ = _tab(monkeypatch, lambda s: _ok(body))
    res = _resolve(tab, {"genus": "Paralepista", "species": "flaccida"})
    assert (res.taxon_id, res.source_field) == (186418, "taxonomy_db.artsdatabanken")


def _raise(_):
    raise observations_tab.requests.ConnectionError("offline")


def _http500(_):
    def boom():
        raise observations_tab.requests.HTTPError("500")
    return SimpleNamespace(status_code=500, text="", raise_for_status=boom)


@pytest.mark.parametrize("responder", [_raise, _http500])
def test_unavailable_search_keeps_local_id(db_path, monkeypatch, responder):
    tab, _ = _tab(monkeypatch, responder)
    res = _resolve(tab, {"genus": "Paralepista", "species": "flaccida"})
    assert (res.taxon_id, res.source_field) == (186418, "taxonomy_db.artsdatabanken")


@pytest.mark.parametrize("name", ["Paralepista", "Paralepista flaccida agg.",
                                  "Paralepista flaccida var. gilva", "Paralepista flaccida subsp. x"])
def test_non_species_rank_never_confirms_species(name):
    results = [SimpleNamespace(taxon_id=1, scientific_name=name)]
    assert confirm_destination_taxon_id(results, name) is None
    assert confirm_destination_taxon_id(results, "Paralepista flaccida") is None


def test_explicit_stored_id_matching_destination_wins(db_path, monkeypatch):
    tab, _ = _tab(monkeypatch, lambda s: _ok(_item(55448, s)))
    obs = {"genus": "Paralepista", "species": "flaccida", "artsobservasjoner_taxon_id": 55448}
    res = _resolve(tab, obs)
    assert (res.taxon_id, res.source_field) == (55448, "artsobservasjoner_taxon_id")
    assert obs["artsobservasjoner_taxon_id"] == 55448


def test_explicit_stored_id_conflicting_with_destination_is_ambiguous(db_path, monkeypatch):
    tab, _ = _tab(monkeypatch, lambda s: _ok(_item(55448, s)))
    with pytest.raises(AmbiguousArtsobservasjonerTaxonIdError):
        _resolve(tab, {"genus": "Paralepista", "species": "flaccida", "artsobs_taxon_id": 186418})
