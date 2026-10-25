"""Evidence-aware taxon suggestions: the Flammulina velutipes case.

The shipped taxonomy holds two same-name species concepts for *Flammulina
velutipes*: COL 98288 (no NorTaxa id) and NorTaxa 625621 (NorTaxa 54701).
Artsorakel identifies the species as ``NBIC:54701``, which the NorTaxa
bridge resolves to 625621. An Android observation synced to desktop keeps
that identifier as ``taxon_id`` / ``taxon.id`` and ``ai_selected_taxon_id``
(owner's observations 366 and 685), while a desktop run keeps it as
``scientific_name_id`` — at the top level or inside ``taxon``.

Pickers must offer the concept the identifier proves, list same-name
concepts separately when there is no identifier, and never bind anything
from name equality.
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from database.taxon_lookup import InstalledTaxonConcept, TaxonLookupService
from database.taxon_suggestions import (
    KIND_NAME_MATCH,
    KIND_NAME_ONLY,
    KIND_VERIFIED,
    ORIGIN_NAME,
    ORIGIN_OBSERVATION,
    ai_prediction_external_id,
    observation_identity_offer,
    reference_taxon_suggestions,
)
from database.vernacular_db import VernacularDB
from utils import taxonomy_v2
from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_SPORELY,
)

NORTAXA_CONCEPT = 625621
COL_CONCEPT = 98288


# Recorded shapes (owner's local database, 2026-10-25), trimmed of URLs.

def _android_synced_prediction() -> dict:
    """An Artsorakel prediction made on Android and pulled from the cloud
    (observation 685): the id is ``taxon_id`` / ``taxon.id``; there is no
    ``scientific_name_id`` key."""
    taxon = {
        "scientificName": "Flammulina velutipes", "scientific_name": "Flammulina velutipes",
        "name": "Flammulina velutipes", "vernacularName": "vintersopp",
        "vernacular_name": "vintersopp", "preferred_common_name": "vintersopp",
        "common_name": "vintersopp", "id": "NBIC:54701", "taxonId": "NBIC:54701",
        "taxon_id": "NBIC:54701", "redListCategory": "LC", "redlistCategory": "LC",
        "redlist_category": "LC",
    }
    return {
        "rank": 1, "service": "artsorakel", "taxon_id": "NBIC:54701", "probability": 0.979365,
        "external_ids": {"gbif": "3341441"}, "redlist_source": "Artsdatabanken",
        "scientific_name": "Flammulina velutipes", "vernacular_name": "vintersopp",
        "redlist_category": "LC", "taxon": taxon, "scientificName": "Flammulina velutipes",
        "vernacularName": "vintersopp", "displayName": "vintersopp (Flammulina velutipes)",
        "display_name": "vintersopp (Flammulina velutipes)",
    }


def _desktop_nested_prediction() -> dict:
    """A desktop Artsorakel run stored as ``{probability, taxon}`` (observation
    19): the id is only ``taxon.scientific_name_id``."""
    return {
        "probability": 0.953618,
        "taxon": {
            "name": "Flammulina velutipes", "scientificName": "Flammulina velutipes",
            "scientific_name": "Flammulina velutipes", "probability": 0.953618,
            "scientific_name_id": "NBIC:54701", "scientific_name_id_shared": "GBIF:3341441",
            "scientific_name_shared": "Flammulina velutipes", "vernacularName": "vintersopp",
            "redListCategory": "LC", "redListCategories": {"NO": "LC"},
        },
    }


def _desktop_flat_prediction() -> dict:
    """A desktop Artsorakel run stored flat (observation 828)."""
    return {
        "name": "Flammulina velutipes", "scientificName": "Flammulina velutipes",
        "scientific_name": "Flammulina velutipes", "probability": 0.0123,
        "scientific_name_id": "NBIC:54701", "scientific_name_id_shared": "GBIF:3341441",
        "vernacularName": "vintersopp",
    }


@pytest.fixture(scope="module")
def shipped_artifact(tmp_path_factory) -> Path:
    return taxonomy_v2.ensure_installed(app_data_dir=tmp_path_factory.mktemp("shipped-taxonomy"))


@pytest.fixture(scope="module")
def lookup(shipped_artifact) -> TaxonLookupService:
    return TaxonLookupService(
        vernacular_db=VernacularDB(shipped_artifact, language_code="no"), language_code="no",
    )


def _ai_entry(external_id: str = "NBIC:54701", name: str = "Flammulina velutipes", source: str = "arts") -> dict:
    genus, species = name.split()
    return {"source": source, "scientific_name": name, "genus": genus, "species": species,
            "vernacular": "vintersopp", "score": 0.98, "external_id": external_id}


@pytest.mark.parametrize("prediction", [
    _android_synced_prediction(), _desktop_nested_prediction(), _desktop_flat_prediction(),
], ids=["android-synced", "desktop-nested", "desktop-flat"])
def test_every_stored_prediction_shape_keeps_the_artsorakel_identifier(prediction):
    from ui.observations_tab import _normalize_ai_prediction_taxon

    taxon = _normalize_ai_prediction_taxon(prediction, source="arts")
    assert ai_prediction_external_id(prediction, taxon) == "NBIC:54701"
    # The observation editor's copy path reads the same id.
    assert taxon["id"] == "NBIC:54701"


def test_the_shipped_release_holds_two_same_name_concepts(lookup):
    ids = {hit["sporely_taxon_id"] for hit in lookup.suggest_scientific_names("Flammulina velutipes")
           if hit["scientific_name"] == "Flammulina velutipes"}
    assert ids == {COL_CONCEPT, NORTAXA_CONCEPT}


def test_an_artsorakel_identifier_offers_its_verified_concept_first(lookup):
    rows = reference_taxon_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes", ai_entries=[_ai_entry()],
    )
    first = rows[0]
    assert (first.kind, first.sporely_taxon_id, first.raw_external_id) == (
        KIND_VERIFIED, NORTAXA_CONCEPT, "NBIC:54701")
    assert first.source_label == "NorTaxa 54701"
    # The identifier settled which concept it is: the COL concept is not
    # offered as an alternative, and the observation's own name-only taxon
    # stays listed (it is what the picker opens on).
    assert COL_CONCEPT not in {r.sporely_taxon_id for r in rows}
    assert [(r.kind, r.origin) for r in rows[1:]] == [(KIND_NAME_ONLY, ORIGIN_OBSERVATION)]


def test_without_an_identifier_both_same_name_concepts_are_offered_and_told_apart(lookup):
    rows = reference_taxon_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes",
        ai_entries=[_ai_entry(external_id="67594", source="inat")],   # iNat ids never resolve
    )
    matches = [r for r in rows if r.kind == KIND_NAME_MATCH]
    assert {r.sporely_taxon_id for r in matches} == {COL_CONCEPT, NORTAXA_CONCEPT}
    assert len({r.source_label for r in matches}) == 2
    assert all(r.source_label for r in matches)
    assert rows[-1].kind == KIND_NAME_ONLY
    assert not any(r.kind == KIND_VERIFIED for r in rows)


def test_an_unknown_identifier_stays_name_only_and_says_so(lookup):
    rows = reference_taxon_suggestions(lookup, ai_entries=[_ai_entry(external_id="NBIC:999999999")])
    name_only = [r for r in rows if r.kind == KIND_NAME_ONLY]
    assert len(name_only) == 1 and name_only[0].external_id_unresolved
    assert {r.sporely_taxon_id for r in rows if r.is_concept} == {COL_CONCEPT, NORTAXA_CONCEPT}
    assert not any(r.kind == KIND_VERIFIED for r in rows)


def test_a_synonym_name_resolves_by_identifier_to_its_concept(lookup):
    rows = reference_taxon_suggestions(lookup, ai_entries=[_ai_entry(name="Collybia velutipes")])
    assert (rows[0].kind, rows[0].sporely_taxon_id) == (KIND_VERIFIED, NORTAXA_CONCEPT)
    assert rows[0].scientific_name == "Collybia velutipes"
    assert rows[0].concept_name == "Flammulina velutipes"


def _observation(**fields) -> dict:
    return {"genus": "Flammulina", "species": "velutipes", "ai_selected_service": "artsorakel",
            "ai_selected_taxon_id": "NBIC:54701", "ai_selected_scientific_name": "Flammulina velutipes",
            **fields}


def test_an_observation_without_identity_is_offered_the_verified_concept(lookup):
    offer = observation_identity_offer(lookup, _observation(), genus="Flammulina", species="velutipes")
    assert (offer.sporely_taxon_id, offer.raw_external_id, offer.source_label) == (
        NORTAXA_CONCEPT, "NBIC:54701", "NorTaxa 54701")


def test_an_unresolved_provider_tuple_on_the_observation_is_offered(lookup):
    row = _observation(ai_selected_taxon_id=None, taxon_identity_state=STATE_EXTERNAL_UNRESOLVED,
                       taxon_identity_source_system="nortaxa",
                       taxon_identity_namespace="nortaxa_taxon_id",
                       taxon_identity_external_id="54701")
    offer = observation_identity_offer(lookup, row, genus="Flammulina", species="velutipes")
    assert offer is not None and offer.sporely_taxon_id == NORTAXA_CONCEPT


@pytest.mark.parametrize("row, genus, species", [
    # The observer named something else: the identifier is not about it.
    (_observation(), "Flammulina", "elastica"),
    # Genus-only names are not offered a species.
    (_observation(), "Flammulina", ""),
    # Already a proven identity (here the COL concept): nothing is offered.
    (_observation(sporely_taxon_id=COL_CONCEPT, taxon_identity_state=STATE_SPORELY,
                  taxon_identity_proof=PROOF_TAXONOMY_V2_ARTIFACT), "Flammulina", "velutipes"),
    # No identifier at all: name equality is not evidence.
    (_observation(ai_selected_taxon_id=None), "Flammulina", "velutipes"),
])
def test_no_offer_without_agreeing_identifier_evidence(lookup, row, genus, species):
    assert observation_identity_offer(lookup, row, genus=genus, species=species) is None


def test_conflicting_identifiers_offer_nothing():
    concepts = {"1": 111, "2": 222}

    class _Lookup:
        vernacular_db = None

        def resolve_external_identity(self, identity):
            taxon_id = concepts.get(identity.external_id)
            return InstalledTaxonConcept(taxon_id, "tax-test", "Flammulina velutipes", "species") if taxon_id else None

    entries = [_ai_entry(external_id="NBIC:1"), _ai_entry(external_id="NBIC:2")]
    row = _observation(ai_selected_taxon_id=None)
    assert observation_identity_offer(_Lookup(), row, genus="Flammulina", species="velutipes",
                                      ai_entries=entries) is None
    assert observation_identity_offer(_Lookup(), row, genus="Flammulina", species="velutipes",
                                      ai_entries=entries[:1]).sporely_taxon_id == 111


def test_reference_ai_entries_carry_the_provider_identifier():
    from ui.main_window import MainWindow

    host = SimpleNamespace(
        _ref_ai_scientific_from_taxon=MainWindow._ref_ai_scientific_from_taxon,
        _ref_ai_genus_species_from_taxon=MainWindow._ref_ai_genus_species_from_taxon,
        _ref_ai_prediction_score=MainWindow._ref_ai_prediction_score,
        _ref_ai_vernacular_from_taxon=lambda taxon, source: "vintersopp",
    )
    entry = MainWindow._build_reference_ai_entry(host, _android_synced_prediction(), "arts")
    assert entry["external_id"] == "NBIC:54701"
    assert (entry["genus"], entry["species"]) == ("Flammulina", "velutipes")


# ── The reference picker ─────────────────────────────────────────────────────


@pytest.fixture
def picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    from PySide6.QtWidgets import QApplication
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry

    QApplication.instance() or QApplication([])

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(picker.MeasurementSetRepository, "get", lambda _id: None)
    monkeypatch.setattr(picker, "QSettings", settings)
    monkeypatch.setattr(geometry, "QSettings", settings)


def _picker(lookup, **kwargs):
    from ui.add_reference_dialog import AddReferenceDialog

    attached = []
    defaults = dict(
        taxon_label="Flammulina velutipes", taxon_id=None, genus="Flammulina", species="velutipes",
        candidates=[], community_results=[], my_observations=[], taxon_lookup=lookup,
        ai_candidates=[_ai_entry()], exclude_observation_id=685,
        attach_callback=lambda *args: attached.append(args),
    )
    defaults.update(kwargs)
    return AddReferenceDialog(None, **defaults), attached


def test_picker_lists_the_verified_concept_first_and_opens_on_the_observation(picker_settings, lookup):
    dialog, attached = _picker(lookup)
    try:
        combo = dialog.taxon_target_combo
        first = combo.itemData(0)
        assert (first["kind"], first["taxon_id"]) == (KIND_VERIFIED, NORTAXA_CONCEPT)
        assert "NorTaxa 54701" in combo.itemText(0)
        own = combo.itemData(combo.currentIndex())
        assert (own["origin"], own["kind"], own["taxon_id"]) == (ORIGIN_OBSERVATION, KIND_NAME_ONLY, None)
        assert combo.itemText(combo.currentIndex()).endswith("name only")
        assert dialog._taxon_id is None

        # One explicit pick binds the concept as the reference taxon only.
        dialog._on_taxon_target_activated(0)
        assert dialog._taxon_id == str(NORTAXA_CONCEPT)
        assert dialog._exclude_observation_id == 685
        assert dialog._own_taxon_id is None
        assert attached == []
    finally:
        dialog.reject()


def test_picker_never_opens_on_a_concept_matched_by_name_only(picker_settings, lookup):
    dialog, _attached = _picker(lookup, ai_candidates=[])
    try:
        combo = dialog.taxon_target_combo
        matches = [combo.itemData(i) for i in range(combo.count())
                   if combo.itemData(i)["kind"] == KIND_NAME_MATCH]
        assert {m["taxon_id"] for m in matches} == {COL_CONCEPT, NORTAXA_CONCEPT}
        assert all(m["origin"] == ORIGIN_NAME for m in matches)
        current = combo.itemData(combo.currentIndex())
        assert (current["kind"], current["origin"], current["taxon_id"]) == (
            KIND_NAME_ONLY, ORIGIN_OBSERVATION, None)
        # Return on the opened row keeps it name-only.
        dialog._on_taxon_target_text_entered()
        assert dialog._taxon_id is None
    finally:
        dialog.reject()


def test_identifiers_are_not_called_unresolved_without_a_taxonomy():
    rows = reference_taxon_suggestions(None, ai_entries=[_ai_entry()])
    assert [(r.kind, r.external_id_unresolved) for r in rows] == [(KIND_NAME_ONLY, False)]


def test_picker_search_keeps_both_same_name_concepts(picker_settings, lookup):
    dialog, _attached = _picker(lookup, ai_candidates=[])
    try:
        rows = dialog.taxon_search_rows("Flammulina velutipes")
        by_id = {data["taxon_id"]: label for label, data in rows
                 if data.get("label") == "Flammulina velutipes"}
        assert {COL_CONCEPT, NORTAXA_CONCEPT} <= set(by_id)
        assert by_id[COL_CONCEPT] != by_id[NORTAXA_CONCEPT]
        assert "NorTaxa 54701" in by_id[NORTAXA_CONCEPT]
    finally:
        dialog.reject()


# ── The observation editor ───────────────────────────────────────────────────


def test_observation_editor_offers_the_concept_and_binds_it_only_on_acceptance(
    monkeypatch, tmp_path, shipped_artifact,
):
    from PySide6.QtWidgets import QApplication
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import _build_dialog

    qapp = QApplication.instance() or QApplication([])
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact,
        draft_data=_observation(common_name="vintersopp"),
    )
    try:
        assert not dialog.identity_offer_row.isHidden()
        assert "NBIC:54701" in dialog.identity_offer_label.text()
        before = dialog.get_data()
        assert before["sporely_taxon_id"] is None

        dialog.identity_offer_button.click()
        qapp.processEvents()

        data = dialog.get_data()
        assert (data["genus"], data["species"]) == ("Flammulina", "velutipes")
        assert data["sporely_taxon_id"] == NORTAXA_CONCEPT
        assert data["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
        assert data["taxon_identity_raw_external_id"] == "NBIC:54701"
        assert dialog.identity_offer_row.isHidden()
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_observation_editor_binds_its_own_tuple_stored_without_a_raw_value(
    monkeypatch, tmp_path, shipped_artifact,
):
    from PySide6.QtWidgets import QApplication
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import _build_dialog

    qapp = QApplication.instance() or QApplication([])
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact,
        draft_data=_observation(
            ai_selected_taxon_id=None, scientific_name_snapshot="Flammulina velutipes",
            taxon_rank_snapshot="species", taxon_identity_state=STATE_EXTERNAL_UNRESOLVED,
            taxon_identity_source_system="nortaxa", taxon_identity_namespace="nortaxa_taxon_id",
            taxon_identity_external_id="54701"),
    )
    try:
        assert not dialog.identity_offer_row.isHidden()
        dialog.identity_offer_button.click()
        qapp.processEvents()
        data = dialog.get_data()
        assert data["sporely_taxon_id"] == NORTAXA_CONCEPT
        assert data["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_observation_editor_drops_the_offer_when_the_name_changes(monkeypatch, tmp_path, shipped_artifact):
    from PySide6.QtWidgets import QApplication
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import _build_dialog

    qapp = QApplication.instance() or QApplication([])
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact,
        draft_data=_observation(),
    )
    try:
        assert not dialog.identity_offer_row.isHidden()
        dialog.species_input.setText("elastica")
        dialog._refresh_identity_offer()
        assert dialog.identity_offer_row.isHidden()
        assert dialog.get_data()["sporely_taxon_id"] is None
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()
