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
    KIND_NAME_ONLY,
    KIND_VERIFIED,
    ai_prediction_external_id,
    collapse_same_name_by_evidence,
    evidence_concept_ids,
    observation_identity_offer,
    reference_default_suggestions,
)
from database.vernacular_db import VernacularDB
from ui.taxon_input_controller import ROLE_TAXON_CHOICE
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


def test_the_own_name_is_offered_a_link_to_the_concept_its_identifier_proves(lookup):
    defaults = reference_default_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes", ai_entries=[_ai_entry()],
    )
    assert (defaults.own.kind, defaults.own.sporely_taxon_id) == (KIND_NAME_ONLY, None)
    assert (defaults.link.sporely_taxon_id, defaults.link.raw_external_id) == (NORTAXA_CONCEPT, "NBIC:54701")
    # The AI suggestion is the link itself, so the AI section repeats nothing.
    assert defaults.ai == ()
    assert defaults.evidence_ids == {NORTAXA_CONCEPT}


def test_ai_scores_play_no_part_in_the_evidence(lookup):
    unlikely = {**_ai_entry(), "score": 0.0001}
    assert evidence_concept_ids(lookup, [unlikely]) == {NORTAXA_CONCEPT}


def test_without_an_identifier_there_is_no_link_and_no_evidence(lookup):
    defaults = reference_default_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes",
        ai_entries=[_ai_entry(external_id="67594", source="inat")],   # iNat ids never resolve
    )
    assert defaults.link is None
    assert defaults.ai == ()                       # same name as the own taxon
    assert defaults.evidence_ids == frozenset()


def test_an_unknown_identifier_stays_name_only_and_says_so(lookup):
    defaults = reference_default_suggestions(lookup, ai_entries=[_ai_entry(external_id="NBIC:999999999")])
    (row,) = defaults.ai
    assert row.kind == KIND_NAME_ONLY and row.external_id_unresolved
    assert defaults.evidence_ids == frozenset()


def test_a_synonym_name_resolves_by_identifier_to_its_concept(lookup):
    (row,) = reference_default_suggestions(lookup, ai_entries=[_ai_entry(name="Collybia velutipes")]).ai
    assert (row.kind, row.sporely_taxon_id) == (KIND_VERIFIED, NORTAXA_CONCEPT)
    assert (row.scientific_name, row.concept_name) == ("Collybia velutipes", "Flammulina velutipes")


def test_an_own_concept_keeps_a_different_same_name_ai_concept(lookup):
    defaults = reference_default_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes", own_taxon_id=COL_CONCEPT,
        ai_entries=[_ai_entry()],
    )
    assert defaults.link is None
    assert [row.sporely_taxon_id for row in defaults.ai] == [NORTAXA_CONCEPT]
    # Both concepts are evidence, so typed search keeps both: real ambiguity.
    assert defaults.evidence_ids == {COL_CONCEPT, NORTAXA_CONCEPT}
    # The own concept alone is no evidence: it never hides the alternative.
    alone = reference_default_suggestions(
        lookup, own_genus="Flammulina", own_species="velutipes", own_taxon_id=COL_CONCEPT)
    assert alone.evidence_ids == frozenset()


@pytest.mark.parametrize("evidence, expected", [
    ({NORTAXA_CONCEPT}, [NORTAXA_CONCEPT, None]),
    (set(), [COL_CONCEPT, NORTAXA_CONCEPT, None]),
    ({COL_CONCEPT, NORTAXA_CONCEPT}, [COL_CONCEPT, NORTAXA_CONCEPT, None]),
    ({12345}, [COL_CONCEPT, NORTAXA_CONCEPT, None]),
])
def test_same_name_concepts_collapse_only_on_one_proven_concept(evidence, expected):
    rows = [("Flammulina velutipes", COL_CONCEPT), ("flammulina velutipes", NORTAXA_CONCEPT),
            ("Flammulina velutipes", None)]
    kept = collapse_same_name_by_evidence(rows, name_of=lambda r: r[0], id_of=lambda r: r[1], evidence=evidence)
    assert [r[1] for r in kept] == expected


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


def test_picker_opens_on_the_observation_taxon_and_offers_the_link(picker_settings, lookup):
    dialog, attached = _picker(lookup)
    try:
        combo = dialog.taxon_target_combo
        assert combo.currentIndex() == 0
        assert combo.itemText(0) == "Flammulina velutipes"
        assert dialog._taxon_id is None
        assert not dialog.taxon_link_button.isHidden()

        dialog.taxon_link_button.click()
        # The reference taxon only: the observation is untouched.
        assert dialog._taxon_id == str(NORTAXA_CONCEPT)
        assert (dialog._genus, dialog._species) == ("Flammulina", "velutipes")
        assert dialog._own_taxon_id is None
        assert dialog._exclude_observation_id == 685
        assert attached == []
        assert dialog.taxon_link_button.isHidden()
        assert "%" not in combo.currentText()
    finally:
        dialog.reject()


def test_picker_offers_no_link_without_an_identifier(picker_settings, lookup):
    dialog, _attached = _picker(lookup, ai_candidates=[])
    try:
        assert dialog.taxon_link_button.isHidden()
        assert dialog.taxon_target_combo.count() == 1
        dialog._on_taxon_target_text_entered()
        assert dialog._taxon_id is None
    finally:
        dialog.reject()


def test_unrelated_ai_suggestions_sit_in_their_own_section(picker_settings, lookup):
    dialog, _attached = _picker(lookup, ai_candidates=[_ai_entry(), _ai_entry(
        external_id="1234567", name="Flammulina elastica", source="inat")])
    try:
        rows = dialog._taxon_default_suggestion_rows()
        labels = [label for label, _data in rows]
        assert labels[0] == "Flammulina velutipes"
        assert rows[1] == ("AI suggestions", None)
        assert rows[2][0].startswith("Flammulina elastica") and rows[2][0].endswith("(name only)")
        # Typed search is a database search: the AI row is not in it.
        typed = dialog.taxon_search_rows("Flammulina elastica")
        assert all(data["taxon_id"] is not None for _label, data in typed)
    finally:
        dialog.reject()


def test_picker_search_shows_one_match_when_an_identifier_settles_it(picker_settings, lookup):
    dialog, _attached = _picker(lookup)
    try:
        rows = dialog.taxon_search_rows("Flammulina velutipes")
        ids = {data["taxon_id"] for _label, data in rows if data.get("label") == "Flammulina velutipes"}
        assert ids == {NORTAXA_CONCEPT}
        assert [label for label, data in rows if data.get("label") == "Flammulina velutipes"] == [
            "Flammulina velutipes (vintersopp)"]
        # A variety no identifier speaks for stays ambiguous, with sources.
        variety = [label for label, data in rows if data.get("label") == "Flammulina velutipes var. velutipes"]
        assert len(variety) == 2 and all("·" in label for label in variety)
    finally:
        dialog.reject()


def test_picker_search_lists_both_sources_when_genuinely_ambiguous(picker_settings, lookup):
    dialog, _attached = _picker(lookup, ai_candidates=[])
    try:
        rows = dialog.taxon_search_rows("Flammulina velutipes")
        by_id = {data["taxon_id"]: label for label, data in rows
                 if data.get("label") == "Flammulina velutipes"}
        assert {COL_CONCEPT, NORTAXA_CONCEPT} <= set(by_id)
        assert "NorTaxa 54701" in by_id[NORTAXA_CONCEPT]
        assert "COL" in by_id[COL_CONCEPT]
    finally:
        dialog.reject()


def test_identifiers_are_not_called_unresolved_without_a_taxonomy():
    (row,) = reference_default_suggestions(None, ai_entries=[_ai_entry()]).ai
    assert (row.kind, row.external_id_unresolved) == (KIND_NAME_ONLY, False)


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
        assert dialog.identity_offer_label.text() == "Flammulina velutipes is in the species database."
        assert "NBIC:54701" in dialog.identity_offer_row.toolTip()
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


# ── The observation editor's Species picker ──────────────────────────────────


def _species_rows(dialog, genus="Flammulina", prefix="vel"):
    controller = dialog._taxon_controller
    dialog.genus_input.setText(genus)
    dialog.species_input.setText(prefix)
    controller.refresh_species_suggestions()
    model = controller.species_model
    return [(model.item(r).text(), model.index(r, 0)) for r in range(model.rowCount())]


@pytest.fixture
def editor(monkeypatch, tmp_path, shipped_artifact):
    from PySide6.QtWidgets import QApplication
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import _build_dialog

    qapp = QApplication.instance() or QApplication([])
    dialogs = []

    def build(**draft):
        dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact,
                               draft_data=draft or None)
        dialogs.append(dialog)
        return dialog

    yield build
    for dialog in dialogs:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


@pytest.mark.parametrize("source, concept", [("COL", COL_CONCEPT), ("NorTaxa", NORTAXA_CONCEPT)])
def test_species_pick_binds_the_picked_rows_own_concept(editor, source, concept):
    dialog = editor()
    rows = [(label, index) for label, index in _species_rows(dialog) if label.startswith("velutipes ·")]
    assert len(rows) == 2                        # genuinely ambiguous: both, with sources
    label, index = next(row for row in rows if source in row[0])
    dialog._taxon_controller.on_species_selected(index)
    data = dialog.get_data()
    assert (data["genus"], data["species"]) == ("Flammulina", "velutipes")
    assert data["sporely_taxon_id"] == concept
    assert data["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT
    assert dialog.scientific_name_input.text() == "Flammulina velutipes"


def test_species_and_scientific_picks_commit_the_same_snapshot(editor):
    species_dialog = editor()
    _label, index = next(row for row in _species_rows(species_dialog) if "NorTaxa" in row[0])
    species_dialog._taxon_controller.on_species_selected(index)

    scientific_dialog = editor()
    controller = scientific_dialog._taxon_controller
    scientific_dialog.scientific_name_input.setText("Flammulina velutipes")
    controller.refresh_scientific_suggestions()
    model = controller._scientific_model
    pick = next(model.index(r, 0) for r in range(model.rowCount())
                if (model.item(r).data(ROLE_TAXON_CHOICE) or {}).get("sporely_taxon_id") == NORTAXA_CONCEPT)
    controller.on_scientific_name_selected(pick)

    assert species_dialog._taxon_controller.committed_snapshot() == controller.committed_snapshot()


def test_an_artsorakel_identifier_settles_the_species_and_scientific_lists(editor):
    dialog = editor(**_observation(genus="", species=""))
    labels = [label for label, _index in _species_rows(dialog)]
    assert "velutipes" in labels
    assert not any(label.startswith("velutipes ·") for label in labels)
    _label, index = next(row for row in _species_rows(dialog) if row[0] == "velutipes")
    dialog._taxon_controller.on_species_selected(index)
    assert dialog.get_data()["sporely_taxon_id"] == NORTAXA_CONCEPT

    controller = dialog._taxon_controller
    dialog.scientific_name_input.setText("Flammulina velutipes")
    rows = controller.refresh_scientific_suggestions()
    assert [r["sporely_taxon_id"] for r in rows if r["scientific_name"] == "Flammulina velutipes"] == [
        NORTAXA_CONCEPT]


def test_genus_only_and_typed_names_bind_nothing_and_warn_nothing(editor):
    dialog = editor()
    dialog.genus_input.setText("Flammulina")
    dialog._refresh_identity_offer()
    assert dialog.identity_offer_row.isHidden()
    assert dialog.get_data()["sporely_taxon_id"] is None
    dialog.species_input.setText("velutipes")          # typed, never picked
    dialog._refresh_identity_offer()
    assert dialog.get_data()["sporely_taxon_id"] is None
    assert dialog.identity_offer_row.isHidden()


def test_a_name_only_species_row_binds_nothing():
    from PySide6.QtWidgets import QApplication, QLineEdit
    from database.taxon_lookup import TaxonChoice
    from ui.taxon_input_controller import TaxonInputController

    QApplication.instance() or QApplication([])

    class _Lookup:
        vernacular_db = None

        def suggest_scientific_names(self, prefix, limit=40):
            return []

        def suggest_species(self, genus, prefix="", limit=40):
            return [TaxonChoice(genus=genus, species="referencia", source="reference")]

    controller = TaxonInputController(
        _Lookup(), QLineEdit(), QLineEdit(), scientific_name_input=QLineEdit(),
        concept_species_suggestions=True,
    )
    controller.genus_input.setText("Flammulina")
    controller.refresh_species_suggestions()
    controller.on_species_selected(controller.species_model.index(0, 0))
    assert controller.species_input.text() == "referencia"
    assert controller.committed_snapshot() is None


# ── Review findings (8989e55) ────────────────────────────────────────────────


def test_evidence_for_one_name_never_hides_another_names_concept():
    """A synonym row ("Foo bar" → X) shares its text with concept Y's own
    name. Evidence for X (from another name) must not drop Y."""
    rows = [("Foo bar", 1, False), ("Foo bar", 2, True)]
    kept = collapse_same_name_by_evidence(
        rows, name_of=lambda r: r[0], id_of=lambda r: r[1], evidence={1}, is_own_name=lambda r: r[2])
    assert kept == rows
    # Own-name duplicates are still settled, and the synonym row survives.
    rows = [("A b", 1, True), ("A b", 2, True), ("A b", 3, False)]
    kept = collapse_same_name_by_evidence(
        rows, name_of=lambda r: r[0], id_of=lambda r: r[1], evidence={1}, is_own_name=lambda r: r[2])
    assert [r[1] for r in kept] == [1, 3]


def _pick_scientific(dialog, concept):
    controller = dialog._taxon_controller
    dialog.scientific_name_input.setText("Flammulina velutipes")
    controller.refresh_scientific_suggestions()
    model = controller._scientific_model
    controller.on_scientific_name_selected(next(
        model.index(r, 0) for r in range(model.rowCount())
        if (model.item(r).data(ROLE_TAXON_CHOICE) or {}).get("sporely_taxon_id") == concept))


def test_a_committed_concept_never_hides_the_alternative(editor):
    """Committing one same-name concept is not evidence: both stay listed
    in Species and Scientific, so the observer can switch."""
    dialog = editor()
    _pick_scientific(dialog, COL_CONCEPT)
    controller = dialog._taxon_controller
    controller.refresh_species_suggestions()
    _pick_scientific(dialog, NORTAXA_CONCEPT)
    assert dialog.get_data()["sporely_taxon_id"] == NORTAXA_CONCEPT

    controller.refresh_species_suggestions()
    model = controller.species_model
    rows = {model.item(r).data(ROLE_TAXON_CHOICE)["sporely_taxon_id"]: r for r in range(model.rowCount())
            if isinstance(model.item(r).data(ROLE_TAXON_CHOICE), dict)
            and model.item(r).text().startswith("velutipes")}
    assert set(rows) == {COL_CONCEPT, NORTAXA_CONCEPT}
    controller.on_species_selected(model.index(rows[COL_CONCEPT], 0))
    assert dialog.get_data()["sporely_taxon_id"] == COL_CONCEPT


def test_link_binds_nothing_once_the_name_no_longer_matches(editor):
    dialog = editor(**_observation())
    assert not dialog.identity_offer_row.isHidden()
    dialog.species_input.setText("elastica")          # a programmatic fill: no textEdited
    dialog.identity_offer_button.click()
    data = dialog.get_data()
    assert data["sporely_taxon_id"] is None
    assert (data["genus"], data["species"]) == ("Flammulina", "elastica")
