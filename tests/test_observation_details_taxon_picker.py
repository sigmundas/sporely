"""The unified taxon picker in the observation editor (taxon picker Stage 3).

The picker replaces the Name, Genus, Species and Scientific rows. Those
inputs stay as hidden backing state, so a pick goes through the taxon
controller's existing Scientific-pick commit and the save path is unchanged.
These tests drive the picker inside a real `ObservationDetailsDialog` on the
shipped taxonomy release and check what `get_data()` would save, after
reopening the editor from that data and after a round trip through the
observations table.

*Flammulina velutipes* has two same-name concepts: NorTaxa 625621
(Artsdatabanken 54701, Artsorakel ``NBIC:54701``) and COL 98288.
"""
from __future__ import annotations

import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QMessageBox

from ui import observations_tab
from utils import taxonomy_v2
from utils.taxon_identity import (
    IDENTITY_COLUMNS,
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
)

NORTAXA = 625621
COL = 98288

# What the editor saves for the identification (the edit path's columns).
TAXON_KEYS = (
    "genus", "species", "common_name", "species_guess", "uncertain", "determination_method",
    "scientific_name_snapshot", "taxon_rank_snapshot", "sporely_taxon_id", *IDENTITY_COLUMNS,
    "ai_selected_service", "ai_selected_taxon_id", "ai_selected_scientific_name",
)


@pytest.fixture(scope="module")
def shipped_artifact(tmp_path_factory):
    return taxonomy_v2.ensure_installed(app_data_dir=tmp_path_factory.mktemp("shipped-taxonomy"))


@pytest.fixture
def editor(monkeypatch, tmp_path, shipped_artifact):
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import _build_dialog

    qapp = QApplication.instance() or QApplication([])
    dialogs = []

    def build(*, observation=None, **draft):
        """A new observation from ``draft``, or an edit of ``observation``
        (a stored row) as the observations tab opens one."""
        dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact,
                               observation=observation, draft_data=draft or None)
        dialog.resize(1100, 820)
        dialog.show()
        qapp.processEvents()
        dialogs.append(dialog)
        return dialog

    yield build
    for dialog in dialogs:
        dialog._cleanup_dialog_threads()
        dialog.hide()
        dialog.deleteLater()


def _taxon(data: dict) -> dict:
    return {key: data.get(key) for key in TAXON_KEYS}


def _type(dialog, text: str) -> None:
    picker = dialog.taxon_picker
    if not picker.search_input.isVisible():
        picker.begin_edit()
    picker.search_input.setFocus()
    picker.search_input.selectAll()
    QTest.keyClicks(picker.search_input, text)
    QTest.qWait(250)  # the picker's debounce
    assert picker.is_popup_visible()


def _move_to(dialog, name: str, kind: str = "taxon") -> None:
    """Arrow down to the row ``name`` like a user would."""
    picker = dialog.taxon_picker
    target = next(i for i, row in enumerate(picker.results()) if row.name == name and row.kind == kind)
    # The first row is highlighted already; each Down moves one row.
    for _ in range(target):
        QTest.keyClick(picker.search_input, Qt.Key_Down)
    if target == 0:
        QTest.keyClick(picker.search_input, Qt.Key_Down)
        QTest.keyClick(picker.search_input, Qt.Key_Up)
    assert picker.current_entry().row.name == name


def _pick(dialog, query: str, name: str, *, source_steps: int = 0, kind: str = "taxon") -> None:
    _type(dialog, query)
    _move_to(dialog, name, kind)
    for _ in range(source_steps):
        QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Right)
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Return)
    QTest.qWait(20)


def _shown(dialog):
    QTest.qWait(20)  # the display sync is queued
    selection = dialog.taxon_picker.selection()
    return (selection[0].name, selection[1].sporely_taxon_id) if selection else dialog.taxon_picker.unlinked()


# --- layout ------------------------------------------------------------------


def test_the_picker_replaces_the_four_taxonomy_rows(editor):
    dialog = editor()
    for widget in (dialog.vernacular_input, dialog.genus_input, dialog.species_input,
                   dialog.scientific_name_input, dialog.scientific_hint_label):
        assert not widget.isVisibleTo(dialog)
    assert dialog.taxon_picker.isVisibleTo(dialog)
    assert dialog.vernacular_language_btn.isVisibleTo(dialog)
    assert dialog.taxon_picker.search_input.placeholderText() == "Search for a fungus or scientific name…"


# --- picking -----------------------------------------------------------------


def test_enter_picks_the_shown_default_and_never_saves(editor):
    dialog = editor()
    accepted = []
    dialog.accepted.connect(lambda: accepted.append(True))
    _type(dialog, "Flammulina velutipes")
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Return)
    assert accepted == [] and dialog.isVisible()
    data = dialog.get_data()
    assert data["sporely_taxon_id"] == NORTAXA  # NorTaxa, the filled chip by default
    assert data["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT
    assert (data["genus"], data["species"], data["scientific_name_snapshot"]) == (
        "Flammulina", "velutipes", "Flammulina velutipes")
    assert data["common_name"] == "vintersopp"
    assert _shown(dialog) == ("Flammulina velutipes", NORTAXA)


def test_enter_in_the_search_with_the_results_closed_never_saves(editor):
    dialog = editor()
    accepted = []
    dialog.accepted.connect(lambda: accepted.append(True))
    _type(dialog, "Flammulina")
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Escape)
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Return)
    assert accepted == [] and dialog.isVisible()
    assert dialog.get_data()["sporely_taxon_id"] is None


def test_tab_leaves_the_search_without_picking(editor):
    dialog = editor()
    _type(dialog, "Flammulina velutipes")
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Tab)
    assert dialog.get_data()["sporely_taxon_id"] is None
    assert dialog.taxon_picker.unfinished_text() == "Flammulina velutipes"


def test_both_sources_are_pickable_and_the_card_switches_between_them(editor):
    dialog = editor()
    _pick(dialog, "velutipes", "Flammulina velutipes", source_steps=1)
    assert dialog.get_data()["sporely_taxon_id"] == COL
    assert _shown(dialog) == ("Flammulina velutipes", COL)
    # Kilde on the card re-commits the other concept, and only that.
    card = dialog.taxon_picker.card
    nortaxa = next(b for b in card.source_buttons if b.text().startswith("NorTaxa"))
    nortaxa.click()
    data = dialog.get_data()
    assert data["sporely_taxon_id"] == NORTAXA
    assert data["scientific_name_snapshot"] == "Flammulina velutipes"


def test_a_synonym_pick_stores_its_name_on_the_target_concept(editor):
    dialog = editor()
    _pick(dialog, "Collybia velutipes", "Collybia velutipes", kind="synonym")
    data = dialog.get_data()
    assert data["sporely_taxon_id"] == NORTAXA
    assert data["scientific_name_snapshot"] == "Collybia velutipes"
    assert (data["genus"], data["species"]) == ("Collybia", "velutipes")
    reopened = editor(**data)
    assert _shown(reopened) == ("Collybia velutipes", NORTAXA)
    assert dialog.taxon_picker.selection()[0].kind == "synonym"
    assert _taxon(reopened.get_data()) == _taxon(data)


def test_a_genus_only_pick_binds_the_genus(editor):
    dialog = editor()
    _pick(dialog, "Russula", "Russula")
    data = dialog.get_data()
    assert (data["genus"], data["species"], data["taxon_rank_snapshot"]) == ("Russula", None, "genus")
    assert data["sporely_taxon_id"] is not None
    reopened = editor(**data)
    assert _shown(reopened) == ("Russula", data["sporely_taxon_id"])
    assert "sp." in reopened.taxon_picker.card.title.text()


def test_a_hyphenated_epithet_is_a_concept(editor):
    dialog = editor()
    _pick(dialog, "albo-och", "Cortinarius albo-ochraceus")
    data = dialog.get_data()
    assert data["sporely_taxon_id"] == 92473
    assert (data["genus"], data["species"]) == ("Cortinarius", "albo-ochraceus")


# --- editing an existing observation -----------------------------------------


def _bound(dialog_factory, concept_source_steps: int = 0, **extra) -> dict:
    dialog = dialog_factory(**extra)
    _pick(dialog, "Flammulina velutipes", "Flammulina velutipes", source_steps=concept_source_steps)
    return dialog.get_data()


def test_opening_and_saving_changes_nothing(editor):
    data = _bound(editor, 1, uncertain=1, determination_method=2)
    assert data["sporely_taxon_id"] == COL
    reopened = editor(**data)
    assert _shown(reopened) == ("Flammulina velutipes", COL)
    assert _taxon(reopened.get_data()) == _taxon(data)
    assert reopened.get_data()["uncertain"] and reopened.get_data()["determination_method"] == 2


def test_the_stored_concept_stays_the_default_against_artsorakel_evidence(editor):
    # Stored COL, Artsorakel says NBIC:54701 (NorTaxa): Endre + Enter keeps COL.
    data = _bound(editor, 1)
    data.update(ai_selected_service="artsorakel", ai_selected_taxon_id="NBIC:54701",
                ai_selected_scientific_name="Flammulina velutipes")
    reopened = editor(**data)
    _type(reopened, "Flammulina velutipes")
    QTest.keyClick(reopened.taxon_picker.search_input, Qt.Key_Return)
    assert reopened.get_data()["sporely_taxon_id"] == COL


def test_an_unlinked_older_name_shows_amber_and_saves_unchanged(editor):
    stored = dict(genus="Russula", species="aeruginea", common_name="grønnkremle", uncertain=0)
    dialog = editor(**stored)
    assert _shown(dialog) == ("Russula aeruginea", "legacy")
    data = dialog.get_data()
    assert (data["genus"], data["species"], data["common_name"]) == ("Russula", "aeruginea", "grønnkremle")
    assert data["sporely_taxon_id"] is None


def test_clearing_removes_the_identification(editor):
    data = _bound(editor)
    dialog = editor(**data)
    dialog.taxon_picker.card.clear_button.click()
    cleared = dialog.get_data()
    for key in ("genus", "species", "common_name", "scientific_name_snapshot", "sporely_taxon_id",
                "red_list_category"):
        assert cleared[key] is None, key
    assert _shown(dialog) is None


def test_a_kept_name_is_stored_whole_without_a_taxon(editor):
    data = _bound(editor)
    dialog = editor(**data)
    name = "Cortinarius sp. «SKA-2026-14»"
    _type(dialog, name)
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Up)  # the last row: keep as a name
    assert dialog.taxon_picker.current_entry().kind == "free"
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Return)
    kept = dialog.get_data()
    assert (kept["genus"], kept["species"], kept["common_name"]) == (None, name, None)
    assert kept["sporely_taxon_id"] is None and kept["scientific_name_snapshot"] is None
    assert kept["taxon_identity_proof"] is None and kept["taxon_identity_source_system"] is None
    reopened = editor(**kept)
    assert _shown(reopened) == (name, "free")
    assert _taxon(reopened.get_data()) == _taxon(kept)


# --- unfinished search text on Save ------------------------------------------


def _answer(monkeypatch, label: str) -> None:
    def exec_(box):
        button = next(b for b in box.buttons() if b.text() == label)
        button.click()
        return 0
    monkeypatch.setattr(QMessageBox, "exec", exec_)


@pytest.mark.parametrize("answer", ["Discard search text", "Keep as name without taxon", "Choose an entry"])
def test_save_with_unfinished_search_text_asks(editor, monkeypatch, answer):
    data = _bound(editor)
    dialog = editor(**data)
    _type(dialog, "Gymnopus dryo")
    _answer(monkeypatch, answer)
    proceeds = dialog._confirm_unfinished_taxon_search()
    after = dialog.get_data()
    if answer == "Discard search text":
        assert proceeds and _taxon(after) == _taxon(data)
        assert _shown(dialog) == ("Flammulina velutipes", NORTAXA)
    elif answer == "Keep as name without taxon":
        assert proceeds and (after["genus"], after["species"]) == (None, "Gymnopus dryo")
        assert after["sporely_taxon_id"] is None
    else:
        assert not proceeds and _taxon(after) == _taxon(data)
        assert dialog.taxon_picker.unfinished_text() == "Gymnopus dryo"


def test_save_stops_while_the_user_goes_back_to_choose(editor, monkeypatch):
    dialog = editor()
    accepted = []
    dialog.accepted.connect(lambda: accepted.append(True))
    _type(dialog, "Gymnopus dryo")
    _answer(monkeypatch, "Choose an entry")
    dialog.accept()
    assert accepted == [] and dialog.isVisible()


# --- Artsorakel ----------------------------------------------------------------


def _artsorakel(dialog, predictions: list[dict]) -> None:
    dialog._ai_predictions_by_index[0] = predictions
    dialog._update_ai_table()


_VINTERSOPP = {"probability": 0.82, "scientificName": "Flammulina velutipes", "name": "Flammulina velutipes",
               "vernacularName": "vintersopp", "taxonId": "NBIC:54701", "id": "NBIC:54701"}
_UNKNOWN = {"probability": 0.05, "scientificName": "Gymnopus", "name": "Gymnopus", "vernacularName": "",
            "taxonId": "", "id": ""}


def test_artsorakel_chips_offer_without_applying(editor):
    dialog = editor()
    before = _taxon(dialog.get_data())
    _artsorakel(dialog, [_VINTERSOPP, _UNKNOWN])
    picker = dialog.taxon_picker
    assert [b.property("resolved") for b in picker.ai_buttons] == [True, False]
    assert _taxon(dialog.get_data()) == before


def test_a_resolved_artsorakel_chip_applies_like_copy_to_taxonomy(editor):
    dialog = editor(uncertain=1)
    _artsorakel(dialog, [_VINTERSOPP])
    dialog.taxon_picker.ai_buttons[0].click()
    QTest.qWait(20)
    data = dialog.get_data()
    assert data["sporely_taxon_id"] == NORTAXA
    assert data["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
    assert data["taxon_identity_raw_external_id"] == "NBIC:54701"
    assert data["ai_selected_taxon_id"] == "NBIC:54701"
    assert data["uncertain"]  # no chip changes Usikker
    assert _shown(dialog) == ("Flammulina velutipes", NORTAXA)


def test_an_unresolved_chip_only_searches(editor):
    dialog = editor()
    _artsorakel(dialog, [_UNKNOWN])
    dialog.taxon_picker.ai_buttons[0].click()
    assert dialog.taxon_picker.is_popup_visible()
    assert dialog.get_data()["sporely_taxon_id"] is None


def test_unidentified_disables_the_picker(editor):
    data = _bound(editor)
    dialog = editor(**data)
    dialog.unidentified_checkbox.setChecked(True)
    assert not dialog.taxon_picker.isEnabled()
    assert dialog.get_data()["sporely_taxon_id"] is None
    assert _shown(dialog) is None


# --- through the observations table ---------------------------------------------


def _fresh_db(tmp_path, monkeypatch):
    from database import schema

    path = tmp_path / "observations.sqlite3"
    monkeypatch.setattr(schema, "get_database_path", lambda: path)
    schema.init_database()
    from database.models import ObservationDB

    return ObservationDB, path


def _save(db, obs_id: int, dialog) -> None:
    """Save as the observations tab's edit accept does: its own column
    mapping, then a clear first for Unidentified."""
    updates = observations_tab.ObservationsTab._edit_observation_updates(
        dialog.get_data(), ai_state_json=None, unidentified=dialog.is_unidentified())
    if dialog.is_unidentified():
        db.clear_observation_identification(obs_id)
    db.update_observation(obs_id, **updates)


def _reopen(editor, db, obs_id: int):
    return editor(observation=db.get_observation(obs_id))


def _identity_row(path, obs_id: int) -> tuple:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            "SELECT genus, species, common_name, sporely_taxon_id, scientific_name_snapshot, "
            "taxon_identity_state, taxon_identity_proof FROM observations WHERE id = ?", (obs_id,),
        ).fetchone()
    finally:
        conn.close()


def test_switching_source_survives_the_database_and_a_fresh_editor(editor, tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(date="2026-10-27 12:00")
    dialog = _reopen(editor, db, obs_id)
    _pick(dialog, "Flammulina velutipes", "Flammulina velutipes")
    _save(db, obs_id, dialog)
    assert _identity_row(path, obs_id)[3] == NORTAXA

    reopened = _reopen(editor, db, obs_id)
    assert _shown(reopened) == ("Flammulina velutipes", NORTAXA)
    reopened.taxon_picker.begin_edit()
    _type(reopened, "velutipes")
    _move_to(reopened, "Flammulina velutipes")
    QTest.keyClick(reopened.taxon_picker.search_input, Qt.Key_Right)
    QTest.keyClick(reopened.taxon_picker.search_input, Qt.Key_Return)
    _save(db, obs_id, reopened)
    row = _identity_row(path, obs_id)
    assert row[:5] == ("Flammulina", "velutipes", "vintersopp", COL, "Flammulina velutipes")

    # A fresh editor on the stored row, saved untouched, writes the same row.
    third = _reopen(editor, db, obs_id)
    assert _shown(third) == ("Flammulina velutipes", COL)
    _save(db, obs_id, third)
    assert _identity_row(path, obs_id) == row


def test_a_kept_name_survives_the_database(editor, tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(date="2026-10-27 12:00", genus="Russula", species="aeruginea")
    dialog = _reopen(editor, db, obs_id)
    dialog.taxon_picker.begin_edit()
    dialog.taxon_picker.search_input.clear()
    name = "gul kremle-aktig"
    _type(dialog, name)
    assert dialog.taxon_picker.keep_unfinished_as_name()
    _save(db, obs_id, dialog)
    assert _identity_row(path, obs_id)[:4] == (None, name, None, None)
    reopened = _reopen(editor, db, obs_id)
    assert _shown(reopened) == (name, "free")


def test_endre_then_save_without_typing_keeps_the_identification(editor, monkeypatch):
    data = _bound(editor)
    dialog = editor(**data)
    dialog.taxon_picker.begin_edit()
    asked = []
    monkeypatch.setattr(QMessageBox, "exec", lambda box: asked.append(box.text()) or 0)
    assert dialog._confirm_unfinished_taxon_search()
    assert asked == []
    assert _taxon(dialog.get_data()) == _taxon(data)
    assert _shown(dialog) == ("Flammulina velutipes", NORTAXA)


def test_edit_images_with_unfinished_text_asks_first(editor, monkeypatch):
    dialog = editor()
    _type(dialog, "Gymnopus dryo")
    _answer(monkeypatch, "Choose an entry")
    dialog._on_edit_images_clicked()
    assert not getattr(dialog, "request_edit_images", False) and dialog.isVisible()


# --- review fixes: publishing taxon ID, AI selection, current concept -------


def _column(path, obs_id: int, *columns):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(f"SELECT {', '.join(columns)} FROM observations WHERE id = ?", (obs_id,)).fetchone()
    finally:
        conn.close()


def _stored_bound(editor, db, path):
    """A stored observation bound to NorTaxa Flammulina velutipes, with a
    publishing taxon ID and a published iNaturalist observation."""
    obs_id = db.create_observation(date="2026-10-27 12:00", inaturalist_id=777)
    dialog = _reopen(editor, db, obs_id)
    _pick(dialog, "Flammulina velutipes", "Flammulina velutipes")
    _save(db, obs_id, dialog)
    db.update_observation(obs_id, inaturalist_taxon_id=48701)
    assert _column(path, obs_id, "sporely_taxon_id", "inaturalist_taxon_id") == (NORTAXA, 48701)
    return obs_id


def test_the_publishing_taxon_id_stays_with_its_concept(editor, tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _stored_bound(editor, db, path)

    untouched = _reopen(editor, db, obs_id)
    _save(db, obs_id, untouched)
    assert _column(path, obs_id, "inaturalist_taxon_id") == (48701,)

    same = _reopen(editor, db, obs_id)
    same.taxon_picker.begin_edit()
    QTest.keyClick(same.taxon_picker.search_input, Qt.Key_Return)  # Endre + Enter: the same concept
    _save(db, obs_id, same)
    assert _column(path, obs_id, "sporely_taxon_id", "inaturalist_taxon_id") == (NORTAXA, 48701)

    switched = _reopen(editor, db, obs_id)
    col = next(b for b in switched.taxon_picker.card.source_buttons if b.text().startswith("COL"))
    col.click()
    _save(db, obs_id, switched)
    assert _column(path, obs_id, "sporely_taxon_id", "inaturalist_taxon_id", "inaturalist_id") == (COL, None, 777)


@pytest.mark.parametrize("change", ["free name", "clear", "other species"])
def test_leaving_the_concept_drops_the_publishing_taxon_id(editor, tmp_path, monkeypatch, change):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _stored_bound(editor, db, path)
    dialog = _reopen(editor, db, obs_id)
    if change == "free name":
        dialog.taxon_picker.begin_edit()
        _type(dialog, "Cortinarius sp. 14")
        assert dialog.taxon_picker.keep_unfinished_as_name()
    elif change == "clear":
        dialog.taxon_picker.card.clear_button.click()
    else:
        dialog.taxon_picker.begin_edit()
        _pick(dialog, "Russula", "Russula")
    _save(db, obs_id, dialog)
    assert _column(path, obs_id, "inaturalist_taxon_id", "inaturalist_id") == (None, 777)


def test_clear_drops_the_chosen_ai_determination_but_not_the_predictions(editor, tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _stored_bound(editor, db, path)
    db.update_observation(obs_id, ai_selected_service="artsorakel", ai_selected_taxon_id="NBIC:54701",
                          ai_selected_scientific_name="Flammulina velutipes")
    dialog = _reopen(editor, db, obs_id)
    prediction = dict(_VINTERSOPP)
    dialog._ai_predictions_by_index[0] = [prediction]
    dialog._ai_selected_by_index[0] = prediction
    dialog.taxon_picker.card.clear_button.click()
    assert dialog.get_ai_state()["predictions"] == {0: [prediction]}
    assert dialog.get_ai_state()["selected"] == {}
    _save(db, obs_id, dialog)
    assert _column(path, obs_id, "genus", "species", "sporely_taxon_id", "ai_selected_service",
                   "ai_selected_scientific_name") == (None, None, None, None, None)
    reopened = _reopen(editor, db, obs_id)
    data = reopened.get_data()
    assert (data["genus"], data["species"], data["sporely_taxon_id"]) == (None, None, None)
    assert reopened.taxon_picker.selection() is None and reopened.taxon_picker.unlinked() is None


def test_endre_enter_after_a_source_switch_keeps_the_switched_concept(editor, tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _stored_bound(editor, db, path)
    dialog = _reopen(editor, db, obs_id)
    col = next(b for b in dialog.taxon_picker.card.source_buttons if b.text().startswith("COL"))
    col.click()
    QTest.qWait(20)  # the display sync is queued
    dialog.taxon_picker.begin_edit()
    _type(dialog, "Flammulina velutipes")  # searched again: the switched concept is the default
    assert dialog.taxon_picker.current_entry().row.concepts[dialog.taxon_picker.current_entry().choice] \
        .sporely_taxon_id == COL
    QTest.keyClick(dialog.taxon_picker.search_input, Qt.Key_Return)
    assert dialog.get_data()["sporely_taxon_id"] == COL
    _save(db, obs_id, dialog)
    reopened = _reopen(editor, db, obs_id)
    assert _shown(reopened) == ("Flammulina velutipes", COL)


@pytest.mark.parametrize("answer", ["Choose an entry", "Discard search text"])
def test_edit_photo_on_a_thumbnail_asks_about_unfinished_text(editor, monkeypatch, answer):
    dialog = editor()
    _type(dialog, "Gymnopus dryo")
    _answer(monkeypatch, answer)
    rejected = []
    dialog.rejected.connect(lambda: rejected.append(True))
    dialog._on_gallery_edit_requested(1, "/tmp/photo.jpg")
    if answer == "Choose an entry":
        assert rejected == [] and not dialog.request_edit_images
        assert dialog.taxon_picker.unfinished_text() == "Gymnopus dryo"
    else:
        assert rejected == [True] and dialog.request_edit_images
        assert dialog.get_data()["species"] is None


def test_the_main_gallery_edit_photo_opens_prepare_images():
    opened = []

    class Tab:
        def open_edit_images_direct(self, selected_image_path=None):
            opened.append(selected_image_path)

    observations_tab.ObservationsTab._on_gallery_edit_requested(Tab(), 3, " /x/photo.jpg ")
    assert opened == ["/x/photo.jpg"]
