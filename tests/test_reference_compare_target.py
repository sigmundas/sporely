"""The reference drawer's "Compare with" (redesign stage 7e, owner 2026-11-14), on a real ``MainWindow``.

The drawer searches for the observation's own taxon by default. "Change…"
opens the taxon picker with the observation's AI suggestions under it; a
picked taxon, or a name used as typed, becomes what Library, Community and
My observations search for, and what "New reference…" files the reference
under. The observation's identification never changes. Reset goes back,
and so does showing another observation. Isolated profile under
``tmp_path``, the sample taxonomy, a fake Community search; no network.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF, Qt

import ui.cloud_reference_dialog as community_module
from database.models import ImageDB, MeasurementDB, ObservationDB
from database.reference_library import (
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    TaxonTreatmentRepository,
)
from tests.test_community_results_pane_requests import _FakeSearchWorker
from tests.test_reference_create_dialog import (  # noqa: F401
    _new_work,
    _observation,
    _open_drawer,
    _parse,
    _pick_publication,
    _run_create_dialog,
    _window,
    library,
    no_message_boxes,
    profile,
)
from tools.review_ui.live_lab_harness import create_observation
from tools.review_ui.taxon_sample import sample_lookup
from ui.reference_picker_drawer import reference_ai_entries

PULCHRIPES = 619323  # in the sample taxonomy

AI_STATE = {
    "predictions": {
        "0": [
            {"probability": 0.41, "scientificName": "Cortinarius pulchripes", "name": "Cortinarius pulchripes"},
            # The observation's own taxon and a genus: neither is offered.
            {"probability": 0.30, "scientificName": "Cortinarius limonius", "name": "Cortinarius limonius"},
            {"probability": 0.12, "scientificName": "Cortinarius", "name": "Cortinarius"},
        ],
    },
    "inat_predictions": {
        "0": [{"combined_score": 0.55, "taxon": {"name": "Cortinarius albonigrellus", "rank": "species"}}],
    },
}


@pytest.fixture
def names(tmp_path, monkeypatch):
    """The sample taxonomy as the picker's names database."""
    from ui.main_window import MainWindow

    lookup = sample_lookup(tmp_path / "taxonomy.sqlite3")
    monkeypatch.setattr(MainWindow, "reference_picker_taxon_lookup", lambda self: lookup)
    return lookup


@pytest.fixture
def community(monkeypatch):
    """Community searches, recorded instead of sent to Sporely Cloud."""
    searches: list[tuple[str, str]] = []

    def make(genus, species):
        searches.append((genus, species))
        return _FakeSearchWorker(genus, species)

    monkeypatch.setattr(community_module, "_CloudSearchWorker", make)
    return searches


def _rows(page):
    return [
        page.results_list.itemWidget(page.results_list.item(i)) for i in range(page.results_list.count())
        if page.results_list.itemWidget(page.results_list.item(i)) is not None
    ]


def _texts(page):
    return [page.results_list.item(i).text() for i in range(page.results_list.count())]


def _change(qtbot, page):
    qtbot.mouseClick(page.change_target_btn, Qt.LeftButton)
    picker = page.target_picker
    qtbot.waitUntil(lambda: picker.isVisible() and picker.search_input.isVisible(), timeout=2000)
    return picker


def _type_and_pick(qtbot, page, text, *, entry=0):
    """Type ``text`` in the picker and take result ``entry`` (the last one is "use as a name")."""
    picker = _change(qtbot, page)
    qtbot.keyClicks(picker.search_input, text)
    qtbot.waitUntil(picker.is_popup_visible, timeout=2000)
    if entry < 0:
        entry = picker.model.rowCount() + entry
    for _step in range(entry):
        qtbot.keyClick(picker.search_input, Qt.Key_Down)
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    qtbot.waitUntil(lambda: not picker.isVisible(), timeout=2000)


def _earlier(profile, key, genus, species):
    """Another of the user's observations with measured spores."""
    observation_id = create_observation(profile, key, genus=genus, species=species, date="2025-09-21")
    image_id = ImageDB.add_image(observation_id, str(profile.jpeg(f"{key}.JPG", seed=6)), "microscope", scale=0.1)
    for index in range(4):
        x = 40 + index * 40
        MeasurementDB.add_measurement(
            image_id, 10.0 + index * 0.2, 5.5, measurement_type="spores",
            points=[QPointF(x, 50), QPointF(x + 28, 50), QPointF(x + 14, 42), QPointF(x + 14, 58)],
        )
    return observation_id


def test_the_drawer_compares_with_the_observation_taxon_by_default(qtbot, profile, library, names):
    window = _window(qtbot, _observation(profile))
    page = _open_drawer(qtbot, window)
    assert "Cortinarius limonius" in page.target_label.text()
    assert page.change_target_btn.isVisible() and not page.reset_target_btn.isVisible()
    assert page.comparison_target() is None
    assert page.target_picker is None  # the names database opens only on Change


def test_a_name_used_as_typed_retargets_library_my_observations_and_community(
    qtbot, profile, library, names, community,
):
    observation_id = _observation(profile)
    _earlier(profile, "earlier-rubellus", "Cortinarius", "rubellus")
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)
    assert any("limonius" in row.taxon_label.full_text() for row in _rows(page))

    _type_and_pick(qtbot, page, "Cortinarius rubellus", entry=-1)  # not in the taxonomy: the name as typed

    assert "Cortinarius rubellus" in page.target_label.text()
    assert page.reset_target_btn.isVisible() and page.change_target_btn.isVisible()
    assert page.comparison_target()["taxon_id"] is None
    # Library: "Only this taxon" is now the chosen name's sets.
    assert page.only_this_taxon_checkbox.isChecked()
    assert [row.taxon_label.full_text() for row in _rows(page)] == ["Cortinarius rubellus Cooke"]
    # My observations: the earlier rubellus, named.
    item = page.my_observations_list.item(0)
    assert item.data(Qt.UserRole) is not None and "Cortinarius rubellus" in item.toolTip()
    # Community searches for it.
    page.show_source("community")
    assert community[-1] == ("Cortinarius", "rubellus")
    # A reference needs a species from the names database.
    assert not page.new_reference_btn.isEnabled()
    # The observation's identification is untouched.
    assert ObservationDB.get_observation(observation_id)["species"] == "limonius"

    qtbot.mouseClick(page.reset_target_btn, Qt.LeftButton)
    assert page.comparison_target() is None and not page.reset_target_btn.isVisible()
    assert "Cortinarius limonius" in page.target_label.text()
    assert all("rubellus" not in row.taxon_label.full_text() for row in _rows(page))
    assert community[-1] == ("Cortinarius", "limonius")
    assert page.new_reference_btn.isEnabled()


def test_ai_suggestions_are_offered_and_one_becomes_the_target(qtbot, profile, library, names, monkeypatch):
    observation_id = _observation(profile)
    ObservationDB.update_observation(observation_id, ai_state_json=json.dumps(AI_STATE))
    window = _window(qtbot, observation_id)

    def no_cloud(*_args, **_kwargs):
        raise AssertionError("Compare with must not ask Sporely Cloud for AI suggestions")

    monkeypatch.setattr(window.observations_tab, "_load_observation_ai_state", no_cloud)
    page = _open_drawer(qtbot, window)
    picker = _change(qtbot, page)
    # Highest score first; the own taxon and the genus are left out.
    assert [button.text() for button in picker.ai_buttons] == [
        "Cortinarius albonigrellus  55 %", "Cortinarius pulchripes  41 %",
    ]
    assert picker.ai_strip.isVisible()

    qtbot.mouseClick(picker.ai_buttons[1], Qt.LeftButton)  # no identifier: it searches the name
    qtbot.waitUntil(picker.is_popup_visible, timeout=2000)
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    qtbot.waitUntil(lambda: not picker.isVisible(), timeout=2000)

    target = page.comparison_target()
    assert (target["taxon_id"], target["genus"], target["species"]) == (PULCHRIPES, "Cortinarius", "pulchripes")
    assert "No measurement sets for this taxon" in " ".join(_texts(page))
    assert page.new_reference_btn.isEnabled()


def test_a_new_reference_is_filed_under_the_chosen_taxon(qtbot, monkeypatch, profile, library, names, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)
    _type_and_pick(qtbot, page, "Cortinarius pulchripes")
    assert page.comparison_target()["taxon_id"] == PULCHRIPES
    work = _new_work()

    def drive(dialog):
        assert "Cortinarius pulchripes" in dialog.taxon_label.text()
        assert dialog.editor.name_as_published_input.text() == "Cortinarius pulchripes"
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 1 and seen["dialog"].added_to_plot
    uses = ObservationReferenceUseRepository.list_for_observation(observation_id)
    assert len(uses) == 1 and uses[0].role == "compared"
    treatment = TaxonTreatmentRepository.get(MeasurementSetRepository.get(str(uses[0].reference_measurement_set_id)).taxon_treatment_id)
    assert str(treatment.taxon_id) == str(PULCHRIPES)
    assert treatment.name_as_published == "Cortinarius pulchripes"
    observation = ObservationDB.get_observation(observation_id)
    assert (observation["species"], observation["sporely_taxon_id"]) == ("limonius", 7)
    # Listed and ticked under the chosen taxon, and no question was asked.
    row = page.row_for(str(uses[0].reference_measurement_set_id))
    assert row is not None and row.is_checked()
    assert no_message_boxes == []


def test_another_observation_compares_with_its_own_taxon(qtbot, profile, library, names):
    first = _observation(profile)
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open_drawer(qtbot, window)
    _type_and_pick(qtbot, page, "Cortinarius pulchripes")
    assert page.comparison_target() is not None

    window.on_observation_selected(second, "Cortinarius limonius", switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == second, timeout=5000)
    page.load()
    assert page.comparison_target() is None
    assert "Cortinarius limonius" in page.target_label.text() and not page.reset_target_btn.isVisible()

    window.on_observation_selected(first, "Cortinarius limonius", switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=5000)
    page.load()
    assert page.comparison_target() is None  # not kept for when it comes back


def test_esc_while_changing_keeps_the_target_and_the_drawer(qtbot, profile, library, names):
    window = _window(qtbot, _observation(profile))
    page = _open_drawer(qtbot, window)
    picker = _change(qtbot, page)
    qtbot.keyClicks(picker.search_input, "Cortinarius")
    qtbot.waitUntil(picker.is_popup_visible, timeout=2000)
    qtbot.keyClick(picker.search_input, Qt.Key_Escape)  # closes the list
    assert picker.isVisible() and not picker.is_popup_visible()
    qtbot.keyClick(picker.search_input, Qt.Key_Escape)  # puts the line back
    assert not picker.isVisible() and page.change_target_btn.isVisible()
    assert window.reference_drawer.is_open()
    assert page.comparison_target() is None


def test_ai_entries_read_names_scores_and_identifiers():
    entries = reference_ai_entries({
        "predictions": {"0": [
            {"probability": 0.6, "scientificName": "Inocybe geophylla (Sowerby) P. Kumm.", "scientific_name_id": "NBIC:123"},
            {"probability": 0.2, "scientificName": "Inocybe"},
        ]},
        "inat_predictions": {"1": [{"combined_score": 0.3, "taxon": {"name": "Inocybe lacera"}}]},
    })
    assert [(e["source"], e["genus"], e["species"], e["score"]) for e in entries] == [
        ("arts", "Inocybe", "geophylla", 0.6),
        ("arts", "Inocybe", "", 0.2),
        ("inat", "Inocybe", "lacera", 0.3),
    ]
    assert entries[0]["external_id"] == "NBIC:123"
    assert reference_ai_entries(None) == [] and reference_ai_entries({"predictions": "junk"}) == []


def test_a_genus_compares_with_every_species_in_it(qtbot, profile, library, names):
    observation_id = _observation(profile)
    earlier = _earlier(profile, "earlier-rubellus", "Cortinarius", "rubellus")
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)

    _type_and_pick(qtbot, page, "Cortinarius", entry=-1)  # the genus, as typed

    assert page.comparison_target()["species"] == ""
    taxa = {row.taxon_label.full_text() for row in _rows(page)}
    assert "Cortinarius rubellus Cooke" in taxa and any("limonius" in taxon for taxon in taxa)
    assert not page.new_reference_btn.isEnabled()  # a reference is for a species
    # My observations lists the genus, each with its own name, and plots it under that name.
    page.show_source("my_observations")
    row = page.my_observation_row(earlier)
    assert row is not None
    qtbot.mouseClick(row.checkbox, Qt.LeftButton)
    plotted = [
        entry["data"] for entry in window.reference_series
        if entry.get("data", {}).get("source_kind") == "observation"
    ]
    assert [(data["genus"], data["species"]) for data in plotted] == [("Cortinarius", "rubellus")]


@pytest.mark.parametrize("taxon_id", [None, 7], ids=["unlinked", "linked"])
def test_the_observation_s_own_name_is_not_another_taxon(qtbot, profile, library, names, taxon_id):
    window = _window(qtbot, _observation(profile, taxon_id=taxon_id))
    page = _open_drawer(qtbot, window)
    _type_and_pick(qtbot, page, "Cortinarius limonius", entry=-1)
    assert page.comparison_target() is None and not page.reset_target_btn.isVisible()


def test_a_series_plotted_under_another_target_stays_ticked_and_removable_after_reset(qtbot, profile, library, names):
    observation_id = create_observation(profile, "genus-only", genus="Cortinarius", species="")
    ObservationDB.update_observation(observation_id, species_guess="")  # identified to genus only
    image_id = ImageDB.add_image(observation_id, str(profile.jpeg("genus-only.JPG", seed=5)), "microscope", scale=0.1)
    MeasurementDB.add_measurement(
        image_id, 9.5, 5.2, measurement_type="spores",
        points=[QPointF(40, 50), QPointF(68, 50), QPointF(54, 42), QPointF(54, 58)],
    )
    earlier = _earlier(profile, "earlier-rubellus", "Cortinarius", "rubellus")
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)
    _type_and_pick(qtbot, page, "Cortinarius rubellus", entry=-1)
    page.show_source("my_observations")
    qtbot.mouseClick(page.my_observation_row(earlier).checkbox, Qt.LeftButton)
    observed = lambda: [  # noqa: E731
        entry for entry in window.reference_series if entry.get("data", {}).get("source_kind") == "observation"
    ]
    assert len(observed()) == 1

    qtbot.mouseClick(page.reset_target_btn, Qt.LeftButton)  # the genus: rubellus is listed again
    row = page.my_observation_row(earlier)
    assert row is not None and row.is_checked()
    qtbot.mouseClick(row.checkbox, Qt.LeftButton)
    assert observed() == []


def test_a_change_begun_for_one_observation_ends_when_another_is_shown(qtbot, profile, library, names):
    first = _observation(profile)
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open_drawer(qtbot, window)
    picker = _change(qtbot, page)
    qtbot.keyClicks(picker.search_input, "Cortinarius")
    window.on_observation_selected(second, "Cortinarius limonius", switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == second, timeout=5000)
    assert picker.isHidden() and page.change_target_btn.isVisible()
    assert page.comparison_target() is None


def test_a_target_is_dropped_when_another_observation_is_shown_with_the_drawer_closed(
    qtbot, profile, library, names, community,
):
    first = _observation(profile)
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open_drawer(qtbot, window)
    page.show_source("community")
    _type_and_pick(qtbot, page, "Cortinarius pulchripes")
    assert community[-1] == ("Cortinarius", "pulchripes")
    qtbot.mouseClick(window.ref_add_reference_btn, Qt.LeftButton)  # closes the drawer
    qtbot.waitUntil(lambda: not window.reference_drawer.is_open(), timeout=2000)

    for observation_id in (second, first):
        window.on_observation_selected(observation_id, "Cortinarius limonius", switch_tab=False)
        qtbot.waitUntil(lambda oid=observation_id: window.active_observation_id == oid, timeout=5000)
    page = _open_drawer(qtbot, window)
    assert page.comparison_target() is None
    assert "Cortinarius limonius" in page.target_label.text() and not page.reset_target_btn.isVisible()
    assert community[-1] == ("Cortinarius", "limonius")


def test_change_asks_for_the_names_database_each_time(qtbot, profile, library, monkeypatch, tmp_path):
    from ui.main_window import MainWindow

    first, second = sample_lookup(tmp_path / "a.sqlite3"), sample_lookup(tmp_path / "b.sqlite3", language="sv")
    lookups = [first, second]
    monkeypatch.setattr(MainWindow, "reference_picker_taxon_lookup", lambda self: lookups.pop(0) if lookups else second)
    searched = []
    original = second.search_taxa
    monkeypatch.setattr(second, "search_taxa", lambda *a, **k: searched.append(a[0]) or original(*a, **k))
    window = _window(qtbot, _observation(profile))
    page = _open_drawer(qtbot, window)
    picker = _change(qtbot, page)
    qtbot.keyClick(picker.search_input, Qt.Key_Escape)
    picker = _change(qtbot, page)  # the names language changed meanwhile: the host gives another
    qtbot.keyClicks(picker.search_input, "Cortinarius")
    qtbot.waitUntil(picker.is_popup_visible, timeout=2000)
    assert searched and searched[-1] == "Cortinarius"


def test_a_change_left_open_in_a_closed_drawer_ends_when_another_observation_is_shown(qtbot, profile, library, names):
    first = _observation(profile)
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open_drawer(qtbot, window)
    _change(qtbot, page)
    qtbot.mouseClick(window.ref_add_reference_btn, Qt.LeftButton)  # closes the drawer, the search still open in it
    qtbot.waitUntil(lambda: not window.reference_drawer.is_open(), timeout=2000)
    window.on_observation_selected(second, "Cortinarius limonius", switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == second, timeout=5000)
    page = _open_drawer(qtbot, window)
    assert page.target_picker.isHidden() and page.change_target_btn.isVisible()
