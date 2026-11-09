"""The reference drawer over the Analysis plot (redesign plan stage 7b), on a real ``MainWindow``.

Ticking a library measurement set attaches it to the observation and plots
it; unticking detaches it; a ticked set's role is changed in its expanded
row (owner's decisions 2026-11-09). The window runs against an isolated
profile under ``tmp_path``; the library is seeded like the reference
renderer scenarios. No network, no live data.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF, Qt

from database.models import ImageDB, MeasurementDB, ObservationDB
from database.reference_library import ObservationReferenceUseRepository
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


def _observation(profile, key="default"):
    observation_id = create_observation(profile, key, genus="Cortinarius", species="limonius")
    ObservationDB.update_observation(observation_id, sporely_taxon_id=7)
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg(f"{key}.JPG", seed=5)), "microscope", scale=0.1, objective_name="100X",
    )
    for index in range(6):
        x = 40 + index * 40
        MeasurementDB.add_measurement(
            image_id, 9.0 + index * 0.3, 5.0 + index * 0.1, measurement_type="Spores",
            points=[QPointF(x, 50), QPointF(x + 28, 50), QPointF(x + 14, 42), QPointF(x + 14, 58)],
        )
    return observation_id


@pytest.fixture
def library(profile):
    from tools.review_ui.scenarios.references import _seed_library

    _work, sets = _seed_library()
    return sets


def _window(qtbot, observation_id):
    from ui.main_window import MainWindow

    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Cortinarius limonius", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    qtbot.waitUntil(lambda: window.active_observation_id == observation_id, timeout=5000)
    qtbot.wait(300)
    return window


def _open(qtbot, window):
    qtbot.mouseClick(window.ref_add_reference_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: window.reference_drawer.is_open() and window.reference_drawer.isVisible(), timeout=2000)
    return window.reference_picker_page


def _uses(observation_id):
    return {use.reference_measurement_set_id: use.role for use in ObservationReferenceUseRepository.list_for_observation(observation_id)}


def test_add_reference_opens_the_drawer_over_the_plot_and_esc_closes_it(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    plot_size = window.plot_width_splitter.size()
    _open(qtbot, window)
    assert window.ref_add_reference_btn.isChecked()
    assert window.analysis_plot_area.isAncestorOf(window.reference_drawer)
    qtbot.wait(300)
    assert window.plot_width_splitter.size() == plot_size  # overlays, does not resize
    # Beside the button that opened it: at the plot's left edge, full height.
    drawer = window.reference_drawer
    assert drawer.geometry().left() == 0
    assert drawer.height() == window.analysis_plot_area.height()
    button_right = window.ref_add_reference_btn.mapTo(window, window.ref_add_reference_btn.rect().topRight()).x()
    drawer_left = drawer.mapTo(window, drawer.rect().topLeft()).x()
    assert 0 <= drawer_left - button_right < 60
    window.resize(1200, 800)
    qtbot.waitUntil(lambda: drawer.height() == window.analysis_plot_area.height(), timeout=2000)
    assert drawer.geometry().left() == 0
    window.activateWindow()
    qtbot.waitActive(window)
    window.reference_picker_page.search_input.setFocus()
    qtbot.keyClick(window.reference_picker_page.search_input, Qt.Key_Escape)
    qtbot.waitUntil(lambda: not window.reference_drawer.isVisible(), timeout=2000)
    assert not window.ref_add_reference_btn.isChecked()


def test_ticking_attaches_and_plots_and_unticking_detaches(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    set_id = library[0].id
    row = page.row_for(set_id)
    assert row is not None and not row.is_checked()  # only this taxon: the three C. limonius sets
    assert page.row_for(library[1].id) is not None

    row.checkbox.click()
    assert _uses(observation_id) == {set_id: "compared"}
    assert set_id in window._current_attached_measurement_set_ids()  # on the plot and in the list
    assert window.comparison_list.isVisible()

    page.row_for(set_id).checkbox.click()
    assert _uses(observation_id) == {}
    assert set_id not in window._current_attached_measurement_set_ids()


def test_an_attached_set_is_ticked_and_its_role_changes_in_the_expanded_row(qtbot, profile, library):
    observation_id = _observation(profile)
    ObservationReferenceUseRepository.attach(observation_id, library[0].id, role="compared")
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    assert row.is_checked()
    qtbot.mouseClick(row.expand_button, Qt.LeftButton)
    assert row.is_expanded() and row.role_row.isVisible() and row.details_link.isVisible()
    row.role_combo.setCurrentIndex(row.role_combo.findData("supports_identification"))
    assert _uses(observation_id) == {library[0].id: "supports_identification"}
    # Still expanded and ticked after a reload (the drawer reopened).
    window.reference_drawer.close_drawer()
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    assert row.is_checked() and row.role() == "supports_identification" and row.is_expanded()


def test_only_this_taxon_hides_other_taxa_until_turned_off(qtbot, profile, library):
    window = _window(qtbot, _observation(profile))
    page = _open(qtbot, window)
    texts = lambda: [page.results_list.item(i).text() for i in range(page.results_list.count())]  # noqa: E731
    rows = lambda: [page.results_list.itemWidget(page.results_list.item(i)) for i in range(page.results_list.count())]  # noqa: E731
    assert page.only_this_taxon_checkbox.isChecked()
    assert all("rubellus" not in row.taxon_label.full_text() for row in rows() if row is not None)
    page.only_this_taxon_checkbox.setChecked(False)
    assert any(row is not None and "rubellus" in row.taxon_label.full_text() for row in rows())
    page.search_input.setText("Brandrud")
    assert [row.taxon_label.full_text() for row in rows() if row is not None] == ["Cortinarius rubellus Cooke"]
    assert texts()


def test_a_declined_public_notice_attaches_nothing(qtbot, profile, library, monkeypatch):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    asked = []
    monkeypatch.setattr(window, "_confirm_picker_batch_attach", lambda oid, roles: asked.append(oid) or False)
    page = _open(qtbot, window)
    page.row_for(library[0].id).checkbox.click()
    assert asked == [observation_id]
    assert not page.row_for(library[0].id).is_checked()
    assert _uses(observation_id) == {}


def test_the_notice_is_asked_once_per_opening(qtbot, profile, library, monkeypatch):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    asked = []
    monkeypatch.setattr(window, "_confirm_picker_batch_attach", lambda oid, roles: asked.append(oid) or True)
    page = _open(qtbot, window)
    page.row_for(library[0].id).checkbox.click()
    page.row_for(library[1].id).checkbox.click()
    assert asked == [observation_id]
    window.reference_drawer.close_drawer()
    page = _open(qtbot, window)
    page.row_for(library[2].id).checkbox.click()
    assert asked == [observation_id, observation_id]
    assert set(_uses(observation_id)) == {library[0].id, library[1].id, library[2].id}


def test_the_drawer_follows_a_new_active_observation(qtbot, profile, library):
    first = _observation(profile, "first")
    ObservationReferenceUseRepository.attach(first, library[0].id, role="compared")
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open(qtbot, window)
    assert page.row_for(library[0].id).is_checked()
    window.on_observation_selected(second, "Cortinarius limonius", switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == second, timeout=5000)
    assert not page.row_for(library[0].id).is_checked()  # the second has nothing attached
    page.row_for(library[1].id).checkbox.click()
    assert set(_uses(second)) == {library[1].id}
    assert set(_uses(first)) == {library[0].id}


def test_a_tick_for_an_observation_that_is_no_longer_active_changes_nothing(qtbot, profile, library):
    first = _observation(profile, "first")
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open(qtbot, window)
    window.active_observation_id = second  # switched without the usual reload
    page.row_for(library[0].id).checkbox.click()
    assert _uses(first) == {} and _uses(second) == {}
    assert page.status_label.isVisible()


def test_details_opens_the_library_on_that_measurement_set(qtbot, profile, library, monkeypatch):
    from ui import reference_library_manager_dialog as manager

    seen = []

    def fake_exec(dialog):
        current = dialog._current_measurement_set
        seen.append(getattr(current, "id", None))
        return 0

    monkeypatch.setattr(manager.ReferenceLibraryManagerDialog, "exec", fake_exec)
    window = _window(qtbot, _observation(profile))
    page = _open(qtbot, window)
    row = page.row_for(library[1].id)
    qtbot.mouseClick(row.expand_button, Qt.LeftButton)
    qtbot.mouseClick(row.details_link, Qt.LeftButton)
    assert seen == [library[1].id]


# ---------------------------------------------------------------------------
# Codex review of stage 7b
# ---------------------------------------------------------------------------


def test_stale_plot_rows_from_another_observation_are_never_detached(qtbot, profile, library):
    first = _observation(profile, "first")
    ObservationReferenceUseRepository.attach(first, library[0].id, role="compared")
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    # The in-memory plot rows still describe the first observation while the
    # second is active (an observation switch that does not reload them).
    window.active_observation_id = second
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    assert not row.is_checked()  # from the database: nothing attached to the second
    row.checkbox.click()
    assert set(_uses(second)) == {library[0].id}
    page.row_for(library[0].id).checkbox.click()
    assert _uses(second) == {}
    assert _uses(first) == {library[0].id: "compared"}  # untouched


def test_a_rebuild_during_the_notice_attaches_nothing_and_does_not_crash(qtbot, profile, library, monkeypatch):
    first = _observation(profile, "first")
    second = _observation(profile, "second")
    window = _window(qtbot, first)
    page = _open(qtbot, window)

    def notice_then_switch(oid, roles):
        window.active_observation_id = second
        page.load()  # the list is rebuilt while the notice is open
        return True

    monkeypatch.setattr(window, "_confirm_picker_batch_attach", notice_then_switch)
    page.row_for(library[0].id).checkbox.click()
    assert _uses(first) == {} and _uses(second) == {}
    assert not page._attach_confirmed  # the second observation still gets its notice


def test_a_failed_role_change_restores_the_selector(qtbot, profile, library, monkeypatch):
    import sqlite3

    observation_id = _observation(profile)
    ObservationReferenceUseRepository.attach(observation_id, library[0].id, role="compared")
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    row.set_expanded(True)

    def locked(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(ObservationReferenceUseRepository, "update", staticmethod(locked))
    row.role_combo.setCurrentIndex(row.role_combo.findData("contradicts"))
    assert row.role() == "compared"
    assert page.status_label.isVisible() and "locked" in page.status_label.text()
    assert _uses(observation_id) == {library[0].id: "compared"}


def test_removing_a_reference_in_the_comparison_list_unticks_it_in_the_drawer(qtbot, profile, library):
    observation_id = _observation(profile)
    ObservationReferenceUseRepository.attach(observation_id, library[0].id, role="compared")
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    assert page.row_for(library[0].id).is_checked()
    entry = next(e for e in window.reference_series if e["data"].get("reference_measurement_set_id") == library[0].id)
    window._remove_reference_series_key(entry["key"])  # the comparison list's Remove
    qtbot.waitUntil(lambda: not page.row_for(library[0].id).is_checked(), timeout=2000)
    assert _uses(observation_id) == {}


def test_reticking_attaches_as_compared_again(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    row.checkbox.click()
    row = page.row_for(library[0].id)
    row.role_combo.setCurrentIndex(row.role_combo.findData("supports_identification"))
    assert _uses(observation_id) == {library[0].id: "supports_identification"}
    page.row_for(library[0].id).checkbox.click()
    page.row_for(library[0].id).checkbox.click()
    assert _uses(observation_id) == {library[0].id: "compared"}


def test_details_finds_a_publication_beyond_the_first_search_results(qtbot, profile, library, monkeypatch):
    from database.reference_library import ReferenceWorkRepository
    from ui import reference_library_manager_dialog as manager

    real_search = ReferenceWorkRepository.search

    def first_page_without_it(query=None, limit=200):
        works = real_search(query, limit=limit)
        return works if query else [w for w in works if "Cortinarius species" not in (w.title or "")]

    monkeypatch.setattr(ReferenceWorkRepository, "search", staticmethod(first_page_without_it))
    seen = []
    monkeypatch.setattr(manager.ReferenceLibraryManagerDialog, "exec",
                        lambda dialog: seen.append(getattr(dialog._current_measurement_set, "id", None)) or 0)
    window = _window(qtbot, _observation(profile))
    page = _open(qtbot, window)
    row = page.row_for(library[2].id)
    row.set_expanded(True)
    qtbot.mouseClick(row.details_link, Qt.LeftButton)
    assert seen == [library[2].id]
