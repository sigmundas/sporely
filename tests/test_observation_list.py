"""The observation list's rail and drawer (redesign stage 9b).

Real ``MainWindow`` in an isolated profile under ``tmp_path``; no network.
The list is a second view on the Observations table's model: same rows,
order and filter; choosing a row opens it on the page shown; the table's
selection follows the observation shown on every page.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt

from database.models import ImageDB, ObservationDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile
from ui.observation_list_panel import observation_list_name

NAMES = (("Gulgrønn lærhatt", "Cortinarius", "olivaceofuscus"), ("", "Testomyces", "exemplaris"), ("Knuskkjuke", "Fomes", "fomentarius"))


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def window(qtbot, profile):
    from ui.main_window import MainWindow

    first = create_observation(profile)
    ImageDB.add_image(first, str(profile.jpeg("slide.JPG", seed=3)), "microscope", scale=0.05, objective_name="63X")
    others = [
        ObservationDB.create_observation(date=f"2026-10-2{n} 10:00", genus=g, species=s, common_name=c or None)
        for n, (c, g, s) in enumerate(NAMES)
    ]
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    tab = window.observations_tab
    qtbot.waitUntil(lambda: tab.table.rowCount() == 4, timeout=5000)
    window.ids_for_test = [first, *others]
    return window


def _row(window, observation_id):
    return window.observations_tab._find_table_row_for_observation(observation_id)


def _open_in_measure(qtbot, window, observation_id):
    tab = window.observations_tab
    tab.table.selectRow(_row(window, observation_id))
    tab.set_selected_as_active(switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.active_observation_id == observation_id, timeout=3000)


def _open_drawer(qtbot, window):
    qtbot.mouseClick(window.observation_rail, Qt.LeftButton)
    qtbot.waitUntil(window.observation_drawer.is_open, timeout=2000)
    qtbot.wait(260)  # the slide


def test_the_rail_shows_on_images_measure_and_analysis_only(qtbot, window):
    rail = window.observation_rail
    for page, shown in (
        ("observations_tab", False), ("measure_tab", True), ("analysis_tab", True),
        ("live_lab_tab", True), ("ingestion_hub_tab", False),
    ):
        window.tab_widget.setCurrentWidget(getattr(window, page))
        assert rail.isVisible() is shown, page


def test_the_list_shows_the_tables_rows_in_its_order(qtbot, window):
    tab = window.observations_tab
    view = window.observation_list_panel.list_view
    model = view.model()
    assert model is tab.table.model()
    names = [observation_list_name(model.index(row, 0)) for row in range(model.rowCount())]
    assert "Gulgrønn lærhatt" in names and "Testomyces exemplaris" in names  # scientific when no common name
    tab.table.sortItems(1, Qt.AscendingOrder)
    resorted = [observation_list_name(model.index(row, 0)) for row in range(model.rowCount())]
    assert resorted != names or tab.table.horizontalHeader().sortIndicatorSection() == 1
    for row in range(model.rowCount()):
        assert view.visualRect(model.index(row, 0)).top() < view.visualRect(model.index(row, 0)).bottom()


def test_choosing_an_observation_in_the_drawer_opens_it_on_the_page_shown(qtbot, window):
    first, chosen = window.ids_for_test[0], window.ids_for_test[3]
    _open_in_measure(qtbot, window, first)
    _open_drawer(qtbot, window)
    view = window.observation_list_panel.list_view
    index = view.model().index(_row(window, chosen), 0)
    assert window.observation_list_panel.marked_row() == _row(window, first)

    qtbot.mouseClick(view.viewport(), Qt.LeftButton, pos=view.visualRect(index).center())
    qtbot.waitUntil(lambda: window.active_observation_id == chosen, timeout=3000)
    assert window.tab_widget.currentWidget() is window.measure_tab
    assert window.observations_tab.selected_observation_ids() == [chosen]
    qtbot.waitUntil(lambda: not window.observation_drawer.is_open(), timeout=2000)


def test_enter_opens_the_current_row_and_esc_closes(qtbot, window):
    first = window.ids_for_test[0]
    _open_in_measure(qtbot, window, first)
    _open_drawer(qtbot, window)
    view = window.observation_list_panel.list_view
    target_row = (_row(window, first) + 1) % view.model().rowCount()
    view.setCurrentIndex(view.model().index(target_row, 0))
    qtbot.keyClick(view, Qt.Key_Return)
    target = window.observations_tab._observation_id_for_row(target_row)
    qtbot.waitUntil(lambda: window.active_observation_id == target, timeout=3000)

    _open_drawer(qtbot, window)
    qtbot.keyClick(window.observation_drawer, Qt.Key_Escape)
    assert not window.observation_drawer.is_open()
    assert window.active_observation_id == target


def test_search_is_shared_with_the_table(qtbot, window):
    tab = window.observations_tab
    panel = window.observation_list_panel
    window.tab_widget.setCurrentWidget(window.measure_tab)
    _open_drawer(qtbot, window)
    panel.search_input.setText("Knusk")
    assert tab.search_input.text() == "Knusk"
    qtbot.waitUntil(lambda: panel.list_view.model().rowCount() == 1, timeout=3000)
    assert observation_list_name(panel.list_view.model().index(0, 0)) == "Knuskkjuke"

    tab.search_input.setText("")
    assert panel.search_input.text() == ""
    qtbot.waitUntil(lambda: panel.list_view.model().rowCount() == 4, timeout=3000)


def test_the_table_follows_the_observation_shown(qtbot, window):
    """Calibration's “Go to observation” loads another observation in Mål."""
    first, other = window.ids_for_test[0], window.ids_for_test[2]
    _open_in_measure(qtbot, window, first)
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: window.observations_tab.selected_observation_ids() == [other], timeout=2000)
    qtbot.waitUntil(lambda: window.observation_list_panel.marked_row() == _row(window, other), timeout=2000)
    window.tab_widget.setCurrentWidget(window.observations_tab)
    assert window.observations_tab.selected_observation_ids() == [other]


def test_table_goes_to_view_and_leaving_closes_the_drawer(qtbot, window):
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    _open_drawer(qtbot, window)
    qtbot.mouseClick(window.observation_list_panel.table_button, Qt.LeftButton)
    assert window.tab_widget.currentWidget() is window.observations_tab
    assert not window.observation_drawer.is_open()

    window.tab_widget.setCurrentWidget(window.measure_tab)
    _open_drawer(qtbot, window)
    window.tab_widget.setCurrentWidget(window.ingestion_hub_tab)
    assert not window.observation_drawer.is_open()


def test_a_batch_chosen_in_view_survives_a_background_activation(qtbot, window):
    """Publishing plots activates each observation and restores the active one."""
    from PySide6.QtCore import QItemSelectionModel

    tab = window.observations_tab
    first, second, third = window.ids_for_test[:3]
    _open_in_measure(qtbot, window, first)
    window.tab_widget.setCurrentWidget(window.observations_tab)
    selection = tab.table.selectionModel()
    for observation_id in (second, third):
        selection.select(
            tab.table.model().index(_row(window, observation_id), 0),
            QItemSelectionModel.Select | QItemSelectionModel.Rows,
        )
    batch = sorted(tab.selected_observation_ids())
    assert len(batch) == 3
    window.on_observation_selected(first, "First", switch_tab=False, suppress_gallery=True)
    qtbot.wait(50)
    assert sorted(tab.selected_observation_ids()) == batch


def test_numbers_stay_when_the_table_shows_thumbnails(qtbot, window):
    tab = window.observations_tab
    model = window.observation_list_panel.list_view.model()
    tab.show_table_thumbnails_checkbox.setChecked(True)
    qtbot.wait(300)
    from ui.observation_list_panel import observation_list_number

    numbers = {observation_list_number(model.index(row, 0)) for row in range(model.rowCount())}
    assert numbers == {str(observation_id) for observation_id in window.ids_for_test}


def test_no_hidden_row_answers_enter(qtbot, window):
    panel = window.observation_list_panel
    window.tab_widget.setCurrentWidget(window.measure_tab)
    _open_drawer(qtbot, window)
    panel.list_view.setCurrentIndex(panel.list_view.model().index(1, 0))
    panel.mark_row(-1)
    chosen = []
    panel.rowChosen.connect(chosen.append)
    qtbot.keyClick(panel.list_view, Qt.Key_Return)
    assert chosen == []


def test_esc_closes_the_drawer_from_the_page_without_cancelling_there(qtbot, window, monkeypatch):
    first = window.ids_for_test[0]
    _open_in_measure(qtbot, window, first)
    cancelled = []
    window.cancel_measurement_shortcut.activated.connect(lambda: cancelled.append(True))
    _open_drawer(qtbot, window)
    page_control = window.measure_category_combo
    page_control.setFocus()
    qtbot.wait(20)
    qtbot.keyClick(page_control, Qt.Key_Escape)
    qtbot.waitUntil(lambda: not window.observation_drawer.is_open(), timeout=2000)
    assert cancelled == []
    qtbot.keyClick(page_control, Qt.Key_Escape)  # closed now: Esc is Mål's again
    assert cancelled == [True]


def test_drawers_open_one_at_a_time(qtbot, window):
    window.tab_widget.setCurrentWidget(window.measure_tab)
    window.measure_drawer.open_page("view")
    _open_drawer(qtbot, window)
    assert not window.measure_drawer.is_open()
    window.measure_drawer.open_page("view")
    assert not window.observation_drawer.is_open()


def test_the_full_image_waits_until_view_is_shown(qtbot, window, monkeypatch):
    tab = window.observations_tab
    tab._apply_view_mode(tab.VIEW_MODE_IMAGES, persist=False)
    first, other = window.ids_for_test[0], window.ids_for_test[2]
    _open_in_measure(qtbot, window, first)
    refreshed = []
    original = tab._refresh_image_browser_for_current_selection
    monkeypatch.setattr(tab, "_refresh_image_browser_for_current_selection", lambda: refreshed.append(True) or original())
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: tab.selected_observation_ids() == [other], timeout=2000)
    qtbot.wait(400)  # the gallery load timer
    assert refreshed == []
    window.tab_widget.setCurrentWidget(window.observations_tab)
    qtbot.waitUntil(lambda: refreshed == [True], timeout=2000)
