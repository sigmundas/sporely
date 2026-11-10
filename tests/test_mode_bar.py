"""The mode bar above the main window's pages (redesign stage 9a).

Real ``MainWindow`` in an isolated profile under ``tmp_path``; no network.
The bar drives ``tab_widget`` (its tab bar hidden), keeps today's ⌥ keys,
opens today's editor from Edit, and shows Camera import's waiting count.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from database.models import ImageDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def window(qtbot, profile):
    from ui.main_window import MainWindow

    observation_id = create_observation(profile)
    ImageDB.add_image(
        observation_id, str(profile.jpeg("slide.JPG", seed=3)), "microscope",
        scale=0.05, objective_name="63X", contrast="DIC",
    )
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    tab = window.observations_tab
    qtbot.waitUntil(lambda: tab._find_table_row_for_observation(observation_id) >= 0, timeout=5000)
    window.observation_id_for_test = observation_id
    return window


def _select_row(qtbot, window):
    tab = window.observations_tab
    tab.table.selectRow(tab._find_table_row_for_observation(window.observation_id_for_test))
    qtbot.wait(50)


def test_the_tab_bar_is_replaced_by_the_mode_bar(window):
    bar = window.mode_bar
    assert not window.tab_widget.tabBar().isVisible()
    assert bar.isVisible()
    assert window._settings_corner_btn.parent() is not None
    assert bar.isAncestorOf(window._settings_corner_btn)
    assert bar.view_button.isChecked()


@pytest.mark.parametrize(
    ("button", "page"),
    [
        ("measure_button", "measure_tab"),
        ("analysis_button", "analysis_tab"),
        ("images_button", "live_lab_tab"),
        ("camera_import_button", "ingestion_hub_tab"),
        ("view_button", "observations_tab"),
    ],
)
def test_clicking_a_mode_shows_its_page(qtbot, window, button, page):
    _select_row(qtbot, window)
    if page == "observations_tab":
        qtbot.mouseClick(window.mode_bar.measure_button, Qt.LeftButton)
    qtbot.mouseClick(getattr(window.mode_bar, button), Qt.LeftButton)
    assert window.tab_widget.currentWidget() is getattr(window, page)
    assert getattr(window.mode_bar, button).isChecked()
    checked = [b for b in window.mode_bar._group.buttons() if b.isChecked()]
    assert checked == [getattr(window.mode_bar, button)]


def test_a_page_change_from_elsewhere_moves_the_check(window):
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    assert window.mode_bar.analysis_button.isChecked()
    window.tab_widget.setCurrentWidget(window.observations_tab)
    assert window.mode_bar.view_button.isChecked()


@pytest.mark.parametrize(
    ("start", "key", "page"),
    [
        ("observations_tab", Qt.Key_M, "measure_tab"),
        ("observations_tab", Qt.Key_A, "analysis_tab"),
        ("observations_tab", Qt.Key_L, "live_lab_tab"),
        ("observations_tab", Qt.Key_I, "ingestion_hub_tab"),
        ("observations_tab", Qt.Key_C, "ingestion_hub_tab"),
        ("measure_tab", Qt.Key_O, "observations_tab"),
        ("analysis_tab", Qt.Key_M, "measure_tab"),
        ("live_lab_tab", Qt.Key_A, "analysis_tab"),
    ],
)
def test_the_alt_keys_still_switch_and_check_the_mode(qtbot, window, start, key, page):
    _select_row(qtbot, window)
    window.tab_widget.setCurrentWidget(getattr(window, start))
    window.activateWindow()
    target = getattr(window, start)
    target.setFocus()
    qtbot.wait(50)
    focus = QApplication.focusWidget() or target
    qtbot.keyClick(focus, key, Qt.AltModifier)
    qtbot.waitUntil(lambda: window.tab_widget.currentWidget() is getattr(window, page), timeout=2000)
    checked = [b for b in window.mode_bar._group.buttons() if b.isChecked()]
    assert len(checked) == 1
    assert window.mode_bar._page_by_button[checked[0]] == window.tab_widget.currentIndex()


def test_edit_needs_one_observation_and_opens_todays_editor(qtbot, window, monkeypatch):
    bar = window.mode_bar
    window.observations_tab.table.clearSelection()
    qtbot.wait(50)
    assert not bar.edit_button.isEnabled()
    _select_row(qtbot, window)
    assert bar.edit_button.isEnabled()
    opened = []
    monkeypatch.setattr(window.observations_tab, "edit_observation", lambda: opened.append(True))
    qtbot.mouseClick(bar.measure_button, Qt.LeftButton)
    qtbot.mouseClick(bar.edit_button, Qt.LeftButton)
    assert opened == [True]
    assert not bar.edit_button.isChecked()  # Edit is not a mode until stage 9f
    assert bar.measure_button.isChecked()
    assert window.tab_widget.currentWidget() is window.measure_tab


def _match(path, observation_id, imported=False):
    return {"filepath": path, "observation_id": observation_id, "already_imported": imported}


def test_the_waiting_count_follows_camera_import(qtbot, window):
    hub = window.ingestion_hub_tab
    badge = window.mode_bar.waiting_badge
    assert not badge.isVisible()
    hub._match_result = {
        "matches": [_match("/a.jpg", 1), _match("/b.jpg", 1), _match("/c.jpg", 2), _match("/d.jpg", 2, imported=True)],
        "unmatched": [],
    }
    hub._update_action_state()
    assert badge.isVisible()
    assert badge.text() == "3"

    hub._on_staging_gallery_delete_selection_requested(["/b.jpg"])  # excluded from this import
    assert badge.text() == "2"

    hub._match_result = {"matches": [], "unmatched": []}
    hub._update_action_state()
    assert not badge.isVisible()


def test_edit_on_another_page_edits_the_observation_shown(qtbot, window, profile, monkeypatch):
    """Calibration's “Go to observation” loads B in Mål while the list still selects A."""
    tab = window.observations_tab
    shown = create_observation(profile, "second")
    tab.refresh_observations()
    qtbot.waitUntil(lambda: tab._find_table_row_for_observation(shown) >= 0, timeout=5000)
    _select_row(qtbot, window)  # A
    window.tab_widget.setCurrentWidget(window.measure_tab)
    window.on_observation_selected(shown, "Second", switch_tab=False)
    qtbot.waitUntil(window.mode_bar.edit_button.isEnabled, timeout=2000)

    edited = []
    monkeypatch.setattr(tab, "edit_observation", lambda: edited.append(tab.selected_observation_ids()))
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    assert edited == [[shown]]
    assert window.tab_widget.currentWidget() is window.measure_tab


def test_edit_turns_off_when_the_list_filter_hides_the_selection(qtbot, window):
    _select_row(qtbot, window)
    assert window.mode_bar.edit_button.isEnabled()
    window.observations_tab.search_input.setText("no observation has this name")
    qtbot.waitUntil(lambda: not window.mode_bar.edit_button.isEnabled(), timeout=3000)


def test_a_running_live_session_shows_its_dot_on_images(window):
    images = window.mode_bar.images_button
    assert images.icon().isNull()
    window.live_lab_tab._update_tab_recording_indicator(True)
    assert not images.icon().isNull()
    window.live_lab_tab._update_tab_recording_indicator(False)
    assert images.icon().isNull()


def test_the_activity_badge_shortens_camera_import_when_it_takes_the_room(qtbot, window):
    bar = window.mode_bar
    window._background_activity_timer.stop()
    activity = window._background_activity_badge
    activity.setVisible(False)
    qtbot.wait(20)
    window.resize(window.width() - bar.width() + bar.sizeHint().width() + 20, 900)
    qtbot.waitUntil(lambda: not bar.camera_import_text_is_short(), timeout=2000)

    activity.setText("Synchronising with Sporely Cloud")
    activity.setVisible(True)
    qtbot.waitUntil(bar.camera_import_text_is_short, timeout=2000)
    activity.setVisible(False)
    qtbot.waitUntil(lambda: not bar.camera_import_text_is_short(), timeout=2000)


def test_camera_import_shortens_its_label_instead_of_the_modes(qtbot, window):
    bar = window.mode_bar
    assert not bar.camera_import_text_is_short()
    minimum = window.minimumSizeHint().width()

    window.resize(minimum, 900)
    qtbot.waitUntil(bar.camera_import_text_is_short, timeout=2000)
    assert window.minimumSizeHint().width() == minimum  # no feedback into the window's size
    for button in (bar.view_button, bar.edit_button, bar.images_button, bar.measure_button, bar.analysis_button):
        assert button.width() >= button.sizeHint().width()  # mode names are never elided

    window.resize(1400, 900)
    qtbot.waitUntil(lambda: not bar.camera_import_text_is_short(), timeout=2000)
