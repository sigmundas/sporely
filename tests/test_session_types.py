"""Session types in Images: Kamera tilkoblet and Kun logg (redesign stage 9e).

Real ``LiveLabTab`` (and a real ``MainWindow`` for the mode bar) in an
isolated profile under ``tmp_path``; the harness' folder watcher and fake
rawpy, no camera or network. The type is chosen in the observation card
(remembered, default in Preferences › Image import); Kamera tilkoblet shows
its folder there; a running session shows its time and “End session”; Kun
logg replaces the viewer with a page that puts the settings first, with the
camera-time QR while the session runs; the mode bar shows the session.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import re

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFileDialog

from database.models import SettingsDB
from tests.test_live_lab_behaviour import _OPEN_TABS, _choose, _make_tab, _start_session, _stop_session, profile  # noqa: F401
from tools.review_ui.live_lab_harness import create_observation

CLOCK = re.compile(r"\d\d:\d\d:\d\d$")


def test_the_card_chooses_the_type_and_remembers_it(qtbot, profile):
    from ui.live_lab_tab import LiveLabTab

    tab = _make_tab(qtbot)
    tab.set_target_observation(create_observation(profile))
    assert tab.session_type_selector.selected_value() == tab.SESSION_MODE_LIVE
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_OFFLINE)
    assert tab._selected_session_mode() == tab.SESSION_MODE_OFFLINE
    assert SettingsDB.get_setting(LiveLabTab.SETTING_SESSION_MODE) == tab.SESSION_MODE_OFFLINE
    assert not tab.session_folder_row.isVisible()  # Kun logg needs no folder

    again = _make_tab(qtbot)  # a restart
    assert again.session_type_selector.selected_value() == tab.SESSION_MODE_OFFLINE


def test_camera_connected_shows_its_folder_and_changes_it_in_place(qtbot, profile, monkeypatch):
    tab = _make_tab(qtbot)
    tab.set_target_observation(create_observation(profile))
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_LIVE)
    tab.watch_dir_input.setText("")
    assert tab.session_folder_row.isVisible()
    assert "Choose the folder" in tab.session_folder_label.text()
    assert not tab.start_stop_btn.isEnabled()

    monkeypatch.setattr(QFileDialog, "getExistingDirectory", lambda *args, **kwargs: str(profile.watch_dir))
    qtbot.mouseClick(tab.session_folder_change_btn, Qt.LeftButton)
    assert tab.watch_dir_input.text() == str(profile.watch_dir)
    assert tab.session_folder_label.text().startswith("Folder:")
    assert tab.start_stop_btn.isEnabled()


def test_a_running_session_shows_its_time_and_end_session(qtbot, profile):
    tab = _make_tab(qtbot)
    obs = create_observation(profile)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    assert not tab.session_type_selector.isVisible()
    assert not tab.session_folder_row.isVisible()
    assert tab.session_clock_label.isVisible()
    assert tab.session_clock_label.text().startswith("Live · ")
    assert CLOCK.search(tab.session_clock_label.text())
    assert tab.start_stop_btn.text() == "End session"
    assert tab.session_badge_text().startswith("Live ")
    _stop_session(qtbot, tab)
    assert tab.session_type_selector.isVisible()
    assert not tab.session_clock_label.isVisible()
    assert tab.session_badge_text() == ""


def test_log_only_puts_the_settings_first_and_shows_the_camera_time_code(qtbot, profile):
    tab = _make_tab(qtbot)
    obs = create_observation(profile)
    tab.set_target_observation(obs)
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_OFFLINE)
    assert tab.viewer_stack.currentWidget() is tab.log_page
    assert tab.log_bar_slot.isAncestorOf(tab.capture_state_bar)  # objective and contrast, on the page
    assert not tab.log_time_offset_card.isVisible()  # the code comes with the session
    assert "Start the session" in tab.log_applies_label.text()

    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    assert tab.log_time_offset_card.isVisible()
    qtbot.waitUntil(tab.log_qr_view.has_code, timeout=3000)
    assert tab.session_clock_label.text().startswith("Logging · ")
    assert tab.log_applies_label.text().startswith("Applies to photos taken from ")
    _choose(qtbot, tab.contrast_combo, "Phase")
    assert "Phase" in tab.log_settings_summary.text()
    _stop_session(qtbot, tab)

    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_LIVE)
    assert tab.viewer_stack.currentWidget() is tab.viewer_panel
    assert tab.capture_state_bar.parentWidget() is tab.live_image_label  # back on the image


def test_the_mode_bar_shows_a_running_session(qtbot, profile):
    from ui.main_window import MainWindow

    obs = create_observation(profile)
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(obs, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    live = window.live_lab_tab
    _OPEN_TABS.append(live)
    _choose(qtbot, live.session_type_selector, live.SESSION_MODE_OFFLINE)
    qtbot.mouseClick(live.start_stop_btn, Qt.LeftButton)
    assert live.is_session_running()
    qtbot.waitUntil(lambda: window.mode_bar.page_badge(3).startswith("Logging "), timeout=2000)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    assert window.mode_bar.page_badge(3).startswith("Logging ")  # seen from every page
    _stop_session(qtbot, live)
    qtbot.waitUntil(lambda: window.mode_bar.page_badge(3) == "", timeout=2000)


def test_a_photo_chosen_in_log_mode_shows_and_the_page_comes_back(qtbot, profile):
    from database.models import ImageDB

    tab = _make_tab(qtbot)
    obs = create_observation(profile)
    image_id = ImageDB.add_image(obs, str(profile.jpeg("saved.JPG", seed=4)), "microscope", objective_name="63X", contrast="DIC")
    tab.set_target_observation(obs)
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_OFFLINE)
    assert tab.viewer_stack.currentWidget() is tab.log_page
    tab._show_session_image(image_id)
    assert tab.viewer_stack.currentWidget() is tab.viewer_panel  # not hidden behind the page
    assert tab.capture_state_bar.parentWidget() is tab.live_image_label
    assert tab.back_to_log_btn.isVisible()
    qtbot.mouseClick(tab.back_to_log_btn, Qt.LeftButton)
    assert tab.viewer_stack.currentWidget() is tab.log_page
    assert tab.log_bar_slot.isAncestorOf(tab.capture_state_bar)
    assert not tab.back_to_log_btn.isVisible()


def test_the_page_bar_fits_a_narrow_window(qtbot, profile):
    tab = _make_tab(qtbot, width=1500)
    tab.set_target_observation(create_observation(profile))
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_OFFLINE)
    wide = tab.capture_state_bar.width()
    tab.resize(1000, 900)
    qtbot.wait(100)
    slot = tab.log_bar_slot
    assert tab.capture_state_bar.width() <= slot.width()
    assert tab.capture_state_bar.width() < wide or wide <= slot.width()


def test_the_summary_follows_settings_applied_from_a_photo(qtbot, profile):
    tab = _make_tab(qtbot)
    tab.set_target_observation(create_observation(profile))
    _choose(qtbot, tab.session_type_selector, tab.SESSION_MODE_OFFLINE)
    _choose(qtbot, tab.contrast_combo, "BF")
    assert "BF" in tab.log_settings_summary.text()
    tab._apply_microscope_state_to_controls({"objective_name": "63X", "contrast": "DIC"})
    assert "DIC" in tab.log_settings_summary.text()
