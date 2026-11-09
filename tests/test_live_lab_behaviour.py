"""Behaviour of today's Live Lab, pinned before the redesign pilot (plan stage P1).

These tests drive the real ``LiveLabTab`` with qtbot. Only the hardware-facing
boundaries are replaced (see ``tools/review_ui/live_lab_harness.py``): the
camera folder watcher becomes a drop folder that reports files the test copies
into it, and ``rawpy`` decodes any RAW file to a synthetic frame. Database,
settings, objectives, thumbnails and temp folders live under ``tmp_path``.

They pin the state the pilot stages (1–5) will move or restyle: lab-state
choices and what is stored on a newly captured image, session start and stop
in both session modes, the session log, RAW controls applied to a pending
capture, and splitter/section state across a restart.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, QPoint, QPointF, Qt
from PySide6.QtGui import QMouseEvent
from PySide6.QtWidgets import QApplication, QSplitter, QWidget

from database.models import ImageDB, SessionLogDB, SettingsDB
from tools.review_ui.live_lab_harness import (
    FakeRawpy,
    FolderDropWatcher,
    create_observation,
    drain_live_lab,
    isolation_patches,
    prepare_profile,
)


_OPEN_TABS: list = []


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)
    # Runs before qtbot closes the widgets and before the patches are undone:
    # a failed test must not leave a RAW save thread running into the next one.
    while _OPEN_TABS:
        tab = _OPEN_TABS.pop()
        try:
            drain_live_lab(tab, lambda predicate: qtbot.waitUntil(predicate, timeout=10000))
        except RuntimeError:  # the tab was already deleted
            pass


def _make_tab(qtbot, *, width: int = 1500, height: int = 950):
    from ui.live_lab_tab import LiveLabTab

    main_window = QWidget()
    main_window.raw_capture_service = None
    qtbot.addWidget(main_window)
    tab = LiveLabTab(main_window=main_window)
    qtbot.addWidget(tab)
    _OPEN_TABS.append(tab)
    # A JPEG waits for a possible RAW companion before import; the hold
    # length is not what these tests are about.
    tab._raw_companion_hold_ms = 20
    tab.resize(width, height)
    tab.show()
    qtbot.waitExposed(tab)
    return tab


def _choose(qtbot, selector, value) -> None:
    """Pick ``value`` the way a user does: click its pill, or use the combo.

    Session mode is a hidden runtime binding (its visible control is in
    Preferences), so it is set through the selector with signals.
    """
    is_pill = getattr(selector, "is_pill_mode", lambda: True)()
    button = selector.button_for_value(value) if is_pill else None
    if button is not None and button.isVisible():
        qtbot.mouseClick(button, Qt.LeftButton)
    elif hasattr(selector, "combo"):
        combo = selector.combo()
        combo.setCurrentIndex(combo.findData(value))
    else:
        selector.set_selected_value(value, emit=True)
    assert selector.selected_value() == value


def _start_session(qtbot, tab, observation_id: int, *, mode: str, watch_dir=None) -> None:
    tab.set_target_observation(observation_id)
    if watch_dir is not None:
        tab.watch_dir_input.setText(str(watch_dir))
    _choose(qtbot, tab.session_mode_selector, mode)
    assert tab.start_stop_btn.isEnabled()
    qtbot.mouseClick(tab.start_stop_btn, Qt.LeftButton)
    assert tab.is_session_running()


def _stop_session(qtbot, tab) -> None:
    qtbot.mouseClick(tab.start_stop_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: not tab.is_session_running(), timeout=5000)


def _events(observation_id: int) -> list[dict]:
    return SessionLogDB.get_events(observation_id=observation_id)


def _images(observation_id: int) -> list[dict]:
    return ImageDB.get_images_for_observation(observation_id)


def _lab_metadata(image: dict) -> dict:
    raw = image.get("lab_metadata") or image.get("lab_metadata_json") or {}
    return json.loads(raw) if isinstance(raw, str) else dict(raw)


LAB_STATE = {
    "objective_combo": "100X",
    "contrast_combo": "DIC",
    "mount_combo": "KOH",
    "stain_combo": "Melzer",
    "sample_combo": "Dried",
    "sample_source_combo": "Hymenium",
}


def _set_lab_state(qtbot, tab, state: dict[str, str]) -> None:
    for attribute, value in state.items():
        _choose(qtbot, getattr(tab, attribute), value)


def _watcher() -> FolderDropWatcher:
    assert FolderDropWatcher.created, "no watcher was started"
    return FolderDropWatcher.created[-1]


def _capture(qtbot, profile, observation_id: int, name: str, *, seed: int = 7) -> dict:
    before = len(_images(observation_id))
    _watcher().drop(profile.jpeg(name, seed=seed))
    qtbot.waitUntil(lambda: len(_images(observation_id)) == before + 1, timeout=5000)
    return _images(observation_id)[-1]


# ---------------------------------------------------------------------------
# Lab state: objective, contrast, mount, stain, sample type and source
# ---------------------------------------------------------------------------


def test_prep_choices_survive_a_restart(qtbot, profile):
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    tab.close()
    tab.deleteLater()

    restarted = _make_tab(qtbot)
    for attribute, value in LAB_STATE.items():
        if attribute == "objective_combo":
            continue
        assert getattr(restarted, attribute).selected_value() == value, attribute


def test_objective_choice_is_saved(qtbot, profile):
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.objective_combo, "100X")
    assert SettingsDB.get_setting(tab.SETTING_LAST_OBJECTIVE) == "100X"


def test_objective_choice_survives_a_restart(qtbot, profile):
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.objective_combo, "100X")
    tab.close()
    tab.deleteLater()

    restarted = _make_tab(qtbot)
    assert restarted.objective_combo.selected_value() == "100X"


def test_new_capture_stores_the_lab_state_chosen_before_it(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)

    expected_capture_state = {
        "objective_name": "100X",
        "contrast": "DIC",
        "mount_medium": "KOH",
        "stain": "Melzer",
        "sample_type": "Dried",
        "sample_source": "Hymenium",
    }
    capture_state = tab._capture_time_metadata()
    for key, value in expected_capture_state.items():
        assert capture_state[key] == value, key

    first = _capture(qtbot, profile, obs, "first.jpg")
    for key, value in expected_capture_state.items():
        assert first[key] == value, key
    assert first["image_type"] == "microscope"
    assert first["scale_microns_per_pixel"] == pytest.approx(0.0315)
    assert _lab_metadata(first)["session_kind"] == "live"

    # After a capture the sidebar prepares the next one: changing contrast
    # does not edit the image already taken.
    _choose(qtbot, tab.contrast_combo, "Phase")
    assert ImageDB.get_image(first["id"])["contrast"] == "DIC"
    assert tab._capture_time_metadata()["contrast"] == "Phase"

    second = _capture(qtbot, profile, obs, "second.jpg", seed=11)
    assert second["contrast"] == "Phase"
    assert second["objective_name"] == "100X"
    _stop_session(qtbot, tab)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Open question (P1): the microscope-state snapshot is taken before "
        "start_session assigns the session id, and later captures inherit it, "
        "so Live Lab images carry no session_id in lab_metadata."
    ),
)
def test_captured_image_records_its_session_id(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    image = _capture(qtbot, profile, obs, "with-session.jpg")
    assert _lab_metadata(image).get("session_id") == tab._session_id


def test_capture_with_an_uncalibrated_objective_has_no_scale(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.objective_combo, "63X")
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)

    image = _capture(qtbot, profile, obs, "uncalibrated.jpg")

    assert image["objective_name"] == "63X"
    assert not image["scale_microns_per_pixel"]
    assert image["calibration_id"] is None
    _stop_session(qtbot, tab)


# ---------------------------------------------------------------------------
# Sessions: start and stop in both modes, and the session log
# ---------------------------------------------------------------------------


def test_live_session_needs_an_observation_and_an_existing_folder(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.session_mode_selector, tab.SESSION_MODE_LIVE)

    tab.watch_dir_input.setText(str(profile.watch_dir))
    assert not tab.start_stop_btn.isEnabled(), "no observation chosen"

    tab.set_target_observation(obs)
    tab.watch_dir_input.setText(str(profile.root / "missing-folder"))
    assert not tab.start_stop_btn.isEnabled(), "watched folder does not exist"

    tab.watch_dir_input.setText(str(profile.watch_dir))
    assert tab.start_stop_btn.isEnabled()
    assert tab.start_stop_btn.text() == "Start Session"


def test_live_session_start_capture_and_stop_are_logged(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)

    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    session_id = tab._session_id
    assert tab.start_stop_btn.text() == "Stop Session"
    assert _watcher().isRunning()
    assert _watcher().directory_to_watch == str(profile.watch_dir)

    image = _capture(qtbot, profile, obs, "logged.jpg")
    _stop_session(qtbot, tab)

    assert not _watcher().isRunning()
    assert tab.start_stop_btn.text() == "Start Session"
    events = _events(obs)
    assert {event["session_id"] for event in events} == {session_id}
    assert {event["session_kind"] for event in events} == {"live"}
    assert [(e["event_type"], e["attribute_name"], e["value"]) for e in events] == [
        ("session_started", None, "live"),
        # The initial state logs five fields; sample source is not among them.
        ("dropdown_change", "objective_name", "100X"),
        ("dropdown_change", "contrast", "DIC"),
        ("dropdown_change", "mount_medium", "KOH"),
        ("dropdown_change", "stain", "Melzer"),
        ("dropdown_change", "sample_type", "Dried"),
        ("image_imported", None, "logged.jpg"),
        ("session_stopped", None, "live"),
    ]
    started, *_rest, imported, stopped = events
    assert started["metadata_json"]["watch_dir"] == str(profile.watch_dir)
    assert started["metadata_json"]["lab_metadata"]["contrast"] == "DIC"
    assert imported["metadata_json"]["image_id"] == image["id"]
    assert stopped["metadata_json"]["import_count"] == 1

    # A notification that arrives after the session stopped is ignored.
    _watcher().deliver_late(profile.jpeg("late.jpg", seed=3))
    qtbot.wait(100)
    assert len(_images(obs)) == 1
    assert len(_events(obs)) == len(events)


def test_offline_session_needs_no_folder_and_logs_offline(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    tab.watch_dir_input.setText("")
    tab.set_target_observation(obs)
    _choose(qtbot, tab.session_mode_selector, tab.SESSION_MODE_OFFLINE)

    assert SettingsDB.get_setting(tab.SETTING_SESSION_MODE) == "offline"
    assert tab.start_stop_btn.isEnabled()
    assert tab.start_stop_btn.text() == "Start Log Session"

    qtbot.mouseClick(tab.start_stop_btn, Qt.LeftButton)
    assert tab.is_session_running()
    assert FolderDropWatcher.created == [], "offline sessions start no watcher"
    _choose(qtbot, tab.stain_combo, "Congo_Red")
    _stop_session(qtbot, tab)

    events = _events(obs)
    assert {event["session_kind"] for event in events} == {"offline"}
    types = [event["event_type"] for event in events]
    assert types[0] == "session_started" and types[-1] == "session_stopped"
    assert events[0]["value"] == "offline"
    assert events[0]["metadata_json"]["watch_dir"] is None
    assert events[-1]["metadata_json"]["import_count"] == 0
    stain_changes = [e for e in events if e["attribute_name"] == "stain"]
    assert stain_changes[-1]["value"] == "Congo_Red"


def test_session_mode_survives_a_restart(qtbot, profile):
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.session_mode_selector, tab.SESSION_MODE_OFFLINE)
    tab.close()
    tab.deleteLater()

    restarted = _make_tab(qtbot)
    assert restarted.session_mode_selector.selected_value() == restarted.SESSION_MODE_OFFLINE


def test_lab_state_changes_are_logged_only_while_a_session_runs(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    tab.set_target_observation(obs)
    _choose(qtbot, tab.contrast_combo, "BF")
    assert _events(obs) == []

    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    count_after_start = len(_events(obs))
    _choose(qtbot, tab.stain_combo, "Melzer")
    _choose(qtbot, tab.sample_source_combo, "Spore_print")
    changes = _events(obs)[count_after_start:]
    assert [(e["event_type"], e["attribute_name"], e["value"]) for e in changes] == [
        ("dropdown_change", "stain", "Melzer"),
        ("dropdown_change", "sample_source", "Spore_print"),
    ]
    assert changes[0]["metadata_json"]["display_value"] == "Melzer"
    assert changes[0]["metadata_json"]["lab_metadata"]["stain"] == "Melzer"

    _stop_session(qtbot, tab)
    count_after_stop = len(_events(obs))
    _choose(qtbot, tab.mount_combo, "Water")
    assert len(_events(obs)) == count_after_stop


def test_manual_note_is_logged_and_cleared(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    assert not tab.session_note_input.isEnabled()
    assert not tab.add_note_btn.isEnabled()

    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    assert tab.session_note_input.isEnabled()
    count_before = len(_events(obs))

    qtbot.mouseClick(tab.add_note_btn, Qt.LeftButton)
    assert len(_events(obs)) == count_before, "an empty note is not logged"

    qtbot.keyClicks(tab.session_note_input, "Sporer amyloide i Melzer")
    qtbot.mouseClick(tab.add_note_btn, Qt.LeftButton)

    note = _events(obs)[-1]
    assert note["event_type"] == "manual_note"
    assert note["value"] == "Sporer amyloide i Melzer"
    assert note["metadata_json"] == {"note_length": len("Sporer amyloide i Melzer")}
    assert tab.session_note_input.text() == ""
    _stop_session(qtbot, tab)
    assert not tab.add_note_btn.isEnabled()


# ---------------------------------------------------------------------------
# RAW: review mode, controls applied to the pending capture, save
# ---------------------------------------------------------------------------


def test_raw_controls_apply_to_the_pending_capture_and_its_saved_image(qtbot, profile):
    from ui.live_lab_tab import LiveLabTab

    SettingsDB.set_setting(LiveLabTab.SETTING_RAW_CAPTURE_MODE, LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.contrast_combo, "Phase")
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)

    _watcher().drop(profile.raw("P1010001.ORF"))
    qtbot.waitUntil(lambda: len(tab._pending_raw_captures) == 1, timeout=5000)
    pending = tab._pending_raw_captures[0]
    assert _images(obs) == [], "review mode keeps the RAW pending until saved"
    assert pending.lab_metadata["contrast"] == "Phase"
    qtbot.waitUntil(lambda: bool(tab.session_gallery._items), timeout=5000)
    assert tab.session_gallery._items[0]["badges"][0] == "UNSAVED RAW"
    # The Save/Copy bar appears once the background preview is rendered.
    qtbot.waitUntil(tab.pending_raw_frame.isVisible, timeout=5000)
    # Review mode opens the RAW section by itself.
    assert tab.raw_processing_toggle_btn.isChecked()
    assert tab.raw_processing_body.isVisible()
    _choose(qtbot, tab.raw_controls.white_balance_selector, "auto")
    assert pending.raw_settings.white_balance_mode == "auto"

    qtbot.mouseClick(tab.pending_raw_save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(_images(obs)) == 1, timeout=10000)
    qtbot.waitUntil(lambda: not tab._pending_raw_captures, timeout=5000)

    image = _images(obs)[0]
    assert image["contrast"] == "Phase"
    raw_processing = _lab_metadata(image)["raw_processing"]
    assert raw_processing["settings"]["white_balance_mode"] == "auto"
    assert raw_processing["source"]["path"].endswith("P1010001.ORF")
    full_renders = [kw for _path, kw in profile.rawpy.postprocess_calls if not kw.get("half_size")]
    assert full_renders and full_renders[-1]["use_auto_wb"] is True
    _stop_session(qtbot, tab)


# ---------------------------------------------------------------------------
# Splitters and sections across a restart
# ---------------------------------------------------------------------------


def _mouse(widget, event_type, pos: QPoint, buttons) -> None:
    button = Qt.NoButton if event_type == QEvent.MouseMove else Qt.LeftButton
    event = QMouseEvent(
        event_type, QPointF(pos), QPointF(widget.mapToGlobal(pos)), button, buttons, Qt.NoModifier
    )
    QApplication.sendEvent(widget, event)


def _drag_handle(qtbot, splitter: QSplitter, delta: int) -> None:
    """Drag the handle like a user. QTest's mouseMove carries no pressed
    button offscreen, so the press/move/release events are sent directly."""
    handle = splitter.handle(1)
    start = handle.rect().center()
    if splitter.orientation() == Qt.Horizontal:
        end = start + QPoint(delta, 0)
    else:
        end = start + QPoint(0, delta)
    _mouse(handle, QEvent.MouseButtonPress, start, Qt.LeftButton)
    _mouse(handle, QEvent.MouseMove, end, Qt.LeftButton)
    _mouse(handle, QEvent.MouseButtonRelease, end, Qt.NoButton)
    qtbot.wait(10)


def _splitters(tab) -> dict[str, QSplitter]:
    content = tab.findChild(QSplitter, "gallerySplitter")
    main = next(s for s in tab.findChildren(QSplitter) if s.orientation() == Qt.Horizontal)
    return {"main": main, "content": content}


def test_splitter_sizes_survive_a_restart(qtbot, profile):
    from ui.live_lab_tab import LiveLabTab

    tab = _make_tab(qtbot)
    splitters = _splitters(tab)
    before = {name: s.sizes() for name, s in splitters.items()}
    _drag_handle(qtbot, splitters["main"], 80)
    _drag_handle(qtbot, splitters["content"], -60)
    after = {name: s.sizes() for name, s in splitters.items()}
    assert after["main"][0] > before["main"][0]
    assert after["content"][0] < before["content"][0]
    assert json.loads(SettingsDB.get_setting(LiveLabTab.SETTING_MAIN_SPLITTER)) == after["main"]
    assert json.loads(SettingsDB.get_setting(LiveLabTab.SETTING_CONTENT_SPLITTER)) == after["content"]
    tab.close()
    tab.deleteLater()

    restarted = _splitters(_make_tab(qtbot))
    assert restarted["main"].sizes()[0] == after["main"][0]
    assert restarted["content"].sizes()[0] == after["content"][0]


def test_raw_section_is_hidden_without_raw_and_not_remembered(qtbot, profile):
    """Today: the RAW card only shows for a RAW capture, and its open state is
    not remembered across a restart (pilot stage 1 adds remembered sections)."""
    tab = _make_tab(qtbot)
    assert not tab.raw_processing_card.isVisible()
    qtbot.mouseClick(tab.raw_processing_toggle_btn, Qt.LeftButton)
    assert tab.raw_processing_toggle_btn.isChecked()
    tab.close()
    tab.deleteLater()

    restarted = _make_tab(qtbot)
    assert not restarted.raw_processing_toggle_btn.isChecked()
    assert not restarted.raw_processing_card.isVisible()


# ---------------------------------------------------------------------------
# Stage 1: collapsible Preparation and Notes sections with summary lines
# ---------------------------------------------------------------------------


def _settle(qtbot, section) -> None:
    qtbot.waitUntil(lambda: section._animation.state() != section._animation.State.Running, timeout=2000)


def test_preparation_summary_follows_each_choice_immediately(qtbot, profile):
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    assert tab.slide_prep_group.summary() == "KOH · Dried · Melzer · Hymenium"
    assert tab.slide_prep_group.summary_alert() == ""
    _choose(qtbot, tab.mount_combo, "Water")
    assert tab.slide_prep_group.summary() == "Water · Dried · Melzer · Hymenium"
    _choose(qtbot, tab.sample_source_combo, "Stipe")
    assert tab.slide_prep_group.summary() == "Water · Dried · Melzer · Stipe"


def test_preparation_summary_follows_changes_made_with_signals_blocked(qtbot, profile):
    """Selecting gallery images fills the sidebar with signals blocked."""
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    blocked = tab.mount_combo.blockSignals(True)
    try:
        tab.mount_combo.setCurrentIndex(tab.mount_combo.findData("L4"))
    finally:
        tab.mount_combo.blockSignals(blocked)
    assert tab.slide_prep_group.summary().startswith("L4 · ")
    tab.stain_combo.set_mixed_values(["Melzer", "Congo_Red"])
    assert "Stain: mixed" in tab.slide_prep_group.summary()


def test_preparation_summary_names_an_unset_value_as_a_warning(qtbot, profile):
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _choose(qtbot, tab.stain_combo, "Not_set")
    assert tab.slide_prep_group.summary() == "KOH · Dried · Hymenium"
    assert tab.slide_prep_group.summary_alert() == "Stain not set"
    _choose(qtbot, tab.mount_combo, "Not_set")
    assert tab.slide_prep_group.summary() == "Dried · Hymenium"
    assert tab.slide_prep_group.summary_alert() == "2 not set"
    _choose(qtbot, tab.stain_combo, "Melzer")
    _choose(qtbot, tab.mount_combo, "KOH")
    assert tab.slide_prep_group.summary_alert() == ""


def test_sections_open_state_survives_a_restart(qtbot, profile):
    tab = _make_tab(qtbot)
    assert tab.slide_prep_group.is_expanded() and tab.notes_group.is_expanded()
    qtbot.mouseClick(tab.slide_prep_group.header, Qt.LeftButton)
    tab.notes_group.header.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(tab.notes_group.header.hasFocus, timeout=1000)
    qtbot.keyClick(tab.notes_group.header, Qt.Key_Space)
    _settle(qtbot, tab.slide_prep_group)
    _settle(qtbot, tab.notes_group)
    assert not tab.slide_prep_group.body.isVisible() and not tab.notes_group.body.isVisible()
    assert tab.mount_combo.isHidden() or not tab.mount_combo.isVisible()
    tab.close()

    restarted = _make_tab(qtbot)
    assert not restarted.slide_prep_group.is_expanded()
    assert not restarted.notes_group.is_expanded()
    assert not restarted.slide_prep_group.body.isVisible()
    qtbot.mouseClick(restarted.slide_prep_group.header, Qt.LeftButton)
    _settle(qtbot, restarted.slide_prep_group)
    assert restarted.mount_combo.isVisible()
    restarted.close()

    again = _make_tab(qtbot)
    assert again.slide_prep_group.is_expanded() and not again.notes_group.is_expanded()


def test_notes_summary_shows_session_notes_and_the_selected_images_note(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    assert tab.notes_group.summary() == "No notes"
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)

    tab.session_note_input.setText("Spores Melzer-negative")
    qtbot.keyClick(tab.session_note_input, Qt.Key_Return)
    first = tab.notes_group.summary()
    assert first.startswith("1 note · ") and first.endswith(" Spores Melzer-negative")

    tab.session_note_input.setText("Basidia 4-spored")
    qtbot.mouseClick(tab.add_note_btn, Qt.LeftButton)
    second = tab.notes_group.summary()
    assert second.startswith("2 notes · last ") and second.endswith(" Basidia 4-spored")

    image = _capture(qtbot, profile, obs, "P1010001.JPG")
    tab._on_session_gallery_clicked(image["id"], image.get("filepath") or "")
    qtbot.waitUntil(lambda: tab._selected_session_image_id == image["id"], timeout=3000)
    tab.image_note_input.setPlainText("Clamp at septum")
    assert tab.notes_group.summary().endswith(" · Image: Clamp at septum")

    _stop_session(qtbot, tab)
    # A new session starts with nothing selected: the note field may still hold
    # the last image's text, but the summary must not present it as current.
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    assert tab._selected_session_image_id is None
    assert tab.notes_group.summary() == "No notes"


def test_enter_on_a_section_header_toggles_it_and_triggers_no_review_shortcut(qtbot, profile):
    """Live Lab binds Return/Enter to "save current" in RAW review; a focused
    section header must take those keys itself."""
    tab = _make_tab(qtbot)
    calls: list[str] = []
    tab._handle_raw_review_shortcut = lambda action: calls.append(action)
    header = tab.slide_prep_group.header
    header.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(header.hasFocus, timeout=1000)
    for key in (Qt.Key_Return, Qt.Key_Enter):
        before = tab.slide_prep_group.is_expanded()
        qtbot.keyClick(header, key)
        _settle(qtbot, tab.slide_prep_group)
        assert tab.slide_prep_group.is_expanded() is not before
    assert calls == []
    # The same key elsewhere in the tab still reaches the review shortcut.
    tab.setFocus(Qt.OtherFocusReason)
    qtbot.keyClick(tab, Qt.Key_Return)
    assert calls == ["save"]


# ---------------------------------------------------------------------------
# Stage 1b: Preparation as chip rows
# ---------------------------------------------------------------------------


def _chip(selector, value):
    button = selector.button_for_value(value)
    assert button is not None and button.isVisible(), (value, selector.visible_chip_values())
    return button


def test_preparation_rows_are_chips_without_a_not_set_chip(qtbot, profile):
    tab = _make_tab(qtbot)
    for selector in (tab.mount_combo, tab.stain_combo, tab.sample_combo, tab.sample_source_combo):
        assert selector.is_chip_mode()
        assert "Not_set" not in selector.visible_chip_values()
        assert not selector.combo().isVisible()


def test_clicking_a_chip_chooses_it_and_clicking_it_again_clears_it(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)

    qtbot.mouseClick(_chip(tab.mount_combo, "Water"), Qt.LeftButton)
    assert tab.mount_combo.selected_value() == "Water"
    assert tab.slide_prep_group.summary().startswith("Water · ")

    qtbot.mouseClick(_chip(tab.mount_combo, "Water"), Qt.LeftButton)
    assert tab.mount_combo.selected_value() == "Not_set"
    assert not any(button.isChecked() for button in tab.mount_combo.buttons() if button.isVisible())
    assert tab.slide_prep_group.summary_alert() == "Mount not set"

    changes = [
        (event.get("attribute_name"), event.get("value"))
        for event in _events(obs)
        if event.get("event_type") == "dropdown_change"
    ]
    assert changes[-2:] == [("mount_medium", "Water"), ("mount_medium", "Not_set")]


def test_more_chip_shows_the_rest_in_place_and_folds_after_a_choice(qtbot, profile):
    tab = _make_tab(qtbot, width=1100, height=720)  # narrow side panel: stains do not fit
    stain = tab.stain_combo
    _choose(qtbot, stain, "Melzer")
    more = stain.more_chip()
    assert more.isVisible()
    folded = stain.visible_chip_values()
    hidden = [v for v in ("Congo_Red", "Cotton_Blue", "Lactofuchsin", "Cresyl_Blue", "Trypan_Blue", "Chlorazol_Black_E") if v not in folded]
    assert hidden and more.text() == f"+ {len(hidden)} more"
    folded_height = stain.height()

    qtbot.mouseClick(more, Qt.LeftButton)
    qtbot.waitUntil(lambda: set(hidden) <= set(stain.visible_chip_values()), timeout=2000)
    assert stain.chips_expanded() and not more.isVisible()
    qtbot.waitUntil(lambda: stain.height() > folded_height, timeout=2000)  # wrapped onto more rows

    qtbot.mouseClick(_chip(stain, hidden[-1]), Qt.LeftButton)
    assert stain.selected_value() == hidden[-1]
    qtbot.waitUntil(lambda: not stain.chips_expanded() and more.isVisible(), timeout=2000)
    assert hidden[-1] in stain.visible_chip_values()  # the choice stays visible when folded
    qtbot.waitUntil(lambda: stain.height() == folded_height, timeout=2000)


def test_a_restored_choice_from_the_folded_part_is_visible(qtbot, profile):
    tab = _make_tab(qtbot, width=1100, height=720)
    _choose(qtbot, tab.stain_combo, "Chlorazol_Black_E")
    tab.close()

    restarted = _make_tab(qtbot, width=1100, height=720)
    assert restarted.stain_combo.selected_value() == "Chlorazol_Black_E"
    assert "Chlorazol_Black_E" in restarted.stain_combo.visible_chip_values()
    assert _chip(restarted.stain_combo, "Chlorazol_Black_E").isChecked()


def test_chip_keyboard_moves_and_chooses_without_review_shortcuts(qtbot, profile):
    """Arrows move between chips, Space/Enter choose; Live Lab's RAW review
    shortcuts (Left/Right/Return) never see keys meant for a focused chip."""
    tab = _make_tab(qtbot, width=1100, height=720)
    calls: list[str] = []
    tab._handle_raw_review_shortcut = lambda action: calls.append(action)
    mount = tab.mount_combo
    _choose(qtbot, mount, "KOH")
    koh = _chip(mount, "KOH")
    koh.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(koh.hasFocus, timeout=1000)

    qtbot.keyClick(koh, Qt.Key_Right)
    next_chip = QApplication.focusWidget()
    assert next_chip is not koh and next_chip in mount.buttons()
    assert mount.selected_value() == "KOH"  # moving never chooses (or clears)
    qtbot.keyClick(next_chip, Qt.Key_Left)
    assert QApplication.focusWidget() is koh

    qtbot.keyClick(koh, Qt.Key_Right)
    target = QApplication.focusWidget()
    qtbot.keyClick(target, Qt.Key_Return)
    assert mount.selected_value() == mount.itemData(int(target.property("choiceIndex")))
    qtbot.keyClick(target, Qt.Key_Space)
    assert mount.selected_value() == "Not_set"  # Space on the chosen chip clears it

    more = tab.stain_combo.more_chip()
    more.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(more.hasFocus, timeout=1000)
    qtbot.keyClick(more, Qt.Key_Return)
    assert tab.stain_combo.chips_expanded()
    assert QApplication.focusWidget() in tab.stain_combo.buttons()
    assert calls == []


def test_chip_rows_are_quiet_when_nothing_changes(qtbot, profile):
    tab = _make_tab(qtbot)
    qtbot.wait(100)
    counts = {"layouts": 0}
    original = tab.stain_combo._layout_chips

    def counting():
        counts["layouts"] += 1
        original()

    tab.stain_combo._layout_chips = counting
    for _ in range(40):
        qtbot.wait(5)
    assert counts["layouts"] <= 2


# ---------------------------------------------------------------------------
# Stage 2: capture-state bar, live-view label and shortcuts
# ---------------------------------------------------------------------------


def _bar_button(tab, kind: str, value: str):
    segments = tab.capture_state_bar.objective_segments if kind == "objective" else tab.capture_state_bar.contrast_segments
    button = segments.button_for_value(value)
    assert button is not None and button.isVisible(), (kind, value, list(segments.buttons()))
    return button


def test_bar_buttons_choose_and_log_objective_and_contrast(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)

    qtbot.mouseClick(_bar_button(tab, "objective", "40X"), Qt.LeftButton)
    qtbot.mouseClick(_bar_button(tab, "contrast", "Phase"), Qt.LeftButton)
    assert tab.objective_combo.selected_value() == "40X"
    assert tab.contrast_combo.selected_value() == "Phase"
    assert _bar_button(tab, "objective", "40X").property("selected") is True
    assert _bar_button(tab, "objective", "100X").property("selected") is False
    assert tab.live_state_label.text() == "40x · Phase · KOH"
    changes = [
        (event.get("attribute_name"), event.get("value"))
        for event in _events(obs)
        if event.get("event_type") == "dropdown_change"
    ]
    assert changes[-2:] == [("objective_name", "40X"), ("contrast", "Phase")]
    assert SettingsDB.get_setting(tab.SETTING_LAST_OBJECTIVE) == "40X"


def test_bar_label_and_thumbnail_badge_share_the_objective_colour(qtbot, profile):
    from ui.adaptive_choice_selector import objective_color
    from database.schema import load_objectives

    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, {**LAB_STATE, "objective_combo": "63X"})
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    _capture(qtbot, profile, obs, "P1010001.JPG")
    qtbot.waitUntil(lambda: bool(tab.session_gallery._items), timeout=3000)

    ring = objective_color(load_objectives()["63X"], "63X")
    assert f"background-color: {ring};" in tab.live_state_label.styleSheet()
    assert ring in _bar_button(tab, "objective", "63X").styleSheet()
    badge_colors = {item.get("microscope_tag_color") for item in tab.session_gallery._items}
    assert badge_colors == {ring}


def test_the_microscope_card_is_gone_and_its_choices_live_on_the_bar(qtbot, profile):
    tab = _make_tab(qtbot)
    assert getattr(tab, "microscope_group", None) is None
    assert not tab.objective_combo.isVisible() and not tab.contrast_combo.isVisible()
    assert tab.capture_state_bar.isVisible()
    assert list(tab.capture_state_bar.objective_segments.buttons()) == ["40X", "63X", "100X"]


def test_number_and_letter_shortcuts_choose_objective_and_contrast(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    tab.live_image_label.setFocus(Qt.OtherFocusReason)
    qtbot.waitUntil(tab.live_image_label.hasFocus, timeout=1000)

    for key, expected in ((Qt.Key_1, "40X"), (Qt.Key_2, "63X"), (Qt.Key_3, "100X")):
        qtbot.keyClick(tab.live_image_label, key)
        assert tab.objective_combo.selected_value() == expected
    qtbot.keyClick(tab.live_image_label, Qt.Key_6)  # no sixth objective: nothing happens
    assert tab.objective_combo.selected_value() == "100X"
    for key, expected in ((Qt.Key_B, "BF"), (Qt.Key_D, "DF"), (Qt.Key_P, "Phase"), (Qt.Key_I, "DIC")):
        qtbot.keyClick(tab.live_image_label, key)
        assert tab.contrast_combo.selected_value() == expected
    assert _bar_button(tab, "objective", "63X").toolTip().endswith("(2)")
    assert _bar_button(tab, "contrast", "DIC").toolTip().endswith("(I)")
    changes = [e.get("value") for e in _events(obs) if e.get("event_type") == "dropdown_change"]
    assert changes[-7:] == ["40X", "63X", "100X", "BF", "DF", "Phase", "DIC"]


def test_shortcut_letters_type_into_text_fields(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_OFFLINE)
    tab.session_note_input.setFocus(Qt.OtherFocusReason)
    qtbot.waitUntil(tab.session_note_input.hasFocus, timeout=1000)
    qtbot.keyClicks(tab.session_note_input, "Bip 12")
    assert tab.session_note_input.text() == "Bip 12"
    assert tab.contrast_combo.selected_value() == "DIC"
    assert tab.objective_combo.selected_value() == "100X"


def test_overlays_stack_without_overlapping(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    qtbot.waitUntil(tab.live_state_label.isVisible, timeout=2000)
    session = tab.session_action_frame.geometry()
    label = tab.live_state_label.geometry()
    bar = tab.capture_state_bar.geometry()
    calibration = tab.calibration_action_frame.geometry()
    assert label.top() > session.bottom()  # label under the session button
    assert not label.intersects(calibration)
    viewer = tab.live_image_label.rect()
    assert viewer.contains(bar)
    assert abs(bar.center().x() - viewer.center().x()) <= 1  # bottom centre
    tab.pending_raw_frame.setVisible(True)
    tab._reposition_pending_raw_floating_tab()
    assert tab.pending_raw_frame.geometry().bottom() < bar.top()  # pending bar sits above it


def test_bar_stacks_its_groups_when_the_viewer_is_narrow(qtbot, profile):
    tab = _make_tab(qtbot)
    bar = tab.capture_state_bar
    tab._reposition_pending_raw_floating_tab()
    assert not bar.is_stacked()
    wide = bar.sizeHint().width()
    bar.fit_to_width(wide - 20)
    assert bar.is_stacked()
    assert bar.sizeHint().width() < wide
    bar.fit_to_width(wide + 20)
    assert not bar.is_stacked()


def test_bar_shows_an_unset_contrast_as_a_warning_and_a_mixed_selection(qtbot, profile):
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _choose(qtbot, tab.contrast_combo, "Not_set")
    segments = tab.capture_state_bar.contrast_segments
    assert "Not_set" not in segments.buttons()
    assert not any(button.property("selected") for button in segments.buttons().values())
    assert "#d64545" in segments.styleSheet() or "#d65a63" in segments.styleSheet()
    tab.objective_combo.set_mixed_values(["40X", "100X"])
    objective_buttons = tab.capture_state_bar.objective_segments.buttons()
    assert "dashed" in objective_buttons["40X"].styleSheet()
    assert "dashed" not in objective_buttons["63X"].styleSheet()


def test_bar_follows_a_changed_choice_list_and_forwards_wheel_events_from_new_buttons(qtbot, profile, monkeypatch):
    from PySide6.QtCore import QPoint, QPointF
    from PySide6.QtGui import QWheelEvent

    tab = _make_tab(qtbot)
    bar = tab.capture_state_bar
    before = bar.width()
    tab.contrast_combo.addItem("Extra wide contrast method", "Extra_wide", pillText="Extra wide contrast method")
    qtbot.waitUntil(lambda: bar.width() > before, timeout=2000)  # repositioned after the rebuild
    assert bar.geometry().width() == bar.sizeHint().width()

    anchors: list[QPointF] = []
    monkeypatch.setattr(tab.live_image_label, "_apply_zoom_around", lambda pos, factor: anchors.append(QPointF(pos)))
    from PySide6.QtGui import QPixmap

    pixmap = QPixmap(64, 48)
    pixmap.fill(Qt.white)
    tab.live_image_label.set_image(pixmap)
    button = bar.contrast_segments.button_for_value("Extra_wide")  # created after the tab was built
    local = QPoint(5, 5)
    event = QWheelEvent(
        QPointF(local), QPointF(button.mapToGlobal(local)), QPoint(0, 0), QPoint(0, 120),
        Qt.NoButton, Qt.NoModifier, Qt.NoScrollPhase, False,
    )
    QApplication.sendEvent(button, event)
    assert anchors, "the wheel event did not reach the viewer"
    expected = tab.live_image_label.mapFromGlobal(button.mapToGlobal(local))
    assert (round(anchors[0].x()), round(anchors[0].y())) == (expected.x(), expected.y())


def test_a_narrow_viewer_gets_a_stacked_compact_bar_clear_of_the_zoom_readout(qtbot, profile):
    tab = _make_tab(qtbot, width=900, height=720)
    label = tab.live_image_label
    bar = tab.capture_state_bar
    for value in ("Extra_1", "Extra_2"):
        tab.objective_combo.addItem(value, value, pillText="999x")
    label.resize(360, label.height())
    tab._reposition_pending_raw_floating_tab()
    assert bar.is_stacked()
    assert bar.geometry().right() <= label.width() or bar.is_compact()
    assert not bar.geometry().intersects(label.bottom_left_readout_rect())


# ---------------------------------------------------------------------------
# Stage 3: scale bar and calibration notice for what the viewer shows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("zoom", "expected"), [(0.25, 25.0), (0.5, 50.0), (1.0, 100.0), (2.0, 200.0), (0.05, 10.0)])
def test_scale_bar_length_follows_the_zoom(qtbot, zoom, expected):
    from ui.zoomable_image_widget import ZoomableImageLabel

    label = ZoomableImageLabel()
    qtbot.addWidget(label)
    label.set_microns_per_pixel(0.1)  # 10 µm = 100 image pixels
    label.set_scale_bar(True, 10.0)
    label.zoom_level = zoom
    assert label.scale_bar_screen_length() == pytest.approx(expected)  # never under 10 px
    assert label.scale_bar_screen_length(available_width=100) == pytest.approx(min(expected, 60.0))


def test_a_capture_keeps_its_own_label_and_scale_when_the_objective_changes(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)  # 100X, calibrated
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    image = _capture(qtbot, profile, obs, "P1010001.JPG")
    qtbot.waitUntil(lambda: tab._viewer_subject == ("image", image["id"]), timeout=3000)
    label = tab.live_image_label
    assert label.show_scale_bar and label.microns_per_pixel == pytest.approx(image["scale_microns_per_pixel"])
    assert tab.live_state_label.text().startswith("100x · DIC")

    qtbot.mouseClick(_bar_button(tab, "objective", "40X"), Qt.LeftButton)  # the next capture's objective
    assert tab.objective_combo.selected_value() == "40X"
    assert tab.live_state_label.text().startswith("100x · DIC")  # still the image on screen
    assert label.microns_per_pixel == pytest.approx(image["scale_microns_per_pixel"])
    assert not tab.calibration_notice.isVisible()


def test_an_empty_viewer_warns_at_once_when_the_chosen_objective_is_not_calibrated(qtbot, profile):
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    assert tab._viewer_subject is None
    assert not tab.calibration_notice.isVisible()
    tab.live_image_label.setFocus(Qt.OtherFocusReason)
    qtbot.waitUntil(tab.live_image_label.hasFocus, timeout=1000)
    qtbot.keyClick(tab.live_image_label, Qt.Key_2)  # 63X has no calibration
    assert tab.objective_combo.selected_value() == "63X"
    assert tab.calibration_notice.isVisible()
    assert tab.calibration_notice_label.text() == "Not calibrated · 63x"
    assert tab.live_state_label.text().startswith("63x")
    qtbot.keyClick(tab.live_image_label, Qt.Key_1)
    assert not tab.calibration_notice.isVisible()


def test_an_uncalibrated_capture_shows_the_notice_and_no_scale_bar(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, {**LAB_STATE, "objective_combo": "63X"})
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    image = _capture(qtbot, profile, obs, "P1010002.JPG")
    qtbot.waitUntil(lambda: tab._viewer_subject == ("image", image["id"]), timeout=3000)
    assert not tab.live_image_label.show_scale_bar
    assert tab.calibration_notice.isVisible()
    notice = tab.calibration_notice.geometry()
    assert not notice.intersects(tab.live_image_label.bottom_left_readout_rect())
    assert not notice.intersects(tab.capture_state_bar.geometry())


def test_calibrate_link_opens_calibration_for_that_objective(qtbot, profile):
    tab = _make_tab(qtbot)
    opened: list[dict] = []
    tab._main_window.open_calibration_dialog = lambda **kwargs: opened.append(kwargs) or False
    _choose(qtbot, tab.objective_combo, "63X")
    assert tab.calibration_notice.isVisible()
    qtbot.mouseClick(tab.calibrate_link_btn, Qt.LeftButton)
    assert opened == [{"objective_key": "63X"}]


def _show_unscaled_image(qtbot, profile, tab, obs, *, objective_name):
    """A saved image with no stored scale, shown in the viewer."""
    image = _capture(qtbot, profile, obs, f"P10{abs(hash(objective_name)) % 10000:04d}.JPG")
    ImageDB.update_image(image["id"], objective_name=objective_name, scale=0.0)
    tab._show_session_image(image["id"])
    qtbot.waitUntil(lambda: tab._viewer_subject == ("image", image["id"]), timeout=3000)
    return image


def test_calibrating_from_the_notice_keeps_the_current_selection(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, {**LAB_STATE, "objective_combo": "63X"})
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    _capture(qtbot, profile, obs, "P1010003.JPG")
    SettingsDB.set_setting(tab.SETTING_LAST_OBJECTIVE, "100X")  # a different stored choice
    opened: list[dict] = []
    tab._main_window.open_calibration_dialog = lambda **kwargs: opened.append(kwargs) or False
    assert tab.calibration_notice.isVisible()
    qtbot.mouseClick(tab.calibrate_link_btn, Qt.LeftButton)  # the dialog is "cancelled"
    assert opened == [{"objective_key": "63X"}]
    assert tab.objective_combo.selected_value() == "63X"


def test_the_notice_offers_calibrate_only_for_a_known_objective(qtbot, profile):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    _show_unscaled_image(qtbot, profile, tab, obs, objective_name="Removed 20X")
    assert tab.calibration_notice.isVisible()
    assert not tab.calibrate_link_btn.isVisible()
    # A legacy image row with neither an objective nor a scale.
    tab._viewer_subject = ("image", 999999)
    tab._update_calibration_notice({"id": 999999, "image_type": "microscope"})
    assert tab.calibration_notice.isVisible()
    assert tab.calibration_notice_label.text() == "Not calibrated"
    assert not tab.calibrate_link_btn.isVisible()


def test_control_changes_do_not_read_the_shown_image_again(qtbot, profile, monkeypatch):
    obs = create_observation(profile)
    tab = _make_tab(qtbot)
    _set_lab_state(qtbot, tab, LAB_STATE)
    _start_session(qtbot, tab, obs, mode=tab.SESSION_MODE_LIVE, watch_dir=profile.watch_dir)
    image = _capture(qtbot, profile, obs, "P1010004.JPG")
    qtbot.waitUntil(lambda: tab._viewer_subject == ("image", image["id"]), timeout=3000)
    reads: list[int] = []
    original = ImageDB.get_image
    monkeypatch.setattr(ImageDB, "get_image", staticmethod(lambda image_id: reads.append(image_id) or original(image_id)))
    for value in ("40X", "63X", "100X"):
        qtbot.mouseClick(_bar_button(tab, "objective", value), Qt.LeftButton)
    _choose(qtbot, tab.contrast_combo, "Phase")
    assert reads == []
    assert tab.live_state_label.text().startswith("100x · DIC")


def test_the_notice_clears_the_pending_bar_in_a_narrow_viewer(qtbot, profile):
    tab = _make_tab(qtbot)
    _choose(qtbot, tab.objective_combo, "63X")  # empty viewer, uncalibrated: notice shown
    label = tab.live_image_label
    label.resize(620, 400)
    tab.pending_raw_frame.setVisible(True)
    tab._reposition_pending_raw_floating_tab()
    notice = tab.calibration_notice.geometry()
    for widget in (tab.capture_state_bar, tab.pending_raw_frame):
        assert not notice.intersects(widget.geometry()), widget.objectName()


def test_the_drawn_scale_bar_uses_the_length_helper(qtbot, monkeypatch):
    from PySide6.QtGui import QPixmap
    from ui.zoomable_image_widget import ZoomableImageLabel

    label = ZoomableImageLabel()
    qtbot.addWidget(label)
    pixmap = QPixmap(400, 300)
    pixmap.fill(Qt.white)
    label.resize(500, 400)
    label.set_image(pixmap)
    label.set_microns_per_pixel(0.1)
    label.set_scale_bar(True, 10.0)
    calls: list[float] = []
    original = label.scale_bar_screen_length
    monkeypatch.setattr(label, "scale_bar_screen_length", lambda width=None: calls.append(width) or original(width))
    label.grab()
    assert calls, "painting did not ask the helper for the bar length"
