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


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Open question (P1): filling the objective list selects the first "
        "objective and saves it over the stored choice before reading it, so "
        "only an objective that sorts first survives a restart."
    ),
)
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
