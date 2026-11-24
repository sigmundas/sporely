"""Live captures in the shared strip (redesign stage 9d).

Real ``MainWindow`` in an isolated profile under ``tmp_path``; RAW captures
arrive through the harness' folder watcher (fake rawpy), no network. Images
holds the same strip as View and Measure: unsaved captures first with an
orange border, then the observation's images; saving is a row above the
strip, not a bar on the image; Live Lab never writes to the strip, or reads
another page's selection, while that page holds it.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt

from database.models import ImageDB, ObservationDB, SettingsDB
from tools.review_ui.live_lab_harness import (
    FakeRawpy,
    FolderDropWatcher,
    create_observation,
    drain_live_lab,
    isolation_patches,
    prepare_profile,
)
from ui.styles import STAGING_BORDER

_WINDOWS: list = []


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)
    while _WINDOWS:
        window = _WINDOWS.pop()
        try:
            _save_all(qtbot, window)  # also after a failed assertion
            drain_live_lab(window.live_lab_tab, lambda predicate: qtbot.waitUntil(predicate, timeout=10000))
        except Exception:
            pass
        window.close()
        window.deleteLater()
        qtbot.wait(50)


@pytest.fixture
def window(qtbot, profile):
    from ui.live_lab_tab import LiveLabTab
    from ui.main_window import MainWindow

    SettingsDB.set_setting(LiveLabTab.SETTING_RAW_CAPTURE_MODE, LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    first = create_observation(profile)
    saved = [
        ImageDB.add_image(first, str(profile.jpeg(f"saved{n}.JPG", seed=n)), "microscope", scale=0.05, objective_name="63X")
        for n in range(3)
    ]
    other = ObservationDB.create_observation(date="2026-10-20 10:00", genus="Fomes", species="fomentarius")
    window = MainWindow()
    _WINDOWS.append(window)  # closed after Live Lab's save threads finish (see profile)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    tab = window.observations_tab
    qtbot.waitUntil(lambda: tab.table.rowCount() == 2, timeout=5000)
    tab.table.selectRow(tab._find_table_row_for_observation(first))
    tab.set_selected_as_active(switch_tab=False)
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=3000)
    window.ids_for_test = {"first": first, "saved": saved, "other": other}
    return window


def _to(qtbot, window, page):
    window.tab_widget.setCurrentWidget(getattr(window, page))
    qtbot.wait(100)


def _start_live(qtbot, window, profile):
    live = window.live_lab_tab
    _to(qtbot, window, "live_lab_tab")
    live.watch_dir_input.setText(str(profile.watch_dir))
    live.session_mode_selector.set_selected_value(live.SESSION_MODE_LIVE)
    qtbot.mouseClick(live.start_stop_btn, Qt.LeftButton)
    assert live.is_session_running()
    return live


def _capture_raw(qtbot, window, profile, name):
    live = window.live_lab_tab
    before = len(live._pending_raw_captures)
    FolderDropWatcher.created[-1].drop(profile.raw(name))
    qtbot.waitUntil(lambda: len(live._pending_raw_captures) == before + 1, timeout=5000)


def _strip(window):
    return window.measure_gallery


def _save_all(qtbot, window):
    """Leave no unsaved captures behind: a teardown with some never goes idle (also before 9d)."""
    live = window.live_lab_tab
    if not live._pending_raw_captures:
        return
    _to(qtbot, window, "live_lab_tab")
    qtbot.waitUntil(live.pending_raw_save_all_btn.isEnabled, timeout=5000)
    qtbot.mouseClick(live.pending_raw_save_all_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: not live._pending_raw_captures, timeout=10000)
    qtbot.waitUntil(lambda: not live._pending_raw_commit_is_busy(), timeout=10000)


def test_images_holds_the_shared_strip_with_saving_above_it(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    strip = _strip(window)
    assert live.strip_slot.isAncestorOf(strip)
    assert live.session_gallery is strip
    assert not live.live_image_label.isAncestorOf(live.pending_raw_frame)  # not on the image any more
    assert live.strip_slot.isAncestorOf(live.pending_raw_frame)


def test_unsaved_captures_come_first_then_the_observations_images(qtbot, window, profile):
    _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    _capture_raw(qtbot, window, profile, "P1010002.ORF")
    strip = _strip(window)
    qtbot.waitUntil(lambda: sum(1 for item in strip._items if item.get("staged")) == 2, timeout=5000)
    assert [bool(item.get("staged")) for item in strip._items] == [True, True, False, False, False]
    assert all(item["frame_border_color"] == STAGING_BORDER for item in strip._items[:2])
    assert {item.get("id") for item in strip._items[2:]} == set(window.ids_for_test["saved"])
    live = window.live_lab_tab
    qtbot.waitUntil(live.pending_raw_frame.isVisible, timeout=5000)
    assert live.pending_raw_count_label.text() == "2 not saved"
    assert window.observation_list_panel.unsaved_counts() == {window.ids_for_test["first"]: 2}


def test_save_to_observation_moves_them_into_the_saved_images(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    qtbot.waitUntil(live.pending_raw_save_all_btn.isEnabled, timeout=5000)
    qtbot.mouseClick(live.pending_raw_save_all_btn, Qt.LeftButton)
    first = window.ids_for_test["first"]
    qtbot.waitUntil(lambda: len(ImageDB.get_images_for_observation(first)) == 4, timeout=10000)
    qtbot.waitUntil(lambda: not live._pending_raw_captures, timeout=5000)
    strip = _strip(window)
    qtbot.waitUntil(lambda: len(strip._items) == 4 and not any(i.get("staged") for i in strip._items), timeout=5000)
    assert window.observation_list_panel.unsaved_counts() == {}


def test_measure_shows_them_too_and_a_new_capture_leaves_its_selection_alone(qtbot, window, profile):
    _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    _to(qtbot, window, "measure_tab")
    strip = _strip(window)
    assert window.measure_strip_slot.isAncestorOf(strip)
    qtbot.waitUntil(lambda: bool(strip._items) and strip._items[0].get("staged"), timeout=5000)
    chosen = window.ids_for_test["saved"][1]
    window._on_measure_gallery_clicked(chosen, "")
    assert strip.selected_keys() == {chosen}

    _capture_raw(qtbot, window, profile, "P1010002.ORF")  # arrives while Measure holds the strip
    qtbot.waitUntil(lambda: sum(1 for item in strip._items if item.get("staged")) == 2, timeout=5000)
    assert strip.selected_keys() == {chosen}
    assert window.live_lab_tab._session_gallery_selected_keys() != [chosen]  # Live keeps its own


def test_live_lab_keeps_its_own_selection_across_pages(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    strip = _strip(window)
    qtbot.waitUntil(lambda: bool(strip._items) and strip._items[0].get("staged"), timeout=5000)
    pending_key = strip._items[0]["id"]
    live._on_session_gallery_clicked(pending_key, strip._items[0]["filepath"])
    strip.select_image(pending_key, center=False)
    _to(qtbot, window, "measure_tab")
    window._on_measure_gallery_clicked(window.ids_for_test["saved"][0], "")
    assert live._session_gallery_selected_keys() == [pending_key]
    _to(qtbot, window, "live_lab_tab")
    assert strip.selected_keys() == {pending_key}


def test_a_running_session_survives_switching_mode_and_keeps_its_observation(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    for page in ("observations_tab", "measure_tab", "analysis_tab", "ingestion_hub_tab", "live_lab_tab"):
        _to(qtbot, window, page)
        assert live.is_session_running(), page
    window.on_observation_selected(other, "Other", switch_tab=False)  # opened from the list
    qtbot.wait(50)
    assert window.active_observation_id == other
    assert live._target_observation_id == first
    assert live.session_target_notice.isVisible()
    assert _strip(window).shown_observation_id() == first
    _to(qtbot, window, "measure_tab")
    _to(qtbot, window, "live_lab_tab")
    assert _strip(window).shown_observation_id() == first


def test_view_and_measure_never_get_unsaved_captures_in_delete(qtbot, window, profile):
    strip = _strip(window)
    assert strip.channel("live", accepts_staged=True).accepts_staged
    assert not strip.channel("view").accepts_staged
    assert not strip.channel("measure").accepts_staged


def test_a_refresh_keeps_a_multi_selection_of_unsaved_captures(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    _capture_raw(qtbot, window, profile, "P1010002.ORF")
    strip = _strip(window)
    qtbot.waitUntil(lambda: sum(1 for item in strip._items if item.get("staged")) == 2, timeout=5000)
    paths = [item["filepath"] for item in strip._items[:2]]
    strip.select_paths(paths, center=False)
    keys = strip.selected_keys()
    assert len(keys) == 2
    live._refresh_session_gallery(reveal="preserve")
    assert strip.selected_keys() == keys


def test_the_stand_in_forgets_an_old_selection_on_an_edit_reset():
    from ui.live_lab_tab import _LiveAwayStrip

    class _Strip:
        pass

    away = _LiveAwayStrip(_Strip(), keys={"pending:a", "pending:b"}, paths=["/a", "/b"])
    away.exit_edit_selection()
    assert away.selected_keys() == set() and away.selected_paths() == []


def test_a_save_finishing_while_measure_holds_the_strip_keeps_its_selection(qtbot, window, profile):
    from database.models import ImageDB as _ImageDB

    live = _start_live(qtbot, window, profile)
    _capture_raw(qtbot, window, profile, "P1010001.ORF")
    qtbot.waitUntil(live.pending_raw_save_all_btn.isEnabled, timeout=5000)
    _to(qtbot, window, "measure_tab")
    strip = _strip(window)
    picked = window.ids_for_test["saved"][:2]
    strip.select_paths([_ImageDB.get_image(i)["filepath"] for i in picked], center=False)
    assert strip.selected_keys() == set(picked)
    live._commit_all_pending_raw_captures()
    first = window.ids_for_test["first"]
    qtbot.waitUntil(lambda: len(_ImageDB.get_images_for_observation(first)) == 4, timeout=10000)
    qtbot.waitUntil(lambda: not live._pending_raw_commit_is_busy(), timeout=10000)
    qtbot.wait(100)
    assert strip.selected_keys() == set(picked)


def test_images_follows_a_new_observation_without_a_session(qtbot, window):
    _to(qtbot, window, "live_lab_tab")
    other = window.ids_for_test["other"]
    window.observations_tab.table.selectRow(window.observations_tab._find_table_row_for_observation(other))
    window.observations_tab.set_selected_as_active(switch_tab=False)
    qtbot.waitUntil(lambda: window.live_lab_tab._target_observation_id == other, timeout=3000)
    assert _strip(window).shown_observation_id() == other


def test_images_saved_before_the_session_are_view_only(qtbot, window, profile):
    live = _start_live(qtbot, window, profile)
    strip = _strip(window)
    earlier = window.ids_for_test["saved"][0]
    qtbot.waitUntil(lambda: any(item.get("id") == earlier for item in strip._items), timeout=3000)
    strip.select_image(earlier, center=False)
    live._on_session_gallery_clicked(earlier, "")
    assert live._sidebar_binds_to_selection is False  # the capture bar edits the next capture
    messages = []
    live._show_status = lambda text, **_kwargs: messages.append(text)
    live._on_session_gallery_delete_requested(earlier)
    assert ImageDB.get_image(earlier) is not None
    assert messages and "before this session" in messages[0]
