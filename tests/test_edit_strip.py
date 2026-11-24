"""Rediger uses the shared strip (redesign stage 9g3c).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. Owner 2026-11-12: Rediger shows
the strip shared with Vis, Mål and Bilder instead of the form's own photo row,
below the form; selecting photos there still chooses which ones AI looks at.
Images are not changed here: no removing, reordering or cloud checkboxes;
Edit and a double-click open Bilder › Files and SD card on the image.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, Qt

from database.models import ImageDB, ObservationDB
from tests.test_files_source import profile, window  # noqa: F401


@pytest.fixture
def rediger(qtbot, window, profile):
    first = window.ids_for_test["first"]
    # Three images: two field photos and a microscope one.
    ImageDB.add_image(first, str(profile.jpeg("second.JPG", seed=3)), "field")
    ImageDB.add_image(first, str(profile.jpeg("micro.JPG", seed=4)), "microscope")
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    qtbot.waitUntil(lambda: len(_shown_keys(window.measure_gallery)) == 3, timeout=5000)
    return page


def _strip(window):
    return window.measure_gallery


def _shown_keys(strip):
    return [item["id"] if item["id"] is not None else item["filepath"] for item in strip._items]


def _image_ids(window):
    return [row["id"] for row in ImageDB.get_images_for_observation(window.ids_for_test["first"])]


def _frame(strip, key):
    return next(f for f in strip._frames if getattr(f, "image_key", None) == key)


def _top(widget, page) -> int:
    return widget.mapTo(page, QPoint(0, 0)).y()


def test_rediger_shows_the_observation_in_the_shared_strip(qtbot, window, rediger):
    strip, editor = _strip(window), rediger.editor()
    assert rediger.holds_shared_strip() and strip.active_channel() == "edit"
    assert editor.holds_strip() and editor.image_gallery is strip
    assert strip.parentWidget() is rediger.strip_slot and strip.isVisible()
    assert not editor._own_image_gallery.isVisible()  # one strip, not two
    assert _shown_keys(strip) == _image_ids(window)
    # Below the form, above the bottom bar (hints, Edit images, the save status).
    bar = rediger._bar
    assert _top(rediger._frame, rediger) < _top(strip, rediger) < _top(bar, rediger)
    assert bar.isVisible() and editor.edit_images_btn.isVisible()


def test_images_are_not_changed_here(qtbot, window, rediger):
    strip = _strip(window)
    assert not strip._show_delete and not strip._show_publish_checkbox
    assert not strip._reorderable
    assert not strip._show_change_settings and not strip._show_move_to_observation
    assert not strip._show_delete_cloud_copy
    # The Delete key reaches no handler: the images stay.
    key = _image_ids(window)[0]
    qtbot.mouseClick(_frame(strip, key), Qt.LeftButton)
    qtbot.keyClick(strip, Qt.Key_Delete)
    assert _image_ids(window)[0] == key and len(_image_ids(window)) == 3


def test_selecting_photos_chooses_what_ai_looks_at(qtbot, window, rediger):
    strip, editor = _strip(window), rediger.editor()
    first, second, micro = _image_ids(window)
    # Nothing chosen: AI looks at the field photos, as with the form's own row.
    strip.select_paths([], center=False)
    assert editor._ai_guess_indices() == [0, 1]
    qtbot.mouseClick(_frame(strip, second), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor._ai_guess_indices() == [1], timeout=2000)
    qtbot.mouseClick(_frame(strip, micro), Qt.LeftButton, Qt.ControlModifier)
    qtbot.waitUntil(lambda: editor._ai_guess_indices() == [1, 2], timeout=2000)
    # Two chosen: the form's hint says so, as it did for its own row.
    assert editor._gallery_multi_hint_active


def test_a_double_click_opens_files_on_that_image(qtbot, window, rediger):
    strip = _strip(window)
    first = window.ids_for_test["first"]
    second = _image_ids(window)[1]
    path = ImageDB.get_image(second)["filepath"]
    qtbot.mouseDClick(_frame(strip, second), Qt.LeftButton)
    live, files = window.live_lab_tab, window.files_source_page
    qtbot.waitUntil(lambda: window.tab_widget.currentWidget() is live, timeout=3000)
    assert live.current_source() == live.SOURCE_FILES and files.observation_id() == first
    # Files holds the strip now; Rediger's form is back on its own (hidden) row.
    assert files.holds_shared_strip() and strip.active_channel() == "files"
    assert not rediger.holds_shared_strip() and not rediger.editor().holds_strip()
    assert strip._show_delete and strip._reorderable  # Files removes and reorders again
    editor = files.editor()
    assert editor.import_results[editor.selected_index].filepath == path


def test_the_strip_goes_back_to_vis_and_returns_with_rediger_s_selection(qtbot, window, rediger):
    strip, editor = _strip(window), rediger.editor()
    second = _image_ids(window)[1]
    qtbot.mouseClick(_frame(strip, second), Qt.LeftButton)
    qtbot.waitUntil(lambda: strip.selected_keys() == {second}, timeout=2000)
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert strip.active_channel() == "view" and strip.parentWidget() is window.observations_tab.gallery_slot
    assert strip._show_delete and strip._reorderable  # Vis removes and reorders again
    assert not editor.holds_strip() and editor.image_gallery is not strip and not editor.image_gallery.isVisible()
    # Rediger's choice is kept with the form, not left in Vis's strip.
    assert editor._ai_guess_indices() == [1]
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: strip.active_channel() == "edit" and strip.parentWidget() is rediger.strip_slot,
                    timeout=3000)
    assert rediger.editor() is editor  # the form was kept (nothing changed meanwhile)
    assert strip.selected_keys() == {second} and editor._ai_guess_indices() == [1]
    assert not strip._show_delete


def test_another_observation_shows_its_own_images(qtbot, window, rediger, profile):
    strip = _strip(window)
    other = window.ids_for_test["other"]
    other_image = ImageDB.add_image(other, str(profile.jpeg("other.JPG", seed=7)), "field")
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: rediger.observation_id() == other and rediger.editor() is not None, timeout=5000)
    qtbot.waitUntil(lambda: _shown_keys(strip) == [other_image], timeout=5000)
    assert rediger.editor().holds_strip() and strip.isVisible()


def test_rediger_keeps_a_compact_strip_and_vis_keeps_its_height(qtbot, window, rediger):
    from ui.splitter_state import GALLERY_MIN_HEIGHT

    strip = _strip(window)
    # One compact row under the form: the form needs the room.
    assert GALLERY_MIN_HEIGHT <= strip.height() <= GALLERY_MIN_HEIGHT + 10
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    qtbot.wait(50)
    vis_height = strip.height()
    assert vis_height > GALLERY_MIN_HEIGHT + 50
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: strip.parentWidget() is rediger.strip_slot, timeout=3000)
    qtbot.wait(50)
    assert strip.height() <= GALLERY_MIN_HEIGHT + 10
    # Back in Vis (and on to Mål) the strip has Vis's height, not Rediger's.
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    qtbot.wait(50)
    assert abs(strip.height() - vis_height) <= 2
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: strip.parentWidget() is rediger.strip_slot, timeout=3000)
    qtbot.mouseClick(window.mode_bar.measure_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: strip.parentWidget() is window.measure_strip_slot, timeout=3000)
    qtbot.wait(50)
    assert abs(strip.height() - vis_height) <= 2


def test_an_empty_rediger_shows_no_strip(qtbot, window, rediger):
    window.edit_page.forget_observation(window.ids_for_test["first"])
    assert not rediger.strip_slot.isVisible() and not _strip(window).isVisible()


# -- Codex review of stage 9g3c ---------------------------------------------------


def test_the_ai_image_chosen_in_the_strip_is_saved(qtbot, window, rediger):
    """As with the form's own row: the image chosen for AI is part of the AI
    state, which saves about a second later (the change watch)."""
    import json

    strip, editor = _strip(window), rediger.editor()
    first = window.ids_for_test["first"]
    second = _image_ids(window)[1]
    assert editor._persist_ai_paused  # opened, nothing touched yet: nothing written
    qtbot.mouseClick(_frame(strip, second), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor._ai_selected_index == 1, timeout=2000)
    qtbot.waitUntil(
        lambda: json.loads(ObservationDB.get_observation(first).get("ai_state_json") or "{}").get("selected_index")
        == 1,
        timeout=4000,
    )


def test_edit_images_key_works_with_a_thumbnail_focused(qtbot, window, rediger):
    from PySide6.QtWidgets import QApplication

    strip = _strip(window)
    second = _image_ids(window)[1]
    qtbot.mouseClick(_frame(strip, second), Qt.LeftButton)
    strip.setFocus(Qt.MouseFocusReason)
    qtbot.waitUntil(lambda: strip.isAncestorOf(QApplication.focusWidget()) or QApplication.focusWidget() is strip,
                    timeout=2000)
    key = rediger.editor()._edit_images_shortcut.key()
    qtbot.keyClick(QApplication.focusWidget(), key[0].key(), key[0].keyboardModifiers())
    qtbot.waitUntil(lambda: window.tab_widget.currentWidget() is window.live_lab_tab, timeout=3000)
