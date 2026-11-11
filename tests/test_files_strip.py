"""Bilder › Files and SD card uses the shared strip (redesign stage 9f3).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. While Bilder shows Files, the
strip shared with View, Measure and Live replaces the editor's own gallery:
the editor renders its images there (new ones first, orange), and the strip's
clicks, multi-selection, removal and reordering reach the editor. The strip
goes back to Live, View or Measure when they show, and comes back with the
editor's selection.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from database.models import ImageDB
from tests.test_files_source import profile, window  # noqa: F401


def _page(window):
    return window.files_source_page


def _editor(window):
    return window.files_source_page.editor()


def _strip(window):
    return window.measure_gallery


def _frame(strip, key):
    return next(f for f in strip._frames if getattr(f, "image_key", None) == key)


def _key(result):
    return result.image_id if result.image_id is not None else result.filepath


def _add(qtbot, window, profile, name, seed):
    editor = _editor(window)
    before = len(editor.unsaved_new_results())
    editor.add_images([str(profile.jpeg(name, seed=seed))])
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == before + 1, timeout=5000)
    return editor


def _shown_keys(strip):
    return [item["id"] if item["id"] is not None else item["filepath"] for item in strip._items]


def test_files_shows_its_images_in_the_shared_strip(qtbot, window, profile):
    page, strip = _page(window), _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    assert page.holds_shared_strip() and strip.active_channel() == "files"
    assert editor.holds_strip() and editor.gallery is strip
    # The strip takes the editor's gallery place, under the image, as in Live.
    assert editor.dialog_gallery_splitter.widget(1) is strip and strip.isVisible()
    assert not editor._own_gallery.isVisible()  # one strip, not two
    saved, new = editor.import_results[0], editor.import_results[1]
    # New images first, orange, as Live's captures are; saved ones after them.
    assert _shown_keys(strip) == [new.filepath, saved.image_id]
    assert strip._items[0]["staged"] and not strip._items[1]["staged"]
    # The editor's order (the save order) is unchanged: new images are saved last.
    assert [r.image_id for r in editor.import_results] == [saved.image_id, None]


def test_clicks_and_multi_selection_reach_the_editor(qtbot, window, profile):
    strip = _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    saved, new = editor.import_results[0], editor.import_results[1]
    qtbot.mouseClick(_frame(strip, saved.image_id), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor.selected_indices == [0], timeout=2000)
    assert editor.selected_index == 0
    qtbot.mouseClick(_frame(strip, new.filepath), Qt.LeftButton, Qt.ControlModifier)
    qtbot.waitUntil(lambda: editor.selected_indices == [0, 1], timeout=2000)
    assert editor.type_hud.isVisibleTo(editor)  # the bars follow the selection made in the strip
    assert not editor.tools_hud.isVisibleTo(editor)  # tools are for one image at a time


def test_removing_in_the_strip_removes_from_the_editor(qtbot, window, profile):
    page, strip = _page(window), _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    new = editor.import_results[1]
    qtbot.mouseClick(_frame(strip, new.filepath), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor.selected_indices == [1], timeout=2000)
    window.activateWindow()  # QShortcuts act only in the active window
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(_frame(strip, new.filepath), Qt.LeftButton)  # focus goes to the strip, as a click does
    assert QApplication.focusWidget() is strip
    qtbot.keyClick(strip, Qt.Key_Delete)
    qtbot.waitUntil(lambda: not editor.unsaved_new_results(), timeout=3000)
    assert new.filepath not in _shown_keys(strip)
    assert len(editor.import_results) == 1
    first = window.ids_for_test["first"]
    qtbot.waitUntil(lambda: _strip(window).staged_items(first, "files") == [], timeout=3000)
    assert not page.save_row.isVisible()  # nothing left to save


def test_reordering_in_the_strip_reorders_the_saved_images(qtbot, window, profile):
    first = window.ids_for_test["first"]
    for n in range(2):
        ImageDB.add_image(first, str(profile.jpeg(f"more{n}.JPG", seed=20 + n)), "field")
    page = _page(window)
    page._drop_editor(first)
    page._observation_id = None
    page.show_observation(first)
    strip = _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    saved = [r.image_id for r in editor.import_results if r.image_id is not None]
    # The strip's drop handler (a real drag is not reliable offscreen): the last
    # saved image moves before the first.
    assert strip._reorder_item(saved[2], saved[0])
    assert [r.image_id for r in editor.import_results] == [saved[2], saved[0], saved[1], None]
    assert _shown_keys(strip)[0] == editor.import_results[3].filepath  # the new image stays first
    qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(ImageDB.get_images_for_observation(first)) == 4, timeout=5000)
    order = [int(row["id"]) for row in ImageDB.get_images_for_observation(first)]
    assert order[:3] == [saved[2], saved[0], saved[1]]


def test_the_strip_follows_the_source_and_keeps_the_editors_selection(qtbot, window, profile):
    live, page, strip = window.live_lab_tab, _page(window), _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    new = editor.import_results[1]
    qtbot.mouseClick(_frame(strip, new.filepath), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor.selected_indices == [1], timeout=2000)

    live.show_source(live.SOURCE_LIVE)
    assert live.holds_shared_strip() and strip.active_channel() == "live"
    assert live.strip_slot.isAncestorOf(strip)
    assert not page.holds_shared_strip() and not editor.holds_strip()
    assert editor.dialog_gallery_splitter.widget(1) is editor.gallery  # its own gallery is back
    # Live shows the observation: its saved image and Files' new one, unsaved.
    first = window.ids_for_test["first"]
    qtbot.waitUntil(lambda: strip.shown_observation_id() == first, timeout=3000)
    assert new.filepath in [item["filepath"] for item in strip._items if item.get("staged")]
    # A click in Live's strip does not reach the editor.
    saved_id = editor.import_results[0].image_id
    qtbot.mouseClick(_frame(strip, saved_id), Qt.LeftButton)
    assert editor.selected_indices == [1]

    live.show_source(live.SOURCE_FILES)
    assert page.holds_shared_strip() and editor.gallery is strip
    assert editor.dialog_gallery_splitter.isAncestorOf(strip) and strip.isVisible()
    assert strip.selected_paths() == [new.filepath]


def test_view_and_measure_take_the_strip_back(qtbot, window, profile):
    page, strip = _page(window), _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    first = window.ids_for_test["first"]
    window.tab_widget.setCurrentWidget(window.observations_tab)
    assert not page.holds_shared_strip() and not editor.holds_strip()
    assert strip.active_channel() == "view"
    assert window.observations_tab.gallery_slot.isAncestorOf(strip) and strip.isVisible()
    window.tab_widget.setCurrentIndex(1)  # Mål
    assert strip.active_channel() == "measure"
    qtbot.waitUntil(lambda: strip.shown_observation_id() == first, timeout=3000)
    # Background work in the editor does not reach the page that holds the strip.
    editor._refresh_gallery()
    assert strip.shown_observation_id() == first
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    assert page.holds_shared_strip() and editor.gallery is strip
    assert _shown_keys(strip)[0] == editor.import_results[1].filepath


def test_each_observation_shows_its_own_editor_in_the_strip(qtbot, window, profile):
    page, strip = _page(window), _strip(window)
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: page.observation_id() == other, timeout=3000)
    other_editor = _editor(window)
    assert other_editor is not editor and other_editor.gallery is strip
    assert not editor.holds_strip()
    assert _shown_keys(strip) == []  # the other observation has no images
    window.on_observation_selected(first, "First", switch_tab=False)
    qtbot.waitUntil(lambda: page.observation_id() == first, timeout=3000)
    assert editor.gallery is strip and len(_shown_keys(strip)) == 2


def test_saving_shows_the_saved_images_in_the_strip(qtbot, window, profile):
    page, strip = _page(window), _strip(window)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    first = window.ids_for_test["first"]
    qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: _editor(window) is not editor, timeout=5000)
    fresh = _editor(window)
    assert fresh.gallery is strip and not editor.holds_strip()
    ids = [int(row["id"]) for row in ImageDB.get_images_for_observation(first)]
    assert _shown_keys(strip) == ids
    assert not any(item.get("staged") for item in strip._items)


def test_lives_keys_do_not_act_in_files(qtbot, window, profile):
    """Live's keys are bound to all of Bilder: in Files they stay off (found in 9f3)."""
    live = window.live_lab_tab
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    contrast = live.contrast_combo.selected_value()
    target = "Phase" if contrast != "Phase" else "BF"
    editor.preview.setFocus(Qt.OtherFocusReason)
    qtbot.keyClick(editor.preview, Qt.Key_P if target == "Phase" else Qt.Key_B)
    assert live.contrast_combo.selected_value() == contrast
    # Delete in the editor itself removes the selected image again (it was ambiguous).
    qtbot.mouseClick(_frame(_strip(window), editor.import_results[1].filepath), Qt.LeftButton)
    qtbot.waitUntil(lambda: editor.selected_indices == [1], timeout=2000)
    editor.setFocus(Qt.OtherFocusReason)  # the preview keeps Delete for its own marks
    qtbot.keyClick(editor, Qt.Key_Delete)
    qtbot.waitUntil(lambda: not editor.unsaved_new_results(), timeout=3000)
    # Back in Live, its keys work.
    live.show_source(live.SOURCE_LIVE)
    live.live_image_label.setFocus(Qt.OtherFocusReason)
    qtbot.keyClick(live.live_image_label, Qt.Key_P if target == "Phase" else Qt.Key_B)
    qtbot.waitUntil(lambda: live.contrast_combo.selected_value() == target, timeout=2000)


def test_cloud_checkboxes_only_where_they_are_saved(qtbot, window, profile):
    """Files and Live save no cloud choice: the strip shows no checkbox there (Codex, 9f3)."""
    live, strip = window.live_lab_tab, _strip(window)
    _add(qtbot, window, profile, "new.JPG", 5)
    qtbot.waitUntil(lambda: bool(strip._frames), timeout=2000)
    assert strip.publish_checkbox_widgets() == []
    live.show_source(live.SOURCE_LIVE)
    qtbot.waitUntil(lambda: strip.shown_observation_id() == window.ids_for_test["first"], timeout=3000)
    assert strip.publish_checkbox_widgets() == []
    window.tab_widget.setCurrentWidget(window.observations_tab)
    qtbot.waitUntil(lambda: len(strip.publish_checkbox_widgets()) == 1, timeout=3000)  # the saved image
    window.tab_widget.setCurrentIndex(1)  # Mål
    qtbot.waitUntil(lambda: len(strip.publish_checkbox_widgets()) == 1, timeout=3000)


def test_the_selection_shown_is_the_editors(qtbot, window, profile):
    """Selection made in code (opening, adding) comes along into the strip and back (Codex, 9f3)."""
    live, strip = window.live_lab_tab, _strip(window)
    editor = _editor(window)
    saved_path = editor.import_results[0].filepath
    assert strip.selected_paths() == [saved_path]  # the first image, selected when the editor opened
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    shown = editor.import_results[editor.selected_index].filepath
    editor._refresh_gallery()  # background work redraws the strip
    assert strip.selected_paths() == [shown]
    editor._select_image(0)  # chosen in code, not by a click
    live.show_source(live.SOURCE_LIVE)
    assert editor._own_gallery is None and editor.gallery.selected_paths() == [saved_path]
    live.show_source(live.SOURCE_FILES)
    assert strip.selected_paths() == [saved_path]
