"""Bilder › Files and SD card: the mockup's layout on the image (redesign stage 9f2).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. In place, Prepare Images puts
Field or Microscope (F/M), a field photo's time and GPS with “Use for the
observation”, Info (EXIF in a drawer) and the image tools on the image, and
Live Lab's objective and contrast bar for microscope images only; the details
panel goes, optimal-sampling resizing is a preference (off by default), and
“Choose files…” sits beside Bilder's source switch. The standalone dialog is
unchanged until stage 9g.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFileDialog

from database.models import ImageDB, ObservationDB, SettingsDB
from tests.test_files_source import profile, window  # noqa: F401
from tools.review_ui.exif_fixture import jpeg_with_exif


def _editor(window):
    return window.files_source_page.editor()


def _add_exif_photo(qtbot, window, profile, name="exif.JPG", **exif):
    editor = _editor(window)
    path = jpeg_with_exif(profile.fixtures_dir / name, seed=41, **exif)
    editor.add_images([str(path)])
    qtbot.waitUntil(lambda: bool(editor.unsaved_new_results()), timeout=5000)
    index = next(i for i, result in enumerate(editor.import_results) if result.image_id is None)
    editor._select_image(index)
    return editor, index


def _visible(widget) -> bool:
    return widget.isVisibleTo(widget.window())


def _click_select(qtbot, editor, results) -> None:
    """Select ``results`` in the editor's gallery as the user does: click, then Ctrl-click."""
    gallery = editor.gallery
    for n, result in enumerate(results):
        key = result.image_id if result.image_id is not None else result.filepath
        frame = next(f for f in gallery._frames if getattr(f, "image_key", None) == key)
        qtbot.mouseClick(frame, Qt.LeftButton, Qt.NoModifier if n == 0 else Qt.ControlModifier)
    qtbot.waitUntil(lambda: len(editor.selected_indices) == len(results), timeout=2000)


def test_the_details_go_onto_the_image(qtbot, window):
    editor = _editor(window)
    assert not _visible(editor.details_panel)
    assert not _visible(editor.image_type_selector)
    assert not _visible(editor.micro_settings_group)
    assert not _visible(editor._add_images_row)
    assert _visible(editor.type_hud)
    assert editor.type_hud.current() == "field"  # the saved field photo is selected
    for button in (editor.image_crop_btn, editor.ai_crop_btn, editor.rotate_preview_btn):
        assert editor.tools_hud.isAncestorOf(button)
    assert not _visible(editor.microscope_bar)  # field photo
    assert not _visible(editor.slide_prep_group)  # from the start, not only after a selection change
    # The dialog's window minimum is gone: the page fits a normal window without scrolling.
    assert editor.minimumSize().height() == 0
    frame = window.files_source_page._frames[window.ids_for_test["first"]]
    qtbot.waitUntil(lambda: frame.verticalScrollBar().maximum() == 0, timeout=2000)


def test_field_and_microscope_on_the_image(qtbot, window):
    editor = _editor(window)
    qtbot.mouseClick(editor.type_hud.micro_btn, Qt.LeftButton)
    result = editor.import_results[editor.selected_index]
    assert result.image_type == "microscope"
    assert editor.type_hud.current() == "micro"
    assert _visible(editor.microscope_bar)
    assert _visible(editor.slide_prep_group)
    assert not _visible(editor.time_gps_hud)
    assert editor.has_unsaved_changes()
    qtbot.mouseClick(editor.type_hud.field_btn, Qt.LeftButton)
    assert editor.import_results[editor.selected_index].image_type == "field"
    assert not _visible(editor.microscope_bar)
    assert not _visible(editor.slide_prep_group)


def test_the_microscope_bar_sets_all_selected(qtbot, window, profile):
    obs = window.ids_for_test["first"]
    for n in range(2):
        ImageDB.add_image(obs, str(profile.jpeg(f"m{n}.JPG", seed=50 + n)), "microscope",
                          objective_name="100X", contrast="DIC")
    window.files_source_page._drop_editor(obs)
    window.files_source_page._observation_id = None
    window.files_source_page.show_observation(obs)
    editor = _editor(window)
    _click_select(qtbot, editor, [r for r in editor.import_results if r.image_type == "microscope"])
    assert _visible(editor.microscope_bar)
    assert editor.type_hud.current() == "micro"
    segments = editor.microscope_bar.contrast_segments
    index = next(i for i in segments._buttons if editor.contrast_combo.item_at(i).value == "Phase")
    qtbot.mouseClick(segments._buttons[index], Qt.LeftButton)
    assert [r.contrast for r in editor.import_results if r.image_type == "microscope"] == ["Phase", "Phase"]


def test_a_mixed_selection_checks_neither_type(qtbot, window, profile):
    editor, _index = _add_exif_photo(qtbot, window, profile)
    editor.import_results[_index].image_type = "microscope"
    _click_select(qtbot, editor, editor.import_results)
    assert editor.type_hud.current() is None
    assert not _visible(editor.microscope_bar)
    assert not _visible(editor.time_gps_hud)  # one photo at a time


def test_use_for_the_observation_saves_time_and_gps(qtbot, window, profile):
    obs = window.ids_for_test["first"]
    editor, _index = _add_exif_photo(qtbot, window, profile)
    assert _visible(editor.time_gps_hud)
    text = editor.time_gps_hud.text_label.text()
    assert "2026-11-07 09:21" in text and "63.4200, 10.3107" in text
    qtbot.mouseClick(editor.time_gps_hud.use_btn, Qt.LeftButton)
    assert not editor.time_gps_hud.use_btn.isEnabled()
    assert editor.time_gps_hud.use_btn.text() == "Used for the observation"
    qtbot.mouseClick(window.files_source_page.save_btn, Qt.LeftButton)
    saved = ObservationDB.get_observation(obs)
    assert saved["date"] == "2026-11-07 09:21:00"
    assert abs(float(saved["gps_latitude"]) - 63.42) < 1e-4
    assert abs(float(saved["gps_longitude"]) - 10.3107) < 1e-4


def test_without_use_the_observation_keeps_its_time_and_gps(qtbot, window, profile):
    obs = window.ids_for_test["first"]
    # An observation without GPS takes the first photo's GPS (the editor's
    # behaviour before stage 9f2 too); one that has GPS keeps it.
    ObservationDB.update_observation(obs, gps_latitude=60.0, gps_longitude=11.0)
    page = window.files_source_page
    page._drop_editor(obs)
    page._observation_id = None
    page.show_observation(obs)
    before = ObservationDB.get_observation(obs)
    _add_exif_photo(qtbot, window, profile)
    qtbot.mouseClick(window.files_source_page.save_btn, Qt.LeftButton)
    after = ObservationDB.get_observation(obs)
    assert after["date"] == before["date"]
    assert after["gps_latitude"] == before["gps_latitude"]
    assert after["gps_longitude"] == before["gps_longitude"]
    assert len(ImageDB.get_images_for_observation(obs)) == 2


def test_use_is_offered_while_either_value_differs(qtbot, window, profile):
    obs = window.ids_for_test["first"]
    # Same GPS as the photo, another time: the time can still be used.
    ObservationDB.update_observation(obs, gps_latitude=63.42, gps_longitude=10.3107)
    page = window.files_source_page
    page._drop_editor(obs)
    page._observation_id = None
    page.show_observation(obs)
    editor, _index = _add_exif_photo(qtbot, window, profile)
    assert editor.time_gps_hud.use_btn.isEnabled()
    qtbot.mouseClick(editor.time_gps_hud.use_btn, Qt.LeftButton)
    assert not editor.time_gps_hud.use_btn.isEnabled()
    # Another photo, another place: offered again.
    other = jpeg_with_exif(profile.fixtures_dir / "other.JPG", seed=42, latitude=59.9, longitude=10.7)
    editor.add_images([str(other)])
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == 2, timeout=5000)
    editor._select_image(len(editor.import_results) - 1)
    assert editor.time_gps_hud.use_btn.isEnabled()


def test_info_shows_the_exif_in_a_drawer_only_when_there_is_exif(qtbot, window, profile):
    editor = _editor(window)
    assert not _visible(editor.info_btn)  # the saved fixture photo has no EXIF
    editor, _index = _add_exif_photo(qtbot, window, profile)
    assert _visible(editor.info_btn)
    qtbot.mouseClick(editor.info_btn, Qt.LeftButton)
    assert editor.info_drawer.is_open()
    assert editor.info_drawer.isAncestorOf(editor.exif_camera_label)
    assert editor.exif_camera_label.text() == "TG-6"
    qtbot.keyClick(editor.info_btn, Qt.Key_Escape)  # focus is not in the drawer
    qtbot.waitUntil(lambda: not editor.info_drawer.is_open(), timeout=2000)
    assert editor.has_unsaved_changes()  # Esc closed the drawer, not the work
    qtbot.mouseClick(editor.info_btn, Qt.LeftButton)
    assert editor.info_drawer.is_open()
    no_exif = next(i for i, r in enumerate(editor.import_results) if r.image_id is not None)
    editor._select_image(no_exif)
    assert not editor.info_drawer.is_open()
    assert not _visible(editor.info_btn)


def test_choose_files_sits_beside_the_switch(qtbot, window, profile, monkeypatch):
    live = window.live_lab_tab
    button = window.files_source_page.choose_files_btn
    assert _visible(button)
    path = str(profile.jpeg("chosen.JPG", seed=60))
    monkeypatch.setattr(QFileDialog, "getOpenFileNames", lambda *args, **kwargs: ([path], ""))
    qtbot.mouseClick(button, Qt.LeftButton)
    editor = _editor(window)
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == 1, timeout=5000)
    live.show_source(live.SOURCE_LIVE)
    assert not _visible(button)


def test_resizing_is_a_preference_off_by_default(qtbot, window, profile):
    from ui import main_window

    assert not bool(SettingsDB.get_setting("resize_to_optimal_sampling", False))
    hub = main_window.SettingsHubDialog(window, start_page=main_window.SettingsHubDialog.PAGE_IMAGE_IMPORT)
    qtbot.addWidget(hub)
    assert not hub._resize_optimal_checkbox.isChecked()
    editor = _editor(window)
    editor.add_images([str(profile.jpeg("before.JPG", seed=61))])
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == 1, timeout=5000)
    assert editor.unsaved_new_results()[0].resize_to_optimal is False
    hub._resize_optimal_checkbox.click()
    assert bool(SettingsDB.get_setting("resize_to_optimal_sampling", False))
    # The page kept its editor (unsaved work); images added now follow the preference.
    assert _editor(window) is editor
    editor.add_images([str(profile.jpeg("after.JPG", seed=62))])
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == 2, timeout=5000)
    assert editor.unsaved_new_results()[1].resize_to_optimal is True


def test_the_standalone_dialog_keeps_its_layout(qtbot, profile):
    from ui.image_import_dialog import ImageImportDialog

    dialog = ImageImportDialog(None, import_results=[], continue_to_observation_details=False)
    qtbot.addWidget(dialog)
    dialog.show()
    assert dialog.details_panel.isVisible()
    assert dialog.image_type_selector.isVisible()
    assert not hasattr(dialog, "type_hud")
    dialog.reject()
