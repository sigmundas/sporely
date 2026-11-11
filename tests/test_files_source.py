"""Bilder › Files and SD card: Prepare Images in place (redesign stage 9f, first step).

Real ``MainWindow`` in an isolated profile under ``tmp_path``; no network.
Bilder switches between Live and Files and SD card; Files hosts today's
Prepare Images editor as a page for the observation shown, saves the same
way as the dialog, keeps unsaved work with its observation, mirrors new
images into the shared strip and the list's counts, and asks before quitting
with unsaved work.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox

from database.models import ImageDB, ObservationDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def window(qtbot, profile, monkeypatch):
    from ui.main_window import MainWindow

    first = create_observation(profile)
    ImageDB.add_image(first, str(profile.jpeg("saved.JPG", seed=2)), "field")
    other = ObservationDB.create_observation(date="2026-10-20 10:00", genus="Fomes", species="fomentarius")
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(first, "First", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    window.live_lab_tab.show_source(window.live_lab_tab.SOURCE_FILES)
    qtbot.waitUntil(lambda: window.files_source_page.editor() is not None, timeout=5000)
    # Quitting at teardown must not wait on the question.
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Discard)
    window.ids_for_test = {"first": first, "other": other}
    return window


def _add(qtbot, window, profile, name, seed):
    editor = window.files_source_page.editor()
    editor.add_images([str(profile.jpeg(name, seed=seed))])
    qtbot.waitUntil(lambda: bool(editor.unsaved_new_results()), timeout=5000)
    return editor


def test_bilder_switches_to_an_in_place_editor_for_the_observation_shown(qtbot, window):
    live = window.live_lab_tab
    page = window.files_source_page
    assert live.source_stack.currentWidget() is page
    editor = page.editor()
    assert not editor.isWindow()  # a page, not a dialog window
    assert editor.isVisible()
    assert page.observation_id() == window.ids_for_test["first"]
    assert not window.files_source_page.save_row.isVisible()  # nothing to save yet
    assert not editor.next_btn.isVisible()  # the page's row saves, not the editor's buttons
    live.show_source(live.SOURCE_LIVE)
    assert live.source_stack.currentWidget() is live._live_content


def test_new_images_are_unsaved_until_saved_and_esc_keeps_them(qtbot, window, profile):
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    first = window.ids_for_test["first"]
    page = window.files_source_page
    assert page.save_row.isVisible() and page.unsaved_label.text() == "1 not saved"
    assert page.save_btn.text() == "Save to observation" and page.discard_btn.text() == "Discard changes"
    corner = page.save_btn.mapTo(window, page.save_btn.rect().bottomRight())
    assert window.rect().contains(corner)  # in view, though the editor is taller than the page
    assert len(window.measure_gallery.staged_items(first, "files")) == 1
    assert window.observation_list_panel.unsaved_counts() == {first: 1}
    qtbot.keyClick(editor, Qt.Key_Escape)
    assert window.files_source_page.editor() is editor and editor.unsaved_new_results()

    qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(ImageDB.get_images_for_observation(first)) == 2, timeout=5000)
    qtbot.waitUntil(lambda: window.files_source_page.editor() is not editor, timeout=3000)
    assert window.measure_gallery.staged_items(first, "files") == []
    assert window.observation_list_panel.unsaved_counts() == {}


def test_discard_drops_the_new_images(qtbot, window, profile):
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    first = window.ids_for_test["first"]
    qtbot.mouseClick(window.files_source_page.discard_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: window.files_source_page.editor() is not editor, timeout=3000)
    assert len(ImageDB.get_images_for_observation(first)) == 1
    assert window.measure_gallery.staged_items(first, "files") == []


def test_unsaved_work_stays_with_its_observation(qtbot, window, profile):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: window.files_source_page.observation_id() == other, timeout=3000)
    assert window.files_source_page.editor() is not editor
    assert window.observation_list_panel.unsaved_counts() == {first: 1}
    window.on_observation_selected(first, "First", switch_tab=False)
    qtbot.waitUntil(lambda: window.files_source_page.observation_id() == first, timeout=3000)
    assert window.files_source_page.editor() is editor
    assert len(editor.unsaved_new_results()) == 1


def test_live_and_files_keep_their_own_unsaved_images(qtbot, window):
    strip = window.measure_gallery
    first = window.ids_for_test["first"]
    strip.set_staged_items(first, [{"id": "pending:a", "filepath": "/a.ORF"}], source="live")
    strip.set_staged_items(first, [{"id": None, "filepath": "/b.jpg"}], source="files")
    assert {item["filepath"] for item in strip.staged_items(first)} == {"/a.ORF", "/b.jpg"}
    strip.set_staged_items(first, [], source="live")
    assert [item["filepath"] for item in strip.staged_items(first)] == ["/b.jpg"]


def test_quitting_with_unsaved_files_asks_first(qtbot, window, profile, monkeypatch):
    _add(qtbot, window, profile, "new.JPG", 5)
    asked = []
    monkeypatch.setattr(
        QMessageBox, "question", lambda *args, **kwargs: asked.append(True) or QMessageBox.Cancel
    )
    window.close()
    assert asked == [True]
    assert window.isVisible()  # Cancel keeps the window and the work
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Discard)


def test_a_standalone_live_lab_has_no_source_switch(qtbot, profile):
    from tests.test_live_lab_behaviour import _make_tab

    tab = _make_tab(qtbot)
    assert getattr(tab, "source_selector", None) is None


def test_an_editor_opens_clean_and_stays_clean(qtbot, window):
    page = window.files_source_page
    qtbot.wait(900)  # longer than the unsaved-work check
    assert not page.editor().has_unsaved_changes()
    assert not page.save_row.isVisible()


def test_an_edit_that_marks_nothing_dirty_still_shows_save(qtbot, window):
    """Removing, cropping and reordering saved images do not mark the editor dirty."""
    page = window.files_source_page
    editor = page.editor()
    editor.import_results[0].notes = "Edited without marking dirty"
    qtbot.waitUntil(page.save_row.isVisible, timeout=2000)
    assert page.unsaved_label.text() == "Unsaved changes"


def test_saving_keeps_newer_edits_made_elsewhere(qtbot, window, profile):
    first = window.ids_for_test["first"]
    saved_id = ImageDB.get_images_for_observation(first)[0]["id"]
    ObservationDB.update_observation(first, gps_latitude=63.4, gps_longitude=10.4)
    ImageDB.update_image(saved_id, notes="Written elsewhere after Files opened")  # e.g. Edit photo
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    page = window.files_source_page
    qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(ImageDB.get_images_for_observation(first)) == 2, timeout=5000)
    assert ImageDB.get_image(saved_id)["notes"] == "Written elsewhere after Files opened"
    observation = ObservationDB.get_observation(first)
    assert (observation["gps_latitude"], observation["gps_longitude"]) == (63.4, 10.4)


def test_the_page_does_not_save_the_dialogs_window_geometry(qtbot, window, profile, monkeypatch):
    from ui.image_import_dialog import ImageImportDialog

    saved = []
    monkeypatch.setattr(ImageImportDialog, "_save_geometry", lambda self, *args: saved.append(True))
    other = window.ids_for_test["other"]
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: window.files_source_page.observation_id() == other, timeout=3000)
    editor = _add(qtbot, window, profile, "new.JPG", 5)
    qtbot.mouseClick(window.files_source_page.discard_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: window.files_source_page.editor() is not editor, timeout=3000)
    assert saved == []
