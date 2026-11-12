"""Ny observasjon in Bilder › Files and SD card (redesign stage 9g4b).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. New (the button, N, the list's
New) and files dropped on View open Files for a not-yet-saved observation.
Nothing is written before its Save, which creates it as Utkast with the
photos' date and GPS and the New observation form's defaults, then Rediger
opens. The Add images and New observation dialogs never open.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox

from database.models import ImageDB, ObservationDB, SettingsDB
from tests.test_files_source import profile, window  # noqa: F401
from ui.image_import_dialog import ImageImportDialog
from ui.observation_details_dialog import ObservationDetailsDialog

LADE = (63.4425, 10.4472)
BYNESET = (63.3311, 10.1234)


@pytest.fixture(autouse=True)
def no_dialogs(monkeypatch):
    """Neither dialog may open: their exec fails the test."""

    def fail(*_args, **_kwargs):
        raise AssertionError("a retired dialog opened")

    monkeypatch.setattr(ImageImportDialog, "exec", fail)
    monkeypatch.setattr(ObservationDetailsDialog, "exec", fail)


def _photo(profile, name, taken, gps):
    """A JPEG with EXIF DateTimeOriginal (``yyyy:MM:dd HH:mm:ss``) and GPS."""
    from PIL import Image

    def dms(value):
        minutes, seconds = divmod(abs(value) * 3600, 60)
        degrees, minutes = divmod(minutes, 60)
        return (degrees, minutes, round(seconds, 4))

    exif = Image.Exif()
    exif[0x8769] = {36867: taken}
    exif[0x8825] = {1: "N", 2: dms(gps[0]), 3: "E", 4: dms(gps[1])}
    path = profile.fixtures_dir / name
    Image.new("RGB", (96, 64), "green").save(path, exif=exif)
    return str(path)


def _ids():
    return {int(row["id"]) for row in ObservationDB.get_all_observations()}


def _start_from_vis(qtbot, window):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    qtbot.mouseClick(window.observations_tab.new_btn, Qt.LeftButton)
    page = window.files_source_page
    qtbot.waitUntil(lambda: page.is_showing_new(), timeout=3000)
    return page


def _add(qtbot, page, *paths):
    editor = page.editor()
    before = len(editor.unsaved_new_results())
    editor.add_images(list(paths))
    qtbot.waitUntil(lambda: len(editor.unsaved_new_results()) == before + len(paths), timeout=5000)
    return editor


def _save_new(qtbot, window, before):
    qtbot.mouseClick(window.files_source_page.save_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(_ids() - before) == 1, timeout=5000)
    (created,) = _ids() - before
    qtbot.waitUntil(lambda: window.tab_widget.currentIndex() == window.EDIT_PAGE, timeout=5000)
    return created


def test_new_opens_files_for_an_unsaved_observation_and_writes_nothing(qtbot, window):
    first = window.ids_for_test["first"]
    before = _ids()
    page = _start_from_vis(qtbot, window)
    live = window.live_lab_tab
    assert window.tab_widget.currentWidget() is live and live.current_source() == live.SOURCE_FILES
    assert page.observation_id() is None and page.editor() is not None
    assert page.save_row.isVisible()
    assert page.save_btn.text() == "Save new observation" and page.discard_btn.text() == "Discard"
    assert page.unsaved_label.text() == "New observation, no photos yet"
    assert int(window.active_observation_id) == first  # the observation shown stays
    assert window.observation_header_label.text() == "New observation"
    assert _ids() == before


def test_save_creates_a_draft_with_the_photos_date_and_gps_then_rediger_opens(qtbot, window, profile):
    SettingsDB.set_setting("sporely_cloud_default_sharing_scope", "friends")
    before = _ids()
    page = _start_from_vis(qtbot, window)
    photo = _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE)
    _add(qtbot, page, photo)
    assert page.unsaved_label.text() == "New observation, 1 not saved"
    assert _ids() == before  # nothing before Save
    created = _save_new(qtbot, window, before)
    row = ObservationDB.get_observation(created)
    assert str(row["date"]).startswith("2026-09-14 10:30")
    assert row["gps_latitude"] == pytest.approx(LADE[0], abs=1e-4)
    assert row["gps_longitude"] == pytest.approx(LADE[1], abs=1e-4)
    assert row["location_source"] == "image_exif" and row["location_status"] == "acquired"
    assert bool(row["is_draft"])
    assert row["sharing_scope"] == "friends"
    images = ImageDB.get_images_for_observation(created)
    assert len(images) == 1 and images[0]["image_type"] == "field"
    assert int(window.active_observation_id) == created
    qtbot.waitUntil(lambda: window.edit_page.observation_id() == created, timeout=5000)
    assert not page.is_showing_new()
    assert not page.has_unsaved_work()


def test_the_photo_used_for_the_observation_sets_its_time_and_gps(qtbot, window, profile):
    before = _ids()
    page = _start_from_vis(qtbot, window)
    lade = _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE)
    byneset = _photo(profile, "byneset.JPG", "2026:09:15 16:45:00", BYNESET)
    editor = _add(qtbot, page, lade, byneset)
    editor.select_image_by_path(byneset)
    qtbot.waitUntil(lambda: editor.import_results[editor.selected_index].filepath == byneset, timeout=3000)
    editor.time_gps_hud.useRequested.emit()  # “Use for the observation”
    editor.select_image_by_path(lade)  # then look at the other photo
    qtbot.waitUntil(lambda: editor.import_results[editor.selected_index].filepath == lade, timeout=3000)
    created = _save_new(qtbot, window, before)
    row = ObservationDB.get_observation(created)
    assert str(row["date"]).startswith("2026-09-15 16:45")
    assert row["gps_latitude"] == pytest.approx(BYNESET[0], abs=1e-4)
    assert len(ImageDB.get_images_for_observation(created)) == 2


def test_save_without_photos_creates_an_observation(qtbot, window):
    before = _ids()
    _start_from_vis(qtbot, window)
    created = _save_new(qtbot, window, before)
    assert ImageDB.get_images_for_observation(created) == []
    assert bool(ObservationDB.get_observation(created)["is_draft"])


def test_discard_writes_nothing_and_shows_the_observation_again(qtbot, window, profile):
    first = window.ids_for_test["first"]
    before = _ids()
    page = _start_from_vis(qtbot, window)
    _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    qtbot.mouseClick(page.discard_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: page.observation_id() == first, timeout=3000)
    assert not page.is_showing_new() and not page.has_unsaved_work()
    assert window.observation_header_label.text() != "New observation"
    assert _ids() == before


@pytest.mark.parametrize("cancelled", ["publish notice", "duplicate question"])
def test_cancelling_a_question_writes_nothing_and_keeps_the_photos(qtbot, window, profile, monkeypatch, cancelled):
    if cancelled == "publish notice":
        monkeypatch.setattr(ObservationDetailsDialog, "confirm_publish_change", lambda self, *a, **k: False)
    else:
        monkeypatch.setattr(
            type(window.observations_tab), "_confirm_duplicate_observation_creation", lambda self, *a, **k: False,
        )
    before = _ids()
    page = _start_from_vis(qtbot, window)
    editor = _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    qtbot.wait(100)
    assert _ids() == before
    assert page.is_showing_new() and page.editor() is editor and editor.isVisible()
    assert len(editor.unsaved_new_results()) == 1
    assert page.save_btn.isEnabled()


def test_switching_pages_keeps_the_new_observation_on_screen(qtbot, window, profile):
    page = _start_from_vis(qtbot, window)
    editor = _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    header = window.observation_header_label
    for index in (0, 1, 3):  # Vis, Mål, back to Bilder
        window.tab_widget.setCurrentIndex(index)
        qtbot.wait(20)
        # The header names the observation shown there.
        assert (header.text() == "New observation") == (index == 3)
    assert page.is_showing_new() and page.editor() is editor
    window.live_lab_tab.show_source(window.live_lab_tab.SOURCE_LIVE)
    assert header.text() != "New observation"
    window.live_lab_tab.show_source(window.live_lab_tab.SOURCE_FILES)
    assert page.is_showing_new() and page.editor() is editor
    assert header.text() == "New observation"


def test_choosing_another_observation_keeps_the_photos_until_new_or_quit(qtbot, window, profile):
    other = window.ids_for_test["other"]
    page = _start_from_vis(qtbot, window)
    editor = _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: page.observation_id() == other, timeout=3000)
    assert not page.is_showing_new()
    assert page.has_unsaved_work()  # quitting asks
    # New shows them again.
    window.observations_tab.create_new_observation()
    qtbot.waitUntil(lambda: page.is_showing_new(), timeout=3000)
    assert page.editor() is editor and len(editor.unsaved_new_results()) == 1
    # Leaving with nothing added frees it.
    editor.reject()
    window.observations_tab.create_new_observation()
    qtbot.waitUntil(lambda: page.is_showing_new(), timeout=3000)
    empty = page.editor()
    window.on_observation_selected(window.ids_for_test["first"], "First", switch_tab=False)
    qtbot.waitUntil(lambda: not page.is_showing_new(), timeout=3000)
    assert empty is not page.editor() and not page.has_unsaved_work()


def test_quitting_with_the_new_observations_photos_asks(qtbot, window, profile, monkeypatch):
    page = _start_from_vis(qtbot, window)
    _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    asked = []
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: asked.append(args[2]) or QMessageBox.Cancel)
    assert not window.close()
    assert asked and "not saved" in asked[0]
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Discard)


def test_files_dropped_on_vis_start_a_new_observation_with_them(qtbot, window, profile):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    photo = _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE)
    window.observations_tab.create_new_observation(initial_image_paths=[photo])  # what a drop schedules
    page = window.files_source_page
    qtbot.waitUntil(lambda: page.is_showing_new() and len(page.editor().unsaved_new_results()) == 1, timeout=5000)
    assert page.editor().unsaved_new_results()[0].filepath == photo


def test_the_lists_new_starts_a_new_observation(qtbot, window):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window._on_observation_list_new_requested()
    qtbot.waitUntil(lambda: window.files_source_page.is_showing_new(), timeout=3000)


def test_a_failure_part_way_never_saves_the_photos_twice(qtbot, window, profile, monkeypatch):
    """The row is written, then a photo fails: Save is not offered again for the same photos."""
    tab = window.observations_tab

    def fail(*_args, **_kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(tab, "_apply_import_results_to_observation", fail)
    before = _ids()
    page = _start_from_vis(qtbot, window)
    _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    with qtbot.capture_exceptions() as raised:
        qtbot.mouseClick(page.save_btn, Qt.LeftButton)
    assert raised and "disk full" in str(raised[0][1])
    assert len(_ids() - before) == 1
    assert not page.is_showing_new() and not page.has_unsaved_work()


def test_saving_does_not_report_the_photos_as_kept(qtbot, window, profile, monkeypatch):
    told = []
    monkeypatch.setattr(
        type(window.observations_tab), "set_status_message", lambda self, message, *a, **k: told.append(message),
    )
    before = _ids()
    page = _start_from_vis(qtbot, window)
    _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    _save_new(qtbot, window, before)
    assert not any("kept" in message for message in told)


def test_edit_photo_of_the_observation_shown_leaves_the_new_observation(qtbot, window, profile):
    first = window.ids_for_test["first"]
    page = _start_from_vis(qtbot, window)
    editor = _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    window.tab_widget.setCurrentWidget(window.observations_tab)
    saved = ImageDB.get_images_for_observation(first)[0]["filepath"]
    assert window.open_files_for_image(first, saved)
    qtbot.waitUntil(lambda: page.observation_id() == first, timeout=3000)
    shown = page.editor()
    assert shown is not editor and shown.import_results[shown.selected_index].filepath == saved
    assert page.has_unsaved_work()  # the new observation's photos wait
    assert window.observation_header_label.text() != "New observation"


def test_choosing_another_observation_during_a_live_session_leaves_the_new_one(qtbot, window, profile, monkeypatch):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    page = _start_from_vis(qtbot, window)
    _add(qtbot, page, _photo(profile, "lade.JPG", "2026:09:14 10:30:00", LADE))
    live = window.live_lab_tab
    # The precondition only: a session runs for the first observation (Bilder stays on it).
    monkeypatch.setattr(live, "is_session_running", lambda: True)
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: not page.is_showing_new(), timeout=3000)
    assert page.observation_id() == first  # the session's observation
    assert page.has_unsaved_work()
