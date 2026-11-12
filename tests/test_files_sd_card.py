"""“From SD card (new photos)” in Bilder › Files and SD card (redesign stage 9i).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); the card is a folder under ``tmp_path`` with a
``DCIM`` folder and EXIF photos, and the mounted-volume listing is a stand-in
(the precondition only). Owner 2026-11-12: a card is a volume with DCIM; a
photo is new when its file name and capture time match no saved image; the
photos added are those taken on the observation's day (Ny observasjon: the
newest day). Nothing is saved until Save.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFileDialog, QInputDialog, QMessageBox

from database.models import ImageDB, ObservationDB, SettingsDB
from tests.test_files_source import profile, window  # noqa: F401
from utils import card_photos


def _photo(folder, name, taken):
    """A JPEG with EXIF DateTimeOriginal (``yyyy:MM:dd HH:mm:ss``)."""
    from PIL import Image

    folder.mkdir(parents=True, exist_ok=True)
    exif = Image.Exif()
    exif[0x8769] = {36867: taken}
    path = folder / name
    Image.new("RGB", (96, 64), "green").save(path, exif=exif)
    return str(path)


@pytest.fixture
def card(tmp_path, monkeypatch):
    """A card whose photos are from the first observation's day (2026-10-12) and the day after."""
    root = tmp_path / "EOS_DIGITAL"
    dcim = root / "DCIM" / "100CANON"
    photos = {
        "same_day": _photo(dcim, "IMG_0001.JPG", "2026:10:12 09:15:00"),
        "imported": _photo(dcim, "IMG_0002.JPG", "2026:10:12 09:20:00"),
        "next_day": _photo(dcim, "IMG_0003.JPG", "2026:10:13 14:00:00"),
    }
    monkeypatch.setattr(card_photos, "find_camera_cards", lambda volume_roots=None: [root])
    return root, photos


def _show_files(qtbot, window):
    page = window.files_source_page
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    window.live_lab_tab.show_source(window.live_lab_tab.SOURCE_FILES)
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    return page


def _new_paths(editor):
    return sorted(os.path.basename(result.filepath) for result in editor.unsaved_new_results())


def test_a_card_is_a_volume_with_dcim(tmp_path):
    with_dcim, without = tmp_path / "CARD", tmp_path / "USB"
    (with_dcim / "DCIM").mkdir(parents=True)
    without.mkdir()
    assert card_photos.find_camera_cards([without, with_dcim]) == [with_dcim]


def test_adds_the_new_photos_from_the_observations_day(qtbot, window, profile, card):
    first = window.ids_for_test["first"]
    _root, photos = card
    # IMG_0002 is in Sporely already (its original file name and capture time).
    saved = ImageDB.add_image(
        first, str(profile.jpeg("stored.JPG", seed=4)), "field",
        captured_at="2026-10-12 09:20:00", original_filepath=photos["imported"],
    )
    assert saved
    page = _show_files(qtbot, window)
    assert page.sd_card_btn.isVisible() and page.sd_card_btn.text() == "From SD card (new photos)"
    images_before = len(ImageDB.get_images_for_observation(first))
    told = []
    window.observations_tab.set_status_message = lambda text, **kwargs: told.append(text)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    editor = page.editor()
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0001.JPG"], timeout=5000)
    assert told and "2026-10-12" in told[-1] and "1" in told[-1]
    assert len(ImageDB.get_images_for_observation(first)) == images_before  # nothing saved before Save
    assert page.save_row.isVisible()
    # Pressing again adds nothing twice.
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    qtbot.wait(200)
    assert _new_paths(editor) == ["IMG_0001.JPG"]
    assert "No new photos from 2026-10-12" in told[-1]


def test_ny_observasjon_takes_the_newest_day(qtbot, window, card):
    page = _show_files(qtbot, window)
    window.start_new_observation()
    qtbot.waitUntil(lambda: page.is_showing_new(), timeout=3000)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    editor = page.editor()
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0003.JPG"], timeout=5000)


def test_no_new_photos_on_the_day_says_so(qtbot, window, card):
    other = window.ids_for_test["other"]  # dated 2026-10-20: nothing on the card that day
    window.on_observation_selected(other, "Other", switch_tab=False)
    page = _show_files(qtbot, window)
    qtbot.waitUntil(lambda: page.observation_id() == other, timeout=3000)
    told = []
    window.observations_tab.set_status_message = lambda text, **kwargs: told.append(text)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    qtbot.wait(200)
    assert page.editor().unsaved_new_results() == []
    assert told and "2026-10-20" in told[-1] and "3 new on other days" in told[-1]


def test_without_a_card_a_chosen_folder_is_read_and_remembered(qtbot, window, card, monkeypatch):
    root, _photos = card
    monkeypatch.setattr(card_photos, "find_camera_cards", lambda volume_roots=None: [])
    asked = []

    def answer(box):
        asked.append([button.text() for button in box.buttons()])
        choose = next(button for button in box.buttons() if button.text() == "Choose folder…")
        box.clickedButton = lambda: choose  # the user's click (the precondition only)
        return 0

    monkeypatch.setattr(QMessageBox, "exec", answer)
    monkeypatch.setattr(QFileDialog, "getExistingDirectory", lambda *args, **kwargs: str(root))
    page = _show_files(qtbot, window)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    editor = page.editor()
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0001.JPG", "IMG_0002.JPG"], timeout=5000)
    assert SettingsDB.get_setting(card_photos.SETTING_CARD_FOLDER, "") == str(root)
    # Next time the remembered folder is offered.
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    assert any(text == f"Use {root.name}" for text in asked[-1])


def test_with_several_cards_it_asks_which(qtbot, window, card, tmp_path, monkeypatch):
    root, _photos = card
    other_card = tmp_path / "OTHER"
    (other_card / "DCIM").mkdir(parents=True)
    monkeypatch.setattr(card_photos, "find_camera_cards", lambda volume_roots=None: [other_card, root])
    offered = []
    monkeypatch.setattr(
        QInputDialog, "getItem", lambda *args, **kwargs: offered.append(args[3]) or (str(root), True),
    )
    page = _show_files(qtbot, window)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    editor = page.editor()
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0001.JPG", "IMG_0002.JPG"], timeout=5000)
    assert offered == [[str(other_card), str(root)]]
    assert ObservationDB.get_observation(window.ids_for_test["first"]) is not None


def test_ny_observasjon_pressed_twice_keeps_the_newest_day(qtbot, window, card):
    page = _show_files(qtbot, window)
    window.start_new_observation()
    qtbot.waitUntil(lambda: page.is_showing_new(), timeout=3000)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    editor = page.editor()
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0003.JPG"], timeout=5000)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    qtbot.wait(200)
    assert _new_paths(editor) == ["IMG_0003.JPG"]  # not the day before


def test_a_second_click_while_photos_are_added_adds_nothing(qtbot, window, card, monkeypatch):
    page = _show_files(qtbot, window)
    editor = page.editor()
    original = editor.add_images
    clicked = []

    def add_images_with_a_click_meanwhile(paths):
        if not clicked:
            clicked.append(True)
            page._from_sd_card()  # the event loop turned: a second click arrives
        original(paths)

    monkeypatch.setattr(editor, "add_images", add_images_with_a_click_meanwhile)
    qtbot.mouseClick(page.sd_card_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: _new_paths(editor) == ["IMG_0001.JPG", "IMG_0002.JPG"], timeout=5000)
    qtbot.wait(200)
    assert _new_paths(editor) == ["IMG_0001.JPG", "IMG_0002.JPG"]
    assert clicked and page.sd_card_btn.isEnabled()


def test_a_raw_pair_is_imported_when_either_file_is():
    signatures = {("img_0001.jpg", "2026-10-12 09:15:00")}
    from datetime import datetime

    row = {
        "filename": "IMG_0001.CR3", "filepath": "/card/DCIM/IMG_0001.CR3", "raw_path": "/card/DCIM/IMG_0001.CR3",
        "camera_jpeg_path": "/card/DCIM/IMG_0001.JPG", "captured_at": datetime(2026, 10, 12, 9, 15),
    }
    assert card_photos.row_is_imported(row, signatures)
    assert not card_photos.row_is_imported({**row, "captured_at": datetime(2026, 10, 12, 9, 16)}, signatures)


def test_an_observation_date_with_an_offset_is_read_as_local_time(monkeypatch):
    import time

    monkeypatch.setenv("TZ", "Europe/Oslo")
    time.tzset()
    try:
        assert str(card_photos.observation_day("2026-10-12T22:30:00+00:00")) == "2026-10-13"
        assert str(card_photos.observation_day("2026-10-12 22:30")) == "2026-10-12"
    finally:
        monkeypatch.undo()
        time.tzset()
