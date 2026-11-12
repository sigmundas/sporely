"""Edit photo opens Bilder › Files and SD card (redesign stage 9g4a).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. The Add images dialog is retired
for saved observations: every “Edit photo” (Vis' and Mål's strip, Live,
Kameraimport, Rediger) shows Bilder › Files and SD card for the photo's
observation, on that photo. The modal calibration and species plate dialogs
no longer offer it (Bilder would open behind them).
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMenu, QMessageBox

from database.models import ImageDB
from tests.test_files_source import profile, window  # noqa: F401
from ui.image_import_dialog import ImageImportDialog


@pytest.fixture(autouse=True)
def no_dialog(monkeypatch):
    """The dialog must never open: its exec fails the test."""

    def fail(*_args, **_kwargs):
        raise AssertionError("the Add images dialog opened")

    monkeypatch.setattr(ImageImportDialog, "exec", fail)


@pytest.fixture
def menu_choice(monkeypatch):
    """Right-click menus pick the action named in ``choice['text']``; their texts are recorded.

    ``QMenu.exec`` is modal, so the strip's menu class is replaced by one whose
    ``exec`` chooses instead of waiting.
    """
    import ui.image_gallery_widget as gallery_module

    choice = {"text": "Edit photo", "seen": []}

    class _ChoosingMenu(QMenu):
        def exec(self, *_args, **_kwargs):
            choice["seen"].append([action.text() for action in self.actions()])
            return next((action for action in self.actions() if action.text() == choice["text"]), None)

    monkeypatch.setattr(gallery_module, "QMenu", _ChoosingMenu)
    return choice


def _strip(window):
    return window.measure_gallery


def _frame(strip, key):
    return next(f for f in strip._frames if getattr(f, "image_key", None) == key)


def _keys(strip):
    return [item["id"] if item["id"] is not None else item["filepath"] for item in strip._items]


def _first_images(window):
    return ImageDB.get_images_for_observation(window.ids_for_test["first"])


def _add_saved(window, profile, observation_id, name, seed, kind="field"):
    """The saved image's stored path (the database may keep a copy)."""
    image_id = ImageDB.add_image(observation_id, str(profile.jpeg(name, seed=seed)), kind)
    return ImageDB.get_image(image_id)["filepath"]


def _files_shows(window, observation_id, path) -> bool:
    page, live = window.files_source_page, window.live_lab_tab
    editor = page.editor()
    return (
        window.tab_widget.currentWidget() is live
        and live.current_source() == live.SOURCE_FILES
        and page.observation_id() == observation_id
        and editor is not None
        and editor.selected_index is not None
        and editor.import_results[editor.selected_index].filepath == path
    )


def _show_view(qtbot, window):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    qtbot.waitUntil(lambda: _strip(window).active_channel() == "view", timeout=3000)


def test_edit_photo_in_vis_opens_files_on_that_photo(qtbot, window, profile, menu_choice):
    first = window.ids_for_test["first"]
    second = _add_saved(window, profile, first, "second.JPG", 3)
    _show_view(qtbot, window)
    window.observations_tab.refresh_observations()
    window.on_observation_selected(first, "First", switch_tab=False)
    strip = _strip(window)
    target = next(row for row in _first_images(window) if row["filepath"] == second)
    qtbot.waitUntil(lambda: target["id"] in _keys(strip), timeout=5000)
    qtbot.mouseClick(_frame(strip, target["id"]), Qt.RightButton)
    assert "Edit photo" in menu_choice["seen"][-1]
    qtbot.waitUntil(lambda: _files_shows(window, first, second), timeout=5000)


def test_edit_photo_in_maal_opens_files_on_that_photo(qtbot, window, profile, menu_choice):
    first = window.ids_for_test["first"]
    second = _add_saved(window, profile, first, "second.JPG", 3)
    window.tab_widget.setCurrentIndex(1)
    window.on_observation_selected(first, "First", switch_tab=False)
    strip = _strip(window)
    qtbot.waitUntil(lambda: strip.active_channel() == "measure", timeout=3000)
    target = next(row for row in _first_images(window) if row["filepath"] == second)
    qtbot.waitUntil(lambda: target["id"] in _keys(strip), timeout=5000)
    qtbot.mouseClick(_frame(strip, target["id"]), Qt.RightButton)
    assert "Edit photo" in menu_choice["seen"][-1]
    qtbot.waitUntil(lambda: _files_shows(window, first, second), timeout=5000)


def test_edit_photo_of_another_observation_shows_that_observation(qtbot, window, profile):
    """Kameraimport's imported photos can belong to any observation.

    From Vis, with the first observation selected in the table: showing Bilder
    must not switch back to it. The card's path is not the stored one.
    """
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    _add_saved(window, profile, other, "earlier.JPG", 6)  # shown first by default
    stored = _add_saved(window, profile, other, "other.JPG", 5)
    card_path = os.path.join("/Volumes/CARD/DCIM/100CANON", os.path.basename(stored))
    assert card_path != stored
    _show_view(qtbot, window)
    window.observations_tab.refresh_observations()
    window.observations_tab.table.selectRow(window.observations_tab._find_table_row_for_observation(first))
    hub = window.ingestion_hub_tab
    # The precondition only: the last scan recognised this card photo as imported.
    hub._match_for_path = lambda path: {"filepath": path, "observation_id": other, "already_imported": True}
    assert int(window.active_observation_id) == first
    hub._on_staging_gallery_edit_requested(None, card_path)
    qtbot.waitUntil(lambda: _files_shows(window, other, stored), timeout=5000)
    assert int(window.active_observation_id) == other
    qtbot.waitUntil(lambda: window.observations_tab.get_selected_observation()[0] == other, timeout=3000)
    qtbot.wait(50)  # nothing queued switches it back
    assert int(window.active_observation_id) == other and _files_shows(window, other, stored)


def test_a_photo_not_imported_yet_opens_nothing_from_kameraimport(qtbot, window, profile):
    hub = window.ingestion_hub_tab
    window.tab_widget.setCurrentWidget(window.observations_tab)
    hub._match_for_path = lambda path: {"filepath": path, "observation_id": 1, "already_imported": False}
    hub._on_staging_gallery_edit_requested(None, "/nowhere/new.JPG")
    assert window.tab_widget.currentWidget() is window.observations_tab


def test_a_running_live_session_for_another_observation_keeps_bilder(qtbot, window, profile, monkeypatch):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    photo = _add_saved(window, profile, other, "other.JPG", 5)
    live = window.live_lab_tab
    _show_view(qtbot, window)
    # The precondition only: a session runs for the first observation.
    monkeypatch.setattr(live, "is_session_running", lambda: True)
    told = []
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: told.append(args[2]))
    assert window.open_files_for_image(other, photo) is True
    assert told and "End the session" in told[0]
    assert window.tab_widget.currentWidget() is window.observations_tab
    assert int(window.active_observation_id) == first
    # Its own observation's photo still opens.
    saved = _first_images(window)[0]["filepath"]
    assert window.open_files_for_image(first, saved) is True
    qtbot.waitUntil(lambda: _files_shows(window, first, saved), timeout=5000)
    assert len(told) == 1


def test_nothing_opens_for_an_unknown_observation(qtbot, window):
    _show_view(qtbot, window)
    assert window.open_files_for_image(987654, "/x.JPG") is False
    assert window.open_files_for_image(None, "/x.JPG") is False
    assert window.tab_widget.currentWidget() is window.observations_tab


def test_modal_dialogs_no_longer_offer_edit_photo(qtbot, window):
    from ui.calibration_dialog import CalibrationDialog
    from ui.species_plate_dialog import SpeciesPlateDialog

    calibration = CalibrationDialog(window)
    qtbot.addWidget(calibration)
    assert not calibration.image_gallery._show_edit
    plate = SpeciesPlateDialog({"id": window.ids_for_test["first"], "genus": "Fomes"}, parent=window)
    qtbot.addWidget(plate)
    assert not plate._gallery._show_edit
