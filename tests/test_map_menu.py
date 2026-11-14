"""Map services as a menu (redesign stage 10f2), on a real ``MainWindow``.

The map services used to be a dialog of buttons. They are a menu now:
Rediger's "Open map" on the map line, Vis's Map link in the table and
Filer's EXIF Map button each open it where they are. Picking an entry opens
that service in the browser (``webbrowser.open``, recorded here). Isolated
profile (the fixtures of ``tests/test_files_source.py``); no network.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, Qt, QTimer
from PySide6.QtWidgets import QLabel, QMenu

from database.models import ObservationDB
from tests.test_edit_mode import rediger  # noqa: F401
from tests.test_files_source import profile, window  # noqa: F401

LAT, LON = 63.43123, 10.39456


@pytest.fixture
def opened(monkeypatch):
    """URLs the browser was asked to open."""
    import webbrowser

    urls: list[str] = []
    monkeypatch.setattr(webbrowser, "open", lambda url, *args, **kwargs: urls.append(url) or True)
    return urls


@pytest.fixture
def menus(monkeypatch):
    """Menus opened with ``exec``: their entries and where they opened. The
    first entry is picked, as a click would."""
    import ui.observations_tab as observations_tab

    shown: list[dict] = []

    class RecordingMenu(QMenu):
        # PySide's exec cannot be patched on QMenu itself: the helper's module gets this one.
        def exec(self, pos=None, *args, **kwargs):
            actions = [action for action in self.actions() if not action.isSeparator()]
            shown.append({"labels": [action.text() for action in actions], "pos": pos})
            if actions:
                actions[0].trigger()
            return actions[0] if actions else None

    monkeypatch.setattr(observations_tab, "QMenu", RecordingMenu)
    return shown


def _labels(menu: QMenu) -> list[str]:
    return [action.text() if not action.isSeparator() else "—" for action in menu.actions()]


def test_open_map_is_on_rediger_s_map_line_and_needs_a_position(qtbot, window, rediger):
    editor = rediger.editor()
    assert editor.map_btn.isVisible() and not editor.map_tools.isVisible()  # no need to open the tools
    assert editor.map_btn.parentWidget() is editor.map_tools_toggle.parentWidget()
    assert editor.map_btn.menu() is editor.map_menu
    editor.lat_input.setValue(editor.lat_input.minimum())
    editor.lon_input.setValue(editor.lon_input.minimum())
    assert not editor.map_btn.isEnabled()
    editor.lat_input.setValue(LAT)
    editor.lon_input.setValue(LON)
    assert editor.map_btn.isEnabled()


def test_open_map_shows_the_services_for_the_position_and_opens_the_one_picked(qtbot, window, rediger, opened):
    editor = rediger.editor()
    editor.lat_input.setValue(LAT)
    editor.lon_input.setValue(LON)
    seen: dict = {}

    def pick():
        menu = editor.map_menu
        seen["visible"] = menu.isVisible()
        seen["labels"] = _labels(menu)
        seen["tooltips"] = menu.toolTipsVisible() and all(
            action.toolTip() for action in menu.actions() if not action.isSeparator()
        )
        google = next(action for action in menu.actions() if action.text() == "Google Maps")
        google.trigger()
        menu.close()

    QTimer.singleShot(200, pick)
    qtbot.mouseClick(editor.map_btn, Qt.LeftButton)  # the menu runs until pick() closes it
    qtbot.waitUntil(lambda: "labels" in seen, timeout=3000)
    assert seen["visible"] and seen["tooltips"]
    # Inocybe geophylla (the observation's name): its species pages follow a separator.
    assert seen["labels"] == [
        "Google Maps", "Kilden (NIBIO)", "Artskart (Artsdatabanken)", "Norge i Bilder",
        "iNaturalist — nearby observations", "—",
        "iNaturalist — Inocybe geophylla", "GBIF — Inocybe geophylla",
    ]
    assert opened == [f"https://www.google.com/maps?q={LAT},{LON}"]


def test_the_menu_follows_the_form_when_it_opens(qtbot, window, rediger):
    editor = rediger.editor()
    editor.lat_input.setValue(LAT)
    editor.lon_input.setValue(LON)
    editor.species_input.setText("")  # a genus only: no species pages
    editor.map_menu.aboutToShow.emit()
    assert "GBIF — Inocybe geophylla" not in _labels(editor.map_menu)
    assert "Google Maps" in _labels(editor.map_menu)
    editor.lat_input.setValue(editor.lat_input.minimum())  # no position: nothing to open
    editor.map_menu.aboutToShow.emit()
    assert _labels(editor.map_menu) == []


def test_vis_s_map_link_opens_the_menu_under_it(qtbot, window, menus, opened):
    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, gps_latitude=LAT, gps_longitude=LON)
    tab = window.observations_tab
    window.tab_widget.setCurrentWidget(tab)
    tab.refresh_observations()
    row = tab._find_table_row_for_observation(first)
    cell = tab.table.cellWidget(row, 8)
    label = cell if isinstance(cell, QLabel) else cell.findChild(QLabel)
    assert label is not None
    label.linkActivated.emit("#")
    assert len(menus) == 1 and menus[0]["labels"][0] == "Google Maps"
    assert menus[0]["pos"] == label.mapToGlobal(label.rect().bottomLeft())
    assert opened == [f"https://www.google.com/maps?q={LAT},{LON}"]


def test_filer_s_exif_map_button_opens_the_menu_under_it(qtbot, window, menus, opened):
    editor = window.files_source_page.editor()
    editor._current_exif_lat, editor._current_exif_lon = LAT, LON
    editor.exif_map_btn.setEnabled(True)
    editor.exif_map_btn.show()
    qtbot.mouseClick(editor.exif_map_btn, Qt.LeftButton)
    assert len(menus) == 1
    assert "Google Maps" in menus[0]["labels"] and not any("GBIF" in label for label in menus[0]["labels"])
    button = editor.exif_map_btn
    assert menus[0]["pos"] == button.mapToGlobal(QPoint(0, button.height() - 1))
    assert opened == [f"https://www.google.com/maps?q={LAT},{LON}"]


@pytest.mark.parametrize("lang, nearby, google", [
    ("nb_NO", "iNaturalist — observasjoner i nærheten", "Åpne sted i Google Maps"),
    ("de_DE", "iNaturalist — Beobachtungen in der Nähe", "Standort in Google Maps öffnen"),
])
def test_the_menu_is_translated(qapp, lang, nearby, google):
    """The services' texts are found under the context they are looked up in."""
    from pathlib import Path

    from PySide6.QtCore import QCoreApplication, QTranslator

    from ui.observations_tab import MapServiceHelper

    translator = QTranslator()
    assert translator.load(str(Path(__file__).resolve().parents[1] / "i18n" / f"Sporely_{lang}.qm"))
    QCoreApplication.installTranslator(translator)
    try:
        services = [s for s in MapServiceHelper(None).map_services(LAT, LON) if s is not None]
    finally:
        QCoreApplication.removeTranslator(translator)
    assert services[4][0] == nearby
    assert services[0][1] == google


def test_the_menu_outlives_a_link_label_replaced_while_it_is_open(qtbot, monkeypatch):
    """Vis's table can rebuild while the menu is open, replacing the Map link."""
    import shiboken6
    from PySide6.QtWidgets import QVBoxLayout, QWidget

    import ui.observations_tab as observations_tab

    host = QWidget()
    QVBoxLayout(host).addWidget(label := QLabel("Map"))
    qtbot.addWidget(host)
    host.show()
    survived = []

    class RebuildingMenu(QMenu):
        def exec(self, pos=None, *args, **kwargs):
            shiboken6.delete(label)  # the table replaced its cell widget meanwhile
            survived.append(shiboken6.isValid(self))
            return None

    monkeypatch.setattr(observations_tab, "QMenu", RebuildingMenu)
    observations_tab.MapServiceHelper(host).show_map_service_menu(LAT, LON, anchor=label)  # does not raise
    assert survived == [True]
