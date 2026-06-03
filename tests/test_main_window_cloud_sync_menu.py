from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.main_window as main_window


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _DummySpeciesAvailability:
    def __init__(self, *args, **kwargs):
        pass


def _build_menu_window(monkeypatch) -> main_window.MainWindow:
    triggered: list[bool] = []

    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "open_cloud_sync_dialog", lambda self: triggered.append(True))
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())

    window = main_window.MainWindow()
    window._cloud_sync_triggered = triggered
    return window


def test_settings_menu_includes_cloud_sync_action(monkeypatch, qapp):
    window = _build_menu_window(monkeypatch)

    settings_action = next(
        action for action in window.menuBar().actions()
        if action.text() == window.tr("Settings")
    )
    settings_menu = settings_action.menu()
    assert settings_menu is not None

    cloud_action = next(
        action for action in settings_menu.actions()
        if action.text() == window.tr("Sporely Cloud Sync")
    )

    cloud_action.trigger()

    assert window._cloud_sync_triggered == [True]

    window.deleteLater()
