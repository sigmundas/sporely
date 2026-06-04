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


class _DummyBadge:
    def __init__(self) -> None:
        self.text = ""
        self.tooltip = ""
        self.visible = True

    def setText(self, value: str) -> None:
        self.text = str(value)

    def setToolTip(self, value: str) -> None:
        self.tooltip = str(value)

    def setVisible(self, value: bool) -> None:
        self.visible = bool(value)

    def clear(self) -> None:
        self.text = ""


class _DummyThread:
    def __init__(self, name: str, running: bool = True) -> None:
        self._name = name
        self._running = running

    def isRunning(self) -> bool:
        return self._running

    def objectName(self) -> str:
        return self._name


class _DummyApp:
    def __init__(self, threads: list[_DummyThread]) -> None:
        self._threads = threads

    def findChildren(self, _type):
        return list(self._threads)


def _build_window(monkeypatch) -> main_window.MainWindow:
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    return main_window.MainWindow()


def test_background_activity_badge_tracks_manual_tokens(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))

    token = window.begin_background_activity("Publishing to Artsobservasjoner")

    assert badge.visible is True
    assert badge.text == window.tr("Working")
    assert badge.tooltip == window.tr("Background work running:\n{details}").format(
        details="Publishing to Artsobservasjoner",
    )

    window.end_background_activity(token)

    assert badge.visible is False
    assert badge.text == ""
    window.deleteLater()


def test_background_activity_badge_summarizes_running_threads(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(
        main_window.QApplication,
        "instance",
        lambda: _DummyApp(
            [
                _DummyThread("Cloud sync"),
                _DummyThread("Cloud sync"),
                _DummyThread("Auto calibration"),
            ]
        ),
    )

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Working (3)")
    assert "Cloud sync × 2" in badge.tooltip
    assert "Auto calibration" in badge.tooltip
    window.deleteLater()
