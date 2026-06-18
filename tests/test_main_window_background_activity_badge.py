from __future__ import annotations

import json
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QCloseEvent
from PySide6.QtCore import Qt

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
        self.cursor_shape = None
        self.mousePressEvent = lambda _event: None

    def setText(self, value: str) -> None:
        self.text = str(value)

    def setToolTip(self, value: str) -> None:
        self.tooltip = str(value)

    def setVisible(self, value: bool) -> None:
        self.visible = bool(value)

    def clear(self) -> None:
        self.text = ""

    def setCursor(self, cursor) -> None:
        self.cursor_shape = cursor

    def unsetCursor(self) -> None:
        self.cursor_shape = None


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

    def processEvents(self):
        return None


def _build_window(monkeypatch) -> main_window.MainWindow:
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_get_cloud_client", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    return main_window.MainWindow()


def test_background_activity_badge_tracks_manual_tokens(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])

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
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])
    monkeypatch.setattr(
        main_window.QApplication,
        "instance",
        lambda: _DummyApp(
            [
                _DummyThread("Export image"),
                _DummyThread("Auto calibration"),
                _DummyThread("Refreshing reference cache"),
            ]
        ),
    )

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Working (3)")
    assert "Export image" in badge.tooltip
    assert "Auto calibration" in badge.tooltip
    window.deleteLater()


def test_background_activity_badge_restores_logged_in_copy_from_cached_client(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    restored_client = SimpleNamespace(user_id="user-123")
    monkeypatch.setattr(main_window.MainWindow, "_get_cloud_client", lambda self: restored_client)
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [390])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])
    monkeypatch.setattr(
        main_window,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "error",
            "cloud_last_sync_summary": "Cloud sync sign-in failed. Please check your email and password.",
        },
    )

    window._cloud_client = None
    window._refresh_background_activity_badge()

    assert window._cloud_client is restored_client
    assert badge.visible is True
    assert badge.text == window.tr("Sync blocked")
    assert "Logged in, click Sync now to sync." in badge.tooltip
    window.deleteLater()


def test_background_activity_badge_shows_syncing_for_active_cloud_sync(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "is_cloud_sync_running", lambda self: True)
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([_DummyThread("Cloud sync")]))

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Syncing...")
    assert "Cloud sync running." in badge.tooltip
    window.deleteLater()


def test_background_activity_badge_shows_sync_pending_when_idle(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    window._cloud_client = SimpleNamespace(user_id="user-123")
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [390, 389, 385])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window, "get_app_settings", lambda: {})

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Sync pending")
    assert "Cloud sync pending for observation IDs 390, 389, 385." in badge.tooltip
    assert "Logged in, click Sync now to sync." in badge.tooltip
    assert "observation IDs 390, 389, 385" in badge.tooltip
    window.deleteLater()


def test_background_activity_badge_shows_sync_blocked_when_login_failed(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    window._cloud_client = None
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [390, 389, 385])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [])
    monkeypatch.setattr(
        main_window,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "error",
            "cloud_last_sync_summary": "Cloud sync sign-in failed. Please check your email and password.",
            "cloud_last_sync_errors_json": json.dumps([
                "Cloud sync sign-in failed. Please check your email and password.",
            ]),
        },
    )

    opened: list[str] = []
    monkeypatch.setattr(window, "_show_cloud_sync_details_dialog", lambda: opened.append("open"))

    window._refresh_background_activity_badge()
    badge.mousePressEvent(None)

    assert badge.visible is True
    assert badge.text == window.tr("Sync blocked")
    assert "Cloud sync sign-in failed. Please check your email and password." in badge.tooltip
    assert "Cloud sync pending" not in badge.tooltip
    assert "Sign in again, then click Sync now to retry uploads." in badge.tooltip
    assert badge.cursor_shape == Qt.PointingHandCursor
    assert opened == ["open"]
    window.deleteLater()


def test_background_activity_badge_prefers_blocked_over_pending_when_login_failed(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    window._cloud_client = None
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [390, 389, 385])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [401])
    monkeypatch.setattr(
        main_window,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "error",
            "cloud_last_sync_summary": "Cloud sync sign-in failed. Please check your email and password.",
            "cloud_last_sync_errors_json": json.dumps([
                "Cloud sync sign-in failed. Please check your email and password.",
            ]),
        },
    )

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Sync blocked")
    assert "Cloud sync sign-in failed. Please check your email and password." in badge.tooltip
    window.deleteLater()


def test_background_activity_badge_shows_sync_blocked_when_only_blocked(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [401, 402])

    window._refresh_background_activity_badge()

    assert badge.visible is True
    assert badge.text == window.tr("Sync blocked")
    assert "Cloud sync blocked for observation IDs 401, 402." in badge.tooltip
    assert "Click Sync blocked to review the error details." in badge.tooltip
    assert badge.cursor_shape == Qt.PointingHandCursor
    window.deleteLater()


def test_background_activity_badge_click_opens_cloud_sync_details(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    monkeypatch.setattr(main_window.QApplication, "instance", lambda: _DummyApp([]))
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [214, 82])

    opened: list[str] = []
    monkeypatch.setattr(window, "_show_cloud_sync_details_dialog", lambda: opened.append("open"))

    window._refresh_background_activity_badge()
    badge.mousePressEvent(None)

    assert opened == ["open"]
    window.deleteLater()


def test_cloud_sync_details_text_includes_blocked_reasons(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_observation_ids", lambda self: [390, 389])
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_blocked_observation_ids", lambda self: [214, 82])
    monkeypatch.setattr(
        main_window,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "blocked",
            "cloud_last_sync_summary": "Cloud sync blocked",
            "cloud_last_sync_at": "2026-06-04T12:34:56+00:00",
            "cloud_last_sync_errors_json": json.dumps(["calibration conflict"]),
        },
    )
    observation_map = {
        214: {
            "id": 214,
            "genus": "Agaricus",
            "species": "campestris",
            "sync_error_code": "privacy_slot_limit",
            "sync_error_message": "obs 214: privacy slot limit reached",
            "sync_blocked_reason": "Free accounts can have up to 20 private or fuzzed-location cloud observations. Make one public, delete one, or upgrade to Pro.",
            "sync_blocked_at": "2026-06-04T12:30:00+00:00",
        },
        82: {
            "id": 82,
            "species_guess": "Unknown species",
            "sync_error_code": "image_too_large_for_plan",
            "sync_error_message": "obs 82: image too large for your plan",
            "sync_blocked_reason": "Image is too large for your plan. Make it smaller or upgrade to Pro.",
            "sync_blocked_at": "2026-06-04T12:31:00+00:00",
        },
    }
    monkeypatch.setattr(main_window.ObservationDB, "get_observation", lambda obs_id: observation_map.get(int(obs_id)))

    details = window._cloud_sync_details_text()

    assert "Status: blocked" in details
    assert "Summary: Cloud sync blocked" in details
    assert "Raw sync errors:" in details
    assert "calibration conflict" in details
    assert "Pending observations:" in details
    assert "Observation IDs 390, 389" in details
    assert "Blocked observations:" in details
    assert "Observation 214: Agaricus campestris" in details
    assert "Free accounts can have up to 20 private or fuzzed-location cloud observations." in details
    assert "Blocked at: 2026-06-04 12:30 UTC" in details
    assert "Observation 82: Unknown species" in details
    assert "Reason: Image upload was rejected by the worker." in details
    assert "Image is too large for your plan" not in details
    window.deleteLater()


def test_close_event_saves_state_and_closes_without_starting_cloud_sync(monkeypatch, qapp):
    window = _build_window(monkeypatch)
    badge = _DummyBadge()
    window._background_activity_badge = badge
    shutdown_calls: list[str] = []
    save_calls: list[str] = []
    window.observations_tab = SimpleNamespace(shutdown=lambda: shutdown_calls.append("observations"))
    window.live_lab_tab = SimpleNamespace(shutdown=lambda: shutdown_calls.append("live_lab"))
    window._save_current_image_measure_view_settings = lambda: save_calls.append("image_view")
    window._save_geometry = lambda: save_calls.append("geometry")
    window._cloud_client = SimpleNamespace(user_id="user-123")
    monkeypatch.setattr(main_window.MainWindow, "_cloud_sync_pending_count", lambda self: 2)
    monkeypatch.setattr(main_window.MainWindow, "is_cloud_sync_running", lambda self: False)
    monkeypatch.setattr(
        window,
        "start_cloud_sync",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("closeEvent must not start cloud sync")),
    )

    event = QCloseEvent()
    window.closeEvent(event)

    assert event.isAccepted() is True
    assert window._close_after_cloud_sync is False
    assert save_calls == ["image_view", "geometry"]
    assert shutdown_calls == ["observations", "live_lab"]
    assert badge.visible is True
    assert badge.text == ""
    window.deleteLater()
