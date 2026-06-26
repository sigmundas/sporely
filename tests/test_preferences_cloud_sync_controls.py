from __future__ import annotations

import json
import os
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QWidget, QCheckBox, QLineEdit
from PySide6.QtWidgets import QPlainTextEdit
from PySide6.QtCore import QPoint, Signal

import ui.main_window as main_window
import utils.cloud_sync as cloud_sync


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _StubDatabaseSettingsDialog(QWidget):
    microscopeTagsChanged = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)


class _FakeMainWindow(QWidget):
    def __init__(self, *, running: bool = False):
        super().__init__()
        self.observations_tab = SimpleNamespace(_cloud_sync_worker=None)
        self._running = bool(running)
        self.start_calls: list[dict] = []

    def start_cloud_sync(
        self,
        *,
        show_status: bool,
        run_refresh_flow: bool,
        sync_images: bool = True,
        materialize_remote_images: bool,
    ) -> bool:
        self.start_calls.append(
            {
                "show_status": bool(show_status),
                "run_refresh_flow": bool(run_refresh_flow),
                "sync_images": bool(sync_images),
                "materialize_remote_images": bool(materialize_remote_images),
            }
        )
        self._running = True
        return True

    def is_cloud_sync_running(self) -> bool:
        return self._running

    def _update_corner_ui(self) -> None:
        return None


def _build_settings_hub_dialog(
    monkeypatch,
    qapp,
    *,
    app_settings: dict | None = None,
    client=None,
    running: bool = False,
    set_setting_sink: list[tuple[str, object]] | None = None,
    uploaders: list | None = None,
    patch_controls: bool = True,
):
    default_client = {
        "user_id": "user-123",
        "fetch_current_user_info": lambda: {"email": "sigmund.as@gmail.com"},
        "fetch_profile": lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        "fetch_cloud_plan_profile": lambda: {"cloud_plan": "free", "is_pro": False},
        "count_remote_privacy_slots": lambda: 1,
        "list_remote_observations": lambda: [
            {"visibility": "private", "location_precision": "exact"},
            {"visibility": "public", "location_precision": "exact"},
        ],
    }
    fake_client = SimpleNamespace(**default_client)
    if client is not None:
        fake_client.__dict__.update(getattr(client, "__dict__", {}))
    fake_parent = _FakeMainWindow(running=running)

    monkeypatch.setattr(main_window, "DatabaseSettingsDialog", _StubDatabaseSettingsDialog)
    monkeypatch.setattr(main_window.SettingsDB, "get_profile", lambda: {
        "username": "",
        "name": "",
        "email": "",
        "bio": "",
        "avatar_url": "",
    })
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    if set_setting_sink is None:
        monkeypatch.setattr(main_window.SettingsDB, "set_setting", lambda *args, **kwargs: None)
    else:
        monkeypatch.setattr(
            main_window.SettingsDB,
            "set_setting",
            lambda key, value: set_setting_sink.append((str(key), value)),
        )
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_window, "get_app_settings", lambda: dict(app_settings or {}))
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_status", lambda self: None)
    if patch_controls:
        monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_controls", lambda self: None)
    monkeypatch.setattr("utils.artsobs_uploaders.list_uploaders", lambda: list(uploaders or []))
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    dialog = main_window.SettingsHubDialog(fake_parent)
    fake_parent._cloud_client = fake_client
    dialog._cloud_client = fake_client
    if hasattr(dialog, "_artsobs_dialog") and dialog._artsobs_dialog is not None:
        dialog._artsobs_dialog._cloud_client = fake_client
    dialog.show()
    qapp.processEvents()
    dialog.refresh_cloud_sync_status()
    qapp.processEvents()
    return fake_parent, dialog


def _auth_refresh_failure():
    try:
        raise cloud_sync.CloudSyncError("GET https://example.test/rest/v1/profiles status=401: auth refresh failed")
    except cloud_sync.CloudSyncError as cause:
        try:
            raise cloud_sync.CloudTemporarilyUnavailableError(
                "Supabase/cloud sync is temporarily unavailable; local data was not overwritten."
            ) from cause
        except cloud_sync.CloudTemporarilyUnavailableError as error:
            return error


def test_profile_cloud_controls_expose_sync_actions(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)

    assert dialog.cloud_sync_now_button.text() == "Sync now"
    assert dialog.cloud_sync_log_button.text() == "Sync log"
    assert dialog._hub_cancel_button.text() == "Cancel"
    assert dialog._hub_save_button.text() == "Save"
    assert dialog.cloud_sync_now_button.isEnabled() is True
    assert dialog.cloud_sync_log_button.isEnabled() is True
    assert not hasattr(dialog, "cloud_sync_summary_label")
    assert not hasattr(dialog, "cloud_offline_media_button")
    assert not hasattr(dialog, "cloud_repair_calibration_conflicts_button")
    assert dialog._artsobs_dialog._cloud_section.objectName() == "sectionCard"
    assert dialog.cloud_sync_group.objectName() == "sectionCard"
    cloud_card = dialog._artsobs_dialog
    assert cloud_card.cloud_status_label.text() == "Signed in as: sigmund.as@gmail.com"
    assert cloud_card.cloud_plan_label.text() == "Plan: Free"
    assert cloud_card.cloud_privacy_slots_label.text() == "Private/fuzzed slots: 1 used of 20 (19 available)"
    assert cloud_card.cloud_upgrade_label.isVisible() is True
    assert "sporely.no" in cloud_card.cloud_upgrade_label.text()
    login_x = cloud_card.cloud_login_button.mapTo(cloud_card, QPoint(0, 0)).x()
    status_x = cloud_card.cloud_status_label.mapTo(cloud_card, QPoint(0, 0)).x()
    selector_x = cloud_card.cloud_sharing_selector.mapTo(cloud_card, QPoint(0, 0)).x()
    assert abs(login_x - selector_x) <= 1
    assert abs(status_x - selector_x) <= 1

    label_texts = {label.text() for label in dialog.findChildren(main_window.QLabel)}
    assert "Default sharing" in label_texts
    assert "Default sharing:" not in label_texts
    assert "Cloud sign-in" in label_texts

    checkbox_texts = {checkbox.text() for checkbox in dialog.findChildren(QCheckBox)}
    assert "Upload desktop images to cloud" not in checkbox_texts
    assert "Download cloud images to this device" not in checkbox_texts

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_sign_in_buttons_follow_cached_login_state(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, patch_controls=False)
    cloud_card = dialog._artsobs_dialog

    cloud_card._update_cloud_controls()

    assert cloud_card.cloud_status_label.text() == "Signed in as: sigmund.as@gmail.com"
    assert cloud_card.cloud_login_button.isEnabled() is False
    assert cloud_card.cloud_logout_button.isEnabled() is True

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_uploader_login_buttons_follow_selected_target_state(monkeypatch, qapp):
    uploaders = [SimpleNamespace(key="web", label="Artsobservasjoner")]
    parent, dialog = _build_settings_hub_dialog(
        monkeypatch,
        qapp,
        uploaders=uploaders,
        patch_controls=False,
    )
    artsobs_dialog = dialog._artsobs_dialog

    artsobs_dialog._target_status = {"web": True}
    artsobs_dialog._update_controls()

    assert artsobs_dialog.login_button.isEnabled() is False
    assert artsobs_dialog.logout_button.isEnabled() is True

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_hide_privacy_slot_count_for_pro(monkeypatch, qapp):
    pro_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "pro", "is_pro": True},
        count_remote_privacy_slots=lambda: 1,
        list_remote_observations=lambda: [
            {"visibility": "private", "location_precision": "fuzzed"},
            {"visibility": "friends", "location_precision": "exact"},
        ],
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=pro_client)
    cloud_card = dialog._artsobs_dialog

    assert cloud_card.cloud_plan_label.text() == "Plan: Pro"
    assert cloud_card.cloud_privacy_slots_label.isVisible() is False
    assert cloud_card.cloud_privacy_slots_label.text() == ""
    assert cloud_card.cloud_upgrade_label.isVisible() is False

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_show_plan_even_when_privacy_count_fails(monkeypatch, qapp):
    failing_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        fetch_cloud_plan_profile=lambda: {
            "cloud_plan": "pro",
            "is_pro": True,
            "storage_quota_bytes": None,
            "full_res_storage_enabled": False,
        },
        count_remote_privacy_slots=lambda: (_ for _ in ()).throw(
            cloud_sync.CloudSyncError('GET observations?user_id=eq.user-123 status=503: {"message":"Service Unavailable"}')
        ),
        list_remote_observations=lambda: [],
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=failing_client)
    cloud_card = dialog._artsobs_dialog

    assert cloud_card.cloud_plan_label.text() == "Plan: Pro"
    assert cloud_card.cloud_plan_label.toolTip() == ""
    assert cloud_card.cloud_privacy_slots_label.isVisible() is False
    assert cloud_card.cloud_privacy_slots_label.text() == ""

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_show_private_slot_error_for_free_plan(monkeypatch, qapp):
    failing_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        fetch_cloud_plan_profile=lambda: {
            "cloud_plan": "free",
            "is_pro": False,
            "storage_quota_bytes": 20_000_000,
            "full_res_storage_enabled": False,
        },
        count_remote_privacy_slots=lambda: (_ for _ in ()).throw(
            cloud_sync.CloudSyncError('GET observations?user_id=eq.user-123 status=503: {"message":"Service Unavailable"}')
        ),
        list_remote_observations=lambda: [],
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=failing_client)
    cloud_card = dialog._artsobs_dialog

    assert cloud_card.cloud_plan_label.text() == "Plan: Free"
    assert cloud_card.cloud_plan_label.toolTip() == ""
    assert cloud_card.cloud_privacy_slots_label.isVisible() is True
    assert cloud_card.cloud_privacy_slots_label.text() == "Private/fuzzed slots: unavailable"
    assert "Service Unavailable" in cloud_card.cloud_privacy_slots_label.toolTip()
    assert "status=503" in cloud_card.cloud_privacy_slots_label.toolTip()
    assert cloud_card.cloud_upgrade_label.isVisible() is True

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_avatar_url_triggers_avatar_fetch(monkeypatch, qapp):
    avatar_calls: list[str] = []

    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "https://example.com/avatar.jpg",
        },
    )

    monkeypatch.setattr(
        main_window.SettingsHubDialog,
        "_fetch_and_set_profile_avatar",
        lambda self, avatar_url: avatar_calls.append(str(avatar_url)),
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=fake_client)

    assert avatar_calls == ["https://example.com/avatar.jpg"]

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_show_last_status_and_details(monkeypatch, qapp):
    app_settings = {
        "cloud_last_sync_at": "2026-06-03T08:30:00+00:00",
        "cloud_last_sync_summary": "1 observation pushed.\nOriginal uploads: 2 uploaded, 1 skipped, 1 failed.",
        "cloud_last_sync_error_count": 2,
        "cloud_last_sync_errors_json": json.dumps(["conflict A", "conflict B"]),
    }
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, app_settings=app_settings)

    assert dialog.cloud_sync_status_label.text() == "Last sync: 2026-06-03 08:30 UTC"
    assert dialog.cloud_sync_error_label.text() == "Errors: 2"
    assert dialog.cloud_sync_log_button.text() == "Sync log"
    assert dialog.cloud_sync_log_button.isVisible() is True
    assert dialog.cloud_sync_log_button.isEnabled() is True

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_sync_log_button_shows_summary_and_errors(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(
        monkeypatch,
        qapp,
        app_settings={
            "cloud_last_sync_at": "2026-06-03T08:30:00+00:00",
            "cloud_last_sync_status": "ok",
            "cloud_last_sync_summary": (
                "Cloud sync complete.\n"
                "Observations: 5 checked; 5 re-dirtied due to pending local images; 5 skipped as no-op.\n"
                "Images: 32 checked; 24 uploaded."
            ),
            "cloud_last_sync_error_count": 2,
            "cloud_last_sync_errors_json": json.dumps(["conflict A", "conflict B"]),
        },
    )

    captured: dict[str, object] = {}

    def fake_build(lines):
        captured["lines"] = list(lines)
        return SimpleNamespace(exec=lambda: captured.setdefault("executed", True))

    monkeypatch.setattr(dialog, "_build_cloud_sync_details_dialog", fake_build)

    dialog.cloud_sync_log_button.click()

    assert captured["executed"] is True
    text = "\n".join(captured["lines"])
    assert "Status: ok" in text
    assert "Summary: Cloud sync complete." in text
    assert "Observations: 5 checked; 5 re-dirtied due to pending local images; 5 skipped as no-op." in text
    assert "Images: 32 checked; 24 uploaded." in text
    assert "Last sync: 2026-06-03 08:30 UTC" in text
    assert "Errors: 2" in text
    assert "Raw sync errors:" in text
    assert "- conflict A" in text
    assert "- conflict B" in text

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_sync_now_starts_full_sync(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)

    dialog.cloud_sync_now_button.click()
    qapp.processEvents()

    assert parent.start_calls == [
        {
            "show_status": True,
            "run_refresh_flow": False,
            "sync_images": True,
            "materialize_remote_images": True,
        }
    ]

    dialog.deleteLater()
    parent.deleteLater()


def test_cloud_login_failure_refreshes_parent_ui_without_crashing(monkeypatch):
    events: list[str] = []
    settings_payload: dict[str, object] = {}
    warning_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    main_window_fake = SimpleNamespace(
        _refresh_background_activity_badge=lambda: events.append("badge_refresh"),
    )
    settings_hub_fake = SimpleNamespace(
        refresh_cloud_sync_status=lambda: events.append("settings_refresh"),
        parent=lambda: main_window_fake,
    )
    fake_dialog = SimpleNamespace(
        tr=lambda text: text,
        parent=lambda: settings_hub_fake,
        observations_tab=SimpleNamespace(
            _reset_status_progress=lambda: events.append("reset_progress"),
            _set_status_progress_visible=lambda visible: events.append(f"progress_visible:{bool(visible)}"),
            set_status_message=lambda message, **kwargs: events.append(f"status:{message}:{kwargs.get('level')}"),
            _refresh_cloud_sync_idle_hint=lambda: events.append("idle_hint"),
        ),
    )
    fake_dialog._refresh_cloud_sync_ui = MethodType(
        main_window.ArtsobservasjonerSettingsDialog._refresh_cloud_sync_ui,
        fake_dialog,
    )

    monkeypatch.setattr(
        main_window,
        "update_app_settings",
        lambda payload: settings_payload.update(payload),
    )
    monkeypatch.setattr(
        main_window.QMessageBox,
        "warning",
        lambda *args, **kwargs: warning_calls.append((args, kwargs)),
    )

    main_window.ArtsobservasjonerSettingsDialog._on_cloud_login_failure(fake_dialog, "Connection closed")

    assert settings_payload["cloud_last_sync_status"] == "error"
    assert settings_payload["cloud_last_sync_error_count"] == 1
    assert events[:4] == [
        "reset_progress",
        "progress_visible:False",
        "status:Cloud sync sign-in failed. Please check your email and password.:warning",
        "idle_hint",
    ]
    assert "settings_refresh" in events
    assert "badge_refresh" in events
    assert warning_calls


def test_cloud_profile_refresh_auth_failure_clears_invalid_session(monkeypatch):
    events: list[str] = []
    clear_calls: list[str] = []

    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: (_ for _ in ()).throw(_auth_refresh_failure()),
        fetch_profile=lambda: pytest.fail("profile fetch should not run after auth failure"),
    )
    fake_dialog = SimpleNamespace(
        tr=lambda text: text,
        _profile_email=QLineEdit("sigmund.as@gmail.com"),
        _profile_username=QLineEdit("sigmundas"),
        _profile_name=QLineEdit("Sigmundas"),
        _profile_bio=QPlainTextEdit("Bio"),
        _profile_avatar_url="",
        _cloud_profile_loaded_user_id="user-123",
        _cached_cloud_client=lambda: fake_client,
        _on_cloud_logout_changed=lambda: events.append("logout"),
    )
    fake_dialog._clear_invalid_cloud_session = MethodType(
        main_window.SettingsHubDialog._clear_invalid_cloud_session,
        fake_dialog,
    )

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "clear_session", lambda: clear_calls.append("clear_session"))
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args, **kwargs: pytest.fail("set_profile should not run"))
    monkeypatch.setattr(main_window, "get_app_settings", lambda: {"cloud_user_email": "sigmund.as@gmail.com"})

    main_window.SettingsHubDialog._refresh_cloud_profile_fields(fake_dialog, force=True)

    assert clear_calls == ["clear_session"]
    assert events == ["logout"]


def test_cloud_login_success_does_not_auto_start_sync(monkeypatch):
    events: list[str] = []
    settings_payload: dict[str, object] = {}
    set_setting_calls: list[tuple[str, object]] = []
    save_calls: list[dict[str, object]] = []
    sync_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    client = SimpleNamespace(
        save_credentials=lambda **kwargs: save_calls.append(dict(kwargs)),
    )

    main_window_fake = SimpleNamespace(
        observations_tab=SimpleNamespace(
            _start_cloud_sync=lambda *args, **kwargs: sync_calls.append((args, kwargs)),
        ),
    )
    settings_hub_fake = SimpleNamespace(
        parent=lambda: main_window_fake,
        _on_cloud_login_changed=lambda *args, **kwargs: events.append("login_changed"),
    )
    fake_dialog = SimpleNamespace(
        tr=lambda text: text,
        parent=lambda: settings_hub_fake,
        _cloud_login_password="secret",
        _cloud_login_remember=True,
        set_hint=lambda text, tone="info": events.append(f"hint:{tone}:{text}"),
        _refresh_cloud_sync_ui=lambda: events.append("refresh_ui"),
    )

    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda client: "user-123")
    monkeypatch.setattr(main_window, "update_app_settings", lambda payload: settings_payload.update(payload))
    monkeypatch.setattr(main_window.SettingsDB, "set_setting", lambda key, value: set_setting_calls.append((key, value)))

    main_window.ArtsobservasjonerSettingsDialog._on_cloud_login_success(fake_dialog, client, "sigmund.as@gmail.com")

    assert settings_payload["cloud_user_email"] == "sigmund.as@gmail.com"
    assert set_setting_calls == [("profile_email", "sigmund.as@gmail.com")]
    assert save_calls == [
        {
            "email": "sigmund.as@gmail.com",
            "password": "secret",
            "remember_password": True,
        }
    ]
    assert sync_calls == []
    assert "login_changed" in events
    assert "refresh_ui" in events


def test_profile_save_auth_failure_clears_invalid_session(monkeypatch):
    events: list[str] = []
    warning_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    settings_payload: dict[str, object] = {}
    profile_rows: list[tuple[tuple[object, ...], dict[str, object]]] = []
    clear_calls: list[str] = []

    fake_client = SimpleNamespace(
        user_id="user-123",
        update_profile=lambda **kwargs: (_ for _ in ()).throw(_auth_refresh_failure()),
    )
    fake_dialog = SimpleNamespace(
        tr=lambda text: text,
        _profile_username=QLineEdit("sigmundas"),
        _profile_name=QLineEdit("Sigmundas"),
        _profile_email=QLineEdit("sigmund.as@gmail.com"),
        _profile_bio=QPlainTextEdit("Bio"),
        _profile_avatar_url="",
        _profile_changed=False,
        _profile_email_for_save=lambda: "sigmund.as@gmail.com",
        _get_cloud_client=lambda: fake_client,
        _on_cloud_logout_changed=lambda: events.append("logout"),
        parent=lambda: None,
    )
    fake_dialog._profile_sync_error_message = MethodType(
        main_window.SettingsHubDialog._profile_sync_error_message,
        fake_dialog,
    )
    fake_dialog._clear_invalid_cloud_session = MethodType(
        main_window.SettingsHubDialog._clear_invalid_cloud_session,
        fake_dialog,
    )

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "clear_session", lambda: clear_calls.append("clear_session"))
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args, **kwargs: profile_rows.append((args, kwargs)))
    monkeypatch.setattr(main_window, "get_app_settings", lambda: {"cloud_user_email": "sigmund.as@gmail.com"})
    monkeypatch.setattr(main_window.QMessageBox, "warning", lambda *args, **kwargs: warning_calls.append((args, kwargs)))

    result = main_window.SettingsHubDialog._save_profile(fake_dialog)

    assert result is False
    assert clear_calls == ["clear_session"]
    assert events == ["logout"]
    assert profile_rows == [
        (
            ("Sigmundas", "sigmund.as@gmail.com", "Bio", "sigmundas", ""),
            {},
        )
    ]
    assert warning_calls
    assert "session expired" in str(warning_calls[0][0][2]).lower()


def test_raw_processing_preferences_page_exposes_advanced_controls(monkeypatch, qapp):
    saved_settings: list[tuple[str, object]] = []
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, set_setting_sink=saved_settings)

    dialog._nav.setCurrentRow(main_window.SettingsHubDialog.PAGE_RAW_PROCESSING)
    qapp.processEvents()

    assert dialog._nav.item(main_window.SettingsHubDialog.PAGE_RAW_PROCESSING).text() == "RAW processing"
    assert dialog._raw_dark_cutoff_spin.value() == pytest.approx(0.0)
    assert dialog._raw_dark_cutoff_slider.value() == 0
    assert dialog._raw_dark_cutoff_slider.maximum() == 200
    assert dialog._raw_bright_cutoff_spin.value() == pytest.approx(0.0)
    assert dialog._raw_bright_cutoff_slider.value() == 0
    assert dialog._raw_bright_cutoff_slider.maximum() == 200
    assert dialog._raw_companion_source_selector.selected_value() == main_window.RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW
    assert dialog._raw_capture_mode_selector.selected_value() == main_window.LiveLabTab.RAW_CAPTURE_MODE_REVIEW

    dialog._raw_dark_cutoff_slider.setValue(12)
    dialog._raw_bright_cutoff_slider.setValue(34)
    dialog._raw_companion_source_selector.set_selected_value(main_window.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG)
    dialog._raw_capture_mode_selector.set_selected_value(main_window.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE)
    dialog._save_raw_processing_preferences()
    qapp.processEvents()

    assert (main_window.SETTING_RAW_PROCESSING_DARK_CUTOFF, pytest.approx(0.12 / 100.0)) in saved_settings
    assert (main_window.SETTING_RAW_PROCESSING_BRIGHT_CUTOFF, pytest.approx(0.34 / 100.0)) in saved_settings
    assert (main_window.SETTING_RAW_COMPANION_SOURCE_PREFERENCE, main_window.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG) in saved_settings
    assert (main_window.LiveLabTab.SETTING_RAW_CAPTURE_MODE, main_window.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE) in saved_settings

    dialog.deleteLater()
    parent.deleteLater()
