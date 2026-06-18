from __future__ import annotations

import json
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QTableWidget, QTableWidgetItem

from ui import observations_tab
import ui.main_window as main_window
import utils.cloud_sync as cloud_sync


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_cloud_auto_sync_worker_disables_remote_media_materialization(monkeypatch):
    fake_client = SimpleNamespace(user_id="user-123")
    sync_kwargs: dict = {}

    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(
        observations_tab,
        "sync_all",
        lambda *args, **kwargs: sync_kwargs.update(kwargs) or {"pushed": 0, "pulled": 0, "errors": []},
    )

    worker = observations_tab._CloudAutoSyncWorker(prepare_images_cb=None)
    worker.run()

    assert sync_kwargs["sync_images"] is True
    assert sync_kwargs["materialize_remote_images"] is False


def test_cloud_auto_sync_worker_metadata_only_skips_image_preparation(monkeypatch):
    fake_client = SimpleNamespace(user_id="user-123")
    sync_kwargs: dict = {}

    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(
        observations_tab,
        "sync_all",
        lambda *args, **kwargs: sync_kwargs.update(kwargs) or {"pushed": 0, "pulled": 0, "errors": []},
    )

    worker = observations_tab._CloudAutoSyncWorker(
        prepare_images_cb=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("image prep should not run")),
        sync_images=False,
    )
    worker.run()

    assert sync_kwargs["sync_images"] is False
    assert sync_kwargs["prepare_images_cb"] is None
    assert sync_kwargs["materialize_remote_images"] is False


def test_refresh_clicked_materializes_remote_images(monkeypatch):
    calls: dict[str, object] = {}

    fake_tab = SimpleNamespace(
        _invalidate_publish_login_status_cache=lambda: calls.setdefault("invalidate", True),
        _start_cloud_sync=lambda **kwargs: calls.setdefault("start", kwargs) or True,
        refresh_observations=lambda show_status=False: calls.setdefault("refresh", bool(show_status)),
        _finish_manual_refresh_flow=lambda: calls.setdefault("finish", True),
    )

    observations_tab.ObservationsTab._on_refresh_clicked(fake_tab)

    assert calls["invalidate"] is True
    assert calls["start"] == {
        "show_status": True,
        "run_refresh_flow": True,
        "materialize_remote_images": True,
    }
    assert "refresh" not in calls
    assert "finish" not in calls


def test_cloud_auto_sync_worker_reports_auth_failure_when_saved_credentials_are_invalid(monkeypatch, qapp):
    errors: list[str] = []
    results: list[dict] = []

    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: None)
    monkeypatch.setattr(observations_tab, "get_app_settings", lambda: {"cloud_access_token": "expired-token"})
    monkeypatch.setattr(observations_tab, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(
        observations_tab,
        "sync_all",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sync_all should not run")),
    )

    worker = observations_tab._CloudAutoSyncWorker()
    worker.error.connect(errors.append)
    worker.sync_finished.connect(results.append)

    worker.run()

    assert errors == ["Cloud sync sign-in failed. Please check your email and password."]
    assert results == []


def test_cloud_auto_sync_worker_skips_without_saved_credentials(monkeypatch, qapp):
    errors: list[str] = []
    results: list[dict] = []

    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: None)
    monkeypatch.setattr(observations_tab, "get_app_settings", lambda: {})
    monkeypatch.setattr(observations_tab, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(
        observations_tab,
        "sync_all",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sync_all should not run")),
    )

    worker = observations_tab._CloudAutoSyncWorker()
    worker.error.connect(errors.append)
    worker.sync_finished.connect(results.append)

    worker.run()

    assert errors == []
    assert results == [{"pushed": 0, "pulled": 0, "errors": [], "skipped": True}]


def test_metadata_sync_timeout_is_disabled(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeTimer:
        def __init__(self):
            self.starts: list[int] = []
            self.stops = 0

        def start(self, interval_ms):
            self.starts.append(int(interval_ms))

        def stop(self):
            self.stops += 1

    fake_tab = SimpleNamespace(
        _metadata_sync_timer=_FakeTimer(),
        _start_cloud_sync=lambda **kwargs: calls.setdefault("start", kwargs) or True,
    )

    observations_tab.ObservationsTab.schedule_metadata_cloud_sync(fake_tab, 17)
    observations_tab.ObservationsTab._on_metadata_sync_timeout(fake_tab)

    assert calls == {}
    assert fake_tab._metadata_sync_timer.starts == []
    assert fake_tab._metadata_sync_timer.stops == 0


def test_metadata_sync_timeout_defers_while_measurement_active():
    class _FakeTimer:
        def __init__(self):
            self.starts: list[int] = []

        def start(self, interval_ms):
            self.starts.append(int(interval_ms))

        def stop(self):
            self.starts.append(-1)

    fake_timer = _FakeTimer()
    fake_tab = SimpleNamespace(
        _metadata_sync_timer=fake_timer,
        _metadata_sync_delay_ms=8000,
        _metadata_sync_should_pause=lambda: True,
        _cloud_sync_pending_ids=lambda: [17],
        _start_cloud_sync=lambda **kwargs: (_ for _ in ()).throw(AssertionError("sync should have been deferred")),
    )

    observations_tab.ObservationsTab._on_metadata_sync_timeout(fake_tab)

    assert fake_timer.starts == []


def test_cloud_sync_finished_skipped_hides_progress_widget():
    calls: dict[str, object] = {}

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        refresh_observations=lambda show_status=False: calls.setdefault("refresh", bool(show_status)),
        _cloud_sync_run_refresh_flow=False,
        _cloud_sync_show_status=True,
        _set_status_progress_visible=lambda visible: calls.setdefault("visible", []).append(bool(visible)),
        _reset_status_progress=lambda: calls.setdefault("reset", True),
        _set_status_progress=lambda *args, **kwargs: calls.setdefault("progress", (args, kwargs)),
        _finish_manual_refresh_flow=lambda: calls.setdefault("finish", True),
        _record_cloud_sync_status=lambda *args, **kwargs: calls.setdefault("record", (args, kwargs)),
        _show_cloud_conflict_dialog=lambda *args, **kwargs: calls.setdefault("conflicts", True),
        _prompt_for_deleted_cloud_observations=lambda *args, **kwargs: calls.setdefault("deleted", True),
    )

    observations_tab.ObservationsTab._on_cloud_sync_finished(
        fake_tab,
        {"pushed": 0, "pulled": 0, "errors": [], "skipped": True},
    )

    assert calls["visible"] == [False]
    assert calls["reset"] is True
    assert "record" not in calls
    assert "progress" not in calls


def test_cloud_media_detail_view_auto_launches_lazy_materialization(monkeypatch):
    launched: list[bool] = []

    dialog = SimpleNamespace(
        _cloud_media_auto_start_attempted=False,
        _set_cloud_media_status=lambda *args, **kwargs: None,
        _start_cloud_media_materialization=lambda *, auto_start: launched.append(bool(auto_start)),
        tr=lambda text: text,
        download_cloud_media_btn=SimpleNamespace(
            setVisible=lambda *args, **kwargs: None,
            setEnabled=lambda *args, **kwargs: None,
            setText=lambda *args, **kwargs: None,
        ),
    )
    dialog._cloud_media_materialization_state = lambda: {
        "status": "needs_materialization",
        "needs_materialization": True,
        "can_auto_start": True,
        "worker_running": False,
    }

    observations_tab.ObservationDetailsDialog._refresh_cloud_media_materialization_controls(dialog, show_prompt=False)

    assert launched == [True]


def test_cloud_sync_plan_limit_error_opens_detail_dialog(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeDetailsDialog:
        def exec(self) -> int:
            calls["dialog_exec"] = True
            return 0

    def _fake_builder(details: str):
        calls["details"] = details
        return _FakeDetailsDialog()

    tab = SimpleNamespace(
        tr=lambda text: text,
        refresh_observations=lambda show_status=False: None,
        _cloud_sync_run_refresh_flow=False,
        _finish_manual_refresh_flow=lambda: calls.setdefault("finished", True),
        _record_cloud_sync_status=lambda *args, **kwargs: calls.setdefault("recorded", (args, kwargs)),
        _cloud_sync_show_status=True,
        _set_status_progress_visible=lambda *args, **kwargs: None,
        _set_status_progress=lambda *args, **kwargs: None,
        set_status_message=lambda *args, **kwargs: calls.setdefault("status_message", (args, kwargs)),
    )
    tab._summarize_sync_error = lambda message: observations_tab.ObservationsTab._summarize_sync_error(tab, message)
    tab._build_cloud_sync_error_details_dialog = _fake_builder

    message = (
        "Image is too large for your plan. Make it smaller or upgrade to Pro.\n\n"
        "Observation: Agaricus campestris (ID 17)\n"
        "Image: source.jpg (field) (ID 42)\n"
        "Original size: 1.2 MB\n"
        "Prepared upload size: 1.1 MB"
    )

    observations_tab.ObservationsTab._on_cloud_sync_error(tab, message)

    assert calls["dialog_exec"] is True
    assert "Observation: Agaricus campestris (ID 17)" in str(calls["details"])
    status_args, status_kwargs = calls["status_message"]
    assert "Cloud sync failed while uploading an image that was rejected by the worker." in status_args[0]
    assert "upgrade to Pro" not in str(calls["details"])
    assert status_kwargs["level"] == "warning"


def test_cloud_sync_error_for_invalid_login_clears_progress_and_reports_sign_in_failure():
    calls: dict[str, object] = {}

    tab = SimpleNamespace(
        tr=lambda text: text,
        refresh_observations=lambda show_status=False: calls.setdefault("refresh", bool(show_status)),
        _cloud_sync_run_refresh_flow=False,
        _cloud_sync_show_status=True,
        _set_status_progress_visible=lambda visible: calls.setdefault("visible", []).append(bool(visible)),
        _set_status_progress=lambda *args, **kwargs: calls.setdefault("progress", (args, kwargs)),
        _finish_manual_refresh_flow=lambda: calls.setdefault("finish", True),
        _record_cloud_sync_status=lambda *args, **kwargs: calls.setdefault("record", (args, kwargs)),
        _show_cloud_conflict_dialog=lambda *args, **kwargs: calls.setdefault("conflicts", True),
        _prompt_for_deleted_cloud_observations=lambda *args, **kwargs: calls.setdefault("deleted", True),
        set_status_message=lambda *args, **kwargs: calls.setdefault("status_message", (args, kwargs)),
    )
    tab._summarize_sync_error = lambda message: observations_tab.ObservationsTab._summarize_sync_error(tab, message)

    observations_tab.ObservationsTab._on_cloud_sync_error(tab, "invalid_grant")

    assert calls["visible"] == [False]
    assert calls["progress"] == (("", 0, 1), {})
    record_args, record_kwargs = calls["record"]
    assert record_args == ("Cloud sync sign-in failed. Please check your email and password.",)
    assert record_kwargs["errors"] == ["invalid_grant"]
    assert record_kwargs["status"] == "error"
    status_args, status_kwargs = calls["status_message"]
    assert status_args == ("Cloud sync sign-in failed. Please check your email and password.",)
    assert status_kwargs["level"] == "warning"


def test_cloud_sync_error_for_jwt_expired_reports_sign_in_failure():
    tab = SimpleNamespace(tr=lambda text: text)

    summary = observations_tab.ObservationsTab._summarize_sync_error(
        tab,
        'GET observation_images?...: {"code":"PGRST303","message":"JWT expired"}',
    )

    assert summary == "Cloud sync sign-in failed. Please check your email and password."


def test_cloud_sync_helpers_include_microscope_media_and_skip_recovery_cache():
    assert cloud_sync.should_pull_cloud_image_to_desktop({"image_type": "microscope"}) is True
    assert cloud_sync.should_pull_cloud_image_to_desktop({"image_type": "field"}) is True
    assert cloud_sync.should_push_local_image_to_cloud(
        {
            "image_type": "microscope",
            "source_role": "local_canonical",
            "file_purpose": "microscope",
        }
    ) is True
    assert cloud_sync.should_push_local_image_to_cloud(
        {
            "image_type": "microscope",
            "source_role": "cloud_recovery_cache",
            "file_purpose": "cache",
        }
    ) is False


def test_prepare_cloud_sync_image_uploads_uses_cloud_image_selection_not_publish_exclusions(tmp_path):
    field_path = tmp_path / "field.jpg"
    microscope_path = tmp_path / "microscope.jpg"
    field_path.write_bytes(b"field")
    microscope_path.write_bytes(b"microscope")

    field_row = {
        "id": 1,
        "filepath": str(field_path),
        "original_filepath": None,
        "image_type": "field",
        "source_role": "local_canonical",
        "file_purpose": "field",
    }
    microscope_row = {
        "id": 2,
        "filepath": str(microscope_path),
        "original_filepath": None,
        "image_type": "microscope",
        "source_role": "local_canonical",
        "file_purpose": "microscope",
    }

    def _fake_prepare_clean_cloud_image_file(*, source_path, temp_dir, image_id, upload_policy):
        out_path = temp_dir / f"cloud_{image_id}.webp"
        out_path.write_bytes(b"webp")
        return out_path, 100, 100, 100, 100, "image/webp", 80

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        _is_main_gui_thread=lambda: True,
        _cloud_sync_upload_policy=lambda: {"uploadMode": "full"},
        _collect_cloud_sync_image_rows=lambda observation_id: [field_row, microscope_row],
        _collect_publish_selected_image_rows=lambda observation_id: (_ for _ in ()).throw(
            AssertionError("publish selection should not drive cloud sync")
        ),
        _publish_excluded_image_ids=lambda observation_id: {2},
        _publish_path_key=observations_tab.ObservationsTab._publish_path_key,
        _prepare_clean_cloud_image_file=_fake_prepare_clean_cloud_image_file,
        _cleanup_publish_temp_dir=lambda temp_dir: None,
        _yield_background_sync_ui=lambda: None,
    )

    prepared, cleanup, warnings = observations_tab.ObservationsTab.prepare_cloud_sync_image_uploads(
        fake_tab,
        {"id": 631},
    )

    assert [item["image_row"]["id"] for item in prepared] == [1, 2]
    assert warnings == []
    assert cleanup is not None
    cleanup()


def test_cloud_sync_webp_support_error_uses_clear_summary():
    tab = SimpleNamespace(tr=lambda text: text)

    summary = observations_tab.ObservationsTab._summarize_sync_error(
        tab,
        observations_tab.WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE,
    )

    assert "WebP support is required" in summary


class _DummyHintController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def set_hint(self, text: str, tone: str = "info") -> None:
        self.calls.append((str(text), str(tone)))


def test_cloud_sync_idle_hint_lists_observation_ids(monkeypatch):
    hint_controller = _DummyHintController()
    fake_window = SimpleNamespace(
        _cloud_client=SimpleNamespace(user_id="user-123"),
        _cloud_sync_pending_observation_ids=lambda: [390, 389, 385],
        _cloud_sync_blocked_observation_ids=lambda: [401],
        _format_cloud_sync_observation_ids=main_window.MainWindow._format_cloud_sync_observation_ids,
    )
    tab = SimpleNamespace(
        tr=lambda text: text,
        _status_hint_controller=hint_controller,
        window=lambda: fake_window,
    )

    observations_tab.ObservationsTab._refresh_cloud_sync_idle_hint(tab)

    assert hint_controller.calls
    message, tone = hint_controller.calls[-1]
    assert tone == "warning"
    assert "Cloud sync blocked for observation ID 401." in message
    assert "Cloud sync pending for observation IDs 390, 389, 385." in message
    assert "Open Sync now to review the error details, then click Sync now to retry uploads." in message


def test_cloud_sync_idle_hint_prefers_error_over_pending(monkeypatch):
    hint_controller = _DummyHintController()
    fake_window = SimpleNamespace(
        _cloud_client=None,
        _cloud_sync_pending_observation_ids=lambda: [390, 389, 385],
        _cloud_sync_blocked_observation_ids=lambda: [],
        _format_cloud_sync_observation_ids=main_window.MainWindow._format_cloud_sync_observation_ids,
    )
    monkeypatch.setattr(
        observations_tab,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "error",
            "cloud_last_sync_summary": "Cloud sync sign-in failed. Please check your email and password.",
            "cloud_last_sync_errors_json": json.dumps([
                "Cloud sync sign-in failed. Please check your email and password.",
            ]),
        },
    )
    tab = SimpleNamespace(
        tr=lambda text: text,
        _status_hint_controller=hint_controller,
        window=lambda: fake_window,
    )

    observations_tab.ObservationsTab._refresh_cloud_sync_idle_hint(tab)

    assert hint_controller.calls
    message, tone = hint_controller.calls[-1]
    assert tone == "warning"
    assert "Cloud sync sign-in failed. Please check your email and password." in message
    assert "Sign in again, then click Sync now to retry uploads." in message
    assert "Cloud sync pending" not in message


def test_cloud_sync_idle_hint_uses_logged_in_copy_when_client_is_restored(monkeypatch):
    hint_controller = _DummyHintController()
    fake_client = SimpleNamespace(user_id="user-123")
    fake_window = SimpleNamespace(
        _cloud_client=None,
        _cached_cloud_client=lambda: fake_client,
        _cloud_sync_pending_observation_ids=lambda: [390, 389, 385],
        _cloud_sync_blocked_observation_ids=lambda: [],
        _format_cloud_sync_observation_ids=main_window.MainWindow._format_cloud_sync_observation_ids,
    )
    monkeypatch.setattr(
        observations_tab,
        "get_app_settings",
        lambda: {
            "cloud_last_sync_status": "error",
            "cloud_last_sync_summary": "Cloud sync sign-in failed. Please check your email and password.",
            "cloud_last_sync_errors_json": json.dumps([
                "Cloud sync sign-in failed. Please check your email and password.",
            ]),
        },
    )
    tab = SimpleNamespace(
        tr=lambda text: text,
        _status_hint_controller=hint_controller,
        window=lambda: fake_window,
    )

    observations_tab.ObservationsTab._refresh_cloud_sync_idle_hint(tab)

    assert hint_controller.calls
    message, tone = hint_controller.calls[-1]
    assert tone == "warning"
    assert "Logged in, click Sync now to sync." in message
    assert "Sign in again, then click Sync now to retry uploads." not in message


def test_observation_status_info_maps_draft_private_friends_public():
    assert observations_tab._observation_status_info({"is_draft": True, "sharing_scope": "public"}) == (
        "Draft",
        "draft",
        1,
    )
    assert observations_tab._observation_status_info({"is_draft": False, "sharing_scope": "private"}) == (
        "Private",
        "private",
        2,
    )
    assert observations_tab._observation_status_info({"is_draft": False, "visibility": "friends"}) == (
        "Friends",
        "friends",
        3,
    )
    assert observations_tab._observation_status_info({"is_draft": False, "sharing_scope": "public"}) == (
        "Public",
        "public",
        4,
    )
    assert observations_tab._observation_status_info({"is_draft": False}) == ("", "", 0)
    assert observations_tab._observation_status_info({}) == ("", "", 0)


def test_render_observations_table_places_status_before_map_and_publish(qapp):
    table = QTableWidget()
    table.setColumnCount(10)

    fake_tab = SimpleNamespace(
        table=table,
        selected_observation_id=None,
        tr=lambda text: text,
        _show_new_imports_only=lambda: False,
        _show_observation_table_thumbnails=lambda: False,
        _observation_table_thumbnail_size=lambda: 48,
        _update_observations_table_geometry=lambda: None,
        _observation_thumb_icon_cache={},
        _status_hint_controller=SimpleNamespace(register_widget=lambda *args, **kwargs: None),
        rename_btn=SimpleNamespace(setEnabled=lambda *args, **kwargs: None),
        delete_btn=SimpleNamespace(setEnabled=lambda *args, **kwargs: None),
        export_btn=SimpleNamespace(setEnabled=lambda *args, **kwargs: None),
        gallery_widget=SimpleNamespace(clear=lambda: None),
        _update_publish_controls=lambda: None,
        set_status_message=lambda *args, **kwargs: None,
        show_map_service_dialog=lambda *args, **kwargs: None,
        _render_publish_cell=lambda row, observation_id, publish_target, arts_id, artportalen_id, inaturalist_id: table.setItem(
            row,
            9,
            QTableWidgetItem("Publish"),
        ),
    )

    row_cache = [
        {
            "row_kind": "local",
            "local_id": 389,
            "id_display": "389",
            "thumbnail_path": None,
            "genus": "Agaricus",
            "species": "campestris",
            "common_name": "Field mushroom",
            "spore_short": "-",
            "date": "2026-06-15",
            "location": "Meadow",
            "status_text": "Draft",
            "status_kind": "draft",
            "status_sort": 1,
            "lat": 59.0,
            "lon": 10.0,
            "has_coords": True,
            "species_name": "Agaricus campestris",
            "arts_id": None,
            "artportalen_id": None,
            "inaturalist_id": None,
            "publish_target": "artsobs_no",
            "mark_star": False,
            "search_text": "field mushroom agaricus campestris draft meadow",
        }
    ]

    observations_tab.ObservationsTab._render_observations_table(
        fake_tab,
        row_cache,
        query="",
        restore_selection=False,
        show_status=False,
        status_message=None,
    )

    assert table.columnCount() == 10
    assert table.item(0, 7).text() == "Draft"
    assert table.item(0, 7).data(Qt.UserRole) == 1
    assert table.item(0, 7).data(Qt.UserRole + 2) == "draft"
    assert table.cellWidget(0, 8) is not None
    assert table.item(0, 8).text() == ""
    assert table.item(0, 9).text() == "Publish"
