from __future__ import annotations

import json
import os
from types import MethodType, SimpleNamespace

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


def test_cloud_workers_have_non_empty_object_names(qapp):
    """Every cloud/background worker must carry an objectName so a
    'QThread: Destroyed while thread "" is still running' warning can be traced
    to a specific worker instead of an anonymous thread.
    """
    from ui.cloud_conflict_dialog import ConflictResolutionWorker

    workers = [
        observations_tab._CloudAutoSyncWorker(prepare_images_cb=None),
        observations_tab._CloudMediaMaterializationWorker(1),
        observations_tab._ThumbnailLoaderWorker([]),
        observations_tab.LocationLookupWorker(0.0, 0.0),
        observations_tab.ArtsobsMobileLinkCheckWorker([]),
        main_window._CloudLoginWorker("a@example.com", "pw"),
        ConflictResolutionWorker([]),
    ]
    try:
        for worker in workers:
            assert worker.objectName(), f"{type(worker).__name__} has an empty objectName"
    finally:
        for worker in workers:
            worker.deleteLater()


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


def test_cloud_auto_sync_worker_cancels_during_sync(monkeypatch, qapp):
    fake_client = SimpleNamespace(user_id="user-123")
    sync_kwargs: dict[str, object] = {}
    results: list[dict] = []
    errors: list[str] = []

    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(observations_tab._CloudAutoSyncWorker, "isInterruptionRequested", lambda self: True)

    def _fake_sync_all(*args, **kwargs):
        sync_kwargs.update(kwargs)
        with pytest.raises(KeyboardInterrupt):
            kwargs["progress_cb"]("Observation 17 (Agaricus campestris): Uploading cloud image 1/1...", 0, 1)
        return {"pushed": 1, "pulled": 0, "errors": []}

    monkeypatch.setattr(observations_tab, "sync_all", _fake_sync_all)

    worker = observations_tab._CloudAutoSyncWorker()
    worker.sync_finished.connect(results.append)
    worker.error.connect(errors.append)
    worker.requestInterruption()
    worker.run()

    assert sync_kwargs["sync_images"] is True
    assert errors == []
    assert results == [{"pushed": 0, "pulled": 0, "errors": [], "skipped": True, "cancelled": True}]


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
        _set_status_progress_cancel_visible=lambda visible: calls.setdefault("cancel_visible", []).append(bool(visible)),
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


def test_cloud_sync_finished_cancelled_hides_progress_widget():
    calls: dict[str, object] = {}

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        refresh_observations=lambda show_status=False: calls.setdefault("refresh", []).append(bool(show_status)),
        _cloud_sync_run_refresh_flow=True,
        _cloud_sync_show_status=True,
        _set_status_progress_visible=lambda visible: calls.setdefault("visible", []).append(bool(visible)),
        _set_status_progress_cancel_visible=lambda visible: calls.setdefault("cancel_visible", []).append(bool(visible)),
        _reset_status_progress=lambda: calls.setdefault("reset", True),
        _set_status_progress=lambda *args, **kwargs: calls.setdefault("progress", (args, kwargs)),
        _finish_manual_refresh_flow=lambda: calls.setdefault("finish", True),
        _record_cloud_sync_status=lambda *args, **kwargs: calls.setdefault("record", (args, kwargs)),
        _show_cloud_conflict_dialog=lambda *args, **kwargs: calls.setdefault("conflicts", True),
        _prompt_for_deleted_cloud_observations=lambda *args, **kwargs: calls.setdefault("deleted", True),
        set_status_message=lambda *args, **kwargs: calls.setdefault("status_message", (args, kwargs)),
    )

    observations_tab.ObservationsTab._on_cloud_sync_finished(
        fake_tab,
        {"pushed": 0, "pulled": 0, "errors": [], "skipped": True, "cancelled": True},
    )

    assert calls["refresh"] == [False]
    assert calls["visible"] == [False]
    assert calls["cancel_visible"] == [False]
    assert calls["reset"] is True
    assert calls["status_message"] == (("Cloud sync cancelled.",), {"level": "info", "auto_clear_ms": 8000})
    assert "record" not in calls
    assert "progress" not in calls
    assert "finish" not in calls


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


def test_cloud_media_materialization_progress_keeps_prefixed_message():
    calls: list[tuple[str, str, int]] = []

    dialog = SimpleNamespace(
        observation={"id": 17, "genus": "Agaricus", "species": "campestris"},
        tr=lambda text: text,
        _set_cloud_media_status=lambda message, level="info", auto_clear_ms=8000: calls.append(
            (str(message), str(level), int(auto_clear_ms))
        ),
    )

    observations_tab.ObservationDetailsDialog._on_cloud_media_materialization_progress(
        dialog,
        "Observation 17 (Agaricus campestris): Importing cloud image 1/2…",
        0,
        2,
    )

    assert calls == [
        ("Observation 17 (Agaricus campestris): Importing cloud image 1/2…", "info", 8000)
    ]


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
        _set_status_progress_cancel_visible=lambda *args, **kwargs: None,
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
        _set_status_progress_cancel_visible=lambda visible: calls.setdefault("cancel_visible", []).append(bool(visible)),
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


def test_cloud_sync_helpers_include_microscope_media_and_allow_recovery_cache():
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
    ) is True


def test_prepare_cloud_sync_image_uploads_uses_publish_checkbox_selection(tmp_path, monkeypatch):
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

    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [field_row, microscope_row],
    )

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        _is_main_gui_thread=lambda: True,
        _cloud_sync_upload_policy=lambda: {"uploadMode": "full"},
        _publish_excluded_image_ids=lambda observation_id: {2},
        _publish_path_key=observations_tab.ObservationsTab._publish_path_key,
        _prepare_clean_cloud_image_file=_fake_prepare_clean_cloud_image_file,
        _cleanup_publish_temp_dir=lambda temp_dir: None,
        _yield_background_sync_ui=lambda: None,
    )
    fake_tab._collect_cloud_sync_image_rows = lambda observation_id, _tab=fake_tab: observations_tab.ObservationsTab._collect_cloud_sync_image_rows(
        _tab,
        observation_id,
    )

    prepared, cleanup, warnings = observations_tab.ObservationsTab.prepare_cloud_sync_image_uploads(
        fake_tab,
        {"id": 631},
    )

    assert [item["image_row"]["id"] for item in prepared] == [1]
    assert warnings == []
    assert cleanup is not None
    cleanup()


def test_prepare_cloud_sync_image_uploads_prefixes_progress_messages(tmp_path, monkeypatch):
    field_path = tmp_path / "field.jpg"
    field_path.write_bytes(b"field")

    field_row = {
        "id": 1,
        "filepath": str(field_path),
        "original_filepath": None,
        "image_type": "field",
        "source_role": "local_canonical",
        "file_purpose": "field",
    }

    def _fake_prepare_clean_cloud_image_file(*, source_path, temp_dir, image_id, upload_policy):
        out_path = temp_dir / f"cloud_{image_id}.webp"
        out_path.write_bytes(b"webp")
        return out_path, 100, 100, 100, 100, "image/webp", 80

    progress_calls: list[tuple[str, int, int]] = []
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [field_row],
    )

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        _is_main_gui_thread=lambda: True,
        _cloud_sync_upload_policy=lambda: {"uploadMode": "full"},
        _publish_excluded_image_ids=lambda observation_id: set(),
        _publish_path_key=observations_tab.ObservationsTab._publish_path_key,
        _prepare_clean_cloud_image_file=_fake_prepare_clean_cloud_image_file,
        _cleanup_publish_temp_dir=lambda temp_dir: None,
        _yield_background_sync_ui=lambda: None,
    )
    fake_tab._collect_cloud_sync_image_rows = lambda observation_id, _tab=fake_tab: observations_tab.ObservationsTab._collect_cloud_sync_image_rows(
        _tab,
        observation_id,
    )

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        prepared, cleanup, warnings = observations_tab.ObservationsTab.prepare_cloud_sync_image_uploads(
            fake_tab,
            {"id": 631, "genus": "Agaricus", "species": "campestris"},
            progress_cb=lambda message, current, total: progress_calls.append((str(message), int(current), int(total))),
        )

    assert [item["image_row"]["id"] for item in prepared] == [1]
    assert progress_calls == [("Observation 631 (Agaricus campestris): Preparing upload 1/1...", 1, 1)]
    assert summary["images_prepared_local"] == 1
    assert warnings == []
    assert cleanup is not None
    cleanup()


def test_cloud_sync_finished_success_uses_neutral_completion_text():
    calls: dict[str, object] = {}

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        refresh_observations=lambda show_status=False: calls.setdefault("refresh", bool(show_status)),
        _cloud_sync_run_refresh_flow=False,
        _cloud_sync_show_status=True,
        _set_status_progress_visible=lambda visible: calls.setdefault("visible", []).append(bool(visible)),
        _set_status_progress_cancel_visible=lambda visible: calls.setdefault("cancel_visible", []).append(bool(visible)),
        _reset_status_progress=lambda: calls.setdefault("reset", True),
        _set_status_progress=lambda *args, **kwargs: calls.setdefault("progress", (args, kwargs)),
        _finish_manual_refresh_flow=lambda: calls.setdefault("finish", True),
        _record_cloud_sync_status=lambda *args, **kwargs: calls.setdefault("record", (args, kwargs)),
        _show_cloud_conflict_dialog=lambda *args, **kwargs: calls.setdefault("conflicts", True),
        _prompt_for_deleted_cloud_observations=lambda *args, **kwargs: calls.setdefault("deleted", True),
        set_status_message=lambda *args, **kwargs: calls.setdefault("status_message", (args, kwargs)),
    )

    observations_tab.ObservationsTab._on_cloud_sync_finished(
        fake_tab,
        {
            "pushed": 1,
            "pulled": 2,
            "errors": [],
            "sync_summary": {
                "observations_checked": 1,
                "observations_redirtied_pending_local_images": 1,
                "observations_patched": 1,
                "observations_skipped_noop": 1,
                "images_checked": 1,
                "images_prepared_local": 1,
                "images_uploaded": 1,
                "images_skipped_already_synced": 1,
                "images_deleted_remote": 1,
                "measurements_checked": 1,
                "measurements_patched": 1,
                "measurements_skipped_noop": 1,
                "storage_quota_delta_rpc_calls": 2,
            },
        },
    )

    status_args, status_kwargs = calls["status_message"]
    message = status_args[0]
    assert message.splitlines()[0] == "Cloud sync complete."
    assert "Sporely Cloud synced:" not in message
    assert "1 re-dirtied due to pending local images" in message
    assert "1 prepared for upload" in message
    assert "1 uploaded" in message
    assert status_kwargs["level"] == "success"


def test_gallery_publish_uncheck_queues_cloud_tombstone(monkeypatch):
    calls: dict[str, list] = {
        "queue": [],
        "clear": [],
        "cloud": [],
        "excluded": [],
        "dirty": [],
    }

    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {"id": 1, "cloud_id": "cloud-1"},
            {"id": 2, "cloud_id": "cloud-2"},
        ],
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "queue_image_tombstone_for_local_image",
        lambda image_id: calls["queue"].append(int(image_id)) or "cloud-1",
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "clear_image_tombstone_by_deleted_cloud_id",
        lambda cloud_id: calls["clear"].append(str(cloud_id)) or True,
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "clear_image_cloud_sync_state",
        lambda image_id: calls["cloud"].append(int(image_id)) or True,
    )
    monkeypatch.setattr(cloud_sync, "mark_observation_dirty", lambda obs_id: calls["dirty"].append(int(obs_id)))

    tab = SimpleNamespace(
        selected_observation_id=7,
        _publish_excluded_image_ids=lambda observation_id: set(),
        _set_publish_excluded_image_ids=lambda obs_id, excluded: calls["excluded"].append(
            (int(obs_id), tuple(sorted(int(v) for v in excluded)))
        ),
        window=lambda: None,
        parent=lambda: None,
    )

    observations_tab.ObservationsTab._on_gallery_publish_selection_changed(tab, {2})

    assert calls["queue"] == [1]
    assert calls["clear"] == []
    assert calls["cloud"] == []
    assert calls["excluded"] == [(7, (1,))]
    assert calls["dirty"] == [7]


def test_gallery_publish_recheck_clears_synced_tombstone(monkeypatch):
    calls: dict[str, list] = {
        "queue": [],
        "clear": [],
        "cloud": [],
        "excluded": [],
        "dirty": [],
    }

    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {"id": 1, "cloud_id": "cloud-1"},
            {"id": 2, "cloud_id": "cloud-2"},
        ],
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "queue_image_tombstone_for_local_image",
        lambda image_id: calls["queue"].append(int(image_id)) or "cloud-1",
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_image_tombstone_by_deleted_cloud_id",
        lambda cloud_id: {
            "deleted_cloud_id": str(cloud_id),
            "delete_synced_at": "2026-06-01T10:00:00+00:00",
        }
        if str(cloud_id) == "cloud-1"
        else None,
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "clear_image_tombstone_by_deleted_cloud_id",
        lambda cloud_id: calls["clear"].append(str(cloud_id)) or True,
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "clear_image_cloud_sync_state",
        lambda image_id: calls["cloud"].append(int(image_id)) or True,
    )
    monkeypatch.setattr(cloud_sync, "mark_observation_dirty", lambda obs_id: calls["dirty"].append(int(obs_id)))

    tab = SimpleNamespace(
        selected_observation_id=7,
        _publish_excluded_image_ids=lambda observation_id: {1},
        _set_publish_excluded_image_ids=lambda obs_id, excluded: calls["excluded"].append(
            (int(obs_id), tuple(sorted(int(v) for v in excluded)))
        ),
        window=lambda: None,
        parent=lambda: None,
    )

    observations_tab.ObservationsTab._on_gallery_publish_selection_changed(tab, {1, 2})

    assert calls["queue"] == []
    assert calls["clear"] == ["cloud-1"]
    assert calls["cloud"] == [1]
    assert calls["excluded"] == [(7, ())]
    assert calls["dirty"] == [7]


def test_default_publish_selection_leaves_all_microscope_images_unchecked():
    selected0, next0 = observations_tab._default_publish_selected_for_new_image("microscope", 0)
    selected1, next1 = observations_tab._default_publish_selected_for_new_image("microscope", next0)
    selected2, next2 = observations_tab._default_publish_selected_for_new_image("microscope", next1)
    selected_field, next_field = observations_tab._default_publish_selected_for_new_image("field", next2)

    assert selected0 is False
    assert selected1 is False
    assert selected2 is False
    assert selected_field is True


def test_refresh_image_gallery_summary_marks_microscope_items_unchecked_by_default(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(observations_tab.ImageGalleryWidget, "build_gallery_badges", lambda **kwargs: [])
    monkeypatch.setattr(observations_tab.MeasurementDB, "get_measurements_for_image", lambda image_id: [])
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_image",
        lambda image_id: {"id": image_id, "cloud_id": None},
    )

    fake_dialog = SimpleNamespace(
        image_results=[
            SimpleNamespace(
                image_id=1,
                filepath="/tmp/micro.jpg",
                image_type="microscope",
                exif_has_gps=False,
                needs_scale=False,
                objective=None,
                custom_scale=False,
                contrast=None,
                lab_metadata=None,
                ai_crop_box=None,
                ai_crop_source_size=None,
            ),
            SimpleNamespace(
                image_id=2,
                filepath="/tmp/field.jpg",
                image_type="field",
                exif_has_gps=False,
                needs_scale=False,
                objective=None,
                custom_scale=False,
                contrast=None,
                lab_metadata=None,
                ai_crop_box=None,
                ai_crop_source_size=None,
            ),
        ],
        _gps_source_index=None,
        objectives={},
        tr=lambda text: text,
        image_gallery=SimpleNamespace(set_items=lambda items: captured.setdefault("items", items)),
        _image_gallery_preview_path=lambda item: item.filepath,
    )

    observations_tab.ObservationDetailsDialog._refresh_image_gallery_summary(fake_dialog)

    items = captured["items"]
    assert items[0]["publish_selected_default"] is False
    assert items[1]["publish_selected_default"] is True


def test_pending_artsobs_upload_status_auto_clears(monkeypatch, tmp_path):
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image")

    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_pending_artsobs_web_uploads",
        lambda: [
            {
                "observation_id": 7,
                "artsdata_id": 9,
                "image_id": 11,
                "filepath": str(image_path),
                "original_filepath": None,
            }
        ],
    )

    class _FakeAuth:
        def get_valid_cookies(self, target="web"):
            return {"cookie": "ok"}

    class _FakeClient:
        def set_cookies_from_browser(self, cookies):
            self.cookies = cookies

        def upload_images_web(self, sighting_id: int, image_paths: list[str], progress_cb=None):
            assert sighting_id == 9
            assert image_paths == [str(image_path)]

    monkeypatch.setattr("utils.artsobservasjoner_auto_login.ArtsObservasjonerAuth", _FakeAuth)
    monkeypatch.setattr("utils.artsobservasjoner_submit.ArtsObservasjonerWebClient", _FakeClient)

    calls: list[tuple[str, str, int]] = []
    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: calls.append(
            (str(message), str(level), int(auto_clear_ms))
        ),
    )

    result = observations_tab.ObservationsTab._upload_pending_artsobs_web_images(fake_tab)

    assert result == "uploaded"
    assert any("Uploading..." in message and timeout == 8000 for message, _, timeout in calls)


def test_cloud_sync_webp_support_error_uses_clear_summary():
    tab = SimpleNamespace(tr=lambda text: text)

    summary = observations_tab.ObservationsTab._summarize_sync_error(
        tab,
        observations_tab.WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE,
    )

    assert "WebP support is required" in summary


class _DummyHintController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, int | None]] = []

    def set_hint(self, text: str, tone: str = "info") -> None:
        self.calls.append(("hint", str(text), str(tone), None))

    def set_status(self, text: str, timeout_ms: int = 4000, tone: str = "info") -> None:
        self.calls.append(("status", str(text), str(tone), int(timeout_ms)))


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
    kind, message, tone, timeout_ms = hint_controller.calls[-1]
    assert kind == "status"
    assert tone == "warning"
    assert timeout_ms and timeout_ms > 0
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
    kind, message, tone, timeout_ms = hint_controller.calls[-1]
    assert kind == "status"
    assert tone == "warning"
    assert timeout_ms and timeout_ms > 0
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
    kind, message, tone, timeout_ms = hint_controller.calls[-1]
    assert kind == "status"
    assert tone == "warning"
    assert timeout_ms and timeout_ms > 0
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


def test_observation_table_row_cache_formats_date_and_spore_count():
    fake_tab = SimpleNamespace(
        _build_common_name_map=lambda observations: {},
        _lookup_common_name=lambda obs, name_map: None,
        _build_observation_thumbnail_map=lambda observation_ids: {},
        _recent_cloud_import_ids=lambda: set(),
        _observation_publish_target=lambda obs: None,
        _build_species_name=lambda obs: f"{(obs.get('genus') or '').strip()} {(obs.get('species') or '').strip()}".strip() or None,
    )

    rows = observations_tab.ObservationsTab._build_observation_table_rows_cache(
        fake_tab,
        [
            {
                "id": 389,
                "genus": "Agaricus",
                "species": "campestris",
                "species_guess": "Agaricus campestris",
                "spore_statistics": "Spores: 12.0-15.0 x 4.0-5.0 um  n = 18",
                "date": "2026-06-16T10:57:17+00:00",
                "location": "Meadow",
            }
        ],
    )

    assert rows[0]["spore_short"] == "18"
    assert rows[0]["date"] == "2026-06-16 10:57"


def test_cloud_observation_table_row_cache_formats_date_and_spore_count():
    rows = observations_tab.ObservationsTab._build_cloud_observation_table_rows_cache(
        SimpleNamespace(),
        [
            {
                "id": "cloud-1",
                "genus": "Agaricus",
                "species": "campestris",
                "species_guess": "Agaricus campestris",
                "spore_statistics": {"count": 9},
                "date": "2026-06-16 07:55:26",
                "location": "Meadow",
                "visibility": "public",
            }
        ],
    )

    assert rows[0]["spore_short"] == "9"
    assert rows[0]["date"] == "2026-06-16 07:55"
