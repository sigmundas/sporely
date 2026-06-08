from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ui import observations_tab
import ui.main_window as main_window


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


def test_metadata_sync_timeout_starts_metadata_only_sync(monkeypatch):
    calls: dict[str, object] = {}

    class _FakeTimer:
        def start(self, interval_ms):
            calls["timer_start"] = int(interval_ms)

        def stop(self):
            calls["timer_stop"] = True

    fake_tab = SimpleNamespace(
        _metadata_sync_timer=_FakeTimer(),
        _metadata_sync_delay_ms=8000,
        _metadata_sync_should_pause=lambda: False,
        _cloud_sync_pending_ids=lambda: [17],
        _start_cloud_sync=lambda **kwargs: calls.setdefault("start", kwargs) or True,
    )

    observations_tab.ObservationsTab._on_metadata_sync_timeout(fake_tab)

    assert calls["start"]["sync_images"] is False
    assert calls["start"]["materialize_remote_images"] is False
    assert calls.get("timer_stop") is True


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

    assert fake_timer.starts == [8000]


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
