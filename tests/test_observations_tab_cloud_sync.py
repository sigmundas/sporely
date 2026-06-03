from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ui import observations_tab


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
    assert "Cloud sync failed while uploading an image that is too large for your plan." in status_args[0]
    assert status_kwargs["level"] == "warning"
