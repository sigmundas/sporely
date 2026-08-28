from __future__ import annotations

import os
import inspect

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QDialog, QFileDialog, QMessageBox

import ui.main_window as main_window
from ui.observations_tab import ObservationsTab


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


class _DummySpeciesAvailability:
    def __init__(self, *args, **kwargs):
        pass


def test_full_restore_worker_emits_prepared_result(monkeypatch, qapp):
    prepared = object()
    received = []
    monkeypatch.setattr(main_window, "prepare_full_restore", lambda *args, **kwargs: prepared)
    worker = main_window._FullRestoreWorker("backup.sporely", "test")
    worker.prepared.connect(received.append)

    worker.run()

    assert received == [prepared]


def test_full_restore_worker_emits_preparation_failure(monkeypatch, qapp):
    received = []

    def fail(*args, **kwargs):
        raise RuntimeError("broken archive")

    monkeypatch.setattr(main_window, "prepare_full_restore", fail)
    worker = main_window._FullRestoreWorker("backup.sporely", "test")
    worker.failed.connect(received.append)

    worker.run()

    assert received == ["broken archive"]


def test_full_restore_swap_worker_runs_filesystem_phase_without_ui_callbacks(
    monkeypatch, qapp
):
    swap = object()
    received = []
    monkeypatch.setattr(
        main_window,
        "execute_prepared_restore_swap",
        lambda prepared, live_quiesced: swap,
    )

    worker = main_window._FullRestoreSwapWorker("apply", object())
    worker.completed.connect(received.append)
    worker.run()

    assert received == [swap]


def test_file_menu_exposes_full_backup_action(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "back_up_sporely", lambda self: calls.append(True))
    monkeypatch.setattr(main_window.MainWindow, "restore_sporely_backup", lambda self: calls.append("restore"))
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    window = main_window.MainWindow(app_version="test")

    file_action = next(
        action for action in window.menuBar().actions()
        if action.text() == window.tr("File")
    )
    file_menu = file_action.menu()
    backup_action = next(
        action for action in file_menu.actions()
        if action.text() == window.tr("Back Up Sporely…")
    )
    backup_action.trigger()
    restore_action = next(
        action for action in file_menu.actions()
        if action.text() == window.tr("Restore Sporely Backup…")
    )
    restore_action.trigger()
    assert calls == [True, "restore"]
    window.deleteLater()


def test_file_menu_export_selected_action_tracks_selection(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(
        main_window.MainWindow, "export_selected_observations", lambda self: calls.append(True)
    )
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    window = main_window.MainWindow(app_version="test")

    action = window.export_selected_observations_action
    assert not action.isEnabled()
    window._on_multi_observation_selected(2)
    assert action.isEnabled()
    action.trigger()
    window._on_multi_observation_selected(0)
    assert not action.isEnabled()
    assert calls == [True]
    window.deleteLater()


def test_file_menu_exposes_import_observations_action(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "import_observations", lambda self: calls.append(True))
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    window = main_window.MainWindow(app_version="test")
    window.import_observations_action.trigger()
    assert calls == [True]
    window.deleteLater()


def test_file_menu_contains_each_final_archive_action_once(monkeypatch, qapp):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: self.create_menu_bar())
    window = main_window.MainWindow(app_version="test")
    file_action = next(
        action for action in window.menuBar().actions()
        if action.text() == window.tr("File")
    )
    file_menu = file_action.menu()
    texts = [action.text() for action in file_menu.actions() if not action.isSeparator()]
    for text in (
        "Back Up Sporely…", "Restore Sporely Backup…",
        "Export Selected Observations…", "Import Observations…",
    ):
        assert texts.count(window.tr(text)) == 1
    assert window.import_observations_action.isEnabled()
    window._on_multi_observation_selected(0)
    assert not window.export_selected_observations_action.isEnabled()
    assert window.import_observations_action.isEnabled()
    window._on_multi_observation_selected(2)
    assert window.export_selected_observations_action.isEnabled()
    assert window.import_observations_action.isEnabled()
    window.deleteLater()


def test_legacy_observation_tab_controls_and_callbacks_are_removed():
    source = inspect.getsource(ObservationsTab.init_ui)
    assert "self.import_btn" not in source
    assert "self.export_btn" not in source
    assert not hasattr(ObservationsTab, "_on_import_db_clicked")
    assert not hasattr(ObservationsTab, "_on_export_db_clicked")
    assert not hasattr(main_window.MainWindow, "export_database_bundle")


def test_file_import_routes_legacy_zip_to_compatibility_importer(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(
        QFileDialog, "getOpenFileName", lambda *args: ("legacy-package.zip", "")
    )
    monkeypatch.setattr(
        main_window, "classify_archive",
        lambda path: main_window.ArchiveRoute.LEGACY_DATA_PACKAGE,
    )
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
        "import_database_bundle": staticmethod(lambda filename: calls.append(filename)),
    })()
    main_window.MainWindow.import_observations(dummy)
    assert calls == ["legacy-package.zip"]


def test_file_import_routes_current_archive_to_new_preview_worker(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(
        QFileDialog, "getOpenFileName", lambda *args: ("current.sporely", "")
    )
    monkeypatch.setattr(
        main_window, "classify_archive",
        lambda path: main_window.ArchiveRoute.PORTABLE_OBSERVATIONS,
    )

    class SignalStub:
        def connect(self, callback):
            pass

    class WorkerStub:
        def __init__(self, filename, parent):
            calls.append(filename)
            self.completed = SignalStub()
            self.failed = SignalStub()
            self.finished = SignalStub()

        def start(self):
            calls.append("start")

    monkeypatch.setattr(main_window, "_PortablePreviewWorker", WorkerStub)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
        "_portable_preview_worker": None,
        "_portable_import_worker": None,
        "_portable_import_filename": None,
        "_set_observations_status": staticmethod(lambda *args, **kwargs: None),
        "_on_portable_preview_completed": staticmethod(lambda preview: None),
        "_on_portable_preview_failed": staticmethod(lambda error: None),
        "_on_portable_preview_worker_finished": staticmethod(lambda: None),
    })()
    main_window.MainWindow.import_observations(dummy)
    assert calls == ["current.sporely", "start"]


def test_restore_rejects_legacy_package_before_backup_validation(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(
        QFileDialog, "getOpenFileName", lambda *args: ("legacy.zip", "")
    )
    monkeypatch.setattr(
        main_window, "classify_archive",
        lambda path: main_window.ArchiveRoute.LEGACY_DATA_PACKAGE,
    )
    monkeypatch.setattr(
        main_window, "validate_full_backup", lambda path: calls.append("validate")
    )
    monkeypatch.setattr(QMessageBox, "critical", lambda *args: calls.append("rejected"))
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_full_backup_worker": None,
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
    })()
    main_window.MainWindow.restore_sporely_backup(dummy)
    assert calls == ["rejected"]


def test_restore_action_starts_preparation_worker_without_running_restore_inline(
    monkeypatch, qapp
):
    calls = []

    class ActionStub:
        def __init__(self):
            self.enabled = True

        def setEnabled(self, enabled):
            self.enabled = enabled

    class SignalStub:
        def connect(self, callback):
            calls.append(("connect", callback.__name__))

    class WorkerStub:
        def __init__(self, filename, app_version, parent):
            calls.append(("worker", filename, app_version))
            self.prepared = SignalStub()
            self.failed = SignalStub()
            self.finished = SignalStub()

        def start(self):
            calls.append("start")

    manifest = type("Manifest", (), {
        "created_at": "now", "app_version": "old",
        "contents": {"observations": 1, "images": 2},
    })()
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: ("backup.sporely", ""))
    monkeypatch.setattr(main_window, "classify_archive", lambda path: main_window.ArchiveRoute.FULL_BACKUP)
    monkeypatch.setattr(main_window, "validate_full_backup", lambda path: manifest)
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
    monkeypatch.setattr(main_window, "_FullRestoreWorker", WorkerStub)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "app_version": "test",
        "_full_backup_worker": None,
        "_full_restore_worker": None,
        "restore_sporely_action": ActionStub(),
        "_running_restore_blocking_threads": staticmethod(lambda: []),
        "_begin_restore_maintenance": staticmethod(lambda: True),
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
        "_set_observations_status": staticmethod(lambda *args, **kwargs: None),
        "_on_full_restore_prepared": staticmethod(lambda value: None),
        "_on_full_restore_prepare_failed": staticmethod(lambda value: None),
        "_on_full_restore_worker_finished": staticmethod(lambda: None),
    })()

    main_window.MainWindow.restore_sporely_backup(dummy)

    assert ("worker", "backup.sporely", "test") in calls
    assert "start" in calls
    assert not dummy.restore_sporely_action.enabled

    dummy._full_restore_worker = type("Worker", (), {"deleteLater": lambda self: None})()
    main_window.MainWindow._on_full_restore_worker_finished(dummy)
    assert dummy.restore_sporely_action.enabled


def test_restore_maintenance_barrier_pauses_ui_and_timers_until_released(qapp):
    class ActionStub:
        def __init__(self):
            self.enabled = True

        def setEnabled(self, enabled):
            self.enabled = enabled

    class TimerStub:
        def __init__(self):
            self.active = True

        def isActive(self):
            return self.active

        def stop(self):
            self.active = False

        def start(self):
            self.active = True

    timer = TimerStub()
    actions = [ActionStub() for _ in range(4)]
    dummy = type("Window", (), {
        "_restore_maintenance_active": False,
        "_restore_paused_timers": [],
        "_restore_window_was_enabled": True,
        "restore_sporely_action": actions[0],
        "backup_sporely_action": actions[1],
        "export_selected_observations_action": actions[2],
        "import_observations_action": actions[3],
        "findChildren": lambda self, kind: [timer],
        "isEnabled": lambda self: self.enabled,
        "setEnabled": lambda self, enabled: setattr(self, "enabled", enabled),
        "enabled": True,
    })()

    assert main_window.MainWindow._begin_restore_maintenance(dummy)
    assert dummy._restore_maintenance_active
    assert not dummy.enabled
    assert not timer.active
    assert all(not action.enabled for action in actions)

    main_window.MainWindow._end_restore_maintenance(dummy)
    assert not dummy._restore_maintenance_active
    assert dummy.enabled
    assert timer.active
    assert all(action.enabled for action in actions)


def test_restore_maintenance_barrier_blocks_new_cloud_sync(qapp):
    starts = []
    tab = type("Tab", (), {
        "_start_cloud_sync": lambda self, **kwargs: starts.append(kwargs) or True,
    })()
    dummy = type("Window", (), {
        "_restore_maintenance_active": True,
        "observations_tab": tab,
    })()

    assert not main_window.MainWindow.start_cloud_sync(dummy)
    assert starts == []


def test_selection_change_cannot_reenable_export_during_restore_maintenance(qapp):
    class ActionStub:
        enabled = False

        def setEnabled(self, enabled):
            self.enabled = enabled

    dummy = type("Window", (), {
        "_restore_maintenance_active": True,
        "export_selected_observations_action": ActionStub(),
        "observations_tab": type("Tab", (), {
            "selected_observation_ids": lambda self: {7},
        })(),
    })()

    main_window.MainWindow._on_multi_observation_selected(dummy, 1)

    assert not dummy.export_selected_observations_action.enabled


def test_restore_worker_finish_does_not_release_active_maintenance(qapp):
    class ActionStub:
        enabled = False

        def setEnabled(self, enabled):
            self.enabled = enabled

    worker = type("Worker", (), {"deleteLater": lambda self: None})()
    dummy = type("Window", (), {
        "_restore_maintenance_active": True,
        "_full_restore_worker": worker,
        "restore_sporely_action": ActionStub(),
    })()

    main_window.MainWindow._on_full_restore_worker_finished(dummy)

    assert dummy._full_restore_worker is None
    assert not dummy.restore_sporely_action.enabled


def test_restore_close_refuses_to_swap_while_shutdown_leaves_worker_running(qapp):
    calls = []
    running_worker = object()
    observations_tab = type("Tab", (), {
        "shutdown": lambda self: calls.append("shutdown"),
    })()
    dummy = type("Window", (), {
        "observations_tab": observations_tab,
        "_running_restore_blocking_threads": lambda self: [running_worker],
    })()

    with pytest.raises(RuntimeError, match="background work is still running"):
        main_window.MainWindow._close_database_state_for_restore(dummy)

    assert calls == []


def test_restore_quiescence_includes_running_parked_workers(monkeypatch, qapp):
    worker = type("Worker", (), {"isRunning": lambda self: True})()
    app = type("App", (), {
        "findChildren": lambda self, kind: [],
        "_sporely_parked_threads": {worker},
    })()
    monkeypatch.setattr(QApplication, "instance", lambda: app)
    dummy = type("Window", (), {
        "_full_restore_worker": None,
        "_full_restore_apply_worker": None,
    })()

    assert main_window.MainWindow._running_restore_blocking_threads(dummy) == [worker]


def test_restore_quiescence_includes_window_owned_workers(monkeypatch, qapp):
    worker = type("Worker", (), {"isRunning": lambda self: True})()
    app = type("App", (), {
        "findChildren": lambda self, kind: [],
        "_sporely_parked_threads": set(),
    })()
    monkeypatch.setattr(QApplication, "instance", lambda: app)
    dummy = type("Window", (), {
        "_full_restore_worker": None,
        "_full_restore_apply_worker": None,
        "findChildren": lambda self, kind: [worker],
    })()

    assert main_window.MainWindow._running_restore_blocking_threads(dummy) == [worker]


def test_restore_cancel_schedules_staged_cleanup_off_ui_thread(monkeypatch, qapp):
    calls = []
    preparation = type("Preparation", (), {
        "manifest": type("Manifest", (), {
            "created_at": "now", "app_version": "old",
            "contents": {"observations": 1, "images": 2},
        })(),
        "cleanup": lambda self: calls.append("inline-cleanup"),
    })()
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Cancel)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_set_observations_status": lambda self, *args, **kwargs: None,
        "_start_restore_filesystem_worker": (
            lambda self, operation, value: calls.append((operation, value))
        ),
    })()

    main_window.MainWindow._on_full_restore_prepared(dummy, preparation)

    assert calls == [("cleanup_cancel", preparation)]


def test_restore_prepare_failure_releases_maintenance_barrier(monkeypatch, qapp):
    calls = []
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_end_restore_maintenance": lambda self: calls.append("release"),
        "_on_full_restore_failed": lambda self, error: (
            main_window.MainWindow._on_full_restore_failed(self, error)
        ),
        "setEnabled": lambda self, enabled: calls.append(("enabled", enabled)),
        "_set_observations_status": lambda self, *args, **kwargs: None,
    })()
    monkeypatch.setattr(QMessageBox, "critical", lambda *args: calls.append("error"))

    main_window.MainWindow._on_full_restore_prepare_failed(dummy, "broken")

    assert calls == ["release", "error"]


def test_prepared_restore_closes_live_state_before_starting_swap_worker(monkeypatch, qapp):
    calls = []
    preparation = type("Preparation", (), {
        "manifest": type("Manifest", (), {
            "created_at": "now", "app_version": "old",
            "contents": {"observations": 1, "images": 2},
        })(),
    })()
    monkeypatch.setattr(QMessageBox, "question", lambda *args: QMessageBox.Yes)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_set_observations_status": lambda self, *args, **kwargs: None,
        "_close_database_state_for_restore": lambda self: calls.append("close"),
        "_start_restore_filesystem_worker": (
            lambda self, operation, value: calls.append((operation, value))
        ),
        "_on_full_restore_failed": lambda self, value: calls.append(("failed", value)),
    })()

    main_window.MainWindow._on_full_restore_prepared(dummy, preparation)

    assert calls == ["close", ("apply", preparation)]


def test_import_observations_cancel_is_noop(monkeypatch, qapp):
    calls = []

    class RejectingDialog:
        def __init__(self, preview, parent):
            pass

        def exec(self):
            return QDialog.Rejected

    monkeypatch.setattr(main_window, "PortableImportDialog", RejectingDialog)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_portable_import_filename": "archive.sporely",
        "_set_observations_status": staticmethod(lambda *args, **kwargs: None),
        "_remember_import_dir": staticmethod(lambda path: calls.append("remember")),
    })()
    main_window.MainWindow._on_portable_preview_completed(dummy, object())
    assert calls == []


def test_invalid_import_archive_never_opens_actionable_dialog(monkeypatch, qapp):
    calls = []
    monkeypatch.setattr(main_window, "PortableImportDialog", lambda *args: calls.append("dialog"))
    monkeypatch.setattr(QMessageBox, "critical", lambda *args: calls.append("error"))
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
    })()
    main_window.MainWindow._on_portable_preview_failed(dummy, "corrupt")
    assert calls == ["error"]


def test_confirm_starts_import_for_exact_selected_subset(monkeypatch, qapp):
    calls = []

    class AcceptingDialog:
        def __init__(self, preview, parent):
            pass

        def exec(self):
            return QDialog.Accepted

        def selected_observation_ids(self):
            return {2, 5}

    class SignalStub:
        def connect(self, callback):
            pass

    class WorkerStub:
        def __init__(self, filename, selected, digest, parent):
            calls.append((filename, selected, digest))
            self.completed = SignalStub()
            self.failed = SignalStub()
            self.finished = SignalStub()

        def start(self):
            calls.append("start")

    monkeypatch.setattr(main_window, "PortableImportDialog", AcceptingDialog)
    monkeypatch.setattr(main_window, "_PortableImportWorker", WorkerStub)
    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_portable_import_filename": "archive.sporely",
        "_portable_import_worker": None,
        "_set_observations_status": staticmethod(lambda *args, **kwargs: None),
        "_on_portable_import_completed": staticmethod(lambda result: None),
        "_on_portable_import_failed": staticmethod(lambda error: None),
        "_on_portable_import_worker_finished": staticmethod(lambda: None),
    })()
    preview = type("Preview", (), {"archive_sha256": "abc"})()
    main_window.MainWindow._on_portable_preview_completed(dummy, preview)
    assert calls == [("archive.sporely", {2, 5}, "abc"), "start"]


def test_preview_finished_during_modal_dialog_does_not_lose_import_filename(
    monkeypatch, qapp
):
    calls = []

    class PreviewWorkerStub:
        def deleteLater(self):
            pass

    dummy = type("Window", (), {
        "tr": staticmethod(lambda value: value),
        "_portable_import_filename": "archive.sporely",
        "_portable_preview_worker": PreviewWorkerStub(),
        "_portable_import_worker": None,
        "_set_observations_status": staticmethod(lambda *args, **kwargs: None),
        "_on_portable_import_completed": staticmethod(lambda result: None),
        "_on_portable_import_failed": staticmethod(lambda error: None),
        "_on_portable_import_worker_finished": staticmethod(lambda: None),
    })()

    class AcceptingDialog:
        def __init__(self, preview, parent):
            pass

        def exec(self):
            main_window.MainWindow._on_portable_preview_worker_finished(dummy)
            return QDialog.Accepted

        def selected_observation_ids(self):
            return {2}

    class SignalStub:
        def connect(self, callback):
            pass

    class WorkerStub:
        def __init__(self, filename, selected, digest, parent):
            calls.append((filename, selected, digest))
            self.completed = SignalStub()
            self.failed = SignalStub()
            self.finished = SignalStub()

        def start(self):
            calls.append("start")

    monkeypatch.setattr(main_window, "PortableImportDialog", AcceptingDialog)
    monkeypatch.setattr(main_window, "_PortableImportWorker", WorkerStub)
    preview = type("Preview", (), {"archive_sha256": "abc"})()

    main_window.MainWindow._on_portable_preview_completed(dummy, preview)

    assert calls == [("archive.sporely", {2}, "abc"), "start"]
    assert dummy._portable_import_filename == "archive.sporely"
