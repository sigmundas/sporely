"""Import, restore and backup messages (redesign stage 10g): wording only.

They stay modal. Each says what happened and what to do next; the
technical error is under Show Details, not in the sentence. The restore
question names its buttons (Restore, Cancel) and defaults to Cancel. The
helper is driven on a real ``QMessageBox``; the main window's paths use
stand-ins for the window, as ``tests/test_full_backup_menu.py`` does.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication, QFileDialog, QMessageBox

import ui.main_window as main_window
from ui.dialog_helpers import show_message_box


@pytest.fixture
def said(monkeypatch):
    messages: list[dict] = []

    def record(_parent, kind, title, text, *, informative=None, details=None):
        messages.append({"kind": kind, "title": title, "text": text, "informative": informative, "details": details})

    monkeypatch.setattr(main_window, "show_message_box", record)
    return messages


def _window(**attributes):
    base = {
        "tr": staticmethod(lambda value: value),
        "_full_backup_worker": None,
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
        "_say_archive_unchecked": main_window.MainWindow._say_archive_unchecked,
    }
    base.update(attributes)
    return type("Window", (), base)()


def test_the_helper_keeps_the_error_under_details(qtbot):
    seen: dict = {}

    def inspect():
        box = QApplication.activeModalWidget()
        # No window title: macOS message boxes do not show one, so the text says it all.
        seen.update(
            text=box.text(), informative=box.informativeText(),
            details=box.detailedText(), icon=box.icon(), buttons=box.standardButtons(),
        )
        box.accept()

    QTimer.singleShot(0, inspect)
    show_message_box(
        None, "critical", "Import Observations", "The observations were not imported.",
        informative="Try again when the database is free.", details="sqlite3.OperationalError: database is locked",
    )
    assert seen == {
        "text": "The observations were not imported.",
        "informative": "Try again when the database is free.", "details": "sqlite3.OperationalError: database is locked",
        "icon": QMessageBox.Critical, "buttons": QMessageBox.Ok,
    }


def test_an_unreadable_import_file_puts_the_error_under_details(monkeypatch, said):
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: ("odd.sporely", ""))

    def broken(_path):
        raise main_window.ArchiveRoutingError("manifest.json missing")

    monkeypatch.setattr(main_window, "classify_archive", broken)
    main_window.MainWindow.import_observations(_window())
    assert said[0]["details"] == "manifest.json missing"
    assert "manifest" not in said[0]["text"] and said[0]["informative"]


def test_a_full_backup_chosen_for_import_points_to_restore(monkeypatch, said):
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: ("backup.sporely", ""))
    monkeypatch.setattr(main_window, "classify_archive", lambda path: main_window.ArchiveRoute.FULL_BACKUP)
    main_window.MainWindow.import_observations(_window())
    assert said[0]["kind"] == "warning"
    assert "Restore Sporely Backup" in said[0]["informative"] and "replaces everything" in said[0]["informative"]


@pytest.mark.parametrize("method, error", [
    ("_on_portable_preview_failed", "checksum mismatch"),
    ("_on_portable_import_failed", "UNIQUE constraint failed: observations.sync_key"),
])
def test_import_failures_keep_the_error_under_details(said, method, error):
    getattr(main_window.MainWindow, method)(_window(), error)
    assert said[0]["details"] == error and error not in said[0]["text"]
    assert said[0]["kind"] == "critical"


@pytest.mark.parametrize("route, points_to", [
    (main_window.ArchiveRoute.LEGACY_DATA_PACKAGE, "older Sporely data package"),
    (main_window.ArchiveRoute.PORTABLE_OBSERVATIONS if hasattr(main_window.ArchiveRoute, "PORTABLE_OBSERVATIONS")
     else next(r for r in main_window.ArchiveRoute
               if r not in (main_window.ArchiveRoute.FULL_BACKUP, main_window.ArchiveRoute.LEGACY_DATA_PACKAGE)),
     "observation archive"),
])
def test_a_non_backup_chosen_for_restore_points_to_import(monkeypatch, said, route, points_to):
    monkeypatch.setattr(QFileDialog, "getOpenFileName", lambda *args: ("file.sporely", ""))
    monkeypatch.setattr(main_window, "classify_archive", lambda path: route)
    main_window.MainWindow.restore_sporely_backup(_window())
    assert points_to in said[0]["text"]
    assert "Import Observations" in said[0]["informative"]


def test_the_restore_question_names_its_buttons_and_defaults_to_cancel(monkeypatch):
    asked: list = []
    monkeypatch.setattr(main_window, "ask_wrapped_yes_no", lambda *args, **kwargs: asked.append((args, kwargs)) or False)
    preparation = type("Preparation", (), {"manifest": type("Manifest", (), {
        "created_at": "2026-11-01 10:00", "app_version": "0.9.30",
        "contents": {"observations": 412, "images": 3150},
    })()})()
    calls = []
    window = _window(_start_restore_filesystem_worker=lambda self, operation, value: calls.append(operation))
    main_window.MainWindow._on_full_restore_prepared(window, preparation)
    (args, kwargs), = asked
    assert args[2].startswith("Replace everything in Sporely with this backup?")
    assert "412 observations, 3150 images" in args[2] and "safety backup" in args[2]
    assert kwargs == {"yes_text": "Restore", "no_text": "Cancel"}  # no default_yes: Cancel is the default
    assert calls == ["cleanup_cancel"]


def _restore_window(**attributes):
    base = dict(
        _end_restore_maintenance=lambda self: None,
        _set_observations_status=lambda self, *args, **kwargs: None,
        _on_full_restore_failed=main_window.MainWindow._on_full_restore_failed,
        _pending_restore_error=None,
        _pending_restore_swap=None,
        _pending_restore_qsettings=None,
    )
    base.update(attributes)
    return _window(**base)


@pytest.mark.parametrize("state, text, informative", [
    ("unchanged", "The backup was not restored.", "Nothing in Sporely was changed."),
    ("rolled_back", "The backup was not restored.", "Your previous data was put back."),
    ("unknown", "The restore did not complete.", "partly restored"),
])
def test_restore_failures_say_what_is_known_of_the_data(said, state, text, informative):
    main_window.MainWindow._on_full_restore_failed(_restore_window(), "Restore rollback failed: disk full", state=state)
    assert said[0]["text"] == text and informative in said[0]["informative"]
    assert said[0]["details"] == "Restore rollback failed: disk full"


def test_an_unknown_state_is_the_default(said):
    main_window.MainWindow._on_full_restore_failed(_restore_window(), "boom")
    assert said[0]["text"] == "The restore did not complete."


@pytest.mark.parametrize("operation, expected", [
    ("cleanup_cancel", "Nothing in Sporely was changed."),
    ("cleanup_failure", "Nothing in Sporely was changed."),
    ("apply", "partly restored"),
    ("commit", "partly restored"),
    ("rollback", "partly restored"),
])
def test_a_failed_restore_step_reports_the_state_it_leaves(said, operation, expected):
    window = _restore_window(_reopen_database_state_after_restore=lambda self: None)
    main_window.MainWindow._on_full_restore_filesystem_failed(window, operation, "disk full")
    assert expected in said[0]["informative"]


@pytest.mark.parametrize("reopen_fails, expected", [(False, "put back"), (True, "partly restored")])
def test_a_completed_rollback_says_the_previous_data_is_back_unless_it_cannot_reopen(said, reopen_fails, expected):
    def reopen(self):
        if reopen_fails:
            raise RuntimeError("database is locked")

    window = _restore_window(_reopen_database_state_after_restore=reopen, _pending_restore_error="sanity check failed")
    main_window.MainWindow._on_full_restore_rollback_completed(window)
    assert expected in said[0]["informative"]


def test_a_failed_preparation_changed_nothing(said):
    main_window.MainWindow._on_full_restore_prepare_failed(_restore_window(), "not a Sporely backup")
    assert said[0]["informative"] == "Nothing in Sporely was changed."


def test_a_failed_import_does_not_claim_nothing_was_imported(said):
    main_window.MainWindow._on_portable_import_failed(_window(), "journal cleanup failed")
    assert "may already have been imported" in said[0]["informative"]
    assert "not imported" not in said[0]["text"]
