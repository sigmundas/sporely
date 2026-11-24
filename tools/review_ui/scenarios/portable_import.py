"""Portable observation import preview scenarios."""

from __future__ import annotations

from PySide6.QtCore import Qt

from ui.portable_import_dialog import PortableImportDialog
from utils.archive.portable_import import PortableClosureCounts, PortableObservationPreview

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


class _PreviewFixture:
    created_at = "2026-08-07T12:00:00+02:00"
    app_version = "0.x.x"
    source_platform = "Linux"
    observations = (
        PortableObservationPreview(1, "Amanita muscaria", "2026-08-03", 5),
        PortableObservationPreview(2, "Russula emetica", "2026-08-02", 3),
        PortableObservationPreview(3, "Lactarius turpis", "2026-08-01", 8),
    )
    full_counts = PortableClosureCounts(3, 16, 212, 3, 6)

    def closure_counts(self, observation_ids: set[int]) -> PortableClosureCounts:
        images = {1: 5, 2: 3, 3: 8}
        return PortableClosureCounts(
            len(observation_ids), sum(images[item] for item in observation_ids),
            147 if observation_ids == {1, 2} else 212,
            len(observation_ids), len(observation_ids) * 2,
        )


def _build(context: ReviewContext, *, subset: bool) -> PortableImportDialog:
    dialog = PortableImportDialog(_PreviewFixture(), context.host)
    if subset:
        dialog.observation_table.item(2, 0).setCheckState(Qt.Unchecked)
    return dialog


def register_portable_import_scenarios(registry: ScenarioRegistry) -> None:
    registry.register(ReviewScenario(
        id="portable-import.all-selected", group="portable-import",
        title="Portable import — all selected",
        description="Validated archive metadata and complete observation selection.",
        build=lambda context: _build(context, subset=False), viewport=(900, 650),
    ))
    registry.register(ReviewScenario(
        id="portable-import.subset", group="portable-import",
        title="Portable import — subset",
        description="Changing checkboxes updates the selected dependency closure.",
        build=lambda context: _build(context, subset=True), viewport=(900, 650),
    ))
    registry.register(ReviewScenario(
        id="portable-import.nb", group="portable-import",
        title="Portable import (nb)",
        description="Norwegian: the dialog's own texts and Qt's standard Cancel button.",
        build=lambda context: _build(context, subset=False), viewport=(900, 650),
        locale="nb_NO",
    ))
    registry.register(ReviewScenario(
        id="portable-import.de", group="portable-import",
        title="Portable import (de)",
        description="German, the long-string locale, including Qt's standard Cancel button.",
        build=lambda context: _build(context, subset=False), viewport=(900, 650),
        locale="de_DE",
    ))


def _archive_message(context: ReviewContext, method: str, *args, details_open: bool = False):
    """The message one of MainWindow's import or restore paths shows (stage
    10g), from the real handler: the helper builds the box instead of
    running it, and the window is a stand-in with MainWindow's translations."""
    from unittest.mock import patch

    from PySide6.QtCore import QCoreApplication
    from PySide6.QtWidgets import QMessageBox

    import ui.main_window as main_window
    from ui.dialog_helpers import build_message_box

    built: list = []

    def capture(_parent, kind, title, text, *, informative=None, details=None):
        built.append(build_message_box(
            context.host, kind, title, text, informative=informative, details=details,
        ))

    window = type("Window", (), {
        "tr": staticmethod(lambda value: QCoreApplication.translate("MainWindow", value)),
        "_say_archive_unchecked": main_window.MainWindow._say_archive_unchecked,
        "_end_restore_maintenance": lambda self: None,
        "_set_observations_status": lambda self, *a, **k: None,
        "_closing_after_restore": False,
        "close": lambda self: None,
        "_full_backup_worker": None,
        "_get_default_import_dir": staticmethod(lambda: "/tmp"),
    })()
    with patch.object(main_window, "show_message_box", capture):
        getattr(main_window.MainWindow, method)(window, *args)
    box = built[-1]
    if details_open:
        for button in box.buttons():
            if box.buttonRole(button) == QMessageBox.ActionRole:
                button.click()  # Show Details…
    return box


def _restore_result():
    return type("Result", (), {"safety_backup": "/Users/me/Library/Application Support/Sporely/safety-backup-2026-11-14.sporely"})()


def _full_backup_for_import(context: ReviewContext):
    from unittest.mock import patch

    import ui.main_window as main_window
    from PySide6.QtWidgets import QFileDialog

    with patch.object(QFileDialog, "getOpenFileName", lambda *a, **k: ("backup.sporely", "")), \
            patch.object(main_window, "classify_archive", lambda path: main_window.ArchiveRoute.FULL_BACKUP):
        return _archive_message(context, "import_observations")


def register_archive_message_scenarios(registry: ScenarioRegistry) -> None:
    error = "UNIQUE constraint failed: observations.sync_key"
    for locale, suffix in (("nb_NO", ""), ("de_DE", "-de")):
        registry.register(ReviewScenario(
            id=f"archive-messages.import-failed{suffix}", group="archive-messages",
            title=f"Import failed ({locale})",
            description="An import that failed: the sentence says what happened, the error is under Show Details (stage 10g).",
            build=lambda context: _archive_message(context, "_on_portable_import_failed", error),
            natural_size=True, viewport=(520, 200), locale=locale,
        ))
        registry.register(ReviewScenario(
            id=f"archive-messages.full-backup-for-import{suffix}", group="archive-messages",
            title=f"A full backup chosen for import ({locale})",
            description="Import Observations given a full backup points to Restore (stage 10g).",
            build=_full_backup_for_import, natural_size=True, viewport=(520, 200), locale=locale,
        ))
    registry.register(ReviewScenario(
        id="archive-messages.import-failed-details", group="archive-messages",
        title="Import failed, details shown (nb)",
        description="Show Details opened: the database error (stage 10g).",
        build=lambda context: _archive_message(context, "_on_portable_import_failed", error, details_open=True),
        natural_size=True, viewport=(520, 200), locale="nb_NO",
    ))
    registry.register(ReviewScenario(
        id="archive-messages.restore-complete", group="archive-messages",
        title="Restore complete (nb)",
        description="The backup restored: Sporely closes, the safety backup's path (stage 10g).",
        build=lambda context: _archive_message(context, "_on_full_restore_completed", _restore_result()),
        natural_size=True, viewport=(520, 200), locale="nb_NO",
    ))
