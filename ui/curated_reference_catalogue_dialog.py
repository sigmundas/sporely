"""Exact-taxon public catalogue picker for explicit personal copies."""
from __future__ import annotations

from PySide6.QtCore import QObject, QThread, Signal, Slot
from PySide6.QtWidgets import (
    QAbstractItemView, QDialog, QDialogButtonBox, QLabel, QMessageBox, QPushButton, QTableWidget,
    QTableWidgetItem, QVBoxLayout, QWidget,
)

from database.curated_reference_forks import (
    CuratedReferenceBundle,
    copy_curated_bundle_to_personal_library,
    search_curated_catalogue,
)


class _CatalogueWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, client: object, taxon_id: int) -> None:
        super().__init__()
        self._client = client
        self._taxon_id = taxon_id

    @Slot()
    def run(self) -> None:
        try:
            self.finished.emit(search_curated_catalogue(self._client, self._taxon_id))
        except Exception as exc:
            self.failed.emit(str(exc))


class CuratedReferenceCatalogueDialog(QDialog):
    copied = Signal(str)

    def __init__(self, parent: QWidget | None, *, cloud_client: object, sporely_taxon_id: int) -> None:
        super().__init__(parent)
        self._client = cloud_client
        self._taxon_id = int(sporely_taxon_id)
        self._bundles: tuple[CuratedReferenceBundle, ...] = ()
        self._thread: QThread | None = None
        self._close_pending = False
        self.setWindowTitle(self.tr("Public reference catalogue"))
        self.resize(720, 420)
        layout = QVBoxLayout(self)
        self.status_label = QLabel(self.tr("Loading exact-taxon references…"), self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        self.table = QTableWidget(0, 4, self)
        self.table.setHorizontalHeaderLabels([
            self.tr("Source"), self.tr("Taxon"), self.tr("Revision"), self.tr("Raw expression"),
        ])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self._update_copy_state)
        layout.addWidget(self.table, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.Close, parent=self)
        self.copy_button = QPushButton(self.tr("Copy to personal library"), self)
        self.copy_button.setEnabled(False)
        self.copy_button.clicked.connect(self._copy_selected)
        buttons.addButton(self.copy_button, QDialogButtonBox.ActionRole)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self._start_load()

    def _start_load(self) -> None:
        thread = QThread(self)
        worker = _CatalogueWorker(self._client, self._taxon_id)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._loaded)
        worker.failed.connect(self._failed)
        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(self._finish_pending_close)
        thread.finished.connect(thread.deleteLater)
        self._thread = thread
        thread.start()

    @Slot(object)
    def _loaded(self, bundles: object) -> None:
        self._bundles = tuple(bundles) if isinstance(bundles, tuple) else ()
        self.table.setRowCount(0)
        for bundle in self._bundles:
            row = self.table.rowCount()
            self.table.insertRow(row)
            values = (
                bundle.citation["short_citation"], bundle.canonical_scientific_name,
                str(bundle.bundle_revision), bundle.snapshot["raw_text"] or "",
            )
            for column, value in enumerate(values):
                self.table.setItem(row, column, QTableWidgetItem(str(value)))
        self.status_label.setText(
            self.tr("No published references found for this exact taxon.")
            if not self._bundles else self.tr("Select a published revision to copy.")
        )
        self._update_copy_state()

    @Slot(str)
    def _failed(self, message: str) -> None:
        self.status_label.setText(self.tr("Could not load the public catalogue: {error}").format(error=message))

    def _finish_pending_close(self) -> None:
        self._thread = None
        if self._close_pending:
            self.close()

    def _update_copy_state(self) -> None:
        self.copy_button.setEnabled(len(self.table.selectionModel().selectedRows()) == 1)

    def _copy_selected(self) -> None:
        rows = self.table.selectionModel().selectedRows()
        if len(rows) != 1:
            return
        try:
            result = copy_curated_bundle_to_personal_library(self._bundles[rows[0].row()])
        except Exception as exc:
            QMessageBox.warning(self, self.tr("Public reference catalogue"), self.tr("Could not copy reference: {error}").format(error=str(exc)))
            return
        self.copied.emit(result.reference_measurement_set_id)
        QMessageBox.information(
            self, self.tr("Public reference catalogue"),
            self.tr("The published revision was copied to your personal library.") if result.created
            else self.tr("This published revision is already in your personal library."),
        )
        self.accept()

    def closeEvent(self, event) -> None:
        if self._thread is not None and self._thread.isRunning():
            self._close_pending = True
            self.status_label.setText(self.tr("Finishing the current catalogue request…"))
            event.ignore()
            return
        super().closeEvent(event)
