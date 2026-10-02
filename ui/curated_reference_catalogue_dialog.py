"""Exact-taxon public catalogue picker for explicit personal copies."""
from __future__ import annotations

from PySide6.QtCore import QCoreApplication, QObject, Qt, QThread, Signal, Slot
from PySide6.QtGui import QBrush, QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView, QDialog, QDialogButtonBox, QLabel, QMessageBox, QPushButton, QTableWidget,
    QTableWidgetItem, QVBoxLayout, QWidget,
)

from database.curated_reference_forks import (
    RELATIONSHIP_ROLE_ORDER,
    CuratedReferenceBundle,
    copy_curated_bundle_to_personal_library,
    search_shared_reference_contributions,
)


def relationship_label(roles: tuple[str, ...] | list[str]) -> str:
    """Stage 2c label rule: one label per role present, in the order
    Supports · Contradicts · Compared. No roles gives no label (never
    "Supports")."""
    names = {
        "supports_identification": QCoreApplication.translate("SharedReferenceCatalogue", "Supports"),
        "contradicts": QCoreApplication.translate("SharedReferenceCatalogue", "Contradicts"),
        "compared": QCoreApplication.translate("SharedReferenceCatalogue", "Compared"),
    }
    present = set(roles or ())
    return " · ".join(names[role] for role in RELATIONSHIP_ROLE_ORDER if role in present)


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
            self.finished.emit(search_shared_reference_contributions(self._client, self._taxon_id))
        except Exception as exc:
            self.failed.emit(str(exc))


class SharedReferenceCatalogueDialog(QDialog):
    copied = Signal(str)

    def __init__(self, parent: QWidget | None, *, cloud_client: object, sporely_taxon_id: int) -> None:
        super().__init__(parent)
        self._client = cloud_client
        self._taxon_id = int(sporely_taxon_id)
        self._bundles: tuple[CuratedReferenceBundle, ...] = ()
        self._thread: QThread | None = None
        self._close_pending = False
        self.setWindowTitle(self.tr("Shared reference contributions"))
        self.resize(720, 420)
        layout = QVBoxLayout(self)
        self.status_label = QLabel(self.tr("Loading exact-taxon references…"), self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        self.table = QTableWidget(0, 6, self)
        self.table.setHorizontalHeaderLabels([
            self.tr("Source"), self.tr("Taxon"), self.tr("Revision"),
            self.tr("Raw expression"), self.tr("Contributor"),
            self.tr("Current relationship"),
        ])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self._update_copy_state)
        layout.addWidget(self.table, 1)
        # The relationship is live: how the contributor currently uses the
        # reference on their public observations of this species.
        self.relationship_label = QLabel(self)
        self.relationship_label.setWordWrap(True)
        self.relationship_label.setTextFormat(Qt.PlainText)
        self.relationship_label.hide()
        layout.addWidget(self.relationship_label)
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
                str(bundle.bundle_revision),
                (bundle.snapshot["raw_text"] or "") + (
                    " " + self.tr("(measurement details omitted; cannot be copied)")
                    if bundle.measurement_details_omitted else ""
                ),
                bundle.contributor_label or self.tr("Sporely user"),
                relationship_label(bundle.relationship_roles),
            )
            for column, value in enumerate(values):
                self.table.setItem(row, column, QTableWidgetItem(str(value)))
            if bundle.measurement_details_omitted:
                for column in range(len(values)):
                    self.table.item(row, column).setForeground(QBrush(QColor("#8a8a8a")))
            if "contradicts" in bundle.relationship_roles:
                cell = self.table.item(row, 5)
                font = QFont(cell.font())
                font.setBold(True)
                cell.setFont(font)
                cell.setForeground(QBrush(QColor("#b02a37")))
        self.status_label.setText(
            self.tr("No shared contributions found for this exact taxon.")
            if not self._bundles else self.tr("Select a contribution revision to copy.")
        )
        self._update_copy_state()

    @Slot(str)
    def _failed(self, message: str) -> None:
        self.status_label.setText(self.tr("Could not load shared contributions: {error}").format(error=message))

    def _finish_pending_close(self) -> None:
        self._thread = None
        if self._close_pending:
            self.close()

    def _update_copy_state(self) -> None:
        rows = self.table.selectionModel().selectedRows()
        bundle = self._bundles[rows[0].row()] if len(rows) == 1 and rows[0].row() < len(self._bundles) else None
        # A row served without its measurement details is never copied (the
        # copy would store a lossy projection as editable content).
        omitted = bundle is not None and bundle.measurement_details_omitted
        self.copy_button.setEnabled(bundle is not None and not omitted)
        self.copy_button.setToolTip(
            self.tr("This contribution was served without its measurement details and cannot be copied.")
            if omitted else ""
        )
        label = relationship_label(bundle.relationship_roles) if bundle is not None else ""
        if not label:
            self.relationship_label.setText("")
            self.relationship_label.hide()
            return
        contradicts = "contradicts" in bundle.relationship_roles
        self.relationship_label.setText(
            self.tr("Contradicts the identification. The contributor currently uses this "
                    "reference as contradicting {species} on their public observations "
                    "(current relationship: {label}).").format(
                species=bundle.canonical_scientific_name, label=label)
            if contradicts else
            self.tr("Current relationship on the contributor's public observations: {label}").format(label=label)
        )
        self.relationship_label.setStyleSheet(
            "background-color: #f8d7da; color: #58151c; border: 1px solid #f1aeb5;"
            " border-radius: 4px; padding: 6px; font-weight: bold;" if contradicts else ""
        )
        self.relationship_label.show()

    def _copy_selected(self) -> None:
        rows = self.table.selectionModel().selectedRows()
        if len(rows) != 1:
            return
        bundle = self._bundles[rows[0].row()]
        if bundle.measurement_details_omitted:
            QMessageBox.warning(
                self, self.tr("Shared reference contributions"),
                self.tr("This contribution was served without its measurement details and cannot be copied."),
            )
            return
        try:
            result = copy_curated_bundle_to_personal_library(bundle)
        except Exception as exc:
            QMessageBox.warning(self, self.tr("Shared reference contributions"), self.tr("Could not copy reference: {error}").format(error=str(exc)))
            return
        self.copied.emit(result.reference_measurement_set_id)
        QMessageBox.information(
            self, self.tr("Shared reference contributions"),
            self.tr("The contribution was copied to your personal library.") if result.created
            else self.tr("This contribution is already in your personal library."),
        )
        self.accept()

    def closeEvent(self, event) -> None:
        if self._thread is not None and self._thread.isRunning():
            self._close_pending = True
            self.status_label.setText(self.tr("Finishing the current catalogue request…"))
            event.ignore()
            return
        super().closeEvent(event)


# Compatibility alias for callers and persisted imports from Stage 6.
CuratedReferenceCatalogueDialog = SharedReferenceCatalogueDialog
