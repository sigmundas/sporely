"""Portable observation archive preview and root selection dialog."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHeaderView,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from utils.archive.portable_import import PortableArchivePreview, PortableClosureCounts


class PortableImportDialog(QDialog):
    def __init__(self, preview: PortableArchivePreview, parent=None) -> None:
        super().__init__(parent)
        self.preview = preview
        self.setWindowTitle(self.tr("Import observations"))
        self.resize(820, 560)

        layout = QVBoxLayout(self)
        metadata = QFormLayout()
        metadata.addRow(self.tr("Archive created:"), QLabel(preview.created_at))
        metadata.addRow(self.tr("Created with Sporely:"), QLabel(preview.app_version))
        metadata.addRow(self.tr("Source platform:"), QLabel(preview.source_platform))
        layout.addLayout(metadata)

        self.observation_table = QTableWidget(len(preview.observations), 3, self)
        self.observation_table.setHorizontalHeaderLabels(
            [self.tr("Observation"), self.tr("Date"), self.tr("Images")]
        )
        self.observation_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.observation_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.observation_table.verticalHeader().setVisible(False)
        header = self.observation_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for row, observation in enumerate(preview.observations):
            name = QTableWidgetItem(observation.name)
            name.setData(Qt.UserRole, observation.observation_id)
            name.setFlags(name.flags() | Qt.ItemIsUserCheckable)
            name.setCheckState(Qt.Checked)
            self.observation_table.setItem(row, 0, name)
            self.observation_table.setItem(row, 1, QTableWidgetItem(observation.date))
            image_count = QTableWidgetItem(str(observation.image_count))
            image_count.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.observation_table.setItem(row, 2, image_count)
        self.observation_table.itemChanged.connect(self._on_selection_changed)
        layout.addWidget(self.observation_table, 1)

        self.selection_summary = QLabel(self)
        self.selection_summary.setWordWrap(True)
        layout.addWidget(self.selection_summary)

        self.button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self
        )
        self.import_button = self.button_box.button(QDialogButtonBox.Ok)
        self.import_button.setText(self.tr("Import"))
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)
        self._update_selection_summary()

    def selected_observation_ids(self) -> set[int]:
        selected: set[int] = set()
        for row in range(self.observation_table.rowCount()):
            item = self.observation_table.item(row, 0)
            if item is not None and item.checkState() == Qt.Checked:
                selected.add(int(item.data(Qt.UserRole)))
        return selected

    def _on_selection_changed(self, item: QTableWidgetItem) -> None:
        if item.column() == 0:
            self._update_selection_summary()

    def _update_selection_summary(self) -> None:
        selected = self.selected_observation_ids()
        counts = (
            self.preview.closure_counts(selected)
            if selected
            else PortableClosureCounts(0, 0, 0, 0, 0)
        )
        self.selection_summary.setText(
            self.tr(
                "Selected: {observations} observations, {images} images, "
                "{measurements} measurements, {calibrations} calibration records, "
                "{references} references"
            ).format(
                observations=counts.observations,
                images=counts.images,
                measurements=counts.measurements,
                calibrations=counts.calibrations,
                references=counts.references,
            )
        )
        self.import_button.setEnabled(bool(selected))
