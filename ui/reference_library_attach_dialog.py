"""Focused chooser dialog for attaching an existing normalized reference
measurement set to the active observation.

Does not create or edit any library entities. Presents unattached
candidate measurement sets from ``MeasurementSetRepository`` and a
translated role selector limited to the ``OBSERVATION_REFERENCE_ROLES``
enum (``compared``, ``supports_identification``, ``contradicts``).
"""
from __future__ import annotations

from typing import Iterable

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.reference_library import (
    MeasurementSetCandidate,
    MeasurementSetRepository,
)


_ROLE_VALUES: tuple[str, ...] = (
    "compared",
    "supports_identification",
    "contradicts",
)


class ReferenceLibraryAttachDialog(QDialog):
    """Chooser dialog returning ``(measurement_set_id, role)`` on accept."""

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        exclude_measurement_set_ids: Iterable[str] | None = None,
        candidates: list[MeasurementSetCandidate] | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Attach library reference"))
        self.setModal(True)
        self._selected_id: str | None = None
        self._exclude_ids = {str(x) for x in (exclude_measurement_set_ids or [])}

        if candidates is None:
            candidates = MeasurementSetRepository.list_attachment_candidates(
                exclude_ids=self._exclude_ids
            )
        else:
            candidates = [
                c for c in candidates
                if str(getattr(c, "measurement_set_id", "")) not in self._exclude_ids
            ]
        self._candidates: list[MeasurementSetCandidate] = list(candidates)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        header = QLabel(
            self.tr(
                "Select a reference measurement set to attach to the active "
                "observation."
            )
        )
        header.setWordWrap(True)
        layout.addWidget(header)

        self.table = QTableWidget(0, 5, self)
        self.table.setObjectName("referenceLibraryAttachTable")
        self.table.setHorizontalHeaderLabels(
            [
                self.tr("Source"),
                self.tr("Taxon (as published)"),
                self.tr("Locator"),
                self.tr("Kind"),
                self.tr("Raw expression"),
            ]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(0, QHeaderView.Stretch)
        header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.setMinimumHeight(220)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.table, 1)

        role_row = QHBoxLayout()
        role_row.setSpacing(8)
        role_label = QLabel(self.tr("Role:"))
        role_row.addWidget(role_label)
        self.role_combo = QComboBox(self)
        for value in _ROLE_VALUES:
            self.role_combo.addItem(self._role_display_label(value), value)
        self.role_combo.setCurrentIndex(0)
        role_row.addWidget(self.role_combo, 1)
        layout.addLayout(role_row)

        self._empty_label = QLabel(
            self.tr(
                "No reference measurement sets are available. Add reference "
                "works to the library before attaching."
            )
        )
        self._empty_label.setWordWrap(True)
        self._empty_label.setVisible(False)
        layout.addWidget(self._empty_label)

        self.buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel,
            Qt.Horizontal,
            self,
        )
        self.buttons.accepted.connect(self._on_accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

        self._populate_table()
        self._update_accept_state()

    def _role_display_label(self, value: str) -> str:
        if value == "compared":
            return self.tr("Compared")
        if value == "supports_identification":
            return self.tr("Supports identification")
        if value == "contradicts":
            return self.tr("Contradicts")
        return value

    def _populate_table(self) -> None:
        self.table.setRowCount(0)
        if not self._candidates:
            self.table.setEnabled(False)
            self._empty_label.setVisible(True)
            return
        self.table.setEnabled(True)
        self._empty_label.setVisible(False)
        for candidate in self._candidates:
            row = self.table.rowCount()
            self.table.insertRow(row)
            source_item = QTableWidgetItem(candidate.short_label or "")
            source_item.setData(Qt.UserRole, candidate.measurement_set_id)
            source_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 0, source_item)
            taxon_item = QTableWidgetItem(candidate.name_as_published or "")
            taxon_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 1, taxon_item)
            locator_item = QTableWidgetItem(candidate.locator_text or "")
            locator_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 2, locator_item)
            kind_item = QTableWidgetItem(candidate.data_kind or "")
            kind_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 3, kind_item)
            raw_item = QTableWidgetItem(candidate.raw_text or "")
            raw_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 4, raw_item)

    def _on_selection_changed(self) -> None:
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            self._selected_id = None
        else:
            row = selected_rows[0].row()
            item = self.table.item(row, 0)
            self._selected_id = str(item.data(Qt.UserRole)) if item else None
        self._update_accept_state()

    def _update_accept_state(self) -> None:
        ok_button = self.buttons.button(QDialogButtonBox.Ok)
        if ok_button is not None:
            ok_button.setEnabled(bool(self._selected_id))

    def _on_accept(self) -> None:
        if not self._selected_id:
            return
        self.accept()

    def selected_measurement_set_id(self) -> str | None:
        return self._selected_id

    def selected_role(self) -> str:
        data = self.role_combo.currentData()
        if isinstance(data, str) and data in _ROLE_VALUES:
            return data
        return "compared"

    def result_pair(self) -> tuple[str | None, str]:
        return self.selected_measurement_set_id(), self.selected_role()


__all__ = ["ReferenceLibraryAttachDialog"]
