"""Focused chooser dialog for attaching an existing normalized reference
measurement set to the active observation.

Does not create or edit any library entities. Presents unattached
candidate measurement sets from ``MeasurementSetRepository`` and a
translated role selector limited to the ``OBSERVATION_REFERENCE_ROLES``
enum (``compared``, ``supports_identification``, ``contradicts``).
"""
from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.reference_library import (
    MeasurementSetCandidate,
    MeasurementSetPreferenceRepository,
    MeasurementSetRepository,
    ReferenceLibraryError,
)


_ROLE_VALUES: tuple[str, ...] = (
    "compared",
    "supports_identification",
    "contradicts",
)


class ReferenceLibraryAttachDialog(QDialog):
    """Chooser dialog returning ``(measurement_set_id, role)`` on accept.

    Also emits :attr:`manage_library_requested` when the user clicks the
    "Manage library…" affordance so the parent (MainWindow) can open the
    normalized library manager. After the manager closes, callers should
    invoke :meth:`refresh_candidates` to repopulate the chooser table.

    When ``taxon_id`` is provided the candidate list is initially scoped
    to treatments whose ``taxon_id`` matches; a checkbox lets the user
    widen the search to the whole library. A live text-search box filters
    the visible rows in-memory against short label, published name,
    locator, kind and raw expression.
    """

    manage_library_requested = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        exclude_measurement_set_ids: Iterable[str] | None = None,
        candidates: list[MeasurementSetCandidate] | None = None,
        taxon_id: int | str | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Attach library reference"))
        self.setModal(True)
        self._selected_id: str | None = None
        self._exclude_ids = {str(x) for x in (exclude_measurement_set_ids or [])}
        # Normalize the taxon id to a string so the join query (which
        # stores taxon_id as TEXT) can be filtered against integer or
        # string inputs without surprising conversions.
        self._taxon_id: str | None
        if taxon_id is None:
            self._taxon_id = None
        else:
            text = str(taxon_id).strip()
            self._taxon_id = text or None

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

        # Search + taxon-scope row. Both controls are always present so
        # the layout stays stable across observations with and without a
        # taxon; the checkbox is disabled (and unchecked) when no taxon
        # id was provided, so users are not misled by a filter that
        # cannot narrow anything.
        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)
        filter_label = QLabel(self.tr("Search:"))
        filter_row.addWidget(filter_label)
        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText(
            self.tr("Filter by publication, taxon, locator, kind, or raw text…")
        )
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self._on_filter_changed)
        filter_row.addWidget(self.search_input, 1)
        self.only_this_taxon_checkbox = QCheckBox(self.tr("Only this taxon"), self)
        self.only_this_taxon_checkbox.setToolTip(
            self.tr(
                "Restrict the list to measurement sets whose treatment matches "
                "the active observation's taxon."
            )
        )
        if self._taxon_id is None:
            self.only_this_taxon_checkbox.setEnabled(False)
            self.only_this_taxon_checkbox.setChecked(False)
        else:
            self.only_this_taxon_checkbox.setChecked(True)
        self.only_this_taxon_checkbox.toggled.connect(self._on_filter_changed)
        filter_row.addWidget(self.only_this_taxon_checkbox)
        self.usage_filter_combo = QComboBox(self)
        self.usage_filter_combo.addItem(self.tr("All"), "all")
        self.usage_filter_combo.addItem(self.tr("Favourites"), "favorites")
        self.usage_filter_combo.addItem(self.tr("Recently used"), "recent")
        self.usage_filter_combo.currentIndexChanged.connect(
            self._on_filter_changed
        )
        filter_row.addWidget(self.usage_filter_combo)
        layout.addLayout(filter_row)

        self.table = QTableWidget(0, 6, self)
        self.table.setObjectName("referenceLibraryAttachTable")
        self.table.setHorizontalHeaderLabels(
            [
                self.tr("Favourite"),
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
        header_view.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        header_view.setSectionResizeMode(2, QHeaderView.Stretch)
        header_view.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(5, QHeaderView.Stretch)
        self.table.setMinimumHeight(220)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        self.table.cellClicked.connect(self._on_table_cell_clicked)
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

        manage_row = QHBoxLayout()
        self.manage_library_btn = QPushButton(self.tr("Manage library…"))
        self.manage_library_btn.clicked.connect(self._on_manage_library_clicked)
        manage_row.addWidget(self.manage_library_btn)
        manage_row.addStretch(1)
        layout.addLayout(manage_row)

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

    # ----- Filtering helpers ------------------------------------------------

    def _filtered_candidates(self) -> list[MeasurementSetCandidate]:
        """Apply the current taxon scope + text search to ``self._candidates``.

        Returns a fresh list preserving the repository's deterministic
        order.
        """
        candidates = list(self._candidates)
        usage_filter = (
            self.usage_filter_combo.currentData()
            if hasattr(self, "usage_filter_combo")
            else "all"
        )
        if usage_filter == "favorites":
            candidates = [c for c in candidates if c.is_favorite]
        elif usage_filter == "recent":
            candidates = [
                c for c in candidates if c.recent_use_sequence is not None
            ]
        if (
            self._taxon_id is not None
            and self.only_this_taxon_checkbox.isChecked()
        ):
            target = self._taxon_id
            candidates = [
                c for c in candidates
                if str(getattr(c, "taxon_id", "") or "") == target
            ]
        query_raw = self.search_input.text() if hasattr(self, "search_input") else ""
        query = (query_raw or "").strip().casefold()
        if query:
            def _match(c: MeasurementSetCandidate) -> bool:
                for field_value in (
                    c.short_label,
                    c.name_as_published,
                    c.locator_text,
                    c.raw_text,
                    c.data_kind,
                ):
                    if field_value is None:
                        continue
                    if query in str(field_value).casefold():
                        return True
                return False

            candidates = [c for c in candidates if _match(c)]
        return candidates

    def _on_filter_changed(self, *_args) -> None:
        # Reset selection when the visible set changes so we do not
        # accept an id that is no longer visible.
        self._selected_id = None
        self._populate_table()
        self._update_accept_state()

    def _on_manage_library_clicked(self) -> None:
        """Open the normalized Reference Library manager as a child
        modal, refresh the candidate table when it closes, then emit
        :attr:`manage_library_requested` so external observers can run
        follow-up work (e.g. telemetry). The signal fires exactly once,
        AFTER the manager closes — this makes the lifecycle
        unambiguous: the chooser is the sole owner of the manager
        window, and consumers must NOT open a second manager in the
        signal slot.
        """
        try:  # pragma: no branch - import guard, no runtime alternative
            from .reference_library_manager_dialog import (
                ReferenceLibraryManagerDialog,
            )
        except Exception:
            # If the manager module cannot be imported, still notify
            # external observers so they can surface an error, but do
            # not crash the chooser.
            self.manage_library_requested.emit()
            return
        manager = ReferenceLibraryManagerDialog(self, active_observation_id=None)
        try:
            manager.exec()
        finally:
            self.refresh_candidates()
            manager.deleteLater()
        self.manage_library_requested.emit()

    def refresh_candidates(self) -> None:
        """Re-query the repository for attachment candidates and rebuild
        the table. External callers (e.g. MainWindow) should invoke this
        after the normalized library manager closes so newly-created
        measurement sets appear immediately without reopening the
        chooser. Preserves the exclusion set passed at construction and
        reapplies the active taxon-scope + text-search filters.
        """
        self._candidates = MeasurementSetRepository.list_attachment_candidates(
            exclude_ids=self._exclude_ids
        )
        self._selected_id = None
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
        visible = self._filtered_candidates()
        if not visible:
            self.table.setEnabled(False)
            # The "empty" message differentiates "no candidates in the
            # library at all" from "no candidates match the current
            # filters" so the user knows which knob to change.
            if not self._candidates:
                self._empty_label.setText(
                    self.tr(
                        "No reference measurement sets are available. Add reference "
                        "works to the library before attaching."
                    )
                )
            else:
                self._empty_label.setText(
                    self.tr(
                        "No measurement sets match the current filters. Clear "
                        "the search box or widen the taxon scope to see more."
                    )
                )
            self._empty_label.setVisible(True)
            return
        self.table.setEnabled(True)
        self._empty_label.setVisible(False)
        for candidate in visible:
            row = self.table.rowCount()
            self.table.insertRow(row)
            star = QTableWidgetItem("★" if candidate.is_favorite else "☆")
            star.setTextAlignment(Qt.AlignCenter)
            star.setData(Qt.UserRole, candidate.measurement_set_id)
            star.setToolTip(
                self.tr("Remove from favourites")
                if candidate.is_favorite
                else self.tr("Add to favourites")
            )
            star.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 0, star)
            source_item = QTableWidgetItem(candidate.short_label or "")
            source_item.setData(Qt.UserRole, candidate.measurement_set_id)
            source_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 1, source_item)
            taxon_item = QTableWidgetItem(candidate.name_as_published or "")
            taxon_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 2, taxon_item)
            locator_item = QTableWidgetItem(candidate.locator_text or "")
            locator_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 3, locator_item)
            kind_item = QTableWidgetItem(candidate.data_kind or "")
            kind_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 4, kind_item)
            raw_item = QTableWidgetItem(candidate.raw_text or "")
            raw_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self.table.setItem(row, 5, raw_item)

    def _toggle_favorite(self, measurement_set_id: str) -> None:
        candidate = next(
            (
                item
                for item in self._candidates
                if item.measurement_set_id == measurement_set_id
            ),
            None,
        )
        if candidate is None:
            return
        favorite = not candidate.is_favorite
        try:
            MeasurementSetPreferenceRepository.set_favorite(
                measurement_set_id, favorite
            )
        except ReferenceLibraryError as exc:
            self.refresh_candidates()
            QMessageBox.warning(
                self,
                self.tr("Reference favourites"),
                self.tr("Could not update favourite: {error}").format(
                    error=str(exc)
                ),
            )
            return
        self._candidates = [
            replace(item, is_favorite=favorite)
            if item.measurement_set_id == measurement_set_id
            else item
            for item in self._candidates
        ]
        self._candidates.sort(
            key=lambda item: (
                not item.is_favorite,
                item.recent_use_sequence is None,
                -(item.recent_use_sequence or 0),
                (item.short_label or "").casefold(),
                (item.name_as_published or "").casefold(),
                item.measurement_set_id,
            )
        )
        self._selected_id = None
        self._populate_table()
        self._update_accept_state()

    def _on_table_cell_clicked(self, row: int, column: int) -> None:
        if column != 0:
            return
        item = self.table.item(row, 0)
        if item is not None:
            self._toggle_favorite(str(item.data(Qt.UserRole) or ""))

    def _on_selection_changed(self) -> None:
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            self._selected_id = None
        else:
            row = selected_rows[0].row()
            item = self.table.item(row, 1)
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
