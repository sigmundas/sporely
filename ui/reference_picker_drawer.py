"""Picking references in a drawer over the Analysis plot (UI redesign stage 7).

``ReferencePickerPage`` is a page of a :class:`~ui.side_drawer.SideDrawer`.
It lists the reference library's measurement sets with a search box and
"Only this taxon" (on by default), grouped by relevance like the old
Add-reference dialog. Ticking a row attaches that set to the observation and
plots it at once; unticking detaches it (decided 2026-11-09). A ticked set is
attached as "Compared"; its expanded row changes the role.

The page holds no persistence of its own. Every change goes through the
``host`` (``MainWindow``), whose methods reuse the existing attach, detach and
role paths and refuse to act when the active observation is no longer the one
the page was loaded for.
"""
from __future__ import annotations

from typing import Protocol

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from database.reference_library import MeasurementSetCandidate, MeasurementSetRepository, ReferenceLibraryError
from references.reference_display import format_measurement_expression

from .add_reference_dialog import AddReferenceDialog, filter_library_candidates, group_library_candidates
from .library_source_row import DEFAULT_REFERENCE_USE_ROLE, LibraryResultsList, LibrarySourceRow
from .segmented_selector import SegmentedSelector


class ReferencePickerHost(Protocol):
    """What the page asks of its host (``MainWindow``)."""

    def reference_picker_context(self) -> dict | None: ...  # observation_id, taxon_id, genus, species
    def reference_picker_attached(self, observation_id: int) -> dict[str, str]: ...  # set id -> role
    def reference_picker_confirm_attach(self, observation_id: int) -> bool: ...
    def reference_picker_attach(self, observation_id: int, set_id: str, role: str) -> tuple[str, str | None]: ...
    def reference_picker_detach(self, observation_id: int, set_id: str) -> tuple[bool, str | None]: ...
    def reference_picker_set_role(self, observation_id: int, set_id: str, role: str) -> tuple[bool, str | None]: ...


class ReferencePickerPage(QWidget):
    # The less used sources and "New reference…" still open the old dialog
    # on that tab until stages 7c and 7d move them: "community",
    # "my_observations" or "new".
    other_source_requested = Signal(str)
    # "Details…" on a row: open this measurement set in the library.
    details_requested = Signal(object)  # MeasurementSetCandidate

    def __init__(self, host: ReferencePickerHost, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._host = host
        self._context: dict | None = None
        self._candidates: list[MeasurementSetCandidate] = []
        self._attached: dict[str, str] = {}
        self._expanded: set[str] = set()
        # The public-observation notice is asked on the first tick each time
        # the drawer opens (an attach on an already public observation).
        self._attach_confirmed = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 4, 12, 12)
        layout.setSpacing(8)

        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText(self.tr("Search publication, taxon or measurements…"))
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self._populate)
        layout.addWidget(self.search_input)

        source_row = QHBoxLayout()
        source_row.setSpacing(8)
        self.source_selector = SegmentedSelector(self, compact=True)
        self.source_selector.add_option(self.tr("Library"), "library", checked=True)
        self.source_selector.add_option(self.tr("Community"), "community")
        self.source_selector.selectionChanged.connect(self._on_source_changed)
        source_row.addWidget(self.source_selector, 0)
        self.my_observations_link = QPushButton(self.tr("My observations"), self)
        self.my_observations_link.setObjectName("linkButton")  # less prominent than the two above
        self.my_observations_link.setCursor(Qt.PointingHandCursor)
        self.my_observations_link.clicked.connect(lambda: self.other_source_requested.emit("my_observations"))
        source_row.addWidget(self.my_observations_link, 0)
        source_row.addStretch(1)
        layout.addLayout(source_row)

        self.only_this_taxon_checkbox = QCheckBox(self.tr("Only this taxon"), self)
        self.only_this_taxon_checkbox.setChecked(True)
        self.only_this_taxon_checkbox.toggled.connect(self._populate)
        layout.addWidget(self.only_this_taxon_checkbox)

        self.results_list = LibraryResultsList(self)
        self.results_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.results_list.setSelectionMode(LibraryResultsList.NoSelection)
        self.results_list.setFocusPolicy(Qt.NoFocus)
        self.results_list.setMinimumHeight(220)
        layout.addWidget(self.results_list, 1)

        self.status_label = QLabel("", self)
        self.status_label.setWordWrap(True)
        self.status_label.setVisible(False)
        layout.addWidget(self.status_label)

        self.new_reference_btn = QPushButton(self.tr("New reference…"), self)
        self.new_reference_btn.clicked.connect(lambda: self.other_source_requested.emit("new"))
        layout.addWidget(self.new_reference_btn, 0, Qt.AlignLeft)

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """Read the active observation, the library and what is attached; rebuild the list."""
        self._attach_confirmed = False
        self._set_status("")
        self._context = self._host.reference_picker_context()
        if self._context is None:
            self._candidates, self._attached = [], {}
            self._populate()
            self._set_status(self.tr("Select an observation first before adding a reference."))
            return
        try:
            self._candidates = MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())
        except ReferenceLibraryError as exc:
            self._candidates = []
            self._set_status(self.tr("Could not load the reference library: {error}").format(error=str(exc)))
        self._attached = dict(self._host.reference_picker_attached(int(self._context["observation_id"])))
        self._populate()

    def _taxon_name(self) -> str:
        context = self._context or {}
        return " ".join(part for part in (context.get("genus") or "", context.get("species") or "") if part).strip()

    def _visible_groups(self) -> list[tuple[str, list[MeasurementSetCandidate]]]:
        context = self._context or {}
        taxon_id = context.get("taxon_id")
        taxon_id = str(taxon_id) if taxon_id not in (None, "") else None
        filtered = filter_library_candidates(
            self._candidates,
            taxon_id=taxon_id,
            only_this_taxon=self.only_this_taxon_checkbox.isChecked(),
            query=self.search_input.text(),
            taxon_text=self._taxon_name(),
        )
        return group_library_candidates(
            filtered, taxon_id=taxon_id, genus=context.get("genus") or "", taxon_name=self._taxon_name(),
        )

    def _populate(self, *_args) -> None:
        self.results_list.clear()
        groups = self._visible_groups()
        for group, members in groups:
            heading = QListWidgetItem(AddReferenceDialog._library_group_heading(group, len(members)))
            heading.setFlags(Qt.NoItemFlags)
            font = QFont(self.results_list.font())
            font.setBold(True)
            heading.setFont(font)
            heading.setForeground(self.palette().mid())
            self.results_list.addItem(heading)
            for candidate in members:
                self._add_row(candidate, group)
        if not any(members for _group, members in groups) and self._context is not None:
            empty = QListWidgetItem(self._empty_text())
            empty.setFlags(Qt.NoItemFlags)
            self.results_list.addItem(empty)

    def _empty_text(self) -> str:
        if not self._candidates:
            return self.tr("The reference library has no measurement sets yet.")
        if self.only_this_taxon_checkbox.isChecked():
            return self.tr("No measurement sets for this taxon. Turn off “Only this taxon” to see the rest of the library.")
        return self.tr("No matching measurement sets in the library.")

    def _add_row(self, candidate: MeasurementSetCandidate, group: str) -> None:
        set_id = str(candidate.measurement_set_id)
        taxon = candidate.name_as_published or self.tr("Unnamed taxon")
        citation = candidate.short_label or candidate.work_title or self.tr("Untitled")
        if candidate.year and str(candidate.year) not in citation:
            citation = f"{citation} ({candidate.year})"
        summary = [line for line in (
            taxon,
            " · ".join(part for part in (candidate.short_label or candidate.work_title or "", candidate.locator_text or "") if part),
            str(candidate.raw_text or ""),
        ) if line]
        row = LibrarySourceRow(
            measurement_set_id=set_id,
            taxon=taxon,
            citation=citation,
            measurement=format_measurement_expression(candidate.source_display()),
            relevance_badge=AddReferenceDialog._relevance_badge_text(group),
            semantic_badge=AddReferenceDialog._semantic_badge_text(candidate),
            summary_lines=tuple(summary),
            parent=self.results_list,
        )
        row.set_checked_silently(set_id in self._attached)
        row.set_role_silently(self._attached.get(set_id, DEFAULT_REFERENCE_USE_ROLE))
        item = QListWidgetItem()
        item.setData(Qt.UserRole, set_id)
        item.setFlags(Qt.ItemIsEnabled)
        self.results_list.addItem(item)
        self.results_list.setItemWidget(item, row)
        if set_id in self._expanded:
            row.set_expanded(True)
        item.setSizeHint(row.sizeHint())
        row.check_toggled.connect(self._on_check_toggled)
        row.role_changed.connect(self._on_role_changed)
        row.expanded_changed.connect(lambda expanded, item=item, row=row: self._on_row_expanded(item, row, expanded))
        row.details_requested.connect(lambda _set_id, candidate=candidate: self.details_requested.emit(candidate))

    def _on_row_expanded(self, item: QListWidgetItem, row: LibrarySourceRow, expanded: bool) -> None:
        if expanded:
            self._expanded.add(row.measurement_set_id)
        else:
            self._expanded.discard(row.measurement_set_id)
        item.setSizeHint(row.sizeHint())

    def row_for(self, set_id: str) -> LibrarySourceRow | None:
        for index in range(self.results_list.count()):
            widget = self.results_list.itemWidget(self.results_list.item(index))
            if isinstance(widget, LibrarySourceRow) and widget.measurement_set_id == str(set_id):
                return widget
        return None

    # -- actions -------------------------------------------------------------

    def _on_source_changed(self, value) -> None:
        if value != "library":
            self.source_selector.set_selected_value("library")  # until stage 7c
            self.other_source_requested.emit(str(value))

    def _current_observation_id(self) -> int:
        return int((self._context or {}).get("observation_id") or 0)

    def _on_check_toggled(self, set_id: str, checked: bool) -> None:
        observation_id = self._current_observation_id()
        if not observation_id:
            row = self.row_for(set_id)
            if row is not None:
                row.set_checked_silently(not checked)
            return
        if checked:
            if not self._attach_confirmed:
                confirmed = self._host.reference_picker_confirm_attach(observation_id)
                # The notice runs a modal loop: the list may have been rebuilt
                # (another observation) meanwhile. Then act on nothing.
                if self._current_observation_id() != observation_id:
                    return
                if not confirmed:
                    row = self.row_for(set_id)
                    if row is not None:
                        row.set_checked_silently(False)
                    return
                self._attach_confirmed = True
            # A tick always attaches as "Compared" (decided 2026-11-09).
            status, reason = self._host.reference_picker_attach(observation_id, set_id, DEFAULT_REFERENCE_USE_ROLE)
            row = self.row_for(set_id)
            if status in ("attached", "already_attached"):
                attached = self._host.reference_picker_attached(observation_id)
                self._attached[set_id] = attached.get(set_id, DEFAULT_REFERENCE_USE_ROLE)
                if row is not None:
                    row.set_role_silently(self._attached[set_id])
                self._set_status(reason or "")
            else:
                if row is not None:
                    row.set_checked_silently(False)
                self._set_status(reason or self.tr("The reference could not be attached."))
        else:
            ok, reason = self._host.reference_picker_detach(observation_id, set_id)
            row = self.row_for(set_id)
            if ok:
                self._attached.pop(set_id, None)
                if row is not None:
                    row.set_role_silently(DEFAULT_REFERENCE_USE_ROLE)
                self._set_status("")
            else:
                if row is not None:
                    row.set_checked_silently(True)
                self._set_status(reason or self.tr("The reference could not be detached."))
        self._refresh_row_height(set_id)

    def sync_attached(self) -> None:
        """Follow attachments changed elsewhere (the comparison list, the dialog) without a rebuild."""
        observation_id = self._current_observation_id()
        if not observation_id:
            return
        self._attached = dict(self._host.reference_picker_attached(observation_id))
        for index in range(self.results_list.count()):
            row = self.results_list.itemWidget(self.results_list.item(index))
            if isinstance(row, LibrarySourceRow):
                set_id = row.measurement_set_id
                row.set_checked_silently(set_id in self._attached)
                row.set_role_silently(self._attached.get(set_id, DEFAULT_REFERENCE_USE_ROLE))
                self._refresh_row_height(set_id)

    def _on_role_changed(self, set_id: str, role: str) -> None:
        if set_id not in self._attached:
            return
        observation_id = self._current_observation_id()
        ok, reason = self._host.reference_picker_set_role(observation_id, set_id, role)
        if ok:
            self._attached[set_id] = role
            self._set_status("")
        else:
            row = self.row_for(set_id)
            if row is not None:
                row.set_role_silently(self._attached[set_id])
            self._set_status(reason or self.tr("The role could not be changed."))

    def _refresh_row_height(self, set_id: str) -> None:
        for index in range(self.results_list.count()):
            item = self.results_list.item(index)
            widget = self.results_list.itemWidget(item)
            if isinstance(widget, LibrarySourceRow) and widget.measurement_set_id == set_id:
                item.setSizeHint(widget.sizeHint())

    def _set_status(self, text: str) -> None:
        self.status_label.setText(text)
        self.status_label.setVisible(bool(text))
