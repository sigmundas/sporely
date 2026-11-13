"""Picking references in a drawer over the Analysis plot (UI redesign stage 7).

``ReferencePickerPage`` is a page of a :class:`~ui.side_drawer.SideDrawer`.
It lists the reference library's measurement sets with a search box and
"Only this taxon" (on by default), grouped by relevance like the old
Add-reference dialog. Ticking a row attaches that set to the observation and
plots it at once; unticking detaches it (decided 2026-11-09). A ticked set is
attached as "Compared"; its expanded row changes the role.

The other two sources tick the same way (stage 7c). "Community" reuses
``CommunityResultsPane`` in its checkable mode: a tick loads that dataset's
detail from Sporely Cloud and then plots it; "My observations" plots another
of the user's observations of the taxon. Neither is an attachment: they are
plot series, as from the old dialog, and unticking removes them.

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
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from database.reference_library import MeasurementSetCandidate, MeasurementSetRepository, ReferenceLibraryError
from references.reference_display import format_measurement_expression

from .add_reference_dialog import (
    PersonalObservationCandidate,
    filter_library_candidates,
    group_library_candidates,
    library_group_heading,
    relevance_badge_text,
    semantic_badge_text,
)
from .library_source_row import DEFAULT_REFERENCE_USE_ROLE, LibraryResultsList, LibrarySourceRow
from .segmented_selector import SegmentedSelector
from .two_line_row import CheckableTwoLineRow


class ReferencePickerHost(Protocol):
    """What the page asks of its host (``MainWindow``)."""

    def reference_picker_context(self) -> dict | None: ...  # observation_id, taxon_id, genus, species
    def reference_picker_attached(self, observation_id: int) -> dict[str, str]: ...  # set id -> role
    def reference_picker_confirm_attach(self, observation_id: int) -> bool: ...
    def reference_picker_attach(self, observation_id: int, set_id: str, role: str) -> tuple[str, str | None]: ...
    def reference_picker_detach(self, observation_id: int, set_id: str) -> tuple[bool, str | None]: ...
    def reference_picker_set_role(self, observation_id: int, set_id: str, role: str) -> tuple[bool, str | None]: ...
    # Plot series that are not attachments (Community, My observations).
    def reference_picker_series_keys(self, observation_id: int) -> set: ...
    def reference_picker_add_series(self, observation_id: int, data: dict) -> object | None: ...  # its key
    def reference_picker_remove_series(self, observation_id: int, key) -> bool: ...
    def reference_picker_my_observations(self, context: dict) -> list[PersonalObservationCandidate]: ...
    def reference_picker_observation_series(self, context: dict, source_observation_id: int) -> dict | None: ...
    def reference_picker_series_key(self, data: dict) -> object | None: ...


class ReferencePickerPage(QWidget):
    # "New reference…": the host opens the two-step create dialog (stage 7d).
    new_reference_requested = Signal()
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
        self._source = "library"
        # Community: result identity -> plot-series key, for this session
        # (a dataset's key is only known once its detail has loaded), and the
        # one tick still waiting for its detail: (observation id, index, identity).
        self.community_pane = None
        self._community_stale = True
        # Per observation, for this session: switching away and back keeps them.
        self._community_keys_by_observation: dict[int, dict[str, object]] = {}
        self._community_keys: dict[str, object] = {}
        self._community_pending: tuple[int, int, str] | None = None
        # My observations: candidates and their plot-series keys.
        self._my_observations: list[PersonalObservationCandidate] = []
        self._my_observation_keys: dict[int, object] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 4, 12, 12)
        layout.setSpacing(8)

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
        self.my_observations_link.clicked.connect(lambda: self.show_source("my_observations"))
        source_row.addWidget(self.my_observations_link, 0)
        source_row.addStretch(1)
        layout.addLayout(source_row)

        self.pages = QStackedWidget(self)
        layout.addWidget(self.pages, 1)

        # Library
        self.library_page = QWidget(self.pages)
        library_layout = QVBoxLayout(self.library_page)
        library_layout.setContentsMargins(0, 0, 0, 0)
        library_layout.setSpacing(8)
        self.search_input = QLineEdit(self.library_page)
        self.search_input.setPlaceholderText(self.tr("Search publication, taxon or measurements…"))
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self._populate)
        library_layout.addWidget(self.search_input)
        self.only_this_taxon_checkbox = QCheckBox(self.tr("Only this taxon"), self.library_page)
        self.only_this_taxon_checkbox.setChecked(True)
        self.only_this_taxon_checkbox.toggled.connect(self._populate)
        library_layout.addWidget(self.only_this_taxon_checkbox)
        self.results_list = LibraryResultsList(self.library_page)
        self.results_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.results_list.setSelectionMode(LibraryResultsList.NoSelection)
        self.results_list.setFocusPolicy(Qt.NoFocus)
        self.results_list.setMinimumHeight(220)
        library_layout.addWidget(self.results_list, 1)
        self.pages.addWidget(self.library_page)

        # Community: built when first shown (its pane searches Sporely Cloud).
        self.community_page = QWidget(self.pages)
        self._community_layout = QVBoxLayout(self.community_page)
        self._community_layout.setContentsMargins(0, 0, 0, 0)
        self.pages.addWidget(self.community_page)

        # My observations
        self.my_observations_page = QWidget(self.pages)
        my_layout = QVBoxLayout(self.my_observations_page)
        my_layout.setContentsMargins(0, 0, 0, 0)
        my_layout.setSpacing(8)
        self.my_observations_list = QListWidget(self.my_observations_page)
        self.my_observations_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.my_observations_list.setSelectionMode(QListWidget.NoSelection)
        self.my_observations_list.setFocusPolicy(Qt.NoFocus)
        my_layout.addWidget(self.my_observations_list, 1)
        self.pages.addWidget(self.my_observations_page)

        self.status_label = QLabel("", self)
        self.status_label.setWordWrap(True)
        self.status_label.setVisible(False)
        layout.addWidget(self.status_label)

        self.new_reference_btn = QPushButton(self.tr("New reference…"), self)
        self.new_reference_btn.clicked.connect(lambda: self.new_reference_requested.emit())
        layout.addWidget(self.new_reference_btn, 0, Qt.AlignLeft)

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """Read the active observation, the library and what is attached; rebuild the lists."""
        self._attach_confirmed = False
        if self._community_pending is not None and self.community_pane is not None:
            # A tick still loading is dropped, so it must not stay ticked.
            self.community_pane.set_result_checked(self._community_pending[1], False)
        self._community_pending = None
        self._set_status("")
        previous = self._current_observation_id()
        self._context = self._host.reference_picker_context()
        if self._current_observation_id() != previous:
            # Another observation: its own plot series, and its own taxon to search.
            self._community_keys = self._community_keys_by_observation.setdefault(self._current_observation_id(), {})
            self._community_stale = True
        self._load_my_observations()
        if self._source == "community":
            self._refresh_community()
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
            heading = QListWidgetItem(library_group_heading(group, len(members)))
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
            relevance_badge=relevance_badge_text(group),
            semantic_badge=semantic_badge_text(candidate),
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

    # -- sources -------------------------------------------------------------

    def current_source(self) -> str:
        return self._source

    def _on_source_changed(self, value) -> None:
        self.show_source(str(value))

    def show_source(self, source: str) -> None:
        """Show "library", "community" or "my_observations"."""
        self._source = source
        self._set_status("")
        if source == "community":
            self.source_selector.set_selected_value("community")
            self._refresh_community()
            self.pages.setCurrentWidget(self.community_page)
        elif source == "my_observations":
            self.source_selector.clear_selection()
            self.pages.setCurrentWidget(self.my_observations_page)
        else:
            self._source = "library"
            self.source_selector.set_selected_value("library")
            self.pages.setCurrentWidget(self.library_page)
        self.my_observations_link.setProperty("active", self._source == "my_observations")
        self.my_observations_link.style().unpolish(self.my_observations_link)
        self.my_observations_link.style().polish(self.my_observations_link)

    # -- Community -------------------------------------------------------------

    def _refresh_community(self) -> None:
        context = self._context or {}
        if self.community_pane is None:
            from .cloud_reference_dialog import CommunityResultsPane

            injected = getattr(self._host, "reference_picker_community_results", None)
            self.community_pane = CommunityResultsPane(
                self.community_page,
                genus=context.get("genus") or "",
                species=context.get("species") or "",
                results=injected() if callable(injected) else None,
                exclude_observation_cloud_id=context.get("cloud_id"),
                checkable=True,
            )
            self.community_pane.results_list.setSelectionMode(QListWidget.SingleSelection)
            self.community_pane.result_check_toggled.connect(self._on_community_toggled)
            self.community_pane.selection_changed.connect(self._finish_community_tick)
            self.community_pane.results_changed.connect(self._sync_community_ticks)
            self._community_layout.addWidget(self.community_pane, 1)
            self._community_stale = False
            self._sync_community_ticks()
        elif self._community_stale:
            self.community_pane._exclude_observation_cloud_id = context.get("cloud_id") or None
            self.community_pane.set_taxon(context.get("genus") or "", context.get("species") or "")
            self._community_stale = False

    def _sync_community_ticks(self) -> None:
        pane = self.community_pane
        if pane is None:
            return
        plotted = self._host.reference_picker_series_keys(self._current_observation_id()) if self._current_observation_id() else set()
        pending_index = self._community_pending[1] if self._community_pending is not None else None
        for index in range(pane.result_count()):
            if index == pending_index:
                continue  # still loading: its tick stands
            key = self._community_keys.get(pane.result_identity(index) or "")
            pane.set_result_checked(index, key is not None and key in plotted)

    def _on_community_toggled(self, index: int, checked: bool) -> None:
        pane = self.community_pane
        observation_id = self._current_observation_id()
        identity = pane.result_identity(index) if pane is not None else None
        if pane is None or not observation_id or identity is None:
            if pane is not None:
                pane.set_result_checked(index, not checked)
            return
        if checked:
            if self._community_pending is not None and self._community_pending[1] != index:
                pane.set_result_checked(self._community_pending[1], False)  # superseded before it loaded
            self._community_pending = (observation_id, index, identity)
            self._set_status(self.tr("Loading the dataset…"))
            if pane.selected_index() == index:
                self._finish_community_tick()
            else:
                pane.select_result(index)  # loads its detail; selection_changed when it has
            return
        if self._community_pending is not None and self._community_pending[1] == index:
            self._community_pending = None
            self._set_status("")
            return
        key = self._community_keys.get(identity)
        if key is None or self._host.reference_picker_remove_series(observation_id, key):
            self._community_keys.pop(identity, None)
            self._set_status("")
        else:
            pane.set_result_checked(index, True)
            self._set_status(self.tr("The reference could not be removed from the plot."))

    def _finish_community_tick(self) -> None:
        pending = self._community_pending
        pane = self.community_pane
        if pending is None or pane is None:
            return
        observation_id, index, identity = pending
        if self._current_observation_id() != observation_id or pane.result_identity(index) != identity:
            self._community_pending = None  # another observation or another search since
            return
        if pane.selected_index() != index:
            # Another row was selected before this one's detail arrived: the
            # tick is abandoned rather than left hanging.
            self._community_pending = None
            pane.set_result_checked(index, False)
            self._set_status("")
            return
        error = pane.detail_error()
        if error:
            self._community_pending = None
            pane.set_result_checked(index, False)
            self._set_status(error)
            return
        if not pane.has_selection():
            return  # still loading
        self._community_pending = None
        payload = pane.current_mode_payload()
        key = self._host.reference_picker_add_series(observation_id, payload) if payload else None
        if key is None:
            pane.set_result_checked(index, False)
            self._set_status(self.tr("The reference could not be added to the plot."))
            return
        # The plot holds one series per key: a result with the same key
        # (same taxon, source and preparation) was just replaced by this one.
        for other_identity, other_key in list(self._community_keys.items()):
            if other_key == key and other_identity != identity:
                del self._community_keys[other_identity]
        self._community_keys[identity] = key
        self._sync_community_ticks()
        # The plot refresh may have let a sync run before the key was known.
        pane.set_result_checked(index, True)
        self._set_status("")

    def shutdown(self) -> None:
        """Stop the Community pane's search and detail workers (the window is closing)."""
        if self.community_pane is not None:
            self.community_pane.close()

    # -- My observations -------------------------------------------------------

    def _load_my_observations(self) -> None:
        self.my_observations_list.clear()
        self._my_observations = []
        self._my_observation_keys = {}
        context = self._context
        if context is None:
            return
        self._my_observations = list(self._host.reference_picker_my_observations(context))
        plotted = self._host.reference_picker_series_keys(int(context["observation_id"]))
        for candidate in self._my_observations:
            data = self._host.reference_picker_observation_series(context, candidate.observation_id)
            key = self._host.reference_picker_series_key(data) if data else None
            label = (
                self.tr("My observation — {author}").format(author=candidate.author)
                if candidate.author else self.tr("My observation")
            )
            detail = " · ".join(part for part in (
                candidate.date, self.tr("n = {count}").format(count=candidate.n), candidate.location,
            ) if part)
            row = CheckableTwoLineRow(label, detail, self.my_observations_list)
            row.set_checked_silently(key is not None and key in plotted)
            row.toggled.connect(lambda checked, oid=candidate.observation_id: self._on_my_observation_toggled(oid, checked))
            item = QListWidgetItem()
            item.setData(Qt.UserRole, candidate.observation_id)
            item.setToolTip(f"{label}\n{detail}")
            self.my_observations_list.addItem(item)
            item.setSizeHint(row.sizeHint())
            self.my_observations_list.setItemWidget(item, row)
            if key is not None:
                self._my_observation_keys[candidate.observation_id] = key
        if not self._my_observations:
            empty = QListWidgetItem(self.tr("No previous observations of this taxon have spore measurements."))
            empty.setFlags(Qt.NoItemFlags)
            self.my_observations_list.addItem(empty)

    def my_observation_row(self, observation_id: int) -> CheckableTwoLineRow | None:
        for index in range(self.my_observations_list.count()):
            item = self.my_observations_list.item(index)
            if item.data(Qt.UserRole) == observation_id:
                widget = self.my_observations_list.itemWidget(item)
                return widget if isinstance(widget, CheckableTwoLineRow) else None
        return None

    def _on_my_observation_toggled(self, source_observation_id: int, checked: bool) -> None:
        observation_id = self._current_observation_id()
        row = self.my_observation_row(source_observation_id)
        if not observation_id:
            if row is not None:
                row.set_checked_silently(not checked)
            return
        if checked:
            data = self._host.reference_picker_observation_series(self._context or {}, source_observation_id)
            key = self._host.reference_picker_add_series(observation_id, data) if data else None
            if key is None:
                if row is not None:
                    row.set_checked_silently(False)
                self._set_status(self.tr("This observation has no usable spore measurements."))
                return
            self._my_observation_keys[source_observation_id] = key
            row = self.my_observation_row(source_observation_id)
            if row is not None:
                row.set_checked_silently(True)  # a sync during the plot refresh may have run first
            self._set_status("")
            return
        key = self._my_observation_keys.get(source_observation_id)
        if key is None or self._host.reference_picker_remove_series(observation_id, key):
            self._set_status("")
        else:
            if row is not None:
                row.set_checked_silently(True)
            self._set_status(self.tr("The reference could not be removed from the plot."))

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
        """Follow attachments and plot series changed elsewhere (the comparison list, the dialog) without a rebuild."""
        observation_id = self._current_observation_id()
        if not observation_id:
            return
        plotted = self._host.reference_picker_series_keys(observation_id)
        for source_id, key in self._my_observation_keys.items():
            row = self.my_observation_row(source_id)
            if row is not None:
                row.set_checked_silently(key in plotted)
        self._sync_community_ticks()
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
