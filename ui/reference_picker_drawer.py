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

"Compare with" (stage 7e, owner 2026-11-14) sets which taxon the three
sources look for: the observation's own by default, or another one picked
with the taxon picker, from its AI suggestions or by name. It changes only
what the drawer searches and what "New reference…" files the reference
under, never the observation's identification. It is kept while the same
observation is shown, and goes back to the observation's own taxon when
another one is.
"""
from __future__ import annotations

from html import escape
from typing import Protocol

from PySide6.QtCore import QEvent, Qt, Signal
from PySide6.QtGui import QFont, QPalette
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
from database.taxon_suggestions import ai_prediction_external_id, reference_default_suggestions
from references.reference_comparison import (
    ObservationBaseline,
    build_domains,
    comparison_view,
    observation_baseline_from_points,
)
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
from .reference_range_bars import ReferenceRangeBars
from .segmented_selector import SegmentedSelector
from .taxon_picker import AiSuggestion, TaxonPicker
from .two_line_row import CheckableTwoLineRow


def _ai_prediction_score(prediction: dict) -> float | None:
    for key in ("probability", "combined_score", "vision_score", "score", "frequency_score"):
        value = prediction.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _ai_scientific_name(taxon: dict) -> tuple[str, str]:
    """``(genus, species)`` of an AI prediction's taxon; species is empty for a genus."""
    genus = str(taxon.get("genus") or taxon.get("genusName") or taxon.get("genus_name") or "").strip()
    species = str(
        taxon.get("species") or taxon.get("specificEpithet") or taxon.get("specific_epithet") or ""
    ).strip()
    if genus and species:
        return genus, species
    name = ""
    for key in ("scientificName", "scientific_name", "name"):
        name = " ".join(str(taxon.get(key) or "").split())
        if name:
            break
    parts = name.split()
    # A binomial: a capitalised genus and a lower-case epithet (authorities follow).
    if len(parts) >= 2 and parts[0][:1].isupper() and parts[1][:1].islower():
        return parts[0], parts[1]
    return (parts[0], "") if len(parts) == 1 and parts[0][:1].isupper() else ("", "")


def reference_ai_entries(ai_state: dict | None) -> list[dict]:
    """The observation's AI predictions as entries for ``reference_default_suggestions``.

    ``ai_state`` is the observation's stored AI state (Artsorakel under
    ``predictions``, iNaturalist under ``inat_predictions``, each by image).
    Only names and scores are read; nothing is looked up or written.
    """
    from .observations_tab import _normalize_ai_prediction_taxon

    entries: list[dict] = []
    if not isinstance(ai_state, dict):
        return entries
    for source, key in (("arts", "predictions"), ("inat", "inat_predictions")):
        by_image = ai_state.get(key)
        if not isinstance(by_image, dict):
            continue
        for predictions in by_image.values():
            for prediction in predictions if isinstance(predictions, (list, tuple)) else ():
                if not isinstance(prediction, dict):
                    continue
                try:
                    taxon = _normalize_ai_prediction_taxon(prediction, source=source) or {}
                except Exception:
                    taxon = dict(prediction)
                genus, species = _ai_scientific_name(taxon)
                if not genus:
                    continue
                entries.append({
                    "source": source,
                    "genus": genus,
                    "species": species,
                    "scientific_name": " ".join(part for part in (genus, species) if part),
                    "score": _ai_prediction_score(prediction),
                    "external_id": ai_prediction_external_id(prediction, taxon),
                })
    return entries


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
    # "Compare with": the names database for the taxon picker (None without
    # one) and the observation's stored AI predictions (reference_ai_entries).
    def reference_picker_taxon_lookup(self) -> object | None: ...
    def reference_picker_ai_entries(self, context: dict) -> list[dict]: ...
    # The observation's own spore measurements, for the range bars.
    def reference_picker_observation_points(self, observation_id: int) -> list[dict]: ...


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
        # The range bars: the observation's own spores, and the axes every
        # row shares (derived once per observation and target, then frozen
        # while the list is searched or filtered).
        self._baseline = ObservationBaseline()
        self._bar_domains: dict | None = None
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
        # "Compare with": None is the observation's own taxon; otherwise the
        # observation it was chosen for and the taxon (taxon_id, genus,
        # species, rank).
        self._target: dict | None = None
        self.target_picker: TaxonPicker | None = None
        self._target_lookup = None  # the picker's names database, opened on first use

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 4, 12, 12)
        layout.setSpacing(8)

        target_row = QHBoxLayout()
        target_row.setSpacing(8)
        self.target_label = QLabel("", self)
        self.target_label.setObjectName("referenceTargetLabel")
        self.target_label.setTextFormat(Qt.RichText)
        self.target_label.setWordWrap(True)
        target_row.addWidget(self.target_label, 1)
        self.change_target_btn = QPushButton(self.tr("Change…"), self)
        self.change_target_btn.setObjectName("linkButton")
        self.change_target_btn.setCursor(Qt.PointingHandCursor)
        self.change_target_btn.setToolTip(self.tr(
            "Compare with another taxon: one of the AI suggestions, or any name. "
            "The observation's identification is not changed."
        ))
        self.change_target_btn.clicked.connect(self.begin_target_change)
        target_row.addWidget(self.change_target_btn, 0, Qt.AlignTop)
        self.reset_target_btn = QPushButton(self.tr("Reset"), self)
        self.reset_target_btn.setObjectName("linkButton")
        self.reset_target_btn.setCursor(Qt.PointingHandCursor)
        self.reset_target_btn.clicked.connect(self.reset_target)
        self.reset_target_btn.setVisible(False)
        target_row.addWidget(self.reset_target_btn, 0, Qt.AlignTop)
        layout.addLayout(target_row)
        # The taxon picker goes here while the target is being changed.
        self._target_layout = QVBoxLayout()
        self._target_layout.setContentsMargins(0, 0, 0, 0)
        layout.addLayout(self._target_layout)

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
        if self._current_observation_id() != previous or self._context is None:
            # A change begun for another observation ends with it.
            self._end_target_change()
        if self._target is not None and self._target.get("observation_id") != self._current_observation_id():
            self._target = None  # chosen for another observation
        self._refresh_target_line()
        if self._current_observation_id() != previous:
            # Another observation: its own plot series, and its own taxon to search.
            self._community_keys = self._community_keys_by_observation.setdefault(self._current_observation_id(), {})
            self._community_stale = True
        self._load_my_observations()
        if self._source == "community":
            self._refresh_community()
        self._bar_domains = None
        self._baseline = self._load_baseline()
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

    def _load_baseline(self) -> ObservationBaseline:
        observation_id = self._current_observation_id()
        loader = getattr(self._host, "reference_picker_observation_points", None)
        if not observation_id or not callable(loader):
            return ObservationBaseline()
        try:
            return observation_baseline_from_points(loader(observation_id) or [])
        except Exception:
            return ObservationBaseline()

    def _range_bar_domains(self) -> dict:
        """The axes all rows share: the observation's spores and the sets
        of this taxon and its genus, whatever the search or "Only this taxon"
        shows, so the bars do not move while the list is filtered. A set far
        outside them is drawn clamped, with a chevron at that end."""
        if self._bar_domains is None:
            context = self._target_context()
            taxon_id = context.get("taxon_id")
            groups = group_library_candidates(
                self._candidates,
                taxon_id=str(taxon_id) if taxon_id not in (None, "") else None,
                genus=context.get("genus") or "",
                taxon_name=self._taxon_name(),
            )
            near = [c for group, members in groups if group != "rest" for c in members]
            self._bar_domains = build_domains(
                baseline=self._baseline,
                displays=[c.source_display() for c in near],
                extents=[c.raw_point_extents for c in near if c.raw_point_extents],
            )
        return self._bar_domains

    def _range_bars(self, candidate: MeasurementSetCandidate) -> ReferenceRangeBars:
        bars = ReferenceRangeBars()
        self._show_range_bars(bars, candidate)
        return bars

    def _show_range_bars(self, bars: ReferenceRangeBars, candidate: MeasurementSetCandidate) -> None:
        domains = self._range_bar_domains()
        bars.set_view(comparison_view(domains=domains, baseline=self._baseline, display=candidate.source_display()))
        axes = self.tr("Axes: length {length}, width {width} µm.").format(
            length=f"{domains['length'].low:.1f}–{domains['length'].high:.1f}",
            width=f"{domains['width'].low:.1f}–{domains['width'].high:.1f}",
        )
        if self._baseline.has_measurements:
            legend = self.tr(
                "Length (L) and width (W). Blue, above the line: this reference. "
                "Green, below: your spores (n = {count}), min–max with the 5–95% range filled."
            ).format(count=self._baseline.point_count)
        else:
            legend = self.tr(
                "Length (L) and width (W) of this reference. "
                "This observation has no spore measurements to compare yet."
            )
        bars.setToolTip(f"{legend}\n{axes}")

    def shown_observation_id(self) -> int:
        """The observation the page was last loaded for (0 for none)."""
        return self._current_observation_id()

    def refresh_range_bars(self) -> None:
        """Re-read the observation's spores and redraw the rows' bars in
        place (the list, its scroll position and expanded rows stay), for
        spores measured since the page was loaded. The axes are rebuilt only
        when the spores changed."""
        baseline = self._load_baseline()
        if baseline == self._baseline:
            return
        self._baseline = baseline
        self._bar_domains = None
        by_id = {str(c.measurement_set_id): c for c in self._candidates}
        for index in range(self.results_list.count()):
            row = self.results_list.itemWidget(self.results_list.item(index))
            candidate = by_id.get(row.measurement_set_id) if isinstance(row, LibrarySourceRow) else None
            if candidate is not None and isinstance(row.range_bars, ReferenceRangeBars):
                self._show_range_bars(row.range_bars, candidate)

    def _target_context(self) -> dict:
        """The observation's context with "Compare with"'s taxon in place of its own."""
        context = dict(self._context or {})
        if self._target is not None and self._context is not None:
            for key in ("taxon_id", "genus", "species"):
                context[key] = self._target.get(key)
        return context

    def _taxon_name(self) -> str:
        context = self._target_context()
        return " ".join(part for part in (context.get("genus") or "", context.get("species") or "") if part).strip()

    def _visible_groups(self) -> list[tuple[str, list[MeasurementSetCandidate]]]:
        context = self._target_context()
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
            range_bars=self._range_bars(candidate),
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
        context = self._target_context()
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
        self._my_observations = list(self._host.reference_picker_my_observations(self._target_context()))
        plotted = self._host.reference_picker_series_keys(int(context["observation_id"]))
        for candidate in self._my_observations:
            data = self._host.reference_picker_observation_series(
                self._series_context(candidate), candidate.observation_id
            )
            key = self._host.reference_picker_series_key(data) if data else None
            label = (
                self.tr("My observation — {author}").format(author=candidate.author)
                if candidate.author else self.tr("My observation")
            )
            # Another target can list other species (a genus, say): name them.
            name = (
                " ".join(part for part in (candidate.genus, candidate.species) if part)
                if self._target is not None else ""
            )
            detail = " · ".join(part for part in (
                name, candidate.date, self.tr("n = {count}").format(count=candidate.n), candidate.location,
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

    def _series_context(self, candidate: PersonalObservationCandidate | None) -> dict:
        """The context a My observations series is labelled with: the
        candidate's own name. The same whatever "Compare with" is, so a
        series plotted under one target is found again under another (its
        plot key includes the name); a genus lists several species."""
        context = self._target_context()
        if candidate is not None and candidate.genus:
            context["genus"], context["species"] = candidate.genus, candidate.species
        return context

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
            candidate = next(
                (c for c in self._my_observations if c.observation_id == source_observation_id), None
            )
            data = self._host.reference_picker_observation_series(
                self._series_context(candidate), source_observation_id
            )
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

    # -- Compare with ----------------------------------------------------------

    def observation_shown(self, observation_id) -> None:
        """The host showed an observation, with the drawer open or not: a
        target chosen for another one is dropped (A, then B, then A again
        compares with A's own taxon)."""
        try:
            shown = int(observation_id or 0)
        except (TypeError, ValueError):
            shown = 0
        if shown != self._current_observation_id():
            self._end_target_change()  # begun for the observation the page still shows
        if self._target is not None and self._target.get("observation_id") != shown:
            self._target = None
            self._end_target_change()
            self._community_stale = True  # it searched for the dropped target
            if self._context is not None and self._current_observation_id() == shown:
                self._apply_target()

    def comparison_target(self) -> dict | None:
        """The taxon chosen in "Compare with" for the shown observation
        (``observation_id``, ``taxon_id``, ``genus``, ``species``, ``rank``),
        or None while it is the observation's own."""
        if self._target is None or self._target.get("observation_id") != self._current_observation_id():
            return None
        return dict(self._target)

    def _refresh_target_line(self) -> None:
        context = self._context
        if context is None:
            self.target_label.setText("")
            self.change_target_btn.setEnabled(False)
            self.reset_target_btn.setVisible(False)
            self.new_reference_btn.setEnabled(True)
            self.new_reference_btn.setToolTip("")
            return
        own = " ".join(part for part in (context.get("genus") or "", context.get("species") or "") if part)
        if self._target is None:
            # Normally the observation's own taxon goes without saying.
            name = own
            note = "" if own else self.tr("the observation has no taxon")
        else:
            name = " ".join(part for part in (self._target.get("genus") or "", self._target.get("species") or "") if part)
            note = ""
        text = self.tr("Compare with {taxon}").format(taxon=f"<i>{escape(name)}</i>") if name else self.tr("Compare with")
        if note:
            dim = self.palette().color(QPalette.PlaceholderText).name()
            text += f" <span style='color: {dim}'>· {escape(note)}</span>"
        self.target_label.setText(text)
        self.change_target_btn.setEnabled(True)
        self.reset_target_btn.setVisible(self._target is not None)
        self.reset_target_btn.setToolTip(
            self.tr("Compare with the observation's taxon ({taxon}) again.").format(taxon=own)
            if own else self.tr("Compare with the observation's taxon again.")
        )
        # A new reference is filed under the target: under another taxon it
        # needs a species from the names database.
        target = self._target
        allowed = target is None or (target.get("taxon_id") is not None and bool(target.get("species")))
        self.new_reference_btn.setEnabled(allowed)
        self.new_reference_btn.setToolTip("" if allowed else self.tr(
            "Choose a species from the list in “Change…” to add a reference for it."
        ))

    def begin_target_change(self) -> None:
        """Show the taxon picker, with the observation's AI suggestions under it."""
        context = self._context
        if context is None:
            return
        # Asked each time: the host's lookup follows the names language.
        self._target_lookup = self._host.reference_picker_taxon_lookup()
        if self.target_picker is None:
            picker = TaxonPicker(self._target_lookup, self)
            picker.setObjectName("referenceTargetPicker")
            picker.concept_picked.connect(self._on_target_concept)
            picker.free_text_committed.connect(self._on_target_free_text)
            picker.search_input.installEventFilter(self)
            self._target_layout.addWidget(picker)
            self.target_picker = picker
        picker = self.target_picker
        picker.set_lookup(self._target_lookup)
        picker.ai_label.setText(self.tr("AI suggestions:"))
        picker.set_ai_suggestions(self._ai_suggestions())
        own_id = context.get("taxon_id")
        try:
            picker.set_current_taxon_id(int(own_id) if own_id not in (None, "") else None)
        except (TypeError, ValueError):
            picker.set_current_taxon_id(None)
        picker.clear_display()
        picker.setVisible(True)
        self.change_target_btn.setVisible(False)
        picker.search_input.setFocus(Qt.OtherFocusReason)

    def _ai_suggestions(self) -> list[AiSuggestion]:
        context = self._context or {}
        try:
            entries = list(self._host.reference_picker_ai_entries(context))
        except Exception:
            entries = []
        if not entries:
            return []
        kwargs = dict(
            own_genus=context.get("genus") or "",
            own_species=context.get("species") or "",
            own_taxon_id=context.get("taxon_id"),
            ai_entries=entries,
        )
        try:
            defaults = reference_default_suggestions(self._target_lookup, **kwargs)
        except Exception:
            defaults = reference_default_suggestions(None, **kwargs)
        suggestions = []
        for row in defaults.ai:
            scores = [score for _service, score in row.ai_scores if score is not None] or (
                [row.score] if row.score is not None else []
            )
            suggestions.append(AiSuggestion(
                label=row.scientific_name,
                scientific_name=row.scientific_name,
                score=max(scores) if scores else None,
                taxon_id=row.sporely_taxon_id,
            ))
        suggestions.sort(key=lambda s: -(s.score or 0.0))
        return suggestions

    def _on_target_concept(self, row, concept) -> None:
        name = " ".join(str(concept.scientific_name or row.name or "").split())
        genus, _space, species = name.partition(" ")
        self._set_target(
            taxon_id=int(concept.sporely_taxon_id), genus=genus, species=species, rank=str(row.rank or ""),
        )

    def _on_target_free_text(self, text: str) -> None:
        genus, _space, species = " ".join(str(text or "").split()).partition(" ")
        self._set_target(taxon_id=None, genus=genus, species=species, rank="")

    def _set_target(self, *, taxon_id, genus: str, species: str, rank: str) -> None:
        self._end_target_change()
        if self._context is None or not genus:
            return
        own_id = self._context.get("taxon_id")
        # The observation's own concept, or its own name used as typed.
        own = (
            (taxon_id is not None and own_id not in (None, "") and str(taxon_id) == str(own_id))
            or (
                taxon_id is None
                and (genus.casefold(), species.casefold())
                == (str(self._context.get("genus") or "").casefold(), str(self._context.get("species") or "").casefold())
            )
        )
        if own:
            self._target = None
        else:
            self._target = {
                "observation_id": self._current_observation_id(),
                "taxon_id": taxon_id,
                "genus": genus,
                "species": species,
                "rank": rank,
            }
        self._apply_target()

    def reset_target(self) -> None:
        """Back to the observation's own taxon."""
        self._end_target_change()
        if self._target is None:
            return
        self._target = None
        self._apply_target()

    def _apply_target(self) -> None:
        """Search the sources again for the target that was just set."""
        self._refresh_target_line()
        self._set_status("")
        self._bar_domains = None  # another taxon's sets size the axes
        self._populate()
        self._load_my_observations()
        self._community_stale = True
        if self._source == "community":
            self._refresh_community()

    def _end_target_change(self) -> None:
        picker = self.target_picker
        # isHidden, not isVisible: the drawer may be closed.
        if picker is not None and not picker.isHidden():
            had_focus = picker.isAncestorOf(self.focusWidget()) if self.focusWidget() else False
            picker.setVisible(False)
            self.change_target_btn.setVisible(True)
            if had_focus:
                self.change_target_btn.setFocus(Qt.OtherFocusReason)

    def eventFilter(self, obj, event):
        picker = self.target_picker
        if (
            picker is not None and obj is picker.search_input
            and event.type() in (QEvent.ShortcutOverride, QEvent.KeyPress)
            and event.key() == Qt.Key_Escape and not picker.is_popup_visible()
        ):
            # Esc with no list open puts the line back; it does not close the
            # drawer (whose Esc is a shortcut: claimed before it fires).
            if event.type() == QEvent.ShortcutOverride:
                event.accept()
            else:
                self._end_target_change()
            return True
        return super().eventFilter(obj, event)

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
