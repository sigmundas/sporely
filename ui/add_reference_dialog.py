"""Tabbed "Add reference" picker dialog.

Consolidates the reference-add entry points into one dialog: source tabs
(Library / Community / My observations / Enter manually) sharing a single
:class:`ReferencePreviewPane`, docked beside the tab widget in a top-level
splitter so every tab's selection populates the same preview instance
instead of each tab owning its own. All four tabs are wired.

The Community tab embeds :class:`~ui.cloud_reference_dialog.CommunityResultsPane`,
which relocates ``CloudReferenceDialog``'s browse/select flow (its search
workers and payload builders, reused rather than duplicated) into the
picker. Unlike that legacy dialog, genus/species are fixed by the picker's
working taxon, so results load automatically instead of behind a search
button, and the dialog's two footer buttons ("Import summary as reference" /
"Use raw points for plot") become a radio the tab exposes instead
("Range summary" / "Raw points (n=X)"). A cloud entry has no measurement-set
or observation identity, so "Add to plot" routes through a dedicated
``cloud_attach_callback(dict)`` rather than the string-identifier
``attach_callback`` the other tabs use, straight to the same
``_add_reference_series_entry`` path the legacy dialog already calls
directly -- no new persistence. ``CloudReferenceDialog`` itself stays
reachable for now; stage 6 removes that dead entry point.

The Library tab does not fork any add/attach logic: selecting a result and
clicking "Add to plot" calls the ``attach_callback`` supplied by the host
(MainWindow), which is expected to be
``MainWindow._attach_normalized_reference_to_active_observation`` — the
exact path the (soon to be retired) ``ReferenceLibraryAttachDialog`` uses.
Color assignment is not touched here: it happens automatically, keyed by
list position, inside ``MainWindow._resolved_reference_series_entries``.

The My observations tab lists previous observations of the working taxon —
the same query (``ObservationDB.get_personal_observations_for_species``)
that populates the legacy Source dropdown's "My data <date>" entries — and
routes "Add to plot" through the same ``attach_callback``, but with an
``"observation:<id>"``-prefixed identifier: a personal observation has no
normalized measurement-set identity, so the host dispatches that prefix to
the legacy ``source_kind == "observation"`` comparison-series path instead
of the measurement-set attach path.

The Enter-manually tab reuses ReferenceEntryEditor and the shared preview.
New data is saved through manual_save_callback(editor), which returns the
new library set ID or None on failure. Saving keeps the picker open and
locks the saved editor; Add to plot then attaches that same ID. Existing-set
selections use manual_attach_callback(editor), whose boolean result controls
whether the picker closes. Changing the comparison target before saving
rebinds the editor's reference identity without changing the observation.

"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Iterable

from PySide6.QtCore import QCoreApplication, QSettings, QSize, Qt, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QStyle,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from database.models import MeasurementDB, ObservationDB
from database.reference_library import (
    MeasurementSet,
    MeasurementSetCandidate,
    MeasurementSetRepository,
)

from app_identity import SETTINGS_APP, SETTINGS_ORG

from .cloud_reference_dialog import CommunityResultsPane
from .reference_entry_editor import ReferenceEntryEditor
from .reference_preview_pane import ReferencePreviewPane
from .two_line_row import TwoLineRow
from .window_state import GeometryMixin

_NEW_PUBLICATION_ROLE = "new_publication"

_USABLE_MEASUREMENT_TYPES = (None, "", "manual", "spore", "spores")


def format_ai_candidate_display(entry: dict) -> str:
    """Display text for one AI-candidate row in the taxon target selector.

    Mirrors ``MainWindow._format_ref_ai_display``'s "scientific (vernacular)
    NN%" convention (the old Genus/Species panel's AI suggestions dropdown),
    reimplemented locally since ``ui.add_reference_dialog`` must not import
    ``main_window`` (it is imported by it).
    """
    scientific = str(entry.get("scientific_name") or "").strip()
    if not scientific:
        genus = str(entry.get("genus") or "").strip()
        species = str(entry.get("species") or "").strip()
        scientific = f"{genus} {species}".strip()
    vernacular = str(entry.get("vernacular") or "").strip()
    base = scientific
    if vernacular and vernacular.casefold() != scientific.casefold():
        base = f"{scientific} ({vernacular})"
    try:
        score = float(entry.get("score"))
    except (TypeError, ValueError):
        return base
    percent = int(round(score * 100)) if score <= 1.0 else int(round(score))
    return f"{base}  {percent}%"


def filter_library_candidates(
    candidates: Iterable[MeasurementSetCandidate],
    *,
    taxon_id: str | None,
    only_this_taxon: bool,
    query: str = "",
    taxon_text: str = "",
) -> list[MeasurementSetCandidate]:
    """Pure taxon+search filter for the Library tab's results list.

    Mirrors ``ReferenceLibraryAttachDialog._filtered_candidates``'s taxon
    scope and text-search semantics (search matches short label, published
    taxon name, and raw expression; taxon scope and search AND together) so
    both dialogs behave identically without sharing dialog state.

    ``taxon_text`` is a fallback for when the picker's taxon target has no
    known ``taxon_id`` -- an AI-suggested or freely-typed genus/species (see
    ``AddReferenceDialog``'s taxon selector) -- and matches candidates whose
    published name contains it instead of comparing ids.
    """
    result = list(candidates)
    if only_this_taxon and taxon_id is not None:
        target = str(taxon_id)
        result = [c for c in result if str(getattr(c, "taxon_id", "") or "") == target]
    elif only_this_taxon and taxon_text.strip():
        needle = taxon_text.strip().casefold()
        result = [c for c in result if needle in str(c.name_as_published or "").casefold()]
    normalized_query = (query or "").strip().casefold()
    if normalized_query:
        def _match(c: MeasurementSetCandidate) -> bool:
            for field_value in (c.short_label, c.name_as_published, c.raw_text):
                if field_value and normalized_query in str(field_value).casefold():
                    return True
            return False

        result = [c for c in result if _match(c)]
    return result


@dataclass
class PersonalObservationCandidate:
    """One row in the My observations tab: a different personal observation
    of the working taxon, with spore measurements usable as a comparison
    series."""

    observation_id: int
    date: str
    author: str
    location: str
    points: list[dict]

    @property
    def n(self) -> int:
        return len(self.points)


def default_my_observation_candidates(
    genus: str, species: str, *, exclude_observation_id: int | None = None
) -> list[PersonalObservationCandidate]:
    """Load My-observations candidates from the same query that populates
    the legacy Source dropdown's "My data <date>" entries.

    Only observations with at least one usable spore measurement are
    returned (mirrors the point-filtering in
    ``MainWindow._maybe_load_reference_panel_reference``'s observation
    branch), since an entry with no points cannot be plotted.
    """
    if not genus or not species:
        return []
    rows = ObservationDB.get_personal_observations_for_species(
        genus, species, exclude_observation_id=exclude_observation_id
    )
    result: list[PersonalObservationCandidate] = []
    for row in rows:
        try:
            obs_id = int(row["id"])
        except (KeyError, TypeError, ValueError):
            continue
        raw = MeasurementDB.get_measurements_for_observation(obs_id)
        points = [
            m for m in raw
            if m.get("length_um") is not None
            and m.get("width_um") is not None
            and m.get("measurement_type") in _USABLE_MEASUREMENT_TYPES
        ]
        if not points:
            continue
        obs = ObservationDB.get_observation(obs_id) or {}
        date_str = (row.get("date") or "").split(" ")[0].split("T")[0]
        result.append(
            PersonalObservationCandidate(
                observation_id=obs_id,
                date=date_str,
                author=(row.get("author") or "").strip(),
                location=(obs.get("location") or "").strip(),
                points=points,
            )
        )
    return result


class AddReferenceDialog(GeometryMixin, QDialog):
    """Tabbed picker for adding a reference dataset to the comparison plot.

    All four source tabs are wired: "Library", "Community", "My
    observations", and "Enter manually" (a
    :class:`~ui.reference_entry_editor.ReferenceEntryEditor` embedded
    without its modal chrome, sharing the same measurement editor,
    validation, and submission path as the legacy Quick-add dialog).
    """

    _geometry_key = "AddReferenceDialog"
    _SPLITTER_SETTINGS_KEY = "geometry/AddReferenceDialog/splitter"

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        taxon_label: str = "",
        taxon_id: int | str | None = None,
        genus: str = "",
        species: str = "",
        exclude_observation_id: int | None = None,
        exclude_observation_cloud_id: str | None = None,
        exclude_measurement_set_ids: Iterable[str] | None = None,
        attach_callback: Callable[[str, str], bool | None] | None = None,
        cloud_attach_callback: Callable[[dict], None] | None = None,
        manual_attach_callback: Callable[["ReferenceEntryEditor"], bool] | None = None,
        manual_save_callback: Callable[["ReferenceEntryEditor"], str | None] | None = None,
        candidates: list[MeasurementSetCandidate] | None = None,
        my_observations: list[PersonalObservationCandidate] | None = None,
        community_results: list[dict] | None = None,
        ai_candidates: list[dict] | None = None,
    ) -> None:
        super().__init__(parent)
        self._taxon_id: str | None = str(taxon_id).strip() or None if taxon_id is not None else None
        self._genus = genus
        self._species = species
        # The taxon this picker opened on -- "Own taxon" in the selector,
        # and the row it always offers regardless of what the user switches
        # to. Never mutated after construction.
        self._own_taxon_id = self._taxon_id
        self._own_genus = genus
        self._own_species = species
        self._own_label = taxon_label
        # AI candidates are display-only data the host (MainWindow) already
        # collected (see MainWindow._collect_reference_ai_suggestions) --
        # this dialog never re-derives them and never writes back to the
        # observation's identification.
        self._ai_candidates = list(ai_candidates or [])
        self._exclude_observation_id = exclude_observation_id
        self._exclude_observation_cloud_id = (
            str(exclude_observation_cloud_id).strip() or None
            if exclude_observation_cloud_id is not None
            else None
        )
        self._exclude_ids = {str(x) for x in (exclude_measurement_set_ids or [])}
        self._attach_callback = attach_callback
        self._cloud_attach_callback = cloud_attach_callback
        self._manual_attach_callback = manual_attach_callback
        self._manual_save_callback = manual_save_callback
        self._saved_manual_set_id: str | None = None
        # Optional injected candidate list, mirroring
        # ReferenceLibraryAttachDialog's testability convention: when
        # provided, skips the repository query so tests/scenarios can run
        # against deterministic fixtures without a real reference DB.
        self._injected_candidates = (
            [c for c in candidates if str(c.measurement_set_id) not in self._exclude_ids]
            if candidates is not None
            else None
        )
        self._injected_my_observations = my_observations
        self._injected_community_results = community_results
        self._candidates: list[MeasurementSetCandidate] = []
        self._selected_candidate: MeasurementSetCandidate | None = None
        self._my_observations: list[PersonalObservationCandidate] = []
        self._selected_observation: PersonalObservationCandidate | None = None

        title = QCoreApplication.translate("AddReferenceDialog", "Add reference")
        if taxon_label:
            title = QCoreApplication.translate("AddReferenceDialog", "Add reference — {taxon}").format(taxon=taxon_label)
        self.setWindowTitle(title)
        self.setModal(True)

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        root.addLayout(self._build_taxon_target_row())

        self._body_splitter = QSplitter(Qt.Horizontal, self)
        self.tabs = QTabWidget(self._body_splitter)
        self._body_splitter.addWidget(self.tabs)
        # Single shared preview pane (reused from Stage 1 — never rebuilt):
        # every tab's selection populates this one instance.
        self.preview_pane = ReferencePreviewPane(self._body_splitter)
        self._body_splitter.addWidget(self.preview_pane)
        self._body_splitter.setStretchFactor(0, 1)
        self._body_splitter.setStretchFactor(1, 2)
        root.addWidget(self._body_splitter, 1)

        self._build_library_tab()
        self.tabs.addTab(self._library_tab, QCoreApplication.translate("AddReferenceDialog", "Library"))
        self._build_community_tab()
        self._community_tab_index = self.tabs.addTab(self._community_tab, QCoreApplication.translate("AddReferenceDialog", "Community"))
        self._build_my_observations_tab()
        self._my_observations_tab_index = self.tabs.addTab(
            self._my_observations_tab, QCoreApplication.translate("AddReferenceDialog", "My observations")
        )
        self._build_manual_tab()
        self._manual_tab_index = self.tabs.addTab(
            self._manual_tab, QCoreApplication.translate("AddReferenceDialog", "Enter manually")
        )
        self.tabs.currentChanged.connect(self._on_tab_changed)

        footer = QHBoxLayout()
        self.status_hint_label = QLabel("", self)
        self.status_hint_label.setStyleSheet("color: #7f8c8d;")
        footer.addWidget(self.status_hint_label, 1)
        self.cancel_btn = QPushButton(QCoreApplication.translate("AddReferenceDialog", "Cancel"), self)
        self.cancel_btn.clicked.connect(self.reject)
        footer.addWidget(self.cancel_btn)
        self.save_to_library_btn = QPushButton(QCoreApplication.translate("AddReferenceDialog", "Save to library"), self)
        self.save_to_library_btn.clicked.connect(self._on_save_to_library_clicked)
        footer.addWidget(self.save_to_library_btn)
        self.add_to_plot_btn = QPushButton(QCoreApplication.translate("AddReferenceDialog", "Add to plot"), self)
        self.add_to_plot_btn.setEnabled(False)
        self.add_to_plot_btn.setDefault(True)
        self.add_to_plot_btn.clicked.connect(self._on_add_to_plot_clicked)
        footer.addWidget(self.add_to_plot_btn)
        root.addLayout(footer)

        self.preview_pane.clear()
        self._refresh_candidates()
        self._refresh_my_observations()

        min_size, source_width, preview_width = self._derive_minimum_size()
        self.setMinimumSize(min_size)
        self.resize(min_size)
        # A floor on each pane, not just an initial setSizes(): the splitter
        # otherwise redistributes space by stretch factor on any later
        # resize (restored geometry, a maximize, a manual drag), which can
        # squeeze either tab bar back into its own scroll-arrow fallback
        # even though the dialog stays wide enough overall.
        self.tabs.setMinimumWidth(source_width)
        self.preview_pane.setMinimumWidth(preview_width)
        self._body_splitter.setSizes([source_width, preview_width])
        self._restore_geometry()
        self._restore_splitter_state()
        self.finished.connect(self._save_geometry)
        self.finished.connect(self._save_splitter_state)
        self.finished.connect(self._community_pane.close)

    def _derive_minimum_size(self) -> tuple[QSize, int, int]:
        """Derive the smallest size at which all four source tabs and all
        five preview sub-tabs are visible without scroll arrows, from the
        widgets' own size hints rather than a hardcoded pixel value.

        Returns the dialog size plus the source/preview pane widths that
        produced it, so the caller can also seat the splitter at that split
        (see the "fresh QSplitter" note where it is called).
        """
        margins = self.layout().contentsMargins()
        # A tab bar's own sizeHint() sits right at the threshold where Qt
        # falls back to scroll arrows, not safely past it (observed: a tab
        # bar rendered at exactly its sizeHint width still used the arrows).
        # PM_TabBarScrollButtonWidth is the width Qt itself reserves per
        # arrow button, so clearing the threshold by twice that (one on
        # each side) is a style-derived margin, not an invented pixel count.
        scroll_button_clearance = 2 * self.style().pixelMetric(
            QStyle.PM_TabBarScrollButtonWidth
        )
        source_width = max(
            self.tabs.tabBar().sizeHint().width(),
            self._library_tab.sizeHint().width(),
            self.manual_editor.sizeHint().width(),
        ) + scroll_button_clearance
        preview_width = max(
            self.preview_pane.review_tabs.tabBar().sizeHint().width(),
            self.preview_pane.sizeHint().width(),
        ) + scroll_button_clearance
        width = (
            margins.left() + margins.right()
            + source_width + self._body_splitter.handleWidth() + preview_width
        )
        return QSize(width, self.sizeHint().height()), source_width, preview_width

    def _restore_splitter_state(self) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        state = settings.value(self._SPLITTER_SETTINGS_KEY)
        if state:
            self._body_splitter.restoreState(state)

    def _save_splitter_state(self) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue(self._SPLITTER_SETTINGS_KEY, self._body_splitter.saveState())

    # ------------------------------------------------------------------
    # Taxon target selector
    # ------------------------------------------------------------------
    #
    # The reference panel's Genus/Species/Norsk navn fields never set the
    # observation's identification -- Edit Observation does, and that is
    # already settled by the time the user reaches Analysis. This selector
    # only chooses whose published spore data to compare against, which is
    # usually the observation's own taxon but need not be. Changing it never
    # writes to the observation.

    def _build_taxon_target_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.setSpacing(8)
        row.addWidget(QLabel(QCoreApplication.translate("AddReferenceDialog", "Compare against:"), self))
        self.taxon_target_combo = QComboBox(self)
        self.taxon_target_combo.setEditable(True)
        self.taxon_target_combo.setInsertPolicy(QComboBox.NoInsert)
        self.taxon_target_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.taxon_target_combo.setToolTip(
            QCoreApplication.translate("AddReferenceDialog",
                "Choose which taxon's published spore data to compare "
                "against -- an AI suggestion, or type a genus and species. "
                "This never changes the observation's own identification."
            )
        )
        row.addWidget(self.taxon_target_combo, 1)
        self._populate_taxon_target_combo()
        self.taxon_target_combo.activated.connect(self._on_taxon_target_activated)
        self.taxon_target_combo.lineEdit().returnPressed.connect(
            self._on_taxon_target_text_entered
        )
        return row

    def _own_taxon_display_label(self) -> str:
        return self._own_label or " ".join(
            part for part in (self._own_genus, self._own_species) if part
        ).strip()

    def _populate_taxon_target_combo(self) -> None:
        combo = self.taxon_target_combo
        combo.blockSignals(True)
        combo.clear()
        own_label = self._own_taxon_display_label()
        combo.addItem(
            QCoreApplication.translate("AddReferenceDialog", "This observation: {taxon}").format(taxon=own_label)
            if own_label
            else QCoreApplication.translate("AddReferenceDialog", "This observation's taxon"),
            {
                "genus": self._own_genus,
                "species": self._own_species,
                "taxon_id": self._own_taxon_id,
                "label": own_label,
            },
        )
        for entry in self._ai_candidates:
            genus = str(entry.get("genus") or "").strip()
            species = str(entry.get("species") or "").strip()
            if not genus or not species:
                continue
            combo.addItem(
                format_ai_candidate_display(entry),
                {
                    "genus": genus,
                    "species": species,
                    "taxon_id": None,
                    "label": f"{genus} {species}",
                },
            )
        combo.setCurrentIndex(0)
        combo.blockSignals(False)

    def _on_taxon_target_activated(self, index: int) -> None:
        data = self.taxon_target_combo.itemData(index)
        if not isinstance(data, dict):
            return
        self._apply_taxon_target(
            genus=data.get("genus") or "",
            species=data.get("species") or "",
            taxon_id=data.get("taxon_id"),
            label=data.get("label") or "",
        )

    def _on_taxon_target_text_entered(self) -> None:
        combo = self.taxon_target_combo
        text = combo.currentText().strip()
        current_index = combo.currentIndex()
        if current_index >= 0 and text == combo.itemText(current_index).strip():
            # Display text is unchanged from the selected item (e.g. Return
            # pressed right after an AI-candidate selection): reapply its
            # structured genus/species/ID instead of re-parsing the label,
            # which can otherwise fold a match percentage or vernacular name
            # into the taxon.
            data = combo.itemData(current_index)
            if isinstance(data, dict):
                self._apply_taxon_target(
                    genus=data.get("genus") or "",
                    species=data.get("species") or "",
                    taxon_id=data.get("taxon_id"),
                    label=data.get("label") or "",
                )
            return
        parts = text.split(None, 1)
        if len(parts) < 2:
            return
        genus, species = parts[0], parts[1]
        self._apply_taxon_target(
            genus=genus, species=species, taxon_id=None, label=f"{genus} {species}"
        )

    def _apply_taxon_target(
        self, *, genus: str, species: str, taxon_id, label: str
    ) -> None:
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()
        self._taxon_id = (
            str(taxon_id).strip() or None if taxon_id is not None else None
        )
        display_label = label or " ".join(
            part for part in (self._genus, self._species) if part
        ).strip()
        self.setWindowTitle(
            QCoreApplication.translate("AddReferenceDialog", "Add reference — {taxon}").format(taxon=display_label)
            if display_label
            else QCoreApplication.translate("AddReferenceDialog", "Add reference")
        )
        self._selected_candidate = None
        self._populate_results_list()
        self._community_pane.set_taxon(self._genus, self._species)
        self._refresh_my_observations()
        self.manual_editor.set_comparison_target(
            genus=self._genus, species=self._species, sporely_taxon_id=self._taxon_id
        )
        self._update_footer_state()

    def _on_tab_changed(self, _index: int) -> None:
        self._update_footer_state()
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            self._populate_observation_preview(self._selected_observation)
        elif self.tabs.currentIndex() == self._community_tab_index:
            self._community_pane.sync_preview()
        elif self.tabs.currentIndex() == self._manual_tab_index:
            self.manual_editor.sync_preview()
        elif self.tabs.currentWidget() is self._library_tab:
            self._populate_preview(self._selected_candidate)
        else:
            self.preview_pane.clear()

    # ------------------------------------------------------------------
    # Library tab
    # ------------------------------------------------------------------

    def _build_library_tab(self) -> None:
        self._library_tab = QWidget(self)
        layout = QVBoxLayout(self._library_tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)
        self.search_input = QLineEdit(self._library_tab)
        self.search_input.setPlaceholderText(
            QCoreApplication.translate("AddReferenceDialog", "Filter by publication, taxon, or raw expression…")
        )
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self._on_filter_changed)
        filter_row.addWidget(self.search_input, 1)
        self.only_this_taxon_checkbox = QCheckBox(QCoreApplication.translate("AddReferenceDialog", "Only this taxon"), self._library_tab)
        # Default checked: unfiltered, library rows show only publication
        # and range (see _add_candidate_item), so without this the user has
        # no way to tell which species several "Funga Nordica (2008)" rows
        # each describe. Falls back to a name-text match (_taxon_target_query_text)
        # when the target has no taxon_id, so it still narrows the list for
        # an AI-suggested or freely-typed genus/species.
        self.only_this_taxon_checkbox.setChecked(True)
        self.only_this_taxon_checkbox.toggled.connect(self._on_filter_changed)
        filter_row.addWidget(self.only_this_taxon_checkbox)
        layout.addLayout(filter_row)

        self.results_list = QListWidget(self._library_tab)
        # Elide long rows instead of growing a horizontal scrollbar; matches
        # the row-eliding convention in ui/comparison_panel.py.
        self.results_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.results_list.itemSelectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.results_list, 1)

    def _on_filter_changed(self, *_args) -> None:
        self._selected_candidate = None
        self._populate_results_list()
        self._update_footer_state()

    def _refresh_candidates(self) -> None:
        if self._injected_candidates is not None:
            self._candidates = list(self._injected_candidates)
        else:
            self._candidates = MeasurementSetRepository.list_attachment_candidates(
                exclude_ids=self._exclude_ids
            )
        self._selected_candidate = None
        self._populate_results_list()
        self._update_footer_state()

    def _taxon_target_query_text(self) -> str:
        """Fallback text for taxon filtering when the current target has no
        ``taxon_id`` (an AI candidate or a freely-typed genus/species)."""
        if self._taxon_id is not None:
            return ""
        return " ".join(part for part in (self._genus, self._species) if part).strip()

    def _filtered_candidates(self) -> list[MeasurementSetCandidate]:
        return filter_library_candidates(
            self._candidates,
            taxon_id=self._taxon_id,
            only_this_taxon=self.only_this_taxon_checkbox.isChecked(),
            query=self.search_input.text(),
            taxon_text=self._taxon_target_query_text(),
        )

    def _populate_results_list(self) -> None:
        self.results_list.clear()
        visible = self._filtered_candidates()
        for candidate in visible:
            self._add_candidate_item(candidate)
        new_pub_item = QListWidgetItem(QCoreApplication.translate("AddReferenceDialog", "+ New publication…"))
        new_pub_item.setData(Qt.UserRole, _NEW_PUBLICATION_ROLE)
        new_pub_item.setForeground(self.palette().link())
        self.results_list.addItem(new_pub_item)

        if not visible:
            self.status_hint_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "No matching measurement sets in the library.")
                if self._candidates
                else QCoreApplication.translate("AddReferenceDialog", "The reference library has no measurement sets yet.")
            )
        else:
            self.status_hint_label.setText("")
        self.preview_pane.clear()

    def _add_candidate_item(self, candidate: MeasurementSetCandidate) -> None:
        label = candidate.short_label or candidate.name_as_published or QCoreApplication.translate("AddReferenceDialog", "Untitled")
        # short_label conventionally already ends with the year (see
        # database.reference_citation.build_short_label); only append it
        # when genuinely missing, to avoid "... 2018 (2018)".
        if candidate.year and str(candidate.year) not in label:
            label = f"{label} ({candidate.year})"
        detail = candidate.data_kind or ""
        if candidate.raw_text:
            detail = f"{detail} · {candidate.raw_text}" if detail else candidate.raw_text
        if not self.only_this_taxon_checkbox.isChecked() and candidate.name_as_published:
            # Unfiltered, several rows can share a publication -- show which
            # taxon each one describes instead of leaving that ambiguous.
            detail = f"{candidate.name_as_published} · {detail}" if detail else candidate.name_as_published

        item = QListWidgetItem()
        item.setToolTip(label if not detail else f"{label}\n{detail}")
        item.setData(Qt.UserRole, candidate.measurement_set_id)
        item.setData(Qt.UserRole + 1, detail)
        self.results_list.addItem(item)
        row_widget = TwoLineRow(label, detail, self.results_list)
        item.setSizeHint(row_widget.sizeHint())
        self.results_list.setItemWidget(item, row_widget)

    def _on_selection_changed(self) -> None:
        items = self.results_list.selectedItems()
        if not items:
            self._selected_candidate = None
            self.preview_pane.clear()
            self._update_footer_state()
            return
        role = items[0].data(Qt.UserRole)
        if role == _NEW_PUBLICATION_ROLE:
            self._selected_candidate = None
            self._on_new_publication_clicked()
            return
        candidate = next(
            (c for c in self._candidates if c.measurement_set_id == role),
            None,
        )
        self._selected_candidate = candidate
        self._populate_preview(candidate)
        self._update_footer_state()

    def _populate_preview(self, candidate: MeasurementSetCandidate | None) -> None:
        if candidate is None:
            self.preview_pane.clear()
            return
        measurement_set: MeasurementSet | None = MeasurementSetRepository.get(
            candidate.measurement_set_id
        )
        title = candidate.short_label or candidate.name_as_published or QCoreApplication.translate("AddReferenceDialog", "Untitled")
        meta = candidate.name_as_published or ""
        if candidate.locator_text:
            meta = f"{meta} · {candidate.locator_text}" if meta else candidate.locator_text
        rows: list[tuple[str, str, str, str]] = []
        derived_cells: list[tuple[bool, bool, bool]] = []
        if measurement_set is not None:
            for label, prefix in (
                (QCoreApplication.translate("AddReferenceDialog", "Length"), "length"),
                (QCoreApplication.translate("AddReferenceDialog", "Width"), "width"),
                (QCoreApplication.translate("AddReferenceDialog", "Q"), "q"),
            ):
                # Extreme (parenthesised) bounds win when present; otherwise
                # fall back to the "core"/typical bound the parser stores
                # separately (``length_core_min``/``width_core_max`` etc —
                # there is no such fallback field for Q). This mirrors the
                # same extreme-or-typical rule already applied when writing
                # Q's min/max (``ReferenceAddDialog.normalized_measurement_
                # set_payload``) and when the plotting path resolves a
                # drawable rectangle (``references.reference_plotting.
                # range_payload_is_plottable``); without it, a source
                # reported only as a typical range (the common case) shows
                # correctly in the raw-text list line but as "—" here.
                vmin = getattr(measurement_set, f"{prefix}_min", None)
                min_derived = False
                if vmin is None:
                    vmin = getattr(measurement_set, f"{prefix}_core_min", None)
                    min_derived = vmin is not None
                vmax = getattr(measurement_set, f"{prefix}_max", None)
                max_derived = False
                if vmax is None:
                    vmax = getattr(measurement_set, f"{prefix}_core_max", None)
                    max_derived = vmax is not None
                vmean = getattr(measurement_set, f"{prefix}_mean", None)
                rows.append(
                    (
                        label,
                        self._format_stat(vmin),
                        self._format_stat(vmean),
                        self._format_stat(vmax),
                    )
                )
                derived_cells.append((min_derived, False, max_derived))
        note = candidate.raw_text or QCoreApplication.translate("AddReferenceDialog", "No additional notes.")
        self.preview_pane.set_summary(title, meta, rows, note, derived=derived_cells)
        method_recorded = bool(
            measurement_set is not None
            and (
                measurement_set.mount_medium
                or measurement_set.stain
                or measurement_set.preparation
                or measurement_set.measurement_method
            )
        )
        sample_size = getattr(measurement_set, "sample_size", None) if measurement_set is not None else None
        not_reported = QCoreApplication.translate("AddReferenceDialog", "not reported")
        self.preview_pane.set_provenance_summary(
            QCoreApplication.translate(
                "AddReferenceDialog", "Reported by: {work} ({year}) · sample size: {size} · method recorded: {method}"
            ).format(
                work=candidate.work_title or candidate.name_as_published or "—",
                year=candidate.year if candidate.year else not_reported,
                size=sample_size if sample_size else not_reported,
                method=QCoreApplication.translate("AddReferenceDialog", "yes") if method_recorded else not_reported,
            )
        )

        if measurement_set is not None and measurement_set.raw_points_json:
            self.preview_pane.set_raw_spores(measurement_set.raw_points_json)
        else:
            # Range-kind measurement sets have no raw points to show.
            self.preview_pane.set_raw_spores(
                QCoreApplication.translate("AddReferenceDialog", "This is a range summary; no raw spore points are stored.")
            )
        if measurement_set is not None:
            self.preview_pane.set_method(
                {
                    "mount": measurement_set.mount_medium or "",
                    "stain": measurement_set.stain or "",
                    "sample_type": measurement_set.preparation or "",
                    "objective": measurement_set.measurement_method or "",
                }
            )
            self.preview_pane.set_calibration(
                measurement_set.notes or QCoreApplication.translate("AddReferenceDialog", "No calibration details recorded.")
            )
        source_notes = (candidate.treatment_notes or "").strip() or QCoreApplication.translate(
            "AddReferenceDialog", "Not reported"
        )
        self.preview_pane.set_provenance(
            QCoreApplication.translate("AddReferenceDialog", "Publication: {work}").format(work=candidate.work_title or candidate.name_as_published or "—")
            + "\n"
            + QCoreApplication.translate("AddReferenceDialog", "Source notes: {notes}").format(notes=source_notes)
        )

    @staticmethod
    def _min_mean_max_from_points(points: list[dict]) -> dict:
        """Length/width/Q min-mean-max, for the My-observations preview.

        A lighter-weight sibling of ``MainWindow._reference_stats_from_points``
        (which also computes percentiles the Summary tab here does not use).
        """
        lengths = [p["length_um"] for p in points if p.get("length_um") is not None]
        widths = [p["width_um"] for p in points if p.get("width_um") is not None]
        if not lengths or not widths:
            return {}
        qs = [l / w for l, w in zip(lengths, widths) if w]
        stats: dict[str, float] = {}
        for prefix, values in (("length", lengths), ("width", widths), ("q", qs)):
            if not values:
                continue
            stats[f"{prefix}_min"] = min(values)
            stats[f"{prefix}_mean"] = sum(values) / len(values)
            stats[f"{prefix}_max"] = max(values)
        return stats

    @staticmethod
    def _format_stat(value) -> str:
        if value is None:
            return "—"
        try:
            return f"{float(value):.2f}"
        except Exception:
            return str(value)

    def _on_new_publication_clicked(self) -> None:
        try:
            from .reference_library_manager_dialog import ReferenceWorkEditor
        except Exception as exc:
            QMessageBox.warning(
                self,
                QCoreApplication.translate("AddReferenceDialog", "New publication"),
                QCoreApplication.translate("AddReferenceDialog", "Reference library editor is unavailable: {error}").format(error=str(exc)),
            )
            self._populate_results_list()
            return
        editor = ReferenceWorkEditor(self, persist_on_accept=True)
        try:
            editor.exec()
        finally:
            editor.deleteLater()
        self._refresh_candidates()

    # ------------------------------------------------------------------
    # Community tab
    # ------------------------------------------------------------------

    def _build_community_tab(self) -> None:
        self._community_tab = QWidget(self)
        layout = QVBoxLayout(self._community_tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self._community_pane = CommunityResultsPane(
            self._community_tab,
            genus=self._genus,
            species=self._species,
            preview_pane=self.preview_pane,
            results=self._injected_community_results,
            exclude_observation_cloud_id=self._exclude_observation_cloud_id,
        )
        self._community_pane.selection_changed.connect(self._update_footer_state)
        layout.addWidget(self._community_pane, 1)

    # ------------------------------------------------------------------
    # My observations tab
    # ------------------------------------------------------------------

    def _build_my_observations_tab(self) -> None:
        self._my_observations_tab = QWidget(self)
        layout = QVBoxLayout(self._my_observations_tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self.my_observations_list = QListWidget(self._my_observations_tab)
        self.my_observations_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.my_observations_list.itemSelectionChanged.connect(
            self._on_my_observations_selection_changed
        )
        layout.addWidget(self.my_observations_list, 1)

        self.my_observations_status_label = QLabel("", self._my_observations_tab)
        self.my_observations_status_label.setStyleSheet("color: #7f8c8d;")
        self.my_observations_status_label.setWordWrap(True)
        layout.addWidget(self.my_observations_status_label)

    def _refresh_my_observations(self) -> None:
        if self._injected_my_observations is not None:
            self._my_observations = list(self._injected_my_observations)
        else:
            self._my_observations = default_my_observation_candidates(
                self._genus,
                self._species,
                exclude_observation_id=self._exclude_observation_id,
            )
        self._selected_observation = None
        self._populate_my_observations_list()
        self._update_footer_state()

    def _populate_my_observations_list(self) -> None:
        self.my_observations_list.clear()
        for candidate in self._my_observations:
            self._add_observation_item(candidate)
        self.my_observations_status_label.setText(
            "" if self._my_observations
            else QCoreApplication.translate("AddReferenceDialog", "No previous observations of this taxon have spore measurements.")
        )
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            self.preview_pane.clear()

    def _add_observation_item(self, candidate: PersonalObservationCandidate) -> None:
        label = (
            QCoreApplication.translate("AddReferenceDialog", "My observation — {author}").format(author=candidate.author)
            if candidate.author
            else QCoreApplication.translate("AddReferenceDialog", "My observation")
        )
        detail_parts = [candidate.date] if candidate.date else []
        detail_parts.append(QCoreApplication.translate("AddReferenceDialog", "n = {count}").format(count=candidate.n))
        if candidate.location:
            detail_parts.append(candidate.location)
        detail = " · ".join(detail_parts)

        item = QListWidgetItem()
        item.setToolTip(label if not detail else f"{label}\n{detail}")
        item.setData(Qt.UserRole, candidate.observation_id)
        self.my_observations_list.addItem(item)
        row_widget = TwoLineRow(label, detail, self.my_observations_list)
        item.setSizeHint(row_widget.sizeHint())
        self.my_observations_list.setItemWidget(item, row_widget)

    def _on_my_observations_selection_changed(self) -> None:
        items = self.my_observations_list.selectedItems()
        if not items:
            self._selected_observation = None
            self.preview_pane.clear()
            self._update_footer_state()
            return
        observation_id = items[0].data(Qt.UserRole)
        candidate = next(
            (c for c in self._my_observations if c.observation_id == observation_id),
            None,
        )
        self._selected_observation = candidate
        self._populate_observation_preview(candidate)
        self._update_footer_state()

    def _populate_observation_preview(
        self, candidate: PersonalObservationCandidate | None
    ) -> None:
        if candidate is None:
            self.preview_pane.clear()
            return
        title = (
            QCoreApplication.translate("AddReferenceDialog", "My observation — {author}").format(author=candidate.author)
            if candidate.author
            else QCoreApplication.translate("AddReferenceDialog", "My observation")
        )
        meta_parts = [candidate.date] if candidate.date else []
        if candidate.location:
            meta_parts.append(candidate.location)
        meta = " · ".join(meta_parts)

        stats = self._min_mean_max_from_points(candidate.points)
        rows: list[tuple[str, str, str, str]] = []
        for label, prefix in (
            (QCoreApplication.translate("AddReferenceDialog", "Length"), "length"),
            (QCoreApplication.translate("AddReferenceDialog", "Width"), "width"),
            (QCoreApplication.translate("AddReferenceDialog", "Q"), "q"),
        ):
            rows.append(
                (
                    label,
                    self._format_stat(stats.get(f"{prefix}_min")),
                    self._format_stat(stats.get(f"{prefix}_mean")),
                    self._format_stat(stats.get(f"{prefix}_max")),
                )
            )
        note = QCoreApplication.translate("AddReferenceDialog", "n = {count} spore measurements").format(count=candidate.n)
        self.preview_pane.set_summary(title, meta, rows, note)
        # A personal observation is not a published/community reference, so
        # there is no reported-source provenance to summarize here.
        self.preview_pane.set_provenance_summary("")

        self.preview_pane.set_raw_spores(
            json.dumps(candidate.points, indent=2, ensure_ascii=False, default=str)
        )
        self.preview_pane.set_method(
            {
                "mount": "",
                "stain": "",
                "sample_type": "",
                "objective": "",
            }
        )
        self.preview_pane.set_calibration(
            QCoreApplication.translate("AddReferenceDialog", "Not applicable: this is a personal observation, not a normalized library entry.")
        )
        self.preview_pane.set_provenance(
            QCoreApplication.translate("AddReferenceDialog", "Personal observation, {date}").format(date=candidate.date)
            if candidate.date
            else QCoreApplication.translate("AddReferenceDialog", "Personal observation")
        )

    # ------------------------------------------------------------------
    # Enter manually tab
    # ------------------------------------------------------------------

    def _build_manual_tab(self) -> None:
        self._manual_tab = QWidget(self)
        layout = QVBoxLayout(self._manual_tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)
        self.manual_editor = ReferenceEntryEditor(
            self._manual_tab,
            self._genus,
            self._species,
            observation_id=self._exclude_observation_id,
            sporely_taxon_id=self._taxon_id,
            observation_taxon_id=self._own_taxon_id,
            preview_pane=self.preview_pane,
        )
        self.manual_editor.data_changed.connect(self._update_footer_state)
        layout.addWidget(self.manual_editor)

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------

    def _update_footer_state(self) -> None:
        if not hasattr(self, "add_to_plot_btn"):
            # The manual editor's construction synchronously refreshes its
            # preview (and emits data_changed) before the footer button
            # exists yet; nothing to update this early.
            return
        manual = self.tabs.currentIndex() == self._manual_tab_index
        self.save_to_library_btn.setVisible(manual)
        self.save_to_library_btn.setEnabled(
            manual and not self._saved_manual_set_id
            and not self.manual_editor.is_use_existing_set()
            and self.manual_editor.is_ready_to_submit()
        )
        if manual:
            self.status_hint_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "Saved to library. Add this set to the plot, or close.")
                if self._saved_manual_set_id else (
                    "" if self.manual_editor.is_use_existing_set() else
                    QCoreApplication.translate("AddReferenceDialog", "Save new data to the library first, then add the saved set to the plot.")
                )
            )
        elif self.tabs.currentWidget() is self._library_tab:
            self.status_hint_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "No matching measurement sets in the library.")
                if not self._filtered_candidates() else ""
            )
        else:
            self.status_hint_label.setText("")
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            self.add_to_plot_btn.setEnabled(self._selected_observation is not None)
        elif self.tabs.currentIndex() == self._community_tab_index:
            self.add_to_plot_btn.setEnabled(self._community_pane.has_selection())
        elif self.tabs.currentIndex() == self._manual_tab_index:
            self.add_to_plot_btn.setEnabled(bool(self._saved_manual_set_id) or (
                self.manual_editor.is_use_existing_set() and self.manual_editor.is_ready_to_submit()
            ))
        else:
            self.add_to_plot_btn.setEnabled(self._selected_candidate is not None)

    def _on_save_to_library_clicked(self) -> None:
        if self._saved_manual_set_id or self._manual_save_callback is None:
            return
        editor = self.manual_editor
        if not editor._selected_work_id and editor.pending_reference_work() is None:
            QMessageBox.warning(self, QCoreApplication.translate("AddReferenceDialog", "Publication required"),
                QCoreApplication.translate("AddReferenceDialog", "Select or create a publication before saving to the library."))
            return
        if not editor.validate_and_build_result():
            return
        saved_id = self._manual_save_callback(editor)
        if not saved_id:
            return
        self._saved_manual_set_id = saved_id
        editor.setEnabled(False)
        self.taxon_target_combo.setEnabled(False)
        self.cancel_btn.setText(QCoreApplication.translate("AddReferenceDialog", "Close"))
        self._update_footer_state()

    def _on_add_to_plot_clicked(self) -> None:
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            if self._attach_callback is None or self._selected_observation is None:
                return
            self._attach_callback(
                f"observation:{self._selected_observation.observation_id}", "compared"
            )
            self.accept()
            return
        if self.tabs.currentIndex() == self._community_tab_index:
            if self._cloud_attach_callback is None:
                return
            payload = self._community_pane.current_mode_payload()
            if not payload:
                return
            self._cloud_attach_callback(payload)
            self.accept()
            return
        if self.tabs.currentIndex() == self._manual_tab_index:
            if self._saved_manual_set_id:
                if self._attach_callback is not None:
                    result = self._attach_callback(self._saved_manual_set_id, "compared")
                    if result is not False:
                        self.accept()
                return
            if not self.manual_editor.is_use_existing_set():
                return
            if self._manual_attach_callback is None:
                return
            if not self.manual_editor.validate_and_build_result():
                return
            # Unlike the other tabs, a manual submission can fail after
            # validation (e.g. the observation drifted while the picker
            # was open) and must leave the picker open rather than claim
            # success — so only accept() when the callback reports success.
            if self._manual_attach_callback(self.manual_editor):
                self.accept()
            return
        if self._attach_callback is None or self._selected_candidate is None:
            return
        self._attach_callback(self._selected_candidate.measurement_set_id, "compared")
        self.accept()


__all__ = [
    "AddReferenceDialog",
    "filter_library_candidates",
    "format_ai_candidate_display",
    "PersonalObservationCandidate",
    "default_my_observation_candidates",
]
