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

Its list carries two states that must not be collapsed into one. The
*selected* row is what the shared preview shows; the *checked* rows are what
the footer will add to the plot. Rows are grouped by relevance to the picker's
taxon (``group_library_candidates``) and rendered by
:class:`~ui.library_source_row.LibrarySourceRow`. The footer counts the
checked rows, but adding more than one source in a single click is not wired:
``attach_callback`` returns ``None`` on success and on every failure alike, so
a batch loop could not report which sources landed (see
``_on_add_to_plot_clicked``).

The My observations tab lists previous observations of the working taxon —
the same query (``ObservationDB.get_personal_observations_for_species``)
that populates the legacy Source dropdown's "My data <date>" entries — and
routes "Add to plot" through the same ``attach_callback``, but with an
``"observation:<id>"``-prefixed identifier: a personal observation has no
normalized measurement-set identity, so the host dispatches that prefix to
the legacy ``source_kind == "observation"`` comparison-series path instead
of the measurement-set attach path.

The Enter-manually tab embeds a
:class:`~ui.reference_entry_editor.ReferenceEntryEditor` — the same paste/
parse table, publication picker, and Data section the legacy Quick-add
dialog uses, without its modal Save/Cancel chrome — sharing the shared
:class:`ReferencePreviewPane` and this dialog's "Add to plot"/Cancel footer
instead. "Add to plot" routes through a dedicated
``manual_attach_callback(editor) -> bool`` rather than the string-identifier
``attach_callback``, since a manual submission can fail validation or an
observation-drift check *after* the click and must leave the picker open
rather than claim success — the callback's return value tells this dialog
whether to close. Changing the "Compare against" target rebinds the
editor's publication/treatment identity (see
:meth:`ReferenceEntryEditor.set_comparison_target`) without discarding
already-entered measurement values, and to the submission path,
``ReferenceEntryEditor`` is duck-typed identically to the legacy dialog's
own wrapper, so ``MainWindow`` reuses one shared post-validation
persistence routine for both.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Callable, Iterable

from PySide6.QtCore import QCoreApplication, QSettings, QSize, Qt, QTimer, Signal
from PySide6.QtGui import QFont
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
    QScrollArea,
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

from references.reference_comparison import (
    MetricDomain,
    ObservationBaseline,
    build_domains,
    comparison_view,
    observation_baseline_from_points,
)
from references.reference_display import (
    display_from_points,
    display_from_row,
    format_measurement_expression,
)

from app_identity import SETTINGS_APP, SETTINGS_ORG

from .cloud_reference_dialog import CommunityResultsPane
from .library_source_row import LibraryResultsList, LibrarySourceRow
from .reference_entry_editor import ReferenceEntryEditor
from .reference_preview_pane import ReferencePreviewPane
from .two_line_row import TwoLineRow
from .window_state import GeometryMixin

_NEW_PUBLICATION_ROLE = "new_publication"
_GROUP_HEADING_ROLE = "group_heading"

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


#: Library relevance groups, in the fixed display order of the design
#: contract (N6). The strings are keys, not wording: the headings are
#: translated in :meth:`AddReferenceDialog._library_group_heading`.
LIBRARY_GROUP_ORDER = ("this_taxon", "same_genus", "rest")


def _published_genus(candidate: MeasurementSetCandidate) -> str:
    """First word of a candidate's published name, which is its genus.

    Published names carry authorities and infraspecific ranks
    (``Cortinarius limonius (Fr.) Fr.``), so nothing but the leading word is
    reliable here — and nothing more than the genus is needed.
    """
    name = str(candidate.name_as_published or "").strip()
    return name.split()[0] if name else ""


def library_relevance(
    candidate: MeasurementSetCandidate,
    *,
    taxon_id: str | None,
    genus: str,
    taxon_name: str,
) -> str:
    """Which relevance group one candidate belongs to.

    Two independent signals establish "this taxon", because the picker's
    target does not always have an id: an AI suggestion or a freely-typed
    binomial gives a name and nothing else (see
    ``AddReferenceDialog._taxon_target_query_text``). Matching on either the
    id *or* the published name keeps a hand-typed target grouping the same
    rows an identified one would, rather than silently demoting every match
    to ``same_genus``.
    """
    target_id = str(taxon_id or "").strip()
    if target_id and str(getattr(candidate, "taxon_id", "") or "") == target_id:
        return "this_taxon"
    name = str(candidate.name_as_published or "").casefold()
    needle = taxon_name.strip().casefold()
    if needle and needle in name:
        return "this_taxon"
    target_genus = genus.strip().casefold()
    if target_genus and _published_genus(candidate).casefold() == target_genus:
        return "same_genus"
    return "rest"


def group_library_candidates(
    candidates: Iterable[MeasurementSetCandidate],
    *,
    taxon_id: str | None,
    genus: str,
    taxon_name: str,
) -> list[tuple[str, list[MeasurementSetCandidate]]]:
    """Split candidates into the contract's three groups, in fixed order.

    Pure, and deliberately separate from :func:`filter_library_candidates`:
    filtering decides what the user may see at all, grouping only decides the
    order it is shown in. Empty groups are dropped rather than rendered as a
    heading with ``(0)``, so the list never advertises a category the library
    cannot fill. Within a group the caller's order is preserved.
    """
    buckets: dict[str, list[MeasurementSetCandidate]] = {
        key: [] for key in LIBRARY_GROUP_ORDER
    }
    for candidate in candidates:
        buckets[
            library_relevance(
                candidate, taxon_id=taxon_id, genus=genus, taxon_name=taxon_name
            )
        ].append(candidate)
    return [(key, buckets[key]) for key in LIBRARY_GROUP_ORDER if buckets[key]]


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
    # The row's own taxon. Only meaningful when the tab is browsing a whole
    # genus, where several species share the list and the date/author label
    # alone does not say which one a row is.
    genus: str = ""
    species: str = ""

    @property
    def n(self) -> int:
        return len(self.points)


def default_my_observation_candidates(
    genus: str, species: str = "", *, exclude_observation_id: int | None = None
) -> list[PersonalObservationCandidate]:
    """Load My-observations candidates from the same query that populates
    the legacy Source dropdown's "My data <date>" entries.

    Only observations with at least one usable spore measurement are
    returned (mirrors the point-filtering in
    ``MainWindow._maybe_load_reference_panel_reference``'s observation
    branch), since an entry with no points cannot be plotted.

    A genus with no species browses every personal observation in that
    genus. This tab previously required both and returned nothing for a
    genus-only identification, which is the common case while an
    observation is still being worked out -- exactly when comparing it
    against one's own earlier collections is most useful.
    """
    if not genus:
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
                genus=str(row.get("genus") or "").strip(),
                species=str(row.get("species") or "").strip(),
            )
        )
    return result


class _EditorScrollArea(QScrollArea):
    """A :class:`QScrollArea` that reports its hosted widget's own size hint.

    ``QScrollArea.sizeHint()`` is a fixed style heuristic that ignores how
    large the hosted widget actually wants to be. Hosting the manual editor
    in a plain scroll area therefore hid the editor's size from the picker's
    derived default size, which opened ~250px too short and put scrollbars
    on the editor at its *default* size -- not just when deliberately
    shrunk.

    Only ``sizeHint`` is overridden, never ``minimumSizeHint``: the whole
    point of the scroll area is that the dialog can still be resized far
    below this hint, at which point the scrollbars are the correct
    fallback.
    """

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        widget = self.widget()
        if widget is None:
            return super().sizeHint()
        hint = widget.sizeHint()
        # Reserve one scrollbar extent on each axis so the hint describes a
        # size at which the editor fits with the scrollbars absent, rather
        # than one where showing a scrollbar immediately clips the content.
        extent = self.style().pixelMetric(QStyle.PM_ScrollBarExtent)
        margins = self.contentsMargins()
        frame = 2 * self.frameWidth()
        return QSize(
            hint.width() + extent + margins.left() + margins.right() + frame,
            hint.height() + extent + margins.top() + margins.bottom() + frame,
        )


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
        attach_callback: Callable[[str, str], None] | None = None,
        cloud_attach_callback: Callable[[dict], None] | None = None,
        manual_attach_callback: Callable[["ReferenceEntryEditor"], bool] | None = None,
        candidates: list[MeasurementSetCandidate] | None = None,
        my_observations: list[PersonalObservationCandidate] | None = None,
        community_results: list[dict] | None = None,
        ai_candidates: list[dict] | None = None,
        observation_points: list[dict] | None = None,
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
        # The Library list carries two independent states. The *preview*
        # candidate is the one row the shared preview pane is showing; the
        # *checked* ids are the zero or more sources the footer will add to
        # the plot. Conflating them is the bug this split exists to prevent:
        # a user must be able to look at a fourth source without silently
        # adding it, and must be able to keep three sources queued while
        # looking at a fourth.
        self._preview_candidate: MeasurementSetCandidate | None = None
        # Insertion-ordered, so a multi-source add happens in the order the
        # user chose the sources rather than in whatever order a set
        # iterates. Ids, not candidates, so a repository refresh cannot
        # leave the dialog holding a stale row object.
        self._checked_ids: list[str] = []
        # How many candidate rows the last repopulate actually showed, so
        # the footer hint can tell "no rows" from "rows, none checked"
        # without re-running the filter.
        self._visible_candidate_count = 0
        # True from the moment a "+ New publication…" editor is scheduled
        # until that editor has closed. A single click on the action row
        # emits itemSelectionChanged several times (QAbstractItemView's
        # mousePressEvent sets the current index and then the selection,
        # and the release can set it again), so without this every one of
        # those emissions would queue its own editor and the user would
        # have to cancel the modal once per emission.
        self._new_publication_editor_active = False
        self._my_observations: list[PersonalObservationCandidate] = []
        self._selected_observation: PersonalObservationCandidate | None = None
        # The user's own observation, summarized once from the measurements
        # the host handed in. The picker never queries them itself: the same
        # points are already loaded by MainWindow to decide whether the
        # observation can be plotted, and a second read here could disagree
        # with the first.
        self._observation_baseline: ObservationBaseline = observation_baseline_from_points(
            observation_points
        )
        # Filled once, after the candidate lists are loaded, and then frozen
        # for the rest of the session -- see _freeze_comparison_domains.
        self._comparison_domains: dict[str, MetricDomain] = {}
        # Assigned properly as each tab is added below. Seeded here because
        # ``CommunityResultsPane`` searches from its own constructor and asks
        # ``_community_tab_is_current`` whether it may paint -- which happens
        # before ``addTab`` has returned this tab's real index. -1 is the
        # correct answer at that moment: the picker opens on the Library tab.
        self._community_tab_index = -1
        self._my_observations_tab_index = -1
        self._manual_tab_index = -1

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
            self._manual_tab, QCoreApplication.translate("AddReferenceDialog", "Add new")
        )
        self.tabs.currentChanged.connect(self._on_tab_changed)

        footer = QHBoxLayout()
        self.status_hint_label = QLabel("", self)
        self.status_hint_label.setStyleSheet("color: #7f8c8d;")
        footer.addWidget(self.status_hint_label, 1)
        self.cancel_btn = QPushButton(QCoreApplication.translate("AddReferenceDialog", "Cancel"), self)
        self.cancel_btn.clicked.connect(self.reject)
        footer.addWidget(self.cancel_btn)
        self.add_to_plot_btn = QPushButton(QCoreApplication.translate("AddReferenceDialog", "Add to plot"), self)
        self.add_to_plot_btn.setEnabled(False)
        self.add_to_plot_btn.setDefault(True)
        self.add_to_plot_btn.clicked.connect(self._on_add_to_plot_clicked)
        footer.addWidget(self.add_to_plot_btn)
        root.addLayout(footer)

        self.preview_pane.clear()
        self._refresh_candidates()
        self._refresh_my_observations()
        self._freeze_comparison_domains()

        default_size, source_width, preview_width = self._derive_default_size()
        # The derived size is this dialog's DEFAULT, not a floor. At this
        # width every source tab and every preview sub-tab is visible with
        # no scroll-arrow fallback, which is what the default should give
        # the user. It must not also become setMinimumSize(): that floor
        # (≈1085x756 with the shipped fonts) exceeded the usable height of
        # a 768px-tall laptop screen, so the dialog could be grown but
        # never shrunk -- effectively unresizable. Below the default,
        # Qt's own tab-bar scroll arrows and the manual tab's scroll area
        # are the correct fallback for a deliberately narrow window.
        self.resize(default_size)
        self.setSizeGripEnabled(True)
        self._body_splitter.setSizes([source_width, preview_width])
        self._restore_geometry()
        self._restore_splitter_state()
        # After restoreState, never before: QSplitter::restoreState carries
        # childrenCollapsible in its saved stream, so a state written by an
        # earlier build silently restores that flag to True. Each pane keeps
        # its own natural minimumSizeHint and cannot be dragged away
        # entirely, so both stay reachable at any dialog size without
        # pinning a hard pixel width on either one.
        self._body_splitter.setChildrenCollapsible(False)
        self.finished.connect(self._save_geometry)
        self.finished.connect(self._save_splitter_state)
        self.finished.connect(self._community_pane.close)

    def _derive_default_size(self) -> tuple[QSize, int, int]:
        """Derive the smallest size at which all four source tabs and all
        five preview sub-tabs are visible without scroll arrows, from the
        widgets' own size hints rather than a hardcoded pixel value.

        This is the dialog's opening size, not a minimum: the user may
        resize below it, at which point Qt's tab-bar scroll arrows take
        over (see the note at the call site).

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
            # The scroll area's hint, not the bare editor's: it adds the
            # scrollbar allowance the editor needs to sit inside it without
            # a scrollbar appearing at the dialog's own default size.
            self._manual_scroll.sizeHint().width(),
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
        row.addWidget(QLabel(QCoreApplication.translate("AddReferenceDialog", "Reference taxon:"), self))
        self.taxon_target_combo = QComboBox(self)
        self.taxon_target_combo.setEditable(True)
        self.taxon_target_combo.setInsertPolicy(QComboBox.NoInsert)
        self.taxon_target_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.taxon_target_combo.setToolTip(
            QCoreApplication.translate("AddReferenceDialog",
                "Choose which taxon's published spore data to compare against. "
                "Select 'Use observation taxon' if available, search another taxon by typing genus and species, "
                "or choose an AI suggestion. This never changes the observation's own identification."
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
            QCoreApplication.translate("AddReferenceDialog", "Use observation taxon: {taxon}").format(taxon=own_label)
            if own_label
            else QCoreApplication.translate("AddReferenceDialog", "Search another taxon…"),
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
        self._preview_candidate = None
        # Checked sources are dropped, not carried across. They were chosen
        # as comparisons *for the previous taxon*, and the relevance groups
        # they were picked from have just been recomputed underneath them —
        # a row the user checked under "This taxon" may now sit in "Rest of
        # library". Silently adding those to a plot of a different taxon is
        # the worse failure; re-checking two rows is cheap.
        self._checked_ids.clear()
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
            self._populate_preview(self._preview_candidate)
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
        # Narrows the list to the picker's taxon. It composes with the
        # search box by AND, and with relevance grouping by preceding it:
        # scoping decides which rows exist, grouping only orders them, so
        # with this checked the list is normally one "This taxon" group.
        # Falls back to a name-text match (_taxon_target_query_text) when
        # the target has no taxon_id, so it still narrows the list for an
        # AI-suggested or freely-typed genus/species.
        self.only_this_taxon_checkbox.setChecked(True)
        self.only_this_taxon_checkbox.toggled.connect(self._on_filter_changed)
        filter_row.addWidget(self.only_this_taxon_checkbox)
        layout.addLayout(filter_row)

        self.results_list = LibraryResultsList(self._library_tab)
        # Elide long rows instead of growing a horizontal scrollbar; matches
        # the row-eliding convention in ui/comparison_panel.py.
        self.results_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        # Qt's default blue selection highlight is suppressed so the
        # approved green treatment LibrarySourceRow paints is the only
        # selected-row signal (design contract N8). Two rules, because a
        # selected row that loses focus is drawn with the Inactive palette
        # and would otherwise come back as a grey block.
        self.results_list.setStyleSheet(
            "QListWidget::item:selected { background: transparent; }"
            " QListWidget::item:selected:!active { background: transparent; }"
        )
        self.results_list.itemSelectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.results_list, 1)

    def _on_filter_changed(self, *_args) -> None:
        # Deliberately does NOT touch _checked_ids. Typing in the search box
        # is a question about the library, not a decision about the plot: a
        # row the user checked and then filtered out of view stays queued,
        # and the footer keeps counting it. Silently un-checking rows as the
        # visible set changes would lose a selection the user never undid.
        self._preview_candidate = None
        self._populate_results_list()
        self._update_footer_state()

    def _refresh_candidates(self) -> None:
        if self._injected_candidates is not None:
            self._candidates = list(self._injected_candidates)
        else:
            self._candidates = MeasurementSetRepository.list_attachment_candidates(
                exclude_ids=self._exclude_ids
            )
        # The library itself was reloaded (a "+ New publication…" editor has
        # just closed), so a checked id may no longer exist. Drop only those;
        # keep everything still attachable.
        known = {str(c.measurement_set_id) for c in self._candidates}
        self._checked_ids = [i for i in self._checked_ids if i in known]
        self._preview_candidate = None
        self._populate_results_list()
        self._update_footer_state()

    def _taxon_target_query_text(self) -> str:
        """Fallback text for taxon filtering when the current target has no
        ``taxon_id`` (an AI candidate or a freely-typed genus/species)."""
        if self._taxon_id is not None:
            return ""
        return " ".join(part for part in (self._genus, self._species) if part).strip()

    def _taxon_name_text(self) -> str:
        """The current target as a binomial, whether or not it has an id.

        Unlike :meth:`_taxon_target_query_text` this is not a fallback:
        relevance grouping wants the name in *both* cases, because a
        candidate row carries a published name even when it carries no
        ``taxon_id`` to compare against.
        """
        return " ".join(part for part in (self._genus, self._species) if part).strip()

    def _filtered_candidates(self) -> list[MeasurementSetCandidate]:
        return filter_library_candidates(
            self._candidates,
            taxon_id=self._taxon_id,
            only_this_taxon=self.only_this_taxon_checkbox.isChecked(),
            query=self.search_input.text(),
            taxon_text=self._taxon_target_query_text(),
        )

    def _grouped_candidates(self) -> list[tuple[str, list[MeasurementSetCandidate]]]:
        return group_library_candidates(
            self._filtered_candidates(),
            taxon_id=self._taxon_id,
            genus=self._genus,
            taxon_name=self._taxon_name_text(),
        )

    @staticmethod
    def _library_group_heading(group: str, count: int) -> str:
        if group == "this_taxon":
            template = QCoreApplication.translate("AddReferenceDialog", "This taxon ({count})")
        elif group == "same_genus":
            template = QCoreApplication.translate("AddReferenceDialog", "Same genus ({count})")
        else:
            template = QCoreApplication.translate("AddReferenceDialog", "Rest of library ({count})")
        return template.format(count=count)

    @staticmethod
    def _relevance_badge_text(group: str) -> str:
        """Badge wording for a relevance group, or ``""`` for no badge.

        "Rest of library" gets none: a badge there would label the absence
        of a relationship, which is what the grouping already shows.
        """
        if group == "this_taxon":
            return QCoreApplication.translate("AddReferenceDialog", "Same taxon")
        if group == "same_genus":
            return QCoreApplication.translate("AddReferenceDialog", "Same genus")
        return ""

    @staticmethod
    def _semantic_badge_text(candidate: MeasurementSetCandidate) -> str:
        """The data-semantics badge, straight from the Stage 1 projection.

        This method chooses wording only. Which badge a row deserves was
        decided by :attr:`SourceDisplay.data_label`, so no widget re-reads
        ``data_kind`` or a database column to guess (design contract
        N10–N14). An explicit percentile interval prints its real bounds, so
        a 10–90% source is never shown as 5–95%.
        """
        label = candidate.source_display().data_label
        if label.kind == "raw_data":
            return QCoreApplication.translate("AddReferenceDialog", "Raw data")
        if label.kind == "percentile_range" and label.percentile_bounds:
            low, high = label.percentile_bounds
            return QCoreApplication.translate(
                "AddReferenceDialog", "{low}–{high}% range"
            ).format(low=f"{low:g}", high=f"{high:g}")
        if label.kind == "published_range":
            return QCoreApplication.translate("AddReferenceDialog", "Published range")
        return ""

    def _add_group_heading(self, group: str, count: int) -> None:
        item = QListWidgetItem(self._library_group_heading(group, count))
        # Not selectable and not enabled: a heading is a label, and letting
        # it become the current row would clear the preview every time the
        # user arrow-keys past it.
        item.setFlags(Qt.NoItemFlags)
        item.setData(Qt.UserRole, _GROUP_HEADING_ROLE)
        item.setData(Qt.UserRole + 1, group)
        font = QFont(self.results_list.font())
        font.setBold(True)
        font.setPointSizeF(max(font.pointSizeF() - 1.0, 7.0))
        item.setFont(font)
        item.setForeground(self.palette().mid())
        self.results_list.addItem(item)

    def _populate_results_list(self) -> None:
        # Signals are blocked for the whole rebuild, and this is load-bearing.
        # ``clear()`` removes the current row from under the view, which walks
        # the current index onto whatever row survives longest -- and the row
        # that survives longest is "+ New publication…", the last item. The
        # resulting itemSelectionChanged would reach _on_selection_changed
        # with the action row selected and schedule a publication editor that
        # the user never asked for: toggling "Only this taxon" with a result
        # selected would pop a modal. Every caller of this method intends to
        # end with no selection, so the rebuild emits none.
        was_blocked = self.results_list.blockSignals(True)
        try:
            self._rebuild_results_items()
            self.results_list.setCurrentItem(None)
            self.results_list.clearSelection()
        finally:
            self.results_list.blockSignals(was_blocked)
        self._refresh_status_hint()
        self.preview_pane.clear()

    def _rebuild_results_items(self) -> None:
        self.results_list.clear()
        groups = self._grouped_candidates()
        for group, members in groups:
            self._add_group_heading(group, len(members))
            for candidate in members:
                self._add_candidate_item(candidate, group)
        new_pub_item = QListWidgetItem(QCoreApplication.translate("AddReferenceDialog", "+ New publication…"))
        new_pub_item.setData(Qt.UserRole, _NEW_PUBLICATION_ROLE)
        new_pub_item.setForeground(self.palette().link())
        # Selectable (that is how the action fires) but explicitly not
        # checkable: it is a command, never a source that could be queued
        # for the plot. Qt's default item flags include ItemIsUserCheckable,
        # so this has to be stated rather than assumed.
        new_pub_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
        self.results_list.addItem(new_pub_item)
        self._visible_candidate_count = sum(len(members) for _, members in groups)

    def _refresh_status_hint(self) -> None:
        """The single writer of the footer hint while the Library tab is up.

        One writer on purpose. The hint used to be set by both
        ``_populate_results_list`` and ``_update_footer_state``, and since
        the latter runs second on every filter change it silently overwrote
        the more specific message (see
        ``tests/test_library_taxon_scope_hint.py``). Both callers now route
        here instead of composing their own text.

        What is queued outranks why the list looks the way it does: once a
        source is checked, the count is the thing the user is about to act
        on.
        """
        if self.tabs.currentWidget() is not self._library_tab:
            return
        checked = len(self._checked_ids)
        if checked == 1:
            self.status_hint_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "1 source selected")
            )
            return
        if checked > 1:
            self.status_hint_label.setText(
                QCoreApplication.translate(
                    "AddReferenceDialog", "{count} sources selected"
                ).format(count=checked)
            )
            return
        self.status_hint_label.setText(
            "" if self._visible_candidate_count else self._empty_library_hint()
        )

    def _empty_library_hint(self) -> str:
        """Why the library list is empty, and what would un-empty it.

        "Only this taxon" is checked by default and ANDs with the search
        box, so a user searching the library for a genus while the picker
        sits on one species gets an empty list and no indication that the
        checkbox -- not their search text -- is what excluded the rows.
        Only say so when unchecking would genuinely reveal something.
        """
        if not self._candidates:
            return QCoreApplication.translate("AddReferenceDialog", "The reference library has no measurement sets yet.")
        if self.only_this_taxon_checkbox.isChecked():
            without_taxon_scope = filter_library_candidates(
                self._candidates,
                taxon_id=self._taxon_id,
                only_this_taxon=False,
                query=self.search_input.text(),
                taxon_text=self._taxon_target_query_text(),
            )
            if without_taxon_scope:
                return QCoreApplication.translate(
                    "AddReferenceDialog",
                    "No matching measurement sets for this taxon. {count} more match if you turn off “Only this taxon”.",
                ).format(count=len(without_taxon_scope))
        return QCoreApplication.translate("AddReferenceDialog", "No matching measurement sets in the library.")

    def _add_candidate_item(
        self, candidate: MeasurementSetCandidate, group: str = "rest"
    ) -> None:
        taxon = candidate.name_as_published or QCoreApplication.translate(
            "AddReferenceDialog", "Unnamed taxon"
        )
        citation = candidate.short_label or candidate.work_title or QCoreApplication.translate("AddReferenceDialog", "Untitled")
        # short_label conventionally already ends with the year (see
        # database.reference_citation.build_short_label); only append it
        # when genuinely missing, to avoid "... 2018 (2018)".
        if candidate.year and str(candidate.year) not in citation:
            citation = f"{citation} ({candidate.year})"
        if candidate.locator_text:
            citation = f"{citation} · {candidate.locator_text}"
        measurement = format_measurement_expression(candidate.source_display())

        item = QListWidgetItem()
        # The raw expression is the full, unrounded thing the source printed;
        # the row's compact cell is a rendering of the stored bounds. Keeping
        # it in the tooltip means the abbreviation never hides the original.
        tooltip_lines = [taxon, citation]
        if candidate.raw_text:
            tooltip_lines.append(str(candidate.raw_text))
        item.setToolTip("\n".join(line for line in tooltip_lines if line))
        item.setData(Qt.UserRole, candidate.measurement_set_id)
        item.setData(Qt.UserRole + 1, group)
        self.results_list.addItem(item)
        row_widget = LibrarySourceRow(
            measurement_set_id=candidate.measurement_set_id,
            taxon=taxon,
            citation=citation,
            measurement=measurement,
            relevance_badge=self._relevance_badge_text(group),
            semantic_badge=self._semantic_badge_text(candidate),
            parent=self.results_list,
        )
        # Before the signal is connected, so restoring a checked row during
        # a rebuild cannot look like the user clicking the checkbox.
        row_widget.set_checked_silently(
            str(candidate.measurement_set_id) in self._checked_ids
        )
        row_widget.check_toggled.connect(self._on_source_check_toggled)
        item.setSizeHint(row_widget.sizeHint())
        self.results_list.setItemWidget(item, row_widget)

    # -- Library selection / check state --------------------------------

    def _library_row_widgets(self) -> list[tuple[QListWidgetItem, LibrarySourceRow]]:
        widgets = []
        for row in range(self.results_list.count()):
            item = self.results_list.item(row)
            widget = self.results_list.itemWidget(item)
            if isinstance(widget, LibrarySourceRow):
                widgets.append((item, widget))
        return widgets

    def _row_for_measurement_set(self, measurement_set_id: str) -> int | None:
        for row in range(self.results_list.count()):
            if self.results_list.item(row).data(Qt.UserRole) == measurement_set_id:
                return row
        return None

    def _sync_selected_row_painting(self) -> None:
        """Tell each row whether it is the preview row.

        Qt's own highlight is off (see ``_build_library_tab``), so nothing
        draws the selected state unless the rows are told.
        """
        current = (
            self._preview_candidate.measurement_set_id
            if self._preview_candidate is not None
            else None
        )
        for _item, widget in self._library_row_widgets():
            widget.set_selected(widget.measurement_set_id == current)

    def _on_source_check_toggled(self, measurement_set_id: str, checked: bool) -> None:
        """A checkbox changed: update the queue, and preview what was checked.

        Checking also previews (design contract N7), because a user who has
        just decided to plot a source is the user most likely to want to
        look at it. The reverse does not hold and is not implemented:
        selecting a row leaves the queue alone.
        """
        measurement_set_id = str(measurement_set_id)
        if checked:
            if measurement_set_id not in self._checked_ids:
                self._checked_ids.append(measurement_set_id)
            row = self._row_for_measurement_set(measurement_set_id)
            if row is not None:
                self.results_list.setCurrentRow(row)
        elif measurement_set_id in self._checked_ids:
            self._checked_ids.remove(measurement_set_id)
        self._update_footer_state()

    def checked_source_ids(self) -> list[str]:
        """The sources queued for the plot, in the order the user checked them.

        Deduplicated by construction (``_on_source_check_toggled`` appends
        only ids it does not already hold), so no source can be attached
        twice. Includes ids whose row is currently filtered out of view:
        a search is not an un-check.
        """
        return list(self._checked_ids)

    def _on_selection_changed(self) -> None:
        """Selection moves the preview and nothing else.

        In particular it never touches ``_checked_ids``: looking at a source
        must not queue it for the plot (design contract N7).
        """
        items = self.results_list.selectedItems()
        if not items:
            self._preview_candidate = None
            self._sync_selected_row_painting()
            self.preview_pane.clear()
            self._update_footer_state()
            return
        role = items[0].data(Qt.UserRole)
        if role == _NEW_PUBLICATION_ROLE:
            self._preview_candidate = None
            self._sync_selected_row_painting()
            # Deferred deliberately, and this must not be inlined back.
            # itemSelectionChanged is emitted from inside
            # QListView::setSelection, which is itself still inside the
            # list's own mousePressEvent. _on_new_publication_clicked runs a
            # nested modal event loop and, when that dialog closes, calls
            # _refresh_candidates -> _populate_results_list ->
            # results_list.clear(). That destroys the very items Qt still
            # holds pointers to further up the stack, so the app segfaults
            # as the mouse event unwinds -- confirmed from a crash report
            # with QListWidget::clear called under QListView::setSelection.
            # Running it on the next event-loop turn lets the mouse event
            # finish first, leaving nothing live to invalidate.
            if self._new_publication_editor_active:
                # A later emission from the same click, or a selection
                # change while the editor is still open. One editor only.
                return
            self._new_publication_editor_active = True
            QTimer.singleShot(0, self._open_new_publication_editor)
            return
        candidate = next(
            (c for c in self._candidates if c.measurement_set_id == role),
            None,
        )
        self._preview_candidate = candidate
        self._sync_selected_row_painting()
        self._populate_preview(candidate)
        self._update_footer_state()

    def _freeze_comparison_domains(self) -> None:
        """Derive the session's three axes once, from what is known right now.

        Called after the Library and My-observations lists have loaded, so the
        axes cover the observation plus every candidate already on screen, and
        never again: contract N19 makes a fixed axis the whole basis of the
        comparison, and a dialog that rescaled when the taxon target changed
        would silently invalidate the shape the user had just read.

        Content that arrives afterwards -- a Community search result -- is not
        represented here by design. It is drawn clipped and marked rather than
        given a new axis (see ``BandView.clipped``).

        A raw-data Library row states no range, so its projection contributes
        no numbers: its extent comes from ``candidate.axis_extents()``, which
        the chooser query projected while loading the list. Without that, a
        library of measured spores would be sized entirely out of the axes and
        then drawn fully clipped the moment one was selected.
        """
        if self._comparison_domains:
            return
        self._comparison_domains = build_domains(
            baseline=self._observation_baseline,
            displays=[candidate.source_display() for candidate in self._candidates],
            point_sets=[candidate.points for candidate in self._my_observations],
            extents=[candidate.axis_extents() for candidate in self._candidates],
        )
        self.preview_pane.set_observation_baseline(self._baseline_comparison_view())

    def _comparison_view_for(self, display=None, source_points=None):
        """Build one comparison model on this session's frozen axes.

        The single place the dialog's axes and observation baseline are bound
        to a source, so every tab -- Library here, Community through the
        factory handed to ``CommunityResultsPane`` -- compares against the same
        two things. A tab that assembled its own ``comparison_view`` call could
        quietly pass different domains and produce a picture that cannot be
        compared with the previous selection's.
        """
        return comparison_view(
            domains=self._comparison_domains,
            baseline=self._observation_baseline,
            display=display,
            source_points=source_points,
        )

    def _baseline_comparison_view(self):
        """The no-selection model: the observation alone, on the frozen axes."""
        return self._comparison_view_for()

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
        # One projection of the stored row, and the comparison model built from
        # it. The old 3x4 table read the columns here directly and applied its
        # own extreme-or-typical fallback, which is how an inner typical range
        # came to be printed in a "Min"/"Max" column with no way to tell it
        # from reported extremes. The projection keeps outer and core apart and
        # names each one, so the pane can draw both and label both.
        display = (
            display_from_row(asdict(measurement_set), source_kind="library")
            if measurement_set is not None
            else candidate.source_display()
        )
        raw_points = self._decoded_raw_points(measurement_set)
        note = candidate.raw_text or QCoreApplication.translate("AddReferenceDialog", "No additional notes.")
        self.preview_pane.set_header(title, meta, note)
        self.preview_pane.set_comparison(
            self._comparison_view_for(display=display, source_points=raw_points)
        )
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

        if raw_points:
            self.preview_pane.set_raw_spore_points(raw_points)
        else:
            # A source that published a range has no per-spore rows, and the
            # tab says exactly that instead of dumping the raw JSON blob or --
            # far worse -- expanding the range into plausible-looking spores
            # (contract N22). A blob that would not decode gets the separate,
            # neutral wording: it is a Sporely problem, not a statement about
            # what the author printed.
            self.preview_pane.set_raw_spores_unavailable(
                plotted_as_band=display.has_any_range,
                unreadable=raw_points is None,
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
    def _decoded_raw_points(measurement_set: MeasurementSet | None) -> list | None:
        """The individual measurements a stored set really holds.

        ``None`` means *unreadable*, and is deliberately not the same answer as
        the empty list. A source with no ``raw_points_json`` published no
        individual measurements, which is a fact about the publication; a
        source whose stored blob is malformed JSON or is not a list establishes
        nothing about what its author published, only that Sporely cannot read
        what is on file. The Raw spores tab words those two differently, so
        collapsing them here would put a claim in the author's mouth.
        """
        if measurement_set is None or not measurement_set.raw_points_json:
            return []
        try:
            decoded = json.loads(measurement_set.raw_points_json)
        except ValueError:
            return None
        return decoded if isinstance(decoded, list) else None

    def _open_new_publication_editor(self) -> None:
        """Deferred entry point for the "+ New publication…" action row.

        Owns ``_new_publication_editor_active`` for the whole lifetime of
        the editor -- including while its nested modal event loop runs, so
        selection events delivered inside that loop cannot queue a second
        editor -- and releases the guard even if the editor raises.
        """
        try:
            self._clear_action_row_selection()
            self._on_new_publication_clicked()
        finally:
            self._new_publication_editor_active = False

    def _clear_action_row_selection(self) -> None:
        """Drop the selection on the action row, which is a command and not
        a selectable candidate.

        Signals are blocked because clearing re-enters
        ``_on_selection_changed``; the guard would stop it rescheduling an
        editor, but the empty-selection branch would still churn the
        preview and footer while an editor is about to open.
        """
        was_blocked = self.results_list.blockSignals(True)
        try:
            self.results_list.setCurrentItem(None)
            self.results_list.clearSelection()
        finally:
            self.results_list.blockSignals(was_blocked)
        self._preview_candidate = None
        self._sync_selected_row_painting()
        self.preview_pane.clear()
        self._update_footer_state()

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
            # The pane owns no axes of its own. It renders into this dialog's
            # frozen domains and against this dialog's observation, so a
            # community source is comparable by eye with the library source
            # the user looked at a moment ago.
            comparison_view_factory=self._comparison_view_for,
            # The pane's search and detail both complete asynchronously, so a
            # response can arrive after the user has moved to another source
            # tab. This is how the pane knows it is no longer the one on
            # screen; it keeps the response either way, and repaints from
            # ``sync_preview`` when the user comes back.
            preview_is_active=self._community_tab_is_current,
        )

        self._community_pane.selection_changed.connect(self._update_footer_state)
        layout.addWidget(self._community_pane, 1)

    def _community_tab_is_current(self) -> bool:
        return self.tabs.currentIndex() == self._community_tab_index

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
        if self._my_observations:
            self.my_observations_status_label.setText("")
        elif not str(self._genus or "").strip():
            # Distinct from the "none have measurements" case below, which
            # would be a false statement here: with no genus the query never
            # ran, so nothing has been looked at yet.
            self.my_observations_status_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "Select a taxon to browse your own observations of it.")
            )
        else:
            self.my_observations_status_label.setText(
                QCoreApplication.translate("AddReferenceDialog", "No previous observations of this taxon have spore measurements.")
            )
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            self.preview_pane.clear()

    def _add_observation_item(self, candidate: PersonalObservationCandidate) -> None:
        label = (
            QCoreApplication.translate("AddReferenceDialog", "My observation — {author}").format(author=candidate.author)
            if candidate.author
            else QCoreApplication.translate("AddReferenceDialog", "My observation")
        )
        detail_parts: list[str] = []
        # Browsing a whole genus puts several species in one list, where the
        # date and author alone do not say which species a row is. Mirrors
        # the same disambiguation the Library tab does when "Only this
        # taxon" is off (see _add_candidate_item).
        if not str(self._species or "").strip():
            row_taxon = " ".join(
                part for part in (candidate.genus, candidate.species) if part
            ).strip()
            if row_taxon:
                detail_parts.append(row_taxon)
        if candidate.date:
            detail_parts.append(candidate.date)
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
        meta_parts: list[str] = []
        # Always name the row's taxon here, even when the list is pinned to
        # one species: this is the pane the user reads before adding the
        # series to the plot, so what is about to be attached should be
        # stated rather than inferred from the tab's current filter.
        row_taxon = " ".join(
            part for part in (candidate.genus, candidate.species) if part
        ).strip()
        if row_taxon:
            meta_parts.append(row_taxon)
        if candidate.date:
            meta_parts.append(candidate.date)
        if candidate.location:
            meta_parts.append(candidate.location)
        meta = " · ".join(meta_parts)

        note = QCoreApplication.translate("AddReferenceDialog", "n = {count} spore measurements").format(count=candidate.n)
        # A personal observation *is* individual measurements, so its
        # projection states no range at all and the comparison derives its
        # bands from the points themselves -- labelled as measured extremes,
        # a measured 5-95 interval and a measured median, never as something
        # anybody published. The old 3x4 table printed a bare min / mean / max
        # with no such distinction, which read identically to a monograph's
        # reported figures.
        display = display_from_points(candidate.points, source_kind="observation")
        self.preview_pane.set_header(title, meta, note)
        self.preview_pane.set_comparison(
            self._comparison_view_for(display=display, source_points=candidate.points)
        )
        # A personal observation is not a published/community reference, so
        # there is no reported-source provenance to summarize here.
        self.preview_pane.set_provenance_summary("")

        # Real rows, not the stored JSON blob: these points genuinely are per
        # spore measurements, which is the one case the Raw spores tab is for.
        self.preview_pane.set_raw_spore_points(candidate.points)
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
        # The editor's own minimum size hint (its measurement tables) is the
        # widest and tallest thing in the picker, and was what stopped the
        # dialog shrinking even once the explicit floors were removed.
        # Scrolling it here -- in the picker only, not in the shared editor
        # widget, which the legacy Quick-add dialog also hosts -- lets the
        # dialog be resized small while keeping every field reachable.
        self._manual_scroll = _EditorScrollArea(self._manual_tab)
        self._manual_scroll.setWidgetResizable(True)
        self._manual_scroll.setFrameShape(QScrollArea.NoFrame)
        self._manual_scroll.setWidget(self.manual_editor)
        layout.addWidget(self._manual_scroll)

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------

    def _update_footer_state(self) -> None:
        if not hasattr(self, "add_to_plot_btn"):
            # The manual editor's construction synchronously refreshes its
            # preview (and emits data_changed) before the footer button
            # exists yet; nothing to update this early.
            return
        default_text = QCoreApplication.translate("AddReferenceDialog", "Add to plot")
        if self.tabs.currentIndex() == self._my_observations_tab_index:
            self.add_to_plot_btn.setText(default_text)
            self.add_to_plot_btn.setToolTip("")
            self.add_to_plot_btn.setEnabled(self._selected_observation is not None)
        elif self.tabs.currentIndex() == self._community_tab_index:
            self.add_to_plot_btn.setText(default_text)
            self.add_to_plot_btn.setToolTip("")
            self.add_to_plot_btn.setEnabled(self._community_pane.has_selection())
        elif self.tabs.currentIndex() == self._manual_tab_index:
            self.add_to_plot_btn.setText(default_text)
            self.add_to_plot_btn.setToolTip("")
            self.add_to_plot_btn.setEnabled(self.manual_editor.is_ready_to_submit())
        else:
            checked = len(self._checked_ids)
            self.add_to_plot_btn.setText(
                QCoreApplication.translate(
                    "AddReferenceDialog", "Add {count} to plot"
                ).format(count=checked)
                if checked > 1
                else default_text
            )
            if checked > 1:
                # Honest, and the reason it is not in the hint (which the
                # contract reserves for the count) but on the button that
                # cannot be pressed. See _on_add_to_plot_clicked.
                self.add_to_plot_btn.setToolTip(
                    QCoreApplication.translate(
                        "AddReferenceDialog",
                        "Adding several sources at once is not available yet. "
                        "Leave one source checked, or add them one at a time.",
                    )
                )
            else:
                self.add_to_plot_btn.setToolTip("")
            self.add_to_plot_btn.setEnabled(
                checked == 1 or (checked == 0 and self._preview_candidate is not None)
            )
        self._refresh_status_hint()

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
        if self._attach_callback is None:
            return
        checked = self.checked_source_ids()
        if len(checked) > 1:
            # Not reachable through the UI: the button is disabled for more
            # than one checked source (see _update_footer_state).
            #
            # Multi-attach is deliberately NOT wired here. ``attach_callback``
            # is ``(measurement_set_id, role) -> None``: its host
            # (``MainWindow._attach_normalized_reference_to_active_
            # observation``) returns ``None`` whether the attach succeeded,
            # was refused because no observation is active, raised
            # ``ReferenceLibraryError``, or produced an untranslatable
            # snapshot. A loop over that callback could not tell which
            # sources landed, so it could neither keep the failed ones for a
            # retry nor avoid closing the dialog on a claim of success it
            # has no evidence for. Failure propagation for this path is part
            # of the unlanded ``feature/reference-save-and-plot`` work the
            # redesign plan explicitly forbids recreating piecemeal.
            return
        target = checked[0] if checked else (
            self._preview_candidate.measurement_set_id
            if self._preview_candidate is not None
            else None
        )
        if target is None:
            return
        self._attach_callback(target, "compared")
        self.accept()


__all__ = [
    "AddReferenceDialog",
    "LIBRARY_GROUP_ORDER",
    "filter_library_candidates",
    "format_ai_candidate_display",
    "group_library_candidates",
    "library_relevance",
    "PersonalObservationCandidate",
    "default_my_observation_candidates",
]
