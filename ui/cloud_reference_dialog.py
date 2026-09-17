"""Dialog for reviewing and importing community spore data."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import QCoreApplication, QEvent, QModelIndex, QStringListModel, QThread, Qt, QTimer, Signal
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QAbstractItemView,
    QButtonGroup,
    QCompleter,
    QDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.models import SettingsDB, SpeciesDataAvailability
from database.taxon_lookup import TAXON_COMPLETER_LIMIT, TaxonChoice, TaxonLookupService
from database.vernacular_db import VernacularDB
from utils.cloud_sync import CloudSyncError, SporelyCloudClient
from utils.vernacular_utils import (
    common_name_display_label,
    normalize_vernacular_language,
    resolve_available_vernacular_language,
    resolve_vernacular_db_path,
)

from .delegates import SpeciesItemDelegate
from .dialog_helpers import make_github_help_button
from .reference_preview_pane import ReferencePreviewPane
from .taxon_input_controller import TaxonInputController
from .two_line_row import TwoLineRow

# Quiet period after the last keystroke in the Community tab's search field
# before a search is issued. One search is a QThread plus three network RPCs,
# so this exists to keep an unthrottled keystroke from spending that on every
# prefix of the word being typed.
_COMMUNITY_SEARCH_DEBOUNCE_MS = 350

# Shortest genus a community search will be issued for. The server matches the
# genus exactly (``lower(o.genus) = lower(p_genus)`` in
# ``search_community_spore_datasets``), never as a prefix, so a one- or
# two-letter fragment cannot match anything and is not worth a round trip.
_COMMUNITY_MIN_GENUS_CHARS = 3


def _should_select_all_on_focus(event) -> bool:
    reason_getter = getattr(event, "reason", None)
    if callable(reason_getter):
        try:
            return reason_getter() != Qt.PopupFocusReason
        except Exception:
            return True
    return True


class _CloudSearchWorker(QThread):
    # Use a distinct name to avoid shadowing QThread.finished, which fires after run() returns
    # and is needed for safe lifecycle management.
    search_done = Signal(list, dict)
    error = Signal(str)

    def __init__(self, genus: str, species: str):
        super().__init__()
        self.setObjectName("Cloud reference search")
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()

    def run(self) -> None:
        try:
            client = SporelyCloudClient.from_stored_credentials()
            if not client:
                raise CloudSyncError("Sign in to Sporely Cloud to search community spore data.")
            observation_rows = client.search_community_spore_datasets(self._genus, self._species, limit=50)
            reference_rows = client.search_public_reference_values(self._genus, self._species, limit=50)
            summary = client.community_spore_taxon_summary(self._genus, self._species) or {}
            combined: list[dict[str, Any]] = []
            for row in observation_rows or []:
                item = dict(row or {})
                item["_kind"] = "observation"
                combined.append(item)
            for row in reference_rows or []:
                item = dict(row or {})
                item["_kind"] = "reference"
                combined.append(item)
            combined.sort(
                key=lambda row: (
                    0 if row.get("_kind") == "observation" else 1,
                    -(int(row.get("measurement_count") or 0)),
                    str(row.get("updated_at") or ""),
                )
            )
            self.search_done.emit(combined, dict(summary or {}))
        except Exception as exc:
            self.error.emit(str(exc))


class _CloudDetailWorker(QThread):
    # Use a distinct name to avoid shadowing QThread.finished.
    detail_done = Signal(dict)
    error = Signal(str)

    def __init__(self, result_row: dict[str, Any]):
        super().__init__()
        self.setObjectName("Cloud reference detail")
        self._row = dict(result_row or {})

    def run(self) -> None:
        try:
            kind = str(self._row.get("_kind") or "").strip()
            if kind == "reference":
                self.detail_done.emit(self._row)
                return
            obs_id = int(self._row.get("observation_id") or 0)
            if obs_id <= 0:
                raise CloudSyncError("Missing observation id for community dataset.")
            client = SporelyCloudClient.from_stored_credentials()
            if not client:
                raise CloudSyncError("Sign in to Sporely Cloud to load dataset details.")
            detail = client.get_community_spore_dataset(obs_id)
            if not isinstance(detail, dict) or not detail:
                raise CloudSyncError("No dataset details were returned.")
            detail["_kind"] = "observation"
            self.detail_done.emit(detail)
        except Exception as exc:
            self.error.emit(str(exc))


def format_stat_value(value: Any, decimals: int = 2) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{decimals}f}"
    except Exception:
        return str(value)


def community_result_source_label(row: dict[str, Any], tr: Callable[[str], str]) -> str:
    kind = str(row.get("_kind") or "").strip()
    species_tag = str(row.get("species") or "").strip()
    if species_tag:
        genus_tag = str(row.get("genus") or "").strip()
        species_tag = f"{genus_tag} {species_tag}".strip() if genus_tag else species_tag
    if kind == "reference":
        source = str(row.get("source") or "").strip() or tr("Reference values")
        mount = str(row.get("mount_medium") or "").strip()
        stain = str(row.get("stain") or "").strip()
        prep = ", ".join(part for part in (mount, stain) if part)
        label = f"{source} [{prep}]".strip() if prep else source
        return f"{species_tag} – {label}" if species_tag else label
    observed_on = str(row.get("observed_on") or "").strip()
    if observed_on:
        base = tr("Community observation {date}").format(date=observed_on)
    else:
        base = tr("Community observation")
    return f"{species_tag} – {base}" if species_tag else base


def community_result_q_range_label(row: dict[str, Any]) -> str:
    q_min = row.get("q_min")
    q_p50 = row.get("q_p50")
    q_max = row.get("q_max")
    if q_min is None and q_p50 is None and q_max is None:
        length = row.get("length_p05")
        width = row.get("width_p50")
        if length is None and width is None:
            return "—"
    parts = []
    if q_min is not None:
        parts.append(f"{float(q_min):.2f}")
    if q_p50 is not None:
        parts.append(f"{float(q_p50):.2f}")
    if q_max is not None and (not parts or parts[-1] != f"{float(q_max):.2f}"):
        parts.append(f"{float(q_max):.2f}")
    return " / ".join(parts) if parts else "—"


def community_default_import_source(detail: dict[str, Any], tr: Callable[[str], str]) -> str:
    contributor = str(detail.get("contributor_label") or "").strip()
    observed_on = str(detail.get("observed_on") or "").strip()
    if detail.get("_kind") == "reference":
        source = str(detail.get("source") or "").strip()
        return f"Cloud: {source}".strip() if source else tr("Cloud reference")
    parts = [part for part in (contributor, observed_on) if part]
    suffix = " - ".join(parts)
    return f"Cloud: {suffix}".strip() if suffix else tr("Cloud observation")


def community_single_detail_value(detail: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = detail.get(key)
        if isinstance(value, list):
            cleaned = [str(item).strip() for item in value if str(item).strip()]
            if len(cleaned) == 1:
                return cleaned[0]
            if cleaned:
                return None
        text = str(value or "").strip()
        if text:
            return text
    return None


def community_summary_metadata_payload(detail: dict[str, Any], tr: Callable[[str], str]) -> dict[str, Any]:
    return {
        "source_type": "cloud",
        "cloud_dataset_kind": str(detail.get("_kind") or "").strip() or "observation",
        "cloud_observation_id": detail.get("observation_id"),
        "cloud_reference_id": detail.get("reference_id"),
        "cloud_source_label": community_default_import_source(detail, tr),
        "contributor_label": str(detail.get("contributor_label") or "").strip() or None,
        "observed_on": str(detail.get("observed_on") or "").strip() or None,
        "license": detail.get("license"),
        "qc_flags": detail.get("qc_flags") or {},
        "imported_via": "cloud_reference_dialog",
        "imported_at": datetime.now().isoformat(timespec="seconds"),
    }


def community_summary_reference_payload(detail: dict[str, Any], tr: Callable[[str], str]) -> dict[str, Any] | None:
    genus = str(detail.get("genus") or "").strip()
    species = str(detail.get("species") or "").strip()
    if not genus or not species:
        return None
    return {
        "genus": genus,
        "species": species,
        "source": community_default_import_source(detail, tr),
        "mount_medium": community_single_detail_value(detail, "mount_media", "mount_medium"),
        "stain": community_single_detail_value(detail, "stains", "stain"),
        "length_min": detail.get("length_min"),
        "length_p05": detail.get("length_p05"),
        "length_p50": detail.get("length_p50"),
        "length_p95": detail.get("length_p95"),
        "length_max": detail.get("length_max"),
        "length_avg": detail.get("length_avg"),
        "width_min": detail.get("width_min"),
        "width_p05": detail.get("width_p05"),
        "width_p50": detail.get("width_p50"),
        "width_p95": detail.get("width_p95"),
        "width_max": detail.get("width_max"),
        "width_avg": detail.get("width_avg"),
        "q_min": detail.get("q_min"),
        "q_p50": detail.get("q_p50"),
        "q_max": detail.get("q_max"),
        "q_avg": detail.get("q_avg"),
        "source_kind": "reference",
        "source_type": "cloud",
        "metadata_json": community_summary_metadata_payload(detail, tr),
    }


def community_points_payload(detail: dict[str, Any], tr: Callable[[str], str]) -> dict[str, Any] | None:
    genus = str(detail.get("genus") or "").strip()
    species = str(detail.get("species") or "").strip()
    measurements = detail.get("measurements_json") or []
    if not genus or not species or not isinstance(measurements, list):
        return None
    points = []
    for row in measurements:
        if not isinstance(row, dict):
            continue
        length = row.get("length_um")
        width = row.get("width_um")
        if length is None or width in (None, 0):
            continue
        try:
            points.append({"length_um": float(length), "width_um": float(width)})
        except Exception:
            continue
    if not points:
        return None
    return {
        "genus": genus,
        "species": species,
        "points": points,
        "points_label": community_default_import_source(detail, tr),
        "source_kind": "points",
        "source_type": "cloud",
        "length_min": detail.get("length_min"),
        "length_p05": detail.get("length_p05"),
        "length_p50": detail.get("length_p50"),
        "length_p95": detail.get("length_p95"),
        "length_max": detail.get("length_max"),
        "length_avg": detail.get("length_avg"),
        "width_min": detail.get("width_min"),
        "width_p05": detail.get("width_p05"),
        "width_p50": detail.get("width_p50"),
        "width_p95": detail.get("width_p95"),
        "width_max": detail.get("width_max"),
        "width_avg": detail.get("width_avg"),
        "q_min": detail.get("q_min"),
        "q_p50": detail.get("q_p50"),
        "q_max": detail.get("q_max"),
        "q_avg": detail.get("q_avg"),
    }


def community_detail_preview_fields(detail: dict[str, Any], tr: Callable[[str], str]) -> dict[str, Any]:
    """Compute every field a review pane needs for one community dataset detail.

    Shared by :class:`CloudReferenceDialog` and the Community tab's
    ``CommunityResultsPane`` so both apply the identical computation to
    whichever :class:`~ui.reference_preview_pane.ReferencePreviewPane`
    instance they own, through that pane's public ``set_*`` API.
    """
    kind = str(detail.get("_kind") or "").strip()
    genus = str(detail.get("genus") or "").strip()
    species = str(detail.get("species") or "").strip()
    contributor = str(detail.get("contributor_label") or "—")
    observed_on = str(detail.get("observed_on") or "").strip()
    measurement_count = int(detail.get("measurement_count") or 0)

    title = f"{genus} {species}".strip() or tr("Community dataset")
    if kind == "reference":
        title += f" ({tr('Reference')})"
    else:
        title += f" ({tr('Observation dataset')})"
    meta = tr("Contributor: {contributor}  •  Date: {date}  •  n={count}").format(
        contributor=contributor,
        date=observed_on or "—",
        count=measurement_count,
    )

    rows: list[tuple[str, str, str, str]] = []
    for key in ("length", "width", "q"):
        median_value = detail.get(f"{key}_p50")
        if median_value is None:
            median_value = detail.get(f"{key}_avg")
        rows.append(
            (
                tr(key.capitalize()),
                format_stat_value(detail.get(f"{key}_min")),
                format_stat_value(median_value),
                format_stat_value(detail.get(f"{key}_max")),
            )
        )

    qc_flags = detail.get("qc_flags") or {}
    qc_lines = []
    if isinstance(qc_flags, dict):
        for key, label in (
            ("has_mount", tr("Mount recorded")),
            ("has_stain", tr("Stain recorded")),
            ("has_sample_type", tr("Sample type recorded")),
            ("has_contrast", tr("Contrast recorded")),
            ("has_objective", tr("Objective recorded")),
            ("has_scale", tr("Scale recorded")),
            ("has_point_geometry", tr("Measurement points recorded")),
        ):
            if qc_flags.get(key):
                qc_lines.append(label)
    note = tr("QC signals: {signals}").format(
        signals=", ".join(qc_lines) if qc_lines else tr("No extra QC metadata")
    )

    measurements = detail.get("measurements_json") or []
    raw_lines = []
    for row in measurements[:200]:
        if not isinstance(row, dict):
            continue
        raw_lines.append(
            f"L={format_stat_value(row.get('length_um'))}  "
            f"W={format_stat_value(row.get('width_um'))}  "
            f"Q={format_stat_value((float(row.get('length_um')) / float(row.get('width_um'))) if row.get('length_um') is not None and row.get('width_um') not in (None, 0) else None)}"
        )
    raw_text = "\n".join(raw_lines) if raw_lines else tr("No raw point data returned.")

    def _join_list(value: Any) -> str:
        if isinstance(value, list):
            cleaned = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(cleaned) if cleaned else "—"
        text = str(value or "").strip()
        return text or "—"

    method_mapping = {
        "mount": _join_list(detail.get("mount_media") or detail.get("mount_medium")),
        "stain": _join_list(detail.get("stains") or detail.get("stain")),
        "sample_type": _join_list(detail.get("sample_types") or detail.get("sample_type")),
        "contrast": _join_list(detail.get("contrasts") or detail.get("contrast")),
        "objective": _join_list(detail.get("objectives") or detail.get("objective_name")),
    }
    scale_min = detail.get("scale_min")
    scale_max = detail.get("scale_max")
    if scale_min is not None or scale_max is not None:
        if scale_min == scale_max or scale_max is None:
            scale_text = f"{format_stat_value(scale_min)} µm/px"
        else:
            scale_text = f"{format_stat_value(scale_min)}-{format_stat_value(scale_max)} µm/px"
    else:
        scale_text = format_stat_value(detail.get("scale_microns_per_pixel"))
        if scale_text != "—":
            scale_text += " µm/px"
    method_mapping["scale"] = scale_text

    calibration_lines = []
    if scale_text != "—":
        calibration_lines.append(f"{tr('Scale')}: {scale_text}")
    if kind == "observation":
        calibration_lines.append(
            tr("Calibration details come from image/objective metadata in the synced observation dataset.")
        )
    else:
        calibration_lines.append(tr("Reference rows currently expose summary values only."))
    calibration_text = "\n".join(calibration_lines)

    provenance_lines = [
        f"{tr('Kind')}: {kind or '—'}",
        f"{tr('Contributor')}: {contributor}",
        f"{tr('Date')}: {observed_on or '—'}",
    ]
    if kind == "reference":
        provenance_lines.append(f"{tr('Source')}: {detail.get('source') or '—'}")
        provenance_lines.append(tr("Imported reference values are currently treated as shared reference material."))
    else:
        provenance_lines.append(f"{tr('Observation id')}: {detail.get('observation_id') or '—'}")
        provenance_lines.append(tr("Location and private observation content are intentionally excluded from this review flow."))
    provenance_text = "\n".join(provenance_lines)

    method_recorded = any(
        method_mapping[key] != "—"
        for key in ("mount", "stain", "sample_type", "contrast", "objective")
    )
    not_reported = tr("not reported")
    provenance_summary = tr(
        "Reported by: {contributor} ({date}) · sample size: {size} · method recorded: {method}"
    ).format(
        contributor=contributor,
        date=observed_on or not_reported,
        size=measurement_count if measurement_count else not_reported,
        method=tr("yes") if method_recorded else not_reported,
    )

    return {
        "title": title,
        "meta": meta,
        "rows": rows,
        "note": note,
        "raw_text": raw_text,
        "method_mapping": method_mapping,
        "calibration_text": calibration_text,
        "provenance_text": provenance_text,
        "provenance_summary": provenance_summary,
        "points_count": len(measurements) if isinstance(measurements, list) else 0,
    }


class CloudReferenceDialog(QDialog):
    """Search, review, and import community spore data."""

    _ROLE_TAXON_CHOICE = Qt.UserRole + 4

    def __init__(
        self,
        parent=None,
        *,
        genus: str = "",
        species: str = "",
        vernacular: str = "",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Search Community Spore Data"))
        self.setModal(True)
        self.setMinimumSize(980, 640)
        self.resize(1080, 700)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)

        self._vernacular_db = None
        self._taxon_lookup = None
        self._species_availability = getattr(parent, "species_availability", None)
        if not isinstance(self._species_availability, SpeciesDataAvailability):
            self._species_availability = SpeciesDataAvailability()
        self._search_worker: _CloudSearchWorker | None = None
        self._detail_worker: _CloudDetailWorker | None = None
        # Strong Python refs to workers that have finished emitting but whose
        # QThread.finished hasn't fired yet (i.e. run() hasn't returned).
        # Prevents PySide6 GC from collecting the wrapper while the OS thread is live.
        self._worker_refs: list[QThread] = []
        self._results: list[dict[str, Any]] = []
        self._community_summary: dict[str, Any] = {}
        self._selected_result: dict[str, Any] | None = None
        self._selected_detail: dict[str, Any] | None = None
        self._accepted_action: str | None = None
        self._accepted_data: dict[str, Any] | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        intro = QLabel(
            self.tr(
                "Search public and friend-visible community spore datasets, review measurement quality, "
                "and import only after checking the method and calibration context."
            )
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        search_row = QHBoxLayout()
        search_row.setSpacing(8)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.vernacular_label = QLabel(self._vernacular_label())
        self.vernacular_input = QLineEdit()
        self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.genus_input = QLineEdit(genus or "")
        self.genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.species_input = QLineEdit(species or "")
        self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))

        form.addRow(self.vernacular_label, self.vernacular_input)
        form.addRow(self.tr("Genus:"), self.genus_input)
        form.addRow(self.tr("Species:"), self.species_input)
        search_row.addLayout(form, 1)

        actions_col = QVBoxLayout()
        actions_col.setContentsMargins(0, 0, 0, 0)
        actions_col.setSpacing(8)
        self.search_button = QPushButton(self.tr("Search"))
        self.search_button.clicked.connect(self._on_search_clicked)
        self.search_button.setDefault(True)
        actions_col.addWidget(self.search_button)
        actions_col.addWidget(
            make_github_help_button(self, "community-spore-data-plan.md"),
            0,
            Qt.AlignRight | Qt.AlignTop,
        )
        actions_col.addStretch(1)
        search_row.addLayout(actions_col)
        root.addLayout(search_row)

        splitter = QSplitter(Qt.Horizontal)
        splitter.setChildrenCollapsible(False)
        root.addWidget(splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        left_title = QLabel(self.tr("Results"))
        left_title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(left_title)

        self.results_table = QTableWidget(0, 4)
        self.results_table.setFocusPolicy(Qt.NoFocus)
        self.results_table.setHorizontalHeaderLabels(
            [self.tr("Source"), self.tr("n"), self.tr("Q / L-W"), self.tr("Contributor")]
        )
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.verticalHeader().setVisible(False)
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.results_table.itemSelectionChanged.connect(self._on_result_selection_changed)
        left_layout.addWidget(self.results_table, 1)

        self.search_status_label = QLabel("")
        self.search_status_label.setWordWrap(True)
        self.search_status_label.setStyleSheet("color: #7f8c8d;")
        left_layout.addWidget(self.search_status_label)
        splitter.addWidget(left_panel)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        preview_title = QLabel(self.tr("Review"))
        preview_title.setStyleSheet("font-weight: 600;")
        right_layout.addWidget(preview_title)

        self._preview_pane = ReferencePreviewPane()
        right_layout.addWidget(self._preview_pane, 1)

        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)

        button_row = QHBoxLayout()
        button_row.setSpacing(8)
        self.footer_hint = QLabel("")
        self.footer_hint.setWordWrap(True)
        self.footer_hint.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.footer_hint.setStyleSheet("color: #7f8c8d;")
        button_row.addWidget(self.footer_hint, 1)

        self.import_summary_button = QPushButton(self.tr("Import summary as reference"))
        self.import_summary_button.setEnabled(False)
        self.import_summary_button.clicked.connect(self._on_import_summary_clicked)
        button_row.addWidget(self.import_summary_button)

        self.plot_points_button = QPushButton(self.tr("Use raw points for plot"))
        self.plot_points_button.setEnabled(False)
        self.plot_points_button.clicked.connect(self._on_plot_points_clicked)
        button_row.addWidget(self.plot_points_button)

        close_button = QPushButton(self.tr("Close"))
        close_button.clicked.connect(self.reject)
        button_row.addWidget(close_button)
        root.addLayout(button_row)

        self._init_completers()
        if vernacular and not self.vernacular_input.text().strip():
            self.vernacular_input.setText(vernacular)
        self._maybe_set_vernacular_from_taxon()
        self._reset_preview()

    def _track_worker(self, worker: QThread) -> None:
        """Keep a strong Python ref to *worker* until after QThread.finished fires."""
        self._worker_refs.append(worker)
        worker.finished.connect(lambda: self._release_worker(worker))

    def _release_worker(self, worker: QThread) -> None:
        try:
            self._worker_refs.remove(worker)
        except ValueError:
            pass
        worker.deleteLater()

    # ------------------------------------------------------------------
    # Delegating properties — keep existing attribute names working
    # ------------------------------------------------------------------

    @property
    def review_tabs(self) -> QTabWidget:
        return self._preview_pane.review_tabs

    @property
    def summary_title_label(self) -> QLabel:
        return self._preview_pane.summary_title_label

    @property
    def summary_meta_label(self) -> QLabel:
        return self._preview_pane.summary_meta_label

    @property
    def summary_table(self) -> QTableWidget:
        return self._preview_pane.summary_table

    @property
    def summary_note_label(self) -> QLabel:
        return self._preview_pane.summary_note_label

    @property
    def raw_spores_text(self) -> QPlainTextEdit:
        return self._preview_pane.raw_spores_text

    @property
    def _method_labels(self) -> dict[str, QLabel]:
        return self._preview_pane._method_labels

    @property
    def calibration_text(self) -> QPlainTextEdit:
        return self._preview_pane.calibration_text

    @property
    def provenance_text(self) -> QPlainTextEdit:
        return self._preview_pane.provenance_text

    def closeEvent(self, event) -> None:
        if self._search_worker is not None:
            self._search_worker.wait()
            self._search_worker = None
        if self._detail_worker is not None:
            self._detail_worker.wait()
            self._detail_worker = None
        super().closeEvent(event)

    def accepted_action(self) -> str | None:
        return self._accepted_action

    def accepted_data(self) -> dict[str, Any] | None:
        return dict(self._accepted_data or {}) if isinstance(self._accepted_data, dict) else None

    def _vernacular_label(self) -> str:
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        base = self.tr("Common name")
        return f"{common_name_display_label(lang, base)}:"

    def _vernacular_placeholder(self) -> str:
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        examples = {
            "no": "Kantarell",
            "de": "Pfifferling",
            "fr": "Girolle",
            "es": "Rebozuelo",
            "da": "Kantarel",
            "sv": "Kantarell",
            "fi": "Kantarelli",
            "pl": "Kurka",
            "pt": "Cantarelo",
            "it": "Gallinaccio",
        }
        return f"e.g., {examples.get(lang, 'Chanterelle')}"

    def _clean_species_text(self, text: str | None) -> str:
        return str(text or "").strip()

    def _clean_genus_text(self, text: str | None) -> str:
        token = str(text or "").strip().split()
        return token[0].strip() if token else ""

    def _set_species_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
            return
        self.species_input.setPlaceholderText(f"e.g., {'; '.join(cleaned[:4])}")

    def _set_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        self.vernacular_input.setPlaceholderText(f"e.g., {'; '.join(cleaned[:4])}")

    def _format_species_choice_display(self, choice: TaxonChoice) -> str:
        return self._clean_species_text(choice.species)

    def _format_common_name_choice_display(self, choice: TaxonChoice) -> str:
        return str(choice.common_name or "").strip()

    def _choice_from_index(self, index: QModelIndex) -> TaxonChoice | None:
        if not index.isValid():
            return None
        choice = index.data(self._ROLE_TAXON_CHOICE)
        if isinstance(choice, TaxonChoice):
            return choice
        genus = self._clean_genus_text(index.data(Qt.UserRole + 1))
        species = self._clean_species_text(index.data(Qt.UserRole + 2))
        common_name = str(index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip() or None
        if genus and species:
            return TaxonChoice(genus=genus, species=species, common_name=common_name)
        return None

    def _customize_species_item(self, item: QStandardItem, choice: TaxonChoice) -> None:
        has_data = False
        if self._species_availability and choice.genus and choice.species:
            try:
                info = self._species_availability.get_detailed_info(choice.genus, choice.species)
                has_data = bool(
                    info.get("has_personal_points")
                    or info.get("has_shared_points")
                    or info.get("has_published_points")
                    or info.get("has_reference_minmax")
                )
            except Exception:
                has_data = False
        item.setData(bool(has_data), Qt.UserRole + 3)

    def _apply_taxon_choice_to_inputs(self, choice: TaxonChoice, *, vernacular_text: str | None = None) -> None:
        genus = self._clean_genus_text(choice.genus)
        species = self._clean_species_text(choice.species)
        if vernacular_text is not None:
            self.vernacular_input.blockSignals(True)
            self.vernacular_input.setText(str(vernacular_text or "").strip())
            self.vernacular_input.blockSignals(False)
        if genus:
            self.genus_input.setText(genus)
        if species:
            self.species_input.setText(species)

    def _init_completers(self) -> None:
        popup_styler = getattr(self.parent(), "_style_dropdown_popup_readability", None)
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            self._vernacular_db = VernacularDB(db_path, language_code=lang)
        else:
            self._vernacular_db = None
        self._taxon_lookup = TaxonLookupService(
            vernacular_db=self._vernacular_db,
            language_code=lang,
            include_reference_data=True,
        )
        self._taxon_controller = TaxonInputController(
            self._taxon_lookup,
            self.genus_input,
            self.species_input,
            self.vernacular_input,
            self,
            species_item_customizer=self._customize_species_item,
        )
        self._genus_model = self._taxon_controller.genus_model
        self._species_model = self._taxon_controller.species_model
        self._vernacular_model = self._taxon_controller.vernacular_model
        self._genus_completer = self._taxon_controller.genus_completer
        self._species_completer = self._taxon_controller.species_completer
        self._vernacular_completer = self._taxon_controller.vernacular_completer

        genus_popup = self._genus_completer.popup() if self._genus_completer else None
        if callable(popup_styler) and genus_popup is not None:
            popup_styler(genus_popup, self.genus_input)
        species_popup = self._species_completer.popup() if self._species_completer else None
        if callable(popup_styler) and species_popup is not None:
            popup_styler(species_popup, self.species_input)
        if species_popup is not None:
            species_popup.setItemDelegate(
                SpeciesItemDelegate(
                    self._species_availability,
                    species_popup,
                    genus_provider=lambda: self._clean_genus_text(self.genus_input.text()),
                )
            )
        vernacular_popup = self._vernacular_completer.popup() if self._vernacular_completer else None
        if callable(popup_styler) and vernacular_popup is not None:
            popup_styler(vernacular_popup, self.vernacular_input)

    def eventFilter(self, obj, event):
        controller = getattr(self, "_taxon_controller", None)
        if controller is not None and controller.eventFilter(obj, event):
            return True
        return super().eventFilter(obj, event)

    def _update_genus_suggestions(self, text: str, hide_on_exact: bool = False) -> list[str]:
        lookup = self._taxon_lookup
        values = lookup.suggest_genera(text or "", limit=TAXON_COMPLETER_LIMIT) if lookup else []
        values = sorted({value for value in values if value})
        if hide_on_exact and text.strip():
            text_lower = text.strip().lower()
            if any(value.lower() == text_lower for value in values):
                self._genus_model.setStringList([])
                self._genus_completer.popup().hide()
                return values
        self._genus_model.setStringList(values)
        return values

    def _update_species_suggestions(self, genus: str, text: str, hide_on_exact: bool = False) -> list[str]:
        genus = self._clean_genus_text(genus)
        prefix = self._clean_species_text(text)
        lookup = self._taxon_lookup
        choices = lookup.suggest_species(genus, prefix, limit=TAXON_COMPLETER_LIMIT) if lookup else []
        values = [self._clean_species_text(choice.species) for choice in choices if self._clean_species_text(choice.species)]
        if hide_on_exact and prefix:
            prefix_lower = prefix.lower()
            if any(value.lower() == prefix_lower for value in values):
                self._species_model.clear()
                self._species_completer.popup().hide()
                return values
        self._species_model.clear()
        for choice in choices:
            species = self._clean_species_text(choice.species)
            if not species:
                continue
            item = QStandardItem(self._format_species_choice_display(choice))
            item.setData(species, Qt.UserRole)
            item.setData(self._clean_genus_text(choice.genus) or genus, Qt.UserRole + 1)
            item.setData(species, Qt.UserRole + 2)
            item.setData(choice, self._ROLE_TAXON_CHOICE)
            if self._species_availability:
                try:
                    info = self._species_availability.get_detailed_info(genus, species)
                    has_data = bool(
                        info.get("has_personal_points")
                        or info.get("has_shared_points")
                        or info.get("has_published_points")
                        or info.get("has_reference_minmax")
                    )
                except Exception:
                    has_data = False
                item.setData(has_data, Qt.UserRole + 3)
            self._species_model.appendRow(item)
        return values

    def _populate_vernacular_model(self, suggestions: list[TaxonChoice]) -> None:
        self._vernacular_model.clear()
        for choice in suggestions or []:
            name = self._format_common_name_choice_display(choice)
            item = QStandardItem(name)
            item.setData(name, Qt.UserRole)
            item.setData(self._clean_genus_text(choice.genus), Qt.UserRole + 1)
            item.setData(self._clean_species_text(choice.species), Qt.UserRole + 2)
            item.setData(choice, self._ROLE_TAXON_CHOICE)
            self._vernacular_model.appendRow(item)

    def _update_vernacular_suggestions_for_taxon(self) -> None:
        lookup = self._taxon_lookup
        if not lookup:
            self._vernacular_model.clear()
            self._set_vernacular_placeholder_from_suggestions([])
            return
        genus = self._clean_genus_text(self.genus_input.text()) or None
        species = self._clean_species_text(self.species_input.text()) or None
        suggestions = lookup.suggest_common_names(prefix="", genus=genus, species=species, limit=TAXON_COMPLETER_LIMIT)
        self._populate_vernacular_model(suggestions)
        self._set_vernacular_placeholder_from_suggestions([choice.common_name for choice in suggestions if choice.common_name])

    def _maybe_set_vernacular_from_taxon(self) -> None:
        lookup = self._taxon_lookup
        if not lookup or self.vernacular_input.text().strip():
            return
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if not genus or not species:
            return
        choice = lookup.best_common_name_for_taxon(genus, species)
        if choice and choice.common_name:
            self.vernacular_input.setText(choice.common_name)
        else:
            suggestions = lookup.suggest_common_names(prefix="", genus=genus, species=species, limit=TAXON_COMPLETER_LIMIT)
            self._set_vernacular_placeholder_from_suggestions([item.common_name for item in suggestions if item.common_name])

    def _on_genus_text_changed(self, text: str) -> None:
        self._update_genus_suggestions(text, hide_on_exact=True)
        genus = self._clean_genus_text(text)
        if self.genus_input.hasFocus():
            if self.vernacular_input.text().strip():
                self.vernacular_input.blockSignals(True)
                self.vernacular_input.clear()
                self.vernacular_input.blockSignals(False)
            if self.species_input.text().strip():
                self.species_input.blockSignals(True)
                self.species_input.clear()
                self.species_input.blockSignals(False)
                self._species_model.clear()
        if genus and not self.species_input.text().strip():
            suggestions = self._update_species_suggestions(genus, "")
            self._set_species_placeholder_from_suggestions(suggestions)
        elif not genus:
            self._species_model.clear()
            self._set_species_placeholder_from_suggestions([])
        self._update_vernacular_suggestions_for_taxon()

    def _on_species_text_changed(self, text: str) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        if not genus:
            self._species_model.clear()
            self._set_species_placeholder_from_suggestions([])
            return
        suggestions = self._update_species_suggestions(genus, text, hide_on_exact=True)
        if not self._clean_species_text(text):
            self._set_species_placeholder_from_suggestions(suggestions)

    def _on_vernacular_text_changed(self, text: str) -> None:
        lookup = self._taxon_lookup
        if not lookup:
            return
        genus = self._clean_genus_text(self.genus_input.text()) or None
        species = self._clean_species_text(self.species_input.text()) or None
        if not text.strip():
            self._update_vernacular_suggestions_for_taxon()
            return
        suggestions = lookup.suggest_common_names(prefix=text, genus=genus, species=species, limit=TAXON_COMPLETER_LIMIT)
        self._populate_vernacular_model(suggestions)

    def _on_taxon_editing_finished(self) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if genus != self.genus_input.text().strip():
            self.genus_input.blockSignals(True)
            self.genus_input.setText(genus)
            self.genus_input.blockSignals(False)
        if species != self.species_input.text().strip():
            self.species_input.blockSignals(True)
            self.species_input.setText(species)
            self.species_input.blockSignals(False)
        self._maybe_set_vernacular_from_taxon()

    def _on_vernacular_selected(self, index: QModelIndex) -> None:
        lookup = self._taxon_lookup
        if not lookup or not index.isValid():
            return
        choice = self._choice_from_index(index)
        if choice is None:
            name = (index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
            resolved = lookup.resolve_common_name(name) if name else []
            choice = resolved[0] if resolved else None
        if not choice:
            return
        name = str(choice.common_name or index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
        self._apply_taxon_choice_to_inputs(choice, vernacular_text=name)

    def _on_vernacular_editing_finished(self) -> None:
        lookup = self._taxon_lookup
        if not lookup:
            return
        name = self.vernacular_input.text().strip()
        if not name:
            return
        matches = lookup.resolve_common_name(name)
        if matches:
            taxon = matches[0]
            genus = self._clean_genus_text(taxon.genus)
            species = self._clean_species_text(taxon.species)
            if not species:
                self.vernacular_input.clear()
                return
            if genus:
                self.genus_input.setText(genus)
            if species:
                self.species_input.setText(species)

    def _on_genus_selected(self, genus: str) -> None:
        cleaned = self._clean_genus_text(genus)
        if cleaned:
            self.genus_input.setText(cleaned)

    def _on_species_selected(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        choice = self._choice_from_index(index)
        species = self._clean_species_text(choice.species) if choice and choice.species else ""
        if not choice:
            species = self._clean_species_text(index.data(Qt.UserRole) or index.data(Qt.DisplayRole))
        if species:
            self.species_input.setText(species)
        if choice and choice.common_name and not self.vernacular_input.text().strip():
            self.vernacular_input.setText(choice.common_name)
        if choice and choice.species:
            self._maybe_set_vernacular_from_taxon()

    def _set_busy(self, busy: bool, status_text: str | None = None) -> None:
        self.search_button.setEnabled(not busy)
        self.genus_input.setEnabled(not busy)
        self.species_input.setEnabled(not busy)
        self.vernacular_input.setEnabled(not busy)
        if status_text is not None:
            self.search_status_label.setText(status_text)

    def _result_source_label(self, row: dict[str, Any]) -> str:
        return community_result_source_label(row, self.tr)

    def _result_q_range_label(self, row: dict[str, Any]) -> str:
        return community_result_q_range_label(row)

    def _populate_results_table(self) -> None:
        self.results_table.setRowCount(0)
        for row_data in self._results:
            row = self.results_table.rowCount()
            self.results_table.insertRow(row)
            values = [
                self._result_source_label(row_data),
                str(int(row_data.get("measurement_count") or 0) or "—"),
                self._result_q_range_label(row_data),
                str(row_data.get("contributor_label") or "—"),
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.results_table.setItem(row, col, item)
        self.results_table.resizeRowsToContents()

    def _reset_preview(self) -> None:
        self._preview_pane.clear()
        self.footer_hint.setText("")
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)
        self._selected_result = None
        self._selected_detail = None

    def _on_search_clicked(self) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if not genus:
            QMessageBox.warning(
                self,
                self.tr("Missing Genus"),
                self.tr("Enter at least a genus to search community spore data."),
            )
            return
        if self._search_worker is not None:
            return
        self._results = []
        self._community_summary = {}
        self.results_table.clearSelection()
        self.results_table.setRowCount(0)
        self._reset_preview()
        self._set_busy(True, self.tr("Searching community spore data..."))
        worker = _CloudSearchWorker(genus, species)
        worker.search_done.connect(self._on_search_finished)
        worker.error.connect(self._on_search_error)
        self._search_worker = worker
        self._track_worker(worker)
        worker.start()

    def _on_search_finished(self, results: list, summary: dict) -> None:
        self._search_worker = None
        self._set_busy(False)
        self._results = [dict(row or {}) for row in (results or [])]
        self._community_summary = dict(summary or {})
        self._populate_results_table()
        if self._results:
            text = self.tr("Found {count} community source(s). Select one to review before importing.").format(
                count=len(self._results)
            )
            dataset_count = int(self._community_summary.get("dataset_count") or 0)
            measurement_count = int(self._community_summary.get("measurement_count") or 0)
            if dataset_count or measurement_count:
                text += " " + self.tr("Community aggregate: {datasets} dataset(s), n={count}.").format(
                    datasets=dataset_count,
                    count=measurement_count,
                )
            self.search_status_label.setText(text)
            self.footer_hint.setText(
                self.tr("Use Import summary to save a local reference, or Use raw points for a temporary comparison plot.")
            )
        else:
            genus_q = self._clean_genus_text(self.genus_input.text())
            species_q = self._clean_species_text(self.species_input.text())
            taxon_q = f"{genus_q} {species_q}".strip() if species_q else genus_q
            self.search_status_label.setText(
                self.tr("No community spore results found for {taxon}.").format(taxon=taxon_q)
            )

    def _on_search_error(self, message: str) -> None:
        self._search_worker = None
        self._set_busy(False)
        self.search_status_label.setText(str(message or "").strip() or self.tr("Community search failed."))

    def _on_result_selection_changed(self) -> None:
        if self._detail_worker is not None:
            return
        row = self.results_table.currentRow()
        if row < 0 or row >= len(self._results):
            self._reset_preview()
            return
        self._selected_result = dict(self._results[row])
        self._selected_detail = None
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)
        self.summary_title_label.setText(self.tr("Loading review details..."))
        self.summary_meta_label.setText(self._result_source_label(self._selected_result))
        self.summary_note_label.setText(self.tr("Loading dataset details..."))
        worker = _CloudDetailWorker(self._selected_result)
        worker.detail_done.connect(self._on_detail_finished)
        worker.error.connect(self._on_detail_error)
        self._detail_worker = worker
        self._track_worker(worker)
        worker.start()

    def _on_detail_finished(self, detail: dict) -> None:
        self._detail_worker = None
        self._selected_detail = dict(detail or {})
        self._populate_detail_preview()

    def _on_detail_error(self, message: str) -> None:
        self._detail_worker = None
        self._selected_detail = None
        self.summary_title_label.setText(self.tr("Could not load dataset"))
        self.summary_meta_label.setText(str(message or "").strip())
        self.summary_note_label.setText(self.tr("This result could not be reviewed."))
        self.raw_spores_text.setPlainText(str(message or "").strip())
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)

    def _format_stat(self, value: Any, decimals: int = 2) -> str:
        return format_stat_value(value, decimals)

    def _populate_detail_preview(self) -> None:
        detail = self._selected_detail or {}
        fields = community_detail_preview_fields(detail, self.tr)
        self._preview_pane.set_summary(fields["title"], fields["meta"], fields["rows"], fields["note"])
        self._preview_pane.set_raw_spores(fields["raw_text"])
        self._preview_pane.set_method(fields["method_mapping"])
        self._preview_pane.set_calibration(fields["calibration_text"])
        self._preview_pane.set_provenance(fields["provenance_text"])
        self._preview_pane.set_provenance_summary(fields["provenance_summary"])

        self.import_summary_button.setEnabled(True)
        self.plot_points_button.setEnabled(bool(fields["points_count"]))
        self.footer_hint.setText(
            self.tr("Review complete. Import summary saves a local reference; Use raw points adds a temporary comparison plot.")
        )

    def _default_import_source(self) -> str:
        return community_default_import_source(self._selected_detail or {}, self.tr)

    def _single_detail_value(self, *keys: str) -> str | None:
        return community_single_detail_value(self._selected_detail or {}, *keys)

    def _summary_metadata_payload(self) -> dict[str, Any]:
        return community_summary_metadata_payload(self._selected_detail or {}, self.tr)

    def _summary_reference_payload(self) -> dict[str, Any] | None:
        return community_summary_reference_payload(self._selected_detail or {}, self.tr)

    def _points_payload(self) -> dict[str, Any] | None:
        return community_points_payload(self._selected_detail or {}, self.tr)

    def _on_import_summary_clicked(self) -> None:
        payload = self._summary_reference_payload()
        if not payload:
            QMessageBox.warning(self, self.tr("Missing Data"), self.tr("No summary data is loaded for import."))
            return
        self._accepted_action = "import_summary"
        self._accepted_data = payload
        self.accept()

    def _on_plot_points_clicked(self) -> None:
        payload = self._points_payload()
        if not payload:
            QMessageBox.warning(self, self.tr("Missing Data"), self.tr("No raw spore points are available for plotting."))
            return
        self._accepted_action = "plot_points"
        self._accepted_data = payload
        self.accept()


class CommunityResultsPane(QWidget):
    """Browse/select flow for the Community tab of ``AddReferenceDialog``.

    Reuses ``CloudReferenceDialog``'s search/detail workers and payload
    builders (``_CloudSearchWorker``, ``_CloudDetailWorker``,
    ``community_summary_reference_payload``, ``community_points_payload``,
    ``community_detail_preview_fields``) rather than duplicating them, and
    populates the host's shared ``ReferencePreviewPane`` instance instead of
    owning its own. Unlike ``CloudReferenceDialog``, there is no full taxon
    entry form: results load automatically for the host's working taxon
    (``set_taxon``), mirroring the My-observations tab. A single search field
    overrides that taxon, debounced per keystroke, and accepts a genus alone
    as well as a genus and species; clearing it returns to the host's taxon.

    The dialog's two footer buttons ("Import summary as reference" / "Use
    raw points for plot") become the ``range_summary_radio`` /
    ``raw_points_radio`` pair; the host reads whichever payload the checked
    radio implies via :meth:`current_mode_payload` when its own "Add to
    plot" button is clicked.
    """

    selection_changed = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        genus: str = "",
        species: str = "",
        preview_pane: ReferencePreviewPane,
        results: list[dict[str, Any]] | None = None,
        exclude_observation_cloud_id: str | None = None,
    ) -> None:
        super().__init__(parent)
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()
        self._preview_pane = preview_pane
        # The active observation's own cloud identity (``observations.cloud_id``
        # locally), so a dataset built from the exact observation already
        # being plotted never reappears as a comparison candidate against
        # itself. Community rows carry a *cloud* ``observation_id`` (see
        # ``search_community_spore_datasets``), never the local sqlite id
        # ``exclude_observation_id`` uses for the My-observations tab, so
        # this is intentionally a separate identity. Only ``_kind ==
        # "observation"`` rows have an observation identity at all --
        # published-reference rows are never self-references and are never
        # filtered here.
        self._exclude_observation_cloud_id = (
            str(exclude_observation_cloud_id).strip() or None
            if exclude_observation_cloud_id is not None
            else None
        )
        # Optional injected result list, mirroring AddReferenceDialog's other
        # tabs' testability convention: when provided, each row is treated
        # as already carrying full detail fields, so selection needs no
        # network detail fetch. Lets renderer scenarios and tests exercise
        # this pane deterministically, with no live network.
        self._injected_results = results
        self._results: list[dict[str, Any]] = []
        self._selected_result: dict[str, Any] | None = None
        self._selected_detail: dict[str, Any] | None = None
        self._search_worker: _CloudSearchWorker | None = None
        self._detail_worker: _CloudDetailWorker | None = None
        self._worker_refs: list[QThread] = []
        # Bumped on every target change (refresh) and every detail request, so a
        # superseded worker's completion/error can be told apart from the
        # latest one and ignored instead of overwriting current state.
        self._search_generation = 0
        self._detail_generation = 0
        # The picker's own taxon, kept apart from the effective query above:
        # typing in the search field overrides it, and clearing the field
        # must fall back to it rather than to whatever was typed last.
        self._host_genus = self._genus
        self._host_species = self._species
        # Per-keystroke search is debounced because one refresh() costs a
        # QThread plus three network RPCs (search_community_spore_datasets,
        # search_public_reference_values, community_spore_taxon_summary).
        # Searching on every keystroke unthrottled would fire those for every
        # prefix of the word being typed.
        self._search_debounce = QTimer(self)
        self._search_debounce.setSingleShot(True)
        self._search_debounce.setInterval(_COMMUNITY_SEARCH_DEBOUNCE_MS)
        self._search_debounce.timeout.connect(self._apply_search_text)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        search_row = QHBoxLayout()
        search_row.setSpacing(8)
        from PySide6.QtWidgets import QLineEdit
        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText(
            QCoreApplication.translate("CommunityResultsPane", "Search genus, or genus and species…")
        )
        self.search_input.setToolTip(
            QCoreApplication.translate("CommunityResultsPane",
                "Type a genus (e.g., 'Hebeloma') to search every community dataset for it, or a "
                "genus and species (e.g., 'Hebeloma mesophaeum') to narrow it. Results refresh as "
                "you type. Clear the field to browse the selected reference taxon again.\n\n"
                "The genus must be spelled out in full: community search matches it exactly, not "
                "as a prefix."
            )
        )
        self.search_input.textChanged.connect(self._on_search_text_changed)
        self.search_input.returnPressed.connect(self._on_search_input_submitted)
        search_row.addWidget(self.search_input)
        layout.addLayout(search_row)

        # Same QListWidget + TwoLineRow convention as the Library and
        # My-observations tabs (title line + independently-elided detail
        # line), rather than the legacy dialog's 4-column QTableWidget, so
        # all source tabs in the picker render consistently.
        self.results_list = QListWidget(self)
        self.results_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.results_list.itemSelectionChanged.connect(self._on_result_selection_changed)
        layout.addWidget(self.results_list, 1)

        self.status_label = QLabel("", self)
        self.status_label.setWordWrap(True)
        self.status_label.setStyleSheet("color: #7f8c8d;")
        layout.addWidget(self.status_label)

        mode_row = QHBoxLayout()
        mode_row.setSpacing(12)
        self.range_summary_radio = QRadioButton(QCoreApplication.translate("CommunityResultsPane", "Range summary"), self)
        self.raw_points_radio = QRadioButton(QCoreApplication.translate("CommunityResultsPane", "Raw points (n=0)"), self)
        self.raw_points_radio.setEnabled(False)
        self.range_summary_radio.setChecked(True)
        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self.range_summary_radio)
        self._mode_group.addButton(self.raw_points_radio)
        mode_row.addWidget(self.range_summary_radio)
        mode_row.addWidget(self.raw_points_radio)
        mode_row.addStretch(1)
        layout.addLayout(mode_row)

        self.refresh()

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def set_taxon(self, genus: str, species: str) -> None:
        """Switch the fixed taxon this tab searches for and reload results.

        Used by the picker's taxon selector (see stage-4b-fix Part 3): the
        Community tab has no per-candidate taxon field to filter by, so a
        target change re-searches for the new genus/species instead.
        """
        self._host_genus = str(genus or "").strip()
        self._host_species = str(species or "").strip()
        self._genus = self._host_genus
        self._species = self._host_species
        self.refresh()

    def _parse_search_text(self) -> tuple[str, str]:
        """Split the search field into (genus, species) for the RPCs.

        An empty field falls back to the picker's own taxon. A single word is
        a genus-only query, which both ``search_community_spore_datasets`` and
        ``search_public_reference_values`` support: each treats an empty
        species as "any species" rather than as no match.
        """
        text = self.search_input.text().strip()
        if not text:
            return self._host_genus, self._host_species
        parts = text.split(None, 1)
        genus = parts[0]
        species = parts[1].strip() if len(parts) > 1 else ""
        return genus, species

    def _on_search_text_changed(self, _text: str) -> None:
        """Restart the debounce window on every keystroke."""
        self._search_debounce.start()

    def _on_search_input_submitted(self) -> None:
        """Search immediately on Return, without waiting out the debounce."""
        self._search_debounce.stop()
        self._apply_search_text()

    def _apply_search_text(self) -> None:
        """Adopt the search field's taxon and reload, if it actually changed.

        Re-searching an unchanged target would discard the current selection
        and its loaded preview for nothing, so a keystroke that does not
        change the effective genus/species (trailing whitespace, retyping the
        same letter) is deliberately inert.
        """
        self._search_debounce.stop()
        genus, species = self._parse_search_text()
        if genus == self._genus and species == self._species:
            return
        self._genus = genus
        self._species = species
        self.refresh()

    def _exclude_self_reference(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Drop the community dataset built from the active observation itself.

        Identity, not display text: only a ``_kind == "observation"`` row
        whose ``observation_id`` matches the active observation's own cloud
        id is dropped. Other observations of the same taxon (a different
        cloud ``observation_id``) and published-reference rows (no
        observation identity to compare) are never affected.
        """
        if not self._exclude_observation_cloud_id:
            return rows
        return [
            row
            for row in rows
            if not (
                str(row.get("_kind") or "") == "observation"
                and str(row.get("observation_id") or "").strip()
                == self._exclude_observation_cloud_id
            )
        ]

    def refresh(self) -> None:
        """(Re)load results for the fixed taxon. Clears any current selection."""
        # Invalidate any outstanding search/detail request for the previous
        # target: their completion/error must not be able to populate results,
        # replace status/preview, or leave an attachable stale payload under
        # the new target. The old worker keeps running and is still cleaned
        # up via _worker_refs; only its effect on this pane's state is cut.
        self._search_generation += 1
        search_generation = self._search_generation
        self._detail_generation += 1
        self._search_worker = None
        self._detail_worker = None

        self._results = []
        self._selected_result = None
        self._selected_detail = None
        self.results_list.clearSelection()
        self.results_list.clear()
        self._preview_pane.clear()
        self.selection_changed.emit()

        if self._injected_results is not None:
            self._results = self._exclude_self_reference(
                [dict(row or {}) for row in self._injected_results]
            )
            self._populate_results_list()
            self._update_status_after_results()
            return
        if not self._genus:
            self.status_label.setText(
                QCoreApplication.translate("CommunityResultsPane", "No taxon selected — enter a genus to search.")
            )
            return
        if len(self._genus) < _COMMUNITY_MIN_GENUS_CHARS:
            self.status_label.setText(
                QCoreApplication.translate("CommunityResultsPane", "Keep typing — enter the full genus name to search.")
            )
            return
        self.status_label.setText(QCoreApplication.translate("CommunityResultsPane", "Searching community spore data..."))
        worker = _CloudSearchWorker(self._genus, self._species)
        worker.search_done.connect(
            lambda results, summary, gen=search_generation: self._on_search_finished(results, summary, gen)
        )
        worker.error.connect(lambda message, gen=search_generation: self._on_search_error(message, gen))
        self._search_worker = worker
        self._track_worker(worker)
        worker.start()

    def _track_worker(self, worker: QThread) -> None:
        self._worker_refs.append(worker)
        worker.finished.connect(lambda: self._release_worker(worker))

    def _release_worker(self, worker: QThread) -> None:
        try:
            self._worker_refs.remove(worker)
        except ValueError:
            pass
        worker.deleteLater()

    def closeEvent(self, event) -> None:  # noqa: N802 - Qt override
        # Stop the debounce before waiting on workers: a pending keystroke
        # must not start a fresh search thread after the pane has begun
        # shutting its outstanding ones down.
        self._search_debounce.stop()
        self._search_generation += 1
        self._detail_generation += 1
        self._selected_result = None
        self._selected_detail = None
        # Wait for every tracked worker, not only the current search/detail
        # one: a superseded worker from an earlier target/selection can still
        # be outstanding and must not survive the pane's close.
        for worker in list(self._worker_refs):
            worker.wait()
        self._search_worker = None
        self._detail_worker = None
        super().closeEvent(event)

    def _on_search_finished(self, results: list, summary: dict, generation: int) -> None:
        if generation != self._search_generation:
            return
        self._search_worker = None
        self._results = self._exclude_self_reference(
            [dict(row or {}) for row in (results or [])]
        )
        self._populate_results_list()
        self._update_status_after_results()

    def _on_search_error(self, message: str, generation: int) -> None:
        if generation != self._search_generation:
            return
        self._search_worker = None
        self.status_label.setText(str(message or "").strip() or QCoreApplication.translate("CommunityResultsPane", "Community search failed."))

    def _update_status_after_results(self) -> None:
        if self._results:
            self.status_label.setText(
                QCoreApplication.translate("CommunityResultsPane", "Found {count} community source(s). Select one to review before adding.").format(
                    count=len(self._results)
                )
            )
        else:
            self.status_label.setText(QCoreApplication.translate("CommunityResultsPane", "No community spore results found for this taxon."))

    def _row_detail_text(self, row_data: dict[str, Any]) -> str:
        parts = []
        n = int(row_data.get("measurement_count") or 0)
        if n:
            parts.append(QCoreApplication.translate("CommunityResultsPane", "n = {count}").format(count=n))
        q_label = community_result_q_range_label(row_data)
        if q_label != "—":
            parts.append(QCoreApplication.translate("CommunityResultsPane", "Q {range}").format(range=q_label))
        contributor = str(row_data.get("contributor_label") or "").strip()
        if contributor:
            parts.append(contributor)
        return " · ".join(parts)

    def _populate_results_list(self) -> None:
        self.results_list.clear()
        for index, row_data in enumerate(self._results):
            label = community_result_source_label(row_data, self.tr)
            detail = self._row_detail_text(row_data)
            item = QListWidgetItem()
            item.setToolTip(label if not detail else f"{label}\n{detail}")
            item.setData(Qt.UserRole, index)
            self.results_list.addItem(item)
            row_widget = TwoLineRow(label, detail, self.results_list)
            item.setSizeHint(row_widget.sizeHint())
            self.results_list.setItemWidget(item, row_widget)

    # ------------------------------------------------------------------
    # Selection / detail
    # ------------------------------------------------------------------

    def _on_result_selection_changed(self) -> None:
        # A new selection always supersedes any in-flight detail request for
        # the previous one; bumping the generation here (rather than blocking
        # on self._detail_worker) lets rapid selection changes each start
        # their own request instead of being ignored while an older one loads.
        self._detail_generation += 1
        detail_generation = self._detail_generation
        self._detail_worker = None
        items = self.results_list.selectedItems()
        if not items:
            self._selected_result = None
            self._selected_detail = None
            self._preview_pane.clear()
            self.selection_changed.emit()
            return
        row = items[0].data(Qt.UserRole)
        if not isinstance(row, int) or row < 0 or row >= len(self._results):
            self._selected_result = None
            self._selected_detail = None
            self._preview_pane.clear()
            self.selection_changed.emit()
            return
        self._selected_result = dict(self._results[row])
        self._selected_detail = None
        self._preview_pane.set_summary(
            QCoreApplication.translate("CommunityResultsPane", "Loading review details..."),
            community_result_source_label(self._selected_result, self.tr),
            [],
            QCoreApplication.translate("CommunityResultsPane", "Loading dataset details..."),
        )
        self.selection_changed.emit()

        if self._injected_results is not None:
            # Deterministic fixtures already carry full detail fields.
            self._on_detail_finished(self._selected_result, detail_generation)
            return
        worker = _CloudDetailWorker(self._selected_result)
        worker.detail_done.connect(lambda detail, gen=detail_generation: self._on_detail_finished(detail, gen))
        worker.error.connect(lambda message, gen=detail_generation: self._on_detail_error(message, gen))
        self._detail_worker = worker
        self._track_worker(worker)
        worker.start()

    def _on_detail_finished(self, detail: dict, generation: int) -> None:
        if generation != self._detail_generation:
            return
        self._detail_worker = None
        self._selected_detail = dict(detail or {})
        self._apply_detail_to_preview()
        self._update_mode_radio_state()
        self.selection_changed.emit()

    def _on_detail_error(self, message: str, generation: int) -> None:
        if generation != self._detail_generation:
            return
        self._detail_worker = None
        self._selected_detail = None
        self._preview_pane.set_summary(
            QCoreApplication.translate("CommunityResultsPane", "Could not load dataset"),
            str(message or "").strip(),
            [],
            QCoreApplication.translate("CommunityResultsPane", "This result could not be reviewed."),
        )
        self._preview_pane.set_raw_spores(str(message or "").strip())
        self.selection_changed.emit()

    def _apply_detail_to_preview(self) -> None:
        if not self._selected_detail:
            self._preview_pane.clear()
            return
        fields = community_detail_preview_fields(self._selected_detail, self.tr)
        self._preview_pane.set_summary(fields["title"], fields["meta"], fields["rows"], fields["note"])
        self._preview_pane.set_raw_spores(fields["raw_text"])
        self._preview_pane.set_method(fields["method_mapping"])
        self._preview_pane.set_calibration(fields["calibration_text"])
        self._preview_pane.set_provenance(fields["provenance_text"])
        self._preview_pane.set_provenance_summary(fields["provenance_summary"])

    def _update_mode_radio_state(self) -> None:
        n = 0
        if self._selected_detail:
            measurements = self._selected_detail.get("measurements_json") or []
            if isinstance(measurements, list):
                n = len(measurements)
        self.raw_points_radio.setText(QCoreApplication.translate("CommunityResultsPane", "Raw points (n={count})").format(count=n))
        self.raw_points_radio.setEnabled(n > 0)
        if not self.raw_points_radio.isEnabled() and self.raw_points_radio.isChecked():
            self.range_summary_radio.setChecked(True)

    # ------------------------------------------------------------------
    # Host-facing API
    # ------------------------------------------------------------------

    def sync_preview(self) -> None:
        """Re-apply the current selection to the (shared) preview pane.

        Called by the host when switching back to this tab, since the
        preview pane is shared across tabs and may show another tab's
        content in between.
        """
        self._apply_detail_to_preview()

    def has_selection(self) -> bool:
        return self._selected_detail is not None

    def current_mode_payload(self) -> dict[str, Any] | None:
        """The reference_series-ready payload for the checked radio, or None."""
        if not self._selected_detail:
            return None
        if self.raw_points_radio.isChecked() and self.raw_points_radio.isEnabled():
            return community_points_payload(self._selected_detail, self.tr)
        return community_summary_reference_payload(self._selected_detail, self.tr)


__all__ = [
    "CloudReferenceDialog",
    "CommunityResultsPane",
    "community_default_import_source",
    "community_detail_preview_fields",
    "community_points_payload",
    "community_result_q_range_label",
    "community_result_source_label",
    "community_single_detail_value",
    "community_summary_metadata_payload",
    "community_summary_reference_payload",
    "format_stat_value",
]
