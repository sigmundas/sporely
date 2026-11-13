"""Dialog for reviewing and importing community spore data."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import QCoreApplication, QEvent, QStringListModel, QThread, Qt, QTimer, Signal
from PySide6.QtGui import QStandardItemModel
from PySide6.QtWidgets import (
    QButtonGroup,
    QCompleter,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)

from references.reference_comparison import ComparisonView, comparison_view
from references.reference_display import (
    display_from_community_summary,
    display_from_points,
)
from utils.cloud_sync import CloudSyncError, SporelyCloudClient

from .reference_preview_pane import ReferencePreviewPane
from .two_line_row import CheckableTwoLineRow, TwoLineRow

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
    # Checkable mode (the Analysis reference drawer, stage 7c): a row's
    # checkbox was ticked or unticked by the user: (result index, checked).
    result_check_toggled = Signal(int, bool)
    # The result list was rebuilt (a search finished or the taxon changed).
    results_changed = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        genus: str = "",
        species: str = "",
        preview_pane: ReferencePreviewPane,
        results: list[dict[str, Any]] | None = None,
        exclude_observation_cloud_id: str | None = None,
        comparison_view_factory: Callable[..., ComparisonView] | None = None,
        preview_is_active: Callable[[], bool] | None = None,
        checkable: bool = False,
    ) -> None:
        super().__init__(parent)
        self._checkable = bool(checkable)
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()
        self._preview_pane = preview_pane
        # How this pane turns a projected community source into the shared
        # preview's comparison model. Supplied by the host, which owns the
        # session's frozen axes and the current observation; without one the
        # pane still renders, on axes derived from the selected source alone,
        # which is the right fallback for a host that has no observation to
        # compare against rather than a reason to fall back to a table.
        self._comparison_view_factory = comparison_view_factory or (
            lambda display=None, source_points=None: comparison_view(
                domains={}, display=display, source_points=source_points
            )
        )
        # Whether this pane is the one the shared preview currently belongs to.
        # Search and detail both complete asynchronously and can land long
        # after the user has moved to the Library or My-observations tab; a
        # write that ignored ownership would replace the source that tab just
        # put on screen with one from a tab the user is no longer looking at.
        # Request generations cannot solve this -- the response is not stale,
        # it is simply no longer the pane being displayed -- so state is kept
        # either way and only the painting is gated. The default suits a host
        # that gives this pane a preview to itself.
        self._preview_is_active = preview_is_active or (lambda: True)
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
        # The message from a failed detail fetch, kept so returning to this
        # tab restores what the user was told rather than silently showing an
        # empty pane for a selection that is still highlighted in the list.
        self._selected_error: str | None = None
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
        self._mode_group.buttonToggled.connect(self._on_mode_changed)
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
        self._selected_error = None
        self.results_list.clearSelection()
        self.results_list.clear()
        self._render_preview()
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
        self._selected_error = None
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
        if self._results and self._checkable:
            self.status_label.setText(
                QCoreApplication.translate("CommunityResultsPane", "Found {count} community source(s). Tick one to add it to the plot.").format(
                    count=len(self._results)
                )
            )
        elif self._results:
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
            if self._checkable:
                row_widget = CheckableTwoLineRow(label, detail, self.results_list)
                row_widget.toggled.connect(
                    lambda checked, index=index: self.result_check_toggled.emit(index, bool(checked))
                )
            else:
                row_widget = TwoLineRow(label, detail, self.results_list)
            item.setSizeHint(row_widget.sizeHint())
            self.results_list.setItemWidget(item, row_widget)
        self.results_changed.emit()

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
            self._selected_error = None
            self._render_preview()
            self.selection_changed.emit()
            return
        row = items[0].data(Qt.UserRole)
        if not isinstance(row, int) or row < 0 or row >= len(self._results):
            self._selected_result = None
            self._selected_detail = None
            self._selected_error = None
            self._render_preview()
            self.selection_changed.emit()
            return
        self._selected_result = dict(self._results[row])
        self._selected_detail = None
        self._selected_error = None
        self._render_preview()
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
        self._selected_error = None
        # Radio state first: the preview renders whichever mode is checked, so
        # settling the mode before drawing avoids showing the raw-points
        # comparison for one paint and then the summary one because the
        # raw-points radio turned out to be unavailable. It is pane-local
        # widget state and is updated whether or not this pane currently owns
        # the shared preview, so the footer's payload stays correct.
        self._update_mode_radio_state()
        self._render_preview()
        self.selection_changed.emit()

    def _on_detail_error(self, message: str, generation: int) -> None:
        if generation != self._detail_generation:
            return
        self._detail_worker = None
        self._selected_detail = None
        self._selected_error = str(message or "").strip()
        self._render_preview()
        self.selection_changed.emit()

    # ------------------------------------------------------------------
    # Rendering into the shared preview
    # ------------------------------------------------------------------

    def _render_preview(self) -> None:
        """Paint this pane's current state, if the preview is currently ours.

        Every transition goes through here rather than writing to the shared
        pane directly, so there is one place that decides both *what* this tab
        has to say and *whether* it is the tab entitled to say it. The state
        itself is always kept, which is what lets :meth:`sync_preview` restore
        a selection -- loaded, still loading or failed -- when the user comes
        back to this tab.
        """
        if not self._preview_is_active():
            return
        if self._selected_error is not None:
            self._show_error_preview(self._selected_error)
        elif self._selected_detail:
            self._show_detail_preview()
        elif self._selected_result is not None:
            self._show_loading_preview()
        else:
            self._preview_pane.clear()

    def _show_loading_preview(self) -> None:
        """A selected result whose detail has not arrived yet.

        ``clear()`` first, and not merely a new header: the pane is shared, so
        the method, calibration, provenance and raw-spore rows still on it
        belong to whatever was selected before. Leaving them under this
        source's name would attribute another dataset's mount medium and
        another author's spores to the row the user just clicked.

        No source band is drawn either. The list-level aggregate is not the
        detail, and a band painted from it would be replaced by a different
        one a moment later.
        """
        self._preview_pane.clear()
        self._preview_pane.set_header(
            QCoreApplication.translate("CommunityResultsPane", "Loading review details..."),
            community_result_source_label(self._selected_result or {}, self.tr),
            QCoreApplication.translate("CommunityResultsPane", "Loading dataset details..."),
        )

    def _show_error_preview(self, message: str) -> None:
        """A selected result whose detail could not be fetched.

        Same reasoning as the loading state, and the same ``clear()``: a
        failed fetch establishes nothing about this dataset, so nothing may
        remain on screen that a reader could take for its content.
        """
        self._preview_pane.clear()
        self._preview_pane.set_header(
            QCoreApplication.translate("CommunityResultsPane", "Could not load dataset"),
            message,
            QCoreApplication.translate("CommunityResultsPane", "This result could not be reviewed."),
        )
        self._preview_pane.set_raw_spores(message)

    def _selected_measurements(self) -> list[Any]:
        """The individual measurements behind the current detail, if any."""
        if not self._selected_detail:
            return []
        measurements = self._selected_detail.get("measurements_json") or []
        return list(measurements) if isinstance(measurements, list) else []

    def _use_raw_points(self) -> bool:
        """Whether the checked mode is the raw-points one."""
        return self.raw_points_radio.isChecked() and self.raw_points_radio.isEnabled()

    def _show_detail_preview(self) -> None:
        """The loaded detail, in whichever mode is currently checked."""
        fields = community_detail_preview_fields(self._selected_detail, self.tr)
        self._preview_pane.set_header(fields["title"], fields["meta"], fields["note"])

        measurements = self._selected_measurements()
        if self._use_raw_points():
            # The mode that attaches the contributors' individual
            # measurements, so the pane compares those: the projection states
            # no range, and the comparison derives measured extremes, a
            # measured 5-95 interval and a measured median from the points.
            display = display_from_points(measurements, source_kind="community")
            source_points: list[Any] | None = measurements
        else:
            # The aggregate. ``display_from_community_summary`` tags the
            # p05/p95 pair as a percentile interval only where the payload
            # actually carries both bounds; a result with bare min/max stays a
            # published range rather than acquiring 5-95 semantics it never
            # stated. No points are passed: the summary's own numbers are what
            # this mode will attach.
            display = display_from_community_summary(self._selected_detail)
            source_points = None
        self._preview_pane.set_comparison(
            self._comparison_view_factory(display=display, source_points=source_points)
        )

        if measurements:
            self._preview_pane.set_raw_spore_points(measurements)
        else:
            # Contract N22: a community aggregate's range is never expanded
            # into plausible-looking per-spore rows to fill this tab.
            self._preview_pane.set_raw_spores_unavailable(
                plotted_as_band=display.has_any_range
            )
        self._preview_pane.set_method(fields["method_mapping"])
        self._preview_pane.set_calibration(fields["calibration_text"])
        self._preview_pane.set_provenance(fields["provenance_text"])
        self._preview_pane.set_provenance_summary(fields["provenance_summary"])

    def _update_mode_radio_state(self) -> None:
        n = len(self._selected_measurements())
        self.raw_points_radio.setText(QCoreApplication.translate("CommunityResultsPane", "Raw points (n={count})").format(count=n))
        self.raw_points_radio.setEnabled(n > 0)
        if not self.raw_points_radio.isEnabled() and self.raw_points_radio.isChecked():
            self.range_summary_radio.setChecked(True)

    def _on_mode_changed(self, _button=None, checked: bool = True) -> None:
        """Redraw the preview when the user switches what will be added.

        The two radios choose between two genuinely different claims -- the
        contributors' aggregate and their individual measurements -- so the
        pane must show the one about to be attached rather than whichever was
        current when the detail loaded.

        ``QButtonGroup.buttonToggled`` fires once for the radio being cleared
        and once for the one being checked; only the second is acted on, so a
        single click repaints the comparison once.
        """
        if not checked:
            return
        if self._selected_detail:
            self._render_preview()

    # ------------------------------------------------------------------
    # Host-facing API
    # ------------------------------------------------------------------

    def sync_preview(self) -> None:
        """Re-apply the current selection to the (shared) preview pane.

        Called by the host when switching back to this tab, since the preview
        pane is shared across tabs and may show another tab's content in
        between. Restores whichever state this tab is in -- a loaded detail, a
        selection still waiting on its detail, or a failed one -- because a
        response that arrived while the user was elsewhere was recorded but
        deliberately not painted.
        """
        self._render_preview()

    def has_selection(self) -> bool:
        return self._selected_detail is not None

    # -- checkable mode (the reference drawer) --------------------------

    def result_count(self) -> int:
        return len(self._results)

    @staticmethod
    def identity_of(row_data: dict[str, Any]) -> str:
        """A stable identity for one result, the same across searches."""
        keys = (
            "_kind", "observation_id", "id", "reference_id", "reference_value_id", "genus", "species",
            "source", "contributor_label", "observed_on", "mount_medium", "mount_media", "stain", "stains",
        )
        return json.dumps({key: row_data.get(key) for key in keys}, sort_keys=True, default=str)

    def result_identity(self, index: int) -> str | None:
        if 0 <= index < len(self._results):
            return self.identity_of(self._results[index])
        return None

    def select_result(self, index: int) -> None:
        """Select a result, which loads its detail (``selection_changed`` when it arrives)."""
        if 0 <= index < self.results_list.count():
            self.results_list.setCurrentRow(index)

    def selected_index(self) -> int | None:
        items = self.results_list.selectedItems()
        value = items[0].data(Qt.UserRole) if items else None
        return value if isinstance(value, int) else None

    def detail_error(self) -> str | None:
        return self._selected_error

    def set_result_checked(self, index: int, checked: bool) -> None:
        if 0 <= index < self.results_list.count():
            widget = self.results_list.itemWidget(self.results_list.item(index))
            if isinstance(widget, CheckableTwoLineRow):
                widget.set_checked_silently(checked)

    def current_mode_payload(self) -> dict[str, Any] | None:
        """The reference_series-ready payload for the checked radio, or None."""
        if not self._selected_detail:
            return None
        if self._use_raw_points():
            return community_points_payload(self._selected_detail, self.tr)
        return community_summary_reference_payload(self._selected_detail, self.tr)


__all__ = [
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
