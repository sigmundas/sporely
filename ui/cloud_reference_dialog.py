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

from utils.cloud_sync import CloudSyncError, SporelyCloudClient

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


class CommunityResultsPane(QWidget):
    """The reference drawer's Community list: browse, select and tick results.

    Uses this module's search/detail workers and payload builders
    (``_CloudSearchWorker``, ``_CloudDetailWorker``,
    ``community_summary_reference_payload``, ``community_points_payload``).
    The drawer's plot is the preview, so this pane draws none itself (stage
    10a4). There is no taxon entry form: results load automatically for the
    host's working taxon
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
        results: list[dict[str, Any]] | None = None,
        exclude_observation_cloud_id: str | None = None,
        checkable: bool = False,
    ) -> None:
        super().__init__(parent)
        self._checkable = bool(checkable)
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()
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
            self.selection_changed.emit()
            return
        row = items[0].data(Qt.UserRole)
        if not isinstance(row, int) or row < 0 or row >= len(self._results):
            self._selected_result = None
            self._selected_detail = None
            self._selected_error = None
            self.selection_changed.emit()
            return
        self._selected_result = dict(self._results[row])
        self._selected_detail = None
        self._selected_error = None
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
        # Radio state before telling the host: the payload it reads follows
        # whichever mode is checked, and raw points are only offered when the
        # detail carries measurements.
        self._update_mode_radio_state()
        self.selection_changed.emit()

    def _on_detail_error(self, message: str, generation: int) -> None:
        if generation != self._detail_generation:
            return
        self._detail_worker = None
        self._selected_detail = None
        self._selected_error = str(message or "").strip()
        self.selection_changed.emit()

    # ------------------------------------------------------------------
    # Rendering into the shared preview
    # ------------------------------------------------------------------

    def _selected_measurements(self) -> list[Any]:
        """The individual measurements behind the current detail, if any."""
        if not self._selected_detail:
            return []
        measurements = self._selected_detail.get("measurements_json") or []
        return list(measurements) if isinstance(measurements, list) else []

    def _use_raw_points(self) -> bool:
        """Whether the checked mode is the raw-points one."""
        return self.raw_points_radio.isChecked() and self.raw_points_radio.isEnabled()

    def _update_mode_radio_state(self) -> None:
        n = len(self._selected_measurements())
        self.raw_points_radio.setText(QCoreApplication.translate("CommunityResultsPane", "Raw points (n={count})").format(count=n))
        self.raw_points_radio.setEnabled(n > 0)
        if not self.raw_points_radio.isEnabled() and self.raw_points_radio.isChecked():
            self.range_summary_radio.setChecked(True)

    # ------------------------------------------------------------------
    # Host-facing API
    # ------------------------------------------------------------------

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
    "community_points_payload",
    "community_result_q_range_label",
    "community_result_source_label",
    "community_single_detail_value",
    "community_summary_metadata_payload",
    "community_summary_reference_payload",
    "format_stat_value",
]
