"""Reference Library manager dialog — desktop slice.

Standalone top-level dialog for viewing and editing the normalized
reference library (``reference_works`` → ``reference_taxon_treatments``
→ ``reference_measurement_sets``). It reuses the existing repositories
and the canonical snapshot service; it never touches the legacy
``reference_values`` table, ``ReferenceDB``, or any cloud/web/landing
code path.

Three-pane layout:

- Left: searchable table of publications.
- Middle: hierarchical tree of treatments + measurement sets for the
  selected work.
- Right: detail panel for the selected record + create/edit/attach
  actions.

Attachment is deliberately delegated to the parent (MainWindow) via the
``attach_requested(str, str)`` signal so the existing
``attach_with_status`` + plotability + warning-row + rollback contract
stays in exactly one place. The button that emits it is only exposed
when the dialog was constructed with a positive ``active_observation_id``
and the current selection is a measurement set.
"""
from __future__ import annotations

import json
from dataclasses import fields
from typing import Any, Iterable

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.reference_citation import build_observation_reference_snapshot
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceLibraryError,
    ReferenceValidationError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import (
    OBSERVATION_REFERENCE_ROLES,
    REFERENCE_WORK_TYPES,
    REFERENCE_WORK_VERIFICATION_STATUSES,
    REFERENCE_WORK_VISIBILITIES,
)


# --- Verification-status badge ---------------------------------------------

_STATUS_COLORS: dict[str, str] = {
    "incomplete": "#f59e0b",   # amber
    "unverified": "#64748b",   # slate
    "verified": "#16a34a",     # green
}

# ``data_kind`` values the manager exposes for creation. ``parmasto`` is a
# known biometric expression kind that the desktop plot pipeline does not
# render yet; existing records remain viewable but the UI must not offer
# it as a new-record option in this slice.
_CREATABLE_DATA_KINDS: tuple[str, ...] = ("range", "summary", "raw_points")


# --- Small helpers ---------------------------------------------------------


def _empty_to_none(text: str | None) -> str | None:
    if text is None:
        return None
    stripped = str(text).strip()
    return stripped or None


def _parse_optional_float(text: str | None) -> float | None:
    value = _empty_to_none(text)
    if value is None:
        return None
    return float(value)


def _parse_optional_int(text: str | None) -> int | None:
    value = _empty_to_none(text)
    if value is None:
        return None
    return int(value)


def _format_optional(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return ("%g" % value)
    return str(value)


def _snapshot_measurement_details(
    work: ReferenceWork,
    treatment: TaxonTreatment,
    measurement_set: MeasurementSet,
) -> dict[str, Any]:
    """Compose the canonical measurement-set detail dict for display.

    Delegates to :func:`build_observation_reference_snapshot` so the
    detail panel shows exactly the same values the attachment snapshot
    would carry — no local formatting drift, no fabricated statistics.
    """
    return build_observation_reference_snapshot(work, treatment, measurement_set)


def _finite_positive(value: Any) -> bool:
    """Mirror ``references.reference_plotting._finite_positive`` — the
    plot hint must not disagree with the translator on which sets are
    drawable."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    import math
    return math.isfinite(f) and f > 0.0


def _rectangle_drawable(lmin: Any, lmax: Any, wmin: Any, wmax: Any) -> bool:
    if not (
        _finite_positive(lmin)
        and _finite_positive(lmax)
        and _finite_positive(wmin)
        and _finite_positive(wmax)
    ):
        return False
    return float(lmax) > float(lmin) and float(wmax) > float(wmin)


def _measurement_set_is_plottable_hint(measurement_set: MeasurementSet) -> bool:
    """Live UI hint aligned with the translator's plotability rule:

    * ``raw_points`` requires at least one finite, strictly positive
      paired point;
    * ``range`` / ``summary`` require either a finite-positive L/W
      rectangle (core or exceptional) or a finite-positive L/W mean pair.

    Attachment-time plotability (:func:`references.reference_plotting.
    translate_observation_reference_use`) remains authoritative; this
    predicate is duplicated only to avoid a runtime import cycle and
    must be updated in lock-step with the translator's rule.
    """
    kind = str(measurement_set.data_kind or "").strip().lower()
    if kind == "raw_points":
        raw = measurement_set.raw_points_json
        if not raw:
            return False
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(parsed, list) or not parsed:
            return False
        for point in parsed:
            if isinstance(point, dict):
                length = point.get("length")
                if length is None:
                    length = point.get("l")
                width = point.get("width")
                if width is None:
                    width = point.get("w")
                if _finite_positive(length) and _finite_positive(width):
                    return True
        return False
    core_ok = _rectangle_drawable(
        measurement_set.length_core_min,
        measurement_set.length_core_max,
        measurement_set.width_core_min,
        measurement_set.width_core_max,
    )
    ext_ok = _rectangle_drawable(
        measurement_set.length_min,
        measurement_set.length_max,
        measurement_set.width_min,
        measurement_set.width_max,
    )
    mean_ok = _finite_positive(measurement_set.length_mean) and _finite_positive(
        measurement_set.width_mean
    )
    return core_ok or ext_ok or mean_ok


class _VerificationBadge(QFrame):
    """Small colored badge with translated text so verification status is
    distinguishable both by color and by label."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._label = QLabel(self)
        self._label.setContentsMargins(6, 2, 6, 2)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self._label)
        self.set_status("")

    def set_status(self, status: str) -> None:
        status = str(status or "").strip().lower()
        color = _STATUS_COLORS.get(status, "#adb5bd")
        text_map = {
            "incomplete": self.tr("Incomplete"),
            "unverified": self.tr("Unverified"),
            "verified": self.tr("Verified"),
        }
        text = text_map.get(status, self.tr("Unknown"))
        self._label.setText(text)
        self._label.setStyleSheet(
            "QLabel {"
            f" background-color: {color};"
            " color: white;"
            " border-radius: 6px;"
            " padding: 1px 6px;"
            " font-weight: 600;"
            "}"
        )


# --- Work / Treatment / Measurement forms ----------------------------------


class _ReferenceWorkForm(QDialog):
    """Modal form for creating or editing a :class:`ReferenceWork`."""

    def __init__(
        self,
        parent: QWidget | None,
        *,
        work: ReferenceWork | None = None,
    ) -> None:
        super().__init__(parent)
        self._work = work
        self.setWindowTitle(
            self.tr("Edit reference work") if work is not None
            else self.tr("New reference work")
        )
        self.setModal(True)
        self.result_work: ReferenceWork | None = None
        self._build_ui()
        if work is not None:
            self._load(work)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)

        self.type_combo = QComboBox()
        for value in sorted(REFERENCE_WORK_TYPES):
            self.type_combo.addItem(self._work_type_label(value), value)
        form.addRow(self.tr("Type:"), self.type_combo)

        self.title_input = QLineEdit()
        form.addRow(self.tr("Title:"), self.title_input)

        self.short_label_input = QLineEdit()
        self.short_label_input.setPlaceholderText(
            self.tr("Auto-generated when blank")
        )
        form.addRow(self.tr("Short label:"), self.short_label_input)

        self.year_input = QLineEdit()
        self.year_input.setPlaceholderText(self.tr("e.g. 1990"))
        form.addRow(self.tr("Year:"), self.year_input)

        self.authors_input = QLineEdit()
        self.authors_input.setPlaceholderText(
            self.tr("JSON list of {family, given} entries")
        )
        form.addRow(self.tr("Authors JSON:"), self.authors_input)
        self.authors_input.setText("[]")

        self.editors_input = QLineEdit()
        self.editors_input.setPlaceholderText(
            self.tr("JSON list of {family, given} entries")
        )
        self.editors_input.setText("[]")
        form.addRow(self.tr("Editors JSON:"), self.editors_input)

        self.citation_key_input = QLineEdit()
        self.citation_key_input.setPlaceholderText(
            self.tr("Optional short key, e.g. petersen-1990")
        )
        form.addRow(self.tr("Citation key:"), self.citation_key_input)

        self.container_input = QLineEdit()
        form.addRow(self.tr("Container title:"), self.container_input)

        self.edition_input = QLineEdit()
        form.addRow(self.tr("Edition:"), self.edition_input)

        self.volume_input = QLineEdit()
        form.addRow(self.tr("Volume:"), self.volume_input)

        self.issue_input = QLineEdit()
        form.addRow(self.tr("Issue:"), self.issue_input)

        self.pages_input = QLineEdit()
        form.addRow(self.tr("Pages:"), self.pages_input)

        self.publisher_input = QLineEdit()
        form.addRow(self.tr("Publisher:"), self.publisher_input)

        self.place_input = QLineEdit()
        form.addRow(self.tr("Place:"), self.place_input)

        self.doi_input = QLineEdit()
        form.addRow(self.tr("DOI:"), self.doi_input)

        self.isbn_input = QLineEdit()
        form.addRow(self.tr("ISBN:"), self.isbn_input)

        self.url_input = QLineEdit()
        form.addRow(self.tr("URL:"), self.url_input)

        self.language_input = QLineEdit()
        self.language_input.setPlaceholderText(self.tr("ISO code, e.g. en"))
        form.addRow(self.tr("Language:"), self.language_input)

        self.citation_override_input = QPlainTextEdit()
        self.citation_override_input.setPlaceholderText(
            self.tr("Optional full citation override text")
        )
        self.citation_override_input.setFixedHeight(60)
        form.addRow(self.tr("Citation override:"), self.citation_override_input)

        self.verification_combo = QComboBox()
        for value in ("incomplete", "unverified", "verified"):
            if value in REFERENCE_WORK_VERIFICATION_STATUSES:
                self.verification_combo.addItem(
                    self._verification_status_label(value), value
                )
        form.addRow(self.tr("Verification:"), self.verification_combo)

        self.visibility_combo = QComboBox()
        for value in ("private", "shared", "curated_public"):
            if value in REFERENCE_WORK_VISIBILITIES:
                self.visibility_combo.addItem(
                    self._visibility_label(value), value
                )
        form.addRow(self.tr("Visibility:"), self.visibility_combo)

        layout.addLayout(form)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _work_type_label(self, value: str) -> str:
        return {
            "book": self.tr("Book"),
            "article": self.tr("Article"),
            "chapter": self.tr("Chapter"),
            "website": self.tr("Website"),
            "dataset": self.tr("Dataset"),
            "other": self.tr("Other"),
        }.get(value, value)

    def _verification_status_label(self, value: str) -> str:
        return {
            "incomplete": self.tr("Incomplete"),
            "unverified": self.tr("Unverified"),
            "verified": self.tr("Verified"),
        }.get(value, value)

    def _visibility_label(self, value: str) -> str:
        return {
            "private": self.tr("Private"),
            "shared": self.tr("Shared"),
            "curated_public": self.tr("Curated public"),
        }.get(value, value)

    def _load(self, work: ReferenceWork) -> None:
        idx = self.type_combo.findData(work.type or "")
        if idx >= 0:
            self.type_combo.setCurrentIndex(idx)
        self.title_input.setText(work.title or "")
        self.short_label_input.setText(work.short_label or "")
        self.year_input.setText("" if work.year is None else str(work.year))
        self.authors_input.setText(work.authors_json or "[]")
        self.editors_input.setText(work.editors_json or "[]")
        self.citation_key_input.setText(work.citation_key or "")
        self.container_input.setText(work.container_title or "")
        self.edition_input.setText(work.edition or "")
        self.volume_input.setText(work.volume or "")
        self.issue_input.setText(work.issue or "")
        self.pages_input.setText(work.pages or "")
        self.publisher_input.setText(work.publisher or "")
        self.place_input.setText(work.place or "")
        self.doi_input.setText(work.doi or "")
        self.isbn_input.setText(work.isbn or "")
        self.url_input.setText(work.url or "")
        self.language_input.setText(work.language or "")
        self.citation_override_input.setPlainText(work.citation_override or "")
        idx = self.verification_combo.findData(work.verification_status or "")
        if idx >= 0:
            self.verification_combo.setCurrentIndex(idx)
        idx = self.visibility_combo.findData(work.visibility or "")
        if idx >= 0:
            self.visibility_combo.setCurrentIndex(idx)

    def _collect(self) -> dict[str, Any]:
        return {
            "type": str(self.type_combo.currentData() or ""),
            "title": self.title_input.text().strip(),
            "short_label": self.short_label_input.text().strip(),
            "year": _parse_optional_int(self.year_input.text()),
            "authors_json": self.authors_input.text() or "[]",
            "editors_json": self.editors_input.text() or "[]",
            "citation_key": _empty_to_none(self.citation_key_input.text()),
            "container_title": _empty_to_none(self.container_input.text()),
            "edition": _empty_to_none(self.edition_input.text()),
            "volume": _empty_to_none(self.volume_input.text()),
            "issue": _empty_to_none(self.issue_input.text()),
            "pages": _empty_to_none(self.pages_input.text()),
            "publisher": _empty_to_none(self.publisher_input.text()),
            "place": _empty_to_none(self.place_input.text()),
            "doi": _empty_to_none(self.doi_input.text()),
            "isbn": _empty_to_none(self.isbn_input.text()),
            "url": _empty_to_none(self.url_input.text()),
            "language": _empty_to_none(self.language_input.text()),
            "citation_override": _empty_to_none(
                self.citation_override_input.toPlainText()
            ),
            "verification_status": str(self.verification_combo.currentData() or ""),
            "visibility": str(self.visibility_combo.currentData() or ""),
        }

    def _on_save(self) -> None:
        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        try:
            if self._work is None:
                new_work = ReferenceWork(id="", **data)
                self.result_work = ReferenceWorkRepository.create(new_work)
            else:
                self.result_work = ReferenceWorkRepository.update(
                    self._work.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


class _TaxonTreatmentForm(QDialog):
    """Modal form for creating or editing a :class:`TaxonTreatment`."""

    def __init__(
        self,
        parent: QWidget | None,
        *,
        reference_work_id: str,
        treatment: TaxonTreatment | None = None,
    ) -> None:
        super().__init__(parent)
        self._reference_work_id = reference_work_id
        self._treatment = treatment
        self.setWindowTitle(
            self.tr("Edit taxon treatment") if treatment is not None
            else self.tr("New taxon treatment")
        )
        self.setModal(True)
        self.result_treatment: TaxonTreatment | None = None
        self._build_ui()
        if treatment is not None:
            self._load(treatment)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_input = QLineEdit()
        form.addRow(self.tr("Name as published:"), self.name_input)

        self.taxon_input = QLineEdit()
        self.taxon_input.setPlaceholderText(self.tr("Optional taxon id"))
        form.addRow(self.tr("Taxon id:"), self.taxon_input)

        self.page_from_input = QLineEdit()
        form.addRow(self.tr("Page from:"), self.page_from_input)

        self.page_to_input = QLineEdit()
        form.addRow(self.tr("Page to:"), self.page_to_input)

        self.locator_input = QLineEdit()
        form.addRow(self.tr("Locator text:"), self.locator_input)

        self.notes_input = QPlainTextEdit()
        self.notes_input.setFixedHeight(70)
        form.addRow(self.tr("Notes:"), self.notes_input)

        layout.addLayout(form)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _load(self, treatment: TaxonTreatment) -> None:
        self.name_input.setText(treatment.name_as_published or "")
        self.taxon_input.setText(treatment.taxon_id or "")
        self.page_from_input.setText(
            "" if treatment.page_from is None else str(treatment.page_from)
        )
        self.page_to_input.setText(
            "" if treatment.page_to is None else str(treatment.page_to)
        )
        self.locator_input.setText(treatment.locator_text or "")
        self.notes_input.setPlainText(treatment.treatment_notes or "")

    def _collect(self) -> dict[str, Any]:
        return {
            "name_as_published": self.name_input.text().strip(),
            "taxon_id": _empty_to_none(self.taxon_input.text()),
            "page_from": _parse_optional_int(self.page_from_input.text()),
            "page_to": _parse_optional_int(self.page_to_input.text()),
            "locator_text": _empty_to_none(self.locator_input.text()),
            "treatment_notes": _empty_to_none(self.notes_input.toPlainText()),
        }

    def _on_save(self) -> None:
        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        try:
            if self._treatment is None:
                new = TaxonTreatment(
                    id="",
                    reference_work_id=self._reference_work_id,
                    **data,
                )
                self.result_treatment = TaxonTreatmentRepository.create(new)
            else:
                self.result_treatment = TaxonTreatmentRepository.update(
                    self._treatment.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


class _MeasurementSetForm(QDialog):
    """Modal form for creating or editing a :class:`MeasurementSet`.

    Empty numeric inputs map to ``None`` — the dialog never fabricates
    zero or midpoint values. The ``data_kind`` combo restricts creation
    to plot-supported kinds (``range``/``summary``/``raw_points``); the
    ``parmasto`` kind is intentionally excluded for new records.
    """

    def __init__(
        self,
        parent: QWidget | None,
        *,
        taxon_treatment_id: str,
        measurement_set: MeasurementSet | None = None,
    ) -> None:
        super().__init__(parent)
        self._taxon_treatment_id = taxon_treatment_id
        self._measurement_set = measurement_set
        self.setWindowTitle(
            self.tr("Edit measurement set") if measurement_set is not None
            else self.tr("New measurement set")
        )
        self.setModal(True)
        self.result_set: MeasurementSet | None = None
        self._build_ui()
        if measurement_set is not None:
            self._load(measurement_set)
        self._sync_visibility()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.data_kind_combo = QComboBox()
        # When editing an existing parmasto set, keep it selectable so the
        # form does not silently switch kinds; but never offer parmasto
        # as a new choice.
        for value in _CREATABLE_DATA_KINDS:
            self.data_kind_combo.addItem(self._data_kind_label(value), value)
        if (
            self._measurement_set is not None
            and self._measurement_set.data_kind
            and self._measurement_set.data_kind not in _CREATABLE_DATA_KINDS
        ):
            self.data_kind_combo.addItem(
                self._data_kind_label(self._measurement_set.data_kind),
                self._measurement_set.data_kind,
            )
        self.data_kind_combo.currentIndexChanged.connect(
            lambda _idx: self._sync_visibility()
        )
        form.addRow(self.tr("Data kind:"), self.data_kind_combo)

        self.raw_text_input = QLineEdit()
        self.raw_text_input.setPlaceholderText(
            self.tr("e.g. (7.5–)8–10(–10.5) × 5–6(–6.5) µm")
        )
        form.addRow(self.tr("Raw expression:"), self.raw_text_input)

        self.length_min_input = QLineEdit()
        self.length_core_min_input = QLineEdit()
        self.length_core_max_input = QLineEdit()
        self.length_max_input = QLineEdit()
        self.length_mean_input = QLineEdit()
        self.width_min_input = QLineEdit()
        self.width_core_min_input = QLineEdit()
        self.width_core_max_input = QLineEdit()
        self.width_max_input = QLineEdit()
        self.width_mean_input = QLineEdit()
        self.q_min_input = QLineEdit()
        self.q_max_input = QLineEdit()
        self.q_mean_input = QLineEdit()
        self.sample_size_input = QLineEdit()
        self.specimen_count_input = QLineEdit()

        # Length row
        length_row = QHBoxLayout()
        length_row.addWidget(self.length_min_input)
        length_row.addWidget(self.length_core_min_input)
        length_row.addWidget(self.length_core_max_input)
        length_row.addWidget(self.length_max_input)
        length_holder = QWidget()
        length_holder.setLayout(length_row)
        self._length_row_label = QLabel(
            self.tr("Length min / core_min / core_max / max (µm):")
        )
        form.addRow(self._length_row_label, length_holder)

        self._length_mean_label = QLabel(self.tr("Length mean (µm):"))
        form.addRow(self._length_mean_label, self.length_mean_input)

        width_row = QHBoxLayout()
        width_row.addWidget(self.width_min_input)
        width_row.addWidget(self.width_core_min_input)
        width_row.addWidget(self.width_core_max_input)
        width_row.addWidget(self.width_max_input)
        width_holder = QWidget()
        width_holder.setLayout(width_row)
        self._width_row_label = QLabel(
            self.tr("Width min / core_min / core_max / max (µm):")
        )
        form.addRow(self._width_row_label, width_holder)

        self._width_mean_label = QLabel(self.tr("Width mean (µm):"))
        form.addRow(self._width_mean_label, self.width_mean_input)

        q_row = QHBoxLayout()
        q_row.addWidget(self.q_min_input)
        q_row.addWidget(self.q_mean_input)
        q_row.addWidget(self.q_max_input)
        q_holder = QWidget()
        q_holder.setLayout(q_row)
        self._q_row_label = QLabel(self.tr("Q min / mean / max:"))
        form.addRow(self._q_row_label, q_holder)

        self._sample_row_label = QLabel(self.tr("Sample size / specimens:"))
        sample_row = QHBoxLayout()
        sample_row.addWidget(self.sample_size_input)
        sample_row.addWidget(self.specimen_count_input)
        sample_holder = QWidget()
        sample_holder.setLayout(sample_row)
        form.addRow(self._sample_row_label, sample_holder)

        # Raw points editor: visible only when data_kind == raw_points.
        self.raw_points_input = QPlainTextEdit()
        self.raw_points_input.setPlaceholderText(
            self.tr(
                'JSON list, e.g. [{"length": 9.0, "width": 5.5}, '
                '{"length": 9.5, "width": 5.7}]'
            )
        )
        self.raw_points_input.setFixedHeight(120)
        self._raw_points_label = QLabel(self.tr("Raw points JSON:"))
        form.addRow(self._raw_points_label, self.raw_points_input)

        self.mount_medium_input = QLineEdit()
        form.addRow(self.tr("Mount medium:"), self.mount_medium_input)
        self.stain_input = QLineEdit()
        form.addRow(self.tr("Stain:"), self.stain_input)
        self.preparation_input = QLineEdit()
        form.addRow(self.tr("Preparation:"), self.preparation_input)
        self.method_input = QLineEdit()
        form.addRow(self.tr("Measurement method:"), self.method_input)

        self.notes_input = QPlainTextEdit()
        self.notes_input.setFixedHeight(60)
        form.addRow(self.tr("Notes:"), self.notes_input)

        layout.addLayout(form)

        self.error_label = QLabel()
        self.error_label.setStyleSheet("QLabel { color: #dc2626; }")
        self.error_label.setWordWrap(True)
        self.error_label.setVisible(False)
        layout.addWidget(self.error_label)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self
        )
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _data_kind_label(self, value: str) -> str:
        return {
            "range": self.tr("Range"),
            "summary": self.tr("Summary"),
            "raw_points": self.tr("Raw points"),
            "parmasto": self.tr("Parmasto (read-only)"),
        }.get(value, value)

    def _load(self, ms: MeasurementSet) -> None:
        idx = self.data_kind_combo.findData(ms.data_kind or "")
        if idx >= 0:
            self.data_kind_combo.setCurrentIndex(idx)
        self.raw_text_input.setText(ms.raw_text or "")
        self.length_min_input.setText(_format_optional(ms.length_min))
        self.length_core_min_input.setText(_format_optional(ms.length_core_min))
        self.length_core_max_input.setText(_format_optional(ms.length_core_max))
        self.length_max_input.setText(_format_optional(ms.length_max))
        self.length_mean_input.setText(_format_optional(ms.length_mean))
        self.width_min_input.setText(_format_optional(ms.width_min))
        self.width_core_min_input.setText(_format_optional(ms.width_core_min))
        self.width_core_max_input.setText(_format_optional(ms.width_core_max))
        self.width_max_input.setText(_format_optional(ms.width_max))
        self.width_mean_input.setText(_format_optional(ms.width_mean))
        self.q_min_input.setText(_format_optional(ms.q_min))
        self.q_max_input.setText(_format_optional(ms.q_max))
        self.q_mean_input.setText(_format_optional(ms.q_mean))
        self.sample_size_input.setText(_format_optional(ms.sample_size))
        self.specimen_count_input.setText(_format_optional(ms.specimen_count))
        self.raw_points_input.setPlainText(ms.raw_points_json or "")
        self.mount_medium_input.setText(ms.mount_medium or "")
        self.stain_input.setText(ms.stain or "")
        self.preparation_input.setText(ms.preparation or "")
        self.method_input.setText(ms.measurement_method or "")
        self.notes_input.setPlainText(ms.notes or "")

    def _sync_visibility(self) -> None:
        kind = str(self.data_kind_combo.currentData() or "").strip().lower()
        raw_points_visible = kind == "raw_points"
        for widget in (
            self.raw_points_input,
            self._raw_points_label,
        ):
            widget.setVisible(raw_points_visible)
        # Length/width bounds are still meaningful for raw_points as an
        # aggregate summary in some datasets, but the plot-relevant flow
        # for raw_points relies on the raw list. Keep the numeric rows
        # hidden for raw_points to reduce clutter.
        for widget in (
            self.length_min_input,
            self.length_core_min_input,
            self.length_core_max_input,
            self.length_max_input,
            self.length_mean_input,
            self.width_min_input,
            self.width_core_min_input,
            self.width_core_max_input,
            self.width_max_input,
            self.width_mean_input,
            self.q_min_input,
            self.q_max_input,
            self.q_mean_input,
            self.sample_size_input,
            self.specimen_count_input,
            self._length_row_label,
            self._length_mean_label,
            self._width_row_label,
            self._width_mean_label,
            self._q_row_label,
            self._sample_row_label,
        ):
            widget.setVisible(not raw_points_visible)

    def _collect(self) -> dict[str, Any]:
        kind = str(self.data_kind_combo.currentData() or "").strip().lower()
        payload: dict[str, Any] = {
            "character": "spore_size",
            "data_kind": kind,
            "raw_text": _empty_to_none(self.raw_text_input.text()),
            "mount_medium": _empty_to_none(self.mount_medium_input.text()),
            "stain": _empty_to_none(self.stain_input.text()),
            "preparation": _empty_to_none(self.preparation_input.text()),
            "measurement_method": _empty_to_none(self.method_input.text()),
            "notes": _empty_to_none(self.notes_input.toPlainText()),
        }
        # When editing an existing raw_points record we deliberately do NOT
        # blank the hidden aggregate statistics — the form's numeric rows
        # are hidden for raw_points, so writing None on save would silently
        # discard the user's previously-stored means/bounds. The fields
        # are only cleared when the user explicitly converts an existing
        # non-raw_points set INTO raw_points (a kind change), or on a
        # brand new record where those fields never existed.
        existing = self._measurement_set
        preserving_existing_raw_points_stats = (
            kind == "raw_points"
            and existing is not None
            and str(existing.data_kind or "").strip().lower() == "raw_points"
        )
        if kind == "raw_points":
            payload["raw_points_json"] = _empty_to_none(
                self.raw_points_input.toPlainText()
            )
            if preserving_existing_raw_points_stats:
                payload["length_min"] = existing.length_min
                payload["length_core_min"] = existing.length_core_min
                payload["length_core_max"] = existing.length_core_max
                payload["length_max"] = existing.length_max
                payload["length_mean"] = existing.length_mean
                payload["width_min"] = existing.width_min
                payload["width_core_min"] = existing.width_core_min
                payload["width_core_max"] = existing.width_core_max
                payload["width_max"] = existing.width_max
                payload["width_mean"] = existing.width_mean
                payload["q_min"] = existing.q_min
                payload["q_max"] = existing.q_max
                payload["q_mean"] = existing.q_mean
                payload["sample_size"] = existing.sample_size
                payload["specimen_count"] = existing.specimen_count
            else:
                # Fresh raw_points record (or user converted an existing
                # non-raw_points record). No pre-existing aggregate stats
                # for the raw list to overwrite; leave the columns NULL.
                payload["length_min"] = None
                payload["length_core_min"] = None
                payload["length_core_max"] = None
                payload["length_max"] = None
                payload["length_mean"] = None
                payload["width_min"] = None
                payload["width_core_min"] = None
                payload["width_core_max"] = None
                payload["width_max"] = None
                payload["width_mean"] = None
                payload["q_min"] = None
                payload["q_max"] = None
                payload["q_mean"] = None
                payload["sample_size"] = None
                payload["specimen_count"] = None
        else:
            payload["length_min"] = _parse_optional_float(self.length_min_input.text())
            payload["length_core_min"] = _parse_optional_float(
                self.length_core_min_input.text()
            )
            payload["length_core_max"] = _parse_optional_float(
                self.length_core_max_input.text()
            )
            payload["length_max"] = _parse_optional_float(self.length_max_input.text())
            payload["length_mean"] = _parse_optional_float(
                self.length_mean_input.text()
            )
            payload["width_min"] = _parse_optional_float(self.width_min_input.text())
            payload["width_core_min"] = _parse_optional_float(
                self.width_core_min_input.text()
            )
            payload["width_core_max"] = _parse_optional_float(
                self.width_core_max_input.text()
            )
            payload["width_max"] = _parse_optional_float(self.width_max_input.text())
            payload["width_mean"] = _parse_optional_float(
                self.width_mean_input.text()
            )
            payload["q_min"] = _parse_optional_float(self.q_min_input.text())
            payload["q_max"] = _parse_optional_float(self.q_max_input.text())
            payload["q_mean"] = _parse_optional_float(self.q_mean_input.text())
            payload["sample_size"] = _parse_optional_int(
                self.sample_size_input.text()
            )
            payload["specimen_count"] = _parse_optional_int(
                self.specimen_count_input.text()
            )
            # Symmetrical F-001 defense: editing a range/summary record
            # that happens to carry a persisted raw_points_json must not
            # silently drop that JSON. Preserve the prior JSON when the
            # data_kind is unchanged; only clear on an explicit kind
            # conversion (from raw_points into a non-raw kind).
            existing = self._measurement_set
            if (
                existing is not None
                and str(existing.data_kind or "").strip().lower() == kind
            ):
                payload["raw_points_json"] = existing.raw_points_json
            else:
                payload["raw_points_json"] = None
        return payload

    def _on_save(self) -> None:
        try:
            data = self._collect()
        except (TypeError, ValueError) as exc:
            self._show_error(str(exc))
            return
        try:
            if self._measurement_set is None:
                new = MeasurementSet(
                    id="",
                    taxon_treatment_id=self._taxon_treatment_id,
                    **data,
                )
                self.result_set = MeasurementSetRepository.create(new)
            else:
                self.result_set = MeasurementSetRepository.update(
                    self._measurement_set.id, data
                )
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            self._show_error(str(exc))
            return
        self.accept()

    def _show_error(self, text: str) -> None:
        self.error_label.setText(text)
        self.error_label.setVisible(True)


# --- Manager dialog --------------------------------------------------------


class ReferenceLibraryManagerDialog(QDialog):
    """Top-level Reference Library manager dialog.

    :param active_observation_id: When present, an "Attach to active
        observation" button is exposed for measurement-set selections.
        The dialog itself never mutates ``observation_reference_uses`` —
        it emits :attr:`attach_requested` and closes so MainWindow can
        route the attachment through its shared helper.
    """

    #: Emitted when the user requests an attachment.
    #: ``(measurement_set_id, role, observation_id)`` — role is a value
    #: from :data:`OBSERVATION_REFERENCE_ROLES`; ``observation_id`` is
    #: the observation id CAPTURED at manager-open time, so a rebind of
    #: the parent's active observation between open and click cannot
    #: silently redirect the attachment to a different observation.
    attach_requested = Signal(str, str, int)

    #: Emitted whenever a record was created/edited so external listeners
    #: (e.g. the attachment chooser) can refresh their state.
    library_changed = Signal()

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        active_observation_id: int | None = None,
    ) -> None:
        super().__init__(parent)
        self._active_observation_id = (
            int(active_observation_id) if active_observation_id else None
        )
        self.setWindowTitle(self.tr("Reference Library"))
        self.setModal(True)
        self.resize(1000, 640)

        self._works: list[ReferenceWork] = []
        self._current_work: ReferenceWork | None = None
        self._current_treatment: TaxonTreatment | None = None
        self._current_measurement_set: MeasurementSet | None = None

        self._build_ui()
        self.refresh_works()

    # --- UI construction ---

    def _build_ui(self) -> None:
        outer = QVBoxLayout(self)
        outer.setContentsMargins(10, 10, 10, 10)
        outer.setSpacing(8)

        splitter = QSplitter(Qt.Horizontal, self)
        splitter.setChildrenCollapsible(False)

        splitter.addWidget(self._build_works_pane())
        splitter.addWidget(self._build_hierarchy_pane())
        splitter.addWidget(self._build_detail_pane())
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 4)
        outer.addWidget(splitter, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Close, Qt.Horizontal, self)
        buttons.rejected.connect(self.reject)
        close_btn = buttons.button(QDialogButtonBox.Close)
        if close_btn is not None:
            close_btn.clicked.connect(self.reject)
        outer.addWidget(buttons)

    def _build_works_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)

        layout.addWidget(QLabel(self.tr("Publications")))

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(self.tr("Search by title, author or label"))
        self.search_input.textChanged.connect(self._on_search_changed)
        layout.addWidget(self.search_input)

        self.works_table = QTableWidget(0, 3, self)
        self.works_table.setHorizontalHeaderLabels(
            [self.tr("Short label"), self.tr("Year"), self.tr("Status")]
        )
        self.works_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.works_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.works_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.works_table.verticalHeader().setVisible(False)
        header = self.works_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.works_table.itemSelectionChanged.connect(self._on_work_selected)
        layout.addWidget(self.works_table, 1)

        row = QHBoxLayout()
        self.new_work_btn = QPushButton(self.tr("New work…"))
        self.new_work_btn.clicked.connect(self._on_new_work_clicked)
        row.addWidget(self.new_work_btn)
        self.edit_work_btn = QPushButton(self.tr("Edit work…"))
        self.edit_work_btn.clicked.connect(self._on_edit_work_clicked)
        self.edit_work_btn.setEnabled(False)
        row.addWidget(self.edit_work_btn)
        row.addStretch(1)
        layout.addLayout(row)

        self._works_placeholder = QLabel(
            self.tr("No publications yet — use \"New work…\" to add one.")
        )
        self._works_placeholder.setWordWrap(True)
        self._works_placeholder.setStyleSheet("QLabel { color: #64748b; }")
        layout.addWidget(self._works_placeholder)
        return pane

    def _build_hierarchy_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(QLabel(self.tr("Treatments and measurement sets")))
        self.hierarchy_tree = QTreeWidget()
        self.hierarchy_tree.setHeaderLabels(
            [self.tr("Name / kind"), self.tr("Revision")]
        )
        self.hierarchy_tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.hierarchy_tree.itemSelectionChanged.connect(self._on_hierarchy_selected)
        layout.addWidget(self.hierarchy_tree, 1)

        row = QHBoxLayout()
        self.new_treatment_btn = QPushButton(self.tr("New treatment…"))
        self.new_treatment_btn.clicked.connect(self._on_new_treatment_clicked)
        self.new_treatment_btn.setEnabled(False)
        row.addWidget(self.new_treatment_btn)
        self.new_set_btn = QPushButton(self.tr("New measurement set…"))
        self.new_set_btn.clicked.connect(self._on_new_measurement_set_clicked)
        self.new_set_btn.setEnabled(False)
        row.addWidget(self.new_set_btn)
        row.addStretch(1)
        layout.addLayout(row)
        return pane

    def _build_detail_pane(self) -> QWidget:
        pane = QWidget()
        layout = QVBoxLayout(pane)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(QLabel(self.tr("Details")))

        header_row = QHBoxLayout()
        self.detail_title = QLabel()
        self.detail_title.setStyleSheet("QLabel { font-weight: 600; }")
        header_row.addWidget(self.detail_title, 1)
        self.status_badge = _VerificationBadge()
        header_row.addWidget(self.status_badge)
        layout.addLayout(header_row)

        self.detail_view = QPlainTextEdit()
        self.detail_view.setReadOnly(True)
        layout.addWidget(self.detail_view, 1)

        self.plot_hint_label = QLabel()
        self.plot_hint_label.setWordWrap(True)
        self.plot_hint_label.setStyleSheet("QLabel { color: #b45309; }")
        self.plot_hint_label.setVisible(False)
        layout.addWidget(self.plot_hint_label)

        self.edit_selected_btn = QPushButton(self.tr("Edit selected…"))
        self.edit_selected_btn.clicked.connect(self._on_edit_selected_clicked)
        self.edit_selected_btn.setEnabled(False)
        layout.addWidget(self.edit_selected_btn)

        attach_row = QHBoxLayout()
        attach_row.addWidget(QLabel(self.tr("Role:")))
        self.role_combo = QComboBox()
        for role in ("compared", "supports_identification", "contradicts"):
            if role in OBSERVATION_REFERENCE_ROLES:
                self.role_combo.addItem(self._role_display_label(role), role)
        attach_row.addWidget(self.role_combo, 1)
        self.attach_btn = QPushButton(self.tr("Attach to active observation"))
        self.attach_btn.clicked.connect(self._on_attach_clicked)
        attach_row.addWidget(self.attach_btn)
        layout.addLayout(attach_row)
        self._update_attach_button_visibility()
        return pane

    # --- Data refresh helpers ---

    def refresh_works(self, *, select_id: str | None = None) -> None:
        query = self.search_input.text().strip() if hasattr(self, "search_input") else ""
        try:
            works = ReferenceWorkRepository.search(query or None, limit=200)
        except ReferenceLibraryError as exc:
            QMessageBox.warning(
                self,
                self.tr("Reference Library"),
                self.tr("Could not load publications: {error}").format(error=str(exc)),
            )
            works = []
        self._works = works
        self.works_table.setRowCount(0)
        for work in works:
            row = self.works_table.rowCount()
            self.works_table.insertRow(row)
            label_item = QTableWidgetItem(work.short_label or work.title or "")
            label_item.setData(Qt.UserRole, work.id)
            self.works_table.setItem(row, 0, label_item)
            year_item = QTableWidgetItem("" if work.year is None else str(work.year))
            self.works_table.setItem(row, 1, year_item)
            status_item = QTableWidgetItem(
                self._verification_label(work.verification_status)
            )
            color = _STATUS_COLORS.get(
                (work.verification_status or "").strip().lower(), None
            )
            if color:
                status_item.setForeground(QColor(color))
            self.works_table.setItem(row, 2, status_item)
        self._works_placeholder.setVisible(not works)
        if select_id:
            for row in range(self.works_table.rowCount()):
                item = self.works_table.item(row, 0)
                if item and item.data(Qt.UserRole) == select_id:
                    self.works_table.selectRow(row)
                    break
        else:
            self._current_work = None
            self._clear_hierarchy()
            self._clear_detail()

    def _verification_label(self, status: str | None) -> str:
        s = (status or "").strip().lower()
        return {
            "incomplete": self.tr("Incomplete"),
            "unverified": self.tr("Unverified"),
            "verified": self.tr("Verified"),
        }.get(s, self.tr("Unknown"))

    def _add_hierarchy_placeholder(self, text: str) -> None:
        """Insert a disabled placeholder item into the hierarchy tree so
        the empty state is visible instead of an unlabelled blank pane."""
        item = QTreeWidgetItem([text, ""])
        item.setFlags(Qt.ItemIsEnabled)  # visible, not selectable
        item.setForeground(0, QColor("#64748b"))
        self.hierarchy_tree.addTopLevelItem(item)

    def _refresh_hierarchy_for_current_work(
        self, *, select_treatment_id: str | None = None,
        select_set_id: str | None = None,
    ) -> None:
        self.hierarchy_tree.clear()
        if self._current_work is None:
            self._add_hierarchy_placeholder(
                self.tr("Select a publication to see its treatments.")
            )
            return
        try:
            treatments = TaxonTreatmentRepository.list_for_work(
                self._current_work.id
            )
        except ReferenceLibraryError as exc:
            QMessageBox.warning(
                self,
                self.tr("Reference Library"),
                self.tr("Could not load treatments: {error}").format(error=str(exc)),
            )
            treatments = []
        if not treatments:
            self._add_hierarchy_placeholder(
                self.tr("No treatments yet — use \"New treatment…\" to add one.")
            )
            return
        selection_target = None
        for treatment in treatments:
            t_item = QTreeWidgetItem(
                [
                    treatment.name_as_published or self.tr("(unnamed treatment)"),
                    str(treatment.revision or 1),
                ]
            )
            t_item.setData(0, Qt.UserRole, ("treatment", treatment.id))
            self.hierarchy_tree.addTopLevelItem(t_item)
            try:
                sets = MeasurementSetRepository.list_for_treatment(treatment.id)
            except ReferenceLibraryError:
                sets = []
            for ms in sets:
                s_item = QTreeWidgetItem(
                    [
                        f"{ms.data_kind} — {ms.raw_text or ms.id}",
                        str(ms.revision or 1),
                    ]
                )
                s_item.setData(0, Qt.UserRole, ("measurement_set", ms.id))
                t_item.addChild(s_item)
                if select_set_id and ms.id == select_set_id:
                    selection_target = s_item
            if selection_target is None and select_treatment_id == treatment.id:
                selection_target = t_item
            t_item.setExpanded(True)
        if selection_target is not None:
            self.hierarchy_tree.setCurrentItem(selection_target)

    # --- Selection slots ---

    def _on_search_changed(self, _text: str) -> None:
        self.refresh_works()

    def _on_work_selected(self) -> None:
        rows = self.works_table.selectionModel().selectedRows() if self.works_table.selectionModel() else []
        if not rows:
            self._current_work = None
            self.edit_work_btn.setEnabled(False)
            self.new_treatment_btn.setEnabled(False)
            self.new_set_btn.setEnabled(False)
            self._clear_hierarchy()
            self._clear_detail()
            return
        row = rows[0].row()
        item = self.works_table.item(row, 0)
        work_id = item.data(Qt.UserRole) if item else None
        work = next((w for w in self._works if w.id == work_id), None)
        self._current_work = work
        self._current_treatment = None
        self._current_measurement_set = None
        self.edit_work_btn.setEnabled(work is not None)
        self.new_treatment_btn.setEnabled(work is not None)
        self.new_set_btn.setEnabled(False)
        self._refresh_hierarchy_for_current_work()
        if work is not None:
            self._render_work_detail(work)
        self._update_attach_button_visibility()

    def _on_hierarchy_selected(self) -> None:
        item = self.hierarchy_tree.currentItem()
        self._current_treatment = None
        self._current_measurement_set = None
        if item is None:
            self.new_set_btn.setEnabled(False)
            if self._current_work is not None:
                self._render_work_detail(self._current_work)
            self._update_attach_button_visibility()
            return
        payload = item.data(0, Qt.UserRole)
        if not isinstance(payload, tuple) or len(payload) != 2:
            self._update_attach_button_visibility()
            return
        kind, entity_id = payload
        if kind == "treatment":
            treatment = TaxonTreatmentRepository.get(entity_id)
            self._current_treatment = treatment
            self.new_set_btn.setEnabled(treatment is not None)
            self.edit_selected_btn.setEnabled(treatment is not None)
            if treatment is not None:
                self._render_treatment_detail(treatment)
        elif kind == "measurement_set":
            ms = MeasurementSetRepository.get(entity_id)
            self._current_measurement_set = ms
            if ms is not None:
                parent_item = item.parent()
                if parent_item is not None:
                    parent_payload = parent_item.data(0, Qt.UserRole)
                    if isinstance(parent_payload, tuple) and parent_payload[0] == "treatment":
                        self._current_treatment = TaxonTreatmentRepository.get(
                            parent_payload[1]
                        )
                self._render_measurement_set_detail(ms)
                self.new_set_btn.setEnabled(self._current_treatment is not None)
                self.edit_selected_btn.setEnabled(True)
        self._update_attach_button_visibility()

    # --- Detail rendering ---

    def _clear_hierarchy(self) -> None:
        self.hierarchy_tree.clear()
        self._current_treatment = None
        self._current_measurement_set = None
        self._add_hierarchy_placeholder(
            self.tr("Select a publication to see its treatments.")
        )

    def _clear_detail(self) -> None:
        self.detail_title.setText("")
        self.detail_view.setPlainText("")
        self.status_badge.set_status("")
        self.edit_selected_btn.setEnabled(False)
        self.plot_hint_label.setVisible(False)

    def _render_work_detail(self, work: ReferenceWork) -> None:
        self.detail_title.setText(work.short_label or work.title or work.id)
        self.status_badge.set_status(work.verification_status or "")
        lines = [
            self.tr("Type: {type}").format(type=work.type or ""),
            self.tr("Title: {title}").format(title=work.title or ""),
        ]
        if work.year is not None:
            lines.append(self.tr("Year: {year}").format(year=work.year))
        if work.citation_key:
            lines.append(self.tr("Citation key: {value}").format(value=work.citation_key))
        if work.container_title:
            lines.append(self.tr("Container: {value}").format(value=work.container_title))
        if work.edition:
            lines.append(self.tr("Edition: {value}").format(value=work.edition))
        if work.volume:
            lines.append(self.tr("Volume: {value}").format(value=work.volume))
        if work.issue:
            lines.append(self.tr("Issue: {value}").format(value=work.issue))
        if work.pages:
            lines.append(self.tr("Pages: {value}").format(value=work.pages))
        if work.publisher:
            lines.append(self.tr("Publisher: {value}").format(value=work.publisher))
        if work.place:
            lines.append(self.tr("Place: {value}").format(value=work.place))
        if work.doi:
            lines.append(self.tr("DOI: {value}").format(value=work.doi))
        if work.isbn:
            lines.append(self.tr("ISBN: {value}").format(value=work.isbn))
        if work.url:
            lines.append(self.tr("URL: {value}").format(value=work.url))
        if work.language:
            lines.append(self.tr("Language: {value}").format(value=work.language))
        if work.citation_override:
            lines.append(self.tr("Citation override: {value}").format(value=work.citation_override))
        lines.append(self.tr("Visibility: {value}").format(value=work.visibility or ""))
        lines.append(self.tr("Revision: {value}").format(value=work.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=work.id))
        self.detail_view.setPlainText("\n".join(lines))
        self.plot_hint_label.setVisible(False)
        self.edit_selected_btn.setEnabled(False)

    def _render_treatment_detail(self, treatment: TaxonTreatment) -> None:
        parent_work = self._current_work
        if parent_work is not None:
            self.detail_title.setText(
                f"{parent_work.short_label or parent_work.title or ''} — "
                f"{treatment.name_as_published or ''}"
            )
            self.status_badge.set_status(parent_work.verification_status or "")
        else:
            self.detail_title.setText(treatment.name_as_published or "")
            self.status_badge.set_status("")
        lines = [
            self.tr("Name as published: {value}").format(value=treatment.name_as_published or ""),
        ]
        if treatment.taxon_id:
            lines.append(self.tr("Taxon id: {value}").format(value=treatment.taxon_id))
        if treatment.page_from is not None or treatment.page_to is not None:
            lines.append(
                self.tr("Pages: {a}-{b}").format(
                    a=treatment.page_from if treatment.page_from is not None else "",
                    b=treatment.page_to if treatment.page_to is not None else "",
                )
            )
        if treatment.locator_text:
            lines.append(self.tr("Locator: {value}").format(value=treatment.locator_text))
        if treatment.treatment_notes:
            lines.append(self.tr("Notes: {value}").format(value=treatment.treatment_notes))
        lines.append(self.tr("Revision: {value}").format(value=treatment.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=treatment.id))
        self.detail_view.setPlainText("\n".join(lines))
        self.plot_hint_label.setVisible(False)

    def _render_measurement_set_detail(self, ms: MeasurementSet) -> None:
        work = self._current_work
        treatment = self._current_treatment
        if work is not None and treatment is not None:
            try:
                snapshot = _snapshot_measurement_details(work, treatment, ms)
            except Exception:
                snapshot = None
        else:
            snapshot = None
        self.detail_title.setText(
            self.tr("Measurement set — {kind}").format(kind=ms.data_kind or "")
        )
        if work is not None:
            self.status_badge.set_status(work.verification_status or "")
        else:
            self.status_badge.set_status("")

        lines: list[str] = []
        if snapshot is not None:
            lines.append(self.tr("Data kind: {value}").format(value=snapshot.get("data_kind") or ""))
            raw_text = snapshot.get("raw_text")
            if raw_text:
                lines.append(self.tr("Raw expression: {value}").format(value=raw_text))
            measurements = snapshot.get("measurements") or {}
            for label, key in (
                (self.tr("Length min"), "length_min"),
                (self.tr("Length core min"), "length_core_min"),
                (self.tr("Length core max"), "length_core_max"),
                (self.tr("Length max"), "length_max"),
                (self.tr("Length mean"), "length_mean"),
                (self.tr("Width min"), "width_min"),
                (self.tr("Width core min"), "width_core_min"),
                (self.tr("Width core max"), "width_core_max"),
                (self.tr("Width max"), "width_max"),
                (self.tr("Width mean"), "width_mean"),
                (self.tr("Q min"), "q_min"),
                (self.tr("Q mean"), "q_mean"),
                (self.tr("Q max"), "q_max"),
                (self.tr("Sample size"), "sample_size"),
                (self.tr("Specimen count"), "specimen_count"),
            ):
                value = measurements.get(key)
                if value is not None:
                    lines.append(f"{label}: {value}")
            raw_points = snapshot.get("raw_points")
            if isinstance(raw_points, list) and raw_points:
                lines.append(
                    self.tr("Raw points: {count} entries").format(count=len(raw_points))
                )
        else:
            lines.append(self.tr("Data kind: {value}").format(value=ms.data_kind or ""))
            if ms.raw_text:
                lines.append(self.tr("Raw expression: {value}").format(value=ms.raw_text))
        lines.append(self.tr("Revision: {value}").format(value=ms.revision or 1))
        lines.append(self.tr("UUID: {value}").format(value=ms.id))
        self.detail_view.setPlainText("\n".join(lines))

        if _measurement_set_is_plottable_hint(ms):
            self.plot_hint_label.setVisible(False)
        else:
            self.plot_hint_label.setText(
                self.tr(
                    "This measurement set is not plottable yet: it lacks a "
                    "drawable length/width rectangle, a complete mean pair, "
                    "or valid raw points."
                )
            )
            self.plot_hint_label.setVisible(True)

    # --- Actions ---

    def _on_new_work_clicked(self) -> None:
        form = _ReferenceWorkForm(self)
        if form.exec() == QDialog.Accepted and form.result_work is not None:
            self.library_changed.emit()
            self.refresh_works(select_id=form.result_work.id)

    def _on_edit_work_clicked(self) -> None:
        if self._current_work is None:
            return
        form = _ReferenceWorkForm(self, work=self._current_work)
        if form.exec() == QDialog.Accepted and form.result_work is not None:
            self.library_changed.emit()
            self.refresh_works(select_id=form.result_work.id)

    def _on_new_treatment_clicked(self) -> None:
        if self._current_work is None:
            return
        form = _TaxonTreatmentForm(
            self, reference_work_id=self._current_work.id
        )
        if form.exec() == QDialog.Accepted and form.result_treatment is not None:
            self.library_changed.emit()
            self._refresh_hierarchy_for_current_work(
                select_treatment_id=form.result_treatment.id
            )

    def _on_new_measurement_set_clicked(self) -> None:
        if self._current_treatment is None:
            return
        form = _MeasurementSetForm(
            self, taxon_treatment_id=self._current_treatment.id
        )
        if form.exec() == QDialog.Accepted and form.result_set is not None:
            self.library_changed.emit()
            self._refresh_hierarchy_for_current_work(
                select_set_id=form.result_set.id
            )

    def _on_edit_selected_clicked(self) -> None:
        if self._current_measurement_set is not None:
            form = _MeasurementSetForm(
                self,
                taxon_treatment_id=self._current_measurement_set.taxon_treatment_id,
                measurement_set=self._current_measurement_set,
            )
            if form.exec() == QDialog.Accepted and form.result_set is not None:
                self.library_changed.emit()
                self._refresh_hierarchy_for_current_work(
                    select_set_id=form.result_set.id
                )
            return
        if self._current_treatment is not None and self._current_work is not None:
            form = _TaxonTreatmentForm(
                self,
                reference_work_id=self._current_work.id,
                treatment=self._current_treatment,
            )
            if form.exec() == QDialog.Accepted and form.result_treatment is not None:
                self.library_changed.emit()
                self._refresh_hierarchy_for_current_work(
                    select_treatment_id=form.result_treatment.id
                )
            return

    def _role_display_label(self, value: str) -> str:
        return {
            "compared": self.tr("Compared"),
            "supports_identification": self.tr("Supports identification"),
            "contradicts": self.tr("Contradicts"),
        }.get(value, value)

    def _on_attach_clicked(self) -> None:
        if self._active_observation_id is None:
            return
        if self._current_measurement_set is None:
            return
        role_data = self.role_combo.currentData()
        role = (
            str(role_data)
            if isinstance(role_data, str) and role_data
            else "compared"
        )
        # The captured observation id is passed with the signal so the
        # attach handler cannot silently redirect the attachment onto a
        # different active observation should the parent's selection have
        # drifted between the manager being opened and the click.
        self.attach_requested.emit(
            self._current_measurement_set.id,
            role,
            int(self._active_observation_id),
        )
        self.accept()

    def _update_attach_button_visibility(self) -> None:
        has_observation = self._active_observation_id is not None
        has_set = self._current_measurement_set is not None
        self.attach_btn.setVisible(has_observation)
        self.role_combo.setVisible(has_observation)
        self.attach_btn.setEnabled(has_observation and has_set)

    # --- Public helpers used by tests / callers ---

    def current_selection_kind(self) -> str:
        if self._current_measurement_set is not None:
            return "measurement_set"
        if self._current_treatment is not None:
            return "treatment"
        if self._current_work is not None:
            return "work"
        return "empty"


__all__ = [
    "ReferenceLibraryManagerDialog",
]
