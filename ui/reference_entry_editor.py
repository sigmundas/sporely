"""Reusable "enter reference measurements manually" editor widget.

Extracted from the old panel-level ``ReferenceAddDialog`` (still defined in
``ui/main_window.py`` as a thin modal wrapper around this widget, kept for
its other callers: Quick add and the per-row Edit action) so the same
paste/parse table, publication picker, and existing-measurement-set chooser
can also be embedded — without a modal ``QDialog`` — as the "Enter manually"
tab of the unified :class:`~ui.add_reference_dialog.AddReferenceDialog`
picker. No parser, validation, or normalized-payload logic is duplicated;
this module owns all of it and both callers share it verbatim.

The editor is one column: the publication the measurements come from,
then the paste-and-parse field and its 3 x 5 grid of plain inputs, then
the two secondary measurement paths (individual spore rows, Parmasto
biometrics) behind disclosures. What the grid holds is folded live into a
:class:`~references.measurement_content.MeasurementContent`, and the
shared preview pane renders that through the same comparison model the
Library and Community tabs use -- so a descriptor the parser established
survives all the way to what the user sees, and a statistic the source
never printed is never drawn.

Comparison-target identity vs. observation identity
----------------------------------------------------
The picker lets the user choose which taxon's published data to compare
against ("Compare against:" — the active observation's own taxon, an AI
suggestion, or a freely typed name), independent of the observation being
annotated. That target may legitimately have a different
``sporely_taxon_id`` than the observation. ``sporely_taxon_id`` on the
result payload is therefore the *target's* id (used to build the
``TaxonTreatment``), while ``observation_taxon_id`` is the *observation's
own*, unchanging id, captured once at construction and used only by
``MainWindow._persist_normalized_reference_from_dialog``'s drift guard to
detect the observation itself changing underneath an open dialog. The two
are deliberately different fields so an intentionally different target is
never mistaken for observation drift, and drift detection never borrows a
different target's id as if it were the observation's own.
"""
from __future__ import annotations

import json
import math
import re

from PySide6.QtCore import (
    QCoreApplication,
    QPoint,
    Qt,
    QSignalBlocker,
    QSortFilterProxyModel,
    Signal,
)
from PySide6.QtGui import QKeySequence
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QComboBox,
    QCompleter,
    QDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSizePolicy,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    SUPPORTED_ATTACHMENT_DATA_KINDS,
    TaxonTreatmentRepository,
)
from references.measurement_content import (
    METRICS,
    MeasurementContent,
    MeasurementContentError,
    MeasurementDetails,
    ScalarStatistic,
    clear_pair,
    encode_measurement_details,
    legacy_projection_losses,
    swap_length_width as swap_content_length_width,
)
from references.measurement_content_gates import (
    blocking_projection_losses,
    enhanced_editing_enabled,
)
from references.reference_plotting import range_payload_is_plottable

from . import measurement_content_view as mcv
from .dialog_helpers import CollapsibleSection, make_github_help_button
from .hint_status import HintBar, HintStatusController
from .styles import pt


class SporeDataTable(QTableWidget):
    """Editable table for spore data with auto-added rows and Q calculation."""

    def __init__(self, parent=None):
        super().__init__(0, 3, parent)
        self.setObjectName("referenceSporeTable")
        self.setFocusPolicy(Qt.StrongFocus)
        self.setHorizontalHeaderLabels([self.tr("Length (μm)"), self.tr("Width (μm)"), "Q"])
        header = self.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.verticalHeader().setVisible(False)
        self.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.setEditTriggers(QAbstractItemView.AllEditTriggers)
        self._updating_q = False
        self.itemChanged.connect(self._on_item_changed)
        self._ensure_rows(1)

    def _ensure_rows(self, count):
        while self.rowCount() < count:
            row = self.rowCount()
            self.insertRow(row)
            q_item = QTableWidgetItem("")
            q_item.setFlags(q_item.flags() & ~Qt.ItemIsEditable)
            self.setItem(row, 2, q_item)

    def _on_item_changed(self, item):
        if self._updating_q:
            return
        if item.column() not in (0, 1):
            return
        self._update_q_for_row(item.row())

    def _update_q_for_row(self, row):
        if row < 0 or row >= self.rowCount():
            return
        length = self._cell_float(row, 0)
        width = self._cell_float(row, 1)
        q_value = None
        if length is not None and width is not None and width > 0:
            q_value = length / width
        self._updating_q = True
        try:
            if self.item(row, 2) is None:
                q_item = QTableWidgetItem("")
                q_item.setFlags(q_item.flags() & ~Qt.ItemIsEditable)
                self.setItem(row, 2, q_item)
            self.item(row, 2).setText(f"{q_value:.2f}" if q_value is not None else "")
        finally:
            self._updating_q = False

    def _cell_float(self, row, col):
        item = self.item(row, col)
        if not item:
            return None
        try:
            return float(item.text().strip())
        except ValueError:
            return None

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Paste):
            self._paste_from_clipboard()
            return
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            current = self.currentIndex()
            if current.isValid() and current.row() == self.rowCount() - 1:
                self._ensure_rows(self.rowCount() + 1)
                self.setCurrentCell(self.rowCount() - 1, current.column())
                return
        text = event.text()
        if text and not text.isspace():
            item = self.currentItem()
            if item and (item.flags() & Qt.ItemIsEditable):
                if self.state() != QAbstractItemView.EditingState:
                    self.editItem(item)
        super().keyPressEvent(event)

    def _paste_from_clipboard(self):
        text = QApplication.clipboard().text()
        if not text:
            return
        rows = [line for line in text.splitlines() if line.strip()]
        if not rows:
            return
        start_row = max(0, self.currentRow())
        start_col = max(0, self.currentColumn())
        needed_rows = start_row + len(rows)
        self._ensure_rows(needed_rows)
        for r_index, line in enumerate(rows):
            cols = [c.strip() for c in re.split(r"[\t,;]", line) if c.strip()]
            for c_index, value in enumerate(cols[:2]):
                row = start_row + r_index
                col = start_col + c_index
                if col > 1:
                    break
                item = self.item(row, col)
                if item is None:
                    item = QTableWidgetItem()
                    self.setItem(row, col, item)
                item.setText(value)
            self._update_q_for_row(start_row + r_index)

    def get_points(self) -> list[dict]:
        points = []
        for row in range(self.rowCount()):
            length = self._cell_float(row, 0)
            width = self._cell_float(row, 1)
            if length is None or width is None or width <= 0:
                continue
            points.append({"length_um": float(length), "width_um": float(width)})
        return points


class _PublicationSearchProxyModel(QSortFilterProxyModel):
    """Proxy model backing the publication-picker completer.

    Re-exposes each source row's private search corpus
    (``Qt.UserRole + 1``, populated by
    :meth:`ReferenceEntryEditor._populate_publication_combo`) via
    ``Qt.EditRole`` — the role :class:`QCompleter` matches against by
    default. Every other role is forwarded unchanged so the completer's
    popup still renders the clean display label instead of the noisy
    corpus text.
    """

    def data(self, index, role=Qt.DisplayRole):  # type: ignore[override]
        if role == Qt.EditRole:
            source = self.mapToSource(index)
            if source.isValid():
                corpus = source.data(Qt.UserRole + 1)
                if corpus:
                    return corpus
                return source.data(Qt.DisplayRole)
        return super().data(index, role)


class ReferenceEntryEditor(QWidget):
    """Paste/parse measurement editor + publication picker + Data section.

    Owns the 3 x 5 measurement grid, the spore-points table, Parmasto fields, the
    publication combo, and the "use an existing measurement set / enter new
    data" choice. Has no Save/Cancel chrome and does not itself accept or
    reject anything — callers call :meth:`validate_and_build_result` and
    read :meth:`result_data` (plus the accessor methods below) to persist.
    """

    data_changed = Signal()

    def __init__(
        self,
        parent,
        genus: str,
        species: str,
        data: dict | None = None,
        observation_id: int | None = None,
        sporely_taxon_id: int | None = None,
        observation_taxon_id: int | None = None,
        require_explicit_publication_assignment: bool = False,
        staged: bool = False,
    ):
        super().__init__(parent)
        # Two steps (``ReferenceCreateDialog``, redesign stage 7d): the
        # publication on one page, the measurements on the next, and the
        # value grid hidden until a string is parsed or "Fill in manually".
        self._staged = bool(staged)
        self._result = None
        self._genus = genus
        self._species = species
        self._prefill_data = data or {}
        self._hint_controller: HintStatusController | None = None
        self._hint_registrations: list[tuple[QWidget, str, str]] = []
        self._validation_labels: list[QLabel] = []
        self._invalid_entry: tuple | None = None
        self._plot_color = None
        self._require_explicit_publication_assignment = bool(
            require_explicit_publication_assignment
        )
        self._default_hint_text = QCoreApplication.translate("ReferenceAddDialog", "Paste from Excel/csv or type values")
        # Normalized-library context: the observation this editor's result
        # will (if submitted) be attached to. ``_sporely_taxon_id`` is the
        # CURRENT comparison target's taxon id (may change via
        # ``set_comparison_target``); ``_observation_taxon_id`` is the
        # observation's own, fixed for this editor's lifetime. See the
        # module docstring.
        self._observation_id: int | None = self._coerce_int_or_none(observation_id)
        self._sporely_taxon_id: int | None = self._coerce_int_or_none(sporely_taxon_id)
        self._observation_taxon_id: int | None = (
            self._coerce_int_or_none(observation_taxon_id)
            if observation_taxon_id
            else self._sporely_taxon_id
        )
        self._selected_work_id: str | None = None
        self._pending_reference_work: ReferenceWork | None = None
        self._pending_reference_work_label: str | None = None
        self._selected_measurement_set_id: str | None = None
        self._normalized_write_ambiguous: bool = False
        self._measurement_column_hints = {
            0: QCoreApplication.translate("ReferenceAddDialog", "Extreme min: outermost observed value (parenthesised in literature)."),
            1: QCoreApplication.translate("ReferenceAddDialog", "Typical min: lower end of the typical range, e.g. the unparenthesised left value."),
            2: QCoreApplication.translate("ReferenceAddDialog", "Mean/central value when explicitly supplied by the source. Not calculated automatically. A source that reports the mean as an interval may be entered as 9.2-11.7."),
            3: QCoreApplication.translate("ReferenceAddDialog", "Typical max: upper end of the typical range, e.g. the unparenthesised right value."),
            4: QCoreApplication.translate("ReferenceAddDialog", "Extreme max: outermost observed value (parenthesised in literature)."),
        }

        # One column, in the approved reading order: who published it, then
        # what they measured. The measurement tab bar this replaced put the
        # min/max grid, the spore table and the Parmasto biometrics on equal
        # footing, which hid the fact that pasting a published expression is
        # the path almost every entry takes.
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        # Where each part goes: one column, or (staged) one page per step.
        if self._staged:
            self.steps = QStackedWidget(self)
            self.source_page = QWidget(self.steps)
            source_column = QVBoxLayout(self.source_page)
            self.measurements_page = QWidget(self.steps)
            measure_column = QVBoxLayout(self.measurements_page)
            for column in (source_column, measure_column):
                column.setContentsMargins(0, 0, 0, 0)
                column.setSpacing(10)
            self.steps.addWidget(self.source_page)
            self.steps.addWidget(self.measurements_page)
        else:
            self.steps = None
            source_column = measure_column = layout

        # Built before the sections so every field below can register its
        # own hint as it is constructed; the bar itself is added to the
        # bottom of the column at the end of this method.
        self.hint_bar = HintBar(self)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        self._hint_controller.set_hint(self._default_hint_text)

        self._measurement_group = QGroupBox(
            QCoreApplication.translate("ReferenceAddDialog", "Measurements")
        )
        self._measurement_group.setObjectName("referenceMeasurementsGroup")
        minmax_layout = QVBoxLayout(self._measurement_group)
        minmax_layout.setContentsMargins(8, 8, 8, 8)
        minmax_layout.setSpacing(6)

        paste_label = QLabel(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Paste the measurement string from the literature — it fills "
                "the grid below and keeps the notation's statistical meaning.",
            )
        )
        paste_label.setWordWrap(True)
        paste_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        minmax_layout.addWidget(paste_label)

        self.measurement_paste_input = QLineEdit()
        self.measurement_paste_input.setPlaceholderText(
            QCoreApplication.translate("ReferenceAddDialog", "e.g. (9.5–)9.8–11.3(–11.7) × (7.3–)8.0–9.4(–9.4) µm, Q = 1.2–1.3, Qm = 1.25, n = 36")
        )
        self.measurement_paste_input.setClearButtonEnabled(True)
        self.measurement_paste_input.returnPressed.connect(self._on_parse_measurement_clicked)

        paste_button_row = QHBoxLayout()
        paste_button_row.setContentsMargins(0, 0, 0, 0)
        paste_button_row.addWidget(self.measurement_paste_input, 1)
        self._parse_measurement_btn = QPushButton(QCoreApplication.translate("ReferenceAddDialog", "Parse"))
        self._parse_measurement_btn.setToolTip(QCoreApplication.translate("ReferenceAddDialog", "Parse the pasted string into the grid below."))
        self._parse_measurement_btn.clicked.connect(self._on_parse_measurement_clicked)
        self._swap_lw_btn = QPushButton(QCoreApplication.translate("ReferenceAddDialog", "Swap L↔W"))
        self._swap_lw_btn.setToolTip(QCoreApplication.translate("ReferenceAddDialog", "Swap the Length and Width rows (in case the source lists width first)."))
        self._swap_lw_btn.clicked.connect(self._on_swap_lw_clicked)
        paste_button_row.addWidget(self._parse_measurement_btn)
        minmax_layout.addLayout(paste_button_row)

        self.fill_manually_btn = QPushButton(
            QCoreApplication.translate("ReferenceAddDialog", "Fill in manually"), self
        )
        self.fill_manually_btn.setObjectName("linkButton")
        self.fill_manually_btn.setCursor(Qt.PointingHandCursor)
        self.fill_manually_btn.clicked.connect(self._on_fill_manually_clicked)
        self.fill_manually_btn.setVisible(False)
        if self._staged:
            minmax_layout.addWidget(self.fill_manually_btn, 0, Qt.AlignLeft)

        # Parse sits with the field it acts on; the two corrective actions go
        # below. Keeping all three inline squeezed the paste field down to a
        # few visible characters at the dialog's ordinary width, which is the
        # one control the user is meant to read what they pasted in.
        # The corrections and the grid are widgets of their own so the staged
        # editor can keep them hidden until there are values to show.
        self._correction_row_widget = QWidget(self._measurement_group)
        correction_row = QHBoxLayout(self._correction_row_widget)
        correction_row.setContentsMargins(0, 0, 0, 0)
        correction_row.addWidget(self._swap_lw_btn)
        correction_row.addStretch(1)
        minmax_layout.addWidget(self._correction_row_widget)

        self._measurement_preview_label = QLabel("")
        self._measurement_preview_label.setWordWrap(True)
        self._measurement_preview_label.setTextFormat(Qt.RichText)
        self._measurement_preview_label.setStyleSheet(
            f"color: #7f8c8d; font-size: {pt(9)}pt; padding: 2px 0px;"
        )
        minmax_layout.addWidget(self._measurement_preview_label)

        self.measurement_grid_panel = QWidget(self._measurement_group)
        grid_panel_layout = QVBoxLayout(self.measurement_grid_panel)
        grid_panel_layout.setContentsMargins(0, 0, 0, 0)
        grid_panel_layout.setSpacing(minmax_layout.spacing())
        minmax_layout.addWidget(self.measurement_grid_panel)

        # Reported statistics (contract version 1): what the source says about
        # the numbers in the table below. They sit with the rest of the parse
        # feedback — compact tags with the full sentence behind them, the
        # reported median/S.D. on their own line so neither is ever read as a
        # mean, and the notice about what this version will actually store.
        self._measurement_content: MeasurementContent | None = None
        self.measurement_tags_label = QLabel("")
        self.measurement_tags_label.setObjectName("referenceMeasurementTags")
        self.measurement_tags_label.setWordWrap(True)
        self.measurement_tags_label.setVisible(False)
        # No colour override: the tags are content, and a hardcoded slate
        # foreground is unreadable against the application's dark theme.
        self.measurement_tags_label.setStyleSheet(
            f"font-size: {pt(9)}pt; padding: 2px 0px 0px 0px;"
        )
        grid_panel_layout.addWidget(self.measurement_tags_label)

        self.reported_statistics_label = QLabel("")
        self.reported_statistics_label.setObjectName("referenceReportedStatistics")
        self.reported_statistics_label.setWordWrap(True)
        self.reported_statistics_label.setVisible(False)
        self.reported_statistics_label.setStyleSheet(
            f"color: #7f8c8d; font-size: {pt(9)}pt;"
        )
        grid_panel_layout.addWidget(self.reported_statistics_label)

        self._reported_statistics_notice_label = QLabel("")
        self._reported_statistics_notice_label.setObjectName(
            "referenceReportedStatisticsNotice"
        )
        self._reported_statistics_notice_label.setWordWrap(True)
        self._reported_statistics_notice_label.setVisible(False)
        self._reported_statistics_notice_label.setStyleSheet(
            f"color: #b58900; font-style: italic; font-size: {pt(9)}pt; padding: 0px 0px 2px 0px;"
        )
        grid_panel_layout.addWidget(self._reported_statistics_notice_label)

        self._raw_measurement_text: str = ""
        prefill_meta = self._prefill_data.get("metadata_json") if self._prefill_data else None
        if isinstance(prefill_meta, dict):
            existing_raw = prefill_meta.get("raw_measurement")
            if isinstance(existing_raw, str) and existing_raw.strip():
                self._raw_measurement_text = existing_raw
                self.measurement_paste_input.setText(existing_raw)

        # Three metric rows by five plain inputs (contract N24), not a table
        # widget. A QTableWidget owns its own focus and edit model, so the
        # keyboard order across a row was Qt's business rather than ours and
        # a cell only committed its value on an item change; five real line
        # edits per row give an explicit left-to-right tab chain and a live
        # ``textChanged`` the preview can follow as the user types.
        self._formatting_minmax = False
        column_labels = (
            QCoreApplication.translate("ReferenceAddDialog", "extreme min"),
            QCoreApplication.translate("ReferenceAddDialog", "typical min"),
            QCoreApplication.translate("ReferenceAddDialog", "mean"),
            QCoreApplication.translate("ReferenceAddDialog", "typical max"),
            QCoreApplication.translate("ReferenceAddDialog", "extreme max"),
        )
        row_labels = (
            QCoreApplication.translate("ReferenceAddDialog", "Length"),
            QCoreApplication.translate("ReferenceAddDialog", "Width"),
            QCoreApplication.translate("ReferenceAddDialog", "Q"),
        )
        grid = QGridLayout()
        grid.setObjectName("referenceMeasurementGrid")
        grid.setContentsMargins(0, 4, 0, 0)
        grid.setHorizontalSpacing(6)
        grid.setVerticalSpacing(4)
        for col, column_label in enumerate(column_labels):
            header = QLabel(column_label)
            header.setAlignment(Qt.AlignCenter)
            header.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
            grid.addWidget(header, 0, col + 1)
        self.measurement_inputs: list[list[QLineEdit]] = []
        for row, row_label in enumerate(row_labels):
            metric_label = QLabel(row_label)
            metric_label.setStyleSheet("font-weight: 600;")
            grid.addWidget(metric_label, row + 1, 0)
            row_inputs: list[QLineEdit] = []
            for col in range(5):
                field = QLineEdit()
                field.setObjectName(f"referenceMeasurement_{row}_{col}")
                field.setAlignment(Qt.AlignCenter)
                field.setAccessibleName(f"{row_label} {column_labels[col]}")
                field.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
                if col == self._MEAN_COLUMN:
                    field.setPlaceholderText(
                        QCoreApplication.translate("ReferenceAddDialog", "if reported")
                    )
                else:
                    field.setPlaceholderText("—")
                # The column hints used to live on the removed table's
                # header. On the inputs themselves each hint is attached to
                # the field it actually describes, so hovering a cell
                # explains that cell rather than a shared header row.
                self._register_hint_widget(field, self._measurement_column_hints.get(col))
                field.textChanged.connect(
                    lambda _text, r=row, c=col: self._on_measurement_text_edited(r, c)
                )
                field.editingFinished.connect(
                    lambda r=row, c=col: self._on_measurement_editing_finished(r, c)
                )
                grid.addWidget(field, row + 1, col + 1)
                row_inputs.append(field)
            self.measurement_inputs.append(row_inputs)
        for col in range(5):
            grid.setColumnStretch(col + 1, 1)
        grid_panel_layout.addLayout(grid)

        # Contract N24: the keyboard order is stated, not inherited. Qt's
        # default chain follows construction order, which happens to be
        # row-major today — but "happens to be" is not a contract, and a
        # later insertion into this block would silently reorder it.
        flat_inputs = [field for row_inputs in self.measurement_inputs for field in row_inputs]
        for previous, following in zip(flat_inputs, flat_inputs[1:]):
            QWidget.setTabOrder(previous, following)

        measurement_footnote = QLabel(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Typical min/max is not a 5–95% percentile range. Leave the "
                "mean empty unless the source states one — a range midpoint "
                "is not a mean.",
            )
        )
        measurement_footnote.setWordWrap(True)
        measurement_footnote.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        grid_panel_layout.addWidget(measurement_footnote)
        self.measurement_validation_label = self._make_validation_label(
            "referenceMeasurementValidationMessage"
        )
        minmax_layout.addWidget(self.measurement_validation_label)
        measure_column.addWidget(self._measurement_group)

        # Correction is offered as *retraction*: a reader who knows a tag is
        # wrong can drop that metric's range interpretation, or discard the
        # reported statistics altogether, while every measured number stays.
        # Asserting a different interpretation is deliberately not offered —
        # see ``measurement_content_view.without_range_tags``.
        # A QPushButton rather than a QToolButton: it carries a menu just as
        # well and inherits the application's button styling, which a bare
        # tool button does not.
        self._clear_reported_statistics_btn = QPushButton(
            QCoreApplication.translate("ReferenceAddDialog", "Correct interpretation")
        )
        self._clear_reported_statistics_btn.setToolTip(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Drop a range interpretation this source does not actually "
                "state, or discard the reported statistics altogether. The "
                "measured values in the table are left untouched.",
            )
        )
        self._reported_statistics_menu = QMenu(self._clear_reported_statistics_btn)
        self._clear_reported_statistics_btn.setMenu(self._reported_statistics_menu)
        self._clear_reported_statistics_btn.setVisible(False)
        # Beside Swap L↔W: both are corrections to the parsed result, and
        # both stay out of the way until there is something to correct.
        correction_row.insertWidget(1, self._clear_reported_statistics_btn)

        # Secondary measurement paths. Both remain fully functional, and
        # neither is the path a published range takes, so both sit behind
        # the shared disclosure the library manager already uses rather
        # than competing with the grid above (contract N23).
        self._spore_section = CollapsibleSection(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Individual spore measurements (paste a block from Excel/CSV)",
            ),
            self,
        )
        self.spore_table = SporeDataTable()
        self.spore_table.itemChanged.connect(lambda *_: self._emit_data_changed())
        self._spore_section.body_layout().addWidget(self.spore_table)
        self.points_validation_label = self._make_validation_label(
            "referencePointsValidationMessage"
        )
        self._spore_section.body_layout().addWidget(self.points_validation_label)
        measure_column.addWidget(self._spore_section)

        self._parmasto_section = CollapsibleSection(
            QCoreApplication.translate("ReferenceAddDialog", "Parmasto Biometrics"), self
        )
        parmasto_layout = QFormLayout()
        parmasto_layout.setContentsMargins(0, 0, 0, 0)
        parmasto_layout.setSpacing(6)
        parmasto_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self.parmasto_inputs: dict[str, QLineEdit] = {}

        def _math_label(html: str, description: str) -> QLabel:
            label = QLabel(f"{html}:")
            label.setTextFormat(Qt.RichText)
            label.setToolTip(description)
            return label

        parmasto_fields = [
            ("parmasto_length_mean", "<span style=\"text-decoration: overline;\">L</span>", QCoreApplication.translate("ReferenceAddDialog", "Species mean length")),
            ("parmasto_width_mean", "<span style=\"text-decoration: overline;\">W</span>", QCoreApplication.translate("ReferenceAddDialog", "Species mean width")),
            ("parmasto_q_mean", "<span style=\"text-decoration: overline;\">Q</span>", QCoreApplication.translate("ReferenceAddDialog", "Species mean quotient")),
            ("parmasto_v_sp_length", "V<sub>spL</sub>", QCoreApplication.translate("ReferenceAddDialog", "Inter-specimen CV for length means (%)")),
            ("parmasto_v_sp_width", "V<sub>spW</sub>", QCoreApplication.translate("ReferenceAddDialog", "Inter-specimen CV for width means (%)")),
            ("parmasto_v_sp_q", "V<sub>spQ</sub>", QCoreApplication.translate("ReferenceAddDialog", "Inter-specimen CV for quotient means (%)")),
            ("parmasto_v_ind_length", "<span style=\"text-decoration: overline;\">V</span><sub>indL</sub>", QCoreApplication.translate("ReferenceAddDialog", "Average intra-specimen variation for length (%)")),
            ("parmasto_v_ind_width", "<span style=\"text-decoration: overline;\">V</span><sub>indW</sub>", QCoreApplication.translate("ReferenceAddDialog", "Average intra-specimen variation for width (%)")),
            ("parmasto_v_ind_q", "<span style=\"text-decoration: overline;\">V</span><sub>indE</sub>", QCoreApplication.translate("ReferenceAddDialog", "Average intra-specimen variation for quotient (Parmasto VindE) (%)")),
        ]
        for key, math_label, description in parmasto_fields:
            line_edit = QLineEdit()
            line_edit.setPlaceholderText(description)
            line_edit.setToolTip(description)
            line_edit.textChanged.connect(lambda *_: self._emit_data_changed())
            parmasto_layout.addRow(_math_label(math_label, description), line_edit)
            self.parmasto_inputs[key] = line_edit
        self._parmasto_section.body_layout().addLayout(parmasto_layout)
        measure_column.addWidget(self._parmasto_section)

        pub_group = QGroupBox(QCoreApplication.translate("ReferenceAddDialog", "Publication"))
        pub_layout = QVBoxLayout(pub_group)
        pub_layout.setContentsMargins(8, 8, 8, 8)
        pub_layout.setSpacing(6)
        pub_row = QHBoxLayout()
        self.publication_combo = QComboBox()
        self.publication_combo.setEditable(True)
        self.publication_combo.setInsertPolicy(QComboBox.NoInsert)
        self.publication_combo.setPlaceholderText(
            QCoreApplication.translate("ReferenceAddDialog", "Search existing publications by title, authors, or citation key")
        )
        self._publication_completer: QCompleter | None = None
        self._publication_search_proxy: QSortFilterProxyModel | None = None
        self.publication_combo.currentIndexChanged.connect(self._on_publication_selected)
        self.publication_combo.editTextChanged.connect(
            self._on_publication_edit_text_changed
        )
        self._publication_search_seen_ids: set[str] = set()
        pub_row.addWidget(self.publication_combo, 1)
        self.new_publication_btn = QPushButton(QCoreApplication.translate("ReferenceAddDialog", "New publication…"))
        self.new_publication_btn.setToolTip(
            QCoreApplication.translate("ReferenceAddDialog", "Create a new publication in the reference library.")
        )
        self.new_publication_btn.clicked.connect(self._on_new_publication_clicked)
        pub_row.addWidget(self.new_publication_btn)
        pub_layout.addLayout(pub_row)
        self.publication_validation_label = self._make_validation_label(
            "referencePublicationValidationMessage"
        )
        pub_layout.addWidget(self.publication_validation_label)

        treatment_form = QFormLayout()
        # No read-only "Taxon" row here: both hosts already show the taxon
        # directly above the editor (the picker's Reference taxon selector,
        # the quick-add dialog's heading), and a second, uneditable copy only
        # invited attempts to change it here.
        current_taxon_label = " ".join(part for part in (genus, species) if part).strip()
        self.name_as_published_input = QLineEdit()
        # Prefilled with the current taxon name, not left blank. The field is
        # required by TaxonTreatmentRepository._validate, and the publication
        # normally uses the same name as the normalized taxon, so a blank
        # field just blocks the save with a raw validation message. Prefilling
        # keeps the stored value visible and editable before saving rather
        # than substituting one silently at save time, so recording a synonym
        # or historical combination is still a deliberate, visible edit.
        self.name_as_published_input.setText(current_taxon_label)
        self.name_as_published_input.setPlaceholderText(
            QCoreApplication.translate("ReferenceAddDialog", "Name exactly as published (e.g., as written in the publication)")
        )
        self.name_as_published_input.setToolTip(
            QCoreApplication.translate("ReferenceAddDialog",
                "The exact name used in the publication. This can be an old synonym, "
                "spelling variant, or historical combination — separate from the reference taxon."
            )
        )
        treatment_form.addRow(
            QCoreApplication.translate("ReferenceAddDialog", "Name as published:"), self.name_as_published_input
        )
        self.locator_input = QLineEdit()
        self.locator_input.setPlaceholderText(
            QCoreApplication.translate("ReferenceAddDialog", "Page, figure, table, plate, or section")
        )
        treatment_form.addRow(QCoreApplication.translate("ReferenceAddDialog", "Locator:"), self.locator_input)
        pub_layout.addLayout(treatment_form)

        self._no_taxon_notice_label = QLabel(
            QCoreApplication.translate("ReferenceAddDialog", 
                "This reference is not linked to a species in the database. "
                "Choose a reference taxon above to link it; otherwise it is "
                "saved under the name as published only."
            )
            if not self._staged
            # No taxon selector in the two-step dialog: the taxon is the
            # observation's, so the way to link it is the observation's own.
            else QCoreApplication.translate("ReferenceAddDialog",
                "The observation's taxon is not linked to a species in the "
                "database, so this reference is saved under the name as "
                "published only. Link the taxon in Edit to connect it."
            )
        )
        self._no_taxon_notice_label.setWordWrap(True)
        self._no_taxon_notice_label.setObjectName("referenceNoTaxonNotice")
        # An amber panel rather than a line of italics: contract N26 makes
        # this a real identity gap the user is meant to notice and act on,
        # not decoration.
        self._no_taxon_notice_label.setStyleSheet(
            "color: #8a6d00; background-color: rgba(181, 137, 0, 40);"
            " border: 1px solid rgba(181, 137, 0, 120); border-radius: 4px;"
            " padding: 6px 8px;"
        )
        self._no_taxon_notice_label.setVisible(
            bool(self._observation_id) and not self._sporely_taxon_id
        )
        pub_layout.addWidget(self._no_taxon_notice_label)
        # Staged only: the drawer lists existing sets; this says they exist.
        self.existing_sets_note = QLabel("", pub_group)
        self.existing_sets_note.setObjectName("referenceExistingSetsNote")
        self.existing_sets_note.setWordWrap(True)
        self.existing_sets_note.setVisible(False)
        pub_layout.addWidget(self.existing_sets_note)
        # Publication first: the mockup's information hierarchy is who
        # published it, then what they measured.
        if self._staged:
            source_column.addWidget(pub_group)
        else:
            layout.insertWidget(0, pub_group)

        data_group = QGroupBox(QCoreApplication.translate("ReferenceAddDialog", "Data"))
        data_layout = QVBoxLayout(data_group)
        data_layout.setContentsMargins(8, 8, 8, 8)
        data_layout.setSpacing(6)
        self._data_choice_group = QButtonGroup(self)
        self.use_existing_radio = QRadioButton(
            QCoreApplication.translate("ReferenceAddDialog", "Use existing measurement set")
        )
        self.enter_new_radio = QRadioButton(QCoreApplication.translate("ReferenceAddDialog", "Enter new data"))
        self.enter_new_radio.setChecked(True)
        self._data_choice_group.addButton(self.use_existing_radio, 0)
        self._data_choice_group.addButton(self.enter_new_radio, 1)
        self.use_existing_radio.setEnabled(False)
        data_layout.addWidget(self.use_existing_radio)
        data_layout.addWidget(self.enter_new_radio)

        self._existing_search_input = QLineEdit()
        self._existing_search_input.setPlaceholderText(
            QCoreApplication.translate("ReferenceAddDialog", "Filter existing sets by locator, kind, or raw expression…")
        )
        self._existing_search_input.setClearButtonEnabled(True)
        self._existing_search_input.textChanged.connect(
            self._refresh_existing_sets_table
        )
        data_layout.addWidget(self._existing_search_input)
        self._existing_sets_table = QTableWidget(0, 3)
        self._existing_sets_table.setHorizontalHeaderLabels(
            [QCoreApplication.translate("ReferenceAddDialog", "Locator"), QCoreApplication.translate("ReferenceAddDialog", "Kind"), QCoreApplication.translate("ReferenceAddDialog", "Raw expression")]
        )
        self._existing_sets_table.setSelectionBehavior(
            QAbstractItemView.SelectRows
        )
        self._existing_sets_table.setSelectionMode(
            QAbstractItemView.SingleSelection
        )
        self._existing_sets_table.setEditTriggers(
            QAbstractItemView.NoEditTriggers
        )
        self._existing_sets_table.verticalHeader().setVisible(False)
        ex_header = self._existing_sets_table.horizontalHeader()
        ex_header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        ex_header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        ex_header.setSectionResizeMode(2, QHeaderView.Stretch)
        self._existing_sets_table.setMinimumHeight(120)
        self._existing_sets_table.itemSelectionChanged.connect(
            self._on_existing_set_selection_changed
        )
        data_layout.addWidget(self._existing_sets_table)
        self.data_validation_label = self._make_validation_label(
            "referenceDataValidationMessage"
        )
        data_layout.addWidget(self.data_validation_label)
        self._existing_sets_cache: list[MeasurementSet] = []
        self._legacy_source_prefill: str | None = None
        source_column.addWidget(data_group)
        # Spare height goes below the form. Without this a host that scrolls
        # the editor at viewport height spreads it between the sections, and
        # the collapsed disclosures float in large empty gaps.
        if self._staged:
            # Picking an existing set is the reference drawer's job; this
            # editor only creates. A note on the first page says when the
            # publication already has sets for this taxon.
            data_group.setVisible(False)
            source_column.addStretch(1)
            measure_column.addStretch(1)
            layout.addWidget(self.steps, 1)
        else:
            layout.addStretch(1)

        self.use_existing_radio.toggled.connect(self._on_data_choice_toggled)
        self.enter_new_radio.toggled.connect(self._on_data_choice_toggled)
        # A validation message goes as soon as the entry changes (not when a
        # field merely loses focus, which also reports data_changed).
        self.data_changed.connect(self._clear_invalid_if_changed)
        self.publication_combo.editTextChanged.connect(self._clear_invalid_if_changed)
        self.publication_combo.currentIndexChanged.connect(self._clear_invalid_if_changed)
        self.measurement_paste_input.textChanged.connect(self._clear_invalid_if_changed)

        # The hint/status row is its own widget so a host that scrolls this
        # editor can lift it out (take_hint_row) and pin it below the
        # scroll area, next to its action buttons. A standalone editor keeps
        # it at the bottom of its own layout.
        self.hint_row_widget = QWidget(self)
        hint_row = QHBoxLayout(self.hint_row_widget)
        hint_row.setContentsMargins(0, 0, 0, 0)
        hint_row.addWidget(self.hint_bar, 1)
        hint_row.addWidget(make_github_help_button(self, "reference-data-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
        layout.addWidget(self.hint_row_widget)

        self._register_hint_widget(self.spore_table, self._default_hint_text)

        self._populate_publication_combo()
        self._apply_prefill()
        self._on_data_choice_toggled()
        # Not staged, or reopening values: the grid is there from the start.
        self.set_grid_revealed(not self._staged or self._has_grid_values())
        self._emit_data_changed()

    @staticmethod
    def _coerce_int_or_none(value) -> int | None:
        """Best-effort int coercion, failing soft on non-numeric ids.

        The picker's "Compare against" target id is opaque to this editor
        (an AI/typed target may carry a non-numeric placeholder in tests
        and review fixtures); only a genuinely numeric id is meaningful
        here (it becomes the treatment's taxon foreign key).
        """
        if not value:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Comparison-target handling
    # ------------------------------------------------------------------

    def set_comparison_target(
        self, *, genus: str, species: str, sporely_taxon_id: int | None
    ) -> None:
        """Rebind the editor to a newly selected comparison target.

        Resets publication/treatment identity (selected work, pending new
        work, the publication combo text, and any already-built result) so
        a payload prepared for the previous target can never be silently
        relabeled onto this one. Entered measurement values (min/max, spore
        points, Parmasto) are literature data independent of the target and
        are preserved.
        """
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()
        self._sporely_taxon_id = self._coerce_int_or_none(sporely_taxon_id)
        self._result = None
        self._selected_work_id = None
        self._pending_reference_work = None
        self._pending_reference_work_label = None
        self._legacy_source_prefill = None
        blocker = QSignalBlocker(self.publication_combo)
        try:
            self.publication_combo.setCurrentIndex(0)
            self.publication_combo.setEditText("")
        finally:
            del blocker
        taxon_label = " ".join(part for part in (self._genus, self._species) if part).strip()
        # Re-prefill for the new target rather than blanking. A name typed
        # for the previous taxon must not survive onto this one, and leaving
        # it empty would block the save on a required field (see the same
        # reasoning where this input is constructed).
        self.name_as_published_input.setText(taxon_label)
        self._refresh_existing_sets_cache()
        self._no_taxon_notice_label.setVisible(
            bool(self._observation_id) and not self._sporely_taxon_id
        )
        self._emit_data_changed()

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    def _emit_data_changed(self) -> None:
        self.data_changed.emit()

    # ------------------------------------------------------------------
    # Hints
    # ------------------------------------------------------------------

    def take_hint_row(self) -> QWidget:
        """Remove the hint/status row from this editor's layout and return
        it, for a host to place outside its scroll area. The hint
        controller keeps driving it (focus, parser and validation hints)."""
        layout = self.layout()
        if layout is not None:
            layout.removeWidget(self.hint_row_widget)
        self.hint_row_widget.setParent(None)
        return self.hint_row_widget

    def _register_hint_widget(self, widget: QWidget, hint_text: str | None, tone: str = "info") -> None:
        if not widget:
            return
        hint = (hint_text or "").strip()
        hint_tone = (tone or "info").strip().lower()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", hint_tone)
        widget.setToolTip("")
        self._hint_registrations.append((widget, hint, hint_tone))
        if self._hint_controller is not None:
            self._hint_controller.register_widget(widget, hint, tone=hint_tone)

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    # Validation (stage 10e2): what keeps the entry from being saved is said
    # in red by the part of the form it is about, instead of a message box,
    # and goes when that part is changed.
    def _make_validation_label(self, name: str) -> QLabel:
        label = QLabel("", self)
        label.setObjectName(name)
        label.setWordWrap(True)
        label.setStyleSheet("color: #c0392b;")
        label.setVisible(False)
        self._validation_labels.append(label)
        return label

    def _entry_state(self) -> tuple:
        """What the user has entered, to tell a change from a focus change."""
        table = self.spore_table
        return (
            self.publication_combo.currentText(),
            self.measurement_paste_input.text(),
            self._selected_work_id,
            self._pending_reference_work is not None,
            self.use_existing_radio.isChecked(),
            self._selected_measurement_set_id,
            tuple(field.text() for row in self.measurement_inputs for field in row),
            tuple(field.text() for field in self.parmasto_inputs.values()),
            tuple(
                item.text() if item is not None else ""
                for r in range(table.rowCount())
                for c in range(table.columnCount())
                for item in (table.item(r, c),)
            ),
            self._blocked_losses_text(),
        )

    def _blocked_losses_text(self) -> str:
        try:
            return repr(self._blocked_projection_losses())
        except Exception:  # an unreadable entry: compared by its fields above
            return ""

    def _show_invalid(self, label: QLabel, field: QWidget | None, message: str) -> None:
        self._clear_invalid()
        label.setText(message)
        label.setVisible(True)
        if field is not None and field.isVisible():
            # Moving the focus can finish an edit (a cell's 9 becomes 9.00),
            # which reports a change: the entry is compared from after it.
            field.setFocus(Qt.OtherFocusReason)
        self._invalid_entry = self._entry_state()
        # A host that scrolls the editor brings the message into view, its
        # start included (ensureWidgetVisible centres a label wider than the
        # view, cutting off the beginning of the sentence).
        scroll = self.parentWidget()
        while scroll is not None and not isinstance(scroll, QScrollArea):
            scroll = scroll.parentWidget()
        if scroll is not None and scroll.widget() is not None:
            QApplication.sendPostedEvents()  # the label's new height
            if scroll.widget().layout() is not None:
                scroll.widget().layout().activate()
            origin = label.mapTo(scroll.widget(), QPoint(0, 0))
            view = scroll.viewport().size()
            bar = scroll.verticalScrollBar()
            bottom = origin.y() + label.sizeHint().height() + 8
            if bottom > bar.value() + view.height():
                bar.setValue(bottom - view.height())
            if origin.y() < bar.value():
                bar.setValue(max(0, origin.y() - 8))
            hbar = scroll.horizontalScrollBar()
            if origin.x() < hbar.value():
                hbar.setValue(max(0, origin.x() - 8))

    def _clear_invalid(self, *_args) -> None:
        self._invalid_entry = None
        for label in self._validation_labels:
            label.clear()
            label.setVisible(False)

    def _clear_invalid_if_changed(self, *_args) -> None:
        if self._invalid_entry is not None and self._entry_state() != self._invalid_entry:
            self._clear_invalid()

    def validation_message(self) -> str:
        """The validation message showing, or "" (for hosts, tests and scenarios)."""
        return next(
            (label.text() for label in self._validation_labels if not label.isHidden()), ""
        )

    def explain_ambiguous_publication(self) -> None:
        """Say, by the publication field, that the typed name matches several."""
        self._show_invalid(
            self.publication_validation_label,
            self.publication_combo,
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Several publications match this name — choose one from the list.",
            ),
        )

    # ------------------------------------------------------------------
    # The 3 x 5 measurement grid
    # ------------------------------------------------------------------

    def _measurement_input(self, row: int, col: int) -> QLineEdit:
        return self.measurement_inputs[row][col]

    def measurement_cell_text(self, row: int, col: int) -> str:
        """The grid cell's current text, for hosts, tests and scenarios."""
        return self._measurement_input(row, col).text().strip()

    def set_measurement_cell_text(self, row: int, col: int, text: str) -> None:
        """Write one grid cell as if the user had typed it and left the field.

        The canonicalisation an edit would have triggered runs here too, so a
        programmatic fill and a typed one cannot leave the working content in
        two different states.
        """
        self._measurement_input(row, col).setText(text)
        self._on_measurement_editing_finished(row, col)

    def _table_value(self, row, col):
        text = self.measurement_cell_text(row, col)
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _on_measurement_text_edited(self, row: int, col: int) -> None:
        """Fold every keystroke into the working content and repaint.

        Live, rather than on commit: the comparison pane beside the grid is
        the point of the redesign, and a preview that only caught up when the
        field lost focus would be describing the previous entry. Folding is
        non-strict, so a half-typed mean (``9.2-``) leaves that metric's
        previously typed mean alone instead of nagging on every character;
        :meth:`_on_measurement_editing_finished` is where an unreadable mean
        is actually refused.
        """
        if self._formatting_minmax:
            return
        self._sync_measurement_content_from_table()
        self._refresh_measurement_content_view()
        self._emit_data_changed()

    def _on_measurement_editing_finished(self, row: int, col: int) -> None:
        """Canonicalise one committed cell, then re-fold.

        The Mean column accepts a scalar *or* an interval. A scalar keeps the
        grid's two-decimal presentation; an interval is normalised to
        ``lower-upper`` with equal endpoints preserved, because ``9.2-9.2`` is
        a different statement from ``9.2`` and the contract stores the two
        differently. Unreadable text is refused outright — the cell is never
        silently reinterpreted as the number it starts with.
        """
        if self._formatting_minmax:
            return
        widget = self._measurement_input(row, col)
        text = widget.text().strip()
        if col == self._MEAN_COLUMN:
            try:
                statistic = mcv.parse_mean_cell(text)
            except MeasurementContentError as exc:
                self._set_hint(str(exc), tone="warning")
                return
            if statistic is None:
                canonical = ""
            elif isinstance(statistic, ScalarStatistic):
                canonical = f"{statistic.value:.2f}"
            else:
                canonical = mcv.format_statistic(statistic)
        elif not text:
            canonical = ""
        else:
            try:
                value = float(text)
            except ValueError:
                # Left as typed rather than erased: the user still owns the
                # text, and the fold has already read it as "no value".
                return
            canonical = f"{value:.2f}"
        if canonical != widget.text():
            self._formatting_minmax = True
            try:
                widget.setText(canonical)
            finally:
                self._formatting_minmax = False
        self._sync_measurement_content_from_table()
        self._refresh_measurement_content_view()
        self._emit_data_changed()

    _MINMAX_ROW_BY_DIMENSION = {"length": 0, "width": 1, "q": 2}

    #: Grid columns, left to right: extreme min, typical min, mean, typical
    #: max, extreme max. Rows come from ``_MINMAX_ROW_BY_DIMENSION``.
    _MEAN_COLUMN = 2
    _PAIR_COLUMNS_BY_METRIC: dict[str, tuple[str, str, str, str]] = {
        "length": ("length_min", "length_core_min", "length_core_max", "length_max"),
        "width": ("width_min", "width_core_min", "width_core_max", "width_max"),
        "q": ("q_min", "q_core_min", "q_core_max", "q_max"),
    }

    def _mean_cell_text(self, row: int) -> str:
        return self.measurement_cell_text(row, self._MEAN_COLUMN)

    def _sync_measurement_content_from_table(self) -> None:
        """Fold the grid's current numbers back into the typed content.

        The grid is the editable surface; the typed content carries what the
        source said about it. Plain numbers move by assignment, but every
        *transition* — emptying a described pair, clearing a mean, switching a
        mean between a scalar and an interval — goes through the contract's
        own edit operations, so a descriptor can never outlive the numbers it
        describes and the two shapes of a mean can never both be stored.

        Content written by a newer version is left exactly as it was found:
        it is inspect-only everywhere else and must not be rewritten here.
        """
        content = self._measurement_content
        if content is None:
            # Manual entry starts with no typed content. An empty one is the
            # right starting point: it says nothing about meaning, so a hand
            # typed range acquires no tag, while a hand typed mean interval
            # still has somewhere to live.
            content = MeasurementContent()
        # ``strict=False``: the cell handler has already told the user why an
        # unreadable mean was refused, and one bad cell must not discard the
        # rest of the edit.
        self._measurement_content = mcv.fold_metric_inputs(
            content, self._metric_inputs(), strict=False
        )

    def _metric_inputs(self) -> dict[str, mcv.MetricInput]:
        """The grid's current values, keyed by metric."""
        inputs: dict[str, mcv.MetricInput] = {}
        for metric in METRICS:
            # Explicit row lookup rather than METRICS' own order: a change to
            # that tuple must not silently shift a metric onto another row.
            row = self._MINMAX_ROW_BY_DIMENSION[metric]
            inputs[metric] = mcv.MetricInput(
                outer_min=self._table_value(row, 0),
                core_min=self._table_value(row, 1),
                core_max=self._table_value(row, 3),
                outer_max=self._table_value(row, 4),
                mean_text=self._mean_cell_text(row),
            )
        return inputs

    def _current_measurement_content(self) -> MeasurementContent | None:
        """The typed content matching what is on screen right now."""
        self._sync_measurement_content_from_table()
        return self._measurement_content

    def _refresh_measurement_content_view(self) -> None:
        """Repaint the tag strip, the reported-statistics line and the notice."""
        content = self._measurement_content
        tags = mcv.content_tags(content) if content is not None else []
        statistics = mcv.content_reported_statistics(content) if content is not None else []
        unsupported = (
            mcv.unsupported_details_notice(content) if content is not None else None
        )

        tag_text = mcv.compact_tag_text(tags)
        tag_explanation = mcv.explanation_text(tags)
        self.measurement_tags_label.setText(tag_text)
        self.measurement_tags_label.setToolTip(tag_explanation)
        self.measurement_tags_label.setAccessibleDescription(tag_explanation)
        self.measurement_tags_label.setVisible(bool(tag_text))

        statistics_text = mcv.compact_tag_text(statistics)
        statistics_explanation = mcv.explanation_text(statistics)
        if statistics_text:
            statistics_text = (
                QCoreApplication.translate("ReferenceAddDialog", "Also reported:")
                + " "
                + statistics_text
            )
        self.reported_statistics_label.setText(statistics_text)
        self.reported_statistics_label.setToolTip(statistics_explanation)
        self.reported_statistics_label.setAccessibleDescription(statistics_explanation)
        self.reported_statistics_label.setVisible(bool(statistics_text))

        # The notice says what saving would do to this entry, so it follows
        # the actual projection losses rather than the mere presence of a
        # tag. Two different states, and conflating them is what let a
        # percentile interval look reviewable-and-storable:
        #
        # * blocking losses -- a statistic would vanish or a percentile
        #   interval would be relabelled, so the save is refused outright
        #   and the notice must say so, not imply the numbers go through;
        # * everything else (today: the Q core pair landing in q_min/q_max)
        #   -- the save proceeds, and the notice names the one thing that
        #   changes meaning so it is not discovered later.
        blocked = blocking_projection_losses(content)
        notice = unsupported or ""
        if not notice and blocked:
            notice = (
                QCoreApplication.translate(
                    "ReferenceAddDialog",
                    "This version cannot store these reported statistics, "
                    "so this entry cannot be saved as it stands:",
                )
                + " "
                + " ".join(mcv.describe_projection_losses(blocked))
            )
        elif not notice and content is not None and not enhanced_editing_enabled():
            remaining = mcv.describe_projection_losses(
                [
                    loss
                    for loss in legacy_projection_losses(content)
                    if loss not in blocked
                ]
            )
            if remaining:
                notice = (
                    QCoreApplication.translate(
                        "ReferenceAddDialog", "Saving changes one thing:"
                    )
                    + " "
                    + " ".join(remaining)
                )
        self._reported_statistics_notice_label.setText(notice)
        self._reported_statistics_notice_label.setVisible(bool(notice))

        self._rebuild_reported_statistics_menu(content)
        self._clear_reported_statistics_btn.setVisible(
            bool(tag_text or statistics_text) and unsupported is None
        )

    def _rebuild_reported_statistics_menu(
        self, content: MeasurementContent | None
    ) -> None:
        """Offer retraction per metric, plus discarding the lot.

        Only metrics that actually carry a range interpretation get an entry,
        so the menu never invites a correction there is nothing to correct.
        """
        menu = self._reported_statistics_menu
        menu.clear()
        if content is None:
            return
        for metric in mcv.metrics_with_range_tags(content):
            action = menu.addAction(
                QCoreApplication.translate(
                    "ReferenceAddDialog", "{metric}: drop the range interpretation"
                ).format(metric=mcv.metric_label(metric))
            )
            action.setToolTip(
                QCoreApplication.translate(
                    "ReferenceAddDialog",
                    "The source does not actually state what this range "
                    "means. Removes the tag and keeps the numbers.",
                )
            )
            action.triggered.connect(
                lambda _checked=False, m=metric: self._on_drop_range_tags(m)
            )
        if not menu.isEmpty():
            menu.addSeparator()
        discard = menu.addAction(
            QCoreApplication.translate(
                "ReferenceAddDialog", "Discard all reported statistics"
            )
        )
        discard.triggered.connect(
            lambda _checked=False: self._on_clear_reported_statistics_clicked()
        )

    def _on_drop_range_tags(self, metric: str) -> None:
        content = self._current_measurement_content()
        if content is None:
            return
        self._measurement_content = mcv.without_range_tags(content, metric)
        self._refresh_measurement_content_view()
        self._set_hint(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "{metric} range interpretation dropped. The measured values "
                "are unchanged.",
            ).format(metric=mcv.metric_label(metric)),
            tone="info",
        )
        self._emit_data_changed()

    def _on_clear_reported_statistics_clicked(self) -> None:
        # Fold the table in first: otherwise the next sync would reinstate
        # whatever the visible cells still say and the tag strip would go
        # stale against the payload.
        content = self._current_measurement_content()
        if content is None:
            return
        self._measurement_content = mcv.without_reported_statistics(content)
        self._refresh_measurement_content_view()
        self._set_hint(
            QCoreApplication.translate(
                "ReferenceAddDialog",
                "Reported statistics discarded. The measured values are unchanged.",
            ),
            tone="info",
        )
        self._emit_data_changed()

    def _apply_parsed_dimension(self, row: int, dim, mean_text: str) -> None:
        """Write one parsed metric into its grid row.

        The Mean column is passed in rather than taken from ``dim`` because a
        mean is not a range endpoint: it is the source's scalar mean, or its
        reported mean interval, or (for the compact ``a-b-c`` form only) the
        printed centre. A source's *median* never reaches this column.

        ``_formatting_minmax`` suppresses the per-field handlers throughout:
        the parser's own typed content is authoritative here, and letting
        fifteen ``textChanged`` folds run mid-fill would rebuild the content
        from a half-written grid and drop the descriptors the parser just
        established.
        """
        from references.measurement_parser import DimensionRange  # local import
        assert isinstance(dim, DimensionRange)
        column_values = (dim.min, dim.p05, None, dim.p95, dim.max)
        self._formatting_minmax = True
        try:
            for col, value in enumerate(column_values):
                widget = self._measurement_input(row, col)
                if col == self._MEAN_COLUMN:
                    widget.setText(mean_text)
                elif value is None:
                    widget.setText("")
                else:
                    widget.setText(f"{float(value):.2f}")
        finally:
            self._formatting_minmax = False

    def _parsed_mean_text(self, content: MeasurementContent, metric: str, dim) -> str:
        """Mean-cell text for one parsed metric.

        Priority: the typed content's mean (a scalar mean column, or a
        reported mean interval), then the legacy centre of an ``a-b-c``
        compact range, which has no typed home and stays a legacy value.
        """
        text = mcv.format_mean_cell(content, metric)
        if text:
            return text
        return mcv.format_number(dim.p50)

    def _set_parsed_result(self, result) -> None:
        # The typed parser output is the editor's working state from here on:
        # tags, reported statistics and the mean's shape all live in it, and
        # the table holds the same numbers in their legacy column layout.
        content = result.to_content()
        self._measurement_content = content
        for metric, dim in (
            ("length", result.length), ("width", result.width), ("q", result.q)
        ):
            self._apply_parsed_dimension(
                self._MINMAX_ROW_BY_DIMENSION[metric],
                dim,
                self._parsed_mean_text(content, metric, dim),
            )
        # A reported Q mean is generic evidence from the source; the Parmasto
        # tab holds species means the user enters deliberately. Routing the
        # parser through it would make a single publication's Qm look like a
        # Parmasto biometric, so the value goes to the Q row's Mean cell.
        self._render_measurement_preview(result)
        self._refresh_measurement_content_view()
        self._emit_data_changed()

    _MEASUREMENT_PREVIEW_NOISE_PREFIXES = (
        "Parsed first range as length.",
        "Q not present in source.",
        "Q: no extreme values found.",
        "Length: no extreme values found.",
        "Width: no extreme values found.",
        "Length: no centre/mean value found.",
        "Width: no centre/mean value found.",
        # This parser diagnostic describes its legacy projection. Full
        # reported content is now saved and shown by this editor.
        "Table mean, median and S.D. captured as reported statistics; they are "
        "not applied to the legacy editor fields.",
    )

    def _filter_preview_warnings(self, warnings: list[str]) -> list[str]:
        actionable: list[str] = []
        for warning in warnings:
            text = (warning or "").strip()
            if not text:
                continue
            if text in self._MEASUREMENT_PREVIEW_NOISE_PREFIXES:
                continue
            actionable.append(text)
        return actionable

    @staticmethod
    def _format_dimension_range(dim) -> str:
        parts: list[str] = []
        if dim.min is not None:
            parts.append(f"({dim.min:g}–)")
        inner: list[str] = []
        if dim.p05 is not None:
            inner.append(f"{dim.p05:g}")
        if dim.p50 is not None:
            inner.append(f"{dim.p50:g}")
        if dim.p95 is not None:
            inner.append(f"{dim.p95:g}")
        parts.append("–".join(inner) if inner else "—")
        if dim.max is not None:
            parts.append(f"(–{dim.max:g})")
        return "".join(parts)

    def _render_measurement_preview(self, result) -> None:
        actionable_warnings = self._filter_preview_warnings(result.warnings)

        if not result.ok:
            warnings_html = "".join(
                f"<div style='color:#b58900;'>• {w}</div>" for w in actionable_warnings
            )
            self._measurement_preview_label.setText(
                "<i>" + QCoreApplication.translate("ReferenceAddDialog", "Nothing parsed.") + "</i>" + warnings_html
            )
            return

        segments: list[str] = []
        if not result.length.is_empty():
            segments.append("L " + self._format_dimension_range(result.length))
        if not result.width.is_empty():
            segments.append("W " + self._format_dimension_range(result.width))
        if not result.q.is_empty():
            segments.append("Q " + self._format_dimension_range(result.q))
        if result.q_mean is not None:
            segments.append(f"Qm {result.q_mean:g}")
        if result.n is not None:
            segments.append(f"n={result.n}")

        prefix = QCoreApplication.translate("ReferenceAddDialog", "Parsed:") + " "
        body = prefix + " · ".join(segments) if segments else ""

        warnings_html = "".join(
            f"<div style='color:#b58900;'>• {w}</div>" for w in actionable_warnings
        )
        self._measurement_preview_label.setText(body + warnings_html)

    def _on_parse_measurement_clicked(self) -> None:
        raw = self.measurement_paste_input.text()
        if not (raw or "").strip():
            self._measurement_preview_label.setText(
                "<i>" + QCoreApplication.translate("ReferenceAddDialog", "Paste a measurement string first.") + "</i>"
            )
            return
        from references.measurement_parser import parse_measurement_string
        result = parse_measurement_string(raw)
        if not result.ok:
            self._render_measurement_preview(result)
            self._set_hint(QCoreApplication.translate("ReferenceAddDialog", "Parsing failed — manual entry preserved."), tone="warning")
            return
        self._raw_measurement_text = raw
        self._set_parsed_result(result)
        self.set_grid_revealed(True)
        self._set_hint(QCoreApplication.translate("ReferenceAddDialog", "Parsed — review and edit before saving."), tone="info")

    # ----- Two steps (staged) ------------------------------------------------

    def is_staged(self) -> bool:
        return self._staged

    def show_step(self, index: int) -> None:
        """Staged only: 0 is the publication, 1 the measurements."""
        if self.steps is None:
            return
        self.steps.setCurrentIndex(max(0, min(int(index), self.steps.count() - 1)))
        if self.steps.currentIndex() == 1 and not self.is_grid_revealed():
            self.measurement_paste_input.setFocus(Qt.OtherFocusReason)

    def current_step(self) -> int:
        return self.steps.currentIndex() if self.steps is not None else 0

    def is_grid_revealed(self) -> bool:
        return not self.measurement_grid_panel.isHidden()

    def set_grid_revealed(self, revealed: bool) -> None:
        """Show or hide the value grid and its corrections (staged: hidden until needed)."""
        revealed = bool(revealed) or not self._staged
        self.measurement_grid_panel.setVisible(revealed)
        self._correction_row_widget.setVisible(revealed)
        self.fill_manually_btn.setVisible(self._staged and not revealed)

    def _on_fill_manually_clicked(self) -> None:
        self.set_grid_revealed(True)
        self._measurement_input(0, 0).setFocus(Qt.OtherFocusReason)
        self.data_changed.emit()

    def has_publication(self) -> bool:
        """A library publication is chosen, or a new one is waiting to be saved."""
        return bool(self._selected_work_id or self._pending_reference_work is not None)

    def bind_typed_publication(self) -> str | None:
        """Select the work whose exact label was typed (see ``_resolve_typed_publication``)."""
        return self._resolve_typed_publication()

    def library_payload_ready(self) -> bool:
        """Whether saving would store a measurement set: the payload builder's own answer.

        A refusal (content this version cannot store) counts as ready, so the
        click reaches validation, which explains it.
        """
        try:
            return self.normalized_measurement_set_payload() is not None
        except MeasurementContentError:
            return True
        except Exception:
            return False

    def _update_existing_sets_note(self) -> None:
        count = len(self._existing_sets_cache)
        note = getattr(self, "existing_sets_note", None)
        if note is None:
            return
        note.setVisible(self._staged and count > 0)
        if count:
            note.setText(
                QCoreApplication.translate(
                    "ReferenceAddDialog",
                    "Measurement sets for this taxon already in this publication: "
                    "{count}. To use one, tick it in the Add reference list.",
                ).format(count=count)
            )

    def _on_swap_lw_clicked(self) -> None:
        row_a, row_b = 0, 1
        self._formatting_minmax = True
        try:
            for col in range(5):
                field_a = self._measurement_input(row_a, col)
                field_b = self._measurement_input(row_b, col)
                text_a = field_a.text()
                text_b = field_b.text()
                field_a.setText(text_b)
                field_b.setText(text_a)
        finally:
            self._formatting_minmax = False
        # The tags and reported statistics must travel with their numbers:
        # a "5%-95%" or "extremes reported" tag left behind would describe the
        # other dimension's values after the swap.
        content = self._measurement_content
        if content is not None:
            try:
                self._measurement_content = swap_content_length_width(content)
            except MeasurementContentError:
                # Unsupported future details cannot be transformed; leave the
                # stored object untouched rather than desynchronising it.
                pass
        self._sync_measurement_content_from_table()
        self._refresh_measurement_content_view()
        self._set_hint(QCoreApplication.translate("ReferenceAddDialog", "Length and width swapped."), tone="info")
        self._emit_data_changed()

    def _parmasto_value(self, key: str):
        widget = self.parmasto_inputs.get(key)
        if widget is None:
            return None
        text = widget.text().strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _current_source_label(self) -> str | None:
        work_id = self._selected_work_id
        if work_id:
            for row in range(self.publication_combo.count()):
                if str(self.publication_combo.itemData(row) or "") == work_id:
                    text = self.publication_combo.itemText(row).strip()
                    if text:
                        return text
                    break
        if self._legacy_source_prefill:
            return self._legacy_source_prefill
        typed = self.publication_combo.currentText().strip()
        return typed or None

    def _reference_record_data(self):
        data = {
            "genus": self._genus,
            "species": self._species,
            "source": self._current_source_label(),
            "plot_color": self._plot_color,
            "parmasto_length_mean": self._parmasto_value("parmasto_length_mean"),
            "parmasto_width_mean": self._parmasto_value("parmasto_width_mean"),
            "parmasto_q_mean": self._parmasto_value("parmasto_q_mean"),
            "parmasto_v_sp_length": self._parmasto_value("parmasto_v_sp_length"),
            "parmasto_v_sp_width": self._parmasto_value("parmasto_v_sp_width"),
            "parmasto_v_sp_q": self._parmasto_value("parmasto_v_sp_q"),
            "parmasto_v_ind_length": self._parmasto_value("parmasto_v_ind_length"),
            "parmasto_v_ind_width": self._parmasto_value("parmasto_v_ind_width"),
            "parmasto_v_ind_q": self._parmasto_value("parmasto_v_ind_q"),
            "length_min": self._table_value(0, 0),
            "length_p05": self._table_value(0, 1),
            "length_p50": self._table_value(0, 2),
            "length_p95": self._table_value(0, 3),
            "length_max": self._table_value(0, 4),
            "width_min": self._table_value(1, 0),
            "width_p05": self._table_value(1, 1),
            "width_p50": self._table_value(1, 2),
            "width_p95": self._table_value(1, 3),
            "width_max": self._table_value(1, 4),
            "q_min": self._table_value(2, 0),
            "q_p05": self._table_value(2, 1),
            "q_p50": self._table_value(2, 2),
            "q_p95": self._table_value(2, 3),
            "q_max": self._table_value(2, 4),
            "length_avg": self._prefill_data.get("length_avg") if self._prefill_data else None,
            "width_avg": self._prefill_data.get("width_avg") if self._prefill_data else None,
            "q_avg": self._prefill_data.get("q_avg") if self._prefill_data else None,
        }
        has_values = any(
            data.get(key) is not None
            for key in (
                "length_min", "length_p05", "length_p50", "length_p95", "length_max",
                "width_min", "width_p05", "width_p50", "width_p95", "width_max",
                "q_min", "q_p05", "q_p50", "q_p95", "q_max",
                "parmasto_length_mean", "parmasto_width_mean", "parmasto_q_mean",
                "parmasto_v_sp_length", "parmasto_v_sp_width", "parmasto_v_sp_q",
                "parmasto_v_ind_length", "parmasto_v_ind_width", "parmasto_v_ind_q",
            )
        )
        if not has_values:
            return None
        data["source_kind"] = "reference"
        existing_meta = self._prefill_data.get("metadata_json") if self._prefill_data else None
        meta_dict = dict(existing_meta) if isinstance(existing_meta, dict) else {}
        raw_text = (self._raw_measurement_text or "").strip()
        if raw_text:
            meta_dict["raw_measurement"] = raw_text
        elif "raw_measurement" in meta_dict:
            meta_dict.pop("raw_measurement", None)
        if meta_dict:
            data["metadata_json"] = meta_dict
        return data

    def _points_data(self):
        points = self.spore_table.get_points()
        if not points:
            return None
        source_label = (self._current_source_label() or "").strip()
        if not source_label:
            source_label = QCoreApplication.translate("ReferenceAddDialog", "Reference points")
        return {
            "genus": self._genus,
            "species": self._species,
            "points": points,
            "points_label": source_label,
            "plot_color": self._plot_color,
            "source_kind": "points",
            "source_type": "custom",
        }

    def is_ready_to_submit(self) -> bool:
        """Cheap, non-mutating readiness check for a host footer button.

        Mirrors :meth:`validate_and_build_result`'s branching without
        popping any confirmation dialogs.
        """
        if self.use_existing_radio.isChecked():
            return bool(self._selected_measurement_set_id)
        if self._entering_individual_points():
            return bool(self._points_data())
        if self._blocked_projection_losses():
            return False
        return bool(self._reference_record_data())

    def _has_grid_values(self) -> bool:
        return any(
            self._table_value(row, col) is not None
            for row in range(3)
            for col in range(5)
        )

    def has_storable_measurement_content(self) -> bool:
        """Whether the normalized library has anything to store from this form.

        The same precondition :meth:`normalized_measurement_set_payload`
        applies before it will build anything: a measurement range in the
        grid, or individual spore points. It is deliberately narrower than
        :meth:`is_ready_to_submit`, which also accepts a Parmasto-only
        entry — those biometrics ride along on the legacy reference row and
        have no normalized measurement set of their own, so offering to
        "save" one to the library would store nothing.

        Exposed so a host can disable a save action up front instead of
        letting the user press it and get silence.
        """
        return bool(self._build_raw_points_json()) or self._has_grid_values()

    def _blocked_projection_losses(self) -> list[tuple[str, str]]:
        """Scientific claims this entry cannot be saved without losing.

        Only meaningful for the range path: individual spore points carry no
        descriptors, and an existing measurement set is already stored.
        """
        return blocking_projection_losses(self._current_measurement_content())

    def _entering_individual_points(self) -> bool:
        """Whether this entry is a set of individual spore measurements.

        The old editor asked which measurement *tab* was in front. The one
        column layout has no tab to ask, so the question is answered by the
        data itself: usable individual points outrank a range, because they
        are the stronger evidence and a range typed beside them would be a
        summary of the same spores rather than a second source.
        """
        return bool(self.spore_table.get_points())

    def validate_and_build_result(self) -> bool:
        """Validate the current form and, on success, populate
        :meth:`result_data`. Returns ``True`` on success; shows a warning
        and returns ``False`` on invalid/incomplete input, or when the
        user declines an explicit legacy-only confirmation.
        """
        self._clear_invalid()
        if self._resolve_typed_publication() == "ambiguous":
            self.explain_ambiguous_publication()
            return False
        if self.use_existing_radio.isChecked():
            if not self._selected_measurement_set_id:
                self._show_invalid(
                    self.data_validation_label,
                    self._existing_sets_table,
                    QCoreApplication.translate("ReferenceAddDialog", "Select an existing measurement set or switch to \"Enter new data\"."),
                )
                return False
            self._result = {
                "genus": self._genus,
                "species": self._species,
                "source": self._current_source_label(),
                "plot_color": self._plot_color,
                "source_kind": "existing_measurement_set",
                "reference_measurement_set_id": self._selected_measurement_set_id,
                "reference_work_id": self._selected_work_id,
            }
            return True
        if self._entering_individual_points():
            data = self._points_data()
            if not data:
                self._show_invalid(
                    self.points_validation_label,
                    self.spore_table,
                    QCoreApplication.translate("ReferenceAddDialog", "Enter at least one length and width value."),
                )
                return False
        else:
            blocked = self._blocked_projection_losses()
            if blocked:
                # Refused here rather than at attach time, and refused rather
                # than silently downgraded: the preview beside this form is
                # showing the source's percentile interval or reported mean,
                # and saving a row that no longer says those things would
                # make the picture and the evidence disagree.
                self._show_invalid(
                    self.measurement_validation_label,
                    self._clear_reported_statistics_btn,
                    QCoreApplication.translate(
                        "ReferenceAddDialog",
                        "This version cannot store everything this source "
                        "reports, and saving it would change what the entry "
                        "claims:",
                    )
                    + "\n\n• "
                    + "\n• ".join(mcv.describe_projection_losses(blocked))
                    + "\n\n"
                    + QCoreApplication.translate(
                        "ReferenceAddDialog",
                        "Remove what this entry cannot carry and save the "
                        "measured values on their own: "
                        "\u201cCorrect interpretation\u201d drops a range "
                        "interpretation, a median and a standard deviation, "
                        "and a mean reported as an interval is cleared by "
                        "emptying its Mean field.",
                    ),
                )
                return False
            data = self._reference_record_data()
            if not data:
                self._show_invalid(
                    self.measurement_validation_label,
                    self._measurement_input(0, 1)
                    if self.is_grid_revealed()
                    else self.measurement_paste_input,
                    QCoreApplication.translate("ReferenceAddDialog", "Enter at least one reference or Parmasto value."),
                )
                return False
        data["reference_work_id"] = self._selected_work_id
        data["observation_id"] = self._observation_id
        data["sporely_taxon_id"] = self._sporely_taxon_id
        data["observation_taxon_id"] = self._observation_taxon_id
        if (
            self._sporely_taxon_id
            and self._observation_id
            and not self._selected_work_id
            and self._pending_reference_work is None
            and self._new_data_is_normalizable()
        ):
            answer = QMessageBox.question(
                self,
                QCoreApplication.translate("ReferenceAddDialog", "No publication selected"),
                QCoreApplication.translate("ReferenceAddDialog", 
                    "No publication is selected. Save as a legacy-only "
                    "reference (no library entry, no observation attachment)?"
                ),
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if answer != QMessageBox.Yes:
                return False
        self._result = data
        return True

    def _new_data_is_normalizable(self) -> bool:
        if self._build_raw_points_json():
            return True
        tentative = {
            "length_min": self._table_value(0, 0),
            "length_core_min": self._table_value(0, 1),
            "length_mean": self._table_value(0, 2),
            "length_core_max": self._table_value(0, 3),
            "length_max": self._table_value(0, 4),
            "width_min": self._table_value(1, 0),
            "width_core_min": self._table_value(1, 1),
            "width_mean": self._table_value(1, 2),
            "width_core_max": self._table_value(1, 3),
            "width_max": self._table_value(1, 4),
        }
        return range_payload_is_plottable(tentative)

    def selected_reference_work_id(self) -> str | None:
        return self._selected_work_id

    def pending_reference_work(self) -> ReferenceWork | None:
        return self._pending_reference_work

    def quick_add_treatment_payload(self) -> dict[str, str]:
        name_as_published = self.name_as_published_input.text().strip()
        if not name_as_published:
            # "Name as published" is a required field on the treatment
            # (TaxonTreatmentRepository._validate), so leaving it empty used
            # to fail the save outright with a raw validation message. The
            # overwhelmingly common case is that the publication uses the
            # same name as the normalized taxon, so fall back to that rather
            # than blocking the save. A user recording an old synonym,
            # spelling variant, or historical combination still types it in
            # and that value is used verbatim.
            name_as_published = " ".join(
                part for part in (self._genus, self._species) if part
            ).strip()
        return {
            "name_as_published": name_as_published,
            "locator_text": self.locator_input.text().strip(),
        }

    def selected_measurement_set_id(self) -> str | None:
        return self._selected_measurement_set_id

    def is_use_existing_set(self) -> bool:
        return bool(self.use_existing_radio.isChecked())

    def result_data(self):
        return self._result

    # ----- Publication picker helpers -------------------------------------

    def _populate_publication_combo(self, select_id: str | None = None) -> None:
        try:
            works = ReferenceWorkRepository.list_recent(limit=500)
        except Exception:
            works = []
        blocker = QSignalBlocker(self.publication_combo)
        try:
            self.publication_combo.clear()
            self.publication_combo.addItem("", None)
            self._publication_search_seen_ids = set()
            for work in works:
                self._append_publication_row(work)
        finally:
            del blocker
        self._ensure_publication_completer()
        if select_id:
            target_row = -1
            for row in range(self.publication_combo.count()):
                if str(self.publication_combo.itemData(row) or "") == select_id:
                    target_row = row
                    break
            if target_row >= 0:
                self.publication_combo.setCurrentIndex(target_row)
                return
        self.publication_combo.setCurrentIndex(0)
        self._selected_work_id = None

    def _append_publication_row(self, work: ReferenceWork) -> None:
        work_id = str(getattr(work, "id", "") or "")
        if not work_id or work_id in self._publication_search_seen_ids:
            return
        self._publication_search_seen_ids.add(work_id)
        display_label = self._format_publication_display_label(work)
        self.publication_combo.addItem(display_label, work.id)
        row = self.publication_combo.count() - 1
        search_text = self._format_publication_search_text(work)
        index = self.publication_combo.model().index(row, 0)
        self.publication_combo.model().setData(index, search_text, Qt.UserRole + 1)

    def _on_publication_edit_text_changed(self, text: str) -> None:
        current = (text or "").strip()
        if (
            self._pending_reference_work is not None
            and current != (self._pending_reference_work_label or "")
        ):
            self._pending_reference_work = None
            self._pending_reference_work_label = None
        # Picking another publication changes the text before Qt emits
        # currentIndexChanged; that is a selection, not typing over one.
        picked_row = self.publication_combo.currentIndex()
        picking = (
            picked_row > 0
            and bool(self.publication_combo.itemData(picked_row))
            and self.publication_combo.itemText(picked_row).strip() == current
        )
        if self._selected_work_id is not None and not picking:
            for row in range(self.publication_combo.count()):
                if str(self.publication_combo.itemData(row) or "") == self._selected_work_id:
                    if self.publication_combo.itemText(row).strip() != current:
                        self._selected_work_id = None
                        blocker = QSignalBlocker(self.publication_combo)
                        try:
                            self.publication_combo.setCurrentIndex(0)
                            self.publication_combo.setEditText(current)
                        finally:
                            del blocker
                        try:
                            self._refresh_existing_sets_cache()
                        except Exception:
                            pass
                    break
        query = current
        if len(query) < 2:
            return
        try:
            matches = ReferenceWorkRepository.search(query=query, limit=50)
        except Exception:
            return
        added = False
        blocker = QSignalBlocker(self.publication_combo)
        try:
            for work in matches:
                work_id = str(getattr(work, "id", "") or "")
                if not work_id or work_id in self._publication_search_seen_ids:
                    continue
                self._append_publication_row(work)
                added = True
        finally:
            del blocker
        if added:
            completer = getattr(self, "_publication_completer", None)
            if completer is not None:
                completer.complete()

    def _ensure_publication_completer(self) -> None:
        existing = getattr(self, "_publication_completer", None)
        if existing is not None:
            return
        proxy = _PublicationSearchProxyModel(self)
        proxy.setSourceModel(self.publication_combo.model())
        completer = QCompleter(proxy, self)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        completer.setCompletionColumn(0)
        self.publication_combo.setCompleter(completer)
        self._publication_completer = completer
        self._publication_search_proxy = proxy

    @staticmethod
    def _format_publication_display_label(work: ReferenceWork) -> str:
        base = (work.short_label or work.title or "").strip()
        year = getattr(work, "year", None)
        if year and str(year).strip():
            return f"{base} ({year})" if base else str(year)
        return base or (work.id or "")

    @staticmethod
    def _format_publication_search_text(work: ReferenceWork) -> str:
        parts: list[str] = []
        title = (getattr(work, "title", None) or "").strip()
        if title:
            parts.append(title)
        short_label = (getattr(work, "short_label", None) or "").strip()
        if short_label:
            parts.append(short_label)
        container = (getattr(work, "container_title", None) or "").strip()
        if container:
            parts.append(container)
        authors_json = getattr(work, "authors_json", None)
        if authors_json:
            try:
                import json as _json

                authors = _json.loads(authors_json)
            except Exception:
                authors = None
            if isinstance(authors, list):
                for entry in authors:
                    if not isinstance(entry, dict):
                        continue
                    given = str(entry.get("given") or "").strip()
                    family = str(entry.get("family") or "").strip()
                    full = " ".join(p for p in (given, family) if p)
                    if full:
                        parts.append(full)
        citation_key = (getattr(work, "citation_key", None) or "").strip()
        if citation_key:
            parts.append(citation_key)
        year = getattr(work, "year", None)
        if year and str(year).strip():
            parts.append(str(year).strip())
        return " ".join(parts)

    def _resolve_typed_publication(self) -> str | None:
        """Bind typed publication text that exactly names a library work.

        The publication combo is editable: typing (or completing) the exact
        label of an existing work leaves the text in the box without
        selecting that row, so no work id was recorded and the entry fell
        back to a legacy-only, unnormalized reference (plotted through the
        legacy shape path). An exact, unambiguous label match selects the
        work, so the entry goes through the normalized library path.
        Returns ``"bound"``, ``"ambiguous"`` (several works carry that
        label; the user must pick one) or ``None``.
        """
        if self._selected_work_id or self._pending_reference_work is not None:
            return None
        typed = " ".join((self.publication_combo.currentText() or "").split()).casefold()
        if not typed:
            return None
        matches = [
            row for row in range(self.publication_combo.count())
            if self.publication_combo.itemData(row)
            and " ".join(self.publication_combo.itemText(row).split()).casefold() == typed
        ]
        if len(matches) == 1:
            self.publication_combo.setCurrentIndex(matches[0])
            self._on_publication_selected(matches[0])
            return "bound"
        if len(matches) > 1:
            return "ambiguous"
        return None

    def _on_publication_selected(self, _index: int) -> None:
        data = self.publication_combo.currentData()
        self._selected_work_id = str(data) if data else None
        if self._selected_work_id:
            self._pending_reference_work = None
            self._pending_reference_work_label = None
        self._refresh_existing_sets_cache()

    def _on_new_publication_clicked(self) -> None:
        try:
            from .reference_library_manager_dialog import ReferenceWorkEditor
        except Exception as exc:
            self._hint_controller.show_notice(
                QCoreApplication.translate("ReferenceAddDialog", "Could not open the publication editor."),
                "error",
                details=str(exc),
            )
            return
        editor = ReferenceWorkEditor(self, persist_on_accept=False)
        try:
            if editor.exec() == QDialog.Accepted and editor.result_work is not None:
                self._pending_reference_work = editor.result_work
                self._selected_work_id = None
                label = (
                    editor.result_work.short_label
                    or editor.result_work.title
                    or QCoreApplication.translate("ReferenceAddDialog", "Untitled reference")
                )
                if editor.result_work.year:
                    label = f"{label} ({editor.result_work.year})"
                self._pending_reference_work_label = label
                self.publication_combo.blockSignals(True)
                self.publication_combo.setEditText(label)
                self.publication_combo.blockSignals(False)
                self._refresh_existing_sets_cache()
        finally:
            editor.deleteLater()

    # ----- Existing-set table helpers -------------------------------------

    def _refresh_existing_sets_cache(self) -> None:
        self._existing_sets_cache = []
        candidates: list[MeasurementSet] = []
        work_id = self._selected_work_id
        taxon_id = self._sporely_taxon_id
        if work_id and taxon_id:
            try:
                treatments = TaxonTreatmentRepository.list_for_work(work_id)
            except Exception:
                treatments = []
            taxon_key = str(int(taxon_id))
            matching_treatments = [
                t for t in treatments
                if str(getattr(t, "taxon_id", "") or "") == taxon_key
            ]
            for treatment in matching_treatments:
                try:
                    sets = MeasurementSetRepository.list_for_treatment(treatment.id)
                except Exception:
                    sets = []
                for ms in sets:
                    if ms.data_kind in SUPPORTED_ATTACHMENT_DATA_KINDS:
                        candidates.append(ms)
        self._existing_sets_cache = candidates
        self._update_existing_sets_note()
        has_candidates = bool(candidates) and not self._staged
        self.use_existing_radio.setEnabled(has_candidates)
        if not has_candidates and self.use_existing_radio.isChecked():
            self.enter_new_radio.setChecked(True)
        self._refresh_existing_sets_table()

    def _refresh_existing_sets_table(self) -> None:
        query_raw = self._existing_search_input.text() if hasattr(self, "_existing_search_input") else ""
        query = (query_raw or "").strip().casefold()

        locator_by_set: dict[str, str] = {}
        for ms in self._existing_sets_cache:
            try:
                treatment = TaxonTreatmentRepository.get(ms.taxon_treatment_id)
            except Exception:
                treatment = None
            if treatment is not None and treatment.locator_text:
                locator_by_set[ms.id] = str(treatment.locator_text)

        def _match(ms: MeasurementSet) -> bool:
            if not query:
                return True
            for value in (
                ms.raw_text,
                getattr(ms, "notes", None),
                ms.data_kind,
                locator_by_set.get(ms.id),
            ):
                if value is None:
                    continue
                if query in str(value).casefold():
                    return True
            return False

        visible = [ms for ms in self._existing_sets_cache if _match(ms)]
        self._existing_sets_table.setRowCount(0)
        for ms in visible:
            row = self._existing_sets_table.rowCount()
            self._existing_sets_table.insertRow(row)
            locator = locator_by_set.get(ms.id, "")
            locator_item = QTableWidgetItem(locator)
            locator_item.setData(Qt.UserRole, ms.id)
            locator_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self._existing_sets_table.setItem(row, 0, locator_item)
            kind_item = QTableWidgetItem(ms.data_kind or "")
            kind_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self._existing_sets_table.setItem(row, 1, kind_item)
            raw_item = QTableWidgetItem(ms.raw_text or "")
            raw_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            self._existing_sets_table.setItem(row, 2, raw_item)
        target_id = self._selected_measurement_set_id
        if target_id:
            for row in range(self._existing_sets_table.rowCount()):
                item = self._existing_sets_table.item(row, 0)
                if item and str(item.data(Qt.UserRole) or "") == target_id:
                    self._existing_sets_table.selectRow(row)
                    break
            else:
                self._selected_measurement_set_id = None
        self._emit_data_changed()

    def _on_existing_set_selection_changed(self) -> None:
        selected_rows = self._existing_sets_table.selectionModel().selectedRows()
        if not selected_rows:
            self._selected_measurement_set_id = None
            self._emit_data_changed()
            return
        row = selected_rows[0].row()
        item = self._existing_sets_table.item(row, 0)
        self._selected_measurement_set_id = (
            str(item.data(Qt.UserRole)) if item else None
        )
        self._emit_data_changed()

    def _on_data_choice_toggled(self, *_args) -> None:
        use_existing = self.use_existing_radio.isChecked()
        self._existing_search_input.setVisible(use_existing)
        self._existing_sets_table.setVisible(use_existing)
        for section in (
            self._measurement_group,
            self._spore_section,
            self._parmasto_section,
        ):
            section.setEnabled(not use_existing)
        if use_existing:
            self._refresh_existing_sets_table()
        self._emit_data_changed()

    def _build_raw_points_json(self) -> str | None:
        points = self.spore_table.get_points()
        if not points:
            return None
        translated = []
        for point in points:
            length = point.get("length_um")
            width = point.get("width_um")
            if length is None or width is None:
                continue
            length_f = float(length)
            width_f = float(width)
            if not (math.isfinite(length_f) and math.isfinite(width_f)):
                continue
            if length_f <= 0 or width_f <= 0:
                continue
            translated.append({"length": length_f, "width": width_f})
        if not translated:
            return None
        return json.dumps(translated, ensure_ascii=False)

    def normalized_measurement_set_payload(
        self, *, legacy_reference_value_id: int | None = None
    ) -> MeasurementSet | None:
        raw_points_json = self._build_raw_points_json()
        if not raw_points_json and not self._has_grid_values():
            return None
        if raw_points_json:
            data_kind = "raw_points"
        else:
            data_kind = "range"
        sample_size: int | None = None
        if raw_points_json:
            try:
                sample_size = len(json.loads(raw_points_json))
            except Exception:
                sample_size = None
        raw_text_input = self._raw_measurement_text or ""
        if not raw_text_input and hasattr(self, "measurement_paste_input"):
            raw_text_input = self.measurement_paste_input.text() or ""
        raw_text_input = raw_text_input or None
        prefill = self._prefill_data or {}
        mount_medium = (str(prefill.get("mount_medium") or "").strip() or None)
        stain = (str(prefill.get("stain") or "").strip() or None)
        notes = None
        legacy_meta = prefill.get("metadata_json") if isinstance(prefill, dict) else None
        if isinstance(legacy_meta, dict):
            note_candidate = legacy_meta.get("notes")
            if isinstance(note_candidate, str) and note_candidate.strip():
                notes = note_candidate.strip()
        # Typed content matching the table as it stands. It carries the
        # meaning tags, the reported median/S.D. and the mean's shape; the
        # ordinary columns below are still read from the table so the legacy
        # mapping is unchanged for every source that says nothing extra.
        content = self._current_measurement_content()
        # Stage 3's semantic boundary. While the reader gate is closed this
        # row would be written as a version-1 projection, and for some
        # content that projection says something different from the source
        # the user is looking at. Refuse rather than freeze it: raising
        # (not returning ``None``) is what the quick-add path in
        # ``ui/main_window.py`` already treats as "an intended normalized
        # attempt", so a refusal here cannot fall through and persist a
        # legacy-only row instead -- which would be the degraded write this
        # check exists to prevent.
        blocked = blocking_projection_losses(content)
        if blocked:
            raise MeasurementContentError(
                " ".join(mcv.describe_projection_losses(blocked))
            )
        store_reported = (
            content is not None
            and mcv.has_reported_content(content)
            and enhanced_editing_enabled()
        )
        typed_q_core = (
            content.q_core_min is not None and content.q_core_max is not None
            if content is not None
            else False
        )
        if store_reported and typed_q_core:
            # The Q core pair has a column of its own now, so q_min/q_max
            # carry only genuine extremes. Falling back to the typical bounds
            # here would write a derived extreme next to a core pair that
            # already holds the same numbers under a different meaning.
            q_min = self._table_value(2, 0)
            q_max = self._table_value(2, 4)
        else:
            q_min = self._table_value(2, 0)
            if q_min is None:
                q_min = self._table_value(2, 1)
            q_max = self._table_value(2, 4)
            if q_max is None:
                q_max = self._table_value(2, 3)
        q_mean = self._table_value(2, 2)
        if q_mean is None:
            q_mean = self._parmasto_value("parmasto_q_mean")
        ms = MeasurementSet(
            id="",
            taxon_treatment_id="",
            character="spore_size",
            data_kind=data_kind,
            raw_text=raw_text_input,
            length_min=self._table_value(0, 0),
            length_core_min=self._table_value(0, 1),
            length_core_max=self._table_value(0, 3),
            length_max=self._table_value(0, 4),
            width_min=self._table_value(1, 0),
            width_core_min=self._table_value(1, 1),
            width_core_max=self._table_value(1, 3),
            width_max=self._table_value(1, 4),
            q_min=q_min,
            q_max=q_max,
            q_mean=q_mean,
            length_mean=self._table_value(0, 2),
            width_mean=self._table_value(1, 2),
            sample_size=sample_size,
            raw_points_json=raw_points_json,
            legacy_reference_value_id=legacy_reference_value_id,
            mount_medium=mount_medium,
            stain=stain,
            notes=notes,
        )
        if store_reported:
            ms.measurement_details_json = encode_measurement_details(content.details)
            ms.q_core_min = content.q_core_min
            ms.q_core_max = content.q_core_max
        if data_kind == "range" and not range_payload_is_plottable(ms):
            return None
        return ms

    def _set_plot_color(self, color: str | None) -> None:
        self._plot_color = str(color).strip().lower() if color else None

    def _apply_prefill(self):
        data = self._prefill_data
        if not data:
            self._set_plot_color(None)
            return
        source_prefill = (
            data.get("source")
            or data.get("points_label")
            or data.get("source_label")
            or ""
        )
        if source_prefill:
            matched_id: str | None = None
            for row in range(self.publication_combo.count()):
                label = self.publication_combo.itemText(row).strip()
                if label and label.casefold() == source_prefill.strip().casefold():
                    matched_id = str(self.publication_combo.itemData(row) or "") or None
                    break
            if matched_id and not self._require_explicit_publication_assignment:
                self._populate_publication_combo(select_id=matched_id)
            else:
                self._legacy_source_prefill = source_prefill
                self.publication_combo.setEditText(source_prefill)

        def _set_cell(row, col, value):
            if value is None:
                return
            self._measurement_input(row, col).setText(f"{value:.2f}")

        _set_cell(0, 0, data.get("length_min"))
        _set_cell(0, 1, data.get("length_p05"))
        _set_cell(0, 2, data.get("length_p50"))
        _set_cell(0, 3, data.get("length_p95"))
        _set_cell(0, 4, data.get("length_max"))
        _set_cell(1, 0, data.get("width_min"))
        _set_cell(1, 1, data.get("width_p05"))
        _set_cell(1, 2, data.get("width_p50"))
        _set_cell(1, 3, data.get("width_p95"))
        _set_cell(1, 4, data.get("width_max"))
        _set_cell(2, 0, data.get("q_min"))
        _set_cell(2, 1, data.get("q_p05"))
        _set_cell(2, 2, data.get("q_p50"))
        _set_cell(2, 3, data.get("q_p95"))
        _set_cell(2, 4, data.get("q_max"))

        points = data.get("points") or []
        if points:
            self.spore_table._ensure_rows(len(points))
            for row, point in enumerate(points):
                length = point.get("length_um")
                width = point.get("width_um")
                if length is not None:
                    self.spore_table.setItem(row, 0, QTableWidgetItem(f"{length:g}"))
                if width is not None:
                    self.spore_table.setItem(row, 1, QTableWidgetItem(f"{width:g}"))
                self.spore_table._update_q_for_row(row)
            # Reopened data must be visible, not hidden behind a collapsed
            # disclosure the user has no reason to suspect holds their rows.
            self._spore_section.set_expanded(True)
        parmasto_has_values = False
        for key, widget in self.parmasto_inputs.items():
            value = data.get(key)
            if value is None:
                widget.clear()
                continue
            widget.setText(f"{float(value):g}")
            parmasto_has_values = True
        if parmasto_has_values:
            self._parmasto_section.set_expanded(True)
        self._set_plot_color(data.get("plot_color"))


__all__ = ["ReferenceEntryEditor", "SporeDataTable"]
