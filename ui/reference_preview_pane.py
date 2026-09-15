"""Reusable community-dataset review pane (Summary / Raw spores / Method / Calibration / Provenance)."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFormLayout,
    QFrame,
    QHeaderView,
    QLabel,
    QPlainTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)


class ReferencePreviewPane(QWidget):
    """Tabbed review pane for a community spore dataset.

    Owns the QTabWidget and all sub-tab widgets. Callers populate it
    through the public API below; all tr() calls live here so strings
    are attributed to this class's context.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._build_ui()

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self.review_tabs = QTabWidget()
        root.addWidget(self.review_tabs, 1)

        # --- Summary tab ----------------------------------------------
        self.summary_tab = QWidget()
        self.summary_layout = QVBoxLayout(self.summary_tab)
        self.summary_layout.setContentsMargins(8, 8, 8, 8)
        self.summary_layout.setSpacing(8)

        self.summary_title_label = QLabel(self.tr("No dataset selected"))
        self.summary_title_label.setStyleSheet("font-weight: 600;")
        self.summary_layout.addWidget(self.summary_title_label)

        self.summary_meta_label = QLabel("")
        self.summary_meta_label.setWordWrap(True)
        self.summary_layout.addWidget(self.summary_meta_label)

        # One-line, non-judgmental "what was reported" summary -- work/year,
        # sample size, whether a method was recorded. Never a claim about
        # whether the current observation agrees with this source.
        self.provenance_summary_label = QLabel("")
        self.provenance_summary_label.setWordWrap(True)
        self.provenance_summary_label.setStyleSheet("color: #7f8c8d; font-style: italic;")
        self.summary_layout.addWidget(self.provenance_summary_label)

        self.summary_table = QTableWidget(3, 4)
        self.summary_table.setFocusPolicy(Qt.NoFocus)
        self.summary_table.setHorizontalHeaderLabels(
            [self.tr("Metric"), self.tr("Min"), self.tr("Median / Mean"), self.tr("Max")]
        )
        self.summary_table.verticalHeader().setVisible(False)
        self.summary_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.summary_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.summary_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.summary_layout.addWidget(self.summary_table)

        # What the source said about the numbers in the table above: compact
        # meaning tags and any reported median / standard deviation. Kept
        # below the table and out of the "Median / Mean" column so a reported
        # median is never read as a mean.
        self.reported_statistics_label = QLabel("")
        self.reported_statistics_label.setWordWrap(True)
        self.reported_statistics_label.setVisible(False)
        # Deliberately no colour override: the tags are content, and a
        # hardcoded light-theme foreground is unreadable in dark mode.
        self.summary_layout.addWidget(self.reported_statistics_label)

        self.summary_note_label = QLabel()
        self.summary_note_label.setWordWrap(True)
        self.summary_note_label.setFrameShape(QFrame.StyledPanel)
        self.summary_note_label.setStyleSheet("color: #7f8c8d; padding: 8px;")
        self.summary_layout.addWidget(self.summary_note_label)

        self.review_tabs.addTab(self.summary_tab, self.tr("Summary"))

        # --- Raw spores tab -------------------------------------------
        self.raw_spores_text = QPlainTextEdit()
        self.raw_spores_text.setReadOnly(True)
        self.review_tabs.addTab(self.raw_spores_text, self.tr("Raw spores"))

        # --- Method tab -----------------------------------------------
        self.method_tab = QWidget()
        self.method_form = QFormLayout(self.method_tab)
        self.method_form.setContentsMargins(8, 8, 8, 8)
        self.method_form.setSpacing(8)
        self._method_labels: dict[str, QLabel] = {}
        for key, label in (
            ("mount", self.tr("Mounting medium (as reported):")),
            ("stain", self.tr("Stain (as reported):")),
            ("sample_type", self.tr("Preparation (as reported):")),
            ("contrast", self.tr("Contrast (as reported):")),
            ("objective", self.tr("Objective / method (as reported):")),
            ("scale", self.tr("Scale (as reported):")),
        ):
            value_label = QLabel("—")
            value_label.setWordWrap(True)
            self._method_labels[key] = value_label
            self.method_form.addRow(label, value_label)
        self.review_tabs.addTab(self.method_tab, self.tr("Method"))

        # --- Calibration tab ------------------------------------------
        self.calibration_text = QPlainTextEdit()
        self.calibration_text.setReadOnly(True)
        self.review_tabs.addTab(self.calibration_text, self.tr("Calibration"))

        # --- Provenance tab -------------------------------------------
        self.provenance_text = QPlainTextEdit()
        self.provenance_text.setReadOnly(True)
        self.review_tabs.addTab(self.provenance_text, self.tr("Provenance"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Reset all sub-tabs to their initial placeholder state."""
        self.summary_title_label.setText(self.tr("No dataset selected"))
        self.summary_meta_label.setText(
            self.tr(
                "Search by genus (or genus + species) and choose a result to review stats, method, calibration, and provenance."
            )
        )
        self.summary_note_label.setText(
            self.tr("Import actions stay disabled until a search result is selected and loaded.")
        )
        self.provenance_summary_label.setText("")
        self.set_reported_statistics("")
        for row, metric in enumerate(
            (self.tr("Length"), self.tr("Width"), self.tr("Q"))
        ):
            self.summary_table.setItem(row, 0, QTableWidgetItem(metric))
            self.summary_table.setItem(row, 1, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 2, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 3, QTableWidgetItem("—"))
        self.raw_spores_text.setPlainText(
            self.tr("Select a community result to review raw spore points.")
        )
        for label in self._method_labels.values():
            label.setText("—")
        self.calibration_text.setPlainText(
            self.tr("Select a community result to review calibration details.")
        )
        self.provenance_text.setPlainText(
            self.tr(
                "Select a community result to review contributor and source provenance."
            )
        )

    def set_summary(
        self,
        title: str,
        meta: str,
        rows: list[tuple[str, str, str, str]],
        note: str,
        derived: list[tuple[bool, bool, bool]] | None = None,
    ) -> None:
        """Populate the Summary tab.

        *rows* is a list of up to 3 ``(metric, min, median_mean, max)`` tuples.
        *derived* (optional), if given, is a parallel list of
        ``(min_derived, median_derived, max_derived)`` flags marking cells
        computed via a typical-range/community fallback rather than a
        directly reported extreme -- never a judgment about whether the
        value holds up, only about where it came from. Derived cells render
        with a footnote marker and an explanatory tooltip.

        Resets the reported-statistics line. This pane is shared between the
        picker's tabs, so a caller that says nothing about meaning tags must
        not inherit the previous dataset's; a caller that has them calls
        :meth:`set_reported_statistics` straight afterwards.
        """
        self.set_reported_statistics("")
        self.summary_title_label.setText(title)
        self.summary_meta_label.setText(meta)
        derived_tooltip = self.tr("Derived from typical range; not directly reported")
        for row_idx, (metric, vmin, vmedian, vmax) in enumerate(rows[:3]):
            flags = derived[row_idx] if derived and row_idx < len(derived) else (False, False, False)
            self.summary_table.setItem(row_idx, 0, QTableWidgetItem(metric))
            for col, text, is_derived in zip((1, 2, 3), (vmin, vmedian, vmax), flags):
                item = QTableWidgetItem(f"{text} †" if is_derived else text)
                if is_derived:
                    item.setToolTip(derived_tooltip)
                self.summary_table.setItem(row_idx, col, item)
        self.summary_note_label.setText(note)

    def set_reported_statistics(self, text: str, explanation: str = "") -> None:
        """Set the compact meaning tags and reported median / S.D. line.

        *text* is the compact form shown on screen; *explanation* is the full
        sentence per tag, exposed as the tooltip and the accessible
        description so the short form is never the only way to read it. An
        empty *text* hides the line entirely — a source that said nothing
        extra must not grow a placeholder row.
        """
        self.reported_statistics_label.setText(text or "")
        self.reported_statistics_label.setToolTip(explanation or "")
        self.reported_statistics_label.setAccessibleDescription(explanation or "")
        self.reported_statistics_label.setVisible(bool(text))

    def set_provenance_summary(self, text: str) -> None:
        """Set the one-line "what was reported" summary above the Summary
        table (work/year, sample size, whether a method was recorded).

        Must never state or imply agreement/disagreement with the current
        observation -- it describes only what the source itself reported.
        """
        self.provenance_summary_label.setText(text)

    def set_raw_spores(self, text: str) -> None:
        """Set the Raw spores tab content."""
        self.raw_spores_text.setPlainText(text)

    def set_method(self, mapping: dict[str, str]) -> None:
        """Set method-field labels.  Keys: mount, stain, sample_type, contrast, objective, scale."""
        for key, label in self._method_labels.items():
            label.setText(mapping.get(key, "—") or "—")

    def set_calibration(self, text: str) -> None:
        """Set the Calibration tab content."""
        self.calibration_text.setPlainText(text)

    def set_provenance(self, text: str) -> None:
        """Set the Provenance tab content."""
        self.provenance_text.setPlainText(text)
