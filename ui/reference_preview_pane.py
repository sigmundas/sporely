"""Reusable reference review pane (Summary / Raw spores / Method / Calibration / Provenance)."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFormLayout,
    QFrame,
    QHeaderView,
    QLabel,
    QPlainTextEdit,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from references.reference_comparison import ComparisonView, raw_point_rows

from .reference_comparison_view import ReferenceComparisonView


class ReferencePreviewPane(QWidget):
    """Tabbed review pane for one reference dataset.

    Shared by every source in the Add reference picker -- the Library,
    Community and My observations tabs all populate this one pane (see
    ``AddReferenceDialog.preview_pane`` and
    ``CommunityResultsPane._preview_pane``). Its placeholder text must
    therefore stay source-neutral: it is equally on screen for a library
    measurement set and for a personal observation, neither of which is a
    "community result".

    Owns the QTabWidget and all sub-tab widgets. Callers populate it
    through the public API below; all tr() calls live here so strings
    are attributed to this class's context.

    The Summary tab has two mutually exclusive bodies:

    * :class:`~ui.reference_comparison_view.ReferenceComparisonView` — the
      comparison the redesign is built around, set through
      :meth:`set_comparison`, and the state :meth:`clear` falls back to (it
      then shows the injected observation baseline rather than an empty grid).
    * :attr:`summary_table` — the original 3x4 numeric table, still driven by
      :meth:`set_summary`. It is the migration surface for the source tabs that
      have not moved onto the comparison model yet (Community, My observations
      and the manual editor); each is a separate stage, and leaving their
      numbers on screen in the meantime is what keeps this dialog working
      between those stages. Exactly one of the two is visible at any time.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        #: The no-selection model, injected by the host that knows the current
        #: observation (see ``AddReferenceDialog``). ``None`` for a host with
        #: no observation context, which produces the honest
        #: "no measurements to compare against" baseline rather than a guess.
        self._baseline_view: ComparisonView | None = None
        #: Which Summary body is current. Tracked explicitly rather than read
        #: back off ``isVisible()``, which is ``False`` for every widget in a
        #: dialog that has not been shown yet and would therefore make a
        #: still-hidden pane answer "table" no matter what it last rendered.
        self._summary_mode = "comparison"
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

        self.comparison_view = ReferenceComparisonView()
        # Scrolled, because the comparison is genuinely taller than a narrow
        # preview pane: three tracks plus the captions that name each band
        # wrap to different heights per source. Without this the layout
        # squeezed the rows until a band was painted across the words
        # describing it, and the dialog is deliberately resizable down to a
        # small laptop screen.
        self._comparison_scroll = QScrollArea(self.summary_tab)
        self._comparison_scroll.setWidgetResizable(True)
        self._comparison_scroll.setFrameShape(QFrame.NoFrame)
        self._comparison_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._comparison_scroll.setWidget(self.comparison_view)
        self.summary_layout.addWidget(self._comparison_scroll, 1)

        self.summary_table = QTableWidget(3, 4)
        self.summary_table.setFocusPolicy(Qt.NoFocus)
        self.summary_table.setHorizontalHeaderLabels(
            [self.tr("Metric"), self.tr("Min"), self.tr("Median / Mean"), self.tr("Max")]
        )
        self.summary_table.verticalHeader().setVisible(False)
        self.summary_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.summary_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.summary_table.setSelectionMode(QAbstractItemView.NoSelection)
        # Hidden until a caller that still uses the legacy row API asks for
        # it; the comparison is the Summary tab's default body.
        self.summary_table.setVisible(False)
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

    def set_observation_baseline(self, view: ComparisonView | None) -> None:
        """Install the no-selection model and show it immediately.

        Injected by the host rather than queried here: the pane has no idea
        which observation is open, and giving it one would mean a second,
        divergent read of the same measurements.
        """
        self._baseline_view = view
        current = self.comparison_view.view()
        if self._summary_mode == "comparison" and (current is None or not current.has_source):
            self.comparison_view.set_view(view)

    def set_header(self, title: str, meta: str, note: str) -> None:
        """Set the Summary tab's title / metadata / note lines.

        The same three labels :meth:`set_summary` writes, separated out so a
        caller on the comparison model can fill them without also asking for
        the legacy table.
        """
        self.summary_title_label.setText(title)
        self.summary_meta_label.setText(meta)
        self.summary_note_label.setText(note)

    def set_comparison(self, view: ComparisonView) -> None:
        """Show one source against the observation on the frozen axes."""
        self.comparison_view.set_view(view)
        self._show_comparison_body()

    def clear(self) -> None:
        """Reset all sub-tabs to their initial placeholder state."""
        self.summary_title_label.setText(self.tr("No dataset selected"))
        self.summary_meta_label.setText(
            self.tr(
                "Choose a result to review stats, method, calibration, and provenance."
            )
        )
        self.summary_note_label.setText(
            self.tr("Actions stay disabled until a result is selected and loaded.")
        )
        self.provenance_summary_label.setText("")
        self.set_reported_statistics("")
        # The baseline comparison, not a grid of dashes (contract N21). The
        # legacy table is still filled underneath so a caller mid-migration
        # that reads it sees a consistent cleared state, but it is hidden.
        self.comparison_view.set_view(self._baseline_view)
        self._show_comparison_body()
        for row, metric in enumerate(
            (self.tr("Length"), self.tr("Width"), self.tr("Q"))
        ):
            self.summary_table.setItem(row, 0, QTableWidgetItem(metric))
            self.summary_table.setItem(row, 1, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 2, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 3, QTableWidgetItem("—"))
        self.raw_spores_text.setPlainText(
            self.tr("Select a result to review raw spore points.")
        )
        for label in self._method_labels.values():
            label.setText("—")
        self.calibration_text.setPlainText(
            self.tr("Select a result to review calibration details.")
        )
        self.provenance_text.setPlainText(
            self.tr(
                "Select a result to review contributor and source provenance."
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
        self._show_table_body()
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

    def _show_comparison_body(self) -> None:
        self._summary_mode = "comparison"
        self._comparison_scroll.setVisible(True)
        self.summary_table.setVisible(False)

    def _show_table_body(self) -> None:
        self._summary_mode = "table"
        self._comparison_scroll.setVisible(False)
        self.summary_table.setVisible(True)

    def set_raw_spores(self, text: str) -> None:
        """Set the Raw spores tab content verbatim."""
        self.raw_spores_text.setPlainText(text)

    def set_raw_spore_points(self, points, *, caption: str = "") -> None:
        """Show the source's *actual* individual measurements.

        Falls through to :meth:`set_raw_spores_unavailable` when the points
        decode to nothing. Which explanation that produces depends on what was
        handed in: an empty list is a source that stores no measurements, while
        a non-empty list none of whose members carries a dimension is content
        this binary could not read. Reporting the second as "not published"
        would state, on the author's behalf, something the corrupt blob does
        not establish.
        """
        rows = raw_point_rows(points)
        if not rows:
            self.set_raw_spores_unavailable(unreadable=bool(points))
            return
        header = (
            self.tr("#").ljust(5)
            + self.tr("Length").rjust(8)
            + self.tr("Width").rjust(8)
            + self.tr("Q").rjust(8)
        )
        lines = [
            self.tr("{count} individual spore measurements").format(count=len(rows)),
            "",
            header,
            "-" * len(header),
        ]
        lines.extend(
            index.ljust(5) + length.rjust(8) + width.rjust(8) + q.rjust(8)
            for index, length, width, q in rows
        )
        if caption:
            lines.extend(("", caption))
        self.raw_spores_text.setPlainText("\n".join(lines))

    def set_raw_spores_unavailable(
        self, *, plotted_as_band: bool = True, unreadable: bool = False
    ) -> None:
        """State plainly that there are no per-spore measurements to show.

        Contract N22: a range is never expanded into invented rows to make
        this tab look populated. Saying what *will* be plotted keeps the
        absence from reading as a failure — the source is still usable, it
        simply contains a different kind of claim.

        ``unreadable`` separates two states this tab must not conflate. A
        source that stores no points did not publish individual measurements,
        and saying so is a fact about the publication. A source whose stored
        points cannot be decoded establishes nothing about what its author
        published — only that Sporely cannot read what is on file — so it gets
        neutral wording naming the real problem.
        """
        if unreadable:
            text = self.tr(
                "No individual spore measurements are available to display: "
                "the measurements stored for this source could not be read."
            )
            if plotted_as_band:
                text += "\n" + self.tr(
                    "The published range will be plotted as a range band."
                )
            self.raw_spores_text.setPlainText(text)
            return
        text = self.tr(
            "Individual spore measurements were not published for this source."
        )
        if plotted_as_band:
            text += "\n" + self.tr("The published range will be plotted as a range band.")
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
