"""The Add-reference preview's comparison surface: three metrics, one axis.

This is the Summary tab's replacement for the old 3x4 numeric table. The table
asked the user to decode six numbers and hold them against their own
observation in their head; this shows the source and the observation on the
same frozen axis, which is the comparison the dialog exists to support
(contract N2, N17, N18).

Division of labour, kept strict on purpose:

* :mod:`references.reference_comparison` decides *what is true* — which bands
  exist, which centre statistics were really stated, where the axis runs, and
  whether anything had to be clipped. It is pure and has no Qt import.
* This module decides *what it looks like and what it is called*. Every
  user-visible string is a ``tr(...)`` here, and every colour and rectangle is
  here. No widget in this file re-reads a database column or infers a
  statistic; if a caption says "typical range", it is because a stored
  descriptor said ``typical_range``.

The two parties are distinguished the way the approved mockup does it: the
source is a **filled** band and the user's own observation is an **outlined**
one, never by text alone (N18). Both sit on the same axis, one above the other,
so the eye compares horizontal position rather than reading numbers.

With no source selected the view switches to the baseline state (N21): the
user's own Length / Width / Q as chips, plus one line of copy saying what
selecting a source will do. Deliberately not a table of dashes — an empty grid
looks like a failure, and it is not one.
"""

from __future__ import annotations

from PySide6.QtCore import QEvent, QPointF, QRectF, QSize, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPalette, QPen, QPolygonF
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from references.measurement_content import METRICS
from references.reference_comparison import (
    MEASURED_EXTREMES,
    MEASURED_PERCENTILE,
    BandView,
    CentreView,
    ComparisonView,
    MetricComparison,
    MetricDomain,
    MetricSeries,
)

#: The selected source. A saturated blue, filled, because the source is the
#: thing being evaluated and should be the loudest mark on the track.
SOURCE_COLOR = QColor("#2d7dd2")
#: The user's own observation. The same success green the picker already uses
#: for the selected row (``ui.library_source_row.SELECTED_BAR_COLOR``), drawn
#: as an outline so it reads as the constant backdrop rather than as a second
#: competing claim.
OBSERVATION_COLOR = QColor("#27ae60")
#: Height of one metric's drawing area: two 12px lanes either side of the axis
#: rule, plus room for a centre tick to overshoot its lane. Three of these plus
#: their captions have to fit a preview pane roughly 400px tall, so the track
#: states a *fixed* height (see :meth:`_MetricTrack.sizeHint`) and the captions
#: take whatever is left.
TRACK_HEIGHT = 38
#: Horizontal breathing room so an end-of-axis band is not painted flush
#: against the widget border, where it would be indistinguishable from a band
#: that ran off the edge.
TRACK_MARGIN = 10


#: How far a caption's colour is moved from the normal text colour towards the
#: background. Enough to read as secondary, nowhere near enough to stop being
#: readable: these captions carry the scientific content of the pane — which
#: band is min–max, which is 5–95%, whether a centre exists at all — so a
#: caption nobody can read defeats the comparison entirely.
CAPTION_FADE = 0.25


def _fill(color: QColor, alpha: int) -> QColor:
    tinted = QColor(color)
    tinted.setAlpha(alpha)
    return tinted


def caption_color(palette) -> QColor:
    """A de-emphasised but legible text colour for the current palette.

    Derived by blending :attr:`QPalette.WindowText` towards
    :attr:`QPalette.Window`, rather than by naming a palette role. The obvious
    role, ``Mid``, is a frame-shading colour and not a text colour: in the dark
    theme it is ``#353534`` against a ``#131313`` window — roughly 2:1, which
    made every caption in this pane practically invisible. Blending keeps the
    contrast tied to the theme's own foreground/background pair, so both
    shipped themes land far above the 4.5:1 readability floor and a future
    theme inherits the same property.
    """
    text = palette.color(QPalette.WindowText)
    background = palette.color(QPalette.Window)
    return QColor(
        round(text.red() + (background.red() - text.red()) * CAPTION_FADE),
        round(text.green() + (background.green() - text.green()) * CAPTION_FADE),
        round(text.blue() + (background.blue() - text.blue()) * CAPTION_FADE),
    )


class _MetricTrack(QWidget):
    """One metric's axis with the source and observation bands painted on it.

    Draws only what the model contains. There is no code path here that
    synthesizes a band from a centre, a centre from a band, or an axis from the
    data: the axis arrives as a frozen :class:`MetricDomain` and is used as
    given, so switching between sources cannot move it.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._comparison: MetricComparison | None = None
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        # A plain QWidget has an invalid size hint, which a Fixed-height policy
        # cannot act on: the enclosing layout then squeezed the track and the
        # axis caption underneath it into overlapping rectangles. Stating the
        # height here is what keeps the two apart at any pane size.
        return QSize(200, TRACK_HEIGHT)

    def minimumSizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(40, TRACK_HEIGHT)

    def set_comparison(self, comparison: MetricComparison | None) -> None:
        self._comparison = comparison
        self.update()

    # -- painting ------------------------------------------------------

    def paintEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().paintEvent(event)
        comparison = self._comparison
        if comparison is None:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        try:
            left = TRACK_MARGIN
            width = max(self.width() - 2 * TRACK_MARGIN, 1)
            # The axis rule sits between the two lanes, so both bands are
            # measured against the same line rather than against the widget.
            axis_y = self.height() / 2.0
            pen = QPen(self.palette().mid().color())
            pen.setWidth(1)
            painter.setPen(pen)
            painter.drawLine(left, int(axis_y), left + width, int(axis_y))

            lane_height = 12.0
            self._paint_series(
                painter,
                comparison.source,
                comparison.domain,
                left,
                width,
                axis_y - 2.0 - lane_height,
                lane_height,
                SOURCE_COLOR,
                filled=True,
            )
            self._paint_series(
                painter,
                comparison.observation,
                comparison.domain,
                left,
                width,
                axis_y + 2.0,
                lane_height,
                OBSERVATION_COLOR,
                filled=False,
            )
        finally:
            painter.end()

    def _paint_series(
        self,
        painter: QPainter,
        series: MetricSeries | None,
        domain: MetricDomain,
        left: int,
        width: int,
        top: float,
        height: float,
        color: QColor,
        *,
        filled: bool,
    ) -> None:
        if series is None:
            return

        def x_of(value: float) -> float:
            return left + domain.position(value) * width

        for band in (series.outer, series.core):
            if band is None:
                continue
            # The inner band is drawn shorter and stronger than the outer one,
            # so an explicit 5-95 interval inside a min/max extent stays
            # visibly a different claim (N15) even in a grey-scale printout.
            inset = 0.0 if band.role == "outer" else height * 0.25
            rect = QRectF(
                x_of(band.low),
                top + inset,
                max(x_of(band.high) - x_of(band.low), 1.5),
                height - 2 * inset,
            )
            if filled:
                painter.setBrush(_fill(color, 160 if band.role == "core" else 70))
                painter.setPen(QPen(color, 1))
            else:
                painter.setBrush(Qt.NoBrush)
                pen = QPen(color, 2 if band.role == "core" else 1)
                if band.role == "core":
                    pen.setStyle(Qt.DashLine)
                painter.setPen(pen)
            painter.drawRect(rect)
            self._paint_overflow(
                painter,
                band.clipped_low,
                band.clipped_high,
                left,
                width,
                top + height / 2.0,
                color,
            )

        for centre in series.centres:
            painter.setBrush(Qt.NoBrush)
            pen = QPen(color, 2)
            if centre.clipped:
                # A clamped centre is painted on the axis edge, where an
                # ordinary solid tick would read as a value that really sits
                # there. Dashed plus the chevron below says it does not.
                pen.setStyle(Qt.DotLine)
            painter.setPen(pen)
            if centre.value is not None:
                x = x_of(centre.value)
                painter.drawLine(int(x), int(top - 2), int(x), int(top + height + 2))
            elif centre.interval is not None:
                low, high = centre.interval
                painter.drawLine(
                    int(x_of(low)), int(top + height / 2), int(x_of(high)), int(top + height / 2)
                )
            self._paint_overflow(
                painter,
                centre.clipped_low,
                centre.clipped_high,
                left,
                width,
                top + height / 2.0,
                color,
            )

    def _paint_overflow(
        self,
        painter: QPainter,
        clipped_low: bool,
        clipped_high: bool,
        left: int,
        width: int,
        mid: float,
        color: QColor,
    ) -> None:
        """Mark an edge the frozen axis could not contain.

        The axis does not move for late-arriving content, so content that does
        not fit has to say so. A silent clip would misreport the source's
        extent as ending exactly at the edge of the track. Used for bands and
        for centre marks alike: a mean clamped to the axis end is exactly as
        misleading as a band clamped there.
        """
        if not (clipped_low or clipped_high):
            return
        painter.setBrush(color)
        painter.setPen(QPen(color, 1))
        if clipped_low:
            painter.drawPolygon(
                QPolygonF(
                    [
                        QPointF(left - 7, mid),
                        QPointF(left - 1, mid - 4),
                        QPointF(left - 1, mid + 4),
                    ]
                )
            )
        if band.clipped_high:
            right = left + width
            painter.drawPolygon(
                QPolygonF(
                    [
                        QPointF(right + 7, mid),
                        QPointF(right + 1, mid - 4),
                        QPointF(right + 1, mid + 4),
                    ]
                )
            )


class MetricComparisonRow(QWidget):
    """Metric heading, painted track, and the captions that name the claims.

    The captions are the honesty half of this row. A band is only meaningful
    beside a statement of what it is, so every drawn band contributes a phrase
    naming its descriptor, and a source that reported no centre contributes an
    explicit "no mean or median reported" rather than silence the user could
    mistake for an oversight.
    """

    def __init__(self, metric: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._metric = metric
        # Populated by _make_caption, so the recolour on a palette change has
        # one list to walk instead of three hand-maintained references.
        self._caption_labels: list[QLabel] = []
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        self.title_label = QLabel(self.metric_title(metric), self)
        title_font = QFont(self.title_label.font())
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        header.addWidget(self.title_label)
        self.delta_label = QLabel("", self)
        header.addSpacing(8)
        header.addWidget(self.delta_label)
        header.addStretch(1)
        # The axis extent rides in the header rather than on a line of its
        # own: three metrics each needed four lines of text under the track,
        # and only one of them fitted the pane at its default height.
        self.axis_label = QLabel("", self)
        self.axis_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self._make_caption(self.axis_label)
        header.addWidget(self.axis_label)
        layout.addLayout(header)

        self.track = _MetricTrack(self)
        layout.addWidget(self.track)

        self.source_caption_label = QLabel("", self)
        self.source_caption_label.setWordWrap(True)
        self._make_caption(self.source_caption_label)
        layout.addWidget(self.source_caption_label)

        self.observation_caption_label = QLabel("", self)
        self.observation_caption_label.setWordWrap(True)
        self._make_caption(self.observation_caption_label)
        layout.addWidget(self.observation_caption_label)

    def _make_caption(self, label: QLabel) -> None:
        font = QFont(label.font())
        font.setPointSizeF(max(font.pointSizeF() - 1.0, 7.0))
        label.setFont(font)
        self._caption_labels.append(label)
        self._apply_caption_color()

    def _apply_caption_color(self) -> None:
        """Recolour every caption from the palette in force right now.

        Computed rather than named (see :func:`caption_color`), and re-applied
        on a palette change so switching theme at runtime cannot leave the
        captions coloured for the theme that was current when the row was
        built.
        """
        color = caption_color(self.palette())
        for label in self._caption_labels:
            label.setStyleSheet(f"color: {color.name()};")

    def changeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().changeEvent(event)
        if event.type() == QEvent.PaletteChange:
            self._apply_caption_color()

    def metric_title(self, metric: str) -> str:
        if metric == "length":
            return self.tr("Length")
        if metric == "width":
            return self.tr("Width")
        return self.tr("Q")

    def unit_suffix(self) -> str:
        # Q is a ratio of two lengths and carries no unit; printing "µm" on it
        # would be a straightforward factual error.
        return "" if self._metric == "q" else " µm"

    # -- content -------------------------------------------------------

    def set_comparison(self, comparison: MetricComparison | None) -> None:
        self.track.set_comparison(comparison)
        if comparison is None:
            self.axis_label.setText("")
            self.source_caption_label.setText("")
            self.observation_caption_label.setText("")
            self.delta_label.setText("")
            self.delta_label.setToolTip("")
            return
        self.axis_label.setText(
            self.tr("axis {low}–{high}").format(
                low=self._number(comparison.domain.low),
                high=self._number(comparison.domain.high),
            )
        )
        self.source_caption_label.setText(
            self._series_caption(self.tr("Source"), comparison.source)
        )
        self.observation_caption_label.setText(
            self._series_caption(self.tr("Yours"), comparison.observation)
        )
        self._set_delta(comparison)

    def _set_delta(self, comparison: MetricComparison) -> None:
        delta = comparison.delta
        if delta is None:
            self.delta_label.setText("")
            self.delta_label.setToolTip(
                self.tr(
                    "No delta: the source and your observation do not state the "
                    "same statistic."
                )
            )
            return
        statistic = (
            self.tr("mean") if delta.statistic == "mean" else self.tr("median")
        )
        difference = delta.difference
        if abs(difference) < 0.05:
            self.delta_label.setText(
                self.tr("same {statistic}").format(statistic=statistic)
            )
        else:
            self.delta_label.setText(
                self.tr("yours {delta}{unit} ({statistic})").format(
                    delta=f"{difference:+.1f}",
                    unit=self.unit_suffix(),
                    statistic=statistic,
                )
            )
        self.delta_label.setToolTip(
            self.tr("Your {statistic} {ours} against the source's {theirs}.").format(
                statistic=statistic,
                ours=self._number(delta.observation_value),
                theirs=self._number(delta.source_value),
            )
        )

    # -- wording -------------------------------------------------------

    @staticmethod
    def _number(value: float) -> str:
        return f"{float(value):.1f}"

    def _series_caption(self, who: str, series: MetricSeries | None) -> str:
        if series is None or series.is_empty:
            return self.tr("{who}: nothing reported for this metric.").format(who=who)
        parts = [
            self._band_phrase(band)
            for band in (series.outer, series.core)
            if band is not None
        ]
        centre_parts = [self._centre_phrase(centre) for centre in series.centres]
        if centre_parts:
            parts.extend(centre_parts)
        else:
            # An absent centre is information, not a gap to be filled. Saying
            # so here is what stops a reader assuming the midpoint of the band
            # is a mean somebody reported (N15).
            parts.append(self.tr("no centre reported"))
        return f"{who}: " + " · ".join(parts)

    def _band_phrase(self, band: BandView) -> str:
        span = self.tr("{low}–{high}{unit}").format(
            low=self._number(band.low), high=self._number(band.high), unit=self.unit_suffix()
        )
        phrase = self.tr("{span} ({meaning})").format(
            span=span, meaning=self._band_meaning(band)
        )
        if band.clipped:
            # The axis is frozen for the session, so a value outside it is
            # drawn at the edge. The reader has to be told that edge is not
            # the real bound.
            phrase = self.tr("{phrase}, beyond the axis").format(phrase=phrase)
        return phrase

    def _band_meaning(self, band: BandView) -> str:
        # A band calculated from measurements is worded the same way whichever
        # descriptor it was stored under. A Community aggregate's extremes are
        # tagged ``reported_extremes`` because that is their shape in the
        # frozen contract, but nobody reported them -- they are the min and max
        # of contributors' spores. Calling them "reported" here would credit an
        # author with a number Sporely computed, and would contradict the
        # "measured median" this very caption prints beside them (N15).
        measured = band.origin == "computed"
        if band.meaning == MEASURED_EXTREMES or (
            measured and band.meaning == "reported_extremes"
        ):
            return self.tr("measured min–max")
        if band.meaning == MEASURED_PERCENTILE or (
            measured and band.meaning == "percentile_interval" and band.percentile_bounds
        ):
            bounds = band.percentile_bounds or (5.0, 95.0)
            return self.tr("measured {low}–{high}%").format(
                low=f"{bounds[0]:g}", high=f"{bounds[1]:g}"
            )
        if band.meaning == "percentile_interval" and band.percentile_bounds:
            bounds = band.percentile_bounds
            return self.tr("{low}–{high}% range").format(
                low=f"{bounds[0]:g}", high=f"{bounds[1]:g}"
            )
        if band.meaning == "reported_extremes":
            return self.tr("reported extremes")
        if band.meaning == "typical_range":
            return self.tr("typical range")
        if band.meaning == "reported_range":
            return self.tr("reported range")
        # ``unspecified`` and a legacy row that stored numbers without saying
        # what they mean both land here. "Published range" is the contract's
        # deliberately unspecific label (N13) and claims nothing further.
        return self.tr("published range")

    def _centre_phrase(self, centre: CentreView) -> str:
        name = self.tr("mean") if centre.statistic == "mean" else self.tr("median")
        if centre.origin == "computed":
            # "measured", not a bare "mean": a number calculated from
            # individual spores is not one an author published, and the two
            # must not read the same (contract N15).
            name = (
                self.tr("measured mean")
                if centre.statistic == "mean"
                else self.tr("measured median")
            )
        if centre.value is not None:
            phrase = self.tr("{name} {value}{unit}").format(
                name=name, value=self._number(centre.value), unit=self.unit_suffix()
            )
        else:
            low, high = centre.interval or (0.0, 0.0)
            phrase = self.tr("{name} {low}–{high}{unit}").format(
                name=name,
                low=self._number(low),
                high=self._number(high),
                unit=self.unit_suffix(),
            )
        if centre.clipped:
            # Same treatment a band gets: the mark is painted clamped to the
            # axis edge, and the reader must not take that edge for the value.
            phrase = self.tr("{phrase}, beyond the axis").format(phrase=phrase)
        return phrase


class ReferenceComparisonView(QWidget):
    """The Summary surface: baseline chips when nothing is selected, else bands.

    One widget with two states rather than two widgets, so the pane cannot end
    up showing both at once or neither. :meth:`set_view` takes the whole model
    and is the only way the content changes.
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)

        # --- baseline (no source selected) ---------------------------
        self.baseline_widget = QWidget(self)
        baseline_layout = QVBoxLayout(self.baseline_widget)
        baseline_layout.setContentsMargins(0, 0, 0, 0)
        baseline_layout.setSpacing(6)
        self.baseline_heading_label = QLabel("", self.baseline_widget)
        heading_font = QFont(self.baseline_heading_label.font())
        heading_font.setBold(True)
        self.baseline_heading_label.setFont(heading_font)
        baseline_layout.addWidget(self.baseline_heading_label)
        chips_row = QHBoxLayout()
        chips_row.setContentsMargins(0, 0, 0, 0)
        chips_row.setSpacing(6)
        self._chips: dict[str, QLabel] = {}
        for metric in METRICS:
            chip = QLabel("", self.baseline_widget)
            # Fixed, so the chip stays a chip: inside the scrolled Summary
            # body an Expanding label grows to fill the whole pane and the
            # three "chips" become three tall empty boxes.
            chip.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            chip.setStyleSheet(
                f"color: {OBSERVATION_COLOR.name()};"
                f" border: 1px solid {OBSERVATION_COLOR.name()};"
                " border-radius: 6px;"
                " padding: 2px 6px;"
            )
            self._chips[metric] = chip
            chips_row.addWidget(chip)
        chips_row.addStretch(1)
        baseline_layout.addLayout(chips_row)
        self.baseline_copy_label = QLabel("", self.baseline_widget)
        self.baseline_copy_label.setWordWrap(True)
        baseline_layout.addWidget(self.baseline_copy_label)
        baseline_layout.addStretch(1)
        root.addWidget(self.baseline_widget, 1)

        # --- comparison (a source is selected) -----------------------
        self.comparison_widget = QWidget(self)
        comparison_layout = QVBoxLayout(self.comparison_widget)
        comparison_layout.setContentsMargins(0, 0, 0, 0)
        comparison_layout.setSpacing(10)
        self.legend_label = QLabel("", self.comparison_widget)
        self.legend_label.setWordWrap(True)
        comparison_layout.addWidget(self.legend_label)
        self.rows: dict[str, MetricComparisonRow] = {}
        for metric in METRICS:
            row = MetricComparisonRow(metric, self.comparison_widget)
            self.rows[metric] = row
            comparison_layout.addWidget(row)
        comparison_layout.addStretch(1)
        root.addWidget(self.comparison_widget, 1)

        self._view: ComparisonView | None = None
        self.set_view(None)

    # -- content -------------------------------------------------------

    def view(self) -> ComparisonView | None:
        """The model currently rendered, for tests and for the dialog."""
        return self._view

    def set_view(self, view: ComparisonView | None) -> None:
        self._view = view
        has_source = bool(view is not None and view.has_source)
        self.baseline_widget.setVisible(not has_source)
        self.comparison_widget.setVisible(has_source)
        if has_source:
            self._apply_comparison(view)  # type: ignore[arg-type]
        else:
            self._apply_baseline(view)

    def _apply_baseline(self, view: ComparisonView | None) -> None:
        count = view.observation_point_count if view is not None else 0
        if count:
            self.baseline_heading_label.setText(
                self.tr("Your observation — {count} measured spores").format(count=count)
            )
            self.baseline_copy_label.setText(
                self.tr(
                    "Select a source to compare it against these measurements. "
                    "The axes stay fixed while you move between sources."
                )
            )
        else:
            # Honest about the real state: there is no baseline to compare
            # against, which is different from a baseline of zero.
            self.baseline_heading_label.setText(self.tr("Your observation"))
            self.baseline_copy_label.setText(
                self.tr(
                    "This observation has no spore measurements yet, so there is "
                    "nothing to compare a source against. Select a source to "
                    "review what it reports."
                )
            )
        for metric, chip in self._chips.items():
            chip.setText(self._chip_text(metric, view))
            chip.setVisible(bool(count))

    def _chip_text(self, metric: str, view: ComparisonView | None) -> str:
        row = self.rows[metric]
        title = row.metric_title(metric)
        comparison = view.metric(metric) if view is not None else None
        series = comparison.observation if comparison is not None else None
        if series is None or series.outer is None:
            return self.tr("{metric} —").format(metric=title)
        return self.tr("{metric} {low}–{high}{unit}").format(
            metric=title,
            low=f"{series.outer.low:.1f}",
            high=f"{series.outer.high:.1f}",
            unit=row.unit_suffix(),
        )

    def _apply_comparison(self, view: ComparisonView) -> None:
        if view.observation_point_count:
            self.legend_label.setText(
                self.tr(
                    "Filled band: the source. Outlined band: your {count} measured "
                    "spores."
                ).format(count=view.observation_point_count)
            )
        else:
            self.legend_label.setText(
                self.tr(
                    "Filled band: the source. Your observation has no spore "
                    "measurements to compare against."
                )
            )
        for metric, row in self.rows.items():
            row.set_comparison(view.metric(metric))


__all__ = [
    "CAPTION_FADE",
    "OBSERVATION_COLOR",
    "SOURCE_COLOR",
    "MetricComparisonRow",
    "ReferenceComparisonView",
    "caption_color",
]
