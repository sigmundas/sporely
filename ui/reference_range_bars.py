"""Small length and width bars beside each reference in the reference drawer.

The retired Add-reference dialog's Summary tab drew the selected source against
the user's own spores on a frozen axis (``_MetricTrack`` in
``ui/reference_comparison_view.py``, removed with the dialog in stage 10a4).
This is that painting, made small enough to sit to the right of every library
row: two tracks, length above width, each with the reference as a filled band
above its axis rule and the user's own measurements below it.

What is drawn comes from :mod:`references.reference_comparison`, which is
unchanged and still decides every rule: a band only where a source stated two
bounds, a centre tick only where it stated a mean or a median, and an axis that
is frozen for the list, so rows can be compared with each other by eye. Content
outside that axis is clamped to the edge and marked with a chevron rather than
silently cut. Q is not drawn here.
"""

from __future__ import annotations

from PySide6.QtCore import QCoreApplication, QPointF, QRectF, QSize, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPalette, QPen, QPolygonF
from PySide6.QtWidgets import QSizePolicy, QWidget

from references.reference_comparison import ComparisonView, MetricComparison, MetricDomain, MetricSeries

# The reference: a saturated blue, filled (as in the retired comparison).
SOURCE_COLOR = QColor("#2d7dd2")
# The user's own spores: the success green the drawer already uses for a
# ticked row (``ui.library_source_row.SELECTED_BAR_COLOR``).
OBSERVATION_COLOR = QColor("#27ae60")
# The two metrics drawn, top to bottom.
BAR_METRICS: tuple[str, ...] = ("length", "width")

BARS_WIDTH = 140
# One metric's track: a lane above the axis rule and one below it.
TRACK_HEIGHT = 17
TRACK_GAP = 3
LANE_HEIGHT = 6.0
# Room for the "L" / "W" letter at the left of each track.
LABEL_WIDTH = 11
# Room at each end for an overflow chevron.
TRACK_MARGIN = 6


def metric_letter(metric: str) -> str:
    if metric == "length":
        return QCoreApplication.translate("ReferenceRangeBars", "L", "length, one letter")
    return QCoreApplication.translate("ReferenceRangeBars", "W", "width, one letter")


def _fill(color: QColor, alpha: int) -> QColor:
    tinted = QColor(color)
    tinted.setAlpha(alpha)
    return tinted


class ReferenceRangeBars(QWidget):
    """Length and width of one reference against the user's measurements."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._view: ComparisonView | None = None
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(BARS_WIDTH, len(BAR_METRICS) * TRACK_HEIGHT + (len(BAR_METRICS) - 1) * TRACK_GAP)

    def minimumSizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return self.sizeHint()

    def view(self) -> ComparisonView | None:
        return self._view

    def set_view(self, view: ComparisonView | None) -> None:
        self._view = view
        self.update()

    # -- painting ------------------------------------------------------

    def paintEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().paintEvent(event)
        view = self._view
        if view is None:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        try:
            letter_font = QFont(self.font())
            letter_font.setPointSizeF(max(letter_font.pointSizeF() - 3.0, 7.0))
            painter.setFont(letter_font)
            top = (self.height() - self.sizeHint().height()) / 2.0
            for index, metric in enumerate(BAR_METRICS):
                comparison = view.metric(metric)
                track_top = top + index * (TRACK_HEIGHT + TRACK_GAP)
                self._paint_track(painter, metric, comparison, track_top)
        finally:
            painter.end()

    def _paint_track(self, painter: QPainter, metric: str, comparison: MetricComparison | None, top: float) -> None:
        palette = self.palette()
        painter.setPen(palette.color(QPalette.WindowText))
        painter.drawText(
            QRectF(0, top, LABEL_WIDTH, TRACK_HEIGHT), Qt.AlignLeft | Qt.AlignVCenter, metric_letter(metric)
        )
        left = LABEL_WIDTH + TRACK_MARGIN
        width = max(self.width() - left - TRACK_MARGIN, 1)
        axis_y = top + TRACK_HEIGHT / 2.0
        painter.setPen(QPen(palette.color(QPalette.Mid), 1))
        painter.drawLine(QPointF(left, axis_y), QPointF(left + width, axis_y))
        if comparison is None:
            return
        self._paint_series(
            painter, comparison.source, comparison.domain, left, width,
            axis_y - 1.0 - LANE_HEIGHT, SOURCE_COLOR, filled=True,
        )
        self._paint_series(
            painter, comparison.observation, comparison.domain, left, width,
            axis_y + 1.0, OBSERVATION_COLOR, filled=False,
        )

    def _paint_series(
        self,
        painter: QPainter,
        series: MetricSeries | None,
        domain: MetricDomain,
        left: float,
        width: float,
        top: float,
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
            if band.high < domain.low or band.low > domain.high:
                # Wholly off the axis: a clamped sliver at the edge would read
                # as overlap, so only the chevron says where it went.
                self._paint_overflow(
                    painter, band.high < domain.low, band.low > domain.high, left, width,
                    top + LANE_HEIGHT / 2.0, color,
                )
                continue
            core = band.role == "core"
            # Inset only inside an outer extent; a typical range on its own fills the lane.
            inset = 1.5 if core and filled and series.outer is not None else 0.0
            rect = QRectF(
                x_of(band.low), top + inset, max(x_of(band.high) - x_of(band.low), 1.5), LANE_HEIGHT - 2 * inset,
            )
            if filled:
                # The source: a light outer extent, a strong inner range.
                painter.setBrush(_fill(color, 190 if core else 80))
                painter.setPen(Qt.NoPen if core else QPen(color, 1))
            elif core:
                # The user's 5–95 % range, filled lightly inside the outlined min–max.
                painter.setBrush(_fill(color, 110))
                painter.setPen(Qt.NoPen)
            else:
                painter.setBrush(Qt.NoBrush)
                painter.setPen(QPen(color, 1))
            painter.drawRect(rect)
            self._paint_overflow(painter, band.clipped_low, band.clipped_high, left, width, top + LANE_HEIGHT / 2.0, color)

        for centre in series.centres:
            pen = QPen(color.darker(130) if filled else color, 1.6)
            if centre.clipped:
                pen.setStyle(Qt.DotLine)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            if centre.value is not None:
                x = x_of(centre.value)
                painter.drawLine(QPointF(x, top - 1.0), QPointF(x, top + LANE_HEIGHT + 1.0))
            elif centre.interval is not None:
                low, high = centre.interval
                mid = top + LANE_HEIGHT / 2.0
                painter.drawLine(QPointF(x_of(low), mid), QPointF(x_of(high), mid))
            self._paint_overflow(
                painter, centre.clipped_low, centre.clipped_high, left, width, top + LANE_HEIGHT / 2.0, color,
            )

    @staticmethod
    def _paint_overflow(
        painter: QPainter, clipped_low: bool, clipped_high: bool, left: float, width: float, mid: float, color: QColor,
    ) -> None:
        """Mark an end the frozen axis could not contain (never a silent clip)."""
        if not (clipped_low or clipped_high):
            return
        painter.setBrush(color)
        painter.setPen(Qt.NoPen)
        if clipped_low:
            painter.drawPolygon(QPolygonF([
                QPointF(left - 5, mid), QPointF(left - 1, mid - 3), QPointF(left - 1, mid + 3),
            ]))
        if clipped_high:
            right = left + width
            painter.drawPolygon(QPolygonF([
                QPointF(right + 5, mid), QPointF(right + 1, mid - 3), QPointF(right + 1, mid + 3),
            ]))


__all__ = ["BAR_METRICS", "OBSERVATION_COLOR", "ReferenceRangeBars", "SOURCE_COLOR", "metric_letter"]
