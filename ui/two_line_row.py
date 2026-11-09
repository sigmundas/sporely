"""Shared two-line results-row widget for the Add-reference picker's tabs."""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QVBoxLayout, QWidget


class TwoLineRow(QWidget):
    """Results-list row: a title line, then an independently-elided detail
    line.

    Each line is elided against the row's own width (mirrors
    ``ui/comparison_panel.py``'s ``_ComparisonRowWidget.resizeEvent``
    pattern) rather than the list's viewport width, so the row stays
    correct across splitter drags. Shared by the Library, My-observations,
    and Community tabs of ``AddReferenceDialog`` so all three source tabs
    render rows identically.
    """

    def __init__(self, label: str, detail: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)
        layout.setSpacing(0)

        self._full_label = label
        self._full_detail = detail

        self.title_label = QLabel(self)
        self.title_label.setMinimumWidth(0)
        layout.addWidget(self.title_label)
        self._title_metrics = QFontMetrics(self.title_label.font())

        self.detail_label = QLabel(self)
        self.detail_label.setStyleSheet("color: #7f8c8d; font-size: 11px;")
        self.detail_label.setMinimumWidth(0)
        self.detail_label.setVisible(bool(detail))
        layout.addWidget(self.detail_label)
        self._detail_metrics = QFontMetrics(self.detail_label.font())

        self.title_label.setText(label)
        self.detail_label.setText(detail)

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().resizeEvent(event)
        width = max(self.width() - 8, 20)
        self.title_label.setText(
            self._title_metrics.elidedText(self._full_label, Qt.ElideRight, width)
        )
        if self._full_detail:
            self.detail_label.setText(
                self._detail_metrics.elidedText(self._full_detail, Qt.ElideRight, width)
            )


class CheckableTwoLineRow(QWidget):
    """A ``TwoLineRow`` with a checkbox in front: ticking adds the source to
    the plot (the Analysis reference drawer, stage 7c)."""

    toggled = Signal(bool)

    def __init__(self, label: str, detail: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(6, 0, 0, 0)
        layout.setSpacing(4)
        self.checkbox = QCheckBox(self)
        self.checkbox.toggled.connect(self.toggled)
        layout.addWidget(self.checkbox, 0, Qt.AlignVCenter)
        self.row = TwoLineRow(label, detail, self)
        layout.addWidget(self.row, 1)

    def is_checked(self) -> bool:
        return self.checkbox.isChecked()

    def set_checked_silently(self, checked: bool) -> None:
        blocked = self.checkbox.blockSignals(True)
        try:
            self.checkbox.setChecked(bool(checked))
        finally:
            self.checkbox.blockSignals(blocked)


__all__ = ["CheckableTwoLineRow", "TwoLineRow"]
