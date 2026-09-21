"""The Add-reference picker's Library row: taxon, citation, measurement, badges.

A sibling of :class:`~ui.two_line_row.TwoLineRow` rather than a replacement for
it. ``TwoLineRow`` is a title/detail row and stays in use on the
My-observations and Community tabs; the Library list has a different anatomy
that those tabs do not share:

* the **taxon** is the headline — italic, visually primary, and the thing the
  user is actually comparing against;
* the **measurement expression** is right-aligned monospace on the same line
  and reserves its width *first*. Truncating ``8.5–10.8 × 4.5–5.8 µm`` to make
  a long publication title fit would hide the only number on the row, so the
  taxon elides and the measurement never does;
* the **citation** drops to a smaller grey second line, where it is metadata
  about the claim rather than the claim itself;
* two **badges** sit beside the citation. They answer different questions and
  are deliberately not merged: the relevance badge says how close this source
  is to the taxon in the picker, and the semantic badge (built from
  :mod:`references.reference_display`) says what the source actually contains.

The row also owns the selected-state painting. Qt's default blue selection
highlight is switched off on the list (see
``AddReferenceDialog._build_library_tab``) because selection here means "shown
in the preview", which is a weaker statement than the checkbox's "will be
added to the plot" and should not be the loudest thing on screen. The approved
treatment is a narrow green left bar plus a pale green fill, painted here so
the row keeps it whether or not the list has focus.
"""
from __future__ import annotations

from PySide6.QtCore import QSize, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontDatabase, QPainter
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

#: The picker's selected-row accent, matching the success/confirmation green
#: already used across the desktop UI (``#27ae60``).
SELECTED_BAR_COLOR = "#27ae60"
#: Pale fill behind a selected row. Alpha rather than a fixed pastel so the
#: same value reads correctly on the light and dark themes the review renderer
#: captures.
SELECTED_FILL_COLOR = QColor(39, 174, 96, 38)
#: Width in pixels of the left bar. Narrow on purpose: it marks the preview
#: row, it does not compete with the checkbox.
SELECTED_BAR_WIDTH = 3


class _ElidedLabel(QLabel):
    """A label that elides its own text to whatever width the layout gave it.

    Elides against its own width, not the row's, so a label that shares a line
    with a fixed-width sibling shrinks by exactly the space that sibling took.
    """

    #: Width the label asks for, in characters. Small on purpose: the full
    #: text must not become a width demand, or a long publication title
    #: would push the row wider than the list and the measurement cell it
    #: shares a line with would be the thing pushed out of view.
    _REQUESTED_CHARS = 8

    def __init__(self, text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._full_text = text
        self.setMinimumWidth(0)
        # Expanding, so the label takes the width its fixed-size siblings
        # left over rather than only what it asked for.
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.setText(text)

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(
            self.fontMetrics().averageCharWidth() * self._REQUESTED_CHARS,
            super().sizeHint().height(),
        )

    def minimumSizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(0, super().minimumSizeHint().height())

    def full_text(self) -> str:
        return self._full_text

    def set_full_text(self, text: str) -> None:
        self._full_text = text
        self._apply_elision()

    def _apply_elision(self) -> None:
        width = max(self.width(), 0)
        if width <= 0:
            super().setText(self._full_text)
            return
        super().setText(
            self.fontMetrics().elidedText(self._full_text, Qt.ElideRight, width)
        )

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().resizeEvent(event)
        self._apply_elision()


class _BadgeLabel(QLabel):
    """A small rounded pill. Purely presentational; the wording is the caller's."""

    def __init__(self, text: str, *, color: str, parent: QWidget | None = None) -> None:
        super().__init__(text, parent)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.setStyleSheet(
            f"color: {color};"
            f" border: 1px solid {color};"
            " border-radius: 6px;"
            " padding: 0px 5px;"
            " font-size: 10px;"
        )


class LibrarySourceRow(QWidget):
    """One measurement set in the Library list.

    ``check_toggled`` carries the row's measurement-set id so the dialog does
    not have to map a sender back to a candidate. The checkbox is driven
    programmatically during every list rebuild, so
    :meth:`set_checked_silently` exists to set it without re-emitting — a
    repopulate is not a user decision and must not look like one.
    """

    check_toggled = Signal(str, bool)

    def __init__(
        self,
        *,
        measurement_set_id: str,
        taxon: str,
        citation: str,
        measurement: str,
        relevance_badge: str = "",
        semantic_badge: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._measurement_set_id = measurement_set_id
        self._selected = False

        root = QHBoxLayout(self)
        root.setContentsMargins(SELECTED_BAR_WIDTH + 5, 3, 6, 3)
        root.setSpacing(6)

        self.checkbox = QCheckBox(self)
        self.checkbox.toggled.connect(self._on_toggled)
        root.addWidget(self.checkbox, 0, Qt.AlignVCenter)

        column = QVBoxLayout()
        column.setContentsMargins(0, 0, 0, 0)
        column.setSpacing(1)
        root.addLayout(column, 1)

        first_line = QHBoxLayout()
        first_line.setContentsMargins(0, 0, 0, 0)
        first_line.setSpacing(8)
        self.taxon_label = _ElidedLabel(taxon, self)
        taxon_font = QFont(self.taxon_label.font())
        taxon_font.setItalic(True)
        self.taxon_label.setFont(taxon_font)
        first_line.addWidget(self.taxon_label, 1)

        self.measurement_label = QLabel(measurement, self)
        measurement_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        # systemFont() returns the platform's fixed family but leaves the
        # fixed-pitch hint unset, so say it explicitly: the digits in
        # "8.5–10.8 × 4.5–5.8" only line up down the column if the fallback
        # is also monospaced.
        measurement_font.setFixedPitch(True)
        measurement_font.setStyleHint(QFont.Monospace)
        measurement_font.setPointSizeF(max(self.font().pointSizeF() - 1.0, 7.0))
        self.measurement_label.setFont(measurement_font)
        self.measurement_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        # Fixed, and added with stretch 0: the layout hands this label its
        # full sizeHint before the taxon gets anything, which is what
        # "reserve its width first" means.
        self.measurement_label.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        self.measurement_label.setVisible(bool(measurement))
        first_line.addWidget(self.measurement_label, 0)
        column.addLayout(first_line)

        second_line = QHBoxLayout()
        second_line.setContentsMargins(0, 0, 0, 0)
        second_line.setSpacing(5)
        self.citation_label = _ElidedLabel(citation, self)
        self.citation_label.setStyleSheet("color: #7f8c8d; font-size: 11px;")
        self.citation_label.setVisible(bool(citation))
        second_line.addWidget(self.citation_label, 1)

        self.relevance_badge = _BadgeLabel(relevance_badge, color="#2980b9", parent=self)
        self.relevance_badge.setVisible(bool(relevance_badge))
        second_line.addWidget(self.relevance_badge, 0)
        self.semantic_badge = _BadgeLabel(semantic_badge, color="#7f8c8d", parent=self)
        self.semantic_badge.setVisible(bool(semantic_badge))
        second_line.addWidget(self.semantic_badge, 0)
        column.addLayout(second_line)

    # -- state ---------------------------------------------------------

    @property
    def measurement_set_id(self) -> str:
        return self._measurement_set_id

    def is_checked(self) -> bool:
        return self.checkbox.isChecked()

    def set_checked_silently(self, checked: bool) -> None:
        was_blocked = self.checkbox.blockSignals(True)
        try:
            self.checkbox.setChecked(checked)
        finally:
            self.checkbox.blockSignals(was_blocked)

    def is_selected(self) -> bool:
        return self._selected

    def set_selected(self, selected: bool) -> None:
        if self._selected == bool(selected):
            return
        self._selected = bool(selected)
        self.update()

    def _on_toggled(self, checked: bool) -> None:
        self.check_toggled.emit(self._measurement_set_id, bool(checked))

    # -- painting ------------------------------------------------------

    def paintEvent(self, event) -> None:  # noqa: N802 - Qt override
        if self._selected:
            painter = QPainter(self)
            painter.fillRect(self.rect(), SELECTED_FILL_COLOR)
            painter.fillRect(
                0, 0, SELECTED_BAR_WIDTH, self.height(), QColor(SELECTED_BAR_COLOR)
            )
            painter.end()
        super().paintEvent(event)


class LibraryResultsList(QListWidget):
    """A results list that keeps its item widgets inside its viewport.

    ``QListWidget.setItemWidget`` sizes a row widget from the item's rect,
    but the reverse also happens: the view derives its content width from
    the widgets it holds. Once a row has been laid out wide, shrinking the
    viewport leaves the widget at its old width and the view simply clips
    it — permanently, because the stale width is what the next content-width
    calculation reads. Everything on the right of the row (the measurement
    cell, the badges) then disappears with no scrollbar to reveal it, and
    widening the pane again does not bring it back.

    Pinning each row to the viewport width breaks that feedback loop in both
    directions: the cap stops a row outgrowing the viewport, and the resize
    makes it follow the viewport back out when the pane is widened again.
    """

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().resizeEvent(event)
        self._fit_row_widgets()

    def _fit_row_widgets(self) -> None:
        available = self.viewport().width()
        if available <= 0:
            return
        for row in range(self.count()):
            widget = self.itemWidget(self.item(row))
            if widget is None:
                continue
            widget.setMaximumWidth(available)
            widget.resize(available, widget.height())

    def setItemWidget(self, item, widget) -> None:  # noqa: N802 - Qt override
        available = self.viewport().width()
        if available > 0:
            widget.setMaximumWidth(available)
        super().setItemWidget(item, widget)
        if available > 0:
            widget.resize(available, widget.height())


__all__ = [
    "LibraryResultsList",
    "LibrarySourceRow",
    "SELECTED_BAR_COLOR",
    "SELECTED_BAR_WIDTH",
    "SELECTED_FILL_COLOR",
]
