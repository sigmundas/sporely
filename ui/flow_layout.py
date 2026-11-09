"""A layout that places widgets left to right and wraps to new rows (Qt's flow layout example)."""
from __future__ import annotations

from PySide6.QtCore import QMargins, QPoint, QRect, QSize, Qt
from PySide6.QtWidgets import QLayout, QLayoutItem, QSizePolicy, QWidget, QWidgetItem


class FlowLayout(QLayout):
    """Lays out visible items in rows, wrapping when the next item would not fit."""

    def __init__(self, parent: QWidget | None = None, *, spacing: int = 4) -> None:
        super().__init__(parent)
        self._items: list[QLayoutItem] = []
        self._spacing = int(spacing)
        self.setContentsMargins(QMargins(0, 0, 0, 0))

    def insert_widget(self, index: int, widget: QWidget) -> None:
        """Insert ``widget`` at ``index`` (``addWidget`` always appends)."""
        self.addChildWidget(widget)
        self._items.insert(max(0, min(int(index), len(self._items))), QWidgetItem(widget))
        self.invalidate()

    def index_of_widget(self, widget: QWidget) -> int:
        for index, item in enumerate(self._items):
            if item.widget() is widget:
                return index
        return -1

    # QLayout interface ------------------------------------------------
    def addItem(self, item: QLayoutItem) -> None:  # type: ignore[override]
        self._items.append(item)

    def count(self) -> int:  # type: ignore[override]
        return len(self._items)

    def itemAt(self, index: int) -> QLayoutItem | None:  # type: ignore[override]
        if 0 <= index < len(self._items):
            return self._items[index]
        return None

    def takeAt(self, index: int) -> QLayoutItem | None:  # type: ignore[override]
        if 0 <= index < len(self._items):
            return self._items.pop(index)
        return None

    def spacing(self) -> int:  # type: ignore[override]
        return self._spacing

    def setSpacing(self, spacing: int) -> None:  # type: ignore[override]
        self._spacing = int(spacing)
        self.invalidate()

    def expandingDirections(self) -> Qt.Orientations:  # type: ignore[override]
        return Qt.Orientations(0)

    def hasHeightForWidth(self) -> bool:  # type: ignore[override]
        return True

    def heightForWidth(self, width: int) -> int:  # type: ignore[override]
        return self._do_layout(QRect(0, 0, width, 0), test_only=True)

    def setGeometry(self, rect: QRect) -> None:  # type: ignore[override]
        super().setGeometry(rect)
        self._do_layout(rect, test_only=False)

    def sizeHint(self) -> QSize:  # type: ignore[override]
        return self.minimumSize()

    def minimumSize(self) -> QSize:  # type: ignore[override]
        size = QSize()
        for item in self._items:
            if not item.isEmpty():
                size = size.expandedTo(item.minimumSize())
        margins = self.contentsMargins()
        return size + QSize(margins.left() + margins.right(), margins.top() + margins.bottom())

    # ------------------------------------------------------------------
    def _do_layout(self, rect: QRect, *, test_only: bool) -> int:
        margins = self.contentsMargins()
        area = rect.adjusted(margins.left(), margins.top(), -margins.right(), -margins.bottom())
        x, y = area.x(), area.y()
        line_height = 0
        for item in self._items:
            if item.isEmpty():  # hidden widgets take no room
                continue
            hint = item.sizeHint()
            # An item wider than the row gets the row's width (its text is
            # clipped) rather than running past the edge.
            hint.setWidth(max(0, min(hint.width(), area.width())))
            next_x = x + hint.width() + self._spacing
            if next_x - self._spacing > area.right() + 1 and line_height > 0:
                x = area.x()
                y += line_height + self._spacing
                next_x = x + hint.width() + self._spacing
                line_height = 0
            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), hint))
            x = next_x
            line_height = max(line_height, hint.height())
        return y + line_height - rect.y() + margins.bottom()


def fixed_chip_policy() -> QSizePolicy:
    """Size policy for a chip: its own width, its own height."""
    return QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
