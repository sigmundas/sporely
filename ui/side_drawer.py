"""Slide-in side drawer for occasional tasks (UI redesign stage 6).

A ``SideDrawer`` overlays the right (or left) edge of a host widget without
resizing the host's layout. It holds named pages and shows one at a time
under a heading with a close button. It closes with Esc, with the close
button, or when the caller finishes the task (``close_drawer``). Opening and
closing slide over ``ANIMATION_MS``; the animation never blocks input.

The drawer follows the host's size through an event filter, so it always
spans the host's full height.
"""
from __future__ import annotations

from PySide6.QtCore import QEasingCurve, QEvent, QPoint, QPropertyAnimation, Qt, Signal
from PySide6.QtWidgets import QApplication
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QScrollArea,
    QStackedWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

ANIMATION_MS = 200
DEFAULT_WIDTH = 360


class SideDrawer(QFrame):
    opened = Signal(str)
    closed = Signal()

    def __init__(
        self, host: QWidget, *, side: str = "right", width: int = DEFAULT_WIDTH, escape_anywhere: bool = False,
    ) -> None:
        super().__init__(host)
        # Close on Esc wherever focus is in the window, before the page's own
        # Esc shortcuts (which would otherwise be ambiguous with a drawer one).
        self._escape_anywhere = bool(escape_anywhere)
        self._swallow_escape_press = False
        self._host = host
        self._side = "left" if side == "left" else "right"
        self._width = int(width)
        self._pages: dict[str, tuple[str, QWidget]] = {}
        self._current: str | None = None
        self._open = False
        self.setObjectName("sectionCard")  # the card look of both themes
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setFocusPolicy(Qt.StrongFocus)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        header = QWidget(self)
        header.setObjectName("sideDrawerHeader")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(14, 8, 8, 8)
        header_layout.setSpacing(6)
        self.title_label = QLabel(header)
        self.title_label.setObjectName("sectionHeader")
        header_layout.addWidget(self.title_label, 1)
        self.close_button = QToolButton(header)
        self.close_button.setText("✕")
        self.close_button.setAutoRaise(True)
        self.close_button.setToolTip(self.tr("Close"))
        self.close_button.setAccessibleName(self.tr("Close"))
        self.close_button.clicked.connect(self.close_drawer)
        header_layout.addWidget(self.close_button, 0)
        layout.addWidget(header)

        self._stack = QStackedWidget(self)
        self._scroll = QScrollArea(self)
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.NoFrame)
        # Normally the drawer is as wide as its page needs; only a host
        # narrower than that leaves a page to scroll sideways.
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._scroll.setWidget(self._stack)
        layout.addWidget(self._scroll, 1)

        self._animation = QPropertyAnimation(self, b"pos", self)
        self._animation.setDuration(ANIMATION_MS)
        self._animation.setEasingCurve(QEasingCurve.OutCubic)
        self._animation.finished.connect(self._on_animation_finished)

        host.installEventFilter(self)
        self.hide()

    # -- pages ---------------------------------------------------------------

    def add_page(self, key: str, title: str, widget: QWidget) -> None:
        self._pages[key] = (title, widget)
        self._stack.addWidget(widget)

    def set_page_title(self, key: str, title: str) -> None:
        if key in self._pages:
            self._pages[key] = (title, self._pages[key][1])
            if key == self._current:
                self.title_label.setText(title)

    def current_page(self) -> str | None:
        return self._current if self._open else None

    def is_open(self) -> bool:
        return self._open

    # -- opening and closing -------------------------------------------------

    def open_page(self, key: str) -> None:
        if key not in self._pages:
            return
        title, widget = self._pages[key]
        self._current = key
        self.title_label.setText(title)
        self._stack.setCurrentWidget(widget)
        if self._open:
            self._place(self._shown_pos())  # the new page may need another width
        if not self._open:
            self._open = True
            if self._escape_anywhere and QApplication.instance() is not None:
                QApplication.instance().installEventFilter(self)
            self._place(self._shown_pos() if not self.isVisible() else self.pos())
            self.show()
            self.raise_()
            self._slide(self._hidden_pos(), self._shown_pos())
        self.opened.emit(key)

    def toggle_page(self, key: str) -> None:
        """Open ``key``; close the drawer if ``key`` is already showing."""
        if self._open and self._current == key:
            self.close_drawer()
        else:
            self.open_page(key)

    def close_drawer(self) -> None:
        if not self._open:
            return
        self._open = False
        if not self._swallow_escape_press:
            self._remove_escape_filter()
        self._slide(self.pos(), self._hidden_pos())
        self.closed.emit()

    # -- geometry ------------------------------------------------------------

    def _drawer_width(self) -> int:
        """The preferred width, or wider when the page needs it; never wider than the host."""
        width = self._width
        page = self._stack.currentWidget()
        if page is not None:
            scrollbar = self._scroll.verticalScrollBar().sizeHint().width()
            width = max(width, page.minimumSizeHint().width() + scrollbar + 2 * self.frameWidth())
        return max(0, min(width, self._host.width()))

    def _shown_pos(self) -> QPoint:
        x = 0 if self._side == "left" else self._host.width() - self._drawer_width()
        return QPoint(x, 0)

    def _hidden_pos(self) -> QPoint:
        x = -self._drawer_width() if self._side == "left" else self._host.width()
        return QPoint(x, 0)

    def _place(self, pos: QPoint) -> None:
        self.setGeometry(pos.x(), pos.y(), self._drawer_width(), self._host.height())

    def _slide(self, start: QPoint, end: QPoint) -> None:
        self._animation.stop()
        self._place(start)
        if not self._host.isVisible():
            self._place(end)
            self._on_animation_finished()
            return
        self._animation.setStartValue(start)
        self._animation.setEndValue(end)
        self._animation.start()

    def _on_animation_finished(self) -> None:
        if not self._open:
            self.hide()

    def _remove_escape_filter(self) -> None:
        if self._escape_anywhere and QApplication.instance() is not None:
            QApplication.instance().removeEventFilter(self)

    def _escape_in_my_window(self, obj, event) -> bool:
        if event.key() != Qt.Key_Escape or event.modifiers() not in (Qt.NoModifier, Qt.KeypadModifier):
            return False
        return isinstance(obj, QWidget) and obj.window() is self.window()

    def eventFilter(self, obj, event) -> bool:  # noqa: N802 - Qt override
        kind = event.type()
        if self._escape_anywhere and kind in (QEvent.ShortcutOverride, QEvent.KeyPress):
            if kind == QEvent.ShortcutOverride and self._open and self._escape_in_my_window(obj, event):
                # Claim the key so no page shortcut fires, then close.
                event.accept()
                self._swallow_escape_press = True
                self.close_drawer()
                return True
            if self._swallow_escape_press and isinstance(obj, QWidget):
                # The Esc press that closed the drawer goes nowhere else.
                swallow = kind == QEvent.KeyPress and self._escape_in_my_window(obj, event)
                if swallow or event.key() != Qt.Key_Escape:
                    self._swallow_escape_press = False
                    if not self._open:
                        self._remove_escape_filter()
                if swallow:
                    return True
        if obj is self._host and event.type() == QEvent.Resize:
            self._animation.stop()
            if self._open:
                self._place(self._shown_pos())
            else:
                self._place(self._hidden_pos())
                self.hide()
        return super().eventFilter(obj, event)

    def keyPressEvent(self, event) -> None:  # noqa: N802 - Qt override
        if event.key() == Qt.Key_Escape and self._open:
            self.close_drawer()
            event.accept()
            return
        super().keyPressEvent(event)
