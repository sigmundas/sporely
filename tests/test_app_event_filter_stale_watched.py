"""Application-wide event filters tolerate a ``watched`` that is not a QObject.

A filter installed on the QApplication sees every receiver. PySide can hand it
a stale wrapper of another type for an object Qt has just destroyed (seen in
CI as a ``QWidgetItem``). ``QObject.eventFilter`` rejects that with a
TypeError, which pytest-qt reports as an error in whichever test happens to be
running, and which the running app prints from its event loop.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QKeyEvent
from PySide6.QtWidgets import QWidget, QWidgetItem

from tests.test_edit_mode import rediger  # noqa: F401
from tests.test_files_source import profile, window  # noqa: F401
from ui.side_drawer import SideDrawer


def _stale_watched(qtbot) -> QWidgetItem:
    """A live object of the type CI saw in place of the receiver."""
    widget = QWidget()
    qtbot.addWidget(widget)
    return QWidgetItem(widget)


def _events() -> list[QEvent]:
    return [
        QEvent(QEvent.User),
        QKeyEvent(QEvent.KeyPress, Qt.Key_Escape, Qt.NoModifier),
        QKeyEvent(QEvent.ShortcutOverride, Qt.Key_Escape, Qt.NoModifier),
    ]


def test_the_drawer_filter_ignores_a_watched_that_is_not_a_qobject(qtbot):
    host = QWidget()
    qtbot.addWidget(host)
    drawer = SideDrawer(host, escape_anywhere=True)
    drawer.add_page("page", "Page", QWidget())
    drawer.open_page("page")  # installs the application-wide filter
    watched = _stale_watched(qtbot)

    for event in _events():
        assert drawer.eventFilter(watched, event) is False
    assert drawer.is_open()


def test_the_edit_page_filter_ignores_a_watched_that_is_not_a_qobject(qtbot, window, rediger):
    watched = _stale_watched(qtbot)

    for event in _events():
        assert rediger.eventFilter(watched, event) is False
