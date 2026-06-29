from __future__ import annotations

import os

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ui.spore_preview_widget import PreviewImageLabel
from ui.zoomable_image_widget import ZoomableImageLabel


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.mark.parametrize("widget_type", [ZoomableImageLabel, PreviewImageLabel])
def test_cursor_helper_skips_redundant_updates(qapp, monkeypatch, widget_type):
    widget = widget_type()
    calls: list = []

    def fake_set_cursor(self, cursor):
        calls.append(cursor.shape())

    monkeypatch.setattr(widget_type, "setCursor", fake_set_cursor, raising=False)

    widget._cursor_shape = None
    widget._set_cursor_shape(Qt.ArrowCursor)
    widget._set_cursor_shape(Qt.ArrowCursor)
    widget._set_cursor_shape(Qt.CrossCursor)
    widget._set_cursor_shape(Qt.CrossCursor)
    widget._set_cursor_shape(Qt.ArrowCursor)

    assert calls == [Qt.ArrowCursor, Qt.CrossCursor, Qt.ArrowCursor]
