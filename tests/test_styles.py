from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QApplication

from ui.styles import apply_palette, get_style


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_light_mode_selection_text_uses_black(qapp) -> None:
    apply_palette("light")
    palette = QApplication.instance().palette()

    assert palette.color(QPalette.HighlightedText).name() == "#000000"
    assert "selection-color: #000000" in get_style("light")
