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


def test_light_mode_selection_text_uses_slate(qapp) -> None:
    apply_palette("light")
    palette = QApplication.instance().palette()

    assert palette.color(QPalette.HighlightedText).name() == "#1e293b"
    assert "selection-color: #1e293b" in get_style("light")


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_both_themes_define_a_link_colour_readable_on_their_base(qapp, theme) -> None:
    """Widgets asking the palette for an accent must not get Qt's #0000ff.

    Neither theme used to set ``QPalette.Link``, so the reference picker's
    "+ New publication…" row and the conflict dialog's per-image status line
    fell back to Qt's default pure blue. On the dark theme's #1c1b1b base
    that is dark-on-dark and effectively unreadable.
    """
    apply_palette(theme)
    palette = QApplication.instance().palette()

    link = palette.color(QPalette.Link)
    assert link.name() != "#0000ff"

    def luminance(colour):
        channels = []
        for raw in (colour.redF(), colour.greenF(), colour.blueF()):
            channels.append(
                raw / 12.92 if raw <= 0.03928 else ((raw + 0.055) / 1.055) ** 2.4
            )
        red, green, blue = channels
        return 0.2126 * red + 0.7152 * green + 0.0722 * blue

    for role in (QPalette.Base, QPalette.Window):
        lighter, darker = sorted(
            (luminance(link), luminance(palette.color(role))), reverse=True
        )
        contrast = (lighter + 0.05) / (darker + 0.05)
        assert contrast >= 3.0, f"{theme} link vs {role}: {contrast:.2f}"
