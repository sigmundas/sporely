"""The Summary tab renders the comparison, and the Raw spores tab stays honest.

Companion to ``tests/test_reference_comparison.py``: that file pins the model,
this one pins what the widgets do with it inside the real Add-reference dialog
— which body of the Summary tab is on screen, what the captions actually say,
and that a source without individual measurements produces an explanation
rather than rows.

These are content assertions, not layout ones. A screenshot would show that
the bands are drawn; only a test can show that the band labelled "typical
range" is the one the source actually described that way.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication



@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(geometry, "QSettings", settings, raising=False)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


# --- No selection -------------------------------------------------------------


# --- Selected sources ---------------------------------------------------------


def _relative_luminance(color) -> float:
    def channel(value: int) -> float:
        srgb = value / 255.0
        return srgb / 12.92 if srgb <= 0.03928 else ((srgb + 0.055) / 1.055) ** 2.4

    return (
        0.2126 * channel(color.red())
        + 0.7152 * channel(color.green())
        + 0.0722 * channel(color.blue())
    )


def _contrast_ratio(foreground, background) -> float:
    first, second = _relative_luminance(foreground), _relative_luminance(background)
    lighter, darker = max(first, second), min(first, second)
    return (lighter + 0.05) / (darker + 0.05)


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_captions_stay_readable_in_both_shipped_themes(theme):
    """The captions are the pane's scientific content, not decoration.

    They are the only thing separating a min-max band from a 5-95% one and
    the only place an absent centre is stated, so they have to be legible.
    They were styled ``color: palette(mid)`` — a frame-shading role, not a
    text role — which came out at 1.4:1 in the light theme and 1.5:1 in the
    dark one, effectively invisible in both.
    """
    from PySide6.QtGui import QPalette

    from ui.reference_comparison_view import caption_color
    from ui.styles import apply_palette

    app = _app()
    try:
        apply_palette(theme)
        palette = app.palette()
        ratio = _contrast_ratio(
            caption_color(palette), palette.color(QPalette.Window)
        )
        assert ratio >= 4.5, f"{theme}: {ratio:.2f}:1"
    finally:
        apply_palette("light")


# --- Raw spores ---------------------------------------------------------------


# --- The legacy table is still reachable for the unmigrated tabs --------------


# --- Community and My observations on the same contract ----------------------
#
# Stage 2: the two non-Library source tabs feed the one comparison model. What
# these pin is not that a band appears but *which claim* each tab is allowed to
# make -- a community aggregate of bare extremes must not acquire 5-95
# semantics, and a personal observation's own spores must be labelled as
# measured rather than as something published.


# --- Community: range summaries ----------------------------------------------


# --- Community: raw points ----------------------------------------------------


# --- My observations ----------------------------------------------------------


# --- Source switching ---------------------------------------------------------


# --- The shared preview has one owner at a time ------------------------------
#
# Community search and detail both complete asynchronously, so a response can
# land after the user has moved to another source tab. These pin that a late
# response never repaints a tab it does not belong to, that the response is
# still kept, and that no half-loaded state leaves the previous source's
# method, calibration, provenance or spore rows under a new source's name.


