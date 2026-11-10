"""Objective ring colours (ISO 8578:2012 Table 2) and the style tokens of stage P3."""
from __future__ import annotations

import math
import os
import re

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QLabel

from ui.adaptive_choice_selector import objective_color, stain_color
from ui.image_gallery_widget import ImageGalleryWidget
from ui.main_window import MainWindow
from ui.styles import (
    OBJECTIVE_RING_BANDS,
    OBJECTIVE_RING_MACRO,
    OBJECTIVE_RING_UNKNOWN,
    RADIUS,
    REAGENT_COLORS,
    _COLOR_TOKENS,
    get_button_icon_color,
    get_design_tokens,
    get_style,
    objective_ring_color,
    objective_ring_outline,
    theme_colors,
)

BLACK, GREY, BROWN, RED, ORANGE, YELLOW = "#202124", "#9aa0a6", "#8b5a2b", "#e74c3c", "#f39c12", "#f1c40f"
LIGHT_GREEN, DARK_GREEN, LIGHT_BLUE, DARK_BLUE, WHITE = "#2ecc71", "#1e8449", "#3498db", "#1f4ea8", "#f7f1e5"


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


@pytest.mark.parametrize(
    ("magnification", "expected"),
    [
        (1, BLACK), (1.25, BLACK),
        (1.6, GREY), (2, GREY),
        (2.5, BROWN), (3.2, BROWN),
        (4, RED), (5, RED),
        (6.3, ORANGE), (8, ORANGE),
        (10, YELLOW), (12.5, YELLOW),
        (16, LIGHT_GREEN), (20, LIGHT_GREEN),
        (25, DARK_GREEN), (32, DARK_GREEN),
        (40, LIGHT_BLUE), (50, LIGHT_BLUE),
        (63, DARK_BLUE), (80, DARK_BLUE),
        (100, WHITE), (125, WHITE), (160, WHITE),
    ],
)
def test_every_standard_magnification_has_its_iso_8578_ring(magnification, expected):
    assert objective_ring_color(magnification) == expected


@pytest.mark.parametrize(
    ("magnification", "expected"),
    [
        (60, DARK_BLUE),    # 63/60 = 1.05 is nearer than 60/50 = 1.2
        (6, ORANGE),        # 6.3
        (9, YELLOW),        # 10/9 = 1.11 beats 9/8 = 1.125
        (30, DARK_GREEN),   # 32
        (35, DARK_GREEN),   # 35/32 = 1.09 beats 40/35 = 1.14
        (45, LIGHT_BLUE),   # 40 or 50, both light blue
        (150, WHITE),
        (0.5, BLACK),       # below the table: first band
        (1000, WHITE),      # above the table: last band
    ],
)
def test_in_between_magnifications_take_the_nearest_band_on_a_log_scale(magnification, expected):
    assert objective_ring_color(magnification) == expected


def test_a_tie_between_two_bands_goes_to_the_higher_magnification():
    assert objective_ring_color(math.sqrt(50 * 63)) == DARK_BLUE
    assert objective_ring_color(math.sqrt(32 * 40)) == LIGHT_BLUE
    assert objective_ring_color(math.sqrt(50 * 63) * 0.999) == LIGHT_BLUE


@pytest.mark.parametrize("value", [None, 0, -4, "abc", float("nan"), float("inf")])
def test_unusable_magnifications_get_the_unknown_colour(value):
    assert objective_ring_color(value) == OBJECTIVE_RING_UNKNOWN


def test_objective_color_uses_the_ring_band_unless_the_objective_stores_a_colour():
    assert objective_color({"magnification": 25.0}) == DARK_GREEN
    assert objective_color({"magnification": 60.0, "name": "60X oil"}) == DARK_BLUE
    assert objective_color({"magnification": 100.0, "plot_color": "#123456"}) == "#123456"
    assert objective_color({"optics_type": "macro", "magnification": 2.0}) == OBJECTIVE_RING_MACRO
    assert objective_color(None, "1:2") == OBJECTIVE_RING_MACRO
    assert objective_color(None, None) == OBJECTIVE_RING_UNKNOWN


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("4x", RED), ("10X", YELLOW), ("25x", DARK_GREEN), ("60x", DARK_BLUE),
        ("63X/1.40 Oil", DARK_BLUE), ("100X/1.25 Plan achro", WHITE), ("100x DIC", WHITE),
        ("6.3x", ORANGE), ("1:2", OBJECTIVE_RING_MACRO), ("", OBJECTIVE_RING_UNKNOWN),
        (None, OBJECTIVE_RING_UNKNOWN), ("Custom", OBJECTIVE_RING_UNKNOWN),
    ],
)
def test_main_window_objective_tag_colours(qapp, name, expected):
    assert MainWindow._objective_color_for_name(None, name).name() == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [("NA 0.65 40X Plan", LIGHT_BLUE), ("NA 1.25 100X Plan achro", WHITE), ("Plan 0.25 10x", YELLOW)],
)
def test_a_name_that_starts_with_its_na_still_reads_its_magnification(name, expected):
    assert objective_color(None, name) == expected
    assert objective_color({"name": name}) == expected


class _MeasureStub:
    """Just enough of MainWindow for apply_objective's objective-tag path."""

    def __init__(self):
        from ui.zoomable_image_widget import ZoomableImageLabel

        self.image_label = ZoomableImageLabel()
        self.microns_per_pixel = 0.1
        self.current_objective = None
        self.current_objective_name = None
        self.current_pixmap = object()
        self.current_image_id = None

    def tr(self, text):
        return text

    def _maybe_rescale_current_image(self, *_args, **_kwargs):
        return True

    def __getattr__(self, name):
        if name in {"_objective_tag_text", "_objective_color_for_name"}:
            return getattr(MainWindow, name).__get__(self)
        return lambda *args, **kwargs: None


@pytest.mark.parametrize(
    ("objective", "expected_tag", "expected_color"),
    [
        # The short tag for a 1:2 macro profile reads "1X"; the colour must still
        # come from the profile (neutral grey), not from "1X" (a black 1x ring).
        ({"key": "1:2 Canon MP-E 65mm", "name": "1:2 Canon MP-E 65mm", "optics_type": "macro",
          "magnification": 2.0, "microns_per_pixel": 2.0}, "1X", OBJECTIVE_RING_MACRO),
        ({"key": "100X", "name": "100X/1.25 Plan achro", "magnification": 100.0,
          "microns_per_pixel": 0.03}, "100X", WHITE),
        ({"key": "40X", "name": "40X", "magnification": 40.0, "color": "#123456",
          "microns_per_pixel": 0.08}, "40X", "#123456"),
    ],
)
def test_measure_objective_tag_colour_comes_from_the_objective_profile(qapp, objective, expected_tag, expected_color):
    stub = _MeasureStub()
    assert MainWindow.apply_objective(stub, objective) is True
    assert stub.image_label.objective_text == expected_tag
    assert stub.image_label.objective_color.name() == expected_color


def test_only_black_and_white_rings_get_an_outline():
    outlined = {color for _n, color, _v in OBJECTIVE_RING_BANDS if objective_ring_outline(color)}
    assert outlined == {BLACK, WHITE}
    assert objective_ring_outline(OBJECTIVE_RING_MACRO) is None
    assert objective_ring_outline("not a colour") is None


def test_black_and_white_rings_stand_out_in_both_themes(qapp):
    from PySide6.QtGui import QColor

    def contrast(a, b):
        def lum(c):
            def ch(v):
                v /= 255
                return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4
            return 0.2126 * ch(c.red()) + 0.7152 * ch(c.green()) + 0.0722 * ch(c.blue())
        la, lb = sorted((lum(QColor(a)), lum(QColor(b))), reverse=True)
        return (la + 0.05) / (lb + 0.05)

    # A black or white ring must stand out in both themes: through its fill
    # where the surface is the opposite tone, through its outline where it
    # is the same. Non-text UI parts need 3:1 (WCAG 1.4.11).
    for ring in (BLACK, WHITE):
        outline = objective_ring_outline(ring)
        for theme in ("light", "dark"):
            for surface in (theme_colors(theme)["surface"], theme_colors(theme)["bg"]):
                best = max(contrast(ring, surface), contrast(outline, surface))
                assert best >= 3.0, (ring, theme, surface)
    assert contrast(objective_ring_outline(WHITE), theme_colors("light")["surface"]) >= 3.0
    assert contrast(objective_ring_outline(BLACK), theme_colors("dark")["surface"]) >= 3.0


def test_gallery_badge_for_a_white_ring_has_an_outline_and_a_blue_one_has_none(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_items(
        [
            {"id": 1, "filepath": str(tmp_path / "a.jpg"), "microscope_tag_text": "100x", "microscope_tag_color": WHITE},
            {"id": 2, "filepath": str(tmp_path / "b.jpg"), "microscope_tag_text": "40x", "microscope_tag_color": LIGHT_BLUE},
        ]
    )
    widget.resize(400, 180)
    widget.show()
    qapp.processEvents()
    labels = {label.text(): label for frame in widget._frames for label in frame.findChildren(QLabel)}
    assert f"border: 1px solid {objective_ring_outline(WHITE)};" in labels["100x"].styleSheet()
    assert "border: none;" in labels["40x"].styleSheet()


def test_an_outlined_compact_badge_keeps_the_size_of_other_badges(qapp, tmp_path):
    widget = ImageGalleryWidget("Images", thumbnail_size=84)
    widget.set_compact_overlay(True)
    widget.set_items(
        [
            {"id": 1, "filepath": str(tmp_path / "a.jpg"), "microscope_tag_text": "100x BF", "microscope_tag_color": WHITE},
            {"id": 2, "filepath": str(tmp_path / "b.jpg"), "microscope_tag_text": "100x BF", "microscope_tag_color": RED},
        ]
    )
    widget.resize(300, 140)
    widget.show()
    qapp.processEvents()
    outlined, plain = (
        next(label for label in frame.findChildren(QLabel) if label.text() == "100x BF") for frame in widget._frames
    )
    assert "1px solid" in outlined.styleSheet() and "border: none;" in plain.styleSheet()
    assert outlined.sizeHint() == plain.sizeHint()
    assert outlined.maximumWidth() == plain.maximumWidth()


def test_stain_colours_come_from_the_reagent_tokens():
    assert stain_color("Melzer") == REAGENT_COLORS["melzer"]
    assert stain_color("Congo red") == REAGENT_COLORS["congored"]
    assert stain_color("chlorazol_black-E") == REAGENT_COLORS["chlorazolblacke"]
    assert stain_color("water") is None


def test_both_themes_define_the_same_colour_tokens():
    assert set(_COLOR_TOKENS["light"]) == set(_COLOR_TOKENS["dark"])


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_stylesheet_is_rendered_from_tokens(qapp, theme):
    style = get_style(theme)
    colors = theme_colors(theme)
    assert "{" in style and "}" in style
    assert not re.search(r"\{[a-z_]+\}", style), "unfilled token placeholder"
    assert f"background-color: {colors['bg']};" in style
    assert f"border-radius: {RADIUS['md']};" in style
    tokens = get_design_tokens(theme)
    assert all(tokens[name] == colors[name] for name in tokens)
    assert get_button_icon_color("primaryButton", theme) == colors["primary_button_text"]
    assert get_button_icon_color("destructiveButton", theme) == colors["destructive_fg"]
    assert get_button_icon_color(None, theme) == colors["text"]
