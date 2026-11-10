"""Mål's scale state after an objective change (owner's stage 8 test, 2026-11-10).

Changing the objective must recalculate against the scale stored for the
image, not Mål's in-memory scale, which can be stale; thumbnails show the new
objective; a field image after a microscope image shows no microscope tags;
the scale bar stays in the view's lower right corner when zoomed in. Real
``MainWindow`` in an isolated profile under ``tmp_path``; no network.
"""
from __future__ import annotations

import math
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF
from PySide6.QtGui import QColor, QPixmap
from PySide6.QtWidgets import QMessageBox

from database.models import ImageDB, MeasurementDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile

CROSS = [QPointF(100, 90), QPointF(130, 90), QPointF(115, 80), QPointF(115, 100)]  # 30 px and 20 px
OBJECTIVES = {
    "63X": {"name": "63X", "magnification": "63X", "microns_per_pixel": 0.05},
    "100X": {"name": "100X", "magnification": "100X", "microns_per_pixel": 0.03},
}


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def accept_prompts(monkeypatch):
    asked = []
    monkeypatch.setattr(QMessageBox, "exec", lambda box: asked.append(box.text()) or 0)
    monkeypatch.setattr(
        QMessageBox, "clickedButton",
        lambda box: next(b for b in box.buttons() if box.buttonRole(b) == QMessageBox.AcceptRole),
    )
    return asked


def _window(qtbot, monkeypatch, observation_id, image_id):
    from ui.main_window import MainWindow

    monkeypatch.setattr(MainWindow, "load_objective_definitions", lambda self: dict(OBJECTIVES))
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.current_image_id == image_id, timeout=5000)
    qtbot.wait(200)
    return window


def _microscope(profile, observation_id, name, objective, scale):
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg(f"{name}.JPG", seed=3)), "microscope",
        scale=scale, objective_name=objective, contrast="DIC",
    )
    MeasurementDB.add_measurement(image_id, 30 * scale, 20 * scale, measurement_type="spores", points=CROSS)
    return image_id


def _length(image_id):
    return MeasurementDB.get_measurements_for_image(image_id)[0]["length_um"]


def test_a_stale_scale_in_mal_does_not_skip_the_recalculation(qtbot, profile, monkeypatch, accept_prompts):
    """Image #8 of observation 991: Mål still held 63X's scale while the image was
    at 100X; choosing 63X wrote 63X but left the measurements at 100X."""
    observation_id = create_observation(profile)
    image_id = _microscope(profile, observation_id, "eight", "100X", 0.03)
    window = _window(qtbot, monkeypatch, observation_id, image_id)
    window.microns_per_pixel = 0.05  # stale: what another image had

    assert window.apply_objective({"key": "63X", **OBJECTIVES["63X"]})

    assert len(accept_prompts) == 1
    assert math.isclose(_length(image_id), 1.5)  # 30 px × 0.05, not still 0.9
    assert ImageDB.get_image(image_id)["scale_microns_per_pixel"] == 0.05


def test_the_thumbnail_shows_the_new_objective(qtbot, profile, monkeypatch, accept_prompts):
    observation_id = create_observation(profile)
    image_id = _microscope(profile, observation_id, "seven", "63X", 0.05)
    window = _window(qtbot, monkeypatch, observation_id, image_id)
    gallery = window.measure_gallery
    qtbot.waitUntil(lambda: bool(gallery._items), timeout=5000)

    def tag():
        return next(str(item.get("microscope_tag_text") or "") for item in gallery._items if item.get("id") == image_id)

    assert "63" in tag()
    window.apply_objective({"key": "100X", **OBJECTIVES["100X"]})
    qtbot.waitUntil(lambda: "100" in tag(), timeout=5000)
    assert window.current_image_id == image_id


def test_a_field_image_after_a_microscope_image_shows_no_microscope_tags(qtbot, profile, monkeypatch):
    observation_id = create_observation(profile)
    microscope = _microscope(profile, observation_id, "slide", "63X", 0.05)
    field = ImageDB.add_image(observation_id, str(profile.jpeg("field.JPG", seed=4)), "field")
    window = _window(qtbot, monkeypatch, observation_id, microscope)
    assert window.image_label.top_left_tags  # objective, contrast, stain

    window._on_measure_gallery_clicked(field, "")
    qtbot.waitUntil(lambda: window.current_image_id == field, timeout=5000)
    qtbot.wait(200)
    assert window.image_label.top_left_tags == []
    assert window.image_label.objective_text == ""


def test_the_scale_bar_stays_in_the_lower_right_corner_when_zoomed(qtbot):
    from ui.zoomable_image_widget import ZoomableImageLabel

    widget = ZoomableImageLabel()
    qtbot.addWidget(widget)
    widget.resize(600, 400)
    widget.show()
    qtbot.waitExposed(widget)
    pixmap = QPixmap(1200, 800)
    pixmap.fill(QColor(0, 0, 0))
    widget.set_image(pixmap)
    widget.set_microns_per_pixel(0.1)
    widget.set_scale_bar(True, 10.0)
    widget.set_view_state(QPointF(300, 200), 4.0)  # zoomed into the upper left: the image runs far past the view

    image = widget.grab().toImage()
    corner = image.pixelColor(widget.width() - 20, widget.height() - 16)
    assert corner.lightness() > 40  # the bar's light box, not the black image


@pytest.mark.parametrize(("zoom", "expected_um"), [(1.0, 10.0), (4.0, 5.0), (12.0, 2.0)])
def test_a_shortened_scale_bar_says_what_it_measures(qtbot, zoom, expected_um):
    """Capped to 60 % of the view, the bar steps down to a round length and labels that."""
    from ui.zoomable_image_widget import ZoomableImageLabel

    widget = ZoomableImageLabel()
    qtbot.addWidget(widget)
    widget.set_microns_per_pixel(0.1)
    widget.set_scale_bar(True, 10.0)
    widget.zoom_level = zoom
    microns, pixels = widget.scale_bar_for_width(600)
    assert microns == expected_um
    assert pixels == pytest.approx(microns / 0.1 * zoom)  # the bar is exactly that long
    assert pixels <= 360


def test_a_change_keeps_the_gallery_multi_selection(qtbot, profile, monkeypatch, accept_prompts):
    observation_id = create_observation(profile)
    first = _microscope(profile, observation_id, "one", "63X", 0.05)
    second = _microscope(profile, observation_id, "two", "63X", 0.05)
    window = _window(qtbot, monkeypatch, observation_id, first)
    gallery = window.measure_gallery
    qtbot.waitUntil(lambda: len(gallery._items) == 2, timeout=5000)
    paths = [ImageDB.get_image(first)["filepath"], ImageDB.get_image(second)["filepath"]]
    gallery.select_paths(paths, center=False)
    assert gallery.selected_keys() == {first, second}

    window.apply_objective({"key": "100X", **OBJECTIVES["100X"]})
    qtbot.waitUntil(
        lambda: "100" in next(str(i.get("microscope_tag_text") or "") for i in gallery._items if i.get("id") == first),
        timeout=5000,
    )
    assert gallery.selected_keys() == {first, second}
