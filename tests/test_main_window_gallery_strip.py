from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt, QRectF
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import QApplication, QFrame, QGridLayout, QScrollArea, QWidget

import ui.main_window as main_window
from ui.image_gallery_widget import center_horizontal_scroll_target


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _DummySpeciesAvailability:
    def __init__(self, *args, **kwargs):
        pass


def _build_minimal_window(monkeypatch) -> main_window.MainWindow:
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummySpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    window = main_window.MainWindow()
    window._refresh_analysis_gallery_frame_state = lambda *args, **kwargs: None
    window.update_graph_plots_only = lambda: None
    window.gallery_selected_measurement_id = None
    window._gallery_thumbnail_frames = {}
    return window


def _build_scroll_scene(frame_specs):
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    container = QWidget()
    container.setMinimumWidth(max(x + width for _, x, width in frame_specs) + 400)
    container.setMinimumHeight(120)
    scroll.setWidget(container)
    scroll.resize(500, 140)
    scroll.show()

    frames = {}
    for key, x, width in frame_specs:
        frame = QFrame(container)
        frame.setGeometry(x, 0, width, 120)
        frames[key] = frame

    return scroll, container, frames


def test_analysis_gallery_selection_recenters_near_edge_thumbnail(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)

    scroll, container, frames = _build_scroll_scene(
        [
            (41, 530, 120),
            (42, 650, 120),
            (43, 780, 120),
        ]
    )
    qapp.processEvents()

    window.gallery_scroll = scroll
    window._gallery_thumbnail_frames = frames
    scroll.horizontalScrollBar().setValue(300)
    qapp.processEvents()

    expected = center_horizontal_scroll_target(
        QRectF(scroll.horizontalScrollBar().value(), 0, scroll.viewport().width(), scroll.viewport().height()),
        QRectF(frames[42].geometry()),
        scroll.horizontalScrollBar().minimum(),
        scroll.horizontalScrollBar().maximum(),
        QRectF(frames[41].geometry()),
        QRectF(frames[43].geometry()),
    )

    window._select_analysis_gallery_measurement(42, update_plot=False)
    qapp.processEvents()
    qapp.processEvents()

    value = scroll.horizontalScrollBar().value()
    assert value == expected

    window.deleteLater()


def test_analysis_gallery_clear_hides_and_detaches_existing_widgets(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)

    container = QWidget()
    grid = QGridLayout(container)
    window.gallery_container = container
    window.gallery_grid = grid

    first = QFrame(container)
    second = QFrame(container)
    grid.addWidget(first, 0, 0)
    grid.addWidget(second, 0, 1)

    window._gallery_thumbnail_frames = {1: first, 2: second}
    window._gallery_thumbnail_labels = {1: first, 2: second}
    window._gallery_measurement_lookup = {1: {"id": 1}, 2: {"id": 2}}
    window._gallery_thumbnail_render_state = {"ready": True}
    window._gallery_render_total_width = 240
    window._gallery_render_max_height = 120
    window._gallery_center_request_generation = 7
    window._gallery_center_request_id = 2

    window._clear_analysis_gallery_widgets()

    assert grid.count() == 0
    assert first.parent() is None
    assert second.parent() is None
    assert first.isHidden()
    assert second.isHidden()
    assert window._gallery_thumbnail_frames == {}
    assert window._gallery_thumbnail_labels == {}
    assert window._gallery_measurement_lookup == {}
    assert window._gallery_thumbnail_render_state is None
    assert window._gallery_render_total_width == 0
    assert window._gallery_render_max_height == 0
    assert window._gallery_center_request_generation == 8
    assert window._gallery_center_request_id is None

    window.deleteLater()


def test_thumbnail_label_position_is_bottom_centered(qapp):
    metrics = QFontMetrics(qapp.font())
    text_width = 80
    tile_width = 200
    tile_height = 180

    text_x, text_y = main_window._thumbnail_label_position(
        tile_width,
        tile_height,
        text_width,
        metrics,
    )

    assert text_x == 60
    assert text_y == max(metrics.ascent() + 4, tile_height - metrics.descent() - 4)
