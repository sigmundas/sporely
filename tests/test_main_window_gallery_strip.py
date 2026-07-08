from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt, QRectF
from PySide6.QtGui import QFontMetrics, QColor, QImage, QPixmap
from PySide6.QtWidgets import QApplication, QDialog, QFrame, QGridLayout, QScrollArea, QWidget

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
    window.update_image_navigation_ui = lambda: None
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


def test_analysis_gallery_selection_nudges_offscreen_thumbnail_into_view(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)

    # Frame 42 is placed off the right edge of the viewport (viewport is
    # 500px wide at scroll=100 → visible x range 100..600, but frame 42
    # spans 620..740). Selecting it should nudge the strip just enough to
    # reveal it, not recenter it or leave it hidden.
    scroll, container, frames = _build_scroll_scene(
        [
            (41, 500, 120),
            (42, 620, 120),
            (43, 750, 120),
        ]
    )
    qapp.processEvents()

    window.gallery_scroll = scroll
    window._gallery_thumbnail_frames = frames
    scroll.horizontalScrollBar().setValue(100)
    qapp.processEvents()

    expected = center_horizontal_scroll_target(
        QRectF(scroll.horizontalScrollBar().value(), 0, scroll.viewport().width(), scroll.viewport().height()),
        QRectF(frames[42].geometry()),
        scroll.horizontalScrollBar().minimum(),
        scroll.horizontalScrollBar().maximum(),
        QRectF(frames[41].geometry()),
        QRectF(frames[43].geometry()),
    )
    assert expected is not None  # sanity: item is off-screen so a scroll IS required

    window._select_analysis_gallery_measurement(42, update_plot=False)
    qapp.processEvents()
    qapp.processEvents()

    assert scroll.horizontalScrollBar().value() == expected

    window.deleteLater()


def test_analysis_gallery_selection_leaves_visible_thumbnail_alone(monkeypatch, qapp):
    # Regression: clicking a thumbnail that's already fully visible must
    # not shift the strip. Old algorithm recentered whenever a neighbour
    # was less than 25% visible, causing jumpy behaviour on right-edge
    # clicks.
    window = _build_minimal_window(monkeypatch)
    scroll, _, frames = _build_scroll_scene(
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

    window._select_analysis_gallery_measurement(42, update_plot=False)
    qapp.processEvents()
    qapp.processEvents()

    assert scroll.horizontalScrollBar().value() == 300  # unchanged

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


def test_refresh_observation_images_skips_measure_gallery_rebuild_when_unchanged(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)

    images = [
        {"id": 1, "filepath": "/tmp/a.jpg"},
        {"id": 2, "filepath": "/tmp/b.jpg"},
    ]

    class _GallerySpy:
        def __init__(self):
            self.calls = []

        def set_observation_id(self, observation_id):
            self.calls.append(("set_observation_id", observation_id))

        def select_image(self, image_id):
            self.calls.append(("select_image", image_id))

        def clear(self):
            self.calls.append(("clear", None))

    window.measure_gallery = _GallerySpy()
    window.active_observation_id = 7
    window.current_image_id = 2
    window.current_image_index = 0
    window.observation_images = list(images)
    window._measure_gallery_observation_id = 7
    window._measure_gallery_signature = tuple((img["id"], img["filepath"]) for img in images)

    monkeypatch.setattr(main_window.ImageDB, "get_images_for_observation", lambda observation_id: list(images))
    monkeypatch.setattr(window, "_apply_measure_gallery_publish_selection", lambda: None)

    refreshed = window.refresh_observation_images(select_image_id=2)

    assert refreshed is False
    assert window.measure_gallery.calls == [("select_image", 2)]
    assert window.current_image_index == 1
    assert window._measure_gallery_observation_id == 7
    assert window._measure_gallery_signature == tuple((img["id"], img["filepath"]) for img in images)

    window.deleteLater()


def test_analysis_gallery_selection_does_not_restyle_frame(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)
    window.gallery_selected_measurement_id = 42

    class _AnalysisFrame(QFrame):
        def __init__(self):
            super().__init__()
            self.style_calls = 0
            self.selected_calls = []
            self.hover_calls = []

        def setStyleSheet(self, *args, **kwargs):
            self.style_calls += 1

        def set_measure_selected(self, selected: bool):
            self.selected_calls.append(bool(selected))

        def set_measure_hovered(self, hovered: bool):
            self.hover_calls.append(bool(hovered))

    frame = _AnalysisFrame()
    window._gallery_thumbnail_frames = {42: frame}
    monkeypatch.setattr(window, "_apply_analysis_gallery_frame_glow", lambda *args, **kwargs: None)

    main_window.MainWindow._refresh_analysis_gallery_frame_state(window, 42)

    assert frame.style_calls == 0
    assert frame.selected_calls == [True]
    assert frame.hover_calls == [False]

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


def test_export_gallery_composite_uses_widest_thumbnail_width(monkeypatch, qapp, tmp_path):
    window = _build_minimal_window(monkeypatch)
    window.active_observation_id = None
    window.gallery_rotations = {}
    window.default_measure_color = QColor("#0044aa")
    window.measure_status_label = SimpleNamespace(
        setText=lambda _text: None,
        setStyleSheet=lambda _style: None,
    )
    window._get_default_export_dir = lambda: str(tmp_path)
    window._remember_export_dir = lambda _filename: None
    window._current_measure_rectangle_style = lambda: "a"
    window._current_measure_rectangle_thickness = lambda: 1

    measurements = [
        {
            "id": 1,
            "image_id": 11,
            "length_um": 6.2,
            "width_um": 4.6,
            "gallery_rotation": 0,
            "p1_x": 0.0,
            "p1_y": 0.0,
            "p2_x": 1.0,
            "p2_y": 0.0,
            "p3_x": 1.0,
            "p3_y": 1.0,
            "p4_x": 0.0,
            "p4_y": 1.0,
        },
        {
            "id": 2,
            "image_id": 12,
            "length_um": 6.8,
            "width_um": 4.8,
            "gallery_rotation": 0,
            "p1_x": 0.0,
            "p1_y": 0.0,
            "p2_x": 1.0,
            "p2_y": 0.0,
            "p3_x": 1.0,
            "p3_y": 1.0,
            "p4_x": 0.0,
            "p4_y": 1.0,
        },
    ]
    window.get_gallery_measurements = lambda: list(measurements)
    window._filter_gallery_measurements = lambda values: list(values)
    window._sort_gallery_measurements = lambda values: list(values)
    window.get_measurement_pixmap = lambda measurement, pixmap_cache: QPixmap(64, 64)
    monkeypatch.setattr(main_window.ImageDB, "get_image", lambda image_id: None)

    thumb1 = QPixmap(40, 80)
    thumb1.fill(QColor("#66aaff"))
    thumb2 = QPixmap(70, 80)
    thumb2.fill(QColor("#ff9966"))

    def _fake_create_spore_thumbnail(
        pixmap,
        points,
        length_um,
        width_um,
        size,
        measurement_num=0,
        **kwargs,
    ):
        return thumb1 if measurement_num == 1 else thumb2

    window.create_spore_thumbnail = _fake_create_spore_thumbnail

    class _FakeExportGalleryDialog:
        def __init__(self, parent=None):
            self.parent = parent

        def exec(self):
            return QDialog.Accepted

        def get_settings(self):
            return {"format": "png", "quality": 90}

    output_path = tmp_path / "gallery.png"
    monkeypatch.setattr(main_window, "ExportGalleryDialog", _FakeExportGalleryDialog)
    monkeypatch.setattr(
        main_window.QFileDialog,
        "getSaveFileName",
        lambda *args, **kwargs: (str(output_path), "PNG Images (*.png)"),
    )

    window.export_gallery_composite()

    image = QImage(str(output_path))
    assert image.width() == 140
    assert image.height() == 80
