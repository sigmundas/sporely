from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt, QPointF, QRectF
from PySide6.QtGui import QFontMetrics, QColor, QImage, QPixmap
from PySide6.QtWidgets import QApplication, QCheckBox, QDialog, QFrame, QGridLayout, QScrollArea, QWidget

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


def test_analysis_gallery_selection_leaves_comfortable_thumbnail_alone(monkeypatch, qapp):
    # Clicking a thumbnail that's fully visible AND whose immediate
    # neighbours are also fully visible must not shift the strip. The old
    # "recenter when a neighbour < 25% visible" algorithm caused jumpy
    # behaviour on right-edge clicks.
    window = _build_minimal_window(monkeypatch)
    # 41, 42, 43 all fit inside viewport [300, 800] with room to spare.
    scroll, _, frames = _build_scroll_scene(
        [
            (41, 400, 120),
            (42, 520, 120),
            (43, 640, 120),
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


def test_analysis_gallery_selection_nudges_to_reveal_partial_neighbour(monkeypatch, qapp):
    # Clicking a fully-visible thumbnail whose previous neighbour is only
    # partially visible must nudge the strip left so the neighbour is
    # comfortably reachable on the next click.
    window = _build_minimal_window(monkeypatch)
    scroll, _, frames = _build_scroll_scene(
        [
            (41, 260, 120),  # 260..380 — starts left of viewport (300)
            (42, 380, 120),  # 380..500 — fully visible
            (43, 500, 120),  # 500..620 — fully visible
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

    # must_left = 260 (previous neighbour's left) → target = 260 - 24 = 236
    assert scroll.horizontalScrollBar().value() == 236

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


def test_measure_tab_prefers_selected_observation_thumbnail(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)
    calls: list[tuple[str, object]] = []

    window.active_observation_id = 7
    window.active_observation_name = "Observation 7"
    window.current_image_id = 2
    window.observation_images = [
        {"id": 2, "filepath": "/tmp/image-2.jpg"},
        {"id": 5, "filepath": "/tmp/image-5.jpg"},
    ]
    window.refresh_observation_images = lambda select_image_id=None, force_refresh=False: calls.append(
        ("refresh", select_image_id)
    )
    window.update_measurements_table = lambda: calls.append(("table", None))
    window.goto_image_index = lambda index: calls.append(("goto", index))
    window.measure_button = SimpleNamespace(
        setEnabled=lambda enabled: calls.append(("measure", bool(enabled)))
    )
    window.observations_tab = SimpleNamespace(
        get_selected_observation=lambda: (7, "Observation 7"),
        selected_observation_id=7,
        _image_browser_observation_id=7,
        image_browser=SimpleNamespace(current_image_id=lambda: 5),
    )

    main_window.MainWindow.on_tab_changed(window, 1)

    assert calls == [
        ("refresh", 5),
        ("table", None),
        ("goto", 1),
        ("measure", True),
    ]

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


def test_analysis_gallery_link_switches_tab_before_loading_source_image(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)
    calls = []
    measurement = {
        "id": 42,
        "image_id": 7,
        "p1_x": 100.0,
        "p1_y": 120.0,
        "p2_x": 160.0,
        "p2_y": 120.0,
        "p3_x": 160.0,
        "p3_y": 150.0,
        "p4_x": 100.0,
        "p4_y": 150.0,
    }

    window.measurement_active = False
    window.current_image_id = 2
    window._get_measurement_by_id = lambda measurement_id: measurement
    window.tab_widget = SimpleNamespace(
        setCurrentIndex=lambda index: calls.append(("tab", index))
    )
    window.load_image_record = lambda image_data, refresh_table=True: calls.append(
        ("load", image_data["id"])
    )
    window.select_measurement_in_table = lambda measurement_id: calls.append(
        ("select", measurement_id)
    )
    window._focus_measurement_in_image = lambda selected: calls.append(
        ("focus", selected["id"])
    )

    monkeypatch.setattr(main_window.ImageDB, "get_image", lambda image_id: {"id": image_id})
    monkeypatch.setattr(main_window.QTimer, "singleShot", lambda _delay, callback: callback())

    main_window.MainWindow._open_measurement_from_gallery_impl(window, 42)

    assert calls == [
        ("tab", 1),
        ("load", 7),
        ("select", 42),
        ("focus", 42),
    ]

    window.deleteLater()


def test_analysis_gallery_link_focuses_viewer_on_measurement_at_fifty_percent(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)
    window.current_image_id = 7
    window.image_label = main_window.ZoomableImageLabel()
    window.image_label.resize(900, 600)
    window.image_label.set_image(QPixmap(2400, 1600))
    measurement = {
        "id": 42,
        "image_id": 7,
        "p1_x": 100.0,
        "p1_y": 150.0,
        "p2_x": 200.0,
        "p2_y": 150.0,
        "p3_x": 200.0,
        "p3_y": 200.0,
        "p4_x": 100.0,
        "p4_y": 200.0,
    }

    main_window.MainWindow._focus_measurement_in_image(window, measurement)

    view = window.image_label.get_view_state()
    assert view is not None
    assert view["center"] == QPointF(150.0, 175.0)
    assert view["zoom"] == pytest.approx(0.5)

    window.image_label.deleteLater()
    window.deleteLater()


def test_new_observation_analysis_gallery_defaults_are_enabled(monkeypatch, qapp):
    """Fresh observations default to `orient=True`. The old
    `uniform_scale_checkbox` was removed as part of the shared-planner
    rebuild — uniform physical scale is now mandatory for every
    persisted output, not a toggle."""
    window = _build_minimal_window(monkeypatch)
    window.orient_checkbox = QCheckBox()
    window.orient_checkbox.setChecked(False)
    window._load_gallery_settings = lambda: {}

    main_window.MainWindow.apply_gallery_settings(window)

    assert window.orient_checkbox.isChecked()
    assert not hasattr(window, "uniform_scale_checkbox")

    window.deleteLater()


def test_apply_gallery_settings_drops_stale_uniform_scale_key(monkeypatch, qapp):
    """Legacy settings dicts may still carry `uniform_scale=False`.
    The restore path must silently drop the key without recreating the
    checkbox or persisting a non-uniform bias anywhere on the window."""
    import inspect

    window = _build_minimal_window(monkeypatch)
    window.orient_checkbox = QCheckBox()
    window.orient_checkbox.setChecked(False)
    window.gallery_plot_settings = {}
    window._sync_reference_overlay_controls_state = lambda: None
    window._sync_gallery_histogram_controls = lambda: None
    window._sync_gallery_kde_controls = lambda: None
    window._load_gallery_settings = lambda: {
        "uniform_scale": False,
        "orient": True,
    }

    main_window.MainWindow.apply_gallery_settings(window)

    assert window.orient_checkbox.isChecked()
    # No checkbox / attribute survives that would tell the gallery to
    # skip the shared-planner uniform crop.
    assert not hasattr(window, "uniform_scale_checkbox")
    # The `apply_gallery_settings` source no longer references the
    # legacy key at all — this belt-and-braces check locks it in.
    src = inspect.getsource(main_window.MainWindow.apply_gallery_settings)
    assert "settings.get(\"uniform_scale\"" not in src
    assert "settings.get('uniform_scale'" not in src

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


def test_export_gallery_composite_delegates_to_export_gallery_module(
    monkeypatch, qapp, tmp_path,
):
    """`main_window.export_gallery_composite` is a thin stub around
    `ui.export_gallery.run_export`. This test guards the plumbing —
    the actual export behaviour (uniform tiles, hybrid SVG, etc.) is
    covered by `tests/test_export_gallery.py`.
    """
    from ui import export_gallery

    window = _build_minimal_window(monkeypatch)

    calls: list[object] = []
    monkeypatch.setattr(export_gallery, "run_export", lambda mw: calls.append(mw))

    window.export_gallery_composite()

    assert calls == [window]
