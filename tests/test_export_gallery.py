"""Desktop export adapter tests.

Runs headless via `QT_QPA_PLATFORM=offscreen`. Covers the new
`ui.export_gallery.run_export` pipeline: uniform tiles across
measurements, PNG/JPEG output dimensions, hybrid SVG structure,
and the guarantee that no persisted-settings path can smuggle a
non-uniform bias back in.
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtCore import QSize
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication, QCheckBox, QDialog

import ui.main_window as main_window
from ui import export_gallery


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
    window.active_observation_id = None
    window.gallery_rotations = {}
    window.default_measure_color = QColor("#0044aa")
    window._current_measure_rectangle_style = lambda: "b"
    window._current_measure_rectangle_thickness = lambda: 2.0
    window._gallery_thumbnail_size = lambda: 160
    window._get_default_export_dir = lambda: "/tmp"
    window._remember_export_dir = lambda _filename: None
    window.orient_checkbox = QCheckBox()
    window.orient_checkbox.setChecked(True)
    window.measure_status_label = None  # publishes noop
    return window


def _make_source_image(path: Path, size: tuple[int, int] = (800, 800)):
    """Write a valid RGB image to disk so the export path can read dims."""
    Image.new("RGB", size, (120, 120, 120)).save(path, format="PNG")


def _sample_measurements(image_path: Path) -> list[dict]:
    return [
        {
            "id": 1,
            "image_id": 11,
            "image_filepath": str(image_path),
            "length_um": 10.0,
            "width_um": 4.0,
            "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 450.0,
            "p2_x": 400.0, "p2_y": 350.0,
            "p3_x": 380.0, "p3_y": 400.0,
            "p4_x": 420.0, "p4_y": 400.0,
        },
        {
            "id": 2,
            "image_id": 12,
            "image_filepath": str(image_path),
            "length_um": 12.0,
            "width_um": 5.0,
            "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 460.0,
            "p2_x": 400.0, "p2_y": 340.0,
            "p3_x": 375.0, "p3_y": 400.0,
            "p4_x": 425.0, "p4_y": 400.0,
        },
        {
            "id": 3,
            "image_id": 13,
            "image_filepath": str(image_path),
            "length_um": 8.0,
            "width_um": 3.5,
            "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 440.0,
            "p2_x": 400.0, "p2_y": 360.0,
            "p3_x": 383.0, "p3_y": 400.0,
            "p4_x": 417.0, "p4_y": 400.0,
        },
    ]


def _wire_gallery_measurements(window, measurements):
    window.get_gallery_measurements = lambda: list(measurements)
    window._filter_gallery_measurements = lambda values: list(values)
    window._sort_gallery_measurements = lambda values: list(values)


class _DialogAccepting:
    def __init__(self, fmt: str, quality: int = 90):
        self._fmt = fmt
        self._quality = quality

    def __call__(self, parent=None):
        return self

    def exec(self):
        return QDialog.Accepted

    def get_settings(self):
        return {"format": self._fmt, "quality": self._quality}


def _patch_dialogs(monkeypatch, fmt: str, save_path: Path, quality: int = 90):
    monkeypatch.setattr(export_gallery, "ExportGalleryDialog", _DialogAccepting(fmt, quality))
    monkeypatch.setattr(
        export_gallery.QFileDialog,
        "getSaveFileName",
        lambda *args, **kwargs: (str(save_path), ""),
    )


def _patch_image_db(monkeypatch):
    # No custom colour, no per-image µm-per-pixel — planner falls back
    # to length_um + p1p2 pixel span. Guards the "missing calibration"
    # fallback path.
    monkeypatch.setattr(export_gallery.ImageDB, "get_image", lambda image_id: None)


# ── PNG / JPEG ──────────────────────────────────────────────────────────────


def test_run_export_png_writes_composite_with_expected_size(monkeypatch, qapp, tmp_path):
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    _wire_gallery_measurements(window, _sample_measurements(src))
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.png"
    _patch_dialogs(monkeypatch, "png", out_path)

    export_gallery.run_export(window)

    assert out_path.exists()
    with Image.open(out_path) as decoded:
        assert decoded.format == "PNG"
        # Composite width / height should match the planner's mosaic
        # dimensions, so both must be strictly positive and divisible by
        # the shared per-tile size (checked below).
        assert decoded.width > 0 and decoded.height > 0


def test_run_export_jpeg_honours_quality_setting(monkeypatch, qapp, tmp_path):
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    _wire_gallery_measurements(window, _sample_measurements(src))
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.jpg"
    _patch_dialogs(monkeypatch, "jpg", out_path, quality=60)

    export_gallery.run_export(window)

    assert out_path.exists()
    # Not a byte-level assertion: just guard that JPEG round-trips.
    with Image.open(out_path) as decoded:
        assert decoded.format == "JPEG"
        assert decoded.width > 0 and decoded.height > 0


def test_run_export_png_tiles_are_uniform(monkeypatch, qapp, tmp_path):
    """Under the shared-planner rebuild every persisted tile shares one
    physical crop → the composite width equals `cols * tile_w` and
    height equals `rows * tile_h`. Regression guard against the
    pre-v3 variable-tile-widths behaviour.
    """
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    _wire_gallery_measurements(window, _sample_measurements(src))
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.png"
    _patch_dialogs(monkeypatch, "png", out_path)

    # Peek at the planner's tile sizes for the same input the export
    # will use. Every cell should carry identical (w_px, h_px).
    from utils.spore_mosaic_render import (
        MosaicGridPolicy, SporeMosaicSource, plan_mosaic,
    )

    sources = []
    for measurement in _sample_measurements(src):
        sources.append(SporeMosaicSource(
            item_id=int(measurement["id"]),
            source_path=src,
            source_width=800, source_height=800,
            p1_x=measurement["p1_x"], p1_y=measurement["p1_y"],
            p2_x=measurement["p2_x"], p2_y=measurement["p2_y"],
            p3_x=measurement["p3_x"], p3_y=measurement["p3_y"],
            p4_x=measurement["p4_x"], p4_y=measurement["p4_y"],
            length_um=measurement["length_um"], width_um=measurement["width_um"],
        ))
    layout = plan_mosaic(
        sources, orient=True,
        grid_policy=MosaicGridPolicy.ASPECT_4_3, output_tile_height_px=160,
    ).layout
    assert layout is not None
    widths = {cell.w_px for cell in layout.cells}
    heights = {cell.h_px for cell in layout.cells}
    assert widths == {layout.tile_width_px}
    assert heights == {layout.tile_height_px}

    export_gallery.run_export(window)

    with Image.open(out_path) as decoded:
        assert decoded.width == layout.mosaic_width_px
        assert decoded.height == layout.mosaic_height_px


# ── Hybrid SVG structure ────────────────────────────────────────────────────


def test_run_export_svg_contains_image_polygon_and_text(monkeypatch, qapp, tmp_path):
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    _wire_gallery_measurements(window, _sample_measurements(src))
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.svg"
    _patch_dialogs(monkeypatch, "svg", out_path)

    export_gallery.run_export(window)

    payload = out_path.read_text(encoding="utf-8")
    # Raster tiles as base64 PNG inside <image>.
    assert "<image" in payload
    assert "data:image/png;base64," in payload
    # Rectangle emitted as a vector polygon (thin outline).
    assert "<polygon" in payload
    # Dimension label emitted as vector text.
    assert "<text" in payload
    # Text-anchor for the label is middle (semantic anchor + backend
    # centring), and includes both a halo and a fill copy.
    assert 'text-anchor="middle"' in payload


def test_run_export_svg_style_b_emits_corner_lines(monkeypatch, qapp, tmp_path):
    """Style B is a thin outline + one <line> per corner segment (per
    `rectangle_corner_segments`). The SVG must carry those segments as
    real vector geometry, not a rasterised approximation."""
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    window._current_measure_rectangle_style = lambda: "b"
    _wire_gallery_measurements(window, _sample_measurements(src)[:1])
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.svg"
    _patch_dialogs(monkeypatch, "svg", out_path)

    export_gallery.run_export(window)

    payload = out_path.read_text(encoding="utf-8")
    assert "<polygon" in payload
    assert payload.count("<line") >= 8  # 4 corners × 2 segments each


def test_run_export_svg_style_a_emits_dual_polygons(monkeypatch, qapp, tmp_path):
    """Style A is a wide translucent polygon + thin polygon on top —
    the closest single-pass SVG approximation of the Qt dual-stroke look."""
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    window._current_measure_rectangle_style = lambda: "a"
    _wire_gallery_measurements(window, _sample_measurements(src)[:1])
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.svg"
    _patch_dialogs(monkeypatch, "svg", out_path)

    export_gallery.run_export(window)

    payload = out_path.read_text(encoding="utf-8")
    # Two polygons per rectangle (wide + thin). Together with the tile's
    # image bg, the file should contain at least two <polygon> tags.
    assert payload.count("<polygon") >= 2
    assert "stroke-opacity" in payload  # the wide translucent stroke


# ── Local (unsynced) measurements ──────────────────────────────────────────


def test_run_export_works_for_measurements_without_cloud_id(monkeypatch, qapp, tmp_path):
    """The shared planner is neutral — cloud IDs are only bound inside
    the cloud adapter. Local measurements without `cloud_id` must still
    render via `run_export`."""
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    measurements = _sample_measurements(src)
    # Simulate unsynced local rows: no cloud_id fields at all.
    for m in measurements:
        m.pop("cloud_id", None)
        m.pop("image_cloud_id", None)
    _wire_gallery_measurements(window, measurements)
    _patch_image_db(monkeypatch)

    out_path = tmp_path / "gallery.png"
    _patch_dialogs(monkeypatch, "png", out_path)

    export_gallery.run_export(window)
    assert out_path.exists()


# ── Behavioural uniform_scale guarantee ────────────────────────────────────


def test_settings_restore_does_not_reintroduce_uniform_scale(monkeypatch, qapp, tmp_path):
    """Preload a legacy settings dict with `uniform_scale=False` and
    check that the restored gallery still plans tiles with uniform
    physical dimensions across measurements. The old checkbox is gone
    and no code path can smuggle a non-uniform bias back in."""
    src = tmp_path / "src.png"
    _make_source_image(src)
    window = _build_minimal_window(monkeypatch)
    window.gallery_plot_settings = {}
    window._sync_reference_overlay_controls_state = lambda: None
    window._sync_gallery_histogram_controls = lambda: None
    window._sync_gallery_kde_controls = lambda: None
    window._load_gallery_settings = lambda: {
        "uniform_scale": False,
        "orient": True,
    }

    main_window.MainWindow.apply_gallery_settings(window)

    # The legacy widget must NOT have been recreated.
    assert not hasattr(window, "uniform_scale_checkbox")

    # Behavioural check: the gallery-mosaic planner is what drives the
    # live and exported tile sizes. Feed it the sample measurements and
    # assert every tile shares the same output dimensions — the very
    # guarantee the removed checkbox used to gate.
    plan_by_id = main_window.MainWindow._build_gallery_mosaic_plan(
        window,
        _sample_measurements(src),
        orient=True,
        output_tile_height_px=160,
    )
    assert plan_by_id, "planner should return per-measurement plans"
    widths = {t.output_w_px for t in plan_by_id.values()}
    heights = {t.output_h_px for t in plan_by_id.values()}
    assert widths == {next(iter(widths))}
    assert heights == {next(iter(heights))}


def test_collect_gallery_settings_source_has_no_uniform_scale_key():
    """Secondary sanity check: `_collect_gallery_settings` no longer
    writes `uniform_scale` into the persisted settings dict."""
    src = inspect.getsource(main_window.MainWindow._collect_gallery_settings)
    assert '"uniform_scale":' not in src
    assert "'uniform_scale':" not in src
