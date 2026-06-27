from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from PIL import Image
from PySide6.QtCore import QEvent, QPointF, Qt
from PySide6.QtGui import QColor, QPalette, QPixmap
from PySide6.QtWidgets import QApplication, QSizePolicy, QSlider

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from tools import plot_image_processing_curve as curve_tool
from ui.zoomable_image_widget import ZoomableImageLabel


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _make_low_contrast_16bit_tiff(path: Path) -> None:
    ramp = np.linspace(12000, 18000, 64, dtype=np.uint16).reshape(8, 8)
    Image.fromarray(ramp).save(path, format="TIFF")


def _make_large_gradient_tiff(path: Path, *, width: int = 2400, height: int = 1600) -> None:
    ramp = np.tile(np.linspace(0, 65535, width, dtype=np.uint16), (height, 1))
    Image.fromarray(ramp).save(path, format="TIFF")


def test_parse_args_without_input_path_launches_blank_interactive_mode():
    args = curve_tool.parse_args([])

    assert args.input_path is None
    assert args.interactive is True


def test_plot_image_processing_curve_script_creates_outputs(tmp_path):
    input_path = tmp_path / "low_contrast.tiff"
    plot_out = tmp_path / "curve_plot.png"
    preview_out = tmp_path / "curve_preview.png"
    _make_low_contrast_16bit_tiff(input_path)

    script_path = Path(__file__).resolve().parents[1] / "tools" / "plot_image_processing_curve.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            str(input_path),
            "--auto-levels",
            "--contrast",
            "18",
            "--shadows",
            "35",
            "--highlights",
            "-20",
            "--dark-cutoff",
            "0.01",
            "--bright-cutoff",
            "0.01",
            "--tone-curve",
            "--curve-strength",
            "0.50",
            "--curve-midpoint",
            "0.50",
            "--plot-out",
            str(plot_out),
            "--preview-out",
            str(preview_out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert plot_out.exists()
    assert preview_out.exists()
    assert plot_out.stat().st_size > 0
    assert preview_out.stat().st_size > 0

    with Image.open(preview_out) as preview:
        assert preview.format == "PNG"
        assert preview.mode == "RGB"
        assert preview.size == (8, 8)

    assert "settings JSON:" in result.stdout
    assert '"contrast": 0.18' in result.stdout
    assert '"shadows": 0.35' in result.stdout
    assert '"highlights": -0.2' in result.stdout
    assert '"tone_curve_enabled": true' in result.stdout
    assert '"preview_mode": "preview-resized"' in result.stdout
    assert '"dark_point":' in result.stdout
    assert '"light_point":' in result.stdout
    assert "snapshot preview bucket: 8px" in result.stdout


def test_processing_curve_window_refreshes_preview_and_graph(tmp_path, qapp):
    input_path = tmp_path / "low_contrast.tiff"
    _make_low_contrast_16bit_tiff(input_path)

    window = curve_tool.ProcessingCurveWindow(input_path)
    assert window.preview_label.original_pixmap is not None
    assert window.canvas is not None
    assert window.canvas.sizePolicy().horizontalPolicy() == QSizePolicy.Expanding
    assert window.canvas.sizePolicy().verticalPolicy() == QSizePolicy.Expanding
    assert len(window.figure.axes) == 1
    assert window.figure.axes[0].get_title() == "Processing curve"
    assert window.figure.axes[0].get_xlabel() == "Input luminance"
    assert window.figure.axes[0].get_ylabel() == "Output luminance"
    assert isinstance(window.preview_label, ZoomableImageLabel)
    assert window.preview_label.pan_without_shift is True
    assert isinstance(window.dark_slider, QSlider)
    assert isinstance(window.light_slider, QSlider)
    assert isinstance(window.dark_cutoff_slider, QSlider)
    assert isinstance(window.bright_cutoff_slider, QSlider)
    assert isinstance(window.contrast_slider, QSlider)
    assert isinstance(window.shadows_slider, QSlider)
    assert isinstance(window.highlights_slider, QSlider)
    assert isinstance(window.curve_strength_slider, QSlider)
    assert isinstance(window.curve_midpoint_slider, QSlider)
    assert window.dark_slider.minimum() == 0
    assert window.dark_slider.maximum() == 1000
    assert window.light_slider.minimum() == 0
    assert window.light_slider.maximum() == 1000
    assert window.contrast_slider.minimum() == -100
    assert window.contrast_slider.maximum() == 100
    assert window.shadows_slider.minimum() == -100
    assert window.shadows_slider.maximum() == 100
    assert window.highlights_slider.minimum() == -100
    assert window.highlights_slider.maximum() == 100
    assert window.dark_cutoff_slider.minimum() == 0
    assert window.dark_cutoff_slider.maximum() == 20
    assert window.bright_cutoff_slider.maximum() == 20
    assert window.auto_levels_checkbox.isChecked() is True
    assert window.tone_curve_checkbox.isChecked() is False
    assert window.auto_black_label.text() != "—"
    assert window.auto_white_label.text() != "—"

    window.auto_levels_checkbox.setChecked(False)
    window.dark_slider.setValue(100)
    window.light_slider.setValue(900)
    window.contrast_slider.setValue(18)
    window.shadows_slider.setValue(35)
    window.highlights_slider.setValue(-20)
    window.tone_curve_checkbox.setChecked(True)
    window.curve_strength_slider.setValue(75)
    window.curve_midpoint_slider.setValue(35)
    window._refresh_outputs()

    assert window.dark_value_label.text() == "0.100"
    assert window.light_value_label.text() == "0.900"
    assert window.contrast_value_label.text() == "+18"
    assert window.shadows_value_label.text() == "+35"
    assert window.highlights_value_label.text() == "-20"
    assert window.curve_strength_value_label.text() == "0.75"
    assert window.curve_midpoint_value_label.text() == "0.35"
    assert "Camera WB" in window.settings_label.text()
    assert "auto levels off" in window.settings_label.text()
    assert "dark point 0.100" in window.settings_label.text()
    assert "light point 0.900" in window.settings_label.text()
    assert "contrast +18" in window.settings_label.text()
    assert "shadows/highlights +35 / -20" in window.settings_label.text()
    assert "tone curve on" in window.settings_label.text()
    assert "preview" in window.settings_label.text()

    axes = window.figure.axes[0]
    lines = {line.get_label(): line for line in axes.lines}
    assert {"identity", "after levels", "after contrast", "after shadows/highlights", "final with tone curve", "tone nodes"}.issubset(lines)

    curve_cache = window._get_curve_cache(window._settings)
    assert curve_cache is not None
    curve = curve_cache.curve
    assert np.allclose(lines["identity"].get_xdata(), curve.input_values)
    assert np.allclose(lines["identity"].get_ydata(), curve.input_values)
    assert np.allclose(lines["after levels"].get_ydata(), curve.levels_output)
    assert np.allclose(lines["after contrast"].get_ydata(), curve.contrast_output)
    assert np.allclose(lines["after shadows/highlights"].get_ydata(), curve.shadow_highlight_output)
    assert np.allclose(lines["final with tone curve"].get_ydata(), curve.final_output)
    assert np.allclose(lines["tone nodes"].get_xdata(), [curve.debug.dark_point, curve.debug.light_point])
    assert np.allclose(lines["tone nodes"].get_ydata(), [0.0, 1.0])


def test_processing_curve_window_shows_compact_curve_preview(tmp_path, qapp):
    input_path = tmp_path / "low_contrast.tiff"
    _make_low_contrast_16bit_tiff(input_path)

    window = curve_tool.ProcessingCurveWindow(input_path)
    assert window.curve_preview_widget is not None
    assert window.curve_preview_widget.width() == window.curve_preview_widget.height()
    assert window.curve_preview_widget.width() > 0

    curve = window.curve_preview_widget.current_curve()
    assert curve is not None
    histogram = window.curve_preview_widget.current_histogram()
    assert histogram is not None
    assert histogram.size == curve_tool._TONE_HISTOGRAM_BINS
    assert float(histogram.min()) >= 0.0
    assert float(histogram.max()) <= 1.0
    curve_cache = window._get_curve_cache(window._settings)
    assert curve_cache is not None
    expected_curve = curve_cache.curve

    assert np.allclose(curve.input_values, expected_curve.input_values)
    assert np.allclose(curve.final_output, expected_curve.final_output)
    assert curve.debug.dark_point == pytest.approx(expected_curve.debug.dark_point, abs=1e-6)
    assert curve.debug.light_point == pytest.approx(expected_curve.debug.light_point, abs=1e-6)


def test_compact_curve_preview_tracks_palette_changes(qapp):
    widget = curve_tool.CompactToneCurvePreview()
    palette = widget.palette()
    palette.setColor(QPalette.Window, QColor("#101010"))
    palette.setColor(QPalette.Base, QColor("#202020"))
    palette.setColor(QPalette.Mid, QColor("#505050"))
    palette.setColor(QPalette.Text, QColor("#eeeeee"))
    palette.setColor(QPalette.Highlight, QColor("#4d7c7a"))
    widget.setPalette(palette)

    widget.changeEvent(QEvent(QEvent.Type.PaletteChange))

    assert widget._theme_colors["window"].name() == "#101010"
    assert widget._theme_colors["plot"].name() == "#202020"
    assert widget._theme_colors["histogram"].alpha() > 0


def test_zoomable_preview_refresh_preserves_view_state(qapp):
    label = ZoomableImageLabel()
    label.setFixedSize(200, 200)

    first_pixmap = QPixmap(100, 100)
    first_pixmap.fill(Qt.red)
    label.set_image_sources(first_pixmap, "/tmp/source_one.jpg")
    label.set_view_state(QPointF(25, 30), 2.0)
    before = label.get_view_state()
    assert before is not None

    second_pixmap = QPixmap(100, 100)
    second_pixmap.fill(Qt.blue)
    label.set_image_sources(second_pixmap, "/tmp/source_one.jpg", preserve_view=True)
    after = label.get_view_state()
    assert after is not None
    assert after["zoom"] == pytest.approx(before["zoom"])
    assert after["center"].x() == pytest.approx(before["center"].x())
    assert after["center"].y() == pytest.approx(before["center"].y())


def test_zoomable_preview_tag_is_set_for_scaled_sources(qapp):
    label = ZoomableImageLabel()
    pixmap = QPixmap(40, 40)
    pixmap.fill(Qt.red)

    label.set_image_sources(pixmap, None, preview_scaled=True)

    assert label._preview_tag_text == "Preview"

    label.set_image(QPixmap())

    assert label._preview_tag_text == ""


def test_processing_curve_transfer_mapping_matches_stages(qapp):
    ramp = np.linspace(0.0, 1.0, 256, dtype=np.float64)
    rgb = np.repeat(ramp[None, :, None], 3, axis=2)
    settings = curve_tool.TestToneSettings(
        dark_point=0.25,
        light_point=0.75,
        auto_levels=False,
        contrast=0.25,
        shadows=0.4,
        highlights=-0.2,
        tone_curve_enabled=False,
    )

    processed_rgb, debug = curve_tool.apply_post_decode_processing(rgb, settings, return_debug=True)
    curve = curve_tool.compute_post_decode_transfer_curve(rgb, settings, debug=debug)

    assert curve.debug.dark_point == pytest.approx(0.25, abs=1e-6)
    assert curve.debug.light_point == pytest.approx(0.75, abs=1e-6)
    assert curve.levels_output[0] == pytest.approx(0.0, abs=1e-6)
    assert curve.levels_output[-1] == pytest.approx(1.0, abs=1e-6)
    assert np.all(np.diff(curve.levels_output) >= -1e-9)
    assert np.all(np.diff(curve.contrast_output) >= -1e-9)
    assert np.all(np.diff(curve.shadow_highlight_output) >= -1e-9)
    assert curve.shadow_highlight_output[0] == pytest.approx(0.0, abs=1e-6)
    assert curve.shadow_highlight_output[-1] == pytest.approx(1.0, abs=1e-6)
    assert curve.final_output[0] == pytest.approx(0.0, abs=1e-6)
    assert curve.final_output[-1] == pytest.approx(1.0, abs=1e-6)
    assert float(processed_rgb.min()) >= 0.0
    assert float(processed_rgb.max()) <= 1.0


@pytest.mark.parametrize(
    ("shadows", "highlights"),
    [
        (-1.0, 0.0),
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, -1.0),
        (0.0, 1.0),
    ],
)
def test_shadow_highlight_curve_stays_pinned_and_monotone(qapp, shadows, highlights):
    ramp = np.linspace(0.0, 1.0, 256, dtype=np.float64)
    rgb = np.repeat(ramp[None, :, None], 3, axis=2)
    settings = curve_tool.TestToneSettings(
        dark_point=0.25,
        light_point=0.75,
        auto_levels=False,
        contrast=0.25,
        shadows=shadows,
        highlights=highlights,
        tone_curve_enabled=False,
    )

    curve = curve_tool.compute_post_decode_transfer_curve(rgb, settings)
    output = curve.shadow_highlight_output

    assert output[0] == pytest.approx(0.0, abs=1e-6)
    assert output[-1] == pytest.approx(1.0, abs=1e-6)
    assert np.all(output >= -1e-9)
    assert np.all(output <= 1.0 + 1e-9)
    assert np.all(np.diff(output) >= -1e-9)


def test_high_positive_highlights_do_not_create_white_hump(qapp):
    ramp = np.linspace(0.0, 1.0, 256, dtype=np.float64)
    rgb = np.repeat(ramp[None, :, None], 3, axis=2)
    settings = curve_tool.TestToneSettings(
        dark_point=0.0,
        light_point=1.0,
        auto_levels=False,
        contrast=0.0,
        shadows=0.0,
        highlights=0.88,
        tone_curve_enabled=False,
    )

    curve = curve_tool.compute_post_decode_transfer_curve(rgb, settings)
    output = curve.shadow_highlight_output

    assert output[0] == pytest.approx(0.0, abs=1e-6)
    assert output[-1] == pytest.approx(1.0, abs=1e-6)
    assert np.all(np.diff(output) >= -1e-9)


def test_high_positive_shadows_do_not_move_black_endpoint(qapp):
    ramp = np.linspace(0.0, 1.0, 256, dtype=np.float64)
    rgb = np.repeat(ramp[None, :, None], 3, axis=2)
    settings = curve_tool.TestToneSettings(
        dark_point=0.0,
        light_point=1.0,
        auto_levels=False,
        contrast=0.0,
        shadows=0.88,
        highlights=0.0,
        tone_curve_enabled=False,
    )

    curve = curve_tool.compute_post_decode_transfer_curve(rgb, settings)
    output = curve.shadow_highlight_output

    assert output[0] == pytest.approx(0.0, abs=1e-6)
    assert output[-1] == pytest.approx(1.0, abs=1e-6)
    assert np.all(np.diff(output) >= -1e-9)
    assert output[1] > output[0]


def test_processing_curve_window_keeps_preview_bucket_capped_when_zooming(tmp_path, qapp):
    input_path = tmp_path / "large.tiff"
    _make_large_gradient_tiff(input_path)

    window = curve_tool.ProcessingCurveWindow(input_path)
    initial_width = window.preview_label.original_pixmap.width()
    initial_height = window.preview_label.original_pixmap.height()
    assert initial_width <= 1600

    window.preview_label.set_view_state(
        QPointF(initial_width / 2.0, initial_height / 2.0),
        3.0,
    )
    zoomed_before = window.preview_label.get_view_state()
    assert zoomed_before is not None

    window.contrast_slider.setValue(12)
    window._refresh_outputs()

    assert window.preview_label.original_pixmap.width() == initial_width
    assert window.preview_label.original_pixmap.width() <= 1600
    assert window.preview_label.original_pixmap.height() <= initial_height
    after = window.preview_label.get_view_state()
    assert after is not None
    assert after["zoom"] == pytest.approx(zoomed_before["zoom"])
    assert after["center"].x() == pytest.approx(zoomed_before["center"].x())
    assert after["center"].y() == pytest.approx(zoomed_before["center"].y())
    assert after["size"] == zoomed_before["size"]


def test_processing_curve_window_supports_raw_preview_and_wb_pick(tmp_path, qapp, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")

    def fake_render_raw_preview(source, *, settings=None, output_path=None, output_dir=None, **_kwargs):
        mode = str(getattr(settings, "white_balance_mode", "camera") or "camera").strip().lower() or "camera"
        color = (80, 80, 110) if mode == "auto" else (120, 60, 30)
        out_path = Path(output_path or (Path(output_dir or tmp_path) / "raw_preview.jpg"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (8, 8), color).save(out_path, format="JPEG")
        return out_path

    monkeypatch.setattr(curve_tool, "is_raw_image_path", lambda path: True)
    monkeypatch.setattr(curve_tool, "render_raw_preview", fake_render_raw_preview)

    window = curve_tool.ProcessingCurveWindow(source_path)

    assert window.source_kind_label.text() == "RAW"
    assert window.raw_base_mode_combo.currentData() == "camera"
    assert window.preview_label.original_pixmap is not None

    window.wb_pick_btn.setChecked(True)
    window.preview_label.clicked.emit(QPointF(4.0, 4.0))

    wb_settings = window.raw_controls.settings()
    assert wb_settings.white_balance_mode == "custom"
    assert wb_settings.wb_multipliers is not None
    assert wb_settings.wb_multiplier_space == "post_decode_rgb"
    assert wb_settings.wb_sample_base_mode == "camera"
    assert window.wb_pick_btn.isChecked() is False
    assert window.wb_readout_label.text().startswith("Custom WB")


def test_processing_curve_window_can_open_a_new_file_from_blank_start(tmp_path, qapp, monkeypatch):
    initial_path = tmp_path / "initial.tiff"
    opened_path = tmp_path / "opened.orf"
    _make_low_contrast_16bit_tiff(initial_path)
    opened_path.write_bytes(b"raw-bytes")

    captured_filter: dict[str, str] = {}

    def fake_render_raw_preview(source, *, settings=None, output_path=None, output_dir=None, **_kwargs):
        mode = str(getattr(settings, "white_balance_mode", "camera") or "camera").strip().lower() or "camera"
        color = (90, 120, 160) if mode == "auto" else (140, 80, 40)
        out_path = Path(output_path or (Path(output_dir or tmp_path) / "opened_preview.jpg"))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (8, 8), color).save(out_path, format="JPEG")
        return out_path

    monkeypatch.setattr(curve_tool, "render_raw_preview", fake_render_raw_preview)

    def fake_get_open_file_name(*args, **kwargs):
        captured_filter["value"] = str(args[3] if len(args) > 3 else kwargs.get("filter", ""))
        return str(opened_path), ""

    monkeypatch.setattr(curve_tool.QFileDialog, "getOpenFileName", fake_get_open_file_name)

    window = curve_tool.ProcessingCurveWindow(None)
    assert window.source_kind_label.text() == "Blank"
    assert window.source_path_label.text() == "No file loaded"
    assert window.preview_label.original_pixmap is not None
    assert window.preview_label.original_pixmap.isNull() is True
    assert window.raw_controls.isEnabled() is False

    window.open_file_btn.click()

    assert window.source_kind_label.text() == "RAW"
    assert window.source_path_label.text() == str(opened_path)
    assert window.raw_base_mode_combo.currentData() == "camera"
    assert window.raw_controls.white_balance_selector.selected_value("camera") == "camera"
    assert window.preview_label.original_pixmap is not None
    assert window.preview_label.original_pixmap.isNull() is False
    zoom_before = window.preview_label.zoom_level
    window.preview_label.zoom_in()
    assert window.preview_label.zoom_level > zoom_before
    assert window.preview_label.pan_without_shift is True
    assert "Camera WB" in window.settings_label.text()
    assert "preview" in window.settings_label.text()
    assert "*.orf" in captured_filter["value"]
    assert "*.cr3" in captured_filter["value"]
    assert "*.dng" in captured_filter["value"]
