from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from PIL import Image
from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QApplication, QSlider, QSizePolicy

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
    image = Image.fromarray(ramp, mode="I;16")
    image.save(path, format="TIFF")


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
            "--auto-levels-strength",
            "0.65",
            "--soft-tails",
            "--tail-size",
            "0.03",
            "--shadow-lift",
            "0.10",
            "--dark-cutoff",
            "0.001",
            "--bright-cutoff",
            "0.001",
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
    assert '"auto_levels_strength": 0.65' in result.stdout
    assert '"auto_levels_soft_tails": true' in result.stdout
    assert '"auto_levels_shadow_lift": 0.1' in result.stdout
    assert f"plot out: {plot_out}" in result.stdout
    assert f"preview out: {preview_out}" in result.stdout


def test_processing_curve_window_refreshes_preview_and_graph(tmp_path, qapp):
    input_path = tmp_path / "low_contrast.tiff"
    ramp = np.linspace(12000, 18000, 64, dtype=np.uint16).reshape(8, 8)
    Image.fromarray(ramp, mode="I;16").save(input_path, format="TIFF")

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
    assert isinstance(window.dark_cutoff_slider, QSlider)
    assert isinstance(window.bright_cutoff_slider, QSlider)
    assert isinstance(window.auto_levels_strength_slider, QSlider)
    assert isinstance(window.shadow_lift_slider, QSlider)
    assert isinstance(window.curve_strength_slider, QSlider)
    assert isinstance(window.curve_midpoint_slider, QSlider)
    assert window.dark_cutoff_slider.minimum() == 0
    assert window.dark_cutoff_slider.maximum() == 50
    assert window.dark_cutoff_value_label.text() == "0.05%"
    assert window.bright_cutoff_value_label.text() == "0.05%"
    assert window.auto_levels_strength_slider.minimum() == 0
    assert window.auto_levels_strength_slider.maximum() == 100
    assert window.auto_levels_strength_value_label.text() == "100%"
    assert window.shadow_lift_slider.minimum() == 0
    assert window.shadow_lift_slider.maximum() == 200
    assert window.shadow_lift_value_label.text() == "0.0%"
    assert window.curve_strength_value_label.text() == "0.50"
    assert window.curve_midpoint_value_label.text() == "0.50"
    assert "auto levels on" in window.settings_label.text()
    assert "strength 100%" in window.settings_label.text()
    assert "soft tails off" in window.settings_label.text()
    assert "shadow lift 0.0%" in window.settings_label.text()
    assert "dark cutoff" in window.settings_label.text()
    assert "bright cutoff" in window.settings_label.text()

    window.tone_curve_checkbox.setChecked(True)
    window.soft_tails_checkbox.setChecked(True)
    window.auto_levels_strength_slider.setValue(65)
    window.shadow_lift_slider.setValue(50)
    window.dark_cutoff_slider.setValue(15)
    window.bright_cutoff_slider.setValue(15)
    window.curve_strength_slider.setValue(75)
    window.curve_midpoint_slider.setValue(35)
    window._refresh_outputs()

    assert "tone curve on" in window.settings_label.text()
    assert "strength 65%" in window.settings_label.text()
    assert "soft tails on" in window.settings_label.text()
    assert "shadow lift 5.0%" in window.settings_label.text()
    assert window.dark_cutoff_value_label.text() == "0.15%"
    assert window.bright_cutoff_value_label.text() == "0.15%"
    assert window.auto_levels_strength_value_label.text() == "65%"
    assert window.shadow_lift_value_label.text() == "5.0%"
    assert window.curve_strength_value_label.text() == "0.75"
    assert window.curve_midpoint_value_label.text() == "0.35"
    assert window.black_level_label.text() != "—"
    assert window.figure.axes[0].get_xlabel() == "Input luminance"
    assert window.figure.axes[0].get_ylabel() == "Output luminance"


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


def test_processing_curve_window_uses_real_chart_transfer_mapping(qapp):
    chart_path = Path(__file__).resolve().parents[2] / "curve_test_chart_low_contrast_16bit.tiff"
    assert chart_path.exists()

    window = curve_tool.ProcessingCurveWindow(chart_path)

    processed_rgb, debug = curve_tool.apply_post_decode_processing(window._source.rgb, window._settings, return_debug=True)
    curve = curve_tool.compute_post_decode_transfer_curve(
        window._source.rgb,
        window._settings,
        debug=debug,
    )

    assert curve.debug.black_level == pytest.approx(0.179995422293431, rel=1e-6)
    assert curve.debug.white_level == pytest.approx(0.8, rel=1e-6)
    assert float(processed_rgb.min()) == pytest.approx(0.0, abs=1e-6)
    assert float(processed_rgb.max()) == pytest.approx(1.0, abs=1e-6)
    black_idx = int(np.where(np.isclose(curve.input_values, curve.debug.black_level))[0][0])
    white_idx = int(np.where(np.isclose(curve.input_values, curve.debug.white_level))[0][0])
    assert float(curve.hard_target[black_idx]) == pytest.approx(0.0, abs=1e-6)
    assert float(curve.hard_target[white_idx]) == pytest.approx(1.0, abs=1e-6)

    window.tone_curve_checkbox.setChecked(True)
    window.curve_strength_slider.setValue(75)
    window.curve_midpoint_slider.setValue(30)
    window._refresh_outputs()

    processed_toned, toned_debug = curve_tool.apply_post_decode_processing(window._source.rgb, window._settings, return_debug=True)
    curve = curve_tool.compute_post_decode_transfer_curve(
        window._source.rgb,
        window._settings,
        debug=toned_debug,
    )

    black_idx = int(np.where(np.isclose(curve.input_values, curve.debug.black_level))[0][0])
    white_idx = int(np.where(np.isclose(curve.input_values, curve.debug.white_level))[0][0])
    assert float(curve.auto_levels_output[black_idx]) == pytest.approx(0.0, abs=1e-6)
    assert float(curve.auto_levels_output[white_idx]) == pytest.approx(1.0, abs=1e-6)
    assert float(curve.final_output[0]) == pytest.approx(0.0, abs=1e-6)
    assert float(curve.final_output[-1]) == pytest.approx(1.0, abs=1e-6)
    assert float(processed_toned.min()) <= 0.05
    assert float(processed_toned.max()) >= 0.95

    axes = window.figure.axes[0]
    lines = {line.get_label(): line for line in axes.lines}
    assert np.allclose(lines["identity"].get_xdata(), curve.input_values)
    assert np.allclose(lines["identity"].get_ydata(), curve.input_values)
    assert np.allclose(lines["hard auto-level target"].get_xdata(), curve.input_values)
    assert np.allclose(lines["hard auto-level target"].get_ydata(), curve.hard_target)
    assert np.allclose(lines["strength-blended auto-level output"].get_xdata(), curve.input_values)
    assert np.allclose(lines["strength-blended auto-level output"].get_ydata(), curve.auto_levels_output)
    assert np.allclose(lines["final output after tone curve"].get_xdata(), curve.input_values)
    assert np.allclose(lines["final output after tone curve"].get_ydata(), curve.final_output)
    assert axes.get_xlim() == pytest.approx((0.0, 1.0))
    assert axes.get_ylim() == pytest.approx((0.0, 1.0))


def test_processing_curve_window_soft_target_is_continuous(tmp_path, qapp):
    input_path = tmp_path / "low_contrast.tiff"
    ramp = np.linspace(12000, 18000, 64, dtype=np.uint16).reshape(8, 8)
    Image.fromarray(ramp, mode="I;16").save(input_path, format="TIFF")

    window = curve_tool.ProcessingCurveWindow(input_path)
    window.soft_tails_checkbox.setChecked(True)
    window.shadow_lift_slider.setValue(50)
    window.dark_cutoff_slider.setValue(0)
    window.bright_cutoff_slider.setValue(0)
    window._refresh_outputs()

    processed_rgb, debug = curve_tool.apply_post_decode_processing(window._source.rgb, window._settings, return_debug=True)
    curve = curve_tool.compute_post_decode_transfer_curve(
        window._source.rgb,
        window._settings,
        debug=debug,
    )

    black_idx = int(np.where(np.isclose(curve.input_values, curve.debug.black_level))[0][0])
    white_idx = int(np.where(np.isclose(curve.input_values, curve.debug.white_level))[0][0])
    assert float(curve.soft_target[black_idx]) == pytest.approx(0.05, abs=1e-6)
    assert float(curve.soft_target[white_idx]) == pytest.approx(1.0, abs=1e-6)
    assert float(np.max(np.abs(np.diff(curve.soft_target)))) < 0.05
    assert float(processed_rgb.min()) == pytest.approx(0.05, abs=1e-6)
    assert float(processed_rgb.max()) == pytest.approx(1.0, abs=1e-6)


def test_processing_curve_window_supports_raw_preview_and_wb_pick(tmp_path, qapp, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")

    def fake_render_raw_preview(source, *, settings=None, output_path=None, output_dir=None, **_kwargs):
        mode = str(getattr(settings, "white_balance_mode", "camera") or "camera").strip().lower() or "camera"
        if mode == "auto":
            color = (80, 80, 110)
        else:
            color = (120, 60, 30)
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

    settings = window._settings
    assert settings.white_balance_mode == "custom"
    assert settings.wb_multipliers is not None
    assert settings.wb_multiplier_space == "post_decode_rgb"
    assert settings.wb_sample_base_mode == "camera"
    assert window.wb_pick_btn.isChecked() is False
    assert window.wb_readout_label.text().startswith("Custom WB")


def test_processing_curve_window_can_open_a_new_file(tmp_path, qapp, monkeypatch):
    initial_path = tmp_path / "initial.tiff"
    opened_path = tmp_path / "opened.orf"
    ramp = np.linspace(12000, 18000, 64, dtype=np.uint16).reshape(8, 8)
    Image.fromarray(ramp, mode="I;16").save(initial_path, format="TIFF")
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

    window = curve_tool.ProcessingCurveWindow(initial_path)
    assert window.source_kind_label.text() == "Raster"
    assert window.source_path_label.text() == str(initial_path)

    window.open_file_btn.click()

    assert window.source_kind_label.text() == "RAW"
    assert window.source_path_label.text() == str(opened_path)
    assert window.raw_base_mode_combo.currentData() == "camera"
    assert window.wb_mode_combo.currentData() == "camera"
    assert window.preview_label.original_pixmap is not None
    zoom_before = window.preview_label.zoom_level
    window.preview_label.zoom_in()
    assert window.preview_label.zoom_level > zoom_before
    assert window.preview_label.pan_without_shift is True
    assert "base camera" in window.settings_label.text()
    assert "*.orf" in captured_filter["value"]
    assert "*.cr3" in captured_filter["value"]
    assert "*.dng" in captured_filter["value"]
