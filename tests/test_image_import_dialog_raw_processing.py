from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import numpy as np
import pytest
from PySide6.QtCore import QPointF, QTimer
from PySide6.QtGui import QPixmap
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QWidget

import ui.live_lab_tab as live_lab_tab
from ui import image_import_dialog
from ui.image_import_dialog import ImageImportDialog, ImageImportResult
from ui.raw_processing_controls import RawProcessingControls
from utils.raw_render import RawRenderSettings


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_raw_result(tmp_path: Path, *, name: str = "sample.nef") -> ImageImportResult:
    raw_path = tmp_path / name
    raw_path.write_bytes(b"raw-bytes")
    return ImageImportResult(
        filepath=str(raw_path),
        preview_path=str(raw_path),
        image_type="field",
        raw_candidate=True,
        raw_pending=True,
        raw_settings=RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            tone_curve_enabled=False,
        ).to_dict(),
        raw_unsaved_changes=False,
    )


def _build_raw_dialog_dummy(result: ImageImportResult) -> SimpleNamespace:
    dummy = SimpleNamespace()
    dummy.import_results = [result]
    dummy.image_paths = [result.filepath]
    dummy.selected_index = 0
    dummy.selected_indices = [0]
    dummy.tr = lambda text: text
    dummy._raw_loading = False
    dummy._raw_preview_proxy_cache = {}
    dummy._pending_raw_preview_result = None
    dummy._converted_import_paths = set()
    dummy._continue_to_observation_details = True
    dummy._raw_preview_refresh_timer = QTimer()
    dummy._raw_preview_refresh_timer.setSingleShot(True)
    dummy._raw_preview_refresh_timer.setInterval(60)
    dummy._raw_preview_refresh_timer.timeout.connect(lambda: ImageImportDialog._flush_pending_raw_preview(dummy))
    dummy._result_is_raw_backed = lambda candidate: bool(getattr(candidate, "raw_candidate", False))
    dummy._current_single_index = lambda: 0
    dummy._collect_raw_settings_from_form = lambda base=None: dict(base or result.raw_settings or {})
    dummy._update_raw_panel_for_result = lambda *_args, **_kwargs: None
    dummy._set_preview_for_result = lambda *_args, **_kwargs: None
    dummy._invalidate_cached_pixmap = lambda *_args, **_kwargs: None
    dummy.set_hint = lambda *_args, **_kwargs: None
    dummy.set_status = lambda *_args, **_kwargs: None
    dummy._set_settings_hint = lambda *_args, **_kwargs: None
    dummy._raw_source_path_for_result = lambda candidate: candidate.filepath
    dummy._schedule_raw_preview_refresh = lambda candidate: ImageImportDialog._schedule_raw_preview_refresh(dummy, candidate)
    dummy._cancel_pending_raw_preview = lambda candidate=None: ImageImportDialog._cancel_pending_raw_preview(dummy, candidate)
    dummy._raw_preview_proxy_cache_key = lambda source, settings: ImageImportDialog._raw_preview_proxy_cache_key(dummy, source, settings)
    dummy._raw_preview_proxy_for_result = lambda source, settings: ImageImportDialog._raw_preview_proxy_for_result(dummy, source, settings)
    dummy._raw_preview_output_path = lambda source: ImageImportDialog._raw_preview_output_path(source)
    dummy._raw_preview_decode_mode = lambda settings: ImageImportDialog._raw_preview_decode_mode(settings)
    dummy._refresh_raw_preview = lambda *_args, **_kwargs: None
    dummy._ensure_raw_settings = lambda result: ImageImportDialog._ensure_raw_settings(dummy, result)
    dummy._load_raw_settings_into_form = lambda settings, **kwargs: ImageImportDialog._load_raw_settings_into_form(
        dummy,
        settings,
        **kwargs,
    )
    dummy._ensure_raw_convert_button = lambda: None
    dummy._refresh_raw_preview_calls = []
    dummy._finalize_raw_settings_for_result = lambda candidate, index=None: (
        ImageImportDialog._finalize_raw_settings_for_result(dummy, candidate, index)
    )
    dummy._get_image_size = lambda *_args, **_kwargs: (0, 0)
    dummy._refresh_gallery = lambda: None
    dummy._select_image = lambda *_args, **_kwargs: None
    dummy._raw_preview_cache_entry = lambda source, settings: (
        ImageImportDialog._raw_preview_cache_entry(dummy, source, settings)
    )
    dummy._raw_preview_resized_for_entry = lambda entry: ImageImportDialog._raw_preview_resized_for_entry(entry)
    return dummy


def test_raw_preview_refresh_is_debounced_and_restarts_timer(monkeypatch, qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)

    calls: list[object] = []
    dummy._refresh_raw_preview = lambda target: calls.append(target)

    ImageImportDialog._on_raw_settings_changed(dummy)
    assert result.raw_unsaved_changes is True
    assert dummy._raw_preview_refresh_timer.isActive() is True
    assert calls == []

    QTest.qWait(20)
    ImageImportDialog._on_raw_settings_changed(dummy)
    assert calls == []

    QTest.qWait(120)
    qapp.processEvents()

    assert len(calls) == 1
    assert calls[0] is result
    assert dummy._raw_preview_refresh_timer.isActive() is False


def test_raw_preview_refresh_skips_non_raw_images(qapp):
    result = ImageImportResult(
        filepath="/tmp/sample.jpg",
        preview_path="/tmp/sample.jpg",
        image_type="field",
        raw_candidate=False,
        raw_pending=False,
        raw_settings=RawRenderSettings.default().to_dict(),
    )
    dummy = _build_raw_dialog_dummy(result)
    dummy._result_is_raw_backed = lambda candidate: False
    dummy._refresh_raw_preview = lambda *_args, **_kwargs: pytest.fail("refresh should not run")

    ImageImportDialog._on_raw_settings_changed(dummy)

    assert dummy._raw_preview_refresh_timer.isActive() is False
    assert result.raw_unsaved_changes is False


def test_raw_preview_refresh_redirects_when_selection_changes(qapp, tmp_path):
    first = _build_raw_result(tmp_path, name="first.nef")
    second = _build_raw_result(tmp_path, name="second.nef")
    dummy = _build_raw_dialog_dummy(first)
    dummy.import_results = [first, second]
    dummy.image_paths = [first.filepath, second.filepath]
    dummy.selected_index = 0
    dummy.selected_indices = [0]
    dummy._current_single_index = lambda: dummy.selected_index

    calls: list[object] = []
    dummy._refresh_raw_preview = lambda target: calls.append(target)

    ImageImportDialog._on_raw_settings_changed(dummy)
    dummy.selected_index = 1
    dummy.selected_indices = [1]
    ImageImportDialog._on_raw_settings_changed(dummy)

    QTest.qWait(120)
    qapp.processEvents()

    assert len(calls) == 1
    assert calls[0] is second


def test_raw_panel_uses_metadata_settings_when_result_raw_settings_missing(qapp, tmp_path):
    result = ImageImportResult(
        filepath=str(tmp_path / "sample.nef"),
        preview_path=str(tmp_path / "sample.nef"),
        image_type="field",
        raw_candidate=True,
        raw_pending=True,
        raw_settings=None,
        lab_metadata={
            "raw_processing": {
                "settings": RawRenderSettings(
                    white_balance_mode="auto",
                    auto_levels=False,
                    tone_curve_enabled=True,
                    tone_curve_strength=0.72,
                    tone_curve_midpoint=0.31,
                ).to_dict(),
            }
        },
    )
    Path(result.filepath).write_bytes(b"raw-bytes")
    dummy = _build_raw_dialog_dummy(result)
    controls = RawProcessingControls()
    dummy.raw_controls = controls

    ImageImportDialog._update_raw_panel_for_result(dummy, result)

    assert result.raw_settings is not None
    assert result.raw_settings["white_balance_mode"] == "auto"
    assert controls.white_balance_selector.selected_value("camera") == "auto"
    assert controls.auto_levels_checkbox.isChecked() is False
    assert controls.tone_curve_checkbox.isChecked() is True
    assert controls.curve_strength_slider.value() == 72
    assert controls.curve_midpoint_slider.value() == 31


def test_raw_panel_preserves_saved_auto_level_bounds_and_slider_positions(qapp, tmp_path):
    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")
    saved = RawRenderSettings(
        auto_levels=True,
        light_ev=0.375,
        dark_ev=-0.125,
        auto_black_level=0.083,
        auto_white_level=0.771,
    )
    result = ImageImportResult(
        filepath=str(raw_path),
        preview_path=str(tmp_path / "saved.jpg"),
        image_type="field",
        raw_candidate=True,
        raw_pending=False,
        raw_settings=None,
        lab_metadata={"raw_processing": {"settings": saved.to_dict()}},
    )
    dummy = _build_raw_dialog_dummy(result)
    dummy.raw_controls = RawProcessingControls()
    refreshes: list[object] = []
    dummy._schedule_raw_preview_refresh = lambda candidate: refreshes.append(candidate)

    ImageImportDialog._update_raw_panel_for_result(dummy, result)

    restored = RawRenderSettings.from_dict(result.raw_settings)
    assert restored.auto_black_level == pytest.approx(0.083)
    assert restored.auto_white_level == pytest.approx(0.771)
    assert dummy.raw_controls.light_slider.value() == 375
    assert dummy.raw_controls.dark_slider.value() == 125
    assert refreshes == []


def test_prepare_images_method_b_uses_larger_proxy_for_auto_levels(tmp_path):
    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")
    raw_rgb = np.full((4, 4, 3), 0.5, dtype=np.float32)
    raw_rgb[0, 0] = 1.0
    resized_rgb = np.asarray(
        [
            [[0.2, 0.2, 0.2], [0.5, 0.5, 0.5]],
            [[0.3, 0.3, 0.3], [0.4, 0.4, 0.4]],
        ],
        dtype=np.float32,
    )
    entry = image_import_dialog._RawPreviewCacheEntry(
        raw_rgb=raw_rgb,
        preview_rgb=resized_rgb,
    )
    dummy = SimpleNamespace(
        _raw_processing_preferences=lambda: {"dark_cutoff": 0.0, "bright_cutoff": 0.0},
        _raw_preview_cache_entry=lambda *_args: entry,
        _raw_preview_resized_for_entry=lambda _entry: _entry.preview_rgb,
    )

    method_a = ImageImportDialog._raw_auto_level_settings_for_source(
        dummy, str(raw_path), RawRenderSettings(auto_levels_method="a")
    )
    method_b = ImageImportDialog._raw_auto_level_settings_for_source(
        dummy, str(raw_path), RawRenderSettings(auto_levels_method="b")
    )

    assert method_a.auto_white_level == pytest.approx(0.5)
    assert method_b.auto_white_level == pytest.approx(1.0)


def test_prepare_images_method_b_matches_historical_pre_custom_wb_analysis(tmp_path):
    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")
    raw_rgb = np.full((2, 2, 3), 0.1, dtype=np.float32)
    raw_rgb[0, 0] = (1.0, 0.0, 0.0)
    entry = image_import_dialog._RawPreviewCacheEntry(
        raw_rgb=raw_rgb,
        preview_rgb=raw_rgb.copy(),
    )
    dummy = SimpleNamespace(
        _raw_processing_preferences=lambda: {"dark_cutoff": 0.0, "bright_cutoff": 0.0},
        _raw_preview_cache_entry=lambda *_args: entry,
        _raw_preview_resized_for_entry=lambda _entry: _entry.preview_rgb,
    )
    settings = RawRenderSettings(
        auto_levels_method="b",
        white_balance_mode="custom",
        wb_multipliers=(0.1, 1.0, 1.0),
        wb_multiplier_space="post_decode_rgb",
    )

    method_b = ImageImportDialog._raw_auto_level_settings_for_source(
        dummy, str(raw_path), settings
    )

    assert method_b.auto_white_level == pytest.approx(0.2126, abs=1e-5)


def test_raw_preview_size_change_preserves_normalized_center_and_visible_zoom(qapp):
    restored: list[tuple[QPointF, float]] = []
    dummy = SimpleNamespace(
        preview=SimpleNamespace(
            set_view_state=lambda center, zoom: restored.append((center, zoom))
        )
    )
    pixmap = QPixmap(1000, 500)
    old_state = {
        "center": QPointF(1000.0, 500.0),
        "zoom": 2.0,
        "size": (4000, 2000),
    }

    ImageImportDialog._restore_raw_preview_view_state(dummy, old_state, pixmap)

    center, zoom = restored[-1]
    assert center.x() == pytest.approx(250.0)
    assert center.y() == pytest.approx(125.0)
    assert zoom == pytest.approx(8.0)


def test_raw_action_tab_shows_apply_copy_and_paste_in_raw_edit_mode(qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    result.raw_pending = False
    result.lab_metadata = {
        "raw_processing": {
            "source": {"kind": "camera_raw"},
            "settings": RawRenderSettings.default().to_dict(),
        }
    }
    dummy = _build_raw_dialog_dummy(result)
    dummy.preview = QWidget()
    dummy.preview.resize(640, 420)
    dummy.preview.show()
    dummy._continue_to_observation_details = False
    dummy._result_is_raw_backed = lambda candidate: True
    dummy._raw_source_path_for_result = lambda candidate: candidate.filepath
    dummy._schedule_raw_preview_refresh = lambda *_args, **_kwargs: None
    dummy._load_raw_settings_into_form = lambda *_args, **_kwargs: None
    dummy._ensure_raw_settings = lambda candidate: dict(candidate.raw_settings or RawRenderSettings.default().to_dict())
    dummy._build_raw_action_tab = lambda: ImageImportDialog._build_raw_action_tab(dummy)
    dummy._ensure_raw_convert_button = lambda: ImageImportDialog._ensure_raw_convert_button(dummy)
    dummy._on_raw_convert_clicked = lambda *_args, **_kwargs: None
    dummy._on_raw_copy_clicked = lambda *_args, **_kwargs: None
    dummy._on_raw_paste_clicked = lambda *_args, **_kwargs: None
    dummy._position_raw_convert_button = lambda: None
    dummy._set_settings_hint = lambda *_args, **_kwargs: None
    dummy._raw_copied_settings = None

    ImageImportDialog._update_raw_panel_for_result(dummy, result)
    qapp.processEvents()

    assert dummy.raw_action_frame.isVisible() is True
    assert dummy.raw_convert_btn.text() == "Apply new raw settings"
    assert dummy.raw_copy_btn.isVisible() is True
    assert dummy.raw_paste_btn.isVisible() is False

    dummy._raw_copied_settings = RawRenderSettings.default().to_dict()
    ImageImportDialog._update_raw_panel_for_result(dummy, result)

    assert dummy.raw_paste_btn.isVisible() is True


def test_prepare_images_raw_curve_histogram_uses_pre_levels_working_buffer(qapp, tmp_path, monkeypatch):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)
    captured: dict[str, object] = {}
    dummy.raw_curve_preview_widget = SimpleNamespace(
        set_curve=lambda curve, histogram: captured.update({"curve": curve, "histogram": histogram}),
    )
    settings = RawRenderSettings.default()
    key = dummy._raw_preview_proxy_cache_key(result.filepath, settings)
    dummy._raw_preview_proxy_cache = {
        key: image_import_dialog._RawPreviewCacheEntry(
            raw_rgb=np.full((2, 2, 3), 0.1, dtype=np.float32),
        )
    }
    entry = dummy._raw_preview_proxy_cache[key]
    dummy._raw_preview_cache_entry = lambda source, settings: entry
    # Force the pre-levels helper to a known constant so we don't rely on
    # the resize path; the histogram should track this buffer, NOT the
    # post-pipeline output.
    pre_levels_rgb = np.full((2, 2, 3), 0.75, dtype=np.float32)
    monkeypatch.setattr(
        image_import_dialog,
        "compute_pre_levels_working_rgb",
        lambda rgb, settings: pre_levels_rgb,
    )

    curve = SimpleNamespace(
        input_values=np.array([0.0, 1.0], dtype=np.float32),
        final_output=np.array([0.0, 1.0], dtype=np.float32),
    )
    ImageImportDialog._refresh_prepare_raw_curve_preview(dummy, result, curve=curve)

    histogram = captured.get("histogram")
    assert histogram is not None
    assert histogram.size == 96
    # 0.75 falls into bin int(0.75 * 96) == 72.
    assert int(histogram.argmax()) == 72


def test_raw_convert_still_calls_final_render_immediately(monkeypatch, qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)
    source_path = Path(result.filepath)
    converted_path = tmp_path / "converted.jpg"
    converted_path.write_bytes(b"jpeg-bytes")

    calls: list[tuple[Path, object]] = []

    def fake_render(source, *, settings=None, output_dir=None):
        calls.append((Path(source), settings))
        return converted_path

    monkeypatch.setattr(image_import_dialog, "render_raw_image", fake_render)
    monkeypatch.setattr(image_import_dialog, "build_raw_processing_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(image_import_dialog, "read_rawpy_capture_datetime", None, raising=False)
    dummy._raw_source_path_for_result = lambda candidate: str(source_path)
    dummy._get_image_size = lambda *_args, **_kwargs: (2, 2)
    dummy._refresh_gallery = lambda: None
    dummy._select_image = lambda *_args, **_kwargs: None
    dummy._set_settings_hint = lambda *_args, **_kwargs: None
    ImageImportDialog._schedule_raw_preview_refresh(dummy, result)

    ImageImportDialog._on_raw_convert_clicked(dummy)

    assert len(calls) == 1
    assert calls[0][0] == source_path
    assert result.raw_pending is False
    assert result.raw_unsaved_changes is False
    assert result.preview_path == str(converted_path)
    assert dummy._raw_preview_refresh_timer.isActive() is False


def test_raw_preview_cache_is_reused_across_tone_changes(monkeypatch, qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)

    proxy_calls: list[tuple[str, dict | None]] = []
    proxy = np.full((2, 2, 3), 0.5, dtype=np.float64)

    def fake_proxy(source, *, settings=None):
        proxy_calls.append((str(source), settings))
        return proxy

    monkeypatch.setattr(image_import_dialog, "render_raw_preview_proxy_rgb", fake_proxy)
    result.raw_settings = RawRenderSettings(
        white_balance_mode="camera",
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.55,
        tone_curve_midpoint=0.42,
    ).to_dict()
    ImageImportDialog._raw_preview_proxy_for_result(dummy, result.filepath, result.raw_settings)

    result.raw_settings = RawRenderSettings(
        white_balance_mode="camera",
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.80,
        tone_curve_midpoint=0.30,
        exposure_ev=0.5,
        shadow_lift=0.03,
    ).to_dict()
    ImageImportDialog._raw_preview_proxy_for_result(dummy, result.filepath, result.raw_settings)

    assert len(proxy_calls) == 1


def test_raw_settings_changes_preserve_custom_wb_and_serialise_exposure_and_shadows(qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)
    controls = RawProcessingControls()
    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="custom",
            wb_multipliers=(1.2, 1.0, 1.4),
            wb_selection=(10.0, 12.0, 20.0, 22.0),
            wb_multiplier_space="post_decode_rgb",
            exposure_ev=0.25,
            auto_levels=True,
            tone_curve_enabled=True,
            tone_curve_strength=0.60,
            tone_curve_midpoint=0.40,
            shadow_lift=0.02,
        )
    )
    dummy.raw_controls = controls

    initial = ImageImportDialog._collect_raw_settings_from_form(dummy, base=result.raw_settings)
    assert initial["white_balance_mode"] == "custom"
    assert initial["wb_multipliers"] == [1.2, 1.0, 1.4]
    assert initial["exposure_ev"] == pytest.approx(0.25)
    assert initial["shadow_lift"] == pytest.approx(0.02)

    controls.exposure_slider.setValue(500)
    controls.dark_slider.setValue(250)
    controls.curve_strength_slider.setValue(60)
    controls.curve_midpoint_slider.setValue(40)

    updated = ImageImportDialog._collect_raw_settings_from_form(dummy, base=initial)

    assert updated["white_balance_mode"] == "custom"
    assert updated["wb_multipliers"] == [1.2, 1.0, 1.4]
    assert updated["exposure_ev"] == pytest.approx(0.25)
    assert updated["shadow_lift"] == pytest.approx(0.02)
    assert updated["tone_curve_strength"] == pytest.approx(0.60)
    assert updated["tone_curve_midpoint"] == pytest.approx(0.40)


def test_raw_convert_stores_exposure_and_shadow_metadata(monkeypatch, qapp, tmp_path):
    result = _build_raw_result(tmp_path)
    dummy = _build_raw_dialog_dummy(result)
    source_path = Path(result.filepath)
    converted_path = tmp_path / "converted.jpg"
    converted_path.write_bytes(b"jpeg-bytes")

    captured = {}

    def fake_render(source, *, settings=None, output_dir=None):
        captured["render_settings"] = RawRenderSettings.from_dict(settings)
        return converted_path

    def fake_metadata(source_path, derivative_path, settings, **kwargs):
        captured["metadata_settings"] = RawRenderSettings.from_dict(settings)
        return {
            "source": {"path": str(source_path)},
            "local_derivative": {"path": str(derivative_path)},
            "settings": RawRenderSettings.from_dict(settings).to_dict(),
        }

    monkeypatch.setattr(image_import_dialog, "render_raw_image", fake_render)
    monkeypatch.setattr(image_import_dialog, "build_raw_processing_metadata", fake_metadata)
    monkeypatch.setattr(image_import_dialog, "read_rawpy_capture_datetime", None, raising=False)
    dummy._raw_source_path_for_result = lambda candidate: str(source_path)
    dummy._get_image_size = lambda *_args, **_kwargs: (2, 2)
    dummy._refresh_gallery = lambda: None
    dummy._select_image = lambda *_args, **_kwargs: None
    dummy._set_settings_hint = lambda *_args, **_kwargs: None
    result.raw_settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.2, 1.0, 1.4),
        wb_selection=(10.0, 12.0, 20.0, 22.0),
        wb_multiplier_space="post_decode_rgb",
        exposure_ev=0.5,
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.60,
        tone_curve_midpoint=0.40,
        shadow_lift=0.03,
    ).to_dict()

    ImageImportDialog._on_raw_convert_clicked(dummy)

    assert captured["render_settings"].exposure_ev == pytest.approx(0.5)
    assert captured["render_settings"].shadow_lift == pytest.approx(0.03)
    assert captured["metadata_settings"].exposure_ev == pytest.approx(0.5)
    assert captured["metadata_settings"].shadow_lift == pytest.approx(0.03)
    assert result.lab_metadata["raw_processing"]["settings"]["exposure_ev"] == pytest.approx(0.5)
    assert result.lab_metadata["raw_processing"]["settings"]["shadow_lift"] == pytest.approx(0.03)
