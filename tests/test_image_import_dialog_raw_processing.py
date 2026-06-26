from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import numpy as np
import pytest
from PySide6.QtCore import QTimer
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication

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
