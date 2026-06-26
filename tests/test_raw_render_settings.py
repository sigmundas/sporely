from datetime import datetime
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image

from utils.exif_reader import get_image_metadata
from utils.raw_render import (
    RAW_DERIVATIVE_FORMAT,
    RAW_DERIVATIVE_QUALITY,
    RAW_DERIVATIVE_SUBSAMPLING,
    RawRenderSettings,
    apply_auto_level_bounds_to_settings,
    build_raw_processing_metadata,
    render_raw_image,
    save_raw_preview_jpeg,
    render_raw_sampling_rgb,
)


class _DummyRaw:
    def __init__(self, rgb: np.ndarray, timestamp: datetime | None = None) -> None:
        self._rgb = rgb
        self.other = SimpleNamespace(timestamp=timestamp)
        self.kwargs = None
        self.source_path = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def postprocess(self, **kwargs):
        self.kwargs = kwargs
        return self._rgb


class _DummyRawpyModule:
    def __init__(self, raw: _DummyRaw) -> None:
        self._raw = raw

    def imread(self, path):
        self._raw.source_path = str(path)
        return self._raw


def test_raw_render_settings_round_trip():
    settings = RawRenderSettings(
        white_balance_mode="auto",
        wb_multipliers=(1.1, 1.0, 1.3),
        wb_selection=(10.0, 20.0, 30.0, 40.0),
        wb_multiplier_space="post_decode_rgb",
        wb_sample_point=(25.0, 35.0),
        wb_sample_size=10,
        wb_sample_base_mode="camera",
        wb_selection_space="preview_pixels",
        exposure_ev=0.75,
        light_ev=0.75,
        dark_ev=0.0,
        auto_levels=True,
        black_percentile=0.01,
        white_percentile=0.99,
        auto_levels_strength=0.65,
        auto_levels_soft_tails=True,
        auto_levels_tail_size=0.04,
        shadow_lift=0.04,
        tone_curve_enabled=True,
        tone_curve_strength=0.75,
        tone_curve_midpoint=0.42,
        tone_shadows=0.25,
        tone_highlights=-0.15,
        output_bps=8,
    )

    assert RawRenderSettings.from_dict(settings.to_dict()) == settings


def test_raw_render_settings_camera_wb_with_multipliers_normalizes_to_custom():
    settings = RawRenderSettings(
        white_balance_mode="camera",
        wb_multipliers=(1.1, 1.0, 1.3),
        wb_selection=(10.0, 20.0, 30.0, 40.0),
    )

    normalized = RawRenderSettings.from_dict(settings)
    assert normalized.white_balance_mode == "custom"
    assert normalized.wb_multiplier_space == "post_decode_rgb"
    assert normalized.wb_sample_base_mode == "camera"


def test_raw_render_settings_default_uses_camera_wb_and_auto_levels():
    settings = RawRenderSettings.default()

    assert settings.white_balance_mode == "camera"
    assert settings.exposure_ev == 0.0
    assert settings.light_ev == 0.0
    assert settings.dark_ev == 0.0
    assert settings.auto_levels is True
    assert settings.auto_levels_strength == 1.0
    assert settings.auto_levels_soft_tails is False
    assert settings.auto_levels_tail_size == 0.03
    assert settings.auto_levels_shadow_lift == 0.0
    assert settings.shadow_lift == 0.0
    assert settings.tone_curve_enabled is False
    assert settings.tone_shadows == 0.0
    assert settings.tone_highlights == 0.0
    assert settings.wb_selection_space is None


def test_raw_render_settings_from_legacy_dict_uses_new_defaults():
    settings = RawRenderSettings.from_dict(
        {
            "white_balance_mode": "camera",
            "auto_levels": False,
            "black_percentile": 0.002,
            "white_percentile": 0.998,
            "auto_levels_shadow_lift": 0.12,
            "exposure_ev": 3.0,
        }
    )

    assert settings.white_balance_mode == "camera"
    assert settings.auto_levels is False
    assert settings.exposure_ev == 2.0
    assert settings.light_ev == 2.0
    assert settings.dark_ev == 0.0
    assert settings.auto_levels_strength == 1.0
    assert settings.auto_levels_soft_tails is False
    assert settings.auto_levels_tail_size == 0.03
    assert settings.auto_levels_shadow_lift == 0.10
    assert settings.shadow_lift == 0.10
    assert settings.tone_shadows == 0.0
    assert settings.tone_highlights == 0.0


def test_raw_render_settings_clamps_shadow_lift_aliases():
    settings = RawRenderSettings.from_dict({"shadow_lift": 0.25, "exposure_ev": -3.0})

    assert settings.exposure_ev == -2.0
    assert settings.light_ev == 0.0
    assert settings.dark_ev == -2.0
    assert settings.auto_levels_shadow_lift == 0.10
    assert settings.shadow_lift == 0.10


def test_apply_auto_level_bounds_to_settings_uses_bounds_without_crashing():
    settings = RawRenderSettings.default()

    adjusted = apply_auto_level_bounds_to_settings(settings, 0.12, 0.92)

    assert adjusted.auto_levels is True
    assert adjusted.dark_ev <= 0.0
    assert adjusted.light_ev >= 0.0
    assert adjusted.exposure_ev == pytest.approx(adjusted.light_ev + adjusted.dark_ev)


def test_render_raw_image_writes_high_quality_local_derivative(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    rgb = np.array(
        [
            [[0.10, 0.20, 0.30], [0.40, 0.50, 0.60]],
            [[0.70, 0.80, 0.90], [0.15, 0.25, 0.35]],
        ],
        dtype=np.float64,
    )
    raw = _DummyRaw(rgb)
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    saved = {}
    real_save = Image.Image.save

    def capture_save(self, fp, format=None, **params):
        saved["format"] = format
        saved["params"] = dict(params)
        return real_save(self, fp, format=format, **params)

    monkeypatch.setattr(Image.Image, "save", capture_save)

    output_path = render_raw_image(source_path, output_dir=output_dir)

    assert output_path.exists()
    assert output_path.suffix == ".jpg"
    assert output_path.parent == output_dir
    assert raw.source_path == str(source_path)
    assert raw.kwargs["use_camera_wb"] is True
    assert raw.kwargs["use_auto_wb"] is False
    assert raw.kwargs["output_bps"] == 16
    assert raw.kwargs["no_auto_bright"] is True
    assert saved["format"] == RAW_DERIVATIVE_FORMAT.upper()
    assert saved["params"]["quality"] == RAW_DERIVATIVE_QUALITY
    assert saved["params"]["subsampling"] == RAW_DERIVATIVE_SUBSAMPLING
    assert saved["params"]["optimize"] is True
    with Image.open(output_path) as rendered:
        assert rendered.size == (2, 2)
        assert rendered.format == "JPEG"


def test_render_raw_image_preview_mode_downscales_to_1600px_max_edge(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    rgb = np.full((2000, 4000, 3), 0.5, dtype=np.float64)
    raw = _DummyRaw(rgb)
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    output_path = render_raw_image(source_path, output_dir=output_dir, preview=True)

    with Image.open(output_path) as rendered:
        assert rendered.size == (1600, 800)
        assert rendered.format == "JPEG"


def test_save_raw_preview_jpeg_downscales_to_1600px_max_edge(tmp_path):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    preview_path = tmp_path / "preview.jpg"
    rgb = np.full((1200, 3600, 3), 0.5, dtype=np.float64)

    save_raw_preview_jpeg(rgb, preview_path, source_path)

    with Image.open(preview_path) as rendered:
        assert rendered.size == (1600, 533)
        assert rendered.format == "JPEG"


def test_render_raw_image_preserves_capture_time_in_exif(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    timestamp = datetime(2026, 5, 16, 19, 44, 11)
    raw = _DummyRaw(np.full((2, 2, 3), 0.5, dtype=np.float64), timestamp=timestamp)
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    output_path = render_raw_image(source_path, output_dir=output_dir, source_capture_datetime=timestamp)

    metadata = get_image_metadata(str(output_path))
    assert metadata["datetime"] == timestamp


def test_render_raw_image_removes_partial_output_on_failure(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    raw = _DummyRaw(np.full((2, 2, 3), 0.5, dtype=np.float64))
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    def fail_save(self, fp, format=None, **params):
        raise OSError("simulated write failure")

    monkeypatch.setattr(Image.Image, "save", fail_save)

    with pytest.raises(RuntimeError, match="RAW rendering failed"):
        render_raw_image(source_path, output_dir=output_dir)

    assert not list(output_dir.glob("*.jpg"))


def test_render_raw_image_supports_background_white_balance(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    raw = _DummyRaw(
        np.array(
            [
                [[0.15, 0.25, 0.35], [0.40, 0.50, 0.60]],
                [[0.20, 0.30, 0.40], [0.90, 0.85, 0.80]],
            ],
            dtype=np.float64,
        )
    )
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    calls: list[RawRenderSettings] = []

    def fake_processing(rgb, settings, *, return_debug=False):
        resolved = RawRenderSettings.from_dict(settings)
        calls.append(resolved)
        return np.asarray(rgb, dtype=np.float64)

    monkeypatch.setattr("utils.raw_render.apply_post_decode_processing", fake_processing)

    output_path = render_raw_image(
        source_path,
        settings=RawRenderSettings(
            white_balance_mode="background",
            wb_selection=(0.0, 0.0, 2.0, 2.0),
        ),
        output_dir=output_dir,
    )

    assert output_path.exists()
    assert raw.kwargs["use_camera_wb"] is True
    assert raw.kwargs["use_auto_wb"] is False
    assert raw.kwargs.get("user_wb") is None
    assert calls[0].white_balance_mode == "custom"
    assert calls[0].wb_multipliers is not None
    assert calls[0].wb_multiplier_space == "post_decode_rgb"
    assert calls[0].wb_sample_base_mode == "camera"


def test_render_raw_image_supports_custom_white_balance(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    raw = _DummyRaw(np.full((2, 2, 3), 0.5, dtype=np.float64))
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    calls: list[RawRenderSettings] = []

    def fake_processing(rgb, settings, *, return_debug=False):
        resolved = RawRenderSettings.from_dict(settings)
        calls.append(resolved)
        return np.asarray(rgb, dtype=np.float64)

    monkeypatch.setattr("utils.raw_render.apply_post_decode_processing", fake_processing)

    output_path = render_raw_image(
        source_path,
        settings=RawRenderSettings(
            white_balance_mode="custom",
            wb_multipliers=(1.2, 1.0, 1.4),
            wb_selection=(0.0, 0.0, 2.0, 2.0),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_base_mode="camera",
            wb_sample_point=(1.0, 1.0),
            wb_sample_size=10,
            wb_selection_space="preview_pixels",
        ),
        output_dir=output_dir,
    )

    assert output_path.exists()
    assert raw.kwargs["use_camera_wb"] is True
    assert raw.kwargs["use_auto_wb"] is False
    assert raw.kwargs.get("user_wb") is None
    assert calls[0].white_balance_mode == "custom"
    assert calls[0].wb_multipliers == (1.2, 1.0, 1.4)
    assert calls[0].wb_multiplier_space == "post_decode_rgb"
    assert calls[0].wb_sample_base_mode == "camera"


def test_render_raw_image_uses_background_multipliers_when_available(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    raw = _DummyRaw(np.full((2, 2, 3), 0.5, dtype=np.float64))
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: _DummyRawpyModule(raw))

    calls: list[RawRenderSettings] = []

    def fake_processing(rgb, settings, *, return_debug=False):
        resolved = RawRenderSettings.from_dict(settings)
        calls.append(resolved)
        return np.asarray(rgb, dtype=np.float64)

    monkeypatch.setattr("utils.raw_render.apply_post_decode_processing", fake_processing)

    output_path = render_raw_image(
        source_path,
        settings=RawRenderSettings(
            white_balance_mode="camera",
            wb_multipliers=(1.2, 1.0, 1.4),
            wb_selection=(0.0, 0.0, 2.0, 2.0),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_base_mode="camera",
            wb_selection_space="preview_pixels",
        ),
        output_dir=output_dir,
    )

    assert output_path.exists()
    assert raw.kwargs["use_camera_wb"] is True
    assert raw.kwargs["use_auto_wb"] is False
    assert raw.kwargs.get("user_wb") is None
    assert calls[0].white_balance_mode == "custom"
    assert calls[0].wb_multipliers == (1.2, 1.0, 1.4)
    assert calls[0].wb_multiplier_space == "post_decode_rgb"
    assert calls[0].wb_sample_base_mode == "camera"


def test_render_raw_sampling_rgb_uses_the_rawpy_path(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    captured: dict[str, object] = {}

    def fake_render_raw_array(rawpy_module, source, settings, *, preview=False, wb_mode=None, user_wb=None):
        captured["rawpy_module"] = rawpy_module
        captured["source"] = str(source)
        captured["preview"] = preview
        captured["settings"] = RawRenderSettings.from_dict(settings)
        return np.array(
            [
                [[0.10, 0.20, 0.30], [0.40, 0.50, 0.60]],
                [[0.70, 0.80, 0.90], [0.15, 0.25, 0.35]],
            ],
            dtype=np.float64,
        )

    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: object())
    monkeypatch.setattr("utils.raw_render._render_raw_array", fake_render_raw_array)

    rgb = render_raw_sampling_rgb(source_path)

    assert captured["source"] == str(source_path)
    assert captured["preview"] is True
    assert isinstance(captured["settings"], RawRenderSettings)
    assert rgb.shape == (2, 2, 3)
    assert np.allclose(rgb, np.array(
        [
            [[0.10, 0.20, 0.30], [0.40, 0.50, 0.60]],
            [[0.70, 0.80, 0.90], [0.15, 0.25, 0.35]],
        ],
        dtype=np.float64,
    ))


def test_build_raw_processing_metadata_includes_rendered_at(tmp_path):
    source_path = tmp_path / "sample.nef"
    derivative_path = tmp_path / "sample.jpg"
    metadata = build_raw_processing_metadata(
        source_path,
        derivative_path,
        RawRenderSettings.default(),
        width=2,
        height=2,
        rendered_at=datetime(2026, 5, 16, 19, 44, 11),
    )

    assert metadata["local_derivative"]["rendered_at"] == "2026:05:16 19:44:11"
