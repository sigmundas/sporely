import numpy as np
import pytest
from PIL import Image

from utils.raw_render import (
    RAW_DERIVATIVE_FORMAT,
    RAW_DERIVATIVE_QUALITY,
    RAW_DERIVATIVE_SUBSAMPLING,
    RawRenderSettings,
    render_raw_image,
)


class _DummyRaw:
    def __init__(self, rgb: np.ndarray) -> None:
        self._rgb = rgb
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
        auto_levels=True,
        black_percentile=0.01,
        white_percentile=0.99,
        tone_curve_enabled=True,
        tone_curve_strength=0.75,
        tone_curve_midpoint=0.42,
        output_bps=8,
    )

    assert RawRenderSettings.from_dict(settings.to_dict()) == settings


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

    output_path = render_raw_image(
        source_path,
        settings=RawRenderSettings(
            white_balance_mode="background",
            wb_selection=(0.0, 0.0, 2.0, 2.0),
        ),
        output_dir=output_dir,
    )

    assert output_path.exists()
    assert raw.kwargs["use_camera_wb"] is False
    assert raw.kwargs["use_auto_wb"] is False
    assert raw.kwargs["user_wb"] is not None
    assert len(raw.kwargs["user_wb"]) == 4
