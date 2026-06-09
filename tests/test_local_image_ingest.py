from datetime import datetime
from types import SimpleNamespace
import sqlite3

import numpy as np
import pytest
from PIL import Image

from utils import local_image_ingest
from utils.exif_reader import get_image_metadata
from utils.local_image_ingest import prepare_local_ingest_image
from utils.raw_render import RawRenderSettings
from utils import thumbnail_generator
from utils.thumbnail_generator import generate_all_sizes


def test_prepare_local_ingest_image_passes_through_raster_files(tmp_path):
    source_path = tmp_path / "sample.jpg"
    source_path.write_bytes(b"jpeg-bytes")

    result = prepare_local_ingest_image(source_path, lab_metadata={"image_type": "microscope"})

    assert result.source_path == str(source_path)
    assert result.working_path == str(source_path)
    assert result.original_path == str(source_path)
    assert result.source_role == "local_canonical"
    assert result.file_purpose == "microscope"
    assert result.original_mime_type == "image/jpeg"
    assert result.working_mime_type == "image/jpeg"
    assert result.provenance_kwargs() == {
        "source_role": "local_canonical",
        "file_purpose": "microscope",
        "original_mime_type": "image/jpeg",
        "working_mime_type": "image/jpeg",
    }


def test_prepare_local_ingest_image_uses_heic_conversion(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.heic"
    source_path.write_bytes(b"heic-bytes")
    output_dir = tmp_path / "imports"
    converted_path = output_dir / "sample.jpg"
    converted_path.parent.mkdir(parents=True, exist_ok=True)
    converted_path.write_bytes(b"converted")

    monkeypatch.setattr(
        local_image_ingest,
        "maybe_convert_heic",
        lambda _source, _output_dir: str(converted_path),
    )

    result = prepare_local_ingest_image(source_path, lab_metadata={"image_type": "field"}, output_dir=output_dir)

    assert result.working_path == str(converted_path)
    assert result.original_path == str(source_path)
    assert result.source_role == "converted_local"
    assert result.file_purpose == "field"
    assert result.original_mime_type == "image/heic"
    assert result.working_mime_type == "image/jpeg"


def test_prepare_local_ingest_image_raises_clear_error_for_heic_conversion_failure(tmp_path, monkeypatch):
    source_path = tmp_path / "broken.heic"
    source_path.write_bytes(b"heic-bytes")

    monkeypatch.setattr(
        local_image_ingest,
        "maybe_convert_heic",
        lambda _source, _output_dir: None,
    )

    with pytest.raises(RuntimeError, match="HEIC conversion failed for broken\\.heic"):
        prepare_local_ingest_image(source_path, output_dir=tmp_path / "imports")


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


def test_prepare_local_ingest_image_renders_raw_files(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    timestamp = datetime(2026, 5, 16, 19, 44, 11)
    raw = _DummyRaw(
        np.array(
            [
                [[0.10, 0.20, 0.30], [0.40, 0.50, 0.60]],
                [[0.70, 0.80, 0.90], [0.15, 0.25, 0.35]],
            ],
            dtype=np.float64,
        ),
        timestamp=timestamp,
    )
    dummy_rawpy_module = _DummyRawpyModule(raw)
    monkeypatch.setattr("utils.raw_render.import_rawpy", lambda: dummy_rawpy_module)
    monkeypatch.setattr("utils.rawpy_import.import_rawpy", lambda: dummy_rawpy_module)

    result = prepare_local_ingest_image(
        source_path,
        lab_metadata={"image_type": "microscope", "contrast": "phase"},
        output_dir=output_dir,
    )

    assert result.working_path.endswith(".jpg")
    assert result.working_path != str(source_path)
    assert result.original_path == str(source_path)
    assert result.source_role == "converted_local"
    assert result.file_purpose == "microscope"
    assert result.original_mime_type == "image/x-raw"
    assert result.working_mime_type == "image/jpeg"
    assert result.lab_metadata is not None
    assert result.lab_metadata["contrast"] == "phase"
    assert result.lab_metadata["raw_processing"]["engine"] == "rawpy"
    assert result.lab_metadata["raw_processing"]["source"]["mime_type"] == "image/x-raw"
    assert result.lab_metadata["raw_processing"]["source"]["kind"] == "camera_raw"
    assert result.lab_metadata["raw_processing"]["source"]["path"] == str(source_path)
    assert result.lab_metadata["raw_processing"]["source"]["captured_at"] == "2026:05:16 19:44:11"
    assert result.lab_metadata["raw_processing"]["local_derivative"]["kind"] == "rendered_from_raw"
    assert result.lab_metadata["raw_processing"]["local_derivative"]["format"] == "jpeg"
    assert result.lab_metadata["raw_processing"]["local_derivative"]["mime_type"] == "image/jpeg"
    assert result.lab_metadata["raw_processing"]["local_derivative"]["quality"] == 95
    assert result.lab_metadata["raw_processing"]["local_derivative"]["subsampling"] == 0
    assert result.lab_metadata["raw_processing"]["local_derivative"]["width"] == 2
    assert result.lab_metadata["raw_processing"]["local_derivative"]["height"] == 2
    assert result.lab_metadata["raw_processing"]["local_derivative"]["path"] == result.working_path
    assert result.lab_metadata["raw_processing"]["local_derivative"]["rendered_at"]
    assert result.lab_metadata["raw_processing"]["settings"] == RawRenderSettings.default().to_dict()
    assert result.lab_metadata["raw_processing"]["settings"]["auto_levels"] is True
    assert result.raw_render_snapshot == result.lab_metadata["raw_processing"]
    assert get_image_metadata(result.working_path)["datetime"] == timestamp
    with Image.open(result.working_path) as rendered:
        assert rendered.size == (2, 2)
        assert rendered.format == "JPEG"

    thumb_db = tmp_path / "thumbnails.db"
    with sqlite3.connect(thumb_db) as conn:
        conn.execute(
            "CREATE TABLE thumbnails (id INTEGER PRIMARY KEY AUTOINCREMENT, image_id INTEGER, size_preset TEXT, filepath TEXT)"
        )
        conn.commit()

    monkeypatch.setattr(thumbnail_generator, "THUMBNAIL_DIR", tmp_path / "thumbnails")
    monkeypatch.setattr(thumbnail_generator, "get_connection", lambda: sqlite3.connect(thumb_db))

    thumbs = generate_all_sizes(result.working_path, 42)
    assert "224x224" in thumbs
    thumb_path = thumbs["224x224"]
    with Image.open(thumb_path) as thumb:
        assert thumb.size == (224, 224)
        assert thumb.format in {"JPEG", "WEBP"}


def test_prepare_local_ingest_image_ignores_raw_settings_for_non_raw_files(tmp_path):
    source_path = tmp_path / "sample.jpg"
    source_path.write_bytes(b"jpeg-bytes")

    settings = RawRenderSettings(
        white_balance_mode="auto",
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.8,
        tone_curve_midpoint=0.4,
    )
    result = prepare_local_ingest_image(source_path, raw_settings=settings, lab_metadata={"image_type": "field"})

    assert result.raw_render_snapshot is None
    assert result.lab_metadata == {"image_type": "field"}
    assert "raw_processing" not in result.lab_metadata
