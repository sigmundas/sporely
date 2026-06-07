from __future__ import annotations

import os
from pathlib import Path

from PIL import Image
import pytest

from utils.exif_reader import get_image_metadata
from utils.local_image_ingest import RawRenderingUnavailableError, prepare_local_ingest_image
from utils.rawpy_import import read_rawpy_capture_datetime


RAW_SMOKE_ENV = "SPORELY_TEST_RAW_FILE"


def test_optional_raw_smoke_rendering(tmp_path):
    raw_file = os.environ.get(RAW_SMOKE_ENV, "").strip()
    if not raw_file:
        pytest.skip(f"Set {RAW_SMOKE_ENV} to run the real-file RAW smoke test")

    source = Path(raw_file).expanduser()
    if not source.exists():
        pytest.skip(f"Smoke RAW file not found: {source}")

    output_dir = tmp_path / "imports"

    try:
        result = prepare_local_ingest_image(
            source,
            lab_metadata={"image_type": "microscope", "contrast": "phase"},
            output_dir=output_dir,
        )
    except RawRenderingUnavailableError:
        pytest.skip("rawpy is not available in this environment")

    output_path = Path(result.working_path)
    assert output_path.exists()
    assert output_path.suffix.lower() == ".jpg"
    assert result.original_path == str(source)
    assert result.source_path == str(source)
    assert result.working_path != result.original_path
    assert result.lab_metadata is not None
    assert result.lab_metadata["contrast"] == "phase"
    assert result.lab_metadata["raw_processing"]["engine"] == "rawpy"
    assert result.lab_metadata["raw_processing"]["source"]["path"] == str(source)
    assert result.lab_metadata["raw_processing"]["source"]["mime_type"] == "image/x-raw"
    assert result.lab_metadata["raw_processing"]["source"]["captured_at"]
    assert result.lab_metadata["raw_processing"]["local_derivative"]["path"] == result.working_path
    assert result.lab_metadata["raw_processing"]["local_derivative"]["mime_type"] == "image/jpeg"
    assert result.lab_metadata["raw_processing"]["local_derivative"]["width"] > 0
    assert result.lab_metadata["raw_processing"]["local_derivative"]["height"] > 0
    assert result.lab_metadata["raw_processing"]["settings"]["white_balance_mode"] == "camera"

    source_timestamp = read_rawpy_capture_datetime(source)
    if source_timestamp is not None:
        assert result.lab_metadata["raw_processing"]["source"]["captured_at"] == source_timestamp.strftime("%Y:%m:%d %H:%M:%S")
        assert get_image_metadata(result.working_path)["datetime"] == source_timestamp

    with Image.open(output_path) as rendered:
        rendered.load()
        assert rendered.format == "JPEG"
