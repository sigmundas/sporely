import pytest

from utils import local_image_ingest
from utils.local_image_ingest import RawRenderingUnavailableError, prepare_local_ingest_image
from utils.raw_render import RawRenderSettings


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


def test_prepare_local_ingest_image_rejects_raw_files(tmp_path):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")

    with pytest.raises(RawRenderingUnavailableError):
        prepare_local_ingest_image(source_path, lab_metadata={"image_type": "microscope"})


def test_prepare_local_ingest_image_preserves_raw_settings_snapshot(tmp_path):
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

    assert result.raw_render_snapshot == settings.to_dict()
