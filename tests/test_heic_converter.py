import sys
import types
from pathlib import Path

from PIL import Image

from utils.heic_converter import build_local_image_provenance, maybe_convert_heic


class _FakeHeifImage:
    def __init__(self, image):
        self._image = image

    def to_pillow(self):
        return self._image


def _install_fake_pillow_heif(monkeypatch, image):
    fake_module = types.SimpleNamespace(
        register_heif_opener=lambda: None,
        open_heif=lambda _path: _FakeHeifImage(image),
    )
    monkeypatch.setitem(sys.modules, "pillow_heif", fake_module)


def test_maybe_convert_heic_writes_jpeg_working_copy(tmp_path, monkeypatch):
    source_path = tmp_path / "sample.heic"
    source_path.write_bytes(b"fake heic bytes")
    output_dir = tmp_path / "imports"
    _install_fake_pillow_heif(monkeypatch, Image.new("RGB", (4, 4), "white"))

    converted_path = maybe_convert_heic(str(source_path), output_dir)

    assert converted_path is not None
    converted = Path(converted_path)
    assert converted.suffix.lower() in {".jpg", ".jpeg"}
    assert converted.exists()

    with Image.open(converted) as saved_image:
        assert saved_image.format == "JPEG"

    provenance = build_local_image_provenance(source_path, converted, image_type="microscope")
    assert provenance["source_role"] == "converted_local"
    assert provenance["file_purpose"] == "microscope"
    assert provenance["original_mime_type"] == "image/heic"
    assert provenance["working_mime_type"] == "image/jpeg"


def test_maybe_convert_heic_leaves_non_heic_paths_unchanged(tmp_path):
    source_path = tmp_path / "sample.jpg"
    source_path.write_bytes(b"fake jpeg bytes")

    result = maybe_convert_heic(str(source_path), tmp_path / "imports")

    assert result == str(source_path)


def test_build_local_image_provenance_accepts_calibration_and_cache_purposes(tmp_path):
    source_path = tmp_path / "sample.jpg"
    working_path = tmp_path / "working.jpg"
    source_path.write_bytes(b"source")
    working_path.write_bytes(b"working")

    calibration = build_local_image_provenance(source_path, working_path, image_type="calibration")
    cache = build_local_image_provenance(source_path, working_path, image_type="cache")

    assert calibration["file_purpose"] == "calibration"
    assert calibration["source_role"] == "local_canonical"
    assert cache["file_purpose"] == "cache"
    assert cache["source_role"] == "local_canonical"
