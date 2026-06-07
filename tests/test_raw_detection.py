from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path, raw_mime_type_for_path


def test_supported_raw_suffixes_are_detected(tmp_path):
    for suffix in sorted(SUPPORTED_RAW_SUFFIXES):
        path = tmp_path / f"sample{suffix}"
        assert is_raw_image_path(path) is True
        assert raw_mime_type_for_path(path) == "image/x-raw"


def test_non_raw_suffixes_are_not_detected(tmp_path):
    path = tmp_path / "sample.jpg"
    assert is_raw_image_path(path) is False
    assert raw_mime_type_for_path(path) == "application/octet-stream"
