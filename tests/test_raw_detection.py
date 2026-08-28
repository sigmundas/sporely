from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path, raw_mime_type_for_path


REQUIRED_CAMERA_RAW_SUFFIXES = {
    ".orf", ".nef", ".nrw", ".arw", ".sr2", ".srf", ".cr2", ".cr3",
    ".dng", ".raf", ".rw2", ".pef", ".ptx", ".3fr", ".fff", ".iiq",
    ".mos", ".mef", ".mrw", ".x3f", ".rwl", ".kdc", ".dcr", ".erf",
    ".raw",
}


def test_supported_raw_suffixes_are_detected(tmp_path):
    assert REQUIRED_CAMERA_RAW_SUFFIXES <= SUPPORTED_RAW_SUFFIXES
    for suffix in sorted(SUPPORTED_RAW_SUFFIXES):
        path = tmp_path / f"sample{suffix}"
        assert is_raw_image_path(path) is True
        assert is_raw_image_path(tmp_path / f"sample{suffix.upper()}") is True
        assert raw_mime_type_for_path(path) == "image/x-raw"


def test_non_raw_suffixes_are_not_detected(tmp_path):
    for suffix in (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp"):
        path = tmp_path / f"sample{suffix}"
        assert is_raw_image_path(path) is False
        assert raw_mime_type_for_path(path) == "application/octet-stream"
