from config import (
    LOCAL_IMPORT_IMAGE_FILTER,
    RAW_FORMATS,
    RASTER_IMAGE_FILTER,
    SUPPORTED_FORMATS,
)
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES


def test_supported_formats_are_raster_only() -> None:
    assert SUPPORTED_FORMATS == RASTER_IMAGE_FILTER
    for suffix in SUPPORTED_RAW_SUFFIXES:
        assert f"*{suffix}" not in SUPPORTED_FORMATS


def test_local_import_filter_includes_raw_suffixes() -> None:
    for suffix in SUPPORTED_RAW_SUFFIXES:
        assert f"*{suffix}" in LOCAL_IMPORT_IMAGE_FILTER


def test_raw_formats_match_raw_detection_suffixes() -> None:
    assert RAW_FORMATS == tuple(sorted(SUPPORTED_RAW_SUFFIXES))
