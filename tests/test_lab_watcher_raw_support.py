from utils.lab_watcher import NewImageHandler
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES


def test_lab_watcher_new_image_handler_recognizes_raw_suffixes():
    handler = NewImageHandler(lambda _path: None)

    assert set(SUPPORTED_RAW_SUFFIXES).issubset(handler.valid_extensions)
