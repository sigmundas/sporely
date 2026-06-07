from utils.lab_watcher import NewImageHandler
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES
from ui.live_lab_tab import LiveLabTab


def test_lab_watcher_new_image_handler_recognizes_raw_suffixes():
    handler = NewImageHandler(lambda _path: None)

    assert set(SUPPORTED_RAW_SUFFIXES).issubset(handler.valid_extensions)


def test_live_lab_tab_only_attempts_failed_raw_ingest_once():
    class _DummyLiveTab:
        SESSION_MODE_LIVE = "live"

        def __init__(self) -> None:
            self._active_session_mode = self.SESSION_MODE_LIVE
            self._seen_source_paths: set[str] = set()
            self.calls: list[str] = []

        def is_session_running(self) -> bool:
            return True

        def _ingest_detected_image(self, source_path: str) -> None:
            self.calls.append(source_path)

    dummy = _DummyLiveTab()

    LiveLabTab._on_new_image_detected(dummy, "/tmp/sample.nef")
    LiveLabTab._on_new_image_detected(dummy, "/tmp/sample.nef")

    assert dummy.calls == ["/tmp/sample.nef"]
    assert "/tmp/sample.nef" in dummy._seen_source_paths
