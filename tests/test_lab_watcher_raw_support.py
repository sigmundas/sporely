import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QObject
from PySide6.QtWidgets import QApplication

from config import (
    RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
)
from utils.lab_watcher import NewImageHandler
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES
from ui.live_lab_tab import LiveLabTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _DummyLiveTab(QObject):
    SESSION_MODE_LIVE = "live"

    def __init__(self, ingest_results: dict[str, bool] | None = None) -> None:
        super().__init__()
        self._active_session_mode = self.SESSION_MODE_LIVE
        self._session_observation_id = 1
        self.RAW_CAPTURE_MODE_AUTO_SAVE = LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE
        self.RAW_CAPTURE_MODE_REVIEW = LiveLabTab.RAW_CAPTURE_MODE_REVIEW
        self._raw_capture_mode = LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE
        self._raw_companion_source_preference = RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW
        self._seen_source_paths: set[str] = set()
        self._pending_companion_groups: dict[str, dict[str, object]] = {}
        self._consumed_companion_groups: set[str] = set()
        self._raw_companion_hold_ms = 5000
        self._ingest_results = {str(key): bool(value) for key, value in (ingest_results or {}).items()}
        self.calls: list[str] = []
        self._normalize_raw_capture_mode = lambda value: LiveLabTab._normalize_raw_capture_mode(self, value)
        self._selected_raw_capture_mode = lambda: self._raw_capture_mode
        self._normalize_raw_companion_source_preference = (
            lambda value: LiveLabTab._normalize_raw_companion_source_preference(self, value)
        )
        self._selected_raw_companion_source_preference = (
            lambda: self._raw_companion_source_preference
        )
        self._queue_companion_source = lambda source: LiveLabTab._queue_companion_source(self, source)
        self._flush_companion_group = lambda group_key: LiveLabTab._flush_companion_group(self, group_key)
        self._companion_state_for_path = lambda source: LiveLabTab._companion_state_for_path(self, source)
        self._clear_companion_group = lambda group_key: LiveLabTab._clear_companion_group(self, group_key)
        self._handle_raw_companion_source = lambda source, group_key, state: LiveLabTab._handle_raw_companion_source(
            self,
            source,
            group_key=group_key,
            state=state,
        )
        self._fallback_companion_path = (
            lambda source, exclude_path=None: LiveLabTab._fallback_companion_path(
                self,
                source,
                exclude_path=exclude_path,
            )
        )
        self._same_stem_companion_paths = lambda source: LiveLabTab._same_stem_companion_paths(self, source)

    def is_session_running(self) -> bool:
        return True

    def _ingest_detected_image(self, source_path: str) -> bool:
        source = str(source_path or "").strip()
        if not source:
            return False
        self.calls.append(source)
        return self._ingest_results.get(source, True)


def test_lab_watcher_new_image_handler_recognizes_raw_suffixes():
    handler = NewImageHandler(lambda _path: None)

    assert set(SUPPORTED_RAW_SUFFIXES).issubset(handler.valid_extensions)


def test_live_lab_tab_only_attempts_raw_ingest_once(tmp_path, qapp):
    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")
    dummy = _DummyLiveTab()

    LiveLabTab._on_new_image_detected(dummy, str(raw_path))
    LiveLabTab._on_new_image_detected(dummy, str(raw_path))

    assert dummy.calls == [str(raw_path)]
    assert str(raw_path) in dummy._seen_source_paths


def test_live_lab_tab_prefers_raw_when_raw_and_jpeg_companions_both_exist(tmp_path, qapp):
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    dummy = _DummyLiveTab()

    LiveLabTab._on_new_image_detected(dummy, str(jpeg_path))
    LiveLabTab._on_new_image_detected(dummy, str(raw_path))

    assert dummy.calls == [str(raw_path)]
    assert str(jpeg_path) in dummy._seen_source_paths
    assert str(raw_path) in dummy._seen_source_paths


def test_live_lab_tab_can_prefer_camera_jpeg_for_companions(tmp_path, qapp):
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    dummy = _DummyLiveTab()
    dummy._raw_companion_source_preference = RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG

    LiveLabTab._on_new_image_detected(dummy, str(raw_path))
    LiveLabTab._on_new_image_detected(dummy, str(jpeg_path))
    LiveLabTab._flush_companion_group(dummy, LiveLabTab._companion_state_for_path(dummy, str(raw_path))[0])

    assert dummy.calls == [str(jpeg_path)]
    assert str(raw_path) in dummy._seen_source_paths
    assert str(jpeg_path) in dummy._seen_source_paths


def test_live_lab_tab_falls_back_to_jpeg_once_when_raw_rendering_fails(tmp_path, qapp):
    raw_path = tmp_path / "P070020_2.ORF"
    jpeg_path = tmp_path / "P070020_2.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    dummy = _DummyLiveTab(ingest_results={str(raw_path): False, str(jpeg_path): True})

    LiveLabTab._on_new_image_detected(dummy, str(raw_path))
    LiveLabTab._on_new_image_detected(dummy, str(jpeg_path))

    assert dummy.calls == [str(raw_path), str(jpeg_path)]
    assert str(raw_path) in dummy._seen_source_paths
    assert dummy._consumed_companion_groups


def test_live_lab_tab_rescan_watch_folder_queues_supported_images_only(monkeypatch, tmp_path, qapp):
    raw_path = tmp_path / "P070020_3.ORF"
    jpeg_path = tmp_path / "P070020_3.JPG"
    notes_path = tmp_path / "notes.txt"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    notes_path.write_text("not an image")

    dummy = _DummyLiveTab()
    dummy.watch_dir_input = SimpleNamespace(text=lambda: str(tmp_path))

    queued_sources: list[str] = []
    monkeypatch.setattr(
        LiveLabTab,
        "_queue_companion_source",
        lambda self, source: queued_sources.append(str(source)) or True,
    )

    queued = LiveLabTab.rescan_watch_folder(dummy)

    assert queued == 2
    assert set(queued_sources) == {str(raw_path.resolve()), str(jpeg_path.resolve())}
