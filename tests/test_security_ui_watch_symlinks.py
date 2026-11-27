"""F15: the watched folder ignores links to files outside it."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from utils import lab_watcher

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="symlinks need privileges on Windows")


@pytest.fixture
def folders(tmp_path):
    watched = tmp_path / "watched"
    outside = tmp_path / "outside"
    watched.mkdir()
    outside.mkdir()
    (watched / "real.jpg").write_bytes(b"real image bytes")
    (outside / "secret.jpg").write_bytes(b"outside image bytes")
    os.symlink(outside / "secret.jpg", watched / "link.jpg")
    return watched, outside


def test_list_file_names_skips_symlinks(folders):
    watched, _ = folders
    assert lab_watcher._list_file_names(str(watched)) == {"real.jpg"}


def test_watcher_reports_only_the_real_file(qtbot, folders, monkeypatch):
    watched, _ = folders
    monkeypatch.setattr(lab_watcher, "WATCHER_FOLDER_POLL_SECONDS", 0.02)
    worker = lab_watcher.LabWatcherWorker(str(watched))
    seen = []
    worker.new_image_detected.connect(seen.append)
    worker.start()
    try:
        qtbot.waitUntil(lambda: worker.listing_started, timeout=3000)
        (watched / "new.jpg").write_bytes(b"new real image")
        os.symlink(watched.parent / "outside" / "secret.jpg", watched / "new_link.jpg")
        qtbot.waitUntil(lambda: len(seen) >= 1, timeout=5000)
        qtbot.wait(300)
    finally:
        worker.stop()
    assert [Path(p).name for p in seen] == ["new.jpg"]


def _live_stub(watch_dir, queued):
    from ui.live_lab_tab import LiveLabTab

    return SimpleNamespace(
        is_session_running=lambda: True,
        _active_session_mode="live",
        SESSION_MODE_LIVE="live",
        watch_dir_input=SimpleNamespace(text=lambda: str(watch_dir)),
        _queue_companion_source=lambda path: queued.append(path) or True,
    ), LiveLabTab


def test_rescan_queues_only_the_real_file(folders):
    watched, _ = folders
    queued = []
    stub, cls = _live_stub(watched, queued)
    assert cls.rescan_watch_folder(stub) == 1
    assert [Path(p).name for p in queued] == ["real.jpg"]


def test_queue_rechecks_for_links_before_ingest(folders):
    from ui.live_lab_tab import LiveLabTab

    watched, _ = folders
    stub = SimpleNamespace(_seen_source_paths=set())
    assert LiveLabTab._queue_companion_source(stub, str(watched / "link.jpg")) is False
    assert stub._seen_source_paths == set()
