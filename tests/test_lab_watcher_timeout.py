import logging
import time
from pathlib import Path
from types import SimpleNamespace

from utils.lab_watcher import NewImageHandler


def _event_for(path: Path) -> SimpleNamespace:
    return SimpleNamespace(src_path=str(path), dest_path=str(path), is_directory=False)


def _install_fake_clock(monkeypatch):
    clock = {"value": 0.0}

    def fake_monotonic():
        return clock["value"]

    def fake_sleep(seconds):
        clock["value"] += float(seconds)

    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    monkeypatch.setattr(time, "sleep", fake_sleep)
    return clock


def _install_size_sequence(monkeypatch, target_path: Path, sizes: list[int]):
    state = {"sizes": list(sizes), "last": sizes[-1] if sizes else 0}
    target_text = str(target_path)

    def fake_stat(self):
        if str(self) == target_text:
            if state["sizes"]:
                state["last"] = state["sizes"].pop(0)
            return SimpleNamespace(st_size=state["last"])
        return SimpleNamespace(st_size=0)

    monkeypatch.setattr(Path, "stat", fake_stat, raising=False)
    return state


def test_new_image_handler_emits_after_file_stabilizes(monkeypatch, tmp_path):
    path = tmp_path / "capture.jpg"
    path.write_bytes(b"jpeg-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [12, 12, 12])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=5.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
    )

    handler._emit_if_supported(str(path))

    assert calls == [str(path)]
    assert str(path.resolve()) in handler._handled_paths


def test_new_image_handler_uses_longer_timeout_for_raw_files(monkeypatch, tmp_path):
    path = tmp_path / "capture.nef"
    path.write_bytes(b"raw-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [1, 2, 3, 3, 3])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=3.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
    )

    handler._emit_if_supported(str(path))

    assert calls == [str(path)]
    assert str(path.resolve()) in handler._handled_paths


def test_new_image_handler_logs_timeout_but_does_not_mark_path_handled(monkeypatch, tmp_path, caplog):
    """A timed-out path must NOT be added to _handled_paths so it can be retried."""
    path = tmp_path / "stuck.jpg"
    path.write_bytes(b"jpeg-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [1, 2, 3, 4, 5])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=3.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
        timeout_log_cooldown_seconds=0.0,
    )

    with caplog.at_level(logging.WARNING, logger="utils.lab_watcher"):
        handler._emit_if_supported(str(path))

    assert calls == []
    # Key invariant: timed-out path is NOT in _handled_paths
    assert str(path.resolve()) not in handler._handled_paths
    assert any("Timed out waiting for" in record.message for record in caplog.records)
    assert any(path.name in record.message for record in caplog.records)


def test_new_image_handler_timeout_path_can_be_retried_after_stabilizing(monkeypatch, tmp_path, caplog):
    """After a timeout, a later watcher event for the same path should succeed."""
    path = tmp_path / "slow.jpg"
    path.write_bytes(b"jpeg-bytes")
    clock = _install_fake_clock(monkeypatch)
    calls: list[str] = []

    # First attempt: sizes keep growing → timeout
    size_state = {"sizes": [1, 2, 3, 4, 5], "last": 5}
    target_text = str(path)

    call_count = {"n": 0}

    def fake_stat(self):
        if str(self) == target_text:
            if size_state["sizes"]:
                size_state["last"] = size_state["sizes"].pop(0)
            return SimpleNamespace(st_size=size_state["last"])
        return SimpleNamespace(st_size=0)

    monkeypatch.setattr(Path, "stat", fake_stat, raising=False)

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=3.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
        timeout_log_cooldown_seconds=0.0,
    )

    with caplog.at_level(logging.WARNING, logger="utils.lab_watcher"):
        handler._emit_if_supported(str(path))

    assert calls == []
    assert str(path.resolve()) not in handler._handled_paths

    # Second attempt: file has now stabilised
    size_state["sizes"] = [100, 100, 100]
    size_state["last"] = 100

    handler._emit_if_supported(str(path))

    assert calls == [str(path)]
    assert str(path.resolve()) in handler._handled_paths


def test_new_image_handler_successful_emit_suppresses_later_duplicates(monkeypatch, tmp_path):
    """Once emitted, later watcher events for the same path are skipped."""
    path = tmp_path / "capture.jpg"
    path.write_bytes(b"jpeg-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [8, 8, 8])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=5.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
    )

    handler._emit_if_supported(str(path))
    handler._emit_if_supported(str(path))

    assert calls == [str(path)]
    assert str(path.resolve()) in handler._handled_paths


def test_new_image_handler_timeout_log_cooldown_suppresses_repeated_warnings(monkeypatch, tmp_path, caplog):
    """Repeated timeouts within the cooldown window should not log again."""
    path = tmp_path / "noisy.jpg"
    path.write_bytes(b"jpeg-bytes")
    clock = _install_fake_clock(monkeypatch)

    size_state = {"sizes": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "last": 10}
    target_text = str(path)

    def fake_stat(self):
        if str(self) == target_text:
            if size_state["sizes"]:
                size_state["last"] = size_state["sizes"].pop(0)
            return SimpleNamespace(st_size=size_state["last"])
        return SimpleNamespace(st_size=0)

    monkeypatch.setattr(Path, "stat", fake_stat, raising=False)

    handler = NewImageHandler(
        lambda _: None,
        stability_timeout_seconds=3.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
        timeout_log_cooldown_seconds=120.0,
    )

    with caplog.at_level(logging.WARNING, logger="utils.lab_watcher"):
        handler._emit_if_supported(str(path))

    first_warning_count = sum(1 for r in caplog.records if "Timed out" in r.message)
    assert first_warning_count == 1

    # Second timeout within cooldown — should NOT log again
    size_state["sizes"] = [11, 12, 13, 14, 15]
    size_state["last"] = 15
    caplog.clear()

    with caplog.at_level(logging.WARNING, logger="utils.lab_watcher"):
        handler._emit_if_supported(str(path))

    second_warning_count = sum(1 for r in caplog.records if "Timed out" in r.message)
    assert second_warning_count == 0
    # Path still not in _handled_paths — retry is still allowed
    assert str(path.resolve()) not in handler._handled_paths


def test_new_image_handler_emits_for_moved_in_complete_file(monkeypatch, tmp_path):
    path = tmp_path / "moved.jpg"
    path.write_bytes(b"jpeg-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [9, 9, 9])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=5.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
    )

    handler.on_moved(_event_for(path))

    assert calls == [str(path)]


def test_new_image_handler_ignores_unsupported_suffix(monkeypatch, tmp_path):
    path = tmp_path / "notes.txt"
    path.write_text("not an image")
    calls: list[str] = []

    handler = NewImageHandler(calls.append)

    handler.on_created(_event_for(path))

    assert calls == []


def test_new_image_handler_deduplicates_create_and_move_events(monkeypatch, tmp_path):
    path = tmp_path / "duplicate.jpg"
    path.write_bytes(b"jpeg-bytes")
    _install_fake_clock(monkeypatch)
    _install_size_sequence(monkeypatch, path, [8, 8, 8])
    calls: list[str] = []

    handler = NewImageHandler(
        calls.append,
        stability_timeout_seconds=5.0,
        raw_stability_timeout_seconds=10.0,
        poll_seconds=1.0,
    )

    handler.on_created(_event_for(path))
    handler.on_moved(_event_for(path))

    assert calls == [str(path)]
    assert str(path.resolve()) in handler._handled_paths
