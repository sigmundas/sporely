import os
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.ingestion_hub_tab as ingestion_hub_tab
from ui.ingestion_hub_tab import IngestionHubTab


class DummyMainWindow:
    active_observation_id = 0

    def refresh_observation_images(self, *args, **kwargs):
        pass

    def update_measurements_table(self):
        pass


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def build_tab(monkeypatch, saved_settings: list[tuple[str, float]] | None = None) -> tuple[IngestionHubTab, list[tuple[str, float]]]:
    if saved_settings is None:
        saved_settings = []
    monkeypatch.setattr(ingestion_hub_tab.SettingsDB, "get_setting", lambda _key, default=None: default)
    monkeypatch.setattr(ingestion_hub_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))
    monkeypatch.setattr(ingestion_hub_tab.TemporalMatcher, "load_sessions", lambda self, observation_id=None: [])
    monkeypatch.setattr(ingestion_hub_tab.ImageDB, "get_images_for_observation", lambda observation_id: [])
    monkeypatch.setattr(ingestion_hub_tab.ObservationDB, "get_observation", lambda observation_id: None)
    monkeypatch.setattr(ingestion_hub_tab, "load_objectives", lambda: {})
    tab = IngestionHubTab(DummyMainWindow())
    app = QApplication.instance()
    if app is not None:
        app.processEvents()
    return tab, saved_settings


def _make_sync_shot_decode_result(session_id: str = "session-1") -> dict:
    return {
        "matches": [
            {
                "session_id": session_id,
                "utc_text": "2026-05-01T11:55:00Z",
                "utc_dt": datetime(2026, 5, 1, 11, 55, 0, tzinfo=timezone.utc),
            }
        ],
        "multiple": False,
    }


def test_choose_sync_shot_image_uses_file_picker(monkeypatch, qapp):
    tab, _saved_settings = build_tab(monkeypatch)
    chosen_path = "/tmp/chosen-sync-shot.jpg"
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        ingestion_hub_tab.QFileDialog,
        "getOpenFileName",
        lambda *args, **kwargs: (chosen_path, "Images"),
    )
    monkeypatch.setattr(
        tab,
        "_apply_sync_shot_path",
        lambda path, **kwargs: calls.append((path, kwargs)) or True,
    )

    tab._choose_sync_shot_image()

    assert calls == [
        (
            chosen_path,
            {"show_status": True, "show_missing_qr_warning": True},
        )
    ]


def test_apply_sync_shot_before_scan_sets_offset_without_excluding_path(monkeypatch, qapp, tmp_path):
    tab, saved_settings = build_tab(monkeypatch)
    qr_path = tmp_path / "qr-outside-batch.jpg"
    qr_path.write_bytes(b"")
    normalized_qr_path = str(qr_path.resolve())
    captured_at = datetime(2026, 5, 1, 12, 0, 0)
    offset_args: dict[str, object] = {}
    recorded: dict[str, object] = {}

    monkeypatch.setattr(ingestion_hub_tab, "get_image_datetime", lambda _path: captured_at)
    monkeypatch.setattr(ingestion_hub_tab, "decode_sync_shot_qr", lambda _path: _make_sync_shot_decode_result())
    monkeypatch.setattr(
        ingestion_hub_tab,
        "choose_sync_shot_offset",
        lambda captured_at_arg, qr_utc_dt_arg: offset_args.update(
            {
                "captured_at": captured_at_arg,
                "qr_utc_dt": qr_utc_dt_arg,
            }
        )
        or {
            "basis": "local",
            "offset_seconds": -300.0,
            "captured_at": captured_at_arg,
            "qr_utc_dt": qr_utc_dt_arg,
        },
    )
    monkeypatch.setattr(tab, "_recompute_matches", lambda: recorded.setdefault("recomputed", True))

    assert tab._apply_sync_shot_path(normalized_qr_path) is True

    assert recorded["recomputed"] is True
    assert offset_args["captured_at"] == captured_at
    assert tab._sync_shot_image_path == normalized_qr_path
    assert normalized_qr_path not in tab._excluded_paths
    assert tab.offset_spin.value() == pytest.approx(-300.0)
    assert saved_settings[-1] == (tab.SETTING_OFFSET_SECONDS, -300.0)


def test_apply_sync_shot_excludes_qr_image_when_it_is_in_the_batch(monkeypatch, qapp, tmp_path):
    tab, saved_settings = build_tab(monkeypatch)
    qr_path = tmp_path / "qr-inside-batch.jpg"
    qr_path.write_bytes(b"")
    normalized_qr_path = str(qr_path.resolve())
    captured_at = datetime(2026, 5, 1, 12, 0, 0)
    tab._batch_images = [
        {
            "filepath": normalized_qr_path,
            "filename": qr_path.name,
            "captured_at": captured_at,
            "has_capture_time": True,
        }
    ]
    recorded: dict[str, object] = {}
    offset_args: dict[str, object] = {}

    monkeypatch.setattr(ingestion_hub_tab, "get_image_datetime", lambda _path: captured_at)
    monkeypatch.setattr(ingestion_hub_tab, "decode_sync_shot_qr", lambda _path: _make_sync_shot_decode_result())
    monkeypatch.setattr(
        ingestion_hub_tab,
        "choose_sync_shot_offset",
        lambda captured_at_arg, qr_utc_dt_arg: offset_args.update(
            {
                "captured_at": captured_at_arg,
                "qr_utc_dt": qr_utc_dt_arg,
            }
        )
        or {
            "basis": "local",
            "offset_seconds": -300.0,
            "captured_at": captured_at_arg,
            "qr_utc_dt": qr_utc_dt_arg,
        },
    )

    def fake_match_images(image_rows, *, offset_seconds, observation_tolerance_seconds, exclude_paths):
        recorded["offset"] = offset_seconds
        recorded["exclude_paths"] = set(exclude_paths or [])
        return {"matches": [], "unmatched": [], "observation_counts": {}}

    monkeypatch.setattr(tab._matcher, "match_images", fake_match_images)

    assert tab._apply_sync_shot_path(normalized_qr_path) is True

    assert recorded["offset"] == pytest.approx(-300.0)
    assert recorded["exclude_paths"] == {normalized_qr_path}
    assert offset_args["captured_at"] == captured_at
    assert normalized_qr_path in tab._excluded_paths
    assert tab._sync_shot_image_path == normalized_qr_path
    assert saved_settings[-1] == (tab.SETTING_OFFSET_SECONDS, -300.0)


def test_scan_folder_does_not_attempt_sync_shot_auto_detection(monkeypatch, qapp, tmp_path):
    tab, _saved_settings = build_tab(monkeypatch)
    first = tmp_path / "a.jpg"
    second = tmp_path / "b.jpg"
    first.write_bytes(b"")
    second.write_bytes(b"")
    rows_by_path = {
        str(first.resolve()): [
            {
                "filepath": str(first.resolve()),
                "filename": first.name,
                "captured_at": datetime(2026, 5, 1, 12, 0, 0),
                "has_capture_time": True,
            }
        ],
        str(second.resolve()): [
            {
                "filepath": str(second.resolve()),
                "filename": second.name,
                "captured_at": datetime(2026, 5, 1, 12, 1, 0),
                "has_capture_time": True,
            }
        ],
    }
    monkeypatch.setattr(
        tab._matcher,
        "prepare_image_rows",
        lambda paths: [row for path in paths for row in rows_by_path.get(str(Path(path).resolve()), [])],
    )
    monkeypatch.setattr(tab, "_recompute_matches", lambda: None)
    monkeypatch.setattr(tab, "_auto_apply_clock_offset_if_helpful", lambda: False)

    def fail_if_called():
        raise AssertionError("_attempt_auto_apply_sync_shot should not run during folder scans")

    monkeypatch.setattr(tab, "_attempt_auto_apply_sync_shot", fail_if_called)
    monkeypatch.setattr(tab, "_show_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(tab, "_set_hint_progress_visible", lambda *args, **kwargs: None)
    monkeypatch.setattr(tab, "_set_hint_progress", lambda *args, **kwargs: None)

    tab.scan_dir_input.setText(str(tmp_path))
    tab._scan_folder()

    assert [row["filepath"] for row in tab._batch_images] == [str(first.resolve()), str(second.resolve())]


def test_recompute_matches_groups_images_by_observation_and_uses_offset(monkeypatch, qapp, tmp_path):
    tab, _saved_settings = build_tab(monkeypatch)
    first = tmp_path / "obs-one.jpg"
    second = tmp_path / "obs-two.jpg"
    first.write_bytes(b"")
    second.write_bytes(b"")
    tab._batch_images = [
        {
            "filepath": str(first.resolve()),
            "filename": first.name,
            "captured_at": datetime(2026, 5, 1, 12, 0, 0),
            "has_capture_time": True,
        },
        {
            "filepath": str(second.resolve()),
            "filename": second.name,
            "captured_at": datetime(2026, 5, 1, 12, 1, 0),
            "has_capture_time": True,
        },
    ]
    tab.offset_spin.blockSignals(True)
    tab.offset_spin.setValue(42.5)
    tab.offset_spin.blockSignals(False)
    recorded: dict[str, object] = {}

    def fake_match_images(image_rows, *, offset_seconds, observation_tolerance_seconds, exclude_paths):
        recorded["offset"] = offset_seconds
        recorded["exclude_paths"] = set(exclude_paths or [])
        return {
            "matches": [
                {
                    "filepath": str(first.resolve()),
                    "filename": first.name,
                    "captured_at": datetime(2026, 5, 1, 12, 0, 0),
                    "adjusted_at": datetime(2026, 5, 1, 12, 0, 0),
                    "observation_id": 11,
                    "image_type": "field",
                    "state": {},
                    "notes": None,
                    "session_id": None,
                    "session_kind": None,
                    "already_imported": False,
                },
                {
                    "filepath": str(second.resolve()),
                    "filename": second.name,
                    "captured_at": datetime(2026, 5, 1, 12, 1, 0),
                    "adjusted_at": datetime(2026, 5, 1, 12, 1, 0),
                    "observation_id": 22,
                    "image_type": "field",
                    "state": {},
                    "notes": None,
                    "session_id": None,
                    "session_kind": None,
                    "already_imported": False,
                },
            ],
            "unmatched": [],
            "observation_counts": {},
        }

    monkeypatch.setattr(tab._matcher, "match_images", fake_match_images)

    tab._recompute_matches()

    assert recorded["offset"] == pytest.approx(42.5)
    assert set(tab._matches_by_observation) == {11, 22}
    assert len(tab._matches_by_observation[11]) == 1
    assert len(tab._matches_by_observation[22]) == 1


def test_clear_sync_shot_resets_offset_and_recomputes_matches(monkeypatch, qapp, tmp_path):
    tab, saved_settings = build_tab(monkeypatch)
    batch_path = tmp_path / "batch.jpg"
    batch_path.write_bytes(b"")
    normalized_batch_path = str(batch_path.resolve())
    tab._batch_images = [
        {
            "filepath": normalized_batch_path,
            "filename": batch_path.name,
            "captured_at": datetime(2026, 5, 1, 12, 0, 0),
            "has_capture_time": True,
        }
    ]
    tab._sync_shot_record = {
        "session_id": "session-1",
        "created_utc_text": "2026-05-01T11:55:00Z",
        "applied_utc_text": "2026-05-01T11:55:00Z",
    }
    tab._sync_shot_image_path = normalized_batch_path
    tab._excluded_paths = {normalized_batch_path}
    tab.offset_spin.blockSignals(True)
    tab.offset_spin.setValue(17.5)
    tab.offset_spin.blockSignals(False)
    recorded: dict[str, object] = {}

    def fake_match_images(image_rows, *, offset_seconds, observation_tolerance_seconds, exclude_paths):
        recorded["offset"] = offset_seconds
        recorded["exclude_paths"] = set(exclude_paths or [])
        return {"matches": [], "unmatched": [], "observation_counts": {}}

    monkeypatch.setattr(tab._matcher, "match_images", fake_match_images)

    tab._clear_sync_shot()

    assert tab._sync_shot_record is None
    assert tab._sync_shot_image_path is None
    assert normalized_batch_path not in tab._excluded_paths
    assert recorded["offset"] == pytest.approx(0.0)
    assert saved_settings[-1] == (tab.SETTING_OFFSET_SECONDS, 0.0)
