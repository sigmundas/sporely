from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

import ui.ingestion_hub_tab as ingestion_hub_tab
from ui.ingestion_hub_tab import IngestionHubTab
from utils.image_import_candidates import (
    IMAGE_IMPORT_SOURCE_KIND_RAW,
    IMAGE_IMPORT_STATUS_FAILED,
    IMAGE_IMPORT_STATUS_READY,
    IMAGE_IMPORT_STATUS_SKIPPED,
    IMAGE_IMPORT_STATUS_STAGED,
    ImageImportCandidate,
)


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


def build_tab(monkeypatch, saved_settings: list[tuple[str, float]] | None = None) -> IngestionHubTab:
    if saved_settings is None:
        saved_settings = []
    monkeypatch.setattr(ingestion_hub_tab.SettingsDB, "get_setting", lambda _key, default=None: default)
    monkeypatch.setattr(
        ingestion_hub_tab.SettingsDB,
        "set_setting",
        lambda key, value: saved_settings.append((key, value)),
    )
    monkeypatch.setattr(ingestion_hub_tab.TemporalMatcher, "load_sessions", lambda self, observation_id=None: [])
    monkeypatch.setattr(ingestion_hub_tab.ImageDB, "get_images_for_observation", lambda observation_id: [])
    monkeypatch.setattr(ingestion_hub_tab.ObservationDB, "get_observation", lambda observation_id: None)
    monkeypatch.setattr(ingestion_hub_tab, "load_objectives", lambda: {})
    monkeypatch.setattr(ingestion_hub_tab, "install_persistent_splitter", lambda *args, **kwargs: None)
    return IngestionHubTab(DummyMainWindow())


def test_import_match_uses_candidate_branch_and_merges_candidate_metadata(
    monkeypatch,
    qapp,
    tmp_path,
):
    tab = build_tab(monkeypatch)
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    converted_path = tmp_path / "imports" / "P070020_1_rendered.jpg"
    raw_path.write_bytes(b"raw-bytes")
    image = Image.new("RGB", (4, 4), "white")
    image.save(jpeg_path, "JPEG")
    converted_path.parent.mkdir(parents=True, exist_ok=True)
    converted_path.write_bytes(b"jpeg-bytes")

    candidate = ImageImportCandidate(
        source_path=raw_path,
        selected_path=raw_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        status=IMAGE_IMPORT_STATUS_STAGED,
        companion_paths=(raw_path, jpeg_path),
        raw_path=raw_path,
        camera_jpeg_path=jpeg_path,
        has_raw_companion=True,
        selected_source_policy="prefer_raw",
        captured_at=datetime(2026, 5, 1, 12, 0, 0),
        lab_metadata={
            "candidate_flag": "yes",
            "image_processing": {
                "steps": {
                    "denoise": {
                        "enabled": False,
                    }
                }
            },
        },
        processing_metadata={
            "provenance": {
                "source_role": "converted_local",
                "file_purpose": "microscope",
                "original_mime_type": "image/x-raw",
                "working_mime_type": "image/jpeg",
            }
        },
    )
    prepared_candidate = deepcopy(candidate)
    prepared_candidate.status = IMAGE_IMPORT_STATUS_READY
    prepared_candidate.working_path = converted_path
    prepared_candidate.preview_path = converted_path

    prepare_calls: list[tuple[ImageImportCandidate, dict]] = []

    def fake_prepare(candidate_arg, **kwargs):
        prepare_calls.append((candidate_arg, dict(kwargs)))
        return deepcopy(prepared_candidate)

    monkeypatch.setattr(ingestion_hub_tab, "prepare_image_import_candidate", fake_prepare)
    monkeypatch.setattr(
        ingestion_hub_tab,
        "prepare_local_ingest_image",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy ingest should not run")),
    )
    monkeypatch.setattr(ingestion_hub_tab, "get_images_dir", lambda: tmp_path / "images")
    monkeypatch.setattr(ingestion_hub_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(ingestion_hub_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ingestion_hub_tab.ImageDB,
        "get_image",
        lambda image_id: {"filepath": str(converted_path)},
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(ingestion_hub_tab.ImageDB, "add_image", lambda **kwargs: captured.update(kwargs) or 77)
    monkeypatch.setattr(
        ingestion_hub_tab,
        "load_objectives",
        lambda: {"40x": {"microns_per_pixel": 0.25}},
    )
    monkeypatch.setattr(
        ingestion_hub_tab,
        "resolve_objective_key",
        lambda objective_key, objectives: objective_key if objective_key in objectives else None,
    )
    monkeypatch.setattr(ingestion_hub_tab.CalibrationDB, "get_active_calibration_id", lambda objective_key: 99)

    result = tab._import_match(
        1,
        {
            "filepath": str(raw_path.resolve()),
            "image_type": "microscope",
            "session_id": "session-1",
            "session_kind": "microscope_session",
            "state": {
                "objective_name": "40x",
                "contrast": "phase",
                "mount_medium": "water",
                "stain": "none",
                "sample_type": "spore",
            },
            "captured_at": datetime(2026, 5, 1, 12, 0, 0),
            "adjusted_at": datetime(2026, 5, 1, 12, 5, 0),
            "candidate": candidate,
        },
    )

    assert result == 77
    assert len(prepare_calls) == 1
    assert prepare_calls[0][0].raw_path == raw_path.resolve()
    assert prepare_calls[0][1]["lab_metadata"] == {"image_type": "microscope"}
    assert captured["filepath"] == str(converted_path)
    assert captured["original_filepath"] == str(raw_path.resolve())
    assert captured["image_type"] == "microscope"
    assert captured["calibration_id"] == 99
    assert captured["scale"] == pytest.approx(0.25)
    assert captured["source_role"] == "converted_local"
    assert captured["file_purpose"] == "microscope"
    assert captured["original_mime_type"] == "image/x-raw"
    assert captured["working_mime_type"] == "image/jpeg"
    assert captured["lab_metadata"]["session_id"] == "session-1"
    assert captured["lab_metadata"]["session_kind"] == "microscope_session"
    assert captured["lab_metadata"]["objective_name"] == "40x"
    assert captured["lab_metadata"]["candidate_flag"] == "yes"
    assert captured["lab_metadata"]["image_processing"]["steps"]["denoise"]["enabled"] is False
    assert captured["lab_metadata"]["matched_at"] == "2026-05-01T12:05:00"


def test_add_match_rows_continues_after_failures_and_summarizes_counts(monkeypatch, qapp):
    tab = build_tab(monkeypatch)
    committed_ids: list[tuple[int, int]] = []
    status_messages: list[str] = []
    progress_updates: list[tuple[str, int | None]] = []
    recompute_calls: list[bool] = []

    monkeypatch.setattr(tab, "_selected_observation_id", lambda: 1)
    monkeypatch.setattr(tab, "_set_hint_progress_visible", lambda *args, **kwargs: None)
    monkeypatch.setattr(tab, "_set_hint_progress", lambda text, value=None: progress_updates.append((text, value)))
    monkeypatch.setattr(tab, "_recompute_matches", lambda: recompute_calls.append(True))
    monkeypatch.setattr(
        tab,
        "_refresh_main_window_after_commit",
        lambda observation_id, image_id: committed_ids.append((observation_id, image_id)),
    )
    monkeypatch.setattr(tab, "_show_status", lambda text, **kwargs: status_messages.append(text))

    rows = [
        {"filepath": "/tmp/one.jpg"},
        {"filepath": "/tmp/two.nef"},
        {"filepath": "/tmp/three.txt"},
    ]
    commit_results = [
        {"image_id": 11, "status": "added"},
        {
            "image_id": 0,
            "status": IMAGE_IMPORT_STATUS_FAILED,
            "failure_reason": "candidate preparation failed",
            "error_detail": "boom",
        },
        {
            "image_id": 0,
            "status": IMAGE_IMPORT_STATUS_SKIPPED,
            "failure_reason": "unsupported file suffix",
            "error_detail": "notes.txt",
        },
    ]
    commit_calls: list[tuple[int, str, bool]] = []

    def fake_commit(observation_id, row, *, show_status=True):
        commit_calls.append((observation_id, str(row.get("filepath") or ""), bool(show_status)))
        return commit_results.pop(0)

    monkeypatch.setattr(tab, "_commit_match_row", fake_commit)

    tab._add_match_rows(rows)

    assert [call[0] for call in commit_calls] == [1, 1, 1]
    assert [call[2] for call in commit_calls] == [False, False, False]
    assert recompute_calls == [True]
    assert committed_ids == [(1, 11)]
    assert any("Added 1 image(s) to the observation." in message for message in status_messages)
    assert any("1 failed" in message for message in status_messages)
    assert any("1 skipped" in message for message in status_messages)
