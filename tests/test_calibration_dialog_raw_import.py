import json
import os
from datetime import datetime
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

from ui import calibration_dialog
from ui.calibration_dialog import CalibrationDialog, _AutoCalibrationWorker
from utils.image_import_candidates import (
    IMAGE_IMPORT_SOURCE_KIND_RAW,
    IMAGE_IMPORT_STATUS_FAILED,
    IMAGE_IMPORT_STATUS_READY,
    ImageImportCandidate,
)
from utils.raw_render import RawRenderSettings


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_dialog(monkeypatch) -> CalibrationDialog:
    monkeypatch.setattr(calibration_dialog, "load_objectives", lambda: {})
    monkeypatch.setattr(
        calibration_dialog.cloud_sync.SporelyCloudClient,
        "from_stored_credentials",
        lambda: None,
    )
    monkeypatch.setattr(
        calibration_dialog.SettingsDB,
        "get_setting",
        lambda key, default=None: default,
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration_history",
        lambda objective_key: [],
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration_usage_summary",
        lambda objective_key: [],
    )
    monkeypatch.setattr(
        calibration_dialog,
        "save_objectives",
        lambda _objectives: None,
    )
    monkeypatch.setattr(
        calibration_dialog,
        "get_camera_model",
        lambda path: "TestCam" if str(path or "").endswith(".nef") else None,
    )

    dialog = CalibrationDialog()
    dialog.current_objective_key = "objective_1"
    dialog.objectives = {
        "objective_1": {
            "na": 0.75,
            "microns_per_pixel": 0.012345,
            "objective_name": "40x",
        }
    }
    return dialog


def _raw_candidate(
    *,
    source_path: Path,
    working_path: Path,
    companion_jpeg: Path,
) -> ImageImportCandidate:
    raw_render_snapshot = {
        "engine": "rawpy",
        "source": {
            "kind": "camera_raw",
            "path": str(source_path.resolve()),
            "mime_type": "image/x-raw",
            "captured_at": "2026-05-01 12:00:00",
        },
        "local_derivative": {
            "kind": "rendered_from_raw",
            "format": "jpeg",
            "mime_type": "image/jpeg",
            "path": str(working_path.resolve()),
            "quality": 95,
            "subsampling": 0,
            "width": 640,
            "height": 480,
            "rendered_at": "2026-05-01 12:01:00",
        },
        "settings": RawRenderSettings(
            white_balance_mode="auto",
            auto_levels=False,
            tone_curve_enabled=True,
        ).to_dict(),
    }
    return ImageImportCandidate(
        source_path=source_path,
        selected_path=source_path,
        working_path=working_path,
        preview_path=working_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        status=IMAGE_IMPORT_STATUS_READY,
        companion_paths=(source_path, companion_jpeg),
        raw_path=source_path,
        camera_jpeg_path=companion_jpeg,
        has_raw_companion=True,
        selected_source_policy="prefer_raw",
        captured_at=datetime(2026, 5, 1, 12, 0, 0),
        gps_latitude=12.34,
        gps_longitude=56.78,
        working_width=640,
        working_height=480,
        processing_settings=RawRenderSettings(
            white_balance_mode="auto",
            auto_levels=False,
            tone_curve_enabled=True,
        ),
        processing_metadata={
            "status": IMAGE_IMPORT_STATUS_READY,
            "source": {
                "path": str(source_path.resolve()),
                "kind": IMAGE_IMPORT_SOURCE_KIND_RAW,
            },
            "selected": {
                "path": str(source_path.resolve()),
                "kind": IMAGE_IMPORT_SOURCE_KIND_RAW,
                "policy": "prefer_raw",
            },
            "working": {
                "path": str(working_path.resolve()),
                "width": 640,
                "height": 480,
            },
            "raw_processing": raw_render_snapshot,
        },
        lab_metadata={
            "image_processing": {
                "steps": {
                    "resize": {"factor": 0.5},
                    "denoise": {"enabled": False},
                }
            }
        },
    )


def test_load_calibration_image_candidates_preserves_raw_provenance_and_reports_failures(
    qapp,
    monkeypatch,
    tmp_path,
):
    dialog = _build_dialog(monkeypatch)

    source_path = tmp_path / "sample.nef"
    companion_jpeg = tmp_path / "sample.jpg"
    working_path = tmp_path / "imports" / "sample.jpg"
    failed_path = tmp_path / "bad.heic"
    working_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(b"raw-bytes")
    failed_path.write_bytes(b"heic-bytes")
    Image.new("RGB", (8, 6), "white").save(companion_jpeg, "JPEG")
    Image.new("RGB", (8, 6), "white").save(working_path, "JPEG")

    ready_candidate = _raw_candidate(
        source_path=source_path,
        working_path=working_path,
        companion_jpeg=companion_jpeg,
    )
    failed_candidate = ImageImportCandidate(
        source_path=failed_path,
        selected_path=failed_path,
        source_kind="heic",
        status=IMAGE_IMPORT_STATUS_FAILED,
        failure_reason="heic conversion failed",
        error_detail="pillow-heif unavailable",
    )

    monkeypatch.setattr(
        calibration_dialog,
        "build_image_import_candidates",
        lambda paths, source_preference: [ready_candidate, failed_candidate],
    )
    monkeypatch.setattr(
        calibration_dialog,
        "prepare_image_import_candidates",
        lambda candidates, **kwargs: list(candidates),
    )

    captured = {}

    def _capture_failures(failures, loaded_count):
        captured["failures"] = list(failures)
        captured["loaded_count"] = loaded_count

    monkeypatch.setattr(dialog, "_show_calibration_load_failures", _capture_failures)

    dialog._load_calibration_image_candidates([str(source_path), str(failed_path)])

    assert len(dialog.calibration_images) == 1
    image = dialog.calibration_images[0]
    assert image["path"] == str(working_path.resolve())
    assert image["working_path"] == str(working_path.resolve())
    assert image["source_path"] == str(source_path.resolve())
    assert image["original_path"] == str(source_path.resolve())
    assert image["candidate_status"] == IMAGE_IMPORT_STATUS_READY
    assert image["source_kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert image["selected_source_policy"] == "prefer_raw"
    assert image["processing_metadata"]["working"]["path"] == str(working_path.resolve())
    assert image["processing_metadata"]["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert image["raw_processing"]["engine"] == "rawpy"
    assert image["image_processing"]["steps"]["resize"]["factor"] == 0.5
    assert image["processing_settings"]["white_balance_mode"] == "auto"
    assert image["derivative_width"] == 640
    assert image["derivative_height"] == 480
    assert isinstance(image["captured_at"], datetime)
    assert image["gps_latitude"] == pytest.approx(12.34)
    assert image["gps_longitude"] == pytest.approx(56.78)
    assert captured["loaded_count"] == 1
    assert len(captured["failures"]) == 1
    assert "bad.heic" in captured["failures"][0]
    assert "heic conversion failed" in captured["failures"][0]


def test_auto_worker_fails_when_crop_prep_cannot_load_raster(monkeypatch, tmp_path):
    working_path = tmp_path / "working.jpg"
    Image.new("RGB", (32, 24), "white").save(working_path, "JPEG")

    calibrate_calls = []

    def _record_calibrate(*args, **kwargs):
        calibrate_calls.append((args, kwargs))
        return None

    monkeypatch.setattr(calibration_dialog.slide_calibration, "calibrate_image", _record_calibrate)

    def _fail_image_open(*args, **kwargs):
        raise OSError("crop load failed")

    monkeypatch.setattr(calibration_dialog.Image, "open", _fail_image_open)

    worker = _AutoCalibrationWorker(
        image_path=str(working_path),
        spacing_um=10.0,
        use_edges=True,
        crop_box=(0.1, 0.1, 0.9, 0.9),
    )
    failed_messages = []
    worker.failed.connect(lambda message: failed_messages.append(message))

    worker.run()

    assert failed_messages
    assert "Could not prepare a raster crop for calibration." in failed_messages[0]
    assert calibrate_calls == []


def test_save_calibration_serializes_raw_provenance_fields(
    qapp,
    monkeypatch,
    tmp_path,
):
    dialog = _build_dialog(monkeypatch)

    source_path = tmp_path / "sample.nef"
    companion_jpeg = tmp_path / "sample.jpg"
    working_path = tmp_path / "imports" / "sample.jpg"
    working_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(b"raw-bytes")
    Image.new("RGB", (8, 6), "white").save(companion_jpeg, "JPEG")
    Image.new("RGB", (640, 480), "white").save(working_path, "JPEG")

    candidate = _raw_candidate(
        source_path=source_path,
        working_path=working_path,
        companion_jpeg=companion_jpeg,
    )
    entry, failure = dialog._calibration_image_entry_from_candidate(candidate)
    assert failure is None
    assert entry is not None
    entry["measurements"] = [
        {
            "known_um": 10.0,
            "measured_px": 100.0,
            "line_coords": [0.0, 0.0, 10.0, 10.0],
            "image_index": 0,
        }
    ]
    dialog.calibration_images = [entry]
    dialog.current_image_index = 0

    saved_kwargs = {}

    def _capture_add_calibration(**kwargs):
        saved_kwargs.update(kwargs)
        return 123

    monkeypatch.setattr(
        calibration_dialog,
        "get_calibrations_dir",
        lambda: tmp_path / "calibrations",
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_active_calibration",
        lambda objective_key: None,
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "add_calibration",
        _capture_add_calibration,
    )

    dialog._on_save_calibration()

    payload = json.loads(saved_kwargs["measurements_json"])
    assert saved_kwargs["image_filepath"] == payload["images"][0]["working_path"]
    assert saved_kwargs["camera"] == "TestCam"
    assert saved_kwargs["calibration_image_width"] == 640
    assert saved_kwargs["calibration_image_height"] == 480

    image_entry = payload["images"][0]
    assert image_entry["source_path"] == str(source_path.resolve())
    assert image_entry["original_path"] == str(source_path.resolve())
    assert image_entry["selected_path"] == str(source_path.resolve())
    assert image_entry["path"] == image_entry["working_path"]
    assert image_entry["candidate_status"] == IMAGE_IMPORT_STATUS_READY
    assert image_entry["source_kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert image_entry["selected_source_policy"] == "prefer_raw"
    assert sorted(Path(path).name for path in image_entry["companion_paths"]) == [
        "sample.jpg",
        "sample.nef",
    ]
    assert image_entry["processing_metadata"]["working"]["path"] == str(working_path.resolve())
    assert image_entry["raw_processing"]["engine"] == "rawpy"
    assert image_entry["image_processing"]["steps"]["resize"]["factor"] == 0.5
    assert image_entry["processing_settings"]["white_balance_mode"] == "auto"
    assert image_entry["derivative_width"] == 640
    assert image_entry["derivative_height"] == 480
    assert isinstance(image_entry["captured_at"], str)
    assert image_entry["captured_at"].startswith("2026-05-01")
    assert image_entry["gps_latitude"] == pytest.approx(12.34)
    assert image_entry["gps_longitude"] == pytest.approx(56.78)

