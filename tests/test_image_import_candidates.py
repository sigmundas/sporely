from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

import utils.image_import_candidates as image_import_candidates
from utils.image_import_candidates import (
    IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG,
    IMAGE_IMPORT_SOURCE_KIND_HEIC,
    IMAGE_IMPORT_SOURCE_KIND_RASTER,
    IMAGE_IMPORT_SOURCE_KIND_RAW,
    IMAGE_IMPORT_STATUS_FAILED,
    IMAGE_IMPORT_STATUS_READY,
    IMAGE_IMPORT_STATUS_SKIPPED,
    IMAGE_IMPORT_STATUS_STAGED,
    ImageImportCandidate,
    RawRenderSettings,
    build_image_import_candidates,
    prepare_image_import_candidate,
    prepare_image_import_candidates,
)
from utils.image_metadata_merge import merge_image_lab_metadata
from utils.raw_render import RawRenderingUnavailableError


def _fake_ingest_result(
    *,
    source_path: Path,
    working_path: Path,
    lab_metadata: dict | None = None,
    raw_render_snapshot: dict | None = None,
    provenance: dict | None = None,
) -> SimpleNamespace:
    provenance = dict(provenance or {})

    return SimpleNamespace(
        source_path=str(source_path),
        working_path=str(working_path),
        original_path=str(source_path),
        raw_render_snapshot=raw_render_snapshot,
        lab_metadata=lab_metadata,
        provenance_kwargs=lambda: dict(provenance),
    )


def _install_fake_capture_metadata(monkeypatch, *, raw_dt: datetime, jpeg_dt: datetime, gps: tuple[float, float] = (59.91, 10.75)) -> None:
    def fake_get_image_metadata(image_path: str) -> dict:
        suffix = Path(image_path).suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            return {
                "missing": False,
                "datetime": jpeg_dt,
                "latitude": gps[0],
                "longitude": gps[1],
                "filename": Path(image_path).name,
                "filepath": image_path,
            }
        return {
            "missing": False,
            "datetime": None,
            "latitude": None,
            "longitude": None,
            "filename": Path(image_path).name,
            "filepath": image_path,
        }

    def fake_raw_capture_datetime(image_path: str) -> datetime | None:
        return raw_dt if Path(image_path).suffix.lower() in {".orf", ".nef", ".cr2", ".dng"} else None

    monkeypatch.setattr(image_import_candidates, "get_image_metadata", fake_get_image_metadata)
    monkeypatch.setattr(image_import_candidates, "read_rawpy_capture_datetime", fake_raw_capture_datetime)


def test_build_image_import_candidates_preserves_raw_and_camera_jpeg_companions(monkeypatch, tmp_path):
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")

    raw_dt = datetime(2026, 5, 16, 19, 44, 11)
    jpeg_dt = datetime(2026, 5, 16, 19, 44, 12)
    _install_fake_capture_metadata(monkeypatch, raw_dt=raw_dt, jpeg_dt=jpeg_dt)

    raw_candidates = build_image_import_candidates([raw_path, jpeg_path], source_preference="prefer_raw")
    jpeg_candidates = build_image_import_candidates([raw_path, jpeg_path], source_preference="camera_jpeg")

    assert len(raw_candidates) == 1
    assert len(jpeg_candidates) == 1

    raw_candidate = raw_candidates[0]
    jpeg_candidate = jpeg_candidates[0]

    assert raw_candidate.status == IMAGE_IMPORT_STATUS_STAGED
    assert raw_candidate.source_path == raw_path.resolve()
    assert raw_candidate.selected_path == raw_path.resolve()
    assert raw_candidate.raw_path == raw_path.resolve()
    assert raw_candidate.camera_jpeg_path == jpeg_path.resolve()
    assert raw_candidate.has_raw_companion is True
    assert raw_candidate.selected_source_policy == "prefer_raw"
    assert raw_candidate.source_kind == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert raw_candidate.captured_at == raw_dt
    assert set(map(str, raw_candidate.all_source_paths)) == {str(raw_path.resolve()), str(jpeg_path.resolve())}
    assert raw_candidate.processing_metadata["source"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert raw_candidate.processing_metadata["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW

    assert jpeg_candidate.status == IMAGE_IMPORT_STATUS_STAGED
    assert jpeg_candidate.source_path == raw_path.resolve()
    assert jpeg_candidate.selected_path == jpeg_path.resolve()
    assert jpeg_candidate.raw_path == raw_path.resolve()
    assert jpeg_candidate.camera_jpeg_path == jpeg_path.resolve()
    assert jpeg_candidate.has_raw_companion is True
    assert jpeg_candidate.selected_source_policy == "camera_jpeg"
    assert jpeg_candidate.source_kind == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert jpeg_candidate.captured_at == jpeg_dt
    assert jpeg_candidate.gps_latitude == pytest.approx(59.91)
    assert jpeg_candidate.gps_longitude == pytest.approx(10.75)
    assert set(map(str, jpeg_candidate.all_source_paths)) == {str(raw_path.resolve()), str(jpeg_path.resolve())}
    assert jpeg_candidate.processing_metadata["source"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert jpeg_candidate.processing_metadata["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG


def test_build_image_import_candidates_keeps_jpeg_only_as_raster(monkeypatch, tmp_path):
    jpeg_path = tmp_path / "field.jpg"
    jpeg_path.write_bytes(b"jpeg-bytes")

    jpeg_dt = datetime(2026, 5, 16, 19, 44, 12)
    _install_fake_capture_metadata(monkeypatch, raw_dt=datetime(2026, 5, 16, 19, 44, 11), jpeg_dt=jpeg_dt)

    candidates = build_image_import_candidates([jpeg_path], source_preference="prefer_raw")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.status == IMAGE_IMPORT_STATUS_STAGED
    assert candidate.source_path == jpeg_path.resolve()
    assert candidate.selected_path == jpeg_path.resolve()
    assert candidate.raw_path is None
    assert candidate.camera_jpeg_path == jpeg_path.resolve()
    assert candidate.source_kind == IMAGE_IMPORT_SOURCE_KIND_RASTER
    assert candidate.captured_at == jpeg_dt
    assert candidate.gps_latitude == pytest.approx(59.91)
    assert candidate.gps_longitude == pytest.approx(10.75)
    assert candidate.processing_metadata["source"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RASTER
    assert candidate.processing_metadata["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RASTER


def test_build_image_import_candidates_does_not_collapse_unrelated_raw_and_jpeg_files(monkeypatch, tmp_path):
    raw_path = tmp_path / "A.ORF"
    jpeg_path = tmp_path / "B.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")

    _install_fake_capture_metadata(
        monkeypatch,
        raw_dt=datetime(2026, 5, 16, 19, 44, 11),
        jpeg_dt=datetime(2026, 5, 16, 19, 44, 12),
    )

    candidates = build_image_import_candidates([raw_path, jpeg_path], source_preference="prefer_raw")

    assert len(candidates) == 2
    assert {candidate.selected_path.name for candidate in candidates} == {"A.ORF", "B.JPG"}
    assert {candidate.source_kind for candidate in candidates} == {IMAGE_IMPORT_SOURCE_KIND_RAW, IMAGE_IMPORT_SOURCE_KIND_RASTER}


def test_build_image_import_candidates_keeps_edited_and_copy_suffixes_separate(tmp_path):
    # TODO(stage3): when we add looser stem grouping, update this regression
    # alongside the new heuristics so edited variants do not collapse too early.
    original = tmp_path / "IMG_123.JPG"
    edited = tmp_path / "IMG_123-edited.JPG"
    copied = tmp_path / "IMG_123 (1).JPG"
    for path in (original, edited, copied):
        path.write_bytes(b"jpeg-bytes")

    candidates = build_image_import_candidates([original, edited, copied], source_preference="prefer_raw")

    assert len(candidates) == 3
    assert {candidate.selected_path.name for candidate in candidates} == {
        "IMG_123.JPG",
        "IMG_123-edited.JPG",
        "IMG_123 (1).JPG",
    }


def test_build_image_import_candidates_skips_unsupported_files_consistently(tmp_path):
    unsupported = tmp_path / "notes.txt"
    unsupported.write_text("not an image")

    candidates = build_image_import_candidates([unsupported], source_preference="prefer_raw")

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.status == IMAGE_IMPORT_STATUS_SKIPPED
    assert candidate.failure_reason == "unsupported file suffix"
    assert candidate.error_detail == f"Unsupported image suffix for {unsupported.name}"


def test_image_import_candidate_can_store_raw_calibration_state(tmp_path):
    source_path = tmp_path / "sample.nef"
    derivative_path = tmp_path / "imports" / "sample.jpg"
    source_path.write_bytes(b"raw-bytes")
    derivative_path.parent.mkdir(parents=True, exist_ok=True)
    derivative_path.write_bytes(b"jpeg-bytes")

    candidate = ImageImportCandidate(
        source_path=source_path,
        selected_path=source_path,
        working_path=derivative_path,
        preview_path=derivative_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        status=IMAGE_IMPORT_STATUS_READY,
        raw_path=source_path,
        companion_paths=(source_path, derivative_path),
        working_width=2048,
        working_height=1536,
        processing_settings=RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=False,
        ),
        processing_metadata={"working": {"path": str(derivative_path)}},
    )

    assert candidate.is_raw_backed is True
    assert candidate.primary_preview_path == derivative_path.resolve()
    assert candidate.working_path == derivative_path.resolve()
    assert candidate.source_path == source_path.resolve()
    assert candidate.working_width == 2048
    assert candidate.working_height == 1536
    assert candidate.processing_settings is not None
    assert candidate.processing_settings.white_balance_mode == "camera"
    assert candidate.processing_metadata["working"]["path"] == str(derivative_path)


def test_prepare_image_import_candidate_marks_missing_source_as_failed(tmp_path):
    missing_path = tmp_path / "missing.nef"
    candidate = ImageImportCandidate(
        source_path=missing_path,
        selected_path=missing_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
    )
    caller_metadata = {"objective_name": "40x", "mount_medium": "water"}
    caller_snapshot = deepcopy(caller_metadata)

    prepared = prepare_image_import_candidate(candidate, lab_metadata=caller_metadata)

    assert prepared.status == IMAGE_IMPORT_STATUS_FAILED
    assert prepared.failure_reason == "missing source file"
    assert prepared.error_detail == str(missing_path.resolve())
    assert prepared.lab_metadata == caller_snapshot
    assert caller_metadata == caller_snapshot
    assert prepared.processing_metadata["failure"]["reason"] == "missing source file"


def test_prepare_image_import_candidate_renders_raw_with_metadata_and_dimensions(monkeypatch, tmp_path):
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    output_dir = tmp_path / "imports"
    working_path = output_dir / "sample.jpg"
    caller_metadata = {
        "objective_name": "40x",
        "mount_medium": "water",
        "image_processing": {
            "steps": {
                "denoise": {
                    "enabled": False,
                    "strength": 0.25,
                }
            }
        },
    }
    caller_snapshot = deepcopy(caller_metadata)
    candidate_lab_metadata = {
        "contrast": "phase",
        "sample_type": "spore",
        "image_processing": {
            "steps": {
                "resize": {
                    "factor": 0.75,
                }
            }
        },
    }
    candidate_lab_snapshot = deepcopy(candidate_lab_metadata)
    processing_settings = RawRenderSettings(
        white_balance_mode="auto",
        auto_levels=False,
        tone_curve_enabled=True,
    )
    candidate = ImageImportCandidate(
        source_path=source_path,
        selected_path=source_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        companion_paths=(source_path,),
        raw_path=source_path,
        has_raw_companion=False,
        processing_settings=processing_settings,
        lab_metadata=candidate_lab_metadata,
        processing_metadata={"existing": {"nested": {"value": 1}}},
    )

    captured: dict[str, object] = {}
    raw_render_snapshot = {
        "engine": "rawpy",
        "source": {
            "kind": "camera_raw",
            "path": str(source_path.resolve()),
            "mime_type": "image/x-raw",
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
        },
        "settings": processing_settings.to_dict(),
    }

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        captured["source"] = Path(source)
        captured["raw_settings"] = raw_settings
        captured["lab_metadata"] = deepcopy(lab_metadata)
        captured["output_dir"] = output_dir
        return _fake_ingest_result(
            source_path=Path(source),
            working_path=working_path,
            lab_metadata=merge_image_lab_metadata(lab_metadata, {"raw_processing": raw_render_snapshot}),
            raw_render_snapshot=raw_render_snapshot,
            provenance={
                "source_role": "converted_local",
                "file_purpose": "microscope",
                "original_mime_type": "image/x-raw",
                "working_mime_type": "image/jpeg",
            },
        )

    monkeypatch.setattr(image_import_candidates, "prepare_local_ingest_image", fake_prepare_local_ingest_image)

    prepared = prepare_image_import_candidate(
        candidate,
        lab_metadata=caller_metadata,
        output_dir=output_dir,
    )

    assert captured["source"] == source_path.resolve()
    assert captured["raw_settings"] == processing_settings
    assert captured["output_dir"] == output_dir
    assert candidate.lab_metadata == candidate_lab_snapshot
    assert caller_metadata == caller_snapshot
    assert prepared.status == IMAGE_IMPORT_STATUS_READY
    assert prepared.source_path == source_path.resolve()
    assert prepared.selected_path == source_path.resolve()
    assert prepared.working_path == working_path.resolve()
    assert prepared.preview_path == working_path.resolve()
    assert prepared.working_width == 640
    assert prepared.working_height == 480
    assert prepared.processing_settings == processing_settings
    assert prepared.lab_metadata["contrast"] == "phase"
    assert prepared.lab_metadata["sample_type"] == "spore"
    assert prepared.lab_metadata["objective_name"] == "40x"
    assert prepared.lab_metadata["mount_medium"] == "water"
    assert prepared.lab_metadata["image_processing"]["steps"]["resize"]["factor"] == 0.75
    assert prepared.lab_metadata["image_processing"]["steps"]["denoise"]["enabled"] is False
    assert prepared.lab_metadata["raw_processing"]["engine"] == "rawpy"
    assert prepared.lab_metadata["raw_processing"]["local_derivative"]["path"] == str(working_path.resolve())
    assert prepared.processing_metadata["existing"]["nested"]["value"] == 1
    assert prepared.processing_metadata["raw_processing"]["engine"] == "rawpy"
    assert prepared.processing_metadata["provenance"]["working_mime_type"] == "image/jpeg"
    assert prepared.processing_metadata["source"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert prepared.processing_metadata["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert prepared.processing_metadata["working"]["width"] == 640
    assert prepared.processing_metadata["working"]["height"] == 480
    assert prepared.fallback_used is False


def test_prepare_image_import_candidate_raster_pass_through_keeps_metadata_without_raw_processing(monkeypatch, tmp_path):
    source_path = tmp_path / "field.jpg"
    image = Image.new("RGB", (2, 3), "white")
    image.save(source_path, "JPEG")
    caller_metadata = {"objective_name": "40x", "mount_medium": "water"}
    caller_snapshot = deepcopy(caller_metadata)
    candidate = ImageImportCandidate(
        source_path=source_path,
        selected_path=source_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RASTER,
        lab_metadata={"contrast": "phase", "image_processing": {"steps": {"resize": {"factor": 0.5}}}},
        processing_metadata={"existing": {"nested": {"value": 2}}},
    )

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        assert raw_settings is None
        return _fake_ingest_result(
            source_path=Path(source),
            working_path=Path(source),
            lab_metadata=merge_image_lab_metadata(
                lab_metadata,
                {
                    "image_processing": {
                        "steps": {
                            "denoise": {
                                "enabled": True,
                            }
                        }
                    }
                },
            ),
            provenance={
                "source_role": "local_canonical",
                "file_purpose": "microscope",
                "original_mime_type": "image/jpeg",
                "working_mime_type": "image/jpeg",
            },
        )

    monkeypatch.setattr(image_import_candidates, "prepare_local_ingest_image", fake_prepare_local_ingest_image)

    prepared = prepare_image_import_candidate(candidate, lab_metadata=caller_metadata)

    assert prepared.status == IMAGE_IMPORT_STATUS_READY
    assert prepared.working_path == source_path.resolve()
    assert prepared.working_width == 2
    assert prepared.working_height == 3
    assert prepared.lab_metadata["contrast"] == "phase"
    assert prepared.lab_metadata["objective_name"] == "40x"
    assert prepared.lab_metadata["image_processing"]["steps"]["resize"]["factor"] == 0.5
    assert prepared.lab_metadata["image_processing"]["steps"]["denoise"]["enabled"] is True
    assert "raw_processing" not in prepared.lab_metadata
    assert "raw_processing" not in prepared.processing_metadata
    assert prepared.processing_metadata["provenance"]["source_role"] == "local_canonical"
    assert prepared.processing_metadata["existing"]["nested"]["value"] == 2
    assert caller_metadata == caller_snapshot


def test_prepare_image_import_candidate_marks_heic_conversion_failure_as_failed(monkeypatch, tmp_path):
    source_path = tmp_path / "sample.heic"
    source_path.write_bytes(b"heic-bytes")
    candidate = ImageImportCandidate(
        source_path=source_path,
        selected_path=source_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_HEIC,
    )

    def fake_prepare_local_ingest_image(*args, **kwargs):
        raise RuntimeError(f"HEIC conversion failed for {source_path.name}")

    monkeypatch.setattr(image_import_candidates, "prepare_local_ingest_image", fake_prepare_local_ingest_image)

    prepared = prepare_image_import_candidate(candidate)

    assert prepared.status == IMAGE_IMPORT_STATUS_FAILED
    assert prepared.failure_reason == "heic conversion failed"
    assert "HEIC conversion failed" in prepared.error_detail
    assert prepared.processing_metadata["failure"]["reason"] == "heic conversion failed"


def test_prepare_image_import_candidate_records_fallback_metadata_when_raw_render_fails_and_jpeg_fallback_is_used(
    monkeypatch,
    tmp_path,
):
    raw_path = tmp_path / "sample.nef"
    jpeg_path = tmp_path / "sample.jpg"
    raw_path.write_bytes(b"raw-bytes")
    image = Image.new("RGB", (3, 4), "white")
    image.save(jpeg_path, "JPEG")

    candidate = ImageImportCandidate(
        source_path=raw_path,
        selected_path=raw_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        raw_path=raw_path,
        camera_jpeg_path=jpeg_path,
        has_raw_companion=True,
        selected_source_policy="prefer_raw",
        lab_metadata={
            "contrast": "phase",
            "raw_processing": {"source": {"kind": "camera_raw"}},
        },
        processing_metadata={"existing": {"nested": {"value": 3}}},
    )

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        source_path = Path(source)
        if source_path == raw_path.resolve():
            raise RawRenderingUnavailableError("RAW rendering requires rawpy")
        assert source_path == jpeg_path.resolve()
        return _fake_ingest_result(
            source_path=source_path,
            working_path=source_path,
            lab_metadata=merge_image_lab_metadata(
                lab_metadata,
                {
                    "image_processing": {
                        "steps": {
                            "fallback": {
                                "used": True,
                            }
                        }
                    }
                },
            ),
            provenance={
                "source_role": "local_canonical",
                "file_purpose": "microscope",
                "original_mime_type": "image/jpeg",
                "working_mime_type": "image/jpeg",
            },
        )

    monkeypatch.setattr(image_import_candidates, "prepare_local_ingest_image", fake_prepare_local_ingest_image)

    prepared = prepare_image_import_candidate(candidate)

    assert prepared.status == IMAGE_IMPORT_STATUS_READY
    assert prepared.fallback_used is True
    assert "raw rendering unavailable" in prepared.fallback_reason
    assert prepared.source_path == raw_path.resolve()
    assert prepared.selected_path == jpeg_path.resolve()
    assert prepared.source_kind == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert prepared.working_path == jpeg_path.resolve()
    assert "raw_processing" not in prepared.lab_metadata
    assert prepared.processing_metadata["fallback"]["used"] is True
    assert "raw rendering unavailable" in prepared.processing_metadata["fallback"]["reason"]
    assert prepared.processing_metadata["fallback"]["fallback_path"] == str(jpeg_path.resolve())
    assert prepared.processing_metadata["selected"]["kind"] == IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG
    assert "raw_processing" not in prepared.processing_metadata


def test_prepare_image_import_candidates_continues_after_one_failed_item(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing.nef"
    raster_path = tmp_path / "field.jpg"
    Image.new("RGB", (4, 1), "white").save(raster_path, "JPEG")

    missing_candidate = ImageImportCandidate(
        source_path=missing_path,
        selected_path=missing_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
    )
    raster_candidate = ImageImportCandidate(
        source_path=raster_path,
        selected_path=raster_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RASTER,
        lab_metadata={"contrast": "phase"},
    )

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        source_path = Path(source)
        assert source_path == raster_path.resolve()
        return _fake_ingest_result(
            source_path=source_path,
            working_path=source_path,
            lab_metadata=merge_image_lab_metadata(lab_metadata, {"raw_processing": None}),
            provenance={
                "source_role": "local_canonical",
                "file_purpose": "microscope",
                "original_mime_type": "image/jpeg",
                "working_mime_type": "image/jpeg",
            },
        )

    monkeypatch.setattr(image_import_candidates, "prepare_local_ingest_image", fake_prepare_local_ingest_image)

    prepared_candidates = prepare_image_import_candidates([missing_candidate, raster_candidate], lab_metadata={"objective_name": "40x"})

    assert len(prepared_candidates) == 2
    assert prepared_candidates[0].status == IMAGE_IMPORT_STATUS_FAILED
    assert prepared_candidates[0].failure_reason == "missing source file"
    assert prepared_candidates[1].status == IMAGE_IMPORT_STATUS_READY
    assert prepared_candidates[1].lab_metadata["objective_name"] == "40x"
    assert prepared_candidates[1].lab_metadata["contrast"] == "phase"
