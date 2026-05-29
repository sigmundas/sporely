import os
import uuid

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ui import calibration_dialog
from ui.calibration_dialog import (
    _calibration_reference_ui_state,
    _find_cached_calibration_reference_path,
)


def test_calibration_reference_ui_state_prefers_local_original_and_hides_download(tmp_path):
    calibration_uuid = str(uuid.uuid4())
    local_path = tmp_path / "reference.jpg"
    local_path.write_bytes(b"local")
    cache_path = tmp_path / "cache" / "reference.webp"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"cache")

    ui_state = _calibration_reference_ui_state(
        {
            "calibration_uuid": calibration_uuid,
            "image_filepath": str(local_path),
            "image_storage_path": f"user-123/{calibration_uuid}/reference.webp",
            "local_original_exists": True,
            "local_original_missing": False,
            "recovery_available": True,
        },
        cache_path=cache_path,
    )

    assert ui_state["status_text"] == "Local reference image available"
    assert ui_state["status_tone"] == "success"
    assert ui_state["show_download_button"] is False
    assert ui_state["preview_path"] is None
    assert ui_state["source_role"] == "local_original"


def test_calibration_reference_ui_state_shows_cached_cloud_reference_and_download_button(tmp_path):
    calibration_uuid = str(uuid.uuid4())
    cache_path = tmp_path / "appdata" / "cloud_cache" / "calibrations" / calibration_uuid / "reference.png"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"cache")

    ui_state = _calibration_reference_ui_state(
        {
            "calibration_uuid": calibration_uuid,
            "image_filepath": str(tmp_path / "missing-reference.jpg"),
            "image_storage_path": f"user-123/{calibration_uuid}/reference.png",
            "local_original_exists": False,
            "local_original_missing": True,
            "recovery_available": True,
        },
        cache_path=cache_path,
    )

    assert ui_state["status_text"] == "Cloud-derived reference only"
    assert ui_state["status_tone"] == "tip"
    assert ui_state["show_download_button"] is True
    assert ui_state["download_button_enabled"] is True
    assert ui_state["preview_path"] == cache_path
    assert ui_state["source_role"] == "cloud_recovery_cache"


def test_find_cached_calibration_reference_path_uses_calibration_uuid_root(tmp_path, monkeypatch):
    app_root = tmp_path / "appdata"
    monkeypatch.setattr(calibration_dialog.cloud_sync, "app_data_dir", lambda: app_root)

    calibration_uuid = str(uuid.uuid4())
    cache_path = app_root / "cloud_cache" / "calibrations" / calibration_uuid / "reference.webp"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"cache")

    assert _find_cached_calibration_reference_path(calibration_uuid) == cache_path
    assert _find_cached_calibration_reference_path("objective_100X") is None
