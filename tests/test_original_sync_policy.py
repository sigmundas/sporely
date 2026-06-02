from pathlib import Path

import pytest

from utils import original_sync_policy as policy


def _write_file(path: Path, content: str = "data") -> str:
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_full_resolution_original_sync_setting_defaults_off(monkeypatch):
    monkeypatch.setattr(policy.SettingsDB, "get_setting", lambda key, default=None: default)

    assert policy.is_full_resolution_original_sync_enabled() is False


@pytest.mark.parametrize(
    ("filename", "file_purpose"),
    [
        ("field.jpg", "field"),
        ("microscope.png", "microscope"),
    ],
)
def test_local_canonical_jpeg_png_are_eligible(tmp_path, filename, file_purpose):
    filepath = _write_file(tmp_path / filename)
    row = {
        "filepath": filepath,
        "source_role": "local_canonical",
        "file_purpose": file_purpose,
    }

    assert policy.is_full_original_sync_candidate(row) is True


def test_heic_original_lineage_requires_original_path_and_opt_in(tmp_path):
    working_path = _write_file(tmp_path / "working.jpg")
    original_path = _write_file(tmp_path / "source.heic")
    row = {
        "filepath": working_path,
        "original_filepath": original_path,
        "source_role": "converted_local",
        "file_purpose": "microscope",
    }

    assert policy.is_full_original_sync_candidate(row) is False
    assert policy.is_full_original_sync_candidate(row, include_original_path=True) is True

    Path(original_path).unlink()
    assert policy.is_full_original_sync_candidate(row, include_original_path=True) is False


def test_converted_local_working_copy_requires_explicit_opt_in(tmp_path):
    working_path = _write_file(tmp_path / "working.jpg")
    row = {
        "filepath": working_path,
        "original_filepath": None,
        "source_role": "converted_local",
        "file_purpose": "microscope",
    }

    assert policy.is_full_original_sync_candidate(row) is False
    assert policy.is_full_original_sync_candidate(row, include_converted_local=True) is True


@pytest.mark.parametrize(
    ("source_role", "file_purpose"),
    [
        ("cloud_derivative", "field"),
        ("cloud_recovery_cache", "cache"),
        ("generated_artifact", "thumbnail"),
        ("generated_artifact", "spore_crop"),
        ("generated_artifact", "plot"),
    ],
)
def test_cloud_derivative_and_generated_rows_are_never_eligible(
    tmp_path,
    source_role,
    file_purpose,
):
    filepath = _write_file(tmp_path / f"{source_role}_{file_purpose}.jpg")
    row = {
        "filepath": filepath,
        "source_role": source_role,
        "file_purpose": file_purpose,
    }

    assert policy.is_full_original_sync_candidate(row) is False


def test_should_download_full_original_stays_disabled_for_current_cloud_rows(monkeypatch):
    monkeypatch.setattr(policy.SettingsDB, "get_setting", lambda key, default=None: True if key == policy.SYNC_FULL_RESOLUTION_ORIGINALS_SETTING else default)

    assert policy.should_download_full_original(
        {"storage_path": "user/obs/derivative.webp"},
        None,
    ) is False


def test_should_download_full_original_refuses_to_overwrite_local_canonical_files(monkeypatch, tmp_path):
    monkeypatch.setattr(policy.SettingsDB, "get_setting", lambda key, default=None: True if key == policy.SYNC_FULL_RESOLUTION_ORIGINALS_SETTING else default)

    remote_meta = {"original_storage_path": "user/obs/original.jpg"}
    local_path = _write_file(tmp_path / "local.jpg")
    local_row = {
        "filepath": local_path,
        "source_role": "local_canonical",
        "file_purpose": "field",
    }

    assert policy.should_download_full_original(remote_meta, None) is True
    assert policy.should_download_full_original(remote_meta, local_row) is False


def test_should_download_full_original_refuses_to_overwrite_local_heic_lineage(monkeypatch, tmp_path):
    monkeypatch.setattr(policy.SettingsDB, "get_setting", lambda key, default=None: True if key == policy.SYNC_FULL_RESOLUTION_ORIGINALS_SETTING else default)

    remote_meta = {"original_storage_path": "user/obs/original.heic"}
    working_path = _write_file(tmp_path / "working.jpg")
    original_path = _write_file(tmp_path / "source.heic")
    local_row = {
        "filepath": working_path,
        "original_filepath": original_path,
        "source_role": "converted_local",
        "file_purpose": "microscope",
    }

    assert policy.should_download_full_original(remote_meta, local_row) is False
