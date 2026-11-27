"""Startup tightens the app data folder and its secret files (audit F25)."""

from __future__ import annotations

import os
import stat
import sys

import pytest

from utils.private_files import tighten_app_data_permissions

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")

SECRET_AND_DB_NAMES = [
    "app_settings.json",
    "artsobservasjoner_cookies.json",
    "artportalen_cookies.json",
    "inaturalist_oauth_tokens.json",
    "inaturalist_tokens.json",
    "mushrooms.db",
    "mushrooms.db-wal",
    "mushrooms.db-shm",
    "mushrooms.db-journal",
    "reference_values.db",
    "reference_values.db-wal",
]


def _mode(path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _loose_profile(root):
    data = root / "profile"
    data.mkdir()
    os.chmod(data, 0o755)
    for name in SECRET_AND_DB_NAMES:
        path = data / name
        path.write_text("x")
        os.chmod(path, 0o644)
    return data


def test_folder_and_secret_files_become_owner_only(tmp_path):
    data = _loose_profile(tmp_path)
    tighten_app_data_permissions(data)
    assert _mode(data) == 0o700
    for name in SECRET_AND_DB_NAMES:
        assert _mode(data / name) == 0o600, name


def test_unrelated_files_and_subfolders_are_left_alone(tmp_path):
    data = _loose_profile(tmp_path)
    other = data / "notes.txt"
    other.write_text("x")
    os.chmod(other, 0o644)
    images = data / "images"
    images.mkdir()
    os.chmod(images, 0o755)
    tighten_app_data_permissions(data)
    assert _mode(other) == 0o644
    assert _mode(images) == 0o755


def test_database_folder_outside_data_dir_is_untouched(tmp_path):
    data = _loose_profile(tmp_path)
    outside = tmp_path / "chosen_db_folder"
    outside.mkdir()
    os.chmod(outside, 0o755)
    db = outside / "mushrooms.db"
    db.write_text("x")
    os.chmod(db, 0o644)
    tighten_app_data_permissions(data)
    assert _mode(outside) == 0o755
    assert _mode(db) == 0o644


def test_symlink_pointing_outside_is_not_followed(tmp_path):
    data = _loose_profile(tmp_path)
    target = tmp_path / "elsewhere.db"
    target.write_text("x")
    os.chmod(target, 0o644)
    (data / "mushrooms.db").unlink()
    (data / "mushrooms.db").symlink_to(target)
    tighten_app_data_permissions(data)
    assert _mode(target) == 0o644


def test_symlinked_data_folder_is_not_followed(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    os.chmod(real, 0o755)
    secret = real / "app_settings.json"
    secret.write_text("x")
    os.chmod(secret, 0o644)
    link = tmp_path / "link"
    link.symlink_to(real)
    tighten_app_data_permissions(link)
    assert _mode(real) == 0o755
    assert _mode(secret) == 0o644


def test_missing_folder_and_permission_errors_do_not_raise(tmp_path, monkeypatch):
    tighten_app_data_permissions(tmp_path / "does-not-exist")
    data = _loose_profile(tmp_path)

    def boom(*args, **kwargs):
        raise PermissionError("denied")

    monkeypatch.setattr(os, "fchmod", boom)
    tighten_app_data_permissions(data)  # must not raise


def test_migrate_legacy_storage_tightens_the_data_folder(tmp_path, monkeypatch):
    import app_identity

    data = _loose_profile(tmp_path)
    monkeypatch.setenv(app_identity.APP_DATA_DIR_ENV, str(data))
    monkeypatch.setattr(app_identity, "migrate_qsettings", lambda: None)
    app_identity.migrate_legacy_storage()
    assert _mode(data) == 0o700
    assert _mode(data / "artsobservasjoner_cookies.json") == 0o600


def test_files_are_tightened_even_when_folder_chmod_fails(tmp_path, monkeypatch):
    data = _loose_profile(tmp_path)
    real_fchmod = os.fchmod

    def selective(fd, mode):
        if mode == 0o700:
            raise PermissionError("not the owner")
        return real_fchmod(fd, mode)

    monkeypatch.setattr(os, "fchmod", selective)
    tighten_app_data_permissions(data)
    assert _mode(data / "app_settings.json") == 0o600


def test_stranded_temporary_copies_and_hard_links(tmp_path):
    data = _loose_profile(tmp_path)
    for name in (".app_settings.json.123.tmp", "inaturalist_tokens.json.tmp"):
        (data / name).write_text("x")
        os.chmod(data / name, 0o644)
    shared = tmp_path / "shared.db"
    shared.write_text("x")
    os.chmod(shared, 0o644)
    (data / "mushrooms.db").unlink()
    os.link(shared, data / "mushrooms.db")
    tighten_app_data_permissions(data)
    assert _mode(data / ".app_settings.json.123.tmp") == 0o600
    assert _mode(data / "inaturalist_tokens.json.tmp") == 0o600
    assert _mode(shared) == 0o644
