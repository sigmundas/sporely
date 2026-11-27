"""F05 (2026-11-26 security audit): a selected-observation export must not
carry images that belong to no observation."""

import sqlite3
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.portable_export import export_observations


@pytest.fixture
def portable_installation(monkeypatch, tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", app_root)
    monkeypatch.setattr(schema, "DATABASE_PATH", app_root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", app_root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", app_root / "app_settings.json")
    schema.init_database()
    return app_root


def test_portable_export_drops_parentless_images(portable_installation, tmp_path):
    images_dir = tmp_path / "images"
    images_dir.mkdir()
    selected = images_dir / "selected.jpg"
    selected.write_bytes(b"selected-working")
    orphan = images_dir / "orphan.jpg"
    orphan.write_bytes(b"PARENTLESS_IMAGE_SENTINEL")
    schema.save_app_settings({"images_dir": str(images_dir)})

    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-01')")
        connection.executemany(
            "INSERT INTO images (id, observation_id, filepath) VALUES (?, ?, ?)",
            [(101, 1, str(selected)), (909, None, str(orphan))],
        )

    destination = tmp_path / "out.sporely"
    result = export_observations({1}, destination, app_version="test")

    with ZipFile(result.path) as archive:
        names = archive.namelist()
        assert not any("/909/" in name for name in names), names
        assert any("/101/" in name for name in names), names
        for name in names:
            assert b"PARENTLESS_IMAGE_SENTINEL" not in archive.read(name)
        staged = tmp_path / "staged.db"
        staged.write_bytes(archive.read("portable/mushrooms.db"))
    with sqlite3.connect(staged) as connection:
        ids = {row[0] for row in connection.execute("SELECT id FROM images")}
    assert ids == {101}
    assert result.manifest.contents["images"] == 1
