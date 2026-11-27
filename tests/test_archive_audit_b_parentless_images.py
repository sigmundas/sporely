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


def test_older_archive_with_a_parentless_image_still_previews(portable_installation, tmp_path):
    """Archives written before the fix may hold a parentless image; import
    keeps reading them (the pruner's import mode is unchanged)."""
    import hashlib
    import json

    from utils.archive.manifest import ArchiveManifest
    from utils.archive.portable_import import preview_portable_archive

    images_dir = tmp_path / "images"
    images_dir.mkdir()
    selected = images_dir / "selected.jpg"
    selected.write_bytes(b"selected-working")
    schema.save_app_settings({"images_dir": str(images_dir)})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-01')")
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath) VALUES (101, 1, ?)", (str(selected),)
        )
    archive = export_observations({1}, tmp_path / "new.sporely", app_version="test").path

    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist() if info.filename != "manifest.json"
        }
    staged = tmp_path / "old.db"
    staged.write_bytes(members["portable/mushrooms.db"])
    connection = sqlite3.connect(staged)
    try:
        connection.execute("INSERT INTO images (id, observation_id, filepath) VALUES (909, NULL, '')")
        connection.commit()
    finally:
        connection.close()
    members["portable/mushrooms.db"] = staged.read_bytes()
    value = manifest.to_dict()
    value["contents"]["images"] += 1
    for entry in value["files"]:
        if entry["path"] == "portable/mushrooms.db":
            entry["size"] = len(members["portable/mushrooms.db"])
            entry["sha256"] = hashlib.sha256(members["portable/mushrooms.db"]).hexdigest()
    old = tmp_path / "old.sporely"
    with ZipFile(old, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)

    preview = preview_portable_archive(old)
    assert [item.observation_id for item in preview.observations] == [1]
