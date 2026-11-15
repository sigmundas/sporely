"""Bundle import keeps every asset inside the images folder and never
replaces bytes that existing observations still use."""

import sqlite3
import zipfile
from pathlib import Path

import pytest

from database import schema
from utils import db_share


def _bundle_db(db_path: Path, filepath: str, original_filepath: str | None = None) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                folder_path TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                filepath TEXT,
                original_filepath TEXT,
                image_type TEXT,
                sort_order INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute("INSERT INTO observations (id, date) VALUES (1, '2026-05-01')")
        conn.execute(
            "INSERT INTO images (id, observation_id, filepath, original_filepath, image_type, sort_order) "
            "VALUES (11, 1, ?, ?, 'field', 0)",
            (filepath, original_filepath),
        )
        conn.commit()
    finally:
        conn.close()


def _write_bundle(
    tmp_path: Path, filepath: str, members: dict[str, bytes], original_filepath: str | None = None
) -> Path:
    db_path = tmp_path / "bundle-src.sqlite"
    if db_path.exists():
        db_path.unlink()
    _bundle_db(db_path, filepath, original_filepath)
    bundle = tmp_path / f"bundle-{len(list(tmp_path.glob('bundle-*.zip')))}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(db_path, arcname="mushrooms.db")
        for name, data in members.items():
            zf.writestr(name, data)
    return bundle


@pytest.fixture
def dest(tmp_path, monkeypatch):
    profile_dir = tmp_path / "profile"
    images_dir = profile_dir / "images"
    images_dir.mkdir(parents=True)
    dest_db = profile_dir / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: dest_db)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    schema.init_database()
    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(dest_db))
    monkeypatch.setattr(db_share, "get_images_dir", lambda: images_dir)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: profile_dir / "objectives.json")
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)
    return profile_dir, images_dir, dest_db


def _assert_placeholder(path: str | None, images_dir: Path, name: str) -> None:
    """A refused or missing asset keeps an absolute, unused path in the images folder."""
    assert path is not None
    placeholder = Path(path)
    assert placeholder.is_absolute()
    assert placeholder.name == name
    assert placeholder.parent.parent == images_dir
    assert placeholder.parent.name.startswith(".missing-import-")
    assert not placeholder.exists() and not placeholder.parent.exists()


def _import(bundle: Path) -> dict:
    return db_share.import_database_bundle(
        str(bundle),
        include_observations=True,
        include_images=True,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=False,
    )


def _imported_filepaths(dest_db: Path) -> list[str | None]:
    return [row[0] for row in _imported_rows(dest_db)]


def _imported_rows(dest_db: Path) -> list[tuple[str | None, str | None]]:
    conn = sqlite3.connect(dest_db)
    try:
        return list(conn.execute("SELECT filepath, original_filepath FROM images ORDER BY id"))
    finally:
        conn.close()


def test_traversing_image_path_cannot_write_outside_images_folder(tmp_path, dest):
    profile_dir, images_dir, dest_db = dest
    settings = profile_dir / "app_settings.json"
    settings.write_text('{"keep": true}', encoding="utf-8")
    bundle = _write_bundle(
        tmp_path,
        "../app_settings.json",
        {"app_settings.json": b'{"hijacked": true}'},
    )

    result = _import(bundle)

    assert settings.read_text(encoding="utf-8") == '{"keep": true}'
    assert any("unsafe" in warning for warning in result["warnings"])
    _assert_placeholder(_imported_filepaths(dest_db)[-1], images_dir, "app_settings.json")


def test_import_never_replaces_an_existing_file_with_other_bytes(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    existing = images_dir / "imports" / "IMG_0001.jpg"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"bytes of an observation already here")
    bundle = _write_bundle(
        tmp_path,
        "imports/IMG_0001.jpg",
        {"images/imports/IMG_0001.jpg": b"bytes from the bundle"},
    )

    _import(bundle)

    assert existing.read_bytes() == b"bytes of an observation already here"
    imported = Path(_imported_filepaths(dest_db)[-1])
    assert imported != existing
    assert imported.parent == existing.parent
    assert imported.read_bytes() == b"bytes from the bundle"


def test_reimport_never_shares_a_file_with_an_existing_observation(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    members = {"images/imports/IMG_0002.jpg": b"same bytes"}
    _import(_write_bundle(tmp_path, "imports/IMG_0002.jpg", members))
    _import(_write_bundle(tmp_path, "imports/IMG_0002.jpg", members))

    first, second = _imported_filepaths(dest_db)[-2:]
    assert first == str(images_dir / "imports" / "IMG_0002.jpg")
    assert second == str(images_dir / "imports" / "IMG_0002_import1.jpg")
    assert Path(second).read_bytes() == b"same bytes"


def test_one_bundled_file_referenced_twice_is_copied_once(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    bundle = _write_bundle(
        tmp_path,
        "imports/IMG_0004.jpg",
        {"images/imports/IMG_0004.jpg": b"bytes"},
        original_filepath="imports/IMG_0004.jpg",
    )

    _import(bundle)

    filepath, original = _imported_rows(dest_db)[-1]
    assert filepath == original == str(images_dir / "imports" / "IMG_0004.jpg")
    assert sorted(p.name for p in (images_dir / "imports").iterdir()) == ["IMG_0004.jpg"]


def test_windows_relative_paths_import_on_every_platform(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    bundle = _write_bundle(
        tmp_path, "imports\\IMG_0005.jpg", {"images/imports/IMG_0005.jpg": b"from windows"}
    )

    _import(bundle)

    imported = Path(_imported_filepaths(dest_db)[-1])
    assert imported == images_dir / "imports" / "IMG_0005.jpg"
    assert imported.read_bytes() == b"from windows"


def test_a_dangling_symlink_at_the_destination_is_not_followed(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    outside = tmp_path / "outside" / "target.jpg"
    outside.parent.mkdir()
    link = images_dir / "imports" / "IMG_0006.jpg"
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)
    bundle = _write_bundle(tmp_path, "imports/IMG_0006.jpg", {"images/imports/IMG_0006.jpg": b"bytes"})

    _import(bundle)

    assert not outside.exists()
    imported = Path(_imported_filepaths(dest_db)[-1])
    assert imported == images_dir / "imports" / "IMG_0006_import1.jpg"
    assert imported.read_bytes() == b"bytes"


def test_a_case_alias_of_an_existing_file_gets_its_own_name(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    existing = images_dir / "imports" / "Photo.jpg"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"same bytes")
    if not (images_dir / "imports" / "photo.jpg").exists():
        pytest.skip("case-sensitive file system")
    bundle = _write_bundle(tmp_path, "imports/photo.jpg", {"images/imports/photo.jpg": b"same bytes"})

    _import(bundle)

    imported = Path(_imported_filepaths(dest_db)[-1])
    assert imported.name == "photo_import1.jpg"
    assert sorted(p.name for p in existing.parent.iterdir()) == ["Photo.jpg", "photo_import1.jpg"]


def test_absolute_path_uses_the_archived_copy_not_a_local_file(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    local_file = tmp_path / "elsewhere" / "outside.jpg"
    local_file.parent.mkdir()
    local_file.write_bytes(b"a file on the importing computer")
    bundle = _write_bundle(
        tmp_path,
        str(local_file),
        {"images/outside.jpg": b"the archived copy"},
    )

    _import(bundle)

    imported = Path(_imported_filepaths(dest_db)[-1])
    assert imported == images_dir / "outside.jpg"
    assert imported.read_bytes() == b"the archived copy"


def test_absolute_path_without_archived_copy_reads_no_local_file(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    local_file = tmp_path / "elsewhere" / "secret.txt"
    local_file.parent.mkdir()
    local_file.write_bytes(b"private bytes")
    bundle = _write_bundle(tmp_path, str(local_file), {})

    result = _import(bundle)

    assert not (images_dir / "secret.txt").exists()
    assert any("Missing bundled image file" in warning for warning in result["warnings"])
    _assert_placeholder(_imported_filepaths(dest_db)[-1], images_dir, "secret.txt")


def test_a_missing_file_never_resolves_to_an_existing_file_of_that_name(tmp_path, dest):
    _profile_dir, images_dir, dest_db = dest
    unrelated = images_dir / "Photo.jpg"
    unrelated.write_bytes(b"another observation's photo")
    bundle = _write_bundle(tmp_path, "/exporter/images/Photo.jpg", {})

    _import(bundle)

    _assert_placeholder(_imported_filepaths(dest_db)[-1], images_dir, "Photo.jpg")
    assert unrelated.read_bytes() == b"another observation's photo"
