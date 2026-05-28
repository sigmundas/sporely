import sqlite3
from pathlib import Path

from database import schema
from utils import db_share


def _create_legacy_source_bundle_db(db_path: Path, image_path: Path, original_path: Path) -> None:
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
        conn.execute(
            "INSERT INTO observations (id, date, folder_path) VALUES (?, ?, ?)",
            (1, "2026-05-01", str(image_path.parent)),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, filepath, original_filepath, image_type, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                str(image_path),
                str(original_path),
                "microscope",
                0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_import_database_bundle_backfills_image_provenance_for_legacy_images(
    tmp_path,
    monkeypatch,
):
    source_db = tmp_path / "source.sqlite"
    dest_db = tmp_path / "dest.sqlite"
    bundle_path = tmp_path / "bundle.zip"
    source_images_dir = tmp_path / "source-images"
    dest_images_dir = tmp_path / "dest-images"
    source_images_dir.mkdir()
    dest_images_dir.mkdir()

    image_dir = source_images_dir / "observation-1"
    image_dir.mkdir(parents=True, exist_ok=True)
    original_path = image_dir / "original.heic"
    working_path = image_dir / "working.jpg"
    original_path.write_bytes(b"legacy heic bytes")
    working_path.write_bytes(b"legacy jpeg bytes")

    _create_legacy_source_bundle_db(source_db, working_path, original_path)

    monkeypatch.setattr(db_share, "get_database_path", lambda: source_db)
    monkeypatch.setattr(db_share, "get_images_dir", lambda: source_images_dir)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: tmp_path / "objectives.json")
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: tmp_path / "reference.sqlite")
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)

    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=True,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=False,
    )

    monkeypatch.setattr(schema, "get_database_path", lambda: dest_db)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    schema.init_database()

    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(dest_db))
    monkeypatch.setattr(db_share, "get_images_dir", lambda: dest_images_dir)

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=True,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=False,
    )

    conn = sqlite3.connect(dest_db)
    try:
        row = conn.execute(
            """
            SELECT filepath, original_filepath, image_type, source_role, file_purpose,
                   original_mime_type, working_mime_type
            FROM images
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["images"] == 1
    assert row is not None
    assert row[2] == "microscope"
    assert row[3] == "converted_local"
    assert row[4] == "microscope"
    assert row[5] == "image/heic"
    assert row[6] == "image/jpeg"
    assert Path(row[0]).exists()
    assert Path(row[1]).exists()
