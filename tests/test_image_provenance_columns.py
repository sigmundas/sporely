import sqlite3
from pathlib import Path

from database import migrate, models, schema


def _table_columns(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA table_info(images)").fetchall()
    return [str(row[1] or "") for row in rows]


def _init_fresh_database(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    schema.init_database()
    return db_path


def _create_legacy_image_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                mount_medium TEXT,
                stain TEXT,
                sort_order INTEGER,
                image_type TEXT,
                micro_category TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO images (
                observation_id, mount_medium, stain, sort_order, image_type, micro_category, created_at
            ) VALUES (
                1, 'Water', NULL, NULL, 'field', NULL, '2026-05-01 10:00:00'
            );
            """
        )
        conn.commit()


def test_init_database_creates_image_provenance_columns(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)

    columns = _table_columns(db_path)
    assert "source_role" in columns
    assert "file_purpose" in columns
    assert "original_mime_type" in columns
    assert "working_mime_type" in columns


def test_migrate_database_adds_image_provenance_columns_idempotently(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    _create_legacy_image_db(db_path)

    monkeypatch.setattr(migrate, "get_database_path", lambda: db_path)
    monkeypatch.setattr(migrate, "backup_database", lambda: False)

    migrate.migrate_database()
    first_columns = _table_columns(db_path)

    migrate.migrate_database()
    second_columns = _table_columns(db_path)

    for column_name in ("source_role", "file_purpose", "original_mime_type", "working_mime_type"):
        assert column_name in first_columns
        assert column_name in second_columns


def test_add_image_works_without_provenance_arguments(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    source_path = tmp_path / "source.jpg"
    source_path.write_text("source", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO observations (id, date) VALUES (?, ?)", (1, "2026-05-01"))
        conn.commit()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    image_id = models.ImageDB.add_image(
        observation_id=1,
        filepath=str(source_path),
        image_type="field",
        captured_at="2026-05-01 10:00:00",
        copy_to_folder=False,
    )
    image = models.ImageDB.get_image(image_id)

    assert image is not None
    assert image["filepath"] == str(source_path)
    assert image["source_role"] is None
    assert image["file_purpose"] is None
    assert image["original_mime_type"] is None
    assert image["working_mime_type"] is None


def test_add_image_round_trips_provenance_arguments(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    source_path = tmp_path / "source.heic"
    source_path.write_text("source", encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO observations (id, date) VALUES (?, ?)", (1, "2026-05-01"))
        conn.commit()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    image_id = models.ImageDB.add_image(
        observation_id=1,
        filepath=str(source_path),
        image_type="microscope",
        captured_at="2026-05-01 10:00:00",
        copy_to_folder=False,
        source_role="converted_local",
        file_purpose="microscope",
        original_mime_type="image/heic",
        working_mime_type="image/jpeg",
    )
    image = models.ImageDB.get_image(image_id)

    assert image is not None
    assert image["source_role"] == "converted_local"
    assert image["file_purpose"] == "microscope"
    assert image["original_mime_type"] == "image/heic"
    assert image["working_mime_type"] == "image/jpeg"
