"""Regression tests for the Sample / Sample source split.

Fresh/Dried describe specimen condition; Spore print / Hymenium / ... describe
where the observed material came from. These used to be conflated under a single
`sample_type` column and dropdown. This test suite pins the split so:

  * `sample` never contains `Spore_print` again;
  * `sample_source` carries the material provenance;
  * public labels never surface the internal `Not set` sentinel;
  * legacy `sample_type='Spore_print'` rows migrate safely into `sample_source`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from database import migrate, models, schema
from database.database_tags import DatabaseTerms


# ---------------------------------------------------------------------------
# Canonical enums
# ---------------------------------------------------------------------------


def test_sample_default_values_no_longer_include_spore_print():
    values = DatabaseTerms.default_values("sample")
    assert "Fresh" in values
    assert "Dried" in values
    assert "Not_set" in values
    assert "Spore_print" not in values


def test_sample_source_default_values_include_spore_print_and_peers():
    values = DatabaseTerms.default_values("sample_source")
    assert values[0] == "Not_set"
    for expected in ("Spore_print", "Hymenium", "Stipe", "Pileus", "Context", "Other"):
        assert expected in values, f"missing {expected} from SAMPLE_SOURCES"


def test_canonicalize_sample_drops_legacy_spore_print():
    # Spore_print is a source, not a condition — refuse to canonicalize it as one.
    assert DatabaseTerms.canonicalize_sample("Spore_print") is None
    assert DatabaseTerms.canonicalize_sample("spore print") is None


def test_canonicalize_sample_source_accepts_spore_print_variants():
    assert DatabaseTerms.canonicalize_sample_source("Spore_print") == "Spore_print"
    assert DatabaseTerms.canonicalize_sample_source("spore print") == "Spore_print"
    assert DatabaseTerms.canonicalize_sample_source("SPOREPRINT") == "Spore_print"


def test_canonicalize_sample_still_maps_fresh_and_dried():
    assert DatabaseTerms.canonicalize_sample("Fresh") == "Fresh"
    assert DatabaseTerms.canonicalize_sample("dried") == "Dried"


# ---------------------------------------------------------------------------
# Public / compact label rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [None, "", "Not_set", "not set", "Spore_print"])
def test_public_sample_label_renders_unknown_when_unset_or_legacy(value):
    # 'Not set' should never appear in a public label. Legacy Spore_print values
    # were misclassified as conditions — the public view collapses them to Unknown.
    label = DatabaseTerms.public_sample_label(value)
    assert label == "Unknown"
    assert "Not set" not in label


def test_public_sample_label_preserves_known_condition():
    assert DatabaseTerms.public_sample_label("Fresh") == "Fresh"
    assert DatabaseTerms.public_sample_label("Dried") == "Dried"


@pytest.mark.parametrize("value", [None, "", "Not_set"])
def test_compact_sample_label_renders_dash_when_unset(value):
    assert DatabaseTerms.compact_sample_label(value) == "–"


def test_compact_sample_label_preserves_known_condition():
    assert DatabaseTerms.compact_sample_label("Fresh") == "Fresh"


def test_public_sample_source_label_renders_unknown_when_unset():
    assert DatabaseTerms.public_sample_source_label(None) == "Unknown"
    assert DatabaseTerms.public_sample_source_label("Not_set") == "Unknown"


def test_public_sample_source_label_preserves_known_source():
    assert DatabaseTerms.public_sample_source_label("Spore_print") == "Spore print"


# ---------------------------------------------------------------------------
# Schema — column existence + add_image / update_image round trip
# ---------------------------------------------------------------------------


def _init_fresh_database(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    schema.init_database()
    return db_path


def _image_columns(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        return [str(row[1] or "") for row in conn.execute("PRAGMA table_info(images)").fetchall()]


def test_init_database_creates_sample_source_column(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    assert "sample_source" in _image_columns(db_path)
    # sample_type must still exist — it now carries specimen condition only.
    assert "sample_type" in _image_columns(db_path)


def test_add_image_persists_sample_and_sample_source(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    working = tmp_path / "img.jpg"
    working.write_text("x")

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO observations (id, date) VALUES (?, ?)", (1, "2026-07-14"))
        conn.commit()
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    image_id = models.ImageDB.add_image(
        observation_id=1,
        filepath=str(working),
        image_type="microscope",
        sample_type="Fresh",
        sample_source="Spore_print",
        copy_to_folder=False,
    )
    image = models.ImageDB.get_image(image_id)
    assert image["sample_type"] == "Fresh"
    assert image["sample_source"] == "Spore_print"


def test_update_image_can_set_and_clear_sample_source(tmp_path, monkeypatch):
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    working = tmp_path / "img.jpg"
    working.write_text("x")

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO observations (id, date) VALUES (?, ?)", (1, "2026-07-14"))
        conn.commit()
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    image_id = models.ImageDB.add_image(
        observation_id=1,
        filepath=str(working),
        image_type="microscope",
        copy_to_folder=False,
    )

    models.ImageDB.update_image(image_id, sample_source="Hymenium")
    assert models.ImageDB.get_image(image_id)["sample_source"] == "Hymenium"

    # Passing None explicitly clears the value; omitting the kwarg preserves it.
    models.ImageDB.update_image(image_id, sample_source=None)
    assert models.ImageDB.get_image(image_id)["sample_source"] is None


# ---------------------------------------------------------------------------
# Migration — legacy sample_type='Spore_print' rows lift into sample_source
# ---------------------------------------------------------------------------


def _create_legacy_images_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                genus TEXT,
                species TEXT,
                source_type TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                filepath TEXT,
                image_type TEXT,
                mount_medium TEXT,
                stain TEXT,
                sample_type TEXT,
                sort_order INTEGER,
                micro_category TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute("INSERT INTO observations (id, date) VALUES (1, '2026-07-14')")
        conn.executemany(
            "INSERT INTO images (observation_id, filepath, image_type, sample_type) VALUES (?, ?, ?, ?)",
            [
                (1, "/tmp/a.jpg", "microscope", "Spore_print"),
                (1, "/tmp/b.jpg", "microscope", "Fresh"),
                (1, "/tmp/c.jpg", "microscope", "Dried"),
                (1, "/tmp/d.jpg", "microscope", None),
                (1, "/tmp/e.jpg", "microscope", "spore print"),
            ],
        )
        conn.commit()


def test_migrate_database_hoists_legacy_spore_print_into_sample_source(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    _create_legacy_images_db(db_path)
    monkeypatch.setattr(migrate, "get_database_path", lambda: db_path)
    monkeypatch.setattr(migrate, "backup_database", lambda: False)

    migrate.migrate_database()

    with sqlite3.connect(db_path) as conn:
        rows = list(
            conn.execute(
                "SELECT filepath, sample_type, sample_source FROM images ORDER BY id"
            )
        )
    by_path = {path: (sample, source) for path, sample, source in rows}

    # Spore_print rows: sample_type cleared, sample_source populated.
    assert by_path["/tmp/a.jpg"] == (None, "Spore_print")
    assert by_path["/tmp/e.jpg"] == (None, "Spore_print")
    # Fresh/Dried untouched — those are legitimate conditions.
    assert by_path["/tmp/b.jpg"] == ("Fresh", None)
    assert by_path["/tmp/c.jpg"] == ("Dried", None)
    # Null row stays null on both columns.
    assert by_path["/tmp/d.jpg"] == (None, None)


def test_migrate_database_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    _create_legacy_images_db(db_path)
    monkeypatch.setattr(migrate, "get_database_path", lambda: db_path)
    monkeypatch.setattr(migrate, "backup_database", lambda: False)

    migrate.migrate_database()
    migrate.migrate_database()  # second pass must not error or re-touch rows

    with sqlite3.connect(db_path) as conn:
        rows = list(
            conn.execute("SELECT sample_type, sample_source FROM images WHERE filepath = ?", ("/tmp/a.jpg",))
        )
    assert rows == [(None, "Spore_print")]


def test_migrate_database_does_not_overwrite_explicit_sample_source(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                genus TEXT,
                species TEXT,
                source_type TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                filepath TEXT,
                image_type TEXT,
                mount_medium TEXT,
                stain TEXT,
                sample_type TEXT,
                sample_source TEXT,
                sort_order INTEGER,
                micro_category TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute("INSERT INTO observations (id, date) VALUES (1, '2026-07-14')")
        conn.execute(
            "INSERT INTO images (observation_id, filepath, image_type, sample_type, sample_source) VALUES (?, ?, ?, ?, ?)",
            (1, "/tmp/x.jpg", "microscope", "Spore_print", "Hymenium"),
        )
        conn.commit()

    monkeypatch.setattr(migrate, "get_database_path", lambda: db_path)
    monkeypatch.setattr(migrate, "backup_database", lambda: False)
    migrate.migrate_database()

    with sqlite3.connect(db_path) as conn:
        rows = list(conn.execute("SELECT sample_type, sample_source FROM images"))
    # Explicit sample_source wins; the migration must not clobber it.
    assert rows == [("Spore_print", "Hymenium")]
