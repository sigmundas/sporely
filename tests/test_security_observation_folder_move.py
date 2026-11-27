"""F02 (2026-11-26 desktop security audit): renaming an observation's folder
after a taxon edit must never move a folder outside the managed images folder,
such as one a legacy bundle row named in ``folder_path``."""

from __future__ import annotations

import sqlite3
import uuid
import zipfile
from pathlib import Path

import pytest

from database import models, schema
from utils import db_share


@pytest.fixture
def profile(tmp_path, monkeypatch):
    profile_dir = tmp_path / "profile"
    profile_dir.mkdir()
    dest_db = profile_dir / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: dest_db)
    monkeypatch.setattr(schema, "get_objectives_path", lambda: profile_dir / "objectives.json")
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    monkeypatch.setattr(models.CalibrationAssetDB, "backfill_all", lambda: 0)
    schema.init_database()
    images_dir = schema.get_images_dir()
    assert images_dir == profile_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "objectives.json").write_text("{}", encoding="utf-8")
    return images_dir


def _victim_folder(tmp_path: Path) -> Path:
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "keep.txt").write_text("unrelated user file", encoding="utf-8")
    return victim


def _folder_path(observation_id: int) -> str | None:
    with sqlite3.connect(schema.get_database_path()) as conn:
        return conn.execute(
            "SELECT folder_path FROM observations WHERE id = ?", (observation_id,)
        ).fetchone()[0]


def test_legacy_import_clears_folder_path_and_rename_leaves_outside_folder(
    tmp_path, monkeypatch, profile
):
    victim = _victim_folder(tmp_path)
    source_db = tmp_path / "bundle-src" / "mushrooms.db"
    source_db.parent.mkdir()
    with monkeypatch.context() as ctx:
        ctx.setattr(schema, "get_database_path", lambda: source_db)
        schema.init_database()
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "INSERT INTO observations (id, date, genus, species, folder_path) "
            "VALUES (7, '2026-05-01', 'Genusa', 'testa', ?)",
            (str(victim),),
        )
    bundle = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(source_db, arcname="mushrooms.db")

    db_share.import_database_bundle(
        str(bundle),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=False,
    )
    with sqlite3.connect(schema.get_database_path()) as conn:
        (observation_id,) = conn.execute(
            "SELECT id FROM observations WHERE genus = 'Genusa'"
        ).fetchone()

    models.ObservationDB.update_observation(observation_id, genus="Genusb")

    assert (victim / "keep.txt").read_text(encoding="utf-8") == "unrelated user file"
    assert _folder_path(observation_id) != str(victim)


def _local_observation() -> int:
    return models.ObservationDB.create_observation(
        date="2026-05-01", genus="Genusa", species="testa"
    )


def test_rename_skips_stored_folder_outside_images_folder(tmp_path, profile):
    victim = _victim_folder(tmp_path)
    observation_id = _local_observation()
    with sqlite3.connect(schema.get_database_path()) as conn:
        conn.execute(
            "UPDATE observations SET folder_path = ? WHERE id = ?", (str(victim), observation_id)
        )

    models.ObservationDB.update_observation(observation_id, genus="Genusb")

    assert (victim / "keep.txt").exists()
    assert _folder_path(observation_id) == str(victim)


def test_rename_skips_inferred_folder_outside_images_folder(tmp_path, profile):
    victim = _victim_folder(tmp_path)
    (victim / "IMG_1.jpg").write_bytes(b"jpeg")
    observation_id = _local_observation()
    with sqlite3.connect(schema.get_database_path()) as conn:
        conn.execute("UPDATE observations SET folder_path = NULL WHERE id = ?", (observation_id,))
        conn.execute(
            "INSERT INTO images (observation_id, filepath, image_type) VALUES (?, ?, 'field')",
            (observation_id, str(victim / "IMG_1.jpg")),
        )

    models.ObservationDB.update_observation(observation_id, genus="Genusb")

    assert (victim / "keep.txt").exists() and (victim / "IMG_1.jpg").exists()


def test_rename_skips_images_root_itself(profile):
    (profile / "IMG_1.jpg").write_bytes(b"jpeg")
    (profile / "other.txt").write_text("other observation data", encoding="utf-8")
    observation_id = _local_observation()
    with sqlite3.connect(schema.get_database_path()) as conn:
        conn.execute("UPDATE observations SET folder_path = NULL WHERE id = ?", (observation_id,))
        conn.execute(
            "INSERT INTO images (observation_id, filepath, image_type) VALUES (?, ?, 'field')",
            (observation_id, str(profile / "IMG_1.jpg")),
        )

    models.ObservationDB.update_observation(observation_id, genus="Genusb")

    assert (profile / "IMG_1.jpg").exists() and (profile / "other.txt").exists()


def test_rename_still_moves_managed_folder(profile):
    observation_id = _local_observation()
    old_folder = Path(_folder_path(observation_id))
    assert profile in old_folder.parents
    old_folder.mkdir(parents=True, exist_ok=True)
    (old_folder / "IMG_1.jpg").write_bytes(b"jpeg")

    new_folder = models.ObservationDB.update_observation(observation_id, genus="Genusb")

    assert new_folder and Path(new_folder).parent.name == "Genusb"
    assert (Path(new_folder) / "IMG_1.jpg").exists()
    assert not old_folder.exists()
    assert _folder_path(observation_id) == new_folder


def test_rename_does_not_move_a_folder_shared_with_another_observation(profile):
    shared = profile / "Shared" / "folder"
    shared.mkdir(parents=True)
    (shared / "IMG_A.jpg").write_bytes(b"a")
    (shared / "IMG_B.jpg").write_bytes(b"b")
    first = _local_observation()
    second = _local_observation()
    with sqlite3.connect(schema.get_database_path()) as conn:
        conn.execute("UPDATE observations SET folder_path = NULL WHERE id IN (?, ?)", (first, second))
        conn.execute(
            "INSERT INTO images (observation_id, filepath, image_type) VALUES (?, ?, 'field'), (?, ?, 'field')",
            (first, str(shared / "IMG_A.jpg"), second, str(shared / "IMG_B.jpg")),
        )

    models.ObservationDB.update_observation(first, genus="Genusb")

    with sqlite3.connect(schema.get_database_path()) as conn:
        (second_path,) = conn.execute(
            "SELECT filepath FROM images WHERE observation_id = ?", (second,)
        ).fetchone()
    assert Path(second_path).is_file()


def test_rename_never_builds_a_folder_outside_images_from_a_crafted_date(profile):
    observation_id = _local_observation()
    with sqlite3.connect(schema.get_database_path()) as conn:
        conn.execute(
            "UPDATE observations SET folder_path = NULL, date = ? WHERE id = ?",
            ("2026-05-01/../../../../escape", observation_id),
        )

    models.ObservationDB.update_observation(observation_id, genus="Genusb")

    folder = _folder_path(observation_id)
    assert folder is None or profile.resolve() in Path(folder).resolve().parents
