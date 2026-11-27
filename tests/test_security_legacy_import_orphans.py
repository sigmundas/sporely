"""F03 (2026-11-26 desktop security audit): a legacy bundle image whose
parent observation is not in the bundle must not attach to whichever local
observation has the same id, and neither may its measurements."""

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
    images_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "objectives.json").write_text("{}", encoding="utf-8")
    return images_dir


@pytest.mark.parametrize("include_measurements", [False, True])
def test_orphaned_bundle_image_does_not_attach_to_local_observation(
    tmp_path, monkeypatch, profile, include_measurements
):
    local_id = models.ObservationDB.create_observation(
        date="2026-04-01", genus="Localgenus", species="testa"
    )
    bundled_id = local_id + 50
    source_db = tmp_path / "bundle-src" / "mushrooms.db"
    source_db.parent.mkdir()
    with monkeypatch.context() as ctx:
        ctx.setattr(schema, "get_database_path", lambda: source_db)
        schema.init_database()
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "INSERT INTO observations (id, date, genus, species) "
            "VALUES (?, '2026-05-01', 'Bundlegenus', 'testa')",
            (bundled_id,),
        )
        # Image 21 belongs to the bundled observation.
        conn.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type) "
            "VALUES (21, ?, 'own/IMG_OWN.jpg', 'microscope')",
            (bundled_id,),
        )
        # Image 31's parent is absent from the bundle but its id exists here.
        conn.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type) "
            "VALUES (31, ?, 'orphan/IMG_ORPHAN.jpg', 'microscope')",
            (local_id,),
        )
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um) "
            "VALUES (41, 21, 10.0, 5.0), (42, 31, 11.0, 6.0)"
        )
        conn.execute(
            "INSERT INTO spore_annotations (image_id, measurement_id, spore_number) "
            "VALUES (21, 41, 1), (31, 42, 1)"
        )
    bundle = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(source_db, arcname="mushrooms.db")
        zf.writestr("images/own/IMG_OWN.jpg", b"jpeg of the bundled observation")
        zf.writestr("images/orphan/IMG_ORPHAN.jpg", b"jpeg from the bundle")

    result = db_share.import_database_bundle(
        str(bundle),
        include_observations=True,
        include_images=True,
        include_measurements=include_measurements,
        include_calibrations=False,
        include_reference_values=False,
    )

    with sqlite3.connect(schema.get_database_path()) as conn:
        local_images = conn.execute(
            "SELECT filepath FROM images WHERE observation_id = ?", (local_id,)
        ).fetchall()
        image_paths = [row[0] for row in conn.execute("SELECT filepath FROM images")]
        lengths = [row[0] for row in conn.execute("SELECT length_um FROM spore_measurements")]
        annotations = conn.execute("SELECT COUNT(*) FROM spore_annotations").fetchone()[0]
    assert local_images == []
    assert len(image_paths) == 1 and image_paths[0].endswith("IMG_OWN.jpg")
    assert not list(profile.rglob("IMG_ORPHAN*"))
    assert any("orphan/IMG_ORPHAN.jpg" in warning for warning in result["warnings"])
    if include_measurements:
        assert lengths == [10.0]
        assert annotations == 1
