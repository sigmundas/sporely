"""F04a (2026-11-26 desktop security audit): every path-bearing key in an
imported calibration's metadata is rebased into the images folder, so no
imported row names a file elsewhere on the importing computer (which a later
backup, export or sync could pick up)."""

from __future__ import annotations

import json
import sqlite3
import uuid
import zipfile
from pathlib import Path

import pytest

from database import models, schema
from utils import db_share

_PATH_KEYS = (
    "source_path",
    "source_filepath",
    "original_path",
    "selected_path",
    "path",
    "working_path",
)


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


def _entry(sentinel: Path) -> dict:
    entry = {key: str(sentinel) for key in _PATH_KEYS}
    entry["companion_paths"] = [str(sentinel), "../../outside/secret.jpg"]
    entry["measurements"] = [{"known_um": 10.0, "measured_px": 100.0}]
    return entry


def _strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


def test_imported_calibration_metadata_names_no_outside_file(tmp_path, monkeypatch, profile):
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "secret.jpg"
    sentinel.write_bytes(b"a file on the importing computer")
    source_db = tmp_path / "bundle-src" / "mushrooms.db"
    source_db.parent.mkdir()
    with monkeypatch.context() as ctx:
        ctx.setattr(schema, "get_database_path", lambda: source_db)
        schema.init_database()
    payload = {
        "images": [_entry(sentinel)],
        "auto_images": [_entry(sentinel)],
    }
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "INSERT INTO calibrations (calibration_uuid, objective_key, calibration_date, "
            "microns_per_pixel, measurements_json) VALUES (?, 'micro_40X', "
            "'2026-05-01 10:00:00', 0.03, ?)",
            (str(uuid.uuid4()), json.dumps(payload)),
        )
    bundle = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(source_db, arcname="mushrooms.db")

    db_share.import_database_bundle(
        str(bundle),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    with sqlite3.connect(schema.get_database_path()) as conn:
        (stored,) = conn.execute("SELECT measurements_json FROM calibrations").fetchone()
    loaded = json.loads(stored)
    values = _strings(loaded)
    images_root = profile.resolve()
    for collection in ("images", "auto_images"):
        entry = loaded[collection][0]
        for key in _PATH_KEYS:
            assert key not in entry or images_root in Path(entry[key]).resolve().parents, (
                collection, key, entry.get(key)
            )
        for companion in entry.get("companion_paths") or []:
            assert images_root in Path(companion).resolve().parents, (collection, companion)
    assert str(sentinel) not in values
    assert not any("outside" in value for value in values)
    assert sentinel.read_bytes() == b"a file on the importing computer"


def test_imported_calibration_metadata_keeps_bundled_files(tmp_path, monkeypatch, profile):
    source_db = tmp_path / "bundle-src" / "mushrooms.db"
    source_db.parent.mkdir()
    with monkeypatch.context() as ctx:
        ctx.setattr(schema, "get_database_path", lambda: source_db)
        schema.init_database()
    payload = {
        "images": [{"path": "calibrations/micro_40X/working.jpg",
                    # An exported absolute path under the exporter's images
                    # folder: the file is bundled at its relative path.
                    "source_path": str(
                        tmp_path / "exporter" / "images" / "calibrations" / "micro_40X" / "source.jpg"
                    ),
                    "companion_paths": ["calibrations/micro_40X/source.xmp"]}],
        "auto_images": [{"working_path": "calibrations/micro_40X/auto.jpg"}],
    }
    with sqlite3.connect(source_db) as conn:
        conn.execute(
            "INSERT INTO calibrations (calibration_uuid, objective_key, calibration_date, "
            "microns_per_pixel, measurements_json) VALUES (?, 'micro_40X', "
            "'2026-05-01 10:00:00', 0.03, ?)",
            (str(uuid.uuid4()), json.dumps(payload)),
        )
    bundle = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(source_db, arcname="mushrooms.db")
        for name in ("working.jpg", "source.jpg", "source.xmp", "auto.jpg"):
            zf.writestr(f"images/calibrations/micro_40X/{name}", name.encode())

    db_share.import_database_bundle(
        str(bundle),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    with sqlite3.connect(schema.get_database_path()) as conn:
        (stored,) = conn.execute("SELECT measurements_json FROM calibrations").fetchone()
    loaded = json.loads(stored)
    image_entry = loaded["images"][0]
    auto_entry = loaded["auto_images"][0]
    for value, name in (
        (image_entry["path"], "working.jpg"),
        (image_entry["source_path"], "source.jpg"),
        (image_entry["companion_paths"][0], "source.xmp"),
        (auto_entry["working_path"], "auto.jpg"),
    ):
        restored = Path(value)
        assert restored.is_file() and restored.read_bytes() == name.encode()
        assert profile.resolve() in restored.resolve().parents
