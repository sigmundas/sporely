"""Security regressions for the legacy bundle importer and folder operations.

Findings F01-F04a of the 2026-11-26 desktop security audit: a crafted legacy
``.zip`` bundle must not be able to make later folder operations (objective
deletion, observation folder rename) touch files outside the managed images
folder, attach rows to unrelated local observations, or leave paths to files
on the importing computer in calibration metadata.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
import zipfile
from pathlib import Path

import pytest

from database import models, schema
from utils import db_share


@pytest.fixture
def profile(tmp_path, monkeypatch):
    """An isolated destination profile with real schema and real path helpers."""
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
    return profile_dir


def _source_db(tmp_path: Path, monkeypatch) -> Path:
    """A source library built with the real schema, outside the profile."""
    source_db = tmp_path / "bundle-src" / "mushrooms.db"
    source_db.parent.mkdir(parents=True, exist_ok=True)
    with monkeypatch.context() as ctx:
        ctx.setattr(schema, "get_database_path", lambda: source_db)
        schema.init_database()
    return source_db


def _write_bundle(tmp_path: Path, source_db: Path, members: dict[str, bytes] | None = None) -> Path:
    bundle = tmp_path / f"bundle-{uuid.uuid4().hex[:8]}.zip"
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.write(source_db, arcname="mushrooms.db")
        for name, data in (members or {}).items():
            zf.writestr(name, data)
    return bundle


def _import(bundle: Path, **overrides) -> dict:
    options = dict(
        include_observations=True,
        include_images=True,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )
    options.update(overrides)
    return db_share.import_database_bundle(str(bundle), **options)


def _victim_folder(tmp_path: Path, name: str = "victim") -> Path:
    victim = tmp_path / name
    victim.mkdir()
    (victim / "keep.txt").write_text("unrelated user file", encoding="utf-8")
    return victim


# --------------------------------------------------------------------------
# F01: an imported objective key must never name a folder outside the
# calibrations folder.
# --------------------------------------------------------------------------


def _insert_calibration(source_db: Path, objective_key: str) -> None:
    conn = sqlite3.connect(source_db)
    try:
        conn.execute(
            "INSERT INTO calibrations (calibration_uuid, objective_key, calibration_date, "
            "microns_per_pixel) VALUES (?, ?, '2026-05-01 10:00:00', 0.03)",
            (str(uuid.uuid4()), objective_key),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize("key_kind", ["absolute", "traversal"])
def test_deleting_imported_objective_never_removes_outside_folder(
    tmp_path, monkeypatch, profile, key_kind
):
    victim = _victim_folder(tmp_path)
    bad_key = str(victim) if key_kind == "absolute" else "../../../victim"
    source_db = _source_db(tmp_path, monkeypatch)
    _insert_calibration(source_db, bad_key)
    bundle = _write_bundle(
        tmp_path,
        source_db,
        {"objectives.json": json.dumps({bad_key: {"name": "Imported 40X", "magnification": 40.0}}).encode()},
    )

    _import(bundle, include_observations=False, include_images=False)

    imported = schema.load_objectives()
    # The owner deletes every imported objective through the model.
    for key in imported:
        models.CalibrationDB.clear_objective_usage(key)
        models.CalibrationDB.delete_calibrations_for_objective(key)

    assert victim.is_dir()
    assert (victim / "keep.txt").read_text(encoding="utf-8") == "unrelated user file"
    assert bad_key not in imported
    calibration_keys = {
        row[0] for row in sqlite3.connect(schema.get_database_path()).execute(
            "SELECT objective_key FROM calibrations"
        )
    }
    assert bad_key not in calibration_keys
    # The imported profile keeps its display name under the fresh key.
    assert any(value.get("name") == "Imported 40X" for value in imported.values())


@pytest.mark.parametrize("key_kind", ["absolute", "traversal", "dot", "calibrations_root"])
def test_delete_calibrations_for_objective_refuses_key_outside_calibrations(
    tmp_path, profile, key_kind
):
    calibrations_dir = schema.get_calibrations_dir()
    calibrations_dir.mkdir(parents=True, exist_ok=True)
    sibling = _victim_folder(tmp_path)
    (calibrations_dir / "other").mkdir()
    (calibrations_dir / "other" / "cal.jpg").write_bytes(b"other objective")
    bad_key = {
        "absolute": str(sibling),
        "traversal": "../../../victim",
        "dot": ".",
        "calibrations_root": "other/..",
    }[key_kind]

    models.CalibrationDB.delete_calibrations_for_objective(bad_key)

    assert sibling.is_dir() and (sibling / "keep.txt").exists()
    assert (calibrations_dir / "other" / "cal.jpg").exists()


def test_delete_calibrations_for_objective_still_removes_its_own_folder(profile):
    calibrations_dir = schema.get_calibrations_dir()
    own = calibrations_dir / "micro_40X"
    own.mkdir(parents=True)
    (own / "cal.jpg").write_bytes(b"calibration")

    assert models.CalibrationDB.delete_calibrations_for_objective("micro_40X") == []

    assert not own.exists()
    assert calibrations_dir.is_dir()


def test_portable_import_refuses_unsafe_objective_key(tmp_path):
    from utils.archive import portable_import

    staged = tmp_path / "staged.db"
    with sqlite3.connect(staged) as conn:
        conn.execute("CREATE TABLE images (objective_name TEXT)")
        conn.execute("CREATE TABLE calibrations (objective_key TEXT)")
        conn.execute("INSERT INTO calibrations VALUES ('../victim')")
    incoming = tmp_path / "objectives.json"
    incoming.write_text(json.dumps({"../victim": {"name": "Imported"}}), encoding="utf-8")

    with pytest.raises(portable_import.PortableImportError):
        portable_import._merged_objectives_payload(staged, incoming, tmp_path / "dest.json")


@pytest.mark.parametrize("key_kind", ["absolute", "traversal"])
def test_saving_calibration_image_never_writes_outside_calibrations(tmp_path, profile, key_kind):
    from types import SimpleNamespace

    from ui.calibration_dialog import CalibrationDialog

    outside = tmp_path / "elsewhere"
    bad_key = str(outside) if key_kind == "absolute" else "../../../elsewhere"
    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"jpeg")

    saved = CalibrationDialog._save_calibration_image(
        SimpleNamespace(current_objective_key=bad_key), str(photo)
    )

    assert not outside.exists()
    assert saved is not None
    calibrations_root = schema.get_calibrations_dir().resolve()
    assert calibrations_root in Path(saved).resolve().parents


@pytest.mark.parametrize(
    "key, safe",
    [
        ("micro_40X", True),
        ("100X", True),
        ("x.y", True),
        ("micro_Plan_achro.", True),
        (".. ", False),
        ("...", False),
        ("CON", False),
        ("nul.txt", False),
        ("C:x", False),
        ("../x", False),
        ("", False),
    ],
)
def test_objective_key_validator(key, safe):
    from utils.objective_keys import is_safe_objective_key

    assert is_safe_objective_key(key) is safe


def test_import_remaps_key_that_aliases_an_existing_key(tmp_path, monkeypatch, profile):
    """On a case-insensitive file system two keys differing in case share a folder."""
    own = schema.get_calibrations_dir() / "micro_40X"
    own.mkdir(parents=True)
    (own / "cal.jpg").write_bytes(b"own calibration")
    schema.save_objectives({"micro_40X": {"name": "Own 40X", "magnification": 40.0}})
    source_db = _source_db(tmp_path, monkeypatch)
    _insert_calibration(source_db, "MICRO_40x")
    bundle = _write_bundle(
        tmp_path,
        source_db,
        {"objectives.json": json.dumps({"MICRO_40x": {"name": "Imported 40X"}}).encode()},
    )

    _import(bundle, include_observations=False, include_images=False)
    _import(bundle, include_observations=False, include_images=False)

    imported = schema.load_objectives()
    assert "MICRO_40x" not in imported
    assert sum(1 for value in imported.values() if value.get("name") == "Imported 40X") == 1
    for key, value in imported.items():
        if value.get("name") == "Imported 40X":
            models.CalibrationDB.delete_calibrations_for_objective(key)
    assert (own / "cal.jpg").read_bytes() == b"own calibration"


def _staged_objective_db(tmp_path: Path, calibration_keys, image_names=()) -> Path:
    staged = tmp_path / "staged.db"
    with sqlite3.connect(staged) as conn:
        conn.execute("CREATE TABLE images (objective_name TEXT)")
        conn.execute("CREATE TABLE calibrations (objective_key TEXT)")
        conn.executemany("INSERT INTO calibrations VALUES (?)", [(k,) for k in calibration_keys])
        conn.executemany("INSERT INTO images VALUES (?)", [(n,) for n in image_names])
    return staged


def test_portable_import_refuses_unsafe_calibration_key_without_profile(tmp_path):
    from utils.archive import portable_import

    # Display names in images.objective_name may hold "/" and stay allowed.
    staged = _staged_objective_db(tmp_path, ["../victim"], ["100X/1.25 Plan achro"])
    incoming = tmp_path / "objectives.json"
    incoming.write_text("{}", encoding="utf-8")

    with pytest.raises(portable_import.PortableImportError):
        portable_import._merged_objectives_payload(staged, incoming, tmp_path / "dest.json")


def test_portable_import_allows_display_names_with_slashes(tmp_path):
    from utils.archive import portable_import

    staged = _staged_objective_db(tmp_path, ["micro_40X"], ["100X/1.25 Plan achro"])
    incoming = tmp_path / "objectives.json"
    incoming.write_text(json.dumps({"micro_40X": {"name": "Own"}}), encoding="utf-8")

    payload = portable_import._merged_objectives_payload(staged, incoming, tmp_path / "dest.json")

    assert payload is not None and b"micro_40X" in payload


def test_portable_import_refuses_key_aliasing_an_existing_key(tmp_path):
    from utils.archive import portable_import

    staged = _staged_objective_db(tmp_path, ["MICRO_40x"])
    incoming = tmp_path / "objectives.json"
    incoming.write_text(json.dumps({"MICRO_40x": {"name": "Imported"}}), encoding="utf-8")
    destination = tmp_path / "dest.json"
    destination.write_text(json.dumps({"micro_40X": {"name": "Own"}}), encoding="utf-8")

    with pytest.raises(portable_import.PortableImportError):
        portable_import._merged_objectives_payload(staged, incoming, destination)
