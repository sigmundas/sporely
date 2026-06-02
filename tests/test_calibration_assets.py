import json
import sqlite3
import uuid
from pathlib import Path

import app_identity

from database import models, schema
from utils import db_share


def _init_calibration_database(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "app" / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    with monkeypatch.context() as ctx:
        ctx.setattr(models.CalibrationAssetDB, "backfill_all", lambda: 0)
        schema.init_database()
    return db_path


def _fetch_assets(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM calibration_assets ORDER BY id"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _build_calibration_payload(source_path: Path, working_path: Path) -> str:
    payload = {
        "images": [
            {
                "index": 0,
                "source_path": str(source_path),
                "path": str(working_path),
                "working_path": str(working_path),
                "measurements": [{"known_um": 10.0, "measured_px": 100.0}],
                "crop_box": [0.1, 0.2, 0.8, 0.9],
                "crop_source_size": [100, 100],
                "division_distance_mm": 0.01,
            }
        ],
        "auto_images": [
            {
                "index": 0,
                "path": str(working_path),
                "working_path": str(working_path),
                "spacing_um": 10.0,
                "division_distance_mm": 0.01,
                "result": {
                    "axis": "x",
                    "angle_deg": 0.0,
                    "spacing_median_px": 100.0,
                    "spacing_median_edges_px": 100.0,
                    "nm_per_px": 100.0,
                    "nm_per_px_edges": 100.0,
                    "agreement_pct": 100.0,
                    "rel_scatter_mad_pct": 0.1,
                    "rel_scatter_iqr_pct": 0.1,
                    "drift_slope": 0.0,
                    "residual_slope_deg": 0.0,
                    "edges_px": [[0, 0], [1, 1]],
                },
            }
        ],
        "auto_summary": {
            "method": "edges",
            "average_nm_per_px": 100.0,
            "max_deviation_nm_per_px": 0.0,
            "n_images": 1,
        },
    }
    return json.dumps(payload)


def _seed_calibration_row(
    source_path: Path,
    working_path: Path,
    *,
    calibration_uuid: str | None = None,
) -> int:
    calibration_id = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-01 10:00:00",
        num_measurements=1,
        measurements_json=_build_calibration_payload(source_path, working_path),
        image_filepath=str(working_path),
        notes="calibration asset test",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )
    return calibration_id


def test_calibration_assets_are_materialized_and_backfilled(tmp_path, monkeypatch):
    db_path = _init_calibration_database(tmp_path, monkeypatch)
    images_dir = db_path.parent / "images" / "calibrations" / "100X"
    images_dir.mkdir(parents=True, exist_ok=True)

    source_path = images_dir / "source.heic"
    working_path = images_dir / "working.jpg"
    cache_path = images_dir / "reference.webp"
    source_path.write_bytes(b"heic-bytes")
    working_path.write_bytes(b"jpeg-bytes")
    cache_path.write_bytes(b"webp-bytes")

    calibration_uuid = str(uuid.uuid4())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    calibration_id = _seed_calibration_row(
        source_path,
        working_path,
        calibration_uuid=calibration_uuid,
    )

    assert calibration_id > 0

    cache_asset_id = models.CalibrationAssetDB.record_reference_cache_asset(
        calibration_id=calibration_id,
        calibration_uuid=calibration_uuid,
        cache_path=cache_path,
        image_storage_path=f"user-123/{calibration_uuid}/reference.webp",
        original_path=str(source_path),
        metadata={"downloaded_via": "test"},
    )
    assert cache_asset_id > 0

    assets = models.CalibrationAssetDB.get_assets_for_calibration(calibration_id)
    by_role = {asset["role"]: asset for asset in assets}

    assert {"source_photo", "working_photo", "calibration_crop", "overlay", "debug_artifact", "reference_cache"} <= set(by_role)
    assert by_role["source_photo"]["source_role"] == "import_source"
    assert by_role["source_photo"]["file_purpose"] == "calibration"
    assert by_role["source_photo"]["mime_type"] == "image/heic"
    assert by_role["working_photo"]["source_role"] == "converted_local"
    assert by_role["working_photo"]["file_purpose"] == "calibration"
    assert by_role["working_photo"]["original_path"] == str(source_path)
    assert by_role["working_photo"]["mime_type"] == "image/jpeg"
    assert by_role["working_photo"]["metadata_json"]["working_mime_type"] == "image/jpeg"
    assert by_role["calibration_crop"]["source_role"] == "generated_artifact"
    assert by_role["calibration_crop"]["file_purpose"] == "calibration"
    assert by_role["calibration_crop"]["mime_type"] is None
    assert by_role["overlay"]["source_role"] == "generated_artifact"
    assert by_role["debug_artifact"]["source_role"] == "generated_artifact"
    assert by_role["reference_cache"]["source_role"] == "cloud_recovery_cache"
    assert by_role["reference_cache"]["file_purpose"] == "cache"
    assert Path(by_role["reference_cache"]["local_path"]).exists()

    before_uuids = {asset["asset_uuid"] for asset in assets}
    monkeypatch.setattr(app_identity, "app_data_dir", lambda: tmp_path / "appdata")
    models.CalibrationAssetDB.backfill_all()
    models.CalibrationAssetDB.backfill_all()
    after_uuids = {asset["asset_uuid"] for asset in _fetch_assets(db_path)}

    assert after_uuids == before_uuids


def test_calibration_asset_backfill_handles_missing_files(tmp_path, monkeypatch):
    db_path = _init_calibration_database(tmp_path, monkeypatch)
    images_dir = db_path.parent / "images" / "calibrations" / "100X"
    images_dir.mkdir(parents=True, exist_ok=True)

    source_path = images_dir / "missing.heic"
    working_path = images_dir / "missing.jpg"
    calibration_uuid = str(uuid.uuid4())

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    calibration_id = _seed_calibration_row(
        source_path,
        working_path,
        calibration_uuid=calibration_uuid,
    )

    monkeypatch.setattr(app_identity, "app_data_dir", lambda: tmp_path / "appdata")
    written = models.CalibrationAssetDB.backfill_all()
    assets = models.CalibrationAssetDB.get_assets_for_calibration(calibration_id)
    by_role = {asset["role"]: asset for asset in assets}

    assert written > 0
    assert by_role["source_photo"]["bytes"] is None
    assert by_role["working_photo"]["bytes"] is None
    assert by_role["calibration_crop"]["mime_type"] is None
    assert by_role["overlay"]["mime_type"] is None
    assert by_role["debug_artifact"]["mime_type"] is None


def test_calibration_bundle_export_import_preserves_calibration_assets(tmp_path, monkeypatch):
    source_db = tmp_path / "source" / "mushrooms.db"
    dest_db = tmp_path / "dest" / "mushrooms.db"
    bundle_path = tmp_path / "calibration_bundle.zip"

    monkeypatch.setattr(schema, "get_database_path", lambda: source_db)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    with monkeypatch.context() as ctx:
        ctx.setattr(models.CalibrationAssetDB, "backfill_all", lambda: 0)
        schema.init_database()

    source_images_dir = source_db.parent / "images"
    source_cal_dir = source_images_dir / "calibrations" / "100X"
    source_cal_dir.mkdir(parents=True, exist_ok=True)

    source_path = source_cal_dir / "source.heic"
    working_path = source_cal_dir / "working.jpg"
    cache_path = source_cal_dir / "reference.webp"
    source_path.write_bytes(b"heic-bytes")
    working_path.write_bytes(b"jpeg-bytes")
    cache_path.write_bytes(b"webp-bytes")

    calibration_uuid = str(uuid.uuid4())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(source_db))
    calibration_id = _seed_calibration_row(
        source_path,
        working_path,
        calibration_uuid=calibration_uuid,
    )
    models.CalibrationAssetDB.record_reference_cache_asset(
        calibration_id=calibration_id,
        calibration_uuid=calibration_uuid,
        cache_path=cache_path,
        image_storage_path=f"user-123/{calibration_uuid}/reference.webp",
        original_path=str(source_path),
        metadata={"downloaded_via": "test"},
    )

    monkeypatch.setattr(db_share, "get_database_path", lambda: source_db)
    monkeypatch.setattr(db_share, "get_images_dir", lambda: source_images_dir)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: source_db.parent / "objectives.json")
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: source_db.parent / "reference_values.db")
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)

    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    monkeypatch.setattr(schema, "get_database_path", lambda: dest_db)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    with monkeypatch.context() as ctx:
        ctx.setattr(models.CalibrationAssetDB, "backfill_all", lambda: 0)
        schema.init_database()

    dest_images_dir = dest_db.parent / "images"
    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(dest_db))
    monkeypatch.setattr(db_share, "get_images_dir", lambda: dest_images_dir)

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    assets = _fetch_assets(dest_db)
    by_role = {asset["role"]: asset for asset in assets}

    assert result["calibrations"] == 1
    assert {"source_photo", "working_photo", "calibration_crop", "overlay", "debug_artifact", "reference_cache"} <= set(by_role)
    assert Path(by_role["source_photo"]["local_path"]).exists()
    assert Path(by_role["working_photo"]["local_path"]).exists()
    assert Path(by_role["reference_cache"]["local_path"]).exists()
    assert by_role["working_photo"]["original_path"].endswith("source.heic")
    assert by_role["reference_cache"]["source_role"] == "cloud_recovery_cache"
