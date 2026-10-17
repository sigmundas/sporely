"""S2: immutable local sync keys on observations, images and measurements.

Every local creation path mints a uuid4 key in the same transaction as the
INSERT; the pull path stores the remote key (always NULL today) and never
mints; fork paths (portable import, legacy bundle import, image move) mint
fresh keys; full restore keeps them. No sync code reads, sends or compares
the key yet.
"""
from __future__ import annotations

import ast
import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from database import models, schema
from database.models import ImageDB, MeasurementDB, ObservationDB
from utils import cloud_sync, db_share
from utils.cloud_sync_impl.image_push import _prepared_item_remote_payload
from utils.cloud_sync_impl.preflight import (
    _local_image_snapshot_payload,
    _observation_push_diff_fields,
)
from utils.cloud_sync_impl.push_payloads import (
    _analyze_observation_field_changes,
    _observation_compare_payload,
    _observation_push_payload,
)
from utils.cloud_sync_impl.reconciliation.images import (
    _analyze_image_changes,
    _image_metadata_payload,
)
from utils.cloud_sync_impl.reconciliation.measurements import (
    _measurement_payloads_match,
    _measurement_push_diff_fields,
    _measurement_sync_payload,
)

TABLES = ("observations", "images", "spore_measurements")


def _is_uuid4(value) -> bool:
    try:
        return uuid.UUID(str(value)).version == 4 and str(uuid.UUID(str(value))) == value
    except (TypeError, ValueError):
        return False


def _forbid_minting(monkeypatch):
    def fail():
        raise AssertionError("sync key minted on a path that must not mint")

    monkeypatch.setattr(models, "new_sync_key", fail)
    monkeypatch.setattr(db_share, "new_sync_key", fail)


@pytest.fixture
def installation(monkeypatch, tmp_path):
    root = tmp_path / "installation"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    return root


def _row(table: str, row_id: int) -> dict:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        return dict(connection.execute(f"SELECT * FROM {table} WHERE id=?", (row_id,)).fetchone())


def _keys(table: str) -> dict[int, str | None]:
    with sqlite3.connect(schema.get_database_path()) as connection:
        return dict(connection.execute(f"SELECT id, sync_key FROM {table} ORDER BY id"))


class _Point:
    def __init__(self, x: float, y: float) -> None:
        self._x, self._y = x, y

    def x(self) -> float:
        return self._x

    def y(self) -> float:
        return self._y


def _image_file(root: Path, name: str = "field.jpg") -> Path:
    path = root / "src" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"image-bytes-" + name.encode())
    return path


# ── Migration ────────────────────────────────────────────────────────────────


def _legacy_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT, notes TEXT,
                                   sync_status TEXT, updated_at TEXT);
        CREATE TABLE images (id INTEGER PRIMARY KEY, observation_id INTEGER, cloud_id TEXT);
        CREATE TABLE spore_measurements (id INTEGER PRIMARY KEY, image_id INTEGER, cloud_id TEXT);
        INSERT INTO observations VALUES (1, NULL, 'unlinked', 'dirty', '2026-01-01');
        INSERT INTO observations VALUES (2, 'cloud-obs', 'linked', 'synced', '2026-01-02');
        INSERT INTO observations VALUES (3, '  ', 'blank link', 'local', '2026-01-03');
        INSERT INTO images VALUES (10, 1, NULL);
        INSERT INTO images VALUES (11, 2, 'cloud-img');
        INSERT INTO spore_measurements VALUES (20, 10, NULL);
        INSERT INTO spore_measurements VALUES (21, 11, 'cloud-meas');
        """
    )
    return connection


def test_migration_keys_only_unlinked_rows_and_is_idempotent(tmp_path):
    connection = _legacy_database(tmp_path / "legacy.db")
    schema.ensure_sync_key_columns(connection.cursor())
    connection.commit()

    first = {
        table: dict(connection.execute(f"SELECT id, sync_key FROM {table} ORDER BY id"))
        for table in TABLES
    }
    assert _is_uuid4(first["observations"][1])
    assert first["observations"][2] is None
    assert _is_uuid4(first["observations"][3])  # whitespace cloud_id is not a link
    assert _is_uuid4(first["images"][10]) and first["images"][11] is None
    assert _is_uuid4(first["spore_measurements"][20]) and first["spore_measurements"][21] is None
    minted = [key for keys in first.values() for key in keys.values() if key]
    assert len(minted) == len(set(minted)) == 4
    # Only sync_key is written: no dirty flag or timestamp moves.
    assert list(connection.execute("SELECT sync_status, updated_at FROM observations ORDER BY id")) == [
        ("dirty", "2026-01-01"), ("synced", "2026-01-02"), ("local", "2026-01-03"),
    ]

    schema.ensure_sync_key_columns(connection.cursor())
    connection.commit()
    second = {
        table: dict(connection.execute(f"SELECT id, sync_key FROM {table} ORDER BY id"))
        for table in TABLES
    }
    assert second == first

    for table in TABLES:
        (index_sql,) = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (f"idx_{table}_sync_key",),
        ).fetchone()
        assert "UNIQUE" in index_sql and "WHERE sync_key IS NOT NULL" in index_sql
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE observations SET sync_key=? WHERE id=3", (first["observations"][1],))
    # Multiple NULL keys stay allowed (linked rows).
    connection.execute("UPDATE observations SET sync_key=NULL WHERE id IN (1, 3)")
    connection.close()


def test_init_database_backfills_after_cloud_columns(installation):
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date, cloud_id) VALUES (1, '2026-01-01', NULL)")
        connection.execute("INSERT INTO observations (id, date, cloud_id) VALUES (2, '2026-01-02', 'c-2')")
        connection.commit()
    schema.init_database()
    keys = _keys("observations")
    assert _is_uuid4(keys[1]) and keys[2] is None
    schema.init_database()
    assert _keys("observations") == keys


def test_sync_key_column_has_no_default(installation):
    """The pull measurement INSERT omits the column, so it must store NULL."""
    with sqlite3.connect(schema.get_database_path()) as connection:
        for table in TABLES:
            column = [row for row in connection.execute(f"PRAGMA table_info({table})") if row[1] == "sync_key"]
            assert column and column[0][4] is None


# ── Creation paths ───────────────────────────────────────────────────────────


def test_each_local_creation_path_mints_a_key(installation):
    obs_id = ObservationDB.create_observation(date="2026-10-13", genus="Amanita")
    image_id = ImageDB.add_image(
        obs_id, str(_image_file(installation)), "field", copy_to_folder=False
    )
    points4 = [_Point(0, 0), _Point(1, 0), _Point(0, 1), _Point(1, 1)]
    m4 = MeasurementDB.add_measurement(image_id, 10.0, 5.0, points=points4)
    m2 = MeasurementDB.add_measurement(image_id, 10.0, 5.0, points=points4[:2])
    m0 = MeasurementDB.add_measurement(image_id, 10.0, 5.0)

    keys = [
        _row("observations", obs_id)["sync_key"],
        _row("images", image_id)["sync_key"],
        *(_row("spore_measurements", mid)["sync_key"] for mid in (m4, m2, m0)),
    ]
    assert all(_is_uuid4(key) for key in keys)
    assert len(set(keys)) == len(keys)


def test_explicit_none_stores_null_without_minting(installation, monkeypatch):
    _forbid_minting(monkeypatch)
    obs_id = ObservationDB.create_observation(date="2026-10-13", sync_key=None)
    image_id = ImageDB.add_image(
        obs_id, str(_image_file(installation)), "field", copy_to_folder=False, sync_key=None
    )
    meas_id = MeasurementDB.add_measurement(image_id, 10.0, sync_key=None)
    assert _row("observations", obs_id)["sync_key"] is None
    assert _row("images", image_id)["sync_key"] is None
    assert _row("spore_measurements", meas_id)["sync_key"] is None


def test_explicit_key_is_stored_as_given(installation):
    key = str(uuid.uuid4())
    obs_id = ObservationDB.create_observation(date="2026-10-13", sync_key=key)
    assert _row("observations", obs_id)["sync_key"] == key


def test_create_observation_stores_location_provenance_and_key(installation):
    key = str(uuid.uuid4())
    obs_id = ObservationDB.create_observation(
        date="2026-10-13",
        gps_latitude=59.9,
        gps_longitude=10.7,
        location_source="photo_exif",
        location_status="confirmed",
        sync_key=key,
    )
    row = _row("observations", obs_id)
    assert row["location_source"] == "photo_exif"
    assert row["location_status"] == "confirmed"
    assert (row["gps_latitude"], row["gps_longitude"]) == (59.9, 10.7)
    assert row["sync_key"] == key

    minted_id = ObservationDB.create_observation(
        date="2026-10-13", location_source="manual", location_status="confirmed"
    )
    minted = _row("observations", minted_id)
    assert (minted["location_source"], minted["location_status"]) == ("manual", "confirmed")
    assert _is_uuid4(minted["sync_key"])


def test_pull_creates_observation_with_null_key_and_never_mints(installation, monkeypatch):
    monkeypatch.setattr(
        cloud_sync, "_import_remote_images",
        lambda *a, **k: {"imported": 0, "warnings": [], "errors": [], "complete": True},
    )
    monkeypatch.setattr(
        cloud_sync, "_import_remote_measurements_for_observation",
        lambda *a, **k: {"warnings": []},
    )
    _forbid_minting(monkeypatch)
    local_id = cloud_sync._create_local_from_remote(
        {"id": "14", "desktop_id": 917, "date": "2026-09-15", "genus": "Conocybe", "species": "rugosa"}
    )
    row = _row("observations", local_id)
    assert row["cloud_id"] == "14"
    assert row["sync_key"] is None


def test_measurement_pull_insert_stores_null_key_without_minting(installation, monkeypatch):
    """Runs the real pull INSERT into spore_measurements on the upgraded schema."""
    image_path = _image_file(installation, "micro.jpg")
    with sqlite3.connect(schema.get_database_path()) as connection:
        obs_id = connection.execute(
            "INSERT INTO observations (date, cloud_id, sync_key) VALUES ('2026-10-13', 'cloud-obs', NULL)"
        ).lastrowid
        image_id = connection.execute(
            "INSERT INTO images (observation_id, filepath, image_type, cloud_id, sync_key) "
            "VALUES (?, ?, 'microscope', 'cloud-image-1', NULL)",
            (obs_id, str(image_path)),
        ).lastrowid
        connection.commit()
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", lambda *a, **k: [])
    _forbid_minting(monkeypatch)

    class Client:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements are supplied directly")

        def set_measurement_desktop_id(self, *args, **kwargs):
            pass

    result = cloud_sync._import_remote_measurements_for_observation(
        Client(),
        obs_id,
        "cloud-obs",
        remote_images=[{
            "id": "cloud-image-1", "observation_id": "cloud-obs", "image_type": "microscope",
            "storage_path": "user/cloud-obs/cloud-image-1.jpg", "sort_order": 0,
        }],
        remote_measurements=[{
            "id": "cloud-measurement-1", "image_id": "cloud-image-1", "length_um": 12.5,
            "width_um": 7.5, "measurement_type": "manual", "measured_at": "2026-05-01T12:00:00Z",
        }],
        materialize_remote_images=False,
    )

    assert result["imported"] == 1, result
    with sqlite3.connect(schema.get_database_path()) as connection:
        rows = connection.execute(
            "SELECT image_id, cloud_id, sync_key FROM spore_measurements"
        ).fetchall()
    assert rows == [(image_id, "cloud-measurement-1", None)]


def test_every_image_pull_add_image_call_stores_the_cloud_key_and_never_mints():
    source = Path(cloud_sync.__file__).parent / "cloud_sync_impl" / "image_pull.py"
    calls = [
        node for node in ast.walk(ast.parse(source.read_text(encoding="utf-8")))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_image"
    ]
    assert len(calls) == 3
    for call in calls:
        keyword = next((kw for kw in call.keywords if kw.arg == "sync_key"), None)
        assert keyword is not None, f"add_image at line {call.lineno} would mint"
        # Stage 5: pull stores the cloud row's key (NULL stays NULL).
        assert isinstance(keyword.value, ast.Call) and keyword.value.func.id == "remote_sync_key"


# ── Fork paths ───────────────────────────────────────────────────────────────


def test_image_move_mints_fresh_keys_for_image_and_measurements(installation):
    source_id = ObservationDB.create_observation(date="2026-10-13", genus="Source")
    target_id = ObservationDB.create_observation(date="2026-10-13", genus="Target")
    image_id = ImageDB.add_image(
        source_id, str(_image_file(installation)), "microscope", copy_to_folder=False
    )
    meas_id = MeasurementDB.add_measurement(image_id, 10.0, 5.0)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE observations SET cloud_id='cloud-src' WHERE id=?", (source_id,))
        connection.execute("UPDATE images SET cloud_id='cloud-img' WHERE id=?", (image_id,))
        connection.execute("UPDATE spore_measurements SET cloud_id='cloud-m' WHERE id=?", (meas_id,))
        connection.commit()
    before_image = _row("images", image_id)["sync_key"]
    before_meas = _row("spore_measurements", meas_id)["sync_key"]

    assert ImageDB.move_images_to_observation([image_id], source_id, target_id) == 1

    image = _row("images", image_id)
    measurement = _row("spore_measurements", meas_id)
    assert image["observation_id"] == target_id and image["cloud_id"] is None
    assert measurement["cloud_id"] is None
    assert _is_uuid4(image["sync_key"]) and image["sync_key"] != before_image
    assert _is_uuid4(measurement["sync_key"]) and measurement["sync_key"] != before_meas


def test_portable_import_mints_fresh_keys(monkeypatch, tmp_path):
    from tests.test_portable_identity_import import (
        _initialize_database_pair,
        _insert_compatible_destination_stable_entities,
        _insert_source_graph,
    )
    from utils.archive.portable_import import import_portable_payload

    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    source_keys = {}
    with sqlite3.connect(source_main) as connection:
        for table, row_id in (("observations", 11), ("images", 21), ("spore_measurements", 31)):
            source_keys[table] = str(uuid.uuid4())
            connection.execute(f"UPDATE {table} SET sync_key=? WHERE id=?", (source_keys[table], row_id))
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _insert_compatible_destination_stable_entities(destination_main, destination_reference)

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="s2-sync-key-test",
    )

    imported = {
        "observations": result.observation_id_map[11],
        "images": result.image_id_map[21],
        "spore_measurements": result.measurement_id_map[31],
    }
    keys = []
    with sqlite3.connect(destination_main) as connection:
        for table, row_id in imported.items():
            cloud_id, key = connection.execute(
                f"SELECT cloud_id, sync_key FROM {table} WHERE id=?", (row_id,)
            ).fetchone()
            assert cloud_id is None
            assert _is_uuid4(key) and key != source_keys[table]
            keys.append(key)
    assert len(set(keys)) == 3


def _bundle_paths(monkeypatch, root: Path) -> None:
    # The bundle module imports path helpers by name; route them to this library.
    monkeypatch.setattr(db_share, "get_database_path", schema.get_database_path)
    monkeypatch.setattr(db_share, "get_images_dir", schema.get_images_dir)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: root / "objectives.json")
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: root / "reference_values.db")
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)


def test_bundle_import_from_same_library_forks_cloud_identity(installation, monkeypatch, tmp_path):
    _bundle_paths(monkeypatch, installation)
    images_dir = schema.get_images_dir()
    images_dir.mkdir(parents=True, exist_ok=True)
    image_path = images_dir / "obs" / "micro.jpg"
    image_path.parent.mkdir(parents=True)
    image_path.write_bytes(b"micro-bytes")

    obs_id = ObservationDB.create_observation(date="2026-10-13", genus="Amanita")
    image_id = ImageDB.add_image(obs_id, str(image_path), "microscope", copy_to_folder=False)
    meas_id = MeasurementDB.add_measurement(image_id, 10.0, 5.0)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "UPDATE observations SET cloud_id='cloud-obs', sync_status='synced', "
            "synced_at='2026-10-01', sync_error_code='e', sync_error_message='m', "
            "sync_blocked_reason='b', sync_blocked_at='2026-10-02', "
            "mosaic_signature='sig', region_id='region' WHERE id=?",
            (obs_id,),
        )
        connection.execute(
            "UPDATE images SET cloud_id='cloud-img', synced_at='2026-10-01' WHERE id=?", (image_id,)
        )
        connection.execute("UPDATE spore_measurements SET cloud_id='cloud-meas' WHERE id=?", (meas_id,))
        connection.commit()
    original_keys = {table: set(_keys(table).values()) for table in TABLES}

    bundle = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle),
        include_observations=True,
        include_images=True,
        include_measurements=True,
        include_calibrations=False,
        include_reference_values=False,
    )
    db_share.import_database_bundle(
        str(bundle),
        include_observations=True,
        include_images=True,
        include_measurements=True,
        include_calibrations=False,
        include_reference_values=False,
    )

    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        for table in TABLES:
            duplicates = connection.execute(
                f"SELECT cloud_id FROM {table} WHERE cloud_id IS NOT NULL "
                "GROUP BY cloud_id HAVING COUNT(*) > 1"
            ).fetchall()
            assert duplicates == [], f"{table} rows share a cloud_id"
            rows = connection.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
            assert len(rows) == 2
            imported = dict(rows[1])
            assert imported["cloud_id"] is None
            assert _is_uuid4(imported["sync_key"])
            assert imported["sync_key"] not in original_keys[table]
        observation = dict(connection.execute("SELECT * FROM observations ORDER BY id DESC").fetchone())
        image = dict(connection.execute("SELECT * FROM images ORDER BY id DESC").fetchone())
    for field in db_share._BUNDLE_OBSERVATION_CLEARED_FIELDS:
        assert observation[field] is None, field
    assert observation["sync_status"] == "local"
    assert observation["portable_cloud_identity_pending"] == 1
    assert image["synced_at"] is None


def test_full_restore_keeps_keys_and_cloud_ids(installation, tmp_path):
    from utils.archive.full_backup import create_full_backup
    from utils.archive.full_restore import restore_full_backup

    image_path = schema.get_images_dir() / "source.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"archived-image")
    linked_image_path = schema.get_images_dir() / "linked.jpg"
    linked_image_path.write_bytes(b"linked-image")
    # Locally minted, unlinked records.
    obs_id = ObservationDB.create_observation(date="2026-10-13", genus="Archived")
    image_id = ImageDB.add_image(obs_id, str(image_path), "field", copy_to_folder=False)
    meas_id = MeasurementDB.add_measurement(image_id, 10.0, 5.0)
    # Linked legacy records: cloud_id set, NULL key.
    with sqlite3.connect(schema.get_database_path()) as connection:
        linked_obs = connection.execute(
            "INSERT INTO observations (date, genus, cloud_id) VALUES ('2026-10-12', 'Linked', 'cloud-obs')"
        ).lastrowid
        linked_image = connection.execute(
            "INSERT INTO images (observation_id, filepath, image_type, cloud_id) "
            "VALUES (?, ?, 'microscope', 'cloud-img')",
            (linked_obs, str(linked_image_path)),
        ).lastrowid
        linked_meas = connection.execute(
            "INSERT INTO spore_measurements (image_id, length_um, cloud_id) VALUES (?, 9.0, 'cloud-meas')",
            (linked_image,),
        ).lastrowid
        connection.commit()

    def state() -> dict:
        with sqlite3.connect(schema.get_database_path()) as connection:
            return {
                table: list(connection.execute(f"SELECT id, cloud_id, sync_key FROM {table} ORDER BY id"))
                for table in TABLES
            }

    before = state()
    assert before == {
        "observations": [(obs_id, None, _row("observations", obs_id)["sync_key"]), (linked_obs, "cloud-obs", None)],
        "images": [(image_id, None, _row("images", image_id)["sync_key"]), (linked_image, "cloud-img", None)],
        "spore_measurements": [
            (meas_id, None, _row("spore_measurements", meas_id)["sync_key"]),
            (linked_meas, "cloud-meas", None),
        ],
    }
    assert all(_is_uuid4(rows[0][2]) for rows in before.values())
    archive = tmp_path / "source.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})

    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM spore_measurements")
        connection.execute("DELETE FROM images")
        connection.execute("DELETE FROM observations")
        connection.commit()
    restore_full_backup(
        archive, app_version="test", safety_backup_path=tmp_path / "safety.sporely",
        close_live=lambda: None, reopen_live=lambda: None,
    )

    assert state() == before


# ── Sync code ignores sync_key ───────────────────────────────────────────────


_OBS = {
    "id": 7, "cloud_id": "cloud-obs", "date": "2026-10-13", "genus": "Amanita",
    "species": "muscaria", "notes": "n", "sharing_scope": "public", "is_draft": 0,
}
_IMG = {
    "id": 9, "cloud_id": "cloud-img", "observation_id": 7, "image_type": "microscope",
    "filepath": "/x/micro.jpg", "notes": "n", "sort_order": 0, "objective_name": "100X",
}
_MEAS = {
    "id": 5, "cloud_id": "cloud-meas", "image_id": 9, "length_um": 10.0, "width_um": 5.0,
    "measurement_type": "manual", "measured_at": "2026-10-13 10:00:00",
}


def _keyed(row: dict) -> dict:
    return {**row, "sync_key": str(uuid.uuid4())}


def test_observation_payloads_and_compare_ignore_sync_key():
    keyed = _keyed(_OBS)
    for local in (True, False):
        payload = _observation_push_payload(keyed, local=local)
        assert "sync_key" not in payload
        assert payload == _observation_push_payload(_OBS, local=local)
        compare = _observation_compare_payload(keyed, local=local)
        assert "sync_key" not in compare
        assert compare == _observation_compare_payload(_OBS, local=local)
    remote = {**_observation_compare_payload(_OBS, local=True), "id": "cloud-obs", "desktop_id": 7}
    remote_keyed = _keyed(remote)
    assert _observation_push_diff_fields(keyed, remote) == _observation_push_diff_fields(_OBS, remote)
    assert _observation_push_diff_fields(keyed, remote_keyed) == _observation_push_diff_fields(_OBS, remote)
    changes = _analyze_observation_field_changes(keyed, remote_keyed, remote)
    assert changes == _analyze_observation_field_changes(_OBS, remote, remote)
    assert "sync_key" not in changes["local_only_fields"] + changes["remote_only_fields"]


def test_observation_snapshot_ignores_sync_key():
    remote = {"id": "cloud-obs", "desktop_id": 7, "date": "2026-10-13", "genus": "Amanita"}
    stored = cloud_sync._cloud_observation_snapshot(remote, [], [])
    assert "sync_key" not in json.loads(stored)["observation"]
    assert cloud_sync._cloud_observation_snapshot(_keyed(remote), [], []) == stored
    assert cloud_sync._remote_snapshot_has_meaningful_changes(_keyed(remote), [], [], stored) is False


def test_image_payloads_and_compare_ignore_sync_key():
    keyed = _keyed(_IMG)
    for build in (_local_image_snapshot_payload, _image_metadata_payload):
        assert "sync_key" not in build(keyed)
        assert build(keyed) == build(_IMG)
    remote_payload = _prepared_item_remote_payload(keyed, "/x/upload.webp", "user/obs/micro.webp")
    assert "sync_key" not in remote_payload
    assert remote_payload == _prepared_item_remote_payload(_IMG, "/x/upload.webp", "user/obs/micro.webp")
    baseline = [_local_image_snapshot_payload(_IMG)]
    assert _analyze_image_changes([_local_image_snapshot_payload(keyed)], baseline)["changed"] is False
    assert _analyze_image_changes([keyed], [_IMG])["changed"] is False


def test_measurement_payloads_and_compare_ignore_sync_key():
    keyed = _keyed(_MEAS)
    payload = _measurement_sync_payload(keyed, local=True, cloud_image_id="cloud-img")
    assert "sync_key" not in payload
    assert payload == _measurement_sync_payload(_MEAS, local=True, cloud_image_id="cloud-img")
    remote = {**_MEAS, "id": "cloud-meas", "desktop_id": 5, "image_id": "cloud-img"}
    assert _measurement_payloads_match(keyed, _keyed(remote), cloud_image_id="cloud-img")
    assert _measurement_push_diff_fields(keyed, _keyed(remote), cloud_image_id="cloud-img") == []


def test_dirty_detection_ignores_sync_key_changes_on_a_real_library(installation):
    """Real DB rows, real snapshot store, real media signature and measurements."""
    obs_id = ObservationDB.create_observation(date="2026-10-13", genus="Amanita", species="muscaria")
    image_id = ImageDB.add_image(
        obs_id, str(_image_file(installation, "micro.jpg")), "microscope", copy_to_folder=False
    )
    meas_id = MeasurementDB.add_measurement(image_id, 10.0, 5.0)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE observations SET cloud_id='cloud-obs' WHERE id=?", (obs_id,))
        connection.execute("UPDATE images SET cloud_id='cloud-img' WHERE id=?", (image_id,))
        connection.execute("UPDATE spore_measurements SET cloud_id='cloud-meas' WHERE id=?", (meas_id,))
        connection.commit()

    local = ObservationDB.get_observation(obs_id)
    _, measurements = cloud_sync._load_local_measurement_lookup(obs_id)
    snapshot = {
        "schema_version": 2,
        "observation": _observation_compare_payload(local, local=True),
        "measurements": [
            cloud_sync._local_measurement_snapshot_payload(row) for row in measurements.values()
        ],
    }
    cloud_sync._store_cloud_observation_snapshot("cloud-obs", json.dumps(snapshot))
    cloud_sync._store_local_cloud_media_signature(obs_id, cloud_sync._local_cloud_media_signature(obs_id))
    assert cloud_sync._local_has_real_changes_since_snapshot(local, "cloud-obs") is False

    with sqlite3.connect(schema.get_database_path()) as connection:
        for table, row_id in (("observations", obs_id), ("images", image_id), ("spore_measurements", meas_id)):
            connection.execute(f"UPDATE {table} SET sync_key=? WHERE id=?", (str(uuid.uuid4()), row_id))
        connection.commit()
    local = ObservationDB.get_observation(obs_id)
    assert cloud_sync._local_has_real_changes_since_snapshot(local, "cloud-obs") is False

    # Control: a real measurement edit is still detected by the same baseline.
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE spore_measurements SET length_um=11.0 WHERE id=?", (meas_id,))
        connection.commit()
    assert cloud_sync._local_has_real_changes_since_snapshot(local, "cloud-obs") is True
