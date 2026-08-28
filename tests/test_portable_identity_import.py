from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import schema
from utils.archive.portable_import import (
    PortableIdentityConflictError,
    PortableImportError,
    import_portable_payload,
)


CALIBRATION_UUID = "11111111-1111-4111-8111-111111111111"
ASSET_UUID = "22222222-2222-4222-8222-222222222222"


def _initialize_database_pair(monkeypatch, root: Path) -> tuple[Path, Path]:
    root.mkdir()
    main = root / "mushrooms.db"
    reference = root / "reference_values.db"
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", main)
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", reference)
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    return main, reference


def _insert_reference_graph(connection: sqlite3.Connection, *, legacy_id: int) -> None:
    connection.execute("DELETE FROM reference_values")
    connection.execute(
        "INSERT INTO reference_values "
        "(id, genus, species, source, mount_medium, stain, metadata_json) "
        "VALUES (?, 'Amanita', 'muscaria', 'book', 'KOH', 'Congo red', ?)",
        (legacy_id, json.dumps({"scientific_note": "provenance"})),
    )
    connection.execute(
        "INSERT INTO reference_works "
        "(id, type, title, short_label, revision, owner_id) "
        "VALUES ('work-a', 'book', 'Selected work', 'Work A', 1, 'source-owner')"
    )
    connection.execute(
        "INSERT INTO reference_taxon_treatments "
        "(id, reference_work_id, name_as_published, revision) "
        "VALUES ('treatment-a', 'work-a', 'Amanita muscaria', 1)"
    )
    connection.execute(
        "INSERT INTO reference_measurement_sets "
        "(id, taxon_treatment_id, character, data_kind, length_min, length_max, "
        " legacy_reference_value_id, revision) "
        "VALUES ('set-a', 'treatment-a', 'spore_size', 'range', 8.0, 11.0, ?, 1)",
        (legacy_id,),
    )


def _insert_source_graph(main: Path, reference: Path) -> None:
    snapshot = {
        "schema_version": 1,
        "reference_work_id": "work-a",
        "reference_treatment_id": "treatment-a",
        "reference_measurement_set_id": "set-a",
        "reference_revision": 1,
        "full_citation": "Selected work",
        "short_label": "Work A",
        "work_type": "book",
        "year": None,
        "doi": None,
        "isbn": None,
        "taxon_id": None,
        "name_as_published": "Amanita muscaria",
        "locator_text": None,
        "page_from": None,
        "page_to": None,
        "character": "spore_size",
        "data_kind": "range",
        "raw_text": None,
        "measurements": {},
        "method": {},
        "raw_points": None,
    }
    with sqlite3.connect(main) as connection:
        connection.execute(
            "INSERT INTO observations "
            "(id, date, genus, species, notes, cloud_id, sync_status, synced_at, "
            " sync_error_code, sync_error_message, sync_blocked_reason, sync_blocked_at, "
            " mosaic_signature, region_id, ai_state_json) "
            "VALUES (11, '2026-08-27', 'Amanita', 'muscaria', 'field provenance', "
            " 'source-cloud-observation', 'synced', '2026-08-27T10:00:00Z', "
            " 'remote-error', 'remote message', 'remote-block', '2026-08-27T10:01:00Z', "
            " 'source-mosaic', 'source-region', ?)",
            (json.dumps({"image_ids": [21], "scores": [0.9]}),),
        )
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            " notes, image_filepath, is_active) "
            "VALUES (61, ?, '100X', '2026-08-20', 0.123, 'calibration provenance', "
            " '/source/calibration.tif', 1)",
            (CALIBRATION_UUID,),
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(id, asset_uuid, calibration_id, calibration_uuid, role, source_role, "
            " file_purpose, local_path, original_path, cloud_storage_path, mime_type, "
            " width, height, bytes, sha256, metadata_json) "
            "VALUES (71, ?, 61, ?, 'source', 'local_original', 'authoritative', "
            " '/source/calibration.tif', '/source/calibration.raw', 'source/cloud/key', "
            " 'image/tiff', 100, 200, 300, 'abc123', ?)",
            (
                ASSET_UUID,
                CALIBRATION_UUID,
                json.dumps({
                    "scientific_label": "stage micrometer",
                    "image_storage_path": "source/cloud/key",
                    "source_path": "/source/calibration.tif",
                }),
            ),
        )
        connection.execute(
            "INSERT INTO images "
            "(id, observation_id, filepath, original_filepath, image_type, objective_name, "
            " calibration_id, cloud_id, synced_at, lab_metadata) "
            "VALUES (21, 11, '/source/image.jpg', '/source/image.raw', 'microscope', "
            " '100X', 61, 'source-cloud-image', '2026-08-27T10:00:00Z', ?)",
            (json.dumps({"session_id": "image-only-session", "objective_name": "100X"}),),
        )
        connection.execute(
            "INSERT INTO spore_measurements "
            "(id, image_id, length_um, width_um, notes, cloud_id) "
            "VALUES (31, 21, 10.0, 5.0, 'measurement provenance', 'source-cloud-measurement')"
        )
        connection.execute(
            "INSERT INTO spore_annotations "
            "(id, image_id, measurement_id, spore_number, annotation_source) "
            "VALUES (41, 21, 31, 1, 'manual')"
        )
        connection.execute(
            "INSERT INTO session_logs "
            "(id, observation_id, session_id, session_kind, event_type, metadata_json) "
            "VALUES (51, 11, 'source-session', 'live', 'image_imported', ?)",
            (json.dumps({
                "image_id": 21,
                "filepath": "/source/image.jpg",
                "watch_dir": "/Users/source-person/Private Capture Folder",
                "lab_metadata": {"session_id": "source-session"},
            }),),
        )
        connection.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, note, "
            " reference_revision, snapshot_json) "
            "VALUES ('source-use', 11, 'set-a', 'compared', 'comparison provenance', 1, ?)",
            (json.dumps(snapshot, sort_keys=True),),
        )
        connection.commit()
    with sqlite3.connect(reference) as connection:
        _insert_reference_graph(connection, legacy_id=81)
        connection.commit()


def _insert_compatible_destination_stable_entities(main: Path, reference: Path) -> None:
    with sqlite3.connect(main) as connection:
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            " notes, image_filepath, is_active) "
            "VALUES (6, ?, '100X', '2026-08-20', 0.123, 'calibration provenance', "
            " '/destination/calibration.tif', 0)",
            (CALIBRATION_UUID,),
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(id, asset_uuid, calibration_id, calibration_uuid, role, source_role, "
            " file_purpose, local_path, original_path, cloud_storage_path, mime_type, "
            " width, height, bytes, sha256, metadata_json) "
            "VALUES (7, ?, 6, ?, 'source', 'local_original', 'authoritative', "
            " '/destination/calibration.tif', '/destination/calibration.raw', NULL, "
            " 'image/tiff', 100, 200, 300, 'abc123', ?)",
            (
                ASSET_UUID,
                CALIBRATION_UUID,
                json.dumps({
                    "scientific_label": "stage micrometer",
                    "source_path": "/destination/calibration.tif",
                }),
            ),
        )
        connection.commit()
    with sqlite3.connect(reference) as connection:
        _insert_reference_graph(connection, legacy_id=8)
        connection.execute("UPDATE reference_works SET owner_id='destination-owner' WHERE id='work-a'")
        connection.commit()


def test_portable_identity_import_remaps_complete_graph_and_neutralizes_cloud_state(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _insert_compatible_destination_stable_entities(destination_main, destination_reference)

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    assert result.observation_id_map[11] != 11
    assert result.image_id_map[21] != 21
    assert result.measurement_id_map[31] != 31
    assert result.annotation_id_map[41] != 41
    assert result.session_log_id_map[51] != 51
    assert result.calibration_id_map == {61: 6}
    assert result.calibration_asset_id_map == {71: 7}
    assert result.reference_value_id_map == {81: 8}
    assert result.reference_use_id_map["source-use"] != "source-use"
    assert result.session_id_map["source-session"] != "source-session"
    assert result.session_id_map["image-only-session"] != "image-only-session"

    observation_id = result.observation_id_map[11]
    image_id = result.image_id_map[21]
    measurement_id = result.measurement_id_map[31]
    with sqlite3.connect(destination_main) as connection:
        connection.row_factory = sqlite3.Row
        observation = dict(connection.execute(
            "SELECT * FROM observations WHERE id=?", (observation_id,)
        ).fetchone())
        image = dict(connection.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone())
        measurement = dict(connection.execute(
            "SELECT * FROM spore_measurements WHERE id=?", (measurement_id,)
        ).fetchone())
        annotation = dict(connection.execute(
            "SELECT * FROM spore_annotations WHERE id=?", (result.annotation_id_map[41],)
        ).fetchone())
        session_log = dict(connection.execute(
            "SELECT * FROM session_logs WHERE id=?", (result.session_log_id_map[51],)
        ).fetchone())
        reference_use = dict(connection.execute(
            "SELECT * FROM observation_reference_uses WHERE id=?",
            (result.reference_use_id_map["source-use"],),
        ).fetchone())

    assert observation["notes"] == "field provenance"
    assert observation["cloud_id"] is None
    assert observation["sync_status"] == "local"
    assert observation["synced_at"] is None
    assert observation["sync_error_code"] is None
    assert observation["sync_error_message"] is None
    assert observation["sync_blocked_reason"] is None
    assert observation["sync_blocked_at"] is None
    assert observation["mosaic_signature"] is None
    assert observation["region_id"] is None
    assert observation["portable_cloud_identity_pending"] == 1
    assert json.loads(observation["ai_state_json"])["image_ids"] == [image_id]
    assert image["observation_id"] == observation_id
    assert image["calibration_id"] == 6
    assert image["cloud_id"] is None
    assert image["synced_at"] is None
    assert measurement["image_id"] == image_id
    assert measurement["cloud_id"] is None
    assert annotation["image_id"] == image_id
    assert annotation["measurement_id"] == measurement_id
    assert session_log["observation_id"] == observation_id
    assert session_log["session_id"] == result.session_id_map["source-session"]
    session_metadata = json.loads(session_log["metadata_json"])
    assert session_metadata["image_id"] == image_id
    assert session_metadata["filepath"] is None
    assert session_metadata["watch_dir"] is None
    assert session_metadata["lab_metadata"]["session_id"] == result.session_id_map["source-session"]
    assert json.loads(image["lab_metadata"])["session_id"] == result.session_id_map["image-only-session"]
    assert reference_use["observation_id"] == observation_id
    assert reference_use["reference_measurement_set_id"] == "set-a"
    assert reference_use["reference_revision"] == 1
    assert json.loads(reference_use["snapshot_json"])["reference_work_id"] == "work-a"
    with sqlite3.connect(destination_main) as connection:
        asset = dict(zip(
            [row[1] for row in connection.execute("PRAGMA table_info(calibration_assets)")],
            connection.execute("SELECT * FROM calibration_assets WHERE id=7").fetchone(),
        ))
        assert asset["cloud_storage_path"] is None
        assert "image_storage_path" not in json.loads(asset["metadata_json"])
        assert connection.execute("SELECT COUNT(*) FROM image_tombstones").fetchone()[0] == 0
    with sqlite3.connect(destination_reference) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reference_works").fetchone()[0] == 1
        assert connection.execute(
            "SELECT owner_id FROM reference_works WHERE id='work-a'"
        ).fetchone()[0] == "destination-owner"
        assert connection.execute(
            "SELECT legacy_reference_value_id FROM reference_measurement_sets WHERE id='set-a'"
        ).fetchone()[0] == 8


def test_portable_identity_import_allocates_fresh_ids_for_every_integer_entity(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    assert result.observation_id_map[11] != 11
    assert result.image_id_map[21] != 21
    assert result.measurement_id_map[31] != 31
    assert result.annotation_id_map[41] != 41
    assert result.session_log_id_map[51] != 51
    assert result.calibration_id_map[61] != 61
    assert result.calibration_asset_id_map[71] != 71
    assert result.reference_value_id_map[81] != 81


def test_portable_identity_import_resolves_uuid_only_calibration_asset_parent(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_main) as connection:
        connection.execute("UPDATE calibration_assets SET calibration_id=NULL")
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute(
            "SELECT calibration_id FROM calibration_assets WHERE id=?",
            (result.calibration_asset_id_map[71],),
        ).fetchone()[0] == result.calibration_id_map[61]


def test_portable_identity_import_resolves_integer_only_calibration_asset_parent(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_main) as connection:
        connection.execute("UPDATE calibration_assets SET calibration_uuid=NULL")
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute(
            "SELECT calibration_id, calibration_uuid FROM calibration_assets WHERE id=?",
            (result.calibration_asset_id_map[71],),
        ).fetchone() == (result.calibration_id_map[61], CALIBRATION_UUID)


def test_portable_identity_import_uses_canonical_calibration_equivalence(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _insert_compatible_destination_stable_entities(destination_main, destination_reference)
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "UPDATE calibrations SET calibration_date=?, microns_per_pixel=?, "
            "measurements_json=? WHERE id=6",
            (
                "2026-08-20T12:30:00Z",
                0.1230000005,
                json.dumps({"source_path": "/destination/a.tif", "count": 4}),
            ),
        )
        connection.commit()
    with sqlite3.connect(source_main) as connection:
        connection.execute(
            "UPDATE calibrations SET measurements_json=? WHERE id=61",
            (json.dumps({"source_path": "/source/a.tif", "count": 4}),),
        )
        connection.commit()

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    assert result.calibration_id_map == {61: 6}


def test_portable_identity_import_ignores_retired_reference_work_fields(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _insert_compatible_destination_stable_entities(destination_main, destination_reference)
    with sqlite3.connect(destination_reference) as connection:
        connection.execute(
            "UPDATE reference_works SET verification_status='verified', "
            "visibility='curated_public' WHERE id='work-a'"
        )
        connection.commit()

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    assert result.reference_work_id_map == {"work-a": "work-a"}


def test_portable_identity_import_targets_attached_reference_database(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "CREATE TABLE reference_values "
            "(id INTEGER PRIMARY KEY, genus TEXT, species TEXT, source TEXT, "
            "mount_medium TEXT, stain TEXT, metadata_json TEXT, updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO reference_values (id, genus, species, source) "
            "VALUES (999, 'Legacy', 'row', 'main-db')"
        )
        connection.commit()

    result = import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reference_values").fetchone()[0] == 1
    with sqlite3.connect(destination_reference) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM reference_values WHERE id=?",
            (result.reference_value_id_map[81],),
        ).fetchone()[0] == 1


def test_portable_identity_import_preserves_external_supersedes_identity(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_reference) as connection:
        connection.execute(
            "UPDATE reference_measurement_sets SET supersedes_id='prior-set' "
            "WHERE id='set-a'"
        )
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id="phase-5-identity-test",
    )

    with sqlite3.connect(destination_reference) as connection:
        assert connection.execute(
            "SELECT supersedes_id FROM reference_measurement_sets WHERE id='set-a'"
        ).fetchone()[0] == "prior-set"


def test_portable_identity_import_rejects_conflicting_legacy_business_key(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    with sqlite3.connect(destination_reference) as connection:
        _insert_reference_graph(connection, legacy_id=8)
        connection.execute(
            "UPDATE reference_values SET metadata_json=? WHERE id=8",
            (json.dumps({"scientific_note": "different"}),),
        )
        connection.execute("DELETE FROM reference_measurement_sets")
        connection.execute("DELETE FROM reference_taxon_treatments")
        connection.execute("DELETE FROM reference_works")
        connection.commit()

    with pytest.raises(PortableIdentityConflictError, match="business key"):
        import_portable_payload(
            source_main,
            source_reference,
            destination_main_database=destination_main,
            destination_reference_database=destination_reference,
            archive_id="phase-5-identity-test",
        )


@pytest.mark.parametrize("conflict", ["calibration", "calibration_asset", "reference"])
def test_portable_identity_import_rejects_conflicting_stable_identity_before_writes(
    monkeypatch, tmp_path, conflict
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _insert_compatible_destination_stable_entities(destination_main, destination_reference)
    if conflict == "calibration":
        with sqlite3.connect(destination_main) as connection:
            connection.execute(
                "UPDATE calibrations SET microns_per_pixel=9.9 WHERE calibration_uuid=?",
                (CALIBRATION_UUID,),
            )
            connection.commit()
    elif conflict == "calibration_asset":
        with sqlite3.connect(destination_main) as connection:
            connection.execute(
                "UPDATE calibration_assets SET sha256='different' WHERE asset_uuid=?",
                (ASSET_UUID,),
            )
            connection.commit()
    else:
        with sqlite3.connect(destination_reference) as connection:
            connection.execute(
                "UPDATE reference_works SET title='Conflicting work' WHERE id='work-a'"
            )
            connection.commit()

    with pytest.raises(PortableIdentityConflictError, match="conflicting"):
        import_portable_payload(
            source_main,
            source_reference,
            destination_main_database=destination_main,
            destination_reference_database=destination_reference,
            archive_id="phase-5-identity-test",
        )

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 0


def test_portable_identity_import_rejects_crossed_reference_snapshot(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_main) as connection:
        snapshot = json.loads(connection.execute(
            "SELECT snapshot_json FROM observation_reference_uses"
        ).fetchone()[0])
        snapshot["reference_measurement_set_id"] = "different-set"
        connection.execute(
            "UPDATE observation_reference_uses SET snapshot_json=?",
            (json.dumps(snapshot, sort_keys=True),),
        )
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    with pytest.raises(PortableImportError, match="snapshot"):
        import_portable_payload(
            source_main,
            source_reference,
            destination_main_database=destination_main,
            destination_reference_database=destination_reference,
            archive_id="phase-5-identity-test",
        )

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
