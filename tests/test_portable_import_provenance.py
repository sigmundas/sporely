from __future__ import annotations

import sqlite3

import pytest

from database import schema
from tests.test_portable_identity_import import (
    _initialize_database_pair,
    _insert_source_graph,
)
from utils.archive.portable_import import PortableImportError, import_portable_payload
from utils.archive import portable_import


ARCHIVE_ID = "33333333-3333-4333-8333-333333333333"


def _import(source_main, source_reference, destination_main, destination_reference):
    return import_portable_payload(
        source_main,
        source_reference,
        destination_main_database=destination_main,
        destination_reference_database=destination_reference,
        archive_id=ARCHIVE_ID,
    )


def test_first_import_records_provenance_and_identical_replay_reuses_every_mapping(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    first = _import(source_main, source_reference, destination_main, destination_reference)
    second = _import(source_main, source_reference, destination_main, destination_reference)

    assert second.observation_id_map == first.observation_id_map
    assert second.image_id_map == first.image_id_map
    assert second.measurement_id_map == first.measurement_id_map
    assert second.annotation_id_map == first.annotation_id_map
    assert second.session_log_id_map == first.session_log_id_map
    assert second.reference_use_id_map == first.reference_use_id_map
    assert second.session_id_map == first.session_id_map
    assert second.new_item_counts == {}
    assert second.reused_item_counts == first.new_item_counts

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM spore_annotations").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM session_logs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM portable_import_provenance").fetchone()[0] > 0


def test_replay_fails_before_mutation_when_destination_mapping_is_missing(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DELETE FROM images WHERE id=?", (first.image_id_map[21],))
        before = connection.total_changes
        connection.commit()

    with pytest.raises(PortableImportError, match="missing destination"):
        _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_replay_fails_when_destination_mapping_has_crossed_relationship(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "INSERT INTO observations (date, notes) VALUES ('2026-08-29', 'unrelated')"
        )
        unrelated_id = connection.execute("SELECT max(id) FROM observations").fetchone()[0]
        connection.execute(
            "UPDATE images SET observation_id=? WHERE id=?",
            (unrelated_id, first.image_id_map[21]),
        )
        connection.commit()

    with pytest.raises(PortableImportError, match="conflicting destination relationship"):
        _import(source_main, source_reference, destination_main, destination_reference)


def test_same_archive_identity_with_changed_source_content_fails_without_writes(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(source_main) as connection:
        connection.execute("UPDATE observations SET notes='changed archive bytes' WHERE id=11")
        connection.commit()

    with pytest.raises(PortableImportError, match="conflicting source content"):
        _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT notes FROM observations").fetchone()[0] == "field provenance"
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_replay_rejects_conflicting_stable_destination_content(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_reference) as connection:
        connection.execute(
            "UPDATE reference_works SET title='conflicting destination' WHERE id='work-a'"
        )
        connection.commit()

    with pytest.raises(PortableImportError, match="conflicting destination content"):
        _import(source_main, source_reference, destination_main, destination_reference)


def test_replay_rejects_higher_revision_with_conflicting_immutable_reference_identity(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_reference) as connection:
        connection.execute(
            "UPDATE reference_measurement_sets SET revision=2, supersedes_id='other-set' "
            "WHERE id='set-a'"
        )
        connection.commit()

    with pytest.raises(PortableImportError, match="conflicting immutable destination identity"):
        _import(source_main, source_reference, destination_main, destination_reference)


def test_replay_accepts_canonicalized_stable_asset_uuid(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_main) as connection:
        connection.execute(
            "UPDATE calibration_assets SET asset_uuid='AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA'"
        )
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )

    first = _import(source_main, source_reference, destination_main, destination_reference)
    second = _import(source_main, source_reference, destination_main, destination_reference)

    assert second.calibration_asset_id_map == first.calibration_asset_id_map


def test_partial_replay_imports_only_genuinely_new_items(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    with sqlite3.connect(source_main) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, notes) VALUES (12, '2026-08-28', 'new item')"
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type) "
            "VALUES (22, 12, '/source/new.jpg', 'field')"
        )
        connection.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um) VALUES (32, 22, 7.5)"
        )
        connection.commit()
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "DELETE FROM portable_import_provenance WHERE archive_id=? AND "
            "((source_item_type='observation' AND source_item_id='12') OR "
            " (source_item_type='image' AND source_item_id='22') OR "
            " (source_item_type='measurement' AND source_item_id='32'))",
            (ARCHIVE_ID,),
        )
        connection.execute("DELETE FROM spore_measurements WHERE id=?", (first.measurement_id_map[32],))
        connection.execute("DELETE FROM images WHERE id=?", (first.image_id_map[22],))
        connection.execute("DELETE FROM observations WHERE id=?", (first.observation_id_map[12],))
        connection.commit()

    second = _import(source_main, source_reference, destination_main, destination_reference)

    assert second.observation_id_map[11] == first.observation_id_map[11]
    assert second.observation_id_map[12] not in {12, first.observation_id_map[12]}
    assert second.reused_item_counts["observation"] == 1
    assert second.new_item_counts["observation"] == 1
    assert second.new_item_counts["image"] == 1
    assert second.new_item_counts["measurement"] == 1
    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 2


def test_failure_during_provenance_write_rolls_back_rows_and_mappings(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    real_record = portable_import._record_provenance
    calls = 0

    def fail_after_first(*args, **kwargs):
        nonlocal calls
        calls += 1
        real_record(*args, **kwargs)
        if calls == 1:
            raise RuntimeError("injected provenance failure")

    monkeypatch.setattr(portable_import, "_record_provenance", fail_after_first)
    with pytest.raises(PortableImportError, match="injected provenance failure"):
        _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM portable_import_provenance").fetchone()[0] == 0
    with sqlite3.connect(destination_reference) as connection:
        assert connection.execute("SELECT COUNT(*) FROM reference_works").fetchone()[0] == 0

    monkeypatch.setattr(portable_import, "_record_provenance", real_record)
    retried = _import(source_main, source_reference, destination_main, destination_reference)
    assert retried.new_item_counts["observation"] == 1


def test_replay_preserves_cloud_neutralization_guard(monkeypatch, tmp_path):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)
    second = _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        observation = connection.execute(
            "SELECT cloud_id, sync_status, portable_cloud_identity_pending "
            "FROM observations WHERE id=?",
            (second.observation_id_map[11],),
        ).fetchone()
        image = connection.execute(
            "SELECT cloud_id, synced_at FROM images WHERE id=?",
            (second.image_id_map[21],),
        ).fetchone()
        measurement = connection.execute(
            "SELECT cloud_id FROM spore_measurements WHERE id=?",
            (second.measurement_id_map[31],),
        ).fetchone()
    assert observation == (None, "local", 1)
    assert image == (None, None)
    assert measurement == (None,)
    assert second.observation_id_map == first.observation_id_map


def test_replay_preserves_fresh_destination_cloud_identity_after_guard_clears(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "UPDATE observations SET cloud_id='new-observation-cloud', sync_status='synced', "
            "portable_cloud_identity_pending=0 WHERE id=?",
            (first.observation_id_map[11],),
        )
        connection.execute(
            "UPDATE images SET cloud_id='new-image-cloud', synced_at='2026-08-28' WHERE id=?",
            (first.image_id_map[21],),
        )
        connection.execute(
            "UPDATE spore_measurements SET cloud_id='new-measurement-cloud' WHERE id=?",
            (first.measurement_id_map[31],),
        )
        connection.commit()

    _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute(
            "SELECT cloud_id, sync_status, portable_cloud_identity_pending FROM observations "
            "WHERE id=?", (first.observation_id_map[11],),
        ).fetchone() == ("new-observation-cloud", "synced", 0)
        assert connection.execute(
            "SELECT cloud_id, synced_at FROM images WHERE id=?", (first.image_id_map[21],),
        ).fetchone() == ("new-image-cloud", "2026-08-28")
        assert connection.execute(
            "SELECT cloud_id FROM spore_measurements WHERE id=?",
            (first.measurement_id_map[31],),
        ).fetchone() == ("new-measurement-cloud",)


def test_replay_rejects_new_child_under_completed_root_before_cloud_guard_can_be_bypassed(
    monkeypatch, tmp_path
):
    source_main, source_reference = _initialize_database_pair(monkeypatch, tmp_path / "source")
    _insert_source_graph(source_main, source_reference)
    destination_main, destination_reference = _initialize_database_pair(
        monkeypatch, tmp_path / "destination"
    )
    first = _import(source_main, source_reference, destination_main, destination_reference)
    with sqlite3.connect(destination_main) as connection:
        connection.execute(
            "UPDATE observations SET portable_cloud_identity_pending=0, cloud_id='new-cloud' "
            "WHERE id=?",
            (first.observation_id_map[11],),
        )
        connection.commit()
    with sqlite3.connect(source_main) as connection:
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type) "
            "VALUES (22, 11, '/source/late.jpg', 'field')"
        )
        connection.commit()

    with pytest.raises(PortableImportError, match="conflicting source content inventory"):
        _import(source_main, source_reference, destination_main, destination_reference)

    with sqlite3.connect(destination_main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 1
        assert connection.execute(
            "SELECT portable_cloud_identity_pending, cloud_id FROM observations WHERE id=?",
            (first.observation_id_map[11],),
        ).fetchone() == (0, "new-cloud")


def test_database_initialization_creates_local_provenance_schema(monkeypatch, tmp_path):
    main, _reference = _initialize_database_pair(monkeypatch, tmp_path / "database")
    with sqlite3.connect(main) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(portable_import_provenance)")
        }
    assert columns == {
        "archive_id", "source_item_type", "source_item_id", "destination_item_id",
        "source_content_sha256", "imported_at",
    }
