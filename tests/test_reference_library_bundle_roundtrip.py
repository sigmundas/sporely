"""Round-trip bundle export/import for the normalized library and
observation reference uses."""
from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from utils import db_share
from tests.test_curated_reference_forks import bundle_row


def _isolate(paths, monkeypatch):
    monkeypatch.setattr(_schema, "get_database_path", lambda: paths["db"])
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: paths["ref"])
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: paths["bundled"],
    )
    monkeypatch.setattr(db_share, "get_database_path", lambda: paths["db"])
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: paths["ref"])
    monkeypatch.setattr(db_share, "get_images_dir", lambda: paths["images"])
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: paths["objectives"])
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)
    monkeypatch.setattr(
        db_share, "get_connection", lambda: sqlite3.connect(paths["db"])
    )
    monkeypatch.setattr(
        db_share,
        "get_reference_connection",
        lambda: sqlite3.connect(paths["ref"]),
    )


def _make_paths(root: Path, suffix: str) -> dict:
    db = root / f"mushrooms-{suffix}.db"
    ref = root / f"reference-{suffix}.db"
    bundled = root / f"bundled-{suffix}.db"
    images = root / f"images-{suffix}"
    images.mkdir(parents=True, exist_ok=True)
    objectives = root / f"objectives-{suffix}.json"
    return {
        "db": db,
        "ref": ref,
        "bundled": bundled,
        "images": images,
        "objectives": objectives,
    }


def _seed_source(monkeypatch, tmp_path):
    paths = _make_paths(tmp_path, "source")
    _isolate(paths, monkeypatch)
    _schema.init_database()
    # One observation to attach references to.
    conn = sqlite3.connect(paths["db"])
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-05-01", "Bundle roundtrip"),
        )
        obs_id = cursor.lastrowid
        conn.commit()
    finally:
        conn.close()

    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="work-round",
            type="book",
            title="Nordic Macromycetes",
            short_label="Hansen & Knudsen 1992",
            authors_json=json.dumps(
                [{"family": "Hansen"}, {"family": "Knudsen"}]
            ),
            year=1992,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="treat-round",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
            locator_text="p. 214",
        )
    )
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="set-round",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_min=7.5,
            length_max=10.5,
        )
    )
    use = ObservationReferenceUseRepository.attach(obs_id, ms.id, role="compared")
    return paths, obs_id, work, treatment, ms, use


def test_bundle_roundtrip_preserves_library_and_uses(tmp_path, monkeypatch):
    source_paths, obs_id, work, treatment, ms, use = _seed_source(
        monkeypatch, tmp_path
    )
    envelope = json.dumps(bundle_row(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    with sqlite3.connect(source_paths["ref"]) as connection:
        connection.execute(
            "INSERT INTO curated_reference_forks "
            "(curated_measurement_set_id,bundle_revision,sporely_taxon_id,reference_work_id,"
            "taxon_treatment_id,reference_measurement_set_id,source_envelope_json,source_sha256) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (bundle_row()["curated_measurement_set_id"], 2, 2_100_000_081,
             work.id, treatment.id, ms.id, envelope,
             hashlib.sha256(envelope.encode("utf-8")).hexdigest()),
        )
    bundle_path = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    # Fresh destination.
    dest_paths = _make_paths(tmp_path, "dest")
    _isolate(dest_paths, monkeypatch)
    _schema.init_database()

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    assert result["observations"] == 1
    assert result["reference_works"] == 1
    assert result["reference_taxon_treatments"] == 1
    assert result["reference_measurement_sets"] == 1
    assert result["observation_reference_uses"] == 1
    assert result["unresolved_observation_reference_uses"] == []

    # Verify the round-tripped data.
    conn_ref = sqlite3.connect(dest_paths["ref"])
    try:
        conn_ref.row_factory = sqlite3.Row
        work_row = conn_ref.execute(
            "SELECT * FROM reference_works WHERE id = ?", (work.id,)
        ).fetchone()
        assert work_row is not None
        assert work_row["title"] == work.title
        treatment_row = conn_ref.execute(
            "SELECT * FROM reference_taxon_treatments WHERE id = ?",
            (treatment.id,),
        ).fetchone()
        assert treatment_row["name_as_published"] == "Russula paludosa"
        ms_row = conn_ref.execute(
            "SELECT * FROM reference_measurement_sets WHERE id = ?", (ms.id,)
        ).fetchone()
        assert ms_row["length_min"] == 7.5
        assert conn_ref.execute("SELECT COUNT(*) FROM curated_reference_forks").fetchone()[0] == 1
    finally:
        conn_ref.close()

    conn_main = sqlite3.connect(dest_paths["db"])
    try:
        conn_main.row_factory = sqlite3.Row
        use_row = conn_main.execute(
            "SELECT * FROM observation_reference_uses"
        ).fetchone()
        assert use_row["id"] == use.id
        # observation_id was remapped to the new local id, which is 1.
        new_obs_id = conn_main.execute(
            "SELECT id FROM observations ORDER BY id"
        ).fetchone()[0]
        assert use_row["observation_id"] == new_obs_id
        snap = json.loads(use_row["snapshot_json"])
        assert snap["reference_measurement_set_id"] == ms.id
    finally:
        conn_main.close()


def test_bundle_export_excludes_reference_cloud_transport_state(
    tmp_path, monkeypatch
):
    source_paths, _obs_id, work, _treatment, _ms, _use = _seed_source(
        monkeypatch, tmp_path
    )
    with sqlite3.connect(source_paths["ref"]) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state "
            "SET cloud_user_id='BUNDLE_SYNC_OWNER_SENTINEL', "
            "remote_identity_state='create_outcome_unknown' "
            "WHERE entity_type='work' AND entity_id=?",
            (work.id,),
        )
        connection.execute(
            "INSERT INTO reference_cloud_tombstones "
            "(entity_type, entity_id, cloud_user_id, remote_identity_state) "
            "VALUES ('work', 'deleted-work', "
            "'BUNDLE_TOMBSTONE_SENTINEL', 'create_outcome_unknown')"
        )
        connection.commit()

    bundle_path = tmp_path / "bundle-with-sync-state.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    archived_reference = tmp_path / "archived-reference.db"
    with ZipFile(bundle_path) as archive:
        archived_reference.write_bytes(archive.read("reference_values.db"))
    with sqlite3.connect(archived_reference) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM reference_works"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM reference_cloud_sync_state"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM reference_cloud_tombstones"
        ).fetchone()[0] == 0
    assert b"BUNDLE_SYNC_OWNER_SENTINEL" not in bundle_path.read_bytes()
    assert b"BUNDLE_TOMBSTONE_SENTINEL" not in bundle_path.read_bytes()


def test_bundle_export_accepts_pre_stage4b_reference_database(
    tmp_path, monkeypatch
):
    source_paths, *_ = _seed_source(monkeypatch, tmp_path)
    with sqlite3.connect(source_paths["ref"]) as connection:
        for (trigger_name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'reference_%_cloud_sync_%'"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{trigger_name}"')
        connection.execute("DROP TABLE reference_cloud_tombstones")
        connection.execute("DROP TABLE reference_cloud_sync_state")
        connection.commit()

    bundle_path = tmp_path / "legacy-reference-bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    with ZipFile(bundle_path) as archive:
        assert "reference_values.db" in archive.namelist()


def test_bundle_roundtrip_is_idempotent(tmp_path, monkeypatch):
    _, _, _, _, _, _ = _seed_source(monkeypatch, tmp_path)
    bundle_path = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    dest_paths = _make_paths(tmp_path, "dest-idempotent")
    _isolate(dest_paths, monkeypatch)
    _schema.init_database()

    first = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )
    # Second import of the SAME bundle should insert no new library rows.
    second = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )
    for table in (
        "reference_works",
        "reference_taxon_treatments",
        "reference_measurement_sets",
    ):
        assert first[table] == 1
        assert second[table] == 0
    assert first["observation_reference_uses"] == 1
    assert second["observation_reference_uses"] == 0

    # Library totals remain single-copy.
    conn_ref = sqlite3.connect(dest_paths["ref"])
    try:
        counts = {
            table: conn_ref.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in (
                "reference_works",
                "reference_taxon_treatments",
                "reference_measurement_sets",
            )
        }
    finally:
        conn_ref.close()
    assert counts == {
        "reference_works": 1,
        "reference_taxon_treatments": 1,
        "reference_measurement_sets": 1,
    }


def test_bundle_roundtrip_is_revision_aware(tmp_path, monkeypatch):
    source_paths, _, work, _, _, _ = _seed_source(monkeypatch, tmp_path)
    # Bump the source work's revision by editing it.
    ReferenceWorkRepository.update(work.id, {"title": "Nordic Macromycetes (2nd ed.)"})
    bundle_path = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )

    dest_paths = _make_paths(tmp_path, "dest-rev")
    _isolate(dest_paths, monkeypatch)
    _schema.init_database()

    # Pre-populate destination with an OLDER revision of the same work.
    conn = sqlite3.connect(dest_paths["ref"])
    try:
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label, revision, verification_status, visibility) "
            "VALUES (?, 'book', ?, ?, 1, 'incomplete', 'private')",
            (work.id, "Legacy title", "Legacy"),
        )
        conn.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id='user-1', "
            "remote_identity_state='acknowledged', cloud_row_version=1, "
            "accepted_payload_json='{}', sync_status='clean' "
            "WHERE entity_type='work' AND entity_id=?",
            (work.id,),
        )
        conn.commit()
    finally:
        conn.close()

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=True,
    )
    # Imported=0 (already exists) but updated=1 (higher revision).
    assert result["reference_works"] == 0
    assert result["reference_library_updates"]["reference_works"] == 1

    conn = sqlite3.connect(dest_paths["ref"])
    try:
        row = conn.execute(
            "SELECT title, revision FROM reference_works WHERE id = ?",
            (work.id,),
        ).fetchone()
        sync_status = conn.execute(
            "SELECT sync_status FROM reference_cloud_sync_state "
            "WHERE entity_type='work' AND entity_id=?",
            (work.id,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert row[0] == "Nordic Macromycetes (2nd ed.)"
    assert row[1] >= 2
    assert sync_status == "dirty"


def test_bundle_import_reports_unresolved_attachments(tmp_path, monkeypatch):
    """An observation reference use whose measurement set is missing
    from the destination library must be PRESERVED (snapshot survives)
    and REPORTED, never silently dropped."""
    source_paths, _obs_id, _work, _treatment, ms, _use = _seed_source(
        monkeypatch, tmp_path
    )
    bundle_path = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        # Deliberately do NOT include reference values — the library
        # side of the bundle is omitted while the observation-side
        # attachment survives.
        include_reference_values=False,
    )

    dest_paths = _make_paths(tmp_path, "dest-unresolved")
    _isolate(dest_paths, monkeypatch)
    _schema.init_database()

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=True,
        include_images=False,
        include_measurements=False,
        include_calibrations=False,
        include_reference_values=False,
    )
    assert result["observation_reference_uses"] == 1
    assert result["unresolved_observation_reference_uses"] == [ms.id]
    assert any(
        "unresolved" in warning.lower() or "missing library" in warning.lower()
        for warning in result["warnings"]
    )
    # The row itself is retained, snapshot intact.
    conn = sqlite3.connect(dest_paths["db"])
    try:
        row = conn.execute(
            "SELECT snapshot_json FROM observation_reference_uses"
        ).fetchone()
    finally:
        conn.close()
    snap = json.loads(row[0])
    assert snap["reference_measurement_set_id"] == ms.id
