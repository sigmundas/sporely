"""Stage 3D: snapshot version 2, version-aware comparison, reader acceptance,
transfer preserve-or-reject and the two rollout gates.

Contract: ``docs/reference-data/measurement-content-contract.md`` sections 6,
7, 8 and 13. The *Required regression matrix* v1/v2 items in
``docs/plans/active/2026-09-10-reported-statistics-and-range-semantics.md``
are covered here: missing-versus-empty equivalence, real-statistics
difference, frozen evidence unchanged, explicit refresh, unsupported-version
feed behaviour, transfer round trips and preserve-or-reject.
"""
from __future__ import annotations

import copy
import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema as _schema
from database.curated_reference_forks import (
    CuratedReferenceError,
    copy_curated_bundle_to_personal_library,
    normalize_curated_bundle,
)
from database.reference_citation import (
    SNAPSHOT_SCHEMA_VERSION,
    SNAPSHOT_SCHEMA_VERSION_ENHANCED,
    SUPPORTED_SNAPSHOT_VERSIONS,
    build_observation_reference_snapshot,
    observation_snapshots_semantically_equal,
    serialize_snapshot,
    snapshot_semantic_projection,
)
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceIntegrityError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import reference_library_has_enhanced_rows
from database.reference_use_sync_reconciliation import (
    stage_observation_reference_use_feed,
)
from database.reference_sync_reconciliation import ReferencePullReconciliationError
from references import measurement_content_gates as gates
from references.measurement_content import MeasurementContentError
from utils import db_share
from utils.archive.portable_import import PortableImportError, _validate_reference_snapshot

from tests.test_curated_reference_forks import bundle_row

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


ENHANCED = _load("row_enhanced.json")
LEGACY = _load("row_legacy_only.json")
HEBELOMA = _load("details_hebeloma_v1.json")
FUTURE = _load("details_unsupported_future_version.json")
SNAPSHOT_V1 = _load("snapshot_v1_legacy_only.json")
SNAPSHOT_V2 = _load("snapshot_v2_enhanced.json")

WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = ENHANCED["taxon_treatment_id"]
_ROW_FIELDS = {f.name for f in MeasurementSet.__dataclass_fields__.values()}


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db"
    )
    _schema.init_database()
    ReferenceWorkRepository.create(
        ReferenceWork(id=WORK_ID, type="book", title="Fixture", short_label="Fixture 2020")
    )
    TaxonTreatmentRepository.create(
        TaxonTreatment(
            id=TREATMENT_ID, reference_work_id=WORK_ID, name_as_published="Hebeloma fixtura"
        )
    )
    return db_path, ref_path


@pytest.fixture()
def bundle_env(libs, tmp_path, monkeypatch):
    """Isolate every path ``db_share`` resolves, so an export test never
    touches the real image directory or objective profiles."""
    db_path, ref_path = libs
    images = tmp_path / "images"
    images.mkdir(exist_ok=True)
    objectives = tmp_path / "objectives.json"
    monkeypatch.setattr(db_share, "get_database_path", lambda: db_path)
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(db_share, "get_images_dir", lambda: images)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: objectives)
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)
    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        db_share, "get_reference_connection", lambda: sqlite3.connect(ref_path)
    )
    return db_path, ref_path


@pytest.fixture()
def reader_gate_open(monkeypatch):
    monkeypatch.setattr(gates, "MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN", True)


@pytest.fixture()
def desktop_gate_open(monkeypatch):
    monkeypatch.setattr(gates, "MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN", True)


def _set(row: dict, **overrides) -> MeasurementSet:
    data = {k: v for k, v in row.items() if k in _ROW_FIELDS}
    data.update(overrides)
    return MeasurementSet(**data)


def _graph(row: dict):
    created = MeasurementSetRepository.create(_set(row))
    return (
        ReferenceWorkRepository.get(WORK_ID),
        TaxonTreatmentRepository.get(TREATMENT_ID),
        created,
    )


def _observation(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (species, date) VALUES (?, ?)",
            ("Hebeloma fixtura", "2026-09-14"),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


# --- Gates ship closed and are the only switch -------------------------------


def test_both_rollout_gates_ship_closed():
    assert gates.MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN is False
    assert gates.MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN is False
    assert gates.enhanced_attachments_enabled() is False
    assert gates.enhanced_bundle_export_enabled() is False


def test_gate_accessors_are_read_at_call_time(monkeypatch):
    """Activation is one reversible switch, not a value captured at import."""
    monkeypatch.setattr(gates, "MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN", True)
    assert gates.enhanced_attachments_enabled() is True
    monkeypatch.setattr(gates, "MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN", False)
    assert gates.enhanced_attachments_enabled() is False


# --- Emit rule ---------------------------------------------------------------


def test_legacy_row_still_builds_a_byte_identical_v1_snapshot(libs):
    work, treatment, created = _graph(LEGACY)
    snapshot = build_observation_reference_snapshot(work, treatment, created)
    assert snapshot["schema_version"] == SNAPSHOT_SCHEMA_VERSION == 1
    assert set(snapshot) == set(SNAPSHOT_V1)
    assert set(snapshot["measurements"]) == set(SNAPSHOT_V1["measurements"])
    assert "measurement_details" not in snapshot


def test_enhanced_row_builds_v2_with_details_outside_measurements(libs):
    work, treatment, created = _graph(ENHANCED)
    snapshot = build_observation_reference_snapshot(work, treatment, created)
    assert snapshot["schema_version"] == SNAPSHOT_SCHEMA_VERSION_ENHANCED == 2
    assert set(snapshot) == set(SNAPSHOT_V1) | {"measurement_details"}
    assert set(snapshot["measurements"]) == set(SNAPSHOT_V1["measurements"]) | {
        "q_core_min",
        "q_core_max",
    }
    # ``measurements`` stays numeric-or-null; the details object is top level.
    for value in snapshot["measurements"].values():
        assert value is None or isinstance(value, (int, float))
    assert snapshot["measurement_details"] == HEBELOMA


def test_q_core_pair_without_details_still_emits_v2(libs):
    """Contract section 2 rule 8: an untagged inner Q pair is enhanced too."""
    work, treatment, created = _graph(
        {**LEGACY, "q_core_min": 1.4, "q_core_max": 2.1, "measurement_details_json": None}
    )
    snapshot = build_observation_reference_snapshot(work, treatment, created)
    assert snapshot["schema_version"] == 2
    assert snapshot["measurement_details"] is None
    assert snapshot["measurements"]["q_core_min"] == 1.4


def test_future_details_version_is_embedded_unchanged(libs):
    """Such a row cannot be created locally (Stage 3B keeps it inspect-only);
    it arrives by import or pull, and its snapshot preserves it verbatim."""
    work, treatment, created = _graph(LEGACY)
    future_row = replace(created, measurement_details_json=json.dumps(FUTURE))
    snapshot = build_observation_reference_snapshot(work, treatment, future_row)
    assert snapshot["schema_version"] == 2
    assert snapshot["measurement_details"] == FUTURE, "never reinterpreted"


def test_oversized_details_refuse_to_become_a_snapshot(libs):
    padded = copy.deepcopy(FUTURE)
    padded["padding"] = "x" * 5000
    work, treatment, created = _graph(LEGACY)
    oversized = replace(created, measurement_details_json=json.dumps(padded))
    with pytest.raises(MeasurementContentError) as excinfo:
        build_observation_reference_snapshot(work, treatment, oversized)
    assert "limit" in str(excinfo.value)


def test_malformed_details_refuse_to_become_a_snapshot(libs):
    work, treatment, created = _graph(LEGACY)
    broken = replace(created, measurement_details_json="{not json")
    with pytest.raises(MeasurementContentError):
        build_observation_reference_snapshot(work, treatment, broken)


# --- Version-aware semantic projection ---------------------------------------


def test_missing_extension_equals_no_extension():
    v2_of_legacy = {
        **SNAPSHOT_V1,
        "schema_version": 2,
        "reference_revision": 41,  # revision churn is ignored
        "measurements": {
            **SNAPSHOT_V1["measurements"],
            "q_core_min": None,
            "q_core_max": None,
        },
        "measurement_details": None,
    }
    assert snapshot_semantic_projection(v2_of_legacy) == snapshot_semantic_projection(
        SNAPSHOT_V1
    )
    assert observation_snapshots_semantically_equal(
        serialize_snapshot(SNAPSHOT_V1), v2_of_legacy
    )


def test_missing_versus_real_statistics_differs():
    stripped = {
        **SNAPSHOT_V2,
        "measurement_details": None,
        "measurements": {
            **SNAPSHOT_V2["measurements"],
            "q_core_min": None,
            "q_core_max": None,
        },
    }
    assert snapshot_semantic_projection(SNAPSHOT_V2) != snapshot_semantic_projection(
        stripped
    )
    assert not observation_snapshots_semantically_equal(
        serialize_snapshot(stripped), SNAPSHOT_V2
    )


def test_unsupported_version_is_never_projected_as_v1():
    future = {**SNAPSHOT_V2, "schema_version": 3}
    assert snapshot_semantic_projection(future) is None
    # Not even equal to itself: a reader holds or rejects, never guesses.
    assert not observation_snapshots_semantically_equal(
        serialize_snapshot(future), future
    )
    assert not observation_snapshots_semantically_equal(
        serialize_snapshot(SNAPSHOT_V1), future
    )
    assert SUPPORTED_SNAPSHOT_VERSIONS == frozenset({1, 2})


def test_invalid_stored_json_is_never_equivalent():
    assert not observation_snapshots_semantically_equal("{not json", SNAPSHOT_V1)
    assert not observation_snapshots_semantically_equal("null", SNAPSHOT_V1)


# --- Frozen evidence and the reader gate -------------------------------------


def test_attaching_an_enhanced_row_is_refused_while_the_gate_is_closed(libs):
    db_path, _ref = libs
    _work, _treatment, created = _graph(ENHANCED)
    observation_id = _observation(db_path)
    with pytest.raises(ReferenceIntegrityError) as excinfo:
        ObservationReferenceUseRepository.attach(
            observation_id=observation_id,
            reference_measurement_set_id=created.id,
            role="compared",
        )
    assert "reported statistics" in str(excinfo.value)


def test_attaching_an_enhanced_row_freezes_v2_once_the_gate_is_open(
    libs, reader_gate_open
):
    db_path, _ref = libs
    _work, _treatment, created = _graph(ENHANCED)
    observation_id = _observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        observation_id=observation_id,
        reference_measurement_set_id=created.id,
        role="compared",
    )
    stored = json.loads(use.snapshot_json)
    assert stored["schema_version"] == 2
    assert stored["measurement_details"] == HEBELOMA
    assert (
        ObservationReferenceUseRepository.snapshot_status(use.id).state == "current"
    )


def test_a_legacy_attachment_does_not_go_stale_on_upgrade(libs):
    """Frozen evidence unchanged: nothing rewrites or enriches history."""
    db_path, _ref = libs
    _work, _treatment, created = _graph(LEGACY)
    observation_id = _observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        observation_id=observation_id,
        reference_measurement_set_id=created.id,
        role="compared",
    )
    assert json.loads(use.snapshot_json)["schema_version"] == 1
    assert ObservationReferenceUseRepository.snapshot_status(use.id).state == "current"
    refreshed, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)
    assert changed is False
    assert refreshed.snapshot_json == use.snapshot_json


def test_refreshing_onto_enhanced_content_is_refused_while_the_gate_is_closed(libs):
    db_path, _ref = libs
    _work, _treatment, created = _graph(LEGACY)
    observation_id = _observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        observation_id=observation_id,
        reference_measurement_set_id=created.id,
        role="compared",
    )
    MeasurementSetRepository.update(
        created.id,
        {
            "q_core_min": 1.4,
            "q_core_max": 2.1,
            "measurement_details_json": json.dumps(
                {"schema_version": 1, "metrics": {"q": {"core_range": {"kind": "unspecified"}}}}
            ),
        },
    )
    with pytest.raises(ReferenceIntegrityError):
        ObservationReferenceUseRepository.refresh_snapshot(use.id)
    # The stored evidence is untouched by the refusal.
    assert (
        ObservationReferenceUseRepository.get(use.id).snapshot_json == use.snapshot_json
    )


def test_explicit_refresh_replaces_v1_with_v2_once_the_gate_is_open(
    libs, reader_gate_open
):
    db_path, _ref = libs
    _work, _treatment, created = _graph(LEGACY)
    observation_id = _observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        observation_id=observation_id,
        reference_measurement_set_id=created.id,
        role="compared",
    )
    assert json.loads(use.snapshot_json)["schema_version"] == 1
    MeasurementSetRepository.update(
        created.id,
        {
            "q_core_min": 1.4,
            "q_core_max": 2.1,
            "measurement_details_json": json.dumps(
                {"schema_version": 1, "metrics": {"q": {"core_range": {"kind": "unspecified"}}}}
            ),
        },
    )
    assert (
        ObservationReferenceUseRepository.snapshot_status(use.id).state
        == "update_available"
    ), "real statistics are a semantic difference"
    refreshed, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)
    assert changed is True
    assert json.loads(refreshed.snapshot_json)["schema_version"] == 2


def test_an_enhanced_successor_is_reviewable_but_not_adoptable_while_gated(libs):
    db_path, _ref = libs
    _work, _treatment, created = _graph(LEGACY)
    observation_id = _observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        observation_id=observation_id,
        reference_measurement_set_id=created.id,
        role="compared",
    )
    MeasurementSetRepository.create_revision(
        created.id,
        {
            "q_core_min": 1.4,
            "q_core_max": 2.1,
            "measurement_details_json": json.dumps(
                {"schema_version": 1, "metrics": {"q": {"core_range": {"kind": "unspecified"}}}}
            ),
        },
    )
    assert (
        ObservationReferenceUseRepository.successor_status(use.id).state == "unsupported"
    )
    with pytest.raises(ReferenceIntegrityError):
        ObservationReferenceUseRepository.adopt_successor(use.id)


# --- Readers first: the desktop use feed -------------------------------------


def _feed_row(snapshot: dict, *, use_id: str = "aa000000-0000-4000-8000-000000000001"):
    return {
        "user_id": "user-1",
        "id": use_id,
        "observation_id": 4242,
        "reference_measurement_set_id": snapshot["reference_measurement_set_id"],
        "role": "compared",
        "note": None,
        "selected_at": "2026-09-14T10:00:00Z",
        "reference_revision": snapshot["reference_revision"],
        "snapshot_json": json.dumps(snapshot),
        "created_at": "2026-09-14T10:00:00Z",
        "updated_at": "2026-09-14T10:00:00Z",
        "deleted_at": None,
        "row_version": 1,
    }


@pytest.mark.parametrize("snapshot", [SNAPSHOT_V1, SNAPSHOT_V2])
def test_use_feed_accepts_v1_and_v2(snapshot):
    staged = stage_observation_reference_use_feed("user-1", (_feed_row(snapshot),))
    assert len(staged.rows) == 1


def test_use_feed_still_rejects_an_unsupported_snapshot_version():
    future = {**SNAPSHOT_V2, "schema_version": 3}
    with pytest.raises(ReferencePullReconciliationError) as excinfo:
        stage_observation_reference_use_feed("user-1", (_feed_row(future),))
    assert "unsupported snapshot schema" in str(excinfo.value)


# --- Portable transfer: preserve or reject, never intersect ------------------


def test_portable_snapshot_validator_accepts_v2_and_rejects_unknown_versions(tmp_path):
    reference = sqlite3.connect(tmp_path / "reference_values.db")
    reference.row_factory = sqlite3.Row
    try:
        reference.executescript(
            """
            CREATE TABLE reference_works (id TEXT PRIMARY KEY);
            CREATE TABLE reference_taxon_treatments (
                id TEXT PRIMARY KEY, reference_work_id TEXT);
            CREATE TABLE reference_measurement_sets (
                id TEXT PRIMARY KEY, taxon_treatment_id TEXT);
            """
        )
        reference.execute(
            "INSERT INTO reference_works (id) VALUES (?)",
            (SNAPSHOT_V2["reference_work_id"],),
        )
        reference.execute(
            "INSERT INTO reference_taxon_treatments (id, reference_work_id) VALUES (?,?)",
            (SNAPSHOT_V2["reference_treatment_id"], SNAPSHOT_V2["reference_work_id"]),
        )
        reference.execute(
            "INSERT INTO reference_measurement_sets (id, taxon_treatment_id) VALUES (?,?)",
            (
                SNAPSHOT_V2["reference_measurement_set_id"],
                SNAPSHOT_V2["reference_treatment_id"],
            ),
        )
        reference.commit()
        row = {
            "id": "use-1",
            "reference_measurement_set_id": SNAPSHOT_V2["reference_measurement_set_id"],
            "reference_revision": SNAPSHOT_V2["reference_revision"],
            "snapshot_json": json.dumps(SNAPSHOT_V2),
        }
        _validate_reference_snapshot(row, reference, destination_schema="main")

        future = {**SNAPSHOT_V2, "schema_version": 3}
        with pytest.raises(PortableImportError):
            _validate_reference_snapshot(
                {**row, "snapshot_json": json.dumps(future)},
                reference,
                destination_schema="main",
            )
        # A v2 snapshot missing its new key is not silently read as v1.
        truncated = {k: v for k, v in SNAPSHOT_V2.items() if k != "measurement_details"}
        with pytest.raises(PortableImportError):
            _validate_reference_snapshot(
                {**row, "snapshot_json": json.dumps(truncated)},
                reference,
                destination_schema="main",
            )
    finally:
        reference.close()


# --- Curated copy: preserve or reject ----------------------------------------


def _curated_v2(details: dict | None = HEBELOMA) -> dict:
    row = copy.deepcopy(bundle_row())
    snapshot = row["snapshot"]
    snapshot["schema_version"] = 2
    snapshot["measurements"]["q_core_min"] = 1.4
    snapshot["measurements"]["q_core_max"] = 2.1
    snapshot["measurements"]["q_min"] = 1.2
    snapshot["measurements"]["q_max"] = 2.5
    snapshot["measurement_details"] = details
    return row


def test_curated_copy_of_a_v1_bundle_is_a_legacy_only_row(libs):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    stored = MeasurementSetRepository.get(fork.reference_measurement_set_id)
    assert stored.measurement_details_json is None
    assert stored.q_core_min is None and stored.q_core_max is None
    assert stored.is_enhanced is False


def test_curated_copy_of_a_v2_bundle_preserves_all_three_extension_fields(libs):
    details = {
        "schema_version": 1,
        "metrics": {"q": {"core_range": {"kind": "percentile_interval",
                                         "percentile_bounds": [5, 95]}}},
    }
    fork = copy_curated_bundle_to_personal_library(
        normalize_curated_bundle(_curated_v2(details))
    )
    stored = MeasurementSetRepository.get(fork.reference_measurement_set_id)
    assert stored.q_core_min == 1.4 and stored.q_core_max == 2.1
    assert json.loads(stored.measurement_details_json) == details
    assert stored.is_enhanced is True


def test_curated_copy_rejects_an_unsupported_details_version(libs):
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(_curated_v2(FUTURE))


def test_curated_copy_rejects_an_unsupported_snapshot_version(libs):
    row = _curated_v2()
    row["snapshot"]["schema_version"] = 3
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row)


def test_curated_copy_rejects_details_inconsistent_with_the_columns(libs):
    """A core-range tag needs its complete pair; no degraded row is stored."""
    row = _curated_v2(
        {"schema_version": 1, "metrics": {"length": {"mean_interval": {
            "lower": 8.0, "upper": 9.0, "kind": "reported_range"}}}}
    )
    row["snapshot"]["measurements"]["length_mean"] = 8.5
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row)


def test_curated_v2_snapshot_missing_the_new_measurement_keys_is_rejected(libs):
    row = _curated_v2()
    del row["snapshot"]["measurements"]["q_core_min"]
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row)


# --- Enhanced bundle export gate ---------------------------------------------


def test_enhanced_row_detection_is_read_only_and_column_tolerant(libs, tmp_path):
    _db_path, ref_path = libs
    connection = sqlite3.connect(ref_path)
    try:
        assert reference_library_has_enhanced_rows(connection) is False
    finally:
        connection.close()
    _graph(ENHANCED)
    connection = sqlite3.connect(ref_path)
    try:
        assert reference_library_has_enhanced_rows(connection) is True
    finally:
        connection.close()

    pre_extension = sqlite3.connect(tmp_path / "old.db")
    try:
        pre_extension.execute(
            "CREATE TABLE reference_measurement_sets (id TEXT PRIMARY KEY)"
        )
        assert reference_library_has_enhanced_rows(pre_extension) is False
    finally:
        pre_extension.close()


def test_bundle_export_is_refused_while_the_desktop_gate_is_closed(bundle_env, tmp_path):
    _graph(ENHANCED)
    destination = tmp_path / "bundle.zip"
    with pytest.raises(db_share.EnhancedBundleExportBlocked):
        db_share.export_database_bundle(str(destination))
    assert not destination.exists(), "a blocked export leaves nothing behind"


def test_bundle_export_of_a_legacy_library_is_unaffected(bundle_env, tmp_path):
    _graph(LEGACY)
    destination = tmp_path / "bundle.zip"
    db_share.export_database_bundle(str(destination))
    assert destination.exists()


def test_bundle_export_of_enhanced_content_succeeds_once_the_gate_is_open(
    bundle_env, tmp_path, desktop_gate_open
):
    _graph(ENHANCED)
    destination = tmp_path / "bundle.zip"
    db_share.export_database_bundle(str(destination))
    with ZipFile(destination) as archive:
        assert "reference_values.db" in archive.namelist()
