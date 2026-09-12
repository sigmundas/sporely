"""Stage 3B: local persistence of typed measurement content.

Exercises the production owners that read and write the Stage 3A extension
columns (``measurement_details_json``, ``q_core_min``, ``q_core_max``) of
``reference_measurement_sets`` through the Stage 2 contract:

* ``MeasurementSetRepository`` create / get / update / create_revision;
* bundle import (``utils.db_share._upsert_library_row_by_revision`` and
  ``import_database_bundle``);
* portable import (``utils.archive.portable_import._merge_reference_entity``);
* pull reconciliation (``database.reference_sync_reconciliation``) as a
  fail-closed guard until Stage 3C transports the extension.

Everything runs against temporary SQLite libraries initialized by production
code, so the Stage 3A write barrier is live in every test.
"""
from __future__ import annotations

import json
import sqlite3
import zipfile
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from database import reference_library as reference_library_module
from database import reference_library_schema as lib_schema
from database import schema as _schema
from database.reference_citation import build_observation_reference_snapshot
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceValidationError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_sync_state import ReferenceCloudSyncStateRepository
from references.measurement_content import (
    EXTENSION_FIELDS,
    IntervalStatistic,
    MeasurementContentError,
    ScalarStatistic,
    UnsupportedMeasurementDetails,
    clear_statistic,
    content_row_updates,
    decode_measurement_details,
    encode_measurement_details,
    measurement_details_equal,
    set_scalar_mean,
    swap_length_width,
)
from utils import db_share
from utils.archive.portable_import import (
    PortableIdentityConflictError,
    PortableImportError,
    _merge_reference_entity,
)
from utils.db_share import _upsert_library_row_by_revision
from utils.reference_cloud_sync import pull_reference_library

from tests.test_reference_library_pull_reconciliation import (
    PullClient,
    _seed_acknowledged_graph,
    _set_row,
    _treatment_row,
    _work_row,
)

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


ENHANCED = _load("row_enhanced.json")
LEGACY = _load("row_legacy_only.json")
HEBELOMA = _load("details_hebeloma_v1.json")
FUTURE = _load("details_unsupported_future_version.json")
FUTURE_TEXT = json.dumps(FUTURE, indent=2)  # deliberately non-canonical bytes
WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = ENHANCED["taxon_treatment_id"]

_ROW_FIELDS = {f.name for f in MeasurementSet.__dataclass_fields__.values()}


# --- Fixtures and helpers -----------------------------------------------------


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


def _set(row: dict, **overrides) -> MeasurementSet:
    data = {k: v for k, v in row.items() if k in _ROW_FIELDS}
    data.update(overrides)
    return MeasurementSet(**data)


def _details(metrics: dict) -> str:
    return json.dumps({"schema_version": 1, "metrics": metrics})


def _raw_row(ref_path: Path, set_id: str) -> dict:
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM reference_measurement_sets WHERE id=?", (set_id,)
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def _raw_insert(ref_path: Path, row: dict) -> None:
    """Seed a row through an aware raw connection (the contract's 'tests and
    tools writing rows directly' case), bypassing repository validation."""
    conn = sqlite3.connect(ref_path)
    lib_schema.register_measurement_contract(conn)
    columns = [k for k in row if k in _ROW_FIELDS and k not in {"created_at", "updated_at"}]
    conn.execute(
        f"INSERT INTO reference_measurement_sets ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        [row[k] for k in columns],
    )
    conn.commit()
    conn.close()


def _unaware(ref_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _aware(ref_path: Path) -> sqlite3.Connection:
    conn = _unaware(ref_path)
    lib_schema.register_measurement_contract(conn)
    return conn


def _metric(ms: MeasurementSet, metric: str):
    return ms.measurement_content().details.metrics[metric]


# --- 1–3: round trips ---------------------------------------------------------


def test_legacy_row_round_trips_unchanged_and_is_not_enhanced(libs):
    created = MeasurementSetRepository.create(_set(LEGACY))
    fetched = MeasurementSetRepository.get(created.id)
    for name, value in LEGACY.items():
        if name in _ROW_FIELDS and name not in {"created_at", "updated_at"}:
            assert getattr(fetched, name) == value, name
    assert not fetched.is_enhanced
    content = fetched.measurement_content()
    assert content.details is None
    assert content.q_core_min is None and content.q_core_max is None
    assert content.q_min == 1.7 and content.q_max == 2.5, "unknown-role Q pair untouched"
    raw = _raw_row(libs[1], created.id)
    assert all(raw[name] is None for name in EXTENSION_FIELDS)


def test_full_v1_enhanced_row_round_trips(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    fetched = MeasurementSetRepository.get(created.id)
    assert fetched.is_enhanced
    assert fetched.measurement_details_json == ENHANCED["measurement_details_json"]
    assert fetched.q_core_min == 1.36 and fetched.q_core_max == 2.19
    assert fetched.q_min == 1.17 and fetched.q_max == 2.79
    content = fetched.measurement_content()
    assert content.details == decode_measurement_details(ENHANCED["measurement_details_json"])
    length = content.details.metrics["length"]
    assert length.core_range.kind == "percentile_interval"
    assert length.core_range.percentile_bounds == (5, 95)
    assert length.mean_interval == IntervalStatistic(8.9, 13.7, "reported_range")
    assert length.median == IntervalStatistic(9.0, 13.9, "reported_range")
    assert length.sd == ScalarStatistic(0.696)
    assert fetched.length_mean is None and fetched.width_mean is None and fetched.q_mean is None


def test_non_canonical_details_text_is_stored_canonically(libs):
    loose = json.dumps(HEBELOMA, indent=3, sort_keys=False)
    assert loose != ENHANCED["measurement_details_json"]
    created = MeasurementSetRepository.create(_set(ENHANCED, measurement_details_json=loose))
    stored = _raw_row(libs[1], created.id)["measurement_details_json"]
    assert stored == ENHANCED["measurement_details_json"], "single contract codec"
    assert measurement_details_equal(stored, loose)


# --- 4–9: statistic shapes survive persistence exactly ------------------------


def test_scalar_mean_is_preserved_as_a_scalar(libs):
    ms = _set(
        LEGACY,
        id="",
        length_mean=11.0,
        measurement_details_json=_details({"length": {"sd": {"value": 0.5}}}),
    )
    created = MeasurementSetRepository.create(ms)
    fetched = MeasurementSetRepository.get(created.id)
    assert fetched.length_mean == 11.0
    assert _metric(fetched, "length").mean_interval is None
    assert _metric(fetched, "length").sd == ScalarStatistic(0.5)


def test_mean_interval_is_preserved_as_an_interval_and_excludes_scalar(libs):
    details = _details(
        {"length": {"mean_interval": {"lower": 9.5, "upper": 11.5, "kind": "typical_range"}}}
    )
    created = MeasurementSetRepository.create(_set(LEGACY, id="", measurement_details_json=details))
    fetched = MeasurementSetRepository.get(created.id)
    assert fetched.length_mean is None
    assert _metric(fetched, "length").mean_interval == IntervalStatistic(9.5, 11.5, "typical_range")
    with pytest.raises(ReferenceValidationError, match="length_mean must be NULL"):
        MeasurementSetRepository.update(created.id, {"length_mean": 10.5})


def test_equal_endpoint_mean_interval_stays_an_interval(libs):
    details = _details(
        {"width": {"mean_interval": {"lower": 5.0, "upper": 5.0, "kind": "reported_range"}}}
    )
    created = MeasurementSetRepository.create(_set(LEGACY, id="", measurement_details_json=details))
    fetched = MeasurementSetRepository.get(created.id)
    interval = _metric(fetched, "width").mean_interval
    assert isinstance(interval, IntervalStatistic)
    assert interval.lower == interval.upper == 5.0
    assert fetched.width_mean is None, "never collapsed into the scalar column"
    assert json.loads(fetched.measurement_details_json)["metrics"]["width"]["mean_interval"] == {
        "lower": 5.0, "upper": 5.0, "kind": "reported_range"
    }


def test_median_scalar_and_median_interval_are_distinct_after_round_trip(libs):
    scalar = MeasurementSetRepository.create(
        _set(LEGACY, id="", measurement_details_json=_details({"length": {"median": {"value": 10.2}}}))
    )
    interval = MeasurementSetRepository.create(
        _set(
            LEGACY, id="",
            measurement_details_json=_details(
                {"length": {"median": {"lower": 10.2, "upper": 10.2, "kind": "reported_range"}}}
            ),
        )
    )
    scalar_median = _metric(MeasurementSetRepository.get(scalar.id), "length").median
    interval_median = _metric(MeasurementSetRepository.get(interval.id), "length").median
    assert isinstance(scalar_median, ScalarStatistic) and scalar_median.value == 10.2
    assert isinstance(interval_median, IntervalStatistic)
    assert interval_median.lower == interval_median.upper == 10.2
    assert scalar_median != interval_median


def test_sd_is_preserved_and_validated(libs):
    created = MeasurementSetRepository.create(
        _set(LEGACY, id="", measurement_details_json=_details({"q": {"sd": {"value": 0.0}}}))
    )
    assert _metric(MeasurementSetRepository.get(created.id), "q").sd == ScalarStatistic(0.0)
    with pytest.raises(ReferenceValidationError, match="non-negative"):
        MeasurementSetRepository.update(
            created.id, {"measurement_details_json": _details({"q": {"sd": {"value": -0.1}}})}
        )
    with pytest.raises(ReferenceValidationError, match="finite number"):
        MeasurementSetRepository.update(
            created.id, {"measurement_details_json": _details({"q": {"sd": {"value": True}}})}
        )


def test_percentile_core_interpretation_is_preserved(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    fetched = MeasurementSetRepository.get(created.id)
    for metric in ("length", "width", "q"):
        core = _metric(fetched, metric).core_range
        assert core.kind == "percentile_interval" and core.percentile_bounds == (5, 95)
        assert _metric(fetched, metric).outer_range.kind == "reported_extremes"
    with pytest.raises(ReferenceValidationError, match="percentile_bounds"):
        bad = json.loads(json.dumps(HEBELOMA))
        bad["metrics"]["length"]["core_range"]["percentile_bounds"] = [95, 5]
        MeasurementSetRepository.update(created.id, {"measurement_details_json": json.dumps(bad)})


# --- 10–11: Q core versus ordinary Q, no cross-metric contamination -----------


def test_q_core_pair_is_independent_from_the_ordinary_q_range(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    updated = MeasurementSetRepository.update(created.id, {"q_min": 1.10, "q_max": 2.90})
    assert (updated.q_min, updated.q_max) == (1.10, 2.90)
    assert (updated.q_core_min, updated.q_core_max) == (1.36, 2.19)
    # An untagged inner Q pair without any details object is legal (rule 8).
    untagged = MeasurementSetRepository.create(
        _set(LEGACY, id="", q_core_min=1.9, q_core_max=2.3)
    )
    fetched = MeasurementSetRepository.get(untagged.id)
    assert fetched.is_enhanced and fetched.measurement_details_json is None
    assert (fetched.q_core_min, fetched.q_core_max) == (1.9, 2.3)
    assert (fetched.q_min, fetched.q_max) == (1.7, 2.5)
    with pytest.raises(ReferenceValidationError, match="q_core_min must not exceed q_core_max"):
        MeasurementSetRepository.update(untagged.id, {"q_core_min": 2.4})


def test_enhanced_q_content_does_not_contaminate_width(libs):
    details = _details(
        {"q": {"mean_interval": {"lower": 1.5, "upper": 1.9, "kind": "reported_range"}, "sd": {"value": 0.1}}}
    )
    created = MeasurementSetRepository.create(
        _set(LEGACY, id="", q_core_min=1.36, q_core_max=2.19, measurement_details_json=details)
    )
    fetched = MeasurementSetRepository.get(created.id)
    content = fetched.measurement_content()
    assert set(content.details.metrics) == {"q"}
    assert (fetched.width_core_min, fetched.width_core_max) == (4.5, 5.5)
    assert fetched.width_mean is None and fetched.width_min is None and fetched.width_max is None
    assert fetched.q_mean is None


# --- 12–15: semantic-empty, rejection, atomicity, future versions -------------


@pytest.mark.parametrize(
    "text",
    [
        '{"schema_version": 1, "metrics": {}}',
        '{"schema_version": 1, "metrics": {"length": {}}}',
        "   ",
        "null",
    ],
    ids=["empty-metrics", "empty-metric-object", "blank", "json-null"],
)
def test_semantically_empty_details_persist_as_null(libs, text):
    created = MeasurementSetRepository.create(_set(LEGACY, id="", measurement_details_json=text))
    raw = _raw_row(libs[1], created.id)
    assert raw["measurement_details_json"] is None
    assert not MeasurementSetRepository.get(created.id).is_enhanced


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda r: r.update(length_mean=11.0), "length_mean must be NULL"),
        (lambda r: r.update(q_core_max=None), "q_core_max must be a finite number"),
        (lambda r: r.update(measurement_details_json="{not json"), "not valid JSON"),
        (lambda r: r.update(measurement_details_json='{"schema_version": 1}'), "exactly schema_version and metrics"),
        (lambda r: r.update(measurement_details_json=_details({"depth": {"sd": {"value": 1}}})), "unknown metrics"),
        (lambda r: r.update(length_min=16.5), "must not exceed"),
    ],
    ids=["mean-exclusivity", "descriptor-without-pair", "malformed-json", "wrong-shape", "unknown-metric", "inverted-pair"],
)
def test_malformed_complete_candidate_is_rejected_before_commit(libs, mutation, message):
    row = dict(ENHANCED)
    mutation(row)
    with pytest.raises(ReferenceValidationError, match=message):
        MeasurementSetRepository.create(_set(row))
    assert _raw_row(libs[1], ENHANCED["id"]) is None, "nothing inserted"


def test_failed_validation_on_update_leaves_the_row_untouched(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    before = _raw_row(libs[1], created.id)
    with pytest.raises(ReferenceValidationError):
        MeasurementSetRepository.update(
            created.id,
            {"length_core_max": 15.0, "measurement_details_json": _details({"length": {"sd": {"value": -1}}})},
        )
    assert _raw_row(libs[1], created.id) == before, "no partial mutation of old or new columns"


def test_write_failure_after_the_update_statement_rolls_everything_back(libs, monkeypatch):
    """Ordinary columns and enhanced semantics change in one transaction."""
    created = MeasurementSetRepository.create(_set(ENHANCED))
    before = _raw_row(libs[1], created.id)

    def explode(*_args, **_kwargs):
        raise RuntimeError("intent recording failed")

    monkeypatch.setattr(reference_library_module, "record_library_mutation_intent", explode)
    new_details = json.loads(json.dumps(HEBELOMA))
    new_details["metrics"]["length"]["core_range"] = {"kind": "typical_range"}
    with pytest.raises(RuntimeError, match="intent recording failed"):
        MeasurementSetRepository.update(
            created.id,
            {"length_core_max": 15.0, "measurement_details_json": json.dumps(new_details)},
        )
    assert _raw_row(libs[1], created.id) == before


def test_unsupported_future_details_are_preserved_and_read_only(libs):
    row = {**ENHANCED, "id": "f0000000-0000-4000-8000-000000000001", "measurement_details_json": FUTURE_TEXT}
    _raw_insert(libs[1], row)
    fetched = MeasurementSetRepository.get(row["id"])
    assert fetched.measurement_details_json == FUTURE_TEXT, "bytes untouched by reading"
    details = fetched.measurement_content().details
    assert isinstance(details, UnsupportedMeasurementDetails)
    assert details.schema_version == 2 and details.raw == FUTURE
    before = _raw_row(libs[1], row["id"])
    with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
        MeasurementSetRepository.update(row["id"], {"notes": "inspect only"})
    with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
        MeasurementSetRepository.create_revision(row["id"], {"notes": "successor"})
    with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
        MeasurementSetRepository.create(_set(ENHANCED, id="", measurement_details_json=FUTURE_TEXT))
    # Overriding the details themselves must not downgrade the stored row:
    # neither clearing them (NULL) nor replacing them with valid v1 content,
    # through update or through a successor.
    downgrades = (
        {"measurement_details_json": None},
        {"measurement_details_json": None, "q_core_min": None, "q_core_max": None},
        {"measurement_details_json": json.dumps(HEBELOMA)},
    )
    for override in downgrades:
        with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
            MeasurementSetRepository.update(row["id"], override)
        with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
            MeasurementSetRepository.update(row["id"], override, bump_revision=False)
        with pytest.raises(ReferenceValidationError, match="unsupported measurement details version"):
            MeasurementSetRepository.create_revision(row["id"], override)
    assert _raw_row(libs[1], row["id"]) == before
    conn = _aware(libs[1])
    assert conn.execute(
        "SELECT COUNT(*) FROM reference_measurement_sets WHERE supersedes_id=?", (row["id"],)
    ).fetchone()[0] == 0
    conn.close()


def test_malformed_stored_details_are_reported_not_reinterpreted(libs):
    row = {**ENHANCED, "id": "f0000000-0000-4000-8000-000000000002", "measurement_details_json": "{broken"}
    _raw_insert(libs[1], row)
    fetched = MeasurementSetRepository.get(row["id"])
    assert fetched.measurement_details_json == "{broken"
    assert fetched.is_enhanced
    with pytest.raises(MeasurementContentError):
        fetched.measurement_content()
    with pytest.raises(ReferenceValidationError, match="not valid JSON"):
        MeasurementSetRepository.update(row["id"], {"notes": "x"})
    # Nor may the broken text be cleared or replaced from the repository.
    before = _raw_row(libs[1], row["id"])
    for override in ({"measurement_details_json": None}, {"measurement_details_json": json.dumps(HEBELOMA)}):
        with pytest.raises(ReferenceValidationError, match="not valid JSON"):
            MeasurementSetRepository.update(row["id"], override)
        with pytest.raises(ReferenceValidationError, match="not valid JSON"):
            MeasurementSetRepository.create_revision(row["id"], override)
    assert _raw_row(libs[1], row["id"]) == before


# --- 16–18: the Stage 3A barrier and the repository ---------------------------


def test_repository_insert_and_update_pass_the_barrier_through_registration(libs):
    conn = _unaware(libs[1])
    triggers = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='reference_measurement_sets'"
        )
    }
    conn.close()
    assert set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS) <= triggers, "barrier is installed"
    created = MeasurementSetRepository.create(_set(ENHANCED))  # INSERT of an enhanced row
    updated = MeasurementSetRepository.update(created.id, {"notes": "edited"})  # UPDATE of an enhanced row
    assert updated.revision == ENHANCED["revision"] + 1
    successor = MeasurementSetRepository.create_revision(created.id, {"notes": "successor"})
    assert successor.supersedes_id == created.id


def test_raw_unaware_connections_remain_blocked_after_repository_use(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    legacy = MeasurementSetRepository.create(_set(LEGACY))
    MeasurementSetRepository.update(created.id, {"notes": "aware write committed"})
    conn = _unaware(libs[1])
    for sql, params in (
        ("UPDATE reference_measurement_sets SET notes='x' WHERE id=?", (created.id,)),
        ("UPDATE reference_measurement_sets SET notes='x' WHERE id=?", (legacy.id,)),
        (
            "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind) "
            "VALUES ('n', ?, 'spore_size', 'range')",
            (TREATMENT_ID,),
        ),
    ):
        with pytest.raises(sqlite3.OperationalError, match="no such function"):
            conn.execute(sql, params)
    conn.rollback()
    assert conn.execute("SELECT notes FROM reference_measurement_sets WHERE id=?", (created.id,)).fetchone()[0] == "aware write committed"
    assert conn.execute("SELECT COUNT(*) FROM reference_measurement_sets").fetchone()[0] == 2
    conn.close()
    # The repository registers per connection only: a fresh sqlite3 connection
    # is unaware, so nothing was registered globally.
    fresh = sqlite3.connect(libs[1])
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        fresh.execute(f"SELECT {lib_schema.MEASUREMENT_CONTRACT_FUNCTION}()")
    fresh.close()


# --- 19–22: bundle and portable import ----------------------------------------


def _bundle_destination(libs):
    conn = _aware(libs[1])
    return conn


def test_bundle_import_preserves_all_enhanced_fields_and_stores_canonical_text(libs):
    conn = _bundle_destination(libs)
    loose = {**ENHANCED, "measurement_details_json": json.dumps(HEBELOMA, indent=1)}
    assert _upsert_library_row_by_revision(loose, conn, table="reference_measurement_sets") == "inserted"
    conn.commit()
    row = _raw_row(libs[1], ENHANCED["id"])
    assert row["measurement_details_json"] == ENHANCED["measurement_details_json"]
    assert (row["q_core_min"], row["q_core_max"]) == (1.36, 2.19)
    fetched = MeasurementSetRepository.get(ENHANCED["id"])
    assert fetched.measurement_content().details == decode_measurement_details(ENHANCED["measurement_details_json"])
    conn.close()


def test_bundle_import_of_legacy_row_infers_nothing(libs):
    conn = _bundle_destination(libs)
    omitting = {k: v for k, v in LEGACY.items() if k not in EXTENSION_FIELDS}
    assert _upsert_library_row_by_revision(omitting, conn, table="reference_measurement_sets") == "inserted"
    conn.commit()
    row = _raw_row(libs[1], LEGACY["id"])
    assert all(row[name] is None for name in EXTENSION_FIELDS)
    assert (row["length_core_min"], row["length_core_max"]) == (8.5, 12.5)
    assert row["length_mean"] is None
    conn.close()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: r.update(length_mean=11.0),
        lambda r: r.update(measurement_details_json="{broken"),
        lambda r: r.update(measurement_details_json=_details({"length": {"sd": {"value": -1}}})),
        lambda r: r.update(q_core_max=None),
    ],
    ids=["mean-exclusivity", "malformed-json", "negative-sd", "incomplete-core-pair"],
)
def test_malformed_enhanced_import_is_rejected_not_downgraded(libs, mutation):
    conn = _bundle_destination(libs)
    row = dict(ENHANCED)
    mutation(row)
    assert _upsert_library_row_by_revision(row, conn, table="reference_measurement_sets") == "rejected_invalid_content"
    conn.commit()
    assert _raw_row(libs[1], ENHANCED["id"]) is None
    # Same against an existing enhanced destination at a lower revision.
    assert _upsert_library_row_by_revision(ENHANCED, conn, table="reference_measurement_sets") == "inserted"
    conn.commit()
    before = _raw_row(libs[1], ENHANCED["id"])
    newer = {**row, "revision": ENHANCED["revision"] + 1}
    assert _upsert_library_row_by_revision(newer, conn, table="reference_measurement_sets") == "rejected_invalid_content"
    conn.commit()
    assert _raw_row(libs[1], ENHANCED["id"]) == before
    conn.close()


def test_bundle_import_applies_the_contract_fixture_decisions(libs):
    conn = _bundle_destination(libs)
    assert _upsert_library_row_by_revision(ENHANCED, conn, table="reference_measurement_sets") == "inserted"
    conn.commit()
    before = _raw_row(libs[1], ENHANCED["id"])
    omitting = _load("import_row_omitting_extension.json")
    partial = _load("import_row_partial_extension.json")
    explicit = _load("import_row_explicit_null_extension.json")

    assert _upsert_library_row_by_revision(partial, conn, table="reference_measurement_sets") == "rejected_partial_extension"
    assert _upsert_library_row_by_revision(omitting, conn, table="reference_measurement_sets") == "rejected_unacknowledged_extension"
    assert _upsert_library_row_by_revision(
        {**omitting, "revision": ENHANCED["revision"]}, conn, table="reference_measurement_sets"
    ) == "skipped_unacknowledged"
    assert _upsert_library_row_by_revision(
        {**omitting, "revision": 1}, conn, table="reference_measurement_sets"
    ) == "skipped_stale"
    assert _upsert_library_row_by_revision(ENHANCED, conn, table="reference_measurement_sets") == "skipped_same"
    assert _upsert_library_row_by_revision(
        {**ENHANCED, "length_core_max": 15.0}, conn, table="reference_measurement_sets"
    ) == "conflict"
    conn.commit()
    assert _raw_row(libs[1], ENHANCED["id"]) == before, "none of the above wrote anything"

    assert _upsert_library_row_by_revision(explicit, conn, table="reference_measurement_sets") == "updated"
    conn.commit()
    after = _raw_row(libs[1], ENHANCED["id"])
    assert after["revision"] == 3 and after["length_core_max"] == 15.0
    assert all(after[name] is None for name in EXTENSION_FIELDS), "explicit NULL is an acknowledged clear"
    # Partial is rejected with no destination as well.
    assert _upsert_library_row_by_revision(
        {**partial, "id": "f0000000-0000-4000-8000-000000000009"}, conn, table="reference_measurement_sets"
    ) == "rejected_partial_extension"
    assert _raw_row(libs[1], "f0000000-0000-4000-8000-000000000009") is None
    conn.close()


def test_bundle_import_of_unsupported_future_content_is_preserved_opaquely(libs):
    conn = _bundle_destination(libs)
    row = {**ENHANCED, "measurement_details_json": FUTURE_TEXT}
    assert _upsert_library_row_by_revision(row, conn, table="reference_measurement_sets") == "inserted"
    conn.commit()
    stored = _raw_row(libs[1], ENHANCED["id"])["measurement_details_json"]
    assert json.loads(stored) == FUTURE, "object preserved, never rewritten as v1"
    assert json.loads(stored)["schema_version"] == 2
    details = MeasurementSetRepository.get(ENHANCED["id"]).measurement_content().details
    assert isinstance(details, UnsupportedMeasurementDetails)
    with pytest.raises(ReferenceValidationError, match="unsupported"):
        MeasurementSetRepository.update(ENHANCED["id"], {"notes": "read-only"})
    conn.close()


def test_bundle_import_end_to_end_surfaces_rejections_in_the_report(libs, tmp_path, monkeypatch):
    MeasurementSetRepository.create(_set(ENHANCED))
    # A bundle written by a pre-contract exporter: its table has no extension
    # columns, so every row omits the keys.
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    source = sqlite3.connect(bundle_dir / "reference_values.db")
    ddl = lib_schema._REFERENCE_MEASUREMENT_SETS_DDL
    for name, sql_type in lib_schema.MEASUREMENT_CONTENT_EXTENSION_COLUMNS:
        ddl = ddl.replace(f"    {name} {sql_type},\n", "", 1)
    source.execute("CREATE TABLE reference_values (id INTEGER PRIMARY KEY, genus TEXT, species TEXT, source TEXT)")
    source.execute(ddl)
    omitting = _load("import_row_omitting_extension.json")
    columns = [k for k in omitting if k not in {"created_at", "updated_at"}]
    source.execute(
        f"INSERT INTO reference_measurement_sets ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [omitting[k] for k in columns],
    )
    source.commit()
    source.close()
    zip_path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(bundle_dir / "reference_values.db", "reference_values.db")

    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(libs[0]))
    monkeypatch.setattr(db_share, "get_reference_connection", lambda: sqlite3.connect(libs[1]))
    monkeypatch.setattr(db_share, "get_images_dir", lambda: tmp_path / "images")
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _s: None)
    before = _raw_row(libs[1], ENHANCED["id"])
    result = db_share.import_database_bundle(
        str(zip_path),
        include_observations=False, include_images=False, include_measurements=False,
        include_calibrations=False, include_reference_values=True,
    )
    assert result["reference_measurement_set_rejections"]["rejected_unacknowledged_extension"] == 1
    assert result["reference_measurement_sets"] == 0
    assert result["reference_library_updates"]["reference_measurement_sets"] == 0
    assert any("rejected_unacknowledged_extension" in w for w in result["warnings"])
    assert _raw_row(libs[1], ENHANCED["id"]) == before


def _attached(libs, tmp_path):
    main = sqlite3.connect(tmp_path / "portable-main.db")
    main.row_factory = sqlite3.Row
    main.execute("ATTACH DATABASE ? AS portable_reference", (str(libs[1]),))
    lib_schema.register_measurement_contract(main)
    return main


_PORTABLE_TABLE = "portable_reference.reference_measurement_sets"
_PORTABLE_KW = dict(
    table=_PORTABLE_TABLE,
    immutable_fields={"taxon_treatment_id", "supersedes_id"},
    json_fields={"raw_points_json", "measurement_details_json"},
)


def test_portable_import_preserves_enhanced_fields_and_rejects_malformed_or_partial(libs, tmp_path):
    main = _attached(libs, tmp_path)
    loose = {**ENHANCED, "measurement_details_json": json.dumps(HEBELOMA, indent=1)}
    _merge_reference_entity(loose, main, **_PORTABLE_KW)
    main.commit()
    row = _raw_row(libs[1], ENHANCED["id"])
    assert row["measurement_details_json"] == ENHANCED["measurement_details_json"]
    assert (row["q_core_min"], row["q_core_max"]) == (1.36, 2.19)
    # Replaying the same acknowledged content at the same revision is equivalent.
    _merge_reference_entity(ENHANCED, main, **_PORTABLE_KW)
    with pytest.raises(PortableIdentityConflictError, match="conflicting content"):
        _merge_reference_entity({**ENHANCED, "length_core_max": 15.0}, main, **_PORTABLE_KW)
    main.rollback()

    before = _raw_row(libs[1], ENHANCED["id"])
    for bad in (
        {**ENHANCED, "revision": 3, "length_mean": 11.0},
        {**ENHANCED, "revision": 3, "measurement_details_json": "{broken"},
        {**ENHANCED, "id": "f0000000-0000-4000-8000-00000000000a", "q_core_max": None},
    ):
        with pytest.raises(PortableImportError, match="invalid measurement content"):
            _merge_reference_entity(bad, main, **_PORTABLE_KW)
        main.rollback()
    with pytest.raises(PortableIdentityConflictError, match="incomplete measurement content extension"):
        _merge_reference_entity(_load("import_row_partial_extension.json"), main, **_PORTABLE_KW)
    main.rollback()
    with pytest.raises(PortableIdentityConflictError, match="incomplete measurement content extension"):
        _merge_reference_entity(
            {**_load("import_row_partial_extension.json"), "id": "f0000000-0000-4000-8000-00000000000b"},
            main, **_PORTABLE_KW,
        )
    main.rollback()
    assert _raw_row(libs[1], ENHANCED["id"]) == before
    assert _raw_row(libs[1], "f0000000-0000-4000-8000-00000000000a") is None
    main.close()


def test_portable_import_applies_the_omitting_and_explicit_null_fixture_decisions(libs, tmp_path):
    main = _attached(libs, tmp_path)
    _merge_reference_entity(ENHANCED, main, **_PORTABLE_KW)
    main.commit()
    before = _raw_row(libs[1], ENHANCED["id"])
    omitting = _load("import_row_omitting_extension.json")
    with pytest.raises(PortableIdentityConflictError, match="predates measurement content contract"):
        _merge_reference_entity(omitting, main, **_PORTABLE_KW)
    main.rollback()
    with pytest.raises(PortableIdentityConflictError, match="predates measurement content contract"):
        _merge_reference_entity({**omitting, "revision": ENHANCED["revision"]}, main, **_PORTABLE_KW)
    main.rollback()
    _merge_reference_entity({**omitting, "revision": 1}, main, **_PORTABLE_KW)  # stale: ignored
    assert _raw_row(libs[1], ENHANCED["id"]) == before

    _merge_reference_entity(_load("import_row_explicit_null_extension.json"), main, **_PORTABLE_KW)
    main.commit()
    after = _raw_row(libs[1], ENHANCED["id"])
    assert after["revision"] == 3 and after["length_core_max"] == 15.0
    assert all(after[name] is None for name in EXTENSION_FIELDS)
    main.close()


def test_portable_import_of_legacy_row_and_future_version(libs, tmp_path):
    main = _attached(libs, tmp_path)
    omitting_legacy = {k: v for k, v in LEGACY.items() if k not in EXTENSION_FIELDS}
    _merge_reference_entity(omitting_legacy, main, **_PORTABLE_KW)
    future = {**ENHANCED, "measurement_details_json": FUTURE_TEXT}
    _merge_reference_entity(future, main, **_PORTABLE_KW)
    main.commit()
    legacy = _raw_row(libs[1], LEGACY["id"])
    assert all(legacy[name] is None for name in EXTENSION_FIELDS)
    stored = _raw_row(libs[1], ENHANCED["id"])["measurement_details_json"]
    assert json.loads(stored) == FUTURE
    # Omitting the extension against a legacy destination replaces as today.
    _merge_reference_entity({**omitting_legacy, "revision": 2, "raw_text": "edited"}, main, **_PORTABLE_KW)
    main.commit()
    assert _raw_row(libs[1], LEGACY["id"])["raw_text"] == "edited"
    main.close()


# --- 23: successor and copy paths ---------------------------------------------


def test_create_revision_copies_all_extension_fields(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    successor = MeasurementSetRepository.create_revision(created.id, {"notes": "successor"})
    assert successor.supersedes_id == created.id and successor.revision == created.revision + 1
    for name in EXTENSION_FIELDS:
        assert getattr(successor, name) == getattr(created, name), name
    fetched = MeasurementSetRepository.get(successor.id)
    assert fetched.measurement_content().details == created.measurement_content().details
    legacy = MeasurementSetRepository.create(_set(LEGACY))
    legacy_successor = MeasurementSetRepository.create_revision(legacy.id, {"notes": "x"})
    assert not MeasurementSetRepository.get(legacy_successor.id).is_enhanced


def test_snapshot_of_enhanced_row_is_still_the_v1_projection_of_its_ordinary_columns(libs):
    """Existing scalar consumers see nothing new (snapshot v2 is Stage 3D)."""
    created = MeasurementSetRepository.create(_set(ENHANCED))
    work = ReferenceWorkRepository.get(WORK_ID)
    treatment = TaxonTreatmentRepository.get(TREATMENT_ID)
    enhanced_snapshot = build_observation_reference_snapshot(work, treatment, created)
    stripped = replace(created, measurement_details_json=None, q_core_min=None, q_core_max=None)
    assert enhanced_snapshot == build_observation_reference_snapshot(work, treatment, stripped)
    assert enhanced_snapshot["schema_version"] == 1
    assert "measurement_details" not in enhanced_snapshot
    assert enhanced_snapshot["measurements"]["length_mean"] is None


# --- 24: pull reconciliation never silently drops the extension ---------------


def test_pull_of_remote_change_over_enhanced_local_row_records_conflict(libs):
    _seed_acknowledged_graph()
    # Local row becomes enhanced without touching any cloud payload column.
    MeasurementSetRepository.update(
        "set-1",
        {"q_core_min": 1.4, "q_core_max": 1.8},
        bump_revision=False,
    )
    before = _raw_row(libs[1], "set-1")
    client = PullClient(
        works=[_work_row()],
        treatments=[_treatment_row()],
        sets=[_set_row(length_max=9.5, revision=2, row_version=2)],
    )
    result = pull_reference_library(client)
    state = ReferenceCloudSyncStateRepository.get_library("measurement_set", "set-1")
    assert result.conflicts == ("measurement_set:set-1",)
    assert state.sync_status == "conflict"
    assert state.conflict["reason"] == "unacknowledged_measurement_content_extension"
    assert _raw_row(libs[1], "set-1") == before, "no ordinary column changed under local descriptors"


def test_pull_merge_over_enhanced_local_row_records_conflict_instead_of_partial_write(libs):
    _seed_acknowledged_graph()
    MeasurementSetRepository.update(
        "set-1",
        {"notes": "local note", "q_core_min": 1.4, "q_core_max": 1.8},
    )
    before = _raw_row(libs[1], "set-1")
    client = PullClient(
        works=[_work_row()],
        treatments=[_treatment_row()],
        sets=[_set_row(length_max=9.5, revision=2, row_version=2)],
    )
    result = pull_reference_library(client)
    assert result.conflicts == ("measurement_set:set-1",)
    assert _raw_row(libs[1], "set-1") == before


def test_pull_of_legacy_rows_is_unchanged_by_the_guard(libs):
    _seed_acknowledged_graph()
    client = PullClient(
        works=[_work_row()],
        treatments=[_treatment_row()],
        sets=[_set_row(length_max=9.5, revision=2, row_version=2)],
    )
    result = pull_reference_library(client)
    assert result.conflicts == ()
    row = _raw_row(libs[1], "set-1")
    assert row["length_max"] == 9.5
    assert all(row[name] is None for name in EXTENSION_FIELDS)


# --- 25–26: stability and explicit edit transitions ----------------------------


def test_repeated_load_and_save_without_semantic_edits_is_stable(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    for _ in range(3):
        current = MeasurementSetRepository.get(created.id)
        MeasurementSetRepository.update(
            created.id, content_row_updates(current.measurement_content()), bump_revision=False
        )
    final = _raw_row(libs[1], created.id)
    assert final["measurement_details_json"] == ENHANCED["measurement_details_json"]
    assert final["revision"] == ENHANCED["revision"]
    assert (final["q_core_min"], final["q_core_max"]) == (1.36, 2.19)


def test_explicit_edit_transitions_through_the_repository(libs):
    """Contract section 12 item 4: setting a scalar mean on a row with a mean
    interval is rejected; clearing the interval first, then switching, works.
    Swapping L/W moves numbers and descriptors together."""
    created = MeasurementSetRepository.create(_set(ENHANCED))
    with pytest.raises(ReferenceValidationError, match="length_mean must be NULL"):
        MeasurementSetRepository.update(created.id, {"length_mean": 11.0})

    content = MeasurementSetRepository.get(created.id).measurement_content()
    content = clear_statistic(content, "length", "mean")
    content = set_scalar_mean(content, "length", 11.0)
    updated = MeasurementSetRepository.update(created.id, content_row_updates(content))
    assert updated.length_mean == 11.0
    assert _metric(MeasurementSetRepository.get(created.id), "length").mean_interval is None
    assert _metric(MeasurementSetRepository.get(created.id), "width").mean_interval is not None

    swapped = swap_length_width(MeasurementSetRepository.get(created.id).measurement_content())
    updated = MeasurementSetRepository.update(created.id, content_row_updates(swapped))
    fetched = MeasurementSetRepository.get(created.id)
    assert (fetched.length_min, fetched.length_max) == (4.1, 8.9)
    assert (fetched.width_min, fetched.width_max) == (6.9, 16.1)
    assert fetched.width_mean == 11.0 and fetched.length_mean is None
    assert _metric(fetched, "length").mean_interval == IntervalStatistic(5.6, 7.5, "reported_range")
    assert _metric(fetched, "width").mean_interval is None
    assert _metric(fetched, "width").sd == ScalarStatistic(0.696)
    assert (fetched.q_core_min, fetched.q_core_max) == (1.36, 2.19), "Q untouched by the swap"


def test_update_rejects_unknown_fields_but_accepts_the_extension_columns(libs):
    created = MeasurementSetRepository.create(_set(LEGACY))
    with pytest.raises(ReferenceValidationError, match="cannot update field"):
        MeasurementSetRepository.update(created.id, {"measurement_details": "{}"})
    updated = MeasurementSetRepository.update(
        created.id,
        {"measurement_details_json": encode_measurement_details(decode_measurement_details(_details({"length": {"core_range": {"kind": "typical_range"}}})))},
    )
    assert updated.is_enhanced
    assert _metric(updated, "length").core_range.kind == "typical_range"
    assert asdict(updated)["length_core_min"] == 8.5, "descriptor describes the existing pair; values untouched"
