"""Focused tests for the legacy ``reference_values`` audit + migration tools.

Covers both scripts against isolated temporary sqlite databases so no
production data is touched. Cloud sync, Supabase, landing-page work, AI
parsing and legacy-dialog removal are out of scope for these tools and
not exercised here.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import init_reference_library_schema
from tools import audit_legacy_reference_values as audit_tool
from tools import migrate_legacy_reference_values as migrate_tool


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    """Fresh temporary databases; both legacy and normalized live in one file
    (same as production layout)."""
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "does_not_exist.db",
    )
    _schema.init_database()
    return db_path, ref_path


def _insert_legacy(
    ref_path: Path,
    *,
    genus: str,
    species: str,
    source: str | None = None,
    mount_medium: str | None = None,
    stain: str | None = None,
    plot_color: str | None = None,
    length_min: float | None = None,
    length_p05: float | None = None,
    length_p50: float | None = None,
    length_p95: float | None = None,
    length_max: float | None = None,
    length_avg: float | None = None,
    width_min: float | None = None,
    width_p05: float | None = None,
    width_p50: float | None = None,
    width_p95: float | None = None,
    width_max: float | None = None,
    width_avg: float | None = None,
    q_min: float | None = None,
    q_p50: float | None = None,
    q_max: float | None = None,
    q_avg: float | None = None,
    parmasto_length_mean: float | None = None,
    metadata_json: str | None = None,
) -> int:
    """Insert one legacy row and return its rowid."""
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO reference_values (
                genus, species, source, mount_medium, stain, plot_color,
                length_min, length_p05, length_p50, length_p95, length_max, length_avg,
                width_min, width_p05, width_p50, width_p95, width_max, width_avg,
                q_min, q_p50, q_max, q_avg,
                parmasto_length_mean,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                genus,
                species,
                source,
                mount_medium,
                stain,
                plot_color,
                length_min, length_p05, length_p50, length_p95, length_max, length_avg,
                width_min, width_p05, width_p50, width_p95, width_max, width_avg,
                q_min, q_p50, q_max, q_avg,
                parmasto_length_mean,
                metadata_json,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _hash_file(path: Path) -> tuple[int, bytes]:
    """Cheap fingerprint of a sqlite file — size + short content prefix."""
    data = path.read_bytes()
    return len(data), data[:64]


# --- Phase A: audit ---------------------------------------------------------


def test_audit_makes_no_changes_to_legacy_or_normalized(libs, tmp_path):
    """AC-A1: the audit is strictly read-only."""
    _, ref_path = libs
    _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0,
        length_max=10.0,
        width_min=5.0,
        width_max=6.0,
    )
    fingerprint_before = _hash_file(ref_path)

    output_dir = tmp_path / "audit"
    audit_tool.main(
        [
            "--database",
            str(ref_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    fingerprint_after = _hash_file(ref_path)
    assert fingerprint_before == fingerprint_after, (
        "audit must not modify the legacy/normalized sqlite file"
    )
    # The three artifacts are produced.
    assert (output_dir / "legacy-reference-audit.md").exists()
    assert (output_dir / "legacy-reference-audit.csv").exists()
    assert (output_dir / "legacy-reference-migration.json").exists()


def test_audit_inventories_legacy_statistics_accurately(libs, tmp_path):
    """AC-A2: every core statistic surfaces in the audit result."""
    _, ref_path = libs
    row_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        mount_medium="KOH",
        stain="Melzer",
        length_min=8.0,
        length_p05=8.1,
        length_p50=9.0,
        length_p95=9.9,
        length_max=10.0,
        length_avg=9.05,
        width_min=5.0,
        width_max=6.0,
        width_avg=5.4,
        q_min=1.4,
        q_max=1.8,
        q_avg=1.6,
    )
    result = audit_tool.run_audit(database_path=ref_path)
    assert len(result["audits"]) == 1
    audit = result["audits"][0]
    assert audit.legacy_id == row_id
    assert audit.genus == "Russula"
    assert audit.species == "paludosa"
    assert audit.source == "Petersen 1990"
    assert audit.mount_medium == "KOH"
    assert audit.stain == "Melzer"
    assert audit.length_stats["length_min"] == 8.0
    assert audit.length_stats["length_avg"] == 9.05
    assert audit.width_stats["width_avg"] == 5.4
    assert audit.q_stats["q_max"] == 1.8
    assert audit.plotable is True
    assert audit.suggested_data_kind == "range"
    # Missing bibliographic fields must be surfaced.
    assert "title" in audit.missing_publication_fields
    assert "authors" in audit.missing_publication_fields
    assert "year" in audit.missing_publication_fields


def test_audit_handles_malformed_metadata_without_crashing(libs, tmp_path):
    """AC-A3: malformed metadata_json is reported, not silently discarded."""
    _, ref_path = libs
    _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        metadata_json="not json {",
    )
    result = audit_tool.run_audit(database_path=ref_path)
    assert len(result["audits"]) == 1
    audit = result["audits"][0]
    assert audit.metadata_malformed is True
    assert any(
        "metadata_json" in field for field in audit.unmapped_fields
    ), audit.unmapped_fields


def test_audit_reports_exact_duplicates_without_merging(libs, tmp_path):
    """AC-A4: two rows with identical (taxon, source, mount, stain,
    numeric columns) surface as exact-duplicate candidates. The audit
    never fuses them or emits a merged record."""
    _, ref_path = libs
    a = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0,
        length_max=10.0,
        width_min=5.0,
        width_max=6.0,
    )
    b = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0,
        length_max=10.0,
        width_min=5.0,
        width_max=6.0,
    )
    result = audit_tool.run_audit(database_path=ref_path)
    by_id = {a.legacy_id: a for a in result["audits"]}
    assert b in by_id[a].exact_duplicate_of
    assert a in by_id[b].exact_duplicate_of
    # Same source string but the audit does NOT collapse them.
    assert len(result["audits"]) == 2


def test_audit_reports_same_source_group_as_suggestion_only(libs, tmp_path):
    """AC-A audit-suggestion policy: rows sharing a source string are
    flagged as a group hint but NOT automatically fused."""
    _, ref_path = libs
    _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_avg=9.0,
        width_avg=5.5,
    )
    _insert_legacy(
        ref_path,
        genus="Russula",
        species="ochroleuca",
        source="Petersen 1990",
        length_avg=8.0,
        width_avg=5.0,
    )
    result = audit_tool.run_audit(database_path=ref_path)
    groups = result["source_groups"]
    assert "petersen 1990" in groups
    # Emitted manifest has an entry per row (never merged upstream).
    manifest_path = ref_path.parent / "manifest.json"
    audit_tool._write_manifest_template(result, output=manifest_path)
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["rows"]) == 2
    for entry in manifest["rows"]:
        assert entry["action"] == "unresolved"


def test_audit_marks_parmasto_and_plot_color_as_unmapped(libs, tmp_path):
    """AC-A: parmasto columns and plot_color are reported as unmapped/
    preserved-in-notes rather than silently dropped."""
    _, ref_path = libs
    _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Parmasto 1965",
        parmasto_length_mean=9.5,
        plot_color="#ff8800",
    )
    result = audit_tool.run_audit(database_path=ref_path)
    audit = result["audits"][0]
    assert any(
        f.startswith("parmasto_*") for f in audit.unmapped_fields
    ), audit.unmapped_fields
    assert any(
        f.startswith("plot_color") for f in audit.unmapped_fields
    ), audit.unmapped_fields
    assert audit.suggested_data_kind == "parmasto"


# --- Phase C: migration -----------------------------------------------------


def _build_manifest(
    *,
    ref_path: Path,
    works: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "generated_at": "2026-08-06T00:00:00+00:00",
        "database_path": str(ref_path),
        "defaults": {},
        "works": works,
        "rows": rows,
    }


def _row_migrate(
    legacy_id: int,
    *,
    work_key: str,
    name_as_published: str,
    data_kind: str = "range",
    notes: str | None = None,
) -> dict[str, Any]:
    return {
        "legacy_id": legacy_id,
        "action": "migrate",
        "work_key": work_key,
        "treatment": {
            "name_as_published": name_as_published,
            "taxon_id": None,
            "page_from": None,
            "page_to": None,
            "locator_text": None,
            "treatment_notes": None,
        },
        "measurement_set": {
            "data_kind": data_kind,
            "raw_text": None,
            "notes": notes,
            "measurement_method": None,
            "preparation": None,
        },
    }


def _work_entry(
    *,
    key: str,
    title: str,
    year: int | None = None,
    authors: list[dict[str, Any]] | None = None,
    citation_key: str | None = None,
    work_type: str = "book",
) -> dict[str, Any]:
    return {
        "work_key": key,
        "type": work_type,
        "title": title,
        "short_label": None,
        "authors": authors or [{"family": "Petersen"}],
        "editors": [],
        "year": year,
        "publisher": None,
        "place": None,
        "container_title": None,
        "edition": None,
        "volume": None,
        "issue": None,
        "pages": None,
        "doi": None,
        "isbn": None,
        "url": None,
        "language": None,
        "citation_key": citation_key,
        "citation_override": None,
    }


def test_migration_dry_run_makes_no_changes(libs, tmp_path):
    """AC-C1: dry-run never writes."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0,
        length_max=10.0,
        width_min=5.0,
        width_max=6.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[_row_migrate(legacy_id, work_key="petersen-1990", name_as_published="Russula paludosa")],
    )
    validated = migrate_tool.validate_manifest(manifest)
    fingerprint_before = _hash_file(ref_path)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=True
    )
    fingerprint_after = _hash_file(ref_path)
    assert fingerprint_before[1] == fingerprint_after[1], (
        "dry-run must not mutate any data in the sqlite file"
    )
    assert report.dry_run is True
    assert len(report.created) == 1  # simulated create
    assert len(report.failed) == 0
    # No normalized rows actually written.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_migration_apply_creates_work_treatment_and_measurement_set(libs, tmp_path):
    """AC-C1: --apply creates all three normalized entities and preserves
    ``legacy_reference_value_id``."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        mount_medium="KOH",
        stain="Melzer",
        length_min=8.0,
        length_max=10.0,
        width_min=5.0,
        width_max=6.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[_row_migrate(legacy_id, work_key="petersen-1990", name_as_published="Russula paludosa")],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert report.dry_run is False
    assert len(report.created) == 1
    assert report.failed == []

    # Verify the row round-trips through the normalized repositories.
    ms_id = report.created[0]["measurement_set_id"]
    ms = MeasurementSetRepository.get(ms_id)
    assert ms is not None
    assert ms.legacy_reference_value_id == legacy_id
    assert ms.length_min == 8.0 and ms.length_max == 10.0
    assert ms.width_min == 5.0 and ms.width_max == 6.0
    assert ms.mount_medium == "KOH"
    assert ms.stain == "Melzer"


def test_migration_reuses_a_work_when_manifest_shares_the_work_key(libs, tmp_path):
    """AC-C2: two rows referring to the same work_key share a single
    normalized work."""
    _, ref_path = libs
    a = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_avg=9.0,
        width_avg=5.5,
    )
    b = _insert_legacy(
        ref_path,
        genus="Russula",
        species="ochroleuca",
        source="Petersen 1990",
        length_avg=8.0,
        width_avg=5.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[
            _row_migrate(a, work_key="petersen-1990", name_as_published="Russula paludosa", data_kind="summary"),
            _row_migrate(b, work_key="petersen-1990", name_as_published="Russula ochroleuca", data_kind="summary"),
        ],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert len(report.created) == 2
    work_ids = {row["work_id"] for row in report.created}
    assert len(work_ids) == 1


def test_migration_separates_works_when_manifest_uses_distinct_keys(libs, tmp_path):
    """AC-C3: two rows with distinct work_keys receive two separate
    normalized works — fuzzy source labels are NEVER auto-merged."""
    _, ref_path = libs
    a = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_avg=9.0,
        width_avg=5.5,
    )
    b = _insert_legacy(
        ref_path,
        genus="Russula",
        species="ochroleuca",
        source="Petersen 1990",  # same source text
        length_avg=8.0,
        width_avg=5.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[
            _work_entry(key="petersen-1990-a", title="Version A", year=1990),
            _work_entry(key="petersen-1990-b", title="Version B", year=1990),
        ],
        rows=[
            _row_migrate(a, work_key="petersen-1990-a", name_as_published="Russula paludosa", data_kind="summary"),
            _row_migrate(b, work_key="petersen-1990-b", name_as_published="Russula ochroleuca", data_kind="summary"),
        ],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    work_ids = {row["work_id"] for row in report.created}
    assert len(work_ids) == 2


def test_migration_is_idempotent_on_rerun(libs, tmp_path):
    """AC-C: a second run over the same manifest reuses previous results
    instead of double-inserting."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[_row_migrate(legacy_id, work_key="petersen-1990", name_as_published="Russula paludosa")],
    )
    validated = migrate_tool.validate_manifest(manifest)
    r1 = migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)
    assert len(r1.created) == 1
    r2 = migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)
    assert len(r2.created) == 0
    assert len(r2.reused) == 1
    # And no duplicate measurement sets exist.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets WHERE legacy_reference_value_id = ?",
            (legacy_id,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 1


def test_migration_refuses_apply_with_unresolved_entries(libs, tmp_path):
    """AC-C: any entry with action=unresolved blocks --apply."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path, genus="Russula", species="paludosa"
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[],
        rows=[{"legacy_id": legacy_id, "action": "unresolved"}],
    )
    validated = migrate_tool.validate_manifest(manifest)
    with pytest.raises(migrate_tool.ManifestError):
        migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)


def test_dry_run_reports_unresolved_entries_without_failing(libs, tmp_path):
    """A dry-run reports unresolved entries so the operator can act on
    them without the tool erroring out."""
    _, ref_path = libs
    legacy_id = _insert_legacy(ref_path, genus="Russula", species="paludosa")
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[],
        rows=[{"legacy_id": legacy_id, "action": "unresolved"}],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=True
    )
    assert [r["legacy_id"] for r in report.unresolved] == [legacy_id]
    # No normalized rows created.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_manifest_validation_rejects_contradictory_entries(libs, tmp_path):
    """AC-C: an unknown work_key referenced by a migrate row is rejected
    BEFORE any writes."""
    _, ref_path = libs
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[],
        rows=[_row_migrate(1, work_key="does-not-exist", name_as_published="Russula paludosa")],
    )
    with pytest.raises(migrate_tool.ManifestError) as excinfo:
        migrate_tool.validate_manifest(manifest)
    assert "does-not-exist" in str(excinfo.value)


def test_manifest_validation_rejects_duplicate_legacy_ids(libs, tmp_path):
    _, ref_path = libs
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="k", title="T")],
        rows=[
            _row_migrate(7, work_key="k", name_as_published="A"),
            _row_migrate(7, work_key="k", name_as_published="B"),
        ],
    )
    with pytest.raises(migrate_tool.ManifestError):
        migrate_tool.validate_manifest(manifest)


def test_partial_failure_does_not_abort_other_rows_but_reports_failure(libs, tmp_path):
    """AC-C: a per-row failure (e.g. legacy_id missing from the database)
    is reported. Successful rows still land. Nothing partially-written
    for the failing row remains."""
    _, ref_path = libs
    good_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    bogus_id = 999999  # not present in reference_values
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[
            _row_migrate(good_id, work_key="petersen-1990", name_as_published="Russula paludosa"),
            _row_migrate(bogus_id, work_key="petersen-1990", name_as_published="Ghost taxon"),
        ],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert len(report.created) == 1
    assert report.created[0]["legacy_id"] == good_id
    assert len(report.failed) == 1
    assert report.failed[0]["legacy_id"] == bogus_id


def test_migration_never_touches_the_source_legacy_row(libs, tmp_path):
    """AC-C: the legacy reference_values row must be identical before and
    after migration (no update, no delete)."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Petersen 1990",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
        rows=[_row_migrate(legacy_id, work_key="petersen-1990", name_as_published="Russula paludosa")],
    )
    validated = migrate_tool.validate_manifest(manifest)

    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        before = dict(conn.execute(
            "SELECT * FROM reference_values WHERE id = ?", (legacy_id,)
        ).fetchone())
    finally:
        conn.close()

    migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)

    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        after = dict(conn.execute(
            "SELECT * FROM reference_values WHERE id = ?", (legacy_id,)
        ).fetchone())
    finally:
        conn.close()

    # Every column identical, including the `updated_at` timestamp — no
    # UPDATE was issued against the legacy row.
    assert before == after


def test_unsupported_fields_reported_and_preserved_in_notes(libs, tmp_path):
    """AC-C: parmasto/plot_color/metadata_json contents are reported and
    preserved in ``notes`` — never silently dropped."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Parmasto 1965",
        parmasto_length_mean=9.5,
        plot_color="#ff8800",
        length_avg=9.0,
        width_avg=5.5,
        metadata_json='{"custom": "value"}',
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(
            key="parmasto-1965", title="Parmasto notebook", year=1965,
            authors=[{"family": "Parmasto"}],
        )],
        rows=[_row_migrate(
            legacy_id,
            work_key="parmasto-1965",
            name_as_published="Russula paludosa",
            data_kind="parmasto",
        )],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert report.unsupported_fields, "must report at least one unsupported field"
    entry = report.unsupported_fields[0]
    assert entry["legacy_id"] == legacy_id
    ms_id = report.created[0]["measurement_set_id"]
    ms = MeasurementSetRepository.get(ms_id)
    assert ms is not None
    assert ms.notes is not None
    assert "parmasto_length_mean=9.5" in ms.notes
    assert "plot_color=#ff8800" in ms.notes
    assert "custom" in ms.notes


def test_null_legacy_values_remain_null_in_normalized(libs, tmp_path):
    """AC-C: nulls must NOT be replaced with 0.0 or fabricated defaults."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Sparse 2020",
        length_avg=9.0,   # only length_avg populated
        width_avg=5.5,    # only width_avg populated
    )
    manifest = _build_manifest(
        ref_path=ref_path,
        works=[_work_entry(key="sparse", title="Sparse work", year=2020)],
        rows=[_row_migrate(
            legacy_id,
            work_key="sparse",
            name_as_published="Russula paludosa",
            data_kind="summary",
        )],
    )
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    ms_id = report.created[0]["measurement_set_id"]
    ms = MeasurementSetRepository.get(ms_id)
    assert ms is not None
    assert ms.length_min is None and ms.length_max is None
    assert ms.width_min is None and ms.width_max is None
    assert ms.length_mean == 9.0
    assert ms.width_mean == 5.5
    assert ms.q_mean is None and ms.q_min is None and ms.q_max is None
    assert ms.sample_size is None


def test_attach_to_existing_stamps_legacy_id_on_existing_set(libs, tmp_path):
    """AC-C: attach_to_existing writes only ``legacy_reference_value_id``
    on an already-existing normalized measurement set."""
    db_path, ref_path = libs

    # Seed an existing normalized work / treatment / measurement set.
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Existing book",
            short_label="Existing 1990",
            authors_json=json.dumps([{"family": "Existing"}]),
            year=1990,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
        )
    )
    legacy_id = _insert_legacy(
        ref_path, genus="Russula", species="paludosa", source="Existing 1990"
    )

    manifest = _build_manifest(
        ref_path=ref_path,
        works=[],
        rows=[
            {
                "legacy_id": legacy_id,
                "action": "attach_to_existing",
                "existing_measurement_set_id": ms.id,
            }
        ],
    )
    validated = migrate_tool.validate_manifest(manifest)
    migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)

    refreshed = MeasurementSetRepository.get(ms.id)
    assert refreshed is not None
    assert refreshed.legacy_reference_value_id == legacy_id


def test_audit_manifest_contract_every_row_gets_migration_state_entry(
    libs, tmp_path
):
    """AC-18: the audit manifest emits ONE migration-state entry per
    audited legacy row. The top-level fields are ``manifest_version``
    and ``rows`` — a prior inspection looking for ``version`` / ``entries``
    reported zero because those field names do not exist in the schema
    this tool produces. This regression pins both facts down.
    """
    from tools import audit_legacy_reference_values as audit_tool

    _, ref_path = libs
    # Seed 25 legacy rows spanning several source strings, plot-kinds
    # and empty-source edge cases — the manifest must still enumerate
    # every one.
    legacy_ids: list[int] = []
    for i in range(25):
        legacy_ids.append(
            _insert_legacy(
                ref_path,
                genus="Russula" if i % 2 else "Flammulina",
                species=f"sp{i}",
                source=(
                    "Ripkova et al, 2010" if i % 3 else "Parmasto, 1987"
                    if i % 5 else None
                ),
                length_min=8.0 if i % 4 else None,
                length_max=10.0 if i % 4 else None,
            )
        )
    output_dir = tmp_path / "audit"
    audit_tool.main(
        [
            "--database",
            str(ref_path),
            "--output-dir",
            str(output_dir),
        ]
    )
    manifest_path = output_dir / "legacy-reference-migration.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # Correct top-level field names.
    assert "manifest_version" in manifest
    assert manifest["manifest_version"] == 1
    assert "rows" in manifest
    # An inspector using the wrong field names would (correctly) find
    # nothing — this asserts the schema shape rather than a defect.
    assert "version" not in manifest
    assert "entries" not in manifest

    # Every audited legacy row must be represented exactly once.
    rows = manifest["rows"]
    assert len(rows) == len(legacy_ids)
    row_ids = [entry["legacy_id"] for entry in rows]
    assert sorted(row_ids) == sorted(legacy_ids)
    # Every entry carries an action.
    for entry in rows:
        assert entry["action"] in {
            "migrate",
            "attach_to_existing",
            "skip",
            "unresolved",
            "already_migrated",
        }


def test_apply_via_cli_requires_confirm_backup(libs, tmp_path):
    """AC-C: the CLI refuses to write without the explicit
    ``--confirm-backup`` acknowledgement."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path, genus="Russula", species="paludosa", source="Petersen 1990",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            _build_manifest(
                ref_path=ref_path,
                works=[_work_entry(key="petersen-1990", title="Danmarks Basidiesvampe", year=1990)],
                rows=[_row_migrate(legacy_id, work_key="petersen-1990", name_as_published="Russula paludosa")],
            )
        )
    )
    fingerprint_before = _hash_file(ref_path)
    exit_code = migrate_tool.main(
        [
            "--manifest",
            str(manifest_path),
            "--database",
            str(ref_path),
            "--apply",
        ]
    )
    assert exit_code == 2
    # Refusal did not modify the database.
    assert _hash_file(ref_path) == fingerprint_before
