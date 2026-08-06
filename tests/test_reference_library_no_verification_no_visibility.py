"""Regressions for the removal of verification_status / visibility.

Product decision (see :mod:`docs/reference-data/PLAN-reference-library-and-public-plots`):

- Reference works no longer carry a manually assigned verification status.
- Reference works no longer carry a private/shared/curated-public
  visibility scope.
- Public exposure of an attached reference is governed by the observation
  visibility and its frozen ``observation_reference_uses.snapshot_json``.
- Bibliographic completeness is derived from the record's fields and
  shown as a non-blocking hint.
- No destructive column drop — existing sqlite files may still carry
  the two legacy columns, but application code must not depend on them.

These tests lock in that contract so a future accidental reintroduction
of either concept fails loudly.
"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from dataclasses import fields as _fields

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
PySide6 = pytest.importorskip("PySide6")

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


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture()
def libs(tmp_path, monkeypatch):
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


@pytest.fixture(scope="module")
def qapp():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _make_observation(db_path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-01-01", "Test"),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


# --- Domain model -----------------------------------------------------------


def test_reference_work_dataclass_no_longer_carries_removed_fields():
    """AC-1 (domain): the dataclass explicitly does not expose
    verification_status or visibility."""
    field_names = {f.name for f in _fields(ReferenceWork)}
    assert "verification_status" not in field_names
    assert "visibility" not in field_names


def test_repository_create_ignores_legacy_ddl_defaults(libs):
    """AC-1 (repo): creating a work no longer requires or accepts
    verification_status/visibility. Old sqlite files that still carry the
    DDL columns fall back to their column DEFAULT so the write remains
    non-destructive."""
    _, ref_path = libs
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="No Verification Required",
            short_label="NV 2020",
            authors_json=json.dumps([{"family": "Author"}]),
            year=2020,
        )
    )
    # Read the raw row to prove the columns are still present at the DDL
    # level (compat) but the application object exposes nothing about them.
    conn = sqlite3.connect(ref_path)
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(reference_works)"
        ).fetchall()}
        assert "verification_status" in cols  # compat DDL retention
        assert "visibility" in cols
        row = conn.execute(
            "SELECT verification_status, visibility FROM reference_works "
            "WHERE id = ?",
            (work.id,),
        ).fetchone()
    finally:
        conn.close()
    # Column default written by sqlite (non-null); values are ignored by
    # the domain object either way.
    assert row is not None
    assert row[0] is not None and row[1] is not None


def test_loading_row_with_legacy_columns_still_works(libs):
    """AC-10 (regression): an old sqlite file already carrying values in
    the two compat columns still loads through the repository without
    raising and without leaking the values onto the domain object."""
    _, ref_path = libs
    work_id = str(uuid.uuid4())
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute(
            """
            INSERT INTO reference_works
            (id, type, title, short_label, authors_json, editors_json,
             verification_status, visibility)
            VALUES (?, 'book', 'Legacy Row', 'LR', '[]', '[]', 'verified', 'shared')
            """,
            (work_id,),
        )
        conn.commit()
    finally:
        conn.close()

    loaded = ReferenceWorkRepository.get(work_id)
    assert loaded is not None
    assert loaded.title == "Legacy Row"
    # Domain object no longer exposes either concept.
    assert not hasattr(loaded, "verification_status")
    assert not hasattr(loaded, "visibility")


def test_repository_update_rejects_removed_fields(libs):
    """AC-1 (repo API): attempting to update either removed column via the
    repository raises the standard validation error rather than silently
    writing to a stale column."""
    from database.reference_library import ReferenceValidationError

    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Reject removed fields",
            short_label="RRF",
            authors_json=json.dumps([{"family": "X"}]),
            year=2020,
        )
    )
    with pytest.raises(ReferenceValidationError):
        ReferenceWorkRepository.update(work.id, {"verification_status": "verified"})
    with pytest.raises(ReferenceValidationError):
        ReferenceWorkRepository.update(work.id, {"visibility": "shared"})


# --- Editor form ------------------------------------------------------------


def test_editor_form_does_not_display_verification_or_visibility(libs, qapp):
    """AC-2 / AC-8: the work editor exposes no verification combo, no
    visibility combo, and the Advanced section only contains short-label
    override / citation key / language / full citation override."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        assert not hasattr(form, "verification_combo")
        assert not hasattr(form, "visibility_combo")
        # The four remaining advanced inputs are still present.
        assert hasattr(form, "short_label_input")
        assert hasattr(form, "citation_key_input")
        assert hasattr(form, "language_input")
        assert hasattr(form, "citation_override_input")
        # And the completeness-hint label appears (derived, non-blocking).
        assert hasattr(form, "completeness_hints_label")
    finally:
        form.deleteLater()


def test_saving_does_not_require_verification_or_visibility(libs, qapp):
    """AC-2 (behavior): the operator can save a fresh work with just the
    basics — no verification / visibility field is required."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        idx = form.type_combo.findData("book")
        form.type_combo.setCurrentIndex(idx)
        form.title_input.setText("Minimal work")
        form.authors_editor.add_row(family="Someone", mark_dirty=True)
        form._on_save()
        assert form.result_work is not None
        # Domain object exposes neither concept.
        assert not hasattr(form.result_work, "verification_status")
        assert not hasattr(form.result_work, "visibility")
    finally:
        form.deleteLater()


# --- Completeness hints -----------------------------------------------------


def test_completeness_hints_are_derived_and_non_blocking(libs, qapp):
    """AC-3 / AC-10: hints are computed from current field values and NEVER
    prevent saving. A fully-blank work saves once the required title +
    authors + a valid type are set, even though every optional hint is
    still active."""
    from ui.reference_library_manager_dialog import (
        _ReferenceWorkForm,
        reference_work_completeness_hints,
    )

    form = _ReferenceWorkForm(None)
    try:
        idx = form.type_combo.findData("book")
        form.type_combo.setCurrentIndex(idx)
        form.title_input.setText("T")
        form.authors_editor.add_row(family="A", mark_dirty=True)
        # No year, no publisher — the hint list should include both.
        preview_work = form._preview_work(use_overrides=True)
        hints = reference_work_completeness_hints(preview_work)
        assert "missing year" in hints
        assert "missing publication/container information" in hints
        # Save still succeeds regardless of these hints.
        form._on_save()
        assert form.result_work is not None
    finally:
        form.deleteLater()


def test_completeness_hints_ignore_verification_and_visibility(libs, qapp):
    """AC-3: the hint list is strictly about bibliographic completeness.
    Verification and visibility are gone from the model, so they cannot
    appear as hints."""
    from ui.reference_library_manager_dialog import (
        reference_work_completeness_hints,
    )

    work = ReferenceWork(
        id="",
        type="book",
        title="Complete",
        short_label="C",
        authors_json=json.dumps([{"family": "Author"}]),
        year=2020,
        publisher="Sun",
    )
    hints = reference_work_completeness_hints(work)
    assert hints == []
    # A minimally incomplete work surfaces only bibliographic hints.
    empty = ReferenceWork(
        id="",
        type="book",
        title="",
        short_label="",
        authors_json="[]",
    )
    hints2 = reference_work_completeness_hints(empty)
    assert "missing title" in hints2
    assert "missing authors" in hints2
    assert "missing year" in hints2
    # Absolutely no "unverified" / "verified" / "visibility" wording.
    combined = " ".join(hints2).lower()
    assert "verif" not in combined
    assert "visibility" not in combined


# --- Migration manifest -----------------------------------------------------


def test_migration_manifest_template_omits_verification_and_visibility(
    tmp_path, libs
):
    """AC-6 (audit): the manifest template no longer emits
    defaults.verification_status or defaults.visibility."""
    from tools import audit_legacy_reference_values as audit_tool

    _, ref_path = libs
    # Insert one legacy row so the template has something to iterate.
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute(
            "INSERT INTO reference_values (genus, species, source) "
            "VALUES ('Russula', 'paludosa', 'Some source')"
        )
        conn.commit()
    finally:
        conn.close()

    output_dir = tmp_path / "audit"
    audit_tool.main(
        [
            "--database",
            str(ref_path),
            "--output-dir",
            str(output_dir),
        ]
    )
    manifest_text = (output_dir / "legacy-reference-migration.json").read_text()
    manifest = json.loads(manifest_text)
    assert manifest["defaults"] == {}
    # The raw text also must not mention either concept anywhere in the
    # template — this guards against reintroduction via free-text notes.
    assert "verification_status" not in manifest_text
    assert "visibility" not in manifest_text


def test_migration_creates_works_without_verification_or_visibility(libs):
    """AC-7: the migration tool creates plain local records that expose
    neither concept."""
    from tools import migrate_legacy_reference_values as migrate_tool

    _, ref_path = libs
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO reference_values
            (genus, species, source, length_min, length_max, width_min, width_max)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("Russula", "paludosa", "Petersen 1990", 8.0, 10.0, 5.0, 6.0),
        )
        legacy_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "manifest_version": 1,
        "generated_at": "2026-08-06T00:00:00+00:00",
        "database_path": str(ref_path),
        "defaults": {},
        "works": [
            {
                "work_key": "petersen-1990",
                "type": "book",
                "title": "Danmarks Basidiesvampe",
                "authors": [{"family": "Petersen"}],
                "year": 1990,
            }
        ],
        "rows": [
            {
                "legacy_id": legacy_id,
                "action": "migrate",
                "work_key": "petersen-1990",
                "treatment": {"name_as_published": "Russula paludosa"},
                "measurement_set": {"data_kind": "range"},
            }
        ],
    }
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert len(report.created) == 1
    ms = MeasurementSetRepository.get(report.created[0]["measurement_set_id"])
    assert ms is not None
    # The created ReferenceWork does not expose either concept.
    work = ReferenceWorkRepository.get(report.created[0]["work_id"])
    assert work is not None
    assert not hasattr(work, "verification_status")
    assert not hasattr(work, "visibility")


def test_migration_manifest_with_legacy_verification_keys_still_loads(libs):
    """AC-7 (compat): a manifest carrying leftover verification_status /
    visibility keys on a work entry validates cleanly. The keys are
    ignored — the migration never infers approval or scope."""
    from tools import migrate_legacy_reference_values as migrate_tool

    _, ref_path = libs
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            "INSERT INTO reference_values (genus, species, source) "
            "VALUES ('Russula', 'paludosa', 'Petersen 1990')"
        )
        legacy_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "manifest_version": 1,
        "generated_at": "2026-08-06T00:00:00+00:00",
        "defaults": {
            "verification_status": "unverified",  # legacy key
            "visibility": "private",              # legacy key
        },
        "works": [
            {
                "work_key": "petersen-1990",
                "type": "book",
                "title": "Legacy Compat",
                "authors": [{"family": "Petersen"}],
                "year": 1990,
                "verification_status": "unverified",
                "visibility": "curated_public",
            }
        ],
        "rows": [
            {
                "legacy_id": legacy_id,
                "action": "migrate",
                "work_key": "petersen-1990",
                "treatment": {"name_as_published": "Russula paludosa"},
                "measurement_set": {"data_kind": "summary"},
            }
        ],
    }
    validated = migrate_tool.validate_manifest(manifest)
    report = migrate_tool.run_migration(
        validated, database_path=ref_path, dry_run=False
    )
    assert report.failed == []
    assert len(report.created) == 1


# --- Idempotency + null preservation preserved ------------------------------


def test_migration_idempotency_survives_removal(libs):
    from tools import migrate_legacy_reference_values as migrate_tool

    _, ref_path = libs
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO reference_values
            (genus, species, source, length_min, length_max, width_min, width_max)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("Russula", "paludosa", "Petersen 1990", 8.0, 10.0, 5.0, 6.0),
        )
        legacy_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    manifest = {
        "manifest_version": 1,
        "generated_at": "2026-08-06T00:00:00+00:00",
        "defaults": {},
        "works": [
            {
                "work_key": "petersen-1990",
                "type": "book",
                "title": "T",
                "authors": [{"family": "P"}],
                "year": 1990,
            }
        ],
        "rows": [
            {
                "legacy_id": legacy_id,
                "action": "migrate",
                "work_key": "petersen-1990",
                "treatment": {"name_as_published": "Russula paludosa"},
                "measurement_set": {"data_kind": "range"},
            }
        ],
    }
    validated = migrate_tool.validate_manifest(manifest)
    r1 = migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)
    r2 = migrate_tool.run_migration(validated, database_path=ref_path, dry_run=False)
    assert len(r1.created) == 1
    assert len(r2.created) == 0
    assert len(r2.reused) == 1


# --- Snapshot semantics -----------------------------------------------------


def test_public_snapshot_does_not_depend_on_reference_visibility(libs):
    """AC-10: public exposure of an attached reference is governed by the
    observation's own visibility and by the frozen snapshot, NOT by any
    reference-level visibility value. The snapshot dict must not carry a
    ``visibility`` key or a ``verification_status`` key sourced from
    the ReferenceWork."""
    from database.reference_citation import build_observation_reference_snapshot

    _, _ = libs
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Snapshot check",
            short_label="SC 2020",
            authors_json=json.dumps([{"family": "Author"}]),
            year=2020,
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
    snapshot = build_observation_reference_snapshot(work, treatment, ms)
    assert "visibility" not in snapshot
    assert "verification_status" not in snapshot


def test_attach_uses_observation_visibility_not_reference_visibility(libs):
    """AC-10: attaching a reference to an observation stores a snapshot
    that does not carry any reference-level visibility hint. Observation
    visibility (spore_data_visibility etc.) remains an observation-side
    concern."""
    db_path, _ = libs
    obs_id = _make_observation(db_path)

    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="No leak",
            short_label="NL 2020",
            authors_json=json.dumps([{"family": "Author"}]),
            year=2020,
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
    use = ObservationReferenceUseRepository.attach(obs_id, ms.id, role="compared")
    payload = json.loads(use.snapshot_json)
    assert "visibility" not in payload
    assert "verification_status" not in payload


# --- Manager dialog ---------------------------------------------------------


def test_manager_dialog_removes_status_badge(libs, qapp):
    """AC-3 (UI): the manager pane no longer has a verification badge —
    the completeness hint label takes its place."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        assert not hasattr(dialog, "status_badge")
        assert hasattr(dialog, "completeness_hint_label")
    finally:
        dialog.deleteLater()
