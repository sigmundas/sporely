"""Focused tests for the interactive legacy → normalized migration workflow.

The interactive walkthrough replaces hand-edited JSON as the normal
migration path. Tests exercise the I/O-free
:class:`InteractiveMigrationSession` engine directly; the thin
stdin/stdout driver is not covered here (its only job is to route
menu keys onto engine methods).
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import fields as _fields
from pathlib import Path

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
from tools import migrate_legacy_reference_values as migrate_tool


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


def _insert_legacy(
    ref_path: Path,
    *,
    genus: str,
    species: str,
    source: str | None = None,
    length_min: float | None = None,
    length_max: float | None = None,
    width_min: float | None = None,
    width_max: float | None = None,
    length_avg: float | None = None,
    width_avg: float | None = None,
    parmasto_length_mean: float | None = None,
    mount_medium: str | None = None,
    stain: str | None = None,
) -> int:
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO reference_values
            (genus, species, source, length_min, length_max, width_min, width_max,
             length_avg, width_avg, parmasto_length_mean, mount_medium, stain)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                genus,
                species,
                source,
                length_min,
                length_max,
                width_min,
                width_max,
                length_avg,
                width_avg,
                parmasto_length_mean,
                mount_medium,
                stain,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _make_session(
    ref_path: Path, tmp_path: Path, *, dry_run: bool = False
) -> migrate_tool.InteractiveMigrationSession:
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    return migrate_tool.InteractiveMigrationSession(
        database_path=ref_path,
        state_path=state_dir / "state.json",
        dry_run=dry_run,
    )


def _create_work(**overrides) -> ReferenceWork:
    defaults = dict(
        id="",
        type="book",
        title="Danmarks Basidiesvampe",
        short_label="Petersen 1990",
        authors_json=json.dumps([{"family": "Petersen"}]),
        year=1990,
    )
    defaults.update(overrides)
    return ReferenceWorkRepository.create(ReferenceWork(**defaults))


def _file_bytes(path: Path) -> bytes:
    return path.read_bytes() if path.exists() else b""


# --- Grouping ---------------------------------------------------------------


def test_groups_by_exact_normalized_source_string(libs, tmp_path):
    """AC-3: identical case-different source labels share a group; a
    completely different source is its OWN group. No fuzzy merging."""
    _, ref_path = libs
    a = _insert_legacy(ref_path, genus="Flammulina", species="fennae",
                       source="Ripkova et al, 2010")
    b = _insert_legacy(ref_path, genus="Flammulina", species="ononidis",
                       source="RIPKOVA et al, 2010")  # case variant
    c = _insert_legacy(ref_path, genus="Flammulina", species="velutipes",
                       source="Ripkova et al., 2010")  # different punctuation

    session = _make_session(ref_path, tmp_path)
    groups = session.load_source_groups()
    keys = [g.source_key for g in groups]
    # Case-only variant collapses to one key; punctuation variant is a
    # DIFFERENT key (exact string match, not fuzzy).
    assert "ripkova et al, 2010" in keys
    assert "ripkova et al., 2010" in keys
    grp_by_key = {g.source_key: g for g in groups}
    assert sorted(r.legacy_id for r in grp_by_key["ripkova et al, 2010"].rows) == [a, b]
    assert [r.legacy_id for r in grp_by_key["ripkova et al., 2010"].rows] == [c]


def test_source_group_contains_multiple_taxa(libs, tmp_path):
    """AC-4: a source group carries taxon names and legacy row IDs."""
    _, ref_path = libs
    ids = [
        _insert_legacy(ref_path, genus="Flammulina", species=sp,
                       source="Ripkova et al, 2010")
        for sp in ("fennae", "ononidis", "velutipes")
    ]
    session = _make_session(ref_path, tmp_path)
    groups = session.load_source_groups()
    assert len(groups) == 1
    group = groups[0]
    labels = [r.taxon_label for r in group.rows]
    assert labels == [
        "Flammulina fennae",
        "Flammulina ononidis",
        "Flammulina velutipes",
    ]
    assert sorted(r.legacy_id for r in group.rows) == sorted(ids)


# --- Candidate listing / selection ------------------------------------------


def test_list_work_candidates_uses_short_label_title_year_authors(libs, tmp_path):
    """AC-5: the candidate projection surfaces human-readable fields;
    the UUID is present as secondary/debug information but never on the
    normal candidate line."""
    _, ref_path = libs
    _create_work(title="Danmarks Basidiesvampe", short_label="Petersen 1990",
                 year=1990,
                 authors_json=json.dumps([{"family": "Petersen"}, {"family": "Knudsen"}]))
    session = _make_session(ref_path, tmp_path)
    cands = session.list_work_candidates()
    assert len(cands) == 1
    c = cands[0]
    assert c.short_label == "Petersen 1990"
    assert c.title == "Danmarks Basidiesvampe"
    assert c.year == 1990
    assert "Petersen" in c.authors_summary
    assert c.work_id  # UUID retained as secondary/debug info
    # Normal display: two lines, no UUID, no duplicated year.
    lines = c.display(1)
    joined = "\n".join(lines)
    assert "Petersen 1990" in joined
    assert "Danmarks Basidiesvampe" in joined
    assert c.work_id not in joined
    # Year 1990 must appear exactly once (it's already in the short label).
    assert joined.count("1990") == 1
    # Debug variant DOES include the UUID for support/troubleshooting.
    assert c.work_id in c.display_debug(1)


def test_candidate_search_filters_existing_works(libs, tmp_path):
    """AC-6 (search): a search string filters the visible candidate list."""
    _, ref_path = libs
    _create_work(title="Danmarks Basidiesvampe", short_label="Petersen 1990")
    _create_work(title="Studies on Russula", short_label="Author B 2001",
                 authors_json=json.dumps([{"family": "AuthorB"}]))
    session = _make_session(ref_path, tmp_path)
    filtered = session.list_work_candidates(query="Russula")
    assert len(filtered) == 1
    assert filtered[0].short_label == "Author B 2001"


def test_refresh_after_work_created_externally_finds_it(libs, tmp_path):
    """AC-6 (refresh): a candidate created via the desktop UI (or any
    external caller) is picked up by the next candidate query — the
    session holds no stale in-memory cache."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Flammulina", species="fennae",
                   source="Ripkova et al, 2010")
    session = _make_session(ref_path, tmp_path)
    assert session.list_work_candidates() == []
    _create_work(title="Ripková et al. (2010). Flammulina revised",
                 short_label="Ripková et al. 2010",
                 authors_json=json.dumps([{"family": "Ripková"}]))
    session.refresh_library()  # explicit hook — no-op but part of the contract
    cands = session.list_work_candidates()
    assert len(cands) == 1
    assert "Ripková" in cands[0].short_label


def test_refresh_preserves_prior_progress(libs, tmp_path):
    """AC-11 corollary: refreshing does not lose earlier decisions."""
    _, ref_path = libs
    a = _insert_legacy(ref_path, genus="Flammulina", species="fennae",
                       source="Src A")
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src B")
    session = _make_session(ref_path, tmp_path)
    session.skip_group("src a")
    session.mark_unresolved("src b")
    session.refresh_library()
    # Even after refresh, the two decisions are still in state.
    assert "src a" in session.state.skipped_sources
    assert "src b" in session.state.unresolved_sources


# --- Assign / migrate -------------------------------------------------------


def test_assign_group_migrates_all_rows_under_one_work(libs, tmp_path):
    """AC-8/AC-9: a source group is assigned to one work in a single
    operation; each legacy row becomes one MeasurementSet under the
    (created or reused) TaxonTreatment."""
    _, ref_path = libs
    ids = [
        _insert_legacy(
            ref_path, genus="Flammulina", species=sp,
            source="Ripkova et al, 2010",
            length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
        )
        for sp in ("fennae", "ononidis", "velutipes")
    ]
    work = _create_work(short_label="Ripková et al. 2010")
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work("ripkova et al, 2010", work.id)
    assert report.dry_run is False
    assert len(report.created) == 3
    # Each MeasurementSet stamped with the legacy id.
    for entry in report.created:
        ms = MeasurementSetRepository.get(entry["measurement_set_id"])
        assert ms is not None
        assert ms.legacy_reference_value_id in ids
        assert ms.length_min == 8.0
    # Three treatments were created — one per taxon name.
    conn = sqlite3.connect(ref_path)
    try:
        rows = conn.execute(
            "SELECT id, name_as_published FROM reference_taxon_treatments "
            "WHERE reference_work_id = ?",
            (work.id,),
        ).fetchall()
    finally:
        conn.close()
    names = sorted(r[1] for r in rows)
    assert names == [
        "Flammulina fennae",
        "Flammulina ononidis",
        "Flammulina velutipes",
    ]


def test_deselecting_row_before_migrate_excludes_it(libs, tmp_path):
    """AC-9: deselecting a legacy row keeps the group intact but excludes
    that row from the assign — for reused source strings that actually
    refer to different publications."""
    _, ref_path = libs
    a = _insert_legacy(
        ref_path, genus="Flammulina", species="fennae",
        source="Ripkova et al, 2010",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    b = _insert_legacy(
        ref_path, genus="Flammulina", species="velutipes",
        source="Ripkova et al, 2010",
        length_min=7.0, length_max=9.0, width_min=4.5, width_max=5.5,
    )
    work = _create_work()
    session = _make_session(ref_path, tmp_path)
    session.deselect_row("ripkova et al, 2010", b)
    report = session.assign_group_to_work("ripkova et al, 2010", work.id)
    migrated_ids = {entry["legacy_id"] for entry in report.created}
    assert migrated_ids == {a}
    # The deselected row is reported as skipped, not silently dropped.
    skipped_ids = [entry["legacy_id"] for entry in report.skipped]
    assert skipped_ids == [b]


def test_treatment_is_reused_when_second_row_shares_taxon_name(libs, tmp_path):
    """AC-8 treatment reuse: two legacy rows with the same taxon name
    under one work end up on ONE treatment, not two."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.0, length_max=10.0,
                   width_min=5.0, width_max=6.0, mount_medium="KOH")
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.5, length_max=10.5,
                   width_min=5.1, width_max=6.1, mount_medium="H2O")
    work = _create_work()
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work("src", work.id)
    assert len(report.created) == 2
    treatment_ids = {entry["treatment_id"] for entry in report.created}
    assert len(treatment_ids) == 1


def test_migration_is_idempotent_across_calls(libs, tmp_path):
    """AC-8 idempotency: assigning the same group twice reuses previously
    migrated rows instead of double-inserting."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.0, length_max=10.0,
                   width_min=5.0, width_max=6.0)
    work = _create_work()
    session = _make_session(ref_path, tmp_path)
    r1 = session.assign_group_to_work("src", work.id)
    r2 = session.assign_group_to_work("src", work.id)
    assert len(r1.created) == 1
    assert len(r2.created) == 0
    assert len(r2.reused) == 1


def test_null_legacy_values_remain_null(libs, tmp_path):
    """AC-8: nulls stay nulls; no fabricated means, no fake ranges."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_avg=9.0, width_avg=5.5)
    work = _create_work()
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work("src", work.id)
    ms = MeasurementSetRepository.get(report.created[0]["measurement_set_id"])
    assert ms is not None
    assert ms.length_min is None and ms.length_max is None
    assert ms.length_mean == 9.0 and ms.width_mean == 5.5
    assert ms.q_mean is None


def test_parmasto_values_and_source_preserved_in_notes(libs, tmp_path):
    """AC-8: parmasto values and the original source string travel into
    the MeasurementSet's notes as migration provenance, never dropped."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Parmasto, 1987", parmasto_length_mean=9.5)
    work = _create_work(short_label="Parmasto 1987",
                        title="Parmasto sample")
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work("parmasto, 1987", work.id)
    assert report.unsupported_fields  # reported, not dropped
    ms = MeasurementSetRepository.get(report.created[0]["measurement_set_id"])
    assert ms is not None
    assert ms.notes is not None
    assert "parmasto_length_mean=9.5" in ms.notes
    assert "Parmasto, 1987" in ms.notes  # legacy source preserved verbatim


def test_legacy_row_is_never_modified_by_apply(libs, tmp_path):
    """AC-8: the source legacy row must be identical before and after."""
    _, ref_path = libs
    legacy_id = _insert_legacy(
        ref_path, genus="Russula", species="paludosa", source="Src",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    work = _create_work()
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    before = dict(
        conn.execute("SELECT * FROM reference_values WHERE id = ?", (legacy_id,)).fetchone()
    )
    conn.close()
    session = _make_session(ref_path, tmp_path)
    session.assign_group_to_work("src", work.id)
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    after = dict(
        conn.execute("SELECT * FROM reference_values WHERE id = ?", (legacy_id,)).fetchone()
    )
    conn.close()
    assert before == after


def test_dry_run_makes_no_normalized_writes(libs, tmp_path):
    """AC-13: dry-run may save selections/state, but does not create any
    normalized measurement sets."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.0, length_max=10.0,
                   width_min=5.0, width_max=6.0)
    work = _create_work()
    session = _make_session(ref_path, tmp_path, dry_run=True)
    report = session.assign_group_to_work("src", work.id)
    assert report.dry_run is True
    assert len(report.created) == 1  # simulated only
    conn = sqlite3.connect(ref_path)
    try:
        real_count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert real_count == 0
    # But the binding IS saved so a subsequent apply can resume.
    assert session.state.source_bindings.get("src") == work.id


def test_missing_selected_work_detected_before_apply(libs, tmp_path):
    """AC-14: if the selected work has been deleted, the assign refuses
    to migrate rows and reports a failure."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.0, length_max=10.0,
                   width_min=5.0, width_max=6.0)
    work = _create_work()
    # Delete the normalized work outside the session.
    from database.reference_library import ReferenceWorkRepository as R
    R.delete(work.id)
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work("src", work.id)
    assert report.failed
    assert "no longer exists" in report.failed[0]["reason"]
    # No measurement sets were created.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


# --- Skip / unresolved / resume --------------------------------------------


def test_skip_and_unresolved_persist_across_sessions(libs, tmp_path):
    """AC-6/AC-10: skip and unresolved decisions survive a session
    restart. A new session picks up the same state file and does not
    re-ask the operator."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="A", species="a", source="Src A")
    _insert_legacy(ref_path, genus="B", species="b", source="Src B")
    _insert_legacy(ref_path, genus="C", species="c", source="Src C")
    s1 = _make_session(ref_path, tmp_path)
    s1.skip_group("src a")
    s1.mark_unresolved("src b")
    # New session over the same state file.
    s2 = migrate_tool.InteractiveMigrationSession(
        database_path=ref_path,
        state_path=s1.state_path,
        dry_run=False,
    )
    assert "src a" in s2.state.skipped_sources
    assert "src b" in s2.state.unresolved_sources
    pending_keys = [g.source_key for g in s2.pending_groups()]
    assert pending_keys == ["src c"]


def test_resume_after_partial_migration_does_not_duplicate(libs, tmp_path):
    """AC-11: on restart, rows already stamped with
    ``legacy_reference_value_id`` are auto-detected as migrated and
    re-migrating them yields ``reused`` (never a second insert)."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="Russula", species="paludosa",
                   source="Src", length_min=8.0, length_max=10.0,
                   width_min=5.0, width_max=6.0)
    work = _create_work()
    s1 = _make_session(ref_path, tmp_path)
    s1.assign_group_to_work("src", work.id)
    # Simulate an operator restart: build a fresh session from the same
    # state file.
    s2 = migrate_tool.InteractiveMigrationSession(
        database_path=ref_path,
        state_path=s1.state_path,
        dry_run=False,
    )
    # The group is no longer pending because every row is migrated.
    assert s2.pending_groups() == []
    # Explicitly re-assigning is idempotent.
    r = s2.assign_group_to_work("src", work.id)
    assert r.created == []
    assert len(r.reused) == 1
    conn = sqlite3.connect(ref_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 1


def test_already_migrated_rows_marked_on_load(libs, tmp_path):
    """AC-11: rows with legacy_reference_value_id set are surfaced as
    already-migrated in the group listing on the very first load,
    independent of any prior session state file."""
    _, ref_path = libs
    legacy_id = _insert_legacy(ref_path, genus="Russula", species="paludosa",
                               source="Src", length_min=8.0, length_max=10.0,
                               width_min=5.0, width_max=6.0)
    # Simulate a prior migration by stamping the legacy id onto a
    # measurement set directly.
    work = _create_work()
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(id="", reference_work_id=work.id,
                       name_as_published="Russula paludosa")
    )
    MeasurementSetRepository.create(
        MeasurementSet(
            id="", taxon_treatment_id=treatment.id, character="spore_size",
            data_kind="range",
            length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
            legacy_reference_value_id=legacy_id,
        )
    )
    # Fresh session, no state file yet.
    session = _make_session(ref_path, tmp_path)
    groups = session.load_source_groups()
    assert len(groups) == 1
    grp = groups[0]
    assert [r.legacy_id for r in grp.migrated_rows] == [legacy_id]
    assert grp.unmigrated_rows == []
    # Summary counts the migration correctly.
    s = session.summary()
    assert s["migrated"] == 1 and s["remaining"] == 0


# --- Summary + no-fuzzy invariant ------------------------------------------


def test_summary_reports_counts_and_next_source(libs, tmp_path):
    """AC-12: the summary command surfaces migrated/remaining/skipped/
    unresolved counts and the next pending source."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="A", species="a", source="Src A")
    _insert_legacy(ref_path, genus="B", species="b", source="Src A")
    _insert_legacy(ref_path, genus="C", species="c", source="Src B")
    _insert_legacy(ref_path, genus="D", species="d", source="Src C")
    session = _make_session(ref_path, tmp_path)
    session.skip_group("src b")
    s = session.summary()
    assert s["total_rows"] == 4
    assert s["migrated"] == 0
    assert s["remaining"] == 3  # A(2) + C(1); B skipped
    assert s["skipped_rows"] == 1
    assert s["next_source"] in ("Src A", "Src C")


def test_no_fuzzy_publication_match(libs, tmp_path):
    """AC-3: two nearly-identical legacy source strings that differ by
    even one character are separate groups and never auto-fused."""
    _, ref_path = libs
    _insert_legacy(ref_path, genus="A", species="a", source="Ripkova et al, 2010")
    _insert_legacy(ref_path, genus="B", species="b", source="Ripkova et al. 2010")
    session = _make_session(ref_path, tmp_path)
    groups = session.load_source_groups()
    keys = {g.source_key for g in groups}
    assert keys == {"ripkova et al, 2010", "ripkova et al. 2010"}
    # There is no method that would fuse them.
    assert not hasattr(session, "auto_merge_similar_sources")
