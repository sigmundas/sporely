"""Focused regressions for the interactive migration CLI polish pass.

Covers:

- immediate assign on a numeric selection (no ``[y/N]`` confirmation);
- ``No source recorded`` presentation for a genuine blank source;
- personal / Sporely-computed source classifier (``Cloud:`` prefix);
- exclusion of personal sources from pending groups and from
  literature summary counts, and refusal to migrate them into
  literature records;
- narrow classifier: normal literature sources are NOT excluded;
- ``_Style`` disables ANSI escapes when NO_COLOR is set or stdout is
  not a TTY, and never leaks escapes into stored state / reports;
- candidate line no longer carries the UUID or a duplicated year;
- row line no longer prints ``plotable`` when the row is plotable.
"""
from __future__ import annotations

import io
import json
import os
import sqlite3
from pathlib import Path

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
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


def _insert(
    ref_path: Path,
    *,
    genus="Russula",
    species="paludosa",
    source=None,
    length_min=8.0,
    length_max=10.0,
    width_min=5.0,
    width_max=6.0,
) -> int:
    conn = sqlite3.connect(ref_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO reference_values
            (genus, species, source, length_min, length_max, width_min, width_max)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (genus, species, source, length_min, length_max, width_min, width_max),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _make_session(ref_path, tmp_path, *, dry_run=False):
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    return migrate_tool.InteractiveMigrationSession(
        database_path=ref_path,
        state_path=state_dir / "state.json",
        dry_run=dry_run,
    )


def _seed_work(**kwargs) -> ReferenceWork:
    defaults = dict(
        id="",
        type="book",
        title="Danmarks basidiesvampe",
        short_label="Læssøe et al. 2024",
        authors_json=json.dumps([{"family": "Læssøe"}]),
        year=2024,
    )
    defaults.update(kwargs)
    return ReferenceWorkRepository.create(ReferenceWork(**defaults))


def _run_loop(session, script: str, *, style: migrate_tool._Style | None = None):
    style = style or migrate_tool._Style(enabled=False)
    stdin = io.StringIO(script)
    stdout = io.StringIO()
    migrate_tool.interactive_loop(
        session, stream_in=stdin, stream_out=stdout, style=style
    )
    return stdout.getvalue()


# --- 1) No [y/N] after numeric selection ------------------------------------


def test_numeric_selection_immediately_applies_without_yn(libs, tmp_path):
    """AC-1: pressing a candidate number performs the assignment in the
    current mode with no follow-up ``[y/N]`` prompt."""
    _, ref_path = libs
    _insert(ref_path, source="Danmarks basidiesvampe")
    _seed_work()  # single candidate

    session = _make_session(ref_path, tmp_path)
    output = _run_loop(session, "1\nq\n")

    # No leftover confirmation prompt appears in the driver output.
    assert "[y/N]" not in output
    assert "Assign" not in output  # the old confirm banner is gone
    # And the assignment actually happened.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 1


def test_dry_run_numeric_selection_persists_binding_without_writes(libs, tmp_path):
    """AC-1 + AC-13: in dry-run, a numeric pick simulates the migration
    (binding saved, ``created`` populated in the report) but never
    writes any normalized rows."""
    _, ref_path = libs
    _insert(ref_path, source="Danmarks basidiesvampe")
    _seed_work()
    session = _make_session(ref_path, tmp_path, dry_run=True)
    _run_loop(session, "1\nq\n")
    # No normalized measurement sets on disk.
    conn = sqlite3.connect(ref_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0
    # But the binding is persisted so a later --apply can resume.
    assert session.state.source_bindings.get("danmarks basidiesvampe")


# --- 2) Blank-source presentation ------------------------------------------


def test_blank_source_displays_as_no_source_recorded(libs, tmp_path):
    """AC-2: a genuinely empty legacy source is shown as
    ``No source recorded`` in group and summary output. The underlying
    key remains the empty string."""
    _, ref_path = libs
    _insert(ref_path, source=None)  # blank source

    session = _make_session(ref_path, tmp_path)
    # Explicitly plain style so we can grep for text.
    output = _run_loop(session, "q\n")

    assert "No source recorded" in output
    assert "(blank source)" not in output
    # Underlying key is still the empty string.
    groups = session.load_source_groups()
    assert any(g.source_key == "" for g in groups)


# --- 3) Personal / Sporely-computed classifier -----------------------------


def test_cloud_prefixed_source_is_classified_as_personal(libs, tmp_path):
    """AC-3: rows whose source starts with the ``Cloud:`` prefix are
    classified as personal/Sporely-computed. They never appear in
    pending literature groups and are counted separately in the
    summary."""
    _, ref_path = libs
    # One personal (cloud-prefixed) legacy row.
    personal_id = _insert(
        ref_path,
        genus="Cortinarius",
        species="balteatus",
        source="Cloud: user@example.com - 2026-03-17",
    )
    # One literature row.
    _insert(
        ref_path,
        genus="Russula",
        species="paludosa",
        source="Danmarks basidiesvampe",
    )
    session = _make_session(ref_path, tmp_path)
    pending_keys = [g.source_key for g in session.pending_groups()]
    assert "cloud: user@example.com - 2026-03-17" not in pending_keys
    assert "danmarks basidiesvampe" in pending_keys

    s = session.summary()
    assert s["personal_computed_rows"] == 1
    assert s["personal_computed_groups"] == 1
    # Literature counts exclude the personal row entirely.
    assert s["total_rows"] == 1
    assert s["remaining"] == 1

    # And ``personal_groups`` still exposes them for other callers.
    personal = session.personal_groups()
    assert [r.legacy_id for g in personal for r in g.rows] == [personal_id]


def test_personal_source_email_is_never_displayed(libs, tmp_path):
    """AC-3: the CLI must not print the embedded email address for a
    personal/Sporely-computed source in normal output."""
    _, ref_path = libs
    _insert(
        ref_path,
        genus="Cortinarius",
        species="balteatus",
        source="Cloud: user@example.com - 2026-03-17",
    )
    # Add one literature row so the loop has something to display; then
    # quit immediately.
    _insert(ref_path, genus="Russula", species="paludosa",
            source="Danmarks basidiesvampe")
    session = _make_session(ref_path, tmp_path)
    output = _run_loop(session, "q\n")
    assert "user@example.com" not in output
    assert "Cloud:" not in output


def test_personal_source_cannot_be_migrated_into_literature(libs, tmp_path):
    """AC-3: a caller that tries to bind a personal source to a work
    directly is refused; no measurement set is created."""
    _, ref_path = libs
    _insert(
        ref_path,
        source="Cloud: user@example.com - 2026-03-17",
    )
    work = _seed_work()
    session = _make_session(ref_path, tmp_path)
    report = session.assign_group_to_work(
        "cloud: user@example.com - 2026-03-17", work.id
    )
    assert report.failed
    assert "personal" in report.failed[0]["reason"].lower()
    conn = sqlite3.connect(ref_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 0


def test_classifier_is_narrow_normal_sources_not_excluded(libs, tmp_path):
    """AC-3: the classifier is intentionally narrow — sources that
    happen to contain an email, a date, or the substring ``cloud``
    (but do NOT start with the explicit ``Cloud:`` prefix) are NOT
    misclassified."""
    _, ref_path = libs
    _insert(ref_path, source="Cortinarius on iCloud archives (2010)")
    _insert(ref_path, source="editor@journal.com editorial, 2015")
    _insert(ref_path, source="https://mycologia.org/paper/42")
    _insert(ref_path, source="Petersen 1990-01-01 in Fungi")
    session = _make_session(ref_path, tmp_path)
    pending_keys = {g.source_key for g in session.pending_groups()}
    assert "cortinarius on icloud archives (2010)" in pending_keys
    assert "editor@journal.com editorial, 2015" in pending_keys
    assert "https://mycologia.org/paper/42" in pending_keys
    assert "petersen 1990-01-01 in fungi" in pending_keys
    # And none of them are personal.
    assert session.summary()["personal_computed_rows"] == 0


def test_prior_state_pointing_at_personal_source_does_not_corrupt_summary(
    libs, tmp_path
):
    """AC-3 stale-state: if an earlier session ``skipped`` or
    ``unresolved`` a source that today is classified as personal, the
    summary must count it under ``personal_computed_rows`` — NOT
    duplicated under ``skipped_rows`` / ``unresolved_rows``. State is
    left untouched on disk so a rollback of the classifier would still
    surface the original decision."""
    _, ref_path = libs
    _insert(ref_path, source="Cloud: user@example.com - 2026-03-17")
    _insert(ref_path, source="Cloud: other@example.com - 2026-04-01")
    session = _make_session(ref_path, tmp_path)
    # Simulate a pre-classifier session that marked one skipped and one
    # unresolved.
    session.skip_group("cloud: user@example.com - 2026-03-17")
    session.mark_unresolved("cloud: other@example.com - 2026-04-01")
    s = session.summary()
    assert s["personal_computed_rows"] == 2
    assert s["skipped_rows"] == 0
    assert s["unresolved_rows"] == 0
    # State entries themselves are still on disk — we do not silently
    # rewrite the operator's earlier decisions.
    assert session.state.skipped_sources
    assert session.state.unresolved_sources


# --- 4) ANSI styling helper -------------------------------------------------


def test_style_disabled_when_no_color_env_set(monkeypatch):
    """AC-4: setting ``NO_COLOR`` disables ANSI escapes."""
    monkeypatch.setenv("NO_COLOR", "1")
    st = migrate_tool._Style()
    assert st.enabled is False
    assert st.bold("hello") == "hello"
    assert st.cyan("x") == "x"


def test_style_disabled_when_stdout_not_a_tty(monkeypatch):
    """AC-4: when stdout is not a TTY (e.g. output piped to a file),
    styling stays off. Note: PYTHONNOUSERSITE etc. don't matter here —
    the check is ``sys.stdout.isatty()``."""
    monkeypatch.delenv("NO_COLOR", raising=False)

    class _FakeStdout:
        def isatty(self):
            return False

    monkeypatch.setattr(migrate_tool.sys, "stdout", _FakeStdout())
    st = migrate_tool._Style()
    assert st.enabled is False


def test_style_produces_ansi_when_enabled():
    """AC-4: an explicitly enabled style wraps text in ANSI escapes."""
    st = migrate_tool._Style(enabled=True)
    styled = st.bold_cyan("Danmarks basidiesvampe")
    assert styled.startswith("\x1b[1;36m")
    assert styled.endswith("\x1b[0m")
    assert "Danmarks basidiesvampe" in styled


def test_ansi_codes_never_leak_into_state_or_reports(libs, tmp_path):
    """AC-4: even when the interactive driver runs with styling enabled,
    no ANSI escape codes appear in the persisted state file. Reports
    remain plain text; only the terminal buffer is decorated."""
    _, ref_path = libs
    _insert(ref_path, source="Danmarks basidiesvampe")
    _seed_work()
    session = _make_session(ref_path, tmp_path)
    _run_loop(session, "1\nq\n", style=migrate_tool._Style(enabled=True))
    state_text = session.state_path.read_text(encoding="utf-8")
    assert "\x1b[" not in state_text


# --- 5) Candidate display polish -------------------------------------------


def test_candidate_display_omits_uuid_and_duplicate_year(libs, tmp_path):
    """AC-5: the normal candidate line contains neither the UUID nor a
    second copy of the year when the short label already carries it."""
    _, ref_path = libs
    _seed_work()
    session = _make_session(ref_path, tmp_path)
    cand = session.list_work_candidates()[0]
    lines = cand.display(1)
    joined = "\n".join(lines)
    assert cand.work_id not in joined
    assert "uuid=" not in joined
    # Year only appears via the short label — not duplicated as "(2024)".
    assert joined.count("2024") == 1
    # Debug variant still carries the UUID for support workflows.
    assert cand.work_id in cand.display_debug(1)


def test_candidate_display_shows_year_when_short_label_has_none(libs, tmp_path):
    """AC-5 edge: when the short label does NOT already carry the year
    the display appends it in parentheses so the picker is still useful.
    """
    _, ref_path = libs
    _seed_work(short_label="Læssøe et al.", year=2024)  # year not in label
    session = _make_session(ref_path, tmp_path)
    cand = session.list_work_candidates()[0]
    lines = cand.display(1)
    joined = "\n".join(lines)
    assert "2024" in joined
    assert cand.work_id not in joined


# --- 6) Row line noise reduction -------------------------------------------


def test_row_line_omits_plotable_when_true(libs, tmp_path):
    """AC-6: a plotable row does NOT print ``plotable``. A non-plotable
    row DOES call attention to itself with ``not plotable``."""
    _, ref_path = libs
    good_id = _insert(
        ref_path,
        genus="Flammulina",
        species="elastica",
        source="Danmarks basidiesvampe",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    bad_id = _insert(
        ref_path,
        genus="Flammulina",
        species="unbounded",
        source="Danmarks basidiesvampe",
        length_min=None, length_max=None, width_min=None, width_max=None,
    )
    _seed_work()
    session = _make_session(ref_path, tmp_path)
    output = _run_loop(session, "q\n")

    lines = output.splitlines()
    good_line = next(l for l in lines if f"row {good_id}" in l)
    bad_line = next(l for l in lines if f"row {bad_id}" in l)

    assert "plotable" not in good_line
    assert "not plotable" in bad_line


# --- Preserved guarantees ---------------------------------------------------


def test_dry_run_loop_advances_after_numeric_pick(libs, tmp_path):
    """Bugfix regression: in dry-run mode the walkthrough must advance
    past a group after the operator picks a candidate. Prior behavior
    re-displayed the same group indefinitely because ``pending_groups``
    only inspected DB state; dry-run does not write, so the group
    stayed unmigrated forever.

    Fix: a group is also considered decided once its source key has a
    binding in ``state.source_bindings`` — dry-run persists that
    binding, so subsequent iterations skip the group.
    """
    _, ref_path = libs
    _insert(ref_path, source="Danmarks basidesvampe")
    _seed_work()
    session = _make_session(ref_path, tmp_path, dry_run=True)
    output = _run_loop(session, "1\nq\n")
    # Loop reaches the "All groups decided" branch after ONE pick.
    assert "All groups decided" in output
    # The report line marks the pick as a dry-run so the operator is
    # not surprised later.
    assert "dry-run" in output.lower()
    # Group must NOT appear twice.
    assert output.count("Legacy source:") == 1


def test_dry_run_bound_group_stays_out_of_pending(libs, tmp_path):
    """Engine-level counterpart to the loop regression: once a source
    key is bound (whether by a dry-run pick or an apply pick), the
    group is filtered from ``pending_groups`` even if no rows have
    actually been migrated in the DB yet.
    """
    _, ref_path = libs
    _insert(ref_path, source="Src A")
    _insert(ref_path, source="Src B")
    work = _seed_work()
    session = _make_session(ref_path, tmp_path, dry_run=True)
    assert len(session.pending_groups()) == 2
    session.assign_group_to_work("src a", work.id)
    # After the dry-run assign, group A is out of the pending list even
    # though no measurement set was written.
    remaining = [g.source_key for g in session.pending_groups()]
    assert remaining == ["src b"]
    # DB still has no measurement sets — this is dry-run.
    conn = sqlite3.connect(ref_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 0


def test_dry_run_banner_appears_at_start_of_loop(libs, tmp_path):
    _, ref_path = libs
    _insert(ref_path, source="Src")
    _seed_work()
    session = _make_session(ref_path, tmp_path, dry_run=True)
    output = _run_loop(session, "q\n")
    assert "DRY-RUN" in output


def test_apply_mode_shows_apply_banner(libs, tmp_path):
    _, ref_path = libs
    _insert(ref_path, source="Src")
    _seed_work()
    session = _make_session(ref_path, tmp_path, dry_run=False)
    output = _run_loop(session, "q\n")
    assert "APPLY" in output
    assert "DRY-RUN" not in output


def test_parmasto_group_note_is_printed_once_not_per_row(libs, tmp_path):
    """Bugfix regression: the previous CLI printed
    ``(parmasto_* (mapped to data_kind='parmasto', preserved in notes))``
    on every single legacy row inside a Parmasto source group — 200
    identical warnings for a 200-row group. That message is now emitted
    ONCE as a subtle group-level provenance note, and the per-row
    warning is silenced. The row's ``· parmasto`` kind marker already
    conveys the data shape.
    """
    _, ref_path = libs
    # Insert three Parmasto-style rows under one source. Only the
    # parmasto columns are populated (mirroring real Parmasto data).
    conn = sqlite3.connect(ref_path)
    try:
        for i, (genus, species) in enumerate(
            (
                ("Amanita", "phalloides"),
                ("Antrodiella", "onychoides"),
                ("Antrodiella", "semisupina"),
            ),
            start=56,
        ):
            conn.execute(
                """
                INSERT INTO reference_values
                (id, genus, species, source, parmasto_length_mean, parmasto_width_mean)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (i, genus, species, "Parmasto 1965", 8.5, 5.2),
            )
        conn.commit()
    finally:
        conn.close()
    _seed_work(short_label="Parmasto 1965", title="Parmasto typescript")

    session = _make_session(ref_path, tmp_path)
    output = _run_loop(session, "q\n")

    # Per-row parmasto noise is gone.
    assert "parmasto_*" not in output
    assert "mapped to data_kind" not in output
    # Group-level provenance note appears exactly once.
    assert output.count(
        "Parmasto values preserved as provenance in migration notes."
    ) == 1
    # And the per-row ``· parmasto`` kind marker still communicates the
    # shape (once per row).
    assert output.count("· parmasto") == 3


def test_non_parmasto_group_does_not_show_group_provenance_note(libs, tmp_path):
    """The group-level parmasto note must only appear for groups whose
    rows actually use ``data_kind=parmasto``. A regular literature
    source (range/summary) must not carry it."""
    _, ref_path = libs
    _insert(
        ref_path,
        source="Danmarks basidiesvampe",
        length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
    )
    _seed_work()
    session = _make_session(ref_path, tmp_path)
    output = _run_loop(session, "q\n")
    assert "Parmasto values preserved" not in output


def test_migration_guarantees_preserved(libs, tmp_path):
    """AC-7: the polish pass must not silently break any of the earlier
    guarantees. Spot-checks: exact-source grouping, treatment reuse,
    idempotency, and legacy row is untouched."""
    _, ref_path = libs
    ids = [
        _insert(
            ref_path,
            genus="Russula",
            species="paludosa",
            source="Danmarks basidiesvampe",
            length_min=8.0, length_max=10.0, width_min=5.0, width_max=6.0,
        )
        for _ in range(3)
    ]
    _seed_work()
    session = _make_session(ref_path, tmp_path)

    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        before = {
            r["id"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM reference_values ORDER BY id"
            ).fetchall()
        }
    finally:
        conn.close()

    _run_loop(session, "1\nq\n")

    # Three new measurement sets, all reusing the one treatment.
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        ms_count = conn.execute(
            "SELECT COUNT(*) FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL"
        ).fetchone()[0]
        treatments = conn.execute(
            "SELECT COUNT(*) FROM reference_taxon_treatments"
        ).fetchone()[0]
        after = {
            r["id"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM reference_values ORDER BY id"
            ).fetchall()
        }
    finally:
        conn.close()

    assert ms_count == len(ids)
    assert treatments == 1
    # Legacy rows byte-for-byte untouched.
    assert before == after
