"""Focused tests for the Reference Library manager dialog.

The manager module imports PySide6 at module top-level, so ALL tests in
this file transitively require Qt. Guarding the whole module with
``pytest.importorskip`` makes the failure mode explicit ("skipped:
PySide6 unavailable") rather than a confusing ImportError at collection
time.

Coverage:

- Non-UI helpers (snapshot composition, plotability hint aligned with
  the translator's finite-positive rule, creatable data-kind set).
- Every form's ``_on_save`` path against an isolated repository,
  exercising UUID/revision semantics and empty-to-NULL conversion.
- The four visible manager states (empty, work-selected,
  treatment-selected, measurement-set-selected) including the empty-
  hierarchy placeholder.
- Case-insensitive publication search.
- The attach signal + chooser-owned manager lifecycle.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

# Skip the whole module cleanly when PySide6 is unavailable.
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
    """Fresh, isolated main+reference sqlite databases for each test."""
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


def _seed_work_treatment(libs, *, name="Russula paludosa"):
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Danmarks Basidiesvampe",
            short_label="Petersen 1990",
            authors_json=json.dumps([{"family": "Petersen"}]),
            year=1990,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published=name,
            locator_text="p. 214",
        )
    )
    return work, treatment


def _make_observation(db_path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-01-01", "Test"),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


# --- Qt-free tests ----------------------------------------------------------


def test_helper_snapshot_detail_uses_canonical_service(libs):
    """The manager renders measurement-set details by delegating to
    ``build_observation_reference_snapshot`` — verify the composed dict
    matches the raw repository state and does not fabricate values."""
    from ui.reference_library_manager_dialog import (
        _snapshot_measurement_details,
        _measurement_set_is_plottable_hint,
    )

    work, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="8–10 × 5–6 µm",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )

    snapshot = _snapshot_measurement_details(work, treatment, ms)

    assert snapshot["reference_measurement_set_id"] == ms.id
    assert snapshot["short_label"] == "Petersen 1990"
    assert snapshot["measurements"]["length_core_min"] == 8.0
    assert snapshot["measurements"]["length_core_max"] == 10.0
    # Fields not populated on the set must remain None in the snapshot.
    assert snapshot["measurements"]["length_mean"] is None
    assert snapshot["measurements"]["width_mean"] is None
    # Plot hint says drawable (core rectangle exists).
    assert _measurement_set_is_plottable_hint(ms) is True


def test_helper_plot_hint_rejects_no_bounds(libs):
    """A range set with no populated bounds/means must NOT report as
    plottable — the hint mirrors the translator's rule."""
    from ui.reference_library_manager_dialog import (
        _measurement_set_is_plottable_hint,
    )

    _, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="unspecified",
        )
    )
    assert _measurement_set_is_plottable_hint(ms) is False


def test_helper_plot_hint_raw_points_requires_finite_positive(libs):
    """raw_points must contain at least one finite, strictly positive
    paired point — an all-zero list is not plottable."""
    from ui.reference_library_manager_dialog import (
        _measurement_set_is_plottable_hint,
    )

    _, treatment = _seed_work_treatment(libs)
    bad = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="raw_points",
            raw_points_json=json.dumps(
                [{"length": 0.0, "width": 0.0}, {"length": -1.0, "width": 5.0}]
            ),
        )
    )
    good = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="raw_points",
            raw_points_json=json.dumps([{"length": 9.0, "width": 5.5}]),
        )
    )
    assert _measurement_set_is_plottable_hint(bad) is False
    assert _measurement_set_is_plottable_hint(good) is True


def test_creation_data_kinds_exclude_parmasto():
    """The manager form must not offer ``parmasto`` as a new-record kind."""
    from ui.reference_library_manager_dialog import _CREATABLE_DATA_KINDS

    assert "parmasto" not in _CREATABLE_DATA_KINDS
    assert set(_CREATABLE_DATA_KINDS) == {"range", "summary", "raw_points"}


def test_repository_create_and_update_preserve_uuid_and_bump_revision(libs):
    """Regression: the manager relies on repository update behavior — new
    records get revision=1; edits increment revision and preserve UUID."""
    work, treatment = _seed_work_treatment(libs)
    original_id = work.id
    assert work.revision == 1
    updated = ReferenceWorkRepository.update(
        work.id, {"title": "Danmarks Basidiesvampe (revised)"}
    )
    assert updated.id == original_id
    assert updated.revision == 2

    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="summary",
            raw_text="9 ± 0.5 × 5.5 ± 0.3",
            length_mean=9.0,
            width_mean=5.5,
        )
    )
    assert ms.revision == 1
    ms_uuid = ms.id
    ms_updated = MeasurementSetRepository.update(
        ms.id, {"length_mean": 9.1}
    )
    assert ms_updated.id == ms_uuid
    assert ms_updated.revision == 2
    assert ms_updated.length_mean == 9.1


# --- Qt fixture -------------------------------------------------------------


@pytest.fixture(scope="module")
def qapp():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_manager_empty_state_renders_without_exception(libs, qapp):
    """AC-01: constructing the dialog against an empty database shows an
    empty publications list and an empty selection kind."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        assert dialog.works_table.rowCount() == 0
        assert dialog.current_selection_kind() == "empty"
        # Attach button is hidden when no observation is provided.
        assert dialog.attach_btn.isHidden() is True
    finally:
        dialog.deleteLater()


def test_manager_populates_works_treatments_and_measurement_sets(libs, qapp):
    """AC-02/AC-03: after seeding a work + treatment + measurement set,
    the manager shows the row, populates the tree when the row is
    selected, and drills into treatment/measurement-set detail."""
    from PySide6.QtCore import Qt as _Qt

    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    work, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="8–10 × 5–6 µm",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        assert dialog.works_table.rowCount() == 1

        # Select the work.
        dialog.works_table.selectRow(0)
        assert dialog.current_selection_kind() == "work"
        # Verification badge was removed with the concept. The derived,
        # non-blocking completeness hint takes its place: the seeded work
        # has title/authors/year but no container/publisher, so the hint
        # is visible and reports exactly that missing field.
        assert dialog.completeness_hint_label.isHidden() is False
        assert "publication" in dialog.completeness_hint_label.text().lower()
        assert dialog.new_treatment_btn.isEnabled() is True

        # The tree now contains the treatment with its measurement set.
        assert dialog.hierarchy_tree.topLevelItemCount() == 1
        t_item = dialog.hierarchy_tree.topLevelItem(0)
        assert t_item.childCount() == 1

        # Select the treatment.
        dialog.hierarchy_tree.setCurrentItem(t_item)
        assert dialog.current_selection_kind() == "treatment"
        assert dialog.new_set_btn.isEnabled() is True

        # Select the measurement set.
        dialog.hierarchy_tree.setCurrentItem(t_item.child(0))
        assert dialog.current_selection_kind() == "measurement_set"
        text = dialog.detail_view.toPlainText()
        assert "range" in text
        assert "Length core min: 8.0" in text
        assert "Length core max: 10.0" in text
        # Plot hint hidden because bounds form a drawable rectangle.
        assert dialog.plot_hint_label.isHidden() is True
    finally:
        dialog.deleteLater()


def test_manager_search_filters_publications_by_query(libs, qapp):
    """AC-02: typing a query in the search box filters the list via the
    repository's case-insensitive search."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Danmarks Basidiesvampe",
            short_label="Petersen 1990",
            authors_json="[]",
            year=1990,
        )
    )
    ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="article",
            title="Studies on Russula",
            short_label="Author B 2001",
            authors_json="[]",
            year=2001,
        )
    )

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        assert dialog.works_table.rowCount() == 2
        dialog.search_input.setText("Petersen")
        # After the text change, refresh_works ran through the signal.
        assert dialog.works_table.rowCount() == 1
        assert dialog.works_table.item(0, 0).text() == "Petersen 1990"
        dialog.search_input.setText("")
        assert dialog.works_table.rowCount() == 2
    finally:
        dialog.deleteLater()


def test_manager_plot_hint_shows_when_set_is_not_plottable(libs, qapp):
    """AC-07 UI hint: a measurement set with no drawable geometry shows
    a translated 'not plottable yet' hint in the detail panel."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    _, treatment = _seed_work_treatment(libs)
    MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="unspecified",
        )
    )

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        dialog.works_table.selectRow(0)
        t_item = dialog.hierarchy_tree.topLevelItem(0)
        dialog.hierarchy_tree.setCurrentItem(t_item.child(0))
        assert dialog.plot_hint_label.isHidden() is False
        assert (
            "not plottable yet" in dialog.plot_hint_label.text().lower()
            or "plottable" in dialog.plot_hint_label.text().lower()
        )
    finally:
        dialog.deleteLater()


def test_manager_attach_requested_signal_emits_when_observation_active(libs, qapp):
    """AC-08: with a positive ``active_observation_id`` and a
    measurement-set selection, clicking Attach emits
    ``attach_requested(measurement_set_id, role)`` and closes the
    dialog. The dialog itself never mutates observation_reference_uses."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="8–10 × 5–6 µm",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )

    dialog = ReferenceLibraryManagerDialog(
        None, active_observation_id=int(obs_id)
    )
    emitted: list[tuple[str, str, int]] = []
    dialog.attach_requested.connect(
        lambda set_id, role, observation_id: emitted.append(
            (set_id, role, observation_id)
        )
    )
    try:
        dialog.works_table.selectRow(0)
        t_item = dialog.hierarchy_tree.topLevelItem(0)
        dialog.hierarchy_tree.setCurrentItem(t_item.child(0))
        assert dialog.attach_btn.isEnabled() is True
        # Click.
        dialog.attach_btn.click()
        # Dialog closed and signal fired with the observation id captured
        # at manager-open time.
        assert emitted == [(ms.id, "compared", int(obs_id))]
        # The dialog itself did NOT mutate observation_reference_uses.
        assert ObservationReferenceUseRepository.list_for_observation(obs_id) == []
    finally:
        dialog.deleteLater()


def test_attach_dialog_refresh_candidates_picks_up_new_sets(libs, qapp):
    """AC-09 sub-check: after new measurement sets are added to the
    library (e.g. inside the manager), calling ``refresh_candidates()``
    re-queries and repopulates the chooser table."""
    from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog

    _, treatment = _seed_work_treatment(libs)
    dialog = ReferenceLibraryAttachDialog(None)
    try:
        assert hasattr(dialog, "manage_library_btn")
        assert dialog.table.rowCount() == 0

        # Create a new measurement set outside the dialog.
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="range",
                raw_text="8–10 × 5–6 µm",
                length_core_min=8.0,
                length_core_max=10.0,
                width_core_min=5.0,
                width_core_max=6.0,
            )
        )

        dialog.refresh_candidates()
        assert dialog.table.rowCount() == 1
    finally:
        dialog.deleteLater()


def test_measurement_set_form_hides_raw_points_for_range(libs, qapp):
    """AC-07: the measurement-set form only shows raw-points editing when
    the selected data_kind is ``raw_points``; blank numeric fields never
    fabricate zero values."""
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    _, treatment = _seed_work_treatment(libs)
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        # The combo now displays translated labels but stores enum values
        # as item data. Default is the first entry in _CREATABLE_DATA_KINDS.
        assert form.data_kind_combo.currentData() == "range"
        assert form.raw_points_input.isHidden() is True
        assert form.length_min_input.isHidden() is False

        # Switch to raw_points and confirm visibility flips.
        idx = form.data_kind_combo.findData("raw_points")
        assert idx >= 0
        form.data_kind_combo.setCurrentIndex(idx)
        assert form.raw_points_input.isHidden() is False
        assert form.length_min_input.isHidden() is True

        # Empty numeric inputs -> None in the payload (no fabricated zeroes).
        form.data_kind_combo.setCurrentIndex(form.data_kind_combo.findData("range"))
        payload = form._collect()
        assert payload["length_min"] is None
        assert payload["length_core_min"] is None
        assert payload["length_mean"] is None
        assert payload["width_mean"] is None
        assert payload["sample_size"] is None
    finally:
        form.deleteLater()


def test_form_save_paths_and_form_regressions(libs, qapp):
    """AC-05/AC-06/AC-07: drive each form's ``_on_save`` against isolated
    repositories. Covers UUID/revision semantics on create + edit, the
    F-001 raw_points regression (aggregate stats preserved on edit),
    empty-to-NULL conversion, and repository validation surfacing via
    the form's error label."""
    from ui.reference_library_manager_dialog import (
        _MeasurementSetForm,
        _ReferenceWorkForm,
        _TaxonTreatmentForm,
    )

    # --- work form: create then edit ------------------------------------
    work_form = _ReferenceWorkForm(None)
    try:
        idx = work_form.type_combo.findData("book")
        assert idx >= 0
        work_form.type_combo.setCurrentIndex(idx)
        work_form.title_input.setText("Danmarks Basidiesvampe")
        work_form.short_label_input.setText("Petersen 1990")
        work_form.year_input.setText("1990")
        work_form.citation_key_input.setText("petersen-1990")
        work_form.volume_input.setText("2")
        work_form._on_save()
        assert work_form.result_work is not None
        assert work_form.result_work.revision == 1
        assert work_form.result_work.citation_key == "petersen-1990"
        assert work_form.result_work.volume == "2"
        work_id = work_form.result_work.id
    finally:
        work_form.deleteLater()

    # Edit the same work -> UUID preserved, revision bumped, extra fields kept.
    persisted_work = ReferenceWorkRepository.get(work_id)
    assert persisted_work is not None
    edit_form = _ReferenceWorkForm(None, work=persisted_work)
    try:
        edit_form.title_input.setText("Danmarks Basidiesvampe (rev)")
        edit_form._on_save()
        assert edit_form.result_work is not None
        assert edit_form.result_work.id == work_id
        assert edit_form.result_work.revision == 2
        assert edit_form.result_work.citation_key == "petersen-1990"
    finally:
        edit_form.deleteLater()

    # --- treatment form: create + edit ---------------------------------
    t_form = _TaxonTreatmentForm(None, reference_work_id=work_id)
    try:
        t_form.name_input.setText("Russula paludosa")
        t_form.page_from_input.setText("210")
        t_form.page_to_input.setText("220")
        t_form.locator_input.setText("p. 214")
        t_form._on_save()
        assert t_form.result_treatment is not None
        assert t_form.result_treatment.revision == 1
        assert t_form.result_treatment.page_from == 210
        treatment_id = t_form.result_treatment.id
    finally:
        t_form.deleteLater()

    persisted_treatment = TaxonTreatmentRepository.get(treatment_id)
    t_edit_form = _TaxonTreatmentForm(
        None, reference_work_id=work_id, treatment=persisted_treatment
    )
    try:
        t_edit_form.locator_input.setText("pp. 214-215")
        t_edit_form._on_save()
        assert t_edit_form.result_treatment is not None
        assert t_edit_form.result_treatment.id == treatment_id
        assert t_edit_form.result_treatment.revision == 2
        assert t_edit_form.result_treatment.locator_text == "pp. 214-215"
    finally:
        t_edit_form.deleteLater()

    # --- measurement set: create range with blanks -> None ---------------
    ms_form = _MeasurementSetForm(None, taxon_treatment_id=treatment_id)
    try:
        ms_form.raw_text_input.setText("8–10 × 5–6 µm")
        ms_form.length_core_min_input.setText("8.0")
        ms_form.length_core_max_input.setText("10.0")
        ms_form.width_core_min_input.setText("5.0")
        ms_form.width_core_max_input.setText("6.0")
        # length_mean / width_mean intentionally blank
        ms_form._on_save()
        assert ms_form.result_set is not None
        result = ms_form.result_set
        assert result.revision == 1
        assert result.data_kind == "range"
        assert result.length_core_min == 8.0
        assert result.length_mean is None
        assert result.width_mean is None
        assert result.sample_size is None
        ms_id = result.id
    finally:
        ms_form.deleteLater()

    # Edit range set -> UUID preserved, revision bumped, blank-remains-None.
    persisted_ms = MeasurementSetRepository.get(ms_id)
    ms_edit_form = _MeasurementSetForm(
        None, taxon_treatment_id=treatment_id, measurement_set=persisted_ms
    )
    try:
        ms_edit_form.length_mean_input.setText("9.0")
        ms_edit_form._on_save()
        assert ms_edit_form.result_set is not None
        assert ms_edit_form.result_set.id == ms_id
        assert ms_edit_form.result_set.revision == 2
        assert ms_edit_form.result_set.length_mean == 9.0
    finally:
        ms_edit_form.deleteLater()

    # --- F-001 regression: editing a raw_points set must not clobber
    # previously stored aggregate statistics (they are hidden in the UI).
    raw_ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment_id,
            character="spore_size",
            data_kind="raw_points",
            raw_text="raw-points source",
            raw_points_json=json.dumps([{"length": 9.0, "width": 5.5}]),
            length_mean=9.0,
            width_mean=5.0,
            sample_size=12,
        )
    )
    raw_edit = _MeasurementSetForm(
        None,
        taxon_treatment_id=treatment_id,
        measurement_set=MeasurementSetRepository.get(raw_ms.id),
    )
    try:
        # User only changes the notes; aggregate stats are hidden.
        raw_edit.notes_input.setPlainText("edited note")
        raw_edit._on_save()
        assert raw_edit.result_set is not None
        preserved = raw_edit.result_set
        assert preserved.id == raw_ms.id
        assert preserved.revision == 2
        assert preserved.length_mean == 9.0, (
            "F-001 regression: raw_points edit silently blanked length_mean"
        )
        assert preserved.width_mean == 5.0
        assert preserved.sample_size == 12
        assert preserved.notes == "edited note"
    finally:
        raw_edit.deleteLater()


def test_form_surfaces_validation_error_from_repository(libs, qapp):
    """Repository validation errors surface in the form's error label
    without accepting the dialog."""
    from ui.reference_library_manager_dialog import _ReferenceWorkForm

    form = _ReferenceWorkForm(None)
    try:
        form.title_input.setText("")  # empty title fails repository validation
        idx = form.type_combo.findData("book")
        form.type_combo.setCurrentIndex(idx)
        form._on_save()
        assert form.result_work is None
        # ``isHidden`` (not isVisible) — the parent dialog is not shown in
        # this test, so isVisible is always False even after setVisible(True).
        assert form.error_label.isHidden() is False
        assert form.error_label.text() != ""
    finally:
        form.deleteLater()


def test_hierarchy_shows_empty_placeholder_when_no_work_selected(libs, qapp):
    """AC-01 refinement: the treatments pane displays a disabled
    placeholder row when no work is selected and when a selected work
    has no treatments."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        # No work selected -> placeholder row present.
        assert dialog.hierarchy_tree.topLevelItemCount() == 1
        placeholder = dialog.hierarchy_tree.topLevelItem(0)
        assert not (placeholder.flags() & PySide6.QtCore.Qt.ItemIsSelectable)
    finally:
        dialog.deleteLater()

    # Seed a work with NO treatments -> another placeholder appears.
    ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Empty Work",
            short_label="Empty 2020",
            authors_json="[]",
            year=2020,
        )
    )
    dialog2 = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        dialog2.works_table.selectRow(0)
        assert dialog2.hierarchy_tree.topLevelItemCount() == 1
        placeholder = dialog2.hierarchy_tree.topLevelItem(0)
        assert not (placeholder.flags() & PySide6.QtCore.Qt.ItemIsSelectable)
        text = placeholder.text(0)
        assert "treatment" in text.lower()
    finally:
        dialog2.deleteLater()


def test_plot_hint_rejects_infinity_and_negative_values(libs):
    """F-006 regression: the plot hint must apply finite-positive checks,
    not just ordering. NaN, infinity, zero and negative values must fail
    exactly like the translator's finite-positive rule."""
    import math as _math

    from ui.reference_library_manager_dialog import (
        _measurement_set_is_plottable_hint,
    )

    _, treatment = _seed_work_treatment(libs)

    negative = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_core_min=-10.0,
            length_core_max=-5.0,
            width_core_min=-6.0,
            width_core_max=-3.0,
        )
    )
    assert _measurement_set_is_plottable_hint(negative) is False

    infinite = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_core_min=1.0,
            length_core_max=float("inf"),
            width_core_min=1.0,
            width_core_max=2.0,
        )
    )
    assert _measurement_set_is_plottable_hint(infinite) is False

    # Mean pair with a NaN element is not plottable either.
    nan_mean = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="summary",
            length_mean=_math.nan,
            width_mean=5.0,
        )
    )
    assert _measurement_set_is_plottable_hint(nan_mean) is False


def test_attach_dialog_manage_button_opens_manager_and_refreshes(libs, qapp, monkeypatch):
    """F-007 regression: the chooser now owns the "Manage library…"
    lifecycle. Clicking it opens the manager as a child modal AND calls
    ``refresh_candidates()`` on close — even without any external
    signal wiring from a parent MainWindow."""
    from ui import reference_library_attach_dialog as attach_module
    from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog

    _, treatment = _seed_work_treatment(libs)
    # Stub the manager so exec() returns immediately without a modal loop,
    # but simulates the "user created a new set inside the manager" case
    # by inserting a candidate into the database during exec().
    exec_calls: list[int] = []

    class _StubManager:
        def __init__(self, parent, *, active_observation_id=None):
            self._parent = parent
            exec_calls.append(1)

        def exec(self):
            MeasurementSetRepository.create(
                MeasurementSet(
                    id="",
                    taxon_treatment_id=treatment.id,
                    character="spore_size",
                    data_kind="range",
                    raw_text="stub",
                    length_core_min=1.0,
                    length_core_max=2.0,
                    width_core_min=1.0,
                    width_core_max=2.0,
                )
            )
            return 1

        def deleteLater(self):
            pass

    monkeypatch.setattr(
        "ui.reference_library_manager_dialog.ReferenceLibraryManagerDialog",
        _StubManager,
        raising=True,
    )

    dialog = ReferenceLibraryAttachDialog(None)
    fired: list[bool] = []
    dialog.manage_library_requested.connect(lambda: fired.append(True))
    try:
        assert dialog.table.rowCount() == 0
        dialog.manage_library_btn.click()
        # The manager was opened by the dialog itself...
        assert exec_calls == [1]
        # ...the manage_library_requested signal was still emitted for
        # external observers...
        assert fired == [True]
        # ...and the chooser refreshed its candidate table without any
        # external caller invoking refresh_candidates.
        assert dialog.table.rowCount() == 1
    finally:
        dialog.deleteLater()


def test_main_window_shared_attach_helper_is_reachable_from_both_dialogs(qapp):
    """Regression: MainWindow exposes a single private helper for the
    post-selection attach path so the attachment chooser and the manager
    signal enter identical rollback/warning-row semantics."""
    from ui import main_window as main_window_module

    assert hasattr(
        main_window_module.MainWindow,
        "_attach_normalized_reference_to_active_observation",
    )
    assert hasattr(
        main_window_module.MainWindow, "_on_manage_reference_library_clicked"
    )
    assert hasattr(
        main_window_module.MainWindow,
        "_on_manage_reference_library_from_attach",
    )
    assert hasattr(
        main_window_module.MainWindow,
        "_attach_normalized_reference_from_manager",
    )


# --- Adversarial-review regression tests -----------------------------------


def test_editing_non_raw_set_preserves_hidden_raw_points_json(libs, qapp):
    """Adversarial T-2 (data_loss): editing a range/summary record must
    not silently wipe any raw_points_json already stored on that row.
    Only an explicit kind conversion (from raw_points -> non-raw) clears
    the JSON."""
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    _, treatment = _seed_work_treatment(libs)
    hybrid = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="hybrid record",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
            # Hybrid: schema allows both aggregate bounds AND raw JSON.
            raw_points_json=json.dumps([{"length": 9.0, "width": 5.5}]),
        )
    )
    form = _MeasurementSetForm(
        None,
        taxon_treatment_id=treatment.id,
        measurement_set=hybrid,
    )
    try:
        # User only changes the notes; raw_points_json is hidden for range.
        form.notes_input.setPlainText("just edit the notes")
        form._on_save()
        assert form.result_set is not None
        preserved = form.result_set
        assert preserved.id == hybrid.id
        assert preserved.raw_points_json is not None
        assert preserved.raw_points_json == hybrid.raw_points_json, (
            "Adversarial regression: editing a range record silently "
            "wiped raw_points_json"
        )
    finally:
        form.deleteLater()


def test_explicit_kind_conversion_from_raw_points_clears_raw_json(libs, qapp):
    """Companion to the previous test: a genuine kind conversion FROM
    raw_points TO a non-raw kind DOES clear the JSON. This is intended
    behavior for a real conversion, not silent data loss."""
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    _, treatment = _seed_work_treatment(libs)
    existing = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="raw_points",
            raw_text="original raw",
            raw_points_json=json.dumps([{"length": 9.0, "width": 5.5}]),
        )
    )
    form = _MeasurementSetForm(
        None,
        taxon_treatment_id=treatment.id,
        measurement_set=existing,
    )
    try:
        # Explicitly convert to range.
        idx = form.data_kind_combo.findData("range")
        assert idx >= 0
        form.data_kind_combo.setCurrentIndex(idx)
        form.length_core_min_input.setText("8.0")
        form.length_core_max_input.setText("10.0")
        form.width_core_min_input.setText("5.0")
        form.width_core_max_input.setText("6.0")
        form._on_save()
        assert form.result_set is not None
        result = form.result_set
        assert result.data_kind == "range"
        # Genuine conversion -> clearing raw_points_json is intended.
        assert result.raw_points_json is None
    finally:
        form.deleteLater()


def test_attach_race_returns_existing_row_instead_of_raw_sqlite_error(libs):
    """Adversarial T-3 (retries): if a second caller loses the unique-
    index race between the SELECT and INSERT in ``_do_attach``, the
    caller must observe the winning row as an ``(existing, False)``
    tuple instead of a raw sqlite3 exception."""
    import sqlite3 as _sqlite3

    from database import reference_library as reflib_module

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )

    # First attach — creates the row normally.
    first, created_first = (
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), ms.id, role="compared"
        )
    )
    assert created_first is True

    # Simulate the race: patch the "existing row" SELECT to always return
    # None on the FIRST invocation so INSERT is attempted, then let the
    # unique-index constraint trip. The repository must recover by
    # re-reading the winning row and returning it as (existing, False).
    real_connect = reflib_module._connect_observations
    call_count = {"n": 0}

    class _RacingConnection:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            if (
                "SELECT * FROM observation_reference_uses" in sql
                and "WHERE observation_id" in sql
                and call_count["n"] == 0
            ):
                call_count["n"] += 1

                class _EmptyCursor:
                    def fetchone(self_inner):
                        return None

                return _EmptyCursor()
            return self._inner.execute(sql, params)

        def commit(self):
            return self._inner.commit()

        def close(self):
            return self._inner.close()

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def _fake_connect():
        return _RacingConnection(real_connect())

    reflib_module._connect_observations = _fake_connect
    try:
        second, created_second = (
            ObservationReferenceUseRepository.attach_with_status(
                int(obs_id), ms.id, role="compared"
            )
        )
    finally:
        reflib_module._connect_observations = real_connect

    # No raw sqlite error escaped; the caller sees the pre-existing row.
    assert created_second is False
    assert second.id == first.id


def test_attach_dialog_manage_signal_emits_after_manager_closes(
    libs, qapp, monkeypatch
):
    """Adversarial T-7 (external_api): the ``manage_library_requested``
    signal must fire exactly once and AFTER the manager closes, so a
    consumer that also opens a manager in the slot cannot open a second
    window while the chooser's own manager is already displayed."""
    from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog

    exec_order: list[str] = []

    class _StubManager:
        def __init__(self, *args, **kwargs):
            exec_order.append("manager_ctor")

        def exec(self):
            exec_order.append("manager_exec")
            return 1

        def deleteLater(self):
            exec_order.append("manager_delete")

    monkeypatch.setattr(
        "ui.reference_library_manager_dialog.ReferenceLibraryManagerDialog",
        _StubManager,
        raising=True,
    )

    dialog = ReferenceLibraryAttachDialog(None)
    dialog.manage_library_requested.connect(
        lambda: exec_order.append("signal")
    )
    try:
        dialog.manage_library_btn.click()
        # Manager was constructed, exec'd, then deleted BEFORE the signal.
        assert exec_order == [
            "manager_ctor",
            "manager_exec",
            "manager_delete",
            "signal",
        ]
    finally:
        dialog.deleteLater()


def test_main_window_refuses_attach_when_observation_drifted(libs, qapp):
    """Adversarial T-5 (authorization): if the active observation drifts
    between the manager opening and the attach click, MainWindow's
    manager-signal handler must refuse to attach the reference onto a
    different observation than the one captured at open time."""
    from types import MethodType, SimpleNamespace

    from ui import main_window as main_window_module

    db_path, _ = libs
    a = _make_observation(db_path)
    b = _make_observation(db_path)
    _, treatment = _seed_work_treatment(libs)
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )

    stub = SimpleNamespace()
    stub.active_observation_id = b  # drifted since manager opened on `a`
    stub.tr = lambda self, text, *args, **kwargs: text
    stub.tr = MethodType(stub.tr, stub)

    warnings: list[tuple[str, ...]] = []

    class _FakeMessageBox:
        @staticmethod
        def warning(*args, **kwargs):
            warnings.append(("warning", args, kwargs))

    monkey_qmb = _FakeMessageBox

    # Also stub the shared attach helper to assert it is NOT called.
    attach_calls: list[tuple] = []

    def _fake_attach(self, ms_id, role):
        attach_calls.append((ms_id, role))

    stub._attach_normalized_reference_to_active_observation = MethodType(
        _fake_attach, stub
    )
    stub._attach_normalized_reference_from_manager = MethodType(
        main_window_module.MainWindow._attach_normalized_reference_from_manager,
        stub,
    )

    # Patch QMessageBox in the module namespace.
    original_qmb = main_window_module.QMessageBox
    main_window_module.QMessageBox = monkey_qmb
    try:
        stub._attach_normalized_reference_from_manager(ms.id, "compared", int(a))
    finally:
        main_window_module.QMessageBox = original_qmb

    # The mismatched observation must NOT have triggered the shared helper.
    assert attach_calls == []
    # A warning surfaced to the user.
    assert warnings and warnings[0][0] == "warning"
