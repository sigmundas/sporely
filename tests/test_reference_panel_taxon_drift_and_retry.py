"""Adversarial-follow-up regressions for the reference panel Add coordinator.

Covers two high-severity concerns surfaced by the adversarial review:

* Taxon-drift guard — if the panel-editable genus/species differs from
  the observation's stored (genus, species), the coordinator must
  require an explicit confirmation before creating the normalized
  treatment (which binds ``name_as_published`` to the panel text but
  ``taxon_id`` to the observation). Confirmation preserves the ability
  to record a historical/synonym name-as-published; refusal keeps the
  legacy row but blocks normalized persistence so an accidental panel
  edit cannot slip through.

* Retry idempotency — the legacy ``ReferenceDB.set_reference`` performs
  DELETE + INSERT, so a retry produces a new legacy_reference_value_id.
  Without a coordinator-level idempotency check, that retry would
  create a second normalized MeasurementSet and strand the previous
  one. The coordinator must reuse an existing compatible MeasurementSet
  on the same (treatment, character, data_kind) key and refresh its
  fields (including the new legacy_reference_value_id) instead of
  inserting a duplicate. Attachment itself is already idempotent per
  ObservationReferenceUseRepository.attach_with_status.
"""
from __future__ import annotations

import json
import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QLabel,
    QLineEdit,
    QMessageBox,
    QTableWidget,
    QWidget,
)

import ui.main_window as main_window
from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatmentRepository,
)


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


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


class _EmptySpeciesAvailability:
    def get_cache(self, force_refresh: bool = False):
        return {}

    def get_detailed_info(self, genus, species, exclude_observation_id=None):
        return {}

    def get_species_display_name(self, genus, species, exclude_observation_id=None):
        return (f"{genus} {species}".strip(), False)


class _StubDialog(QWidget):
    """A stand-in for ReferenceAddDialog that returns a preset payload."""

    def __init__(self, payload, *args, **kwargs):
        super().__init__()
        self._payload = payload

    def exec(self):
        from PySide6.QtWidgets import QDialog

        return QDialog.Accepted

    def result_data(self):
        return self._payload

    def delete_requested(self):
        return False

    def normalized_measurement_set_payload(self, *, legacy_reference_value_id=None):
        rng = self._payload.get("_normalized_range")
        if not rng:
            return None
        return MeasurementSet(
            id="",
            taxon_treatment_id="",
            character="spore_size",
            data_kind="range",
            raw_text=rng.get("raw_text"),
            length_min=rng.get("length_min"),
            length_max=rng.get("length_max"),
            width_min=rng.get("width_min"),
            width_max=rng.get("width_max"),
            length_mean=rng.get("length_mean"),
            width_mean=rng.get("width_mean"),
            legacy_reference_value_id=legacy_reference_value_id,
        )


def _build_window(monkeypatch, qapp):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _EmptySpeciesAvailability)
    monkeypatch.setattr(
        main_window.SettingsDB, "get_setting", lambda key, default=None: default
    )
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    window = main_window.MainWindow()
    window.ref_vernacular_input = QLineEdit()
    window.ref_vernacular_label = QLabel()
    window.ref_genus_input = QLineEdit()
    window.ref_species_input = QLineEdit()
    window.ref_source_input = QComboBox()
    window.ref_source_input.setEditable(True)
    window.ref_source_input.addItem("")
    window.table = QTableWidget(3, 5)
    window.ref_table = window.table
    window.reference_values = {}
    window.reference_series = []
    window.species_availability = _EmptySpeciesAvailability()
    window._refresh_reference_species_availability = lambda: None
    window._populate_reference_panel_sources = lambda auto_select_single=True: None
    window._apply_reference_panel_values = lambda data: None
    window._update_reference_add_state = lambda: None
    window._clean_ref_genus_text = lambda t: (t or "").strip()
    window._clean_ref_species_text = lambda t: (t or "").strip()
    return window


def _make_observation(
    db_path,
    *,
    genus: str,
    species: str,
    sporely_taxon_id: int = 7,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location, genus, species, sporely_taxon_id) "
            "VALUES (?, ?, ?, ?, ?)",
            ("2026-01-01", "TX-drift", genus, species, sporely_taxon_id),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def _seed_work(short_label: str = "TXWK 2021") -> ReferenceWork:
    return ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title=short_label,
            short_label=short_label,
            year=2021,
        )
    )


def _range_payload(work_id: str, taxon_id: int, panel_genus: str, panel_species: str):
    return {
        "genus": panel_genus,
        "species": panel_species,
        "source_kind": "reference",
        "source": "TXWK 2021 (2021)",
        "reference_work_id": work_id,
        "sporely_taxon_id": taxon_id,
        "length_min": 5.5,
        "length_max": 8.5,
        "width_min": 3.0,
        "width_max": 5.0,
        "_normalized_range": {
            "raw_text": "5.5-8.5 x 3-5",
            "length_min": 5.5,
            "length_max": 8.5,
            "width_min": 3.0,
            "width_max": 5.0,
        },
    }


def test_taxon_drift_confirmation_declined_skips_normalized_write(
    monkeypatch, qapp, libs
):
    """When the panel-entered species differs from the observation's
    stored (genus, species) and the user declines the confirmation
    prompt, the legacy row is still written but no MeasurementSet or
    ObservationReferenceUse is created.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Mycena", species="filopes", sporely_taxon_id=7
    )
    work = _seed_work()
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    # Panel edited to a completely different species.
    window.ref_genus_input.setText("Amanita")
    window.ref_species_input.setText("muscaria")
    window._active_sporely_taxon_id = lambda: 7

    payload = _range_payload(work.id, 7, "Amanita", "muscaria")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    # User clicks "No".
    monkeypatch.setattr(
        main_window.QMessageBox, "question", lambda *a, **kw: QMessageBox.No
    )
    window._on_reference_panel_add_clicked()

    # Legacy row was still written (upstream of the guard).
    from database.models import ReferenceDB

    assert ReferenceDB.get_reference(
        "Amanita", "muscaria", payload["source"], None, None
    ) is not None
    # But no normalized MeasurementSet was created and no attachment.
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert listed == []


def test_taxon_drift_confirmation_accepted_records_as_published_synonym(
    monkeypatch, qapp, libs
):
    """When the panel-entered species differs from the observation's
    stored (genus, species) but the user confirms, the coordinator
    proceeds and creates a treatment whose ``name_as_published`` reflects
    the panel text — preserving the legitimate historical/synonym use
    case.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Mycena", species="filopes", sporely_taxon_id=7
    )
    work = _seed_work("TXWK-A 2022")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Prunulus")  # historical synonym
    window.ref_species_input.setText("filopes")
    window._active_sporely_taxon_id = lambda: 7

    payload = _range_payload(work.id, 7, "Prunulus", "filopes")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    monkeypatch.setattr(
        main_window.QMessageBox, "question", lambda *a, **kw: QMessageBox.Yes
    )
    window._on_reference_panel_add_clicked()

    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    ms = MeasurementSetRepository.get(listed[0].reference_measurement_set_id)
    assert ms is not None
    treatment = TaxonTreatmentRepository.get(ms.taxon_treatment_id)
    assert treatment is not None
    # taxon_id still bound to the observation's identity, but the
    # as-published name reflects the user-confirmed synonym.
    assert str(treatment.taxon_id) == "7"
    assert treatment.name_as_published == "Prunulus filopes"


def test_taxon_matches_observation_no_confirmation_prompt(monkeypatch, qapp, libs):
    """When the panel-entered (genus, species) matches the observation's
    stored identity (case-insensitive), the confirmation prompt is
    skipped entirely and the coordinator proceeds straight through.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("TXWK-B 2023")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("agaricus")  # differs only in case
    window.ref_species_input.setText("Bisporus")
    window._active_sporely_taxon_id = lambda: 7

    payload = _range_payload(work.id, 7, "agaricus", "Bisporus")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    prompted: list[bool] = []

    def _q(*a, **kw):
        prompted.append(True)
        return QMessageBox.No

    monkeypatch.setattr(main_window.QMessageBox, "question", _q)
    window._on_reference_panel_add_clicked()

    assert prompted == [], "no confirmation prompt should be shown when identities match"
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1


def test_retry_after_attachment_failure_no_stranded_set(
    monkeypatch, qapp, libs
):
    """First attempt: legacy row + treatment + MeasurementSet are all
    created; attachment simulated as failing. The compensating cleanup
    must delete the just-created MeasurementSet (and the just-created
    treatment) so nothing is stranded pointing at the legacy row that
    a retry will DELETE+INSERT away. On retry, exactly one fresh
    treatment + one MeasurementSet + one attachment exist and the
    MeasurementSet's legacy_reference_value_id points at the retry's
    live legacy row.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("TXWK-C 2024")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7

    payload = _range_payload(work.id, 7, "Agaricus", "bisporus")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    # Silence the modal QMessageBox that _on_reference_panel_add_clicked
    # shows when _persist_normalized_reference_from_dialog raises — the
    # simulated attachment failure below is a deliberate control-flow
    # event, not an error the test needs to surface.
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    def _direct_attach(set_id, role):
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), str(set_id), role=str(role)
        )

    def _failing_attach(set_id, role):
        raise RuntimeError("simulated attachment failure")

    # First attempt: attachment fails after the MeasurementSet is
    # created; compensating cleanup deletes the set + fresh treatment.
    window._attach_normalized_reference_to_active_observation = _failing_attach
    window._on_reference_panel_add_clicked()

    treatments_after_fail = TaxonTreatmentRepository.list_for_work(work.id)
    assert treatments_after_fail == [], (
        "compensating cleanup should remove the newly created treatment "
        "when its only MeasurementSet is being rolled back"
    )
    assert ObservationReferenceUseRepository.list_for_observation(obs_id) == []
    # The just-created legacy row is preserved (legacy behavior is
    # intentional in this feature), but no normalized artifacts remain.
    from database.models import ReferenceDB

    assert ReferenceDB.get_reference(
        "Agaricus", "bisporus", payload["source"], None, None
    ) is not None

    # Second attempt (retry) with a working attach.
    window._attach_normalized_reference_to_active_observation = _direct_attach
    window._on_reference_panel_add_clicked()

    treatments_second = TaxonTreatmentRepository.list_for_work(work.id)
    assert len(treatments_second) == 1
    sets_second = MeasurementSetRepository.list_for_treatment(treatments_second[0].id)
    assert len(sets_second) == 1
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    assert listed[0].reference_measurement_set_id == sets_second[0].id
    # legacy_reference_value_id points at the retry's fresh legacy row.
    assert sets_second[0].legacy_reference_value_id is not None


def test_observation_drift_between_dialog_open_and_save_blocks_normalized_write(
    monkeypatch, qapp, libs
):
    """The dialog captures ``observation_id`` at open time. If the
    active observation changes while the modal is up (a programmatic
    switch, a background reload), the coordinator must refuse to
    commit against the current-active observation — otherwise the
    scope-A selection could be attached to observation B (confused
    deputy). The legacy row was already saved upstream and remains.
    """
    db_path, _ = libs
    obs_id_a = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    obs_id_b = _make_observation(
        db_path, genus="Boletus", species="edulis", sporely_taxon_id=8
    )
    work = _seed_work("OBS-DRIFT 2024")
    window = _build_window(monkeypatch, qapp)
    # Dialog opens on observation A.
    window.active_observation_id = obs_id_a
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    payload = _range_payload(work.id, 7, "Agaricus", "bisporus")
    # Dialog carries the observation_id it was constructed with.
    payload["observation_id"] = obs_id_a

    # Between construction and the dialog-accepted branch, an external
    # actor switches the active observation. We simulate that by
    # flipping active_observation_id right before returning the dialog.
    class _DriftingDialog(_StubDialog):
        def exec(self):
            from PySide6.QtWidgets import QDialog

            window.active_observation_id = obs_id_b
            return QDialog.Accepted

    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _DriftingDialog(payload, *a, **kw),
    )

    def _direct_attach(set_id, role):
        # Use the CURRENT active_observation_id, mirroring the real
        # helper's behaviour, so a drift bug would surface as an
        # attachment on obs_id_b.
        ObservationReferenceUseRepository.attach_with_status(
            int(window.active_observation_id), str(set_id), role=str(role)
        )

    window._attach_normalized_reference_to_active_observation = _direct_attach
    window._on_reference_panel_add_clicked()

    # Legacy row was still written under the panel's genus/species.
    from database.models import ReferenceDB

    assert ReferenceDB.get_reference(
        "Agaricus", "bisporus", payload["source"], None, None
    ) is not None
    # Crucially: neither observation received a normalized attachment.
    assert ObservationReferenceUseRepository.list_for_observation(obs_id_a) == []
    assert ObservationReferenceUseRepository.list_for_observation(obs_id_b) == []
    assert TaxonTreatmentRepository.list_for_work(work.id) == []


def test_two_same_shape_submissions_on_same_observation_are_not_conflated(
    monkeypatch, qapp, libs
):
    """A second same-shape (treatment, character, data_kind) submission
    on the SAME observation must NOT overwrite the first attached
    MeasurementSet — the coarse tuple cannot prove retry identity and
    conflating the two would silently mutate an already-attached
    dataset. The coordinator therefore always creates a fresh
    MeasurementSet; the user is expected to detach or delete the
    previous one via the library manager if it was actually a retry.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("NO-CONFLATE 2024")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    payload_a = _range_payload(work.id, 7, "Agaricus", "bisporus")
    payload_a["source"] = "NO-CONFLATE 2024 A"
    payload_a["_normalized_range"] = {
        **payload_a["_normalized_range"], "length_min": 5.5, "length_max": 8.5,
    }
    payload_b = _range_payload(work.id, 7, "Agaricus", "bisporus")
    payload_b["source"] = "NO-CONFLATE 2024 B"
    payload_b["_normalized_range"] = {
        **payload_b["_normalized_range"], "length_min": 6.0, "length_max": 9.0,
    }

    def _direct_attach(set_id, role):
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), str(set_id), role=str(role)
        )

    window._attach_normalized_reference_to_active_observation = _direct_attach

    # First submission.
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload_a, *a, **kw),
    )
    window._on_reference_panel_add_clicked()
    # Second submission with same treatment+character+data_kind but
    # different values — must not overwrite the first.
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload_b, *a, **kw),
    )
    window._on_reference_panel_add_clicked()

    treatments = TaxonTreatmentRepository.list_for_work(work.id)
    assert len(treatments) == 1
    sets = MeasurementSetRepository.list_for_treatment(treatments[0].id)
    assert len(sets) == 2
    length_mins = sorted(s.length_min for s in sets)
    assert length_mins == pytest.approx([5.5, 6.0])
    uses = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(uses) == 2


def test_silent_attach_failure_still_triggers_compensating_cleanup(
    monkeypatch, qapp, libs
):
    """If the attach helper returns without raising but no
    ``observation_reference_uses`` row was actually created (matching
    the helper's real behavior of catching ReferenceLibraryError and
    unplottable-snapshot cases), the coordinator must detect the
    missing use and roll back the just-created MeasurementSet + fresh
    treatment.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("SILENT-FAIL 2024")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    payload = _range_payload(work.id, 7, "Agaricus", "bisporus")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    # Simulate the shared attach helper's silent-failure return:
    # returns None without raising and without inserting a use row.
    window._attach_normalized_reference_to_active_observation = (
        lambda set_id, role: None
    )
    window._on_reference_panel_add_clicked()

    treatments = TaxonTreatmentRepository.list_for_work(work.id)
    assert treatments == [], (
        "compensating cleanup should remove the fresh treatment when "
        "attach silently fails"
    )
    # No measurement sets stranded anywhere for this treatment work.
    for t in treatments:
        assert MeasurementSetRepository.list_for_treatment(t.id) == []
    assert ObservationReferenceUseRepository.list_for_observation(obs_id) == []


def test_two_distinct_datasets_on_same_treatment_are_not_conflated(
    monkeypatch, qapp, libs
):
    """A legitimate second range dataset on the same treatment (both
    fully attached, both linked to live legacy rows) must NOT be
    removed by the compensating-cleanup sweep. This proves the
    cleanup targets only orphan prior-attempt sets and does not
    clobber unrelated datasets.
    """
    db_path, _ = libs
    obs_id_a = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    obs_id_b = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("TWO-SETS 2024")
    window = _build_window(monkeypatch, qapp)
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    def _direct_attach_factory(obs_id):
        def _attach(set_id, role):
            ObservationReferenceUseRepository.attach_with_status(
                int(obs_id), str(set_id), role=str(role)
            )
        return _attach

    # Two distinct submissions with different measurement values — the
    # second must not overwrite the first, because the first is live
    # (its legacy row is intact and it is attached to observation A).
    payload_a = _range_payload(work.id, 7, "Agaricus", "bisporus")
    payload_a["source"] = "TWO-SETS 2024 A"
    payload_a["_normalized_range"] = {
        **payload_a["_normalized_range"], "length_min": 5.5, "length_max": 8.5,
    }
    payload_b = _range_payload(work.id, 7, "Agaricus", "bisporus")
    payload_b["source"] = "TWO-SETS 2024 B"
    payload_b["_normalized_range"] = {
        **payload_b["_normalized_range"], "length_min": 6.0, "length_max": 9.0,
    }

    # Submission A on observation A.
    window.active_observation_id = obs_id_a
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload_a, *a, **kw),
    )
    window._attach_normalized_reference_to_active_observation = _direct_attach_factory(
        obs_id_a
    )
    window._on_reference_panel_add_clicked()

    # Submission B on observation B — reuses the treatment (same
    # work+taxon) but must produce a SECOND MeasurementSet without
    # cleaning up A's set.
    window.active_observation_id = obs_id_b
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload_b, *a, **kw),
    )
    window._attach_normalized_reference_to_active_observation = _direct_attach_factory(
        obs_id_b
    )
    window._on_reference_panel_add_clicked()

    treatments = TaxonTreatmentRepository.list_for_work(work.id)
    assert len(treatments) == 1
    sets = MeasurementSetRepository.list_for_treatment(treatments[0].id)
    assert len(sets) == 2, sets
    # Both original values survive — neither was overwritten.
    length_mins = sorted(s.length_min for s in sets)
    assert length_mins == pytest.approx([5.5, 6.0])
    # Each observation has exactly one attachment, pointing at its own set.
    uses_a = ObservationReferenceUseRepository.list_for_observation(obs_id_a)
    uses_b = ObservationReferenceUseRepository.list_for_observation(obs_id_b)
    assert len(uses_a) == 1
    assert len(uses_b) == 1
    assert uses_a[0].reference_measurement_set_id != uses_b[0].reference_measurement_set_id


def test_retry_after_measurement_set_failure_creates_no_duplicate_treatment(
    monkeypatch, qapp, libs
):
    """If MeasurementSet creation fails on the first attempt, the
    treatment is still created. On retry, the coordinator must reuse
    that treatment (via list_for_work/matching-taxon lookup) and add
    exactly one MeasurementSet, not a second treatment.
    """
    db_path, _ = libs
    obs_id = _make_observation(
        db_path, genus="Agaricus", species="bisporus", sporely_taxon_id=7
    )
    work = _seed_work("TXWK-D 2025")
    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._active_sporely_taxon_id = lambda: 7

    payload = _range_payload(work.id, 7, "Agaricus", "bisporus")
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(payload, *a, **kw),
    )
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )

    def _direct_attach(set_id, role):
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), str(set_id), role=str(role)
        )

    window._attach_normalized_reference_to_active_observation = _direct_attach
    # Cause MeasurementSet creation to fail on first attempt.
    original_create = MeasurementSetRepository.create

    class _OneShotFailer:
        def __init__(self):
            self.failed = False

        def __call__(self, ms):
            if not self.failed:
                self.failed = True
                raise RuntimeError("simulated set-create failure")
            return original_create(ms)

    failer = _OneShotFailer()
    monkeypatch.setattr(MeasurementSetRepository, "create", failer)
    monkeypatch.setattr(
        main_window.QMessageBox, "warning", lambda *a, **kw: QMessageBox.Ok
    )
    window._on_reference_panel_add_clicked()

    # The set-create failure triggers compensating cleanup that also
    # removes the freshly-created treatment (only artifacts created by
    # this attempt are compensated). Legacy row remains.
    treatments = TaxonTreatmentRepository.list_for_work(work.id)
    assert treatments == []

    # Retry: the failer permits the second call through. A fresh
    # treatment is created, then a MeasurementSet, then attachment.
    window._on_reference_panel_add_clicked()
    treatments_after = TaxonTreatmentRepository.list_for_work(work.id)
    assert len(treatments_after) == 1
    sets_after = MeasurementSetRepository.list_for_treatment(treatments_after[0].id)
    assert len(sets_after) == 1
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    assert listed[0].reference_measurement_set_id == sets_after[0].id
