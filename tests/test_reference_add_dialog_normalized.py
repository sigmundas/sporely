"""Regression tests for ReferenceAddDialog's normalized-library payload.

These tests exercise :meth:`ReferenceAddDialog.normalized_measurement_set_payload`
and the surrounding publication picker / data-choice controls without
running the full MainWindow. They rely on the repository's isolated
mushrooms.db / reference_values.db fixture pattern established in
tests/test_reference_library_repository.py.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QTableWidgetItem, QWidget

from database import schema as _schema
from database.reference_library import (
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
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


def _seed_work(short_label: str = "Petersen 1990") -> ReferenceWork:
    return ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title=short_label,
            short_label=short_label,
            year=1990,
        )
    )


def _new_dialog(qapp, **kwargs):
    from ui.main_window import ReferenceAddDialog

    parent = QWidget()
    dialog = ReferenceAddDialog(parent, "Agaricus", "bisporus", **kwargs)
    return parent, dialog


def _fill_minmax_cell(dialog, row: int, col: int, value: float) -> None:
    item = QTableWidgetItem(f"{value:.2f}")
    dialog.minmax_table.setItem(row, col, item)


def test_normalized_payload_supports_name_only_treatment_without_taxon(qapp, libs):
    parent, dialog = _new_dialog(qapp, observation_id=42)
    try:
        _fill_minmax_cell(dialog, 0, 0, 6.0)
        _fill_minmax_cell(dialog, 0, 4, 10.0)
        _fill_minmax_cell(dialog, 1, 0, 3.0)
        _fill_minmax_cell(dialog, 1, 4, 5.0)
        assert dialog.normalized_measurement_set_payload() is not None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_range_uses_migration_mapping(qapp, libs):
    parent, dialog = _new_dialog(qapp, observation_id=42, sporely_taxon_id=7)
    try:
        # Length row 0: extreme min/max at cols 0/4, typical bounds at cols 1/3, mean at col 2
        _fill_minmax_cell(dialog, 0, 0, 5.5)
        _fill_minmax_cell(dialog, 0, 1, 6.2)
        _fill_minmax_cell(dialog, 0, 2, 7.0)
        _fill_minmax_cell(dialog, 0, 3, 7.8)
        _fill_minmax_cell(dialog, 0, 4, 8.5)
        _fill_minmax_cell(dialog, 1, 0, 3.0)
        _fill_minmax_cell(dialog, 1, 4, 5.0)
        ms = dialog.normalized_measurement_set_payload(legacy_reference_value_id=99)
        assert ms is not None
        assert ms.data_kind == "range"
        assert ms.length_min == pytest.approx(5.5)
        assert ms.length_core_min == pytest.approx(6.2)
        assert ms.length_mean == pytest.approx(7.0)
        assert ms.length_core_max == pytest.approx(7.8)
        assert ms.length_max == pytest.approx(8.5)
        assert ms.legacy_reference_value_id == 99
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_quick_add_treatment_fields_prefill_and_preserve_locator(qapp, libs):
    parent, dialog = _new_dialog(qapp, observation_id=42, sporely_taxon_id=7)
    try:
        assert dialog.name_as_published_input.text() == "Agaricus bisporus"
        dialog.name_as_published_input.setText("Agaricus campestris var. bisporus")
        dialog.locator_input.setText("p. 42, fig. 3")

        assert dialog.quick_add_treatment_payload() == {
            "name_as_published": "Agaricus campestris var. bisporus",
            "locator_text": "p. 42, fig. 3",
        }
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_new_publication_is_kept_as_unpersisted_draft(qapp, libs, monkeypatch):
    from PySide6.QtWidgets import QDialog
    from ui import reference_library_manager_dialog as manager

    draft = ReferenceWork(
        id="", type="article", title="Draft source", short_label="Draft 2026"
    )

    class _DraftEditor:
        def __init__(self, _parent, *, persist_on_accept):
            assert persist_on_accept is False
            self.result_work = draft

        def exec(self):
            return QDialog.Accepted

        def deleteLater(self):
            pass

    monkeypatch.setattr(manager, "ReferenceWorkEditor", _DraftEditor)
    parent, dialog = _new_dialog(qapp, observation_id=42, sporely_taxon_id=7)
    try:
        dialog._on_new_publication_clicked()

        assert dialog.pending_reference_work() is draft
        dialog.reject()
        assert ReferenceWorkRepository.search("Draft source") == []
        dialog.publication_combo.setEditText("Different typed source")
        assert dialog.pending_reference_work() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_preserves_verbatim_measurement_expression(qapp, libs):
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        raw = "  (9–)10–12 × 5–6 µm  "
        dialog.measurement_paste_input.setText(raw)
        dialog._on_parse_measurement_clicked()

        payload = dialog.normalized_measurement_set_payload()

        assert payload is not None
        assert payload.raw_text == raw
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_parser_failure_produces_no_normalized_payload_or_records(qapp, libs):
    parent, dialog = _new_dialog(
        qapp, observation_id=42, sporely_taxon_id=7
    )
    try:
        dialog.measurement_paste_input.setText("not a measurement")
        dialog._on_parse_measurement_clicked()

        assert dialog.normalized_measurement_set_payload() is None
        assert ReferenceWorkRepository.search() == []
        assert MeasurementSetRepository.list_attachment_candidates() == []
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_raw_points_wins_over_range(qapp, libs):
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 0, 6.0)
        _fill_minmax_cell(dialog, 0, 4, 10.0)
        # Add spore data rows so raw_points wins
        dialog.spore_table._ensure_rows(2)
        dialog.spore_table.setItem(0, 0, QTableWidgetItem("6.0"))
        dialog.spore_table.setItem(0, 1, QTableWidgetItem("3.0"))
        dialog.spore_table.setItem(1, 0, QTableWidgetItem("7.0"))
        dialog.spore_table.setItem(1, 1, QTableWidgetItem("3.5"))
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        assert ms.data_kind == "raw_points"
        assert ms.sample_size == 2
        points = json.loads(ms.raw_points_json)
        # Widget keeps length_um/width_um; JSON must use length/width.
        assert set(points[0].keys()) == {"length", "width"}
        assert points[0]["length"] == pytest.approx(6.0)
        assert points[1]["width"] == pytest.approx(3.5)
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_empty_returns_none(qapp, libs):
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        # No cells, no rows.
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_length_only_range_skipped(qapp, libs):
    # Partial ranges (length bounds only, no width bounds) must NOT
    # create a normalized set — the plot translator would reject them
    # and leave orphan library rows behind.
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 0, 6.0)
        _fill_minmax_cell(dialog, 0, 4, 10.0)
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_parmasto_only_submission_skips_normalized_write(qapp, libs):
    # A Parmasto-only submission (values only in the Parmasto tab)
    # must NOT create a normalized measurement set — Parmasto stays
    # legacy-only until the schema learns to represent it.
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        dialog.parmasto_inputs["parmasto_length_mean"].setText("8.5")
        dialog.parmasto_inputs["parmasto_width_mean"].setText("4.2")
        dialog.parmasto_inputs["parmasto_q_mean"].setText("2.02")
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_prefilled_mount_and_stain_carry_into_normalized_payload(qapp, libs):
    parent, dialog = _new_dialog(
        qapp,
        sporely_taxon_id=7,
        data={
            "mount_medium": "KOH",
            "stain": "Congo red",
            "metadata_json": {"notes": "Sample from herbarium K"},
        },
    )
    try:
        _fill_minmax_cell(dialog, 0, 0, 6.0)
        _fill_minmax_cell(dialog, 0, 4, 10.0)
        _fill_minmax_cell(dialog, 1, 0, 3.0)
        _fill_minmax_cell(dialog, 1, 4, 5.0)
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        assert ms.mount_medium == "KOH"
        assert ms.stain == "Congo red"
        assert ms.notes == "Sample from herbarium K"
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_combo_lists_existing_works(qapp, libs):
    work = _seed_work("Foo 2001")
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        # The combo has an empty placeholder plus at least the seeded work.
        labels = [
            dialog.publication_combo.itemText(i)
            for i in range(dialog.publication_combo.count())
        ]
        assert any("Foo 2001" in label for label in labels), labels
        ids = [
            str(dialog.publication_combo.itemData(i) or "")
            for i in range(dialog.publication_combo.count())
        ]
        assert work.id in ids
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_use_existing_radio_enables_when_treatment_and_set_exist(qapp, libs):
    work = _seed_work("Bar 2010")
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            taxon_id="7",
            name_as_published="Agaricus bisporus",
        )
    )
    MeasurementSetRepository.create(
        _make_range_set(treatment.id, raw_text="7-10 x 4-6")
    )
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        # Select the seeded work.
        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work.id:
                target_row = row
                break
        assert target_row >= 0
        dialog.publication_combo.setCurrentIndex(target_row)
        # Selecting a work populates the "use existing" candidate list.
        assert dialog.use_existing_radio.isEnabled()
        assert dialog._existing_sets_cache, "expected ≥1 candidate"
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_use_existing_radio_disabled_when_no_matching_treatment(qapp, libs):
    work = _seed_work("Baz 2020")
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            taxon_id="99",  # different taxon
            name_as_published="Other species",
        )
    )
    MeasurementSetRepository.create(_make_range_set(treatment.id))
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work.id:
                target_row = row
                break
        dialog.publication_combo.setCurrentIndex(target_row)
        assert not dialog.use_existing_radio.isEnabled()
        assert not dialog._existing_sets_cache
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def _make_range_set(treatment_id: str, *, raw_text: str = "7-10"):
    from database.reference_library import MeasurementSet

    return MeasurementSet(
        id="",
        taxon_treatment_id=treatment_id,
        character="spore_size",
        data_kind="range",
        raw_text=raw_text,
        length_min=7.0,
        length_max=10.0,
        width_min=4.0,
        width_max=6.0,
    )


def test_normalized_payload_mean_only_pair_accepted(qapp, libs):
    """A complete positive length_mean + width_mean pair is a valid
    range payload — the plot translator accepts it, so the dialog must
    persist it as a normalized MeasurementSet even when no explicit
    core/extreme bounds were entered."""
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 2, 7.0)  # length mean
        _fill_minmax_cell(dialog, 1, 2, 4.5)  # width mean
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        assert ms.data_kind == "range"
        assert ms.length_mean == pytest.approx(7.0)
        assert ms.width_mean == pytest.approx(4.5)
        # No bound columns were filled, so all rectangle bounds stay
        # None — the mean pair alone is what makes the payload plottable.
        assert ms.length_min is None
        assert ms.length_max is None
        assert ms.width_min is None
        assert ms.width_max is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_length_range_plus_width_min_only_rejected(qapp, libs):
    """A partial combo — a full length min/max range plus only
    ``width_min`` — is NOT plottable: it lacks the counterpart bound
    on width, so the plot translator would reject the snapshot and the
    attach helper would leave an orphan MeasurementSet behind. The
    dialog must skip creating a normalized set in that case."""
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 0, 6.0)  # length min
        _fill_minmax_cell(dialog, 0, 4, 10.0)  # length max
        _fill_minmax_cell(dialog, 1, 0, 3.0)  # width min only
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_inverted_range_rejected(qapp, libs):
    """An inverted length rectangle (min > max) is not plottable and
    must not produce a normalized set, even if both dimensions have
    bounds entered."""
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 0, 10.0)  # length min
        _fill_minmax_cell(dialog, 0, 4, 5.0)  # length max — inverted
        _fill_minmax_cell(dialog, 1, 0, 3.0)  # width min
        _fill_minmax_cell(dialog, 1, 4, 5.0)  # width max
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_display_label_has_no_search_suffix(qapp, libs):
    """The visible combo item text must be the clean legacy-style
    label — ``short_label`` (or ``title``) + ` (year)`. It must not
    include the bracketed search-corpus suffix that would otherwise
    leak into the persisted legacy ``reference_values.source``.
    """
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="article",
            title="Some Long Title About Spores",
            short_label="Petersen 1990",
            year=1990,
            container_title="Mycologia",
            citation_key="petersen1990",
            authors_json=json.dumps(
                [{"given": "John", "family": "Smith"}]
            ),
        )
    )
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work.id:
                target_row = row
                break
        assert target_row >= 0, "seeded work must appear in the combo"
        display_label = dialog.publication_combo.itemText(target_row)
        assert "[" not in display_label
        assert "·" not in display_label
        # The clean legacy-style label is what the dialog persists into
        # the legacy source column, so it must equal exactly the
        # short_label + " (year)" convention.
        assert display_label == "Petersen 1990 (1990)"
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_rejects_non_positive_length_but_keeps_legacy(qapp, libs):
    """The normalized MeasurementSet must drop rows whose length or
    width is non-positive so the plot translator, sample_size and the
    stored raw_points_json agree. The legacy get_points accessor keeps
    its historical behavior so the legacy observation-scoped points
    payload continues to preserve every historically accepted row.
    """
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        dialog.spore_table._ensure_rows(3)
        # Row 0: valid positive pair
        dialog.spore_table.setItem(0, 0, QTableWidgetItem("7.0"))
        dialog.spore_table.setItem(0, 1, QTableWidgetItem("3.5"))
        # Row 1: length = 0 (kept by legacy get_points, dropped from normalized)
        dialog.spore_table.setItem(1, 0, QTableWidgetItem("0"))
        dialog.spore_table.setItem(1, 1, QTableWidgetItem("4.0"))
        # Row 2: negative length (kept by legacy get_points, dropped from normalized)
        dialog.spore_table.setItem(2, 0, QTableWidgetItem("-1"))
        dialog.spore_table.setItem(2, 1, QTableWidgetItem("3.0"))
        # Legacy accessor preserves every previously accepted row so
        # the legacy observation-scoped points payload is unchanged.
        points = dialog.spore_table.get_points()
        assert len(points) == 3
        lengths = sorted(p["length_um"] for p in points)
        assert lengths == pytest.approx([-1.0, 0.0, 7.0])
        # Normalized payload filters non-positive rows.
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        assert ms.sample_size == 1
        stored = json.loads(ms.raw_points_json)
        assert len(stored) == 1
        assert stored[0]["length"] == pytest.approx(7.0)
        assert stored[0]["width"] == pytest.approx(3.5)
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_rejects_non_finite_length_width(qapp, libs):
    """NaN / Infinity in a raw-point row must be rejected by the
    normalized payload builder — the plot translator applies the same
    finite requirement, so agreement here keeps the persisted
    raw_points_json, sample_size, and plot output consistent.
    """
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        dialog.spore_table._ensure_rows(3)
        dialog.spore_table.setItem(0, 0, QTableWidgetItem("7.0"))
        dialog.spore_table.setItem(0, 1, QTableWidgetItem("3.5"))
        # Row 1: NaN length
        dialog.spore_table.setItem(1, 0, QTableWidgetItem("nan"))
        dialog.spore_table.setItem(1, 1, QTableWidgetItem("4.0"))
        # Row 2: infinite width
        dialog.spore_table.setItem(2, 0, QTableWidgetItem("6.0"))
        dialog.spore_table.setItem(2, 1, QTableWidgetItem("inf"))
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        # Only the finite, positive row survives.
        assert ms.sample_size == 1
        stored = json.loads(ms.raw_points_json)
        assert len(stored) == 1
        assert stored[0]["length"] == pytest.approx(7.0)
        assert stored[0]["width"] == pytest.approx(3.5)
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_normalized_payload_all_invalid_raw_points_returns_none(qapp, libs):
    """When every raw-point row is invalid (non-positive or non-finite),
    the normalized payload builder returns None — matching the "no
    plottable data" contract the plot translator would apply. The
    legacy row is still written upstream via the ReferenceDB path.
    """
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        dialog.spore_table._ensure_rows(2)
        dialog.spore_table.setItem(0, 0, QTableWidgetItem("nan"))
        dialog.spore_table.setItem(0, 1, QTableWidgetItem("4.0"))
        dialog.spore_table.setItem(1, 0, QTableWidgetItem("-2.0"))
        dialog.spore_table.setItem(1, 1, QTableWidgetItem("3.0"))
        assert dialog.normalized_measurement_set_payload() is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_raw_text_falls_back_to_paste_input(qapp, libs):
    """If the user types into measurement_paste_input but never presses
    Parse (so ``_raw_measurement_text`` is empty) the current typed
    expression must still round-trip into ``raw_text``.
    """
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        _fill_minmax_cell(dialog, 0, 2, 7.0)
        _fill_minmax_cell(dialog, 1, 2, 4.5)
        dialog.measurement_paste_input.setText("  7 x 4.5 (unparsed)  ")
        dialog._raw_measurement_text = ""  # simulate never-parsed
        ms = dialog.normalized_measurement_set_payload()
        assert ms is not None
        assert ms.raw_text == "  7 x 4.5 (unparsed)  "
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_selection_invalidates_when_user_types_over_it(qapp, libs):
    """After selecting publication A, the user can edit the combo text
    to search for B. If they then save without picking B via the
    completer, the coordinator must not persist against A's hidden
    selected ID. Verify _selected_work_id is cleared as soon as the
    visible text no longer matches the selected row's display label.
    """
    work_a = _seed_work("Alpha 2010")
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        # Programmatically pick work A via the combo (matches how a
        # completer activation would leave the model).
        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work_a.id:
                target_row = row
                break
        assert target_row >= 0
        dialog.publication_combo.setCurrentIndex(target_row)
        assert dialog._selected_work_id == work_a.id

        # User types over the selection. The editTextChanged handler
        # must observe the mismatch and clear _selected_work_id so a
        # subsequent save cannot secretly commit against work A.
        dialog.publication_combo.setEditText("Something else entirely")
        assert dialog._selected_work_id is None
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_selection_invalidation_also_clears_existing_sets_cache(qapp, libs):
    """When the publication selection is invalidated by a typed drift,
    the ``Use existing measurement set`` radio and its backing cache
    (scoped to the previously-selected work) must also be cleared so
    a stale existing-set cannot be persisted against.
    """
    work_a = _seed_work("Alpha 2011")
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work_a.id,
            taxon_id="7",
            name_as_published="Agaricus bisporus",
        )
    )
    MeasurementSetRepository.create(_make_range_set(treatment.id))
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work_a.id:
                target_row = row
                break
        assert target_row >= 0
        dialog.publication_combo.setCurrentIndex(target_row)
        # Sanity: selection made and cache populated for A.
        assert dialog._selected_work_id == work_a.id
        assert dialog._existing_sets_cache, "expected ≥1 candidate for work A"
        assert dialog.use_existing_radio.isEnabled()

        # User types over the selection — typed-drift invalidation
        # must clear the selection AND its scoped existing-sets state.
        dialog.publication_combo.setEditText("Completely different title")
        assert dialog._selected_work_id is None
        assert dialog._existing_sets_cache == []
        assert not dialog.use_existing_radio.isEnabled()
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_search_finds_work_outside_recent_page(qapp, libs, monkeypatch):
    """Works outside the initial 500-work recent page must still be
    reachable via live repository-backed search when the user types.
    """
    from ui import main_window as mw

    seed = _seed_work("Older 1985")
    hidden = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="article",
            title="Something completely different",
            short_label="Zorglub 1650",
            year=1650,
        )
    )
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        # Simulate a library larger than the initial page by clearing
        # the combo of the hidden work.
        blocker = None
        try:
            from PySide6.QtCore import QSignalBlocker

            blocker = QSignalBlocker(dialog.publication_combo)
            for row in range(dialog.publication_combo.count() - 1, -1, -1):
                if str(dialog.publication_combo.itemData(row) or "") == hidden.id:
                    dialog.publication_combo.removeItem(row)
            dialog._publication_search_seen_ids.discard(hidden.id)
        finally:
            del blocker
        ids_before = {
            str(dialog.publication_combo.itemData(i) or "")
            for i in range(dialog.publication_combo.count())
        }
        assert hidden.id not in ids_before
        # Now simulate the user typing part of the hidden work's label.
        dialog._on_publication_edit_text_changed("Zorglub")
        ids_after = {
            str(dialog.publication_combo.itemData(i) or "")
            for i in range(dialog.publication_combo.count())
        }
        assert hidden.id in ids_after
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_publication_completer_matches_title_and_given_name(qapp, libs):
    """The completer must still surface the row when the user types
    the title or an author's given name — the search corpus stored on
    ``Qt.UserRole + 1`` must therefore include both. Exercised
    directly on the corpus (not via the async popup) per the task
    scope.
    """
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="article",
            title="Studies on Some Long Title About Spores",
            short_label="Petersen 1990",
            year=1990,
            container_title="Mycologia",
            citation_key="petersen1990",
            authors_json=json.dumps(
                [{"given": "John", "family": "Smith"}]
            ),
        )
    )
    parent, dialog = _new_dialog(qapp, sporely_taxon_id=7)
    try:
        from PySide6.QtCore import Qt

        target_row = -1
        for row in range(dialog.publication_combo.count()):
            if str(dialog.publication_combo.itemData(row) or "") == work.id:
                target_row = row
                break
        assert target_row >= 0
        model = dialog.publication_combo.model()
        index = model.index(target_row, 0)
        corpus = model.data(index, Qt.UserRole + 1) or ""
        assert "Studies on Some Long Title About Spores" in corpus
        # Given name must be present so typing "John" hits this row.
        assert "John" in corpus
        # Family name and container/citation-key still belong in the
        # corpus (they were in the round-1 label already).
        assert "Smith" in corpus
        assert "Mycologia" in corpus
        assert "petersen1990" in corpus
        # The clean display label itself must NOT be the corpus.
        display_label = dialog.publication_combo.itemText(target_row)
        assert display_label != corpus
    finally:
        dialog.deleteLater()
        parent.deleteLater()
