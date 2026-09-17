"""Stage 4: reported statistics in the two reference editors.

The entry editor gains compact meaning tags, a scalar-or-interval mean
field, a reported median / S.D. line and one explicit discard control; the
library-manager form presents the same information read-only and preserves
it untouched. Both build their measurement set through the Stage 2 edit
operations rather than a second entry workflow.

The guard these tests pin down: while the minimum-supported-reader-version
gate is closed, the entry editor *shows* reported statistics but persists
the legacy-only projection, because everything it creates is bound for an
observation attachment and an enhanced row would be refused there.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QTableWidgetItem, QWidget

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from references import measurement_content_gates as gates
from references.measurement_content import decode_measurement_details

MEAN_COLUMN = 2

HEBELOMA_TABLE = "\n".join(
    [
        "Spore\t(min) 5%-95% (max)\tmean\tmedian\tS.D.",
        "Length\t(7.5) 8.4-13.0 (13.2)\t9.2-11.7\t9.2-11.7\t0.600",
        "Width\t(5.0) 5.1-7.2 (7.6)\t5.6-6.7\t5.6-6.7\t0.280",
        "Q\t(1.30) 1.42-1.96 (2.07)\t1.55-1.78\t1.54-1.79\t0.095",
    ]
)

SCALAR_MEAN_TABLE = "\n".join(
    [
        "Spore\trange\tmean\tmedian\tS.D.",
        "Length\t8.4-13.0\t10.4\t9.9\t0.600",
        "Width\t5.1-7.2\t6.1\t5.9\t0.280",
    ]
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


@pytest.fixture()
def reader_gate_open(monkeypatch):
    """Open the minimum-supported-reader-version gate for one test."""
    monkeypatch.setattr(gates, "MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN", True)
    return True


@pytest.fixture()
def editor(qapp, libs):
    from ui.reference_entry_editor import ReferenceEntryEditor
    from ui.reference_preview_pane import ReferencePreviewPane

    parent = QWidget()
    pane = ReferencePreviewPane(parent)
    widget = ReferenceEntryEditor(
        parent, "Hebeloma", "crustuliniforme", observation_id=42,
        sporely_taxon_id=7, preview_pane=pane,
    )
    try:
        yield widget
    finally:
        widget.deleteLater()
        parent.deleteLater()


def _make_observation(db_path) -> int:
    import sqlite3

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


def _parse(widget, text: str) -> None:
    widget.measurement_paste_input.setText(text)
    widget._on_parse_measurement_clicked()


def _cell(widget, row: int, col: int) -> str:
    item = widget.minmax_table.item(row, col)
    return item.text() if item else ""


def _set_cell(widget, row: int, col: int, text: str) -> None:
    widget.minmax_table.setItem(row, col, QTableWidgetItem(text))


def _seed_treatment():
    work = ReferenceWorkRepository.create(
        ReferenceWork(id="", type="book", title="Danmarks", short_label="P 1990", year=1990)
    )
    return TaxonTreatmentRepository.create(
        TaxonTreatment(id="", reference_work_id=work.id, name_as_published="Russula sp.")
    )


# --- Entry editor: parser wiring ---------------------------------------------


def test_reported_mean_interval_fills_the_mean_column_and_the_median_does_not(editor):
    """The Hebeloma layout reports both a mean and a median as intervals.

    Before Stage 4 the Mean column was filled from the parser's ``p50``
    centre and the reported mean was dropped entirely. The mean belongs in
    the mean column; the median belongs in the details line beside it.
    """
    _parse(editor, HEBELOMA_TABLE)

    assert _cell(editor, 0, MEAN_COLUMN) == "9.2-11.7"
    assert _cell(editor, 1, MEAN_COLUMN) == "5.6-6.7"
    assert _cell(editor, 2, MEAN_COLUMN) == "1.55-1.78"

    statistics = editor.reported_statistics_label.text()
    assert "L median 9.2-11.7" in statistics
    assert "L S.D. 0.6" in statistics
    assert "Q median 1.54-1.79" in statistics


def test_scalar_mean_column_fills_the_mean_cell_as_a_number(editor):
    _parse(editor, SCALAR_MEAN_TABLE)

    assert _cell(editor, 0, MEAN_COLUMN) == "10.4"
    assert _cell(editor, 1, MEAN_COLUMN) == "6.1"
    assert "L median 9.9" in editor.reported_statistics_label.text()


def test_a_reported_q_mean_does_not_reach_the_parmasto_species_mean_field(editor):
    """``Qm``/``Qav`` is one publication's reported Q mean.

    The Parmasto tab holds species means the user enters deliberately;
    routing a parsed Qm through it would make a single source's value look
    like a Parmasto biometric.
    """
    _parse(editor, "9.8-11.3 x 8.0-9.4 um, Q = 1.1-1.3, Qm = 1.25, n = 36")

    assert editor.parmasto_inputs["parmasto_q_mean"].text() == ""
    assert _cell(editor, 2, MEAN_COLUMN) == "1.25"

    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.q_mean == pytest.approx(1.25)


def test_compact_tags_carry_their_explanation_for_assistive_technology(editor):
    _parse(editor, HEBELOMA_TABLE)

    label = editor.measurement_tags_label
    assert label.isVisible() or label.text()
    assert "L inner 5–95%" in label.text()
    assert "L extremes: reported" in label.text()
    explanation = label.accessibleDescription()
    assert "5th–95th percentile" in explanation
    assert explanation == label.toolTip()


def test_a_range_with_no_percentile_heading_shows_no_percentile_number(editor):
    _parse(editor, "(9.5-)9.8-11.3(-11.7) x (7.3-)8.0-9.4(-9.4) um")

    text = editor.measurement_tags_label.text()
    assert "L inner: unspecified" in text
    assert "%" not in text


# --- Entry editor: the reader-version guard ----------------------------------


def test_closed_reader_gate_keeps_the_payload_legacy_only(editor):
    """Guarded editing: shown, reviewed, not yet stored.

    An enhanced row cannot become an attachment while the gate is closed, so
    persisting one here would only move the refusal to the save button.
    """
    assert gates.enhanced_editing_enabled() is False
    _parse(editor, HEBELOMA_TABLE)

    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.measurement_details_json is None
    assert payload.q_core_min is None
    assert payload.q_core_max is None
    # ...and the ordinary columns are unchanged by the extension being off.
    assert payload.length_core_min == pytest.approx(8.4)
    assert payload.length_max == pytest.approx(13.2)
    # The user is told, rather than left to assume the tags were stored.
    assert editor._reported_statistics_notice_label.text()


def test_a_parsed_table_still_attaches_to_an_observation_while_the_gate_is_closed(
    editor, libs
):
    """The whole reason the editor is guarded rather than merely delayed.

    ``_gated_observation_reference_snapshot`` refuses to freeze an enhanced
    row as evidence while the reader gate is closed, and the Stage 2 parser
    tags every inner range — so an ungated editor would turn almost every
    paste-and-attach into a ``ReferenceIntegrityError`` at save time.
    """
    from database.reference_library import (
        ObservationReferenceUseRepository,
        QuickAddReferenceRequest,
        QuickAddReferenceService,
    )

    db_path, _ = libs
    observation_id = _make_observation(db_path)
    _parse(editor, HEBELOMA_TABLE)
    payload = editor.normalized_measurement_set_payload()
    assert payload is not None

    result = QuickAddReferenceService.create_and_attach(
        QuickAddReferenceRequest(
            observation_id=observation_id,
            existing_work_id=None,
            work=ReferenceWork(
                id="", type="book", title="Hebeloma", short_label="H 2004", year=2004
            ),
            treatment=TaxonTreatment(
                id="", reference_work_id="", name_as_published="Hebeloma sp."
            ),
            measurement_set=payload,
            role="compared",
        )
    )

    assert result.use is not None and result.created_attachment
    stored = MeasurementSetRepository.get(result.measurement_set.id)
    assert stored.measurement_details_json is None
    assert stored.q_core_min is None and stored.q_core_max is None
    # The ordinary columns the same source always produced are all there.
    assert stored.length_core_min == pytest.approx(8.4)
    assert stored.width_max == pytest.approx(7.6)
    uses = ObservationReferenceUseRepository.list_for_observation(observation_id)
    assert [u.reference_measurement_set_id for u in uses] == [stored.id]


def test_open_reader_gate_persists_the_tags_and_the_q_core_pair(editor, reader_gate_open):
    _parse(editor, HEBELOMA_TABLE)

    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.q_core_min == pytest.approx(1.42)
    assert payload.q_core_max == pytest.approx(1.96)

    details = decode_measurement_details(payload.measurement_details_json)
    length = details.metrics["length"]
    assert length.core_range.kind == "percentile_interval"
    assert length.core_range.percentile_bounds == (5.0, 95.0)
    assert length.outer_range.kind == "reported_extremes"
    assert length.median.lower == pytest.approx(9.2)
    assert length.sd.value == pytest.approx(0.6)
    # Rule 4: the interval mean excludes the scalar mean column.
    assert payload.length_mean is None
    assert length.mean_interval.lower == pytest.approx(9.2)
    assert editor._reported_statistics_notice_label.text() == ""

    # ...and the repository accepts it: the editor must not be able to build
    # content that fails the contract's own edit-mode validation.
    treatment = _seed_treatment()
    payload.taxon_treatment_id = treatment.id
    stored = MeasurementSetRepository.create(payload)
    assert stored.q_core_min == pytest.approx(1.42)
    assert decode_measurement_details(
        stored.measurement_details_json
    ).metrics["length"].core_range.percentile_bounds == (5.0, 95.0)


def test_open_gate_writes_no_derived_q_extreme_next_to_a_tagged_core_pair(
    editor, reader_gate_open
):
    """The Q core pair has a column of its own once the extension is on.

    Falling back to the typical bounds for ``q_min``/``q_max`` would write a
    derived extreme beside a core pair holding the same numbers.
    """
    _parse(editor, "9.8-11.3 x 8.0-9.4 um, Q = 1.1-1.3")

    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.q_core_min == pytest.approx(1.1)
    assert payload.q_core_max == pytest.approx(1.3)
    assert payload.q_min is None
    assert payload.q_max is None


def test_closed_gate_keeps_the_historical_q_bound_fallback(editor):
    _parse(editor, "9.8-11.3 x 8.0-9.4 um, Q = 1.1-1.3")

    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.q_min == pytest.approx(1.1)
    assert payload.q_max == pytest.approx(1.3)


def test_a_hand_typed_range_acquires_no_tag_and_no_extension(editor, reader_gate_open):
    """Typing numbers is not a statement about their meaning."""
    for row, values in ((0, ("6.00", "7.00", "9.00", "10.00")), (1, ("3.00", "3.50", "4.50", "5.00"))):
        _set_cell(editor, row, 0, values[0])
        _set_cell(editor, row, 1, values[1])
        _set_cell(editor, row, 3, values[2])
        _set_cell(editor, row, 4, values[3])

    assert editor.measurement_tags_label.text() == ""
    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.measurement_details_json is None


# --- Entry editor: explicit correction and clearing ---------------------------


def test_the_mean_cell_accepts_an_interval_typed_by_hand(editor, reader_gate_open):
    _set_cell(editor, 0, 1, "9.00")
    _set_cell(editor, 0, 3, "11.00")
    _set_cell(editor, 1, 1, "5.00")
    _set_cell(editor, 1, 3, "7.00")
    _set_cell(editor, 0, MEAN_COLUMN, "9.2 – 11.7")

    assert _cell(editor, 0, MEAN_COLUMN) == "9.2-11.7"
    payload = editor.normalized_measurement_set_payload()
    assert payload is not None
    assert payload.length_mean is None
    details = decode_measurement_details(payload.measurement_details_json)
    assert details.metrics["length"].mean_interval.upper == pytest.approx(11.7)


def test_unreadable_mean_text_is_refused_and_leaves_the_typed_mean_alone(editor):
    _parse(editor, SCALAR_MEAN_TABLE)
    assert _cell(editor, 0, MEAN_COLUMN) == "10.4"

    _set_cell(editor, 0, MEAN_COLUMN, "about 10")

    content = editor._current_measurement_content()
    assert content.length_mean == pytest.approx(10.4)


def test_clearing_a_tagged_pair_removes_its_tag_with_its_numbers(editor):
    _parse(editor, HEBELOMA_TABLE)
    assert "L extremes: reported" in editor.measurement_tags_label.text()

    _set_cell(editor, 0, 0, "")
    _set_cell(editor, 0, 4, "")

    content = editor._current_measurement_content()
    assert content.details.metrics["length"].outer_range is None
    assert "L extremes: reported" not in editor.measurement_tags_label.text()
    assert "L inner 5–95%" in editor.measurement_tags_label.text()


def test_a_wrong_range_interpretation_can_be_retracted_per_metric(editor):
    """Correction is offered as retraction, not as assertion.

    A reader who knows the parser read a heading wrongly can drop that
    metric's claim about what its range means, keeping the numbers and the
    reported median / S.D. Asserting a *different* interpretation is
    deliberately not offered — it would manufacture evidence.
    """
    _parse(editor, HEBELOMA_TABLE)
    assert "L inner 5–95%" in editor.measurement_tags_label.text()

    editor._on_drop_range_tags("length")

    text = editor.measurement_tags_label.text()
    assert "L inner 5–95%" not in text
    assert "L extremes: reported" not in text
    # Width and Q keep theirs; the reported statistics survive.
    assert "W inner 5–95%" in text
    assert "L median 9.2-11.7" in editor.reported_statistics_label.text()

    content = editor._current_measurement_content()
    length = content.details.metrics["length"]
    assert (length.outer_range, length.core_range) == (None, None)
    assert length.sd.value == pytest.approx(0.6)
    assert content.length_core_min == pytest.approx(8.4)
    assert content.length_min == pytest.approx(7.5)


def test_the_correction_menu_offers_only_metrics_that_carry_a_range_tag(editor):
    _parse(editor, SCALAR_MEAN_TABLE)

    labels = [a.text() for a in editor._reported_statistics_menu.actions() if a.text()]
    assert any("Length" in label for label in labels)
    assert any("Width" in label for label in labels)
    assert not any("Q" in label and "drop" in label for label in labels)
    assert any("Discard all" in label for label in labels)


def test_discarding_reported_statistics_keeps_the_measured_values(editor):
    """Tags, medians and S.D.s go; every number the table shows stays.

    A mean reported as an interval is a visible, editable cell, so it
    survives the discard — otherwise the control would empty a field it
    promises not to touch. It is cleared by clearing that cell.
    """
    _parse(editor, HEBELOMA_TABLE)
    before = [_cell(editor, 0, col) for col in range(5)]

    editor._on_clear_reported_statistics_clicked()

    assert editor.reported_statistics_label.text() == ""
    assert "5–95%" not in editor.measurement_tags_label.text()
    assert "extremes" not in editor.measurement_tags_label.text()
    assert [_cell(editor, 0, col) for col in range(5)] == before

    content = editor._current_measurement_content()
    length = content.details.metrics["length"]
    assert (length.core_range, length.outer_range, length.median, length.sd) == (
        None, None, None, None,
    )
    assert length.mean_interval.upper == pytest.approx(11.7)
    assert content.length_core_min == pytest.approx(8.4)
    assert content.length_min == pytest.approx(7.5)


def test_clearing_the_mean_cell_removes_a_reported_mean_interval(editor):
    _parse(editor, HEBELOMA_TABLE)

    _set_cell(editor, 0, MEAN_COLUMN, "")

    content = editor._current_measurement_content()
    assert content.details.metrics["length"].mean_interval is None
    assert content.length_mean is None
    assert "L mean: interval" not in editor.measurement_tags_label.text()


def test_swapping_length_and_width_moves_the_tags_with_the_numbers(editor):
    _parse(editor, "(7.5-)8.4-13.0(-13.2) x 5.1-7.2 um")
    before_length = [_cell(editor, 0, col) for col in range(5)]

    editor._on_swap_lw_clicked()

    assert [_cell(editor, 1, col) for col in range(5)] == before_length
    content = editor._current_measurement_content()
    # The reported extremes travelled to the width row with their values.
    assert content.details.metrics["width"].outer_range.kind == "reported_extremes"
    assert content.details.metrics["length"].outer_range is None
    text = editor.measurement_tags_label.text()
    assert "W extremes: reported" in text
    assert "L extremes: reported" not in text


# --- Entry editor: preview pane ----------------------------------------------


def test_the_preview_pane_shows_the_tags_and_the_reported_median(editor):
    _parse(editor, HEBELOMA_TABLE)
    editor.sync_preview()

    pane_text = editor._preview_pane.reported_statistics_label.text()
    assert "L inner 5–95%" in pane_text
    assert "L median 9.2-11.7" in pane_text
    assert editor._preview_pane.reported_statistics_label.accessibleDescription()


def test_a_later_summary_without_tags_does_not_inherit_the_previous_one(editor):
    """The preview pane is shared between the picker's tabs.

    Showing a tagged literature entry and then a community dataset must not
    leave the first one's meaning tags describing the second one's numbers.
    """
    _parse(editor, HEBELOMA_TABLE)
    editor.sync_preview()
    pane = editor._preview_pane
    assert pane.reported_statistics_label.text()

    pane.set_summary("Community dataset", "", [("Length", "8", "9", "10")], "note")

    assert pane.reported_statistics_label.text() == ""
    assert pane.reported_statistics_label.isVisible() is False


def test_the_preview_shows_an_interval_mean_rather_than_an_em_dash(editor):
    _parse(editor, HEBELOMA_TABLE)
    editor.sync_preview()

    assert editor._preview_pane.summary_table.item(0, 2).text() == "9.2-11.7"


def test_reopening_a_stored_enhanced_set_shows_what_it_reports(editor, libs):
    work = ReferenceWorkRepository.create(
        ReferenceWork(id="", type="book", title="Hebeloma", short_label="H 2004", year=2004)
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(id="", reference_work_id=work.id, name_as_published="Hebeloma sp.")
    )
    stored = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_core_min=8.4,
            length_core_max=13.0,
            width_core_min=5.1,
            width_core_max=7.2,
            measurement_details_json=json.dumps(
                {
                    "schema_version": 1,
                    "metrics": {
                        "length": {
                            "core_range": {
                                "kind": "percentile_interval",
                                "percentile_bounds": [5, 95],
                            },
                            "mean_interval": {
                                "lower": 9.2,
                                "upper": 11.7,
                                "kind": "reported_range",
                            },
                            "sd": {"value": 0.6},
                        }
                    },
                }
            ),
        )
    )
    editor.use_existing_radio.setEnabled(True)
    editor.use_existing_radio.setChecked(True)
    editor._existing_sets_cache = [stored]
    editor._selected_measurement_set_id = stored.id
    editor.sync_preview()

    pane = editor._preview_pane
    assert "L inner 5–95%" in pane.reported_statistics_label.text()
    assert "L S.D. 0.6" in pane.reported_statistics_label.text()
    assert pane.summary_table.item(0, 2).text() == "9.2-11.7"


# --- Library manager form: inspect-only presentation --------------------------


def test_manager_parse_uses_the_reported_mean_not_the_median(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        form.raw_text_input.setText(SCALAR_MEAN_TABLE)
        form.parse_btn.click()

        assert form.length_mean_input.text() == "10.4"
        assert form.width_mean_input.text() == "6.1"
        # The median is reported beside the fields, never inside one.
        shown = form.reported_statistics_label.text()
        assert "L median 9.9" in shown
        assert "not stored by this version" in shown
    finally:
        form.deleteLater()


def test_manager_save_builds_the_set_from_to_content(qapp, libs, reader_gate_open):
    """The parser → repository wiring Stage 3B deferred to this stage.

    With enhanced editing enabled, a newly parsed manager entry must persist
    the interval means, median, S.D., range tags and Q core pair the
    expression carries — not just its legacy scalar projection.
    """
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        form.raw_text_input.setText(HEBELOMA_TABLE)
        form.parse_btn.click()
        assert form.length_mean_input.text() == "9.2-11.7"
        assert form.q_core_min_input.text() == "1.42"
        form._on_save()
        assert form.error_label.text() == "", form.error_label.text()
        saved = form.result_set
    finally:
        form.deleteLater()

    assert saved is not None
    assert saved.q_core_min == pytest.approx(1.42)
    assert saved.q_core_max == pytest.approx(1.96)
    # Role-for-role: no derived extreme beside the tagged core pair.
    assert saved.q_min == pytest.approx(1.30)
    assert saved.q_max == pytest.approx(2.07)

    details = decode_measurement_details(saved.measurement_details_json)
    length = details.metrics["length"]
    assert length.core_range.kind == "percentile_interval"
    assert length.core_range.percentile_bounds == (5.0, 95.0)
    assert length.outer_range.kind == "reported_extremes"
    assert length.median.lower == pytest.approx(9.2)
    assert length.sd.value == pytest.approx(0.6)
    # Rule 4: the interval mean excludes the scalar column.
    assert saved.length_mean is None
    assert length.mean_interval.upper == pytest.approx(11.7)
    assert details.metrics["q"].mean_interval.lower == pytest.approx(1.55)


def test_manager_mean_field_accepts_an_interval_typed_by_hand(
    qapp, libs, reader_gate_open
):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        form.length_core_min_input.setText("9")
        form.length_core_max_input.setText("11")
        form.width_core_min_input.setText("5")
        form.width_core_max_input.setText("7")
        form.length_mean_input.setText("9.2 – 11.7")
        form._on_save()
        assert form.error_label.text() == "", form.error_label.text()
        saved = form.result_set
    finally:
        form.deleteLater()

    assert saved is not None
    assert saved.length_mean is None
    details = decode_measurement_details(saved.measurement_details_json)
    assert details.metrics["length"].mean_interval.upper == pytest.approx(11.7)


def test_manager_refuses_an_unreadable_mean_instead_of_saving_null(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        form.length_core_min_input.setText("9")
        form.length_core_max_input.setText("11")
        form.length_mean_input.setText("about 10")
        form._on_save()

        assert form.result_set is None
        assert "9.2-11.7" in form.error_label.text()
    finally:
        form.deleteLater()


def test_manager_q_core_inputs_are_hidden_while_the_gate_is_closed(qapp, libs):
    """A field this version cannot persist must not be offered."""
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        assert form.q_core_min_input.isVisibleTo(form) is False
        assert form.q_core_max_input.isVisibleTo(form) is False
        assert "core_min" not in form._q_row_label.text()
    finally:
        form.deleteLater()


def test_manager_parse_writes_no_extension_column(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    form = _MeasurementSetForm(None, taxon_treatment_id=treatment.id)
    try:
        form.raw_text_input.setText(SCALAR_MEAN_TABLE)
        form.parse_btn.click()
        payload = form._collect()

        assert "measurement_details_json" not in payload
        assert "q_core_min" not in payload
    finally:
        form.deleteLater()


def _enhanced_set(treatment_id: str) -> MeasurementSet:
    return MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment_id,
            character="spore_size",
            data_kind="range",
            length_min=7.5,
            length_core_min=8.4,
            length_core_max=13.0,
            length_max=13.2,
            width_core_min=5.1,
            width_core_max=7.2,
            q_core_min=1.42,
            q_core_max=1.96,
            measurement_details_json=json.dumps(
                {
                    "schema_version": 1,
                    "metrics": {
                        "length": {
                            "outer_range": {"kind": "reported_extremes"},
                            "core_range": {
                                "kind": "percentile_interval",
                                "percentile_bounds": [5, 95],
                            },
                            "sd": {"value": 0.6},
                        },
                        "q": {"core_range": {"kind": "unspecified"}},
                    },
                }
            ),
        )
    )


def test_manager_shows_a_stored_set_s_tags_read_only(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    stored = _enhanced_set(treatment.id)
    form = _MeasurementSetForm(
        None, taxon_treatment_id=treatment.id, measurement_set=stored
    )
    try:
        shown = form.reported_statistics_label.text()
        assert "L inner 5–95%" in shown
        assert "L S.D. 0.6" in shown
        assert form.reported_statistics_label.accessibleDescription()
    finally:
        form.deleteLater()


def test_an_ordinary_manager_edit_preserves_the_extension_byte_for_byte(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    stored = _enhanced_set(treatment.id)
    form = _MeasurementSetForm(
        None, taxon_treatment_id=treatment.id, measurement_set=stored
    )
    try:
        form.notes_input.setPlainText("checked against the plate")
        form._on_save()
        saved = form.result_set
    finally:
        form.deleteLater()

    assert saved is not None
    assert saved.notes == "checked against the plate"
    assert saved.measurement_details_json == stored.measurement_details_json
    assert saved.q_core_min == pytest.approx(1.42)
    assert saved.q_core_max == pytest.approx(1.96)


def test_clearing_a_tagged_pair_in_the_manager_removes_only_that_tag(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    stored = _enhanced_set(treatment.id)
    form = _MeasurementSetForm(
        None, taxon_treatment_id=treatment.id, measurement_set=stored
    )
    try:
        form.length_min_input.setText("")
        form.length_max_input.setText("")
        form._on_save()
        assert form.error_label.text() == "", form.error_label.text()
        saved = form.result_set
    finally:
        form.deleteLater()

    assert saved is not None
    details = decode_measurement_details(saved.measurement_details_json)
    assert details.metrics["length"].outer_range is None
    assert details.metrics["length"].core_range.kind == "percentile_interval"
    assert details.metrics["length"].sd.value == pytest.approx(0.6)
    assert details.metrics["q"].core_range.kind == "unspecified"
    assert saved.length_min is None and saved.length_max is None


def test_converting_an_enhanced_set_to_raw_points_discards_its_tags(qapp, libs):
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    treatment = _seed_treatment()
    stored = _enhanced_set(treatment.id)
    form = _MeasurementSetForm(
        None, taxon_treatment_id=treatment.id, measurement_set=stored
    )
    try:
        index = form.data_kind_combo.findData("raw_points")
        form.data_kind_combo.setCurrentIndex(index)
        form.raw_points_input.setPlainText(
            json.dumps([{"length": 9.0, "width": 5.5}, {"length": 9.5, "width": 5.7}])
        )
        form._on_save()
        assert form.error_label.text() == "", form.error_label.text()
        saved = form.result_set
    finally:
        form.deleteLater()

    assert saved is not None
    assert saved.measurement_details_json is None
    assert saved.q_core_min is None and saved.q_core_max is None


def test_manager_detail_pane_reports_the_meaning_and_the_q_core_pair(qapp, libs):
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    treatment = _seed_treatment()
    stored = _enhanced_set(treatment.id)
    dialog = ReferenceLibraryManagerDialog(None)
    try:
        dialog._current_work = ReferenceWorkRepository.get(treatment.reference_work_id)
        dialog._current_treatment = treatment
        dialog._render_measurement_set_detail(stored)
        text = dialog.detail_view.toPlainText()
    finally:
        dialog.deleteLater()

    assert "Q core min: 1.42" in text
    assert "Q core max: 1.96" in text
    assert "L inner 5–95%" in text
    assert "5th–95th percentile" in text
    assert "L S.D. 0.6" in text
