"""Model-level tests for ui.reference_entry_editor.ReferenceEntryEditor.

Covers the surface stage 4c added when the editor was extracted out of the
old ``ReferenceAddDialog`` so the Add-reference picker's Enter-manually tab
could embed it directly: validation/readiness, the target-identity split
(``sporely_taxon_id`` vs. ``observation_taxon_id``), target-change
invalidation of publication/treatment state, and shared-preview-pane sync.
Uses no real database -- publication/measurement-set repositories are
monkeypatched to empty results, matching the isolation convention in
``tests/test_add_reference_dialog.py``.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox, QTableWidgetItem

from ui.reference_entry_editor import ReferenceEntryEditor


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture(autouse=True)
def isolated_editor_repositories(monkeypatch):
    import ui.reference_entry_editor as editor_mod

    monkeypatch.setattr(
        editor_mod.ReferenceWorkRepository, "list_recent", lambda limit=500: []
    )
    monkeypatch.setattr(
        editor_mod.ReferenceWorkRepository,
        "search",
        lambda query, limit=50: [],
    )
    monkeypatch.setattr(
        editor_mod.TaxonTreatmentRepository, "list_for_work", lambda work_id: []
    )
    # ``validate_and_build_result`` shows a real, blocking QMessageBox on
    # invalid input; record calls instead of hanging the test on a modal
    # dialog with no user to click it.
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    # Default: confirm the legacy-only-save gate ("no publication
    # selected") as Yes, since most tests here submit range/points data
    # with no publication and want to observe the resulting payload, not
    # this gate's own reject branch.
    monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.Yes)


def _make_editor(**kwargs) -> ReferenceEntryEditor:
    _app()
    defaults = dict(
        genus="Cortinarius",
        species="limonius",
        observation_id=42,
        sporely_taxon_id=7,
    )
    defaults.update(kwargs)
    return ReferenceEntryEditor(None, defaults.pop("genus"), defaults.pop("species"), **defaults)


def _set_range(editor: ReferenceEntryEditor, *, length=(8.0, 11.0), width=(6.0, 8.0)) -> None:
    editor.set_measurement_cell_text(0, 0, f"{length[0]:.2f}")
    editor.set_measurement_cell_text(0, 4, f"{length[1]:.2f}")
    editor.set_measurement_cell_text(1, 0, f"{width[0]:.2f}")
    editor.set_measurement_cell_text(1, 4, f"{width[1]:.2f}")


def test_valid_range_builds_reference_result_with_both_taxon_fields():
    editor = _make_editor(observation_taxon_id=7)
    _set_range(editor)
    assert editor.validate_and_build_result() is True
    data = editor.result_data()
    assert data["source_kind"] == "reference"
    assert data["length_min"] == 8.0
    assert data["length_max"] == 11.0
    assert data["sporely_taxon_id"] == 7
    assert data["observation_taxon_id"] == 7
    assert data["observation_id"] == 42


def test_raw_points_build_points_result():
    editor = _make_editor()
    editor.spore_table._ensure_rows(2)
    editor.spore_table.setItem(0, 0, QTableWidgetItem("9.0"))
    editor.spore_table.setItem(0, 1, QTableWidgetItem("6.0"))
    editor.spore_table.setItem(1, 0, QTableWidgetItem("10.0"))
    editor.spore_table.setItem(1, 1, QTableWidgetItem("6.5"))
    assert editor.validate_and_build_result() is True
    data = editor.result_data()
    assert data["source_kind"] == "points"
    assert len(data["points"]) == 2


def test_declining_legacy_only_confirmation_keeps_fail_closed(monkeypatch):
    monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.No)
    editor = _make_editor()
    _set_range(editor)
    assert editor.validate_and_build_result() is False
    assert editor.result_data() is None


def test_empty_input_cannot_submit_and_leaves_no_result():
    editor = _make_editor()
    assert editor.is_ready_to_submit() is False
    assert editor.validate_and_build_result() is False
    assert editor.result_data() is None


def test_malformed_points_cannot_submit():
    editor = _make_editor()
    # A width of zero is filtered out by SporeDataTable.get_points().
    editor.spore_table._ensure_rows(1)
    editor.spore_table.setItem(0, 0, QTableWidgetItem("9.0"))
    editor.spore_table.setItem(0, 1, QTableWidgetItem("0"))
    assert editor.is_ready_to_submit() is False
    assert editor.validate_and_build_result() is False


def test_is_ready_to_submit_matches_validate_for_range_and_points():
    editor = _make_editor()
    assert editor.is_ready_to_submit() is False
    _set_range(editor)
    assert editor.is_ready_to_submit() is True
    assert editor.validate_and_build_result() is True


def test_existing_measurement_set_requires_a_selection():
    editor = _make_editor()
    editor.use_existing_radio.setEnabled(True)
    editor.use_existing_radio.setChecked(True)
    assert editor.is_ready_to_submit() is False
    assert editor.validate_and_build_result() is False
    editor._selected_measurement_set_id = "ms-1"
    assert editor.is_ready_to_submit() is True
    assert editor.validate_and_build_result() is True
    assert editor.result_data()["source_kind"] == "existing_measurement_set"


def test_target_change_invalidates_publication_and_result_but_keeps_measurements():
    editor = _make_editor(sporely_taxon_id=7, observation_taxon_id=7)
    _set_range(editor)
    assert editor.validate_and_build_result() is True
    editor._selected_work_id = "w-1"
    editor.name_as_published_input.setText("Cortinarius limonius (custom synonym)")

    editor.set_comparison_target(genus="Cortinarius", species="rubellus", sporely_taxon_id=99)

    assert editor._selected_work_id is None
    assert editor._sporely_taxon_id == 99
    # The observation's own taxon id must never change with the target.
    assert editor._observation_taxon_id == 7
    assert editor.name_as_published_input.text() == "Cortinarius rubellus"
    assert editor.result_data() is None
    # Measurement values entered before the target switch are preserved.
    assert editor.measurement_cell_text(0, 0) == "8.00"
    assert editor.measurement_cell_text(0, 4) == "11.00"
    assert editor.validate_and_build_result() is True
    assert editor.result_data()["sporely_taxon_id"] == 99
    assert editor.result_data()["observation_taxon_id"] == 7


def test_target_change_tolerates_non_numeric_taxon_id():
    """A freely-typed or fixture taxon id that never resolves to a real
    taxon must fail soft (None), not raise, matching
    ``MainWindow._active_sporely_taxon_id``'s existing fail-soft contract.
    """
    editor = _make_editor()
    editor.set_comparison_target(
        genus="Amanita", species="muscaria", sporely_taxon_id="not-a-real-id"
    )
    assert editor._sporely_taxon_id is None


def test_typing_updates_the_entry_without_leaving_the_field():
    """The editor follows the grid live, not only on commit.

    ``editingFinished`` never fires while a field still has focus, so a
    footer wired to it alone would judge the previous entry for as long as
    the user kept typing. The create dialog's buttons follow ``data_changed``.
    """
    editor = _make_editor()
    assert editor.is_ready_to_submit() is False
    changes = []
    editor.data_changed.connect(lambda: changes.append(editor.is_ready_to_submit()))

    editor.measurement_inputs[0][0].setText("8.0")
    editor.measurement_inputs[0][4].setText("11.0")
    editor.measurement_inputs[1][0].setText("6.0")
    editor.measurement_inputs[1][4].setText("8.0")

    assert len(changes) >= 4
    assert changes[-1] is True
    content = editor._current_measurement_content()
    assert (content.length_min, content.length_max) == pytest.approx((8.0, 11.0))


def test_the_measurement_grid_tabs_left_to_right_across_each_row():
    """Contract N24: the keyboard order is explicit, not inherited."""
    editor = _make_editor()
    flat = [field for row in editor.measurement_inputs for field in row]
    known = set(flat)

    for current, expected in zip(flat, flat[1:]):
        widget = current.nextInFocusChain()
        while widget is not None and widget not in known:
            widget = widget.nextInFocusChain()
            if widget is current:
                widget = None
        assert widget is expected, (
            f"{current.accessibleName()!r} should tab to {expected.accessibleName()!r}"
        )


def test_the_measurement_grid_is_plain_inputs_not_a_table_widget():
    """Contract N24 names the widget kind, because the old table owned the
    focus chain and only committed a value on an item change."""
    from PySide6.QtWidgets import QLineEdit, QTableWidget

    editor = _make_editor()
    assert not hasattr(editor, "minmax_table")
    assert len(editor.measurement_inputs) == 3
    for row in editor.measurement_inputs:
        assert len(row) == 5
        assert all(isinstance(field, QLineEdit) for field in row)
        assert all(not isinstance(field, QTableWidget) for field in row)


def test_swapping_length_and_width_moves_the_numbers_in_the_grid():
    editor = _make_editor()
    _set_range(editor, length=(8.0, 11.0), width=(6.0, 8.0))

    editor._on_swap_lw_clicked()

    assert editor.measurement_cell_text(0, 0) == "6.00"
    assert editor.measurement_cell_text(0, 4) == "8.00"
    assert editor.measurement_cell_text(1, 0) == "8.00"
    assert editor.measurement_cell_text(1, 4) == "11.00"


# ---------------------------------------------------------------------
# "Name as published" fallback to the normalized taxon name
# ---------------------------------------------------------------------


def test_treatment_payload_falls_back_to_taxon_name_when_name_as_published_blank():
    """An empty "Name as published" must not block the save.

    ``TaxonTreatmentRepository._validate`` requires the field, so leaving it
    blank previously failed the whole save with a raw
    "taxon_treatment.name_as_published is required" message. The publication
    normally uses the same name as the normalized taxon, so that is the
    fallback.
    """
    editor = _make_editor(genus="Lacrymaria", species="lacrymabunda")
    editor.name_as_published_input.setText("")

    payload = editor.quick_add_treatment_payload()

    assert payload["name_as_published"] == "Lacrymaria lacrymabunda"


def test_treatment_payload_keeps_an_explicit_name_as_published_verbatim():
    """The fallback must never overwrite a name the user actually typed.

    Recording an old synonym or historical combination is the entire point
    of the field being separate from the normalized taxon.
    """
    editor = _make_editor(genus="Lacrymaria", species="lacrymabunda")
    editor.name_as_published_input.setText("Psathyrella velutina (Pers.) Singer")

    payload = editor.quick_add_treatment_payload()

    assert payload["name_as_published"] == "Psathyrella velutina (Pers.) Singer"


def test_treatment_payload_stays_blank_when_there_is_no_taxon_either():
    """With no taxon to fall back to, the field stays empty and the
    repository's own validation remains the thing that reports it."""
    editor = _make_editor(genus="", species="")
    editor.name_as_published_input.setText("")

    assert editor.quick_add_treatment_payload()["name_as_published"] == ""


def test_treatment_payload_follows_a_changed_comparison_target():
    """The fallback must track the current reference taxon, not the one the
    editor happened to be constructed with."""
    editor = _make_editor(genus="Cortinarius", species="limonius")
    editor.set_comparison_target(
        genus="Lacrymaria", species="lacrymabunda", sporely_taxon_id=None
    )

    assert (
        editor.quick_add_treatment_payload()["name_as_published"]
        == "Lacrymaria lacrymabunda"
    )
