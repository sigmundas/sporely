"""Stage 2: the Library list's row anatomy, relevance grouping and multi-select.

The list has to keep two states apart. "Selected" means *shown in the
preview*; "checked" means *will be added to the plot*. A dialog that conflates
them either adds a source the user only wanted to look at, or loses two queued
sources the moment the user looks at a third — so most of what these tests pin
is the independence of those two states under selection, checkbox clicks,
searching, scope toggling and a change of comparison taxon.

The rest pins the information hierarchy of a row (italic taxon headline, grey
citation below it, monospace measurement that must not be truncated, relevance
and data-semantics badges) and the fact that the semantics badge comes from the
Stage 1 projection rather than from a ``data_kind`` column.

Everything here runs against injected candidates, so no reference database is
touched.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import json

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog

from database.reference_library import MeasurementSetCandidate
from references.reference_display import display_from_row, format_measurement_expression
from ui.add_reference_dialog import (
    AddReferenceDialog,
    group_library_candidates,
    library_relevance,
)
from ui.library_source_row import LibrarySourceRow


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(picker.MeasurementSetRepository, "get", lambda _id: None)
    monkeypatch.setattr(picker, "QSettings", settings)
    monkeypatch.setattr(geometry, "QSettings", settings)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


_PERCENTILE_DETAILS = json.dumps(
    {
        "schema_version": 1,
        "metrics": {
            "length": {
                "outer_range": {"kind": "reported_extremes"},
                "core_range": {
                    "kind": "percentile_interval",
                    "percentile_bounds": [5, 95],
                },
            }
        },
    }
)

_DECILE_DETAILS = json.dumps(
    {
        "schema_version": 1,
        "metrics": {
            "length": {
                "core_range": {
                    "kind": "percentile_interval",
                    "percentile_bounds": [10, 90],
                }
            }
        },
    }
)


def _candidate(
    measurement_set_id: str,
    *,
    name_as_published: str,
    short_label: str,
    taxon_id: str | None,
    row: dict,
    locator_text: str | None = None,
    raw_text: str | None = None,
    year: int | None = None,
) -> MeasurementSetCandidate:
    return MeasurementSetCandidate(
        measurement_set_id=measurement_set_id,
        short_label=short_label,
        name_as_published=name_as_published,
        locator_text=locator_text,
        data_kind=str(row.get("data_kind") or "range"),
        raw_text=raw_text,
        revision=1,
        reference_work_id=f"w-{measurement_set_id}",
        reference_treatment_id=f"t-{measurement_set_id}",
        year=year,
        taxon_id=taxon_id,
        display=display_from_row(row),
    )


def _candidates() -> list[MeasurementSetCandidate]:
    """Three relevance tiers for a picker sitting on Cortinarius limonius."""
    return [
        # This taxon — a plain published range.
        _candidate(
            "ms-this-range",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            short_label="Niskanen et al. 2018",
            locator_text="pp. 146–148",
            taxon_id="7",
            year=2018,
            raw_text="(8.1–)8.5–10.8(–11.4) µm",
            row={
                "data_kind": "range",
                "length_core_min": 8.5,
                "length_core_max": 10.8,
                "width_core_min": 4.5,
                "width_core_max": 5.8,
            },
        ),
        # This taxon — real individual points.
        _candidate(
            "ms-this-points",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            short_label="Niskanen et al. 2018",
            locator_text="supplementary dataset S4",
            taxon_id="7",
            row={
                "data_kind": "raw_points",
                "raw_points_json": json.dumps(
                    [{"length": 9.0, "width": 5.1}, {"length": 9.6, "width": 5.4}]
                ),
            },
        ),
        # Same genus — an explicit 5–95 percentile interval.
        _candidate(
            "ms-genus-percentile",
            name_as_published="Cortinarius rubellus Cooke",
            short_label="Brandrud et al. 2020",
            taxon_id="99",
            row={
                "data_kind": "range",
                "length_min": 7.0,
                "length_max": 12.0,
                "length_core_min": 8.0,
                "length_core_max": 11.0,
                "measurement_details_json": _PERCENTILE_DETAILS,
            },
        ),
        # Rest of library — an explicit, and deliberately different,
        # percentile interval.
        _candidate(
            "ms-rest-decile",
            name_as_published="Amanita muscaria (L.) Lam.",
            short_label="Funga Nordica 2012",
            taxon_id="500",
            row={
                "data_kind": "range",
                "length_core_min": 9.0,
                "length_core_max": 11.0,
                "measurement_details_json": _DECILE_DETAILS,
            },
        ),
    ]


def _make_dialog(**kwargs) -> AddReferenceDialog:
    _app()
    defaults = {
        "taxon_label": "Cortinarius limonius",
        "taxon_id": 7,
        "genus": "Cortinarius",
        "species": "limonius",
        "candidates": _candidates(),
        "community_results": [],
        "my_observations": [],
    }
    defaults.update(kwargs)
    dialog = AddReferenceDialog(None, **defaults)
    # The whole library, not just the working taxon: grouping only has
    # something to show when more than one tier is visible.
    dialog.only_this_taxon_checkbox.setChecked(False)
    return dialog


def _headings(dialog: AddReferenceDialog) -> list[str]:
    import ui.add_reference_dialog as picker

    return [
        dialog.results_list.item(row).text()
        for row in range(dialog.results_list.count())
        if dialog.results_list.item(row).data(Qt.UserRole) == picker._GROUP_HEADING_ROLE
    ]


def _row_widget(dialog: AddReferenceDialog, measurement_set_id: str) -> LibrarySourceRow:
    for row in range(dialog.results_list.count()):
        item = dialog.results_list.item(row)
        if item.data(Qt.UserRole) == measurement_set_id:
            widget = dialog.results_list.itemWidget(item)
            assert isinstance(widget, LibrarySourceRow)
            return widget
    raise AssertionError(f"no Library row for {measurement_set_id!r}")


def _row_index(dialog: AddReferenceDialog, measurement_set_id: str) -> int:
    for row in range(dialog.results_list.count()):
        if dialog.results_list.item(row).data(Qt.UserRole) == measurement_set_id:
            return row
    raise AssertionError(f"no Library row for {measurement_set_id!r}")


# --- Relevance grouping ------------------------------------------------------


def test_relevance_is_decided_by_taxon_id_then_published_name():
    candidates = {c.measurement_set_id: c for c in _candidates()}
    kwargs = {"taxon_id": "7", "genus": "Cortinarius", "taxon_name": "Cortinarius limonius"}
    assert library_relevance(candidates["ms-this-range"], **kwargs) == "this_taxon"
    assert library_relevance(candidates["ms-genus-percentile"], **kwargs) == "same_genus"
    assert library_relevance(candidates["ms-rest-decile"], **kwargs) == "rest"


def test_relevance_still_finds_this_taxon_without_an_id():
    """An AI suggestion or a typed binomial has a name and no taxon id.

    Without the name check every row would fall through to ``same_genus``,
    which would put the exact taxon the user asked for below rows that only
    share its genus.
    """
    candidate = {c.measurement_set_id: c for c in _candidates()}["ms-this-range"]
    assert (
        library_relevance(
            candidate, taxon_id=None, genus="Cortinarius", taxon_name="Cortinarius limonius"
        )
        == "this_taxon"
    )


def test_groups_are_emitted_in_the_contract_order_with_their_members():
    grouped = group_library_candidates(
        _candidates(), taxon_id="7", genus="Cortinarius", taxon_name="Cortinarius limonius"
    )
    assert [key for key, _ in grouped] == ["this_taxon", "same_genus", "rest"]
    assert [len(members) for _, members in grouped] == [2, 1, 1]


def test_empty_groups_are_dropped_rather_than_shown_with_a_zero_count():
    only_this_taxon = [c for c in _candidates() if c.taxon_id == "7"]
    grouped = group_library_candidates(
        only_this_taxon,
        taxon_id="7",
        genus="Cortinarius",
        taxon_name="Cortinarius limonius",
    )
    assert [key for key, _ in grouped] == ["this_taxon"]


def test_list_renders_headings_with_counts_in_the_fixed_order():
    dialog = _make_dialog()
    try:
        assert _headings(dialog) == [
            "This taxon (2)",
            "Same genus (1)",
            "Rest of library (1)",
        ]
    finally:
        dialog.close()


def test_scoping_to_this_taxon_leaves_exactly_one_heading():
    dialog = _make_dialog()
    try:
        dialog.only_this_taxon_checkbox.setChecked(True)
        assert _headings(dialog) == ["This taxon (2)"]
    finally:
        dialog.close()


def test_group_headings_are_not_selectable():
    """A heading is a label. Arrow-keying onto it would clear the preview."""
    dialog = _make_dialog()
    try:
        heading_rows = [
            row
            for row in range(dialog.results_list.count())
            if dialog.results_list.itemWidget(dialog.results_list.item(row)) is None
            and dialog.results_list.item(row).text().startswith("This taxon")
        ]
        assert heading_rows
        item = dialog.results_list.item(heading_rows[0])
        assert not (item.flags() & Qt.ItemIsSelectable)
    finally:
        dialog.close()


# --- Row anatomy -------------------------------------------------------------


def test_row_shows_taxon_headline_citation_metadata_and_measurement():
    dialog = _make_dialog()
    try:
        widget = _row_widget(dialog, "ms-this-range")
        assert widget.taxon_label.full_text() == "Cortinarius limonius (Fr.) Fr."
        assert widget.taxon_label.font().italic() is True
        assert widget.citation_label.full_text() == (
            "Niskanen et al. 2018 · pp. 146–148"
        )
        assert widget.measurement_label.text() == "8.5–10.8 × 4.5–5.8 µm"
        assert widget.measurement_label.font().fixedPitch() is True
    finally:
        dialog.close()


def test_measurement_cell_is_never_truncated_to_fit_a_long_taxon():
    """The taxon elides; the only number on the row does not.

    Layout, not wording: the measurement label is added with stretch 0 and a
    Fixed policy, so it is allocated its full hint before the taxon gets any
    of the remaining width.
    """
    dialog = _make_dialog()
    dialog.show()
    try:
        widget = _row_widget(dialog, "ms-this-range")
        for source_pane_width in (600, 300):
            dialog.resize(1100, 520)
            dialog._body_splitter.setSizes([source_pane_width, 1100 - source_pane_width])
            _app().processEvents()
            assert widget.measurement_label.text() == "8.5–10.8 × 4.5–5.8 µm"
            assert (
                widget.measurement_label.width()
                >= widget.measurement_label.sizeHint().width()
            )
        # The taxon asks for very little, so it is always the label with
        # room to give; see test_a_taxon_too_long_for_the_row_elides_...
        # for what it does with too little.
        assert (
            widget.taxon_label.sizeHint().width()
            < widget.measurement_label.sizeHint().width()
        )
    finally:
        dialog.close()


def test_a_taxon_too_long_for_the_row_elides_instead_of_clipping():
    from PySide6.QtWidgets import QVBoxLayout, QWidget

    _app()
    host = QWidget()
    QVBoxLayout(host)
    row = LibrarySourceRow(
        measurement_set_id="ms-long",
        taxon="Cortinarius limonius (Fr.) Fr. with a deliberately long authority",
        citation="Niskanen et al. 2018",
        measurement="8.5–10.8 × 4.5–5.8 µm",
        parent=host,
    )
    host.layout().addWidget(row)
    try:
        host.resize(360, 80)
        host.show()
        _app().processEvents()
        assert row.taxon_label.text() != row.taxon_label.full_text()
        assert row.taxon_label.text().endswith("…")
        assert row.measurement_label.text() == "8.5–10.8 × 4.5–5.8 µm"
    finally:
        host.close()


def test_relevance_badges_name_the_relationship_only_where_there_is_one():
    dialog = _make_dialog()
    try:
        assert _row_widget(dialog, "ms-this-range").relevance_badge.text() == "Same taxon"
        assert (
            _row_widget(dialog, "ms-genus-percentile").relevance_badge.text()
            == "Same genus"
        )
        rest = _row_widget(dialog, "ms-rest-decile")
        assert rest.relevance_badge.text() == ""
        assert rest.relevance_badge.isVisibleTo(rest) is False
    finally:
        dialog.close()


def test_semantic_badges_come_from_the_projection_not_from_data_kind():
    """Each badge is the honest reading of what the source stores.

    ``ms-rest-decile`` is the one that matters: it is a percentile interval
    whose bounds are 10 and 90, and mislabelling it ``5–95% range`` would be
    a false statistical claim (design contract N14).
    """
    dialog = _make_dialog()
    try:
        assert _row_widget(dialog, "ms-this-points").semantic_badge.text() == "Raw data"
        assert (
            _row_widget(dialog, "ms-this-range").semantic_badge.text()
            == "Published range"
        )
        assert (
            _row_widget(dialog, "ms-genus-percentile").semantic_badge.text()
            == "5–95% range"
        )
        assert (
            _row_widget(dialog, "ms-rest-decile").semantic_badge.text() == "10–90% range"
        )
    finally:
        dialog.close()


def test_measurement_expression_prefers_the_extreme_pair_when_both_are_stored():
    display = display_from_row(
        {
            "data_kind": "range",
            "length_min": 7.0,
            "length_max": 12.0,
            "length_core_min": 8.0,
            "length_core_max": 11.0,
            "measurement_details_json": _PERCENTILE_DETAILS,
        }
    )
    assert format_measurement_expression(display) == "7.0–12.0 µm"


def test_measurement_expression_is_empty_when_the_source_stores_no_range():
    display = display_from_row({"data_kind": "range"})
    assert format_measurement_expression(display) == ""


# --- Selected versus checked -------------------------------------------------


def test_selecting_a_row_previews_it_without_checking_it():
    dialog = _make_dialog()
    try:
        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-this-range"))
        assert dialog._preview_candidate.measurement_set_id == "ms-this-range"
        assert dialog.checked_source_ids() == []
        assert _row_widget(dialog, "ms-this-range").is_checked() is False
    finally:
        dialog.close()


def test_checking_a_row_also_makes_it_the_preview_row():
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-genus-percentile").checkbox.setChecked(True)
        assert dialog.checked_source_ids() == ["ms-genus-percentile"]
        assert dialog._preview_candidate.measurement_set_id == "ms-genus-percentile"
    finally:
        dialog.close()


def test_preview_can_move_to_a_third_row_while_two_stay_checked():
    """The case the whole split exists for: look without adding."""
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        _row_widget(dialog, "ms-this-points").checkbox.setChecked(True)
        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-rest-decile"))

        assert dialog.checked_source_ids() == ["ms-this-range", "ms-this-points"]
        assert dialog._preview_candidate.measurement_set_id == "ms-rest-decile"
        assert _row_widget(dialog, "ms-rest-decile").is_checked() is False
    finally:
        dialog.close()


def test_unchecking_removes_only_that_source_and_keeps_the_order():
    dialog = _make_dialog()
    try:
        for ms_id in ("ms-this-range", "ms-this-points", "ms-rest-decile"):
            _row_widget(dialog, ms_id).checkbox.setChecked(True)
        _row_widget(dialog, "ms-this-points").checkbox.setChecked(False)
        assert dialog.checked_source_ids() == ["ms-this-range", "ms-rest-decile"]
    finally:
        dialog.close()


def test_selected_row_paints_the_custom_state_and_qt_blue_is_suppressed():
    dialog = _make_dialog()
    try:
        style = dialog.results_list.styleSheet()
        assert "item:selected" in style
        assert "background: transparent" in style
        # Both the active and the inactive selected state, or a row that
        # lost focus would come back as a grey block.
        assert style.count("background: transparent") == 2

        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-this-range"))
        assert _row_widget(dialog, "ms-this-range").is_selected() is True
        assert _row_widget(dialog, "ms-this-points").is_selected() is False

        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-this-points"))
        assert _row_widget(dialog, "ms-this-range").is_selected() is False
        assert _row_widget(dialog, "ms-this-points").is_selected() is True
    finally:
        dialog.close()


# --- Checked state under filtering -------------------------------------------


def test_searching_does_not_silently_uncheck_a_hidden_source():
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        dialog.search_input.setText("Funga")
        assert dialog.checked_source_ids() == ["ms-this-range"]
        assert dialog.status_hint_label.text() == "1 source selected"

        dialog.search_input.setText("")
        assert _row_widget(dialog, "ms-this-range").is_checked() is True
    finally:
        dialog.close()


def test_toggling_the_taxon_scope_preserves_checked_sources_and_their_boxes():
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-rest-decile").checkbox.setChecked(True)
        dialog.only_this_taxon_checkbox.setChecked(True)
        assert dialog.checked_source_ids() == ["ms-rest-decile"]

        dialog.only_this_taxon_checkbox.setChecked(False)
        assert _row_widget(dialog, "ms-rest-decile").is_checked() is True
        assert dialog.checked_source_ids() == ["ms-rest-decile"]
    finally:
        dialog.close()


def test_restoring_a_checkbox_during_a_rebuild_does_not_reorder_the_queue():
    """A repopulate is not a user decision.

    If the restored checkbox re-emitted ``toggled``, the row would be
    appended to ``_checked_ids`` again and the add order would follow the
    last rebuild rather than the user's choices.
    """
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-rest-decile").checkbox.setChecked(True)
        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        dialog.search_input.setText("e")  # matches every row; forces a rebuild
        assert dialog.checked_source_ids() == ["ms-rest-decile", "ms-this-range"]
    finally:
        dialog.close()


def test_rebuilding_the_list_never_schedules_a_publication_editor():
    """Filtering with a row selected must not pop the publication editor.

    ``QListWidget.clear()`` walks the current index down the rows it is
    removing, and the row that survives longest is the last one — the
    "+ New publication…" action. Left unblocked, that emits
    ``itemSelectionChanged`` with the action row selected, which schedules
    a modal editor nobody asked for. Pinned here because the symptom
    (a stray modal, one event-loop turn later) does not show up in any
    assertion about the list's end state.
    """
    dialog = _make_dialog()
    scheduled: list[int] = []
    dialog._open_new_publication_editor = lambda: scheduled.append(1)
    try:
        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-rest-decile"))
        dialog.only_this_taxon_checkbox.setChecked(True)
        dialog.search_input.setText("Niskanen")
        dialog.only_this_taxon_checkbox.setChecked(False)
        _app().processEvents()

        assert scheduled == []
        assert dialog._new_publication_editor_active is False
    finally:
        dialog.close()


def test_changing_the_comparison_taxon_clears_checked_sources():
    dialog = _make_dialog()
    try:
        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        dialog.taxon_target_combo.setCurrentText("Cortinarius rubellus")
        dialog._on_taxon_target_text_entered()

        assert dialog.checked_source_ids() == []
        assert dialog._preview_candidate is None
    finally:
        dialog.close()


# --- Footer ------------------------------------------------------------------


def test_footer_counts_up_and_says_selected_not_ready():
    dialog = _make_dialog()
    try:
        assert dialog.add_to_plot_btn.text() == "Add to plot"

        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        assert dialog.add_to_plot_btn.text() == "Add to plot"
        assert dialog.status_hint_label.text() == "1 source selected"

        _row_widget(dialog, "ms-this-points").checkbox.setChecked(True)
        assert dialog.add_to_plot_btn.text() == "Add 2 to plot"
        assert dialog.status_hint_label.text() == "2 sources selected"
        assert "ready" not in dialog.status_hint_label.text()

        _row_widget(dialog, "ms-rest-decile").checkbox.setChecked(True)
        assert dialog.add_to_plot_btn.text() == "Add 3 to plot"
    finally:
        dialog.close()


def test_one_checked_source_is_what_add_to_plot_attaches():
    received: list[tuple[str, str]] = []
    dialog = _make_dialog(attach_callback=lambda ms_id, role: received.append((ms_id, role)))
    try:
        # Checked one row, previewing another: the checked source wins,
        # because checking is the statement about the plot.
        _row_widget(dialog, "ms-genus-percentile").checkbox.setChecked(True)
        dialog.results_list.setCurrentRow(_row_index(dialog, "ms-rest-decile"))
        dialog._on_add_to_plot_clicked()
        assert received == [("ms-genus-percentile", "compared")]
    finally:
        dialog.close()


def test_multi_source_add_is_disabled_rather_than_half_performed():
    """Stage 2's reported contract gap, pinned so it cannot regress quietly.

    ``attach_callback`` returns ``None`` for success and for every failure
    alike, so a batch loop could not report which sources landed. Until that
    callback can report failure, the footer counts the queue but the click
    attaches nothing rather than attaching some and closing on a claim of
    success it cannot support.
    """
    received: list[tuple[str, str]] = []
    dialog = _make_dialog(attach_callback=lambda ms_id, role: received.append((ms_id, role)))
    try:
        _row_widget(dialog, "ms-this-range").checkbox.setChecked(True)
        _row_widget(dialog, "ms-this-points").checkbox.setChecked(True)

        assert dialog.add_to_plot_btn.text() == "Add 2 to plot"
        assert dialog.add_to_plot_btn.isEnabled() is False
        assert dialog.add_to_plot_btn.toolTip() != ""

        dialog._on_add_to_plot_clicked()
        assert received == []
        assert dialog.result() != QDialog.Accepted
    finally:
        dialog.close()


# --- The action row is not a source ------------------------------------------


def test_new_publication_row_is_an_action_and_never_checkable():
    import ui.add_reference_dialog as picker

    dialog = _make_dialog()
    try:
        rows = [
            row
            for row in range(dialog.results_list.count())
            if dialog.results_list.item(row).data(Qt.UserRole)
            == picker._NEW_PUBLICATION_ROLE
        ]
        assert len(rows) == 1
        item = dialog.results_list.item(rows[0])
        assert dialog.results_list.itemWidget(item) is None
        assert not (item.flags() & Qt.ItemIsUserCheckable)
        # It is the last row, below every group, so it never breaks a group
        # apart.
        assert rows[0] == dialog.results_list.count() - 1
    finally:
        dialog.close()
