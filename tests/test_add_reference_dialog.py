"""Model-level tests for ui.add_reference_dialog.AddReferenceDialog.

Exercises the Library tab's taxon filter and search-filter AND-combination,
the My-observations tab's candidate loading and row rendering, and the
Add-to-plot enable/disable state and callback dispatch for both tabs --
without touching persistence or the plotting path (``attach_callback`` is a
stub). Uses the ``candidates=``/``my_observations=`` constructor overrides
(mirroring ReferenceLibraryAttachDialog's testability convention) so no real
database is needed.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QDialog

from database.reference_library import MeasurementSetCandidate
from ui.add_reference_dialog import (
    AddReferenceDialog,
    PersonalObservationCandidate,
    default_my_observation_candidates,
    filter_library_candidates,
    format_ai_candidate_display,
)


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
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _candidates() -> list[MeasurementSetCandidate]:
    return [
        MeasurementSetCandidate(
            measurement_set_id="ms-1",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148",
            data_kind="range",
            raw_text="(8.1–)8.5–10.8(–11.4) µm",
            revision=1,
            reference_work_id="w-1",
            reference_treatment_id="t-1",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-2",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="supplementary dataset S4",
            data_kind="raw_points",
            raw_text="8 paired holotype measurements",
            revision=1,
            reference_work_id="w-1",
            reference_treatment_id="t-1",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-3",
            short_label="Brandrud et al. 2020",
            name_as_published="Cortinarius rubellus Cooke",
            locator_text="Vol. 2, p. 311",
            data_kind="range",
            raw_text="8.0–9.5 µm",
            revision=1,
            reference_work_id="w-2",
            reference_treatment_id="t-2",
            taxon_id="99",
        ),
    ]


def _make_dialog(**kwargs) -> AddReferenceDialog:
    _app()
    defaults = {
        "taxon_label": "Cortinarius limonius",
        "taxon_id": 7,
        "candidates": _candidates(),
        # Injected (even empty) so the Community tab never spawns a real
        # network search thread just from constructing the dialog.
        "community_results": [],
        "my_observations": [],
    }
    defaults.update(kwargs)
    return AddReferenceDialog(None, **defaults)


def _result_row_for(dialog: AddReferenceDialog, measurement_set_id: str) -> int:
    from PySide6.QtCore import Qt

    for row in range(dialog.results_list.count()):
        item = dialog.results_list.item(row)
        if item.data(Qt.UserRole) == measurement_set_id:
            return row
    raise AssertionError(f"no results-list row for {measurement_set_id!r}")


def _ai_candidates() -> list[dict]:
    return [
        {
            "source": "arts",
            "scientific_name": "Cortinarius rubellus",
            "vernacular": "Bittersnerlerørsopp",
            "genus": "Cortinarius",
            "species": "rubellus",
            "score": 0.82,
        },
    ]


def test_format_ai_candidate_display():
    text = format_ai_candidate_display(_ai_candidates()[0])
    assert text == "Cortinarius rubellus (Bittersnerlerørsopp)  82%"


def test_taxon_target_combo_defaults_to_own_taxon():
    dialog = _make_dialog(genus="Cortinarius", species="limonius")
    assert dialog.taxon_target_combo.currentIndex() == 0
    assert "Cortinarius limonius" in dialog.taxon_target_combo.currentText()


def test_selecting_ai_candidate_refilters_library_and_updates_title():
    dialog = _make_dialog(
        genus="Cortinarius", species="limonius", ai_candidates=_ai_candidates()
    )
    assert dialog.taxon_target_combo.count() == 2
    # A real click both moves currentIndex and emits activated(); the two
    # are driven separately here to exercise the same path.
    dialog.taxon_target_combo.setCurrentIndex(1)
    dialog._on_taxon_target_activated(1)

    assert dialog._genus == "Cortinarius"
    assert dialog._species == "rubellus"
    assert dialog._taxon_id is None
    assert "Cortinarius rubellus" in dialog.windowTitle()
    assert "82%" in dialog.taxon_target_combo.currentText()
    # ms-3 is published as "Cortinarius rubellus Cooke" with no taxon_id
    # match against the picker's target -- the text fallback must still
    # narrow to it.
    assert {c.measurement_set_id for c in dialog._filtered_candidates()} == {"ms-3"}


def test_typing_arbitrary_taxon_filters_library_via_text_match():
    dialog = _make_dialog(genus="Cortinarius", species="limonius")
    dialog.taxon_target_combo.setCurrentText("Cortinarius rubellus")
    dialog._on_taxon_target_text_entered()

    assert dialog._genus == "Cortinarius"
    assert dialog._species == "rubellus"
    assert {c.measurement_set_id for c in dialog._filtered_candidates()} == {"ms-3"}


def test_typed_text_with_only_one_word_is_ignored():
    dialog = _make_dialog(genus="Cortinarius", species="limonius")
    dialog.taxon_target_combo.setCurrentText("Cortinarius")
    dialog._on_taxon_target_text_entered()
    # Unchanged: still the taxon the dialog opened on.
    assert dialog._genus == "Cortinarius"
    assert dialog._species == "limonius"


def test_only_this_taxon_defaults_checked_even_without_taxon_id():
    dialog = _make_dialog(taxon_id=None, genus="", species="")
    assert dialog.only_this_taxon_checkbox.isChecked() is True


def test_unchecking_only_this_taxon_shows_taxon_in_row_detail():
    dialog = _make_dialog()
    dialog.only_this_taxon_checkbox.setChecked(False)
    row = _result_row_for(dialog, "ms-3")
    widget = dialog.results_list.itemWidget(dialog.results_list.item(row))
    assert "Cortinarius rubellus" in widget._full_detail


def test_changing_taxon_target_never_touches_exclude_observation_id():
    dialog = _make_dialog(
        genus="Cortinarius", species="limonius", ai_candidates=_ai_candidates()
    )
    before = dialog._exclude_observation_id
    dialog._on_taxon_target_activated(1)
    assert dialog._exclude_observation_id == before


def test_derived_minimum_size_fits_both_tab_bars():
    """The dialog's minimum width must be at least the source tab bar's own
    size hint plus the preview pane's sub-tab bar's own size hint -- the
    exact condition that avoids the QTabWidget scroll-arrow fallback (see
    stage-4b-fix Part 2.2). No hardcoded pixel width is asserted here; the
    check is relative to the widgets' own hints.
    """
    dialog = _make_dialog()
    min_size = dialog.minimumSize()
    source_tabbar_w = dialog.tabs.tabBar().sizeHint().width()
    preview_tabbar_w = dialog.preview_pane.review_tabs.tabBar().sizeHint().width()
    assert min_size.width() >= source_tabbar_w + preview_tabbar_w


def test_only_this_taxon_checked_shows_only_working_taxon_sets():
    dialog = _make_dialog()
    assert dialog.only_this_taxon_checkbox.isChecked() is True
    filtered = dialog._filtered_candidates()
    assert {c.measurement_set_id for c in filtered} == {"ms-1", "ms-2"}


def test_only_this_taxon_unchecked_shows_all_sets():
    dialog = _make_dialog()
    dialog.only_this_taxon_checkbox.setChecked(False)
    filtered = dialog._filtered_candidates()
    assert {c.measurement_set_id for c in filtered} == {"ms-1", "ms-2", "ms-3"}


def test_search_filter_combines_with_taxon_filter():
    dialog = _make_dialog()
    # "Brandrud" only matches ms-3, which is a different taxon -- AND
    # semantics mean it must be excluded while "Only this taxon" is checked.
    dialog.search_input.setText("Brandrud")
    assert dialog._filtered_candidates() == []

    dialog.only_this_taxon_checkbox.setChecked(False)
    filtered = dialog._filtered_candidates()
    assert {c.measurement_set_id for c in filtered} == {"ms-3"}


def test_search_filter_narrows_within_working_taxon():
    dialog = _make_dialog()
    # "holotype" appears only in ms-2's raw expression -- confirms the
    # search matches raw expression text, not just publication/taxon.
    dialog.search_input.setText("holotype")
    filtered = dialog._filtered_candidates()
    assert {c.measurement_set_id for c in filtered} == {"ms-2"}


def test_filter_library_candidates_pure_function_matches_dialog_behavior():
    candidates = _candidates()
    only_taxon = filter_library_candidates(
        candidates, taxon_id="7", only_this_taxon=True
    )
    assert {c.measurement_set_id for c in only_taxon} == {"ms-1", "ms-2"}
    all_candidates = filter_library_candidates(
        candidates, taxon_id="7", only_this_taxon=False
    )
    assert {c.measurement_set_id for c in all_candidates} == {"ms-1", "ms-2", "ms-3"}


def test_add_to_plot_disabled_with_no_selection():
    dialog = _make_dialog()
    assert dialog.add_to_plot_btn.isEnabled() is False


def test_add_to_plot_enabled_once_a_result_is_selected():
    dialog = _make_dialog()
    dialog.results_list.setCurrentRow(_result_row_for(dialog, "ms-1"))
    assert dialog._selected_candidate is not None
    assert dialog._selected_candidate.measurement_set_id == "ms-1"
    assert dialog.add_to_plot_btn.isEnabled() is True


def test_add_to_plot_invokes_callback_and_accepts():
    received = []
    dialog = _make_dialog(attach_callback=lambda ms_id, role: received.append((ms_id, role)))
    dialog.results_list.setCurrentRow(_result_row_for(dialog, "ms-1"))
    dialog._on_add_to_plot_clicked()
    assert received == [("ms-1", "compared")]


def test_library_row_renders_two_lines_title_and_detail():
    dialog = _make_dialog()
    row = _result_row_for(dialog, "ms-2")
    widget = dialog.results_list.itemWidget(dialog.results_list.item(row))
    assert "Niskanen" in widget.title_label.text()
    # data_kind + raw_text, per the row-anatomy spec (kind + raw expression).
    assert "raw_points" in widget.detail_label.text()
    assert "8 paired holotype measurements" in widget.detail_label.text()


def test_preview_summary_falls_back_to_core_bounds_when_extremes_missing(monkeypatch):
    """Stage-6 manual test 2, defect B: a source reported only as a typical
    (unparenthesised) range -- e.g. "7-12 x 4-6, q=1.5-1.8" -- is stored with
    ``length_min``/``width_min``/``max`` left ``None`` and the typical bound
    in ``length_core_min``/``core_max``/``width_core_min``/``core_max``
    (mirrors how ``ReferenceEntryEditor.normalized_measurement_set_payload``
    writes Q's own extreme-or-typical fallback, and how
    ``references.reference_plotting`` already resolves a drawable rectangle).
    The Summary pane must show the core bound instead of "--", exactly like
    the list's raw-text line already does."""
    from database.reference_library import MeasurementSet
    from ui import add_reference_dialog as picker

    measurement_set = MeasurementSet(
        id="ms-1",
        taxon_treatment_id="t-1",
        character="spore_size",
        data_kind="range",
        raw_text="7-12 x 4-6, q=1.5-1.8",
        length_core_min=7.0,
        length_core_max=12.0,
        width_core_min=4.0,
        width_core_max=6.0,
        q_min=1.5,
        q_max=1.8,
    )
    monkeypatch.setattr(
        picker.MeasurementSetRepository, "get", lambda _id: measurement_set
    )
    dialog = _make_dialog()
    dialog.results_list.setCurrentRow(_result_row_for(dialog, "ms-1"))

    table = dialog.preview_pane.summary_table
    length_row = [table.item(0, col).text() for col in range(4)]
    width_row = [table.item(1, col).text() for col in range(4)]
    q_row = [table.item(2, col).text() for col in range(4)]
    # Length/Width bounds are derived from the core/typical fallback (marked
    # with the existing "†" derived-cell convention); Q's bounds were
    # written directly (no core fallback field exists for Q) and carry no
    # derived marker.
    assert length_row == ["Length", "7.00 †", "—", "12.00 †"]
    assert width_row == ["Width", "4.00 †", "—", "6.00 †"]
    assert q_row == ["Q", "1.50", "—", "1.80"]


def test_preview_summary_extreme_bounds_win_over_core_when_both_present(monkeypatch):
    from database.reference_library import MeasurementSet
    from ui import add_reference_dialog as picker

    measurement_set = MeasurementSet(
        id="ms-1",
        taxon_treatment_id="t-1",
        character="spore_size",
        data_kind="range",
        length_min=6.5,
        length_core_min=7.0,
        length_core_max=12.0,
        length_max=12.5,
    )
    monkeypatch.setattr(
        picker.MeasurementSetRepository, "get", lambda _id: measurement_set
    )
    dialog = _make_dialog()
    dialog.results_list.setCurrentRow(_result_row_for(dialog, "ms-1"))

    table = dialog.preview_pane.summary_table
    length_row = [table.item(0, col).text() for col in range(4)]
    assert length_row == ["Length", "6.50", "—", "12.50"]


# ---------------------------------------------------------------------
# My observations tab
# ---------------------------------------------------------------------


def _my_observations() -> list[PersonalObservationCandidate]:
    return [
        PersonalObservationCandidate(
            observation_id=101,
            date="2024-05-01",
            author="Åse Øyen",
            location="Trøndelag, æøå-lokalitet med et forbausende langt navn på over seksti tegn",
            points=[{"length_um": 8.0, "width_um": 5.0}, {"length_um": 9.0, "width_um": 5.5}],
        ),
        PersonalObservationCandidate(
            observation_id=102,
            date="2023-11-20",
            author="",
            location="",
            points=[{"length_um": 7.5, "width_um": 4.5}],
        ),
    ]


def _my_obs_row_for(dialog: AddReferenceDialog, observation_id: int) -> int:
    for row in range(dialog.my_observations_list.count()):
        item = dialog.my_observations_list.item(row)
        if item.data(Qt.UserRole) == observation_id:
            return row
    raise AssertionError(f"no my-observations row for {observation_id!r}")


def _make_my_obs_dialog(**kwargs) -> AddReferenceDialog:
    _app()
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        candidates=[],
        my_observations=_my_observations(),
        **kwargs,
    )


def test_my_observations_tab_lists_injected_candidates():
    dialog = _make_my_obs_dialog()
    assert dialog.my_observations_list.count() == 2


def test_my_observations_detail_line_shows_date_n_and_locality():
    dialog = _make_my_obs_dialog()
    row = _my_obs_row_for(dialog, 101)
    widget = dialog.my_observations_list.itemWidget(dialog.my_observations_list.item(row))
    detail = widget._full_detail
    assert "2024-05-01" in detail
    assert "n = 2" in detail
    assert "Trøndelag" in detail


def test_my_observations_detail_line_omits_locality_when_unavailable():
    dialog = _make_my_obs_dialog()
    row = _my_obs_row_for(dialog, 102)
    widget = dialog.my_observations_list.itemWidget(dialog.my_observations_list.item(row))
    detail = widget._full_detail
    assert "2023-11-20" in detail
    assert "n = 1" in detail


def test_my_observations_selection_populates_shared_preview_pane():
    dialog = _make_my_obs_dialog()
    dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
    dialog.my_observations_list.setCurrentRow(_my_obs_row_for(dialog, 101))
    assert "n = 2" in dialog.preview_pane.summary_note_label.text()
    assert "8.0" in dialog.preview_pane.raw_spores_text.toPlainText()


def test_my_observations_add_to_plot_uses_observation_prefixed_identifier():
    received = []
    dialog = _make_my_obs_dialog(
        attach_callback=lambda identifier, role: received.append((identifier, role))
    )
    dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
    dialog.my_observations_list.setCurrentRow(_my_obs_row_for(dialog, 101))
    dialog._on_add_to_plot_clicked()
    assert received == [("observation:101", "compared")]


def test_my_observations_add_to_plot_disabled_with_no_selection():
    dialog = _make_my_obs_dialog()
    dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
    assert dialog.add_to_plot_btn.isEnabled() is False


# ---------------------------------------------------------------------
# Community tab
# ---------------------------------------------------------------------


def _community_results() -> list[dict]:
    return [
        {
            "_kind": "observation",
            "observation_id": 4242,
            "genus": "Cortinarius",
            "species": "limonius",
            "contributor_label": "sporely_community_user_7",
            "observed_on": "2025-05-01",
            "measurement_count": 3,
            "q_min": 1.1,
            "q_p50": 1.3,
            "q_max": 1.5,
            "measurements_json": [
                {"length_um": 8.0, "width_um": 5.0},
                {"length_um": 9.0, "width_um": 6.0},
                {"length_um": 10.0, "width_um": 7.0},
            ],
            "length_min": 8.0,
            "length_p50": 9.0,
            "length_max": 10.0,
            "width_min": 5.0,
            "width_p50": 6.0,
            "width_max": 7.0,
        },
        {
            "_kind": "reference",
            "genus": "Cortinarius",
            "species": "limonius",
            "source": "mycena.no",
            "contributor_label": "mycena.no",
            "measurement_count": 0,
            "length_min": 8.0,
            "length_max": 10.5,
        },
    ]


def _make_community_dialog(**kwargs) -> AddReferenceDialog:
    _app()
    kwargs.setdefault("community_results", _community_results())
    kwargs.setdefault("my_observations", [])
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        genus="Cortinarius",
        species="limonius",
        candidates=[],
        **kwargs,
    )


def test_community_tab_lists_injected_results():
    dialog = _make_community_dialog()
    assert dialog._community_pane.results_list.count() == 2


def test_community_selection_populates_shared_preview_pane():
    dialog = _make_community_dialog()
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    dialog._community_pane.results_list.setCurrentRow(0)
    assert "Cortinarius limonius" in dialog.preview_pane.summary_title_label.text()
    assert "8.0" in dialog.preview_pane.raw_spores_text.toPlainText()


def test_community_add_to_plot_range_summary_uses_reference_source_kind():
    received = []
    dialog = _make_community_dialog(cloud_attach_callback=lambda data: received.append(data))
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    dialog._community_pane.results_list.setCurrentRow(0)
    assert dialog._community_pane.range_summary_radio.isChecked() is True
    dialog._on_add_to_plot_clicked()
    assert len(received) == 1
    assert received[0]["source_kind"] == "reference"


def test_community_add_to_plot_raw_points_uses_points_source_kind_and_real_n():
    received = []
    dialog = _make_community_dialog(cloud_attach_callback=lambda data: received.append(data))
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    dialog._community_pane.results_list.setCurrentRow(0)
    assert "n=3" in dialog._community_pane.raw_points_radio.text()
    dialog._community_pane.raw_points_radio.setChecked(True)
    dialog._on_add_to_plot_clicked()
    assert len(received) == 1
    assert received[0]["source_kind"] == "points"
    assert len(received[0]["points"]) == 3


def test_community_raw_points_radio_disabled_without_measurements():
    dialog = _make_community_dialog()
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    dialog._community_pane.results_list.setCurrentRow(1)
    assert dialog._community_pane.raw_points_radio.isEnabled() is False
    assert dialog._community_pane.range_summary_radio.isChecked() is True


def test_community_add_to_plot_disabled_with_no_selection():
    dialog = _make_community_dialog()
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    assert dialog.add_to_plot_btn.isEnabled() is False


def test_community_tab_shows_empty_state_with_no_results():
    dialog = _make_community_dialog(community_results=[])
    assert dialog._community_pane.results_list.count() == 0
    assert dialog._community_pane.status_label.text() != ""


def test_community_excludes_exact_current_observation_by_cloud_id():
    """The dataset built from the observation already being plotted must not
    reappear as its own comparison candidate (stage-6 manual test 2, defect A)."""
    dialog = _make_community_dialog(exclude_observation_cloud_id="4242")
    # _community_results() has one "observation" row with observation_id=4242
    # (the self-reference) and one "reference" row -- only the self-reference
    # is dropped.
    assert dialog._community_pane.results_list.count() == 1
    remaining = dialog._community_pane._results
    assert len(remaining) == 1
    assert remaining[0]["_kind"] == "reference"


def test_community_keeps_other_observation_of_same_taxon():
    """A different observation's dataset for the same taxon is a valid
    comparison candidate and must not be filtered by taxon identity."""
    dialog = _make_community_dialog(exclude_observation_cloud_id="9999")
    assert dialog._community_pane.results_list.count() == 2
    kinds = [row["_kind"] for row in dialog._community_pane._results]
    assert kinds == ["observation", "reference"]


def test_community_unrelated_taxon_rows_unaffected_by_exclusion():
    """Rows for other taxa (never matching the excluded cloud id) pass
    through the exclusion filter untouched."""
    results = _community_results() + [
        {
            "_kind": "observation",
            "observation_id": 7777,
            "genus": "Amanita",
            "species": "muscaria",
            "contributor_label": "sporely_community_user_9",
            "measurement_count": 5,
        }
    ]
    dialog = _make_community_dialog(
        community_results=results, exclude_observation_cloud_id="4242"
    )
    remaining_ids = [
        row.get("observation_id")
        for row in dialog._community_pane._results
        if row["_kind"] == "observation"
    ]
    assert remaining_ids == [7777]


def test_community_no_exclusion_id_keeps_all_rows():
    dialog = _make_community_dialog()
    assert dialog._community_pane.results_list.count() == 2


def test_default_my_observation_candidates_filters_by_taxon_and_requires_points(monkeypatch):
    from ui import add_reference_dialog as mod

    def fake_get_personal_observations_for_species(genus, species, exclude_observation_id=None):
        assert (genus, species) == ("Cortinarius", "limonius")
        assert exclude_observation_id == 42
        return [
            {"id": 201, "date": "2024-01-01", "author": "A"},
            {"id": 202, "date": "2024-02-02", "author": "B"},
        ]

    def fake_get_measurements_for_observation(observation_id):
        # Observation 202 has no usable measurements and must be excluded.
        if observation_id == 201:
            return [{"length_um": 8.0, "width_um": 5.0, "measurement_type": "spore"}]
        return []

    def fake_get_observation(observation_id):
        return {"location": "Oppland"} if observation_id == 201 else {}

    monkeypatch.setattr(
        mod.ObservationDB,
        "get_personal_observations_for_species",
        staticmethod(fake_get_personal_observations_for_species),
    )
    monkeypatch.setattr(
        mod.MeasurementDB,
        "get_measurements_for_observation",
        staticmethod(fake_get_measurements_for_observation),
    )
    monkeypatch.setattr(
        mod.ObservationDB, "get_observation", staticmethod(fake_get_observation)
    )

    result = default_my_observation_candidates(
        "Cortinarius", "limonius", exclude_observation_id=42
    )
    assert [c.observation_id for c in result] == [201]
    assert result[0].location == "Oppland"
    assert result[0].n == 1


# ---------------------------------------------------------------------
# Return-through-the-real-signal-path taxon target regressions
# (stage-4b-fix2 Part 2 -- see review at
# ui/add_reference_dialog.py:425-428,488-498)
# ---------------------------------------------------------------------


def test_return_after_selecting_ai_candidate_preserves_structured_taxon():
    """Selecting an AI candidate (vernacular name + 82% in its label), then
    pressing Return through the actual Qt input/signal path, must retain the
    candidate's exact structured genus/species rather than re-parsing the
    combo's display text -- which would fold "(Bittersnerlerørsopp)  82%"
    into the species. Must also not trigger an unintended dialog submission.
    """
    dialog = _make_dialog(
        genus="Cortinarius", species="limonius", ai_candidates=_ai_candidates()
    )
    accepted = []
    dialog.accepted.connect(lambda: accepted.append(True))
    dialog.show()
    QTest.keyClick(dialog.taxon_target_combo, Qt.Key_Down)
    dialog.add_to_plot_btn.setEnabled(True)
    submissions = []
    dialog.add_to_plot_btn.clicked.connect(lambda: submissions.append(True))
    assert dialog._species == "rubellus"

    line_edit = dialog.taxon_target_combo.lineEdit()
    line_edit.setFocus()
    QTest.keyClick(line_edit, Qt.Key_Return)

    assert dialog._genus == "Cortinarius"
    assert dialog._species == "rubellus"
    assert dialog._taxon_id is None
    assert "82%" in dialog.taxon_target_combo.currentText()
    assert accepted == []
    assert submissions == []
    dialog.reject()


def test_return_after_own_taxon_selection_unchanged_keeps_own_target():
    """Same sequence for the default own-taxon entry (index 0): Return with
    unchanged display text must not corrupt the own genus/species/ID either.
    """
    dialog = _make_dialog(genus="Cortinarius", species="limonius", taxon_id=7)
    line_edit = dialog.taxon_target_combo.lineEdit()
    line_edit.setFocus()
    QTest.keyClick(line_edit, Qt.Key_Return)

    assert dialog._genus == "Cortinarius"
    assert dialog._species == "limonius"
    assert dialog._taxon_id == "7"


def test_return_with_deliberately_edited_text_still_parses_free_taxon():
    """Editing the combo's text to a genus/species absent from any item, then
    pressing Return through the real signal path, must still parse the free
    text using the existing supported genus/species semantics and update the
    filter/title -- the correction must not disable arbitrary entry.
    """
    dialog = _make_dialog(genus="Cortinarius", species="limonius")
    line_edit = dialog.taxon_target_combo.lineEdit()
    dialog.taxon_target_combo.setCurrentText("Amanita muscaria")
    QTest.keyClick(line_edit, Qt.Key_Return)

    assert dialog._genus == "Amanita"
    assert dialog._species == "muscaria"
    assert "Amanita muscaria" in dialog.windowTitle()
    assert dialog._filtered_candidates() == []
    assert dialog._community_pane._genus == "Amanita"
    assert dialog._community_pane._species == "muscaria"


# ---------------------------------------------------------------------
# Enter-manually tab (stage 4c)
# ---------------------------------------------------------------------


def _set_manual_range(dialog: AddReferenceDialog, *, length=(8.0, 11.0), width=(6.0, 8.0)) -> None:
    from PySide6.QtWidgets import QTableWidgetItem

    editor = dialog.manual_editor
    editor.minmax_table.setItem(0, 0, QTableWidgetItem(f"{length[0]:.2f}"))
    editor.minmax_table.setItem(0, 4, QTableWidgetItem(f"{length[1]:.2f}"))
    editor.minmax_table.setItem(1, 0, QTableWidgetItem(f"{width[0]:.2f}"))
    editor.minmax_table.setItem(1, 4, QTableWidgetItem(f"{width[1]:.2f}"))


def test_manual_tab_uses_pickers_observation_and_own_taxon_id():
    dialog = _make_dialog(taxon_id=7)
    assert dialog.manual_editor._observation_taxon_id == 7
    assert dialog.manual_editor._sporely_taxon_id == 7


def test_manual_tab_add_to_plot_disabled_until_valid_input():
    dialog = _make_dialog()
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    assert dialog.add_to_plot_btn.isEnabled() is False
    _set_manual_range(dialog)
    assert dialog.add_to_plot_btn.isEnabled() is False
    assert dialog.save_to_library_btn.isEnabled() is True


def test_manual_tab_invalid_input_cannot_submit(monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    received = []
    dialog = _make_dialog(manual_attach_callback=lambda editor: received.append(editor) or True)
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    dialog._on_add_to_plot_clicked()
    assert received == []
    assert dialog.result() != QDialog.Accepted


@pytest.mark.parametrize("save_succeeds", [False, True])
def test_manual_save_then_attach_are_separate(monkeypatch, save_succeeds):
    saved, attached = [], []
    dialog = _make_dialog(
        manual_save_callback=lambda editor: saved.append(editor) or ("set-saved" if save_succeeds else None),
        attach_callback=lambda identifier, role: attached.append((identifier, role)) or True,
    )
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    _set_manual_range(dialog)
    dialog.manual_editor._selected_work_id = "work-1"
    dialog._on_add_to_plot_clicked()
    assert not saved and not attached
    dialog._on_save_to_library_clicked()
    assert saved == [dialog.manual_editor]
    assert not attached
    assert dialog.result() != QDialog.Accepted
    assert dialog.add_to_plot_btn.isEnabled() is save_succeeds
    if save_succeeds:
        dialog._on_save_to_library_clicked()
        assert len(saved) == 1
        dialog._on_add_to_plot_clicked()
        assert attached == [("set-saved", "compared")]
        assert dialog.result() == QDialog.Accepted


def test_saved_manual_set_stays_open_when_attachment_fails():
    dialog = _make_dialog(attach_callback=lambda *_: False)
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    dialog._saved_manual_set_id = "saved-set"
    dialog._on_add_to_plot_clicked()
    assert dialog.result() != QDialog.Accepted


def test_changing_taxon_target_resets_manual_editor_publication_state():
    dialog = _make_dialog(taxon_id=7, ai_candidates=_ai_candidates())
    _set_manual_range(dialog)
    dialog.manual_editor._selected_work_id = "w-1"
    dialog.taxon_target_combo.setCurrentIndex(1)
    dialog.taxon_target_combo.activated.emit(1)
    assert dialog.manual_editor._selected_work_id is None
    assert dialog.manual_editor._sporely_taxon_id is None  # AI candidate has no taxon_id
    assert dialog.manual_editor._genus == "Cortinarius"
    assert dialog.manual_editor._species == "rubellus"
    # Entered measurement values survive the target switch.
    assert dialog.manual_editor._table_value(0, 0) == 8.0


def test_switching_to_manual_tab_resyncs_shared_preview_pane():
    dialog = _make_dialog()
    _set_manual_range(dialog)
    dialog.tabs.setCurrentIndex(0)  # Library tab
    dialog.preview_pane.clear()
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    assert dialog.preview_pane.summary_table.item(0, 1).text() == "8.00"


# ---------------------------------------------------------------------
# Host integration -- MainWindow._on_add_reference_clicked
# (stage-4b-fix2 Part 3 -- see review at ui/main_window.py:11540-11545)
# ---------------------------------------------------------------------


@pytest.mark.parametrize("observation", [
    {"genus": "Cortinarius", "species": "limonius", "sporely_taxon_id": 7},
    {"genus": "Cortinarius", "species": "limonius", "sporely_taxon_id": None},
    {"genus": "", "species": "", "sporely_taxon_id": None},
])
def test_host_own_target_uses_captured_observation(monkeypatch, observation):
    from types import SimpleNamespace
    from unittest.mock import Mock
    import ui.main_window as host

    _app()
    reads = Mock(return_value=dict(observation))
    monkeypatch.setattr(host.ObservationDB, "get_observation", reads)
    captured = []
    writes = Mock(side_effect=AssertionError("picker must not write identity"))
    monkeypatch.setattr(host.ObservationDB, "update_observation", writes)
    window = SimpleNamespace(
        active_observation_id=42,
        species_availability=SimpleNamespace(),
        ref_genus_input=SimpleNamespace(text=lambda: "Amanita"),
        ref_species_input=SimpleNamespace(text=lambda: "muscaria"),
        _current_attached_measurement_set_ids=lambda: set(),
        _collect_reference_ai_suggestions=_ai_candidates,
    )
    for name in ("_clean_ref_genus_text", "_clean_ref_species_text", "_active_sporely_taxon_id"):
        setattr(window, name, getattr(host.MainWindow, name).__get__(window))

    class Dialog:
        def __init__(self, parent, **kwargs):
            captured.append(kwargs)

        def exec(self):
            kwargs = dict(captured[0])
            kwargs.update(candidates=_candidates(), my_observations=[], community_results=[])
            dialog = AddReferenceDialog(None, **kwargs)
            own = dialog.taxon_target_combo.itemData(0)
            assert own["genus"] == observation["genus"]
            assert own["species"] == observation["species"]
            dialog.taxon_target_combo.setCurrentIndex(1)
            dialog.taxon_target_combo.activated.emit(1)
            dialog.reject()
            return 0

    monkeypatch.setattr(host, "AddReferenceDialog", Dialog)
    host.MainWindow._on_add_reference_clicked(window)
    kwargs = captured[0]
    assert kwargs["taxon_id"] == observation["sporely_taxon_id"]
    assert kwargs["genus"] == observation["genus"]
    assert kwargs["species"] == observation["species"]
    assert kwargs["taxon_label"] == " ".join(
        part for part in (observation["genus"], observation["species"]) if part
    )
    assert all(call.args == (42,) for call in reads.call_args_list)
    writes.assert_not_called()


def _make_host_window_for_manual_callback(monkeypatch):
    """Build a minimal ``MainWindow``-like namespace and capture the
    ``manual_attach_callback`` ``_on_add_reference_clicked`` builds for the
    picker, without constructing a real ``AddReferenceDialog``.
    """
    from types import SimpleNamespace
    from unittest.mock import Mock
    import ui.main_window as host

    _app()
    monkeypatch.setattr(
        host.ObservationDB,
        "get_observation",
        Mock(return_value={"genus": "Cortinarius", "species": "limonius", "sporely_taxon_id": 7}),
    )
    window = SimpleNamespace(
        active_observation_id=42,
        species_availability=SimpleNamespace(),
        tr=lambda text: text,
        _current_attached_measurement_set_ids=lambda: set(),
        _collect_reference_ai_suggestions=_ai_candidates,
        _submit_reference_editor_result=Mock(),
    )
    for name in ("_clean_ref_genus_text", "_clean_ref_species_text", "_active_sporely_taxon_id"):
        setattr(window, name, getattr(host.MainWindow, name).__get__(window))

    captured = []

    class Dialog:
        def __init__(self, parent, **kwargs):
            captured.append(kwargs)

        def exec(self):
            return 0

    monkeypatch.setattr(host, "AddReferenceDialog", Dialog)
    host.MainWindow._on_add_reference_clicked(window)
    return window, captured[0]


def test_manual_callback_routes_through_shared_submission_helper(monkeypatch):
    """Both the legacy Quick-add dialog and the picker's manual tab must
    route through the same shared submission helper -- exercised here via
    the callback ``_on_add_reference_clicked`` builds for
    ``manual_attach_callback``.
    """
    window, kwargs = _make_host_window_for_manual_callback(monkeypatch)
    editor = object()  # opaque stand-in; the callback never inspects it
    window._submit_reference_editor_result.return_value = True
    result = kwargs["manual_attach_callback"](editor)
    assert result is True
    window._submit_reference_editor_result.assert_called_once_with(
        editor, sync_panel=False
    )


def test_manual_callback_rejects_when_observation_drifted(monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: None)
    window, kwargs = _make_host_window_for_manual_callback(monkeypatch)
    window.active_observation_id = 999  # drifted since the picker opened
    editor = object()
    result = kwargs["manual_attach_callback"](editor)
    assert result is False
    window._submit_reference_editor_result.assert_not_called()


def test_manual_callback_propagates_failed_submission(monkeypatch):
    window, kwargs = _make_host_window_for_manual_callback(monkeypatch)
    window._submit_reference_editor_result.return_value = False
    assert kwargs["manual_attach_callback"](object()) is False
