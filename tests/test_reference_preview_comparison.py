"""The Summary tab renders the comparison, and the Raw spores tab stays honest.

Companion to ``tests/test_reference_comparison.py``: that file pins the model,
this one pins what the widgets do with it inside the real Add-reference dialog
— which body of the Summary tab is on screen, what the captions actually say,
and that a source without individual measurements produces an explanation
rather than rows.

These are content assertions, not layout ones. A screenshot would show that
the bands are drawn; only a test can show that the band labelled "typical
range" is the one the source actually described that way.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from database.reference_library import MeasurementSet, MeasurementSetCandidate
from references.reference_comparison import point_extents
from ui import add_reference_dialog as picker
from ui.add_reference_dialog import AddReferenceDialog


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(picker, "QSettings", settings, raising=False)
    monkeypatch.setattr(geometry, "QSettings", settings, raising=False)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _candidate(
    identifier: str = "ms-1", points: list[dict] | None = None
) -> MeasurementSetCandidate:
    """A chooser row, optionally carrying the measured extent the repository
    projects beside its display semantics (``raw_point_extents``)."""
    return MeasurementSetCandidate(
        measurement_set_id=identifier,
        raw_point_extents=point_extents(points) if points else None,
        short_label="Niskanen 2013",
        name_as_published="Cortinarius limonius",
        locator_text="p. 214",
        data_kind="range",
        raw_text="(7–)8–11(–12) × 4.5–5.5 µm",
        revision=1,
        reference_work_id="w-1",
        reference_treatment_id="t-1",
        work_title="Cortinarius in Fennoscandia",
        year=2013,
        taxon_id="7",
    )


def _observation_points(count: int = 6) -> list[dict]:
    return [
        {"length_um": 8.0 + index * 0.4, "width_um": 5.0 + index * 0.1}
        for index in range(count)
    ]


def _dialog(
    monkeypatch,
    measurement_sets: dict[str, MeasurementSet],
    *,
    candidate_points: dict[str, list[dict]] | None = None,
    **kwargs,
):
    monkeypatch.setattr(
        picker.MeasurementSetRepository,
        "get",
        lambda identifier: measurement_sets.get(identifier),
    )
    _app()
    points = candidate_points or {}
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        genus="Cortinarius",
        species="limonius",
        candidates=[
            _candidate(identifier, points.get(identifier))
            for identifier in measurement_sets
        ],
        community_results=[],
        my_observations=[],
        **kwargs,
    )


def _row_for(dialog: AddReferenceDialog, identifier: str) -> int:
    from PySide6.QtCore import Qt

    for index in range(dialog.results_list.count()):
        if dialog.results_list.item(index).data(Qt.UserRole) == identifier:
            return index
    raise AssertionError(f"no row for {identifier}")


def _set(identifier: str, **columns) -> MeasurementSet:
    return MeasurementSet(
        id=identifier,
        taxon_treatment_id="t-1",
        character="spore_size",
        data_kind=columns.pop("data_kind", "range"),
        **columns,
    )


def _details(metric: str = "length", **body) -> str:
    return json.dumps({"schema_version": 1, "metrics": {metric: dict(body)}})


# --- No selection -------------------------------------------------------------


def test_no_selection_shows_the_observation_as_chips_and_not_a_table(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=7.0, length_max=12.0)},
        observation_points=_observation_points(),
    )
    try:
        view = dialog.preview_pane.comparison_view
        assert view.view().has_source is False
        # The comparison body, never the 3x4 grid of dashes (contract N21).
        assert view.baseline_widget.isVisibleTo(dialog.preview_pane)
        assert not dialog.preview_pane.summary_table.isVisibleTo(dialog.preview_pane)
        assert "6" in view.baseline_heading_label.text()
        assert "8.0" in view._chips["length"].text()
        assert view.baseline_copy_label.text()
    finally:
        dialog.close()


def test_observation_without_measurements_says_so_rather_than_showing_zeros(monkeypatch):
    dialog = _dialog(monkeypatch, {"ms-1": _set("ms-1", length_min=7.0, length_max=12.0)})
    try:
        view = dialog.preview_pane.comparison_view
        assert not view._chips["length"].isVisibleTo(view)
        assert "no spore measurements" in view.baseline_copy_label.text()
    finally:
        dialog.close()


def test_clearing_the_selection_returns_to_the_baseline(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=7.0, length_max=12.0)},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        assert dialog.preview_pane.comparison_view.view().has_source is True
        dialog.results_list.clearSelection()
        assert dialog.preview_pane.comparison_view.view().has_source is False
        assert dialog.preview_pane.comparison_view.view().observation_point_count == 6
    finally:
        dialog.close()


# --- Selected sources ---------------------------------------------------------


def test_published_range_caption_names_the_range_and_the_absent_centre(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {
            "ms-1": _set(
                "ms-1",
                length_core_min=8.0,
                length_core_max=11.0,
                measurement_details_json=_details(core_range={"kind": "typical_range"}),
            )
        },
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        caption = dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        assert "8.0–11.0 µm" in caption
        assert "typical range" in caption
        assert "no centre reported" in caption
        # And no delta, because there is no statistic to compare.
        assert dialog.preview_pane.comparison_view.rows["length"].delta_label.text() == ""
    finally:
        dialog.close()


def test_explicit_percentile_caption_prints_its_own_bounds(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {
            "ms-1": _set(
                "ms-1",
                length_min=6.0,
                length_max=14.0,
                length_core_min=8.0,
                length_core_max=12.0,
                measurement_details_json=_details(
                    outer_range={"kind": "reported_extremes"},
                    core_range={
                        "kind": "percentile_interval",
                        "percentile_bounds": [10, 90],
                    },
                ),
            )
        },
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        caption = dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        # An explicit 10-90 interval is never relabelled 5-95 (contract N14).
        assert "10–90% range" in caption
        assert "reported extremes" in caption
    finally:
        dialog.close()


def test_q_caption_carries_no_micrometre_unit(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", q_min=1.5, q_max=1.8)},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        caption = dialog.preview_pane.comparison_view.rows["q"].source_caption_label.text()
        assert "1.5–1.8" in caption
        assert "µm" not in caption
    finally:
        dialog.close()


def test_axes_do_not_move_when_the_selection_moves(monkeypatch):
    """The user's whole reason to trust the shapes side by side (N19)."""
    dialog = _dialog(
        monkeypatch,
        {
            "ms-1": _set("ms-1", length_min=8.0, length_max=11.0),
            "ms-2": _set("ms-2", length_min=9.2, length_max=9.8),
        },
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        first = dialog.preview_pane.comparison_view.rows["length"].axis_label.text()
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-2"))
        second = dialog.preview_pane.comparison_view.rows["length"].axis_label.text()
        dialog.results_list.clearSelection()
        cleared = dialog.preview_pane.comparison_view.view().metric("length").domain

        assert first == second
        assert cleared == dialog._comparison_domains["length"]
    finally:
        dialog.close()


def test_a_known_raw_only_candidate_is_inside_the_frozen_axes(monkeypatch):
    """End to end for the extent seam: a raw-data row is not drawn off-scale.

    The candidate carries no range at all, so before ``raw_point_extents`` the
    axes were derived from the observation alone and this source -- already
    loaded in the list when the dialog opened -- rendered fully clipped.
    """
    points = [{"length": 40.0 + index, "width": 20.0 + index} for index in range(10)]
    dialog = _dialog(
        monkeypatch,
        {
            "ms-1": _set(
                "ms-1", data_kind="raw_points", raw_points_json=json.dumps(points)
            )
        },
        candidate_points={"ms-1": points},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        comparison = dialog.preview_pane.comparison_view.view().metric("length")
        assert comparison.source is not None
        assert not comparison.clipped
        assert "beyond the axis" not in (
            dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        )
    finally:
        dialog.close()


def test_a_late_arriving_centre_outside_the_axis_says_so(monkeypatch):
    """Finding's companion: what clipping must look like when it is real.

    A source not known when the axes froze can legitimately fall outside
    them. Its mean is then painted clamped to the axis edge, and the caption
    has to say that rather than letting the edge read as the value.
    """
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        from references.reference_comparison import comparison_view
        from references.reference_display import display_from_row

        latecomer = display_from_row(
            {
                "character": "spore_size",
                "data_kind": "range",
                "raw_text": None,
                "measurement_details_json": None,
                "raw_points_json": None,
                "length_mean": 40.0,
            }
        )
        dialog.preview_pane.set_comparison(
            comparison_view(
                domains=dialog._comparison_domains,
                baseline=dialog._observation_baseline,
                display=latecomer,
            )
        )
        caption = dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        assert "40.0" in caption
        assert "beyond the axis" in caption
    finally:
        dialog.close()


def test_changing_the_taxon_target_does_not_rebuild_the_axes(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        before = dict(dialog._comparison_domains)
        dialog._refresh_candidates()
        dialog._freeze_comparison_domains()
        assert dialog._comparison_domains == before
    finally:
        dialog.close()


@pytest.mark.parametrize("height", [420, 520, 700])
def test_the_track_never_overlaps_its_own_caption(monkeypatch, height):
    """The painted band and the axis caption must stay separate rectangles.

    A plain QWidget has an invalid size hint, so the first version of the
    track could not act on its Fixed height policy: in the real dialog the
    enclosing layout squeezed it and the caption underneath into overlapping
    geometry, and the 5-95 band was drawn through the words describing it.
    """
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        dialog.resize(900, height)
        dialog.show()
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        QApplication.processEvents()
        for row in dialog.preview_pane.comparison_view.rows.values():
            assert (
                row.source_caption_label.geometry().top() > row.track.geometry().bottom()
            )
            assert (
                row.observation_caption_label.geometry().top()
                > row.source_caption_label.geometry().bottom()
            )
    finally:
        dialog.close()


def _relative_luminance(color) -> float:
    def channel(value: int) -> float:
        srgb = value / 255.0
        return srgb / 12.92 if srgb <= 0.03928 else ((srgb + 0.055) / 1.055) ** 2.4

    return (
        0.2126 * channel(color.red())
        + 0.7152 * channel(color.green())
        + 0.0722 * channel(color.blue())
    )


def _contrast_ratio(foreground, background) -> float:
    first, second = _relative_luminance(foreground), _relative_luminance(background)
    lighter, darker = max(first, second), min(first, second)
    return (lighter + 0.05) / (darker + 0.05)


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_captions_stay_readable_in_both_shipped_themes(theme):
    """The captions are the pane's scientific content, not decoration.

    They are the only thing separating a min-max band from a 5-95% one and
    the only place an absent centre is stated, so they have to be legible.
    They were styled ``color: palette(mid)`` — a frame-shading role, not a
    text role — which came out at 1.4:1 in the light theme and 1.5:1 in the
    dark one, effectively invisible in both.
    """
    from PySide6.QtGui import QPalette

    from ui.reference_comparison_view import caption_color
    from ui.styles import apply_palette

    app = _app()
    try:
        apply_palette(theme)
        palette = app.palette()
        ratio = _contrast_ratio(
            caption_color(palette), palette.color(QPalette.Window)
        )
        assert ratio >= 4.5, f"{theme}: {ratio:.2f}:1"
    finally:
        apply_palette("light")


def test_a_built_row_actually_uses_the_readable_caption_colour(monkeypatch):
    """Guards the wiring, not just the helper: every caption is recoloured."""
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        from ui.reference_comparison_view import caption_color

        row = dialog.preview_pane.comparison_view.rows["length"]
        expected = caption_color(row.palette()).name()
        for label in (
            row.axis_label,
            row.source_caption_label,
            row.observation_caption_label,
        ):
            assert expected in label.styleSheet()
            assert "palette(mid)" not in label.styleSheet()
    finally:
        dialog.close()


# --- Raw spores ---------------------------------------------------------------


def test_raw_spores_tab_shows_the_actual_measurements(monkeypatch):
    points = [{"length": 9.1, "width": 5.2}, {"length": 9.4, "width": 5.0}]
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", data_kind="raw_points", raw_points_json=json.dumps(points))},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "9.1" in text and "5.2" in text
        assert "2 individual spore measurements" in text
        # Not the stored JSON blob it used to dump.
        assert "raw_points_json" not in text and "{" not in text
    finally:
        dialog.close()


def test_raw_spores_tab_of_a_published_range_explains_rather_than_invents(monkeypatch):
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "were not published for this source" in text
        assert "plotted as a range band" in text
        # No fabricated per-spore rows (contract N22).
        assert "9.5" not in text
    finally:
        dialog.close()


@pytest.mark.parametrize(
    "raw_points_json", ["{not json", '{"length": 9.0}', "[{\"id\": 1}, null]"]
)
def test_an_unreadable_points_blob_is_not_reported_as_a_publication_fact(
    monkeypatch, raw_points_json
):
    """Corrupt storage says nothing about what the author printed.

    Malformed JSON, a non-list, and a list whose members carry no dimension
    all mean Sporely cannot read what is on file. Saying "were not published
    for this source" there would put a claim about the publication in the
    author's mouth on the strength of a decoding failure.
    """
    dialog = _dialog(
        monkeypatch,
        {
            "ms-1": _set(
                "ms-1",
                data_kind="raw_points",
                raw_points_json=raw_points_json,
                length_min=8.0,
                length_max=11.0,
            )
        },
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "could not be read" in text
        assert "were not published" not in text
        # Still no invented rows, and still says what will be plotted.
        assert "plotted as a range band" in text
    finally:
        dialog.close()


def test_a_source_that_simply_stores_no_points_still_states_that_plainly(monkeypatch):
    """The other half of the distinction: absence really is absence here."""
    dialog = _dialog(
        monkeypatch,
        {"ms-1": _set("ms-1", raw_points_json="[]", length_min=8.0, length_max=11.0)},
        observation_points=_observation_points(),
    )
    try:
        dialog.results_list.setCurrentRow(_row_for(dialog, "ms-1"))
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "were not published for this source" in text
        assert "could not be read" not in text
    finally:
        dialog.close()


# --- The legacy table is still reachable for the unmigrated tabs --------------


def test_a_caller_on_the_legacy_row_api_still_gets_its_table(monkeypatch):
    """Manual entry moves onto the comparison in a later stage.

    Until then ``set_summary`` must keep working and must swap the Summary
    body, so the pane never shows a comparison and a table at once.
    """
    dialog = _dialog(monkeypatch, {"ms-1": _set("ms-1", length_min=8.0, length_max=11.0)})
    try:
        pane = dialog.preview_pane
        pane.set_summary("A dataset", "meta", [("Length", "8", "9", "11")], "note")
        assert pane.summary_table.isVisibleTo(pane)
        assert not pane.comparison_view.isVisibleTo(pane)
        pane.clear()
        assert pane.comparison_view.isVisibleTo(pane)
        assert not pane.summary_table.isVisibleTo(pane)
    finally:
        dialog.close()


# --- Community and My observations on the same contract ----------------------
#
# Stage 2: the two non-Library source tabs feed the one comparison model. What
# these pin is not that a band appears but *which claim* each tab is allowed to
# make -- a community aggregate of bare extremes must not acquire 5-95
# semantics, and a personal observation's own spores must be labelled as
# measured rather than as something published.


def _community_dialog(results: list[dict], **kwargs) -> AddReferenceDialog:
    _app()
    kwargs.setdefault("observation_points", _observation_points())
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        genus="Cortinarius",
        species="limonius",
        candidates=[],
        my_observations=[],
        community_results=results,
        **kwargs,
    )


def _my_obs_dialog(candidates, **kwargs) -> AddReferenceDialog:
    _app()
    kwargs.setdefault("observation_points", _observation_points())
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        genus="Cortinarius",
        species="limonius",
        candidates=[],
        community_results=[],
        my_observations=candidates,
        **kwargs,
    )


def _community_row(**payload) -> dict:
    """One injected community result, already carrying its detail fields.

    ``AddReferenceDialog(community_results=...)`` treats injected rows as
    complete details, so selection needs no network fetch.
    """
    row = {
        "_kind": "reference",
        "genus": "Cortinarius",
        "species": "limonius",
        "contributor_label": "mycena.no",
        "source": "mycena.no",
        "measurement_count": 0,
    }
    row.update(payload)
    return row


def _personal(points: list[dict], observation_id: int = 101):
    return picker.PersonalObservationCandidate(
        observation_id=observation_id,
        date="2024-05-01",
        author="Åse Øyen",
        location="Trøndelag",
        points=points,
    )


# --- Community: range summaries ----------------------------------------------


def test_community_bare_extremes_stay_a_published_range(monkeypatch):
    """The payload states two bounds and nothing else, so that is all it gets.

    Specifically no inner band: a community aggregate that carries no p05/p95
    must not be drawn as though it reported a 5-95 interval.
    """
    dialog = _community_dialog([_community_row(length_min=8.0, length_max=11.0)])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        view = dialog.preview_pane.comparison_view.view()
        assert view.has_source is True
        assert view.source_display.data_label.kind == "published_range"
        length = view.metric("length").source
        assert (length.outer.low, length.outer.high) == (8.0, 11.0)
        assert length.core is None
        # No p50 and no avg in the payload, so no centre is invented.
        assert length.centres == ()
        caption = dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        # An aggregate's extremes are the min and max of contributors'
        # measurements, so the caption must not call them reported.
        assert "measured min–max" in caption
        assert "reported extremes" not in caption
        assert "no centre reported" in caption
    finally:
        dialog.close()


def test_community_explicit_percentiles_keep_their_own_bounds(monkeypatch):
    """p05/p95 really are a percentile interval, so they may say so."""
    dialog = _community_dialog(
        [
            _community_row(
                length_min=7.0,
                length_max=12.0,
                length_p05=8.0,
                length_p95=11.0,
                length_p50=9.5,
            )
        ]
    )
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        view = dialog.preview_pane.comparison_view.view()
        label = view.source_display.data_label
        assert label.kind == "percentile_range"
        assert label.percentile_bounds == (5.0, 95.0)
        length = view.metric("length").source
        assert (length.core.low, length.core.high) == (8.0, 11.0)
        assert length.core.percentile_bounds == (5.0, 95.0)
        caption = dialog.preview_pane.comparison_view.rows["length"].source_caption_label.text()
        # The bounds are the payload's own, and the wording says they were
        # calculated: a community percentile is not one an author printed.
        assert "measured 5–95%" in caption
        assert "reported" not in caption
    finally:
        dialog.close()


def test_community_centre_is_attributed_as_calculated(monkeypatch):
    """A community median was computed from contributors' spores, not printed."""
    dialog = _community_dialog(
        [_community_row(length_min=8.0, length_max=11.0, length_p50=9.5)]
    )
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        length = dialog.preview_pane.comparison_view.view().metric("length").source
        centre = length.centre("median")
        assert centre.value == 9.5
        assert centre.origin == "computed"
    finally:
        dialog.close()


def test_community_summary_without_points_says_so_in_raw_spores(monkeypatch):
    dialog = _community_dialog([_community_row(length_min=8.0, length_max=11.0)])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "were not published for this source" in text
        assert "plotted as a range band" in text
    finally:
        dialog.close()


# --- Community: raw points ----------------------------------------------------


def _community_points_row() -> dict:
    return _community_row(
        _kind="observation",
        observation_id="cloud-1",
        contributor_label="sporely_community_user_7",
        observed_on="2025-05-01",
        measurement_count=3,
        length_min=8.0,
        length_max=10.0,
        measurements_json=[
            {"length_um": 8.0, "width_um": 5.0},
            {"length_um": 9.0, "width_um": 6.0},
            {"length_um": 10.0, "width_um": 7.0},
        ],
    )


def test_community_raw_points_mode_is_raw_data_derived_from_the_points(monkeypatch):
    dialog = _community_dialog([_community_points_row()])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        pane = dialog._community_pane
        pane.results_list.setCurrentRow(0)
        assert pane.raw_points_radio.isEnabled() is True
        pane.raw_points_radio.setChecked(True)
        view = dialog.preview_pane.comparison_view.view()
        assert view.source_display.data_label.kind == "raw_data"
        length = view.metric("length").source
        assert (length.outer.low, length.outer.high) == (8.0, 10.0)
        # Derived from the measurements, and labelled as measured rather than
        # as anything a contributor published.
        assert length.outer.meaning == "measured_extremes"
        assert length.outer.origin == "computed"
    finally:
        dialog.close()


def test_switching_community_mode_redraws_the_comparison(monkeypatch):
    """The two radios attach two different claims; the pane shows the checked one."""
    dialog = _community_dialog([_community_points_row()])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        pane = dialog._community_pane
        pane.results_list.setCurrentRow(0)
        assert pane.range_summary_radio.isChecked() is True
        assert (
            dialog.preview_pane.comparison_view.view().source_display.data_label.kind
            == "published_range"
        )
        pane.raw_points_radio.setChecked(True)
        assert (
            dialog.preview_pane.comparison_view.view().source_display.data_label.kind
            == "raw_data"
        )
        pane.range_summary_radio.setChecked(True)
        assert (
            dialog.preview_pane.comparison_view.view().source_display.data_label.kind
            == "published_range"
        )
    finally:
        dialog.close()


def test_community_points_render_as_rows_not_a_blob(monkeypatch):
    dialog = _community_dialog([_community_points_row()])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "3 individual spore measurements" in text
        assert "length_um" not in text
    finally:
        dialog.close()


def test_community_selection_shows_the_comparison_body_not_the_table(monkeypatch):
    dialog = _community_dialog([_community_row(length_min=8.0, length_max=11.0)])
    try:
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        pane = dialog.preview_pane
        assert pane.comparison_view.isVisibleTo(pane)
        assert not pane.summary_table.isVisibleTo(pane)
    finally:
        dialog.close()


def test_community_result_does_not_move_the_frozen_axes(monkeypatch):
    """A result fetched after the axes froze is clipped and marked, not rescaled."""
    dialog = _community_dialog([_community_row(length_min=1.0, length_max=90.0)])
    try:
        before = dialog.preview_pane.comparison_view.view().metric("length").domain
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        after = dialog.preview_pane.comparison_view.view().metric("length")
        assert (after.domain.low, after.domain.high) == (before.low, before.high)
        assert after.clipped is True
        assert after.source.outer.clipped_high is True
    finally:
        dialog.close()


def test_community_load_failure_draws_no_source_band(monkeypatch):
    """A failed fetch establishes nothing, so nothing is drawn for the source."""
    dialog = _community_dialog([])
    try:
        pane = dialog._community_pane
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        pane._on_detail_error("network unavailable", pane._detail_generation)
        view = dialog.preview_pane.comparison_view.view()
        assert view.has_source is False
        assert "Could not load dataset" in dialog.preview_pane.summary_title_label.text()
    finally:
        dialog.close()


# --- My observations ----------------------------------------------------------


def test_my_observation_is_its_own_measured_spores(monkeypatch):
    points = [{"length_um": 8.0 + i * 0.5, "width_um": 5.0} for i in range(8)]
    dialog = _my_obs_dialog([_personal(points)])
    try:
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        view = dialog.preview_pane.comparison_view.view()
        assert view.has_source is True
        assert view.source_display.data_label.kind == "raw_data"
        assert view.source_display.sample_size == 8
        length = view.metric("length").source
        assert (length.outer.low, length.outer.high) == (8.0, 11.5)
        assert length.outer.meaning == "measured_extremes"
        assert length.outer.origin == "computed"
        assert length.centre("median").origin == "computed"
        # A measured 5-95 interval is stated as measured, never as a
        # percentile anybody published.
        assert length.core.meaning == "measured_percentile_interval"
    finally:
        dialog.close()


def test_my_observation_raw_spores_shows_rows_not_json(monkeypatch):
    points = [{"length_um": 8.0, "width_um": 5.0}, {"length_um": 9.0, "width_um": 5.5}]
    dialog = _my_obs_dialog([_personal(points)])
    try:
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        text = dialog.preview_pane.raw_spores_text.toPlainText()
        assert "2 individual spore measurements" in text
        assert "length_um" not in text
        assert "{" not in text
    finally:
        dialog.close()


def test_my_observation_shows_the_comparison_body_not_the_table(monkeypatch):
    dialog = _my_obs_dialog([_personal([{"length_um": 8.0, "width_um": 5.0}])])
    try:
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        pane = dialog.preview_pane
        assert pane.comparison_view.isVisibleTo(pane)
        assert not pane.summary_table.isVisibleTo(pane)
    finally:
        dialog.close()


def test_my_observations_are_inside_the_frozen_axes(monkeypatch):
    """Unlike a community result, these are known before the axes freeze."""
    points = [{"length_um": 20.0, "width_um": 9.0}, {"length_um": 24.0, "width_um": 10.0}]
    dialog = _my_obs_dialog([_personal(points)])
    try:
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        assert dialog.preview_pane.comparison_view.view().metric("length").clipped is False
    finally:
        dialog.close()


# --- Source switching ---------------------------------------------------------


def test_switching_source_tabs_leaves_no_stale_preview(monkeypatch):
    """Three tabs share one pane, so each switch must state its own source."""
    monkeypatch.setattr(picker.MeasurementSetRepository, "get", lambda _id: None)
    _app()
    dialog = AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        genus="Cortinarius",
        species="limonius",
        candidates=[_candidate("ms-1")],
        community_results=[_community_row(length_min=8.0, length_max=11.0)],
        my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])],
        observation_points=_observation_points(),
    )
    try:
        pane = dialog.preview_pane

        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        assert pane.comparison_view.view().source_display.source_kind == "community"

        # Switching to a tab with nothing selected must drop the community
        # source rather than leave its bands on screen.
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        assert pane.comparison_view.view().has_source is False

        dialog.my_observations_list.setCurrentRow(0)
        assert pane.comparison_view.view().source_display.source_kind == "observation"

        dialog.tabs.setCurrentIndex(0)  # Library, no row selected
        assert pane.comparison_view.view().has_source is False

        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        assert pane.comparison_view.view().source_display.source_kind == "community"
    finally:
        dialog.close()


# --- The shared preview has one owner at a time ------------------------------
#
# Community search and detail both complete asynchronously, so a response can
# land after the user has moved to another source tab. These pin that a late
# response never repaints a tab it does not belong to, that the response is
# still kept, and that no half-loaded state leaves the previous source's
# method, calibration, provenance or spore rows under a new source's name.


class _FakeWorker(QObject):
    """Stand-in for ``_CloudSearchWorker`` / ``_CloudDetailWorker``.

    Created synchronously and inert on ``start()``: nothing completes until a
    test emits it, which is the only way to place a response *after* a tab
    switch deterministically. Same manual-completion contract as the fakes in
    ``tests/test_community_results_pane_requests.py``.
    """

    search_done = Signal(list, dict)
    detail_done = Signal(dict)
    error = Signal(str)
    finished = Signal()

    def __init__(self, *_args) -> None:
        super().__init__()

    def start(self) -> None:
        pass

    def wait(self, *_args, **_kwargs) -> bool:
        return True

    def deleteLater(self) -> None:  # pragma: no cover - Qt cleanup no-op
        pass


def _async_dialog(monkeypatch, **kwargs):
    """The picker with Community on its real asynchronous worker path.

    ``community_results`` is deliberately not injected: injected rows are
    treated as complete details and complete synchronously, which is exactly
    the timing these tests need to avoid.
    """
    import ui.cloud_reference_dialog as cloud

    searches: list[_FakeWorker] = []
    details: list[_FakeWorker] = []

    def make_search(_genus, _species):
        worker = _FakeWorker()
        searches.append(worker)
        return worker

    def make_detail(_row):
        worker = _FakeWorker()
        details.append(worker)
        return worker

    monkeypatch.setattr(cloud, "_CloudSearchWorker", make_search)
    monkeypatch.setattr(cloud, "_CloudDetailWorker", make_detail)
    monkeypatch.setattr(picker.MeasurementSetRepository, "get", lambda _id: None)
    _app()
    kwargs.setdefault("observation_points", _observation_points())
    dialog = AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        genus="Cortinarius",
        species="limonius",
        candidates=[],
        **kwargs,
    )
    return dialog, searches, details


def _search_rows() -> list[dict]:
    return [
        {"_kind": "reference", "observation_id": "cloud-1", "contributor_label": "one"},
        {"_kind": "reference", "observation_id": "cloud-2", "contributor_label": "two"},
    ]


def _loaded_detail(**overrides) -> dict:
    detail = _community_row(
        length_min=8.0,
        length_max=11.0,
        mount_medium="KOH 5%",
        stain="Congo red",
        measurement_count=12,
    )
    detail.update(overrides)
    return detail


def _source_kind(dialog):
    display = dialog.preview_pane.comparison_view.view().source_display
    return None if display is None else display.source_kind


def test_late_community_detail_does_not_repaint_another_tab(monkeypatch):
    """The reported race: a detail arriving after the user moved on.

    The response is not stale -- its generation still matches -- it simply is
    no longer the tab on screen, which a generation check cannot express.
    """
    dialog, searches, details = _async_dialog(
        monkeypatch, my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])]
    )
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        assert len(details) == 1  # the detail request is genuinely outstanding

        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        assert _source_kind(dialog) == "observation"

        details[0].detail_done.emit(_loaded_detail())

        assert _source_kind(dialog) == "observation"
        assert "My observation" in dialog.preview_pane.summary_title_label.text()
        # The response was kept, not discarded: the footer can still attach it.
        assert dialog._community_pane.has_selection() is True
    finally:
        dialog.close()


def test_returning_to_community_restores_the_detail_that_arrived_while_away(monkeypatch):
    dialog, searches, details = _async_dialog(
        monkeypatch, my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])]
    )
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        details[0].detail_done.emit(_loaded_detail())

        dialog.tabs.setCurrentIndex(dialog._community_tab_index)

        assert _source_kind(dialog) == "community"
        length = dialog.preview_pane.comparison_view.view().metric("length").source
        assert (length.outer.low, length.outer.high) == (8.0, 11.0)
        assert dialog.preview_pane._method_labels["mount"].text() == "KOH 5%"
    finally:
        dialog.close()


def test_late_community_detail_error_does_not_repaint_another_tab(monkeypatch):
    dialog, searches, details = _async_dialog(
        monkeypatch, my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])]
    )
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)

        details[0].error.emit("network unavailable")

        assert _source_kind(dialog) == "observation"
        assert "Could not load dataset" not in dialog.preview_pane.summary_title_label.text()

        # And the failure is not forgotten: it is what Community shows on return.
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        assert "Could not load dataset" in dialog.preview_pane.summary_title_label.text()
        assert "network unavailable" in dialog.preview_pane.summary_meta_label.text()
        assert dialog.preview_pane.comparison_view.view().has_source is False
    finally:
        dialog.close()


def test_late_community_search_does_not_clear_another_tabs_preview(monkeypatch):
    """A debounced search reloads the list, which clears its selection.

    That clear used to reach the shared pane, so a search settling just after
    a tab switch wiped the source the user was looking at.
    """
    dialog, searches, details = _async_dialog(
        monkeypatch, my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])]
    )
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        details[0].detail_done.emit(_loaded_detail())

        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        assert _source_kind(dialog) == "observation"

        # The keystroke was typed on the Community tab; its debounce fires now.
        dialog._community_pane.search_input.setText("Hebeloma")
        dialog._community_pane._apply_search_text()

        assert _source_kind(dialog) == "observation"
        assert "My observation" in dialog.preview_pane.summary_title_label.text()
    finally:
        dialog.close()


def test_taxon_target_change_does_not_clear_another_tabs_preview(monkeypatch):
    """The same clear, reached through the picker's taxon selector."""
    dialog, searches, details = _async_dialog(
        monkeypatch, my_observations=[_personal([{"length_um": 8.0, "width_um": 5.0}])]
    )
    try:
        dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
        dialog.my_observations_list.setCurrentRow(0)
        assert _source_kind(dialog) == "observation"

        dialog._community_pane.set_taxon("Hebeloma", "mesophaeum")

        assert _source_kind(dialog) == "observation"
    finally:
        dialog.close()


def test_pending_community_selection_drops_the_previous_sources_details(monkeypatch):
    """Loading must not leave one dataset's method under another's name."""
    dialog, searches, details = _async_dialog(monkeypatch, my_observations=[])
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        details[0].detail_done.emit(_loaded_detail())
        pane = dialog.preview_pane
        assert pane._method_labels["mount"].text() == "KOH 5%"
        assert "12" in pane.provenance_summary_label.text()

        dialog._community_pane.results_list.setCurrentRow(1)

        assert "Loading review details" in pane.summary_title_label.text()
        assert pane._method_labels["mount"].text() == "—"
        assert pane._method_labels["stain"].text() == "—"
        assert "Select a result" in pane.calibration_text.toPlainText()
        assert "Select a result" in pane.provenance_text.toPlainText()
        assert "Select a result" in pane.raw_spores_text.toPlainText()
        assert pane.provenance_summary_label.text() == ""
        assert pane.comparison_view.view().has_source is False
    finally:
        dialog.close()


def test_failed_community_selection_drops_the_previous_sources_details(monkeypatch):
    """The populated -> pending -> error path, end to end."""
    dialog, searches, details = _async_dialog(monkeypatch, my_observations=[])
    try:
        searches[0].search_done.emit(_search_rows(), {})
        dialog.tabs.setCurrentIndex(dialog._community_tab_index)
        dialog._community_pane.results_list.setCurrentRow(0)
        details[0].detail_done.emit(_loaded_detail())
        pane = dialog.preview_pane
        assert pane._method_labels["mount"].text() == "KOH 5%"

        dialog._community_pane.results_list.setCurrentRow(1)
        details[1].error.emit("network unavailable")

        assert "Could not load dataset" in pane.summary_title_label.text()
        assert pane._method_labels["mount"].text() == "—"
        assert "Select a result" in pane.calibration_text.toPlainText()
        assert "Select a result" in pane.provenance_text.toPlainText()
        assert pane.provenance_summary_label.text() == ""
        assert "network unavailable" in pane.raw_spores_text.toPlainText()
        assert pane.comparison_view.view().has_source is False
        # A failed fetch is not a selection the footer may attach.
        assert dialog._community_pane.has_selection() is False
    finally:
        dialog.close()
