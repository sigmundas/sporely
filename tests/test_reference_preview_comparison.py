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
    """Community, My observations and manual entry move in later stages.

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
