"""Range bars beside each reference in the Analysis reference drawer, on a real ``MainWindow``.

Each library row draws the set's length and width against the observation's
own spores on one axis shared by the whole list (owner, 2026-11-22). The
window runs against an isolated profile under ``tmp_path``; the library is
seeded like the reference renderer scenarios. No network, no live data.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF, Qt

from database.models import ImageDB, MeasurementDB, ObservationDB
from tools.review_ui.scenarios.analysis_tab import EXTRA_LIBRARY
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile
from ui.library_source_row import LibrarySourceRow
from ui.reference_range_bars import (
    LANE_HEIGHT,
    OBSERVATION_COLOR,
    SOURCE_COLOR,
    TRACK_HEIGHT,
    ReferenceRangeBars,
)


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


def _observation(profile, key="default", *, spores=True):
    observation_id = create_observation(profile, key, genus="Cortinarius", species="limonius")
    ObservationDB.update_observation(observation_id, sporely_taxon_id=7)
    if not spores:
        return observation_id
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg(f"{key}.JPG", seed=5)), "microscope", scale=0.1, objective_name="100X",
    )
    # The stored "Spores", the older "spores", and a basidium that must not count.
    for index, (length, width, kind) in enumerate((
        (9.0, 5.0, "spores"), (9.6, 5.2, "spores"), (10.2, 5.4, "Spores"), (10.8, 5.6, "Spores"),
        (30.0, 9.0, "Basidia"),
    )):
        x = 40 + index * 40
        MeasurementDB.add_measurement(
            image_id, length, width, measurement_type=kind,
            points=[QPointF(x, 50), QPointF(x + 28, 50), QPointF(x + 14, 42), QPointF(x + 14, 58)],
        )
    return observation_id


@pytest.fixture
def library(profile):
    from tools.review_ui.scenarios.analysis_tab import _seed_extra_library
    from tools.review_ui.scenarios.references import _seed_library

    _work, sets = _seed_library()
    _seed_extra_library()
    return sets


def _window(qtbot, observation_id):
    from ui.main_window import MainWindow

    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Cortinarius limonius", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    qtbot.waitUntil(lambda: window.active_observation_id == observation_id, timeout=5000)
    qtbot.wait(300)
    return window


def _open(qtbot, window):
    qtbot.mouseClick(window.ref_add_reference_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: window.reference_drawer.is_open() and window.reference_drawer.isVisible(), timeout=2000)
    return window.reference_picker_page


def _rows(page) -> list[LibrarySourceRow]:
    rows = []
    for index in range(page.results_list.count()):
        widget = page.results_list.itemWidget(page.results_list.item(index))
        if isinstance(widget, LibrarySourceRow):
            rows.append(widget)
    return rows


def _row_by_taxon(page, prefix: str) -> LibrarySourceRow:
    return next(row for row in _rows(page) if row.taxon_label.full_text().startswith(prefix))


def _domains(row: LibrarySourceRow) -> tuple:
    view = row.range_bars.view()
    return tuple((m.metric, m.domain.low, m.domain.high) for m in view.metrics)


def test_every_row_draws_its_set_against_the_observations_spores_on_one_axis(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    assert window.reference_drawer.width() == 600
    page.only_this_taxon_checkbox.setChecked(False)
    rows = _rows(page)
    assert len(rows) == 3 + 1 + len(EXTRA_LIBRARY)  # _seed_library's four sets and the extra rows
    for row in rows:
        assert isinstance(row.range_bars, ReferenceRangeBars) and row.range_bars.isVisible()
        # Right of the text, left of the arrow.
        assert row.taxon_label.geometry().right() < row.range_bars.geometry().left()
        assert row.range_bars.geometry().right() < row.expand_button.geometry().left()
        view = row.range_bars.view()
        # The four spores, whatever their stored spelling; not the basidium.
        assert view.observation_point_count == 4
        assert view.metric("length").observation.outer.low == pytest.approx(9.0)
        assert view.metric("length").observation.outer.high == pytest.approx(10.8)
    assert len({_domains(row) for row in rows}) == 1  # one shared axis

    # A stated range: the typical range and the extremes.
    ranged = page.row_for(library[0].id).range_bars.view().metric("length").source
    assert (ranged.core.low, ranged.core.high) == (8.5, 10.8)
    assert (ranged.outer.low, ranged.outer.high) == (8.1, 11.4)
    # A mean only: a centre tick, no band.
    summary = page.row_for(library[2].id).range_bars.view().metric("length").source
    assert summary.outer is None and summary.core is None and summary.centres[0].value == 9.6
    # Raw measurements only: nothing is drawn for the set (its extents size the axis only).
    assert page.row_for(library[1].id).range_bars.view().metric("length").source is None
    # No width stated: the width track has no reference band.
    no_width = _row_by_taxon(page, "Laccaria laccata").range_bars.view()
    assert no_width.metric("length").source is not None and no_width.metric("width").source is None
    # Far outside the axis (built from this taxon, its genus and the spores): clamped and marked.
    big = _row_by_taxon(page, "Morchella esculenta").range_bars.view().metric("length").source
    assert big.core.clipped_high


def test_the_axis_stays_put_while_searching_and_filtering_and_follows_compare_with(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    axis = _domains(_rows(page)[0])
    page.only_this_taxon_checkbox.setChecked(False)
    assert {_domains(row) for row in _rows(page)} == {axis}
    qtbot.keyClicks(page.search_input, "Funga")
    assert _rows(page) and {_domains(row) for row in _rows(page)} == {axis}
    page.search_input.clear()

    # Another taxon to compare with: its sets size the axis instead.
    page._set_target(taxon_id="206", genus="Macrolepiota", species="procera", rank="species")
    moved = {_domains(row) for row in _rows(page)}
    assert len(moved) == 1 and moved != {axis}
    page.reset_target()
    assert {_domains(row) for row in _rows(page)} == {axis}


def test_without_measurements_only_the_reference_is_drawn(qtbot, profile, library):
    observation_id = _observation(profile, spores=False)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    row = page.row_for(library[0].id)
    view = row.range_bars.view()
    assert view.observation_point_count == 0
    assert all(metric.observation is None for metric in view.metrics)
    assert view.metric("length").source is not None
    assert "no spore measurements" in row.range_bars.toolTip()


def _lane_colours(bars: ReferenceRangeBars, lane_y: float) -> set[str]:
    image = bars.grab().toImage()
    y = int(lane_y * image.height() / bars.height())
    seen = set()
    for x in range(image.width()):
        pixel = image.pixelColor(x, y)
        if pixel.blue() > pixel.green() + 40 and pixel.blue() > pixel.red() + 40:
            seen.add("blue")
        if pixel.green() > pixel.red() + 40 and pixel.green() > pixel.blue() + 20:
            seen.add("green")
    return seen


def test_the_reference_is_painted_above_the_axis_and_the_spores_below(qtbot, profile, library):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open(qtbot, window)
    bars = page.row_for(library[0].id).range_bars
    top = (bars.height() - bars.sizeHint().height()) / 2.0
    axis_y = top + TRACK_HEIGHT / 2.0
    assert SOURCE_COLOR.name() != OBSERVATION_COLOR.name()
    assert _lane_colours(bars, axis_y - 1.0 - LANE_HEIGHT / 2.0) == {"blue"}
    assert _lane_colours(bars, axis_y + 1.0 + LANE_HEIGHT / 2.0) == {"green"}
