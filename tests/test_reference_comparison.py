"""The comparison model behind the Add-reference Summary tab.

``references/reference_comparison.py`` turns "what did this source report?"
(:mod:`references.reference_display`) into "what is drawn, on which axis, and
what may be said about it". It imports no Qt, so every rule below is checked
without a QApplication or a widget.

The cases that carry the weight are the refusals. A published range must not
grow a centre mark; a mean must not be compared against a median; and the axis
must not move when the selected source moves, because an axis that rescales
per selection makes two sources impossible to compare by eye — which is the
only reason this pane exists.
"""
from __future__ import annotations

import json

from references.reference_comparison import (
    MEASURED_EXTREMES,
    MEASURED_PERCENTILE,
    MetricDomain,
    build_domains,
    comparison_view,
    normalize_points,
    observation_baseline_from_points,
    point_extents,
    raw_point_rows,
    source_series,
)
from references.reference_display import display_from_row


def _row(**overrides) -> dict:
    row = {
        "character": "spore_size",
        "data_kind": "range",
        "raw_text": None,
        "measurement_details_json": None,
        "raw_points_json": None,
    }
    row.update(overrides)
    return row


def _details(metric: str = "length", **body) -> str:
    return json.dumps({"schema_version": 1, "metrics": {metric: dict(body)}})


def _observation_points(count: int = 5) -> list[dict]:
    return [
        {"length_um": 8.0 + index * 0.5, "width_um": 5.0 + index * 0.1}
        for index in range(count)
    ]


def _domains(**kwargs):
    return build_domains(**kwargs)


# --- No selection: the observation baseline ----------------------------------


def test_baseline_summarizes_the_observation_and_names_each_statistic():
    baseline = observation_baseline_from_points(_observation_points(10))
    length = baseline.series("length")

    assert baseline.point_count == 10
    assert baseline.has_measurements
    # min-max of the measurements, explicitly labelled as that and not as a
    # range anybody published.
    assert (length.outer.low, length.outer.high) == (8.0, 12.5)
    assert length.outer.meaning == MEASURED_EXTREMES
    assert length.outer.origin == "computed"
    # The inner band is the 5-95 percentile interval of the same points, and
    # carries its bounds so it renders with an explicit percentage.
    assert length.core.meaning == MEASURED_PERCENTILE
    assert length.core.percentile_bounds == (5.0, 95.0)
    # One centre, and it is the median -- never a midpoint of the extent.
    assert [centre.statistic for centre in length.centres] == ["median"]
    assert length.centre("median").origin == "computed"
    assert length.centre("mean") is None


def test_baseline_of_an_observation_with_no_measurements_is_empty_not_zero():
    baseline = observation_baseline_from_points([])

    assert baseline.point_count == 0
    assert not baseline.has_measurements
    assert baseline.series("length") is None


def test_no_selection_view_has_the_observation_and_no_source():
    baseline = observation_baseline_from_points(_observation_points())
    view = comparison_view(domains=_domains(baseline=baseline), baseline=baseline)

    assert view.has_source is False
    assert view.observation_point_count == 5
    assert view.metric("length").source is None
    assert view.metric("length").observation is not None
    # Every metric is present, so no caller has to handle a missing row.
    assert [m.metric for m in view.metrics] == ["length", "width", "q"]


def test_q_is_derived_per_point_and_never_from_the_aggregate_extremes():
    """Q is length/width *of the same spore*, not max length over min width."""
    points = [{"length_um": 10.0, "width_um": 5.0}, {"length_um": 8.0, "width_um": 4.0}]
    baseline = observation_baseline_from_points(points)

    q = baseline.series("q")
    assert (round(q.outer.low, 6), round(q.outer.high, 6)) == (2.0, 2.0)


def test_a_point_measured_in_one_dimension_still_counts_for_that_dimension():
    baseline = observation_baseline_from_points(
        [{"length_um": 9.0}, {"length_um": 11.0, "width_um": 5.0}]
    )

    assert baseline.point_count == 2
    assert (baseline.series("length").outer.low, baseline.series("length").outer.high) == (
        9.0,
        11.0,
    )
    # Nothing is invented for the width the first point never had.
    assert baseline.series("width").point_count == 1


def test_points_with_no_usable_dimension_are_dropped_rather_than_counted():
    assert normalize_points([{}, None, "9.0", [], {"id": 3}]) == ()


# --- A source with actual raw data -------------------------------------------


def test_raw_data_source_with_no_stored_ranges_is_summarized_from_its_points():
    points = [{"length": 9.0 + i * 0.4, "width": 5.0} for i in range(8)]
    display = display_from_row(
        _row(data_kind="raw_points", raw_points_json=json.dumps(points))
    )
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    series = source_series(display, "length", domain=domain, points=points)

    assert series.outer.meaning == MEASURED_EXTREMES
    assert series.outer.origin == "computed"
    assert series.point_count == 8
    assert series.centre("median") is not None


def test_stored_statistics_beat_the_points_they_sit_beside():
    """A published mean is not replaced by one calculated from the points.

    A library row can hold both a figure its author printed and the individual
    measurements somebody transcribed. Recomputing over the points would quietly
    overwrite the author's number with Sporely's.
    """
    points = [{"length": 9.0}, {"length": 15.0}]
    display = display_from_row(
        _row(
            length_min=8.0,
            length_max=12.0,
            length_mean=9.9,
            raw_points_json=json.dumps(points),
            measurement_details_json=_details(outer_range={"kind": "reported_extremes"}),
        )
    )
    domain = MetricDomain(metric="length", low=5.0, high=16.0)

    series = source_series(display, "length", domain=domain, points=points)

    assert (series.outer.low, series.outer.high) == (8.0, 12.0)
    assert series.outer.meaning == "reported_extremes"
    assert series.centre("mean").value == 9.9
    assert series.centre("median") is None


# --- Published ranges ---------------------------------------------------------


def test_published_range_without_a_centre_draws_no_centre():
    display = display_from_row(
        _row(
            length_core_min=7.0,
            length_core_max=12.0,
            measurement_details_json=_details(core_range={"kind": "typical_range"}),
        )
    )
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    series = source_series(display, "length", domain=domain)

    assert (series.core.low, series.core.high) == (7.0, 12.0)
    assert series.core.meaning == "typical_range"
    assert series.core.percentile_bounds is None
    # The midpoint 9.5 is not a statistic anyone reported, so it is absent.
    assert series.centres == ()


def test_explicit_five_to_ninety_five_stays_separate_from_the_outer_extent():
    display = display_from_row(
        _row(
            length_min=6.0,
            length_max=14.0,
            length_core_min=8.0,
            length_core_max=12.0,
            measurement_details_json=_details(
                outer_range={"kind": "reported_extremes"},
                core_range={"kind": "percentile_interval", "percentile_bounds": [5, 95]},
            ),
        )
    )
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    series = source_series(display, "length", domain=domain)

    assert (series.outer.low, series.outer.high) == (6.0, 14.0)
    assert series.outer.meaning == "reported_extremes"
    assert (series.core.low, series.core.high) == (8.0, 12.0)
    assert series.core.meaning == "percentile_interval"
    assert series.core.percentile_bounds == (5.0, 95.0)


def test_an_inner_range_without_a_percentile_descriptor_is_not_a_percentile():
    display = display_from_row(_row(length_core_min=8.0, length_core_max=12.0))
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    series = source_series(display, "length", domain=domain)

    assert series.core.percentile_bounds is None
    assert series.core.meaning is None


def test_a_metric_the_source_says_nothing_about_produces_no_series():
    display = display_from_row(_row(length_min=6.0, length_max=14.0))
    domain = MetricDomain(metric="q", low=1.0, high=2.5)

    assert source_series(display, "q", domain=domain) is None


# --- Centre statistics --------------------------------------------------------


def test_an_explicit_mean_is_carried_as_a_mean():
    display = display_from_row(_row(length_min=6.0, length_max=14.0, length_mean=9.8))
    series = source_series(
        display, "length", domain=MetricDomain(metric="length", low=5.0, high=15.0)
    )

    assert series.centre("mean").value == 9.8
    assert series.centre("mean").origin == "reported"
    assert series.centre("median") is None


def test_an_explicit_median_is_carried_as_a_median():
    display = display_from_row(
        _row(
            length_min=6.0,
            length_max=14.0,
            measurement_details_json=_details(median={"value": 9.4}),
        )
    )
    series = source_series(
        display, "length", domain=MetricDomain(metric="length", low=5.0, high=15.0)
    )

    assert series.centre("median").value == 9.4
    assert series.centre("mean") is None


def test_a_mean_reported_as_an_interval_keeps_both_bounds():
    display = display_from_row(
        _row(
            length_min=6.0,
            length_max=14.0,
            measurement_details_json=_details(
                mean_interval={"lower": 9.1, "upper": 9.6, "kind": "reported_range"}
            ),
        )
    )
    series = source_series(
        display, "length", domain=MetricDomain(metric="length", low=5.0, high=15.0)
    )

    centre = series.centre("mean")
    assert centre.interval == (9.1, 9.6)
    # The midpoint 9.35 is nobody's mean and must not stand in for one.
    assert centre.value is None


# --- Deltas -------------------------------------------------------------------


def test_delta_is_shown_for_two_medians():
    baseline = observation_baseline_from_points(
        [{"length_um": 10.0, "width_um": 5.0}, {"length_um": 10.0, "width_um": 5.0}]
    )
    display = display_from_row(
        _row(
            length_min=6.0,
            length_max=14.0,
            measurement_details_json=_details(median={"value": 9.5}),
        )
    )
    view = comparison_view(
        domains=_domains(baseline=baseline, displays=[display]),
        baseline=baseline,
        display=display,
    )

    delta = view.metric("length").delta
    assert delta.statistic == "median"
    assert round(delta.difference, 6) == 0.5


def test_no_delta_when_the_source_reports_a_mean_against_a_calculated_median():
    """A mean and a median are not the same claim (contract N16)."""
    baseline = observation_baseline_from_points(_observation_points())
    display = display_from_row(_row(length_min=6.0, length_max=14.0, length_mean=9.8))
    view = comparison_view(
        domains=_domains(baseline=baseline, displays=[display]),
        baseline=baseline,
        display=display,
    )

    assert view.metric("length").source.centre("mean") is not None
    assert view.metric("length").observation.centre("median") is not None
    assert view.metric("length").delta is None


def test_no_delta_against_a_source_that_reports_only_a_range():
    baseline = observation_baseline_from_points(_observation_points())
    display = display_from_row(_row(length_core_min=7.0, length_core_max=12.0))
    view = comparison_view(
        domains=_domains(baseline=baseline, displays=[display]),
        baseline=baseline,
        display=display,
    )

    assert view.metric("length").delta is None


def test_no_delta_between_a_scalar_and_an_interval_statistic():
    baseline = observation_baseline_from_points(_observation_points())
    display = display_from_row(
        _row(
            measurement_details_json=_details(
                median={"lower": 9.0, "upper": 9.5, "kind": "reported_range"}
            ),
        )
    )
    view = comparison_view(
        domains=_domains(baseline=baseline, displays=[display]),
        baseline=baseline,
        display=display,
    )

    assert view.metric("length").delta is None


# --- The frozen axis ----------------------------------------------------------


def test_domains_cover_the_observation_and_every_known_candidate():
    baseline = observation_baseline_from_points(_observation_points())
    wide = display_from_row(_row(length_min=4.0, length_max=20.0))

    domains = build_domains(baseline=baseline, displays=[wide])

    assert domains["length"].low <= 4.0
    assert domains["length"].high >= 20.0


def test_domains_do_not_change_when_the_selected_source_changes():
    baseline = observation_baseline_from_points(_observation_points())
    first = display_from_row(_row(length_min=8.0, length_max=11.0))
    second = display_from_row(_row(length_min=9.0, length_max=10.0))
    domains = build_domains(baseline=baseline, displays=[first, second])

    view_first = comparison_view(domains=domains, baseline=baseline, display=first)
    view_second = comparison_view(domains=domains, baseline=baseline, display=second)

    assert view_first.metric("length").domain == view_second.metric("length").domain
    assert view_first.metric("q").domain == view_second.metric("q").domain


def test_a_raw_only_candidate_contributes_its_measured_extent_to_the_axis():
    """A raw-data row states no range, so its projection carries no numbers.

    ``display_from_row`` keeps a count of the stored points and nothing about
    where they sit, so a library of spores measured at 40-50 µm used to
    contribute nothing to ``build_domains`` and was then drawn entirely
    clipped the moment it was selected. ``point_extents`` is what carries that
    span to the axis.
    """
    points = [{"length": 40.0 + index, "width": 20.0 + index} for index in range(10)]
    display = display_from_row(
        _row(data_kind="raw_points", raw_points_json=json.dumps(points))
    )
    assert not display.has_any_range  # the projection itself states no range

    without = build_domains(displays=[display])
    with_extents = build_domains(displays=[display], extents=[point_extents(points)])

    assert without["length"].clips_above(49.0)
    assert not with_extents["length"].clips_above(49.0)
    assert not with_extents["length"].clips_below(40.0)


def test_point_extents_report_only_the_metrics_that_were_measured():
    extents = point_extents([{"length": 9.0}, {"length": 11.0}])

    assert extents["length"] == (9.0, 11.0)
    assert "width" not in extents and "q" not in extents
    assert point_extents([]) == {}


def test_a_clipped_centre_is_flagged_on_the_side_it_fell_off():
    """A mean of 40 on a 5-15 axis is painted at the edge; it must say so."""
    display = display_from_row(_row(length_mean=40.0))
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    centre = source_series(display, "length", domain=domain).centre("mean")

    assert centre.clipped_high and not centre.clipped_low
    assert centre.clipped


def test_a_centre_interval_reaching_past_the_axis_is_flagged():
    display = display_from_row(
        _row(
            measurement_details_json=_details(
                mean_interval={"lower": 2.0, "upper": 40.0, "kind": "reported_range"}
            )
        )
    )
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    centre = source_series(display, "length", domain=domain).centre("mean")

    assert centre.clipped_low and centre.clipped_high


def test_a_centre_inside_the_axis_is_not_flagged():
    display = display_from_row(_row(length_mean=9.8))
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    assert not source_series(display, "length", domain=domain).centre("mean").clipped


def test_content_outside_the_frozen_axis_is_marked_rather_than_rescaled():
    """A Community result arriving later must not move the axis (N19)."""
    baseline = observation_baseline_from_points(_observation_points())
    domains = build_domains(baseline=baseline)
    latecomer = display_from_row(_row(length_min=0.5, length_max=40.0))

    view = comparison_view(domains=domains, baseline=baseline, display=latecomer)

    assert view.metric("length").domain == domains["length"]
    band = view.metric("length").source.outer
    assert band.clipped_low and band.clipped_high
    assert view.metric("length").clipped


def test_a_domain_never_collapses_onto_a_single_value():
    display = display_from_row(_row(length_min=9.9, length_max=10.0))
    domains = build_domains(displays=[display])

    assert domains["length"].span >= 4.0
    assert domains["length"].low >= 0.0


def test_every_metric_gets_an_axis_even_with_nothing_known():
    domains = build_domains()

    assert set(domains) == {"length", "width", "q"}
    assert all(domain.span > 0 for domain in domains.values())


def test_position_clamps_to_the_axis_and_reports_that_it_did():
    domain = MetricDomain(metric="length", low=5.0, high=15.0)

    assert domain.position(10.0) == 0.5
    assert domain.position(100.0) == 1.0
    assert domain.position(-3.0) == 0.0
    assert domain.clips_above(100.0)
    assert domain.clips_below(-3.0)


# --- Raw spore rows -----------------------------------------------------------


def test_raw_point_rows_format_the_measurements_that_exist():
    rows = raw_point_rows([{"length": 9.25, "width": 5.0}, {"length": 8.0}])

    assert rows[0] == ("1", "9.2", "5.0", "1.9")
    # No width was measured for the second spore, so no width or Q is printed.
    assert rows[1] == ("2", "8.0", "—", "—")


def test_raw_point_rows_of_a_source_with_no_points_are_empty():
    """The honest empty state's precondition: a range yields no rows at all."""
    assert raw_point_rows(None) == ()
    assert raw_point_rows([]) == ()
    assert raw_point_rows([{"id": 1}, None]) == ()
