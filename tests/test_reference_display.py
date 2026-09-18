"""Stage 1: the one reading of what a reference source contains.

``references/reference_display.py`` decides which compact badge a chooser row
deserves and which statistics a comparison row may show. It imports no Qt, so
every case here runs without a QApplication.

The cases that carry the scientific weight are the ones that must *not* happen:
a ``data_kind`` of ``raw_points`` with no stored points must not earn the
``Raw data`` badge, an inner range with no percentile descriptor must not be
read as a 5–95% interval, a range's midpoint must never appear as a mean or a
median, and details written by a newer Sporely must not be guessed at.
"""
from __future__ import annotations

import json

import pytest

from references.measurement_content import (
    IntervalStatistic,
    MeasurementContent,
    MeasurementDetails,
    MetricDetails,
    RangeDescriptor,
    ScalarStatistic,
)
from references.reference_display import (
    COMMUNITY_PERCENTILE_BOUNDS,
    DataLabel,
    community_sample_size,
    display_from_community_summary,
    display_from_content,
    display_from_points,
    display_from_row,
    raw_point_count,
)


def _row(**overrides) -> dict:
    """A stored measurement-set row with only the keys a test names."""
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
    return json.dumps(
        {
            "schema_version": 1,
            "metrics": {metric: {k: v for k, v in body.items()}},
        }
    )


# --- Raw data requires actual points -----------------------------------------


def test_stored_points_earn_the_raw_data_label():
    points = [{"length": 9.1, "width": 5.2}, {"length": 9.4, "width": 5.0}]
    display = display_from_row(_row(data_kind="raw_points", raw_points_json=json.dumps(points)))

    assert display.has_raw_points
    assert display.raw_point_count == 2
    assert display.data_label == DataLabel(kind="raw_data")


def test_raw_points_data_kind_without_points_is_not_raw_data():
    display = display_from_row(
        _row(data_kind="raw_points", length_core_min=8.0, length_core_max=11.0)
    )

    assert not display.has_raw_points
    assert display.stored_data_kind == "raw_points"
    assert display.data_label == DataLabel(kind="published_range")


@pytest.mark.parametrize("stored", [None, "", "[]", "null", "not json", '{"a": 1}'])
def test_raw_point_count_is_zero_for_anything_that_is_not_a_points_list(stored):
    assert raw_point_count(stored) == 0


@pytest.mark.parametrize(
    "members",
    [
        [None, {}],
        [None],
        [{}],
        ["9.0 x 5.0"],
        [[9.0, 5.0]],
        [{"note": "wide"}],
        [{"length": "nine"}],
        [{"q": 1.8}],
        [{"length": True}],
    ],
)
def test_list_members_that_are_not_measurements_earn_nothing(members):
    display = display_from_row(
        _row(data_kind="raw_points", raw_points_json=json.dumps(members))
    )

    assert display.raw_point_count == 0
    assert not display.has_raw_points
    assert display.data_label == DataLabel(kind="none")


def test_only_the_real_measurements_in_a_mixed_list_are_counted():
    members = [None, {"length": 9.0, "width": 5.0}, {}, 9.4, {"note": "x"}]
    display = display_from_row(
        _row(data_kind="raw_points", raw_points_json=json.dumps(members))
    )

    assert display.raw_point_count == 2
    assert display.data_label == DataLabel(kind="raw_data")


def test_observation_points_without_a_dimension_do_not_inflate_the_count():
    display = display_from_points(
        [
            {"id": 1, "length_um": 9.0, "width_um": 5.0},
            {"id": 2, "length_um": None, "width_um": None},
            {},
            None,
        ]
    )

    assert display.raw_point_count == 1
    assert display.sample_size == 1


# --- Published range ----------------------------------------------------------


def test_ordinary_published_range_without_descriptor():
    display = display_from_row(_row(length_core_min=8.0, length_core_max=11.0))
    length = display.metric("length")

    assert display.data_label == DataLabel(kind="published_range")
    assert length.core_range == (8.0, 11.0)
    assert length.core_kind is None
    assert not display.has_reported_descriptor


def test_typical_core_range_keeps_its_descriptor_but_shows_published_range():
    display = display_from_row(
        _row(
            length_core_min=8.0,
            length_core_max=11.0,
            measurement_details_json=_details(core_range={"kind": "typical_range"}),
        )
    )

    assert display.data_label == DataLabel(kind="published_range")
    assert display.metric("length").core_kind == "typical_range"
    assert not display.metric("length").is_percentile_core
    assert display.has_reported_descriptor


def test_half_open_pair_is_not_closed_into_a_range():
    display = display_from_row(_row(length_core_min=8.0))

    assert display.metric("length").core_range is None
    assert display.data_label == DataLabel(kind="none")


# --- Percentile intervals -----------------------------------------------------


def test_outer_extremes_with_explicit_five_to_ninetyfive_core():
    display = display_from_row(
        _row(
            length_min=7.0,
            length_max=13.0,
            length_core_min=8.0,
            length_core_max=11.0,
            measurement_details_json=_details(
                outer_range={"kind": "reported_extremes"},
                core_range={"kind": "percentile_interval", "percentile_bounds": [5, 95]},
            ),
        )
    )
    length = display.metric("length")

    assert display.data_label == DataLabel(kind="percentile_range", percentile_bounds=(5, 95))
    assert length.outer_range == (7.0, 13.0)
    assert length.outer_kind == "reported_extremes"
    assert length.core_range == (8.0, 11.0)
    assert length.is_percentile_core


def test_non_five_ninetyfive_percentile_bounds_are_reported_exactly():
    display = display_from_row(
        _row(
            width_core_min=4.5,
            width_core_max=6.0,
            measurement_details_json=_details(
                "width",
                core_range={"kind": "percentile_interval", "percentile_bounds": [10, 90]},
            ),
        )
    )

    assert display.data_label == DataLabel(kind="percentile_range", percentile_bounds=(10, 90))


def test_percentile_kind_without_bounds_does_not_claim_a_percentile_label():
    display = display_from_row(
        _row(
            length_core_min=8.0,
            length_core_max=11.0,
            measurement_details_json=_details(core_range={"kind": "percentile_interval"}),
        )
    )

    assert display.metric("length").percentile_bounds is None
    assert not display.metric("length").is_percentile_core
    assert display.data_label == DataLabel(kind="published_range")


@pytest.mark.parametrize(
    "columns",
    [
        {},
        {"length_core_min": 8.0},
        {"length_core_max": 11.0},
    ],
)
def test_a_percentile_descriptor_without_its_interval_claims_nothing(columns):
    """The descriptor describes a column pair. With no pair there is no range,
    and a badge built from the descriptor alone would promise data the source
    does not contain."""
    display = display_from_row(
        _row(
            measurement_details_json=_details(
                core_range={"kind": "percentile_interval", "percentile_bounds": [5, 95]}
            ),
            **columns,
        )
    )
    length = display.metric("length")

    assert length.core_range is None
    assert length.core_kind == "percentile_interval"
    assert length.percentile_bounds == (5, 95)
    assert not length.is_percentile_core
    assert display.data_label == DataLabel(kind="none")


# --- Centre statistics --------------------------------------------------------


def test_explicit_mean_and_median_are_reported_separately():
    display = display_from_row(
        _row(
            length_core_min=8.0,
            length_core_max=11.0,
            length_mean=9.4,
            measurement_details_json=_details(median={"value": 9.2}),
        )
    )
    length = display.metric("length")

    assert length.scalar_mean == 9.4
    assert length.median == ScalarStatistic(value=9.2)
    assert length.mean_interval is None
    assert length.has_centre


def test_mean_interval_and_median_interval_stay_intervals():
    display = display_from_row(
        _row(
            length_core_min=8.0,
            length_core_max=11.0,
            measurement_details_json=_details(
                mean_interval={"lower": 9.2, "upper": 9.8, "kind": "reported_range"},
                median={"lower": 9.0, "upper": 9.5, "kind": "reported_range"},
            ),
        )
    )
    length = display.metric("length")

    assert length.mean_interval == IntervalStatistic(lower=9.2, upper=9.8, kind="reported_range")
    assert length.median == IntervalStatistic(lower=9.0, upper=9.5, kind="reported_range")
    assert length.scalar_mean is None


def test_a_range_alone_yields_no_centre_statistic():
    display = display_from_row(_row(length_core_min=8.0, length_core_max=12.0))
    length = display.metric("length")

    assert not length.has_centre
    assert length.scalar_mean is None
    assert length.median is None
    assert length.mean_interval is None


# --- Reported versus computed statistics --------------------------------------


def test_a_published_mean_is_reported_not_computed():
    display = display_from_row(
        _row(length_core_min=8.0, length_core_max=11.0, length_mean=9.4, sample_size=30)
    )

    assert display.statistics_origin == "reported"
    assert display.has_centre_statistic
    assert display.has_reported_statistic
    assert not display.has_computed_statistic
    assert display.sample_size_is_reported


def test_a_community_mean_and_median_are_computed_not_reported():
    display = display_from_community_summary(
        {"length_p50": 9.3, "length_avg": 9.4, "measurement_count": 30}
    )

    assert display.statistics_origin == "computed"
    assert display.has_centre_statistic
    assert display.has_computed_statistic
    assert not display.has_reported_statistic
    # The n behind an aggregate is a count of measurements, not a printed
    # figure, so it must not be presented as something an author reported.
    assert display.sample_size == 30
    assert not display.sample_size_is_reported


def test_personal_observation_statistics_are_computed():
    display = display_from_points([{"length_um": 9.0, "width_um": 5.0}] * 8)

    assert display.statistics_origin == "computed"
    assert display.sample_size == 8
    assert not display.sample_size_is_reported
    # Points alone state no centre, so neither flag claims one.
    assert not display.has_centre_statistic
    assert not display.has_computed_statistic


def test_a_reported_sample_size_survives_without_any_centre_statistic():
    display = display_from_row(_row(length_core_min=8.0, length_core_max=11.0, sample_size=30))

    assert display.statistics_origin == "reported"
    assert display.sample_size_is_reported
    assert not display.has_centre_statistic
    assert not display.has_reported_statistic


def test_a_source_that_states_nothing_has_no_statistics_origin():
    display = display_from_row(_row())

    assert display.statistics_origin == "none"
    assert not display.has_reported_statistic
    assert not display.has_computed_statistic
    assert not display.sample_size_is_reported


# --- Provenance is not a data kind --------------------------------------------


def test_parmasto_style_content_is_a_published_range_with_its_method_kept():
    display = display_from_row(
        _row(
            data_kind="parmasto",
            measurement_method="Parmasto & Parmasto (1987)",
            length_min=7.0,
            length_max=13.0,
            length_core_min=8.0,
            length_core_max=11.0,
            sample_size=30,
            measurement_details_json=_details(
                outer_range={"kind": "reported_extremes"},
                core_range={"kind": "typical_range"},
            ),
        )
    )

    assert display.data_label == DataLabel(kind="published_range")
    assert display.measurement_method == "Parmasto & Parmasto (1987)"
    assert display.stored_data_kind == "parmasto"
    assert display.sample_size == 30
    assert not display.has_raw_points


# --- Legacy and future rows ---------------------------------------------------


def test_legacy_untagged_row_shows_its_numbers_without_inventing_meaning():
    display = display_from_row(
        _row(data_kind="", length_min=7.0, length_max=13.0, length_mean=9.5)
    )
    length = display.metric("length")

    assert display.data_label == DataLabel(kind="published_range")
    assert length.outer_range == (7.0, 13.0)
    assert length.outer_kind is None
    assert length.core_kind is None
    assert length.scalar_mean == 9.5
    assert not display.has_reported_descriptor
    assert not display.inspect_only


def test_unsupported_future_details_version_is_inspect_only():
    display = display_from_row(
        _row(
            length_core_min=8.0,
            length_core_max=11.0,
            measurement_details_json=json.dumps(
                {"schema_version": 7, "something_new": {"length": {}}}
            ),
        )
    )

    assert display.unsupported_details_version == 7
    assert display.inspect_only
    # No descriptor is guessed from a version this binary cannot read.
    assert display.metric("length").core_kind is None
    assert display.data_label == DataLabel(kind="published_range")


def test_undecodable_details_do_not_remove_the_row_from_the_chooser():
    display = display_from_row(
        _row(length_core_min=8.0, length_core_max=11.0, measurement_details_json="{oops")
    )

    assert display.details_unreadable
    assert display.inspect_only
    assert display.metric("length").core_range == (8.0, 11.0)
    assert display.data_label == DataLabel(kind="published_range")


def test_empty_row_states_that_it_holds_no_range():
    display = display_from_row(_row())

    assert display.data_label == DataLabel(kind="none")
    assert not display.has_any_range


# --- The other three source paths ---------------------------------------------


def test_personal_observation_points_are_raw_data_and_summarise_nothing():
    display = display_from_points([{"length_um": 9.0, "width_um": 5.0}] * 24)

    assert display.source_kind == "observation"
    assert display.data_label == DataLabel(kind="raw_data")
    assert display.sample_size == 24
    assert display.metric("length").core_range is None
    assert not display.metric("length").has_centre


def test_community_aggregate_maps_onto_the_existing_contract():
    display = display_from_community_summary(
        {
            "measurement_count": 30,
            "dataset_count": 4,
            "contributor_label": "Community",
            "mount_medium": "KOH",
            "length_min": 7.2,
            "length_p05": 8.0,
            "length_p50": 9.3,
            "length_p95": 11.0,
            "length_max": 12.4,
            "length_avg": 9.4,
            "width_min": 4.0,
            "width_p05": 4.4,
            "width_p95": 6.1,
            "width_max": 6.8,
            "q_min": 1.4,
            "q_p50": 1.7,
            "q_max": 2.1,
            "q_avg": 1.75,
        }
    )
    length = display.metric("length")

    assert display.source_kind == "community"
    assert display.data_label == DataLabel(
        kind="percentile_range", percentile_bounds=COMMUNITY_PERCENTILE_BOUNDS
    )
    assert length.outer_range == (7.2, 12.4)
    assert length.core_range == (8.0, 11.0)
    assert length.is_percentile_core
    assert length.scalar_mean == 9.4
    assert length.median == ScalarStatistic(value=9.3)
    # Q carries no percentile pair in the aggregate: extremes and centre only.
    q = display.metric("q")
    assert q.outer_range == (1.4, 2.1)
    assert q.core_range is None
    assert q.scalar_mean == 1.75
    # An aggregate of other people's points is not this desktop's raw data.
    assert not display.has_raw_points
    # The same n that community_detail_preview_fields prints as "n=30".
    assert display.sample_size == 30
    assert display.statistics_origin == "computed"


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"measurement_count": 30}, 30),
        ({"sample_size": 12}, 12),
        # A locally normalized payload and a raw cloud detail may both be
        # handed in; the cloud's own count wins where they disagree.
        ({"measurement_count": 30, "sample_size": 12}, 30),
        # Zero means no measurements behind the aggregate, not n=0.
        ({"measurement_count": 0}, None),
        ({"measurement_count": None}, None),
        ({"measurement_count": "not a number"}, None),
        ({}, None),
    ],
)
def test_community_sample_size_reads_the_cloud_measurement_count(payload, expected):
    assert community_sample_size(payload) == expected
    assert display_from_community_summary(payload).sample_size == expected


def test_community_aggregate_without_percentiles_is_a_published_range():
    display = display_from_community_summary(
        {"length_min": 7.0, "length_max": 12.0, "width_min": 4.0, "width_max": 6.0}
    )

    assert display.data_label == DataLabel(kind="published_range")
    assert display.metric("length").outer_kind == "reported_extremes"


def test_parser_content_projects_through_the_same_entry_point():
    from references.measurement_parser import parse_measurement_string

    content = parse_measurement_string("(7.0-)8.0-11.0(-13.0) x 4.5-6.0 um").to_content()
    display = display_from_content(content, source_kind="manual")

    assert display.metric("length").outer_range == (7.0, 13.0)
    assert display.metric("length").core_range == (8.0, 11.0)
    assert display.metric("length").outer_kind == "reported_extremes"
    # The parser refuses to say what a printed inner range means, and the
    # projection keeps that refusal instead of promoting it to a percentile.
    assert display.metric("length").core_kind == "unspecified"
    assert display.data_label == DataLabel(kind="published_range")
    assert not display.has_raw_points


def test_typed_content_with_an_explicit_point_count_override():
    display = display_from_content(
        MeasurementContent(
            length_core_min=8.0,
            length_core_max=11.0,
            details=MeasurementDetails(
                metrics={
                    "length": MetricDetails(
                        core_range=RangeDescriptor(
                            kind="percentile_interval", percentile_bounds=(5.0, 95.0)
                        )
                    )
                }
            ),
        ),
        source_kind="manual",
        raw_points=12,
    )

    # Real points outrank every descriptor.
    assert display.data_label == DataLabel(kind="raw_data")
    assert display.raw_point_count == 12


# --- The module's own public surface ------------------------------------------


def test_every_exported_name_and_annotation_actually_resolves():
    """The type aliases must exist, not merely be spelled in an annotation.

    ``from __future__ import annotations`` means a dataclass field typed with a
    name that was never defined still constructs fine, so the behavioural tests
    above cannot catch a half-finished rename of one of the aliases. A star
    import and a type-hint resolution both do, and both are things a caller is
    entitled to do.
    """
    import typing

    from references import reference_display

    missing = [
        name for name in reference_display.__all__ if not hasattr(reference_display, name)
    ]
    assert missing == [], f"__all__ names nothing defines: {missing}"

    for cls in (
        reference_display.DataLabel,
        reference_display.MetricDisplay,
        reference_display.SourceDisplay,
    ):
        typing.get_type_hints(cls)


def test_statistics_origin_lists_exactly_the_values_the_projection_produces():
    """The alias is a contract for later UI stages, so it may not drift.

    A value the module never produces would invite a widget to write a branch
    that can never run; a value it produces but does not list would make a
    legitimate branch look like a typo.
    """
    import typing

    from references.reference_display import StatisticsOrigin

    assert set(typing.get_args(StatisticsOrigin)) == {"reported", "computed", "none"}
