"""Stage 4: the shared presentation rules for typed measurement content.

``ui/measurement_content_view.py`` is the one place that turns a
:class:`~references.measurement_content.MeasurementContent` into compact
tags, a reported median / S.D. list and a scalar-or-interval mean field.
It creates no widgets, so these cases run without a QApplication.

Two invariants carry the stage's scientific weight and are asserted here
directly: a percentile number is printed only for a descriptor that
explicitly carries percentile bounds, and a median never reaches any form
of the mean.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

import ui.measurement_content_view as mcv
from references.measurement_content import (
    IntervalStatistic,
    MeasurementContent,
    MeasurementContentError,
    MeasurementDetails,
    MetricDetails,
    RangeDescriptor,
    ScalarStatistic,
    UnsupportedMeasurementDetails,
    validate_measurement_content,
)


def _content(metric: str = "length", **body) -> MeasurementContent:
    return MeasurementContent(
        length_core_min=9.0,
        length_core_max=11.0,
        details=MeasurementDetails(metrics={metric: MetricDetails(**body)}),
    )


# --- Mean input: scalar or interval -------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("9.2", ScalarStatistic(value=9.2)),
        ("  9.2  ", ScalarStatistic(value=9.2)),
        ("9,2", ScalarStatistic(value=9.2)),
        ("9.2-11.7", IntervalStatistic(lower=9.2, upper=11.7, kind="reported_range")),
        ("9.2–11.7", IntervalStatistic(lower=9.2, upper=11.7, kind="reported_range")),
        ("9.2 - 11.7", IntervalStatistic(lower=9.2, upper=11.7, kind="reported_range")),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_parse_mean_cell_reads_scalars_and_intervals(text, expected):
    assert mcv.parse_mean_cell(text) == expected


def test_equal_endpoint_interval_is_not_collapsed_to_a_scalar():
    """The shape distinction the contract stores must survive the editor.

    ``9.2`` and ``9.2-9.2`` are different statements about a source; a UI
    that folded the second into the first would make the round trip lossy.
    """
    statistic = mcv.parse_mean_cell("9.2-9.2")
    assert isinstance(statistic, IntervalStatistic)
    assert (statistic.lower, statistic.upper) == (9.2, 9.2)
    assert mcv.format_statistic(statistic) == "9.2-9.2"
    assert mcv.format_statistic(ScalarStatistic(value=9.2)) == "9.2"


@pytest.mark.parametrize(
    "text",
    ["abc", "9.2-", "-9.2", "9.2-11.7-13", "9.2 11.7", "9.2x", "1e", "--"],
)
def test_unreadable_mean_text_is_refused_not_partially_read(text):
    with pytest.raises(MeasurementContentError):
        mcv.parse_mean_cell(text)


def test_inverted_mean_interval_is_refused():
    with pytest.raises(MeasurementContentError):
        mcv.parse_mean_cell("11.7-9.2")


def test_apply_mean_cell_switches_between_the_two_shapes():
    content = MeasurementContent(length_core_min=9.0, length_core_max=11.0)

    scalar = mcv.apply_mean_cell(content, "length", "9.2")
    assert scalar.length_mean == 9.2
    assert mcv.format_mean_cell(scalar, "length") == "9.2"

    interval = mcv.apply_mean_cell(scalar, "length", "9.2-11.7")
    assert interval.length_mean is None, "switching to an interval must clear the column"
    assert interval.details.metrics["length"].mean_interval == IntervalStatistic(
        lower=9.2, upper=11.7, kind="reported_range"
    )
    assert mcv.format_mean_cell(interval, "length") == "9.2-11.7"

    back = mcv.apply_mean_cell(interval, "length", "9.4")
    assert back.length_mean == 9.4
    assert back.details is None, "switching back must clear the interval"

    cleared = mcv.apply_mean_cell(back, "length", "")
    assert cleared.length_mean is None
    assert cleared.details is None


def test_apply_mean_cell_output_passes_contract_validation():
    """Rule 4: the scalar column and the mean interval are exclusive."""
    content = MeasurementContent(width_core_min=5.0, width_core_max=7.0)
    interval = mcv.apply_mean_cell(content, "width", "5.6-6.7")
    validate_measurement_content(interval, mode="edit")
    assert interval.width_mean is None


# --- Tags ---------------------------------------------------------------------


def test_percentile_numbers_appear_only_for_an_explicit_percentile_descriptor():
    explicit = _content(
        core_range=RangeDescriptor(
            kind="percentile_interval", percentile_bounds=(5.0, 95.0)
        )
    )
    tags = mcv.metric_tags(explicit, "length")
    assert [tag.label for tag in tags] == ["L inner 5–95%"]
    assert "5th–95th percentile" in tags[0].explanation

    for kind in ("unspecified", "typical_range", "reported_range"):
        tags = mcv.metric_tags(_content(core_range=RangeDescriptor(kind=kind)), "length")
        assert len(tags) == 1
        assert "%" not in tags[0].label, kind
        assert "percentile" not in tags[0].label.lower(), kind


def test_unspecified_inner_range_says_so_in_full():
    tags = mcv.metric_tags(_content(core_range=RangeDescriptor(kind="unspecified")), "length")
    assert tags[0].label == "L inner: unspecified"
    assert "interpretation unspecified" in tags[0].explanation


def test_outer_tag_precedes_core_tag_and_names_the_extremes():
    content = MeasurementContent(
        length_min=7.5,
        length_core_min=9.0,
        length_core_max=11.0,
        length_max=13.0,
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(
                    outer_range=RangeDescriptor(kind="reported_extremes"),
                    core_range=RangeDescriptor(kind="unspecified"),
                )
            }
        ),
    )
    labels = [tag.label for tag in mcv.metric_tags(content, "length")]
    assert labels == ["L extremes: reported", "L inner: unspecified"]


def test_a_mean_reported_as_an_interval_earns_its_own_tag():
    content = _content(
        mean_interval=IntervalStatistic(lower=9.2, upper=11.7, kind="reported_range")
    )
    assert [tag.label for tag in mcv.metric_tags(content, "length")] == [
        "L mean: interval"
    ]


def test_content_without_details_has_no_tags():
    assert mcv.content_tags(MeasurementContent(length_core_min=9.0)) == []


def test_content_tags_follow_length_width_q_order():
    content = MeasurementContent(
        details=MeasurementDetails(
            metrics={
                "q": MetricDetails(core_range=RangeDescriptor(kind="unspecified")),
                "length": MetricDetails(core_range=RangeDescriptor(kind="unspecified")),
                "width": MetricDetails(core_range=RangeDescriptor(kind="unspecified")),
            }
        )
    )
    assert [tag.label[0] for tag in mcv.content_tags(content)] == ["L", "W", "Q"]


# --- Reported median and standard deviation -----------------------------------


def test_reported_statistics_never_include_a_mean_in_any_shape():
    content = MeasurementContent(
        length_mean=9.2,
        details=MeasurementDetails(
            metrics={
                "width": MetricDetails(
                    mean_interval=IntervalStatistic(
                        lower=5.6, upper=6.7, kind="reported_range"
                    ),
                    median=ScalarStatistic(value=6.0),
                    sd=ScalarStatistic(value=0.28),
                )
            }
        ),
    )
    labels = [entry.label for entry in mcv.content_reported_statistics(content)]
    assert labels == ["W median 6", "W S.D. 0.28"]
    assert not any("mean" in label.lower() for label in labels)


def test_an_interval_median_keeps_its_interval_shape():
    content = _content(
        median=IntervalStatistic(lower=9.2, upper=11.7, kind="reported_range")
    )
    assert [e.label for e in mcv.reported_statistics(content, "length")] == [
        "L median 9.2-11.7"
    ]


def test_a_reported_median_is_not_offered_as_a_mean_field_value():
    content = _content(median=ScalarStatistic(value=9.2))
    assert mcv.format_mean_cell(content, "length") == ""


# --- Whole-content helpers ----------------------------------------------------


def test_unsupported_details_are_announced_not_interpreted():
    content = MeasurementContent(
        details=UnsupportedMeasurementDetails(schema_version=99, raw={"schema_version": 99})
    )
    notice = mcv.unsupported_details_notice(content)
    assert notice and "99" in notice
    assert mcv.content_tags(content) == []
    assert mcv.content_reported_statistics(content) == []
    assert mcv.has_reported_content(content) is True


@pytest.mark.parametrize(
    "content, expected",
    [
        (MeasurementContent(), False),
        (MeasurementContent(length_core_min=9.0, length_core_max=11.0), False),
        (MeasurementContent(q_core_min=1.1, q_core_max=1.3), True),
        (_content(core_range=RangeDescriptor(kind="unspecified")), True),
    ],
)
def test_has_reported_content_tracks_the_extension_columns(content, expected):
    assert mcv.has_reported_content(content) is expected


def test_discarding_reported_statistics_keeps_every_measured_number():
    content = MeasurementContent(
        length_min=7.5,
        length_core_min=9.0,
        length_core_max=11.0,
        length_max=13.0,
        length_mean=9.2,
        q_core_min=1.1,
        q_core_max=1.3,
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(
                    outer_range=RangeDescriptor(kind="reported_extremes"),
                    core_range=RangeDescriptor(kind="unspecified"),
                    median=ScalarStatistic(value=9.9),
                    sd=ScalarStatistic(value=0.6),
                )
            }
        ),
    )
    stripped = mcv.without_reported_statistics(content)
    assert stripped.details is None
    for column in (
        "length_min",
        "length_core_min",
        "length_core_max",
        "length_max",
        "length_mean",
        "q_core_min",
        "q_core_max",
    ):
        assert getattr(stripped, column) == getattr(content, column), column
    validate_measurement_content(stripped, mode="edit")


def test_discarding_reported_statistics_keeps_a_mean_reported_as_an_interval():
    """The mean field is editable, so the discard control must not empty it."""
    content = MeasurementContent(
        length_core_min=9.0,
        length_core_max=11.0,
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(
                    core_range=RangeDescriptor(kind="unspecified"),
                    mean_interval=IntervalStatistic(
                        lower=9.2, upper=11.7, kind="reported_range"
                    ),
                    median=ScalarStatistic(value=9.9),
                )
            }
        ),
    )
    stripped = mcv.without_reported_statistics(content)
    body = stripped.details.metrics["length"]
    assert body.mean_interval == content.details.metrics["length"].mean_interval
    assert (body.core_range, body.median, body.sd) == (None, None, None)
    assert stripped.length_mean is None
    validate_measurement_content(stripped, mode="edit")


def test_discarding_reported_statistics_leaves_unsupported_details_alone():
    content = MeasurementContent(
        details=UnsupportedMeasurementDetails(schema_version=99, raw={"schema_version": 99})
    )
    assert mcv.without_reported_statistics(content) is content


def test_retracting_a_range_interpretation_keeps_everything_else():
    """Correction by retraction: the claim goes, the evidence stays."""
    content = MeasurementContent(
        length_min=7.5,
        length_core_min=9.0,
        length_core_max=11.0,
        length_max=13.0,
        width_core_min=5.0,
        width_core_max=7.0,
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(
                    outer_range=RangeDescriptor(kind="reported_extremes"),
                    core_range=RangeDescriptor(
                        kind="percentile_interval", percentile_bounds=(5.0, 95.0)
                    ),
                    median=ScalarStatistic(value=9.9),
                    sd=ScalarStatistic(value=0.6),
                ),
                "width": MetricDetails(core_range=RangeDescriptor(kind="unspecified")),
            }
        ),
    )
    corrected = mcv.without_range_tags(content, "length")

    length = corrected.details.metrics["length"]
    assert (length.outer_range, length.core_range) == (None, None)
    assert length.median.value == pytest.approx(9.9)
    assert length.sd.value == pytest.approx(0.6)
    # Other metrics and every number are untouched.
    assert corrected.details.metrics["width"].core_range.kind == "unspecified"
    assert corrected.length_min == pytest.approx(7.5)
    assert corrected.length_core_max == pytest.approx(11.0)
    validate_measurement_content(corrected, mode="edit")


def test_retracting_the_only_claim_normalises_the_details_away():
    content = _content(core_range=RangeDescriptor(kind="unspecified"))
    assert mcv.without_range_tags(content, "length").details is None


def test_retraction_is_a_no_op_for_a_metric_with_no_range_tag():
    content = _content(median=ScalarStatistic(value=9.9))
    assert mcv.without_range_tags(content, "width") == content
    assert mcv.without_range_tags(content, "length") == content


def test_metrics_with_range_tags_lists_only_what_can_be_corrected():
    content = MeasurementContent(
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(core_range=RangeDescriptor(kind="unspecified")),
                "width": MetricDetails(median=ScalarStatistic(value=6.0)),
                "q": MetricDetails(
                    outer_range=RangeDescriptor(kind="reported_extremes")
                ),
            }
        )
    )
    assert mcv.metrics_with_range_tags(content) == ["length", "q"]
    assert mcv.metrics_with_range_tags(MeasurementContent()) == []


# --- Folding editor values back into content ----------------------------------


def test_fold_assigns_numbers_without_inventing_a_tag():
    folded = mcv.fold_metric_inputs(
        MeasurementContent(),
        {"length": mcv.MetricInput(core_min=9.0, core_max=11.0)},
    )
    assert (folded.length_core_min, folded.length_core_max) == (9.0, 11.0)
    assert folded.details is None


def test_fold_clears_a_descriptor_with_the_pair_it_describes():
    content = MeasurementContent(
        length_min=7.5,
        length_core_min=9.0,
        length_core_max=11.0,
        length_max=13.0,
        details=MeasurementDetails(
            metrics={
                "length": MetricDetails(
                    outer_range=RangeDescriptor(kind="reported_extremes"),
                    core_range=RangeDescriptor(kind="unspecified"),
                )
            }
        ),
    )
    folded = mcv.fold_metric_inputs(
        content, {"length": mcv.MetricInput(core_min=9.0, core_max=11.0)}
    )
    assert folded.details.metrics["length"].outer_range is None
    assert folded.details.metrics["length"].core_range.kind == "unspecified"
    assert folded.length_min is None and folded.length_max is None
    validate_measurement_content(folded, mode="edit")


def test_fold_routes_the_mean_through_the_contract_operations():
    folded = mcv.fold_metric_inputs(
        MeasurementContent(),
        {
            "length": mcv.MetricInput(core_min=9.0, core_max=11.0, mean_text="9.2-11.7"),
            "width": mcv.MetricInput(core_min=5.0, core_max=7.0, mean_text="6.1"),
        },
    )
    assert folded.length_mean is None
    assert folded.details.metrics["length"].mean_interval.upper == pytest.approx(11.7)
    assert folded.width_mean == pytest.approx(6.1)
    assert "width" not in folded.details.metrics
    validate_measurement_content(folded, mode="edit")


def test_fold_strict_raises_on_a_bad_mean_but_lenient_keeps_the_previous_one():
    base = mcv.fold_metric_inputs(
        MeasurementContent(), {"length": mcv.MetricInput(mean_text="9.2")}
    )
    bad = {"length": mcv.MetricInput(mean_text="about 10")}

    with pytest.raises(MeasurementContentError):
        mcv.fold_metric_inputs(base, bad, strict=True)

    kept = mcv.fold_metric_inputs(base, bad, strict=False)
    assert kept.length_mean == pytest.approx(9.2)


def test_fold_leaves_unsupported_details_untouched():
    content = MeasurementContent(
        length_core_min=9.0,
        details=UnsupportedMeasurementDetails(
            schema_version=99, raw={"schema_version": 99}
        ),
    )
    folded = mcv.fold_metric_inputs(
        content, {"length": mcv.MetricInput(core_min=1.0, core_max=2.0)}
    )
    assert folded is content


def test_fold_ignores_metrics_the_caller_did_not_supply():
    content = MeasurementContent(width_core_min=5.0, width_core_max=7.0)
    folded = mcv.fold_metric_inputs(
        content, {"length": mcv.MetricInput(core_min=9.0, core_max=11.0)}
    )
    assert folded.width_core_min == pytest.approx(5.0)
    assert folded.length_core_min == pytest.approx(9.0)


def test_compact_and_explanation_text_pair_up():
    tags = [mcv.MeaningTag("A", "first"), mcv.MeaningTag("B", "second")]
    assert mcv.compact_tag_text(tags) == "A · B"
    assert mcv.explanation_text(tags) == "first\nsecond"


def test_format_number_is_compact_and_empty_for_none():
    assert mcv.format_number(None) == ""
    assert mcv.format_number(0.6) == "0.6"
    assert mcv.format_number(9.0) == "9"
