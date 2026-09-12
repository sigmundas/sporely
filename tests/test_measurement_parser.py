"""Unit tests for the fungal spore reference measurement parser."""

from __future__ import annotations

import math

import pytest

from references.measurement_parser import (
    DimensionRange,
    parse_measurement_string,
    swap_length_width,
)


def _close(a, b, tol=1e-9):
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, abs_tol=tol)


# --- Simple A-B x C-D --------------------------------------------------------


def test_parses_plain_ascii_range():
    r = parse_measurement_string("10.2-12.5 x 5.5-6.8")
    assert r.ok
    assert _close(r.length.p05, 10.2) and _close(r.length.p95, 12.5)
    assert r.length.min is None and r.length.max is None
    assert _close(r.width.p05, 5.5) and _close(r.width.p95, 6.8)
    assert r.width.min is None and r.width.max is None
    assert r.q.is_empty()
    assert r.q_mean is None and r.n is None


def test_parses_comma_decimals_and_unicode_dash_and_unit():
    r = parse_measurement_string("10,2–12,5 × 5,5–6,8 µm")
    assert r.ok
    assert _close(r.length.p05, 10.2) and _close(r.length.p95, 12.5)
    assert _close(r.width.p05, 5.5) and _close(r.width.p95, 6.8)


def test_accepts_micro_sign_um_and_plain_um_without_unit():
    for raw in ("10-12 x 5-6 µm", "10-12 x 5-6 μm", "10-12 x 5-6 um", "10-12 x 5-6"):
        r = parse_measurement_string(raw)
        assert r.ok, raw
        assert _close(r.length.p05, 10.0) and _close(r.length.p95, 12.0)
        assert _close(r.width.p05, 5.0) and _close(r.width.p95, 6.0)


def test_accepts_em_dash_and_asterisk_separator():
    r = parse_measurement_string("10—12 * 5—6")
    assert r.ok
    assert _close(r.length.p05, 10.0) and _close(r.length.p95, 12.0)
    assert _close(r.width.p05, 5.0) and _close(r.width.p95, 6.0)


# --- (A-)B-C(-D) -------------------------------------------------------------


def test_parses_full_paren_range_for_both_dimensions():
    r = parse_measurement_string("(9.5-)9.8-11.3(-11.7) × (7.3-)8.0-9.4(-9.4) µm")
    assert r.ok
    assert _close(r.length.min, 9.5)
    assert _close(r.length.p05, 9.8)
    assert _close(r.length.p95, 11.3)
    assert _close(r.length.max, 11.7)
    assert _close(r.width.min, 7.3)
    assert _close(r.width.p05, 8.0)
    assert _close(r.width.p95, 9.4)
    assert _close(r.width.max, 9.4)


def test_handles_leading_extreme_only():
    r = parse_measurement_string("(9-)10-12 × 5-6")
    assert _close(r.length.min, 9.0)
    assert _close(r.length.p05, 10.0)
    assert _close(r.length.p95, 12.0)
    assert r.length.max is None


def test_handles_trailing_extreme_only():
    r = parse_measurement_string("10-12(-13) × 5-6")
    assert r.length.min is None
    assert _close(r.length.p05, 10.0)
    assert _close(r.length.p95, 12.0)
    assert _close(r.length.max, 13.0)


# --- Q and Qm and n ----------------------------------------------------------


def test_parses_simple_q_range():
    r = parse_measurement_string("9.5–12 × 7–10 µm, Q = 1.2–1.4")
    assert r.ok
    assert _close(r.q.p05, 1.2) and _close(r.q.p95, 1.4)
    assert r.q.min is None and r.q.max is None
    assert r.q_mean is None


def test_parses_full_q_with_extremes_qm_and_n():
    raw = (
        "(9.5–)9.8–11.3(–11.7) × (7.3–)8.0–9.4(–9.4), "
        "Q = (1.1–)1.1–1.3(–1.3), Qm = 1.2, n = 36"
    )
    r = parse_measurement_string(raw)
    assert r.ok
    assert _close(r.q.min, 1.1)
    assert _close(r.q.p05, 1.1)
    assert _close(r.q.p95, 1.3)
    assert _close(r.q.max, 1.3)
    assert _close(r.q_mean, 1.2)
    assert r.n == 36


def test_qm_alone_populates_q_mean_only_and_not_centre():
    r = parse_measurement_string("10-12 × 5-6, Qm = 1.9")
    assert _close(r.q_mean, 1.9)
    assert r.q.is_empty(), (
        "Qm must not silently fill Q centre; the schema stores them separately"
    )


def test_q_not_derived_from_length_and_width():
    r = parse_measurement_string("10-12 × 5-6")
    assert r.q.is_empty()
    assert r.q_mean is None
    assert any("Q not present in source." == w for w in r.warnings)


# --- Edge inputs -------------------------------------------------------------


def test_empty_input_returns_empty_result():
    r = parse_measurement_string("")
    assert not r.ok
    assert r.length.is_empty() and r.width.is_empty() and r.q.is_empty()

    r2 = parse_measurement_string("   ")
    assert not r2.ok


def test_garbage_input_does_not_raise():
    r = parse_measurement_string("hjulsopp")
    # No numerics → nothing to parse, but no crash.
    assert not r.ok
    assert any("could not parse" in w for w in r.warnings)


def test_only_one_range_warns_about_missing_width():
    r = parse_measurement_string("9-12 µm")
    assert r.ok
    assert _close(r.length.p05, 9.0) and _close(r.length.p95, 12.0)
    assert r.width.is_empty()
    assert any("width left empty" in w for w in r.warnings)


def test_triple_range_records_centre_explicitly():
    # Some keys give "lower-typical / mean / upper-typical" as three numbers.
    r = parse_measurement_string("9-10-11 × 5-5.5-6")
    assert _close(r.length.p05, 9.0)
    assert _close(r.length.p50, 10.0)
    assert _close(r.length.p95, 11.0)
    assert _close(r.width.p05, 5.0)
    assert _close(r.width.p50, 5.5)
    assert _close(r.width.p95, 6.0)


def test_single_value_treated_as_centre():
    r = parse_measurement_string("11 × 6")
    assert _close(r.length.p50, 11.0)
    assert _close(r.width.p50, 6.0)
    assert r.length.p05 is None and r.length.p95 is None


def test_n_can_be_parsed_independently():
    r = parse_measurement_string("10-12 × 5-6, n = 42")
    assert r.n == 42


def test_warnings_flag_when_first_range_is_assumed_to_be_length():
    r = parse_measurement_string("10-12 × 5-6")
    assert any("Parsed first range as length." == w for w in r.warnings)


# --- Swap helper -------------------------------------------------------------


def test_swap_length_width_swaps_dimensions_and_preserves_q_qm_n():
    r = parse_measurement_string("10-12 × 5-6, Q = 1.6-2.4, Qm = 2.0, n = 30")
    swapped = swap_length_width(r)
    assert _close(swapped.length.p05, 5.0) and _close(swapped.length.p95, 6.0)
    assert _close(swapped.width.p05, 10.0) and _close(swapped.width.p95, 12.0)
    assert _close(swapped.q.p05, 1.6) and _close(swapped.q.p95, 2.4)
    assert _close(swapped.q_mean, 2.0)
    assert swapped.n == 30
    assert any("swapped" in w.lower() for w in swapped.warnings)


# --- to_record_dict ----------------------------------------------------------


def test_to_record_dict_uses_legacy_column_names():
    raw = "(9.5-)9.8-11.3(-11.7) × (7.3-)8.0-9.4(-9.4), Q = 1.2-1.3, Qm = 1.25, n = 36"
    r = parse_measurement_string(raw)
    rec = r.to_record_dict()
    assert _close(rec["length_min"], 9.5)
    assert _close(rec["length_p05"], 9.8)
    assert _close(rec["length_p95"], 11.3)
    assert _close(rec["length_max"], 11.7)
    assert _close(rec["width_min"], 7.3)
    assert _close(rec["width_p05"], 8.0)
    assert _close(rec["width_p95"], 9.4)
    assert _close(rec["width_max"], 9.4)
    assert _close(rec["q_p05"], 1.2)
    assert _close(rec["q_p95"], 1.3)
    assert _close(rec["q_avg"], 1.25)
    assert rec["n"] == 36


# --- DimensionRange smoke ----------------------------------------------------


def test_dimension_range_is_empty_for_default():
    assert DimensionRange().is_empty()


def test_dimension_range_not_empty_when_any_field_set():
    assert not DimensionRange(p50=1.0).is_empty()
    assert not DimensionRange(min=1.0).is_empty()


# --- Whitespace / casing tolerance ------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "10.2-12.5x5.5-6.8",
        "10.2-12.5  X  5.5-6.8",
        "  10.2-12.5  x  5.5-6.8  µm  ",
        "10.2 - 12.5 × 5.5 - 6.8",
    ],
)
def test_whitespace_and_case_tolerance(raw):
    r = parse_measurement_string(raw)
    assert r.ok, raw
    assert _close(r.length.p05, 10.2) and _close(r.length.p95, 12.5)
    assert _close(r.width.p05, 5.5) and _close(r.width.p95, 6.8)


# =============================================================================
# Stage 2 — reported statistics, explicit range semantics, tables
# =============================================================================

import json
from pathlib import Path

from references.measurement_content import (
    IntervalStatistic,
    MeasurementDetails,
    RangeDescriptor,
    ScalarStatistic,
    encode_measurement_details,
    validate_measurement_content,
)

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"
UNSPECIFIED = RangeDescriptor("unspecified")
EXTREMES = RangeDescriptor("reported_extremes")
P05_95 = RangeDescriptor("percentile_interval", (5, 95))

HEBELOMA_ROWS = [
    ["Length (µm)", "(7.5) 8.4–13.0 (13.2)", "9.2–11.7", "9.2–11.7", "0.600"],
    ["Width (µm)", "(5.0) 5.1–7.2 (7.6)", "5.6–6.7", "5.6–6.7", "0.280"],
    ["Q", "(1.30) 1.42–1.96 (2.07)", "1.55–1.78", "1.54–1.79", "0.095"],
]
HEBELOMA_HEADER = ["Spore", "(min) 5%-95% (max)", "mean", "median", "S.D."]


def _table(header, rows, sep="\t"):
    lines = [sep.join(header)] if header else []
    lines += [sep.join(row) for row in rows]
    return "\n".join(lines)


def _markdown(header, rows):
    lines = []
    if header:
        lines.append("| " + " | ".join(header) + " |")
        lines.append("| " + " | ".join("---" for _ in header) + " |")
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def _valid_content(result):
    content = result.to_content()
    validate_measurement_content(content, mode="edit")
    return content


# --- Compact string forms, HTML, contamination -------------------------------


def test_hebeloma_compact_parentheses_without_dashes():
    r = parse_measurement_string(
        "(7.5) 8.4–13.0 (13.2)x(5.0) 5.1–7.2 (7.6), q=(1.30) 1.42–1.96 (2.07)"
    )
    assert r.length == DimensionRange(7.5, 8.4, None, 13.0, 13.2)
    assert r.width == DimensionRange(5.0, 5.1, None, 7.2, 7.6)
    assert r.q == DimensionRange(1.30, 1.42, None, 1.96, 2.07)
    assert r.q_mean is None
    for metric in ("length", "width", "q"):
        assert r.metric_details[metric].outer_range == EXTREMES
        assert r.metric_details[metric].core_range == UNSPECIFIED, "no heading, no percentile claim"
    content = _valid_content(r)
    assert (content.q_min, content.q_core_min, content.q_core_max, content.q_max) == (1.30, 1.42, 1.96, 2.07)


@pytest.mark.parametrize(
    "raw",
    [
        "8.4&ndash;13.0&#x20;&times;&#x20;5.1&ndash;7.2 &micro;m",
        "8.4–13.0&nbsp;x&nbsp;5.1–7.2 µm",
        "8.4&#8211;13.0 &#215; 5.1&#8211;7.2",
    ],
)
def test_html_entities_are_decoded_before_dimension_splitting(raw):
    r = parse_measurement_string(raw)
    assert _close(r.length.p05, 8.4) and _close(r.length.p95, 13.0)
    assert _close(r.width.p05, 5.1) and _close(r.width.p95, 7.2)
    assert r.raw == raw, "raw source preserved verbatim"
    assert not any("could not parse" in w for w in r.warnings)


def test_named_statistics_do_not_contaminate_width():
    r = parse_measurement_string("8.4–13.0 x 5.1–7.2 Qav = 1.5 n = 30")
    assert r.width == DimensionRange(None, 5.1, None, 7.2, None)
    assert r.q_mean == 1.5 and r.n == 30
    # A trailing stray token makes the whole named expression malformed; it is
    # rejected and removed as a unit, so width is still intact.
    r = parse_measurement_string("8.4–13.0 × 5.1–7.2, Qm = 1.5–1.7 x")
    assert r.width == DimensionRange(None, 5.1, None, 7.2, None)
    assert "q" not in r.metric_details and r.q_mean is None
    assert "Qm: could not parse '1.5-1.7 x'." in r.warnings
    r = parse_measurement_string("8.4–13.0 × 5.1–7.2, Qm = 1.5–1.7")
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.5, 1.7, "reported_range")


# --- Q, Qm, Qav ----------------------------------------------------------------


@pytest.mark.parametrize("label", ["Qav", "Qm", "qav", "QM"])
def test_qav_and_qm_scalar_populate_q_mean_only(label):
    r = parse_measurement_string(f"10-12 × 5-6, {label} = 1.9")
    assert _close(r.q_mean, 1.9)
    assert r.q.is_empty()
    assert "q" not in r.metric_details
    content = _valid_content(r)
    assert content.q_mean == 1.9 and content.details is not None
    assert "q" not in content.details.metrics


@pytest.mark.parametrize("label", ["Qav", "Qm"])
def test_qav_and_qm_interval_populate_q_mean_interval(label):
    r = parse_measurement_string(f"10-12 × 5-6, {label} = 1.5–1.7")
    assert r.q_mean is None, "an interval never becomes a scalar mean"
    assert r.q.is_empty()
    assert r.metric_details["q"] == MetricDetails_like(mean_interval=IntervalStatistic(1.5, 1.7, "reported_range"))
    content = _valid_content(r)
    assert content.q_mean is None
    assert content.details.metrics["q"].mean_interval == IntervalStatistic(1.5, 1.7, "reported_range")


def MetricDetails_like(**kwargs):
    from references.measurement_content import MetricDetails
    return MetricDetails(**kwargs)


def test_equal_endpoint_qav_interval_is_preserved_as_interval():
    r = parse_measurement_string("10-12 × 5-6, Qav = 1.6–1.6")
    assert r.q_mean is None
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.6, 1.6, "reported_range")


def test_ordinary_q_range_stays_independent_of_qav():
    r = parse_measurement_string("10-12 × 5-6, Q = 1.4–1.8, Qav = 1.5–1.7")
    assert r.q == DimensionRange(None, 1.4, None, 1.8, None)
    details = r.metric_details["q"]
    assert details.core_range == UNSPECIFIED and details.outer_range is None
    assert details.mean_interval == IntervalStatistic(1.5, 1.7, "reported_range")
    content = _valid_content(r)
    assert (content.q_core_min, content.q_core_max, content.q_min, content.q_max) == (1.4, 1.8, None, None)

    full = parse_measurement_string("10-12 × 5-6, Q = (1.1–)1.2–1.8(–1.9), Qm = 1.5")
    assert full.q == DimensionRange(1.1, 1.2, None, 1.8, 1.9)
    assert full.q_mean == 1.5
    assert full.metric_details["q"].outer_range == EXTREMES
    assert full.metric_details["q"].mean_interval is None
    _valid_content(full)


def test_conflicting_repeated_named_values_warn_and_keep_the_first():
    r = parse_measurement_string("10-12 × 5-6, Qm = 1.5, Qav = 1.7")
    assert r.q_mean == 1.5
    assert any("repeated with a different value" in w and "kept the first" in w for w in r.warnings)
    r = parse_measurement_string("10-12 × 5-6, Q = 1.4-1.8, Q = 1.5-1.9, n = 20, n = 25")
    assert r.q == DimensionRange(None, 1.4, None, 1.8, None)
    assert r.n == 20
    assert sum("repeated with a different value" in w for w in r.warnings) == 2
    # Identical repeats are not conflicts.
    r = parse_measurement_string("10-12 × 5-6, Qm = 1.5, Qav = 1.5")
    assert r.q_mean == 1.5
    assert not any("repeated" in w for w in r.warnings)


def test_single_q_value_together_with_qm_warns_and_keeps_both():
    r = parse_measurement_string("10-12 × 5-6, Q = 1.5, Qm = 1.6")
    assert r.q.p50 == 1.5 and r.q_mean == 1.6
    assert any("both given" in w for w in r.warnings)
    content = _valid_content(r)
    assert content.q_mean == 1.6
    assert content.q_core_min is None, "the legacy centre has no typed home"


def test_named_label_without_value_warns():
    r = parse_measurement_string("10-12 × 5-6, Qm = approx, n = 30")
    assert r.q_mean is None and r.n == 30
    assert any(w.startswith("Qm: could not parse") for w in r.warnings)
    assert r.width == DimensionRange(None, 5.0, None, 6.0, None)


# --- Typed output of the compact form -------------------------------------------


def test_compact_form_typed_content_tags_unspecified_core_and_no_outer_without_extremes():
    r = parse_measurement_string("10.2-12.5 x 5.5-6.8")
    content = _valid_content(r)
    assert content.length_core_min == 10.2 and content.length_core_max == 12.5
    assert content.length_min is None and content.length_max is None
    assert content.details.metrics["length"].core_range == UNSPECIFIED
    assert content.details.metrics["length"].outer_range is None
    assert "q" not in content.details.metrics
    assert content.q_mean is None and content.length_mean is None and content.width_mean is None
    assert content.raw_text == "10.2-12.5 x 5.5-6.8"


def test_one_sided_extreme_is_not_tagged_as_reported_extremes():
    r = parse_measurement_string("(9-)10-12 × 5-6")
    assert r.length == DimensionRange(9.0, 10.0, None, 12.0, None)
    assert r.metric_details["length"].outer_range is None
    content = r.to_content()
    assert content.length_min == 9.0 and content.length_max is None
    validate_measurement_content(content, mode="edit")


def test_legacy_centre_values_stay_out_of_typed_content():
    r = parse_measurement_string("11 × 6")
    assert r.length.p50 == 11.0 and r.width.p50 == 6.0
    content = _valid_content(r)
    assert content.length_mean is None and content.width_mean is None
    assert content.details is None
    triple = parse_measurement_string("9-10-11 × 5-5.5-6")
    content = _valid_content(triple)
    assert content.length_mean is None
    assert content.details.metrics["length"].core_range == UNSPECIFIED


def test_to_record_dict_and_legacy_fields_unchanged_by_typed_output():
    raw = _table(HEBELOMA_HEADER, HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    record = r.to_record_dict()
    assert set(record) == {
        f"{p}_{k}" for p in ("length", "width", "q") for k in ("min", "p05", "p50", "p95", "max")
    } | {"q_avg", "n"}
    assert record["length_p50"] is None and record["width_p50"] is None and record["q_p50"] is None
    assert record["q_avg"] is None, "interval means never reach the legacy scalar"
    assert r.q_mean is None and r.length_mean is None and r.width_mean is None


# --- Tables: Hebeloma layouts ----------------------------------------------------


def _assert_hebeloma_numbers(r):
    assert r.length == DimensionRange(7.5, 8.4, None, 13.0, 13.2)
    assert r.width == DimensionRange(5.0, 5.1, None, 7.2, 7.6)
    assert r.q == DimensionRange(1.30, 1.42, None, 1.96, 2.07)
    assert r.q_mean is None and r.length.p50 is None


@pytest.mark.parametrize("separator", ["\t", " ", " | "])
def test_hebeloma_headed_table_in_clipboard_shapes(separator):
    raw = "Spore measurements\n" + _table(HEBELOMA_HEADER, HEBELOMA_ROWS, sep=separator)
    r = parse_measurement_string(raw)
    _assert_hebeloma_numbers(r)
    length = r.metric_details["length"]
    assert length.outer_range == EXTREMES and length.core_range == P05_95
    assert length.mean_interval == IntervalStatistic(9.2, 11.7, "reported_range")
    assert length.median == IntervalStatistic(9.2, 11.7, "reported_range")
    assert length.sd == ScalarStatistic(0.6)
    assert r.metric_details["width"].sd == ScalarStatistic(0.28)
    assert r.metric_details["q"].median == IntervalStatistic(1.54, 1.79, "reported_range")
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.55, 1.78, "reported_range")
    assert r.raw == raw
    assert any("5%-95% percentile interval" in w for w in r.warnings)
    assert not any("could not parse" in w or "Unknown" in w for w in r.warnings)
    _valid_content(r)


def test_hebeloma_rows_with_newline_separated_cells_are_not_a_table():
    # Each cell on its own line is indistinguishable from unlabelled rows;
    # it must not be silently reinterpreted. Nothing is shifted anywhere.
    raw = "\n".join("\n".join(row) for row in HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    assert r.q_mean is None and r.length_mean is None


def test_hebeloma_markdown_with_regular_header():
    r = parse_measurement_string(_markdown(HEBELOMA_HEADER, HEBELOMA_ROWS))
    _assert_hebeloma_numbers(r)
    assert r.metric_details["length"].core_range == P05_95
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.55, 1.78, "reported_range")
    _valid_content(r)


def test_glued_hebeloma_heading_is_an_explicit_recognized_form():
    raw = "| Spore(min) 5%-95% (max)meanmedianS.D. | | | | |\n| --- | --- | --- | --- | --- |\n"
    raw += "\n".join("| " + " | ".join(row) + " |" for row in HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    _assert_hebeloma_numbers(r)
    assert r.metric_details["length"].core_range == P05_95
    assert r.metric_details["length"].mean_interval == IntervalStatistic(9.2, 11.7, "reported_range")
    assert not any("Unrecognized" in w or "Unknown" in w for w in r.warnings)
    # Also as a bare glued line above tab rows.
    r2 = parse_measurement_string("Spore(min) 5%-95% (max)meanmedianS.D.\n" + _table(None, HEBELOMA_ROWS))
    assert r2.metric_details == r.metric_details
    _valid_content(r)


def test_stage1_enhanced_fixture_raw_text_reproduces_the_fixture():
    """The Stage 1 acceptance fixture: glued heading, unlabelled L/W/Q rows."""
    row = json.loads((FIXTURES / "row_enhanced.json").read_text(encoding="utf-8"))
    hebeloma = json.loads((FIXTURES / "details_hebeloma_v1.json").read_text(encoding="utf-8"))
    r = parse_measurement_string(row["raw_text"])
    assert any("assumed length, width and Q" in w for w in r.warnings)
    content = _valid_content(r)
    for column in (
        "length_min", "length_core_min", "length_core_max", "length_max",
        "width_min", "width_core_min", "width_core_max", "width_max",
        "q_min", "q_core_min", "q_core_max", "q_max", "q_mean", "length_mean", "width_mean",
    ):
        assert getattr(content, column) == row[column], column
    assert content.sample_size is None
    assert encode_measurement_details(content.details) == row["measurement_details_json"]
    assert json.loads(encode_measurement_details(content.details)) == hebeloma


@pytest.mark.parametrize("line_break", ["<br>", "<br/>", "<BR />"])
def test_velutipes_markdown_mean_ranges_with_html_break(line_break):
    raw = """| Spore(min) 5%-95% (max)meanmedianS.D. | | | | |
| --- | --- | --- | --- | --- |
| Length (µm) | (6.9) 8.0–15.2 (16.1) | 8.9–13.7 | 9.0–13.9 | 0.696 |
| Width (µm) | (4.1) 5.1–8.2 (8.9) | 5.6–7.5 | 5.6–7.5 | 0.323 |
| Q | (1.17) 1.36–2.19 (2.79) | 1.51–1.96 | 1.50–1.95 | 0.097BREAK |""".replace("BREAK", line_break)
    r = parse_measurement_string(raw)
    assert r.metric_details["length"].mean_interval == IntervalStatistic(8.9, 13.7, "reported_range")
    assert r.metric_details["width"].mean_interval == IntervalStatistic(5.6, 7.5, "reported_range")
    assert r.metric_details["width"].median == IntervalStatistic(5.6, 7.5, "reported_range")
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.51, 1.96, "reported_range")
    assert r.metric_details["q"].median == IntervalStatistic(1.50, 1.95, "reported_range")
    assert r.metric_details["q"].sd == ScalarStatistic(0.097)
    assert r.q.min == 1.17 and r.q.max == 2.79
    assert r.q_mean is None and r.length.p50 is None
    assert r.raw == raw
    assert not any("could not parse" in w for w in r.warnings)


def test_html_breaks_between_rows_become_row_boundaries():
    raw = "Spore\t(min) 5%-95% (max)\tmean<br>" + "<br/>".join("\t".join(row[:3]) for row in HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    _assert_hebeloma_numbers(r)
    assert r.metric_details["width"].mean_interval == IntervalStatistic(5.6, 6.7, "reported_range")
    assert r.metric_details["width"].median is None and r.metric_details["width"].sd is None


# --- Tables: column mapping -----------------------------------------------------


def test_missing_mean_column_does_not_shift_median_and_sd():
    header = ["Spore", "(min) 5%-95% (max)", "median", "S.D."]
    rows = [[row[0], row[1], row[3], row[4]] for row in HEBELOMA_ROWS]
    r = parse_measurement_string(_table(header, rows))
    _assert_hebeloma_numbers(r)
    for metric, row in zip(("length", "width", "q"), HEBELOMA_ROWS):
        details = r.metric_details[metric]
        assert details.mean_interval is None
        lo, hi = (float(v) for v in row[3].replace("–", "-").split("-"))
        assert details.median == IntervalStatistic(lo, hi, "reported_range")
        assert details.sd == ScalarStatistic(float(row[4]))
    assert r.length_mean is None and r.width_mean is None and r.q_mean is None
    _valid_content(r)


def test_reordered_columns_follow_their_headings():
    raw = (
        "Spore | S.D. | median | mean | range\n"
        "Length | 0.6 | 9.2-11.7 | 10.1 | (7.5) 8.4-13.0 (13.2)\n"
        "Width | 0.28 | 5.6-6.7 | 6.1-6.1 | (5.0) 5.1-7.2 (7.6)"
    )
    r = parse_measurement_string(raw)
    assert r.length == DimensionRange(7.5, 8.4, None, 13.0, 13.2)
    assert r.width == DimensionRange(5.0, 5.1, None, 7.2, 7.6)
    length, width = r.metric_details["length"], r.metric_details["width"]
    # "range" heading claims no percentile: core stays unspecified.
    assert length.core_range == UNSPECIFIED and length.outer_range == EXTREMES
    assert r.length_mean == 10.1 and length.mean_interval is None, "scalar mean cell is a scalar mean"
    assert width.mean_interval == IntervalStatistic(6.1, 6.1, "reported_range"), "equal endpoints stay an interval"
    assert r.width_mean is None
    assert length.median == IntervalStatistic(9.2, 11.7, "reported_range")
    assert length.sd == ScalarStatistic(0.6) and width.sd == ScalarStatistic(0.28)
    assert not any("percentile" in w for w in r.warnings)
    content = _valid_content(r)
    assert content.length_mean == 10.1 and content.width_mean is None


def test_range_only_headed_table_and_percentile_heading_variants():
    r = parse_measurement_string("Spore\t(min) 5 %–95 % (max)\nLength\t(7.5) 8.4–13.0 (13.2)\nWidth\t(5.0) 5.1–7.2 (7.6)")
    assert r.metric_details["length"].core_range == P05_95
    assert r.metric_details["length"].mean_interval is None
    r = parse_measurement_string("Spore\t2.5%-97.5%\nLength\t8.4–13.0\nWidth\t5.1–7.2")
    assert r.metric_details["length"].core_range == RangeDescriptor("percentile_interval", (2.5, 97.5))
    assert r.metric_details["length"].outer_range is None
    _valid_content(r)
    r = parse_measurement_string("Spore\t95%-5%\nLength\t8.4–13.0\nWidth\t5.1–7.2")
    assert r.metric_details["length"].core_range == UNSPECIFIED
    assert any("not a valid percentile interval" in w for w in r.warnings)


def test_scalar_q_mean_cell_in_a_headed_table_populates_q_mean():
    r = parse_measurement_string("Spore\trange\tmean\nLength\t8-12\t10\nWidth\t4-6\t5\nQ\t1.5-2.5\t2.0")
    assert r.length_mean == 10.0 and r.width_mean == 5.0 and r.q_mean == 2.0
    assert r.length.p50 is None and r.q.p50 is None
    content = _valid_content(r)
    assert (content.length_mean, content.width_mean, content.q_mean) == (10.0, 5.0, 2.0)
    assert content.details.metrics["q"].mean_interval is None


def test_unknown_duplicate_and_ambiguous_headings_warn_and_ignore_their_cells():
    r = parse_measurement_string("Spore\trange\tfoo\tmean\nLength\t8-12\t999\t9\nWidth\t4-7\t888\t5")
    assert any("Unknown table column heading 'foo'" in w for w in r.warnings)
    assert r.length_mean == 9.0 and r.width_mean == 5.0
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)

    r = parse_measurement_string("Spore\tmean\tmean\trange\nLength\t9\t10\t8-12\nWidth\t5\t6\t4-7")
    assert any("Duplicate 'mean'" in w for w in r.warnings)
    assert r.length_mean is None and r.width_mean is None
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)

    r = parse_measurement_string("Spore\trange\tmean/median\nLength\t8-12\t9\nWidth\t4-7\t5")
    assert any("Ambiguous table column heading" in w for w in r.warnings)
    assert r.length_mean is None and "length" in r.metric_details
    assert r.metric_details["length"].median is None


def test_short_rows_and_malformed_cells_never_shift_values():
    raw = "Spore\trange\tmean\tmedian\tS.D.\nLength\t8-12\t\t9.5\t0.6\nWidth\t4-7\tabc\t5.5\nQ\t1.5-2.5\t2.0"
    r = parse_measurement_string(raw)
    length, width, q = (r.metric_details[m] for m in ("length", "width", "q"))
    assert r.length_mean is None and length.median == ScalarStatistic(9.5) and length.sd == ScalarStatistic(0.6)
    assert r.width_mean is None and width.median == ScalarStatistic(5.5) and width.sd is None
    assert any("Width: mean cell 'abc' not recognized" in w for w in r.warnings)
    assert any("Width: no cell for the 'sd' column" in w for w in r.warnings)
    assert r.q_mean == 2.0 and q.median is None and q.sd is None
    assert any("Q: no cell for the 'median' column" in w for w in r.warnings)
    _valid_content(r)


def test_extra_cells_and_duplicate_rows_warn():
    raw = "Spore\trange\tmean\nLength\t8-12\t10\t77\nWidth\t4-7\t5\nLength\t1-2\t1.5"
    r = parse_measurement_string(raw)
    assert any("Length: 1 extra cell(s) ignored" in w for w in r.warnings)
    assert any("Length: duplicate row ignored" in w for w in r.warnings)
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None) and r.length_mean == 10.0


def test_inverted_or_prose_cells_are_not_recognized():
    r = parse_measurement_string("Spore\trange\tmean\tS.D.\nLength\t8-12\t11-9\t-0.5\nWidth\t4-7\t5\t0.2")
    assert any("Length: mean cell '11-9' not recognized" in w for w in r.warnings)
    assert r.metric_details["length"].mean_interval is None and r.length_mean is None
    assert r.metric_details["length"].sd is None
    assert r.metric_details["width"].sd == ScalarStatistic(0.2)


def test_placeholder_cells_are_silently_absent():
    r = parse_measurement_string("Spore\trange\tmean\tmedian\nLength\t8-12\t—\tn/a\nWidth\t4-7\t-\t")
    assert r.length_mean is None and r.metric_details["length"].median is None
    assert not any("not recognized" in w for w in r.warnings)


# --- Tables: headerless legacy layout -------------------------------------------


def test_headerless_labelled_rows_use_documented_layout_with_unspecified_meaning():
    r = parse_measurement_string(_table(None, HEBELOMA_ROWS))
    _assert_hebeloma_numbers(r)
    length = r.metric_details["length"]
    assert length.core_range == UNSPECIFIED, "no heading: no percentile claim"
    assert length.outer_range == EXTREMES
    assert length.mean_interval == IntervalStatistic(9.2, 11.7, "reported_range")
    assert length.median == IntervalStatistic(9.2, 11.7, "reported_range")
    assert length.sd == ScalarStatistic(0.6)
    assert any("range meanings are unspecified" in w for w in r.warnings)
    _valid_content(r)


def test_headerless_range_only_rows_do_not_warn_about_missing_statistics():
    r = parse_measurement_string("Length (µm) 8–12\nWidth (µm) 5–7")
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)
    assert r.width == DimensionRange(None, 5.0, None, 7.0, None)
    assert not any("no cell for" in w for w in r.warnings)
    assert r.metric_details["length"] == MetricDetails_like(core_range=UNSPECIFIED)


def test_headerless_rows_with_unexpected_cell_counts_use_only_the_range():
    r = parse_measurement_string("Length (µm) 8–12 9–10 9–10 0.6 999\nWidth (µm) 5–7 6")
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)
    assert r.metric_details["length"].mean_interval is None and r.metric_details["length"].sd is None
    assert r.width == DimensionRange(None, 5.0, None, 7.0, None)
    assert r.width_mean is None, "a lone second cell is not assumed to be the mean"
    assert sum("only the range was used" in w for w in r.warnings) == 2


def test_headerless_rows_with_prose_are_malformed_not_guessed():
    r = parse_measurement_string("Length 8–12 unknown\nWidth 5–7")
    assert r.length.is_empty()
    assert any("Length: could not parse table row" in w for w in r.warnings)
    assert r.width.p05 == 5.0


def test_unlabelled_rows_need_a_heading_and_exactly_two_or_three_rows():
    rows = "\n".join("\t".join(row[1:]) for row in HEBELOMA_ROWS)
    headed = parse_measurement_string(_table(HEBELOMA_HEADER, []) + "\n" + rows)
    _assert_hebeloma_numbers(headed)
    assert any("assumed length, width and Q" in w for w in headed.warnings)
    too_many = parse_measurement_string(_table(HEBELOMA_HEADER, []) + "\n" + rows + "\n1-2\t1-2\t1-2\t0.1")
    assert not too_many.ok
    assert any("4 unlabelled row(s)" in w for w in too_many.warnings)
    mixed = parse_measurement_string(_table(HEBELOMA_HEADER, HEBELOMA_ROWS[:2]) + "\n" + "\t".join(HEBELOMA_ROWS[2][1:]))
    assert mixed.q.is_empty()
    assert any("Unlabelled table row ignored" in w for w in mixed.warnings)


def test_reordered_headings_are_read_by_heading_not_position():
    raw = "Spore (min) 5%-95% (max) median mean S.D.\n" + _table(None, HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    _assert_hebeloma_numbers(r)
    # Third cell is the mean per the heading order (median, mean), values equal here.
    assert r.metric_details["q"].median == IntervalStatistic(1.55, 1.78, "reported_range")
    assert r.metric_details["q"].mean_interval == IntervalStatistic(1.54, 1.79, "reported_range")


def test_compact_strings_are_never_mistaken_for_tables():
    for raw in ("10-12 x 5-6, Q = 1.5-2.0, Qm = 1.7, n = 30", "Spore range\n10-12 x 5-6", "9-12 µm"):
        r = parse_measurement_string(raw)
        assert not any("table" in w.lower() for w in r.warnings), raw


# --- Swap -----------------------------------------------------------------------


def test_swap_moves_numbers_means_and_details_together():
    raw = "Spore | range | mean | median | S.D.\nLength | (7.5) 8.4-13.0 (13.2) | 10.1 | 9.2-11.7 | 0.6\nWidth | (5.0) 5.1-7.2 (7.6) | 6.1-6.1 | 5.6-6.7 | 0.28\nQ | 1.4-1.9 | 1.7 | | "
    r = parse_measurement_string(raw)
    s = swap_length_width(r)
    assert s.length == r.width and s.width == r.length
    assert s.length_mean is None and s.width_mean == 10.1
    assert s.metric_details["length"] == r.metric_details["width"]
    assert s.metric_details["width"] == r.metric_details["length"]
    assert s.metric_details["q"] == r.metric_details["q"] and s.q == r.q and s.q_mean == 1.7
    content = _valid_content(s)
    assert content.width_mean == 10.1 and content.length_mean is None
    assert content.details.metrics["length"].mean_interval == IntervalStatistic(6.1, 6.1, "reported_range")
    assert swap_length_width(s).metric_details == r.metric_details


def test_to_content_shape():
    content = parse_measurement_string("").to_content()
    assert content.details is None and content.raw_text is None
    r = parse_measurement_string(_table(HEBELOMA_HEADER, HEBELOMA_ROWS))
    content = r.to_content()
    assert isinstance(content.details, MeasurementDetails)
    assert set(content.details.metrics) == {"length", "width", "q"}
    assert content.character is None and content.data_kind is None, "the caller owns row identity"


# --- Sparring round 1 regressions ------------------------------------------------


def test_unknown_word_in_space_separated_heading_keeps_its_column_position():
    raw = "Spore Range variance mean median S.D.\nLength 8-12 0.1 10 9 0.4\nWidth 5-7 0.2 6 5.5 0.3"
    r = parse_measurement_string(raw)
    assert any("Unrecognized text in table heading: 'variance'" in w for w in r.warnings)
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)
    assert r.length_mean == 10.0, "mean is bound to its own column, not to the variance cell"
    assert r.metric_details["length"].median == ScalarStatistic(9.0)
    assert r.metric_details["length"].sd == ScalarStatistic(0.4)
    assert r.width_mean == 6.0 and r.metric_details["width"].sd == ScalarStatistic(0.3)
    _valid_content(r)


def test_row_count_mismatch_under_ambiguous_heading_binds_nothing():
    raw = "Spore Range variance mean median S.D.\nLength 8-12 10 9 0.4\nWidth 5-7 0.2 6 5.5 0.3"
    r = parse_measurement_string(raw)
    assert r.length.is_empty() and r.length_mean is None and "length" not in r.metric_details
    assert any("Length: 4 value(s) for 5 heading column(s)" in w and "row not read" in w for w in r.warnings)
    assert r.width_mean == 6.0


def test_partial_heading_match_is_unknown_not_a_statistic():
    raw = "Spore | range | mean error | mean\nLength | 8-12 | 0.5 | 10\nWidth | 5-7 | 0.2 | 6"
    r = parse_measurement_string(raw)
    assert any("Unknown table column heading 'mean error'" in w for w in r.warnings)
    assert not any("Duplicate" in w for w in r.warnings)
    assert r.length_mean == 10.0 and r.width_mean == 6.0
    r = parse_measurement_string("Spore\trange\tmeaningful\nLength\t8-12\t9\nWidth\t5-7\t6")
    assert any("Unknown table column heading 'meaningful'" in w for w in r.warnings)
    assert r.length_mean is None and r.width_mean is None


def test_short_space_separated_row_binds_nothing_but_delimited_short_row_binds_positionally():
    raw = "Spore Range mean median S.D.\nLength 8-12    9 0.4\nWidth 5-7 6 5.5 0.3"
    r = parse_measurement_string(raw)
    assert r.length.is_empty() and r.length_mean is None and "length" not in r.metric_details
    assert any("Length: 3 value(s) for 4 heading column(s) with no cell boundaries" in w for w in r.warnings)
    assert r.width == DimensionRange(None, 5.0, None, 7.0, None)
    assert r.width_mean == 6.0 and r.metric_details["width"].median == ScalarStatistic(5.5)
    # Long rows are equally ambiguous without boundaries.
    r = parse_measurement_string("Spore Range mean\nLength 8-12 9 0.4\nWidth 5-7 6")
    assert r.length.is_empty() and any("Length: 3 value(s) for 2 heading" in w for w in r.warnings)
    assert r.width_mean == 6.0
    # With tab boundaries the same short row binds range and mean only.
    r = parse_measurement_string("Spore\tRange\tmean\tmedian\tS.D.\nLength\t8-12\t9\nWidth\t5-7\t6\t5.5\t0.3")
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None) and r.length_mean == 9.0
    assert r.metric_details["length"].median is None and r.metric_details["length"].sd is None
    assert any("Length: no cell for the 'median' column" in w for w in r.warnings)


def test_headings_without_a_label_column_and_unknown_first_headings_align_by_position():
    r = parse_measurement_string("Range | mean\nLength | 8-12 | 9\nWidth | 5-7 | 6")
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None) and r.length_mean == 9.0
    assert r.width_mean == 6.0
    r = parse_measurement_string("| foo | range | mean |\n| 1 | 8-12 | 9 |\n| 2 | 5-7 | 6 |")
    assert any("Unknown table column heading 'foo'" in w for w in r.warnings)
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None) and r.length_mean == 9.0
    assert r.width == DimensionRange(None, 5.0, None, 7.0, None) and r.width_mean == 6.0


@pytest.mark.parametrize(
    "value",
    [
        "1.5e2", "1.5%", "1.5x", "1.5.2", "12abc",
        # Sparring round 2: no fallback to a shorter scalar or interval prefix.
        "1.5-2e2", "1.5-", "1.5-2.5%", "1.5±0.2", "1.5/2", "1.5 approx", "1.5-1.7 x",
        "(1.3) 1.4-1.9 (2.1)x", "1.5 1.7", "-1.5",
    ],
)
def test_malformed_named_values_are_rejected_whole_and_do_not_leak_into_width(value):
    r = parse_measurement_string(f"8-12 x 5-7; Qav = {value}")
    assert r.q_mean is None and "q" not in r.metric_details
    assert any(w == f"Qav: could not parse '{value}'." for w in r.warnings), r.warnings
    assert r.length == DimensionRange(None, 8.0, None, 12.0, None)
    assert r.width == DimensionRange(None, 5.0, None, 7.0, None)
    assert not any("Width: could not parse" in w for w in r.warnings)


def test_glued_heading_with_other_percentiles_is_still_recognized():
    raw = "Spore(min) 2.5%-97.5% (max)meanmedianS.D.\n" + _table(None, HEBELOMA_ROWS)
    r = parse_measurement_string(raw)
    _assert_hebeloma_numbers(r)
    assert r.metric_details["length"].core_range == RangeDescriptor("percentile_interval", (2.5, 97.5))
    assert r.metric_details["length"].mean_interval == IntervalStatistic(9.2, 11.7, "reported_range")
    assert not any("Unrecognized" in w for w in r.warnings)
