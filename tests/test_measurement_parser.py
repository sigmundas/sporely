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
