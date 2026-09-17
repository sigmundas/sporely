"""Stage 2: the real ``references.measurement_content`` module against the
frozen contract (``docs/reference-data/measurement-content-contract.md``) and
the Stage 1 fixtures. Covers contract section 12 cases 1, 2, 3 and 18 at the
module level; repository, import and snapshot cases wait for Stage 3.
"""
from __future__ import annotations

import json
import math
from dataclasses import replace
from pathlib import Path

import pytest

from references import measurement_content as mc
from references.measurement_content import (
    IntervalStatistic,
    MeasurementContent,
    MeasurementContentError,
    MeasurementDetails,
    MetricDetails,
    RangeDescriptor,
    ScalarStatistic,
    UnsupportedMeasurementDetails,
    acknowledgement_state,
    acknowledges_extension,
    clear_pair,
    clear_statistic,
    content_from_row,
    content_row_updates,
    decode_measurement_details,
    encode_measurement_details,
    is_enhanced_row,
    measurement_details_equal,
    set_mean_interval,
    set_scalar_mean,
    swap_length_width,
    validate_measurement_content,
)
from tests.test_measurement_content_contract_fixtures import load_contract_spec

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def enhanced_row() -> dict:
    return load("row_enhanced.json")


@pytest.fixture
def legacy_row() -> dict:
    return load("row_legacy_only.json")


@pytest.fixture
def hebeloma() -> dict:
    return load("details_hebeloma_v1.json")


@pytest.fixture
def enhanced(enhanced_row) -> MeasurementContent:
    return content_from_row(enhanced_row)


def _validate_row(row: dict, details_obj: dict, mode: str = "edit") -> None:
    """Decode ``details_obj`` as the row's details and validate together."""
    text = json.dumps(details_obj)
    content = content_from_row({**row, "measurement_details_json": text})
    validate_measurement_content(content, mode=mode)


# --- Constants pinned to the contract spec block -----------------------------


def test_module_constants_match_the_contract_spec_block():
    spec = load_contract_spec()
    assert spec["details_schema_version"] == mc.MEASUREMENT_DETAILS_SCHEMA_VERSION
    assert set(spec["supported_details_versions"]) == mc.SUPPORTED_DETAILS_VERSIONS
    assert spec["details_max_bytes"] == mc.MEASUREMENT_DETAILS_MAX_BYTES
    assert tuple(spec["metrics"]) == mc.METRICS
    assert set(spec["metric_keys"]) == mc.METRIC_KEYS
    assert set(spec["outer_range_kinds"]) == mc.OUTER_RANGE_KINDS
    assert set(spec["core_range_kinds"]) == mc.CORE_RANGE_KINDS
    assert set(spec["interval_kinds"]) == mc.INTERVAL_KINDS
    assert tuple(spec["extension_fields"]) == mc.EXTENSION_FIELDS
    assert set(spec["scientific_content_fields"]) == mc.SCIENTIFIC_CONTENT_FIELDS
    assert len(mc.SCIENTIFIC_CONTENT_FIELDS) == 26
    assert set(spec["validation_modes"]) == {"edit", "authoritative"}
    # The typed intermediate carries exactly the 26 fields, details decoded.
    content_fields = {f for f in MeasurementContent.__dataclass_fields__}
    assert content_fields == (mc.SCIENTIFIC_CONTENT_FIELDS - {"measurement_details_json"}) | {"details"}


def test_measurement_content_error_is_a_value_error():
    assert issubclass(MeasurementContentError, ValueError)


# --- Codec ---------------------------------------------------------------------


def test_decode_none_empty_and_json_null_are_absent():
    assert decode_measurement_details(None) is None
    assert decode_measurement_details("") is None
    assert decode_measurement_details("   ") is None
    assert decode_measurement_details("null") is None


@pytest.mark.parametrize("text", ["{", "[]", "1", '"x"', '{"metrics": {}}', '{"schema_version": "1", "metrics": {}}',
                                  '{"schema_version": true, "metrics": {}}'])
def test_decode_rejects_malformed_text_and_versions(text):
    with pytest.raises(MeasurementContentError):
        decode_measurement_details(text)


def test_encode_hebeloma_equals_the_stored_canonical_text(enhanced_row, hebeloma):
    """Contract section 12 case 3."""
    details = decode_measurement_details(json.dumps(hebeloma, indent=2))
    assert isinstance(details, MeasurementDetails)
    encoded = encode_measurement_details(details)
    assert encoded == enhanced_row["measurement_details_json"]
    # Canonical: compact, sorted, ASCII, integer percentile bounds preserved.
    assert encoded == json.dumps(hebeloma, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    assert "[5,95]" in encoded
    # Round trip is stable and typed.
    assert decode_measurement_details(encoded) == details


def test_details_equality_is_decoded_object_equality(enhanced_row, hebeloma):
    stored = enhanced_row["measurement_details_json"]
    reordered = json.dumps(hebeloma, indent=3, sort_keys=False)
    assert stored != reordered
    assert measurement_details_equal(stored, reordered)
    assert measurement_details_equal(None, None)
    assert not measurement_details_equal(stored, None)
    assert not measurement_details_equal(None, stored)
    changed = json.loads(stored)
    changed["metrics"]["length"]["sd"]["value"] = 0.7
    assert not measurement_details_equal(stored, json.dumps(changed))


def test_semantically_empty_details_encode_to_none_and_are_invalid_when_stored():
    assert encode_measurement_details(None) is None
    assert encode_measurement_details(MeasurementDetails(metrics={})) is None
    assert encode_measurement_details(MeasurementDetails(metrics={"length": MetricDetails()})) is None
    # Stored form must never be empty: decoding succeeds, validation rejects.
    stored_empty = decode_measurement_details('{"schema_version": 1, "metrics": {}}')
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(MeasurementContent(details=stored_empty), mode="authoritative")
    with pytest.raises(MeasurementContentError):
        _validate_row({}, {"schema_version": 1, "metrics": {"width": {}}})


def test_content_row_round_trip(enhanced_row, legacy_row, hebeloma):
    enhanced = content_from_row(enhanced_row)
    assert isinstance(enhanced.details, MeasurementDetails)
    assert enhanced.q_core_min == 1.36 and enhanced.q_core_max == 2.19
    updates = content_row_updates(enhanced)
    assert set(updates) == mc.SCIENTIFIC_CONTENT_FIELDS
    assert updates["measurement_details_json"] == enhanced_row["measurement_details_json"]
    for key, value in updates.items():
        assert value == enhanced_row[key], key

    legacy = content_from_row(legacy_row)
    assert legacy.details is None
    legacy_updates = content_row_updates(legacy)
    assert legacy_updates["measurement_details_json"] is None
    assert legacy_updates["q_core_min"] is None and legacy_updates["q_core_max"] is None
    assert legacy.q_min == 1.7 and legacy.q_max == 2.5, "unknown-role Q pair preserved"

    # Rows without the extension keys (today's MeasurementSet) read as None.
    partial = content_from_row({k: v for k, v in legacy_row.items() if k not in mc.EXTENSION_FIELDS})
    assert partial.details is None and partial.q_core_min is None


def test_acknowledgement_and_enhancement_helpers(enhanced_row, legacy_row):
    assert is_enhanced_row(enhanced_row) and not is_enhanced_row(legacy_row)
    assert acknowledgement_state(enhanced_row) == "complete"
    assert acknowledgement_state(legacy_row) == "complete"
    assert acknowledgement_state(load("import_row_omitting_extension.json")) == "absent"
    assert acknowledgement_state(load("import_row_explicit_null_extension.json")) == "complete"
    assert acknowledgement_state(load("import_row_partial_extension.json")) == "partial"
    assert acknowledges_extension(enhanced_row)
    assert not acknowledges_extension(load("import_row_partial_extension.json"))
    # Enhancement is about values, acknowledgement about key presence.
    assert is_enhanced_row(load("import_row_partial_extension.json"))
    assert not is_enhanced_row(load("import_row_explicit_null_extension.json"))


# --- Validation of the example and its mutations ------------------------------


def test_enhanced_fixture_validates_in_both_modes(enhanced):
    validate_measurement_content(enhanced, mode="edit")
    validate_measurement_content(enhanced, mode="authoritative")


def test_legacy_fixture_validates(legacy_row):
    validate_measurement_content(content_from_row(legacy_row), mode="edit")


def test_mode_is_required_and_closed(enhanced):
    with pytest.raises(TypeError):
        validate_measurement_content(enhanced)  # type: ignore[call-arg]
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(enhanced, mode="lenient")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d["metrics"]["length"].__setitem__("basis", "verbatim"),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("percentile_bounds", [95, 5]),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("kind", "typical_range"),
        lambda d: d["metrics"]["length"]["mean_interval"].__setitem__("lower", 14.0),
        lambda d: d["metrics"]["length"]["sd"].__setitem__("value", -0.1),
        lambda d: d["metrics"]["length"]["sd"].__setitem__("value", True),
        lambda d: d["metrics"]["length"]["median"].__setitem__("value", 9.0),
        lambda d: d["metrics"].__setitem__("depth", {"sd": {"value": 1}}),
        lambda d: d["metrics"].__setitem__("width", {}),
        lambda d: d.__setitem__("schema_version", 0),
        lambda d: d["metrics"]["length"]["outer_range"].__setitem__("kind", "typical_range"),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("kind", "reported_extremes"),
        lambda d: d["metrics"]["length"]["mean_interval"].__setitem__("kind", "percentile_interval"),
        lambda d: d["metrics"]["length"]["mean_interval"].__setitem__("lower", 0),
        lambda d: d["metrics"]["length"]["mean_interval"].__setitem__("upper", "13.7"),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("percentile_bounds", [5, 101]),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("percentile_bounds", [5, 5]),
        lambda d: d["metrics"]["length"]["core_range"].__setitem__("percentile_bounds", [-1, 95]),
        lambda d: d["metrics"]["length"]["core_range"].__delitem__("percentile_bounds"),
        lambda d: d["metrics"]["length"]["median"].pop("kind"),
        lambda d: d["metrics"]["length"]["outer_range"].__setitem__("percentile_bounds", [5, 95]),
        lambda d: d.__setitem__("notes", "x"),
    ],
    ids=[
        "unknown-key-basis", "inverted-percentiles", "bounds-without-percentile-kind",
        "inverted-interval", "negative-sd", "boolean-number", "median-value-and-interval",
        "unknown-metric", "empty-metric", "wrong-version",
        "outer-kind-not-extremes", "core-kind-from-outer-enum", "mean-kind-not-interval-kind",
        "non-positive-endpoint", "string-number", "percentile-above-100", "equal-percentiles",
        "negative-percentile", "percentile-kind-without-bounds", "median-interval-missing-kind",
        "outer-with-bounds", "unknown-top-level-key",
    ],
)
def test_invalid_mutations_of_the_example_are_rejected(mutation, hebeloma, enhanced_row):
    """Contract section 12 case 1, with the module raising MeasurementContentError."""
    mutated = json.loads(json.dumps(hebeloma))
    mutation(mutated)
    with pytest.raises(MeasurementContentError):
        _validate_row(enhanced_row, mutated)
    if mutated.get("schema_version") in mc.SUPPORTED_DETAILS_VERSIONS:
        with pytest.raises(MeasurementContentError):
            _validate_row(enhanced_row, mutated, mode="authoritative")
    else:
        # An unknown version is opaque on authoritative paths (section 3).
        _validate_row(enhanced_row, mutated, mode="authoritative")


def test_mean_interval_and_scalar_mean_are_exclusive(enhanced):
    with pytest.raises(MeasurementContentError, match="length_mean"):
        validate_measurement_content(replace(enhanced, length_mean=11.0), mode="edit")
    with pytest.raises(MeasurementContentError, match="q_mean"):
        validate_measurement_content(replace(enhanced, q_mean=1.7), mode="edit")
    # Clearing the interval first, then setting the scalar, is the valid path.
    cleared = clear_statistic(enhanced, "length", "mean")
    validate_measurement_content(set_scalar_mean(cleared, "length", 11.0), mode="edit")


def test_descriptor_without_complete_pair_is_rejected(enhanced):
    with pytest.raises(MeasurementContentError, match="q_core_max"):
        validate_measurement_content(replace(enhanced, q_core_max=None), mode="edit")
    with pytest.raises(MeasurementContentError, match="length_max"):
        validate_measurement_content(replace(enhanced, length_max=None), mode="edit")
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(replace(enhanced, width_core_min=None), mode="authoritative")


def test_outer_range_must_enclose_core_range(enhanced):
    with pytest.raises(MeasurementContentError, match="enclose"):
        validate_measurement_content(replace(enhanced, length_min=8.5), mode="edit")
    with pytest.raises(MeasurementContentError, match="enclose"):
        validate_measurement_content(replace(enhanced, q_max=2.0), mode="edit")
    # Equal outer and core endpoints are allowed.
    validate_measurement_content(replace(enhanced, length_min=8.0, length_max=15.2), mode="edit")


def test_enclosure_is_checked_only_when_the_outer_role_is_explicit(enhanced):
    untagged = clear_pair(enhanced, "length", "outer")
    untagged = replace(untagged, length_min=9.0, length_max=14.0)  # unknown-role pair inside the core
    validate_measurement_content(untagged, mode="edit")
    assert untagged.details.metrics["length"].outer_range is None


def test_pair_ordering_is_enforced_without_descriptors(legacy_row):
    legacy = content_from_row(legacy_row)
    for lo, hi in (("q_min", "q_max"), ("length_core_min", "length_core_max")):
        inverted = replace(legacy, **{lo: 9.0, hi: 3.0})
        with pytest.raises(MeasurementContentError, match=lo):
            validate_measurement_content(inverted, mode="authoritative")
    # Ordered or half-present pairs are fine.
    validate_measurement_content(replace(legacy, length_min=None, length_max=13.0), mode="edit")
    validate_measurement_content(replace(legacy, q_core_min=1.9, q_core_max=1.9), mode="edit")


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, "10", -1.0, 0, 0.0])
@pytest.mark.parametrize("mode", ["edit", "authoritative"])
def test_column_numbers_must_be_finite_positive_non_boolean_numbers(legacy_row, bad, mode):
    """Contract section 1: dimensions and Q values are finite and positive.
    Positivity does not depend on a descriptor being present."""
    legacy = content_from_row(legacy_row)
    with pytest.raises(MeasurementContentError, match="length_min"):
        validate_measurement_content(replace(legacy, length_min=bad), mode=mode)


@pytest.mark.parametrize(
    "columns",
    [
        {"length_mean": -2.0},
        {"q_mean": 0},
        {"width_mean": float("nan")},
        {"length_min": -3.0, "length_max": -1.0},  # ordered, untagged, still negative
        {"q_core_min": 0.0, "q_core_max": 2.0},
    ],
    ids=["negative-scalar-mean", "zero-q-mean", "nan-width-mean", "negative-untagged-pair", "zero-core-bound"],
)
def test_nonpositive_scalar_means_and_untagged_bounds_are_rejected(legacy_row, columns):
    legacy = content_from_row(legacy_row)
    for mode in ("edit", "authoritative"):
        with pytest.raises(MeasurementContentError):
            validate_measurement_content(replace(legacy, **columns), mode=mode)
    # Positive values in the same slots are fine.
    positive = {name: 1.5 if name.endswith("min") or name.endswith("mean") else 2.5 for name in columns}
    validate_measurement_content(replace(legacy, **positive), mode="edit")


def test_equal_endpoint_intervals_are_preserved_as_intervals():
    details = MeasurementDetails(
        metrics={
            "width": MetricDetails(
                mean_interval=IntervalStatistic(6.1, 6.1, "reported_range"),
                median=IntervalStatistic(6.1, 6.1, "typical_range"),
            )
        }
    )
    content = MeasurementContent(details=details)
    validate_measurement_content(content, mode="edit")
    encoded = encode_measurement_details(details)
    obj = json.loads(encoded)
    assert obj["metrics"]["width"]["mean_interval"] == {"lower": 6.1, "upper": 6.1, "kind": "reported_range"}
    assert obj["metrics"]["width"]["median"] == {"lower": 6.1, "upper": 6.1, "kind": "typical_range"}
    decoded = decode_measurement_details(encoded)
    assert isinstance(decoded.metrics["width"].median, IntervalStatistic)
    assert isinstance(decoded.metrics["width"].mean_interval, IntervalStatistic)


def test_median_scalar_and_interval_are_distinct_shapes():
    scalar = MeasurementDetails(metrics={"length": MetricDetails(median=ScalarStatistic(10.5))})
    interval = MeasurementDetails(
        metrics={"length": MetricDetails(median=IntervalStatistic(10.5, 10.5, "reported_range"))}
    )
    for details in (scalar, interval):
        validate_measurement_content(MeasurementContent(details=details), mode="edit")
    assert json.loads(encode_measurement_details(scalar))["metrics"]["length"]["median"] == {"value": 10.5}
    assert not measurement_details_equal(
        encode_measurement_details(scalar), encode_measurement_details(interval)
    )
    assert isinstance(decode_measurement_details(encode_measurement_details(scalar)).metrics["length"].median, ScalarStatistic)


def test_sd_is_non_negative_and_zero_is_allowed():
    zero = MeasurementContent(details=MeasurementDetails(metrics={"q": MetricDetails(sd=ScalarStatistic(0))}))
    validate_measurement_content(zero, mode="edit")
    negative = MeasurementContent(details=MeasurementDetails(metrics={"q": MetricDetails(sd=ScalarStatistic(-0.01))}))
    with pytest.raises(MeasurementContentError, match="non-negative"):
        validate_measurement_content(negative, mode="edit")
    nan = MeasurementContent(details=MeasurementDetails(metrics={"q": MetricDetails(sd=ScalarStatistic(math.nan))}))
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(nan, mode="edit")


def test_percentile_bounds_edge_values():
    def content(bounds):
        return MeasurementContent(
            length_core_min=8.0,
            length_core_max=15.0,
            details=MeasurementDetails(
                metrics={"length": MetricDetails(core_range=RangeDescriptor("percentile_interval", bounds))}
            ),
        )

    validate_measurement_content(content((0, 100)), mode="edit")
    validate_measurement_content(content((2.5, 97.5)), mode="edit")
    for bad in ((100, 0), (50, 50), (0, 100.5), (True, 95), (5,)):
        with pytest.raises(MeasurementContentError):
            validate_measurement_content(content(bad), mode="edit")


def test_unspecified_core_tag_requires_the_pair_but_nothing_else():
    tagged = MeasurementContent(
        q_core_min=1.4, q_core_max=1.8,
        details=MeasurementDetails(metrics={"q": MetricDetails(core_range=RangeDescriptor("unspecified"))}),
    )
    validate_measurement_content(tagged, mode="edit")
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(replace(tagged, q_core_max=None), mode="edit")


# --- Unsupported future versions -----------------------------------------------


def test_unsupported_future_version_is_opaque_and_read_only(enhanced_row):
    """Contract section 12 cases 2 and 18."""
    future = load("details_unsupported_future_version.json")
    text = json.dumps(future, indent=2)
    decoded = decode_measurement_details(text)
    assert isinstance(decoded, UnsupportedMeasurementDetails)
    assert decoded.schema_version == 2
    assert decoded.raw == future
    canonical = encode_measurement_details(decoded)
    assert json.loads(canonical) == future
    assert canonical == json.dumps(future, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    assert measurement_details_equal(text, canonical)

    content = content_from_row({**enhanced_row, "measurement_details_json": text})
    with pytest.raises(MeasurementContentError, match="unsupported measurement details version"):
        validate_measurement_content(content, mode="edit")
    validate_measurement_content(content, mode="authoritative")
    assert content_row_updates(content)["measurement_details_json"] == canonical

    # Every edit operation refuses to touch it.
    for op in (
        lambda c: clear_pair(c, "length", "core"),
        lambda c: clear_statistic(c, "length", "mean"),
        lambda c: set_scalar_mean(c, "length", 11.0),
        lambda c: set_mean_interval(c, "length", 9.0, 12.0, "reported_range"),
        swap_length_width,
    ):
        with pytest.raises(MeasurementContentError, match="unsupported"):
            op(content)


def test_authoritative_mode_still_enforces_numeric_rules_around_opaque_details(enhanced_row):
    future = json.dumps(load("details_unsupported_future_version.json"))
    content = content_from_row({**enhanced_row, "measurement_details_json": future, "q_core_min": 3.0})
    with pytest.raises(MeasurementContentError, match="q_core_min"):
        validate_measurement_content(content, mode="authoritative")
    oversized = {"schema_version": 7, "blob": "x" * (mc.MEASUREMENT_DETAILS_MAX_BYTES + 1)}
    content = content_from_row({**enhanced_row, "measurement_details_json": json.dumps(oversized)})
    with pytest.raises(MeasurementContentError, match="exceed"):
        validate_measurement_content(content, mode="authoritative")


def test_only_versions_in_the_supported_set_decode_as_typed():
    for version in (0, -1, 2, 3):
        decoded = decode_measurement_details(json.dumps({"schema_version": version, "metrics": {}}))
        assert isinstance(decoded, UnsupportedMeasurementDetails), version
        assert version not in mc.SUPPORTED_DETAILS_VERSIONS


# --- Edit operations -----------------------------------------------------------


def test_clear_pair_removes_numbers_and_descriptor_together(enhanced):
    cleared = clear_pair(enhanced, "q", "core")
    assert cleared.q_core_min is None and cleared.q_core_max is None
    assert cleared.details.metrics["q"].core_range is None
    assert cleared.details.metrics["q"].outer_range == RangeDescriptor("reported_extremes")
    assert cleared.details.metrics["q"].sd == ScalarStatistic(0.097)
    validate_measurement_content(cleared, mode="edit")
    # Input is untouched.
    assert enhanced.q_core_min == 1.36 and enhanced.details.metrics["q"].core_range is not None

    outer_cleared = clear_pair(enhanced, "width", "outer")
    assert outer_cleared.width_min is None and outer_cleared.width_max is None
    assert outer_cleared.details.metrics["width"].outer_range is None
    assert outer_cleared.details.metrics["width"].core_range is not None
    validate_measurement_content(outer_cleared, mode="edit")


def test_clear_statistic_covers_scalar_and_interval_forms(enhanced):
    no_mean = clear_statistic(enhanced, "length", "mean")
    assert no_mean.details.metrics["length"].mean_interval is None and no_mean.length_mean is None
    with_scalar = set_scalar_mean(no_mean, "length", 11.0)
    assert clear_statistic(with_scalar, "length", "mean").length_mean is None
    no_median = clear_statistic(enhanced, "q", "median")
    assert no_median.details.metrics["q"].median is None
    no_sd = clear_statistic(enhanced, "width", "sd")
    assert no_sd.details.metrics["width"].sd is None
    for content in (no_mean, with_scalar, no_median, no_sd):
        validate_measurement_content(content, mode="edit")


def test_clearing_everything_normalizes_details_to_none():
    content = MeasurementContent(
        q_core_min=1.4, q_core_max=1.8,
        details=MeasurementDetails(metrics={"q": MetricDetails(core_range=RangeDescriptor("unspecified"))}),
    )
    cleared = clear_pair(content, "q", "core")
    assert cleared.details is None
    assert content_row_updates(cleared)["measurement_details_json"] is None
    validate_measurement_content(cleared, mode="edit")


def test_switching_between_scalar_mean_and_mean_interval(enhanced):
    scalar = set_scalar_mean(clear_statistic(enhanced, "width", "mean"), "width", 6.2)
    assert scalar.width_mean == 6.2 and scalar.details.metrics["width"].mean_interval is None
    validate_measurement_content(scalar, mode="edit")

    interval = set_mean_interval(scalar, "width", 5.9, 6.4, "typical_range")
    assert interval.width_mean is None
    assert interval.details.metrics["width"].mean_interval == IntervalStatistic(5.9, 6.4, "typical_range")
    validate_measurement_content(interval, mode="edit")

    # Switching directly from interval to scalar also removes the interval.
    direct = set_scalar_mean(enhanced, "length", 11.0)
    assert direct.length_mean == 11.0 and direct.details.metrics["length"].mean_interval is None
    validate_measurement_content(direct, mode="edit")

    # Equal endpoints stay an interval; inverted or non-positive input raises.
    equal = set_mean_interval(scalar, "width", 6.2, 6.2, "reported_range")
    assert equal.details.metrics["width"].mean_interval == IntervalStatistic(6.2, 6.2, "reported_range")
    for args in ((6.4, 5.9, "reported_range"), (0, 6.4, "reported_range"), (5.9, 6.4, "percentile_interval")):
        with pytest.raises(MeasurementContentError):
            set_mean_interval(scalar, "width", *args)
    for value in (0, -1, math.nan, True):
        with pytest.raises(MeasurementContentError):
            set_scalar_mean(scalar, "width", value)


def test_edit_operations_start_from_legacy_content(legacy_row):
    legacy = content_from_row(legacy_row)
    tagged = set_mean_interval(legacy, "length", 9.5, 11.0, "reported_range")
    assert tagged.details == MeasurementDetails(
        metrics={"length": MetricDetails(mean_interval=IntervalStatistic(9.5, 11.0, "reported_range"))}
    )
    validate_measurement_content(tagged, mode="edit")
    # Contract section 2 rule 7: adding statistics never tags the historic Q pair.
    assert "q" not in tagged.details.metrics
    assert tagged.q_min == 1.7 and tagged.q_max == 2.5


def test_swap_length_width_moves_numbers_and_interpretation_together(enhanced):
    swapped = swap_length_width(enhanced)
    for l_col, w_col in (
        ("length_min", "width_min"), ("length_core_min", "width_core_min"),
        ("length_core_max", "width_core_max"), ("length_max", "width_max"),
        ("length_mean", "width_mean"),
    ):
        assert getattr(swapped, l_col) == getattr(enhanced, w_col)
        assert getattr(swapped, w_col) == getattr(enhanced, l_col)
    assert swapped.details.metrics["length"] == enhanced.details.metrics["width"]
    assert swapped.details.metrics["width"] == enhanced.details.metrics["length"]
    assert swapped.details.metrics["q"] == enhanced.details.metrics["q"]
    assert (swapped.q_min, swapped.q_core_min, swapped.q_core_max, swapped.q_max, swapped.q_mean) == (
        enhanced.q_min, enhanced.q_core_min, enhanced.q_core_max, enhanced.q_max, enhanced.q_mean
    )
    validate_measurement_content(swapped, mode="edit")
    assert swap_length_width(swapped) == enhanced


def test_swap_with_one_sided_details_and_scalar_mean():
    content = MeasurementContent(
        length_core_min=8.0, length_core_max=12.0, length_mean=10.0,
        width_core_min=4.0, width_core_max=6.0,
        details=MeasurementDetails(metrics={"length": MetricDetails(core_range=RangeDescriptor("unspecified"))}),
    )
    swapped = swap_length_width(content)
    assert swapped.width_mean == 10.0 and swapped.length_mean is None
    assert (swapped.width_core_min, swapped.width_core_max) == (8.0, 12.0)
    assert (swapped.length_core_min, swapped.length_core_max) == (4.0, 6.0)
    assert set(swapped.details.metrics) == {"width"}
    validate_measurement_content(swapped, mode="edit")
    assert swap_length_width(MeasurementContent()).details is None


def test_edit_operations_reject_unknown_targets(enhanced):
    with pytest.raises(MeasurementContentError):
        clear_pair(enhanced, "depth", "core")
    with pytest.raises(MeasurementContentError):
        clear_pair(enhanced, "length", "inner")  # type: ignore[arg-type]
    with pytest.raises(MeasurementContentError):
        clear_statistic(enhanced, "length", "cv")  # type: ignore[arg-type]


def test_edit_operations_do_not_validate_transported_state(enhanced):
    """Transitions are separate from validation: an edit on content that is
    currently invalid still applies, and validation reports the remaining
    problem afterwards."""
    broken = replace(enhanced, length_mean=11.0)  # invalid: scalar plus interval
    with pytest.raises(MeasurementContentError):
        validate_measurement_content(broken, mode="edit")
    fixed = clear_statistic(broken, "length", "mean")
    validate_measurement_content(fixed, mode="edit")
    still_broken = clear_statistic(broken, "length", "sd")  # unrelated edit applies
    assert still_broken.details.metrics["length"].sd is None
    with pytest.raises(MeasurementContentError, match="length_mean"):
        validate_measurement_content(still_broken, mode="edit")
