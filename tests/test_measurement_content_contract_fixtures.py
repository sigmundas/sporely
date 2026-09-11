"""Stage 1 contract fixtures: internal consistency with
``docs/reference-data/measurement-content-contract.md``.

These tests pin the frozen contract to executable checks so the JSON fixtures
under ``tests/fixtures/reference_statistics/`` cannot drift from the prose.
They deliberately use no production code beyond the existing snapshot
serializer: the validation rules are restated here as the specification that
Stage 2 (``references/measurement_content.py``) must implement. When Stage 2
lands, these helpers should be replaced by the real module and the tests
kept.
"""
from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pytest

from database.reference_citation import serialize_snapshot

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"
CONTRACT_DOC = Path(__file__).parents[1] / "docs" / "reference-data" / "measurement-content-contract.md"
_SPEC_BLOCK = re.compile(r"```json contract-spec\n(.*?)\n```", re.DOTALL)


def load_contract_spec() -> dict:
    """The machine-readable block in contract section 14."""
    text = CONTRACT_DOC.read_text(encoding="utf-8")
    blocks = _SPEC_BLOCK.findall(text)
    assert len(blocks) == 1, "exactly one contract-spec block is expected"
    return json.loads(blocks[0])

# --- Contract constants (must match the contract document) -------------------

MEASUREMENT_DETAILS_SCHEMA_VERSION = 1
SUPPORTED_DETAILS_VERSIONS = frozenset({1})
SUPPORTED_SNAPSHOT_VERSIONS = frozenset({1, 2})
MEASUREMENT_DETAILS_MAX_BYTES = 4096

METRICS = ("length", "width", "q")
METRIC_KEYS = frozenset({"outer_range", "core_range", "mean_interval", "median", "sd"})
OUTER_RANGE_KINDS = frozenset({"reported_extremes"})
CORE_RANGE_KINDS = frozenset(
    {"unspecified", "typical_range", "reported_range", "percentile_interval"}
)
INTERVAL_KINDS = frozenset({"reported_range", "typical_range"})

EXTENSION_FIELDS = ("measurement_details_json", "q_core_min", "q_core_max")

# Numeric column pairs each descriptor describes, per metric.
OUTER_PAIR = {
    "length": ("length_min", "length_max"),
    "width": ("width_min", "width_max"),
    "q": ("q_min", "q_max"),
}
CORE_PAIR = {
    "length": ("length_core_min", "length_core_max"),
    "width": ("width_core_min", "width_core_max"),
    "q": ("q_core_min", "q_core_max"),
}
SCALAR_MEAN = {"length": "length_mean", "width": "width_mean", "q": "q_mean"}

SCIENTIFIC_CONTENT_FIELDS = frozenset({
    "character", "data_kind", "raw_text",
    "length_min", "length_core_min", "length_core_max", "length_max",
    "width_min", "width_core_min", "width_core_max", "width_max",
    "q_min", "q_core_min", "q_core_max", "q_max",
    "q_mean", "length_mean", "width_mean",
    "sample_size", "specimen_count",
    "mount_medium", "stain", "preparation", "measurement_method",
    "raw_points_json", "measurement_details_json",
})

SNAPSHOT_V1_KEYS = frozenset({
    "schema_version", "reference_work_id", "reference_treatment_id",
    "reference_measurement_set_id", "reference_revision", "short_label",
    "full_citation", "work_type", "year", "doi", "isbn", "taxon_id",
    "name_as_published", "locator_text", "page_from", "page_to", "character",
    "data_kind", "raw_text", "measurements", "method", "raw_points",
})
SNAPSHOT_V2_KEYS = SNAPSHOT_V1_KEYS | {"measurement_details"}
MEASUREMENT_V1_KEYS = frozenset({
    "length_min", "length_core_min", "length_core_max", "length_max",
    "width_min", "width_core_min", "width_core_max", "width_max", "q_min",
    "q_max", "q_mean", "length_mean", "width_mean", "sample_size",
    "specimen_count",
})
MEASUREMENT_V2_KEYS = MEASUREMENT_V1_KEYS | {"q_core_min", "q_core_max"}
METHOD_KEYS = frozenset({"mount_medium", "stain", "preparation", "measurement_method"})


# --- Specification helpers (Stage 2 implements these for real) ---------------


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def encode_details(obj: dict) -> str:
    """The single contract codec: compact, sorted, ASCII-safe JSON."""
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _positive(value) -> bool:
    return _is_number(value) and value > 0


def _interval(obj: dict, *, positive: bool = True) -> None:
    assert set(obj) == {"lower", "upper", "kind"}, obj
    assert obj["kind"] in INTERVAL_KINDS
    check = _positive if positive else _is_number
    assert check(obj["lower"]) and check(obj["upper"])
    assert obj["lower"] <= obj["upper"], "equal endpoints are preserved, inverted ones rejected"


def validate_details_v1(details: dict, row: dict) -> None:
    """Assert ``details`` is a valid v1 object for ``row`` (contract §2, §3)."""
    assert set(details) == {"schema_version", "metrics"}
    assert details["schema_version"] == MEASUREMENT_DETAILS_SCHEMA_VERSION
    metrics = details["metrics"]
    assert isinstance(metrics, dict) and metrics, "semantically empty objects normalize to NULL"
    assert set(metrics) <= set(METRICS)
    for metric, body in metrics.items():
        assert isinstance(body, dict) and body, f"empty metric object {metric!r}"
        assert set(body) <= METRIC_KEYS, body
        if "outer_range" in body:
            assert set(body["outer_range"]) == {"kind"}
            assert body["outer_range"]["kind"] in OUTER_RANGE_KINDS
            lo, hi = (row[c] for c in OUTER_PAIR[metric])
            assert _positive(lo) and _positive(hi) and lo <= hi, "outer descriptor needs a complete pair"
        if "core_range" in body:
            core = body["core_range"]
            assert core["kind"] in CORE_RANGE_KINDS
            lo, hi = (row[c] for c in CORE_PAIR[metric])
            assert _positive(lo) and _positive(hi) and lo <= hi, "core descriptor needs a complete pair"
            if core["kind"] == "percentile_interval":
                assert set(core) == {"kind", "percentile_bounds"}
                p_lo, p_hi = core["percentile_bounds"]
                assert _is_number(p_lo) and _is_number(p_hi)
                assert 0 <= p_lo < p_hi <= 100
            else:
                assert set(core) == {"kind"}, "percentile bounds only for percentile kind"
            if "outer_range" in body:
                o_lo, o_hi = (row[c] for c in OUTER_PAIR[metric])
                assert o_lo <= lo and hi <= o_hi, "explicit extremes must enclose the core pair"
        if "mean_interval" in body:
            _interval(body["mean_interval"])
            assert row[SCALAR_MEAN[metric]] is None, "mean interval excludes the scalar mean"
        if "median" in body:
            median = body["median"]
            if set(median) == {"value"}:
                assert _positive(median["value"])
            else:
                _interval(median)
        if "sd" in body:
            assert set(body["sd"]) == {"value"}
            assert _is_number(body["sd"]["value"]) and body["sd"]["value"] >= 0


def is_enhanced(row: dict) -> bool:
    return any(row.get(field) is not None for field in EXTENSION_FIELDS)


def acknowledgement_state(row: dict) -> str:
    present = sum(field in row for field in EXTENSION_FIELDS)
    if present == 0:
        return "absent"
    if present == len(EXTENSION_FIELDS):
        return "complete"
    return "partial"


def acknowledges_extension(row: dict) -> bool:
    return acknowledgement_state(row) == "complete"


def import_decision(incoming: dict, destination: dict | None) -> str:
    """Contract §8 policy for a row arriving by bundle/portable import."""
    if acknowledgement_state(incoming) == "partial":
        return "reject_partial_extension"
    if destination is None:
        return "replace"
    src_rev = int(incoming.get("revision") or 1)
    dst_rev = int(destination.get("revision") or 1)
    if src_rev < dst_rev:
        return "skip_stale"
    acknowledged = acknowledges_extension(incoming)
    if src_rev == dst_rev:
        if not acknowledged:
            return "skip_unacknowledged" if is_enhanced(destination) else "equivalent_if_content_equal"
        return "equivalent_if_content_equal"
    if not acknowledged:
        return "reject_unacknowledged_extension" if is_enhanced(destination) else "replace"
    return "replace"


def snapshot_semantic_projection(snapshot: dict):
    """Contract §5 version-aware projection; ``None`` for unsupported versions."""
    version = snapshot.get("schema_version")
    if version not in SUPPORTED_SNAPSHOT_VERSIONS:
        return None
    measurements = dict(snapshot["measurements"])
    measurements.setdefault("q_core_min", None)
    measurements.setdefault("q_core_max", None)
    projected = {
        key: value
        for key, value in snapshot.items()
        if key not in {"schema_version", "reference_revision", "measurements", "measurement_details"}
    }
    projected["measurements"] = measurements
    projected["measurement_details"] = snapshot.get("measurement_details")
    return projected


# --- Fixture tests -----------------------------------------------------------


@pytest.fixture(scope="module")
def hebeloma():
    return load("details_hebeloma_v1.json")


@pytest.fixture(scope="module")
def enhanced_row():
    return load("row_enhanced.json")


@pytest.fixture(scope="module")
def legacy_row():
    return load("row_legacy_only.json")


@pytest.fixture(scope="module")
def snapshot_v1():
    return load("snapshot_v1_legacy_only.json")


@pytest.fixture(scope="module")
def snapshot_v2():
    return load("snapshot_v2_enhanced.json")


def test_fixture_set_is_complete():
    expected = {
        "details_hebeloma_v1.json",
        "details_unsupported_future_version.json",
        "row_legacy_only.json",
        "row_enhanced.json",
        "snapshot_v1_legacy_only.json",
        "snapshot_v2_enhanced.json",
        "import_row_omitting_extension.json",
        "import_row_explicit_null_extension.json",
        "import_row_partial_extension.json",
    }
    assert {path.name for path in FIXTURES.glob("*.json")} == expected


def test_contract_document_spec_matches_these_constants():
    """Executable linkage: the prose contract's machine-readable block and the
    constants restated here must agree, so editing either alone fails."""
    spec = load_contract_spec()
    assert spec["details_schema_version"] == MEASUREMENT_DETAILS_SCHEMA_VERSION
    assert set(spec["supported_details_versions"]) == SUPPORTED_DETAILS_VERSIONS
    assert set(spec["supported_snapshot_versions"]) == SUPPORTED_SNAPSHOT_VERSIONS
    assert spec["details_max_bytes"] == MEASUREMENT_DETAILS_MAX_BYTES
    assert spec["snapshot_max_bytes"] == 65536
    assert tuple(spec["metrics"]) == METRICS
    assert set(spec["metric_keys"]) == METRIC_KEYS
    assert set(spec["outer_range_kinds"]) == OUTER_RANGE_KINDS
    assert set(spec["core_range_kinds"]) == CORE_RANGE_KINDS
    assert set(spec["interval_kinds"]) == INTERVAL_KINDS
    assert tuple(spec["extension_fields"]) == EXTENSION_FIELDS
    assert set(spec["scientific_content_fields"]) == SCIENTIFIC_CONTENT_FIELDS
    assert len(spec["scientific_content_fields"]) == len(SCIENTIFIC_CONTENT_FIELDS)
    assert set(spec["snapshot_v2_added_keys"]) == SNAPSHOT_V2_KEYS - SNAPSHOT_V1_KEYS
    assert set(spec["measurements_v2_added_keys"]) == MEASUREMENT_V2_KEYS - MEASUREMENT_V1_KEYS
    assert set(spec["acknowledgement_states"]) == {"absent", "complete", "partial"}
    decisions = {
        "reject_partial_extension", "skip_stale", "skip_unacknowledged",
        "equivalent_if_content_equal", "reject_unacknowledged_extension", "replace",
    }
    assert set(spec["import_decisions"]) == decisions
    assert set(spec["validation_modes"]) == {"edit", "authoritative"}
    assert spec["cloud_rejection_status"] == "invalid_payload"


def test_contract_prose_names_every_enum_value_and_field():
    text = CONTRACT_DOC.read_text(encoding="utf-8")
    for value in OUTER_RANGE_KINDS | CORE_RANGE_KINDS | INTERVAL_KINDS:
        assert f"`{value}`" in text, f"enum value {value!r} missing from the contract prose"
    for field in SCIENTIFIC_CONTENT_FIELDS | set(EXTENSION_FIELDS):
        assert field in text, f"field {field!r} missing from the contract"
    for outcome in ("rejected_partial_extension", "rejected_unacknowledged_extension", "skipped_unacknowledged"):
        assert f"`{outcome}`" in text
    assert 'mode="edit"' in text and 'mode="authoritative"' in text


def test_hebeloma_details_match_the_plan_example(hebeloma, enhanced_row):
    validate_details_v1(hebeloma, enhanced_row)
    length, width, q = (hebeloma["metrics"][m] for m in METRICS)
    assert length["core_range"] == {"kind": "percentile_interval", "percentile_bounds": [5, 95]}
    assert (length["mean_interval"]["lower"], length["mean_interval"]["upper"]) == (8.9, 13.7)
    assert (length["median"]["lower"], length["median"]["upper"]) == (9.0, 13.9)
    assert length["sd"] == {"value": 0.696}
    assert (width["mean_interval"]["lower"], width["mean_interval"]["upper"]) == (5.6, 7.5)
    assert (width["median"]["lower"], width["median"]["upper"]) == (5.6, 7.5)
    assert width["sd"] == {"value": 0.323}
    assert (q["mean_interval"]["lower"], q["mean_interval"]["upper"]) == (1.51, 1.96)
    assert (q["median"]["lower"], q["median"]["upper"]) == (1.50, 1.95)
    assert q["sd"] == {"value": 0.097}
    # Width mean and median coincide: equal-looking intervals stay separate statistics.
    assert width["mean_interval"] != width["median"] or width["mean_interval"]["kind"] == width["median"]["kind"]


def test_enhanced_row_numeric_columns_match_the_plan_example(enhanced_row):
    assert (enhanced_row["length_min"], enhanced_row["length_core_min"],
            enhanced_row["length_core_max"], enhanced_row["length_max"]) == (6.9, 8.0, 15.2, 16.1)
    assert (enhanced_row["width_min"], enhanced_row["width_core_min"],
            enhanced_row["width_core_max"], enhanced_row["width_max"]) == (4.1, 5.1, 8.2, 8.9)
    assert (enhanced_row["q_min"], enhanced_row["q_core_min"],
            enhanced_row["q_core_max"], enhanced_row["q_max"]) == (1.17, 1.36, 2.19, 2.79)
    # No scalar mean may coexist with a mean interval; no inferred means anywhere.
    assert enhanced_row["length_mean"] is None
    assert enhanced_row["width_mean"] is None
    assert enhanced_row["q_mean"] is None


def test_enhanced_row_stores_the_canonical_codec_form(enhanced_row, hebeloma):
    stored = enhanced_row["measurement_details_json"]
    assert json.loads(stored) == hebeloma, "decoded-object equality is the comparison rule"
    assert stored == encode_details(hebeloma), "stored text is the canonical compact sorted encoding"
    assert len(stored.encode("utf-8")) <= MEASUREMENT_DETAILS_MAX_BYTES
    assert is_enhanced(enhanced_row) and acknowledges_extension(enhanced_row)


def test_legacy_only_row_has_null_extension_and_unknown_q_role(legacy_row):
    assert acknowledges_extension(legacy_row)
    assert not is_enhanced(legacy_row)
    assert all(legacy_row[field] is None for field in EXTENSION_FIELDS)
    # Historical q_min/q_max stay as reported bounds of unknown role: no descriptor.
    assert legacy_row["q_min"] == 1.7 and legacy_row["q_max"] == 2.5


def test_rows_share_the_full_column_set(enhanced_row, legacy_row):
    assert set(enhanced_row) == set(legacy_row)
    assert SCIENTIFIC_CONTENT_FIELDS <= set(enhanced_row)
    assert {"id", "taxon_treatment_id", "supersedes_id", "notes", "revision",
            "legacy_reference_value_id"}.isdisjoint(SCIENTIFIC_CONTENT_FIELDS)
    assert len(SCIENTIFIC_CONTENT_FIELDS) == 26


def test_unsupported_future_details_version_is_not_v1():
    future = load("details_unsupported_future_version.json")
    assert future["schema_version"] not in SUPPORTED_DETAILS_VERSIONS
    assert future["schema_version"] > MEASUREMENT_DETAILS_SCHEMA_VERSION
    with pytest.raises(AssertionError):
        validate_details_v1(future, load("row_enhanced.json"))
    # Preserved opaque: the codec round-trips it byte-for-byte after decoding.
    assert json.loads(encode_details(future)) == future


def test_snapshot_v1_shape_and_correspondence(snapshot_v1, legacy_row):
    assert snapshot_v1["schema_version"] == 1
    assert set(snapshot_v1) == SNAPSHOT_V1_KEYS
    assert set(snapshot_v1["measurements"]) == MEASUREMENT_V1_KEYS
    assert set(snapshot_v1["method"]) == METHOD_KEYS
    assert snapshot_v1["reference_measurement_set_id"] == legacy_row["id"]
    assert snapshot_v1["reference_treatment_id"] == legacy_row["taxon_treatment_id"]
    assert snapshot_v1["reference_revision"] == legacy_row["revision"]
    for key, value in snapshot_v1["measurements"].items():
        assert value == legacy_row[key]
    for key, value in snapshot_v1["method"].items():
        assert value == legacy_row[key]
    assert snapshot_v1["raw_text"] == legacy_row["raw_text"]
    assert snapshot_v1["data_kind"] == legacy_row["data_kind"]
    assert "measurement_details" not in snapshot_v1
    assert len(serialize_snapshot(snapshot_v1).encode("utf-8")) <= 65536


def test_snapshot_v2_shape_and_correspondence(snapshot_v2, enhanced_row, hebeloma):
    assert snapshot_v2["schema_version"] == 2
    assert set(snapshot_v2) == SNAPSHOT_V2_KEYS
    assert set(snapshot_v2["measurements"]) == MEASUREMENT_V2_KEYS
    assert set(snapshot_v2["method"]) == METHOD_KEYS
    assert snapshot_v2["reference_measurement_set_id"] == enhanced_row["id"]
    assert snapshot_v2["reference_revision"] == enhanced_row["revision"]
    for key, value in snapshot_v2["measurements"].items():
        assert value == enhanced_row[key], key
    assert snapshot_v2["measurement_details"] == hebeloma
    assert snapshot_v2["measurement_details"] == json.loads(enhanced_row["measurement_details_json"])
    # The details object keeps its own version, independent of the snapshot version.
    assert snapshot_v2["measurement_details"]["schema_version"] == 1
    assert len(serialize_snapshot(snapshot_v2).encode("utf-8")) <= 65536
    assert len(encode_details(snapshot_v2["measurement_details"]).encode("utf-8")) <= MEASUREMENT_DETAILS_MAX_BYTES


def test_v1_and_v2_share_everything_except_the_extension(snapshot_v1, snapshot_v2):
    assert SNAPSHOT_V2_KEYS - SNAPSHOT_V1_KEYS == {"measurement_details"}
    assert MEASUREMENT_V2_KEYS - MEASUREMENT_V1_KEYS == {"q_core_min", "q_core_max"}
    # Both fixtures cite the same work and treatment: only the set differs.
    for key in ("reference_work_id", "reference_treatment_id", "short_label",
                "full_citation", "work_type", "year", "name_as_published"):
        assert snapshot_v1[key] == snapshot_v2[key]


def test_semantic_projection_rules(snapshot_v1, snapshot_v2):
    v1_projection = snapshot_semantic_projection(snapshot_v1)
    # A v2 rendering of the same legacy-only content: no extension anywhere.
    v2_of_legacy = dict(snapshot_v1)
    v2_of_legacy["schema_version"] = 2
    v2_of_legacy["reference_revision"] = 7  # revision churn is ignored
    v2_of_legacy["measurements"] = {**snapshot_v1["measurements"], "q_core_min": None, "q_core_max": None}
    v2_of_legacy["measurement_details"] = None
    assert snapshot_semantic_projection(v2_of_legacy) == v1_projection, "missing extension == no extension"

    assert snapshot_semantic_projection(snapshot_v2) != v1_projection, "real statistics differ"
    stripped_v2 = dict(snapshot_v2)
    stripped_v2["measurement_details"] = None
    stripped_v2["measurements"] = {**snapshot_v2["measurements"], "q_core_min": None, "q_core_max": None}
    assert snapshot_semantic_projection(stripped_v2) != snapshot_semantic_projection(snapshot_v2)

    future = dict(snapshot_v2)
    future["schema_version"] = 3
    assert snapshot_semantic_projection(future) is None, "unsupported versions never project as v1"
    assert snapshot_semantic_projection(future) != v1_projection


def test_import_fixture_omitting_extension_is_unacknowledged(enhanced_row):
    omitting = load("import_row_omitting_extension.json")
    assert not acknowledges_extension(omitting)
    assert not any(field in omitting for field in EXTENSION_FIELDS)
    assert omitting["id"] == enhanced_row["id"]
    assert omitting["revision"] > enhanced_row["revision"]
    assert omitting["length_core_max"] != enhanced_row["length_core_max"], "it also changes a bound"
    assert import_decision(omitting, enhanced_row) == "reject_unacknowledged_extension"
    same_revision = {**omitting, "revision": enhanced_row["revision"]}
    assert import_decision(same_revision, enhanced_row) == "skip_unacknowledged"
    legacy_destination = load("row_legacy_only.json")
    assert import_decision(omitting, legacy_destination) == "replace", "nothing to lose on a legacy row"


def test_import_fixture_with_explicit_nulls_is_an_acknowledged_clear(enhanced_row):
    explicit = load("import_row_explicit_null_extension.json")
    omitting = load("import_row_omitting_extension.json")
    assert acknowledges_extension(explicit)
    assert all(explicit[field] is None for field in EXTENSION_FIELDS)
    assert {k: v for k, v in explicit.items() if k not in EXTENSION_FIELDS} == omitting, (
        "the two import fixtures differ only in extension key presence"
    )
    assert import_decision(explicit, enhanced_row) == "replace"
    assert not is_enhanced(explicit)


def test_import_fixture_with_partial_extension_is_rejected_everywhere(enhanced_row, legacy_row):
    partial = load("import_row_partial_extension.json")
    omitting = load("import_row_omitting_extension.json")
    assert acknowledgement_state(partial) == "partial"
    assert not acknowledges_extension(partial)
    assert "measurement_details_json" not in partial
    assert partial["q_core_min"] == 1.36 and partial["q_core_max"] == 2.19
    assert {k: v for k, v in partial.items() if k not in EXTENSION_FIELDS} == omitting
    # Rejected regardless of destination state, revision, or absence of a destination.
    assert import_decision(partial, enhanced_row) == "reject_partial_extension"
    assert import_decision(partial, legacy_row) == "reject_partial_extension"
    assert import_decision(partial, None) == "reject_partial_extension"
    assert import_decision({**partial, "revision": 1}, enhanced_row) == "reject_partial_extension"
    # Whereas the same content with all keys present would simply be inserted.
    assert import_decision(load("import_row_explicit_null_extension.json"), None) == "replace"


def test_acknowledgement_states_are_exhaustive(enhanced_row, legacy_row):
    assert acknowledgement_state(enhanced_row) == "complete"
    assert acknowledgement_state(legacy_row) == "complete"
    assert acknowledgement_state(load("import_row_omitting_extension.json")) == "absent"
    assert acknowledgement_state(load("import_row_explicit_null_extension.json")) == "complete"
    assert acknowledgement_state(load("import_row_partial_extension.json")) == "partial"


def test_enum_values_are_closed_sets():
    assert OUTER_RANGE_KINDS == {"reported_extremes"}
    assert CORE_RANGE_KINDS == {"unspecified", "typical_range", "reported_range", "percentile_interval"}
    assert INTERVAL_KINDS == {"reported_range", "typical_range"}
    assert INTERVAL_KINDS < CORE_RANGE_KINDS
    assert METRICS == ("length", "width", "q")


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
    ],
    ids=[
        "unknown-key-basis", "inverted-percentiles", "bounds-without-percentile-kind",
        "inverted-interval", "negative-sd", "boolean-number", "median-value-and-interval",
        "unknown-metric", "empty-metric", "wrong-version",
    ],
)
def test_invalid_mutations_of_the_example_are_rejected(mutation, hebeloma, enhanced_row):
    mutated = json.loads(json.dumps(hebeloma))
    mutation(mutated)
    with pytest.raises(AssertionError):
        validate_details_v1(mutated, enhanced_row)


def test_mean_interval_and_scalar_mean_are_exclusive(hebeloma, enhanced_row):
    with_scalar = {**enhanced_row, "length_mean": 11.0}
    with pytest.raises(AssertionError):
        validate_details_v1(hebeloma, with_scalar)


def test_descriptor_without_complete_pair_is_rejected(hebeloma, enhanced_row):
    missing_core = {**enhanced_row, "q_core_max": None}
    with pytest.raises(AssertionError):
        validate_details_v1(hebeloma, missing_core)
    missing_outer = {**enhanced_row, "length_max": None}
    with pytest.raises(AssertionError):
        validate_details_v1(hebeloma, missing_outer)
