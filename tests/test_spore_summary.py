"""Tests for utils.spore_summary (Stage C — structured observation-level
spore summary computation)."""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from utils.spore_summary import (
    CONTEXT_KEYS,
    SPORE_SUMMARY_SOURCE_APP,
    SPORE_SUMMARY_STATS_VERSION,
    build_context,
    compute_context_hash,
    compute_observation_spore_summaries,
    normalize_context_value,
    serialize_context,
)


FIXED_COMPUTED_AT = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)


def _paired(length, width, measurement_type="manual", **image_ctx):
    row = {"length_um": length, "width_um": width, "measurement_type": measurement_type}
    row.update(image_ctx)
    return row


# ---------------------------------------------------------------------------
# Context normalization / hashing
# ---------------------------------------------------------------------------


def test_normalize_context_value_lowercases_and_trims():
    assert normalize_context_value("  KOH ") == "koh"


def test_normalize_context_value_collapses_internal_whitespace():
    assert normalize_context_value("congo   red") == "congo red"
    assert normalize_context_value("congo\tred") == "congo red"


def test_normalize_context_value_empty_becomes_none():
    assert normalize_context_value("") is None
    assert normalize_context_value("   ") is None
    assert normalize_context_value(None) is None


def test_build_context_defaults_measurement_type_to_spore():
    ctx = build_context()
    assert ctx["measurement_type"] == "spore"
    assert all(ctx[k] is None for k in CONTEXT_KEYS if k != "measurement_type")


def test_build_context_preserves_fixed_key_order():
    ctx = build_context(contrast_method="DIC", sample_type="fresh")
    assert list(ctx.keys()) == list(CONTEXT_KEYS)


def test_serialize_context_uses_fixed_key_order_not_alphabetical():
    """The wire contract for context_hash is the fixed CONTEXT_KEYS order:
    measurement_type, sample_type, mount_reagent, stain_reagent,
    contrast_method — NOT alphabetical sort_keys order (which would put
    contrast_method first)."""
    ctx = build_context(mount_reagent="water", contrast_method="dic")
    encoded = serialize_context(ctx)
    assert encoded == (
        '{"measurement_type":"spore","sample_type":null,'
        '"mount_reagent":"water","stain_reagent":null,"contrast_method":"dic"}'
    )
    # And the order in the encoded string matches CONTEXT_KEYS exactly.
    positions = [encoded.index(f'"{key}"') for key in CONTEXT_KEYS]
    assert positions == sorted(positions)


def test_serialize_context_ignores_caller_dict_order():
    """Passing keys in any order must produce the same canonical string."""
    scrambled = {
        "contrast_method": "dic",
        "measurement_type": "spore",
        "stain_reagent": None,
        "mount_reagent": "water",
        "sample_type": None,
    }
    assert (
        serialize_context(scrambled)
        == serialize_context(build_context(mount_reagent="water", contrast_method="dic"))
    )


def test_context_hash_is_stable_across_whitespace_and_case():
    a = compute_context_hash(build_context(mount_reagent="  KOH ", contrast_method="DIC"))
    b = compute_context_hash(build_context(mount_reagent="koh", contrast_method="dic"))
    assert a == b


def test_context_hash_changes_when_a_real_field_changes():
    a = compute_context_hash(build_context(mount_reagent="koh"))
    b = compute_context_hash(build_context(mount_reagent="water"))
    assert a != b


# ---------------------------------------------------------------------------
# Grouping by context
# ---------------------------------------------------------------------------


def test_two_contexts_produce_two_summary_rows():
    measurements = [
        _paired(10.0, 5.0, mount_medium="KOH", contrast="DIC"),
        _paired(10.2, 5.1, mount_medium="KOH", contrast="DIC"),
        _paired(11.0, 5.5, mount_medium="Water", contrast="Brightfield"),
        _paired(11.4, 5.7, mount_medium="Water", contrast="Brightfield"),
    ]
    summaries = compute_observation_spore_summaries(
        observation_id=1,
        measurements=measurements,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert len(summaries) == 2
    contexts = {(s["mount_reagent"], s["contrast_method"]) for s in summaries}
    assert contexts == {("koh", "dic"), ("water", "brightfield")}


def test_missing_context_produces_null_context_row():
    measurements = [_paired(9.0, 4.5), _paired(9.4, 4.7)]
    summaries = compute_observation_spore_summaries(
        observation_id=2,
        measurements=measurements,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert len(summaries) == 1
    s = summaries[0]
    assert s["measurement_type"] == "spore"
    assert s["sample_type"] is None
    assert s["mount_reagent"] is None
    assert s["stain_reagent"] is None
    assert s["contrast_method"] is None
    # And its context_hash is exactly the "all-null except measurement_type" hash
    assert s["context_hash"] == compute_context_hash(build_context())


def test_summary_field_names_win_over_image_column_names():
    """When both `mount_medium` and `mount_reagent` are present, the
    summary-facing name should be used. Same for stain/contrast."""
    row = _paired(
        10.0, 5.0,
        mount_medium="KOH", mount_reagent="Water",
        stain="Congo Red", stain_reagent="Melzer",
        contrast="BF", contrast_method="DIC",
    )
    summaries = compute_observation_spore_summaries(
        observation_id=3, measurements=[row], computed_at=FIXED_COMPUTED_AT
    )
    assert len(summaries) == 1
    s = summaries[0]
    assert s["mount_reagent"] == "water"
    assert s["stain_reagent"] == "melzer"
    assert s["contrast_method"] == "dic"


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def test_real_lm_wm_qm_from_paired_measurements():
    pairs = [(10.0, 5.0), (11.0, 5.5), (12.0, 6.0), (13.0, 6.5), (14.0, 7.0)]
    measurements = [_paired(l, w) for l, w in pairs]
    s = compute_observation_spore_summaries(
        observation_id=10, measurements=measurements, computed_at=FIXED_COMPUTED_AT
    )[0]
    # Arithmetic means
    assert s["length_mean_um"] == pytest.approx(12.0)
    assert s["width_mean_um"] == pytest.approx(6.0)
    # q_mean is mean of individual q_i; since every pair is 2.0 here, so is q_mean
    assert s["q_mean"] == pytest.approx(2.0)
    # Sanity: min/max/percentiles are computed
    assert s["length_min_um"] == pytest.approx(10.0)
    assert s["length_max_um"] == pytest.approx(14.0)
    assert s["length_median_um"] == pytest.approx(12.0)
    assert s["length_p05_um"] == pytest.approx(10.2)
    assert s["length_p95_um"] == pytest.approx(13.8)


def test_q_mean_is_mean_of_ratios_not_ratio_of_means():
    # Handpicked pairs where mean(l)/mean(w) != mean(l_i / w_i).
    pairs = [(10.0, 2.0), (10.0, 5.0)]
    measurements = [_paired(l, w) for l, w in pairs]
    s = compute_observation_spore_summaries(
        observation_id=11, measurements=measurements, computed_at=FIXED_COMPUTED_AT
    )[0]
    ratio_of_means = s["length_mean_um"] / s["width_mean_um"]  # 10 / 3.5 ≈ 2.857
    mean_of_ratios = (10.0 / 2.0 + 10.0 / 5.0) / 2.0            # (5 + 2) / 2 = 3.5
    assert s["q_mean"] == pytest.approx(mean_of_ratios)
    assert not math.isclose(s["q_mean"], ratio_of_means)


def test_sample_sd_is_none_for_single_value_and_ddof1_for_more():
    # Single measurement -> sd is None everywhere the count is 1.
    single = compute_observation_spore_summaries(
        observation_id=20,
        measurements=[_paired(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )[0]
    assert single["length_sd_um"] is None
    assert single["width_sd_um"] is None
    assert single["q_sd"] is None

    # Two measurements -> sample SD (ddof=1)
    pair = compute_observation_spore_summaries(
        observation_id=21,
        measurements=[_paired(10.0, 5.0), _paired(12.0, 6.0)],
        computed_at=FIXED_COMPUTED_AT,
    )[0]
    # length values [10, 12] -> sample SD = sqrt(2) ≈ 1.4142
    assert pair["length_sd_um"] == pytest.approx(math.sqrt(2.0))


def test_percentile_convention_is_numpy_linear_interpolation():
    # numpy.percentile default is linear interpolation. For [1..10]:
    #   p5  = 1.45,  p50 = 5.5,  p95 = 9.55
    lengths = list(range(1, 11))
    measurements = [_paired(float(l), float(l) / 2.0) for l in lengths]
    s = compute_observation_spore_summaries(
        observation_id=30, measurements=measurements, computed_at=FIXED_COMPUTED_AT
    )[0]
    assert s["length_p05_um"] == pytest.approx(1.45)
    assert s["length_median_um"] == pytest.approx(5.5)
    assert s["length_p95_um"] == pytest.approx(9.55)


# ---------------------------------------------------------------------------
# Eligibility / counts
# ---------------------------------------------------------------------------


def test_zero_negative_null_length_width_are_excluded_from_paired():
    rows = [
        _paired(10.0, 5.0),           # valid pair
        _paired(11.0, 0.0),           # zero width -> excluded from paired/width; length ok
        _paired(-1.0, 5.5),           # negative length -> length invalid; width valid
        _paired(12.0, None),          # width-missing -> length-only
        _paired(None, 6.0),           # length-missing -> width-only (but length_um NOT NULL in DB; still handle)
    ]
    s = compute_observation_spore_summaries(
        observation_id=40, measurements=rows, computed_at=FIXED_COMPUTED_AT
    )[0]
    assert s["n_spores"] == 5           # raw row count
    assert s["n_length"] == 3           # 10.0, 11.0, 12.0
    assert s["n_width"] == 3            # 5.0, 5.5, 6.0
    assert s["n_paired"] == 1           # only (10.0, 5.0)
    assert s["q_mean"] == pytest.approx(2.0)


def test_length_only_row_increases_n_length_but_not_length_mean():
    """A length-only measurement must count toward n_length for
    transparency, but must NOT shift the canonical length_mean_um — that
    mean is computed from paired rows only so Lm/Wm/Qm share a single
    denominator."""
    paired = [_paired(10.0, 5.0), _paired(12.0, 6.0)]
    baseline = compute_observation_spore_summaries(
        observation_id=42, measurements=paired, computed_at=FIXED_COMPUTED_AT
    )[0]

    with_length_only = compute_observation_spore_summaries(
        observation_id=42,
        measurements=paired + [_paired(20.0, None)],
        computed_at=FIXED_COMPUTED_AT,
    )[0]

    assert with_length_only["n_length"] == baseline["n_length"] + 1
    assert with_length_only["n_paired"] == baseline["n_paired"]
    assert with_length_only["n_width"] == baseline["n_width"]
    assert with_length_only["length_mean_um"] == pytest.approx(baseline["length_mean_um"])
    assert with_length_only["length_min_um"] == pytest.approx(baseline["length_min_um"])
    assert with_length_only["length_max_um"] == pytest.approx(baseline["length_max_um"])


def test_width_only_row_increases_n_width_but_not_width_mean():
    """Symmetric to the length-only case."""
    paired = [_paired(10.0, 5.0), _paired(12.0, 6.0)]
    baseline = compute_observation_spore_summaries(
        observation_id=43, measurements=paired, computed_at=FIXED_COMPUTED_AT
    )[0]

    with_width_only = compute_observation_spore_summaries(
        observation_id=43,
        measurements=paired + [_paired(0.0, 9.0)],  # length is zero → invalid
        computed_at=FIXED_COMPUTED_AT,
    )[0]

    assert with_width_only["n_width"] == baseline["n_width"] + 1
    assert with_width_only["n_paired"] == baseline["n_paired"]
    assert with_width_only["n_length"] == baseline["n_length"]
    assert with_width_only["width_mean_um"] == pytest.approx(baseline["width_mean_um"])
    assert with_width_only["width_min_um"] == pytest.approx(baseline["width_min_um"])
    assert with_width_only["width_max_um"] == pytest.approx(baseline["width_max_um"])


def test_lm_wm_qm_share_the_same_paired_denominator():
    """When a length-only row and a width-only row are added to two
    paired rows, the canonical length_mean_um / width_mean_um / q_mean
    must all still come from the two paired rows only."""
    rows = [
        _paired(10.0, 5.0),        # paired
        _paired(12.0, 6.0),        # paired
        _paired(50.0, None),       # length-only — must not touch length_mean_um
        _paired(None, 50.0),       # width-only — must not touch width_mean_um
    ]
    s = compute_observation_spore_summaries(
        observation_id=44, measurements=rows, computed_at=FIXED_COMPUTED_AT
    )[0]

    assert s["n_paired"] == 2
    assert s["n_length"] == 3
    assert s["n_width"] == 3
    assert s["length_mean_um"] == pytest.approx((10.0 + 12.0) / 2.0)
    assert s["width_mean_um"] == pytest.approx((5.0 + 6.0) / 2.0)
    # Same paired rows for q as well
    assert s["q_mean"] == pytest.approx(((10.0 / 5.0) + (12.0 / 6.0)) / 2.0)


def test_non_spore_measurement_type_rows_are_excluded():
    rows = [
        _paired(10.0, 5.0, measurement_type="basidium"),
        _paired(11.0, 5.5, measurement_type="cystidium"),
        _paired(12.0, 6.0, measurement_type="manual"),
        _paired(13.0, 6.5, measurement_type=None),
        _paired(14.0, 7.0, measurement_type="Spore"),
    ]
    summaries = compute_observation_spore_summaries(
        observation_id=41, measurements=rows, computed_at=FIXED_COMPUTED_AT
    )
    assert len(summaries) == 1
    s = summaries[0]
    assert s["n_spores"] == 3            # only manual + None + 'Spore'
    assert s["length_mean_um"] == pytest.approx((12.0 + 13.0 + 14.0) / 3.0)


# ---------------------------------------------------------------------------
# Payload contract
# ---------------------------------------------------------------------------


def test_payload_contract_fields_source_app_and_version():
    rows = [_paired(10.0, 5.0), _paired(12.0, 6.0)]
    s = compute_observation_spore_summaries(
        observation_id=50,
        measurements=rows,
        computed_at=FIXED_COMPUTED_AT,
        source_app_version="0.9.6",
    )[0]

    # Fixed identity / provenance fields
    assert s["observation_id"] == 50
    assert s["stats_version"] == SPORE_SUMMARY_STATS_VERSION == 1
    assert s["source_app"] == SPORE_SUMMARY_SOURCE_APP == "sporely-py"
    assert s["source_app_version"] == "0.9.6"
    assert s["computed_at"] == FIXED_COMPUTED_AT.isoformat()

    # context_json is the same normalized object used to derive context_hash
    assert list(s["context_json"].keys()) == list(CONTEXT_KEYS)
    assert compute_context_hash(s["context_json"]) == s["context_hash"]

    # All expected columns are present (Stage B contract minus db-generated
    # id / created_at / updated_at / user_id).
    expected = {
        "observation_id", "context_hash", "context_json",
        "measurement_type", "sample_type", "mount_reagent",
        "stain_reagent", "contrast_method",
        "n_spores", "n_paired", "n_length", "n_width",
        "length_min_um", "length_p05_um", "length_mean_um", "length_median_um",
        "length_p95_um", "length_max_um", "length_sd_um",
        "width_min_um", "width_p05_um", "width_mean_um", "width_median_um",
        "width_p95_um", "width_max_um", "width_sd_um",
        "q_min", "q_p05", "q_mean", "q_median", "q_p95", "q_max", "q_sd",
        "stats_version", "computed_at", "source_app", "source_app_version",
    }
    assert set(s.keys()) == expected


def test_empty_measurements_returns_empty_list():
    assert compute_observation_spore_summaries(
        observation_id=99, measurements=[], computed_at=FIXED_COMPUTED_AT,
    ) == []


def test_all_non_spore_measurements_returns_empty_list():
    rows = [
        _paired(10.0, 5.0, measurement_type="basidium"),
        _paired(11.0, 5.5, measurement_type="cystidium"),
    ]
    assert compute_observation_spore_summaries(
        observation_id=100, measurements=rows, computed_at=FIXED_COMPUTED_AT,
    ) == []
