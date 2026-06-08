from __future__ import annotations

import numpy as np
import pytest

from utils.image_processing_pipeline import (
    apply_auto_levels_from_bounds,
    apply_custom_white_balance,
    apply_post_decode_processing,
    compute_auto_level_bounds,
    compute_post_decode_transfer_curve,
    hard_luminance_levels,
    ProcessingDebugInfo,
    soft_luminance_levels,
)
from utils.raw_render import RawRenderSettings


def test_apply_post_decode_processing_custom_wb_applies_before_auto_levels():
    rgb = np.array(
        [
            [[0.10, 0.20, 0.30], [0.20, 0.30, 0.40]],
            [[0.30, 0.40, 0.50], [0.40, 0.50, 0.60]],
        ],
        dtype=np.float64,
    )
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(2.0, 1.0, 1.0),
        wb_multiplier_space="post_decode_rgb",
        auto_levels=True,
        black_percentile=0.0,
        white_percentile=1.0,
    )

    processed, debug = apply_post_decode_processing(rgb, settings, return_debug=True)
    custom_wb = apply_custom_white_balance(rgb, settings.wb_multipliers)
    black_level, white_level = compute_auto_level_bounds(custom_wb, 0.0, 1.0)
    expected = apply_auto_levels_from_bounds(custom_wb, black_level, white_level)

    assert np.allclose(processed, expected)
    assert debug.black_level == pytest.approx(black_level)
    assert debug.white_level == pytest.approx(white_level)


def test_hard_luminance_levels_respects_shadow_lift():
    values = np.array([0.20, 0.50, 0.80], dtype=np.float64)

    lifted = hard_luminance_levels(values, 0.20, 0.80, shadow_lift=0.10)
    legacy = hard_luminance_levels(values, 0.20, 0.80, shadow_lift=0.0)

    assert np.allclose(legacy, np.array([0.0, 0.5, 1.0], dtype=np.float64))
    assert np.allclose(lifted, np.array([0.10, 0.55, 1.0], dtype=np.float64))


@pytest.mark.parametrize(
    "input_min, black_level, white_level, input_max, expected_black, expected_white",
    [
        (0.20, 0.20, 0.80, 0.80, 0.05, 1.00),
        (0.15, 0.20, 0.80, 0.80, 0.15, 1.00),
        (0.15, 0.20, 0.80, 0.90, 0.15, 0.90),
    ],
)
def test_soft_luminance_levels_are_continuous_and_monotonic(
    input_min: float,
    black_level: float,
    white_level: float,
    input_max: float,
    expected_black: float,
    expected_white: float,
):
    values = np.linspace(0.0, 1.0, 1001, dtype=np.float64)
    soft = soft_luminance_levels(
        values,
        input_min,
        black_level,
        white_level,
        input_max,
        0.10,
        shadow_lift=0.05,
    )

    black_idx = int(np.where(np.isclose(values, black_level))[0][0])
    white_idx = int(np.where(np.isclose(values, white_level))[0][0])

    assert float(soft[black_idx]) == pytest.approx(expected_black)
    assert float(soft[white_idx]) == pytest.approx(expected_white)
    assert np.all(np.diff(soft) >= -1e-9)
    assert float(np.max(np.abs(np.diff(soft)))) < 0.05


def test_apply_post_decode_processing_tone_curve_modifies_post_level_values():
    rgb = np.full((2, 2, 3), 0.45, dtype=np.float64)
    linear = apply_post_decode_processing(
        rgb,
        RawRenderSettings(auto_levels=False, tone_curve_enabled=False),
    )
    curved = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            auto_levels=False,
            tone_curve_enabled=True,
            tone_curve_strength=0.75,
            tone_curve_midpoint=0.30,
        ),
    )

    assert np.allclose(linear, rgb)
    assert not np.allclose(curved, linear)
    assert float(curved.mean()) != pytest.approx(float(linear.mean()))


def test_apply_post_decode_processing_auto_levels_expands_low_contrast_range():
    rgb = np.array(
        [
            [[0.30, 0.30, 0.30], [0.32, 0.32, 0.32]],
            [[0.36, 0.36, 0.36], [0.40, 0.40, 0.40]],
        ],
        dtype=np.float64,
    )
    processed, debug = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            auto_levels=True,
            black_percentile=0.0,
            white_percentile=1.0,
        ),
        return_debug=True,
    )

    assert debug.black_level is not None
    assert debug.white_level is not None
    assert float(processed.min()) <= 0.05
    assert float(processed.max()) >= 0.95


def test_apply_post_decode_processing_shadow_lift_sets_output_floor():
    rgb = np.array(
        [
            [[0.20, 0.20, 0.20], [0.30, 0.30, 0.30]],
            [[0.50, 0.50, 0.50], [0.80, 0.80, 0.80]],
        ],
        dtype=np.float64,
    )
    processed = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            auto_levels=True,
            black_percentile=0.0,
            white_percentile=1.0,
            auto_levels_shadow_lift=0.10,
        ),
    )

    assert float(processed.min()) == pytest.approx(0.10, abs=1e-6)
    assert float(processed.max()) == pytest.approx(1.0, abs=1e-6)


def test_compute_post_decode_transfer_curve_soft_tails_and_strength_blend():
    settings = RawRenderSettings(
        auto_levels=True,
        black_percentile=0.2,
        white_percentile=0.8,
        auto_levels_strength=0.5,
        auto_levels_soft_tails=True,
        auto_levels_tail_size=0.1,
        auto_levels_shadow_lift=0.05,
        tone_curve_enabled=True,
        tone_curve_strength=0.75,
        tone_curve_midpoint=0.5,
    )
    debug = ProcessingDebugInfo(
        input_min=0.0,
        input_max=1.0,
        black_level=0.2,
        white_level=0.8,
        settings=settings.to_dict(),
    )

    curve = compute_post_decode_transfer_curve(np.zeros((1, 1, 3), dtype=np.float64), settings, debug=debug)
    black_idx = int(np.where(np.isclose(curve.input_values, 0.2))[0][0])
    white_idx = int(np.where(np.isclose(curve.input_values, 0.8))[0][0])

    assert float(curve.hard_target[black_idx]) == pytest.approx(0.05)
    assert float(curve.hard_target[white_idx]) == pytest.approx(1.0)
    assert float(curve.soft_target[black_idx]) == pytest.approx(0.15)
    assert float(curve.soft_target[white_idx]) == pytest.approx(0.90)
    assert float(curve.auto_levels_output[black_idx]) == pytest.approx(0.175)
    assert float(curve.auto_levels_output[white_idx]) == pytest.approx(0.85)
    assert float(curve.final_output[0]) > 0.0
    assert float(curve.final_output[0]) < float(curve.auto_levels_output[0])
    assert float(curve.final_output[-1]) == pytest.approx(1.0)
