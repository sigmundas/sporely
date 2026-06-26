from __future__ import annotations

import numpy as np
import pytest

from utils.image_processing_pipeline import (
    apply_auto_levels_from_bounds,
    apply_custom_white_balance,
    apply_exposure_compensation,
    apply_light_dark_levels,
    apply_post_decode_processing,
    apply_shadow_toe_lift,
    compute_auto_level_bounds,
    compute_post_decode_transfer_curve,
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
        exposure_ev=0.0,
        auto_levels=True,
        black_percentile=0.0,
        white_percentile=1.0,
    )

    processed, debug = apply_post_decode_processing(rgb, settings, return_debug=True)
    custom_wb = apply_custom_white_balance(rgb, settings.wb_multipliers)
    black_level, white_level = compute_auto_level_bounds(custom_wb, 0.0, 1.0)
    expected = apply_auto_levels_from_bounds(custom_wb, black_level, white_level)

    assert np.allclose(processed, np.clip(expected, 0.0, 1.0))
    assert debug.black_level == pytest.approx(black_level)
    assert debug.white_level == pytest.approx(white_level)


def test_apply_post_decode_processing_light_alias_shifts_manual_levels():
    values = np.array(
        [
            [[0.10, 0.10, 0.10], [0.40, 0.40, 0.40]],
            [[0.70, 0.70, 0.70], [0.15, 0.15, 0.15]],
        ],
        dtype=np.float64,
    )
    processed = apply_post_decode_processing(
        values,
        RawRenderSettings(
            exposure_ev=1.0,
            auto_levels=False,
            tone_curve_enabled=False,
        ),
    )

    expected = apply_light_dark_levels(np.array([0.10, 0.40, 0.70, 0.15], dtype=np.float64), 1.0, 0.0)

    assert np.allclose(processed[..., 0].ravel(), expected)


def test_apply_post_decode_processing_exposure_remains_visible_with_auto_levels():
    rgb = np.array(
        [
            [[0.12, 0.12, 0.12], [0.18, 0.18, 0.18]],
            [[0.24, 0.24, 0.24], [0.36, 0.36, 0.36]],
        ],
        dtype=np.float64,
    )
    base = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            exposure_ev=0.0,
            auto_levels=True,
            black_percentile=0.0,
            white_percentile=1.0,
            tone_curve_enabled=False,
        ),
    )
    bright = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            exposure_ev=1.0,
            auto_levels=True,
            black_percentile=0.0,
            white_percentile=1.0,
            tone_curve_enabled=False,
        ),
    )

    assert not np.allclose(base, bright)
    assert float(bright.mean()) > float(base.mean())


def test_apply_post_decode_processing_light_and_dark_shift_endpoints():
    rgb = np.array(
        [
            [[0.00, 0.00, 0.00], [0.25, 0.25, 0.25]],
            [[0.50, 0.50, 0.50], [1.00, 1.00, 1.00]],
        ],
        dtype=np.float64,
    )
    settings = RawRenderSettings(
        light_ev=0.5,
        dark_ev=-0.25,
        auto_levels=False,
        tone_curve_enabled=False,
    )

    processed = apply_post_decode_processing(rgb, settings)
    expected_luminance = apply_light_dark_levels(np.array([0.0, 0.25, 0.5, 1.0], dtype=np.float64), 0.5, -0.25)

    assert np.allclose(processed[..., 0].ravel(), expected_luminance)


def test_apply_shadow_toe_lift_is_monotonic_and_capped():
    values = np.linspace(0.0, 1.0, 1001, dtype=np.float64)
    lifted = apply_shadow_toe_lift(values, 0.10, cutoff=0.5)
    gentle = apply_shadow_toe_lift(values, 0.02, cutoff=0.5)

    assert np.all(np.diff(lifted) >= -1e-9)
    assert np.all(np.diff(gentle) >= -1e-9)
    assert float(np.max(lifted - values)) == pytest.approx(0.05, abs=1e-6)
    assert float(np.max(lifted - values)) > float(np.max(gentle - values))
    assert float(np.max(np.abs(np.diff(lifted)))) < 0.05


def test_apply_shadow_toe_lift_affects_deep_shadows_more_than_midtones():
    values = np.array([0.02, 0.10, 0.35, 0.60, 0.90], dtype=np.float64)

    lifted = apply_shadow_toe_lift(values, 0.10, cutoff=0.20)
    boosts = lifted - values

    assert boosts[0] > boosts[2]
    assert boosts[1] > boosts[3]
    assert float(lifted[-1]) == pytest.approx(values[-1], abs=1e-6)


@pytest.mark.parametrize(
    "input_min, black_level, white_level, input_max, expected_black, expected_white",
    [
        (0.20, 0.20, 0.80, 0.80, 0.00, 1.00),
        (0.15, 0.20, 0.80, 0.80, 0.10, 1.00),
        (0.15, 0.20, 0.80, 0.90, 0.10, 0.90),
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


def test_apply_post_decode_processing_tone_curve_still_pivots_with_exposure_and_shadows():
    rgb = np.full((2, 2, 3), 0.35, dtype=np.float64)
    base = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            light_ev=1.0,
            auto_levels=False,
            shadow_lift=0.0,
            tone_curve_enabled=False,
        ),
    )
    low_mid = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            light_ev=1.0,
            auto_levels=False,
            shadow_lift=0.10,
            tone_curve_enabled=True,
            tone_curve_strength=0.75,
            tone_curve_midpoint=0.30,
        ),
    )
    high_mid = apply_post_decode_processing(
        rgb,
        RawRenderSettings(
            light_ev=1.0,
            auto_levels=False,
            shadow_lift=0.10,
            tone_curve_enabled=True,
            tone_curve_strength=0.75,
            tone_curve_midpoint=0.70,
        ),
    )

    assert np.allclose(base, apply_light_dark_levels(rgb, 1.0, 0.0))
    assert not np.allclose(low_mid, high_mid)
    assert float(low_mid.mean()) != pytest.approx(float(high_mid.mean()))


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


def test_apply_post_decode_processing_shadow_toe_lift_lifts_deep_shadows_more_than_midtones():
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
            shadow_lift=0.05,
        ),
    )

    assert float(processed[0, 0, 0]) == pytest.approx(0.01, abs=1e-6)
    assert float(processed[0, 1, 0]) > float(processed[0, 0, 0])
    assert float(processed[1, 0, 0]) > float(processed[0, 1, 0])
    assert float(processed.max()) == pytest.approx(1.0, abs=1e-6)


def test_apply_post_decode_processing_shadow_highlights_anchor_endpoints():
    ramp = np.linspace(0.0, 1.0, 512, dtype=np.float64)
    rgb = np.repeat(ramp[None, :, None], 3, axis=2)
    settings = RawRenderSettings(
        auto_levels=False,
        tone_curve_enabled=False,
        tone_shadows=0.85,
        tone_highlights=0.70,
    )

    processed = apply_post_decode_processing(rgb, settings)

    assert float(processed[0, 0, 0]) == pytest.approx(0.0, abs=1e-6)
    assert float(processed[0, -1, 0]) == pytest.approx(1.0, abs=1e-6)
    assert np.all(np.diff(processed[0, :, 0]) >= -1e-9)
    assert float(processed.min()) >= 0.0
    assert float(processed.max()) <= 1.0


def test_compute_post_decode_transfer_curve_soft_tails_and_strength_blend():
    settings = RawRenderSettings(
        auto_levels=True,
        black_percentile=0.2,
        white_percentile=0.8,
        auto_levels_strength=0.5,
        auto_levels_soft_tails=True,
        auto_levels_tail_size=0.1,
        exposure_ev=0.0,
        shadow_lift=0.0,
        tone_shadows=0.75,
        tone_highlights=-0.35,
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

    assert float(curve.hard_target[black_idx]) == pytest.approx(0.0)
    assert float(curve.hard_target[white_idx]) == pytest.approx(1.0)
    assert float(curve.soft_target[black_idx]) == pytest.approx(0.10)
    assert float(curve.soft_target[white_idx]) == pytest.approx(0.90)
    assert float(curve.auto_levels_output[black_idx]) == pytest.approx(0.15)
    assert float(curve.auto_levels_output[white_idx]) == pytest.approx(0.85)
    assert float(curve.shadow_highlight_output[0]) == pytest.approx(0.0, abs=1e-6)
    assert float(curve.shadow_highlight_output[-1]) == pytest.approx(1.0, abs=1e-6)
    assert float(curve.final_output[-1]) == pytest.approx(1.0)
    assert not np.allclose(curve.final_output, curve.shadow_toe_output)
    assert np.all(np.diff(curve.manual_levels_output) >= -1e-9)
    assert np.all(np.diff(curve.auto_levels_output) >= -1e-9)
    assert np.all(np.diff(curve.shadow_toe_output) >= -1e-9)
    assert np.all(np.diff(curve.shadow_highlight_output) >= -1e-9)
    assert np.all(np.diff(curve.final_output) >= -1e-9)
