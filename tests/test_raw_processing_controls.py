from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from ui.raw_processing_controls import RawProcessingControls
from utils.raw_render import RawRenderSettings


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_raw_processing_controls_round_trip_preserves_raw_settings(qapp):
    controls = RawProcessingControls()
    assert not hasattr(controls, "shadow_lift_label")
    assert not hasattr(controls, "preserve_tails_checkbox")
    assert controls.light_label.text() == "Light:"
    assert controls.dark_label.text() == "Dark:"
    assert controls.light_slider.minimum() == 0
    assert controls.light_slider.maximum() == 20
    assert controls.dark_slider.minimum() == 0
    assert controls.dark_slider.maximum() == 10
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.15, 1.0, 1.42),
        wb_selection=(12.0, 14.0, 24.0, 26.0),
        wb_multiplier_space="post_decode_rgb",
        wb_sample_point=(18.0, 19.0),
        wb_sample_size=9,
        wb_sample_base_mode="camera",
        wb_selection_space="preview_pixels",
        light_ev=0.5,
        dark_ev=-0.25,
        auto_levels=False,
        black_percentile=0.01,
        white_percentile=0.99,
        auto_levels_strength=0.7,
        auto_levels_soft_tails=True,
        auto_levels_tail_size=0.05,
        shadow_lift=0.03,
        tone_curve_enabled=True,
        tone_curve_strength=0.65,
        tone_curve_midpoint=0.45,
        output_bps=8,
    )

    controls.set_settings(settings)

    round_tripped = controls.settings()

    assert round_tripped.white_balance_mode == settings.white_balance_mode
    assert round_tripped.wb_multipliers == settings.wb_multipliers
    assert round_tripped.wb_selection == settings.wb_selection
    assert round_tripped.wb_multiplier_space == settings.wb_multiplier_space
    assert round_tripped.wb_sample_point == settings.wb_sample_point
    assert round_tripped.wb_sample_size == settings.wb_sample_size
    assert round_tripped.wb_sample_base_mode == settings.wb_sample_base_mode
    assert round_tripped.wb_selection_space == settings.wb_selection_space
    assert round_tripped.light_ev == settings.light_ev
    assert round_tripped.dark_ev == settings.dark_ev
    assert round_tripped.exposure_ev == pytest.approx(settings.light_ev + settings.dark_ev)
    assert round_tripped.auto_levels == settings.auto_levels
    assert round_tripped.auto_levels_soft_tails is False
    assert round_tripped.shadow_lift == settings.shadow_lift
    assert round_tripped.tone_curve_enabled == settings.tone_curve_enabled
    assert round_tripped.tone_curve_strength == settings.tone_curve_strength
    assert round_tripped.tone_curve_midpoint == settings.tone_curve_midpoint
    assert round_tripped.output_bps == settings.output_bps


def test_raw_processing_controls_exposure_and_shadows_update_settings_without_losing_custom_wb(qapp):
    controls = RawProcessingControls()
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.2, 1.0, 1.4),
        wb_selection=(10.0, 12.0, 20.0, 22.0),
        wb_multiplier_space="post_decode_rgb",
        light_ev=0.25,
        dark_ev=-0.10,
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.55,
        tone_curve_midpoint=0.40,
        shadow_lift=0.02,
    )

    controls.set_settings(settings)
    controls.light_slider.setValue(10)
    controls.dark_slider.setValue(8)

    updated = controls.settings()

    assert updated.white_balance_mode == "custom"
    assert updated.wb_multipliers == settings.wb_multipliers
    assert updated.light_ev == pytest.approx(0.50)
    assert updated.dark_ev == pytest.approx(-0.40)
    assert updated.exposure_ev == pytest.approx(0.10)
    assert updated.shadow_lift == pytest.approx(settings.shadow_lift)
    assert updated.tone_curve_enabled is True
    assert updated.tone_curve_strength == settings.tone_curve_strength
    assert updated.tone_curve_midpoint == settings.tone_curve_midpoint


def test_raw_processing_controls_set_settings_does_not_emit_settings_changed(qapp):
    controls = RawProcessingControls()
    emissions: list[object] = []
    controls.settingsChanged.connect(lambda settings: emissions.append(settings))

    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="custom",
            wb_multipliers=(1.2, 1.0, 1.4),
            tone_curve_enabled=True,
            tone_curve_strength=0.6,
            tone_curve_midpoint=0.4,
        )
    )

    assert emissions == []
