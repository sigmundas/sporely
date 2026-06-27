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
    assert not hasattr(controls, "preserve_tails_checkbox")
    assert controls.light_label.text() == "Light:"
    assert controls.dark_label.text() == "Dark:"
    assert controls.curve_strength_label.text() == "Strength:"
    assert controls.strength_label is controls.curve_strength_label
    assert controls.shadow_lift_label is controls.shadows_label
    assert controls.shadow_lift_slider is controls.shadows_slider
    assert controls.shadow_lift_value_label is controls.shadows_value_label
    assert controls.auto_levels_checkbox.isHidden() is False
    assert controls.auto_levels_checkbox.text() == "Auto levels"
    assert controls.light_slider.minimum() == 0
    assert controls.light_slider.maximum() == 2000
    assert controls.dark_slider.minimum() == 0
    assert controls.dark_slider.maximum() == 2000
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
        tone_shadows=0.30,
        tone_highlights=-0.20,
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
    assert round_tripped.tone_shadows == settings.tone_shadows
    assert round_tripped.tone_highlights == settings.tone_highlights
    assert round_tripped.output_bps == settings.output_bps
    assert controls.light_value_label.text() == "0.500"
    assert controls.dark_value_label.text() == "0.250"


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
        tone_shadows=0.35,
        tone_highlights=-0.15,
    )

    controls.set_settings(settings)
    controls.light_slider.setValue(500)
    controls.dark_slider.setValue(400)

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
    assert updated.tone_shadows == pytest.approx(0.35)
    assert updated.tone_highlights == pytest.approx(-0.15)


def test_raw_processing_controls_dragging_light_slider_emits_live_and_clears_auto_levels(qapp):
    controls = RawProcessingControls()
    emissions: list[RawRenderSettings] = []
    controls.settingsChanged.connect(lambda settings: emissions.append(settings))

    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.0,
            dark_ev=0.0,
        )
    )

    controls.light_slider.setSliderDown(True)
    controls.light_slider.setValue(600)
    assert len(emissions) == 1
    assert emissions[0].light_ev == pytest.approx(0.60)
    assert controls.auto_levels_checkbox.isChecked() is False

    controls.light_slider.setSliderDown(False)
    controls._on_slider_released()

    assert len(emissions) == 1
    assert emissions[0].auto_levels is False


def test_raw_processing_controls_reenabling_auto_levels_restores_cached_slider_positions(qapp):
    controls = RawProcessingControls()
    controls.set_auto_level_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.357,
            dark_ev=-0.143,
        )
    )
    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.125,
            dark_ev=-0.031,
        )
    )

    assert controls.auto_levels_checkbox.isChecked() is True
    assert controls.light_slider.value() == 357
    assert controls.dark_slider.value() == 143

    controls.light_slider.setValue(1234)
    assert controls.auto_levels_checkbox.isChecked() is False

    controls.auto_levels_checkbox.setChecked(True)

    assert controls.auto_levels_checkbox.isChecked() is True
    assert controls.light_slider.value() == 357
    assert controls.dark_slider.value() == 143


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
