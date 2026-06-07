import pytest

from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError, render_raw_image


def test_raw_render_settings_round_trip():
    settings = RawRenderSettings(
        white_balance_mode="auto",
        wb_multipliers=(1.1, 1.0, 1.3),
        wb_selection=(10.0, 20.0, 30.0, 40.0),
        auto_levels=True,
        black_percentile=0.01,
        white_percentile=0.99,
        tone_curve_enabled=True,
        tone_curve_strength=0.75,
        tone_curve_midpoint=0.42,
        output_bps=8,
    )

    assert RawRenderSettings.from_dict(settings.to_dict()) == settings


def test_raw_render_placeholder_raises():
    with pytest.raises(RawRenderingUnavailableError):
        render_raw_image("sample.nef")
