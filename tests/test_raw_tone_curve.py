import numpy as np

from utils.raw_tone_curve import apply_luminance_tone_curve, normalized_sigmoid_curve


def test_normalized_sigmoid_curve_is_identity_when_strength_is_zero():
    values = np.array([-0.25, 0.25, 1.25], dtype=np.float64)

    mapped = normalized_sigmoid_curve(values, strength=0.0, midpoint=0.5)

    assert np.allclose(mapped, np.array([0.0, 0.25, 1.0], dtype=np.float64))


def test_apply_luminance_tone_curve_preserves_hue_and_alpha():
    rgb = np.array([[[0.2, 0.4, 0.6, 0.8]]], dtype=np.float64)

    curved = apply_luminance_tone_curve(rgb, strength=0.75, midpoint=0.45)

    assert curved.shape == rgb.shape
    assert curved[0, 0, 3] == np.float64(0.8)
    original_ratio = rgb[0, 0, :3] / rgb[0, 0, :3].sum()
    curved_ratio = curved[0, 0, :3] / curved[0, 0, :3].sum()
    assert np.allclose(curved_ratio, original_ratio)
    assert np.all((curved >= 0.0) & (curved <= 1.0))
