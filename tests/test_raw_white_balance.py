import numpy as np
import pytest

from utils.raw_white_balance import estimate_white_balance_from_background


def test_estimate_white_balance_from_background_uses_channel_means():
    rgb = np.zeros((4, 4, 3), dtype=np.float64)
    rgb[..., 0] = 0.4
    rgb[..., 1] = 0.5
    rgb[..., 2] = 0.8
    rgb[0, 0, :] = [0.0, 0.0, 0.0]
    rgb[0, 1, :] = [1.0, 1.0, 1.0]

    gains = estimate_white_balance_from_background(rgb, rect=(0, 0, 4, 4))

    assert np.allclose(gains, np.array([1.25, 1.0, 0.625], dtype=np.float64))


def test_estimate_white_balance_rejects_out_of_bounds_rect():
    rgb = np.full((4, 4, 3), 0.5, dtype=np.float64)

    with pytest.raises(ValueError):
        estimate_white_balance_from_background(rgb, rect=(10, 10, 1, 1))
