"""Statistical calculation utilities."""
import numpy as np
from statistics import NormalDist
from typing import List, Dict


def calculate_statistics(measurements: List[float]) -> Dict[str, float]:
    """
    Calculate statistical measures for a list of measurements.

    Args:
        measurements: List of measurement values

    Returns:
        Dictionary containing mean, std, min, max, and count
    """
    if not measurements:
        return {
            'mean': 0.0,
            'std': 0.0,
            'min': 0.0,
            'max': 0.0,
            'count': 0
        }

    measurements_array = np.array(measurements)

    return {
        'mean': float(np.mean(measurements_array)),
        'std': float(np.std(measurements_array)),
        'min': float(np.min(measurements_array)),
        'max': float(np.max(measurements_array)),
        'count': len(measurements)
    }


def calculate_confidence_interval(
    measurements: List[float],
    confidence: float = 0.95,
) -> tuple[float, float]:
    """Return a normal-approximation confidence interval for the mean."""
    if len(measurements) < 2:
        return 0.0, 0.0

    measurements_array = np.array(measurements, dtype=float)
    mean = float(np.mean(measurements_array))
    std_error = float(np.std(measurements_array, ddof=1) / np.sqrt(len(measurements_array)))
    if std_error == 0.0:
        return mean, mean

    alpha = 1.0 - float(confidence)
    z_score = NormalDist().inv_cdf(1.0 - alpha / 2.0)
    margin = z_score * std_error
    return mean - margin, mean + margin
