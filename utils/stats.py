"""Statistical calculation utilities."""
import numpy as np
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



