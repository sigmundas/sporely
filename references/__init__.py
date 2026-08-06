"""Reference-data helpers (measurement parsing etc.).

Pure-Python utilities with no Qt/UI dependency so they can be unit-tested
without a QApplication and reused outside the desktop client.
"""

from references.measurement_parser import (
    DimensionRange,
    MeasurementParseResult,
    parse_measurement_string,
)
from references.reference_plotting import (
    translate_observation_reference_use,
    translate_observation_reference_uses,
)

__all__ = [
    "DimensionRange",
    "MeasurementParseResult",
    "parse_measurement_string",
    "translate_observation_reference_use",
    "translate_observation_reference_uses",
]
