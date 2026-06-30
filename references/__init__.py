"""Reference-data helpers (measurement parsing etc.).

Pure-Python utilities with no Qt/UI dependency so they can be unit-tested
without a QApplication and reused outside the desktop client.
"""

from references.measurement_parser import (
    DimensionRange,
    MeasurementParseResult,
    parse_measurement_string,
)

__all__ = [
    "DimensionRange",
    "MeasurementParseResult",
    "parse_measurement_string",
]
