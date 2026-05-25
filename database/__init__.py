"""Database module for mushroom spore measurements.

The package keeps its imports lazy so non-UI helpers can import submodules like
``database.taxon_lookup`` without immediately pulling in Qt/PySide6 through the
database model layer.
"""
from __future__ import annotations

from importlib import import_module


_MODEL_EXPORTS = {
    "ObservationDB",
    "ImageDB",
    "MeasurementDB",
    "ReferenceDB",
    "SettingsDB",
    "SessionLogDB",
    "CalibrationDB",
    "MeasurementRepository",
}


def init_database(*args, **kwargs):
    from .schema import init_database as _init_database

    return _init_database(*args, **kwargs)


def __getattr__(name: str):
    if name in _MODEL_EXPORTS:
        models = import_module(".models", __name__)
        if name == "MeasurementRepository":
            return getattr(models, "MeasurementDB")
        return getattr(models, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "init_database",
    "ObservationDB",
    "ImageDB",
    "MeasurementDB",
    "ReferenceDB",
    "SettingsDB",
    "SessionLogDB",
    "CalibrationDB",
    "MeasurementRepository",
]
