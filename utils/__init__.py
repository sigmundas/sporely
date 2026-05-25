"""Utility functions for the mushroom spore analyzer.

The package stays lazy so importing a lightweight helper like
``utils.vernacular_utils`` does not eagerly pull in Qt/PySide6 through
``utils.image_utils``.
"""
from __future__ import annotations


_LAZY_EXPORTS = {
    "load_image": (".image_utils", "load_image"),
    "scale_image": (".image_utils", "scale_image"),
    "calculate_statistics": (".stats", "calculate_statistics"),
}


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        module_name, attr_name = _LAZY_EXPORTS[name]
        module = __import__(f"{__name__}{module_name}", fromlist=[attr_name])
        return getattr(module, attr_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = list(_LAZY_EXPORTS)
