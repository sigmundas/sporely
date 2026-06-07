"""Lazy rawpy import helpers."""
from __future__ import annotations

import importlib
from datetime import datetime
from pathlib import Path
from typing import Any


def _raise_unavailable(message: str, exc: Exception | None = None) -> None:
    from utils.raw_render import RawRenderingUnavailableError

    if exc is None:
        raise RawRenderingUnavailableError(message)
    raise RawRenderingUnavailableError(message) from exc


def import_rawpy() -> Any:
    """Import rawpy lazily and normalize import failures."""
    try:
        return importlib.import_module("rawpy")
    except Exception as exc:  # pragma: no cover - import failure path is environment-specific
        _raise_unavailable(
            "RAW rendering requires rawpy. Install rawpy to enable RAW imports.",
            exc,
        )


def read_rawpy_capture_datetime(source_path: str | Path) -> datetime | None:
    """Return the source capture timestamp when rawpy exposes one."""
    rawpy_module = import_rawpy()
    try:
        with rawpy_module.imread(str(source_path)) as raw:
            other = getattr(raw, "other", None)
            timestamp = getattr(other, "timestamp", None)
            return timestamp if isinstance(timestamp, datetime) else None
    except Exception:
        return None


__all__ = ["import_rawpy", "read_rawpy_capture_datetime"]

# TODO(pyinstaller): rawpy/LibRaw may need hidden-import or native-library checks
# in the desktop bundle once packaging is exercised with real RAW files.
