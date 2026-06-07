"""Lazy rawpy import helpers."""
from __future__ import annotations

import importlib
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


__all__ = ["import_rawpy"]
