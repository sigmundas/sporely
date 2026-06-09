"""Lazy rawpy import helpers."""
from __future__ import annotations

import importlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawpyDiagnostic:
    available: bool
    error_type: str | None = None
    error_message: str | None = None
    rawpy_version: str | None = None


def _debug_rawpy_enabled() -> bool:
    return str(os.environ.get("SPORELY_DEBUG_RAWPY") or "").strip().lower() not in {"", "0", "false", "no", "off"}


def _probe_rawpy() -> tuple[Any | None, Exception | None, RawpyDiagnostic]:
    try:
        module = importlib.import_module("rawpy")
    except Exception as exc:  # pragma: no cover - import failure path is environment-specific
        diagnostic = RawpyDiagnostic(
            available=False,
            error_type=exc.__class__.__name__,
            error_message=str(exc) or None,
            rawpy_version=None,
        )
        return None, exc, diagnostic

    version = getattr(module, "__version__", None)
    version_text = str(version).strip() if version is not None else None
    diagnostic = RawpyDiagnostic(
        available=True,
        rawpy_version=version_text or None,
    )
    return module, None, diagnostic


def _raise_unavailable(message: str, exc: Exception | None = None) -> None:
    from utils.raw_render import RawRenderingUnavailableError

    if exc is None:
        raise RawRenderingUnavailableError(message)
    raise RawRenderingUnavailableError(message) from exc


def import_rawpy() -> Any:
    """Import rawpy lazily and normalize import failures."""
    module, exc, diagnostic = _probe_rawpy()
    if module is not None:
        return module
    if _debug_rawpy_enabled() and exc is not None:
        logger.debug(
            "rawpy import failed; diagnostics=%s",
            diagnostic,
            exc_info=(exc.__class__, exc, exc.__traceback__),
        )
    _raise_unavailable(
        "RAW rendering requires rawpy. Install rawpy to enable RAW imports.",
        exc,
    )


def rawpy_diagnostics() -> RawpyDiagnostic:
    """Return a best-effort diagnostic snapshot for rawpy availability."""
    _module, _exc, diagnostic = _probe_rawpy()
    return diagnostic


def read_rawpy_capture_datetime(source_path: str | Path) -> datetime | None:
    """Return the source capture timestamp when rawpy exposes one."""
    try:
        rawpy_module = import_rawpy()
    except Exception as exc:
        if _debug_rawpy_enabled():
            logger.debug(
                "rawpy capture timestamp import failed for %s; diagnostics=%s",
                source_path,
                rawpy_diagnostics(),
                exc_info=(exc.__class__, exc, exc.__traceback__),
            )
        return None
    try:
        with rawpy_module.imread(str(source_path)) as raw:
            other = getattr(raw, "other", None)
            timestamp = getattr(other, "timestamp", None)
            return timestamp if isinstance(timestamp, datetime) else None
    except Exception as read_exc:
        if _debug_rawpy_enabled():
            logger.debug(
                "rawpy capture timestamp read failed for %s: %s: %s",
                source_path,
                read_exc.__class__.__name__,
                read_exc,
                exc_info=(read_exc.__class__, read_exc, read_exc.__traceback__),
            )
        return None


__all__ = ["RawpyDiagnostic", "import_rawpy", "rawpy_diagnostics", "read_rawpy_capture_datetime"]

# TODO(pyinstaller): rawpy/LibRaw may need hidden-import or native-library checks
# in the desktop bundle once packaging is exercised with real RAW files.
