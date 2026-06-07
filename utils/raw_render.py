"""RAW render settings scaffolding for the future rawpy-backed renderer."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on", "enabled"}


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _coerce_float_tuple(value: Any, length: int) -> tuple[float, ...] | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, list):
        items = list(value)
    else:
        return None
    if len(items) != length:
        return None
    try:
        return tuple(float(item) for item in items)
    except Exception:
        return None


@dataclass(frozen=True, slots=True)
class RawRenderSettings:
    """Serializable RAW rendering parameters."""

    white_balance_mode: str = "camera"
    wb_multipliers: tuple[float, float, float] | None = None
    wb_selection: tuple[float, float, float, float] | None = None
    auto_levels: bool = False
    black_percentile: float = 0.001
    white_percentile: float = 0.999
    tone_curve_enabled: bool = False
    tone_curve_strength: float = 0.5
    tone_curve_midpoint: float = 0.5
    output_bps: int = 16

    @classmethod
    def default(cls) -> "RawRenderSettings":
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {
            "white_balance_mode": self.white_balance_mode,
            "wb_multipliers": list(self.wb_multipliers) if self.wb_multipliers is not None else None,
            "wb_selection": list(self.wb_selection) if self.wb_selection is not None else None,
            "auto_levels": bool(self.auto_levels),
            "black_percentile": float(self.black_percentile),
            "white_percentile": float(self.white_percentile),
            "tone_curve_enabled": bool(self.tone_curve_enabled),
            "tone_curve_strength": float(self.tone_curve_strength),
            "tone_curve_midpoint": float(self.tone_curve_midpoint),
            "output_bps": int(self.output_bps),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | dict[str, Any] | None) -> "RawRenderSettings":
        if value is None:
            return cls.default()
        if isinstance(value, cls):
            return value
        mapping = dict(value) if isinstance(value, Mapping) else {}
        white_balance_mode = str(mapping.get("white_balance_mode") or "camera").strip().lower() or "camera"
        wb_multipliers = _coerce_float_tuple(mapping.get("wb_multipliers"), 3)
        wb_selection = _coerce_float_tuple(mapping.get("wb_selection"), 4)
        return cls(
            white_balance_mode=white_balance_mode,
            wb_multipliers=wb_multipliers,  # type: ignore[arg-type]
            wb_selection=wb_selection,  # type: ignore[arg-type]
            auto_levels=_coerce_bool(mapping.get("auto_levels"), False),
            black_percentile=_coerce_float(mapping.get("black_percentile"), 0.001),
            white_percentile=_coerce_float(mapping.get("white_percentile"), 0.999),
            tone_curve_enabled=_coerce_bool(mapping.get("tone_curve_enabled"), False),
            tone_curve_strength=_coerce_float(mapping.get("tone_curve_strength"), 0.5),
            tone_curve_midpoint=_coerce_float(mapping.get("tone_curve_midpoint"), 0.5),
            output_bps=_coerce_int(mapping.get("output_bps"), 16),
        )


class RawRenderingUnavailableError(RuntimeError):
    """Raised when RAW rendering is requested before the renderer exists."""


def render_raw_image(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    preview: bool = False,
    **_kwargs: Any,
) -> Path:
    """Placeholder for the future rawpy-backed renderer.

    The signature is intentionally stable now so the ingest façade can be wired
    without changing later call sites.
    """
    _ = source_path, settings, output_path, output_dir, preview
    raise RawRenderingUnavailableError(
        "RAW rendering is not enabled yet. rawpy-based demosaicing will be added in a later pass."
    )


__all__ = [
    "RawRenderSettings",
    "RawRenderingUnavailableError",
    "render_raw_image",
]
