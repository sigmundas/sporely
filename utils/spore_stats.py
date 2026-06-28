"""Utility for building structured spore-statistics dicts for cloud sync."""
from __future__ import annotations

import json


def build_structured_spore_statistics(stats: dict | None) -> dict | None:
    """Return a structured spore statistics dict ready for JSON serialisation.

    Normalises numpy scalars to builtin Python numbers so json.dumps never
    raises.  Returns None when stats is empty or has no count, so callers
    can write NULL rather than fabricating ranges.

    The returned dict contains:
    - version, n, length_*_um, optionally width_*_um and q_* (numeric)
    - rendered: the English-format legacy display string
    - method, method_version
    """
    if not stats or not stats.get('count'):
        return None

    n = int(stats['count'])
    rendered = (
        f"Spores: ({float(stats['length_min']):.1f}-){float(stats['length_p5']):.1f}-"
        f"{float(stats['length_p95']):.1f}(-{float(stats['length_max']):.1f}) um"
    )
    obj: dict = {
        "version": 1,
        "n": n,
        "length_min_um": round(float(stats['length_min']), 3),
        "length_max_um": round(float(stats['length_max']), 3),
        "length_core_min_um": round(float(stats['length_p5']), 3),
        "length_core_max_um": round(float(stats['length_p95']), 3),
        "length_mean_um": round(float(stats['length_mean']), 3),
    }

    has_widths = 'width_mean' in stats and float(stats.get('width_mean') or 0) > 0
    if has_widths:
        rendered += (
            f" x ({float(stats['width_min']):.1f}-){float(stats['width_p5']):.1f}-"
            f"{float(stats['width_p95']):.1f}(-{float(stats['width_max']):.1f}) um"
        )
        rendered += (
            f", Q = ({float(stats['ratio_min']):.1f}-){float(stats['ratio_p5']):.1f}-"
            f"{float(stats['ratio_p95']):.1f}(-{float(stats['ratio_max']):.1f})"
        )
        rendered += f", Qm = {float(stats['ratio_mean']):.1f}"
        obj.update({
            "width_min_um": round(float(stats['width_min']), 3),
            "width_max_um": round(float(stats['width_max']), 3),
            "width_core_min_um": round(float(stats['width_p5']), 3),
            "width_core_max_um": round(float(stats['width_p95']), 3),
            "width_mean_um": round(float(stats['width_mean']), 3),
            "q_min": round(float(stats['ratio_min']), 4),
            "q_max": round(float(stats['ratio_max']), 4),
            "q_mean": round(float(stats['ratio_mean']), 4),
        })

    rendered += f", n = {n}"
    obj["rendered"] = rendered
    obj["method"] = "sporely-py"
    obj["method_version"] = "1"
    return obj


def serialise_spore_statistics(stats: dict | None) -> str | None:
    """Build and JSON-serialise a structured spore statistics dict.

    Returns None when there are no measurements to avoid writing fake data.
    """
    structured = build_structured_spore_statistics(stats)
    if structured is None:
        return None
    return json.dumps(structured, sort_keys=True, ensure_ascii=False)
