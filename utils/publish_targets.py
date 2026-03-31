"""Shared publish-target helpers for Norway/Sweden reporting services."""
from __future__ import annotations

PUBLISH_TARGET_ARTSOBS_NO = "artsobs_no"
PUBLISH_TARGET_ARTPORTALEN_SE = "artportalen_se"
SETTING_ACTIVE_REPORTING_TARGET = "active_reporting_target"

PUBLISH_TARGET_CHOICES = (
    PUBLISH_TARGET_ARTSOBS_NO,
    PUBLISH_TARGET_ARTPORTALEN_SE,
)


def normalize_publish_target(value: str | None, fallback: str = PUBLISH_TARGET_ARTSOBS_NO) -> str:
    text = (value or "").strip().lower()
    if text in {"artsobservasjoner", "artsobs", "no", "norway", "norwegian", PUBLISH_TARGET_ARTSOBS_NO}:
        return PUBLISH_TARGET_ARTSOBS_NO
    if text in {"artportalen", "se", "sweden", "swedish", PUBLISH_TARGET_ARTPORTALEN_SE}:
        return PUBLISH_TARGET_ARTPORTALEN_SE
    return fallback


def publish_target_label(target: str | None) -> str:
    normalized = normalize_publish_target(target)
    if normalized == PUBLISH_TARGET_ARTPORTALEN_SE:
        return "Artportalen (Sweden)"
    return "Artsobservasjoner (Norway)"


def uploader_key_for_publish_target(target: str | None) -> str:
    normalized = normalize_publish_target(target)
    if normalized == PUBLISH_TARGET_ARTPORTALEN_SE:
        return "artportalen"
    return "web"


def nonregional_uploader_keys() -> tuple[str, ...]:
    return ("inat", "mo")


def infer_publish_target_from_coords(
    latitude: float | None,
    longitude: float | None,
) -> str | None:
    """Return a safe default only when the point is clearly inside one country box."""
    try:
        lat = float(latitude) if latitude is not None else None
        lon = float(longitude) if longitude is not None else None
    except (TypeError, ValueError):
        return None
    if lat is None or lon is None:
        return None

    in_norway_box = 57.0 <= lat <= 71.5 and 4.0 <= lon <= 32.5
    in_sweden_box = 55.0 <= lat <= 69.5 and 10.5 <= lon <= 24.8

    if in_norway_box and not in_sweden_box:
        return PUBLISH_TARGET_ARTSOBS_NO
    if in_sweden_box and not in_norway_box:
        return PUBLISH_TARGET_ARTPORTALEN_SE
    return None
