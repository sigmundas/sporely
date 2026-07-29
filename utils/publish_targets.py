"""Shared publish-target helpers for Norway/Sweden reporting services."""
from __future__ import annotations

import re

PUBLISH_TARGET_ARTSOBS_NO = "artsobs_no"
PUBLISH_TARGET_ARTPORTALEN_SE = "artportalen_se"
SETTING_ACTIVE_REPORTING_TARGET = "active_reporting_target"
SPORELY_PUBLIC_SITE_ORIGIN = "https://sporely.no"

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


def sporely_public_observation_url(observation: dict | None) -> str | None:
    """Return the landing-page URL when the cloud observation is public."""
    obs = observation or {}
    cloud_id = obs.get("cloud_id")
    if cloud_id is None or isinstance(cloud_id, bool):
        return None
    try:
        normalized_cloud_id = int(str(cloud_id).strip())
    except (TypeError, ValueError):
        return None
    if normalized_cloud_id <= 0:
        return None

    visibility = str(
        obs.get("sharing_scope") or obs.get("visibility") or ""
    ).strip().lower()
    if visibility != "public":
        return None

    raw_is_draft = obs.get("is_draft")
    if isinstance(raw_is_draft, str):
        is_draft = raw_is_draft.strip().lower() in {
            "1", "true", "t", "yes", "y", "on",
        }
    else:
        is_draft = bool(raw_is_draft)
    if is_draft:
        return None

    return f"{SPORELY_PUBLIC_SITE_ORIGIN}/observations/{normalized_cloud_id}"


def compose_publish_notes(
    comment: str | None,
    spore_dimensions: str | None,
    sporely_url: str | None,
    *,
    uploader_key: str | None,
) -> str | None:
    """Compose external notes, keeping dimensions immediately before the URL."""
    dimensions = str(spore_dimensions or "").strip()
    if dimensions and str(uploader_key or "").strip().lower() == "inat":
        dimensions = re.sub(
            r"^\s*(?:Spores?|Sporer)\s*:",
            "Spores:",
            dimensions,
            count=1,
            flags=re.IGNORECASE,
        )

    parts = [
        part
        for part in (
            str(comment or "").strip(),
            dimensions,
            str(sporely_url or "").strip(),
        )
        if part
    ]
    return "\n".join(parts) if parts else None


def publish_target_from_country_code(country_code: str | None) -> str | None:
    text = str(country_code or "").strip().lower()
    if text == "no":
        return PUBLISH_TARGET_ARTSOBS_NO
    if text == "se":
        return PUBLISH_TARGET_ARTPORTALEN_SE
    return None


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
