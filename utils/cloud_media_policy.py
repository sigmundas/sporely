from __future__ import annotations

from math import sqrt
from typing import Any


CLOUD_QUALITY_PROFILE_STANDARD = "standard"
CLOUD_QUALITY_PROFILE_HIGH = "high"

CLOUD_REDUCED_MAX_PIXELS = 2_000_000
CLOUD_FULL_MAX_PIXELS = 20_000_000
CLOUD_THUMB_MAX_EDGE = 400

CLOUD_STANDARD_FULL_WEBP_QUALITY = 65
CLOUD_HIGH_FULL_WEBP_QUALITY = 80
CLOUD_THUMB_WEBP_QUALITY = 65
CLOUD_THUMB_JPEG_QUALITY = 75

CLOUD_STANDARD_FULL_BYTE_CAP = 1_000_000
CLOUD_HIGH_FULL_BYTE_CAP = 5_000_000

IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE = "Image too large for plan"

_CLOUD_STANDARD_FULL_WEBP_QUALITIES = (65, 55, 45, 35, 25)
_CLOUD_HIGH_FULL_WEBP_QUALITIES = (80, 70, 60, 50, 40)


def _parse_nullable_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def normalize_cloud_plan_profile(profile: dict | None) -> dict[str, Any]:
    record = dict(profile or {})
    raw_plan = str(record.get("cloud_plan") or record.get("cloudPlan") or "").strip().lower()
    has_pro_access = raw_plan == "pro" or bool(record.get("is_pro") or record.get("isPro"))
    cloud_plan = "pro" if has_pro_access else "free"
    quality_profile = CLOUD_QUALITY_PROFILE_HIGH if has_pro_access else CLOUD_QUALITY_PROFILE_STANDARD
    full_res_storage_enabled = bool(record.get("full_res_storage_enabled") or record.get("fullResStorageEnabled"))
    return {
        "cloud_plan": cloud_plan,
        "cloudPlan": cloud_plan,
        "quality_profile": quality_profile,
        "qualityProfile": quality_profile,
        "has_pro_access": has_pro_access,
        "full_res_storage_enabled": full_res_storage_enabled,
        "fullResStorageEnabled": full_res_storage_enabled,
        "storage_quota_bytes": _parse_nullable_int(record.get("storage_quota_bytes") or record.get("storageQuotaBytes")),
        "storageQuotaBytes": _parse_nullable_int(record.get("storage_quota_bytes") or record.get("storageQuotaBytes")),
        "storage_used_bytes": max(0, _parse_nullable_int(
            record.get("total_storage_bytes")
            or record.get("storage_used_bytes")
            or record.get("storageUsedBytes")
        ) or 0),
        "storageUsedBytes": max(0, _parse_nullable_int(
            record.get("total_storage_bytes")
            or record.get("storage_used_bytes")
            or record.get("storageUsedBytes")
        ) or 0),
        "image_count": max(0, _parse_nullable_int(record.get("image_count") or record.get("imageCount")) or 0),
        "imageCount": max(0, _parse_nullable_int(record.get("image_count") or record.get("imageCount")) or 0),
    }


def normalize_cloud_upload_mode(value: str | None) -> str:
    return "full" if str(value or "").strip().lower() == "full" else "reduced"


def scale_dimensions_to_max_pixels(width, height, max_pixels) -> dict[str, int | bool]:
    source_width = max(1, int(width or 0))
    source_height = max(1, int(height or 0))
    cap = max(1, int(max_pixels or 0))
    pixels = source_width * source_height
    if pixels <= cap:
        return {
            "width": source_width,
            "height": source_height,
            "resized": False,
        }

    scale = sqrt(cap / pixels)
    return {
        "width": max(1, int(round(source_width * scale))),
        "height": max(1, int(round(source_height * scale))),
        "resized": True,
    }


def build_full_image_webp_quality_attempts(quality_profile: str | None) -> tuple[int, ...]:
    profile = str(quality_profile or "").strip().lower()
    if profile == CLOUD_QUALITY_PROFILE_HIGH:
        return _CLOUD_HIGH_FULL_WEBP_QUALITIES
    return _CLOUD_STANDARD_FULL_WEBP_QUALITIES


def build_cloud_upload_policy(profile: dict | None, upload_mode: str = "reduced") -> dict[str, Any]:
    normalized = profile if isinstance(profile, dict) and "quality_profile" in profile else normalize_cloud_plan_profile(profile)
    mode = normalize_cloud_upload_mode(upload_mode)
    quality_profile = str(normalized.get("quality_profile") or CLOUD_QUALITY_PROFILE_STANDARD).strip().lower()
    if quality_profile not in {CLOUD_QUALITY_PROFILE_STANDARD, CLOUD_QUALITY_PROFILE_HIGH}:
        quality_profile = CLOUD_QUALITY_PROFILE_STANDARD

    return {
        **normalized,
        "upload_mode": mode,
        "uploadMode": mode,
        "image_resolution_mode": "max" if mode == "full" else "reduced",
        "imageResolutionMode": "max" if mode == "full" else "reduced",
        "max_pixels": CLOUD_FULL_MAX_PIXELS if mode == "full" else CLOUD_REDUCED_MAX_PIXELS,
        "maxPixels": CLOUD_FULL_MAX_PIXELS if mode == "full" else CLOUD_REDUCED_MAX_PIXELS,
        "full_image_webp_quality": CLOUD_HIGH_FULL_WEBP_QUALITY if quality_profile == CLOUD_QUALITY_PROFILE_HIGH else CLOUD_STANDARD_FULL_WEBP_QUALITY,
        "fullImageWebpQuality": CLOUD_HIGH_FULL_WEBP_QUALITY if quality_profile == CLOUD_QUALITY_PROFILE_HIGH else CLOUD_STANDARD_FULL_WEBP_QUALITY,
        "full_image_byte_cap": CLOUD_HIGH_FULL_BYTE_CAP if quality_profile == CLOUD_QUALITY_PROFILE_HIGH else CLOUD_STANDARD_FULL_BYTE_CAP,
        "fullImageByteCap": CLOUD_HIGH_FULL_BYTE_CAP if quality_profile == CLOUD_QUALITY_PROFILE_HIGH else CLOUD_STANDARD_FULL_BYTE_CAP,
        "full_image_webp_quality_attempts": build_full_image_webp_quality_attempts(quality_profile),
        "fullImageWebpQualityAttempts": build_full_image_webp_quality_attempts(quality_profile),
        "thumbnail_max_edge": CLOUD_THUMB_MAX_EDGE,
        "thumbnailMaxEdge": CLOUD_THUMB_MAX_EDGE,
        "thumbnail_webp_quality": CLOUD_THUMB_WEBP_QUALITY,
        "thumbnailWebpQuality": CLOUD_THUMB_WEBP_QUALITY,
        "thumbnail_jpeg_quality": CLOUD_THUMB_JPEG_QUALITY,
        "thumbnailJpegQuality": CLOUD_THUMB_JPEG_QUALITY,
    }
