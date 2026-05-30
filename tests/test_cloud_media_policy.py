from utils.cloud_media_policy import (
    CLOUD_FULL_MAX_PIXELS,
    CLOUD_HIGH_FULL_BYTE_CAP,
    CLOUD_HIGH_FULL_WEBP_QUALITY,
    CLOUD_QUALITY_PROFILE_HIGH,
    CLOUD_QUALITY_PROFILE_STANDARD,
    CLOUD_STANDARD_FULL_BYTE_CAP,
    CLOUD_STANDARD_FULL_WEBP_QUALITY,
    build_cloud_upload_policy,
    build_full_image_webp_quality_attempts,
    normalize_cloud_plan_profile,
)


def test_normalize_cloud_plan_profile_maps_free_and_pro():
    free_profile = normalize_cloud_plan_profile({"cloud_plan": "free"})
    pro_profile = normalize_cloud_plan_profile({"cloud_plan": "pro", "is_pro": True})

    assert free_profile["cloudPlan"] == "free"
    assert free_profile["qualityProfile"] == CLOUD_QUALITY_PROFILE_STANDARD
    assert free_profile["has_pro_access"] is False
    assert pro_profile["cloudPlan"] == "pro"
    assert pro_profile["qualityProfile"] == CLOUD_QUALITY_PROFILE_HIGH
    assert pro_profile["has_pro_access"] is True


def test_build_cloud_upload_policy_uses_expected_quality_and_caps():
    free_policy = build_cloud_upload_policy(normalize_cloud_plan_profile({"cloud_plan": "free"}), upload_mode="full")
    pro_policy = build_cloud_upload_policy(normalize_cloud_plan_profile({"cloud_plan": "pro", "is_pro": True}), upload_mode="full")

    assert free_policy["uploadMode"] == "full"
    assert pro_policy["uploadMode"] == "full"
    assert free_policy["maxPixels"] == CLOUD_FULL_MAX_PIXELS
    assert free_policy["fullImageWebpQuality"] == CLOUD_STANDARD_FULL_WEBP_QUALITY
    assert free_policy["fullImageByteCap"] == CLOUD_STANDARD_FULL_BYTE_CAP
    assert pro_policy["maxPixels"] == CLOUD_FULL_MAX_PIXELS
    assert pro_policy["fullImageWebpQuality"] == CLOUD_HIGH_FULL_WEBP_QUALITY
    assert pro_policy["fullImageByteCap"] == CLOUD_HIGH_FULL_BYTE_CAP


def test_build_full_image_webp_quality_attempts_are_descending():
    assert build_full_image_webp_quality_attempts(CLOUD_QUALITY_PROFILE_STANDARD) == (65, 55, 45, 35, 25)
    assert build_full_image_webp_quality_attempts(CLOUD_QUALITY_PROFILE_HIGH) == (80, 70, 60, 50, 40)
