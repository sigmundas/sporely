from utils.cloud_media_policy import (
    CLOUD_FULL_MAX_PIXELS,
    CLOUD_FULL_RESIZE_MAX_EDGE,
    CLOUD_FULL_RESIZE_MAX_PIXELS,
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
    assert free_policy["resizeMaxPixels"] == CLOUD_FULL_RESIZE_MAX_PIXELS
    assert free_policy["resizeMaxEdge"] == CLOUD_FULL_RESIZE_MAX_EDGE
    assert free_policy["fullImageWebpQuality"] == CLOUD_STANDARD_FULL_WEBP_QUALITY
    assert free_policy["fullImageByteCap"] == CLOUD_STANDARD_FULL_BYTE_CAP
    assert pro_policy["maxPixels"] == CLOUD_FULL_MAX_PIXELS
    assert pro_policy["resizeMaxPixels"] == CLOUD_FULL_RESIZE_MAX_PIXELS
    assert pro_policy["resizeMaxEdge"] == CLOUD_FULL_RESIZE_MAX_EDGE
    assert pro_policy["fullImageWebpQuality"] == CLOUD_HIGH_FULL_WEBP_QUALITY
    assert pro_policy["fullImageByteCap"] == CLOUD_HIGH_FULL_BYTE_CAP


def test_build_full_image_webp_quality_attempts_are_descending():
    assert build_full_image_webp_quality_attempts(CLOUD_QUALITY_PROFILE_STANDARD) == (65, 55, 45, 35, 25)
    assert build_full_image_webp_quality_attempts(CLOUD_QUALITY_PROFILE_HIGH) == (80, 70, 60, 50, 40)


def test_scale_dimensions_to_max_pixels_leaves_sub_threshold_full_images_unchanged():
    from utils.cloud_media_policy import scale_dimensions_to_max_pixels

    scaled = scale_dimensions_to_max_pixels(5184, 3888, CLOUD_FULL_RESIZE_MAX_PIXELS, CLOUD_FULL_RESIZE_MAX_EDGE)

    assert scaled["resized"] is False
    assert scaled["width"] == 5184
    assert scaled["height"] == 3888


def test_scale_dimensions_to_max_pixels_resizes_when_long_edge_exceeds_cap():
    from utils.cloud_media_policy import scale_dimensions_to_max_pixels

    scaled = scale_dimensions_to_max_pixels(6000, 4000, CLOUD_FULL_RESIZE_MAX_PIXELS, CLOUD_FULL_RESIZE_MAX_EDGE)

    assert scaled["resized"] is True
    assert max(scaled["width"], scaled["height"]) == CLOUD_FULL_RESIZE_MAX_EDGE
    assert scaled["width"] * scaled["height"] <= CLOUD_FULL_RESIZE_MAX_PIXELS


def test_scale_dimensions_to_max_pixels_resizes_when_pixel_area_exceeds_cap():
    from utils.cloud_media_policy import scale_dimensions_to_max_pixels

    scaled = scale_dimensions_to_max_pixels(5600, 5600, CLOUD_FULL_RESIZE_MAX_PIXELS, CLOUD_FULL_RESIZE_MAX_EDGE)

    assert scaled["resized"] is True
    assert scaled["width"] * scaled["height"] <= CLOUD_FULL_RESIZE_MAX_PIXELS
    assert max(scaled["width"], scaled["height"]) <= CLOUD_FULL_RESIZE_MAX_EDGE
