from utils.publish_targets import (
    PUBLISH_TARGET_ARTPORTALEN_SE,
    PUBLISH_TARGET_ARTSOBS_NO,
    publish_target_from_country_code,
)


def test_publish_target_from_country_code_maps_norway_and_sweden_only():
    assert publish_target_from_country_code("no") == PUBLISH_TARGET_ARTSOBS_NO
    assert publish_target_from_country_code("SE") == PUBLISH_TARGET_ARTPORTALEN_SE
    assert publish_target_from_country_code("dk") is None
    assert publish_target_from_country_code(None) is None
