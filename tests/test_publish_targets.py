from utils.publish_targets import (
    PUBLISH_TARGET_ARTPORTALEN_SE,
    PUBLISH_TARGET_ARTSOBS_NO,
    compose_publish_notes,
    publish_target_from_country_code,
    sporely_public_observation_url,
)


def test_publish_target_from_country_code_maps_norway_and_sweden_only():
    assert publish_target_from_country_code("no") == PUBLISH_TARGET_ARTSOBS_NO
    assert publish_target_from_country_code("SE") == PUBLISH_TARGET_ARTPORTALEN_SE
    assert publish_target_from_country_code("dk") is None
    assert publish_target_from_country_code(None) is None


def test_sporely_public_observation_url_requires_public_non_draft_cloud_observation():
    public_observation = {
        "cloud_id": "849",
        "sharing_scope": "public",
        "is_draft": 0,
    }

    assert (
        sporely_public_observation_url(public_observation)
        == "https://sporely.no/observations/849"
    )
    assert sporely_public_observation_url({**public_observation, "cloud_id": None}) is None
    assert sporely_public_observation_url({**public_observation, "sharing_scope": "private"}) is None
    assert sporely_public_observation_url({**public_observation, "is_draft": 1}) is None


def test_compose_publish_notes_places_public_link_after_spore_dimensions():
    assert compose_publish_notes(
        "Found under spruce.",
        "Sporer: 8.0-10.0 x 4.0-5.0 µm",
        "https://sporely.no/observations/849",
        uploader_key="web",
    ) == (
        "Found under spruce.\n"
        "Sporer: 8.0-10.0 x 4.0-5.0 µm\n"
        "https://sporely.no/observations/849"
    )


def test_compose_publish_notes_uses_english_spores_label_for_inaturalist():
    assert compose_publish_notes(
        None,
        "Sporer: 8.0-10.0 x 4.0-5.0 µm",
        "https://sporely.no/observations/849",
        uploader_key="inat",
    ) == (
        "Spores: 8.0-10.0 x 4.0-5.0 µm\n"
        "https://sporely.no/observations/849"
    )
