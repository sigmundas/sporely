from __future__ import annotations

from utils import cloud_sync


def _client(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-1")
    calls = {"get": [], "post": [], "patch": []}

    def fail_reverse_lookup(path):
        calls["get"].append(path)
        raise AssertionError(f"portable identity guard allowed reverse lookup: {path}")

    def post(path, payload):
        calls["post"].append((path, dict(payload)))
        return [{"id": f"new-{path}"}]

    monkeypatch.setattr(client, "_get", fail_reverse_lookup)
    monkeypatch.setattr(client, "_post", post)
    monkeypatch.setattr(
        client, "_patch", lambda path, payload: calls["patch"].append((path, dict(payload)))
    )
    return client, calls


def test_portable_observation_guard_forces_new_cloud_identity(monkeypatch):
    client, calls = _client(monkeypatch)
    observation = {
        "id": 91,
        "cloud_id": None,
        "date": "2026-08-27",
        "genus": "Amanita",
        "species": "muscaria",
        "sharing_scope": "public",
        "location_precision": "exact",
        "portable_cloud_identity_pending": 1,
        "sync_key": "7a6b5c4d-3e2f-4a1b-8c9d-0e1f2a3b4c5d",
    }

    cloud_id = client.push_observation(observation)

    assert cloud_id == "new-observations"
    assert calls["get"] == []
    assert calls["patch"] == []
    assert "desktop_id" not in calls["post"][0][1]
    assert calls["post"][0][1]["sync_key"] == observation["sync_key"]


def test_portable_image_guard_applies_to_child_identity(monkeypatch):
    client, calls = _client(monkeypatch)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_storage_exif_safe", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_sample_source", lambda: False)
    monkeypatch.setattr(client, "_set_observation_media_keys", lambda *args: None)
    monkeypatch.setattr(cloud_sync, "_apply_image_sample_fields_to_push_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda _image_id: "")
    monkeypatch.setattr(cloud_sync, "committed_parent_cloud_id", lambda table, row: "new-observation")
    image = {
        "id": 92,
        "observation_id": 91,
        "cloud_id": None,
        "filepath": "/tmp/imported.jpg",
        "image_type": "field",
        "portable_cloud_identity_pending": 1,
        "sync_key": "5d4c3b2a-1f0e-4d9c-8b7a-6f5e4d3c2b1a",
    }

    cloud_id = client.push_image_metadata(image, "new-observation", "user-1/key")

    assert cloud_id == "new-observation_images"
    assert calls["get"] == []
    assert calls["patch"] == []
    assert "desktop_id" not in calls["post"][0][1]
    assert calls["post"][0][1]["sync_key"] == image["sync_key"]


def test_portable_measurement_guard_ignores_desktop_cache_and_posts_without_reverse_id(
    monkeypatch,
):
    client, calls = _client(monkeypatch)
    monkeypatch.setattr(client, "_measurement_supports_media_keys", lambda: False)
    monkeypatch.setattr(cloud_sync, "committed_parent_cloud_id", lambda table, row: "new-cloud-image")
    measurement = {
        "id": 93,
        "cloud_id": None,
        "image_id": 92,
        "length_um": 10.0,
        "width_um": 5.0,
        "measurement_type": "manual",
        "portable_cloud_identity_pending": 1,
        "sync_key": "6e5d4c3b-2a1f-4e0d-9c8b-7a6f5e4d3c2b",
    }
    collision = {
        "id": "source-cloud-measurement",
        "desktop_id": 93,
        "image_id": "source-cloud-image",
    }

    cloud_id = client.push_measurement(
        measurement,
        "new-cloud-image",
        remote_measurement_cache={"desktop:93": collision},
    )

    assert cloud_id == "new-spore_measurements"
    assert calls["get"] == []
    assert calls["patch"] == []
    assert "desktop_id" not in calls["post"][0][1]


def test_portable_image_guard_selects_only_verified_direct_cache_identity():
    # desktop_id is retired as an identity: an unlinked image has no listed
    # candidate at all, and a linked one only its verified cloud_id row.
    direct_match = {"id": "fresh-image", "desktop_id": None}
    desktop_collision = {"id": "unrelated", "desktop_id": 92}
    existing_by_id = {"fresh-image": direct_match, "unrelated": desktop_collision}

    assert cloud_sync._select_remote_image_identity_candidate(
        local_cloud_id="",
        existing_by_id=existing_by_id,
    ) is None
    assert cloud_sync._select_remote_image_identity_candidate(
        local_cloud_id="fresh-image",
        existing_by_id=existing_by_id,
    ) == direct_match


def test_portable_image_guard_applies_to_public_spore_metadata_anchor(monkeypatch):
    client, calls = _client(monkeypatch)
    monkeypatch.setattr(
        cloud_sync, "microscope_image_requires_public_spore_anchor", lambda _id: True
    )
    monkeypatch.setattr(
        cloud_sync, "_cancel_microscope_anchor_tombstones", lambda *args: None
    )
    monkeypatch.setattr(cloud_sync, "_commit_anchor_link", lambda *args: None)
    monkeypatch.setattr(cloud_sync, "committed_parent_cloud_id", lambda table, row: "new-observation")
    monkeypatch.setattr(
        cloud_sync, "_set_cloud_image_metadata_only_state", lambda *args: None
    )
    monkeypatch.setattr(
        cloud_sync,
        "_metadata_only_microscope_image_payload",
        lambda _client, observation_id, row: {
            "observation_id": observation_id,
            "desktop_id": row["id"],
            "image_type": "microscope",
        },
    )

    cloud_id = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client,
        91,
        "new-observation",
        {
            "id": 92,
            "observation_id": 91,
            "cloud_id": None,
            "filepath": "",
            "image_type": "microscope",
            "portable_cloud_identity_pending": 1,
            "sync_key": "4c3b2a1f-0e9d-4c8b-a7a6-5f4e3d2c1b0a",
        },
        remote_images=[
            {
                "id": "unrelated-cloud-image",
                "desktop_id": 92,
                "observation_id": "new-observation",
                "image_type": "microscope",
            }
        ],
    )

    assert cloud_id == "new-observation_images"
    assert "desktop_id" not in calls["post"][0][1]


def test_pull_never_matches_a_portable_row_by_desktop_id():
    # Integer ids are never cloud identities: a cloud row naming local id 91
    # never pairs with it, portable or not. A cloud id link still does.
    imported = {
        "id": 91,
        "cloud_id": None,
        "portable_cloud_identity_pending": 1,
    }
    remote = {"id": "source-cloud-observation", "desktop_id": 91}

    assert cloud_sync._find_local_observation_for_remote_cached(
        remote, {}, {}
    ) == (None, cloud_sync.KEY_FREE)

    imported["cloud_id"] = "new-cloud-observation"
    local, outcome = cloud_sync._find_local_observation_for_remote_cached(
        {"id": "new-cloud-observation", "desktop_id": None},
        {"new-cloud-observation": imported},
        {},
    )
    assert local["id"] == 91 and outcome == cloud_sync.KEY_LINKED
