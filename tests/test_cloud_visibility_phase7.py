from utils import cloud_sync


def test_phase7_visibility_normalization_maps_cloud_draft_to_local_private():
    assert cloud_sync._normalize_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("friends") == "friends"
    assert cloud_sync._cloud_visibility_to_sharing_scope("public") == "public"


def test_phase7_visibility_normalization_sends_private_visibility_to_cloud():
    assert cloud_sync._sharing_scope_to_cloud_visibility("private") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("draft") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("friends") == "friends"
    assert cloud_sync._sharing_scope_to_cloud_visibility("public") == "public"


def test_push_observation_sends_private_visibility_for_local_private(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: None)

    def fake_post(path, payload):
        posted_payloads.append((path, dict(payload)))
        return [{"id": "cloud-obs-1"}]

    monkeypatch.setattr(client, "_post", fake_post)

    cloud_id = client.push_observation(
        {
            "id": 7,
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "fuzzed",
            "uncertain": 0,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-1"
    assert posted_payloads[0][0] == "observations"
    assert posted_payloads[0][1]["visibility"] == "private"
    assert posted_payloads[0][1]["is_draft"] is True
    assert posted_payloads[0][1]["location_precision"] == "fuzzed"
    assert posted_payloads[0][1]["user_id"] == "user-123"
    assert posted_payloads[0][1]["desktop_id"] == 7


def test_push_observation_preserves_public_visibility(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: "cloud-obs-2")

    def fake_patch(path, payload):
        patched_payloads.append((path, dict(payload)))

    monkeypatch.setattr(client, "_patch", fake_patch)

    cloud_id = client.push_observation(
        {
            "id": 8,
            "sharing_scope": "public",
            "location_public": 1,
            "is_draft": False,
            "location_precision": "exact",
            "uncertain": 1,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-2"
    assert patched_payloads[0][0] == "observations?id=eq.cloud-obs-2"
    assert patched_payloads[0][1]["visibility"] == "public"
    assert patched_payloads[0][1]["location_public"] is True
    assert patched_payloads[0][1]["is_draft"] is False
    assert patched_payloads[0][1]["location_precision"] == "exact"
