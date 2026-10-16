"""Desktop edits of web-created rows must not rewrite unrelated cloud metadata.

Regression for Stage 8b run ``w2b-r2-2a-desktop-edit``: the desktop pulled a
web-created observation/image, the user edited the observation, and the
desktop push NULLed the image's upload metadata (``stored_bytes``,
``source_*``, ``stored_*``, ``upload_mode``), filled ``original_filename`` and
``captured_at`` the cloud never held, and turned NULL ``habitat_host_*`` into
``''``.

These tests run the real ``SporelyCloudClient`` push code; only the HTTP
transport (``_get`` / ``_patch``) is replaced.
"""

from __future__ import annotations

from utils import cloud_sync


USER_ID = "user-123"
UPLOAD_META = ("upload_mode", "source_width", "source_height", "stored_width", "stored_height", "stored_bytes")


def _client(monkeypatch, remote_rows: dict[str, list[dict]] | None = None):
    client = cloud_sync.SporelyCloudClient("token", USER_ID)
    patches: list[tuple[str, dict]] = []

    def fake_get(path):
        for prefix, rows in (remote_rows or {}).items():
            if path.startswith(prefix):
                return [dict(r) for r in rows]
        return []  # column-support probes: column exists

    def fake_patch(path, payload):
        patches.append((path, dict(payload)))
        return [{"id": "x"}]

    monkeypatch.setattr(client, "_get", fake_get)
    monkeypatch.setattr(client, "_patch", fake_patch)
    return client, patches


def _web_remote_image() -> dict:
    # Shape of web-created image 357 at the time of the desktop push.
    return {
        "id": "357",
        "observation_id": "366",
        "user_id": USER_ID,
        "deleted_at": None,
        "sort_order": 0,
        "image_type": "field",
        "objective_name": None,
        "captured_at": None,
        "measure_color": None,
        "crop_mode": None,
        "original_filename": None,
        "storage_path": f"{USER_ID}/366/0_abc.webp",
        "upload_mode": "full",
        "source_width": 540,
        "source_height": 960,
        "stored_width": 540,
        "stored_height": 960,
        "stored_bytes": 22946,
    }


def _pulled_local_image(**overrides) -> dict:
    # A local ImageDB row as pull + the edit dialog leave it: no upload-meta
    # columns exist locally; captured_at was injected from the observation
    # datetime; the dialog persisted UI defaults.
    row = {
        "id": 194,
        "observation_id": 201,
        "cloud_id": "357",
        "sort_order": 0,
        "image_type": "field",
        "objective_name": "",
        "captured_at": "2026-10-13 14:30:00",
        "measure_color": "#000000",
        "crop_mode": "ai",
        "filepath": "/tmp/profile/images/img.webp",
        "source_role": "cloud_recovery_cache",
        "file_purpose": None,
    }
    row.update(overrides)
    return row


def test_metadata_patch_of_pulled_image_keeps_web_upload_metadata(monkeypatch):
    client, patches = _client(monkeypatch)
    remote = _web_remote_image()

    cloud_id = client.push_image_metadata(
        _pulled_local_image(), "366", remote["storage_path"], remote_row=remote,
    )

    assert cloud_id == "357"
    image_patches = [(p, b) for p, b in patches if p.startswith("observation_images?")]
    assert len(image_patches) == 1
    path, body = image_patches[0]
    assert path.startswith("observation_images?id=eq.357")
    for key in UPLOAD_META:
        assert key not in body, key
    # Derived from the local recovery copy; the cloud never held them.
    assert "original_filename" not in body
    assert "captured_at" not in body
    # Blank text never replaces a cloud NULL.
    assert body["objective_name"] is None


def test_metadata_patch_still_sends_upload_metadata_the_uploader_supplied(monkeypatch):
    client, patches = _client(monkeypatch)
    remote = _web_remote_image()
    img = _pulled_local_image(
        source_role=None,
        upload_mode="reduced",
        stored_bytes=1234,
        original_filename="IMG_1.jpg",
    )

    client.push_image_metadata(img, "366", remote["storage_path"], remote_row=remote)

    body = patches[0][1]
    assert body["upload_mode"] == "reduced"
    assert body["stored_bytes"] == 1234
    # Not supplied by the caller: left at the cloud value.
    assert "source_width" not in body
    # A desktop-owned (non-recovery) image keeps its file facts.
    assert body["original_filename"] == "IMG_1.jpg"
    assert body["captured_at"] is not None


def test_new_image_create_payload_unchanged_for_desktop_rows():
    from utils.cloud_sync_impl.image_payloads import _preserve_unedited_remote_image_values

    # No remote row: nothing to preserve, payload untouched.
    payload = {"objective_name": "", "captured_at": "x", "original_filename": "a.jpg"}
    _preserve_unedited_remote_image_values(payload, {"source_role": "cloud_recovery_cache"}, None)
    assert payload == {"objective_name": "", "captured_at": "x", "original_filename": "a.jpg"}


def test_observation_patch_keeps_cloud_null_for_blank_local_text(monkeypatch):
    remote_obs = {
        "id": "366",
        "user_id": USER_ID,
        "date": "2026-10-13",
        "habitat_host_genus": None,
        "habitat_host_species": None,
        "open_comment": None,
        "notes": None,
        "visibility": "public",
        "is_draft": True,
        "location_precision": "exact",
    }
    client, patches = _client(
        monkeypatch, {"observations?id=eq.366": [remote_obs]},
    )
    local_obs = {
        "id": 201,
        "cloud_id": "366",
        "date": "2026-10-13",
        "habitat_host_genus": "",
        "habitat_host_species": "",
        "open_comment": "desktop edit",
        "notes": "",
        "sharing_scope": "public",
        "is_draft": 1,
        "location_precision": "exact",
    }

    client.push_observation(local_obs, remote_obs=remote_obs)

    obs_patches = [body for path, body in patches if path.startswith("observations?id=eq.366")]
    assert len(obs_patches) == 1
    body = obs_patches[0]
    assert body["open_comment"] == "desktop edit"
    assert body["habitat_host_genus"] is None
    assert body["habitat_host_species"] is None
    assert body["notes"] is None


def test_observation_patch_still_clears_a_cloud_value_to_blank(monkeypatch):
    # A real edit (cloud value -> blank) is still sent.
    remote_obs = {
        "id": "366", "user_id": USER_ID, "date": "2026-10-13",
        "habitat_host_genus": "Betula", "visibility": "public",
        "is_draft": True, "location_precision": "exact",
    }
    client, patches = _client(monkeypatch, {"observations?id=eq.366": [remote_obs]})
    local_obs = {
        "id": 201, "cloud_id": "366", "date": "2026-10-13",
        "habitat_host_genus": "", "sharing_scope": "public",
        "is_draft": 1, "location_precision": "exact",
    }

    client.push_observation(local_obs, remote_obs=remote_obs)

    body = [b for p, b in patches if p.startswith("observations?id=eq.366")][0]
    assert body["habitat_host_genus"] == ""


def test_metadata_only_drift_ignores_values_the_push_keeps_at_cloud(monkeypatch):
    """A pulled image whose only local differences are the pull-injected
    capture time and blank text is not drift: no PATCH (no-op write rule)."""
    local_image = _pulled_local_image(crop_mode=None, measure_color=None, synced_at="2026-10-16T00:00:00Z")
    remote_image = _web_remote_image()
    pushed = []

    class Client:
        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return True

        def push_image_metadata(self, image, observation_id, storage_path, *, remote_row=None):
            pushed.append(dict(image))
            return "357"

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _obs_id: [dict(local_image)])
    monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _obs_id: {})
    monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_args: True)

    skipped = cloud_sync._reconcile_metadata_only_linked_images(Client(), {"id": 201}, "366", [remote_image])

    assert skipped == {194}
    assert pushed == []


def test_metadata_only_drift_still_patches_a_real_local_edit(monkeypatch):
    local_image = _pulled_local_image(
        crop_mode=None, measure_color=None, notes="desktop note", synced_at="2026-10-16T00:00:00Z",
    )
    remote_image = _web_remote_image()
    pushed = []

    class Client:
        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return True

        def push_image_metadata(self, image, observation_id, storage_path, *, remote_row=None):
            pushed.append(dict(image))
            return "357"

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _obs_id: [dict(local_image)])
    monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _obs_id: {})
    monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_args: True)

    cloud_sync._reconcile_metadata_only_linked_images(Client(), {"id": 201}, "366", [remote_image])

    assert len(pushed) == 1
