from __future__ import annotations

import sqlite3

from database import schema
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
    }

    cloud_id = client.push_observation(observation)

    assert cloud_id == "new-observations"
    assert calls["get"] == []
    assert calls["patch"] == []
    assert "desktop_id" not in calls["post"][0][1]


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
    image = {
        "id": 92,
        "cloud_id": None,
        "filepath": "/tmp/imported.jpg",
        "image_type": "field",
        "portable_cloud_identity_pending": 1,
    }

    cloud_id = client.push_image_metadata(image, "new-observation", "user-1/key")

    assert cloud_id == "new-observation_images"
    assert calls["get"] == []
    assert calls["patch"] == []
    assert "desktop_id" not in calls["post"][0][1]


def test_portable_measurement_guard_ignores_desktop_cache_and_posts_without_reverse_id(
    monkeypatch,
):
    client, calls = _client(monkeypatch)
    monkeypatch.setattr(client, "_measurement_supports_media_keys", lambda: False)
    measurement = {
        "id": 93,
        "cloud_id": None,
        "image_id": 92,
        "length_um": 10.0,
        "width_um": 5.0,
        "measurement_type": "manual",
        "portable_cloud_identity_pending": 1,
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


def test_portable_image_guard_disables_preparation_time_desktop_reassociation(
    monkeypatch,
):
    client, _calls = _client(monkeypatch)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(
        client, "_observation_images_support_upload_metadata", lambda: False
    )
    monkeypatch.setattr(
        cloud_sync.ImageDB,
        "get_images_for_observation",
        lambda _observation_id: [
            {
                "id": 92,
                "cloud_id": None,
                "filepath": "/tmp/imported.jpg",
                "image_type": "field",
            }
        ],
    )
    monkeypatch.setattr(cloud_sync, "should_push_local_image_to_cloud", lambda _img: True)
    reconciled = []
    monkeypatch.setattr(
        cloud_sync,
        "_reconcile_local_image_cloud_id",
        lambda *args, **kwargs: reconciled.append((args, kwargs)) or True,
    )

    associated = cloud_sync._associate_persisted_cloud_images(
        client,
        {
            "id": 91,
            "cloud_id": "new-observation",
            "portable_cloud_identity_pending": 1,
        },
        [
            {
                "id": "unrelated-cloud-image",
                "desktop_id": 92,
                "observation_id": "new-observation",
                "image_type": "field",
            }
        ],
    )

    assert associated == set()
    assert reconciled == []


def test_portable_image_guard_selects_only_verified_direct_cache_identity():
    desktop_collision = {"id": "unrelated", "desktop_id": 92}
    direct_match = {"id": "fresh-image", "desktop_id": None}

    assert cloud_sync._select_remote_image_identity_candidate(
        local_image_id=92,
        local_cloud_id="",
        existing_by_id={"fresh-image": direct_match},
        existing_by_desktop_id={92: desktop_collision},
        portable_identity_pending=True,
    ) is None
    assert cloud_sync._select_remote_image_identity_candidate(
        local_image_id=92,
        local_cloud_id="fresh-image",
        existing_by_id={"fresh-image": direct_match},
        existing_by_desktop_id={92: desktop_collision},
        portable_identity_pending=True,
    ) == direct_match


def test_portable_image_guard_applies_to_public_spore_metadata_anchor(monkeypatch):
    client, calls = _client(monkeypatch)
    monkeypatch.setattr(
        cloud_sync, "microscope_image_requires_public_spore_anchor", lambda _id: True
    )
    monkeypatch.setattr(
        cloud_sync, "_cancel_microscope_anchor_tombstones", lambda *args: None
    )
    monkeypatch.setattr(
        cloud_sync, "_reconcile_local_image_cloud_id", lambda *args, **kwargs: True
    )
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
            "cloud_id": None,
            "filepath": "",
            "image_type": "microscope",
            "portable_cloud_identity_pending": 1,
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


def test_portable_guard_blocks_pull_side_observation_reverse_match():
    imported = {
        "id": 91,
        "cloud_id": None,
        "portable_cloud_identity_pending": 1,
    }
    remote = {"id": "source-cloud-observation", "desktop_id": 91}

    assert cloud_sync._find_local_observation_for_remote_cached(
        remote, {}, {91: imported}
    ) is None

    imported["cloud_id"] = "new-cloud-observation"
    assert cloud_sync._find_local_observation_for_remote_cached(
        {"id": "new-cloud-observation", "desktop_id": None},
        {"new-cloud-observation": imported},
        {91: imported},
    )["id"] == 91


def test_portable_guard_clears_only_after_entire_cloud_graph_has_identity(
    monkeypatch, tmp_path
):
    class IdentityClient:
        user_id = "user-1"

        def __init__(self, observation_collision=None):
            self.observation_collision = observation_collision
            self.linked = []

        def _find_cloud_observation(self, _desktop_id):
            return self.observation_collision

        def _find_cloud_image(self, _desktop_id, _observation_id, image_type=None):
            return None

        def _get(self, _path):
            return []

        def set_desktop_id(self, cloud_id, desktop_id):
            self.linked.append(("observation", cloud_id, desktop_id))

        def set_image_desktop_id(self, cloud_id, desktop_id):
            self.linked.append(("image", cloud_id, desktop_id))

        def set_measurement_desktop_id(self, cloud_id, desktop_id):
            self.linked.append(("measurement", cloud_id, desktop_id))

    client = IdentityClient()
    root = tmp_path / "app"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO observations "
            "(id, date, cloud_id, portable_cloud_identity_pending) "
            "VALUES (1, '2026-08-27', 'new-observation', 1)"
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type) "
            "VALUES (2, 1, '/tmp/imported.jpg', 'microscope')"
        )
        connection.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um) "
            "VALUES (3, 2, 10.0, 5.0)"
        )
        connection.commit()

    assert not cloud_sync._finalize_portable_cloud_identity_guard(client, 1)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE images SET cloud_id='new-image' WHERE id=2")
        connection.commit()
    assert not cloud_sync._finalize_portable_cloud_identity_guard(client, 1)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE spore_measurements SET cloud_id='new-measurement' WHERE id=3")
        connection.commit()

    assert cloud_sync._finalize_portable_cloud_identity_guard(client, 1)
    assert client.linked == [
        ("observation", "new-observation", 1),
        ("image", "new-image", 2),
        ("measurement", "new-measurement", 3),
    ]
    with sqlite3.connect(schema.get_database_path()) as connection:
        assert connection.execute(
            "SELECT portable_cloud_identity_pending FROM observations WHERE id=1"
        ).fetchone()[0] == 0


def test_portable_guard_stays_set_when_destination_reverse_id_collides(
    monkeypatch, tmp_path
):
    root = tmp_path / "app"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO observations "
            "(id, date, cloud_id, portable_cloud_identity_pending) "
            "VALUES (1, '2026-08-27', 'new-observation', 1)"
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, image_type, cloud_id) "
            "VALUES (2, 1, '/tmp/imported.jpg', 'field', 'new-image')"
        )
        connection.commit()

    class CollisionClient:
        user_id = "user-1"

        def __init__(self):
            self.linked = []

        def _find_cloud_observation(self, _desktop_id):
            return None

        def _get(self, path):
            if path.startswith("observation_images?"):
                return [{"id": "source-cloud-image"}]
            return []

        def set_desktop_id(self, cloud_id, desktop_id):
            self.linked.append((cloud_id, desktop_id))

        def set_image_desktop_id(self, cloud_id, desktop_id):
            self.linked.append((cloud_id, desktop_id))

    client = CollisionClient()

    assert not cloud_sync._finalize_portable_cloud_identity_guard(client, 1)
    assert client.linked == []
    with sqlite3.connect(schema.get_database_path()) as connection:
        assert connection.execute(
            "SELECT portable_cloud_identity_pending FROM observations WHERE id=1"
        ).fetchone()[0] == 1
