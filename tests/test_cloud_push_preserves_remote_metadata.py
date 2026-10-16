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
    _record_injected(194, "2026-10-13 14:30:00")
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
        id=500,
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
    _record_injected(194, "2026-10-13 14:30:00")
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


def _record_injected(local_image_id: int, value: str) -> None:
    from database.models import SettingsDB

    SettingsDB.set_setting(f"cloud_image_pull_injected_captured_at:{local_image_id}", value)


def test_user_set_capture_time_on_pulled_image_is_pushed(monkeypatch):
    _record_injected(194, "2026-10-13 14:30:00")
    client, patches = _client(monkeypatch)
    remote = _web_remote_image()

    client.push_image_metadata(
        _pulled_local_image(captured_at="2026-10-13 09:15:00"), "366",
        remote["storage_path"], remote_row=remote,
    )

    body = [b for p, b in patches if p.startswith("observation_images?")][0]
    assert body["captured_at"] == cloud_sync._normalize_image_captured_at_for_cloud(
        "2026-10-13 09:15:00", local=True,
    )


def test_recorded_pull_injected_capture_time_is_not_pushed(monkeypatch):
    _record_injected(194, "2026-10-13 14:30:00")
    client, patches = _client(monkeypatch)
    remote = _web_remote_image()

    client.push_image_metadata(
        _pulled_local_image(), "366", remote["storage_path"], remote_row=remote,
    )

    body = [b for p, b in patches if p.startswith("observation_images?")][0]
    assert "captured_at" not in body


def test_user_set_capture_time_is_drift(monkeypatch):
    _record_injected(194, "2026-10-13 14:30:00")
    local_image = _pulled_local_image(
        crop_mode=None, measure_color=None, captured_at="2026-10-13 09:15:00",
        synced_at="2026-10-16T00:00:00Z",
    )
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

    cloud_sync._reconcile_metadata_only_linked_images(Client(), {"id": 201}, "366", [_web_remote_image()])

    assert len(pushed) == 1


def test_image_text_with_cloud_value_can_be_cleared(monkeypatch):
    client, patches = _client(monkeypatch)
    remote = dict(_web_remote_image(), notes="web note", objective_name="40x")

    client.push_image_metadata(
        _pulled_local_image(notes="", objective_name=""), "366",
        remote["storage_path"], remote_row=remote,
    )

    body = [b for p, b in patches if p.startswith("observation_images?")][0]
    assert body["notes"] == ""
    assert body["objective_name"] == ""


def test_legacy_pulled_copy_without_record_falls_back_to_observation_datetime(monkeypatch):
    """Copies pulled before the record existed: the injected value is the
    observation datetime's wall-clock text, either as pulled (UTC offset
    dropped) or after the editor re-saved it as naive local time."""
    from datetime import datetime, timezone

    from utils.cloud_sync_impl import exif

    monkeypatch.setattr(exif.SettingsDB, "get_setting", staticmethod(lambda key, default=None: default))
    img = _pulled_local_image(id=9001, captured_at="2026-10-13 14:30:00")

    monkeypatch.setattr(exif, "_load_obs_exif_fallback", lambda _oid, **_k: (None,) * 4 + ("2026-10-13T14:30:00+00:00",))
    assert exif._captured_at_is_pull_injected(img) is True

    naive_local = (
        datetime(2026, 10, 13, 14, 30, tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    monkeypatch.setattr(exif, "_load_obs_exif_fallback", lambda _oid, **_k: (None,) * 4 + (naive_local,))
    assert exif._captured_at_is_pull_injected(img) is True

    assert exif._captured_at_is_pull_injected(dict(img, captured_at="2026-10-13 09:15:00")) is False
    # Not a recovery copy: never treated as injected without a record.
    assert exif._captured_at_is_pull_injected(dict(img, source_role=None, file_purpose=None)) is False


# ---------------------------------------------------------------------------
# Cloud-only image edits are adopted locally before the post-push baseline
# ---------------------------------------------------------------------------

import json  # noqa: E402

from database.schema import get_connection  # noqa: E402
from utils.cloud_sync_impl import baseline as baseline_mod  # noqa: E402
from utils.cloud_sync_impl import image_push  # noqa: E402


def _insert_local_pulled_image(obs_id: int, image_id: int, cloud_image_id: str, notes, stain=None) -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM images WHERE id = ?", (image_id,))
        conn.execute("DELETE FROM observations WHERE id = ?", (obs_id,))
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, date) VALUES (?, ?, 'synced', '2026-10-13')",
            (obs_id, f"cloud-obs-{obs_id}"),
        )
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, sort_order, notes, stain, "
            "synced_at, source_role, file_purpose) VALUES (?, ?, ?, ?, 'field', 0, ?, ?, ?, "
            "'cloud_recovery_cache', 'cache')",
            (image_id, obs_id, cloud_image_id, f"/tmp/{image_id}.webp", notes, stain, "2026-10-16T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()


def _local_image(image_id: int) -> dict:
    conn = get_connection()
    try:
        row = conn.execute("SELECT notes, stain FROM images WHERE id = ?", (image_id,)).fetchone()
        status = conn.execute(
            "SELECT o.sync_status FROM observations o JOIN images i ON i.observation_id = o.id WHERE i.id = ?",
            (image_id,),
        ).fetchone()
    finally:
        conn.close()
    return {"notes": row[0], "stain": row[1], "obs_status": status[0]}


def _cloud_image(cloud_image_id: str, obs_cloud_id: str, **fields) -> dict:
    row = {
        "id": cloud_image_id, "observation_id": obs_cloud_id, "sort_order": 0, "image_type": "field",
        "notes": None, "stain": None, "captured_at": None,
        "storage_path": f"{USER_ID}/{obs_cloud_id}/{cloud_image_id}.webp",
    }
    row.update(fields)
    return row


def _store_baseline(obs_cloud_id: str, images: list[dict]) -> None:
    baseline_mod._store_cloud_observation_snapshot(
        obs_cloud_id, json.dumps({"schema_version": 2, "observation": {"id": obs_cloud_id}, "images": images, "measurements": []}),
    )


class _SnapshotClient:
    user_id = USER_ID

    def __init__(self, obs_cloud_id, images):
        self._obs = {"id": obs_cloud_id, "user_id": USER_ID}
        self._images = images

    def get_observation(self, _cid):
        return dict(self._obs)

    def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
        return [dict(r) for r in self._images]

    def _observation_images_support_ai_crop(self):
        return False


def _run_post_push_baseline(obs_cloud_id, obs_local_id, remote_images):
    client = _SnapshotClient(obs_cloud_id, remote_images)
    baseline_mod._store_remote_snapshot(
        client, obs_cloud_id, remote_measurements=[],
        before_store_images=lambda rows: image_push._adopt_remote_image_edits_before_baseline(
            client, obs_cloud_id, obs_local_id, rows,
        ),
    )
    return json.loads(baseline_mod._load_cloud_observation_snapshot(obs_cloud_id))


def test_web_image_edit_reaches_local_row_and_baseline_without_dirtying():
    _insert_local_pulled_image(9101, 9201, "c-9201", notes="old note")
    _store_baseline("cloud-obs-9101", [_cloud_image("c-9201", "cloud-obs-9101", notes="old note")])

    snapshot = _run_post_push_baseline(
        "cloud-obs-9101", 9101, [_cloud_image("c-9201", "cloud-obs-9101", notes="web note")],
    )

    assert _local_image(9201) == {"notes": "web note", "stain": None, "obs_status": "synced"}
    assert snapshot["images"][0]["notes"] == "web note"


def test_concurrent_local_edit_of_same_field_is_not_adopted_and_conflicts(monkeypatch):
    _insert_local_pulled_image(9102, 9202, "c-9202", notes="desktop note")
    base = _cloud_image("c-9202", "cloud-obs-9102", notes="old note")
    _store_baseline("cloud-obs-9102", [base])

    _run_post_push_baseline("cloud-obs-9102", 9102, [_cloud_image("c-9202", "cloud-obs-9102", notes="web note")])
    assert _local_image(9202)["notes"] == "desktop note"

    monkeypatch.setattr(cloud_sync, "_local_tombstoned_cloud_image_ids", lambda *_a, **_k: set())
    report = cloud_sync._analyze_observation_push_conflicts(
        local_obs={"id": 9102, "cloud_id": "cloud-obs-9102"},
        local_images=[dict(cloud_sync.ImageDB.get_images_for_observation(9102)[0])],
        local_measurements_by_cloud_id={},
        remote_obs={"id": "cloud-obs-9102"},
        remote_images=[_cloud_image("c-9202", "cloud-obs-9102", notes="web note")],
        remote_measurements=[],
        baseline_snapshot={"observation": {"id": "cloud-obs-9102"}, "images": [base], "measurements": []},
    )
    assert report.image_conflict_keys == ["cloud:c-9202"]


def test_local_edit_of_other_field_is_pushed_and_web_edit_kept(monkeypatch):
    _insert_local_pulled_image(9103, 9203, "c-9203", notes="old note", stain="Melzer")
    _store_baseline("cloud-obs-9103", [_cloud_image("c-9203", "cloud-obs-9103", notes="old note")])
    remote = _cloud_image("c-9203", "cloud-obs-9103", notes="web note")
    client, patches = _client(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _obs_id: {})
    monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_args: True)

    cloud_sync._reconcile_metadata_only_linked_images(client, {"id": 9103}, "cloud-obs-9103", [remote])

    bodies = [b for p, b in patches if p.startswith("observation_images?id=eq.c-9203")]
    assert len(bodies) == 1
    assert bodies[0]["stain"] == "Melzer"
    assert "notes" not in bodies[0]

    # The post-push baseline then brings the web note into the local row.
    _run_post_push_baseline("cloud-obs-9103", 9103, [dict(remote, stain="Melzer")])
    assert _local_image(9203) == {"notes": "web note", "stain": "Melzer", "obs_status": "synced"}


def _reconcile_with_baseline(monkeypatch, local_image: dict, baseline_image: dict, remote_image: dict, obs_cloud_id: str):
    _store_baseline(obs_cloud_id, [baseline_image])
    client, patches = _client(monkeypatch)
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _obs_id: [dict(local_image)])
    monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _obs_id: {})
    monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_args: True)
    cloud_sync._reconcile_metadata_only_linked_images(client, {"id": local_image["observation_id"]}, obs_cloud_id, [remote_image])
    return [b for p, b in patches if p.startswith("observation_images?")]


def test_blank_local_text_against_null_baseline_does_not_overwrite_web_edit(monkeypatch):
    local = _pulled_local_image(
        id=9301, observation_id=9401, cloud_id="c-9301", captured_at=None, crop_mode=None,
        measure_color=None, notes="", stain="Melzer", synced_at="2026-10-16T00:00:00Z",
    )
    base = _cloud_image("c-9301", "o-9401", notes=None)
    remote = _cloud_image("c-9301", "o-9401", notes="web note")

    bodies = _reconcile_with_baseline(monkeypatch, local, base, remote, "o-9401")

    assert len(bodies) == 1  # the real local edit (stain) is pushed
    assert bodies[0]["stain"] == "Melzer"
    assert "notes" not in bodies[0]


def test_injected_capture_time_against_null_baseline_does_not_overwrite_web_capture_time(monkeypatch):
    _record_injected(9302, "2026-10-13 14:30:00")
    local = _pulled_local_image(
        id=9302, observation_id=9402, cloud_id="c-9302", crop_mode=None, measure_color=None,
        stain="Melzer", synced_at="2026-10-16T00:00:00Z",
    )
    base = _cloud_image("c-9302", "o-9402")
    remote = _cloud_image("c-9302", "o-9402", captured_at="2026-10-01T08:00:00+00:00")

    bodies = _reconcile_with_baseline(monkeypatch, local, base, remote, "o-9402")

    assert len(bodies) == 1
    assert "captured_at" not in bodies[0]


def test_cloud_held_capture_time_record_stops_legacy_fallback(monkeypatch):
    from utils.cloud_sync_impl import exif

    _record_injected(9303, exif._CLOUD_HELD_CAPTURED_AT)
    monkeypatch.setattr(exif, "_load_obs_exif_fallback", lambda _oid, **_k: (None,) * 4 + ("2026-10-13T14:30:00+00:00",))
    img = _pulled_local_image(id=9303, captured_at="2026-10-13 14:30:00")

    assert exif._captured_at_is_pull_injected(img) is False


# ---------------------------------------------------------------------------
# The choke point: push_image_metadata enforces the three-way check itself,
# whichever caller (Refresh, upload/replace, preparation, resolution) sent it.
# ---------------------------------------------------------------------------

import pytest  # noqa: E402

from utils.cloud_sync_impl.reconciliation.values import _normalize_image_captured_at_for_cloud  # noqa: E402


def _db_image(image_id: int) -> dict:
    conn = get_connection()
    try:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute("SELECT * FROM images WHERE id = ?", (image_id,)).fetchone()
    finally:
        conn.close()
    return dict(row)


def _set_local(image_id: int, **fields) -> None:
    conn = get_connection()
    try:
        conn.execute(
            f"UPDATE images SET {', '.join(f'{k} = ?' for k in fields)} WHERE id = ?",
            [*fields.values(), image_id],
        )
        conn.commit()
    finally:
        conn.close()


def _image_bodies(patches, cloud_image_id):
    return [b for p, b in patches if p.startswith(f"observation_images?id=eq.{cloud_image_id}")]


def test_refresh_patch_without_remote_row_keeps_web_notes_and_cloud_null(monkeypatch):
    # Codex finding 1: Refresh (cloud_sync.py, sync_images=False) calls
    # push_image_metadata(img, cloud_id, storage_path) with no remote row.
    _insert_local_pulled_image(9501, 9601, "c-9601", notes="old note", stain="")
    base = _cloud_image("c-9601", "cloud-obs-9501", notes="old note")
    _store_baseline("cloud-obs-9501", [base])
    remote = dict(base, notes="web note")
    client, patches = _client(monkeypatch, {"observation_images?id=eq.c-9601": [remote]})

    client.push_image_metadata(_db_image(9601), "cloud-obs-9501", "")

    body = _image_bodies(patches, "c-9601")[0]
    assert "notes" not in body
    assert body["stain"] is None  # blank local text never turns cloud NULL into ''


def test_replacing_bytes_keeps_web_notes(monkeypatch):
    # Codex finding 2: the prepared-image path PATCHes the new key with the
    # remote row but no keep_remote_fields.
    _insert_local_pulled_image(9502, 9602, "c-9602", notes="old note")
    base = _cloud_image("c-9602", "cloud-obs-9502", notes="old note")
    _store_baseline("cloud-obs-9502", [base])
    remote = dict(base, notes="web note")
    client, patches = _client(monkeypatch, {"observation_images?id=eq.c-9602": [remote]})
    new_key = f"{USER_ID}/cloud-obs-9502/c-9602-v2.webp"

    client.push_image_metadata(_db_image(9602), "cloud-obs-9502", new_key, remote_row=remote)

    body = _image_bodies(patches, "c-9602")[0]
    assert body["storage_path"] == new_key
    assert "notes" not in body


def test_desktop_edit_is_still_sent_by_the_choke_point(monkeypatch):
    _insert_local_pulled_image(9503, 9603, "c-9603", notes="")
    base = _cloud_image("c-9603", "cloud-obs-9503", notes="old note")
    _store_baseline("cloud-obs-9503", [base])
    client, patches = _client(monkeypatch, {"observation_images?id=eq.c-9603": [base]})

    client.push_image_metadata(_db_image(9603), "cloud-obs-9503", "")

    # A deliberate clear of a cloud value is sent.
    assert _image_bodies(patches, "c-9603")[0]["notes"] == ""


def test_web_capture_time_survives_two_pushes(monkeypatch):
    # Codex finding 3: web sets captured_at over a NULL baseline; the local
    # value is the pull-injected one.
    _insert_local_pulled_image(9504, 9604, "c-9604", notes=None)
    _set_local(9604, captured_at="2026-10-13 14:30:00")
    _record_injected(9604, "2026-10-13 14:30:00")
    base = _cloud_image("c-9604", "cloud-obs-9504")
    _store_baseline("cloud-obs-9504", [base])
    remote = dict(base, captured_at="2026-10-01T08:00:00+00:00")
    client, patches = _client(monkeypatch, {"observation_images?id=eq.c-9604": [remote]})

    client.push_image_metadata(_db_image(9604), "cloud-obs-9504", "")
    _run_post_push_baseline("cloud-obs-9504", 9504, [remote])
    client.push_image_metadata(_db_image(9604), "cloud-obs-9504", "")

    first, second = _image_bodies(patches, "c-9604")
    assert "captured_at" not in first
    assert _normalize_image_captured_at_for_cloud(second.get("captured_at", remote["captured_at"]), local=False) \
        == _normalize_image_captured_at_for_cloud(remote["captured_at"], local=False)


class _AiCropSnapshotClient(_SnapshotClient):
    def _observation_images_support_ai_crop(self):
        return True


# field: (local baseline value, cloud baseline value, cloud web value)
_ADOPTED_FIELD_CASES = {
    "notes": ("b", "b", "w"),
    "objective_name": ("10x", "10x", "40x"),
    "measure_color": ("#111111", "#111111", "#222222"),
    "mount_medium": ("Water", "Water", "KOH"),
    "stain": ("Melzer", "Melzer", "Congo"),
    "sample_type": ("Fresh", "Fresh", "Dried"),
    "sample_source": ("Hymenium", "hymenium", "spore_print"),
    "contrast": ("BF", "BF", "DIC"),
    "crop_mode": ("full", "full", "ai"),
    "micro_category": ("spores", "spores", "basidia"),
    "captured_at": (None, None, "2026-10-01T08:00:00+00:00"),
    "sort_order": (0, 0, 2),
    "image_type": ("field", "field", "microscope"),
    "scale_microns_per_pixel": (0.5, 0.5, 0.25),
    "resample_scale_factor": (1.0, 1.0, 0.5),
    "gps_source": (0, False, True),
    "ai_crop_x1": (1.0, 1.0, 2.0),
    "ai_crop_y1": (1.0, 1.0, 2.0),
    "ai_crop_x2": (5.0, 5.0, 6.0),
    "ai_crop_y2": (5.0, 5.0, 6.0),
    "ai_crop_source_w": (100, 100, 200),
    "ai_crop_source_h": (100, 100, 200),
    "ai_crop_is_custom": (0, False, True),
}


def test_every_protected_field_has_a_second_push_case():
    assert set(_ADOPTED_FIELD_CASES) == set(image_push._ADOPTABLE_IMAGE_FIELDS)


@pytest.mark.parametrize("field", sorted(_ADOPTED_FIELD_CASES))
def test_adopted_field_survives_the_second_push(monkeypatch, field):
    local_value, base_value, web_value = _ADOPTED_FIELD_CASES[field]
    obs_id, image_id, cloud_image_id, obs_cloud = 9700, 9800, "c-9800", "cloud-obs-9700"
    _insert_local_pulled_image(obs_id, image_id, cloud_image_id, notes=None)
    _set_local(image_id, **{field: local_value})
    base = _cloud_image(cloud_image_id, obs_cloud, **{field: base_value})
    _store_baseline(obs_cloud, [base])
    remote = dict(base, **{field: web_value})
    client, patches = _client(monkeypatch, {f"observation_images?id=eq.{cloud_image_id}": [remote]})

    client.push_image_metadata(_db_image(image_id), obs_cloud, "")
    snapshot_client = _AiCropSnapshotClient(obs_cloud, [remote])
    baseline_mod._store_remote_snapshot(
        snapshot_client, obs_cloud, remote_measurements=[],
        before_store_images=lambda rows: image_push._adopt_remote_image_edits_before_baseline(
            snapshot_client, obs_cloud, obs_id, rows,
        ),
    )
    local_after = _db_image(image_id)
    client.push_image_metadata(local_after, obs_cloud, "")

    first, second = _image_bodies(patches, cloud_image_id)
    assert field not in first
    # The local row now holds the cloud value, so the second push is not a
    # desktop edit of it.
    local_cmp = image_push._prepared_item_remote_payload(local_after, local_after["filepath"], "")
    remote_cmp = image_push._remote_image_compare_payload(remote)
    assert local_cmp[field] == remote_cmp[field]
    assert image_push._image_fields_to_keep_at_cloud(client, local_after, obs_cloud, cloud_image_id, remote) == set()
    stale = image_push._prepared_item_remote_payload(
        dict(local_after, **{field: local_value}), local_after["filepath"], "",
    )[field]
    if field in second:
        assert second[field] != stale or stale == remote_cmp[field]
