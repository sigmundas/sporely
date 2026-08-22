"""Image push identity resolution.

Regression coverage for the Download-from-Cloud duplicate-image bug:
``push_image_metadata`` used to resolve identity only via the remote reverse
link (``observation_images.desktop_id``). Images imported by pull-only sync
legitimately carry a local ``cloud_id`` while the remote ``desktop_id`` is
still NULL (pull-only performs zero cloud writes), so the reverse lookup
found nothing and a later normal push POSTed a duplicate cloud image.

The canonical resolver under test is
``SporelyCloudClient._resolve_existing_image_for_push``:

* verified local ``cloud_id`` is the primary identity (PATCH, never POST);
* remote ``desktop_id`` in the same observation is the recovery identity;
* direct/reverse disagreement or an ambiguous reverse match raises
  ``ImageIdentityConflictError`` and must leave the image dirty/retryable.
"""

from __future__ import annotations

import re

import pytest

from utils import cloud_sync


# ---------------------------------------------------------------------------
# Fake user-scoped PostgREST image store
# ---------------------------------------------------------------------------


class _FakeImageCloud:
    """Serves the two identity lookups from an in-memory image list.

    Handles:
    - Direct GET: ``observation_images?id=eq.X&user_id=eq.U&select=id,desktop_id,deleted_at,observation_id&limit=1``
    - Reverse GET: ``observation_images?desktop_id=eq.X&user_id=eq.U&observation_id=eq.OBS&select=id,deleted_at&order=id.asc``
    """

    def __init__(self, rows: list[dict], current_user: str = "user-123"):
        self.rows = [dict(r) for r in rows]
        self.current_user = current_user
        self.get_paths: list[str] = []

    def get(self, path: str) -> list[dict]:
        self.get_paths.append(path)
        if not path.startswith("observation_images?"):
            raise AssertionError(f"unexpected GET path: {path}")

        def _eq(name: str):
            match = re.search(rf"[?&]{name}=eq\.([^&]+)", path)
            return match.group(1) if match else None

        user_id = _eq("user_id")
        assert user_id == self.current_user, "identity lookups must stay user-scoped"

        matches = [dict(r) for r in self.rows if str(r.get("user_id")) == user_id]

        direct_id = _eq("id")
        if direct_id is not None:
            matches = [r for r in matches if str(r.get("id")) == direct_id]

        desktop_id = _eq("desktop_id")
        if desktop_id is not None:
            matches = [r for r in matches if str(r.get("desktop_id") or "") == desktop_id]

        obs_id = _eq("observation_id")
        if obs_id is not None:
            matches = [r for r in matches if str(r.get("observation_id") or "") == obs_id]

        matches.sort(key=lambda r: str(r.get("id")))

        if "&limit=1" in path:
            matches = matches[:1]

        # Return only selected fields
        if "select=id,deleted_at" in path and "observation_id" not in path.split("select=")[1].split("&")[0]:
            return [{"id": r["id"], "deleted_at": r.get("deleted_at")} for r in matches]
        if "select=id,desktop_id,deleted_at,observation_id" in path:
            return [{k: r.get(k) for k in ("id", "desktop_id", "deleted_at", "observation_id")} for r in matches]

        return matches


def _make_client(monkeypatch, remote_rows: list[dict], *, user: str = "user-123") -> tuple[cloud_sync.SporelyCloudClient, dict]:
    client = cloud_sync.SporelyCloudClient("token", user)
    cloud = _FakeImageCloud(remote_rows, current_user=user)
    calls: dict[str, list] = {"patch": [], "post": [], "cloud": cloud}
    monkeypatch.setattr(client, "_get", cloud.get)
    monkeypatch.setattr(
        client, "_patch", lambda path, payload: calls["patch"].append((path, dict(payload)))
    )

    def _fake_post(path, payload):
        calls["post"].append((path, dict(payload)))
        return [{"id": "new-cloud-image-id"}]

    monkeypatch.setattr(client, "_post", _fake_post)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_storage_exif_safe", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_sample_source", lambda: False)
    monkeypatch.setattr(client, "_set_observation_media_keys", lambda *a, **kw: None)
    monkeypatch.setattr(cloud_sync, "_apply_image_sample_fields_to_push_payload", lambda *a, **kw: None)
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda image_id: "")
    monkeypatch.setattr(cloud_sync, "_clear_explicit_image_restore_source", lambda image_id: None)
    return client, calls


def _local_img(local_id: int, cloud_id: str | None = None, **overrides) -> dict:
    img = {
        "id": local_id,
        "cloud_id": cloud_id,
        "filepath": "/tmp/img.jpg",
        "image_type": "field",
        "sort_order": 0,
    }
    img.update(overrides)
    return img


OBS_CLOUD_ID = "cloud-obs-100"
USER = "user-123"


# ---------------------------------------------------------------------------
# Test 1: local cloud_id valid, remote desktop_id NULL → PATCH existing row
# ---------------------------------------------------------------------------


def test_local_cloud_id_valid_remote_desktop_id_null(monkeypatch):
    """Rule A: verified direct link wins even when remote desktop_id is NULL."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-200", "desktop_id": None, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None}],
    )

    cloud_id = client.push_image_metadata(_local_img(42, cloud_id="img-200"), OBS_CLOUD_ID, "")

    assert cloud_id == "img-200"
    patch_paths = [p for p, _ in calls["patch"]]
    assert any("observation_images?id=eq.img-200" in p for p in patch_paths)
    assert all("user_id=eq." in p for p in patch_paths), "PATCH must include owner filter"
    assert calls["post"] == [], "verified direct identity must never POST"


# ---------------------------------------------------------------------------
# Test 2: local cloud_id valid, reverse points elsewhere → ImageIdentityConflictError
# ---------------------------------------------------------------------------


def test_local_cloud_id_valid_reverse_points_elsewhere(monkeypatch):
    """Rule C: direct → img-200, reverse → img-201 → conflict."""
    client, calls = _make_client(
        monkeypatch,
        [
            {"id": "img-200", "desktop_id": None, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
            {"id": "img-201", "desktop_id": 42, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
        ],
    )

    with pytest.raises(cloud_sync.ImageIdentityConflictError):
        client.push_image_metadata(_local_img(42, cloud_id="img-200"), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 3: no cloud_id, unique desktop_id in SAME observation → reuse (PATCH)
# ---------------------------------------------------------------------------


def test_no_cloud_id_unique_desktop_id_same_observation(monkeypatch):
    """Rule B: no direct link + exactly one reverse match in same observation → PATCH."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-300", "desktop_id": 77, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None}],
    )

    cloud_id = client.push_image_metadata(_local_img(77, cloud_id=None), OBS_CLOUD_ID, "")

    assert cloud_id == "img-300"
    assert any("observation_images?id=eq.img-300" in p for p, _ in calls["patch"])
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 4: no cloud_id, unique desktop_id in ANOTHER observation → POST
# ---------------------------------------------------------------------------


def test_no_cloud_id_unique_desktop_id_other_observation(monkeypatch):
    """Reverse lookup is scoped to the target observation; other-obs rows are invisible."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-300", "desktop_id": 77, "user_id": USER, "observation_id": "cloud-obs-OTHER", "deleted_at": None}],
    )

    cloud_id = client.push_image_metadata(_local_img(77, cloud_id=None), OBS_CLOUD_ID, "")

    assert cloud_id == "new-cloud-image-id"
    assert calls["patch"] == []
    assert len(calls["post"]) == 1


# ---------------------------------------------------------------------------
# Test 5: no cloud_id, multiple desktop matches (same observation) → ImageIdentityConflictError
# ---------------------------------------------------------------------------


def test_multiple_desktop_id_matches_same_observation(monkeypatch):
    """Rule B: ambiguous reverse match → conflict, no PATCH or POST."""
    client, calls = _make_client(
        monkeypatch,
        [
            {"id": "img-400", "desktop_id": 55, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
            {"id": "img-401", "desktop_id": 55, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
        ],
    )

    with pytest.raises(cloud_sync.ImageIdentityConflictError):
        client.push_image_metadata(_local_img(55, cloud_id=None), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 6: local cloud_id points to missing/invisible row → POST
# ---------------------------------------------------------------------------


def test_local_cloud_id_points_to_missing_row(monkeypatch):
    """Rule D/G: unverifiable direct link + no reverse match → POST."""
    client, calls = _make_client(monkeypatch, [])

    cloud_id = client.push_image_metadata(_local_img(88, cloud_id="img-GONE"), OBS_CLOUD_ID, "")

    assert cloud_id == "new-cloud-image-id"
    assert calls["patch"] == []
    assert len(calls["post"]) == 1


# ---------------------------------------------------------------------------
# Test 7: cloud_id resolves to soft-deleted row, no restore intent → error
# ---------------------------------------------------------------------------


def test_cloud_id_soft_deleted_no_restore_intent(monkeypatch):
    """Soft-deleted direct target without explicit restore intent → conflict."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-500", "desktop_id": None, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": "2026-01-01T00:00:00Z"}],
    )

    with pytest.raises(cloud_sync.ImageIdentityConflictError, match="soft-deleted"):
        client.push_image_metadata(_local_img(99, cloud_id="img-500"), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 8: desktop_id resolves to soft-deleted row, no restore intent → error
# ---------------------------------------------------------------------------


def test_desktop_id_resolves_to_soft_deleted_no_restore_intent(monkeypatch):
    """Soft-deleted reverse match without explicit restore intent → conflict."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-600", "desktop_id": 33, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": "2026-01-01T00:00:00Z"}],
    )

    with pytest.raises(cloud_sync.ImageIdentityConflictError, match="soft-deleted"):
        client.push_image_metadata(_local_img(33, cloud_id=None), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 9: explicit restore: release, new row POSTed, marker cleared
# ---------------------------------------------------------------------------


def test_explicit_restore_releases_and_creates_new_row(monkeypatch):
    """Restore intent: PATCH release of old desktop_id, POST new row, clear marker."""
    cleared: list[int] = []
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-700", "desktop_id": 44, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None}],
    )
    # Override the defaults from _make_client
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda image_id: "img-700")
    monkeypatch.setattr(
        cloud_sync, "_clear_explicit_image_restore_source", lambda image_id: cleared.append(int(image_id))
    )

    cloud_id = client.push_image_metadata(_local_img(44, cloud_id=None), OBS_CLOUD_ID, "")

    assert cloud_id == "new-cloud-image-id"
    release_patches = [p for p, body in calls["patch"] if body == {"desktop_id": None}]
    assert len(release_patches) == 1
    assert "img-700" in release_patches[0]
    assert "user_id=eq." in release_patches[0]
    assert len(calls["post"]) == 1
    assert cleared == [44]


# ---------------------------------------------------------------------------
# Test 10: remote_row supplied with matching id AND observation_id → no extra GET
# ---------------------------------------------------------------------------


def test_remote_row_trusted_when_ids_match(monkeypatch):
    """Caller-supplied remote_row is trusted (no extra GET) when both ids match."""
    client, calls = _make_client(monkeypatch, [])
    # No rows in the fake store; resolver must trust remote_row for direct leg.
    remote_row = {"id": "img-800", "observation_id": OBS_CLOUD_ID, "desktop_id": None, "deleted_at": None}

    cloud_id = client.push_image_metadata(
        _local_img(55, cloud_id="img-800"),
        OBS_CLOUD_ID,
        "",
        remote_row=remote_row,
    )

    assert cloud_id == "img-800"
    # No GET for direct verification (trusted)
    direct_gets = [p for p in calls["cloud"].get_paths if "id=eq.img-800" in p]
    assert direct_gets == [], "remote_row match must skip the independent GET"
    assert calls["post"] == []


# ---------------------------------------------------------------------------
# Test 11: remote_row supplied with mismatched observation_id → NOT trusted
# ---------------------------------------------------------------------------


def test_remote_row_not_trusted_when_obs_id_mismatches(monkeypatch):
    """remote_row with wrong observation_id falls back to independent GET."""
    remote_row = {"id": "img-800", "observation_id": "cloud-obs-WRONG", "desktop_id": None, "deleted_at": None}
    # Provide the actual row in the store so independent GET succeeds
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-800", "desktop_id": None, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None}],
    )

    cloud_id = client.push_image_metadata(
        _local_img(55, cloud_id="img-800"),
        OBS_CLOUD_ID,
        "",
        remote_row=remote_row,
    )

    assert cloud_id == "img-800"
    # Independent GET was issued because remote_row didn't match
    direct_gets = [p for p in calls["cloud"].get_paths if "id=eq.img-800" in p]
    assert direct_gets, "mismatched remote_row must trigger independent GET"


# ---------------------------------------------------------------------------
# Test 12: ImageIdentityConflictError is subclass of CloudSyncError
# ---------------------------------------------------------------------------


def test_image_identity_conflict_error_is_cloud_sync_error():
    """push_all error handlers catch CloudSyncError; conflict must ride that path."""
    assert issubclass(cloud_sync.ImageIdentityConflictError, cloud_sync.CloudSyncError)


# ---------------------------------------------------------------------------
# Test 13: soft-deleted reverse match == restore source → no conflict raised
# ---------------------------------------------------------------------------


def test_explicit_restore_with_deleted_source_row_bypasses_deletion_guard(monkeypatch):
    """Deletion guard must not raise when the soft-deleted reverse match IS the restore source."""
    cleared: list[int] = []
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-700", "desktop_id": 44, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": "2026-07-21T10:00:00Z"}],
    )
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda image_id: "img-700")
    monkeypatch.setattr(
        cloud_sync, "_clear_explicit_image_restore_source", lambda image_id: cleared.append(int(image_id))
    )

    # Must not raise even though the reverse-matched row is soft-deleted
    cloud_id = client.push_image_metadata(_local_img(44, cloud_id=None), OBS_CLOUD_ID, "")

    assert cloud_id == "new-cloud-image-id"
    # Release PATCH (desktop_id: None) must fire on img-700
    release_patches = [p for p, body in calls["patch"] if body == {"desktop_id": None}]
    assert len(release_patches) == 1, "restore release PATCH must fire"
    assert "img-700" in release_patches[0]
    assert "user_id=eq." in release_patches[0]
    # New row POSTed (not a PATCH to update img-700)
    assert len(calls["post"]) == 1, "POST for new row must fire"


# ---------------------------------------------------------------------------
# Test 14: direct-leg restore — local cloud_id == restore source (soft-deleted)
# ---------------------------------------------------------------------------


def test_explicit_restore_via_direct_cloud_id_link(monkeypatch):
    """Direct leg: soft-deleted row found via cloud_id == restore_source_id → no conflict."""
    cleared: list[int] = []
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-800", "desktop_id": 55, "user_id": USER, "observation_id": "obs-X", "deleted_at": "2026-07-21T10:00:00Z"}],
    )
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda image_id: "img-800")
    monkeypatch.setattr(
        cloud_sync, "_clear_explicit_image_restore_source", lambda image_id: cleared.append(int(image_id))
    )

    # img has cloud_id pointing directly at the tombstoned row
    cloud_id = client.push_image_metadata(_local_img(55, cloud_id="img-800"), OBS_CLOUD_ID, "")

    assert cloud_id == "new-cloud-image-id"
    # Release PATCH must fire on img-800
    release_patches = [p for p, body in calls["patch"] if body == {"desktop_id": None}]
    assert len(release_patches) == 1, "restore release PATCH must fire on direct leg"
    assert "img-800" in release_patches[0]
    # POST for new row must fire (not updating the deleted row)
    assert len(calls["post"]) == 1, "POST for new row must fire"
