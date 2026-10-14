"""Image push identity resolution.

Regression coverage for the Download-from-Cloud duplicate-image bug:
``push_image_metadata`` used to resolve identity only via the remote reverse
link (``observation_images.desktop_id``). Images imported by pull-only sync
legitimately carry a local ``cloud_id`` while the remote ``desktop_id`` is
still NULL (pull-only performs zero cloud writes), so the reverse lookup
found nothing and a later normal push POSTed a duplicate cloud image.

The canonical resolver under test is
``SporelyCloudClient._resolve_existing_image_for_push``:

* verified local ``cloud_id`` is the only push target (PATCH, never POST);
* ``desktop_id`` is retired as an identity (per-record sync keys, Stage 4):
  it is never looked up, released or written, so another row's
  ``desktop_id`` can never redirect a push;
* a linked row that is gone, soft-deleted or under another observation raises
  ``ImageIdentityConflictError`` and must leave the image dirty/retryable.

An unlinked image takes the create path (``create_child_row``), covered with
two databases and a shared fake cloud in ``tests/test_sync_key_child_identity.py``.
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
# Test 2: another row carrying this local id as desktop_id is ignored
# ---------------------------------------------------------------------------


def _desktop_id_lookups(calls) -> list[str]:
    return [p for p in calls["cloud"].get_paths if "desktop_id=" in p]


def test_local_cloud_id_valid_ignores_a_desktop_id_match_elsewhere(monkeypatch):
    """Another device's row with desktop_id=42 never redirects or blocks the PATCH."""
    client, calls = _make_client(
        monkeypatch,
        [
            {"id": "img-200", "desktop_id": None, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
            {"id": "img-201", "desktop_id": 42, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
        ],
    )

    cloud_id = client.push_image_metadata(_local_img(42, cloud_id="img-200"), OBS_CLOUD_ID, "")

    assert cloud_id == "img-200"
    assert [p for p, _ in calls["patch"]] == [f"observation_images?id=eq.img-200&user_id=eq.{USER}"]
    assert all("desktop_id" not in body for _, body in calls["patch"])
    assert calls["post"] == []
    assert _desktop_id_lookups(calls) == []


# ---------------------------------------------------------------------------
# Test 3: an unlinked image never looks up or reuses a desktop_id row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "remote_rows",
    [
        [{"id": "img-300", "desktop_id": 77, "observation_id": OBS_CLOUD_ID, "deleted_at": None}],
        [{"id": "img-300", "desktop_id": 77, "observation_id": "cloud-obs-OTHER", "deleted_at": None}],
        [
            {"id": "img-400", "desktop_id": 77, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
            {"id": "img-401", "desktop_id": 77, "observation_id": OBS_CLOUD_ID, "deleted_at": None},
        ],
        [{"id": "img-600", "desktop_id": 77, "observation_id": OBS_CLOUD_ID, "deleted_at": "2026-01-01T00:00:00Z"}],
    ],
    ids=["same-observation", "other-observation", "ambiguous", "soft-deleted"],
)
def test_unlinked_image_never_looks_up_or_reuses_desktop_id_rows(monkeypatch, remote_rows):
    """No cloud_id: the resolver answers "create" without any desktop_id read."""
    client, calls = _make_client(
        monkeypatch, [{**row, "user_id": USER} for row in remote_rows],
    )

    assert client._resolve_existing_image_for_push(_local_img(77, cloud_id=None), OBS_CLOUD_ID) is None
    assert calls["cloud"].get_paths == []
    assert calls["patch"] == [] and calls["post"] == []


# ---------------------------------------------------------------------------
# Test 6: local cloud_id points to missing/invisible row → conflict, no write
# ---------------------------------------------------------------------------


def test_local_cloud_id_points_to_missing_row(monkeypatch):
    """A linked row whose cloud copy is gone is never POSTed or PATCHed blindly."""
    client, calls = _make_client(monkeypatch, [])

    with pytest.raises(cloud_sync.ImageIdentityConflictError, match="not found"):
        client.push_image_metadata(_local_img(88, cloud_id="img-GONE"), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []


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
# Test 13: restore intent never releases or PATCHes the deleted source row
# ---------------------------------------------------------------------------


def test_restore_source_is_never_released_or_patched(monkeypatch):
    """A restore republishes as a new keyed row: the deleted row is left alone."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-700", "desktop_id": 44, "user_id": USER, "observation_id": OBS_CLOUD_ID, "deleted_at": "2026-07-21T10:00:00Z"}],
    )
    monkeypatch.setattr(cloud_sync, "_explicit_image_restore_source", lambda image_id: "img-700")

    assert client._resolve_existing_image_for_push(_local_img(44, cloud_id=None), OBS_CLOUD_ID) is None
    assert calls["patch"] == [] and calls["post"] == []
    assert _desktop_id_lookups(calls) == []


# ---------------------------------------------------------------------------
# Test 15: direct cloud_id points to another observation → ImageIdentityConflictError
# ---------------------------------------------------------------------------


def test_direct_cloud_id_points_to_another_observation_raises_conflict(monkeypatch):
    """Finding 2: cloud_id verified but observation_id mismatch → conflict, no write."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "img-900", "desktop_id": None, "user_id": USER, "observation_id": "other-obs", "deleted_at": None}],
    )

    with pytest.raises(cloud_sync.ImageIdentityConflictError):
        client.push_image_metadata(_local_img(10, cloud_id="img-900"), OBS_CLOUD_ID, "")

    assert calls["patch"] == []
    assert calls["post"] == []
