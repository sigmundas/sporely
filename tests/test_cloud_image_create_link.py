"""A new image this desktop creates is linked in the same push (v0.9.27 defect).

Observed 2026-10-18: every new image was created in the cloud, but the
confirming metadata step compared it with no baseline and raised
``ImageBaselineMissingError`` over representation-only differences, so the
local ``cloud_id`` stayed NULL and the observation stayed dirty:

* field photo, local ``sample_type='Not_set'`` (the push sends NULL);
* microscope image, ``scale_microns_per_pixel`` with 16-17 digits (the cloud
  float8 answers 15), ``captured_at`` local wall clock (the cloud: UTC);
* metadata-only anchor, created without ``captured_at`` (cloud NULL).

``CloudShapedChildCloud`` answers image rows the way PostgREST does (float8 at
15 significant digits, timestamptz in UTC). It is a mock transport: these
tests prove the client flow, not live server behavior.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from database.models import ObservationDB
from utils import cloud_sync
from utils.cloud_sync_impl.child_identity import _child_create_pending_key, mark_child_create_pending
from utils.cloud_sync_impl.image_policy import _add_cloud_image_storage_excluded_image_id
from tests.test_sync_key_child_identity import (
    ChildCloud,
    _image,
    _measurement,
    _observation,
    _push,
    _row,
    _set,
    two_desktops,  # noqa: F401  (fixture)
)


SCALE_LOCAL = 0.05349373209020841
SCALE_CLOUD = 0.0534937320902084
CAPTURED_LOCAL = "2026-10-18 09:17:18"


def _cloud_shape(row: dict) -> dict:
    out = dict(row)
    for key in ("scale_microns_per_pixel", "resample_scale_factor"):
        if isinstance(out.get(key), float):
            out[key] = float(f"{out[key]:.15g}")
    if out.get("captured_at"):
        parsed = datetime.fromisoformat(str(out["captured_at"]).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        out["captured_at"] = parsed.astimezone(timezone.utc).isoformat()
    return out


class CloudShapedChildCloud(ChildCloud):
    """Stores and answers observation_images as Postgres/PostgREST would."""

    def _post(self, table, payload):
        if table == "observation_images":
            payload = _cloud_shape(payload)
        return super()._post(table, payload)

    def _patch(self, table, filters, payload, headers):
        if table == "observation_images":
            payload = _cloud_shape(payload)
        return super()._patch(table, filters, payload, headers)


def _setting(key: str):
    from database.schema import get_connection
    conn = get_connection()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def _assert_linked_and_clean(cloud, obs_id, image_ids):
    rows = cloud.rows("observation_images")
    assert len(rows) == len(image_ids), "no duplicate cloud rows"
    by_key = {r["sync_key"]: r for r in rows}
    for image_id in image_ids:
        local = _row("images", image_id)
        assert str(local["cloud_id"]) == str(by_key[local["sync_key"]]["id"])
    assert _setting(_child_create_pending_key(obs_id)) is None
    assert ObservationDB.get_observation(obs_id)["sync_status"] == "synced"


def _second_push_writes_no_image(cloud, obs_id):
    cloud.requests.clear()
    ObservationDB.update_observation(obs_id, notes="edited")
    assert _push(cloud.client())["errors"] == []
    assert cloud.writes("observation_images") == [], "no echo PATCH"


# ── The three observed shapes, first push ────────────────────────────────


def test_new_field_image_with_not_set_sample_type_is_linked(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="field", sample_type="Not_set")

    result = _push(cloud.client())

    assert result["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["sample_type"] is None
    _assert_linked_and_clean(cloud, obs_id, [image_id])
    _second_push_writes_no_image(cloud, obs_id)


def test_new_microscope_image_with_float_and_local_time_is_linked(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(
        obs_id, tmp_path, image_type="microscope", sample_type="Dried",
        scale=SCALE_LOCAL, captured_at=CAPTURED_LOCAL,
    )
    _set("images", image_id, scale_microns_per_pixel=SCALE_LOCAL, captured_at=CAPTURED_LOCAL)

    result = _push(cloud.client())

    assert result["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["scale_microns_per_pixel"] == SCALE_CLOUD
    assert row["captured_at"].endswith("+00:00")
    _assert_linked_and_clean(cloud, obs_id, [image_id])
    _second_push_writes_no_image(cloud, obs_id)


def test_new_metadata_only_anchor_with_local_capture_time_is_linked(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="microscope", sample_type="Dried")
    _set("images", image_id, scale_microns_per_pixel=SCALE_LOCAL, captured_at=CAPTURED_LOCAL)
    _measurement(image_id)
    _add_cloud_image_storage_excluded_image_id(obs_id, image_id)

    result = _push(cloud.client())

    assert result["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["storage_path"] is None, "bytes not desired: a metadata-only anchor"
    _assert_linked_and_clean(cloud, obs_id, [image_id])
    _second_push_writes_no_image(cloud, obs_id)


# ── Recovery of rows the released build already created ──────────────────


def test_rows_created_by_the_released_build_are_linked_without_duplicates(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    field_id = _image(obs_id, tmp_path, image_type="field", name="f.jpg", sample_type="Not_set")
    micro_id = _image(obs_id, tmp_path, image_type="microscope", name="m.jpg", sample_type="Dried")
    anchor_id = _image(obs_id, tmp_path, image_type="microscope", name="a.jpg", sample_type="Dried")
    for image_id in (micro_id, anchor_id):
        _set("images", image_id, scale_microns_per_pixel=SCALE_LOCAL, captured_at=CAPTURED_LOCAL)
    _measurement(anchor_id)
    _add_cloud_image_storage_excluded_image_id(obs_id, anchor_id)
    client = cloud.client()
    _push(client)  # links the observation; images below are reset to the defect state
    rows = {r["sync_key"]: r for r in cloud.rows("observation_images")}
    assert len(rows) == 3
    # The released build's end state: cloud rows exist (keyed, anchor without
    # captured_at), two images unlinked, the anchor linked, no image baseline,
    # the child-create marker set and the observation dirty.
    for image_id in (field_id, micro_id):
        _set("images", image_id, cloud_id=None, synced_at=None)
    anchor_cloud = rows[_row("images", anchor_id)["sync_key"]]
    cloud.edit_elsewhere("observation_images", anchor_cloud["id"], captured_at=None)
    snapshot = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(str(_row("observations", obs_id)["cloud_id"]))
    )
    snapshot["images"] = []
    cloud_sync._store_cloud_observation_snapshot(str(_row("observations", obs_id)["cloud_id"]), json.dumps(snapshot))
    mark_child_create_pending(obs_id)
    ObservationDB.update_observation(obs_id, notes="recover")
    cloud.requests.clear()

    cloud.media_requests.clear()

    result = _push(cloud.client())

    assert result["errors"] == []
    assert [w for w in cloud.writes("observation_images", "POST")
            if w["payload"].get("sync_key") not in rows] == []
    _assert_linked_and_clean(cloud, obs_id, [field_id, micro_id, anchor_id])
    # Bytes go to the SAME reserved keys; no new key, no new object.
    puts = {key for method, key in cloud.media_requests if method == "PUT"}
    for image_id in (field_id, micro_id):
        key = rows[_row("images", image_id)["sync_key"]]["storage_path"]
        assert key in puts
    assert puts <= {k for r in rows.values() if r["storage_path"]
                    for k in (r["storage_path"], cloud_sync.media_variant_key(r["storage_path"], "thumb"))}
    anchor_patches = [w for w in cloud.writes("observation_images", "PATCH")
                      if ("id", f"eq.{anchor_cloud['id']}") in w["params"]]
    assert len(anchor_patches) == 1 and "captured_at" in anchor_patches[0]["payload"]
    _second_push_writes_no_image(cloud, obs_id)


# ── Genuine disagreements still go to review ─────────────────────────────


def test_keyed_cloud_row_with_a_real_scale_edit_is_not_adopted(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="microscope", sample_type="Dried")
    _set("images", image_id, scale_microns_per_pixel=SCALE_LOCAL)
    _push(cloud.client())
    [row] = cloud.rows("observation_images")
    _set("images", image_id, cloud_id=None, synced_at=None)
    cloud.edit_elsewhere("observation_images", row["id"], scale_microns_per_pixel=0.06)
    ObservationDB.update_observation(obs_id, notes="retry")
    cloud.requests.clear()

    result = _push(cloud.client())

    assert result["errors"], "a real difference is reviewed"
    assert _row("images", image_id)["cloud_id"] is None
    assert cloud.writes("observation_images", "PATCH") == []
    assert len(cloud.rows("observation_images")) == 1


@pytest.mark.parametrize("sample_type", ["Not_set", "Spore_print"])
def test_new_anchor_with_raw_sample_fields_is_linked(sample_type, two_desktops, tmp_path):
    # The anchor create sends the raw local sample fields; the cloud holds them.
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="microscope", sample_type=sample_type)
    _measurement(image_id)
    _add_cloud_image_storage_excluded_image_id(obs_id, image_id)

    result = _push(cloud.client())

    assert result["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["storage_path"] is None
    _assert_linked_and_clean(cloud, obs_id, [image_id])


def test_linked_image_then_cloud_only_notes_edit_is_adopted_without_conflict(two_desktops, tmp_path):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="microscope", sample_type="Dried")
    _set("images", image_id, scale_microns_per_pixel=SCALE_LOCAL, resample_scale_factor=0.3333333333333333)
    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    assert _row("images", image_id)["scale_microns_per_pixel"] == SCALE_LOCAL  # local keeps 16 digits
    cloud.edit_elsewhere("observation_images", row["id"], notes="web note")
    ObservationDB.update_observation(obs_id, notes="desktop edit")

    result = _push(cloud.client())

    assert result["errors"] == []
    assert _row("images", image_id)["notes"] == "web note"
    assert ObservationDB.get_observation(obs_id)["sync_status"] == "synced"


def _no_baseline_push(two_desktops, tmp_path, *, local: dict, cloud_edit: dict, image_type="field"):
    """A linked image with no baseline row, then a push of its metadata."""
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type=image_type)
    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    obs_cloud = str(_row("observations", obs_id)["cloud_id"])
    snapshot = cloud_sync._parse_cloud_observation_snapshot(cloud_sync._load_cloud_observation_snapshot(obs_cloud))
    snapshot["images"] = []
    cloud_sync._store_cloud_observation_snapshot(obs_cloud, json.dumps(snapshot))
    _set("images", image_id, **local)
    cloud.edit_elsewhere("observation_images", row["id"], **cloud_edit)
    client = cloud.client()
    return client, _row("images", image_id), obs_cloud, cloud, row


@pytest.mark.parametrize("local, cloud_edit", [
    ({"scale_microns_per_pixel": SCALE_LOCAL}, {"scale_microns_per_pixel": 0.06}),
    ({"resample_scale_factor": 0.3333333333333333}, {"resample_scale_factor": 0.25}),
    ({"sample_type": "Dried"}, {"sample_type": None}),
    ({"captured_at": CAPTURED_LOCAL}, {"captured_at": None}),  # byte-backed: no anchor fill
    ({"sample_type": "Spore_print"}, {"sample_type": None, "sample_source": None}),
])
def test_no_baseline_real_difference_is_refused_by_the_push(local, cloud_edit, two_desktops, tmp_path):
    from utils.cloud_sync_impl.errors import ImageBaselineMissingError
    client, img, obs_cloud, cloud, row = _no_baseline_push(two_desktops, tmp_path, local=local, cloud_edit=cloud_edit)
    cloud.requests.clear()
    with pytest.raises(ImageBaselineMissingError):
        client.push_image_metadata(img, obs_cloud, str(row["storage_path"]))
    assert cloud.writes("observation_images", "PATCH") == []


@pytest.mark.parametrize("local, cloud_edit", [
    ({"resample_scale_factor": 0.3333333333333333}, {"resample_scale_factor": 0.333333333333333}),
    ({"sample_type": "Spore_print"}, {"sample_type": None, "sample_source": "spore_print"}),
    ({"sample_type": "Not_set"}, {"sample_type": None}),
])
def test_no_baseline_representation_only_difference_is_not_refused(local, cloud_edit, two_desktops, tmp_path):
    client, img, obs_cloud, _cloud, row = _no_baseline_push(two_desktops, tmp_path, local=local, cloud_edit=cloud_edit)
    client.push_image_metadata(img, obs_cloud, str(row["storage_path"]))  # no exception


def test_cleared_capture_time_on_a_byte_desired_bare_microscope_row_is_refused(two_desktops, tmp_path):
    # A bare microscope row whose bytes this desktop still wants is not the
    # anchor this desktop creates: a web clear is not silently refilled.
    from utils.cloud_sync_impl.image_push import _image_patch_three_way
    client, img, obs_cloud, _cloud, row = _no_baseline_push(
        two_desktops, tmp_path, local={"captured_at": CAPTURED_LOCAL}, cloud_edit={},
        image_type="microscope",
    )
    remote = dict(row, storage_path=None, captured_at=None)
    assert _image_patch_three_way(client, img, obs_cloud, str(row["id"]), remote)[1] == {"captured_at"}
    _add_cloud_image_storage_excluded_image_id(img["observation_id"], img["id"])
    assert _image_patch_three_way(client, img, obs_cloud, str(row["id"]), remote)[1] == set()


def test_float8_noop_check():
    from utils.cloud_sync_impl.image_push import _image_patch_matches_remote
    assert _image_patch_matches_remote({"scale_microns_per_pixel": SCALE_LOCAL},
                                       {"scale_microns_per_pixel": SCALE_CLOUD})
    assert not _image_patch_matches_remote({"scale_microns_per_pixel": SCALE_LOCAL},
                                           {"scale_microns_per_pixel": 0.0534937320902})


# ── Baselined three-way: the push's sample normalization on every side ───


def _linked(tmp_path, sample_type, *, anchor):
    cloud = CloudShapedChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="microscope" if anchor else "field", sample_type=sample_type)
    if anchor:
        _measurement(image_id)
        _add_cloud_image_storage_excluded_image_id(obs_id, image_id)
    assert _push(cloud.client())["errors"] == []
    if anchor:  # a second push: the fill PATCH normalizes the anchor's cloud sample fields
        ObservationDB.update_observation(obs_id, notes="again")
        assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    return cloud, obs_id, image_id, row


@pytest.mark.parametrize("sample_type, anchor", [
    ("Not_set", False), ("Not_set", True), ("Spore_print", True),
])
def test_linked_raw_sample_image_adopts_a_cloud_only_notes_edit(sample_type, anchor, two_desktops, tmp_path):
    cloud, obs_id, image_id, row = _linked(tmp_path, sample_type, anchor=anchor)
    cloud.edit_elsewhere("observation_images", row["id"], notes="web note")
    ObservationDB.update_observation(obs_id, notes="desktop edit")

    result = _push(cloud.client())

    assert result["errors"] == []
    assert _row("images", image_id)["notes"] == "web note"
    assert ObservationDB.get_observation(obs_id)["sync_status"] == "synced"


def test_local_sample_edit_after_linking_is_pushed(two_desktops, tmp_path):
    cloud, obs_id, image_id, row = _linked(tmp_path, "Not_set", anchor=False)
    _set("images", image_id, sample_type="Dried")
    ObservationDB.update_observation(obs_id, notes="edit")

    assert _push(cloud.client())["errors"] == []
    [after] = cloud.rows("observation_images")
    assert after["sample_type"] == "Dried"


def test_both_sides_sample_change_still_conflicts(two_desktops, tmp_path):
    cloud, obs_id, image_id, row = _linked(tmp_path, "Not_set", anchor=False)
    _set("images", image_id, sample_type="Dried")
    cloud.edit_elsewhere("observation_images", row["id"], sample_type="Fresh")
    ObservationDB.update_observation(obs_id, notes="edit")

    result = _push(cloud.client())

    assert result["errors"], "a field both sides changed is reviewed"
    [after] = cloud.rows("observation_images")
    assert after["sample_type"] == "Fresh"
