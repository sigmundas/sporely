"""Systematic coverage: a desktop push sends only what the desktop edited.

One authoritative classification of every ``observation_images`` column the
desktop sends (``image_push._IMAGE_*_FIELDS``) and, per field and per image
write path, the real push code with only HTTP faked: web-created state with web
values in every field and a matching baseline, one desktop edit, then assert
that no other field is sent with a value different from the cloud's.
Observation fields run through the real ``sync_all`` against an in-memory cloud.
"""
from __future__ import annotations

import json

import pytest

from utils import cloud_sync
from utils.cloud_sync_impl import baseline as baseline_mod
from utils.cloud_sync_impl import image_push
from utils.cloud_sync_impl.image_push import _image_patch_matches_remote
from utils.cloud_sync_impl.reconciliation.values import _cloud_image_captured_at_to_local

USER_ID = "user-123"
OBS = "cloud-obs-7000"
CID = "c-7000"
UUID_B, UUID_C = "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333"

_CLASSES = (
    image_push._IMAGE_THREE_WAY_FIELDS, image_push._IMAGE_FILL_ONLY_FIELDS,
    image_push._IMAGE_UPLOAD_OWNED_FIELDS, image_push._IMAGE_STRUCTURAL_FIELDS,
)

# Web values (cloud form) for every column; desktop edit values (local form).
WEB = {
    "notes": "web note", "objective_name": "web 40x", "measure_color": "#123456",
    "mount_medium": "KOH", "stain": "Congo", "sample_type": "Dried", "sample_source": "spore_print",
    "contrast": "DIC", "crop_mode": "full", "micro_category": "basidia",
    "captured_at": "2026-10-01T08:00:00.123456+00:00", "sort_order": 2, "image_type": "field",
    "scale_microns_per_pixel": 0.25, "resample_scale_factor": 0.5, "gps_source": True,
    "ai_crop_x1": 0.1, "ai_crop_y1": 0.2, "ai_crop_x2": 0.8, "ai_crop_y2": 0.9,
    "ai_crop_source_w": 100, "ai_crop_source_h": 200, "ai_crop_is_custom": True,
    "calibration_uuid": UUID_B, "original_filename": "web.jpg",
}
EDIT = {
    "notes": "desktop note", "objective_name": "100x", "measure_color": "#654321",
    "mount_medium": "Water", "stain": "Melzer", "sample_type": "Fresh", "sample_source": "Hymenium",
    "contrast": "BF", "crop_mode": "ai", "micro_category": "spores",
    "captured_at": "2026-09-01 10:00:00", "sort_order": 5, "image_type": "microscope",
    "scale_microns_per_pixel": 0.5, "resample_scale_factor": 1.0, "gps_source": 0,
    "ai_crop_x1": 0.15, "ai_crop_y1": 0.25, "ai_crop_x2": 0.85, "ai_crop_y2": 0.95,
    "ai_crop_source_w": 150, "ai_crop_source_h": 250, "ai_crop_is_custom": 0,
    "calibration_uuid": UUID_C,
}


def _web_row() -> dict:
    return {
        "id": CID, "observation_id": OBS, "user_id": USER_ID, "deleted_at": None,
        "storage_path": f"{USER_ID}/{OBS}/0_web.webp", "storage_exif_safe": True,
        "upload_mode": "full", "source_width": 540, "source_height": 960,
        "stored_width": 540, "stored_height": 960, "stored_bytes": 22946, **WEB,
    }


def _local_row(**edit) -> dict:
    row = {k: v for k, v in WEB.items() if k != "original_filename"}
    row.update({
        "id": 7001, "observation_id": 7000, "cloud_id": CID, "filepath": "/tmp/7001.webp",
        "captured_at": _cloud_image_captured_at_to_local(WEB["captured_at"]),
        "sample_source": "Spore_print", "gps_source": 1, "ai_crop_is_custom": 1,
        "source_role": None, "file_purpose": None, "synced_at": "2026-10-16T00:00:00Z",
    })
    row.update(edit)
    return row


def test_every_column_the_desktop_sends_is_classified():
    classified = set().union(*map(set, _CLASSES))
    sent = set(cloud_sync._IMG_PUSH_COLS) | set(cloud_sync._IMG_UPLOAD_META_COLS) | {
        "captured_at", "calibration_uuid", "observation_id", "user_id", "deleted_at",
        "original_filename", "storage_exif_safe",
    }
    assert sent - classified == set()
    # No column is in two classes; protection and adoption share one list.
    assert sum(len(c) for c in _CLASSES) == len(classified)
    assert set(image_push._ADOPTABLE_IMAGE_FIELDS) <= set(image_push._IMAGE_THREE_WAY_FIELDS)
    # The real payload carries nothing unclassified.
    client, patches = _client(None, _web_row())
    client.push_image_metadata(_local_row(upload_mode="reduced", stored_bytes=1), OBS, "")
    assert set(patches[0][1]) - classified == set()


def _client(monkeypatch, remote):
    client = cloud_sync.SporelyCloudClient("token", USER_ID)
    patches = []
    client._get = lambda path: [dict(remote)] if path.startswith(f"observation_images?id=eq.{CID}") else []
    client._patch = lambda path, payload: patches.append((path, dict(payload))) or [{}]
    client._set_observation_media_keys = lambda *a, **k: None
    baseline_mod._store_cloud_observation_snapshot(OBS, json.dumps({
        "schema_version": 2, "observation": {"id": OBS}, "images": [dict(remote)], "measurements": [],
    }))
    return client, patches


def _changed_keys(body: dict, remote: dict) -> set[str]:
    return {k for k, v in body.items() if not _image_patch_matches_remote({k: v}, remote)}


def _push(path, client, img, remote, monkeypatch):
    if path == "refresh":
        client.push_image_metadata(img, OBS, "")
    elif path == "replace":
        client.push_image_metadata(img, OBS, f"{USER_ID}/{OBS}/0_v2.webp", remote_row=remote)
    elif path == "resolution":
        client._explicit_conflict_resolution = True
        client.push_image_metadata(img, OBS, "", remote_row=remote)
    elif path == "drift":
        monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _o: [dict(img)])
        monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _o: {})
        monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_a: True)
        cloud_sync._reconcile_metadata_only_linked_images(client, {"id": 7000}, OBS, [remote])


@pytest.mark.parametrize("path", ["refresh", "replace", "drift", "resolution"])
@pytest.mark.parametrize("field", [None, *EDIT])
def test_only_the_edited_image_field_is_sent_changed(monkeypatch, path, field):
    remote = _web_row()
    client, patches = _client(monkeypatch, remote)
    img = _local_row(**({field: EDIT[field]} if field else {}))

    _push(path, client, img, remote, monkeypatch)

    allowed = {field} if field else set()
    if path == "replace":
        allowed.add("storage_path")
    for _p, body in patches:
        assert _changed_keys(body, remote) <= allowed, body
    if field:
        sent = [b for _p, b in patches if field in _changed_keys(b, remote)]
        assert sent, f"the desktop edit of {field} was not sent via {path}"


# ---------------------------------------------------------------------------
# Observation fields: the real sync_all against an in-memory cloud.
# ---------------------------------------------------------------------------

from tests.test_cloud_sync_remote_only_adoption import _FakeCloud, _sync, env  # noqa: E402,F401
from database import models  # noqa: E402

_OBS_WEB = {
    "date": "2026-09-21", "genus": "Boletus", "species": "edulis", "common_name": "Web common",
    "species_guess": "web guess", "uncertain": True, "unspontaneous": True,
    "determination_method": 2, "location": "Web place",
    "location_public": False, "is_draft": False, "location_precision": "fuzzed",
    "ai_selected_service": "web-ai", "ai_selected_taxon_id": 77, "ai_selected_scientific_name": "Web sci",
    "ai_selected_probability": 0.5, "ai_selected_at": "2026-09-21T10:00:00+00:00",
    "red_list_category": "VU", "red_list_categories_json": {"NO": "VU"},
    "habitat": "web habitat", "habitat_nin2_path": "web nin2", "habitat_substrate_path": "web sub",
    "habitat_host_genus": "Betula", "habitat_host_species": "pendula", "habitat_host_common_name": "birch",
    "habitat_nin2_note": "n2", "habitat_substrate_note": "sn", "habitat_grows_on_note": "go",
    "notes": "web notes", "open_comment": "web comment", "interesting_comment": True,
    "publish_target": "artsobs_no", "artsdata_id": 11, "artportalen_id": 12, "inaturalist_id": 13,
    "mushroomobserver_id": 14, "auto_threshold": 0.7,
    "source_type": "web", "citation": "web cite", "data_provider": "web dp", "author": "web author",
    "spore_data_visibility": "friends", "country_code": "SE",
}


# Stop-rule finding (not fixed this round): the desktop stores a derived
# species_guess ("Genus species") and sends it as a local edit, over a web
# species_guess or after a web genus/species change.
_DERIVED_GUESS = pytest.mark.xfail(strict=True, reason="derived species_guess sent as a desktop edit")
_OBS_CASES = [pytest.param(f, marks=_DERIVED_GUESS) if f in {"genus", "species", "species_guess"} else f
              for f in sorted(_OBS_WEB)]


@pytest.mark.parametrize("field", _OBS_CASES)
def test_only_the_edited_observation_field_is_sent_changed(env, field):
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, **{field: _OBS_WEB[field]})
    desk_field = "location" if field == "notes" else "notes"
    models.ObservationDB.update_observation(local_id, **{desk_field: "desktop value"})
    before = dict(cloud.rows[cloud_id])

    _sync(cloud)

    for patch in cloud.patches:
        changed = {k for k, v in patch.items() if k != "user_id" and v != before.get(k)
                   and str(v) != str(before.get(k))}
        assert changed <= {desk_field}, (field, changed)
    assert str(cloud.rows[cloud_id][field]) == str(_OBS_WEB[field])  # value, not JSON type


@pytest.mark.parametrize("field", ["habitat_host_genus", "habitat", "open_comment", "habitat_host_species"])
def test_observation_blank_local_text_never_turns_cloud_null_into_empty(env, field):
    cloud, local_id, cloud_id = env
    cloud.rows[cloud_id][field] = None
    models.ObservationDB.update_observation(local_id, **{field: "", "notes": "desktop value"})

    _sync(cloud)

    assert cloud.rows[cloud_id][field] is None
