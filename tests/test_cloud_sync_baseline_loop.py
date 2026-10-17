"""0.9.26 data-loss fix: the image baseline survives pulls, and every blocked
conflict is reviewable.

The pull, push, mixed-finalize and T6 tests run the real client,
``push_all``/``pull_all``/``finalize_sync_candidates`` and SQLite against the
shared fake cloud of ``test_sync_key_child_identity``. T3/T4/T5 use the
final-gate scaffold of ``test_cloud_conflict_plan_execution``; their claims
are about classification, which that scaffold runs unchanged.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from utils import cloud_sync
from utils.cloud_sync_impl.conflict_plan import _build_plan_from_automatic_decisions
from tests.test_cloud_conflict_plan_execution import _GateEnv, _StubClient
from tests.test_sync_key_child_identity import (
    ChildCloud,
    _TABLES,
    _copy_database,
    _create_full,
    _image,
    _observation,
    _push,
    _row,
    _set,
    two_desktops,  # noqa: F401  (fixture)
)


def _pull(client, *, sync_images: bool = True) -> dict:
    return cloud_sync.pull_all(
        client,
        remote_obs=client.list_remote_observations(),
        sync_calibrations=False,
        materialize_remote_images=True,
        sync_images=sync_images,
        full_pull=False,
    )


def _snapshot(obs_cloud_id) -> dict:
    return json.loads(cloud_sync._load_cloud_observation_snapshot(str(obs_cloud_id)) or '{}')


def _linked_field_image(tmp_path):
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path)
    cloud = ChildCloud()
    assert _push(cloud.client())["errors"] == []
    obs_cloud_id = _row("observations", obs_id)["cloud_id"]
    image_cloud_id = _row("images", image_id)["cloud_id"]
    assert obs_cloud_id and image_cloud_id
    return cloud, obs_id, image_id, str(obs_cloud_id), int(image_cloud_id)


def _linked_microscope_image_with_measurement(tmp_path):
    obs_id, image_id, measurement_id = _create_full(tmp_path)
    cloud = ChildCloud()
    assert _push(cloud.client())["errors"] == []
    obs_cloud_id = _row("observations", obs_id)["cloud_id"]
    image_cloud_id = _row("images", image_id)["cloud_id"]
    assert obs_cloud_id and image_cloud_id and _row("spore_measurements", measurement_id)["cloud_id"]
    return cloud, obs_id, image_id, str(obs_cloud_id), int(image_cloud_id)


def _wipe_image_baseline(obs_cloud_id: str) -> None:
    """What 0.9.26 left behind: an observation-only snapshot."""
    wiped = _snapshot(obs_cloud_id)
    wiped.pop("images")
    wiped.pop("measurements")
    cloud_sync._store_cloud_observation_snapshot(obs_cloud_id, json.dumps(wiped))


def _baseline_image_ids(obs_cloud_id: str) -> list:
    return [row["id"] for row in _snapshot(obs_cloud_id).get("images") or []]


def _signature_current(obs_id: int) -> bool:
    return cloud_sync._local_media_signatures_match(
        cloud_sync._load_local_cloud_media_signature(obs_id),
        cloud_sync._local_cloud_media_signature(obs_id),
    )


# ── T1: a pull with pending local media keeps the child baseline ─────────────

def test_pull_with_pending_local_media_keeps_the_image_baseline(two_desktops, tmp_path):
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_microscope_image_with_measurement(tmp_path)
    baseline = _snapshot(obs_cloud_id)
    assert [row["id"] for row in baseline["images"]] == [image_cloud_id]
    assert len(baseline["measurements"]) == 1
    # A local image edit the stored signature does not acknowledge yet, and a
    # newer cloud observation row.
    _set("images", image_id, notes="desktop edit")
    assert not _signature_current(obs_id)
    cloud.edit_elsewhere("observations", int(obs_cloud_id), notes="web note")

    first = _pull(cloud.client())

    snapshot = _snapshot(obs_cloud_id)
    assert snapshot["images"] == baseline["images"]
    assert snapshot["measurements"] == baseline["measurements"]
    assert _row("images", image_id)["notes"] == "desktop edit"
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    assert first["errors"] == []

    # The next pull finds nothing new in the cloud: no cloud image is applied
    # over the linked local row and nothing is downloaded or reviewed.
    cloud.media_requests.clear()
    second = _pull(cloud.client())

    assert second["errors"] == []
    assert _row("images", image_id)["notes"] == "desktop edit"
    assert [r for r in cloud.media_requests if r[0] == "GET"] == []
    assert _snapshot(obs_cloud_id)["images"] == baseline["images"]
    assert _snapshot(obs_cloud_id)["measurements"] == baseline["measurements"]


def test_pull_never_applies_a_differing_cloud_image_over_a_linked_row_without_baseline(
    two_desktops, tmp_path,
):
    """An installation whose baseline an earlier release already wiped."""
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_field_image(tmp_path)
    _wipe_image_baseline(obs_cloud_id)
    _set("images", image_id, notes="desktop edit")
    cloud.edit_elsewhere("observation_images", image_cloud_id, notes="web image note")
    cloud.edit_elsewhere("observations", int(obs_cloud_id), notes="web note")

    result = _pull(cloud.client())

    assert _row("images", image_id)["notes"] == "desktop edit"
    issues = cloud_sync.summarize_sync_issues(result["errors"])
    assert [entry["local_id"] for entry in issues["conflicts"]] == [obs_id], result["errors"]
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    assert "images" not in _snapshot(obs_cloud_id), "no baseline is invented for the image"


def test_pull_without_any_snapshot_never_applies_a_differing_cloud_image(two_desktops, tmp_path):
    """No stored snapshot at all is no image baseline either."""
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_field_image(tmp_path)
    cloud_sync._clear_cloud_observation_snapshot(obs_cloud_id)
    _set("images", image_id, notes="desktop edit")
    cloud.edit_elsewhere("observation_images", image_cloud_id, notes="web image note")
    cloud.edit_elsewhere("observations", int(obs_cloud_id), notes="web note")

    result = _pull(cloud.client())

    assert _row("images", image_id)["notes"] == "desktop edit"
    issues = cloud_sync.summarize_sync_issues(result["errors"])
    assert [entry["local_id"] for entry in issues["conflicts"]] == [obs_id], result["errors"]
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    assert image_cloud_id not in _baseline_image_ids(obs_cloud_id), "the undecided image gets no baseline"


def test_missing_file_retry_never_reapplies_an_undecided_cloud_image(two_desktops, tmp_path):
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_field_image(tmp_path)
    _wipe_image_baseline(obs_cloud_id)
    _set("images", image_id, notes="desktop edit")
    cloud.edit_elsewhere("observation_images", image_cloud_id, notes="web image note")
    cloud.edit_elsewhere("observations", int(obs_cloud_id), notes="web note")
    # The local file is gone, so the missing-media retry selects this image.
    Path(_row("images", image_id)["filepath"]).unlink()
    client = cloud.client()
    downloads = []

    def download_image_file(storage_path, dest_path):
        downloads.append(storage_path)
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (64, 48), (10, 20, 30)).save(dest_path, format="JPEG")

    client.download_image_file = download_image_file

    result = _pull(client)

    assert downloads == []
    assert _row("images", image_id)["notes"] == "desktop edit"
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    issues = cloud_sync.summarize_sync_issues(result["errors"])
    assert [entry["local_id"] for entry in issues["conflicts"]] == [obs_id], result["errors"]


# ── Mixed automatic and manual work keeps the undecided image in review ──────

def test_automatic_work_beside_an_undecided_image_keeps_it_in_review(two_desktops, tmp_path):
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_field_image(tmp_path)
    _wipe_image_baseline(obs_cloud_id)
    # Undecided: no baseline row, differs outside the old 12-field set.
    _set("images", image_id, measure_color="#ff0000")
    cloud.edit_elsewhere("observation_images", image_cloud_id, measure_color="#0000ff")
    # Automatic: a cloud-only observation field change.
    cloud.edit_elsewhere("observations", int(obs_cloud_id), notes="web note")
    cloud_sync._set_observation_conflict_review_pending(obs_id)
    client = cloud.client()

    manual, errors = cloud_sync.finalize_sync_candidates(
        client, [{"local_id": obs_id, "cloud_id": obs_cloud_id}],
    )

    assert errors == []
    assert _row("observations", obs_id)["notes"] == "web note", "the automatic part ran"
    assert [c["local_id"] for c in manual] == [obs_id]
    [pair] = [p for p in manual[0]["detail"]["image_pairs"] if p.get("metadata_diff_details")]
    assert [d["field"] for d in pair["metadata_diff_details"]] == ["measure_color"]
    assert image_cloud_id not in _baseline_image_ids(obs_cloud_id)
    assert _row("images", image_id)["measure_color"] == "#ff0000"
    assert _row("observations", obs_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert _row("observations", obs_id)["sync_status"] == "dirty"


# ── T2: the post-push adoption is acknowledged in the media signature ────────

def test_push_that_adopts_a_web_image_edit_leaves_no_local_media_change(two_desktops, tmp_path):
    cloud, obs_id, image_id, obs_cloud_id, image_cloud_id = _linked_field_image(tmp_path)
    cloud.edit_elsewhere("observation_images", image_cloud_id, notes="edited on the web")
    _set("observations", obs_id, notes="desktop observation edit", sync_status="dirty")

    assert _push(cloud.client())["errors"] == []

    assert _row("images", image_id)["notes"] == "edited on the web", "the web edit was adopted"
    assert _signature_current(obs_id)
    result = _pull(cloud.client())
    assert result["errors"] == []
    assert _row("observations", obs_id)["sync_status"] == "synced"


# ── T3/T4/T5: one classification for linked images without a baseline ───────

def _no_baseline_env(monkeypatch, *, local_image: dict, remote_image: dict, **local_obs):
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_cloud_image_ids", lambda *_a, **_k: set())
    return _GateEnv(
        monkeypatch,
        local_obs={"id": 1, "cloud_id": "obs-c", **local_obs},
        remote_obs={"id": "obs-c"},
        local_images=[{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                       "filepath": "/tmp/five.jpg", **local_image}],
        remote_images=[{"id": "c5", "image_type": "microscope", **remote_image}],
        baseline_obs={"id": "obs-c"},
    )


def _push_gate_image_conflicts(env) -> list[str]:
    report = cloud_sync._analyze_observation_push_conflicts(
        local_obs=dict(env.local_obs), local_images=[dict(r) for r in env.local_images],
        local_measurements_by_cloud_id={}, remote_obs=dict(env.remote_obs),
        remote_images=[dict(r) for r in env.remote_images], remote_measurements=[],
        baseline_snapshot=json.loads(env._snapshot_json()),
    )
    return list(report.image_conflict_keys)


def test_no_baseline_difference_outside_the_old_dialog_fields_is_manual(monkeypatch):
    env = _no_baseline_env(
        monkeypatch,
        local_image={"measure_color": "#ff0000"},
        remote_image={"measure_color": "#0000ff"},
    )
    assert _push_gate_image_conflicts(env) == ["cloud:c5"]

    detail = cloud_sync.get_conflict_detail(_StubClient(env), 1, "obs-c")
    assert detail["has_manual_conflicts"]
    [pair] = [p for p in detail["image_pairs"] if p.get("metadata_diff_details")]
    assert [(d["field"], d["change_origin"]) for d in pair["metadata_diff_details"]] == [
        ("measure_color", "baseline_unavailable"),
    ]

    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{"local_id": 1, "cloud_id": "obs-c"}],
    )
    assert [c["local_id"] for c in manual] == [1] and errors == []


def test_no_baseline_difference_in_a_dialog_field_is_never_automatic_cloud_wins(monkeypatch):
    env = _no_baseline_env(
        monkeypatch,
        local_image={"notes": "desktop note"},
        remote_image={"notes": "cloud note"},
    )
    assert _push_gate_image_conflicts(env) == ["cloud:c5"]

    detail = cloud_sync.get_conflict_detail(_StubClient(env), 1, "obs-c")
    assert detail["has_manual_conflicts"]
    assert not [d for d in detail["automatic_decisions"]["media"] if d["kind"] == "image_metadata"]
    assert _build_plan_from_automatic_decisions(detail)["items"] == []
    # A no-baseline entry reaching the plan builder is still never cloud-wins.
    forged = {"automatic_decisions": {"fields": [], "media": [{
        "kind": "image_metadata", "action": "auto_apply_baseline_unavailable",
        "local_id": 5, "cloud_id": "c5",
    }]}}
    assert _build_plan_from_automatic_decisions(forged)["items"] == []

    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{"local_id": 1, "cloud_id": "obs-c"}],
    )
    assert [c["local_id"] for c in manual] == [1] and errors == []
    assert env.local_images[0]["notes"] == "desktop note"
    assert env.calls["apply_remote_images"] == []


def test_review_pending_candidate_without_work_is_reported_not_dropped(monkeypatch):
    env = _no_baseline_env(
        monkeypatch,
        local_image={"notes": "same"},
        remote_image={"notes": "same"},
        sync_blocked_reason=cloud_sync.CONFLICT_REVIEW_PENDING_MARKER,
    )
    cleared = []
    monkeypatch.setattr(cloud_sync, "_clear_observation_conflict_review_pending", cleared.append)

    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{"local_id": 1, "cloud_id": "obs-c"}],
    )

    assert manual == []
    assert [(e["local_id"], e["phase"]) for e in errors] == [(1, "classify")]
    assert cleared == [], "the review marker stays until a push re-checks the observation"


# ── T6: a differing keyed child copy reaches conflict review ─────────────────

def test_image_adoption_content_conflict_is_routed_to_review(two_desktops, monkeypatch, tmp_path):
    table = "observation_images"
    cloud = ChildCloud()
    obs_id, image_id, _measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere(table, cloud.rows(table)[0]["id"], notes="edited on the web")
    expected = {name: cloud.rows(name) for name in _TABLES}

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result = _push(cloud.client())

    issues = cloud_sync.summarize_sync_issues(result["errors"])
    [entry] = [e for e in issues["conflicts"] if e["local_id"] == obs_id]
    assert entry["push_skipped"] is True and entry["pull_skipped"] is False
    assert _row("observations", obs_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    # Routing changes nothing in the cloud: the copy is still never PATCHed.
    assert {w["method"] for w in cloud.writes(table)} == {"POST"}
    for name in ("observation_images", "spore_measurements"):
        assert cloud.rows(name) == expected[name]

    # The entry the UI hands to the dialog path reaches the dialog.
    cloud.requests.clear()
    manual, _errors = cloud_sync.finalize_sync_candidates(cloud.client(), [entry])
    assert [c["local_id"] for c in manual] == [obs_id]
    assert cloud.writes(table, "PATCH") == []
    assert _row("images", image_id)["notes"] is None


def _assert_routed_to_review(result: dict, obs_id: int) -> dict:
    issues = cloud_sync.summarize_sync_issues(result["errors"])
    [entry] = [e for e in issues["conflicts"] if e["local_id"] == obs_id]
    assert entry["push_skipped"] is True and entry["pull_skipped"] is False
    assert _row("observations", obs_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert _row("observations", obs_id)["sync_status"] == "dirty"
    return entry


def _prepare_nothing(observation, progress_cb=None):
    """A prepare step that offers no image for upload: only anchors run."""
    return [], None, []


def test_measurement_adoption_content_conflict_is_routed_to_review(two_desktops, monkeypatch, tmp_path):
    table = "spore_measurements"
    cloud = ChildCloud()
    obs_id, _image_id, measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere(table, cloud.rows(table)[0]["id"], length_um=12.5)
    expected = {name: cloud.rows(name) for name in _TABLES}

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result = _push(cloud.client())

    entry = _assert_routed_to_review(result, obs_id)
    assert {w["method"] for w in cloud.writes(table)} == {"POST"}, "a differing copy is never PATCHed"
    for name in ("observation_images", "spore_measurements"):
        assert cloud.rows(name) == expected[name]
    assert _row("spore_measurements", measurement_id)["cloud_id"] is None

    # The dialog path receives it as a manual decision: the automatic part
    # of the gate never pushes the local copy over the web edit.
    cloud.requests.clear()
    manual, _errors = cloud_sync.finalize_sync_candidates(cloud.client(), [entry])
    assert [c["local_id"] for c in manual] == [obs_id]
    assert cloud.writes(table, "PATCH") == []
    assert cloud.rows(table)[0]["length_um"] == 12.5
    assert _row("spore_measurements", measurement_id)["length_um"] == 10.0
    assert _row("observations", obs_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER


def test_anchor_adoption_content_conflict_is_routed_to_review(two_desktops, monkeypatch, tmp_path):
    table = "observation_images"
    cloud = ChildCloud()
    obs_id, image_id, _measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere(table, cloud.rows(table)[0]["id"], notes="edited on the web")
    expected = {name: cloud.rows(name) for name in _TABLES}

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    client = cloud.client()
    result = cloud_sync.push_all(
        client, sync_images=True, prepare_images_cb=_prepare_nothing,
        remote_obs=client.list_remote_observations(), sync_calibrations=False,
        full_pull=False, verify_stamped_measurements=False,
    )

    assert not [e for e in result["errors"] if "Image 1:" in e], "only the anchor step met the copy"
    entry = _assert_routed_to_review(result, obs_id)
    assert {w["method"] for w in cloud.writes(table)} == {"POST"}
    for name in ("observation_images", "spore_measurements"):
        assert cloud.rows(name) == expected[name]
    assert _row("images", image_id)["cloud_id"] is None

    # The unlinked image never agreed with its keyed copy: no automatic side.
    detail = cloud_sync.get_conflict_detail(cloud.client(), obs_id)
    assert not [d for d in detail["automatic_decisions"]["media"] if d["kind"] == "image_metadata"]
    [pair] = [p for p in detail["image_pairs"] if p.get("metadata_diff_details")]
    assert {d["change_origin"] for d in pair["metadata_diff_details"]} == {"baseline_unavailable"}

    cloud.requests.clear()
    manual, _errors = cloud_sync.finalize_sync_candidates(cloud.client(), [entry])
    assert [c["local_id"] for c in manual] == [obs_id]
    assert cloud.writes(table, "PATCH") == []
    assert cloud.rows(table)[0]["notes"] == "edited on the web"
    assert _row("images", image_id)["notes"] is None


def test_anchor_content_conflict_reaches_measurement_push_callers_typed(two_desktops, monkeypatch, tmp_path):
    """Callers outside the push loop (conflict plans, mosaic) get the typed error."""
    cloud = ChildCloud()
    obs_id, _image_id, _measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere("observation_images", cloud.rows("observation_images")[0]["id"], notes="web")
    two_desktops["B"].activate(monkeypatch)
    # Desktop B's observation is linked; its image is not.
    _set("observations", obs_id, cloud_id=str(cloud.rows("observations")[0]["id"]))
    cloud.requests.clear()

    with pytest.raises(cloud_sync.ChildAdoptionConflictError) as raised:
        cloud_sync._push_measurements_for_observation(cloud.client(), obs_id)
    assert raised.value.reason == "content"
    assert cloud.writes("observation_images", "PATCH") == []
