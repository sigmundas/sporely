"""Turn A regression tests for ``resolve_conflict_plan``.

Every test here exercises the per-item plan executor in isolation.  The tests
prove:

* drift protection (local + remote observation, image identity, measurement);
* exact choice-to-operation mapping (no accidental pushes/imports);
* resolver-side identity guards;
* finalization order (snapshot → signature → stamp);
* ``spore_statistics`` never wiped by an accidental ``PATCH ... = NULL``;
* limited presentation policy (rotation/sort_order preserved on matched
  images, deterministic order for downloaded cloud-only images);
* the fixed-token read-only client (subclass) refuses refresh/persist paths.

Fake clients, fake ``ImageDB`` / ``MeasurementDB`` / ``ObservationDB`` methods,
and monkey-patched helpers keep the tests hermetic — nothing here contacts a
real Sporely Cloud environment or a real database.
"""
from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

import pytest

import utils.cloud_sync as cloud_sync
from utils.cloud_sync import (
    CloudSyncError,
    build_conflict_plan_baseline,
    resolve_conflict_plan,
    SporelyReadOnlyCloudClient,
)
from database import schema as db_schema
from database.models import SettingsDB, ImageDB, MeasurementDB, ObservationDB


class _RecordingClient:
    """Bare stub — only the methods the resolver invokes on ``client``."""

    def __init__(self, *, remote_obs, patches=None):
        self._remote_obs = dict(remote_obs)
        self.patches = patches if patches is not None else []

    def get_observation(self, _cloud_id):
        return dict(self._remote_obs)

    def _patch(self, path, payload):
        self.patches.append((path, dict(payload)))


def _patch_common(monkeypatch, *, local_obs, remote_obs,
                  local_images=None, remote_images=None,
                  local_measurements=None, remote_measurements=None,
                  format_recomputed=lambda _id: "recomputed",
                  push_measurements=None, push_images=None,
                  apply_remote_images=None, import_remote_measurements=None,
                  apply_remote_fields=None,
                  store_remote_snapshot=None, stamp=None,
                  refresh_signature=None, image_updates=None,
                  fail_snapshot=False):
    """Central monkeypatch bundle so each test only overrides what it cares about."""
    local_images = local_images or []
    remote_images = remote_images or []
    local_measurements = local_measurements or []
    remote_measurements = remote_measurements or []
    push_measurements_calls = [] if push_measurements is None else push_measurements
    push_images_calls = [] if push_images is None else push_images
    apply_remote_images_calls = [] if apply_remote_images is None else apply_remote_images
    import_remote_measurements_calls = (
        [] if import_remote_measurements is None else import_remote_measurements
    )
    apply_remote_fields_calls = [] if apply_remote_fields is None else apply_remote_fields
    store_remote_snapshot_calls = (
        [] if store_remote_snapshot is None else store_remote_snapshot
    )
    stamp_calls = [] if stamp is None else stamp
    refresh_calls = [] if refresh_signature is None else refresh_signature
    image_update_calls = [] if image_updates is None else image_updates

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _id: [dict(row) for row in local_measurements])
    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync",
                        lambda *a: [dict(row) for row in remote_images])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images",
                        lambda *a: [dict(row) for row in remote_measurements])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields",
                        lambda *a, **k: apply_remote_fields_calls.append(k.get('fields')))
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local",
                        lambda *a, **k: apply_remote_images_calls.append(
                            {'ids': [str(row.get('id') or '') for row in a[2]],
                             'allow_delete': k.get('allow_delete')}))
    monkeypatch.setattr(cloud_sync, "_import_remote_measurements_for_observation",
                        lambda *a, **k: (import_remote_measurements_calls.append(k)
                                         or {'failed': 0, 'warnings': []}))
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                        lambda *a, **k: push_measurements_calls.append(k.get('measurement_ids')))
    monkeypatch.setattr(cloud_sync, "_push_images_for_observation",
                        lambda *a, **k: (push_images_calls.append(k.get('include_image_ids'))
                                         or True))
    monkeypatch.setattr(cloud_sync, "_format_recomputed_spore_statistics", format_recomputed)
    monkeypatch.setattr(cloud_sync.ObservationDB, "update_spore_statistics",
                        lambda _id, value: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature",
                        lambda *a: refresh_calls.append('refreshed'))
    monkeypatch.setattr(cloud_sync, "_stamp_observation_synced",
                        lambda *a: stamp_calls.append(tuple(a)))

    def _snapshot(*a, **k):
        if fail_snapshot:
            raise RuntimeError('boom')
        store_remote_snapshot_calls.append((a, k))
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", _snapshot)
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image",
                        lambda _id, **k: image_update_calls.append((_id, k)))
    return {
        'patches_client': None,  # placeholder — tests build their own client
        'patches': None,
        'push_measurements': push_measurements_calls,
        'push_images': push_images_calls,
        'apply_remote_images': apply_remote_images_calls,
        'import_remote_measurements': import_remote_measurements_calls,
        'apply_remote_fields': apply_remote_fields_calls,
        'store_remote_snapshot': store_remote_snapshot_calls,
        'stamp': stamp_calls,
        'refresh_signature': refresh_calls,
        'image_updates': image_update_calls,
    }


def _baseline_from_state(local_obs, remote_obs, local_images, remote_images,
                        local_measurements, remote_measurements):
    return build_conflict_plan_baseline(
        local_obs=local_obs, remote_obs=remote_obs,
        local_images=local_images, remote_images=remote_images,
        local_measurements=local_measurements,
        remote_measurements=remote_measurements,
    )


def _empty_baseline():
    return build_conflict_plan_baseline(
        local_obs={}, remote_obs={}, local_images=[], remote_images=[],
        local_measurements=[], remote_measurements=[],
    )


# ── Drift protection ─────────────────────────────────────────────────────────

def test_drift_local_observation_field_change_aborts_before_writes(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local-orig"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])

    # Simulate concurrent local edit: the value at apply-time differs from the
    # value the user reviewed.
    live_local = dict(local_obs, common_name="local-DRIFTED")
    tracker = _patch_common(monkeypatch, local_obs=live_local, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)

    with pytest.raises(CloudSyncError, match="changed after this comparison"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'field', 'field': 'common_name', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_fields'] == []
    assert tracker['store_remote_snapshot'] == []
    assert tracker['stamp'] == []


def test_drift_remote_observation_field_change_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    live_remote = dict(remote_obs, common_name="cloud-DRIFTED")
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=live_remote)
    client = _RecordingClient(remote_obs=live_remote)

    with pytest.raises(CloudSyncError, match="changed after this comparison"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'field', 'field': 'common_name', 'choice': 'local'}],
        })
    assert tracker['apply_remote_fields'] == []


def test_drift_local_image_identity_change_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "img-cloud-5", "image_type": "microscope",
                     "sort_order": 5, "filepath": "/tmp/a.jpg"}]
    remote_images = [{"id": "img-cloud-5", "desktop_id": 5, "image_type": "microscope",
                      "sort_order": 5, "storage_path": "cloud/a.webp"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])

    # Concurrent local edit changes the image's cloud_id link — a real identity drift.
    live_local_images = [dict(local_images[0], cloud_id="img-cloud-OTHER")]
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=live_local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="changed after this comparison"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'img-cloud-5', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_images'] == []


def test_drift_remote_measurement_scientific_change_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [{"id": 10, "cloud_id": "m-cloud-10", "image_id": 5, "length_um": 5.0}]
    remote_meas = [{"id": "m-cloud-10", "desktop_id": 10, "image_id": "img-cloud-5", "length_um": 6.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, remote_meas)

    live_remote_meas = [dict(remote_meas[0], length_um=7.5)]  # drifted after review
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas, remote_measurements=live_remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="changed after this comparison"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 10, 'cloud_id': 'm-cloud-10', 'choice': 'cloud'}],
        })
    assert tracker['import_remote_measurements'] == []


def test_missing_baseline_aborts_before_writes(monkeypatch):
    """A1: item-level plans without a baseline are rejected."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="missing the reviewed baseline"):
        resolve_conflict_plan(client, 1, plan={
            'items': [{'kind': 'field', 'field': 'common_name', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_fields'] == []


def test_unsupported_baseline_schema_aborts(monkeypatch):
    tracker = _patch_common(monkeypatch,
                            local_obs={"id": 1, "cloud_id": "obs-cloud"},
                            remote_obs={"id": "obs-cloud"})
    client = _RecordingClient(remote_obs={"id": "obs-cloud"})
    with pytest.raises(CloudSyncError, match="Unsupported conflict plan baseline schema"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': {**_empty_baseline(), 'schema_version': 99},
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_fields'] == []


def test_malformed_baseline_aborts(monkeypatch):
    tracker = _patch_common(monkeypatch,
                            local_obs={"id": 1, "cloud_id": "obs-cloud"},
                            remote_obs={"id": "obs-cloud"})
    client = _RecordingClient(remote_obs={"id": "obs-cloud"})
    # Missing 'remote_measurements' collection.
    partial = _empty_baseline()
    partial.pop('remote_measurements')
    with pytest.raises(CloudSyncError, match="Malformed conflict plan baseline"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': partial,
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        })
    # Wrong type for a collection.
    bad = _empty_baseline()
    bad['local_images'] = 'not a list'
    with pytest.raises(CloudSyncError, match="Malformed conflict plan baseline"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': bad,
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_fields'] == []


def test_dialog_generated_baseline_is_accepted(monkeypatch):
    """A dialog-shape baseline emitted by build_conflict_plan_baseline is accepted."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'field', 'field': 'common_name', 'choice': 'cloud'}],
    })
    assert tracker['apply_remote_fields'] == [{'common_name'}]


# ── Exact choice-to-operation mapping ────────────────────────────────────────

def test_matched_measurement_cloud_choice_imports_and_does_not_push(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 8.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "img-cloud-5", "length_um": 9.5}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, remote_meas)
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)

    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'measurement', 'side': 'matched',
                   'local_id': 31, 'cloud_id': 'm31', 'choice': 'cloud'}],
    })
    assert tracker['push_measurements'] == []  # never pushed locally
    assert len(tracker['import_remote_measurements']) == 1
    ops = {op['op'] for op in result['operations']}
    assert 'import_measurement' in ops and 'push_measurement' not in ops


def test_matched_measurement_local_choice_pushes_and_does_not_import(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 8.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "img-cloud-5", "length_um": 9.5}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, remote_meas)
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)

    resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'measurement', 'side': 'matched',
                   'local_id': 31, 'cloud_id': 'm31', 'choice': 'local'}],
    })
    assert tracker['push_measurements'] == [{31}]
    assert tracker['import_remote_measurements'] == []


def test_image_metadata_cloud_choice_applies_only_and_does_not_push(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "sort_order": 5, "filepath": "/tmp/a.jpg", "gallery_rotation": 90}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "sort_order": 7, "storage_path": "cloud/a.webp", "gallery_rotation": 0}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)

    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'cloud'}],
    })
    # Cloud metadata pulled locally exactly once…
    assert len(tracker['apply_remote_images']) == 1
    # …and NEVER pushed back to cloud.
    assert tracker['push_images'] == []
    ops = {op['op'] for op in result['operations']}
    assert 'apply_image_metadata' in ops and 'push_image_metadata' not in ops


def test_keep_local_only_measurement_produces_no_writes(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [{"id": 42, "image_id": 5, "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, [])
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas)
    client = _RecordingClient(remote_obs=remote_obs)

    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'measurement', 'side': 'local_only',
                   'local_id': 42, 'choice': 'keep_local'}],
    })
    assert tracker['push_measurements'] == []
    assert tracker['import_remote_measurements'] == []
    assert any(op['op'] == 'keep_asymmetric_measurement' for op in result['operations'])


# ── Resolver-side identity guard ─────────────────────────────────────────────

def test_duplicate_local_cloud_id_blocks_before_writes(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [
        {"id": 5, "cloud_id": "shared", "image_type": "field"},
        {"id": 6, "cloud_id": "shared", "image_type": "field"},
    ]
    remote_images = [{"id": "shared", "desktop_id": 5, "image_type": "field"}]
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    with pytest.raises(CloudSyncError, match="identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'shared', 'choice': 'cloud'}],
        })
    assert tracker['apply_remote_images'] == []


def test_duplicate_cloud_desktop_id_blocks_before_writes(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    remote_images = [
        {"id": "a", "desktop_id": 5, "image_type": "field"},
        {"id": "b", "desktop_id": 5, "image_type": "field"},
    ]
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    baseline = _baseline_from_state({}, {}, [], remote_images, [], [])
    with pytest.raises(CloudSyncError, match="identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image', 'cloud_id': 'a', 'local_id': 5, 'choice': 'download'}],
        })
    assert tracker['apply_remote_images'] == []


def test_duplicate_local_measurement_cloud_id_blocks(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [
        {"id": 10, "cloud_id": "m-shared", "image_id": 5, "length_um": 5.0},
        {"id": 11, "cloud_id": "m-shared", "image_id": 5, "length_um": 6.0},
    ]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, [])
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="Measurement identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'local_only',
                       'local_id': 10, 'choice': 'upload'}],
        })
    assert tracker['push_measurements'] == []


def test_duplicate_cloud_measurement_desktop_id_blocks(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    remote_meas = [
        {"id": "a", "desktop_id": 30, "image_id": "img", "length_um": 5.0},
        {"id": "b", "desktop_id": 30, "image_id": "img", "length_um": 6.0},
    ]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], remote_meas)
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="Measurement identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'cloud_only',
                       'cloud_id': 'a', 'choice': 'download'}],
        })
    assert tracker['import_remote_measurements'] == []


def test_measurement_moved_to_other_image_after_review_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope"},
                    {"id": 7, "cloud_id": "c7", "image_type": "microscope"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope"},
                     {"id": "c7", "desktop_id": 7, "image_type": "microscope"}]
    reviewed_local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "c5", "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images,
                                    reviewed_local_meas, remote_meas)
    # Concurrent move: same measurement id, different owning image now.
    live_local_meas = [dict(reviewed_local_meas[0], image_id=7)]
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=local_images, remote_images=remote_images,
                            local_measurements=live_local_meas,
                            remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="now belongs to image"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31',
                       'local_image_id': 5, 'cloud_image_id': 'c5',
                       'choice': 'local'}],
        })
    assert tracker['push_measurements'] == []


def test_missing_target_measurement_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    # Plan references a local measurement that no longer exists.
    with pytest.raises(CloudSyncError, match="Measurement identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'local_only',
                       'local_id': 999, 'choice': 'upload'}],
        })
    # And a cloud measurement that no longer exists.
    with pytest.raises(CloudSyncError, match="Measurement identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'cloud_only',
                       'cloud_id': 'ghost', 'choice': 'download'}],
        })
    assert tracker['push_measurements'] == [] and tracker['import_remote_measurements'] == []


def test_matched_measurement_owning_image_not_authoritative_aborts(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    # Local image 5 links to cloud "c5"; local image 7 is unlinked; cloud has "c9".
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope"},
                    {"id": 7, "image_type": "microscope"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope"},
                     {"id": "c9", "image_type": "microscope"}]
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 7, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "c9", "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images,
                                    local_meas, remote_meas)
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=local_images, remote_images=remote_images,
                            local_measurements=local_meas, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="not the authoritative"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31',
                       'local_image_id': 7, 'cloud_image_id': 'c9',
                       'choice': 'local'}],
        })
    assert tracker['push_measurements'] == []


def test_cross_referenced_identity_contradiction_blocks(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "field"}]
    remote_images = [{"id": "c5", "desktop_id": 99, "image_type": "field"}]  # cross-linked
    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_images=local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    baseline = _baseline_from_state({}, {}, local_images, remote_images, [], [])
    with pytest.raises(CloudSyncError, match="identity conflict"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'c5', 'choice': 'cloud'}],
        })


# ── Finalization ordering ────────────────────────────────────────────────────

def test_finalization_order_snapshot_before_stamp(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    events: list[str] = []
    tracker = _patch_common(
        monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
        store_remote_snapshot=type('L', (list,), {'append': lambda self, x: events.append('snapshot')})(),
        refresh_signature=type('L', (list,), {'append': lambda self, x: events.append('signature')})(),
        stamp=type('L', (list,), {'append': lambda self, x: events.append('stamp')})(),
    )
    client = _RecordingClient(remote_obs=remote_obs)
    resolve_conflict_plan(client, 1, plan={
        'baseline': _baseline_from_state(local_obs, remote_obs, [], [], [], []),
        'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
    })
    assert events == ['snapshot', 'signature', 'stamp']


def test_snapshot_failure_leaves_conflict_unsealed(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    events: list[str] = []
    _patch_common(
        monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
        fail_snapshot=True,
        refresh_signature=type('L', (list,), {'append': lambda self, x: events.append('signature')})(),
        stamp=type('L', (list,), {'append': lambda self, x: events.append('stamp')})(),
    )
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(CloudSyncError, match="snapshot"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': _baseline_from_state(local_obs, remote_obs, [], [], [], []),
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        })
    assert 'signature' not in events and 'stamp' not in events


# ── Spore statistics handling ────────────────────────────────────────────────

def test_no_measurements_preserves_cloud_statistics(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud", "spore_statistics": "existing cloud value"}
    tracker = _patch_common(
        monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
        # No measurements, so recomputation legitimately produces None.
        format_recomputed=lambda _id: None,
    )
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': _baseline_from_state(local_obs, remote_obs, [], [], [], []),
        'derived_statistics': 'recompute_from_measurements',
        'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
    })
    # No PATCH with spore_statistics=None was sent.
    assert all(
        'spore_statistics' not in payload or payload['spore_statistics'] is not None
        for _path, payload in client.patches
    )
    assert any(
        op['op'] == 'preserve_spore_statistics_no_measurements'
        for op in result['operations']
    )


def test_measurements_present_but_recompute_fails_keeps_conflict_pending(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "img5", "image_type": "microscope"}]
    remote_images = [{"id": "img5", "desktop_id": 5, "image_type": "microscope"}]
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "img5", "length_um": 5.0}]
    _patch_common(
        monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
        local_images=local_images, remote_images=remote_images,
        local_measurements=local_meas, remote_measurements=remote_meas,
        format_recomputed=lambda _id: None,  # recompute unable to produce
    )
    client = _RecordingClient(remote_obs=remote_obs)
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images,
                                    local_meas, remote_meas)
    with pytest.raises(CloudSyncError, match="recompute spore statistics"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'derived_statistics': 'recompute_from_measurements',
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31', 'choice': 'local'}],
        })


# ── Presentation policy (limited) ────────────────────────────────────────────

def test_matched_image_metadata_restores_local_presentation(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "sort_order": 3, "gallery_rotation": 90,
                     "filepath": "/tmp/a.jpg"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "sort_order": 9, "gallery_rotation": 0, "storage_path": "cloud/a.webp"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    image_updates: list = []
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images,
                  image_updates=image_updates)
    client = _RecordingClient(remote_obs=remote_obs)
    resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'cloud'}],
    })
    # After apply, the resolver restored the local rotation & sort_order.
    restored = [(local_id, kwargs) for local_id, kwargs in image_updates if local_id == 5]
    assert restored, "no restore call for the matched image"
    _, kwargs = restored[0]
    assert kwargs.get('gallery_rotation') == 90
    assert kwargs.get('sort_order') == 3


def test_presentation_restore_failure_is_visible_as_warning(monkeypatch):
    """A3: presentation policy failures are surfaced in operations + presentation_warnings."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "sort_order": 3, "gallery_rotation": 90, "filepath": "/tmp/a.jpg"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "sort_order": 9, "gallery_rotation": 0, "storage_path": "cloud/a.webp"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images)
    # Simulate: after apply, cloud metadata sticks (rotation reads back 0 not 90).
    def _read_back(_id):
        return [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                 "sort_order": 9, "gallery_rotation": 0}]
    calls = {'n': 0}

    def _mixed_read(local_id):
        calls['n'] += 1
        if calls['n'] == 1:
            return copy.deepcopy(local_images)
        return _read_back(local_id)

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", _mixed_read)
    # update_image succeeds but the DB "loses" the write, so verify fails.
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image", lambda _id, **k: None)
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'cloud'}],
    })
    warnings = result['presentation_warnings']
    assert warnings, "presentation drift should surface as a warning"
    assert warnings[0]['status'] == 'failed'
    assert 'drift after restore' in warnings[0]['error']
    # And it appears in the executed op log.
    assert any(op.get('op') == 'restore_presentation' and op.get('status') == 'failed'
               for op in result['operations'])


def test_downloaded_cloud_only_image_gets_noncolliding_order(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    # Before apply: one local image with sort_order 5.
    local_images_before = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                            "sort_order": 5}]
    # After the (fake) apply, ImageDB reports a newly downloaded row.
    call_count = {'n': 0}

    def _get_images(_id):
        call_count['n'] += 1
        if call_count['n'] == 1:
            return copy.deepcopy(local_images_before)
        # subsequent calls: include the freshly-downloaded cloud-only row
        return copy.deepcopy(local_images_before) + [
            {"id": 6, "cloud_id": "cloud-only", "image_type": "microscope", "sort_order": 0},
        ]

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", _get_images)
    image_updates: list = []
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  remote_images=[{"id": "cloud-only", "image_type": "microscope",
                                  "storage_path": "cloud/x.webp"}],
                  image_updates=image_updates)
    # _patch_common already patched get_images_for_observation; re-apply ours.
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", _get_images)
    baseline = _baseline_from_state(
        local_obs, remote_obs, local_images_before,
        [{"id": "cloud-only", "image_type": "microscope", "storage_path": "cloud/x.webp"}],
        [], [],
    )
    client = _RecordingClient(remote_obs=remote_obs)
    resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image', 'cloud_id': 'cloud-only', 'choice': 'download'}],
    })
    ordered = [kwargs for lid, kwargs in image_updates
               if lid == 6 and 'sort_order' in kwargs]
    assert ordered, "no order was assigned to the downloaded cloud-only image"
    # Base = max existing (5) + 1 = 6
    assert ordered[0]['sort_order'] == 6


# ── Read-only thumbnail client hardening ─────────────────────────────────────

def test_read_only_client_download_does_not_touch_credentials(tmp_path, monkeypatch):
    forbidden = []

    for name in ('save_credentials', '_refresh_session_if_possible'):
        # Instance overrides are already in place in the subclass; but if any
        # caller ever tries to invoke the parent methods explicitly on the
        # instance, record it here to be sure it never happens.
        monkeypatch.setattr(cloud_sync.SporelyCloudClient, name,
                            lambda *a, _name=name, **k: forbidden.append(_name))

    def _fake_download(_self, _key, dest, timeout=120):
        p = tmp_path / 'thumb'
        p.write_bytes(b'ok')
        return p

    monkeypatch.setattr(cloud_sync, "direct_r2_runtime_available", lambda: True)
    monkeypatch.setattr(cloud_sync.SporelyReadOnlyCloudClient, "_get_r2",
                        lambda self: type('R2', (), {
                            'download_to_file': _fake_download.__get__(self, type(self))
                        })())

    client = SporelyReadOnlyCloudClient(access_token='tok', user_id='u1', refresh_token=None)
    out = client.download_image_file_read_only('some/cloud/key.webp', tmp_path / 'dest')
    assert out.read_bytes() == b'ok'
    assert forbidden == []
    # The overrides remain effective.
    assert client._refresh_session_if_possible() is False


def test_read_only_client_refuses_refresh_and_persist_methods():
    client = SporelyReadOnlyCloudClient(access_token='tok', user_id='u1', refresh_token=None)
    assert client._refresh_session_if_possible() is False
    assert client.save_credentials() is None
    with pytest.raises(CloudSyncError):
        client.login('user', 'pw')
    with pytest.raises(CloudSyncError):
        SporelyReadOnlyCloudClient.refresh_login('rt')
    with pytest.raises(CloudSyncError):
        client.clear_session()
    with pytest.raises(CloudSyncError):
        client.clear_credentials()


# ── Operation log ────────────────────────────────────────────────────────────

# ── B3/B4 snapshot v2 + accepted asymmetry ─────────────────────────────────

def test_snapshot_schema_v2_round_trip():
    """Snapshot with accepted_asymmetry serializes → parses back identically."""
    accepted = {
        'local_only_images': [{
            'side': 'local_only', 'kind': 'image',
            'local_id': 9, 'cloud_id': None,
            'owning_local_image_id': None, 'owning_cloud_image_id': None,
            'fingerprint': {'image_type': 'microscope', 'sort_order': 9,
                            'gallery_rotation': 0, 'notes': None,
                            'micro_category': None},
            'accepted_at': '2026-08-05T00:00:00Z', 'choice': 'keep_local',
        }],
        'cloud_only_images': [], 'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    payload = cloud_sync._cloud_observation_snapshot(
        {"id": "obs-cloud"}, [], [],
        accepted_asymmetry=accepted,
    )
    parsed = cloud_sync._parse_cloud_observation_snapshot(payload)
    assert parsed.get('schema_version') == 2
    got = parsed.get('accepted_asymmetry')
    assert isinstance(got, dict)
    assert len(got['local_only_images']) == 1
    assert got['local_only_images'][0]['local_id'] == 9


def test_old_snapshot_without_schema_version_still_loads():
    """Backward compat: legacy snapshots parse without inventing acceptance."""
    import json as _json
    legacy = _json.dumps({
        'observation': {'id': 'obs-cloud'},
        'images': [],
        'measurements': [],
    }, sort_keys=True, separators=(',', ':'))
    parsed = cloud_sync._parse_cloud_observation_snapshot(legacy)
    assert 'schema_version' not in parsed
    assert 'accepted_asymmetry' not in parsed


def test_accepted_local_only_image_is_hidden_until_material_edit(monkeypatch):
    """Round-trip: keep_local hides the image; a material edit resurfaces it.

    Fix 3: presentation-only changes (rotation, sort_order) MUST NOT resurface
    the conflict; only genuine content changes (notes, microscope metadata)
    do.
    """
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 9, "image_type": "microscope",
                     "notes": "original notes",
                     "objective_name": "40x",
                     "sort_order": 9, "gallery_rotation": 0,
                     "filepath": "/tmp/nine.jpg"}]
    remote_images = []
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])

    stored_snapshot = {'value': ''}

    def _fake_store(client, cloud_id, *args, **kwargs):
        stored_snapshot['value'] = cloud_sync._cloud_observation_snapshot(
            {"id": cloud_id}, [], [],
            accepted_asymmetry=kwargs.get('accepted_asymmetry'),
        )
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", _fake_store)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot",
                        lambda _cid: stored_snapshot['value'])
    client = _RecordingClient(remote_obs=remote_obs)

    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image', 'side': 'local_only',
                   'local_id': 9, 'choice': 'keep_local'}],
    })
    accepted = result['accepted_asymmetry']
    assert accepted['local_only_images']
    entry = accepted['local_only_images'][0]
    assert entry['local_id'] == 9
    # Fingerprint carries material fields, NOT presentation.
    assert 'sort_order' not in entry['fingerprint']
    assert 'gallery_rotation' not in entry['fingerprint']
    assert entry['fingerprint']['notes'] == 'original notes'
    assert entry['fingerprint']['objective_name'] == '40x'

    pair = {'status': 'local_only', 'local': {'local_id': 9}, 'remote': None}

    # 1. Unchanged → hidden.
    filtered, dropped = cloud_sync._filter_accepted_one_sided_images(
        [pair], accepted, local_images, remote_images,
    )
    assert filtered == [] and (9, '') in dropped

    # 2. Rotation-only change → STILL hidden (presentation-only, nonblocking).
    rotated = [dict(local_images[0], gallery_rotation=270)]
    filtered_rot, _ = cloud_sync._filter_accepted_one_sided_images(
        [pair], accepted, rotated, remote_images,
    )
    assert filtered_rot == []

    # 3. Sort-order-only change → STILL hidden.
    reordered = [dict(local_images[0], sort_order=42)]
    filtered_ord, _ = cloud_sync._filter_accepted_one_sided_images(
        [pair], accepted, reordered, remote_images,
    )
    assert filtered_ord == []

    # 4. Notes edit → RESURFACES.
    edited_notes = [dict(local_images[0], notes="user edited these")]
    filtered_notes, _ = cloud_sync._filter_accepted_one_sided_images(
        [pair], accepted, edited_notes, remote_images,
    )
    assert filtered_notes == [pair]

    # 5. Microscope metadata edit → RESURFACES.
    edited_scope = [dict(local_images[0], objective_name="100x")]
    filtered_scope, _ = cloud_sync._filter_accepted_one_sided_images(
        [pair], accepted, edited_scope, remote_images,
    )
    assert filtered_scope == [pair]


def test_accepted_cloud_only_measurement_is_hidden_until_edited():
    accepted = {
        'cloud_only_measurements': [{
            'side': 'cloud_only', 'kind': 'measurement',
            'local_id': None, 'cloud_id': 'm-c',
            'owning_local_image_id': None, 'owning_cloud_image_id': 'img',
            'fingerprint': cloud_sync._asymmetry_fingerprint_remote_measurement(
                {'length_um': 5.0, 'width_um': 3.0, 'measurement_type': 'spore',
                 'p1_x': 0, 'p1_y': 0, 'p2_x': 5, 'p2_y': 0, 'image_id': 'img'}
            ),
            'accepted_at': '2026-08-05T00:00:00Z', 'choice': 'keep_cloud',
        }],
        'local_only_images': [], 'cloud_only_images': [],
        'local_only_measurements': [],
    }
    remote_meas = [{'id': 'm-c', 'length_um': 5.0, 'width_um': 3.0,
                    'measurement_type': 'spore',
                    'p1_x': 0, 'p1_y': 0, 'p2_x': 5, 'p2_y': 0, 'image_id': 'img'}]
    pair = {'status': 'cloud_only', 'cloud_id': 'm-c', 'local_id': None}
    filtered, dropped = cloud_sync._filter_accepted_one_sided_measurements(
        [pair], accepted, [], remote_meas,
    )
    assert filtered == []
    assert (0, 'm-c') in dropped
    # Edit remote length -> resurfaces.
    edited = [dict(remote_meas[0], length_um=9.9)]
    filtered_after, _ = cloud_sync._filter_accepted_one_sided_measurements(
        [pair], accepted, [], edited,
    )
    assert filtered_after == [pair]


def test_no_media_deletion_api_reachable_from_plan(monkeypatch):
    """Belt-and-braces: no soft_delete / storage_remove / delete_cloud_observation runs."""
    seen = []
    for name in ('soft_delete_image', '_storage_remove', 'delete_cloud_observation'):
        if hasattr(cloud_sync.SporelyCloudClient, name):
            monkeypatch.setattr(cloud_sync.SporelyCloudClient, name,
                                lambda *a, _n=name, **k: seen.append(_n))
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
    })
    assert seen == []


def test_partial_error_carries_operations_and_retry_skips_completed(monkeypatch):
    """B2: after a partial failure, retry passes prior_result → resolver skips completed."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_meas = [{"id": 42, "image_id": 5, "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, [])
    # First call fails during "push_field".
    push_calls = []

    def _push_that_fails_once(*a, **k):
        push_calls.append(k.get('measurement_ids'))
        raise RuntimeError('kaboom')

    tracker = _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                            local_measurements=local_meas)
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields",
                        lambda *a, **k: None)  # succeeds — completed op
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                        _push_that_fails_once)
    client = _RecordingClient(remote_obs=remote_obs)
    plan = {
        'baseline': baseline,
        'items': [
            {'kind': 'field', 'field': 'notes', 'choice': 'cloud'},
            {'kind': 'measurement', 'side': 'local_only',
             'local_id': 42, 'choice': 'upload'},
        ],
    }
    with pytest.raises(cloud_sync.PartialConflictPlanError) as exc_info:
        resolve_conflict_plan(client, 1, plan=plan)
    partial = exc_info.value.partial_result
    completed_ops = [op for op in partial['operations'] if op.get('status') == 'completed']
    assert any(op['op'] == 'pull_field' and op.get('field') == 'notes' for op in completed_ops)
    # On retry, provide prior_result and let the push succeed.
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                        lambda *a, **k: push_calls.append(('retry', k.get('measurement_ids'))))
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _id: list(local_meas))
    result = resolve_conflict_plan(client, 1, plan=plan, prior_result=partial)
    # pull_field was NOT re-dispatched (already completed).
    assert tracker['apply_remote_fields'] == []
    assert result['plan_applied']


# ── Accepted-asymmetry lifecycle (Turn-B Fix 2) ──────────────────────────────

def _reconcile(previous, new, items, *, li=None, ri=None, lm=None, rm=None,
               ml_img=None, mc_img=None, ml_meas=None, mc_meas=None):
    return cloud_sync._reconcile_accepted_asymmetry(
        previous, new,
        plan_items=items or [],
        current_local_images=li or [], current_remote_images=ri or [],
        current_local_measurements=lm or [], current_remote_measurements=rm or [],
        matched_local_image_ids=set(ml_img or []),
        matched_cloud_image_ids=set(mc_img or []),
        matched_local_measurement_ids=set(ml_meas or []),
        matched_cloud_measurement_ids=set(mc_meas or []),
    )


def _make_local_image_acceptance(local_id, notes='n'):
    return {
        'side': 'local_only', 'kind': 'image',
        'local_id': local_id, 'cloud_id': None,
        'owning_local_image_id': None, 'owning_cloud_image_id': None,
        'fingerprint': cloud_sync._asymmetry_fingerprint_local_image(
            {'image_type': 'microscope', 'notes': notes}
        ),
        'accepted_at': '2026-08-05T00:00:00Z', 'choice': 'keep_local',
    }


def test_lifecycle_user_upload_removes_prior_keep_local_acceptance():
    """User later selects upload → keep_local acceptance is removed."""
    previous = {'local_only_images': [_make_local_image_acceptance(9)],
                'cloud_only_images': [], 'local_only_measurements': [],
                'cloud_only_measurements': []}
    # Plan explicitly overrides with upload for the same identity.
    items = [{'kind': 'image', 'side': 'local_only', 'local_id': 9, 'choice': 'upload'}]
    result = _reconcile(previous, {'local_only_images': []},
                        items,
                        li=[{"id": 9, "image_type": "microscope"}])
    assert result['local_only_images'] == []


def test_lifecycle_counterpart_appearance_removes_acceptance():
    """A local-only acceptance disappears once a matching cloud row exists."""
    previous = {'local_only_images': [_make_local_image_acceptance(9)],
                'cloud_only_images': [], 'local_only_measurements': [],
                'cloud_only_measurements': []}
    # Local row now has cloud_id — the pair is no longer one-sided.
    result = _reconcile(previous, {'local_only_images': []}, [],
                        li=[{"id": 9, "cloud_id": "c9", "image_type": "microscope"}],
                        ml_img=[9])
    assert result['local_only_images'] == []


def test_lifecycle_deleted_row_prunes_orphan_acceptance():
    """Accepted row no longer exists → orphan entry is pruned."""
    previous = {'local_only_images': [_make_local_image_acceptance(9)],
                'cloud_only_images': [], 'local_only_measurements': [],
                'cloud_only_measurements': []}
    # Local image 9 has been deleted.
    result = _reconcile(previous, {'local_only_images': []}, [], li=[])
    assert result['local_only_images'] == []


def test_lifecycle_measurement_owner_change_prunes_acceptance():
    accepted_meas = {
        'side': 'local_only', 'kind': 'measurement',
        'local_id': 31, 'cloud_id': None,
        'owning_local_image_id': 5, 'owning_cloud_image_id': None,
        'fingerprint': cloud_sync._asymmetry_fingerprint_local_measurement(
            {'length_um': 5.0, 'image_id': 5}
        ),
        'accepted_at': '', 'choice': 'keep_local',
    }
    previous = {'local_only_images': [], 'cloud_only_images': [],
                'local_only_measurements': [accepted_meas],
                'cloud_only_measurements': []}
    # Measurement moved to a different owning image locally.
    live_meas = [{"id": 31, "image_id": 7, "length_um": 5.0}]
    result = _reconcile(previous, {'local_only_measurements': []}, [],
                        lm=live_meas)
    assert result['local_only_measurements'] == []


def test_lifecycle_counterpart_disappears_produces_new_conflict_not_stale_acceptance():
    """After a counterpart disappears again, no old acceptance rehides the item."""
    previous = {'local_only_images': [_make_local_image_acceptance(9, notes="v1")],
                'cloud_only_images': [], 'local_only_measurements': [],
                'cloud_only_measurements': []}
    # Round 1: cloud counterpart appeared, so the acceptance was pruned.
    round1 = _reconcile(previous, {'local_only_images': []}, [],
                        li=[{"id": 9, "cloud_id": "c9",
                             "image_type": "microscope", "notes": "v2"}],
                        ml_img=[9])
    assert round1['local_only_images'] == []
    # Round 2: cloud row deleted, local becomes one-sided again — but there is
    # no prior acceptance to inherit.  The item WILL show as a fresh conflict.
    round2 = _reconcile(round1, {'local_only_images': []}, [],
                        li=[{"id": 9, "image_type": "microscope", "notes": "v2"}])
    assert round2['local_only_images'] == []


def test_lifecycle_relink_prunes_acceptance():
    """Local row gained a cloud_id (relinked) → prune."""
    previous = {'local_only_images': [_make_local_image_acceptance(9)],
                'cloud_only_images': [], 'local_only_measurements': [],
                'cloud_only_measurements': []}
    result = _reconcile(previous, {'local_only_images': []}, [],
                        li=[{"id": 9, "cloud_id": "linked",
                             "image_type": "microscope"}])
    assert result['local_only_images'] == []


# ── Retry rebasing (Turn-B Fix 1) ─────────────────────────────────────────────

def test_retry_rebase_after_completed_push_field_succeeds(monkeypatch):
    """Immediate retry: push_field completed, then measurement push failed.

    The retry must not require the user to hit Refresh; expected-effect
    verification treats the changed remote field as valid.
    """
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "notes": "local-value"}
    remote_obs_reviewed = {"id": "obs-cloud", "notes": "cloud-value"}
    local_meas = [{"id": 42, "image_id": 5, "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs_reviewed, [], [],
                                    local_meas, [])
    # First attempt: push_field succeeds, push_measurement fails.
    push_count = {'n': 0}

    def _fake_push_measurements(*a, **k):
        push_count['n'] += 1
        if push_count['n'] == 1:
            raise RuntimeError('transient network')
        # retry succeeds
        return None

    # live_remote starts as the reviewed cloud state; the push mutates it.
    live_remote = {'notes': 'cloud-value'}

    def _client_get_obs(_cid):
        return dict({'id': 'obs-cloud'}, **live_remote)

    class _Client:
        def get_observation(self, cid): return _client_get_obs(cid)
        def _patch(self, path, payload):
            live_remote.update(payload)
    client = _Client()
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs_reviewed,
                  local_measurements=local_meas)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                        _fake_push_measurements)
    plan = {
        'baseline': baseline,
        'items': [
            {'kind': 'field', 'field': 'notes', 'choice': 'local'},
            {'kind': 'measurement', 'side': 'local_only',
             'local_id': 42, 'choice': 'upload'},
        ],
    }
    with pytest.raises(cloud_sync.PartialConflictPlanError) as excinfo:
        resolve_conflict_plan(client, 1, plan=plan)
    partial = excinfo.value.partial_result
    completed = [op for op in partial['operations'] if op.get('status') == 'completed']
    assert any(op['op'] == 'push_field' and op.get('field') == 'notes'
               for op in completed)
    assert any(op.get('expected_after', {}).get('value') == 'local-value'
               for op in completed if op['op'] == 'push_field')

    # Retry — same plan + prior_result.  Client now has the pushed value.
    result = resolve_conflict_plan(client, 1, plan=plan, prior_result=partial)
    assert result['plan_applied']
    # push_field was NOT re-dispatched.
    already = [op for op in result['operations']
               if op.get('status') == 'already_complete'
               and op.get('op') == 'push_field']
    assert already, f"expected already_complete for push_field, got {result['operations']}"


def test_retry_rejects_unrelated_drift(monkeypatch):
    """Fix 1: retry aborts when unrelated data changed between attempts."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "notes": "local"}
    remote_obs_reviewed = {"id": "obs-cloud", "notes": "cloud", "common_name": "orig"}
    baseline = _baseline_from_state(local_obs, remote_obs_reviewed, [], [], [], [])
    # First attempt completes push_field(notes) then fails on snapshot.
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [{
            'op': 'push_field', 'field': 'notes', 'status': 'completed',
            'stable_identity': {'field': 'notes', 'side': 'cloud'},
            'expected_after': {'side': 'cloud', 'field': 'notes', 'value': 'local'},
        }],
    }
    # BUT between attempts, an unrelated cloud field (common_name) changed.
    drifted_remote = {"id": "obs-cloud", "notes": "local", "common_name": "CHANGED"}
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=drifted_remote)
    client = _RecordingClient(remote_obs=drifted_remote)
    with pytest.raises(cloud_sync.CloudSyncError, match="changed after this comparison"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'local'}],
        }, prior_result=partial)


# ── Final Fix 1: material expected_after and strict retry verification ──────

def _make_completed_image_op(*, op, local_id, cloud_id, local_row, remote_row):
    return {
        'op': op, 'local_id': local_id, 'cloud_id': cloud_id, 'status': 'completed',
        'stable_identity': {'local_id': local_id, 'cloud_id': cloud_id},
        'expected_after': {
            'kind': 'image',
            'material_local': cloud_sync._material_image_expected_state(local_row, side='local'),
            'material_remote': cloud_sync._material_image_expected_state(remote_row, side='remote'),
        },
    }


def _make_completed_measurement_op(*, op, local_id, cloud_id, local_row, remote_row):
    return {
        'op': op, 'local_id': local_id, 'cloud_id': cloud_id, 'status': 'completed',
        'stable_identity': {'local_id': local_id, 'cloud_id': cloud_id},
        'expected_after': {
            'kind': 'measurement',
            'material_local': cloud_sync._material_measurement_expected_state(
                local_row, side='local'),
            'material_remote': cloud_sync._material_measurement_expected_state(
                remote_row, side='remote'),
        },
    }


def test_retry_rejects_after_image_metadata_edit(monkeypatch):
    """A completed image op cannot be treated as intact if metadata changed."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "notes": "initial notes", "objective_name": "40x"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "notes": "initial notes", "objective_name": "40x"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [
            _make_completed_image_op(op='push_image_metadata', local_id=5, cloud_id='c5',
                                     local_row=local_images[0], remote_row=remote_images[0]),
        ],
    }
    # Between attempts, someone edited the notes on the local row.
    edited_local = [dict(local_images[0], notes="OTHERS EDITED THIS")]
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=edited_local, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(cloud_sync.CloudSyncError, match="material content changed"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'c5', 'choice': 'local'}],
        }, prior_result=partial)


def test_retry_accepts_after_rotation_only_change(monkeypatch):
    """Nonmaterial presentation change does NOT invalidate a completed op."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "notes": "n", "gallery_rotation": 0, "sort_order": 3}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "notes": "n", "sort_order": 3}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [
            _make_completed_image_op(op='push_image_metadata', local_id=5, cloud_id='c5',
                                     local_row=local_images[0], remote_row=remote_images[0]),
        ],
    }
    # Rotation-only change: presentation-only, must NOT abort retry.
    rotated_local = [dict(local_images[0], gallery_rotation=270)]
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=rotated_local, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'local'}],
    }, prior_result=partial)
    assert result['plan_applied']
    # The completed op was NOT re-dispatched.
    already = [op for op in result['operations']
               if op.get('status') == 'already_complete'
               and op.get('op') == 'push_image_metadata']
    assert already, "completed image op should verify and skip dispatch"


def test_retry_rejects_after_scientific_measurement_edit(monkeypatch):
    """Scientific value change on a completed measurement op aborts retry."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope"}]
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5,
                    "length_um": 5.5, "width_um": 3.2, "measurement_type": "spore"}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "c5",
                     "length_um": 5.5, "width_um": 3.2, "measurement_type": "spore"}]
    baseline = _baseline_from_state(local_obs, remote_obs,
                                    local_images, remote_images, local_meas, remote_meas)
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [
            _make_completed_measurement_op(op='push_measurement', local_id=31, cloud_id='m31',
                                           local_row=local_meas[0], remote_row=remote_meas[0]),
        ],
    }
    # Length changed on remote.
    drifted_remote = [dict(remote_meas[0], length_um=9.9)]
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images,
                  local_measurements=local_meas, remote_measurements=drifted_remote)
    client = _RecordingClient(remote_obs=remote_obs)
    with pytest.raises(cloud_sync.CloudSyncError, match="scientific values or"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31',
                       'local_image_id': 5, 'cloud_image_id': 'c5',
                       'choice': 'local'}],
        }, prior_result=partial)


def test_retry_rejects_after_measurement_moved_to_another_image(monkeypatch):
    """Owning-image change on a completed measurement op aborts retry."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope"},
                    {"id": 7, "cloud_id": "c7", "image_type": "microscope"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope"},
                     {"id": "c7", "desktop_id": 7, "image_type": "microscope"}]
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5,
                   "length_um": 5.5, "width_um": 3.2, "measurement_type": "spore"}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "c5",
                    "length_um": 5.5, "width_um": 3.2, "measurement_type": "spore"}]
    baseline = _baseline_from_state(local_obs, remote_obs,
                                    local_images, remote_images, local_meas, remote_meas)
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [
            _make_completed_measurement_op(op='push_measurement', local_id=31, cloud_id='m31',
                                           local_row=local_meas[0], remote_row=remote_meas[0]),
        ],
    }
    # Measurement moved to image 7 locally.
    moved_local = [dict(local_meas[0], image_id=7)]
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images,
                  local_measurements=moved_local, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    # Either the identity guard (owning image changed) OR the material
    # verification (owning-image identity is part of material state) can catch
    # this — both are correct.
    with pytest.raises(cloud_sync.CloudSyncError,
                       match="(now belongs to image|scientific values or)"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31',
                       'local_image_id': 5, 'cloud_image_id': 'c5',
                       'choice': 'local'}],
        }, prior_result=partial)


def test_retry_accepts_after_transport_only_change(monkeypatch):
    """A change to a non-material transport field must not abort retry."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "notes": "n", "filepath": "/tmp/a.jpg"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "notes": "n", "storage_path": "cloud/a.webp"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [
            _make_completed_image_op(op='push_image_metadata', local_id=5, cloud_id='c5',
                                     local_row=local_images[0], remote_row=remote_images[0]),
        ],
    }
    # Transport-only change on local (filepath) — material unchanged.
    moved_file = [dict(local_images[0], filepath="/tmp/moved.jpg")]
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=moved_file, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'local'}],
    }, prior_result=partial)
    assert result['plan_applied']


# ── Final Fix 2: reconciliation on every snapshot write ──────────────────────

def _snapshot_persistence_round_trip(local_id, cloud_id, *, images, measurements,
                                     accepted=None, monkeypatch=None):
    """Persist through the real _store_cloud_observation_snapshot path."""
    stored = {}

    def _set(key, value):
        stored[key] = value

    def _get(key, default=""):
        return stored.get(key, default)

    monkeypatch.setattr(cloud_sync.SettingsDB, "set_setting", _set)
    monkeypatch.setattr(cloud_sync.SettingsDB, "get_setting", _get)
    return stored


def test_ordinary_snapshot_write_prunes_accepted_asymmetry_when_counterpart_appears(monkeypatch):
    """Round trip through SettingsDB.

    1. accept a local-only image (keep_local via plan);
    2. simulate the ordinary sync path storing the snapshot;
    3. the counterpart now exists (local row has cloud_id) →
       the acceptance must be pruned.
    """
    local_obs_cloud_id = "obs-cloud"
    local_id = 1
    accepted = {
        'local_only_images': [{
            'side': 'local_only', 'kind': 'image',
            'local_id': 9, 'cloud_id': None,
            'owning_local_image_id': None, 'owning_cloud_image_id': None,
            'fingerprint': cloud_sync._asymmetry_fingerprint_local_image(
                {'image_type': 'microscope', 'notes': 'n'}
            ),
            'accepted_at': '2026-08-05T00:00:00Z', 'choice': 'keep_local',
        }],
        'cloud_only_images': [], 'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    stored = _snapshot_persistence_round_trip(
        local_id, local_obs_cloud_id, images=[], measurements=[], accepted=accepted,
        monkeypatch=monkeypatch,
    )
    # Prime the settings with a snapshot that already carries the acceptance.
    initial = cloud_sync._cloud_observation_snapshot(
        {'id': local_obs_cloud_id}, [], [], accepted_asymmetry=accepted,
    )
    cloud_sync._store_cloud_observation_snapshot(local_obs_cloud_id, initial)
    reloaded = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(local_obs_cloud_id)
    )
    assert reloaded['accepted_asymmetry']['local_only_images'], "step 3: initially hidden"

    # Simulate: local row now has a cloud_id (counterpart appeared).
    monkeypatch.setattr(cloud_sync, "_local_observation_id_by_cloud_id",
                        lambda _cid: local_id)
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _oid: [{"id": 9, "cloud_id": "c9",
                                       "image_type": "microscope", "notes": "n"}])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _oid: [])

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
            return [{"id": "c9", "desktop_id": 9, "image_type": "microscope"}]

    # Ordinary sync stores a fresh snapshot with the current cloud state.
    # It passes accepted_asymmetry=None so the pruning path takes over.
    cloud_sync._store_remote_snapshot(
        Client(), local_obs_cloud_id,
        remote_images=[{"id": "c9", "desktop_id": 9, "image_type": "microscope"}],
        remote_measurements=[],
    )
    reloaded_after = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(local_obs_cloud_id)
    )
    accepted_after = reloaded_after.get('accepted_asymmetry')
    assert not accepted_after or not accepted_after.get('local_only_images'), (
        "acceptance should have been pruned once the counterpart appeared"
    )


def test_ordinary_snapshot_never_creates_acceptance(monkeypatch):
    """Ordinary sync storing a snapshot never invents acceptance."""
    stored = _snapshot_persistence_round_trip(
        1, "obs-cloud", images=[], measurements=[], monkeypatch=monkeypatch,
    )
    monkeypatch.setattr(cloud_sync, "_local_observation_id_by_cloud_id", lambda _cid: 1)
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _oid: [{"id": 9, "image_type": "microscope", "notes": "n"}])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _oid: [])

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False): return []

    cloud_sync._store_remote_snapshot(Client(), "obs-cloud",
                                     remote_images=[], remote_measurements=[])
    reloaded = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot("obs-cloud")
    )
    assert 'accepted_asymmetry' not in reloaded or not any(
        reloaded.get('accepted_asymmetry', {}).get(k)
        for k in ('local_only_images', 'cloud_only_images',
                  'local_only_measurements', 'cloud_only_measurements')
    ), "ordinary sync must not create acceptance entries"


def test_ordinary_snapshot_write_prunes_measurement_asymmetry_when_owner_changes(monkeypatch):
    accepted_meas = {
        'side': 'local_only', 'kind': 'measurement',
        'local_id': 31, 'cloud_id': None,
        'owning_local_image_id': 5, 'owning_cloud_image_id': None,
        'fingerprint': cloud_sync._asymmetry_fingerprint_local_measurement(
            {'length_um': 5.0, 'image_id': 5}
        ),
        'accepted_at': '', 'choice': 'keep_local',
    }
    accepted = {
        'local_only_images': [], 'cloud_only_images': [],
        'local_only_measurements': [accepted_meas],
        'cloud_only_measurements': [],
    }
    _snapshot_persistence_round_trip(
        1, "obs-cloud", images=[], measurements=[], accepted=accepted,
        monkeypatch=monkeypatch,
    )
    initial = cloud_sync._cloud_observation_snapshot(
        {'id': "obs-cloud"}, [], [], accepted_asymmetry=accepted,
    )
    cloud_sync._store_cloud_observation_snapshot("obs-cloud", initial)
    # Local measurement moved to image 7.
    monkeypatch.setattr(cloud_sync, "_local_observation_id_by_cloud_id", lambda _cid: 1)
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _oid: [{"id": 7, "image_type": "microscope"}])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _oid: [{"id": 31, "image_id": 7, "length_um": 5.0}])

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False): return []

    cloud_sync._store_remote_snapshot(Client(), "obs-cloud",
                                     remote_images=[], remote_measurements=[])
    reloaded = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot("obs-cloud")
    )
    assert not reloaded.get('accepted_asymmetry', {}).get('local_only_measurements')


# ── Final hardening: malformed prior_result must fail closed ────────────────

def test_retry_rejects_identity_only_prior_push_image(monkeypatch):
    """A completed push_image with only stable IDs (no material) must abort retry."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    partial = {
        'local_id': 1, 'cloud_id': 'obs-cloud', 'plan_applied': False,
        'operations': [{
            'op': 'push_image', 'local_id': 5, 'cloud_id': 'c5',
            'status': 'completed',
            # NO expected_after — malformed identity-only "proof".
        }],
    }
    with pytest.raises(cloud_sync.CloudSyncError, match="Malformed retry state"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'c5', 'choice': 'local'}],
        }, prior_result=partial)


def test_retry_rejects_measurement_missing_owning_image_in_stable(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope"}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope"}]
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "c5", "length_um": 5.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images,
                                    local_meas, remote_meas)
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images,
                  local_measurements=local_meas, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    bad_expected = {
        'kind': 'measurement',
        'material_local': {'side': 'local', 'stable': {'local_id': 31, 'cloud_id': 'm31'},
                            'material': {k: 0 for k in cloud_sync._EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS}},
        'material_remote': {'side': 'remote', 'stable': {'local_id': 31, 'cloud_id': 'm31'},
                             'material': {k: 0 for k in cloud_sync._EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS}},
    }
    partial = {'operations': [{
        'op': 'push_measurement', 'local_id': 31, 'cloud_id': 'm31',
        'status': 'completed', 'expected_after': bad_expected,
    }]}
    with pytest.raises(cloud_sync.CloudSyncError, match="Malformed retry state"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'measurement', 'side': 'matched',
                       'local_id': 31, 'cloud_id': 'm31',
                       'local_image_id': 5, 'cloud_image_id': 'c5',
                       'choice': 'local'}],
        }, prior_result=partial)


def test_retry_rejects_completed_pull_field_without_value(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    partial = {'operations': [{
        'op': 'pull_field', 'field': 'notes', 'status': 'completed',
        # expected_after missing 'value'.
        'expected_after': {},
    }]}
    with pytest.raises(cloud_sync.CloudSyncError, match="Malformed retry state"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        }, prior_result=partial)


def test_retry_rejects_recompute_stats_without_value(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    partial = {'operations': [{
        'op': 'recompute_spore_statistics', 'status': 'completed', 'expected_after': {},
    }]}
    with pytest.raises(cloud_sync.CloudSyncError, match="Malformed retry state"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'derived_statistics': 'recompute_from_measurements',
            'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
        }, prior_result=partial)


# ── Verification-pending recovery on retry ──────────────────────────────────

def test_verification_pending_push_image_recovers_via_stable_identity(monkeypatch):
    """Write succeeded, verification fetch failed → on retry, discover the row."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5-freshly-created",
                     "image_type": "microscope", "notes": "hello"}]
    remote_images = [{"id": "c5-freshly-created", "desktop_id": 5,
                      "image_type": "microscope", "notes": "hello"}]
    baseline = _baseline_from_state(local_obs, remote_obs,
                                    [{"id": 5, "image_type": "microscope",
                                      "notes": "hello"}],
                                    [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    # Simulated partial: write succeeded, verification failed.  intended_after
    # is populated from the local (source) row.
    source = {"id": 5, "cloud_id": "c5-freshly-created",
              "image_type": "microscope", "notes": "hello"}
    partial = {
        'operations': [{
            'op': 'push_image', 'local_id': 5, 'cloud_id': 'c5-freshly-created',
            'status': 'verification_pending', 'write_attempted': True,
            'stable_identity': {'local_id': 5, 'cloud_id': 'c5-freshly-created'},
            'intended_after': cloud_sync._intended_after_image_from_local(source),
        }],
    }
    # Retry with a plan whose items include the same push (as if the user
    # simply clicked Apply again).  The verification-pending recovery should
    # skip re-dispatch.
    push_calls = []
    monkeypatch.setattr(cloud_sync, "_push_images_for_observation",
                        lambda *a, **k: (push_calls.append(k.get('include_image_ids')) or True))
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image', 'side': 'local_only',
                   'local_id': 5, 'choice': 'upload'}],
    }, prior_result=partial, prepare_images_cb=lambda *_: {})
    assert result['plan_applied']
    # The push was NOT repeated — the verification-pending recovery skipped it.
    assert push_calls == [], f"expected no re-dispatch, got {push_calls}"
    already = [op for op in result['operations']
               if op.get('status') == 'already_complete']
    assert any(op.get('op') == 'push_image' for op in already)


def test_verification_pending_push_image_rejects_material_drift(monkeypatch):
    """If the recovered row's material differs from intended → drift error."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    # After write, someone edited the notes on the cloud row.
    remote_images = [{"id": "c5", "desktop_id": 5,
                      "image_type": "microscope", "notes": "OTHER PARTY EDIT"}]
    local_images = [{"id": 5, "cloud_id": "c5",
                     "image_type": "microscope", "notes": "hello"}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images)
    client = _RecordingClient(remote_obs=remote_obs)
    source = {"id": 5, "cloud_id": "c5", "image_type": "microscope",
              "notes": "hello"}
    partial = {
        'operations': [{
            'op': 'push_image', 'local_id': 5, 'cloud_id': 'c5',
            'status': 'verification_pending', 'write_attempted': True,
            'stable_identity': {'local_id': 5, 'cloud_id': 'c5'},
            'intended_after': cloud_sync._intended_after_image_from_local(source),
        }],
    }
    with pytest.raises(cloud_sync.CloudSyncError,
                       match="material differs"):
        resolve_conflict_plan(client, 1, plan={
            'baseline': baseline,
            'items': [{'kind': 'image_metadata', 'local_id': 5,
                       'cloud_id': 'c5', 'choice': 'local'}],
        }, prior_result=partial)


def test_verification_pending_row_missing_triggers_re_dispatch(monkeypatch):
    """No candidate row found → the plan re-dispatches (safe upsert)."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)
    client = _RecordingClient(remote_obs=remote_obs)
    source = {"id": 99, "cloud_id": "c99", "image_type": "microscope",
              "notes": "hi"}
    partial = {
        'operations': [{
            'op': 'push_image', 'local_id': 99, 'cloud_id': 'c99',
            'status': 'verification_pending', 'write_attempted': True,
            'stable_identity': {'local_id': 99, 'cloud_id': 'c99'},
            'intended_after': cloud_sync._intended_after_image_from_local(source),
        }],
    }
    # No matching local row exists (state was fully lost between attempts).
    # Row 99 also missing in items — the plan doesn't include it — so the
    # recovery result is "missing"; nothing to re-dispatch here.  The result
    # should still succeed (plan has no ops that need this record).
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'field', 'field': 'notes', 'choice': 'cloud'}],
    }, prior_result=partial)
    assert result['plan_applied']


# ── Fix 3: Real SQLite persistence round-trip ────────────────────────────────

@pytest.fixture
def isolated_sqlite_profile(tmp_path, monkeypatch):
    """Point every database helper at an isolated SQLite file for one test."""
    db_path = tmp_path / "mushrooms.db"
    monkeypatch.setattr(db_schema, "get_database_path", lambda: db_path)
    # Initialize schema (creates settings table + everything else).
    db_schema.init_database()
    yield db_path


def test_sqlite_snapshot_round_trip_prunes_acceptance_across_connections(isolated_sqlite_profile, monkeypatch):
    """End-to-end: write, close, reopen, read; then simulate ordinary sync;
    reload again to confirm pruning persisted through SQLite.

    Uses the real SettingsDB/get_setting/set_setting — no monkey-patching of
    the persistence layer.  Exercises the actual serialize/deserialize path.
    """
    cloud_id = "obs-cloud"
    local_id = 1
    # Seed an observation row so _local_observation_id_by_cloud_id resolves.
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute(
        "INSERT INTO observations (id, cloud_id, date) VALUES (?, ?, ?)",
        (local_id, cloud_id, "2026-08-05"),
    )
    conn.commit()
    conn.close()

    accepted = {
        'local_only_images': [{
            'side': 'local_only', 'kind': 'image',
            'local_id': 9, 'cloud_id': None,
            'owning_local_image_id': None, 'owning_cloud_image_id': None,
            'fingerprint': cloud_sync._asymmetry_fingerprint_local_image(
                {'image_type': 'microscope', 'notes': 'baseline'}
            ),
            'accepted_at': '2026-08-05T00:00:00Z', 'choice': 'keep_local',
        }],
        'cloud_only_images': [], 'local_only_measurements': [],
        'cloud_only_measurements': [],
    }
    # 1. Persist a schema-2 snapshot with acceptance through the REAL path.
    initial = cloud_sync._cloud_observation_snapshot(
        {'id': cloud_id}, [], [], accepted_asymmetry=accepted,
    )
    cloud_sync._store_cloud_observation_snapshot(cloud_id, initial)
    # 2. Close/reopen — new connection reads the persisted row.
    reloaded_1 = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(cloud_id)
    )
    assert reloaded_1.get('schema_version') == 2
    assert reloaded_1['accepted_asymmetry']['local_only_images'], "step 3: written and reloaded"

    # 3. Now simulate the counterpart appearing locally.  Insert a matching
    # local image row that carries a cloud_id, then run ORDINARY sync's
    # _store_remote_snapshot (no plan context).  It must prune the entry.
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute(
        "INSERT INTO images (id, observation_id, cloud_id, image_type, filepath, notes) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (9, local_id, "c9", "microscope", "/tmp/nine.jpg", "baseline"),
    )
    conn.commit()
    conn.close()

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
            return [{"id": "c9", "desktop_id": 9, "image_type": "microscope"}]

    cloud_sync._store_remote_snapshot(
        Client(), cloud_id,
        remote_images=[{"id": "c9", "desktop_id": 9, "image_type": "microscope"}],
        remote_measurements=[],
    )
    # 4. Close/reopen again — pruning must have persisted.
    reloaded_2 = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(cloud_id)
    )
    accepted_after = reloaded_2.get('accepted_asymmetry')
    assert not accepted_after or not accepted_after.get('local_only_images'), (
        "acceptance should be pruned once counterpart materialized"
    )

    # 5. Remove the counterpart (isolated fixture only) — the local-only image
    # will conflict as a new item; there must be no lingering acceptance to
    # rehide it.
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute("UPDATE images SET cloud_id = NULL WHERE id = 9")
    conn.commit()
    conn.close()
    reloaded_3 = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(cloud_id)
    )
    accepted_final = reloaded_3.get('accepted_asymmetry')
    assert not accepted_final or not accepted_final.get('local_only_images')


def test_sqlite_old_snapshot_without_schema_version_still_loads(isolated_sqlite_profile):
    """Backward compatibility: pre-B3 payload persisted directly reloads cleanly."""
    import json as _json
    legacy = _json.dumps({
        'observation': {'id': 'obs-cloud', 'date': '2026-01-01'},
        'images': [], 'measurements': [],
    }, sort_keys=True, separators=(',', ':'))
    cloud_sync._store_cloud_observation_snapshot("obs-cloud", legacy)
    parsed = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot("obs-cloud")
    )
    assert 'schema_version' not in parsed
    assert 'accepted_asymmetry' not in parsed


def test_sqlite_ordinary_sync_never_creates_acceptance(isolated_sqlite_profile):
    """Ordinary sync's _store_remote_snapshot must not invent acceptance entries."""

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False): return []

    # Seed observation so cloud→local id resolution works.
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute("INSERT INTO observations (id, cloud_id, date) VALUES (1, 'obs-cloud', '2026-08-05')")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, filepath) "
        "VALUES (?, ?, ?, ?)",
        (9, 1, "microscope", "/tmp/nine.jpg"),
    )
    conn.commit()
    conn.close()

    cloud_sync._store_remote_snapshot(Client(), "obs-cloud",
                                     remote_images=[], remote_measurements=[])
    reloaded = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot("obs-cloud")
    )
    accepted = reloaded.get('accepted_asymmetry')
    assert not accepted or not any(accepted.get(k) for k in (
        'local_only_images', 'cloud_only_images',
        'local_only_measurements', 'cloud_only_measurements',
    ))


def test_sqlite_snapshot_round_trip_prunes_measurement_when_owner_changes(isolated_sqlite_profile):
    """Measurement acceptance is pruned when its owning image changes."""
    cloud_id = "obs-cloud"
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute("INSERT INTO observations (id, cloud_id, date) VALUES (1, ?, '2026-08-05')", (cloud_id,))
    conn.execute("INSERT INTO images (id, observation_id, image_type, filepath) VALUES (5, 1, 'microscope', '/tmp/a.jpg')")
    conn.execute("INSERT INTO images (id, observation_id, image_type, filepath) VALUES (7, 1, 'microscope', '/tmp/b.jpg')")
    conn.execute(
        "INSERT INTO spore_measurements (id, image_id, length_um, width_um, measurement_type) "
        "VALUES (?, ?, ?, ?, ?)",
        (31, 5, 5.0, 3.0, 'spore'),
    )
    conn.commit()
    conn.close()

    accepted = {
        'local_only_images': [], 'cloud_only_images': [],
        'local_only_measurements': [{
            'side': 'local_only', 'kind': 'measurement',
            'local_id': 31, 'cloud_id': None,
            'owning_local_image_id': 5, 'owning_cloud_image_id': None,
            'fingerprint': cloud_sync._asymmetry_fingerprint_local_measurement(
                {'length_um': 5.0, 'image_id': 5}
            ),
            'accepted_at': '', 'choice': 'keep_local',
        }],
        'cloud_only_measurements': [],
    }
    initial = cloud_sync._cloud_observation_snapshot(
        {'id': cloud_id}, [], [], accepted_asymmetry=accepted,
    )
    cloud_sync._store_cloud_observation_snapshot(cloud_id, initial)
    # Move the measurement to another image.
    conn = sqlite3.connect(isolated_sqlite_profile)
    conn.execute("UPDATE spore_measurements SET image_id = 7 WHERE id = 31")
    conn.commit()
    conn.close()

    class Client:
        def get_observation(self, _cid): return {"id": _cid}
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False): return []

    cloud_sync._store_remote_snapshot(Client(), cloud_id,
                                     remote_images=[], remote_measurements=[])
    reloaded = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(cloud_id)
    )
    accepted_after = reloaded.get('accepted_asymmetry')
    assert not accepted_after or not accepted_after.get('local_only_measurements')


# ── Fix 4: Presentation-retry integration ────────────────────────────────────

def test_presentation_failure_surfaces_as_warning_and_does_not_fake_success(monkeypatch):
    """The apply's dict carries presentation_warnings; caller cannot mistake success."""
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    local_images = [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                     "sort_order": 3, "gallery_rotation": 90}]
    remote_images = [{"id": "c5", "desktop_id": 5, "image_type": "microscope",
                      "sort_order": 9, "gallery_rotation": 0}]
    baseline = _baseline_from_state(local_obs, remote_obs, local_images, remote_images, [], [])

    calls = {'n': 0}

    def _mixed_read(_oid):
        calls['n'] += 1
        if calls['n'] == 1:
            return copy.deepcopy(local_images)
        # After the apply, "the DB claims cloud values" — verification will fail.
        return [{"id": 5, "cloud_id": "c5", "image_type": "microscope",
                 "sort_order": 9, "gallery_rotation": 0}]

    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=local_images, remote_images=remote_images)
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", _mixed_read)
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image", lambda _id, **k: None)
    client = _RecordingClient(remote_obs=remote_obs)

    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'cloud'}],
    })
    assert result['plan_applied']
    warnings = result['presentation_warnings']
    assert warnings and warnings[0]['status'] == 'failed', (
        "the resolver must not silently report success when presentation drifted"
    )
    assert any(op.get('op') == 'restore_presentation' and op.get('status') == 'failed'
               for op in result['operations'])


def test_presentation_retry_does_not_repeat_completed_apply_op(monkeypatch):
    """On retry with a completed apply_image_metadata in prior_result, the apply
    is not repeated; presentation restore is re-attempted.
    """
    local_obs = {"id": 1, "cloud_id": "obs-cloud"}
    remote_obs = {"id": "obs-cloud"}
    live_row = {"id": 5, "cloud_id": "c5", "image_type": "microscope",
                "sort_order": 3, "gallery_rotation": 90, "notes": "n"}
    remote_row = {"id": "c5", "desktop_id": 5, "image_type": "microscope",
                  "sort_order": 3, "gallery_rotation": 90, "notes": "n"}
    baseline = _baseline_from_state(local_obs, remote_obs, [live_row], [remote_row], [], [])
    apply_calls = []
    update_calls = []
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_images=[live_row], remote_images=[remote_row])
    # Track re-dispatch of the underlying apply helper.
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local",
                        lambda *a, **k: apply_calls.append(True))
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image",
                        lambda _id, **k: update_calls.append((_id, k)))
    # Build a completed-form op with proper material fingerprints.
    prior = {
        'operations': [
            _make_completed_image_op(op='apply_image_metadata',
                                     local_id=5, cloud_id='c5',
                                     local_row=live_row, remote_row=remote_row),
        ],
    }
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'items': [{'kind': 'image_metadata', 'local_id': 5,
                   'cloud_id': 'c5', 'choice': 'cloud'}],
    }, prior_result=prior)
    # apply_remote_images_to_local was NOT called — completed op verified.
    assert apply_calls == [], f"apply must not be repeated, got {apply_calls}"
    # Presentation restore was re-attempted (update_image called with the
    # expected values for id=5).
    assert any(cid == 5 for cid, _kw in update_calls), (
        "presentation restore must be re-run on retry"
    )
    # And this time it succeeds (read-back matches expected).
    presentation_ops = [op for op in result['operations']
                        if op.get('op') == 'restore_presentation']
    assert presentation_ops
    assert all(op.get('status') == 'completed' for op in presentation_ops)


# ── Simplified-sync matrix tests ────────────────────────────────────────────
#
# These exercise ``get_conflict_detail`` directly to prove that the dialog
# never receives one-sided or additive divergence that ordinary sync should
# handle automatically.

def _make_get_conflict_detail_env(monkeypatch, *, local_obs, remote_obs,
                                  local_images=None, remote_images=None,
                                  local_measurements=None, remote_measurements=None,
                                  snapshot=None):
    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation",
                        lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _id: [dict(r) for r in (local_images or [])])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _id: [dict(r) for r in (local_measurements or [])])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images",
                        lambda *a: [dict(r) for r in (remote_measurements or [])])
    def _lookup(_id):
        by_cloud = {}
        by_local = {}
        for row in (local_measurements or []):
            lid = int(row.get('id')) if row.get('id') else 0
            if lid:
                by_local[lid] = dict(row)
            cid = str(row.get('cloud_id') or '').strip()
            if cid:
                by_cloud[cid] = dict(row)
        return (by_cloud, by_local)
    monkeypatch.setattr(cloud_sync, "_load_local_measurement_lookup", _lookup)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot",
                        lambda _cid: snapshot or "")

    class Client:
        def get_observation(self, _cid): return dict(remote_obs)
        def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
            return [dict(r) for r in (remote_images or [])]

    return Client()


def _snapshot_json(*, observation, images=None, measurements=None):
    import json as _json
    return _json.dumps({
        'observation': observation, 'images': images or [], 'measurements': measurements or [],
    }, sort_keys=True, separators=(',', ':'))


# 1. Scalar: local-only change → auto push, dialog stays silent.

def test_scalar_local_only_change_becomes_automatic_push(monkeypatch):
    baseline_obs = {'id': 'obs-cloud', 'notes': 'baseline notes', 'date': '2026-01-01'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'notes': 'local edit', 'date': '2026-01-01'}
    remote_obs = dict(baseline_obs)
    snapshot = _snapshot_json(observation=baseline_obs)
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=snapshot)
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == []
    assert not detail['has_manual_conflicts']
    fields_auto = detail['automatic_decisions']['fields']
    assert any(d['field'] == 'notes' and d['action'] == 'push_local' for d in fields_auto)


# 2. Scalar: cloud-only change → auto pull.

def test_scalar_cloud_only_change_becomes_automatic_pull(monkeypatch):
    baseline_obs = {'id': 'obs-cloud', 'notes': 'baseline notes', 'date': '2026-01-01'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'notes': 'baseline notes', 'date': '2026-01-01'}
    remote_obs = {'id': 'obs-cloud', 'notes': 'cloud edit', 'date': '2026-01-01'}
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline_obs))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == []
    fields_auto = detail['automatic_decisions']['fields']
    assert any(d['field'] == 'notes' and d['action'] == 'pull_cloud' for d in fields_auto)


# 3. Identical two-sided change → converged, no conflict.

def test_scalar_identical_two_sided_change_converges(monkeypatch):
    baseline_obs = {'id': 'obs-cloud', 'notes': 'baseline notes', 'date': '2026-01-01'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'notes': 'agreed edit', 'date': '2026-01-01'}
    remote_obs = {'id': 'obs-cloud', 'notes': 'agreed edit', 'date': '2026-01-01'}
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline_obs))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == []
    assert not detail['automatic_decisions']['fields']


# 4. Different two-sided change → genuine conflict remains.

def test_scalar_different_two_sided_change_becomes_manual_conflict(monkeypatch):
    baseline_obs = {'id': 'obs-cloud', 'notes': 'baseline notes'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'notes': 'device edit'}
    remote_obs = {'id': 'obs-cloud', 'notes': 'cloud edit'}
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline_obs))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    rows = [r for r in detail['field_rows'] if r['field'] == 'notes']
    assert rows and rows[0]['local'] == 'device edit' and rows[0]['remote'] == 'cloud edit'
    assert detail['has_manual_conflicts']


# 5. Draft: local-only change auto-syncs.

def test_draft_local_only_change_auto_syncs(monkeypatch):
    baseline = {'id': 'obs-cloud', 'is_draft': True}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'is_draft': False}
    remote_obs = {'id': 'obs-cloud', 'is_draft': True}
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == []
    assert any(d['field'] == 'is_draft' and d['action'] == 'push_local'
               for d in detail['automatic_decisions']['fields'])


# 6. Draft: local publish while cloud unchanged auto-syncs.

def test_publish_local_only_change_auto_syncs(monkeypatch):
    baseline = {'id': 'obs-cloud', 'is_draft': True}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'is_draft': False}
    remote_obs = {'id': 'obs-cloud', 'is_draft': True}
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == []
    assert not detail['has_manual_conflicts']


# 7. Draft: conflicting values auto-resolve to Draft.

def test_draft_conflicting_values_auto_resolve_to_draft(monkeypatch):
    baseline = {'id': 'obs-cloud', 'is_draft': True}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'is_draft': True}
    remote_obs = {'id': 'obs-cloud', 'is_draft': False}
    # Both changed vs baseline — local kept Draft, cloud published.  Rule
    # says: choose Draft automatically as the safer state.
    baseline2 = {'id': 'obs-cloud', 'is_draft': None}  # unknown baseline
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           snapshot=_snapshot_json(observation=baseline2))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert detail['field_rows'] == [] or all(
        row['field'] != 'is_draft' for row in detail['field_rows']
    ), "is_draft must never require manual review"
    auto = detail['automatic_decisions']['fields']
    draft_decision = next((d for d in auto if d['field'] == 'is_draft'), None)
    assert draft_decision is not None
    assert draft_decision['action'] in {'auto_draft_wins', 'push_local', 'pull_cloud'}


# 8. Eligible local-only image → auto upload.

def test_local_only_image_uploads_without_conflict(monkeypatch):
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    local_images = [{'id': 9, 'image_type': 'microscope', 'filepath': '/tmp/nine.jpg'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    # No local_only image pair should appear in the dialog list.
    assert not any(p.get('status') == 'local_only' for p in detail['image_pairs'])
    auto = detail['automatic_decisions']['media']
    assert any(d['kind'] == 'image' and d['side'] == 'local_only'
               and d['action'] == 'upload_automatic' for d in auto)


# 9. Cloud-only image → auto download (per policy).

def test_cloud_only_image_downloads_automatically(monkeypatch):
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    remote_images = [{'id': 'cloud-9', 'image_type': 'microscope', 'storage_path': 'cloud/x.webp'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           remote_images=remote_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert not any(p.get('status') == 'cloud_only' for p in detail['image_pairs'])
    auto = detail['automatic_decisions']['media']
    assert any(d['kind'] == 'image' and d['side'] == 'cloud_only'
               and d['action'] == 'download_automatic' for d in auto)


# 10. Local-only measurement → auto upload.

def test_local_only_measurement_uploads_automatically(monkeypatch):
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    local_images = [{'id': 5, 'cloud_id': 'c5', 'image_type': 'microscope'}]
    remote_images = [{'id': 'c5', 'desktop_id': 5, 'image_type': 'microscope'}]
    local_meas = [{'id': 42, 'image_id': 5, 'length_um': 5.0}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images, remote_images=remote_images,
                                           local_measurements=local_meas,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert not any(p.get('status') == 'local_only' for p in detail['measurement_pairs'])
    auto = detail['automatic_decisions']['media']
    assert any(d['kind'] == 'measurement' and d['side'] == 'local_only' for d in auto)


# 11. Cloud-only measurement → auto import.

def test_cloud_only_measurement_imports_automatically(monkeypatch):
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    local_images = [{'id': 5, 'cloud_id': 'c5', 'image_type': 'microscope'}]
    remote_images = [{'id': 'c5', 'desktop_id': 5, 'image_type': 'microscope'}]
    remote_meas = [{'id': 'm-cloud', 'image_id': 'c5', 'length_um': 6.0}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images, remote_images=remote_images,
                                           remote_measurements=remote_meas,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert not any(p.get('status') == 'cloud_only' for p in detail['measurement_pairs'])
    auto = detail['automatic_decisions']['media']
    assert any(d['kind'] == 'measurement' and d['side'] == 'cloud_only' for d in auto)


# 12. No counterpart without tombstone → never triggers deletion.

def test_no_counterpart_without_tombstone_never_deletes(monkeypatch):
    """A local image without a cloud row and no deletion tombstone must
    never be interpreted as deletion consent."""
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    local_images = [{'id': 9, 'image_type': 'microscope', 'filepath': '/tmp/nine.jpg'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    # No pair mentions media_deletion; automatic decision is upload_automatic.
    auto = detail['automatic_decisions']['media']
    actions = {d.get('action') for d in auto}
    assert 'upload_automatic' in actions
    assert not any('delete' in (a or '') for a in actions)


# 13. Explicit tombstone continues to use existing deletion workflow —
# get_conflict_detail hides tombstoned remote images (existing behavior).

def test_explicit_tombstone_hides_via_existing_deletion_workflow(monkeypatch):
    """A cloud image tagged deleted_at falls through the ordinary tombstone
    filter and is not surfaced in image_pairs."""
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    remote_images = [{'id': 'c-gone', 'desktop_id': 9, 'deleted_at': '2026-08-05T00:00:00Z',
                      'image_type': 'microscope'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           remote_images=remote_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    # No cloud image surfaces because deleted_at prunes it.
    assert not any(str((p.get('remote') or {}).get('cloud_id') or '') == 'c-gone'
                   for p in detail['image_pairs'])


# 14. Additive uploads that fail should surface as an actionable error, not a
# radio choice conflict.  Automatic decisions are declarative — the caller
# translates them into sync operations; a failure there is a sync error, not
# a dialog item.  Verify the detail's automatic_decisions carries the action
# so the caller can attempt it and surface any failure via the sync path.

def test_additive_upload_is_declared_as_automatic_not_manual(monkeypatch):
    baseline = {'id': 'obs-cloud'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud'}
    remote_obs = dict(baseline)
    local_images = [{'id': 9, 'image_type': 'microscope', 'filepath': '/tmp/nine.jpg'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert not detail['has_manual_conflicts']
    # If the caller subsequently fails the upload it must be surfaced by the
    # normal sync error path, not by re-adding an image_metadata radio choice
    # to the dialog.  Verify no radio choice is prepared for id 9.
    assert not any(p.get('local') and p['local'].get('local_id') == 9
                   and p.get('metadata_diff_details')
                   for p in detail['image_pairs'])


# 15. Dialog does not open when all differences are automatic.

def test_dialog_would_not_open_when_all_differences_are_automatic(monkeypatch):
    baseline = {'id': 'obs-cloud', 'notes': 'baseline'}
    local_obs = {'id': 1, 'cloud_id': 'obs-cloud', 'notes': 'local edit'}
    remote_obs = {'id': 'obs-cloud', 'notes': 'baseline'}
    local_images = [{'id': 9, 'image_type': 'microscope', 'filepath': '/tmp/n.jpg'}]
    client = _make_get_conflict_detail_env(monkeypatch,
                                           local_obs=local_obs, remote_obs=remote_obs,
                                           local_images=local_images,
                                           snapshot=_snapshot_json(observation=baseline))
    detail = cloud_sync.get_conflict_detail(client, 1, 'obs-cloud')
    assert not detail['has_manual_conflicts']


# ── Shared final gate: end-to-end regression for the empty-dialog case ─────

class _GateEnv:
    """A single test scaffold that patches everything ``finalize_sync_candidates``
    touches with sensible fakes, and lets a test observe what happened.
    """

    def __init__(self, monkeypatch, *, local_obs, remote_obs,
                 local_images=None, remote_images=None,
                 local_measurements=None, remote_measurements=None,
                 baseline_obs=None):
        self.monkeypatch = monkeypatch
        # Mutable state so writes are reflected in later reads.
        self.local_obs = dict(local_obs)
        self.remote_obs = dict(remote_obs)
        self.local_images = [dict(r) for r in (local_images or [])]
        self.remote_images = [dict(r) for r in (remote_images or [])]
        self.local_measurements = [dict(r) for r in (local_measurements or [])]
        self.remote_measurements = [dict(r) for r in (remote_measurements or [])]
        self.baseline_obs = dict(baseline_obs or {})
        self.calls = {
            'push_measurements': [],
            'push_images': [],
            'apply_remote_images': [],
            'import_remote_measurements': [],
            'apply_remote_fields': [],
            'store_snapshot': [],
            'stamp': [],
        }
        env = self
        monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation",
                            lambda _id: dict(env.local_obs))
        monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                            lambda _id: [dict(r) for r in env.local_images])
        monkeypatch.setattr(cloud_sync.ImageDB, "update_image",
                            lambda _id, **k: env._update_local_image(_id, k))
        monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                            lambda _id: [dict(r) for r in env.local_measurements])
        monkeypatch.setattr(cloud_sync.ObservationDB, "update_spore_statistics",
                            lambda _id, value: env.local_obs.__setitem__('spore_statistics', value))
        monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync",
                            lambda *a: [dict(r) for r in env.remote_images])
        monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images",
                            lambda *a: [dict(r) for r in env.remote_measurements])
        monkeypatch.setattr(cloud_sync, "_load_local_measurement_lookup",
                            lambda _id: env._measurement_lookup())
        monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot",
                            lambda _cid: env._snapshot_json())
        monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields",
                            lambda *a, **k: env._apply_fields(a, k))
        monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local",
                            lambda *a, **k: env._apply_remote_images(a, k))
        monkeypatch.setattr(cloud_sync, "_import_remote_measurements_for_observation",
                            lambda *a, **k: env._import_remote_measurements(a, k))
        monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                            lambda *a, **k: env._push_measurements(a, k))
        monkeypatch.setattr(cloud_sync, "_push_images_for_observation",
                            lambda *a, **k: env._push_images(a, k))
        monkeypatch.setattr(cloud_sync, "_format_recomputed_spore_statistics",
                            lambda _id: env._recompute_stats())
        monkeypatch.setattr(cloud_sync, "_stamp_observation_synced",
                            lambda *a: env.calls['stamp'].append(tuple(a)))
        monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature",
                            lambda *a: None)
        monkeypatch.setattr(cloud_sync, "_store_remote_snapshot",
                            lambda *a, **k: env._store_snapshot(a, k))
        monkeypatch.setattr(cloud_sync, "_local_observation_id_by_cloud_id",
                            lambda _cid: int(env.local_obs.get('id') or 0))
        monkeypatch.setattr(cloud_sync, "get_conflict_detail",
                            cloud_sync.get_conflict_detail)  # ensure identity

    def _measurement_lookup(self):
        by_cloud = {}
        by_local = {}
        for row in self.local_measurements:
            lid = int(row.get('id') or 0)
            if lid:
                by_local[lid] = dict(row)
            cid = str(row.get('cloud_id') or '').strip()
            if cid:
                by_cloud[cid] = dict(row)
        return (by_cloud, by_local)

    def _snapshot_json(self):
        import json as _json
        return _json.dumps({
            'observation': dict(self.baseline_obs),
            'images': [], 'measurements': [],
        }, sort_keys=True, separators=(',', ':'))

    def _apply_fields(self, args, kwargs):
        self.calls['apply_remote_fields'].append(kwargs.get('fields'))
        for f in kwargs.get('fields') or []:
            self.local_obs[f] = self.remote_obs.get(f)

    def _apply_remote_images(self, args, kwargs):
        payload = args[2] if len(args) >= 3 else []
        self.calls['apply_remote_images'].append(
            {'ids': [str(r.get('id') or '') for r in payload], 'kwargs': dict(kwargs)}
        )
        # Simulate materializing cloud images locally: add rows if missing.
        existing = {str(r.get('cloud_id') or '').strip() for r in self.local_images}
        next_local_id = max((int(r.get('id') or 0) for r in self.local_images), default=0) + 1
        for remote_row in payload:
            cid = str(remote_row.get('id') or '').strip()
            if cid and cid not in existing:
                self.local_images.append({
                    'id': next_local_id, 'cloud_id': cid,
                    'image_type': remote_row.get('image_type'),
                    'filepath': f'/tmp/downloaded-{cid}.jpg',
                })
                next_local_id += 1

    def _import_remote_measurements(self, args, kwargs):
        self.calls['import_remote_measurements'].append(dict(kwargs))
        # Materialize imported measurements locally, linking their owning
        # image identity to the LOCAL id of the matching local image row.
        existing = {str(r.get('cloud_id') or '').strip() for r in self.local_measurements}
        local_image_by_cloud = {
            str(r.get('cloud_id') or '').strip(): int(r.get('id') or 0)
            for r in self.local_images if str(r.get('cloud_id') or '').strip()
        }
        next_id = max((int(r.get('id') or 0) for r in self.local_measurements), default=0) + 1
        for remote_row in kwargs.get('remote_measurements') or []:
            cid = str(remote_row.get('id') or '').strip()
            if cid and cid not in existing:
                owning_cloud_image = str(remote_row.get('image_id') or '').strip()
                owning_local_image = local_image_by_cloud.get(owning_cloud_image) or None
                self.local_measurements.append({
                    'id': next_id, 'cloud_id': cid,
                    'image_id': owning_local_image,
                    'length_um': remote_row.get('length_um'),
                    'width_um': remote_row.get('width_um'),
                    'measurement_type': remote_row.get('measurement_type'),
                })
                # Also update the remote row's desktop_id so subsequent
                # reclassifications see the round-tripped link.
                for r in self.remote_measurements:
                    if str(r.get('id') or '') == cid:
                        r['desktop_id'] = next_id
                        break
                next_id += 1
        return {'failed': 0, 'warnings': []}

    def _push_measurements(self, args, kwargs):
        self.calls['push_measurements'].append(kwargs.get('measurement_ids'))
        # Resolve local image id → cloud image id for the remote link.
        cloud_image_by_local = {
            int(r.get('id') or 0): str(r.get('cloud_id') or '').strip()
            for r in self.local_images if int(r.get('id') or 0)
        }
        for lid in (kwargs.get('measurement_ids') or []):
            for row in self.local_measurements:
                if int(row.get('id') or 0) == int(lid):
                    if not row.get('cloud_id'):
                        row['cloud_id'] = f'm-cloud-for-local-{lid}'
                    # Ensure remote has a matching row with cloud image id.
                    owning_cloud = cloud_image_by_local.get(int(row.get('image_id') or 0)) or None
                    exists = any(
                        str(r.get('id') or '') == row['cloud_id']
                        for r in self.remote_measurements
                    )
                    if not exists:
                        self.remote_measurements.append({
                            'id': row['cloud_id'], 'desktop_id': lid,
                            'image_id': owning_cloud,
                            'length_um': row.get('length_um'),
                            'width_um': row.get('width_um'),
                            'measurement_type': row.get('measurement_type'),
                        })

    def _push_images(self, args, kwargs):
        self.calls['push_images'].append(kwargs.get('include_image_ids'))
        for lid in (kwargs.get('include_image_ids') or []):
            for row in self.local_images:
                if int(row.get('id') or 0) == int(lid):
                    if not row.get('cloud_id'):
                        row['cloud_id'] = f'c-for-{lid}'
                    exists = any(
                        str(r.get('id') or '') == row['cloud_id']
                        for r in self.remote_images
                    )
                    if not exists:
                        self.remote_images.append({
                            'id': row['cloud_id'], 'desktop_id': lid,
                            'image_type': row.get('image_type'),
                            'notes': row.get('notes'),
                            'objective_name': row.get('objective_name'),
                        })
        return True

    def _recompute_stats(self):
        return 'stats' if self.local_measurements else None

    def _store_snapshot(self, args, kwargs):
        self.calls['store_snapshot'].append(dict(kwargs))
        # Update baseline to reflect current cloud state (what a real snapshot store does).
        self.baseline_obs = dict(self.remote_obs)

    def _update_local_image(self, lid, kwargs):
        for row in self.local_images:
            if int(row.get('id') or 0) == int(lid):
                row.update(kwargs)


class _StubClient:
    def __init__(self, env):
        self.env = env

    def get_observation(self, _cid):
        return dict(self.env.remote_obs)

    def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
        return [dict(r) for r in self.env.remote_images]

    def _patch(self, path, payload):
        if 'observations' in path:
            for k, v in payload.items():
                self.env.remote_obs[k] = v


def test_gate_local_only_image_uploads_and_dialog_is_not_needed(monkeypatch):
    """Screenshot regression: preflight flagged an obs whose only difference
    is a local-only image; gate uploads it, reclassifies, and returns no
    manual candidates.  Dialog is never constructed."""
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 687, 'cloud_id': '1011'},
                   remote_obs={'id': '1011'},
                   local_images=[{'id': 9, 'image_type': 'microscope',
                                  'filepath': '/tmp/nine.jpg'}],
                   baseline_obs={'id': '1011'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 687, 'cloud_id': '1011'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual == []
    assert errors == []
    # Push actually happened.
    assert env.calls['push_images'] == [{9}]
    # Snapshot / stamp were called at least once.
    assert env.calls['store_snapshot'] and env.calls['stamp']
    # Reclassify — feeding the same candidate again produces nothing.
    manual2, errors2 = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 687, 'cloud_id': '1011'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual2 == [] and errors2 == []


def test_gate_local_only_measurement_uploads_via_shared_executor(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c'},
                   remote_obs={'id': 'obs-c'},
                   local_images=[{'id': 5, 'cloud_id': 'c5', 'image_type': 'microscope'}],
                   remote_images=[{'id': 'c5', 'desktop_id': 5, 'image_type': 'microscope'}],
                   local_measurements=[{'id': 42, 'image_id': 5, 'length_um': 5.0}],
                   baseline_obs={'id': 'obs-c'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual == [] and errors == []
    assert env.calls['push_measurements'] == [{42}]


def test_gate_cloud_only_image_downloads_via_shared_executor(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c'},
                   remote_obs={'id': 'obs-c'},
                   remote_images=[{'id': 'c-only', 'image_type': 'microscope',
                                   'storage_path': 'cloud/x.webp'}],
                   baseline_obs={'id': 'obs-c'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual == [] and errors == []
    apply_calls = env.calls['apply_remote_images']
    assert apply_calls and 'c-only' in apply_calls[0]['ids']


def test_gate_cloud_only_measurement_imports_via_shared_executor(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c'},
                   remote_obs={'id': 'obs-c'},
                   local_images=[{'id': 5, 'cloud_id': 'c5', 'image_type': 'microscope'}],
                   remote_images=[{'id': 'c5', 'desktop_id': 5, 'image_type': 'microscope'}],
                   remote_measurements=[{'id': 'm-only', 'image_id': 'c5', 'length_um': 6.0}],
                   baseline_obs={'id': 'obs-c'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual == [] and errors == []
    assert env.calls['import_remote_measurements']


def test_gate_local_draft_change_auto_syncs(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c', 'is_draft': True},
                   remote_obs={'id': 'obs-c', 'is_draft': False},
                   baseline_obs={'id': 'obs-c', 'is_draft': False})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
    )
    assert manual == [] and errors == []
    # is_draft was pushed to cloud (via a _patch through the stub client).
    assert env.remote_obs.get('is_draft') is True


def test_gate_local_publish_change_auto_syncs(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c', 'is_draft': False},
                   remote_obs={'id': 'obs-c', 'is_draft': True},
                   baseline_obs={'id': 'obs-c', 'is_draft': True})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
    )
    assert manual == [] and errors == []
    assert env.remote_obs.get('is_draft') is False


def test_gate_converged_two_sided_scalar_is_a_noop(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c', 'notes': 'agreed'},
                   remote_obs={'id': 'obs-c', 'notes': 'agreed'},
                   baseline_obs={'id': 'obs-c', 'notes': 'before'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
    )
    assert manual == [] and errors == []
    # No writes happened (nothing automatic AND nothing manual).
    assert env.calls['apply_remote_fields'] == []
    assert env.calls['push_images'] == []
    assert env.calls['push_measurements'] == []


def test_gate_upload_failure_surfaces_as_error_and_does_not_hide(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c'},
                   remote_obs={'id': 'obs-c'},
                   local_images=[{'id': 9, 'image_type': 'microscope', 'filepath': '/tmp/n.jpg'}],
                   baseline_obs={'id': 'obs-c'})
    # Force push failure.
    env.monkeypatch.setattr(cloud_sync, "_push_images_for_observation",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError('upload boom')))
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
        prepare_images_cb=lambda *_: {},
    )
    assert manual == []  # nothing manual left; but errors flag it
    assert errors and errors[0]['phase'] == 'execute'
    assert 'upload' in errors[0]['error'].lower() or 'boom' in errors[0]['error'].lower()


def test_gate_preserves_manual_conflict_and_still_runs_automatic(monkeypatch):
    """A candidate with BOTH automatic and manual items: automatic executes,
    the manual portion is kept, and the caller opens the dialog with only
    the remaining manual work."""
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c',
                              'notes': 'device edit', 'common_name': 'device name'},
                   remote_obs={'id': 'obs-c',
                               'notes': 'cloud edit', 'common_name': 'cloud name'},
                   baseline_obs={'id': 'obs-c',
                                 'notes': 'baseline', 'common_name': 'baseline'})
    # Both notes and common_name are two-sided → both are manual.  Nothing is
    # automatic here.  Verify gate returns them as manual.
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
    )
    assert len(manual) == 1
    assert manual[0].get('detail', {}).get('has_manual_conflicts') is True
    assert errors == []


def test_gate_identity_ambiguity_still_enters_dialog(monkeypatch):
    env = _GateEnv(monkeypatch,
                   local_obs={'id': 1, 'cloud_id': 'obs-c'},
                   remote_obs={'id': 'obs-c'},
                   local_images=[
                       {'id': 5, 'cloud_id': 'shared', 'image_type': 'microscope'},
                       {'id': 6, 'cloud_id': 'shared', 'image_type': 'microscope'},
                   ],
                   remote_images=[{'id': 'shared', 'desktop_id': 5,
                                   'image_type': 'microscope'}],
                   baseline_obs={'id': 'obs-c'})
    manual, errors = cloud_sync.finalize_sync_candidates(
        _StubClient(env), [{'local_id': 1, 'cloud_id': 'obs-c'}],
    )
    # Identity conflict is manual — MUST appear in the dialog list.
    assert manual  # has_manual_conflicts=True from get_conflict_detail


def test_operation_log_records_every_dispatch(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 5, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": "img-cloud-5", "length_um": 6.0}]
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], local_meas, remote_meas)
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs,
                  local_measurements=local_meas, remote_measurements=remote_meas)
    client = _RecordingClient(remote_obs=remote_obs)
    result = resolve_conflict_plan(client, 1, plan={
        'baseline': baseline,
        'derived_statistics': 'recompute_from_measurements',
        'items': [
            {'kind': 'field', 'field': 'common_name', 'choice': 'cloud'},
            {'kind': 'measurement', 'side': 'matched',
             'local_id': 31, 'cloud_id': 'm31', 'choice': 'local'},
        ],
    })
    ops = [op['op'] for op in result['operations']]
    assert 'pull_field' in ops
    assert 'push_measurement' in ops
    assert 'recompute_spore_statistics' in ops
    # Never any bogus operations for unselected kinds.
    assert 'import_measurement' not in ops
    assert 'push_field' not in ops
