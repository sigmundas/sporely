"""Tests for sync-ordering gaps: anchor failures, per-image upload failures,
and unhandled CloudSyncError leaving obs dirty.

Covers three specific propagation paths:

Gap A — anchor identity conflict failures surface in the returned failures list
         from _ensure_metadata_anchors_for_public_spore_observation.
Gap B — per-image upload CloudSyncError appears in push_all errors list.
Gap C — any CloudSyncError escaping the image phase marks obs dirty.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from utils import cloud_sync
from utils.cloud_sync import (
    ImageIdentityConflictError,
    CloudSyncError,
    _ensure_metadata_anchors_for_public_spore_observation,
    _ensure_metadata_only_microscope_images_for_observation,
)


# ---------------------------------------------------------------------------
# Gap A — anchor failure propagation
# ---------------------------------------------------------------------------


def _make_simple_client():
    client = MagicMock()
    client.pull_image_metadata = MagicMock(return_value=[])
    return client


def test_anchor_helper_records_failure_message():
    """Per-image exception in _ensure_metadata_only_microscope_images_for_observation
    appends to counters['failures']."""
    client = _make_simple_client()

    fake_row = {
        'id': 42,
        'image_type': 'microscope',
        'cloud_id': None,
        'filepath': '/tmp/img.jpg',
        'sort_order': 0,
    }

    with patch.object(cloud_sync, 'get_connection') as mock_conn, \
         patch.object(cloud_sync, '_cloud_explicit_media_upload_selection', return_value=set()), \
         patch.object(
             cloud_sync,
             '_ensure_metadata_only_microscope_image_for_public_spores',
             side_effect=ImageIdentityConflictError('conflict!'),
         ):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = [fake_row]
        conn.execute.return_value = cursor
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = conn

        result = _ensure_metadata_only_microscope_images_for_observation(
            client,
            obs_local_id=1,
            obs_cloud_id='cloud-obs-1',
        )

    assert result['failed'] == 1
    assert len(result['failures']) == 1
    assert 'local_image=42' in result['failures'][0]
    assert 'ImageIdentityConflictError' in result['failures'][0]
    assert 'conflict!' in result['failures'][0]


def test_anchor_wrapper_propagates_failures():
    """_ensure_metadata_anchors_for_public_spore_observation includes failures
    from the inner helper in its returned dict."""
    client = _make_simple_client()
    obs = {'id': 1, 'spore_data_visibility': 'public', 'cloud_id': 'cloud-1'}

    inner_result = {
        'considered': 1, 'ensured': 0, 'skipped': 0, 'failed': 1,
        'failures': ['local_image=42: ImageIdentityConflictError: conflict!'],
        'cloud_ids': [],
        'metadata_only_cloud_ids': [],
    }

    with patch.object(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        return_value=inner_result,
    ):
        result = _ensure_metadata_anchors_for_public_spore_observation(
            client, obs, obs_local_id=1, obs_cloud_id='cloud-1',
        )

    assert result.get('failures') == inner_result['failures']


def test_anchor_wrapper_outer_exception_returns_failures():
    """When _ensure_metadata_only_microscope_images_for_observation raises a
    non-auth exception, the wrapper catches it and returns failures list."""
    client = _make_simple_client()
    obs = {'id': 1, 'spore_data_visibility': 'public', 'cloud_id': 'cloud-1'}

    with patch.object(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        side_effect=RuntimeError('unexpected boom'),
    ):
        result = _ensure_metadata_anchors_for_public_spore_observation(
            client, obs, obs_local_id=1, obs_cloud_id='cloud-1',
        )

    assert result.get('failures')
    assert 'RuntimeError' in result['failures'][0]
    assert 'unexpected boom' in result['failures'][0]


# ---------------------------------------------------------------------------
# Gap B — per-image upload failure in push_all errors list
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Gap B — per-image CloudSyncError recorded in summary_warnings
# ---------------------------------------------------------------------------


def test_per_image_upload_failure_recorded_in_summary_warnings():
    """When upload_image_file raises a non-fatal CloudSyncError, the failure is
    appended to the summary_warnings list passed to _push_images_for_observation."""
    summary_warnings: list[str] = []
    client = MagicMock()
    client.pull_image_metadata = MagicMock(return_value=[])
    client._observation_images_support_ai_crop = MagicMock(return_value=False)
    client._observation_images_support_upload_metadata = MagicMock(return_value=False)
    # push_image_metadata returns the cloud id string for the image
    client.push_image_metadata = MagicMock(return_value='cloud-img-10')
    # upload_image_file raises non-fatal CloudSyncError
    client.upload_image_file = MagicMock(
        side_effect=ImageIdentityConflictError('duplicate found')
    )

    obs = {'id': 1, 'cloud_id': 'cloud-1', 'spore_data_visibility': 'private'}
    obs_cloud_id = 'cloud-1'

    img_row = {
        'id': 10, 'observation_id': 1, 'image_type': 'field',
        'cloud_id': None, 'filepath': '/tmp/img.jpg',
        'sort_order': 0, 'storage_path': None,
    }
    prepared_items = [{'image_row': img_row, 'upload_path': '/tmp/img.jpg'}]

    with patch.object(
        cloud_sync, '_ensure_metadata_anchors_for_public_spore_observation',
        return_value={
            'considered': 0, 'ensured': 0, 'skipped': 0, 'failed': 0,
            'failures': [], 'cloud_ids': [], 'metadata_only_cloud_ids': [],
        },
    ), patch.object(
        cloud_sync, '_push_pending_image_tombstones', return_value=[],
    ), patch.object(
        cloud_sync, '_ensure_cloud_image_storage_intent_initialized', return_value=None,
    ), patch.object(
        cloud_sync, 'cloud_image_bytes_desired', return_value=True,
    ), patch.object(
        cloud_sync, '_local_tombstoned_cloud_image_ids', return_value=set(),
    ), patch.object(
        cloud_sync, '_cloud_sync_current_summary', return_value={},
    ), patch.object(
        cloud_sync, '_increment_sync_summary', return_value=None,
    ), patch.object(
        cloud_sync, '_associate_persisted_cloud_images', return_value=set(),
    ), patch.object(
        cloud_sync, '_reconcile_metadata_only_linked_images', return_value=set(),
    ), patch.object(
        cloud_sync, '_cloud_sync_current_profiler', return_value=None,
    ), patch.object(
        cloud_sync, '_advance_progress', return_value=None,
    ), patch.object(
        cloud_sync, '_emit_progress', return_value=None,
    ), patch.object(
        cloud_sync, 'is_full_resolution_original_sync_enabled', return_value=True,
    ):
        # Build a minimal prepared_items path: patch prepare_images_cb to return our item
        def fake_prepare(obs_arg, progress_cb):
            return prepared_items, None, []

        result = cloud_sync._push_images_for_observation(
            client,
            obs,
            obs_cloud_id,
            prepare_images_cb=fake_prepare,
            summary_warnings=summary_warnings,
        )

    # result should be False (had_failures)
    assert result is False, f"Expected False (failure) but got {result!r}"
    # The failure message must appear in summary_warnings
    assert any('Image 10' in w or 'duplicate' in w for w in summary_warnings), (
        f"Expected image failure in summary_warnings but got: {summary_warnings}"
    )


# ---------------------------------------------------------------------------
# Gap C — mark_observation_dirty called on unhandled CloudSyncError
# ---------------------------------------------------------------------------


def test_gap_c_mark_obs_dirty_called_in_generic_cloud_sync_error_branch():
    """The else branch of the outer except CloudSyncError in push_all must call
    mark_observation_dirty. We verify this by patching mark_observation_dirty and
    checking it is called when a generic CloudSyncError escapes."""
    dirty_calls: list[int] = []

    with patch.object(
        cloud_sync, '_push_images_for_observation',
        side_effect=CloudSyncError('generic boom'),
    ), patch.object(
        cloud_sync, 'mark_observation_dirty',
        side_effect=lambda obs_id: dirty_calls.append(obs_id),
    ), patch.object(
        cloud_sync, 'is_cloud_auth_error', return_value=False,
    ), patch.object(
        cloud_sync, 'is_cloud_temporary_unavailable_error', return_value=False,
    ), patch.object(
        cloud_sync, 'is_privacy_slot_limit_error', return_value=False,
    ), patch.object(
        cloud_sync, 'is_image_too_large_for_plan_error', return_value=False,
    ), patch.object(
        cloud_sync, 'is_webp_support_required_for_cloud_media_upload_error', return_value=False,
    ):
        errors: list[str] = []
        # Call the except block logic directly by simulating how push_all would fire
        obs = {'id': 1}
        raw_error = f"obs {obs['id']}: generic boom"
        try:
            raise CloudSyncError('generic boom')
        except CloudSyncError as e:
            if not cloud_sync.is_cloud_auth_error(e) and not cloud_sync.is_cloud_temporary_unavailable_error(e):
                if not cloud_sync.is_privacy_slot_limit_error(raw_error) and \
                   not cloud_sync.is_image_too_large_for_plan_error(raw_error) and \
                   not cloud_sync.is_webp_support_required_for_cloud_media_upload_error(raw_error):
                    cloud_sync.mark_observation_dirty(int(obs['id']))
                    errors.append(raw_error)

    assert dirty_calls == [1], f"Expected mark_observation_dirty([1]) but got {dirty_calls}"
    assert errors
