"""Durable intent for a local observation's cloud delete (contract rules 6 and 8).

A delete interrupted after the local row was removed (crash between the
image-row and observation DELETEs, a failed request, no session) is finished
by the next write-enabled sync; pull never re-imports the observation
meanwhile. Uses the shared fake cloud of the sync-key child tests.
"""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from database import schema
from database.models import ObservationDB
from utils import cloud_sync
from utils.cloud_sync_impl.errors import CloudUploadRefusedError
from tests.test_sync_key_child_identity import (  # noqa: F401  (fixture)
    ChildCloud,
    _image,
    _observation,
    _push,
    _row,
    two_desktops,
)


def _local_rows_for(cloud_id) -> list[int]:
    with sqlite3.connect(schema.get_database_path()) as connection:
        return [r[0] for r in connection.execute(
            "SELECT id FROM observations WHERE cloud_id = ?", (str(cloud_id),),
        )]


def _uploaded_observation(cloud: ChildCloud, tmp_path) -> tuple[int, str]:
    obs_id = _observation()
    _image(obs_id, tmp_path, image_type="field")
    result = _push(cloud.client())
    assert result["errors"] == []
    return obs_id, str(_row("observations", obs_id)["cloud_id"])


def _delete_locally_after_partial_cloud_delete(cloud: ChildCloud, obs_id: int, cloud_id: str) -> None:
    """The UI's order: intent, local delete, then the cloud delete, which is
    cut off after the image-row DELETE (the ``between_deletes`` crash)."""
    cloud_sync.mark_observation_delete_pending(cloud_id, cloud.client().user_id)
    ObservationDB.delete_observation(obs_id)
    cloud.tables["observation_images"] = [
        r for r in cloud.tables["observation_images"] if str(r["observation_id"]) != cloud_id
    ]
    cloud.requests.clear()


def test_an_interrupted_delete_is_never_re_imported_and_finishes_on_the_next_sync(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, cloud_id = _uploaded_observation(cloud, tmp_path)
    _delete_locally_after_partial_cloud_delete(cloud, obs_id, cloud_id)
    assert [str(r["id"]) for r in cloud.rows("observations")] == [cloud_id], "the partial cloud row"

    cloud_sync.sync_all(cloud.client(), pull_only=True, materialize_remote_images=False)
    assert _local_rows_for(cloud_id) == [], "Download from Cloud does not resurrect it"
    assert cloud.writes("observations") == [], "and writes nothing"
    assert cloud_sync.remote_observations_not_on_this_desktop(cloud.rows("observations")) == []

    result = cloud_sync.sync_all(cloud.client(), materialize_remote_images=False, full_pull=False)

    assert cloud.rows("observations") == [], "the next sync finished the delete"
    assert _local_rows_for(cloud_id) == []
    assert cloud_sync.pending_observation_deletes() == {}
    assert not any("pending delete failed" in str(e) for e in result.get("errors") or [])


def test_a_relinked_local_row_voids_the_intent_and_nothing_is_deleted(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, cloud_id = _uploaded_observation(cloud, tmp_path)
    cloud_sync.mark_observation_delete_pending(cloud_id, cloud.client().user_id)  # local delete never ran
    cloud.requests.clear()

    summary = cloud_sync.retry_pending_observation_deletes(cloud.client())

    assert summary["void"] == [cloud_id]
    assert cloud.writes("observations", "DELETE") == [] and cloud.writes("observation_images", "DELETE") == []
    assert cloud_sync.pending_observation_deletes() == {}
    assert _local_rows_for(cloud_id) == [obs_id]


def test_another_accounts_intent_is_not_sent(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, cloud_id = _uploaded_observation(cloud, tmp_path)
    cloud_sync.mark_observation_delete_pending(cloud_id, "someone-else")
    ObservationDB.delete_observation(obs_id)

    cloud_sync.retry_pending_observation_deletes(cloud.client())

    assert [str(r["id"]) for r in cloud.rows("observations")] == [cloud_id]
    assert cloud_sync.pending_observation_deletes() == {cloud_id: "someone-else"}


def test_an_upload_refusal_stops_the_retry_and_keeps_the_intent(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, cloud_id = _uploaded_observation(cloud, tmp_path)
    _delete_locally_after_partial_cloud_delete(cloud, obs_id, cloud_id)
    client = cloud.client()
    client._upload_refusal = SimpleNamespace(hint="sporely_upgrade_required", __str__=lambda self: "refused")

    with pytest.raises(CloudUploadRefusedError):
        cloud_sync.retry_pending_observation_deletes(client)

    assert [str(r["id"]) for r in cloud.rows("observations")] == [cloud_id]
    assert cloud_id in cloud_sync.pending_observation_deletes()


def test_a_failed_retry_keeps_the_intent_for_the_next_sync(two_desktops, tmp_path, monkeypatch):
    cloud = ChildCloud()
    obs_id, cloud_id = _uploaded_observation(cloud, tmp_path)
    _delete_locally_after_partial_cloud_delete(cloud, obs_id, cloud_id)
    client = cloud.client()

    def unavailable(_cloud_id):
        raise cloud_sync.CloudSyncError("worker unavailable")
    monkeypatch.setattr(client, "delete_cloud_observation", unavailable)

    summary = cloud_sync.retry_pending_observation_deletes(client)

    assert summary["completed"] == [] and len(summary["errors"]) == 1
    assert cloud_id in cloud_sync.pending_observation_deletes()


def test_pending_delete_retry_is_a_pull_only_blocked_write(two_desktops):
    assert "delete_cloud_observation" in cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
