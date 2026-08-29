from __future__ import annotations

from typing import Any

import pytest

from utils import cloud_sync


class _RecordingClient:
    user_id = "user-1"
    access_token = "token"

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events
        self.observation_list_calls = 0
        self.calibration_list_calls = 0

    def list_remote_observations(self) -> list[dict[str, Any]]:
        if self.events is not None:
            self.events.append("list_remote_observations")
        self.observation_list_calls += 1
        return [{"id": "cloud-observation-1"}]

    def list_remote_calibrations(self) -> list[dict[str, Any]]:
        if self.events is not None:
            self.events.append("list_remote_calibrations")
        self.calibration_list_calls += 1
        return [{"id": "cloud-calibration-1"}]


class _ReferenceReadingClient(_RecordingClient):
    def __init__(self, events: list[str] | None = None) -> None:
        super().__init__(events)
        self.reference_reads: list[str] = []

    def _reference_read(self, name: str) -> list[dict[str, Any]]:
        self.reference_reads.append(name)
        if self.events is not None:
            self.events.append(name)
        return []

    def list_reference_works(self) -> list[dict[str, Any]]:
        return self._reference_read("list_reference_works")

    def list_reference_taxon_treatments(self) -> list[dict[str, Any]]:
        return self._reference_read("list_reference_taxon_treatments")

    def list_reference_measurement_sets(self) -> list[dict[str, Any]]:
        return self._reference_read("list_reference_measurement_sets")

    def list_observation_reference_uses(self) -> list[dict[str, Any]]:
        return self._reference_read("list_observation_reference_uses")


def _patch_normal_sync(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str],
    pushed: int = 2,
) -> dict[str, dict[str, Any]]:
    observed: dict[str, dict[str, Any]] = {}
    monkeypatch.setattr(
        cloud_sync,
        "ensure_database_linked_to_cloud_user",
        lambda _client: events.append("account_binding"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "_cloud_measurement_remote_verification_due",
        lambda **_kwargs: (False, "characterization"),
    )

    def _push_calibrations(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        events.append("push_calibrations")
        observed["push_calibrations"] = kwargs
        return {"pushed": 1, "total": 1, "errors": ["calibration push"]}

    def _push_all(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        events.append("push_all")
        observed["push_all"] = kwargs
        return {
            "pushed": pushed,
            "total": pushed,
            "errors": ["observation push"],
            "original_sync": {"uploaded": 3},
            "spore_measurement_reconcile": {"attempted": pushed},
            "spore_summary_reconcile": {"attempted": 0},
            "sync_summary": cloud_sync._new_sync_summary(),
        }

    def _pull_all(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        events.append("pull_all")
        observed["pull_all"] = kwargs
        return {
            "pulled": 4,
            "total": 4,
            "errors": ["observation pull"],
            "deleted_remote": [9],
        }

    def _pull_calibrations(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        events.append("pull_calibrations")
        observed["pull_calibrations"] = kwargs
        return {"pulled": 5, "total": 5, "errors": ["calibration pull"]}

    monkeypatch.setattr(cloud_sync, "push_calibrations", _push_calibrations)
    monkeypatch.setattr(cloud_sync, "push_all", _push_all)
    monkeypatch.setattr(cloud_sync, "pull_all", _pull_all)
    monkeypatch.setattr(cloud_sync, "pull_calibrations", _pull_calibrations)
    return observed


def test_sync_all_preserves_legacy_result_and_caller_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from utils import reference_cloud_sync

    events: list[str] = []
    observed = _patch_normal_sync(monkeypatch, events=events)
    monkeypatch.setattr(
        reference_cloud_sync,
        "sync_reference_library",
        lambda client, *, pull_only=False: (
            events.append("sync_reference_library"),
            reference_cloud_sync.ReferenceSyncResult(
                pushed=6,
                pulled=7,
                errors=("reference transport",),
                retryable_errors=("reference transport",),
                conflicts=("work:conflict-1",),
                blocked=("treatment:blocked-1",),
            ),
        )[1],
    )
    client = _RecordingClient(events)

    result = cloud_sync.sync_all(
        client,
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=False,
    )

    sync_summary = result.pop("sync_summary")
    assert result == {
        "pushed": 2,
        "pulled": 4,
        "calibrations_pushed": 1,
        "calibrations_pulled": 5,
        "errors": [
            "calibration push",
            "observation push",
            "observation pull",
            "calibration pull",
            "reference transport",
            "reference sync conflict: work:conflict-1",
            "reference sync blocked: treatment:blocked-1",
        ],
        "deleted_remote": [9],
        "original_sync": {"uploaded": 3},
        "reference_sync": {
            "pushed": 6,
            "pulled": 7,
            "errors": ["reference transport"],
            "retryable_errors": ["reference transport"],
            "terminal_errors": [],
            "conflicts": ["work:conflict-1"],
            "blocked": ["treatment:blocked-1"],
        },
    }
    assert sync_summary == {key: 0 for key in cloud_sync._SYNC_SUMMARY_KEYS}
    assert events == [
        "account_binding",
        "list_remote_observations",
        "list_remote_calibrations",
        "push_calibrations",
        "push_all",
        "list_remote_observations",
        "pull_all",
        "pull_calibrations",
        "sync_reference_library",
    ]
    assert observed["push_all"]["sync_images"] is False
    assert observed["push_all"]["full_pull"] is False
    assert observed["pull_all"]["sync_images"] is False
    assert observed["pull_all"]["materialize_remote_images"] is False
    assert observed["pull_all"]["full_pull"] is False


def test_sync_all_proven_noop_reuses_remote_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from utils import reference_cloud_sync

    events: list[str] = []
    observed = _patch_normal_sync(monkeypatch, events=events, pushed=0)
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *_args, **_kwargs: {"pushed": 0, "total": 0, "errors": []},
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_all",
        lambda *_args, **_kwargs: {
            "pushed": 0,
            "total": 0,
            "errors": [],
            "spore_measurement_reconcile": {"attempted": 0},
            "spore_summary_reconcile": {"attempted": 0},
            "sync_summary": cloud_sync._new_sync_summary(),
        },
    )
    client = _RecordingClient()
    monkeypatch.setattr(
        reference_cloud_sync,
        "sync_reference_library",
        lambda _client, *, pull_only=False: reference_cloud_sync.ReferenceSyncResult(),
    )

    cloud_sync.sync_all(client, full_pull=False)

    assert client.observation_list_calls == 1
    assert observed["pull_all"]["remote_obs"] == [
        {"id": "cloud-observation-1"}
    ]


def test_sync_all_pull_only_preserves_legacy_result_and_skips_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from utils import reference_cloud_sync

    events: list[str] = []
    monkeypatch.setattr(
        cloud_sync,
        "ensure_database_linked_to_cloud_user",
        lambda _client: events.append("account_binding"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *_args, **_kwargs: pytest.fail("pull-only called push_calibrations"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_all",
        lambda *_args, **_kwargs: pytest.fail("pull-only called push_all"),
    )

    def _pull_all(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        events.append("pull_all")
        assert kwargs["sync_images"] is False
        assert kwargs["materialize_remote_images"] is False
        assert kwargs["full_pull"] is True
        assert kwargs["pull_only"] is True
        return {
            "pulled": 3,
            "calibrations_pulled": 2,
            "errors": ["pull issue"],
            "deleted_remote": [7],
        }

    monkeypatch.setattr(cloud_sync, "pull_all", _pull_all)

    def _sync_reference(client: object, *, pull_only: bool = False):
        events.append("sync_reference_library")
        assert isinstance(client, cloud_sync.PullOnlyCloudClient)
        assert pull_only is True
        return reference_cloud_sync.ReferenceSyncResult(pulled=8)

    monkeypatch.setattr(
        reference_cloud_sync, "sync_reference_library", _sync_reference
    )
    client = _RecordingClient()

    result = cloud_sync.sync_all(
        client,
        sync_images=False,
        materialize_remote_images=False,
        pull_only=True,
    )

    sync_summary = result.pop("sync_summary")
    assert result == {
        "pushed": 0,
        "pulled": 3,
        "calibrations_pushed": 0,
        "calibrations_pulled": 2,
        "errors": ["pull issue"],
        "deleted_remote": [7],
        "pull_only": True,
        "images_downloaded": 0,
        "observations_updated": 3,
        "cloud_writes_completed": 0,
        "blocked_write_attempts": [],
        "reference_sync": {
            "pushed": 0,
            "pulled": 8,
            "errors": [],
            "retryable_errors": [],
            "terminal_errors": [],
            "conflicts": [],
            "blocked": [],
        },
    }
    assert sync_summary == {key: 0 for key in cloud_sync._SYNC_SUMMARY_KEYS}
    assert events == ["account_binding", "pull_all", "sync_reference_library"]


def test_empty_reference_result_adds_compatible_typed_surface() -> None:
    from utils.reference_cloud_sync import (
        ReferenceSyncResult,
        merge_reference_sync_result,
    )

    legacy_result = {
        "pushed": 2,
        "pulled": 4,
        "errors": ["existing issue"],
        "sync_summary": {"images_uploaded": 1},
    }

    merged = merge_reference_sync_result(legacy_result, ReferenceSyncResult())

    assert merged is legacy_result
    assert merged == {
        "pushed": 2,
        "pulled": 4,
        "errors": ["existing issue"],
        "sync_summary": {"images_uploaded": 1},
        "reference_sync": {
            "pushed": 0,
            "pulled": 0,
            "errors": [],
            "retryable_errors": [],
            "terminal_errors": [],
            "conflicts": [],
            "blocked": [],
        },
    }


def test_reference_result_merge_preserves_legacy_counts_and_surfaces_issues() -> None:
    from utils.reference_cloud_sync import (
        ReferenceSyncResult,
        merge_reference_sync_result,
    )

    merged = merge_reference_sync_result(
        {"pushed": 2, "pulled": 3, "errors": ["legacy"]},
        ReferenceSyncResult(
            pushed=1,
            pulled=4,
            conflicts=("work:w1",),
            blocked=("treatment:t1",),
        ),
    )

    assert merged["pushed"] == 2
    assert merged["pulled"] == 3
    assert merged["errors"] == [
        "legacy",
        "reference sync conflict: work:w1",
        "reference sync blocked: treatment:t1",
    ]


def test_reference_facade_pull_only_never_runs_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from utils import reference_cloud_sync

    pulled = reference_cloud_sync.ReferenceSyncResult(pulled=2)
    monkeypatch.setattr(
        reference_cloud_sync, "pull_reference_library", lambda _client: pulled
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "_push_reference_library",
        lambda _client: pytest.fail("pull-only reference sync attempted a push"),
    )

    assert (
        reference_cloud_sync.sync_reference_library(object(), pull_only=True)
        is pulled
    )


def test_sync_all_pull_only_runs_real_facade_reads_through_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from utils import reference_cloud_sync

    monkeypatch.setattr(
        cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None
    )
    monkeypatch.setattr(
        cloud_sync,
        "pull_all",
        lambda *_args, **_kwargs: {
            "pulled": 0,
            "errors": [],
            "deleted_remote": [],
        },
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "stage_reference_library_feed",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "stage_observation_reference_use_feed",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "reconcile_reference_library_feed",
        lambda *_args, **_kwargs: SimpleNamespace(
            applied=0, conflicts=(), blocked=()
        ),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "_push_reference_library",
        lambda _client: pytest.fail("pull-only reached reference push"),
    )
    client = _ReferenceReadingClient()

    result = cloud_sync.sync_all(client, pull_only=True)

    assert client.reference_reads == [
        "list_reference_works",
        "list_reference_taxon_treatments",
        "list_reference_measurement_sets",
        "list_observation_reference_uses",
    ]
    assert result["reference_sync"]["pushed"] == 0
    assert result["blocked_write_attempts"] == []


def test_sync_all_normal_runs_real_facade_reads_then_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from utils import reference_cloud_sync

    events: list[str] = []
    _patch_normal_sync(monkeypatch, events=events, pushed=0)
    monkeypatch.setattr(
        reference_cloud_sync,
        "stage_reference_library_feed",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "stage_observation_reference_use_feed",
        lambda *_args: object(),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "reconcile_reference_library_feed",
        lambda *_args, **_kwargs: SimpleNamespace(
            applied=0, conflicts=(), blocked=()
        ),
    )
    monkeypatch.setattr(
        reference_cloud_sync,
        "_push_reference_library",
        lambda _client: (
            events.append("push_reference_library"),
            reference_cloud_sync.ReferenceSyncResult(pushed=1),
        )[1],
    )
    client = _ReferenceReadingClient(events)

    result = cloud_sync.sync_all(client, full_pull=False)

    assert events[-5:] == [
        "list_reference_works",
        "list_reference_taxon_treatments",
        "list_reference_measurement_sets",
        "list_observation_reference_uses",
        "push_reference_library",
    ]
    assert result["reference_sync"]["pushed"] == 1
