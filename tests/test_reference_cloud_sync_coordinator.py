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
    events: list[str] = []
    observed = _patch_normal_sync(monkeypatch, events=events)
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
        ],
        "deleted_remote": [9],
        "original_sync": {"uploaded": 3},
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
    ]
    assert observed["push_all"]["sync_images"] is False
    assert observed["push_all"]["full_pull"] is False
    assert observed["pull_all"]["sync_images"] is False
    assert observed["pull_all"]["materialize_remote_images"] is False
    assert observed["pull_all"]["full_pull"] is False


def test_sync_all_proven_noop_reuses_remote_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    cloud_sync.sync_all(client, full_pull=False)

    assert client.observation_list_calls == 1
    assert observed["pull_all"]["remote_obs"] == [
        {"id": "cloud-observation-1"}
    ]


def test_sync_all_pull_only_preserves_legacy_result_and_skips_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    }
    assert sync_summary == {key: 0 for key in cloud_sync._SYNC_SUMMARY_KEYS}
    assert events == ["account_binding", "pull_all"]


def test_reference_sync_facade_is_a_side_effect_free_noop() -> None:
    from utils.reference_cloud_sync import (
        ReferenceSyncResult,
        sync_reference_library,
    )

    class _ClientThatMustNotBeInspected:
        def __getattribute__(self, name: str) -> Any:
            if name.startswith("__"):
                return super().__getattribute__(name)
            raise AssertionError(f"no-op reference sync inspected client.{name}")

    result = sync_reference_library(_ClientThatMustNotBeInspected())

    assert result == ReferenceSyncResult()
    assert result.pushed == 0
    assert result.pulled == 0
    assert result.errors == ()


def test_empty_reference_result_merges_without_changing_legacy_result() -> None:
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
    }


def test_stage4a_merge_rejects_nonempty_reference_results() -> None:
    from utils.reference_cloud_sync import (
        ReferenceSyncResult,
        merge_reference_sync_result,
    )

    with pytest.raises(ValueError, match="Stage 4a supports only empty"):
        merge_reference_sync_result(
            {"pushed": 0, "pulled": 0, "errors": []},
            ReferenceSyncResult(pushed=1),
        )
