"""Tests for sync change classification and progress-gap tracing.

These cover two regressions:
  - A no-change sync (dirty observations checked but nothing actually written
    remotely or pulled locally) must not be classified as a real change, so the
    UI does not claim an observation was synced.
  - When a slow backend step runs between two UI progress updates, the gap is
    logged so the silent pause can be traced.
"""

from utils import cloud_sync


def _result_with_summary(**summary):
    base = cloud_sync._new_sync_summary()
    base.update(summary)
    return {"sync_summary": base}


def test_noop_dirty_sync_is_not_a_real_change():
    # An observation was re-dirtied and checked, the upsert was a no-op, and a
    # local-only cloud_id association was repaired. None of that is a real
    # remote-facing or local change the user should be told about.
    result = _result_with_summary(
        observations_checked=1,
        observations_skipped_noop=1,
        images_cloud_id_repaired=2,
    )
    result["pushed"] = 1  # dirty-scan count, not a real change
    result["pulled"] = 0

    activity = cloud_sync.summarize_sync_change_activity(result)

    assert activity["any_real_change"] is False
    assert activity["real_remote_change"] == 0
    assert activity["real_local_change"] == 0
    assert activity["observations_checked_noop"] == 1
    assert activity["observations_images_repaired_local_only"] == 2


def test_observation_metadata_patch_is_a_real_change():
    result = _result_with_summary(observations_checked=1, observations_patched=1)
    result["pushed"] = 1

    activity = cloud_sync.summarize_sync_change_activity(result)

    assert activity["any_real_change"] is True
    assert activity["observations_metadata_patched"] == 1


def test_image_upload_is_a_real_change():
    result = _result_with_summary(images_uploaded=1)

    activity = cloud_sync.summarize_sync_change_activity(result)

    assert activity["any_real_change"] is True
    assert activity["real_remote_change"] == 1


def test_pulled_observation_is_a_real_local_change():
    result = _result_with_summary()
    result["pulled"] = 2

    activity = cloud_sync.summarize_sync_change_activity(result)

    assert activity["any_real_change"] is True
    assert activity["real_local_change"] == 2


def test_remote_deletion_review_is_not_folded_into_real_change():
    # Cloud deletions awaiting local review are surfaced by their own
    # notification branch, so they are reported but do not force "complete".
    result = _result_with_summary()
    result["deleted_remote"] = [{"cloud_id": "abc"}]

    activity = cloud_sync.summarize_sync_change_activity(result)

    assert activity["deleted_remote_rows"] == 1
    assert activity["any_real_change"] is False


def test_progress_gap_logs_slow_step_between_ui_updates(monkeypatch, capsys):
    trace = {"start": 0.0, "last_t": None, "last_msg": None}
    token = cloud_sync._CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.set(trace)
    times = iter([0.0, 5.0])  # first emit at t=0, second at t=5s
    monkeypatch.setattr(cloud_sync, "_cloud_sync_perf_counter", lambda: next(times))
    try:
        cloud_sync._emit_progress(None, "Checking calibration 4/8: 100X…", {})
        cloud_sync._emit_progress(None, "Linking calibration images…", {})
    finally:
        cloud_sync._CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.reset(token)

    output = capsys.readouterr().out
    assert "progress gap" in output
    assert "Checking calibration 4/8: 100X…" in output
    assert "Linking calibration images…" in output


def test_progress_gap_quiet_for_fast_updates(monkeypatch, capsys):
    trace = {"start": 0.0, "last_t": None, "last_msg": None}
    token = cloud_sync._CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.set(trace)
    times = iter([0.0, 0.1])
    monkeypatch.setattr(cloud_sync, "_cloud_sync_perf_counter", lambda: next(times))
    try:
        cloud_sync._emit_progress(None, "Syncing observation 1/3…", {})
        cloud_sync._emit_progress(None, "Syncing observation 2/3…", {})
    finally:
        cloud_sync._CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.reset(token)

    assert "progress gap" not in capsys.readouterr().out


def test_progress_trace_is_noop_without_active_context(capsys):
    # Outside a sync_all run there is no trace context; emission must be silent.
    cloud_sync._emit_progress(None, "Standalone message", {})
    assert "progress gap" not in capsys.readouterr().out
