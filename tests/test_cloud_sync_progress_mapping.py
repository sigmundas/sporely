"""Progress bar semantics for the cloud sync worker.

The UI shows a single progress bar backed by the ``(message, current, total)``
progress_cb events emitted by :mod:`utils.cloud_sync`. This test-file locks
in the weighted phase mapping so:

- early messages ("Loading cloud observations", preflight scans, …) do not
  jump the bar to 99% while real work is still ahead;
- ``Checking cloud observation N/M`` progresses monotonically within the
  pull-observation slice;
- calibration pull / linking sits in the 92–97% range;
- only the final ``Finalizing`` event reaches 100%.
"""

from __future__ import annotations

import pytest

from utils import cloud_sync


def test_phase_ranges_are_monotonic_and_cover_0_to_100():
    phases = cloud_sync._SYNC_PROGRESS_PHASES
    assert phases[0][1] == 0, "first phase must start at 0%"
    assert phases[-1][2] == 100, "last phase must end at 100%"
    for prev, curr in zip(phases, phases[1:]):
        prev_end = prev[2]
        curr_start = curr[1]
        assert curr_start == prev_end, (
            f"phase ranges must be contiguous: {prev[0]} ends at {prev_end}, "
            f"{curr[0]} starts at {curr_start}"
        )
        assert curr[2] > curr[1], f"phase {curr[0]} must span a range"


def test_sync_progress_percent_maps_within_phase_bounds():
    # A phase's start value is emitted when no work has begun.
    assert cloud_sync._sync_progress_percent('pull_observations', 0, 156) == 75
    # Half-way through a phase → half-way through its range.
    mid = cloud_sync._sync_progress_percent('pull_observations', 78, 156)
    assert 82 <= mid <= 85
    # Overrun is clamped to the phase's end.
    assert cloud_sync._sync_progress_percent('pull_observations', 200, 156) == 92
    # Unknown phase → 0 (never reports a false 99%).
    assert cloud_sync._sync_progress_percent('__unknown__', 5, 10) == 0


def test_emit_progress_uses_phase_percent_when_phase_is_set():
    events: list[tuple[str, int, int]] = []
    state: dict = {}
    cloud_sync._set_progress_phase(state, 'pull_observations', phase_total=156)
    cb = lambda msg, cur, tot: events.append((msg, int(cur), int(tot)))

    cloud_sync._emit_progress(cb, "Checking cloud observation 1/156…", state)
    cloud_sync._advance_progress(state, 110)
    cloud_sync._emit_progress(cb, "Checking cloud observation 111/156…", state)

    assert events[0] == ("Checking cloud observation 1/156…", 75, 100)
    # Second event is well inside the pull_observations 75..92 slice.
    _, second_cur, second_tot = events[1]
    assert second_tot == 100
    assert 75 <= second_cur <= 92


def test_emit_progress_falls_back_to_raw_counts_without_phase():
    """A caller that seeds only {done, total} keeps the old behavior.

    Standalone helper tests seed a raw progress_state without wiring phases;
    they must keep working exactly as before.
    """
    events: list[tuple[str, int, int]] = []
    state = {'done': 3, 'total': 10}
    cb = lambda msg, cur, tot: events.append((msg, int(cur), int(tot)))

    cloud_sync._emit_progress(cb, "raw", state)

    assert events == [("raw", 3, 10)]


def test_set_progress_phase_resets_per_phase_counters():
    state = {'done': 42, 'total': 42}
    cloud_sync._set_progress_phase(state, 'push_observations', phase_total=10)
    assert state['done'] == 0
    assert state['total'] == 10
    assert state['phase'] == 'push_observations'


def test_early_loading_cloud_observations_stays_under_50_percent():
    """The message the user reported (bar sitting near 99%) belongs to auth.

    Emitting ``Loading cloud observations…`` from the auth phase must not
    approach the end of the bar — otherwise the sync visibly "hangs" at 99%
    while every remaining phase is still running.
    """
    events: list[tuple[str, int, int]] = []
    state: dict = {}
    cloud_sync._set_progress_phase(state, 'auth')
    cb = lambda msg, cur, tot: events.append((msg, int(cur), int(tot)))

    cloud_sync._emit_progress(cb, "Loading cloud observations…", state)

    _, percent, total = events[-1]
    assert total == 100
    assert percent < 50
    # And well under the 99% mark the user reported.
    assert percent < 90


def test_calibration_pull_reaches_high_nineties_but_not_finalize():
    events: list[tuple[str, int, int]] = []
    state: dict = {}
    cloud_sync._set_progress_phase(state, 'calibration_pull', phase_total=8)
    cb = lambda msg, cur, tot: events.append((msg, int(cur), int(tot)))

    for i in range(1, 9):
        cloud_sync._advance_progress(state, 1)
        cloud_sync._emit_progress(cb, f"Checking calibration {i}/8…", state)

    percent_first = events[0][1]
    percent_last = events[-1][1]
    # Starts inside the calibration_pull slice, ends at the top of it.
    assert 92 <= percent_first <= 97
    assert percent_last == 97
    # And it does NOT bleed into the finalize slice.
    assert percent_last < 99


def test_finalize_phase_reaches_100_percent():
    events: list[tuple[str, int, int]] = []
    state: dict = {}
    cloud_sync._set_progress_phase(state, 'finalize', phase_total=1)
    cloud_sync._advance_progress(state, 1)
    cloud_sync._emit_progress(
        lambda msg, cur, tot: events.append((msg, int(cur), int(tot))),
        "Finalizing cloud sync…",
        state,
    )
    assert events[-1] == ("Finalizing cloud sync…", 100, 100)


def test_pull_observation_progresses_monotonically_across_the_phase():
    """Bar advances between the first and 111th of 156 observations."""
    events: list[tuple[str, int, int]] = []
    state: dict = {}
    cloud_sync._set_progress_phase(state, 'pull_observations', phase_total=156)
    cb = lambda msg, cur, tot: events.append((msg, int(cur), int(tot)))

    cloud_sync._emit_progress(cb, "Checking cloud observation 1/156…", state)
    for _ in range(110):
        cloud_sync._advance_progress(state, 1)
    cloud_sync._emit_progress(cb, "Checking cloud observation 111/156…", state)

    first_percent = events[0][1]
    later_percent = events[-1][1]
    assert later_percent > first_percent
    # And still well under 99% — real work remains after pull_observations.
    assert later_percent < 92


def test_unknown_phase_name_leaves_state_untouched():
    state = {'done': 5, 'total': 10, 'phase': 'push_observations'}
    cloud_sync._set_progress_phase(state, 'nonexistent-phase')
    # Unknown phase requests are silently ignored — bad wiring must not
    # blank out an existing phase.
    assert state['phase'] == 'push_observations'
    assert state['done'] == 5
    assert state['total'] == 10
