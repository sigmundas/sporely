"""D8: summary-failure dirty marking uses the real two-argument
``mark_observation_sync_dirty(cursor, id)`` signature.

Before D8, ``_push_summary_for_current_observation`` and
``_reconcile_missing_spore_measurements`` called
``mark_observation_sync_dirty(local_id)`` with one argument. That raised
``TypeError`` which ``except Exception: pass`` swallowed, so a failed
per-observation summary push left the observation looking synced. These
tests use the real DB helpers against a temp SQLite file (no one-arg fakes).
"""
from __future__ import annotations

import sqlite3

import pytest

from database.models import mark_observation_sync_dirty
from utils import cloud_sync as cs


def _make_db(path, obs_ids=(17,)):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT, "
        "sync_status TEXT, sync_error_code TEXT, sync_error_message TEXT, "
        "sync_blocked_reason TEXT, sync_blocked_at TEXT)"
    )
    for obs_id in obs_ids:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, 'synced')",
            (obs_id, f"cloud-{obs_id}"),
        )
    conn.commit()
    conn.close()


def _status(path, obs_id):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT sync_status FROM observations WHERE id = ?", (obs_id,)
        ).fetchone()[0]
    finally:
        conn.close()


class _Client:
    user_id = "user-x"


def _raise_summary(*a, **kw):
    raise RuntimeError("boom: RLS denied")


def test_one_argument_call_raises_type_error():
    """Mechanism: the old call shape cannot work with the real signature."""
    with pytest.raises(TypeError):
        mark_observation_sync_dirty(17)  # type: ignore[call-arg]


def test_per_observation_summary_failure_marks_observation_dirty(tmp_path, monkeypatch):
    db_path = tmp_path / "d8.sqlite"
    _make_db(db_path)
    monkeypatch.setattr(cs, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _raise_summary)

    errors: list[str] = []
    result = cs._push_summary_for_current_observation(
        _Client(), obs={"id": 17}, local_obs_id=17, cloud_id="cloud-17", errors=errors,
    )
    assert result is None
    assert len(errors) == 1 and "spore summary sync failed" in errors[0]
    assert _status(db_path, 17) == "dirty"


def test_backfill_summary_failure_reports_without_dirtying(tmp_path, monkeypatch):
    """D5=b: backfill passes report failures but do not dirty."""
    db_path = tmp_path / "d8.sqlite"
    _make_db(db_path)
    monkeypatch.setattr(cs, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _raise_summary)

    errors: list[str] = []
    result = cs._push_summary_for_current_observation(
        _Client(), obs={"id": 17}, local_obs_id=17, cloud_id="cloud-17",
        errors=errors, mark_dirty_on_error=False,
    )
    assert result is None
    assert len(errors) == 1 and "spore summary sync failed" in errors[0]
    assert _status(db_path, 17) == "synced"
