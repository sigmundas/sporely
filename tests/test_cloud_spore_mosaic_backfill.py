"""Tests for the explicit spore-mosaic backfill helper + CLI.

These tests stub `_push_spore_mosaic_for_observation` (which is exercised
end-to-end by the mosaic tests) so we can focus on the backfill helper's
own contract:

* pre-filtering by cloud_id / limit
* per-observation dispatch to the pusher
* status → counter mapping
* auth/temporary errors abort the run
* other exceptions count as `failed` but the loop keeps going
* logging matches the interface the operator UI expects
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from utils import cloud_sync


# ── DB fixture ──────────────────────────────────────────────────────────────


def _init_backfill_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sporely_backfill.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            spore_data_visibility TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_obs(db_path: Path, *, cloud_id: str | None, spore_data_visibility: str = 'public') -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            'INSERT INTO observations (cloud_id, spore_data_visibility) VALUES (?, ?)',
            (cloud_id, spore_data_visibility),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


class _FakeClient:
    """Cheap stand-in for SporelyCloudClient — the backfill helper only reads
    `client.user_id` and passes the whole thing through to the pusher."""

    def __init__(self, user_id: str = 'user-uuid'):
        self.user_id = user_id


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_backfill_db(tmp_path)
    monkeypatch.setattr(cloud_sync, 'get_connection', lambda: sqlite3.connect(db_path))
    # By default silence the measurement push — the tests here don't stage
    # spore_measurements rows and would otherwise print noisy "no such
    # table" errors. Tests that specifically exercise the measurement-push
    # hook stub it themselves with monkeypatch.
    monkeypatch.setattr(
        cloud_sync,
        '_push_measurements_for_observation',
        lambda client, obs_local_id: None,
    )
    return db_path


# ── Basic dispatch ──────────────────────────────────────────────────────────


def test_backfill_finds_eligible_observation_and_calls_pusher(db, monkeypatch):
    local_id = _insert_obs(db, cloud_id='719', spore_data_visibility='public')

    calls: list[tuple[int, str]] = []

    def fake_push(client, obs_local_id, obs_cloud_id):
        calls.append((obs_local_id, obs_cloud_id))
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    assert calls == [(local_id, '719')]
    assert result == {
        'candidates': 1,
        'generated': 1,
        'skipped_no_cloud_id': 0,
        'skipped_no_public_spores': 0,
        'skipped_no_measurement_cloud_ids': 0,
        'skipped_missing_source_images': 0,
        'failed': 0,
    }


def test_backfill_respects_observation_cloud_ids_whitelist(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='100')
    target = _insert_obs(db, cloud_id='719')
    _insert_obs(db, cloud_id='999')

    calls: list[tuple[int, str]] = []

    def fake_push(client, obs_local_id, obs_cloud_id):
        calls.append((obs_local_id, obs_cloud_id))
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(
        _FakeClient(),
        observation_cloud_ids=[719],  # int is accepted
    )

    assert calls == [(target, '719')]
    assert result['candidates'] == 1
    assert result['generated'] == 1

    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic backfill: start observation_cloud_ids=' in stdout
    assert "['719']" in stdout


def test_backfill_reports_whitelist_entry_with_no_matching_local_observation(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='100')

    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    result = cloud_sync.backfill_public_spore_mosaics(
        _FakeClient(),
        observation_cloud_ids=[8888],
    )

    assert result['candidates'] == 0
    assert result['generated'] == 0
    stdout, _stderr = capfd.readouterr()
    assert 'reason=no_local_observation_with_cloud_id' in stdout
    assert 'cloud=8888' in stdout


def test_backfill_respects_limit(db, monkeypatch):
    ids = [_insert_obs(db, cloud_id=str(cid)) for cid in range(1, 6)]
    calls: list[int] = []

    def fake_push(client, obs_local_id, obs_cloud_id):
        calls.append(obs_local_id)
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient(), limit=2)

    assert result['candidates'] == 2
    assert result['generated'] == 2
    assert calls == ids[:2]


def test_backfill_ignores_observations_without_cloud_id(db, monkeypatch):
    _insert_obs(db, cloud_id=None)
    _insert_obs(db, cloud_id='')
    target = _insert_obs(db, cloud_id='42')

    calls: list[tuple[int, str]] = []

    def fake_push(client, obs_local_id, obs_cloud_id):
        calls.append((obs_local_id, obs_cloud_id))
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    assert calls == [(target, '42')]
    assert result['candidates'] == 1


# ── Status → counter mapping ────────────────────────────────────────────────


def test_backfill_counts_skips_by_reason(db, monkeypatch, capfd):
    ids = {
        'private': _insert_obs(db, cloud_id='100'),
        'no_meas': _insert_obs(db, cloud_id='200'),
        'missing': _insert_obs(db, cloud_id='300'),
        'no_usable': _insert_obs(db, cloud_id='400'),
        'good': _insert_obs(db, cloud_id='500'),
    }

    def fake_push(client, obs_local_id, obs_cloud_id):
        return {
            ids['private']:   cloud_sync.MOSAIC_STATUS_SKIP_NO_PUBLIC_SPORE_DATA,
            ids['no_meas']:   cloud_sync.MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS,
            ids['missing']:   cloud_sync.MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES,
            ids['no_usable']: cloud_sync.MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES,
            ids['good']:      cloud_sync.MOSAIC_STATUS_GENERATED,
        }[obs_local_id]

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    assert result['candidates'] == 5
    assert result['generated'] == 1
    assert result['skipped_no_public_spores'] == 1
    assert result['skipped_no_measurement_cloud_ids'] == 1
    # Missing-source-images and no-usable-sources both roll up to the same
    # user-facing bucket — operators care that "we can't crop from local
    # files", not which SQL corner triggered it.
    assert result['skipped_missing_source_images'] == 2
    assert result['failed'] == 0

    stdout, _stderr = capfd.readouterr()
    assert 'reason=no_public_spore_data' in stdout
    assert 'reason=no_measurement_cloud_ids' in stdout
    assert 'reason=missing_source_images' in stdout


def test_backfill_counts_fail_statuses(db, monkeypatch):
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')

    responses = iter([
        cloud_sync.MOSAIC_STATUS_FAIL_UPLOAD,
        cloud_sync.MOSAIC_STATUS_FAIL_TILE_INSERT,
    ])
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: next(responses),
    )

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())
    assert result['generated'] == 0
    assert result['failed'] == 2


def test_backfill_unknown_status_counted_as_failed(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='1')

    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: 'something_new_from_the_future',
    )

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())
    assert result['failed'] == 1
    stdout, _stderr = capfd.readouterr()
    assert 'unknown status' in stdout


# ── Failure isolation ───────────────────────────────────────────────────────


def test_backfill_continues_after_unexpected_exception(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')
    _insert_obs(db, cloud_id='3')

    def fake_push(client, obs_local_id, obs_cloud_id):
        if obs_cloud_id == '2':
            raise RuntimeError('kaboom')
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    assert result['generated'] == 2
    assert result['failed'] == 1
    stdout, _stderr = capfd.readouterr()
    assert 'unexpected failure' in stdout
    assert 'kaboom' in stdout


def test_backfill_aborts_on_auth_error(db, monkeypatch):
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')

    invocations: list[str] = []

    class AuthError(Exception):
        pass

    def fake_push(client, obs_local_id, obs_cloud_id):
        invocations.append(obs_cloud_id)
        raise AuthError('token expired')

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: isinstance(exc, AuthError))
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: False)

    with pytest.raises(AuthError):
        cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    # We should have bailed on the very first observation.
    assert invocations == ['1']


def test_backfill_aborts_on_temporary_error(db, monkeypatch):
    _insert_obs(db, cloud_id='1')

    class Temp(Exception):
        pass

    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: (_ for _ in ()).throw(Temp('503')),
    )
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: False)
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: isinstance(exc, Temp))

    with pytest.raises(Temp):
        cloud_sync.backfill_public_spore_mosaics(_FakeClient())


# ── Complete-log line ───────────────────────────────────────────────────────


def test_backfill_logs_start_and_complete_lines(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='719')
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )
    cloud_sync.backfill_public_spore_mosaics(
        _FakeClient(),
        observation_cloud_ids=['719'],
        limit=None,
    )
    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic backfill: start observation_cloud_ids=' in stdout
    assert "['719']" in stdout
    assert 'limit=None' in stdout
    assert 'Mosaic backfill: candidate local=' in stdout
    assert 'cloud=719' in stdout
    assert (
        'Mosaic backfill: complete candidates=1 generated=1 skipped=0 failed=0'
        in stdout
    )


# ── push_measurements + diagnose flags ─────────────────────────────────────


def test_backfill_pushes_measurements_before_mosaic_by_default(db, monkeypatch):
    """Default behaviour: measurement push runs before mosaic push for every
    candidate, so measurements added since the last regular sync are cloud-
    resident by the time the mosaic pusher runs its `m.cloud_id IS NOT NULL`
    filter."""
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')
    order: list[tuple[str, int]] = []

    def fake_measurements(client, obs_local_id):
        order.append(('measurements', obs_local_id))

    def fake_mosaic(client, obs_local_id, obs_cloud_id):
        order.append(('mosaic', obs_local_id))
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_measurements_for_observation', fake_measurements)
    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_mosaic)

    cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    # For each candidate: measurements first, then mosaic.
    assert order == [
        ('measurements', 1), ('mosaic', 1),
        ('measurements', 2), ('mosaic', 2),
    ]


def test_backfill_skips_measurement_push_when_disabled(db, monkeypatch):
    _insert_obs(db, cloud_id='1')
    calls = {'measurements': 0, 'mosaic': 0}

    def fake_measurements(client, obs_local_id):
        calls['measurements'] += 1

    def fake_mosaic(client, obs_local_id, obs_cloud_id):
        calls['mosaic'] += 1
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_measurements_for_observation', fake_measurements)
    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_mosaic)

    cloud_sync.backfill_public_spore_mosaics(_FakeClient(), push_measurements=False)

    assert calls == {'measurements': 0, 'mosaic': 1}


def test_backfill_measurement_push_error_does_not_stop_mosaic(db, monkeypatch, capfd):
    """A non-fatal measurement-push exception is logged and the mosaic
    pusher still runs for the same observation. Sync auth/temporary errors
    still propagate — covered in the dedicated abort tests."""
    _insert_obs(db, cloud_id='1')
    mosaic_calls: list[int] = []

    def failing_measurements(client, obs_local_id):
        raise RuntimeError('measurement push exploded')

    def fake_mosaic(client, obs_local_id, obs_cloud_id):
        mosaic_calls.append(obs_local_id)
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_measurements_for_observation', failing_measurements)
    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_mosaic)

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())

    assert mosaic_calls == [1]
    assert result['generated'] == 1
    stdout, _stderr = capfd.readouterr()
    assert 'measurement push failed' in stdout
    assert 'measurement push exploded' in stdout


def test_backfill_measurement_push_auth_error_aborts(db, monkeypatch):
    """Auth errors from the measurement push must abort the whole backfill —
    otherwise every remaining observation would just log the same auth
    failure over and over."""
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')

    class AuthError(Exception):
        pass

    def failing_measurements(client, obs_local_id):
        raise AuthError('token expired')

    monkeypatch.setattr(cloud_sync, '_push_measurements_for_observation', failing_measurements)
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: isinstance(exc, AuthError))
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: False)

    with pytest.raises(AuthError):
        cloud_sync.backfill_public_spore_mosaics(_FakeClient())


def test_backfill_diagnose_flag_calls_gate_diagnostic(db, monkeypatch):
    _insert_obs(db, cloud_id='719')
    calls: list[tuple[int, str]] = []

    def fake_diag(client, obs_local_id, obs_cloud_id=None, **kwargs):
        calls.append((obs_local_id, str(obs_cloud_id)))
        return {}

    monkeypatch.setattr(cloud_sync, 'diagnose_public_spore_mosaic_gates', fake_diag)
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    cloud_sync.backfill_public_spore_mosaics(_FakeClient(), diagnose=True)
    assert calls == [(1, '719')]


def test_backfill_diagnose_default_off(db, monkeypatch):
    _insert_obs(db, cloud_id='719')
    calls: list[int] = []

    monkeypatch.setattr(
        cloud_sync,
        'diagnose_public_spore_mosaic_gates',
        lambda *a, **kw: calls.append(1) or {},
    )
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    cloud_sync.backfill_public_spore_mosaics(_FakeClient())
    assert calls == []


# ── CLI smoke: --help parses ────────────────────────────────────────────────


def test_cli_help_runs_without_touching_db_or_cloud():
    """`--help` must not import cloud_sync or fail with anything DB-y.

    Running the module in a subprocess isolates env, argparse behavior, and
    avoids sharing state with other tests.
    """
    result = subprocess.run(
        [sys.executable, '-m', 'utils.cloud_spore_mosaic_backfill', '--help'],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert 'Generate/refresh public spore mosaics' in result.stdout
    assert '--observation-cloud-id' in result.stdout
    assert '--limit' in result.stdout
    assert '--no-push-measurements' in result.stdout
    assert '--diagnose' in result.stdout
