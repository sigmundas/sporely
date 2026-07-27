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
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            filepath TEXT,
            original_filepath TEXT,
            cloud_id TEXT,
            synced_at TEXT,
            image_type TEXT,
            sort_order INTEGER,
            micro_category TEXT,
            objective_name TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL,
            mount_medium TEXT,
            stain TEXT,
            sample_type TEXT,
            contrast TEXT,
            measure_color TEXT,
            crop_mode TEXT,
            notes TEXT,
            gps_source INTEGER,
            ai_crop_x1 REAL, ai_crop_y1 REAL,
            ai_crop_x2 REAL, ai_crop_y2 REAL,
            ai_crop_source_w INTEGER, ai_crop_source_h INTEGER,
            ai_crop_is_custom INTEGER
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER,
            cloud_id TEXT,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_image(db_path: Path, **kwargs) -> int:
    cols = list(kwargs.keys())
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            f"INSERT INTO images ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            [kwargs[c] for c in cols],
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _insert_measurement(db_path: Path, **kwargs) -> int:
    cols = list(kwargs.keys())
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            f"INSERT INTO spore_measurements ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            [kwargs[c] for c in cols],
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _select_image(db_path: Path, image_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute('SELECT * FROM images WHERE id = ?', (image_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


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
        'skipped_unchanged': 0,
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


# ── Metadata-only microscope image anchor helper ────────────────────────────


class _RecordingClient:
    """Records every mutating cloud call so we can assert what was (and
    wasn't) sent."""

    def __init__(self, *, user_id: str = 'user-uuid', existing_cloud_id: str | None = None,
                 post_id: str = 'cloud-img-42'):
        self.user_id = user_id
        self._existing = existing_cloud_id
        self._post_id = post_id
        self.calls: list[tuple[str, object]] = []

    def _observation_images_support_ai_crop(self):
        return True

    def _observation_images_support_ai_crop_custom(self):
        return True

    def _find_cloud_image(self, desktop_id: int) -> str | None:
        self.calls.append(('_find_cloud_image', desktop_id))
        return self._existing

    def _post(self, path: str, payload):
        self.calls.append(('_post', (path, payload)))
        return [{'id': self._post_id}]

    def _patch(self, path: str, payload):  # pragma: no cover — should not fire here
        self.calls.append(('_patch', (path, payload)))
        return None

    def upload_image_file(self, *a, **kw):  # pragma: no cover — must never be called
        self.calls.append(('upload_image_file', (a, kw)))
        raise AssertionError('upload_image_file must not be called for metadata-only rows')

    def _get_r2(self):  # pragma: no cover — must never be called
        raise AssertionError('R2 client must not be used for metadata-only rows')


def test_ensure_metadata_only_creates_row_with_null_storage_path(db, monkeypatch, capfd):
    """The helper posts a row with storage_path=None and image_type=microscope,
    never touches R2 / upload_image_file, and persists the returned cloud_id."""
    obs_local = _insert_obs(db, cloud_id='745', spore_data_visibility='public')
    image_id = _insert_image(
        db,
        observation_id=obs_local,
        filepath='/tmp/does-not-need-to-exist.jpg',
        image_type='microscope',
        cloud_id=None,
        sort_order=0,
        micro_category='spores',
        objective_name='100x',
        scale_microns_per_pixel=0.12,
        resample_scale_factor=1.0,
        mount_medium='water',
        stain='none',
        sample_type='fresh',
        contrast='brightfield',
        measure_color='#ff0',
        crop_mode='ai',
        notes='meta test',
        gps_source=0,
        ai_crop_x1=10.0, ai_crop_y1=20.0,
        ai_crop_x2=110.0, ai_crop_y2=120.0,
        ai_crop_source_w=4000, ai_crop_source_h=3000,
        ai_crop_is_custom=0,
    )
    _insert_measurement(db, image_id=image_id, length_um=10.0, width_um=5.0, measurement_type='manual')

    client = _RecordingClient(post_id='cloud-img-777')
    image_row = _select_image(db, image_id)

    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )

    assert result == 'cloud-img-777'
    posts = [c for c in client.calls if c[0] == '_post']
    assert len(posts) == 1
    path, payload = posts[0][1]
    assert path == 'observation_images'
    assert payload['storage_path'] is None
    assert payload['image_type'] == 'microscope'
    assert payload['observation_id'] == '745'
    assert payload['user_id'] == 'user-uuid'
    assert payload['desktop_id'] == image_id
    # Metadata fields carried through:
    for expected in ('sort_order', 'micro_category', 'objective_name',
                     'scale_microns_per_pixel', 'mount_medium', 'sample_type',
                     'contrast', 'crop_mode', 'notes'):
        assert expected in payload
    # gps_source coerced to boolean like push_image_metadata does.
    assert isinstance(payload['gps_source'], bool)
    # No upload paths hit.
    forbidden = [c for c in client.calls if c[0] in {'upload_image_file'}]
    assert forbidden == []

    # Local cloud_id persisted.
    stored = _select_image(db, image_id).get('cloud_id')
    assert stored == 'cloud-img-777'

    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic image metadata: create' in stdout
    assert 'storage_path=NULL' in stdout
    assert 'Mosaic image metadata: linked' in stdout
    assert f'cloud_image=cloud-img-777' in stdout


def test_ensure_metadata_only_short_circuits_when_row_already_linked(db, monkeypatch, capfd):
    obs_local = _insert_obs(db, cloud_id='745', spore_data_visibility='public')
    image_id = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/x.jpg',
        image_type='microscope', cloud_id='pre-existing-uuid',
    )
    _insert_measurement(
        db, image_id=image_id, length_um=10.0, width_um=5.0,
        measurement_type='manual',
    )
    image_row = _select_image(db, image_id)
    client = _RecordingClient(existing_cloud_id='pre-existing-uuid')

    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )

    assert result == 'pre-existing-uuid'
    # The non-null local id is validated remotely; no duplicate is written.
    assert client.calls == [('_find_cloud_image', image_id)]
    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic image metadata: linked' in stdout


def test_ensure_metadata_only_reuses_remote_row_by_desktop_id(db, monkeypatch, capfd):
    obs_local = _insert_obs(db, cloud_id='745', spore_data_visibility='public')
    image_id = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/x.jpg',
        image_type='microscope', cloud_id=None,
    )
    _insert_measurement(db, image_id=image_id, length_um=10.0, width_um=5.0, measurement_type='manual')
    image_row = _select_image(db, image_id)

    client = _RecordingClient(existing_cloud_id='remote-uuid-9')

    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )

    assert result == 'remote-uuid-9'
    # Lookup happened, no POST created a duplicate.
    assert ('_find_cloud_image', image_id) in client.calls
    posts = [c for c in client.calls if c[0] == '_post']
    assert posts == []
    # Local cloud_id updated.
    assert _select_image(db, image_id).get('cloud_id') == 'remote-uuid-9'
    stdout, _stderr = capfd.readouterr()
    assert '(validated)' in stdout


def test_ensure_metadata_only_skips_non_microscope(db, capfd):
    obs_local = _insert_obs(db, cloud_id='745')
    image_id = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/x.jpg',
        image_type='field', cloud_id=None,
    )
    _insert_measurement(db, image_id=image_id, length_um=10.0, width_um=5.0, measurement_type='manual')
    image_row = _select_image(db, image_id)

    client = _RecordingClient()
    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )
    assert result is None
    assert client.calls == []
    stdout, _stderr = capfd.readouterr()
    assert 'reason=not_microscope' in stdout


def test_ensure_metadata_only_skips_when_no_public_spore_measurements(db, capfd):
    obs_local = _insert_obs(db, cloud_id='745')
    image_id = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/x.jpg',
        image_type='microscope', cloud_id=None,
    )
    # Non-eligible measurement_type: excluded.
    _insert_measurement(
        db, image_id=image_id, length_um=10.0, width_um=5.0,
        measurement_type='cystidia',
    )
    image_row = _select_image(db, image_id)

    client = _RecordingClient()
    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )
    assert result is None
    posts = [c for c in client.calls if c[0] == '_post']
    assert posts == []
    stdout, _stderr = capfd.readouterr()
    assert 'reason=no_public_spore_measurements' in stdout


def test_ensure_metadata_only_missing_source_file_still_creates_row(db, capfd):
    obs_local = _insert_obs(db, cloud_id='745')
    image_id = _insert_image(
        db, observation_id=obs_local,
        filepath='/definitely/does/not/exist.jpg',
        image_type='microscope', cloud_id=None,
    )
    _insert_measurement(db, image_id=image_id, length_um=10.0, width_um=5.0, measurement_type='manual')
    image_row = _select_image(db, image_id)

    client = _RecordingClient(post_id='cloud-img-88')
    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        client, obs_local, '745', image_row,
    )

    # Missing source is a note, not a hard skip — the anchor row still
    # exists so its measurements reach public sporePoints.
    assert result == 'cloud-img-88'
    posts = [c for c in client.calls if c[0] == '_post']
    assert len(posts) == 1
    stdout, _stderr = capfd.readouterr()
    assert 'reason=missing_source_file' in stdout


def test_metadata_anchor_match_tolerates_cloud_float_rounding():
    payload = {
        'image_type': 'microscope',
        'desktop_id': 1280,
        'scale_microns_per_pixel': 0.05349373209020841,
    }
    remote = {
        'image_type': 'microscope',
        'desktop_id': 1280,
        'scale_microns_per_pixel': 0.0534937320902084,
        'storage_path': None,
        'original_storage_path': None,
        'deleted_at': None,
    }

    assert cloud_sync._remote_image_row_matches_anchor_payload(
        remote, payload, metadata_only=True,
    )


# ── Backfill integration with the helper ───────────────────────────────────


def test_backfill_calls_metadata_helper_before_measurement_push(db, monkeypatch):
    _insert_obs(db, cloud_id='745', spore_data_visibility='public')
    order: list[str] = []

    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda client, local_id, cloud_id: order.append('ensure') or {},
    )
    monkeypatch.setattr(
        cloud_sync,
        '_push_measurements_for_observation',
        lambda client, local_id: order.append('measurements'),
    )
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: order.append('mosaic') or cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    cloud_sync.backfill_public_spore_mosaics(_FakeClient())
    assert order == ['ensure', 'measurements', 'mosaic']


def test_backfill_no_ensure_image_metadata_skips_helper(db, monkeypatch):
    _insert_obs(db, cloud_id='745')
    calls: list[str] = []

    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: calls.append('ensure') or {},
    )
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    cloud_sync.backfill_public_spore_mosaics(_FakeClient(), ensure_image_metadata=False)
    assert calls == []


def test_backfill_metadata_helper_auth_error_aborts(db, monkeypatch):
    _insert_obs(db, cloud_id='1')
    _insert_obs(db, cloud_id='2')

    class AuthError(Exception):
        pass

    def failing(client, local_id, cloud_id):
        raise AuthError('token expired')

    monkeypatch.setattr(cloud_sync, '_ensure_metadata_only_microscope_images_for_observation', failing)
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: isinstance(exc, AuthError))
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: False)
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda *a, **kw: cloud_sync.MOSAIC_STATUS_GENERATED,
    )

    with pytest.raises(AuthError):
        cloud_sync.backfill_public_spore_mosaics(_FakeClient())


def test_backfill_metadata_helper_non_auth_failure_does_not_stop_mosaic(db, monkeypatch, capfd):
    _insert_obs(db, cloud_id='1')

    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('meta boom')),
    )
    calls: list[int] = []
    monkeypatch.setattr(
        cloud_sync,
        '_push_spore_mosaic_for_observation',
        lambda client, local_id, cloud_id: (calls.append(local_id) or cloud_sync.MOSAIC_STATUS_GENERATED),
    )

    result = cloud_sync.backfill_public_spore_mosaics(_FakeClient())
    assert calls == [1]
    assert result['generated'] == 1
    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic image metadata: observation failed' in stdout
    assert 'meta boom' in stdout


def test_ensure_wrapper_validates_linked_and_unlinked_microscope_images(db, monkeypatch, capfd):
    obs_local = _insert_obs(db, cloud_id='745')
    linked = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/a.jpg',
        image_type='microscope', cloud_id='already-linked',
    )
    unlinked = _insert_image(
        db, observation_id=obs_local, filepath='/tmp/b.jpg',
        image_type='microscope', cloud_id=None,
    )
    _insert_image(
        db, observation_id=obs_local, filepath='/tmp/c.jpg',
        image_type='field', cloud_id=None,
    )
    _insert_measurement(db, image_id=unlinked, length_um=9.0, width_um=4.0, measurement_type='manual')

    seen: list[int] = []

    def fake_helper(client, local_id, cloud_id, image_row, *, remote_images=None):
        seen.append(image_row['id'])
        return 'x'

    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_image_for_public_spores',
        fake_helper,
    )

    counts = cloud_sync._ensure_metadata_only_microscope_images_for_observation(
        _FakeClient(), obs_local, '745',
    )
    assert seen == [linked, unlinked]
    assert counts['considered'] == 2
    assert counts['ensured'] == 2


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
    assert '--no-ensure-image-metadata' in result.stdout
    assert '--diagnose' in result.stdout
