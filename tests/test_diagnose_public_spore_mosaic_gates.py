"""Unit tests for `diagnose_public_spore_mosaic_gates` — the counter that
tells an operator exactly why measurements are or aren't reaching the
public sporePoints list.

We stage a mini SQLite fixture that mirrors the columns the diagnostic
touches (no more, no less), and swap `cloud_sync.get_connection` with
a factory that returns a connection to that DB. The remote checks are
covered by driving a fake client whose `_get` and `_rpc` return the
shapes PostgREST would return.
"""

from __future__ import annotations

import sqlite3

import pytest

from utils import cloud_sync


def _init_diag_db(tmp_path):
    db_path = tmp_path / "sporely_diag.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            cloud_id TEXT,
            image_type TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER,
            cloud_id TEXT,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            p1_x REAL, p1_y REAL,
            p2_x REAL, p2_y REAL,
            p3_x REAL, p3_y REAL,
            p4_x REAL, p4_y REAL
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_image(db_path, **kw) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cols = list(kw.keys())
        cur = conn.execute(
            f"INSERT INTO images ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            [kw[c] for c in cols],
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _insert_measurement(db_path, **kw) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cols = list(kw.keys())
        cur = conn.execute(
            f"INSERT INTO spore_measurements ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            [kw[c] for c in cols],
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


class _FakeClient:
    def __init__(self, *, user_id: str = 'user', gets: dict | None = None, rpc_returns=None):
        self.user_id = user_id
        self._gets = gets or {}
        self._rpc_returns = rpc_returns
        self.calls: list[tuple[str, object]] = []

    def _get(self, path: str):
        self.calls.append(('_get', path))
        for prefix, payload in self._gets.items():
            if path.startswith(prefix):
                return payload
        raise AssertionError(f'no stub _get for {path!r}')

    def _rpc(self, name: str, payload: dict | None = None):
        self.calls.append(('_rpc', (name, payload)))
        return self._rpc_returns


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_diag_db(tmp_path)
    monkeypatch.setattr(cloud_sync, 'get_connection', lambda: sqlite3.connect(db_path))
    return db_path


# ── Local gates ─────────────────────────────────────────────────────────────


def test_diagnose_counts_every_local_gate(db, capfd):
    obs_local = 42

    # image 1: microscope with cloud_id.
    img_ok = _insert_image(db, observation_id=obs_local, cloud_id='c-img-1', image_type='microscope')
    # image 2: field (should be excluded via image_type).
    img_field = _insert_image(db, observation_id=obs_local, cloud_id='c-img-2', image_type='field')
    # image 3: microscope, but no cloud_id.
    img_no_cloud = _insert_image(db, observation_id=obs_local, cloud_id=None, image_type='microscope')

    # 3 measurements on img_ok:
    #  - full geometry + cloud_id + all filters pass
    _insert_measurement(
        db, image_id=img_ok, cloud_id='c-m-1',
        length_um=10.0, width_um=4.0, measurement_type='manual',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10, p3_x=0, p3_y=6, p4_x=2, p4_y=6,
    )
    #  - full geometry but no measurement cloud_id
    _insert_measurement(
        db, image_id=img_ok, cloud_id=None,
        length_um=10.0, width_um=4.0, measurement_type='manual',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10, p3_x=0, p3_y=6, p4_x=2, p4_y=6,
    )
    #  - excluded by measurement_type
    _insert_measurement(
        db, image_id=img_ok, cloud_id='c-m-3',
        length_um=10.0, width_um=4.0, measurement_type='calibration',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10, p3_x=0, p3_y=6, p4_x=2, p4_y=6,
    )
    # 1 on img_field (field image; excluded by image_type but counts in `total_local`)
    _insert_measurement(
        db, image_id=img_field, cloud_id='c-m-4',
        length_um=10.0, width_um=4.0, measurement_type='spore',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10, p3_x=0, p3_y=6, p4_x=2, p4_y=6,
    )
    # 1 on img_no_cloud (measurement has p3/p4 but image has no cloud_id)
    _insert_measurement(
        db, image_id=img_no_cloud, cloud_id='c-m-5',
        length_um=10.0, width_um=4.0, measurement_type='manual',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10, p3_x=0, p3_y=6, p4_x=2, p4_y=6,
    )
    # 1 on img_ok with missing p3/p4
    _insert_measurement(
        db, image_id=img_ok, cloud_id='c-m-6',
        length_um=10.0, width_um=4.0, measurement_type='manual',
        p1_x=1, p1_y=2, p2_x=1, p2_y=10,
        p3_x=None, p3_y=None, p4_x=None, p4_y=None,
    )

    result = cloud_sync.diagnose_public_spore_mosaic_gates(
        None, obs_local_id=obs_local, obs_cloud_id=None,
    )

    assert result['obs_local_id'] == obs_local
    assert result['total_local'] == 6
    assert result['with_p1_p2'] == 6
    assert result['with_p1_p2_p3_p4'] == 5      # one measurement is missing p3/p4
    assert result['with_length_and_width_um'] == 6
    assert result['image_has_cloud_id'] == 5     # img_no_cloud has one measurement
    assert result['measurement_has_cloud_id'] == 5  # one measurement has cloud_id=None
    assert result['excluded_by_measurement_type'] == 1  # 'calibration'
    assert result['by_image_type'] == {'microscope': 5, 'field': 1}
    # pusher_would_select must satisfy ALL of:
    #   image_type=microscope, image.cloud_id, measurement.cloud_id,
    #   length_um + width_um, p1/p2 non-null, measurement_type in the allow list.
    # The pusher does NOT require p3/p4 — a measurement without p3/p4 still
    # renders as an oriented tile, it just skips the polygon overlay. So
    # measurements 1 and 6 both pass; measurement 3 is excluded by type,
    # 4 by image_type, 5 by image cloud_id, 2 by measurement cloud_id.
    assert result['pusher_would_select'] == 2

    stdout, _stderr = capfd.readouterr()
    assert f'Mosaic gate obs {obs_local}' in stdout
    assert 'total_local=6' in stdout
    assert 'pusher_would_select=2' in stdout


def test_diagnose_returns_zero_counts_for_unknown_observation(db):
    result = cloud_sync.diagnose_public_spore_mosaic_gates(
        None, obs_local_id=999, obs_cloud_id=None, log=False,
    )
    assert result['total_local'] == 0
    assert result['pusher_would_select'] == 0
    assert result['by_image_type'] == {}


# ── Remote gates ────────────────────────────────────────────────────────────


def test_diagnose_reports_remote_counts_and_public_rpc(db):
    obs_local = 7
    img = _insert_image(db, observation_id=obs_local, cloud_id='c-1', image_type='microscope')
    _insert_measurement(
        db, image_id=img, cloud_id='c-m-1',
        length_um=10.0, width_um=4.0, measurement_type='manual',
        p1_x=0, p1_y=0, p2_x=0, p2_y=10, p3_x=-2, p3_y=5, p4_x=2, p4_y=5,
    )
    remote_images = [
        {'id': 501, 'image_type': 'microscope', 'deleted_at': None, 'purged_at': None},
        {'id': 502, 'image_type': 'field', 'deleted_at': None, 'purged_at': None},
        {'id': 503, 'image_type': 'microscope', 'deleted_at': '2026-01-01', 'purged_at': None},
    ]
    remote_measurements = [
        {'id': 1001, 'measurement_type': 'manual'},
        {'id': 1002, 'measurement_type': 'spore'},
    ]
    fake_client = _FakeClient(
        gets={
            'observation_images?observation_id=eq.719': remote_images,
            'spore_measurements?image_id=in.': remote_measurements,
        },
        rpc_returns=[{
            'sporePoints': [{'id': '1001'}, {'id': '1002'}, {'id': '1003'}],
        }],
    )

    result = cloud_sync.diagnose_public_spore_mosaic_gates(
        fake_client, obs_local_id=obs_local, obs_cloud_id='719', log=False,
    )

    assert result['remote_images'] == 3
    # Only the non-deleted microscope image counts.
    assert result['remote_microscope_images'] == 1
    assert result['remote_measurements'] == 2
    assert result['public_rpc_sporePoints'] == 3
    # And the fake client saw the exact calls we care about.
    kinds = [call[0] for call in fake_client.calls]
    assert kinds == ['_get', '_get', '_rpc']


def test_diagnose_records_remote_errors_without_raising(db):
    """A network error on the remote side must not blow up the whole helper."""
    obs_local = 11
    _insert_image(db, observation_id=obs_local, cloud_id='c-1', image_type='microscope')

    class BrokenClient:
        user_id = 'u'

        def _get(self, path: str):
            raise RuntimeError('supabase 500')

        def _rpc(self, name: str, payload=None):
            raise RuntimeError('rpc down')

    result = cloud_sync.diagnose_public_spore_mosaic_gates(
        BrokenClient(), obs_local_id=obs_local, obs_cloud_id='42', log=False,
    )

    # Local gates still populated.
    assert result['total_local'] == 0
    # Remote errors captured, not raised.
    assert 'remote_images_error' in result
    assert 'public_rpc_error' in result


def test_diagnose_skips_remote_when_no_cloud_id(db):
    _insert_image(db, observation_id=1, cloud_id='c-1', image_type='microscope')
    fake_client = _FakeClient()
    result = cloud_sync.diagnose_public_spore_mosaic_gates(
        fake_client, obs_local_id=1, obs_cloud_id=None, log=False,
    )
    assert 'remote_images' not in result
    assert 'public_rpc_sporePoints' not in result
    assert fake_client.calls == []
