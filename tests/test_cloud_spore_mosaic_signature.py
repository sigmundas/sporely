"""Tests for the sync-time mosaic signature.

The signature is a compact hash over the set of local inputs that
determine both the rendered mosaic bytes and the remote tile manifest.
It lets normal sync skip the expensive Pillow / WebP / R2 / tile-rewrite
work when nothing that matters has changed.

These tests focus on:

* `_local_spore_mosaic_signature` — determinism + input coverage.
* `_push_spore_mosaic_for_observation` — signature skip vs rebuild
  decisions, plus signature persistence on success only.
* `sync_all`-level metadata-anchor wiring (public vs private, error
  isolation).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from utils import cloud_sync
from utils.cloud_spore_mosaic import MOSAIC_PIPELINE_VERSION


# ── DB fixture ──────────────────────────────────────────────────────────────


def _init_signature_db(tmp_path: Path) -> Path:
    """Minimal schema with just the columns the pusher touches. Mirrors the
    other pusher tests but adds `mosaic_signature` so the pusher can
    persist and read it back."""
    db_path = tmp_path / "sporely_sig.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            spore_data_visibility TEXT,
            mosaic_signature TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            filepath TEXT,
            cloud_id TEXT,
            image_type TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER,
            cloud_id TEXT,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            gallery_rotation INTEGER,
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


def _insert_obs(db_path: Path, **kw) -> int:
    kw.setdefault('spore_data_visibility', 'public')
    conn = sqlite3.connect(db_path)
    try:
        cols = list(kw.keys())
        cur = conn.execute(
            f"INSERT INTO observations ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            [kw[c] for c in cols],
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _insert_image(db_path: Path, **kw) -> int:
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


def _insert_meas(db_path: Path, **kw) -> int:
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


def _read_signature(db_path: Path, obs_id: int) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            'SELECT mosaic_signature FROM observations WHERE id = ?',
            (obs_id,),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row[0]


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_signature_db(tmp_path)
    monkeypatch.setattr(cloud_sync, 'get_connection', lambda: sqlite3.connect(db_path))
    # Prevent get_images_dir()-> user dir hits during signature resolution.
    monkeypatch.setattr(cloud_sync, 'get_images_dir', lambda: tmp_path)
    return db_path


# ── Signature helper ────────────────────────────────────────────────────────


def _touch(path: Path, content: bytes = b"pixels") -> Path:
    path.write_bytes(content)
    return path


def _row(*, mid=1, image_id=10, image_cloud_id='c-img',
         cloud_id='c-m', filepath='src.jpg',
         p1_x=10.0, p1_y=20.0, p2_x=30.0, p2_y=20.0,
         p3_x=20.0, p3_y=10.0, p4_x=20.0, p4_y=30.0,
         length_um=10.0, width_um=4.0,
         measurement_type='manual', gallery_rotation=0,
         scale_microns_per_pixel=0.5,
         resample_scale_factor=1.0) -> dict:
    return {
        'id': mid,
        'image_id': image_id,
        'cloud_id': cloud_id,
        'image_cloud_id': image_cloud_id,
        'image_filepath': filepath,
        'p1_x': p1_x, 'p1_y': p1_y,
        'p2_x': p2_x, 'p2_y': p2_y,
        'p3_x': p3_x, 'p3_y': p3_y,
        'p4_x': p4_x, 'p4_y': p4_y,
        'length_um': length_um,
        'width_um': width_um,
        'measurement_type': measurement_type,
        'gallery_rotation': gallery_rotation,
        'scale_microns_per_pixel': scale_microns_per_pixel,
        'resample_scale_factor': resample_scale_factor,
    }


def _obs(**kw) -> dict:
    return {'spore_data_visibility': kw.get('spore_data_visibility', 'public')}


def test_signature_deterministic_across_calls(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    rows = [_row(filepath=str(src))]
    obs = _obs()
    a = cloud_sync._local_spore_mosaic_signature(1, rows, obs)
    b = cloud_sync._local_spore_mosaic_signature(1, rows, obs)
    assert a == b
    assert len(a) == 40  # sha1 hex


def test_signature_changes_when_measurement_added(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    grown = cloud_sync._local_spore_mosaic_signature(
        1,
        [_row(filepath=str(src)), _row(mid=2, cloud_id='c-m-2', filepath=str(src))],
        obs,
    )
    assert base != grown


def test_signature_changes_when_measurement_removed(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    two = cloud_sync._local_spore_mosaic_signature(
        1,
        [_row(filepath=str(src)), _row(mid=2, cloud_id='c-m-2', filepath=str(src))],
        obs,
    )
    one = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    assert two != one


@pytest.mark.parametrize('field', ['p1_x', 'p1_y', 'p2_x', 'p2_y', 'p3_x', 'p3_y', 'p4_x', 'p4_y'])
def test_signature_changes_when_polygon_endpoint_changes(tmp_path, db, field):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    changed = _row(filepath=str(src))
    changed[field] = changed[field] + 1.0
    changed_sig = cloud_sync._local_spore_mosaic_signature(1, [changed], obs)
    assert base != changed_sig


@pytest.mark.parametrize('field,new', [
    ('length_um', 99.9),
    ('width_um', 88.8),
    ('measurement_type', 'spore'),
    ('gallery_rotation', 90),
])
def test_signature_changes_when_scalar_measurement_field_changes(tmp_path, db, field, new):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    changed = _row(filepath=str(src))
    changed[field] = new
    changed_sig = cloud_sync._local_spore_mosaic_signature(1, [changed], obs)
    assert base != changed_sig


def test_signature_changes_when_measurement_cloud_id_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    changed = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), cloud_id='c-m-XXX')], obs,
    )
    assert base != changed


def test_signature_changes_when_image_cloud_id_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    changed = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), image_cloud_id='c-img-XXX')], obs,
    )
    assert base != changed


def test_signature_changes_when_source_path_changes(tmp_path, db):
    a = _touch(tmp_path / 'a.jpg', b'aa')
    b = _touch(tmp_path / 'b.jpg', b'bb')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(a))], obs)
    changed = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(b))], obs)
    assert base != changed


def test_signature_changes_when_source_mtime_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    import os, time
    time.sleep(0.01)
    os.utime(src, (time.time() + 60, time.time() + 60))
    changed = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    assert base != changed


def test_signature_changes_when_source_size_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg', b'aa')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    src.write_bytes(b'aaaaaaaaaaa')
    changed = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    assert base != changed


def test_signature_changes_when_scale_microns_per_pixel_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), scale_microns_per_pixel=0.5)], obs,
    )
    changed = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), scale_microns_per_pixel=0.25)], obs,
    )
    assert base != changed


def test_signature_changes_when_resample_scale_factor_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), resample_scale_factor=1.0)], obs,
    )
    changed = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src), resample_scale_factor=2.0)], obs,
    )
    assert base != changed


def test_signature_changes_when_spore_data_visibility_changes(tmp_path, db):
    src = _touch(tmp_path / 'a.jpg')
    base = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src))], {'spore_data_visibility': 'public'},
    )
    changed = cloud_sync._local_spore_mosaic_signature(
        1, [_row(filepath=str(src))], {'spore_data_visibility': 'private'},
    )
    assert base != changed


def test_signature_changes_when_pipeline_version_bumps(tmp_path, db, monkeypatch):
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    base = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    monkeypatch.setattr('utils.cloud_spore_mosaic.MOSAIC_PIPELINE_VERSION', MOSAIC_PIPELINE_VERSION + 1)
    bumped = cloud_sync._local_spore_mosaic_signature(1, [_row(filepath=str(src))], obs)
    assert base != bumped


def test_signature_input_order_does_not_matter(tmp_path, db):
    """Signature is sorted by local id — passing rows in reverse order
    still produces the same digest as the pusher's ORDER BY m.id result."""
    src = _touch(tmp_path / 'a.jpg')
    obs = _obs()
    rows_asc = [
        _row(mid=1, cloud_id='c-1', filepath=str(src)),
        _row(mid=2, cloud_id='c-2', filepath=str(src)),
    ]
    rows_desc = list(reversed(rows_asc))
    assert (
        cloud_sync._local_spore_mosaic_signature(1, rows_asc, obs)
        == cloud_sync._local_spore_mosaic_signature(1, rows_desc, obs)
    )


# ── Pusher integration: skip vs rebuild + signature persistence ─────────────


class _StubClient:
    """Records the mosaic-row and tile calls the pusher makes."""

    def __init__(self, *, user_id='user-uuid', existing_mosaic=None, existing_tiles=None):
        self.user_id = user_id
        self._existing_mosaic = existing_mosaic  # list[dict] or None
        self._existing_tiles = existing_tiles    # list[dict] or None
        self.calls: list[tuple[str, object]] = []

    def _get(self, path: str):
        self.calls.append(('_get', path))
        if path.startswith('spore_measurement_mosaics'):
            return self._existing_mosaic if self._existing_mosaic is not None else []
        if path.startswith('spore_measurement_mosaic_tiles'):
            return self._existing_tiles if self._existing_tiles is not None else []
        raise AssertionError(f'unexpected _get: {path!r}')

    def _post(self, path: str, payload):
        self.calls.append(('_post', (path, payload)))
        if path == 'spore_measurement_mosaics':
            return [{'id': 'mosaic-uuid-1'}]
        return None

    def _patch(self, path: str, payload):
        self.calls.append(('_patch', (path, payload)))
        return None

    def _delete(self, path: str):
        self.calls.append(('_delete', path))
        return None


def _seed_full_observation(db_path: Path, tmp_path: Path) -> tuple[int, str, Path]:
    """Insert an obs + microscope image + one eligible measurement.

    Returns (obs_local_id, obs_cloud_id, source_path).
    """
    obs_local = _insert_obs(db_path, cloud_id='719', spore_data_visibility='public')
    src = _touch(tmp_path / 'source.jpg')
    img_id = _insert_image(
        db_path,
        observation_id=obs_local,
        filepath=str(src),
        cloud_id='c-img-1',
        image_type='microscope',
        scale_microns_per_pixel=0.5,
        resample_scale_factor=1.0,
    )
    _insert_meas(
        db_path,
        image_id=img_id,
        cloud_id='c-m-1',
        length_um=10.0,
        width_um=4.0,
        measurement_type='manual',
        gallery_rotation=0,
        p1_x=10.0, p1_y=20.0,
        p2_x=30.0, p2_y=20.0,
        p3_x=20.0, p3_y=10.0,
        p4_x=20.0, p4_y=30.0,
    )
    return obs_local, '719', src


def _mock_render(monkeypatch, *, tiles_bytes=b'MOSAIC-BYTES'):
    """Stub `sources_from_measurement_rows` + `build_spore_mosaic` so the
    pusher runs without Pillow. Returns the manifest object the pusher
    will see."""
    from utils import cloud_spore_mosaic

    class _Tile:
        def __init__(self, i):
            self.measurement_id = 100 + i
            self.cloud_measurement_id = f'c-m-{i}'
            self.cloud_image_id = 'c-img-1'
            self.x_px = 0
            self.y_px = 0
            self.w_px = 320
            self.h_px = 320
            self.overlay_json = None
            self.diagnostics = {}

    class _Manifest:
        def __init__(self, tiles_bytes):
            self.image_bytes = tiles_bytes
            self.content_type = 'image/webp'
            self.width_px = 320
            self.height_px = 320
            self.tile_size_px = 320
            self.tile_width_px = 320
            self.tile_height_px = 320
            self.common_crop_width_px = 320
            self.common_crop_height_px = 320
            self.common_crop_width_um = 20.0
            self.common_crop_height_um = 20.0
            self.tiles = [_Tile(1)]
            self.skipped = []

    def fake_sources(rows, image_dir):
        return (['x'], [])

    def fake_build(sources, tile_size_px):
        return _Manifest(tiles_bytes)

    monkeypatch.setattr(cloud_spore_mosaic, 'sources_from_measurement_rows', fake_sources)
    monkeypatch.setattr(cloud_spore_mosaic, 'build_spore_mosaic', fake_build)
    # The R2 direct runtime is off in tests; ensure the media worker path is
    # what the pusher hits (or short-circuit both paths).
    monkeypatch.setattr(cloud_sync, 'direct_r2_runtime_available', lambda: False)

    class _Worker:
        def put_bytes(self, *a, **kw):
            return {'key': 'stubbed/key'}

    def fake_get_media_worker(self):
        return _Worker()

    # Attach to the client class via method injection is awkward; the stub
    # client already exposes ._get_media_worker via monkeypatch below.


def test_pusher_skips_when_signature_matches_and_remote_row_exists(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    # Pre-populate the signature to match what the helper will compute.
    conn = sqlite3.connect(db)
    try:
        rows = [
            {
                'id': 1, 'image_id': 1, 'cloud_id': 'c-m-1',
                'image_cloud_id': 'c-img-1', 'image_filepath': str(src),
                'p1_x': 10.0, 'p1_y': 20.0, 'p2_x': 30.0, 'p2_y': 20.0,
                'p3_x': 20.0, 'p3_y': 10.0, 'p4_x': 20.0, 'p4_y': 30.0,
                'length_um': 10.0, 'width_um': 4.0,
                'measurement_type': 'manual', 'gallery_rotation': 0,
                'scale_microns_per_pixel': 0.5, 'resample_scale_factor': 1.0,
            }
        ]
        expected_sig = cloud_sync._local_spore_mosaic_signature(
            obs_local, rows, {'spore_data_visibility': 'public'},
        )
        conn.execute(
            'UPDATE observations SET mosaic_signature = ? WHERE id = ?',
            (expected_sig, obs_local),
        )
        conn.commit()
    finally:
        conn.close()

    client = _StubClient(
        existing_mosaic=[{'id': 'mosaic-uuid-1', 'storage_key': 'u/o/spore.webp'}],
        existing_tiles=[{'measurement_id': 'c-m-1'}],
    )

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_SKIP_UNCHANGED
    # No upload/mosaic write path was hit — only the presence probes.
    assert not any(c[0] == '_post' for c in client.calls)
    assert not any(c[0] == '_patch' for c in client.calls)
    assert not any(c[0] == '_delete' for c in client.calls)


def test_pusher_rebuilds_when_signature_matches_but_remote_row_missing(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    # Pre-populate the signature.
    from utils import cloud_sync as cs

    fake_rows = [
        {
            'id': 1, 'image_id': 1, 'cloud_id': 'c-m-1',
            'image_cloud_id': 'c-img-1', 'image_filepath': str(src),
            'p1_x': 10.0, 'p1_y': 20.0, 'p2_x': 30.0, 'p2_y': 20.0,
            'p3_x': 20.0, 'p3_y': 10.0, 'p4_x': 20.0, 'p4_y': 30.0,
            'length_um': 10.0, 'width_um': 4.0,
            'measurement_type': 'manual', 'gallery_rotation': 0,
            'scale_microns_per_pixel': 0.5, 'resample_scale_factor': 1.0,
        }
    ]
    expected_sig = cs._local_spore_mosaic_signature(
        obs_local, fake_rows, {'spore_data_visibility': 'public'},
    )
    cs._store_local_mosaic_signature(obs_local, expected_sig)

    # Remote mosaic row absent → rebuild path must run.
    client = _StubClient(existing_mosaic=[], existing_tiles=[])
    # Provide the media worker for the upload leg.
    monkeypatch.setattr(client, '_get_media_worker', lambda: type('W', (), {'put_bytes': lambda self, *a, **k: {'key': 'k'}})(), raising=False)

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_GENERATED


def test_pusher_rebuilds_when_signature_missing(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    client = _StubClient(existing_mosaic=[], existing_tiles=[])
    monkeypatch.setattr(
        client, '_get_media_worker',
        lambda: type('W', (), {'put_bytes': lambda self, *a, **k: {'key': 'k'}})(),
        raising=False,
    )

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_GENERATED
    # Signature persisted after successful tile rewrite.
    stored = _read_signature(db, obs_local)
    assert stored and len(stored) == 40


def test_pusher_does_not_store_signature_on_upload_failure(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    class _BadWorker:
        def put_bytes(self, *a, **kw):
            raise RuntimeError('R2 down')

    client = _StubClient(existing_mosaic=[], existing_tiles=[])
    monkeypatch.setattr(client, '_get_media_worker', lambda: _BadWorker(), raising=False)

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_FAIL_UPLOAD
    assert _read_signature(db, obs_local) is None


def test_pusher_does_not_store_signature_on_tile_insert_failure(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    class _BadTilesClient(_StubClient):
        def _post(self, path, payload):
            self.calls.append(('_post', (path, payload)))
            if path == 'spore_measurement_mosaic_tiles':
                raise RuntimeError('tile insert boom')
            if path == 'spore_measurement_mosaics':
                return [{'id': 'mosaic-uuid-1'}]
            return None

    client = _BadTilesClient(existing_mosaic=[], existing_tiles=[])
    monkeypatch.setattr(
        client, '_get_media_worker',
        lambda: type('W', (), {'put_bytes': lambda self, *a, **k: {'key': 'k'}})(),
        raising=False,
    )

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_FAIL_TILE_INSERT
    assert _read_signature(db, obs_local) is None


def test_pusher_stores_new_signature_when_content_changes(tmp_path, db, monkeypatch):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    # Seed a stale signature that no longer matches.
    cloud_sync._store_local_mosaic_signature(obs_local, 'stale-sha1')

    client = _StubClient(existing_mosaic=[], existing_tiles=[])
    monkeypatch.setattr(
        client, '_get_media_worker',
        lambda: type('W', (), {'put_bytes': lambda self, *a, **k: {'key': 'k'}})(),
        raising=False,
    )

    status = cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    assert status == cloud_sync.MOSAIC_STATUS_GENERATED
    stored = _read_signature(db, obs_local)
    assert stored is not None
    assert stored != 'stale-sha1'
    assert len(stored) == 40


def test_pusher_signature_skip_log_line(tmp_path, db, monkeypatch, capfd):
    obs_local, obs_cloud, src = _seed_full_observation(db, tmp_path)
    _mock_render(monkeypatch)

    rows = [
        {
            'id': 1, 'image_id': 1, 'cloud_id': 'c-m-1',
            'image_cloud_id': 'c-img-1', 'image_filepath': str(src),
            'p1_x': 10.0, 'p1_y': 20.0, 'p2_x': 30.0, 'p2_y': 20.0,
            'p3_x': 20.0, 'p3_y': 10.0, 'p4_x': 20.0, 'p4_y': 30.0,
            'length_um': 10.0, 'width_um': 4.0,
            'measurement_type': 'manual', 'gallery_rotation': 0,
            'scale_microns_per_pixel': 0.5, 'resample_scale_factor': 1.0,
        }
    ]
    expected_sig = cloud_sync._local_spore_mosaic_signature(
        obs_local, rows, {'spore_data_visibility': 'public'},
    )
    cloud_sync._store_local_mosaic_signature(obs_local, expected_sig)

    client = _StubClient(
        existing_mosaic=[{'id': 'mosaic-uuid-1', 'storage_key': 'u/o/spore.webp'}],
        existing_tiles=[{'measurement_id': 'c-m-1'}],
    )
    cloud_sync._push_spore_mosaic_for_observation(client, obs_local, obs_cloud)

    stdout, _stderr = capfd.readouterr()
    assert f'Mosaic skip obs {obs_local}: signature unchanged' in stdout


# ── Normal-sync metadata anchor helper ─────────────────────────────────────


def test_ensure_metadata_anchors_calls_helper_for_public_observation(monkeypatch):
    calls: list[tuple] = []

    def fake(client, local_id, cloud_id):
        calls.append((local_id, cloud_id))
        return {}

    monkeypatch.setattr(cloud_sync, '_ensure_metadata_only_microscope_images_for_observation', fake)
    cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
        object(), {'spore_data_visibility': 'public'}, 42, 'cloud-42',
    )
    assert calls == [(42, 'cloud-42')]


def test_ensure_metadata_anchors_skips_when_visibility_private(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: calls.append(a),
    )
    cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
        object(), {'spore_data_visibility': 'private'}, 42, 'cloud-42',
    )
    assert calls == []


def test_ensure_metadata_anchors_skips_when_no_cloud_id(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: calls.append(a),
    )
    cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
        object(), {'spore_data_visibility': 'public'}, 42, '',
    )
    assert calls == []


def test_ensure_metadata_anchors_propagates_auth_error(monkeypatch):
    class AuthError(Exception):
        pass

    def boom(*a, **kw):
        raise AuthError('token')

    monkeypatch.setattr(
        cloud_sync, '_ensure_metadata_only_microscope_images_for_observation', boom,
    )
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: isinstance(exc, AuthError))
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: False)

    with pytest.raises(AuthError):
        cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
            object(), {'spore_data_visibility': 'public'}, 42, 'cloud-42',
        )


def test_ensure_metadata_anchors_propagates_temporary_error(monkeypatch):
    class Temp(Exception):
        pass

    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: (_ for _ in ()).throw(Temp('503')),
    )
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: False)
    monkeypatch.setattr(
        cloud_sync,
        'is_cloud_temporary_unavailable_error',
        lambda exc: isinstance(exc, Temp),
    )

    with pytest.raises(Temp):
        cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
            object(), {'spore_data_visibility': 'public'}, 42, 'cloud-42',
        )


def test_ensure_metadata_anchors_swallows_other_errors_and_logs(monkeypatch, capfd):
    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('meta blew up')),
    )
    monkeypatch.setattr(cloud_sync, 'is_cloud_auth_error', lambda exc: False)
    monkeypatch.setattr(cloud_sync, 'is_cloud_temporary_unavailable_error', lambda exc: False)

    cloud_sync._ensure_metadata_anchors_for_public_spore_observation(
        object(), {'spore_data_visibility': 'public'}, 42, 'cloud-42',
    )
    stdout, _stderr = capfd.readouterr()
    assert 'Mosaic image metadata: observation failed' in stdout
    assert 'meta blew up' in stdout


def test_backfill_bypasses_signature_and_rebuilds(tmp_path, db, monkeypatch):
    """Backfill clears the local signature before dispatching so operators
    can force a rebuild even when local rows are unchanged."""
    obs_local, obs_cloud, _src = _seed_full_observation(db, tmp_path)
    cloud_sync._store_local_mosaic_signature(obs_local, 'some-stale-sig')

    calls: list[str] = []

    def fake_push(client, local_id, cloud_id):
        calls.append(cloud_id)
        # If the guard were still in effect, this pusher wouldn't run.
        assert cloud_sync._load_local_mosaic_signature(local_id) == ''
        return cloud_sync.MOSAIC_STATUS_GENERATED

    monkeypatch.setattr(cloud_sync, '_push_spore_mosaic_for_observation', fake_push)
    monkeypatch.setattr(cloud_sync, '_push_measurements_for_observation', lambda *a, **kw: None)
    monkeypatch.setattr(
        cloud_sync,
        '_ensure_metadata_only_microscope_images_for_observation',
        lambda *a, **kw: {},
    )

    result = cloud_sync.backfill_public_spore_mosaics(type('C', (), {'user_id': 'u'})())
    assert calls == [obs_cloud]
    assert result['generated'] == 1
