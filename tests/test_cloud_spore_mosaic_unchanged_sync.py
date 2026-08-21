"""Second-sync unchanged-observation verification for public spore mosaics.

Complements `test_cloud_spore_mosaic_signature.py` with the invariants
the brief spelled out under item 8:

* Repeated sync with zero local/remote change → every eligible mosaic
  returns ``MOSAIC_STATUS_SKIP_UNCHANGED``.
* No source ``Image.open`` call fires on the skip path.
* No bytes reach the media worker on the skip path.
* Signature unchanged but the remote mosaic row is missing → rebuild.
* Signature unchanged but remote tile rows are missing → rebuild.
* Successful rebuild persists the local signature only after upload +
  tile writes both complete.
* Failed upload does not poison the local signature.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from PIL import Image

from utils import cloud_spore_mosaic, cloud_sync


# ── Minimal fixture DB (mirrors the signature-test schema) ─────────────────


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "mosaic_unchanged.db"
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


def _write_source(path: Path, size=(200, 200)) -> None:
    Image.new("RGB", size, (120, 40, 40)).save(path, format="PNG")


def _seed_public_mosaic(db_path: Path, tmp_path: Path) -> tuple[int, str, Path]:
    conn = sqlite3.connect(db_path)
    obs_cur = conn.execute(
        "INSERT INTO observations (cloud_id, spore_data_visibility) VALUES (?, ?)",
        ("obs-cloud-7", "public"),
    )
    obs_local = int(obs_cur.lastrowid)
    src = tmp_path / "src.png"
    _write_source(src)
    img_cur = conn.execute(
        """
        INSERT INTO images (
            observation_id, filepath, cloud_id, image_type,
            scale_microns_per_pixel, resample_scale_factor
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (obs_local, str(src), "c-img-1", "microscope", 0.5, 1.0),
    )
    img_id = int(img_cur.lastrowid)
    conn.execute(
        """
        INSERT INTO spore_measurements (
            image_id, cloud_id, length_um, width_um,
            measurement_type, gallery_rotation,
            p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            img_id, "c-m-1", 10.0, 4.0,
            "manual", 0,
            10.0, 20.0, 30.0, 20.0,
            20.0, 10.0, 20.0, 30.0,
        ),
    )
    conn.commit()
    conn.close()
    return obs_local, "obs-cloud-7", src


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_images_dir", lambda: tmp_path)
    monkeypatch.setattr(cloud_sync, "direct_r2_runtime_available", lambda: False)
    return db_path


class _RecordingClient:
    def __init__(self, *, existing_mosaic=None, existing_tiles=None,
                 upload_raises: Exception | None = None):
        self.user_id = "user-uuid"
        self._existing_mosaic = existing_mosaic
        self._existing_tiles = existing_tiles
        self.upload_raises = upload_raises
        self.calls: list[tuple[str, object]] = []
        self.upload_count = 0

    def _get(self, path: str):
        self.calls.append(("_get", path))
        if path.startswith("spore_measurement_mosaics"):
            return self._existing_mosaic if self._existing_mosaic is not None else []
        if path.startswith("spore_measurement_mosaic_tiles"):
            return self._existing_tiles if self._existing_tiles is not None else []
        raise AssertionError(f"unexpected _get: {path!r}")

    def _post(self, path: str, payload):
        self.calls.append(("_post", (path, payload)))
        if path == "spore_measurement_mosaics":
            return [{"id": "mosaic-uuid-1"}]
        return None

    def _patch(self, path: str, payload):
        self.calls.append(("_patch", (path, payload)))
        return None

    def _delete(self, path: str):
        self.calls.append(("_delete", path))
        return None

    def _storage_remove(self, paths: list):
        self.calls.append(("_storage_remove", paths))
        return None

    def _get_media_worker(self):
        outer = self

        class _Worker:
            def put_bytes(self, *a, **kw):
                outer.upload_count += 1
                if outer.upload_raises is not None:
                    raise outer.upload_raises
                return {"key": "u/o/spore.webp"}

        return _Worker()


def _sig_for_seeded_row(db_path: Path, obs_local: int, src: Path) -> str:
    row = {
        "id": 1, "image_id": 1, "cloud_id": "c-m-1",
        "image_cloud_id": "c-img-1", "image_filepath": str(src),
        "p1_x": 10.0, "p1_y": 20.0, "p2_x": 30.0, "p2_y": 20.0,
        "p3_x": 20.0, "p3_y": 10.0, "p4_x": 20.0, "p4_y": 30.0,
        "length_um": 10.0, "width_um": 4.0,
        "measurement_type": "manual", "gallery_rotation": 0,
        "scale_microns_per_pixel": 0.5, "resample_scale_factor": 1.0,
    }
    return cloud_sync._local_spore_mosaic_signature(
        obs_local, [row], {"spore_data_visibility": "public"},
    )


def _persist_signature(db_path: Path, obs_local: int, sig: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE observations SET mosaic_signature = ? WHERE id = ?",
        (sig, obs_local),
    )
    conn.commit()
    conn.close()


def _read_signature(db_path: Path, obs_local: int) -> str | None:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT mosaic_signature FROM observations WHERE id = ?",
        (obs_local,),
    ).fetchone()
    conn.close()
    return None if row is None else row[0]


# ── The main item-8 assertion: repeated sync is byte-noop ──────────────────


def test_unchanged_second_sync_skips_render_upload_and_source_open(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    sig = _sig_for_seeded_row(db, obs_local, src)
    _persist_signature(db, obs_local, sig)

    # Guard that nothing opens the source file, nothing calls into the
    # renderer, and no bytes reach the worker.
    open_calls: list[Path] = []

    def spy_open(path):
        open_calls.append(Path(path))
        return cloud_spore_mosaic._open_source_image(path)

    build_calls: list = []

    def spy_build(*args, **kwargs):
        build_calls.append((args, kwargs))
        raise AssertionError(
            "build_spore_mosaic must not run on an unchanged-signature skip",
        )

    monkeypatch.setattr(cloud_spore_mosaic, "_open_source_image", spy_open)
    monkeypatch.setattr(cloud_spore_mosaic, "build_spore_mosaic", spy_build)

    client = _RecordingClient(
        existing_mosaic=[{"id": "mosaic-uuid-1", "storage_key": "u/o/spore.webp"}],
        existing_tiles=[{"measurement_id": "c-m-1"}],
    )

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )

    assert status == cloud_sync.MOSAIC_STATUS_SKIP_UNCHANGED
    # No source open, no build, no worker upload.
    assert open_calls == []
    assert build_calls == []
    assert client.upload_count == 0
    # No mutating REST call fired.
    assert not any(op in ("_post", "_patch", "_delete") for op, _ in client.calls)


def test_unchanged_signature_but_remote_mosaic_missing_triggers_rebuild(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    sig = _sig_for_seeded_row(db, obs_local, src)
    _persist_signature(db, obs_local, sig)

    # Remote mosaic row absent → the rebuild branch must run.
    client = _RecordingClient(existing_mosaic=[], existing_tiles=[])

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )

    assert status == cloud_sync.MOSAIC_STATUS_GENERATED
    assert client.upload_count == 1


def test_unchanged_signature_but_remote_tiles_missing_triggers_rebuild(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    sig = _sig_for_seeded_row(db, obs_local, src)
    _persist_signature(db, obs_local, sig)

    # Mosaic row present but zero tile rows → helper returns False and
    # the pusher rebuilds.
    client = _RecordingClient(
        existing_mosaic=[{"id": "mosaic-uuid-1", "storage_key": "u/o/spore.webp"}],
        existing_tiles=[],
    )

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )

    assert status == cloud_sync.MOSAIC_STATUS_GENERATED
    assert client.upload_count == 1


def test_successful_rebuild_persists_signature_only_after_all_writes(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    # No cached signature yet — first-time rebuild.
    assert _read_signature(db, obs_local) is None
    client = _RecordingClient(existing_mosaic=[], existing_tiles=[])

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )
    assert status == cloud_sync.MOSAIC_STATUS_GENERATED
    stored = _read_signature(db, obs_local)
    assert stored and len(stored) == 40


def test_failed_upload_does_not_poison_local_signature(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    # Ensure no prior signature.
    assert _read_signature(db, obs_local) is None
    # Media worker upload raises a non-auth, non-503 error — the pusher
    # should log + return FAIL_UPLOAD and leave the signature untouched.
    client = _RecordingClient(
        existing_mosaic=[], existing_tiles=[],
        upload_raises=RuntimeError("simulated R2 outage"),
    )

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )
    assert status == cloud_sync.MOSAIC_STATUS_FAIL_UPLOAD
    # Signature never persisted since the upload failed.
    assert _read_signature(db, obs_local) is None


def test_failed_tile_insert_does_not_poison_local_signature(
    db, tmp_path, monkeypatch,
):
    obs_local, obs_cloud, src = _seed_public_mosaic(db, tmp_path)
    assert _read_signature(db, obs_local) is None

    class _ExplodingClient(_RecordingClient):
        def _post(self, path, payload):
            self.calls.append(("_post", (path, payload)))
            if path == "spore_measurement_mosaic_tiles":
                raise RuntimeError("tile insert boom")
            if path == "spore_measurement_mosaics":
                return [{"id": "mosaic-uuid-1"}]
            return None

    client = _ExplodingClient(existing_mosaic=[], existing_tiles=[])

    status = cloud_sync._push_spore_mosaic_for_observation(
        client, obs_local, obs_cloud,
    )
    assert status == cloud_sync.MOSAIC_STATUS_FAIL_TILE_INSERT
    assert _read_signature(db, obs_local) is None
