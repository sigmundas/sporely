"""Safety-critical tests for the media-upload policy.

Pins the invariants that stop background/Refresh sync from silently pushing
old local microscope photos to the cloud:

  * ``explain_pending_cloud_image_decision`` returns the right ``pending`` +
    ``reason`` value for each row shape (already-synced, excluded, cache row,
    missing file, duplicate path, microscope-no-measurements, etc.).
  * ``_pending_cloud_pushable_image_ids`` uses that predicate so the dirty
    scan and the upload collection stay in lock-step.
  * ``_mark_cloud_observations_dirty_for_pending_local_images`` is a strict
    no-op unless the caller explicitly opts into a media-upload pass.
  * Repeating a metadata-only sync does NOT re-dirty observations just
    because they contain local microscope rows with ``cloud_id`` still NULL.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from database import models
from utils import cloud_sync
from utils.cloud_sync import (
    PENDING_REASON_ALREADY_SYNCED,
    PENDING_REASON_CACHE_ROW,
    PENDING_REASON_DUPLICATE,
    PENDING_REASON_EXCLUDED,
    PENDING_REASON_GENERATED,
    PENDING_REASON_MICROSCOPE_NO_MEASUREMENTS,
    PENDING_REASON_MISSING_FILE,
    PENDING_REASON_PENDING_UPLOAD,
    PENDING_REASON_WRONG_TYPE,
    explain_pending_cloud_image_decision,
)


# ---------------------------------------------------------------------------
# Shared predicate — one row at a time
# ---------------------------------------------------------------------------


def _make_row(**overrides) -> dict:
    row = {
        "id": 1,
        "observation_id": 100,
        "image_type": "field",
        "cloud_id": None,
        "filepath": "",
        "original_filepath": "",
        "source_role": "",
        "file_purpose": "",
        "notes": "",
        "sort_order": 0,
    }
    row.update(overrides)
    return row


def _existing_file(tmp_path: Path, name: str = "img.webp") -> str:
    p = tmp_path / name
    p.write_bytes(b"webp")
    return str(p)


def test_pending_when_new_field_image_with_existing_file(tmp_path):
    row = _make_row(id=1, image_type="field", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert decision == {"pending": True, "reason": PENDING_REASON_PENDING_UPLOAD}


def test_skipped_when_row_already_has_cloud_id(tmp_path):
    row = _make_row(cloud_id="cloud-abc", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert not decision["pending"]
    assert decision["reason"] == PENDING_REASON_ALREADY_SYNCED


def test_skipped_when_image_type_is_neither_field_nor_microscope(tmp_path):
    row = _make_row(image_type="raw", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert not decision["pending"]
    assert decision["reason"] == PENDING_REASON_WRONG_TYPE


def test_skipped_when_generated_cloud_image_by_negative_desktop_id(tmp_path):
    row = _make_row(id=-5, filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert not decision["pending"]
    # `_is_generated_cloud_image` returns True for negative desktop_id,
    # which is checked before the id > 0 guard.
    assert decision["reason"] in {PENDING_REASON_GENERATED, PENDING_REASON_WRONG_TYPE}


def test_skipped_when_user_excluded_the_image(tmp_path):
    row = _make_row(id=42, filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids={42}
    )
    assert not decision["pending"]
    assert decision["reason"] == PENDING_REASON_EXCLUDED


def test_skipped_when_local_file_is_missing():
    row = _make_row(filepath="/tmp/does/not/exist.webp")
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert not decision["pending"]
    assert decision["reason"] == PENDING_REASON_MISSING_FILE


def test_skipped_when_path_is_duplicate(tmp_path):
    path = _existing_file(tmp_path)
    seen: set[str] = set()
    first = explain_pending_cloud_image_decision(
        _make_row(id=1, filepath=path), seen_paths=seen, excluded_ids=set()
    )
    second = explain_pending_cloud_image_decision(
        _make_row(id=2, filepath=path), seen_paths=seen, excluded_ids=set()
    )
    assert first["pending"] is True
    assert second == {"pending": False, "reason": PENDING_REASON_DUPLICATE}


def test_microscope_without_measurements_is_not_pending_by_default(tmp_path):
    row = _make_row(id=7, image_type="microscope", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row,
        seen_paths=set(),
        excluded_ids=set(),
        image_measurement_counts={},
    )
    assert not decision["pending"]
    assert decision["reason"] == PENDING_REASON_MICROSCOPE_NO_MEASUREMENTS


def test_microscope_with_measurements_is_pending(tmp_path):
    row = _make_row(id=7, image_type="microscope", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row,
        seen_paths=set(),
        excluded_ids=set(),
        image_measurement_counts={7: 3},
    )
    assert decision == {"pending": True, "reason": PENDING_REASON_PENDING_UPLOAD}


def test_microscope_with_explicit_selection_is_pending_even_without_measurements(tmp_path):
    row = _make_row(id=7, image_type="microscope", filepath=_existing_file(tmp_path))
    decision = explain_pending_cloud_image_decision(
        row,
        seen_paths=set(),
        excluded_ids=set(),
        image_measurement_counts={},
        explicit_media_upload_selection={7},
    )
    assert decision == {"pending": True, "reason": PENDING_REASON_PENDING_UPLOAD}


def test_cache_row_without_cloud_id_is_pending_for_repair():
    """Cloud-cache rows that lost their cloud_id are broken state — sync must
    still see them as pending so the metadata patch can repair the link. They
    bypass the file-existence check because the local bytes may not have been
    materialized yet."""
    row = _make_row(
        id=99,
        image_type="field",
        cloud_id=None,
        filepath="",
        source_role="cloud_recovery_cache",
        file_purpose="cache",
    )
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert decision == {"pending": True, "reason": PENDING_REASON_PENDING_UPLOAD}


def test_cache_row_with_cloud_id_is_already_synced():
    row = _make_row(
        id=99,
        image_type="field",
        cloud_id="cloud-abc",
        source_role="cloud_recovery_cache",
        file_purpose="cache",
    )
    decision = explain_pending_cloud_image_decision(
        row, seen_paths=set(), excluded_ids=set()
    )
    assert decision == {"pending": False, "reason": PENDING_REASON_ALREADY_SYNCED}


# ---------------------------------------------------------------------------
# Dirty-scan gating: metadata-only sync must be a no-op
# ---------------------------------------------------------------------------


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(tmp_path):
    db_path = tmp_path / "policy.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            observation_id INTEGER,
            image_type TEXT,
            cloud_id TEXT,
            sort_order INTEGER,
            notes TEXT,
            source_role TEXT,
            file_purpose TEXT,
            filepath TEXT,
            original_filepath TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY,
            image_id INTEGER
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def test_metadata_only_sync_does_not_dirty_observations_with_pending_local_microscope(tmp_path, monkeypatch):
    """The exact regression from the field: a fresh install has 22 observations
    with old microscope images (cloud_id NULL, no measurements). Refresh must
    NOT mark them dirty."""
    db_path = _init_db(tmp_path)
    conn = _connect(db_path)
    conn.execute("INSERT INTO observations (id, cloud_id, sync_status) VALUES (500, 'cloud-500', 'synced')")
    # A local microscope image with a real file but no measurements — the
    # exact shape the user reported as unwantedly re-uploaded.
    fpath = tmp_path / "old_micro.webp"
    fpath.write_bytes(b"webp")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, "
        "notes, source_role, file_purpose, filepath) "
        "VALUES (900, 500, 'microscope', NULL, 0, '', 'converted_local', 'microscope', ?)",
        (str(fpath),),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    # Default (metadata-only) call: no-op.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    status = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 500"
    ).fetchone()[0]
    assert status == "synced", (
        "Metadata-only sync must not dirty an observation just because it has "
        "a local microscope row with cloud_id NULL"
    )


def test_repeated_metadata_only_sync_does_not_flip_synced_to_dirty(tmp_path, monkeypatch):
    """Simulate the actual user complaint: sync, sync, sync — status must stay 'synced'."""
    db_path = _init_db(tmp_path)
    conn = _connect(db_path)
    conn.execute("INSERT INTO observations (id, cloud_id, sync_status) VALUES (500, 'cloud-500', 'synced')")
    fpath = tmp_path / "old_micro.webp"
    fpath.write_bytes(b"webp")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, "
        "notes, source_role, file_purpose, filepath) "
        "VALUES (900, 500, 'microscope', NULL, 0, '', 'converted_local', 'microscope', ?)",
        (str(fpath),),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    for _ in range(3):
        cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    status = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 500"
    ).fetchone()[0]
    assert status == "synced"


def test_explicit_media_upload_does_re_dirty_when_microscope_has_measurements(tmp_path, monkeypatch):
    """Under explicit media-upload mode, a microscope image WITH measurements
    still triggers re-dirty — that's the mosaic-anchor use case where bytes
    need to reach cloud."""
    db_path = _init_db(tmp_path)
    conn = _connect(db_path)
    conn.execute("INSERT INTO observations (id, cloud_id, sync_status) VALUES (500, 'cloud-500', 'synced')")
    fpath = tmp_path / "measured_micro.webp"
    fpath.write_bytes(b"webp")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, "
        "notes, source_role, file_purpose, filepath) "
        "VALUES (901, 500, 'microscope', NULL, 0, '', 'converted_local', 'microscope', ?)",
        (str(fpath),),
    )
    conn.execute("INSERT INTO spore_measurements (id, image_id) VALUES (1, 901)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )

    status = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 500"
    ).fetchone()[0]
    assert status == "dirty"


def test_explicit_media_upload_leaves_measurement_less_microscope_alone(tmp_path, monkeypatch):
    """Even under explicit media-upload mode, a microscope image with NO
    measurements and no explicit selection stays local — that's the whole
    point of the tightened push policy."""
    db_path = _init_db(tmp_path)
    conn = _connect(db_path)
    conn.execute("INSERT INTO observations (id, cloud_id, sync_status) VALUES (500, 'cloud-500', 'synced')")
    fpath = tmp_path / "old_micro.webp"
    fpath.write_bytes(b"webp")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, "
        "notes, source_role, file_purpose, filepath) "
        "VALUES (902, 500, 'microscope', NULL, 0, '', 'converted_local', 'microscope', ?)",
        (str(fpath),),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )

    status = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 500"
    ).fetchone()[0]
    assert status == "synced"


def test_explicit_media_upload_dirties_when_user_selected_the_microscope(tmp_path, monkeypatch):
    """The opt-in path: user explicitly picked a microscope image for upload."""
    db_path = _init_db(tmp_path)
    conn = _connect(db_path)
    conn.execute("INSERT INTO observations (id, cloud_id, sync_status) VALUES (500, 'cloud-500', 'synced')")
    fpath = tmp_path / "old_micro.webp"
    fpath.write_bytes(b"webp")
    conn.execute(
        "INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, "
        "notes, source_role, file_purpose, filepath) "
        "VALUES (903, 500, 'microscope', NULL, 0, '', 'converted_local', 'microscope', ?)",
        (str(fpath),),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
        explicit_media_upload_selection={903},
    )

    status = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 500"
    ).fetchone()[0]
    assert status == "dirty"


# ---------------------------------------------------------------------------
# UI default: _start_cloud_sync must default to metadata-only
# ---------------------------------------------------------------------------


def test_start_cloud_sync_defaults_to_metadata_only():
    """The one-liner regression: `_start_cloud_sync` was defaulting to
    sync_images=True, which meant every Refresh became a media-upload."""
    pytest.importorskip("PySide6.QtCore")
    from ui.observations_tab import ObservationsTab

    import inspect

    sig = inspect.signature(ObservationsTab._start_cloud_sync)
    assert sig.parameters["sync_images"].default is False, (
        "_start_cloud_sync must default sync_images=False so background sync "
        "and Refresh stay metadata-only unless the user explicitly opts in"
    )
