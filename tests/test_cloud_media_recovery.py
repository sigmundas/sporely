from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.recover_cloud_media import _confirmation_matches, main
from utils.cloud_media_audit import ReadCollection
from utils.cloud_media_recovery import (
    CloudRecoveryAdapter, RecoveryError, apply_recovery, build_recovery_plan,
    verify_recovery,
)


class FakeReader:
    user_id = "user"

    def __init__(self, cloud_images=(), *, storage=None):
        self.cloud_images = tuple(cloud_images)
        self.storage = dict(storage or {})

    @staticmethod
    def _collection(rows, requested=1):
        return ReadCollection(tuple(rows), requested, len(rows), 1, 1, True)

    def fetch_observations(self, ids):
        return self._collection([{"id": value, "spore_data_visibility": "public"} for value in ids], len(ids))

    def fetch_images(self, ids):
        return self._collection(self.cloud_images, len(ids))

    def fetch_measurements(self, ids):
        return self._collection([], len(ids))

    def check_storage(self, key):
        return self.storage.get(key, "not_applicable" if not key else "exists")


def make_db(tmp_path: Path, *, tombstone=False, excluded=False, target_type="microscope"):
    db = tmp_path / "db.sqlite"
    source1, source2 = tmp_path / "one.jpg", tmp_path / "two.jpg"
    source1.write_bytes(b"one")
    source2.write_bytes(b"two")
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT, genus TEXT, species TEXT,
            spore_data_visibility TEXT, synced_at TEXT);
        CREATE TABLE images (id INTEGER PRIMARY KEY, observation_id INTEGER, cloud_id TEXT,
            filepath TEXT, original_filepath TEXT, image_type TEXT, sort_order INTEGER,
            notes TEXT, source_role TEXT, file_purpose TEXT, synced_at TEXT);
        CREATE TABLE spore_measurements (id INTEGER PRIMARY KEY, image_id INTEGER,
            desktop_id INTEGER, length_um REAL, width_um REAL, measurement_type TEXT);
        CREATE TABLE image_tombstones (local_image_id INTEGER, deleted_cloud_id TEXT,
            deleted_at TEXT, delete_synced_at TEXT);
        CREATE TABLE settings (key TEXT, value TEXT);
    """)
    conn.execute("INSERT INTO observations VALUES (7, 'cloud-7', 'Mycena', 'haematopus', 'public', NULL)")
    conn.execute("INSERT INTO images VALUES (11,7,NULL,?,NULL,?,3,NULL,'local_canonical','source',NULL)", (str(source1), target_type))
    conn.execute("INSERT INTO images VALUES (12,7,'healthy-12',?,NULL,'field',0,NULL,'local_canonical','source',NULL)", (str(source2),))
    conn.execute("INSERT INTO spore_measurements VALUES (21,11,21,10,5,'manual')")
    conn.execute("INSERT INTO settings VALUES ('sporely_cloud_image_file_sig_7_11','abc123')")
    if tombstone:
        conn.execute("INSERT INTO image_tombstones VALUES (11,NULL,'2026-08-03T10:00:00Z',NULL)")
    if excluded:
        conn.execute("INSERT INTO settings VALUES ('artsobs_publish_excluded_image_ids_7','[11]')")
    conn.commit()
    conn.close()
    healthy = {"id": "healthy-12", "observation_id": "cloud-7", "desktop_id": 12,
               "image_type": "field", "sort_order": 0, "storage_path": "healthy-key", "deleted_at": None}
    return db, source1, healthy


def plan(tmp_path, **kwargs):
    db, source, healthy = make_db(tmp_path, **kwargs)
    reader = FakeReader([healthy], storage={"healthy-key": "exists"})
    result = build_recovery_plan(db, reader, 7, audited_observation_id=7, audited_target_ids=[11])
    return result, db, source, reader


def test_plan_selects_only_audited_missing_and_ignores_publication_exclusion(tmp_path):
    result, *_ = plan(tmp_path, excluded=True)
    assert [item.local_image_id for item in result.items] == [11]
    assert result.items[0].measurement_count == 1
    assert result.healthy_count == 1


def test_dry_run_planning_performs_no_writes(tmp_path):
    result, db, *_ = plan(tmp_path)
    conn = sqlite3.connect(db)
    before = conn.total_changes
    assert result.items[0].status == "ready_create"
    assert conn.execute("SELECT cloud_id FROM images WHERE id=11").fetchone()[0] is None
    assert conn.total_changes == before
    conn.close()


def test_exact_observation_required(tmp_path):
    db, _, healthy = make_db(tmp_path)
    with pytest.raises(RecoveryError, match="only permits"):
        build_recovery_plan(db, FakeReader([healthy]), 8, audited_observation_id=7, audited_target_ids=[11])


def test_apply_requires_exact_cloud_confirmation(capsys):
    assert main(["--observation-id", "704", "--apply"]) == 2
    assert "requires --confirm-cloud-observation" in capsys.readouterr().err
    assert _confirmation_matches("cloud-7", "cloud-7")
    assert not _confirmation_matches("cloud-8", "cloud-7")


def test_unreadable_and_tombstoned_sources_abort_before_writes(tmp_path):
    result, db, source, reader = plan(tmp_path)
    source.unlink()
    with pytest.raises(RecoveryError, match="unreadable"):
        build_recovery_plan(db, reader, 7, audited_observation_id=7, audited_target_ids=[11])
    other = tmp_path / "other"
    other.mkdir()
    db2, _, healthy2 = make_db(other, tombstone=True)
    with pytest.raises(RecoveryError, match="tombstone"):
        build_recovery_plan(db2, FakeReader([healthy2]), 7, audited_observation_id=7, audited_target_ids=[11])


def test_duplicate_or_ambiguous_rows_abort(tmp_path):
    db, _, healthy = make_db(tmp_path)
    duplicate = {"id": "dup", "observation_id": "cloud-7", "desktop_id": 11,
                 "image_type": "microscope", "sort_order": 3, "storage_path": "dup-key", "deleted_at": None}
    duplicate2 = dict(duplicate, id="dup2")
    with pytest.raises(RecoveryError, match="duplicate or ambiguous"):
        build_recovery_plan(db, FakeReader([healthy, duplicate, duplicate2]), 7,
                            audited_observation_id=7, audited_target_ids=[11])


class FakeCloud:
    def __init__(self, *, fail_upload=False, fail_create=False):
        self.events = []
        self.fail_upload = fail_upload
        self.fail_create = fail_create
        self.keys = []

    def recovery_storage_key(self, source, image, obs):
        return f"user/{obs}/recovery/{image['id']}_one.jpg"

    def create_row_with_storage(self, image, obs, key, upload_ref):
        self.events.append(("create", image["id"], key))
        if self.fail_create:
            raise RuntimeError("row creation failed")
        return "new-11"

    def upload_derivative(self, source, image, obs, cloud_id, *, storage_path=None):
        key = storage_path or "existing-key"
        self.events.append(("upload", image["id"], cloud_id, key))
        self.keys.append(key)
        if self.fail_upload:
            raise RuntimeError("upload failed")
        return key

    def attach_derivative(self, cloud_id, key): self.events.append(("attach", cloud_id))
    def derivative_exists(self, key): self.events.append(("verify", key)); return True
    def verify_row(self, cloud_id, obs, image_id, key): self.events.append(("verify_row", cloud_id, key))
    def upload_original_if_enabled(self, image, obs, cloud_id): self.events.append(("original_policy", cloud_id))
    def upload_measurement(self, measurement, cloud_id): self.events.append(("measurement", cloud_id)); return "m"
    def measurement_count(self, cloud_id): self.events.append(("measurement_count", cloud_id)); return 1


class FakeWriter:
    def __init__(self, *, fail=False): self.events = []; self.fail = fail
    def link_completed(self, obs, image, cloud, signature):
        self.events.append(("link", image, cloud))
        if self.fail:
            raise RuntimeError("local linking failed")


@pytest.mark.parametrize("target_type", ["field", "microscope"])
def test_ready_create_uploads_before_row_with_nonempty_path(tmp_path, target_type):
    db, _, healthy = make_db(tmp_path, target_type=target_type)
    recovery_plan = build_recovery_plan(
        db, FakeReader([healthy], storage={"healthy-key": "exists"}), 7,
        audited_observation_id=7, audited_target_ids=[11],
    )
    cloud, writer = FakeCloud(), FakeWriter()
    result = apply_recovery(recovery_plan, cloud, writer)
    assert result[0]["status"] == "recovered"
    upload = next(event for event in cloud.events if event[0] == "upload")
    create = next(event for event in cloud.events if event[0] == "create")
    assert upload[3]
    assert create[2] == upload[3]
    assert cloud.events.index(upload) < cloud.events.index(create)


def test_apply_orders_cloud_confirmation_before_local_link_and_measurements(tmp_path):
    recovery_plan, *_ = plan(tmp_path)
    cloud, writer = FakeCloud(), FakeWriter()
    result = apply_recovery(recovery_plan, cloud, writer)
    assert result[0]["status"] == "recovered"
    assert [event[0] for event in cloud.events] == ["upload", "verify", "create", "verify_row", "original_policy", "measurement", "measurement_count"]
    assert writer.events == [("link", 11, "new-11")]


def test_failed_upload_leaves_local_link_unchanged(tmp_path):
    recovery_plan, *_ = plan(tmp_path)
    cloud, writer = FakeCloud(fail_upload=True), FakeWriter()
    result = apply_recovery(recovery_plan, cloud, writer)
    assert result[0]["status"] == "failed"
    assert writer.events == []


def test_partial_success_reuses_existing_row_without_duplicate_creation(tmp_path):
    db, _, healthy = make_db(tmp_path)
    partial = {"id": "partial-11", "observation_id": "cloud-7", "desktop_id": 11,
               "image_type": "microscope", "sort_order": 3, "storage_path": None, "deleted_at": None}
    recovery_plan = build_recovery_plan(db, FakeReader([healthy, partial]), 7,
                                       audited_observation_id=7, audited_target_ids=[11])
    assert recovery_plan.items[0].status == "resume_existing_row"
    cloud, writer = FakeCloud(), FakeWriter()
    apply_recovery(recovery_plan, cloud, writer)
    assert not any(event[0] == "create" for event in cloud.events)
    assert cloud.events[0] == ("upload", 11, "partial-11", "existing-key")


def test_uploaded_object_is_retained_and_retry_key_is_stable_when_row_creation_fails(tmp_path):
    recovery_plan, *_ = plan(tmp_path)
    writer = FakeWriter()
    first = FakeCloud(fail_create=True)
    result = apply_recovery(recovery_plan, first, writer)
    assert result[0]["status"] == "failed"
    assert result[0]["partial_state"] == "derivative_uploaded_row_unconfirmed"
    assert writer.events == []
    second = FakeCloud()
    apply_recovery(recovery_plan, second, writer)
    assert first.keys == second.keys
    assert len(second.keys) == 1


def test_row_created_but_local_link_failed_is_reused_on_retry(tmp_path):
    recovery_plan, db, _, _ = plan(tmp_path)
    first = FakeCloud()
    result = apply_recovery(recovery_plan, first, FakeWriter(fail=True))
    assert result[0]["status"] == "failed"
    complete = {"id": "new-11", "observation_id": "cloud-7", "desktop_id": 11,
                "image_type": "microscope", "sort_order": 3,
                "storage_path": first.keys[0], "deleted_at": None}
    healthy = {"id": "healthy-12", "observation_id": "cloud-7", "desktop_id": 12,
               "image_type": "field", "sort_order": 0, "storage_path": "healthy-key", "deleted_at": None}
    retry_plan = build_recovery_plan(
        db, FakeReader([healthy, complete], storage={"healthy-key": "exists", first.keys[0]: "exists"}), 7,
        audited_observation_id=7, audited_target_ids=[11],
    )
    assert retry_plan.items[0].status == "already_complete"
    retry_cloud, writer = FakeCloud(), FakeWriter()
    apply_recovery(retry_plan, retry_cloud, writer)
    assert not any(event[0] in {"upload", "create"} for event in retry_cloud.events)
    assert writer.events == [("link", 11, "new-11")]


def test_already_complete_is_not_uploaded_twice_and_reconciles_link(tmp_path):
    db, _, healthy = make_db(tmp_path)
    complete = {"id": "complete-11", "observation_id": "cloud-7", "desktop_id": 11,
                "image_type": "microscope", "sort_order": 3, "storage_path": "complete-key", "deleted_at": None}
    recovery_plan = build_recovery_plan(db, FakeReader([healthy, complete], storage={"healthy-key": "exists", "complete-key": "exists"}), 7,
                                       audited_observation_id=7, audited_target_ids=[11])
    cloud, writer = FakeCloud(), FakeWriter()
    result = apply_recovery(recovery_plan, cloud, writer)
    assert result[0]["status"] == "already_complete"
    assert not any(event[0] in {"create", "upload", "attach"} for event in cloud.events)
    assert writer.events == [("link", 11, "complete-11")]


def test_original_upload_follows_existing_opt_in(monkeypatch, tmp_path):
    source = tmp_path / "source.jpg"
    source.write_bytes(b"source")
    events = []

    class Client:
        def upload_original_image_file(
            self,
            path,
            obs,
            image,
            upload_meta=None,
            *,
            observation_id=None,
            image_id=None,
            recovery_authorized=False,
        ):
            events.append((path, obs, image, recovery_authorized))
            return "original-key"

        def set_image_original_storage_path(self, image, key):
            events.append((image, key))

    adapter = object.__new__(CloudRecoveryAdapter)
    adapter.client = Client()
    image = {"id": 11, "observation_id": 7, "filepath": str(source),
             "source_role": "local_canonical", "file_purpose": "microscope"}
    monkeypatch.setattr("utils.cloud_media_recovery.is_full_resolution_original_sync_enabled", lambda: False)
    adapter.upload_original_if_enabled(image, "cloud-7", "cloud-11")
    assert events == []
    monkeypatch.setattr("utils.cloud_media_recovery.is_full_resolution_original_sync_enabled", lambda: True)
    adapter.upload_original_if_enabled(image, "cloud-7", "cloud-11")
    assert events == [
        (str(source), "cloud-7", "cloud-11", True),
        ("cloud-11", "original-key"),
    ]


@pytest.mark.parametrize("change", ["missing_row", "missing_object", "duplicate", "measurement"])
def test_final_verification_detects_required_failures(monkeypatch, tmp_path, change):
    row = {
        "local_image_id": 11, "cloud_image_id": "cloud-11", "cloud": {"id": "cloud-11"},
        "primary_state": "healthy_cloud_image", "derivative_status": "exists",
        "duplicate_cloud_ids": [], "flags": [], "local_measurement_count": 1,
        "cloud_measurement_count": 1,
    }
    if change == "missing_row": row["primary_state"] = "cloud_row_missing_local_file_available"
    if change == "missing_object": row["derivative_status"] = "missing"
    if change == "duplicate": row["flags"] = ["duplicate_desktop_id"]
    if change == "measurement": row["cloud_measurement_count"] = 0
    monkeypatch.setattr("utils.cloud_media_recovery.AUDITED_TARGET_IMAGE_IDS", (11,))
    monkeypatch.setattr("utils.cloud_media_recovery.load_local_inventory", lambda *a, **k: {})
    monkeypatch.setattr("utils.cloud_media_recovery.build_audit_report", lambda *a, **k: {"images": [row]})
    result = verify_recovery(tmp_path / "unused", object(), 7)
    assert not result["ok"] and result["failure_count"] == 1
