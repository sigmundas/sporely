from __future__ import annotations

import sqlite3

import pytest

from database import schema
from database.curated_reference_forks import copy_curated_bundle_to_personal_library, normalize_curated_bundle
from tests.test_curated_reference_forks import bundle_row
from utils.curated_reference_sync import pull_curated_reference_forks, push_curated_reference_forks


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    main_path = tmp_path / "mushrooms.db"
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: main_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db")
    schema.init_database()
    return reference_path


class Cloud:
    def __init__(self, user_id="user-a"):
        self.user_id = user_id
        self.rows = []

    def sync_reference_curated_fork(self, payload, expected):
        existing = next((row for row in self.rows if row["curated_measurement_set_id"] == payload["curated_measurement_set_id"] and row["bundle_revision"] == payload["bundle_revision"]), None)
        if existing:
            return {"status": "no_change" if all(existing[k] == v for k, v in payload.items()) else "conflict", "row": existing}
        row = {"user_id": self.user_id, **payload, "row_version": 1, "created_at": "2026-08-30T12:00:00Z", "updated_at": "2026-08-30T12:00:00Z"}
        self.rows.append(row)
        return {"status": "created", "row": row}

    def list_reference_curated_forks(self):
        return list(self.rows)

    def get_public_curated_reference_set(self, set_id, revision):
        return [bundle_row(revision=revision)]


class FailingCloud(Cloud):
    def sync_reference_curated_fork(self, payload, expected):
        raise RuntimeError("offline")


def test_provenance_sync_a_cloud_b_round_trip(isolated, monkeypatch, tmp_path):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    cloud = Cloud()
    with sqlite3.connect(isolated) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id=?,remote_identity_state='acknowledged',"
            "cloud_row_version=1,accepted_payload_json='{}',sync_status='clean'",
            (cloud.user_id,),
        )
        connection.commit()
    assert push_curated_reference_forks(cloud).pushed == 1
    assert push_curated_reference_forks(cloud).pushed == 0

    second_reference = tmp_path / "profile-b-reference.db"
    second_main = tmp_path / "profile-b-main.db"
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: second_reference)
    monkeypatch.setattr(schema, "get_database_path", lambda: second_main)
    schema.init_database()
    # The canonical work -> treatment -> set pull owns graph materialization.
    # Recreate its result here so this focused test exercises only the new
    # provenance phase and its ordering after graph reconciliation.
    bundle = normalize_curated_bundle(bundle_row())
    copied = copy_curated_bundle_to_personal_library(bundle)
    with sqlite3.connect(second_reference) as connection:
        connection.execute("DELETE FROM curated_reference_fork_cloud_sync_state")
        connection.execute("DELETE FROM curated_reference_forks")
        connection.execute("UPDATE reference_works SET id=? WHERE id=?", (fork.reference_work_id, copied.reference_work_id))
        connection.execute("UPDATE reference_taxon_treatments SET reference_work_id=?,id=? WHERE id=?", (fork.reference_work_id, fork.taxon_treatment_id, copied.taxon_treatment_id))
        connection.execute("UPDATE reference_measurement_sets SET taxon_treatment_id=?,id=? WHERE id=?", (fork.taxon_treatment_id, fork.reference_measurement_set_id, copied.reference_measurement_set_id))
        connection.commit()
    result = pull_curated_reference_forks(cloud)
    assert result.pulled == 1 and result.errors == () and result.conflicts == ()
    with sqlite3.connect(second_reference) as connection:
        row = connection.execute("SELECT curated_measurement_set_id,bundle_revision,reference_measurement_set_id FROM curated_reference_forks").fetchone()
    assert row == (fork.curated_measurement_set_id, fork.bundle_revision, fork.reference_measurement_set_id)


def test_pull_rejects_cross_account_row_without_local_write(isolated):
    cloud = Cloud("user-a")
    cloud.rows = [{"user_id": "user-b"}]
    result = pull_curated_reference_forks(cloud)
    assert result.pulled == 0 and result.errors
    with sqlite3.connect(isolated) as connection:
        assert connection.execute("SELECT count(*) FROM curated_reference_forks").fetchone()[0] == 0


def test_pull_validates_whole_feed_before_any_local_write(isolated):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    cloud = Cloud()
    with sqlite3.connect(isolated) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id=?,remote_identity_state='acknowledged',"
            "cloud_row_version=1,accepted_payload_json='{}',sync_status='clean'",
            (cloud.user_id,),
        )
    assert push_curated_reference_forks(cloud).pushed == 1
    cloud.rows.append({"user_id": "another-account"})
    with sqlite3.connect(isolated) as connection:
        connection.execute("DELETE FROM curated_reference_fork_cloud_sync_state")
        connection.execute("DELETE FROM curated_reference_forks")
    result = pull_curated_reference_forks(cloud)
    assert result.pulled == 0 and result.errors
    with sqlite3.connect(isolated) as connection:
        assert connection.execute("SELECT count(*) FROM curated_reference_forks").fetchone()[0] == 0


def test_push_transport_failure_is_reported_without_aborting_reference_sync(isolated):
    copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    cloud = FailingCloud()
    with sqlite3.connect(isolated) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id=?,remote_identity_state='acknowledged',"
            "cloud_row_version=1,accepted_payload_json='{}',sync_status='clean'",
            (cloud.user_id,),
        )
        connection.commit()
    result = push_curated_reference_forks(cloud)
    assert result.pushed == 0
    assert result.errors == ("curated fork 68000000-0000-4000-8000-000000006701@2: offline",)
