import importlib.util
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from utils import cloud_media_audit as audit
from utils import cloud_sync


FIXED_TIME = "2026-08-05T12:00:00+00:00"


def _cli_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_cloud_media.py"
    spec = importlib.util.spec_from_file_location("audit_cloud_media_cli", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class FakeReader:
    user_id = "user-1"

    def __init__(self, *, images=(), observations=(), measurements=(), statuses=None, complete=True):
        self.images = tuple(images)
        self.observations = tuple(observations or ({"id": "cloud-obs-1"},))
        self.measurements = tuple(measurements)
        self.statuses = dict(statuses or {})
        self.complete = complete
        self.storage_calls = []

    def _collection(self, rows, requested):
        return audit.ReadCollection(tuple(rows), len(set(requested)), len(rows), 1 if requested else 0, 1 if requested else 0, self.complete)

    def fetch_observations(self, ids):
        return self._collection([row for row in self.observations if row["id"] in ids], ids)

    def fetch_images(self, ids):
        return self._collection([row for row in self.images if row["observation_id"] in ids], ids)

    def fetch_measurements(self, ids):
        return self._collection([row for row in self.measurements if row["image_id"] in ids], ids)

    def check_storage(self, path):
        self.storage_calls.append(path)
        return self.statuses.get(path, audit.STORAGE_EXISTS)


def _db(tmp_path: Path, *, observation_date="2020-05-01", observation_synced="2026-08-04T12:00:00Z") -> Path:
    path = tmp_path / "audit.sqlite"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY, cloud_id TEXT, date TEXT, genus TEXT,
                species TEXT, common_name TEXT, species_guess TEXT,
                sync_status TEXT, synced_at TEXT, visibility TEXT, is_draft INTEGER,
                spore_data_visibility TEXT, latitude REAL, longitude REAL
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY, observation_id INTEGER, cloud_id TEXT,
                image_type TEXT, sort_order INTEGER, filepath TEXT,
                original_filepath TEXT, source_role TEXT, file_purpose TEXT, synced_at TEXT
            );
            CREATE TABLE image_tombstones (
                id INTEGER PRIMARY KEY, local_image_id INTEGER, deleted_cloud_id TEXT,
                deleted_at TEXT, delete_synced_at TEXT
            );
            CREATE TABLE spore_measurements (
                id INTEGER PRIMARY KEY, image_id INTEGER, length_um REAL,
                width_um REAL, measurement_type TEXT
            );
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        conn.execute(
            "INSERT INTO observations VALUES (1,'cloud-obs-1',?,'Mycena','haematopus',"
            "'Burgundydrop Bonnet','Mycena haematopus','synced',?,'private',0,'public',59.91,10.75)",
            (observation_date, observation_synced),
        )
    return path


def _insert_image(db, *, image_id=11, cloud_id="cloud-image-11", image_type="field", sort_order=0,
                  filepath=None, original_filepath=None, synced_at="2026-08-04T12:00:00Z"):
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO images VALUES (?,1,?,?,?,?,?,'local_canonical',?,?)",
            (image_id, cloud_id, image_type, sort_order, str(filepath) if filepath else None,
             str(original_filepath) if original_filepath else None,
             "microscope" if image_type == "microscope" else "field", synced_at),
        )


def _cloud_image(**changes):
    row = {
        "id": "cloud-image-11", "desktop_id": 11, "observation_id": "cloud-obs-1",
        "image_type": "field", "sort_order": 0, "storage_path": "media/11.webp",
        "original_storage_path": None, "deleted_at": None, "purged_at": None,
        "original_filename": "field.jpg", "upload_mode": "full", "upload_variant": "clean",
    }
    row.update(changes)
    return row


def _inventory(db):
    return audit.load_local_inventory(db, audit.AuditFilters())


def _report(db, reader, **kwargs):
    return audit.build_audit_report(_inventory(db), reader, generated_at=FIXED_TIME, **kwargs)


def test_get_read_only_never_refreshes_or_persists_credentials(monkeypatch):
    client = cloud_sync.SporelyCloudClient("fixed-token", "user-1", "stored-refresh")

    class Response:
        ok = True
        status_code = 200
        text = ""
        content = b"[]"
        def json(self): return []

    monkeypatch.setattr(client._s, "request", lambda *args, **kwargs: Response())
    monkeypatch.setattr(client, "_refresh_session_if_possible", lambda: pytest.fail("refresh reached"))
    monkeypatch.setattr(client, "save_credentials", lambda *args, **kwargs: pytest.fail("save reached"))

    reader = audit.ReadOnlyCloudAuditReader(client)
    result = reader.fetch_observations(["cloud-obs-1"])
    assert result.complete is True


def test_expired_token_fails_closed_without_refresh(monkeypatch):
    client = cloud_sync.SporelyCloudClient("expired", "user-1", "stored-refresh")

    class Response:
        ok = False
        status_code = 401
        text = "JWT expired"
        content = b""

    monkeypatch.setattr(client._s, "request", lambda *args, **kwargs: Response())
    monkeypatch.setattr(client, "_refresh_session_if_possible", lambda: pytest.fail("refresh reached"))
    monkeypatch.setattr(client, "save_credentials", lambda *args, **kwargs: pytest.fail("save reached"))
    with pytest.raises(cloud_sync.CloudReauthRequiredError, match="authentication expired"):
        audit.ReadOnlyCloudAuditReader(client).fetch_observations(["cloud-obs-1"])


def test_reader_collects_multiple_batches_and_pages_deterministically():
    all_ids = [f"id-{number:03d}" for number in range(125)]

    class Client:
        user_id = "user-1"
        def __init__(self): self.calls = []
        def get_read_only(self, path):
            self.calls.append(path)
            query = parse_qs(urlsplit("?" + path.split("?", 1)[1]).query)
            filter_name = next(name for name in ("id", "observation_id", "image_id") if name in query)
            requested = query[filter_name][0].removeprefix("in.(").removesuffix(")").split(",")
            offset, limit = int(query["offset"][0]), int(query["limit"][0])
            rows = [
                {"id": f"row-{value}", filter_name: value}
                if filter_name != "id" else {"id": value}
                for value in requested
            ]
            return rows[offset:offset + limit]

    client = Client()
    result = audit.ReadOnlyCloudAuditReader(client, batch_size=50, page_size=20).fetch_observations(all_ids)
    assert result.returned_rows == 125
    assert result.batches == 3
    assert result.pages == 8
    assert [row["id"] for row in result.rows] == all_ids
    assert all("limit=20" in call and "offset=" in call for call in client.calls)
    image_result = audit.ReadOnlyCloudAuditReader(client, batch_size=50, page_size=20).fetch_images(all_ids)
    measurement_result = audit.ReadOnlyCloudAuditReader(client, batch_size=50, page_size=20).fetch_measurements(all_ids)
    assert image_result.returned_rows == measurement_result.returned_rows == 125


def test_incomplete_fetch_aborts_before_classification(tmp_path):
    db = _db(tmp_path); _insert_image(db)
    with pytest.raises(audit.CloudInventoryIncompleteError, match="classification aborted"):
        _report(db, FakeReader(images=[_cloud_image()], complete=False))


def test_field_null_storage_is_not_legitimate_anchor(tmp_path):
    db = _db(tmp_path)
    source = tmp_path / "field.jpg"; source.write_bytes(b"x")
    _insert_image(db, filepath=source)
    record = _report(db, FakeReader(images=[_cloud_image(storage_path=None)]))["images"][0]
    assert record["primary_state"] == "suspicious_metadata_only_anchor"  # incident sync evidence is stronger

    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE images SET cloud_id=NULL, synced_at=NULL")
        conn.execute("UPDATE observations SET synced_at=NULL")
    record = _report(db, FakeReader(images=[_cloud_image(storage_path=None)]))["images"][0]
    assert record["primary_state"] == "active_cloud_row_missing_storage_path"


@pytest.mark.parametrize(
    "length,width,kind,eligible",
    [(10, 5, None, True), (10, 5, "", True), (10, 5, "manual", True),
     (10, 5, "spore", True), (10, 5, "spores", True), (10, 5, "auto", False),
     (10, 5, "MANUAL", True), (10, 5, " ", False),
     (None, 5, "manual", False), (10, None, "manual", False)],
)
def test_anchor_eligibility_exactly_matches_shared_production_predicate(tmp_path, length, width, kind, eligible):
    db = _db(tmp_path, observation_synced=None)
    _insert_image(db, cloud_id=None, image_type="microscope", synced_at=None)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO spore_measurements VALUES (1,11,?,?,?)", (length, width, kind))
    cloud = _cloud_image(storage_path=None, image_type="microscope")
    record = _report(db, FakeReader(images=[cloud]))["images"][0]
    assert cloud_sync.measurement_qualifies_for_public_spore_anchor({
        "length_um": length, "width_um": width, "measurement_type": kind,
    }) is eligible
    assert record["qualifying_local_measurement_count"] == int(eligible)
    assert record["primary_state"] == (
        "legitimate_metadata_only_anchor" if eligible else "active_cloud_row_missing_storage_path"
    )


def test_old_observation_synced_during_incident_is_flagged_but_date_alone_is_not(tmp_path):
    db = _db(tmp_path, observation_date="2018-01-01", observation_synced="2026-08-04T10:00:00Z")
    _insert_image(db, image_type="microscope", filepath=tmp_path / "missing.jpg", synced_at=None)
    source = tmp_path / "missing.jpg"; source.write_bytes(b"x")
    record = _report(db, FakeReader(images=[_cloud_image(storage_path=None, image_type="microscope")]))["images"][0]
    assert "synced_during_incident_window" in record["evidence_flags"]
    assert record["primary_state"] == "suspicious_metadata_only_anchor"

    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE observations SET date='2026-08-04', synced_at='2026-08-02T10:00:00Z'")
    record = _report(db, FakeReader(images=[_cloud_image(storage_path=None, image_type="microscope")]))["images"][0]
    assert "synced_during_incident_window" not in record["evidence_flags"]


def test_empty_and_observation_signatures_are_weak_evidence_only(tmp_path):
    db = _db(tmp_path, observation_synced=None)
    source = tmp_path / "micro.jpg"; source.write_bytes(b"x")
    _insert_image(db, cloud_id=None, image_type="microscope", filepath=source, synced_at=None)
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO settings VALUES ('sporely_cloud_image_file_sig_1_11','')")
        conn.execute("INSERT INTO settings VALUES ('sporely_cloud_local_media_sig_obs_1','obs-sig')")
    record = _report(db, FakeReader(images=[_cloud_image(storage_path=None, image_type="microscope")]))["images"][0]
    assert record["signature_key_exists"] is True
    assert record["signature_value_present"] is False
    assert record["signature_value_empty"] is True
    assert "observation_level_signature" in record["evidence_flags"]
    assert record["metadata_only_suspicious"] is False


def test_no_storage_check_is_not_unverifiable_and_never_calls_worker(tmp_path):
    db = _db(tmp_path); _insert_image(db)
    reader = FakeReader(images=[_cloud_image(original_storage_path="media/original.jpg")])
    report = _report(db, reader, check_storage=False)
    record = report["images"][0]
    assert record["primary_state"] == "healthy_cloud_image"
    assert record["derivative_status"] == audit.STORAGE_NOT_CHECKED
    assert record["original_status"] == audit.STORAGE_NOT_CHECKED
    assert reader.storage_calls == []
    assert report["storage_verification_skipped"] is True


def test_attempted_network_failure_is_unverifiable_not_missing(tmp_path):
    db = _db(tmp_path); _insert_image(db)
    record = _report(db, FakeReader(
        images=[_cloud_image()], statuses={"media/11.webp": audit.STORAGE_UNAVAILABLE},
    ))["images"][0]
    assert record["primary_state"] == "unable_to_verify_storage"
    assert "cloud_derivative_missing" not in record["flags"]


def test_unmatched_cloud_row_checks_derivative_and_original(tmp_path):
    db = _db(tmp_path)
    reader = FakeReader(images=[_cloud_image(id="orphan", desktop_id=999, original_storage_path="media/original.jpg")])
    record = _report(db, reader)["images"][0]
    assert sorted(reader.storage_calls) == ["media/11.webp", "media/original.jpg"]
    assert record["derivative_status"] == audit.STORAGE_EXISTS
    assert record["original_status"] == audit.STORAGE_EXISTS


def test_duplicate_desktop_id_rows_are_surfaced_without_arbitrary_match(tmp_path):
    db = _db(tmp_path); _insert_image(db, cloud_id=None, synced_at=None)
    rows = [_cloud_image(id="duplicate-a"), _cloud_image(id="duplicate-b")]
    records = _report(db, FakeReader(images=rows))["images"]
    local = next(row for row in records if row["local_image_id"] == 11)
    assert local["match_method"] is None
    assert local["primary_state"] == "possible_duplicate"
    assert local["duplicate_cloud_ids"] == ["duplicate-a", "duplicate-b"]


def test_filename_fallback_is_never_authoritative_and_multiple_candidates_are_duplicates(tmp_path):
    db = _db(tmp_path, observation_synced=None)
    source = tmp_path / "field.jpg"; source.write_bytes(b"x")
    _insert_image(db, cloud_id=None, filepath=source, synced_at=None)
    rows = [
        _cloud_image(id="fallback-a", desktop_id=None),
        _cloud_image(id="fallback-b", desktop_id=None),
    ]
    local = next(row for row in _report(db, FakeReader(images=rows))["images"] if row["local_image_id"] == 11)
    assert local["match_method"] is None
    assert local["possible_match_ids"] == ["fallback-a", "fallback-b"]
    assert local["primary_state"] == "possible_duplicate"


def test_non_missing_table_sqlite_errors_propagate():
    class Connection:
        def execute(self, *args, **kwargs):
            raise sqlite3.OperationalError("database disk image is malformed")
    with pytest.raises(sqlite3.OperationalError, match="malformed"):
        audit._optional_rows(Connection(), "settings", "SELECT 1", (), [])


def test_report_write_refuses_overwrite_and_is_atomic(tmp_path, monkeypatch):
    cli = _cli_module()
    destination = tmp_path / "audit.json"
    destination.write_text("existing", encoding="utf-8")
    with pytest.raises(FileExistsError):
        cli.write_report_atomic(destination, "new")
    assert destination.read_text(encoding="utf-8") == "existing"

    replacements = []
    real_replace = os.replace
    monkeypatch.setattr(cli.os, "replace", lambda source, target: (replacements.append((Path(source), Path(target))), real_replace(source, target))[1])
    cli.write_report_atomic(destination, "new", force=True)
    assert destination.read_text(encoding="utf-8") == "new"
    assert replacements and replacements[0][0].parent == destination.parent
    assert not replacements[0][0].exists()
    if os.name != "nt":
        assert destination.stat().st_mode & 0o777 == 0o600


def test_since_validation_rejects_malformed_date():
    assert audit.validate_since("2026-08-03") == "2026-08-03"
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        audit.validate_since("03/08/2026")


def test_json_csv_deterministic_expanded_and_private_fields_excluded(tmp_path):
    db = _db(tmp_path); source = tmp_path / "field.jpg"; source.write_bytes(b"abc")
    _insert_image(db, filepath=source)
    cloud = _cloud_image(access_token="secret", latitude=59.91, longitude=10.75, purged_at=None)
    first = _report(db, FakeReader(images=[cloud]))
    second = _report(db, FakeReader(images=[dict(cloud)]))
    assert audit.report_json(first) == audit.report_json(second)
    assert audit.report_csv(first) == audit.report_csv(second)
    rendered = audit.report_json(first)
    assert "secret" not in rendered and "59.91" not in rendered and "10.75" not in rendered
    header = audit.report_csv(first).splitlines()[0]
    for field in ("filepath_readable", "filepath_size", "purged_at", "qualifying_local_measurement_count", "evidence_flags"):
        assert field in header
    assert json.loads(rendered)["cloud_inventory_complete"] is True
