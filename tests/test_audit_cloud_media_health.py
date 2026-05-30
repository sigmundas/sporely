import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from PIL import Image

from database import schema
from tools import audit_cloud_media_health as audit
from utils.r2_storage import media_variant_key


def _create_test_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                folder_path TEXT,
                sync_status TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT,
                original_filepath TEXT,
                sort_order INTEGER,
                image_type TEXT,
                micro_category TEXT,
                source_role TEXT,
                file_purpose TEXT,
                original_mime_type TEXT,
                working_mime_type TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.commit()
    finally:
        conn.close()


def _seed_local_image(
    db_path: Path,
    *,
    image_id: int,
    observation_id: int,
    filepath: Path | None,
    original_filepath: Path | None = None,
    cloud_id: str | None = None,
    sort_order: int | None = None,
    image_type: str = "field",
    micro_category: str | None = None,
    source_role: str | None = "local_canonical",
    file_purpose: str | None = "field",
    original_mime_type: str | None = "image/jpeg",
    working_mime_type: str | None = "image/jpeg",
    notes: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO observations (id, cloud_id, folder_path, sync_status)
            VALUES (?, ?, ?, ?)
            """,
            (observation_id, "606", "/tmp/observation", "synced"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, sort_order,
                image_type, micro_category, source_role, file_purpose,
                original_mime_type, working_mime_type, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                image_id,
                observation_id,
                cloud_id,
                str(filepath) if filepath else None,
                str(original_filepath) if original_filepath else None,
                sort_order,
                image_type,
                micro_category,
                source_role,
                file_purpose,
                original_mime_type,
                working_mime_type,
                notes,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _make_image(path: Path, size=(32, 24)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, (120, 20, 20))
    image.save(path, format="JPEG")


def _probe_from_uploaded_keys(uploaded_urls: set[str]):
    def _probe(url: str, session=None):
        return {
            "status": "exists" if url in uploaded_urls else "missing_404",
            "http_status": 200 if url in uploaded_urls else 404,
            "url": url,
            "detail": None,
        }

    return _probe


def test_probe_uses_cache_busted_range_get_and_treats_206_as_exists(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 206

        def close(self):
            captured["closed"] = True

    class FakeSession:
        def get(self, url, timeout=None, allow_redirects=None, headers=None, stream=None):
            captured["url"] = url
            captured["timeout"] = timeout
            captured["allow_redirects"] = allow_redirects
            captured["headers"] = dict(headers or {})
            captured["stream"] = stream
            return FakeResponse()

    result = audit._probe_public_media_url("https://media.sporely.no/path/image.webp", session=FakeSession())

    assert result["status"] == "exists"
    assert result["http_status"] == 206
    parsed = urlsplit(captured["url"])
    params = parse_qs(parsed.query)
    assert audit._MEDIA_CACHE_BUST_PARAM in params
    assert captured["headers"]["Range"] == "bytes=0-0"
    assert captured["headers"]["Cache-Control"] == "no-cache, no-store, max-age=0"
    assert captured["headers"]["Pragma"] == "no-cache"
    assert captured["stream"] is True
    assert captured["closed"] is True


class DummyClient:
    def __init__(self, uploaded_urls: set[str]):
        self.user_id = "8c471394-b274-4933-b830-59805820d93c"
        self.uploaded_urls = uploaded_urls
        self.upload_calls: list[dict] = []

    def upload_image_file(self, local_path, obs_cloud_id, img_cloud_id, storage_path=None):
        self.upload_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path),
            }
        )
        original_url, thumb_url = audit._cloud_row_urls(storage_path)
        if original_url:
            self.uploaded_urls.add(original_url)
        if thumb_url:
            self.uploaded_urls.add(thumb_url)
        return storage_path


def _local_context(db_path: Path, monkeypatch):
    monkeypatch.setattr(audit.models, "get_connection", lambda: sqlite3.connect(db_path))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return audit._load_local_context(conn)
    finally:
        conn.close()


def test_dry_run_maps_cloud_desktop_id_to_local_image_id(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    image_path = tmp_path / "images" / "field.jpg"
    _make_image(image_path)
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=image_path, cloud_id="1487", sort_order=0)
    _seed_local_image(
        db_path,
        image_id=12,
        observation_id=1,
        filepath=tmp_path / "images" / "microscope.jpg",
        cloud_id=None,
        sort_order=1,
        image_type="microscope",
        file_purpose="microscope",
    )
    context = _local_context(db_path, monkeypatch)
    monkeypatch.setattr(audit, "_probe_public_media_url", lambda url, session=None: {"status": "exists", "http_status": 200, "url": url, "detail": None})

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }

    report = audit._prepare_row_report(cloud_row, context, None)

    assert report["local_image_id"] == 11
    assert report["match_method"] == "desktop_id"
    assert report["local_file_exists"] is True
    assert report["repairable"] == "no"


def test_dry_run_marks_missing_original_and_thumb_repairable_when_local_file_exists(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    image_path = tmp_path / "images" / "field.jpg"
    _make_image(image_path)
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=image_path, cloud_id="1487", sort_order=0)
    context = _local_context(db_path, monkeypatch)
    monkeypatch.setattr(audit, "_probe_public_media_url", _probe_from_uploaded_keys(set()))

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }

    report = audit._prepare_row_report(cloud_row, context, None)

    assert report["original_status"] == "missing_404"
    assert report["thumb_status"] == "missing_404"
    assert report["repairable"] == "yes"
    assert "missing media object" in report["reason"]
    assert report["thumb_key"] == media_variant_key(cloud_row["storage_path"], "thumb")


def test_repair_mode_uploads_to_existing_storage_path_without_inserting_a_new_cloud_row(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    image_path = tmp_path / "images" / "field.jpg"
    _make_image(image_path)
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=image_path, cloud_id="1487", sort_order=0)
    context = _local_context(db_path, monkeypatch)
    uploaded_urls: set[str] = set()
    monkeypatch.setattr(audit, "_probe_public_media_url", _probe_from_uploaded_keys(uploaded_urls))
    client = DummyClient(uploaded_urls)

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }
    report = audit._prepare_row_report(cloud_row, context, None)

    repaired = audit._repair_rows(client, [report], None)

    repaired_row = repaired[0]
    assert repaired_row["repair_status"] == "repaired"
    assert repaired_row["repair_uploaded_original_key"] == report["storage_path"]
    assert repaired_row["repair_uploaded_thumb_key"] == report["thumb_key"]
    assert repaired_row["repair_post_original_status"] == "exists"
    assert repaired_row["repair_post_thumb_status"] == "exists"
    assert len(client.upload_calls) == 1
    assert client.upload_calls[0]["storage_path"] == report["storage_path"]
    assert client.upload_calls[0]["obs_cloud_id"] == "606"
    assert client.upload_calls[0]["img_cloud_id"] == "1487"


def test_tombstoned_rows_are_skipped(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    image_path = tmp_path / "images" / "field.jpg"
    _make_image(image_path)
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=image_path, cloud_id="1487", sort_order=0)
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("1487", "2026-05-29 11:26:30", "606", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()
    context = _local_context(db_path, monkeypatch)
    monkeypatch.setattr(audit, "_probe_public_media_url", _probe_from_uploaded_keys(set()))

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }
    report = audit._prepare_row_report(cloud_row, context, None)

    assert report["local_tombstoned"] is True
    assert report["repairable"] == "no"
    assert "tombstoned" in report["reason"]


def test_local_missing_file_is_reported_not_repaired(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    image_path = tmp_path / "images" / "field.jpg"
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=image_path, cloud_id="1487", sort_order=0)
    context = _local_context(db_path, monkeypatch)
    monkeypatch.setattr(audit, "_probe_public_media_url", _probe_from_uploaded_keys(set()))
    client = DummyClient(set())

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }
    report = audit._prepare_row_report(cloud_row, context, None)

    repaired = audit._repair_rows(client, [report], None)

    repaired_row = repaired[0]
    assert repaired_row["repairable"] == "no"
    assert repaired_row["repair_status"] == "skipped_not_repairable"
    assert repaired_row["reason"] == "local file is missing"
    assert client.upload_calls == []


def test_excluded_local_only_microscope_images_are_not_reported_as_missing_cloud_objects(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "sporely.db"
    _create_test_db(db_path)
    field_path = tmp_path / "images" / "field.jpg"
    microscope_path = tmp_path / "images" / "microscope.jpg"
    _make_image(field_path)
    _make_image(microscope_path)
    _seed_local_image(db_path, image_id=11, observation_id=1, filepath=field_path, cloud_id="1487", sort_order=0)
    _seed_local_image(
        db_path,
        image_id=12,
        observation_id=1,
        filepath=microscope_path,
        cloud_id=None,
        sort_order=1,
        image_type="microscope",
        file_purpose="microscope",
    )
    context = _local_context(db_path, monkeypatch)
    monkeypatch.setattr(audit, "_probe_public_media_url", _probe_from_uploaded_keys(set()))

    cloud_row = {
        "id": "1487",
        "desktop_id": 11,
        "observation_id": "606",
        "storage_path": "8c471394-b274-4933-b830-59805820d93c/606/0_1779475038038.webp",
        "original_filename": "0_1779475038038.webp",
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "original",
        "source_width": 3000,
        "source_height": 4000,
        "stored_width": 3000,
        "stored_height": 4000,
        "stored_bytes": 1234,
    }
    report = audit._prepare_row_report(cloud_row, context, None)

    assert report["cloud_image_id"] == "1487"
    assert report["local_image_id"] == 11
    assert report["local_observation_id"] == 1
    assert report["repairable"] == "yes"
    assert report["image_type"] == "field"
    assert report["match_method"] == "desktop_id"
