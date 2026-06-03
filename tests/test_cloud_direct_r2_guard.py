from pathlib import Path

import pytest
from PIL import Image

from utils import cloud_sync


def _write_test_image(path: Path, size=(800, 600), color=(96, 64, 32)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color)
    image.save(path)
    return path


def _forbid_from_env(cls):
    raise AssertionError("CloudflareR2Client.from_env should not be called")


class _DummyMediaWorker:
    def __init__(self):
        self.calls = []

    def put_file(self, file_path, key, *, content_type=None, cache_control=None, upload_meta=None, options=None, timeout=None):
        self.calls.append(
            (
                "put_file",
                str(file_path),
                key,
                content_type,
                cache_control,
                dict(upload_meta or {}),
                dict(options or {}),
                timeout,
                Path(file_path).stat().st_size,
            )
        )
        return {"ok": True, "key": key, "url": f"https://media.sporely.no/{key}"}

    def put_bytes(self, data, key, *, content_type=None, cache_control=None, upload_meta=None, options=None, timeout=None):
        self.calls.append(
            (
                "put_bytes",
                bytes(data),
                key,
                content_type,
                cache_control,
                dict(upload_meta or {}),
                dict(options or {}),
                timeout,
                len(bytes(data)),
            )
        )
        return {"ok": True, "key": key, "url": f"https://media.sporely.no/{key}"}

    def download_to_file(self, storage_key, dest_path, *, timeout=120):
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"downloaded-bytes")
        self.calls.append(("download_to_file", storage_key, str(destination), timeout))
        return destination

    def delete_objects(self, keys, timeout=120):
        self.calls.append(("delete_objects", [str(key) for key in keys], timeout))

    def public_url(self, key):
        return f"https://media.sporely.no/{key}"


def test_upload_image_file_uses_worker_without_r2_secrets(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    worker = _DummyMediaWorker()

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_get_media_worker", lambda: worker)

    uploaded_key = client.upload_image_file(
        str(source),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-123/cloud-obs-1/source.jpg",
    )

    assert uploaded_key == "user-123/cloud-obs-1/source.jpg"
    assert [call[0] for call in worker.calls[:2]] == ["put_file", "put_bytes"]
    assert Path(worker.calls[0][1]).name.startswith("cloud_")
    assert worker.calls[0][-1] <= source.stat().st_size


def test_upload_image_file_uses_worker_even_when_admin_env_exists_without_explicit_flag(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    admin_env = tmp_path / "sporely-admin.env"
    admin_env.write_text(
        "R2_ACCESS_KEY_ID=test_key\n"
        "R2_SECRET_ACCESS_KEY=test_secret\n"
        "R2_S3_ENDPOINT=https://s3.test\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr("utils.r2_storage._ENV_FILE_CANDIDATES", (admin_env,))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ENV_FILE_CANDIDATES", (tmp_path / "missing-python.env",))
    worker = _DummyMediaWorker()
    monkeypatch.setattr(client, "_get_media_worker", lambda: worker)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))

    uploaded_key = client.upload_image_file(
        str(source),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-123/cloud-obs-1/source.jpg",
    )

    assert uploaded_key == "user-123/cloud-obs-1/source.jpg"
    assert [call[0] for call in worker.calls[:2]] == ["put_file", "put_bytes"]
    assert Path(worker.calls[0][1]).name.startswith("cloud_")
    assert worker.calls[0][-1] <= source.stat().st_size


def test_upload_image_file_surfaces_plan_limit_context(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    upload_meta = {
        "observation_id": 17,
        "observation_label": "Agaricus campestris",
        "image_id": 42,
        "image_label": "source.jpg (field)",
        "source_path": str(source),
        "source_filename": source.name,
        "source_bytes": source.stat().st_size,
        "source_width": 800,
        "source_height": 600,
        "stored_width": 640,
        "stored_height": 480,
        "stored_bytes": source.stat().st_size,
        "upload_mode": "full",
        "quality_profile": "standard",
        "full_image_byte_cap": 1_000_000,
    }

    monkeypatch.setattr(
        cloud_sync,
        "_prepare_cloud_image_upload_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cloud_sync.CloudSyncError(cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE)
        ),
    )

    with pytest.raises(cloud_sync.CloudSyncError) as excinfo:
        client.upload_image_file(
            str(source),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-123/cloud-obs-1/source.jpg",
            upload_meta=upload_meta,
        )

    text = str(excinfo.value)
    assert text.startswith(cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE)
    assert "Observation: Agaricus campestris (ID 17)" in text
    assert "Image: source.jpg (field) (ID 42)" in text
    assert "Original file: " in text
    assert cloud_sync._format_size(source.stat().st_size) in text
    assert "Prepared upload size: " in text
    assert "Plan cap: " in text


def test_download_image_file_uses_public_media_without_r2_secrets(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))

    def fake_public_download(storage_path, dest_path, *, timeout=120):
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"public-bytes")
        return destination

    monkeypatch.setattr(client, "_download_public_media_file", fake_public_download)
    monkeypatch.setattr(client, "_get_media_worker", lambda: (_ for _ in ()).throw(AssertionError("worker download should not be needed for public media")))

    dest_path = tmp_path / "downloaded.jpg"
    result = client.download_image_file(
        "user-123/cloud-obs-1/source.jpg",
        dest_path,
    )

    assert result == dest_path
    assert result.read_bytes() == b"public-bytes"


def test_download_image_file_falls_back_to_worker_when_public_media_fails(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    worker = _DummyMediaWorker()

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_download_public_media_file", lambda *args, **kwargs: (_ for _ in ()).throw(cloud_sync.CloudSyncError("public media unavailable")))
    monkeypatch.setattr(client, "_get_media_worker", lambda: worker)

    dest_path = tmp_path / "downloaded.jpg"
    result = client.download_image_file("user-123/cloud-obs-1/source.jpg", dest_path)

    assert result == dest_path
    assert result.read_bytes() == b"downloaded-bytes"
    assert [call[0] for call in worker.calls] == ["download_to_file"]


def test_delete_cloud_observation_uses_worker_without_r2_secrets(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    worker = _DummyMediaWorker()
    delete_calls = []

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(
        client,
        "pull_image_metadata",
        lambda cloud_id, include_deleted_for_sync=False: [
            {"storage_path": "user-123/cloud-obs-1/source.jpg"},
        ],
    )
    monkeypatch.setattr(client, "_get_media_worker", lambda: worker)
    monkeypatch.setattr(client, "_delete", lambda path: delete_calls.append(path))

    client.delete_cloud_observation("cloud-obs-1")

    assert delete_calls == [
        "observation_images?observation_id=eq.cloud-obs-1",
        "observations?id=eq.cloud-obs-1",
    ]
    assert worker.calls and worker.calls[0][0] == "delete_objects"
    assert "user-123/cloud-obs-1/source.jpg" in worker.calls[0][1]
    assert "user-123/cloud-obs-1/thumb_source.jpg" in worker.calls[0][1]


def test_upload_image_file_allows_direct_r2_when_explicit_flag_and_admin_env_are_present(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    admin_env = tmp_path / "sporely-admin.env"
    admin_env.write_text(
        "R2_ACCESS_KEY_ID=test_key\n"
        "R2_SECRET_ACCESS_KEY=test_secret\n"
        "R2_S3_ENDPOINT=https://s3.test\n",
        encoding="utf-8",
    )
    calls = []

    class DummyR2:
        def put_file(self, file_path, key, *, content_type=None, cache_control=None, custom_metadata=None, timeout=None):
            calls.append(("put_file", str(file_path), key))

        def put_bytes(self, data, key, *, content_type=None, cache_control=None, custom_metadata=None, timeout=None):
            calls.append(("put_bytes", bytes(data), key))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SPORELY_ENABLE_DIRECT_R2", "1")
    monkeypatch.setattr("utils.r2_storage._ENV_FILE_CANDIDATES", (admin_env,))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ENV_FILE_CANDIDATES", (tmp_path / "missing-python.env",))
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(lambda cls: DummyR2()))

    uploaded_key = client.upload_image_file(
        str(source),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-123/cloud-obs-1/source.jpg",
    )

    assert uploaded_key == "user-123/cloud-obs-1/source.jpg"
    assert [call[0] for call in calls] == ["put_file", "put_bytes"]
