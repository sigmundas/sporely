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


def test_upload_image_file_raises_clear_error_without_direct_r2_config(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))

    with pytest.raises(cloud_sync.CloudSyncError, match=cloud_sync.R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE):
        client.upload_image_file(
            str(source),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-123/cloud-obs-1/source.jpg",
        )


def test_upload_image_file_does_not_touch_admin_env_without_explicit_flag(monkeypatch, tmp_path):
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
    monkeypatch.setattr(
        "utils.r2_storage.load_admin_env_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("admin env should not be loaded without the explicit runtime flag")),
    )
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))

    with pytest.raises(cloud_sync.CloudSyncError, match=cloud_sync.R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE):
        client.upload_image_file(
            str(source),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-123/cloud-obs-1/source.jpg",
        )


def test_download_image_file_raises_clear_error_without_direct_r2_config(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))

    with pytest.raises(cloud_sync.CloudSyncError, match=cloud_sync.R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE):
        client.download_image_file(
            "user-123/cloud-obs-1/source.jpg",
            tmp_path / "downloaded.jpg",
        )


def test_delete_cloud_observation_raises_clear_error_without_direct_r2_config(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
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
    monkeypatch.setattr(client, "_delete", lambda path: delete_calls.append(path))

    with pytest.raises(cloud_sync.CloudSyncError, match=cloud_sync.R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE):
        client.delete_cloud_observation("cloud-obs-1")

    assert delete_calls == []


def test_upload_image_file_still_uses_monkeypatched_r2_when_public_config_is_missing(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    calls = []

    class DummyR2:
        def put_file(self, file_path, key, *, content_type=None, cache_control=None, custom_metadata=None, timeout=None):
            calls.append(
                (
                    "put_file",
                    str(file_path),
                    key,
                    content_type,
                    cache_control,
                    dict(custom_metadata or {}),
                    timeout,
                )
            )

        def put_bytes(self, data, key, *, content_type=None, cache_control=None, custom_metadata=None, timeout=None):
            calls.append(
                (
                    "put_bytes",
                    bytes(data),
                    key,
                    content_type,
                    cache_control,
                    dict(custom_metadata or {}),
                    timeout,
                )
            )

    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_get_r2", lambda: DummyR2())

    uploaded_key = client.upload_image_file(
        str(source),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-123/cloud-obs-1/source.jpg",
    )

    assert uploaded_key == "user-123/cloud-obs-1/source.jpg"
    assert [call[0] for call in calls] == ["put_file", "put_bytes"]


def test_download_image_file_still_uses_monkeypatched_r2_when_public_config_is_missing(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    calls = []

    class DummyR2:
        def download_to_file(self, key, dest_path, *, timeout=120):
            destination = Path(dest_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"downloaded-bytes")
            calls.append((key, str(destination), timeout))
            return destination

    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_get_r2", lambda: DummyR2())

    dest_path = tmp_path / "downloaded.jpg"
    result = client.download_image_file("user-123/cloud-obs-1/source.jpg", dest_path)

    assert result == dest_path
    assert result.read_bytes() == b"downloaded-bytes"
    assert calls == [("user-123/cloud-obs-1/source.jpg", str(dest_path), 120)]


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
