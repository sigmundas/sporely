from pathlib import Path

import pytest
from PIL import Image

from utils import cloud_sync
from utils.r2_storage import CloudflareWorkerError


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
    assert Path(worker.calls[0][1]).suffix == ".webp"
    assert worker.calls[0][3] == "image/webp"
    assert worker.calls[0][5]["encoding_format"] == "image/webp"
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
    assert Path(worker.calls[0][1]).suffix == ".webp"
    assert worker.calls[0][3] == "image/webp"
    assert worker.calls[0][5]["encoding_format"] == "image/webp"
    assert worker.calls[0][-1] <= source.stat().st_size


def test_upload_original_image_file_uses_worker_without_r2_secrets(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")
    worker = _DummyMediaWorker()

    monkeypatch.delenv("SPORELY_ENABLE_DIRECT_R2", raising=False)
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_get_media_worker", lambda: worker)

    uploaded_key = client.upload_original_image_file(
        str(source),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-123/cloud-obs-1/originals/source.jpg",
    )

    assert uploaded_key == "user-123/cloud-obs-1/originals/source.jpg"
    assert [call[0] for call in worker.calls[:1]] == ["put_file"]
    assert Path(worker.calls[0][1]).suffix == ".webp"
    assert worker.calls[0][3] == "image/webp"
    assert worker.calls[0][5]["upload_variant"] == "original"
    assert worker.calls[0][5]["encoding_format"] == "image/webp"
    assert worker.calls[0][6]["uploadVariant"] == "original"
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
    monkeypatch.setattr(cloud_sync, "media_worker_base_url", lambda: "https://upload.test")

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
    assert "Prepared dimensions: 640 × 480 px" in text
    assert "Plan cap: " in text
    assert "Local upload variant: full" in text
    assert "Local upload mode: full / standard" in text
    assert "Worker base URL: https://upload.test" in text
    assert "Storage key: user-123/cloud-obs-1/source.jpg" in text
    assert "Content type: image/webp" in text
    assert "Prepared path suffix: .webp" in text


def test_upload_image_file_surfaces_worker_plan_limit_context_with_missing_details(monkeypatch, tmp_path):
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
        "stored_width": 800,
        "stored_height": 600,
        "upload_mode": "full",
        "quality_profile": "standard",
        "full_image_byte_cap": 1_000_000,
    }

    class _FailingWorker:
        base_url = "https://upload.test"

        def put_file(self, *args, **kwargs):
            raise CloudflareWorkerError(
                "Worker upload failed: Image is too large for plan",
                status_code=413,
                code="image_too_large_for_plan",
                payload={
                    "error": "image_too_large_for_plan",
                    "message": "Image is too large for plan",
                },
                request_url="https://upload.test/upload/user_123/cloud-obs-1/source.jpg",
                request_method="PUT",
                response_status=413,
                response_text='{"error":"image_too_large_for_plan","message":"Image is too large for plan"}',
                request_headers={
                    "Content-Type": "image/webp",
                    "X-Sporely-Upload-Mode": "full",
                    "X-Sporely-Upload-Variant": "full",
                    "X-Sporely-Cloud-Plan": "pro",
                    "X-Sporely-Quality-Profile": "standard",
                    "X-Sporely-Encoding-Format": "image/webp",
                    "X-Sporely-Stored-Width": "800",
                    "X-Sporely-Stored-Height": "600",
                },
            )

    monkeypatch.setattr(cloud_sync, "direct_r2_runtime_available", lambda: False)
    monkeypatch.setattr(cloud_sync, "media_worker_base_url", lambda: "https://upload.test")
    monkeypatch.setattr(cloud_sync.CloudflareR2Client, "from_env", classmethod(_forbid_from_env))
    monkeypatch.setattr(client, "_get_media_worker", lambda: _FailingWorker())

    with pytest.raises(cloud_sync.CloudSyncError) as excinfo:
        client.upload_image_file(
            str(source),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-123/cloud-obs-1/source.jpg",
            upload_meta=upload_meta,
        )

    text = str(excinfo.value)
    assert "Observation: Agaricus campestris (ID 17)" in text
    assert "Image: source.jpg (field) (ID 42)" in text
    assert "Original file: " in text
    assert cloud_sync._format_size(source.stat().st_size) in text
    assert "Local upload variant: full" in text
    assert "Local upload mode: full / standard" in text
    assert "Worker base URL: https://upload.test" in text
    assert "Storage key: user-123/cloud-obs-1/source.jpg" in text
    assert "Content type: image/webp" in text
    assert "Prepared path suffix: .webp" in text
    assert "Prepared dimensions: 800 × 600 px" in text
    assert "Prepared upload size: " in text
    assert "Worker details: missing" in text
    assert "Worker error code: image_too_large_for_plan" in text


@pytest.mark.parametrize(
    "payload, expected_reason, expected_message",
    [
        (
            {
                "error": "image_too_large_for_plan",
                "details": {
                    "bodyBytes": 5_200_001,
                    "planByteCap": 5_000_000,
                    "cloudPlan": "pro",
                    "qualityProfile": "high",
                },
            },
            "byte_cap",
            "Image exceeds the byte cap for this upload policy.",
        ),
        (
            {
                "error": "image_too_large_for_plan",
                "details": {
                    "storedPixels": 21_000_001,
                    "storedPixelCap": 21_000_000,
                    "cloudPlan": "pro",
                    "qualityProfile": "high",
                },
            },
            "pixel_cap",
            "Image exceeds the pixel cap for this upload policy.",
        ),
        (
            {
                "error": "image_too_large_for_plan",
                "details": {
                    "storedWidth": 5_301,
                    "storedHeight": 3_888,
                    "resizeMaxEdge": 5_300,
                    "cloudPlan": "pro",
                    "qualityProfile": "high",
                },
            },
            "edge_cap",
            "Image exceeds the longest-edge cap for this upload policy.",
        ),
    ],
)
def test_infer_image_too_large_for_plan_reason_from_worker_details(payload, expected_reason, expected_message):
    assert cloud_sync.infer_image_too_large_for_plan_reason(payload) == expected_reason
    assert cloud_sync.format_image_too_large_for_plan_reason(payload) == expected_message


def test_upload_image_file_requires_webp_support(monkeypatch, tmp_path):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    source = _write_test_image(tmp_path / "source.jpg")

    monkeypatch.setattr(cloud_sync.features, "check", lambda name: False if name == "webp" else True)

    with pytest.raises(cloud_sync.CloudSyncError) as excinfo:
        client.upload_image_file(
            str(source),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-123/cloud-obs-1/source.jpg",
        )

    assert str(excinfo.value) == cloud_sync.WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE


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
