import warnings
from pathlib import Path

import pytest
from utils.r2_storage import (
    CloudflareMediaWorkerClient,
    CloudflareR2Client,
    R2Config,
    media_worker_base_url,
    r2_config_available,
)


class MockResponse:
    def __init__(self):
        self.ok = True
        self.text = ""
        self.status_code = 200
        self.headers = {}

    def iter_content(self, chunk_size=1024):
        yield b"fake_jpeg_bytes"

    def json(self):
        return {"ok": True}


class MockJsonResponse:
    def __init__(self, *, ok=True, status_code=200, payload=None, text=""):
        self.ok = ok
        self.status_code = status_code
        self._payload = dict(payload or {})
        self.text = text or ("" if ok else "error")
        self.headers = {}

    def json(self):
        return dict(self._payload)

    def iter_content(self, chunk_size=1024):
        yield b"fake_worker_bytes"


def test_download_to_file(monkeypatch, tmp_path):
    config = R2Config(
        access_key_id="test_key",
        secret_access_key="test_secret",
        s3_endpoint="https://s3.test",
        public_base_url="https://media.sporely.no"
    )
    client = CloudflareR2Client(config)

    requested_urls = []

    def mock_request(self, method, url, **kwargs):
        if method.upper() == "GET":
            requested_urls.append(url)
            return MockResponse()
        return MockResponse()

    monkeypatch.setattr("requests.Session.request", mock_request)

    dest_file = tmp_path / "test_download.jpg"
    
    result = client.download_to_file(
        key="user_123/obs_456/photo.avif",
        dest_path=dest_file,
    )

    assert result == dest_file
    assert result.read_bytes() == b"fake_jpeg_bytes"
    
    assert len(requested_urls) == 1
    assert requested_urls[0] == "https://s3.test/sporely-media/user_123/obs_456/photo.avif"


def test_put_bytes_sends_custom_metadata_headers(monkeypatch):
    config = R2Config(
        access_key_id="test_key",
        secret_access_key="test_secret",
        s3_endpoint="https://s3.test",
        public_base_url="https://media.sporely.no",
    )
    client = CloudflareR2Client(config)

    captured = {}

    def mock_request(self, method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = dict(kwargs.get("headers") or {})

        class _Response:
            ok = True
            text = ""
            status_code = 200

        return _Response()

    monkeypatch.setattr("requests.Session.request", mock_request)

    client.put_bytes(
        b"fake_bytes",
        "user_123/obs_456/photo.webp",
        content_type="image/webp",
        cache_control="public, max-age=31536000, immutable",
        custom_metadata={
            "quality_profile": "high",
            "encoding_quality": 80,
            "encoding_format": "image/webp",
        },
    )

    assert captured["method"] == "PUT"
    assert captured["url"] == "https://s3.test/sporely-media/user_123/obs_456/photo.webp"
    assert captured["headers"]["x-amz-meta-quality-profile"] == "high"
    assert captured["headers"]["x-amz-meta-encoding-quality"] == "80"
    assert captured["headers"]["x-amz-meta-encoding-format"] == "image/webp"


def test_r2_config_available_false_when_values_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_S3_ENDPOINT", raising=False)
    monkeypatch.setattr("utils.r2_storage._ENV_FILE_CANDIDATES", (tmp_path / "missing-sporely-admin.env",))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ENV_FILE_CANDIDATES", (tmp_path / "missing-python.env",))

    assert r2_config_available() is False


def test_r2_config_available_true_when_sporely_admin_env_present(monkeypatch, tmp_path):
    admin_env = tmp_path / "sporely-admin.env"
    admin_env.write_text(
        "R2_ACCESS_KEY_ID=test_key\n"
        "R2_SECRET_ACCESS_KEY=test_secret\n"
        "R2_S3_ENDPOINT=https://s3.test\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_S3_ENDPOINT", raising=False)
    monkeypatch.setattr("utils.r2_storage._ENV_FILE_CANDIDATES", (admin_env,))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ENV_FILE_CANDIDATES", (tmp_path / "missing-python.env",))

    assert r2_config_available() is True


def test_r2_config_available_legacy_python_env_fallback_warns(monkeypatch, tmp_path):
    legacy_env = tmp_path / "python.env"
    legacy_env.write_text(
        "R2_ACCESS_KEY_ID=test_key\n"
        "R2_SECRET_ACCESS_KEY=test_secret\n"
        "R2_S3_ENDPOINT=https://s3.test\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("R2_S3_ENDPOINT", raising=False)
    monkeypatch.setattr("utils.r2_storage._ENV_FILE_CANDIDATES", (tmp_path / "missing-sporely-admin.env",))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ENV_FILE_CANDIDATES", (legacy_env,))
    monkeypatch.setattr("utils.r2_storage._LEGACY_ADMIN_ENV_WARNING_EMITTED", False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert r2_config_available() is True

    assert any("python.env is deprecated" in str(item.message) for item in caught)


def test_media_worker_base_url_defaults_to_production_endpoint(monkeypatch):
    monkeypatch.delenv("SPORELY_MEDIA_WORKER_URL", raising=False)
    assert media_worker_base_url() == "https://upload.sporely.no"


def test_media_worker_upload_sends_bearer_auth_and_worker_headers(monkeypatch, tmp_path):
    client = CloudflareMediaWorkerClient("test-access-token", base_url="https://upload.test")

    captured = {}

    def mock_request(self, method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = dict(kwargs.get("headers") or {})
        captured["data"] = kwargs.get("data")
        return MockJsonResponse(payload={
            "ok": True,
            "key": "user_123/obs_456/photo.webp",
            "url": "https://media.sporely.no/user_123/obs_456/photo.webp",
        }, status_code=201)

    monkeypatch.setattr("requests.Session.request", mock_request)

    payload = b"fake-bytes"
    result = client.put_bytes(
        payload,
        "user_123/obs_456/photo.webp",
        content_type="image/webp",
        cache_control="public, max-age=31536000, immutable",
        upload_meta={
            "upload_mode": "reduced",
            "quality_profile": "high",
            "encoding_quality": 80,
            "encoding_format": "image/webp",
            "source_width": 800,
            "source_height": 600,
            "stored_width": 400,
            "stored_height": 300,
        },
        options={
            "uploadMode": "reduced",
            "uploadVariant": "full",
            "cloudPlan": "free",
            "qualityProfile": "high",
            "encodingQuality": 80,
            "encodingFormat": "image/webp",
            "sourceWidth": 800,
            "sourceHeight": 600,
            "storedWidth": 400,
            "storedHeight": 300,
        },
    )

    assert result["key"] == "user_123/obs_456/photo.webp"
    assert captured["method"] == "PUT"
    assert captured["url"] == "https://upload.test/upload/user_123/obs_456/photo.webp"
    assert captured["data"] == payload
    assert captured["headers"]["Authorization"] == "Bearer test-access-token"
    assert captured["headers"]["Content-Type"] == "image/webp"
    assert captured["headers"]["Cache-Control"] == "public, max-age=31536000, immutable"
    assert captured["headers"]["X-Sporely-Upload-Mode"] == "reduced"
    assert captured["headers"]["X-Sporely-Upload-Variant"] == "full"
    assert captured["headers"]["X-Sporely-Cloud-Plan"] == "free"
    assert captured["headers"]["X-Sporely-Quality-Profile"] == "high"
    assert captured["headers"]["X-Sporely-Encoding-Quality"] == "80"
    assert captured["headers"]["X-Sporely-Encoding-Format"] == "image/webp"
    assert captured["headers"]["X-Sporely-Source-Width"] == "800"
    assert captured["headers"]["X-Sporely-Source-Height"] == "600"
    assert captured["headers"]["X-Sporely-Stored-Width"] == "400"
    assert captured["headers"]["X-Sporely-Stored-Height"] == "300"


def test_media_worker_download_uses_bearer_auth(monkeypatch, tmp_path):
    client = CloudflareMediaWorkerClient("test-access-token", base_url="https://upload.test")
    captured = {}

    def mock_request(self, method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = dict(kwargs.get("headers") or {})
        return MockResponse()

    monkeypatch.setattr("requests.Session.request", mock_request)

    dest_file = tmp_path / "downloaded.jpg"
    result = client.download_to_file("user_123/obs_456/photo.jpg", dest_file)

    assert result == dest_file
    assert result.read_bytes() == b"fake_jpeg_bytes"
    assert captured["method"] == "GET"
    assert captured["url"] == "https://upload.test/upload/user_123/obs_456/photo.jpg"
    assert captured["headers"]["Authorization"] == "Bearer test-access-token"


def test_media_worker_delete_tolerates_missing_objects(monkeypatch):
    client = CloudflareMediaWorkerClient("test-access-token", base_url="https://upload.test")
    captured = {}

    def mock_request(self, method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = dict(kwargs.get("headers") or {})
        return MockJsonResponse(ok=False, status_code=404, payload={"message": "not found"}, text="not found")

    monkeypatch.setattr("requests.Session.request", mock_request)

    assert client.delete_object("user_123/obs_456/photo.jpg") is None
    assert captured["method"] == "DELETE"
    assert captured["url"] == "https://upload.test/upload/user_123/obs_456/photo.jpg"
    assert captured["headers"]["Authorization"] == "Bearer test-access-token"
