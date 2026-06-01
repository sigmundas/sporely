import warnings
from pathlib import Path

import pytest
from utils.r2_storage import CloudflareR2Client, R2Config, r2_config_available


class MockResponse:
    def __init__(self):
        self.ok = True
        self.text = ""
        self.status_code = 200
    
    def iter_content(self, chunk_size=1024):
        yield b"fake_jpeg_bytes"


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
