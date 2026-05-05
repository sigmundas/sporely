import pytest
from pathlib import Path
from utils.r2_storage import CloudflareR2Client, R2Config


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