"""F18: the Worker URL override only accepts https or loopback http."""
import logging

import pytest

from utils.r2_storage import MEDIA_WORKER_DEFAULT_URL, media_worker_base_url


@pytest.mark.parametrize(
    "value",
    [
        "http://example.invalid",
        "ftp://upload.test",
        "file:///etc/passwd",
        "https://",
        "upload.test",
        "http://localhost.evil.test",
    ],
)
def test_worker_url_override_rejects_unsafe_values(monkeypatch, caplog, value):
    monkeypatch.setenv("SPORELY_MEDIA_WORKER_URL", value)
    with caplog.at_level(logging.WARNING):
        assert media_worker_base_url() == MEDIA_WORKER_DEFAULT_URL
    assert any("SPORELY_MEDIA_WORKER_URL" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("https://upload.test/", "https://upload.test"),
        ("http://localhost:8787", "http://localhost:8787"),
        ("http://127.0.0.1:8787/", "http://127.0.0.1:8787"),
        ("http://[::1]:8787", "http://[::1]:8787"),
    ],
)
def test_worker_url_override_accepts_https_and_loopback_http(monkeypatch, value, expected):
    monkeypatch.setenv("SPORELY_MEDIA_WORKER_URL", value)
    assert media_worker_base_url() == expected
