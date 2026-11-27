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


@pytest.mark.parametrize(
    "value",
    [
        "http://localhost\\@evil.test",
        "http://127.0.0.1\\.evil.test",
        "http://localhost:8787 @evil.test",
        "http://user@localhost.evil.test:8787",
        "http://evil.test\\@localhost:8787",
        "http://evil.test\\@127.0.0.1",
    ],
)
def test_worker_url_override_rejects_ambiguous_authorities(monkeypatch, value):
    monkeypatch.setenv("SPORELY_MEDIA_WORKER_URL", value)
    assert media_worker_base_url() == MEDIA_WORKER_DEFAULT_URL


def test_accepted_override_has_the_same_host_for_the_transport(monkeypatch):
    import urllib3

    monkeypatch.setenv("SPORELY_MEDIA_WORKER_URL", "http://localhost:8787")
    assert urllib3.util.parse_url(media_worker_base_url()).host == "localhost"
