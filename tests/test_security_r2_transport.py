"""F17: media downloads are capped and replace the destination atomically."""
import pytest

from utils import r2_storage
from utils.r2_storage import (
    CloudflareMediaWorkerClient,
    CloudflareR2Client,
    R2Config,
)


class FailingMidwayResponse:
    ok = True
    status_code = 200
    text = ""

    def __init__(self):
        self.headers = {"content-type": "image/jpeg"}

    def iter_content(self, chunk_size=1024):
        yield b"partial"
        raise ConnectionError("connection dropped")

    def close(self):
        pass


class HugeResponse:
    ok = True
    status_code = 200
    text = ""

    def __init__(self):
        self.headers = {"content-type": "image/jpeg"}

    def iter_content(self, chunk_size=1024):
        for _ in range(100):
            yield b"x" * 1024

    def close(self):
        pass


class GoodResponse:
    ok = True
    status_code = 200
    text = ""

    def __init__(self):
        self.headers = {"content-type": "image/jpeg"}

    def iter_content(self, chunk_size=1024):
        yield b"new-complete-bytes"

    def close(self):
        pass


def _clients():
    r2 = CloudflareR2Client(
        R2Config(
            access_key_id="dummy-key",
            secret_access_key="dummy-secret",
            s3_endpoint="https://s3.test",
            public_base_url="https://media.test",
        )
    )
    worker = CloudflareMediaWorkerClient("dummy-token", base_url="https://upload.test")
    return {"r2": r2, "worker": worker}


@pytest.mark.parametrize("which", ["r2", "worker"])
@pytest.mark.parametrize("response_cls", [FailingMidwayResponse, HugeResponse])
def test_failed_or_oversized_download_leaves_no_file(monkeypatch, tmp_path, which, response_cls):
    monkeypatch.setattr("requests.Session.request", lambda self, *a, **kw: response_cls())
    monkeypatch.setattr(r2_storage, "MAX_MEDIA_DOWNLOAD_BYTES", 10 * 1024, raising=False)
    client = _clients()[which]
    dest = tmp_path / "out.jpg"
    with pytest.raises(Exception):
        client.download_to_file("user/obs/photo.jpg", dest)
    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("which", ["r2", "worker"])
@pytest.mark.parametrize("response_cls", [FailingMidwayResponse, HugeResponse])
def test_failed_download_keeps_existing_good_file(monkeypatch, tmp_path, which, response_cls):
    monkeypatch.setattr("requests.Session.request", lambda self, *a, **kw: response_cls())
    monkeypatch.setattr(r2_storage, "MAX_MEDIA_DOWNLOAD_BYTES", 10 * 1024, raising=False)
    client = _clients()[which]
    dest = tmp_path / "out.jpg"
    dest.write_bytes(b"good-old")
    with pytest.raises(Exception):
        client.download_to_file("user/obs/photo.jpg", dest)
    assert dest.read_bytes() == b"good-old"
    assert [p.name for p in tmp_path.iterdir()] == ["out.jpg"]


@pytest.mark.parametrize("which", ["r2", "worker"])
def test_successful_download_replaces_file_atomically(monkeypatch, tmp_path, which):
    monkeypatch.setattr("requests.Session.request", lambda self, *a, **kw: GoodResponse())
    client = _clients()[which]
    dest = tmp_path / "out.jpg"
    dest.write_bytes(b"old")
    assert client.download_to_file("user/obs/photo.jpg", dest) == dest
    assert dest.read_bytes() == b"new-complete-bytes"
    assert [p.name for p in tmp_path.iterdir()] == ["out.jpg"]


@pytest.mark.parametrize("which", ["r2", "worker"])
def test_replacement_keeps_the_existing_files_permissions(monkeypatch, tmp_path, which):
    import os
    import stat

    monkeypatch.setattr("requests.Session.request", lambda self, *a, **kw: GoodResponse())
    client = _clients()[which]
    dest = tmp_path / "out.jpg"
    dest.write_bytes(b"old")
    os.chmod(dest, 0o600)
    client.download_to_file("user/obs/photo.jpg", dest)
    assert stat.S_IMODE(os.stat(dest).st_mode) == 0o600
