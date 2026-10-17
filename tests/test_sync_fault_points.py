"""The ``during_upload`` and ``between_deletes`` crash points of ``SPORELY_SYNC_FAULT``.

Both are inert unless the environment variable names them. ``during_upload``
is driven against a real local HTTP server, so the test shows that the first
body chunk reached the server before the process would have ended.
"""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from utils import cloud_sync
from utils.cloud_sync_impl import fault_injection
from utils.r2_storage import CloudflareMediaWorkerClient


class _Killed(BaseException):
    """The process exit of ``SPORELY_SYNC_FAULT``, raised so the test survives it."""


def _raise_killed() -> None:
    raise _Killed()


@pytest.fixture(autouse=True)
def _no_exit(monkeypatch):
    monkeypatch.setattr(fault_injection, "_terminate", _raise_killed)
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR, raising=False)


@pytest.fixture
def upload_server():
    received: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            got = b""
            while len(got) < length:
                chunk = self.rfile.read(min(4096, length - len(got)))
                if not chunk:
                    break
                got += chunk
            received.append(got)
            body = b'{"key": "user-1/1/a.webp"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", received
    finally:
        server.shutdown()
        server.server_close()


def _upload(base_url: str, tmp_path, size: int) -> dict:
    path = tmp_path / "a.webp"
    path.write_bytes(b"x" * size)
    worker = CloudflareMediaWorkerClient("token", base_url=base_url)
    return worker.put_file(path, "user-1/1/a.webp", content_type="image/webp", timeout=10)


def test_the_new_points_are_inert_unless_set_and_scoped_by_table(monkeypatch):
    for point in ("during_upload", "between_deletes"):
        fault_injection.sync_fault_point(point, table="observations")
        assert not fault_injection.sync_fault_armed(point, table="observations")
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "between_deletes:observation_images")
    fault_injection.sync_fault_point("between_deletes", table="observations")
    with pytest.raises(_Killed):
        fault_injection.sync_fault_point("between_deletes", table="observation_images")


def test_an_unarmed_upload_body_is_the_file_itself(monkeypatch, tmp_path):
    handle = object()
    assert fault_injection.fault_upload_body(handle, table="observation_images") is handle
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_upload")
    assert fault_injection.fault_upload_body(handle, table="observation_images") is handle


def test_an_unset_hook_uploads_the_whole_file(upload_server, tmp_path):
    base_url, received = upload_server
    assert _upload(base_url, tmp_path, 200_000)["key"] == "user-1/1/a.webp"
    assert received == [b"x" * 200_000]


def test_during_upload_ends_the_process_after_the_first_chunk_was_sent(
    upload_server, tmp_path, monkeypatch
):
    base_url, received = upload_server
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "during_upload")
    with pytest.raises(_Killed):
        _upload(base_url, tmp_path, 200_000)
    # The server got the first chunk only; the rest never left the client.
    for _ in range(50):
        if received:
            break
        time.sleep(0.05)
    assert received and 0 < len(received[0]) < 200_000


def test_between_deletes_stops_before_the_observation_delete(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    deletes: list[str] = []
    monkeypatch.setattr(
        client, "pull_image_metadata", lambda cloud_id, include_deleted_for_sync=False: []
    )
    monkeypatch.setattr(client, "_get", lambda path: [])
    monkeypatch.setattr(client, "_delete", lambda path: deletes.append(path))

    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "between_deletes:observations")
    with pytest.raises(_Killed):
        client.delete_cloud_observation("cloud-obs-1")
    assert deletes == ["observation_images?observation_id=eq.cloud-obs-1"]

    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    deletes.clear()
    client.delete_cloud_observation("cloud-obs-1")
    assert deletes == [
        "observation_images?observation_id=eq.cloud-obs-1",
        "observations?id=eq.cloud-obs-1",
    ]
