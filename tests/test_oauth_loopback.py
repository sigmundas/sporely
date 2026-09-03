"""Tests for utils/oauth_loopback.py — generic loopback OAuth transport."""
from __future__ import annotations

import socket
import threading
import time
import urllib.error
import urllib.request

import pytest

from utils.oauth_loopback import (
    LoopbackCallbackServer,
    OAuthCallbackPayload,
    loopback_port_is_free,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    """Return an ephemeral port that is currently free on 127.0.0.1."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 2.0) -> bool:
    """Return True once 127.0.0.1:port is accepting connections, False on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return True
        except OSError:
            time.sleep(0.02)
    return False


def _get(port: int, path: str) -> tuple[int, str]:
    """Perform GET http://127.0.0.1:{port}{path}, return (status, body)."""
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def _run_server(server: LoopbackCallbackServer, timeout: int = 5):
    """Run wait_for_callback in a daemon thread; return (thread, result_list, error_list)."""
    result: list[OAuthCallbackPayload] = []
    errors: list[Exception] = []

    def _target():
        try:
            result.append(server.wait_for_callback(timeout=timeout))
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    return t, result, errors


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_rejects_https_redirect_uri():
    with pytest.raises(ValueError, match="http://"):
        LoopbackCallbackServer("https://127.0.0.1:8765/auth/callback")


def test_rejects_non_loopback_host():
    with pytest.raises(ValueError, match="loopback"):
        LoopbackCallbackServer("http://example.com:8765/auth/callback")


def test_rejects_host_0_0_0_0():
    with pytest.raises(ValueError, match="loopback"):
        LoopbackCallbackServer("http://0.0.0.0:8765/auth/callback")


def test_rejects_missing_port():
    with pytest.raises(ValueError, match="port"):
        LoopbackCallbackServer("http://127.0.0.1/auth/callback")


def test_accepts_localhost():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://localhost:{port}/auth/callback")
    assert server.port == port
    assert server.callback_path == "/auth/callback"


def test_accepts_127_0_0_1():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/callback")
    assert server.callback_path == "/callback"


# ---------------------------------------------------------------------------
# Successful callback
# ---------------------------------------------------------------------------


def test_successful_callback_returns_code_and_state():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port), "Server did not start in time"
    status, body = _get(port, "/auth/callback?code=mycode&state=mystate")

    t.join(timeout=3)
    assert not errors, errors
    assert status == 200
    assert "Login complete" in body
    assert len(result) == 1
    assert result[0].code == "mycode"
    assert result[0].state == "mystate"
    assert result[0].error is None
    assert result[0].error_description is None


# ---------------------------------------------------------------------------
# Error callback
# ---------------------------------------------------------------------------


def test_error_callback_surfaces_error_fields():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    status, body = _get(
        port, "/auth/callback?error=access_denied&error_description=User+cancelled"
    )

    t.join(timeout=3)
    assert not errors, errors
    assert status == 200
    assert "Login failed" in body
    assert result[0].error == "access_denied"
    assert result[0].error_description == "User cancelled"
    assert result[0].code is None


# ---------------------------------------------------------------------------
# Wrong path → 404 and server keeps waiting
# ---------------------------------------------------------------------------


def test_wrong_path_returns_404_server_continues():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    wrong_status, _ = _get(port, "/wrong/path")
    assert wrong_status == 404

    # Server still running — correct path must succeed
    _get(port, "/auth/callback?code=after-404&state=s")
    t.join(timeout=3)

    assert not errors, errors
    assert result[0].code == "after-404"


def test_root_path_returns_404():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    status, _ = _get(port, "/")
    assert status == 404

    _get(port, "/auth/callback?code=ok&state=s")
    t.join(timeout=3)
    assert not errors, errors


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_timeout_raises_timeout_error():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server, timeout=1)

    t.join(timeout=5)
    assert not t.is_alive(), "Server thread did not complete"
    assert len(errors) == 1
    assert isinstance(errors[0], TimeoutError)
    assert not result


def test_timeout_completes_within_reasonable_time():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    start = time.time()
    with pytest.raises(TimeoutError):
        server.wait_for_callback(timeout=1)
    assert time.time() - start < 4, "1s timeout took longer than 4s to resolve"


# ---------------------------------------------------------------------------
# Server closes after completion
# ---------------------------------------------------------------------------


def test_server_closes_after_successful_callback():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    _get(port, "/auth/callback?code=done&state=s")
    t.join(timeout=3)

    # Port should be free after server closes
    time.sleep(0.05)
    assert loopback_port_is_free(port), "Port still occupied after server closed"


def test_server_closes_after_timeout():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server, timeout=1)
    t.join(timeout=5)

    time.sleep(0.05)
    assert loopback_port_is_free(port), "Port still occupied after timeout-closed server"


# ---------------------------------------------------------------------------
# IPv4 loopback binding
# ---------------------------------------------------------------------------


def test_ipv4_loopback_connection_accepted():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    with urllib.request.urlopen(
        f"http://127.0.0.1:{port}/auth/callback?code=ipv4&state=s", timeout=5
    ) as resp:
        assert resp.status == 200

    t.join(timeout=3)
    assert not errors, errors
    assert result[0].code == "ipv4"


def test_localhost_uri_binds_to_ipv4_loopback():
    """localhost redirect URI is served via 127.0.0.1 (no DNS lookup)."""
    port = _free_port()
    server = LoopbackCallbackServer(f"http://localhost:{port}/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    _get(port, f"/callback?code=local&state=s")
    t.join(timeout=3)

    assert not errors, errors
    assert result[0].code == "local"


# ---------------------------------------------------------------------------
# Port already occupied → RuntimeError
# ---------------------------------------------------------------------------


def test_raises_runtime_error_when_port_occupied():
    """Trying to start the server when the port is already bound raises RuntimeError."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupier:
        occupier.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupier.bind(("127.0.0.1", 0))
        occupied_port = occupier.getsockname()[1]
        occupier.listen(1)

        server = LoopbackCallbackServer(f"http://127.0.0.1:{occupied_port}/auth/callback")
        with pytest.raises(RuntimeError, match="Could not bind"):
            server.wait_for_callback(timeout=1)


# ---------------------------------------------------------------------------
# No auth code in logs
# ---------------------------------------------------------------------------


def test_auth_code_not_logged_to_stderr(capsys):
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run_server(server)

    assert _wait_for_port(port)
    _get(port, "/auth/callback?code=secret-auth-code-9876&state=s")
    t.join(timeout=3)

    captured = capsys.readouterr()
    assert "secret-auth-code-9876" not in captured.out
    assert "secret-auth-code-9876" not in captured.err


# ---------------------------------------------------------------------------
# OAuthCallbackPayload dataclass
# ---------------------------------------------------------------------------


def test_payload_defaults_are_none():
    p = OAuthCallbackPayload()
    assert p.code is None
    assert p.state is None
    assert p.error is None
    assert p.error_description is None


def test_payload_fields_set():
    p = OAuthCallbackPayload(code="c", state="s", error="e", error_description="d")
    assert p.code == "c"
    assert p.state == "s"
    assert p.error == "e"
    assert p.error_description == "d"


# ---------------------------------------------------------------------------
# loopback_port_is_free helper
# ---------------------------------------------------------------------------


def test_loopback_port_is_free_reports_free_port():
    port = _free_port()
    assert loopback_port_is_free(port)


def test_loopback_port_is_free_reports_occupied_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.listen(1)
        assert not loopback_port_is_free(port)
