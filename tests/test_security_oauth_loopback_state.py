"""F21: a stray request must not end the sign-in wait; idle connections must not stall it."""
import socket
import threading
import time
import urllib.error
import urllib.request

from utils.oauth_loopback import LoopbackCallbackServer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port, timeout=2.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return True
        except OSError:
            time.sleep(0.02)
    return False


def _get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def _run(server, **kwargs):
    result, errors = [], []

    def target():
        try:
            result.append(server.wait_for_callback(**kwargs))
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t, result, errors


def test_wrong_state_request_is_ignored_and_real_callback_still_succeeds():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run(server, timeout=5, expected_state="right-state")
    assert _wait_for_port(port)

    assert _get(port, "/auth/callback?code=stray&state=wrong-state") >= 400
    assert _get(port, "/auth/callback") >= 400
    assert t.is_alive(), "a stray request must not end the wait"

    assert _get(port, "/auth/callback?code=realcode&state=right-state") == 200
    t.join(timeout=3)
    assert not errors
    assert result[0].code == "realcode"
    assert result[0].state == "right-state"


def test_without_expected_state_behaviour_is_unchanged():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run(server, timeout=5)
    assert _wait_for_port(port)
    assert _get(port, "/auth/callback?code=c&state=whatever") == 200
    t.join(timeout=3)
    assert result[0].state == "whatever"


def test_idle_connection_cannot_outlast_the_deadline():
    port = _free_port()
    server = LoopbackCallbackServer(f"http://127.0.0.1:{port}/auth/callback")
    t, result, errors = _run(server, timeout=1)
    assert _wait_for_port(port)
    idle = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        t.join(timeout=5)
        assert not t.is_alive(), "an idle connection kept the wait running past its deadline"
        assert errors and isinstance(errors[0], TimeoutError)
    finally:
        idle.close()
        t.join(timeout=3)
