"""Generic loopback OAuth callback transport.

Provides a single-use localhost HTTP server that receives an OAuth
authorization code callback. Usable by any OAuth client that uses a
loopback redirect URI.

Provider-specific concerns (URLs, client IDs, token exchange, scopes,
state validation, PKCE semantics) remain in the provider client module.
"""
from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse


_LOOPBACK_HOSTNAMES = frozenset({"localhost", "127.0.0.1", "::1"})

_HTML_SUCCESS = (
    "<html><body><h3>Login complete.</h3>You may close this tab.</body></html>"
)
_HTML_ERROR = (
    "<html><body><h3>Login failed.</h3>You may close this tab.</body></html>"
)
_HTML_NOT_FOUND = "<html><body><h3>Not Found</h3></body></html>"


@dataclass
class OAuthCallbackPayload:
    """Parsed query parameters from a loopback OAuth callback request."""

    code: str | None = None
    state: str | None = None
    error: str | None = None
    error_description: str | None = None


def _bind_host_for(hostname: str) -> str:
    """Return the explicit bind address for a validated loopback hostname.

    Always binds to an explicit IP rather than a name so there is no
    DNS resolution and the bind is provably loopback-only.
    """
    if hostname == "::1":
        return "::1"
    return "127.0.0.1"


class LoopbackCallbackServer:
    """Single-use loopback HTTP server for OAuth redirect callbacks.

    Binds exclusively to a loopback IP address. Rejects requests that do
    not match the registered callback path with a 404. Closes after the
    first matching callback or after the timeout elapses.

    State/PKCE validation is the caller's responsibility — this transport
    surfaces the raw payload so that provider-specific semantics remain in
    the provider layer.
    """

    def __init__(self, redirect_uri: str) -> None:
        parsed = urlparse(redirect_uri)
        if parsed.scheme != "http":
            raise ValueError(
                "Loopback callback server requires an http:// redirect URI."
            )
        host = parsed.hostname or ""
        if host not in _LOOPBACK_HOSTNAMES:
            raise ValueError(
                f"Redirect URI host {host!r} is not a loopback address. "
                "Only localhost, 127.0.0.1, and ::1 are permitted."
            )
        if not parsed.port:
            raise ValueError(
                "Redirect URI must specify an explicit port, "
                "e.g. http://127.0.0.1:8765/auth/callback."
            )
        self.redirect_uri = redirect_uri
        self.port = int(parsed.port)
        self.callback_path = parsed.path or "/callback"
        self._bind_host = _bind_host_for(host)

    def wait_for_callback(
        self, timeout: int = 180, tick_callback=None
    ) -> OAuthCallbackPayload:
        """Block until an OAuth callback arrives or *timeout* seconds elapse.

        Starts a temporary HTTP server on loopback:{self.port}. The server
        is always closed before this method returns, whether or not a
        callback was received.

        *tick_callback* is called after each server poll cycle when provided.
        An InterruptedError raised by tick_callback propagates immediately.

        Raises RuntimeError if the port cannot be bound.
        Raises TimeoutError on timeout.
        """
        payload = OAuthCallbackPayload()
        callback_event = threading.Event()
        callback_path = self.callback_path

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A003
                return  # suppress all HTTP logging (includes auth codes in query strings)

            def _send(self, status: int, body: str) -> None:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path != callback_path:
                    self._send(404, _HTML_NOT_FOUND)
                    return
                query = parse_qs(parsed.query)
                payload.code = (query.get("code") or [None])[0]
                payload.state = (query.get("state") or [None])[0]
                payload.error = (query.get("error") or [None])[0]
                payload.error_description = (
                    query.get("error_description") or [None]
                )[0]
                callback_event.set()
                if payload.error:
                    self._send(200, _HTML_ERROR)
                else:
                    self._send(200, _HTML_SUCCESS)

        try:
            server = HTTPServer((self._bind_host, self.port), _Handler)
        except OSError as exc:
            raise RuntimeError(
                f"Could not bind callback server to {self._bind_host}:{self.port}: {exc}"
            ) from exc

        try:
            server.timeout = 0.05
            deadline = time.time() + max(1, int(timeout))
            while time.time() < deadline and not callback_event.is_set():
                server.handle_request()
                if tick_callback is not None:
                    try:
                        tick_callback()
                    except Exception as exc:
                        if isinstance(exc, InterruptedError):
                            raise
        finally:
            server.server_close()

        if not callback_event.is_set():
            raise TimeoutError("Timed out waiting for OAuth callback.")
        return payload


def loopback_port_is_free(port: int) -> bool:
    """Return True if loopback port *port* is available for a new server.

    Uses SO_REUSEADDR to match HTTPServer's binding behaviour — a port in
    TIME_WAIT (from a recently closed connection) is still reported as free
    because HTTPServer can bind to it successfully.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False
