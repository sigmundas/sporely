"""Minimal iNaturalist OAuth2 helper (desktop auth-code flow)."""
from __future__ import annotations

import json
import hashlib
import base64
import os
import secrets
import threading
import time
import webbrowser
import socket
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests


@dataclass
class OAuthCallbackPayload:
    code: str | None = None
    state: str | None = None
    error: str | None = None
    error_description: str | None = None


class DualStackServer(HTTPServer):
    """HTTP server that binds to both IPv4 and IPv6 if available."""

    def __init__(self, server_address, RequestHandlerClass):
        host, port = server_address
        if host in ("localhost", "127.0.0.1", "::1", "") and getattr(socket, "has_ipv6", False):
            self.address_family = socket.AF_INET6
            host = "::"
        super().__init__((host, port), RequestHandlerClass)

    def server_bind(self):
        if self.address_family == getattr(socket, "AF_INET6", object()):
            try:
                self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except Exception:
                pass
        super().server_bind()


class LocalCallbackServer:
    """Temporary local HTTP server used for OAuth redirect callbacks."""

    def __init__(self, redirect_uri: str) -> None:
        parsed = urlparse(redirect_uri)
        if parsed.scheme != "http":
            raise ValueError("Only http redirect URIs are supported for local callback server.")
        if not parsed.hostname or not parsed.port:
            raise ValueError("Redirect URI must include host and port, e.g. http://localhost:8000/callback.")
        self.redirect_uri = redirect_uri
        self.host = parsed.hostname
        self.port = int(parsed.port)
        self.path = parsed.path or "/callback"
        self.bind_host = "127.0.0.1" if self.host in {"localhost", "127.0.0.1"} else self.host

    def wait_for_callback(self, timeout: int = 180, tick_callback=None) -> OAuthCallbackPayload:
        payload = OAuthCallbackPayload()
        callback_event = threading.Event()
        callback_path = self.path

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # noqa: A003 - inherited API
                return

            def _send(self, status: int, body: str) -> None:
                data = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):  # noqa: N802 - inherited API
                parsed = urlparse(self.path)
                if parsed.path != callback_path:
                    self._send(404, "<html><body><h3>Not Found</h3></body></html>")
                    return
                query = parse_qs(parsed.query)
                payload.code = (query.get("code") or [None])[0]
                payload.state = (query.get("state") or [None])[0]
                payload.error = (query.get("error") or [None])[0]
                payload.error_description = (query.get("error_description") or [None])[0]
                callback_event.set()
                if payload.error:
                    self._send(200, "<html><body><h3>Login failed.</h3>You may close this tab.</body></html>")
                else:
                    self._send(200, "<html><body><h3>Login complete.</h3>You may close this tab.</body></html>")

        try:
            try:
                server = DualStackServer((self.bind_host, self.port), Handler)
            except OSError:
                # Fallback to standard IPv4 if dual-stack IPv6 failed
                server = HTTPServer(("127.0.0.1", self.port), Handler)
        except OSError as exc:
            raise RuntimeError(f"Could not start local callback server on {self.bind_host}:{self.port}: {exc}") from exc
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


class INatOAuthClient:
    """Desktop OAuth2 client for iNaturalist."""

    DEFAULT_CLIENT_ID = "bJW2eDa8qF8GJIQbQbuG_LBgmOQYRGMh9-Ja58QBqmc"
    DEFAULT_REDIRECT_URI = "http://localhost:8000/callback"
    AUTH_URL = "https://www.inaturalist.org/oauth/authorize"
    TOKEN_URL = "https://www.inaturalist.org/oauth/token"
    JWT_URL = "https://www.inaturalist.org/users/api_token"

    def __init__(
        self,
        client_id: str,
        client_secret: str | None = None,
        redirect_uri: str = DEFAULT_REDIRECT_URI,
        token_file: Path | str = "inaturalist_tokens.json",
        scope: str = "write",
        timeout_seconds: int = 20,
    ) -> None:
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip() or None
        self.redirect_uri = redirect_uri.strip()
        self.token_file = Path(token_file)
        self.scope = (scope or "write").strip()
        self.timeout_seconds = int(timeout_seconds)
        self._tokens = self._load_tokens()

    def _load_tokens(self) -> dict:
        if not self.token_file.exists():
            return {}
        try:
            with open(self.token_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_tokens(self) -> None:
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.token_file.with_suffix(self.token_file.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(self._tokens, handle, indent=2)
        os.replace(tmp_path, self.token_file)
        try:
            os.chmod(self.token_file, 0o600)
        except Exception:
            pass

    def clear_tokens(self) -> None:
        self._tokens = {}
        try:
            if self.token_file.exists():
                self.token_file.unlink()
        except Exception:
            pass

    def clear_api_token(self) -> None:
        """Forget only the short-lived iNaturalist API JWT."""
        for key in ("api_token", "api_token_created_at", "api_token_expires_at"):
            self._tokens.pop(key, None)
        self._save_tokens()

    def _auth_query(self, state: str, code_challenge: str | None = None) -> dict[str, str]:
        query = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": self.scope,
            "state": state,
        }
        if code_challenge:
            query["code_challenge"] = code_challenge
            query["code_challenge_method"] = "S256"
        return query

    def build_authorization_url(self, state: str, code_challenge: str | None = None) -> str:
        return f"{self.AUTH_URL}?{urlencode(self._auth_query(state, code_challenge))}"

    def _store_token_payload(self, token_payload: dict) -> None:
        if not isinstance(token_payload, dict):
            raise RuntimeError("Invalid token payload.")
        access_token = token_payload.get("access_token")
        if not access_token:
            raise RuntimeError("Token payload does not include access_token.")
        refresh_token = token_payload.get("refresh_token") or self._tokens.get("refresh_token")
        token_type = token_payload.get("token_type") or self._tokens.get("token_type") or "Bearer"
        expires_in = token_payload.get("expires_in")
        created_at = token_payload.get("created_at")
        now_ts = int(time.time())
        try:
            expires_in_i = int(expires_in) if expires_in is not None else None
        except (TypeError, ValueError):
            expires_in_i = None
        try:
            created_at_i = int(created_at) if created_at is not None else now_ts
        except (TypeError, ValueError):
            created_at_i = now_ts
        expires_at = (created_at_i + expires_in_i) if expires_in_i is not None else None
        self._tokens = {
            "access_token": str(access_token),
            "refresh_token": str(refresh_token) if refresh_token else "",
            "token_type": str(token_type),
            "expires_in": expires_in_i,
            "created_at": created_at_i,
            "expires_at": expires_at,
            "saved_at": now_ts,
            "api_token": self._tokens.get("api_token") or "",
            "api_token_created_at": self._tokens.get("api_token_created_at"),
            "api_token_expires_at": self._tokens.get("api_token_expires_at"),
        }
        self._save_tokens()

    def _store_api_token(self, api_token: str) -> None:
        now_ts = int(time.time())
        self._tokens["api_token"] = str(api_token or "")
        self._tokens["api_token_created_at"] = now_ts
        # iNaturalist API tokens are intentionally short lived. Refresh a bit early.
        self._tokens["api_token_expires_at"] = now_ts + (20 * 60 * 60)
        self._save_tokens()

    def exchange_code_for_tokens(self, code: str, code_verifier: str | None = None) -> dict:
        data = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code",
            "code": code,
        }
        if code_verifier:
            data["code_verifier"] = code_verifier
        elif self.client_secret:
            data["client_secret"] = self.client_secret
        else:
            raise RuntimeError("A client_secret or code_verifier is required for token exchange.")

        response = requests.post(self.TOKEN_URL, data=data, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            raise RuntimeError(f"Token exchange failed ({response.status_code}): {response.text}")
        payload = response.json()
        self._store_token_payload(payload)
        return payload

    def refresh_access_token(self) -> dict:
        refresh_token = (self._tokens.get("refresh_token") or "").strip()
        if not refresh_token:
            raise RuntimeError("No refresh_token available.")
        data = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        if self.client_secret:
            data["client_secret"] = self.client_secret
        response = requests.post(self.TOKEN_URL, data=data, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            raise RuntimeError(f"Token refresh failed ({response.status_code}): {response.text}")
        payload = response.json()
        self._store_token_payload(payload)
        return payload

    def authorize(self, open_browser: bool = True, timeout: int = 180, tick_callback=None) -> dict:
        if not self.client_id:
            raise RuntimeError("Missing client_id.")
        state = secrets.token_urlsafe(24)
        code_challenge = None
        self._code_verifier = None

        if not self.client_secret:
            self._code_verifier = secrets.token_urlsafe(32)
            hashed = hashlib.sha256(self._code_verifier.encode("utf-8")).digest()
            code_challenge = base64.urlsafe_b64encode(hashed).rstrip(b'=').decode("utf-8")

        url = self.build_authorization_url(state, code_challenge)
        callback_server = LocalCallbackServer(self.redirect_uri)
        if open_browser:
            print(f"Opening browser for iNaturalist login: {url}\n(If browser doesn't open automatically, ensure xdg-utils is installed on Linux)")
            webbrowser.open(url, new=2)
        callback = callback_server.wait_for_callback(timeout=timeout, tick_callback=tick_callback)
        if callback.error:
            desc = callback.error_description or callback.error
            raise RuntimeError(f"Authorization failed: {desc}")
        if not callback.code:
            raise RuntimeError("Authorization callback did not include a code.")
        if callback.state != state:
            raise RuntimeError("OAuth state mismatch.")
        return self.exchange_code_for_tokens(callback.code, self._code_verifier)

    def _is_expired(self, leeway_seconds: int = 60) -> bool:
        expires_at = self._tokens.get("expires_at")
        if expires_at is None:
            return False
        try:
            return time.time() >= (float(expires_at) - float(leeway_seconds))
        except Exception:
            return True

    def _api_token_is_expired(self, leeway_seconds: int = 60) -> bool:
        expires_at = self._tokens.get("api_token_expires_at")
        if expires_at is None:
            created_at = self._tokens.get("api_token_created_at")
            try:
                return time.time() >= (float(created_at) + (20 * 60 * 60) - float(leeway_seconds))
            except Exception:
                return True
        try:
            return time.time() >= (float(expires_at) - float(leeway_seconds))
        except Exception:
            return True

    def is_logged_in(self) -> bool:
        token = (self._tokens.get("access_token") or "").strip()
        refresh = (self._tokens.get("refresh_token") or "").strip()
        return bool(token) or bool(refresh)

    def get_valid_access_token(self) -> str | None:
        """Return a valid access token; refresh automatically when needed."""
        token = (self._tokens.get("access_token") or "").strip()
        if token and not self._is_expired():
            return token
        refresh = (self._tokens.get("refresh_token") or "").strip()
        if refresh:
            self.refresh_access_token()
            token = (self._tokens.get("access_token") or "").strip()
            return token or None
        return token or None

    def get_valid_api_token(self) -> str | None:
        """Return an iNaturalist API JWT, refreshing it from the saved login when possible."""
        api_token = (self._tokens.get("api_token") or "").strip()
        if api_token and not self._api_token_is_expired():
            return api_token

        access_token = self.get_valid_access_token()
        if not access_token:
            return None

        response = requests.get(
            self.JWT_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 401 and (self._tokens.get("refresh_token") or "").strip():
            self.refresh_access_token()
            access_token = self.get_valid_access_token()
            if access_token:
                response = requests.get(
                    self.JWT_URL,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=self.timeout_seconds,
                )
        if response.status_code >= 400:
            raise RuntimeError(f"iNaturalist API token refresh failed ({response.status_code}): {response.text}")
        payload = response.json()
        api_token = (payload.get("api_token") or "").strip() if isinstance(payload, dict) else ""
        if not api_token:
            raise RuntimeError("iNaturalist API token response did not include api_token.")
        self._store_api_token(api_token)
        return api_token


def example_login_and_print_access_token() -> None:
    client_id = os.getenv("INAT_CLIENT_ID", "").strip()
    client_secret = os.getenv("INAT_CLIENT_SECRET", "").strip() or None
    if not client_id:
        raise RuntimeError("Set INAT_CLIENT_ID first.")
    client = INatOAuthClient(
        client_id=client_id,
        client_secret=client_secret,  # Pass None for public clients
        redirect_uri="http://localhost:8000/callback",
        token_file=Path("inaturalist_tokens.json"),
    )
    client.authorize()
    token = client.get_valid_access_token()
    print(token or "No access token")


if __name__ == "__main__":
    example_login_and_print_access_token()
