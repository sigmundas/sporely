"""Sporely Desktop OAuth 2.1 + PKCE client.

Implements the authorization-code + PKCE flow against the Supabase OAuth
endpoint. Returns a validated OAuthTokenResult dataclass; never leaks raw
token values through logs or exception messages.

Security constraints enforced by design:
- No client secret (public client: token_endpoint_auth_method=none).
- No Supabase service-role key, Turnstile secret, or OAuth client secret.
- Authorization codes, PKCE verifiers, access tokens, and refresh tokens are
  never written to logs or included in exception messages.
- Raw Supabase error bodies from token endpoints are not forwarded to callers.
"""
from __future__ import annotations

import base64
import hashlib
import secrets
import time
import webbrowser
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

from utils.cloud_sync import SUPABASE_URL
from utils.oauth_loopback import LoopbackCallbackServer, loopback_port_is_free

CLIENT_ID = "b141fed6-e257-4de1-b784-3a28c777dadf"
REDIRECT_URI = "http://127.0.0.1:8765/auth/callback"
_CALLBACK_PORT = 8765
_AUTH_URL = f"{SUPABASE_URL}/auth/v1/oauth/authorize"
_TOKEN_URL = f"{SUPABASE_URL}/auth/v1/oauth/token"
_NETWORK_TIMEOUT = 30  # seconds


@dataclass
class OAuthTokenResult:
    """Validated token result from a Supabase OAuth exchange or refresh.

    Contains only what Stage 5 (SporelyCloudClient session integration) needs.
    Raw Supabase token response fields are not exposed.
    """

    access_token: str
    refresh_token: str  # Empty string if server omits it (non-rotating config); Stage 5 must preserve the old token in that case.
    token_type: str
    expires_at: int | None  # Unix timestamp; None when server omits expires_in
    user_id: str | None     # Supabase user UUID (from user.id in response)
    user_email: str | None  # Supabase user email (from user.email in response)


class OAuthError(RuntimeError):
    """OAuth flow error safe to surface to a UI.

    Message text never contains authorization codes, verifiers, or tokens.
    """


class SporelyDesktopOAuthClient:
    """Headless Supabase OAuth 2.1 + PKCE client for Sporely Desktop.

    Implements the authorization-code + PKCE flow and returns a validated
    OAuthTokenResult. Does NOT integrate with SporelyCloudClient or any UI
    (that is Stage 5).
    """

    def __init__(
        self,
        *,
        timeout: int = 180,
        network_timeout: int = _NETWORK_TIMEOUT,
    ) -> None:
        self._timeout = int(timeout)
        self._network_timeout = int(network_timeout)

    # ------------------------------------------------------------------
    # PKCE helpers
    # ------------------------------------------------------------------

    @staticmethod
    def generate_pkce_pair() -> tuple[str, str]:
        """Return (code_verifier, code_challenge) using S256.

        verifier: 64 bytes of entropy → 86-char base64url string.
        RFC 7636 requires 43–128 chars; 86 chars is well within bounds.
        """
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode("utf-8")).digest()
        challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("utf-8")
        return verifier, challenge

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_authorization_url(self, state: str, code_challenge: str) -> str:
        """Build the Supabase OAuth authorization URL."""
        params = {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "state": state,
        }
        return f"{_AUTH_URL}?{urlencode(params)}"

    def authorize(
        self,
        *,
        open_browser: bool = True,
        timeout: int | None = None,
        tick_callback=None,
    ) -> OAuthTokenResult:
        """Run the full authorization-code + PKCE flow.

        The loopback callback listener is constructed before the browser is
        opened. State and PKCE verifier are generated fresh for each call.

        Raises RuntimeError for infrastructure failures (port occupied, no
        browser) that the UI may want to handle distinctly.
        Raises OAuthError for user-visible OAuth failures (denial, timeout,
        state mismatch, missing code).
        """
        effective_timeout = timeout if timeout is not None else self._timeout

        if not loopback_port_is_free(_CALLBACK_PORT):
            raise RuntimeError(
                f"OAuth callback port {_CALLBACK_PORT} is already in use. "
                "Close any other Sporely window and try again."
            )

        state = secrets.token_urlsafe(32)
        code_verifier, code_challenge = self.generate_pkce_pair()
        url = self.build_authorization_url(state, code_challenge)

        # Construct the server before opening the browser so the port is
        # validated and ready to bind before any network activity starts.
        callback_server = LoopbackCallbackServer(REDIRECT_URI)

        if open_browser:
            try:
                opened = webbrowser.open(url, new=2)
            except Exception as exc:
                raise RuntimeError(
                    f"Could not open browser ({type(exc).__name__})."
                ) from exc
            if not opened:
                raise RuntimeError(
                    "No browser available (webbrowser.open returned False)."
                )

        try:
            payload = callback_server.wait_for_callback(
                timeout=effective_timeout, tick_callback=tick_callback
            )
        except TimeoutError as exc:
            raise OAuthError(
                "Timed out waiting for browser sign-in. Please try again."
            ) from exc

        # Validate state before consuming any other payload fields.
        if payload.state != state:
            raise OAuthError("OAuth state mismatch — authorization rejected.")

        if payload.error:
            if payload.error in ("access_denied", "user_denied"):
                raise OAuthError("Sign-in was cancelled.")
            # Do not forward the raw error_description: it may contain
            # server-internal detail or sensitive strings.
            raise OAuthError(f"Authorization failed ({payload.error}).")

        if not payload.code:
            raise OAuthError("Authorization callback did not include a code.")

        return self.exchange_code(payload.code, code_verifier)

    def exchange_code(self, code: str, code_verifier: str) -> OAuthTokenResult:
        """Exchange an authorization code for tokens at the Supabase OAuth endpoint.

        The code and verifier are never included in exception messages.
        """
        data = {
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "code": code,
            "code_verifier": code_verifier,
        }
        try:
            response = requests.post(
                _TOKEN_URL, data=data, timeout=self._network_timeout
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Token endpoint request failed ({type(exc).__name__})."
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(
                f"Token exchange failed (HTTP {response.status_code})."
            )

        return self._parse_token_response(response)

    def refresh(self, refresh_token: str) -> OAuthTokenResult:
        """Refresh an access token using a refresh token.

        Supports rotated refresh tokens: always use the refresh_token from the
        returned OAuthTokenResult for the next refresh call.

        The refresh token value is never included in exception messages.
        """
        data = {
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": refresh_token,
        }
        try:
            response = requests.post(
                _TOKEN_URL, data=data, timeout=self._network_timeout
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Token refresh request failed ({type(exc).__name__})."
            ) from exc

        if response.status_code in (400, 401):
            # Both 400 (invalid_grant) and 401 (expired/revoked token) indicate
            # the refresh token is no longer usable and the user must re-auth.
            raise OAuthError("Refresh token is invalid or has been revoked.")
        if response.status_code >= 400:
            raise RuntimeError(
                f"Token refresh failed (HTTP {response.status_code})."
            )

        return self._parse_token_response(response)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _parse_token_response(self, response: requests.Response) -> OAuthTokenResult:
        """Parse and validate a Supabase token endpoint JSON response.

        Raises RuntimeError for malformed or incomplete payloads.
        Token values are never included in error messages.
        """
        try:
            payload = response.json()
        except Exception:
            raise RuntimeError("Token endpoint returned a non-JSON response.")

        if not isinstance(payload, dict):
            raise RuntimeError("Token endpoint returned an unexpected response type.")

        access_token = (payload.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("Token response did not include an access_token.")

        refresh_token = (payload.get("refresh_token") or "").strip()
        token_type = (payload.get("token_type") or "bearer").strip()

        expires_at: int | None = None
        expires_in = payload.get("expires_in")
        if expires_in is not None:
            try:
                expires_at = int(time.time()) + int(expires_in)
            except (TypeError, ValueError):
                pass

        user_id: str | None = None
        user_email: str | None = None
        user = payload.get("user")
        if isinstance(user, dict):
            user_id = (user.get("id") or "").strip() or None
            user_email = (user.get("email") or "").strip() or None

        return OAuthTokenResult(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=token_type,
            expires_at=expires_at,
            user_id=user_id,
            user_email=user_email,
        )
