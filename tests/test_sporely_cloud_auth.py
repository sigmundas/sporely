"""Tests for utils/sporely_cloud_auth.py — Stage 4 of desktop browser auth."""
from __future__ import annotations

import base64
import hashlib
import json
from unittest.mock import MagicMock, call, patch

import pytest
import requests as requests_lib

from utils.oauth_loopback import OAuthCallbackPayload
from utils.sporely_cloud_auth import (
    CLIENT_ID,
    REDIRECT_URI,
    OAuthError,
    OAuthTokenResult,
    SporelyDesktopOAuthClient,
    _AUTH_URL,
    _TOKEN_URL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _token_response(
    access_token: str = "acc_tok",
    refresh_token: str = "ref_tok",
    token_type: str = "bearer",
    expires_in: int | None = 3600,
    user_id: str | None = "uid-123",
    user_email: str | None = "user@example.com",
) -> dict:
    payload: dict = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
    }
    if expires_in is not None:
        payload["expires_in"] = expires_in
    if user_id or user_email:
        payload["user"] = {
            "id": user_id or "",
            "email": user_email or "",
        }
    return payload


def _mock_response(
    status_code: int = 200,
    payload: dict | None = None,
    text: str = "",
) -> MagicMock:
    resp = MagicMock(spec=requests_lib.Response)
    resp.status_code = status_code
    resp.text = text
    if payload is not None:
        resp.json.return_value = payload
    else:
        resp.json.side_effect = ValueError("No JSON")
    return resp


def _make_client(**kwargs) -> SporelyDesktopOAuthClient:
    return SporelyDesktopOAuthClient(timeout=10, network_timeout=5, **kwargs)


# ---------------------------------------------------------------------------
# build_authorization_url
# ---------------------------------------------------------------------------

class TestBuildAuthorizationUrl:
    def test_contains_response_type_code(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "ch1")
        assert "response_type=code" in url

    def test_contains_client_id(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "ch1")
        assert f"client_id={CLIENT_ID}" in url

    def test_contains_redirect_uri(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "ch1")
        assert "redirect_uri=" in url
        assert "127.0.0.1%3A8765" in url or "127.0.0.1:8765" in url

    def test_contains_code_challenge(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "mychallenge")
        assert "code_challenge=mychallenge" in url

    def test_contains_code_challenge_method_s256(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "ch1")
        assert "code_challenge_method=S256" in url

    def test_contains_state(self):
        client = _make_client()
        url = client.build_authorization_url("my_state_value", "ch1")
        assert "state=my_state_value" in url

    def test_url_starts_with_auth_url(self):
        client = _make_client()
        url = client.build_authorization_url("s1", "ch1")
        assert url.startswith(_AUTH_URL)

    def test_all_six_parameters_present(self):
        client = _make_client()
        url = client.build_authorization_url("state123", "challenge456")
        for param in (
            "response_type=code",
            f"client_id={CLIENT_ID}",
            "redirect_uri=",
            "code_challenge=challenge456",
            "code_challenge_method=S256",
            "state=state123",
        ):
            assert param in url, f"Missing parameter: {param}"


# ---------------------------------------------------------------------------
# PKCE verifier / challenge correctness
# ---------------------------------------------------------------------------

class TestPkce:
    def test_challenge_is_s256_of_verifier(self):
        verifier, challenge = SporelyDesktopOAuthClient.generate_pkce_pair()
        digest = hashlib.sha256(verifier.encode("utf-8")).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("utf-8")
        assert challenge == expected

    def test_verifier_length_within_rfc7636_bounds(self):
        verifier, _ = SporelyDesktopOAuthClient.generate_pkce_pair()
        assert 43 <= len(verifier) <= 128

    def test_verifier_is_urlsafe_characters(self):
        import re
        verifier, _ = SporelyDesktopOAuthClient.generate_pkce_pair()
        assert re.fullmatch(r"[A-Za-z0-9_\-]+", verifier), (
            f"Verifier contains non-URL-safe characters: {verifier}"
        )

    def test_challenge_is_base64url_no_padding(self):
        import re
        _, challenge = SporelyDesktopOAuthClient.generate_pkce_pair()
        assert "=" not in challenge
        assert re.fullmatch(r"[A-Za-z0-9_\-]+", challenge)

    def test_each_call_generates_different_pair(self):
        v1, c1 = SporelyDesktopOAuthClient.generate_pkce_pair()
        v2, c2 = SporelyDesktopOAuthClient.generate_pkce_pair()
        assert v1 != v2
        assert c1 != c2


# ---------------------------------------------------------------------------
# authorize — ordering: listener constructed before browser opened
# ---------------------------------------------------------------------------

class TestAuthorizeOrdering:
    def test_callback_server_constructed_before_browser_open(self):
        """LoopbackCallbackServer must be constructed before webbrowser.open."""
        call_order = []

        class TrackingServer:
            def __init__(self, uri):
                call_order.append("server_constructed")

            def wait_for_callback(self, *, timeout, tick_callback=None):
                return OAuthCallbackPayload(code="code123", state=_captured_state[0])

        _captured_state = [None]

        client = _make_client()

        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            _captured_state[0] = state
            return original_build(state, challenge)

        def tracking_browser(url, new=0):
            call_order.append("browser_opened")
            return True

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", TrackingServer),
            patch("utils.sporely_cloud_auth.webbrowser.open", tracking_browser),
            patch.object(client, "build_authorization_url", capturing_build),
            patch.object(client, "exchange_code", return_value=MagicMock(spec=OAuthTokenResult)),
        ):
            client.authorize(open_browser=True)

        assert call_order.index("server_constructed") < call_order.index("browser_opened"), (
            f"Expected server before browser, got: {call_order}"
        )


# ---------------------------------------------------------------------------
# authorize — success
# ---------------------------------------------------------------------------

class TestAuthorizeSuccess:
    def _run_happy_path(self, tick_callback=None):
        client = _make_client()
        captured_state = []

        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            captured_state.append(state)
            return original_build(state, challenge)

        fake_result = OAuthTokenResult(
            access_token="acc",
            refresh_token="ref",
            token_type="bearer",
            expires_at=9999999999,
            user_id="uid",
            user_email="u@e.com",
        )

        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            code="auth_code_xyz", state=captured_state[0]
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
            patch.object(client, "build_authorization_url", capturing_build),
            patch.object(client, "exchange_code", return_value=fake_result),
        ):
            result = client.authorize(open_browser=True, tick_callback=tick_callback)

        return result, fake_result

    def test_returns_oauth_token_result(self):
        result, _ = self._run_happy_path()
        assert isinstance(result, OAuthTokenResult)

    def test_returns_expected_result(self):
        result, expected = self._run_happy_path()
        assert result is expected

    def test_tick_callback_forwarded(self):
        ticks = []
        mock_server = MagicMock()
        captured_state = []
        client = _make_client()
        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            captured_state.append(state)
            return original_build(state, challenge)

        def tick():
            ticks.append(1)

        mock_server.wait_for_callback.side_effect = lambda **kw: (
            kw["tick_callback"]() or OAuthCallbackPayload(code="c", state=captured_state[0])
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
            patch.object(client, "build_authorization_url", capturing_build),
            patch.object(client, "exchange_code", return_value=MagicMock(spec=OAuthTokenResult)),
        ):
            client.authorize(open_browser=True, tick_callback=tick)

        assert ticks, "tick_callback was never invoked"


# ---------------------------------------------------------------------------
# authorize — OAuth denial
# ---------------------------------------------------------------------------

class TestAuthorizeDenial:
    def _run_with_error(self, error: str, error_description: str | None = None):
        client = _make_client()
        mock_server = MagicMock()
        captured_state = []
        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            captured_state.append(state)
            return original_build(state, challenge)

        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            error=error,
            error_description=error_description,
            state=captured_state[0],
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
            patch.object(client, "build_authorization_url", capturing_build),
        ):
            with pytest.raises(OAuthError) as exc_info:
                client.authorize(open_browser=True)

        return exc_info.value

    def test_access_denied_raises_oauth_error(self):
        err = self._run_with_error("access_denied")
        assert isinstance(err, OAuthError)

    def test_access_denied_message_says_cancelled(self):
        err = self._run_with_error("access_denied")
        assert "cancelled" in str(err).lower()

    def test_user_denied_message_says_cancelled(self):
        err = self._run_with_error("user_denied")
        assert "cancelled" in str(err).lower()

    def test_other_error_raises_oauth_error(self):
        err = self._run_with_error("server_error", "something internal")
        assert isinstance(err, OAuthError)


# ---------------------------------------------------------------------------
# authorize — missing code
# ---------------------------------------------------------------------------

class TestAuthorizeMissingCode:
    def test_missing_code_raises_oauth_error(self):
        client = _make_client()
        captured_state = []
        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            captured_state.append(state)
            return original_build(state, challenge)

        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            code=None, state=captured_state[0]
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
            patch.object(client, "build_authorization_url", capturing_build),
        ):
            with pytest.raises(OAuthError, match="code"):
                client.authorize(open_browser=True)


# ---------------------------------------------------------------------------
# authorize — state mismatch
# ---------------------------------------------------------------------------

class TestAuthorizeStateMismatch:
    def test_wrong_state_raises_oauth_error(self):
        client = _make_client()
        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            code="somecode", state="WRONG_STATE"
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
        ):
            with pytest.raises(OAuthError, match="[Ss]tate"):
                client.authorize(open_browser=True)

    def test_none_state_raises_oauth_error(self):
        client = _make_client()
        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            code="somecode", state=None
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
        ):
            with pytest.raises(OAuthError):
                client.authorize(open_browser=True)


# ---------------------------------------------------------------------------
# authorize — callback timeout
# ---------------------------------------------------------------------------

class TestAuthorizeTimeout:
    def test_timeout_raises_oauth_error(self):
        client = _make_client()
        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = TimeoutError("timed out")

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=True),
        ):
            with pytest.raises(OAuthError, match="[Tt]ime"):
                client.authorize(open_browser=True)


# ---------------------------------------------------------------------------
# authorize — occupied callback port
# ---------------------------------------------------------------------------

class TestAuthorizePortOccupied:
    def test_occupied_port_raises_runtime_error(self):
        client = _make_client()

        with patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=False):
            with pytest.raises(RuntimeError, match="[Pp]ort"):
                client.authorize(open_browser=True)

    def test_occupied_port_does_not_open_browser(self):
        client = _make_client()

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=False),
            patch("utils.sporely_cloud_auth.webbrowser.open") as mock_browser,
        ):
            with pytest.raises(RuntimeError):
                client.authorize(open_browser=True)
            mock_browser.assert_not_called()


# ---------------------------------------------------------------------------
# authorize — browser open failure
# ---------------------------------------------------------------------------

class TestAuthorizeBrowserFailure:
    def test_browser_exception_raises_runtime_error(self):
        client = _make_client()

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer"),
            patch(
                "utils.sporely_cloud_auth.webbrowser.open",
                side_effect=OSError("no browser"),
            ),
        ):
            with pytest.raises(RuntimeError, match="[Bb]rowser"):
                client.authorize(open_browser=True)

    def test_browser_returns_false_raises_runtime_error(self):
        client = _make_client()

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer"),
            patch("utils.sporely_cloud_auth.webbrowser.open", return_value=False),
        ):
            with pytest.raises(RuntimeError, match="[Bb]rowser"):
                client.authorize(open_browser=True)

    def test_open_browser_false_skips_browser_call(self):
        client = _make_client()
        captured_state = []
        original_build = client.build_authorization_url

        def capturing_build(state, challenge):
            captured_state.append(state)
            return original_build(state, challenge)

        mock_server = MagicMock()
        mock_server.wait_for_callback.side_effect = lambda **kw: OAuthCallbackPayload(
            code="c", state=captured_state[0]
        )

        with (
            patch("utils.sporely_cloud_auth.loopback_port_is_free", return_value=True),
            patch("utils.sporely_cloud_auth.LoopbackCallbackServer", return_value=mock_server),
            patch("utils.sporely_cloud_auth.webbrowser.open") as mock_browser,
            patch.object(client, "build_authorization_url", capturing_build),
            patch.object(client, "exchange_code", return_value=MagicMock(spec=OAuthTokenResult)),
        ):
            client.authorize(open_browser=False)

        mock_browser.assert_not_called()


# ---------------------------------------------------------------------------
# exchange_code — token endpoint HTTP failure
# ---------------------------------------------------------------------------

class TestExchangeCodeHttpFailure:
    def test_400_raises_runtime_error(self):
        client = _make_client()
        resp = _mock_response(400, text="bad_request")

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="400"):
                client.exchange_code("code", "verifier")

    def test_500_raises_runtime_error(self):
        client = _make_client()
        resp = _mock_response(500, text="server error")

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="500"):
                client.exchange_code("code", "verifier")

    def test_network_exception_raises_runtime_error(self):
        client = _make_client()

        with patch(
            "utils.sporely_cloud_auth.requests.post",
            side_effect=requests_lib.ConnectionError("refused"),
        ):
            with pytest.raises(RuntimeError, match="[Cc]onnection"):
                client.exchange_code("code", "verifier")

    def test_error_message_does_not_contain_code(self):
        client = _make_client()
        resp = _mock_response(400)

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError) as exc_info:
                client.exchange_code("SUPER_SECRET_CODE", "SUPER_SECRET_VERIFIER")

        msg = str(exc_info.value)
        assert "SUPER_SECRET_CODE" not in msg
        assert "SUPER_SECRET_VERIFIER" not in msg


# ---------------------------------------------------------------------------
# exchange_code — malformed token response
# ---------------------------------------------------------------------------

class TestExchangeCodeMalformed:
    def test_non_json_response_raises_runtime_error(self):
        client = _make_client()
        resp = _mock_response(200, payload=None, text="not json")

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="[Jj][Ss][Oo][Nn]"):
                client.exchange_code("code", "verifier")

    def test_json_array_raises_runtime_error(self):
        client = _make_client()
        resp = MagicMock(spec=requests_lib.Response)
        resp.status_code = 200
        resp.json.return_value = ["not", "a", "dict"]

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError):
                client.exchange_code("code", "verifier")

    def test_missing_access_token_raises_runtime_error(self):
        client = _make_client()
        resp = _mock_response(200, payload={"refresh_token": "r", "token_type": "bearer"})

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="access_token"):
                client.exchange_code("code", "verifier")

    def test_empty_access_token_raises_runtime_error(self):
        client = _make_client()
        resp = _mock_response(200, payload={"access_token": "", "refresh_token": "r"})

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError, match="access_token"):
                client.exchange_code("code", "verifier")


# ---------------------------------------------------------------------------
# exchange_code — success + OAuthTokenResult fields
# ---------------------------------------------------------------------------

class TestExchangeCodeSuccess:
    def test_returns_oauth_token_result(self):
        client = _make_client()
        payload = _token_response()
        resp = _mock_response(200, payload=payload)

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert isinstance(result, OAuthTokenResult)

    def test_access_token_set(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response(access_token="at_abc"))

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert result.access_token == "at_abc"

    def test_refresh_token_set(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response(refresh_token="rt_xyz"))

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert result.refresh_token == "rt_xyz"

    def test_expires_at_computed_from_expires_in(self):
        import time as time_mod

        client = _make_client()
        resp = _mock_response(200, payload=_token_response(expires_in=3600))

        before = int(time_mod.time())
        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")
        after = int(time_mod.time())

        assert result.expires_at is not None
        assert before + 3600 <= result.expires_at <= after + 3600

    def test_missing_refresh_token_returns_empty_string(self):
        client = _make_client()
        payload = {"access_token": "acc", "token_type": "bearer"}
        resp = _mock_response(200, payload=payload)

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert result.refresh_token == ""

    def test_user_id_and_email_parsed(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response(user_id="u-99", user_email="a@b.com"))

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert result.user_id == "u-99"
        assert result.user_email == "a@b.com"

    def test_no_user_block_returns_none_ids(self):
        client = _make_client()
        payload = {"access_token": "acc", "refresh_token": "ref", "token_type": "bearer"}
        resp = _mock_response(200, payload=payload)

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.exchange_code("code", "verifier")

        assert result.user_id is None
        assert result.user_email is None

    def test_token_url_is_oauth_endpoint(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp) as mock_post:
            client.exchange_code("code", "verifier")

        called_url = mock_post.call_args[0][0]
        assert "/auth/v1/oauth/token" in called_url
        assert "/auth/v1/token" not in called_url.replace("/auth/v1/oauth/token", "")

    def test_no_client_secret_in_request(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp) as mock_post:
            client.exchange_code("code", "verifier")

        sent_data = mock_post.call_args[1].get("data") or mock_post.call_args[0][1] if len(mock_post.call_args[0]) > 1 else mock_post.call_args[1].get("data", {})
        if isinstance(sent_data, dict):
            assert "client_secret" not in sent_data


# ---------------------------------------------------------------------------
# refresh — success
# ---------------------------------------------------------------------------

class TestRefreshSuccess:
    def test_returns_oauth_token_result(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.refresh("old_refresh_tok")

        assert isinstance(result, OAuthTokenResult)

    def test_access_token_updated(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response(access_token="new_access"))

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.refresh("old_rt")

        assert result.access_token == "new_access"

    def test_rotated_refresh_token_returned(self):
        """Rotated refresh token must be returned so caller can store it."""
        client = _make_client()
        resp = _mock_response(200, payload=_token_response(refresh_token="new_rt"))

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            result = client.refresh("old_rt")

        assert result.refresh_token == "new_rt"

    def test_grant_type_is_refresh_token(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp) as mock_post:
            client.refresh("rt_val")

        sent_data = mock_post.call_args[1].get("data") or {}
        assert sent_data.get("grant_type") == "refresh_token"

    def test_client_id_sent(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp) as mock_post:
            client.refresh("rt_val")

        sent_data = mock_post.call_args[1].get("data") or {}
        assert sent_data.get("client_id") == CLIENT_ID

    def test_no_client_secret_in_refresh_request(self):
        client = _make_client()
        resp = _mock_response(200, payload=_token_response())

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp) as mock_post:
            client.refresh("rt_val")

        sent_data = mock_post.call_args[1].get("data") or {}
        assert "client_secret" not in sent_data


# ---------------------------------------------------------------------------
# refresh — failure / invalid grant
# ---------------------------------------------------------------------------

class TestRefreshFailure:
    def test_400_raises_oauth_error(self):
        client = _make_client()
        resp = _mock_response(400, text='{"error":"invalid_grant"}')

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(OAuthError, match="[Rr]efresh|[Ii]nvalid|[Rr]evoked"):
                client.refresh("bad_rt")

    def test_401_raises_oauth_error(self):
        # Supabase returns 401 for expired/revoked refresh tokens; treat as
        # OAuthError (re-auth required) so Stage 5 shows the right UX.
        client = _make_client()
        resp = _mock_response(401, text="unauthorized")

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(OAuthError):
                client.refresh("bad_rt")

    def test_network_error_raises_runtime_error(self):
        client = _make_client()

        with patch(
            "utils.sporely_cloud_auth.requests.post",
            side_effect=requests_lib.Timeout("timed out"),
        ):
            with pytest.raises(RuntimeError, match="[Tt]ime"):
                client.refresh("rt_val")

    def test_refresh_token_not_in_error_message(self):
        client = _make_client()
        resp = _mock_response(400, text="error")

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(OAuthError) as exc_info:
                client.refresh("SUPER_SECRET_REFRESH_TOKEN")

        assert "SUPER_SECRET_REFRESH_TOKEN" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Security: tokens/codes never appear in exception messages
# ---------------------------------------------------------------------------

class TestSecurityNoLeakage:
    def test_access_token_not_in_malformed_error(self):
        client = _make_client()
        resp = MagicMock(spec=requests_lib.Response)
        resp.status_code = 200
        resp.json.return_value = {"access_token": "MY_PRECIOUS_ACCESS_TOKEN"}

        # access_token present but empty after strip would fail;
        # instead test a slightly different path: non-dict payload
        resp.json.return_value = ["MY_PRECIOUS_ACCESS_TOKEN"]

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises(RuntimeError) as exc_info:
                client.exchange_code("a_code", "a_verifier")

        assert "MY_PRECIOUS_ACCESS_TOKEN" not in str(exc_info.value)
        assert "a_code" not in str(exc_info.value)
        assert "a_verifier" not in str(exc_info.value)

    def test_http_error_does_not_include_response_body(self):
        """Raw Supabase error bodies must not leak to callers."""
        client = _make_client()
        sensitive_body = '{"error":"invalid_grant","hint":"Refresh token matches active pool"}'
        resp = _mock_response(400, text=sensitive_body)
        resp.json.side_effect = None
        resp.json.return_value = {"error": "invalid_grant"}

        with patch("utils.sporely_cloud_auth.requests.post", return_value=resp):
            with pytest.raises((RuntimeError, OAuthError)) as exc_info:
                client.exchange_code("code", "verifier")

        assert sensitive_body not in str(exc_info.value)
        assert "active pool" not in str(exc_info.value)
