"""F22: the iNaturalist helpers must not print access tokens or authorization URLs."""
from utils import inat_oauth
from utils.oauth_loopback import OAuthCallbackPayload

FAKE_TOKEN = "dummy-access-token-not-real"


def test_example_entry_point_does_not_print_the_access_token(monkeypatch, capsys, tmp_path):
    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def authorize(self):
            return {}

        def get_valid_access_token(self):
            return FAKE_TOKEN

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("INAT_CLIENT_ID", "dummy-client-id")
    monkeypatch.setattr(inat_oauth, "INatOAuthClient", FakeClient)
    inat_oauth.example_login_and_print_access_token()
    captured = capsys.readouterr()
    assert FAKE_TOKEN not in captured.out + captured.err


def test_authorize_does_not_print_the_authorization_url(monkeypatch, capsys, tmp_path):
    client = inat_oauth.INatOAuthClient(
        client_id="dummy-client-id",
        redirect_uri="http://127.0.0.1:8765/callback",
        token_file=tmp_path / "tokens.json",
    )

    class FakeServer:
        def __init__(self, redirect_uri):
            pass

        def wait_for_callback(self, timeout=180, tick_callback=None, expected_state=None):
            return OAuthCallbackPayload(code="dummy-code", state=expected_state)

    monkeypatch.setattr(inat_oauth, "LoopbackCallbackServer", FakeServer)
    monkeypatch.setattr(inat_oauth.webbrowser, "open", lambda url, new=0: True)
    monkeypatch.setattr(client, "exchange_code_for_tokens", lambda code, verifier: {})
    client.authorize(open_browser=True)
    captured = capsys.readouterr()
    text = captured.out + captured.err
    assert "inaturalist.org/oauth/authorize" not in text
    assert "code_challenge" not in text
    assert "state=" not in text
