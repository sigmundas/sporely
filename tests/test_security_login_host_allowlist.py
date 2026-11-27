"""F19: login helpers must not post the password to a host outside their allow-list."""
from urllib.parse import urlsplit

import pytest
import requests
from requests.adapters import HTTPAdapter

from utils import artportalen_auth, artsobservasjoner_auto_login as aso

PASSWORD = "dummy-password-not-real"
EVIL = "https://evil.invalid/collect"


def _page(action):
    return (
        f'<html><body><form action="{action}" method="post">'
        '<input type="hidden" name="token" value="t1">'
        '<input type="text" name="username">'
        '<input type="password" name="password">'
        "</form></body></html>"
    )


class FakeWeb:
    """Serves canned pages through HTTPAdapter.send and records every request."""

    def __init__(self, monkeypatch, routes):
        self.routes = routes
        self.sent = []
        monkeypatch.setattr(HTTPAdapter, "send", self._send)

    def _send(self, request, **kwargs):
        self.sent.append(request)
        key = (request.method, request.url.split("?")[0])
        status, headers, body = self.routes.get(key, (200, {}, "<html></html>"))
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body.encode()
        resp.url = request.url
        resp.request = request
        return resp

    def hosts(self):
        return [urlsplit(r.url).hostname for r in self.sent]

    def password_posted_to(self):
        return [
            urlsplit(r.url).hostname
            for r in self.sent
            if r.method == "POST" and PASSWORD in str(r.body or "")
        ]


def test_artportalen_does_not_post_password_to_foreign_form_action(monkeypatch):
    report = artportalen_auth.ArtportalenLogin.REPORT_URL
    web = FakeWeb(monkeypatch, {("GET", report): (200, {}, _page(EVIL))})
    login = artportalen_auth.ArtportalenLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert web.password_posted_to() == []
    assert "evil.invalid" not in web.hosts()


def test_artportalen_does_not_follow_redirect_off_the_allow_list(monkeypatch):
    report = artportalen_auth.ArtportalenLogin.REPORT_URL
    web = FakeWeb(monkeypatch, {("GET", report): (302, {"Location": EVIL}, "")})
    login = artportalen_auth.ArtportalenLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert "evil.invalid" not in web.hosts()


def test_artportalen_rejects_plain_http_form_action(monkeypatch):
    report = artportalen_auth.ArtportalenLogin.REPORT_URL
    web = FakeWeb(
        monkeypatch,
        {("GET", report): (200, {}, _page("http://useradmin-auth.slu.se/Account/Login"))},
    )
    login = artportalen_auth.ArtportalenLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert web.password_posted_to() == []


def test_artportalen_still_posts_to_its_identity_provider(monkeypatch):
    report = artportalen_auth.ArtportalenLogin.REPORT_URL
    idp = "https://useradmin-auth.slu.se/Account/Login"
    web = FakeWeb(monkeypatch, {("GET", report): (200, {}, _page(idp))})
    login = artportalen_auth.ArtportalenLogin()
    try:
        login.login("dummy-user", PASSWORD)
    except Exception:
        pass
    assert web.password_posted_to() == ["useradmin-auth.slu.se"]


def test_artsobs_mobile_does_not_post_password_to_foreign_form_action(monkeypatch):
    start = aso.ArtsObservasjonerMobileBffLogin.LOGIN_URL.split("?")[0]
    web = FakeWeb(monkeypatch, {("GET", start): (200, {}, _page(EVIL))})
    login = aso.ArtsObservasjonerMobileBffLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert web.password_posted_to() == []
    assert "evil.invalid" not in web.hosts()


def test_artsobs_mobile_does_not_follow_redirect_off_the_allow_list(monkeypatch):
    start = aso.ArtsObservasjonerMobileBffLogin.LOGIN_URL.split("?")[0]
    web = FakeWeb(monkeypatch, {("GET", start): (302, {"Location": EVIL}, "")})
    login = aso.ArtsObservasjonerMobileBffLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert "evil.invalid" not in web.hosts()


def test_artsobs_mobile_still_posts_to_its_identity_provider(monkeypatch):
    start = aso.ArtsObservasjonerMobileBffLogin.LOGIN_URL.split("?")[0]
    idp = "https://useradmin-auth.slu.se/Account/Login"
    web = FakeWeb(monkeypatch, {("GET", start): (200, {}, _page(idp))})
    login = aso.ArtsObservasjonerMobileBffLogin()
    try:
        login.login("dummy-user", PASSWORD)
    except Exception:
        pass
    assert web.password_posted_to() == ["useradmin-auth.slu.se"]


def test_artsobs_web_login_does_not_post_password_after_foreign_redirect(monkeypatch):
    logon = f"{aso.ArtsObservasjonerWebLogin.BASE_URL}/Logon"
    web = FakeWeb(monkeypatch, {("GET", logon): (302, {"Location": EVIL}, "")})
    login = aso.ArtsObservasjonerWebLogin()
    with pytest.raises(Exception):
        login.login("dummy-user", PASSWORD)
    assert web.password_posted_to() == []
    assert "evil.invalid" not in web.hosts()
