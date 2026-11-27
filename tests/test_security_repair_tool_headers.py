"""F16: the media repair tool must not send Supabase credentials to the public media host."""
import argparse
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from tools import repair_supabase_storage_media as repair

DUMMY_KEY = "dummy-service-key-not-real"


def _fake_response(request, status=200, body=b"[]"):
    resp = requests.Response()
    resp.status_code = status
    resp._content = body
    resp.headers["content-type"] = "application/json"
    resp.url = request.url
    resp.request = request
    return resp


def test_audit_never_sends_supabase_credentials_to_media_host(monkeypatch):
    sent = []

    def fake_send(self, request, **kwargs):
        sent.append(request)
        if "/rest/v1/observation_images" in request.url:
            return _fake_response(
                request,
                body=b'[{"id": "1", "observation_id": "2", "user_id": "u", "storage_path": "u/2/a.webp"}]',
            )
        return _fake_response(request, status=206, body=b"x")

    monkeypatch.setattr(HTTPAdapter, "send", fake_send)
    client = repair.AdminServiceRoleClient(DUMMY_KEY)
    args = argparse.Namespace(
        storage_path=None,
        legacy_upload_mode_null=False,
        cloud_observation_id="2",
        local_observation_id=None,
        all=False,
        limit=1,
        repair=False,
        soft_delete_stale_metadata=False,
        output_format="json",
        admin_service_role=True,
    )
    repair.run_audit(args, client=client)

    media_host = urlsplit(repair.R2_PUBLIC_BASE_URL).hostname
    media_requests = [r for r in sent if urlsplit(r.url).hostname == media_host]
    assert media_requests, "expected probes to the public media host"
    for req in media_requests:
        names = {k.lower() for k in req.headers}
        assert "apikey" not in names
        assert "authorization" not in names
    supabase_probes = [r for r in sent if "/storage/v1/object/" in r.url]
    assert supabase_probes and all(r.headers.get("apikey") for r in supabase_probes)


def test_r2_requests_strip_credentials_from_a_session_that_carries_them(monkeypatch):
    sent = []

    def fake_send(self, request, **kwargs):
        sent.append(request)
        return _fake_response(request, body=b"img")

    monkeypatch.setattr(HTTPAdapter, "send", fake_send)
    session = requests.Session()
    session.headers.update({"apikey": DUMMY_KEY, "Authorization": f"Bearer {DUMMY_KEY}"})
    url = repair._storage_object_url("u/2/a.webp", backend="r2")
    repair._download_r2_object(url, session)
    repair._probe_url(url, session)
    assert len(sent) == 2
    for req in sent:
        names = {k.lower() for k in req.headers}
        assert "apikey" not in names
        assert "authorization" not in names


def test_supabase_credentials_are_dropped_when_a_redirect_leaves_the_supabase_host(monkeypatch):
    sent = []
    supabase_url = repair._storage_object_url("u/2/a.webp", backend="supabase")

    def fake_send(self, request, **kwargs):
        sent.append(request)
        if request.url == supabase_url:
            resp = _fake_response(request, status=302, body=b"")
            resp.headers["Location"] = "https://other.invalid/object"
            return resp
        return _fake_response(request, status=206, body=b"x")

    monkeypatch.setattr(HTTPAdapter, "send", fake_send)
    session = repair.credential_safe_session()
    repair._probe_url(
        supabase_url,
        session,
        headers=repair._supabase_object_headers(DUMMY_KEY, DUMMY_KEY),
    )
    foreign = [r for r in sent if "other.invalid" in r.url]
    assert foreign, "expected the redirect to be followed"
    for req in foreign:
        names = {k.lower() for k in req.headers}
        assert "apikey" not in names
        assert "authorization" not in names
