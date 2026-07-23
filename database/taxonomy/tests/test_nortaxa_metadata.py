import json
import socket
import sys
from dataclasses import replace
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from nortaxa_metadata import (  # noqa: E402
    AcquisitionError,
    HTML_TYPES,
    MAX_CUMULATIVE_BODY_BYTES,
    MetadataPolicy,
    Response,
    XML_TYPES,
    evidence_sha256,
    normalize_head,
    parse_eml,
    parse_resource_html,
    validate_response,
    verify,
)
from refresh_nortaxa import load_proposal, load_request  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nortaxa"


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("metadata fixture tests attempted network access")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


def selected():
    proposal = load_proposal()
    request = load_request(FIXTURES / "valid-request.json")
    policy = MetadataPolicy(allowed_host="ipt.artsdatabanken.no")
    return proposal, request, policy


def resource_body() -> bytes:
    return (FIXTURES / "synthetic-resource-page.html").read_bytes()


def eml_body() -> bytes:
    return (FIXTURES / "synthetic-eml.xml").read_bytes()


def response(method: str, url: str, content_type: str, body: bytes = b"", **headers) -> Response:
    return Response(
        method=method, requested_url=url, final_url=url, redirect_urls=(), status=200,
        headers={"Content-Type": content_type, **headers}, body=body,
    )


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, policy, body_budget):
        self.calls.append((method, url, body_budget))
        if not self.responses:
            raise AssertionError("unexpected retry")
        result = self.responses.pop(0)
        assert result.method == method and result.requested_url == url
        return result


def valid_transport():
    proposal, _, _ = selected()
    return FakeTransport([
        response("GET", proposal.resource_page, "text/html; charset=UTF-8", resource_body()),
        response("GET", proposal.eml_endpoint, "application/xml", eml_body()),
        response(
            "HEAD", proposal.archive_endpoint, "application/zip",
            **{"Content-Length": "1234567", "ETag": '"fixture"', "Last-Modified": "Fri, 17 Jul 2026 00:00:00 GMT",
               "Accept-Ranges": "bytes", "Content-Disposition": 'attachment; filename="dwca.zip"'},
        ),
    ])


def test_valid_sanitized_resource_and_eml_fixtures() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    evidence, sanitized = verify(proposal, request, transport, policy=policy, verified_at="2026-07-23T00:00:00Z")
    assert evidence["verdict"] == "passed"
    assert evidence["cumulative_body_bytes"] == len(resource_body()) + len(eml_body())
    assert [call[0] for call in transport.calls] == ["GET", "GET", "HEAD"]
    assert evidence["observed"]["archive_head"]["content_length"] == 1234567
    assert len(evidence_sha256(evidence)) == 64
    assert set(sanitized) == {"resource-page.sanitized.json", "eml.sanitized.json"}
    assert b"@" not in b"".join(sanitized.values())


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (b"1.284", b"9.999", "version"),
        (b"2026-07-17", b"2025-01-01", "issued_date"),
        (b"229018", b"1", "Taxon_count"),
        (b"58773", b"2", "VernacularName_count"),
        (b"CC-BY 4.0", b"All rights reserved", "license"),
        (b"a6c6cead-b5ce-4a4e-8cf5-1542ba708dec", b"00000000-0000-0000-0000-000000000000", "dataset_uuid"),
    ],
)
def test_proposal_response_mismatches(old: bytes, new: bytes, message: str) -> None:
    proposal, request, policy = selected()
    changed = resource_body().replace(old, new)
    transport = valid_transport()
    transport.responses[0] = replace(transport.responses[0], body=changed)
    transport.responses[1] = replace(transport.responses[1], body=eml_body().replace(old, new))
    evidence, _ = verify(proposal, request, transport, policy=policy)
    assert message in evidence["mismatches"]
    assert evidence["verdict"] == "mismatch"


def test_login_html_at_eml_endpoint_is_rejected() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[1] = response("GET", proposal.eml_endpoint, "text/html", b"<html><form>login</form></html>")
    with pytest.raises(AcquisitionError, match="Content-Type"):
        verify(proposal, request, transport, policy=policy)


def test_resource_page_may_contain_navigation_login_form() -> None:
    proposal = load_proposal()
    body = resource_body().replace(b"</body>", b'<form action="/login"><input type="password"></form></body>')
    assert parse_resource_html(body, proposal)["resource_key_present"] is True


def test_resource_login_page_without_selected_resource_is_rejected() -> None:
    proposal = load_proposal()
    with pytest.raises(AcquisitionError, match="login"):
        parse_resource_html(b'<!doctype html><html><form action="/login"><input type="password"></form></html>', proposal)


def test_each_response_is_recorded_before_parsing_and_failure_stops_sequence() -> None:
    proposal, request, policy = selected()
    login = b'<!doctype html><html><form action="/login"><input type="password"></form></html>'
    transport = valid_transport()
    transport.responses[0] = replace(transport.responses[0], body=login)
    recorded = []
    with pytest.raises(AcquisitionError, match="login"):
        verify(proposal, request, transport, policy=policy, response_recorder=recorded.append)
    assert len(recorded) == 1
    assert [call[0] for call in transport.calls] == ["GET"]


@pytest.mark.parametrize("payload", [
    b'<!DOCTYPE eml SYSTEM "https://example.org/x"><eml/>',
    b'<!ENTITY x SYSTEM "file:///etc/passwd"><eml>&x;</eml>',
])
def test_unsafe_xml_is_rejected(payload: bytes) -> None:
    with pytest.raises(AcquisitionError, match="unsafe"):
        parse_eml(payload, load_proposal())


def test_oversized_body_and_unexpected_content_type() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[0] = replace(
        transport.responses[0], body=b"x" * (MAX_CUMULATIVE_BODY_BYTES + 1),
    )
    with pytest.raises(AcquisitionError, match="cumulative|budget"):
        verify(proposal, request, transport, policy=policy)
    bad = response("GET", proposal.resource_page, "application/json", b"{}")
    with pytest.raises(AcquisitionError, match="Content-Type"):
        validate_response(bad, policy, HTML_TYPES)


@pytest.mark.parametrize("length", [None, "bad", "0", "67108865"])
def test_unsupported_or_excessive_archive_head(length: str | None) -> None:
    proposal, _, policy = selected()
    headers = {"Content-Type": "application/zip"}
    if length is not None:
        headers["Content-Length"] = length
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 200, headers, b"")
    with pytest.raises(AcquisitionError, match="Content-Length|length"):
        normalize_head(head, proposal, policy)


def test_missing_malformed_headers_and_redirect_host() -> None:
    proposal, _, policy = selected()
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 405,
                    {"Content-Type": "application/zip"}, b"")
    with pytest.raises(AcquisitionError, match="HTTP 405"):
        normalize_head(head, proposal, policy)
    redirected = response("GET", proposal.resource_page, "text/html", resource_body())
    redirected = replace(redirected, final_url="https://example.org/resource", redirect_urls=("https://example.org/resource",))
    with pytest.raises(AcquisitionError, match="HTTPS/host"):
        validate_response(redirected, policy, HTML_TYPES)


def test_duplicate_query_parameters_rejected() -> None:
    proposal, _, policy = selected()
    bad = response("GET", proposal.resource_page + "&v=1.284", "text/html", resource_body())
    with pytest.raises(AcquisitionError, match="query"):
        validate_response(bad, policy, HTML_TYPES)


def test_sanitization_does_not_retain_contact_details() -> None:
    proposal = load_proposal()
    body = resource_body().replace(
        b"</body>", b"<p>Contact: Person Name person@example.org +47 12345678</p></body>",
    )
    parsed = parse_resource_html(body, proposal)
    encoded = json.dumps(parsed)
    assert "person@example.org" not in encoded
    eml = eml_body().replace(
        b"</dataset>", b"<contact><individualName>Person Name</individualName><electronicMailAddress>person@example.org</electronicMailAddress></contact></dataset>",
    )
    assert "person@example.org" not in json.dumps(parse_eml(eml, proposal))


def test_immutable_proposal_and_request_hashes() -> None:
    proposal, request, _ = selected()
    assert proposal.canonical_sha256 == "e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3"
    assert request.request_sha256 == "38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392"
