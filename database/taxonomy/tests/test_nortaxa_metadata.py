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
    EML_EXPECTED_ROOT,
    HTML_TYPES,
    MAX_CUMULATIVE_BODY_BYTES,
    MAX_XML_PROLOG_BYTES,
    MetadataPolicy,
    OPERATION_NAMES,
    Response,
    VERIFICATION_STATE_SCHEMA,
    XML_TYPES,
    evaluate_archive_get,
    evidence_sha256,
    normalize_head,
    parse_content_length,
    parse_eml,
    parse_resource_html,
    replay_journal_state,
    validate_response,
    validate_xml_prolog,
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
    head_obs = evidence["observed"]["archive_head"]
    assert head_obs["content_length"] == 1234567
    assert head_obs["content_length_declared"] is True
    assert head_obs["size_declaration_status"] == "declared"
    assert head_obs["size_declaration_reason"] is None
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


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b'<!DOCTYPE eml:eml SYSTEM "https://example.org/x"><eml:eml/>', "SYSTEM/PUBLIC"),
        (b'<!DOCTYPE eml:eml PUBLIC "-//x//EN" "x.dtd"><eml:eml/>', "SYSTEM/PUBLIC"),
        (b'<!ENTITY x SYSTEM "file:///etc/passwd"><eml:eml>&x;</eml:eml>', "ENTITY"),
        (b'<!DOCTYPE eml:eml [<!ENTITY x "y">]><eml:eml>&x;</eml:eml>', "internal subset"),
        (b'<!DOCTYPE eml><eml/>', "root name"),
    ],
)
def test_unsafe_xml_is_rejected(payload: bytes, expected: str) -> None:
    with pytest.raises(AcquisitionError, match=expected):
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


@pytest.mark.parametrize(
    ("length", "match"),
    [
        ("bad", "malformed"),
        ("0", "positive"),
        ("-1", "malformed"),
        ("", "malformed"),
        ("12, 34", "conflicting"),
        ("67108865", "exceeds"),
        ("1 234 567", "malformed"),
        ("1.5e6", "malformed"),
    ],
)
def test_malformed_or_excessive_archive_head_length_still_rejected(length: str, match: str) -> None:
    proposal, _, policy = selected()
    headers = {"Content-Type": "application/zip", "Content-Length": length}
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 200, headers, b"")
    with pytest.raises(AcquisitionError, match=match):
        normalize_head(head, proposal, policy)


def test_archive_head_missing_content_length_is_accepted_as_unavailable() -> None:
    proposal, _, policy = selected()
    headers = {"Content-Type": "application/zip"}
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 200, headers, b"")
    parsed = normalize_head(head, proposal, policy)
    assert parsed["content_length"] is None
    assert parsed["content_length_declared"] is False
    assert parsed["size_declaration_status"] == "unavailable"
    assert parsed["size_declaration_reason"] == "header_absent"


def test_archive_head_boundary_equal_to_ceiling_is_accepted() -> None:
    proposal, _, policy = selected()
    ceiling = proposal.proposed_maximum_bytes
    headers = {"Content-Type": "application/zip", "Content-Length": str(ceiling)}
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 200, headers, b"")
    parsed = normalize_head(head, proposal, policy)
    assert parsed["content_length"] == ceiling
    assert parsed["size_declaration_status"] == "declared"


def test_archive_head_optional_evidence_fields_may_be_absent() -> None:
    proposal, _, policy = selected()
    headers = {"Content-Type": "application/zip"}  # no ETag/Last-Modified/Accept-Ranges/Disposition
    head = Response("HEAD", proposal.archive_endpoint, proposal.archive_endpoint, (), 200, headers, b"")
    parsed = normalize_head(head, proposal, policy)
    assert parsed["etag"] is None
    assert parsed["last_modified"] is None
    assert parsed["accept_ranges"] is None
    assert parsed["content_disposition"] is None
    assert parsed["size_declaration_status"] == "unavailable"


def test_verify_completes_when_archive_head_omits_content_length() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[2] = response("HEAD", proposal.archive_endpoint, "application/zip")
    evidence, sanitized = verify(proposal, request, transport, policy=policy)
    assert evidence["verdict"] == "passed"
    head = evidence["observed"]["archive_head"]
    assert head["content_length"] is None
    assert head["size_declaration_status"] == "unavailable"
    assert set(sanitized) == {"resource-page.sanitized.json", "eml.sanitized.json"}
    assert [call[0] for call in transport.calls] == ["GET", "GET", "HEAD"]


# ----- parse_content_length -----


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("1", 1),
        ("1234567", 1234567),
    ],
)
def test_parse_content_length_accepts_absent_or_positive(header, expected) -> None:
    assert parse_content_length(header, 67108864) == expected


@pytest.mark.parametrize(
    ("header", "match"),
    [
        ("", "malformed"),
        ("   ", "malformed"),
        ("bad", "malformed"),
        ("1.0", "malformed"),
        ("0", "positive"),
        ("-1", "malformed"),
        ("100, 200", "conflicting"),
        ("100, 100, 200", "conflicting"),
        ("67108865", "exceeds"),
    ],
)
def test_parse_content_length_rejects_malformed_or_over_ceiling(header, match) -> None:
    with pytest.raises(AcquisitionError, match=match):
        parse_content_length(header, 67108864)


def test_parse_content_length_repeated_identical_values_accepted() -> None:
    # Some proxies duplicate the header; if the values are identical it is not conflicting.
    assert parse_content_length("100, 100", 1000) == 100


def test_parse_content_length_requires_positive_ceiling() -> None:
    with pytest.raises(AcquisitionError, match="ceiling"):
        parse_content_length("1", 0)


# ----- evaluate_archive_get: GET-stream policy -----


CEILING = 1_000


def test_evaluate_get_declared_length_matches_completed_bytes() -> None:
    result = evaluate_archive_get(
        ceiling=CEILING, content_length_header="500",
        completed_bytes=500, reached_eof=True,
    )
    assert result["size_declaration_status"] == "declared"
    assert result["declared_length"] == 500
    assert result["completed_bytes"] == 500


def test_evaluate_get_missing_length_with_clean_eof_under_ceiling() -> None:
    result = evaluate_archive_get(
        ceiling=CEILING, content_length_header=None,
        completed_bytes=750, reached_eof=True,
    )
    assert result["size_declaration_status"] == "unavailable"
    assert result["declared_length"] is None
    assert result["completed_bytes"] == 750
    assert result["reached_eof"] is True


def test_evaluate_get_boundary_equality_declared() -> None:
    result = evaluate_archive_get(
        ceiling=CEILING, content_length_header=str(CEILING),
        completed_bytes=CEILING, reached_eof=True,
    )
    assert result["completed_bytes"] == CEILING
    assert result["declared_length"] == CEILING


def test_evaluate_get_boundary_equality_undeclared() -> None:
    result = evaluate_archive_get(
        ceiling=CEILING, content_length_header=None,
        completed_bytes=CEILING, reached_eof=True,
    )
    assert result["size_declaration_status"] == "unavailable"
    assert result["completed_bytes"] == CEILING


def test_evaluate_get_one_byte_overflow_declared_fails() -> None:
    with pytest.raises(AcquisitionError, match="exceeded the ceiling"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=str(CEILING),
            completed_bytes=CEILING + 1, reached_eof=True,
        )


def test_evaluate_get_one_byte_overflow_undeclared_fails() -> None:
    with pytest.raises(AcquisitionError, match="exceeded the ceiling"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=None,
            completed_bytes=CEILING + 1, reached_eof=True,
        )


def test_evaluate_get_truncated_declared_body_fails() -> None:
    with pytest.raises(AcquisitionError, match="disagrees with declared Content-Length"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header="500",
            completed_bytes=400, reached_eof=True,
        )


def test_evaluate_get_declared_but_stream_did_not_reach_eof_fails() -> None:
    with pytest.raises(AcquisitionError, match="before EOF"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header="500",
            completed_bytes=500, reached_eof=False,
        )


def test_evaluate_get_undeclared_and_no_eof_fails() -> None:
    with pytest.raises(AcquisitionError, match="before EOF"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=None,
            completed_bytes=500, reached_eof=False,
        )


@pytest.mark.parametrize(
    ("header", "match"),
    [
        ("bad", "malformed"),
        ("0", "positive"),
        ("-5", "malformed"),
        ("100, 200", "conflicting"),
        (str(CEILING + 1), "exceeds"),
    ],
)
def test_evaluate_get_rejects_malformed_or_over_ceiling_declaration(header, match) -> None:
    with pytest.raises(AcquisitionError, match=match):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=header,
            completed_bytes=500, reached_eof=True,
        )


def test_evaluate_get_server_ignored_range_full_body_ok() -> None:
    # Caller had asked for a Range, but the server ignored it and streamed the
    # full body. Policy must still accept it purely on the ceiling/length rules,
    # because Range is not part of the declared-size decision.
    result = evaluate_archive_get(
        ceiling=CEILING, content_length_header=str(CEILING),
        completed_bytes=CEILING, reached_eof=True,
    )
    assert result["declared_length"] == CEILING


def test_evaluate_get_server_ignored_range_full_body_over_ceiling_rejected() -> None:
    with pytest.raises(AcquisitionError, match="exceeded the ceiling"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=None,
            completed_bytes=CEILING + 1, reached_eof=True,
        )


def test_evaluate_get_ceiling_must_be_positive_integer() -> None:
    with pytest.raises(AcquisitionError, match="ceiling"):
        evaluate_archive_get(
            ceiling=0, content_length_header=None,
            completed_bytes=0, reached_eof=True,
        )


def test_evaluate_get_completed_bytes_must_be_non_negative() -> None:
    with pytest.raises(AcquisitionError, match="completed_bytes"):
        evaluate_archive_get(
            ceiling=CEILING, content_length_header=None,
            completed_bytes=-1, reached_eof=True,
        )


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


# ----- Declaration-aware XML prolog policy -----

MINIMAL_BODY = b"<eml:eml/>"
MINIMAL_DECL = b'<?xml version="1.0" encoding="UTF-8"?>\n<eml:eml/>'
NS_BODY = b'<eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"/>'
NS_DECL = b'<?xml version="1.0" encoding="UTF-8"?>\n<eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1"/>'


@pytest.mark.parametrize(
    "payload",
    [
        MINIMAL_BODY,
        MINIMAL_DECL,
        b"<!DOCTYPE eml:eml>\n<eml:eml/>",
        b'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE eml:eml>\n<eml:eml/>',
        b'<?xml version="1.0" encoding="UTF-8"?>\n<!-- ordinary comment -->\n<eml:eml/>',
        b'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE eml:eml    >\n<eml:eml/>',
        b'<?xml version="1.0" encoding="UTF-8"?>\n<eml:eml>SYSTEM PUBLIC are ordinary words</eml:eml>',
        b"\xef\xbb\xbf" + MINIMAL_DECL,
    ],
)
def test_prolog_policy_accepts_documented_shapes(payload: bytes) -> None:
    result = validate_xml_prolog(payload)
    assert result["expected_root"] == EML_EXPECTED_ROOT
    assert result["root_element"] == EML_EXPECTED_ROOT


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (b'<!DOCTYPE eml:eml SYSTEM "u"><eml:eml/>', "SYSTEM/PUBLIC"),
        (b'<!DOCTYPE eml:eml PUBLIC "-//x//EN" "x.dtd"><eml:eml/>', "SYSTEM/PUBLIC"),
        (b'<!DOCTYPE eml:eml [<!ENTITY x "y">]><eml:eml/>', "internal subset"),
        (b'<!ENTITY x "y"><eml:eml/>', "ENTITY"),
        (b"<!DOCTYPE %pe;><eml:eml/>", "root name"),
        (b'<!DOCTYPE eml:eml><!DOCTYPE eml:eml><eml:eml/>', "multiple DOCTYPE"),
        (b"<!DOCTYPE eml><eml/>", "root name"),
        (b'<!-- <!DOCTYPE eml:eml SYSTEM "u"> --><!DOCTYPE eml:eml SYSTEM "u"><eml:eml/>', "SYSTEM/PUBLIC"),
        (b"<!DOCTYPE eml:eml<eml:eml/>", "root name is malformed"),
        (b"<?xml-stylesheet href=\"x.xsl\"?><eml:eml/>", "processing instructions"),
        (b"<!NOTATION jpeg SYSTEM \"x\"><eml:eml/>", "NOTATION"),
        (b"<![CDATA[oops]]><eml:eml/>", "CDATA"),
        (b"<xi:include href=\"x\"/><eml:eml/>", "root element must be"),  # rejected by root-element check
        (b"\xff\xfe" + MINIMAL_BODY, "UTF-16"),
        (b"\x00" + MINIMAL_BODY, "NUL byte"),
        (b"<?xml version=\"1.0\" encoding=\"UTF-16\"?><eml:eml/>", "must be UTF-8"),
        (b"<?xml version=\"9.9\"?><eml:eml/>", "version"),
    ],
)
def test_prolog_policy_rejects_forbidden_shapes(payload: bytes, match: str) -> None:
    with pytest.raises(AcquisitionError, match=match):
        validate_xml_prolog(payload)


def test_prolog_policy_rejects_oversized_prolog() -> None:
    filler = b"<!-- " + (b"x" * (MAX_XML_PROLOG_BYTES + 32)) + b" -->\n<eml:eml/>"
    with pytest.raises(AcquisitionError, match="prolog|comment"):
        validate_xml_prolog(filler)


def test_prolog_policy_rejects_billion_laughs_entity_declaration() -> None:
    payload = (
        b"<!DOCTYPE eml:eml [\n"
        b"  <!ENTITY a \"aaaaaaaaaa\">\n"
        b"  <!ENTITY b \"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\">\n"
        b"]>\n<eml:eml>&b;</eml:eml>"
    )
    with pytest.raises(AcquisitionError, match="internal subset"):
        validate_xml_prolog(payload)


def test_parse_eml_rejects_entity_declaration_smuggled_after_prolog() -> None:
    payload = NS_DECL[:-2] + b'><!ENTITY x SYSTEM "u"/></eml:eml>'
    with pytest.raises(AcquisitionError, match="ENTITY|DOCTYPE"):
        parse_eml(payload, load_proposal())


def test_parse_eml_rejects_xinclude_directive() -> None:
    payload = NS_DECL[:-2] + b'><xi:include href="http://example/"/></eml:eml>'
    with pytest.raises(AcquisitionError, match="XInclude"):
        parse_eml(payload, load_proposal())


def test_parse_eml_rejects_doctype_after_root() -> None:
    payload = NS_DECL + b"<!DOCTYPE eml:eml>"
    with pytest.raises(AcquisitionError, match="DOCTYPE|malformed"):
        parse_eml(payload, load_proposal())


def test_parse_eml_element_text_may_mention_system_or_public() -> None:
    body = NS_DECL[:-2] + b"><title>Uses SYSTEM tables and PUBLIC data</title></eml:eml>"
    parsed = parse_eml(body, load_proposal())
    assert parsed["title"] == "Uses SYSTEM tables and PUBLIC data"


# ----- Per-operation atomic parsed-result journaling -----


def _snapshots(sink_log):
    """Deep-copy each state emitted by verify()'s journal_sink."""
    import copy
    return [copy.deepcopy(state) for state in sink_log]


def _record_sink():
    log: list = []

    def sink(state):
        log.append(state)

    return log, sink


def test_verify_journals_each_transition_in_order() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    log, sink = _record_sink()
    _, snapshots = None, None
    verify(proposal, request, transport, policy=policy, verified_at="2026-07-24T00:00:00Z",
           journal_sink=lambda s: log.append({k: (dict(v) if isinstance(v, dict) else v)
                                              for k, v in s["operations"].items()} | {"final": s["final"], "verdict": s["verdict"]}))
    statuses = [{name: op["status"] for name, op in entry.items() if name in OPERATION_NAMES} for entry in log]
    # Initial snapshot: all pending
    assert statuses[0] == {name: "pending" for name in OPERATION_NAMES}
    # Final snapshot: all parse_succeeded
    assert statuses[-1] == {name: "parse_succeeded" for name in OPERATION_NAMES}
    assert log[-1]["final"] is True and log[-1]["verdict"] == "passed"


def test_resource_transport_failure_prevents_parsing_and_later_operations() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[0] = replace(transport.responses[0], status=500)
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    final = log[-1]["operations"]
    assert final["resource_page"]["status"] == "transport_failed"
    assert final["eml"]["status"] == "skipped"
    assert final["archive_head"]["status"] == "skipped"
    assert [call[0] for call in transport.calls] == ["GET"]


def test_resource_parse_failure_prevents_eml_and_head() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    login = b'<!doctype html><html><form action="/login"><input type="password"></form></html>'
    transport.responses[0] = replace(transport.responses[0], body=login)
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError, match="login"):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    final = log[-1]["operations"]
    assert final["resource_page"]["status"] == "parse_failed"
    assert "transport" in final["resource_page"]
    assert final["eml"]["status"] == "skipped"
    assert final["archive_head"]["status"] == "skipped"
    assert [call[0] for call in transport.calls] == ["GET"]


def test_resource_parsed_evidence_survives_eml_transport_failure() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[1] = replace(transport.responses[1], status=502)
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    final = log[-1]["operations"]
    assert final["resource_page"]["status"] == "parse_succeeded"
    assert final["resource_page"]["parsed"]["resource_key_present"] is True
    assert final["eml"]["status"] == "transport_failed"
    assert final["archive_head"]["status"] == "skipped"


def test_resource_parsed_evidence_survives_eml_parse_failure() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[1] = replace(
        transport.responses[1],
        body=b'<!DOCTYPE eml:eml SYSTEM "https://evil.example/x"><eml:eml/>',
    )
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError, match="SYSTEM/PUBLIC"):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    final = log[-1]["operations"]
    assert final["resource_page"]["status"] == "parse_succeeded"
    assert final["resource_page"]["parsed"]["resource_key_present"] is True
    assert final["eml"]["status"] == "parse_failed"
    assert "transport" in final["eml"]
    assert final["archive_head"]["status"] == "skipped"


def test_resource_and_eml_survive_archive_head_transport_failure() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[2] = replace(transport.responses[2], status=503)
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    final = log[-1]["operations"]
    assert final["resource_page"]["status"] == "parse_succeeded"
    assert final["eml"]["status"] == "parse_succeeded"
    assert final["archive_head"]["status"] == "transport_failed"
    assert log[-1]["final"] is False and log[-1]["verdict"] is None


def test_final_verification_absent_when_any_operation_incomplete() -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    transport.responses[1] = replace(transport.responses[1], status=500)
    log, sink = _record_sink()
    with pytest.raises(AcquisitionError):
        verify(proposal, request, transport, policy=policy, journal_sink=sink)
    assert log[-1]["final"] is False


def test_replay_journal_state_is_deterministic_and_networkless(monkeypatch) -> None:
    proposal, request, policy = selected()
    transport = valid_transport()
    log, sink = _record_sink()
    import copy
    verify(proposal, request, transport, policy=policy, verified_at="2026-07-24T00:00:00Z",
           journal_sink=lambda s: log.append(copy.deepcopy(s)))
    final_state = log[-1]
    # Replay is a pure function of the persisted state and never touches the transport.
    first = replay_journal_state(final_state)
    second = replay_journal_state(final_state)
    assert first == second
    assert first["final"] is True and first["verdict"] == "passed"
    for name in OPERATION_NAMES:
        assert first["operations"][name]["status"] == "parse_succeeded"
        assert first["operations"][name]["parsed_available"] is True


def test_replay_rejects_unsupported_schema() -> None:
    with pytest.raises(AcquisitionError, match="schema"):
        replay_journal_state({"verification_state_schema_version": 999, "operations": {}})


def test_state_schema_and_operation_names_are_frozen() -> None:
    assert VERIFICATION_STATE_SCHEMA == 2
    assert OPERATION_NAMES == ("resource_page", "eml", "archive_head")


def test_attempt_1_and_attempt_2_records_are_unchanged() -> None:
    import hashlib
    RELEASE = Path(__file__).resolve().parents[1] / "sources" / "nortaxa" / "1.284"

    def canonical(path):
        data = json.loads(path.read_text())
        canon = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        return hashlib.sha256(canon).hexdigest()

    assert canonical(RELEASE / "metadata-verification-attempt-1.json") == \
        "9665bb1ed16958830304e753dfdb73829bc9383b45d67ee9bc4dc332c66e067a"
    assert canonical(RELEASE / "metadata-verification-attempt-2.json") == \
        "92ab2958c151eab417da4d29084682293002950bdc5a72b1c0caaf8a48c66ad9"
