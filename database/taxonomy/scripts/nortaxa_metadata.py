#!/usr/bin/env python3
"""Bounded metadata-only verification for a selected NorTaxa IPT release."""

from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from xml.etree import ElementTree

from refresh_col_xr import AcquisitionError, sha256_json
from refresh_nortaxa import NorTaxaRequest, SourceProposal

MAX_CUMULATIVE_BODY_BYTES = 4_194_304
MAX_REDIRECTS = 3
TIMEOUT_SECONDS = 20
HTML_TYPES = {"text/html", "application/xhtml+xml"}
XML_TYPES = {"application/xml", "text/xml", "application/eml+xml"}
ZIP_TYPES = {
    "application/zip", "application/octet-stream", "application/x-zip-compressed",
    "application/zip-compressed",
}
USER_AGENT = "Sporely-Taxonomy-Metadata/1"

EML_EXPECTED_ROOT = "eml:eml"
MAX_XML_PROLOG_BYTES = 4096
MAX_XML_DECLARATION_BYTES = 256
MAX_DOCTYPE_BYTES = 256
MAX_COMMENT_BYTES = 4096
_NAME_RE = rb"[A-Za-z_][A-Za-z0-9_.\-]*(?::[A-Za-z_][A-Za-z0-9_.\-]*)?"
_DOCTYPE_ROOT_RE = re.compile(rb"^(" + _NAME_RE + rb")\s*$")
_ROOT_ELEMENT_RE = re.compile(rb"<(" + _NAME_RE + rb")(?:[\s/>]|$)")
_XML_DECL_ENCODING_RE = re.compile(rb"""encoding\s*=\s*['"]([^'"]{1,64})['"]""")
_XML_DECL_VERSION_RE = re.compile(rb"""version\s*=\s*['"]([^'"]{1,16})['"]""")
_UTF8_BOM = b"\xef\xbb\xbf"
_FORBIDDEN_BOMS = (
    b"\xff\xfe\x00\x00",  # UTF-32 LE
    b"\x00\x00\xfe\xff",  # UTF-32 BE
    b"\xff\xfe",          # UTF-16 LE
    b"\xfe\xff",          # UTF-16 BE
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_https_url(url: str, allowed_host: str) -> None:
    parsed = urlparse(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise AcquisitionError("metadata URL port is invalid") from exc
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    keys = [key for key, _ in pairs]
    if (
        parsed.scheme != "https" or parsed.hostname != allowed_host
        or parsed.username or parsed.password or port not in {None, 443}
        or parsed.fragment or len(keys) != len(set(keys))
    ):
        raise AcquisitionError("metadata URL violates the approved HTTPS/host/query policy")


@dataclass(frozen=True)
class MetadataPolicy:
    allowed_host: str
    timeout_seconds: int = TIMEOUT_SECONDS
    max_redirects: int = MAX_REDIRECTS
    cumulative_max_bytes: int = MAX_CUMULATIVE_BODY_BYTES


@dataclass(frozen=True)
class Response:
    method: str
    requested_url: str
    final_url: str
    redirect_urls: tuple[str, ...]
    status: int
    headers: dict[str, str]
    body: bytes


class Transport(Protocol):
    def request(self, method: str, url: str, policy: MetadataPolicy, body_budget: int) -> Response: ...


class _Redirects(HTTPRedirectHandler):
    def __init__(self, method: str, policy: MetadataPolicy):
        self.method = method
        self.policy = policy
        self.urls: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_https_url(newurl, self.policy.allowed_host)
        if newurl in [req.full_url, *self.urls]:
            raise AcquisitionError("metadata redirect loop detected")
        self.urls.append(newurl)
        if len(self.urls) > self.policy.max_redirects:
            raise AcquisitionError("metadata redirect limit exceeded")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            redirected.method = self.method
        return redirected


class UrllibTransport:
    def request(self, method: str, url: str, policy: MetadataPolicy, body_budget: int) -> Response:
        if method not in {"GET", "HEAD"}:
            raise AcquisitionError("metadata transport supports only GET and HEAD")
        validate_https_url(url, policy.allowed_host)
        redirects = _Redirects(method, policy)
        opener = build_opener(redirects)
        accept = "text/html" if method == "GET" and "/resource" in url else "application/xml,text/xml"
        request = Request(url, method=method, headers={"Accept": accept, "User-Agent": USER_AGENT})
        try:
            with opener.open(request, timeout=policy.timeout_seconds) as handle:
                length_text = handle.headers.get("Content-Length")
                length = int(length_text) if length_text and length_text.isdigit() else None
                if method == "GET" and length is not None and length > body_budget:
                    raise AcquisitionError("metadata Content-Length exceeds remaining cumulative budget")
                body = b"" if method == "HEAD" else handle.read(body_budget + 1)
                if len(body) > body_budget:
                    raise AcquisitionError("metadata body exceeds remaining cumulative budget")
                return Response(
                    method=method, requested_url=url, final_url=handle.geturl(),
                    redirect_urls=tuple(redirects.urls), status=handle.status,
                    headers=dict(handle.headers.items()), body=body,
                )
        except AcquisitionError:
            raise
        except HTTPError as exc:
            raise AcquisitionError(f"{method} metadata endpoint returned HTTP {exc.code}") from exc
        except (OSError, URLError) as exc:
            raise AcquisitionError(f"{method} metadata request failed: {exc}") from exc


class _Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs):
        if tag.casefold() in {"script", "style"}:
            self.hidden += 1

    def handle_endtag(self, tag: str):
        if tag.casefold() in {"script", "style"} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data: str):
        if not self.hidden and data.strip():
            self.parts.append(data.strip())


def _media_type(headers: dict[str, str]) -> str:
    values = {key.casefold(): value for key, value in headers.items()}
    return values.get("content-type", "").split(";", 1)[0].strip().casefold()


def validate_response(response: Response, policy: MetadataPolicy, allowed_types: set[str]) -> None:
    if response.status != 200:
        raise AcquisitionError(f"metadata endpoint returned HTTP {response.status}")
    if len(response.redirect_urls) > policy.max_redirects:
        raise AcquisitionError("metadata redirect limit exceeded")
    validate_https_url(response.requested_url, policy.allowed_host)
    validate_https_url(response.final_url, policy.allowed_host)
    for redirect in response.redirect_urls:
        validate_https_url(redirect, policy.allowed_host)
    media = _media_type(response.headers)
    if media not in allowed_types and not (allowed_types is XML_TYPES and media.endswith("+xml")):
        raise AcquisitionError(f"unexpected metadata Content-Type: {media!r}")


def parse_resource_html(body: bytes, proposal: SourceProposal) -> dict[str, Any]:
    try:
        decoded = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise AcquisitionError("resource page is not strict UTF-8") from exc
    prefix = decoded.lstrip()[:256].casefold()
    if "<html" not in prefix and "<!doctype html" not in prefix:
        raise AcquisitionError("resource endpoint did not return HTML")
    login_markup = re.search(r"<form\b[^>]*(login|signin)|type=[\"']password", decoded, re.I)
    selection_markup = proposal.title.casefold() in decoded.casefold() or proposal.resource_key.casefold() in decoded.casefold()
    if login_markup and not selection_markup:
        raise AcquisitionError("resource endpoint returned an HTML login page")
    parser = _Text()
    parser.feed(decoded)
    text = html.unescape(" ".join(parser.parts))
    compact = re.sub(r"\s+", " ", text).strip()

    def present(value: Any) -> bool:
        return str(value) in compact

    counts: dict[str, int] = {}
    for label in ("Taxon", "VernacularName"):
        patterns = [
            rf"{label}\s*(?:count)?\s*[:=]?\s*([0-9][0-9, ]*)",
            rf"([0-9][0-9, ]*)\s*{label}",
        ]
        for pattern in patterns:
            match = re.search(pattern, compact, re.I)
            if match:
                counts[label] = int(re.sub(r"[^0-9]", "", match.group(1)))
                break
    publisher_match = re.search(
        r"(?:publisher|publishing organisation|organization)\s*[:=]?\s*([^|]{2,160}?)(?=\s+(?:license|rights|update|frequency|version|published|$))",
        compact, re.I,
    )
    license_match = re.search(
        r"(?:license|rights)\s*[:=]?\s*(CC[- ]?BY(?:\s*4\.0)?|Creative Commons Attribution|All rights reserved)",
        compact, re.I,
    )
    uuid_match = re.search(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", compact, re.I)
    return {
        "text_sha256": hashlib.sha256(compact.encode("utf-8")).hexdigest(),
        "resource_key_present": present(proposal.resource_key),
        "title_present": present(proposal.title),
        "version_present": present(proposal.version),
        "issued_date_present": present(proposal.issued_date),
        "counts": counts,
        "weekly_present": bool(re.search(r"\bweekly\b", compact, re.I)),
        "license_present": bool(re.search(r"CC[- ]?BY(?:\s*4\.0)?|Creative Commons Attribution", compact, re.I)),
        "observed_license": license_match.group(1).strip() if license_match else None,
        "dataset_uuid_present": present(proposal.dataset_uuid),
        "observed_dataset_uuid": uuid_match.group(0).lower() if uuid_match else None,
        "publisher": publisher_match.group(1).strip() if publisher_match else None,
    }


def validate_xml_prolog(body: bytes, expected_root: str = EML_EXPECTED_ROOT) -> dict[str, Any]:
    """Bounded, declaration-aware inspection of the XML prolog.

    Never resolves, loads, or fetches an external DTD, entity, or resource. Raises
    ``AcquisitionError`` on any policy violation. The raw body is not modified.
    """
    if not body:
        raise AcquisitionError("XML body is empty")
    for bom in _FORBIDDEN_BOMS:
        if body.startswith(bom):
            raise AcquisitionError("XML encoding must be UTF-8; UTF-16/UTF-32 BOM rejected")
    start = 3 if body.startswith(_UTF8_BOM) else 0
    window_end = min(len(body), start + MAX_XML_PROLOG_BYTES)
    if b"\x00" in body[start:window_end]:
        raise AcquisitionError("NUL byte in XML prolog")

    cursor = start
    has_xml_declaration = False
    xml_declaration_encoding: str | None = None
    xml_declaration_version: str | None = None
    has_doctype = False
    doctype_root: str | None = None
    root_element_name: str | None = None
    accepted_comments = 0

    def _at(index: int, needle: bytes) -> bool:
        return body[index : index + len(needle)] == needle

    while True:
        # No whitespace is legally accepted before the XML declaration.
        if has_xml_declaration or cursor > start:
            while cursor < window_end and body[cursor : cursor + 1] in (b" ", b"\t", b"\n", b"\r"):
                cursor += 1
        if cursor >= window_end:
            raise AcquisitionError("no root element found within bounded XML prolog")
        if body[cursor : cursor + 1] != b"<":
            raise AcquisitionError("unexpected content in XML prolog")

        if _at(cursor, b"<?xml") and (
            cursor + 5 >= len(body) or body[cursor + 5 : cursor + 6] in (b" ", b"\t", b"\n", b"\r")
        ):
            if has_xml_declaration:
                raise AcquisitionError("multiple XML declarations")
            if cursor != start:
                raise AcquisitionError("XML declaration is not at the prolog start")
            terminator = body.find(b"?>", cursor + 5, cursor + MAX_XML_DECLARATION_BYTES)
            if terminator < 0:
                raise AcquisitionError("malformed or unterminated XML declaration")
            declaration = body[cursor + 5 : terminator]
            if b"<" in declaration or b">" in declaration:
                raise AcquisitionError("XML declaration contains illegal characters")
            version_match = _XML_DECL_VERSION_RE.search(declaration)
            if not version_match or version_match.group(1) not in (b"1.0", b"1.1"):
                raise AcquisitionError("XML declaration requires version 1.0 or 1.1")
            xml_declaration_version = version_match.group(1).decode("ascii")
            encoding_match = _XML_DECL_ENCODING_RE.search(declaration)
            if encoding_match:
                encoding_value = encoding_match.group(1).decode("ascii", errors="replace")
                if encoding_value.casefold() not in {"utf-8", "utf8"}:
                    raise AcquisitionError(
                        f"XML declaration encoding must be UTF-8: {encoding_value!r}"
                    )
                xml_declaration_encoding = encoding_value
            has_xml_declaration = True
            cursor = terminator + 2
            continue

        if _at(cursor, b"<?"):
            raise AcquisitionError("processing instructions other than the XML declaration are not permitted")

        if _at(cursor, b"<!--"):
            end = body.find(b"-->", cursor + 4, cursor + 4 + MAX_COMMENT_BYTES)
            if end < 0:
                raise AcquisitionError("malformed or unterminated XML comment")
            interior = body[cursor + 4 : end]
            if b"--" in interior:
                raise AcquisitionError("XML comment must not contain `--`")
            accepted_comments += 1
            cursor = end + 3
            continue

        if _at(cursor, b"<!DOCTYPE"):
            if has_doctype:
                raise AcquisitionError("multiple DOCTYPE declarations are not permitted")
            if root_element_name is not None:
                raise AcquisitionError("DOCTYPE declaration after the root element is not permitted")
            greater = body.find(b">", cursor)
            bracket = body.find(b"[", cursor, greater if greater >= 0 else cursor + MAX_DOCTYPE_BYTES)
            if bracket >= 0 and bracket < (greater if greater >= 0 else bracket + 1):
                raise AcquisitionError("DOCTYPE internal subset (`[`) is not permitted")
            if greater < 0 or greater - cursor > MAX_DOCTYPE_BYTES:
                raise AcquisitionError("DOCTYPE declaration is missing or exceeds maximum length")
            declaration_body = body[cursor + len(b"<!DOCTYPE") : greater]
            if not declaration_body.startswith((b" ", b"\t", b"\n", b"\r")):
                raise AcquisitionError("DOCTYPE requires whitespace after the keyword")
            declaration_body = declaration_body.strip()
            if not declaration_body:
                raise AcquisitionError("DOCTYPE root name is missing")
            if re.search(rb"\b(SYSTEM|PUBLIC)\b", declaration_body):
                raise AcquisitionError("DOCTYPE external identifier (SYSTEM/PUBLIC) is not permitted")
            if b"[" in declaration_body or b"]" in declaration_body:
                raise AcquisitionError("DOCTYPE internal subset (`[`) is not permitted")
            root_match = _DOCTYPE_ROOT_RE.match(declaration_body)
            if not root_match:
                raise AcquisitionError("DOCTYPE root name is malformed or has trailing content")
            root_name = root_match.group(1).decode("ascii")
            if root_name != expected_root:
                raise AcquisitionError(
                    f"DOCTYPE root name must be {expected_root!r}, got {root_name!r}"
                )
            has_doctype = True
            doctype_root = root_name
            cursor = greater + 1
            continue

        if _at(cursor, b"<!ENTITY"):
            raise AcquisitionError("ENTITY declarations are not permitted")
        if _at(cursor, b"<!NOTATION"):
            raise AcquisitionError("NOTATION declarations are not permitted")
        if _at(cursor, b"<!ATTLIST"):
            raise AcquisitionError("ATTLIST declarations are not permitted outside an accepted DOCTYPE subset")
        if _at(cursor, b"<!ELEMENT"):
            raise AcquisitionError("ELEMENT declarations are not permitted outside an accepted DOCTYPE subset")
        if _at(cursor, b"<!["):
            raise AcquisitionError("CDATA sections and conditional sections are not permitted in the prolog")
        if _at(cursor, b"<!"):
            raise AcquisitionError("unknown XML markup declaration")

        # Root element start.
        window = body[cursor : cursor + 128]
        root_match = _ROOT_ELEMENT_RE.match(window)
        if not root_match:
            raise AcquisitionError("malformed root element")
        root_element_name = root_match.group(1).decode("ascii")
        if root_element_name != expected_root:
            raise AcquisitionError(
                f"root element must be {expected_root!r}, got {root_element_name!r}"
            )
        break

    return {
        "prolog_bytes_scanned": cursor - start,
        "utf8_bom": start == 3,
        "has_xml_declaration": has_xml_declaration,
        "xml_declaration_version": xml_declaration_version,
        "xml_declaration_encoding": xml_declaration_encoding,
        "has_doctype": has_doctype,
        "doctype_root": doctype_root,
        "expected_root": expected_root,
        "root_element": root_element_name,
        "accepted_comments": accepted_comments,
    }


def parse_eml(body: bytes, proposal: SourceProposal) -> dict[str, Any]:
    prolog = validate_xml_prolog(body, EML_EXPECTED_ROOT)
    # Belt-and-braces: reject any residual external-resource declaration outside
    # the bounded window before invoking the parser. The prolog validator already
    # forbids these constructs before the root element; this check catches an
    # attacker who smuggles them inside the root element's content.
    if re.search(rb"<!ENTITY|<!DOCTYPE", body[prolog["prolog_bytes_scanned"] :], re.I):
        raise AcquisitionError("ENTITY or DOCTYPE declaration inside XML content is not permitted")
    if b"xi:include" in body.lower():
        raise AcquisitionError("XInclude directives are not permitted")
    parser = ElementTree.XMLParser()
    try:
        root = ElementTree.fromstring(body, parser=parser)
    except ElementTree.ParseError as exc:
        raise AcquisitionError(f"malformed EML XML: {exc}") from exc
    values: dict[str, list[str]] = {}
    for element in root.iter():
        local = element.tag.rsplit("}", 1)[-1]
        text = " ".join("".join(element.itertext()).split())
        if text and local in {
            "title", "alternateIdentifier", "pubDate", "organizationName",
            "intellectualRights", "para", "edition",
        }:
            values.setdefault(local, []).append(text)
    package_id = root.attrib.get("packageId") or root.attrib.get("packageID")
    all_text = " ".join(value for items in values.values() for value in items)
    return {
        "title": next(iter(values.get("title", [])), None),
        "alternate_identifiers": values.get("alternateIdentifier", []),
        "publication_dates": values.get("pubDate", []),
        "publishers": values.get("organizationName", []),
        "rights": values.get("intellectualRights", []) or [
            value for value in values.get("para", [])
            if re.search(r"license|creative commons|CC[- ]?BY", value, re.I)
        ],
        "edition": next(iter(values.get("edition", [])), None),
        "package_id": package_id,
        "uuid_present": proposal.dataset_uuid in all_text or package_id == proposal.dataset_uuid,
        "prolog": prolog,
    }


def parse_content_length(header: str | None, ceiling: int) -> int | None:
    """Parse a Content-Length header value against a positive-integer ceiling.

    Returns the declared length in bytes when the header is present, well-formed,
    positive, and within ``ceiling``. Returns ``None`` when the header is absent
    (a legitimate "declared size unavailable" state). Raises ``AcquisitionError``
    on any other shape: empty string, non-digit characters, comma-separated
    conflicting values, zero, negative, or over the ceiling.
    """
    if not isinstance(ceiling, int) or ceiling <= 0:
        raise AcquisitionError("ceiling must be a positive integer")
    if header is None:
        return None
    text = header.strip()
    if not text:
        raise AcquisitionError("archive Content-Length is malformed")
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        raise AcquisitionError("archive Content-Length is malformed")
    if len(set(parts)) != 1:
        raise AcquisitionError("archive Content-Length has conflicting values")
    value = parts[0]
    if not value.isdigit():
        raise AcquisitionError("archive Content-Length is malformed")
    length = int(value)
    if length <= 0:
        raise AcquisitionError("archive Content-Length must be positive")
    if length > ceiling:
        raise AcquisitionError("archive Content-Length exceeds the ceiling")
    return length


def normalize_head(response: Response, proposal: SourceProposal, policy: MetadataPolicy) -> dict[str, Any]:
    if response.method != "HEAD" or response.body:
        raise AcquisitionError("archive metadata operation must remain bodyless HEAD")
    validate_response(response, policy, ZIP_TYPES)
    headers = {key.casefold(): value.strip() for key, value in response.headers.items()}
    raw_length = headers.get("content-length")
    length = parse_content_length(raw_length, proposal.proposed_maximum_bytes)
    if length is None:
        size_status = "unavailable"
        size_reason = "header_absent"
    else:
        size_status = "declared"
        size_reason = None
    disposition = headers.get("content-disposition")
    if _media_type(response.headers) not in ZIP_TYPES and not (disposition and ".zip" in disposition.casefold()):
        raise AcquisitionError("archive HEAD is inconsistent with ZIP delivery")
    return {
        "requested_url": response.requested_url, "final_url": response.final_url,
        "redirect_chain": list(response.redirect_urls), "status": response.status,
        "content_type": _media_type(response.headers),
        "content_length": length,
        "content_length_declared": length is not None,
        "size_declaration_status": size_status,
        "size_declaration_reason": size_reason,
        "etag": headers.get("etag"), "last_modified": headers.get("last-modified"),
        "accept_ranges": headers.get("accept-ranges"),
        "content_disposition": disposition,
    }


def evaluate_archive_get(
    *,
    ceiling: int,
    content_length_header: str | None,
    completed_bytes: int,
    reached_eof: bool,
) -> dict[str, Any]:
    """Policy check for a completed archive GET stream.

    The caller enforces ``ceiling`` while streaming and must abort before
    exceeding it; this helper only inspects the caller-reported final state.
    Raises ``AcquisitionError`` on any policy violation. Returns a small
    evidence record describing the accepted outcome.

    A malformed, conflicting, zero, negative, or over-ceiling Content-Length is
    a hard failure. A missing Content-Length is accepted only when the stream
    reached a clean EOF within the ceiling. A declared Content-Length must
    match the completed byte count exactly and the stream must have reached
    EOF. The ``Range`` request header is not required and is not consulted;
    a server that ignores ``Range`` and returns the full body is acceptable
    as long as the ceiling and length rules pass.
    """
    if not isinstance(ceiling, int) or ceiling <= 0:
        raise AcquisitionError("archive GET ceiling must be a positive integer")
    if not isinstance(completed_bytes, int) or completed_bytes < 0:
        raise AcquisitionError("archive GET completed_bytes must be a non-negative integer")
    if completed_bytes > ceiling:
        raise AcquisitionError("archive GET stream exceeded the ceiling")
    declared = parse_content_length(content_length_header, ceiling)
    if declared is None:
        if not reached_eof:
            raise AcquisitionError("archive GET terminated before EOF and no Content-Length was declared")
        return {
            "size_declaration_status": "unavailable",
            "declared_length": None,
            "completed_bytes": completed_bytes,
            "reached_eof": True,
            "ceiling": ceiling,
        }
    if completed_bytes != declared:
        raise AcquisitionError("archive GET completed byte count disagrees with declared Content-Length")
    if not reached_eof:
        raise AcquisitionError("archive GET terminated before EOF despite declared Content-Length")
    return {
        "size_declaration_status": "declared",
        "declared_length": declared,
        "completed_bytes": completed_bytes,
        "reached_eof": True,
        "ceiling": ceiling,
    }


def _comparison(expected: Any, observed: Any, source: str) -> dict[str, Any]:
    return {
        "expected": expected, "observed": observed, "source": source,
        "result": "verified" if observed == expected else ("not_exposed" if observed is None else "mismatch"),
    }


VERIFICATION_STATE_SCHEMA = 2
OPERATION_NAMES = ("resource_page", "eml", "archive_head")


def _transport_evidence(response: Response) -> dict[str, Any]:
    return {
        "method": response.method,
        "requested_url": response.requested_url,
        "final_url": response.final_url,
        "redirect_chain": list(response.redirect_urls),
        "status": response.status,
        "content_type": _media_type(response.headers),
        "body_bytes": len(response.body),
        "body_sha256": hashlib.sha256(response.body).hexdigest() if response.body else None,
    }


def _new_state(proposal: SourceProposal, request: NorTaxaRequest, policy: MetadataPolicy,
               verified_at: str) -> dict[str, Any]:
    return {
        "verification_state_schema_version": VERIFICATION_STATE_SCHEMA,
        "proposal_sha256": proposal.canonical_sha256,
        "request_sha256": request.request_sha256,
        "started_at": verified_at,
        "tool": {"name": "nortaxa_metadata.py", "version": 1},
        "authorization": {
            "metadata_only": True,
            "archive_get_authorized": False,
            "acquisition_approval_created": False,
            "maximum_cumulative_body_bytes": policy.cumulative_max_bytes,
        },
        "operations": {name: {"status": "pending"} for name in OPERATION_NAMES},
        "cumulative_body_bytes": 0,
        "verdict": None,
        "finished_at": None,
        "final": False,
    }


def _mark_skipped(state: dict[str, Any], starting_after: str) -> None:
    remaining = OPERATION_NAMES[OPERATION_NAMES.index(starting_after) + 1 :]
    for name in remaining:
        if state["operations"][name]["status"] == "pending":
            state["operations"][name] = {
                "status": "skipped",
                "reason": f"aborted after {starting_after}",
            }


def verify(
    proposal: SourceProposal,
    request: NorTaxaRequest,
    transport: Transport,
    *,
    policy: MetadataPolicy,
    verified_at: str | None = None,
    response_recorder: Callable[[Response], None] | None = None,
    journal_sink: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    responses: list[Response] = []
    remaining = policy.cumulative_max_bytes
    started = verified_at or utc_now()
    state = _new_state(proposal, request, policy, started)

    def emit() -> None:
        if journal_sink is not None:
            journal_sink(state)

    emit()

    def perform(method: str, endpoint: str, allowed_types: set[str]) -> Response:
        nonlocal remaining
        response = transport.request(method, endpoint, policy, remaining)
        validate_response(response, policy, allowed_types)
        if len(response.body) > remaining:
            raise AcquisitionError("metadata body exceeds remaining cumulative budget")
        length_text = next(
            (value for key, value in response.headers.items() if key.casefold() == "content-length"),
            None,
        )
        if method == "GET" and length_text:
            if not length_text.isdigit():
                raise AcquisitionError("metadata Content-Length is malformed")
            if int(length_text) > remaining:
                raise AcquisitionError("metadata Content-Length exceeds remaining cumulative budget")
        if method == "HEAD" and response.body:
            raise AcquisitionError("archive HEAD unexpectedly returned a body")
        remaining -= len(response.body)
        responses.append(response)
        if response_recorder is not None:
            response_recorder(response)
        return response

    def run_step(name: str, method: str, endpoint: str, allowed_types: set[str],
                 parser: Callable[[Response], dict[str, Any]]) -> dict[str, Any]:
        op = state["operations"][name]
        try:
            response = perform(method, endpoint, allowed_types)
        except AcquisitionError as exc:
            op.update({
                "status": "transport_failed",
                "error": {"type": type(exc).__name__, "message": str(exc), "phase": "transport"},
            })
            state["cumulative_body_bytes"] = policy.cumulative_max_bytes - remaining
            _mark_skipped(state, name)
            emit()
            raise
        op.update({"status": "transport_succeeded", "transport": _transport_evidence(response)})
        state["cumulative_body_bytes"] = policy.cumulative_max_bytes - remaining
        emit()
        try:
            parsed = parser(response)
        except AcquisitionError as exc:
            op.update({
                "status": "parse_failed",
                "error": {"type": type(exc).__name__, "message": str(exc), "phase": "parse"},
            })
            _mark_skipped(state, name)
            emit()
            raise
        op["status"] = "parse_succeeded"
        op["parsed"] = parsed
        emit()
        return parsed

    resource_values = run_step(
        "resource_page", "GET", proposal.resource_page, HTML_TYPES,
        lambda r: parse_resource_html(r.body, proposal),
    )
    eml_values = run_step(
        "eml", "GET", proposal.eml_endpoint, XML_TYPES,
        lambda r: parse_eml(r.body, proposal),
    )
    head = run_step(
        "archive_head", "HEAD", proposal.archive_endpoint, ZIP_TYPES,
        lambda r: normalize_head(r, proposal, policy),
    )
    comparisons = {
        "resource_key": _comparison(proposal.resource_key, proposal.resource_key if resource_values["resource_key_present"] else None, "resource_page"),
        "title": _comparison(proposal.title, proposal.title if resource_values["title_present"] else eml_values["title"], "resource_page_or_eml"),
        "version": _comparison(proposal.version, proposal.version if resource_values["version_present"] else eml_values["edition"], "resource_page_or_eml"),
        "issued_date": _comparison(proposal.issued_date, proposal.issued_date if resource_values["issued_date_present"] else next(iter(eml_values["publication_dates"]), None), "resource_page_or_eml"),
        "Taxon_count": _comparison(proposal.raw["published_counts"]["Taxon"], resource_values["counts"].get("Taxon"), "resource_page"),
        "VernacularName_count": _comparison(proposal.raw["published_counts"]["VernacularName"], resource_values["counts"].get("VernacularName"), "resource_page"),
        "update_frequency": _comparison(proposal.raw["published_update_frequency"], "weekly" if resource_values["weekly_present"] else None, "resource_page"),
        "license": _comparison(
            proposal.expected_license,
            proposal.expected_license if resource_values["license_present"] or any(
                re.search(r"CC[- ]?BY|Creative Commons Attribution", value, re.I)
                for value in eml_values["rights"]
            ) else resource_values["observed_license"] or next(iter(eml_values["rights"]), None),
            "resource_page_or_eml",
        ),
        "dataset_uuid": _comparison(
            proposal.dataset_uuid,
            proposal.dataset_uuid if resource_values["dataset_uuid_present"] or eml_values["uuid_present"]
            else resource_values["observed_dataset_uuid"] or eml_values["package_id"],
            "resource_page_or_eml",
        ),
        "publisher": {"expected": None, "observed": resource_values["publisher"] or next(iter(eml_values["publishers"]), None), "source": "resource_page_or_eml", "result": "verified_if_exposed"},
    }
    mismatches = [key for key, value in comparisons.items() if value["result"] == "mismatch"]
    verdict = "passed" if not mismatches else "mismatch"
    evidence = {
        "metadata_verification_schema_version": 1,
        "proposal_sha256": proposal.canonical_sha256,
        "request_sha256": request.request_sha256,
        "verified_at": verified_at or utc_now(),
        "tool": {"name": "nortaxa_metadata.py", "version": 1},
        "authorization": {
            "metadata_only": True, "archive_get_authorized": False,
            "acquisition_approval_created": False, "maximum_cumulative_body_bytes": policy.cumulative_max_bytes,
        },
        "operations": [
            {
                "method": response.method, "requested_url": response.requested_url,
                "final_url": response.final_url, "redirect_chain": list(response.redirect_urls),
                "status": response.status, "content_type": _media_type(response.headers),
                "body_bytes": len(response.body),
                "body_sha256": hashlib.sha256(response.body).hexdigest() if response.body else None,
            }
            for response in responses
        ],
        "observed": {"resource_page": resource_values, "eml": eml_values, "archive_head": head},
        "comparisons": comparisons, "mismatches": mismatches,
        "unknown_fields": [key for key, value in comparisons.items() if value["result"] == "not_exposed"],
        "cumulative_body_bytes": policy.cumulative_max_bytes - remaining,
        "verdict": verdict,
    }
    fixtures = {
        "resource-page.sanitized.json": json.dumps(resource_values, ensure_ascii=False, sort_keys=True, indent=2).encode() + b"\n",
        "eml.sanitized.json": json.dumps(eml_values, ensure_ascii=False, sort_keys=True, indent=2).encode() + b"\n",
    }
    state["verdict"] = verdict
    state["finished_at"] = utc_now()
    state["comparisons"] = comparisons
    state["mismatches"] = mismatches
    state["unknown_fields"] = evidence["unknown_fields"]
    state["final"] = verdict == "passed"
    emit()
    return evidence, fixtures


def evidence_sha256(evidence: dict[str, Any]) -> str:
    return sha256_json(evidence)


RESOLVABLE_PARSE_FAILURES: dict[str, dict[str, Any]] = {
    "nortaxa-archive-head-content-length-absent-v1": {
        "operation": "archive_head",
        "superseded_error_type": "AcquisitionError",
        "superseded_error_messages": (
            "archive HEAD lacks a positive Content-Length",
        ),
        "required_transport_evidence": (
            "raw_headers_recorded",
            "content_length_presence_recorded",
        ),
    },
}


def verify_closure_conditions(
    *,
    policy_resolution: dict[str, Any],
    attempt_record: dict[str, Any],
    replacement_evaluator: Callable[[dict[str, Any]], str] | None = None,
) -> dict[str, Any]:
    """Enforce the formal contract for closing a `parse_failed` attempt.

    A final metadata artifact may be emitted from an immutable parse-failed
    attempt ONLY when all six conditions hold:

    1. the failure is exclusively an explicitly superseded policy decision;
    2. the resolution binds the attempt hash and the exact recorded transport
       evidence (status code, content type, redirect chain, body bytes);
    3. deterministic re-evaluation under the identified replacement policy
       succeeds against the preserved transport evidence;
    4. all other operations succeeded;
    5. the new policy requires no evidence the attempt failed to preserve;
    6. the final artifact references the resolution (asserted at write time).

    Raises ``AcquisitionError`` with a specific reason string when any
    condition fails. On success returns a summary dict; the caller writes
    the final artifact and asserts condition 6 by binding the resolution
    canonical SHA-256.
    """
    identifier = policy_resolution.get("policy_identifier")
    if identifier not in RESOLVABLE_PARSE_FAILURES:
        raise AcquisitionError(
            f"closure: unknown or unregistered policy identifier {identifier!r}"
        )
    spec = RESOLVABLE_PARSE_FAILURES[identifier]

    # Condition 2 (bindings before anything else): attempt hash and transport must be bound.
    bound = policy_resolution.get("bound_evidence") or {}
    attempt_number = attempt_record.get("attempt_number")
    if attempt_number is None:
        raise AcquisitionError("closure: attempt record does not identify an attempt_number")
    key = f"attempt_{attempt_number}_sha256"
    expected_hash = bound.get(key)
    if not expected_hash:
        raise AcquisitionError(f"closure: policy resolution does not bind {key}")
    computed_hash = sha256_json(attempt_record)
    if expected_hash != computed_hash:
        raise AcquisitionError("closure: attempt hash does not match the policy resolution binding")

    operations = attempt_record.get("operations") or []
    if not isinstance(operations, list) or not operations:
        raise AcquisitionError("closure: attempt record has no operations to evaluate")

    target = spec["operation"]
    target_ops = [op for op in operations if op.get("operation") == target]
    if len(target_ops) != 1:
        raise AcquisitionError(f"closure: attempt record lacks a unique {target!r} operation")
    target_op = target_ops[0]

    # Condition 1: failure is exclusively the superseded policy decision.
    if target_op.get("status") != "parse_failed":
        raise AcquisitionError(
            f"closure: {target!r} status is {target_op.get('status')!r}, not parse_failed"
        )
    err = target_op.get("error") or {}
    if err.get("phase") != "parse":
        raise AcquisitionError("closure: failure was not raised in the parse phase")
    if err.get("type") != spec["superseded_error_type"]:
        raise AcquisitionError(
            f"closure: error type {err.get('type')!r} is not superseded by this policy"
        )
    if err.get("message") not in spec["superseded_error_messages"]:
        raise AcquisitionError(
            "closure: error message is not among the explicitly superseded set"
        )

    # Condition 4: all other operations succeeded.
    for op in operations:
        if op.get("operation") == target:
            continue
        if op.get("status") != "parse_succeeded":
            raise AcquisitionError(
                f"closure: sibling operation {op.get('operation')!r} did not reach parse_succeeded"
            )

    # Condition 2 (continued): resolution's transport observation matches the record exactly.
    observation = policy_resolution.get("archive_head_observation") or {}
    for field in ("status_code", "content_type", "redirect_chain", "body_bytes", "body_sha256",
                  "requested_url", "final_url"):
        if field not in observation:
            raise AcquisitionError(
                f"closure: policy resolution omits transport field {field!r}"
            )
        if observation[field] != target_op.get(field):
            raise AcquisitionError(
                f"closure: policy resolution transport field {field!r} does not match the attempt record"
            )

    # Condition 5: new policy must not require evidence the attempt did not preserve.
    preserved: set[str] = set()
    # The attempt journal preserves a raw headers dict only when the transport step
    # journaled it explicitly. In the frozen schema-2 attempt-3 record, HEAD headers
    # were not captured because the parse gate stopped the sequence before header
    # normalization; presence/absence of ETag, Last-Modified, Accept-Ranges,
    # Content-Disposition, and even the exact shape of Content-Length is not in
    # the record.
    if any(key.lower() in {"raw_headers", "headers"} for key in target_op):
        preserved.update({"raw_headers_recorded", "content_length_presence_recorded"})
    missing_evidence = [name for name in spec["required_transport_evidence"] if name not in preserved]
    if missing_evidence:
        raise AcquisitionError(
            "closure: attempt did not preserve required evidence: " + ", ".join(missing_evidence)
        )

    # Condition 3: deterministic re-evaluation succeeds.
    if replacement_evaluator is None:
        raise AcquisitionError(
            "closure: a deterministic replacement evaluator is required to close a parse_failed attempt"
        )
    verdict = replacement_evaluator(target_op)
    if verdict != "parse_succeeded":
        raise AcquisitionError(
            f"closure: replacement evaluator returned {verdict!r}, not 'parse_succeeded'"
        )

    return {
        "operation": target,
        "policy_identifier": identifier,
        "attempt_sha256": computed_hash,
        "resolved_status": "parse_succeeded",
    }


def replay_journal_state(state: dict[str, Any]) -> dict[str, Any]:
    """Rebuild a read-only summary of an on-disk verification-state journal.

    Deterministic and network-free. Never resolves external resources or invokes
    parsers a second time. Returns a summary that distinguishes transport
    success, parse success, absent-from-source values, and unavailable values
    (never reached because an earlier operation failed).
    """
    if state.get("verification_state_schema_version") != VERIFICATION_STATE_SCHEMA:
        raise AcquisitionError(
            f"unsupported verification-state schema: {state.get('verification_state_schema_version')!r}"
        )
    operations = state.get("operations") or {}
    summary: dict[str, Any] = {
        "final": bool(state.get("final")),
        "verdict": state.get("verdict"),
        "operations": {},
    }
    for name in OPERATION_NAMES:
        op = operations.get(name) or {"status": "pending"}
        entry = {"status": op.get("status", "pending")}
        if "transport" in op:
            entry["transport"] = op["transport"]
        if op.get("status") == "parse_succeeded":
            entry["parsed_available"] = True
        elif op.get("status") in {"transport_failed", "parse_failed"}:
            entry["error"] = op.get("error")
        elif op.get("status") == "skipped":
            entry["reason"] = op.get("reason")
        summary["operations"][name] = entry
    return summary
