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


def parse_eml(body: bytes, proposal: SourceProposal) -> dict[str, Any]:
    upper = body.upper()
    if any(token in upper for token in (b"<!DOCTYPE", b"<!ENTITY", b"SYSTEM", b"PUBLIC")):
        raise AcquisitionError("unsafe EML declaration")
    try:
        root = ElementTree.fromstring(body)
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
    }


def normalize_head(response: Response, proposal: SourceProposal, policy: MetadataPolicy) -> dict[str, Any]:
    if response.method != "HEAD" or response.body:
        raise AcquisitionError("archive metadata operation must remain bodyless HEAD")
    validate_response(response, policy, ZIP_TYPES)
    headers = {key.casefold(): value.strip() for key, value in response.headers.items()}
    length_text = headers.get("content-length")
    if not length_text or not length_text.isdigit() or int(length_text) <= 0:
        raise AcquisitionError("archive HEAD lacks a positive Content-Length")
    length = int(length_text)
    if length > proposal.proposed_maximum_bytes:
        raise AcquisitionError("archive HEAD length exceeds the proposed ceiling")
    disposition = headers.get("content-disposition")
    if _media_type(response.headers) not in ZIP_TYPES and not (disposition and ".zip" in disposition.casefold()):
        raise AcquisitionError("archive HEAD is inconsistent with ZIP delivery")
    return {
        "requested_url": response.requested_url, "final_url": response.final_url,
        "redirect_chain": list(response.redirect_urls), "status": response.status,
        "content_type": _media_type(response.headers), "content_length": length,
        "etag": headers.get("etag"), "last_modified": headers.get("last-modified"),
        "accept_ranges": headers.get("accept-ranges"),
        "content_disposition": disposition,
    }


def _comparison(expected: Any, observed: Any, source: str) -> dict[str, Any]:
    return {
        "expected": expected, "observed": observed, "source": source,
        "result": "verified" if observed == expected else ("not_exposed" if observed is None else "mismatch"),
    }


def verify(
    proposal: SourceProposal,
    request: NorTaxaRequest,
    transport: Transport,
    *,
    policy: MetadataPolicy,
    verified_at: str | None = None,
    response_recorder: Callable[[Response], None] | None = None,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    responses: list[Response] = []
    remaining = policy.cumulative_max_bytes

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

    resource = perform("GET", proposal.resource_page, HTML_TYPES)
    resource_values = parse_resource_html(resource.body, proposal)
    eml = perform("GET", proposal.eml_endpoint, XML_TYPES)
    eml_values = parse_eml(eml.body, proposal)
    head_response = perform("HEAD", proposal.archive_endpoint, ZIP_TYPES)
    head = normalize_head(head_response, proposal, policy)
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
    return evidence, fixtures


def evidence_sha256(evidence: dict[str, Any]) -> str:
    return sha256_json(evidence)
