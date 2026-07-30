#!/usr/bin/env python3
"""Read-only ChecklistBank metadata verification for a pinned COL XR release."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from refresh_col_xr import AcquisitionError, OFFICIAL_ENDPOINT_HOSTS


DEFAULT_METADATA_TIMEOUT_SECONDS = 20
DEFAULT_METADATA_MAX_REDIRECTS = 3
DEFAULT_METADATA_MAX_BYTES = 512 * 1024
JSON_MEDIA_TYPES = {"application/json", "application/problem+json"}


@dataclass(frozen=True)
class MetadataPolicy:
    timeout_seconds: int = DEFAULT_METADATA_TIMEOUT_SECONDS
    max_redirects: int = DEFAULT_METADATA_MAX_REDIRECTS
    max_bytes: int = DEFAULT_METADATA_MAX_BYTES
    user_agent: str = "Sporely-Taxonomy-Metadata/1 (+https://sporely.com)"

    def __post_init__(self) -> None:
        if (
            self.timeout_seconds <= 0
            or self.max_redirects < 0
            or self.max_bytes <= 0
            or not self.user_agent.strip()
        ):
            raise AcquisitionError("metadata policy must use bounded positive limits and a user agent")


@dataclass(frozen=True)
class MetadataResponse:
    status: int
    final_url: str
    content_type: str
    body: bytes
    redirect_urls: tuple[str, ...] = ()
    content_length: int | None = None


class MetadataTransport(Protocol):
    def get(self, url: str, policy: MetadataPolicy) -> MetadataResponse: ...


class _BoundedOfficialRedirectHandler(HTTPRedirectHandler):
    def __init__(self, max_redirects: int):
        self.max_redirects = max_redirects
        self.redirect_urls: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _validate_public_metadata_url(newurl)
        self.redirect_urls.append(newurl)
        if len(self.redirect_urls) > self.max_redirects:
            raise AcquisitionError("metadata response exceeded the redirect limit")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class UrllibMetadataTransport:
    """Small public-GET transport; callers may inject a fake for offline tests."""

    def get(self, url: str, policy: MetadataPolicy) -> MetadataResponse:
        _validate_public_metadata_url(url)
        redirects = _BoundedOfficialRedirectHandler(policy.max_redirects)
        opener = build_opener(redirects)
        request = Request(
            url,
            method="GET",
            headers={"Accept": "application/json", "User-Agent": policy.user_agent},
        )
        try:
            with opener.open(request, timeout=policy.timeout_seconds) as handle:
                body = handle.read(policy.max_bytes + 1)
                length_header = handle.headers.get("Content-Length")
                content_length = int(length_header) if length_header and length_header.isdigit() else None
                return MetadataResponse(
                    status=handle.status,
                    final_url=handle.geturl(),
                    content_type=handle.headers.get("Content-Type", ""),
                    body=body,
                    redirect_urls=tuple(redirects.redirect_urls),
                    content_length=content_length,
                )
        except AcquisitionError:
            raise
        except HTTPError as exc:
            raise AcquisitionError(f"metadata endpoint returned HTTP {exc.code}") from exc
        except (OSError, URLError) as exc:
            raise AcquisitionError(f"metadata GET failed: {exc}") from exc


@dataclass(frozen=True)
class ReleaseCandidate:
    release_label: str
    release_type: str
    dataset_key: int
    issued_date: str
    doi: str


@dataclass(frozen=True)
class VerifiedMetadata:
    endpoint: str
    final_url: str
    response_sha256: str
    normalized: dict[str, Any]


def _validate_public_metadata_url(url: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in OFFICIAL_ENDPOINT_HOSTS
        or parsed.username
        or parsed.password
    ):
        raise AcquisitionError("metadata URL must use an official credential-free HTTPS host")


def _decode_json_response(response: MetadataResponse, policy: MetadataPolicy) -> dict[str, Any]:
    if response.status != 200:
        raise AcquisitionError(f"metadata endpoint returned HTTP {response.status}")
    for redirect in response.redirect_urls:
        _validate_public_metadata_url(redirect)
    if len(response.redirect_urls) > policy.max_redirects:
        raise AcquisitionError("metadata response exceeded the redirect limit")
    _validate_public_metadata_url(response.final_url)
    media_type = response.content_type.split(";", 1)[0].strip().casefold()
    if media_type not in JSON_MEDIA_TYPES and not media_type.endswith("+json"):
        raise AcquisitionError(f"metadata response is not JSON: {response.content_type!r}")
    if response.content_length is not None and response.content_length > policy.max_bytes:
        raise AcquisitionError("metadata response Content-Length exceeds the size limit")
    if len(response.body) > policy.max_bytes:
        raise AcquisitionError("metadata response body exceeds the size limit")
    prefix = response.body.lstrip()[:32].lower()
    if prefix.startswith((b"<!doctype html", b"<html")):
        raise AcquisitionError("metadata endpoint returned HTML")
    try:
        value = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionError("metadata endpoint returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise AcquisitionError("metadata endpoint JSON must be an object")
    return value


def normalize_release_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    origin = str(raw.get("origin", "")).strip().casefold()
    release_type = "Extended Release" if origin == "xrelease" else origin
    return {
        "release_label": str(raw.get("version", "")).strip(),
        "release_type": release_type,
        "dataset_key": raw.get("key"),
        "issued_date": str(raw.get("issued", "")).strip(),
        "doi": str(raw.get("doi", "")).strip().lower(),
    }


def verify_release_metadata(
    candidate: ReleaseCandidate,
    endpoint: str,
    transport: MetadataTransport,
    policy: MetadataPolicy = MetadataPolicy(),
) -> VerifiedMetadata:
    _validate_public_metadata_url(endpoint)
    response = transport.get(endpoint, policy)
    raw = _decode_json_response(response, policy)
    normalized = normalize_release_metadata(raw)
    expected = {
        "release_label": candidate.release_label.strip(),
        "release_type": candidate.release_type.strip(),
        "dataset_key": candidate.dataset_key,
        "issued_date": candidate.issued_date.strip(),
        "doi": candidate.doi.strip().lower(),
    }
    mismatches = {
        field: {"expected": expected[field], "observed": normalized[field]}
        for field in expected
        if normalized[field] != expected[field]
    }
    if mismatches:
        raise AcquisitionError(f"candidate release metadata mismatch: {json.dumps(mismatches, sort_keys=True)}")
    return VerifiedMetadata(
        endpoint=endpoint,
        final_url=response.final_url,
        response_sha256=hashlib.sha256(response.body).hexdigest(),
        normalized=normalized,
    )


def verify_fungi_root(
    response: MetadataResponse,
    *,
    dataset_key: int,
    policy: MetadataPolicy = MetadataPolicy(),
) -> dict[str, Any]:
    raw = _decode_json_response(response, policy)
    matches: list[dict[str, Any]] = []
    for wrapper in raw.get("result", []):
        if not isinstance(wrapper, dict):
            continue
        usage = wrapper.get("usage")
        if not isinstance(usage, dict):
            continue
        name = usage.get("name")
        if not isinstance(name, dict):
            continue
        if str(name.get("scientificName", "")).casefold() == "fungi":
            matches.append(usage)
    if len(matches) != 1:
        raise AcquisitionError(f"Fungi root lookup is ambiguous: expected 1 exact match, observed {len(matches)}")
    usage = matches[0]
    name = usage["name"]
    if usage.get("datasetKey") != dataset_key:
        raise AcquisitionError("Fungi root belongs to the wrong dataset")
    if str(name.get("rank", "")).casefold() != "kingdom":
        raise AcquisitionError("Fungi root must have kingdom rank")
    if str(usage.get("status", "")).casefold() != "accepted":
        raise AcquisitionError("Fungi root must be accepted")
    identifier = usage.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise AcquisitionError("Fungi root usage ID must be a non-empty opaque string")
    parent_id = usage.get("parentId")
    if not isinstance(parent_id, str) or not parent_id:
        raise AcquisitionError("Fungi root must identify its parent")
    return {
        "dataset_key": dataset_key,
        "usage_id": identifier,
        "identifier_type": "opaque string",
        "scientific_name": name["scientificName"],
        "rank": name["rank"],
        "status": usage["status"],
        "parent_usage_id": parent_id,
        "response_sha256": hashlib.sha256(response.body).hexdigest(),
        "endpoint": response.final_url,
    }
