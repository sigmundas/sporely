#!/usr/bin/env python3
"""Public COL XR delivery and future approval contracts.

No real-download CLI is exposed by this module.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

from refresh_col_xr import AcquisitionError, canonical_json


PUBLIC_ARCHIVE_HOST = "api.checklistbank.org"
PUBLIC_REDIRECT_HOST = "download.checklistbank.org"
PUBLIC_ARCHIVE_PATH = "/dataset/315834/export.zip"
PUBLIC_ARCHIVE_QUERY = {"extended": "true", "format": "ColDP"}
PUBLIC_ARCHIVE_ENDPOINT = (
    "https://api.checklistbank.org/dataset/315834/export.zip"
    "?extended=true&format=ColDP"
)
ACCEPTED_ARCHIVE_CONTENT_TYPES = {
    "application/zip",
    "application/octet-stream",
    "application/zip-compressed",
    "application/x-zip-compressed",
}


def sha256_value(value: Any) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _exact_public_endpoint(endpoint: str) -> None:
    parsed = urlparse(endpoint)
    if (
        parsed.scheme != "https"
        or parsed.hostname != PUBLIC_ARCHIVE_HOST
        or parsed.path != PUBLIC_ARCHIVE_PATH
        or parsed.username
        or parsed.password
        or dict(parse_qsl(parsed.query, keep_blank_values=True)) != PUBLIC_ARCHIVE_QUERY
        or len(parse_qsl(parsed.query, keep_blank_values=True)) != 2
    ):
        raise AcquisitionError("full-release delivery must use the exact pinned public GET endpoint")


def proposal_request_identity(proposal: dict[str, Any]) -> dict[str, Any]:
    delivery = proposal.get("delivery", {})
    return {
        "release": {
            "release_label": proposal.get("release_label"),
            "release_type": proposal.get("release_type"),
            "dataset_key": proposal.get("dataset_key"),
            "issued_date": proposal.get("issued_date"),
            "doi": proposal.get("doi"),
            "archive_format": proposal.get("archive_format"),
        },
        "delivery": {
            "http_method": delivery.get("http_method"),
            "delivery_mode": delivery.get("delivery_mode"),
            "canonical_entry_endpoint": delivery.get("canonical_entry_endpoint"),
            "dataset_key": delivery.get("dataset_key"),
            "extended": delivery.get("extended"),
            "format": delivery.get("format"),
            "authentication_required": delivery.get("authentication_required"),
            "export_job_submission_required": delivery.get("export_job_submission_required"),
            "permitted_redirect_hosts": delivery.get("permitted_redirect_hosts"),
            "max_redirects": delivery.get("max_redirects"),
            "accepted_final_content_types": delivery.get("accepted_final_content_types"),
        },
    }


def validate_proposal(proposal: dict[str, Any]) -> None:
    if proposal.get("approval_status") != "proposed" or proposal.get("download_authorized") is not False:
        raise AcquisitionError("source-selection proposal must remain proposed and unauthorized")
    delivery = proposal.get("delivery")
    if not isinstance(delivery, dict):
        raise AcquisitionError("proposal delivery contract is required")
    if delivery.get("http_method") != "GET":
        raise AcquisitionError("public full-release delivery must use GET, not custom POST export")
    if delivery.get("delivery_mode") != "public prebuilt full release":
        raise AcquisitionError("proposal is not a public prebuilt full release")
    _exact_public_endpoint(str(delivery.get("canonical_entry_endpoint", "")))
    expected = {
        "dataset_key": 315834,
        "extended": True,
        "format": "ColDP",
        "authentication_required": False,
        "export_job_submission_required": False,
        "permitted_redirect_hosts": [PUBLIC_REDIRECT_HOST],
    }
    for key, value in expected.items():
        if delivery.get(key) != value:
            raise AcquisitionError(f"invalid public full-release delivery field: {key}")
    if delivery.get("max_redirects") != 3:
        raise AcquisitionError("public archive redirect limit must be 3")
    if set(delivery.get("accepted_final_content_types", [])) != ACCEPTED_ARCHIVE_CONTENT_TYPES:
        raise AcquisitionError("public archive content-type policy does not match the contract")
    for obsolete in ("included_fields", "audit_only_fields", "excluded_bulk_entities"):
        if obsolete in proposal:
            raise AcquisitionError(f"{obsolete} incorrectly represents compiler choices as export filters")
    compiler = proposal.get("compiler_consumption")
    if not isinstance(compiler, dict) or compiler.get("archive_preservation") != "preserve complete immutable bytes":
        raise AcquisitionError("proposal must separate complete archive preservation from compiler consumption")
    maximum = proposal.get("archive_policy", {}).get("proposed_maximum_bytes")
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0:
        raise AcquisitionError("proposal must state a positive proposed maximum archive size")


@dataclass(frozen=True)
class HeadResponse:
    status: int
    requested_url: str
    redirect_urls: tuple[str, ...]
    final_url: str
    headers: dict[str, str]


def normalize_head_metadata(response: HeadResponse, *, max_redirects: int = 3) -> dict[str, Any]:
    _exact_public_endpoint(response.requested_url)
    if len(response.redirect_urls) > max_redirects:
        raise AcquisitionError("archive HEAD exceeded the redirect limit")
    visited = [response.requested_url, *response.redirect_urls]
    if len(set(visited)) != len(visited):
        raise AcquisitionError("archive HEAD redirect loop detected")
    for redirect in response.redirect_urls:
        parsed = urlparse(redirect)
        if parsed.scheme != "https" or parsed.hostname != PUBLIC_REDIRECT_HOST:
            raise AcquisitionError("archive HEAD redirected to an unapproved host")
    final = urlparse(response.final_url)
    expected_final_host = PUBLIC_REDIRECT_HOST if response.redirect_urls else PUBLIC_ARCHIVE_HOST
    if final.scheme != "https" or final.hostname != expected_final_host:
        raise AcquisitionError("archive HEAD final host is not approved")
    if response.status != 200:
        raise AcquisitionError(f"archive HEAD returned HTTP {response.status}")
    headers = {
        key.casefold(): value.strip()
        for key, value in response.headers.items()
        if isinstance(value, str) and value.strip()
    }
    content_type = headers.get("content-type", "").split(";", 1)[0].casefold()
    if content_type not in ACCEPTED_ARCHIVE_CONTENT_TYPES:
        raise AcquisitionError(f"archive HEAD returned unsupported Content-Type: {content_type!r}")
    length_text = headers.get("content-length")
    content_length = None
    if length_text:
        try:
            content_length = int(length_text)
        except ValueError as exc:
            raise AcquisitionError("archive HEAD Content-Length is invalid") from exc
        if content_length <= 0:
            raise AcquisitionError("archive HEAD Content-Length must be positive")
    return {
        "status": response.status,
        "canonical_entry_endpoint": response.requested_url,
        "redirect_chain": list(response.redirect_urls),
        "final_official_host": final.hostname,
        "content_type": content_type,
        "content_length": content_length,
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "accept_ranges": headers.get("accept-ranges"),
        "content_disposition": headers.get("content-disposition"),
    }


def preflight_download(
    *,
    approved_maximum_bytes: int,
    content_length: int | None,
    destination: Path,
    free_bytes: int | None = None,
) -> dict[str, Any]:
    if approved_maximum_bytes <= 0:
        raise AcquisitionError("approved maximum bytes must be positive")
    if content_length is not None and content_length > approved_maximum_bytes:
        raise AcquisitionError("declared archive size exceeds the approved maximum")
    available = free_bytes
    if available is None:
        existing = destination.resolve().parent
        while not existing.exists():
            existing = existing.parent
        available = shutil.disk_usage(existing).free
    required = content_length if content_length is not None else approved_maximum_bytes
    if available < required:
        raise AcquisitionError("insufficient local free space for the approved archive")
    return {
        "expected_bytes": content_length,
        "approved_maximum_bytes": approved_maximum_bytes,
        "available_disk_bytes": available,
    }


def validate_approved_artifact(proposal: dict[str, Any], approved: dict[str, Any]) -> dict[str, Any]:
    validate_proposal(proposal)
    required = {
        "proposal_sha256",
        "approval_status",
        "download_authorized",
        "approved_at",
        "approved_canonical_endpoint",
        "approved_maximum_bytes",
        "approved_redirect_hosts",
        "release_identity",
        "request_sha256",
    }
    missing = required - set(approved)
    if missing:
        raise AcquisitionError(f"approved artifact lacks required fields: {sorted(missing)}")
    if approved["approval_status"] != "approved" or approved["download_authorized"] is not True:
        raise AcquisitionError("artifact does not explicitly authorize download")
    if approved["proposal_sha256"] != sha256_value(proposal):
        raise AcquisitionError("approved artifact proposal hash mismatch")
    request_hash = sha256_value(proposal_request_identity(proposal))
    if approved["request_sha256"] != request_hash:
        raise AcquisitionError("approved artifact request hash mismatch")
    delivery = proposal["delivery"]
    if approved["approved_canonical_endpoint"] != delivery["canonical_entry_endpoint"]:
        raise AcquisitionError("approved canonical endpoint mismatch")
    expected_hosts = [PUBLIC_ARCHIVE_HOST, *delivery["permitted_redirect_hosts"]]
    if approved["approved_redirect_hosts"] != expected_hosts:
        raise AcquisitionError("approved redirect-host policy mismatch")
    maximum = approved["approved_maximum_bytes"]
    proposed_maximum = proposal["archive_policy"]["proposed_maximum_bytes"]
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0 or maximum > proposed_maximum:
        raise AcquisitionError("approved maximum bytes are invalid")
    expected_release = proposal_request_identity(proposal)["release"]
    if approved["release_identity"] != expected_release:
        raise AcquisitionError("approved release identity mismatch")
    try:
        datetime.fromisoformat(str(approved["approved_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise AcquisitionError("approved_at must be an ISO-8601 timestamp") from exc
    return {
        "proposal_sha256": approved["proposal_sha256"],
        "request_sha256": request_hash,
        "canonical_endpoint": approved["approved_canonical_endpoint"],
        "maximum_bytes": maximum,
        "redirect_hosts": approved["approved_redirect_hosts"],
    }


def validate_retry_authorization(
    proposal: dict[str, Any],
    original_approval: dict[str, Any],
    retry: dict[str, Any],
) -> dict[str, Any]:
    validate_approved_artifact(proposal, original_approval)
    required = {
        "retry_authorization_schema_version",
        "authorization_status",
        "retry_authorized",
        "request_sha256",
        "proposal_sha256",
        "original_approval_sha256",
        "failed_attempt_number",
        "authorized_attempt_number",
        "authorization_reason",
        "maximum_get_attempts",
        "canonical_endpoint",
        "maximum_archive_bytes",
        "expected_content_length",
        "approved_redirect_hosts",
        "release_identity",
        "attempt_1_response_opened",
        "attempt_1_bytes_written",
        "authorized_at",
        "canonical_authorization_sha256",
    }
    missing = required - set(retry)
    if missing:
        raise AcquisitionError(f"retry authorization lacks required fields: {sorted(missing)}")
    expected = {
        "retry_authorization_schema_version": 1,
        "authorization_status": "approved",
        "retry_authorized": True,
        "request_sha256": original_approval["request_sha256"],
        "proposal_sha256": original_approval["proposal_sha256"],
        "original_approval_sha256": sha256_value(original_approval),
        "failed_attempt_number": 1,
        "authorized_attempt_number": 2,
        "maximum_get_attempts": 2,
        "canonical_endpoint": original_approval["approved_canonical_endpoint"],
        "maximum_archive_bytes": original_approval["approved_maximum_bytes"],
        "expected_content_length": original_approval["declared_content_length"],
        "approved_redirect_hosts": original_approval["approved_redirect_hosts"],
        "release_identity": original_approval["release_identity"],
        "attempt_1_response_opened": True,
        "attempt_1_bytes_written": 0,
    }
    for key, value in expected.items():
        if retry.get(key) != value:
            raise AcquisitionError(f"retry authorization mismatch: {key}")
    if not str(retry["authorization_reason"]).strip():
        raise AcquisitionError("retry authorization reason is required")
    try:
        datetime.fromisoformat(str(retry["authorized_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise AcquisitionError("retry authorized_at must be an ISO-8601 timestamp") from exc
    authorization_identity = {
        key: value for key, value in retry.items()
        if key != "canonical_authorization_sha256"
    }
    canonical_hash = sha256_value(authorization_identity)
    if retry["canonical_authorization_sha256"] != canonical_hash:
        raise AcquisitionError("retry canonical authorization hash mismatch")
    return {
        "authorized_attempt_number": 2,
        "maximum_get_attempts": 2,
        "original_approval_sha256": expected["original_approval_sha256"],
        "canonical_authorization_sha256": canonical_hash,
    }


def validate_attempt3_authorization(
    proposal: dict[str, Any],
    original_approval: dict[str, Any],
    attempt2_authorization: dict[str, Any],
    attempt3: dict[str, Any],
) -> dict[str, Any]:
    attempt2 = validate_retry_authorization(proposal, original_approval, attempt2_authorization)
    required = {
        "transfer_authorization_schema_version",
        "authorization_status",
        "transfer_authorized",
        "proposal_sha256",
        "request_sha256",
        "original_approval_sha256",
        "attempt_2_authorization_sha256",
        "previous_attempt_number",
        "authorized_attempt_number",
        "maximum_full_transfer_attempts",
        "release_identity",
        "canonical_endpoint",
        "expected_content_length",
        "maximum_archive_bytes",
        "expected_archive_sha256",
        "member_warning_threshold",
        "member_emergency_ceiling",
        "approved_redirect_hosts",
        "authorized_at",
        "authorization_reason",
        "canonical_authorization_sha256",
    }
    missing = required - set(attempt3)
    if missing:
        raise AcquisitionError(f"attempt-3 authorization lacks fields: {sorted(missing)}")
    expected = {
        "transfer_authorization_schema_version": 1,
        "authorization_status": "approved",
        "transfer_authorized": True,
        "proposal_sha256": original_approval["proposal_sha256"],
        "request_sha256": original_approval["request_sha256"],
        "original_approval_sha256": sha256_value(original_approval),
        "attempt_2_authorization_sha256": attempt2["canonical_authorization_sha256"],
        "previous_attempt_number": 2,
        "authorized_attempt_number": 3,
        "maximum_full_transfer_attempts": 3,
        "release_identity": original_approval["release_identity"],
        "canonical_endpoint": original_approval["approved_canonical_endpoint"],
        "expected_content_length": original_approval["declared_content_length"],
        "maximum_archive_bytes": original_approval["approved_maximum_bytes"],
        "expected_archive_sha256": "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9",
        "member_warning_threshold": 20_000,
        "member_emergency_ceiling": 250_000,
        "approved_redirect_hosts": original_approval["approved_redirect_hosts"],
    }
    for key, value in expected.items():
        if attempt3.get(key) != value:
            raise AcquisitionError(f"attempt-3 authorization mismatch: {key}")
    if not str(attempt3["authorization_reason"]).strip():
        raise AcquisitionError("attempt-3 authorization reason is required")
    try:
        datetime.fromisoformat(str(attempt3["authorized_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise AcquisitionError("attempt-3 authorized_at must be ISO-8601") from exc
    identity = {
        key: value for key, value in attempt3.items()
        if key != "canonical_authorization_sha256"
    }
    canonical_hash = sha256_value(identity)
    if attempt3["canonical_authorization_sha256"] != canonical_hash:
        raise AcquisitionError("attempt-3 canonical authorization hash mismatch")
    return {
        "authorized_attempt_number": 3,
        "maximum_full_transfer_attempts": 3,
        "expected_archive_sha256": expected["expected_archive_sha256"],
        "canonical_authorization_sha256": canonical_hash,
    }


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"invalid JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AcquisitionError(f"JSON artifact must be an object: {path}")
    return value
