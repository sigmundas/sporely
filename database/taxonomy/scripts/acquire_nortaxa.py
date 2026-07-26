#!/usr/bin/env python3
"""Fail-closed, one-attempt NorTaxa archive acquisition executor.

The module is deliberately transport-injectable.  The production adapter is
defined here, but offline tests exercise only injected responses.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from nortaxa_metadata import ZIP_TYPES, evaluate_archive_get, parse_content_length
from refresh_col_xr import AcquisitionError, sha256_file, sha256_json
from refresh_nortaxa import load_request, validate_fixture


MAXIMUM_BYTES = 67_108_864
CANONICAL_ENDPOINT = "https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.284"
ALLOWED_HOST = "ipt.artsdatabanken.no"
SOURCE_CODE = "nortaxa"
PROFILE_CODE = "nortaxa_dwca"
VERSION = "1.284"
ISSUED_DATE = "2026-07-17"
DATASET_UUID = "a6c6cead-b5ce-4a4e-8cf5-1542ba708dec"
SOURCE_SELECTION_PROPOSAL_SHA256 = "e025d53350422d1590836ddc6383f5ed93665ba82ec48db1b3708f2e337a67e3"
MAXIMUM_APPROVAL_LIFETIME_SECONDS = 86_400
REQUEST_SHA256 = "38091edd85d40172539d3086732de2569a00102ff5564c66c55efb59360e7392"
POLICY_RESOLUTION_SHA256 = "771d02ca3c656e43eeb9c448838b25faab443947c20a93dc8451dc5f656918fc"
METADATA_VERIFICATION_SHA256 = "46dc942b3d2724dfc50848427063ba64eb6919031fe8f73d4585c150b1c9b8fa"
ATTEMPT_4_SHA256 = "36b1aa2504d4b6eec998d734916e56ce9ab759e45020b2917ff3bb7c8d715938"
CHUNK_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 180
USER_AGENT = "Sporely-Taxonomy-Acquisition/1 (NorTaxa 1.284)"
EVIDENCE_HEADERS = {
    "content-type", "content-length", "last-modified", "etag",
    "content-disposition", "accept-ranges", "content-encoding",
}
APPROVAL_KEYS = {
    "approval_schema_version", "approval_status", "download_authorized",
    "acquisition_proposal_sha256", "source_selection_proposal_sha256",
    "request_sha256", "policy_resolution_sha256", "metadata_verification_sha256",
    "attempt_4_sha256", "source_code", "profile_code", "version", "issued_date",
    "dataset_uuid", "approved_canonical_endpoint", "allowed_hosts",
    "approved_redirect_hosts",
    "approved_maximum_bytes", "permitted_get_attempts", "redirects_authorized",
    "range_requests_authorized", "retries_authorized", "resume_authorized",
    "fallback_endpoint_authorized", "authentication_authorized",
    "cookies_authorized", "conditional_requests_authorized", "approved_at",
    "expires_at", "superseded_by", "executor_git_sha", "executor_script_sha256",
    "executor_test_evidence_sha256", "executor_readiness_sha256",
}
ATTEMPT_KEYS = {
    "acquisition_attempt_schema_version", "state", "approval_sha256",
    "acquisition_proposal_sha256", "endpoint", "maximum_bytes", "consumed_at",
}
RECEIPT_KEYS = {
    "promotion_receipt_schema_version", "state", "approval_sha256",
    "attempt_sha256", "observed_bytes", "archive_sha256", "response_metadata",
    "validation", "final_path", "staged_name", "staged_device", "staged_inode",
}
RESULT_KEYS = {
    "acquisition_result_schema_version", "result", "source_code", "version",
    "approval_sha256", "attempt_sha256", "receipt_sha256",
    "acquisition_proposal_sha256", "metadata_verification_sha256",
    "attempt_4_sha256", "tool_commit", "executor_script_sha256",
    "observed_bytes", "archive_sha256", "response_metadata", "validation",
    "final_path", "recovered_after_promotion", "recorded_at",
}


class LockContentionError(AcquisitionError):
    """Another process currently owns the release acquisition lock."""


@dataclass(frozen=True)
class GitState:
    head: str
    clean: bool
    committed_executor_sha256: str | None = None
    committed_test_sha256: str | None = None
    committed_executor_blob_id: str | None = None
    committed_test_blob_id: str | None = None


@dataclass(frozen=True)
class Approval:
    raw: dict[str, Any]
    canonical_sha256: str
    maximum_bytes: int
    executor_git_sha: str
    proposal_sha256: str
    readiness_sha256: str


@dataclass
class TransportResponse:
    status: int
    final_url: str
    headers: Mapping[str, str]
    stream: BinaryIO
    close: Callable[[], None] = lambda: None


class Transport(Protocol):
    def open(self, endpoint: str) -> TransportResponse:
        """Open exactly one response for the endpoint."""


@dataclass(frozen=True)
class AcquisitionPaths:
    taxonomy_root: Path
    repository_root: Path

    @property
    def release(self) -> Path:
        return self.taxonomy_root / "sources" / "nortaxa" / VERSION

    @property
    def approval(self) -> Path:
        return self.taxonomy_root / "nortaxa-acquisition.approved.json"

    @property
    def proposal(self) -> Path:
        return self.taxonomy_root / "nortaxa-acquisition.proposal.json"

    @property
    def source_proposal(self) -> Path:
        return self.taxonomy_root / "nortaxa-source-selection.proposal.json"

    @property
    def request(self) -> Path:
        return self.release / "request.json"

    @property
    def metadata(self) -> Path:
        return self.release / "metadata-verification.json"

    @property
    def attempt4(self) -> Path:
        return self.release / "metadata-verification-attempt-4.json"

    @property
    def policy_resolution(self) -> Path:
        return self.release / "policy-resolution.json"

    @property
    def readiness(self) -> Path:
        return self.release / "executor-readiness.json"

    @property
    def lock(self) -> Path:
        return self.release / ".nortaxa-acquisition.lock"

    @property
    def attempt(self) -> Path:
        return self.release / "nortaxa-acquisition-attempt.json"

    @property
    def receipt(self) -> Path:
        return self.release / "nortaxa-acquisition-promotion-ready.json"

    @property
    def result(self) -> Path:
        return self.release / "nortaxa-acquisition-result.json"

    @property
    def final_archive(self) -> Path:
        return self.release / "archive.zip"

    @property
    def staging(self) -> Path:
        return self.release / ".staging"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise AcquisitionError(f"approval {label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise AcquisitionError(f"approval {label} is invalid") from exc
    return parsed


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"cannot load required JSON artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AcquisitionError(f"required JSON artifact is not an object: {path}")
    return value


def _self_bound_sha(path: Path, label: str = "artifact") -> str:
    value = _load_json(path)
    declared = value.pop("canonical_sha256", None)
    observed = sha256_json(value)
    if declared != observed:
        raise AcquisitionError(f"{label} self-bound canonical hash mismatch: {path}")
    return observed


def _file_canonical_sha(path: Path) -> str:
    return sha256_json(_load_json(path))


def _executor_sha() -> str:
    return sha256_file(Path(__file__).resolve())


def _executor_test_sha() -> str:
    return sha256_file(
        Path(__file__).resolve().parents[1] / "tests" / "test_acquire_nortaxa.py"
    )


def _parse_porcelain(status: str) -> list[tuple[str, str]]:
    """Return (xy, path) entries from `git status --porcelain=v1` output."""
    entries: list[tuple[str, str]] = []
    for line in status.splitlines():
        if len(line) < 4:
            continue
        entries.append((line[:2], line[3:]))
    return entries


def current_git_state(
    repository_root: Path,
    *,
    allowed_untracked_paths: frozenset[str] | None = None,
) -> GitState:
    """Inspect the current commit and working tree.

    ``clean`` is True when the working tree is clean OR when the only entries
    reported by ``git status`` are untracked additions at exactly one of the
    ``allowed_untracked_paths`` (typically the separately supplied approval
    artifact at its exact permitted repo-relative path).
    """
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain=v1"], cwd=repository_root, check=True,
            capture_output=True, text=True,
        ).stdout
        executor_relative = Path(__file__).resolve().relative_to(repository_root).as_posix()
        test_relative = (
            Path(__file__).resolve().parents[1] / "tests" / "test_acquire_nortaxa.py"
        ).relative_to(repository_root).as_posix()
        committed_executor = subprocess.run(
            ["git", "show", f"{head}:{executor_relative}"],
            cwd=repository_root, check=True, capture_output=True,
        ).stdout
        committed_test = subprocess.run(
            ["git", "show", f"{head}:{test_relative}"],
            cwd=repository_root, check=True, capture_output=True,
        ).stdout
        # Committed Git blob IDs at HEAD (deterministic, not derived from
        # working-tree file contents).
        executor_blob = subprocess.run(
            ["git", "rev-parse", f"{head}:{executor_relative}"],
            cwd=repository_root, check=True, capture_output=True, text=True,
        ).stdout.strip()
        test_blob = subprocess.run(
            ["git", "rev-parse", f"{head}:{test_relative}"],
            cwd=repository_root, check=True, capture_output=True, text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AcquisitionError(f"cannot establish Git state: {exc}") from exc
    except ValueError as exc:
        raise AcquisitionError("executor paths are outside the repository root") from exc
    allowed = frozenset(allowed_untracked_paths or ())
    entries = _parse_porcelain(status)
    non_conforming = [
        (xy, path) for xy, path in entries
        if not (xy == "??" and path in allowed)
    ]
    return GitState(
        head=head,
        clean=not non_conforming,
        committed_executor_sha256=hashlib.sha256(committed_executor).hexdigest(),
        committed_test_sha256=hashlib.sha256(committed_test).hexdigest(),
        committed_executor_blob_id=executor_blob,
        committed_test_blob_id=test_blob,
    )


def validate_approval(
    raw: dict[str, Any],
    *,
    paths: AcquisitionPaths,
    git: GitState,
    now: datetime,
) -> Approval:
    if set(raw) != APPROVAL_KEYS:
        unknown = sorted(set(raw) - APPROVAL_KEYS)
        missing = sorted(APPROVAL_KEYS - set(raw))
        raise AcquisitionError(f"approval fields differ; unknown={unknown}, missing={missing}")
    if not git.clean:
        raise AcquisitionError("working tree must be clean (only the approval artifact may be untracked)")
    # Fixed authority-reducing constants. `acquisition_proposal_sha256` is
    # explicitly NOT compared to a compile-time constant; it is computed at
    # runtime from the committed on-disk proposal and validated below.
    expected = {
        "approval_schema_version": 1,
        "approval_status": "approved",
        "download_authorized": True,
        "source_selection_proposal_sha256": SOURCE_SELECTION_PROPOSAL_SHA256,
        "request_sha256": REQUEST_SHA256,
        "policy_resolution_sha256": POLICY_RESOLUTION_SHA256,
        "metadata_verification_sha256": METADATA_VERIFICATION_SHA256,
        "attempt_4_sha256": ATTEMPT_4_SHA256,
        "source_code": SOURCE_CODE,
        "profile_code": PROFILE_CODE,
        "version": VERSION,
        "issued_date": ISSUED_DATE,
        "dataset_uuid": DATASET_UUID,
        "approved_canonical_endpoint": CANONICAL_ENDPOINT,
        "allowed_hosts": [ALLOWED_HOST],
        "approved_redirect_hosts": [],
        "permitted_get_attempts": 1,
        "redirects_authorized": False,
        "range_requests_authorized": False,
        "retries_authorized": False,
        "resume_authorized": False,
        "fallback_endpoint_authorized": False,
        "authentication_authorized": False,
        "cookies_authorized": False,
        "conditional_requests_authorized": False,
        "superseded_by": None,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise AcquisitionError(f"approval {key} must be {value!r}")
    maximum = raw.get("approved_maximum_bytes")
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 0 < maximum <= MAXIMUM_BYTES:
        raise AcquisitionError("approval maximum broadens or invalidates the archive ceiling")
    if raw.get("executor_git_sha") != git.head or len(git.head) != 40:
        raise AcquisitionError("approval executor Git SHA does not match current HEAD")
    if raw.get("executor_script_sha256") != _executor_sha():
        raise AcquisitionError("approval executor script hash does not match")
    if raw.get("executor_test_evidence_sha256") != _executor_test_sha():
        raise AcquisitionError("approval executor test evidence hash does not match")
    if (
        git.committed_executor_sha256 != _executor_sha()
        or git.committed_test_sha256 != _executor_test_sha()
    ):
        raise AcquisitionError("approved executor and test evidence must be committed at current HEAD")

    # Runtime-computed acquisition-proposal canonical SHA.
    proposal_sha = _self_bound_sha(paths.proposal, "acquisition proposal")
    if raw.get("acquisition_proposal_sha256") != proposal_sha:
        raise AcquisitionError(
            "approval acquisition_proposal_sha256 does not match on-disk proposal"
        )

    # Runtime-computed executor-readiness canonical SHA.
    readiness_sha = _self_bound_sha(paths.readiness, "executor readiness")
    if raw.get("executor_readiness_sha256") != readiness_sha:
        raise AcquisitionError(
            "approval executor_readiness_sha256 does not match on-disk readiness"
        )

    # Readiness must bind the committed executor and test blob identities
    # that Git reports for the current HEAD. This is the sole authoritative
    # link between an audit-time attestation and the executor's runtime.
    readiness = _load_json(paths.readiness)
    exe_binding = readiness.get("executor") or {}
    test_binding = readiness.get("executor_tests") or {}
    if (
        exe_binding.get("committed_blob_sha256") != git.committed_executor_sha256
        or exe_binding.get("committed_git_blob_id") != git.committed_executor_blob_id
    ):
        raise AcquisitionError(
            "readiness executor bindings do not match the committed executor at HEAD"
        )
    if (
        test_binding.get("committed_blob_sha256") != git.committed_test_sha256
        or test_binding.get("committed_git_blob_id") != git.committed_test_blob_id
    ):
        raise AcquisitionError(
            "readiness executor-test bindings do not match the committed tests at HEAD"
        )
    if readiness.get("executor_ready") is not True:
        raise AcquisitionError("readiness executor_ready is not True")

    # Time policy: unambiguous UTC ISO8601, positive duration, within the
    # 24-hour lifetime maintainer decision, and current time inside the window.
    approved_at = _parse_time(raw.get("approved_at"), "approved_at")
    expires_at = _parse_time(raw.get("expires_at"), "expires_at")
    if expires_at <= approved_at:
        raise AcquisitionError("approval expires_at must be strictly after approved_at")
    lifetime = (expires_at - approved_at).total_seconds()
    if lifetime > MAXIMUM_APPROVAL_LIFETIME_SECONDS:
        raise AcquisitionError(
            f"approval lifetime {lifetime:.0f}s exceeds maximum "
            f"{MAXIMUM_APPROVAL_LIFETIME_SECONDS}s policy"
        )
    if approved_at > now or expires_at <= now:
        raise AcquisitionError("approval is not currently valid or has expired")

    # Immutable evidence hashes still pinned as constants; only the acquisition
    # proposal's hash is computed at runtime from disk.
    artifact_hashes = {
        "source-selection proposal": (_file_canonical_sha(paths.source_proposal), SOURCE_SELECTION_PROPOSAL_SHA256),
        "request": (load_request(paths.request, paths.source_proposal).request_sha256, REQUEST_SHA256),
        "policy resolution": (
            _self_bound_sha(paths.policy_resolution, "policy resolution"),
            POLICY_RESOLUTION_SHA256,
        ),
        "metadata verification": (
            _self_bound_sha(paths.metadata, "metadata verification"),
            METADATA_VERIFICATION_SHA256,
        ),
        "attempt 4": (_file_canonical_sha(paths.attempt4), ATTEMPT_4_SHA256),
    }
    for label, (observed, pinned) in artifact_hashes.items():
        if observed != pinned:
            raise AcquisitionError(f"{label} canonical hash differs from pinned evidence")
    return Approval(
        raw=dict(raw), canonical_sha256=sha256_json(raw),
        maximum_bytes=maximum, executor_git_sha=git.head,
        proposal_sha256=proposal_sha, readiness_sha256=readiness_sha,
    )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ProductionHTTPTransport:
    """One-call urllib adapter with redirects and ambient credentials disabled."""

    def __init__(self) -> None:
        self._used = False
        self.request_headers: dict[str, str] | None = None

    def open(self, endpoint: str) -> TransportResponse:
        if self._used:
            raise AcquisitionError("production transport instance permits exactly one GET")
        self._used = True
        if endpoint != CANONICAL_ENDPOINT:
            raise AcquisitionError("production transport endpoint mismatch")
        headers = {
            "Accept": "application/zip, application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        }
        prohibited = {
            "authorization", "cookie", "range", "if-match", "if-none-match",
            "if-modified-since", "if-unmodified-since",
        }
        if prohibited.intersection(key.casefold() for key in headers):
            raise AcquisitionError("production request contains prohibited headers")
        self.request_headers = dict(headers)
        request = Request(endpoint, method="GET", headers=headers)
        # Do not inherit proxy credentials or routing from the environment.
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        try:
            response = opener.open(request, timeout=TIMEOUT_SECONDS)
        except HTTPError as exc:
            raise AcquisitionError(f"archive GET returned HTTP {exc.code}") from exc
        except (URLError, OSError) as exc:
            raise AcquisitionError(f"archive GET failed: {exc}") from exc
        get_all = getattr(response.headers, "get_all", None)
        if get_all is None:
            response_headers = dict(response.headers.items())
        else:
            response_headers = {}
            for key in EVIDENCE_HEADERS:
                values = get_all(key, [])
                if values:
                    response_headers[key] = ", ".join(str(value) for value in values)
        return TransportResponse(
            status=response.status,
            final_url=response.geturl(),
            headers=response_headers,
            stream=response,
            close=response.close,
        )


def _bounded_headers(headers: Mapping[str, str]) -> tuple[dict[str, dict[str, Any]], str | None]:
    evidence: dict[str, dict[str, Any]] = {}
    total = 0
    repeated: dict[str, list[str]] = {}
    for raw_key, raw_value in headers.items():
        key = str(raw_key).casefold()
        if key in EVIDENCE_HEADERS:
            repeated.setdefault(key, []).append(str(raw_value).strip())
    normalized = {key: ", ".join(values) for key, values in repeated.items()}
    for key in sorted(EVIDENCE_HEADERS):
        present = key in normalized
        value = normalized.get(key)
        if value is not None:
            encoded = value.encode("utf-8")
            total += len(key) + len(encoded)
            if len(encoded) > 1024 or total > 8192:
                raise AcquisitionError("response header evidence exceeds bounded policy")
        evidence[key] = {"present": present, "value": value}
    return evidence, normalized.get("content-length")


def _validate_response(response: TransportResponse, maximum: int) -> tuple[dict[str, Any], str | None]:
    if response.status != 200:
        raise AcquisitionError(f"archive GET requires HTTP 200, observed {response.status}")
    if response.final_url != CANONICAL_ENDPOINT:
        raise AcquisitionError("archive GET final URL differs from the canonical endpoint")
    parsed = urlparse(response.final_url)
    if (
        parsed.scheme != "https" or parsed.hostname != ALLOWED_HOST
        or parsed.username or parsed.password or parsed.port not in {None, 443}
    ):
        raise AcquisitionError("archive GET final host or authority differs")
    evidence, length_header = _bounded_headers(response.headers)
    content_encoding = evidence["content-encoding"]["value"]
    if content_encoding is not None and content_encoding.casefold() != "identity":
        raise AcquisitionError(
            f"archive GET Content-Encoding must be absent or identity: {content_encoding!r}"
        )
    content_type = evidence["content-type"]["value"]
    media_type = content_type.split(";", 1)[0].strip().casefold() if content_type else ""
    if media_type not in ZIP_TYPES:
        raise AcquisitionError(f"archive GET Content-Type is unsupported: {media_type!r}")
    # Reuse the committed parser before any body read.
    parse_content_length(length_header, maximum)
    return {"headers": evidence, "content_type": media_type}, length_header


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_exclusive(path: Path, value: dict[str, Any]) -> None:
    _require_real_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        flags = (
            os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, indent=2, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise AcquisitionError(f"append-only artifact already exists: {path}") from exc
        _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _path_lexists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise AcquisitionError(f"cannot inspect acquisition path {path}: {exc}") from exc
    return True


def _require_regular_file(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise AcquisitionError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise AcquisitionError(f"{label} must be a real regular file: {path}")


def _require_absent(path: Path, label: str) -> None:
    if _path_lexists(path):
        raise AcquisitionError(f"{label} already exists: {path}")


def _require_real_directory(path: Path, *, beneath: Path | None = None) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise AcquisitionError(f"required directory is unavailable: {path}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise AcquisitionError(f"required directory must be a real directory: {path}")
    if beneath is not None:
        try:
            path.resolve(strict=True).relative_to(beneath.resolve(strict=True))
        except (OSError, ValueError) as exc:
            raise AcquisitionError(f"directory escapes its approved root: {path}") from exc


def _require_real_directory_chain(path: Path, *, root: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise AcquisitionError(f"directory is not lexically beneath its approved root: {path}") from exc
    _require_real_directory(root)
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        _require_real_directory(cursor, beneath=root)


def _validate_staging_directory(
    paths: AcquisitionPaths,
    *,
    allowed_name: str | None = None,
) -> None:
    if not _path_lexists(paths.staging):
        return
    _require_real_directory(paths.staging, beneath=paths.release)
    names = {entry.name for entry in paths.staging.iterdir()}
    allowed = {allowed_name} if allowed_name is not None else set()
    if not names.issubset(allowed):
        raise AcquisitionError("staging directory contains an unrelated payload")


def _discard_consumed_staging(paths: AcquisitionPaths) -> None:
    if not _path_lexists(paths.staging):
        return
    _require_real_directory(paths.staging, beneath=paths.release)
    for entry in paths.staging.iterdir():
        if (
            not entry.name.startswith(f"nortaxa-{VERSION}-")
            or not entry.name.endswith(".part")
        ):
            raise AcquisitionError("consumed staging directory contains an unrelated payload")
        _require_regular_file(entry, "consumed staging payload")
        entry.unlink()
    _fsync_directory(paths.staging)
    with contextlib.suppress(OSError):
        paths.staging.rmdir()


@contextlib.contextmanager
def release_lock(path: Path):
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
        handle = os.fdopen(descriptor, "a+b")
    except OSError as exc:
        raise AcquisitionError(f"cannot safely open acquisition lock: {path}") from exc
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LockContentionError("NorTaxa 1.284 acquisition lock is already held") from exc
        opened = os.fstat(handle.fileno())
        linked = path.lstat()
        if (
            stat.S_ISLNK(linked.st_mode) or not stat.S_ISREG(linked.st_mode)
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
        ):
            raise AcquisitionError("acquisition lock path changed while it was opened")
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _result_value(
    *,
    approval: Approval,
    receipt: dict[str, Any],
    paths: AcquisitionPaths,
    recovered: bool,
    now: datetime,
) -> dict[str, Any]:
    return {
        "acquisition_result_schema_version": 1,
        "result": "passed",
        "source_code": SOURCE_CODE,
        "version": VERSION,
        "approval_sha256": approval.canonical_sha256,
        "attempt_sha256": receipt["attempt_sha256"],
        "receipt_sha256": sha256_json(receipt),
        "acquisition_proposal_sha256": approval.proposal_sha256,
        "metadata_verification_sha256": METADATA_VERIFICATION_SHA256,
        "attempt_4_sha256": ATTEMPT_4_SHA256,
        "tool_commit": approval.executor_git_sha,
        "executor_script_sha256": _executor_sha(),
        "observed_bytes": receipt["observed_bytes"],
        "archive_sha256": receipt["archive_sha256"],
        "response_metadata": receipt["response_metadata"],
        "validation": receipt["validation"],
        "final_path": str(paths.final_archive.relative_to(paths.repository_root)),
        "recovered_after_promotion": recovered,
        "recorded_at": isoformat(now),
    }


def _validation_evidence(report: dict[str, Any]) -> dict[str, Any]:
    if report.get("result") != "passed":
        raise AcquisitionError("structural validator did not return its passed result")
    evidence = dict(report)
    evidence.pop("result")
    return {"status": "structurally_validated", **evidence}


def _validate_attempt_record(value: dict[str, Any], approval: Approval) -> str:
    if set(value) != ATTEMPT_KEYS:
        raise AcquisitionError("consumed-attempt journal has unknown or missing fields")
    expected = {
        "acquisition_attempt_schema_version": 1,
        "state": "network_attempt_consumed",
        "approval_sha256": approval.canonical_sha256,
        "acquisition_proposal_sha256": approval.proposal_sha256,
        "endpoint": CANONICAL_ENDPOINT,
        "maximum_bytes": approval.maximum_bytes,
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise AcquisitionError("consumed-attempt journal does not match the approval")
    consumed_at = _parse_time(value.get("consumed_at"), "consumed_at")
    approved_at = _parse_time(approval.raw["approved_at"], "approved_at")
    expires_at = _parse_time(approval.raw["expires_at"], "expires_at")
    if not approved_at <= consumed_at < expires_at:
        raise AcquisitionError("consumed-attempt timestamp is outside the approval window")
    return sha256_json(value)


def _validate_receipt_record(
    value: dict[str, Any],
    *,
    paths: AcquisitionPaths,
    approval: Approval,
    attempt_sha256: str,
) -> None:
    if set(value) != RECEIPT_KEYS:
        raise AcquisitionError("promotion receipt has unknown or missing fields")
    expected = {
        "promotion_receipt_schema_version": 1,
        "state": "validated_ready_for_atomic_promotion",
        "approval_sha256": approval.canonical_sha256,
        "attempt_sha256": attempt_sha256,
        "final_path": str(paths.final_archive.relative_to(paths.repository_root)),
    }
    if any(value.get(key) != expected_value for key, expected_value in expected.items()):
        raise AcquisitionError("promotion receipt does not match the consumed authorization")
    observed = value.get("observed_bytes")
    archive_hash = value.get("archive_sha256")
    if (
        isinstance(observed, bool) or not isinstance(observed, int)
        or not 0 < observed <= approval.maximum_bytes
        or not isinstance(archive_hash, str) or len(archive_hash) != 64
        or any(character not in "0123456789abcdef" for character in archive_hash)
    ):
        raise AcquisitionError("promotion receipt archive identity is invalid")
    staged_name = value.get("staged_name")
    if (
        not isinstance(staged_name, str)
        or Path(staged_name).name != staged_name
        or not staged_name.startswith(f"nortaxa-{VERSION}-")
        or not staged_name.endswith(".part")
    ):
        raise AcquisitionError("promotion receipt staging identity is invalid")
    if (
        isinstance(value.get("staged_device"), bool)
        or not isinstance(value.get("staged_device"), int)
        or isinstance(value.get("staged_inode"), bool)
        or not isinstance(value.get("staged_inode"), int)
        or value["staged_device"] < 0 or value["staged_inode"] <= 0
    ):
        raise AcquisitionError("promotion receipt staging file identity is invalid")
    metadata = value.get("response_metadata")
    if not isinstance(metadata, dict) or set(metadata) != {
        "headers", "content_type", "status", "final_url", "stream_policy",
    }:
        raise AcquisitionError("promotion receipt response metadata is invalid")
    headers = metadata.get("headers")
    if not isinstance(headers, dict) or set(headers) != EVIDENCE_HEADERS:
        raise AcquisitionError("promotion receipt header evidence is invalid")
    for key, item in headers.items():
        if (
            not isinstance(item, dict) or set(item) != {"present", "value"}
            or not isinstance(item.get("present"), bool)
            or (item["value"] is not None and not isinstance(item["value"], str))
            or item["present"] != (item["value"] is not None)
        ):
            raise AcquisitionError(f"promotion receipt {key} evidence is invalid")
    content_type = headers["content-type"]["value"]
    media_type = content_type.split(";", 1)[0].strip().casefold() if content_type else ""
    content_encoding = headers["content-encoding"]["value"]
    if (
        metadata.get("status") != 200
        or metadata.get("final_url") != CANONICAL_ENDPOINT
        or metadata.get("content_type") != media_type
        or media_type not in ZIP_TYPES
        or (content_encoding is not None and content_encoding.casefold() != "identity")
    ):
        raise AcquisitionError("promotion receipt response authority is invalid")
    expected_policy = evaluate_archive_get(
        ceiling=approval.maximum_bytes,
        content_length_header=headers["content-length"]["value"],
        completed_bytes=observed,
        reached_eof=True,
    )
    if metadata.get("stream_policy") != expected_policy:
        raise AcquisitionError("promotion receipt stream policy is inconsistent")
    validation = value.get("validation")
    if not isinstance(validation, dict):
        raise AcquisitionError("promotion receipt validation evidence is invalid")
    archive_validation = validation.get("archive")
    if (
        validation.get("status") != "structurally_validated"
        or validation.get("profile_code") != PROFILE_CODE
        or validation.get("request_definition_sha256") != REQUEST_SHA256
        or validation.get("network_calls") != 0
        or not isinstance(archive_validation, dict)
        or archive_validation.get("bytes") != observed
        or archive_validation.get("sha256") != archive_hash
    ):
        raise AcquisitionError("promotion receipt structural validation is inconsistent")


def _remove_recovered_staging(paths: AcquisitionPaths, receipt: dict[str, Any]) -> None:
    staged = paths.staging / receipt["staged_name"]
    if not _path_lexists(staged):
        return
    _require_regular_file(staged, "recovered staging payload")
    staged_info = staged.lstat()
    if (
        staged_info.st_dev != receipt["staged_device"]
        or staged_info.st_ino != receipt["staged_inode"]
        or staged_info.st_size != receipt["observed_bytes"]
        or sha256_file(staged) != receipt["archive_sha256"]
    ):
        raise AcquisitionError("recovered staging payload differs from the promotion receipt")
    staged.unlink()
    _fsync_directory(paths.staging)
    with contextlib.suppress(OSError):
        paths.staging.rmdir()


def _recover_promoted(
    *,
    paths: AcquisitionPaths,
    approval: Approval,
    now: datetime,
) -> dict[str, Any]:
    attempt = _load_json(paths.attempt)
    receipt = _load_json(paths.receipt)
    attempt_sha256 = _validate_attempt_record(attempt, approval)
    _validate_receipt_record(
        receipt, paths=paths, approval=approval, attempt_sha256=attempt_sha256,
    )
    _validate_staging_directory(paths, allowed_name=receipt["staged_name"])
    _require_regular_file(paths.final_archive, "promoted archive")
    promoted_info = paths.final_archive.lstat()
    if (
        promoted_info.st_dev != receipt["staged_device"]
        or promoted_info.st_ino != receipt["staged_inode"]
    ):
        raise AcquisitionError("promoted archive is not the receipt-bound staging file")
    if (
        paths.final_archive.stat().st_size != receipt.get("observed_bytes")
        or sha256_file(paths.final_archive) != receipt.get("archive_sha256")
    ):
        raise AcquisitionError("promoted archive differs from durable promotion receipt")
    validation = _validation_evidence(validate_fixture(
        paths.final_archive, load_request(paths.request, paths.source_proposal),
    ))
    if validation != receipt.get("validation"):
        raise AcquisitionError("recovered archive structural validation differs from receipt")
    # Recovery never opens transport. It makes an observed post-link archive
    # durable before recording success and removes only the receipt-bound twin
    # left by a crash between link and unlink.
    _fsync_directory(paths.release)
    _remove_recovered_staging(paths, receipt)
    result = _result_value(
        approval=approval, receipt=receipt, paths=paths, recovered=True, now=now,
    )
    _write_json_exclusive(paths.result, result)
    return result


def _validate_completed_result(
    *,
    paths: AcquisitionPaths,
    approval: Approval,
    result: dict[str, Any],
) -> dict[str, Any]:
    if set(result) != RESULT_KEYS:
        raise AcquisitionError("completed result has unknown or missing fields")
    _require_regular_file(paths.attempt, "consumed-attempt journal")
    _require_regular_file(paths.receipt, "promotion receipt")
    _require_regular_file(paths.final_archive, "promoted archive")
    receipt = _load_json(paths.receipt)
    attempt = _load_json(paths.attempt)
    attempt_sha256 = _validate_attempt_record(attempt, approval)
    _validate_receipt_record(
        receipt, paths=paths, approval=approval, attempt_sha256=attempt_sha256,
    )
    _validate_staging_directory(paths)
    promoted_info = paths.final_archive.lstat()
    if (
        promoted_info.st_dev != receipt["staged_device"]
        or promoted_info.st_ino != receipt["staged_inode"]
    ):
        raise AcquisitionError("completed archive is not the receipt-bound staging file")
    recovered = result.get("recovered_after_promotion")
    if not isinstance(recovered, bool):
        raise AcquisitionError("completed result recovery state is invalid")
    recorded_at = _parse_time(result.get("recorded_at"), "recorded_at")
    expected_result = _result_value(
        approval=approval, receipt=receipt, paths=paths,
        recovered=recovered, now=recorded_at,
    )
    if result != expected_result:
        raise AcquisitionError("completed result does not match its durable evidence")
    observed_size = paths.final_archive.stat().st_size
    observed_hash = sha256_file(paths.final_archive)
    if (
        observed_size != receipt.get("observed_bytes")
        or observed_size != result.get("observed_bytes")
        or observed_hash != receipt.get("archive_sha256")
        or observed_hash != result.get("archive_sha256")
    ):
        raise AcquisitionError("completed archive differs from durable result evidence")
    validation = _validation_evidence(validate_fixture(
        paths.final_archive, load_request(paths.request, paths.source_proposal),
    ))
    if validation != receipt.get("validation") or validation != result.get("validation"):
        raise AcquisitionError("completed archive validation differs from durable result evidence")
    return result


def acquire(
    *,
    paths: AcquisitionPaths,
    approval_path: Path | None = None,
    transport: Transport | None = None,
    git_state: GitState | None = None,
    clock: Callable[[], datetime] = utc_now,
    free_space: Callable[[Path], int] = lambda path: shutil.disk_usage(path).free,
    preflight_report: Callable[[int, int, Path], None] = lambda expected, available, path: None,
    transition: Callable[[str], None] = lambda state: None,
) -> dict[str, Any]:
    """Execute or recover the pinned acquisition under an exclusive lock."""
    approval_path = approval_path or paths.approval
    now = clock()
    _require_real_directory_chain(paths.taxonomy_root, root=paths.repository_root)
    _require_real_directory_chain(paths.release, root=paths.taxonomy_root)
    raw_approval = _load_json(approval_path)
    # The approval artifact at its exact repo-relative path is the sole
    # untracked/modified path the working tree may contain.
    try:
        approval_repo_relative = approval_path.resolve().relative_to(
            paths.repository_root.resolve()
        ).as_posix()
        allowed_untracked = frozenset({approval_repo_relative})
    except ValueError:
        allowed_untracked = frozenset()
    approval = validate_approval(
        raw_approval, paths=paths,
        git=(git_state
             or current_git_state(paths.repository_root,
                                  allowed_untracked_paths=allowed_untracked)),
        now=now,
    )
    request = load_request(paths.request, paths.source_proposal)

    with release_lock(paths.lock):
        transition("lock_acquired")
        if _path_lexists(paths.staging):
            _require_real_directory(paths.staging, beneath=paths.release)
        if _path_lexists(paths.result):
            _require_regular_file(paths.result, "acquisition result")
            return _validate_completed_result(
                paths=paths, approval=approval, result=_load_json(paths.result),
            )
        if _path_lexists(paths.attempt):
            _require_regular_file(paths.attempt, "consumed-attempt journal")
            if _path_lexists(paths.final_archive) and _path_lexists(paths.receipt):
                _require_regular_file(paths.receipt, "promotion receipt")
                _require_regular_file(paths.final_archive, "promoted archive")
                return _recover_promoted(paths=paths, approval=approval, now=now)
            if _path_lexists(paths.receipt):
                _require_regular_file(paths.receipt, "promotion receipt")
            _discard_consumed_staging(paths)
            raise AcquisitionError("the approval's one GET authorization was already consumed")
        _require_absent(paths.final_archive, "final archive")
        _require_absent(paths.receipt, "promotion receipt")
        _require_absent(paths.result, "acquisition result")

        paths.staging.mkdir(parents=True, exist_ok=True)
        _require_real_directory(paths.staging, beneath=paths.release)
        if any(paths.staging.iterdir()):
            raise AcquisitionError("staging directory is not empty before attempt consumption")
        if os.stat(paths.staging).st_dev != os.stat(paths.release).st_dev:
            raise AcquisitionError("staging and final archive are on different filesystems")
        available = free_space(paths.staging)
        preflight_report(approval.maximum_bytes, available, paths.staging)
        if available < approval.maximum_bytes:
            raise AcquisitionError(
                f"insufficient free space: {available} < approved ceiling {approval.maximum_bytes}"
            )
        transition("local_preflight_complete")
        attempt = {
            "acquisition_attempt_schema_version": 1,
            "state": "network_attempt_consumed",
            "approval_sha256": approval.canonical_sha256,
            "acquisition_proposal_sha256": approval.proposal_sha256,
            "endpoint": CANONICAL_ENDPOINT,
            "maximum_bytes": approval.maximum_bytes,
            "consumed_at": isoformat(now),
        }
        _write_json_exclusive(paths.attempt, attempt)
        transition("attempt_consumed")

        staged = paths.staging / f"nortaxa-{VERSION}-{uuid.uuid4().hex}.part"
        response: TransportResponse | None = None
        promoted = False
        try:
            with staged.open("xb") as output:
                transition("staging_created")
                chosen_transport = transport or ProductionHTTPTransport()
                response = chosen_transport.open(CANONICAL_ENDPOINT)
                response_metadata, length_header = _validate_response(
                    response, approval.maximum_bytes,
                )
                digest = hashlib.sha256()
                received = 0
                reached_eof = False
                while True:
                    remaining = approval.maximum_bytes - received
                    requested = min(CHUNK_BYTES, remaining + 1)
                    chunk = response.stream.read(requested)
                    if not chunk:
                        reached_eof = True
                        break
                    if len(chunk) > requested:
                        raise AcquisitionError("archive stream returned more bytes than requested")
                    if len(chunk) > remaining:
                        raise AcquisitionError("archive stream exceeds the approved ceiling")
                    output.write(chunk)
                    digest.update(chunk)
                    received += len(chunk)
                if received == 0:
                    raise AcquisitionError("archive GET returned an empty body")
                stream_policy = evaluate_archive_get(
                    ceiling=approval.maximum_bytes,
                    content_length_header=length_header,
                    completed_bytes=received,
                    reached_eof=reached_eof,
                )
                output.flush()
                os.fsync(output.fileno())
            transition("stream_complete")
            _require_regular_file(staged, "staged archive")
            validation = _validation_evidence(validate_fixture(staged, request))
            transition("structural_validation_complete")
            staged_info = staged.lstat()
            if (
                staged_info.st_size != received
                or validation.get("archive", {}).get("bytes") != received
                or validation.get("archive", {}).get("sha256") != digest.hexdigest()
            ):
                raise AcquisitionError("validated staging bytes differ from the streamed archive")
            receipt = {
                "promotion_receipt_schema_version": 1,
                "state": "validated_ready_for_atomic_promotion",
                "approval_sha256": approval.canonical_sha256,
                "attempt_sha256": sha256_json(attempt),
                "observed_bytes": received,
                "archive_sha256": digest.hexdigest(),
                "response_metadata": {
                    **response_metadata,
                    "status": response.status,
                    "final_url": response.final_url,
                    "stream_policy": stream_policy,
                },
                "validation": validation,
                "final_path": str(paths.final_archive.relative_to(paths.repository_root)),
                "staged_name": staged.name,
                "staged_device": staged_info.st_dev,
                "staged_inode": staged_info.st_ino,
            }
            _validate_receipt_record(
                receipt, paths=paths, approval=approval,
                attempt_sha256=sha256_json(attempt),
            )
            _write_json_exclusive(paths.receipt, receipt)
            transition("promotion_receipt_persisted")
            _require_absent(paths.final_archive, "final archive")
            try:
                os.link(staged, paths.final_archive, follow_symlinks=False)
            except FileExistsError as exc:
                raise AcquisitionError("final archive appeared before promotion") from exc
            promoted = True
            transition("archive_linked")
            staged.unlink()
            _fsync_directory(paths.release)
            transition("archive_promoted")
            result = _result_value(
                approval=approval, receipt=receipt, paths=paths,
                recovered=False, now=clock(),
            )
            _write_json_exclusive(paths.result, result)
            transition("result_persisted")
            return result
        finally:
            if response is not None:
                with contextlib.suppress(BaseException):
                    response.close()
            if not promoted:
                # Explicit policy: every unpromoted partial/validated staging
                # payload is deleted; the consumed-attempt journal is retained.
                with contextlib.suppress(FileNotFoundError):
                    staged.unlink()
            with contextlib.suppress(OSError):
                paths.staging.rmdir()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--approval", type=Path,
        default=Path(__file__).resolve().parents[1] / "nortaxa-acquisition.approved.json",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    taxonomy = Path(__file__).resolve().parents[1]
    repository = taxonomy.parents[1]
    try:
        result = acquire(
            paths=AcquisitionPaths(taxonomy_root=taxonomy, repository_root=repository),
            approval_path=args.approval,
            preflight_report=lambda expected, available, path: print(
                f"approved maximum bytes: {expected}; available bytes at {path}: {available}",
                file=sys.stderr,
            ),
        )
    except AcquisitionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
