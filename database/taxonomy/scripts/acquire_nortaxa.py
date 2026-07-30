#!/usr/bin/env python3
"""Simple manual NorTaxa 1.284 archive acquisition.

Manual invocation of this script with ``--execute`` is the acquisition
authorization. Each invocation may open at most one HTTP GET to the pinned
public endpoint. There is no automatic retry, no redirect following, no
Range/resume, no authentication, and no automatic recovery.

A hard crash may leave a stale ``.part`` file in ``.staging/``; the next
invocation will refuse and tell the maintainer to inspect it manually. That
refusal is intentional.
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
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from nortaxa_metadata import ZIP_TYPES, evaluate_archive_get, parse_content_length
from refresh_col_xr import AcquisitionError
from refresh_nortaxa import load_request, validate_fixture


MAXIMUM_BYTES = 67_108_864
CANONICAL_ENDPOINT = "https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.284"
ALLOWED_HOST = "ipt.artsdatabanken.no"
VERSION = "1.284"
USER_AGENT = "Sporely-Taxonomy-Acquisition/2 (NorTaxa 1.284)"
CHUNK_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 180
EVIDENCE_HEADERS = (
    "content-type", "content-length", "last-modified", "etag",
    "content-disposition", "accept-ranges", "content-encoding",
)


class LockContentionError(AcquisitionError):
    """Another process currently owns the release lock."""


@dataclass
class TransportResponse:
    status: int
    final_url: str
    headers: Mapping[str, str]
    stream: BinaryIO
    close: Callable[[], None] = lambda: None


class Transport(Protocol):
    def open(self, endpoint: str) -> TransportResponse: ...


@dataclass(frozen=True)
class ReleasePaths:
    taxonomy_root: Path

    @property
    def release(self) -> Path:
        return self.taxonomy_root / "sources" / "nortaxa" / VERSION

    @property
    def request(self) -> Path:
        return self.release / "request.json"

    @property
    def source_proposal(self) -> Path:
        return self.taxonomy_root / "nortaxa-source-selection.proposal.json"

    @property
    def manifest(self) -> Path:
        return self.release / "manifest.json"

    @property
    def archive(self) -> Path:
        return self.release / "archive.zip"

    @property
    def staging(self) -> Path:
        return self.release / ".staging"

    @property
    def lock(self) -> Path:
        return self.release / ".nortaxa-acquisition.lock"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ProductionHTTPTransport:
    """One-call urllib adapter. Redirects and ambient proxies are disabled."""

    def __init__(self) -> None:
        self._used = False

    def open(self, endpoint: str) -> TransportResponse:
        if self._used:
            raise AcquisitionError("production transport permits exactly one GET")
        self._used = True
        if endpoint != CANONICAL_ENDPOINT:
            raise AcquisitionError("production transport endpoint mismatch")
        headers = {
            "Accept": "application/zip, application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": USER_AGENT,
        }
        request = Request(endpoint, method="GET", headers=headers)
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        try:
            response = opener.open(request, timeout=TIMEOUT_SECONDS)
        except HTTPError as exc:
            raise AcquisitionError(f"archive GET returned HTTP {exc.code}") from exc
        except (URLError, OSError) as exc:
            raise AcquisitionError(f"archive GET failed: {exc}") from exc
        get_all = getattr(response.headers, "get_all", None)
        collected: dict[str, str] = {}
        for key in EVIDENCE_HEADERS:
            values = get_all(key, []) if get_all else (
                [response.headers[key]] if response.headers.get(key) else []
            )
            if values:
                collected[key] = ", ".join(str(v).strip() for v in values)
        return TransportResponse(
            status=response.status,
            final_url=response.geturl(),
            headers=collected,
            stream=response,
            close=response.close,
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _bounded_headers(headers: Mapping[str, str]) -> tuple[dict[str, dict[str, Any]], str | None]:
    """Return the allowlisted response-header evidence and the raw
    Content-Length string (possibly comma-joined for repeated occurrences)."""
    evidence: dict[str, dict[str, Any]] = {}
    total = 0
    for key in EVIDENCE_HEADERS:
        raw = headers.get(key)
        present = raw is not None
        if present:
            encoded = str(raw).encode("utf-8")
            total += len(key) + len(encoded)
            if len(encoded) > 1024 or total > 8192:
                raise AcquisitionError("response header evidence exceeds bounded policy")
        evidence[key] = {"present": present, "value": raw}
    return evidence, headers.get("content-length")


def _validate_response(response: TransportResponse) -> tuple[dict[str, Any], str | None]:
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
    parse_content_length(length_header, MAXIMUM_BYTES)
    return {"headers": evidence, "content_type": media_type}, length_header


@contextlib.contextmanager
def _release_lock(path: Path):
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    handle = os.fdopen(descriptor, "a+b")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LockContentionError("NorTaxa 1.284 acquisition lock is already held") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_real_directory(path: Path) -> None:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise AcquisitionError(f"path must be a real directory: {path}")


def _atomic_update_manifest(path: Path, updater: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    updated = updater(manifest)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(tmp, flags, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(updated, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_directory(path.parent)


def _append_attempt(manifest: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    attempts = list(manifest.get("execution_attempts") or [])
    numbers = [a.get("attempt_number", 0) for a in attempts if isinstance(a, dict)]
    entry.setdefault("attempt_number", (max(numbers) if numbers else 0) + 1)
    attempts.append(entry)
    manifest["execution_attempts"] = attempts
    return manifest


def _record_success(
    manifest: dict[str, Any],
    *,
    started: str,
    completed: str,
    observed: int,
    archive_sha: str,
    response_evidence: dict[str, Any],
    stream_policy: dict[str, Any],
    validation_summary: dict[str, Any],
) -> dict[str, Any]:
    manifest["state"] = "validated"
    manifest["download"] = {
        "endpoint": CANONICAL_ENDPOINT,
        "retrieved_at": completed,
        "observed_bytes": observed,
        "archive_sha256": archive_sha,
        "stream_policy": stream_policy,
        "response_headers": response_evidence["headers"],
        "content_type": response_evidence["content_type"],
    }
    manifest["validation"] = validation_summary
    return _append_attempt(manifest, {
        "started_at": started,
        "completed_at": completed,
        "endpoint": CANONICAL_ENDPOINT,
        "outcome": "succeeded",
        "phase": "validated_and_promoted",
        "observed_bytes": observed,
        "archive_sha256": archive_sha,
        "validation_summary": validation_summary,
    })


def _record_failure(
    manifest: dict[str, Any],
    *,
    started: str,
    completed: str,
    phase: str,
    error: BaseException,
    observed: int | None = None,
    archive_sha: str | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "started_at": started,
        "completed_at": completed,
        "endpoint": CANONICAL_ENDPOINT,
        "outcome": "failed",
        "phase": phase,
        "error": {"type": type(error).__name__, "message": str(error)[:512]},
    }
    if observed is not None:
        entry["observed_bytes"] = observed
    if archive_sha is not None:
        entry["archive_sha256"] = archive_sha
    return _append_attempt(manifest, entry)


def _clean_temp(temp_file: Path, staging: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        temp_file.unlink()
    if staging.exists():
        # Only remove staging if it is now empty.
        with contextlib.suppress(OSError):
            staging.rmdir()


def _refuse_stale_staging(staging: Path) -> None:
    """If staging exists and contains anything, refuse. Do not attempt recovery."""
    if not staging.exists():
        return
    _require_real_directory(staging)
    contents = sorted(entry.name for entry in staging.iterdir())
    if contents:
        raise AcquisitionError(
            f"staging directory contains pre-existing payload; manual review "
            f"required at {staging} (entries: {contents})"
        )


def acquire(
    paths: ReleasePaths,
    *,
    transport: Transport | None = None,
    clock: Callable[[], datetime] = _utc_now,
    free_space: Callable[[Path], int] = lambda p: shutil.disk_usage(p).free,
) -> dict[str, Any]:
    """Execute exactly one acquisition attempt. Manual invocation authorizes it.

    Returns the terminal manifest snapshot. Raises ``AcquisitionError`` on any
    handled failure (with the failure appended to manifest execution_attempts).
    """
    _require_real_directory(paths.release)
    request = load_request(paths.request, paths.source_proposal)
    if paths.archive.exists() or paths.archive.is_symlink():
        raise AcquisitionError(f"final archive already exists: {paths.archive}")

    started = _iso(clock())

    with _release_lock(paths.lock):
        # Refuse any pre-existing staging payload (crash residue).
        _refuse_stale_staging(paths.staging)
        paths.staging.mkdir(parents=True, exist_ok=True)
        _require_real_directory(paths.staging)
        if os.stat(paths.staging).st_dev != os.stat(paths.release).st_dev:
            raise AcquisitionError("staging and final archive are on different filesystems")

        available = free_space(paths.staging)
        if available < MAXIMUM_BYTES:
            raise AcquisitionError(
                f"insufficient free space: {available} < ceiling {MAXIMUM_BYTES}"
            )

        temp_file = paths.staging / f"nortaxa-{VERSION}-{uuid.uuid4().hex}.part"
        response: TransportResponse | None = None
        received = 0
        archive_sha: str | None = None
        response_evidence: dict[str, Any] | None = None
        length_header: str | None = None
        chosen_transport = transport or ProductionHTTPTransport()

        # Streaming phase.
        try:
            with temp_file.open("xb") as output:
                response = chosen_transport.open(CANONICAL_ENDPOINT)
                response_evidence, length_header = _validate_response(response)
                digest = hashlib.sha256()
                reached_eof = False
                while True:
                    remaining = MAXIMUM_BYTES - received
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
                    ceiling=MAXIMUM_BYTES,
                    content_length_header=length_header,
                    completed_bytes=received,
                    reached_eof=reached_eof,
                )
                output.flush()
                os.fsync(output.fileno())
                archive_sha = digest.hexdigest()
        except BaseException as exc:
            completed = _iso(clock())
            _atomic_update_manifest(paths.manifest, lambda m: _record_failure(
                m, started=started, completed=completed, phase="streaming",
                error=exc,
                observed=received if received else None,
                archive_sha=None,
            ))
            _clean_temp(temp_file, paths.staging)
            raise
        finally:
            if response is not None:
                with contextlib.suppress(BaseException):
                    response.close()

        # Structural validation phase (non-extracting).
        try:
            report = validate_fixture(temp_file, request)
        except BaseException as exc:
            completed = _iso(clock())
            _atomic_update_manifest(paths.manifest, lambda m: _record_failure(
                m, started=started, completed=completed, phase="structural_validation",
                error=exc, observed=received, archive_sha=archive_sha,
            ))
            _clean_temp(temp_file, paths.staging)
            raise
        if report.get("result") != "passed":
            error = AcquisitionError("structural validator did not return its passed result")
            completed = _iso(clock())
            _atomic_update_manifest(paths.manifest, lambda m: _record_failure(
                m, started=started, completed=completed, phase="structural_validation",
                error=error, observed=received, archive_sha=archive_sha,
            ))
            _clean_temp(temp_file, paths.staging)
            raise error
        validation_summary = {
            k: v for k, v in report.items()
            if k in {"profile_code", "record_counts", "identifier_contract",
                     "meta_xml", "linkage", "archive", "request_definition_sha256",
                     "network_calls", "taxon_column_gaps", "reference_gaps",
                     "compiler_ready", "hierarchy_complete"}
        }

        # Exclusive promotion (non-overwriting) then release-dir fsync.
        try:
            os.link(temp_file, paths.archive, follow_symlinks=False)
        except FileExistsError as exc:
            error = AcquisitionError("final archive appeared before promotion")
            completed = _iso(clock())
            _atomic_update_manifest(paths.manifest, lambda m: _record_failure(
                m, started=started, completed=completed, phase="promotion",
                error=error, observed=received, archive_sha=archive_sha,
            ))
            _clean_temp(temp_file, paths.staging)
            raise error from exc
        temp_file.unlink()
        _fsync_directory(paths.release)
        with contextlib.suppress(OSError):
            paths.staging.rmdir()

        completed = _iso(clock())
        _atomic_update_manifest(paths.manifest, lambda m: _record_success(
            m, started=started, completed=completed,
            observed=received, archive_sha=archive_sha,
            response_evidence=response_evidence,
            stream_policy=stream_policy,
            validation_summary=validation_summary,
        ))
        return json.loads(paths.manifest.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute", action="store_true",
        help="Explicitly authorize one live GET to the pinned NorTaxa endpoint.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if not args.execute:
        print(
            "Refusing to run without --execute. Manual invocation with --execute is\n"
            "the explicit acquisition authorization; each invocation opens at most\n"
            "one HTTP GET and there is no automatic retry.",
            file=sys.stderr,
        )
        return 2
    taxonomy = Path(__file__).resolve().parents[1]
    paths = ReleasePaths(taxonomy_root=taxonomy)
    try:
        result = acquire(paths)
    except AcquisitionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({
        "state": result.get("state"),
        "download": result.get("download"),
        "last_attempt": (result.get("execution_attempts") or [{}])[-1],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
