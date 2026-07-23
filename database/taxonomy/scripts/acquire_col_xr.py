#!/usr/bin/env python3
"""One-shot approved public COL XR acquisition and non-extracting validation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
import time
import unicodedata
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from col_xr_delivery import (
    ACCEPTED_ARCHIVE_CONTENT_TYPES,
    PUBLIC_ARCHIVE_ENDPOINT,
    HeadResponse,
    load_json,
    normalize_head_metadata,
    preflight_download,
    proposal_request_identity,
    validate_approved_artifact,
    validate_attempt3_authorization,
    validate_retry_authorization,
)
from col_xr_yaml import (
    COL_XR_METADATA_BYTES,
    DEFAULT_METADATA_BYTES,
    YAML_POLICY_VERSION,
    YamlLimits,
    validate_yaml_events,
)
from coldp_schema import CHECKLISTBANK_COL_XR_PROFILE, resolve_entity_header
from coldp_table import (
    bounded_quote_context,
    former_strict_csv_failure,
    parse_literal_tsv_record,
)
from col_xr_corrections import (
    ARCHIVE_SHA256,
    BOM,
    MEMBER_SHA256,
    apply_exact_source_correction,
)
from refresh_col_xr import AcquisitionError, sha256_file, sha256_json


CHUNK_BYTES = 8 * 1024 * 1024
READ_TIMEOUT_SECONDS = 180
ZIP_MEMBER_POLICY_VERSION = 2
MEMBER_WARNING_THRESHOLD = 20_000
MAX_MEMBERS = 250_000
MAX_CENTRAL_DIRECTORY_NAME_BYTES = 16 * 1024 * 1024
MAX_METADATA_BYTES = DEFAULT_METADATA_BYTES
MAX_TOTAL_UNCOMPRESSED_BYTES = 80 * 1024 * 1024 * 1024
MAX_INDIVIDUAL_UNCOMPRESSED_BYTES = 40 * 1024 * 1024 * 1024
MAX_HARD_COMPRESSION_RATIO = 1_000
RATIO_WARNING = 200
MAX_TABLE_LINE_BYTES = 16 * 1024 * 1024
MAX_TABLE_FIELD_BYTES = 4 * 1024 * 1024
TABLE_READ_BYTES = 1024 * 1024
USER_AGENT = "Sporely-Taxonomy-Acquisition/1 (approved COL XR 2026-07-17)"
APPROVED_HOSTS = {"api.checklistbank.org", "download.checklistbank.org"}
PINNED_COL_XR_IDENTITY = {
    "proposal_sha256": "1f0bc14ca41a0870a99eb581edea7227af449a5dba920edc253778dc0bd2c632",
    "request_sha256": "d700581e3ac1df895fb4639daa1b7aa842660df3257794ed924abce3f0c92c0a",
    "approved_canonical_endpoint": PUBLIC_ARCHIVE_ENDPOINT,
    "declared_content_length": 1_383_646_570,
}
PINNED_COL_XR_RELEASE = {
    "archive_format": "ColDP",
    "dataset_key": 315834,
    "doi": "10.48580/dgykv",
    "issued_date": "2026-07-17",
    "release_label": "2026-07-17 XR",
    "release_type": "Extended Release",
}
PINNED_METADATA_SHA256 = "ae02692eaf1364d2928736435caac655271d28ea50177d57c197d0fd9e137771"
SOURCE_SAMPLE_COUNT = 16


class TransferFailure(AcquisitionError):
    def __init__(self, message: str, evidence: dict[str, Any]):
        super().__init__(message)
        self.evidence = evidence


class StructuralPolicyError(AcquisitionError):
    def __init__(self, message: str, *, policy_rule: str):
        super().__init__(message)
        self.policy_rule = policy_rule
        self.classification = "unresolved_policy_limit"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _absolute_without_resolution(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _require_beneath(path: Path, root: Path, label: str) -> None:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise AcquisitionError(f"{label} escapes its approved root") from exc


def _reject_existing_symlinks_and_non_dirs(path: Path, repository_root: Path) -> None:
    _require_beneath(path, repository_root, "proposed path")
    current = repository_root
    for part in path.relative_to(repository_root).parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode):
            raise AcquisitionError(f"existing path component is a symlink: {current}")
        if current != path and not stat.S_ISDIR(info.st_mode):
            raise AcquisitionError(f"file exists where a directory is required: {current}")


def nearest_existing_ancestor(path: Path, repository_root: Path) -> Path:
    candidate = _absolute_without_resolution(path)
    root = _absolute_without_resolution(repository_root)
    _reject_existing_symlinks_and_non_dirs(candidate, root)
    current = candidate
    while True:
        try:
            info = current.lstat()
        except FileNotFoundError:
            if current == root:
                raise AcquisitionError("repository root does not exist")
            current = current.parent
            continue
        if stat.S_ISLNK(info.st_mode):
            raise AcquisitionError(f"nearest existing ancestor is a symlink: {current}")
        if not stat.S_ISDIR(info.st_mode):
            current = current.parent
            continue
        return current


def plan_filesystem_layout(
    *,
    repository_root: Path,
    source_root: Path,
    staged_archive: Path,
    final_archive: Path,
) -> dict[str, Any]:
    repo = _absolute_without_resolution(repository_root)
    source = _absolute_without_resolution(source_root)
    staged = _absolute_without_resolution(staged_archive)
    final = _absolute_without_resolution(final_archive)
    _require_beneath(source, repo, "source root")
    _require_beneath(staged, source, "staged archive")
    _require_beneath(final, source, "final archive")
    staged_ancestor = nearest_existing_ancestor(staged.parent, repo)
    final_ancestor = nearest_existing_ancestor(final.parent, repo)
    staged_device = os.stat(staged_ancestor).st_dev
    final_device = os.stat(final_ancestor).st_dev
    if staged_device != final_device:
        raise AcquisitionError("proposed staging and final ancestors are on different filesystems")
    return {
        "repository_root": repo,
        "source_root": source,
        "staged_archive": staged,
        "final_archive": final,
        "staged_nearest_existing_ancestor": staged_ancestor,
        "final_nearest_existing_ancestor": final_ancestor,
        "planned_device": staged_device,
    }


def _create_directory_chain(path: Path, repository_root: Path) -> list[Path]:
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    _reject_existing_symlinks_and_non_dirs(path, repository_root)
    created: list[Path] = []
    for directory in reversed(missing):
        directory.mkdir()
        created.append(directory)
        info = directory.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise AcquisitionError(f"created directory became unsafe: {directory}")
    return created


def cleanup_created_empty_directories(created: list[Path]) -> None:
    for directory in reversed(created):
        try:
            directory.rmdir()
        except (FileNotFoundError, OSError):
            pass


def create_and_revalidate_layout(plan: dict[str, Any]) -> dict[str, Any]:
    repo = plan["repository_root"]
    staged_parent = plan["staged_archive"].parent
    final_parent = plan["final_archive"].parent
    created: list[Path] = []
    try:
        created.extend(_create_directory_chain(final_parent, repo))
        created.extend(_create_directory_chain(staged_parent, repo))
        _reject_existing_symlinks_and_non_dirs(staged_parent, repo)
        _reject_existing_symlinks_and_non_dirs(final_parent, repo)
        staged_device = os.stat(staged_parent).st_dev
        final_device = os.stat(final_parent).st_dev
        if staged_device != final_device or staged_device != plan["planned_device"]:
            raise AcquisitionError("created staging and final directories are not on the planned filesystem")
        return {
            "created_directories": created,
            "staging_directory": staged_parent,
            "release_directory": final_parent,
            "device": staged_device,
            "atomic_replace_layout": True,
        }
    except BaseException:
        cleanup_created_empty_directories(created)
        raise


class ApprovedRedirects(HTTPRedirectHandler):
    def __init__(self, maximum: int):
        self.maximum = maximum
        self.urls: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urlparse(newurl)
        if parsed.scheme != "https" or parsed.hostname not in APPROVED_HOSTS:
            raise AcquisitionError("network redirect used an unapproved host")
        if newurl in [req.full_url, *self.urls]:
            raise AcquisitionError("network redirect loop detected")
        self.urls.append(newurl)
        if len(self.urls) > self.maximum:
            raise AcquisitionError("network redirect limit exceeded")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if req.method == "HEAD" and redirected is not None:
            redirected.method = "HEAD"
        return redirected


def _opener(max_redirects: int) -> tuple[Any, ApprovedRedirects]:
    redirects = ApprovedRedirects(max_redirects)
    return build_opener(redirects), redirects


def fresh_head(endpoint: str, max_redirects: int) -> dict[str, Any]:
    opener, redirects = _opener(max_redirects)
    request = Request(endpoint, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with opener.open(request, timeout=READ_TIMEOUT_SECONDS) as response:
            get_all = getattr(response.headers, "get_all", lambda key: [])
            if get_all("Set-Cookie"):
                raise AcquisitionError("HEAD response unexpectedly attempted to set cookies")
            evidence = HeadResponse(
                status=response.status,
                requested_url=endpoint,
                redirect_urls=tuple(redirects.urls),
                final_url=response.geturl(),
                headers=dict(response.headers.items()),
            )
    except (HTTPError, URLError, OSError) as exc:
        raise AcquisitionError(f"HEAD preflight failed: {exc}") from exc
    return normalize_head_metadata(evidence, max_redirects=max_redirects)


def stream_once(
    endpoint: str,
    destination: Path,
    *,
    max_redirects: int,
    maximum_bytes: int,
    declared_content_length: int,
    expected_staging_root: Path,
    expected_device: int,
    expected_etag: str | None = None,
    expected_last_modified: str | None = None,
    opener_builder=_opener,
) -> dict[str, Any]:
    digest = hashlib.sha256()
    received = 0
    next_progress = 5
    started = utc_now()
    evidence = {
        "transport_open_attempted": False,
        "response_opened": False,
        "bytes_written": 0,
        "expected_bytes": declared_content_length,
        "partial_file_removed": False,
        "cleanup_error": None,
        "failure_phase": None,
        "failure_type": None,
        "failure_message": None,
        "started_at": started,
        "failed_at": None,
    }
    parent = destination.parent
    staging_root = _absolute_without_resolution(expected_staging_root)
    destination_absolute = _absolute_without_resolution(destination)
    try:
        info = parent.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise AcquisitionError("staging parent must be a real existing directory")
        _require_beneath(destination_absolute, staging_root, "staging destination")
        if parent.resolve(strict=True) != staging_root.resolve(strict=True):
            raise AcquisitionError("staging parent resolved outside its validated location")
        if os.stat(parent).st_dev != expected_device:
            raise AcquisitionError("staging parent device changed after validation")
        if destination.exists() or destination.is_symlink():
            raise AcquisitionError("staging destination already exists")
        handle = destination.open("xb")
    except BaseException as exc:
        evidence.update({
            "failure_phase": "local_destination_preflight",
            "failure_type": type(exc).__name__,
            "failure_message": str(exc),
            "failed_at": utc_now(),
        })
        raise TransferFailure(str(exc), evidence) from exc
    try:
        opener, redirects = opener_builder(max_redirects)
        request = Request(
            endpoint,
            method="GET",
            headers={"Accept": "application/zip, application/octet-stream", "User-Agent": USER_AGENT},
        )
        evidence["transport_open_attempted"] = True
        with opener.open(request, timeout=READ_TIMEOUT_SECONDS) as response:
            evidence["response_opened"] = True
            final = urlparse(response.geturl())
            if final.scheme != "https" or final.hostname not in APPROVED_HOSTS:
                raise AcquisitionError("GET final host is not approved")
            media_type = response.headers.get("Content-Type", "").split(";", 1)[0].casefold()
            if media_type not in ACCEPTED_ARCHIVE_CONTENT_TYPES:
                raise AcquisitionError(f"GET returned unsupported Content-Type: {media_type!r}")
            response_length = response.headers.get("Content-Length")
            if response_length is None or int(response_length) != declared_content_length:
                raise AcquisitionError("GET Content-Length differs from explicit approval")
            if expected_etag is not None and response.headers.get("ETag") != expected_etag:
                raise AcquisitionError("GET ETag differs from verified HEAD")
            if (
                expected_last_modified is not None
                and response.headers.get("Last-Modified") != expected_last_modified
            ):
                raise AcquisitionError("GET Last-Modified differs from verified HEAD")
            get_all = getattr(response.headers, "get_all", lambda key: [])
            if get_all("Set-Cookie"):
                raise AcquisitionError("GET response unexpectedly attempted to set cookies")
            with handle:
                while True:
                    chunk = response.read(CHUNK_BYTES)
                    if not chunk:
                        break
                    received += len(chunk)
                    if received > maximum_bytes:
                        raise AcquisitionError("received bytes exceed approved maximum")
                    if received > declared_content_length:
                        raise AcquisitionError("received bytes exceed declared Content-Length")
                    handle.write(chunk)
                    evidence["bytes_written"] = received
                    digest.update(chunk)
                    percent = received * 100 // declared_content_length
                    if percent >= next_progress:
                        print(f"download progress: {received}/{declared_content_length} bytes ({percent}%)", flush=True)
                        next_progress = (percent // 5 + 1) * 5
                handle.flush()
                os.fsync(handle.fileno())
            if received != declared_content_length:
                raise AcquisitionError("received bytes differ from declared Content-Length")
            return {
                "started_at": started,
                "completed_at": utc_now(),
                "canonical_endpoint": endpoint,
                "redirect_chain": redirects.urls,
                "resolved_source_url": response.geturl(),
                "final_host": final.hostname,
                "content_type": media_type,
                "content_length": int(response_length),
                "bytes": received,
                "sha256": digest.hexdigest(),
                "transfer_attempts": 1,
                "user_agent": USER_AGENT,
                "read_timeout_seconds": READ_TIMEOUT_SECONDS,
                "attempt_evidence": evidence,
            }
    except BaseException as exc:
        try:
            if not handle.closed:
                handle.close()
            if destination.exists() or destination.is_symlink():
                destination.unlink()
            evidence["partial_file_removed"] = not destination.exists()
        except BaseException as cleanup_exc:
            evidence["cleanup_error"] = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
        evidence.update({
            "failure_phase": (
                "response_streaming" if evidence["response_opened"]
                else "response_open"
            ),
            "failure_type": type(exc).__name__,
            "failure_message": str(exc),
            "failed_at": utc_now(),
        })
        raise TransferFailure(str(exc), evidence) from exc


def _safe_members(archive: zipfile.ZipFile) -> tuple[list[zipfile.ZipInfo], list[str]]:
    members = archive.infolist()
    if not members:
        raise AcquisitionError("ZIP member count is empty")
    if len(members) > MAX_MEMBERS:
        raise StructuralPolicyError(
            "ZIP member count exceeds the configured safety ceiling",
            policy_rule="max_members",
        )
    seen: dict[str, str] = {}
    offsets: set[int] = set()
    warnings: list[str] = []
    if len(members) > MEMBER_WARNING_THRESHOLD:
        warnings.append(
            f"ZIP member count {len(members)} exceeds warning threshold "
            f"{MEMBER_WARNING_THRESHOLD} under policy v{ZIP_MEMBER_POLICY_VERSION}"
        )
    total_names = 0
    total_uncompressed = 0
    for member in members:
        name = member.filename
        total_names += len(name.encode("utf-8"))
        path = PurePosixPath(name)
        folded = unicodedata.normalize("NFC", name.replace("\\", "/")).casefold()
        if (
            not name
            or path.is_absolute()
            or ".." in path.parts
            or "\\" in name
            or re.match(r"^[A-Za-z]:", name)
        ):
            raise AcquisitionError(f"unsafe ZIP member: {name!r}")
        if folded in seen:
            raise AcquisitionError(
                f"duplicate or normalized-path collision: {seen[folded]!r}, {name!r}"
            )
        seen[folded] = name
        if member.header_offset in offsets:
            raise AcquisitionError(f"overlapping central records share a local offset: {name!r}")
        offsets.add(member.header_offset)
        if member.flag_bits & 0x1:
            raise AcquisitionError(f"encrypted ZIP member is forbidden: {name!r}")
        if member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
            raise AcquisitionError(
                f"unsupported ZIP compression method {member.compress_type}: {name!r}"
            )
        mode = member.external_attr >> 16
        kind = stat.S_IFMT(mode)
        if kind not in {0, stat.S_IFREG, stat.S_IFDIR} or stat.S_ISLNK(mode):
            raise AcquisitionError(f"ZIP special file is forbidden: {name!r}")
        total_uncompressed += member.file_size
        if member.file_size > MAX_INDIVIDUAL_UNCOMPRESSED_BYTES:
            raise AcquisitionError(f"ZIP member exceeds uncompressed safety ceiling: {name!r}")
        ratio = member.file_size / max(1, member.compress_size)
        if ratio > MAX_HARD_COMPRESSION_RATIO:
            raise AcquisitionError(f"ZIP member compression ratio exceeds safety ceiling: {name!r}")
        if ratio > RATIO_WARNING:
            warnings.append(f"high compression ratio {ratio:.1f}: {name}")
    if total_names > MAX_CENTRAL_DIRECTORY_NAME_BYTES:
        raise AcquisitionError("ZIP central-directory names exceed the safety ceiling")
    if total_uncompressed > MAX_TOTAL_UNCOMPRESSED_BYTES:
        raise AcquisitionError("ZIP declared uncompressed total exceeds the safety ceiling")
    return members, warnings


def _metadata_scalars(raw: bytes) -> dict[str, str]:
    if len(raw) > MAX_METADATA_BYTES:
        raise AcquisitionError("metadata.yaml exceeds the safety ceiling")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise AcquisitionError("metadata.yaml is not UTF-8") from exc
    values: dict[str, str] = {}
    stack: list[tuple[int, str]] = []
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, value = line.strip().split(":", 1)
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = ".".join([item[1] for item in stack] + [key.strip()])
        scalar = value.strip().strip("'\"")
        if scalar:
            values[path] = scalar
        else:
            stack.append((indent, key.strip()))
    return values


def _find_nameusage(members: list[zipfile.ZipInfo]) -> zipfile.ZipInfo:
    candidates = [
        member for member in members
        if PurePosixPath(member.filename).name.casefold()
        in {"nameusage.tsv", "nameusage.csv", "nameusage.txt", "taxon.tsv", "taxon.csv", "taxon.txt"}
    ]
    if len(candidates) != 1:
        raise AcquisitionError(f"expected one NameUsage core table, observed {len(candidates)}")
    return candidates[0]


def _bounded_binary_lines(handle, state: dict[str, Any]):
    pending = bytearray()
    while True:
        chunk = handle.read(TABLE_READ_BYTES)
        if not chunk:
            break
        state["bytes"] += len(chunk)
        state["sha256"].update(chunk)
        pending.extend(chunk)
        while True:
            newline = pending.find(b"\n")
            if newline < 0:
                break
            line = bytes(pending[: newline + 1])
            del pending[: newline + 1]
            if len(line) > MAX_TABLE_LINE_BYTES:
                raise AcquisitionError("NameUsage line exceeds the safety ceiling")
            yield line
        if len(pending) > MAX_TABLE_LINE_BYTES:
            raise AcquisitionError("NameUsage line exceeds the safety ceiling")
        if state["bytes"] >= state["next_progress"]:
            print(
                f"NameUsage validation progress: {state['bytes']} bytes",
                flush=True,
            )
            state["next_progress"] += 256 * 1024 * 1024
    if pending:
        yield bytes(pending)
    state["eof"] = True


def _parse_tsv_line(raw: bytes, *, line_number: int, header: bool = False):
    return parse_literal_tsv_record(
        raw,
        line_number=line_number,
        header=header,
        max_line_bytes=MAX_TABLE_LINE_BYTES,
        max_field_bytes=MAX_TABLE_FIELD_BYTES,
    )


def _identifier_shape(value: str) -> str:
    if not value:
        return "empty"
    if value.isdecimal():
        return "decimal"
    if re.fullmatch(r"[A-Z0-9]+", value):
        return "uppercase_alphanumeric"
    if ":" in value:
        return "colon_qualified"
    if re.fullmatch(r"[A-Za-z0-9._-]+", value):
        return "ascii_token"
    return "other"


def _scan_nameusage(
    handle,
    filename: str,
    *,
    correction_policy: dict[str, Any] | None = None,
    archive_sha256: str = "",
    member_sha256: str = "",
) -> dict[str, Any]:
    if not filename.casefold().endswith(".tsv"):
        raise AcquisitionError("pinned ChecklistBank NameUsage must use exact TSV delivery")
    stream = {
        "bytes": 0,
        "sha256": hashlib.sha256(),
        "next_progress": 256 * 1024 * 1024,
        "eof": False,
    }
    lines = _bounded_binary_lines(handle, stream)
    try:
        header_record = _parse_tsv_line(next(lines), line_number=1, header=True)
        original_header = list(header_record.raw_fields)
    except StopIteration as exc:
        raise AcquisitionError("NameUsage table is empty") from exc
    resolved = resolve_entity_header(
        "NameUsage",
        original_header,
        source_profile=CHECKLISTBANK_COL_XR_PROFILE,
    )
    header = list(resolved.normalized_tokens)
    indexes = {value: index for index, value in enumerate(header)}
    columns = {
        "id": indexes["ID"],
        "parent_id": indexes["parentID"],
        "scientific_name": indexes["scientificName"],
        "authorship": indexes["authorship"],
        "rank": indexes["rank"],
        "status": indexes["status"],
    }
    audit = {
        "code": "code" in indexes,
        "extinct": "extinct" in indexes,
        "origin": "origin" in indexes,
        "reference_id": "referenceID" in indexes,
        "sector_id": "sectorID" in indexes,
        "source_id": "sourceID" in indexes,
    }
    rows = blank_rows = malformed_rows = duplicate_ids = self_parents = 0
    former_csv_failures = 0
    correction_evidence: list[dict[str, Any]] = []
    unapproved_bom_occurrences = 0
    accepted = synonyms = fungi_rows = 0
    ranks: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    identifier_shapes: Counter[str] = Counter()
    missing = Counter({name: 0 for name in columns})
    malformed_samples: list[dict[str, Any]] = []
    former_csv_samples: list[dict[str, Any]] = []
    probes: dict[str, dict[str, str]] = {}
    representative_nonempty = 0
    with tempfile.TemporaryDirectory(prefix="sporely-col-xr-validation-") as temporary:
        database = sqlite3.connect(Path(temporary) / "ids.sqlite3")
        database.execute("PRAGMA journal_mode=OFF")
        database.execute("PRAGMA synchronous=OFF")
        database.execute("PRAGMA temp_store=FILE")
        database.execute("CREATE TABLE ids (id TEXT PRIMARY KEY) WITHOUT ROWID")
        for line_number, raw_line in enumerate(lines, start=2):
            if not raw_line.strip(b"\r\n"):
                blank_rows += 1
                continue
            try:
                if BOM in raw_line:
                    if correction_policy is None:
                        raise AcquisitionError("unapproved BOM occurrence in NameUsage")
                    applied = apply_exact_source_correction(
                        raw_line,
                        line_number=line_number,
                        normalized_header=tuple(header),
                        archive_sha256=archive_sha256,
                        member_sha256=member_sha256,
                        source_profile=CHECKLISTBANK_COL_XR_PROFILE,
                        release_label="2026-07-17 XR",
                        dataset_key=315834,
                        policy=correction_policy,
                        max_line_bytes=MAX_TABLE_LINE_BYTES,
                        max_field_bytes=MAX_TABLE_FIELD_BYTES,
                    )
                    record = applied.record
                    correction_evidence.append(applied.evidence)
                else:
                    record = _parse_tsv_line(raw_line, line_number=line_number)
            except AcquisitionError as exc:
                if BOM in raw_line:
                    unapproved_bom_occurrences += raw_line.count(BOM)
                malformed_rows += 1
                if len(malformed_samples) < 20:
                    malformed_samples.append({"line": line_number, "reason": str(exc)})
                continue
            row = list(record.raw_fields)
            semantic_row = list(record.semantic_fields)
            csv_failure = former_strict_csv_failure(raw_line)
            if csv_failure is not None:
                former_csv_failures += 1
                if len(former_csv_samples) < 20:
                    former_csv_samples.append({
                        "line": line_number,
                        "reason": csv_failure,
                        "raw_row_sha256": hashlib.sha256(raw_line).hexdigest(),
                        "row_bytes": len(raw_line),
                        "context": bounded_quote_context(raw_line),
                        "literal_tsv_column_count": len(row),
                    })
            if len(row) != len(header):
                malformed_rows += 1
                if len(malformed_samples) < 20:
                    malformed_samples.append({
                        "line": line_number,
                        "reason": f"column count {len(row)} != {len(header)}",
                    })
                continue
            rows += 1
            values = {key: row[index] for key, index in columns.items()}
            semantic_values = {
                key: semantic_row[index] for key, index in columns.items()
            }
            for key, value in values.items():
                if not value:
                    missing[key] += 1
            identifier = values["id"]
            identifier_shapes[_identifier_shape(identifier)] += 1
            if identifier:
                cursor = database.execute("INSERT OR IGNORE INTO ids(id) VALUES (?)", (identifier,))
                if cursor.rowcount == 0:
                    duplicate_ids += 1
            if identifier and identifier == values["parent_id"]:
                self_parents += 1
            status_value = values["status"].casefold()
            rank_value = values["rank"].casefold()
            statuses[status_value or "<empty>"] += 1
            ranks[rank_value or "<empty>"] += 1
            if len(statuses) > 10_000 or len(ranks) > 10_000:
                raise AcquisitionError("NameUsage status or rank cardinality exceeds safety limit")
            if status_value in {"accepted", "provisionally accepted", "provisionally_accepted"}:
                accepted += 1
            if "synonym" in status_value or status_value in {"misapplied", "ambiguous synonym"}:
                synonyms += 1
            if (
                representative_nonempty < 100
                and values["id"]
                and values["scientific_name"]
                and values["rank"]
                and values["status"]
            ):
                representative_nonempty += 1
            if values["id"] == "F":
                fungi_rows += 1
                probes["fungi_root"] = {
                    "raw": {**values, "accepted_id": values["parent_id"]},
                    "semantic": {
                        **semantic_values,
                        "accepted_id": semantic_values["parent_id"],
                    },
                }
            if values["scientific_name"] in {
                "Candolleomyces candolleanus", "Psathyrella candolleana"
            }:
                probes[values["scientific_name"]] = {
                    "raw": {**values, "accepted_id": values["parent_id"]},
                    "semantic": {
                        **semantic_values,
                        "accepted_id": semantic_values["parent_id"],
                    },
                }
            if rows % 100_000 == 0:
                database.commit()
        database.commit()
    return {
        "scan_schema_version": 1,
        "completion_status": "complete" if stream["eof"] else "partial",
        "filename": filename,
        "delimiter": "literal physical tab",
        "quotes_structural": False,
        "encoding": "strict UTF-8",
        "original_header": list(resolved.original_tokens),
        "normalized_header": header,
        "original_to_normalized": [list(item) for item in resolved.original_to_normalized],
        "unknown_columns": list(resolved.unknown_columns),
        "header_profile": resolved.source_profile,
        "required_column_mapping": columns,
        "audit_fields_present": audit,
        "row_count": rows,
        "uncompressed_bytes_read": stream["bytes"],
        "sha256": stream["sha256"].hexdigest(),
        "final_eof_reached": stream["eof"],
        "crc_verified_by_zip_stream": stream["eof"],
        "blank_row_count": blank_rows,
        "malformed_row_count": malformed_rows,
        "malformed_row_samples": malformed_samples,
        "former_strict_csv_rejection_count": former_csv_failures,
        "former_strict_csv_rejection_samples": former_csv_samples,
        "applied_correction_count": len(correction_evidence),
        "removed_semantic_bom_code_points": sum(
            item["removed_code_point_count"] for item in correction_evidence
        ),
        "unapproved_bom_occurrence_count": unapproved_bom_occurrences,
        "applied_corrections": correction_evidence,
        "duplicate_primary_id_count": duplicate_ids,
        "self_parent_reference_count": self_parents,
        "missing_required_value_counts": dict(missing),
        "identifier_shape_counts": dict(identifier_shapes),
        "accepted_count": accepted,
        "synonym_or_related_count": synonyms,
        "rank_distribution": dict(ranks.most_common()),
        "status_distribution": dict(statuses.most_common()),
        "semantic_probes": probes,
        "fungi_root_rows": fungi_rows,
        "representative_nonempty_rows": representative_nonempty,
        "terminal_validation_findings": [],
    }


def _evaluate_nameusage(report: dict[str, Any]) -> None:
    findings: list[str] = []
    if report["completion_status"] != "complete" or not report["final_eof_reached"]:
        findings.append("NameUsage scan did not reach final EOF")
    if report["malformed_row_count"]:
        findings.append(
            f"NameUsage contains {report['malformed_row_count']} genuine malformed rows"
        )
    if report["duplicate_primary_id_count"]:
        findings.append(
            f"NameUsage contains {report['duplicate_primary_id_count']} duplicate primary IDs"
        )
    if report["sha256"] != MEMBER_SHA256:
        findings.append("NameUsage member SHA-256 differs from the correction fingerprint")
    if report["applied_correction_count"] != 1:
        findings.append(
            f"expected exactly one source correction, observed "
            f"{report['applied_correction_count']}"
        )
    if report["removed_semantic_bom_code_points"] != 2:
        findings.append("expected exactly two removed semantic BOM code points")
    if report["unapproved_bom_occurrence_count"] != 0:
        findings.append(
            f"observed {report['unapproved_bom_occurrence_count']} unapproved BOM occurrences"
        )
    probes = report["semantic_probes"]
    fungi_entry = probes.get("fungi_root")
    fungi = fungi_entry["semantic"] if fungi_entry else None
    if (
        report["fungi_root_rows"] != 1
        or fungi is None
        or fungi["scientific_name"] != "Fungi"
        or fungi["rank"].casefold() != "kingdom"
        or fungi["status"].casefold() != "accepted"
        or fungi["parent_id"] != "CS5HF"
    ):
        findings.append(
            f"approved Fungi root identity mismatch: {fungi!r}, "
            f"rows={report['fungi_root_rows']}"
        )
    current_entry = probes.get("Candolleomyces candolleanus")
    former_entry = probes.get("Psathyrella candolleana")
    current = current_entry["semantic"] if current_entry else None
    former = former_entry["semantic"] if former_entry else None
    if current is None or former is None:
        findings.append("Candolleomyces source probes are incomplete")
    elif current["status"].casefold() != "accepted":
        findings.append(f"Candolleomyces current-name status requires review: {current!r}")
    elif "synonym" not in former["status"].casefold() or former["accepted_id"] != current["id"]:
        findings.append(
            f"Candolleomyces former-name accepted relationship requires review: current={current!r}, former={former!r}"
        )
    if report["representative_nonempty_rows"] < min(100, report["row_count"]):
        findings.append("required identity columns are empty in representative NameUsage rows")
    report["terminal_validation_findings"] = findings
    if findings:
        raise AcquisitionError("; ".join(findings))


def _metadata_limit(approved: dict[str, Any], member: zipfile.ZipInfo) -> tuple[int, bool]:
    if member.file_size <= MAX_METADATA_BYTES:
        return MAX_METADATA_BYTES, False
    identity_matches = all(
        approved.get(key) == value for key, value in PINNED_COL_XR_IDENTITY.items()
    ) and approved.get("release_identity") == PINNED_COL_XR_RELEASE
    if not identity_matches:
        raise AcquisitionError(
            "large metadata override requires the exact pinned COL XR request identity"
        )
    if member.file_size > COL_XR_METADATA_BYTES:
        raise AcquisitionError("metadata.yaml exceeds the pinned COL XR 256 MiB ceiling")
    return COL_XR_METADATA_BYTES, True


def _source_members(members: list[zipfile.ZipInfo]) -> list[zipfile.ZipInfo]:
    return sorted(
        (
            member for member in members
            if re.fullmatch(r"source/[0-9]+\.yaml", member.filename)
        ),
        key=lambda member: member.filename.casefold(),
    )


def _sample_source_members(
    archive: zipfile.ZipFile,
    sources: list[zipfile.ZipInfo],
) -> dict[str, Any]:
    if not sources:
        raise AcquisitionError("COL XR archive lacks source provenance metadata")
    expected_source_paths = [
        member for member in archive.infolist() if member.filename.startswith("source/")
    ]
    if len(expected_source_paths) != len(sources):
        anomalies = sorted(
            member.filename for member in expected_source_paths if member not in sources
        )
        raise AcquisitionError(f"anomalous source metadata paths: {anomalies[:20]}")
    selected: dict[str, tuple[str, zipfile.ZipInfo]] = {}

    def choose(reason: str, member: zipfile.ZipInfo) -> None:
        existing = selected.get(member.filename)
        selected[member.filename] = (
            reason if existing is None else f"{existing[0]},{reason}",
            member,
        )

    choose("first", sources[0])
    choose("last", sources[-1])
    choose("smallest", min(sources, key=lambda member: (member.file_size, member.filename)))
    choose("largest", max(sources, key=lambda member: (member.file_size, member.filename)))
    hash_ranked = sorted(
        sources,
        key=lambda member: hashlib.sha256(member.filename.encode("utf-8")).digest(),
    )
    for member in hash_ranked[:SOURCE_SAMPLE_COUNT]:
        choose("hash-selected", member)
    reports = []
    for filename, (reason, member) in sorted(selected.items()):
        if member.file_size > MAX_METADATA_BYTES:
            raise AcquisitionError(f"source metadata exceeds 5 MiB ceiling: {filename}")
        with archive.open(member) as handle:
            parsed = validate_yaml_events(
                handle,
                limits=YamlLimits(
                    max_bytes=MAX_METADATA_BYTES,
                    max_nodes=500_000,
                    max_mapping_entries=50_000,
                    max_sequence_entries=50_000,
                    max_anchors=100,
                    max_aliases=10,
                    max_seconds=30,
                ),
                expected_bytes=member.file_size,
            )
        reports.append({
            "path": filename,
            "selection_reason": reason,
            "compressed_bytes": member.compress_size,
            "uncompressed_bytes": member.file_size,
            "crc32": f"{member.CRC:08x}",
            "yaml": parsed,
        })
    return {
        "population": len(sources),
        "sample_size": len(reports),
        "method": (
            "first and last canonical path, smallest and largest uncompressed "
            f"member, plus {SOURCE_SAMPLE_COUNT} lowest SHA-256(path) selections"
        ),
        "reports": reports,
    }


def validate_archive(path: Path, approved: dict[str, Any]) -> dict[str, Any]:
    archive_sha256 = sha256_file(path)
    if archive_sha256 != ARCHIVE_SHA256:
        raise AcquisitionError("archive SHA-256 differs from the pinned correction fingerprint")
    release_directory = path.parent.parent if path.parent.name == ".quarantine" else path.parent
    correction_artifact = load_json(release_directory / "source-corrections.json")
    corrections = correction_artifact.get("corrections")
    if not isinstance(corrections, list) or len(corrections) != 1:
        raise AcquisitionError("expected exactly one approved source correction")
    correction_policy = corrections[0]
    with path.open("rb") as raw:
        if raw.read(4) != b"PK\x03\x04":
            raise AcquisitionError("archive lacks ZIP local-file magic")
    try:
        with zipfile.ZipFile(path) as archive:
            members, warnings = _safe_members(archive)
            names = {member.filename.casefold(): member for member in members}
            metadata_member = names.get("metadata.yaml")
            if metadata_member is None:
                raise AcquisitionError("ColDP archive lacks root metadata.yaml")
            metadata_limit, large_metadata = _metadata_limit(approved, metadata_member)
            with archive.open(metadata_member) as handle:
                metadata = validate_yaml_events(
                    handle,
                    limits=YamlLimits(max_bytes=metadata_limit),
                    expected_bytes=metadata_member.file_size,
                    capture_source_ids=True,
                )
            if large_metadata and metadata["sha256"] != PINNED_METADATA_SHA256:
                raise AcquisitionError(
                    "large metadata bytes differ from the inspected pinned COL XR metadata"
                )
            root = metadata["root_scalars"]
            expected_metadata = {
                "key": str(PINNED_COL_XR_RELEASE["dataset_key"]),
                "doi": PINNED_COL_XR_RELEASE["doi"],
                "title": "Catalogue of Life",
                "issued": PINNED_COL_XR_RELEASE["issued_date"],
                "version": PINNED_COL_XR_RELEASE["release_label"],
            }
            mismatches = {
                key: {"expected": value, "observed": root.get(key)}
                for key, value in expected_metadata.items() if root.get(key) != value
            }
            if metadata["root_type"] != "mapping" or mismatches:
                raise AcquisitionError(
                    f"COL XR metadata identity or structure mismatch: {mismatches}"
                )
            sources = _source_members(members)
            source_filenames = {
                PurePosixPath(member.filename).stem for member in sources
            }
            source_identifiers = metadata.pop("source_identifiers")
            if (
                metadata["source_reference_count"] != len(sources)
                or len(source_identifiers) != len(set(source_identifiers))
                or set(source_identifiers) != source_filenames
            ):
                raise AcquisitionError(
                    "root metadata source references do not reconcile with source/ members"
                )
            source_sample = _sample_source_members(archive, sources)
            nameusage = _find_nameusage(members)
            nameusage_report = None
            for member in members:
                if member.is_dir():
                    continue
                with archive.open(member) as handle:
                    if member.filename == nameusage.filename:
                        nameusage_report = _scan_nameusage(
                            handle,
                            member.filename,
                            correction_policy=correction_policy,
                            archive_sha256=archive_sha256,
                            member_sha256=MEMBER_SHA256,
                        )
                        evidence_path = (
                            release_directory / "nameusage-scan.json"
                            if path.parent.name == ".quarantine"
                            else path.parent / "nameusage-scan.json"
                        )
                        write_json_atomic(evidence_path, nameusage_report)
                        try:
                            _evaluate_nameusage(nameusage_report)
                        except AcquisitionError:
                            write_json_atomic(evidence_path, nameusage_report)
                            raise
                        write_json_atomic(evidence_path, nameusage_report)
                    else:
                        while handle.read(CHUNK_BYTES):
                            pass
            if nameusage_report is None:
                raise AcquisitionError("NameUsage integrity scan was not performed")
    except (zipfile.BadZipFile, RuntimeError, UnicodeDecodeError, csv.Error) as exc:
        raise AcquisitionError(f"archive integrity or table parsing failed: {exc}") from exc
    top_level = Counter(
        member.filename.split("/", 1)[0] for member in members
    )
    ignored_names = {
        PurePosixPath(member.filename).stem.casefold(): member.filename
        for member in members
    }
    ignored = approved["compiler_consumption"]["compiler_ignored_entities"]
    return {
        "result": "passed",
        "validated_at": utc_now(),
        "validation_mode": "bounded YAML events plus streaming ZIP CRC and ColDP semantic scan; no extraction",
        "safety_ceilings": {
            "zip_member_policy_version": ZIP_MEMBER_POLICY_VERSION,
            "yaml_policy_version": YAML_POLICY_VERSION,
            "default_metadata_bytes": MAX_METADATA_BYTES,
            "pinned_col_xr_metadata_bytes": COL_XR_METADATA_BYTES,
            "member_warning_threshold": MEMBER_WARNING_THRESHOLD,
            "max_members": MAX_MEMBERS,
            "max_total_uncompressed_bytes": MAX_TOTAL_UNCOMPRESSED_BYTES,
            "max_individual_uncompressed_bytes": MAX_INDIVIDUAL_UNCOMPRESSED_BYTES,
            "max_hard_compression_ratio": MAX_HARD_COMPRESSION_RATIO,
        },
        "archive": {
            "member_count": len(members),
            "compressed_member_total": sum(member.compress_size for member in members),
            "declared_uncompressed_total": sum(member.file_size for member in members),
            "top_level_summary": dict(top_level),
            "warnings": warnings,
            "all_members_crc_streamed": True,
        },
        "coldp": {
            "metadata_file": metadata_member.filename,
            "metadata": metadata,
            "large_metadata_evidence": {
                "override_applied": large_metadata,
                "observed_uncompressed_bytes": metadata_member.file_size,
                "observed_compressed_bytes": metadata_member.compress_size,
                "crc32": f"{metadata_member.CRC:08x}",
                "compression_ratio": metadata_member.file_size / max(1, metadata_member.compress_size),
                "bound_to_proposal_sha256": approved["proposal_sha256"],
                "bound_to_request_sha256": approved["request_sha256"],
            },
            "license_metadata": {"license": root.get("license")},
            "source_reconciliation": {
                "root_reference_count": len(source_identifiers),
                "source_member_count": len(sources),
                "unique_identifiers": len(set(source_identifiers)),
                "filenames_reconciled": True,
                "compiler_role": "delivery provenance; not a compiler-required entity",
            },
            "source_sample_validation": source_sample,
            "nameusage": nameusage_report,
            "compiler_required_members_fully_validated": [nameusage.filename],
            "ignored_entities_present": {
                entity: ignored_names.get(entity.casefold()) for entity in ignored
            },
        },
        "full_extraction_performed": False,
        "taxonomy_import_performed": False,
    }


def quarantine_complete_archive(
    staged: Path,
    release_dir: Path,
    *,
    byte_count: int,
    sha256: str,
    attempt_number: int,
    reason: str,
    validator: str,
    policy_rule: str,
    classification: str,
) -> dict[str, Any]:
    if not staged.is_file() or staged.is_symlink():
        raise AcquisitionError("only a byte-complete real staging file can be quarantined")
    if staged.stat().st_size != byte_count or sha256_file(staged) != sha256:
        raise AcquisitionError("staged bytes do not match completed-transfer evidence")
    quarantine_dir = release_dir / ".quarantine"
    quarantine = quarantine_dir / "archive.zip"
    if quarantine.exists() or quarantine.is_symlink():
        raise AcquisitionError("existing quarantine archive cannot be overwritten")
    if quarantine_dir.exists():
        if quarantine_dir.is_symlink() or not quarantine_dir.is_dir():
            raise AcquisitionError("quarantine path is not a real directory")
        unexpected = [
            path.name for path in quarantine_dir.iterdir()
            if path.name not in {"quarantine.json", "checksums.txt"}
        ]
        if unexpected:
            raise AcquisitionError(f"unrecognized quarantine payload blocks retention: {unexpected}")
    else:
        quarantine_dir.mkdir()
    os.replace(staged, quarantine)
    evidence = {
        "state": "quarantined",
        "byte_count": byte_count,
        "sha256": sha256,
        "original_transfer_attempt": attempt_number,
        "quarantine_path": ".quarantine/archive.zip",
        "quarantine_reason": reason,
        "failed_validator": validator,
        "policy_rule": policy_rule,
        "classification": classification,
        "validation_timestamp": utc_now(),
        "active_archive_exposed": False,
    }
    write_json_atomic(quarantine_dir / "quarantine.json", evidence)
    (quarantine_dir / "checksums.txt").write_text(
        f"{sha256}  archive.zip\n", encoding="utf-8"
    )
    return evidence


def promote_quarantined_archive(
    release_dir: Path,
    approved: dict[str, Any],
    *,
    validator=validate_archive,
) -> dict[str, Any]:
    quarantine = release_dir / ".quarantine/archive.zip"
    metadata = load_json(release_dir / ".quarantine/quarantine.json")
    final = release_dir / "archive.zip"
    if final.exists() or final.is_symlink():
        raise AcquisitionError("active archive already exists")
    if not quarantine.is_file() or quarantine.is_symlink():
        raise AcquisitionError("recognized quarantine archive is missing")
    if quarantine.stat().st_size != metadata.get("byte_count"):
        raise AcquisitionError("quarantine byte count changed")
    if sha256_file(quarantine) != metadata.get("sha256"):
        raise AcquisitionError("quarantine checksum changed")
    validation = validator(quarantine, approved)
    os.replace(quarantine, final)
    validation["promotion_source"] = ".quarantine/archive.zip"
    validation["promoted_archive"] = "archive.zip"
    validation["promoted_at"] = utc_now()
    return validation


def failed_release_retry_status(
    release_dir: Path,
    proposal_path: Path,
    approval_path: Path,
) -> dict[str, Any]:
    blockers: list[str] = []
    expected_files = {
        "request.json",
        "approval.json",
        "manifest.json",
        "retry-authorization-attempt-2.json",
    }
    if not release_dir.is_dir() or release_dir.is_symlink():
        blockers.append("release directory is missing or not a real directory")
        return {"eligible_for_retry_authorization": False, "blocking_reasons": blockers}
    try:
        proposal = load_json(proposal_path)
        original_approval = load_json(approval_path)
        local_request = load_json(release_dir / "request.json")
        local_approval = load_json(release_dir / "approval.json")
        manifest = load_json(release_dir / "manifest.json")
        validate_approved_artifact(proposal, original_approval)
    except AcquisitionError as exc:
        blockers.append(str(exc))
        return {"eligible_for_retry_authorization": False, "blocking_reasons": blockers}
    expected_request = {
        **proposal_request_identity(proposal),
        "proposal_sha256": original_approval["proposal_sha256"],
        "request_sha256": original_approval["request_sha256"],
    }
    if local_request != expected_request:
        blockers.append("release request evidence differs from the approved request")
    if local_approval != original_approval:
        blockers.append("release approval evidence differs from the original approval")
    if manifest.get("state") != "failed":
        blockers.append("manifest state is not failed")
    if manifest.get("proposal_sha256") != original_approval["proposal_sha256"]:
        blockers.append("manifest proposal hash mismatch")
    if manifest.get("request_definition_sha256") != original_approval["request_sha256"]:
        blockers.append("manifest request hash mismatch")
    if manifest.get("release") != original_approval["release_identity"]:
        blockers.append("manifest release identity mismatch")
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 1:
        blockers.append("manifest must preserve exactly one prior attempt")
        attempt_summary = None
    else:
        attempt_summary = attempts[0]
        expected_attempt = {
            "attempt_number": 1,
            "transport_open_attempted": True,
            "response_opened": True,
            "bytes_written": 0,
            "expected_bytes": original_approval["declared_content_length"],
        }
        for key, value in expected_attempt.items():
            if attempt_summary.get(key) != value:
                blockers.append(f"attempt 1 evidence mismatch: {key}")
    unexpected: list[str] = []
    for path in release_dir.rglob("*"):
        relative = path.relative_to(release_dir).as_posix()
        if path.is_symlink():
            unexpected.append(relative)
        elif path.is_file() and relative not in expected_files:
            unexpected.append(relative)
        elif path.is_dir() and relative not in {".staging"}:
            unexpected.append(relative + "/")
    staging = release_dir / ".staging"
    if staging.exists() and (staging.is_symlink() or not staging.is_dir() or any(staging.iterdir())):
        blockers.append("staging directory is not an empty real directory")
    if unexpected:
        blockers.append("unexpected release payload exists")
    for forbidden in ("archive.zip", "extracted"):
        if (release_dir / forbidden).exists() or (release_dir / forbidden).is_symlink():
            blockers.append(f"forbidden failed-release payload exists: {forbidden}")
    return {
        "eligible_for_retry_authorization": not blockers,
        "blocking_reasons": blockers,
        "proposal_sha256": original_approval["proposal_sha256"],
        "request_sha256": original_approval["request_sha256"],
        "original_approval_sha256": hashlib.sha256(
            json.dumps(original_approval, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "prior_attempt_summary": attempt_summary,
        "unexpected_paths": unexpected,
        "required_next_authorization_number": 2,
        "network_calls": 0,
        "state_modified": False,
    }


def require_retry_authorization(
    release_dir: Path,
    proposal_path: Path,
    approval_path: Path,
    retry_authorization_path: Path | None,
) -> dict[str, Any]:
    status = failed_release_retry_status(release_dir, proposal_path, approval_path)
    if not status["eligible_for_retry_authorization"]:
        raise AcquisitionError(f"failed release is not retry-eligible: {status['blocking_reasons']}")
    if retry_authorization_path is None:
        raise AcquisitionError("separate retry authorization is required before any network operation")
    proposal = load_json(proposal_path)
    approval = load_json(approval_path)
    retry = load_json(retry_authorization_path)
    validated = validate_retry_authorization(proposal, approval, retry)
    return {"status": status, "retry_authorization": validated}


def failed_release_attempt3_status(
    release_dir: Path,
    proposal_path: Path,
    approval_path: Path,
    attempt2_authorization_path: Path,
) -> dict[str, Any]:
    blockers: list[str] = []
    try:
        proposal = load_json(proposal_path)
        approval = load_json(approval_path)
        attempt2_auth = load_json(attempt2_authorization_path)
        validate_retry_authorization(proposal, approval, attempt2_auth)
        request = load_json(release_dir / "request.json")
        local_approval = load_json(release_dir / "approval.json")
        manifest = load_json(release_dir / "manifest.json")
    except AcquisitionError as exc:
        return {"eligible_for_attempt_3": False, "blocking_reasons": [str(exc)]}
    expected_request = {
        **proposal_request_identity(proposal),
        "proposal_sha256": approval["proposal_sha256"],
        "request_sha256": approval["request_sha256"],
    }
    if request != expected_request or local_approval != approval:
        blockers.append("managed request or approval evidence mismatch")
    if manifest.get("state") != "failed":
        blockers.append("manifest is not in failed state")
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list) or len(attempts) != 2:
        blockers.append("exactly two append-only attempts are required")
    else:
        second = attempts[1]
        if (
            second.get("attempt_number") != 2
            or second.get("bytes_written") != approval["declared_content_length"]
            or second.get("transfer_result") != "completed"
        ):
            blockers.append("attempt 2 completion evidence mismatch")
    download = manifest.get("download") or {}
    if (
        download.get("bytes") != approval["declared_content_length"]
        or download.get("sha256")
        != "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9"
    ):
        blockers.append("attempt 2 download evidence mismatch")
    expected_paths = {
        "request.json", "approval.json", "manifest.json",
        "retry-authorization-attempt-2.json",
    }
    unexpected = []
    if not release_dir.is_dir() or release_dir.is_symlink():
        blockers.append("managed release directory is missing or unsafe")
    else:
        for path in release_dir.rglob("*"):
            relative = path.relative_to(release_dir).as_posix()
            if path.is_symlink():
                unexpected.append(relative)
            elif path.is_file() and relative not in expected_paths:
                unexpected.append(relative)
            elif path.is_dir() and relative != ".staging":
                unexpected.append(relative + "/")
    staging = release_dir / ".staging"
    if not staging.is_dir() or staging.is_symlink() or any(staging.iterdir()):
        blockers.append("staging is not an empty real directory")
    if unexpected:
        blockers.append("unexpected active, partial, extracted, or quarantine payload exists")
    return {
        "eligible_for_attempt_3": not blockers,
        "blocking_reasons": blockers,
        "unexpected_paths": unexpected,
        "attempts_sha256": sha256_json(attempts) if isinstance(attempts, list) else None,
        "network_calls": 0,
        "state_modified": False,
    }


def require_attempt3_authorization(
    release_dir: Path,
    proposal_path: Path,
    approval_path: Path,
    attempt2_authorization_path: Path,
    attempt3_authorization_path: Path,
) -> dict[str, Any]:
    status = failed_release_attempt3_status(
        release_dir, proposal_path, approval_path, attempt2_authorization_path
    )
    if not status["eligible_for_attempt_3"]:
        raise AcquisitionError(f"failed release is not attempt-3 eligible: {status['blocking_reasons']}")
    proposal = load_json(proposal_path)
    approval = load_json(approval_path)
    attempt2 = load_json(attempt2_authorization_path)
    attempt3 = load_json(attempt3_authorization_path)
    validated = validate_attempt3_authorization(proposal, approval, attempt2, attempt3)
    return {"status": status, "attempt3_authorization": validated}


def resume_failed_acquisition(
    proposal_path: Path,
    approval_path: Path,
    retry_authorization_path: Path,
    release_dir: Path,
    *,
    attempt_number: int = 2,
    attempt2_authorization_path: Path | None = None,
) -> dict[str, Any]:
    if attempt_number == 2:
        authorized = require_retry_authorization(
            release_dir, proposal_path, approval_path, retry_authorization_path
        )
        authorization_result = authorized["retry_authorization"]
        expected_archive_sha256 = None
        local_retry_name = "retry-authorization-attempt-2.json"
    elif attempt_number == 3 and attempt2_authorization_path is not None:
        authorized = require_attempt3_authorization(
            release_dir,
            proposal_path,
            approval_path,
            attempt2_authorization_path,
            retry_authorization_path,
        )
        authorization_result = authorized["attempt3_authorization"]
        expected_archive_sha256 = authorization_result["expected_archive_sha256"]
        local_retry_name = "transfer-authorization-attempt-3.json"
    else:
        raise AcquisitionError("only an explicitly validated attempt 2 or 3 is supported")
    proposal = load_json(proposal_path)
    approved = load_json(approval_path)
    retry = load_json(retry_authorization_path)
    manifest_path = release_dir / "manifest.json"
    manifest = load_json(manifest_path)
    prior_attempts_snapshot = json.dumps(
        manifest["attempts"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    staged = release_dir / ".staging" / "archive.part"
    final = release_dir / "archive.zip"
    filesystem_plan = plan_filesystem_layout(
        repository_root=Path(__file__).resolve().parents[3],
        source_root=release_dir.parent,
        staged_archive=staged,
        final_archive=final,
    )
    layout = create_and_revalidate_layout(filesystem_plan)
    if layout["created_directories"]:
        raise AcquisitionError("retry preflight unexpectedly needed to recreate managed directories")
    probe = staged.parent / ".exclusive-open-probe"
    try:
        with probe.open("xb") as handle:
            handle.write(b"")
            handle.flush()
            os.fsync(handle.fileno())
        probe.unlink()
    except BaseException:
        if probe.exists():
            probe.unlink()
        raise
    disk = preflight_download(
        approved_maximum_bytes=approved["approved_maximum_bytes"],
        content_length=approved["declared_content_length"],
        destination=staged,
    )
    if disk["available_disk_bytes"] < approved["minimum_free_disk_bytes"]:
        raise AcquisitionError("retry filesystem has less than the approved 4 GiB free-space floor")
    write_json_atomic(release_dir / local_retry_name, retry)
    manifest.setdefault("retry_authorizations", []).append({
        "attempt_number": attempt_number,
        "file": local_retry_name,
        "canonical_authorization_sha256": authorization_result[
            "canonical_authorization_sha256"
        ],
        "recorded_at": utc_now(),
    })
    manifest["retry_filesystem_preflight"] = {
        "device": layout["device"],
        "staging_directory": str(layout["staging_directory"]),
        "release_directory": str(layout["release_directory"]),
        "exclusive_probe_removed": not probe.exists(),
        "available_disk_bytes": disk["available_disk_bytes"],
    }
    write_json_atomic(manifest_path, manifest)
    head = fresh_head(
        approved["approved_canonical_endpoint"], proposal["delivery"]["max_redirects"]
    )
    original_head = manifest["network_preflight"]
    required_head = {
        "content_length": approved["declared_content_length"],
        "content_type": original_head["content_type"],
        "etag": original_head["etag"],
        "last_modified": original_head["last_modified"],
        "final_official_host": original_head["final_official_host"],
    }
    for key, expected in required_head.items():
        if head.get(key) != expected:
            raise AcquisitionError(f"attempt {attempt_number} HEAD evidence changed: {key}")
    manifest[f"attempt_{attempt_number}_head_preflight"] = {
        "attempt_number": attempt_number,
        "verified_at": utc_now(),
        **head,
    }
    write_json_atomic(manifest_path, manifest)
    transfer_completed = False
    download: dict[str, Any] | None = None
    try:
        download = stream_once(
            approved["approved_canonical_endpoint"],
            staged,
            max_redirects=proposal["delivery"]["max_redirects"],
            maximum_bytes=approved["approved_maximum_bytes"],
            declared_content_length=approved["declared_content_length"],
            expected_staging_root=staged.parent,
            expected_device=layout["device"],
            expected_etag=head["etag"],
            expected_last_modified=head["last_modified"],
        )
        transfer_completed = True
        completed_attempt = {
            "attempt_number": attempt_number,
            **download.pop("attempt_evidence"),
            "completed_at": download["completed_at"],
            "transfer_result": "completed",
        }
        manifest["attempts"].append(completed_attempt)
        if json.dumps(
            manifest["attempts"][:-1], ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ) != prior_attempts_snapshot:
            raise AcquisitionError("prior attempt evidence changed during retry")
        if expected_archive_sha256 is not None and download["sha256"] != expected_archive_sha256:
            raise AcquisitionError("completed archive SHA-256 differs from attempt authorization")
        manifest["state"] = "downloaded"
        manifest["lifecycle_outcome"] = "downloaded"
        manifest["download"] = download
        write_json_atomic(manifest_path, manifest)
        validation = validate_archive(staged, approved)
        os.replace(staged, final)
        staged.parent.rmdir()
        validation["archive_sha256"] = download["sha256"]
        validation["archive_bytes"] = download["bytes"]
        validation["promoted_archive"] = final.name
        write_json_atomic(release_dir / "validation.json", validation)
        (release_dir / "checksums.txt").write_text(
            f"{download['sha256']}  {final.name}\n", encoding="utf-8"
        )
        manifest["state"] = "validated"
        manifest["lifecycle_outcome"] = "promoted"
        manifest["validation"] = {
            "result": "passed",
            "validated_at": validation["validated_at"],
            "report": "validation.json",
            "promoted_archive": final.name,
        }
        manifest["errors"] = manifest.get("errors", [])
        write_json_atomic(manifest_path, manifest)
        return {"manifest": manifest, "validation": validation}
    except BaseException as exc:
        if isinstance(exc, TransferFailure):
            manifest["attempts"].append({"attempt_number": attempt_number, **exc.evidence})
        if transfer_completed and download is not None and staged.exists():
            classification = (
                exc.classification
                if isinstance(exc, StructuralPolicyError)
                else "structural_rejection"
            )
            policy_rule = (
                exc.policy_rule if isinstance(exc, StructuralPolicyError)
                else "structural_validation"
            )
            try:
                quarantine = quarantine_complete_archive(
                    staged,
                    release_dir,
                    byte_count=download["bytes"],
                    sha256=download["sha256"],
                    attempt_number=attempt_number,
                    reason=str(exc),
                    validator="validate_archive",
                    policy_rule=policy_rule,
                    classification=classification,
                )
                manifest["state"] = "quarantined"
                manifest["lifecycle_outcome"] = "quarantined"
                manifest["quarantine"] = quarantine
                write_json_atomic(
                    release_dir / "validation.json",
                    {
                        "result": "policy_blocked" if classification == "unresolved_policy_limit" else "rejected",
                        **quarantine,
                    },
                )
            except BaseException as quarantine_exc:
                manifest["state"] = "failed"
                manifest["lifecycle_outcome"] = "downloaded_quarantine_failed"
                manifest["quarantine_error"] = (
                    f"{type(quarantine_exc).__name__}: {quarantine_exc}"
                )
        else:
            manifest["state"] = "failed"
            manifest["lifecycle_outcome"] = "transfer_failed"
        manifest["failed_at"] = utc_now()
        manifest["errors"] = [
            *manifest.get("errors", []),
            f"attempt {attempt_number} {type(exc).__name__}: {exc}",
        ]
        write_json_atomic(manifest_path, manifest)
        if not transfer_completed and staged.exists():
            staged.unlink()
        raise


def acquire(
    proposal_path: Path,
    approval_path: Path,
    release_dir: Path,
) -> dict[str, Any]:
    proposal = load_json(proposal_path)
    approved = load_json(approval_path)
    approval = validate_approved_artifact(proposal, approved)
    if release_dir.exists():
        raise AcquisitionError("immutable source release directory already exists")
    staged = release_dir / ".staging" / "archive.part"
    final = release_dir / "archive.zip"
    filesystem_plan = plan_filesystem_layout(
        repository_root=Path(__file__).resolve().parents[3],
        source_root=release_dir.parent,
        staged_archive=staged,
        final_archive=final,
    )
    head = fresh_head(approval["canonical_endpoint"], proposal["delivery"]["max_redirects"])
    if head["content_length"] != approved["declared_content_length"]:
        raise AcquisitionError("fresh HEAD Content-Length differs from explicit approval")
    preflight = preflight_download(
        approved_maximum_bytes=approved["approved_maximum_bytes"],
        content_length=head["content_length"],
        destination=release_dir / ".staging" / "archive.part",
    )
    if preflight["available_disk_bytes"] < approved["minimum_free_disk_bytes"]:
        raise AcquisitionError("available disk is below the explicit 4 GiB approval floor")
    print(json.dumps({"network_preflight": head, "disk_preflight": preflight}, sort_keys=True), flush=True)
    layout = create_and_revalidate_layout(filesystem_plan)
    actual_preflight = preflight_download(
        approved_maximum_bytes=approved["approved_maximum_bytes"],
        content_length=head["content_length"],
        destination=staged,
    )
    if actual_preflight["available_disk_bytes"] < approved["minimum_free_disk_bytes"]:
        cleanup_created_empty_directories(layout["created_directories"])
        raise AcquisitionError("actual target filesystem is below the explicit 4 GiB approval floor")
    request = {
        **proposal_request_identity(proposal),
        "proposal_sha256": approved["proposal_sha256"],
        "request_sha256": approved["request_sha256"],
    }
    write_json_atomic(release_dir / "request.json", request)
    write_json_atomic(release_dir / "approval.json", approved)
    manifest = {
        "manifest_schema_version": 1,
        "source_code": "col_xr",
        "state": "planned",
        "release": approved["release_identity"],
        "proposal_sha256": approved["proposal_sha256"],
        "request_definition_sha256": approved["request_sha256"],
        "approval_file": "approval.json",
        "network_preflight": head,
        "disk_preflight": preflight,
        "filesystem_preflight": {
            "staged_nearest_existing_ancestor": str(filesystem_plan["staged_nearest_existing_ancestor"]),
            "final_nearest_existing_ancestor": str(filesystem_plan["final_nearest_existing_ancestor"]),
            "planned_device": filesystem_plan["planned_device"],
            "created_directories": [str(path) for path in layout["created_directories"]],
            "actual_device": layout["device"],
            "actual_available_disk_bytes": actual_preflight["available_disk_bytes"],
            "atomic_replace_layout": True,
        },
        "execution": {"planned_at": utc_now(), "transfer_attempt_limit": 1},
        "download": None,
        "validation": None,
        "errors": [],
    }
    write_json_atomic(release_dir / "manifest.json", manifest)
    transfer_completed = False
    download: dict[str, Any] | None = None
    try:
        download = stream_once(
            approval["canonical_endpoint"],
            staged,
            max_redirects=proposal["delivery"]["max_redirects"],
            maximum_bytes=approved["approved_maximum_bytes"],
            declared_content_length=approved["declared_content_length"],
            expected_staging_root=staged.parent,
            expected_device=layout["device"],
        )
        manifest["state"] = "downloaded"
        manifest["lifecycle_outcome"] = "downloaded"
        manifest["download"] = download
        write_json_atomic(release_dir / "manifest.json", manifest)
        transfer_completed = True
        validation = validate_archive(staged, approved)
        os.replace(staged, final)
        staging = staged.parent
        staging.rmdir()
        validation["archive_sha256"] = download["sha256"]
        validation["archive_bytes"] = download["bytes"]
        validation["promoted_archive"] = final.name
        write_json_atomic(release_dir / "validation.json", validation)
        (release_dir / "checksums.txt").write_text(
            f"{download['sha256']}  {final.name}\n", encoding="utf-8"
        )
        manifest["state"] = "validated"
        manifest["lifecycle_outcome"] = "promoted"
        manifest["validation"] = {
            "result": "passed",
            "validated_at": validation["validated_at"],
            "report": "validation.json",
            "promoted_archive": final.name,
        }
        write_json_atomic(release_dir / "manifest.json", manifest)
        return {"manifest": manifest, "validation": validation}
    except BaseException as exc:
        if transfer_completed and download is not None and staged.exists():
            classification = (
                exc.classification
                if isinstance(exc, StructuralPolicyError)
                else "structural_rejection"
            )
            policy_rule = exc.policy_rule if isinstance(exc, StructuralPolicyError) else "structural_validation"
            quarantine = quarantine_complete_archive(
                staged,
                release_dir,
                byte_count=download["bytes"],
                sha256=download["sha256"],
                attempt_number=1,
                reason=str(exc),
                validator="validate_archive",
                policy_rule=policy_rule,
                classification=classification,
            )
            manifest["state"] = "quarantined"
            manifest["lifecycle_outcome"] = "quarantined"
            manifest["quarantine"] = quarantine
        else:
            manifest["state"] = "failed"
            manifest["lifecycle_outcome"] = "transfer_failed"
        manifest["failed_at"] = utc_now()
        manifest["errors"] = [f"{type(exc).__name__}: {exc}"]
        if isinstance(exc, TransferFailure):
            manifest.setdefault("attempts", []).append({
                "attempt_number": len(manifest.get("attempts", [])) + 1,
                **exc.evidence,
            })
        write_json_atomic(release_dir / "manifest.json", manifest)
        if not transfer_completed and staged.exists():
            staged.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    status_parser = sub.add_parser("retry-status")
    status_parser.add_argument("release_dir", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    if args.command == "retry-status":
        result = failed_release_retry_status(
            args.release_dir,
            root / "database/taxonomy/col-xr-source-selection.proposal.json",
            root / "database/taxonomy/col-xr-source-selection.approved.json",
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0 if result["eligible_for_retry_authorization"] else 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
