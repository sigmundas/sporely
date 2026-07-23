#!/usr/bin/env python3
"""Fixture-first, version-pinned COL XR request and manifest handling.

This module deliberately exposes no live-download CLI command. Transport is
injected for offline tests and later acquisition work.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Protocol
from urllib.parse import parse_qsl, urlparse


REQUEST_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
SOURCE_CODE = "col_xr"
RELEASE_TYPE = "Extended Release"
SUPPORTED_REQUEST_FORMATS = {"ColDP", "DwCA", "TextTree"}
STRUCTURALLY_VALIDATED_FORMATS = {"ColDP"}
MANIFEST_STATES = {"planned", "downloaded", "validated", "failed"}
SECRET_KEYS = {
    "authorization", "authorization_header", "api_key", "apikey", "password",
    "secret", "signed_url", "token", "access_token", "refresh_token",
}
SENSITIVE_QUERY_KEYS = {
    "authorization", "key", "signature", "sig", "token", "access_token",
}
DEFAULT_SOURCES_ROOT = Path(__file__).resolve().parents[1] / "sources" / SOURCE_CODE
OFFICIAL_ENDPOINT_HOSTS = {
    "api.checklistbank.org",
    "checklistbank.org",
    "www.checklistbank.org",
    "download.checklistbank.org",
    "catalogueoflife.org",
    "www.catalogueoflife.org",
}
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_REDIRECTS = 5
DEFAULT_USER_AGENT = "Sporely-Taxonomy-Acquisition/1 (+https://sporely.com)"


class AcquisitionError(ValueError):
    pass


class ImmutableReleaseError(AcquisitionError):
    pass


class TransportError(AcquisitionError):
    pass


@dataclass(frozen=True)
class DownloadPolicy:
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_redirects: int = DEFAULT_MAX_REDIRECTS
    user_agent: str = DEFAULT_USER_AGENT
    max_bytes: int | None = None
    declared_content_length: int | None = None

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0 or self.max_redirects < 0 or not self.user_agent.strip():
            raise AcquisitionError("download policy must have bounded timeout/redirects and a user agent")
        if self.max_bytes is not None and self.max_bytes <= 0:
            raise AcquisitionError("maximum archive bytes must be positive")
        if self.declared_content_length is not None:
            if self.declared_content_length <= 0:
                raise AcquisitionError("declared Content-Length must be positive")
            if self.max_bytes is not None and self.declared_content_length > self.max_bytes:
                raise AcquisitionError("declared archive size exceeds the approved maximum")


class Transport(Protocol):
    def stream(self, request: "ColXrRequest", policy: DownloadPolicy) -> Iterable[bytes]: ...


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path, chunk_bytes: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_bytes):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[3],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _reject_secrets(value: Any, path: str = "request") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            if normalized in SECRET_KEYS or normalized.endswith("_token") or normalized.endswith("_secret"):
                raise AcquisitionError(f"secret field is forbidden in persisted request: {path}.{key}")
            _reject_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secrets(child, f"{path}[{index}]")


def _validate_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise AcquisitionError("issued_date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise AcquisitionError("issued_date must be canonical YYYY-MM-DD")
    return value


def _validate_endpoint(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise AcquisitionError("endpoint must be an absolute HTTPS URL")
    if parsed.username or parsed.password:
        raise AcquisitionError("endpoint must not contain credentials")
    if parsed.hostname not in OFFICIAL_ENDPOINT_HOSTS:
        raise AcquisitionError("endpoint must use an official COL or ChecklistBank host")
    if any(key.lower() in SENSITIVE_QUERY_KEYS for key, _ in parse_qsl(parsed.query)):
        raise AcquisitionError("endpoint must not contain signed or secret query parameters")
    return value


@dataclass(frozen=True)
class ColXrRequest:
    release_label: str
    dataset_key: int
    issued_date: str
    doi: str | None
    archive_format: str
    taxonomic_scope: dict[str, Any]
    included_fields: tuple[str, ...]
    endpoint: str
    expected_license: str
    release_type: str = RELEASE_TYPE
    source_code: str = SOURCE_CODE
    request_schema_version: int = REQUEST_SCHEMA_VERSION

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ColXrRequest":
        if not isinstance(raw, dict):
            raise AcquisitionError("request definition must be an object")
        _reject_secrets(raw)
        if "approval_status" in raw or "download_authorized" in raw:
            if raw.get("approval_status") != "approved" or raw.get("download_authorized") is not True:
                raise AcquisitionError("source-selection proposal is not approved for acquisition")
        release_label = str(raw.get("release_label", "")).strip()
        if not release_label:
            raise AcquisitionError("release_label is required")
        if "latest" in release_label.casefold():
            raise AcquisitionError("floating latest release selection is forbidden")
        release_type = str(raw.get("release_type", "")).strip()
        if release_type != RELEASE_TYPE:
            raise AcquisitionError(f"release_type must be {RELEASE_TYPE!r}")
        dataset_key = raw.get("dataset_key")
        if isinstance(dataset_key, bool) or not isinstance(dataset_key, int) or dataset_key <= 0:
            raise AcquisitionError("dataset_key must be a positive ChecklistBank integer")
        issued_date = _validate_date(str(raw.get("issued_date", "")).strip())
        if issued_date not in release_label:
            raise AcquisitionError("release_label and issued_date metadata do not match")
        archive_format = str(raw.get("archive_format", "")).strip()
        if archive_format not in SUPPORTED_REQUEST_FORMATS:
            raise AcquisitionError(f"unsupported archive_format: {archive_format!r}")
        scope = raw.get("taxonomic_scope")
        if not isinstance(scope, dict) or not scope:
            raise AcquisitionError("taxonomic_scope must be an explicit non-empty object")
        fields = raw.get("included_fields")
        if not isinstance(fields, list) or not fields or not all(isinstance(item, str) and item.strip() for item in fields):
            raise AcquisitionError("included_fields must be a non-empty string list")
        normalized_fields = tuple(sorted(set(item.strip() for item in fields)))
        endpoint = _validate_endpoint(str(raw.get("endpoint", "")).strip())
        endpoint_dataset = re.search(r"/dataset/([0-9]+)(?:/|$)", urlparse(endpoint).path)
        if endpoint_dataset and int(endpoint_dataset.group(1)) != dataset_key:
            raise AcquisitionError("endpoint dataset key and request metadata do not match")
        expected_license = str(raw.get("expected_license", "")).strip()
        if not expected_license:
            raise AcquisitionError("expected_license is required")
        doi_raw = raw.get("doi")
        doi = str(doi_raw).strip() if doi_raw is not None else None
        if doi == "":
            doi = None
        if doi is not None and not doi.startswith("10."):
            raise AcquisitionError("doi must be absent or start with '10.'")
        schema_version = raw.get("request_schema_version", REQUEST_SCHEMA_VERSION)
        if schema_version != REQUEST_SCHEMA_VERSION:
            raise AcquisitionError(f"request_schema_version must be {REQUEST_SCHEMA_VERSION}")
        source_code = str(raw.get("source_code", SOURCE_CODE)).strip()
        if source_code != SOURCE_CODE:
            raise AcquisitionError(f"source_code must be {SOURCE_CODE!r}")
        return cls(
            release_label=release_label,
            dataset_key=dataset_key,
            issued_date=issued_date,
            doi=doi,
            archive_format=archive_format,
            taxonomic_scope=scope,
            included_fields=normalized_fields,
            endpoint=endpoint,
            expected_license=expected_license,
            release_type=release_type,
            source_code=source_code,
            request_schema_version=schema_version,
        )

    def identity(self) -> dict[str, Any]:
        return {
            "request_schema_version": self.request_schema_version,
            "source_code": self.source_code,
            "release_label": self.release_label,
            "release_type": self.release_type,
            "dataset_key": self.dataset_key,
            "issued_date": self.issued_date,
            "doi": self.doi,
            "archive_format": self.archive_format,
            "taxonomic_scope": self.taxonomic_scope,
            "included_fields": list(self.included_fields),
            "endpoint": self.endpoint,
            "expected_license": self.expected_license,
        }

    @property
    def request_sha256(self) -> str:
        return sha256_json(self.identity())

    def persisted(self, *, created_at: str | None = None, tool_git_commit: str | None = None) -> dict[str, Any]:
        return {
            **self.identity(),
            "canonical_request_sha256": self.request_sha256,
            "execution": {
                "created_at": created_at or utc_now(),
                "tool_git_commit": tool_git_commit if tool_git_commit is not None else git_commit(),
            },
        }


def load_request(path: Path) -> ColXrRequest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"invalid request file {path}: {exc}") from exc
    return ColXrRequest.from_dict(raw)


def safe_release_slug(release_label: str) -> str:
    text = release_label.strip()
    if not text or Path(text).is_absolute() or ".." in text or "/" in text or "\\" in text:
        raise AcquisitionError("unsafe release label")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-._")
    if not slug or slug in {".", ".."}:
        raise AcquisitionError("release label cannot produce an empty safe path")
    return slug


def release_dir(sources_root: Path, request: ColXrRequest) -> Path:
    root = sources_root.resolve()
    candidate = (root / safe_release_slug(request.release_label)).resolve()
    if candidate.parent != root:
        raise AcquisitionError("release path escapes source root")
    return candidate


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"invalid state file {path}: {exc}") from exc


def planned_manifest(request: ColXrRequest, *, created_at: str | None = None) -> dict[str, Any]:
    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "source_code": SOURCE_CODE,
        "state": "planned",
        "request_definition_sha256": request.request_sha256,
        "release": {
            "label": request.release_label,
            "type": request.release_type,
            "dataset_key": request.dataset_key,
            "issued_date": request.issued_date,
            "doi": request.doi,
            "archive_format": request.archive_format,
            "expected_license": request.expected_license,
        },
        "execution": {
            "planned_at": created_at or utc_now(),
            "tool_git_commit": git_commit(),
        },
        "download": None,
        "validation": None,
        "errors": [],
        "warnings": [],
    }


def validate_manifest(manifest: dict[str, Any]) -> None:
    state = manifest.get("state")
    if state not in MANIFEST_STATES:
        raise AcquisitionError(f"invalid manifest state: {state!r}")
    if state in {"downloaded", "validated"}:
        download = manifest.get("download") or {}
        required = {"resolved_source_url", "archive_filename", "bytes", "sha256", "completed_at", "staging_path"}
        if required - set(download) or not isinstance(download.get("bytes"), int) or download["bytes"] <= 0:
            raise AcquisitionError("downloaded manifest lacks required non-empty archive evidence")
    if state == "validated":
        validation = manifest.get("validation") or {}
        required = {
            "validated_at", "result", "extraction_format", "metadata_files",
            "declared_record_counts", "observed_record_counts",
        }
        if required - set(validation) or validation.get("result") != "passed":
            raise AcquisitionError("validated manifest lacks passing validation evidence")
        if not manifest["download"].get("promoted_archive"):
            raise AcquisitionError("validated manifest lacks promoted archive evidence")


def plan(request: ColXrRequest, sources_root: Path = DEFAULT_SOURCES_ROOT) -> tuple[Path, bool]:
    target = release_dir(sources_root, request)
    request_path = target / "request.json"
    manifest_path = target / "manifest.json"
    if target.exists():
        if not request_path.is_file() or not manifest_path.is_file():
            raise ImmutableReleaseError("release directory exists without complete managed state")
        existing = _read_json(request_path)
        if existing.get("canonical_request_sha256") != request.request_sha256:
            raise ImmutableReleaseError("release directory belongs to a different immutable request")
        manifest = _read_json(manifest_path)
        validate_manifest(manifest)
        return target, True
    target.mkdir(parents=True)
    _write_json_atomic(request_path, request.persisted())
    _write_json_atomic(manifest_path, planned_manifest(request))
    return target, False


def _failed_manifest(manifest: dict[str, Any], message: str) -> dict[str, Any]:
    failed = dict(manifest)
    failed["state"] = "failed"
    failed["failed_at"] = utc_now()
    failed["errors"] = [*manifest.get("errors", []), message]
    return failed


def stage_download(
    request: ColXrRequest,
    target: Path,
    transport: Transport,
    *,
    resolved_source_url: str | None = None,
    download_policy: DownloadPolicy = DownloadPolicy(),
) -> Path:
    manifest_path = target / "manifest.json"
    manifest = _read_json(manifest_path)
    if manifest.get("request_definition_sha256") != request.request_sha256:
        raise ImmutableReleaseError("manifest request identity mismatch")
    if manifest.get("state") != "planned":
        raise AcquisitionError("download staging requires planned state")
    staging = target / ".staging" / request.request_sha256[:16]
    staging.mkdir(parents=True, exist_ok=True)
    part = staging / "archive.part"
    digest = hashlib.sha256()
    byte_count = 0
    try:
        safe_resolved_source_url = _validate_endpoint(resolved_source_url or request.endpoint)
        with part.open("xb") as handle:
            for chunk in transport.stream(request, download_policy):
                if not isinstance(chunk, bytes) or not chunk:
                    raise TransportError("transport yielded an invalid or empty chunk")
                handle.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
                if download_policy.max_bytes is not None and byte_count > download_policy.max_bytes:
                    raise TransportError("received archive bytes exceed the approved maximum")
            handle.flush()
            os.fsync(handle.fileno())
        if byte_count == 0:
            raise TransportError("downloaded archive is empty")
        if (
            download_policy.declared_content_length is not None
            and byte_count != download_policy.declared_content_length
        ):
            raise TransportError("received archive bytes disagree with declared Content-Length")
    except BaseException as exc:
        _write_json_atomic(manifest_path, _failed_manifest(manifest, f"{type(exc).__name__}: {exc}"))
        raise
    downloaded = dict(manifest)
    downloaded["state"] = "downloaded"
    downloaded["download"] = {
        "resolved_source_url": safe_resolved_source_url,
        "archive_filename": "archive.zip",
        "bytes": byte_count,
        "sha256": digest.hexdigest(),
        "completed_at": utc_now(),
        "staging_path": str(part.relative_to(target)),
        "promoted_archive": None,
        "transport_policy": {
            "timeout_seconds": download_policy.timeout_seconds,
            "max_redirects": download_policy.max_redirects,
            "user_agent": download_policy.user_agent,
            "max_bytes": download_policy.max_bytes,
            "declared_content_length": download_policy.declared_content_length,
        },
    }
    validate_manifest(downloaded)
    _write_json_atomic(manifest_path, downloaded)
    return part


def _safe_zip_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    members = archive.infolist()
    if not members:
        raise AcquisitionError("archive is empty")
    for member in members:
        name = member.filename
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or "\\" in name or not name:
            raise AcquisitionError(f"unsafe ZIP member path: {name!r}")
        mode = member.external_attr >> 16
        if (mode & 0o170000) == 0o120000:
            raise AcquisitionError(f"ZIP symlink is forbidden: {name!r}")
    return members


def _flat_fixture_metadata(raw: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in raw.decode("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if ":" not in line:
            raise AcquisitionError("fixture metadata.yaml must use flat key: value lines")
        key, value = line.split(":", 1)
        result[key.strip()] = value.strip().strip("'\"")
    return result


def inspect_fixture_archive(archive_path: Path, request: ColXrRequest) -> dict[str, Any]:
    if request.archive_format not in STRUCTURALLY_VALIDATED_FORMATS:
        raise AcquisitionError(f"fixture structural validation is not implemented for {request.archive_format}")
    if not archive_path.is_file() or archive_path.stat().st_size == 0:
        raise AcquisitionError("archive is missing or empty")
    try:
        with zipfile.ZipFile(archive_path) as archive:
            bad = archive.testzip()
            if bad:
                raise AcquisitionError(f"ZIP integrity failed at {bad}")
            members = _safe_zip_members(archive)
            names = {member.filename.casefold(): member.filename for member in members if not member.is_dir()}
            if "metadata.yaml" not in names:
                raise AcquisitionError("ColDP fixture archive lacks metadata.yaml")
            usage_names = [
                original for folded, original in names.items()
                if re.fullmatch(r"(name[-_]?usage|taxon)\.(csv|tsv|tab|txt)", folded)
            ]
            if not usage_names:
                raise AcquisitionError("ColDP fixture archive lacks a NameUsage or Taxon table")
            metadata_name = names["metadata.yaml"]
            metadata = _flat_fixture_metadata(archive.read(metadata_name))
            expected = {
                "release": request.release_label,
                "datasetKey": str(request.dataset_key),
                "issued": request.issued_date,
                "format": request.archive_format,
            }
            for key, value in expected.items():
                if metadata.get(key) != value:
                    raise AcquisitionError(f"archive metadata mismatch for {key}: {metadata.get(key)!r} != {value!r}")
            observed = 0
            for name in usage_names:
                lines = [line for line in archive.read(name).decode("utf-8").splitlines() if line.strip()]
                observed += max(0, len(lines) - 1)
    except (zipfile.BadZipFile, UnicodeDecodeError) as exc:
        raise AcquisitionError(f"malformed fixture archive: {exc}") from exc
    return {
        "result": "passed",
        "extraction_format": "ColDP fixture structural boundary",
        "metadata_files": [metadata_name],
        "declared_record_counts": {"name_usages": int(metadata.get("recordCount", observed))},
        "observed_record_counts": {"name_usages": observed},
        "warnings": [
            "Synthetic fixture validation does not prove the selected production COL export structure or full field contract"
        ],
    }


def validate_and_promote(request: ColXrRequest, target: Path, archive_path: Path) -> dict[str, Any]:
    manifest_path = target / "manifest.json"
    manifest = _read_json(manifest_path)
    if manifest.get("request_definition_sha256") != request.request_sha256:
        raise ImmutableReleaseError("manifest request identity mismatch")
    if manifest.get("state") != "downloaded":
        raise AcquisitionError("archive validation requires downloaded state")
    expected_staging = (target / manifest["download"]["staging_path"]).resolve()
    if archive_path.resolve() != expected_staging:
        raise AcquisitionError("archive path does not match manifest staging evidence")
    digest = sha256_file(archive_path)
    if digest != manifest["download"]["sha256"] or archive_path.stat().st_size != manifest["download"]["bytes"]:
        raise AcquisitionError("staged archive checksum or byte size changed")
    try:
        validation = inspect_fixture_archive(archive_path, request)
    except BaseException as exc:
        _write_json_atomic(manifest_path, _failed_manifest(manifest, f"{type(exc).__name__}: {exc}"))
        raise
    promoted = target / "archive.zip"
    if promoted.exists():
        if sha256_file(promoted) != digest:
            raise ImmutableReleaseError("different immutable archive already exists")
    else:
        os.replace(archive_path, promoted)
    validation["validated_at"] = utc_now()
    validation["warnings"] = validation.get("warnings", [])
    _write_json_atomic(target / "validation.json", validation)
    validated = dict(manifest)
    validated["state"] = "validated"
    validated["download"] = {**manifest["download"], "promoted_archive": promoted.name}
    validated["validation"] = validation
    validated["warnings"] = [*manifest.get("warnings", []), *validation["warnings"]]
    validate_manifest(validated)
    _write_json_atomic(manifest_path, validated)
    staging_dir = target / ".staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    return validated


def status(target: Path) -> dict[str, Any]:
    manifest = _read_json(target / "manifest.json")
    validate_manifest(manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("validate-request", "normalize", "plan"):
        item = sub.add_parser(command)
        item.add_argument("request", type=Path)
        if command == "plan":
            item.add_argument("--sources-root", type=Path, default=DEFAULT_SOURCES_ROOT)
    validate_archive = sub.add_parser("validate-archive")
    validate_archive.add_argument("request", type=Path)
    validate_archive.add_argument("archive", type=Path)
    show = sub.add_parser("status")
    show.add_argument("release_dir", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "status":
            print(json.dumps(status(args.release_dir), ensure_ascii=False, sort_keys=True, indent=2))
            return 0
        request = load_request(args.request)
        if args.command == "validate-request":
            print(request.request_sha256)
        elif args.command == "normalize":
            print(json.dumps(
                {**request.identity(), "canonical_request_sha256": request.request_sha256},
                ensure_ascii=False, sort_keys=True, indent=2,
            ))
        elif args.command == "plan":
            target, idempotent = plan(request, args.sources_root)
            print(json.dumps({
                "dry_run": True,
                "network_calls": 0,
                "release_dir": str(target),
                "request_sha256": request.request_sha256,
                "idempotent": idempotent,
            }, sort_keys=True))
        elif args.command == "validate-archive":
            result = inspect_fixture_archive(args.archive, request)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    except AcquisitionError as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
