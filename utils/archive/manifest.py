"""Deterministic, side-effect-free Sporely archive manifest v1 contract."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from utils.archive.paths import canonical_archive_path


FORMAT = "sporely-archive"
FORMAT_VERSION = 1
_MODES = {"full_backup": "preserve", "portable_observations": "portable"}
_STATUSES = {"included", "excluded_by_policy", "missing_at_source"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TOP_LEVEL_KEYS = {
    "format", "format_version", "mode", "identity_policy", "archive_id",
    "created_at", "app_version", "schema_version", "source_platform",
    "contents", "files",
}


class ManifestError(ValueError):
    """Raised when a manifest violates the v1 contract."""


@dataclass(frozen=True)
class ManifestFile:
    path: str
    status: str
    size: int | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        try:
            canonical_archive_path(self.path)
        except ValueError as exc:
            raise ManifestError(str(exc)) from exc
        if self.path == "manifest.json":
            raise ManifestError("manifest.json must not list or hash itself")
        if not isinstance(self.status, str) or self.status not in _STATUSES:
            raise ManifestError(f"invalid file status: {self.status!r}")
        if self.status == "included":
            if not isinstance(self.size, int) or isinstance(self.size, bool) or self.size < 0:
                raise ManifestError("included files require a non-negative integer size")
            if not isinstance(self.sha256, str) or not _SHA256.fullmatch(self.sha256):
                raise ManifestError("included files require a lowercase SHA-256 digest")
        elif self.size is not None or self.sha256 is not None:
            raise ManifestError("excluded or missing files must omit size and sha256")

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"path": self.path, "status": self.status}
        if self.status == "included":
            result["size"] = self.size
            result["sha256"] = self.sha256
        return result

    @classmethod
    def from_dict(cls, value: object) -> "ManifestFile":
        if not isinstance(value, dict):
            raise ManifestError("file entries must be objects")
        allowed = {"path", "status", "size", "sha256"}
        if set(value) - allowed:
            raise ManifestError("file entry contains unknown fields")
        if "path" not in value or "status" not in value:
            raise ManifestError("file entry requires path and status")
        return cls(
            path=value["path"],
            status=value["status"],
            size=value.get("size"),
            sha256=value.get("sha256"),
        )


@dataclass(frozen=True)
class ArchiveManifest:
    mode: str
    identity_policy: str
    archive_id: str
    created_at: str
    app_version: str
    source_platform: str
    contents: dict[str, int]
    files: tuple[ManifestFile, ...]
    schema_version: None = None
    format: str = FORMAT
    format_version: int = FORMAT_VERSION

    def __post_init__(self) -> None:
        if (
            self.format != FORMAT
            or type(self.format_version) is not int
            or self.format_version != FORMAT_VERSION
        ):
            raise ManifestError("unsupported archive format or version")
        if not isinstance(self.mode, str) or self.mode not in _MODES:
            raise ManifestError(f"invalid archive mode: {self.mode!r}")
        if self.identity_policy != _MODES[self.mode]:
            raise ManifestError("identity policy does not match archive mode")
        for field_name in ("archive_id", "created_at", "app_version", "source_platform"):
            if not isinstance(getattr(self, field_name), str) or not getattr(self, field_name):
                raise ManifestError(f"{field_name} must be a non-empty string")
        if self.schema_version is not None:
            raise ManifestError("manifest v1 schema_version must be null")
        if not isinstance(self.contents, dict):
            raise ManifestError("contents must be an object")
        for key, count in self.contents.items():
            if not isinstance(key, str) or not key:
                raise ManifestError("content names must be non-empty strings")
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ManifestError("content counts must be non-negative integers")
        if not isinstance(self.files, tuple) or not all(isinstance(entry, ManifestFile) for entry in self.files):
            raise ManifestError("files must be a tuple of ManifestFile entries")
        paths = [entry.path for entry in self.files]
        if len(paths) != len(set(paths)):
            raise ManifestError("manifest contains duplicate file paths")
        if len({path.casefold() for path in paths}) != len(paths):
            raise ManifestError("manifest contains case-folding path collisions")

    def to_dict(self) -> dict[str, object]:
        return {
            "format": self.format,
            "format_version": self.format_version,
            "mode": self.mode,
            "identity_policy": self.identity_policy,
            "archive_id": self.archive_id,
            "created_at": self.created_at,
            "app_version": self.app_version,
            "schema_version": None,
            "source_platform": self.source_platform,
            "contents": {key: self.contents[key] for key in sorted(self.contents)},
            "files": [entry.to_dict() for entry in sorted(self.files, key=lambda item: item.path)],
        }

    def to_json_bytes(self) -> bytes:
        return (json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    @classmethod
    def from_json(cls, payload: str | bytes) -> "ArchiveManifest":
        try:
            value = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise ManifestError("manifest is not valid UTF-8 JSON") from exc
        if not isinstance(value, dict) or set(value) != _TOP_LEVEL_KEYS:
            raise ManifestError("manifest fields do not match the v1 contract")
        files = value["files"]
        if not isinstance(files, list):
            raise ManifestError("files must be an array")
        return cls(
            format=value["format"], format_version=value["format_version"],
            mode=value["mode"], identity_policy=value["identity_policy"],
            archive_id=value["archive_id"], created_at=value["created_at"],
            app_version=value["app_version"], schema_version=value["schema_version"],
            source_platform=value["source_platform"], contents=value["contents"],
            files=tuple(ManifestFile.from_dict(item) for item in files),
        )


def build_manifest(
    *, mode: str, archive_id: str, created_at: str, app_version: str,
    source_platform: str, contents: dict[str, int], files: list[ManifestFile] | tuple[ManifestFile, ...],
) -> ArchiveManifest:
    """Build a v1 manifest with the mode's required identity policy."""
    identity_policy = _MODES.get(mode)
    if identity_policy is None:
        raise ManifestError(f"invalid archive mode: {mode!r}")
    return ArchiveManifest(
        mode=mode, identity_policy=identity_policy, archive_id=archive_id,
        created_at=created_at, app_version=app_version,
        source_platform=source_platform, contents=dict(contents), files=tuple(files),
    )
