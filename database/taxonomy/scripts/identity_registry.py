#!/usr/bin/env python3
"""Append-only Sporely taxon-identity registry.

The registry is the single authoritative allocator of ``sporely_taxon_id``.
Every entry binds an internal Sporely concept to one external source usage
identified by ``(source, namespace, identifier)``. Identifiers are text; the
``source`` and ``namespace`` values follow the taxonomy identity contract in
``database/taxonomy/docs/identity-contract.md``.

Design constraints:

* Append-only. Historic anchor entries and aliases are never rewritten.
  Corrections would be new lines with a ``superseded_by`` reference; that
  workflow is not exercised at Stage 3A but the file format leaves room for
  it.
* ``sporely_taxon_id`` is never derived from an external identifier value or
  from a source row number. Allocation is a monotonically increasing integer
  starting at 1, ordered by the caller's deterministic driving sequence.
* Never allocates two Sporely IDs for the same ``(source, namespace,
  identifier)`` key.
* Deterministic: for a given ordered sequence of ``allocate`` / ``bind_alias``
  calls, the on-disk registry file is byte-identical across runs.
* Human-reviewable: each line is a single JSON object with sorted keys.

The registry stores only identity allocations. Source-usage attributes such
as scientific name, rank, or kingdom live in normalized-source files and in
compiled-release artifacts. This keeps the registry compact and stable.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


REGISTRY_SCHEMA_VERSION = 1
REGISTRY_HEADER_KEY = "__registry_header__"

SHARD_MANIFEST_SCHEMA_VERSION = 1
SHARD_MANIFEST_FILENAME = "manifest.json"
SHARD_FILENAME_PATTERN = "part-{index:04d}.jsonl"
SHARD_DEFAULT_TARGET_BYTES = 25 * 1024 * 1024  # 25 MiB
CHUNK_BYTES = 1 * 1024 * 1024
ENTRY_KIND_ANCHOR = "anchor"
ENTRY_KIND_ALIAS = "alias"


class RegistryError(Exception):
    """Raised on any registry-consistency or IO problem."""


@dataclass(frozen=True)
class Allocation:
    sporely_taxon_id: int
    source: str
    namespace: str
    identifier: str
    allocated_in_release: str
    first_seen_source_release: str
    kind: str  # ENTRY_KIND_ANCHOR or ENTRY_KIND_ALIAS

    def key(self) -> tuple[str, str, str]:
        return (self.source, self.namespace, self.identifier)

    def to_json_line(self) -> str:
        payload = {
            "sporely_taxon_id": self.sporely_taxon_id,
            "source": self.source,
            "namespace": self.namespace,
            "identifier": self.identifier,
            "allocated_in_release": self.allocated_in_release,
            "first_seen_source_release": self.first_seen_source_release,
            "kind": self.kind,
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _header_line() -> str:
    payload = {
        REGISTRY_HEADER_KEY: True,
        "registry_schema_version": REGISTRY_SCHEMA_VERSION,
        "description": (
            "Sporely taxonomy identity registry (append-only). "
            "Do not hand-edit; edits break the compatibility contract."
        ),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _allocation_from_payload(payload: dict, line_number: int) -> Allocation:
    try:
        kind = str(payload.get("kind", ENTRY_KIND_ANCHOR))
        if kind not in (ENTRY_KIND_ANCHOR, ENTRY_KIND_ALIAS):
            raise ValueError(f"unknown kind: {kind!r}")
        return Allocation(
            sporely_taxon_id=int(payload["sporely_taxon_id"]),
            source=str(payload["source"]),
            namespace=str(payload["namespace"]),
            identifier=str(payload["identifier"]),
            allocated_in_release=str(payload["allocated_in_release"]),
            first_seen_source_release=str(payload["first_seen_source_release"]),
            kind=kind,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RegistryError(
            f"registry line {line_number} is malformed: {exc}"
        ) from exc


class IdentityRegistry:
    """Persistent append-only registry loaded/flushed as one JSONL file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._by_key: dict[tuple[str, str, str], Allocation] = {}
        self._anchors: dict[int, Allocation] = {}
        self._aliases: list[Allocation] = []
        self._next_id = 1
        self._loaded = False

    # ----- I/O -----

    def load(self) -> None:
        self._by_key.clear()
        self._anchors.clear()
        self._aliases.clear()
        self._next_id = 1
        if not self.path.exists():
            self._loaded = True
            return
        # A shard directory is the canonical repository format; a single
        # ``.jsonl`` file is the dry-run / experiment format. Both stream
        # through the same line loader below.
        if self.path.is_dir():
            line_source: Iterable[bytes] = iter_shard_lines(self.path)
        else:
            line_source = self._iter_single_file_lines(self.path)
        self._consume_lines(line_source)
        self._loaded = True

    @staticmethod
    def _iter_single_file_lines(path: Path) -> Iterator[bytes]:
        with path.open("rb") as handle:
            for line in handle:
                yield line

    def _consume_lines(self, lines: Iterable[bytes]) -> None:
        for line_number, raw in enumerate(lines, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise RegistryError(
                    f"registry line {line_number} is not valid JSON: {exc}"
                ) from exc
            if not isinstance(payload, dict):
                raise RegistryError(
                    f"registry line {line_number} is not a JSON object"
                )
            if payload.get(REGISTRY_HEADER_KEY) is True:
                version = payload.get("registry_schema_version")
                if version != REGISTRY_SCHEMA_VERSION:
                    raise RegistryError(
                        f"registry schema version mismatch: {version!r}"
                    )
                continue
            entry = _allocation_from_payload(payload, line_number)
            if entry.key() in self._by_key:
                raise RegistryError(
                    f"registry line {line_number} duplicates key {entry.key()!r}"
                )
            self._by_key[entry.key()] = entry
            if entry.kind == ENTRY_KIND_ANCHOR:
                if entry.sporely_taxon_id in self._anchors:
                    raise RegistryError(
                        f"registry line {line_number} duplicates anchor "
                        f"sporely_taxon_id {entry.sporely_taxon_id!r}"
                    )
                self._anchors[entry.sporely_taxon_id] = entry
            else:
                if entry.sporely_taxon_id not in self._anchors:
                    raise RegistryError(
                        f"registry line {line_number} alias references "
                        f"unknown anchor sporely_taxon_id "
                        f"{entry.sporely_taxon_id!r}"
                    )
                self._aliases.append(entry)
            if entry.sporely_taxon_id >= self._next_id:
                self._next_id = entry.sporely_taxon_id + 1

    def flush(self) -> None:
        """Atomically rewrite the registry from the in-memory view.

        The output is byte-deterministic: a header line, then anchors ordered
        by ascending ``sporely_taxon_id``, then aliases ordered by
        ``(sporely_taxon_id, source, namespace, identifier)``.
        """
        if not self._loaded:
            raise RegistryError("registry.load() must precede flush()")
        if self.path.is_dir():
            raise RegistryError(
                f"flush() writes a single JSONL file; the canonical shard "
                f"directory {self.path!r} is a read-only promoted "
                f"representation. Use shard_registry(source, dest) to "
                f"regenerate it from an accepted single-file registry."
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = [_header_line()]
        for anchor in sorted(self._anchors.values(),
                             key=lambda a: a.sporely_taxon_id):
            lines.append(anchor.to_json_line())
        for alias in sorted(
            self._aliases,
            key=lambda a: (a.sporely_taxon_id, a.source, a.namespace, a.identifier),
        ):
            lines.append(alias.to_json_line())
        payload = "\n".join(lines) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp",
            dir=str(self.path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    # ----- lookups -----

    def lookup(self, source: str, namespace: str, identifier: str) -> Allocation | None:
        return self._by_key.get((source, namespace, identifier))

    def get_anchor(self, sporely_taxon_id: int) -> Allocation | None:
        return self._anchors.get(sporely_taxon_id)

    def anchor_count(self) -> int:
        return len(self._anchors)

    def alias_count(self) -> int:
        return len(self._aliases)

    def all_entries(self) -> Iterator[Allocation]:
        for anchor in sorted(self._anchors.values(),
                             key=lambda a: a.sporely_taxon_id):
            yield anchor
        for alias in sorted(
            self._aliases,
            key=lambda a: (a.sporely_taxon_id, a.source, a.namespace, a.identifier),
        ):
            yield alias

    # ----- mutation -----

    def allocate(
        self,
        *,
        source: str,
        namespace: str,
        identifier: str,
        allocated_in_release: str,
        first_seen_source_release: str,
    ) -> Allocation:
        """Return the existing entry for the key or allocate a new anchor.

        A ``(source, namespace, identifier)`` key that already resolves to an
        anchor OR an alias is returned unchanged. Otherwise a fresh anchor is
        allocated with the next available integer.
        """
        if not self._loaded:
            raise RegistryError("registry.load() must precede allocate()")
        if not (source and namespace and identifier):
            raise RegistryError(
                "allocate() requires non-empty source, namespace, identifier"
            )
        key = (source, namespace, identifier)
        existing = self._by_key.get(key)
        if existing is not None:
            return existing
        anchor = Allocation(
            sporely_taxon_id=self._next_id,
            source=source,
            namespace=namespace,
            identifier=identifier,
            allocated_in_release=allocated_in_release,
            first_seen_source_release=first_seen_source_release,
            kind=ENTRY_KIND_ANCHOR,
        )
        self._next_id += 1
        self._by_key[key] = anchor
        self._anchors[anchor.sporely_taxon_id] = anchor
        return anchor

    def bind_alias(
        self,
        *,
        existing_sporely_taxon_id: int,
        source: str,
        namespace: str,
        identifier: str,
        allocated_in_release: str,
        first_seen_source_release: str,
    ) -> Allocation:
        """Bind a new source key to an existing Sporely anchor.

        Used when a reviewed exact mapping declares that a second source usage
        shares identity with an already-allocated Sporely concept. Fails if
        the new key is already bound to a different Sporely ID.
        """
        if not self._loaded:
            raise RegistryError("registry.load() must precede bind_alias()")
        anchor = self._anchors.get(existing_sporely_taxon_id)
        if anchor is None:
            raise RegistryError(
                f"cannot bind alias to unknown sporely_taxon_id "
                f"{existing_sporely_taxon_id!r}"
            )
        key = (source, namespace, identifier)
        existing = self._by_key.get(key)
        if existing is not None:
            if existing.sporely_taxon_id != existing_sporely_taxon_id:
                raise RegistryError(
                    f"alias conflict: key {key!r} is already bound to "
                    f"sporely_taxon_id={existing.sporely_taxon_id}, cannot "
                    f"rebind to {existing_sporely_taxon_id}"
                )
            return existing
        alias = Allocation(
            sporely_taxon_id=existing_sporely_taxon_id,
            source=source,
            namespace=namespace,
            identifier=identifier,
            allocated_in_release=allocated_in_release,
            first_seen_source_release=first_seen_source_release,
            kind=ENTRY_KIND_ALIAS,
        )
        self._by_key[key] = alias
        self._aliases.append(alias)
        return alias


# --------------------------------------------------------- Shard support ---


def _iter_lines_with_newline(path: Path) -> Iterator[bytes]:
    """Yield each line of ``path`` as raw bytes INCLUDING its terminator.

    A file that ends without a final ``\\n`` yields its trailing partial line
    verbatim. This preserves byte-exact reproducibility.
    """
    with path.open("rb") as handle:
        buffer = bytearray()
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                if buffer:
                    yield bytes(buffer)
                return
            buffer.extend(chunk)
            while True:
                newline = buffer.find(b"\n")
                if newline < 0:
                    break
                yield bytes(buffer[: newline + 1])
                del buffer[: newline + 1]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def shard_registry(
    source_path: Path,
    dest_dir: Path,
    *,
    shard_bytes_target: int = SHARD_DEFAULT_TARGET_BYTES,
) -> dict:
    """Split a single-file JSONL registry into ordered, size-bounded shards.

    Semantics:

    * ``concat(part-0001.jsonl, part-0002.jsonl, …)`` reproduces the source
      bytes exactly (including any trailing newline).
    * No shard exceeds ``shard_bytes_target``. A single line larger than the
      target is written to its own shard rather than being truncated.
    * Line boundaries are respected — a shard never ends in the middle of
      a line.
    * ``manifest.json`` records per-shard byte size, line count, SHA-256; the
      total byte size, entry count, and concatenated SHA-256.
    """
    if shard_bytes_target <= 0:
        raise RegistryError("shard_bytes_target must be positive")
    if not source_path.exists():
        raise RegistryError(f"source registry not found: {source_path}")
    if dest_dir.exists():
        raise RegistryError(f"shard directory already exists: {dest_dir}")

    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".{dest_dir.name}.", suffix=".tmp",
        dir=str(dest_dir.parent),
    ))
    committed = False
    try:
        shards: list[dict] = []
        total_digest = hashlib.sha256()
        total_bytes = 0
        total_lines = 0
        shard_index = 0
        current_bytes = bytearray()
        current_lines = 0

        def _flush_current() -> None:
            nonlocal shard_index
            if not current_bytes:
                return
            shard_index += 1
            name = SHARD_FILENAME_PATTERN.format(index=shard_index)
            path = staging / name
            with path.open("wb") as handle:
                handle.write(bytes(current_bytes))
            shards.append({
                "name": name,
                "bytes": len(current_bytes),
                "line_count": current_lines,
                "sha256": hashlib.sha256(bytes(current_bytes)).hexdigest(),
            })

        for raw in _iter_lines_with_newline(source_path):
            # If adding this line would overflow AND we already have data,
            # flush the current shard first. A single oversize line goes
            # into its own shard.
            if current_bytes and \
                    len(current_bytes) + len(raw) > shard_bytes_target:
                _flush_current()
                current_bytes = bytearray()
                current_lines = 0
            current_bytes.extend(raw)
            current_lines += 1
            total_digest.update(raw)
            total_bytes += len(raw)
            total_lines += 1
        _flush_current()

        if not shards:
            # An empty source registry — still produce an empty shard so a
            # loader has something to stream. Preserves the invariant that
            # concat(shards) == source bytes.
            name = SHARD_FILENAME_PATTERN.format(index=1)
            (staging / name).write_bytes(b"")
            shards.append({"name": name, "bytes": 0,
                           "line_count": 0,
                           "sha256": hashlib.sha256(b"").hexdigest()})

        manifest = {
            "manifest_schema_version": SHARD_MANIFEST_SCHEMA_VERSION,
            "registry_schema_version": REGISTRY_SCHEMA_VERSION,
            "shard_bytes_target": shard_bytes_target,
            "shards": shards,
            "total_bytes": total_bytes,
            "total_line_count": total_lines,
            "concatenated_sha256": total_digest.hexdigest(),
        }
        (staging / SHARD_MANIFEST_FILENAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        os.replace(staging, dest_dir)
        committed = True
        return manifest
    finally:
        if not committed:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)


def load_shard_manifest(directory: Path) -> dict:
    """Validate a shard directory and return its manifest.

    Fails closed on: missing manifest, missing/extra files, mismatched
    per-shard bytes/line-count/SHA, mismatched concatenated SHA, or any
    ordering inconsistency between the manifest listing and the physical
    files.
    """
    if not directory.is_dir():
        raise RegistryError(f"shard directory not found: {directory}")
    manifest_path = directory / SHARD_MANIFEST_FILENAME
    if not manifest_path.exists():
        raise RegistryError(f"shard manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RegistryError(f"malformed shard manifest: {exc}") from exc
    if manifest.get("manifest_schema_version") != SHARD_MANIFEST_SCHEMA_VERSION:
        raise RegistryError(
            f"unsupported shard manifest schema: "
            f"{manifest.get('manifest_schema_version')!r}"
        )
    if manifest.get("registry_schema_version") != REGISTRY_SCHEMA_VERSION:
        raise RegistryError(
            f"unsupported registry schema in manifest: "
            f"{manifest.get('registry_schema_version')!r}"
        )
    listed = manifest.get("shards", [])
    if not isinstance(listed, list) or not listed:
        raise RegistryError("shard manifest has no shards")
    listed_names = [str(s["name"]) for s in listed]
    # Detect duplicates in the manifest order.
    if len(listed_names) != len(set(listed_names)):
        raise RegistryError("shard manifest lists duplicate shard names")
    # Reject any extra file in the directory.
    on_disk = {
        p.name for p in directory.iterdir()
        if p.is_file() and p.name != SHARD_MANIFEST_FILENAME
    }
    listed_set = set(listed_names)
    extras = on_disk - listed_set
    missing = listed_set - on_disk
    if extras:
        raise RegistryError(
            f"shard directory has extra files not in manifest: "
            f"{sorted(extras)!r}"
        )
    if missing:
        raise RegistryError(
            f"shard directory is missing declared shards: {sorted(missing)!r}"
        )
    total_digest = hashlib.sha256()
    running_bytes = 0
    running_lines = 0
    for entry in listed:
        name = str(entry["name"])
        expected_bytes = int(entry["bytes"])
        expected_sha = str(entry["sha256"])
        expected_lines = int(entry["line_count"])
        path = directory / name
        actual_bytes = path.stat().st_size
        if actual_bytes != expected_bytes:
            raise RegistryError(
                f"shard {name!r} byte size mismatch: "
                f"expected {expected_bytes}, got {actual_bytes}"
            )
        line_count = 0
        with path.open("rb") as handle:
            digest = hashlib.sha256()
            while True:
                chunk = handle.read(CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                total_digest.update(chunk)
                line_count += chunk.count(b"\n")
        actual_sha = digest.hexdigest()
        if actual_sha != expected_sha:
            raise RegistryError(
                f"shard {name!r} SHA-256 mismatch: "
                f"expected {expected_sha}, got {actual_sha}"
            )
        # Trailing partial line (no newline) still counts as a line.
        if actual_bytes > 0:
            with path.open("rb") as handle:
                handle.seek(-1, os.SEEK_END)
                tail = handle.read(1)
            if tail != b"\n":
                line_count += 1
        if line_count != expected_lines:
            raise RegistryError(
                f"shard {name!r} line count mismatch: "
                f"expected {expected_lines}, got {line_count}"
            )
        running_bytes += actual_bytes
        running_lines += line_count
    if running_bytes != int(manifest.get("total_bytes", -1)):
        raise RegistryError(
            f"concatenated byte size mismatch: expected "
            f"{manifest.get('total_bytes')}, got {running_bytes}"
        )
    if running_lines != int(manifest.get("total_line_count", -1)):
        raise RegistryError(
            f"concatenated line count mismatch: expected "
            f"{manifest.get('total_line_count')}, got {running_lines}"
        )
    if total_digest.hexdigest() != str(manifest.get("concatenated_sha256", "")):
        raise RegistryError(
            f"concatenated SHA-256 mismatch: expected "
            f"{manifest.get('concatenated_sha256')!r}, "
            f"got {total_digest.hexdigest()!r}"
        )
    return manifest


def iter_shard_lines(directory: Path) -> Iterator[bytes]:
    """Stream lines (as raw bytes with terminators) from a validated shard
    directory in manifest order."""
    manifest = load_shard_manifest(directory)
    for entry in manifest["shards"]:
        path = directory / str(entry["name"])
        yield from _iter_lines_with_newline(path)
