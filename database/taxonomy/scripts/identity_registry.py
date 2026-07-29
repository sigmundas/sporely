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

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


REGISTRY_SCHEMA_VERSION = 1
REGISTRY_HEADER_KEY = "__registry_header__"
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
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, raw in enumerate(handle, start=1):
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
        self._loaded = True

    def flush(self) -> None:
        """Atomically rewrite the registry from the in-memory view.

        The output is byte-deterministic: a header line, then anchors ordered
        by ascending ``sporely_taxon_id``, then aliases ordered by
        ``(sporely_taxon_id, source, namespace, identifier)``.
        """
        if not self._loaded:
            raise RegistryError("registry.load() must precede flush()")
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
