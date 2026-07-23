#!/usr/bin/env python3
"""Bounded event-stream validation for COL XR YAML metadata."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any, BinaryIO

import yaml
from yaml.events import (
    AliasEvent,
    DocumentEndEvent,
    DocumentStartEvent,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
    StreamEndEvent,
)

from refresh_col_xr import AcquisitionError


DEFAULT_METADATA_BYTES = 5 * 1024 * 1024
COL_XR_METADATA_BYTES = 256 * 1024 * 1024
YAML_POLICY_VERSION = 1
STANDARD_TAG_PREFIX = "tag:yaml.org,2002:"
CRITICAL_KEYS = {
    "key", "datasetkey", "doi", "title", "issued", "version", "license",
    "source", "type", "format",
}


@dataclass(frozen=True)
class YamlLimits:
    max_bytes: int
    max_depth: int = 64
    max_nodes: int = 25_000_000
    max_scalar_bytes: int = 2 * 1024 * 1024
    max_mapping_entries: int = 250_000
    max_sequence_entries: int = 250_000
    max_anchors: int = 10_000
    max_aliases: int = 1_000
    max_seconds: float = 600.0
    read_chunk_bytes: int = 64 * 1024


class BoundedBinaryReader:
    """Prevent a parser from issuing unbounded reads or crossing a byte ceiling."""

    def __init__(self, raw: BinaryIO, *, max_bytes: int, chunk_bytes: int):
        self.raw = raw
        self.max_bytes = max_bytes
        self.chunk_bytes = chunk_bytes
        self.bytes_read = 0
        self.read_calls = 0
        self.sha256 = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        requested = self.chunk_bytes if size is None or size < 0 else min(size, self.chunk_bytes)
        remaining = self.max_bytes + 1 - self.bytes_read
        if remaining <= 0:
            raise AcquisitionError("YAML stream exceeds its byte ceiling")
        data = self.raw.read(min(requested, remaining))
        self.read_calls += 1
        self.bytes_read += len(data)
        self.sha256.update(data)
        if self.bytes_read > self.max_bytes:
            raise AcquisitionError("YAML stream exceeds its byte ceiling")
        return data


def _check_tag(event: Any) -> None:
    tag = getattr(event, "tag", None)
    if tag is not None and not tag.startswith(STANDARD_TAG_PREFIX):
        raise AcquisitionError(f"unsafe or custom YAML tag is forbidden: {tag}")


def validate_yaml_events(
    raw: BinaryIO,
    *,
    limits: YamlLimits,
    expected_bytes: int | None = None,
    capture_source_ids: bool = False,
) -> dict[str, Any]:
    """Validate one complete YAML document without constructing its object graph."""

    reader = BoundedBinaryReader(
        raw, max_bytes=limits.max_bytes, chunk_bytes=limits.read_chunk_bytes
    )
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    started = time.monotonic()
    stack: list[dict[str, Any]] = []
    anchors: set[str] = set()
    aliases = nodes = events = documents = max_depth = 0
    stream_ended = document_ended = False
    root_type: str | None = None
    top_level_keys: list[str] = []
    root_scalars: dict[str, str] = {}
    source_count = 0
    source_ids: list[str] = []

    def progress() -> None:
        if time.monotonic() - started > limits.max_seconds:
            raise AcquisitionError("YAML parser duration limit exceeded")

    def add_node() -> None:
        nonlocal nodes
        nodes += 1
        if nodes > limits.max_nodes:
            raise AcquisitionError("YAML node limit exceeded")

    def value_path() -> tuple[str, ...]:
        if not stack:
            return ()
        parent = stack[-1]
        if parent["kind"] == "map":
            key = parent["pending_key"]
            if key is None:
                raise AcquisitionError("malformed YAML mapping value")
            parent["pending_key"] = None
            parent["expect_key"] = True
            parent["count"] += 1
            if parent["count"] > limits.max_mapping_entries:
                raise AcquisitionError("YAML mapping size limit exceeded")
            return parent["path"] + (key,)
        index = parent["count"]
        parent["count"] += 1
        if parent["count"] > limits.max_sequence_entries:
            raise AcquisitionError("YAML sequence size limit exceeded")
        return parent["path"] + (str(index),)

    try:
        iterator = yaml.parse(reader, Loader=loader)
        for event in iterator:
            events += 1
            if events % 10_000 == 0:
                progress()
            _check_tag(event)
            anchor = getattr(event, "anchor", None)
            if anchor and not isinstance(event, AliasEvent):
                if anchor in anchors:
                    raise AcquisitionError(f"duplicate YAML anchor is forbidden: {anchor}")
                anchors.add(anchor)
                if len(anchors) > limits.max_anchors:
                    raise AcquisitionError("YAML anchor limit exceeded")
            if isinstance(event, AliasEvent):
                aliases += 1
                add_node()
                if aliases > limits.max_aliases:
                    raise AcquisitionError("YAML alias limit exceeded")
                if event.anchor not in anchors:
                    raise AcquisitionError("YAML alias references an unknown anchor")
                value_path()
                continue
            if isinstance(event, DocumentStartEvent):
                documents += 1
                if documents > 1:
                    raise AcquisitionError("multiple YAML documents are forbidden")
                continue
            if isinstance(event, DocumentEndEvent):
                document_ended = True
                continue
            if isinstance(event, StreamEndEvent):
                stream_ended = True
                continue
            if isinstance(event, ScalarEvent):
                add_node()
                if len(event.value.encode("utf-8")) > limits.max_scalar_bytes:
                    raise AcquisitionError("YAML scalar length limit exceeded")
                if stack and stack[-1]["kind"] == "map" and stack[-1]["expect_key"]:
                    frame = stack[-1]
                    key = event.value
                    folded = key.casefold()
                    if folded in CRITICAL_KEYS and folded in frame["critical_keys"]:
                        raise AcquisitionError(f"duplicate critical YAML key: {key}")
                    if folded in CRITICAL_KEYS:
                        frame["critical_keys"].add(folded)
                    frame["pending_key"] = key
                    frame["expect_key"] = False
                    if len(stack) == 1:
                        top_level_keys.append(key)
                    continue
                path = value_path()
                if len(path) == 1:
                    root_scalars[path[0]] = event.value
                if capture_source_ids and len(path) == 3 and path[0] == "source":
                    if path[2].casefold() in {"key", "datasetkey", "id"}:
                        source_ids.append(event.value)
                continue
            if isinstance(event, (MappingStartEvent, SequenceStartEvent)):
                add_node()
                path = value_path() if stack else ()
                kind = "map" if isinstance(event, MappingStartEvent) else "seq"
                if root_type is None:
                    root_type = "mapping" if kind == "map" else "sequence"
                if path and path[0] == "source" and len(path) == 2:
                    source_count += 1
                stack.append({
                    "kind": kind,
                    "path": path,
                    "count": 0,
                    "expect_key": kind == "map",
                    "pending_key": None,
                    "critical_keys": set(),
                })
                max_depth = max(max_depth, len(stack))
                if max_depth > limits.max_depth:
                    raise AcquisitionError("YAML nesting depth limit exceeded")
                continue
            if isinstance(event, (MappingEndEvent, SequenceEndEvent)):
                if not stack:
                    raise AcquisitionError("malformed YAML collection termination")
                frame = stack.pop()
                expected = "map" if isinstance(event, MappingEndEvent) else "seq"
                if frame["kind"] != expected:
                    raise AcquisitionError("malformed YAML collection termination")
                if frame["kind"] == "map" and not frame["expect_key"]:
                    raise AcquisitionError("YAML mapping ends without a value")
                continue
    except yaml.YAMLError as exc:
        raise AcquisitionError(f"malformed YAML: {exc}") from exc
    progress()
    if stack or documents != 1 or not document_ended or not stream_ended:
        raise AcquisitionError("YAML stream lacks valid final-document termination")
    if expected_bytes is not None and reader.bytes_read != expected_bytes:
        raise AcquisitionError(
            f"YAML stream length mismatch: {reader.bytes_read} != {expected_bytes}"
        )
    return {
        "yaml_policy_version": YAML_POLICY_VERSION,
        "loader": loader.__name__,
        "bytes": reader.bytes_read,
        "sha256": reader.sha256.hexdigest(),
        "read_calls": reader.read_calls,
        "root_type": root_type,
        "top_level_keys": top_level_keys,
        "root_scalars": root_scalars,
        "source_reference_count": source_count,
        "source_identifiers": source_ids,
        "events": events,
        "nodes": nodes,
        "max_depth": max_depth,
        "anchors": len(anchors),
        "aliases": aliases,
        "complete_document": True,
        "duration_seconds": round(time.monotonic() - started, 3),
        "limits": {
            "max_bytes": limits.max_bytes,
            "max_depth": limits.max_depth,
            "max_nodes": limits.max_nodes,
            "max_scalar_bytes": limits.max_scalar_bytes,
            "max_mapping_entries": limits.max_mapping_entries,
            "max_sequence_entries": limits.max_sequence_entries,
            "max_anchors": limits.max_anchors,
            "max_aliases": limits.max_aliases,
            "max_seconds": limits.max_seconds,
            "read_chunk_bytes": limits.read_chunk_bytes,
        },
    }
