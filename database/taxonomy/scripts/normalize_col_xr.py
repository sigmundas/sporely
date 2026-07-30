#!/usr/bin/env python3
"""Normalize a pinned COL XR archive into shared-compiler input.

Streams ``NameUsage.tsv`` from ``archive.zip`` and emits a normalized
``taxa.jsonl`` in the same shape as ``national_source.normalize_archive``, so
the shared compiler can consume COL alongside any national DwC-A source.

Scope (implements the ``policies/scope.yml`` rules ``global_fungi`` +
``fungal_navigation_ancestors``):

* every ``col:kingdom == "Fungi"`` row with ``col:status`` in
  ``{"accepted", "provisionally accepted"}`` is included;
* every ancestor of an included row (walked transitively through
  ``col:parentID``) is included so that classification, matching, and
  homonym handling remain navigable. Ancestors are included regardless of
  their own kingdom or status column so the Fungi root ``F`` and its
  higher-taxon lineage (Eukaryota, Life, ...) become part of the
  normalized output rather than orphan parent references.

Boundaries:

* No extraction to disk. The archive member is streamed from the ZIP.
* No network access.
* Byte-safety limits mirror ``national_source`` (per-line and per-field).
* Output namespaces follow the taxonomy identity contract: COL usages use
  ``col_usage_id`` (see ``docs/identity-contract.md``).
* Reference validation matches ``national_source``: an unresolved
  non-empty ``parentID`` is preserved as a warning (never invented into a
  hierarchy edge), and an unresolved non-empty accepted-parent is a
  compilation blocker (COL treats synonyms via ``col:status``; the
  Stage 3A COL filter emits only ``accepted`` and ``provisionally
  accepted`` rows).
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, Iterator

MAX_LINE_BYTES = 1024 * 1024
MAX_FIELD_BYTES = 256 * 1024
CHUNK_BYTES = 512 * 1024

NAME_USAGE_MEMBER = "NameUsage.tsv"

REQUIRED_COLUMNS = (
    "ID", "parentID", "status", "scientificName", "authorship", "rank",
    "kingdom",
)
OPTIONAL_COLUMNS = (
    "genus", "specificEpithet", "family", "order", "class", "phylum",
    "infraspecificEpithet", "notho",
)
ACCEPTED_STATUSES = frozenset({"accepted", "provisionally accepted"})
SOURCE_CODE = "col_xr"
IDENTIFIER_NAMESPACE_PREFIX = "COL:"


class ColNormalizeError(Exception):
    """Raised on any COL normalization consistency or IO failure."""


def _strip_col_prefix(name: str) -> str:
    return name[4:] if name.startswith("col:") else name


def _archive_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iter_tsv_lines(handle: io.BufferedReader) -> Iterator[tuple[int, bytes]]:
    line_number = 0
    buffer = bytearray()
    while True:
        chunk = handle.read(CHUNK_BYTES)
        if not chunk:
            if buffer:
                line_number += 1
                yield line_number, bytes(buffer)
            return
        buffer.extend(chunk)
        while True:
            newline = buffer.find(b"\n")
            if newline < 0:
                break
            raw = bytes(buffer[:newline])
            del buffer[: newline + 1]
            if raw.endswith(b"\r"):
                raw = raw[:-1]
            if len(raw) > MAX_LINE_BYTES:
                raise ColNormalizeError(
                    f"NameUsage line {line_number + 1} exceeds "
                    f"MAX_LINE_BYTES={MAX_LINE_BYTES}"
                )
            line_number += 1
            yield line_number, raw


def _split_row(raw: bytes, expected_width: int, line_number: int) -> list[str]:
    fields = raw.split(b"\t")
    if any(len(f) > MAX_FIELD_BYTES for f in fields):
        raise ColNormalizeError(
            f"NameUsage line {line_number}: field exceeds MAX_FIELD_BYTES"
        )
    try:
        decoded = [f.decode("utf-8") for f in fields]
    except UnicodeDecodeError as exc:
        raise ColNormalizeError(
            f"NameUsage line {line_number}: not strict UTF-8: {exc}"
        ) from exc
    if len(decoded) != expected_width:
        raise ColNormalizeError(
            f"NameUsage line {line_number}: expected {expected_width} columns, "
            f"got {len(decoded)}"
        )
    return decoded


def _identifier_namespaces() -> dict[str, str]:
    return {
        "core_row_id": "col_usage_id",
        "taxon_id": "col_usage_id",
        "accepted_name_usage_id": "col_usage_id",
        "parent_name_usage_id": "col_usage_id",
    }


def _emit_record(
    *,
    row: list[str],
    columns: dict[str, int],
    source_release: dict[str, str],
    parent_resolution: str,
    inclusion_reason: str,
) -> dict:
    def value(name: str) -> str:
        idx = columns.get(name)
        if idx is None:
            return ""
        return row[idx].lstrip("﻿")

    core_row_id = value("ID")
    parent_id = value("parentID")
    return {
        "source_code": SOURCE_CODE,
        "source_release": source_release,
        "core_row_id": {
            "value": core_row_id,
            "namespace": "col_usage_id",
        },
        "taxon_id": {
            "value": core_row_id,
            "namespace": "col_usage_id",
        },
        "accepted_name_usage_id": None,
        "parent_name_usage_id": (
            {"value": parent_id, "namespace": "col_usage_id"} if parent_id else None
        ),
        "parent_reference_resolution": parent_resolution,
        "identifier_namespace": IDENTIFIER_NAMESPACE_PREFIX,
        "scientific_name": value("scientificName"),
        "authorship": value("authorship"),
        "rank": value("rank"),
        "taxonomic_status": value("status"),
        "external_ids": {},
        "provenance": {
            "source_code": SOURCE_CODE,
            "source_release": source_release,
            "identifier_namespace": IDENTIFIER_NAMESPACE_PREFIX,
            "member": NAME_USAGE_MEMBER,
        },
        "classification": {
            "kingdom": value("kingdom"),
            "phylum": value("phylum"),
            "class": value("class"),
            "order": value("order"),
            "family": value("family"),
            "genus": value("genus"),
            "specific_epithet": value("specificEpithet"),
            "infraspecific_epithet": value("infraspecificEpithet"),
        },
        "col_inclusion_reason": inclusion_reason,
    }


def normalize_col_xr(
    *,
    archive_path: Path,
    output_dir: Path,
    source_release: dict[str, str],
    kingdom_filter: str = "Fungi",
    row_limit: int | None = None,
) -> dict:
    """Two-pass streaming normalize of the pinned COL XR archive.

    Pass 1 streams every NameUsage row and captures ``parent_of[ID]`` for the
    entire archive so the ancestor closure can be resolved without a third
    pass. Pass 2 rewrites the normalized JSONL for the (Fungi ∪ ancestors)
    set. Streaming avoids loading the ~2.9 GiB member as one string.

    ``row_limit`` bounds the number of matching-kingdom rows written (used by
    the dry-run driver to keep output disk usage predictable during
    development).
    """
    if output_dir.exists() or output_dir.is_symlink():
        raise ColNormalizeError(f"output directory already exists: {output_dir}")
    if not archive_path.exists():
        raise ColNormalizeError(f"archive not found: {archive_path}")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".{output_dir.name}.", suffix=".tmp",
        dir=str(output_dir.parent),
    ))
    committed = False
    try:
        report = _normalize_into(
            archive_path=archive_path,
            staging=staging,
            source_release=source_release,
            kingdom_filter=kingdom_filter,
            row_limit=row_limit,
        )
        os.replace(staging, output_dir)
        committed = True
        return report
    finally:
        if not committed:
            shutil.rmtree(staging, ignore_errors=True)


def _parse_header(archive: zipfile.ZipFile) -> tuple[dict[str, int], int]:
    with archive.open(NAME_USAGE_MEMBER, "r") as raw:
        iterator = _iter_tsv_lines(raw)
        try:
            _, header_bytes = next(iterator)
        except StopIteration as exc:
            raise ColNormalizeError("NameUsage is empty") from exc
        header_tokens = header_bytes.decode("utf-8").split("\t")
        if header_tokens and header_tokens[0].startswith("﻿"):
            header_tokens[0] = header_tokens[0][1:]
        columns = {_strip_col_prefix(name): index
                   for index, name in enumerate(header_tokens)}
        for required in REQUIRED_COLUMNS:
            if required not in columns:
                raise ColNormalizeError(
                    f"NameUsage header lacks required column {required!r}"
                )
        return columns, len(header_tokens)


def _normalize_into(
    *, archive_path: Path, staging: Path,
    source_release: dict[str, str], kingdom_filter: str, row_limit: int | None,
) -> dict:
    taxa_out = staging / "taxa.jsonl"
    report_out = staging / "report.json"

    with zipfile.ZipFile(archive_path, "r") as archive:
        if NAME_USAGE_MEMBER not in archive.namelist():
            raise ColNormalizeError(
                f"archive lacks {NAME_USAGE_MEMBER}: {archive_path}"
            )
        columns, header_width = _parse_header(archive)
        id_index = columns["ID"]
        parent_index = columns["parentID"]
        kingdom_index = columns["kingdom"]
        status_index = columns["status"]

        # ------ Pass 1: build parent_of for every row + initial target set ---
        parent_of: dict[str, str] = {}
        initial_targets: set[str] = set()
        kingdom_bytes = kingdom_filter.encode("utf-8")
        with archive.open(NAME_USAGE_MEMBER, "r") as raw:
            iterator = _iter_tsv_lines(raw)
            _ = next(iterator)  # header
            initial_added = 0
            for line_number, raw_line in iterator:
                row = _split_row(raw_line, header_width, line_number)
                row_id = row[id_index]
                if not row_id:
                    raise ColNormalizeError(
                        f"NameUsage line {line_number}: empty ID"
                    )
                if row_id in parent_of:
                    raise ColNormalizeError(
                        f"NameUsage line {line_number}: duplicate ID {row_id!r}"
                    )
                parent_of[row_id] = row[parent_index]
                # Fast filter on raw_line before per-column check
                if kingdom_bytes in raw_line and \
                        row[kingdom_index] == kingdom_filter and \
                        row[status_index] in ACCEPTED_STATUSES:
                    if row_limit is not None and initial_added >= row_limit:
                        continue
                    initial_targets.add(row_id)
                    initial_added += 1

        # ------ Ancestor closure ---------------------------------------------
        # Walk transitively via parent_of. An empty parent means "root of the
        # full COL tree" — closure stops there. A missing parent (dangling
        # reference to an ID that was not observed in the archive) is
        # preserved as a warning in pass 2 rather than being invented.
        target_ids: set[str] = set(initial_targets)
        ancestors: set[str] = set()
        frontier = list(initial_targets)
        while frontier:
            current = frontier.pop()
            parent = parent_of.get(current, "")
            if not parent:
                continue
            if parent in target_ids or parent in ancestors:
                continue
            if parent not in parent_of:
                # Dangling parent — kept as a warning in pass 2.
                continue
            ancestors.add(parent)
            target_ids.add(parent)
            frontier.append(parent)

        # ------ Pass 2: emit normalized JSONL for target set ------------------
        orphan_parents: set[tuple[str, str]] = set()
        rows_written = 0
        ancestor_rows_written = 0
        with archive.open(NAME_USAGE_MEMBER, "r") as raw:
            iterator = _iter_tsv_lines(raw)
            _ = next(iterator)
            with taxa_out.open("w", encoding="utf-8") as handle:
                for line_number, raw_line in iterator:
                    # We cannot cheap-prefilter by kingdom_bytes here because
                    # ancestor rows do NOT contain the "Fungi" kingdom token
                    # (e.g. the ancestor "Eukaryota" row). Cost is one full
                    # scan of NameUsage.tsv, same as pass 1.
                    row = _split_row(raw_line, header_width, line_number)
                    row_id = row[id_index]
                    if row_id not in target_ids:
                        continue
                    is_ancestor = row_id in ancestors
                    inclusion_reason = "ancestor" if is_ancestor else "fungi"
                    parent = row[parent_index]
                    if not parent:
                        resolution = "absent"
                    elif parent in target_ids:
                        resolution = "resolved"
                    else:
                        resolution = "unresolved"
                        orphan_parents.add((row_id, parent))
                    record = _emit_record(
                        row=row, columns=columns,
                        source_release=source_release,
                        parent_resolution=resolution,
                        inclusion_reason=inclusion_reason,
                    )
                    handle.write(
                        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                    )
                    rows_written += 1
                    if is_ancestor:
                        ancestor_rows_written += 1

    MAX_SAMPLES = 25
    unresolved_samples = [
        {"source_taxon_id": s, "raw_reference": r}
        for s, r in sorted(orphan_parents)[:MAX_SAMPLES]
    ]
    report = {
        "result": "passed",
        "profile_source_code": SOURCE_CODE,
        "profile_source_release": source_release,
        "record_counts": {
            "Taxon": rows_written,
            "TaxonAncestor": ancestor_rows_written,
            "TaxonFungi": rows_written - ancestor_rows_written,
        },
        "outputs": {"taxa": taxa_out.name},
        "distribution_imported": False,
        "identifier_namespaces": _identifier_namespaces(),
        "archive_sha256": _archive_sha256(archive_path),
        "reference_gaps": {
            "orphan_parent_reference_count": len(orphan_parents),
            "orphan_accepted_reference_count": 0,
            "orphan_parent_reference_samples": unresolved_samples,
            "orphan_accepted_reference_samples": [],
            "sample_bound": MAX_SAMPLES,
        },
        "hierarchy_complete": len(orphan_parents) == 0,
        "compiler_ready": True,
        "kingdom_filter": kingdom_filter,
        "row_limit": row_limit,
        "scope_rule": "global_fungi + fungal_navigation_ancestors",
    }
    report_out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-release-version", required=True)
    parser.add_argument("--source-release-issued-date", required=True)
    parser.add_argument("--kingdom", default="Fungi")
    parser.add_argument("--row-limit", type=int, default=None,
                        help="bound the number of matching Fungi rows (dry-run aid)")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        report = normalize_col_xr(
            archive_path=args.archive,
            output_dir=args.output,
            source_release={
                "version": args.source_release_version,
                "issued_date": args.source_release_issued_date,
            },
            kingdom_filter=args.kingdom,
            row_limit=args.row_limit,
        )
    except ColNormalizeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
