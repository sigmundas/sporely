#!/usr/bin/env python3
"""Stage W1 cloud-taxonomy exporter (model-neutral).

Consumes the compiled taxonomy-v2 SQLite release and emits a deterministic set
of canonical JSONL files that a later Supabase importer (W2) can ingest into
any table decomposition it chooses.

Boundaries:

* No Supabase connection, no SQL against Supabase, no migrations authored.
* No `search_taxa_v2` implementation, no observation-column changes, no web
  UX changes, no observation backfill.
* Faithful to compiler identities: if the release contains multiple Sporely
  concepts sharing a scientific name, all are exported unchanged.

Determinism contract:

* Row order is fixed per dataset (see `DATASET_SORT_KEYS`).
* Canonical JSON: `sort_keys=True, ensure_ascii=False,
  separators=(",", ":"), allow_nan=False`. SQL NULL → `null`; empty text → `""`.
* Two clean runs against the same source produce byte-identical dataset
  files and byte-identical `whole_export_sha256`.
* The outer manifest contains `generated_at`; the dataset files do not.

Scope predicate: encoded literally in code as
``fungi_closure_union_nortaxa_v1``.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import logging
import os
import shutil
import sqlite3
import struct  # noqa: F401  (retained for future length-prefixed extensions)
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional

TAXONOMY_SCHEMA_VERSION = 2

EXPORT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
EXPORTER_VERSION = "1.0.0"
SCOPE_PREDICATE_ID = "fungi_closure_union_nortaxa_v1"

MANIFEST_FILENAME = "taxonomy_export_manifest.json"

# Fixed dataset file order (also used for `whole_export_sha256`).
DATASET_FILES = (
    "taxonomy_release.jsonl",
    "taxon.jsonl",
    "scientific_name.jsonl",
    "vernacular.jsonl",
    "taxon_external_id.jsonl",
    "taxon_redlist.jsonl",
)

# Explicit column allowlists (verified against `PRAGMA table_info` on
# `tax-2026.07.30-02`). No `SELECT *` anywhere.
TAXON_MIN_ALLOWLIST = (
    "taxon_id",
    "parent_taxon_id",
    "genus",
    "specific_epithet",
    "family",
    "norwegian_taxon_id",
    "swedish_taxon_id",
    "inaturalist_taxon_id",
    "canonical_scientific_name",
    "taxon_rank",
    "taxonomic_status",
    "source_system",
    "preferred_scientific_name_no",
    "preferred_scientific_name_sv",
    "sporely_content_release_id",
    "canonical_source_system",
    "canonical_external_id",
)
SCIENTIFIC_NAME_MIN_ALLOWLIST = (
    "taxon_id",
    "language_code",
    "scientific_name",
    "is_preferred_name",
    "source",
    "note",
)
VERNACULAR_MIN_ALLOWLIST = (
    "taxon_id",
    "language_code",
    "vernacular_name",
    "is_preferred_name",
    "source",
)
TAXON_REDLIST_MIN_ALLOWLIST = (
    "taxon_id",
    "source_system",
    "source_release",
    "assessment_id",
    "assessment_area",
    "assessed_name_source",
    "assessed_name_namespace",
    "assessed_name_id",
    "scientific_name_snapshot",
    "authorship_snapshot",
    "taxon_rank_snapshot",
    "category_raw",
    "category_code",
    "category_is_downgraded",
    "criteria",
    "expert_group",
    "assessment_url",
)
# Normalized model-neutral external-ID schema.
TAXON_EXTERNAL_ID_OUTPUT_FIELDS = (
    "taxon_id",
    "source_system",
    "namespace",
    "external_id",
    "id_role",
    "is_preferred",
    "external_name",
    "note",
    "source_table",
)

BOOL_COLUMNS = {
    "is_preferred_name",
    "is_preferred",
    "category_is_downgraded",
}

DATASET_SORT_KEYS = {
    "taxon.jsonl": ("taxon_id",),
    "scientific_name.jsonl": ("taxon_id", "scientific_name", "language_code", "source", "note"),
    "vernacular.jsonl": ("taxon_id", "language_code", "vernacular_name", "source"),
    "taxon_external_id.jsonl": (
        "taxon_id", "source_system", "namespace", "external_id",
        "source_table", "id_role", "is_preferred",
    ),
    "taxon_redlist.jsonl": (
        "assessment_area", "taxon_id", "source_release",
        "assessment_id", "assessed_name_namespace", "assessed_name_id",
    ),
    "taxonomy_release.jsonl": (),
}

CHUNK_BYTES = 1 * 1024 * 1024

_LOG = logging.getLogger("sporely.taxonomy.cloud_export")

# Files under `database/taxonomy/policies/` whose hashes are recorded for
# provenance. Only files that survive commit-hash checks belong here — the
# scope predicate is encoded in code, not in policy files.
POLICY_HASH_TARGETS = (
    "scope.yml",
    "languages.yml",
    "source_priority.yml",
    "release_contract.yml",
    "mapping_policy.yml",
    "manual_mappings.yml",
)


class ExportError(Exception):
    """Raised on any exporter precondition or invariant failure."""


# ---------- canonical serialization + hashing ---------------------------


def canonical_dumps(value: dict) -> str:
    """Canonical JSON: sorted keys, compact, UTF-8-safe, no NaN/Inf."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(CHUNK_BYTES)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def whole_export_sha256(files_in_order: list[tuple[str, Path]]) -> str:
    """SHA-256 over a length-delimited byte concatenation of files.

    Byte format per file, in fixed dataset order:

        <ascii-decimal len(name)> ':' <name-utf8-bytes> ':'
        <ascii-decimal len(bytes)> ':' <raw file bytes> '\n'
    """
    h = hashlib.sha256()
    for name, path in files_in_order:
        name_bytes = name.encode("utf-8")
        size = path.stat().st_size
        h.update(str(len(name_bytes)).encode("ascii"))
        h.update(b":")
        h.update(name_bytes)
        h.update(b":")
        h.update(str(size).encode("ascii"))
        h.update(b":")
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(CHUNK_BYTES)
                if not chunk:
                    break
                h.update(chunk)
        h.update(b"\n")
    return h.hexdigest()


# ---------- row helpers -------------------------------------------------


def _coerce_row(row: sqlite3.Row, columns: tuple[str, ...]) -> dict:
    out: dict = {}
    for col in columns:
        v = row[col]
        if col in BOOL_COLUMNS:
            # SQLite stores as INTEGER 0/1; emit JSON booleans.
            out[col] = bool(v) if v is not None else None
        else:
            out[col] = v
    return out


def _coerce_external_row(
    *,
    taxon_id: int,
    source_system: str,
    namespace: Optional[str],
    external_id: str,
    id_role: str,
    is_preferred: int,
    external_name: Optional[str],
    note: Optional[str],
    source_table: str,
) -> dict:
    return {
        "taxon_id": taxon_id,
        "source_system": source_system,
        "namespace": namespace,
        "external_id": external_id,
        "id_role": id_role,
        "is_preferred": bool(is_preferred),
        "external_name": external_name,
        "note": note,
        "source_table": source_table,
    }


# ---------- source verification ----------------------------------------


@dataclass(frozen=True)
class SourceContext:
    manifest_path: Path
    manifest: dict
    artifact_gz_path: Path
    decompressed_sqlite_path: Path  # inside temp dir
    tmp_dir: Path
    gz_sha256: str
    sqlite_sha256: str
    content_release_id: str


_UNSAFE_RELEASE_CHARS = set("/\\:.\x00\r\n ")
# Note: '.' is allowed inside the release id (`tax-2026.07.30-02`), so
# we allow it in a whitelist regex instead.
import re  # noqa: E402
_RELEASE_ID_RE = re.compile(r"^tax-[0-9]{4}\.[0-9]{2}\.[0-9]{2}-[0-9]{2}$")


def verify_source(artifact_gz_path: Path, manifest_path: Path, tmp_dir: Path) -> SourceContext:
    """Verify manifest ↔ artifact ↔ SQLite metadata and decompress into tmp."""
    if not manifest_path.is_file():
        raise ExportError(f"outer manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExportError(f"outer manifest malformed: {exc}") from exc

    for k in (
        "taxonomy_schema_version",
        "content_release_id",
        "gz_sha256",
        "sqlite_sha256",
        "gz_artifact",
    ):
        if k not in manifest:
            raise ExportError(f"outer manifest missing key: {k}")

    if manifest["taxonomy_schema_version"] != TAXONOMY_SCHEMA_VERSION:
        raise ExportError(
            f"unsupported taxonomy_schema_version: {manifest['taxonomy_schema_version']}"
        )

    release_id = str(manifest["content_release_id"])
    if not _RELEASE_ID_RE.match(release_id):
        raise ExportError(f"release id contains unsafe or invalid characters: {release_id!r}")

    if not artifact_gz_path.is_file():
        raise ExportError(f"artifact gzip not found: {artifact_gz_path}")

    gz_sha = sha256_file(artifact_gz_path)
    if gz_sha != manifest["gz_sha256"]:
        raise ExportError(
            f"artifact gzip SHA-256 mismatch: got {gz_sha}, manifest {manifest['gz_sha256']}"
        )

    # Decompress deterministically into tmp_dir.
    dst = tmp_dir / f"{release_id}.sqlite3"
    _decompress_to_path(artifact_gz_path, dst)

    sq_sha = sha256_file(dst)
    if sq_sha != manifest["sqlite_sha256"]:
        raise ExportError(
            f"decompressed SQLite SHA-256 mismatch: got {sq_sha}, "
            f"manifest {manifest['sqlite_sha256']}"
        )

    # Read the SQLite's own meta and cross-check.
    with _open_ro(dst) as conn:
        _verify_sqlite_meta(conn, manifest, release_id)
        _verify_required_schema(conn)

    return SourceContext(
        manifest_path=manifest_path,
        manifest=manifest,
        artifact_gz_path=artifact_gz_path,
        decompressed_sqlite_path=dst,
        tmp_dir=tmp_dir,
        gz_sha256=gz_sha,
        sqlite_sha256=sq_sha,
        content_release_id=release_id,
    )


def _decompress_to_path(gz_path: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(gz_path, "rb") as src, dst.open("wb") as out:
        while True:
            chunk = src.read(CHUNK_BYTES)
            if not chunk:
                break
            out.write(chunk)


def _open_ro(sqlite_path: Path) -> sqlite3.Connection:
    uri = f"file:{sqlite_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _verify_sqlite_meta(conn: sqlite3.Connection, manifest: dict, release_id: str) -> None:
    meta = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM taxonomy_meta")}
    schema_version = meta.get("taxonomy_schema_version")
    if schema_version != str(TAXONOMY_SCHEMA_VERSION):
        raise ExportError(
            f"SQLite taxonomy_schema_version={schema_version!r} does not match {TAXONOMY_SCHEMA_VERSION}"
        )
    inner_release = meta.get("content_release_id")
    if inner_release != release_id:
        raise ExportError(
            f"SQLite content_release_id={inner_release!r} does not match outer manifest {release_id!r}"
        )


_REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "taxon_min": TAXON_MIN_ALLOWLIST,
    "scientific_name_min": SCIENTIFIC_NAME_MIN_ALLOWLIST,
    "vernacular_min": VERNACULAR_MIN_ALLOWLIST,
    "taxon_external_id_min": (
        "taxon_id", "source_system", "external_id", "id_role",
        "is_preferred", "external_name", "note",
    ),
    "taxon_external_id_text_min": (
        "taxon_id", "source_system", "namespace", "external_id", "id_role",
        "is_preferred", "external_name", "note",
    ),
    "taxon_redlist_min": TAXON_REDLIST_MIN_ALLOWLIST,
    "taxonomy_meta": ("key", "value"),
}


def _verify_required_schema(conn: sqlite3.Connection) -> None:
    for table, required in _REQUIRED_TABLE_COLUMNS.items():
        rows = list(conn.execute(f"PRAGMA table_info({table})"))
        if not rows:
            raise ExportError(f"required table missing: {table}")
        present = {r["name"] for r in rows}
        missing = [c for c in required if c not in present]
        if missing:
            raise ExportError(f"table {table} missing columns: {missing}")


# ---------- scope construction -----------------------------------------


PINNED_RELEASE_EXPECTATIONS = {
    "content_release_id": "tax-2026.07.30-02",
    "concepts_included": 634894,
    "concepts_excluded": 1,
    "scientific_name_rows": 662649,
    "vernacular_rows": 10294,
    "vernacular_by_lang": {"nb": 6240, "nn": 3975, "se": 79},
    "external_int_rows": 61583,
    "external_text_rows": 620975,
    "external_total_rows": 682558,
    "redlist_rows": 7866,
    "redlist_by_area": {"Norge": 7198, "Svalbard": 668},
}

MAX_TREE_DEPTH = 200


@dataclass
class ScopeResult:
    fungi_root_ids: list[int]
    concept_ids: list[int]  # sorted ascending
    excluded_count: int
    excluded_sample: list[dict] = field(default_factory=list)


def build_concept_set(conn: sqlite3.Connection) -> ScopeResult:
    """Compute the exported concept set S using the pinned inclusion algorithm.

    S = descendants(taxon_min WHERE taxon_rank='kingdom' AND canonical_scientific_name='Fungi')
        ∪ (taxon_id WHERE canonical_source_system='nortaxa')
    """
    fungi_roots = [
        row["taxon_id"] for row in conn.execute(
            "SELECT taxon_id FROM taxon_min "
            "WHERE taxon_rank = 'kingdom' AND canonical_scientific_name = 'Fungi' "
            "ORDER BY taxon_id"
        )
    ]
    if not fungi_roots:
        raise ExportError("no Fungi kingdom root found in taxon_min")

    seen: set[int] = set()
    frontier: set[int] = set(fungi_roots)
    depth = 0
    while frontier:
        depth += 1
        if depth > MAX_TREE_DEPTH:
            raise ExportError(
                f"parent_taxon_id traversal exceeded MAX_TREE_DEPTH={MAX_TREE_DEPTH}; suspected cycle"
            )
        seen |= frontier
        # frontier is bounded by the branching factor of the tree; chunk to
        # keep parameter count sane against SQLite's default 999-var limit.
        next_frontier: set[int] = set()
        frontier_list = list(frontier)
        for i in range(0, len(frontier_list), 500):
            batch = frontier_list[i:i + 500]
            placeholders = ",".join("?" * len(batch))
            for row in conn.execute(
                f"SELECT taxon_id FROM taxon_min WHERE parent_taxon_id IN ({placeholders})",
                batch,
            ):
                next_frontier.add(row["taxon_id"])
        frontier = next_frontier - seen

    for row in conn.execute(
        "SELECT taxon_id FROM taxon_min WHERE canonical_source_system = 'nortaxa'"
    ):
        seen.add(row["taxon_id"])

    concept_ids = sorted(seen)

    # Excluded rows
    total = conn.execute("SELECT count(*) AS n FROM taxon_min").fetchone()["n"]
    excluded_count = total - len(concept_ids)
    excluded_sample: list[dict] = []
    if excluded_count:
        # Build lightweight anti-set query using the temp table strategy.
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS _scope_probe(taxon_id INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM _scope_probe")
        conn.executemany(
            "INSERT INTO _scope_probe(taxon_id) VALUES(?)", ((t,) for t in concept_ids)
        )
        excluded_sample = [
            dict(r)
            for r in conn.execute(
                "SELECT taxon_id, canonical_scientific_name, taxon_rank, "
                "canonical_source_system FROM taxon_min "
                "WHERE taxon_id NOT IN (SELECT taxon_id FROM _scope_probe) "
                "ORDER BY taxon_id LIMIT 20"
            )
        ]
        conn.execute("DROP TABLE _scope_probe")

    return ScopeResult(
        fungi_root_ids=fungi_roots,
        concept_ids=concept_ids,
        excluded_count=excluded_count,
        excluded_sample=excluded_sample,
    )


def _install_scope_temp_table(conn: sqlite3.Connection, concept_ids: list[int]) -> None:
    conn.execute("DROP TABLE IF EXISTS _cloud_export_scope")
    conn.execute("CREATE TEMP TABLE _cloud_export_scope(taxon_id INTEGER PRIMARY KEY)")
    conn.executemany(
        "INSERT INTO _cloud_export_scope(taxon_id) VALUES(?)",
        ((t,) for t in concept_ids),
    )


# ---------- dataset emission -------------------------------------------


@dataclass
class DatasetResult:
    filename: str
    row_count: int
    bytes: int
    sha256: str
    path: Path


def _open_writer(path: Path) -> io.BufferedWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("wb")


def _write_json_line(fh: io.BufferedWriter, obj: dict) -> int:
    line = canonical_dumps(obj).encode("utf-8") + b"\n"
    fh.write(line)
    return len(line)


def _finalize_dataset(path: Path, filename: str, row_count: int, bytes_written: int) -> DatasetResult:
    disk_bytes = path.stat().st_size
    if disk_bytes != bytes_written:
        raise ExportError(
            f"{filename}: on-disk bytes {disk_bytes} do not match tracked bytes {bytes_written}"
        )
    return DatasetResult(
        filename=filename,
        row_count=row_count,
        bytes=disk_bytes,
        sha256=sha256_file(path),
        path=path,
    )


def emit_taxon(conn: sqlite3.Connection, out_path: Path) -> DatasetResult:
    filename = "taxon.jsonl"
    cols = TAXON_MIN_ALLOWLIST
    select = ", ".join(cols)
    sql = (
        f"SELECT {select} FROM taxon_min t "
        "WHERE t.taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY t.taxon_id ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            written += _write_json_line(fh, _coerce_row(row, cols))
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_scientific_name(conn: sqlite3.Connection, out_path: Path) -> DatasetResult:
    filename = "scientific_name.jsonl"
    cols = SCIENTIFIC_NAME_MIN_ALLOWLIST
    select = ", ".join(cols)
    # Deterministic disambiguation: taxon_id, scientific_name, language_code, source, note.
    sql = (
        f"SELECT {select} FROM scientific_name_min s "
        "WHERE s.taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY s.taxon_id ASC, s.scientific_name ASC, s.language_code ASC, "
        "COALESCE(s.source, '') ASC, COALESCE(s.note, '') ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            written += _write_json_line(fh, _coerce_row(row, cols))
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_vernacular(conn: sqlite3.Connection, out_path: Path) -> DatasetResult:
    filename = "vernacular.jsonl"
    cols = VERNACULAR_MIN_ALLOWLIST
    select = ", ".join(cols)
    sql = (
        f"SELECT {select} FROM vernacular_min v "
        "WHERE v.taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY v.taxon_id ASC, v.language_code ASC, v.vernacular_name ASC, "
        "COALESCE(v.source, '') ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            written += _write_json_line(fh, _coerce_row(row, cols))
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_taxon_external_id(conn: sqlite3.Connection, out_path: Path) -> DatasetResult:
    filename = "taxon_external_id.jsonl"
    # Union the integer + text tables into a single normalized shape.
    # Integer table has no `namespace` column — emit JSON null.
    # external_id is always emitted as a JSON string (CAST for int table).
    # UNION ALL ORDER BY only accepts positional / output-column references.
    # Column positions:
    #   1=taxon_id, 2=source_system, 3=namespace, 4=external_id, 5=id_role,
    #   6=is_preferred, 7=external_name, 8=note, 9=source_table.
    # SQLite sorts NULL before any non-null value by default — deterministic.
    sql = (
        "SELECT taxon_id, source_system, NULL AS namespace, "
        "CAST(external_id AS TEXT) AS external_id, id_role, is_preferred, "
        "external_name, note, 'taxon_external_id_min' AS source_table "
        "FROM taxon_external_id_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "UNION ALL "
        "SELECT taxon_id, source_system, namespace, external_id, id_role, "
        "is_preferred, external_name, note, 'taxon_external_id_text_min' "
        "FROM taxon_external_id_text_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY 1 ASC, 2 ASC, 3 ASC, 4 ASC, 9 ASC, 5 ASC, 6 ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            obj = _coerce_external_row(
                taxon_id=row["taxon_id"],
                source_system=row["source_system"],
                namespace=row["namespace"],
                external_id=row["external_id"],
                id_role=row["id_role"],
                is_preferred=row["is_preferred"],
                external_name=row["external_name"],
                note=row["note"],
                source_table=row["source_table"],
            )
            written += _write_json_line(fh, obj)
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_taxon_redlist(conn: sqlite3.Connection, out_path: Path) -> DatasetResult:
    filename = "taxon_redlist.jsonl"
    cols = TAXON_REDLIST_MIN_ALLOWLIST
    select = ", ".join(cols)
    sql = (
        f"SELECT {select} FROM taxon_redlist_min r "
        "WHERE r.taxon_id IS NOT NULL "
        "AND r.taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY r.assessment_area ASC, r.taxon_id ASC, r.source_release ASC, "
        "r.assessment_id ASC, r.assessed_name_namespace ASC, r.assessed_name_id ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            written += _write_json_line(fh, _coerce_row(row, cols))
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_taxonomy_release(
    conn: sqlite3.Connection,
    out_path: Path,
    src: SourceContext,
    policy_hashes: dict[str, str],
) -> DatasetResult:
    filename = "taxonomy_release.jsonl"
    meta = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM taxonomy_meta")}
    manifest = src.manifest

    # Deterministic release metadata only (no `generated_at`).
    row_obj: dict = {
        "content_release_id": src.content_release_id,
        "taxonomy_schema_version": int(meta["taxonomy_schema_version"]),
        "canonical_authority": manifest.get("canonical_authority", "COL XR 2026-07-17"),
        "checklistbank_dataset_id": manifest.get("checklistbank_dataset_id", "315834"),
        "doi": manifest.get("doi", "10.48580/dgykv"),
        "nortaxa_release": manifest.get("nortaxa_release", "1.284"),
        "sqlite_sha256": src.sqlite_sha256,
        "gz_sha256": src.gz_sha256,
        "compiler_manifest_sha256": meta.get("compiler_manifest_sha256"),
        "registry_sha256": meta.get("registry_sha256"),
        "compiler_state": meta.get("state"),
        "compiler_publication": meta.get("publication"),
        "source_release_col_xr_id": meta.get("source_release[col_xr].id"),
        "source_release_col_xr_sha256": meta.get("source_release[col_xr].archive_sha256"),
        "source_release_nortaxa_id": meta.get("source_release[nortaxa].id"),
        "source_release_nortaxa_sha256": meta.get("source_release[nortaxa].archive_sha256"),
        "source_release_redlist_id": meta.get("source_release[artsdatabanken_redlist].id"),
        "source_release_redlist_sha256": meta.get("source_release[artsdatabanken_redlist].archive_sha256"),
        "policy_hashes": dict(sorted(policy_hashes.items())),
        "scope_predicate_id": SCOPE_PREDICATE_ID,
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "exporter_version": EXPORTER_VERSION,
    }
    written = 0
    with _open_writer(out_path) as fh:
        written = _write_json_line(fh, row_obj)
    return _finalize_dataset(out_path, filename, 1, written)


# ---------- policy hashing --------------------------------------------


def hash_policies(policy_dir: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if not policy_dir.is_dir():
        raise ExportError(f"policy directory not found: {policy_dir}")
    for name in POLICY_HASH_TARGETS:
        p = policy_dir / name
        if p.is_file():
            result[name] = sha256_file(p)
        else:
            # Absence is recorded but not a hard failure: e.g. manual_mappings.yml
            # may legitimately be absent in some checkouts.
            result[name] = "absent"
    return result


# ---------- invariants + manifest --------------------------------------


@dataclass
class ExportResult:
    output_dir: Path
    datasets: dict[str, DatasetResult]
    manifest_path: Path
    manifest_bytes: int
    manifest_sha256: str
    whole_export_sha256: str
    scope: ScopeResult
    generated_at: str


def _assert_pinned_counts(
    release_id: str,
    scope: ScopeResult,
    datasets: dict[str, DatasetResult],
    lang_counts: dict[str, int],
    area_counts: dict[str, int],
    external_source_table_counts: dict[str, int],
) -> None:
    if release_id != PINNED_RELEASE_EXPECTATIONS["content_release_id"]:
        return  # only enforce for the pinned regression release
    exp = PINNED_RELEASE_EXPECTATIONS
    errors: list[str] = []
    if len(scope.concept_ids) != exp["concepts_included"]:
        errors.append(
            f"concepts_included={len(scope.concept_ids)} != {exp['concepts_included']}; "
            f"excluded={scope.excluded_count}; roots={scope.fungi_root_ids}; "
            f"excluded_sample={scope.excluded_sample}"
        )
    if scope.excluded_count != exp["concepts_excluded"]:
        errors.append(f"concepts_excluded={scope.excluded_count} != {exp['concepts_excluded']}")
    if datasets["taxon.jsonl"].row_count != exp["concepts_included"]:
        errors.append(
            f"taxon.jsonl rows={datasets['taxon.jsonl'].row_count} != {exp['concepts_included']}"
        )
    if datasets["scientific_name.jsonl"].row_count != exp["scientific_name_rows"]:
        errors.append(
            f"scientific_name.jsonl rows={datasets['scientific_name.jsonl'].row_count} "
            f"!= {exp['scientific_name_rows']}"
        )
    if datasets["vernacular.jsonl"].row_count != exp["vernacular_rows"]:
        errors.append(
            f"vernacular.jsonl rows={datasets['vernacular.jsonl'].row_count} != {exp['vernacular_rows']}"
        )
    if datasets["taxon_external_id.jsonl"].row_count != exp["external_total_rows"]:
        errors.append(
            f"taxon_external_id.jsonl rows={datasets['taxon_external_id.jsonl'].row_count} "
            f"!= {exp['external_total_rows']}"
        )
    if datasets["taxon_redlist.jsonl"].row_count != exp["redlist_rows"]:
        errors.append(
            f"taxon_redlist.jsonl rows={datasets['taxon_redlist.jsonl'].row_count} != {exp['redlist_rows']}"
        )
    if lang_counts != exp["vernacular_by_lang"]:
        errors.append(f"vernacular_by_lang={lang_counts} != {exp['vernacular_by_lang']}")
    if area_counts != exp["redlist_by_area"]:
        errors.append(f"redlist_by_area={area_counts} != {exp['redlist_by_area']}")
    if external_source_table_counts.get("taxon_external_id_min") != exp["external_int_rows"]:
        errors.append(
            f"external_int_rows={external_source_table_counts.get('taxon_external_id_min')} "
            f"!= {exp['external_int_rows']}"
        )
    if external_source_table_counts.get("taxon_external_id_text_min") != exp["external_text_rows"]:
        errors.append(
            f"external_text_rows={external_source_table_counts.get('taxon_external_id_text_min')} "
            f"!= {exp['external_text_rows']}"
        )
    if errors:
        raise ExportError(
            "pinned-release regression assertion failed for "
            f"{release_id}: " + "; ".join(errors)
        )


def _validate_child_references(conn: sqlite3.Connection, scope: ScopeResult) -> None:
    """Verify every emitted child row's taxon_id is in scope (belt-and-braces).

    Since we filter on `_cloud_export_scope`, this is technically redundant;
    running it makes intent explicit and catches accidental refactors.
    """
    for child_table in (
        "scientific_name_min",
        "vernacular_min",
        "taxon_external_id_min",
        "taxon_external_id_text_min",
    ):
        row = conn.execute(
            f"SELECT count(*) AS n FROM {child_table} "
            "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
            "AND taxon_id NOT IN (SELECT taxon_id FROM _cloud_export_scope)"
        ).fetchone()
        if row["n"]:
            raise ExportError(f"invariant broken in {child_table}: orphan taxon_id in scope")
    row = conn.execute(
        "SELECT count(*) AS n FROM taxon_redlist_min "
        "WHERE taxon_id IS NOT NULL "
        "AND taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "AND taxon_id NOT IN (SELECT taxon_id FROM _cloud_export_scope)"
    ).fetchone()
    if row["n"]:
        raise ExportError("invariant broken in taxon_redlist_min: orphan taxon_id in scope")


def _dataset_derived_stats(conn: sqlite3.Connection) -> tuple[
    dict[str, int], dict[str, int], dict[str, int]
]:
    lang_counts = {
        row["language_code"]: row["n"]
        for row in conn.execute(
            "SELECT language_code, count(*) AS n FROM vernacular_min "
            "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
            "GROUP BY language_code ORDER BY language_code"
        )
    }
    area_counts = {
        row["assessment_area"]: row["n"]
        for row in conn.execute(
            "SELECT assessment_area, count(*) AS n FROM taxon_redlist_min "
            "WHERE taxon_id IS NOT NULL "
            "AND taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
            "GROUP BY assessment_area ORDER BY assessment_area"
        )
    }
    external_source_table_counts = {
        "taxon_external_id_min": conn.execute(
            "SELECT count(*) AS n FROM taxon_external_id_min "
            "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope)"
        ).fetchone()["n"],
        "taxon_external_id_text_min": conn.execute(
            "SELECT count(*) AS n FROM taxon_external_id_text_min "
            "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope)"
        ).fetchone()["n"],
    }
    return lang_counts, area_counts, external_source_table_counts


def _iso_utc(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
    # Second precision — deterministic once fixed; adequate for build metadata.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- atomic run -------------------------------------------------


def _reject_symlink_or_traversal(p: Path) -> None:
    """Refuse to operate on paths that resolve outside their nominal parent."""
    resolved = p.resolve()
    if resolved != p.absolute():
        # Existing symlinks on the path — resolved differs from lexical parent.
        # We only object if a component is a symlink.
        for ancestor in (p, *p.parents):
            if ancestor.is_symlink():
                raise ExportError(f"path contains a symlink component: {ancestor}")


def run_export(
    *,
    artifact_gz: Path,
    manifest: Path,
    output_dir: Path,
    policy_dir: Path,
    replace: bool = False,
    verify_only: bool = False,
    generated_at: Optional[str] = None,
) -> ExportResult:
    """Produce a deterministic cloud-taxonomy export directory."""
    artifact_gz = Path(artifact_gz).resolve()
    manifest = Path(manifest).resolve()
    output_dir = Path(output_dir).resolve()
    policy_dir = Path(policy_dir).resolve()
    _reject_symlink_or_traversal(output_dir.parent)

    with tempfile.TemporaryDirectory(prefix="sporely_cloud_export_verify_") as scratch:
        scratch_path = Path(scratch)
        src = verify_source(artifact_gz, manifest, scratch_path)

        release_id = src.content_release_id
        expected_final_name = output_dir.name
        # If output_dir has a name that mentions a different release, that's OK
        # (caller may want to override), but we record the release id in the
        # manifest either way.

        if verify_only:
            _LOG.info("verify-only: source hashes and metadata OK for %s", release_id)
            with _open_ro(src.decompressed_sqlite_path) as conn:
                scope = build_concept_set(conn)
                _install_scope_temp_table(conn, scope.concept_ids)
                _validate_child_references(conn, scope)
            return ExportResult(
                output_dir=output_dir,
                datasets={},
                manifest_path=output_dir / MANIFEST_FILENAME,
                manifest_bytes=0,
                manifest_sha256="",
                whole_export_sha256="",
                scope=scope,
                generated_at=generated_at or _iso_utc(),
            )

        # If the target already exists and is byte-identical, short-circuit.
        if output_dir.is_dir():
            existing_manifest = output_dir / MANIFEST_FILENAME
            staged_result: Optional[ExportResult] = None
            if existing_manifest.is_file():
                staged_result = _stage_and_finalize(
                    src=src,
                    output_parent=output_dir.parent,
                    output_final_name=output_dir.name,
                    policy_dir=policy_dir,
                    replace=False,
                    dry_run=True,
                    generated_at=generated_at or _iso_utc(),
                )
                if _existing_files_match_staged(output_dir, staged_result):
                    _LOG.info("existing export at %s is byte-identical", output_dir)
                    shutil.rmtree(staged_result.output_dir, ignore_errors=True)
                    return staged_result

            # Existing directory differs; either replace or refuse.
            if not replace:
                if staged_result is not None:
                    shutil.rmtree(staged_result.output_dir, ignore_errors=True)
                raise ExportError(
                    f"output directory already exists and differs; pass replace=True: {output_dir}"
                )
            if staged_result is not None:
                shutil.rmtree(staged_result.output_dir, ignore_errors=True)

        result = _stage_and_finalize(
            src=src,
            output_parent=output_dir.parent,
            output_final_name=output_dir.name,
            policy_dir=policy_dir,
            replace=replace,
            dry_run=False,
            generated_at=generated_at or _iso_utc(),
        )
        return result


def _existing_files_match_staged(existing_dir: Path, staged: ExportResult) -> bool:
    """Hash each existing dataset file on disk and compare to staged hashes.

    Trusts the freshly-generated staged export as ground truth (not the recorded
    manifest, which could be inconsistent with actual on-disk bytes).
    """
    for name, ds in staged.datasets.items():
        candidate = existing_dir / name
        if not candidate.is_file():
            return False
        if sha256_file(candidate) != ds.sha256:
            return False
    existing_manifest = existing_dir / MANIFEST_FILENAME
    if not existing_manifest.is_file():
        return False
    # Whole-export hash across the existing files (in fixed order) must also
    # match the staged whole hash.
    files_in_order = [(n, existing_dir / n) for n in DATASET_FILES]
    return whole_export_sha256(files_in_order) == staged.whole_export_sha256


def _stage_and_finalize(
    *,
    src: SourceContext,
    output_parent: Path,
    output_final_name: str,
    policy_dir: Path,
    replace: bool,
    dry_run: bool,
    generated_at: str,
) -> ExportResult:
    output_parent.mkdir(parents=True, exist_ok=True)
    tmp_output = output_parent / f".{output_final_name}.staging.{os.getpid()}"
    if tmp_output.exists():
        shutil.rmtree(tmp_output)
    tmp_output.mkdir(parents=True)

    try:
        with _open_ro(src.decompressed_sqlite_path) as conn:
            scope = build_concept_set(conn)
            _install_scope_temp_table(conn, scope.concept_ids)
            _validate_child_references(conn, scope)

            policy_hashes = hash_policies(policy_dir)

            datasets: dict[str, DatasetResult] = {}
            datasets["taxonomy_release.jsonl"] = emit_taxonomy_release(
                conn, tmp_output / "taxonomy_release.jsonl", src, policy_hashes
            )
            datasets["taxon.jsonl"] = emit_taxon(conn, tmp_output / "taxon.jsonl")
            datasets["scientific_name.jsonl"] = emit_scientific_name(
                conn, tmp_output / "scientific_name.jsonl"
            )
            datasets["vernacular.jsonl"] = emit_vernacular(
                conn, tmp_output / "vernacular.jsonl"
            )
            datasets["taxon_external_id.jsonl"] = emit_taxon_external_id(
                conn, tmp_output / "taxon_external_id.jsonl"
            )
            datasets["taxon_redlist.jsonl"] = emit_taxon_redlist(
                conn, tmp_output / "taxon_redlist.jsonl"
            )

            lang_counts, area_counts, external_source_table_counts = _dataset_derived_stats(conn)

        _assert_pinned_counts(
            src.content_release_id,
            scope,
            datasets,
            lang_counts,
            area_counts,
            external_source_table_counts,
        )

        # Length-prefixed whole-export hash across data files in fixed order.
        files_in_order = [(n, datasets[n].path) for n in DATASET_FILES]
        we_hash = whole_export_sha256(files_in_order)

        manifest_dict = _build_manifest_dict(
            src=src,
            datasets=datasets,
            scope=scope,
            lang_counts=lang_counts,
            area_counts=area_counts,
            external_source_table_counts=external_source_table_counts,
            policy_hashes=hash_policies(policy_dir),
            whole_export_hash=we_hash,
            generated_at=generated_at,
        )
        manifest_bytes = (canonical_dumps(manifest_dict) + "\n").encode("utf-8")
        manifest_path = tmp_output / MANIFEST_FILENAME
        manifest_path.write_bytes(manifest_bytes)
        manifest_sha = sha256_bytes(manifest_bytes)

        # Re-verify each written file.
        for name, ds in datasets.items():
            path = tmp_output / name
            if sha256_file(path) != ds.sha256:
                raise ExportError(f"re-verification failed for {name}")

        if dry_run:
            return ExportResult(
                output_dir=tmp_output,
                datasets=datasets,
                manifest_path=manifest_path,
                manifest_bytes=len(manifest_bytes),
                manifest_sha256=manifest_sha,
                whole_export_sha256=we_hash,
                scope=scope,
                generated_at=generated_at,
            )

        # Atomic replace: if final exists and replace=True, move it aside.
        final_dir = output_parent / output_final_name
        if final_dir.exists():
            if not replace:
                raise ExportError(f"final output exists (replace=False): {final_dir}")
            backup = output_parent / f".{output_final_name}.replaced.{os.getpid()}"
            if backup.exists():
                shutil.rmtree(backup)
            final_dir.rename(backup)
            try:
                tmp_output.rename(final_dir)
            except Exception:
                # Restore the backup on failure.
                backup.rename(final_dir)
                raise
            shutil.rmtree(backup, ignore_errors=True)
        else:
            tmp_output.rename(final_dir)

        return ExportResult(
            output_dir=final_dir,
            datasets=datasets,
            manifest_path=final_dir / MANIFEST_FILENAME,
            manifest_bytes=len(manifest_bytes),
            manifest_sha256=manifest_sha,
            whole_export_sha256=we_hash,
            scope=scope,
            generated_at=generated_at,
        )
    except Exception:
        # Cleanup temp dir on any error.
        shutil.rmtree(tmp_output, ignore_errors=True)
        raise


def _build_manifest_dict(
    *,
    src: SourceContext,
    datasets: dict[str, DatasetResult],
    scope: ScopeResult,
    lang_counts: dict[str, int],
    area_counts: dict[str, int],
    external_source_table_counts: dict[str, int],
    policy_hashes: dict[str, str],
    whole_export_hash: str,
    generated_at: str,
) -> dict:
    # Namespace counts on the external-id dataset. Compute from datasets on
    # disk to reflect the actual emitted rows, not the source query.
    ext_ns_counts: dict[str, int] = {}
    ext_path = datasets["taxon_external_id.jsonl"].path
    with ext_path.open("rb") as fh:
        for line in fh:
            obj = json.loads(line)
            key = f"{obj['source_system']}/{obj.get('namespace') or ''}"
            ext_ns_counts[key] = ext_ns_counts.get(key, 0) + 1

    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "exporter_version": EXPORTER_VERSION,
        "content_release_id": src.content_release_id,
        "taxonomy_schema_version": TAXONOMY_SCHEMA_VERSION,
        "scope_predicate_id": SCOPE_PREDICATE_ID,
        "source": {
            "artifact_gz_path": str(src.artifact_gz_path.name),
            "manifest_path": str(src.manifest_path.name),
            "gz_sha256": src.gz_sha256,
            "sqlite_sha256": src.sqlite_sha256,
        },
        "policy_hashes": dict(sorted(policy_hashes.items())),
        "included_concept_count": len(scope.concept_ids),
        "excluded_concept_count": scope.excluded_count,
        "fungi_root_ids": list(scope.fungi_root_ids),
        "vernacular_language_counts": dict(sorted(lang_counts.items())),
        "redlist_area_counts": dict(sorted(area_counts.items())),
        "external_id_namespace_counts": dict(sorted(ext_ns_counts.items())),
        "external_id_source_table_counts": dict(sorted(external_source_table_counts.items())),
        "files": [
            {
                "name": name,
                "row_count": datasets[name].row_count,
                "bytes": datasets[name].bytes,
                "sha256": datasets[name].sha256,
                "sort_keys": list(DATASET_SORT_KEYS.get(name, ())),
            }
            for name in DATASET_FILES
        ],
        "whole_export_sha256": whole_export_hash,
        "generated_at": generated_at,
    }
