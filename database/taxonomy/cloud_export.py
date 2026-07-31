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
#
# Authoritative-namespace external IDs (from the compiler's text table) are
# split out from namespace-lost legacy integer IDs (from the compiler's
# integer table where `namespace` is not preserved). The two files have
# different schemas; combining them would silently misrepresent the
# authoritative rows as sharing a schema with rows whose namespace is only
# knowable by inference outside this exporter.
DATASET_FILES = (
    "taxonomy_release.jsonl",
    "taxon.jsonl",
    "scientific_name.jsonl",
    "vernacular.jsonl",
    "taxon_external_id.jsonl",
    "taxon_external_id_legacy_integer.jsonl",
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
# Authoritative external IDs (namespace declared by compiler text table).
TAXON_EXTERNAL_ID_AUTHORITATIVE_FIELDS = (
    "taxon_id",
    "source_system",
    "namespace",
    "external_id",
    "id_role",
    "is_preferred",
    "external_name",
    "note",
)
# Legacy integer external IDs — the compiler's integer table does not
# preserve `namespace`. Rows are emitted verbatim under an explicit "legacy"
# label so downstream consumers cannot misread them as authoritative.
TAXON_EXTERNAL_ID_LEGACY_INTEGER_FIELDS = (
    "taxon_id",
    "source_system",
    "external_id",  # cast to text; the source column type is INTEGER
    "id_role",
    "is_preferred",
    "external_name",
    "note",
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
        "id_role", "is_preferred",
    ),
    "taxon_external_id_legacy_integer.jsonl": (
        "taxon_id", "source_system", "external_id", "id_role", "is_preferred",
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


def _coerce_authoritative_external_row(row: sqlite3.Row) -> dict:
    return {
        "taxon_id": row["taxon_id"],
        "source_system": row["source_system"],
        "namespace": row["namespace"],
        "external_id": row["external_id"],
        "id_role": row["id_role"],
        "is_preferred": bool(row["is_preferred"]),
        "external_name": row["external_name"],
        "note": row["note"],
    }


def _coerce_legacy_integer_external_row(row: sqlite3.Row) -> dict:
    return {
        "taxon_id": row["taxon_id"],
        "source_system": row["source_system"],
        "external_id": str(row["external_id"]),
        "id_role": row["id_role"],
        "is_preferred": bool(row["is_preferred"]),
        "external_name": row["external_name"],
        "note": row["note"],
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
    # Authoritative external IDs split by source_system:
    "external_authoritative_col_rows": 620975,   # from taxon_external_id_text_min
    "external_authoritative_nortaxa_rows": 13919,  # derived from taxon_min.norwegian_taxon_id
    "external_authoritative_total_rows": 634894,   # = 620975 + 13919
    # Legacy namespace-lost integer rows:
    "external_legacy_int_rows": 61583,
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


def emit_taxon_external_id_authoritative(
    conn: sqlite3.Connection, out_path: Path
) -> DatasetResult:
    """Emit compiler-authoritative external IDs (namespace declared).

    Two authoritative sources, merged in a single deterministic file:

    1. `taxon_external_id_text_min` — every scoped row, namespace declared
       by the compiler per `policies/source_priority.yml.identifier_namespaces`.

    2. `taxon_min.norwegian_taxon_id` — derived authoritative NorTaxa row.
       The compiler establishes `taxon_min.norwegian_taxon_id` only from a
       unique preferred source usage whose original namespace is
       `nortaxa_taxon_id`. This invariant is documented at
       `docs/identity-contract.md` and enforced by `build_sqlite_candidate.py`
       through the UNIQUE partial index `idx_taxon_no_id ON (norwegian_taxon_id)
       WHERE norwegian_taxon_id IS NOT NULL`. The exporter therefore emits
       one derived row per scoped concept with a non-null `norwegian_taxon_id`:

           source_system : "nortaxa"
           namespace     : "nortaxa_taxon_id"
           external_id   : CAST(norwegian_taxon_id AS TEXT)
           id_role       : "accepted"
           is_preferred  : true
           external_name : canonical_scientific_name
           note          : "derived_from_taxon_min.norwegian_taxon_id"

       No other namespace is derived from `taxon_external_id_min`; those rows
       remain in `taxon_external_id_legacy_integer.jsonl` verbatim.

    Duplicate authoritative semantic keys `(source_system, namespace,
    external_id, taxon_id)` are detected and cause an ExportError.
    """
    filename = "taxon_external_id.jsonl"
    sql = (
        # Text-table authoritative rows.
        "SELECT taxon_id, source_system, namespace, external_id, id_role, "
        "is_preferred, external_name, note "
        "FROM taxon_external_id_text_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "UNION ALL "
        # Derived NorTaxa authoritative rows.
        "SELECT taxon_id, 'nortaxa' AS source_system, "
        "'nortaxa_taxon_id' AS namespace, "
        "CAST(norwegian_taxon_id AS TEXT) AS external_id, "
        "'accepted' AS id_role, 1 AS is_preferred, "
        "canonical_scientific_name AS external_name, "
        "'derived_from_taxon_min.norwegian_taxon_id' AS note "
        "FROM taxon_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "AND norwegian_taxon_id IS NOT NULL "
        # UNION ALL ORDER BY uses positional refs. Deterministic sort.
        "ORDER BY 1 ASC, 2 ASC, 3 ASC, 4 ASC, 5 ASC, 6 ASC"
    )

    # Duplicate detection: no two rows in the combined output may share the
    # same `(source_system, namespace, external_id, taxon_id)` semantic key.
    seen: set[tuple] = set()
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            key = (row["source_system"], row["namespace"], row["external_id"], row["taxon_id"])
            if key in seen:
                raise ExportError(
                    f"duplicate authoritative semantic key in {filename}: "
                    f"source_system={key[0]!r}, namespace={key[1]!r}, "
                    f"external_id={key[2]!r}, taxon_id={key[3]!r}"
                )
            seen.add(key)
            written += _write_json_line(fh, _coerce_authoritative_external_row(row))
            rows += 1
    return _finalize_dataset(out_path, filename, rows, written)


def emit_taxon_external_id_legacy_integer(
    conn: sqlite3.Connection, out_path: Path
) -> DatasetResult:
    """Emit legacy integer external IDs (compiler-lost namespace).

    Source: `taxon_external_id_min`. The compiler's integer table does not
    preserve the originating namespace (e.g. `nortaxa_taxon_id` vs
    `nortaxa_dwc_id`); only `source_system` remains. These rows are emitted
    under an explicit legacy label so downstream code cannot misread them as
    authoritative.

    `external_id` is cast to text so numeric equality across namespaces
    cannot silently produce type-based joins.
    """
    filename = "taxon_external_id_legacy_integer.jsonl"
    sql = (
        "SELECT taxon_id, source_system, CAST(external_id AS TEXT) AS external_id, "
        "id_role, is_preferred, external_name, note "
        "FROM taxon_external_id_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "ORDER BY taxon_id ASC, source_system ASC, external_id ASC, "
        "id_role ASC, is_preferred ASC"
    )
    rows = 0
    written = 0
    with _open_writer(out_path) as fh:
        for row in conn.execute(sql):
            written += _write_json_line(fh, _coerce_legacy_integer_external_row(row))
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


def _parse_source_release_version(source_release_id: Optional[str]) -> Optional[str]:
    """Extract the upstream version from a `<source>:<version>[:<date>]` id.

    Compiler-declared format (per `policies/release_contract.yml`) is
    `<source-code>:<upstream-version-or-issued-date>[:<issued_at>]`. Returns
    `None` if the input is missing or malformed rather than guessing.
    """
    if not source_release_id:
        return None
    parts = source_release_id.split(":")
    if len(parts) < 2:
        return None
    version = parts[1].strip()
    return version or None


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
    #
    # Fields not present in either the compiler's `taxonomy_meta` nor the
    # outer manifest are emitted as `null`. No literal fabricated fallback
    # is applied — this exporter never invents provenance.
    #
    # `nortaxa_release` is derivable from the compiler's own record
    # `source_release[nortaxa].id` (e.g. "nortaxa:1.284:2026-07-17" →
    # version "1.284"). If the input format changes, `nortaxa_release`
    # falls back to `null` rather than a guessed constant.
    row_obj: dict = {
        "content_release_id": src.content_release_id,
        "taxonomy_schema_version": int(meta["taxonomy_schema_version"]),
        "canonical_authority": manifest.get("canonical_authority"),
        "checklistbank_dataset_id": manifest.get("checklistbank_dataset_id"),
        "doi": manifest.get("doi"),
        "nortaxa_release": _parse_source_release_version(
            meta.get("source_release[nortaxa].id")
        ),
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
    dangling_parents: "DanglingParentReport"
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
    if datasets["taxon_external_id.jsonl"].row_count != exp["external_authoritative_total_rows"]:
        errors.append(
            f"taxon_external_id.jsonl (authoritative) rows="
            f"{datasets['taxon_external_id.jsonl'].row_count} "
            f"!= {exp['external_authoritative_total_rows']}"
        )
    if datasets["taxon_external_id_legacy_integer.jsonl"].row_count != exp["external_legacy_int_rows"]:
        errors.append(
            f"taxon_external_id_legacy_integer.jsonl rows="
            f"{datasets['taxon_external_id_legacy_integer.jsonl'].row_count} "
            f"!= {exp['external_legacy_int_rows']}"
        )
    if datasets["taxon_redlist.jsonl"].row_count != exp["redlist_rows"]:
        errors.append(
            f"taxon_redlist.jsonl rows={datasets['taxon_redlist.jsonl'].row_count} != {exp['redlist_rows']}"
        )
    if lang_counts != exp["vernacular_by_lang"]:
        errors.append(f"vernacular_by_lang={lang_counts} != {exp['vernacular_by_lang']}")
    if area_counts != exp["redlist_by_area"]:
        errors.append(f"redlist_by_area={area_counts} != {exp['redlist_by_area']}")
    if errors:
        raise ExportError(
            "pinned-release regression assertion failed for "
            f"{release_id}: " + "; ".join(errors)
        )


@dataclass
class DanglingParentReport:
    """Concept rows in scope whose `parent_taxon_id` lies outside scope.

    Reported, never repaired. Parent values are preserved verbatim in the
    emitted `taxon.jsonl`; W2 or a future compiler stage decides how to
    interpret them (e.g. treat as root-in-cloud-scope, or import the parent).
    """
    count: int
    total_with_parent: int
    sample: list[dict]  # up to 20 sample rows for the manifest


def _validate_child_schema(conn: sqlite3.Connection) -> None:
    """Schema-level drift check on compiler child tables.

    Verifies each dependent child table still declares `taxon_id NOT NULL`
    and that every child row has a matching `taxon_min` row. This is NOT
    an emitted-row validation — that runs after the JSONL files are on
    disk (see `_validate_emitted_taxon_id_references`).
    """
    for child_table in (
        "scientific_name_min",
        "vernacular_min",
        "taxon_external_id_min",
        "taxon_external_id_text_min",
    ):
        info = list(conn.execute(f"PRAGMA table_info({child_table})"))
        tcol = next((r for r in info if r["name"] == "taxon_id"), None)
        if tcol is None:
            raise ExportError(f"child table {child_table} missing taxon_id column")
        if tcol["notnull"] != 1:
            raise ExportError(
                f"child table {child_table} taxon_id no longer NOT NULL — schema drift"
            )
        orphan = conn.execute(
            f"SELECT count(*) AS n FROM {child_table} c "
            "WHERE c.taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
            "AND NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = c.taxon_id)"
        ).fetchone()["n"]
        if orphan:
            raise ExportError(
                f"child table {child_table} has {orphan} in-scope rows with no "
                "matching taxon_min row"
            )


_DEPENDENT_JSONL_FILES = (
    "scientific_name.jsonl",
    "vernacular.jsonl",
    "taxon_external_id.jsonl",
    "taxon_external_id_legacy_integer.jsonl",
    "taxon_redlist.jsonl",
)


def _validate_emitted_taxon_id_references(
    staging_dir: Path, concept_ids: frozenset[int]
) -> None:
    """Stream every emitted child JSONL row and validate its `taxon_id`.

    For each dependent file (`scientific_name.jsonl`, `vernacular.jsonl`,
    `taxon_external_id.jsonl`, `taxon_external_id_legacy_integer.jsonl`,
    `taxon_redlist.jsonl`), open line-by-line and require:

    * the line parses as JSON;
    * the object has a `taxon_id` field;
    * the value is neither null, boolean, nor string;
    * the value is a Python `int` (JSON integer);
    * the integer is a member of the exported concept set `S`.

    Runs BEFORE the manifest is written and BEFORE atomic publication.

    Does not load a whole file into memory: iterates line-by-line.
    """
    for name in _DEPENDENT_JSONL_FILES:
        path = staging_dir / name
        if not path.is_file():
            raise ExportError(f"post-emission validator: missing file {name}")
        with path.open("rb") as fh:
            line_number = 0
            for raw in fh:
                line_number += 1
                stripped = raw.strip()
                if not stripped:
                    raise ExportError(
                        f"{name}:{line_number}: unexpected empty line in JSONL"
                    )
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise ExportError(f"{name}:{line_number}: JSON parse error: {exc}") from exc
                if not isinstance(obj, dict):
                    raise ExportError(f"{name}:{line_number}: not a JSON object")
                if "taxon_id" not in obj:
                    raise ExportError(f"{name}:{line_number}: missing taxon_id field")
                value = obj["taxon_id"]
                # bool is a subclass of int in Python — reject explicitly.
                if isinstance(value, bool):
                    raise ExportError(
                        f"{name}:{line_number}: taxon_id={value!r}: "
                        "reason=taxon_id must be a JSON integer, not boolean"
                    )
                if value is None:
                    raise ExportError(
                        f"{name}:{line_number}: taxon_id={value!r}: "
                        "reason=taxon_id must not be null"
                    )
                if isinstance(value, str):
                    raise ExportError(
                        f"{name}:{line_number}: taxon_id={value!r}: "
                        "reason=taxon_id must be a JSON integer, not a string"
                    )
                if not isinstance(value, int):
                    raise ExportError(
                        f"{name}:{line_number}: taxon_id={value!r}: "
                        f"reason=taxon_id must be a JSON integer, got type={type(value).__name__}"
                    )
                if value not in concept_ids:
                    raise ExportError(
                        f"{name}:{line_number}: taxon_id={value}: "
                        "reason=taxon_id is not in the exported concept set"
                    )


def _audit_dangling_parents(conn: sqlite3.Connection) -> DanglingParentReport:
    """List concepts whose parent lies outside the exported scope.

    Preserves the compiler's `parent_taxon_id` in the emitted taxon rows
    verbatim; W1 never nulls a dangling parent. Downstream consumers decide
    whether such rows are treated as roots in the cloud scope, whether the
    parent should be pulled in, or whether the discrepancy is a data bug.
    """
    total_with_parent = conn.execute(
        "SELECT count(*) AS n FROM taxon_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "AND parent_taxon_id IS NOT NULL"
    ).fetchone()["n"]
    count = conn.execute(
        "SELECT count(*) AS n FROM taxon_min "
        "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
        "AND parent_taxon_id IS NOT NULL "
        "AND parent_taxon_id NOT IN (SELECT taxon_id FROM _cloud_export_scope)"
    ).fetchone()["n"]
    sample = [
        dict(r)
        for r in conn.execute(
            "SELECT taxon_id, parent_taxon_id, canonical_scientific_name, "
            "taxon_rank, canonical_source_system FROM taxon_min "
            "WHERE taxon_id IN (SELECT taxon_id FROM _cloud_export_scope) "
            "AND parent_taxon_id IS NOT NULL "
            "AND parent_taxon_id NOT IN (SELECT taxon_id FROM _cloud_export_scope) "
            "ORDER BY taxon_id LIMIT 20"
        )
    ]
    return DanglingParentReport(count=count, total_with_parent=total_with_parent, sample=sample)


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
    """Reject paths containing a symlink component or a traversal token.

    Walks lexical ancestors up to the filesystem root, testing every
    existing component with `os.path.islink` (which does NOT follow the
    link). Purely comparing `resolve()` vs `absolute()` is unreliable
    because a canonical macOS path can equal a resolved symlink target
    (e.g. `/tmp` → `/private/tmp`), producing false negatives on the target
    side and false positives on paths that pass through `/tmp`.

    Also rejects lexical `..` traversal tokens in the caller-supplied path.
    """
    # Lexical traversal check on the original (pre-resolution) path.
    for part in Path(p).parts:
        if part == "..":
            raise ExportError(f"path contains '..' traversal token: {p}")

    # Ancestor symlink check — includes `p` itself.
    current = Path(p).absolute()
    checked: set = set()
    while True:
        if current in checked:
            break
        checked.add(current)
        if os.path.lexists(current) and current.is_symlink():
            raise ExportError(f"path contains a symlink component: {current}")
        parent = current.parent
        if parent == current:
            break
        current = parent


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
    # Reject symlink / traversal on the caller-supplied output path BEFORE
    # any `.resolve()` step — resolve() follows symlinks and normalizes '..',
    # which would erase exactly the tokens we need to catch.
    _reject_symlink_or_traversal(Path(output_dir))

    artifact_gz = Path(artifact_gz).resolve()
    manifest = Path(manifest).resolve()
    output_dir = Path(output_dir).resolve()
    policy_dir = Path(policy_dir).resolve()

    with tempfile.TemporaryDirectory(prefix="sporely_cloud_export_verify_") as scratch:
        scratch_path = Path(scratch)
        src = verify_source(artifact_gz, manifest, scratch_path)

        release_id = src.content_release_id
        gen_at = generated_at or _iso_utc()

        if verify_only:
            _LOG.info("verify-only: source hashes and metadata OK for %s", release_id)
            with _open_ro(src.decompressed_sqlite_path) as conn:
                scope = build_concept_set(conn)
                _install_scope_temp_table(conn, scope.concept_ids)
                _validate_child_schema(conn)
                dangling = _audit_dangling_parents(conn)
            return ExportResult(
                output_dir=output_dir,
                datasets={},
                manifest_path=output_dir / MANIFEST_FILENAME,
                manifest_bytes=0,
                manifest_sha256="",
                whole_export_sha256="",
                scope=scope,
                dangling_parents=dangling,
                generated_at=gen_at,
            )

        # If the target already exists and every dataset file's on-disk
        # SHA-256 matches what a fresh staging run would produce, short-
        # circuit and return a result whose paths point at the existing
        # final directory (never at a staging dir that has been cleaned up).
        if output_dir.is_dir() and (output_dir / MANIFEST_FILENAME).is_file():
            staged = _stage_and_finalize(
                src=src,
                output_parent=output_dir.parent,
                output_final_name=output_dir.name,
                policy_dir=policy_dir,
                replace=False,
                dry_run=True,
                generated_at=gen_at,
            )
            try:
                if _existing_matches_staged(output_dir, staged):
                    _LOG.info("existing export at %s is byte-identical", output_dir)
                    existing_manifest = json.loads(
                        (output_dir / MANIFEST_FILENAME).read_text(encoding="utf-8")
                    )
                    existing_generated_at = existing_manifest.get("generated_at", gen_at)
                    final_datasets: dict[str, DatasetResult] = {}
                    for name, ds in staged.datasets.items():
                        p = output_dir / name
                        final_datasets[name] = DatasetResult(
                            filename=name,
                            row_count=ds.row_count,
                            bytes=p.stat().st_size,
                            sha256=sha256_file(p),
                            path=p,
                        )
                    final_manifest = output_dir / MANIFEST_FILENAME
                    manifest_bytes = final_manifest.read_bytes()
                    return ExportResult(
                        output_dir=output_dir,
                        datasets=final_datasets,
                        manifest_path=final_manifest,
                        manifest_bytes=len(manifest_bytes),
                        manifest_sha256=sha256_bytes(manifest_bytes),
                        whole_export_sha256=whole_export_sha256(
                            [(n, output_dir / n) for n in DATASET_FILES]
                        ),
                        scope=staged.scope,
                        dangling_parents=staged.dangling_parents,
                        generated_at=existing_generated_at,
                    )
                # Differs — cleanup staging.
                if not replace:
                    raise ExportError(
                        "output directory already exists and differs; pass "
                        f"replace=True: {output_dir}"
                    )
            finally:
                shutil.rmtree(staged.output_dir, ignore_errors=True)

        # Fresh build (either no output_dir at all, or replace=True after
        # confirming difference).
        result = _stage_and_finalize(
            src=src,
            output_parent=output_dir.parent,
            output_final_name=output_dir.name,
            policy_dir=policy_dir,
            replace=replace,
            dry_run=False,
            generated_at=gen_at,
        )
        return result


_DETERMINISTIC_MANIFEST_KEYS = (
    "manifest_schema_version",
    "export_schema_version",
    "exporter_version",
    "content_release_id",
    "taxonomy_schema_version",
    "scope_predicate_id",
    "source",
    "policy_hashes",
    "included_concept_count",
    "excluded_concept_count",
    "fungi_root_ids",
    "vernacular_language_counts",
    "redlist_area_counts",
    "external_id_authoritative_namespace_counts",
    "external_id_legacy_integer_source_counts",
    "external_id_source_table_counts",
    "dangling_parent_references",
    "files",
    "whole_export_sha256",
)


def _existing_matches_staged(existing_dir: Path, staged: ExportResult) -> bool:
    """Faithfully validate an existing export against a freshly staged one.

    Passes only when ALL of the following hold:
      * Existing manifest parses.
      * Its `files` list exactly equals `DATASET_FILES`.
      * Each recorded `row_count`, `bytes`, and `sha256` matches the actual
        on-disk file at the recorded name.
      * The recorded `whole_export_sha256` matches a fresh recomputation
        over the existing files.
      * The recorded `dangling_parent_references` block matches the staged
        one (structural equality).
      * Every deterministic manifest field equals the staged manifest's
        field. `generated_at` is deliberately excluded from comparison —
        it is the only field the caller may legitimately vary between runs.

    A stale, forged, or partially-corrupted manifest fails validation and
    forces the caller to treat the existing output as differing.
    """
    existing_manifest_path = existing_dir / MANIFEST_FILENAME
    if not existing_manifest_path.is_file():
        return False
    try:
        existing = json.loads(existing_manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False

    # File list must exactly match DATASET_FILES.
    existing_files = existing.get("files") or []
    if [f.get("name") for f in existing_files] != list(DATASET_FILES):
        return False

    # Cross-check each recorded per-file row_count/bytes/sha256 vs on-disk.
    for entry in existing_files:
        name = entry["name"]
        path = existing_dir / name
        if not path.is_file():
            return False
        if entry.get("bytes") != path.stat().st_size:
            return False
        if entry.get("sha256") != sha256_file(path):
            return False
        staged_ds = staged.datasets.get(name)
        if staged_ds is None or staged_ds.row_count != entry.get("row_count"):
            return False
        if staged_ds.sha256 != entry.get("sha256"):
            return False

    # whole_export_sha256 recomputed over the existing files.
    if whole_export_sha256([(n, existing_dir / n) for n in DATASET_FILES]) != existing.get(
        "whole_export_sha256"
    ):
        return False
    if existing.get("whole_export_sha256") != staged.whole_export_sha256:
        return False

    # Dangling-parent block must match the staged report (structural equality).
    staged_dp = {
        "count": staged.dangling_parents.count,
        "total_with_parent": staged.dangling_parents.total_with_parent,
        "sample": staged.dangling_parents.sample,
    }
    if existing.get("dangling_parent_references") != staged_dp:
        return False

    # Deterministic fields must all match the staged manifest bytes.
    staged_manifest = json.loads(staged.manifest_path.read_text(encoding="utf-8"))
    for key in _DETERMINISTIC_MANIFEST_KEYS:
        if existing.get(key) != staged_manifest.get(key):
            return False

    return True


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
            _validate_child_schema(conn)
            dangling = _audit_dangling_parents(conn)

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
            datasets["taxon_external_id.jsonl"] = emit_taxon_external_id_authoritative(
                conn, tmp_output / "taxon_external_id.jsonl"
            )
            datasets["taxon_external_id_legacy_integer.jsonl"] = emit_taxon_external_id_legacy_integer(
                conn, tmp_output / "taxon_external_id_legacy_integer.jsonl"
            )
            datasets["taxon_redlist.jsonl"] = emit_taxon_redlist(
                conn, tmp_output / "taxon_redlist.jsonl"
            )

            lang_counts, area_counts, external_source_table_counts = _dataset_derived_stats(conn)

        # Post-emission reference validation: stream every emitted child JSONL
        # line and confirm its `taxon_id` is an integer belonging to `S`.
        _validate_emitted_taxon_id_references(tmp_output, frozenset(scope.concept_ids))

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
            dangling=dangling,
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
                dangling_parents=dangling,
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

        # Rewrite dataset `path` fields to point at the final dir.
        final_datasets = {
            name: DatasetResult(
                filename=name,
                row_count=ds.row_count,
                bytes=ds.bytes,
                sha256=ds.sha256,
                path=final_dir / name,
            )
            for name, ds in datasets.items()
        }
        return ExportResult(
            output_dir=final_dir,
            datasets=final_datasets,
            manifest_path=final_dir / MANIFEST_FILENAME,
            manifest_bytes=len(manifest_bytes),
            manifest_sha256=manifest_sha,
            whole_export_sha256=we_hash,
            scope=scope,
            dangling_parents=dangling,
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
    dangling: DanglingParentReport,
    lang_counts: dict[str, int],
    area_counts: dict[str, int],
    external_source_table_counts: dict[str, int],
    policy_hashes: dict[str, str],
    whole_export_hash: str,
    generated_at: str,
) -> dict:
    # Authoritative external-id namespace counts (from the namespaced file).
    ns_counts: dict[str, int] = {}
    with datasets["taxon_external_id.jsonl"].path.open("rb") as fh:
        for line in fh:
            obj = json.loads(line)
            key = f"{obj['source_system']}/{obj['namespace']}"
            ns_counts[key] = ns_counts.get(key, 0) + 1

    # Legacy integer external-id source_system counts (namespace not present).
    legacy_source_counts: dict[str, int] = {}
    with datasets["taxon_external_id_legacy_integer.jsonl"].path.open("rb") as fh:
        for line in fh:
            obj = json.loads(line)
            key = obj["source_system"]
            legacy_source_counts[key] = legacy_source_counts.get(key, 0) + 1

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
        "external_id_authoritative_namespace_counts": dict(sorted(ns_counts.items())),
        "external_id_legacy_integer_source_counts": dict(sorted(legacy_source_counts.items())),
        "external_id_source_table_counts": dict(sorted(external_source_table_counts.items())),
        "dangling_parent_references": {
            "count": dangling.count,
            "total_with_parent": dangling.total_with_parent,
            "sample": dangling.sample,
        },
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
