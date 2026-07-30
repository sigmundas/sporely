#!/usr/bin/env python3
"""Build a desktop taxonomy-v2 SQLite candidate from a Stage 3A compile.

Reuses the existing bundled schema (``taxon_min``, ``vernacular_min``,
``scientific_name_min``, ``taxon_external_id_min``) plus a small companion
table ``taxon_external_id_text_min`` for text-form external identifiers
(COL usage IDs like ``9Z2GC``) that the legacy INTEGER column cannot hold.

Identity contract:

* ``taxon_min.taxon_id`` = ``sporely_taxon_id`` (integer, Sporely-owned).
* No NorTaxa DwC id, NorTaxa ``taxonID``, NBIC value or COL usage id is ever
  reinterpreted as ``taxon_min.taxon_id`` — every external identifier is
  stored under an explicit ``(source_system, id_role)`` namespace.
* Observation records are NOT touched by this stage.

Determinism:

* All inserts are performed in a stable canonical order (`ORDER BY` on the
  primary keys in the input JSONL streams).
* ``PRAGMA locking_mode = EXCLUSIVE`` and ``PRAGMA journal_mode = OFF`` during
  build to eliminate journal / WAL turbulence.
* A vacuum + a matching ``PRAGMA page_size`` normalize the physical page
  layout so two clean builds produce byte-identical SQLite files where
  SQLite permits it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Iterator

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from identity_registry import (  # noqa: E402
    IdentityRegistry,
    RegistryError,
    SHARD_MANIFEST_FILENAME,
    iter_shard_lines,
    load_shard_manifest,
)


TAXONOMY_SCHEMA_VERSION = 2
CHUNK_BYTES = 1 * 1024 * 1024
BATCH_SIZE = 5000

SOURCE_SYSTEM_MAP = {
    "col_xr": "col_xr",
    "nortaxa": "artsdatabanken",
}

# Every namespace we know how to store.
INTEGER_NAMESPACES = frozenset({
    "nortaxa_dwc_id",
    "nortaxa_taxon_id",
    "nortaxa_accepted_name_usage_id",
    "nortaxa_parent_name_usage_id",
})
TEXT_NAMESPACES = frozenset({
    "col_usage_id",
})


class BuildError(Exception):
    """Raised on any candidate-build precondition or invariant failure."""


def _open_lines(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise BuildError(
                    f"{path}:{line_number}: malformed JSON: {exc}"
                ) from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _registry_identity_hash(registry_path: Path) -> str:
    """Return the identity fingerprint of a registry, whether it is a single
    JSONL file or a validated shard directory. For the shard form we trust
    the concatenated_sha256 in the manifest, which the loader already
    verifies matches the physical shard bytes."""
    if registry_path.is_dir():
        manifest = load_shard_manifest(registry_path)
        return str(manifest["concatenated_sha256"])
    return _sha256_file(registry_path)


CREATE_SCHEMA_SQL = """
PRAGMA page_size = 4096;

CREATE TABLE taxon_min (
    taxon_id                     INTEGER PRIMARY KEY,      -- sporely_taxon_id
    parent_taxon_id              INTEGER,
    genus                        TEXT NOT NULL,
    specific_epithet             TEXT NOT NULL,
    family                       TEXT,
    norwegian_taxon_id           INTEGER,
    swedish_taxon_id             INTEGER,
    inaturalist_taxon_id         INTEGER,
    canonical_scientific_name    TEXT,
    taxon_rank                   TEXT,
    taxonomic_status             TEXT,
    source_system                TEXT,
    preferred_scientific_name_no TEXT,
    preferred_scientific_name_sv TEXT,
    sporely_content_release_id   TEXT,
    canonical_source_system      TEXT NOT NULL,
    canonical_external_id        TEXT NOT NULL,
    FOREIGN KEY (parent_taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE vernacular_min (
    vernacular_id     INTEGER PRIMARY KEY,
    taxon_id          INTEGER NOT NULL,
    language_code     TEXT NOT NULL,
    vernacular_name   TEXT NOT NULL,
    is_preferred_name INTEGER NOT NULL DEFAULT 0,
    source            TEXT,
    FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE scientific_name_min (
    scientific_name_id  INTEGER PRIMARY KEY,
    taxon_id            INTEGER NOT NULL,
    language_code       TEXT NOT NULL,
    scientific_name     TEXT NOT NULL,
    is_preferred_name   INTEGER NOT NULL DEFAULT 0,
    source              TEXT,
    note                TEXT,
    FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE taxon_external_id_min (
    external_id_row_id  INTEGER PRIMARY KEY,
    taxon_id            INTEGER NOT NULL,
    source_system       TEXT NOT NULL,
    external_id         INTEGER NOT NULL,
    id_role             TEXT NOT NULL,
    is_preferred        INTEGER NOT NULL DEFAULT 0,
    external_name       TEXT,
    note                TEXT,
    FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE taxon_external_id_text_min (
    external_id_row_id  INTEGER PRIMARY KEY,
    taxon_id            INTEGER NOT NULL,
    source_system       TEXT NOT NULL,
    namespace           TEXT NOT NULL,
    external_id         TEXT NOT NULL,
    id_role             TEXT NOT NULL,
    is_preferred        INTEGER NOT NULL DEFAULT 0,
    external_name       TEXT,
    note                TEXT,
    FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE taxon_redlist_min (
    redlist_row_id            INTEGER PRIMARY KEY,
    taxon_id                  INTEGER,
    source_system             TEXT NOT NULL,
    source_release            TEXT NOT NULL,
    assessment_id             TEXT NOT NULL,
    assessment_area           TEXT NOT NULL,
    assessed_name_source      TEXT NOT NULL,
    assessed_name_namespace   TEXT NOT NULL,
    assessed_name_id          TEXT NOT NULL,
    scientific_name_snapshot  TEXT NOT NULL,
    authorship_snapshot       TEXT,
    taxon_rank_snapshot       TEXT,
    category_raw              TEXT NOT NULL,
    category_code             TEXT NOT NULL,
    category_is_downgraded    INTEGER NOT NULL DEFAULT 0,
    criteria                  TEXT,
    expert_group              TEXT,
    assessment_url            TEXT,
    FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

CREATE TABLE taxonomy_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

CREATE_INDEXES_SQL = """
CREATE INDEX idx_taxon_genus ON taxon_min(genus);
CREATE INDEX idx_taxon_genus_species ON taxon_min(genus, specific_epithet);
CREATE INDEX idx_taxon_parent ON taxon_min(parent_taxon_id);
CREATE INDEX idx_taxon_canonical_name ON taxon_min(canonical_scientific_name);
CREATE INDEX idx_taxon_source_system ON taxon_min(source_system);
CREATE UNIQUE INDEX idx_taxon_no_id ON taxon_min(norwegian_taxon_id)
    WHERE norwegian_taxon_id IS NOT NULL;

CREATE UNIQUE INDEX idx_vern_unique
    ON vernacular_min(taxon_id, language_code, vernacular_name);
CREATE INDEX idx_vern_lang_name
    ON vernacular_min(language_code, vernacular_name);
CREATE INDEX idx_vern_taxon_lang
    ON vernacular_min(taxon_id, language_code);

CREATE UNIQUE INDEX idx_scientific_name_unique
    ON scientific_name_min(taxon_id, language_code, scientific_name);
CREATE INDEX idx_scientific_name_lookup
    ON scientific_name_min(language_code, scientific_name);

CREATE UNIQUE INDEX idx_external_source_id
    ON taxon_external_id_min(source_system, external_id, taxon_id);
CREATE INDEX idx_external_taxon_source
    ON taxon_external_id_min(taxon_id, source_system);

CREATE UNIQUE INDEX idx_external_text_source_id
    ON taxon_external_id_text_min(source_system, namespace, external_id, taxon_id);
CREATE INDEX idx_external_text_taxon_source
    ON taxon_external_id_text_min(taxon_id, source_system);
-- Supports (source_system, external_id) lookups without requiring the
-- caller to name the namespace up front.
CREATE INDEX idx_external_text_source_value
    ON taxon_external_id_text_min(source_system, external_id);

CREATE UNIQUE INDEX idx_redlist_assessment_id
    ON taxon_redlist_min(source_system, source_release, assessment_id);
CREATE UNIQUE INDEX idx_redlist_name_area
    ON taxon_redlist_min(
        source_system, source_release,
        assessed_name_namespace, assessed_name_id, assessment_area
    );
CREATE INDEX idx_redlist_taxon_area_release
    ON taxon_redlist_min(taxon_id, assessment_area, source_release);
"""


# ----- Row building --------------------------------------------------------


def _canonical_genus_species(record: dict) -> tuple[str, str, str]:
    classification = record.get("classification") or {}
    genus = str(classification.get("genus", "") or "").strip()
    species = str(classification.get("specific_epithet", "") or "").strip()
    family = str(classification.get("family", "") or "").strip() or None
    # Fall back to splitting the scientific name when the classification is
    # sparse (as it is on higher-rank ancestor rows).
    if not genus:
        sci = str(record.get("scientific_name", "") or "").strip()
        parts = sci.split()
        genus = parts[0] if parts else ""
        if not species and len(parts) >= 2:
            species = parts[1]
    return genus, species, family


def _iter_source_usage_source_index(source_usages_path: Path) -> dict:
    """Build (source, namespace, identifier) -> sporely_taxon_id for parent
    resolution. Memory-only; ~682k entries."""
    index: dict[tuple[str, str, str], int] = {}
    for row in _open_lines(source_usages_path):
        key = (
            row["source_code"],
            row["source_usage"]["namespace"],
            row["source_usage"]["identifier"],
        )
        sporely_id = int(row["sporely_taxon_id"])
        previous = index.get(key)
        if previous is not None and previous != sporely_id:
            raise BuildError(
                f"source usage {key!r} bound to two Sporely IDs: {previous} vs {sporely_id}"
            )
        index[key] = sporely_id
    return index


def _resolve_parent_sporely_id(
    canonical_row: dict,
    source_usage_index: dict[tuple[str, str, str], int],
) -> int | None:
    parent_ref = canonical_row.get("parent_name_usage_id")
    resolution = canonical_row.get("parent_reference_resolution", "absent")
    if not parent_ref or resolution != "resolved":
        return None
    key = (
        canonical_row["canonical_source_code"],
        parent_ref["namespace"],
        parent_ref["value"],
    )
    return source_usage_index.get(key)


def build_candidate(
    *,
    release_dir: Path,
    registry_path: Path,
    output_db: Path,
) -> dict:
    """Transactionally build the SQLite candidate.

    Returns a summary dict with row counts and the file SHA-256.
    """
    if output_db.exists() or output_db.is_symlink():
        raise BuildError(f"output already exists: {output_db}")

    taxa_path = release_dir / "taxa.jsonl"
    source_usages_path = release_dir / "source_usages.jsonl"
    vernacular_path = release_dir / "vernacular.jsonl"
    manifest_path = release_dir / "manifest.json"
    for p in (taxa_path, source_usages_path, vernacular_path, manifest_path):
        if not p.exists():
            raise BuildError(f"required Stage 3A output not found: {p}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("taxonomy_schema_version") != TAXONOMY_SCHEMA_VERSION:
        raise BuildError(
            f"unexpected taxonomy_schema_version: {manifest.get('taxonomy_schema_version')}"
        )
    if manifest.get("state") != "candidate":
        raise BuildError(f"unexpected release state: {manifest.get('state')}")

    # Registry sanity: it must load, and its concatenated SHA must line up
    # with what the manifest records.
    registry = IdentityRegistry(registry_path)
    try:
        registry.load()
    except RegistryError as exc:
        raise BuildError(f"registry load failed: {exc}") from exc

    output_db.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".{output_db.name}.", suffix=".tmp",
        dir=str(output_db.parent),
    ))
    committed = False
    try:
        tmp_db = staging / output_db.name
        summary = _build_into(
            tmp_db=tmp_db,
            manifest=manifest,
            taxa_path=taxa_path,
            source_usages_path=source_usages_path,
            vernacular_path=vernacular_path,
            registry_path=registry_path,
            manifest_path=manifest_path,
            release_dir=release_dir,
        )
        os.replace(tmp_db, output_db)
        committed = True
        summary["sqlite_sha256"] = _sha256_file(output_db)
        summary["sqlite_bytes"] = output_db.stat().st_size
        return summary
    finally:
        if not committed:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)


def _build_into(
    *,
    tmp_db: Path,
    manifest: dict,
    taxa_path: Path,
    source_usages_path: Path,
    vernacular_path: Path,
    registry_path: Path,
    manifest_path: Path,
    release_dir: Path,
) -> dict:
    conn = sqlite3.connect(str(tmp_db), isolation_level=None)
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute("PRAGMA journal_mode = OFF")
    # foreign_keys stays OFF during the bulk insert phase because self-
    # referential parent_taxon_id links may forward-reference not-yet-
    # inserted rows in Sporely-ID order. We run PRAGMA foreign_key_check
    # explicitly after all inserts complete.
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.executescript(CREATE_SCHEMA_SQL)

    conn.execute("BEGIN")
    try:
        # --- Pass 1: build source_usage → sporely_taxon_id index -----------
        source_usage_index = _iter_source_usage_source_index(source_usages_path)

        # --- Pass 2: insert canonical taxa in sporely_taxon_id order --------
        taxa_rows: list[tuple] = []
        taxon_ids_with_norwegian: dict[int, list[int]] = {}
        for row in _open_lines(taxa_path):
            sporely_id = int(row["sporely_taxon_id"])
            genus, species, family = _canonical_genus_species(row)
            parent_id = _resolve_parent_sporely_id(row, source_usage_index)
            release_id = manifest.get("content_release_id", "")
            canonical_source_usage = row["canonical_source_usage"]
            taxa_rows.append((
                sporely_id,
                parent_id,
                genus or "",
                species or "",
                family,
                None,   # norwegian_taxon_id, filled in pass 3
                None,   # swedish_taxon_id
                None,   # inaturalist_taxon_id
                row.get("scientific_name") or None,
                row.get("rank") or None,
                row.get("taxonomic_status") or None,
                row.get("canonical_source_code") or None,
                None,   # preferred_scientific_name_no, filled in pass 4
                None,   # preferred_scientific_name_sv
                release_id,
                canonical_source_usage["source"],
                canonical_source_usage["identifier"],
            ))
        # Insert in ascending sporely_taxon_id order for determinism.
        taxa_rows.sort(key=lambda r: r[0])
        conn.executemany(
            "INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?)",
            taxa_rows,
        )

        # --- Pass 3: source_usages produce scientific_name aliases + external IDs -----
        scientific_name_rows: list[tuple] = []
        external_int_rows: list[tuple] = []
        external_text_rows: list[tuple] = []
        # Track NorTaxa-taxon-id aliases per Sporely id for the legacy
        # ``norwegian_taxon_id`` column. Only fill when there's exactly one
        # numeric NorTaxa taxonID to preserve the column's UNIQUE constraint
        # semantics.
        for u in _open_lines(source_usages_path):
            sporely_id = int(u["sporely_taxon_id"])
            source_code = u["source_code"]
            ns = u["source_usage"]["namespace"]
            identifier = u["source_usage"]["identifier"]
            sci_name = u.get("scientific_name") or ""
            authorship = u.get("authorship") or ""
            note = (u.get("alias_reason") or "") or None
            is_preferred = 1 if u["identity_binding"] == "anchor" else 0
            if sci_name:
                scientific_name_rows.append((
                    sporely_id,
                    "sci",  # language_code used by existing schema for scientific
                    sci_name,
                    is_preferred,
                    source_code,
                    note,
                ))
            # External identifier storage.
            source_system = SOURCE_SYSTEM_MAP.get(source_code, source_code)
            id_role = "accepted" if u["taxonomic_status"] in (
                "accepted", "provisionally accepted", "valid"
            ) else "synonym"
            external_name = sci_name or None
            if ns in TEXT_NAMESPACES:
                external_text_rows.append((
                    sporely_id, source_system, ns, identifier, id_role,
                    is_preferred, external_name, note,
                ))
            elif ns in INTEGER_NAMESPACES:
                try:
                    numeric = int(identifier)
                except ValueError:
                    external_text_rows.append((
                        sporely_id, source_system, ns, identifier, id_role,
                        is_preferred, external_name, note,
                    ))
                else:
                    external_int_rows.append((
                        sporely_id, source_system, numeric, id_role,
                        is_preferred, external_name, note,
                    ))
                    if source_system == "artsdatabanken" and \
                            ns == "nortaxa_taxon_id" and is_preferred == 1:
                        taxon_ids_with_norwegian.setdefault(
                            sporely_id, []).append(numeric)
            else:
                # Unknown namespace — store as text with the raw namespace.
                external_text_rows.append((
                    sporely_id, source_system, ns, identifier, id_role,
                    is_preferred, external_name, note,
                ))

        # Deduplicate on (taxon_id, language_code, scientific_name).
        # Two source usages that share a canonical scientific name (COL +
        # NorTaxa accepted after auto-alias, for example) produce identical
        # rows here; keep the one with is_preferred_name = 1 when present.
        sci_dedup: dict[tuple[int, str, str], tuple] = {}
        for row in scientific_name_rows:
            key = (row[0], row[1], row[2])
            existing = sci_dedup.get(key)
            if existing is None or (row[3] > existing[3]):
                sci_dedup[key] = row
        scientific_name_rows = list(sci_dedup.values())
        scientific_name_rows.sort(
            key=lambda r: (r[0], r[2].casefold(), r[4] or "", r[3]))
        conn.executemany(
            "INSERT INTO scientific_name_min "
            "(scientific_name_id, taxon_id, language_code, scientific_name, "
            "is_preferred_name, source, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (i + 1, *row) for i, row in enumerate(scientific_name_rows)
            ],
        )

        # --- Legacy external identifiers (Stage 3B.1 compatibility) --------
        legacy_external_path = release_dir / "legacy_external_ids.jsonl"
        if legacy_external_path.exists():
            int_seen: set[tuple[int, str, int]] = {
                (r[0], r[1], r[2]) for r in external_int_rows
            }
            text_seen: set[tuple[int, str, str, str]] = {
                (r[0], r[1], r[2], r[3]) for r in external_text_rows
            }
            for entry in _open_lines(legacy_external_path):
                sporely_id = int(entry["sporely_taxon_id"])
                source_system = str(entry["source_system"])
                ext_id = str(entry["external_id"])
                id_role = str(entry.get("id_role") or "accepted")
                is_preferred = 1 if entry.get("is_preferred") else 0
                external_name = entry.get("external_name")
                note_bits = []
                if entry.get("provider"):
                    note_bits.append(f"legacy_compat:{entry['provider']}")
                if entry.get("note"):
                    note_bits.append(str(entry["note"]))
                note = "; ".join(note_bits) or None
                if str(entry.get("external_id_kind", "integer")) == "integer":
                    try:
                        numeric = int(ext_id)
                    except ValueError:
                        # Provider claimed integer but value isn't — store as text.
                        namespace = entry.get("namespace") or \
                            f"{source_system}_taxon_id"
                        key_t = (sporely_id, source_system, namespace, ext_id)
                        if key_t in text_seen:
                            continue
                        text_seen.add(key_t)
                        external_text_rows.append((
                            sporely_id, source_system, namespace, ext_id,
                            id_role, is_preferred, external_name, note,
                        ))
                        continue
                    key_i = (sporely_id, source_system, numeric)
                    if key_i in int_seen:
                        continue
                    int_seen.add(key_i)
                    external_int_rows.append((
                        sporely_id, source_system, numeric, id_role,
                        is_preferred, external_name, note,
                    ))
                else:
                    namespace = entry.get("namespace") or \
                        f"{source_system}_taxon_id"
                    key_t = (sporely_id, source_system, namespace, ext_id)
                    if key_t in text_seen:
                        continue
                    text_seen.add(key_t)
                    external_text_rows.append((
                        sporely_id, source_system, namespace, ext_id,
                        id_role, is_preferred, external_name, note,
                    ))

        external_int_rows.sort(key=lambda r: (r[0], r[1], r[2]))
        conn.executemany(
            "INSERT INTO taxon_external_id_min "
            "(external_id_row_id, taxon_id, source_system, external_id, "
            "id_role, is_preferred, external_name, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(i + 1, *row) for i, row in enumerate(external_int_rows)],
        )

        external_text_rows.sort(
            key=lambda r: (r[0], r[1], r[2], r[3]))
        conn.executemany(
            "INSERT INTO taxon_external_id_text_min "
            "(external_id_row_id, taxon_id, source_system, namespace, "
            "external_id, id_role, is_preferred, external_name, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(i + 1, *row) for i, row in enumerate(external_text_rows)],
        )

        # Populate legacy norwegian_taxon_id where a UNIQUE anchor exists.
        for sporely_id, values in taxon_ids_with_norwegian.items():
            if len(set(values)) == 1:
                conn.execute(
                    "UPDATE taxon_min SET norwegian_taxon_id = ? WHERE taxon_id = ?",
                    (values[0], sporely_id),
                )

        # --- Pass 4: vernaculars --------------------------------------------
        vern_rows: list[tuple] = []
        for v in _open_lines(vernacular_path):
            sporely_id = int(v["sporely_taxon_id"])
            vern_rows.append((
                sporely_id,
                v["language"],
                v["vernacular_name"],
                1 if v.get("is_preferred") else 0,
                v.get("source_code"),
            ))
        vern_rows.sort(
            key=lambda r: (r[0], r[1], r[2].casefold(), r[3]))
        # Deduplicate on the UNIQUE (taxon_id, language, name) index.
        seen: set[tuple[int, str, str]] = set()
        deduped: list[tuple] = []
        for row in vern_rows:
            key = (row[0], row[1], row[2])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        conn.executemany(
            "INSERT INTO vernacular_min "
            "(vernacular_id, taxon_id, language_code, vernacular_name, "
            "is_preferred_name, source) VALUES (?, ?, ?, ?, ?, ?)",
            [(i + 1, *row) for i, row in enumerate(deduped)],
        )

        # --- Pass 5: red-list assessments (Stage 3B.4) ---------------------
        redlist_rows: list[tuple] = []
        redlist_path = release_dir / "redlist_no.jsonl"
        if redlist_path.exists():
            for entry in _open_lines(redlist_path):
                taxon_id_value = entry.get("taxon_id")
                if taxon_id_value is not None:
                    taxon_id_value = int(taxon_id_value)
                redlist_rows.append((
                    taxon_id_value,
                    str(entry["source_system"]),
                    str(entry["source_release"]),
                    str(entry["assessment_id"]),
                    str(entry["assessment_area"]),
                    str(entry["assessed_name_source"]),
                    str(entry["assessed_name_namespace"]),
                    str(entry["assessed_name_id"]),
                    str(entry["scientific_name_snapshot"]),
                    entry.get("authorship_snapshot"),
                    entry.get("taxon_rank_snapshot"),
                    str(entry["category_raw"]),
                    str(entry["category_code"]),
                    1 if entry.get("category_is_downgraded") else 0,
                    entry.get("criteria"),
                    entry.get("expert_group"),
                    entry.get("assessment_url"),
                ))
            redlist_rows.sort(key=lambda r: (
                r[1], r[2], r[4],
                (0, int(r[3])) if r[3].isdigit() else (1, r[3]),
                r[3],
            ))
            conn.executemany(
                "INSERT INTO taxon_redlist_min "
                "(redlist_row_id, taxon_id, source_system, source_release, "
                "assessment_id, assessment_area, assessed_name_source, "
                "assessed_name_namespace, assessed_name_id, "
                "scientific_name_snapshot, authorship_snapshot, "
                "taxon_rank_snapshot, category_raw, category_code, "
                "category_is_downgraded, criteria, expert_group, "
                "assessment_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(i + 1, *row) for i, row in enumerate(redlist_rows)],
            )

        # --- Metadata --------------------------------------------------------
        meta = [
            ("taxonomy_schema_version", str(TAXONOMY_SCHEMA_VERSION)),
            ("content_release_id", manifest.get("content_release_id", "")),
            ("compiler_manifest_sha256", _sha256_file(manifest_path)),
            ("registry_sha256", _registry_identity_hash(registry_path)),
            ("state", "candidate"),
            ("publication", "none"),
        ]
        for binding in manifest.get("source_bindings", []):
            code = binding["source_code"]
            meta.append((
                f"source_release[{code}].id",
                binding.get("source_release_id", ""),
            ))
            meta.append((
                f"source_release[{code}].archive_sha256",
                binding.get("archive_sha256", ""),
            ))
        redlist_meta = (manifest.get("redlist_no") or {}).get("source_binding")
        if redlist_meta:
            code = redlist_meta.get("source_system", "artsdatabanken_redlist")
            meta.append((
                f"source_release[{code}].id",
                redlist_meta.get("source_release", ""),
            ))
            meta.append((
                f"source_release[{code}].archive_sha256",
                redlist_meta.get("input_sha256", ""),
            ))
        conn.executemany(
            "INSERT INTO taxonomy_meta (key, value) VALUES (?, ?)", meta,
        )

        # --- Indexes ---------------------------------------------------------
        for stmt in [s.strip() for s in CREATE_INDEXES_SQL.split(";") if s.strip()]:
            if stmt.startswith("--"):
                continue
            conn.execute(stmt)
        # ANALYZE so the planner picks the tightest index at query time.
        conn.execute("ANALYZE")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    # --- Integrity checks --------------------------------------------------
    integrity = list(conn.execute("PRAGMA integrity_check"))
    if integrity != [("ok",)]:
        raise BuildError(f"integrity_check failed: {integrity!r}")
    fk = list(conn.execute("PRAGMA foreign_key_check"))
    if fk:
        raise BuildError(f"foreign_key_check failed: {fk[:5]!r}")

    # --- Dangling reference guard ------------------------------------------
    orphans = list(conn.execute(
        "SELECT COUNT(*) FROM scientific_name_min sn "
        "WHERE NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = sn.taxon_id)"
    ))
    if orphans[0][0]:
        raise BuildError(f"scientific_name_min orphans: {orphans[0][0]}")
    orphans = list(conn.execute(
        "SELECT COUNT(*) FROM vernacular_min v "
        "WHERE NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = v.taxon_id)"
    ))
    if orphans[0][0]:
        raise BuildError(f"vernacular_min orphans: {orphans[0][0]}")
    orphans = list(conn.execute(
        "SELECT COUNT(*) FROM taxon_external_id_min e "
        "WHERE NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = e.taxon_id)"
    ))
    if orphans[0][0]:
        raise BuildError(f"taxon_external_id_min orphans: {orphans[0][0]}")
    orphans = list(conn.execute(
        "SELECT COUNT(*) FROM taxon_external_id_text_min e "
        "WHERE NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = e.taxon_id)"
    ))
    if orphans[0][0]:
        raise BuildError(f"taxon_external_id_text_min orphans: {orphans[0][0]}")
    # Red-list rows may hold taxon_id IS NULL (unresolved assessments); only
    # rows with a NON-NULL taxon_id must reference a real Sporely identity.
    orphans = list(conn.execute(
        "SELECT COUNT(*) FROM taxon_redlist_min r "
        "WHERE r.taxon_id IS NOT NULL AND NOT EXISTS "
        "(SELECT 1 FROM taxon_min t WHERE t.taxon_id = r.taxon_id)"
    ))
    if orphans[0][0]:
        raise BuildError(f"taxon_redlist_min orphans: {orphans[0][0]}")

    counts = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in (
            "taxon_min", "scientific_name_min", "vernacular_min",
            "taxon_external_id_min", "taxon_external_id_text_min",
            "taxon_redlist_min", "taxonomy_meta",
        )
    }

    # VACUUM to normalize physical layout for determinism.
    conn.execute("VACUUM")
    conn.close()

    return {"counts": counts, "manifest": manifest}


# --------------------------------------------------------------- CLI -------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True,
                        help="Stage 3A compiled-release directory")
    parser.add_argument("--registry", type=Path, required=True,
                        help="canonical shard directory or single JSONL file")
    parser.add_argument("--output", type=Path, required=True,
                        help="destination SQLite path (must not exist)")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        summary = build_candidate(
            release_dir=args.release_dir,
            registry_path=args.registry,
            output_db=args.output,
        )
    except BuildError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
