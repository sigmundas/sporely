"""Prepare or atomically apply historical literature reuse; never sync to cloud.

Preparation is read-only. Apply requires a verified SQLite backup directory.
The existing importer runs on a disposable copy first; only new normalized
entities are inserted into the live library, in one transaction. Original
rows, observations, plot state and existing normalized entities are untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database import schema
from database.reference_library_schema import register_measurement_contract
from tools.migrate_legacy_reference_values import run_migration, validate_manifest

TABLES = ("reference_works", "reference_taxon_treatments", "reference_measurement_sets")


def readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def row_digest(rows: list[dict]) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def prepare(database: Path) -> dict:
    """One work per exact source label; never infer bibliography or taxon IDs."""
    with readonly(database) as connection:
        legacy = [dict(row) for row in connection.execute("SELECT * FROM reference_values ORDER BY id")]
        linked = {row[0] for row in connection.execute(
            "SELECT legacy_reference_value_id FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL")}
        works = [dict(row) for row in connection.execute("SELECT * FROM reference_works")]
    result = {"manifest_version": 1, "database_path": str(database.resolve()),
              "works": [], "rows": [], "excluded_ids": []}
    sources: dict[str, str] = {}
    selected = []
    for row in legacy:
        source = str(row.get("source") or "").strip()
        try:
            metadata = json.loads(row.get("metadata_json") or "{}")
        except (ValueError, TypeError):
            metadata = None
        # Do not turn cloud/personal observations or unclassified sources into
        # literature. Malformed provenance also requires a separate decision.
        if (not source or metadata is None or not isinstance(metadata, dict)
                or source.casefold().startswith(("cloud:", "my data", "my observation"))
                or metadata.get("cloud_dataset_kind") == "observation"
                or metadata.get("source_kind") == "observation"):
            result["excluded_ids"].append(row["id"])
            continue
        selected.append(row)
        if row["id"] in linked:
            result["rows"].append({"legacy_id": row["id"], "action": "already_migrated"})
            continue
        if not row.get("genus") or not row.get("species"):
            raise ValueError(f"legacy row {row['id']} has no complete published name")
        if source not in sources:
            key = "legacy-source-" + hashlib.sha256(source.encode()).hexdigest()
            sources[source] = key
            # Reuse only an explicit original citation/key, not a generic title
            # that could designate another edition or publication.
            matches = [work for work in works if work.get("citation_key") == key
                       or work.get("citation_override") == source]
            if len(matches) > 1:
                raise ValueError(f"ambiguous existing publication for {source!r}")
            year = re.search(r"\b([12][0-9]{3})\)?$", source)
            work = {"work_key": key, "citation_key": key, "type": "other",
                    "title": source, "short_label": source, "citation_override": source,
                    "year": int(year[1]) if year else None, "authors": [], "editors": []}
            if matches:
                work["existing_work_id"] = matches[0]["id"]
            result["works"].append(work)
        result["rows"].append({
            "legacy_id": row["id"], "action": "migrate", "work_key": sources[source],
            "treatment": {"name_as_published": f"{row['genus']} {row['species']}", "taxon_id": None},
            "measurement_set": {"data_kind": "summary" if any(
                row.get(metric + "_avg") is not None for metric in ("length", "width", "q")) else "range"},
        })
    result["source_rows_sha256"] = row_digest(selected)
    return result


def apply(database: Path, manifest: dict, backup_dir: Path) -> dict:
    """Validate on a copy, then publish only additions atomically and idempotently."""
    backup = backup_dir / "reference_values.db"
    backup_manifest = json.loads((backup_dir / "manifest.json").read_text())
    if hashlib.sha256(backup.read_bytes()).hexdigest() != backup_manifest["reference_values.db"]["sha256"]:
        raise ValueError("backup checksum mismatch")
    with readonly(backup) as connection:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("backup integrity check failed")
    validated = validate_manifest(manifest)
    with tempfile.TemporaryDirectory(prefix="sporely-literature-") as folder:
        scratch = Path(folder) / "reference_values.db"
        with readonly(database) as source, sqlite3.connect(scratch) as destination:
            source.backup(destination)
        with readonly(scratch) as connection:
            before_rows = {table: [dict(row) for row in connection.execute(
                f"SELECT * FROM {table} ORDER BY id")] for table in TABLES}
            before = {table: {row["id"] for row in rows} for table, rows in before_rows.items()}
        with patch.object(schema, "get_reference_database_path", lambda: scratch):
            report = run_migration(validated, database_path=scratch, dry_run=False)
        if report.failed or report.unresolved:
            raise ValueError(f"migration validation failed: {report.failed}; unresolved={report.unresolved}")
        with readonly(scratch) as connection:
            additions = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")
                                 if row["id"] not in before[table]] for table in TABLES}
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        register_measurement_contract(connection)
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            ids = [entry["legacy_id"] for entry in manifest["rows"]]
            current = [dict(row) for row in connection.execute("SELECT * FROM reference_values ORDER BY id")
                       if row["id"] in ids]
            if row_digest(current) != manifest["source_rows_sha256"]:
                raise ValueError("source rows changed since preparation; prepare again")
            # The candidate was built from these exact parents. Stop on any
            # concurrent normalized-library change instead of overwriting it.
            for table in TABLES:
                current_rows = [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                if row_digest(current_rows) != row_digest(before_rows[table]):
                    raise ValueError("normalized library changed during validation")
                for row in additions[table]:
                    columns = list(row)
                    connection.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES "
                                       f"({','.join('?' for _ in columns)})", tuple(row.values()))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    return {"created": len(report.created), "reused": len(report.reused),
            "excluded": len(manifest.get("excluded_ids", []))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--prepare", type=Path)
    action.add_argument("--apply", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args()
    if args.prepare:
        manifest = prepare(args.database)
        args.prepare.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({"selected": len(manifest["rows"]), "excluded": len(manifest["excluded_ids"])}))
    else:
        if not args.backup_dir:
            parser.error("--apply requires --backup-dir with verified SQLite backups")
        print(json.dumps(apply(args.database, json.loads(args.apply.read_text()), args.backup_dir)))


if __name__ == "__main__":
    main()
