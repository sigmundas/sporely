#!/usr/bin/env python3
"""Offline rebuild orchestrator for the local Sporely taxonomy database.

This script intentionally does *not* fetch new data from iNaturalist,
Artsdatabanken, GBIF, or Artportalen. It only combines already-downloaded
source/generated files into the final SQLite database.

Default layout, relative to the repository root:

    database/reference_data/sources/taxon.txt
    database/reference_data/sources/vernacularname.txt
    database/reference_data/generated/vernacular_inat_11lang.csv
    database/reference_data/generated/artportalen_taxon_ids_by_genus.csv
    database/reference_data/generated/artportalen_taxon_ids_swedish_only_reconciled.csv
    database/reference_data/generated/vernacular_multilanguage.sqlite3

Typical use:

    python database/rebuild_taxonomy_db.py

If you do not have Artportalen CSVs yet and want a Norway/iNat-only DB:

    python database/rebuild_taxonomy_db.py --without-artportalen

If the iNaturalist CSV is intentionally absent:

    python database/rebuild_taxonomy_db.py --allow-missing-inat

The actual schema/data merge is still delegated to
``build_unified_multilang_taxonomy_db.py`` so this file stays small and easy to
review. This script mainly enforces clean source paths, no accidental network
refreshes, and post-build sanity checks.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Iterable, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

try:
    from database.reference_data_paths import (  # type: ignore
        REFERENCE_DATA_GENERATED_DIR,
        REFERENCE_DATA_SOURCES_DIR,
    )
except Exception:  # pragma: no cover - fallback for direct/script relocation
    REFERENCE_DATA_SOURCES_DIR = SCRIPT_DIR / "reference_data" / "sources"
    REFERENCE_DATA_GENERATED_DIR = SCRIPT_DIR / "reference_data" / "generated"


DEFAULT_TAXON_TXT = REFERENCE_DATA_SOURCES_DIR / "taxon.txt"
DEFAULT_VERNACULAR_TXT = REFERENCE_DATA_SOURCES_DIR / "vernacularname.txt"
DEFAULT_INAT_CSV = REFERENCE_DATA_GENERATED_DIR / "vernacular_inat_11lang.csv"
DEFAULT_ARTPORTALEN_MATCHED = REFERENCE_DATA_GENERATED_DIR / "artportalen_taxon_ids_by_genus.csv"
DEFAULT_ARTPORTALEN_RECONCILED = (
    REFERENCE_DATA_GENERATED_DIR / "artportalen_taxon_ids_swedish_only_reconciled.csv"
)
DEFAULT_OUTPUT_DB = REFERENCE_DATA_GENERATED_DIR / "vernacular_multilanguage.sqlite3"
DEFAULT_BUILDER = SCRIPT_DIR / "build_unified_multilang_taxonomy_db.py"


EMPTY_ARTPORTALEN_MATCHED_FIELDS = [
    "adb_taxon_id",
    "artportalen_taxon_id",
    "scientific_name",
    "matched_scientific_name",
    "swedish_name",
    "match_note",
]

EMPTY_ARTPORTALEN_RECONCILED_FIELDS = [
    "artportalen_taxon_id",
    "scientific_name",
    "swedish_name",
    "norway_match_status",
    "norway_taxon_id",
    "norway_accepted_taxon_id",
    "norway_accepted_scientific_name",
    "note",
]

EMPTY_INAT_FIELDS = [
    "scientificName",
    "inaturalist_taxon_id",
    "en",
    "de",
    "fr",
    "es",
    "da",
    "sv",
    "no",
    "fi",
    "pl",
    "pt",
    "it",
]


class RebuildError(SystemExit):
    """Expected command-line validation/build failure."""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the local multilingual taxonomy SQLite DB from already-downloaded "
            "Nortaxa, iNaturalist, and optional Artportalen generated files."
        )
    )
    parser.add_argument("--taxon-txt", type=Path, default=DEFAULT_TAXON_TXT)
    parser.add_argument("--vernacular-txt", type=Path, default=DEFAULT_VERNACULAR_TXT)
    parser.add_argument("--inat-csv", type=Path, default=DEFAULT_INAT_CSV)
    parser.add_argument("--artportalen-matched", type=Path, default=DEFAULT_ARTPORTALEN_MATCHED)
    parser.add_argument("--artportalen-reconciled", type=Path, default=DEFAULT_ARTPORTALEN_RECONCILED)
    parser.add_argument("--out-db", type=Path, default=DEFAULT_OUTPUT_DB)
    parser.add_argument("--builder", type=Path, default=DEFAULT_BUILDER)
    parser.add_argument(
        "--without-artportalen",
        action="store_true",
        help=(
            "Build without Artportalen Swedish-ID overlays. This creates temporary empty "
            "CSV inputs for the current unified builder."
        ),
    )
    parser.add_argument(
        "--allow-missing-inat",
        action="store_true",
        help=(
            "Allow the iNaturalist CSV to be missing. The DB will still be built from "
            "Artsdatabanken/Nortaxa sources, but without iNaturalist IDs/names."
        ),
    )
    parser.add_argument(
        "--skip-sanity-checks",
        action="store_true",
        help="Skip post-build SQLite sanity checks.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the command that would be run, but do not build the DB.",
    )
    return parser


def resolve_existing(path: Path) -> Path:
    return Path(path).expanduser().resolve()


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise RebuildError(f"Missing {label}: {path}")
    if not path.is_file():
        raise RebuildError(f"Expected {label} to be a file: {path}")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_empty_csv(path: Path, fieldnames: Sequence[str]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()


def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            return [str(item or "").strip() for item in next(reader)]
        except StopIteration:
            return []


def validate_inat_csv(path: Path, allow_missing: bool) -> Path | None:
    if not path.exists():
        if allow_missing:
            print(f"iNaturalist CSV not found; continuing without it: {path}", flush=True)
            return None
        raise RebuildError(
            f"Missing iNaturalist CSV: {path}\n"
            "Run the iNaturalist refresh script first, or pass --allow-missing-inat."
        )

    header = {name.lower() for name in read_csv_header(path)}
    if "scientificname" not in header and "scientific_name" not in header and "name" not in header:
        raise RebuildError(
            f"Unsupported iNaturalist CSV header in {path}. "
            "Expected scientificName, scientific_name, or name."
        )
    if "inaturalist_taxon_id" not in header:
        print(
            f"WARNING: {path} has no inaturalist_taxon_id column; "
            "the DB can build, but iNaturalist IDs will be empty.",
            flush=True,
        )
    return path


def duplicate_scientific_names(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return 0
        fields = {field.lower(): field for field in reader.fieldnames if field}
        sci_field = (
            fields.get("scientificname")
            or fields.get("scientific_name")
            or fields.get("name")
        )
        if not sci_field:
            return 0
        counts: Counter[str] = Counter()
        for row in reader:
            name = str(row.get(sci_field) or "").strip().casefold()
            if name:
                counts[name] += 1
        return sum(1 for count in counts.values() if count > 1)


def sqlite_scalar(conn: sqlite3.Connection, sql: str, params: Iterable[object] = ()) -> int:
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def run_sanity_checks(out_db: Path, inat_csv: Path | None) -> None:
    if not out_db.exists():
        raise RebuildError(f"Output DB was not created: {out_db}")

    conn = sqlite3.connect(out_db)
    try:
        print("\nSanity checks", flush=True)

        taxon_count = sqlite_scalar(conn, "SELECT COUNT(*) FROM taxon_min")
        vernacular_count = sqlite_scalar(conn, "SELECT COUNT(*) FROM vernacular_min")
        print(f"  taxon_min rows: {taxon_count}", flush=True)
        print(f"  vernacular_min rows: {vernacular_count}", flush=True)

        if taxon_count <= 0:
            raise RebuildError("Sanity check failed: taxon_min is empty.")
        if vernacular_count <= 0:
            raise RebuildError("Sanity check failed: vernacular_min is empty.")

        if table_exists(conn, "scientific_name_min"):
            scientific_count = sqlite_scalar(conn, "SELECT COUNT(*) FROM scientific_name_min")
            print(f"  scientific_name_min rows: {scientific_count}", flush=True)

        if table_exists(conn, "taxon_external_id_min"):
            external_count = sqlite_scalar(conn, "SELECT COUNT(*) FROM taxon_external_id_min")
            print(f"  taxon_external_id_min rows: {external_count}", flush=True)

        if table_exists(conn, "taxon_redlist_min"):
            redlist_count = sqlite_scalar(conn, "SELECT COUNT(*) FROM taxon_redlist_min")
            print(f"  taxon_redlist_min rows: {redlist_count}", flush=True)

        languages = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT language_code FROM vernacular_min ORDER BY language_code"
            ).fetchall()
            if row[0]
        ]
        print(f"  vernacular languages: {', '.join(languages) if languages else '(none)'}", flush=True)

        norwegian_count = sqlite_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM vernacular_min
            WHERE language_code IN ('no', 'nb', 'nb-NO', 'nn', 'nn-NO', 'se', 'se-NO')
            """,
        )
        print(f"  Norwegian/Sámi vernacular rows: {norwegian_count}", flush=True)
        if norwegian_count <= 0:
            raise RebuildError(
                "Sanity check failed: no Norwegian/Sámi vernacular rows found."
            )

        if column_exists(conn, "taxon_min", "inaturalist_taxon_id"):
            inat_rows = sqlite_scalar(
                conn,
                "SELECT COUNT(*) FROM taxon_min WHERE inaturalist_taxon_id IS NOT NULL",
            )
            print(f"  rows with iNaturalist IDs: {inat_rows}", flush=True)

        if column_exists(conn, "taxon_min", "swedish_taxon_id"):
            sv_rows = sqlite_scalar(
                conn,
                "SELECT COUNT(*) FROM taxon_min WHERE swedish_taxon_id IS NOT NULL",
            )
            print(f"  rows with Swedish Artportalen IDs: {sv_rows}", flush=True)

        duplicate_count = duplicate_scientific_names(inat_csv)
        if duplicate_count:
            print(
                f"  WARNING: iNaturalist CSV has {duplicate_count} duplicate scientificName values.",
                flush=True,
            )
    finally:
        conn.close()


def run_command(command: Sequence[str], dry_run: bool) -> None:
    print("$ " + " ".join(command), flush=True)
    if dry_run:
        return
    subprocess.run(list(command), check=True)


def main() -> None:
    args = build_arg_parser().parse_args()

    taxon_txt = resolve_existing(args.taxon_txt)
    vernacular_txt = resolve_existing(args.vernacular_txt)
    inat_csv = resolve_existing(args.inat_csv)
    artportalen_matched = resolve_existing(args.artportalen_matched)
    artportalen_reconciled = resolve_existing(args.artportalen_reconciled)
    out_db = resolve_existing(args.out_db)
    builder = resolve_existing(args.builder)

    require_file(taxon_txt, "Nortaxa taxon.txt")
    require_file(vernacular_txt, "Nortaxa vernacularname.txt")
    require_file(builder, "unified taxonomy builder")
    ensure_parent_dir(out_db)

    inat_map = validate_inat_csv(inat_csv, allow_missing=bool(args.allow_missing_inat))

    temp_dir_obj: tempfile.TemporaryDirectory[str] | None = None
    try:
        if args.without_artportalen or inat_map is None:
            temp_dir_obj = tempfile.TemporaryDirectory(prefix="sporely-taxonomy-rebuild-")
            temp_dir = Path(temp_dir_obj.name)

        if inat_map is None:
            inat_csv = temp_dir / "empty_vernacular_inat_11lang.csv"
            write_empty_csv(inat_csv, EMPTY_INAT_FIELDS)
            print("Building without iNaturalist common-name/ID CSV.", flush=True)

        if args.without_artportalen:
            artportalen_matched = temp_dir / "artportalen_taxon_ids_by_genus.csv"
            artportalen_reconciled = temp_dir / "artportalen_taxon_ids_swedish_only_reconciled.csv"
            write_empty_csv(artportalen_matched, EMPTY_ARTPORTALEN_MATCHED_FIELDS)
            write_empty_csv(artportalen_reconciled, EMPTY_ARTPORTALEN_RECONCILED_FIELDS)
            print("Building without Artportalen overlays.", flush=True)
        else:
            require_file(artportalen_matched, "Artportalen matched CSV")
            require_file(artportalen_reconciled, "Artportalen reconciled CSV")

        command = [
            sys.executable,
            str(builder),
            "--csv",
            str(inat_csv),
            "--out",
            str(out_db),
            "--no-taxon",
            str(taxon_txt),
            "--no-vernacular",
            str(vernacular_txt),
            "--artportalen-matched",
            str(artportalen_matched),
            "--artportalen-reconciled",
            str(artportalen_reconciled),
        ]
        if inat_map is not None:
            command.extend(["--inat-map", str(inat_map)])

        run_command(command, dry_run=bool(args.dry_run))

        if not args.dry_run and not args.skip_sanity_checks:
            run_sanity_checks(out_db, inat_map)

        if not args.dry_run:
            print(f"\nUnified taxonomy DB rebuilt: {out_db}", flush=True)
    finally:
        if temp_dir_obj is not None:
            temp_dir_obj.cleanup()


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.returncode) from exc
