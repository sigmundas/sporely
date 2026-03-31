#!/usr/bin/env python3
"""Run the full local taxonomy rebuild pipeline for Sporely."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TAXON_TXT = SCRIPT_DIR / "taxon.txt"
DEFAULT_VERNACULAR_TXT = SCRIPT_DIR / "vernacularname.txt"
DEFAULT_INAT_CSV = SCRIPT_DIR / "vernacular_inat_11lang.csv"
DEFAULT_BASE_DB = SCRIPT_DIR / "vernacular_multilanguage_legacy.sqlite3"
DEFAULT_ARTPORTALEN_MATCHED = SCRIPT_DIR / "artportalen_taxon_ids_by_genus.csv"
DEFAULT_ARTPORTALEN_SWEDISH_ONLY = SCRIPT_DIR / "artportalen_taxon_ids_swedish_only.csv"
DEFAULT_ARTPORTALEN_RECONCILED = SCRIPT_DIR / "artportalen_taxon_ids_swedish_only_reconciled.csv"
DEFAULT_OUTPUT_DB = SCRIPT_DIR / "vernacular_multilanguage.sqlite3"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild the Sporely taxonomy database from local source files."
    )
    parser.add_argument("--taxon-txt", type=Path, default=DEFAULT_TAXON_TXT)
    parser.add_argument("--vernacular-txt", type=Path, default=DEFAULT_VERNACULAR_TXT)
    parser.add_argument("--inat-csv", type=Path, default=DEFAULT_INAT_CSV)
    parser.add_argument("--base-db", type=Path, default=DEFAULT_BASE_DB)
    parser.add_argument("--artportalen-matched", type=Path, default=DEFAULT_ARTPORTALEN_MATCHED)
    parser.add_argument("--artportalen-swedish-only", type=Path, default=DEFAULT_ARTPORTALEN_SWEDISH_ONLY)
    parser.add_argument("--artportalen-reconciled", type=Path, default=DEFAULT_ARTPORTALEN_RECONCILED)
    parser.add_argument("--out-db", type=Path, default=DEFAULT_OUTPUT_DB)
    parser.add_argument("--cookie-json", type=Path, help="Artportalen cookie JSON for Swedish taxon harvesting.")
    parser.add_argument("--cookie-header", help="Raw Artportalen Cookie header.")
    parser.add_argument("--request-delay", type=float, default=0.05, help="Delay between iNaturalist requests.")
    parser.add_argument("--artportalen-pause-seconds", type=float, default=0.4)
    parser.add_argument("--overwrite", action="store_true", help="Rewrite generated files from scratch.")
    parser.add_argument("--skip-inat", action="store_true", help="Reuse the existing iNaturalist CSV.")
    parser.add_argument(
        "--skip-artportalen-fetch",
        action="store_true",
        help="Reuse the existing Artportalen matched/Swedish-only CSVs.",
    )
    parser.add_argument(
        "--skip-artportalen-reconcile",
        action="store_true",
        help="Reuse the existing reconciled Swedish-only CSV.",
    )
    return parser


def run_step(command: list[str]) -> None:
    print("$", " ".join(command), flush=True)
    subprocess.run(command, check=True)


def require_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"Missing {label}: {path}")


def main() -> None:
    args = build_arg_parser().parse_args()

    python = sys.executable
    taxon_txt = Path(args.taxon_txt).resolve()
    vernacular_txt = Path(args.vernacular_txt).resolve()
    inat_csv = Path(args.inat_csv).resolve()
    base_db = Path(args.base_db).resolve()
    matched_csv = Path(args.artportalen_matched).resolve()
    swedish_only_csv = Path(args.artportalen_swedish_only).resolve()
    reconciled_csv = Path(args.artportalen_reconciled).resolve()
    out_db = Path(args.out_db).resolve()

    require_exists(taxon_txt, "taxon.txt")
    require_exists(vernacular_txt, "vernacularname.txt")

    if not args.skip_inat:
        cmd = [
            python,
            str(SCRIPT_DIR / "inat_common_names_from_taxon.py"),
            "--taxon-file",
            str(taxon_txt),
            "--out-csv",
            str(inat_csv),
            "--request-delay",
            str(args.request_delay),
        ]
        if args.overwrite:
            cmd.append("--overwrite")
        run_step(cmd)
    require_exists(inat_csv, "iNaturalist vernacular CSV")

    run_step(
        [
            python,
            str(SCRIPT_DIR / "build_multilang_vernacular_db.py"),
            "--csv",
            str(inat_csv),
            "--out",
            str(base_db),
            "--no-taxon",
            str(taxon_txt),
            "--no-vernacular",
            str(vernacular_txt),
        ]
    )

    if not args.skip_artportalen_fetch:
        if not args.cookie_json and not args.cookie_header:
            raise SystemExit(
                "Artportalen fetch requires --cookie-json or --cookie-header, "
                "or use --skip-artportalen-fetch to reuse existing CSVs."
            )
        cmd = [
            python,
            str(SCRIPT_DIR / "fetch_artportalen_taxon_ids_by_genus.py"),
            "--source-db",
            str(base_db),
            "--output-csv",
            str(matched_csv),
            "--swedish-only-csv",
            str(swedish_only_csv),
            "--pause-seconds",
            str(args.artportalen_pause_seconds),
        ]
        if args.cookie_json:
            cmd.extend(["--cookie-json", str(Path(args.cookie_json).resolve())])
        if args.cookie_header:
            cmd.extend(["--cookie-header", args.cookie_header])
        if args.overwrite:
            cmd.append("--overwrite")
        run_step(cmd)
    require_exists(matched_csv, "Artportalen matched CSV")
    require_exists(swedish_only_csv, "Artportalen Swedish-only CSV")

    if not args.skip_artportalen_reconcile:
        cmd = [
            python,
            str(SCRIPT_DIR / "reconcile_artportalen_swedish_only.py"),
            "--input-csv",
            str(swedish_only_csv),
            "--taxon-txt",
            str(taxon_txt),
            "--local-db",
            str(base_db),
            "--output-csv",
            str(reconciled_csv),
        ]
        if args.overwrite:
            cmd.append("--overwrite")
        run_step(cmd)
    require_exists(reconciled_csv, "Artportalen reconciled CSV")

    run_step(
        [
            python,
            str(SCRIPT_DIR / "build_unified_multilang_taxonomy_db.py"),
            "--csv",
            str(inat_csv),
            "--out",
            str(out_db),
            "--no-taxon",
            str(taxon_txt),
            "--no-vernacular",
            str(vernacular_txt),
            "--artportalen-matched",
            str(matched_csv),
            "--artportalen-reconciled",
            str(reconciled_csv),
            "--inat-mapping-csv",
            str(inat_csv),
        ]
    )

    print(f"Unified taxonomy DB rebuilt: {out_db}", flush=True)


if __name__ == "__main__":
    main()
