#!/usr/bin/env python3
# Read-only snapshot export specification. Never run against production.
# See w2d-input-snapshot-contract.md.
"""Specification-only export tool.

This file is a **specification**, not a production export utility. It
documents — via docstring and a ``--dry-run`` mode that prints the plan
without touching data — how a safe read-only anonymised export from a
local SQLite ``observations.sqlite3`` would be performed to produce a
Stage W2D reconciliation-input snapshot.

The script:

* refuses to run when ``--production`` is passed (hard-fail),
* refuses to connect to any Supabase / cloud endpoint,
* runs only in ``--dry-run`` mode, which walks the observation table
  read-only and emits the *plan* to stdout,
* never writes to the source database.

Everything else is documentation. If you want to run an actual export,
implement a separate tool (in another file) and follow the contract in
`../docs/w2d-input-snapshot-contract.md`. Do not add write logic to
this script.

Usage (spec / dry-run):

    python -m database.taxonomy.scripts.export_observations_snapshot \\
        --observations /path/to/observations.sqlite3 \\
        --output /tmp/snapshot-plan \\
        --dry-run

Refusal cases (hard-fail, exit non-zero):

* ``--production`` supplied — the tool refuses to write anywhere and
  exits 3;
* the observations DB argument is missing or points to a non-SQLite
  file — the tool exits 4;
* the output directory already contains a
  ``reconciliation-inputs.jsonl`` — the tool refuses to overwrite an
  existing snapshot and exits 5;
* the environment carries any variable prefixed ``SPORELY_CLOUD_`` or
  ``SUPABASE_`` — the tool refuses to run within a production shell
  and exits 6.

The tool NEVER reads password / token / key environment variables; the
prefix check is a safety measure against being invoked from an
operator's live session.

Field projection (spec):

    id                              -> observation_id (hashed with SHA-256 truncated
                                       to 16 hex chars; the plaintext id never leaves
                                       the local machine).
    sporely_taxon_id                -> RawSignal(sporely:sporely_taxon_id)
    artsdata_id                     -> RawSignal(nortaxa:nortaxa_taxon_id)
    artportalen_id                  -> RawSignal(artportalen:artportalen_taxon_id)
    inaturalist_taxon_id            -> RawSignal(inaturalist:inaturalist_taxon_id)
    inaturalist_id                  -> RawSignal (preserve_only)
    mushroomobserver_id             -> RawSignal (preserve_only)
    ai_selected_service +
        ai_selected_taxon_id        -> RawSignal via namespace_rules
    genus + species                 -> RawSignal (text-only)
    ai_selected_scientific_name     -> RawSignal (text-only)
    scientific_name_snapshot        -> RawSignal (text-only)
    common_name                     -> RawSignal (text-only)
    species_guess                   -> RawSignal (text-only)
    taxon_rank_snapshot             -> stored_rank on ReconciliationInput
    manual_identification_flag      -> derived from source_type/uncertain
    scientific_name_snapshot        -> stored_scientific_name
    common_name                     -> stored_vernacular_name

Fields explicitly PROJECTED OUT (never emitted):

    author, private_comment, open_comment (as prose), location, habitat,
    country_code, region_id, gps_latitude, gps_longitude, folder_path,
    citation, data_provider, created_at, updated_at, ai_state_json,
    red_list_categories_json, and every image / calibration column.

Refer to `w2d-input-snapshot-contract.md` §5 for the full PII list.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Iterable


# The columns the SPEC would read. Presented as a constant so a future
# implementer can code against it and this file remains the source of
# truth.
READ_COLUMNS: tuple[str, ...] = (
    "id",
    "sporely_taxon_id",
    "artsdata_id",
    "artportalen_id",
    "inaturalist_taxon_id",
    "inaturalist_id",
    "mushroomobserver_id",
    "ai_selected_service",
    "ai_selected_taxon_id",
    "ai_selected_scientific_name",
    "genus",
    "species",
    "common_name",
    "species_guess",
    "scientific_name_snapshot",
    "taxon_rank_snapshot",
    "uncertain",
    "source_type",
)


_FORBIDDEN_ENV_PREFIXES = ("SPORELY_CLOUD_", "SUPABASE_")


class ExportRefused(SystemExit):
    """Raised when the tool refuses to run.

    Uses SystemExit-derived so the exit code propagates cleanly and the
    caller cannot silently ignore the refusal.
    """


def _refuse(code: int, reason: str) -> None:
    print(f"REFUSED (exit {code}): {reason}", file=sys.stderr)
    raise ExportRefused(code)


def _guard_environment() -> None:
    for key in os.environ:
        for prefix in _FORBIDDEN_ENV_PREFIXES:
            if key.startswith(prefix):
                _refuse(
                    6,
                    f"environment variable {key!r} matches production prefix "
                    f"{prefix!r}; refuse to run inside a live shell",
                )


def _obfuscate(row_id: int | str) -> str:
    """Hash the internal id so plaintext values never leave the machine."""
    digest = hashlib.sha256(str(row_id).encode("utf-8")).hexdigest()
    return digest[:16]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Specification / dry-run only. Never writes.",
    )
    parser.add_argument("--observations", type=Path, required=False, default=None,
                        help="path to a LOCAL observations.sqlite3 (never a cloud dump)")
    parser.add_argument("--output", type=Path, required=False, default=None,
                        help="planned output directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the export plan and exit (this is the only supported mode)")
    parser.add_argument("--production", action="store_true",
                        help="explicit production flag; the tool refuses when set")
    return parser


def _print_plan(observations: Path | None, output: Path | None) -> None:
    plan = {
        "action": "SPEC_ONLY_dry_run",
        "columns_read": list(READ_COLUMNS),
        "forbidden_env_prefixes": list(_FORBIDDEN_ENV_PREFIXES),
        "notes": [
            "Read-only sqlite3 URI 'file:{path}?mode=ro'.",
            "One SELECT statement; no COMMIT; no ALTER; no CREATE.",
            "observation_id is sha256(row_id)[:16] — plaintext id never leaves the box.",
            "Fields marked forbidden in w2d-input-snapshot-contract.md §5 are dropped.",
            "Output writes only two files: reconciliation-inputs.jsonl and snapshot-manifest.json.",
            "The tool exits with code 3 when --production is set; the workflow is not implemented here.",
        ],
        "observations_argument": str(observations) if observations else None,
        "output_argument": str(output) if output else None,
        "sha256_chaining": {
            "inputs_sha256": "sha256(reconciliation-inputs.jsonl)",
            "recorded_in": "snapshot-manifest.json.inputs_sha256",
        },
        "spec_reference": "database/taxonomy/docs/w2d-input-snapshot-contract.md",
    }
    print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if args.production:
        _refuse(3, "--production is not supported; this file is specification-only")
    _guard_environment()
    if not args.dry_run:
        _refuse(
            2,
            "the tool is specification-only; only --dry-run is supported. "
            "See w2d-input-snapshot-contract.md.",
        )
    if args.observations is not None and not args.observations.exists():
        _refuse(4, f"observations DB not found: {args.observations}")
    if args.output is not None:
        inputs_path = args.output / "reconciliation-inputs.jsonl"
        if inputs_path.exists():
            _refuse(5, f"snapshot already exists: {inputs_path}")
    if args.observations is not None:
        # We DO open the file read-only just to sanity-check the schema.
        # We never write. We never SELECT the private columns listed in
        # the contract §5.
        try:
            conn = sqlite3.connect(f"file:{args.observations}?mode=ro", uri=True)
            try:
                cur = conn.execute("PRAGMA table_info(observations)")
                have = {(row[1] or "") for row in cur.fetchall()}
                missing = [col for col in READ_COLUMNS if col not in have]
                # Not an error — legacy DBs may not have every column. Just
                # emitted as part of the plan for auditors.
                extra_plan = {"missing_columns": sorted(missing)}
            finally:
                conn.close()
        except sqlite3.DatabaseError as exc:
            _refuse(4, f"invalid sqlite database: {exc}")
        _print_plan(args.observations, args.output)
        print(json.dumps({"schema_check": extra_plan}, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        _print_plan(None, args.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except ExportRefused as exc:
        raise SystemExit(int(exc.code) if isinstance(exc.code, int) else 1)
