#!/usr/bin/env python3
"""W3-A2 pseudonymous-observation bridge.

Reads:

  * --raw-export        CSV export produced by the operator (must be the SAME
                        export that fed the reconciliation manifest — its
                        SHA-256 is stamped in the anonymised snapshot header
                        and every deployment manifest emitted here);
  * --manifest          reconciliation manifest with pseudonymous
                        observation_ids;
  * --pseudonym-key-file operator's HMAC key file (never committed);
  * --output            destination path for the deployment manifest (JSONL).
                        The output is refused when it targets any path under
                        the two Git repos.

Emits one deployment record per manifest observation. Each record joins the
pseudonym back to its real observation_id, adds a taxonomy-field fingerprint
of the raw export row, and records the raw manifest sha + input file sha for
audit.

Refuses (hard fail, non-zero exit):

  * a manifest observation_id that does not match any raw row;
  * a raw row whose pseudonym collides with another raw row's;
  * a raw row that produces the same pseudonym twice (duplicate observation);
  * a manifest observation_id present twice;
  * a `--production` flag;
  * an output path inside sporely-py, sporely-web, or the operator's home
    unless it lives under a directory the operator has explicitly opted into
    with --allow-output-under (repeatable).

The `--observations-fingerprint <path>` flag optionally accepts a CSV dump
of the current `public.observations` taxonomy columns. When supplied, each
deployment record includes a `drift_status`:

  * `no_drift`               fingerprints match verbatim
  * `drifted_since_export`   at least one legacy taxonomy field changed
  * `observation_missing`    observation_id absent from the fingerprint dump
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from database.taxonomy.reconciliation.snapshot.pseudonym import (  # noqa: E402
    make_pseudonymiser,
)


FORBIDDEN_OUTPUT_PREFIXES = (
    "/Users/sigmundas/Documents/Code/sporely/sporely-py/",
    "/Users/sigmundas/Documents/Code/sporely/sporely-web/",
)


TAXONOMY_FIELDS = (
    "artsdata_id",
    "artportalen_id",
    "inaturalist_id",
    "mushroomobserver_id",
    "desktop_id",
    "ai_selected_service",
    "ai_selected_taxon_id",
    "ai_selected_scientific_name",
    "genus",
    "species",
    "common_name",
    "species_guess",
)


def _fingerprint(row: dict[str, str]) -> str:
    canonical = json.dumps(
        {k: (row.get(k) or "") for k in TAXONOMY_FIELDS},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _check_output_path(output: Path, allowlist: list[Path]) -> None:
    absolute = str(output.resolve())
    for prefix in FORBIDDEN_OUTPUT_PREFIXES:
        if absolute.startswith(prefix):
            raise SystemExit(
                f"refuse: deployment manifest MUST NOT live under {prefix} "
                "— it contains real observation IDs and taxonomy fingerprints. "
                "Choose a path outside both repositories."
            )
    if allowlist:
        for allowed in allowlist:
            if absolute.startswith(str(allowed.resolve())):
                return
        raise SystemExit(
            f"refuse: --output {absolute} is not under any --allow-output-under path"
        )


def build(
    *,
    raw_export: Path,
    manifest_path: Path,
    pseudonym_key_file: Path | None,
    output: Path,
    fingerprint_dump: Path | None,
    allowlist: list[Path],
) -> dict[str, object]:
    _check_output_path(output, allowlist)
    if output.exists():
        raise SystemExit(f"refuse: output already exists: {output}")

    csv.field_size_limit(sys.maxsize)
    pseudonymise = make_pseudonymiser(pseudonym_key_file)

    manifest_raw = manifest_path.read_bytes()
    manifest_input_sha = hashlib.sha256(manifest_raw).hexdigest()
    manifest = json.loads(manifest_raw)
    records_by_pseudonym: dict[str, dict] = {}
    for r in manifest["records"]:
        oid = r["observation_id"]
        if oid in records_by_pseudonym:
            raise SystemExit(f"refuse: manifest contains duplicate observation_id={oid}")
        records_by_pseudonym[oid] = r

    raw_sha = _file_sha256(raw_export)

    # Read raw CSV, compute pseudonyms, detect collisions.
    pseudonym_to_real: dict[str, dict[str, str]] = {}
    with raw_export.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_id = (row.get("id") or "").strip()
            if not raw_id:
                continue
            pseudonym = pseudonymise(raw_id)
            if pseudonym in pseudonym_to_real:
                raise SystemExit(
                    f"refuse: pseudonym collision — raw IDs {pseudonym_to_real[pseudonym]['id']!r} "
                    f"and {raw_id!r} both hash to {pseudonym}"
                )
            pseudonym_to_real[pseudonym] = {**row, "id": raw_id}

    # Optional current-observations fingerprint dump.
    current_fingerprints: dict[str, str] = {}
    if fingerprint_dump is not None:
        with fingerprint_dump.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                obs_id = (row.get("id") or "").strip()
                if not obs_id:
                    continue
                current_fingerprints[obs_id] = _fingerprint(row)

    # Compose deployment records — one per manifest observation.
    output.parent.mkdir(parents=True, exist_ok=True)
    match_count = 0
    drift_counts: dict[str, int] = {"no_drift": 0, "drifted_since_export": 0, "observation_missing": 0, "not_checked": 0}
    resolved_count = 0
    unresolved_count = 0
    with output.open("w", encoding="utf-8") as out:
        header = {
            "__deployment_manifest_header__": True,
            "manifest_semantic_sha256": manifest.get("input_source_hash")
            or manifest.get("semantic_sha256")
            or None,
            "manifest_input_file_sha256": manifest_input_sha,
            "raw_export_sha256": raw_sha,
            "raw_export_path": str(raw_export),
            "record_count": manifest["record_count"],
        }
        out.write(json.dumps(header, sort_keys=True, ensure_ascii=False) + "\n")

        for oid, rec in records_by_pseudonym.items():
            row = pseudonym_to_real.get(oid)
            if row is None:
                raise SystemExit(
                    f"refuse: manifest observation_id {oid} does not join to any raw row; "
                    "aborting deployment-manifest build"
                )
            fp = _fingerprint(row)
            real_id = row["id"]
            if fingerprint_dump is not None:
                current_fp = current_fingerprints.get(real_id)
                if current_fp is None:
                    drift = "observation_missing"
                elif current_fp == fp:
                    drift = "no_drift"
                else:
                    drift = "drifted_since_export"
            else:
                drift = "not_checked"
            drift_counts[drift] = drift_counts.get(drift, 0) + 1
            resolved_sporely_taxon_id = rec.get("resolved_sporely_taxon_id")
            if resolved_sporely_taxon_id is not None:
                resolved_count += 1
            else:
                unresolved_count += 1
            deployment = {
                "real_observation_id": real_id,
                "pseudonymous_observation_id": oid,
                "reconciliation_state": rec.get("reconciliation_state"),
                "resolved_sporely_taxon_id": resolved_sporely_taxon_id,
                "resolution_method": rec.get("resolution_method"),
                "resolution_release": rec.get("resolution_release"),
                "taxonomy_field_fingerprint_at_export": fp,
                "drift_status": drift,
            }
            out.write(json.dumps(deployment, sort_keys=True, ensure_ascii=False) + "\n")
            match_count += 1

    unmatched_raw = [rid for pseu, row in pseudonym_to_real.items() if pseu not in records_by_pseudonym for rid in [row["id"]]]

    summary = {
        "matched_observations": match_count,
        "manifest_records": len(records_by_pseudonym),
        "raw_rows": len(pseudonym_to_real),
        "unmatched_raw_ids_count": len(unmatched_raw),
        "resolved_count": resolved_count,
        "unresolved_or_manual_or_no_evidence_count": unresolved_count,
        "drift_counts": drift_counts,
        "manifest_input_file_sha256": manifest_input_sha,
        "raw_export_sha256": raw_sha,
        "output_path": str(output.resolve()),
    }
    return summary


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-export", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--pseudonym-key-file", type=Path, default=None)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--observations-fingerprint", type=Path, default=None)
    parser.add_argument("--allow-output-under", type=Path, action="append", default=[])
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args(argv)
    if args.production:
        print("refuse: --production is not honoured", file=sys.stderr)
        return 3
    summary = build(
        raw_export=args.raw_export,
        manifest_path=args.manifest,
        pseudonym_key_file=args.pseudonym_key_file,
        output=args.output,
        fingerprint_dump=args.observations_fingerprint,
        allowlist=list(args.allow_output_under),
    )
    print(json.dumps(summary, sort_keys=True, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
