#!/usr/bin/env python3
"""CLI wrapper for the Stage W1 cloud-taxonomy exporter.

Usage:

    python -m database.taxonomy.scripts.export_cloud_taxonomy \
        --artifact database/reference_data/generated/taxonomy_v2/tax-2026.07.30-02.sqlite3.gz \
        --manifest database/reference_data/generated/taxonomy_v2/manifest.json \
        --output   database/reference_data/generated/taxonomy_v2/cloud_export_tax-2026.07.30-02

If ``--artifact`` is omitted, the active artifact referenced by the outer
manifest (``gz_artifact`` field) is used, resolved relative to the manifest's
directory.
"""
from __future__ import annotations

import argparse
import json
import logging
import resource
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent  # database/taxonomy/scripts → repo root
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from database.taxonomy.cloud_export import (  # noqa: E402
    ExportError,
    ExportResult,
    MANIFEST_FILENAME,
    run_export,
)


_DEFAULT_MANIFEST = Path("database/reference_data/generated/taxonomy_v2/manifest.json")
_DEFAULT_POLICIES = Path("database/taxonomy/policies")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="export_cloud_taxonomy",
        description="Model-neutral cloud-taxonomy exporter (Stage W1).",
    )
    p.add_argument(
        "--artifact",
        type=Path,
        default=None,
        help="Path to the gzip'd compiled SQLite artifact. "
             "Defaults to <manifest_dir>/<manifest.gz_artifact>.",
    )
    p.add_argument(
        "--manifest",
        type=Path,
        default=_DEFAULT_MANIFEST,
        help=f"Path to the outer compiled manifest.json (default: {_DEFAULT_MANIFEST})",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: <manifest_dir>/cloud_export_<release_id>)",
    )
    p.add_argument(
        "--policies",
        type=Path,
        default=_DEFAULT_POLICIES,
        help=f"Policy directory to hash for provenance (default: {_DEFAULT_POLICIES})",
    )
    p.add_argument(
        "--replace",
        action="store_true",
        help="Replace an existing output directory that differs from the fresh output.",
    )
    p.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify source hashes and scope; do not write any output files.",
    )
    p.add_argument(
        "--no-compress",
        action="store_true",
        help="Reserved: compression is not enabled by default in W1.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-error stdout.",
    )
    return p.parse_args(argv)


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    manifest = args.manifest.resolve()
    if not manifest.is_file():
        raise ExportError(f"manifest not found: {manifest}")
    manifest_dir = manifest.parent

    if args.artifact is None:
        try:
            manifest_json = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ExportError(f"manifest malformed: {exc}") from exc
        gz_name = manifest_json.get("gz_artifact")
        if not gz_name:
            raise ExportError("manifest lacks `gz_artifact`; supply --artifact explicitly")
        artifact = (manifest_dir / gz_name).resolve()
    else:
        artifact = args.artifact.resolve()

    if args.output is None:
        try:
            manifest_json  # type: ignore[name-defined]
        except NameError:
            manifest_json = json.loads(manifest.read_text(encoding="utf-8"))
        release_id = manifest_json.get("content_release_id")
        if not release_id:
            raise ExportError("manifest lacks `content_release_id`; supply --output explicitly")
        output = (manifest_dir / f"cloud_export_{release_id}").resolve()
    else:
        output = args.output.resolve()

    policies = args.policies.resolve()
    return artifact, manifest, output, policies


def _print_result(result: ExportResult, elapsed: float, peak_kb: int, quiet: bool) -> None:
    if quiet:
        return
    print(f"Cloud export written to: {result.output_dir}")
    print(f"  content_release_id  : {result.output_dir.name}")
    print(f"  whole_export_sha256 : {result.whole_export_sha256}")
    print(f"  manifest_sha256     : {result.manifest_sha256}")
    print(f"  concepts_included   : {len(result.scope.concept_ids)}")
    print(f"  concepts_excluded   : {result.scope.excluded_count}")
    print(f"  elapsed             : {elapsed:.2f}s")
    print(f"  peak_rss_kb (self)  : {peak_kb}")
    print(f"  files:")
    for name, ds in result.datasets.items():
        print(
            f"    {name:<30}  rows={ds.row_count:>7}  bytes={ds.bytes:>10}  sha256={ds.sha256}"
        )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        artifact, manifest, output, policies = _resolve_paths(args)
        start = time.monotonic()
        result = run_export(
            artifact_gz=artifact,
            manifest=manifest,
            output_dir=output,
            policy_dir=policies,
            replace=args.replace,
            verify_only=args.verify_only,
        )
        elapsed = time.monotonic() - start
        # ru_maxrss is kilobytes on Linux, bytes on macOS. Report raw.
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        _print_result(result, elapsed, peak, args.quiet)
        return 0
    except ExportError as exc:
        print(f"export failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - unexpected paths
        print(f"unexpected error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
