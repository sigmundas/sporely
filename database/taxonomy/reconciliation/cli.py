"""CLI entry point for the W2D reconciliation engine.

Usage:

    python -m database.taxonomy.reconciliation.cli \\
        --input database/taxonomy/reconciliation/fixtures/all_states.jsonl \\
        --output /tmp/w2d-run-1 \\
        --release-dir database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01 \\
        --policy database/taxonomy/policies/w2d-reconciliation-policy.json

The CLI is deterministic — the same inputs + policy + release must produce
byte-identical outputs across runs. It refuses to overwrite an existing
manifest that would result in a different semantic hash without an
explicit ``--force`` flag.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Iterable, Iterator

from database.taxonomy.reconciliation.input_model import ReconciliationInput
from database.taxonomy.reconciliation.manifest import (
    build_manifest_body,
    write_manifest,
)
from database.taxonomy.reconciliation.namespace_rules import load_policy
from database.taxonomy.reconciliation.resolver import Resolver
from database.taxonomy.reconciliation.sources import PinnedRelease


logger = logging.getLogger(__name__)


def _iter_input(path: Path) -> Iterator[ReconciliationInput]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            row = json.loads(stripped)
            if row.get("__synthetic__") or row.get("__snapshot_header__"):
                # Sentinel / snapshot-header record consumed by the loader
                # and dropped from the input stream. See fixtures README
                # and the W2D-R input contract.
                continue
            yield ReconciliationInput.from_dict(row)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True,
                        help="JSONL file of ReconciliationInput records")
    parser.add_argument("--output", type=Path, required=True,
                        help="output directory (created if missing)")
    parser.add_argument("--release-dir", type=Path, required=True,
                        help="pinned macrofungi release directory")
    parser.add_argument("--policy", type=Path, required=True,
                        help="W2D reconciliation policy JSON")
    parser.add_argument("--canonical-registry", type=Path, action="append", default=None,
                        help="base canonical identity registry (JSONL or shard dir); may be given once")
    parser.add_argument("--supplement", type=Path, action="append", default=[],
                        help="registry supplement directory (contains canonical/ and release/); repeatable and validated for artifact_kind, base_release match, depends_on hashes, and topological order via database.taxonomy.reconciliation.supplement_loader")
    parser.add_argument("--no-summary", action="store_true",
                        help="do not emit the Markdown summary")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    _configure_logging(args.verbose)

    rule_set = load_policy(args.policy)
    # Formal supplement-chain validation runs BEFORE any registry byte is
    # consumed. This is the loader-side guarantee the accepted contract
    # requires: no supplement can be loaded standalone, out of order, or
    # against a base whose hash differs from what the supplement declares.
    from database.taxonomy.reconciliation.supplement_loader import (
        load_supplement_chain,
    )
    chain = load_supplement_chain(
        base_release_dir=args.release_dir,
        supplement_dirs=list(args.supplement or []),
    )
    registry_paths: list[Path] = []
    if args.canonical_registry:
        registry_paths.extend(args.canonical_registry)
    registry_paths.extend(s.canonical_dir for s in chain.supplements)
    release = PinnedRelease.load(
        args.release_dir,
        canonical_registry_path=registry_paths or None,
    )
    resolver = Resolver(release=release, rule_set=rule_set)

    inputs = list(_iter_input(args.input))
    logger.info("loaded %d input records from %s", len(inputs), args.input)
    results = [resolver.resolve(record) for record in inputs]

    artefact = build_manifest_body(
        results=results, rule_set=rule_set, release=release,
    )
    paths = write_manifest(
        args.output, artefact, write_summary=not args.no_summary,
    )
    print(
        json.dumps(
            {
                "manifest": str(paths["manifest"]),
                "semantic_sha256": artefact.semantic_hash,
                "record_count": len(results),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
