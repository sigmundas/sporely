#!/usr/bin/env python3
"""Freeze a validated compiler/SQLite/evidence set before publication."""
import argparse
import json
from pathlib import Path
from promote_desktop_bundle import freeze, PromotionError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('sqlite', 'release-dir', 'redlist-report', 'redlist-workbook',
                 'bundle-dir', 'registry-manifest', 'validation-receipt', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--compatibility', type=Path)
    parser.add_argument('--evidence-input', action='append', default=[], metavar='NAME=PATH')
    args = parser.parse_args()
    try:
        evidence = {}
        for spec in args.evidence_input:
            name, path = spec.split('=', 1)
            if name in evidence:
                raise PromotionError('duplicate evidence input name')
            evidence[name] = Path(path)
        result = freeze(sqlite_path=args.sqlite, release_dir=args.release_dir,
            redlist_report_path=args.redlist_report, redlist_workbook=args.redlist_workbook,
            bundle_dir=args.bundle_dir, registry_manifest_path=args.registry_manifest,
            compatibility_path=args.compatibility, validation_receipt_path=args.validation_receipt,
            evidence_inputs=evidence, output_dir=args.output)
    except (PromotionError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f'refused: {exc}\n')
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
