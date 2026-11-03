#!/usr/bin/env python3
"""Freeze a validated compiler/SQLite/evidence set before publication.

With ``--derivation-dir`` it freezes a release derived from the published,
frozen base by ``name_associations.py`` instead (see ``freeze_derived``).
"""
import argparse
import json
from pathlib import Path
from promote_desktop_bundle import freeze, freeze_derived, PromotionError

_COMPILER_ARGS = ('sqlite', 'release-dir', 'redlist-report', 'redlist-workbook')
_DERIVED_ARGS = ('norm-dir', 'policy')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('bundle-dir', 'registry-manifest', 'validation-receipt', 'output'):
        parser.add_argument('--'+name, type=Path, required=True)
    for name in _COMPILER_ARGS + _DERIVED_ARGS + ('derivation-dir', 'holds'):
        parser.add_argument('--'+name, type=Path)
    parser.add_argument('--compatibility', type=Path)
    parser.add_argument('--evidence-input', action='append', default=[], metavar='NAME=PATH')
    args = parser.parse_args()
    derived = args.derivation_dir is not None
    required = _DERIVED_ARGS if derived else _COMPILER_ARGS
    other = _COMPILER_ARGS if derived else _DERIVED_ARGS + ('holds',)
    missing = [name for name in required if getattr(args, name.replace('-', '_')) is None]
    stray = [name for name in other if getattr(args, name.replace('-', '_')) is not None]
    if missing:
        parser.error('missing ' + ', '.join('--' + name for name in missing))
    if stray or (derived and args.evidence_input):
        parser.error('not for this kind of release: ' + ', '.join(
            '--' + name for name in stray + (['evidence-input'] if derived and args.evidence_input else [])))
    try:
        if derived:
            result = freeze_derived(derivation_dir=args.derivation_dir, norm_dir=args.norm_dir,
                policy_path=args.policy, holds_path=args.holds, bundle_dir=args.bundle_dir,
                registry_manifest_path=args.registry_manifest, compatibility_path=args.compatibility,
                validation_receipt_path=args.validation_receipt, output_dir=args.output)
        else:
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
