"""Manual backfill: (re)generate public spore mosaics for existing observations.

Normal sync only pushes mosaics when it has other work to do for the
observation. This module gives an operator a way to force a mosaic
build/upload for observations that were synced before mosaic support
existed (or to re-do a specific one during debugging), without depending on
the "dirty observation" preflight.

Usage:

    ./.venv/bin/python -m utils.cloud_spore_mosaic_backfill --help

    # Regenerate the mosaic for one specific cloud observation:
    ./.venv/bin/python -m utils.cloud_spore_mosaic_backfill \\
        --observation-cloud-id 719

    # Sweep up to 10 observations:
    ./.venv/bin/python -m utils.cloud_spore_mosaic_backfill --limit 10

Auth reuses `SporelyCloudClient.from_stored_credentials()`, so this uses the
same login the desktop app is already using (access token → refresh token →
saved keychain password fallback).
"""

from __future__ import annotations

import argparse
import sys
from typing import Sequence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m utils.cloud_spore_mosaic_backfill',
        description=(
            'Generate/refresh public spore mosaics for existing observations. '
            'Runs the same code path as normal sync but bypasses the dirty '
            'observation filter, so mosaics can be created for observations '
            'that already synced before mosaic support existed.'
        ),
    )
    parser.add_argument(
        '--observation-cloud-id',
        action='append',
        default=None,
        metavar='CLOUD_ID',
        help=(
            'Cloud observation id to target. May be passed multiple times. '
            'Values are matched as strings against local observations.cloud_id. '
            'If omitted, every observation with a cloud_id is considered.'
        ),
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        metavar='N',
        help='Cap on the number of observations to process (after --observation-cloud-id).',
    )
    parser.add_argument(
        '--no-push-measurements',
        dest='push_measurements',
        action='store_false',
        default=True,
        help=(
            'Skip the per-observation measurement push before building the '
            'mosaic. Default is to push first so measurements added since the '
            'last regular sync appear in the mosaic. Turn off only when you '
            'want a strict rebuild without touching spore_measurements.'
        ),
    )
    parser.add_argument(
        '--no-ensure-image-metadata',
        dest='ensure_image_metadata',
        action='store_false',
        default=True,
        help=(
            'Skip creating metadata-only microscope image rows before '
            'pushing measurements. Default is on: local microscope images '
            'that have public-eligible spore measurements but no remote '
            'observation_images row get a metadata-only anchor row '
            '(storage_path = NULL, image_type = microscope) so their '
            'measurements can sync. No image bytes are uploaded — only '
            'metadata. Turn off if you already know every needed image is '
            'linked, or to debug the previous "8 of 26" gate.'
        ),
    )
    parser.add_argument(
        '--diagnose',
        action='store_true',
        default=False,
        help=(
            'Log a compact gate-count table for each observation showing why '
            'measurements are included or excluded (`total_local`, '
            '`with_p1_p2_p3_p4`, `image_has_cloud_id`, `measurement_has_cloud_id`, '
            '`pusher_would_select`, `remote_measurements`, `public_rpc_sporePoints`).'
        ),
    )
    return parser


def _resolve_client():
    """Import cloud_sync lazily so `--help` runs without hitting the DB."""
    from utils.cloud_sync import SporelyCloudClient

    try:
        client = SporelyCloudClient.from_stored_credentials()
    except Exception as exc:
        print(
            f'[cloud_sync] Mosaic backfill: could not resume cloud session: {exc}',
            file=sys.stderr,
            flush=True,
        )
        return None
    if client is None:
        print(
            '[cloud_sync] Mosaic backfill: no stored cloud session — '
            'log in through the desktop app first.',
            file=sys.stderr,
            flush=True,
        )
        return None
    return client


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    client = _resolve_client()
    if client is None:
        return 2

    from utils.cloud_sync import backfill_public_spore_mosaics

    counts = backfill_public_spore_mosaics(
        client,
        observation_cloud_ids=args.observation_cloud_id,
        limit=args.limit,
        push_measurements=args.push_measurements,
        ensure_image_metadata=args.ensure_image_metadata,
        diagnose=args.diagnose,
    )
    # Machine-friendly one-liner at the very end, on top of the human logs.
    print(f'[cloud_sync] Mosaic backfill: result {counts}', flush=True)
    if counts.get('failed', 0) > 0:
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
