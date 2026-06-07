#!/usr/bin/env python3
"""Manual smoke test for RAW ingestion.

Usage:
    .venv/bin/python scripts/smoke_raw_import.py /path/to/sample.NEF

This is intentionally small and only prints the rendered derivative path plus
the RAW processing metadata JSON.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.local_image_ingest import RawRenderingUnavailableError, prepare_local_ingest_image


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke-test RAW ingestion")
    parser.add_argument("raw_path", help="Path to a local RAW file")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for the rendered local derivative",
    )
    args = parser.parse_args(argv)

    source = Path(args.raw_path).expanduser()
    if not source.exists():
        print(f"RAW file not found: {source}", file=sys.stderr)
        return 2

    try:
        result = prepare_local_ingest_image(source, lab_metadata={"image_type": "microscope"}, output_dir=args.output_dir)
    except RawRenderingUnavailableError as exc:
        print(f"RAW rendering unavailable: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"RAW import failed: {exc}", file=sys.stderr)
        return 4

    payload = {
        "source_path": result.source_path,
        "working_path": result.working_path,
        "original_path": result.original_path,
        "lab_metadata": result.lab_metadata,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
