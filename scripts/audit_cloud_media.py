#!/usr/bin/env python3
"""Run the read-only cloud-media incident inventory."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_database_path  # noqa: E402
from utils.cloud_media_audit import (  # noqa: E402
    AuditFilters, CloudInventoryIncompleteError, ReadOnlyCloudAuditReader,
    build_audit_report, load_local_inventory, report_csv, report_json,
    terminal_summary, validate_since,
)
from utils.cloud_sync import CloudReauthRequiredError, SporelyCloudClient, get_app_settings  # noqa: E402


def _since_arg(value: str) -> str:
    try:
        return validate_since(value) or ""
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only cloud-media incident audit; --output writes a private diagnostic report",
    )
    parser.add_argument("--observation-id", type=int)
    parser.add_argument("--cloud-observation-id")
    parser.add_argument("--name", action="append", default=[])
    parser.add_argument("--since", type=_since_arg)
    parser.add_argument("--output", type=Path, help="write JSON or CSV diagnostic report")
    parser.add_argument("--force", action="store_true", help="overwrite an existing report")
    parser.add_argument("--no-storage-check", action="store_true")
    parser.add_argument("--max-observations", type=int)
    parser.add_argument("--verbose", action="store_true")
    return parser


def write_report_atomic(path: Path, content: str, *, force: bool = False) -> None:
    """Atomically write a private diagnostic report with restrictive mode."""
    destination = Path(path)
    if destination.exists() and not force:
        raise FileExistsError(f"Report already exists: {destination}; pass --force to overwrite it")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent),
    )
    temporary = Path(temporary_name)
    try:
        try:
            os.fchmod(descriptor, 0o600)
        except (AttributeError, OSError):
            pass
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if destination.exists() and not force:
            raise FileExistsError(f"Report already exists: {destination}; pass --force to overwrite it")
        os.replace(temporary, destination)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    print("READ-ONLY audit: source database and cloud data will not be changed.", flush=True)
    print("Diagnostic reports contain private local paths and cloud object keys.", flush=True)
    if args.output and args.output.exists() and not args.force:
        print(f"Report already exists: {args.output}; pass --force to overwrite it", file=sys.stderr)
        return 2
    settings = get_app_settings()
    access_token = str(settings.get("cloud_access_token") or "").strip()
    user_id = str(settings.get("cloud_user_id") or "").strip()
    if not access_token or not user_id:
        print("No authenticated Sporely Cloud session is available; sign in and retry.", file=sys.stderr)
        return 2
    # No refresh token is supplied, and all REST reads use get_read_only(),
    # which disables refresh at the request call itself.
    client = SporelyCloudClient(access_token, user_id, refresh_token=None)
    filters = AuditFilters(
        observation_id=args.observation_id,
        cloud_observation_id=args.cloud_observation_id,
        names=tuple(args.name), since=args.since,
        max_observations=args.max_observations,
    )
    try:
        inventory = load_local_inventory(get_database_path(), filters)
        report = build_audit_report(
            inventory, ReadOnlyCloudAuditReader(client),
            check_storage=not args.no_storage_check,
        )
    except CloudReauthRequiredError as exc:
        print(f"Authentication expired: {exc}", file=sys.stderr)
        return 3
    except CloudInventoryIncompleteError as exc:
        print(f"Audit aborted: {exc}", file=sys.stderr)
        return 4
    print(terminal_summary(report))
    if args.output:
        content = report_csv(report) if args.output.suffix.lower() == ".csv" else report_json(report)
        try:
            write_report_atomic(args.output, content, force=args.force)
        except (OSError, FileExistsError) as exc:
            print(f"Could not write diagnostic report: {exc}", file=sys.stderr)
            return 5
        print(f"Wrote private diagnostic report: {args.output}")
    elif args.verbose:
        print(report_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
