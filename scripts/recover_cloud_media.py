#!/usr/bin/env python3
"""Targeted recovery for the audited Mycena haematopus image rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_database_path  # noqa: E402
from utils.cloud_media_audit import ReadOnlyCloudAuditReader  # noqa: E402
from utils.cloud_media_recovery import (  # noqa: E402
    CloudRecoveryAdapter, SQLiteRecoveryWriter, apply_recovery,
    build_recovery_plan, verify_recovery,
)
from utils.cloud_sync import SporelyCloudClient, get_app_settings  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Targeted recovery of the 12 audited Mycena cloud image rows (dry-run by default)")
    parser.add_argument("--observation-id", type=int, required=True, help="exact local observation ID")
    parser.add_argument("--apply", action="store_true", help="perform the narrowly scoped recovery writes")
    parser.add_argument("--confirm-cloud-observation", help="required with --apply; must exactly match the current cloud observation ID")
    return parser


def _confirmation_matches(supplied: str | None, actual: str) -> bool:
    return bool(str(supplied or "").strip()) and str(supplied).strip() == str(actual).strip()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.confirm_cloud_observation and not args.apply:
        print("--confirm-cloud-observation is only valid with --apply", file=sys.stderr)
        return 2
    if args.apply and not args.confirm_cloud_observation:
        print("--apply requires --confirm-cloud-observation", file=sys.stderr)
        return 2
    settings = get_app_settings()
    token = str(settings.get("cloud_access_token") or "").strip()
    user_id = str(settings.get("cloud_user_id") or "").strip()
    if not token or not user_id:
        print("No authenticated Sporely Cloud session is available; sign in and retry.", file=sys.stderr)
        return 2
    client = SporelyCloudClient(token, user_id, refresh_token=None)
    reader = ReadOnlyCloudAuditReader(client)
    try:
        plan = build_recovery_plan(get_database_path(), reader, args.observation_id)
    except Exception as exc:
        print(f"Recovery preflight aborted: {exc}", file=sys.stderr)
        return 3
    print("DRY RUN" if not args.apply else "APPLY")
    print(f"observation_id={plan.observation_id} cloud_observation_id={plan.cloud_observation_id}")
    print(f"inventory_complete={str(plan.inventory_complete).lower()} local_images={plan.local_image_count} healthy={plan.healthy_count} unmatched_cloud={plan.unmatched_cloud_count} targets={len(plan.items)}")
    for item in plan.items:
        print(f"image_id={item.local_image_id} type={item.image_type} sort_order={item.sort_order} status={item.status} measurements={item.measurement_count}")
    if not args.apply:
        print("No local or cloud writes performed.")
        return 0
    if not _confirmation_matches(args.confirm_cloud_observation, plan.cloud_observation_id):
        print("Cloud observation confirmation does not match the current local cloud link", file=sys.stderr)
        return 2
    results = apply_recovery(plan, CloudRecoveryAdapter(client), SQLiteRecoveryWriter(get_database_path()))
    for result in results:
        print(f"image_id={result['local_image_id']} status={result['status']}")
    if any(row["status"] == "failed" for row in results):
        return 4
    verification = verify_recovery(
        get_database_path(), reader, plan.observation_id, baseline_report=plan.report,
    )
    print(f"verification={'passed' if verification['ok'] else 'failed'} failures={verification['failure_count']}")
    return 0 if verification["ok"] else 5


if __name__ == "__main__":
    raise SystemExit(main())
