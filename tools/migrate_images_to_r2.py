#!/usr/bin/env python3
"""Backfill local Sporely images to Cloudflare R2 using the cloud sync key layout."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_connection
from utils.cloud_sync import SporelyCloudClient, _normalize_cloud_media_key  # noqa: E402


def _load_local_images(limit: int | None = None) -> list[dict]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        sql = """
            SELECT
                i.id,
                i.observation_id,
                i.filepath,
                i.original_filepath,
                i.sort_order,
                i.image_type,
                i.micro_category,
                i.objective_name,
                i.scale_microns_per_pixel,
                i.resample_scale_factor,
                i.mount_medium,
                i.stain,
                i.sample_type,
                i.contrast,
                i.notes,
                i.gps_source,
                i.cloud_id AS image_cloud_id,
                o.cloud_id AS observation_cloud_id
            FROM images i
            JOIN observations o ON o.id = i.observation_id
            ORDER BY o.id, COALESCE(i.sort_order, i.id), i.id
        """
        if limit and int(limit) > 0:
            sql += f" LIMIT {int(limit)}"
        cursor.execute(sql)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def _remember_cloud_id(local_image_id: int, cloud_image_id: str) -> None:
    if not local_image_id or not cloud_image_id:
        return
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?",
            (str(cloud_image_id), datetime.now(timezone.utc).isoformat(), int(local_image_id)),
        )
        conn.commit()
    finally:
        conn.close()


def _resolve_upload_path(image_row: dict) -> Path | None:
    candidates = [
        Path(str(image_row.get("filepath") or "").strip()),
        Path(str(image_row.get("original_filepath") or "").strip()),
    ]
    for candidate in candidates:
        if str(candidate) and candidate.exists() and candidate.is_file():
            return candidate
    return None


def migrate_images(dry_run: bool = False, limit: int | None = None) -> int:
    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        raise RuntimeError("Could not load stored Sporely cloud credentials.")

    rows = _load_local_images(limit=limit)
    uploaded = 0
    skipped = 0

    for row in rows:
        local_image_id = int(row.get("id") or 0)
        observation_cloud_id = str(row.get("observation_cloud_id") or "").strip()
        if not observation_cloud_id and row.get("observation_id"):
            observation_cloud_id = str(client._find_cloud_observation(int(row["observation_id"])) or "").strip()
        if not observation_cloud_id:
            print(f"[skip] image {local_image_id}: observation has no cloud id")
            skipped += 1
            continue

        upload_path = _resolve_upload_path(row)
        if upload_path is None:
            print(f"[skip] image {local_image_id}: local file missing")
            skipped += 1
            continue

        cloud_image_id = str(row.get("image_cloud_id") or "").strip()
        if not cloud_image_id:
            cloud_image_id = str(client._find_cloud_image(local_image_id) or "").strip()
        provisional_cloud_image_id = cloud_image_id or str(local_image_id)
        storage_key = _normalize_cloud_media_key(
            client._build_storage_path(observation_cloud_id, provisional_cloud_image_id, str(upload_path))
        )

        print(f"[plan] image {local_image_id}: {upload_path.name} -> {storage_key}")
        if dry_run:
            continue

        image_payload = dict(row)
        actual_cloud_image_id = client.push_image_metadata(image_payload, observation_cloud_id, storage_key)
        expected_key = _normalize_cloud_media_key(
            client._build_storage_path(observation_cloud_id, actual_cloud_image_id, str(upload_path))
        )
        if expected_key != storage_key:
            storage_key = expected_key
            client._patch(
                f'observation_images?id=eq.{actual_cloud_image_id}',
                {'storage_path': storage_key},
            )
        client.upload_image_file(str(upload_path), observation_cloud_id, actual_cloud_image_id)
        _remember_cloud_id(local_image_id, actual_cloud_image_id)
        uploaded += 1
        print(f"[ok] image {local_image_id}: uploaded to {storage_key}")

    print(f"Completed: {uploaded} uploaded, {skipped} skipped.")
    return uploaded


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print planned uploads without writing anything.")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for a test run.")
    args = parser.parse_args()

    try:
        migrate_images(dry_run=bool(args.dry_run), limit=int(args.limit or 0) or None)
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
