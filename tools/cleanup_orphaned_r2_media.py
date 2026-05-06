#!/usr/bin/env python3
"""Find and optionally delete Cloudflare R2 media not referenced by Supabase.

Dry run by default. Use --delete only after reviewing the printed object list.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.cloud_sync import SUPABASE_KEY, SUPABASE_URL, SporelyCloudClient  # noqa: E402
from utils.r2_storage import CloudflareR2Client, media_variant_key, normalize_media_key  # noqa: E402


def _chunks(values: list[str], size: int = 1000):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _xml_text(node, local_name: str) -> str:
    for child in list(node):
        if child.tag.rsplit("}", 1)[-1] == local_name:
            return str(child.text or "").strip()
    return ""


def _list_r2_objects(client: CloudflareR2Client, prefix: str = "") -> list[dict]:
    objects: list[dict] = []
    token = ""
    while True:
        query = {"list-type": "2", "prefix": prefix}
        if token:
            query["continuation-token"] = token
        response = client._request("GET", key="", query=query, timeout=120)
        client._raise_for_status(response, "R2 list failed")
        root = ElementTree.fromstring(response.content)
        for contents in [node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "Contents"]:
            key = _xml_text(contents, "Key")
            if not key:
                continue
            try:
                size = int(_xml_text(contents, "Size") or 0)
            except ValueError:
                size = 0
            objects.append(
                {
                    "key": normalize_media_key(key),
                    "size": size,
                    "last_modified": _xml_text(contents, "LastModified"),
                }
            )
        truncated = _xml_text(root, "IsTruncated").lower() == "true"
        token = _xml_text(root, "NextContinuationToken")
        if not truncated or not token:
            break
    return objects


def _fetch_supabase_column(table: str, column: str, access_token: str) -> set[str]:
    session = requests.Session()
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {access_token}",
    }
    result: set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        response = session.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            params={
                "select": column,
                column: "not.is.null",
                "order": f"{column}.asc",
            },
            headers={**headers, "Range": f"{offset}-{offset + page_size - 1}"},
            timeout=60,
        )
        if not response.ok:
            text = response.text.lower()
            if response.status_code in {400, 404} and ("does not exist" in text or "schema cache" in text):
                return result
            raise RuntimeError(f"Supabase {table}.{column} fetch failed: {response.text}")
        rows = response.json() or []
        for row in rows:
            key = normalize_media_key((row or {}).get(column))
            if key:
                result.add(key)
        if len(rows) < page_size:
            break
        offset += page_size
    return result


def _referenced_media_keys(access_token: str) -> set[str]:
    referenced: set[str] = set()
    for table, column in (
        ("observation_images", "storage_path"),
        ("observations", "image_key"),
        ("observations", "thumb_key"),
        ("spore_measurements", "image_key"),
        ("spore_measurements", "thumb_key"),
    ):
        referenced.update(_fetch_supabase_column(table, column, access_token))

    expanded = set(referenced)
    for key in list(referenced):
        if not key:
            continue
        if Path(key).name.startswith("thumb_"):
            continue
        expanded.add(media_variant_key(key, "thumb"))
        expanded.add(media_variant_key(key, "small"))
        expanded.add(media_variant_key(key, "medium"))
    return expanded


def _format_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB")
    size = float(max(0, int(value or 0)))
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GB"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", default="", help="Only inspect R2 keys under this prefix, e.g. a user id.")
    parser.add_argument("--delete", action="store_true", help="Delete orphaned objects. Without this, only prints a dry run.")
    parser.add_argument("--limit", type=int, default=0, help="Only print/delete the first N orphaned objects.")
    parser.add_argument("--summary-only", action="store_true", help="Only print counts and total bytes.")
    args = parser.parse_args()

    prefix = normalize_media_key(args.prefix)
    cloud_client = SporelyCloudClient.from_stored_credentials()
    if not cloud_client:
        raise RuntimeError("Sign in to Sporely Cloud before running R2 cleanup.")
    client = CloudflareR2Client.from_env()
    objects = _list_r2_objects(client, prefix=prefix)
    referenced = _referenced_media_keys(cloud_client.access_token)
    orphaned = [obj for obj in objects if normalize_media_key(obj.get("key")) not in referenced]
    orphaned.sort(key=lambda obj: str(obj.get("key") or ""))
    total_count = len(orphaned)
    total_bytes = sum(int(obj.get("size") or 0) for obj in orphaned)
    selected = orphaned[: args.limit] if args.limit and args.limit > 0 else orphaned
    selected_bytes = sum(int(obj.get("size") or 0) for obj in selected)
    mode = "DELETE" if args.delete else "DRY RUN"
    print(f"{mode}: {total_count} orphaned object(s), {_format_bytes(total_bytes)}")
    if selected is not orphaned:
        print(f"Selected by --limit: {len(selected)} object(s), {_format_bytes(selected_bytes)}")
    if prefix:
        print(f"Prefix: {prefix}")
    print()
    if not args.summary_only:
        for obj in selected:
            print(f"{_format_bytes(int(obj.get('size') or 0)):>10}  {obj.get('last_modified') or '-':<24}  {obj.get('key')}")

    if args.delete and selected:
        keys = [str(obj.get("key") or "") for obj in selected if obj.get("key")]
        for batch in _chunks(keys):
            client.delete_objects(batch)
        print(f"\nDeleted {len(keys)} orphaned object(s).")
    elif not args.delete:
        print("\nNo changes made. Re-run with --delete to remove these objects.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
