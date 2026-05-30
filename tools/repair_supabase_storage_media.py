#!/usr/bin/env python3
"""Audit Supabase-only media rows and copy missing objects into Cloudflare R2.

This tool is for the migration cleanup window after media moved from Supabase
Storage to Cloudflare R2. It scans active observation image rows, checks each
object in R2 first, and if the object is missing there but still exists in the
old Supabase Storage bucket, it re-uploads the same key to R2.
"""
from __future__ import annotations

import argparse
import mimetypes
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_database_path  # noqa: E402
from utils.cloud_sync import SUPABASE_KEY, SUPABASE_URL, SporelyCloudClient  # noqa: E402
from utils.r2_storage import CloudflareR2Client, R2_PUBLIC_BASE_URL, media_variant_key, normalize_media_key  # noqa: E402


SUPABASE_STORAGE_BUCKET = "observation-images"
MEDIA_PROBE_TIMEOUT = 15
DEFAULT_PAGE_SIZE = 500
MISSING_STATUSES = {"missing_404"}
REPORT_FIELDS = [
    "cloud_observation_id",
    "cloud_image_id",
    "storage_path",
    "thumb_key",
    "r2_original_status",
    "r2_thumb_status",
    "supabase_original_status",
    "supabase_thumb_status",
    "repairable",
    "repairable_variants",
    "repair_status",
    "repair_error",
    "repair_uploaded_original_key",
    "repair_uploaded_thumb_key",
    "repair_post_original_status",
    "repair_post_thumb_status",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _encoded_media_key(storage_path: str | None) -> str:
    return quote(normalize_media_key(storage_path), safe="/")


def _storage_object_url(storage_path: str | None, *, backend: str, variant: str = "original") -> str:
    storage_key = normalize_media_key(storage_path)
    if not storage_key:
        return ""
    key = storage_key if variant == "original" else media_variant_key(storage_key, variant)
    encoded_key = _encoded_media_key(key)
    if backend == "r2":
        return f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{encoded_key}"
    if backend == "supabase":
        return f"{SUPABASE_URL}/storage/v1/object/authenticated/{SUPABASE_STORAGE_BUCKET}/{encoded_key}"
    raise ValueError(f"Unknown backend: {backend}")


def _probe_url(url: str, session: requests.Session, headers: dict[str, str] | None = None) -> dict[str, Any]:
    result = {
        "status": "unknown",
        "http_status": None,
        "url": url,
        "detail": None,
    }
    if not _normalize_text(url):
        result["detail"] = "missing url"
        return result

    try:
        response = session.get(
            url,
            timeout=MEDIA_PROBE_TIMEOUT,
            allow_redirects=True,
            headers={
                "Range": "bytes=0-0",
                "Cache-Control": "no-cache, no-store, max-age=0",
                "Pragma": "no-cache",
                **dict(headers or {}),
            },
            stream=True,
        )
    except requests.Timeout as exc:
        result["status"] = "timeout"
        result["detail"] = str(exc or "").strip() or exc.__class__.__name__
        return result
    except requests.RequestException as exc:
        result["status"] = "inaccessible"
        result["detail"] = str(exc or "").strip() or exc.__class__.__name__
        return result
    except Exception as exc:
        result["status"] = "unknown"
        result["detail"] = str(exc or "").strip() or exc.__class__.__name__
        return result

    try:
        status_code = int(getattr(response, "status_code", 0) or 0)
        result["http_status"] = status_code
        if status_code in {200, 206}:
            result["status"] = "exists"
        elif status_code in {404, 410}:
            result["status"] = "missing_404"
        elif status_code:
            result["status"] = "inaccessible"
        else:
            result["status"] = "unknown"
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
    return result


def _probe_media_backends(storage_path: str | None, session: requests.Session, access_token: str) -> dict[str, Any]:
    storage_key = normalize_media_key(storage_path)
    original_r2 = _probe_url(_storage_object_url(storage_key, backend="r2"), session)
    thumb_r2 = _probe_url(_storage_object_url(storage_key, backend="r2", variant="thumb"), session)
    auth_headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {access_token}",
    }
    original_supabase = _probe_url(
        _storage_object_url(storage_key, backend="supabase"),
        session,
        headers=auth_headers,
    )
    thumb_supabase = _probe_url(
        _storage_object_url(storage_key, backend="supabase", variant="thumb"),
        session,
        headers=auth_headers,
    )
    return {
        "r2_original": original_r2,
        "r2_thumb": thumb_r2,
        "supabase_original": original_supabase,
        "supabase_thumb": thumb_supabase,
    }


def _repairable_variants(probes: dict[str, Any]) -> list[str]:
    variants: list[str] = []
    if probes["r2_original"]["status"] in MISSING_STATUSES and probes["supabase_original"]["status"] == "exists":
        variants.append("original")
    if probes["r2_thumb"]["status"] in MISSING_STATUSES and probes["supabase_thumb"]["status"] == "exists":
        variants.append("thumb")
    return variants


def _sqlite_connect_readonly() -> Any:
    db_path = get_database_path()
    if not db_path.exists():
        raise RuntimeError(f"Local database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_cloud_rows(client: SporelyCloudClient, observation_ids: list[str] | None, limit: int | None) -> list[dict]:
    select_fields = [
        "id",
        "desktop_id",
        "observation_id",
        "storage_path",
        "original_filename",
        "image_type",
        "sort_order",
        "deleted_at",
    ]
    query = [
        "user_id=eq." + str(client.user_id),
        "deleted_at=is.null",
        "select=" + ",".join(select_fields),
        "order=observation_id.asc,sort_order.asc,id.asc",
    ]
    if observation_ids is not None:
        if not observation_ids:
            return []
        query.insert(0, f"observation_id=in.({','.join(observation_ids)})")

    rows: list[dict] = []
    offset = 0
    page_size = DEFAULT_PAGE_SIZE
    while True:
        current_limit = page_size
        if limit is not None and limit > 0:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            current_limit = min(current_limit, remaining)
        page_query = query + [f"limit={current_limit}", f"offset={offset}"]
        page = client._get("observation_images?" + "&".join(page_query))
        if not page:
            break
        rows.extend([dict(row or {}) for row in page])
        offset += len(page)
        if len(page) < current_limit:
            break
        if limit is not None and limit > 0 and len(rows) >= limit:
            rows = rows[:limit]
            break
    return rows


def _prepare_row_report(
    cloud_row: dict,
    probes: dict[str, Any],
) -> dict[str, Any]:
    storage_path = normalize_media_key(cloud_row.get("storage_path")) or None
    thumb_key = media_variant_key(storage_path, "thumb") if storage_path else None
    repairable_variants = _repairable_variants(probes)
    report = {
        "cloud_observation_id": _normalize_text(cloud_row.get("observation_id")) or None,
        "cloud_image_id": _normalize_text(cloud_row.get("id")) or None,
        "storage_path": storage_path,
        "thumb_key": thumb_key,
        "r2_original_status": probes["r2_original"]["status"],
        "r2_thumb_status": probes["r2_thumb"]["status"],
        "supabase_original_status": probes["supabase_original"]["status"],
        "supabase_thumb_status": probes["supabase_thumb"]["status"],
        "repairable": "yes" if repairable_variants else "no",
        "repairable_variants": ",".join(repairable_variants) or None,
        "repair_status": "not_attempted",
        "repair_error": None,
        "repair_uploaded_original_key": None,
        "repair_uploaded_thumb_key": None,
        "repair_post_original_status": None,
        "repair_post_thumb_status": None,
    }
    return report


def _download_supabase_object(url: str, session: requests.Session, access_token: str) -> tuple[bytes, str | None]:
    response = session.get(
        url,
        timeout=120,
        allow_redirects=True,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {access_token}",
        },
    )
    if not response.ok:
        raise RuntimeError(f"Supabase media download failed: {response.status_code} {response.text}".strip())
    content_type = str(response.headers.get("content-type") or "").strip().lower() or None
    return response.content, content_type


def _content_type_for_key(key: str | None) -> str:
    suffix = Path(str(key or "")).suffix.lower()
    if suffix == ".webp":
        return "image/webp"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    return mimetypes.guess_type(Path(str(key or "")).name)[0] or "image/jpeg"


def _repair_rows(client: SporelyCloudClient, rows: list[dict], probe_session: requests.Session) -> list[dict]:
    repaired_rows: list[dict] = []
    r2_client = client._get_r2()
    total_rows = len(rows)
    for index, row in enumerate(rows, start=1):
        row = dict(row)
        storage_path = _normalize_text(row.get("storage_path"))
        repairable_variants = [
            variant
            for variant in _normalize_text(row.get("repairable_variants")).split(",")
            if variant
        ]
        print(
            f"Repairing {index}/{total_rows} cloud {row.get('cloud_image_id') or '-'} "
            f"({', '.join(repairable_variants) or 'none'})..."
        )
        if row.get("repairable") != "yes" or not storage_path:
            row["repair_status"] = "skipped_not_repairable"
            repaired_rows.append(row)
            continue

        try:
            if "original" in repairable_variants:
                original_url = _storage_object_url(storage_path, backend="supabase")
                data, content_type = _download_supabase_object(original_url, probe_session, client.access_token)
                r2_client.put_bytes(
                    data,
                    storage_path,
                    content_type=content_type or _content_type_for_key(storage_path),
                    cache_control="public, max-age=31536000, immutable",
                )
                row["repair_uploaded_original_key"] = storage_path

            if "thumb" in repairable_variants:
                thumb_key = _normalize_text(row.get("thumb_key"))
                if thumb_key:
                    thumb_url = _storage_object_url(storage_path, backend="supabase", variant="thumb")
                    data, content_type = _download_supabase_object(thumb_url, probe_session, client.access_token)
                    r2_client.put_bytes(
                        data,
                        thumb_key,
                        content_type=content_type or _content_type_for_key(thumb_key),
                        cache_control="public, max-age=31536000, immutable",
                    )
                    row["repair_uploaded_thumb_key"] = thumb_key

            post_original = _probe_url(_storage_object_url(storage_path, backend="r2"), probe_session)
            post_thumb = _probe_url(_storage_object_url(storage_path, backend="r2", variant="thumb"), probe_session)
            row["repair_post_original_status"] = post_original["status"]
            row["repair_post_thumb_status"] = post_thumb["status"]
            if (
                row.get("repair_uploaded_original_key")
                and post_original["status"] != "exists"
            ) or (
                row.get("repair_uploaded_thumb_key")
                and post_thumb["status"] != "exists"
            ):
                row["repair_status"] = "partial"
                row["repair_error"] = "one or more repaired objects are still not visible"
            else:
                row["repair_status"] = "repaired"
        except Exception as exc:
            row["repair_status"] = "failed"
            row["repair_error"] = str(exc or "").strip() or exc.__class__.__name__
        repaired_rows.append(row)
    return repaired_rows


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    repairable = sum(1 for row in rows if row.get("repairable") == "yes")
    repaired = sum(1 for row in rows if row.get("repair_status") == "repaired")
    partial = sum(1 for row in rows if row.get("repair_status") == "partial")
    failed = sum(1 for row in rows if row.get("repair_status") == "failed")
    return {
        "rows": len(rows),
        "repairable": repairable,
        "repaired": repaired,
        "partial": partial,
        "failed": failed,
    }


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        raise RuntimeError("Could not load stored Sporely cloud credentials.")

    observation_ids: list[str] | None = None
    local_observation_id = None
    if args.cloud_observation_id:
        observation_ids = [str(args.cloud_observation_id).strip()]
    elif args.all:
        observation_ids = None
    else:
        with _sqlite_connect_readonly() as local_conn:
            if args.local_observation_id is None:
                raise RuntimeError("Select --local-observation-id, --cloud-observation-id, or --all.")
            row = local_conn.execute(
                "SELECT id, cloud_id FROM observations WHERE id = ?",
                (int(args.local_observation_id),),
            ).fetchone()
            if not row:
                raise RuntimeError(f"Local observation {args.local_observation_id} not found.")
            row = dict(row)
            cloud_id = _normalize_text(row.get("cloud_id"))
            if not cloud_id:
                raise RuntimeError(f"Local observation {args.local_observation_id} has no cloud_id.")
            observation_ids = [cloud_id]
            local_observation_id = int(row.get("id")) if row.get("id") is not None else None

    cloud_rows = _fetch_cloud_rows(client, observation_ids, args.limit)
    probe_session = requests.Session()
    report_rows: list[dict[str, Any]] = []
    total_rows = len(cloud_rows)
    for index, row in enumerate(cloud_rows, start=1):
        print(
            f"Probing {index}/{total_rows} cloud {row.get('id') or '-'} "
            f"(desktop {row.get('desktop_id') or '-'})..."
        )
        probes = _probe_media_backends(row.get("storage_path"), probe_session, client.access_token)
        report_rows.append(_prepare_row_report(row, probes))

    if args.repair:
        report_rows = _repair_rows(client, report_rows, probe_session)

    summary = _summarize_rows(report_rows)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "all": bool(args.all),
            "local_observation_id": local_observation_id,
            "cloud_observation_id": observation_ids[0] if observation_ids and len(observation_ids) == 1 else None,
            "limit": int(args.limit) if args.limit else None,
            "repair": bool(args.repair),
        },
        "summary": summary,
        "rows": report_rows,
    }
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--local-observation-id", type=int, help="Audit the cloud rows linked to one local observation.")
    scope.add_argument("--cloud-observation-id", help="Audit one cloud observation id directly.")
    scope.add_argument("--all", action="store_true", help="Audit every active cloud image row for the linked user.")
    parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of cloud rows to inspect.")
    parser.add_argument("--repair", action="store_true", help="Copy missing objects from Supabase Storage into R2.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if not (args.all or args.cloud_observation_id or args.local_observation_id is not None):
        print("Select --local-observation-id, --cloud-observation-id, or --all.", file=sys.stderr)
        return 1

    try:
        report = run_audit(args)
    except Exception as exc:
        print(f"Audit failed: {exc}", file=sys.stderr)
        return 1

    for row in report["rows"]:
        message = (
            f"cloud {row.get('cloud_image_id')}: r2={row.get('r2_original_status')}/{row.get('r2_thumb_status')} "
            f"supabase={row.get('supabase_original_status')}/{row.get('supabase_thumb_status')} "
            f"repairable={row.get('repairable')}"
        )
        if args.repair:
            message += f" repair={row.get('repair_status')}"
        print(message)

    summary = report["summary"]
    print(
        "Summary: "
        f"{summary['rows']} row(s), {summary['repairable']} repairable, "
        f"{summary['repaired']} repaired, {summary['partial']} partial, {summary['failed']} failed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
