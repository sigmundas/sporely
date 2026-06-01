#!/usr/bin/env python3
"""Audit and repair observation image media after the Supabase Storage -> R2 split.

R2 is the canonical runtime media backend. This tool is for manual/admin repair
of legacy observation image rows whose objects still exist in legacy Supabase
Storage, plus optional soft-deletion of stale metadata that is missing from both
places.

Usage examples:
    python tools/repair_supabase_storage_media.py \
        --storage-path af912ffe-bdde-4f4a-a003-7938bf4f3504/59/558_cloud_0002.jpg \
        --admin-service-role \
        --env-file sporely-admin.env

    python tools/repair_supabase_storage_media.py \
        --legacy-upload-mode-null \
        --admin-service-role \
        --env-file sporely-admin.env

Set SUPABASE_SERVICE_ROLE_KEY in the environment before using --admin-service-role.
The recommended local admin env file is sporely-admin.env; python.env is a deprecated fallback.
"""
from __future__ import annotations

import argparse
import io
import csv
import json
import mimetypes
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from PIL import Image, ImageOps, features


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_database_path  # noqa: E402
from utils.cloud_sync import SUPABASE_KEY, SUPABASE_URL, SporelyCloudClient  # noqa: E402
from utils.cloud_media_policy import CLOUD_THUMB_MAX_EDGE  # noqa: E402
from utils.r2_storage import CloudflareR2Client, R2_PUBLIC_BASE_URL, load_admin_env_file, media_variant_key, normalize_media_key  # noqa: E402


SUPABASE_STORAGE_BUCKET = "observation-images"
ADMIN_SERVICE_ROLE_ENV = "SUPABASE_SERVICE_ROLE_KEY"
MEDIA_PROBE_TIMEOUT = 15
DEFAULT_PAGE_SIZE = 500
MISSING_STATUSES = {"missing_404"}
REPORT_FIELDS = [
    "id",
    "observation_id",
    "user_id",
    "storage_path",
    "r2_original",
    "r2_thumb",
    "supabase_original",
    "supabase_thumb",
    "action",
    "error",
]


class AdminServiceRoleClient:
    """Minimal Supabase REST client for explicit admin/service-role repair use."""

    def __init__(self, access_token: str):
        token = str(access_token or "").strip()
        if not token:
            raise RuntimeError(f"Set {ADMIN_SERVICE_ROLE_ENV} environment variable first.")
        self.access_token = token
        self.user_id = "service-role"
        self.supabase_api_key = token
        self._s = requests.Session()
        self._s.headers.update(
            {
                "apikey": token,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._r2: CloudflareR2Client | None = None

    def _get_r2(self) -> CloudflareR2Client:
        if self._r2 is None:
            self._r2 = CloudflareR2Client.from_env()
        return self._r2

    def _get(self, path: str) -> list:
        resp = self._s.get(f"{SUPABASE_URL}/rest/v1/{path}", timeout=20)
        if not resp.ok:
            raise RuntimeError(f"GET {path}: {resp.text}")
        try:
            return resp.json()
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"GET {path}: could not decode JSON ({exc})") from exc

    def _patch(self, path: str, payload: dict) -> None:
        resp = self._s.patch(
            f"{SUPABASE_URL}/rest/v1/{path}",
            json=payload,
            headers={"Prefer": "return=minimal"},
            timeout=20,
        )
        if not resp.ok:
            raise RuntimeError(f"PATCH {path}: {resp.text}")


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


def _supabase_object_headers(api_key: str, access_token: str) -> dict[str, str]:
    return {
        "apikey": str(api_key or "").strip(),
        "Authorization": f"Bearer {str(access_token or '').strip()}",
    }


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
    except Exception as exc:  # pragma: no cover - defensive
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


def _probe_media_backends(
    storage_path: str | None,
    session: requests.Session,
    access_token: str,
    *,
    supabase_api_key: str,
) -> dict[str, Any]:
    storage_key = normalize_media_key(storage_path)
    headers = _supabase_object_headers(supabase_api_key, access_token)
    return {
        "r2_original": _probe_url(_storage_object_url(storage_key, backend="r2"), session),
        "r2_thumb": _probe_url(_storage_object_url(storage_key, backend="r2", variant="thumb"), session),
        "supabase_original": _probe_url(
            _storage_object_url(storage_key, backend="supabase"),
            session,
            headers=headers,
        ),
        "supabase_thumb": _probe_url(
            _storage_object_url(storage_key, backend="supabase", variant="thumb"),
            session,
            headers=headers,
        ),
    }


def _cloud_thumb_save_format() -> tuple[str, str, dict[str, Any]]:
    if features.check("webp"):
        return "WEBP", "image/webp", {"quality": 65, "method": 4}
    return "JPEG", "image/jpeg", {"quality": 72}


def _download_r2_object(url: str, session: requests.Session) -> tuple[bytes, str | None]:
    response = session.get(
        url,
        timeout=120,
        allow_redirects=True,
        headers={
            "Cache-Control": "no-cache, no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )
    if not response.ok:
        raise RuntimeError(f"R2 media download failed: {response.status_code} {response.text}".strip())
    content_type = str(response.headers.get("content-type") or "").strip().lower() or None
    return response.content, content_type


def _generate_thumbnail_bytes(source_bytes: bytes) -> tuple[bytes, str]:
    content = bytes(source_bytes or b"")
    if not content:
        raise RuntimeError("Missing source image data")

    with Image.open(io.BytesIO(content)) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode in ("RGBA", "LA"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "RGBA":
                background.paste(img, mask=img.split()[3])
            else:
                background.paste(img, mask=img.split()[1])
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

        orig_w, orig_h = img.size
        scale = min(1.0, CLOUD_THUMB_MAX_EDGE / max(orig_w, orig_h))
        target_w = max(1, int(orig_w * scale))
        target_h = max(1, int(orig_h * scale))
        img_resized = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
        buffer = io.BytesIO()
        thumb_format, thumb_mime, thumb_options = _cloud_thumb_save_format()
        img_resized.save(buffer, format=thumb_format, **thumb_options)
    return buffer.getvalue(), thumb_mime


def _repair_plan_for_probes(probes: dict[str, Any]) -> dict[str, Any]:
    r2_original = _normalize_text(probes["r2_original"]["status"])
    r2_thumb = _normalize_text(probes["r2_thumb"]["status"])
    supabase_original = _normalize_text(probes["supabase_original"]["status"])
    supabase_thumb = _normalize_text(probes["supabase_thumb"]["status"])

    plan: dict[str, Any] = {
        "action": "needs_review",
        "error": None,
        "source_backend": None,
        "original_needs_upload": False,
        "thumb_needs_upload": False,
    }

    if r2_original == "exists":
        plan["source_backend"] = "r2"
    elif supabase_original == "exists":
        plan["source_backend"] = "supabase"
    elif r2_original in MISSING_STATUSES and supabase_original in MISSING_STATUSES:
        plan["action"] = "stale_metadata"
        plan["error"] = "original missing in both backends"
        return plan
    else:
        plan["error"] = f"original={r2_original}/{supabase_original}"
        return plan

    plan["original_needs_upload"] = plan["source_backend"] == "supabase" and r2_original != "exists"
    plan["thumb_needs_upload"] = r2_thumb != "exists"

    if not plan["original_needs_upload"] and not plan["thumb_needs_upload"]:
        plan["action"] = "noop"
        return plan

    plan["action"] = "needs_repair"
    if r2_thumb not in {"exists"} and supabase_thumb in MISSING_STATUSES and r2_original == "exists":
        plan["error"] = None
    return plan


def _sqlite_connect_readonly() -> Any:
    db_path = get_database_path()
    if not db_path.exists():
        raise RuntimeError(f"Local database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_cloud_rows(client: Any, filters: list[str], limit: int | None) -> list[dict]:
    select_fields = [
        "id",
        "observation_id",
        "user_id",
        "storage_path",
    ]
    base_query = list(filters) + [
        "select=" + ",".join(select_fields),
        "order=observation_id.asc,sort_order.asc,id.asc",
    ]
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
        page_query = base_query + [f"limit={current_limit}", f"offset={offset}"]
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


def _prepare_row_report(cloud_row: dict, probes: dict[str, Any]) -> dict[str, Any]:
    storage_path = normalize_media_key(cloud_row.get("storage_path")) or None
    plan = _repair_plan_for_probes(probes)
    report = {
        "id": _safe_int(cloud_row.get("id")) or None,
        "observation_id": _safe_int(cloud_row.get("observation_id")) or None,
        "user_id": _normalize_text(cloud_row.get("user_id")) or None,
        "storage_path": storage_path,
        "r2_original": probes["r2_original"]["status"],
        "r2_thumb": probes["r2_thumb"]["status"],
        "supabase_original": probes["supabase_original"]["status"],
        "supabase_thumb": probes["supabase_thumb"]["status"],
        "action": plan["action"] if storage_path else "needs_review",
        "error": plan["error"] if storage_path else "missing storage_path",
    }
    if storage_path:
        report["action"] = plan["action"]
        report["error"] = plan["error"]
    return report


def _download_supabase_object(
    url: str,
    session: requests.Session,
    access_token: str,
    *,
    supabase_api_key: str,
) -> tuple[bytes, str | None]:
    response = session.get(
        url,
        timeout=120,
        allow_redirects=True,
        headers=_supabase_object_headers(supabase_api_key, access_token),
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


def _download_source_original_bytes(
    source_backend: str,
    storage_path: str,
    session: requests.Session,
    access_token: str,
    *,
    supabase_api_key: str,
) -> tuple[bytes, str | None]:
    if source_backend == "r2":
        return _download_r2_object(_storage_object_url(storage_path, backend="r2"), session)
    if source_backend == "supabase":
        return _download_supabase_object(
            _storage_object_url(storage_path, backend="supabase"),
            session,
            access_token,
            supabase_api_key=supabase_api_key,
        )
    raise RuntimeError(f"Unknown source backend: {source_backend}")


def _repair_rows(
    client: Any,
    rows: list[dict],
    probe_session: requests.Session,
    *,
    supabase_api_key: str,
    repair: bool,
    soft_delete_stale_metadata: bool,
    verbose: bool = True,
) -> list[dict]:
    repaired_rows: list[dict] = []
    r2_client = client._get_r2()
    total_rows = len(rows)
    for index, row in enumerate(rows, start=1):
        row = dict(row)
        storage_path = _normalize_text(row.get("storage_path"))
        plan = _repair_plan_for_probes(
            {
                "r2_original": {"status": row.get("r2_original")},
                "r2_thumb": {"status": row.get("r2_thumb")},
                "supabase_original": {"status": row.get("supabase_original")},
                "supabase_thumb": {"status": row.get("supabase_thumb")},
            }
        )

        if not storage_path:
            if row.get("action") != "noop":
                row["action"] = "needs_review"
                row["error"] = "missing storage_path"
            repaired_rows.append(row)
            continue

        if repair and plan["action"] == "needs_repair":
            if verbose:
                print(
                    f"Repairing {index}/{total_rows} cloud image {row.get('id') or '-'} "
                    f"(original={'yes' if plan['original_needs_upload'] else 'no'}, thumb={'yes' if plan['thumb_needs_upload'] else 'no'})..."
                )
            repair_errors: list[str] = []
            original_bytes: bytes | None = None
            original_content_type: str | None = None
            uploaded_any = False
            try:
                need_source_bytes = bool(plan["original_needs_upload"] or plan["thumb_needs_upload"])
                if need_source_bytes:
                    original_bytes, original_content_type = _download_source_original_bytes(
                        str(plan["source_backend"] or ""),
                        storage_path,
                        probe_session,
                        client.access_token,
                        supabase_api_key=supabase_api_key,
                    )
            except Exception as exc:
                repair_errors.append(f"original_source: {str(exc or '').strip() or exc.__class__.__name__}")
                original_bytes = None
                original_content_type = None

            if original_bytes is not None and plan["original_needs_upload"]:
                try:
                    r2_client.put_bytes(
                        original_bytes,
                        storage_path,
                        content_type=original_content_type or _content_type_for_key(storage_path),
                        cache_control="public, max-age=31536000, immutable",
                    )
                    uploaded_any = True
                except Exception as exc:
                    repair_errors.append(f"original: {str(exc or '').strip() or exc.__class__.__name__}")

            if original_bytes is not None and plan["thumb_needs_upload"]:
                try:
                    thumb_bytes, thumb_content_type = _generate_thumbnail_bytes(original_bytes)
                    thumb_key = media_variant_key(storage_path, "thumb")
                    r2_client.put_bytes(
                        thumb_bytes,
                        thumb_key,
                        content_type=thumb_content_type,
                        cache_control="public, max-age=31536000, immutable",
                    )
                    uploaded_any = True
                except Exception as exc:
                    repair_errors.append(f"thumb: {str(exc or '').strip() or exc.__class__.__name__}")

            post_probes = _probe_media_backends(
                storage_path,
                probe_session,
                client.access_token,
                supabase_api_key=supabase_api_key,
            )
            row["r2_original"] = post_probes["r2_original"]["status"]
            row["r2_thumb"] = post_probes["r2_thumb"]["status"]
            row["supabase_original"] = post_probes["supabase_original"]["status"]
            row["supabase_thumb"] = post_probes["supabase_thumb"]["status"]
            post_original_ok = (not plan["original_needs_upload"]) or row["r2_original"] == "exists"
            post_thumb_ok = (not plan["thumb_needs_upload"]) or row["r2_thumb"] == "exists"
            verification_errors: list[str] = []
            if plan["original_needs_upload"] and row["r2_original"] != "exists":
                verification_errors.append(f"original={row['r2_original']}")
            if plan["thumb_needs_upload"] and row["r2_thumb"] != "exists":
                verification_errors.append(f"thumb={row['r2_thumb']}")

            if post_original_ok and post_thumb_ok:
                row["action"] = "repaired"
            elif post_original_ok or post_thumb_ok:
                row["action"] = "partial_repaired" if uploaded_any else "failed"
            else:
                row["action"] = "failed"

            combined_errors = repair_errors + verification_errors
            row["error"] = "; ".join(combined_errors) if combined_errors else None

        elif soft_delete_stale_metadata and plan["action"] == "stale_metadata":
            try:
                row_id = _safe_int(row.get("id"))
                if row_id <= 0:
                    raise RuntimeError("missing row id")
                client._patch(
                    f"observation_images?id=eq.{row_id}",
                    {"deleted_at": datetime.now(timezone.utc).isoformat()},
                )
                row["action"] = "soft_deleted_stale_metadata"
                row["error"] = None
            except Exception as exc:
                row["action"] = "failed"
                row["error"] = str(exc or "").strip() or exc.__class__.__name__

        repaired_rows.append(row)
    return repaired_rows


def _summarize_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    actions = Counter(_normalize_text(row.get("action")) or "unknown" for row in rows)
    summary = {"rows": len(rows)}
    for action in (
        "noop",
        "needs_repair",
        "partial_repairable",
        "repaired",
        "partial_repaired",
        "stale_metadata",
        "soft_deleted_stale_metadata",
        "needs_review",
        "failed",
    ):
        summary[action] = int(actions.get(action, 0))
    return summary


def _write_json_report(report: dict[str, Any], stream: Any = sys.stdout) -> None:
    stream.write(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n")


def _write_csv_report(rows: list[dict[str, Any]], stream: Any = sys.stdout) -> None:
    writer = csv.DictWriter(stream, fieldnames=REPORT_FIELDS)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field) for field in REPORT_FIELDS})


def _write_text_report(report: dict[str, Any], *, repair: bool, soft_delete_stale_metadata: bool) -> None:
    for row in report["rows"]:
        message = (
            f"image {row.get('id')}: obs={row.get('observation_id')} user={row.get('user_id') or '-'} "
            f"r2={row.get('r2_original')}/{row.get('r2_thumb')} "
            f"supabase={row.get('supabase_original')}/{row.get('supabase_thumb')} "
            f"action={row.get('action')}"
        )
        if row.get("error"):
            message += f" error={row.get('error')}"
        print(message)

    summary = report["summary"]
    print(
        "Summary: "
        f"{summary['rows']} row(s), {summary['noop']} noop, {summary['needs_repair']} needs_repair, "
        f"{summary['partial_repairable']} partial_repairable, {summary['repaired']} repaired, "
        f"{summary['partial_repaired']} partial_repaired, {summary['stale_metadata']} stale_metadata, "
        f"{summary['soft_deleted_stale_metadata']} soft_deleted_stale_metadata, "
        f"{summary['needs_review']} needs_review, {summary['failed']} failed."
    )
    if repair or soft_delete_stale_metadata:
        print("Done.")


def _load_client(args: argparse.Namespace) -> tuple[Any, str]:
    if args.admin_service_role or getattr(args, "env_file", None) is not None:
        load_admin_env_file(getattr(args, "env_file", None))
    if args.admin_service_role:
        service_key = _normalize_text(os.environ.get(ADMIN_SERVICE_ROLE_ENV))
        if not service_key:
            raise RuntimeError(f"Set {ADMIN_SERVICE_ROLE_ENV} environment variable first.")
        client = AdminServiceRoleClient(service_key)
        return client, service_key

    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        raise RuntimeError("Could not load stored Sporely cloud credentials.")
    return client, SUPABASE_KEY


def _resolve_row_scope(
    args: argparse.Namespace,
    client: Any,
    *,
    admin_mode: bool,
) -> tuple[list[str], dict[str, Any]]:
    scope = {
        "admin_service_role": bool(admin_mode),
        "all": bool(args.all),
        "local_observation_id": None,
        "cloud_observation_id": None,
        "storage_path": None,
        "legacy_upload_mode_null": bool(args.legacy_upload_mode_null),
        "limit": int(args.limit) if args.limit else None,
        "repair": bool(args.repair),
        "soft_delete_stale_metadata": bool(args.soft_delete_stale_metadata),
        "output_format": args.output_format,
    }
    filters = ["deleted_at=is.null"]

    if args.storage_path:
        if not admin_mode:
            raise RuntimeError("Select --admin-service-role before using --storage-path.")
        storage_path = normalize_media_key(args.storage_path)
        if not storage_path:
            raise RuntimeError("Storage path is empty.")
        filters.append(f"storage_path=eq.{quote(storage_path, safe='')}")
        scope["storage_path"] = storage_path
    elif args.legacy_upload_mode_null:
        if not admin_mode:
            raise RuntimeError("Select --admin-service-role before using --legacy-upload-mode-null.")
        filters.append("upload_mode=is.null")
    elif args.cloud_observation_id:
        cloud_observation_id = _normalize_text(args.cloud_observation_id)
        if not cloud_observation_id:
            raise RuntimeError("Cloud observation id is empty.")
        filters.append(f"observation_id=eq.{quote(cloud_observation_id, safe='')}")
        scope["cloud_observation_id"] = cloud_observation_id
    elif args.all:
        pass
    else:
        if args.local_observation_id is None:
            raise RuntimeError("Select --local-observation-id, --cloud-observation-id, --all, --storage-path, or --legacy-upload-mode-null.")
        with _sqlite_connect_readonly() as local_conn:
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
            filters.append(f"observation_id=eq.{quote(cloud_id, safe='')}")
            scope["local_observation_id"] = _safe_int(row.get("id")) or None
            scope["cloud_observation_id"] = cloud_id

    if not admin_mode:
        filters.insert(0, f"user_id=eq.{client.user_id}")

    return filters, scope


def run_audit(
    args: argparse.Namespace,
    *,
    client: Any | None = None,
    probe_session: requests.Session | None = None,
    supabase_api_key: str | None = None,
) -> dict[str, Any]:
    if client is None:
        client, supabase_api_key = _load_client(args)
    elif supabase_api_key is None:
        supabase_api_key = getattr(client, "supabase_api_key", SUPABASE_KEY)
    assert supabase_api_key is not None
    probe_session = probe_session or getattr(client, "_s", None) or requests.Session()

    filters, scope = _resolve_row_scope(args, client, admin_mode=bool(args.admin_service_role))
    cloud_rows = _fetch_cloud_rows(client, filters, args.limit)

    report_rows: list[dict[str, Any]] = []
    total_rows = len(cloud_rows)
    verbose = args.output_format == "text"
    for index, row in enumerate(cloud_rows, start=1):
        if verbose:
            print(
                f"Probing {index}/{total_rows} cloud image {row.get('id') or '-'} "
                f"(obs {row.get('observation_id') or '-'})..."
            )
        probes = _probe_media_backends(
            row.get("storage_path"),
            probe_session,
            client.access_token,
            supabase_api_key=supabase_api_key,
        )
        report_rows.append(_prepare_row_report(row, probes))

    if args.repair or args.soft_delete_stale_metadata:
        report_rows = _repair_rows(
            client,
            report_rows,
            probe_session,
            supabase_api_key=supabase_api_key,
            repair=bool(args.repair),
            soft_delete_stale_metadata=bool(args.soft_delete_stale_metadata),
            verbose=verbose,
        )

    summary = _summarize_rows(report_rows)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": scope,
        "summary": summary,
        "rows": report_rows,
    }
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--local-observation-id", type=int, help="Audit the cloud rows linked to one local observation.")
    scope.add_argument("--cloud-observation-id", help="Audit one cloud observation id directly.")
    scope.add_argument("--all", action="store_true", help="Audit every active cloud image row for the selected auth context.")
    scope.add_argument("--storage-path", help="Audit the exact storage_path for one observation image row.")
    scope.add_argument("--legacy-upload-mode-null", action="store_true", help="Audit active rows where deleted_at is null and upload_mode is null.")
    parser.add_argument("--admin-service-role", action="store_true", help="Use SUPABASE_SERVICE_ROLE_KEY for admin repair access.")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional local admin env file path (defaults to sporely-admin.env with python.env fallback).",
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of cloud rows to inspect.")
    parser.add_argument("--repair", action="store_true", help="Copy missing objects from legacy Supabase Storage into R2.")
    parser.add_argument("--soft-delete-stale-metadata", action="store_true", help="Soft-delete active rows whose media is missing from both backends.")
    parser.add_argument("--output-format", choices=("text", "json", "csv"), default="text", help="Choose the report output format.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if (args.storage_path or args.legacy_upload_mode_null) and not args.admin_service_role:
        print("Select --admin-service-role before using --storage-path or --legacy-upload-mode-null.", file=sys.stderr)
        return 1

    if not (args.all or args.cloud_observation_id or args.local_observation_id is not None or args.storage_path or args.legacy_upload_mode_null):
        print(
            "Select --local-observation-id, --cloud-observation-id, --all, --storage-path, or --legacy-upload-mode-null.",
            file=sys.stderr,
        )
        return 1

    try:
        report = run_audit(args)
    except Exception as exc:
        print(f"Audit failed: {exc}", file=sys.stderr)
        return 1

    if args.output_format == "json":
        _write_json_report(report)
        return 0
    if args.output_format == "csv":
        _write_csv_report(report["rows"])
        return 0

    _write_text_report(report, repair=bool(args.repair), soft_delete_stale_metadata=bool(args.soft_delete_stale_metadata))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
