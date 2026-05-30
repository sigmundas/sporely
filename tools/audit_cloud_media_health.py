#!/usr/bin/env python3
"""Audit and optionally repair active cloud image rows with missing media objects."""

from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from PIL import Image


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database import models  # noqa: E402
from database.schema import get_database_path  # noqa: E402
from utils.cloud_sync import SporelyCloudClient, should_push_local_image_to_cloud  # noqa: E402
from utils.heic_converter import guess_local_image_mime_type  # noqa: E402
from utils.r2_storage import R2_PUBLIC_BASE_URL, media_variant_key, normalize_media_key  # noqa: E402


MEDIA_PROBE_TIMEOUT = 15
DEFAULT_PAGE_SIZE = 500
MISSING_STATUSES = {"missing_404"}
_MEDIA_CACHE_BUST_PARAM = "sporely_audit"
REPORT_FIELDS = [
    "cloud_observation_id",
    "local_observation_id",
    "cloud_image_id",
    "local_image_id",
    "match_method",
    "image_type",
    "sort_order",
    "storage_path",
    "thumb_key",
    "deleted_at",
    "local_file_exists",
    "local_tombstoned",
    "original_status",
    "thumb_status",
    "repairable",
    "reason",
    "repair_status",
    "repair_error",
    "repair_uploaded_original_key",
    "repair_uploaded_thumb_key",
    "repair_post_original_status",
    "repair_post_thumb_status",
    "repair_source_path",
    "local_filepath",
    "local_original_filepath",
    "local_source_role",
    "local_file_purpose",
    "local_original_mime_type",
    "local_working_mime_type",
    "local_file_extension",
    "local_mime_type",
    "local_width",
    "local_height",
    "cloud_desktop_id",
    "cloud_original_filename",
    "cloud_upload_mode",
    "cloud_source_width",
    "cloud_source_height",
    "cloud_stored_width",
    "cloud_stored_height",
    "cloud_stored_bytes",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _sqlite_connect_readonly() -> sqlite3.Connection:
    db_path = get_database_path()
    if not db_path.exists():
        raise RuntimeError(f"Local database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _load_all_rows(conn: sqlite3.Connection, table: str) -> list[dict]:
    try:
        cursor = conn.execute(f"SELECT * FROM {table}")
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in cursor.fetchall()]


def _first_existing_path(*path_values: Any) -> tuple[str | None, bool]:
    for value in path_values:
        text = _normalize_text(value)
        if not text:
            continue
        path = Path(text)
        if path.exists() and path.is_file():
            return text, True
    for value in path_values:
        text = _normalize_text(value)
        if text:
            return text, False
    return None, False


def _file_info(path_value: str | None) -> dict[str, Any]:
    text = _normalize_text(path_value)
    if not text:
        return {
            "exists": False,
            "width": None,
            "height": None,
            "extension": None,
            "mime_type": None,
        }
    path = Path(text)
    exists = path.exists() and path.is_file()
    width = height = None
    if exists:
        try:
            with Image.open(path) as img:
                width = int(img.width or 0) or None
                height = int(img.height or 0) or None
        except Exception:
            width = None
            height = None
    mime_type = guess_local_image_mime_type(path) or mimetypes.guess_type(path.name)[0]
    return {
        "exists": exists,
        "width": width,
        "height": height,
        "extension": path.suffix.lower() or None,
        "mime_type": mime_type,
    }


def _is_generated_artifact_local_image(image_row: dict) -> bool:
    source_role = _normalize_text(image_row.get("source_role")).lower()
    file_purpose = _normalize_text(image_row.get("file_purpose")).lower()
    if source_role == "generated_artifact":
        return True
    if file_purpose in {"thumbnail", "spore_crop", "plot", "plate", "calibration_overlay"}:
        return True
    if not should_push_local_image_to_cloud(image_row):
        return True
    return False


def _cache_busted_media_url(url: str, *, probe_kind: str) -> str:
    text = _normalize_text(url)
    if not text:
        return ""
    parsed = urlsplit(text)
    query_items = list(parse_qsl(parsed.query, keep_blank_values=True))
    query_items.append((
        _MEDIA_CACHE_BUST_PARAM,
        f"{probe_kind}_{time.time_ns()}_{uuid.uuid4().hex}",
    ))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_items), parsed.fragment))


def _probe_public_media_url(url: str, session: requests.Session | None = None) -> dict[str, Any]:
    result = {
        "status": "unknown",
        "http_status": None,
        "url": url,
        "detail": None,
    }
    if not _normalize_text(url):
        result["detail"] = "missing url"
        return result

    client = session or requests.Session()
    probe_url = _cache_busted_media_url(url, probe_kind="existence")
    try:
        response = client.get(
            probe_url,
            timeout=MEDIA_PROBE_TIMEOUT,
            allow_redirects=True,
            headers={
                "Range": "bytes=0-0",
                "Cache-Control": "no-cache, no-store, max-age=0",
                "Pragma": "no-cache",
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


def _load_local_context(conn: sqlite3.Connection) -> dict[str, Any]:
    observations = _load_all_rows(conn, "observations")
    images = _load_all_rows(conn, "images")

    observations_by_id: dict[int, dict] = {}
    observations_by_cloud_id: dict[str, dict] = {}
    for row in observations:
        obs_id = _safe_int(row.get("id"))
        if obs_id > 0:
            observations_by_id[obs_id] = row
        cloud_id = _normalize_text(row.get("cloud_id"))
        if cloud_id:
            observations_by_cloud_id[cloud_id] = row

    images_by_id: dict[int, list[dict]] = {}
    images_by_cloud_id: dict[str, list[dict]] = {}
    images_by_observation_id: dict[int, list[dict]] = {}
    for row in images:
        image_id = _safe_int(row.get("id"))
        if image_id <= 0:
            continue
        images_by_id.setdefault(image_id, []).append(row)
        cloud_id = _normalize_text(row.get("cloud_id"))
        if cloud_id:
            images_by_cloud_id.setdefault(cloud_id, []).append(row)
        obs_id = _safe_int(row.get("observation_id"))
        if obs_id > 0:
            images_by_observation_id.setdefault(obs_id, []).append(row)

    local_image_ids = sorted(images_by_id)
    local_cloud_ids = sorted({cloud_id for cloud_id in images_by_cloud_id})
    tombstones_by_local_image_id = models.get_image_tombstones_by_local_image_id(local_image_ids)
    tombstones_by_cloud_id = models.get_image_tombstones_by_deleted_cloud_id(local_cloud_ids)

    return {
        "observations_by_id": observations_by_id,
        "observations_by_cloud_id": observations_by_cloud_id,
        "images_by_id": images_by_id,
        "images_by_cloud_id": images_by_cloud_id,
        "images_by_observation_id": images_by_observation_id,
        "tombstones_by_local_image_id": tombstones_by_local_image_id,
        "tombstones_by_cloud_id": tombstones_by_cloud_id,
    }


def _local_image_tombstoned(row: dict, context: dict[str, Any]) -> bool:
    local_image_id = _safe_int(row.get("id"))
    cloud_id = _normalize_text(row.get("cloud_id"))
    if local_image_id > 0 and local_image_id in context["tombstones_by_local_image_id"]:
        return True
    if cloud_id and cloud_id in context["tombstones_by_cloud_id"]:
        return True
    return False


def _load_local_source_path(image_row: dict) -> tuple[str | None, bool]:
    return _first_existing_path(image_row.get("filepath"), image_row.get("original_filepath"))


def _augment_local_image(image_row: dict, context: dict[str, Any]) -> dict[str, Any]:
    source_path, file_exists = _load_local_source_path(image_row)
    file_info = _file_info(source_path) if source_path else {
        "exists": False,
        "width": None,
        "height": None,
        "extension": None,
        "mime_type": None,
    }
    return {
        "id": _safe_int(image_row.get("id")),
        "observation_id": _safe_int(image_row.get("observation_id")),
        "cloud_id": _normalize_text(image_row.get("cloud_id")),
        "filepath": _normalize_text(image_row.get("filepath")) or None,
        "original_filepath": _normalize_text(image_row.get("original_filepath")) or None,
        "sort_order": image_row.get("sort_order"),
        "image_type": _normalize_text(image_row.get("image_type")) or None,
        "micro_category": _normalize_text(image_row.get("micro_category")) or None,
        "source_role": _normalize_text(image_row.get("source_role")) or None,
        "file_purpose": _normalize_text(image_row.get("file_purpose")) or None,
        "original_mime_type": _normalize_text(image_row.get("original_mime_type")) or None,
        "working_mime_type": _normalize_text(image_row.get("working_mime_type")) or None,
        "notes": _normalize_text(image_row.get("notes")) or None,
        "source_path": source_path,
        "local_file_exists": file_exists,
        "local_tombstoned": _local_image_tombstoned(image_row, context),
        "local_width": file_info["width"],
        "local_height": file_info["height"],
        "local_file_extension": file_info["extension"],
        "local_mime_type": file_info["mime_type"],
        "generated_artifact": _is_generated_artifact_local_image(image_row),
    }


def _select_local_candidate(
    cloud_row: dict,
    context: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    cloud_image_id = _normalize_text(cloud_row.get("id"))
    desktop_id = _safe_int(cloud_row.get("desktop_id"))
    cloud_observation_id = _normalize_text(cloud_row.get("observation_id"))
    expected_local_obs_row = context["observations_by_cloud_id"].get(cloud_observation_id)
    expected_local_obs_id = _safe_int((expected_local_obs_row or {}).get("id"))
    image_type = _normalize_text(cloud_row.get("image_type")).lower()
    sort_order = cloud_row.get("sort_order")
    try:
        sort_order_int = int(sort_order) if sort_order is not None else None
    except (TypeError, ValueError):
        sort_order_int = None

    direct_candidates: list[tuple[str, dict]] = []
    if desktop_id > 0:
        for candidate in context["images_by_id"].get(desktop_id, []):
            direct_candidates.append(("desktop_id", candidate))
    if cloud_image_id:
        for candidate in context["images_by_cloud_id"].get(cloud_image_id, []):
            direct_candidates.append(("cloud_id", candidate))

    direct_candidates = [
        (method, candidate)
        for method, candidate in direct_candidates
        if _safe_int(candidate.get("id")) > 0
    ]

    if direct_candidates:
        unique_rows: list[tuple[str, dict]] = []
        seen_ids: set[int] = set()
        for method, candidate in direct_candidates:
            candidate_id = _safe_int(candidate.get("id"))
            if candidate_id in seen_ids:
                continue
            seen_ids.add(candidate_id)
            unique_rows.append((method, candidate))
        if len(unique_rows) == 1:
            method, candidate = unique_rows[0]
            candidate_obs_id = _safe_int(candidate.get("observation_id"))
            if expected_local_obs_id > 0 and candidate_obs_id > 0 and candidate_obs_id != expected_local_obs_id:
                return _augment_local_image(candidate, context), method, "local observation mismatch"
            if _is_generated_artifact_local_image(candidate):
                return _augment_local_image(candidate, context), method, "generated artifact local row"
            return _augment_local_image(candidate, context), method, None
        candidate_ids = { _safe_int(candidate.get("id")) for _method, candidate in unique_rows }
        if len(candidate_ids) > 1:
            return None, None, "ambiguous direct local match"

    if expected_local_obs_id <= 0:
        return None, None, "no local observation match"

    local_candidates = list(context["images_by_observation_id"].get(expected_local_obs_id, []))
    if sort_order_int is not None:
        local_candidates = [
            candidate
            for candidate in local_candidates
            if _safe_int(candidate.get("sort_order"), default=-999999) == sort_order_int
        ]
    if image_type:
        local_candidates = [
            candidate
            for candidate in local_candidates
            if _normalize_text(candidate.get("image_type")).lower() == image_type
        ]
    local_candidates = [candidate for candidate in local_candidates if not _is_generated_artifact_local_image(candidate)]
    if len(local_candidates) == 1:
        candidate = local_candidates[0]
        return _augment_local_image(candidate, context), "sort_order", None
    if len(local_candidates) > 1:
        return None, None, "ambiguous fallback match"
    return None, None, "no safe local fallback"


def _cloud_row_urls(storage_path: str | None) -> tuple[str | None, str | None]:
    storage_key = normalize_media_key(storage_path)
    if not storage_key:
        return None, None
    thumb_key = media_variant_key(storage_key, "thumb")
    return f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{storage_key}", f"{R2_PUBLIC_BASE_URL.rstrip('/')}/{thumb_key}"


def _probe_original_and_thumb(storage_path: str | None, session: requests.Session) -> tuple[dict[str, Any], dict[str, Any]]:
    original_url, thumb_url = _cloud_row_urls(storage_path)
    original_probe = _probe_public_media_url(original_url or "", session=session)
    thumb_probe = _probe_public_media_url(thumb_url or "", session=session)
    return original_probe, thumb_probe


def _repairable_reason(
    cloud_row: dict,
    local_candidate: dict[str, Any] | None,
    original_status: str,
    thumb_status: str,
    match_reason: str | None,
) -> tuple[str, str]:
    deleted_at = _normalize_text(cloud_row.get("deleted_at"))
    if deleted_at:
        return "no", "cloud row is tombstoned"

    if not local_candidate:
        return "no", match_reason or "no local match"

    if local_candidate.get("generated_artifact"):
        return "no", "local row is a generated artifact"

    if local_candidate.get("local_tombstoned"):
        return "no", "local row is tombstoned"

    if not local_candidate.get("local_file_exists"):
        return "no", "local file is missing"

    if original_status in MISSING_STATUSES or thumb_status in MISSING_STATUSES:
        return "yes", (
            f"missing media object(s): original={original_status}, thumb={thumb_status}; "
            f"matched via {match_reason or 'unknown'}"
        )

    if original_status == "exists" and thumb_status == "exists":
        return "no", "original and thumb exist"

    return "no", f"statuses are not safely repairable: original={original_status}, thumb={thumb_status}"


def _prepare_row_report(
    cloud_row: dict,
    context: dict[str, Any],
    probe_session: requests.Session,
) -> dict[str, Any]:
    local_candidate, match_method, match_reason = _select_local_candidate(cloud_row, context)
    match_label = match_method or match_reason
    storage_path = normalize_media_key(cloud_row.get("storage_path")) or None
    thumb_key = media_variant_key(storage_path, "thumb") if storage_path else None
    original_probe, thumb_probe = _probe_original_and_thumb(storage_path, probe_session)
    repairable, reason = _repairable_reason(
        cloud_row,
        local_candidate,
        original_probe["status"],
        thumb_probe["status"],
        match_label,
    )
    local_obs_id = None
    if local_candidate:
        local_obs_id = _safe_int(local_candidate.get("observation_id")) or None
    elif _normalize_text(cloud_row.get("observation_id")) in context["observations_by_cloud_id"]:
        local_obs_id = _safe_int(context["observations_by_cloud_id"][_normalize_text(cloud_row.get("observation_id"))].get("id")) or None

    report = {
        "cloud_observation_id": _normalize_text(cloud_row.get("observation_id")) or None,
        "local_observation_id": local_obs_id,
        "cloud_image_id": _normalize_text(cloud_row.get("id")) or None,
        "local_image_id": local_candidate.get("id") if local_candidate else None,
        "match_method": match_method,
        "image_type": _normalize_text(cloud_row.get("image_type")) or None,
        "sort_order": cloud_row.get("sort_order"),
        "storage_path": storage_path,
        "thumb_key": thumb_key,
        "deleted_at": _normalize_text(cloud_row.get("deleted_at")) or None,
        "local_file_exists": bool(local_candidate.get("local_file_exists")) if local_candidate else False,
        "local_tombstoned": bool(local_candidate.get("local_tombstoned")) if local_candidate else False,
        "original_status": original_probe["status"],
        "thumb_status": thumb_probe["status"],
        "repairable": repairable,
        "reason": reason,
        "repair_status": "not_attempted",
        "repair_error": None,
        "repair_uploaded_original_key": None,
        "repair_uploaded_thumb_key": None,
        "repair_post_original_status": None,
        "repair_post_thumb_status": None,
        "repair_source_path": local_candidate.get("source_path") if local_candidate else None,
        "local_filepath": local_candidate.get("filepath") if local_candidate else None,
        "local_original_filepath": local_candidate.get("original_filepath") if local_candidate else None,
        "local_source_role": local_candidate.get("source_role") if local_candidate else None,
        "local_file_purpose": local_candidate.get("file_purpose") if local_candidate else None,
        "local_original_mime_type": local_candidate.get("original_mime_type") if local_candidate else None,
        "local_working_mime_type": local_candidate.get("working_mime_type") if local_candidate else None,
        "local_file_extension": local_candidate.get("local_file_extension") if local_candidate else None,
        "local_mime_type": local_candidate.get("local_mime_type") if local_candidate else None,
        "local_width": local_candidate.get("local_width") if local_candidate else None,
        "local_height": local_candidate.get("local_height") if local_candidate else None,
        "cloud_desktop_id": _safe_int(cloud_row.get("desktop_id")) or None,
        "cloud_original_filename": _normalize_text(cloud_row.get("original_filename")) or None,
        "cloud_upload_mode": _normalize_text(cloud_row.get("upload_mode")) or None,
        "cloud_source_width": cloud_row.get("source_width"),
        "cloud_source_height": cloud_row.get("source_height"),
        "cloud_stored_width": cloud_row.get("stored_width"),
        "cloud_stored_height": cloud_row.get("stored_height"),
        "cloud_stored_bytes": cloud_row.get("stored_bytes"),
    }
    return report


def _scope_to_cloud_ids(
    client: SporelyCloudClient,
    args: argparse.Namespace,
    local_conn: sqlite3.Connection,
) -> tuple[list[str] | None, int | None]:
    if args.all:
        return None, None
    if args.cloud_observation_id:
        return [str(args.cloud_observation_id).strip()], None
    if args.local_observation_id is None:
        raise RuntimeError("Select --local-observation-id, --cloud-observation-id, or --all.")
    try:
        local_row = local_conn.execute(
            "SELECT id, cloud_id FROM observations WHERE id = ?",
            (int(args.local_observation_id),),
        ).fetchone()
    except sqlite3.OperationalError:
        local_row = None
    if not local_row:
        raise RuntimeError(f"Local observation {args.local_observation_id} not found.")
    local_row = dict(local_row)
    cloud_id = _normalize_text(local_row.get("cloud_id"))
    if not cloud_id:
        raise RuntimeError(
            f"Local observation {args.local_observation_id} has no cloud_id, so there is no cloud observation to audit."
        )
    return [cloud_id], int(args.local_observation_id)


def _fetch_cloud_rows(
    client: SporelyCloudClient,
    observation_ids: list[str] | None,
    limit: int | None,
) -> list[dict]:
    select_fields = [
        "id",
        "desktop_id",
        "observation_id",
        "storage_path",
        "original_filename",
        "image_type",
        "micro_category",
        "sort_order",
        "deleted_at",
        "upload_mode",
        "source_width",
        "source_height",
        "stored_width",
        "stored_height",
        "stored_bytes",
        "created_at",
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


def _write_json_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def _write_csv_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in REPORT_FIELDS})


def _repair_rows(
    client: SporelyCloudClient,
    rows: list[dict[str, Any]],
    probe_session: requests.Session,
) -> list[dict[str, Any]]:
    repaired_rows: list[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        if row.get("repairable") != "yes":
            row["repair_status"] = "skipped_not_repairable"
            repaired_rows.append(row)
            continue

        source_path = _normalize_text(row.get("repair_source_path"))
        storage_path = _normalize_text(row.get("storage_path"))
        cloud_observation_id = _normalize_text(row.get("cloud_observation_id"))
        cloud_image_id = _normalize_text(row.get("cloud_image_id"))
        thumb_key = _normalize_text(row.get("thumb_key"))
        if not source_path or not Path(source_path).exists():
            row["repair_status"] = "failed"
            row["repair_error"] = "local file disappeared before repair"
            repaired_rows.append(row)
            continue

        try:
            client.upload_image_file(
                source_path,
                cloud_observation_id,
                cloud_image_id,
                storage_path=storage_path,
            )
            row["repair_status"] = "uploaded"
            row["repair_uploaded_original_key"] = storage_path
            row["repair_uploaded_thumb_key"] = thumb_key
            post_original, post_thumb = _probe_original_and_thumb(storage_path, probe_session)
            row["repair_post_original_status"] = post_original["status"]
            row["repair_post_thumb_status"] = post_thumb["status"]
            if post_original["status"] != "exists" or post_thumb["status"] != "exists":
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
    healthy = sum(1 for row in rows if row.get("repairable") == "no" and row.get("original_status") == "exists" and row.get("thumb_status") == "exists")
    repaired = sum(1 for row in rows if row.get("repair_status") == "repaired")
    partial = sum(1 for row in rows if row.get("repair_status") == "partial")
    failed = sum(1 for row in rows if row.get("repair_status") == "failed")
    return {
        "rows": len(rows),
        "repairable": repairable,
        "healthy": healthy,
        "repaired": repaired,
        "partial": partial,
        "failed": failed,
    }


def run_audit(
    args: argparse.Namespace,
) -> dict[str, Any]:
    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        raise RuntimeError("Could not load stored Sporely cloud credentials.")

    with _sqlite_connect_readonly() as local_conn:
        scope_cloud_ids, scope_local_id = _scope_to_cloud_ids(client, args, local_conn)
        context = _load_local_context(local_conn)
        local_observation_id = scope_local_id
        if local_observation_id is None and scope_cloud_ids:
            local_row = context["observations_by_cloud_id"].get(scope_cloud_ids[0])
            if local_row:
                local_observation_id = _safe_int(local_row.get("id")) or None
        cloud_rows = _fetch_cloud_rows(client, scope_cloud_ids, args.limit)

    probe_session = requests.Session()
    report_rows = [_prepare_row_report(row, context, probe_session) for row in cloud_rows]
    if args.repair:
        report_rows = _repair_rows(client, report_rows, probe_session)

    summary = _summarize_rows(report_rows)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "all": bool(args.all),
            "local_observation_id": local_observation_id,
            "cloud_observation_id": scope_cloud_ids[0] if scope_cloud_ids and len(scope_cloud_ids) == 1 else None,
            "limit": int(args.limit) if args.limit else None,
            "repair": bool(args.repair),
            "all_repair": bool(args.all_repair),
        },
        "summary": summary,
        "rows": report_rows,
    }

    if args.json:
        _write_json_report(Path(args.json), report)
    if args.csv:
        _write_csv_report(Path(args.csv), report_rows)

    for row in report_rows:
        repairable = row.get("repairable")
        original_status = row.get("original_status")
        thumb_status = row.get("thumb_status")
        message = (
            f"cloud {row.get('cloud_image_id')}: local {row.get('local_image_id') or '-'} "
            f"original={original_status} thumb={thumb_status} repairable={repairable} reason={row.get('reason')}"
        )
        if args.repair:
            message += f" repair={row.get('repair_status')}"
            if row.get("repair_status") in {"uploaded", "partial"}:
                message += (
                    f" uploaded_original={row.get('repair_uploaded_original_key')}"
                    f" uploaded_thumb={row.get('repair_uploaded_thumb_key')}"
                )
        print(message)

    print(
        "Summary: "
        f"{summary['rows']} row(s), {summary['repairable']} repairable, "
        f"{summary['healthy']} healthy, {summary['repaired']} repaired, "
        f"{summary['partial']} partial, {summary['failed']} failed."
    )

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--local-observation-id", type=int, help="Audit the cloud rows linked to one local observation.")
    scope.add_argument("--cloud-observation-id", help="Audit one cloud observation id directly.")
    scope.add_argument("--all", action="store_true", help="Audit every active cloud image row for the linked user.")
    parser.add_argument("--limit", type=int, default=0, help="Optional maximum number of cloud rows to inspect.")
    parser.add_argument("--json", help="Write a JSON report to this path.")
    parser.add_argument("--csv", help="Write a CSV report to this path.")
    parser.add_argument("--repair", action="store_true", help="Upload missing media back to existing cloud keys.")
    parser.add_argument(
        "--all-repair",
        action="store_true",
        help="Allow --repair to run against all rows; otherwise repair requires a narrow scope.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.all_repair and not args.repair:
        print("--all-repair requires --repair.", file=sys.stderr)
        return 1
    has_scope = bool(args.all or args.cloud_observation_id or args.local_observation_id is not None)
    if args.repair and args.all_repair:
        if args.cloud_observation_id or args.local_observation_id is not None:
            print("--all-repair cannot be combined with a specific observation scope.", file=sys.stderr)
            return 1
        args.all = True
        has_scope = True
    if args.repair and args.all and not args.all_repair:
        print("Repair mode requires a narrow scope unless --all-repair is explicitly passed.", file=sys.stderr)
        return 1
    if not has_scope:
        print("Select --local-observation-id, --cloud-observation-id, or --all.", file=sys.stderr)
        return 1

    try:
        run_audit(args)
    except Exception as exc:
        print(f"Audit failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
