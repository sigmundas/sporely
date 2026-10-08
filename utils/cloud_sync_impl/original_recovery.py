"""Full-resolution original recovery and its local recovery cache.

Every caller of the recovery cache is this recovery path. Cache bytes are
never re-uploaded.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from app_identity import app_data_dir
from database.models import ImageDB, ObservationDB
from utils.original_sync_policy import (
    is_full_resolution_original_sync_enabled,
    should_download_full_original,
)

from utils.cloud_sync_impl.baseline import (
    _load_cloud_observation_snapshot,
    _parse_cloud_observation_snapshot,
)
from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _normalize_slug,
    _safe_int,
)
from utils.cloud_sync_impl.errors import CloudSyncError
from utils.cloud_sync_impl.local_files import _resolve_existing_local_image_asset_path
from utils.cloud_sync_impl.progress import _cloud_sync_current_profiler, _emit_progress


def _sanitize_original_recovery_cache_component(value: str | None, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(text).name).strip("._")
    return cleaned or fallback


def _normalize_original_recovery_cache_extension(value: str | None, fallback: str = ".jpg") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    if not text.startswith("."):
        text = f".{text}"
    cleaned = re.sub(r"[^a-z0-9.]+", "", text)
    if not cleaned or cleaned == ".":
        return fallback
    if not cleaned.startswith("."):
        cleaned = f".{cleaned.lstrip('.')}"
    return cleaned if cleaned != "." else fallback


def _original_recovery_cache_root() -> Path:
    return app_data_dir() / "cloud_cache" / "originals"


def _original_recovery_cache_path(
    user_id: str | None,
    observation_cloud_id: str | None,
    image_cloud_id: str | None,
    original_storage_path: str | None,
    *,
    original_filename: str | None = None,
    local_image_id: int | None = None,
) -> Path:
    storage_name = Path(str(original_storage_path or "").strip()).name
    filename_source = str(original_filename or "").strip() or storage_name
    fallback_name = f"original_{_safe_int(local_image_id) or 'image'}"
    base_name = Path(filename_source or fallback_name).name or fallback_name
    stem = _sanitize_original_recovery_cache_component(Path(base_name).stem or base_name, "original")
    suffix = _normalize_original_recovery_cache_extension(Path(base_name).suffix or None, ".jpg")
    filename = f"{stem}{suffix or '.jpg'}"
    user_component = _sanitize_original_recovery_cache_component(user_id, "unknown_user")
    observation_component = _sanitize_original_recovery_cache_component(
        observation_cloud_id,
        "unknown_observation",
    )
    image_component = _sanitize_original_recovery_cache_component(
        image_cloud_id or f"desktop_{_safe_int(local_image_id) or 'unknown'}",
        "unknown_image",
    )
    return _original_recovery_cache_root() / user_component / observation_component / image_component / filename


def _original_recovery_sidecar_path(cache_path: Path) -> Path:
    return cache_path.with_name(f"{cache_path.name}.json")


def _write_original_recovery_sidecar(
    cache_path: Path,
    *,
    local_image: dict | None,
    local_observation: dict | None,
    remote_image: dict | None,
    original_storage_path: str,
    bytes_downloaded: int,
) -> Path:
    payload = {
        "asset_type": "full_original_recovery_cache",
        "source_role": "cloud_recovery_cache",
        "file_purpose": "cache",
        "recovered_from_source_role": str((local_image or {}).get("source_role") or "").strip() or None,
        "recovered_from_file_purpose": str((local_image or {}).get("file_purpose") or "").strip() or None,
        "local_image_id": _safe_int((local_image or {}).get("id")) or None,
        "local_observation_id": _safe_int((local_observation or {}).get("id")) or None,
        "cloud_image_id": str((remote_image or {}).get("id") or "").strip() or None,
        "cloud_observation_id": str((local_observation or {}).get("cloud_id") or "").strip() or None,
        "original_storage_path": str(original_storage_path or "").strip() or None,
        "downloaded_path": str(cache_path),
        "bytes": max(0, int(bytes_downloaded)),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    sidecar_path = _original_recovery_sidecar_path(cache_path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return sidecar_path


def _local_image_existing_original_path(local_image: dict | None) -> Path | None:
    row = dict(local_image or {})
    source_role = _normalize_slug(row.get("source_role"))
    if source_role == "local_canonical":
        readable_path = _resolve_existing_local_image_asset_path(row.get("filepath"))
        if readable_path is not None:
            return readable_path
        readable_original = _resolve_existing_local_image_asset_path(row.get("original_filepath"))
        if readable_original is not None:
            return readable_original
    elif source_role == "converted_local":
        readable_original = _resolve_existing_local_image_asset_path(row.get("original_filepath"))
        if readable_original is not None:
            return readable_original
    return None


def _remote_image_matches_local_image(remote_image: dict | None, local_image: dict | None) -> bool:
    remote = dict(remote_image or {})
    local = dict(local_image or {})
    remote_cloud_id = str(remote.get("id") or "").strip()
    local_cloud_id = str(local.get("cloud_id") or "").strip()
    if remote_cloud_id and local_cloud_id and remote_cloud_id == local_cloud_id:
        return True
    remote_desktop_id = _safe_int(remote.get("desktop_id"))
    local_image_id = _safe_int(local.get("id"))
    return remote_desktop_id > 0 and local_image_id > 0 and remote_desktop_id == local_image_id


def _load_remote_original_recovery_image_rows(
    client: "SporelyCloudClient",
    observation_cloud_id: str,
    *,
    prefer_snapshot: bool = True,
) -> list[dict]:
    cloud_value = str(observation_cloud_id or "").strip()
    if not cloud_value:
        return []

    rows: list[dict] = []
    if prefer_snapshot:
        snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
        rows = [dict(row or {}) for row in (snapshot.get("images") or [])]
        if rows:
            return rows

    try:
        rows = [dict(row or {}) for row in (client.pull_image_metadata(cloud_value, include_deleted_for_sync=True) or [])]
    except Exception:
        rows = []
    return rows


def _find_remote_original_for_local_image(
    client: "SporelyCloudClient",
    local_image: dict,
    observation_cloud_id: str,
) -> dict | None:
    snapshot_rows = _load_remote_original_recovery_image_rows(client, observation_cloud_id, prefer_snapshot=True)
    snapshot_match: dict | None = None
    for remote_image in snapshot_rows:
        if _remote_image_matches_local_image(remote_image, local_image):
            snapshot_match = remote_image
            if _normalize_cloud_media_key(
                remote_image.get("original_storage_path") or remote_image.get("original_image_key")
            ):
                return remote_image
            break
    live_rows = _load_remote_original_recovery_image_rows(client, observation_cloud_id, prefer_snapshot=False)
    for remote_image in live_rows:
        if _remote_image_matches_local_image(remote_image, local_image):
            return remote_image
    return snapshot_match


def recover_full_original_for_image(
    client: "SporelyCloudClient",
    local_image_id: int | str,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    """Recover a cloud original into the local cache without touching the canonical file."""
    local_id = _safe_int(local_image_id)
    result: dict[str, object] = {
        "status": "skipped",
        "skipped_reason": None,
        "downloaded_path": None,
        "bytes": 0,
        "used_original_storage_path": None,
        "warnings": [],
        "errors": [],
        "cache_sidecar_path": None,
        "local_image_id": local_id,
        "cloud_image_id": None,
        "cloud_observation_id": None,
    }

    profiler = _cloud_sync_current_profiler()
    progress_state = {"done": 0, "total": 1}

    def _add_warning(message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        result["warnings"].append(text)

    def _add_error(message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        result["errors"].append(text)

    if local_id <= 0:
        result["status"] = "skipped_invalid_local_image_id"
        result["skipped_reason"] = "invalid_local_image_id"
        return result

    local_image = ImageDB.get_image(local_id)
    if not local_image:
        result["status"] = "skipped_local_image_missing"
        result["skipped_reason"] = "local_image_missing"
        return result

    local_observation_id = _safe_int(local_image.get("observation_id"))
    local_observation = ObservationDB.get_observation(local_observation_id) if local_observation_id > 0 else None
    cloud_observation_id = str((local_observation or {}).get("cloud_id") or "").strip()
    result["cloud_observation_id"] = cloud_observation_id or None

    if not cloud_observation_id:
        result["status"] = "skipped_missing_cloud_link"
        result["skipped_reason"] = "missing_cloud_link"
        return result

    if not is_full_resolution_original_sync_enabled():
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_disabled()
            except Exception:
                pass
        result["status"] = "skipped_disabled"
        result["skipped_reason"] = "disabled"
        return result

    canonical_original_path = _local_image_existing_original_path(local_image)
    if canonical_original_path is not None:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_existing_local_original()
            except Exception:
                pass
        result["status"] = "skipped_existing_local_original"
        result["skipped_reason"] = "existing_local_original"
        return result

    _emit_progress(
        progress_cb,
        f"Recovering full-resolution original for image {local_id}…",
        progress_state,
    )

    remote_image = _find_remote_original_for_local_image(client, local_image, cloud_observation_id)
    if not remote_image:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_missing_remote_image"
        result["skipped_reason"] = "missing_remote_image"
        return result

    remote_original_storage_path = _normalize_cloud_media_key(
        remote_image.get("original_storage_path") or remote_image.get("original_image_key")
    )
    result["cloud_image_id"] = str(remote_image.get("id") or "").strip() or None
    result["used_original_storage_path"] = remote_original_storage_path or None

    if not remote_original_storage_path:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_missing_key"
        result["skipped_reason"] = "missing_original_storage_path"
        return result

    policy_allows = should_download_full_original(remote_image, local_image)
    if not policy_allows:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_missing_key()
            except Exception:
                pass
        result["status"] = "skipped_policy_rejected"
        result["skipped_reason"] = "policy_rejected"
        return result

    cache_path = _original_recovery_cache_path(
        client.user_id,
        cloud_observation_id,
        result["cloud_image_id"] or f"desktop_{local_id}",
        remote_original_storage_path,
        original_filename=str(remote_image.get("original_filename") or "").strip() or None,
        local_image_id=local_id,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    existing_cache_path = _resolve_existing_local_image_asset_path(str(cache_path))
    if existing_cache_path is not None:
        if profiler is not None:
            try:
                profiler.record_original_download_skipped_existing_cache()
            except Exception:
                pass
        try:
            bytes_downloaded = int(existing_cache_path.stat().st_size)
        except Exception:
            bytes_downloaded = 0
        try:
            sidecar_path = _write_original_recovery_sidecar(
                existing_cache_path,
                local_image=local_image,
                local_observation=local_observation,
                remote_image=remote_image,
                original_storage_path=remote_original_storage_path,
                bytes_downloaded=bytes_downloaded,
            )
        except Exception as exc:
            _add_warning(f"obs {local_id}: could not refresh original recovery sidecar ({exc})")
            sidecar_path = None
        result.update(
            {
                "status": "skipped_existing_cache",
                "skipped_reason": "existing_cache",
                "downloaded_path": existing_cache_path,
                "bytes": bytes_downloaded,
                "cache_sidecar_path": sidecar_path,
            }
        )
        _emit_progress(
            progress_cb,
            f"Recovered original already cached for image {local_id}",
            progress_state,
        )
        return result

    temp_path: Path | None = None
    downloaded_path: Path | None = None
    try:
        cache_suffix = cache_path.suffix or ".bin"
        with tempfile.NamedTemporaryFile(
            dir=cache_path.parent,
            prefix=f"{cache_path.stem}_",
            suffix=cache_suffix,
            delete=False,
        ) as tmp:
            temp_path = Path(tmp.name)

        downloaded_temp = Path(client.download_image_file(remote_original_storage_path, temp_path))
        if not downloaded_temp.exists() or not downloaded_temp.is_file() or not os.access(downloaded_temp, os.R_OK):
            raise CloudSyncError("downloaded original image was not written")

        downloaded_path = cache_path
        if downloaded_temp != cache_path:
            if cache_path.exists():
                cache_path.unlink()
            downloaded_temp.replace(cache_path)
            downloaded_path = cache_path

        if not downloaded_path.exists() or not downloaded_path.is_file() or not os.access(downloaded_path, os.R_OK):
            raise CloudSyncError("downloaded original cache file is not readable")

        bytes_downloaded = 0
        try:
            bytes_downloaded = int(downloaded_path.stat().st_size)
        except Exception:
            bytes_downloaded = 0

        try:
            sidecar_path = _write_original_recovery_sidecar(
                downloaded_path,
                local_image=local_image,
                local_observation=local_observation,
                remote_image=remote_image,
                original_storage_path=remote_original_storage_path,
                bytes_downloaded=bytes_downloaded,
            )
        except Exception as exc:
            _add_warning(f"obs {local_id}: recovered original but could not write sidecar ({exc})")
            sidecar_path = None

        if profiler is not None:
            try:
                profiler.record_original_download_success(bytes_downloaded)
            except Exception:
                pass

        result.update(
            {
                "status": "downloaded_to_cache",
                "skipped_reason": None,
                "downloaded_path": downloaded_path,
                "bytes": bytes_downloaded,
                "cache_sidecar_path": sidecar_path,
            }
        )
        _emit_progress(
            progress_cb,
            f"Recovered full-resolution original for image {local_id}",
            progress_state,
        )
        return result
    except Exception as exc:
        if profiler is not None:
            try:
                profiler.record_original_download_failed()
            except Exception:
                pass
        detail = str(exc or "").strip() or exc.__class__.__name__
        _add_error(f"obs {local_id}: original recovery failed ({detail})")
        result["status"] = "download_failed"
        result["skipped_reason"] = None
        result["downloaded_path"] = None
        result["bytes"] = 0
        return result
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
