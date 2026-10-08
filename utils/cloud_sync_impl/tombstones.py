"""Local image tombstones and the pending tombstone flush.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import sqlite3

from database.models import (
    ImageDB,
    _upsert_image_tombstone,
    get_image_tombstones_by_deleted_cloud_id,
    get_image_tombstones_by_local_image_id,
    list_pending_image_tombstones,
    mark_image_tombstone_synced,
    reconcile_legacy_publish_exclusion_tombstones,
)
from database.schema import get_connection
from datetime import (
    datetime,
    timezone,
)

from utils.cloud_sync_impl.capabilities import _owner_sync_parents_supported
from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.image_policy import (
    microscope_image_requires_owner_sync_anchor,
    microscope_image_requires_public_spore_anchor,
)
from utils.cloud_sync_impl.progress import (
    _cloud_sync_current_summary,
    _increment_sync_summary,
)


def _local_tombstoned_cloud_image_ids(cloud_image_ids: list[str] | tuple[str, ...] | set[str] | None = None) -> set[str]:
    tombstones = get_image_tombstones_by_deleted_cloud_id(cloud_image_ids)
    return set(tombstones.keys())


def _local_tombstoned_local_image_ids(local_image_ids: list[int] | tuple[int, ...] | set[int] | None = None) -> set[int]:
    tombstones = get_image_tombstones_by_local_image_id(local_image_ids)
    return set(tombstones.keys())


def _record_remote_image_tombstones(
    remote_images,
    *,
    local_observation_id: int | None = None,
    cloud_observation_id: str | None = None,
) -> set[str]:
    # Option A: keep the local active image row visible for now.
    # Recording the tombstone is enough to block reupload/recreation; local
    # hiding/deletion and any explicit confirmation flow stay deferred.
    rows = [dict(row or {}) for row in (remote_images or [])]
    tombstone_rows = [
        row
        for row in rows
        if str(row.get("id") or "").strip() and str(row.get("deleted_at") or "").strip()
    ]
    if not tombstone_rows:
        return set()

    tombstone_cloud_ids = [
        str(row.get("id") or "").strip()
        for row in tombstone_rows
        if str(row.get("id") or "").strip()
    ]
    existing_tombstones = get_image_tombstones_by_deleted_cloud_id(tombstone_cloud_ids)
    new_tombstone_cloud_ids = [
        cloud_id
        for cloud_id in dict.fromkeys(tombstone_cloud_ids)
        if cloud_id not in existing_tombstones
    ]
    _increment_sync_summary(_cloud_sync_current_summary(), 'images_deleted_remote', len(new_tombstone_cloud_ids))
    deleted_cloud_ids: set[str] = set()
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("PRAGMA table_info(images)")
            image_columns = {str(row[1] or "") for row in cursor.fetchall()}
        except Exception:
            image_columns = set()

        has_cloud_id = "cloud_id" in image_columns
        select_columns = ["id"]
        for column in ("observation_id", "image_type", "filepath", "original_filepath"):
            if column in image_columns:
                select_columns.append(column)

        local_image_sql = (
            f"SELECT {', '.join(select_columns)} FROM images WHERE cloud_id = ? LIMIT 1"
            if has_cloud_id
            else None
        )
        local_desktop_image_sql = (
            f"SELECT {', '.join(select_columns)} FROM images WHERE id = ? LIMIT 1"
        )

        for remote_image in tombstone_rows:
            cloud_image_id = str(remote_image.get("id") or "").strip()
            deleted_at = str(remote_image.get("deleted_at") or "").strip()
            resolved_local_observation_id = None
            if local_observation_id is not None:
                local_observation_id_value = _safe_int(local_observation_id)
                if local_observation_id_value > 0:
                    resolved_local_observation_id = local_observation_id_value
            local_image_row = None
            if local_image_sql:
                local_image_row = cursor.execute(local_image_sql, (cloud_image_id,)).fetchone()
            if local_image_row is None:
                desktop_image_id = _safe_int(remote_image.get("desktop_id"))
                if desktop_image_id > 0:
                    local_image_row = cursor.execute(
                        local_desktop_image_sql,
                        (desktop_image_id,),
                    ).fetchone()

            image_type = None
            filepath = None
            original_filepath = None
            if local_image_row:
                local_image_data = dict(local_image_row)
                if resolved_local_observation_id is None and "observation_id" in local_image_data:
                    local_observation_id_value = _safe_int(local_image_data.get("observation_id"))
                    if local_observation_id_value > 0:
                        resolved_local_observation_id = local_observation_id_value
                image_type = str(local_image_data.get("image_type") or "").strip() or None
                filepath = str(local_image_data.get("filepath") or "").strip() or None
                original_filepath = str(local_image_data.get("original_filepath") or "").strip() or None

            _upsert_image_tombstone(
                cursor,
                deleted_cloud_id=cloud_image_id,
                deleted_at=deleted_at,
                deleted_storage_path=_normalize_cloud_media_key(remote_image.get("storage_path")) or None,
                deleted_observation_cloud_id=(
                    str(cloud_observation_id or remote_image.get("observation_id") or "").strip() or None
                ),
                local_observation_id=resolved_local_observation_id,
                # A remote deletion must not classify a matching active image
                # as locally deleted.  The upsert keeps any pre-existing ID
                # from a genuine local deletion via COALESCE.
                local_image_id=None,
                image_type=image_type,
                filepath=filepath,
                original_filepath=original_filepath,
            )
            deleted_cloud_ids.add(cloud_image_id)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return deleted_cloud_ids


def _tombstoned_cloud_image_warning(local_id: int | None, cloud_image_id: str) -> str:
    return f"obs {int(local_id or 0)}: skipped cloud image {cloud_image_id} because it has a local tombstone"


def _push_pending_image_tombstones(client: "SporelyCloudClient") -> list[str]:
    warnings: list[str] = []
    # Compatibility repair: older versions may have queued a cloud deletion
    # merely because an external-publish checkbox was unchecked.
    repaired = reconcile_legacy_publish_exclusion_tombstones()
    repaired_pending = int(repaired.get("pending") or 0)
    repaired_synced = int(repaired.get("synced") or 0)
    if repaired_pending or repaired_synced:
        print(
            "[cloud_sync] Reconciled legacy external-publish tombstones: "
            f"{repaired_pending} pending, {repaired_synced} previously synced"
        )
    pending = list_pending_image_tombstones()
    if pending:
        print(f"[cloud_sync] Image tombstones pending push: {len(pending)}", flush=True)
    protected_count = 0
    soft_deleted: list[str] = []
    for tombstone in pending:
        cloud_image_id = str(tombstone.get('deleted_cloud_id') or '').strip()
        if not cloud_image_id:
            continue
        local_image_id = _safe_int(tombstone.get('local_image_id'))
        if local_image_id > 0 and (
            microscope_image_requires_public_spore_anchor(local_image_id)
            or (
                microscope_image_requires_owner_sync_anchor(local_image_id)
                and _owner_sync_parents_supported(client)
            )
        ):
            try:
                ImageDB.clear_image_tombstone_by_deleted_cloud_id(cloud_image_id)
            except Exception as exc:
                warning = (
                    f"obs {int(tombstone.get('local_observation_id') or 0)}: "
                    f"could not cancel protected microscope anchor tombstone "
                    f"{cloud_image_id}: {exc}"
                )
                warnings.append(warning)
                print(f'[cloud_sync] Warning: {warning}')
            else:
                protected_count += 1
                print(
                    f'[cloud_sync] Cancelled tombstone for microscope image '
                    f'{local_image_id}: public spore metadata anchor required',
                    flush=True,
                )
            continue
        deleted_at = str(tombstone.get('deleted_at') or '').strip() or datetime.now(timezone.utc).isoformat()
        try:
            client.soft_delete_image(cloud_image_id, deleted_at)
        except Exception as exc:
            warning = (
                f"obs {int(tombstone.get('local_observation_id') or 0)}: "
                f"could not sync cloud image tombstone {cloud_image_id}: {exc}"
            )
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        try:
            mark_image_tombstone_synced(cloud_image_id)
        except Exception as exc:
            warning = (
                f"obs {int(tombstone.get('local_observation_id') or 0)}: "
                f"synced cloud image tombstone {cloud_image_id} but could not mark it locally: {exc}"
            )
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        soft_deleted.append(cloud_image_id)
    if pending:
        # One summary line makes the delete flow diagnosable from logs
        # without dumping cloud IDs into normal-path noise.
        print(
            "[cloud_sync] Image tombstone push complete: "
            f"soft_deleted={len(soft_deleted)}, "
            f"protected_microscope={protected_count}, "
            f"failed={len(warnings)}",
            flush=True,
        )
        if soft_deleted:
            print(
                "[cloud_sync] Soft-deleted cloud image ids: "
                + ", ".join(soft_deleted),
                flush=True,
            )
    return warnings
