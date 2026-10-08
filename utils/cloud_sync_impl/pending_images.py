"""Pending local image decisions and the pending-image repair scan.

``_CLOUD_PENDING_IMAGE_REPAIR_VERSION`` lives with the scan; it changes only
when the scan's selection changes.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database.models import SettingsDB, mark_observation_sync_dirty
from database.schema import get_connection

from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.image_policy import (
    _cloud_explicit_media_upload_selection,
    _cloud_image_storage_intent_initialized_ids,
    _ensure_cloud_image_storage_intent_initialized,
    _is_generated_cloud_image,
)
from utils.cloud_sync_impl.progress import (
    _cloud_sync_current_summary,
    _increment_sync_summary,
)
from utils.cloud_sync_impl.reconciliation.values import _parse_sync_timestamp
from utils.cloud_sync_impl.sync_state import _clear_local_cloud_media_signature


_CLOUD_PENDING_IMAGE_REPAIR_VERSION_SETTING = 'cloud_pending_image_repair_version'


_CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING = 'cloud_pending_image_repair_at'


# Repair generation. Bump this when a fix changes which observations the
# pending-image scan can actually rescue, so installations that already hold a
# fresh watermark still perform one new scan on their next explicit
# sync_images=True synchronization. After that transition the ordinary
# interval throttling below resumes.
# v1: original versioned pending-image repair scan.
# v2: mosaic fix — an unchanged local render signature no longer implies the
#     media was uploaded, so already-synced observations stranded with
#     cloud_id IS NULL media become recoverable.
_CLOUD_PENDING_IMAGE_REPAIR_VERSION = 2


_CLOUD_PENDING_IMAGE_REPAIR_INTERVAL_HOURS = 24


def _cloud_pending_image_repair_scan_due(
    now: datetime | None = None,
) -> tuple[bool, str]:
    """Schedule the legacy/interrupted-state scan outside ordinary no-op syncs."""
    stored_version = _safe_int(
        SettingsDB.get_setting(_CLOUD_PENDING_IMAGE_REPAIR_VERSION_SETTING, '')
    )
    if stored_version != _CLOUD_PENDING_IMAGE_REPAIR_VERSION:
        return True, f'version_{stored_version}_to_{_CLOUD_PENDING_IMAGE_REPAIR_VERSION}'

    last_text = str(
        SettingsDB.get_setting(_CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING, '') or ''
    ).strip()
    last_at = _parse_sync_timestamp(last_text)
    if last_at is None:
        return True, 'missing_or_invalid_watermark'

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    stale_before = current.astimezone(timezone.utc) - timedelta(
        hours=_CLOUD_PENDING_IMAGE_REPAIR_INTERVAL_HOURS
    )
    if last_at <= stale_before:
        return True, 'stale_watermark'
    return False, 'fresh_watermark'


def _record_cloud_pending_image_repair_scan_complete() -> bool:
    completed_at = datetime.now(timezone.utc).isoformat()
    try:
        SettingsDB.set_setting(
            _CLOUD_PENDING_IMAGE_REPAIR_VERSION_SETTING,
            str(_CLOUD_PENDING_IMAGE_REPAIR_VERSION),
        )
        SettingsDB.set_setting(_CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING, completed_at)
    except Exception as exc:
        print(
            f'[cloud_sync] pending image repair watermark persist failed: {exc}',
            flush=True,
        )
        return False
    return True


def _cloud_publish_path_key(path: str | None) -> str:
    if not path:
        return ""
    try:
        return str(Path(path).resolve()).lower()
    except Exception:
        return str(Path(path)).lower()


PendingImageDecision = dict  # {"pending": bool, "reason": str}


# Reasons emitted by explain_pending_cloud_image_decision. Kept as string
# constants so callers can group-count them in diagnostic summaries.
PENDING_REASON_PENDING_UPLOAD = "pending_upload"


PENDING_REASON_ALREADY_SYNCED = "skipped_already_synced"


PENDING_REASON_WRONG_TYPE = "skipped_wrong_type"


PENDING_REASON_GENERATED = "skipped_generated_cloud_image"


PENDING_REASON_EXCLUDED = "skipped_excluded_by_user"


# Stage 1: split the "user has not requested a cloud copy" reason out of the
# generic exclusion bucket so diagnostic reports and callers can distinguish
# gallery-desired-state rejects from any other exclusion input.
PENDING_REASON_NOT_DESIRED_BY_USER = "skipped_not_desired_by_user"


# A row whose storage intent has never been assigned (not in the per-image
# ledger) must never be interpreted as pending cloud media: an unseeded
# excluded set makes everything look desired.
PENDING_REASON_INTENT_UNINITIALIZED = "skipped_storage_intent_uninitialized"


PENDING_REASON_DUPLICATE = "skipped_duplicate_path"


PENDING_REASON_MISSING_FILE = "skipped_missing_file"


PENDING_REASON_CACHE_ROW = "skipped_cloud_cache_row"


PENDING_REASON_MICROSCOPE_NO_MEASUREMENTS = "skipped_microscope_no_measurements"


def explain_pending_cloud_image_decision(
    row: dict,
    *,
    seen_paths: set[str],
    excluded_ids: set[int],
    image_measurement_counts: dict[int, int] | None = None,
    explicit_media_upload_selection: set[int] | None = None,
    initialized_intent_ids: set[int] | None = None,
) -> PendingImageDecision:
    """Shared predicate: should this image row upload its bytes to cloud right now?

    Used by both the dirty-scan (`_mark_cloud_observations_dirty_for_pending_local_images`
    → `_pending_cloud_pushable_image_ids`) and the upload-collection code path so
    both agree on which rows are pending. If they disagree the dirty-scan can
    perpetually re-dirty observations over rows that upload will skip anyway.

    Policy (matches the ``sync_images=True`` explicit media-upload semantics):

      * Field and microscope image bytes are eligible only when their thumbnail
        gallery checkbox is selected. Spore measurements never override an
        unchecked image; their metadata uses a metadata-only microscope anchor.
      * Cloud-cache rows (`source_role=cloud_recovery_cache` or
        `file_purpose=cache`): never re-upload bytes; they're stubs that only
        need metadata patches, which the sync handles separately.
      * Any image that already has ``cloud_id`` set: not pending — it already
        exists on cloud.

    The helper mutates ``seen_paths`` when it accepts the row so duplicate paths
    within the same observation collapse to a single upload.
    """
    reasons: dict[str, bool] = {}
    image_id = _safe_int(row.get("id"))
    if image_id <= 0:
        return {"pending": False, "reason": PENDING_REASON_WRONG_TYPE}

    image_type = str(row.get("image_type") or "").strip().lower()
    if image_type not in {"field", "microscope"}:
        return {"pending": False, "reason": PENDING_REASON_WRONG_TYPE}

    if _is_generated_cloud_image(row):
        return {"pending": False, "reason": PENDING_REASON_GENERATED}

    if image_id in excluded_ids:
        return {"pending": False, "reason": PENDING_REASON_EXCLUDED}

    if str(row.get("cloud_id") or "").strip():
        filepath = str(row.get("filepath") or row.get("original_filepath") or "").strip()
        path_key = _cloud_publish_path_key(filepath) if filepath else ""
        if path_key:
            # Synced canonical rows own their filepath too. Reserving it here
            # prevents a duplicate cloud_id-null row from being re-dirtied and
            # uploaded as a second copy of the same local file.
            seen_paths.add(path_key)
        return {"pending": False, "reason": PENDING_REASON_ALREADY_SYNCED}

    source_role = str(row.get("source_role") or "").strip().lower()
    file_purpose = str(row.get("file_purpose") or "").strip().lower()
    is_cloud_origin = source_role == "cloud_recovery_cache" or file_purpose == "cache"

    if (
        not is_cloud_origin
        and initialized_intent_ids is not None
        and image_id not in initialized_intent_ids
    ):
        # No storage-intent decision exists for this row yet. Refuse to treat
        # it as pending — the caller must seed intent first.
        return {"pending": False, "reason": PENDING_REASON_INTENT_UNINITIALIZED}

    explicit = explicit_media_upload_selection
    if not is_cloud_origin and explicit is not None and image_id not in explicit:
        return {"pending": False, "reason": PENDING_REASON_NOT_DESIRED_BY_USER}

    filepath = str(row.get("filepath") or row.get("original_filepath") or "").strip()
    if not is_cloud_origin and (not filepath or not Path(filepath).exists()):
        # Non cloud-origin rows need a local file to encode. Cache rows are
        # allowed through without a file — the sync will repair their cloud_id
        # via a metadata patch.
        return {"pending": False, "reason": PENDING_REASON_MISSING_FILE}

    path_key = _cloud_publish_path_key(filepath) if filepath else ""
    if path_key and path_key in seen_paths:
        return {"pending": False, "reason": PENDING_REASON_DUPLICATE}

    if image_type == "microscope" and not is_cloud_origin and explicit is None:
        # Callers that do not provide the persisted gallery selection must not
        # infer byte-upload consent from the presence of measurements.
        return {
            "pending": False,
            "reason": PENDING_REASON_MICROSCOPE_NO_MEASUREMENTS,
        }

    if path_key:
        seen_paths.add(path_key)
    return {"pending": True, "reason": PENDING_REASON_PENDING_UPLOAD}


def _measurement_counts_for_observation_images(observation_id: int) -> dict[int, int]:
    """Return image_id → count of spore_measurements. Empty on any DB error."""
    counts: dict[int, int] = {}
    try:
        conn = get_connection()
        try:
            for row in conn.execute(
                "SELECT image_id, COUNT(*) AS n FROM spore_measurements "
                "WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?) "
                "GROUP BY image_id",
                (int(observation_id),),
            ).fetchall():
                counts[_safe_int(row[0])] = int(row[1] or 0)
        finally:
            conn.close()
    except Exception as exc:
        print(f"[cloud_sync] Could not fetch measurement counts for obs {observation_id}: {exc}")
    return counts


def _pending_cloud_pushable_image_ids(
    observation_id: int,
    *,
    explicit_media_upload_selection: set[int] | None = None,
    diagnostic_log: bool = False,
) -> list[int]:
    """Image ids still missing a cloud_id that cloud sync would actually push.

    Uses :func:`explain_pending_cloud_image_decision` so this stays in lock-step
    with the upload-collection predicate. External-publish exclusions are
    intentionally ignored. Any row the upload path would otherwise skip
    (duplicate, missing file, cache row, microscope-no-measurements) is dropped
    here too, so the dirty-scan cannot perpetually re-dirty observations over
    rows the sync intentionally leaves local.
    """
    excluded_ids: set[int] = set()

    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        existing_cols = {
            str(info["name"])
            for info in conn.execute("PRAGMA table_info(images)").fetchall()
        }
        if "id" not in existing_cols or "image_type" not in existing_cols:
            return []
        wanted = [
            col
            for col in (
                "id", "image_type", "cloud_id", "filepath", "original_filepath",
                "source_role", "file_purpose", "notes", "sort_order",
            )
            if col in existing_cols
        ]
        order_bits: list[str] = []
        if "sort_order" in existing_cols:
            order_bits.append("CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END")
            order_bits.append("sort_order")
        order_bits.append("id")
        rows = conn.execute(
            f"SELECT {', '.join(wanted)} FROM images WHERE observation_id = ? "
            f"ORDER BY {', '.join(order_bits)}",
            (int(observation_id),),
        ).fetchall()
    finally:
        conn.close()

    measurement_counts = _measurement_counts_for_observation_images(observation_id)
    # Only rows with recorded storage intent may be evaluated as pending. A
    # caller-provided explicit selection IS storage intent for those ids
    # (e.g. an explicit "upload media" request), so it counts as initialized;
    # the derived default selection computed below does not.
    initialized_intent_ids = _cloud_image_storage_intent_initialized_ids(observation_id)
    if explicit_media_upload_selection is None:
        explicit_media_upload_selection = _cloud_explicit_media_upload_selection(observation_id)
    else:
        initialized_intent_ids = initialized_intent_ids | {
            _safe_int(value)
            for value in explicit_media_upload_selection
            if _safe_int(value) > 0
        }

    pending: list[int] = []
    seen_paths: set[str] = set()
    reason_counts: dict[str, int] = {}
    for image in rows or []:
        row = dict(image)
        decision = explain_pending_cloud_image_decision(
            row,
            seen_paths=seen_paths,
            excluded_ids=excluded_ids,
            image_measurement_counts=measurement_counts,
            explicit_media_upload_selection=explicit_media_upload_selection,
            initialized_intent_ids=initialized_intent_ids,
        )
        reason = decision.get("reason") or ""
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if decision.get("pending"):
            pending.append(_safe_int(row.get("id")))

    if diagnostic_log and rows:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(reason_counts.items()))
        print(
            f"[cloud_sync] pending scan obs {observation_id}: rows={len(rows)} "
            f"pending={len(pending)} {summary}"
        )

    return pending


def _mark_cloud_observations_dirty_for_pending_local_images(
    *,
    include_pending_local_media_uploads: bool = False,
    explicit_media_upload_selection: set[int] | None = None,
    diagnostic_log: bool = False,
) -> bool:
    """Mark synced observations dirty when they still have cloud-eligible local images.

    Only runs when the caller explicitly requests a media-upload pass. Metadata-only
    background sync must NOT re-dirty observations over local rows that have no
    cloud_id — those rows are treated as local-only until the user explicitly runs
    "Upload media". Otherwise a fresh install with hundreds of pre-existing local
    microscope photos would surface as "everything dirty" and start pushing bytes.

    Callers pass ``include_pending_local_media_uploads=True`` from the explicit
    media-upload code path. All other callers (Refresh, auto-sync) must leave the
    default False so the scan is a no-op.
    """
    if not include_pending_local_media_uploads:
        return True
    dirty_ids: list[int] = []
    scan_completed = True
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT DISTINCT o.id AS observation_id
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            WHERE o.cloud_id IS NOT NULL
              AND i.cloud_id IS NULL
              AND i.image_type IN ('field', 'microscope')
            ORDER BY o.id
            """
        ).fetchall()
        candidate_ids = [
            _safe_int(dict(row or {}).get("observation_id"))
            for row in rows or []
        ]
    except Exception as exc:
        print(f"[cloud_sync] Could not mark observations dirty for pending local images: {exc}")
        candidate_ids = []
        scan_completed = False
    finally:
        conn.close()

    for obs_id in candidate_ids:
        if obs_id <= 0:
            continue
        # Incremental storage-intent initialization MUST run before any
        # cloud-null row here can be interpreted as pending cloud media. A
        # newly-imported microscope image must never appear desired merely
        # because the excluded set has not been seeded yet. Fail closed: if
        # seeding fails, skip this observation instead of evaluating it.
        try:
            _ensure_cloud_image_storage_intent_initialized(obs_id)
        except Exception as exc:
            print(
                f"[cloud_sync] Could not initialize storage intent for observation {obs_id}: {exc}"
            )
            scan_completed = False
            continue
        try:
            pending_ids = _pending_cloud_pushable_image_ids(
                obs_id,
                explicit_media_upload_selection=explicit_media_upload_selection,
                diagnostic_log=diagnostic_log,
            )
        except Exception as exc:
            print(
                f"[cloud_sync] Could not evaluate pending local images for observation {obs_id}: {exc}"
            )
            scan_completed = False
            continue
        pending_count = len(pending_ids)
        if pending_count > 0:
            print(
                f"[cloud_sync] Observation {obs_id}: re-dirtied because "
                f"{pending_count} cloud-eligible local image row(s) still have cloud_id IS NULL"
            )
            dirty_ids.append(obs_id)
    if not dirty_ids:
        return scan_completed

    _increment_sync_summary(
        _cloud_sync_current_summary(),
        'observations_redirtied_pending_local_images',
        len(dirty_ids),
    )

    for obs_id in dirty_ids:
        try:
            _clear_local_cloud_media_signature(obs_id)
        except Exception as exc:
            print(
                f"[cloud_sync] Could not clear local media signature for observation {obs_id}: {exc}"
            )

    conn = get_connection()
    try:
        cursor = conn.cursor()
        for obs_id in dirty_ids:
            mark_observation_sync_dirty(cursor, obs_id)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"[cloud_sync] Could not update dirty state for pending local images: {exc}")
        scan_completed = False
    finally:
        conn.close()
    return scan_completed
