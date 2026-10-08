"""Image push for one observation and its helpers.

Order: intent initialization, identity and link, tombstone/protection filter,
prep, metadata reserve or create, byte upload, metadata finalize, local
``cloud_id`` bookkeeping. ``prepared_items`` is never desired-state truth.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from database.models import ImageDB
from database.schema import get_connection
from utils.original_sync_policy import (
    FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES,
    is_full_resolution_original_sync_enabled,
    is_full_resolution_original_upload_too_large,
    resolve_full_original_upload_source,
)
from utils.r2_storage import media_variant_key

from utils.cloud_sync_impl.anchors import (
    _ensure_metadata_anchors_for_public_spore_observation,
    _reserve_anchor_promotion_key,
    _rollback_anchor_promotion,
)
from utils.cloud_sync_impl.common import (
    _format_size,
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.errors import (
    CloudImageBytesNotDesiredError,
    CloudSyncError,
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
    is_image_too_large_for_plan_error,
    is_webp_support_required_for_cloud_media_upload_error,
)
from utils.cloud_sync_impl.image_files import (
    _build_worker_storage_path,
    _file_content_signature,
)
from utils.cloud_sync_impl.image_identity import _reconcile_local_image_cloud_id
from utils.cloud_sync_impl.image_payloads import _remote_image_payload
from utils.cloud_sync_impl.image_policy import (
    _ensure_cloud_image_storage_intent_initialized,
    _is_generated_cloud_image,
    cloud_image_bytes_desired,
)
from utils.cloud_sync_impl.media_signature import (
    _parsed_local_media_signature,
    _path_stat_signature,
)
from utils.cloud_sync_impl.preflight import _image_calibration_uuid
from utils.cloud_sync_impl.progress import (
    _advance_progress,
    _cloud_sync_current_profiler,
    _cloud_sync_current_summary,
    _emit_progress,
    _extend_progress_total,
    _increment_sync_summary,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
    _normalize_snapshot_value,
    _parse_sync_timestamp,
)
from utils.cloud_sync_impl.sample_source import _cloud_to_desktop_sample_source
from utils.cloud_sync_impl.status_messages import _format_cloud_sync_observation_status
from utils.cloud_sync_impl.sync_state import (
    _clear_pending_image_promotion_key,
    _load_cloud_image_file_signature,
    _load_local_cloud_media_signature,
    _load_pending_image_promotion_key,
    _set_cloud_image_metadata_only_state,
    _store_cloud_image_file_signature,
)
from utils.cloud_sync_impl.tombstones import (
    _local_tombstoned_cloud_image_ids,
    _local_tombstoned_local_image_ids,
    _push_pending_image_tombstones,
    _tombstoned_cloud_image_warning,
)


def should_push_local_image_to_cloud(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if _is_generated_cloud_image(row):
        return False
    source_role = str(row.get('source_role') or '').strip().lower()
    file_purpose = str(row.get('file_purpose') or '').strip().lower()
    if source_role == 'cloud_recovery_cache' or file_purpose == 'cache':
        # Cloud-imported rows are cached locally, but they are still the
        # canonical syncable image rows and must remain publishable so the
        # cloud copy can be preserved or restored.
        return True
    return True


def _prepared_item_remote_payload(
    image_row: dict,
    upload_path: str,
    storage_path: str,
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    normalized_key = _normalize_cloud_media_key(storage_path)
    payload = {
        'id': _normalize_snapshot_value(image_row.get('cloud_id')),
        'desktop_id': _safe_int(image_row.get('id')),
        'sort_order': _normalize_snapshot_value(image_row.get('sort_order')),
        'image_type': _normalize_snapshot_value(image_row.get('image_type')),
        'micro_category': _normalize_snapshot_value(image_row.get('micro_category')),
        'captured_at': _normalize_image_captured_at_for_cloud(
            image_row.get('captured_at'), local=True
        ),
        'calibration_uuid': _normalize_snapshot_value(_image_calibration_uuid(image_row)),
        'objective_name': _normalize_snapshot_value(image_row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image_row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image_row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image_row.get('mount_medium')),
        'stain': _normalize_snapshot_value(image_row.get('stain')),
        'sample_type': _normalize_snapshot_value(image_row.get('sample_type')),
        # Compared against the same field on the remote payload — both sides
        # normalize to the desktop-canonical Title_Case form so lowercase
        # cloud values (`spore_print`) match Title_Case local values
        # (`Spore_print`) without triggering a false diff.
        'sample_source': _normalize_snapshot_value(
            _cloud_to_desktop_sample_source(image_row.get('sample_source'))
        ),
        'contrast': _normalize_snapshot_value(image_row.get('contrast')),
        'measure_color': _normalize_snapshot_value(image_row.get('measure_color')),
        'crop_mode': _normalize_snapshot_value(image_row.get('crop_mode')),
        'notes': _normalize_snapshot_value(image_row.get('notes')),
        'gps_source': _normalize_snapshot_value(
            None if image_row.get('gps_source') is None else bool(image_row.get('gps_source'))
        ),
        'storage_path': _normalize_snapshot_value(normalized_key or None),
        'original_filename': _normalize_snapshot_value(Path(str(upload_path or '').strip()).name or None),
    }
    if include_ai_crop:
        payload.update({
            'ai_crop_x1': _normalize_snapshot_value(image_row.get('ai_crop_x1')),
            'ai_crop_y1': _normalize_snapshot_value(image_row.get('ai_crop_y1')),
            'ai_crop_x2': _normalize_snapshot_value(image_row.get('ai_crop_x2')),
            'ai_crop_y2': _normalize_snapshot_value(image_row.get('ai_crop_y2')),
            'ai_crop_source_w': _normalize_snapshot_value(image_row.get('ai_crop_source_w')),
            'ai_crop_source_h': _normalize_snapshot_value(image_row.get('ai_crop_source_h')),
            'ai_crop_is_custom': _normalize_snapshot_value(image_row.get('ai_crop_is_custom')),
        })
    if include_upload_meta:
        payload.update({
            'upload_mode': _normalize_snapshot_value(image_row.get('upload_mode')),
            'source_width': _normalize_snapshot_value(image_row.get('source_width')),
            'source_height': _normalize_snapshot_value(image_row.get('source_height')),
            'stored_width': _normalize_snapshot_value(image_row.get('stored_width')),
            'stored_height': _normalize_snapshot_value(image_row.get('stored_height')),
            'stored_bytes': _normalize_snapshot_value(image_row.get('stored_bytes')),
        })
    return payload


# Image rows the upload-preparation step should skip because a remote-first
# pass already re-associated them with an existing cloud image. Passed to the
# prepare callback via the observation dict so no temporary WebP candidate is
# encoded for metadata-only associations.
CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY = '_cloud_sync_skip_prepare_image_ids'


def _lookup_unlisted_remote_image_storage_path(
    client: SporelyCloudClient,
    obs_cloud_id: str,
    img: dict,
) -> str:
    """Return the storage_path of the live row this image is linked to.

    Used only when the observation's image listing failed: mirrors the
    identity legs of ``_resolve_existing_image_for_push`` (direct cloud_id,
    else desktop_id reverse link unless portable identity is pending) so a
    later PATCH keeps the row's existing key. Returns '' when none is found.
    """
    obs_value = str(obs_cloud_id or '').strip()
    cloud_id = str(img.get('cloud_id') or '').strip()
    if cloud_id:
        query = f'id=eq.{cloud_id}'
    elif not bool(img.get('portable_cloud_identity_pending')) and _safe_int(img.get('id')) > 0:
        query = f'desktop_id=eq.{_safe_int(img.get("id"))}'
        if img.get('image_type'):
            query += f'&image_type=eq.{img.get("image_type")}'
    else:
        return ''
    try:
        rows = client._get(
            f'observation_images?{query}'
            f'&user_id=eq.{client.user_id}'
            f'&observation_id=eq.{obs_value}'
            f'&deleted_at=is.null'
            f'&select=id,storage_path'
            f'&limit=2'
        ) or []
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        raise CloudSyncError(
            f'could not verify existing storage key for image {img.get("id")}: {exc}'
        ) from exc
    if len(rows) != 1:
        return ''
    return _normalize_cloud_media_key(rows[0].get('storage_path'))


def _select_remote_image_identity_candidate(
    *,
    local_image_id: int,
    local_cloud_id: str,
    existing_by_id: dict[str, dict],
    existing_by_desktop_id: dict[int, dict],
    portable_identity_pending: bool,
) -> dict | None:
    """Select only identity candidates permitted by the current guard."""
    normalized_cloud_id = str(local_cloud_id or '').strip()
    if normalized_cloud_id:
        return existing_by_id.get(normalized_cloud_id)
    if portable_identity_pending:
        return None
    return existing_by_desktop_id.get(int(local_image_id or 0))


def _associate_persisted_cloud_images(
    client: SporelyCloudClient,
    obs: dict,
    existing_rows: list[dict],
) -> set[int]:
    """Re-link orphaned local image rows to existing remote cloud images.

    Targets the repair case described in the sync bug: a local image row whose
    ``cloud_id`` was lost but which still has an unambiguous remote cloud
    image match (same ``desktop_id``, same owner, same ``image_type``). The
    local ``cloud_id`` is restored without uploading any bytes.

    Stage 1: repair is intentionally NOT gated on the cloud-storage-desired
    checkbox — even an unchecked image with a lost link should be relinked so
    the byte gate can refuse re-upload and any pending tombstone can complete
    its deletion instead of creating a duplicate.

    Returns local image ids that the upload-preparation step can skip.
    """
    associated_ids: set[int] = set()
    try:
        obs_local_id = int(obs.get('id'))
    except Exception:
        return associated_ids
    if bool(obs.get('portable_cloud_identity_pending')):
        return associated_ids

    # Bucket remote rows by desktop_id so we can detect ambiguity (2+ rows
    # sharing the same desktop_id). Ambiguous groups are skipped: identity
    # cannot be repaired safely without human intervention.
    remote_rows_by_desktop_id: dict[int, list[dict]] = {}
    for row in (existing_rows or []):
        desktop_id = _safe_int(row.get('desktop_id'))
        if desktop_id <= 0:
            continue
        remote_rows_by_desktop_id.setdefault(desktop_id, []).append(row)
    if not remote_rows_by_desktop_id:
        return associated_ids

    obs_cloud_id = str(obs.get('cloud_id') or '').strip()
    obs_owner_id = str(obs.get('user_id') or '').strip()

    include_ai_crop = client._observation_images_support_ai_crop()
    include_upload_meta = client._observation_images_support_upload_metadata()
    # Fields that describe the uploaded bytes rather than user metadata. They
    # cannot be known without encoding, so they are excluded from the
    # metadata-only match decision — the remote bytes are unchanged anyway.
    upload_derived_keys = {
        'id',
        'storage_path',
        'original_filename',
        'source_width',
        'source_height',
        'stored_width',
        'stored_height',
        'stored_bytes',
        'upload_mode',
    }

    for image_row in ImageDB.get_images_for_observation(obs_local_id):
        img = dict(image_row or {})
        img['portable_cloud_identity_pending'] = bool(
            obs.get('portable_cloud_identity_pending')
        )
        local_image_id = _safe_int(img.get('id'))
        if local_image_id <= 0 or not should_push_local_image_to_cloud(img):
            continue
        # Only repair orphaned rows. Rows that still hold a cloud_id (correct or
        # conflicting) flow through the normal path.
        if str(img.get('cloud_id') or '').strip():
            continue
        matches = remote_rows_by_desktop_id.get(local_image_id) or []
        if not matches:
            continue
        if len(matches) > 1:
            print(
                f'[cloud_sync] Observation {obs_local_id}: ambiguous cloud image match '
                f'for desktop_id={local_image_id} (found {len(matches)} candidates); '
                f'skipping identity repair'
            )
            continue
        remote_row = matches[0]
        remote_cloud_id = str(remote_row.get('id') or '').strip()
        remote_storage_path = _normalize_cloud_media_key(remote_row.get('storage_path'))
        if not remote_cloud_id:
            continue

        # Owner scoping: require either the observation cloud_id match or the
        # remote row's user_id to match the local observation's owner. Prevents
        # cross-account bleed if a stale cache leaked rows from another user.
        remote_obs_cloud_id = str(remote_row.get('observation_id') or '').strip()
        remote_owner_id = str(remote_row.get('user_id') or '').strip()
        if obs_cloud_id and remote_obs_cloud_id and obs_cloud_id != remote_obs_cloud_id:
            print(
                f'[cloud_sync] Observation {obs_local_id}: cloud image '
                f'desktop_id={local_image_id} does not match this observation '
                f'({remote_obs_cloud_id} != {obs_cloud_id}); skipping identity repair'
            )
            continue
        if obs_owner_id and remote_owner_id and obs_owner_id != remote_owner_id:
            print(
                f'[cloud_sync] Observation {obs_local_id}: cloud image '
                f'desktop_id={local_image_id} has different owner; skipping identity repair'
            )
            continue

        # image_type must agree — otherwise this is not the same image.
        local_type = str(img.get('image_type') or '').strip().lower()
        remote_type = str(remote_row.get('image_type') or '').strip().lower()
        if local_type and remote_type and local_type != remote_type:
            continue

        was_synced_previously = bool(str(img.get('synced_at') or '').strip())
        if was_synced_previously and remote_storage_path:
            # Historical repair path: descriptive metadata must already match
            # so we don't silently drop pending edits. Newly-created local
            # rows (no prior synced_at) do not have this constraint — their
            # descriptive metadata is being introduced right now and will be
            # patched by the normal reconcile step.
            expected_payload = _prepared_item_remote_payload(
                img,
                '',
                remote_storage_path,
                include_ai_crop=include_ai_crop,
                include_upload_meta=include_upload_meta,
            )
            remote_payload = _remote_image_payload(
                remote_row,
                include_ai_crop=include_ai_crop,
                include_upload_meta=include_upload_meta,
            )
            if not _image_calibration_uuid(img):
                expected_payload.pop('calibration_uuid', None)
                remote_payload.pop('calibration_uuid', None)
            for key in upload_derived_keys:
                expected_payload.pop(key, None)
                remote_payload.pop(key, None)
            if expected_payload != remote_payload:
                # Descriptive metadata changed while the link was missing — let
                # the normal prepare + metadata-patch path handle it.
                continue

        if _reconcile_local_image_cloud_id(local_image_id, remote_cloud_id, mark_synced=True):
            _increment_sync_summary(_cloud_sync_current_summary(), 'images_cloud_id_repaired')
            print(
                f'[cloud_sync] Observation {obs_local_id}: metadata association for cloud image '
                f'actual_upload=False image_id={local_image_id} cloud_image_id={remote_cloud_id} '
                f'storage_path={remote_storage_path} '
                f'(restored local cloud_id without preparing an upload candidate)'
            )
        associated_ids.add(local_image_id)
    return associated_ids


def _stored_local_media_signature_image_stats(
    observation_id: int | str,
) -> dict[int, dict]:
    """Return the last-synced path/stat signature per local image id.

    Reads the stored per-observation local media signature and pulls out the
    ``filepath`` :func:`_path_stat_signature` dict for each image row. Used to
    decide whether an image's source file has changed since the last sync
    without hashing its content.
    """
    stored_text = _load_local_cloud_media_signature(observation_id)
    parsed = _parsed_local_media_signature(stored_text)
    result: dict[int, dict] = {}
    for row in (parsed.get('images') or []):
        if not isinstance(row, dict):
            continue
        try:
            image_id = int(row.get('id') or 0)
        except Exception:
            image_id = 0
        if image_id <= 0:
            continue
        filepath_sig = row.get('filepath')
        if isinstance(filepath_sig, dict):
            result[image_id] = dict(filepath_sig)
    return result


def _local_image_source_bytes_unchanged(
    stored_stats: dict[int, dict],
    image_row: dict,
) -> bool:
    """Best-effort: True when the image's local source file hasn't changed.

    Compares the stored path/stat signature captured at the last successful
    sync against the current signature. Mismatches (file replaced, edited,
    resized) fall through to the full prepare path so a stale WebP is never
    reused. If we don't have a stored signature we conservatively return
    False so the caller falls back to the encode-and-hash path.
    """
    try:
        image_id = int(image_row.get('id') or 0)
    except Exception:
        image_id = 0
    if image_id <= 0:
        return False
    filepath = str(image_row.get('filepath') or '').strip()
    if not filepath:
        return False
    current_sig = _path_stat_signature(filepath)
    if not current_sig.get('exists'):
        return False
    stored_sig = stored_stats.get(image_id)
    if not isinstance(stored_sig, dict):
        # A missing observation-wide signature must not turn every linked row
        # into an upload. The per-image synced_at timestamp is a conservative
        # fallback: files modified after the last successful image sync still
        # enter the encode/hash path, while older unchanged sources do not.
        synced_at = _parse_sync_timestamp(image_row.get('synced_at'))
        try:
            current_mtime = datetime.fromtimestamp(
                int(current_sig.get('mtime_ns') or 0) / 1_000_000_000,
                tz=timezone.utc,
            )
        except Exception:
            current_mtime = None
        return bool(synced_at and current_mtime and current_mtime <= synced_at)
    # mtime_ns can be missing on stored payloads normalized via
    # _normalized_local_media_signature_payload — fall back to (path, size).
    def _cmp_key(sig: dict) -> tuple:
        return (
            str(sig.get('path') or ''),
            int(sig.get('size') or 0),
            int(sig.get('mtime_ns') or 0),
        )
    return _cmp_key(stored_sig) == _cmp_key(current_sig)


def _reconcile_metadata_only_linked_images(
    client: SporelyCloudClient,
    obs: dict,
    obs_cloud_id: str,
    existing_rows: list[dict],
) -> set[int]:
    """Skip WebP prep for linked images whose bytes haven't changed.

    Companion to :func:`_associate_persisted_cloud_images`. When an
    observation is re-dirtied for a *different* image (e.g. a new local image
    that still has ``cloud_id IS NULL``), the sibling images that are already
    correctly linked and whose local source file hasn't changed should not be
    re-encoded to WebP just to end up as a metadata-only patch. This pass
    applies any pending metadata delta directly and returns their ids so the
    prepare callback skips them.
    """
    skip_ids: set[int] = set()
    try:
        obs_local_id = int(obs.get('id'))
    except Exception:
        return skip_ids
    if not existing_rows:
        return skip_ids

    existing_by_id = {
        str(row.get('id') or '').strip(): row
        for row in existing_rows
        if str(row.get('id') or '').strip()
    }
    existing_by_desktop_id = {
        _safe_int(row.get('desktop_id')): row
        for row in existing_rows
        if _safe_int(row.get('desktop_id')) != 0
    }

    include_ai_crop = client._observation_images_support_ai_crop()
    include_upload_meta = client._observation_images_support_upload_metadata()
    upload_derived_keys = {
        'id',
        'storage_path',
        'original_filename',
        'source_width',
        'source_height',
        'stored_width',
        'stored_height',
        'stored_bytes',
        'upload_mode',
    }

    stored_stats = _stored_local_media_signature_image_stats(obs_local_id)

    for image_row in ImageDB.get_images_for_observation(obs_local_id):
        img = dict(image_row or {})
        img['portable_cloud_identity_pending'] = bool(
            obs.get('portable_cloud_identity_pending')
        )
        local_image_id = _safe_int(img.get('id'))
        if local_image_id <= 0 or not should_push_local_image_to_cloud(img):
            continue
        local_cloud_id = str(img.get('cloud_id') or '').strip()
        if not local_cloud_id:
            continue
        if not str(img.get('synced_at') or '').strip():
            continue
        if _load_pending_image_promotion_key(obs_local_id, local_image_id):
            # An anchor promotion reserved a byte key on this row but the
            # upload was never confirmed. The remote storage_path may be
            # non-NULL without any bytes behind it — this row must go
            # through the upload path, never the metadata-only fast path.
            continue
        remote_row = _select_remote_image_identity_candidate(
            local_image_id=local_image_id,
            local_cloud_id=local_cloud_id,
            existing_by_id=existing_by_id,
            existing_by_desktop_id=existing_by_desktop_id,
            portable_identity_pending=bool(
                obs.get('portable_cloud_identity_pending')
            ),
        )
        if not remote_row:
            continue
        remote_storage_path = _normalize_cloud_media_key(remote_row.get('storage_path'))
        if not remote_storage_path:
            continue
        source_role = str(img.get('source_role') or '').strip().lower()
        file_purpose = str(img.get('file_purpose') or '').strip().lower()
        is_cloud_recovery_cache = (
            source_role == 'cloud_recovery_cache' or file_purpose == 'cache'
        )
        if not is_cloud_recovery_cache and not _local_image_source_bytes_unchanged(stored_stats, img):
            continue

        # Detect metadata drift without any WebP encoding.
        expected_payload = _prepared_item_remote_payload(
            img,
            str(img.get('filepath') or ''),
            remote_storage_path,
            include_ai_crop=include_ai_crop,
            include_upload_meta=include_upload_meta,
        )
        remote_payload = _remote_image_payload(
            remote_row,
            include_ai_crop=include_ai_crop,
            include_upload_meta=include_upload_meta,
        )
        if not _image_calibration_uuid(img):
            expected_payload.pop('calibration_uuid', None)
            remote_payload.pop('calibration_uuid', None)
        for key in upload_derived_keys:
            expected_payload.pop(key, None)
            remote_payload.pop(key, None)

        if expected_payload != remote_payload:
            try:
                client.push_image_metadata(img, obs_cloud_id, remote_storage_path, remote_row=remote_row)
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                if is_cloud_recovery_cache:
                    # Recovery-cache bytes came from this cloud row. Never
                    # feed them back through WebP preparation/upload merely
                    # because a local metadata patch failed.
                    print(
                        f'[cloud_sync] Observation {obs_local_id}: metadata-only patch '
                        f'for recovery-cache image {local_cloud_id} failed ({exc}); '
                        f'skipping byte upload'
                    )
                    skip_ids.add(local_image_id)
                    continue
                # Fall back to the normal path if the direct patch failed.
                print(
                    f'[cloud_sync] Observation {obs_local_id}: metadata-only patch '
                    f'for cloud image {local_cloud_id} failed ({exc}); using the '
                    f'prepare/upload path as fallback'
                )
                continue
            print(
                f'[cloud_sync] Observation {obs_local_id}: metadata patch for cloud image '
                f'actual_upload=False image_id={local_image_id} cloud_image_id={local_cloud_id} '
                f'storage_path={remote_storage_path} '
                f'(source bytes unchanged since last sync)'
            )
        else:
            reason = (
                'cloud recovery cache is remote-owned'
                if is_cloud_recovery_cache
                else 'source bytes unchanged since last sync, metadata matches'
            )
            print(
                f'[cloud_sync] Observation {obs_local_id}: skipped already synced cloud image '
                f'actual_upload=False image_id={local_image_id} cloud_image_id={local_cloud_id} '
                f'storage_path={remote_storage_path} '
                f'reason=already_synced_unchanged ({reason})'
            )

        skip_ids.add(local_image_id)
    return skip_ids


def _push_images_for_observation(
    client: SporelyCloudClient,
    obs: dict,
    obs_cloud_id: str,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    observation_index: int | None = None,
    observation_total: int | None = None,
    summary_warnings: list[str] | None = None,
    original_summary: dict | None = None,
    include_image_ids: set[int] | None = None,
) -> bool:
    """Push selected observation images for one observation."""
    warnings: list[str] = []
    anchor_result = _ensure_metadata_anchors_for_public_spore_observation(
        client,
        obs,
        _safe_int(obs.get('id')),
        str(obs_cloud_id or '').strip(),
    ) or {}
    anchor_failures = anchor_result.get('failures') or []
    for msg in anchor_failures:
        warnings.append(f'anchor: {msg}')
    anchor_had_failures = bool(anchor_failures)
    # Existing metadata-only anchors remain protected, but publication
    # selection must never downgrade a cloud-backed image to this state.
    metadata_only_anchor_cloud_ids = {
        str(value or '').strip()
        for value in (anchor_result.get('metadata_only_cloud_ids') or [])
        if str(value or '').strip()
    }
    warnings.extend(_push_pending_image_tombstones(client))
    # Storage-intent initialization is a sync prerequisite — not a UI
    # concern. Before any prepare_images_cb runs, before any candidate
    # filtering, before any byte-upload boundary, ensure every local image
    # of this observation has a per-image intent record. Incremental and
    # idempotent — images already in the intent ledger are never rewritten,
    # and images imported after a previous initialization get their default
    # here instead of falling through the byte gate as "desired" (empty
    # excluded set → everything desired; the 2026-08-19 mass microscope
    # upload). Runs AFTER ``_push_pending_image_tombstones`` so that
    # protected microscope anchors whose tombstones get cancelled in that
    # step are not incorrectly written into the excluded set by the
    # initializer's tombstone rule.
    _ensure_cloud_image_storage_intent_initialized(_safe_int(obs.get('id')))
    prepared_items: list[dict] = []
    cleanup = None
    preparation_failed = False
    # Remote-first pass: pull existing cloud image metadata once up front so we
    # can re-associate orphaned local rows (cloud_id lost but already uploaded)
    # before any temporary WebP candidate is encoded for them. Reused by the
    # main upload loop below to avoid a second metadata fetch.
    prepass_existing_rows: list[dict] | None = None
    skip_prepare_image_ids: set[int] = set()
    if include_image_ids is not None:
        allowed_ids = {_safe_int(value) for value in include_image_ids if _safe_int(value) > 0}
        skip_prepare_image_ids.update(
            _safe_int(row.get('id'))
            for row in ImageDB.get_images_for_observation(_safe_int(obs.get('id')))
            if _safe_int(row.get('id')) > 0 and _safe_int(row.get('id')) not in allowed_ids
        )
    if callable(prepare_images_cb):
        try:
            prepass_existing_rows = client.pull_image_metadata(obs_cloud_id) or []
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(
                f'[cloud_sync] Could not pre-fetch cloud images for observation {obs["id"]}: {e}'
            )
            prepass_existing_rows = None
        if prepass_existing_rows is not None:
            associated_skip_ids = _associate_persisted_cloud_images(
                client, obs, prepass_existing_rows
            )
            skip_prepare_image_ids = skip_prepare_image_ids | associated_skip_ids
            # Also skip WebP prep for sibling images that are already linked
            # and whose local bytes haven't changed since last sync. Their
            # metadata delta (if any) is patched in place here so the
            # prepare_images_cb can focus on images that actually need bytes.
            metadata_only_ids = _reconcile_metadata_only_linked_images(
                client, obs, obs_cloud_id, prepass_existing_rows
            )
            skip_prepare_image_ids = skip_prepare_image_ids | metadata_only_ids
    if callable(prepare_images_cb):
        try:
            def prepare_progress(message: str, _current: int | None = None, _total: int | None = None) -> None:
                _emit_progress(progress_cb, message, progress_state)

            prepare_obs = dict(obs)
            if skip_prepare_image_ids:
                prepare_obs[CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY] = sorted(skip_prepare_image_ids)
            prepared_items, cleanup, prep_warnings = prepare_images_cb(prepare_obs, prepare_progress)
            warnings.extend(prep_warnings or [])
        except Exception as e:
            if is_image_too_large_for_plan_error(e) or is_webp_support_required_for_cloud_media_upload_error(e):
                raise
            print(f'[cloud_sync] Observation {obs["id"]} image preparation failed: {e}')
            prepared_items = []
            cleanup = None
            warnings.append(str(e))
            preparation_failed = True
    else:
        images = ImageDB.get_images_for_observation(obs['id'])
        for img in images:
            if img.get('image_type') == 'microscope' and not img.get('cloud_id'):
                continue
            # Stage 1: the prepare_images_cb=None fallback must also honor the
            # cloud-storage-desired predicate. Sending bytes for an image the
            # user unchecked would bypass the client boundary gate, and the
            # boundary gate would then correctly raise.
            local_img_id = _safe_int(img.get('id'))
            local_obs_id = _safe_int(obs.get('id'))
            if (
                local_obs_id > 0
                and local_img_id > 0
                and not cloud_image_bytes_desired(local_obs_id, local_img_id, img)
            ):
                continue
            prepared_items.append({
                'image_row': img,
                'upload_path': img.get('filepath'),
            })

    for warning in warnings:
        print(f'[cloud_sync] Observation {obs["id"]}: {warning}')

    def _record_original_upload_warning(message: str) -> None:
        text = str(message or '').strip()
        if not text:
            return
        print(f'[cloud_sync] Observation {obs["id"]}: {text}')
        if isinstance(summary_warnings, list):
            summary_warnings.append(text)

    def _record_original_summary(metric: str) -> None:
        if not isinstance(original_summary, dict):
            return
        current = _safe_int(original_summary.get(metric))
        original_summary[metric] = current + 1

    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [
            str(dict(item.get('image_row') or {}).get('cloud_id') or '').strip()
            for item in prepared_items
            if str(dict(item.get('image_row') or {}).get('cloud_id') or '').strip()
        ]
    )
    tombstoned_local_image_ids = _local_tombstoned_local_image_ids(
        [
            _safe_int(dict(item.get('image_row') or {}).get('id'))
            for item in prepared_items
            if _safe_int(dict(item.get('image_row') or {}).get('id')) > 0
        ]
    )
    if tombstoned_cloud_ids:
        filtered_items: list[dict] = []
        for item in prepared_items:
            img = dict(item.get('image_row') or {})
            cloud_image_id = str(img.get('cloud_id') or '').strip()
            if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                warning = _tombstoned_cloud_image_warning(obs.get('id'), cloud_image_id)
                warnings.append(warning)
                print(f'[cloud_sync] Warning: {warning}')
                continue
            filtered_items.append(item)
        prepared_items = filtered_items
    if tombstoned_local_image_ids:
        filtered_items = []
        for item in prepared_items:
            img = dict(item.get('image_row') or {})
            local_image_id = _safe_int(img.get('id'))
            if local_image_id > 0 and local_image_id in tombstoned_local_image_ids:
                warning = f"obs {obs.get('id')}: skipped local image {local_image_id} because it has a local tombstone"
                warnings.append(warning)
                print(f'[cloud_sync] Warning: {warning}')
                continue
            filtered_items.append(item)
        prepared_items = filtered_items

    if prepared_items:
        _extend_progress_total(progress_state, len(prepared_items))
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Prepared {len(prepared_items)} image(s) for upload in observation {observation_index}/{max(1, observation_total)}",
                ),
                progress_state,
            )

    if preparation_failed:
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Cloud media preparation failed for observation {observation_index}/{max(1, observation_total)}",
                ),
                progress_state,
            )
        return False

    existing_rows_unavailable = False
    if prepass_existing_rows is not None:
        existing_rows = prepass_existing_rows
    else:
        try:
            existing_rows = client.pull_image_metadata(obs_cloud_id) or []
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            print(f'[cloud_sync] Could not fetch existing cloud images for observation {obs["id"]}: {e}')
            existing_rows = []
            existing_rows_unavailable = True
    existing_by_id = {
        str(row.get('id') or '').strip(): row
        for row in existing_rows
        if str(row.get('id') or '').strip()
    }
    existing_by_desktop_id = {
        _safe_int(row.get('desktop_id')): row
        for row in existing_rows
        if _safe_int(row.get('desktop_id')) != 0
    }

    try:
        processed_items = 0
        total_items = len(prepared_items)
        had_failures = False
        include_ai_crop = client._observation_images_support_ai_crop()
        include_upload_meta = client._observation_images_support_upload_metadata()
        original_sync_enabled = is_full_resolution_original_sync_enabled()
        for item_index, item in enumerate(prepared_items, start=1):
            _increment_sync_summary(_cloud_sync_current_summary(), 'images_checked')
            img = dict(item.get('image_row') or {})
            img.update(dict(item.get('cloud_upload_meta') or {}))
            img['portable_cloud_identity_pending'] = bool(
                obs.get('portable_cloud_identity_pending')
            )
            if observation_index and observation_total:
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Checking cloud image {item_index}/{max(1, total_items)}…",
                    ),
                    progress_state,
                )
            if not img:
                print(f'[cloud_sync] Observation {obs["id"]}: skipped empty cloud image item {item_index}')
                continue
            local_image_id = _safe_int(img.get('id'))
            local_cloud_id = str(img.get('cloud_id') or '').strip()
            pending_promotion_key = (
                _load_pending_image_promotion_key(obs.get('id'), local_image_id)
                if local_image_id > 0
                else ''
            )
            remote_row = _select_remote_image_identity_candidate(
                local_image_id=local_image_id,
                local_cloud_id=local_cloud_id,
                existing_by_id=existing_by_id,
                existing_by_desktop_id=existing_by_desktop_id,
                portable_identity_pending=bool(
                    img.get('portable_cloud_identity_pending')
                ),
            )
            remote_cloud_id = str((remote_row or {}).get('id') or '').strip()
            if not should_push_local_image_to_cloud(img):
                print(
                    f'[cloud_sync] Observation {obs["id"]}: skipped cloud image '
                    f'{local_image_id or item_index} because it is not eligible for upload'
                )
                continue
            upload_path = str(item.get('upload_path') or img.get('filepath') or '').strip()
            if not upload_path:
                print(
                    f'[cloud_sync] Observation {obs["id"]}: skipped cloud image '
                    f'{local_image_id or item_index} because upload_path is missing'
                )
                continue
            try:
                # Protected metadata-only anchor short-circuit.
                # The pre-step already downgraded this row: bytes removed,
                # `storage_path` PATCHed to NULL. Do NOT rebuild a worker
                # storage_path here — that would set us on the "file
                # differs, upload" branch below and orphan bytes in
                # storage that the metadata row will never reference.
                # Metadata reconciliation for other fields (notes, EXIF,
                # sort_order, …) still runs downstream; only the bytes
                # path is skipped.
                is_metadata_only_anchor = bool(
                    remote_cloud_id
                    and remote_cloud_id in metadata_only_anchor_cloud_ids
                )
                if is_metadata_only_anchor:
                    existing_storage_path = ''
                    storage_path = ''
                else:
                    existing_storage_path = _normalize_cloud_media_key((remote_row or {}).get('storage_path'))
                    if not remote_row and existing_rows_unavailable:
                        # The listing failed, so the identity selector cannot
                        # see a live row that push_image_metadata would still
                        # PATCH. Reuse that row's key instead of minting a new
                        # one (which would orphan the existing object).
                        existing_storage_path = _lookup_unlisted_remote_image_storage_path(
                            client, obs_cloud_id, img,
                        )
                    storage_path = existing_storage_path or _build_worker_storage_path(
                        client.user_id,
                        obs_cloud_id,
                        img,
                        upload_path,
                    )
                expected_payload = _prepared_item_remote_payload(
                    img,
                    upload_path,
                    storage_path,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                local_calibration_uuid = _image_calibration_uuid(img)
                if remote_row and remote_row.get('original_filename'):
                    expected_payload['original_filename'] = _normalize_snapshot_value(remote_row.get('original_filename'))
                remote_payload = _remote_image_payload(
                    remote_row,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                if not local_calibration_uuid:
                    expected_payload.pop('calibration_uuid', None)
                    remote_payload.pop('calibration_uuid', None)
                metadata_matches = bool(remote_row) and remote_payload == expected_payload

                current_file_sig = _file_content_signature(upload_path)
                stored_file_sig = _load_cloud_image_file_signature(obs.get('id'), local_image_id)
                file_matches = False
                if is_metadata_only_anchor:
                    # Bytes have been intentionally cleared by the pre-step;
                    # there is nothing to compare and nothing to upload.
                    # Both `storage_path`s normalize to '' and `file_matches`
                    # is forced True so the "upload replacement bytes"
                    # branch below never fires for this row.
                    file_matches = True
                elif remote_row and _normalize_cloud_media_key(remote_row.get('storage_path')) == _normalize_cloud_media_key(storage_path):
                    if stored_file_sig and stored_file_sig == current_file_sig:
                        file_matches = True
                    elif (
                        not stored_file_sig
                        and (
                            local_cloud_id == remote_cloud_id
                            or bool(img.get('synced_at'))
                        )
                        ):
                            file_matches = True

                if (
                    file_matches
                    and pending_promotion_key
                    and pending_promotion_key == _normalize_cloud_media_key(storage_path)
                ):
                    # An earlier anchor promotion reserved this key but never
                    # confirmed the byte upload (interrupted between the
                    # reservation PATCH and upload success). A non-NULL
                    # storage_path alone must not prove that bytes exist —
                    # force the upload; rewriting the same key is idempotent.
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: unconfirmed anchor '
                        f'promotion for image {local_image_id} '
                        f'(storage_path={storage_path}); forcing byte upload'
                    )
                    file_matches = False

                if file_matches and metadata_matches:
                    _increment_sync_summary(_cloud_sync_current_summary(), 'images_skipped_already_synced')
                    # The remote bytes and metadata already match, but the local
                    # row may have lost its cloud_id (e.g. an earlier overwrite).
                    # Restore the association so this observation is not
                    # re-dirtied forever. This is a metadata-only repair —
                    # actual_upload=False, no bytes are sent.
                    if remote_cloud_id and local_image_id > 0 and local_cloud_id != remote_cloud_id:
                        if _reconcile_local_image_cloud_id(
                            local_image_id, remote_cloud_id, mark_synced=True
                        ):
                            _increment_sync_summary(
                                _cloud_sync_current_summary(), 'images_cloud_id_repaired'
                            )
                            print(
                                f'[cloud_sync] Observation {obs["id"]}: metadata association for cloud image '
                                f'actual_upload=False image_id={local_image_id or item_index} '
                                f'cloud_image_id={remote_cloud_id} storage_path={storage_path} '
                                f'(restored missing local cloud_id)'
                            )
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: skipped already synced cloud image '
                        f'{local_image_id or item_index} (storage_path={storage_path})'
                    )
                    continue

                if not file_matches:
                    if observation_index and observation_total:
                        _emit_progress(
                            progress_cb,
                            _format_cloud_sync_observation_status(
                                obs,
                                f"Uploading cloud image {item_index}/{max(1, total_items)}…",
                            ),
                            progress_state,
                        )
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: Uploading cloud image request '
                        f'actual_upload=True image_id={local_image_id or item_index} '
                        f'cloud_image_id={local_cloud_id or remote_cloud_id or "new"} '
                        f'storage_path={storage_path} (uploading cloud image bytes)'
                    )
                else:
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: metadata patch for cloud image '
                        f'actual_upload=False image_id={local_image_id or item_index} '
                        f'cloud_image_id={local_cloud_id or remote_cloud_id or "new"} '
                        f'storage_path={storage_path}'
                    )

                img_cloud_id = remote_cloud_id
                # Stage 1 defense in depth: even after prepare_images_cb emitted
                # this row, skip the byte upload if the user unchecked the
                # image between preparation and this loop. This must NOT
                # create a metadata-only anchor for non-microscope images; it
                # only bypasses the byte send and lets downstream metadata
                # bookkeeping proceed.
                if (
                    not file_matches
                    and local_image_id > 0
                    and not cloud_image_bytes_desired(
                        obs.get('id'), local_image_id, img
                    )
                ):
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: skipped byte upload '
                        f'for image {local_image_id} because cloud image bytes '
                        f'are not desired by the user'
                    )
                    file_matches = True
                    if not img_cloud_id and remote_cloud_id:
                        img_cloud_id = remote_cloud_id
                    # No bytes are sent this round, so the metadata patch
                    # below must not invent a storage key either. Collapse to
                    # the remote row's actual key: a bare metadata anchor
                    # (NULL) stays a bare anchor — push_image_metadata omits
                    # an empty storage_path so the cloud value is untouched.
                    storage_path = _normalize_cloud_media_key(
                        (remote_row or {}).get('storage_path')
                    )
                if not file_matches:
                    # Private Worker writes require a server-known image
                    # identity. Reserve/upsert the metadata row before bytes
                    # are sent; if the upload fails, remove any partial bytes
                    # before releasing a row created by this attempt.
                    reserved_image_row = False
                    promotion_reserved_key = ''
                    remote_row_storage_path = _normalize_cloud_media_key(
                        (remote_row or {}).get('storage_path')
                    )
                    if not img_cloud_id:
                        img_cloud_id = client.push_image_metadata(
                            img, obs_cloud_id, storage_path)
                        reserved_image_row = True
                    elif not remote_row_storage_path and not is_metadata_only_anchor:
                        # Linked metadata-anchor → byte-backed promotion: a
                        # cloud row already exists for this image (same owned
                        # identity) but carries no storage key. Reserve the
                        # intended Worker key on that exact row so the
                        # Worker's storage_path check passes. Never create a
                        # second observation_images row for this transition.
                        promotion_reserved_key = _reserve_anchor_promotion_key(
                            client,
                            obs.get('id'),
                            local_image_id,
                            img_cloud_id,
                            storage_path,
                        )
                        print(
                            f'[cloud_sync] Observation {obs["id"]}: promoted '
                            f'metadata anchor to byte-backed image '
                            f'image_id={local_image_id} '
                            f'cloud_image_id={img_cloud_id} '
                            f'reserved_storage_path={promotion_reserved_key}'
                        )
                    elif (
                        pending_promotion_key
                        and remote_row_storage_path == pending_promotion_key
                        and remote_row_storage_path == _normalize_cloud_media_key(storage_path)
                    ):
                        # Resuming an interrupted promotion: the key is
                        # already reserved on the row by a previous attempt;
                        # adopt it so a failure here still rolls back to a
                        # clean NULL anchor.
                        promotion_reserved_key = pending_promotion_key
                    try:
                        uploaded_key = client.upload_image_file(
                            upload_path,
                            obs_cloud_id,
                            img_cloud_id,
                            storage_path=storage_path,
                            upload_meta=dict(item.get('cloud_upload_meta') or {}),
                            observation_id=_safe_int(obs.get('id')),
                            image_id=local_image_id,
                        )
                    except Exception:
                        if reserved_image_row and img_cloud_id:
                            try:
                                client._storage_remove([
                                    storage_path,
                                    media_variant_key(storage_path, 'thumb'),
                                ])
                            finally:
                                client._delete(
                                    f'observation_images?id=eq.{img_cloud_id}'
                                    f'&user_id=eq.{client.user_id}'
                                )
                        elif promotion_reserved_key and img_cloud_id:
                            # Promotion failure: restore the clean metadata
                            # anchor (conditional NULL rollback + partial
                            # object cleanup). The anchor row is kept.
                            _rollback_anchor_promotion(
                                client,
                                obs.get('id'),
                                local_image_id,
                                img_cloud_id,
                                promotion_reserved_key,
                            )
                        raise
                    if uploaded_key is None:
                        # A None return from upload_image_file means the
                        # local file was missing before any Worker call —
                        # zero bytes were uploaded. On the promotion path
                        # this must be treated as a failure so the
                        # reservation is released (or the marker kept for
                        # a later retry) instead of clearing the pending
                        # promotion marker as if the bytes had landed.
                        if promotion_reserved_key and img_cloud_id:
                            _rollback_anchor_promotion(
                                client,
                                obs.get('id'),
                                local_image_id,
                                img_cloud_id,
                                promotion_reserved_key,
                            )
                        raise CloudSyncError(
                            f'upload_image_file returned no key for '
                            f'image {local_image_id}; local file missing'
                        )
                    storage_path = _normalize_cloud_media_key(uploaded_key or storage_path)
                    if promotion_reserved_key:
                        # Bytes confirmed — the reservation is now a real
                        # derivative key; drop the pending marker.
                        _clear_pending_image_promotion_key(
                            obs.get('id'), local_image_id,
                        )

                if not img_cloud_id or not metadata_matches:
                    if remote_row and remote_row.get('original_filename'):
                        img['original_filename'] = str(remote_row.get('original_filename') or '').strip()
                    img_cloud_id = client.push_image_metadata(img, obs_cloud_id, storage_path, remote_row=remote_row)
                    remote_payload = _prepared_item_remote_payload(
                        img,
                        upload_path,
                        storage_path,
                        include_ai_crop=include_ai_crop,
                        include_upload_meta=include_upload_meta,
                    )
                    if remote_row and remote_row.get('original_filename'):
                        remote_payload['original_filename'] = _normalize_snapshot_value(remote_row.get('original_filename'))
                    metadata_matches = True

                try:
                    image_id = int(img['id'])
                except Exception:
                    image_id = 0
                if image_id > 0:
                    conn = get_connection()
                    conn.execute(
                        'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                        (img_cloud_id, datetime.now(timezone.utc).isoformat(), image_id),
                    )
                    conn.commit()
                    conn.close()
                if current_file_sig:
                    _store_cloud_image_file_signature(obs.get('id'), local_image_id, current_file_sig)
                if _normalize_cloud_media_key(storage_path):
                    _set_cloud_image_metadata_only_state(
                        obs.get('id'), local_image_id, False,
                    )

                original_upload_source = resolve_full_original_upload_source(img)
                original_storage_path = _normalize_cloud_media_key((remote_row or {}).get('original_storage_path'))
                if not original_storage_path:
                    profiler = _cloud_sync_current_profiler()
                    if not original_sync_enabled:
                        if profiler is not None:
                            try:
                                if original_upload_source is None:
                                    profiler.record_original_upload_skipped_ineligible()
                                else:
                                    profiler.record_original_upload_skipped_disabled()
                            except Exception:
                                pass
                        if original_upload_source is None:
                            _record_original_summary('skipped_ineligible')
                        else:
                            _record_original_summary('skipped_disabled')
                    elif original_upload_source is None:
                        if profiler is not None:
                            try:
                                profiler.record_original_upload_skipped_ineligible()
                            except Exception:
                                pass
                        _record_original_summary('skipped_ineligible')
                    else:
                        source_path = str(original_upload_source.get('source_path') or '').strip()
                        source_kind = str(original_upload_source.get('source_kind') or '').strip() or 'filepath'
                        source_size = 0
                        try:
                            source_size = int(Path(source_path).stat().st_size)
                        except Exception:
                            source_size = 0

                        if is_full_resolution_original_upload_too_large(source_path):
                            if profiler is not None:
                                try:
                                    profiler.record_original_upload_skipped_too_large()
                                except Exception:
                                    pass
                            _record_original_summary('skipped_too_large')
                            _record_original_upload_warning(
                                (
                                    f"skipped original upload for image {img.get('id')} "
                                    f"because {source_kind} is too large "
                                    f"({_format_size(source_size)} > "
                                    f"{_format_size(FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES)})"
                                )
                            )
                        else:
                            original_upload_meta = dict(item.get('cloud_upload_meta') or {})
                            original_upload_meta.update(
                                {
                                    'source_role': original_upload_source.get('source_role'),
                                    'source_kind': source_kind,
                                    'upload_mode': 'full',
                                    'upload_variant': 'original',
                                }
                            )
                            original_storage_path = client._build_original_storage_path(
                                obs_cloud_id,
                                img_cloud_id,
                                source_path,
                            )
                            try:
                                # Stage 1 defense in depth for the full-res
                                # original: never send bytes for an image the
                                # user has unchecked. The client-boundary gate
                                # would raise anyway; this local skip keeps
                                # the loop's bookkeeping tidy.
                                if (
                                    local_image_id > 0
                                    and not cloud_image_bytes_desired(
                                        obs.get('id'), local_image_id, img
                                    )
                                ):
                                    raise CloudImageBytesNotDesiredError(
                                        f"cloud original bytes not desired: "
                                        f"obs={obs.get('id')} image={local_image_id}"
                                    )
                                # Bind the canonical original identity before
                                # the private upload so the Worker can verify
                                # X-Sporely-Image-Id against this exact key.
                                client.set_image_original_storage_path(
                                    img_cloud_id,
                                    original_storage_path,
                                )
                                uploaded_original_key = client.upload_original_image_file(
                                    source_path,
                                    obs_cloud_id,
                                    img_cloud_id,
                                    storage_path=original_storage_path,
                                    upload_meta=original_upload_meta,
                                    observation_id=_safe_int(obs.get('id')),
                                    image_id=local_image_id,
                                )
                            except CloudSyncError as exc:
                                try:
                                    client._storage_remove([original_storage_path])
                                finally:
                                    client._patch(
                                        f'observation_images?id=eq.{img_cloud_id}'
                                        f'&user_id=eq.{client.user_id}',
                                        {'original_storage_path': None},
                                    )
                                if profiler is not None:
                                    try:
                                        profiler.record_original_upload_failed()
                                    except Exception:
                                        pass
                                _record_original_summary('failed_uploads')
                                _record_original_upload_warning(
                                    (
                                        f"original upload failed for image {img.get('id')} "
                                        f"from {source_kind}: {exc}"
                                    )
                                )
                            else:
                                original_storage_path = _normalize_cloud_media_key(
                                    uploaded_original_key or original_storage_path
                                )
                                if original_storage_path:
                                    if profiler is not None:
                                        try:
                                            profiler.record_original_upload_success(source_size)
                                        except Exception:
                                            pass
                                    _record_original_summary('uploaded')
                                    print(
                                        f'[cloud_sync] Observation {obs["id"]}: original upload source for image {img.get("id")} '
                                        f'was {source_kind}'
                                    )
            except CloudSyncError as e:
                if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                    raise
                if is_image_too_large_for_plan_error(e):
                    raise
                had_failures = True
                failure_msg = f'Image {img["id"]}: {type(e).__name__}: {e}'
                print(f'[cloud_sync] {failure_msg}')
                _record_original_upload_warning(failure_msg)
            finally:
                processed_items += 1
                _advance_progress(progress_state, 1)
        if total_items > processed_items:
            _advance_progress(progress_state, total_items - processed_items)
        return not (had_failures or anchor_had_failures)
    finally:
        if callable(cleanup):
            try:
                cleanup()
            except Exception:
                pass
