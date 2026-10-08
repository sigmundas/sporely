"""Image pull: applying remote images locally, importing new remote images,
the local pull-side metadata-only anchor insert, the mosaic signature
carry-forward across the working-file swap, and materialization state.

The default-client entry point ``materialize_cloud_media_for_observation``
stays in the facade (it constructs ``SporelyCloudClient``).

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from database.models import ImageDB, MeasurementDB, ObservationDB
from database.schema import get_connection, get_images_dir
from utils.heic_converter import guess_local_image_mime_type
from utils.thumbnail_generator import generate_all_sizes

from utils.cloud_sync_impl.baseline import (
    _load_cloud_observation_snapshot,
    _parse_cloud_observation_snapshot,
)
from utils.cloud_sync_impl.calibrations import _local_calibration_id_for_image
from utils.cloud_sync_impl.common import _normalize_cloud_media_key, _safe_int
from utils.cloud_sync_impl.errors import (
    CloudSyncError,
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.exif import (
    _inject_obs_exif_into_field_image,
    _load_obs_exif_fallback,
)
from utils.cloud_sync_impl.image_files import (
    _detected_image_extension,
    _file_content_signature,
    _rename_to_detected_image_extension,
)
from utils.cloud_sync_impl.image_identity import (
    _portable_cloud_identity_pending_for_observation,
)
from utils.cloud_sync_impl.image_policy import (
    _is_local_metadata_only_microscope_anchor,
    _is_metadata_only_microscope_cloud_image,
    should_pull_cloud_image_to_desktop,
)
from utils.cloud_sync_impl.local_files import _resolve_existing_local_image_asset_path
from utils.cloud_sync_impl.mosaic_signature import (
    _current_local_mosaic_signature,
    _local_mosaic_signature_is_current,
    _store_local_mosaic_signature,
)
from utils.cloud_sync_impl.preflight import _load_local_measurement_lookup
from utils.cloud_sync_impl.progress import (
    _advance_progress,
    _cloud_sync_current_profiler,
    _cloud_sync_current_summary,
    _cloud_sync_perf_counter,
    _emit_progress,
    _extend_progress_total,
    _increment_sync_summary,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
)
from utils.cloud_sync_impl.remote_reads import _group_remote_measurements_by_observation
from utils.cloud_sync_impl.sample_source import _cloud_to_desktop_sample_source
from utils.cloud_sync_impl.status_messages import _format_cloud_sync_observation_status
from utils.cloud_sync_impl.sync_state import (
    _load_cloud_image_file_signature,
    _store_cloud_image_file_signature,
)
from utils.cloud_sync_impl.tombstones import (
    _local_tombstoned_cloud_image_ids,
    _record_remote_image_tombstones,
    _tombstoned_cloud_image_warning,
)


def _cloud_image_captured_at_to_local(value) -> str | None:
    """Convert a cloud capture instant to SQLite's local wall-clock format."""
    canonical = _normalize_image_captured_at_for_cloud(value, local=False)
    if canonical is None:
        return None
    parsed = datetime.fromisoformat(canonical)
    return parsed.astimezone().replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')


def _update_image_columns_without_touching_observation(
    image_id: int,
    updates: dict[str, object | None],
) -> None:
    if image_id <= 0 or not updates:
        return
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(images)")
        image_columns = {
            str(row[1] or '').strip()
            for row in cursor.fetchall()
            if str(row[1] or '').strip()
        }
        assignments: list[str] = []
        values: list[object | None] = []
        for column_name, column_value in updates.items():
            if column_name not in image_columns:
                continue
            assignments.append(f"{column_name} = ?")
            values.append(column_value)
        if not assignments:
            return
        values.append(int(image_id))
        cursor.execute(
            f"UPDATE images SET {', '.join(assignments)} WHERE id = ?",
            values,
        )
        conn.commit()
    finally:
        conn.close()


def _cloud_pulled_image_order_key(image_row: dict) -> tuple[int, int, str, int]:
    image_type = str(image_row.get('image_type') or '').strip().lower()
    if image_type == 'field':
        type_rank = 0
    elif image_type == 'microscope':
        type_rank = 1
    else:
        type_rank = 2
    sort_order = _safe_int(image_row.get('sort_order'))
    sort_rank = sort_order if sort_order is not None else 10**9
    created_at = str(image_row.get('created_at') or '').strip()
    image_id = _safe_int(image_row.get('id')) or 0
    return (type_rank, sort_rank, created_at, image_id)


def _normalize_cloud_pulled_image_order(local_id: int) -> None:
    try:
        obs_id = int(local_id)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return

    image_rows = [dict(row or {}) for row in ImageDB.get_images_for_observation(obs_id) or []]
    if len(image_rows) < 2:
        return

    # Only normalize the mixed cloud-pulled cases. Local manual galleries keep
    # their existing order unless we have imported cloud recovery cache rows.
    has_cloud_cache_row = any(
        str(row.get('source_role') or '').strip().lower() == 'cloud_recovery_cache'
        for row in image_rows
    )
    if not has_cloud_cache_row:
        return

    image_types = {str(row.get('image_type') or '').strip().lower() for row in image_rows}
    if 'field' not in image_types or 'microscope' not in image_types:
        return

    ordered_rows = sorted(image_rows, key=_cloud_pulled_image_order_key)
    ordered_ids = [_safe_int(row.get('id')) for row in ordered_rows]
    ordered_ids = [image_id for image_id in ordered_ids if image_id is not None]
    current_ids = [_safe_int(row.get('id')) for row in image_rows]
    current_ids = [image_id for image_id in current_ids if image_id is not None]
    if ordered_ids == current_ids:
        return

    for index, image_id in enumerate(ordered_ids):
        _update_image_columns_without_touching_observation(int(image_id), {'sort_order': index})


def _remote_images_missing_locally(local_id: int, remote_images: list[dict] | None) -> list[dict]:
    """Return cloud images that should exist locally but are missing or unreadable."""
    pullable_remote_images = [
        dict(row or {})
        for row in (remote_images or [])
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
        and str(row.get('id') or '').strip()
    ]
    if not pullable_remote_images:
        return []

    local_images = ImageDB.get_images_for_observation(int(local_id))
    local_cloud_map = {
        str(img.get('cloud_id') or '').strip(): img
        for img in local_images
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('cloud_id') or '').strip()
    }
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in pullable_remote_images]
    )

    missing_remote_images: list[dict] = []
    for remote_image in pullable_remote_images:
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id or cloud_image_id in tombstoned_cloud_ids:
            continue
        local_image = local_cloud_map.get(cloud_image_id)
        if local_image is None:
            missing_remote_images.append(remote_image)
            continue
        if (
            _is_metadata_only_microscope_cloud_image(remote_image)
            and _is_local_metadata_only_microscope_anchor(local_image)
        ):
            continue
        if _resolve_existing_local_image_asset_path(local_image.get('filepath')) is None:
            missing_remote_images.append(remote_image)
    return missing_remote_images


def _remote_ai_crop_box(image_row: dict | None) -> tuple[float, float, float, float] | None:
    row = dict(image_row or {})
    values = []
    for key in ('ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2'):
        value = row.get(key)
        if value is None:
            return None
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            return None
    return tuple(values) if len(values) == 4 else None


def _remote_ai_crop_source_size(image_row: dict | None) -> tuple[int, int] | None:
    row = dict(image_row or {})
    width = row.get('ai_crop_source_w')
    height = row.get('ai_crop_source_h')
    if width is None or height is None:
        return None
    try:
        return int(width), int(height)
    except (TypeError, ValueError):
        return None


def _remote_ai_crop_is_custom(image_row: dict | None) -> bool | None:
    row = dict(image_row or {})
    value = row.get('ai_crop_is_custom')
    if value is None:
        return None
    return bool(value)


def _load_local_image_lookup(observation_id: int) -> tuple[dict[str, dict], dict[int, dict]]:
    local_images = ImageDB.get_images_for_observation(int(observation_id))
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for image_row in local_images or []:
        local_image_id = _safe_int(image_row.get('id'))
        if local_image_id > 0:
            by_local_id[local_image_id] = dict(image_row or {})
        cloud_image_id = str(image_row.get('cloud_id') or '').strip()
        if cloud_image_id:
            by_cloud_id[cloud_image_id] = dict(image_row or {})
    return by_cloud_id, by_local_id


def _is_missing_cloud_image_error(exc: Exception | str | None) -> bool:
    text = str(exc or '').strip().lower()
    if not text:
        return False
    return (
        'cloud image file is missing from storage' in text
        or 'nosuchkey' in text
    )


def _cloud_missing_image_warning(local_id: int, remote_image: dict) -> str:
    cloud_image_id = str(remote_image.get('id') or '').strip() or '?'
    filename = Path(str(remote_image.get('original_filename') or '')).name or f'cloud image {cloud_image_id}'
    return (
        f'obs {int(local_id)}: skipped missing cloud image {cloud_image_id}'
        f' ({filename})'
    )


def _profile_generate_all_sizes(image_path: str, image_id: int):
    profiler = _cloud_sync_current_profiler()
    start = _cloud_sync_perf_counter()
    try:
        return generate_all_sizes(image_path, image_id)
    finally:
        if profiler is not None:
            try:
                profiler.record_generate_all_sizes(max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0))
            except Exception:
                pass


def _apply_remote_image_metadata_only_to_local(
    local_image: dict,
    remote_image: dict,
) -> None:
    """Patch the local image row from remote metadata without touching bytes.

    Used when the remote row differs only in descriptive fields (AI crop,
    calibration reference, notes, sort order, etc.). Skips the download,
    thumbnail regeneration and file signature bookkeeping that
    ``_sync_existing_remote_image_to_local`` performs.
    """
    image_id = int(local_image.get('id'))
    calibration_id = _local_calibration_id_for_image(remote_image)
    update_kwargs = {
        'image_type': str(remote_image.get('image_type') or 'field'),
        'scale': remote_image.get('scale_microns_per_pixel'),
        'notes': remote_image.get('notes'),
        'micro_category': remote_image.get('micro_category'),
        'objective_name': remote_image.get('objective_name'),
        'measure_color': remote_image.get('measure_color'),
        'mount_medium': remote_image.get('mount_medium'),
        'stain': remote_image.get('stain'),
        'sample_type': remote_image.get('sample_type'),
        'sample_source': _cloud_to_desktop_sample_source(remote_image.get('sample_source')),
        'contrast': remote_image.get('contrast'),
        'crop_mode': remote_image.get('crop_mode'),
        'gps_source': remote_image.get('gps_source'),
        'resample_scale_factor': remote_image.get('resample_scale_factor'),
        'ai_crop_box': _remote_ai_crop_box(remote_image),
        'ai_crop_source_size': _remote_ai_crop_source_size(remote_image),
        'ai_crop_is_custom': _remote_ai_crop_is_custom(remote_image),
        'sort_order': remote_image.get('sort_order'),
    }
    if calibration_id is not None:
        update_kwargs['calibration_id'] = calibration_id
    ImageDB.update_image(image_id, **update_kwargs)
    remote_captured_at = _cloud_image_captured_at_to_local(remote_image.get('captured_at'))
    if remote_captured_at is not None:
        _update_image_columns_without_touching_observation(
            image_id,
            {'captured_at': remote_captured_at},
        )
    conn = get_connection()
    try:
        conn.execute(
            'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
            (str(remote_image.get('id') or '').strip() or None, datetime.now(timezone.utc).isoformat(), image_id),
        )
        conn.commit()
    finally:
        conn.close()


def _promote_temp_imported_image_if_needed(
    local_image_id: int,
    observation_id: int,
    temp_dir: Path,
) -> Path | None:
    """Copy a temp-backed imported image into a durable images path.

    Some sync tests and older rows do not have an observation folder_path yet,
    which means ``ImageDB.add_image(..., copy_to_folder=True)`` can leave the
    image pointing at the temp download path. That path is cleaned up at the
    end of the import, so the row would look missing on the next reconciliation
    pass. If the stored filepath still lives under the temp dir, copy it to a
    stable observation-owned location and update the row.
    """
    if local_image_id <= 0:
        return None
    try:
        temp_root = Path(temp_dir).resolve()
        image_row = ImageDB.get_image(int(local_image_id)) or {}
    except Exception:
        return None
    current_path_text = str(image_row.get('filepath') or '').strip()
    if not current_path_text:
        return None
    try:
        current_path = Path(current_path_text).resolve()
    except Exception:
        return None
    try:
        if not current_path.is_relative_to(temp_root):
            return current_path
    except Exception:
        if str(current_path).startswith(str(temp_root)):
            pass
        else:
            return current_path

    fallback_dir = Path(get_images_dir()) / f'observation_{int(observation_id)}'
    fallback_dir.mkdir(parents=True, exist_ok=True)
    target_path = fallback_dir / current_path.name
    counter = 1
    while target_path.exists():
        target_path = fallback_dir / f"{current_path.stem}_{counter}{current_path.suffix}"
        counter += 1
    shutil.copy2(current_path, target_path)
    try:
        ImageDB.update_image(int(local_image_id), filepath=str(target_path))
    except Exception:
        return None
    return target_path


def _ensure_local_metadata_only_microscope_anchor(
    client: "SporelyCloudClient" | None,
    local_observation_id: int,
    remote_image: dict,
    local_image: dict | None = None,
) -> int | None:
    """Create or update a file-less microscope anchor for a remote row.

    These rows are valid measurement anchors even though the raw microscope
    bytes were never uploaded. The local row must keep its cloud id mapping and
    metadata, but it must not trigger the download/materialization branch.
    """
    remote_row = dict(remote_image or {})
    if not _is_metadata_only_microscope_cloud_image(remote_row):
        return None

    cloud_image_id = str(remote_row.get('id') or '').strip()
    if not cloud_image_id:
        return None

    existing_local = dict(local_image or {})
    local_image_id = _safe_int(existing_local.get('id'))
    existing_path = str(existing_local.get('filepath') or '').strip()
    has_local_file = bool(existing_path)
    if has_local_file:
        try:
            has_local_file = Path(existing_path).exists()
        except Exception:
            has_local_file = False

    calibration_id = _local_calibration_id_for_image(remote_row)
    ai_crop_box = _remote_ai_crop_box(remote_row)
    ai_crop_source_size = _remote_ai_crop_source_size(remote_row)
    metadata_columns = {
        'image_type': 'microscope',
        'scale_microns_per_pixel': remote_row.get('scale_microns_per_pixel'),
        'notes': remote_row.get('notes'),
        'micro_category': remote_row.get('micro_category'),
        'objective_name': remote_row.get('objective_name'),
        'measure_color': remote_row.get('measure_color'),
        'mount_medium': remote_row.get('mount_medium'),
        'stain': remote_row.get('stain'),
        'sample_type': remote_row.get('sample_type'),
        'sample_source': _cloud_to_desktop_sample_source(remote_row.get('sample_source')),
        'contrast': remote_row.get('contrast'),
        'sort_order': remote_row.get('sort_order'),
        'crop_mode': remote_row.get('crop_mode'),
        'gps_source': remote_row.get('gps_source'),
        'resample_scale_factor': remote_row.get('resample_scale_factor'),
        'ai_crop_x1': ai_crop_box[0] if ai_crop_box and len(ai_crop_box) == 4 else None,
        'ai_crop_y1': ai_crop_box[1] if ai_crop_box and len(ai_crop_box) == 4 else None,
        'ai_crop_x2': ai_crop_box[2] if ai_crop_box and len(ai_crop_box) == 4 else None,
        'ai_crop_y2': ai_crop_box[3] if ai_crop_box and len(ai_crop_box) == 4 else None,
        'ai_crop_source_w': ai_crop_source_size[0] if ai_crop_source_size and len(ai_crop_source_size) == 2 else None,
        'ai_crop_source_h': ai_crop_source_size[1] if ai_crop_source_size and len(ai_crop_source_size) == 2 else None,
        'ai_crop_is_custom': _remote_ai_crop_is_custom(remote_row),
    }
    remote_captured_at = _cloud_image_captured_at_to_local(remote_row.get('captured_at'))
    if remote_captured_at is not None:
        metadata_columns['captured_at'] = remote_captured_at
    if calibration_id is not None:
        metadata_columns['calibration_id'] = calibration_id

    if local_image_id > 0:
        update_columns = dict(metadata_columns)
        if not has_local_file:
            update_columns['filepath'] = ''
        _update_image_columns_without_touching_observation(local_image_id, update_columns)
    else:
        created_local_image_id = ImageDB.add_image(
            observation_id=int(local_observation_id),
            filepath='',
            image_type='microscope',
            scale=remote_row.get('scale_microns_per_pixel'),
            notes=remote_row.get('notes'),
            micro_category=remote_row.get('micro_category'),
            objective_name=remote_row.get('objective_name'),
            measure_color=remote_row.get('measure_color'),
            mount_medium=remote_row.get('mount_medium'),
            stain=remote_row.get('stain'),
            sample_type=remote_row.get('sample_type'),
            sample_source=_cloud_to_desktop_sample_source(remote_row.get('sample_source')),
            contrast=remote_row.get('contrast'),
            sort_order=remote_row.get('sort_order'),
            crop_mode=remote_row.get('crop_mode'),
            gps_source=remote_row.get('gps_source'),
            resample_scale_factor=remote_row.get('resample_scale_factor'),
            calibration_id=calibration_id,
            ai_crop_box=ai_crop_box,
            ai_crop_source_size=ai_crop_source_size,
            ai_crop_is_custom=_remote_ai_crop_is_custom(remote_row),
            captured_at=remote_captured_at,
            copy_to_folder=False,
            mark_observation_dirty=False,
            source_role='cloud_recovery_cache',
            file_purpose='cache',
            original_mime_type=None,
            working_mime_type=None,
        )
        local_image_id = _safe_int(created_local_image_id)
        if local_image_id <= 0:
            return None
        has_local_file = False

    _update_image_columns_without_touching_observation(
        local_image_id,
        {
            'cloud_id': cloud_image_id,
            'synced_at': datetime.now(timezone.utc).isoformat(),
        },
    )
    if not has_local_file:
        _update_image_columns_without_touching_observation(
            local_image_id,
            {
                'filepath': '',
                'source_role': 'cloud_recovery_cache',
                'file_purpose': 'cache',
                'original_mime_type': None,
                'working_mime_type': None,
            },
        )

    if (
        client is not None
        and not getattr(client, 'is_pull_only', False)
        and not _portable_cloud_identity_pending_for_observation(
            int(local_observation_id)
        )
        and not _remote_image_desktop_id_current(remote_row, local_image_id)
    ):
        set_image_desktop_id = getattr(client, 'set_image_desktop_id', None)
        if callable(set_image_desktop_id):
            try:
                set_image_desktop_id(cloud_image_id, int(local_image_id))
                remote_row['desktop_id'] = int(local_image_id)
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise

    if local_image is not None:
        local_image['id'] = local_image_id
        local_image['cloud_id'] = cloud_image_id
        if not has_local_file:
            local_image['filepath'] = ''
    return local_image_id


def _remote_image_bytes_match_local(local_image: dict, remote_image: dict) -> bool:
    """True when the remote row points at bytes the local copy already has.

    Guards the metadata-only pull path: we only skip the download when the
    local file exists and its file-content signature matches the signature we
    recorded the last time we synced from the same cloud row. That signature
    is stamped both after upload and after previous downloads, so a cloud-side
    metadata-only edit (AI crop, calibration reference, notes …) keeps the
    signature stable and lets us patch the DB row without re-downloading.
    """
    local_cloud_id = str(local_image.get('cloud_id') or '').strip()
    remote_cloud_id = str(remote_image.get('id') or '').strip()
    if not local_cloud_id or not remote_cloud_id or local_cloud_id != remote_cloud_id:
        return False
    existing_path = str(local_image.get('filepath') or '').strip()
    if not existing_path:
        return False
    try:
        if not Path(existing_path).exists():
            return False
    except Exception:
        return False
    try:
        current_sig = _file_content_signature(existing_path)
    except Exception:
        return False
    if not current_sig:
        return False
    stored_sig = _load_cloud_image_file_signature(
        local_image.get('observation_id'), local_image.get('id')
    )
    return bool(stored_sig) and stored_sig == current_sig


def _sync_existing_remote_image_to_local(
    client: "SporelyCloudClient",
    local_image: dict,
    remote_image: dict,
    materialize_remote_images: bool = True,
) -> None:
    if not materialize_remote_images:
        return
    if _remote_image_bytes_match_local(local_image, remote_image):
        # Metadata-only remote change (e.g. AI crop or calibration reference).
        # Skip the download and thumbnail regeneration — patch the row only.
        _apply_remote_image_metadata_only_to_local(local_image, remote_image)
        return
    image_id = int(local_image.get('id'))
    existing_path = str(local_image.get('filepath') or '').strip()
    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_image_{image_id}_'))
    try:
        filename = Path(str(remote_image.get('original_filename') or '')).name or f'cloud_{image_id}.jpg'
        temp_path = temp_dir / filename
        client.download_image_file(str(remote_image.get('storage_path') or ''), temp_path)
        temp_path = _rename_to_detected_image_extension(temp_path)
        image_type = str(remote_image.get('image_type') or 'field').strip().lower()
        target_path = Path(existing_path) if existing_path else temp_path

        # Preserve any existing local field image. Cloud field copies are the
        # reduced sync artifact, so metadata can update without replacing the
        # desktop original bytes.
        local_file_exists = bool(existing_path and Path(existing_path).exists())
        local_is_larger = bool(local_file_exists and image_type == 'field')

        if image_type == 'field' and not local_is_larger:
            obs_id = int(local_image.get('observation_id') or 0)
            if obs_id > 0:
                lat, lon, altitude, gps_acc, datetime_str = _load_obs_exif_fallback(
                    obs_id,
                    fallback_datetime=remote_image.get('captured_at'),
                )
                img_lat = remote_image.get('gps_latitude') if remote_image.get('gps_latitude') is not None else lat
                img_lon = remote_image.get('gps_longitude') if remote_image.get('gps_longitude') is not None else lon
                img_alt = remote_image.get('gps_altitude') if remote_image.get('gps_altitude') is not None else altitude
                img_acc = remote_image.get('gps_accuracy') if remote_image.get('gps_accuracy') is not None else gps_acc
                _inject_obs_exif_into_field_image(
                    temp_path, img_lat, img_lon, img_alt, datetime_str,
                    camera_model=remote_image.get('camera_model'),
                    iso=remote_image.get('iso'),
                    exposure_time=remote_image.get('exposure_time'),
                    f_number=remote_image.get('f_number'),
                    gps_accuracy=img_acc,
                )

        # Replacing a microscope working file with the cloud copy is this
        # sync's own write-back, not a user edit. The mosaic signature
        # fingerprints each source image by path, size and mtime, so without
        # this the swap would make the next push re-render and re-key an
        # unchanged mosaic, breaking the no-op fast-path contract. Record
        # whether the signature was current BEFORE the swap; it is carried
        # forward below only in that case.
        obs_local_id = int(local_image.get('observation_id') or 0)
        mosaic_current_before_swap = bool(
            image_type == 'microscope'
            and existing_path
            and not local_is_larger
            and _local_mosaic_signature_is_current(obs_local_id)
        )

        if existing_path and not local_is_larger:
            detected_ext = _detected_image_extension(temp_path)
            if detected_ext and target_path.suffix.lower() != detected_ext:
                target_path = target_path.with_suffix(detected_ext)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(temp_path, target_path)
        # If local is larger it is the full-res desktop-imported original — keep it as-is.

        calibration_id = _local_calibration_id_for_image(remote_image)
        update_kwargs = {
            'filepath': str(target_path),
            'image_type': str(remote_image.get('image_type') or 'field'),
            'scale': remote_image.get('scale_microns_per_pixel'),
            'notes': remote_image.get('notes'),
            'micro_category': remote_image.get('micro_category'),
            'objective_name': remote_image.get('objective_name'),
            'measure_color': remote_image.get('measure_color'),
            'mount_medium': remote_image.get('mount_medium'),
            'stain': remote_image.get('stain'),
            'sample_type': remote_image.get('sample_type'),
            'sample_source': _cloud_to_desktop_sample_source(remote_image.get('sample_source')),
            'contrast': remote_image.get('contrast'),
            'crop_mode': remote_image.get('crop_mode'),
            'gps_source': remote_image.get('gps_source'),
            'resample_scale_factor': remote_image.get('resample_scale_factor'),
            'ai_crop_box': _remote_ai_crop_box(remote_image),
            'ai_crop_source_size': _remote_ai_crop_source_size(remote_image),
            'ai_crop_is_custom': _remote_ai_crop_is_custom(remote_image),
        }
        if calibration_id is not None:
            update_kwargs['calibration_id'] = calibration_id
        ImageDB.update_image(image_id, **update_kwargs)
        remote_captured_at = _cloud_image_captured_at_to_local(remote_image.get('captured_at'))
        if remote_captured_at is not None:
            _update_image_columns_without_touching_observation(
                image_id,
                {'captured_at': remote_captured_at},
            )
        conn = get_connection()
        try:
            conn.execute(
                'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                (str(remote_image.get('id') or '').strip() or None, datetime.now(timezone.utc).isoformat(), image_id),
            )
            conn.commit()
        finally:
            conn.close()
        try:
            _profile_generate_all_sizes(str(target_path), image_id)
        except Exception:
            pass
        try:
            file_sig = _file_content_signature(str(target_path))
            if file_sig:
                _store_cloud_image_file_signature(int(local_image.get('observation_id') or 0), image_id, file_sig)
        except Exception:
            pass
        # Same principle as the image file signature just above: a swap this
        # sync performed re-stamps its own change detector.
        if mosaic_current_before_swap:
            _carry_forward_local_mosaic_signature(obs_local_id)
        _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _remote_image_desktop_id_current(remote_image, local_image_id) -> bool:
    """True when the cloud row's desktop_id already equals the local image id.

    Guards every ``set_image_desktop_id`` call in the pull path. The PATCH used
    to be a tolerated no-op when the link was already correct, but the cloud
    ``updated_at`` trigger now advances on EVERY update — so an unconditional
    relink PATCH per pulled image makes each sync's own writes look like child
    changes to the next sync's probe (self-sustaining full-pull echo loop).
    """
    try:
        lid = int(local_image_id)
    except (TypeError, ValueError):
        return False
    if lid <= 0:
        return False
    try:
        return int((remote_image or {}).get('desktop_id')) == lid
    except (TypeError, ValueError):
        return False


def _apply_remote_images_to_local(
    client: "SporelyCloudClient",
    local_id: int,
    remote_images: list[dict],
    *,
    allow_delete: bool = True,
    materialize_remote_images: bool = True,
) -> list[str]:
    warnings: list[str] = []
    # ``set_image_desktop_id`` PATCHes the cloud row's desktop_id link — a
    # cloud write. Skip it when running under Download-from-Cloud so we
    # can honour the zero-cloud-writes contract. The pull_only flag comes
    # from the wrapping ``PullOnlyCloudClient`` proxy.
    pull_only = bool(getattr(client, 'is_pull_only', False))
    suppress_reverse_identity = (
        pull_only or _portable_cloud_identity_pending_for_observation(int(local_id))
    )
    local_images = ImageDB.get_images_for_observation(int(local_id))
    local_cloud_map = {
        str(img.get('cloud_id') or '').strip(): img
        for img in local_images
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('cloud_id') or '').strip()
    }
    remote_map = {
        str(img.get('id') or '').strip(): img
        for img in (remote_images or [])
        if should_pull_cloud_image_to_desktop(img)
        if str(img.get('id') or '').strip()
    }
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(remote_map.keys())

    for cloud_image_id, remote_image in remote_map.items():
        if cloud_image_id in tombstoned_cloud_ids:
            warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        local_image = local_cloud_map.get(cloud_image_id)
        if local_image:
            if _is_metadata_only_microscope_cloud_image(remote_image):
                _ensure_local_metadata_only_microscope_anchor(
                    client,
                    int(local_id),
                    remote_image,
                    local_image=local_image,
                )
                continue
            if not materialize_remote_images:
                _apply_remote_image_metadata_only_to_local(local_image, remote_image)
                if not suppress_reverse_identity and not _remote_image_desktop_id_current(
                    remote_image, local_image.get('id')
                ):
                    try:
                        client.set_image_desktop_id(cloud_image_id, int(local_image.get('id')))
                        remote_image['desktop_id'] = int(local_image.get('id'))
                    except Exception:
                        pass
                continue
            try:
                _sync_existing_remote_image_to_local(
                    client,
                    local_image,
                    remote_image,
                    materialize_remote_images=materialize_remote_images,
                )
            except CloudSyncError as exc:
                if _is_missing_cloud_image_error(exc):
                    print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, remote_image)}')
                    continue
                raise
            if not suppress_reverse_identity and not _remote_image_desktop_id_current(
                remote_image, local_image.get('id')
            ):
                try:
                    client.set_image_desktop_id(cloud_image_id, int(local_image.get('id')))
                    remote_image['desktop_id'] = int(local_image.get('id'))
                except Exception:
                    pass
            continue

        if _is_metadata_only_microscope_cloud_image(remote_image):
            _ensure_local_metadata_only_microscope_anchor(
                client,
                int(local_id),
                remote_image,
            )
            continue
        if not materialize_remote_images:
            continue

        storage_path = _normalize_cloud_media_key(remote_image.get('storage_path'))
        if not storage_path:
            warning = (
                f"obs {local_id}: skipped cloud image {cloud_image_id} "
                f"because it is missing storage path"
            )
            warnings.append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue
        temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
        try:
            filename = Path(str(remote_image.get('original_filename') or '')).name or f'{cloud_image_id}.jpg'
            download_path = temp_dir / filename
            try:
                client.download_image_file(storage_path, download_path)
                download_path = _rename_to_detected_image_extension(download_path)
            except CloudSyncError as exc:
                if _is_missing_cloud_image_error(exc):
                    print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, remote_image)}')
                    continue
                raise
            new_image_type = str(remote_image.get('image_type') or 'field').strip().lower()
            if new_image_type == 'field':
                lat, lon, altitude, gps_acc, datetime_str = _load_obs_exif_fallback(
                    int(local_id),
                    fallback_datetime=remote_image.get('captured_at'),
                )
                img_lat = remote_image.get('gps_latitude') if remote_image.get('gps_latitude') is not None else lat
                img_lon = remote_image.get('gps_longitude') if remote_image.get('gps_longitude') is not None else lon
                img_alt = remote_image.get('gps_altitude') if remote_image.get('gps_altitude') is not None else altitude
                img_acc = remote_image.get('gps_accuracy') if remote_image.get('gps_accuracy') is not None else gps_acc
                _inject_obs_exif_into_field_image(
                    download_path, img_lat, img_lon, img_alt, datetime_str,
                    camera_model=remote_image.get('camera_model'),
                    iso=remote_image.get('iso'),
                    exposure_time=remote_image.get('exposure_time'),
                    f_number=remote_image.get('f_number'),
                    gps_accuracy=img_acc,
                )
            local_image_id = ImageDB.add_image(
                observation_id=int(local_id),
                filepath=str(download_path),
                image_type=str(remote_image.get('image_type') or 'field'),
                scale=remote_image.get('scale_microns_per_pixel'),
                notes=remote_image.get('notes'),
                micro_category=remote_image.get('micro_category'),
                objective_name=remote_image.get('objective_name'),
                measure_color=remote_image.get('measure_color'),
                mount_medium=remote_image.get('mount_medium'),
                stain=remote_image.get('stain'),
                sample_type=remote_image.get('sample_type'),
                sample_source=_cloud_to_desktop_sample_source(remote_image.get('sample_source')),
                contrast=remote_image.get('contrast'),
                crop_mode=remote_image.get('crop_mode'),
                sort_order=remote_image.get('sort_order'),
                gps_source=remote_image.get('gps_source'),
                resample_scale_factor=remote_image.get('resample_scale_factor'),
                calibration_id=_local_calibration_id_for_image(remote_image),
                ai_crop_box=_remote_ai_crop_box(remote_image),
                ai_crop_source_size=_remote_ai_crop_source_size(remote_image),
                ai_crop_is_custom=_remote_ai_crop_is_custom(remote_image),
                captured_at=_cloud_image_captured_at_to_local(
                    remote_image.get('captured_at')
                ),
                copy_to_folder=True,
                mark_observation_dirty=False,
                source_role='cloud_recovery_cache',
                file_purpose='cache',
                original_mime_type=None,
                working_mime_type=guess_local_image_mime_type(download_path),
            )
            download_path = _promote_temp_imported_image_if_needed(
                int(local_image_id),
                int(local_id),
                temp_dir,
            ) or download_path
            conn = get_connection()
            try:
                conn.execute(
                    'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                    (cloud_image_id, datetime.now(timezone.utc).isoformat(), int(local_image_id)),
                )
                conn.commit()
            finally:
                conn.close()
            if not suppress_reverse_identity and not _remote_image_desktop_id_current(
                remote_image, local_image_id
            ):
                try:
                    client.set_image_desktop_id(cloud_image_id, int(local_image_id))
                    remote_image['desktop_id'] = int(local_image_id)
                except Exception:
                    pass
            try:
                _profile_generate_all_sizes(str(download_path), int(local_image_id))
            except Exception:
                pass
            _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    if allow_delete:
        for cloud_image_id, local_image in local_cloud_map.items():
            if cloud_image_id in remote_map:
                continue
            image_id = int(local_image.get('id') or 0)
            if image_id <= 0:
                continue
            if MeasurementDB.get_measurements_for_image(image_id):
                warnings.append(
                    f"obs {local_id}: kept local image {image_id} because it has measurements, even though the cloud copy was removed"
                )
                continue
            try:
                ImageDB.delete_image(image_id)
            except Exception as exc:
                warnings.append(f"obs {local_id}: could not remove local image {image_id}: {exc}")

    _normalize_cloud_pulled_image_order(int(local_id))
    return warnings


def _carry_forward_local_mosaic_signature(obs_local_id: int) -> None:
    """Re-stamp the stored mosaic signature after a sync-originated file swap.

    Only called when the signature was current immediately before the swap,
    so a mosaic that was already out of date is never masked.
    """
    try:
        signature = _current_local_mosaic_signature(obs_local_id)
        if signature:
            _store_local_mosaic_signature(obs_local_id, signature)
    except Exception:
        pass


def _import_remote_images(
    client: SporelyCloudClient | None,
    remote: dict,
    local_id: int,
    cloud_id: str | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
    materialize_remote_images: bool = True,
) -> dict:
    """Download and create local image rows for a newly pulled cloud observation."""
    result = {
        'imported': 0,
        'metadata_applied': 0,
        'skipped_materialization': 0,
        'failed': 0,
        'warnings': [],
        'errors': [],
        'complete': True,
    }
    if client is None:
        result['errors'].append('Could not load Sporely Cloud credentials.')
        result['complete'] = False
        return result
    suppress_reverse_identity = _portable_cloud_identity_pending_for_observation(
        int(local_id)
    )
    synced_at = datetime.now(timezone.utc).isoformat()
    
    remote_images_raw = (
        [dict(row or {}) for row in remote_images]
        if remote_images is not None
        else [dict(row or {}) for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])]
    )
    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=local_id,
        cloud_observation_id=cloud_id,
    )

    # Keep only active rows for the existing image import path.
    images_to_pull = [
        dict(row or {})
        for row in remote_images_raw
        if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
    ]
    tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in images_to_pull if str(row.get('id') or '').strip()]
    )
    
    if not images_to_pull:
        return result
        
    _extend_progress_total(progress_state, len(images_to_pull))
    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
    try:
        for idx, image_row in enumerate(images_to_pull, start=1):
            try:
                cloud_image_id = str(image_row.get('id') or '').strip()
                if cloud_image_id and cloud_image_id in tombstoned_cloud_ids:
                    warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
                    print(f'[cloud_sync] Warning: {warning}')
                    continue
                if remote_index and remote_total:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Importing cloud image {idx}/{len(images_to_pull)}…",
                        ),
                        progress_state,
                    )

                if not materialize_remote_images:
                    if _is_metadata_only_microscope_cloud_image(image_row):
                        local_image_id = _ensure_local_metadata_only_microscope_anchor(
                            client,
                            int(local_id),
                            image_row,
                        )
                        if local_image_id is None:
                            result['failed'] += 1
                            result['complete'] = False
                            result['errors'].append(
                                f'obs {int(local_id)}: failed to create metadata-only microscope anchor for cloud image {cloud_image_id or "?"}'
                            )
                        else:
                            result['imported'] += 1
                            result['metadata_applied'] += 1
                        continue
                    result['skipped_materialization'] += 1
                    result['complete'] = False
                    result['warnings'].append(
                        f"obs {int(local_id)}: deferred cloud image {cloud_image_id or '?'} because byte materialization is disabled"
                    )
                    continue

                storage_path = _normalize_cloud_media_key(image_row.get('storage_path'))
                if _is_metadata_only_microscope_cloud_image(image_row):
                    local_image_id = _ensure_local_metadata_only_microscope_anchor(
                        client,
                        int(local_id),
                        image_row,
                    )
                    if local_image_id is None:
                        result['failed'] += 1
                        result['complete'] = False
                        result['errors'].append(
                            f'obs {int(local_id)}: failed to create metadata-only microscope anchor for cloud image {cloud_image_id or "?"}'
                        )
                    else:
                        result['imported'] += 1
                        result['metadata_applied'] += 1
                    continue
                if not storage_path:
                    warning = (
                        f"obs {int(local_id)}: skipped cloud image {cloud_image_id or '?'} "
                        f"because it is missing storage path"
                    )
                    print(f'[cloud_sync] Warning: {warning}')
                    result['failed'] += 1
                    result['complete'] = False
                    result['warnings'].append(warning)
                    result['errors'].append(warning)
                    continue

                image_temp_dir = temp_dir / (str(image_row.get('id') or idx).strip() or str(idx))
                image_temp_dir.mkdir(parents=True, exist_ok=True)
                download_path = image_temp_dir / (Path(str(image_row.get('original_filename') or '')).name or 'img.jpg')
                client.download_image_file(storage_path, download_path)
                download_path = _rename_to_detected_image_extension(download_path)
                image_type = str(image_row.get('image_type') or 'field').strip().lower()
                if image_type == 'field':
                    lat, lon, altitude, gps_acc, datetime_str = _load_obs_exif_fallback(
                        int(local_id),
                        fallback_datetime=image_row.get('captured_at'),
                    )
                    img_lat = image_row.get('gps_latitude') if image_row.get('gps_latitude') is not None else lat
                    img_lon = image_row.get('gps_longitude') if image_row.get('gps_longitude') is not None else lon
                    img_alt = image_row.get('gps_altitude') if image_row.get('gps_altitude') is not None else altitude
                    img_acc = image_row.get('gps_accuracy') if image_row.get('gps_accuracy') is not None else gps_acc
                    _inject_obs_exif_into_field_image(
                        download_path,
                        img_lat,
                        img_lon,
                        img_alt,
                        datetime_str,
                        camera_model=image_row.get('camera_model'),
                        iso=image_row.get('iso'),
                        exposure_time=image_row.get('exposure_time'),
                        f_number=image_row.get('f_number'),
                        gps_accuracy=img_acc,
                    )

                local_image_id = ImageDB.add_image(
                    observation_id=int(local_id),
                    filepath=str(download_path),
                    image_type=str(image_type or 'field'),
                    scale=image_row.get('scale_microns_per_pixel'),
                    notes=image_row.get('notes'),
                    micro_category=image_row.get('micro_category'),
                    objective_name=image_row.get('objective_name'),
                    measure_color=image_row.get('measure_color'),
                    mount_medium=image_row.get('mount_medium'),
                    stain=image_row.get('stain'),
                    sample_type=image_row.get('sample_type'),
                    sample_source=_cloud_to_desktop_sample_source(image_row.get('sample_source')),
                    contrast=image_row.get('contrast'),
                    sort_order=image_row.get('sort_order'),
                    crop_mode=image_row.get('crop_mode'),
                    gps_source=image_row.get('gps_source'),
                    resample_scale_factor=image_row.get('resample_scale_factor'),
                    calibration_id=_local_calibration_id_for_image(image_row),
                    ai_crop_box=_remote_ai_crop_box(image_row),
                    ai_crop_source_size=_remote_ai_crop_source_size(image_row),
                    ai_crop_is_custom=_remote_ai_crop_is_custom(image_row),
                    captured_at=_cloud_image_captured_at_to_local(
                        image_row.get('captured_at')
                    ),
                    copy_to_folder=True,
                    mark_observation_dirty=False,
                    source_role='cloud_recovery_cache',
                    file_purpose='cache',
                    original_mime_type=None,
                    working_mime_type=guess_local_image_mime_type(download_path),
                )
                download_path = _promote_temp_imported_image_if_needed(
                    int(local_image_id),
                    int(local_id),
                    temp_dir,
                ) or download_path
                cloud_image_id = str(image_row.get('id') or '').strip()

                # Update sync metadata
                conn = get_connection()
                try:
                    conn.execute('UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?', 
                                 (cloud_image_id or None, synced_at, int(local_image_id)))
                    conn.commit()
                finally:
                    conn.close()

                if (
                    not getattr(client, 'is_pull_only', False)
                    and not suppress_reverse_identity
                    and not _remote_image_desktop_id_current(
                    image_row, local_image_id
                    )
                ):
                    set_image_desktop_id = getattr(client, 'set_image_desktop_id', None)
                    if cloud_image_id and callable(set_image_desktop_id):
                        try:
                            set_image_desktop_id(cloud_image_id, int(local_image_id))
                            image_row['desktop_id'] = int(local_image_id)
                        except Exception as exc:
                            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                                raise

                # Generate thumbnails and signature
                _profile_generate_all_sizes(str(download_path), int(local_image_id))
                file_sig = _file_content_signature(download_path)
                if file_sig:
                    _store_cloud_image_file_signature(local_id, local_image_id, file_sig)
                result['imported'] += 1
                _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_materializations')

            except Exception as e:
                if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                    raise
                detail = str(e or '').strip() or e.__class__.__name__
                result['failed'] += 1
                result['complete'] = False
                result['errors'].append(
                    f'obs {int(local_id)}: failed image import for cloud image {image_row.get("id") or "?"}: {detail}'
                )
                print(f'[cloud_sync] Failed image import: {e}')
            finally:
                _advance_progress(progress_state, 1)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    if result['failed'] or result['skipped_materialization']:
        result['complete'] = False
    try:
        _normalize_cloud_pulled_image_order(int(local_id))
    except Exception:
        pass
    return result


def cloud_media_materialization_state_for_observation(local_observation_id: int | str) -> dict:
    """Inspect whether a cloud-linked observation still needs media materialized locally."""
    summary = {
        'status': 'skipped',
        'reason': None,
        'local_observation_id': _safe_int(local_observation_id),
        'cloud_observation_id': None,
        'snapshot_available': False,
        'snapshot_has_media': False,
        'remote_images_considered': 0,
        'remote_measurements_considered': 0,
        'local_images_total': 0,
        'local_images_ready': 0,
        'local_images_missing_files': 0,
        'local_measurements_total': 0,
        'local_measurements_linked': 0,
        'local_measurements_missing': 0,
        'needs_materialization': False,
        'can_auto_start': False,
        'warnings': [],
    }

    local_id = _safe_int(local_observation_id)
    if local_id <= 0:
        summary['reason'] = 'invalid_local_observation_id'
        return summary

    local_obs = ObservationDB.get_observation(local_id)
    if not local_obs:
        summary['reason'] = 'local_observation_not_found'
        return summary
    suppress_reverse_identity = bool(
        local_obs.get('portable_cloud_identity_pending')
    )

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    summary['cloud_observation_id'] = cloud_id or None
    if not cloud_id:
        summary['reason'] = 'no_cloud_snapshot'
        return summary

    local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
    local_measurements_by_cloud_id, local_measurements_by_id = _load_local_measurement_lookup(local_id)
    local_image_rows = list(local_images_by_id.values()) or list(local_images_by_cloud_id.values())
    summary['local_images_total'] = len(local_image_rows)
    summary['local_measurements_total'] = len(local_measurements_by_id)

    snapshot_data = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_id))
    snapshot_has_media = 'images' in snapshot_data and 'measurements' in snapshot_data
    summary['snapshot_available'] = bool(snapshot_data)
    summary['snapshot_has_media'] = snapshot_has_media

    if snapshot_has_media:
        remote_images_raw = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
        remote_measurements_source = [dict(row or {}) for row in (snapshot_data.get('measurements') or [])]
        active_remote_images = [
            row
            for row in remote_images_raw
            if should_pull_cloud_image_to_desktop(row)
            and not str(row.get('deleted_at') or '').strip()
            and str(row.get('id') or '').strip()
        ]
        active_remote_images.sort(
            key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))
        )
        summary['remote_images_considered'] = len(active_remote_images)

        remote_measurements_by_obs = _group_remote_measurements_by_observation(
            remote_images_raw,
            remote_measurements_source,
        )
        remote_measurements = [dict(row or {}) for row in remote_measurements_by_obs.get(cloud_id, [])]
        summary['remote_measurements_considered'] = len(remote_measurements)

        tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(
            [str(row.get('id') or '').strip() for row in active_remote_images]
        )
        active_remote_image_ids = {
            str(row.get('id') or '').strip()
            for row in active_remote_images
            if str(row.get('id') or '').strip() and str(row.get('id') or '').strip() not in tombstoned_remote_image_ids
        }

        for remote_image in active_remote_images:
            cloud_image_id = str(remote_image.get('id') or '').strip()
            if not cloud_image_id or cloud_image_id in tombstoned_remote_image_ids:
                continue
            local_image = local_images_by_cloud_id.get(cloud_image_id)
            if local_image is None and not suppress_reverse_identity:
                remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
                if remote_desktop_id > 0:
                    local_image = local_images_by_id.get(remote_desktop_id)
            existing_asset_path = _resolve_existing_local_image_asset_path(
                str((local_image or {}).get('filepath') or '')
            )
            if (
                existing_asset_path is None
                and _is_metadata_only_microscope_cloud_image(remote_image)
                and _is_local_metadata_only_microscope_anchor(local_image)
            ):
                summary['local_images_ready'] += 1
            elif existing_asset_path is None:
                summary['local_images_missing_files'] += 1
            else:
                summary['local_images_ready'] += 1

        for remote_row in remote_measurements:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue
            local_measurement = local_measurements_by_cloud_id.get(remote_measurement_id)
            remote_image_id = str(remote_row.get('image_id') or '').strip()
            if remote_image_id not in active_remote_image_ids:
                continue
            if local_measurement is None:
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                if remote_desktop_measurement_id > 0:
                    local_measurement = local_measurements_by_id.get(remote_desktop_measurement_id)
            if local_measurement is None:
                summary['local_measurements_missing'] += 1
            else:
                summary['local_measurements_linked'] += 1

        summary['needs_materialization'] = bool(
            summary['remote_images_considered'] and (
                summary['local_images_missing_files'] > 0
                or summary['local_measurements_missing'] > 0
                or summary['local_images_ready'] < summary['remote_images_considered']
            )
        )
        summary['can_auto_start'] = bool(summary['needs_materialization'])
        summary['status'] = 'needs_materialization' if summary['needs_materialization'] else 'already_materialized'
        summary['reason'] = 'missing_local_media' if summary['needs_materialization'] else 'already_materialized'
        return summary

    # No stored snapshot media. Keep the UI conservative: if there are already
    # local files, treat the observation as materialized; otherwise let the UI
    # offer a manual retry path without auto-starting a live fetch.
    for local_image in local_image_rows:
        existing_asset_path = _resolve_existing_local_image_asset_path(
            str((local_image or {}).get('filepath') or '')
        )
        if existing_asset_path is None:
            summary['local_images_missing_files'] += 1
        else:
            summary['local_images_ready'] += 1

    if summary['local_images_total'] > 0 and summary['local_images_missing_files'] == 0:
        summary['status'] = 'already_materialized'
        summary['reason'] = 'snapshot_missing_media_but_local_images_exist'
    else:
        summary['status'] = 'needs_materialization'
        summary['reason'] = 'snapshot_missing_media'
        summary['needs_materialization'] = bool(summary['local_images_total'] == 0 or summary['local_images_missing_files'] > 0)
        summary['can_auto_start'] = False
    return summary
