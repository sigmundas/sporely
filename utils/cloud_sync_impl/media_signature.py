"""Observation local media signature: computation, comparison and refresh.

A local media signature never proves remote upload completeness.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction, except that ``_local_cloud_media_signature``
sets ``sqlite3.Row`` instead of ``__import__('sqlite3').Row`` (the same object):
owners may not use dynamic import machinery.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from database.schema import get_connection

from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.reconciliation.measurements import _measurement_compare_payload
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_image_captured_at_for_cloud,
    _normalize_snapshot_value,
)
from utils.cloud_sync_impl.sync_state import _store_local_cloud_media_signature
from utils.cloud_sync_impl.tombstones import _local_tombstoned_cloud_image_ids


_CLOUD_LOCAL_MEDIA_RENDER_VERSION = "2"


_LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS = (
    # Image capture time joined the cloud contract after local signatures were
    # already in use. Missing legacy values are equivalent to explicit NULL.
    'captured_at',
    'ai_crop_x1',
    'ai_crop_y1',
    'ai_crop_x2',
    'ai_crop_y2',
    'ai_crop_source_w',
    'ai_crop_source_h',
    'ai_crop_is_custom',
    # calibration_uuid was added to the signature so retroactive recalibration
    # (which only reassigns the image's calibration link) is picked up as a
    # metadata-only change. Older stored signatures don't have it — normalize
    # missing to None to keep those comparisons stable.
    'calibration_uuid',
)


def _parsed_local_media_signature(signature: str | None) -> dict:
    text = str(signature or '').strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalized_local_media_signature_payload(
    payload: dict | None,
    *,
    include_measurements: bool = True,
) -> dict:
    normalized = dict(payload or {})
    # These legacy fields affected external publishing or its selection UI,
    # never the clean cloud bytes. Drop them so old stored signatures remain
    # comparable without forcing a one-time WebP regeneration.
    normalized.pop('cloud_media_signature', None)
    normalized.pop('excluded_image_ids_raw', None)
    normalized.pop('gallery_settings_raw', None)
    images = []
    for row in list(normalized.get('images') or []):
        if not isinstance(row, dict):
            continue
        image_payload = dict(row)
        image_payload.pop('sort_order', None)
        for key in _LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS:
            image_payload.setdefault(key, None)
        for path_key in ('filepath', 'original_filepath'):
            path_payload = image_payload.get(path_key)
            if isinstance(path_payload, dict):
                normalized_path = dict(path_payload)
                normalized_path.pop('mtime_ns', None)
                image_payload[path_key] = normalized_path
        images.append(image_payload)
    normalized['images'] = sorted(
        images,
        key=lambda row: (
            str(row.get('desktop_id') or ''),
            str(row.get('id') or ''),
            str(row.get('original_filename') or ''),
            str(row.get('image_type') or ''),
        ),
    )
    if not include_measurements:
        normalized.pop('measurements', None)
    return normalized


def _local_media_signatures_match(
    stored_signature: str | None,
    current_signature: str | None,
    *,
    include_measurements: bool = True,
) -> bool:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    if not stored_text or not current_text:
        return stored_text == current_text
    if stored_text == current_text:
        return True
    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    if not stored_payload or not current_payload:
        return False
    return _normalized_local_media_signature_payload(
        stored_payload,
        include_measurements=include_measurements,
    ) == _normalized_local_media_signature_payload(
        current_payload,
        include_measurements=include_measurements,
    )


def _store_local_media_signature_if_equivalent(
    observation_id: int | str,
    stored_signature: str | None,
    current_signature: str | None,
) -> None:
    current_text = str(current_signature or '').strip()
    if not current_text:
        return
    stored_text = str(stored_signature or '').strip()
    if stored_text == current_text:
        return
    if _local_media_signatures_match(stored_text, current_text):
        _store_local_cloud_media_signature(observation_id, current_text)


def _refresh_local_cloud_media_signature(observation_id: int | str) -> str:
    signature = _local_cloud_media_signature(observation_id)
    if str(signature or '').strip():
        _store_local_cloud_media_signature(observation_id, signature)
    return signature


def _path_stat_signature(path_value: str | None) -> dict:
    path_text = str(path_value or '').strip()
    if not path_text:
        return {'path': '', 'exists': False}
    path = Path(path_text)
    try:
        stat = path.stat()
        return {
            'path': path_text,
            'exists': True,
            'size': int(stat.st_size),
            'mtime_ns': int(getattr(stat, 'st_mtime_ns', int(stat.st_mtime * 1_000_000_000))),
        }
    except Exception:
        return {'path': path_text, 'exists': path.exists()}


def _local_cloud_media_signature(
    observation_id: int | str,
    *,
    include_measurements: bool = True,
) -> str:
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return ''
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        try:
            image_columns = {
                str(info["name"])
                for info in cursor.execute("PRAGMA table_info(images)").fetchall()
            }
        except Exception:
            image_columns = set()
        try:
            calibration_table_exists = cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='calibrations'"
            ).fetchone() is not None
        except Exception:
            calibration_table_exists = False
        has_image_calibration_id = 'calibration_id' in image_columns
        calibration_id_column_sql = (
            "images.calibration_id" if has_image_calibration_id else "NULL AS calibration_id"
        )
        calibration_uuid_column_sql = (
            "calibrations.calibration_uuid AS calibration_uuid"
            if calibration_table_exists and has_image_calibration_id
            else "NULL AS calibration_uuid"
        )
        # `sample_source` is a Stage-2 addition; on databases that haven't
        # applied `database/schema.py` migration yet (test schemas, older
        # installs) the column may not exist. Fall back to NULL so the SELECT
        # doesn't blow up, matching the calibration_id treatment above.
        sample_source_column_sql = (
            "images.sample_source" if 'sample_source' in image_columns
            else "NULL AS sample_source"
        )
        captured_at_column_sql = (
            "images.captured_at" if 'captured_at' in image_columns
            else "NULL AS captured_at"
        )
        calibration_join_sql = (
            "LEFT JOIN calibrations ON calibrations.id = images.calibration_id"
            if calibration_table_exists and has_image_calibration_id
            else ""
        )
        cursor.execute(
            f'''
            SELECT
                images.id,
                images.filepath,
                images.original_filepath,
                images.sort_order,
                images.image_type,
                images.micro_category,
                {captured_at_column_sql},
                images.objective_name,
                images.scale_microns_per_pixel,
                images.resample_scale_factor,
                images.mount_medium,
                images.stain,
                images.sample_type,
                {sample_source_column_sql},
                images.contrast,
                images.measure_color,
                images.crop_mode,
                images.notes,
                images.gps_source,
                images.ai_crop_x1,
                images.ai_crop_y1,
                images.ai_crop_x2,
                images.ai_crop_y2,
                images.ai_crop_source_w,
                images.ai_crop_source_h,
                images.ai_crop_is_custom,
                {calibration_id_column_sql},
                {calibration_uuid_column_sql}
            FROM images
            {calibration_join_sql}
            WHERE images.observation_id = ?
            ORDER BY
                CASE WHEN images.sort_order IS NULL THEN 1 ELSE 0 END,
                images.sort_order,
                images.image_type,
                images.micro_category,
                images.created_at,
                images.id
            ''',
            (obs_id,),
        )
        image_rows = [dict(row) for row in cursor.fetchall()]
        tombstoned_cloud_ids = _local_tombstoned_cloud_image_ids(
            [str(row.get('cloud_id') or '').strip() for row in image_rows if str(row.get('cloud_id') or '').strip()]
        )
        if tombstoned_cloud_ids:
            image_rows = [
                row
                for row in image_rows
                if str(row.get('cloud_id') or '').strip() not in tombstoned_cloud_ids
            ]
        measurement_rows: list[dict] = []
        if include_measurements:
            cursor.execute(
                '''
                SELECT
                    m.id,
                    m.image_id,
                    m.length_um,
                    m.width_um,
                    m.measurement_type,
                    m.notes,
                    m.p1_x,
                    m.p1_y,
                    m.p2_x,
                    m.p2_y,
                    m.p3_x,
                    m.p3_y,
                    m.p4_x,
                    m.p4_y,
                    m.gallery_rotation
                FROM spore_measurements m
                JOIN images i ON i.id = m.image_id
                WHERE i.observation_id = ?
                ORDER BY m.id
                ''',
                (obs_id,),
            )
            measurement_rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    payload = {
        'render_version': _CLOUD_LOCAL_MEDIA_RENDER_VERSION,
        'cloud_image_size_mode': 'full',
        'images': [
            {
                'id': _safe_int(row.get('id')),
                'filepath': _path_stat_signature(row.get('filepath')),
                'original_filepath': _path_stat_signature(row.get('original_filepath')),
                'sort_order': _normalize_snapshot_value(row.get('sort_order')),
                'image_type': _normalize_snapshot_value(row.get('image_type')),
                'micro_category': _normalize_snapshot_value(row.get('micro_category')),
                'captured_at': _normalize_image_captured_at_for_cloud(
                    row.get('captured_at'), local=True
                ),
                'objective_name': _normalize_snapshot_value(row.get('objective_name')),
                'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
                'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
                'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
                'stain': _normalize_snapshot_value(row.get('stain')),
                'sample_type': _normalize_snapshot_value(row.get('sample_type')),
                'sample_source': _normalize_snapshot_value(row.get('sample_source')),
                'contrast': _normalize_snapshot_value(row.get('contrast')),
                'measure_color': _normalize_snapshot_value(row.get('measure_color')),
                'crop_mode': _normalize_snapshot_value(row.get('crop_mode')),
                'notes': _normalize_snapshot_value(row.get('notes')),
                'gps_source': _normalize_snapshot_value(row.get('gps_source')),
                'ai_crop_x1': _normalize_snapshot_value(row.get('ai_crop_x1')),
                'ai_crop_y1': _normalize_snapshot_value(row.get('ai_crop_y1')),
                'ai_crop_x2': _normalize_snapshot_value(row.get('ai_crop_x2')),
                'ai_crop_y2': _normalize_snapshot_value(row.get('ai_crop_y2')),
                'ai_crop_source_w': _normalize_snapshot_value(row.get('ai_crop_source_w')),
                'ai_crop_source_h': _normalize_snapshot_value(row.get('ai_crop_source_h')),
                'calibration_uuid': _normalize_snapshot_value(row.get('calibration_uuid')),
            }
            for row in image_rows
        ],
    }
    if include_measurements:
        payload['measurements'] = [
            {
                **_measurement_compare_payload(row, local=False),
                'notes': _normalize_snapshot_value(row.get('notes')),
            }
            for row in measurement_rows
        ]
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _local_cloud_image_media_signature(observation_id: int | str) -> str:
    return _local_cloud_media_signature(observation_id, include_measurements=False)
