"""Pure observation value normalizers, tolerances and snapshot field sets.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import math
from datetime import (
    datetime,
    timezone,
)


def _normalize_sharing_scope(value: str | None, fallback: str = 'private') -> str:
    raw = str(value or '').strip().lower()
    if raw == 'draft':
        return 'private'
    if raw in {'private', 'friends', 'public'}:
        return raw
    fallback_raw = str(fallback or 'private').strip().lower()
    if fallback_raw == 'draft':
        return 'private'
    return fallback_raw if fallback_raw in {'private', 'friends', 'public'} else 'private'


def _sharing_scope_to_cloud_visibility(value: str | None, fallback: str = 'private') -> str:
    """Map local desktop sharing scope to the Phase 7 cloud visibility value."""
    normalized = _normalize_sharing_scope(value, fallback=fallback)
    return normalized


_OBSERVATION_INT_FIELDS = {
    'artsdata_id',
    'artportalen_id',
    'inaturalist_id',
    'mushroomobserver_id',
    'determination_method',
}


_OBSERVATION_FLOAT_FIELDS = {
    'gps_latitude',
    'gps_longitude',
    'ai_selected_probability',
    'auto_threshold',
}


_OBSERVATION_FLOAT_ABS_TOL = 1e-9


_OBSERVATION_FLOAT_REL_TOL = 1e-9


_OBSERVATION_GPS_ABS_TOL = 1e-6


def _normalize_observation_bool_value(value, *, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, float):
        return value != 0.0
    text = str(value or '').strip().lower()
    if not text:
        return default
    if text in {'true', '1', 'yes', 'on'}:
        return True
    if text in {'false', '0', 'no', 'off'}:
        return False
    if text in {'none', 'null'}:
        return default
    return bool(value)


def _normalize_observation_int_value(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _normalize_observation_float_value(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_observation_json_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        normalized = _normalize_snapshot_value(value)
        if normalized in ({}, [], ''):
            return None
        return normalized
    if isinstance(value, (bool, int, float)):
        return value
    text = str(value or '').strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return text
    if data in ({}, [], ''):
        return None
    return _normalize_snapshot_value(data)


_SNAPSHOT_OBS_FIELDS = [
    'id', 'desktop_id', 'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude', 'location_public',
    'is_draft', 'location_precision',
    'ai_selected_service', 'ai_selected_taxon_id',
    'ai_selected_scientific_name', 'ai_selected_probability',
    'ai_selected_at',
    # Red-list category picked for the observation (either directly by the
    # user or copied from the selected AI prediction). Included in the
    # snapshot so cloud-pulled observations show the same badge under
    # Taxonomy → Red list as they do in sporely-web.
    'red_list_category', 'red_list_categories_json',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'visibility',
    'spore_data_visibility',
    'country_code',
    'region_id',
]


_SNAPSHOT_IMG_FIELDS = [
    'id', 'desktop_id', 'sort_order', 'image_type', 'micro_category',
    'captured_at',
    'calibration_uuid',
    'objective_name', 'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain',
    # sample_type = specimen condition; sample_source = where the material
    # was taken from. Both participate in the image snapshot / media
    # signature so cloud pull round-trips them and metadata-only sync can
    # patch them without triggering byte uploads.
    'sample_type', 'sample_source',
    'contrast', 'measure_color',
    'crop_mode', 'notes',
    'gps_source', 'storage_path', 'original_filename',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h', 'ai_crop_is_custom',
    'upload_mode', 'source_width', 'source_height',
    'stored_width', 'stored_height', 'stored_bytes',
]


# Future original-object metadata that we preserve in snapshots when it is
# already present on the cloud row, but do not yet use for sync decisions.
_SNAPSHOT_IMG_PASSIVE_FIELDS = [
    'original_storage_path',
]


_OBSERVATION_TIMESTAMP_FIELDS = frozenset({'ai_selected_at'})


def _normalize_observation_timestamp_value(value) -> str | None:
    """Canonical instant for comparison: '...Z' and '...+00:00' must match.

    The desktop stores UTC timestamps with a 'Z' suffix while PostgREST
    returns '+00:00'; raw string equality treats the same instant as a
    perpetual local-only change (dirty/push loop). Unparseable values fall
    back to the stripped original so garbage still compares deterministically.
    """
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).isoformat()
    except ValueError:
        return text


def _observation_field_values_match(field: str, left, right) -> bool:
    if field in _OBSERVATION_TIMESTAMP_FIELDS:
        return (
            _normalize_observation_timestamp_value(left)
            == _normalize_observation_timestamp_value(right)
        )
    if field in _OBSERVATION_FLOAT_FIELDS:
        left_value = _normalize_observation_float_value(left)
        right_value = _normalize_observation_float_value(right)
        if left_value is None or right_value is None:
            return left_value == right_value
        return math.isclose(
            left_value,
            right_value,
            rel_tol=_OBSERVATION_FLOAT_REL_TOL,
            abs_tol=(
                _OBSERVATION_GPS_ABS_TOL
                if field in {'gps_latitude', 'gps_longitude'}
                else _OBSERVATION_FLOAT_ABS_TOL
            ),
        )
    return left == right


def _parse_sync_timestamp(value) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_snapshot_value(value):
    if isinstance(value, bool):
        return bool(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): _normalize_snapshot_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize_snapshot_value(v) for v in value]
    return str(value)


def _normalize_image_captured_at_for_cloud(value, *, local: bool) -> str | None:
    """Return one image capture timestamp as a canonical UTC ISO value.

    SQLite stores capture timestamps as local wall-clock text. Postgres
    ``timestamptz`` values are absolute instants. Naive local values therefore
    use the host timezone (including historical DST), while a defensive naive
    cloud value is interpreted as UTC. A missing/invalid value stays missing;
    ``created_at`` is never used as a substitute.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or '').strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except Exception:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone() if local else parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


_SNAPSHOT_MEAS_FIELDS = [
    'id', 'desktop_id', 'image_id', 'length_um', 'width_um', 'measurement_type',
    'gallery_rotation', 'p1_x', 'p1_y', 'p2_x', 'p2_y', 'p3_x', 'p3_y',
    'p4_x', 'p4_y', 'measured_at',
]
