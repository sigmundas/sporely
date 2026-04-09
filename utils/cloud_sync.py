"""Bidirectional sync between local SQLite and Sporely cloud (Supabase).

Desktop → Cloud  push observations and selected images not yet synced
Cloud → Desktop  pull observations created on mobile/web (no desktop_id)

One-time Supabase SQL to run in the SQL editor for optimal upsert performance:
    ALTER TABLE public.observations
        ADD CONSTRAINT observations_desktop_id_user_unique UNIQUE (desktop_id, user_id);
    ALTER TABLE public.observation_images
        ADD CONSTRAINT observation_images_desktop_id_user_unique UNIQUE (desktop_id, user_id);
"""
from __future__ import annotations

import io
import json
import hashlib
import mimetypes
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import requests
from PIL import Image

from app_identity import runtime_profile_scope, using_isolated_profile
from database.schema import get_connection, get_app_settings, update_app_settings
from database.models import ObservationDB, ImageDB, SettingsDB, MeasurementDB
from utils.thumbnail_generator import generate_all_sizes

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'
SUPABASE_KEY = 'sb_publishable_nZrERVFN3WR4Aqn2yggc7Q_siAG1TCV'
_CLOUD_KEYRING_SERVICE = 'Sporely.Cloud'
_CLOUD_LEGACY_KEYRING_SERVICE = 'MycoLog.Cloud'
_profile_suffix = runtime_profile_scope()
_CLOUD_KEYRING_ACCOUNT = f'password:{_profile_suffix}' if _profile_suffix else 'password'

# Observation columns we push to cloud (excludes local-only fields)
_OBS_PUSH_COLS = [
    'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude',
    'location_public',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'spore_data_visibility',
]
# Never push: private_comment, ai_state_json, folder_path, cloud_id, sync_status, synced_at


def _normalize_sharing_scope(value: str | None, fallback: str = 'private') -> str:
    raw = str(value or '').strip().lower()
    if raw in {'private', 'friends', 'public'}:
        return raw
    fallback_raw = str(fallback or 'private').strip().lower()
    return fallback_raw if fallback_raw in {'private', 'friends', 'public'} else 'private'


def _encode_postgrest_filter_value(value: str | None) -> str:
    """Encode filter values for PostgREST query strings.

    Timestamps may contain '+' in timezone offsets, which must be percent-encoded
    inside a URL query or they can be parsed incorrectly.
    """
    return quote(str(value or '').strip(), safe='')

_IMG_PUSH_COLS = [
    'sort_order', 'image_type', 'micro_category', 'objective_name',
    'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'contrast', 'notes',
    'gps_source', 'storage_path',
]

_MEAS_PUSH_COLS = [
    'length_um', 'width_um', 'measurement_type',
    'p1_x', 'p1_y', 'p2_x', 'p2_y',
    'p3_x', 'p3_y', 'p4_x', 'p4_y',
    'measured_at',
]

_SETTING_INCLUDE_ANNOTATIONS = "artsobs_publish_include_annotations"
_SETTING_SHOW_SCALE_BAR = "artsobs_publish_show_scale_bar"
_SETTING_INCLUDE_MEASURE_PLOTS = "artsobs_publish_include_measure_plots"
_SETTING_INCLUDE_THUMBNAIL_GALLERY = "artsobs_publish_include_thumbnail_gallery"
_SETTING_INCLUDE_PLATE = "artsobs_publish_include_plate"
_SETTING_INCLUDE_COPYRIGHT = "artsobs_publish_include_copyright"
_SETTING_IMAGE_LICENSE = "artsobs_publish_image_license"
_SETTING_PROFILE_NAME = "profile_name"
_SETTING_PROFILE_EMAIL = "profile_email"
_SETTING_CLOUD_MEDIA_SIGNATURE = "sporely_cloud_media_signature_v1"
_SETTING_CLOUD_IMAGE_SIZE_MODE = "sporely_cloud_image_size_mode"
_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX = "sporely_cloud_snapshot_obs_"
_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX = "sporely_cloud_image_file_sig_"
_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX = "sporely_cloud_local_media_sig_obs_"
_CLOUD_LOCAL_MEDIA_RENDER_VERSION = "2"

_SNAPSHOT_OBS_FIELDS = [
    'id', 'desktop_id', 'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude', 'location_public',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'visibility', 'sharing_scope',
    'spore_data_visibility',
]

_SNAPSHOT_IMG_FIELDS = [
    'id', 'desktop_id', 'sort_order', 'image_type', 'micro_category',
    'objective_name', 'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'contrast', 'notes',
    'gps_source', 'storage_path', 'original_filename',
]

_CONFLICT_COMPARE_FIELDS = [
    'date',
    'genus',
    'species',
    'common_name',
    'species_guess',
    'location',
    'gps_latitude',
    'gps_longitude',
    'habitat',
    'notes',
    'open_comment',
    'publish_target',
    'visibility',
    'location_public',
    'spore_statistics',
]

_CONFLICT_FIELD_LABELS = {
    'date': 'Date',
    'genus': 'Genus',
    'species': 'Species',
    'common_name': 'Common name',
    'species_guess': 'Species guess',
    'location': 'Location',
    'gps_latitude': 'Latitude',
    'gps_longitude': 'Longitude',
    'habitat': 'Habitat',
    'notes': 'Notes',
    'open_comment': 'Public comment',
    'publish_target': 'Publishing target',
    'visibility': 'Visibility',
    'location_public': 'Public GPS',
    'spore_statistics': 'Spore statistics',
}

ProgressCallback = Callable[[str, int, int], None]
PreparedImagesCallback = Callable[[dict, ProgressCallback | None], tuple[list[dict], object | None, list[str]]]

_PUSH_CONFLICT_RE = re.compile(
    r"^obs\s+(?P<local_id>\d+):\s+skipped desktop push because the linked cloud observation changed on the web$"
)
_PULL_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+skipped remote update because local observation\s+(?P<local_id>\d+)\s+has unsynced desktop edits$"
)
_REVIEW_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+needs review before applying remaining cloud changes to local observation\s+(?P<local_id>\d+)(?:\s+\((?P<reason>.*)\))?$"
)


class CloudSyncError(Exception):
    pass


def summarize_sync_issues(errors: list[str] | tuple[str, ...] | None) -> dict:
    conflict_entries: dict[str, dict] = {}
    other_errors: list[str] = []

    for raw_error in list(errors or []):
        text = str(raw_error or '').strip()
        if not text:
            continue
        push_match = _PUSH_CONFLICT_RE.match(text)
        if push_match:
            local_id = int(push_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['push_skipped'] = True
            continue
        pull_match = _PULL_CONFLICT_RE.match(text)
        if pull_match:
            local_id = int(pull_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(pull_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            continue
        review_match = _REVIEW_CONFLICT_RE.match(text)
        if review_match:
            local_id = int(review_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(review_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            continue
        other_errors.append(text)

    conflicts = sorted(
        conflict_entries.values(),
        key=lambda row: (
            int(row.get('local_id') or 0),
            str(row.get('cloud_id') or ''),
        ),
    )
    return {
        'conflicts': conflicts,
        'conflict_count': len(conflicts),
        'other_errors': other_errors,
        'other_count': len(other_errors),
        'display_count': len(conflicts) + len(other_errors),
    }


def _parse_cloud_observation_snapshot(snapshot: str | None) -> dict:
    text = str(snapshot or '').strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    images = data.get('images')
    if isinstance(images, list):
        data['images'] = [
            dict(row or {})
            for row in images
            if should_pull_cloud_image_to_desktop(row)
        ]
    return data


def _normalize_observation_field_value(field: str, value):
    if field == 'date':
        text = str(value or '').strip()
        if not text:
            return None
        if len(text) >= 10 and re.match(r'^\d{4}-\d{2}-\d{2}', text):
            return text[:10]
        return text
    if field in {'location_public', 'uncertain', 'unspontaneous', 'interesting_comment'}:
        return None if value is None else bool(value)
    return _normalize_snapshot_value(value)


def _observation_compare_payload(record: dict | None, *, local: bool) -> dict:
    row = dict(record or {})
    payload: dict = {}
    for field in _SNAPSHOT_OBS_FIELDS:
        if field == 'id':
            payload[field] = _normalize_observation_field_value(field, (
                (row.get('cloud_id') if local else row.get('id'))
            ))
        elif field == 'desktop_id':
            payload[field] = _normalize_observation_field_value(field, (
                (row.get('id') if local else row.get('desktop_id'))
            ))
        elif field == 'visibility':
            payload[field] = _normalize_observation_field_value(field, (
                _normalize_sharing_scope(
                    row.get('sharing_scope') if local else (row.get('visibility') or row.get('sharing_scope')),
                    fallback='private',
                )
            ))
        elif field == 'sharing_scope':
            payload[field] = _normalize_observation_field_value(field, (
                _normalize_sharing_scope(
                    row.get('sharing_scope') if local else (row.get('visibility') or row.get('sharing_scope')),
                    fallback='private',
                )
            ))
        else:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    return payload


def _baseline_observation_compare_payload(record: dict | None) -> dict:
    """Normalize a stored snapshot observation for fair comparison with live rows."""
    row = dict(record or {})
    payload: dict = {}
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
        elif field in {'visibility', 'sharing_scope'}:
            payload[field] = _normalize_observation_field_value(
                field,
                _normalize_sharing_scope(
                    row.get('visibility') or row.get('sharing_scope'),
                    fallback='private',
                ),
            )
        else:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    return payload


def _local_image_snapshot_payload(image_row: dict | None) -> dict:
    row = dict(image_row or {})
    payload = {
        'id': _normalize_snapshot_value(str(row.get('cloud_id') or '').strip() or None),
        'desktop_id': _normalize_snapshot_value(row.get('id')),
        'sort_order': _normalize_snapshot_value(row.get('sort_order')),
        'image_type': _normalize_snapshot_value(row.get('image_type')),
        'micro_category': _normalize_snapshot_value(row.get('micro_category')),
        'objective_name': _normalize_snapshot_value(row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
        'stain': _normalize_snapshot_value(row.get('stain')),
        'sample_type': _normalize_snapshot_value(row.get('sample_type')),
        'contrast': _normalize_snapshot_value(row.get('contrast')),
        'notes': _normalize_snapshot_value(row.get('notes')),
        'gps_source': _normalize_snapshot_value(
            None if row.get('gps_source') is None else bool(row.get('gps_source'))
        ),
        'storage_path': None,
        'original_filename': _normalize_snapshot_value(
            Path(str(row.get('filepath') or '')).name or None
        ),
    }
    return payload


def _image_compare_key(image_row: dict | None) -> str:
    row = dict(image_row or {})
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    filename = str(row.get('original_filename') or '').strip()
    if desktop_id:
        return f'desktop:{desktop_id}'
    if filename:
        return f'name:{filename}'
    if cloud_id:
        return f'cloud:{cloud_id}'
    return json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _image_label(image_row: dict | None) -> str:
    row = dict(image_row or {})
    filename = str(row.get('original_filename') or '').strip()
    image_type = str(row.get('image_type') or '').strip()
    if filename and image_type:
        return f'{filename} ({image_type})'
    if filename:
        return filename
    cloud_id = str(row.get('id') or '').strip()
    desktop_id = str(row.get('desktop_id') or '').strip()
    if cloud_id:
        return f'cloud image {cloud_id}'
    if desktop_id:
        return f'local image {desktop_id}'
    return 'image'


def _image_metadata_payload(image_row: dict | None) -> dict:
    row = dict(image_row or {})
    return {
        field: row.get(field)
        for field in _SNAPSHOT_IMG_FIELDS
        if field not in {'id', 'desktop_id', 'sort_order', 'storage_path', 'original_filename'}
    }


def _summarize_image_changes(current_images: list[dict], baseline_images: list[dict]) -> list[str]:
    current = [dict(row or {}) for row in (current_images or [])]
    baseline = [dict(row or {}) for row in (baseline_images or [])]
    current_keys = [_image_compare_key(row) for row in current]
    baseline_keys = [_image_compare_key(row) for row in baseline]
    current_map = {_image_compare_key(row): row for row in current}
    baseline_map = {_image_compare_key(row): row for row in baseline}

    added = [current_map[key] for key in current_keys if key not in baseline_map]
    removed = [baseline_map[key] for key in baseline_keys if key not in current_map]
    shared_keys = [key for key in current_keys if key in baseline_map]

    metadata_changed = 0
    for key in shared_keys:
        if _image_metadata_payload(current_map[key]) != _image_metadata_payload(baseline_map[key]):
            metadata_changed += 1

    current_shared_order = [key for key in current_keys if key in baseline_map]
    baseline_shared_order = [key for key in baseline_keys if key in current_map]
    order_changed = current_shared_order != baseline_shared_order

    lines: list[str] = []
    if added:
        labels = ", ".join(_image_label(row) for row in added[:3])
        if len(added) > 3:
            labels += ", …"
        lines.append(f'Added {len(added)} image(s) since last sync: {labels}')
    if removed:
        labels = ", ".join(_image_label(row) for row in removed[:3])
        if len(removed) > 3:
            labels += ", …"
        lines.append(f'Removed {len(removed)} image(s) since last sync: {labels}')
    if metadata_changed:
        lines.append(f'Changed metadata on {metadata_changed} image(s) since last sync')
    if order_changed:
        lines.append('Image order changed since last sync')
    if not lines and len(current) != len(baseline):
        lines.append(f'Image count changed since last sync: {len(baseline)} -> {len(current)}')
    return lines


def _analyze_observation_field_changes(local_obs: dict | None, remote_obs: dict | None, baseline_obs: dict | None) -> dict:
    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    baseline_payload = _baseline_observation_compare_payload(baseline_obs)
    remote_only_fields: list[str] = []
    local_only_fields: list[str] = []
    conflict_fields: list[str] = []
    shared_same_fields: list[str] = []

    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        baseline_value = baseline_payload.get(field)
        local_value = local_payload.get(field)
        remote_value = remote_payload.get(field)
        local_changed = local_value != baseline_value
        remote_changed = remote_value != baseline_value
        if local_changed and remote_changed:
            if local_value == remote_value:
                shared_same_fields.append(field)
            else:
                conflict_fields.append(field)
        elif local_changed:
            local_only_fields.append(field)
        elif remote_changed:
            remote_only_fields.append(field)

    return {
        'local_payload': local_payload,
        'remote_payload': remote_payload,
        'baseline_payload': baseline_payload,
        'local_only_fields': local_only_fields,
        'remote_only_fields': remote_only_fields,
        'conflict_fields': conflict_fields,
        'shared_same_fields': shared_same_fields,
    }


def _analyze_image_changes(current_images: list[dict], baseline_images: list[dict]) -> dict:
    current = [dict(row or {}) for row in (current_images or [])]
    baseline = [dict(row or {}) for row in (baseline_images or [])]
    current_keys = [_image_compare_key(row) for row in current]
    baseline_keys = [_image_compare_key(row) for row in baseline]
    current_map = {_image_compare_key(row): row for row in current}
    baseline_map = {_image_compare_key(row): row for row in baseline}

    added_keys = [key for key in current_keys if key not in baseline_map]
    removed_keys = [key for key in baseline_keys if key not in current_map]
    shared_keys = [key for key in current_keys if key in baseline_map]
    metadata_changed_keys = [
        key
        for key in shared_keys
        if _image_metadata_payload(current_map[key]) != _image_metadata_payload(baseline_map[key])
    ]

    current_shared_order = [key for key in current_keys if key in baseline_map]
    baseline_shared_order = [key for key in baseline_keys if key in current_map]
    order_changed = current_shared_order != baseline_shared_order

    return {
        'added_keys': added_keys,
        'removed_keys': removed_keys,
        'metadata_changed_keys': metadata_changed_keys,
        'order_changed': order_changed,
        'added': [current_map[key] for key in added_keys],
        'removed': [baseline_map[key] for key in removed_keys],
        'changed': bool(added_keys or removed_keys or metadata_changed_keys or order_changed),
    }


def _remaining_local_changes_after_remote_merge(
    field_changes: dict,
    *,
    local_media_changed: bool,
) -> bool:
    return bool(field_changes.get('local_only_fields') or local_media_changed)


def _format_review_needed_error(local_id: int, cloud_id: str, reasons: list[str] | None = None) -> str:
    reason_text = ', '.join(str(reason or '').strip() for reason in (reasons or []) if str(reason or '').strip())
    base = (
        f"cloud {str(cloud_id or '').strip() or '?'}: needs review before applying remaining "
        f"cloud changes to local observation {int(local_id)}"
    )
    return f'{base} ({reason_text})' if reason_text else base


def _local_has_real_changes_since_snapshot(local_obs: dict, cloud_id: str | None = None) -> bool:
    cloud_value = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not cloud_value:
        return True
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_value))
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    local_payload = _observation_compare_payload(local_obs, local=True)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if local_payload.get(field) != baseline_obs.get(field):
            return True

    local_id = _safe_int(local_obs.get('id'))
    if local_id <= 0:
        return False
    stored_media_sig = _load_local_cloud_media_signature(local_id)
    if not stored_media_sig:
        return True
    current_media_sig = _local_cloud_media_signature(local_id)
    return bool(current_media_sig and current_media_sig != stored_media_sig)


def _clear_observation_dirty_if_no_real_changes(local_id: int, cloud_id: str) -> bool:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        return False
    if _local_has_real_changes_since_snapshot(local_obs, cloud_id):
        return False
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE observations
            SET sync_status = 'synced'
            WHERE id = ?
            """,
            (int(local_id),),
        )
        conn.commit()
    finally:
        conn.close()
    return True


def _progress_done(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('done', 0) or 0))
    except Exception:
        return 0


def _progress_total(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('total', 0) or 0))
    except Exception:
        return 0


def _emit_progress(
    progress_cb: ProgressCallback | None,
    message: str,
    progress_state: dict | None,
) -> None:
    if callable(progress_cb):
        progress_cb(message, _progress_done(progress_state), max(1, _progress_total(progress_state)))


def _advance_progress(
    progress_state: dict | None,
    amount: int = 1,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state) + increment
    state['total'] = _progress_total(state)
    return _progress_done(state), _progress_total(state)


def _extend_progress_total(
    progress_state: dict | None,
    amount: int,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state)
    state['total'] = _progress_total(state) + increment
    return _progress_done(state), _progress_total(state)


def _observation_display_name(obs: dict | None) -> str:
    record = obs or {}
    parts = [
        str(record.get('genus') or '').strip(),
        str(record.get('species') or '').strip(),
    ]
    name = " ".join(part for part in parts if part).strip()
    if name:
        return name
    species_guess = str(record.get('species_guess') or '').strip()
    if species_guess:
        return species_guess
    location = str(record.get('location') or '').strip()
    if location:
        return location
    obs_id = str(record.get('id') or '').strip()
    return f'observation {obs_id}' if obs_id else 'observation'


def _cloud_media_signature() -> str:
    snapshot = {
        'include_annotations': str(SettingsDB.get_setting(_SETTING_INCLUDE_ANNOTATIONS, '0') or '0').strip(),
        'show_scale_bar': str(SettingsDB.get_setting(_SETTING_SHOW_SCALE_BAR, '0') or '0').strip(),
        'include_measure_plots': str(SettingsDB.get_setting(_SETTING_INCLUDE_MEASURE_PLOTS, '0') or '0').strip(),
        'include_thumbnail_gallery': str(SettingsDB.get_setting(_SETTING_INCLUDE_THUMBNAIL_GALLERY, '0') or '0').strip(),
        'include_plate': str(SettingsDB.get_setting(_SETTING_INCLUDE_PLATE, '0') or '0').strip(),
        'include_copyright': str(SettingsDB.get_setting(_SETTING_INCLUDE_COPYRIGHT, '0') or '0').strip(),
        'image_license': str(SettingsDB.get_setting(_SETTING_IMAGE_LICENSE, '60') or '60').strip(),
        'profile_name': str(SettingsDB.get_setting(_SETTING_PROFILE_NAME, '') or '').strip(),
        'profile_email': str(SettingsDB.get_setting(_SETTING_PROFILE_EMAIL, '') or '').strip(),
    }
    return json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _cloud_observation_snapshot_key(cloud_id: str) -> str:
    return f"{_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX}{str(cloud_id or '').strip()}"


def _cloud_image_file_signature_key(observation_id: int | str, image_id: int | str) -> str:
    return (
        f"{_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX}"
        f"{str(observation_id or '').strip()}_{str(image_id or '').strip()}"
    )


def _cloud_local_media_signature_key(observation_id: int | str) -> str:
    return f"{_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX}{str(observation_id or '').strip()}"


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


def _is_generated_cloud_image(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    notes = str(row.get('notes') or '').strip().lower()
    filename = str(row.get('original_filename') or '').strip().lower()
    desktop_id = _safe_int(row.get('desktop_id'))
    if notes.startswith('generated media'):
        return True
    if filename.startswith('cloud_extra_'):
        return True
    if desktop_id < 0:
        return True
    return False


def should_push_local_image_to_cloud(image_row: dict | None) -> bool:
    return not _is_generated_cloud_image(image_row)


def should_pull_cloud_image_to_desktop(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if _is_generated_cloud_image(row):
        return False
    image_type = str(row.get('image_type') or '').strip().lower()
    if image_type == 'microscope':
        return False
    return True


def _cloud_observation_snapshot(remote: dict, remote_images: list[dict]) -> str:
    obs_part = {
        field: _normalize_snapshot_value((remote or {}).get(field))
        for field in _SNAPSHOT_OBS_FIELDS
    }
    images_part = []
    filtered_images = [
        dict(row or {})
        for row in (remote_images or [])
        if should_pull_cloud_image_to_desktop(row)
    ]
    for image in sorted(filtered_images, key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))):
        images_part.append(
            {
                field: _normalize_snapshot_value(image.get(field))
                for field in _SNAPSHOT_IMG_FIELDS
            }
        )
    payload = {'observation': obs_part, 'images': images_part}
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _load_cloud_observation_snapshot(cloud_id: str) -> str:
    raw = str(SettingsDB.get_setting(_cloud_observation_snapshot_key(cloud_id), '') or '').strip()
    if not raw:
        return ''
    parsed = _parse_cloud_observation_snapshot(raw)
    if not parsed:
        return raw
    return json.dumps(parsed, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _store_cloud_observation_snapshot(cloud_id: str, snapshot: str) -> None:
    if not str(cloud_id or '').strip():
        return
    normalized = str(snapshot or '').strip()
    if normalized:
        parsed = _parse_cloud_observation_snapshot(normalized)
        if parsed:
            normalized = json.dumps(parsed, ensure_ascii=True, sort_keys=True, separators=(',', ':'))
    SettingsDB.set_setting(_cloud_observation_snapshot_key(cloud_id), normalized)


def _load_cloud_image_file_signature(observation_id: int | str, image_id: int | str) -> str:
    return str(
        SettingsDB.get_setting(_cloud_image_file_signature_key(observation_id, image_id), '') or ''
    ).strip()


def _store_cloud_image_file_signature(
    observation_id: int | str,
    image_id: int | str,
    signature: str,
) -> None:
    SettingsDB.set_setting(
        _cloud_image_file_signature_key(observation_id, image_id),
        str(signature or '').strip(),
    )


def _clear_cloud_image_file_signature(observation_id: int | str, image_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_image_file_signature_key(observation_id, image_id), '')


def _load_local_cloud_media_signature(observation_id: int | str) -> str:
    return str(SettingsDB.get_setting(_cloud_local_media_signature_key(observation_id), '') or '').strip()


def _store_local_cloud_media_signature(observation_id: int | str, signature: str) -> None:
    SettingsDB.set_setting(
        _cloud_local_media_signature_key(observation_id),
        str(signature or '').strip(),
    )


def _clear_cloud_observation_snapshot(cloud_id: str) -> None:
    if not str(cloud_id or '').strip():
        return
    SettingsDB.set_setting(_cloud_observation_snapshot_key(cloud_id), '')


def _clear_local_cloud_media_signature(observation_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_local_media_signature_key(observation_id), '')


def _refresh_local_cloud_media_signature(observation_id: int | str) -> str:
    signature = _local_cloud_media_signature(observation_id)
    if str(signature or '').strip():
        _store_local_cloud_media_signature(observation_id, signature)
    return signature


def _detect_deleted_remote_observations(remote_obs: list[dict] | None) -> list[dict]:
    remote_ids = {
        str(row.get('id') or '').strip()
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT *
            FROM observations
            WHERE cloud_id IS NOT NULL
              AND TRIM(COALESCE(cloud_id, '')) != ''
            ORDER BY date DESC, id DESC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    deleted: list[dict] = []
    for local_obs in rows:
        cloud_id = str(local_obs.get('cloud_id') or '').strip()
        if not cloud_id or cloud_id in remote_ids:
            continue
        deleted.append(
            {
                'local_id': int(local_obs.get('id') or 0),
                'cloud_id': cloud_id,
                'title': _observation_display_name(local_obs),
                'date': local_obs.get('date'),
                'location': local_obs.get('location'),
                'sync_status': str(local_obs.get('sync_status') or '').strip().lower(),
                'observation': dict(local_obs),
            }
        )
    return deleted


def unlink_local_observation_from_cloud(local_id: int) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    image_rows = list(ImageDB.get_images_for_observation(int(local_id)) or [])

    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE observations
            SET cloud_id = NULL,
                sync_status = NULL,
                synced_at = NULL
            WHERE id = ?
            """,
            (int(local_id),),
        )
        conn.execute(
            """
            UPDATE images
            SET cloud_id = NULL,
                synced_at = NULL
            WHERE observation_id = ?
            """,
            (int(local_id),),
        )
        conn.commit()
    finally:
        conn.close()

    if cloud_id:
        _clear_cloud_observation_snapshot(cloud_id)
    _clear_local_cloud_media_signature(int(local_id))
    for image_row in image_rows:
        image_id = _safe_int(image_row.get('id'))
        cloud_image_id = str(image_row.get('cloud_id') or '').strip()
        if image_id > 0:
            _clear_cloud_image_file_signature(int(local_id), image_id)
        if cloud_image_id:
            _clear_cloud_image_file_signature(int(local_id), cloud_image_id)
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _file_content_signature(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return ''
    digest = hashlib.sha1()
    with open(file_path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


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


def _local_cloud_media_signature(observation_id: int | str) -> str:
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return ''
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                id,
                filepath,
                original_filepath,
                sort_order,
                image_type,
                micro_category,
                objective_name,
                scale_microns_per_pixel,
                resample_scale_factor,
                mount_medium,
                stain,
                sample_type,
                contrast,
                notes,
                gps_source
            FROM images
            WHERE observation_id = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                image_type,
                micro_category,
                created_at,
                id
            ''',
            (obs_id,),
        )
        image_rows = [dict(row) for row in cursor.fetchall()]
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

    excluded_raw = str(SettingsDB.get_setting(f"artsobs_publish_excluded_image_ids_{obs_id}", '[]') or '[]')
    gallery_settings_raw = str(SettingsDB.get_setting(f"gallery_settings_{obs_id}", '') or '').strip()
    payload = {
        'render_version': _CLOUD_LOCAL_MEDIA_RENDER_VERSION,
        'cloud_media_signature': _cloud_media_signature(),
        'cloud_image_size_mode': str(SettingsDB.get_setting(_SETTING_CLOUD_IMAGE_SIZE_MODE, 'reduced') or 'reduced').strip(),
        'excluded_image_ids_raw': excluded_raw,
        'gallery_settings_raw': gallery_settings_raw,
        'images': [
            {
                'id': _safe_int(row.get('id')),
                'filepath': _path_stat_signature(row.get('filepath')),
                'original_filepath': _path_stat_signature(row.get('original_filepath')),
                'sort_order': _normalize_snapshot_value(row.get('sort_order')),
                'image_type': _normalize_snapshot_value(row.get('image_type')),
                'micro_category': _normalize_snapshot_value(row.get('micro_category')),
                'objective_name': _normalize_snapshot_value(row.get('objective_name')),
                'scale_microns_per_pixel': _normalize_snapshot_value(row.get('scale_microns_per_pixel')),
                'resample_scale_factor': _normalize_snapshot_value(row.get('resample_scale_factor')),
                'mount_medium': _normalize_snapshot_value(row.get('mount_medium')),
                'stain': _normalize_snapshot_value(row.get('stain')),
                'sample_type': _normalize_snapshot_value(row.get('sample_type')),
                'contrast': _normalize_snapshot_value(row.get('contrast')),
                'notes': _normalize_snapshot_value(row.get('notes')),
                'gps_source': _normalize_snapshot_value(row.get('gps_source')),
            }
            for row in image_rows
        ],
        'measurements': [
            {
                'id': _safe_int(row.get('id')),
                'image_id': _safe_int(row.get('image_id')),
                'length_um': _normalize_snapshot_value(row.get('length_um')),
                'width_um': _normalize_snapshot_value(row.get('width_um')),
                'measurement_type': _normalize_snapshot_value(row.get('measurement_type')),
                'notes': _normalize_snapshot_value(row.get('notes')),
                'p1_x': _normalize_snapshot_value(row.get('p1_x')),
                'p1_y': _normalize_snapshot_value(row.get('p1_y')),
                'p2_x': _normalize_snapshot_value(row.get('p2_x')),
                'p2_y': _normalize_snapshot_value(row.get('p2_y')),
                'p3_x': _normalize_snapshot_value(row.get('p3_x')),
                'p3_y': _normalize_snapshot_value(row.get('p3_y')),
                'p4_x': _normalize_snapshot_value(row.get('p4_x')),
                'p4_y': _normalize_snapshot_value(row.get('p4_y')),
                'gallery_rotation': _normalize_snapshot_value(row.get('gallery_rotation')),
            }
            for row in measurement_rows
        ],
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


def _prepared_item_remote_payload(
    image_row: dict,
    upload_path: str,
    storage_path: str,
) -> dict:
    payload = {
        'desktop_id': _safe_int(image_row.get('id')),
        'sort_order': _normalize_snapshot_value(image_row.get('sort_order')),
        'image_type': _normalize_snapshot_value(image_row.get('image_type')),
        'micro_category': _normalize_snapshot_value(image_row.get('micro_category')),
        'objective_name': _normalize_snapshot_value(image_row.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image_row.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image_row.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image_row.get('mount_medium')),
        'stain': _normalize_snapshot_value(image_row.get('stain')),
        'sample_type': _normalize_snapshot_value(image_row.get('sample_type')),
        'contrast': _normalize_snapshot_value(image_row.get('contrast')),
        'notes': _normalize_snapshot_value(image_row.get('notes')),
        'gps_source': _normalize_snapshot_value(
            None if image_row.get('gps_source') is None else bool(image_row.get('gps_source'))
        ),
        'storage_path': _normalize_snapshot_value(str(storage_path or '').strip() or None),
        'original_filename': _normalize_snapshot_value(Path(str(upload_path or '').strip()).name or None),
    }
    return payload


def _remote_image_payload(remote_image: dict | None) -> dict:
    image = remote_image or {}
    return {
        'desktop_id': _safe_int(image.get('desktop_id')),
        'sort_order': _normalize_snapshot_value(image.get('sort_order')),
        'image_type': _normalize_snapshot_value(image.get('image_type')),
        'micro_category': _normalize_snapshot_value(image.get('micro_category')),
        'objective_name': _normalize_snapshot_value(image.get('objective_name')),
        'scale_microns_per_pixel': _normalize_snapshot_value(image.get('scale_microns_per_pixel')),
        'resample_scale_factor': _normalize_snapshot_value(image.get('resample_scale_factor')),
        'mount_medium': _normalize_snapshot_value(image.get('mount_medium')),
        'stain': _normalize_snapshot_value(image.get('stain')),
        'sample_type': _normalize_snapshot_value(image.get('sample_type')),
        'contrast': _normalize_snapshot_value(image.get('contrast')),
        'notes': _normalize_snapshot_value(image.get('notes')),
        'gps_source': _normalize_snapshot_value(image.get('gps_source')),
        'storage_path': _normalize_snapshot_value(image.get('storage_path')),
        'original_filename': _normalize_snapshot_value(image.get('original_filename')),
    }


def _mark_cloud_observations_dirty_for_media_changes() -> None:
    current_signature = _cloud_media_signature()
    previous_signature = str(SettingsDB.get_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, '') or '').strip()
    if previous_signature == current_signature:
        return
    # Background cloud sync currently uploads source images/metadata, not the
    # optional rendered overlays/gallery/plate outputs tied to these settings.
    # Persist the new signature so future comparisons are stable, but don't mark
    # every linked observation dirty just because a global render preference changed.
    SettingsDB.set_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, current_signature)


def _has_pending_local_push_work() -> bool:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT 1 FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty' LIMIT 1"
        ).fetchone()
        return bool(row)
    finally:
        conn.close()


def _find_local_observation_for_remote(remote: dict) -> dict | None:
    cloud_id = str((remote or {}).get('id') or '').strip()
    desktop_id = (remote or {}).get('desktop_id')
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        if cloud_id:
            cursor.execute('SELECT * FROM observations WHERE cloud_id = ? LIMIT 1', (cloud_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        try:
            local_id = int(desktop_id)
        except (TypeError, ValueError):
            local_id = 0
        if local_id > 0:
            cursor.execute('SELECT * FROM observations WHERE id = ? LIMIT 1', (local_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return None
    finally:
        conn.close()


def _stamp_observation_synced(local_id: int, cloud_id: str) -> None:
    _set_observation_sync_state(int(local_id), str(cloud_id or '').strip(), dirty=False)


def _set_observation_sync_state(local_id: int, cloud_id: str, *, dirty: bool) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE observations SET cloud_id = ?, sync_status = ?, synced_at = ? WHERE id = ?",
            (
                str(cloud_id or '').strip() or None,
                'dirty' if dirty else 'synced',
                datetime.now(timezone.utc).isoformat(),
                int(local_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _remote_observation_update_kwargs(remote: dict) -> dict:
    raw_location_public = remote.get('location_public')
    location_public = None if raw_location_public is None else bool(raw_location_public)
    return {
        'date': remote.get('date'),
        'genus': remote.get('genus'),
        'species': remote.get('species'),
        'common_name': remote.get('common_name'),
        'species_guess': remote.get('species_guess'),
        'location': remote.get('location'),
        'habitat': remote.get('habitat'),
        'notes': remote.get('notes'),
        'open_comment': remote.get('open_comment'),
        'sharing_scope': _normalize_sharing_scope(
            remote.get('visibility') or remote.get('sharing_scope'),
            fallback='friends' if location_public else 'private',
        ),
        'location_public': location_public,
        'spore_data_visibility': (lambda v: v if v in {'private', 'friends', 'public'} else 'public')(
            str(remote.get('spore_data_visibility') or 'public').strip().lower()
        ),
        'uncertain': bool(remote.get('uncertain', False)),
        'unspontaneous': bool(remote.get('unspontaneous', False)),
        'gps_latitude': remote.get('gps_latitude'),
        'gps_longitude': remote.get('gps_longitude'),
        'artsdata_id': remote.get('artsdata_id'),
        'artportalen_id': remote.get('artportalen_id'),
        'publish_target': remote.get('publish_target'),
        'determination_method': remote.get('determination_method'),
        'habitat_nin2_path': remote.get('habitat_nin2_path'),
        'habitat_substrate_path': remote.get('habitat_substrate_path'),
        'habitat_host_genus': remote.get('habitat_host_genus'),
        'habitat_host_species': remote.get('habitat_host_species'),
        'habitat_host_common_name': remote.get('habitat_host_common_name'),
        'habitat_nin2_note': remote.get('habitat_nin2_note'),
        'habitat_substrate_note': remote.get('habitat_substrate_note'),
        'habitat_grows_on_note': remote.get('habitat_grows_on_note'),
        'allow_nulls': True,
    }


def _remote_observation_extra_values(remote: dict) -> dict:
    return {
        'inaturalist_id': remote.get('inaturalist_id'),
        'mushroomobserver_id': remote.get('mushroomobserver_id'),
        'source_type': remote.get('source_type'),
        'citation': remote.get('citation'),
        'data_provider': remote.get('data_provider'),
        'author': remote.get('author'),
        'spore_statistics': remote.get('spore_statistics'),
        'auto_threshold': remote.get('auto_threshold'),
    }


def _apply_remote_observation_fields(
    local_id: int,
    remote: dict,
    *,
    fields: set[str] | None = None,
) -> None:
    requested_fields = {
        str(field or '').strip()
        for field in (fields or set(_SNAPSHOT_OBS_FIELDS))
        if str(field or '').strip()
    }
    if not requested_fields:
        return

    normalized_fields = {
        'sharing_scope' if field in {'visibility', 'sharing_scope'} else field
        for field in requested_fields
    }
    update_kwargs = _remote_observation_update_kwargs(remote)
    partial_kwargs = {
        key: value
        for key, value in update_kwargs.items()
        if key == 'allow_nulls' or key in normalized_fields
    }
    if len(partial_kwargs) > 1:
        ObservationDB.update_observation(int(local_id), **partial_kwargs)

    extra_values = _remote_observation_extra_values(remote)
    extra_updates = {
        key: value
        for key, value in extra_values.items()
        if key in normalized_fields
    }
    if not extra_updates:
        return

    conn = get_connection()
    try:
        assignments = [f'{column} = ?' for column in extra_updates]
        values = list(extra_updates.values())
        values.append(int(local_id))
        conn.execute(
            f"UPDATE observations SET {', '.join(assignments)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
    finally:
        conn.close()


def _sync_existing_remote_image_to_local(
    client: "SporelyCloudClient",
    local_image: dict,
    remote_image: dict,
) -> None:
    image_id = int(local_image.get('id'))
    existing_path = str(local_image.get('filepath') or '').strip()
    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_image_{image_id}_'))
    try:
        filename = Path(str(remote_image.get('original_filename') or '')).name or f'cloud_{image_id}.jpg'
        temp_path = temp_dir / filename
        client.download_image_file(str(remote_image.get('storage_path') or ''), temp_path)
        target_path = Path(existing_path) if existing_path else temp_path
        if existing_path:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(temp_path, target_path)
        ImageDB.update_image(
            image_id,
            filepath=str(target_path),
            image_type=str(remote_image.get('image_type') or 'field'),
            scale=remote_image.get('scale_microns_per_pixel'),
            notes=remote_image.get('notes'),
            micro_category=remote_image.get('micro_category'),
            objective_name=remote_image.get('objective_name'),
            mount_medium=remote_image.get('mount_medium'),
            stain=remote_image.get('stain'),
            sample_type=remote_image.get('sample_type'),
            contrast=remote_image.get('contrast'),
            sort_order=remote_image.get('sort_order'),
            gps_source=remote_image.get('gps_source'),
            resample_scale_factor=remote_image.get('resample_scale_factor'),
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
            generate_all_sizes(str(target_path), image_id)
        except Exception:
            pass
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _apply_remote_images_to_local(
    client: "SporelyCloudClient",
    local_id: int,
    remote_images: list[dict],
    *,
    allow_delete: bool = True,
) -> list[str]:
    warnings: list[str] = []
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

    for cloud_image_id, remote_image in remote_map.items():
        local_image = local_cloud_map.get(cloud_image_id)
        if local_image:
            _sync_existing_remote_image_to_local(client, local_image, remote_image)
            try:
                client.set_image_desktop_id(cloud_image_id, int(local_image.get('id')))
            except Exception:
                pass
            continue

        storage_path = str(remote_image.get('storage_path') or '').strip()
        if not storage_path:
            continue
        temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
        try:
            filename = Path(str(remote_image.get('original_filename') or '')).name or f'{cloud_image_id}.jpg'
            download_path = temp_dir / filename
            client.download_image_file(storage_path, download_path)
            local_image_id = ImageDB.add_image(
                observation_id=int(local_id),
                filepath=str(download_path),
                image_type=str(remote_image.get('image_type') or 'field'),
                scale=remote_image.get('scale_microns_per_pixel'),
                notes=remote_image.get('notes'),
                micro_category=remote_image.get('micro_category'),
                objective_name=remote_image.get('objective_name'),
                mount_medium=remote_image.get('mount_medium'),
                stain=remote_image.get('stain'),
                sample_type=remote_image.get('sample_type'),
                contrast=remote_image.get('contrast'),
                sort_order=remote_image.get('sort_order'),
                gps_source=remote_image.get('gps_source'),
                resample_scale_factor=remote_image.get('resample_scale_factor'),
                copy_to_folder=True,
                mark_observation_dirty=False,
            )
            conn = get_connection()
            try:
                conn.execute(
                    'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                    (cloud_image_id, datetime.now(timezone.utc).isoformat(), int(local_image_id)),
                )
                conn.commit()
            finally:
                conn.close()
            try:
                client.set_image_desktop_id(cloud_image_id, int(local_image_id))
            except Exception:
                pass
            try:
                generate_all_sizes(str(download_path), int(local_image_id))
            except Exception:
                pass
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

    return warnings


def _store_remote_snapshot(client: "SporelyCloudClient", cloud_id: str, remote: dict | None = None, remote_images: list[dict] | None = None) -> None:
    cloud_value = str(cloud_id or '').strip()
    if not cloud_value:
        return
    remote_obs = remote or client.get_observation(cloud_value)
    if not remote_obs:
        return
    images = list(remote_images or client.pull_image_metadata(cloud_value) or [])
    _store_cloud_observation_snapshot(cloud_value, _cloud_observation_snapshot(remote_obs, images))


def get_conflict_detail(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    remote_obs = client.get_observation(resolved_cloud_id)
    if not remote_obs:
        raise CloudSyncError(f'Cloud observation {resolved_cloud_id} not found')
    remote_images = [
        dict(row or {})
        for row in (client.pull_image_metadata(resolved_cloud_id) or [])
        if should_pull_cloud_image_to_desktop(row)
    ]

    snapshot_raw = _load_cloud_observation_snapshot(resolved_cloud_id)
    snapshot = _parse_cloud_observation_snapshot(snapshot_raw)
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]

    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    field_rows = []
    for field in _CONFLICT_COMPARE_FIELDS:
        baseline_value = baseline_obs.get(field)
        local_value = local_payload.get(field)
        remote_value = remote_payload.get(field)
        if local_value == baseline_value and remote_value == baseline_value:
            continue
        if local_value == remote_value:
            continue
        field_rows.append(
            {
                'field': field,
                'label': _CONFLICT_FIELD_LABELS.get(field, field.replace('_', ' ').title()),
                'baseline': baseline_value,
                'local': local_value,
                'remote': remote_value,
                'local_changed': local_value != baseline_value,
                'remote_changed': remote_value != baseline_value,
            }
        )

    local_images = [
        _local_image_snapshot_payload(img)
        for img in ImageDB.get_images_for_observation(int(local_id))
        if should_pull_cloud_image_to_desktop(img)
    ]
    remote_image_payloads = [_remote_image_payload(img) for img in (remote_images or [])]
    local_image_changes = _summarize_image_changes(local_images, baseline_images)
    remote_image_changes = _summarize_image_changes(remote_image_payloads, baseline_images)

    stored_local_media_signature = _load_local_cloud_media_signature(local_id)
    current_local_media_signature = _local_cloud_media_signature(local_id)
    local_media_changed = bool(
        stored_local_media_signature
        and current_local_media_signature
        and stored_local_media_signature != current_local_media_signature
    )
    local_measurement_count = len(MeasurementDB.get_measurements_for_observation(int(local_id)))
    if local_media_changed:
        local_image_changes.append(
            f'Desktop media details changed since last sync ({local_measurement_count} measurement(s), publish settings, or gallery layout)'
        )

    return {
        'local_id': int(local_id),
        'cloud_id': resolved_cloud_id,
        'title': _observation_display_name(local_obs) or _observation_display_name(remote_obs),
        'local_observation': local_obs,
        'remote_observation': remote_obs,
        'baseline_observation': baseline_obs,
        'field_rows': field_rows,
        'local_image_changes': local_image_changes,
        'remote_image_changes': remote_image_changes,
        'last_synced_at': local_obs.get('synced_at'),
        'local_updated_at': local_obs.get('updated_at') or local_obs.get('created_at'),
        'remote_updated_at': remote_obs.get('updated_at') or remote_obs.get('created_at'),
        'local_measurement_count': local_measurement_count,
        'remote_image_count': len(remote_images),
        'local_image_count': len(local_images),
    }


def resolve_conflict_keep_local(
    client: "SporelyCloudClient",
    local_id: int,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    cloud_id = client.push_observation(local_obs)
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE observations SET cloud_id = ?, sync_status = 'synced', synced_at = ? WHERE id = ?",
            (cloud_id, datetime.now(timezone.utc).isoformat(), int(local_id)),
        )
        conn.commit()
    finally:
        conn.close()

    if prepare_images_cb is not None:
        images_ok = _push_images_for_observation(
            client,
            local_obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            progress_cb=progress_cb,
            progress_state={'done': 0, 'total': 0},
            observation_index=1,
            observation_total=1,
        )
        if not images_ok:
            mark_observation_dirty(int(local_id))
            raise CloudSyncError(f'Could not fully upload images for observation {local_id}')

    _store_remote_snapshot(client, cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def resolve_conflict_keep_cloud(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    remote_obs = client.get_observation(resolved_cloud_id)
    if not remote_obs:
        raise CloudSyncError(f'Cloud observation {resolved_cloud_id} not found')
    remote_images = [
        dict(row or {})
        for row in (client.pull_image_metadata(resolved_cloud_id) or [])
        if should_pull_cloud_image_to_desktop(row)
    ]

    _apply_remote_observation_fields(int(local_id), remote_obs)
    warnings = _apply_remote_images_to_local(client, int(local_id), remote_images)
    _stamp_observation_synced(int(local_id), resolved_cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    _store_cloud_observation_snapshot(
        resolved_cloud_id,
        _cloud_observation_snapshot(remote_obs, remote_images),
    )
    return {'local_id': int(local_id), 'cloud_id': resolved_cloud_id, 'warnings': warnings}


def _get_keyring_module():
    try:
        import keyring  # type: ignore

        return keyring
    except Exception:
        return None


def load_saved_cloud_password() -> tuple[str, str | None, bool]:
    settings = get_app_settings()
    email = str(settings.get('cloud_user_email') or '').strip()
    keyring = _get_keyring_module()
    if keyring is None:
        return email, None, False
    try:
        password = keyring.get_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
        if password is None and not using_isolated_profile():
            password = keyring.get_password(_CLOUD_LEGACY_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
    except Exception:
        return email, None, False
    return email, password, True


def save_cloud_password(email: str, password: str) -> None:
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure password storage is unavailable on this system.")
    try:
        keyring.set_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT, password)
    except Exception as exc:
        raise RuntimeError(f"Could not securely save password: {exc}") from exc
    update_app_settings({'cloud_user_email': str(email or '').strip()})


def clear_saved_cloud_password() -> None:
    keyring = _get_keyring_module()
    if keyring is None:
        return
    for service_name in (_CLOUD_KEYRING_SERVICE, _CLOUD_LEGACY_KEYRING_SERVICE):
        try:
            keyring.delete_password(service_name, _CLOUD_KEYRING_ACCOUNT)
        except Exception:
            continue


def has_saved_cloud_password() -> bool:
    email, password, _ = load_saved_cloud_password()
    return bool(email and password)


class SporelyCloudClient:
    """Thin wrapper around Supabase REST API."""

    def __init__(self, access_token: str, user_id: str, refresh_token: str | None = None):
        self.access_token = access_token
        self.user_id = user_id
        self.refresh_token = str(refresh_token or '').strip() or None
        self._s = requests.Session()
        self._s.headers.update({
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        })

    # ── Auth ────────────────────────────────────────────────────────────

    @classmethod
    def login(cls, email: str, password: str) -> 'SporelyCloudClient':
        resp = requests.post(
            f'{SUPABASE_URL}/auth/v1/token?grant_type=password',
            json={'email': email, 'password': password},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=15,
        )
        if not resp.ok:
            raise CloudSyncError(f'Login failed: {resp.text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token'),
        )

    @classmethod
    def refresh_login(cls, refresh_token: str) -> 'SporelyCloudClient':
        token = str(refresh_token or '').strip()
        if not token:
            raise CloudSyncError('Missing refresh token')
        resp = requests.post(
            f'{SUPABASE_URL}/auth/v1/token?grant_type=refresh_token',
            json={'refresh_token': token},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=15,
        )
        if not resp.ok:
            raise CloudSyncError(f'Refresh failed: {resp.text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token') or token,
        )

    @classmethod
    def from_stored_credentials(cls) -> 'SporelyCloudClient | None':
        settings = get_app_settings()
        token = settings.get('cloud_access_token')
        user_id = settings.get('cloud_user_id')
        refresh_token = settings.get('cloud_refresh_token')
        if token and user_id:
            client = cls(access_token=token, user_id=user_id, refresh_token=refresh_token)
            try:
                client._get('observations?limit=1&select=id')
                return client
            except CloudSyncError:
                pass
        if refresh_token:
            try:
                client = cls.refresh_login(str(refresh_token))
                client.save_credentials()
                return client
            except CloudSyncError:
                pass
        email, password, _ = load_saved_cloud_password()
        if email and password:
            try:
                client = cls.login(email, password)
                client.save_credentials(email=email)
                return client
            except CloudSyncError:
                return None
        return None

    def save_credentials(
        self,
        email: str | None = None,
        password: str | None = None,
        remember_password: bool | None = None,
    ) -> None:
        updates = {
            'cloud_access_token': self.access_token,
            'cloud_user_id': self.user_id,
            'cloud_refresh_token': self.refresh_token,
        }
        if email is not None:
            updates['cloud_user_email'] = str(email or '').strip()
        update_app_settings(updates)
        # Only change the saved password when the caller explicitly asked us to.
        # Auto-login/refresh paths should preserve whatever the user chose earlier.
        if remember_password is True and email and password:
            save_cloud_password(str(email or '').strip(), password)
        elif remember_password is False:
            clear_saved_cloud_password()

    @staticmethod
    def clear_credentials() -> None:
        clear_saved_cloud_password()
        update_app_settings({
            'cloud_access_token': None,
            'cloud_user_id': None,
            'cloud_refresh_token': None,
            'cloud_user_email': None,
        })

    # ── REST helpers ─────────────────────────────────────────────────────

    def _get(self, path: str) -> list:
        resp = self._s.get(f'{SUPABASE_URL}/rest/v1/{path}', timeout=20)
        if not resp.ok:
            raise CloudSyncError(f'GET {path}: {resp.text}')
        return resp.json()

    def _post(self, path: str, payload: dict) -> list:
        resp = self._s.post(
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=representation'},
            timeout=20,
        )
        if not resp.ok:
            raise CloudSyncError(f'POST {path}: {resp.text}')
        return resp.json()

    def _rpc(self, function_name: str, payload: dict | None = None):
        rpc_name = str(function_name or '').strip()
        if not rpc_name:
            raise CloudSyncError('Missing RPC function name')
        resp = self._s.post(
            f'{SUPABASE_URL}/rest/v1/rpc/{rpc_name}',
            json=dict(payload or {}),
            timeout=20,
        )
        if not resp.ok:
            raise CloudSyncError(f'RPC {rpc_name}: {resp.text}')
        if not resp.content:
            return None
        return resp.json()

    def _patch(self, path: str, payload: dict) -> None:
        resp = self._s.patch(
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=minimal'},
            timeout=20,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')

    def _delete(self, path: str) -> None:
        resp = self._s.delete(
            f'{SUPABASE_URL}/rest/v1/{path}',
            headers={'Prefer': 'return=minimal'},
            timeout=20,
        )
        if not resp.ok:
            raise CloudSyncError(f'DELETE {path}: {resp.text}')

    def _storage_remove(self, storage_paths: list[str]) -> None:
        cleaned = []
        for path in (storage_paths or []):
            path_str = str(path or '').strip().lstrip('/')
            if not path_str:
                continue
            cleaned.append(path_str)
            
            parts = path_str.split('/')
            file_name = parts[-1]
            dir_path = '/'.join(parts[:-1])
            for variant in ('small', 'medium'):
                if dir_path:
                    cleaned.append(f"{dir_path}/thumb_{variant}_{file_name}")
                else:
                    cleaned.append(f"thumb_{variant}_{file_name}")
            
        if not cleaned:
            return
        resp = self._s.delete(
            f'{SUPABASE_URL}/storage/v1/object/observation-images',
            json={'prefixes': cleaned},
            timeout=60,
        )
        if not resp.ok:
            raise CloudSyncError(f'Storage delete failed: {resp.text}')

    # ── Observation push ─────────────────────────────────────────────────

    def _find_cloud_observation(self, desktop_id: int) -> str | None:
        rows = self._get(
            f'observations?desktop_id=eq.{desktop_id}&user_id=eq.{self.user_id}&select=id'
        )
        return rows[0]['id'] if rows else None

    def get_observation(self, cloud_id: str) -> dict | None:
        cloud_value = str(cloud_id or '').strip()
        if not cloud_value:
            return None
        rows = self._get(
            f'observations?id=eq.{cloud_value}&user_id=eq.{self.user_id}&select=*'
        )
        return rows[0] if rows else None

    def list_remote_observations(self) -> list[dict]:
        return self._get(
            f'observations?user_id=eq.{self.user_id}&order=created_at.asc&select=*'
        )

    def push_observation(self, obs: dict) -> str:
        """Upsert observation to cloud. Returns cloud UUID."""
        payload = {col: obs.get(col) for col in _OBS_PUSH_COLS}
        payload['user_id']    = self.user_id
        payload['desktop_id'] = obs['id']
        payload['visibility'] = _normalize_sharing_scope(obs.get('sharing_scope'), fallback='private')
        raw_vis = str(payload.get('spore_data_visibility') or 'public').strip().lower()
        payload['spore_data_visibility'] = raw_vis if raw_vis in {'private', 'friends', 'public'} else 'public'

        # Normalise SQLite 0/1 integers to proper JSON booleans
        for col in ('uncertain', 'unspontaneous', 'interesting_comment', 'location_public'):
            if payload.get(col) is not None:
                payload[col] = bool(payload[col])

        # spore_statistics is stored as JSON text in SQLite; send as object
        if isinstance(payload.get('spore_statistics'), str):
            try:
                payload['spore_statistics'] = json.loads(payload['spore_statistics'])
            except (json.JSONDecodeError, TypeError):
                pass

        existing_id = self._find_cloud_observation(obs['id'])
        if existing_id:
            self._patch(f'observations?id=eq.{existing_id}', payload)
            return existing_id
        else:
            rows = self._post('observations', payload)
            return rows[0]['id']

    # ── Image push ───────────────────────────────────────────────────────

    def _find_cloud_image(self, desktop_id: int) -> str | None:
        rows = self._get(
            f'observation_images?desktop_id=eq.{desktop_id}&user_id=eq.{self.user_id}&select=id'
        )
        return rows[0]['id'] if rows else None

    def _build_storage_path(self, obs_cloud_id: str, img_cloud_id: str, local_path: str) -> str:
        import urllib.parse
        path = Path(local_path)
        safe_name = urllib.parse.quote(path.name)
        return f'{self.user_id}/{obs_cloud_id}/{img_cloud_id}_{safe_name}'

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        """Upsert image metadata row. Returns cloud UUID."""
        payload = {col: img.get(col) for col in _IMG_PUSH_COLS}
        payload['observation_id']    = obs_cloud_id
        payload['user_id']           = self.user_id
        payload['desktop_id']        = img['id']
        payload['original_filename'] = Path(img.get('filepath') or '').name or None
        payload['storage_path']      = str(storage_path or '').strip()
        if payload.get('gps_source') is not None:
            payload['gps_source'] = bool(payload['gps_source'])

        existing_id = self._find_cloud_image(img['id'])
        if existing_id:
            self._patch(f'observation_images?id=eq.{existing_id}', payload)
            return existing_id
        else:
            rows = self._post('observation_images', payload)
            return rows[0]['id']

    def upload_image_file(self, local_path: str, obs_cloud_id: str, img_cloud_id: str) -> str | None:
        """Upload file to Supabase Storage. Returns storage path or None if file missing."""
        path = Path(local_path)
        if not path.exists():
            return None

        storage_path = self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)
        mime = mimetypes.guess_type(path.name)[0] or 'image/jpeg'

        with open(path, 'rb') as f:
            resp = requests.post(
                f'{SUPABASE_URL}/storage/v1/object/observation-images/{storage_path}',
                data=f,
                headers={
                    'apikey': SUPABASE_KEY,
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': mime,
                    'x-upsert': 'true',
                },
                timeout=120,
            )
        if not resp.ok:
            raise CloudSyncError(f'Storage upload failed: {resp.text}')

        # Generate and upload thumbnail variants to match web app behavior
        variants = [
            ('small', 240, 74),
            ('medium', 720, 82),
        ]
        try:
            with Image.open(path) as img:
                if img.mode in ('RGBA', 'LA'):
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        background.paste(img, mask=img.split()[3])
                    else:
                        background.paste(img, mask=img.split()[1])
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                    
                orig_w, orig_h = img.size
                
                parts = storage_path.split('/')
                file_name = parts[-1]
                dir_path = '/'.join(parts[:-1])

                for variant, max_edge, quality in variants:
                    scale = min(1.0, max_edge / max(orig_w, orig_h))
                    target_w = max(1, int(orig_w * scale))
                    target_h = max(1, int(orig_h * scale))
                    
                    variant_path = f"{dir_path}/thumb_{variant}_{file_name}" if dir_path else f"thumb_{variant}_{file_name}"
                    
                    img_resized = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
                    buffer = io.BytesIO()
                    img_resized.save(buffer, format='JPEG', quality=quality)
                    buffer.seek(0)
                    
                    v_resp = requests.post(
                        f'{SUPABASE_URL}/storage/v1/object/observation-images/{variant_path}',
                        data=buffer,
                        headers={
                            'apikey': SUPABASE_KEY,
                            'Authorization': f'Bearer {self.access_token}',
                            'Content-Type': 'image/jpeg',
                            'x-upsert': 'true',
                        },
                        timeout=60,
                    )
                    if not v_resp.ok:
                        print(f"[cloud_sync] Warning: Thumbnail variant {variant} upload failed: {v_resp.text}")
        except Exception as e:
            print(f"[cloud_sync] Warning: Could not generate/upload thumbnail variants for {storage_path}: {e}")

        return storage_path

    # ── Pull new web observations ─────────────────────────────────────────

    def pull_web_observations(self, after_iso: str | None = None) -> list[dict]:
        """Fetch observations created on mobile/web (desktop_id IS NULL)."""
        qs = f'observations?desktop_id=is.null&user_id=eq.{self.user_id}&order=created_at.asc&select=*'
        if after_iso:
            qs += f'&created_at=gt.{_encode_postgrest_filter_value(after_iso)}'
        return self._get(qs)

    def set_desktop_id(self, cloud_id: str, desktop_id: int) -> None:
        """Write the local SQLite ID back to the cloud row for future dedup."""
        self._patch(f'observations?id=eq.{cloud_id}', {'desktop_id': desktop_id})

    def pull_image_metadata(self, obs_cloud_id: str) -> list[dict]:
        return self._get(
            f'observation_images?observation_id=eq.{obs_cloud_id}&select=*'
        )

    def pull_bulk_image_metadata(self, obs_cloud_ids: list[str]) -> list[dict]:
        if not obs_cloud_ids:
            return []
        all_images = []
        for i in range(0, len(obs_cloud_ids), 50):
            chunk = obs_cloud_ids[i:i+50]
            ids_str = ','.join(chunk)
            rows = self._get(f'observation_images?observation_id=in.({ids_str})&select=*')
            all_images.extend(rows)
        return all_images

    def search_community_spore_datasets(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_community_spore_datasets', payload)
        return rows if isinstance(rows, list) else []

    def get_community_spore_dataset(self, observation_id: int) -> dict | None:
        rows = self._rpc(
            'get_community_spore_dataset',
            {'p_observation_id': int(observation_id)},
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def community_spore_taxon_summary(self, genus: str, species: str) -> dict | None:
        rows = self._rpc(
            'community_spore_taxon_summary',
            {
                'p_genus': str(genus or '').strip(),
                'p_species': str(species or '').strip(),
            },
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def search_public_reference_values(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_public_reference_values', payload)
        return rows if isinstance(rows, list) else []

    def set_image_desktop_id(self, cloud_image_id: str, desktop_id: int) -> None:
        """Write the local SQLite image ID back to the cloud image row."""
        self._patch(f'observation_images?id=eq.{cloud_image_id}', {'desktop_id': desktop_id})

    def push_measurement(self, meas: dict, cloud_image_id: str) -> str:
        """Upsert one spore measurement row. Returns cloud UUID."""
        payload = {col: meas.get(col) for col in _MEAS_PUSH_COLS}
        payload['image_id'] = cloud_image_id
        payload['user_id'] = self.user_id
        payload['desktop_id'] = int(meas['id'])
        rows = self._get(
            f'spore_measurements?desktop_id=eq.{payload["desktop_id"]}&user_id=eq.{self.user_id}&select=id'
        )
        if rows:
            existing_id = rows[0]['id']
            self._patch(f'spore_measurements?id=eq.{existing_id}', payload)
            return existing_id
        else:
            rows = self._post('spore_measurements', payload)
            return rows[0]['id']

    def delete_cloud_measurements_for_image(self, cloud_image_id: str) -> None:
        """Delete all cloud spore_measurements rows for one image."""
        self._delete(f'spore_measurements?image_id=eq.{cloud_image_id}&user_id=eq.{self.user_id}')

    def delete_cloud_observation(self, obs_cloud_id: str) -> None:
        """Delete one cloud observation and its associated cloud image rows/files."""
        cloud_id = str(obs_cloud_id or '').strip()
        if not cloud_id:
            raise CloudSyncError('Missing cloud observation id')
        image_rows = self.pull_image_metadata(cloud_id) or []
        storage_paths = [
            str(row.get('storage_path') or '').strip()
            for row in image_rows
            if str(row.get('storage_path') or '').strip()
        ]
        if storage_paths:
            try:
                self._storage_remove(storage_paths)
            except CloudSyncError as exc:
                print(f'[cloud_sync] Warning: could not remove storage files for {cloud_id}: {exc}')
        self._delete(f'observation_images?observation_id=eq.{cloud_id}')
        self._delete(f'observations?id=eq.{cloud_id}')

    def download_image_file(self, storage_path: str, dest_path: str | Path) -> Path:
        """Download one cloud image from Supabase Storage into a local path."""
        storage_key = str(storage_path or '').strip().lstrip('/')
        if not storage_key:
            raise CloudSyncError('Missing storage path')
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        encoded_key = requests.utils.quote(storage_key, safe="/")
        resp = self._s.get(
            f'{SUPABASE_URL}/storage/v1/object/authenticated/observation-images/{encoded_key}',
            timeout=120,
            stream=True,
        )
        if not resp.ok:
            raise CloudSyncError(f'Download failed: {resp.text}')
        with open(destination, 'wb') as handle:
            shutil.copyfileobj(resp.raw, handle)
        return destination


# ── High-level sync entry points ──────────────────────────────────────────────

def push_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
) -> dict:
    """Push all unsynced / dirty observations (and optionally images) to cloud.

    Returns a summary dict with counts.
    """
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()

    _mark_cloud_observations_dirty_for_media_changes()

    cursor.execute(
        "SELECT * FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty' ORDER BY date DESC"
    )
    observations = [dict(r) for r in cursor.fetchall()]
    conn.close()

    total = len(observations)
    pushed = 0
    errors = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    progress_state['done'] = _progress_done(progress_state)
    progress_state['total'] = _progress_total(progress_state) + total
    remote_lookup = {
        str(row.get('id') or '').strip(): row
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }

    for i, obs in enumerate(observations):
        name = _observation_display_name(obs)
        _emit_progress(
            progress_cb,
            f"Syncing observation {i + 1}/{max(1, total)}: {name}…",
            progress_state,
        )
        try:
            cloud_id = str(obs.get('cloud_id') or '').strip()
            had_existing_cloud = bool(cloud_id)
            stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
            remote = remote_lookup.get(cloud_id) if cloud_id else None
            if cloud_id and remote is None:
                _advance_progress(progress_state, 1)
                _emit_progress(
                    progress_cb,
                    f"Cloud copy was deleted for observation {i + 1}/{max(1, total)}: {name}",
                    progress_state,
                )
                continue
            if cloud_id and stored_snapshot and remote:
                remote_images = client.pull_image_metadata(cloud_id) or []
                remote_snapshot = _cloud_observation_snapshot(remote, remote_images)
                if remote_snapshot != stored_snapshot:
                    if _clear_observation_dirty_if_no_real_changes(int(obs['id']), cloud_id):
                        _advance_progress(progress_state, 1)
                        _emit_progress(
                            progress_cb,
                            f"Skipped stale local change for observation {i + 1}/{max(1, total)}: {name}",
                            progress_state,
                        )
                        continue
                    _advance_progress(progress_state, 1)
                    _emit_progress(
                        progress_cb,
                        f"Deferred push for observation {i + 1}/{max(1, total)} until cloud changes are reviewed: {name}",
                        progress_state,
                    )
                    continue

            cloud_id = client.push_observation(obs)

            # Update local record with cloud_id and sync_status
            conn2 = get_connection()
            conn2.execute(
                "UPDATE observations SET cloud_id = ?, sync_status = 'synced', synced_at = ? WHERE id = ?",
                (cloud_id, datetime.now(timezone.utc).isoformat(), obs['id']),
            )
            conn2.commit()
            conn2.close()

            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                f"Observation {i + 1}/{max(1, total)} synced: {name}",
                progress_state,
            )

            images_synced = True
            if sync_images:
                local_obs_id = _safe_int(obs.get('id'))
                current_local_media_signature = ''
                stored_local_media_signature = (
                    _load_local_cloud_media_signature(local_obs_id)
                    if had_existing_cloud and local_obs_id > 0
                    else ''
                )
                if had_existing_cloud and local_obs_id > 0 and stored_local_media_signature:
                    current_local_media_signature = _local_cloud_media_signature(local_obs_id)
                if (
                    had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_media_signature == stored_local_media_signature
                ):
                    _emit_progress(
                        progress_cb,
                        f"Skipping unchanged cloud media for observation {i + 1}/{max(1, total)}: {name}",
                        progress_state,
                    )
                    # Images unchanged but measurements may have been added/updated
                    if local_obs_id > 0:
                        try:
                            _push_measurements_for_observation(client, local_obs_id)
                        except Exception as e:
                            print(f'[cloud_sync] Measurement push failed for obs {local_obs_id}: {e}')
                else:
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=prepare_images_cb,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                    )
                    if images_synced and local_obs_id > 0:
                        try:
                            _push_measurements_for_observation(client, local_obs_id)
                        except Exception as e:
                            print(f'[cloud_sync] Measurement push failed for obs {local_obs_id}: {e}')
                if local_obs_id > 0:
                    if images_synced:
                        if not current_local_media_signature:
                            current_local_media_signature = _refresh_local_cloud_media_signature(local_obs_id)
                        else:
                            _store_local_cloud_media_signature(local_obs_id, current_local_media_signature)
                    else:
                        mark_observation_dirty(local_obs_id)
            _store_remote_snapshot(client, cloud_id)

            pushed += 1
        except CloudSyncError as e:
            errors.append(f"obs {obs['id']}: {e}")
            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                f"Observation {i + 1}/{max(1, total)} failed: {name}",
                progress_state,
            )

    return {'pushed': pushed, 'total': total, 'errors': errors}


def _push_images_for_observation(
    client: SporelyCloudClient,
    obs: dict,
    obs_cloud_id: str,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    observation_index: int | None = None,
    observation_total: int | None = None,
) -> bool:
    """Push selected observation images for one observation."""
    prepared_items: list[dict] = []
    cleanup = None
    warnings: list[str] = []
    preparation_failed = False
    observation_name = _observation_display_name(obs)
    if callable(prepare_images_cb):
        try:
            def prepare_progress(message: str, _current: int | None = None, _total: int | None = None) -> None:
                _emit_progress(progress_cb, message, progress_state)

            prepared_items, cleanup, warnings = prepare_images_cb(obs, prepare_progress)
        except Exception as e:
            print(f'[cloud_sync] Observation {obs["id"]} image preparation failed: {e}')
            prepared_items = []
            cleanup = None
            warnings = [str(e)]
            preparation_failed = True
    else:
        images = ImageDB.get_images_for_observation(obs['id'])
        for img in images:
            if img.get('image_type') == 'microscope' and not img.get('cloud_id'):
                continue
            prepared_items.append({
                'image_row': img,
                'upload_path': img.get('filepath'),
            })

    for warning in warnings:
        print(f'[cloud_sync] Observation {obs["id"]}: {warning}')

    if prepared_items:
        _extend_progress_total(progress_state, len(prepared_items))
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                f"Prepared {len(prepared_items)} cloud image(s) for observation {observation_index}/{max(1, observation_total)}: {observation_name}",
                progress_state,
            )

    if preparation_failed:
        if observation_index and observation_total:
            _emit_progress(
                progress_cb,
                f"Cloud media preparation failed for observation {observation_index}/{max(1, observation_total)}: {observation_name}",
                progress_state,
            )
        return False

    try:
        existing_rows = client.pull_image_metadata(obs_cloud_id) or []
    except Exception as e:
        print(f'[cloud_sync] Could not fetch existing cloud images for observation {obs["id"]}: {e}')
        existing_rows = []
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
        kept_cloud_ids: set[str] = set()
        had_failures = False
        for item_index, item in enumerate(prepared_items, start=1):
            img = dict(item.get('image_row') or {})
            if not img:
                processed_items += 1
                _advance_progress(progress_state, 1)
                continue
            if not should_push_local_image_to_cloud(img):
                processed_items += 1
                _advance_progress(progress_state, 1)
                continue
            upload_path = str(item.get('upload_path') or img.get('filepath') or '').strip()
            if not upload_path:
                processed_items += 1
                _advance_progress(progress_state, 1)
                continue
            try:
                if observation_index and observation_total:
                    _emit_progress(
                        progress_cb,
                        (
                            f"Uploading cloud image {item_index}/{max(1, total_items)} "
                            f"for observation {observation_index}/{max(1, observation_total)}: {observation_name}…"
                        ),
                        progress_state,
                    )
                local_image_id = _safe_int(img.get('id'))
                remote_row = existing_by_desktop_id.get(local_image_id)
                local_cloud_id = str(img.get('cloud_id') or '').strip()
                if remote_row is None and local_cloud_id:
                    remote_row = existing_by_id.get(local_cloud_id)

                remote_cloud_id = str((remote_row or {}).get('id') or '').strip()
                storage_path = client._build_storage_path(
                    obs_cloud_id,
                    remote_cloud_id or str(local_image_id or img.get('id') or ''),
                    upload_path,
                )
                expected_payload = _prepared_item_remote_payload(img, upload_path, storage_path)
                remote_payload = _remote_image_payload(remote_row)
                metadata_matches = bool(remote_row) and remote_payload == expected_payload

                current_file_sig = _file_content_signature(upload_path)
                stored_file_sig = _load_cloud_image_file_signature(obs.get('id'), local_image_id)
                file_matches = False
                if remote_row and str(remote_row.get('storage_path') or '').strip() == str(storage_path or '').strip():
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

                img_cloud_id = remote_cloud_id
                if not img_cloud_id or not metadata_matches:
                    img_cloud_id = client.push_image_metadata(img, obs_cloud_id, storage_path)
                    if storage_path != client._build_storage_path(obs_cloud_id, img_cloud_id, upload_path):
                        storage_path = client._build_storage_path(obs_cloud_id, img_cloud_id, upload_path)
                        client._patch(
                            f'observation_images?id=eq.{img_cloud_id}',
                            {'storage_path': storage_path},
                        )
                    remote_payload = _prepared_item_remote_payload(img, upload_path, storage_path)
                    metadata_matches = True
                if not file_matches:
                    client.upload_image_file(upload_path, obs_cloud_id, img_cloud_id)
                kept_cloud_ids.add(str(img_cloud_id or '').strip())

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
            except CloudSyncError as e:
                had_failures = True
                print(f'[cloud_sync] Image {img["id"]} push failed: {e}')
            finally:
                processed_items += 1
                _advance_progress(progress_state, 1)
                if observation_index and observation_total:
                    _emit_progress(
                        progress_cb,
                        (
                            f"Processed cloud image {item_index}/{max(1, total_items)} "
                            f"for observation {observation_index}/{max(1, observation_total)}: {observation_name}"
                        ),
                        progress_state,
                    )
        stale_rows = [
            row for row in existing_rows
            if str(row.get('id') or '').strip() and str(row.get('id') or '').strip() not in kept_cloud_ids
        ]
        for stale_row in stale_rows:
            stale_cloud_id = str(stale_row.get('id') or '').strip()
            stale_storage_path = str(stale_row.get('storage_path') or '').strip()
            if stale_storage_path:
                try:
                    client._storage_remove([stale_storage_path])
                except Exception as e:
                    print(
                        f'[cloud_sync] Could not remove old cloud storage file for observation {obs["id"]}: {e}'
                    )
            try:
                client._delete(f'observation_images?id=eq.{stale_cloud_id}')
            except Exception as e:
                print(f'[cloud_sync] Could not remove old cloud image row for observation {obs["id"]}: {e}')
            stale_desktop_id = _safe_int(stale_row.get('desktop_id'))
            if stale_desktop_id > 0:
                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE images SET cloud_id = NULL, synced_at = NULL WHERE id = ?',
                        (stale_desktop_id,),
                    )
                    conn.commit()
                finally:
                    conn.close()
            _clear_cloud_image_file_signature(obs.get('id'), stale_desktop_id or stale_cloud_id)
        if total_items > processed_items:
            _advance_progress(progress_state, total_items - processed_items)
        return not had_failures
    finally:
        if callable(cleanup):
            try:
                cleanup()
            except Exception:
                pass


def _push_measurements_for_observation(
    client: SporelyCloudClient,
    obs_local_id: int,
) -> None:
    """Push all spore measurements for an observation's microscope images to the cloud.

    Only images that have a cloud_id (i.e. have been synced) are included.
    Measurements are upserted by desktop_id; stale cloud rows for images that
    still exist locally are cleaned up.
    """
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT m.id, m.image_id, m.length_um, m.width_um, m.measurement_type,
                   m.p1_x, m.p1_y, m.p2_x, m.p2_y,
                   m.p3_x, m.p3_y, m.p4_x, m.p4_y,
                   m.measured_at, m.cloud_id,
                   i.cloud_id AS image_cloud_id
            FROM spore_measurements m
            JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ?
              AND i.image_type = 'microscope'
              AND i.cloud_id IS NOT NULL
            ORDER BY m.id
            ''',
            (obs_local_id,),
        )
        measurements = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    pushed_cloud_ids: set[str] = set()
    for meas in measurements:
        cloud_image_id = str(meas.get('image_cloud_id') or '').strip()
        if not cloud_image_id:
            continue
        try:
            cloud_meas_id = client.push_measurement(meas, cloud_image_id)
            pushed_cloud_ids.add(cloud_meas_id)
            if str(meas.get('cloud_id') or '').strip() != cloud_meas_id:
                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE spore_measurements SET cloud_id = ? WHERE id = ?',
                        (cloud_meas_id, int(meas['id'])),
                    )
                    conn.commit()
                finally:
                    conn.close()
        except Exception as e:
            print(f'[cloud_sync] Measurement {meas["id"]} push failed: {e}')


def pull_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
) -> dict:
    """Pull new cloud observations and apply remote updates to clean local rows."""
    remote_obs = list(remote_obs or client.list_remote_observations())
    total = len(remote_obs)
    pulled = 0
    errors = []
    imported_local_ids: list[int] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)

    all_cloud_ids = [str(r.get('id') or '').strip() for r in remote_obs if str(r.get('id') or '').strip()]
    bulk_images = client.pull_bulk_image_metadata(all_cloud_ids)
    remote_images_by_obs = {}
    for img in bulk_images:
        obs_id = str(img.get('observation_id') or '').strip()
        if obs_id:
            remote_images_by_obs.setdefault(obs_id, []).append(img)

    for i, remote in enumerate(remote_obs):
        name = _observation_display_name(remote)
        cloud_id = str(remote.get('id') or '').strip()
        _emit_progress(
            progress_cb,
            f"Checking cloud observation {i + 1}/{max(1, total)}: {name}…",
            progress_state,
        )

        try:
            local_obs = _find_local_observation_for_remote(remote)
            # DO NOT filter by should_pull_cloud_image_to_desktop here, otherwise the 
            # conflict logic falsely thinks microscope images were deleted by the cloud!
            remote_images = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])]

            remote_snapshot = _cloud_observation_snapshot(remote, remote_images)
            stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''

            if local_obs is None:
                local_id = _create_local_from_remote(
                    remote,
                    progress_cb=progress_cb,
                    progress_state=progress_state,
                    remote_index=i + 1,
                    remote_total=total,
                    remote_images=remote_images,
                )
                if cloud_id:
                    client.set_desktop_id(cloud_id, local_id)
                    # Re-fetch images after set_image_desktop_id calls inside
                    # _create_local_from_remote so the snapshot captures the
                    # updated desktop_id values.  Storing the pre-pull snapshot
                    # would cause a spurious conflict on the very next sync
                    # because image keys would shift from cloud:<id> to
                    # desktop:<id> between the stored baseline and the live data.
                    _store_remote_snapshot(client, cloud_id)
                _refresh_local_cloud_media_signature(local_id)
                pulled += 1
                imported_local_ids.append(int(local_id))
            else:
                local_id = int(local_obs['id'])
                if cloud_id and int(remote.get('desktop_id') or 0) != local_id:
                    try:
                        client.set_desktop_id(cloud_id, local_id)
                    except Exception:
                        pass
                local_dirty = str(local_obs.get('sync_status') or '').strip().lower() == 'dirty'
                if local_dirty and cloud_id and _clear_observation_dirty_if_no_real_changes(local_id, cloud_id):
                    local_obs = ObservationDB.get_observation(local_id) or local_obs
                    local_dirty = False
                remote_changed = (not stored_snapshot) or (remote_snapshot != stored_snapshot)
                should_store_snapshot = True
                if remote_changed and not stored_snapshot:
                    if local_dirty:
                        errors.append(
                            _format_review_needed_error(
                                local_id,
                                cloud_id,
                                ['both desktop and cloud changed since the last known sync'],
                            )
                        )
                        should_store_snapshot = False
                    else:
                        _emit_progress(
                            progress_cb,
                            f"Applying cloud changes to local observation {local_id}: {name}…",
                            progress_state,
                        )
                        _apply_remote_observation_fields(local_id, remote)
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            remote_images,
                            allow_delete=False,
                        )
                        errors.extend(warnings)
                        _stamp_observation_synced(local_id, cloud_id)
                        _refresh_local_cloud_media_signature(local_id)
                        pulled += 1
                elif remote_changed:
                    snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                    baseline_obs = _baseline_observation_compare_payload(
                        snapshot_data.get('observation') or {}
                    )
                    baseline_images = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
                    field_changes = _analyze_observation_field_changes(local_obs, remote, baseline_obs)
                    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
                    remote_image_changes = _analyze_image_changes(remote_image_payloads, baseline_images)
                    remote_raw_map = {_image_compare_key(row): row for row in remote_images}
                    stored_local_media_signature = _load_local_cloud_media_signature(local_id)
                    current_local_media_signature = _local_cloud_media_signature(local_id)
                    local_media_changed = bool(
                        stored_local_media_signature
                        and current_local_media_signature
                        and stored_local_media_signature != current_local_media_signature
                    )
                    media_conflict = bool(remote_image_changes.get('removed_keys'))
                    if local_media_changed and (
                        remote_image_changes.get('metadata_changed_keys')
                        or remote_image_changes.get('order_changed')
                    ):
                        media_conflict = True

                    applied_remote_fields = False
                    applied_safe_media = False
                    _emit_progress(
                        progress_cb,
                        f"Applying cloud changes to local observation {local_id}: {name}…",
                        progress_state,
                    )
                    if field_changes.get('remote_only_fields'):
                        _apply_remote_observation_fields(
                            local_id,
                            remote,
                            fields=set(field_changes.get('remote_only_fields') or []),
                        )
                        applied_remote_fields = True

                    if not media_conflict and remote_image_changes.get('changed'):
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            remote_images,
                            allow_delete=False,
                        )
                        errors.extend(warnings)
                        applied_safe_media = True
                    elif remote_image_changes.get('added_keys'):
                        added_remote_images = [
                            remote_raw_map[key]
                            for key in remote_image_changes.get('added_keys') or []
                            if key in remote_raw_map
                        ]
                        if added_remote_images:
                            warnings = _apply_remote_images_to_local(
                                client,
                                local_id,
                                added_remote_images,
                                allow_delete=False,
                            )
                            errors.extend(warnings)
                            applied_safe_media = True

                    review_reasons: list[str] = []
                    if field_changes.get('conflict_fields'):
                        review_reasons.append('both desktop and cloud changed the same observation fields')
                    if remote_image_changes.get('removed_keys'):
                        review_reasons.append('cloud removed local image files')
                    if local_media_changed and (
                        remote_image_changes.get('metadata_changed_keys')
                        or remote_image_changes.get('order_changed')
                    ):
                        review_reasons.append('both desktop and cloud changed image or media details')

                    if review_reasons:
                        errors.append(_format_review_needed_error(local_id, cloud_id, review_reasons))
                        should_store_snapshot = False
                        if not local_dirty:
                            _set_observation_sync_state(local_id, cloud_id, dirty=False)
                        if applied_remote_fields or applied_safe_media:
                            pulled += 1
                    else:
                        remaining_local_changes = _remaining_local_changes_after_remote_merge(
                            field_changes,
                            local_media_changed=local_media_changed,
                        )
                        _set_observation_sync_state(local_id, cloud_id, dirty=remaining_local_changes)
                        if not local_media_changed:
                            _refresh_local_cloud_media_signature(local_id)
                        pulled += 1
                if cloud_id and should_store_snapshot:
                    # Re-fetch images so the snapshot reflects any desktop_id
                    # values that were written back to the cloud during this pull.
                    _store_remote_snapshot(client, cloud_id)
        except Exception as e:
            errors.append(f"cloud {remote.get('id')}: {e}")
        finally:
            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                f"Processed cloud observation {i + 1}/{max(1, total)}: {name}",
                progress_state,
            )

    updates = {'cloud_last_pull_at': datetime.now(timezone.utc).isoformat()}
    if imported_local_ids:
        updates['cloud_recent_import_local_ids'] = json.dumps(imported_local_ids)
    update_app_settings(updates)
    return {
        'pulled': pulled,
        'total': total,
        'errors': errors,
        'deleted_remote': _detect_deleted_remote_observations(remote_obs),
    }


def _create_local_from_remote(
    remote: dict,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
) -> int:
    """Insert a cloud observation into local SQLite. Returns new local ID."""
    raw_location_public = remote.get('location_public')
    location_public = None if raw_location_public is None else bool(raw_location_public)
    sharing_scope = _normalize_sharing_scope(
        remote.get('visibility') or remote.get('sharing_scope'),
        fallback='friends' if location_public else 'private',
    )
    raw_spore_vis = str(remote.get('spore_data_visibility') or 'public').strip().lower()
    spore_data_visibility = raw_spore_vis if raw_spore_vis in {'private', 'friends', 'public'} else 'public'

    # Map cloud columns to create_observation kwargs
    kwargs = dict(
        date=remote.get('date') or datetime.now().strftime('%Y-%m-%d'),
        genus=remote.get('genus'),
        species=remote.get('species'),
        common_name=remote.get('common_name'),
        species_guess=remote.get('species_guess'),
        location=remote.get('location'),
        habitat=remote.get('habitat'),
        notes=remote.get('notes'),
        open_comment=remote.get('open_comment'),
        sharing_scope=sharing_scope,
        location_public=location_public,
        spore_data_visibility=spore_data_visibility,
        uncertain=bool(remote.get('uncertain', False)),
        unspontaneous=bool(remote.get('unspontaneous', False)),
        gps_latitude=remote.get('gps_latitude'),
        gps_longitude=remote.get('gps_longitude'),
        source_type=remote.get('source_type') or 'personal',
        author=remote.get('author'),
        habitat_nin2_path=remote.get('habitat_nin2_path'),
        habitat_substrate_path=remote.get('habitat_substrate_path'),
        habitat_host_genus=remote.get('habitat_host_genus'),
        habitat_host_species=remote.get('habitat_host_species'),
        habitat_host_common_name=remote.get('habitat_host_common_name'),
        habitat_nin2_note=remote.get('habitat_nin2_note'),
        habitat_substrate_note=remote.get('habitat_substrate_note'),
        habitat_grows_on_note=remote.get('habitat_grows_on_note'),
        publish_target=remote.get('publish_target'),
    )
    local_id = ObservationDB.create_observation(**kwargs)

    # Stamp the cloud_id and sync_status on the newly created row
    conn = get_connection()
    conn.execute(
        "UPDATE observations SET cloud_id = ?, sync_status = 'synced', synced_at = ? WHERE id = ?",
        (remote['id'], datetime.now(timezone.utc).isoformat(), local_id),
    )
    conn.commit()
    conn.close()

    cloud_id = str(remote.get('id') or '').strip()
    if cloud_id:
        _import_remote_images(
            remote,
            local_id,
            cloud_id,
            progress_cb=progress_cb,
            progress_state=progress_state,
            remote_index=remote_index,
            remote_total=remote_total,
            remote_images=remote_images,
        )

    return local_id


def _import_remote_images(
    remote: dict,
    local_id: int,
    cloud_id: str,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
) -> None:
    """Download and create local image rows for a newly pulled cloud observation."""
    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        return
    remote_images = list(remote_images or client.pull_image_metadata(cloud_id) or [])
    remote_images = [dict(row or {}) for row in remote_images if should_pull_cloud_image_to_desktop(row)]
    if not remote_images:
        return
    _extend_progress_total(progress_state, len(remote_images))

    def _sort_key(image_row: dict) -> tuple[int, str]:
        try:
            sort_value = int(image_row.get('sort_order'))
        except (TypeError, ValueError):
            sort_value = 10**9
        return sort_value, str(image_row.get('id') or '')

    temp_dir = Path(tempfile.mkdtemp(prefix=f'sporely_cloud_pull_{local_id}_'))
    synced_at = datetime.now(timezone.utc).isoformat()
    created_image_ids: list[int] = []
    observation_name = _observation_display_name(remote)
    try:
        images_to_pull = [img for img in remote_images if should_pull_cloud_image_to_desktop(img)]
        for idx, image_row in enumerate(sorted(images_to_pull, key=_sort_key), start=1):
            try:
                if remote_index and remote_total:
                    _emit_progress(
                        progress_cb,
                        (
                            f"Importing cloud image {idx}/{len(remote_images)} "
                            f"for observation {remote_index}/{max(1, remote_total)}: {observation_name}…"
                        ),
                        progress_state,
                    )
                storage_path = str(image_row.get('storage_path') or '').strip()
                if not storage_path:
                    continue
                cloud_image_id = str(image_row.get('id') or '').strip()
                filename = Path(str(image_row.get('original_filename') or '')).name or f'cloud_{idx}.jpg'
                download_path = temp_dir / f'{idx:02d}_{filename}'
                client.download_image_file(storage_path, download_path)

                local_image_id = ImageDB.add_image(
                    observation_id=int(local_id),
                    filepath=str(download_path),
                    image_type=str(image_row.get('image_type') or 'field'),
                    scale=image_row.get('scale_microns_per_pixel'),
                    notes=image_row.get('notes'),
                    micro_category=image_row.get('micro_category'),
                    objective_name=image_row.get('objective_name'),
                    mount_medium=image_row.get('mount_medium'),
                    stain=image_row.get('stain'),
                    sample_type=image_row.get('sample_type'),
                    contrast=image_row.get('contrast'),
                    sort_order=image_row.get('sort_order'),
                    gps_source=image_row.get('gps_source'),
                    resample_scale_factor=image_row.get('resample_scale_factor'),
                    copy_to_folder=True,
                    mark_observation_dirty=False,
                )
                created_image_ids.append(int(local_image_id))

                conn = get_connection()
                try:
                    conn.execute(
                        'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                        (cloud_image_id, synced_at, int(local_image_id)),
                    )
                    conn.commit()
                finally:
                    conn.close()

                try:
                    client.set_image_desktop_id(cloud_image_id, int(local_image_id))
                except Exception:
                    pass

                try:
                    generate_all_sizes(str(download_path), int(local_image_id))
                except Exception:
                    try:
                        stored = ImageDB.get_image(int(local_image_id))
                        if stored and stored.get('filepath'):
                            generate_all_sizes(str(stored.get('filepath')), int(local_image_id))
                    except Exception:
                        pass
            finally:
                _advance_progress(progress_state, 1)
                if remote_index and remote_total:
                    _emit_progress(
                        progress_cb,
                        (
                            f"Imported cloud image {idx}/{len(remote_images)} "
                            f"for observation {remote_index}/{max(1, remote_total)}: {observation_name}"
                        ),
                        progress_state,
                    )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def sync_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
) -> dict:
    """Run push then pull. Returns combined summary."""
    progress_state = {'done': 0, 'total': 0}
    try:
        remote_obs_before_push = client.list_remote_observations()
    except CloudSyncError as exc:
        raise CloudSyncError(
            "Could not fetch the current cloud state before syncing.\n\n"
            f"Details:\n{exc}"
        ) from exc
    try:
        push_result = push_all(
            client,
            progress_cb=progress_cb,
            sync_images=sync_images,
            prepare_images_cb=prepare_images_cb,
            progress_state=progress_state,
            remote_obs=remote_obs_before_push,
        )
    except CloudSyncError as exc:
        raise CloudSyncError(
            "Push phase failed while uploading local observations to Sporely Cloud.\n\n"
            f"Details:\n{exc}"
        ) from exc

    try:
        remote_obs_after_push = client.list_remote_observations()
    except CloudSyncError as exc:
        raise CloudSyncError(
            "Could not refresh cloud observations after push.\n\n"
            f"Details:\n{exc}"
        ) from exc

    try:
        pull_result = pull_all(
            client,
            progress_cb=progress_cb,
            progress_state=progress_state,
            remote_obs=remote_obs_after_push,
        )
    except CloudSyncError as exc:
        raise CloudSyncError(
            "Pull phase failed while fetching observations from Sporely Cloud.\n\n"
            f"Details:\n{exc}"
        ) from exc

    try:
        remote_obs_after_pull = client.list_remote_observations()
    except CloudSyncError as exc:
        raise CloudSyncError(
            "Could not refresh cloud observations before finishing sync.\n\n"
            f"Details:\n{exc}"
        ) from exc

    if _has_pending_local_push_work():
        try:
            final_push_result = push_all(
                client,
                progress_cb=progress_cb,
                sync_images=sync_images,
                prepare_images_cb=prepare_images_cb,
                progress_state=progress_state,
                remote_obs=remote_obs_after_pull,
            )
        except CloudSyncError as exc:
            raise CloudSyncError(
                "Final push phase failed while uploading remaining local observations to Sporely Cloud.\n\n"
                f"Details:\n{exc}"
            ) from exc
    else:
        final_push_result = {'pushed': 0, 'errors': []}

    return {
        'pushed': push_result['pushed'] + final_push_result['pushed'],
        'pulled': pull_result['pulled'],
        'errors': push_result['errors'] + pull_result['errors'] + final_push_result['errors'],
        'deleted_remote': list(pull_result.get('deleted_remote') or []),
    }


def mark_observation_dirty(observation_id: int) -> None:
    """Call this after updating an observation locally so it gets re-pushed."""
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE observations
            SET sync_status = 'dirty',
                updated_at = ?
            WHERE id = ?
              AND cloud_id IS NOT NULL
            """,
            (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), observation_id),
        )
    except Exception:
        conn.execute(
            "UPDATE observations SET sync_status = 'dirty' WHERE id = ? AND cloud_id IS NOT NULL",
            (observation_id,),
        )
    conn.commit()
    conn.close()
