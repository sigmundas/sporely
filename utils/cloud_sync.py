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

import base64
import hashlib
import io
import json
import mimetypes
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import requests
from PIL import Image, ImageOps, features

from app_identity import runtime_profile_scope, using_isolated_profile
from database.schema import get_connection, get_app_settings, update_app_settings
from database.models import ObservationDB, ImageDB, SettingsDB, MeasurementDB
from utils.r2_storage import CloudflareR2Client, media_variant_key, normalize_media_key
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
    'is_draft',
    'location_precision',
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


def _normalize_sharing_scope(value: str | None, fallback: str = 'public') -> str:
    raw = str(value or '').strip().lower()
    if raw == 'draft':
        return 'private'
    if raw in {'private', 'friends', 'public'}:
        return raw
    fallback_raw = str(fallback or 'public').strip().lower()
    if fallback_raw == 'draft':
        return 'private'
    return fallback_raw if fallback_raw in {'private', 'friends', 'public'} else 'public'


def _sharing_scope_to_cloud_visibility(value: str | None, fallback: str = 'public') -> str:
    """Map local desktop sharing scope to the Phase 7 cloud visibility value."""
    return _normalize_sharing_scope(value, fallback=fallback)


def _cloud_visibility_to_sharing_scope(value: str | None, fallback: str = 'public') -> str:
    """Map Phase 7 cloud visibility back to the local desktop sharing scope."""
    return _normalize_sharing_scope(value, fallback=fallback)


def _normalize_location_precision(value: str | None, fallback: str = 'exact') -> str:
    raw = str(value or '').strip().lower()
    if raw in {'exact', 'fuzzed'}:
        return raw
    fallback_raw = str(fallback or 'exact').strip().lower()
    return fallback_raw if fallback_raw in {'exact', 'fuzzed'} else 'exact'


def _encode_postgrest_filter_value(value: str | None) -> str:
    """Encode filter values for PostgREST query strings.

    Timestamps may contain '+' in timezone offsets, which must be percent-encoded
    inside a URL query or they can be parsed incorrectly.
    """
    return quote(str(value or '').strip(), safe='')


def _normalize_cloud_media_key(value: str | None) -> str:
    """Normalize cloud media references to the stored relative key form."""
    return normalize_media_key(value)

_IMG_PUSH_COLS = [
    'sort_order', 'image_type', 'micro_category', 'objective_name',
    'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain', 'sample_type', 'contrast', 'notes',
    'gps_source', 'storage_path',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h',
]
_IMG_UPLOAD_META_COLS = [
    'upload_mode',
    'source_width',
    'source_height',
    'stored_width',
    'stored_height',
    'stored_bytes',
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
_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX = "sporely_cloud_snapshot_obs_"
_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX = "sporely_cloud_image_file_sig_"
_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX = "sporely_cloud_local_media_sig_obs_"
_SETTING_LINKED_CLOUD_USER_ID = "linked_cloud_user_id"
_CLOUD_LOCAL_MEDIA_RENDER_VERSION = "2"
_REMOTE_SYNC_TIMESTAMP_GRACE_SECONDS = 5.0
_CLOUD_THUMB_MAX_EDGE = 400
_LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS = (
    'ai_crop_x1',
    'ai_crop_y1',
    'ai_crop_x2',
    'ai_crop_y2',
    'ai_crop_source_w',
    'ai_crop_source_h',
)

_SNAPSHOT_OBS_FIELDS = [
    'id', 'desktop_id', 'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude', 'location_public',
    'is_draft', 'location_precision',
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
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h',
    'upload_mode', 'source_width', 'source_height',
    'stored_width', 'stored_height', 'stored_bytes',
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
    'is_draft',
    'location_precision',
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
    'is_draft': 'Draft',
    'location_precision': 'Location precision',
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


class AccountMismatchError(CloudSyncError):
    pass


ACCOUNT_MISMATCH_MESSAGE = (
    "This local database is permanently linked to another Sporely Cloud account. "
    "Please switch to the correct OS user profile, or use the 'Reset Cloud Sync' "
    "tool in Settings to migrate your data to a new account."
)


def _normalize_cloud_user_id(value: str | None) -> str:
    return str(value or '').strip()


def _decode_jwt_subject(access_token: str | None) -> str:
    token = str(access_token or '').strip()
    parts = token.split('.')
    if len(parts) < 2:
        return ''
    payload = parts[1]
    padding = '=' * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode((payload + padding).encode('ascii')).decode('utf-8'))
    except Exception:
        return ''
    return _normalize_cloud_user_id(data.get('sub') if isinstance(data, dict) else None)


def _load_linked_cloud_user_id() -> str:
    return _normalize_cloud_user_id(get_app_settings().get(_SETTING_LINKED_CLOUD_USER_ID))


def _save_linked_cloud_user_id(user_id: str) -> None:
    normalized = _normalize_cloud_user_id(user_id)
    if normalized:
        update_app_settings({_SETTING_LINKED_CLOUD_USER_ID: normalized})


def ensure_database_linked_to_cloud_user(client: "SporelyCloudClient") -> str:
    """Bind this local DB to the active cloud account, or reject a mismatch."""
    current_user_id = _normalize_cloud_user_id(client.fetch_current_user_id())
    if not current_user_id:
        raise CloudSyncError("Could not verify the active Sporely Cloud account before syncing.")
    linked_user_id = _load_linked_cloud_user_id()
    if not linked_user_id:
        _save_linked_cloud_user_id(current_user_id)
        return current_user_id
    if linked_user_id != current_user_id:
        raise AccountMismatchError(ACCOUNT_MISMATCH_MESSAGE)
    return current_user_id


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
    if field in {'location_public', 'is_draft', 'uncertain', 'unspontaneous', 'interesting_comment'}:
        return None if value is None else bool(value)
    return _normalize_snapshot_value(value)


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
                    fallback='public',
                )
            ))
        elif field == 'sharing_scope':
            payload[field] = _normalize_observation_field_value(field, (
                _normalize_sharing_scope(
                    row.get('sharing_scope') if local else (row.get('visibility') or row.get('sharing_scope')),
                    fallback='public',
                )
            ))
        elif field == 'location_precision':
            payload[field] = _normalize_observation_field_value(field, (
                _normalize_location_precision(row.get('location_precision'))
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
                    fallback='public',
                ),
            )
        elif field == 'location_precision':
            payload[field] = _normalize_observation_field_value(field, _normalize_location_precision(row.get('location_precision')))
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
        'ai_crop_x1': _normalize_snapshot_value(row.get('ai_crop_x1')),
        'ai_crop_y1': _normalize_snapshot_value(row.get('ai_crop_y1')),
        'ai_crop_x2': _normalize_snapshot_value(row.get('ai_crop_x2')),
        'ai_crop_y2': _normalize_snapshot_value(row.get('ai_crop_y2')),
        'ai_crop_source_w': _normalize_snapshot_value(row.get('ai_crop_source_w')),
        'ai_crop_source_h': _normalize_snapshot_value(row.get('ai_crop_source_h')),
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

    lines: list[str] = []
    if added:
        labels = ", ".join(_image_label(row) for row in added[:3])
        if len(added) > 3:
            labels += ", …"
        lines.append(f'Images added since last sync: {labels}')
    if removed:
        labels = ", ".join(_image_label(row) for row in removed[:3])
        if len(removed) > 3:
            labels += ", …"
        lines.append(f'Images removed since last sync: {labels}')
    if metadata_changed:
        lines.append(f'Image metadata changed on {metadata_changed} image(s) since last sync')
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

    return {
        'added_keys': added_keys,
        'removed_keys': removed_keys,
        'metadata_changed_keys': metadata_changed_keys,
        'order_changed': False,
        'added': [current_map[key] for key in added_keys],
        'removed': [baseline_map[key] for key in removed_keys],
        'changed': bool(added_keys or removed_keys or metadata_changed_keys),
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
    if not current_media_sig:
        return False
    if _local_media_signatures_match(stored_media_sig, current_media_sig):
        _store_local_media_signature_if_equivalent(local_id, stored_media_sig, current_media_sig)
        return False
    return True


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


def _parsed_local_media_signature(signature: str | None) -> dict:
    text = str(signature or '').strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalized_local_media_signature_payload(payload: dict | None) -> dict:
    normalized = dict(payload or {})
    # Gallery layout state only affects desktop-local generated views.
    normalized['gallery_settings_raw'] = ''
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
    return normalized


def _local_media_signatures_match(stored_signature: str | None, current_signature: str | None) -> bool:
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
    return _normalized_local_media_signature_payload(stored_payload) == _normalized_local_media_signature_payload(current_payload)


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
                gps_source,
                ai_crop_x1,
                ai_crop_y1,
                ai_crop_x2,
                ai_crop_y2,
                ai_crop_source_w,
                ai_crop_source_h
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
        'cloud_image_size_mode': 'full',
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
                'ai_crop_x1': _normalize_snapshot_value(row.get('ai_crop_x1')),
                'ai_crop_y1': _normalize_snapshot_value(row.get('ai_crop_y1')),
                'ai_crop_x2': _normalize_snapshot_value(row.get('ai_crop_x2')),
                'ai_crop_y2': _normalize_snapshot_value(row.get('ai_crop_y2')),
                'ai_crop_source_w': _normalize_snapshot_value(row.get('ai_crop_source_w')),
                'ai_crop_source_h': _normalize_snapshot_value(row.get('ai_crop_source_h')),
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
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    normalized_key = _normalize_cloud_media_key(storage_path)
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


def _remote_image_payload(
    remote_image: dict | None,
    *,
    include_ai_crop: bool = True,
    include_upload_meta: bool = True,
) -> dict:
    image = remote_image or {}
    payload = {
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
        'storage_path': _normalize_snapshot_value(_normalize_cloud_media_key(image.get('storage_path')) or None),
        'original_filename': _normalize_snapshot_value(image.get('original_filename')),
    }
    if include_ai_crop:
        payload.update({
            'ai_crop_x1': _normalize_snapshot_value(image.get('ai_crop_x1')),
            'ai_crop_y1': _normalize_snapshot_value(image.get('ai_crop_y1')),
            'ai_crop_x2': _normalize_snapshot_value(image.get('ai_crop_x2')),
            'ai_crop_y2': _normalize_snapshot_value(image.get('ai_crop_y2')),
            'ai_crop_source_w': _normalize_snapshot_value(image.get('ai_crop_source_w')),
            'ai_crop_source_h': _normalize_snapshot_value(image.get('ai_crop_source_h')),
        })
    if include_upload_meta:
        payload.update({
            'upload_mode': _normalize_snapshot_value(image.get('upload_mode')),
            'source_width': _normalize_snapshot_value(image.get('source_width')),
            'source_height': _normalize_snapshot_value(image.get('source_height')),
            'stored_width': _normalize_snapshot_value(image.get('stored_width')),
            'stored_height': _normalize_snapshot_value(image.get('stored_height')),
            'stored_bytes': _normalize_snapshot_value(image.get('stored_bytes')),
        })
    return payload


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


def _load_local_observation_lookup() -> tuple[dict[str, dict], dict[int, dict]]:
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM observations')
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for row in rows:
        local_id = _safe_int(row.get('id'))
        cloud_id = str(row.get('cloud_id') or '').strip()
        if local_id > 0:
            by_local_id[local_id] = row
        if cloud_id:
            by_cloud_id[cloud_id] = row
    return by_cloud_id, by_local_id


def _find_local_observation_for_remote_cached(
    remote: dict,
    by_cloud_id: dict[str, dict],
    by_local_id: dict[int, dict],
) -> dict | None:
    cloud_id = str((remote or {}).get('id') or '').strip()
    if cloud_id and cloud_id in by_cloud_id:
        return dict(by_cloud_id[cloud_id])
    local_id = _safe_int((remote or {}).get('desktop_id'))
    if local_id > 0 and local_id in by_local_id:
        return dict(by_local_id[local_id])
    return None


def _remote_observation_changed_since_last_sync(local_obs: dict | None, remote: dict | None) -> bool:
    if not local_obs:
        return True
    synced_at = _parse_sync_timestamp((local_obs or {}).get('synced_at'))
    remote_changed_at = _parse_sync_timestamp((remote or {}).get('updated_at') or (remote or {}).get('created_at'))
    if synced_at is None or remote_changed_at is None:
        return True
    return (remote_changed_at - synced_at).total_seconds() > _REMOTE_SYNC_TIMESTAMP_GRACE_SECONDS


def _remote_snapshot_has_meaningful_changes(
    remote: dict | None,
    remote_images: list[dict] | None,
    stored_snapshot: str | None,
) -> bool:
    snapshot = _parse_cloud_observation_snapshot(stored_snapshot)
    if not snapshot:
        return True
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    remote_payload = _observation_compare_payload(remote, local=False)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if remote_payload.get(field) != baseline_obs.get(field):
            return True
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]
    remote_image_payloads = [_remote_image_payload(img) for img in (remote_images or [])]
    remote_image_changes = _analyze_image_changes(remote_image_payloads, baseline_images)
    return bool(
        remote_image_changes.get('added_keys')
        or remote_image_changes.get('removed_keys')
        or remote_image_changes.get('metadata_changed_keys')
    )


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
        'sharing_scope': _cloud_visibility_to_sharing_scope(
            remote.get('visibility') or remote.get('sharing_scope'),
            fallback='friends' if location_public else 'public',
        ),
        'location_public': location_public,
        'is_draft': bool(remote.get('is_draft', True)),
        'location_precision': _normalize_location_precision(remote.get('location_precision')),
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


def _inject_obs_exif_into_field_image(
    image_path: Path,
    obs_lat: float | None,
    obs_lon: float | None,
    obs_altitude: float | None,
    obs_datetime_str: str | None,
) -> None:
    """Write observation GPS/datetime into a JPEG that has no EXIF.

    Called on cloud-synced field images whose EXIF was stripped by the web
    app's 2 MP conversion.  Only modifies the file when the image has no
    existing DateTimeOriginal AND the observation has GPS or datetime data.
    Does nothing for non-JPEG files or on any error.
    """
    if not image_path.exists():
        return
    suffix = image_path.suffix.lower()
    if suffix not in {'.jpg', '.jpeg'}:
        return
    has_coords = obs_lat is not None and obs_lon is not None
    has_datetime = bool(obs_datetime_str)
    if not has_coords and not has_datetime:
        return
    try:
        from PIL import Image as _PilImage, ExifTags as _ExifTags
        with _PilImage.open(image_path) as img:
            existing_exif = img.getexif()
            existing_tags = {
                _ExifTags.TAGS.get(k, k): v for k, v in existing_exif.items()
            } if existing_exif else {}
            already_has_dt = any(
                t in existing_tags
                for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
            )
            try:
                already_has_gps = bool(existing_exif.get_ifd(0x8825))
            except Exception:
                already_has_gps = False
            if already_has_dt and already_has_gps:
                return  # nothing to do

            exif = existing_exif if existing_exif is not None else img.getexif()

            if not already_has_dt and has_datetime:
                try:
                    dt_exif = _exif_datetime_from_text(obs_datetime_str)
                    # Tag 306 = DateTime, 36867 = DateTimeOriginal
                    if dt_exif:
                        exif[306] = dt_exif
                        exif[36867] = dt_exif
                        exif[36868] = dt_exif
                except Exception:
                    pass

            if not already_has_gps and has_coords:
                try:
                    def _deg_to_rational(deg_float):
                        d = int(abs(deg_float))
                        m_float = (abs(deg_float) - d) * 60
                        m = int(m_float)
                        s_float = (m_float - m) * 60
                        s_num = int(round(s_float * 1000))
                        return ((d, 1), (m, 1), (s_num, 1000))

                    gps_ifd = {
                        1: 'N' if obs_lat >= 0 else 'S',    # GPSLatitudeRef
                        2: _deg_to_rational(obs_lat),        # GPSLatitude
                        3: 'E' if obs_lon >= 0 else 'W',    # GPSLongitudeRef
                        4: _deg_to_rational(obs_lon),        # GPSLongitude
                    }
                    if obs_altitude is not None:
                        altitude = float(obs_altitude)
                        gps_ifd[5] = 1 if altitude < 0 else 0  # GPSAltitudeRef
                        gps_ifd[6] = (int(round(abs(altitude) * 100)), 100)
                    exif[34853] = gps_ifd  # GPSInfo
                except Exception:
                    pass

            mode = img.mode
            if mode not in {'RGB', 'L'}:
                img = img.convert('RGB')
            try:
                exif_bytes = exif.tobytes()
                img.save(image_path, format='JPEG', quality=92, exif=exif_bytes)
            except Exception:
                pass
    except Exception as exc:
        print(f'[cloud_sync] Could not inject EXIF into {image_path.name}: {exc}')


def _exif_datetime_from_text(value: str | None) -> str | None:
    """Return EXIF datetime text (YYYY:MM:DD HH:MM:SS) from ISO/date text."""
    text = str(value or '').strip()
    if not text:
        return None
    try:
        normalized = text.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime('%Y:%m:%d %H:%M:%S')
    except Exception:
        pass
    try:
        if 'T' in text:
            date_part, time_part = text.split('T', 1)
        elif ' ' in text:
            date_part, time_part = text.split(' ', 1)
        else:
            date_part, time_part = text, '00:00:00'
        time_part = time_part.split('+', 1)[0].split('-', 1)[0].split('.', 1)[0]
        bits = [part for part in time_part.split(':') if part]
        while len(bits) < 3:
            bits.append('00')
        return f"{date_part.replace('-', ':')} {':'.join(bits[:3])}"
    except Exception:
        return None


def _load_obs_exif_fallback(observation_id: int, fallback_datetime: str | None = None) -> tuple[float | None, float | None, float | None, str | None]:
    """Return (lat, lon, altitude, datetime_str) from local observation data."""
    try:
        obs = ObservationDB.get_observation(observation_id)
        if not obs:
            return None, None, None, fallback_datetime
        lat = obs.get('gps_latitude')
        lon = obs.get('gps_longitude')
        altitude = obs.get('gps_altitude')
        datetime_str = str(
            obs.get('captured_at')
            or obs.get('date')
            or fallback_datetime
            or ''
        ).strip() or None
        return (float(lat) if lat is not None else None,
                float(lon) if lon is not None else None,
                float(altitude) if altitude is not None else None,
                datetime_str)
    except Exception:
        return None, None, None, fallback_datetime


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


def _cloud_thumb_save_format(path: Path) -> tuple[str, str, dict]:
    if features.check('webp'):
        return 'WEBP', 'image/webp', {'quality': 65, 'method': 4}
    return 'JPEG', 'image/jpeg', {'quality': 72}


def _content_type_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == '.webp':
        return 'image/webp'
    return mimetypes.guess_type(path.name)[0] or 'image/jpeg'


def _detected_image_extension(path: str | Path) -> str:
    try:
        with Image.open(path) as img:
            fmt = str(img.format or '').strip().upper()
    except Exception:
        return Path(path).suffix.lower() or '.jpg'
    if fmt == 'WEBP':
        return '.webp'
    if fmt == 'AVIF':
        return '.avif'
    if fmt in {'JPEG', 'JPG'}:
        return '.jpg'
    if fmt == 'PNG':
        return '.png'
    if fmt == 'TIFF':
        return '.tif'
    return Path(path).suffix.lower() or '.jpg'


def _rename_to_detected_image_extension(path: Path) -> Path:
    detected_ext = _detected_image_extension(path)
    if not detected_ext or path.suffix.lower() == detected_ext:
        return path
    target = path.with_suffix(detected_ext)
    counter = 1
    while target.exists() and target != path:
        target = path.with_name(f"{path.stem}_{counter}{detected_ext}")
        counter += 1
    path.rename(target)
    return target


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
        temp_path = _rename_to_detected_image_extension(temp_path)
        image_type = str(remote_image.get('image_type') or 'field').strip().lower()
        target_path = Path(existing_path) if existing_path else temp_path

        # Never overwrite a locally-held higher-resolution file with the cloud
        # 2 MP copy.  A locally-imported image is larger than the cloud version;
        # keep the local file and only update DB metadata.
        local_file_exists = existing_path and Path(existing_path).exists()
        local_is_larger = False
        if local_file_exists and image_type == 'field':
            try:
                local_size = Path(existing_path).stat().st_size
                cloud_size = temp_path.stat().st_size
                local_is_larger = local_size > cloud_size
            except Exception:
                pass

        if image_type == 'field' and not local_is_larger:
            obs_id = int(local_image.get('observation_id') or 0)
            if obs_id > 0:
                lat, lon, altitude, datetime_str = _load_obs_exif_fallback(
                    obs_id,
                    fallback_datetime=remote_image.get('captured_at'),
                )
                _inject_obs_exif_into_field_image(temp_path, lat, lon, altitude, datetime_str)

        if existing_path and not local_is_larger:
            detected_ext = _detected_image_extension(temp_path)
            if detected_ext and target_path.suffix.lower() != detected_ext:
                target_path = target_path.with_suffix(detected_ext)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(temp_path, target_path)
        # If local is larger it is the full-res desktop-imported original — keep it as-is.

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
            ai_crop_box=_remote_ai_crop_box(remote_image),
            ai_crop_source_size=_remote_ai_crop_source_size(remote_image),
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
        try:
            file_sig = _file_content_signature(str(target_path))
            if file_sig:
                _store_cloud_image_file_signature(int(local_image.get('observation_id') or 0), image_id, file_sig)
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
            try:
                _sync_existing_remote_image_to_local(client, local_image, remote_image)
            except CloudSyncError as exc:
                if _is_missing_cloud_image_error(exc):
                    print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, remote_image)}')
                    continue
                raise
            try:
                client.set_image_desktop_id(cloud_image_id, int(local_image.get('id')))
            except Exception:
                pass
            continue

        storage_path = _normalize_cloud_media_key(remote_image.get('storage_path'))
        if not storage_path:
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
                lat, lon, altitude, datetime_str = _load_obs_exif_fallback(
                    int(local_id),
                    fallback_datetime=remote_image.get('captured_at'),
                )
                _inject_obs_exif_into_field_image(download_path, lat, lon, altitude, datetime_str)
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
                ai_crop_box=_remote_ai_crop_box(remote_image),
                ai_crop_source_size=_remote_ai_crop_source_size(remote_image),
                captured_at=remote_image.get('captured_at'),
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
        and not _local_media_signatures_match(
            stored_local_media_signature,
            current_local_media_signature,
        )
    )
    if not local_media_changed:
        _store_local_media_signature_if_equivalent(
            local_id,
            stored_local_media_signature,
            current_local_media_signature,
        )
    local_measurement_count = len(MeasurementDB.get_measurements_for_observation(int(local_id)))
    if local_media_changed:
        local_image_changes.append(
            f'Desktop media details changed since last sync ({local_measurement_count} measurement(s) or publish-image settings)'
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

    should_push_images = prepare_images_cb is not None
    stored_local_media_signature = _load_local_cloud_media_signature(int(local_id))
    current_local_media_signature = _local_cloud_media_signature(int(local_id))
    local_media_changed = bool(
        stored_local_media_signature
        and current_local_media_signature
        and not _local_media_signatures_match(
            stored_local_media_signature,
            current_local_media_signature,
        )
    )
    if not local_media_changed:
        _store_local_media_signature_if_equivalent(
            int(local_id),
            stored_local_media_signature,
            current_local_media_signature,
        )

    if should_push_images and cloud_id:
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id)
        if stored_snapshot:
            baseline_images = [dict(row or {}) for row in (_parse_cloud_observation_snapshot(stored_snapshot).get('images') or [])]
            remote_images = [
                dict(row or {})
                for row in (client.pull_image_metadata(cloud_id) or [])
                if should_pull_cloud_image_to_desktop(row)
            ]
            remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
            remote_image_changes = _analyze_image_changes(remote_image_payloads, baseline_images)
            should_push_images = bool(
                local_media_changed
                or remote_image_changes.get('added_keys')
                or remote_image_changes.get('removed_keys')
                or remote_image_changes.get('metadata_changed_keys')
            )

    if should_push_images:
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


def resolve_conflict_merge(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    # For merge, keep local observation but add any new images from cloud
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    # First, pull any new images from cloud and add to local
    remote_obs = client.get_observation(resolved_cloud_id)
    if remote_obs:
        remote_images = [
            dict(row or {})
            for row in (client.pull_image_metadata(resolved_cloud_id) or [])
            if should_pull_cloud_image_to_desktop(row)
        ]
        warnings = _apply_remote_images_to_local(client, int(local_id), remote_images)
    else:
        warnings = []

    # Then push the local observation (which now includes merged images)
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

    # Push images if needed
    should_push_images = prepare_images_cb is not None
    if should_push_images:
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
    return {'local_id': int(local_id), 'cloud_id': cloud_id, 'warnings': warnings}


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
        self._r2: CloudflareR2Client | None = None
        self._column_support_cache: dict[tuple[str, str], bool] = {}
        self._cloud_image_storage_key_cache: dict[str, str] = {}
        self._s.headers.update({
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        })

    def _get_r2(self) -> CloudflareR2Client:
        if self._r2 is None:
            self._r2 = CloudflareR2Client.from_env()
        return self._r2

    def _has_column(self, table_name: str, column_name: str) -> bool:
        cache_key = (str(table_name or '').strip(), str(column_name or '').strip())
        if not all(cache_key):
            return False
        if cache_key in self._column_support_cache:
            return self._column_support_cache[cache_key]
        try:
            self._get(f'{cache_key[0]}?select={cache_key[1]}&limit=1')
            supported = True
        except CloudSyncError as exc:
            text = str(exc or '').lower()
            if (
                ('column' in text and cache_key[1].lower() in text and 'does not exist' in text)
                or 'could not find the' in text
            ):
                supported = False
            else:
                raise
        self._column_support_cache[cache_key] = supported
        return supported

    def _observation_supports_media_keys(self) -> bool:
        return self._has_column('observations', 'image_key') or self._has_column('observations', 'thumb_key')

    def _observation_images_support_ai_crop(self) -> bool:
        return self._has_column('observation_images', 'ai_crop_x1') or self._has_column('observation_images', 'ai_crop_source_w')

    def _observation_images_support_upload_metadata(self) -> bool:
        return self._has_column('observation_images', 'upload_mode') or self._has_column('observation_images', 'stored_bytes')

    def _measurement_supports_media_keys(self) -> bool:
        return self._has_column('spore_measurements', 'image_key') or self._has_column('spore_measurements', 'thumb_key')

    def _set_observation_media_keys(self, obs_cloud_id: str, storage_key: str, sort_order) -> None:
        if not obs_cloud_id:
            return
        if sort_order not in (None, 0, '0'):
            return
        if not self._observation_supports_media_keys():
            return
        normalized_key = _normalize_cloud_media_key(storage_key)
        if not normalized_key:
            return
        payload: dict[str, str] = {}
        if self._has_column('observations', 'image_key'):
            payload['image_key'] = normalized_key
        if self._has_column('observations', 'thumb_key'):
            payload['thumb_key'] = media_variant_key(normalized_key, 'thumb')
        if payload:
            self._patch(f'observations?id=eq.{obs_cloud_id}', payload)

    def _cloud_image_storage_key(self, cloud_image_id: str) -> str:
        normalized_id = str(cloud_image_id or '').strip()
        if not normalized_id:
            return ''
        if normalized_id in self._cloud_image_storage_key_cache:
            return self._cloud_image_storage_key_cache[normalized_id]
        rows = self._get(f'observation_images?id=eq.{normalized_id}&select=storage_path&limit=1')
        storage_key = _normalize_cloud_media_key((rows[0] or {}).get('storage_path') if rows else '')
        self._cloud_image_storage_key_cache[normalized_id] = storage_key
        return storage_key

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

    def fetch_current_user_id(self) -> str:
        """Return the authenticated Supabase user id for the current session."""
        user_info = self.fetch_current_user_info()
        user_id = _normalize_cloud_user_id(user_info.get('id') if isinstance(user_info, dict) else None)
        if user_id:
            if user_id != self.user_id:
                self.user_id = user_id
            return user_id
        raise CloudSyncError('Could not fetch current cloud user.')

    def fetch_current_user_info(self) -> dict:
        """Return the authenticated Supabase user record."""
        resp = self._s.get(f'{SUPABASE_URL}/auth/v1/user', timeout=15)
        if resp.ok:
            try:
                data = resp.json()
            except Exception:
                data = {}
            user_id = _normalize_cloud_user_id(data.get('id') if isinstance(data, dict) else None)
            if user_id:
                if user_id != self.user_id:
                    self.user_id = user_id
                return data if isinstance(data, dict) else {'id': user_id}
        token_user_id = _decode_jwt_subject(self.access_token)
        if token_user_id:
            if token_user_id != self.user_id:
                self.user_id = token_user_id
            return {'id': token_user_id}
        raise CloudSyncError(f'Could not fetch current cloud user: {resp.text if resp is not None else ""}')

    def fetch_profile(self) -> dict:
        rows = self._get(
            f'profiles?id=eq.{self.user_id}&select=id,username,display_name,bio,avatar_url&limit=1'
        )
        return dict(rows[0] or {}) if rows else {}

    def update_profile(
        self,
        *,
        username: str | None = None,
        display_name: str | None = None,
        bio: str | None = None,
        avatar_url: str | None = None,
    ) -> None:
        payload: dict[str, object] = {}
        if username is not None:
            normalized = str(username or '').strip().lstrip('@')
            payload['username'] = normalized or None
        if display_name is not None:
            normalized = str(display_name or '').strip()
            payload['display_name'] = normalized or None
        if bio is not None:
            normalized = str(bio or '').strip()
            payload['bio'] = normalized or None
        if avatar_url is not None:
            normalized = str(avatar_url or '').strip()
            payload['avatar_url'] = normalized or None
        if not payload:
            return
        self._patch(f'profiles?id=eq.{self.user_id}', payload)

    def upload_profile_avatar(self, jpeg_bytes: bytes) -> str:
        content = bytes(jpeg_bytes or b'')
        if not content:
            raise CloudSyncError('Missing avatar image data.')
        path = f'{self.user_id}/avatar.jpg'
        url = f'{SUPABASE_URL}/storage/v1/object/avatars/{path}'
        headers = {
            'Content-Type': 'image/jpeg',
            'x-upsert': 'true',
        }
        resp = self._s.post(url, data=content, headers=headers, timeout=30)
        if not resp.ok:
            resp = self._s.put(url, data=content, headers=headers, timeout=30)
        if not resp.ok:
            raise CloudSyncError(f'Avatar upload failed: {resp.text}')
        public_url = f'{SUPABASE_URL}/storage/v1/object/public/avatars/{path}'
        self.update_profile(avatar_url=public_url)
        return public_url

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
            path_str = _normalize_cloud_media_key(path)
            if not path_str:
                continue
            cleaned.append(path_str)

            for variant in ('thumb', 'small', 'medium'):
                cleaned.append(media_variant_key(path_str, variant))

        if not cleaned:
            return
        try:
            self._get_r2().delete_objects(cleaned)
        except Exception as exc:
            raise CloudSyncError(f'R2 delete failed: {exc}') from exc

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
        payload['visibility'] = _sharing_scope_to_cloud_visibility(obs.get('sharing_scope'), fallback='public')
        payload['is_draft'] = bool(obs.get('is_draft', True))
        payload['location_precision'] = _normalize_location_precision(obs.get('location_precision'))
        raw_vis = str(payload.get('spore_data_visibility') or 'public').strip().lower()
        payload['spore_data_visibility'] = raw_vis if raw_vis in {'private', 'friends', 'public'} else 'public'

        # Normalise SQLite 0/1 integers to proper JSON booleans
        for col in ('uncertain', 'unspontaneous', 'interesting_comment', 'location_public', 'is_draft'):
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
        payload['original_filename'] = (
            str(img.get('original_filename') or '').strip()
            or Path(img.get('filepath') or '').name
            or None
        )
        payload['storage_path']      = _normalize_cloud_media_key(storage_path)
        if payload.get('gps_source') is not None:
            payload['gps_source'] = bool(payload['gps_source'])
        if not self._observation_images_support_ai_crop():
            for key in (
                'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
                'ai_crop_source_w', 'ai_crop_source_h',
            ):
                payload.pop(key, None)
        if self._observation_images_support_upload_metadata():
            for key in _IMG_UPLOAD_META_COLS:
                payload[key] = img.get(key)

        existing_id = self._find_cloud_image(img['id'])
        if existing_id:
            self._patch(f'observation_images?id=eq.{existing_id}', payload)
            cloud_id = existing_id
        else:
            rows = self._post('observation_images', payload)
            cloud_id = rows[0]['id']
        normalized_key = _normalize_cloud_media_key(payload.get('storage_path'))
        if cloud_id and normalized_key:
            self._cloud_image_storage_key_cache[str(cloud_id)] = normalized_key
            self._set_observation_media_keys(obs_cloud_id, normalized_key, img.get('sort_order'))
        return cloud_id

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
    ) -> str | None:
        """Upload file to Cloudflare R2. Returns the relative media key or None if missing."""
        path = Path(local_path)
        if not path.exists():
            return None

        storage_path = _normalize_cloud_media_key(
            storage_path or self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)
        )
        mime = _content_type_for_path(path)
        cache_control = 'public, max-age=31536000, immutable'
        r2 = self._get_r2()

        try:
            r2.put_file(path, storage_path, content_type=mime, cache_control=cache_control, timeout=120)
        except Exception as exc:
            raise CloudSyncError(f'R2 upload failed: {exc}') from exc

        # Generate the single cloud thumbnail variant used by web and desktop.
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
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
                scale = min(1.0, _CLOUD_THUMB_MAX_EDGE / max(orig_w, orig_h))
                target_w = max(1, int(orig_w * scale))
                target_h = max(1, int(orig_h * scale))

                variant_path = media_variant_key(storage_path, 'thumb')
                img_resized = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
                buffer = io.BytesIO()
                thumb_format, thumb_mime, thumb_options = _cloud_thumb_save_format(path)
                img_resized.save(buffer, format=thumb_format, **thumb_options)
                r2.put_bytes(
                    buffer.getvalue(),
                    variant_path,
                    content_type=thumb_mime,
                    cache_control=cache_control,
                    timeout=60,
                )
        except Exception as e:
            print(f"[cloud_sync] Warning: Could not generate/upload thumbnail for {storage_path}: {e}")

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
        if self._measurement_supports_media_keys():
            storage_key = self._cloud_image_storage_key(cloud_image_id)
            if storage_key:
                if self._has_column('spore_measurements', 'image_key'):
                    payload['image_key'] = storage_key
                if self._has_column('spore_measurements', 'thumb_key'):
                    payload['thumb_key'] = media_variant_key(storage_key, 'thumb')
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
            _normalize_cloud_media_key(row.get('storage_path'))
            for row in image_rows
            if _normalize_cloud_media_key(row.get('storage_path'))
        ]
        if storage_paths:
            try:
                self._storage_remove(storage_paths)
            except CloudSyncError as exc:
                print(f'[cloud_sync] Warning: could not remove storage files for {cloud_id}: {exc}')
        self._delete(f'observation_images?observation_id=eq.{cloud_id}')
        self._delete(f'observations?id=eq.{cloud_id}')

    def download_image_file(self, storage_path: str, dest_path: str | Path) -> Path:
        """Download one cloud image from Cloudflare R2 into a local path."""
        storage_key = _normalize_cloud_media_key(storage_path)
        if not storage_key:
            raise CloudSyncError('Missing storage path')
        try:
            return self._get_r2().download_to_file(storage_key, dest_path, timeout=120)
        except Exception as exc:
            detail = str(exc or '').strip()
            if 'nosuchkey' in detail.lower():
                raise CloudSyncError(
                    f'Cloud image file is missing from storage ({storage_key})'
                ) from exc
            raise CloudSyncError(f'Download failed: {exc}') from exc


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
                    and _local_media_signatures_match(
                        stored_local_media_signature,
                        current_local_media_signature,
                    )
                ):
                    _store_local_media_signature_if_equivalent(
                        local_obs_id,
                        stored_local_media_signature,
                        current_local_media_signature,
                    )
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
        include_ai_crop = client._observation_images_support_ai_crop()
        include_upload_meta = client._observation_images_support_upload_metadata()
        for item_index, item in enumerate(prepared_items, start=1):
            img = dict(item.get('image_row') or {})
            img.update(dict(item.get('cloud_upload_meta') or {}))
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
                existing_storage_path = _normalize_cloud_media_key((remote_row or {}).get('storage_path'))
                upload_suffix = Path(upload_path).suffix.lower()
                if existing_storage_path and upload_suffix:
                    existing_parts = existing_storage_path.split('/')
                    existing_name = existing_parts[-1] if existing_parts else existing_storage_path
                    existing_suffix = Path(existing_name).suffix.lower()
                    if existing_suffix and existing_suffix != upload_suffix:
                        migrated_name = f"{Path(existing_name).stem}{upload_suffix}"
                        existing_storage_path = '/'.join([*existing_parts[:-1], migrated_name])
                storage_path = existing_storage_path or client._build_storage_path(
                    obs_cloud_id,
                    remote_cloud_id or str(local_image_id or img.get('id') or ''),
                    upload_path,
                )
                expected_payload = _prepared_item_remote_payload(
                    img,
                    upload_path,
                    storage_path,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                if remote_row and remote_row.get('original_filename'):
                    expected_payload['original_filename'] = _normalize_snapshot_value(remote_row.get('original_filename'))
                remote_payload = _remote_image_payload(
                    remote_row,
                    include_ai_crop=include_ai_crop,
                    include_upload_meta=include_upload_meta,
                )
                metadata_matches = bool(remote_row) and remote_payload == expected_payload

                current_file_sig = _file_content_signature(upload_path)
                stored_file_sig = _load_cloud_image_file_signature(obs.get('id'), local_image_id)
                file_matches = False
                if remote_row and _normalize_cloud_media_key(remote_row.get('storage_path')) == _normalize_cloud_media_key(storage_path):
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
                    if remote_row and remote_row.get('original_filename'):
                        img['original_filename'] = str(remote_row.get('original_filename') or '').strip()
                    img_cloud_id = client.push_image_metadata(img, obs_cloud_id, storage_path)
                    if (
                        not existing_storage_path
                        and storage_path != client._build_storage_path(obs_cloud_id, img_cloud_id, upload_path)
                    ):
                        storage_path = _normalize_cloud_media_key(client._build_storage_path(obs_cloud_id, img_cloud_id, upload_path))
                        client._patch(
                            f'observation_images?id=eq.{img_cloud_id}',
                            {'storage_path': storage_path},
                        )
                    elif remote_row and _normalize_cloud_media_key(remote_row.get('storage_path')) != _normalize_cloud_media_key(storage_path):
                        client._patch(
                            f'observation_images?id=eq.{img_cloud_id}',
                            {'storage_path': storage_path},
                        )
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
                if not file_matches:
                    client.upload_image_file(upload_path, obs_cloud_id, img_cloud_id, storage_path=storage_path)
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
            stale_storage_path = _normalize_cloud_media_key(stale_row.get('storage_path'))
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


def _backfill_missing_exif_on_cloud_images() -> None:
    """One-shot backfill: inject observation GPS/datetime into any field images
    that have a cloud_id but no EXIF datetime (stripped by the web app's 2 MP
    conversion).  Runs at the start of each pull; safe to call repeatedly since
    it skips files that already have EXIF.
    """
    try:
        from PIL import Image as _PilImg, ExifTags as _ET
        conn = get_connection()
        try:
            rows = conn.execute(
                '''
                SELECT i.id, i.filepath, i.observation_id, i.image_type
                FROM images i
                WHERE i.cloud_id IS NOT NULL
                  AND i.image_type != 'microscope'
                  AND i.filepath IS NOT NULL
                '''
            ).fetchall()
        finally:
            conn.close()
        for row in rows:
            filepath = str(row[1] or '').strip()
            if not filepath:
                continue
            p = Path(filepath)
            if not p.exists() or p.suffix.lower() not in {'.jpg', '.jpeg'}:
                continue
            try:
                with _PilImg.open(p) as img:
                    exif = img.getexif()
                    tags = {_ET.TAGS.get(k, k): v for k, v in exif.items()} if exif else {}
                    already_has_dt = any(
                        t in tags for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
                    )
                    try:
                        already_has_gps = bool(exif.get_ifd(0x8825))
                    except Exception:
                        already_has_gps = False
                    if already_has_dt and already_has_gps:
                        continue
            except Exception:
                continue
            obs_id = int(row[2] or 0)
            if obs_id <= 0:
                continue
            lat, lon, altitude, date_str = _load_obs_exif_fallback(obs_id)
            _inject_obs_exif_into_field_image(p, lat, lon, altitude, date_str)
    except Exception as exc:
        print(f'[cloud_sync] EXIF backfill skipped: {exc}')


def pull_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
) -> dict:
    """Pull new cloud observations and apply remote updates to clean local rows."""
    _backfill_missing_exif_on_cloud_images()
    remote_obs = list(remote_obs or client.list_remote_observations())
    pulled = 0
    errors = []
    imported_local_ids: list[int] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    local_by_cloud_id, local_by_id = _load_local_observation_lookup()
    candidates: list[tuple[dict, dict | None, str]] = []
    candidate_cloud_ids: list[str] = []
    for remote in remote_obs:
        cloud_id = str(remote.get('id') or '').strip()
        local_obs = _find_local_observation_for_remote_cached(remote, local_by_cloud_id, local_by_id)
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
        local_id = _safe_int((local_obs or {}).get('id'))
        remote_desktop_id = _safe_int(remote.get('desktop_id'))
        should_check = False
        if local_obs is None:
            should_check = True
        elif cloud_id and remote_desktop_id not in (0, local_id):
            should_check = True
        elif not stored_snapshot:
            should_check = True
        elif _remote_observation_changed_since_last_sync(local_obs, remote):
            should_check = True
        if should_check:
            candidates.append((remote, local_obs, stored_snapshot))
            if cloud_id:
                candidate_cloud_ids.append(cloud_id)

    total = len(candidates)
    _extend_progress_total(progress_state, total)
    bulk_images = client.pull_bulk_image_metadata(candidate_cloud_ids)
    remote_images_by_obs = {}
    for img in bulk_images:
        obs_id = str(img.get('observation_id') or '').strip()
        if obs_id:
            remote_images_by_obs.setdefault(obs_id, []).append(img)

    for i, (remote, local_obs, stored_snapshot) in enumerate(candidates):
        name = _observation_display_name(remote)
        cloud_id = str(remote.get('id') or '').strip()
        _emit_progress(
            progress_cb,
            f"Checking cloud observation {i + 1}/{max(1, total)}: {name}…",
            progress_state,
        )

        try:
            # DO NOT filter by should_pull_cloud_image_to_desktop here, otherwise the 
            # conflict logic falsely thinks microscope images were deleted by the cloud!
            remote_images = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])]

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
                remote_changed = (not stored_snapshot) or _remote_snapshot_has_meaningful_changes(
                    remote,
                    remote_images,
                    stored_snapshot,
                )
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
                        and not _local_media_signatures_match(
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    )
                    if not local_media_changed:
                        _store_local_media_signature_if_equivalent(
                            local_id,
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    media_conflict = bool(remote_image_changes.get('removed_keys'))
                    if local_media_changed and remote_image_changes.get('metadata_changed_keys'):
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
                    if local_media_changed and remote_image_changes.get('metadata_changed_keys'):
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
    sharing_scope = _cloud_visibility_to_sharing_scope(
        remote.get('visibility') or remote.get('sharing_scope'),
        fallback='friends' if location_public else 'public',
    )
    raw_spore_vis = str(remote.get('spore_data_visibility') or 'public').strip().lower()
    spore_data_visibility = raw_spore_vis if raw_spore_vis in {'private', 'friends', 'public'} else 'public'

    # Map cloud columns to create_observation kwargs
    remote_captured_at = str(remote.get('captured_at') or '').strip()
    kwargs = dict(
        date=remote_captured_at or remote.get('date') or datetime.now().strftime('%Y-%m-%d'),
        genus=remote.get('genus'),
        species=remote.get('species'),
        common_name=remote.get('common_name'),
        species_guess=remote.get('species_guess'),
        location=remote.get('location'),
        habitat=remote.get('habitat'),
        notes=remote.get('notes'),
        open_comment=remote.get('open_comment'),
        is_draft=bool(remote.get('is_draft', True)),
        sharing_scope=sharing_scope,
        location_public=location_public,
        location_precision=_normalize_location_precision(remote.get('location_precision')),
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
                storage_path = _normalize_cloud_media_key(image_row.get('storage_path'))
                if not storage_path:
                    continue
                cloud_image_id = str(image_row.get('id') or '').strip()
                filename = Path(str(image_row.get('original_filename') or '')).name or f'cloud_{idx}.jpg'
                download_path = temp_dir / f'{idx:02d}_{filename}'
                try:
                    client.download_image_file(storage_path, download_path)
                    download_path = _rename_to_detected_image_extension(download_path)
                except CloudSyncError as exc:
                    if _is_missing_cloud_image_error(exc):
                        print(f'[cloud_sync] Warning: {_cloud_missing_image_warning(local_id, image_row)}')
                        continue
                    raise

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
                    ai_crop_box=_remote_ai_crop_box(image_row),
                    ai_crop_source_size=_remote_ai_crop_source_size(image_row),
                    captured_at=image_row.get('captured_at') or remote.get('captured_at'),
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
                try:
                    stored = ImageDB.get_image(int(local_image_id))
                    sig_path = str((stored or {}).get('filepath') or download_path)
                    file_sig = _file_content_signature(sig_path)
                    if file_sig:
                        _store_cloud_image_file_signature(int(local_id), int(local_image_id), file_sig)
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
    ensure_database_linked_to_cloud_user(client)
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
