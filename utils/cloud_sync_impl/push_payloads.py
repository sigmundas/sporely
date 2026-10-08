"""Observation push payload serializer and observation compare payloads.

Stateless, but outside the pure package: the payload normalizers use
``database.reverse_location_lookup.normalize_country_code``. No push execution.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import re

from database.models import ObservationDB
from database.reverse_location_lookup import normalize_country_code
from utils.publish_targets import normalize_publish_target

from utils.cloud_sync_impl.reconciliation.identity import (
    _classify_identity_sync_change,
    _local_identity_sync_key,
    _remote_identity_claim,
    TAXON_IDENTITY_SYNC_FIELD,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_observation_bool_value,
    _normalize_observation_float_value,
    _normalize_observation_int_value,
    _normalize_observation_json_value,
    _normalize_snapshot_value,
    _observation_field_values_match,
    _OBSERVATION_FLOAT_FIELDS,
    _OBSERVATION_INT_FIELDS,
    _sharing_scope_to_cloud_visibility,
    _SNAPSHOT_OBS_FIELDS,
)


# Cloud contract audit:
# - Synced now: `is_draft`, `location_precision`, `ai_selected_*`, image `measure_color`
#   and `crop_mode`, and spore measurement `gallery_rotation`.
# - Future work: image `scale_bar_*`, spore measurement `notes`, `image_key`,
#   `thumb_key`, and cloud upload metadata/derived keys remain intentionally
#   out of the desktop contract for now.
# - Intentionally blocked / future work for the desktop schema: observation
#   `captured_at`, `gps_altitude`, `gps_accuracy`, and the stored
#   `observation_identifications` table.
# - Avoid user-facing conflicts for harmless reduced cloud media copies.
# Observation columns we push to cloud (excludes local-only fields)
_OBS_PUSH_COLS = [
    'date', 'genus', 'species', 'common_name', 'species_guess',
    'uncertain', 'unspontaneous', 'determination_method',
    'location', 'gps_latitude', 'gps_longitude',
    'location_public',
    'is_draft', 'location_precision',
    'ai_selected_service', 'ai_selected_taxon_id',
    'ai_selected_scientific_name', 'ai_selected_probability',
    'ai_selected_at',
    # Red-list is pushable so a desktop-derived value (fallback from an AI
    # prediction, or an explicit user pick) can round-trip to the cloud. Local
    # NULL is protected from wiping cloud values by `_merge_cloud_selected_ai_fields`
    # which fills in the remote value before push.
    'red_list_category', 'red_list_categories_json',
    'habitat', 'habitat_nin2_path', 'habitat_substrate_path',
    'habitat_host_genus', 'habitat_host_species', 'habitat_host_common_name',
    'habitat_nin2_note', 'habitat_substrate_note', 'habitat_grows_on_note',
    'notes', 'open_comment', 'interesting_comment',
    'publish_target', 'artsdata_id', 'artportalen_id',
    'inaturalist_id', 'mushroomobserver_id',
    'spore_statistics', 'auto_threshold',
    'source_type', 'citation', 'data_provider', 'author',
    'spore_data_visibility',
    # Geography: `country_code` is normalized to NULL or ^[A-Z]{2}$ before
    # sending. `region_id` is preserve-only from cloud — the desktop never
    # invents it, and outgoing PATCH payloads intentionally omit this key so
    # the cloud value survives. See `push_observation` for the patch shaping.
    'country_code',
    'region_id',
]


def _normalize_observation_field_value(field: str, value):
    if field == 'date':
        text = str(value or '').strip()
        if not text:
            return None
        if len(text) >= 10 and re.match(r'^\d{4}-\d{2}-\d{2}', text):
            return text[:10]
        return text
    if field in {'location_public', 'uncertain', 'unspontaneous', 'interesting_comment'}:
        return _normalize_observation_bool_value(value, default=None)
    if field == 'is_draft':
        return _normalize_observation_bool_value(value, default=True)
    if field in _OBSERVATION_FLOAT_FIELDS:
        return _normalize_observation_float_value(value)
    if field in _OBSERVATION_INT_FIELDS:
        return _normalize_observation_int_value(value)
    if field == 'location_precision':
        return ObservationDB._normalize_location_precision(value)
    if field == 'spore_data_visibility':
        raw = str(value or 'public').strip().lower()
        return raw if raw in {'private', 'friends', 'public'} else 'public'
    if field == 'spore_statistics':
        return _normalize_observation_json_value(value)
    if field == 'red_list_categories_json':
        # Local column is TEXT (JSON string); cloud column is JSONB (dict).
        # Compare structurally so re-pulls don't perpetually report "changed"
        # just because of the string/dict shape difference.
        return _normalize_observation_json_value(value)
    if field == 'country_code':
        return normalize_country_code(value)
    if field == 'region_id':
        if value is None:
            return None
        text = str(value).strip()
        return text or None
    return _normalize_snapshot_value(value)


def _observation_push_payload(record: dict | None, *, local: bool) -> dict:
    row = dict(record or {})
    payload = {col: row.get(col) for col in _OBS_PUSH_COLS}
    payload['date'] = _normalize_observation_field_value('date', payload.get('date'))
    scope_source = row.get('sharing_scope') if local else (row.get('visibility') or row.get('sharing_scope'))
    payload['visibility'] = _sharing_scope_to_cloud_visibility(scope_source, fallback='private')
    raw_vis = str(payload.get('spore_data_visibility') or 'public').strip().lower()
    payload['spore_data_visibility'] = raw_vis if raw_vis in {'private', 'friends', 'public'} else 'public'
    payload['location_precision'] = ObservationDB._normalize_location_precision(payload.get('location_precision'))
    payload['is_draft'] = _normalize_observation_bool_value(payload.get('is_draft'), default=True)
    for field in ('location_public', 'uncertain', 'unspontaneous', 'interesting_comment'):
        payload[field] = _normalize_observation_bool_value(payload.get(field), default=None)
    for field in _OBSERVATION_INT_FIELDS:
        payload[field] = _normalize_observation_int_value(payload.get(field))
    for field in _OBSERVATION_FLOAT_FIELDS:
        payload[field] = _normalize_observation_float_value(payload.get(field))
    payload['spore_statistics'] = _normalize_observation_json_value(payload.get('spore_statistics'))
    # red_list_categories_json is TEXT locally, JSONB on cloud. Decode the string
    # (or pass through a dict) so PostgREST sees a JSON object, not a quoted
    # string. Missing / empty values map to None so nothing gets pushed.
    payload['red_list_categories_json'] = _normalize_observation_json_value(
        payload.get('red_list_categories_json')
    )
    raw_publish_target = str(payload.get('publish_target') or '').strip()
    if raw_publish_target:
        payload['publish_target'] = normalize_publish_target(raw_publish_target)
    payload['country_code'] = normalize_country_code(payload.get('country_code'))
    payload['region_id'] = _normalize_observation_field_value('region_id', payload.get('region_id'))
    return payload


def _observation_compare_payload(record: dict | None, *, local: bool) -> dict:
    row = dict(record or {})
    payload = _observation_push_payload(row, local=local)
    payload['id'] = _normalize_observation_field_value(
        'id',
        row.get('cloud_id') if local else row.get('id'),
    )
    payload['desktop_id'] = _normalize_observation_field_value(
        'desktop_id',
        row.get('id') if local else row.get('desktop_id'),
    )
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if field not in payload:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    # The virtual identity field (never pushed as a column): what each side
    # holds, in one vocabulary. A remote row without identity columns has none.
    if local:
        payload[TAXON_IDENTITY_SYNC_FIELD] = _local_identity_sync_key(row)
    else:
        claim = _remote_identity_claim(row)
        payload[TAXON_IDENTITY_SYNC_FIELD] = claim.key if claim is not None else None
    return payload


def _baseline_observation_compare_payload(record: dict | None) -> dict:
    """Normalize a stored snapshot observation for fair comparison with live rows."""
    row = dict(record or {})
    payload = _observation_push_payload(row, local=False)
    payload['id'] = _normalize_observation_field_value('id', row.get('id'))
    payload['desktop_id'] = _normalize_observation_field_value('desktop_id', row.get('desktop_id'))
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if field not in payload:
            payload[field] = _normalize_observation_field_value(field, row.get(field))
    genus = str(payload.get('genus') or '').strip()
    species = str(payload.get('species') or '').strip()
    species_guess = str(payload.get('species_guess') or '').strip()
    derived_guess = f'{genus} {species}'.strip() if genus and species else ''
    if species_guess and derived_guess and species_guess == derived_guess:
        payload['species_guess'] = None
    # Preserve whether the snapshot recorded an identity at all: a snapshot
    # from before identity joined change detection must stay "unknown".
    if TAXON_IDENTITY_SYNC_FIELD in row:
        payload[TAXON_IDENTITY_SYNC_FIELD] = str(row.get(TAXON_IDENTITY_SYNC_FIELD) or '')
    return payload


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
        local_changed = not _observation_field_values_match(field, local_value, baseline_value)
        remote_changed = not _observation_field_values_match(field, remote_value, baseline_value)
        if local_changed and remote_changed:
            if _observation_field_values_match(field, local_value, remote_value):
                shared_same_fields.append(field)
            else:
                conflict_fields.append(field)
        elif local_changed:
            local_only_fields.append(field)
        elif remote_changed:
            remote_only_fields.append(field)

    identity_change = _classify_identity_sync_change(
        local_obs, remote_obs, baseline_obs,
        identification_locally_owned=bool(
            {'genus', 'species'} & (set(local_only_fields) | set(conflict_fields))
        ),
    )
    if identity_change == 'remote_only':
        remote_only_fields.append(TAXON_IDENTITY_SYNC_FIELD)
    elif identity_change == 'local_only':
        local_only_fields.append(TAXON_IDENTITY_SYNC_FIELD)
    elif identity_change == 'conflict':
        conflict_fields.append(TAXON_IDENTITY_SYNC_FIELD)
    elif identity_change == 'shared':
        shared_same_fields.append(TAXON_IDENTITY_SYNC_FIELD)

    return {
        'local_payload': local_payload,
        'remote_payload': remote_payload,
        'baseline_payload': baseline_payload,
        'local_only_fields': local_only_fields,
        'remote_only_fields': remote_only_fields,
        'conflict_fields': conflict_fields,
        'shared_same_fields': shared_same_fields,
    }
