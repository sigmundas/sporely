"""Read-only conflict detail for the review dialog: ``get_conflict_detail``,
the observation display name, the compared fields and their labels, and the
image-change summary helpers it alone uses.

``get_conflict_detail`` performs no cloud or local write.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S7 of the cloud-sync extraction.
"""

from __future__ import annotations

import math

from database.models import (
    ImageDB,
    MeasurementDB,
    ObservationDB,
)

from utils.cloud_sync_impl.baseline import (
    _load_cloud_observation_snapshot,
    _parse_cloud_observation_snapshot,
)
from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.conflict_plan import build_conflict_plan_baseline
from utils.cloud_sync_impl.errors import CloudSyncError
from utils.cloud_sync_impl.image_payloads import (
    _deleted_remote_image_identity_keys,
    _remote_image_payload,
)
from utils.cloud_sync_impl.image_policy import should_pull_cloud_image_to_desktop
from utils.cloud_sync_impl.location_precision import _guard_local_location_precision
from utils.cloud_sync_impl.preflight import (
    _load_local_measurement_lookup,
    _local_image_snapshot_payload,
    _locally_tombstoned_snapshot_image_identity_keys,
)
from utils.cloud_sync_impl.push_payloads import (
    _baseline_observation_compare_payload,
    _observation_compare_payload,
)
from utils.cloud_sync_impl.reconciliation.asymmetry import (
    _filter_accepted_one_sided_images,
    _filter_accepted_one_sided_measurements,
)
from utils.cloud_sync_impl.reconciliation.identity import (
    _classify_identity_sync_change,
    _local_identity_is_claim,
    _remote_identity_changed_since,
    TAXON_IDENTITY_SYNC_FIELD,
)
from utils.cloud_sync_impl.reconciliation.images import (
    _image_compare_key,
    _image_identity_keys,
    _image_metadata_payload,
)
from utils.cloud_sync_impl.reconciliation.measurements import (
    _baseline_measurement_compare_payload,
    _local_measurement_snapshot_payload,
    _measurement_field_values_match,
    _measurement_payloads_match,
    _measurement_push_diff_fields,
    _remote_measurement_snapshot_payload,
)
from utils.cloud_sync_impl.reconciliation.values import (
    _normalize_snapshot_value,
    _observation_field_values_match,
)
from utils.cloud_sync_impl.remote_reads import _pull_remote_measurements_for_images


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
    'is_draft': 'Draft state',
    'location_precision': 'Location precision',
    'spore_statistics': 'Spore statistics',
    'taxon_identity': 'Taxon identity',
}


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


def _image_kind_noun(image_row: dict | None, *, plural: bool = False) -> str:
    """Human-readable name for what kind of image a row is.

    We group by (image_type, micro_category) so summaries can say
    "2 microscope photos" instead of listing filenames the user cannot
    map back to what they saw in the app.
    """
    row = dict(image_row or {})
    image_type = str(row.get('image_type') or '').strip().lower()
    micro_category = str(row.get('micro_category') or '').strip().lower()
    if image_type == 'microscope':
        if micro_category == 'spore':
            singular = 'spore photo'
        elif micro_category:
            singular = f'{micro_category} microscope photo'
        else:
            singular = 'microscope photo'
    elif image_type == 'field':
        singular = 'field photo'
    elif image_type:
        singular = f'{image_type} photo'
    else:
        singular = 'photo'
    return singular + ('s' if plural and not singular.endswith('s') else '')


def _pluralize_image_count(rows: list[dict]) -> str:
    """Turn a list of image rows into 'N x' where x collapses by kind.

    Examples: '2 microscope photos'; '1 field photo and 1 spore photo';
    '3 photos' when kinds are mixed and there are many.
    """
    if not rows:
        return ''
    buckets: dict[str, list[dict]] = {}
    order: list[str] = []
    for row in rows:
        key = _image_kind_noun(row, plural=False)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(row)
    parts: list[str] = []
    for key in order:
        count = len(buckets[key])
        noun = key if count == 1 else (key + 's' if not key.endswith('s') else key)
        parts.append(f'{count} {noun}')
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f'{parts[0]} and {parts[1]}'
    return ', '.join(parts[:-1]) + f', and {parts[-1]}'


_IMAGE_METADATA_FIELD_GROUPS = (
    (
        'the AI crop',
        {
            'ai_crop_x1',
            'ai_crop_y1',
            'ai_crop_x2',
            'ai_crop_y2',
            'ai_crop_source_w',
            'ai_crop_source_h',
            'ai_crop_is_custom',
            'crop_mode',
        },
    ),
    (
        'the microscope calibration',
        {
            'calibration_uuid',
            'objective_name',
            'scale_microns_per_pixel',
            'resample_scale_factor',
        },
    ),
    (
        'the microscope settings',
        {
            'mount_medium',
            'stain',
            'sample_type',
            'contrast',
            'measure_color',
        },
    ),
    ('the notes', {'notes'}),
    ('the photo type', {'image_type', 'micro_category'}),
    ('the GPS source', {'gps_source'}),
)


def _image_metadata_group_for_field(field: str) -> str:
    normalized = str(field or '').strip()
    for label, members in _IMAGE_METADATA_FIELD_GROUPS:
        if normalized in members:
            return label
    return _format_image_metadata_field_label(normalized)


def _format_image_metadata_field_label(field: str) -> str:
    labels = {
        'captured_at': 'capture time',
        'measure_color': 'measurement color',
        'crop_mode': 'crop mode',
        'ai_crop_x1': 'AI crop left',
        'ai_crop_y1': 'AI crop top',
        'ai_crop_x2': 'AI crop right',
        'ai_crop_y2': 'AI crop bottom',
        'ai_crop_source_w': 'AI crop source width',
        'ai_crop_source_h': 'AI crop source height',
        'ai_crop_is_custom': 'custom crop',
    }
    return labels.get(field, field.replace('_', ' '))


def _summarize_image_changes(
    current_images: list[dict],
    baseline_images: list[dict],
    *,
    ignored_keys: set[str] | None = None,
) -> list[str]:
    """Return short, user-facing lines describing what changed to images.

    We group by "kind of change" (crop, notes, calibration) and by
    "kind of image" (microscope photo, field photo) rather than listing
    raw filenames — the user sees results in the app UI, they don't
    remember `img_1.webp`.
    """
    current = [dict(row or {}) for row in (current_images or [])]
    baseline = [dict(row or {}) for row in (baseline_images or [])]
    ignored = {str(key or '').strip() for key in (ignored_keys or set()) if str(key or '').strip()}
    current_keys = [_image_compare_key(row) for row in current]
    baseline_keys = [_image_compare_key(row) for row in baseline]
    current_map = {_image_compare_key(row): row for row in current}
    baseline_map = {_image_compare_key(row): row for row in baseline}

    added = [current_map[key] for key in current_keys if key not in baseline_map and key not in ignored]
    removed = [baseline_map[key] for key in baseline_keys if key not in current_map and key not in ignored]
    shared_keys = [key for key in current_keys if key in baseline_map and key not in ignored]

    lines: list[str] = []

    # {group_label: [image_row, ...]} — each image row is only recorded
    # once per group even when several fields inside the group changed.
    change_buckets: dict[str, list[dict]] = {}
    change_order: list[str] = []
    for key in shared_keys:
        c_meta = _image_metadata_payload(current_map[key])
        b_meta = _image_metadata_payload(baseline_map[key])
        if c_meta == b_meta:
            continue
        seen_groups: set[str] = set()
        for field, value in c_meta.items():
            if value == b_meta.get(field):
                continue
            group = _image_metadata_group_for_field(field)
            if group in seen_groups:
                continue
            seen_groups.add(group)
            if group not in change_buckets:
                change_buckets[group] = []
                change_order.append(group)
            change_buckets[group].append(current_map[key])

    if added:
        # This is a comparison with the stored sync snapshot, not evidence
        # that a particular app or device originally added the photo. Older
        # observation-only snapshots may contain no photo history at all.
        lines.append(
            f'Present now but not recorded in the previous sync: '
            f'{_pluralize_image_count(added)}.'
        )
    if removed:
        lines.append(f'Removed {_pluralize_image_count(removed)}.')
    for group in change_order:
        rows = change_buckets[group]
        lines.append(f'Changed {group} on {_pluralize_image_count(rows)}.')
    if not lines and len(current) != len(baseline) and not ignored:
        lines.append(f'Photo count went from {len(baseline)} to {len(current)}.')
    return lines


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


_MEASUREMENT_PRESENTATION_FIELDS = ('gallery_rotation',)


def get_conflict_detail(client: "SporelyCloudClient", local_id: int, cloud_id: str | None = None) -> dict:
    """Enhanced conflict details with filename-level media differences."""
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    remote_obs = client.get_observation(resolved_cloud_id)

    remote_images_raw = client.pull_image_metadata(resolved_cloud_id, include_deleted_for_sync=True) or []
    snapshot = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(resolved_cloud_id))
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]
    tombstoned_remote_image_keys = (
        _deleted_remote_image_identity_keys(remote_images_raw)
        | _locally_tombstoned_snapshot_image_identity_keys(baseline_images)
    )
    remote_images = [
        dict(row or {})
        for row in remote_images_raw
        if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
    ]
    remote_measurements = _pull_remote_measurements_for_images(
        client,
        [str(row.get('id') or '').strip() for row in remote_images if str(row.get('id') or '').strip()],
    )

    # 1. Field comparisons — simplified sync model.
    #
    # The dialog receives ONLY genuine two-sided divergence.  One-sided
    # changes (local moved, cloud unchanged — or vice versa) are handled by
    # ordinary sync and merely reported in ``automatic_decisions``.  The
    # ``is_draft`` field never opens the dialog; if both sides changed
    # differently it resolves automatically to Draft (the safer state) and is
    # also reported in ``automatic_decisions``.
    local_obs = _guard_local_location_precision(local_obs, baseline_obs, remote_obs)
    local_payload = _observation_compare_payload(local_obs, local=True)
    remote_payload = _observation_compare_payload(remote_obs, local=False)
    field_rows: list[dict] = []
    automatic_field_decisions: list[dict] = []
    for field in _CONFLICT_COMPARE_FIELDS:
        l_val = local_payload.get(field)
        r_val = remote_payload.get(field)
        b_val = baseline_obs.get(field)
        # (a) already converged — no work.
        if _observation_field_values_match(field, l_val, r_val):
            continue
        local_changed = not _observation_field_values_match(field, l_val, b_val)
        remote_changed = not _observation_field_values_match(field, r_val, b_val)
        # (b) only local changed → push local automatically.
        if local_changed and not remote_changed:
            automatic_field_decisions.append({
                'field': field, 'action': 'push_local',
                'local': l_val, 'remote': r_val, 'baseline': b_val,
            })
            continue
        # (c) only cloud changed → pull cloud automatically.
        if remote_changed and not local_changed:
            automatic_field_decisions.append({
                'field': field, 'action': 'pull_cloud',
                'local': l_val, 'remote': r_val, 'baseline': b_val,
            })
            continue
        # (d) is_draft: both changed differently → automatically choose Draft.
        if field == 'is_draft' and local_changed and remote_changed:
            # Draft = truthy 'is_draft'.  Prefer whichever side is Draft.
            local_is_draft = bool(l_val)
            remote_is_draft = bool(r_val)
            chosen_side = 'local' if local_is_draft else 'cloud'
            if remote_is_draft and not local_is_draft:
                chosen_side = 'cloud'
            elif local_is_draft and not remote_is_draft:
                chosen_side = 'local'
            elif not local_is_draft and not remote_is_draft:
                # Both flipped to Published — treat as converged Published.
                # (This can only happen if the values reported by
                # _observation_field_values_match differ despite both being
                # falsy; extremely unlikely but handled defensively.)
                chosen_side = 'local'
            automatic_field_decisions.append({
                'field': field,
                'action': 'auto_draft_wins' if (local_is_draft or remote_is_draft) else 'converged',
                'chosen_side': chosen_side,
                'local': l_val, 'remote': r_val, 'baseline': b_val,
            })
            continue
        # (e) genuine two-sided divergence — user must choose.
        field_rows.append({
            'field': field,
            'label': _CONFLICT_FIELD_LABELS.get(field, field.replace('_', ' ').title()),
            'baseline': b_val, 'local': l_val, 'remote': r_val,
            'local_changed': local_changed, 'remote_changed': remote_changed,
        })

    # The taxonomy identity is classified by its own three-way rule, never by
    # plain equality: a cloud-derived local token is not a local edit, and an
    # identity follows the identification (genus/species) it names.
    identity_change = _classify_identity_sync_change(
        local_obs, remote_obs, baseline_obs,
        identification_locally_owned=any(
            row['field'] in {'genus', 'species'} for row in field_rows
        ) or any(
            entry['field'] in {'genus', 'species'} and entry['action'] == 'push_local'
            for entry in automatic_field_decisions
        ),
    )
    identity_values = {
        'local': local_payload.get(TAXON_IDENTITY_SYNC_FIELD),
        'remote': remote_payload.get(TAXON_IDENTITY_SYNC_FIELD),
        'baseline': baseline_obs.get(TAXON_IDENTITY_SYNC_FIELD),
    }
    if identity_change == 'local_only':
        automatic_field_decisions.append({
            'field': TAXON_IDENTITY_SYNC_FIELD, 'action': 'push_local', **identity_values,
        })
    elif identity_change == 'remote_only':
        automatic_field_decisions.append({
            'field': TAXON_IDENTITY_SYNC_FIELD, 'action': 'pull_cloud', **identity_values,
        })
    elif identity_change == 'conflict':
        field_rows.append({
            'field': TAXON_IDENTITY_SYNC_FIELD,
            'label': _CONFLICT_FIELD_LABELS[TAXON_IDENTITY_SYNC_FIELD],
            **identity_values,
            'local_changed': _local_identity_is_claim(local_obs),
            'remote_changed': _remote_identity_changed_since(remote_obs, baseline_obs),
        })

    # 2. Detailed Image Differences
    local_images_raw = ImageDB.get_images_for_observation(int(local_id))
    local_image_payloads = [_local_image_snapshot_payload(img) for img in local_images_raw]
    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]

    # Conflict review deliberately uses identity stricter than the historical
    # snapshot compare key.  A filename/type/order resemblance is useful as a
    # suggestion, but is never authoritative enough to put two images in the
    # same row or to drive a resolution.
    remote_by_cloud_id = {
        str(row.get('id') or '').strip(): (raw, row)
        for raw, row in zip(remote_images, remote_image_payloads)
        if str(row.get('id') or '').strip()
    }
    remote_by_desktop_id: dict[int, list[tuple[dict, dict]]] = {}
    for raw, row in zip(remote_images, remote_image_payloads):
        desktop_id = _safe_int(row.get('desktop_id'))
        if desktop_id > 0:
            remote_by_desktop_id.setdefault(desktop_id, []).append((raw, row))

    local_cloud_id_counts: dict[str, int] = {}
    for row in local_images_raw:
        linked_cloud_id = str(row.get('cloud_id') or '').strip()
        if linked_cloud_id:
            local_cloud_id_counts[linked_cloud_id] = local_cloud_id_counts.get(linked_cloud_id, 0) + 1

    paired_remote_ids: set[str] = set()
    image_pairs: list[dict] = []
    local_pair_by_id: dict[int, dict] = {}
    remote_pair_by_id: dict[str, dict] = {}

    def _review_image(raw: dict | None, payload: dict | None, *, local: bool) -> dict | None:
        if not raw or not payload:
            return None
        result = dict(payload)
        if local:
            result.update({
                'local_id': _safe_int(raw.get('id')),
                'cloud_id': str(raw.get('cloud_id') or '').strip() or None,
                # Internal rendering input.  The dialog never displays it.
                'thumbnail_source': str(raw.get('filepath') or '').strip() or None,
            })
        else:
            result.update({
                'cloud_id': str(raw.get('id') or '').strip() or None,
                'local_id': _safe_int(raw.get('desktop_id')) or None,
                'thumbnail_source': _normalize_cloud_media_key(raw.get('storage_path')) or None,
            })
        return result

    for raw, payload in zip(local_images_raw, local_image_payloads):
        if _image_identity_keys(payload) & tombstoned_remote_image_keys:
            continue
        local_image_id = _safe_int(raw.get('id'))
        local_cloud_id = str(raw.get('cloud_id') or '').strip()
        match: tuple[dict, dict] | None = None
        match_basis = ''
        identity_reasons: list[str] = []
        cloud_match = remote_by_cloud_id.get(local_cloud_id) if local_cloud_id else None
        desktop_candidates = remote_by_desktop_id.get(local_image_id, []) if local_image_id > 0 else []
        if local_cloud_id and local_cloud_id_counts.get(local_cloud_id, 0) > 1:
            identity_reasons.append('multiple local images share the same cloud ID')
        if len(desktop_candidates) > 1:
            identity_reasons.append('multiple cloud images reference this local image ID')
        if cloud_match:
            cloud_desktop_id = _safe_int(cloud_match[1].get('desktop_id'))
            if cloud_desktop_id > 0 and cloud_desktop_id != local_image_id:
                identity_reasons.append('the linked cloud image references another local image')
        if cloud_match and len(desktop_candidates) == 1:
            desktop_cloud_id = str(desktop_candidates[0][1].get('id') or '').strip()
            if desktop_cloud_id and desktop_cloud_id != local_cloud_id:
                identity_reasons.append('cloud ID and desktop ID identify different cloud images')
        if cloud_match and str(cloud_match[1].get('id') or '').strip() in paired_remote_ids:
            identity_reasons.append('the cloud image is already paired with another local image')
        if identity_reasons:
            remote_raw, remote_payload = cloud_match or (
                desktop_candidates[0] if len(desktop_candidates) == 1 else (None, None)
            )
            remote_id = str((remote_payload or {}).get('id') or '').strip()
            if remote_id:
                paired_remote_ids.add(remote_id)
            pair = {
                'pairing': 'identity_conflict',
                'match_basis': None,
                'identity_conflict_reasons': identity_reasons,
                'local': _review_image(raw, payload, local=True),
                'remote': _review_image(remote_raw, remote_payload, local=False),
                'status': 'identity_conflict',
            }
            image_pairs.append(pair)
            if local_image_id > 0:
                local_pair_by_id[local_image_id] = pair
            if remote_id:
                remote_pair_by_id[remote_id] = pair
            continue
        if local_cloud_id and local_cloud_id in remote_by_cloud_id:
            match = remote_by_cloud_id[local_cloud_id]
            match_basis = 'cloud_id'
        elif local_image_id > 0:
            candidates = [
                candidate for candidate in remote_by_desktop_id.get(local_image_id, [])
                if str(candidate[1].get('id') or '').strip() not in paired_remote_ids
            ]
            if len(candidates) == 1:
                match = candidates[0]
                match_basis = 'desktop_id'
        remote_raw, remote_payload = match if match else (None, None)
        remote_id = str((remote_payload or {}).get('id') or '').strip()
        if remote_id:
            paired_remote_ids.add(remote_id)
        pair = {
            'pairing': 'authoritative' if match else 'unpaired',
            'match_basis': match_basis or None,
            'local': _review_image(raw, payload, local=True),
            'remote': _review_image(remote_raw, remote_payload, local=False),
        }
        image_pairs.append(pair)
        if local_image_id > 0:
            local_pair_by_id[local_image_id] = pair
        if remote_id:
            remote_pair_by_id[remote_id] = pair

    for raw, payload in zip(remote_images, remote_image_payloads):
        remote_id = str(payload.get('id') or '').strip()
        if remote_id in paired_remote_ids:
            continue
        pair = {
            'pairing': 'unpaired',
            'match_basis': None,
            'local': None,
            'remote': _review_image(raw, payload, local=False),
        }
        image_pairs.append(pair)
        if remote_id:
            remote_pair_by_id[remote_id] = pair

    # Unique fallback resemblances are displayed only as warnings.  They stay
    # as two distinct unpaired cards and never affect sync identity.
    unpaired_local = [row for row in image_pairs if row.get('local') and not row.get('remote')]
    unpaired_remote = [row for row in image_pairs if row.get('remote') and not row.get('local')]
    for local_pair in unpaired_local:
        local_image = dict(local_pair.get('local') or {})
        candidates = []
        for remote_pair in unpaired_remote:
            remote_image = dict(remote_pair.get('remote') or {})
            if (
                str(local_image.get('original_filename') or '').casefold()
                and str(local_image.get('original_filename') or '').casefold()
                == str(remote_image.get('original_filename') or '').casefold()
                and str(local_image.get('image_type') or '') == str(remote_image.get('image_type') or '')
                and local_image.get('sort_order') == remote_image.get('sort_order')
            ):
                candidates.append(remote_pair)
        if len(candidates) == 1:
            remote_pair = candidates[0]
            local_pair['possible_counterpart'] = {
                'cloud_id': (remote_pair.get('remote') or {}).get('cloud_id'),
                'reason': 'filename, image type, and sort position match',
            }
            remote_pair['possible_counterpart'] = {
                'local_id': local_image.get('local_id'),
                'reason': 'filename, image type, and sort position match',
            }

    _, local_measurements_by_id = _load_local_measurement_lookup(int(local_id))
    local_measurements = [dict(row or {}) for row in local_measurements_by_id.values()]
    baseline_measurements = [dict(row or {}) for row in (snapshot.get('measurements') or [])]
    baseline_by_cloud_id = {
        str(row.get('id') or '').strip(): row
        for row in baseline_measurements if str(row.get('id') or '').strip()
    }
    baseline_by_desktop_id = {
        _safe_int(row.get('desktop_id')): row
        for row in baseline_measurements if _safe_int(row.get('desktop_id')) > 0
    }
    remote_measurements_by_id = {
        str(row.get('id') or '').strip(): dict(row)
        for row in remote_measurements if str(row.get('id') or '').strip()
    }
    remote_measurements_by_desktop_id: dict[int, list[dict]] = {}
    for row in remote_measurements:
        desktop_id = _safe_int(row.get('desktop_id'))
        if desktop_id > 0:
            remote_measurements_by_desktop_id.setdefault(desktop_id, []).append(dict(row))
    local_measurement_cloud_counts: dict[str, int] = {}
    for row in local_measurements:
        measurement_cloud_id = str(row.get('cloud_id') or '').strip()
        if measurement_cloud_id:
            local_measurement_cloud_counts[measurement_cloud_id] = (
                local_measurement_cloud_counts.get(measurement_cloud_id, 0) + 1
            )

    geometry_fields = tuple(
        f'p{point}_{axis}' for point in range(1, 5) for axis in ('x', 'y')
    )

    def _geometry_available(values: dict | None) -> bool:
        return any((values or {}).get(field) is not None for field in geometry_fields)

    def _geometry_change_summary(current: dict | None, baseline: dict | None) -> str:
        if baseline is None:
            return 'Current geometry' if _geometry_available(current) else 'No geometry'
        changed_length = any(
            not _measurement_field_values_match(field, (current or {}).get(field), baseline.get(field))
            for field in ('p1_x', 'p1_y', 'p2_x', 'p2_y')
        )
        changed_width = any(
            not _measurement_field_values_match(field, (current or {}).get(field), baseline.get(field))
            for field in ('p3_x', 'p3_y', 'p4_x', 'p4_y')
        )
        if changed_length and changed_width:
            return 'Length and width axes moved'
        if changed_length:
            return 'Length axis moved'
        if changed_width:
            return 'Width axis moved'
        return 'Unchanged'

    def _measurement_review_pair(
        local_measurement: dict | None,
        remote_measurement: dict | None,
        *,
        status: str,
        pairing: str,
        identity_reasons: list[str] | None = None,
    ) -> dict:
        local_row = dict(local_measurement or {})
        remote_row = dict(remote_measurement or {})
        local_id_value = _safe_int(local_row.get('id'))
        remote_id_value = str(remote_row.get('id') or '').strip()
        baseline_row = (
            baseline_by_cloud_id.get(remote_id_value)
            or baseline_by_cloud_id.get(str(local_row.get('cloud_id') or '').strip())
            or baseline_by_desktop_id.get(local_id_value)
        )
        local_values = _local_measurement_snapshot_payload(local_row) if local_row else None
        remote_values = _remote_measurement_snapshot_payload(remote_row) if remote_row else None
        baseline_values = (
            _baseline_measurement_compare_payload(baseline_row) if baseline_row else None
        )
        local_geometry = _geometry_change_summary(local_values, baseline_values)
        remote_geometry = _geometry_change_summary(remote_values, baseline_values)
        if (
            baseline_values
            and local_values
            and remote_values
            and local_geometry == remote_geometry
            and local_geometry not in {'Unchanged', 'No geometry'}
            and any(
                not _measurement_field_values_match(
                    field, local_values.get(field), remote_values.get(field)
                )
                for field in geometry_fields
            )
        ):
            remote_geometry += ' differently'
        result = {
            'pairing': pairing,
            'status': status,
            'local_id': local_id_value or None,
            'cloud_id': remote_id_value or str(local_row.get('cloud_id') or '').strip() or None,
            'local_image_id': _safe_int(local_row.get('image_id')) or None,
            'cloud_image_id': str(remote_row.get('image_id') or '').strip() or None,
            'local_values': local_values,
            'remote_values': remote_values,
            'baseline_values': baseline_values,
            'baseline_available': baseline_values is not None,
            'geometry_baseline': (
                'Original geometry' if _geometry_available(baseline_values) else 'No geometry'
            ) if baseline_values is not None else 'Previous baseline unavailable',
            'geometry_local': local_geometry if local_values else 'Missing',
            'geometry_cloud': remote_geometry if remote_values else 'Missing',
            'identity_conflict_reasons': list(identity_reasons or []),
            'presentation_differences': [
                {
                    'field': field,
                    'local': (local_values or {}).get(field),
                    'remote': (remote_values or {}).get(field),
                    'automatic_policy': 'local_desktop',
                }
                for field in _MEASUREMENT_PRESENTATION_FIELDS
                if local_values and remote_values and not _measurement_field_values_match(
                    field, local_values.get(field), remote_values.get(field)
                )
            ],
        }
        if local_values and remote_values:
            local_image = next(
                (
                    row for row in local_images_raw
                    if _safe_int(row.get('id')) == _safe_int(local_row.get('image_id'))
                ),
                {},
            )
            diff_fields = _measurement_push_diff_fields(
                local_row,
                remote_row,
                cloud_image_id=str(local_image.get('cloud_id') or '').strip(),
            )
            result['fields'] = diff_fields
            if baseline_values is None:
                result['change_origin'] = 'baseline_unavailable'
            else:
                local_changed = any(
                    not _measurement_field_values_match(
                        field, local_values.get(field), baseline_values.get(field)
                    )
                    for field in diff_fields
                )
                remote_changed = any(
                    not _measurement_field_values_match(
                        field, remote_values.get(field), baseline_values.get(field)
                    )
                    for field in diff_fields
                )
                result['change_origin'] = (
                    'both' if local_changed and remote_changed
                    else 'local' if local_changed
                    else 'cloud' if remote_changed
                    else 'unknown'
                )
        else:
            result['fields'] = []
            result['change_origin'] = (
                'added_local' if local_values else 'added_cloud'
            )
        return result

    measurement_pairs: list[dict] = []
    paired_remote_measurement_ids: set[str] = set()
    for local_measurement in local_measurements:
        local_measurement_id = _safe_int(local_measurement.get('id'))
        linked_cloud_id = str(local_measurement.get('cloud_id') or '').strip()
        cloud_match = remote_measurements_by_id.get(linked_cloud_id) if linked_cloud_id else None
        desktop_candidates = remote_measurements_by_desktop_id.get(local_measurement_id, [])
        identity_reasons: list[str] = []
        if linked_cloud_id and local_measurement_cloud_counts.get(linked_cloud_id, 0) > 1:
            identity_reasons.append('multiple local measurements share the same cloud ID')
        if len(desktop_candidates) > 1:
            identity_reasons.append('multiple cloud measurements reference this local measurement ID')
        if cloud_match:
            remote_desktop_id = _safe_int(cloud_match.get('desktop_id'))
            if remote_desktop_id > 0 and remote_desktop_id != local_measurement_id:
                identity_reasons.append('the linked cloud measurement references another local measurement')
        if cloud_match and len(desktop_candidates) == 1:
            desktop_cloud_id = str(desktop_candidates[0].get('id') or '').strip()
            if desktop_cloud_id and desktop_cloud_id != linked_cloud_id:
                identity_reasons.append('cloud ID and desktop ID identify different measurements')
        match = cloud_match
        pairing = 'cloud_id' if cloud_match else ''
        if match is None and len(desktop_candidates) == 1:
            match = desktop_candidates[0]
            pairing = 'desktop_id'
        matched_remote_id = str((match or {}).get('id') or '').strip()
        if matched_remote_id in paired_remote_measurement_ids:
            identity_reasons.append('the cloud measurement is already paired with another local measurement')
        if identity_reasons:
            status = 'identity_conflict'
            pairing = 'identity_conflict'
        elif match is None:
            status = 'local_only'
            pairing = 'unpaired'
        else:
            local_image = next(
                (
                    row for row in local_images_raw
                    if _safe_int(row.get('id')) == _safe_int(local_measurement.get('image_id'))
                ),
                {},
            )
            status = (
                'same'
                if _measurement_payloads_match(
                    local_measurement,
                    match,
                    cloud_image_id=str(local_image.get('cloud_id') or '').strip(),
                )
                else 'values_differ'
            )
            paired_remote_measurement_ids.add(matched_remote_id)
        measurement_pairs.append(_measurement_review_pair(
            local_measurement,
            match,
            status=status,
            pairing=pairing,
            identity_reasons=identity_reasons,
        ))

    for remote_measurement in remote_measurements:
        remote_measurement_id = str(remote_measurement.get('id') or '').strip()
        if remote_measurement_id in paired_remote_measurement_ids:
            continue
        if any(
            pair.get('cloud_id') == remote_measurement_id
            for pair in measurement_pairs
            if pair.get('status') == 'identity_conflict'
        ):
            continue
        measurement_pairs.append(_measurement_review_pair(
            None,
            remote_measurement,
            status='cloud_only',
            pairing='unpaired',
        ))

    measurement_differences = [
        pair for pair in measurement_pairs if pair.get('status') != 'same'
    ]
    for measurement_pair in measurement_pairs:
        image_pair = (
            local_pair_by_id.get(_safe_int(measurement_pair.get('local_image_id')))
            or remote_pair_by_id.get(str(measurement_pair.get('cloud_image_id') or ''))
        )
        if image_pair is not None:
            image_pair.setdefault('measurement_pairs', []).append(measurement_pair)
            if measurement_pair.get('status') != 'same':
                image_pair.setdefault('measurement_conflicts', []).append(measurement_pair)

    local_measurement_counts: dict[int, int] = {}
    for row in local_measurements_by_id.values():
        image_id = _safe_int(row.get('image_id'))
        local_measurement_counts[image_id] = local_measurement_counts.get(image_id, 0) + 1
    remote_measurement_counts: dict[str, int] = {}
    for row in remote_measurements:
        image_id = str(row.get('image_id') or '').strip()
        remote_measurement_counts[image_id] = remote_measurement_counts.get(image_id, 0) + 1

    image_metadata_fields = (
        'image_type', 'micro_category', 'objective_name',
        'scale_microns_per_pixel', 'mount_medium', 'stain', 'sample_type', 'sample_source',
        'contrast', 'notes', 'gps_source', 'crop_mode',
    )
    def _review_image_values_match(left, right) -> bool:
        if left in (None, '') and right in (None, ''):
            return True
        try:
            return math.isclose(float(left), float(right), rel_tol=1e-6, abs_tol=1e-6)
        except (TypeError, ValueError):
            return _normalize_snapshot_value(left) == _normalize_snapshot_value(right)

    for pair in image_pairs:
        local_image = pair.get('local') or {}
        remote_image = pair.get('remote') or {}
        if local_image:
            local_image['measurement_count'] = local_measurement_counts.get(
                _safe_int(local_image.get('local_id')), 0
            )
        if remote_image:
            remote_image['measurement_count'] = remote_measurement_counts.get(
                str(remote_image.get('cloud_id') or ''), 0
            )

        metadata_diff: list[str] = []
        if local_image and remote_image:
            pair['presentation_differences'] = [
                {
                    'field': 'sort_order',
                    'local': local_image.get('sort_order'),
                    'remote': remote_image.get('sort_order'),
                    'automatic_policy': 'local_desktop',
                }
            ] if not _review_image_values_match(
                local_image.get('sort_order'), remote_image.get('sort_order')
            ) else []
            metadata_diff = [
                field for field in image_metadata_fields
                if not _review_image_values_match(local_image.get(field), remote_image.get(field))
            ]
            pair['metadata_diff_fields'] = metadata_diff
            baseline_image = next(
                (
                    row for row in baseline_images
                    if (
                        str(row.get('id') or '').strip()
                        and str(row.get('id') or '').strip()
                        == str(remote_image.get('cloud_id') or '').strip()
                    ) or (
                        _safe_int(row.get('desktop_id')) > 0
                        and _safe_int(row.get('desktop_id')) == _safe_int(local_image.get('local_id'))
                    )
                ),
                None,
            )
            metadata_details = []
            for field in metadata_diff:
                baseline_value = (baseline_image or {}).get(field)
                local_changed = (
                    baseline_image is None
                    or not _review_image_values_match(local_image.get(field), baseline_value)
                )
                cloud_changed = (
                    baseline_image is None
                    or not _review_image_values_match(remote_image.get(field), baseline_value)
                )
                metadata_details.append({
                    'field': field,
                    'baseline': baseline_value,
                    'local': local_image.get(field),
                    'remote': remote_image.get(field),
                    'change_origin': (
                        'baseline_unavailable' if baseline_image is None
                        else 'both' if local_changed and cloud_changed
                        else 'local' if local_changed
                        else 'cloud' if cloud_changed
                        else 'unknown'
                    ),
                })
            pair['metadata_diff_details'] = metadata_details

        if pair.get('pairing') == 'identity_conflict':
            pair['status'] = 'identity_conflict'
        elif pair.get('measurement_conflicts'):
            pair['status'] = 'measurements_differ'
        elif metadata_diff:
            pair['status'] = 'metadata_differs'
        elif local_image and remote_image:
            pair['status'] = 'same'
        elif local_image:
            pair['status'] = 'possible_match' if pair.get('possible_counterpart') else 'local_only'
        else:
            pair['status'] = 'possible_match' if pair.get('possible_counterpart') else 'cloud_only'

    image_pairs.sort(key=lambda pair: (
        0 if str(((pair.get('local') or pair.get('remote') or {}).get('image_type') or '')).lower() == 'field' else 1,
        _safe_int((pair.get('local') or pair.get('remote') or {}).get('sort_order')),
        _safe_int((pair.get('local') or {}).get('local_id')),
        str((pair.get('remote') or {}).get('cloud_id') or ''),
    ))

    image_mismatches = [
        {
            'filename': _image_label(pair.get('local') or pair.get('remote')),
            'status': pair.get('status'),
        }
        for pair in image_pairs
        if pair.get('status') in {'local_only', 'cloud_only', 'possible_match', 'identity_conflict'}
    ]

    measurement_data_available = bool(local_measurements or remote_measurements)
    derived_statistics_rows = [
        row for row in field_rows if row.get('field') == 'spore_statistics'
    ]
    if derived_statistics_rows:
        field_rows = [row for row in field_rows if row.get('field') != 'spore_statistics']
    derived_statistics = {
        'status': (
            'recompute_from_measurements'
            if measurement_data_available
            else 'diagnostic_without_measurements'
        ),
        'rows': derived_statistics_rows,
    } if derived_statistics_rows else None

    # ── B3: filter out accepted-asymmetry items whose fingerprint is unchanged ──
    accepted_asymmetry = snapshot.get('accepted_asymmetry') if isinstance(snapshot, dict) else None
    local_meas_all_raw = list(MeasurementDB.get_measurements_for_observation(int(local_id)) or [])
    image_pairs, filtered_image_pair_ids = _filter_accepted_one_sided_images(
        image_pairs, accepted_asymmetry, local_images_raw, remote_images,
    )
    measurement_pairs, filtered_measurement_pair_ids = _filter_accepted_one_sided_measurements(
        measurement_pairs, accepted_asymmetry, local_meas_all_raw, remote_measurements,
    )
    # If a paired measurement is filtered we may still have its parent image
    # pair; drop empty measurement_conflicts referring to filtered rows.
    measurement_differences = [
        diff for diff in measurement_differences
        if (
            _safe_int(diff.get('local_id')), str(diff.get('cloud_id') or '').strip()
        ) not in filtered_measurement_pair_ids
    ]

    # ── Simplified sync model: drop additive one-sided items from the dialog ──
    #
    # A local-only image or measurement without an explicit tombstone is an
    # additive change handled by ordinary sync (upload/import automatic).  A
    # cloud-only image/measurement is likewise additive on the cloud side.
    # We keep such pairs ONLY when identity is ambiguous or a real error
    # blocks safe automatic handling (indicated by ``status`` in
    # {'identity_conflict', 'possible_match'} or a ``pairing`` of
    # ``identity_conflict``).  Everything else surfaces as an automatic
    # decision so the dialog does not falsely present it as a conflict.
    def _pair_is_ambiguous(pair: dict) -> bool:
        return (
            pair.get('pairing') == 'identity_conflict'
            or pair.get('status') in {'identity_conflict', 'possible_match'}
        )

    automatic_media_decisions: list[dict] = []
    kept_image_pairs: list[dict] = []
    for pair in image_pairs:
        status = pair.get('status')
        if status == 'local_only' and not _pair_is_ambiguous(pair):
            local_side = dict(pair.get('local') or {})
            automatic_media_decisions.append({
                'kind': 'image', 'side': 'local_only',
                'action': 'upload_automatic',
                'local_id': _safe_int(local_side.get('local_id')) or None,
                'cloud_id': None,
            })
            continue
        if status == 'cloud_only' and not _pair_is_ambiguous(pair):
            remote_side = dict(pair.get('remote') or {})
            automatic_media_decisions.append({
                'kind': 'image', 'side': 'cloud_only',
                'action': 'download_automatic',
                'local_id': None,
                'cloud_id': str(remote_side.get('cloud_id') or '').strip() or None,
            })
            continue
        # For authoritatively-paired images with metadata_diff, drop the diff
        # entirely unless BOTH sides changed relative to baseline.  One-sided
        # metadata changes flow through ordinary sync.
        if pair.get('pairing') == 'authoritative' and pair.get('metadata_diff_details'):
            filtered_details = []
            for detail_row in pair.get('metadata_diff_details') or []:
                change_origin = str(detail_row.get('change_origin') or '').strip()
                # change_origin is 'local', 'cloud', or 'both'.  Only 'both'
                # is a true divergence; 'local'/'cloud' auto-sync.
                if change_origin == 'both':
                    filtered_details.append(detail_row)
                else:
                    automatic_media_decisions.append({
                        'kind': 'image_metadata',
                        'field': detail_row.get('field'),
                        'action': f'auto_apply_{change_origin}',
                        'local_id': _safe_int((pair.get('local') or {}).get('local_id')) or None,
                        'cloud_id': str((pair.get('remote') or {}).get('cloud_id') or '').strip() or None,
                    })
            if filtered_details:
                pair = dict(pair)
                pair['metadata_diff_details'] = filtered_details
            else:
                # No genuine two-sided metadata conflict remains → drop the pair
                # UNLESS it still has a measurement conflict on it.
                if not pair.get('measurement_conflicts'):
                    continue
                pair = dict(pair)
                pair.pop('metadata_diff_details', None)
        kept_image_pairs.append(pair)
    image_pairs = kept_image_pairs

    kept_measurement_pairs: list[dict] = []
    for pair in measurement_pairs:
        status = pair.get('status')
        if status == 'local_only':
            automatic_media_decisions.append({
                'kind': 'measurement', 'side': 'local_only',
                'action': 'upload_automatic',
                'local_id': _safe_int(pair.get('local_id')) or None,
                'cloud_id': None,
            })
            continue
        if status == 'cloud_only':
            automatic_media_decisions.append({
                'kind': 'measurement', 'side': 'cloud_only',
                'action': 'download_automatic',
                'local_id': None,
                'cloud_id': str(pair.get('cloud_id') or '').strip() or None,
            })
            continue
        if status == 'identity_conflict':
            kept_measurement_pairs.append(pair)
            continue
        # Matched measurement conflicts: check change_origin.  Only 'both'
        # requires manual review; 'local' / 'cloud' auto-sync.
        change_origin = str(pair.get('change_origin') or '').strip()
        if status == 'values_differ' and change_origin in {'local', 'cloud'}:
            automatic_media_decisions.append({
                'kind': 'measurement', 'side': 'matched',
                'action': f'auto_apply_{change_origin}',
                'local_id': _safe_int(pair.get('local_id')) or None,
                'cloud_id': str(pair.get('cloud_id') or '').strip() or None,
            })
            continue
        kept_measurement_pairs.append(pair)
    measurement_pairs = kept_measurement_pairs
    # Also drop measurement_conflicts (top-level) whose paired row was auto-resolved.
    kept_conflict_ids: set = {
        (_safe_int(p.get('local_id')) or 0, str(p.get('cloud_id') or '').strip())
        for p in measurement_pairs
    }
    measurement_differences = [
        d for d in measurement_differences
        if (_safe_int(d.get('local_id')) or 0,
            str(d.get('cloud_id') or '').strip()) in kept_conflict_ids
    ]

    return {
        'local_id': int(local_id),
        'cloud_id': resolved_cloud_id,
        'title': _observation_display_name(local_obs),
        'local_observation': dict(local_obs or {}),
        'remote_observation': dict(remote_obs or {}),
        'field_rows': field_rows,
        'image_mismatches': image_mismatches,
        'image_pairs': image_pairs,
        'identity_conflicts': [
            pair for pair in image_pairs if pair.get('status') == 'identity_conflict'
        ],
        'local_image_changes': _summarize_image_changes(
            local_image_payloads,
            baseline_images,
            ignored_keys=tombstoned_remote_image_keys,
        ),
        'remote_image_changes': _summarize_image_changes(
            remote_image_payloads,
            baseline_images,
            ignored_keys=tombstoned_remote_image_keys,
        ),
        'local_measurement_count': len(local_meas_all_raw),
        'measurement_pairs': measurement_pairs,
        'measurement_conflicts': measurement_differences,
        'derived_statistics': derived_statistics,
        'baseline_available': bool(snapshot),
        'accepted_asymmetry': accepted_asymmetry or None,
        # Simplified sync model: additive one-sided items and pure one-sided
        # scalar changes are reported here for the sync log, NOT surfaced in
        # the manual conflict dialog.  The dialog opens only when at least
        # one of ``field_rows``, ``image_pairs`` (with divergence), or
        # ``measurement_pairs`` has content.
        'automatic_decisions': {
            'fields': automatic_field_decisions,
            'media': automatic_media_decisions,
        },
        'has_manual_conflicts': bool(
            field_rows
            or [
                p for p in measurement_pairs
                if p.get('status') in {'values_differ', 'identity_conflict'}
            ]
            or [
                p for p in image_pairs
                if p.get('status') in {'identity_conflict', 'possible_match'}
                or p.get('measurement_conflicts')
                or (p.get('pairing') == 'authoritative' and p.get('metadata_diff_details'))
            ]
        ),
        'plan_baseline': build_conflict_plan_baseline(
            local_obs=local_obs,
            remote_obs=remote_obs,
            local_images=list(local_images_raw),
            remote_images=list(remote_images),
            local_measurements=local_meas_all_raw,
            remote_measurements=list(remote_measurements or []),
        ),
    }
