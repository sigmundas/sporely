"""Per-observation sync status message formatting.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

from utils.taxon_text import resolve_observation_taxon_fields

from utils.cloud_sync_impl.common import _safe_int


def _observation_sync_species_label(obs: dict | None) -> str:
    record = dict(obs or {})
    genus, species, species_guess = resolve_observation_taxon_fields(
        record.get('genus'),
        record.get('species'),
        record.get('species_guess'),
        record.get('ai_selected_scientific_name'),
    )
    label = " ".join(part for part in (genus, species) if part).strip()
    if label:
        return label
    common_name = str(record.get('common_name') or '').strip()
    if common_name:
        return common_name
    if species_guess:
        return species_guess
    return ''


def _format_cloud_sync_observation_status(obs: dict | None, message: str) -> str:
    record = dict(obs or {})
    obs_id = _safe_int(record.get('id'))
    label = _observation_sync_species_label(record)
    if obs_id > 0 and label:
        prefix = f'Observation {obs_id} ({label})'
    elif obs_id > 0:
        prefix = f'Observation {obs_id}'
    elif label:
        prefix = label
    else:
        prefix = 'Observation'
    text = str(message or '').strip()
    return f'{prefix}: {text}' if text else prefix
