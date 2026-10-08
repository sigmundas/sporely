"""Pure push-preflight conflict report type and its formatting.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from dataclasses import dataclass


def _format_observation_metadata_field_label(field: str) -> str:
    labels = {
        'date': 'date',
        'genus': 'genus',
        'species': 'species',
        'common_name': 'common name',
        'species_guess': 'species guess',
        'uncertain': 'uncertain',
        'unspontaneous': 'unspontaneous',
        'determination_method': 'determination method',
        'location': 'location',
        'gps_latitude': 'latitude',
        'gps_longitude': 'longitude',
        'location_public': 'location public',
        'is_draft': 'draft flag',
        'location_precision': 'location precision',
        'ai_selected_service': 'AI service',
        'ai_selected_taxon_id': 'AI taxon id',
        'ai_selected_scientific_name': 'AI scientific name',
        'ai_selected_probability': 'AI probability',
        'ai_selected_at': 'AI selected at',
        'habitat': 'habitat',
        'habitat_nin2_path': 'NIN2 path',
        'habitat_substrate_path': 'substrate path',
        'habitat_host_genus': 'host genus',
        'habitat_host_species': 'host species',
        'habitat_host_common_name': 'host common name',
        'habitat_nin2_note': 'NIN2 note',
        'habitat_substrate_note': 'substrate note',
        'habitat_grows_on_note': 'grows-on note',
        'notes': 'notes',
        'open_comment': 'open comment',
        'interesting_comment': 'interesting comment',
        'publish_target': 'publish target',
        'artsdata_id': 'Artsobs id',
        'artportalen_id': 'Artportalen id',
        'inaturalist_id': 'iNaturalist id',
        'mushroomobserver_id': 'Mushroom Observer id',
        'spore_data_visibility': 'spore visibility',
        'visibility': 'visibility',
        'sharing_scope': 'sharing scope',
        'taxon_identity': 'taxon identity',
    }
    normalized = str(field or '').strip()
    return labels.get(normalized, normalized.replace('_', ' '))


@dataclass(frozen=True)
class ObservationPushConflictReport:
    """Structured result from :func:`_analyze_observation_push_conflicts`.

    Mirrors the categories pull_all already reports as review-needed so push
    never silently overwrites a remote divergence.
    """

    has_conflict: bool
    categories: list[str]
    field_labels: list[str]
    measurement_conflict_ids: list[int]
    image_conflict_keys: list[str]
    remote_removed_image_keys: list[str]


def _format_push_conflict_review_reasons(report: ObservationPushConflictReport) -> list[str]:
    """Human-readable review reasons corresponding to conflict categories."""
    reasons: list[str] = []
    if report.field_labels:
        reasons.append(', '.join(report.field_labels))
    if report.image_conflict_keys:
        reasons.append(
            f'images changed on both sides ({len(report.image_conflict_keys)})'
        )
    if report.measurement_conflict_ids:
        reasons.append(
            f'measurements changed on both sides ({len(report.measurement_conflict_ids)})'
        )
    if report.remote_removed_image_keys:
        reasons.append('cloud removed local image files')
    return reasons
