"""Artsobservasjoner taxon-id selection helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping
import logging


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArtsobservasjonerTaxonResolution:
    taxon_id: int
    source_field: str


class ArtsobservasjonerTaxonIdError(RuntimeError):
    """Raised when an observation has no safe Artsobservasjoner taxon id."""


class AmbiguousArtsobservasjonerTaxonIdError(ArtsobservasjonerTaxonIdError):
    """Raised when multiple incompatible verified taxon ids are present."""


_INAT_SERVICES = {"inat", "inaturalist", "inaturalist.org"}
_ARTSOBS_SERVICES = {"artsobs", "artsobservasjoner", "artsobservasjoner.no"}
_ARTSOBS_MARKERS = {
    "artsobs",
    "artsobservasjoner",
    "artsobservasjoner.no",
    "artsobservasjoner-compatible",
    "artsobs-compatible",
}


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def observation_taxon_name(observation: Mapping[str, Any] | None) -> str:
    obs = observation or {}
    genus = _text(obs.get("genus"))
    species = _text(obs.get("species"))
    if genus and species:
        return f"{genus} {species}"
    for key in ("ai_selected_scientific_name", "species_guess", "common_name"):
        value = _text(obs.get(key))
        if value:
            return value
    obs_id = _text(obs.get("id"))
    return f"observation {obs_id}" if obs_id else "this observation"


def _no_verified_id_error(observation: Mapping[str, Any] | None) -> ArtsobservasjonerTaxonIdError:
    return ArtsobservasjonerTaxonIdError(
        "Cannot publish to Artsobservasjoner: "
        f"no verified Artsobservasjoner taxon id for {observation_taxon_name(observation)}."
    )


def _ai_taxon_is_explicitly_artsobs_compatible(observation: Mapping[str, Any]) -> bool:
    service = _text(observation.get("ai_selected_service")).lower()
    if service in _ARTSOBS_SERVICES:
        return True
    source = _text(
        observation.get("ai_selected_taxon_id_source")
        or observation.get("ai_selected_taxon_source")
    ).lower()
    if source in _ARTSOBS_MARKERS:
        return True
    return any(
        _truthy(observation.get(key))
        for key in (
            "ai_selected_taxon_id_artsobservasjoner_compatible",
            "ai_selected_taxon_id_is_artsobservasjoner_compatible",
            "ai_selected_taxon_id_verified_for_artsobservasjoner",
        )
    )


def select_artsobservasjoner_taxon_id(
    observation: Mapping[str, Any] | None,
    *,
    resolved_taxonomy_taxon_id: int | str | None = None,
    resolved_taxonomy_source: str = "taxonomy_db.artsdatabanken",
) -> ArtsobservasjonerTaxonResolution:
    """Return a taxon id only when its source is Artsobservasjoner-compatible."""
    obs = observation or {}
    candidates: list[ArtsobservasjonerTaxonResolution] = []

    for explicit_field in ("artsobservasjoner_taxon_id", "artsobs_taxon_id"):
        explicit_id = _positive_int(obs.get(explicit_field))
        if explicit_id:
            candidates.append(
                ArtsobservasjonerTaxonResolution(
                    taxon_id=explicit_id,
                    source_field=explicit_field,
                )
            )

    taxonomy_id = _positive_int(resolved_taxonomy_taxon_id)
    if taxonomy_id:
        candidates.append(
            ArtsobservasjonerTaxonResolution(
                taxon_id=taxonomy_id,
                source_field=resolved_taxonomy_source,
            )
        )

    ai_id = _positive_int(obs.get("ai_selected_taxon_id"))
    ai_service = _text(obs.get("ai_selected_service")).lower()
    if ai_id and ai_service not in _INAT_SERVICES and _ai_taxon_is_explicitly_artsobs_compatible(obs):
        candidates.append(
            ArtsobservasjonerTaxonResolution(
                taxon_id=ai_id,
                source_field="ai_selected_taxon_id:verified_artsobservasjoner_compatible",
            )
        )

    if not candidates:
        raise _no_verified_id_error(obs)

    unique_ids = {candidate.taxon_id for candidate in candidates}
    if len(unique_ids) > 1:
        name = observation_taxon_name(obs)
        details = ", ".join(
            f"{candidate.source_field}={candidate.taxon_id}"
            for candidate in candidates
        )
        raise AmbiguousArtsobservasjonerTaxonIdError(
            "Cannot publish to Artsobservasjoner: "
            f"ambiguous Artsobservasjoner taxon id for {name} ({details})."
        )

    return candidates[0]


def log_artsobservasjoner_taxon_diagnostic(
    observation: Mapping[str, Any] | None,
    resolution: ArtsobservasjonerTaxonResolution,
) -> None:
    obs = observation or {}
    logger.warning(
        "Artsobservasjoner publish taxon diagnostic: "
        "local_observation_id=%r genus=%r species=%r common_name=%r "
        "ai_selected_service=%r ai_selected_scientific_name=%r ai_selected_taxon_id=%r "
        "artsdata_id=%r artportalen_id=%r final_taxon_id=%r source_field=%r",
        obs.get("id"),
        obs.get("genus"),
        obs.get("species"),
        obs.get("common_name"),
        obs.get("ai_selected_service"),
        obs.get("ai_selected_scientific_name"),
        obs.get("ai_selected_taxon_id"),
        obs.get("artsdata_id"),
        obs.get("artportalen_id"),
        resolution.taxon_id,
        resolution.source_field,
    )
