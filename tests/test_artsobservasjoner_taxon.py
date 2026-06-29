from __future__ import annotations

import pytest

from utils.artsobservasjoner_taxon import (
    AmbiguousArtsobservasjonerTaxonIdError,
    ArtsobservasjonerTaxonIdError,
    select_artsobservasjoner_taxon_id,
)


def test_verified_artsobservasjoner_id_is_accepted() -> None:
    result = select_artsobservasjoner_taxon_id(
        {"genus": "Atheniella", "species": "flavoalba"},
        resolved_taxonomy_taxon_id=223130,
    )

    assert result.taxon_id == 223130
    assert result.source_field == "taxonomy_db.artsdatabanken"


def test_inaturalist_ai_id_is_rejected() -> None:
    with pytest.raises(ArtsobservasjonerTaxonIdError) as excinfo:
        select_artsobservasjoner_taxon_id(
            {
                "genus": "Atheniella",
                "species": "flavoalba",
                "ai_selected_service": "inat",
                "ai_selected_taxon_id": "499704",
            }
        )

    assert str(excinfo.value) == (
        "Cannot publish to Artsobservasjoner: no verified Artsobservasjoner "
        "taxon id for Atheniella flavoalba."
    )


def test_artsorakel_ai_id_is_rejected_unless_marked_compatible() -> None:
    observation = {
        "genus": "Atheniella",
        "species": "flavoalba",
        "ai_selected_service": "artsorakel",
        "ai_selected_taxon_id": "223130",
    }
    with pytest.raises(ArtsobservasjonerTaxonIdError):
        select_artsobservasjoner_taxon_id(observation)

    result = select_artsobservasjoner_taxon_id(
        {
            **observation,
            "ai_selected_taxon_id_artsobservasjoner_compatible": True,
        }
    )

    assert result.taxon_id == 223130
    assert result.source_field == "ai_selected_taxon_id:verified_artsobservasjoner_compatible"


def test_missing_id_gives_clear_error() -> None:
    with pytest.raises(ArtsobservasjonerTaxonIdError) as excinfo:
        select_artsobservasjoner_taxon_id(
            {
                "genus": "Atheniella",
                "species": "flavoalba",
            }
        )

    assert str(excinfo.value) == (
        "Cannot publish to Artsobservasjoner: no verified Artsobservasjoner "
        "taxon id for Atheniella flavoalba."
    )


def test_conflicting_verified_ids_are_ambiguous() -> None:
    with pytest.raises(AmbiguousArtsobservasjonerTaxonIdError):
        select_artsobservasjoner_taxon_id(
            {"artsobservasjoner_taxon_id": 1, "genus": "A", "species": "b"},
            resolved_taxonomy_taxon_id=2,
        )
