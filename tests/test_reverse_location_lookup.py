from database import reverse_location_lookup as lookup


def test_nominatim_suggestions_include_display_and_local_hierarchy():
    data = {
        "display_name": "Broder Knudtzons vei, Trondheim, Trøndelag, Norge",
        "address": {
            "road": "Broder Knudtzons vei",
            "suburb": "Østbyen",
            "city": "Trondheim",
            "county": "Trøndelag",
            "country": "Norge",
            "country_code": "no",
        },
    }

    assert lookup.nominatim_suggestions(data) == [
        "Broder Knudtzons vei, Trondheim, Trøndelag, Norge",
        "Broder Knudtzons vei, Østbyen, Trondheim, Trøndelag, Norge",
    ]


def test_lookup_location_suggestions_prefers_valid_artsdatabanken_for_norway(monkeypatch):
    monkeypatch.setattr(
        lookup,
        "_request_nominatim",
        lambda lat, lon, timeout=10.0: {
            "display_name": "Broder Knudtzons vei, Trondheim, Trøndelag, Norge",
            "address": {"road": "Broder Knudtzons vei", "country": "Norge", "country_code": "no"},
        },
    )
    monkeypatch.setattr(
        lookup,
        "_request_artsdatabanken",
        lambda lat, lon, timeout=10.0: {"navn": "Broder Knudtzons vei", "dist": 0.000027},
    )

    result = lookup.lookup_location_suggestions(63.425816, 10.412362)

    assert result.country_code == "no"
    assert result.source == "artsdatabanken"
    assert result.suggestions[0] == "Broder Knudtzons vei"
    assert "Broder Knudtzons vei, Trondheim, Trøndelag, Norge" in result.suggestions


def test_lookup_location_suggestions_falls_back_when_artsdatabanken_distance_is_large(monkeypatch):
    monkeypatch.setattr(
        lookup,
        "_request_nominatim",
        lambda lat, lon, timeout=10.0: {
            "display_name": "Norwegian fallback",
            "address": {"road": "Local road", "country": "Norge", "country_code": "no"},
        },
    )
    monkeypatch.setattr(
        lookup,
        "_request_artsdatabanken",
        lambda lat, lon, timeout=10.0: {"navn": "Offshore anomaly", "dist": 0.2},
    )

    result = lookup.lookup_location_suggestions(63.425816, 10.412362)

    assert result.source == "nominatim"
    assert result.suggestions == ["Norwegian fallback", "Local road, Norge"]


def test_lookup_location_suggestions_prefers_dawa_for_denmark(monkeypatch):
    monkeypatch.setattr(
        lookup,
        "_request_nominatim",
        lambda lat, lon, timeout=10.0: {
            "display_name": "Søndergade, Vejle Kommune, Danmark",
            "address": {
                "road": "Søndergade",
                "town": "Vejle",
                "country": "Danmark",
                "country_code": "dk",
            },
        },
    )
    monkeypatch.setattr(
        lookup,
        "_request_dawa",
        lambda lat, lon, timeout=10.0: {
            "vejstykke": {"navn": "Søndergade"},
            "postnummer": {"navn": "Vejle"},
            "kommune": {"navn": "Vejle"},
            "region": {"navn": "Region Syddanmark"},
        },
    )

    result = lookup.lookup_location_suggestions(55.708928, 9.539420)

    assert result.country_code == "dk"
    assert result.source == "dawa"
    assert result.suggestions == [
        "Søndergade, Vejle, Region Syddanmark, Danmark",
        "Søndergade, Vejle Kommune, Danmark",
        "Søndergade, Vejle, Danmark",
    ]
