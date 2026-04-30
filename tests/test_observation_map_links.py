from ui.observations_tab import (
    _extract_coords_from_osm_url,
    _extract_location_name_from_map_url,
)


def test_extract_location_name_from_osm_search_link():
    url = "https://www.openstreetmap.org/search?query=Bymarka%2C%20Trondheim#map=15/63.418/10.250"

    assert _extract_coords_from_osm_url(url) == (63.418, 10.25)
    assert _extract_location_name_from_map_url(url) == "Bymarka, Trondheim"


def test_extract_location_name_ignores_coordinate_only_query():
    url = "https://www.openstreetmap.org/?mlat=63.418&mlon=10.250#map=18/63.418/10.250"

    assert _extract_coords_from_osm_url(url) == (63.418, 10.25)
    assert _extract_location_name_from_map_url(url) is None


def test_extract_location_name_from_google_place_link():
    url = "https://www.google.com/maps/place/Bymarka,+Trondheim/@63.418,10.250,14z"

    assert _extract_location_name_from_map_url(url) == "Bymarka, Trondheim"
