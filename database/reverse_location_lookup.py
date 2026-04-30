"""Reverse geocoding helpers for observation location suggestions."""
from __future__ import annotations

from dataclasses import dataclass
import threading
import time
from typing import Any

import requests

NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_USER_AGENT = "SporelyApp/1.0 (contact@sporely.no)"
ARTSDATABANKEN_REVERSE_URL = "https://stedsnavn.artsdatabanken.no/v1/punkt"
DAWA_REVERSE_URL = "https://api.dataforsyningen.dk/adgangsadresser/reverse"
ARTSDATABANKEN_MAX_DIST = 0.006

_nominatim_lock = threading.Lock()
_last_nominatim_request_at = 0.0


@dataclass(frozen=True)
class LocationLookupResult:
    """Ordered place-name suggestions for a coordinate."""

    suggestions: list[str]
    latitude: float | None = None
    longitude: float | None = None
    country_code: str | None = None
    country_name: str | None = None
    nominatim_display_name: str | None = None
    source: str | None = None

    @property
    def best_name(self) -> str:
        return self.suggestions[0] if self.suggestions else ""


def _dedupe_text(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = " ".join(text.casefold().split())
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def extract_hierarchy(
    places: list[str | None],
    country: str | None = None,
    max_parts: int = 5,
) -> str:
    """Return a compact local-to-regional location label."""
    result = _dedupe_text([*(places or []), country])
    return ", ".join(result[:max(1, int(max_parts))])


def _safe_json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _request_nominatim(lat: float, lon: float, timeout: float = 10.0) -> dict[str, Any]:
    global _last_nominatim_request_at
    with _nominatim_lock:
        elapsed = time.monotonic() - _last_nominatim_request_at
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        response = requests.get(
            NOMINATIM_REVERSE_URL,
            params={
                "lat": lat,
                "lon": lon,
                "format": "json",
                "addressdetails": 1,
            },
            headers={
                "User-Agent": NOMINATIM_USER_AGENT,
                "Accept": "application/json",
            },
            timeout=timeout,
        )
        _last_nominatim_request_at = time.monotonic()
    if response.status_code != 200:
        return {}
    return _safe_json(response)


def nominatim_local_parts(data: dict[str, Any]) -> list[str]:
    """Return the two most local Nominatim address fields as separate suggestions."""
    addr = data.get("address")
    if not isinstance(addr, dict):
        return []
    return _dedupe_text([
        addr.get("amenity") or addr.get("road"),
        addr.get("neighbourhood") or addr.get("suburb"),
    ])


def nominatim_local_name(data: dict[str, Any]) -> str:
    """Build the compact two-part Nominatim name for display outside dropdowns."""
    return ", ".join(nominatim_local_parts(data))


def nominatim_suggestions(data: dict[str, Any]) -> list[str]:
    """Return user-facing Nominatim suggestions."""
    local_parts = nominatim_local_parts(data)
    return _dedupe_text([
        *local_parts,
        str(data.get("display_name") or "").strip() if not local_parts else "",
    ])


def _country_code_from_nominatim(data: dict[str, Any]) -> str | None:
    addr = data.get("address")
    if not isinstance(addr, dict):
        return None
    code = str(addr.get("country_code") or "").strip().lower()
    return code or None


def _country_name_from_nominatim(data: dict[str, Any]) -> str | None:
    addr = data.get("address")
    if not isinstance(addr, dict):
        return None
    country = str(addr.get("country") or "").strip()
    return country or None


def _request_artsdatabanken(lat: float, lon: float, timeout: float = 10.0) -> dict[str, Any]:
    response = requests.get(
        ARTSDATABANKEN_REVERSE_URL,
        params={"lat": lat, "lng": lon, "zoom": 45},
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
    if response.status_code != 200:
        return {}
    return _safe_json(response)


def artsdatabanken_suggestion(data: dict[str, Any]) -> str:
    """Return a validated Artsdatabanken place name, or an empty string."""
    try:
        dist = float(data.get("dist"))
    except (TypeError, ValueError):
        return ""
    if dist > ARTSDATABANKEN_MAX_DIST:
        return ""
    return str(data.get("navn") or "").strip()


def _request_dawa(lat: float, lon: float, timeout: float = 10.0) -> dict[str, Any]:
    response = requests.get(
        DAWA_REVERSE_URL,
        params={"x": lon, "y": lat},
        headers={"Accept": "application/json"},
        timeout=timeout,
    )
    if response.status_code != 200:
        return {}
    return _safe_json(response)


def dawa_suggestion(data: dict[str, Any]) -> str:
    """Format DAWA reverse-geocoding output from local to regional."""
    places = [
        (data.get("vejstykke") or {}).get("navn") if isinstance(data.get("vejstykke"), dict) else None,
        (data.get("postnummer") or {}).get("navn") if isinstance(data.get("postnummer"), dict) else None,
        (data.get("kommune") or {}).get("navn") if isinstance(data.get("kommune"), dict) else None,
        (data.get("region") or {}).get("navn") if isinstance(data.get("region"), dict) else None,
    ]
    return extract_hierarchy(places, "Danmark")


def lookup_location_suggestions(
    lat: float,
    lon: float,
    timeout: float = 10.0,
) -> LocationLookupResult:
    """Return ordered location suggestions for a GPS coordinate."""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return LocationLookupResult([])
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return LocationLookupResult([])

    nominatim = _request_nominatim(lat, lon, timeout=timeout)
    country_code = _country_code_from_nominatim(nominatim)
    country_name = _country_name_from_nominatim(nominatim)
    display_name = str(nominatim.get("display_name") or "").strip() or None
    suggestions: list[str | None] = []
    source = "nominatim"

    if country_code == "no":
        arts = _request_artsdatabanken(lat, lon, timeout=timeout)
        arts_name = artsdatabanken_suggestion(arts)
        if arts_name:
            suggestions.append(arts_name)
            source = "artsdatabanken"
    elif country_code == "dk":
        dawa = _request_dawa(lat, lon, timeout=timeout)
        dawa_name = dawa_suggestion(dawa)
        if dawa_name:
            suggestions.append(dawa_name)
            source = "dawa"

    suggestions.extend(nominatim_suggestions(nominatim))
    return LocationLookupResult(
        suggestions=_dedupe_text(suggestions),
        latitude=lat,
        longitude=lon,
        country_code=country_code,
        country_name=country_name,
        nominatim_display_name=display_name,
        source=source,
    )


if __name__ == "__main__":
    for label, lat, lon in (
        ("Norway", 63.425816, 10.412362),
        ("Denmark", 55.708928, 9.539420),
    ):
        result = lookup_location_suggestions(lat, lon)
        print(f"\n--- {label} ---")
        print(result.country_code, result.source)
        for suggestion in result.suggestions:
            print(suggestion)
