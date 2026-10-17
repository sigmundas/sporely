"""Private observation location provenance: vocabulary, pair rules and bundle helpers.

The cloud stores ``observations.location_source`` / ``location_status``
(sporely-web migration 20261012105630) as owner diagnostics describing the
current coordinates. On the desktop the pair is a member of the coordinate
bundle: it is stored as received from the cloud, invalidated locally when
coordinates change without an attributed origin, and written to the cloud only
through ``set_observation_location``. Contract:
``docs/supabase-sync-contract.md`` ("Private ingestion and location provenance").

Pure code: no SQLite, settings, cloud client, I/O or Qt.
"""

from __future__ import annotations

import math

LOCATION_SOURCE_FIELD = 'location_source'
LOCATION_STATUS_FIELD = 'location_status'
LOCATION_PROVENANCE_FIELDS = (LOCATION_SOURCE_FIELD, LOCATION_STATUS_FIELD)

#: Coordinate fields the desktop syncs.
LOCATION_COORDINATE_FIELDS = ('gps_latitude', 'gps_longitude')

#: Cloud bundle members the desktop does not store. They are read and kept in
#: the baseline so a cloud change to them counts as a bundle change, but the
#: desktop never has a local value (absence is not clearing intent).
LOCATION_CLOUD_ONLY_BUNDLE_FIELDS = ('gps_altitude', 'gps_accuracy')

#: Optional observation read/snapshot columns of the provenance migration.
LOCATION_PROVENANCE_READ_FIELDS = LOCATION_PROVENANCE_FIELDS + LOCATION_CLOUD_ONLY_BUNDLE_FIELDS

LOCATION_SOURCES = frozenset({'device_gps', 'image_exif', 'manual', 'none', 'unknown'})
LOCATION_STATUSES = frozenset({
    'acquired', 'disabled', 'denied', 'timeout', 'skipped', 'unavailable', 'unknown',
})
_ACQUIRED_SOURCES = frozenset({'device_gps', 'image_exif', 'manual', 'unknown'})
_ABSENT_SOURCES = frozenset({'none', 'unknown'})

#: Cloud column on ``observation_images``; preserve-only on the desktop.
INGEST_METHOD_FIELD = 'ingest_method'

#: Same tolerance as ``values._OBSERVATION_GPS_ABS_TOL`` (6 decimal places).
_COORDINATE_ABS_TOL = 1e-6

#: Pairs the desktop itself attributes (contract "Desktop requirements" 5).
MANUAL_LOCATION_PAIR = ('manual', 'acquired')
IMAGE_EXIF_LOCATION_PAIR = ('image_exif', 'acquired')
CLEARED_LOCATION_PAIR = ('none', 'skipped')


def _coordinate(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def coordinate_values_match(left, right) -> bool:
    """NULL-safe coordinate equality within the sync GPS tolerance."""
    left_value = _coordinate(left)
    right_value = _coordinate(right)
    if left_value is None or right_value is None:
        return left_value is None and right_value is None
    return math.isclose(left_value, right_value, rel_tol=0.0, abs_tol=_COORDINATE_ABS_TOL)


def coordinates_match(left_lat, left_lon, right_lat, right_lon) -> bool:
    return coordinate_values_match(left_lat, right_lat) and coordinate_values_match(left_lon, right_lon)


def cloud_only_bundle_changed(remote: dict | None, baseline: dict | None) -> bool:
    """Whether the cloud changed altitude or accuracy since the baseline.

    Only fields recorded on both sides count: a baseline (or row) without the
    key has no evidence of a change.
    """
    remote_row = remote or {}
    baseline_row = baseline or {}
    return any(
        field in remote_row and field in baseline_row
        and not coordinate_values_match(remote_row.get(field), baseline_row.get(field))
        for field in LOCATION_CLOUD_ONLY_BUNDLE_FIELDS
    )


def provenance_text(value) -> str | None:
    """Received text as stored: no validation, remapping or defaulting."""
    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


def provenance_pair(row: dict | None) -> tuple[str | None, str | None]:
    data = row or {}
    return (
        provenance_text(data.get(LOCATION_SOURCE_FIELD)),
        provenance_text(data.get(LOCATION_STATUS_FIELD)),
    )


def row_carries_provenance(row: dict | None) -> bool:
    """Whether a cloud row (or snapshot) carries the provenance columns at all.

    A target or projection without the columns omits the keys; that is not
    evidence the pair is NULL.
    """
    data = row or {}
    return all(field in data for field in LOCATION_PROVENANCE_FIELDS)


def location_pair_is_writable(source, status, latitude, longitude) -> bool:
    """Whether the desktop may send ``(source, status)`` with these coordinates.

    Mirrors the server CHECKs for new provenance: ``acquired`` requires valid
    latitude/longitude and an acquired-capable source; every other status
    requires an absent bundle and ``none``/``unknown``. A NULL pair is never
    written by the desktop (it is the server's invalidation result).
    """
    if source not in LOCATION_SOURCES or status not in LOCATION_STATUSES:
        return False
    lat = _coordinate(latitude)
    lon = _coordinate(longitude)
    if status == 'acquired':
        return (
            source in _ACQUIRED_SOURCES
            and lat is not None and lon is not None
            and -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0
        )
    return source in _ABSENT_SOURCES and latitude is None and longitude is None


def attribute_coordinate_edit(
    *,
    before_lat,
    before_lon,
    after_lat,
    after_lon,
    image_exif_coords: tuple | None = None,
    allow_clear: bool = True,
) -> tuple[str, str] | None:
    """Classify one desktop coordinate edit, or ``None`` when nothing is known.

    ``None`` means "leave attribution to the local invalidation rule": either
    the coordinates did not change, or the origin is not known. Coordinates
    equal to the last ones applied from a photo's EXIF are ``image_exif``;
    a deliberate clear is ``none``/``skipped`` (only where ``allow_clear``);
    any other change is a manual entry.
    """
    if coordinates_match(before_lat, before_lon, after_lat, after_lon):
        return None
    if _coordinate(after_lat) is None and _coordinate(after_lon) is None:
        return CLEARED_LOCATION_PAIR if allow_clear else None
    if _coordinate(after_lat) is None or _coordinate(after_lon) is None:
        return None
    if image_exif_coords is not None:
        exif_lat, exif_lon = image_exif_coords
        if coordinates_match(exif_lat, exif_lon, after_lat, after_lon):
            return IMAGE_EXIF_LOCATION_PAIR
    return MANUAL_LOCATION_PAIR
