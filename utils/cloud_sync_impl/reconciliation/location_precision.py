"""Pure location-precision ranking.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from database.models import ObservationDB


# --- Location precision: never widen silently (Stage 2c) ---------------------
# Server levels, least to most precise. The views coalesce a missing value
# to 'exact'. Builds before Stage 2c stored a cloud 'hidden'/'region' locally
# as 'exact' while the sync snapshot kept the raw cloud value, so a local
# value that is MORE precise than the cloud/baseline is only trusted when the
# user explicitly chose (and, if publishing, confirmed) it on this device.
_LOCATION_PRECISION_RANK = {'hidden': 0, 'region': 1, 'fuzzed': 2, 'exact': 3}


def _location_precision_rank(value) -> int:
    return _LOCATION_PRECISION_RANK[ObservationDB._normalize_location_precision(value)]
