"""Sample-source representation helpers (desktop vs cloud) and the image
sample-field push payload adapter.

Stateless, but outside the pure package: ``DatabaseTerms``
(``database.database_tags``) imports Qt, and the push adapter asks the client
for a schema capability.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

_LEGACY_SAMPLE_SOURCE_ON_SAMPLE_TYPE = {'Spore_print', 'spore_print', 'spore print', 'Print', 'print'}


# Canonical CLOUD representation for `observation_images.sample_source`.
# The sporely-web Stage 2A migration
# (`20260715120000_add_sample_source_to_observation_images.sql`) picked
# lowercase snake_case (`spore_print`, `hymenium`, `stipe`, `pileus`,
# `context`, `other`) so public RPCs can `lower(btrim(...))`-normalize
# variants safely. Desktop keeps Title_Case locally (matches every other
# tag category) and translates at the boundary — see
# `_desktop_to_cloud_sample_source` on push and
# `_cloud_to_desktop_sample_source` on pull.
_CLOUD_SAMPLE_SOURCE_VALUES = frozenset({
    'spore_print', 'hymenium', 'stipe', 'pileus', 'context', 'other',
})


def _desktop_to_cloud_sample_source(value: object) -> str | None:
    """Translate a desktop-canonical `sample_source` to the cloud canonical form.

    Desktop stores Title_Case (`Spore_print`, `Hymenium`, ...). Cloud stores
    lowercase snake_case. Legacy / compact variants (`Print`, `spore print`,
    ...) are canonicalized via `DatabaseTerms.canonicalize_sample_source`
    first, then lowercased. Returns None for empty / unknown values so the
    push omits the field entirely rather than sending garbage.
    """
    from database.database_tags import DatabaseTerms

    text = str(value or '').strip()
    if not text:
        return None
    # The `Print` compact-pill label isn't in SAMPLE_SOURCE_DISPLAY but should
    # still round-trip to `spore_print` on the cloud.
    if text.lower() in {'print', 'spore print', 'spore_print', 'sporeprint'}:
        return 'spore_print'
    canonical = DatabaseTerms.canonicalize_sample_source(text)
    if not canonical or canonical == 'Not_set':
        return None
    lowered = canonical.lower()
    return lowered if lowered in _CLOUD_SAMPLE_SOURCE_VALUES else None


def _cloud_to_desktop_sample_source(value: object) -> str | None:
    """Translate a cloud `sample_source` value back to desktop Title_Case.

    Accepts either the canonical lowercase snake_case (`spore_print`,
    `hymenium`, ...) or the historical Title_Case some clients may still
    emit (`Spore_print`, `Hymenium`, ...). Anything else — including 'Not_set'
    and empty strings — returns None so the local column stays NULL.
    """
    from database.database_tags import DatabaseTerms

    text = str(value or '').strip()
    if not text:
        return None
    canonical = DatabaseTerms.canonicalize_sample_source(text)
    if not canonical or canonical == 'Not_set':
        return None
    return canonical


def _split_legacy_sample_type_into_source(
    sample_type: object,
    sample_source: object,
) -> tuple[str | None, str | None]:
    """Split a legacy `sample_type='Spore_print'` row into (condition, source).

    Historically, `Spore_print` lived on `images.sample_type` alongside
    Fresh/Dried. Stage 1 moved it to its own `sample_source` category. If the
    local column still carries the legacy value (e.g. because a row hasn't
    been touched since the migration), route it into `sample_source` for the
    push payload and clear the condition side. Explicit sample_source always
    wins — this only fills gaps.

    Returns ``(condition, source)`` as canonical strings or None.
    """
    from database.database_tags import DatabaseTerms

    raw_type = str(sample_type or '').strip()
    raw_source = str(sample_source or '').strip()

    normalized_type = DatabaseTerms.canonicalize_sample(raw_type) if raw_type else None
    normalized_source = (
        DatabaseTerms.canonicalize_sample_source(raw_source) if raw_source else None
    )

    if raw_type in _LEGACY_SAMPLE_SOURCE_ON_SAMPLE_TYPE and not normalized_source:
        # Legacy value stuck on the wrong column — promote it. Any of the
        # historical spore-print spellings (Spore_print, spore print, and the
        # compact-pill label "Print") map to the canonical Spore_print value.
        normalized_source = 'Spore_print'
        normalized_type = None
    elif raw_type in _LEGACY_SAMPLE_SOURCE_ON_SAMPLE_TYPE:
        # Already have a source; just clear the legacy condition value.
        normalized_type = None

    if normalized_type == 'Not_set':
        normalized_type = None
    if normalized_source == 'Not_set':
        normalized_source = None
    return normalized_type, normalized_source


def _apply_image_sample_fields_to_push_payload(
    payload: dict,
    image_row: dict,
    *,
    client: 'SporelyCloudClient | None' = None,
    obs_cloud_id: str | None = None,
) -> None:
    """Normalize sample_type / sample_source on the outgoing image payload.

    * Splits legacy `sample_type='Spore_print'` into `sample_source='Spore_print'`.
    * Null-safe merge for `sample_source`: when the local column is empty we
      OMIT `sample_source` from the payload entirely. On a PATCH, PostgREST
      leaves the existing cloud value untouched; on a POST (new row), the
      column defaults to NULL — same as sending NULL — so nothing is lost.
      This avoids a network round-trip and prevents unrelated image-metadata
      patches from wiping cloud values.
    * Drops `sample_source` from the payload entirely when the cloud
      deployment is older and doesn't have the column (schema-cache safety).
      The capability probe only runs when we actually have a value to send —
      keeps the no-op path (empty local sample_source) free of any network
      round-trip.
    """
    condition, source = _split_legacy_sample_type_into_source(
        image_row.get('sample_type'),
        image_row.get('sample_source'),
    )
    payload['sample_type'] = condition

    if not source:
        # No value to push. Omit the field so PATCH leaves cloud alone; POST
        # defaults NULL. No capability probe needed on this fast path — that
        # keeps existing tests (which don't monkeypatch every probe) working.
        payload.pop('sample_source', None)
        return

    # Boundary translation: desktop Title_Case → cloud lowercase snake_case.
    cloud_source = _desktop_to_cloud_sample_source(source)
    if not cloud_source:
        payload.pop('sample_source', None)
        return

    if client is not None:
        try:
            supported = bool(client._observation_images_support_sample_source())
        except Exception:
            supported = False
        if not supported:
            payload.pop('sample_source', None)
            return

    payload['sample_source'] = cloud_source
