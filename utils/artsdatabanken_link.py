"""Shared Artsdatabanken concept-link resolver.

Stage 3B.5 introduces this tiny module so the ``arter/takson/{concept-id}``
URL is built from a *concept id* (``taxonID``), not the *scientific-name id*
that Artsorakel / iNaturalist / Artsobservasjoner surfaces. When the
resolver fails, callers get a NorTaxa fallback (``nortaxa.artsdatabanken.no/
name-info/{scientific-name-id}``) that always points at a page describing
the same name — never a wrong concept page.

Rules of use
------------

- Do **NOT** call ``resolve_concept_id`` from a paint / render callback:
  the ``requests.get`` timeout is 5 s and Qt paint events must return in
  milliseconds.
- ``concept_link_from_name_id`` may run synchronously from user-initiated
  actions only (e.g. clicking a link cell in the AI panel). Long-lived
  loops MUST NOT call it with ``network=True``; pass ``network=False`` to
  short-circuit to the NorTaxa fallback whenever the concept id is not
  already cached.
- The public surface is exactly the two functions below. No general
  Artsdatabanken client belongs here. Red List assessment-area selection
  lives in ``database/taxon_lookup.py`` next to the Red List runtime.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any


# Public tunables (documented for the audit trail).
_CACHE_MAX_ENTRIES = 256
_NEGATIVE_TTL_SECONDS = 900.0
_REQUEST_TIMEOUT_SECONDS = 5.0

# Sentinel stored in the cache to mark a known-negative result. Externally
# ``concept_link_from_name_id`` returns the NorTaxa fallback for it.
_NEGATIVE = object()

# Explicit no-network guard. Tests set this to False (or monkeypatch
# ``_perform_request``) to keep unit tests hermetic.
_REQUESTS_ENABLED = True

# Manual dict-based LRU. ``functools.lru_cache`` cannot expire entries,
# and we need to expire negatives after ``_NEGATIVE_TTL_SECONDS``. Each
# value is ``(payload, expires_at_or_none)``.
_cache: "OrderedDict[int, tuple[Any, float | None]]" = OrderedDict()
_cache_lock = threading.Lock()


def _now() -> float:
    """Small wrapper so tests can monkeypatch a virtual clock."""
    return time.monotonic()


def _cache_get(name_id: int) -> tuple[Any, bool]:
    """Return ``(value, hit)``. Silently drops expired entries."""
    with _cache_lock:
        entry = _cache.get(name_id)
        if entry is None:
            return (None, False)
        payload, expires_at = entry
        if expires_at is not None and _now() >= expires_at:
            _cache.pop(name_id, None)
            return (None, False)
        _cache.move_to_end(name_id)
        return (payload, True)


def _cache_put(name_id: int, value: Any, *, negative: bool) -> None:
    with _cache_lock:
        expires_at = (_now() + _NEGATIVE_TTL_SECONDS) if negative else None
        _cache[name_id] = (value, expires_at)
        _cache.move_to_end(name_id)
        while len(_cache) > _CACHE_MAX_ENTRIES:
            _cache.popitem(last=False)


def _normalize_name_id(scientific_name_id: int | str) -> int | None:
    try:
        value = int(str(scientific_name_id).strip())
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _perform_request(url: str) -> dict:
    """Isolated network call so tests can monkeypatch it wholesale.

    Raises on any failure; callers translate to a cache negative.
    """
    if not _REQUESTS_ENABLED:
        raise RuntimeError("network disabled")
    import requests  # local import — keeps module light for tests

    response = requests.get(url, timeout=_REQUEST_TIMEOUT_SECONDS)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}")
    return response.json()


def resolve_concept_id(scientific_name_id: int | str) -> int | None:
    """Resolve a scientific-name id to its Artsdatabanken concept id.

    Returns ``None`` on any failure (including invalid input). Success and
    negative results are cached; negatives expire after ~900 s so a
    transient outage does not permanently disable the link.
    """
    name_id = _normalize_name_id(scientific_name_id)
    if name_id is None:
        return None
    cached, hit = _cache_get(name_id)
    if hit:
        return None if cached is _NEGATIVE else int(cached)
    url = f"https://artsdatabanken.no/Api/Taxon/ScientificName/{name_id}"
    try:
        data = _perform_request(url)
        concept_id = int(data["taxonID"])
        if concept_id <= 0:
            raise ValueError("non-positive taxonID")
    except Exception:
        _cache_put(name_id, _NEGATIVE, negative=True)
        return None
    _cache_put(name_id, concept_id, negative=False)
    return concept_id


def concept_link_from_name_id(
    scientific_name_id: int | str,
    *,
    network: bool = True,
) -> str | None:
    """Return an Artsdatabanken concept URL, or the NorTaxa fallback.

    Contract:
      - On success: ``https://artsdatabanken.no/arter/takson/{concept-id}``
      - On any failure: ``https://nortaxa.artsdatabanken.no/name-info/
        {scientific-name-id}`` — never ``/arter/takson/{name-id}``.
      - On invalid input: ``None``.

    ``network`` gate:
      - Default (``True``): full behaviour — a cache miss triggers the
        HTTP resolve, which may block up to the timeout on failure.
      - ``False``: cache-only mode. A cached success returns the concept
        URL immediately; a cached negative or any miss returns the
        NorTaxa fallback without touching the network. Use this from
        per-observation loops on the GUI thread (e.g. cloud AI-state
        assembly) where a network stall would freeze the editor.
    """
    name_id = _normalize_name_id(scientific_name_id)
    if name_id is None:
        return None
    if network:
        concept_id = resolve_concept_id(name_id)
    else:
        cached, hit = _cache_get(name_id)
        concept_id = None if not hit or cached is _NEGATIVE else int(cached)
    if concept_id is None:
        return f"https://nortaxa.artsdatabanken.no/name-info/{name_id}"
    return f"https://artsdatabanken.no/arter/takson/{concept_id}"


def _reset_cache_for_tests() -> None:
    """Test-only: clear the process-wide cache. Not part of the public API."""
    with _cache_lock:
        _cache.clear()
