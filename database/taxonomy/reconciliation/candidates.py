"""Level-5 candidate generation.

Candidates never assign identity — the resolver records them alongside a
non-resolving primary state (typically ``manual_unresolved`` or
``ambiguous_multiple_candidates``). The logic here is intentionally
conservative:

* exact scientific-name match (case-insensitive) against the pinned
  release's ``scientific_name.jsonl`` and ``taxon.jsonl`` canonical names;
* rank consistency check when the input carries a
  ``taxon_rank_snapshot`` and the concept exposes a rank;
* higher-classification consistency check when the input carries a
  ``stored_scientific_name`` whose leading token differs from the
  concept's genus (a common false-positive from historical binomial
  drift).

Candidates are returned sorted by ``sporely_taxon_id`` for determinism.
"""

from __future__ import annotations

from typing import Iterable

from database.taxonomy.reconciliation.input_model import Candidate, RawSignal
from database.taxonomy.reconciliation.sources import PinnedRelease


def _text_values(signals: Iterable[RawSignal], origin: str) -> list[str]:
    values: list[str] = []
    for signal in signals:
        if signal.kind != "text-only":
            continue
        if signal.origin_field == origin and signal.raw_value:
            values.append(signal.raw_value)
    return values


def generate_candidates(
    *,
    signals: tuple[RawSignal, ...],
    stored_rank: str | None,
    release: PinnedRelease,
    exclude_taxon_ids: frozenset[int] = frozenset(),
    max_candidates: int = 25,
) -> tuple[Candidate, ...]:
    """Produce Level-5 candidates for a reconciliation input.

    ``exclude_taxon_ids`` prevents already-resolved concepts from being
    duplicated as candidates. ``max_candidates`` guards against pathological
    fixtures (a genus-only signal that matches every species in a family).
    """
    rank = (stored_rank or "").strip().lower() or None

    # Collect all text signals that plausibly encode a scientific name.
    name_signals: list[tuple[str, str]] = []
    for signal in signals:
        if signal.kind != "text-only" or not signal.raw_value:
            continue
        if signal.origin_field in (
            "observations.ai_selected_scientific_name",
            "observations.scientific_name_snapshot",
            "observations.genus+species",
        ):
            name_signals.append((signal.raw_value, signal.origin_field))

    seen: set[int] = set(exclude_taxon_ids)
    out: list[Candidate] = []
    for raw_name, origin in name_signals:
        cleaned = raw_name.strip()
        if not cleaned:
            continue
        for taxon_id in release.candidates_for_name(cleaned):
            if taxon_id in seen:
                continue
            concept = release.concept(taxon_id)
            if concept is None:
                continue
            # Rank consistency: if the observation carried a rank, and the
            # concept exposes one, they must agree (case-insensitive).
            if rank and concept.taxon_rank and concept.taxon_rank.lower() != rank:
                continue
            # Higher-classification consistency: if the observation encoded
            # a binomial and the release concept sits in a different genus,
            # reject the candidate. This is a defensive check — the exact
            # name index already keys on the full scientific name, so this
            # only fires when name aliases cross genus boundaries.
            first_token = cleaned.split(" ", 1)[0].strip()
            if first_token and concept.genus and first_token.lower() != concept.genus.lower():
                # Allow aliases that are genus-only.
                if " " in cleaned:
                    continue
            seen.add(taxon_id)
            out.append(
                Candidate(
                    sporely_taxon_id=taxon_id,
                    canonical_name=concept.canonical_scientific_name,
                    rank=concept.taxon_rank,
                    scope_state=concept.scope_state,
                    match_kind="scientific_name_exact",
                    match_field=origin,
                    match_value=cleaned,
                )
            )
            if len(out) >= max_candidates:
                break
        if len(out) >= max_candidates:
            break

    out.sort(key=Candidate.sort_key)
    return tuple(out)
