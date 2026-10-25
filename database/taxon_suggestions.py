"""Evidence-aware taxon suggestions for pickers.

AI predictions, an observation's own name and typed search text all end up
as rows a user can pick a taxon from. They do not carry the same evidence:

- a **verified concept** is a Sporely concept proven by an identifier — the
  observation's own proven identity, or a provider identifier (Artsorakel
  ``NBIC:54701``) that the installed taxonomy resolves to exactly one concept
  through :meth:`TaxonLookupService.resolve_external_identity`, the same
  resolver and NorTaxa bridge the observation editor's AI copy uses;
- a **name-match concept** is a Sporely concept whose scientific name equals a
  suggested name. It is offered so the user can choose it in one click, but
  name equality never selects it: two concepts with the same name (a COL
  concept and a NorTaxa concept, say) are both listed, each with its source;
- a **name-only** row carries a name and no concept at all.

Nothing here writes identity. Callers bind a concept only when the user
explicitly picks its row. Qt-free so the reference picker and the
observation editor share it.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import sqlite3
from typing import Iterable

from utils.taxon_identity import TaxonIdentity

KIND_VERIFIED = "verified"
KIND_NAME_MATCH = "name_match"
KIND_NAME_ONLY = "name_only"

ORIGIN_OBSERVATION = "observation"
ORIGIN_AI = "ai"

#: How many same-name concepts are offered for one suggested name.
NAME_MATCH_LIMIT = 6

_SOURCE_LABELS = {
    "nortaxa": "NorTaxa",
    "col_xr": "COL",
    "col": "COL",
    "dyntaxa": "Dyntaxa",
    "artportalen": "Artportalen",
}


@dataclass(frozen=True)
class TaxonSuggestion:
    """One pickable taxon row and the evidence behind it."""

    kind: str
    origin: str
    scientific_name: str
    genus: str = ""
    species: str = ""
    sporely_taxon_id: int | None = None
    #: The concept's own scientific name when it differs from the suggested
    #: name (a provider id that resolves to a synonym's accepted concept).
    concept_name: str = ""
    #: ``NorTaxa 54701`` / ``COL 6J82Y``: tells same-name concepts apart.
    source_label: str = ""
    #: Verbatim provider identifier (``NBIC:54701``), when there is one.
    raw_external_id: str = ""
    #: AI service key (``arts`` / ``inat``) for AI rows.
    service: str = ""
    vernacular: str = ""
    score: float | None = None
    #: True when the provider identifier did not resolve to one concept
    #: (unknown to the installed taxonomy, or ambiguous).
    external_id_unresolved: bool = False

    @property
    def is_concept(self) -> bool:
        return self.sporely_taxon_id is not None


def _text(value: object) -> str:
    return " ".join(str(value or "").split())


def _split_name(name: str) -> tuple[str, str]:
    parts = _text(name).split(None, 1)
    if not parts:
        return "", ""
    return parts[0], (parts[1] if len(parts) > 1 else "")


def ai_prediction_external_id(prediction: dict | None, taxon: dict | None = None) -> str:
    """The provider identifier an AI prediction carries, verbatim.

    Covers every stored shape: a desktop Artsorakel run (``scientific_name_id``
    at the top level or inside ``taxon``) and an Android/web prediction pulled
    from the cloud (``taxon_id`` and ``taxon.id``, already ``NBIC:``
    prefixed). ``scientific_name_id_shared`` is a GBIF id and is never used.
    """
    for source in (taxon or {}, (prediction or {}).get("taxon") or {}, prediction or {}):
        if not isinstance(source, dict):
            continue
        for key in ("id", "taxonId", "taxon_id", "scientific_name_id", "scientificNameId"):
            value = _text(source.get(key))
            if value:
                return value
    return ""


def resolve_provider_concept(lookup, raw_external_id: str, scientific_name: str = ""):
    """The installed concept a prefixed provider id names, or ``None``.

    Only namespaced identifiers are considered; a bare integer (an
    iNaturalist id, or a namespace-lost value) never resolves.
    """
    raw = _text(raw_external_id)
    if not raw or lookup is None or not hasattr(lookup, "resolve_external_identity"):
        return None
    genus, species = _split_name(scientific_name)
    identity = TaxonIdentity.from_prefixed_external_id(
        raw,
        scientific_name=_text(scientific_name) or None,
        rank="species" if species else ("genus" if genus else None),
    )
    if not identity.has_external_evidence:
        return None
    try:
        return lookup.resolve_external_identity(identity)
    except Exception:
        return None


def _db_path(lookup) -> str | None:
    return getattr(getattr(lookup, "vernacular_db", None), "db_path", None)


def concept_source_labels(lookup, taxon_ids: Iterable[int]) -> dict[int, str]:
    """``{sporely_taxon_id: "NorTaxa 54701"}`` from the installed artifact.

    Missing columns or a missing artifact yield an empty mapping; labels are
    display-only.
    """
    ids = sorted({int(t) for t in taxon_ids if t is not None})
    db_path = _db_path(lookup)
    if not ids or not db_path:
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return {}
    try:
        try:
            rows = conn.execute(
                "SELECT taxon_id, canonical_source_system, canonical_external_id "
                f"FROM taxon_min WHERE taxon_id IN ({placeholders})",
                ids,
            ).fetchall()
        except sqlite3.OperationalError:
            rows = [
                (taxon_id, source, None)
                for taxon_id, source in conn.execute(
                    f"SELECT taxon_id, canonical_source_system FROM taxon_min WHERE taxon_id IN ({placeholders})",
                    ids,
                ).fetchall()
            ]
    except sqlite3.Error:
        return {}
    finally:
        conn.close()
    labels: dict[int, str] = {}
    for taxon_id, source, external_id in rows:
        source_text = _text(source)
        label = _SOURCE_LABELS.get(source_text.casefold(), source_text)
        if label and _text(external_id):
            label = f"{label} {_text(external_id)}"
        if label:
            labels[int(taxon_id)] = label
    return labels


def name_match_concepts(lookup, scientific_name: str, *, origin: str, limit: int = NAME_MATCH_LIMIT) -> list[TaxonSuggestion]:
    """Every installed concept whose own or alias name equals ``scientific_name``.

    Exact (case-insensitive) equality only — never a prefix — and every
    distinct concept is returned, each tagged with its source so same-name
    concepts stay distinguishable. A synonym hit keeps the name it matched
    and records the concept it belongs to.
    """
    name = _text(scientific_name)
    if not name or lookup is None:
        return []
    try:
        hits = lookup.suggest_scientific_names(name, limit=max(limit * 3, 15)) or []
    except Exception:
        return []
    folded = name.casefold()
    out: list[TaxonSuggestion] = []
    seen: set[int] = set()
    for hit in hits:
        hit_name = _text(hit.get("scientific_name"))
        taxon_id = hit.get("sporely_taxon_id")
        if hit_name.casefold() != folded or taxon_id is None:
            continue
        taxon_id = int(taxon_id)
        if taxon_id in seen:
            continue
        seen.add(taxon_id)
        concept = _text(hit.get("display_scientific_name") or hit.get("canonical_scientific_name"))
        genus, species = _split_name(hit_name)
        out.append(TaxonSuggestion(
            kind=KIND_NAME_MATCH,
            origin=origin,
            scientific_name=hit_name,
            genus=genus,
            species=species,
            sporely_taxon_id=taxon_id,
            concept_name=concept if concept and concept != hit_name else "",
        ))
        if len(out) >= limit:
            break
    return out


def ai_entry_suggestion(lookup, entry: dict) -> TaxonSuggestion | None:
    """One AI candidate entry as a verified concept or a name-only row.

    ``entry`` is the host's AI candidate dict (``scientific_name``,
    ``genus``, ``species``, ``vernacular``, ``score``, ``source`` and,
    when the provider gave one, ``external_id``).
    """
    genus = _text(entry.get("genus"))
    species = _text(entry.get("species"))
    # Structured genus/species win over the free scientific-name string,
    # as in the picker's other rows.
    scientific = _text(f"{genus} {species}") if genus and species else _text(entry.get("scientific_name"))
    if not scientific:
        return None
    if not genus:
        genus, species = _split_name(scientific)
    try:
        score = float(entry.get("score"))
    except (TypeError, ValueError):
        score = None
    raw = _text(entry.get("external_id"))
    base = TaxonSuggestion(
        kind=KIND_NAME_ONLY,
        origin=ORIGIN_AI,
        scientific_name=scientific,
        genus=genus,
        species=species,
        raw_external_id=raw,
        service=_text(entry.get("source")),
        vernacular=_text(entry.get("vernacular")),
        score=score,
    )
    concept = resolve_provider_concept(lookup, raw, scientific)
    if concept is None:
        prefixed = TaxonIdentity.from_prefixed_external_id(raw).has_external_evidence if raw else False
        return replace(base, external_id_unresolved=prefixed)
    concept_name = _text(concept.scientific_name)
    return replace(
        base,
        kind=KIND_VERIFIED,
        sporely_taxon_id=int(concept.sporely_taxon_id),
        concept_name=concept_name if concept_name and concept_name != scientific else "",
    )


def reference_taxon_suggestions(
    lookup,
    *,
    own_genus: str = "",
    own_species: str = "",
    own_taxon_id: object = None,
    own_label: str = "",
    ai_entries: Iterable[dict] = (),
) -> list[TaxonSuggestion]:
    """Ordered suggestions for a reference-taxon picker.

    Order: the observation's own taxon when it is a concept, verified AI
    concepts, concepts matching a suggested name, then name-only rows. Rows
    are deduplicated by concept id, never by name; name-only rows (which
    carry no identity) are deduplicated by name and dropped when a listed
    concept has the same name. The observation's own name-only taxon is
    always kept, as the first name-only row. ``own_label`` names the
    observation's taxon when genus and species are not given.
    """
    own_name = _text(f"{own_genus} {own_species}") or _text(own_label)
    own_id = None
    if own_taxon_id not in (None, ""):
        try:
            own_id = int(str(own_taxon_id).strip())
        except (TypeError, ValueError):
            own_id = None

    verified: list[TaxonSuggestion] = []
    name_only: list[TaxonSuggestion] = []
    if own_name and own_id is not None:
        verified.append(TaxonSuggestion(
            kind=KIND_VERIFIED,
            origin=ORIGIN_OBSERVATION,
            scientific_name=own_name,
            genus=_text(own_genus),
            species=_text(own_species),
            sporely_taxon_id=own_id,
        ))
    elif own_name:
        name_only.append(TaxonSuggestion(
            kind=KIND_NAME_ONLY,
            origin=ORIGIN_OBSERVATION,
            scientific_name=own_name,
            genus=_text(own_genus),
            species=_text(own_species),
        ))

    for entry in ai_entries or ():
        if not isinstance(entry, dict):
            continue
        suggestion = ai_entry_suggestion(lookup, entry)
        if suggestion is None or not suggestion.species:
            # A reference taxon is a species; genus-only AI rows add nothing.
            continue
        (verified if suggestion.is_concept else name_only).append(suggestion)

    out: list[TaxonSuggestion] = []
    seen_ids: set[int] = set()
    for suggestion in verified:
        if suggestion.sporely_taxon_id in seen_ids:
            continue
        seen_ids.add(suggestion.sporely_taxon_id)
        out.append(suggestion)

    # Concepts matching a name that has no verified concept. A name that
    # already has a verified concept gets no extra same-name rows: the
    # identifier has settled which concept it is.
    verified_names = {s.scientific_name.casefold() for s in verified}
    matched: list[TaxonSuggestion] = []
    checked_names: set[str] = set()
    for suggestion in name_only:
        folded = suggestion.scientific_name.casefold()
        if folded in checked_names or folded in verified_names:
            continue
        checked_names.add(folded)
        for concept in name_match_concepts(lookup, suggestion.scientific_name, origin=suggestion.origin):
            if concept.sporely_taxon_id in seen_ids:
                continue
            seen_ids.add(concept.sporely_taxon_id)
            matched.append(replace(concept, vernacular=suggestion.vernacular))
    out.extend(matched)

    # An AI name-only row repeating a listed concept's name adds nothing to
    # choose from. The observation's own name-only taxon is always kept: it
    # is what the picker opens on.
    seen_names: set[str] = set(verified_names)
    for suggestion in name_only:
        folded = suggestion.scientific_name.casefold()
        if folded in seen_names and suggestion.origin != ORIGIN_OBSERVATION:
            continue
        seen_names.add(folded)
        out.append(suggestion)

    labels = concept_source_labels(lookup, [s.sporely_taxon_id for s in out if s.is_concept])
    return [
        replace(s, source_label=labels.get(s.sporely_taxon_id, "")) if s.is_concept else s
        for s in out
    ]


def observation_identity_offer(
    lookup,
    observation: dict | None,
    *,
    genus: str = "",
    species: str = "",
    ai_entries: Iterable[dict] = (),
) -> TaxonSuggestion | None:
    """A verified concept to offer an observation that lacks a proven identity.

    Evidence, in order: the observation's own preserved external identity
    (an ``external_unresolved`` provider tuple that the installed taxonomy
    now resolves), then its stored ``ai_selected_taxon_id``, then the AI
    predictions. Only a provider identifier counts as evidence; the
    observation's name is a guard, not evidence: a candidate is offered only
    when its suggested name or its concept name equals the observation's
    current ``genus species``. When candidates name different concepts,
    nothing is offered. Returns ``None`` when the observation already has a
    proven identity.
    """
    row = dict(observation or {})
    identity = TaxonIdentity.from_row(row)
    if identity.is_proven_sporely:
        return None
    current = _text(f"{genus} {species}")
    if not current or not _text(species):
        return None
    folded = current.casefold()
    candidates: list[TaxonSuggestion] = []

    if identity.has_external_evidence and lookup is not None and hasattr(lookup, "resolve_external_identity"):
        try:
            concept = lookup.resolve_external_identity(identity)
        except Exception:
            concept = None
        if concept is not None:
            concept_name = _text(concept.scientific_name)
            candidates.append(TaxonSuggestion(
                kind=KIND_VERIFIED,
                origin=ORIGIN_OBSERVATION,
                scientific_name=_text(identity.scientific_name) or concept_name,
                genus=_text(genus),
                species=_text(species),
                sporely_taxon_id=int(concept.sporely_taxon_id),
                concept_name=concept_name,
                raw_external_id=_text(identity.raw_external_id),
            ))

    entries: list[dict] = []
    selected_id = _text(row.get("ai_selected_taxon_id"))
    if selected_id:
        entries.append({
            "scientific_name": row.get("ai_selected_scientific_name") or "",
            "external_id": selected_id,
            "source": row.get("ai_selected_service") or "",
        })
    entries.extend(e for e in (ai_entries or ()) if isinstance(e, dict))
    for entry in entries:
        suggestion = ai_entry_suggestion(lookup, entry)
        if suggestion is not None and suggestion.is_concept:
            candidates.append(suggestion)

    agreeing = [
        s for s in candidates
        if folded in {s.scientific_name.casefold(), (s.concept_name or "").casefold()}
    ]
    ids = {s.sporely_taxon_id for s in agreeing}
    if len(ids) != 1:
        return None
    offer = agreeing[0]
    labels = concept_source_labels(lookup, ids)
    return replace(offer, source_label=labels.get(offer.sporely_taxon_id, ""))


__all__ = [
    "KIND_NAME_MATCH",
    "KIND_NAME_ONLY",
    "KIND_VERIFIED",
    "ORIGIN_AI",
    "ORIGIN_OBSERVATION",
    "TaxonSuggestion",
    "ai_entry_suggestion",
    "ai_prediction_external_id",
    "concept_source_labels",
    "name_match_concepts",
    "observation_identity_offer",
    "reference_taxon_suggestions",
    "resolve_provider_concept",
]
