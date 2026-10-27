"""Unified taxon search for the observation editor's taxon picker.

One query over the installed taxonomy-v2 artifact: genus and full scientific
names, scientific-name variants (synonyms and older combinations), species
epithets across genera, and vernacular names in the current language, by
prefix and by word part (``kremle`` finds ``grønnkremle``).

Results are **rows of names**, each carrying the concepts it can mean. A row
is a presentation unit, not an identity merge: every concept keeps its own
Sporely id and its full commit data, and nothing here writes identity. Two
concepts share a row only when they carry exactly the same name, rank and
kind and come from different taxonomy sources (a COL concept and a NorTaxa
concept); same-source homonyms always get rows of their own.

Inside a row the concepts are ordered so the first is the default a click
picks: the observation's current concept, then concepts proven by provider
evidence (an Artsorakel id that resolves to exactly one concept), then
concepts with an Artsdatabanken id, then COL. Evidence and the current
concept also rank their rows first within a match tier. They never remove a
concept from the results.

Qt-free; :class:`database.taxon_lookup.TaxonLookupService` wraps it with a
cache and the service's language.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import re
import sqlite3
from typing import Iterable

from database.vernacular_db import (
    _PICKER_EXCLUDED_NAMES,
    _PICKER_TAXON_RANKS,
    _prefix_upper_bound,
    parse_scientific_name_snapshot,
)

TAXON_SEARCH_MIN_CHARS = 2
# Word-part and epithet searches scan rather than seek; below this length
# they would match most of the table and say nothing.
TAXON_SEARCH_PART_MIN_CHARS = 3
TAXON_SEARCH_DEFAULT_LIMIT = 40

# Match tiers, best first. Within a tier rows sort by the text that matched.
TIER_EXACT = 0
TIER_SCIENTIFIC = 1
TIER_VERNACULAR_PREFIX = 2
TIER_EPITHET = 3
TIER_VERNACULAR_PART = 4

MATCH_SCIENTIFIC = "scientific"
MATCH_SYNONYM = "synonym"
MATCH_EPITHET = "epithet"
MATCH_VERNACULAR = "vernacular"
MATCH_VERNACULAR_PART = "vernacular_part"

KIND_TAXON = "taxon"
KIND_SYNONYM = "synonym"

_ROW_RANKS = _PICKER_TAXON_RANKS | {"genus", "aggregate"}
_RANK_ORDER = {"genus": 0, "species": 1, "aggregate": 2, "subspecies": 3, "variety": 4, "form": 5}
_SOURCE_NAMES = {"nortaxa": "NorTaxa", "col_xr": "COL", "col": "COL", "dyntaxa": "Dyntaxa"}
_EPITHET_QUERY = re.compile(r"^[a-z]+(?:-[a-z]*)*$")
_RANK_MARKER_WORDS = ("var.", "subsp.", "ssp.", "coll.", "agg.", "f.")
# Norwegian plural endings of group names (``kremler``, ``vintersopper``,
# ``vintersoppar``): the singular stem finds the member species by word part.
_PLURAL_ENDINGS = ("er", "ar")
_NORWEGIAN_CODES = frozenset({"no", "nb", "nn"})
_MAX_WIDENINGS = 6


@dataclass(frozen=True)
class TaxonSearchConcept:
    """One Sporely concept a search row can bind, with everything a commit needs."""

    sporely_taxon_id: int
    #: The name the selection writes: the row's name (a synonym row writes
    #: the synonym, as the Scientific picker does).
    scientific_name: str
    taxon_rank_snapshot: str
    link_kind: str
    canonical_scientific_name: str
    canonical_rank: str
    canonical_taxonomic_status: str
    canonical_source_system: str
    family: str | None
    display_scientific_name: str
    #: The concept's Artsdatabanken taxon id, when the release links one.
    artsdatabanken_id: int | None = None
    vernacular_name: str | None = None
    is_evidence: bool = False
    is_current: bool = False

    @property
    def source_name(self) -> str:
        """``NorTaxa`` / ``COL``: the label a source chip shows."""
        return source_display_name(self.canonical_source_system)

    def as_scientific_suggestion(self) -> dict:
        """The row shape :meth:`VernacularDB.suggest_scientific_names` returns,
        for the controller's existing Scientific-pick commit path."""
        return {
            "sporely_taxon_id": self.sporely_taxon_id,
            "scientific_name": self.scientific_name,
            "taxon_rank_snapshot": self.taxon_rank_snapshot,
            "is_canonical": self.link_kind == "canonical",
            "canonical_scientific_name": self.canonical_scientific_name,
            "canonical_rank": self.canonical_rank,
            "canonical_taxonomic_status": self.canonical_taxonomic_status,
            "family": self.family,
            "canonical_source_system": self.canonical_source_system,
            "display_scientific_name": self.display_scientific_name,
            "authorship": None,
            "link_kind": self.link_kind,
        }


@dataclass(frozen=True)
class TaxonSearchRow:
    """One pickable name and the concepts it can mean (default first)."""

    name: str
    rank: str
    kind: str
    match_kind: str
    matched_text: str
    tier: int
    concepts: tuple[TaxonSearchConcept, ...]
    #: Vernacular shown beside the name: the matched one for a vernacular
    #: hit, else the default concept's best name in the search language.
    vernacular_name: str | None = None
    #: For a synonym row, the concept name it points to.
    synonym_of: str | None = None
    #: Another row in the same results carries exactly this name and rank
    #: (a same-source homonym, or a split group): show the family.
    shares_name: bool = False

    @property
    def default_concept(self) -> TaxonSearchConcept:
        return self.concepts[0]

    @property
    def has_source_choice(self) -> bool:
        return len(self.concepts) > 1


@dataclass(frozen=True)
class TaxonSearchPage:
    rows: tuple[TaxonSearchRow, ...] = ()
    has_more: bool = False
    offset: int = 0


def source_display_name(source_system: str | None) -> str:
    text = " ".join(str(source_system or "").split())
    return _SOURCE_NAMES.get(text.casefold(), text)


def search_language_codes(language_code: str | None) -> tuple[str, ...]:
    """Vernacular codes to search, in display preference order (``nb``
    before the umbrella ``no`` before ``nn``)."""
    from utils.vernacular_utils import resolve_query_language_codes

    codes = resolve_query_language_codes(language_code)
    preferred = [code for code in ("nb", "no", "nn") if code in codes]
    return tuple(preferred + [code for code in codes if code not in preferred])


# ----------------------------------------------------------------------------
# Hits: one (concept, name) match from one source
# ----------------------------------------------------------------------------


@dataclass
class _Hit:
    taxon_id: int
    row_name: str
    rank: str
    kind: str
    link_kind: str
    match_kind: str
    matched_text: str
    tier: int
    sort_text: str
    vernacular: str | None = None
    #: Position of the vernacular's language in the search's preference
    #: order: with the umbrella ``no``, a Bokmål name beats the Nynorsk one
    #: for the same row.
    lang_rank: int = 0


@dataclass
class _Source:
    """One ordered scan. ``boundary`` is the sort text of its last row when
    it stopped at its cap: rows of its tier at or past that text may be
    incomplete."""

    tier: int
    hits: list[_Hit] = field(default_factory=list)
    boundary: str | None = None


_ASCII_FOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _nocase(text: str) -> str:
    """SQLite's ``NOCASE`` collation key (folds ASCII only), so Python orders
    vernacular rows exactly as the scans that produced them."""
    return text.translate(_ASCII_FOLD)


def _norm(text: object) -> str:
    return " ".join(str(text or "").split())


def _rank_snapshot(name: str, structured_rank: str | None) -> str | None:
    """The picker rank of a name, or ``None`` when the picker cannot offer it."""
    if not name or "(" in name or name in _PICKER_EXCLUDED_NAMES:
        return None
    parsed = parse_scientific_name_snapshot(name)
    structured = str(structured_rank or "").strip().lower()
    if parsed is not None:
        rank = parsed[2]
        if rank == "genus" and structured not in ("genus", ""):
            # A family or order's one-word name parses like a genus.
            return None
        return rank if rank in _ROW_RANKS else None
    return None


def _like_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _case_variants(query: str) -> list[str]:
    variants = [query]
    for variant in (query[:1].upper() + query[1:].lower(), query.lower()):
        if variant not in variants:
            variants.append(variant)
    return variants


def _tier(matched: str, query: str, base: int) -> int:
    return TIER_EXACT if matched.casefold() == query.casefold() else base


def _scientific_sources(conn, query: str, cap: int) -> list[_Source]:
    sources: list[_Source] = []
    for lo in _case_variants(query):
        hi = _prefix_upper_bound(lo)
        canonical = _Source(TIER_SCIENTIFIC)
        rows = conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank FROM taxon_min "
            "WHERE canonical_scientific_name >= ? AND canonical_scientific_name < ? "
            "ORDER BY canonical_scientific_name LIMIT ?",
            (lo, hi, cap),
        ).fetchall()
        for taxon_id, name, rank in rows:
            snapshot = _rank_snapshot(name, rank)
            if snapshot is None:
                continue
            canonical.hits.append(_Hit(
                int(taxon_id), name, snapshot, KIND_TAXON, "canonical", MATCH_SCIENTIFIC,
                name, _tier(name, query, TIER_SCIENTIFIC), name,
            ))
        if len(rows) >= cap:
            canonical.boundary = rows[-1][1]
        sources.append(canonical)

        alias = _Source(TIER_SCIENTIFIC)
        rows = conn.execute(
            "SELECT s.taxon_id, s.scientific_name, t.canonical_scientific_name, t.taxon_rank "
            "FROM scientific_name_min s JOIN taxon_min t ON t.taxon_id = s.taxon_id "
            "WHERE s.language_code = 'sci' AND s.is_preferred_name = 0 "
            "  AND s.scientific_name >= ? AND s.scientific_name < ? "
            "ORDER BY s.scientific_name LIMIT ?",
            (lo, hi, cap),
        ).fetchall()
        for taxon_id, name, canonical_name, rank in rows:
            hit = _alias_hit(int(taxon_id), name, canonical_name, rank, query, TIER_SCIENTIFIC, MATCH_SYNONYM)
            if hit is not None:
                alias.hits.append(hit)
        if len(rows) >= cap:
            alias.boundary = rows[-1][1]
        sources.append(alias)
    return sources


def _alias_hit(taxon_id, name, canonical_name, structured_rank, query, base_tier, match_kind) -> _Hit | None:
    """A ``scientific_name_min`` alias as a synonym row hit (or ``None``).

    The alias's rank comes from the name itself. Equal to the concept's rank
    it is a synonym of that concept; otherwise the concept is linked (an
    infraspecific name kept on its species, say). Same rule as the
    Scientific picker.
    """
    name = _norm(name)
    if not name or name == _norm(canonical_name):
        return None
    snapshot = _rank_snapshot(name, None)
    if snapshot is None:
        return None
    if snapshot == "genus" and str(structured_rank or "").lower() != "genus":
        return None
    concept_rank = str(structured_rank or "").lower()
    link_kind = "synonym_of_accepted" if concept_rank == snapshot or (
        snapshot == "aggregate" and concept_rank == "species"
    ) else "linked"
    return _Hit(
        taxon_id, name, snapshot, KIND_SYNONYM, link_kind, match_kind,
        name, _tier(name, query, base_tier), name,
    )


def _epithet_token(query: str) -> str | None:
    """The lower-case epithet a query can stand for, or ``None``."""
    token = query.strip().lower()
    if " " in token or len(token) < TAXON_SEARCH_PART_MIN_CHARS or not _EPITHET_QUERY.match(token):
        return None
    return token


def _epithet_matches(name: str, token: str) -> bool:
    """Whether a word after the genus starts with ``token`` and is not a rank
    marker (``var``, ``subsp`` would otherwise match every variety)."""
    return any(
        word.lower().startswith(token) and word not in _RANK_MARKER_WORDS
        for word in name.split()[1:]
    )


def _epithet_clause(token: str) -> tuple[str, list[str]]:
    """``GLOB`` filter for names with a word starting with ``token``. When the
    token begins a rank marker, a name matching only through that marker is
    excluded in SQL, so the marker cannot fill the scan's cap."""
    clauses = ["s.scientific_name GLOB ?"]
    params = [f"* {token}*"]
    for marker in _RANK_MARKER_WORDS:
        if not marker.startswith(token):
            continue
        # A real word starting with the token departs from the marker at
        # some character at or after the token's end.
        real = [f"* {marker[:i]}[!{marker[i]}]*" for i in range(len(token), len(marker))]
        clauses.append(
            "(s.scientific_name NOT GLOB ? OR "
            + " OR ".join("s.scientific_name GLOB ?" for _ in real) + ")"
        )
        params += [f"* {marker}*", *real]
    return " AND ".join(clauses), params


def _epithet_source(conn, query: str, cap: int) -> _Source | None:
    """Names with a word starting with ``query`` after the genus: the species
    epithet across genera, and infraspecific epithets. Scans
    ``scientific_name_min``, which holds every canonical name too."""
    token = _epithet_token(query)
    if token is None:
        return None
    source = _Source(TIER_EPITHET)
    clause, params = _epithet_clause(token)
    rows = conn.execute(
        "SELECT s.taxon_id, s.scientific_name, s.is_preferred_name, "
        "       t.canonical_scientific_name, t.taxon_rank "
        "FROM scientific_name_min s JOIN taxon_min t ON t.taxon_id = s.taxon_id "
        f"WHERE s.language_code = 'sci' AND {clause} "
        "ORDER BY s.scientific_name LIMIT ?",
        (*params, cap),
    ).fetchall()
    for taxon_id, name, is_preferred, canonical_name, rank in rows:
        name = _norm(name)
        if not _epithet_matches(name, token):
            continue
        if name == _norm(canonical_name):
            snapshot = _rank_snapshot(name, rank)
            if snapshot is None or snapshot == "genus":
                continue
            source.hits.append(_Hit(
                int(taxon_id), name, snapshot, KIND_TAXON, "canonical", MATCH_EPITHET,
                name, TIER_EPITHET, name,
            ))
        elif not is_preferred:
            hit = _alias_hit(int(taxon_id), name, canonical_name, rank, query, TIER_EPITHET, MATCH_EPITHET)
            if hit is not None:
                hit.tier = TIER_EPITHET
                source.hits.append(hit)
    if len(rows) >= cap:
        source.boundary = _norm(rows[-1][1])
    return source


def _vernacular_hit(taxon_id, vernacular, canonical_name, rank, tier, match_kind) -> _Hit | None:
    canonical_name = _norm(canonical_name)
    snapshot = _rank_snapshot(canonical_name, rank)
    if snapshot is None:
        return None
    return _Hit(
        int(taxon_id), canonical_name, snapshot, KIND_TAXON, "canonical", match_kind,
        vernacular, tier, _nocase(vernacular), vernacular=vernacular,
    )


def _vernacular_prefix_sources(conn, query: str, codes: tuple[str, ...], cap: int) -> list[_Source]:
    sources: list[_Source] = []
    for code in codes:
        for lo in _case_variants(query):
            source = _Source(TIER_VERNACULAR_PREFIX)
            rows = conn.execute(
                "SELECT v.taxon_id, v.vernacular_name, t.canonical_scientific_name, t.taxon_rank "
                "FROM vernacular_min v JOIN taxon_min t ON t.taxon_id = v.taxon_id "
                "WHERE v.language_code = ? AND v.vernacular_name >= ? AND v.vernacular_name < ? "
                "ORDER BY v.vernacular_name COLLATE NOCASE LIMIT ?",
                (code, lo, _prefix_upper_bound(lo), cap),
            ).fetchall()
            for taxon_id, vernacular, canonical_name, rank in rows:
                vernacular = _norm(vernacular)
                hit = _vernacular_hit(
                    taxon_id, vernacular, canonical_name, rank,
                    _tier(vernacular, query, TIER_VERNACULAR_PREFIX), MATCH_VERNACULAR,
                )
                if hit is not None:
                    hit.lang_rank = codes.index(code)
                    source.hits.append(hit)
            if len(rows) >= cap:
                source.boundary = _nocase(_norm(rows[-1][1]))
            sources.append(source)
    return sources


def _word_part_stems(query: str, codes: tuple[str, ...]) -> list[str]:
    stems = [query]
    if _NORWEGIAN_CODES & set(codes) and " " not in query:
        for ending in _PLURAL_ENDINGS:
            stem = query[: -len(ending)]
            if query.lower().endswith(ending) and len(stem) >= TAXON_SEARCH_PART_MIN_CHARS + 1:
                stems.append(stem)
    return stems


def _vernacular_part_sources(conn, query: str, codes: tuple[str, ...], cap: int) -> list[_Source]:
    if len(query) < TAXON_SEARCH_PART_MIN_CHARS:
        return []
    sources: list[_Source] = []
    for code in codes:
        for stem in _word_part_stems(query, codes):
            source = _Source(TIER_VERNACULAR_PART)
            rows = conn.execute(
                "SELECT v.taxon_id, v.vernacular_name, t.canonical_scientific_name, t.taxon_rank "
                "FROM vernacular_min v JOIN taxon_min t ON t.taxon_id = v.taxon_id "
                "WHERE v.language_code = ? AND v.vernacular_name LIKE ? ESCAPE '\\' "
                "ORDER BY v.vernacular_name COLLATE NOCASE LIMIT ?",
                (code, f"%{_like_escape(stem)}%", cap),
            ).fetchall()
            for taxon_id, vernacular, canonical_name, rank in rows:
                vernacular = _norm(vernacular)
                if vernacular.casefold().startswith(query.casefold()):
                    continue  # the prefix source has it, in a better tier
                hit = _vernacular_hit(
                    taxon_id, vernacular, canonical_name, rank,
                    TIER_VERNACULAR_PART, MATCH_VERNACULAR_PART,
                )
                if hit is not None:
                    hit.lang_rank = codes.index(code)
                    source.hits.append(hit)
            if len(rows) >= cap:
                source.boundary = _nocase(_norm(rows[-1][1]))
            sources.append(source)
    return sources


def _boosted_hits(conn, query: str, codes: tuple[str, ...], taxon_ids: Iterable[int]) -> list[_Hit]:
    """Hits for the current and evidence concepts, found directly so the
    ranking boost never depends on where a capped scan stopped."""
    ids = sorted({int(t) for t in taxon_ids if t is not None})
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    folded = query.casefold()
    token = _epithet_token(query)
    hits: list[_Hit] = []
    for taxon_id, name, rank in conn.execute(
        f"SELECT taxon_id, canonical_scientific_name, taxon_rank FROM taxon_min WHERE taxon_id IN ({placeholders})",
        ids,
    ):
        name = _norm(name)
        snapshot = _rank_snapshot(name, rank)
        if snapshot is None:
            continue
        if name.casefold().startswith(folded):
            hits.append(_Hit(int(taxon_id), name, snapshot, KIND_TAXON, "canonical", MATCH_SCIENTIFIC,
                             name, _tier(name, query, TIER_SCIENTIFIC), name))
        elif token is not None and _epithet_matches(name, token):
            hits.append(_Hit(int(taxon_id), name, snapshot, KIND_TAXON, "canonical", MATCH_EPITHET,
                             name, TIER_EPITHET, name))
    for taxon_id, alias, canonical_name, rank in conn.execute(
        "SELECT s.taxon_id, s.scientific_name, t.canonical_scientific_name, t.taxon_rank "
        "FROM scientific_name_min s JOIN taxon_min t ON t.taxon_id = s.taxon_id "
        f"WHERE s.language_code = 'sci' AND s.is_preferred_name = 0 AND s.taxon_id IN ({placeholders})",
        ids,
    ):
        if _norm(alias).casefold().startswith(folded):
            hit = _alias_hit(int(taxon_id), alias, canonical_name, rank, query, TIER_SCIENTIFIC, MATCH_SYNONYM)
            if hit is not None:
                hits.append(hit)
    if codes:
        code_marks = ",".join("?" for _ in codes)
        stems = [s.casefold() for s in _word_part_stems(query, codes)] if len(query) >= TAXON_SEARCH_PART_MIN_CHARS else []
        for taxon_id, code, vernacular, canonical_name, rank in conn.execute(
            "SELECT v.taxon_id, v.language_code, v.vernacular_name, t.canonical_scientific_name, t.taxon_rank "
            "FROM vernacular_min v JOIN taxon_min t ON t.taxon_id = v.taxon_id "
            f"WHERE v.language_code IN ({code_marks}) AND v.taxon_id IN ({placeholders})",
            (*codes, *ids),
        ):
            vernacular = _norm(vernacular)
            folded_name = vernacular.casefold()
            if folded_name.startswith(folded):
                hit = _vernacular_hit(taxon_id, vernacular, canonical_name, rank,
                                      _tier(vernacular, query, TIER_VERNACULAR_PREFIX), MATCH_VERNACULAR)
            elif any(stem in folded_name for stem in stems):
                hit = _vernacular_hit(taxon_id, vernacular, canonical_name, rank,
                                      TIER_VERNACULAR_PART, MATCH_VERNACULAR_PART)
            else:
                hit = None
            if hit is not None:
                hit.lang_rank = codes.index(code) if code in codes else len(codes)
                hits.append(hit)
    return hits


# ----------------------------------------------------------------------------
# Rows
# ----------------------------------------------------------------------------


@dataclass
class _RowDraft:
    key: tuple
    best: _Hit
    taxon_ids: dict[int, _Hit]
    synonym_target: str | None = None


def _row_key(hit: _Hit, canonical_names: dict[int, str]) -> tuple:
    target = canonical_names.get(hit.taxon_id, "") if hit.kind == KIND_SYNONYM else ""
    return (hit.kind, hit.row_name, hit.rank, target)


def _hit_order(hit: _Hit) -> tuple:
    return (hit.tier, hit.lang_rank, hit.sort_text)


def _concept_details(conn, ids: list[int], codes: tuple[str, ...], national_expr: str) -> dict[int, dict]:
    details: dict[int, dict] = {}
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        marks = ",".join("?" for _ in chunk)
        for row in conn.execute(
            "SELECT t.taxon_id, t.canonical_scientific_name, t.taxon_rank, t.taxonomic_status, "
            f"       t.family, t.canonical_source_system, {national_expr} "
            f"FROM taxon_min t WHERE t.taxon_id IN ({marks})",
            chunk,
        ):
            taxon_id, name, rank, status, family, source, national = row
            details[int(taxon_id)] = {
                "canonical_scientific_name": _norm(name),
                "canonical_rank": str(rank or ""),
                "canonical_taxonomic_status": str(status or ""),
                "family": _norm(family) or None,
                "canonical_source_system": str(source or ""),
                "display_scientific_name": _norm(national) or _norm(name),
                "artsdatabanken_id": None,
                "vernacular_name": None,
            }
        try:
            for taxon_id, external_id in conn.execute(
                "SELECT taxon_id, external_id FROM taxon_external_id_min "
                f"WHERE source_system = 'artsdatabanken' AND id_role = 'accepted' AND taxon_id IN ({marks}) "
                "ORDER BY is_preferred DESC, external_id",
                chunk,
            ):
                entry = details.get(int(taxon_id))
                if entry is not None and entry["artsdatabanken_id"] is None:
                    entry["artsdatabanken_id"] = int(external_id)
        except sqlite3.OperationalError:
            pass
        if codes:
            rank_of = {code: i for i, code in enumerate(codes)}
            best: dict[int, tuple] = {}
            code_marks = ",".join("?" for _ in codes)
            for taxon_id, code, name, preferred in conn.execute(
                "SELECT taxon_id, language_code, vernacular_name, is_preferred_name FROM vernacular_min "
                f"WHERE language_code IN ({code_marks}) AND taxon_id IN ({marks})",
                (*codes, *chunk),
            ):
                order = (0 if preferred else 1, rank_of.get(code, 99), str(name))
                if int(taxon_id) not in best or order < best[int(taxon_id)]:
                    best[int(taxon_id)] = order
            for taxon_id, order in best.items():
                if taxon_id in details:
                    details[taxon_id]["vernacular_name"] = _norm(order[2])
    return details


def _same_name_siblings(conn, drafts: list[_RowDraft]) -> dict[tuple, list[int]]:
    """Every concept carrying a taxon row's exact canonical name and rank,
    so a row offers both sources even when only one of them matched (the
    other may lack the vernacular that was typed)."""
    names = sorted({d.key[1] for d in drafts if d.key[0] == KIND_TAXON})
    found: dict[tuple, list[int]] = {}
    for start in range(0, len(names), 500):
        chunk = names[start:start + 500]
        marks = ",".join("?" for _ in chunk)
        for taxon_id, name, rank in conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank FROM taxon_min "
            f"WHERE canonical_scientific_name IN ({marks})",
            chunk,
        ):
            snapshot = _rank_snapshot(_norm(name), rank)
            if snapshot is not None:
                found.setdefault((KIND_TAXON, _norm(name), snapshot, ""), []).append(int(taxon_id))
    return found


def _concept_order(concept: TaxonSearchConcept) -> tuple:
    return (
        0 if concept.is_current else 1,
        0 if concept.is_evidence else 1,
        0 if concept.artsdatabanken_id is not None else 1,
        0 if concept.canonical_source_system in ("col_xr", "col") else 1,
        concept.sporely_taxon_id,
    )


def _build_rows(conn, query, codes, national_expr, hits, evidence_ids, current_id) -> list[TaxonSearchRow]:
    if not hits:
        return []
    canonical_names = {
        int(tid): _norm(name)
        for tid, name in _rows_by_ids(conn, "SELECT taxon_id, canonical_scientific_name FROM taxon_min",
                                      {h.taxon_id for h in hits if h.kind == KIND_SYNONYM})
    }
    drafts: dict[tuple, _RowDraft] = {}
    for hit in hits:
        key = _row_key(hit, canonical_names)
        draft = drafts.get(key)
        if draft is None:
            drafts[key] = _RowDraft(key, hit, {hit.taxon_id: hit},
                                    canonical_names.get(hit.taxon_id) if hit.kind == KIND_SYNONYM else None)
            continue
        if _hit_order(hit) < _hit_order(draft.best):
            draft.best = hit
        held = draft.taxon_ids.get(hit.taxon_id)
        if held is None or _hit_order(hit) < _hit_order(held):
            draft.taxon_ids[hit.taxon_id] = hit
    siblings = _same_name_siblings(conn, list(drafts.values()))
    for key, ids in siblings.items():
        draft = drafts.get(key)
        if draft is None:
            continue
        for taxon_id in ids:
            draft.taxon_ids.setdefault(taxon_id, None)

    all_ids = sorted({tid for d in drafts.values() for tid in d.taxon_ids})
    details = _concept_details(conn, all_ids, codes, national_expr)

    rows: list[TaxonSearchRow] = []
    for draft in drafts.values():
        concepts = []
        for taxon_id, hit in draft.taxon_ids.items():
            info = details.get(taxon_id)
            if info is None:
                continue
            concepts.append(TaxonSearchConcept(
                sporely_taxon_id=taxon_id,
                scientific_name=draft.key[1],
                taxon_rank_snapshot=draft.key[2],
                link_kind=hit.link_kind if hit is not None else "canonical",
                canonical_scientific_name=info["canonical_scientific_name"],
                canonical_rank=info["canonical_rank"],
                canonical_taxonomic_status=info["canonical_taxonomic_status"],
                canonical_source_system=info["canonical_source_system"],
                family=info["family"],
                display_scientific_name=info["display_scientific_name"],
                artsdatabanken_id=info["artsdatabanken_id"],
                vernacular_name=info["vernacular_name"],
                is_evidence=taxon_id in evidence_ids,
                is_current=taxon_id == current_id,
            ))
        if not concepts:
            continue
        # A row offers a source choice only between different sources; two
        # concepts from one source are homonyms and each get a row.
        sources = [c.canonical_source_system for c in concepts]
        groups = [concepts] if len(set(sources)) == len(sources) else [[c] for c in concepts]
        for group in groups:
            group.sort(key=_concept_order)
            best = draft.best
            if len(groups) > 1:
                own = [draft.taxon_ids.get(c.sporely_taxon_id) for c in group]
                own = [h for h in own if h is not None]
                if not own:
                    continue  # a sibling that matched nothing on its own
                best = min(own, key=_hit_order)
            vernacular = best.vernacular if best.vernacular else group[0].vernacular_name
            rows.append(TaxonSearchRow(
                name=draft.key[1],
                rank=draft.key[2],
                kind=draft.key[0],
                match_kind=best.match_kind,
                matched_text=best.matched_text,
                tier=best.tier,
                concepts=tuple(group),
                vernacular_name=vernacular or None,
                synonym_of=(group[0].display_scientific_name or draft.synonym_target) if draft.key[0] == KIND_SYNONYM else None,
            ))
    counts: dict[tuple, int] = {}
    for row in rows:
        counts[(row.name, row.rank)] = counts.get((row.name, row.rank), 0) + 1
    return [
        row if counts[(row.name, row.rank)] == 1 else _with_shared_name(row)
        for row in rows
    ]


def _with_shared_name(row: TaxonSearchRow) -> TaxonSearchRow:
    return replace(row, shares_name=True)


def _rows_by_ids(conn, select_sql: str, ids) -> list:
    ids = sorted({int(t) for t in ids})
    out: list = []
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        marks = ",".join("?" for _ in chunk)
        out.extend(conn.execute(f"{select_sql} WHERE taxon_id IN ({marks})", chunk).fetchall())
    return out


def _row_sort_text(row: TaxonSearchRow) -> str:
    """The text a row sorts by within its tier, in its source's collation."""
    if row.match_kind in (MATCH_VERNACULAR, MATCH_VERNACULAR_PART):
        return _nocase(row.matched_text)
    return row.matched_text


def _row_sort_key(row: TaxonSearchRow) -> tuple:
    boosted = any(c.is_current or c.is_evidence for c in row.concepts)
    return (
        row.tier,
        0 if boosted else 1,
        _row_sort_text(row),
        0 if row.kind == KIND_TAXON else 1,
        _RANK_ORDER.get(row.rank, 9),
        row.name,
        row.default_concept.sporely_taxon_id,
    )


def _is_safe(row: TaxonSearchRow, sources: list[_Source]) -> bool:
    """Whether every row that sorts before ``row`` is certainly present:
    no capped source could still hold a row ahead of it."""
    boosted = any(c.is_current or c.is_evidence for c in row.concepts)
    for source in sources:
        if source.boundary is None:
            continue
        if row.tier > source.tier:
            return False
        if row.tier == source.tier and not boosted and _row_sort_text(row) >= source.boundary:
            return False
    return True


def search(
    conn: sqlite3.Connection,
    query: str,
    *,
    language_codes: tuple[str, ...] = (),
    national_expr: str = "NULL",
    offset: int = 0,
    limit: int = TAXON_SEARCH_DEFAULT_LIMIT,
    evidence_taxon_ids: Iterable[int] = (),
    current_taxon_id: int | None = None,
) -> TaxonSearchPage:
    """Rows ``offset .. offset+limit`` of the ranked search, and whether more follow."""
    query = _norm(query)
    offset = max(0, int(offset))
    limit = max(0, int(limit))
    if len(query) < TAXON_SEARCH_MIN_CHARS or limit == 0:
        return TaxonSearchPage(offset=offset)
    evidence = frozenset(int(t) for t in evidence_taxon_ids if t is not None)
    current = int(current_taxon_id) if current_taxon_id is not None else None
    need = offset + limit + 1
    cap = max(need * 3, 60)
    for _ in range(_MAX_WIDENINGS):
        sources = _scientific_sources(conn, query, cap)
        epithet = _epithet_source(conn, query, cap)
        if epithet is not None:
            sources.append(epithet)
        sources += _vernacular_prefix_sources(conn, query, language_codes, cap)
        sources += _vernacular_part_sources(conn, query, language_codes, cap)
        hits = [h for s in sources for h in s.hits]
        hits += _boosted_hits(conn, query, language_codes, evidence | ({current} if current else set()))
        rows = sorted(
            _build_rows(conn, query, language_codes, national_expr, hits, evidence, current),
            key=_row_sort_key,
        )
        truncated = any(s.boundary is not None for s in sources)
        safe = 0
        while safe < len(rows) and _is_safe(rows[safe], sources):
            safe += 1
        if not truncated or safe >= need:
            break
        cap *= 4
    else:
        truncated = True
    page = rows[offset:offset + limit]
    has_more = len(rows) > offset + limit or (truncated and safe <= offset + limit)
    return TaxonSearchPage(rows=tuple(page), has_more=has_more, offset=offset)


__all__ = [
    "KIND_SYNONYM", "KIND_TAXON",
    "MATCH_EPITHET", "MATCH_SCIENTIFIC", "MATCH_SYNONYM", "MATCH_VERNACULAR", "MATCH_VERNACULAR_PART",
    "TAXON_SEARCH_DEFAULT_LIMIT", "TAXON_SEARCH_MIN_CHARS",
    "TaxonSearchConcept", "TaxonSearchPage", "TaxonSearchRow",
    "search", "search_language_codes", "source_display_name",
]
