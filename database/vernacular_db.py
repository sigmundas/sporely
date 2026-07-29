"""SQLite helper for vernacular name lookup."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from utils.vernacular_utils import (
    normalize_vernacular_language,
    resolve_query_language_codes,
)


class VernacularDB:
    """Simple helper for vernacular name lookup."""

    def __init__(self, db_path: Path, language_code: str | None = None):
        self.db_path = db_path
        # Preserve `nb`/`nn`/Sámi codes verbatim — the taxonomy v2 identity
        # contract keeps them distinct. `resolve_query_language_codes` in
        # `_language_clause` handles the umbrella `no` → `('no','nb','nn')`
        # fan-out.
        self.language_code = language_code.strip() if language_code else None
        self._has_language_column = None
        self._tables: set[str] | None = None

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _table_names(self) -> set[str]:
        if self._tables is None:
            with self._connect() as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                self._tables = {str(row[0] or "") for row in cur.fetchall()}
        return self._tables

    def _has_scientific_name_table(self) -> bool:
        return "scientific_name_min" in self._table_names()

    def _is_v2(self) -> bool:
        """A v2 candidate carries a ``taxonomy_meta`` table. Legacy multi-
        language DBs do not. When we detect legacy, the language clause
        keeps the umbrella `nb`/`nn`→`no` behavior for backwards compat."""
        return "taxonomy_meta" in self._table_names()

    def _has_language(self) -> bool:
        if self._has_language_column is None:
            with self._connect() as conn:
                cur = conn.execute("PRAGMA table_info(vernacular_min)")
                self._has_language_column = any(row[1] == "language_code" for row in cur.fetchall())
        return bool(self._has_language_column)

    def _language_clause(self, language_code: str | None) -> tuple[str, list[str]]:
        """Language filter.

        For **v2** taxonomy candidates (``taxonomy_meta`` present) the codes
        ``nb``, ``nn`` and Sámi variants are queried distinctly, and the
        umbrella ``no`` fans out to ``('no','nb','nn')`` per the identity
        contract. For **legacy** DBs the pre-existing umbrella behavior
        (``normalize_vernacular_language`` collapse) is preserved to keep
        old production callers unbroken.
        """
        if not self._has_language():
            return "", []
        raw = language_code or self.language_code
        if not raw:
            return "", []
        if self._is_v2():
            codes = resolve_query_language_codes(raw)
            if not codes:
                return "", []
            placeholders = ",".join("?" for _ in codes)
            return f" AND v.language_code IN ({placeholders}) ", list(codes)
        lang = normalize_vernacular_language(raw)
        if not lang:
            return "", []
        return " AND v.language_code = ? ", [lang]

    def list_languages(self) -> list[str]:
        if not self._has_language():
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT language_code
                FROM vernacular_min
                WHERE language_code IS NOT NULL AND language_code != ''
                ORDER BY language_code
                """
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular(self, prefix: str, genus: str | None = None, species: str | None = None) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.vernacular_name
                LIMIT 200
                """,
                (prefix, genus, genus, species, species, *lang_params),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular_entries(
        self,
        prefix: str,
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        prefix = prefix.strip()
        if not prefix:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name, t.genus, t.specific_epithet, t.family, v.is_preferred_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ? COLLATE NOCASE)
                  AND (? IS NULL OR t.specific_epithet = ? COLLATE NOCASE)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name, t.genus, t.specific_epithet
                LIMIT ?
                """,
                (prefix, genus, genus, species, species, *lang_params, int(limit)),
            )
            return [
                {
                    "vernacular_name": row[0],
                    "genus": row[1],
                    "species": row[2],
                    "family": row[3],
                    "is_preferred_name": bool(row[4]),
                }
                for row in cur.fetchall()
                if row and row[0] and row[1] and row[2]
            ]

    def suggest_vernacular_for_taxon(
        self, genus: str | None = None, species: str | None = None, limit: int = 200
    ) -> list[str]:
        genus = genus.strip() if genus else None
        species = species.strip() if species else None
        if not genus and not species:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT ?
                """,
                (genus, genus, species, species, *lang_params, limit),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_genus(self, prefix: str) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT genus
                FROM taxon_min
                WHERE genus LIKE ? || '%'
                ORDER BY genus
                LIMIT 200
                """,
                (prefix,),
            )
            for row in cur.fetchall():
                genus = str(row[0] or "").strip()
                lowered = genus.casefold()
                if genus and lowered not in seen:
                    seen.add(lowered)
                    values.append(genus)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' %'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (prefix,),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    genus = scientific_name.split(" ", 1)[0].strip() if scientific_name else ""
                    lowered = genus.casefold()
                    if genus and lowered not in seen:
                        seen.add(lowered)
                        values.append(genus)
            return values[:200]

    def suggest_species(self, genus: str, prefix: str) -> list[str]:
        genus = genus.strip()
        prefix = prefix.strip()
        if not genus:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT specific_epithet
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet LIKE ? || '%'
                ORDER BY specific_epithet
                LIMIT 200
                """,
                (genus, prefix),
            )
            for row in cur.fetchall():
                species = str(row[0] or "").strip()
                lowered = species.casefold()
                if species and lowered not in seen:
                    seen.add(lowered)
                    values.append(species)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' ' || ? || '%'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (genus, prefix),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    parts = scientific_name.split()
                    if len(parts) < 2 or parts[0].casefold() != genus.casefold():
                        continue
                    species = parts[1].strip()
                    lowered = species.casefold()
                    if species and lowered not in seen:
                        seen.add(lowered)
                        values.append(species)
            return values[:200]

    def taxon_from_scientific(self, genus: str, species: str) -> tuple[str, str, str | None] | None:
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        scientific_name = f"{genus} {species}".strip()
        with self._connect() as conn:
            cur = conn.cursor()
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT t.genus, t.specific_epithet, t.family
                    FROM taxon_min t
                    LEFT JOIN scientific_name_min s ON s.taxon_id = t.taxon_id
                    WHERE (
                            t.genus = ? COLLATE NOCASE
                        AND t.specific_epithet = ? COLLATE NOCASE
                    )
                       OR (
                            t.canonical_scientific_name = ? COLLATE NOCASE
                    )
                       OR (
                            s.scientific_name = ? COLLATE NOCASE
                    )
                    ORDER BY
                        CASE
                            WHEN t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE THEN 0
                            WHEN s.is_preferred_name = 1 THEN 1
                            ELSE 2
                        END,
                        t.genus,
                        t.specific_epithet
                    LIMIT 1
                    """,
                    (genus, species, scientific_name, scientific_name, genus, species),
                )
            else:
                cur.execute(
                    """
                    SELECT genus, specific_epithet, family
                    FROM taxon_min
                    WHERE genus = ? COLLATE NOCASE
                      AND specific_epithet = ? COLLATE NOCASE
                    ORDER BY genus, specific_epithet
                    LIMIT 1
                    """,
                    (genus, species),
                )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_from_vernacular(self, name: str) -> tuple[str, str, str | None] | None:
        name = name.strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.genus, t.specific_epithet, t.family
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name = ?
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (name, *lang_params),
            )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_ids_from_scientific(self, genus: str, species: str) -> list[int]:
        """Return every taxon_id that matches ``genus + species``.

        Stage 3A conservative rule preserves distinct concepts that happen
        to share a scientific name when authorship disagrees, so this can
        legitimately return more than one Sporely id (e.g. two ``Laccaria
        laccata`` concepts). The caller decides how to handle multiplicity;
        callers holding a known ``sporely_taxon_id`` MUST NOT use this to
        re-resolve identity.
        """
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return []
        with self._connect() as conn:
            rows = list(conn.execute(
                """
                SELECT DISTINCT t.taxon_id FROM taxon_min t
                WHERE (t.genus = ? COLLATE NOCASE
                       AND t.specific_epithet = ? COLLATE NOCASE)
                   OR t.canonical_scientific_name = ? COLLATE NOCASE
                ORDER BY t.taxon_id
                """,
                (genus, species, f"{genus} {species}"),
            ))
        return [int(r[0]) for r in rows]

    def taxon_id_from_scientific(self, genus: str, species: str) -> int | None:
        """Return a taxon_id for a scientific name ONLY if the match is
        unique (or a preferred-alias uniquely breaks the tie).

        Returns ``None`` when zero or multiple rows would qualify. This is
        the safe API for post-selection lookups; callers with an ambiguous
        (genus, species) pair must obtain identity through an explicit
        suggestion selection, not this method.
        """
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        candidates = self.taxon_ids_from_scientific(genus, species)
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        # Multiple canonical rows share the (genus, species) — refuse to
        # bind unless an explicit preferred-alias row uniquely identifies
        # one of them.
        scientific_name = f"{genus} {species}"
        with self._connect() as conn:
            preferred = list(conn.execute(
                """
                SELECT DISTINCT s.taxon_id
                FROM scientific_name_min s
                WHERE s.scientific_name = ? COLLATE NOCASE
                  AND s.is_preferred_name = 1
                  AND s.taxon_id IN (%s)
                """ % ",".join("?" for _ in candidates),
                (scientific_name, *candidates),
            ))
        if len(preferred) == 1:
            return int(preferred[0][0])
        return None

    def taxon_id_from_vernacular(self, name: str, language_code: str | None = None) -> int | None:
        """Resolve a vernacular query to a single taxon_id. Uses the
        language fan-out ``no → (no, nb, nn)`` when applicable."""
        name = (name or "").strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(language_code)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT v.taxon_id "
                "FROM vernacular_min v "
                "WHERE v.vernacular_name = ? COLLATE NOCASE "
                + lang_clause +
                " ORDER BY v.is_preferred_name DESC, v.language_code, v.vernacular_id "
                "LIMIT 1",
                (name, *lang_params),
            ).fetchone()
        return int(row[0]) if row else None

    def list_vernacular_alternatives(
        self, taxon_id: int, languages: tuple[str, ...] | None = None,
    ) -> list[dict]:
        """Return every vernacular row for ``taxon_id``, ordered so that the
        preferred user-facing language block comes first and each language
        keeps its own rows distinct.

        Ordering is deterministic: (language-priority, is_preferred DESC,
        vernacular_id). ``languages`` is a tuple of codes that should be
        promoted to the top; every remaining language follows in
        alphabetical order.
        """
        if not self._has_language():
            return []
        # `source` is a Stage-3B.1 addition; older schemas may not have it.
        with self._connect() as conn:
            cols = {row[1] for row in conn.execute(
                "PRAGMA table_info(vernacular_min)")}
            has_source = "source" in cols
            select_source = "source" if has_source else "NULL AS source"
            rows = list(conn.execute(
                f"SELECT vernacular_id, language_code, vernacular_name, "
                f"       is_preferred_name, {select_source} "
                f"FROM vernacular_min WHERE taxon_id = ? ORDER BY "
                f"vernacular_id",
                (int(taxon_id),),
            ))
        result = [
            {"language_code": str(row[1]), "vernacular_name": str(row[2]),
             "is_preferred": bool(row[3]), "source": row[4],
             "vernacular_id": int(row[0])}
            for row in rows
        ]
        promote = tuple(languages) if languages else ()

        def sort_key(item):
            lang = item["language_code"]
            if lang in promote:
                priority = promote.index(lang)
            else:
                priority = len(promote) + 1
            return (priority, 0 if item["is_preferred"] else 1,
                    item["language_code"], item["vernacular_id"])
        result.sort(key=sort_key)
        return result

    def vernacular_from_taxon(self, genus: str, species: str) -> str | None:
        """Return one preferred vernacular for the given scientific taxon.

        Two-step query: resolve ``taxon_id`` via ``idx_taxon_genus_species``
        first, then look up vernaculars via ``idx_vern_taxon_lang``. Both
        indexes are exact-match — placing ``COLLATE NOCASE`` in the WHERE
        would defeat the covering index and force a full-table scan of
        ``vernacular_min`` (~7 ms → ~15 µs per call on real data).

        Skips the historical ``taxon_from_scientific`` normalization pass:
        that helper itself runs a ``COLLATE NOCASE`` scan (~130 ms per call
        on the real v2 database) which the hot per-observation-row path
        cannot afford. Case robustness is handled inline: try plain
        equality first, then a bounded ``COLLATE NOCASE`` fallback only
        when the fast lookup misses.
        """
        if not genus or not species:
            return None
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT taxon_id FROM taxon_min "
                "WHERE genus = ? AND specific_epithet = ? LIMIT 1",
                (genus, species),
            ).fetchone()
            if row is None:
                # Safety net for legacy rows with mixed case.
                row = conn.execute(
                    "SELECT taxon_id FROM taxon_min "
                    "WHERE genus = ? COLLATE NOCASE "
                    "  AND specific_epithet = ? COLLATE NOCASE LIMIT 1",
                    (genus, species),
                ).fetchone()
            if row is None:
                return None
            taxon_id = row[0]
            vern = conn.execute(
                "SELECT v.vernacular_name FROM vernacular_min v "
                "WHERE v.taxon_id = ? "
                + lang_clause +
                " ORDER BY v.is_preferred_name DESC, v.vernacular_name LIMIT 1",
                (int(taxon_id), *lang_params),
            ).fetchone()
        return vern[0] if vern else None


import re

# --------------------------------------------------------------------------
# Stage 3B.3: scientific-name suggestion source for the observation editor
# --------------------------------------------------------------------------

# Ranks the observation editor accepts. Same whitelist enforced on write in
# `models.py`. `aggregate` corresponds to `... coll.` / `... agg.` strings.
_SCIENTIFIC_PICKER_RANKS = frozenset(
    {"species", "subspecies", "variety", "form"}
)
# Aliases that appear in `scientific_name_min` but never make it into
# `taxon_min.taxon_rank` — we parse them out of the name string.
_ALIAS_MARKERS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"^([A-Z][a-z]+)$"),                                                 "genus"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+)$"),                                        "species"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) subsp\. ([a-z]+)$"),                       "subspecies"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) ssp\. ([a-z]+)$"),                         "subspecies"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) var\. ([a-z]+)$"),                         "variety"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) f\. ([a-z]+)$"),                           "form"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) coll\.$"),                                 "aggregate"),
    (re.compile(r"^([A-Z][a-z]+) ([a-z]+) agg\.$"),                                  "aggregate"),
)

# Names that are placeholder / curatorial and MUST NOT appear in the picker.
_PICKER_EXCLUDED_NAMES = frozenset({"Incertae sedis"})

# Rank tokens on the taxon_min row itself that we DO expose (variety /
# form / subspecies canonical rows plus species-rank rows that may carry
# `coll.`). Everything else is dropped from the picker.
_PICKER_TAXON_RANKS = frozenset({"species", "subspecies", "variety", "form"})


def parse_scientific_name_snapshot(name: str) -> tuple[str, str | None, str] | None:
    """Return ``(genus, species_or_None, rank_snapshot)`` for the exact,
    bounded set of scientific-name shapes the observation editor supports.

    Rejects (returns ``None``) any authorship suffix, non-Latin punctuation,
    multiple rank markers, or unrecognized formatting. Structured
    ``taxon_min`` rows go through their own ``taxon_rank`` field — this
    parser is used only on ``scientific_name_min`` alias strings and on the
    ``taxon_min.canonical_scientific_name`` for the aggregate-marker
    detection.
    """
    if not name:
        return None
    text = " ".join(str(name).strip().split())
    for pattern, rank in _ALIAS_MARKERS:
        m = pattern.match(text)
        if m:
            groups = m.groups()
            genus = groups[0]
            species = groups[1] if len(groups) >= 2 else None
            return (genus, species, rank)
    return None


def _prefix_upper_bound(prefix: str) -> str:
    """Half-open range upper bound for a canonical prefix scan. SQLite
    treats ``>=`` and ``<`` on strings as byte-wise; appending U+FFFF picks
    up every string with the requested prefix."""
    return prefix + "￿"


VernacularDB._SCIENTIFIC_PICKER_RANKS = _SCIENTIFIC_PICKER_RANKS  # type: ignore[attr-defined]


def _suggest_scientific_names(
    self: "VernacularDB",
    prefix: str,
    *,
    limit: int = 40,
) -> list[dict]:
    """Return scientific-name completion candidates for the observation
    editor. Each row includes enough disambiguation fields for the caller
    to build a distinct label.

    * Uses **range scans** (`col >= ? AND col < ?`) so completion hits an
      indexed path instead of a full-table scan (LIKE prefix on
      case-sensitive strings can degrade to a scan when the planner is
      uncertain — measured 19-52 ms → 11-35 μs after switch).
    * Filters out ``Incertae sedis`` placeholders and any alias whose
      parsed rank is not in the observation whitelist.
    * Returns a stable, deterministic order (canonical first, then
      alphabetical).
    """
    prefix = (prefix or "").strip()
    if not prefix:
        return []
    lo = prefix
    hi = _prefix_upper_bound(prefix)

    with self._connect() as conn:
        # taxon_min canonical rows — structured rank from the DB.
        canonical_rows = list(conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank, "
            "       taxonomic_status, family, canonical_source_system "
            "FROM taxon_min "
            "WHERE canonical_scientific_name >= ? AND canonical_scientific_name < ? "
            "ORDER BY canonical_scientific_name LIMIT ?",
            (lo, hi, int(limit) * 2),
        ))
        # scientific_name_min alias rows — rank parsed from the string.
        alias_rows = list(conn.execute(
            "SELECT s.taxon_id, s.scientific_name, s.is_preferred_name, "
            "       t.canonical_scientific_name, t.taxon_rank, t.taxonomic_status, "
            "       t.family, t.canonical_source_system "
            "FROM scientific_name_min s "
            "JOIN taxon_min t ON t.taxon_id = s.taxon_id "
            "WHERE s.language_code = 'sci' "
            "  AND s.scientific_name >= ? AND s.scientific_name < ? "
            "  AND s.is_preferred_name = 0 "
            "ORDER BY s.scientific_name LIMIT ?",
            (lo, hi, int(limit) * 2),
        ))

    seen: set[tuple[int, str]] = set()
    out: list[dict] = []

    def parsed_rank_or(structured_rank: str | None, name: str) -> str | None:
        parsed = parse_scientific_name_snapshot(name)
        if parsed:
            return parsed[2]
        if structured_rank in _PICKER_TAXON_RANKS:
            return structured_rank
        return None

    for taxon_id, name, rank, status, family, source in canonical_rows:
        if not name or "(" in name:
            continue
        if name in _PICKER_EXCLUDED_NAMES:
            continue
        rank_snapshot = parsed_rank_or(rank, name)
        if rank_snapshot not in _SCIENTIFIC_PICKER_RANKS and rank_snapshot != "aggregate":
            continue
        key = (int(taxon_id), name)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "sporely_taxon_id": int(taxon_id),
            "scientific_name": str(name),
            "taxon_rank_snapshot": rank_snapshot,
            "is_canonical": True,
            "canonical_scientific_name": str(name),
            "canonical_rank": str(rank or ""),
            "canonical_taxonomic_status": str(status or ""),
            "family": str(family or "") or None,
            "canonical_source_system": str(source or "") or None,
            "authorship": None,
            # For canonical rows the link kind is "canonical" — the
            # observer picks the accepted concept directly.
            "link_kind": "canonical",
        })

    for (taxon_id, name, is_pref, canonical_name, rank,
         status, family, source) in alias_rows:
        if not name or "(" in name:
            continue
        if name in _PICKER_EXCLUDED_NAMES:
            continue
        parsed = parse_scientific_name_snapshot(name)
        if parsed is None:
            continue
        rank_snapshot = parsed[2]
        if rank_snapshot not in _SCIENTIFIC_PICKER_RANKS and rank_snapshot != "aggregate":
            continue
        key = (int(taxon_id), name)
        if key in seen:
            continue
        seen.add(key)
        # Determine link_kind explicitly. Prior to Stage 3B.3 the compiler
        # marked synonym aliases with `is_preferred_name = 0` in
        # `scientific_name_min`; the accepted concept sits on the same row's
        # `taxon_id`. When the alias's parsed rank matches the canonical
        # concept's rank we mark it "synonym_of_accepted" (Accepted
        # concept: <canonical>). When the ranks differ we mark it "linked"
        # (Linked concept: <canonical>). This encoding comes from the
        # explicit relation, not from any rank-based heuristic.
        if str(rank or "") in _PICKER_TAXON_RANKS and \
                str(rank).lower() == rank_snapshot:
            link_kind = "synonym_of_accepted"
        else:
            link_kind = "linked"
        out.append({
            "sporely_taxon_id": int(taxon_id),
            "scientific_name": str(name),
            "taxon_rank_snapshot": rank_snapshot,
            "is_canonical": False,
            "canonical_scientific_name": str(canonical_name or ""),
            "canonical_rank": str(rank or ""),
            "canonical_taxonomic_status": str(status or ""),
            "family": str(family or "") or None,
            "canonical_source_system": str(source or "") or None,
            "authorship": None,
            "link_kind": link_kind,
        })

    out.sort(key=lambda r: (
        r["scientific_name"].casefold(),
        # Prefer canonical over alias at equal names.
        0 if r["is_canonical"] else 1,
        r["sporely_taxon_id"],
    ))
    if len(out) > limit:
        out = out[:limit]
    return out


VernacularDB.suggest_scientific_names = _suggest_scientific_names  # type: ignore[attr-defined]


__all__ = ["VernacularDB", "parse_scientific_name_snapshot"]
