"""Shared non-UI taxonomy lookup helpers.

This module centralizes the current local taxonomy/common-name behavior while
also merging in reference-data genus/species suggestions. It deliberately stays
UI-free so a future autocomplete controller can reuse it without pulling in
Qt/PySide6.

Identity resolution vs national overlay
---------------------------------------
Taxonomic identity is bound from source-system canonical data
(``taxon_min.canonical_source_system``). When a user manually enters an
unambiguous ``(genus, species)`` pair, the resolver prefers the COL
(``col_xr``) canonical concept — COL is the source-system authority for
species concepts in this DB. NorTaxa (``nortaxa``) rows are bound only
when NO COL canonical exists for the exact name.

The Norwegian Red List is a **national overlay** on top of taxonomic
identity. When the runtime looks up an assessment for a bound
``sporely_taxon_id`` and finds none, it MAY (via
``get_redlist_lookup_with_overlay``) fall back to the assessment on a
unique NorTaxa row that shares the same canonical scientific name. That
fallback never changes the observation's bound identity; it only
surfaces the assessed category, tagged with ``overlay_source`` so the
caller can debug where it came from.

Proper COL↔NorTaxa concept unification (a single Sporely id per
concept) is a **compile-pipeline** concern (the taxonomy compiler that
produces ``taxon_min`` is where source-system rows should be de-duped
into a single concept). This module keeps runtime conservative and
observable while that compile-time work happens elsewhere.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace as _dc_replace
import sqlite3
from typing import Any

from database.vernacular_db import VernacularDB
from utils.vernacular_utils import normalize_vernacular_language


@dataclass(frozen=True)
class RedlistLookupResult:
    """Explicit outcome of a red-list lookup for
    ``(sporely_taxon_id, area, source_release)``.

    Exactly one of four statuses:
      - ``"none"``: no assessment row (or the ``taxon_redlist_min`` table is
        absent, e.g. legacy DB). ``assessment`` and ``conflicting_assessments``
        are both empty.
      - ``"unique"``: exactly one assessment row. ``assessment`` is set.
      - ``"multiple_same_category"``: several rows for the same Sporely id
        via distinct Artsnavnebase name-ids that all agree on the category
        code. ``assessment`` is the deterministic representative (smallest
        numeric ``assessment_id``). Callers that want the full set can read
        ``conflicting_assessments``.
      - ``"conflict"``: several rows disagree on category or rank. No
        representative is chosen. ``conflicting_assessments`` lists them in
        deterministic order (smallest numeric ``assessment_id`` first).

    The result never auto-picks a category for conflict groups. That is a
    curation decision, not a runtime one.

    ``overlay_source`` / ``overlay_taxon_id`` are populated only by
    :meth:`TaxonLookupService.get_redlist_lookup_with_overlay` when the
    primary ``sporely_taxon_id`` had no assessment and the runtime
    surfaced one via an exact-canonical-name NorTaxa counterpart. The
    default entrypoint ``get_redlist_lookup`` never sets these fields —
    callers pinning strict behaviour keep their existing semantics.
    """
    status: str
    assessment: "RedlistAssessment | None" = None
    conflicting_assessments: tuple["RedlistAssessment", ...] = ()
    overlay_source: str = ""
    overlay_taxon_id: int | None = None


@dataclass(frozen=True)
class RedlistAssessment:
    """A single Norwegian Red List assessment for a resolved Sporely taxon."""
    taxon_id: int
    source_system: str
    source_release: str
    assessment_area: str
    assessment_id: str
    category_raw: str
    category_code: str
    category_is_downgraded: bool
    criteria: str | None
    expert_group: str | None
    assessment_url: str | None
    scientific_name_snapshot: str
    authorship_snapshot: str | None
    taxon_rank_snapshot: str | None
    assessed_name_source: str
    assessed_name_namespace: str
    assessed_name_id: str


@dataclass(frozen=True)
class TaxonChoice:
    genus: str
    species: str | None = None
    common_name: str | None = None
    family: str | None = None
    source: str = "taxonomy"
    taxon_id: int | None = None
    language_code: str | None = None
    red_list_category: str | None = None
    red_list_source: str | None = None


@dataclass(frozen=True)
class ManualScientificResolution:
    """Result of an unambiguous ``(genus, species)`` -> ``sporely_taxon_id``
    resolution done outside the completer picker.

    Populated only when exactly one canonical concept matches the pair
    (see :meth:`TaxonLookupService.resolve_manual_scientific`). Consumers
    treat this as the same class of identity a picker selection would
    produce: it carries enough state to fill a
    :class:`ui.taxon_input_controller.TaxonInputController` committed
    snapshot without additional queries.
    """
    sporely_taxon_id: int
    genus: str
    species: str
    scientific_name: str
    taxon_rank_snapshot: str
    canonical_scientific_name: str | None
    canonical_rank: str | None
    link_kind: str = "canonical"


TAXON_COMPLETER_LIMIT = 200


def _normalize_genus_display(genus: str | None) -> str:
    text = str(genus or "").strip()
    if not text:
        return ""
    if len(text) == 1:
        return text.upper()
    return text[0].upper() + text[1:].lower()


def _normalize_species_display(species: str | None) -> str:
    return str(species or "").strip().lower()


def _normalize_text(value: str | None) -> str:
    return str(value or "").strip()


def _casefold_key(value: str | None) -> str:
    return _normalize_text(value).casefold()


def _coerce_reference_provider(reference_db_factory: Any | None) -> Any | None:
    if reference_db_factory is None:
        from database.models import ReferenceDB

        return ReferenceDB
    if hasattr(reference_db_factory, "list_genera") and hasattr(reference_db_factory, "list_species"):
        return reference_db_factory
    if callable(reference_db_factory):
        try:
            candidate = reference_db_factory()
        except TypeError:
            return reference_db_factory
        if candidate is not None:
            return candidate
    return reference_db_factory


class TaxonLookupService:
    def __init__(
        self,
        vernacular_db: VernacularDB | None = None,
        reference_db_factory=None,
        language_code: str | None = None,
        include_reference_data: bool = True,
    ):
        self.vernacular_db = vernacular_db
        self.include_reference_data = bool(include_reference_data)
        self._language_code: str | None = None
        self._reference_provider = _coerce_reference_provider(reference_db_factory)
        self._local_table_names_cache: set[str] | None = None
        self._local_column_cache: dict[str, set[str]] = {}
        self._suggest_genera_cache: dict[tuple[str, int], tuple[str, ...]] = {}
        self._suggest_species_cache: dict[tuple[str, str, int], tuple[TaxonChoice, ...]] = {}
        self._suggest_common_names_cache: dict[tuple[str, str | None, str | None, int], tuple[TaxonChoice, ...]] = {}
        self._resolve_common_name_cache: dict[tuple[str, str | None, str | None], tuple[TaxonChoice, ...]] = {}
        self._resolve_scientific_cache: dict[tuple[str, str], TaxonChoice | None] = {}
        self._best_common_name_cache: dict[tuple[str, str], TaxonChoice | None] = {}
        self._common_names_for_taxon_cache: dict[tuple[str, str, int], tuple[TaxonChoice, ...]] = {}

        initial_language = normalize_vernacular_language(language_code) if language_code else None
        if self.vernacular_db is not None:
            existing_language = normalize_vernacular_language(getattr(self.vernacular_db, "language_code", None))
            if initial_language:
                pass
            elif existing_language:
                initial_language = existing_language
        self.language_code = initial_language

    @property
    def language_code(self) -> str | None:
        return self._language_code

    @language_code.setter
    def language_code(self, value: str | None) -> None:
        new_language = normalize_vernacular_language(value) if value else None
        if new_language == self._language_code:
            return
        self._language_code = new_language
        if self.vernacular_db is not None:
            try:
                self.vernacular_db.language_code = self._language_code
            except Exception:
                pass
        self.clear_cache()

    def clear_cache(self) -> None:
        self._suggest_genera_cache.clear()
        self._suggest_species_cache.clear()
        self._suggest_common_names_cache.clear()
        self._resolve_common_name_cache.clear()
        self._resolve_scientific_cache.clear()
        self._best_common_name_cache.clear()
        self._common_names_for_taxon_cache.clear()

    @contextmanager
    def _local_connection(self):
        if not self.vernacular_db:
            yield None
            return
        try:
            conn = sqlite3.connect(self.vernacular_db.db_path)
        except Exception:
            yield None
            return
        try:
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            conn.close()

    def _fetch_local_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        if not self.vernacular_db:
            return []
        with self._local_connection() as conn:
            if conn is None:
                return []
            try:
                cursor = conn.execute(query, params)
                return cursor.fetchall()
            except sqlite3.Error:
                return []

    def _local_table_names(self) -> set[str]:
        if self._local_table_names_cache is not None:
            return self._local_table_names_cache
        tables: set[str] = set()
        if not self.vernacular_db:
            self._local_table_names_cache = tables
            return tables
        with self._local_connection() as conn:
            if conn is None:
                self._local_table_names_cache = tables
                return tables
            try:
                rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            except sqlite3.Error:
                rows = []
            tables = {str(row[0] or "") for row in rows if row and row[0]}
        self._local_table_names_cache = tables
        return tables

    def _local_columns(self, table: str) -> set[str]:
        if table in self._local_column_cache:
            return self._local_column_cache[table]
        columns: set[str] = set()
        if not self.vernacular_db:
            self._local_column_cache[table] = columns
            return columns
        with self._local_connection() as conn:
            if conn is None:
                self._local_column_cache[table] = columns
                return columns
            try:
                rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            except sqlite3.Error:
                rows = []
            columns = {str(row[1] or "") for row in rows if row and row[1]}
        self._local_column_cache[table] = columns
        return columns

    def _has_local_table(self, table: str) -> bool:
        return table in self._local_table_names()

    def _has_local_column(self, table: str, column: str) -> bool:
        return column in self._local_columns(table)

    def _reference_values(self, method_name: str, *args) -> list[Any]:
        if not self.include_reference_data or self._reference_provider is None:
            return []
        method = getattr(self._reference_provider, method_name, None)
        if not callable(method):
            return []
        try:
            values = method(*args)
        except Exception:
            return []
        return list(values or [])

    def _local_suggest_genera(self, prefix: str, limit: int) -> list[str]:
        if not self.vernacular_db:
            return []
        prefix = _normalize_text(prefix)
        limit_value = max(0, int(limit))
        if not prefix:
            seen: dict[str, str] = {}
            if self._has_local_table("taxon_min") and self._has_local_column("taxon_min", "genus"):
                rows = self._fetch_local_rows(
                    """
                    SELECT DISTINCT genus
                    FROM taxon_min
                    WHERE genus IS NOT NULL AND genus != ''
                    ORDER BY genus
                    LIMIT ?
                    """
                    ,
                    (limit_value,)
                )
                for row in rows:
                    genus = _normalize_genus_display(row[0])
                    if genus:
                        seen.setdefault(genus.casefold(), genus)
            if self._has_local_table("scientific_name_min") and self._has_local_column("scientific_name_min", "scientific_name"):
                rows = self._fetch_local_rows(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name IS NOT NULL AND scientific_name != ''
                    ORDER BY scientific_name
                    LIMIT ?
                    """
                    ,
                    (limit_value,)
                )
                for row in rows:
                    scientific_name = str(row[0] or "").strip()
                    genus = _normalize_genus_display(scientific_name.split(" ", 1)[0] if scientific_name else "")
                    if genus:
                        seen.setdefault(genus.casefold(), genus)
            return sorted(seen.values(), key=str.casefold)
        try:
            return list(self.vernacular_db.suggest_genus(prefix) or [])
        except Exception:
            return []

    def _local_suggest_species(self, genus: str, prefix: str, limit: int) -> list[str]:
        if not self.vernacular_db:
            return []
        genus = _normalize_genus_display(genus)
        prefix = _normalize_species_display(prefix)
        limit_value = max(0, int(limit))
        if not genus:
            return []
        if prefix:
            try:
                return list(self.vernacular_db.suggest_species(genus, prefix) or [])
            except Exception:
                return []
        seen: dict[str, str] = {}
        if self._has_local_table("taxon_min") and self._has_local_column("taxon_min", "specific_epithet"):
            rows = self._fetch_local_rows(
                """
                SELECT DISTINCT specific_epithet
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet IS NOT NULL AND specific_epithet != ''
                ORDER BY specific_epithet
                LIMIT ?
                """,
                (genus, limit_value),
            )
            for row in rows:
                species = _normalize_species_display(row[0])
                if species:
                    seen.setdefault(species.casefold(), species)
        if self._has_local_table("scientific_name_min") and self._has_local_column("scientific_name_min", "scientific_name"):
            rows = self._fetch_local_rows(
                """
                SELECT DISTINCT scientific_name
                FROM scientific_name_min
                WHERE scientific_name LIKE ? || ' %'
                ORDER BY scientific_name
                LIMIT ?
                """,
                (genus, limit_value),
            )
            for row in rows:
                scientific_name = str(row[0] or "").strip()
                parts = scientific_name.split()
                if len(parts) < 2 or parts[0].casefold() != genus.casefold():
                    continue
                species = _normalize_species_display(parts[1])
                if species:
                    seen.setdefault(species.casefold(), species)
        return sorted(seen.values(), key=str.casefold)

    def _local_taxon_record(self, genus: str, species: str) -> dict[str, Any] | None:
        genus = _normalize_genus_display(genus)
        species = _normalize_species_display(species)
        if not self.vernacular_db or not genus or not species:
            return None

        taxon_columns = self._local_columns("taxon_min")
        if not taxon_columns or "genus" not in taxon_columns or "specific_epithet" not in taxon_columns:
            return None
        if "taxon_id" in taxon_columns:
            taxon_id_expr = "t.taxon_id AS taxon_id"
        else:
            taxon_id_expr = "NULL AS taxon_id"
        if "family" in taxon_columns:
            family_expr = "t.family AS family"
        else:
            family_expr = "NULL AS family"

        has_scientific_table = self._has_local_table("scientific_name_min")
        has_scientific_name_column = self._has_local_column("scientific_name_min", "scientific_name")
        has_canonical_name = "canonical_scientific_name" in taxon_columns

        select_columns = [
            taxon_id_expr,
            "t.genus AS genus",
            "t.specific_epithet AS species",
            family_expr,
        ]
        where_parts = ["(t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE)"]
        params: list[Any] = [genus, species]

        if has_canonical_name:
            where_parts.append("t.canonical_scientific_name = ? COLLATE NOCASE")
            params.append(f"{genus} {species}")

        join_clause = ""
        if has_scientific_table and has_scientific_name_column:
            join_clause = " LEFT JOIN scientific_name_min s ON s.taxon_id = t.taxon_id"
            where_parts.append("s.scientific_name = ? COLLATE NOCASE")
            params.append(f"{genus} {species}")

        query = f"""
            SELECT DISTINCT {", ".join(select_columns)}
            FROM taxon_min t
            {join_clause}
            WHERE {" OR ".join(where_parts)}
            ORDER BY
                CASE
                    WHEN t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE THEN 0
                    ELSE 1
                END,
                t.genus,
                t.specific_epithet
            LIMIT 1
        """
        params.extend([genus, species])
        rows = self._fetch_local_rows(query, tuple(params))
        if not rows:
            return None
        row = rows[0]
        return {
            "taxon_id": row["taxon_id"],
            "genus": _normalize_genus_display(row["genus"]),
            "species": _normalize_species_display(row["species"]),
            "family": _normalize_text(row["family"]) or None,
        }

    def _local_common_name_rows(
        self,
        *,
        prefix: str | None = None,
        name: str | None = None,
        genus: str | None = None,
        species: str | None = None,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        if not self.vernacular_db:
            return []
        vernacular_columns = self._local_columns("vernacular_min")
        taxon_columns = self._local_columns("taxon_min")
        if not vernacular_columns or not taxon_columns:
            return []
        if "taxon_id" not in vernacular_columns or "vernacular_name" not in vernacular_columns:
            return []
        if "taxon_id" not in taxon_columns or "genus" not in taxon_columns or "specific_epithet" not in taxon_columns:
            return []

        has_language = "language_code" in vernacular_columns
        has_preferred = "is_preferred_name" in vernacular_columns
        has_family = "family" in taxon_columns

        select_columns = [
            "v.vernacular_name AS common_name",
            "t.taxon_id AS taxon_id",
            "t.genus AS genus",
            "t.specific_epithet AS species",
            "t.family AS family" if has_family else "NULL AS family",
            "v.language_code AS language_code" if has_language else "NULL AS language_code",
            "v.is_preferred_name AS is_preferred_name" if has_preferred else "0 AS is_preferred_name",
        ]

        filters: list[str] = []
        params: list[Any] = []

        if name is not None:
            filters.append("v.vernacular_name = ?")
            params.append(name)
        elif prefix is not None:
            prefix = prefix.strip()
            if not prefix:
                return []
            filters.append("v.vernacular_name LIKE ? || '%'")
            params.append(prefix)

        if genus is not None or species is not None:
            resolved = self._local_taxon_record(genus or "", species or "") if genus and species else None
            target_genus = _normalize_genus_display((resolved or {}).get("genus") or genus)
            target_species = _normalize_species_display((resolved or {}).get("species") or species)
            if genus is not None:
                filters.append("t.genus = ? COLLATE NOCASE")
                params.append(target_genus)
            if species is not None:
                filters.append("t.specific_epithet = ? COLLATE NOCASE")
                params.append(target_species)

        language_code = self.language_code
        if has_language and language_code:
            # Fan out the umbrella `no` code to (`no`, `nb`, `nn`) so v2
            # candidates (which store `nb` / `nn` distinct) still match
            # Norwegian queries. Legacy DBs continue to work because they
            # only carry the umbrella `no` code and IN (?, ?, ?) still hits
            # it. `nb`, `nn` and Sámi codes remain distinct when named.
            try:
                from utils.vernacular_utils import resolve_query_language_codes
                codes = resolve_query_language_codes(language_code)
            except Exception:
                codes = (language_code,)
            if codes:
                placeholders = ",".join("?" for _ in codes)
                filters.append(f"v.language_code IN ({placeholders})")
                params.extend(codes)

        query = f"""
            SELECT DISTINCT {", ".join(select_columns)}
            FROM vernacular_min v
            JOIN taxon_min t ON t.taxon_id = v.taxon_id
            {'WHERE ' + ' AND '.join(filters) if filters else ''}
            ORDER BY
                {("COALESCE(v.is_preferred_name, 0) DESC, " if has_preferred else "")}
                v.vernacular_name,
                t.genus,
                t.specific_epithet
        """
        if limit is not None:
            query += "\n            LIMIT ?"
            params.append(int(limit))
        return self._fetch_local_rows(query, tuple(params))

    def _row_to_choice(self, row: sqlite3.Row, source: str) -> TaxonChoice:
        return TaxonChoice(
            genus=_normalize_genus_display(row["genus"]),
            species=_normalize_species_display(row["species"]),
            common_name=_normalize_text(row["common_name"]) or None,
            family=_normalize_text(row["family"]) or None,
            source=source,
            taxon_id=row["taxon_id"],
            language_code=_normalize_text(row["language_code"]) or None,
            red_list_category=None,
            red_list_source=None,
        )

    def _local_common_names_for_taxon(self, genus: str, species: str, limit: int | None = None) -> list[TaxonChoice]:
        rows = self._local_common_name_rows(genus=genus, species=species, limit=limit)
        return [self._row_to_choice(row, "taxonomy") for row in rows]

    def suggest_genera(self, prefix: str = "", limit: int = TAXON_COMPLETER_LIMIT) -> list[str]:
        prefix = _normalize_text(prefix)
        limit_value = max(0, int(limit))
        cache_key = (prefix.casefold(), limit_value)
        cached = self._suggest_genera_cache.get(cache_key)
        if cached is not None:
            return list(cached[:limit_value])
        seen: dict[str, str] = {}

        for value in self._local_suggest_genera(prefix, limit_value):
            genus = _normalize_genus_display(value)
            if genus:
                seen.setdefault(genus.casefold(), genus)

        for value in self._reference_values("list_genera", prefix):
            genus = _normalize_genus_display(value)
            if genus:
                seen.setdefault(genus.casefold(), genus)

        values = tuple(sorted(seen.values(), key=str.casefold)[:limit_value])
        self._suggest_genera_cache[cache_key] = values
        return list(values)

    def suggest_species(self, genus: str, prefix: str = "", limit: int = TAXON_COMPLETER_LIMIT) -> list[TaxonChoice]:
        genus = _normalize_genus_display(genus)
        prefix = _normalize_species_display(prefix)
        if not genus:
            return []
        limit_value = max(0, int(limit))
        cache_key = (genus.casefold(), prefix.casefold(), limit_value)
        cached = self._suggest_species_cache.get(cache_key)
        if cached is not None:
            return list(cached[:limit_value])

        local_species = {
            _normalize_species_display(value)
            for value in self._local_suggest_species(genus, prefix, limit_value)
            if _normalize_species_display(value)
        }
        reference_species = {
            _normalize_species_display(value)
            for value in self._reference_values("list_species", genus, prefix)
            if _normalize_species_display(value)
        }

        ordered_species = sorted(local_species | reference_species, key=str.casefold)
        choices: list[TaxonChoice] = []
        for species in ordered_species[:limit_value]:
            source = "taxonomy" if species in local_species else "reference"
            if species in local_species and species in reference_species:
                source = "both"
            choices.append(TaxonChoice(genus=genus, species=species, source=source))
        self._suggest_species_cache[cache_key] = tuple(choices)
        return choices

    def suggest_scientific_names(
        self, prefix: str = "", limit: int = TAXON_COMPLETER_LIMIT,
    ) -> list[dict]:
        """Stage 3B.3 scientific-name completer source. Delegates to
        :meth:`VernacularDB.suggest_scientific_names`. Returns rows with
        ``sporely_taxon_id``, ``scientific_name``, ``taxon_rank_snapshot``,
        ``link_kind`` and disambiguation fields (`family`, `authorship`,
        `canonical_source_system`). Empty list when no v2 DB is available.
        """
        prefix = (prefix or "").strip()
        if not prefix or self.vernacular_db is None:
            return []
        try:
            return self.vernacular_db.suggest_scientific_names(prefix, limit=limit)
        except Exception:
            return []

    def suggest_common_names(
        self,
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = TAXON_COMPLETER_LIMIT,
    ) -> list[TaxonChoice]:
        prefix = _normalize_text(prefix)
        if not prefix and genus is None and species is None:
            return []
        limit_value = max(0, int(limit))
        cache_key = (
            prefix.casefold(),
            _normalize_genus_display(genus) if genus is not None else None,
            _normalize_species_display(species) if species is not None else None,
            limit_value,
        )
        cached = self._suggest_common_names_cache.get(cache_key)
        if cached is not None:
            return list(cached[:limit_value])
        rows = self._local_common_name_rows(prefix=prefix or None, genus=genus, species=species, limit=limit_value)
        values = tuple(self._row_to_choice(row, "taxonomy") for row in rows[:limit_value])
        self._suggest_common_names_cache[cache_key] = values
        return list(values)

    def resolve_scientific(self, genus: str, species: str) -> TaxonChoice | None:
        key = (_normalize_genus_display(genus).casefold(), _normalize_species_display(species).casefold())
        if key in self._resolve_scientific_cache:
            return self._resolve_scientific_cache[key]
        record = self._local_taxon_record(genus, species)
        if not record:
            self._resolve_scientific_cache[key] = None
            return None
        best_common_name = self.best_common_name_for_taxon(record["genus"], record["species"])
        choice = TaxonChoice(
            genus=record["genus"],
            species=record["species"],
            common_name=best_common_name.common_name if best_common_name else None,
            family=record.get("family"),
            source="taxonomy",
            taxon_id=record.get("taxon_id"),
            language_code=best_common_name.language_code if best_common_name else None,
            red_list_category=None,
            red_list_source=None,
        )
        self._resolve_scientific_cache[key] = choice
        return choice

    def resolve_manual_scientific(
        self, genus: str, species: str,
    ) -> ManualScientificResolution | None:
        """Resolve a manually-typed ``(genus, species)`` pair to a single
        canonical ``sporely_taxon_id``, or ``None`` when the pair is empty,
        unknown, or ambiguous.

        Used by the observation editor's genus/species ``editingFinished``
        path so a manual identity edit refreshes the Red List badge
        without requiring the user to open the scientific-name completer.
        Fires primarily when the taxonomy DB (``VernacularDB``)
        unambiguously pins the pair via
        :meth:`~database.vernacular_db.VernacularDB.taxon_id_from_scientific`;
        that helper already returns ``None`` for zero-hit or multi-hit
        pairs and breaks preferred-alias ties conservatively.

        Source-system preference fallback: when the strict resolver
        returns ``None`` because multiple canonical rows share the pair
        (e.g. a ``col_xr`` canonical alongside a ``nortaxa`` canonical
        that carry the same exact scientific name), fall back to
        :meth:`_resolve_manual_via_source_system_preference`. That
        fallback prefers the COL row because COL is the source-system
        authority for species concepts in the compiled DB. It never
        binds identity based on Red List presence — the national Red
        List is treated as a separate overlay by
        :meth:`get_redlist_lookup_with_overlay`, and identity is
        selected purely from source-system canonical evidence.
        """
        genus_display = _normalize_genus_display(genus)
        species_display = _normalize_species_display(species)
        if not genus_display or not species_display:
            return None
        vdb = self.vernacular_db
        if vdb is None:
            return None
        try:
            resolver = getattr(vdb, "taxon_id_from_scientific", None)
        except Exception:
            resolver = None
        if not callable(resolver):
            return None
        try:
            sporely_id = resolver(genus_display, species_display)
        except Exception:
            sporely_id = None
        if sporely_id is None:
            sporely_id = self._resolve_manual_via_source_system_preference(
                genus_display, species_display,
            )
        if sporely_id is None:
            return None
        try:
            sporely_id_int = int(sporely_id)
        except (TypeError, ValueError):
            return None
        # Pull the canonical fields off ``taxon_min`` for the snapshot.
        rows = self._fetch_local_rows(
            "SELECT genus, specific_epithet, canonical_scientific_name, "
            "taxon_rank FROM taxon_min WHERE taxon_id = ? LIMIT 1",
            (sporely_id_int,),
        )
        if not rows:
            return None
        row = rows[0]
        canonical_name = _normalize_text(row["canonical_scientific_name"]) or None
        canonical_rank = _normalize_text(row["taxon_rank"]) or None
        # Only species-level or lower ranks are valid picker-committable
        # identities; a manual genus/species entry must resolve to one of
        # those or we treat it as unresolved (matches the picker's own
        # whitelist in ``_suggest_scientific_names``).
        allowed_ranks = {"species", "subspecies", "variety", "form"}
        if canonical_rank and canonical_rank.lower() not in allowed_ranks:
            return None
        scientific_name = canonical_name or f"{genus_display} {species_display}"
        rank_snapshot = (canonical_rank or "species").lower()
        return ManualScientificResolution(
            sporely_taxon_id=sporely_id_int,
            genus=genus_display,
            species=species_display,
            scientific_name=scientific_name,
            taxon_rank_snapshot=rank_snapshot,
            canonical_scientific_name=canonical_name,
            canonical_rank=canonical_rank,
            link_kind="canonical",
        )

    def _resolve_manual_via_source_system_preference(
        self, genus_display: str, species_display: str,
    ) -> int | None:
        """Prefer the COL canonical concept when a ``(genus, species)``
        pair matches multiple canonical rows in ``taxon_min``.

        Called only when the strict
        :meth:`~database.vernacular_db.VernacularDB.taxon_id_from_scientific`
        refused to bind identity. Two filter steps run first:

        1. Consider only rows whose ``canonical_scientific_name`` matches
           the typed pair exactly (case-insensitive). Drops
           variety/subspecies/form rows that happen to share genus +
           specific_epithet with a base-rank canonical (e.g.
           ``Cantharellus cibarius var. monstrosus`` when the user typed
           ``Cantharellus cibarius``).
        2. Consider only rows on the picker rank whitelist
           (``species``, ``subspecies``, ``variety``, ``form``).

        Then apply the source-system preference:

        * If exactly one surviving candidate has
          ``canonical_source_system = 'col_xr'`` → bind that
          ``taxon_id`` (COL is the source-system authority for species
          concepts in this DB).
        * If zero COL candidates survive AND exactly one NorTaxa
          candidate (``canonical_source_system = 'nortaxa'``)
          survives → bind that ``taxon_id`` (no ambiguity to resolve).
        * Otherwise → ``None``. The observer must use the picker.

        The Norwegian Red List is deliberately NOT used to influence
        identity here — it is a national overlay handled separately by
        :meth:`get_redlist_lookup_with_overlay`. Using an overlay signal
        to select identity would silently bind observations to the
        NorTaxa concept whenever it carries the assessment (which is
        the common shape) even though the observer typed a name that
        COL owns as its own concept.
        """
        vdb = self.vernacular_db
        if vdb is None:
            return None
        candidates_resolver = getattr(vdb, "taxon_ids_from_scientific", None)
        if not callable(candidates_resolver):
            return None
        try:
            candidates = [
                int(c)
                for c in (candidates_resolver(genus_display, species_display) or [])
            ]
        except Exception:
            return None
        if not candidates:
            return None
        scientific_name = f"{genus_display} {species_display}"
        allowed_ranks = {"species", "subspecies", "variety", "form"}
        placeholders = ",".join("?" for _ in candidates)
        rows = self._fetch_local_rows(
            f"SELECT taxon_id, taxon_rank, canonical_scientific_name, "
            f"canonical_source_system "
            f"FROM taxon_min WHERE taxon_id IN ({placeholders}) "
            f"AND canonical_scientific_name = ? COLLATE NOCASE",
            (*candidates, scientific_name),
        )
        filtered = [
            (int(r["taxon_id"]),
             str(r["canonical_source_system"] or "").strip().lower())
            for r in rows
            if str(r["taxon_rank"] or "").strip().lower() in allowed_ranks
        ]
        if not filtered:
            return None
        col_ids = [tid for tid, source in filtered if source == "col_xr"]
        if len(col_ids) == 1:
            return col_ids[0]
        if not col_ids:
            nortaxa_ids = [tid for tid, source in filtered if source == "nortaxa"]
            if len(nortaxa_ids) == 1:
                return nortaxa_ids[0]
        return None

    def _fetch_redlist_rows(
        self,
        taxon_id: int,
        area: str,
        source_release: str,
    ) -> list[RedlistAssessment]:
        if not self._has_local_table("taxon_redlist_min"):
            return []
        rows = self._fetch_local_rows(
            "SELECT taxon_id, source_system, source_release, assessment_area, "
            "assessment_id, category_raw, category_code, category_is_downgraded, "
            "criteria, expert_group, assessment_url, scientific_name_snapshot, "
            "authorship_snapshot, taxon_rank_snapshot, assessed_name_source, "
            "assessed_name_namespace, assessed_name_id "
            "FROM taxon_redlist_min "
            "WHERE taxon_id = ? AND assessment_area = ? AND source_release = ?",
            (taxon_id, area, source_release),
        )
        assessments = [
            RedlistAssessment(
                taxon_id=int(r["taxon_id"]),
                source_system=str(r["source_system"]),
                source_release=str(r["source_release"]),
                assessment_area=str(r["assessment_area"]),
                assessment_id=str(r["assessment_id"]),
                category_raw=str(r["category_raw"]),
                category_code=str(r["category_code"]),
                category_is_downgraded=bool(r["category_is_downgraded"]),
                criteria=r["criteria"],
                expert_group=r["expert_group"],
                assessment_url=r["assessment_url"],
                scientific_name_snapshot=str(r["scientific_name_snapshot"]),
                authorship_snapshot=r["authorship_snapshot"],
                taxon_rank_snapshot=r["taxon_rank_snapshot"],
                assessed_name_source=str(r["assessed_name_source"]),
                assessed_name_namespace=str(r["assessed_name_namespace"]),
                assessed_name_id=str(r["assessed_name_id"]),
            )
            for r in rows
        ]

        # Deterministic ordering: numeric assessment_id ascending, then
        # lexicographic fallback so tie behavior is fully specified.
        def sort_key(a: RedlistAssessment):
            try:
                return (0, int(a.assessment_id), a.assessment_id)
            except ValueError:
                return (1, 0, a.assessment_id)
        assessments.sort(key=sort_key)
        return assessments

    def get_redlist_lookup(
        self,
        sporely_taxon_id: int,
        *,
        area: str = "Norge",
        source_release: str = "2021",
    ) -> RedlistLookupResult:
        """Return the explicit red-list lookup result for a Sporely taxon.

        Never merges Norway and Svalbard: pass ``area="Svalbard"`` explicitly.
        Never returns a category automatically for a conflict group — the
        result explicitly tags conflicts and lists all conflicting rows.

        Statuses (see :class:`RedlistLookupResult`): ``"none"``,
        ``"unique"``, ``"multiple_same_category"``, ``"conflict"``.

        Stage 3B.5: the same-category collapse keys on the pair
        ``(category_code, category_is_downgraded)``. Rows that share a
        base category but differ by the degree marker (e.g. ``VU`` vs
        ``VU°``) are treated as ``conflict``, not
        ``multiple_same_category``. Differences in rank, criteria,
        expert group, or assessed name do NOT turn agreement on the
        category into a conflict — they only mean the representative's
        metadata for those fields is not authoritative for the whole
        group (which the caller already knows for
        ``multiple_same_category`` and can inspect via
        ``conflicting_assessments``).
        """
        if sporely_taxon_id is None:
            return RedlistLookupResult(status="none")
        try:
            taxon_id_int = int(sporely_taxon_id)
        except (TypeError, ValueError):
            return RedlistLookupResult(status="none")
        assessments = self._fetch_redlist_rows(
            taxon_id_int, str(area), str(source_release)
        )
        if not assessments:
            return RedlistLookupResult(status="none")
        if len(assessments) == 1:
            return RedlistLookupResult(status="unique",
                                       assessment=assessments[0])
        distinct_keys = {
            (a.category_code, bool(a.category_is_downgraded))
            for a in assessments
        }
        if len(distinct_keys) == 1:
            return RedlistLookupResult(
                status="multiple_same_category",
                assessment=assessments[0],
                conflicting_assessments=tuple(assessments),
            )
        return RedlistLookupResult(
            status="conflict",
            assessment=None,
            conflicting_assessments=tuple(assessments),
        )

    def get_redlist_lookup_with_overlay(
        self,
        sporely_taxon_id: int,
        *,
        area: str = "Norge",
        source_release: str = "2021",
    ) -> RedlistLookupResult:
        """Return the red-list lookup for ``sporely_taxon_id`` and,
        when the primary lookup yields ``"none"``, transparently overlay
        an assessment from an exact-canonical-name NorTaxa counterpart.

        This is the entrypoint the observation editor uses: identity is
        already bound (typically to a COL canonical) and the caller
        wants to render the Norwegian Red List category. When the bound
        id has no assessment for the ``(area, source_release)`` pair,
        the overlay looks for NorTaxa rows in ``taxon_min`` whose
        ``canonical_scientific_name`` exactly matches the primary id's
        canonical name (case-sensitive, as stored) at picker-whitelist
        ranks. If exactly one such NorTaxa counterpart exists AND it
        carries an assessment, the returned :class:`RedlistLookupResult`
        is populated from the counterpart with ``overlay_source`` set to
        ``"nortaxa_name"`` and ``overlay_taxon_id`` set to that
        counterpart's ``taxon_id``.

        Safety limits:

        * Fires only when the primary lookup's status is ``"none"``. A
          primary ``"unique"``, ``"multiple_same_category"``, or
          ``"conflict"`` result is never overridden or augmented — the
          identity's own assessment is authoritative.
        * Requires exactly one NorTaxa counterpart. Zero or more than
          one → the overlay is skipped and the primary ``"none"``
          result is returned unchanged.
        * Excludes the primary ``sporely_taxon_id`` itself from the
          counterpart candidates (avoids picking a NorTaxa row that
          happens to already be the primary identity).
        * Match is by exact canonical string, not fuzzy or case-folded.
          Different concepts with distinct canonical names never
          cross-populate.

        The overlay never changes the observation's bound identity —
        only the surfaced assessment. Downstream code that persists
        identity fields (``sporely_taxon_id``, ``scientific_name_snapshot``,
        ``taxon_rank_snapshot``) reads from
        ``TaxonInputController.committed_snapshot`` and is unaffected.

        Proper COL↔NorTaxa concept unification (a single Sporely id per
        concept) belongs to the taxonomy compile pipeline; this overlay
        is a runtime accommodation until that unification lands.
        """
        primary = self.get_redlist_lookup(
            sporely_taxon_id, area=area, source_release=source_release,
        )
        if primary.status != "none":
            return primary
        try:
            primary_id_int = int(sporely_taxon_id)
        except (TypeError, ValueError):
            return primary
        rows = self._fetch_local_rows(
            "SELECT canonical_scientific_name FROM taxon_min "
            "WHERE taxon_id = ? LIMIT 1",
            (primary_id_int,),
        )
        if not rows:
            return primary
        canonical_name = str(rows[0]["canonical_scientific_name"] or "").strip()
        if not canonical_name:
            return primary
        allowed_ranks = ("species", "subspecies", "variety", "form")
        rank_placeholders = ",".join("?" for _ in allowed_ranks)
        counterpart_rows = self._fetch_local_rows(
            f"SELECT taxon_id FROM taxon_min "
            f"WHERE canonical_scientific_name = ? "
            f"AND canonical_source_system = 'nortaxa' "
            f"AND taxon_rank IN ({rank_placeholders}) "
            f"AND taxon_id != ?",
            (canonical_name, *allowed_ranks, primary_id_int),
        )
        counterpart_ids = [int(r["taxon_id"]) for r in counterpart_rows]
        if len(counterpart_ids) != 1:
            return primary
        overlay_id = counterpart_ids[0]
        overlay_result = self.get_redlist_lookup(
            overlay_id, area=area, source_release=source_release,
        )
        if overlay_result.status == "none":
            return primary
        return _dc_replace(
            overlay_result,
            overlay_source="nortaxa_name",
            overlay_taxon_id=overlay_id,
        )

    def get_redlist_assessment(
        self,
        sporely_taxon_id: int,
        *,
        area: str = "Norge",
        source_release: str = "2021",
    ) -> RedlistAssessment | None:
        """Return the Norwegian Red List assessment for a Sporely taxon, or
        ``None`` when there is nothing safely automatable to return.

        Returns ``None`` when:
          - the ``taxon_redlist_min`` table is absent (legacy DB);
          - no assessment row exists for ``(taxon, area, release)``;
          - the assessment group is in **conflict** (multiple rows disagree
            on category or rank). Callers that need to render a
            conflict-aware UI must go through :meth:`get_redlist_lookup`
            and handle ``status == "conflict"`` explicitly.

        Returns the deterministic representative (smallest numeric
        ``assessment_id``) on ``"unique"`` and ``"multiple_same_category"``.
        Never auto-picks a category for conflict groups.
        """
        result = self.get_redlist_lookup(
            sporely_taxon_id, area=area, source_release=source_release,
        )
        if result.status in ("unique", "multiple_same_category"):
            return result.assessment
        return None

    def resolve_common_name(
        self,
        name: str,
        genus: str | None = None,
        species: str | None = None,
    ) -> list[TaxonChoice]:
        name = _normalize_text(name)
        if not name:
            return []
        cache_key = (
            name.casefold(),
            _normalize_genus_display(genus) if genus is not None else None,
            _normalize_species_display(species) if species is not None else None,
        )
        cached = self._resolve_common_name_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        rows = self._local_common_name_rows(name=name, genus=genus, species=species)
        values = tuple(self._row_to_choice(row, "taxonomy") for row in rows)
        self._resolve_common_name_cache[cache_key] = values
        return list(values)

    def common_names_for_taxon(self, genus: str, species: str, limit: int = TAXON_COMPLETER_LIMIT) -> list[TaxonChoice]:
        return self.suggest_common_names(prefix="", genus=genus, species=species, limit=limit)

    def best_common_name_for_taxon(self, genus: str, species: str) -> TaxonChoice | None:
        key = (_normalize_genus_display(genus).casefold(), _normalize_species_display(species).casefold())
        if key in self._best_common_name_cache:
            return self._best_common_name_cache[key]
        rows = self._local_common_name_rows(genus=genus, species=species)
        if not rows:
            self._best_common_name_cache[key] = None
            return None
        if len(rows) == 1:
            choice = self._row_to_choice(rows[0], "taxonomy")
            self._best_common_name_cache[key] = choice
            return choice

        preferred_rows = [row for row in rows if bool(row["is_preferred_name"])]
        if len(preferred_rows) == 1:
            choice = self._row_to_choice(preferred_rows[0], "taxonomy")
            self._best_common_name_cache[key] = choice
            return choice

        # Multiple preferred rows — pick deterministically by the language
        # fan-out order derived from the caller's requested language. This
        # preserves the Norwegian display for taxa like `Laccaria laccata`
        # that carry both `nb` and `nn` preferred vernaculars (both should
        # display; the field takes the first fan-out language, and the
        # observation editor's chooser lists the rest as alternatives).
        try:
            from utils.vernacular_utils import resolve_query_language_codes
            priority = resolve_query_language_codes(self.language_code)
        except Exception:
            priority = ()
        candidates = preferred_rows or rows
        priority_index = {code: idx for idx, code in enumerate(priority)}

        def sort_key(row):
            lang = (row["language_code"] or "").lower()
            return (
                priority_index.get(lang, len(priority_index) + 1),
                lang,
                str(row["common_name"] or "").casefold(),
            )
        candidates_sorted = sorted(candidates, key=sort_key)
        choice = self._row_to_choice(candidates_sorted[0], "taxonomy")
        self._best_common_name_cache[key] = choice
        return choice


def determine_redlist_area(country_code: str | None) -> str | None:
    """Return ``"Norge"``, ``"Svalbard"``, or ``None`` from an ISO-3166-1 code.

    Companion to :meth:`TaxonLookupService.get_redlist_lookup`: picks the
    assessment area to query for a given observation's resolved country
    code. Uses Nominatim's convention where Svalbard is ``sj`` (Svalbard
    and Jan Mayen) and mainland Norway is ``no``. Anything else, and any
    missing/ambiguous code, yields ``None`` — callers must not silently
    fall back to a mainland assessment.

    Nominatim's ``sj`` ISO-3166-1 code covers both Svalbard and Jan
    Mayen. Stage 3B.5 maps ``sj`` to the Red List's ``Svalbard``
    assessment area because no finer persisted geographic field
    currently exists on observations, and Jan Mayen falls outside the
    compiled Norwegian Red List areas. A finer geographic classifier is
    a follow-up, not a Stage 3B.5 concern.
    """
    code = (country_code or "").strip().lower()
    if code == "sj":
        return "Svalbard"
    if code == "no":
        return "Norge"
    return None


__all__ = [
    "TAXON_COMPLETER_LIMIT",
    "ManualScientificResolution",
    "TaxonChoice",
    "TaxonLookupService",
    "RedlistAssessment",
    "RedlistLookupResult",
    "determine_redlist_area",
]
