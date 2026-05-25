"""Shared non-UI taxonomy lookup helpers.

This module centralizes the current local taxonomy/common-name behavior while
also merging in reference-data genus/species suggestions. It deliberately stays
UI-free so a future autocomplete controller can reuse it without pulling in
Qt/PySide6.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import sqlite3
from typing import Any

from database.vernacular_db import VernacularDB
from utils.vernacular_utils import normalize_vernacular_language


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
        self.language_code = normalize_vernacular_language(language_code) if language_code else None
        if self.vernacular_db is not None:
            existing_language = normalize_vernacular_language(getattr(self.vernacular_db, "language_code", None))
            if self.language_code:
                try:
                    self.vernacular_db.language_code = self.language_code
                except Exception:
                    pass
            elif existing_language:
                self.language_code = existing_language
        self._reference_provider = _coerce_reference_provider(reference_db_factory)
        self._local_table_names_cache: set[str] | None = None
        self._local_column_cache: dict[str, set[str]] = {}

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

    def _local_suggest_genera(self, prefix: str) -> list[str]:
        if not self.vernacular_db:
            return []
        try:
            return list(self.vernacular_db.suggest_genus(prefix) or [])
        except Exception:
            return []

    def _local_suggest_species(self, genus: str, prefix: str) -> list[str]:
        if not self.vernacular_db:
            return []
        try:
            return list(self.vernacular_db.suggest_species(genus, prefix) or [])
        except Exception:
            return []

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
            filters.append("v.language_code = ?")
            params.append(language_code)

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

    def suggest_genera(self, prefix: str = "", limit: int = 50) -> list[str]:
        prefix = _normalize_text(prefix)
        seen: dict[str, str] = {}

        for value in self._local_suggest_genera(prefix):
            genus = _normalize_genus_display(value)
            if genus:
                seen.setdefault(genus.casefold(), genus)

        for value in self._reference_values("list_genera", prefix):
            genus = _normalize_genus_display(value)
            if genus:
                seen.setdefault(genus.casefold(), genus)

        return sorted(seen.values(), key=str.casefold)[: max(0, int(limit))]

    def suggest_species(self, genus: str, prefix: str = "", limit: int = 50) -> list[TaxonChoice]:
        genus = _normalize_genus_display(genus)
        prefix = _normalize_species_display(prefix)
        if not genus:
            return []

        local_species = {_normalize_species_display(value) for value in self._local_suggest_species(genus, prefix) if _normalize_species_display(value)}
        reference_species = {
            _normalize_species_display(value)
            for value in self._reference_values("list_species", genus, prefix)
            if _normalize_species_display(value)
        }

        ordered_species = sorted(local_species | reference_species, key=str.casefold)
        choices: list[TaxonChoice] = []
        for species in ordered_species[: max(0, int(limit))]:
            source = "taxonomy" if species in local_species else "reference"
            if species in local_species and species in reference_species:
                source = "both"
            if species in local_species:
                choice = self.resolve_scientific(genus, species)
                if choice is None:
                    choice = TaxonChoice(genus=genus, species=species, source="taxonomy")
                if source != choice.source:
                    choice = TaxonChoice(
                        genus=choice.genus,
                        species=choice.species,
                        common_name=choice.common_name,
                        family=choice.family,
                        source=source,
                        taxon_id=choice.taxon_id,
                        language_code=choice.language_code,
                        red_list_category=choice.red_list_category,
                        red_list_source=choice.red_list_source,
                    )
            else:
                choice = TaxonChoice(genus=genus, species=species, source=source)
            choices.append(choice)
        return choices

    def suggest_common_names(
        self,
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 50,
    ) -> list[TaxonChoice]:
        prefix = _normalize_text(prefix)
        if not prefix and genus is None and species is None:
            return []
        rows = self._local_common_name_rows(prefix=prefix or None, genus=genus, species=species, limit=limit)
        return [self._row_to_choice(row, "taxonomy") for row in rows[: max(0, int(limit))]]

    def resolve_scientific(self, genus: str, species: str) -> TaxonChoice | None:
        record = self._local_taxon_record(genus, species)
        if not record:
            return None
        best_common_name = self.best_common_name_for_taxon(record["genus"], record["species"])
        return TaxonChoice(
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

    def resolve_common_name(
        self,
        name: str,
        genus: str | None = None,
        species: str | None = None,
    ) -> list[TaxonChoice]:
        name = _normalize_text(name)
        if not name:
            return []
        rows = self._local_common_name_rows(name=name, genus=genus, species=species)
        return [self._row_to_choice(row, "taxonomy") for row in rows]

    def common_names_for_taxon(self, genus: str, species: str, limit: int = 20) -> list[TaxonChoice]:
        genus = _normalize_genus_display(genus)
        species = _normalize_species_display(species)
        if not genus or not species:
            return []
        rows = self._local_common_name_rows(genus=genus, species=species, limit=limit)
        return [self._row_to_choice(row, "taxonomy") for row in rows[: max(0, int(limit))]]

    def best_common_name_for_taxon(self, genus: str, species: str) -> TaxonChoice | None:
        rows = self._local_common_name_rows(genus=genus, species=species, limit=2)
        if not rows:
            return None
        if len(rows) == 1:
            return self._row_to_choice(rows[0], "taxonomy")

        first = rows[0]
        second = rows[1]
        first_preferred = bool(first["is_preferred_name"])
        second_preferred = bool(second["is_preferred_name"])
        if first_preferred and not second_preferred:
            return self._row_to_choice(first, "taxonomy")
        return None


__all__ = ["TaxonChoice", "TaxonLookupService"]
