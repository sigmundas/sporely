"""Helpers for working with scientific-name text."""
from __future__ import annotations

import html
import re
from typing import Any

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_INFRASPECIFIC_MARKERS = {"cf", "cf.", "aff", "aff.", "sp", "sp.", "spp", "spp."}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = text.replace("\xa0", " ")
    text = text.replace("×", " x ")
    text = _TAG_RE.sub(" ", text)
    text = text.strip()
    if text.startswith("?"):
        text = text.lstrip("?").strip()
    return _WHITESPACE_RE.sub(" ", text)


def split_scientific_name_text(text: str | None) -> tuple[str | None, str | None]:
    """Return ``(genus, species)`` from a scientific-name string if possible."""
    value = _clean_text(text)
    if not value:
        return None, None

    parts = [part for part in value.split() if part]
    if len(parts) < 2:
        return None, None

    genus = parts[0].strip()
    species = parts[1].strip()
    if species.lower() in _INFRASPECIFIC_MARKERS:
        if len(parts) < 3:
            return None, None
        species = parts[2].strip()

    if not genus or not species:
        return None, None
    return genus, species


def resolve_observation_taxon_fields(
    genus: Any = None,
    species: Any = None,
    species_guess: Any = None,
    ai_selected_scientific_name: Any = None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve canonical taxon fields from the available observation text."""
    genus_text = _clean_text(genus)
    species_text = _clean_text(species)
    guess_text = _clean_text(species_guess)

    if genus_text and species_text:
        return genus_text, species_text, f"{genus_text} {species_text}".strip()

    for candidate in (guess_text, ai_selected_scientific_name):
        split = split_scientific_name_text(candidate)
        if not split:
            continue
        candidate_genus, candidate_species = split
        if not genus_text:
            genus_text = candidate_genus
        if not species_text:
            species_text = candidate_species
        if genus_text and species_text:
            return genus_text, species_text, f"{genus_text} {species_text}".strip()

    return genus_text or None, species_text or None, None


def format_probability_percent(value: Any) -> str:
    """Format a probability-like score as a percentage string."""
    if value in (None, ""):
        return ""

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value).strip()

    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0

    text = f"{numeric:.1f}".rstrip("0").rstrip(".")
    return f"{text}%"
