"""Citation and snapshot service for the normalized reference library.

One shared, deterministic house style. Structured fields are preferred;
``citation_override`` overrides the generated full citation when present.
No fabricated author, year, page, DOI, ISBN, or measurement values.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from database.reference_library import (
        MeasurementSet,
        ReferenceWork,
        TaxonTreatment,
    )


SNAPSHOT_SCHEMA_VERSION = 1


# --- Author / editor formatting ---------------------------------------------


def _load_json_list(raw: str | list | None) -> list[dict]:
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        parsed = raw
    else:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    if not isinstance(parsed, list):
        return []
    result: list[dict] = []
    for entry in parsed:
        if isinstance(entry, dict):
            result.append(entry)
        elif isinstance(entry, str):
            result.append({"family": entry.strip()})
    return result


def _agent_label(agent: dict) -> str:
    literal = str(agent.get("literal") or "").strip()
    if literal:
        return literal
    family = str(agent.get("family") or "").strip()
    given = str(agent.get("given") or "").strip()
    if family and given:
        # Short-form initials from the first character of each given name part.
        initials = " ".join(
            f"{part[0]}." for part in given.split() if part
        )
        return f"{family} {initials}".strip()
    if family:
        return family
    if given:
        return given
    return ""


def _agent_family_only(agent: dict) -> str:
    family = str(agent.get("family") or "").strip()
    if family:
        return family
    literal = str(agent.get("literal") or "").strip()
    if literal:
        return literal
    return str(agent.get("given") or "").strip()


def _format_agent_list(agents: list[dict]) -> str:
    labels = [label for label in (_agent_family_only(a) for a in agents) if label]
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} & {labels[1]}"
    return f"{labels[0]} et al."


def _format_agent_full_list(agents: list[dict]) -> str:
    labels = [label for label in (_agent_label(a) for a in agents) if label]
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + " & " + labels[-1]


# --- Public label / citation helpers ----------------------------------------


def build_short_label(work: "ReferenceWork") -> str:
    """Short display label (e.g. "Petersen et al. 1990").

    Prefers a stored ``short_label`` from the record itself; falls back to
    a deterministic derivation from structured author + year fields. If
    nothing usable is available, returns the title (or an empty string).
    """
    stored = str(getattr(work, "short_label", "") or "").strip()
    if stored:
        return stored
    authors = _load_json_list(getattr(work, "authors_json", None))
    author_label = _format_agent_list(authors)
    year = getattr(work, "year", None)
    year_text = str(year) if year is not None else ""
    if author_label and year_text:
        return f"{author_label} {year_text}"
    if author_label:
        return author_label
    if year_text:
        return year_text
    title = str(getattr(work, "title", "") or "").strip()
    return title


def build_full_citation(work: "ReferenceWork") -> str:
    """Generate a full citation from structured fields.

    Uses a single deterministic house style: ``Author(s) (Year). Title.
    Container, Volume(Issue), Pages. Edition. Publisher, Place. DOI/URL``.
    Missing components are simply omitted — incomplete citations remain
    visibly incomplete. If ``citation_override`` is set, it replaces the
    generated citation verbatim.
    """
    override = str(getattr(work, "citation_override", "") or "").strip()
    if override:
        return override

    authors = _load_json_list(getattr(work, "authors_json", None))
    editors = _load_json_list(getattr(work, "editors_json", None))
    parts: list[str] = []

    author_text = _format_agent_full_list(authors)
    if not author_text and editors:
        editor_text = _format_agent_full_list(editors)
        if editor_text:
            author_text = f"{editor_text} (ed.)"
    if author_text:
        parts.append(author_text)

    year = getattr(work, "year", None)
    if year is not None:
        parts.append(f"({year})")

    title = str(getattr(work, "title", "") or "").strip()
    if title:
        parts.append(f"{title}.")

    container = str(getattr(work, "container_title", "") or "").strip()
    volume = str(getattr(work, "volume", "") or "").strip()
    issue = str(getattr(work, "issue", "") or "").strip()
    pages = str(getattr(work, "pages", "") or "").strip()
    container_bits: list[str] = []
    if container:
        container_bits.append(container)
    if volume:
        container_bits.append(f"{volume}({issue})" if issue else volume)
    if pages:
        container_bits.append(pages)
    if container_bits:
        parts.append(", ".join(container_bits) + ".")

    edition = str(getattr(work, "edition", "") or "").strip()
    if edition:
        parts.append(f"{edition}.")

    publisher = str(getattr(work, "publisher", "") or "").strip()
    place = str(getattr(work, "place", "") or "").strip()
    if publisher and place:
        parts.append(f"{publisher}, {place}.")
    elif publisher:
        parts.append(f"{publisher}.")
    elif place:
        parts.append(f"{place}.")

    doi = str(getattr(work, "doi", "") or "").strip()
    url = str(getattr(work, "url", "") or "").strip()
    if doi:
        parts.append(f"https://doi.org/{doi}")
    elif url:
        parts.append(url)

    return " ".join(parts).strip()


# --- Snapshot ---------------------------------------------------------------


def build_observation_reference_snapshot(
    work: "ReferenceWork",
    treatment: "TaxonTreatment",
    measurement_set: "MeasurementSet",
) -> dict[str, Any]:
    """Build the canonical, public-safe observation reference snapshot.

    The result is a plain JSON-serializable dict with deterministic keys.
    It intentionally excludes local filesystem paths, owner identity,
    credentials, and private notes.
    """
    if measurement_set.taxon_treatment_id != treatment.id:
        raise ValueError(
            "measurement_set.taxon_treatment_id does not match treatment.id"
        )
    if treatment.reference_work_id != work.id:
        raise ValueError(
            "treatment.reference_work_id does not match work.id"
        )

    raw_points: Any = None
    if measurement_set.raw_points_json:
        try:
            raw_points = json.loads(measurement_set.raw_points_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            raw_points = None

    measurements = {
        "length_min": measurement_set.length_min,
        "length_core_min": measurement_set.length_core_min,
        "length_core_max": measurement_set.length_core_max,
        "length_max": measurement_set.length_max,
        "width_min": measurement_set.width_min,
        "width_core_min": measurement_set.width_core_min,
        "width_core_max": measurement_set.width_core_max,
        "width_max": measurement_set.width_max,
        "q_min": measurement_set.q_min,
        "q_max": measurement_set.q_max,
        "q_mean": measurement_set.q_mean,
        "length_mean": measurement_set.length_mean,
        "width_mean": measurement_set.width_mean,
        "sample_size": measurement_set.sample_size,
        "specimen_count": measurement_set.specimen_count,
    }

    method = {
        "mount_medium": measurement_set.mount_medium,
        "stain": measurement_set.stain,
        "preparation": measurement_set.preparation,
        "measurement_method": measurement_set.measurement_method,
    }

    snapshot: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "reference_work_id": work.id,
        "reference_measurement_set_id": measurement_set.id,
        "reference_treatment_id": treatment.id,
        "reference_revision": measurement_set.revision,
        "short_label": build_short_label(work),
        "full_citation": build_full_citation(work),
        "work_type": work.type,
        "year": work.year,
        "doi": work.doi,
        "isbn": work.isbn,
        "taxon_id": treatment.taxon_id,
        "name_as_published": treatment.name_as_published,
        "locator_text": treatment.locator_text,
        "page_from": treatment.page_from,
        "page_to": treatment.page_to,
        "character": measurement_set.character,
        "data_kind": measurement_set.data_kind,
        "raw_text": measurement_set.raw_text,
        "measurements": measurements,
        "method": method,
        "raw_points": raw_points,
    }
    return snapshot


def serialize_snapshot(snapshot: dict) -> str:
    """Deterministic JSON encoding for the snapshot."""
    return json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
