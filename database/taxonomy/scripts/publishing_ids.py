"""Reviewed publishing-ID inputs for the SQLite builder.

Two committed, fingerprinted inputs add target-specific taxon ids that the
compiled release cannot derive on its own:

* the Artportalen publishing-ID overlay (``artportalen_overlay.py``), whose
  entries a person accepted;
* the iNaturalist refresh cache (``refresh_inaturalist_ids.py``), which
  re-validated, against the iNaturalist API, species whose legacy iNaturalist
  id was contradictory.

Both are applied fail-closed: an entry whose concept is missing or whose
scientific name no longer matches the release stops the build, so a rename or
split forces a new review instead of silently attaching an id to the wrong
concept. Neither input allocates or changes Sporely identity.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from artportalen_overlay import OverlayError, load_overlay

INATURALIST_REFRESH_FORMAT = "sporely-inaturalist-refresh-v1"

# (taxon_id, source_system, external_id, id_role, is_preferred, external_name, note)
IntRow = tuple


class PublishingIdError(Exception):
    pass


@dataclass(frozen=True)
class RefreshEntry:
    sporely_taxon_id: int
    scientific_name: str
    inaturalist_taxon_id: int | None
    inaturalist_name: str | None


@dataclass(frozen=True)
class InaturalistRefresh:
    acquired_on: str
    entries: tuple[RefreshEntry, ...]


def load_artportalen_overlay(path: Path) -> list[dict]:
    try:
        return list(load_overlay(path)["entries"])
    except OverlayError as exc:
        raise PublishingIdError(str(exc)) from exc


def load_inaturalist_refresh(path: Path) -> InaturalistRefresh:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PublishingIdError(f"cannot read iNaturalist refresh {path}: {exc}") from exc
    if data.get("format") != INATURALIST_REFRESH_FORMAT:
        raise PublishingIdError(f"{path}: not a {INATURALIST_REFRESH_FORMAT} file")
    entries: list[RefreshEntry] = []
    resolved_ids: set[int] = set()
    for raw in data.get("entries") or []:
        resolved = raw.get("status") == "resolved"
        inat = raw.get("inaturalist") or {}
        taxon_id = int(inat["taxon_id"]) if resolved else None
        if resolved:
            if taxon_id in resolved_ids:
                raise PublishingIdError(f"{path}: iNaturalist id {taxon_id} resolved for two concepts")
            resolved_ids.add(taxon_id)
        entries.append(RefreshEntry(int(raw["sporely_taxon_id"]), raw["scientific_name"],
                                    taxon_id, inat.get("name") if resolved else None))
    keys = [e.sporely_taxon_id for e in entries]
    if keys != sorted(set(keys)):
        raise PublishingIdError(f"{path}: entries must be unique and sorted by sporely_taxon_id")
    return InaturalistRefresh(str(data.get("acquired_on", "")), tuple(entries))


def _canonical_names(conn, taxon_ids: list[int]) -> dict[int, str]:
    names: dict[int, str] = {}
    for start in range(0, len(taxon_ids), 500):
        chunk = taxon_ids[start:start + 500]
        placeholders = ",".join("?" * len(chunk))
        names.update(conn.execute(
            f"SELECT taxon_id, canonical_scientific_name FROM taxon_min WHERE taxon_id IN ({placeholders})",
            chunk).fetchall())
    return names


def _require_names(conn, pairs: list[tuple[int, str]], label: str) -> None:
    names = _canonical_names(conn, [taxon_id for taxon_id, _ in pairs])
    for taxon_id, name in pairs:
        if taxon_id not in names:
            raise PublishingIdError(f"{label}: concept {taxon_id} ({name}) is not in this release")
        if names[taxon_id] != name:
            raise PublishingIdError(f"{label}: concept {taxon_id} is now {names[taxon_id]!r}, "
                                    f"the entry was reviewed as {name!r}; review it again")


def apply_artportalen_overlay(conn, entries: list[dict], rows: list[IntRow]) -> int:
    """Append overlay rows to the integer external-id rows. Returns rows added."""
    _require_names(conn, [(int(e["sporely_taxon_id"]), e["scientific_name"]) for e in entries],
                   "Artportalen overlay")
    holders: dict[int, set[int]] = {}
    for row in rows:
        if row[1] == "artportalen":
            holders.setdefault(int(row[2]), set()).add(int(row[0]))
    concept_ids = {int(r[0]) for r in rows if r[1] == "artportalen"}
    added = 0
    for entry in entries:
        concept, target_id = int(entry["sporely_taxon_id"]), int(entry["artportalen_taxon_id"])
        others = holders.get(target_id, set()) - {concept}
        if others:
            raise PublishingIdError(f"Artportalen overlay: id {target_id} is already attached to "
                                    f"{sorted(others)}, not only {concept}")
        if concept in concept_ids:
            if concept in holders.get(target_id, set()):
                continue  # the release already carries exactly this id
            raise PublishingIdError(f"Artportalen overlay: concept {concept} already has another "
                                    f"Artportalen id in this release")
        rows.append((concept, "artportalen", target_id, "publishing", 1,
                     entry["artportalen_scientific_name"],
                     f"reviewed_publishing_overlay:{entry['decision']}"))
        added += 1
    return added


def apply_inaturalist_refresh_rows(conn, refresh: InaturalistRefresh, rows: list[IntRow]) -> tuple[list[IntRow], dict]:
    """Replace legacy iNaturalist rows the refresh re-validated.

    For every resolved entry the concept's legacy iNaturalist rows, and legacy
    rows elsewhere that carry the freshly validated id, are dropped, and one
    row with the validated id is added. Unresolved entries change nothing.
    """
    resolved = [e for e in refresh.entries if e.inaturalist_taxon_id is not None]
    _require_names(conn, [(e.sporely_taxon_id, e.scientific_name) for e in refresh.entries],
                   "iNaturalist refresh")
    concepts = {e.sporely_taxon_id for e in resolved}
    fresh_ids = {e.inaturalist_taxon_id for e in resolved}
    kept: list[IntRow] = []
    dropped = 0
    for row in rows:
        if row[1] == "inaturalist" and (int(row[0]) in concepts or int(row[2]) in fresh_ids):
            dropped += 1
            continue
        kept.append(row)
    for e in resolved:
        kept.append((e.sporely_taxon_id, "inaturalist", e.inaturalist_taxon_id, "accepted", 1,
                     e.inaturalist_name, f"inaturalist_refresh:{refresh.acquired_on}"))
    return kept, {"resolved": len(resolved), "unresolved": len(refresh.entries) - len(resolved),
                  "legacy_rows_superseded": dropped}


def set_refreshed_inaturalist_columns(conn, refresh: InaturalistRefresh) -> None:
    """Fill taxon_min.inaturalist_taxon_id for resolved entries, one-to-one only."""
    for e in refresh.entries:
        if e.inaturalist_taxon_id is None:
            continue
        holder = conn.execute(
            "SELECT taxon_id FROM taxon_min WHERE inaturalist_taxon_id = ? AND taxon_id <> ?",
            (e.inaturalist_taxon_id, e.sporely_taxon_id)).fetchone()
        if holder:
            raise PublishingIdError(f"iNaturalist refresh: id {e.inaturalist_taxon_id} for concept "
                                    f"{e.sporely_taxon_id} is already the lookup id of concept {holder[0]}")
        current = conn.execute("SELECT inaturalist_taxon_id FROM taxon_min WHERE taxon_id = ?",
                               (e.sporely_taxon_id,)).fetchone()[0]
        if current not in (None, e.inaturalist_taxon_id):
            raise PublishingIdError(f"iNaturalist refresh: concept {e.sporely_taxon_id} already has "
                                    f"lookup id {current}")
        conn.execute("UPDATE taxon_min SET inaturalist_taxon_id = ? WHERE taxon_id = ?",
                     (e.inaturalist_taxon_id, e.sporely_taxon_id))
