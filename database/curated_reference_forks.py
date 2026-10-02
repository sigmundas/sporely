"""Fail-closed shared-contribution reads and owner-private copy creation."""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import sqlite3
import uuid
from datetime import datetime
from dataclasses import dataclass, replace
from typing import Any, Mapping, Protocol

from database.reference_library import ReferenceIntegrityError
from database.reference_library_schema import init_reference_library_schema
from database.schema import get_reference_connection
from references.measurement_content import (
    MeasurementContentError,
    content_from_row,
    encode_measurement_details,
    validate_measurement_content,
)


_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_CITATION_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,127}$")
_DOI = re.compile(r"^10\.[0-9]{4,9}/[-._;()/:a-z0-9]+$", re.I)
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:?\d{2})$")
_FULL_KEYS = frozenset({
    "curated_measurement_set_id", "bundle_revision", "status", "superseded_by_id",
    "published_at", "sporely_taxon_id", "canonical_scientific_name", "snapshot",
    "citation", "exports",
})
_SHARED_KEYS = frozenset({
    "contribution_id", "revision", "status", "shared_at", "sporely_taxon_id",
    "canonical_scientific_name", "contributor", "snapshot", "citation", "exports",
    "relationship_roles",
})
# ``relationship_roles`` is live, served state (Stage 2c, sporely-web
# 20261001091940): it is the owner's *current* public relationship and changes
# without a new revision. It is never part of frozen provenance, so a stored
# fork envelope is the served envelope without it.
_LIVE_ONLY_SHARED_KEYS = frozenset({"relationship_roles"})
# Stage A (sporely-web 20261001213000): a public read whose caller does not
# accept snapshot version 2 receives a v2 item projected to v1 and stamped
# with this optional envelope-level marker (the snapshot keeps the exact v1
# key set). Tolerated on both served envelope shapes, rejected in frozen
# provenance; a served marked item is readable but
# never copied (the copy would store a lossy projection as editable content).
MEASUREMENT_DETAILS_OMITTED_KEY = "measurement_details_omitted"

logger = logging.getLogger(__name__)
_FROZEN_SHARED_KEYS = _SHARED_KEYS - _LIVE_ONLY_SHARED_KEYS
# Owner decision (2026-10-02, sporely-web 20261002150000): stored fork
# provenance carries no contributor name, so a contributor's account deletion
# leaves nothing personal in other users' copies. New copies store the served
# envelope without ``contributor``; copies made before still carry it and
# stay valid. The contributor is resolved live for display
# (``contributor_label_for_fork``).
_PROVENANCE_EXCLUDED_KEYS = _LIVE_ONLY_SHARED_KEYS | {"contributor"}
_FROZEN_SHARED_KEYS_WITHOUT_CONTRIBUTOR = _FROZEN_SHARED_KEYS - {"contributor"}
# Label order for display: Supports · Contradicts · Compared.
RELATIONSHIP_ROLE_ORDER = ("supports_identification", "contradicts", "compared")
_RELATIONSHIP_ROLES = frozenset(RELATIONSHIP_ROLE_ORDER)
_CONTRIBUTOR_KEYS = frozenset({"id", "label"})
_SNAPSHOT_KEYS = frozenset({
    "schema_version", "reference_work_id", "reference_treatment_id",
    "reference_measurement_set_id", "reference_revision", "short_label",
    "full_citation", "work_type", "year", "doi", "isbn", "taxon_id",
    "name_as_published", "locator_text", "page_from", "page_to", "character",
    "data_kind", "raw_text", "measurements", "method", "raw_points",
})
_MEASUREMENT_KEYS = frozenset({
    "length_min", "length_core_min", "length_core_max", "length_max",
    "width_min", "width_core_min", "width_core_max", "width_max", "q_min",
    "q_max", "q_mean", "length_mean", "width_mean", "sample_size",
    "specimen_count",
})
# Version-keyed exact key sets (contract section 7). A curated bundle is
# accepted at exactly one of these shapes; an unknown version is rejected
# rather than read as version 1.
_SNAPSHOT_KEYS_BY_VERSION: Mapping[int, frozenset[str]] = {
    1: _SNAPSHOT_KEYS,
    2: _SNAPSHOT_KEYS | {"measurement_details"},
}
_MEASUREMENT_KEYS_BY_VERSION: Mapping[int, frozenset[str]] = {
    1: _MEASUREMENT_KEYS,
    2: _MEASUREMENT_KEYS | {"q_core_min", "q_core_max"},
}
_METHOD_KEYS = frozenset({"mount_medium", "stain", "preparation", "measurement_method"})
_CITATION_KEYS = frozenset({
    "schema_version", "citation_key", "type", "authors", "editors", "title",
    "container_title", "year", "edition", "publisher", "place", "volume",
    "issue", "pages", "doi", "isbn", "url", "language", "short_citation",
    "full_citation",
})
# Served since sporely-web 20260930232633: the shared candidate's work carries
# ``short_label`` (the snapshot label, <=512) and ``citation_override``
# (<=8192), and the public citation is ``work || {schema_version,
# citation_key, short_citation, full_citation}``, so both keys appear in every
# newer citation. Older revisions lack them; both shapes are accepted.
_CITATION_WORK_KEYS = frozenset({"short_label", "citation_override"})
_CITATION_KEYS_WITH_WORK = _CITATION_KEYS | _CITATION_WORK_KEYS
_EXPORT_KEYS = frozenset({"plain_text", "bibtex", "csl_json"})
_AGENT_KEYS = frozenset({"family", "given", "literal"})
_CSL_KEYS = frozenset({
    "id", "type", "author", "editor", "title", "container-title", "issued",
    "edition", "publisher", "publisher-place", "volume", "issue", "page",
    "DOI", "ISBN", "URL", "language",
})


class CuratedReferenceError(ValueError):
    """A catalogue payload or requested operation violates Stage 6k."""


class CuratedCatalogueClient(Protocol):
    def search_public_reference_contributions_v2(
        self, sporely_taxon_id: int, limit: int, after_shared_at: str | None,
        after_id: str | None,
    ) -> object: ...

    def get_public_curated_reference_set(
        self, curated_measurement_set_id: str, bundle_revision: int,
    ) -> object: ...

    def submit_private_reference_for_curation(
        self, source_measurement_set_id: str, expected_work_revision: int,
        expected_treatment_revision: int, expected_measurement_set_revision: int,
        attestation_version: str, rights_confirmed: bool,
        curation_consent_confirmed: bool,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class CuratedReferenceBundle:
    curated_measurement_set_id: str
    bundle_revision: int
    sporely_taxon_id: int
    canonical_scientific_name: str
    published_at: str
    snapshot: dict[str, Any]
    citation: dict[str, Any]
    exports: dict[str, Any]
    source_envelope: dict[str, Any]
    contributor_id: str | None = None
    contributor_label: str | None = None
    # Current public relationship of the owner's uses (live, not provenance).
    relationship_roles: tuple[str, ...] = ()
    # The server projected a v2 item to v1 for this read (Stage A marker).
    measurement_details_omitted: bool = False

    @property
    def contribution_id(self) -> str:
        return self.curated_measurement_set_id

    @property
    def revision(self) -> int:
        return self.bundle_revision


@dataclass(frozen=True, slots=True)
class CuratedReferenceFork:
    curated_measurement_set_id: str
    bundle_revision: int
    sporely_taxon_id: int
    reference_work_id: str
    taxon_treatment_id: str
    reference_measurement_set_id: str
    source_sha256: str
    created: bool


@dataclass(frozen=True, slots=True)
class CuratedSubmissionResult:
    status: str
    submission_id: str | None
    candidate_revision: int | None


def _positive_int(value: object, maximum: int = 2_147_483_647) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value <= maximum


def _bounded_text(value: object, maximum: int, *, required: bool = False) -> bool:
    if value is None:
        return not required
    return isinstance(value, str) and len(value) <= maximum and (not required or bool(value.strip()))


def _finite_number_or_none(value: object) -> bool:
    return value is None or (
        isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
    )


def _exact_mapping(value: object, keys: frozenset[str]) -> Mapping[str, Any] | None:
    return value if isinstance(value, dict) and frozenset(value) == keys else None


def _measurement_details_from_snapshot(snapshot: Mapping[str, Any]) -> str | None:
    """Validate the version-2 extension against the whole candidate row and
    return its canonical stored encoding.

    A curated copy creates *editable* local content, so it is deliberately
    excluded from the opaque-acceptance rule for future details versions
    (contract section 8): an unsupported version is rejected here instead of
    producing a local row nothing in this desktop may edit. ``mode="edit"``
    is what enforces that.
    """
    measurements = snapshot["measurements"]
    details_object = snapshot["measurement_details"]
    if details_object is not None and not isinstance(details_object, dict):
        raise CuratedReferenceError("invalid curated measurement details")
    row = {
        "character": snapshot["character"],
        "data_kind": snapshot["data_kind"],
        "raw_text": snapshot["raw_text"],
        "mount_medium": snapshot["method"]["mount_medium"],
        "stain": snapshot["method"]["stain"],
        "preparation": snapshot["method"]["preparation"],
        "measurement_method": snapshot["method"]["measurement_method"],
        "sample_size": measurements["sample_size"],
        "specimen_count": measurements["specimen_count"],
        "measurement_details_json": (
            None if details_object is None else _json(details_object)
        ),
    }
    for key in _MEASUREMENT_KEYS_BY_VERSION[2] - {"sample_size", "specimen_count"}:
        row[key] = measurements[key]
    try:
        content = content_from_row(row)
        validate_measurement_content(content, mode="edit")
    except MeasurementContentError as exc:
        raise CuratedReferenceError(
            f"invalid curated measurement details: {exc}"
        ) from exc
    return encode_measurement_details(content.details)


def _validate_snapshot(value: object, set_id: str, revision: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CuratedReferenceError("invalid curated snapshot shape")
    version = value.get("schema_version")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version not in _SNAPSHOT_KEYS_BY_VERSION
    ):
        raise CuratedReferenceError("unsupported curated snapshot version")
    snapshot = _exact_mapping(value, _SNAPSHOT_KEYS_BY_VERSION[version])
    if snapshot is None:
        raise CuratedReferenceError("invalid curated snapshot shape")
    if snapshot["reference_measurement_set_id"] != set_id or snapshot["reference_revision"] != revision:
        raise CuratedReferenceError("curated snapshot identity mismatch")
    for key in ("reference_work_id", "reference_treatment_id", "reference_measurement_set_id"):
        if not isinstance(snapshot[key], str) or not _UUID.fullmatch(snapshot[key]):
            raise CuratedReferenceError("invalid curated snapshot UUID")
    if snapshot["work_type"] not in {"book", "article", "chapter", "website", "dataset", "other"}:
        raise CuratedReferenceError("invalid curated work type")
    if snapshot["character"] != "spore_size" or snapshot["data_kind"] not in {"range", "summary", "raw_points", "parmasto"}:
        raise CuratedReferenceError("invalid curated measurement kind")
    if len(json.dumps(snapshot, ensure_ascii=False).encode("utf-8")) > 65536:
        raise CuratedReferenceError("oversized curated snapshot")
    for key in ("short_label", "full_citation", "name_as_published"):
        if not _bounded_text(snapshot[key], 65536, required=True):
            raise CuratedReferenceError(f"invalid curated snapshot {key}")
    for key in ("doi", "isbn", "taxon_id", "locator_text", "raw_text"):
        if snapshot[key] is not None and not isinstance(snapshot[key], str):
            raise CuratedReferenceError(f"invalid curated snapshot {key}")
    for key in ("year", "page_from", "page_to"):
        if snapshot[key] is not None and (not isinstance(snapshot[key], int) or isinstance(snapshot[key], bool)):
            raise CuratedReferenceError(f"invalid curated snapshot {key}")
    measurements = _exact_mapping(
        snapshot["measurements"], _MEASUREMENT_KEYS_BY_VERSION[version]
    )
    method = _exact_mapping(snapshot["method"], _METHOD_KEYS)
    if measurements is None or method is None or not all(_finite_number_or_none(v) for v in measurements.values()):
        raise CuratedReferenceError("invalid curated measurement payload")
    for key in ("sample_size", "specimen_count"):
        value = measurements[key]
        if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
            raise CuratedReferenceError("invalid curated count")
    if any(not _bounded_text(v, 4096) for v in method.values()):
        raise CuratedReferenceError("invalid curated method")
    raw_points = snapshot["raw_points"]
    if raw_points is not None:
        if not isinstance(raw_points, list) or not raw_points or len(raw_points) > 10_000:
            raise CuratedReferenceError("invalid curated raw points")
        for point in raw_points:
            if isinstance(point, (int, float, bool)):
                if isinstance(point, float) and not math.isfinite(point):
                    raise CuratedReferenceError("invalid curated raw point")
                continue
            if (not isinstance(point, dict) or not point or not set(point) <= {"length", "width", "l", "w", "q"}
                    or not any(key in point for key in ("length", "width", "l", "w"))
                    or any(not isinstance(item, (int, float, bool)) or (isinstance(item, float) and not math.isfinite(item)) for item in point.values())):
                raise CuratedReferenceError("invalid curated raw point")
    if version >= 2:
        _measurement_details_from_snapshot(snapshot)
    return dict(snapshot)


def _validate_agents(value: object) -> bool:
    if not isinstance(value, list) or len(value) > 100 or len(json.dumps(value, ensure_ascii=False).encode("utf-8")) > 65536:
        return False
    for agent in value:
        if isinstance(agent, str):
            if not agent.strip() or len(agent.encode("utf-16-le")) // 2 > 1024:
                return False
            continue
        if not isinstance(agent, dict) or not agent or not set(agent) <= _AGENT_KEYS:
            return False
        if not any(isinstance(v, str) and v.strip() for v in agent.values()):
            return False
        if any(v is not None and (
            not isinstance(v, str) or len(v.encode("utf-16-le")) // 2 > 1024
        ) for v in agent.values()):
            return False
    return True


def _validate_csl(csl: object, citation: Mapping[str, Any]) -> bool:
    if not isinstance(csl, dict) or not set(csl) <= _CSL_KEYS:
        return False
    type_map = {"book": "book", "article": "article-journal", "chapter": "chapter", "website": "webpage", "dataset": "dataset", "other": "document"}
    if csl.get("id") != citation["citation_key"] or csl.get("type") != type_map[citation["type"]] or not _bounded_text(csl.get("title"), 2048, required=True):
        return False
    for key in ("author", "editor"):
        if key in csl:
            agents = csl[key]
            if (not isinstance(agents, list) or len(agents) > 100
                    or any(not isinstance(agent, dict) or not agent or not set(agent) <= _AGENT_KEYS
                           or any(not isinstance(value, str) or not value.strip() or len(value) > 1024 for value in agent.values())
                           for agent in agents)):
                return False
    bounds = {
        "container-title": 2048, "edition": 256, "publisher": 1024,
        "publisher-place": 1024, "volume": 128, "issue": 128, "page": 256,
        "DOI": 255, "ISBN": 64, "URL": 2048, "language": 64,
    }
    if any(key in csl and not _bounded_text(csl[key], maximum, required=True)
           for key, maximum in bounds.items()):
        return False
    if "URL" in csl and not re.match(r"^https?://", csl["URL"], re.I):
        return False
    if csl.get("DOI") != citation["doi"]:
        return False
    if "issued" in csl:
        issued = csl["issued"]
        if (not isinstance(issued, dict) or set(issued) != {"date-parts"}
                or not isinstance(issued["date-parts"], list)
                or len(issued["date-parts"]) != 1
                or not isinstance(issued["date-parts"][0], list)
                or len(issued["date-parts"][0]) != 1
                or not _positive_int(issued["date-parts"][0][0], 9999)):
            return False
    return len(json.dumps(csl, ensure_ascii=False).encode("utf-8")) <= 131072


def _validate_relationship_roles(value: object) -> tuple[str, ...]:
    if (not isinstance(value, list) or len(value) > len(_RELATIONSHIP_ROLES)
            or any(not isinstance(role, str) or role not in _RELATIONSHIP_ROLES for role in value)
            or len(set(value)) != len(value) or value != sorted(value)):
        raise CuratedReferenceError("invalid contribution relationship roles")
    return tuple(role for role in RELATIONSHIP_ROLE_ORDER if role in value)


def normalize_curated_bundle(
    value: object, *, expected_taxon_id: int | None = None, frozen: bool = False,
) -> CuratedReferenceBundle:
    """Validate a served (or, with ``frozen=True``, stored) envelope.

    A served shared row must carry ``relationship_roles``; a frozen
    provenance envelope must not (it is stripped before hashing).
    """
    if isinstance(value, dict) and MEASUREMENT_DETAILS_OMITTED_KEY in value:
        if frozen:
            # Frozen provenance (cloud fork pull, portable and bundle import)
            # must be a complete served envelope; a projected one is never
            # accepted as a frozen copy.
            raise CuratedReferenceError(
                "frozen provenance must not omit measurement details"
            )
        if value[MEASUREMENT_DETAILS_OMITTED_KEY] is not True:
            raise CuratedReferenceError("invalid measurement-details-omitted marker")
        unmarked = {
            key: item for key, item in value.items()
            if key != MEASUREMENT_DETAILS_OMITTED_KEY
        }
        bundle = normalize_curated_bundle(
            unmarked, expected_taxon_id=expected_taxon_id, frozen=frozen,
        )
        return replace(
            bundle,
            measurement_details_omitted=True,
            source_envelope={**bundle.source_envelope, MEASUREMENT_DETAILS_OMITTED_KEY: True},
        )
    if (
        frozen and isinstance(value, dict)
        and frozenset(value) == _FROZEN_SHARED_KEYS_WITHOUT_CONTRIBUTOR
    ):
        # Contribution fork provenance as stored since the owner decision:
        # validated exactly like the served envelope, attribution omitted.
        bundle = normalize_curated_bundle(
            {**value, "contributor": {"id": None, "label": "-"}},
            expected_taxon_id=expected_taxon_id, frozen=True,
        )
        return replace(
            bundle, source_envelope=json.loads(_json(value)),
            contributor_id=None, contributor_label=None,
        )
    shared_keys = _FROZEN_SHARED_KEYS if frozen else _SHARED_KEYS
    if isinstance(value, dict) and frozenset(value) == shared_keys:
        contributor = _exact_mapping(value["contributor"], _CONTRIBUTOR_KEYS)
        if contributor is None:
            raise CuratedReferenceError("invalid contribution attribution")
        contributor_id = contributor["id"]
        if contributor_id is not None and (
            not isinstance(contributor_id, str) or not _UUID.fullmatch(contributor_id)
        ):
            raise CuratedReferenceError("invalid contributor identity")
        if not _bounded_text(contributor["label"], 512, required=True):
            raise CuratedReferenceError("invalid contributor label")
        legacy = {
            "curated_measurement_set_id": value["contribution_id"],
            "bundle_revision": value["revision"],
            "status": "published" if value["status"] == "shared" else value["status"],
            "superseded_by_id": None,
            "published_at": value["shared_at"],
            "sporely_taxon_id": value["sporely_taxon_id"],
            "canonical_scientific_name": value["canonical_scientific_name"],
            "snapshot": value["snapshot"],
            "citation": value["citation"],
            "exports": value["exports"],
        }
        roles = () if frozen else _validate_relationship_roles(value["relationship_roles"])
        bundle = normalize_curated_bundle(legacy, expected_taxon_id=expected_taxon_id)
        # Provenance excludes the live relationship (a role change never
        # changes the stored envelope or its sha256) and, for a served
        # envelope, the contributor (never stored). A frozen envelope is kept
        # as stored, so an older copy that still has it keeps its digest.
        excluded = _LIVE_ONLY_SHARED_KEYS if frozen else _PROVENANCE_EXCLUDED_KEYS
        provenance = {key: item for key, item in value.items() if key not in excluded}
        return CuratedReferenceBundle(
            bundle.curated_measurement_set_id, bundle.bundle_revision,
            bundle.sporely_taxon_id, bundle.canonical_scientific_name,
            bundle.published_at, bundle.snapshot, bundle.citation, bundle.exports,
            json.loads(json.dumps(provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
            contributor_id, contributor["label"], roles,
        )
    row = _exact_mapping(value, _FULL_KEYS)
    if row is None or row["status"] != "published" or row["superseded_by_id"] is not None:
        raise CuratedReferenceError("curated bundle is not selectable")
    set_id = row["curated_measurement_set_id"]
    revision = row["bundle_revision"]
    taxon_id = row["sporely_taxon_id"]
    if not isinstance(set_id, str) or not _UUID.fullmatch(set_id) or not _positive_int(revision):
        raise CuratedReferenceError("invalid curated identity")
    if not _positive_int(taxon_id) or (expected_taxon_id is not None and taxon_id != expected_taxon_id):
        raise CuratedReferenceError("curated exact taxon identity mismatch")
    if not _bounded_text(row["canonical_scientific_name"], 1024, required=True):
        raise CuratedReferenceError("invalid canonical scientific name")
    if not _bounded_text(row["published_at"], 64, required=True) or not _TIMESTAMP.match(row["published_at"]):
        raise CuratedReferenceError("invalid publication timestamp")
    try:
        datetime.fromisoformat(row["published_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise CuratedReferenceError("invalid publication timestamp") from exc
    snapshot = _validate_snapshot(row["snapshot"], set_id, revision)
    citation = _exact_mapping(row["citation"], _CITATION_KEYS)
    if citation is None:
        citation = _exact_mapping(row["citation"], _CITATION_KEYS_WITH_WORK)
        if citation is not None and not (
            _bounded_text(citation["short_label"], 512)
            and _bounded_text(citation["citation_override"], 8192)
        ):
            raise CuratedReferenceError("invalid curated citation label or override")
    exports = _exact_mapping(row["exports"], _EXPORT_KEYS)
    if citation is None or citation["schema_version"] != 1 or exports is None:
        raise CuratedReferenceError("invalid curated citation or exports")
    if not isinstance(citation["citation_key"], str) or not _CITATION_KEY.fullmatch(citation["citation_key"]):
        raise CuratedReferenceError("invalid curated citation key")
    if citation["type"] not in {"book", "article", "chapter", "website", "dataset", "other"}:
        raise CuratedReferenceError("invalid curated citation type")
    if not _validate_agents(citation["authors"]):
        raise CuratedReferenceError("invalid curated authors")
    if not _validate_agents(citation["editors"]):
        raise CuratedReferenceError("invalid curated editors")
    if not _bounded_text(citation["title"], 2048, required=True):
        raise CuratedReferenceError("invalid curated title")
    if not _bounded_text(citation["short_citation"], 512, required=True) or not _bounded_text(citation["full_citation"], 65536, required=True):
        raise CuratedReferenceError("invalid curated citation text")
    for key, maximum in (
        ("citation_key", 128), ("container_title", 2048), ("edition", 256),
        ("publisher", 1024), ("place", 1024), ("volume", 128), ("issue", 128),
        ("pages", 256), ("doi", 255), ("isbn", 64), ("url", 2048), ("language", 64),
    ):
        if not _bounded_text(citation[key], maximum):
            raise CuratedReferenceError(f"invalid curated citation {key}")
    if citation["year"] is not None and not _positive_int(citation["year"], 9999):
        raise CuratedReferenceError("invalid curated citation year")
    if citation["doi"] is not None and not _DOI.fullmatch(citation["doi"]):
        raise CuratedReferenceError("invalid curated citation DOI")
    if citation["url"] is not None and citation["url"].strip() and not re.match(r"^https?://", citation["url"], re.I):
        raise CuratedReferenceError("invalid curated citation URL")
    if not isinstance(exports["plain_text"], str) or not exports["plain_text"] or len(exports["plain_text"].encode()) > 65536:
        raise CuratedReferenceError("invalid plain-text export")
    if not isinstance(exports["bibtex"], str) or not exports["bibtex"] or len(exports["bibtex"].encode()) > 131072:
        raise CuratedReferenceError("invalid BibTeX export")
    csl = exports["csl_json"]
    if not _validate_csl(csl, citation):
        raise CuratedReferenceError("invalid CSL export")
    envelope = json.loads(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return CuratedReferenceBundle(
        set_id, revision, taxon_id, row["canonical_scientific_name"], row["published_at"],
        snapshot, dict(citation), dict(exports), envelope,
    )


def validate_frozen_curated_provenance(
    source_envelope_json: object,
    source_sha256: object,
    *,
    curated_measurement_set_id: object,
    bundle_revision: object,
    sporely_taxon_id: object,
) -> CuratedReferenceBundle:
    """Validate immutable imported/cloud provenance before it reaches SQLite."""
    if not isinstance(source_envelope_json, str):
        raise CuratedReferenceError("invalid frozen envelope")
    encoded = source_envelope_json.encode("utf-8")
    if not 2 <= len(encoded) <= 1_048_576:
        raise CuratedReferenceError("invalid frozen envelope size")
    if not isinstance(source_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise CuratedReferenceError("invalid frozen envelope digest")
    if hashlib.sha256(encoded).hexdigest() != source_sha256:
        raise CuratedReferenceError("frozen envelope digest mismatch")
    try:
        bundle = normalize_curated_bundle(
            json.loads(source_envelope_json), expected_taxon_id=sporely_taxon_id, frozen=True,
        )
    except (json.JSONDecodeError, TypeError) as exc:
        raise CuratedReferenceError("invalid frozen envelope JSON") from exc
    if (bundle.curated_measurement_set_id != curated_measurement_set_id
            or bundle.bundle_revision != bundle_revision):
        raise CuratedReferenceError("frozen envelope identity mismatch")
    return bundle


def search_shared_reference_contributions(client: CuratedCatalogueClient, sporely_taxon_id: int, *, limit: int = 25) -> tuple[CuratedReferenceBundle, ...]:
    if not _positive_int(sporely_taxon_id) or not _positive_int(limit, 100):
        raise CuratedReferenceError("catalogue search requires a positive exact taxon ID and limit <= 100")
    search = getattr(client, "search_public_reference_contributions_v2", None)
    if search is None:
        search = getattr(client, "search_public_curated_reference_sets", None)
    if not callable(search):
        raise CuratedReferenceError("cloud client does not support shared contribution search")
    response = search(sporely_taxon_id, limit, None, None)
    if not isinstance(response, list) or len(response) > limit:
        raise CuratedReferenceError("invalid or oversized catalogue response")
    result: list[CuratedReferenceBundle] = []
    seen: set[tuple[str, int]] = set()
    for index, row in enumerate(response):
        # One unreadable item (unknown shape or version, bad marker) must not
        # fail the whole page: skip it and log, keep the rest.
        try:
            bundle = normalize_curated_bundle(row, expected_taxon_id=sporely_taxon_id)
        except CuratedReferenceError as exc:
            logger.warning("skipping shared reference item %d: %s", index, exc)
            continue
        key = (bundle.curated_measurement_set_id, bundle.bundle_revision)
        if key in seen:
            logger.warning("skipping duplicate shared reference item %d", index)
            continue
        seen.add(key)
        result.append(bundle)
    return tuple(result)


# Compatibility alias for Stage 6 callers and persisted imports.
search_curated_catalogue = search_shared_reference_contributions


def submit_personal_reference_for_curation(
    client: CuratedCatalogueClient,
    measurement_set_id: str,
    *,
    attestation_version: str,
    rights_confirmed: bool,
    curation_consent_confirmed: bool,
) -> CuratedSubmissionResult:
    """Submit exactly the current owner graph revisions; never accepts actor IDs."""
    if not isinstance(measurement_set_id, str) or not _UUID.fullmatch(measurement_set_id):
        raise CuratedReferenceError("submission requires a measurement-set UUID")
    if not isinstance(attestation_version, str) or not attestation_version.strip():
        raise CuratedReferenceError("submission requires an attestation version")
    if rights_confirmed is not True or curation_consent_confirmed is not True:
        raise CuratedReferenceError("submission requires explicit rights and curation consent")
    conn = get_reference_connection()
    conn.row_factory = sqlite3.Row
    init_reference_library_schema(conn)
    try:
        row = conn.execute(
            "SELECT m.revision AS measurement_revision,t.revision AS treatment_revision,"
            "w.revision AS work_revision FROM reference_measurement_sets m "
            "JOIN reference_taxon_treatments t ON t.id=m.taxon_treatment_id "
            "JOIN reference_works w ON w.id=t.reference_work_id WHERE m.id=?",
            (measurement_set_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise CuratedReferenceError("submission source graph does not exist")
    response = client.submit_private_reference_for_curation(
        measurement_set_id, row["work_revision"], row["treatment_revision"],
        row["measurement_revision"], attestation_version.strip(), True, True,
    )
    if not isinstance(response, dict) or set(response) not in ({"status"}, {"status", "submission"}):
        raise CuratedReferenceError("malformed submission response")
    status = response.get("status")
    allowed = {
        "created", "no_change", "intake_disabled", "policy_not_configured",
        "rate_limited", "attestation_required", "account_deleting",
        "account_unavailable", "source_not_found_or_stale", "source_out_of_bounds",
        "active_submission_exists", "already_accepted",
    }
    if status not in allowed:
        raise CuratedReferenceError("unknown submission status")
    submission = response.get("submission")
    if submission is None:
        return CuratedSubmissionResult(status, None, None)
    if not isinstance(submission, dict) or not isinstance(submission.get("id"), str) or not _UUID.fullmatch(submission["id"]):
        raise CuratedReferenceError("malformed submission identity")
    revision = submission.get("candidate_revision")
    if not _positive_int(revision):
        raise CuratedReferenceError("malformed submission revision")
    return CuratedSubmissionResult(status, submission["id"], revision)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def same_fork_provenance(
    stored_json: object, stored_sha: object, other_json: object, other_sha: object,
) -> bool:
    """One rule for "same frozen source" (copy, push, pull, portable and
    bundle import).

    Byte-identical text and digest are the same. Otherwise both texts must
    match their own sha256 and parse to the same shared-contribution envelope
    once a top-level ``contributor`` is ignored: the server stores its own
    text form without the contributor (sporely-web 20261002150000) and older
    local copies still carry it. Legacy publication envelopes (no
    ``contribution_id``) must be byte-identical.
    """
    if stored_json == other_json and stored_sha == other_sha:
        return True
    parsed = []
    for text, sha in ((stored_json, stored_sha), (other_json, other_sha)):
        if not isinstance(text, str) or not isinstance(sha, str):
            return False
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != sha:
            return False
        try:
            value = json.loads(text)
        except ValueError:
            return False
        if not isinstance(value, dict) or "contribution_id" not in value:
            return False
        value.pop("contributor", None)
        parsed.append(value)
    return parsed[0] == parsed[1]


# Live contributor display ------------------------------------------------------
#: Label the server serves for a deleted contributor account.
DELETED_CONTRIBUTOR_LABEL = "Deleted user"
_contributor_cache: dict[tuple[str, str, int], str | None] = {}


def contributor_label_for_fork(
    client: object, contribution_id: str, revision: int, *, refresh: bool = False,
) -> str | None:
    """The contributor label of a copied contribution, resolved live.

    Stored provenance has no contributor, so display asks the public read
    (``get_public_reference_contribution_v2``) once per contribution revision
    and caches the answer for the session. ``None`` means unavailable
    (withdrawn, hidden, offline, malformed or no client); the caller shows a
    neutral label. Never raises and never touches the fork.
    """
    # Keyed by the signed-in account too; cleared on sign-in, sign-out and
    # account switch (utils.cloud_sync credential changes).
    viewer = str(getattr(client, "user_id", "") or "")
    key = (str(contribution_id), int(revision))
    cache_key = (viewer, *key)
    if not refresh and cache_key in _contributor_cache:
        return _contributor_cache[cache_key]
    label: str | None = None
    getter = getattr(client, "get_public_reference_contribution_v2", None)
    if callable(getter):
        try:
            rows = getter(str(contribution_id), int(revision))
            for row in rows if isinstance(rows, list) else ():
                try:
                    bundle = normalize_curated_bundle(row)
                except CuratedReferenceError:
                    continue
                if (bundle.contribution_id, bundle.bundle_revision) == key:
                    label = bundle.contributor_label
                    break
        except Exception as exc:  # display only; never fatal
            logger.info("contributor lookup failed for %s@%s: %s", key[0], key[1], exc)
            return None  # not cached: a transient failure may resolve later
    _contributor_cache[cache_key] = label
    return label


def clear_contributor_label_cache() -> None:
    """Forget live contributor labels (sign-in, sign-out, account switch)."""
    _contributor_cache.clear()


_clear_contributor_cache_for_tests = clear_contributor_label_cache


def _fork_from_row(row: sqlite3.Row, created: bool) -> CuratedReferenceFork:
    return CuratedReferenceFork(
        row["curated_measurement_set_id"], row["bundle_revision"], row["sporely_taxon_id"],
        row["reference_work_id"], row["taxon_treatment_id"],
        row["reference_measurement_set_id"], row["source_sha256"], created,
    )


def copy_curated_bundle_to_personal_library(bundle: CuratedReferenceBundle) -> CuratedReferenceFork:
    """Create one fresh local graph and provenance mapping atomically."""
    if bundle.measurement_details_omitted:
        raise CuratedReferenceError(
            "this contribution was served without its measurement details and cannot be copied"
        )
    source_json = _json(bundle.source_envelope)
    source_sha = hashlib.sha256(source_json.encode("utf-8")).hexdigest()
    conn = get_reference_connection()
    conn.row_factory = sqlite3.Row
    init_reference_library_schema(conn)
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM curated_reference_forks WHERE curated_measurement_set_id=? AND bundle_revision=?",
            (bundle.curated_measurement_set_id, bundle.bundle_revision),
        ).fetchone()
        if existing is not None:
            if (
                not same_fork_provenance(existing["source_envelope_json"], existing["source_sha256"],
                                         source_json, source_sha)
                or existing["sporely_taxon_id"] != bundle.sporely_taxon_id
            ):
                raise CuratedReferenceError("existing curated fork provenance disagrees")
            conn.commit()
            return _fork_from_row(existing, False)

        work_id, treatment_id, set_id = (str(uuid.uuid4()) for _ in range(3))
        citation, snapshot = bundle.citation, bundle.snapshot
        conn.execute(
            "INSERT INTO reference_works (id,type,citation_key,authors_json,editors_json,title,container_title,year,edition,publisher,place,volume,issue,pages,doi,isbn,url,language,short_label,citation_override,revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (work_id, citation["type"], citation["citation_key"], _json(citation["authors"]),
             _json(citation["editors"]), citation["title"], citation["container_title"], citation["year"],
             citation["edition"], citation["publisher"], citation["place"], citation["volume"],
             citation["issue"], citation["pages"], citation["doi"], citation["isbn"], citation["url"],
             citation["language"], citation["short_citation"], citation["full_citation"]),
        )
        conn.execute(
            "INSERT INTO reference_taxon_treatments (id,reference_work_id,taxon_id,name_as_published,page_from,page_to,locator_text,revision) VALUES (?,?,?,?,?,?,?,1)",
            (treatment_id, work_id, str(bundle.sporely_taxon_id), snapshot["name_as_published"],
             snapshot["page_from"], snapshot["page_to"], snapshot["locator_text"]),
        )
        m, method = snapshot["measurements"], snapshot["method"]
        # A version-1 bundle produces a legacy-only row with a NULL extension;
        # a version-2 bundle copies all three extension fields. There is no
        # third outcome: an unsupported version or unsupported details version
        # was already rejected by ``_validate_snapshot``, so this path never
        # stores a legacy-only projection of enhanced content.
        details_json = (
            _measurement_details_from_snapshot(snapshot)
            if snapshot["schema_version"] >= 2
            else None
        )
        conn.execute(
            "INSERT INTO reference_measurement_sets (id,taxon_treatment_id,character,raw_text,data_kind,length_min,length_core_min,length_core_max,length_max,width_min,width_core_min,width_core_max,width_max,q_min,q_max,q_mean,length_mean,width_mean,sample_size,specimen_count,mount_medium,stain,preparation,measurement_method,raw_points_json,measurement_details_json,q_core_min,q_core_max,revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (set_id, treatment_id, snapshot["character"], snapshot["raw_text"], snapshot["data_kind"],
             m["length_min"], m["length_core_min"], m["length_core_max"], m["length_max"],
             m["width_min"], m["width_core_min"], m["width_core_max"], m["width_max"],
             m["q_min"], m["q_max"], m["q_mean"], m["length_mean"], m["width_mean"],
             m["sample_size"], m["specimen_count"], method["mount_medium"], method["stain"],
             method["preparation"], method["measurement_method"],
             None if snapshot["raw_points"] is None else _json(snapshot["raw_points"]),
             details_json, m.get("q_core_min"), m.get("q_core_max")),
        )
        conn.execute(
            "INSERT INTO curated_reference_forks (curated_measurement_set_id,bundle_revision,sporely_taxon_id,reference_work_id,taxon_treatment_id,reference_measurement_set_id,source_envelope_json,source_sha256) VALUES (?,?,?,?,?,?,?,?)",
            (bundle.curated_measurement_set_id, bundle.bundle_revision, bundle.sporely_taxon_id,
             work_id, treatment_id, set_id, source_json, source_sha),
        )
        row = conn.execute(
            "SELECT * FROM curated_reference_forks WHERE curated_measurement_set_id=? AND bundle_revision=?",
            (bundle.curated_measurement_set_id, bundle.bundle_revision),
        ).fetchone()
        conn.commit()
        return _fork_from_row(row, True)
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise ReferenceIntegrityError(str(exc)) from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
