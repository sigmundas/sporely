"""Fail-closed Stage 6k curated catalogue reads and personal fork creation."""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import uuid
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from database.reference_library import ReferenceIntegrityError
from database.reference_library_schema import init_reference_library_schema
from database.schema import get_reference_connection


_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_CITATION_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,127}$")
_DOI = re.compile(r"^10\.[0-9]{4,9}/[-._;()/:a-z0-9]+$", re.I)
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T.+(?:Z|[+-]\d{2}:?\d{2})$")
_FULL_KEYS = frozenset({
    "curated_measurement_set_id", "bundle_revision", "status", "superseded_by_id",
    "published_at", "sporely_taxon_id", "canonical_scientific_name", "snapshot",
    "citation", "exports",
})
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
_METHOD_KEYS = frozenset({"mount_medium", "stain", "preparation", "measurement_method"})
_CITATION_KEYS = frozenset({
    "schema_version", "citation_key", "type", "authors", "editors", "title",
    "container_title", "year", "edition", "publisher", "place", "volume",
    "issue", "pages", "doi", "isbn", "url", "language", "short_citation",
    "full_citation",
})
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
    def search_public_curated_reference_sets(
        self, sporely_taxon_id: int, limit: int, after_published_at: str | None,
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


def _validate_snapshot(value: object, set_id: str, revision: int) -> dict[str, Any]:
    snapshot = _exact_mapping(value, _SNAPSHOT_KEYS)
    if snapshot is None or snapshot["schema_version"] != 1:
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
    measurements = _exact_mapping(snapshot["measurements"], _MEASUREMENT_KEYS)
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


def normalize_curated_bundle(value: object, *, expected_taxon_id: int | None = None) -> CuratedReferenceBundle:
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
        bundle = normalize_curated_bundle(json.loads(source_envelope_json), expected_taxon_id=sporely_taxon_id)
    except (json.JSONDecodeError, TypeError) as exc:
        raise CuratedReferenceError("invalid frozen envelope JSON") from exc
    if (bundle.curated_measurement_set_id != curated_measurement_set_id
            or bundle.bundle_revision != bundle_revision):
        raise CuratedReferenceError("frozen envelope identity mismatch")
    return bundle


def search_curated_catalogue(client: CuratedCatalogueClient, sporely_taxon_id: int, *, limit: int = 20) -> tuple[CuratedReferenceBundle, ...]:
    if not _positive_int(sporely_taxon_id) or not _positive_int(limit, 50):
        raise CuratedReferenceError("catalogue search requires a positive exact taxon ID and limit <= 50")
    response = client.search_public_curated_reference_sets(sporely_taxon_id, limit, None, None)
    if not isinstance(response, list) or len(response) > limit:
        raise CuratedReferenceError("invalid or oversized catalogue response")
    result: list[CuratedReferenceBundle] = []
    seen: set[tuple[str, int]] = set()
    for row in response:
        bundle = normalize_curated_bundle(row, expected_taxon_id=sporely_taxon_id)
        key = (bundle.curated_measurement_set_id, bundle.bundle_revision)
        if key in seen:
            raise CuratedReferenceError("duplicate curated identity")
        seen.add(key)
        result.append(bundle)
    return tuple(result)


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


def _fork_from_row(row: sqlite3.Row, created: bool) -> CuratedReferenceFork:
    return CuratedReferenceFork(
        row["curated_measurement_set_id"], row["bundle_revision"], row["sporely_taxon_id"],
        row["reference_work_id"], row["taxon_treatment_id"],
        row["reference_measurement_set_id"], row["source_sha256"], created,
    )


def copy_curated_bundle_to_personal_library(bundle: CuratedReferenceBundle) -> CuratedReferenceFork:
    """Create one fresh local graph and provenance mapping atomically."""
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
            if existing["source_sha256"] != source_sha or existing["sporely_taxon_id"] != bundle.sporely_taxon_id:
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
        conn.execute(
            "INSERT INTO reference_measurement_sets (id,taxon_treatment_id,character,raw_text,data_kind,length_min,length_core_min,length_core_max,length_max,width_min,width_core_min,width_core_max,width_max,q_min,q_max,q_mean,length_mean,width_mean,sample_size,specimen_count,mount_medium,stain,preparation,measurement_method,raw_points_json,revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (set_id, treatment_id, snapshot["character"], snapshot["raw_text"], snapshot["data_kind"],
             m["length_min"], m["length_core_min"], m["length_core_max"], m["length_max"],
             m["width_min"], m["width_core_min"], m["width_core_max"], m["width_max"],
             m["q_min"], m["q_max"], m["q_mean"], m["length_mean"], m["width_mean"],
             m["sample_size"], m["specimen_count"], method["mount_medium"], method["stain"],
             method["preparation"], method["measurement_method"],
             None if snapshot["raw_points"] is None else _json(snapshot["raw_points"])),
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
