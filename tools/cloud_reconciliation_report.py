#!/usr/bin/env python3
"""Stage 2 read-only cloud image reconciliation report.

Compares local SQLite desired state (authoritative) against the current cloud
image inventory and classifies each discrepancy into one of the fixed
categories described in ``docs/supabase-sync-contract.md``. The tool is
strictly non-mutating: it never writes to the SQLite database, never issues a
mutating cloud call, and never triggers the storage-desired initializer side
effect. Callers reach an apply/delete mode only by passing an explicitly
unimplemented flag, which raises immediately.

Usage::

    python tools/cloud_reconciliation_report.py [--db PATH] [--json OUT]
        [--observation ID] [--limit N]

The tool prints a human-readable report on stdout and optionally writes a
full JSON payload for automated inspection.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_database_path  # noqa: E402


# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------

CATEGORY_HEALTHY_UPLOADED = "A_healthy_uploaded"
CATEGORY_METADATA_ONLY_ANCHOR_OK = "B_metadata_only_anchor_ok"
CATEGORY_UNWANTED_CLOUD_BYTES = "C_unwanted_cloud_bytes"
CATEGORY_LOST_LINK_REPAIRABLE_KEEP = "D1_lost_link_repairable_keep"
CATEGORY_LOST_LINK_REPAIRABLE_REMOVE = "D2_lost_link_repairable_remove"
CATEGORY_DUPLICATE_OR_AMBIGUOUS = "E_duplicate_or_ambiguous"
CATEGORY_CLOUD_ONLY_ORPHAN = "F_cloud_only_orphan"
CATEGORY_BROKEN_ACTIVE = "G_broken_active"
CATEGORY_INCOMPLETE_UPLOAD_METADATA = "H_incomplete_upload_metadata"


CATEGORY_ORDER = [
    CATEGORY_HEALTHY_UPLOADED,
    CATEGORY_METADATA_ONLY_ANCHOR_OK,
    CATEGORY_UNWANTED_CLOUD_BYTES,
    CATEGORY_LOST_LINK_REPAIRABLE_KEEP,
    CATEGORY_LOST_LINK_REPAIRABLE_REMOVE,
    CATEGORY_DUPLICATE_OR_AMBIGUOUS,
    CATEGORY_CLOUD_ONLY_ORPHAN,
    CATEGORY_BROKEN_ACTIVE,
    CATEGORY_INCOMPLETE_UPLOAD_METADATA,
]


CATEGORY_HUMAN_LABELS = {
    CATEGORY_HEALTHY_UPLOADED: (
        "A. healthy_uploaded — desired=true, cloud row has bytes, link intact"
    ),
    CATEGORY_METADATA_ONLY_ANCHOR_OK: (
        "B. metadata_only_anchor_ok — legitimate microscope metadata-only anchor"
    ),
    CATEGORY_UNWANTED_CLOUD_BYTES: (
        "C. unwanted_cloud_bytes — local desired=false, cloud still has bytes"
    ),
    CATEGORY_LOST_LINK_REPAIRABLE_KEEP: (
        "D1. lost_link_repairable_keep — desktop_id match, desired=true, "
        "repair link and keep bytes"
    ),
    CATEGORY_LOST_LINK_REPAIRABLE_REMOVE: (
        "D2. lost_link_repairable_remove — desktop_id match, desired=false, "
        "repair link then unwanted_cloud_bytes"
    ),
    CATEGORY_DUPLICATE_OR_AMBIGUOUS: (
        "E. duplicate_or_ambiguous — multi-way match; needs human review"
    ),
    CATEGORY_CLOUD_ONLY_ORPHAN: (
        "F. cloud_only_orphan — no local match by cloud_id or desktop_id"
    ),
    CATEGORY_BROKEN_ACTIVE: (
        "G. broken_active — local UPLOADED/DELETE_PENDING but no cloud row"
    ),
    CATEGORY_INCOMPLETE_UPLOAD_METADATA: (
        "H. incomplete_upload_metadata — cloud row missing storage_path where expected"
    ),
}


CATEGORIES_ALWAYS_LIST_EXAMPLES = {
    CATEGORY_UNWANTED_CLOUD_BYTES,
    CATEGORY_LOST_LINK_REPAIRABLE_KEEP,
    CATEGORY_LOST_LINK_REPAIRABLE_REMOVE,
    CATEGORY_DUPLICATE_OR_AMBIGUOUS,
    CATEGORY_CLOUD_ONLY_ORPHAN,
    CATEGORY_BROKEN_ACTIVE,
    CATEGORY_INCOMPLETE_UPLOAD_METADATA,
}


EXAMPLES_PER_CATEGORY = 10


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_microscope_image_type(value: Any) -> bool:
    return _normalize_text(value).lower() == "microscope"


def _has_bytes(cloud_row: dict) -> bool:
    return bool(_normalize_text(cloud_row.get("storage_path")))


def _is_cloud_row_deleted(cloud_row: dict) -> bool:
    return bool(_normalize_text(cloud_row.get("deleted_at")))


# ---------------------------------------------------------------------------
# Read-only SQLite access
# ---------------------------------------------------------------------------


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise RuntimeError(f"Local database not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _load_rows(conn: sqlite3.Connection, table: str) -> list[dict]:
    try:
        cursor = conn.execute(f"SELECT * FROM {table}")
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in cursor.fetchall()]


CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX = "sporely_cloud_image_storage_excluded_ids_"
CLOUD_METADATA_ONLY_PREFIX = "sporely_cloud_metadata_only_image_ids_"
CLOUD_STORAGE_INIT_PREFIX = "sporely_cloud_image_storage_initialized_"


def _load_setting(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None
    value = row["value"] if isinstance(row, sqlite3.Row) else row[0]
    return value if value is not None else None


def _load_all_settings_with_prefix(
    conn: sqlite3.Connection, prefix: str
) -> dict[str, str]:
    try:
        cursor = conn.execute(
            "SELECT key, value FROM settings WHERE key LIKE ?",
            (f"{prefix}%",),
        )
    except sqlite3.OperationalError:
        return {}
    result: dict[str, str] = {}
    for row in cursor.fetchall():
        key = row["key"] if isinstance(row, sqlite3.Row) else row[0]
        value = row["value"] if isinstance(row, sqlite3.Row) else row[1]
        if key is None:
            continue
        result[str(key)] = str(value) if value is not None else ""
    return result


def _parse_json_id_list(raw: str | None) -> set[int]:
    if not raw:
        return set()
    try:
        values = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(values, list):
        return set()
    out: set[int] = set()
    for item in values:
        parsed = _safe_int(item)
        if parsed > 0:
            out.add(parsed)
    return out


def _observation_id_from_setting_key(key: str, prefix: str) -> int:
    return _safe_int(key[len(prefix):])


# ---------------------------------------------------------------------------
# Local state snapshot
# ---------------------------------------------------------------------------


@dataclass
class LocalState:
    """Read-only snapshot of the relevant local SQLite state."""

    observations_by_id: dict[int, dict] = field(default_factory=dict)
    observations_by_cloud_id: dict[str, dict] = field(default_factory=dict)
    images_by_id: dict[int, dict] = field(default_factory=dict)
    images_by_cloud_id: dict[str, list[dict]] = field(default_factory=dict)
    images_by_observation_id: dict[int, list[dict]] = field(default_factory=dict)
    tombstones_by_deleted_cloud_id: dict[str, dict] = field(default_factory=dict)
    tombstones_by_local_image_id: dict[int, dict] = field(default_factory=dict)
    cloud_storage_excluded_by_obs: dict[int, set[int]] = field(default_factory=dict)
    cloud_metadata_only_by_obs: dict[int, set[int]] = field(default_factory=dict)
    cloud_storage_initialized_obs: set[int] = field(default_factory=set)

    def cloud_bytes_desired(self, observation_id: int, image_id: int) -> bool:
        """Local Stage-1 canonical byte-storage predicate.

        Mirrors ``utils.cloud_sync.cloud_image_bytes_desired`` semantics:
        anything not in the excluded set is desired. Reads only the raw
        setting the user already persisted; it never triggers the Stage 1
        initializer.
        """
        if observation_id <= 0 or image_id <= 0:
            return False
        excluded = self.cloud_storage_excluded_by_obs.get(observation_id, set())
        return image_id not in excluded

    def is_metadata_only_anchor_local(self, image_row: dict) -> bool:
        """Return True when local considers this image a legitimate anchor.

        Anchors are stored in ``sporely_cloud_metadata_only_image_ids_<obs>``
        per Stage 1 semantics. This predicate deliberately does not fall back
        to heuristics: the local persistence is authoritative for anchor
        intent.
        """
        obs_id = _safe_int(image_row.get("observation_id"))
        local_image_id = _safe_int(image_row.get("id"))
        if obs_id <= 0 or local_image_id <= 0:
            return False
        anchor_ids = self.cloud_metadata_only_by_obs.get(obs_id, set())
        return local_image_id in anchor_ids


def load_local_state(conn: sqlite3.Connection) -> LocalState:
    """Read all local rows/settings we need in a single pass."""
    state = LocalState()

    observations = _load_rows(conn, "observations")
    for row in observations:
        obs_id = _safe_int(row.get("id"))
        if obs_id > 0:
            state.observations_by_id[obs_id] = row
        cloud_id = _normalize_text(row.get("cloud_id"))
        if cloud_id:
            state.observations_by_cloud_id[cloud_id] = row

    images = _load_rows(conn, "images")
    for row in images:
        image_id = _safe_int(row.get("id"))
        if image_id <= 0:
            continue
        state.images_by_id[image_id] = row
        cloud_id = _normalize_text(row.get("cloud_id"))
        if cloud_id:
            state.images_by_cloud_id.setdefault(cloud_id, []).append(row)
        obs_id = _safe_int(row.get("observation_id"))
        if obs_id > 0:
            state.images_by_observation_id.setdefault(obs_id, []).append(row)

    tombstones = _load_rows(conn, "image_tombstones")
    for row in tombstones:
        deleted_cloud_id = _normalize_text(row.get("deleted_cloud_id"))
        if deleted_cloud_id:
            state.tombstones_by_deleted_cloud_id[deleted_cloud_id] = row
        local_image_id = _safe_int(row.get("local_image_id"))
        if local_image_id > 0:
            state.tombstones_by_local_image_id[local_image_id] = row

    excluded_settings = _load_all_settings_with_prefix(
        conn, CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX
    )
    for key, raw in excluded_settings.items():
        obs_id = _observation_id_from_setting_key(
            key, CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX
        )
        if obs_id <= 0:
            continue
        state.cloud_storage_excluded_by_obs[obs_id] = _parse_json_id_list(raw)

    metadata_only_settings = _load_all_settings_with_prefix(
        conn, CLOUD_METADATA_ONLY_PREFIX
    )
    for key, raw in metadata_only_settings.items():
        obs_id = _observation_id_from_setting_key(
            key, CLOUD_METADATA_ONLY_PREFIX
        )
        if obs_id <= 0:
            continue
        state.cloud_metadata_only_by_obs[obs_id] = _parse_json_id_list(raw)

    init_settings = _load_all_settings_with_prefix(
        conn, CLOUD_STORAGE_INIT_PREFIX
    )
    for key, raw in init_settings.items():
        obs_id = _observation_id_from_setting_key(
            key, CLOUD_STORAGE_INIT_PREFIX
        )
        if obs_id > 0 and _normalize_text(raw) == "1":
            state.cloud_storage_initialized_obs.add(obs_id)

    return state


# ---------------------------------------------------------------------------
# Cloud row inventory
# ---------------------------------------------------------------------------


CLOUD_ROW_SELECT_FIELDS = [
    "id",
    "desktop_id",
    "user_id",
    "observation_id",
    "storage_path",
    "original_filename",
    "image_type",
    "micro_category",
    "sort_order",
    "deleted_at",
    "upload_mode",
    "source_width",
    "source_height",
    "stored_width",
    "stored_height",
    "stored_bytes",
    "created_at",
]

DEFAULT_PAGE_SIZE = 500


def fetch_cloud_image_rows(
    client: Any,
    *,
    scope_cloud_observation_ids: list[str] | None = None,
    include_deleted: bool = True,
    limit: int | None = None,
) -> list[dict]:
    """Fetch cloud ``observation_images`` rows via the client's read path.

    Uses ``get_read_only`` when present (canonical read-only entry) and falls
    back to ``_get`` otherwise. Deleted rows are included by default so
    orphan detection can distinguish tombstoned identity from truly missing
    ones.
    """
    fetch: Callable[[str], list] = getattr(
        client, "get_read_only", None
    ) or client._get

    select_clause = "select=" + ",".join(CLOUD_ROW_SELECT_FIELDS)
    ordering = "order=observation_id.asc,sort_order.asc,id.asc"
    user_id = _normalize_text(getattr(client, "user_id", ""))
    query = [
        f"user_id=eq.{user_id}",
        select_clause,
        ordering,
    ]
    if not include_deleted:
        query.insert(1, "deleted_at=is.null")
    if scope_cloud_observation_ids is not None:
        if not scope_cloud_observation_ids:
            return []
        ids_list = ",".join(str(v).strip() for v in scope_cloud_observation_ids if str(v).strip())
        if not ids_list:
            return []
        query.insert(0, f"observation_id=in.({ids_list})")

    rows: list[dict] = []
    offset = 0
    while True:
        current_limit = DEFAULT_PAGE_SIZE
        if limit is not None and limit > 0:
            remaining = limit - len(rows)
            if remaining <= 0:
                break
            current_limit = min(current_limit, remaining)
        page_query = query + [f"limit={current_limit}", f"offset={offset}"]
        page = fetch("observation_images?" + "&".join(page_query)) or []
        rows.extend([dict(item or {}) for item in page])
        if len(page) < current_limit:
            break
        offset += len(page)
        if limit is not None and limit > 0 and len(rows) >= limit:
            rows = rows[:limit]
            break
    return rows


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


MATCH_METHOD_CLOUD_ID = "cloud_id"
MATCH_METHOD_DESKTOP_ID = "desktop_id"
MATCH_METHOD_NONE = None


@dataclass
class MatchResult:
    """Result of resolving one cloud row to a local image row."""

    local_image: dict | None
    method: str | None
    ambiguous: bool = False
    ambiguity_reason: str | None = None
    # When resolving by desktop_id, note whether cloud_id was present but did
    # not match any local row (i.e. link is truly broken vs. never set).
    cloud_id_link_broken: bool = False
    candidate_local_image_ids: list[int] = field(default_factory=list)


def match_cloud_row_to_local(
    cloud_row: dict,
    local: LocalState,
) -> MatchResult:
    """Resolve one cloud row against the local snapshot.

    Primary: ``cloud_id`` link. If the local row's ``cloud_id`` matches the
    cloud row's id, return it.

    Fallback: unambiguous ``desktop_id`` match, scoped to the *same local
    observation* (via the cloud observation's ``desktop_id`` -> local
    observations_by_cloud_id) *and* same ``image_type``. If more than one
    candidate remains, refuse to match and flag ambiguous.
    """
    cloud_image_id = _normalize_text(cloud_row.get("id"))
    cloud_observation_id = _normalize_text(cloud_row.get("observation_id"))
    cloud_desktop_id = _safe_int(cloud_row.get("desktop_id"))
    cloud_image_type = _normalize_text(cloud_row.get("image_type")).lower()

    # Primary: exact cloud_id match on local images.
    if cloud_image_id and cloud_image_id in local.images_by_cloud_id:
        candidates = local.images_by_cloud_id[cloud_image_id]
        unique_ids = { _safe_int(c.get("id")) for c in candidates if _safe_int(c.get("id")) > 0 }
        if len(unique_ids) > 1:
            return MatchResult(
                local_image=None,
                method=None,
                ambiguous=True,
                ambiguity_reason=(
                    f"multiple local images ({sorted(unique_ids)}) point to "
                    f"cloud_id={cloud_image_id}"
                ),
                candidate_local_image_ids=sorted(unique_ids),
            )
        if len(candidates) == 1:
            return MatchResult(local_image=candidates[0], method=MATCH_METHOD_CLOUD_ID)

    # Fallback: desktop_id + owning-observation + image_type match.
    if cloud_desktop_id > 0:
        # Resolve the expected local observation via the cloud observation's
        # cloud_id (the desktop's cloud_id column on the observations table
        # holds the *cloud observation id*, so lookup is direct).
        local_obs_row = local.observations_by_cloud_id.get(cloud_observation_id)
        local_obs_id = _safe_int((local_obs_row or {}).get("id"))
        if local_obs_id > 0:
            candidates = [
                row
                for row in local.images_by_observation_id.get(local_obs_id, [])
                if _safe_int(row.get("id")) == cloud_desktop_id
            ]
            if cloud_image_type:
                candidates = [
                    row
                    for row in candidates
                    if _normalize_text(row.get("image_type")).lower() == cloud_image_type
                ]
            unique_ids = { _safe_int(c.get("id")) for c in candidates if _safe_int(c.get("id")) > 0 }
            if len(unique_ids) > 1:
                return MatchResult(
                    local_image=None,
                    method=None,
                    ambiguous=True,
                    ambiguity_reason=(
                        f"multiple local images ({sorted(unique_ids)}) match "
                        f"desktop_id={cloud_desktop_id} scoped to obs={local_obs_id}"
                    ),
                    candidate_local_image_ids=sorted(unique_ids),
                )
            if candidates:
                # A cloud row that has a cloud_id but no matching local
                # cloud_id is a *broken link* — the local cloud_id was cleared
                # or never persisted.
                local_row = candidates[0]
                local_cloud_id = _normalize_text(local_row.get("cloud_id"))
                broken = bool(cloud_image_id) and local_cloud_id != cloud_image_id
                return MatchResult(
                    local_image=local_row,
                    method=MATCH_METHOD_DESKTOP_ID,
                    cloud_id_link_broken=broken,
                )
    return MatchResult(local_image=None, method=MATCH_METHOD_NONE)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


# Sub-classifications for categories that have meaningful internal splits.
# These do not create new top-level categories; they annotate a row so the
# reader can drill down when the spec-defined A-H bucket is broad. Recorded
# on ``ClassifiedCloudRow.subcategory``.
SUBCATEGORY_G_HEALTHY_DELETE_LIFECYCLE = "G_healthy_delete_lifecycle"
SUBCATEGORY_G_REMOTE_ONLY_DELETION = "G_remote_only_deletion"
SUBCATEGORY_G_NO_CLOUD_ROW_FOUND = "G_no_cloud_row_found"
SUBCATEGORY_F_WITH_BYTES = "F_with_bytes"
SUBCATEGORY_F_METADATA_ONLY = "F_metadata_only"


@dataclass
class ClassifiedCloudRow:
    """One classified cloud image row and its resolution context."""

    category: str
    note: str
    cloud_id: str | None
    cloud_observation_id: str | None
    cloud_desktop_id: int | None
    cloud_image_type: str | None
    cloud_sort_order: Any
    cloud_storage_path: str | None
    cloud_has_bytes: bool
    cloud_is_deleted: bool
    match_method: str | None
    local_image_id: int | None
    local_observation_id: int | None
    local_cloud_id: str | None
    local_image_type: str | None
    local_cloud_bytes_desired: bool | None
    local_is_metadata_only_anchor: bool
    local_tombstone_present: bool
    ambiguity: str | None = None
    candidate_local_image_ids: list[int] = field(default_factory=list)
    subcategory: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "subcategory": self.subcategory,
            "note": self.note,
            "cloud_id": self.cloud_id,
            "cloud_observation_id": self.cloud_observation_id,
            "cloud_desktop_id": self.cloud_desktop_id,
            "cloud_image_type": self.cloud_image_type,
            "cloud_sort_order": self.cloud_sort_order,
            "cloud_storage_path": self.cloud_storage_path,
            "cloud_has_bytes": self.cloud_has_bytes,
            "cloud_is_deleted": self.cloud_is_deleted,
            "match_method": self.match_method,
            "local_image_id": self.local_image_id,
            "local_observation_id": self.local_observation_id,
            "local_cloud_id": self.local_cloud_id,
            "local_image_type": self.local_image_type,
            "local_cloud_bytes_desired": self.local_cloud_bytes_desired,
            "local_is_metadata_only_anchor": self.local_is_metadata_only_anchor,
            "local_tombstone_present": self.local_tombstone_present,
            "ambiguity": self.ambiguity,
            "candidate_local_image_ids": list(self.candidate_local_image_ids),
        }


def classify_cloud_row(
    cloud_row: dict,
    match: MatchResult,
    local: LocalState,
    *,
    duplicate_cloud_rows_for_local: set[int] | None = None,
) -> ClassifiedCloudRow:
    """Classify one cloud image row into a single Stage 2 category.

    ``duplicate_cloud_rows_for_local`` — set of local image ids that resolve
    to more than one *active* cloud row. When the current cloud row's matched
    local image is in that set, the classifier flags the row as E (ambiguous)
    even if the individual match itself was unambiguous.
    """
    cloud_image_id = _normalize_text(cloud_row.get("id")) or None
    cloud_observation_id = _normalize_text(cloud_row.get("observation_id")) or None
    cloud_desktop_id = _safe_int(cloud_row.get("desktop_id")) or None
    cloud_image_type = _normalize_text(cloud_row.get("image_type")) or None
    cloud_sort_order = cloud_row.get("sort_order")
    cloud_storage_path = _normalize_text(cloud_row.get("storage_path")) or None
    cloud_has_bytes = _has_bytes(cloud_row)
    cloud_is_deleted = _is_cloud_row_deleted(cloud_row)

    local_row = match.local_image or {}
    local_image_id = _safe_int(local_row.get("id")) or None
    local_observation_id = _safe_int(local_row.get("observation_id")) or None
    local_cloud_id = _normalize_text(local_row.get("cloud_id")) or None
    local_image_type = _normalize_text(local_row.get("image_type")) or None
    local_cloud_bytes_desired: bool | None
    if local_image_id and local_observation_id:
        local_cloud_bytes_desired = local.cloud_bytes_desired(
            local_observation_id, local_image_id
        )
    else:
        local_cloud_bytes_desired = None

    local_is_metadata_only_anchor = (
        local.is_metadata_only_anchor_local(local_row) if local_row else False
    )
    local_tombstone_present = False
    if local_image_id:
        local_tombstone_present = local_image_id in local.tombstones_by_local_image_id
    if not local_tombstone_present and cloud_image_id:
        local_tombstone_present = (
            cloud_image_id in local.tombstones_by_deleted_cloud_id
        )

    def build(
        category: str,
        note: str,
        *,
        ambiguity: str | None = None,
        subcategory: str | None = None,
    ) -> ClassifiedCloudRow:
        return ClassifiedCloudRow(
            category=category,
            note=note,
            cloud_id=cloud_image_id,
            cloud_observation_id=cloud_observation_id,
            cloud_desktop_id=cloud_desktop_id,
            cloud_image_type=cloud_image_type,
            cloud_sort_order=cloud_sort_order,
            cloud_storage_path=cloud_storage_path,
            cloud_has_bytes=cloud_has_bytes,
            cloud_is_deleted=cloud_is_deleted,
            match_method=match.method,
            local_image_id=local_image_id,
            local_observation_id=local_observation_id,
            local_cloud_id=local_cloud_id,
            local_image_type=local_image_type,
            local_cloud_bytes_desired=local_cloud_bytes_desired,
            local_is_metadata_only_anchor=local_is_metadata_only_anchor,
            local_tombstone_present=local_tombstone_present,
            ambiguity=ambiguity,
            candidate_local_image_ids=list(match.candidate_local_image_ids),
            subcategory=subcategory,
        )

    # Ambiguity from the match itself always wins.
    if match.ambiguous:
        return build(
            CATEGORY_DUPLICATE_OR_AMBIGUOUS,
            f"match ambiguous: {match.ambiguity_reason}",
            ambiguity=match.ambiguity_reason,
        )

    # Two active cloud rows pointing at the same local image: also E.
    if (
        not cloud_is_deleted
        and local_image_id
        and duplicate_cloud_rows_for_local
        and local_image_id in duplicate_cloud_rows_for_local
    ):
        return build(
            CATEGORY_DUPLICATE_OR_AMBIGUOUS,
            (
                f"multiple active cloud rows resolve to local image "
                f"{local_image_id}"
            ),
            ambiguity=f"multiple cloud rows -> local image {local_image_id}",
        )

    # Cloud-only orphan: no local match at all. Sub-flag bytes vs metadata.
    if match.local_image is None:
        deleted_flag = " (soft-deleted cloud row)" if cloud_is_deleted else ""
        if cloud_has_bytes:
            return build(
                CATEGORY_CLOUD_ONLY_ORPHAN,
                f"no local image matched by cloud_id or desktop_id (with_bytes){deleted_flag}",
                subcategory=SUBCATEGORY_F_WITH_BYTES,
            )
        return build(
            CATEGORY_CLOUD_ONLY_ORPHAN,
            f"no local image matched by cloud_id or desktop_id (metadata_only){deleted_flag}",
            subcategory=SUBCATEGORY_F_METADATA_ONLY,
        )

    # Match via desktop_id fallback => lost/broken link.
    if match.method == MATCH_METHOD_DESKTOP_ID and match.cloud_id_link_broken:
        # Include cloud-deleted rows here too: broken link on a deleted row
        # still deserves visibility, but classify by the local desire state.
        if local_cloud_bytes_desired:
            return build(
                CATEGORY_LOST_LINK_REPAIRABLE_KEEP,
                (
                    "cloud row matched by desktop_id only "
                    f"(local cloud_id={local_cloud_id or 'missing'}); "
                    "local desires bytes"
                ),
            )
        return build(
            CATEGORY_LOST_LINK_REPAIRABLE_REMOVE,
            (
                "cloud row matched by desktop_id only "
                f"(local cloud_id={local_cloud_id or 'missing'}); "
                "local does NOT desire bytes; after repair this becomes C"
            ),
        )

    # From here on we have a healthy cloud_id link (or a desktop_id match
    # where the cloud_id link is not broken — unusual, but treat like linked).
    # Cloud row is soft-deleted: split by whether local acknowledges the
    # deletion. A matching local tombstone means we are in healthy
    # DELETE_PENDING/DELETED lifecycle; a missing tombstone means the remote
    # deletion has not yet been recognized locally.
    if cloud_is_deleted:
        if not cloud_has_bytes and _is_microscope_image_type(cloud_image_type):
            return build(
                CATEGORY_METADATA_ONLY_ANCHOR_OK,
                "cloud row is soft-deleted; anchor-shape retained",
            )
        if local_tombstone_present:
            return build(
                CATEGORY_BROKEN_ACTIVE,
                (
                    "cloud row is soft-deleted with a matching local "
                    "tombstone (healthy DELETE_PENDING/DELETED lifecycle; "
                    "no Stage 2 action required)"
                ),
                subcategory=SUBCATEGORY_G_HEALTHY_DELETE_LIFECYCLE,
            )
        return build(
            CATEGORY_BROKEN_ACTIVE,
            (
                "cloud row is soft-deleted but no local tombstone exists "
                "(remote-only deletion discovered on desktop; needs "
                "explicit accept/restore decision)"
            ),
            subcategory=SUBCATEGORY_G_REMOTE_ONLY_DELETION,
        )

    # Case H: cloud row exists but storage_path missing where NOT a
    # legitimate anchor.
    if not cloud_has_bytes:
        if _is_microscope_image_type(cloud_image_type):
            if local_is_metadata_only_anchor:
                return build(
                    CATEGORY_METADATA_ONLY_ANCHOR_OK,
                    "metadata-only microscope anchor per local setting",
                )
            # No local anchor flag: still a *microscope* row with NULL
            # storage_path. Treat as metadata-only per contract (image_type
            # microscope + NULL storage_path is the canonical anchor shape)
            # BUT surface it as B with a note when local also considers it
            # microscope; else flag H.
            if _is_microscope_image_type(local_image_type):
                return build(
                    CATEGORY_METADATA_ONLY_ANCHOR_OK,
                    "microscope row with NULL storage_path (anchor shape); "
                    "local anchor flag not set (informational)",
                )
            return build(
                CATEGORY_INCOMPLETE_UPLOAD_METADATA,
                (
                    "cloud row missing storage_path; cloud image_type is "
                    "microscope but local row is not microscope"
                ),
            )
        return build(
            CATEGORY_INCOMPLETE_UPLOAD_METADATA,
            (
                f"cloud row missing storage_path; cloud image_type="
                f"{cloud_image_type or 'unknown'} (not a legitimate anchor)"
            ),
        )

    # Cloud row has bytes: apply Stage 2 primary target logic.
    if local_cloud_bytes_desired is False:
        return build(
            CATEGORY_UNWANTED_CLOUD_BYTES,
            (
                "local desired=false but cloud row still has bytes; "
                "primary Stage 2 target"
            ),
        )

    # Desired=true and bytes present, link intact => healthy.
    return build(
        CATEGORY_HEALTHY_UPLOADED,
        "desired=true, cloud row has bytes, cloud_id link intact",
    )


def collect_broken_active_orphans(
    local: LocalState,
    matched_local_image_ids: set[int],
) -> list[ClassifiedCloudRow]:
    """Category G: local rows that indicate UPLOADED/DELETE_PENDING but the
    corresponding cloud row was NOT observed during the scan.

    Only meaningful when the caller scans the full account (or the same
    observation the local row belongs to). Callers with a narrow scope should
    still see these, but the reporter surfaces the scope in the top-level
    summary so a user knows why local id X is flagged G.
    """
    out: list[ClassifiedCloudRow] = []
    for image_id, image_row in local.images_by_id.items():
        if image_id in matched_local_image_ids:
            continue
        local_cloud_id = _normalize_text(image_row.get("cloud_id"))
        if not local_cloud_id:
            continue
        # Skip images with a *synced* tombstone — those are DELETED, not
        # broken_active. A pending tombstone is DELETE_PENDING; still surface
        # it as G because the cloud row should exist somewhere.
        tombstone = local.tombstones_by_deleted_cloud_id.get(local_cloud_id)
        if tombstone and _normalize_text(tombstone.get("delete_synced_at")):
            continue
        obs_id = _safe_int(image_row.get("observation_id"))
        image_type = _normalize_text(image_row.get("image_type")) or None
        desired: bool | None
        if obs_id > 0 and image_id > 0:
            desired = local.cloud_bytes_desired(obs_id, image_id)
        else:
            desired = None
        anchor = local.is_metadata_only_anchor_local(image_row)
        state = "DELETE_PENDING" if tombstone else "UPLOADED"
        out.append(
            ClassifiedCloudRow(
                category=CATEGORY_BROKEN_ACTIVE,
                note=(
                    f"local state={state}: local cloud_id={local_cloud_id} "
                    f"has no corresponding cloud row in scanned scope"
                ),
                cloud_id=local_cloud_id,
                cloud_observation_id=None,
                cloud_desktop_id=None,
                cloud_image_type=None,
                cloud_sort_order=None,
                cloud_storage_path=None,
                cloud_has_bytes=False,
                cloud_is_deleted=False,
                match_method=None,
                local_image_id=image_id,
                local_observation_id=obs_id or None,
                local_cloud_id=local_cloud_id,
                local_image_type=image_type,
                local_cloud_bytes_desired=desired,
                local_is_metadata_only_anchor=anchor,
                local_tombstone_present=bool(tombstone),
                subcategory=SUBCATEGORY_G_NO_CLOUD_ROW_FOUND,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Reconciliation driver
# ---------------------------------------------------------------------------


@dataclass
class ReconciliationReport:
    generated_at: str
    scope: dict[str, Any]
    counts: dict[str, int]
    subcounts: dict[str, int]
    total_observations_scanned: int
    total_cloud_rows_scanned: int
    total_local_images_scanned: int
    rows: list[ClassifiedCloudRow]
    ambiguity_warnings: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "scope": self.scope,
            "counts": self.counts,
            "subcounts": self.subcounts,
            "total_observations_scanned": self.total_observations_scanned,
            "total_cloud_rows_scanned": self.total_cloud_rows_scanned,
            "total_local_images_scanned": self.total_local_images_scanned,
            "rows": [row.to_dict() for row in self.rows],
            "ambiguity_warnings": list(self.ambiguity_warnings),
        }


def reconcile(
    local: LocalState,
    cloud_rows: Iterable[dict],
    *,
    scope: dict[str, Any] | None = None,
    include_broken_active: bool = True,
) -> ReconciliationReport:
    """Classify every cloud row and, when scoped globally, emit category G rows."""
    cloud_rows_list = [dict(row or {}) for row in cloud_rows]

    # First pass: run matches and note which local ids get multi-hit by
    # *active* cloud rows.
    matches: list[MatchResult] = []
    active_hits_by_local: dict[int, int] = {}
    for cloud_row in cloud_rows_list:
        match = match_cloud_row_to_local(cloud_row, local)
        matches.append(match)
        if not _is_cloud_row_deleted(cloud_row) and match.local_image is not None:
            local_image_id = _safe_int(match.local_image.get("id"))
            if local_image_id > 0:
                active_hits_by_local[local_image_id] = (
                    active_hits_by_local.get(local_image_id, 0) + 1
                )
    duplicate_locals = {
        image_id for image_id, count in active_hits_by_local.items() if count > 1
    }

    classified: list[ClassifiedCloudRow] = []
    matched_local_image_ids: set[int] = set()
    ambiguity_warnings: list[str] = []
    for cloud_row, match in zip(cloud_rows_list, matches):
        row = classify_cloud_row(
            cloud_row,
            match,
            local,
            duplicate_cloud_rows_for_local=duplicate_locals,
        )
        classified.append(row)
        if match.local_image is not None:
            local_image_id = _safe_int(match.local_image.get("id"))
            if local_image_id > 0:
                matched_local_image_ids.add(local_image_id)
        if row.category == CATEGORY_DUPLICATE_OR_AMBIGUOUS:
            detail = row.ambiguity or row.note
            ambiguity_warnings.append(
                f"cloud_id={row.cloud_id} obs={row.cloud_observation_id}: {detail}"
            )

    if include_broken_active:
        classified.extend(
            collect_broken_active_orphans(local, matched_local_image_ids)
        )

    counts: dict[str, int] = {category: 0 for category in CATEGORY_ORDER}
    subcounts: dict[str, int] = {}
    for row in classified:
        counts[row.category] = counts.get(row.category, 0) + 1
        if row.subcategory:
            subcounts[row.subcategory] = subcounts.get(row.subcategory, 0) + 1

    observations_scanned = len(
        {
            row.cloud_observation_id
            for row in classified
            if row.cloud_observation_id
        }
    )
    if not observations_scanned:
        observations_scanned = len(
            {
                row.local_observation_id
                for row in classified
                if row.local_observation_id
            }
        )
    if not observations_scanned:
        observations_scanned = len(local.observations_by_id)

    return ReconciliationReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        scope=dict(scope or {}),
        counts=counts,
        subcounts=subcounts,
        total_observations_scanned=observations_scanned,
        total_cloud_rows_scanned=len(cloud_rows_list),
        total_local_images_scanned=len(local.images_by_id),
        rows=classified,
        ambiguity_warnings=ambiguity_warnings,
    )


# ---------------------------------------------------------------------------
# Text report renderer
# ---------------------------------------------------------------------------


def render_text_report(report: ReconciliationReport) -> str:
    lines: list[str] = []
    lines.append("Sporely Stage 2 cloud reconciliation report")
    lines.append(f"Generated at: {report.generated_at}")
    lines.append("Scope:")
    if not report.scope:
        lines.append("  (default)")
    else:
        for key in sorted(report.scope):
            lines.append(f"  {key}: {report.scope[key]}")
    lines.append("")
    lines.append(
        f"Total observations scanned: {report.total_observations_scanned}"
    )
    lines.append(
        f"Total cloud image rows scanned: {report.total_cloud_rows_scanned}"
    )
    lines.append(
        f"Total local image rows loaded: {report.total_local_images_scanned}"
    )
    lines.append("")
    lines.append("Per-category counts:")
    total = 0
    for category in CATEGORY_ORDER:
        count = report.counts.get(category, 0)
        total += count
        label = CATEGORY_HUMAN_LABELS.get(category, category)
        lines.append(f"  {label}: {count}")
    lines.append(f"  TOTAL classified rows: {total}")
    lines.append("")
    if report.subcounts:
        lines.append("Sub-category breakdown:")
        for sub in sorted(report.subcounts):
            lines.append(f"  {sub}: {report.subcounts[sub]}")
        lines.append("")
    if report.ambiguity_warnings:
        lines.append("*** AMBIGUITY WARNINGS ***")
        for warning in report.ambiguity_warnings:
            lines.append(f"  ! {warning}")
        lines.append("")

    for category in CATEGORY_ORDER:
        examples = [row for row in report.rows if row.category == category]
        if not examples:
            continue
        header_label = CATEGORY_HUMAN_LABELS.get(category, category)
        lines.append(f"--- {header_label} ({len(examples)} rows) ---")
        # Show subcategory breakdown when the category has one.
        sub_examples: dict[str, list[ClassifiedCloudRow]] = {}
        for row in examples:
            if row.subcategory:
                sub_examples.setdefault(row.subcategory, []).append(row)
        if sub_examples:
            for sub in sorted(sub_examples):
                lines.append(f"  subcategory {sub}: {len(sub_examples[sub])} rows")
        if category not in CATEGORIES_ALWAYS_LIST_EXAMPLES and len(examples) > 10:
            lines.append(
                f"(counts only for category {category}; enable JSON to see all rows)"
            )
            lines.append("")
            continue
        limit = EXAMPLES_PER_CATEGORY
        # When a category has subcategories, show up to `limit` examples per
        # subcategory so the reader gets a taste of each subtype.
        if sub_examples and len(sub_examples) > 1:
            for sub in sorted(sub_examples):
                bucket = sub_examples[sub]
                lines.append(f"  [{sub}] ({len(bucket)}):")
                shown = bucket[:limit]
                for row in shown:
                    has_bytes = "bytes" if row.cloud_has_bytes else "no-bytes"
                    match_method = row.match_method or "no-match"
                    lines.append(
                        f"    obs={row.cloud_observation_id or row.local_observation_id or '-'} "
                        f"cloud_id={row.cloud_id or '-'} local_id={row.local_image_id or '-'} "
                        f"image_type={row.cloud_image_type or row.local_image_type or '-'} "
                        f"storage_path={'set' if row.cloud_storage_path else 'null'} "
                        f"{has_bytes} match={match_method} note={row.note}"
                    )
                if len(bucket) > limit:
                    lines.append(f"    ... {len(bucket) - limit} more not shown")
            lines.append("")
            continue
        shown = examples[:limit]
        for row in shown:
            has_bytes = "bytes" if row.cloud_has_bytes else "no-bytes"
            match_method = row.match_method or "no-match"
            lines.append(
                f"  obs={row.cloud_observation_id or row.local_observation_id or '-'} "
                f"cloud_id={row.cloud_id or '-'} local_id={row.local_image_id or '-'} "
                f"image_type={row.cloud_image_type or row.local_image_type or '-'} "
                f"storage_path={'set' if row.cloud_storage_path else 'null'} "
                f"{has_bytes} match={match_method} note={row.note}"
            )
        if len(examples) > limit:
            lines.append(f"  ... {len(examples) - limit} more not shown")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        help=(
            "Path to the SQLite DB. Defaults to the standard app path "
            "(same resolution as the running app)."
        ),
    )
    parser.add_argument(
        "--json",
        help="Optional file to write the full JSON report to.",
    )
    parser.add_argument(
        "--observation",
        help=(
            "Limit the scan to one observation. Value may be a local "
            "observation id (integer) or a cloud observation UUID."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on the number of cloud rows scanned.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Stage 2 apply/delete mode is NOT implemented in this tool. "
            "Passing this flag raises immediately."
        ),
    )
    return parser.parse_args(argv)


def _resolve_scope(
    args: argparse.Namespace, local: LocalState
) -> tuple[list[str] | None, int | None, str | None]:
    """Turn --observation into (cloud_observation_ids, local_observation_id, display)."""
    if not args.observation:
        return None, None, None
    raw = str(args.observation).strip()
    if not raw:
        return None, None, None
    # Numeric = local id first; string with dashes = cloud id.
    if raw.isdigit():
        local_obs_id = int(raw)
        local_row = local.observations_by_id.get(local_obs_id)
        if not local_row:
            raise RuntimeError(f"Local observation {local_obs_id} not found.")
        cloud_id = _normalize_text(local_row.get("cloud_id"))
        if not cloud_id:
            raise RuntimeError(
                f"Local observation {local_obs_id} has no cloud_id; nothing "
                "to reconcile on the cloud side."
            )
        return [cloud_id], local_obs_id, f"local={local_obs_id} cloud={cloud_id}"
    # Otherwise treat as a cloud observation id.
    local_row = local.observations_by_cloud_id.get(raw)
    local_id = _safe_int((local_row or {}).get("id")) or None
    return [raw], local_id, f"cloud={raw} local={local_id or '-'}"


def _load_client_or_die() -> Any:
    from utils.cloud_sync import SporelyCloudClient  # noqa: E402

    client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        raise RuntimeError(
            "Could not load stored Sporely cloud credentials — sign in first."
        )
    return client


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.apply:
        raise NotImplementedError(
            "Stage 2 apply/delete mode is intentionally not implemented in "
            "this dry-run tool."
        )

    db_path = Path(args.db).expanduser() if args.db else get_database_path()
    with _open_readonly(db_path) as conn:
        local = load_local_state(conn)

    client = _load_client_or_die()
    scope_cloud_ids, scope_local_id, scope_display = _resolve_scope(args, local)

    limit = args.limit if args.limit and args.limit > 0 else None
    cloud_rows = fetch_cloud_image_rows(
        client,
        scope_cloud_observation_ids=scope_cloud_ids,
        include_deleted=True,
        limit=limit,
    )

    scope_meta = {
        "db_path": str(db_path),
        "observation_filter": scope_display,
        "limit": limit,
        "cloud_user_id": _normalize_text(getattr(client, "user_id", "")) or None,
    }

    include_broken_active = scope_cloud_ids is None
    report = reconcile(
        local,
        cloud_rows,
        scope=scope_meta,
        include_broken_active=include_broken_active,
    )

    text = render_text_report(report)
    print(text, end="")

    if args.json:
        out_path = Path(args.json).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report.to_json(), indent=2, ensure_ascii=False, default=str)
            + "\n",
            encoding="utf-8",
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(argv)
    except NotImplementedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - final catch-all
        print(f"Reconciliation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
