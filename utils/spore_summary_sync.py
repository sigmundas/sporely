"""Stage D — sync structured observation spore summaries to
public.observation_spore_summaries in Supabase.

Called from ``cloud_sync._push_measurements_for_current_observation`` after
the observation has an established cloud id. This module keeps summary sync
isolated from the ~15k-line ``cloud_sync.py`` so it stays trivially
unit-testable with a fake HTTP client.

Design notes:

* No local SQLite cache. Summaries are deterministic and cheap to recompute
  from the raw ``spore_measurements`` rows plus their parent image context;
  recomputing on every sync is the least-risky Stage D behavior (plan Stage
  D: "Local persistence decision — compute-on-sync is acceptable if
  summaries are deterministic"). If we ever need a cache for performance,
  it can be added as a pure additive layer.

* Every ``spore_measurements`` row has ``image_id INTEGER NOT NULL`` with a
  FK to ``images`` (see database/schema.py:1630). There are no orphan
  measurements in the local schema, so the Stage C
  ``load_measurements_with_context`` join is complete: every eligible
  measurement carries a context. Rows whose image context is entirely NULL
  land in the single "null-context" summary row.

* Upserts use the SporelyCloudClient private HTTP primitives
  (``_get`` / ``_post`` / ``_patch`` / ``_delete``). Same style as
  ``_push_spore_mosaic_for_observation`` in ``cloud_sync.py``. Tests can
  duck-type a fake client with those four methods.

* Missing-table handling matches the existing "compatibility skip" pattern
  in ``pull_observation_identifications``: if PostgREST reports that the
  table does not exist, we return ``STATUS_SKIP_TABLE_MISSING`` so an older
  cloud deployment does not break the whole sync.

* ``source_app`` is always ``"sporely-py"`` (baked into
  ``compute_observation_spore_summaries``). ``source_app_version`` is
  provided by the caller — normally sourced from ``main.APP_VERSION`` via
  ``cloud_sync.set_cloud_sync_source_app_version()`` — because importing
  ``main`` from a utility module would drag in PySide6.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Callable, Mapping, Protocol

from utils.spore_summary import (
    compute_observation_spore_summaries,
    load_measurements_with_context,
)


_SUMMARY_TABLE = "observation_spore_summaries"

# Result status codes (mirrors the mosaic pattern in cloud_sync.py — short
# kebab-cased strings so callers can aggregate and log them verbatim).
STATUS_SYNCED = "synced"
STATUS_SKIP_TABLE_MISSING = "skip_table_missing"
STATUS_SKIP_NO_CLOUD_ID = "skip_no_cloud_id"


class SummaryHttpClient(Protocol):
    """Minimal duck-typed interface required by the sync helper.

    In production this is a ``SporelyCloudClient``. Tests use a fake
    class implementing the same four methods.
    """

    user_id: str

    def _get(self, path: str) -> list: ...
    def _post(self, path: str, payload: Any) -> list: ...
    def _patch(self, path: str, payload: dict) -> None: ...
    def _delete(self, path: str) -> None: ...


# Full column set sent to Supabase for one summary row. Matches the Stage B
# schema minus db-generated fields (id, created_at, updated_at).
_SUMMARY_UPSERT_FIELDS: tuple[str, ...] = (
    "observation_id", "user_id",
    "context_hash", "context_json",
    "measurement_type",
    "sample_type", "mount_reagent", "stain_reagent", "contrast_method",
    "n_spores", "n_paired", "n_length", "n_width",
    "length_min_um", "length_p05_um", "length_mean_um", "length_median_um",
    "length_p95_um", "length_max_um", "length_sd_um",
    "width_min_um", "width_p05_um", "width_mean_um", "width_median_um",
    "width_p95_um", "width_max_um", "width_sd_um",
    "q_min", "q_p05", "q_mean", "q_median", "q_p95", "q_max", "q_sd",
    "stats_version", "computed_at", "source_app", "source_app_version",
)

# Fields used to decide whether a locally-computed payload MATERIALLY
# differs from an existing remote row. Excludes:
#   * observation_id / user_id — identity, same by construction.
#   * computed_at — a fresh timestamp on every compute. Including it
#     would make every reconciliation PATCH the row (moving computed_at
#     and, via trg_observation_spore_summaries_updated_at, updated_at).
#   * source_app / source_app_version — provenance stamps recording
#     "who wrote this row and with what writer version". They document
#     history rather than reflect measurement content, so a desktop
#     client version bump alone must NOT rewrite otherwise-identical
#     rows. When a real data change PATCHes the row, the current
#     version stamp goes along atomically; but the version alone never
#     triggers a rewrite.
# Only genuine data / summary-semantic columns remain (context fields,
# counts, statistics, `stats_version`) — the stats_version bump IS a
# legitimate rewrite signal because it means the writer's percentile
# or SD convention changed.
_SUMMARY_MATERIAL_FIELDS: tuple[str, ...] = tuple(
    key for key in _SUMMARY_UPSERT_FIELDS
    if key not in {
        "observation_id",
        "user_id",
        "computed_at",
        "source_app",
        "source_app_version",
    }
)

# PostgREST select= for the existing-row fetch. `id` is needed to build
# the PATCH URL; everything else is compared against the computed
# payload to decide PATCH-vs-skip.
_SUMMARY_SELECT_COLUMNS = ",".join(("id",) + _SUMMARY_MATERIAL_FIELDS)

# Numeric material columns whose values may cross a Python-int/float
# vs. PostgREST-int/float boundary. Coerce both sides before compare so
# a bigint round-tripping as string does not spuriously trigger a PATCH.
_SUMMARY_INT_FIELDS: frozenset[str] = frozenset({
    "n_spores", "n_paired", "n_length", "n_width", "stats_version",
})
_SUMMARY_FLOAT_FIELDS: frozenset[str] = frozenset({
    "length_min_um", "length_p05_um", "length_mean_um", "length_median_um",
    "length_p95_um", "length_max_um", "length_sd_um",
    "width_min_um", "width_p05_um", "width_mean_um", "width_median_um",
    "width_p95_um", "width_max_um", "width_sd_um",
    "q_min", "q_p05", "q_mean", "q_median", "q_p95", "q_max", "q_sd",
})
# Columns whose semantic type is JSON (jsonb on the server side).
# Compared STRUCTURALLY, never as raw strings, so a difference in
# serialization key order between local payload and remote row cannot
# produce a false PATCH. Applies to `context_json` today; kept as a
# set so a future jsonb column (e.g. Stage-later `quality_flags`) can
# be added without touching the equality function.
_SUMMARY_JSON_FIELDS: frozenset[str] = frozenset({
    "context_json",
})


def _coerce_json_value(value: Any) -> Any:
    """Best-effort coerce PostgREST / writer output to a structural
    JSON value. PostgREST normally returns jsonb columns as already-
    parsed Python dicts/lists, but a proxy layer, alternate client
    config, or hand-written payload could pass a serialized string.
    Coercing on both sides lets `_material_field_equal` compare
    structurally (order-insensitive for dicts) instead of textually
    (order-sensitive)."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except (TypeError, ValueError):
            return value
    return value


def _material_field_equal(field: str, remote_value: Any, payload_value: Any) -> bool:
    """Compare one material column between the remote row and the
    locally-computed payload. Null-safe; type-tolerant for the
    int/float/text distinctions PostgREST may round-trip differently
    than the Python writer produced; STRUCTURAL for JSON columns so
    reordered keys / whitespace variation cannot produce a false
    PATCH."""
    if remote_value is None and payload_value is None:
        return True
    if remote_value is None or payload_value is None:
        return False
    if field in _SUMMARY_INT_FIELDS:
        try:
            return int(remote_value) == int(payload_value)
        except (TypeError, ValueError):
            return False
    if field in _SUMMARY_FLOAT_FIELDS:
        try:
            return float(remote_value) == float(payload_value)
        except (TypeError, ValueError):
            return False
    if field in _SUMMARY_JSON_FIELDS:
        # Coerce serialized-JSON strings to their structural form on
        # both sides, then compare. Python dict/list `==` is
        # order-insensitive for dicts and recursive for lists.
        return _coerce_json_value(remote_value) == _coerce_json_value(payload_value)
    return remote_value == payload_value


def _remote_row_matches_payload(
    remote_row: Mapping[str, Any], payload: Mapping[str, Any],
) -> bool:
    """True iff every material column on `remote_row` equals its
    counterpart on `payload`. `computed_at` and identity fields are not
    compared — see `_SUMMARY_MATERIAL_FIELDS`."""
    for field in _SUMMARY_MATERIAL_FIELDS:
        if not _material_field_equal(
            field, remote_row.get(field), payload.get(field),
        ):
            return False
    return True


def _diff_debug_enabled() -> bool:
    """True when the `SPORE_SUMMARY_DIFF_DEBUG` env var is set to a
    truthy value. When enabled, the writer prints a one-line diagnostic
    for the first material-field mismatch on every row that would
    trigger a PATCH — used to hunt "second sync still writes even
    though local data is unchanged" cases in production."""
    return bool(str(os.environ.get("SPORE_SUMMARY_DIFF_DEBUG") or "").strip())


def _summarize_value_for_debug(value: Any) -> str:
    """Compact repr for the diff log. Keeps output single-line and
    bounded even for large dicts."""
    text = repr(value)
    if len(text) > 160:
        text = text[:157] + "..."
    return text


def _first_material_field_diff(
    remote_row: Mapping[str, Any], payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return diagnostic info for the FIRST material field that would
    trigger a PATCH, or None if all material fields match.

    The returned dict contains raw and coerced values for both sides
    so a maintainer can see exactly why the writer chose to PATCH."""
    for field in _SUMMARY_MATERIAL_FIELDS:
        remote_value = remote_row.get(field)
        payload_value = payload.get(field)
        if _material_field_equal(field, remote_value, payload_value):
            continue

        coerced_remote: Any = remote_value
        coerced_payload: Any = payload_value
        if field in _SUMMARY_JSON_FIELDS:
            coerced_remote = _coerce_json_value(remote_value)
            coerced_payload = _coerce_json_value(payload_value)
        elif field in _SUMMARY_INT_FIELDS:
            try:
                coerced_remote = (
                    None if remote_value is None else int(remote_value)
                )
                coerced_payload = (
                    None if payload_value is None else int(payload_value)
                )
            except (TypeError, ValueError):
                pass
        elif field in _SUMMARY_FLOAT_FIELDS:
            try:
                coerced_remote = (
                    None if remote_value is None else float(remote_value)
                )
                coerced_payload = (
                    None if payload_value is None else float(payload_value)
                )
            except (TypeError, ValueError):
                pass
        return {
            "field": field,
            "remote": remote_value,
            "remote_type": type(remote_value).__name__,
            "payload": payload_value,
            "payload_type": type(payload_value).__name__,
            "coerced_remote": coerced_remote,
            "coerced_payload": coerced_payload,
        }
    return None


def _log_patch_diagnostic(
    payload: Mapping[str, Any], diff: Mapping[str, Any],
) -> None:
    """Emit the one-line PATCH diagnostic. Only called when
    `_diff_debug_enabled()` is True and a real diff was found."""
    print(
        "[spore_summary_sync] PATCH-diff "
        f"obs={payload.get('observation_id')!r} "
        f"context_hash={payload.get('context_hash')!r} "
        f"first_diff_field={diff['field']} "
        f"remote={_summarize_value_for_debug(diff['remote'])} "
        f"(remote_type={diff['remote_type']}) "
        f"payload={_summarize_value_for_debug(diff['payload'])} "
        f"(payload_type={diff['payload_type']}) "
        f"coerced_remote={_summarize_value_for_debug(diff['coerced_remote'])} "
        f"coerced_payload={_summarize_value_for_debug(diff['coerced_payload'])}",
        flush=True,
    )


def _is_missing_table_error(exc: BaseException) -> bool:
    """Return True if the exception describes the target table missing.

    Matches the string-sniffing pattern from
    ``pull_observation_identifications`` in cloud_sync.py.
    """
    message = str(exc or "").lower()
    if _SUMMARY_TABLE not in message:
        return False
    if "could not find the table" in message:
        return True
    if "does not exist" in message:
        return True
    if "pgrst205" in message:  # PostgREST: relation does not exist / undefined table
        return True
    return False


def _project_summary_payload(
    summary: Mapping[str, Any], *, user_id: str, remote_observation_id: Any,
) -> dict[str, Any]:
    """Build the exact dict sent to Supabase for one summary row.

    The remote (cloud) observation id and the sync-provided user id are
    stamped in here so the sync module remains the single place responsible
    for the local -> remote id mapping. The compute layer never sees the
    local SQLite id after this point.
    """
    payload = {key: summary.get(key) for key in _SUMMARY_UPSERT_FIELDS}
    payload["observation_id"] = remote_observation_id
    payload["user_id"] = user_id
    return payload


def sync_observation_spore_summaries(
    client: SummaryHttpClient,
    *,
    local_observation_id: int,
    remote_observation_id: Any,
    user_id: str,
    source_app_version: str | None = None,
    load_measurements: Callable[[int], list[Mapping[str, Any]]] | None = None,
    computed_at: datetime | None = None,
) -> dict[str, Any]:
    """Compute and sync summary rows for one observation.

    Returns a status dict:

        {
            "status": STATUS_SYNCED | STATUS_SKIP_TABLE_MISSING
                      | STATUS_SKIP_NO_CLOUD_ID,
            "inserted": int,
            "updated": int,
            "deleted": int,
            "total_local": int,
        }

    Errors we treat as "cloud deployment is older than this stage" — i.e.
    the target table does not yet exist — become
    ``STATUS_SKIP_TABLE_MISSING`` instead of raising. All other errors
    propagate to the caller so its existing retry/skip logic can decide.
    """
    result: dict[str, Any] = {
        "status": STATUS_SYNCED,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "deleted": 0,
        "total_local": 0,
    }

    remote_id_norm: Any
    if isinstance(remote_observation_id, (int,)):
        remote_id_norm = remote_observation_id
    else:
        remote_id_str = str(remote_observation_id or "").strip()
        if not remote_id_str:
            result["status"] = STATUS_SKIP_NO_CLOUD_ID
            return result
        remote_id_norm = remote_id_str

    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        # No credential to sync as — mirrors client-side assumptions elsewhere
        # in cloud_sync. Caller should not have reached us in this state.
        result["status"] = STATUS_SKIP_NO_CLOUD_ID
        return result

    loader = load_measurements or load_measurements_with_context
    measurements = list(loader(int(local_observation_id)))

    # Compute with the REMOTE observation id so the returned payloads are
    # ready to POST as-is (except user_id, which the loader has no way to
    # know). _project_summary_payload will also overwrite observation_id
    # as a defense-in-depth guard against a future refactor that forgets
    # to pass remote_observation_id here.
    summaries = compute_observation_spore_summaries(
        observation_id=remote_id_norm,
        measurements=measurements,
        computed_at=computed_at,
        source_app_version=source_app_version,
    )
    result["total_local"] = len(summaries)

    filter_query = (
        f"?observation_id=eq.{remote_id_norm}"
        f"&user_id=eq.{normalized_user_id}"
        f"&select={_SUMMARY_SELECT_COLUMNS}"
    )
    try:
        existing_rows = client._get(f"{_SUMMARY_TABLE}{filter_query}")
    except Exception as exc:
        if _is_missing_table_error(exc):
            result["status"] = STATUS_SKIP_TABLE_MISSING
            return result
        raise

    # Full remote rows keyed by context_hash so we can compare every
    # material column (not just id) before deciding to PATCH. The prior
    # revision keyed by hash -> id and PATCHed unconditionally on hash
    # match, which meant `computed_at` (a fresh timestamp on every
    # compute) always moved and the Stage B updated_at trigger fired —
    # neither is truly idempotent even when nothing about the data
    # changed. Compare full material fields here to skip identical rows.
    existing_by_hash: dict[str, dict[str, Any]] = {}
    for row in existing_rows or []:
        row_hash = str((row or {}).get("context_hash") or "").strip()
        row_id = (row or {}).get("id")
        if row_hash and row_id is not None:
            existing_by_hash[row_hash] = dict(row)

    # Upsert each computed summary. Patch-if-materially-different,
    # insert-if-missing. Fully-matching rows are left untouched so
    # `computed_at` and `updated_at` remain stable across no-op syncs.
    for summary in summaries:
        payload = _project_summary_payload(
            summary,
            user_id=normalized_user_id,
            remote_observation_id=remote_id_norm,
        )
        context_hash = payload["context_hash"]
        existing_row = existing_by_hash.pop(context_hash, None)
        try:
            if existing_row is not None:
                if _remote_row_matches_payload(existing_row, payload):
                    result["unchanged"] += 1
                else:
                    if _diff_debug_enabled():
                        diff = _first_material_field_diff(existing_row, payload)
                        if diff is not None:
                            _log_patch_diagnostic(payload, diff)
                    client._patch(
                        f"{_SUMMARY_TABLE}?id=eq.{existing_row['id']}",
                        payload,
                    )
                    result["updated"] += 1
            else:
                client._post(_SUMMARY_TABLE, payload)
                result["inserted"] += 1
        except Exception as exc:
            if _is_missing_table_error(exc):
                result["status"] = STATUS_SKIP_TABLE_MISSING
                return result
            raise

    # Delete stale remote rows: any context_hash still in existing_by_hash
    # is a row that used to exist locally but has been recomputed away
    # (e.g. because the user changed an image's mount_medium so the two
    # groups merged, or because all measurements in that context were
    # deleted). When ``summaries`` is empty this also correctly wipes
    # every remote row for the observation.
    for stale_row in existing_by_hash.values():
        stale_id = stale_row.get("id")
        if stale_id is None:
            continue
        try:
            client._delete(f"{_SUMMARY_TABLE}?id=eq.{stale_id}")
            result["deleted"] += 1
        except Exception as exc:
            if _is_missing_table_error(exc):
                result["status"] = STATUS_SKIP_TABLE_MISSING
                return result
            raise

    return result
