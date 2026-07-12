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
        f"&select=id,context_hash"
    )
    try:
        existing_rows = client._get(f"{_SUMMARY_TABLE}{filter_query}")
    except Exception as exc:
        if _is_missing_table_error(exc):
            result["status"] = STATUS_SKIP_TABLE_MISSING
            return result
        raise

    existing_by_hash: dict[str, Any] = {}
    for row in existing_rows or []:
        row_hash = str((row or {}).get("context_hash") or "").strip()
        row_id = (row or {}).get("id")
        if row_hash and row_id is not None:
            existing_by_hash[row_hash] = row_id

    # Upsert each computed summary. Patch-if-exists, insert-otherwise.
    # This mirrors the pattern used by _push_spore_mosaic_for_observation
    # rather than PostgREST's on_conflict header — check-then-patch reads
    # the same, works with the client's existing HTTP primitives, and is
    # safe under single-user desktop concurrency.
    for summary in summaries:
        payload = _project_summary_payload(
            summary,
            user_id=normalized_user_id,
            remote_observation_id=remote_id_norm,
        )
        context_hash = payload["context_hash"]
        existing_id = existing_by_hash.pop(context_hash, None)
        try:
            if existing_id is not None:
                client._patch(
                    f"{_SUMMARY_TABLE}?id=eq.{existing_id}", payload,
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
    for stale_id in existing_by_hash.values():
        try:
            client._delete(f"{_SUMMARY_TABLE}?id=eq.{stale_id}")
            result["deleted"] += 1
        except Exception as exc:
            if _is_missing_table_error(exc):
                result["status"] = STATUS_SKIP_TABLE_MISSING
                return result
            raise

    return result
