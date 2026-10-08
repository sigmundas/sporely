"""Spore-mosaic glue around ``utils.cloud_spore_mosaic``: status codes, gate
diagnosis, per-observation mosaic push and the public mosaic backfill.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from database.schema import get_connection, get_images_dir

from utils.cloud_sync_impl.anchors import (
    _ensure_metadata_only_microscope_images_for_observation,
)
from utils.cloud_sync_impl.common import _normalize_cloud_media_key
from utils.cloud_sync_impl.errors import (
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.measurements import _push_measurements_for_observation
from utils.cloud_sync_impl.mosaic_signature import (
    _clear_local_mosaic_signature,
    _load_local_mosaic_signature,
    _load_spore_mosaic_eligible_rows,
    _local_spore_mosaic_signature,
    _store_local_mosaic_signature,
)


# Public status codes returned by `_push_spore_mosaic_for_observation`. Kept
# as short kebab-cased strings so callers can aggregate them into counters
# and log them verbatim. Auth / temporary errors are NOT translated to a
# code — they propagate as exceptions so the caller can abort cleanly.
#
# Skip-status differentiation (Phase 2.D): previously every "nothing to
# upload" outcome collapsed into MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES
# regardless of whether the source files were absent, calibration was
# missing, geometry was invalid, or the render loop failed on every
# tile. Each has a different remediation, so callers now branch on:
#
#   * SKIP_MISSING_SOURCE_IMAGES — every eligible item's source file
#     was absent from the local media directory.
#   * SKIP_MISSING_CALIBRATION   — every item lacked both a per-image
#     µm/px calibration and a stored length_um the planner could use.
#   * SKIP_INVALID_GEOMETRY      — every item had degenerate source
#     dimensions or a degenerate p1..p4 axis.
#   * SKIP_NO_USABLE_SOURCES     — the batch was skipped for mixed or
#     otherwise-uncategorised reasons; keeps the generic bucket alive
#     for backfills of pre-instrumented rows.
#   * SKIP_RENDER_FAILURE        — the planner produced tiles but the
#     render loop raised on every one of them.
MOSAIC_STATUS_GENERATED = 'generated'


MOSAIC_STATUS_SKIP_UNCHANGED = 'skip_unchanged'


MOSAIC_STATUS_SKIP_NO_OBSERVATION = 'skip_no_observation'


MOSAIC_STATUS_SKIP_NO_PUBLIC_SPORE_DATA = 'skip_no_public_spore_data'


MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS = 'skip_no_eligible_measurements'


MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES = 'skip_missing_source_images'


MOSAIC_STATUS_SKIP_MISSING_CALIBRATION = 'skip_missing_calibration'


MOSAIC_STATUS_SKIP_INVALID_GEOMETRY = 'skip_invalid_geometry'


MOSAIC_STATUS_SKIP_RENDER_FAILURE = 'skip_render_failure'


MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES = 'skip_no_usable_sources'


MOSAIC_STATUS_FAIL_BUILD = 'fail_build'


MOSAIC_STATUS_FAIL_UPLOAD = 'fail_upload'


MOSAIC_STATUS_FAIL_MOSAIC_LOOKUP = 'fail_mosaic_lookup'


MOSAIC_STATUS_FAIL_MOSAIC_UPSERT = 'fail_mosaic_upsert'


MOSAIC_STATUS_FAIL_NO_MOSAIC_ID = 'fail_no_mosaic_id'


MOSAIC_STATUS_FAIL_TILE_CLEANUP = 'fail_tile_cleanup'


MOSAIC_STATUS_FAIL_TILE_INSERT = 'fail_tile_insert'


def _classify_mosaic_build_skips(
    skipped: list[tuple[int, str]],
    *,
    include_all_missing_source: bool = True,
) -> str:
    """Bucket the per-item skip reasons from `build_spore_mosaic` into
    one of the ``MOSAIC_STATUS_SKIP_*`` failure codes.

    A batch with a uniform reason maps to a specific code; mixed reasons
    map to ``MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES``. When
    ``include_all_missing_source`` is False (callers who already handled
    the ``sources_from_measurement_rows`` prefilter separately), the
    "source image missing" bucket falls through to the generic code
    instead of hiding actual planner failures.
    """
    if not skipped:
        return MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES
    reasons = {reason for _mid, reason in skipped}
    if (
        include_all_missing_source
        and reasons and all(r == 'source image missing' for r in reasons)
    ):
        return MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES
    if reasons and all(r == 'missing_calibration' for r in reasons):
        return MOSAIC_STATUS_SKIP_MISSING_CALIBRATION
    if reasons and all(r == 'invalid source dims' for r in reasons):
        return MOSAIC_STATUS_SKIP_INVALID_GEOMETRY
    if any(str(r).startswith('render failed:') for r in reasons):
        return MOSAIC_STATUS_SKIP_RENDER_FAILURE
    return MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES


def _remote_mosaic_row_exists(
    client: 'SporelyCloudClient',
    obs_cloud_id: str,
    version: int,
) -> bool:
    """Return True iff a `spore_measurement_mosaics` row exists for the
    observation + pipeline version and (best-effort) at least one tile.

    On lookup failure we return False so the caller rebuilds — better a
    redundant rebuild than a silent skip that hides a wiped mosaic.
    """
    obs = str(obs_cloud_id or '').strip()
    if not obs or version < 1:
        return False
    try:
        rows = client._get(
            f'spore_measurement_mosaics'
            f'?observation_id=eq.{obs}'
            f'&version=eq.{int(version)}'
            f'&user_id=eq.{client.user_id}'
            f'&select=id,storage_key&limit=1'
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        return False
    if not rows:
        return False
    mosaic_row = rows[0] if isinstance(rows, list) else rows
    mosaic_id = str((mosaic_row or {}).get('id') or '').strip()
    storage_key = str((mosaic_row or {}).get('storage_key') or '').strip()
    if not mosaic_id or not storage_key:
        return False
    # Cheap tile-existence probe. If we can't read tiles (e.g. RLS quirk)
    # we still trust the mosaic row's presence — the caller can rebuild
    # the manifest later if it turns out to be missing.
    try:
        tiles = client._get(
            f'spore_measurement_mosaic_tiles'
            f'?mosaic_id=eq.{mosaic_id}'
            f'&select=measurement_id&limit=1'
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        return True
    return bool(tiles)


def diagnose_public_spore_mosaic_gates(
    client: 'SporelyCloudClient | None',
    obs_local_id: int,
    obs_cloud_id: str | None = None,
    *,
    include_remote: bool = True,
    log: bool = True,
) -> dict:
    """Report why the mosaic pusher would (or would not) include each spore.

    Counts measurements at every gate between "row exists locally" and
    "spore point is served by `get_public_observation`" so an operator
    can see, at a glance, which predicate is dropping measurements.
    Local counts are always populated. Remote counts (Supabase +
    `get_public_observation`) are best-effort; on failure the error
    string lands in the result under `*_error` keys and the local counts
    are still returned. Pass `client=None` to skip the remote step
    entirely.

    Returns a dict with these keys:
      obs_local_id, obs_cloud_id,
      total_local,
      with_p1_p2,
      with_p1_p2_p3_p4,
      with_length_and_width_um,
      image_has_cloud_id,
      measurement_has_cloud_id,
      excluded_by_measurement_type,
      by_image_type   -> {image_type: count},
      pusher_would_select,
      remote_images (optional),
      remote_microscope_images (optional),
      remote_measurements (optional),
      public_rpc_sporePoints (optional).
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()

        def _count(sql: str, params: tuple = ()) -> int:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0

        total_local = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ?',
            (obs_local_id,),
        )
        with_p1p2 = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? '
            '  AND m.p1_x IS NOT NULL AND m.p1_y IS NOT NULL '
            '  AND m.p2_x IS NOT NULL AND m.p2_y IS NOT NULL',
            (obs_local_id,),
        )
        with_p1234 = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? '
            '  AND m.p1_x IS NOT NULL AND m.p1_y IS NOT NULL '
            '  AND m.p2_x IS NOT NULL AND m.p2_y IS NOT NULL '
            '  AND m.p3_x IS NOT NULL AND m.p3_y IS NOT NULL '
            '  AND m.p4_x IS NOT NULL AND m.p4_y IS NOT NULL',
            (obs_local_id,),
        )
        with_um = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? '
            '  AND m.length_um IS NOT NULL AND m.width_um IS NOT NULL',
            (obs_local_id,),
        )
        image_has_cloud_id = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? AND i.cloud_id IS NOT NULL',
            (obs_local_id,),
        )
        meas_has_cloud_id = _count(
            'SELECT COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? AND m.cloud_id IS NOT NULL',
            (obs_local_id,),
        )
        excluded_by_type = _count(
            "SELECT COUNT(*) FROM spore_measurements m "
            "JOIN images i ON i.id = m.image_id "
            "WHERE i.observation_id = ? "
            "  AND m.measurement_type IS NOT NULL "
            "  AND m.measurement_type != '' "
            "  AND lower(m.measurement_type) NOT IN ('manual', 'spore', 'spores')",
            (obs_local_id,),
        )
        pusher_selected = _count(
            "SELECT COUNT(*) "
            "FROM spore_measurements m "
            "JOIN images i ON i.id = m.image_id "
            "WHERE i.observation_id = ? "
            "  AND i.image_type = 'microscope' "
            "  AND i.cloud_id IS NOT NULL "
            "  AND m.cloud_id IS NOT NULL "
            "  AND m.length_um IS NOT NULL AND m.width_um IS NOT NULL "
            "  AND m.p1_x IS NOT NULL AND m.p1_y IS NOT NULL "
            "  AND m.p2_x IS NOT NULL AND m.p2_y IS NOT NULL "
            "  AND ("
            "    m.measurement_type IS NULL"
            "    OR m.measurement_type = ''"
            "    OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')"
            "  )",
            (obs_local_id,),
        )
        cursor.execute(
            'SELECT i.image_type, COUNT(*) FROM spore_measurements m '
            'JOIN images i ON i.id = m.image_id '
            'WHERE i.observation_id = ? GROUP BY i.image_type',
            (obs_local_id,),
        )
        by_image_type = {
            str(row[0] or 'NULL'): int(row[1]) for row in cursor.fetchall()
        }
    finally:
        conn.close()

    result: dict = {
        'obs_local_id': obs_local_id,
        'obs_cloud_id': obs_cloud_id,
        'total_local': total_local,
        'with_p1_p2': with_p1p2,
        'with_p1_p2_p3_p4': with_p1234,
        'with_length_and_width_um': with_um,
        'image_has_cloud_id': image_has_cloud_id,
        'measurement_has_cloud_id': meas_has_cloud_id,
        'excluded_by_measurement_type': excluded_by_type,
        'by_image_type': by_image_type,
        'pusher_would_select': pusher_selected,
    }

    obs_cloud_id_str = str(obs_cloud_id or '').strip()
    if include_remote and client is not None and obs_cloud_id_str:
        try:
            img_rows = client._get(
                'observation_images'
                f'?observation_id=eq.{obs_cloud_id_str}'
                f'&user_id=eq.{client.user_id}'
                '&select=id,image_type,deleted_at,purged_at&limit=1000'
            )
        except Exception as exc:
            result['remote_images_error'] = str(exc)
            img_rows = []
        result['remote_images'] = len(img_rows)
        microscope_ids: list[str] = []
        for row in img_rows:
            if row.get('deleted_at') or row.get('purged_at'):
                continue
            if row.get('image_type') != 'microscope':
                continue
            image_id = str(row.get('id') or '').strip()
            if image_id:
                microscope_ids.append(image_id)
        result['remote_microscope_images'] = len(microscope_ids)

        if microscope_ids:
            try:
                ids_in = ','.join(microscope_ids)
                m_rows = client._get(
                    'spore_measurements'
                    f'?image_id=in.({ids_in})'
                    f'&user_id=eq.{client.user_id}'
                    '&select=id,measurement_type&limit=2000'
                )
                result['remote_measurements'] = len(m_rows)
            except Exception as exc:
                result['remote_measurements_error'] = str(exc)
        else:
            result['remote_measurements'] = 0

        try:
            rpc_result = client._rpc(
                'get_public_observation',
                {'p_observation_id': int(obs_cloud_id_str)},
            )
            row = None
            if isinstance(rpc_result, list) and rpc_result:
                row = rpc_result[0]
            elif isinstance(rpc_result, dict):
                row = rpc_result
            spore_points = row.get('sporePoints') if isinstance(row, dict) else None
            result['public_rpc_sporePoints'] = (
                len(spore_points) if isinstance(spore_points, list) else 0
            )
        except Exception as exc:
            result['public_rpc_error'] = str(exc)

    if log:
        prefix = f'[cloud_sync] Mosaic gate obs {obs_local_id} cloud={obs_cloud_id_str or "?"}:'
        print(f'{prefix} total_local={result["total_local"]}', flush=True)
        print(
            f'{prefix}   with_p1_p2={result["with_p1_p2"]} '
            f'with_p1_p2_p3_p4={result["with_p1_p2_p3_p4"]} '
            f'with_length_and_width_um={result["with_length_and_width_um"]}',
            flush=True,
        )
        print(
            f'{prefix}   image_has_cloud_id={result["image_has_cloud_id"]} '
            f'measurement_has_cloud_id={result["measurement_has_cloud_id"]} '
            f'excluded_by_measurement_type={result["excluded_by_measurement_type"]}',
            flush=True,
        )
        print(f'{prefix}   by_image_type={result["by_image_type"]}', flush=True)
        print(f'{prefix}   pusher_would_select={result["pusher_would_select"]}', flush=True)
        if 'remote_images' in result:
            print(
                f'{prefix}   remote_images={result["remote_images"]} '
                f'remote_microscope_images={result["remote_microscope_images"]} '
                f'remote_measurements={result.get("remote_measurements", "?")}',
                flush=True,
            )
        if 'public_rpc_sporePoints' in result:
            print(
                f'{prefix}   public_rpc_sporePoints={result["public_rpc_sporePoints"]}',
                flush=True,
            )
        for key in ('remote_images_error', 'remote_measurements_error', 'public_rpc_error'):
            if key in result:
                print(f'{prefix}   {key}={result[key]!r}', flush=True)

    return result


def _push_spore_mosaic_for_observation(
    client: 'SporelyCloudClient',
    obs_local_id: int,
    obs_cloud_id: str,
    *,
    status_cb: Callable[[str], None] | None = None,
) -> str:
    """Generate + upload one public spore mosaic (atlas + tile manifest).

    Best-effort: for anything other than an auth / temporary-unavailable
    error, the function logs a `[cloud_sync] Mosaic …` line and returns a
    short status string (see `MOSAIC_STATUS_*` constants) instead of
    raising, so a mosaic problem never breaks the rest of an observation
    sync. Auth / temporary errors DO propagate so a stale token or 503
    aborts the caller cleanly instead of quietly turning every remaining
    observation into a `fail_*` line.

    Runs only when the observation has `spore_data_visibility='public'`
    and at least one microscope measurement already has a cloud id —
    matching the visibility surface of the public observation RPC. The
    per-measurement `thumb_key` / `cropUrl` fallback on the public RPC
    remains authoritative when this step is skipped or fails.

    Progress
    --------
    ``status_cb(message)`` fires once per visible stage transition
    (planning, rendering, encoding, uploading, saving metadata). The
    caller is expected to bind the outer sync progress hook to it —
    e.g. ``lambda msg: _emit_progress(progress_cb, msg, state)`` — so
    the desktop UI surfaces per-observation mosaic progress without
    the mosaic module having to know about ``ProgressCallback`` or the
    outer progress state.  Optional: leaving it ``None`` keeps behaviour
    at the pre-Phase-2.E log-only surface.
    """
    def _status(message: str) -> None:
        if status_cb is None:
            return
        try:
            status_cb(message)
        except Exception:
            # Progress callbacks must never break a mosaic sync.
            pass

    if not obs_cloud_id:
        return MOSAIC_STATUS_SKIP_NO_OBSERVATION

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT spore_data_visibility FROM observations WHERE id = ?',
            (obs_local_id,),
        )
        obs_row = cursor.fetchone()
        if obs_row is None:
            return MOSAIC_STATUS_SKIP_NO_OBSERVATION
        observation_row = dict(obs_row)
        visibility = str(observation_row.get('spore_data_visibility') or 'public').strip().lower()
        if visibility != 'public':
            print(
                f'[cloud_sync] Mosaic skip obs {obs_local_id}: '
                f'spore_data_visibility={visibility!r}',
                flush=True,
            )
            return MOSAIC_STATUS_SKIP_NO_PUBLIC_SPORE_DATA

        rows = _load_spore_mosaic_eligible_rows(cursor, obs_local_id)
    finally:
        conn.close()

    if not rows:
        print(
            f'[cloud_sync] Mosaic skip obs {obs_local_id}: no eligible public measurements',
            flush=True,
        )
        return MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS

    # Local import to keep the top-level cloud_sync import graph unchanged
    # for tests that stub PIL or the mosaic module.
    from utils.cloud_spore_mosaic import (
        DEFAULT_TILE_SIZE_PX,
        MOSAIC_PIPELINE_VERSION,
        MOSAIC_PROGRESS_COMPLETE,
        MOSAIC_PROGRESS_DIGEST,
        MOSAIC_PROGRESS_ENCODING,
        MOSAIC_PROGRESS_PLANNING,
        MOSAIC_PROGRESS_RENDERING,
        build_spore_mosaic,
        build_storage_key,
        compute_content_digest,
        sources_from_measurement_rows,
    )

    # Stage timings — each block below stamps a monotonic delta into
    # `stage_ns` so the aggregate line at the end reads like:
    #   {signature_ms, remote_check_ms, build_ms, upload_ms,
    #    mosaic_row_ms, tile_rows_ms, local_signature_ms, total_ms}.
    # `build_ms` is the local CPU/I/O cost inside `build_spore_mosaic`;
    # `upload_ms` is the R2 / media-worker network cost. The two are
    # deliberately named so an operator glancing at the log can tell
    # whether a slow observation is spending time in the pipeline or
    # over the wire.
    push_start_ns = time.monotonic_ns()
    stage_ns: dict[str, int] = {
        'signature_ms': 0, 'remote_check_ms': 0, 'build_ms': 0,
        'upload_ms': 0, 'mosaic_row_ms': 0, 'tile_rows_ms': 0,
        'local_signature_ms': 0,
    }

    # Cheap change-detection guard. If nothing that determines the mosaic
    # bytes or tile manifest has changed AND a valid remote mosaic row
    # still exists, we can skip Pillow / WebP / R2 / tile-rewrite entirely.
    # Missing remote row → rebuild even when the local signature matches,
    # so a wiped mosaic (or a fresh pipeline version) always recovers.
    signature_start_ns = time.monotonic_ns()
    new_signature = _local_spore_mosaic_signature(obs_local_id, rows, observation_row)
    stored_signature = _load_local_mosaic_signature(obs_local_id)
    stage_ns['signature_ms'] = time.monotonic_ns() - signature_start_ns
    if new_signature and stored_signature and new_signature == stored_signature:
        remote_check_start_ns = time.monotonic_ns()
        try:
            remote_ok = _remote_mosaic_row_exists(
                client, obs_cloud_id, MOSAIC_PIPELINE_VERSION,
            )
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            remote_ok = False
        stage_ns['remote_check_ms'] = time.monotonic_ns() - remote_check_start_ns
        if remote_ok:
            print(
                f'[cloud_sync] Mosaic skip obs {obs_local_id}: '
                f'signature unchanged',
                flush=True,
            )
            return MOSAIC_STATUS_SKIP_UNCHANGED
        print(
            f'[cloud_sync] Mosaic rebuild obs {obs_local_id}: '
            f'signature unchanged but remote mosaic row missing',
            flush=True,
        )

    print(
        f'[cloud_sync] Mosaic start obs {obs_local_id}: measurements={len(rows)}',
        flush=True,
    )
    sources, source_skipped = sources_from_measurement_rows(
        rows,
        image_dir=get_images_dir(),
    )
    for mid, reason in source_skipped:
        print(f'[cloud_sync]   Mosaic source skip m={mid}: {reason}', flush=True)

    if not sources:
        # Distinguish "we couldn't open any file" (fixable by resyncing
        # media) from "the source rows themselves were malformed".
        code = _classify_mosaic_build_skips(source_skipped)
        print(
            f'[cloud_sync] Mosaic abort obs {obs_local_id}: no usable sources ({code})',
            flush=True,
        )
        return code

    # Progress hook: emit human-readable stage transitions on both the
    # print log AND the optional outer status callback. The message set
    # matches the desktop UI's "Rendering spore mosaic k/N…" progression
    # so operators see meaningful transitions during a long build.
    # Deterministic guarantee: the callback body never depends on the
    # wall clock, and every branch is a no-op if the flag is False.
    def _mosaic_progress(stage: str, current: int, total: int) -> None:
        if stage == MOSAIC_PROGRESS_PLANNING and current == 0:
            message = "Planning spore mosaic…"
        elif stage == MOSAIC_PROGRESS_RENDERING:
            if total > 0:
                message = f"Rendering spore mosaic {current}/{total}…"
            else:
                message = "Rendering spore mosaic…"
        elif stage == MOSAIC_PROGRESS_ENCODING and current == 0:
            message = "Encoding spore mosaic…"
        elif stage == MOSAIC_PROGRESS_DIGEST and current == 0:
            # Digest is fast; keep the UI on the encoding message
            # rather than flashing "digest" for a millisecond.
            return
        elif stage == MOSAIC_PROGRESS_COMPLETE:
            # Terminal state; caller emits a specific "Uploading…"
            # message next, so no UI-visible transition here.
            return
        else:
            return
        if current in (0, total) or stage == MOSAIC_PROGRESS_RENDERING:
            print(
                f'[cloud_sync] Mosaic status obs {obs_local_id}: {message}',
                flush=True,
            )
        _status(message)

    build_start_ns = time.monotonic_ns()
    try:
        build_result = build_spore_mosaic(
            sources,
            tile_size_px=DEFAULT_TILE_SIZE_PX,
            progress_cb=_mosaic_progress,
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic build failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_BUILD
    stage_ns['build_ms'] = time.monotonic_ns() - build_start_ns

    manifest = build_result.manifest
    if manifest is None or not manifest.tiles:
        # Distinguish the planner-level "all skipped" categories from
        # the render-level "no tiles rendered" so the operator sees a
        # specific remediation string.  Mixed skips fall through to the
        # generic NO_USABLE_SOURCES bucket rather than MISSING_SOURCE_IMAGES.
        aggregate_skips: list[tuple[int, str]] = list(build_result.skipped)
        aggregate_reason = build_result.reason
        if aggregate_reason == 'no_input':
            code = MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS
        elif aggregate_reason == 'no_tiles_rendered':
            code = _classify_mosaic_build_skips(aggregate_skips)
            # Render-loop failures with mixed non-render reasons still
            # need the RENDER_FAILURE code — the classifier already
            # prefers that bucket when any 'render failed:' entry
            # exists, but if none exist we default to
            # NO_USABLE_SOURCES since something else went wrong.
        else:
            code = _classify_mosaic_build_skips(aggregate_skips)
        for mid, reason in aggregate_skips:
            print(
                f'[cloud_sync]   Mosaic aggregate skip m={mid}: {reason}',
                flush=True,
            )
        print(
            (
                f'[cloud_sync] Mosaic empty obs {obs_local_id}: '
                f'nothing to upload (reason={aggregate_reason!r}, code={code})'
            ),
            flush=True,
        )
        return code

    for mid, reason in manifest.skipped:
        print(f'[cloud_sync]   Mosaic tile skip m={mid}: {reason}', flush=True)

    timings = getattr(manifest, "timings", None)
    if timings is not None:
        try:
            summary = timings.summary()
        except Exception:
            summary = None
        if summary is not None:
            print(
                f'[cloud_sync] Mosaic timings obs {obs_local_id}: {summary}',
                flush=True,
            )

    print(
        (
            f'[cloud_sync] Mosaic physical crop plan obs {obs_local_id}: '
            f'common_crop_um=({manifest.common_crop_width_um:.3f},{manifest.common_crop_height_um:.3f}) '
            f'output_tile=({manifest.tile_width_px},{manifest.tile_height_px})'
        ),
        flush=True,
    )

    # Diagnostic log for the first few tiles so we can see, at a glance,
    # whether the polygon geometry landed for a given observation. Useful
    # when a backfill run "succeeds" but the landing tile still looks
    # empty — the log makes it obvious whether the pipeline is emitting
    # polygon overlays or falling back to bare tiles.
    _DIAG_TILE_LIMIT = 3
    with_polygon = sum(
        1 for tile in manifest.tiles if tile.overlay_json is not None
    )
    print(
        (
            f'[cloud_sync] Mosaic diag obs {obs_local_id}: '
            f'tiles={len(manifest.tiles)} with_polygon={with_polygon}'
        ),
        flush=True,
    )
    for tile in manifest.tiles[:_DIAG_TILE_LIMIT]:
        diag = tile.diagnostics or {}
        print(
            (
                f'[cloud_sync]   Mosaic diag m={tile.measurement_id} '
                f'p3={diag.get("have_p3")} p4={diag.get("have_p4")} '
                f'gallery_rot={diag.get("gallery_rotation_deg")} '
                f'rot={diag.get("rotation_deg")} '
                f'L_um={diag.get("length_um")} W_um={diag.get("width_um")} '
                f'L_axis_px={diag.get("length_axis_px")} '
                f'W_axis_px={diag.get("width_axis_px")} '
                f'L_pxpum={diag.get("length_axis_px_per_um")} '
                f'W_pxpum={diag.get("width_axis_px_per_um")} '
                f'fallback={diag.get("scale_fallback_reason")} '
                f'natural_um={diag.get("natural_crop_um")} '
                f'common_um={diag.get("common_crop_um")} '
                f'crop_px={diag.get("crop_px")} '
                f'crop_after={diag.get("crop_rect_after_shift")} '
                f'padded=({diag.get("padded_x")},{diag.get("padded_y")}) '
                f'tile=({tile.w_px},{tile.h_px}) '
                f'polygon={diag.get("polygon_present")} '
                f'reason={diag.get("reason_no_polygon")} '
                f'poly_bounds={diag.get("polygon_bounds")}'
            ),
            flush=True,
        )

    version = int(MOSAIC_PIPELINE_VERSION)
    # Content-address the storage key so `Cache-Control: immutable` is safe:
    # the URL changes on every byte change, so browsers/CDNs never return
    # stale mosaic bytes for an updated observation. The DB version stays
    # 1 — only the storage key rotates.
    content_digest = compute_content_digest(manifest.image_bytes)
    storage_key = build_storage_key(client.user_id, obs_cloud_id, version, content_digest)
    cache_control = 'public, max-age=31536000, immutable'
    upload_meta = {
        'user_id': client.user_id,
        'uploaded_at': datetime.now(timezone.utc).isoformat(),
        'uploaded_by': client.user_id,
        'upload_mode': 'full',
        'upload_variant': 'spore_mosaic',
        'stored_width': str(manifest.width_px),
        'stored_height': str(manifest.height_px),
        'stored_bytes': str(len(manifest.image_bytes)),
    }

    mosaic_payload: dict = {
        'observation_id': obs_cloud_id,
        'user_id': client.user_id,
        'storage_key': storage_key,
        'width_px': manifest.width_px,
        'height_px': manifest.height_px,
        'tile_size_px': manifest.tile_size_px,
        'version': version,
        'tile_width_px': (
            int(manifest.tile_width_px) if manifest.tile_width_px > 0 else None
        ),
        'tile_height_px': (
            int(manifest.tile_height_px) if manifest.tile_height_px > 0 else None
        ),
        'common_crop_width_um': (
            float(manifest.common_crop_width_um)
            if manifest.common_crop_width_um > 0 else None
        ),
        'common_crop_height_um': (
            float(manifest.common_crop_height_um)
            if manifest.common_crop_height_um > 0 else None
        ),
    }

    # Reserve a server-known mosaic identity before a private Worker write.
    # Existing replacements keep their old key until the new object is
    # durable; the Worker validates the new content-addressed key against the
    # same owned observation directory.
    mosaic_row_start_ns = time.monotonic_ns()
    try:
        existing = client._get(
            f'spore_measurement_mosaics'
            f'?observation_id=eq.{obs_cloud_id}'
            f'&version=eq.{version}'
            f'&user_id=eq.{client.user_id}'
            f'&select=id,storage_key'
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic lookup failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_MOSAIC_LOOKUP

    previous_storage_key = ''
    mosaic_row_reserved = False
    try:
        if existing:
            mosaic_id = str(existing[0]['id'])
            previous_storage_key = _normalize_cloud_media_key(existing[0].get('storage_key'))
        else:
            rows_ret = client._post('spore_measurement_mosaics', mosaic_payload)
            mosaic_id = str(rows_ret[0]['id']) if rows_ret else ''
            mosaic_row_reserved = bool(mosaic_id)
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic identity reservation failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_MOSAIC_UPSERT
    if not mosaic_id:
        print(
            f'[cloud_sync] Mosaic identity reservation returned no id obs {obs_local_id}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_NO_MOSAIC_ID

    _status("Uploading spore mosaic…")
    print(
        f'[cloud_sync] Mosaic status obs {obs_local_id}: Uploading spore mosaic…',
        flush=True,
    )
    upload_start_ns = time.monotonic_ns()
    try:
        worker = client._get_media_worker()
        response = worker.put_bytes(
            manifest.image_bytes,
            storage_key,
            content_type=manifest.content_type,
            cache_control=cache_control,
            timeout=120,
            upload_meta=upload_meta,
            options={
                'mosaicId': mosaic_id,
                'uploadMode': 'full',
                'uploadVariant': 'spore_mosaic',
                'sourceWidth': manifest.width_px,
                'sourceHeight': manifest.height_px,
                'storedWidth': manifest.width_px,
                'storedHeight': manifest.height_px,
            },
        )
        confirmed = _normalize_cloud_media_key(
            str((response or {}).get('key') or storage_key)
        )
        if confirmed:
            storage_key = confirmed
    except Exception as exc:
        if mosaic_row_reserved:
            try:
                client._storage_remove([storage_key])
            finally:
                client._delete(
                    f'spore_measurement_mosaics?id=eq.{mosaic_id}'
                    f'&user_id=eq.{client.user_id}'
                )
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic upload failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_UPLOAD
    stage_ns['upload_ms'] = time.monotonic_ns() - upload_start_ns

    _status("Saving spore mosaic metadata…")
    print(
        f'[cloud_sync] Mosaic status obs {obs_local_id}: Saving spore mosaic metadata…',
        flush=True,
    )
    try:
        if existing:
            patch_payload = dict(mosaic_payload)
            patch_payload['updated_at'] = datetime.now(timezone.utc).isoformat()
            client._patch(f'spore_measurement_mosaics?id=eq.{mosaic_id}', patch_payload)
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic upsert failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_MOSAIC_UPSERT
    stage_ns['mosaic_row_ms'] = time.monotonic_ns() - mosaic_row_start_ns

    tile_rows_start_ns = time.monotonic_ns()
    # Refresh tile manifest: DELETE any existing tiles for the
    # measurement ids we're about to insert, then bulk INSERT.
    #
    # We filter by `measurement_id` (the tile table PK) rather than
    # by `mosaic_id` so a version bump correctly clears the previous
    # version's tiles: on the v1 → v2 transition the freshly-upserted
    # v2 mosaic row has no tiles yet, so filtering by mosaic_id would
    # be a no-op and leave the v1 tiles in place, which then collide
    # on INSERT because the PK is (measurement_id) and unique across
    # all mosaic versions.
    tile_measurement_ids = [
        str(tile.cloud_measurement_id).strip()
        for tile in manifest.tiles
        if str(tile.cloud_measurement_id or '').strip()
    ]
    if tile_measurement_ids:
        # PostgREST `in.(a,b,c)` filter. Bigint ids don't need quoting.
        # Batch in chunks to keep the URL under typical proxy limits
        # even for observations with hundreds of measurements.
        try:
            _BATCH = 200
            for i in range(0, len(tile_measurement_ids), _BATCH):
                chunk = tile_measurement_ids[i : i + _BATCH]
                ids_expr = ','.join(chunk)
                client._delete(
                    f'spore_measurement_mosaic_tiles?measurement_id=in.({ids_expr})'
                )
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            print(
                f'[cloud_sync] Mosaic tile cleanup failed obs {obs_local_id}: {exc}',
                flush=True,
            )
            return MOSAIC_STATUS_FAIL_TILE_CLEANUP

    tile_payload = [
        {
            'measurement_id': tile.cloud_measurement_id,
            'mosaic_id': mosaic_id,
            'x_px': tile.x_px,
            'y_px': tile.y_px,
            'w_px': tile.w_px,
            'h_px': tile.h_px,
            'overlay_json': tile.overlay_json,
        }
        for tile in manifest.tiles
    ]

    try:
        client._post('spore_measurement_mosaic_tiles', tile_payload)  # bulk insert
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic tile insert failed obs {obs_local_id}: {exc}',
            flush=True,
        )
        return MOSAIC_STATUS_FAIL_TILE_INSERT
    stage_ns['tile_rows_ms'] = time.monotonic_ns() - tile_rows_start_ns

    if previous_storage_key and previous_storage_key != storage_key:
        try:
            client._storage_remove([previous_storage_key])
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            print(
                f'[cloud_sync] Superseded mosaic cleanup failed obs {obs_local_id}: {exc}',
                flush=True,
            )
            return MOSAIC_STATUS_FAIL_MOSAIC_UPSERT

    # Only persist the signature once tile rewrite completes cleanly.
    # A partial success (upload OK but tile insert failed) leaves the
    # cache untouched so the next sync retries the rebuild.
    local_signature_start_ns = time.monotonic_ns()
    if new_signature:
        try:
            _store_local_mosaic_signature(obs_local_id, new_signature)
        except Exception as exc:  # pragma: no cover
            print(
                f'[cloud_sync] Mosaic signature persist failed obs {obs_local_id}: {exc}',
                flush=True,
            )
    stage_ns['local_signature_ms'] = time.monotonic_ns() - local_signature_start_ns

    overlay_count = sum(1 for t in manifest.tiles if t.overlay_json is not None)
    total_ns_elapsed = time.monotonic_ns() - push_start_ns
    stage_ns_ms = {key: round(value / 1e6, 2) for key, value in stage_ns.items()}
    stage_ns_ms['total_ms'] = round(total_ns_elapsed / 1e6, 2)
    print(
        (
            f'[cloud_sync] Mosaic stage timings obs {obs_local_id}: '
            f'{stage_ns_ms}'
        ),
        flush=True,
    )
    print(
        (
            f'[cloud_sync] Mosaic done obs {obs_local_id}: '
            f'tiles={len(manifest.tiles)} overlays={overlay_count} '
            f'size={manifest.width_px}x{manifest.height_px} '
            f'tile_size={manifest.tile_size_px} '
            f'bytes={len(manifest.image_bytes)} '
            f'storage_key={storage_key}'
        ),
        flush=True,
    )
    return MOSAIC_STATUS_GENERATED


def backfill_public_spore_mosaics(
    client: 'SporelyCloudClient',
    *,
    observation_cloud_ids: list[int] | list[str] | None = None,
    limit: int | None = None,
    push_measurements: bool = True,
    ensure_image_metadata: bool = True,
    diagnose: bool = False,
) -> dict:
    """Explicit backfill/repair path for public spore mosaics.

    Iterates every locally-known observation that already has a cloud id
    and calls `_push_spore_mosaic_for_observation` for each — same code
    path normal sync uses, but bypassing the "dirty observation" filter
    that skips clean rows during regular pushes. This lets a user
    (re)generate mosaics for observations that were synced before mosaic
    support existed, or for a specific set of `--observation-cloud-id`
    values during debugging.

    Arguments:
    * `observation_cloud_ids` — optional whitelist. Values may be ints or
      digit strings; they are compared as strings against the local
      `observations.cloud_id` column.
    * `limit` — optional cap on how many observations to process. Applied
      after the whitelist filter.

    Auth / temporary-unavailable errors from the underlying pusher
    propagate here, aborting the backfill mid-run so a bad token doesn't
    silently mark every remaining observation as `failed`. Every other
    per-observation failure is logged and the loop continues.

    Returns a counts dict with these keys (all ints):
      candidates, generated,
      skipped_no_cloud_id, skipped_no_public_spores,
      skipped_no_measurement_cloud_ids, skipped_missing_source_images,
      failed.
    """
    id_filter: set[str] | None = None
    if observation_cloud_ids is not None:
        id_filter = {
            str(value).strip()
            for value in observation_cloud_ids
            if str(value or '').strip()
        }

    print(
        (
            f'[cloud_sync] Mosaic backfill: start '
            f'observation_cloud_ids='
            f'{sorted(id_filter) if id_filter is not None else None} '
            f'limit={limit}'
        ),
        flush=True,
    )

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id AS local_id,
                   cloud_id,
                   spore_data_visibility
            FROM observations
            WHERE cloud_id IS NOT NULL AND cloud_id != ''
            ORDER BY id
            '''
        )
        all_rows = [dict(r) for r in cursor.fetchall()]
    finally:
        conn.close()

    filtered: list[dict] = []
    for row in all_rows:
        cloud_id = str(row.get('cloud_id') or '').strip()
        if id_filter is not None and cloud_id not in id_filter:
            continue
        filtered.append({**row, 'cloud_id': cloud_id})
        if limit is not None and len(filtered) >= limit:
            break

    counts = {
        'candidates': len(filtered),
        'generated': 0,
        'skipped_no_cloud_id': 0,          # placeholder for symmetry — pre-filtered
        'skipped_no_public_spores': 0,
        'skipped_no_measurement_cloud_ids': 0,
        'skipped_missing_source_images': 0,
        'skipped_unchanged': 0,
        'failed': 0,
    }

    if id_filter is not None:
        # Whitelist entries that never matched anything locally deserve a log.
        matched = {str(row.get('cloud_id') or '').strip() for row in filtered}
        for wanted in sorted(id_filter - matched):
            print(
                f'[cloud_sync] Mosaic backfill: skipped local=? cloud={wanted} '
                f'reason=no_local_observation_with_cloud_id',
                flush=True,
            )

    for row in filtered:
        local_id = int(row['local_id'])
        cloud_id = str(row['cloud_id'])
        print(
            f'[cloud_sync] Mosaic backfill: candidate local={local_id} cloud={cloud_id}',
            flush=True,
        )

        # Anchor every local microscope image that has public-eligible spore
        # measurements to a metadata-only cloud row (storage_path = NULL).
        # This is what unlocks the "26 vs 8 sporePoints" gap for
        # observations whose microscope frames were intentionally not
        # uploaded. No image bytes are uploaded here — only metadata.
        if ensure_image_metadata:
            try:
                _ensure_metadata_only_microscope_images_for_observation(
                    client, local_id, cloud_id,
                )
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                print(
                    f'[cloud_sync] Mosaic image metadata: observation failed '
                    f'local={local_id} cloud={cloud_id}: {exc}',
                    flush=True,
                )

        # Ensure every locally-known measurement for this observation exists
        # in the cloud before we build a mosaic. Otherwise the pusher's
        # `m.cloud_id IS NOT NULL` gate drops measurements that were made
        # in the desktop app after the last regular sync — which is exactly
        # what caused observations to show only a subset of their spores in
        # the public strip.
        if push_measurements:
            try:
                _push_measurements_for_observation(client, local_id)
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                print(
                    f'[cloud_sync] Mosaic backfill: measurement push failed '
                    f'local={local_id} cloud={cloud_id}: {exc}',
                    flush=True,
                )

        if diagnose:
            try:
                diagnose_public_spore_mosaic_gates(
                    client, local_id, cloud_id, include_remote=True, log=True,
                )
            except Exception as exc:
                print(
                    f'[cloud_sync] Mosaic backfill: diagnose failed '
                    f'local={local_id} cloud={cloud_id}: {exc}',
                    flush=True,
                )

        # Backfill always bypasses the sync-time mosaic signature guard so
        # an operator can force a rebuild without touching the local rows.
        # Clearing the cached signature is enough — the pusher's own check
        # requires a non-empty stored value to skip, so a NULL means
        # "always rebuild, then store the fresh signature on success".
        _clear_local_mosaic_signature(local_id)

        try:
            status = _push_spore_mosaic_for_observation(client, local_id, cloud_id)
        except Exception as exc:
            # Auth / temporary errors abort the whole backfill run. Everything
            # else was translated to a status code inside the pusher; if we
            # still see an exception it's a real unexpected error, so count
            # it as a failure but don't stop the loop.
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                print(
                    f'[cloud_sync] Mosaic backfill: aborting on auth/temporary error '
                    f'local={local_id} cloud={cloud_id}: {exc}',
                    flush=True,
                )
                raise
            counts['failed'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: unexpected failure local={local_id} '
                f'cloud={cloud_id}: {exc}',
                flush=True,
            )
            continue

        if status == MOSAIC_STATUS_GENERATED:
            counts['generated'] += 1
        elif status == MOSAIC_STATUS_SKIP_UNCHANGED:
            # Backfill always passes force_rebuild=True so this branch is
            # only hit from a stray direct pusher call; count it as a
            # skip but keep it separate from failure buckets.
            counts['skipped_unchanged'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: skipped local={local_id} '
                f'cloud={cloud_id} reason=signature_unchanged',
                flush=True,
            )
        elif status == MOSAIC_STATUS_SKIP_NO_PUBLIC_SPORE_DATA:
            counts['skipped_no_public_spores'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: skipped local={local_id} '
                f'cloud={cloud_id} reason=no_public_spore_data',
                flush=True,
            )
        elif status == MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS:
            counts['skipped_no_measurement_cloud_ids'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: skipped local={local_id} '
                f'cloud={cloud_id} reason=no_measurement_cloud_ids',
                flush=True,
            )
        elif status in (
            MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES,
            MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES,
        ):
            counts['skipped_missing_source_images'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: skipped local={local_id} '
                f'cloud={cloud_id} reason=missing_source_images',
                flush=True,
            )
        elif status == MOSAIC_STATUS_SKIP_NO_OBSERVATION:
            # We only ever queue observations that came from the DB, so this
            # shouldn't fire. Count it as a failure rather than silently drop.
            counts['failed'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: skipped local={local_id} '
                f'cloud={cloud_id} reason=no_observation_row',
                flush=True,
            )
        elif status.startswith('fail_'):
            counts['failed'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: failed local={local_id} '
                f'cloud={cloud_id} reason={status}',
                flush=True,
            )
        else:
            # Unknown status — count as failed so we notice.
            counts['failed'] += 1
            print(
                f'[cloud_sync] Mosaic backfill: unknown status local={local_id} '
                f'cloud={cloud_id} reason={status!r}',
                flush=True,
            )

    total_skipped = (
        counts['skipped_no_cloud_id']
        + counts['skipped_no_public_spores']
        + counts['skipped_no_measurement_cloud_ids']
        + counts['skipped_missing_source_images']
        + counts['skipped_unchanged']
    )
    print(
        (
            f'[cloud_sync] Mosaic backfill: complete '
            f'candidates={counts["candidates"]} generated={counts["generated"]} '
            f'skipped={total_skipped} failed={counts["failed"]}'
        ),
        flush=True,
    )
    return counts
