"""Calibration push, pull, conflict listing, local-wins repair and image links.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import sqlite3
import threading

from database.models import CalibrationDB
from database.schema import get_connection

from utils.cloud_sync_impl.baseline import (
    _parse_cloud_observation_snapshot,
    _SETTING_CLOUD_OBS_SNAPSHOT_PREFIX,
)
from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.errors import (
    CloudSyncError,
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
)
from utils.cloud_sync_impl.preflight import _image_calibration_uuid
from utils.cloud_sync_impl.progress import (
    _advance_progress,
    _cloud_sync_current_summary,
    _cloud_sync_perf_counter,
    _CLOUD_SYNC_SLOW_STEP_SECONDS,
    _emit_progress,
    _extend_progress_total,
    _increment_sync_summary,
    ProgressCallback,
)
from utils.cloud_sync_impl.reconciliation.calibrations import (
    _calibration_diff_fields,
    _calibration_display_name,
    _calibration_field_changes,
    _calibration_insert_kwargs,
    _calibration_local_wins_patch_payload,
    _calibration_payloads_match,
    _calibration_sync_warning,
    _normalize_calibration_bool,
    _normalize_calibration_date,
    _normalize_calibration_measurements_json,
    _normalize_calibration_text,
    _normalize_calibration_uuid,
)


def _load_local_calibration_rows() -> list[dict]:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM calibrations
            ORDER BY objective_key ASC, calibration_date ASC, id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _load_local_calibration_by_uuid(calibration_uuid: str) -> dict | None:
    uuid_value = _normalize_calibration_uuid(calibration_uuid)
    if not uuid_value:
        return None
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM calibrations WHERE calibration_uuid = ? LIMIT 1",
            (uuid_value,),
        ).fetchone()
        return dict(row) if row else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def _local_calibration_lookup(rows: list[dict] | None = None) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for row in (rows or _load_local_calibration_rows()):
        uuid_value = _normalize_calibration_uuid((row or {}).get('calibration_uuid'))
        if uuid_value:
            lookup[uuid_value] = dict(row or {})
    return lookup


def _local_calibration_id_for_image(image_row: dict | None) -> int | None:
    calibration_uuid = _image_calibration_uuid(image_row)
    if not calibration_uuid:
        return None

    calibration = _load_local_calibration_by_uuid(calibration_uuid)
    if not calibration:
        return None

    calibration_id = _safe_int(calibration.get('id'))
    return calibration_id if calibration_id > 0 else None


def _reconcile_local_image_calibration_links() -> int:
    """Backfill local image calibration_id values from stored cloud snapshots."""
    phase_start = _cloud_sync_perf_counter()
    snapshot_count = 0
    image_count = 0
    calibration_count = 0
    thread_name = threading.current_thread().name
    execution_context = (
        'ui_thread' if threading.current_thread() is threading.main_thread() else 'worker_thread'
    )
    print(
        f"[cloud_sync] calibration image linking: start "
        f"thread={thread_name} execution_context={execution_context}",
        flush=True,
    )
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        try:
            snapshot_rows = cursor.execute(
                'SELECT key, value FROM settings WHERE key LIKE ?',
                (f'{_SETTING_CLOUD_OBS_SNAPSHOT_PREFIX}%',),
            ).fetchall()
        except sqlite3.OperationalError:
            return 0
        if not snapshot_rows:
            return 0
        snapshot_count = len(snapshot_rows)

        calibration_lookup = _local_calibration_lookup()
        calibration_count = len(calibration_lookup)
        try:
            local_image_rows = cursor.execute(
                'SELECT id, cloud_id, calibration_id FROM images WHERE cloud_id IS NOT NULL'
            ).fetchall()
        except sqlite3.OperationalError:
            return 0

        local_images_by_cloud_id = {
            str(row['cloud_id']).strip(): dict(row)
            for row in local_image_rows
            if str(row['cloud_id']).strip()
        }
        image_count = len(local_images_by_cloud_id)
        updates: list[tuple[int, int]] = []

        for snapshot_row in snapshot_rows:
            snapshot = _parse_cloud_observation_snapshot(snapshot_row['value'])
            for remote_image in snapshot.get('images') or []:
                calibration_uuid = _normalize_calibration_uuid(remote_image.get('calibration_uuid'))
                if not calibration_uuid:
                    continue
                calibration_row = calibration_lookup.get(calibration_uuid)
                if not calibration_row:
                    continue
                local_calibration_id = _safe_int(calibration_row.get('id'))
                if local_calibration_id <= 0:
                    continue

                cloud_image_id = str(remote_image.get('id') or '').strip()
                if not cloud_image_id:
                    continue
                local_image_row = local_images_by_cloud_id.get(cloud_image_id)
                if not local_image_row:
                    continue
                current_calibration_id = _safe_int(local_image_row.get('calibration_id'))
                if current_calibration_id == local_calibration_id:
                    continue
                updates.append((local_calibration_id, _safe_int(local_image_row.get('id'))))

        if not updates:
            return 0

        cursor.executemany(
            'UPDATE images SET calibration_id = ? WHERE id = ?',
            updates,
        )
        conn.commit()
        return len(updates)
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()
        print(
            f"[cloud_sync] calibration image linking: complete "
            f"snapshots={snapshot_count} images={image_count} "
            f"calibrations={calibration_count} duration="
            f"{(_cloud_sync_perf_counter() - phase_start) * 1000:.0f}ms "
            f"thread={thread_name} execution_context={execution_context}",
            flush=True,
        )


def push_calibrations(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Push calibration metadata rows that exist only on the desktop."""
    phase_start = _cloud_sync_perf_counter()
    summary = _cloud_sync_current_summary()
    remote_rows = [dict(row or {}) for row in (remote_calibrations or client.list_remote_calibrations())]
    remote_map = {
        _normalize_calibration_uuid(row.get('calibration_uuid')): row
        for row in remote_rows
        if _normalize_calibration_uuid(row.get('calibration_uuid'))
    }
    local_rows = _load_local_calibration_rows()
    total = len(local_rows)
    pushed = 0
    matched_noop = 0
    conflicts = 0
    remote_lookups = 0
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)
    reference_image_uploader = getattr(client, 'push_calibration_reference_image', None)
    print(
        f"[cloud_sync] calibration push: start (local={total}, remote={len(remote_rows)})",
        flush=True,
    )
    if total:
        _emit_progress(progress_cb, "Checking local calibrations…", progress_state)

    for index, local_row in enumerate(local_rows, start=1):
        step_start = _cloud_sync_perf_counter()
        step_kind = 'metadata'
        calibration_uuid = _normalize_calibration_uuid(local_row.get('calibration_uuid'))
        label = _calibration_display_name(local_row)
        _emit_progress(
            progress_cb,
            f"Syncing calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped push because calibration_uuid is missing')
                continue

            remote_row = remote_map.get(calibration_uuid)
            if remote_row is not None:
                if not _calibration_payloads_match(local_row, remote_row):
                    conflicts += 1
                    errors.append(_calibration_sync_warning('push', local_row, remote_row, _calibration_diff_fields(local_row, remote_row)))
                    continue
                matched_noop += 1
                if callable(reference_image_uploader):
                    step_kind = 'reference_image'
                    warning = reference_image_uploader(
                        local_row,
                        cloud_row_id=str(remote_row.get('id') or '').strip() or None,
                        remote_row=remote_row,
                    )
                    if warning:
                        errors.append(warning)
                continue

            # Not in the freshly-listed remote set: double-check the server
            # before inserting so a row created since the list (e.g. another
            # device) is not duplicated. This is an extra remote call per
            # not-yet-synced calibration; tracked so an N+1 shows up in logs.
            step_kind = 'remote_lookup'
            remote_lookups += 1
            current_remote = client.find_remote_calibration(calibration_uuid)
            if current_remote is not None:
                if _calibration_payloads_match(local_row, current_remote):
                    matched_noop += 1
                    if callable(reference_image_uploader):
                        step_kind = 'reference_image'
                        warning = reference_image_uploader(
                            local_row,
                            cloud_row_id=str(current_remote.get('id') or '').strip() or None,
                            remote_row=current_remote,
                        )
                        if warning:
                            errors.append(warning)
                    continue
                conflicts += 1
                errors.append(_calibration_sync_warning('push', local_row, current_remote, _calibration_diff_fields(local_row, current_remote)))
                continue

            step_kind = 'metadata_insert'
            cloud_row_id = client.push_calibration_metadata(local_row)
            pushed += 1
            if callable(reference_image_uploader):
                step_kind = 'reference_image'
                warning = reference_image_uploader(
                    local_row,
                    cloud_row_id=cloud_row_id,
                    remote_row={'id': cloud_row_id, 'image_storage_path': None},
                )
                if warning:
                    errors.append(warning)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            step_elapsed = _cloud_sync_perf_counter() - step_start
            if step_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                print(
                    f"[cloud_sync] calibration push: slow step "
                    f"calibration {index}/{max(1, total)} ({label}) "
                    f"took {step_elapsed * 1000:.0f}ms during {step_kind}",
                    flush=True,
                )
            _advance_progress(progress_state, 1)

    _increment_sync_summary(summary, 'calibrations_pushed', pushed)
    _increment_sync_summary(summary, 'calibrations_skipped_noop', matched_noop)
    _increment_sync_summary(summary, 'calibrations_conflicts', conflicts)
    _increment_sync_summary(summary, 'calibration_remote_lookups', remote_lookups)
    print(
        f"[cloud_sync] calibration push: complete pushed={pushed} matched_noop={matched_noop} "
        f"conflicts={conflicts} remote_lookups={remote_lookups} errors={len(errors)} "
        f"duration={(_cloud_sync_perf_counter() - phase_start) * 1000:.0f}ms",
        flush=True,
    )
    return {
        'pushed': pushed,
        'total': total,
        'matched_noop': matched_noop,
        'conflicts': conflicts,
        'remote_lookups': remote_lookups,
        'errors': errors,
    }


def pull_calibrations(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Pull cloud calibration metadata into local rows keyed by UUID."""
    phase_start = _cloud_sync_perf_counter()
    summary = _cloud_sync_current_summary()
    remote_rows = [dict(row or {}) for row in (remote_calibrations or client.list_remote_calibrations())]
    local_rows = _load_local_calibration_rows()
    local_map = _local_calibration_lookup(local_rows)
    total = len(remote_rows)
    pulled = 0
    matched_noop = 0
    conflicts = 0
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)
    print(
        f"[cloud_sync] calibration pull: start (remote={total}, local={len(local_rows)})",
        flush=True,
    )

    remote_rows_sorted = sorted(
        remote_rows,
        key=lambda row: (
            _normalize_calibration_bool(row.get('is_active')),
            _normalize_calibration_text(row.get('objective_key')) or '',
            _normalize_calibration_date(row.get('calibration_date')) or '',
            str(row.get('id') or ''),
        ),
    )

    for index, remote_row in enumerate(remote_rows_sorted, start=1):
        step_start = _cloud_sync_perf_counter()
        calibration_uuid = _normalize_calibration_uuid(remote_row.get('calibration_uuid'))
        label = _calibration_display_name(remote_row)
        _emit_progress(
            progress_cb,
            f"Checking calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped pull because calibration_uuid is missing')
                continue

            local_row = local_map.get(calibration_uuid)
            if local_row is not None:
                if not _calibration_payloads_match(local_row, remote_row):
                    conflicts += 1
                    errors.append(_calibration_sync_warning('pull', local_row, remote_row, _calibration_diff_fields(local_row, remote_row)))
                else:
                    matched_noop += 1
                continue

            try:
                CalibrationDB.add_calibration(**_calibration_insert_kwargs(remote_row))
                pulled += 1
                local_map[calibration_uuid] = _load_local_calibration_by_uuid(calibration_uuid) or dict(remote_row)
            except sqlite3.IntegrityError:
                current_local = _load_local_calibration_by_uuid(calibration_uuid)
                if current_local and _calibration_payloads_match(current_local, remote_row):
                    matched_noop += 1
                    local_map[calibration_uuid] = current_local
                    continue
                conflicts += 1
                errors.append(_calibration_sync_warning('pull', current_local or remote_row, remote_row, _calibration_diff_fields(current_local or {}, remote_row)))
            except Exception as exc:
                errors.append(f'calibration {calibration_uuid}: {exc}')
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            step_elapsed = _cloud_sync_perf_counter() - step_start
            if step_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                print(
                    f"[cloud_sync] calibration pull: slow step "
                    f"calibration {index}/{max(1, total)} ({label}) took {step_elapsed * 1000:.0f}ms",
                    flush=True,
                )
            _advance_progress(progress_state, 1)

    reconcile_start = _cloud_sync_perf_counter()
    reconciled_links = 0
    try:
        _emit_progress(progress_cb, "Linking calibration images…", progress_state)
        reconciled_links = _reconcile_local_image_calibration_links()
        _emit_progress(progress_cb, "Calibration image linking complete.", progress_state)
    except Exception as exc:
        errors.append(f'calibration reconciliation: {exc}')
    reconcile_elapsed = _cloud_sync_perf_counter() - reconcile_start
    if reconcile_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
        print(
            f"[cloud_sync] calibration pull: image link reconciliation took "
            f"{reconcile_elapsed * 1000:.0f}ms ({reconciled_links} link(s) updated)",
            flush=True,
        )

    _increment_sync_summary(summary, 'calibrations_pulled', pulled)
    _increment_sync_summary(summary, 'calibrations_skipped_noop', matched_noop)
    _increment_sync_summary(summary, 'calibrations_conflicts', conflicts)
    print(
        f"[cloud_sync] calibration pull: complete pulled={pulled} matched_noop={matched_noop} "
        f"conflicts={conflicts} links_updated={reconciled_links} errors={len(errors)} "
        f"duration={(_cloud_sync_perf_counter() - phase_start) * 1000:.0f}ms",
        flush=True,
    )
    return {
        'pulled': pulled,
        'total': total,
        'matched_noop': matched_noop,
        'conflicts': conflicts,
        'links_updated': reconciled_links,
        'errors': errors,
    }


def list_calibration_conflicts(
    client: SporelyCloudClient,
    calibration_uuids: list[str] | None = None,
    remote_calibrations: list[dict] | None = None,
) -> list[dict]:
    """Return explicit calibration UUID conflicts between the local DB and cloud."""
    remote_source = remote_calibrations if remote_calibrations is not None else client.list_remote_calibrations()
    remote_rows = [dict(row or {}) for row in remote_source]
    remote_map = {
        _normalize_calibration_uuid(row.get('calibration_uuid')): row
        for row in remote_rows
        if _normalize_calibration_uuid(row.get('calibration_uuid'))
    }
    target_uuids = None
    if calibration_uuids is not None:
        target_uuids = {
            _normalize_calibration_uuid(value)
            for value in calibration_uuids
            if _normalize_calibration_uuid(value)
        }
    local_rows = _load_local_calibration_rows()
    if target_uuids is not None:
        local_rows = [
            row
            for row in local_rows
            if _normalize_calibration_uuid(row.get('calibration_uuid')) in target_uuids
        ]

    conflicts: list[dict] = []
    for local_row in local_rows:
        calibration_uuid = _normalize_calibration_uuid(local_row.get('calibration_uuid'))
        if not calibration_uuid:
            continue
        remote_row = remote_map.get(calibration_uuid)
        if remote_row is None:
            continue
        changes = _calibration_field_changes(local_row, remote_row)
        if not changes:
            continue
        conflict = {
            'calibration_uuid': calibration_uuid,
            'cloud_row_id': str(remote_row.get('id') or '').strip() or None,
            'label': _calibration_display_name(local_row),
            'fields': list(changes.keys()),
            'local_row': dict(local_row),
            'remote_row': dict(remote_row),
        }
        if 'measurements_json' in changes:
            conflict['normalized_local_measurements_json'] = _normalize_calibration_measurements_json(
                local_row.get('measurements_json')
            )
            conflict['normalized_remote_measurements_json'] = _normalize_calibration_measurements_json(
                remote_row.get('measurements_json')
            )
        conflicts.append(conflict)
    return conflicts


def repair_calibrations_local_wins(
    client: SporelyCloudClient,
    calibration_uuids: list[str] | None = None,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_calibrations: list[dict] | None = None,
) -> dict:
    """Repair conflicting cloud calibration metadata using the local desktop rows as source of truth."""
    conflicts = list_calibration_conflicts(
        client,
        calibration_uuids=calibration_uuids,
        remote_calibrations=remote_calibrations,
    )
    total = len(conflicts)
    repaired = 0
    repairs: list[dict] = []
    errors: list[str] = []
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    _extend_progress_total(progress_state, total)

    for index, conflict in enumerate(conflicts, start=1):
        calibration_uuid = _normalize_calibration_uuid(conflict.get('calibration_uuid'))
        local_row = dict(conflict.get('local_row') or {})
        remote_row = dict(conflict.get('remote_row') or {})
        label = str(conflict.get('label') or _calibration_display_name(local_row))
        _emit_progress(
            progress_cb,
            f"Repairing calibration {index}/{max(1, total)}: {label}…",
            progress_state,
        )
        try:
            if not calibration_uuid:
                errors.append('calibration ?: skipped repair because calibration_uuid is missing')
                continue

            fields = list(conflict.get('fields') or [])
            if not fields:
                continue

            cloud_row_id = str(conflict.get('cloud_row_id') or remote_row.get('id') or '').strip()
            if not cloud_row_id:
                current_remote = client.find_remote_calibration(calibration_uuid)
                remote_row = dict(current_remote or {})
                cloud_row_id = str(remote_row.get('id') or '').strip()
            if not cloud_row_id:
                errors.append(
                    f'calibration {calibration_uuid}: skipped repair because the cloud row id is unavailable'
                )
                continue

            patch_payload = _calibration_local_wins_patch_payload(local_row, remote_row)
            if not patch_payload:
                continue

            client._patch(
                f'calibrations?user_id=eq.{client.user_id}&id=eq.{cloud_row_id}',
                patch_payload,
            )
            repaired += 1
            fields = list(fields or _calibration_diff_fields(local_row, remote_row))
            repair_entry = {
                'calibration_uuid': calibration_uuid,
                'cloud_row_id': cloud_row_id,
                'fields': fields,
                'message': (
                    f'calibration {calibration_uuid}: repaired local-wins cloud row {cloud_row_id} '
                    f'overwrote fields ({", ".join(fields)})'
                ),
            }
            refreshed_remote = None
            if 'measurements_json' in fields:
                try:
                    refreshed_remote = client.find_remote_calibration(calibration_uuid)
                except Exception as exc:
                    if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                        raise
                    errors.append(
                        f'calibration {calibration_uuid}: could not re-read cloud row after repair ({exc})'
                    )
                else:
                    remaining_changes = _calibration_field_changes(local_row, refreshed_remote or remote_row)
                    if 'measurements_json' in remaining_changes:
                        repair_entry['remaining_fields'] = list(remaining_changes.keys())
                        repair_entry['normalized_local_measurements_json'] = _normalize_calibration_measurements_json(
                            local_row.get('measurements_json')
                        )
                        repair_entry['normalized_remote_measurements_json'] = _normalize_calibration_measurements_json(
                            (refreshed_remote or remote_row).get('measurements_json')
                        )
                        print(
                            '[cloud_sync] '
                            f'calibration {calibration_uuid}: measurements_json still differs after local-wins repair '
                            f'(local={json.dumps(repair_entry["normalized_local_measurements_json"], ensure_ascii=False, sort_keys=True)}, '
                            f'remote={json.dumps(repair_entry["normalized_remote_measurements_json"], ensure_ascii=False, sort_keys=True)})'
                        )
            repairs.append(repair_entry)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            errors.append(f'calibration {calibration_uuid or "?"}: {exc}')
        finally:
            _advance_progress(progress_state, 1)

    return {'repaired': repaired, 'total': total, 'repairs': repairs, 'errors': errors}
