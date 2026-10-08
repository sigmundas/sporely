"""Cloud-sync profiling, progress emission and the sync summary.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import os
import time
import uuid

from contextlib import (
    contextmanager,
    nullcontext,
)
from contextvars import ContextVar
from dataclasses import (
    dataclass,
    field,
)
from typing import Callable

from utils.cloud_sync_impl.common import _safe_int
from utils.cloud_sync_impl.errors import (
    _MEASUREMENT_CONFLICT_RE,
    _PULL_CONFLICT_RE,
    _PUSH_CONFLICT_RE,
    _REVIEW_CONFLICT_RE,
    infer_image_too_large_for_plan_reason,
    is_image_too_large_for_plan_error,
    is_privacy_slot_limit_error,
    privacy_slot_limit_user_message,
    summarize_image_too_large_for_plan_error,
)


ProgressCallback = Callable[[str, int, int], None]


_CLOUD_SYNC_PROFILE_ENV = 'SPORELY_CLOUD_SYNC_PROFILE'


_CLOUD_SYNC_DEBUG_ENV = 'SPORELY_DEBUG_CLOUD_SYNC'


_CLOUD_SYNC_PROFILE_CONTEXT: ContextVar['CloudSyncProfiler | None'] = ContextVar(
    'cloud_sync_profiler',
    default=None,
)


_CLOUD_SYNC_SUMMARY_CONTEXT: ContextVar[dict[str, int] | None] = ContextVar(
    'cloud_sync_summary',
    default=None,
)


# A single sync sub-step taking longer than this is logged so a silent UI pause
# can be traced to the exact calibration / step responsible.
_CLOUD_SYNC_SLOW_STEP_SECONDS = 1.0


# Per-sync progress trace. When set, every progress message emission records its
# monotonic timestamp so a gap between two UI updates (i.e. a backend step that
# produced no progress text) can be logged and traced to whatever was running.
_CLOUD_SYNC_PROGRESS_TRACE_CONTEXT: ContextVar[dict | None] = ContextVar(
    'cloud_sync_progress_trace',
    default=None,
)


def _cloud_sync_progress_trace() -> dict | None:
    try:
        return _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.get()
    except Exception:
        return None


def _cloud_sync_profile_enabled() -> bool:
    return str(os.getenv(_CLOUD_SYNC_PROFILE_ENV) or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _cloud_sync_debug_enabled() -> bool:
    return str(os.getenv(_CLOUD_SYNC_DEBUG_ENV) or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _cloud_sync_current_profiler() -> 'CloudSyncProfiler | None':
    try:
        return _CLOUD_SYNC_PROFILE_CONTEXT.get()
    except Exception:
        return None


def _cloud_sync_current_summary() -> dict[str, int] | None:
    try:
        return _CLOUD_SYNC_SUMMARY_CONTEXT.get()
    except Exception:
        return None


@contextmanager
def _cloud_sync_profile_scope(profiler: 'CloudSyncProfiler'):
    token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
    try:
        yield profiler
    finally:
        try:
            _CLOUD_SYNC_PROFILE_CONTEXT.reset(token)
        except Exception:
            pass


@contextmanager
def _cloud_sync_summary_scope(sync_summary: dict[str, int]):
    token = _CLOUD_SYNC_SUMMARY_CONTEXT.set(sync_summary)
    try:
        yield sync_summary
    finally:
        try:
            _CLOUD_SYNC_SUMMARY_CONTEXT.reset(token)
        except Exception:
            pass


def _cloud_sync_phase_scope(profiler: 'CloudSyncProfiler | None', phase_name: str):
    if profiler is None:
        return nullcontext()
    return profiler.phase(phase_name)


def _cloud_sync_perf_counter() -> float:
    try:
        return time.perf_counter()
    except Exception:
        return 0.0


def _cloud_sync_profile_print(payload: dict) -> None:
    try:
        print(
            f"[cloud_sync_profile] {json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))}",
            flush=True,
        )
    except Exception:
        pass


@dataclass
class CloudSyncProfiler:
    sync_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    started_at: float = field(default_factory=_cloud_sync_perf_counter)
    phase_durations_ms: dict[str, float] = field(default_factory=dict)
    download_image_file_calls: int = 0
    download_image_file_duration_ms: float = 0.0
    download_image_file_bytes: int = 0
    generate_all_sizes_calls: int = 0
    generate_all_sizes_duration_ms: float = 0.0
    pull_bulk_image_metadata_calls: int = 0
    pull_bulk_image_metadata_rows: int = 0
    pull_measurements_for_images_calls: int = 0
    pull_measurements_for_images_rows: int = 0
    store_remote_snapshot_fetch_images_count: int = 0
    store_remote_snapshot_fetch_measurements_count: int = 0
    retry_missing_cloud_media_branch_runs: int = 0
    original_upload_calls: int = 0
    original_upload_bytes: int = 0
    original_upload_skipped_disabled: int = 0
    original_upload_skipped_ineligible: int = 0
    original_upload_skipped_too_large: int = 0
    original_upload_failed_uploads: int = 0
    original_download_calls: int = 0
    original_download_bytes: int = 0
    original_download_skipped_disabled: int = 0
    original_download_skipped_missing_key: int = 0
    original_download_skipped_existing_local_original: int = 0
    original_download_skipped_existing_cache: int = 0
    original_download_failed_downloads: int = 0

    def _emit(self, payload: dict) -> None:
        payload = dict(payload or {})
        payload.setdefault('sync_id', self.sync_id)
        _cloud_sync_profile_print(payload)

    def phase(self, phase_name: str):
        @contextmanager
        def _phase_scope():
            start = _cloud_sync_perf_counter()
            try:
                yield
            finally:
                try:
                    elapsed_ms = max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0)
                    key = str(phase_name or '').strip() or 'unknown'
                    self.phase_durations_ms[key] = self.phase_durations_ms.get(key, 0.0) + elapsed_ms
                    self._emit({
                        'event': 'phase',
                        'phase': key,
                        'duration_ms': round(elapsed_ms, 3),
                    })
                except Exception:
                    pass

        return _phase_scope()

    def record_download_image_file(self, duration_ms: float, bytes_downloaded: int = 0) -> None:
        try:
            self.download_image_file_calls += 1
            self.download_image_file_duration_ms += max(0.0, float(duration_ms))
            self.download_image_file_bytes += max(0, int(bytes_downloaded))
        except Exception:
            pass

    def record_generate_all_sizes(self, duration_ms: float) -> None:
        try:
            self.generate_all_sizes_calls += 1
            self.generate_all_sizes_duration_ms += max(0.0, float(duration_ms))
        except Exception:
            pass

    def record_pull_bulk_image_metadata(self, row_count: int) -> None:
        try:
            self.pull_bulk_image_metadata_calls += 1
            self.pull_bulk_image_metadata_rows += max(0, int(row_count))
        except Exception:
            pass

    def record_pull_measurements_for_images(self, row_count: int) -> None:
        try:
            self.pull_measurements_for_images_calls += 1
            self.pull_measurements_for_images_rows += max(0, int(row_count))
        except Exception:
            pass

    def record_store_remote_snapshot_fetch(self, *, images: bool = False, measurements: bool = False) -> None:
        try:
            if images:
                self.store_remote_snapshot_fetch_images_count += 1
            if measurements:
                self.store_remote_snapshot_fetch_measurements_count += 1
        except Exception:
            pass

    def record_retry_missing_cloud_media_branch(self) -> None:
        try:
            self.retry_missing_cloud_media_branch_runs += 1
        except Exception:
            pass

    def record_original_upload_success(self, bytes_uploaded: int = 0) -> None:
        try:
            self.original_upload_calls += 1
            self.original_upload_bytes += max(0, int(bytes_uploaded))
        except Exception:
            pass

    def record_original_upload_skipped_disabled(self) -> None:
        try:
            self.original_upload_skipped_disabled += 1
        except Exception:
            pass

    def record_original_upload_skipped_ineligible(self) -> None:
        try:
            self.original_upload_skipped_ineligible += 1
        except Exception:
            pass

    def record_original_upload_skipped_too_large(self) -> None:
        try:
            self.original_upload_skipped_too_large += 1
        except Exception:
            pass

    def record_original_upload_failed(self) -> None:
        try:
            self.original_upload_failed_uploads += 1
        except Exception:
            pass

    def record_original_download_success(self, bytes_downloaded: int = 0) -> None:
        try:
            self.original_download_calls += 1
            self.original_download_bytes += max(0, int(bytes_downloaded))
        except Exception:
            pass

    def record_original_download_skipped_disabled(self) -> None:
        try:
            self.original_download_skipped_disabled += 1
        except Exception:
            pass

    def record_original_download_skipped_missing_key(self) -> None:
        try:
            self.original_download_skipped_missing_key += 1
        except Exception:
            pass

    def record_original_download_skipped_existing_local_original(self) -> None:
        try:
            self.original_download_skipped_existing_local_original += 1
        except Exception:
            pass

    def record_original_download_skipped_existing_cache(self) -> None:
        try:
            self.original_download_skipped_existing_cache += 1
        except Exception:
            pass

    def record_original_download_failed(self) -> None:
        try:
            self.original_download_failed_downloads += 1
        except Exception:
            pass

    def summary_payload(self, result: dict | None = None, error: Exception | None = None) -> dict:
        try:
            now = _cloud_sync_perf_counter()
            payload = {
                'event': 'summary',
                'status': 'error' if error else 'ok',
                'duration_ms': round(max(0.0, (now - self.started_at) * 1000.0), 3),
                'phases_ms': {
                    key: round(value, 3)
                    for key, value in sorted(self.phase_durations_ms.items(), key=lambda item: item[0])
                },
                'metrics': {
                    'download_image_file': {
                        'calls': self.download_image_file_calls,
                        'duration_ms': round(self.download_image_file_duration_ms, 3),
                        'bytes': self.download_image_file_bytes,
                    },
                    'generate_all_sizes': {
                        'calls': self.generate_all_sizes_calls,
                        'duration_ms': round(self.generate_all_sizes_duration_ms, 3),
                    },
                    'pull_bulk_image_metadata': {
                        'calls': self.pull_bulk_image_metadata_calls,
                        'rows': self.pull_bulk_image_metadata_rows,
                    },
                    'pull_measurements_for_images': {
                        'calls': self.pull_measurements_for_images_calls,
                        'rows': self.pull_measurements_for_images_rows,
                    },
                    'store_remote_snapshot': {
                        'fetched_images': self.store_remote_snapshot_fetch_images_count,
                        'fetched_measurements': self.store_remote_snapshot_fetch_measurements_count,
                    },
                    'retry_missing_cloud_media': {
                        'branch_runs': self.retry_missing_cloud_media_branch_runs,
                    },
                    'original_upload': {
                        'calls': self.original_upload_calls,
                        'bytes': self.original_upload_bytes,
                        'skipped_disabled': self.original_upload_skipped_disabled,
                        'skipped_ineligible': self.original_upload_skipped_ineligible,
                        'skipped_too_large': self.original_upload_skipped_too_large,
                        'failed_uploads': self.original_upload_failed_uploads,
                    },
                    'original_download': {
                        'calls': self.original_download_calls,
                        'bytes': self.original_download_bytes,
                        'skipped_disabled': self.original_download_skipped_disabled,
                        'skipped_missing_key': self.original_download_skipped_missing_key,
                        'skipped_existing_local_original': self.original_download_skipped_existing_local_original,
                        'skipped_existing_cache': self.original_download_skipped_existing_cache,
                        'failed_downloads': self.original_download_failed_downloads,
                    },
                },
            }
            if result is not None:
                payload['result'] = {
                    'pushed': int(result.get('pushed', 0) or 0),
                    'pulled': int(result.get('pulled', 0) or 0),
                    'calibrations_pushed': int(result.get('calibrations_pushed', 0) or 0),
                    'calibrations_pulled': int(result.get('calibrations_pulled', 0) or 0),
                    'deleted_remote': len(result.get('deleted_remote') or []),
                    'error_count': len(result.get('errors') or []),
                }
                sync_summary = result.get('sync_summary')
                if isinstance(sync_summary, dict):
                    payload['result']['sync_summary'] = {
                        str(key): _safe_int(value)
                        for key, value in sync_summary.items()
                    }
            if error is not None:
                error_text = str(error or '').strip()
                if error_text:
                    payload['error'] = error_text[:300]
                payload['error_type'] = error.__class__.__name__
            return payload
        except Exception:
            return {
                'event': 'summary',
                'status': 'error' if error else 'ok',
                'duration_ms': 0.0,
                'phases_ms': {},
                'metrics': {},
            }

    def finish(self, result: dict | None = None, error: Exception | None = None) -> None:
        try:
            self._emit(self.summary_payload(result=result, error=error))
        except Exception:
            pass


def summarize_blocked_write_attempts(attempts) -> str:
    """Compress a raw list of blocked writer names into ``name ×N`` groups.

    A single leaky path (e.g. ``set_image_desktop_id``) can fire hundreds of
    times per run; printing the name once with a count is what the user
    actually reads.
    """
    from collections import Counter as _Counter
    if not attempts:
        return ""
    counts = _Counter(str(n) for n in attempts if n)
    return ", ".join(f"{name} ×{count}" for name, count in counts.most_common())


def partition_download_from_cloud_issues(errors) -> tuple[list[str], list[str]]:
    """Split pull-only messages into (review_items, real_errors).

    Tombstone-skip and needs-review lines are informational — the pull did
    the safe thing (skipped write, kept local state, flagged a conflict).
    They belong in an expandable review list, not the top-level error banner.
    """
    review_items: list[str] = []
    real_errors: list[str] = []
    for raw in errors or []:
        text = str(raw or '').strip()
        if not text:
            continue
        if _REVIEW_CONFLICT_RE.match(text):
            review_items.append(text)
            continue
        if 'has a local tombstone' in text or 'tombstoned' in text.lower():
            review_items.append(text)
            continue
        if _PULL_CONFLICT_RE.match(text) or _MEASUREMENT_CONFLICT_RE.match(text):
            review_items.append(text)
            continue
        real_errors.append(text)
    return review_items, real_errors


def summarize_sync_issues(errors: list[str] | tuple[str, ...] | None) -> dict:
    conflict_entries: dict[str, dict] = {}
    blocked_errors: list[dict] = []
    retryable_errors: list[dict] = []
    other_errors: list[str] = []

    for raw_error in list(errors or []):
        text = str(raw_error or '').strip()
        if not text:
            continue
        if is_privacy_slot_limit_error(text):
            blocked_errors.append({
                'error': text,
                'message': privacy_slot_limit_user_message(),
            })
            continue
        if is_image_too_large_for_plan_error(text):
            reason = infer_image_too_large_for_plan_reason(text)
            retryable_errors.append({
                'error': text,
                'reason': reason,
                'message': summarize_image_too_large_for_plan_error(reason),
            })
            continue
        push_match = _PUSH_CONFLICT_RE.match(text)
        if push_match:
            local_id = int(push_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['push_skipped'] = True
            continue
        pull_match = _PULL_CONFLICT_RE.match(text)
        if pull_match:
            local_id = int(pull_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(pull_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            continue
        review_match = _REVIEW_CONFLICT_RE.match(text)
        if review_match:
            local_id = int(review_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['cloud_id'] = str(review_match.group('cloud_id') or '').strip() or None
            entry['pull_skipped'] = True
            if 'push_blocked' in str(review_match.group('reason') or ''):
                entry['push_skipped'] = True
            continue
        measurement_match = _MEASUREMENT_CONFLICT_RE.match(text)
        if measurement_match:
            local_id = int(measurement_match.group('local_id'))
            entry = conflict_entries.setdefault(
                str(local_id),
                {'local_id': local_id, 'cloud_id': None, 'push_skipped': False, 'pull_skipped': False},
            )
            entry['pull_skipped'] = True
            entry['measurement_conflict'] = True
            measurement_ids = entry.setdefault('measurement_ids', [])
            measurement_id = str(measurement_match.group('measurement_id') or '').strip()
            if measurement_id and measurement_id not in measurement_ids:
                measurement_ids.append(measurement_id)
            continue
        other_errors.append(text)

    conflicts = sorted(
        conflict_entries.values(),
        key=lambda row: (
            int(row.get('local_id') or 0),
            str(row.get('cloud_id') or ''),
        ),
    )
    return {
        'conflicts': conflicts,
        'conflict_count': len(conflicts),
        'blocked_errors': blocked_errors,
        'blocked_count': len(blocked_errors),
        'retryable_errors': retryable_errors,
        'retryable_count': len(retryable_errors),
        'other_errors': other_errors,
        'other_count': len(other_errors),
        'display_count': len(conflicts) + len(blocked_errors) + len(retryable_errors) + len(other_errors),
    }


def _progress_done(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('done', 0) or 0))
    except Exception:
        return 0


def _progress_total(progress_state: dict | None) -> int:
    try:
        return max(0, int((progress_state or {}).get('total', 0) or 0))
    except Exception:
        return 0


def _trace_progress_gap(message: str) -> None:
    """Log when a long backend step elapsed between two UI progress updates.

    The UI only shows the *last* emitted message. If a slow step runs while that
    message stays on screen (e.g. the bar appears frozen on "Checking
    calibration 4/8"), the gap is logged here naming both messages so the pause
    can be traced to the actual backend work, even when that work emits no
    progress text of its own.
    """
    trace = _cloud_sync_progress_trace()
    if not isinstance(trace, dict):
        return
    now = _cloud_sync_perf_counter()
    last_t = trace.get('last_t')
    last_msg = trace.get('last_msg')
    start = trace.get('start', now)
    if last_t is not None:
        gap = now - last_t
        if gap >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
            print(
                f"[cloud_sync] progress gap: {gap * 1000:.0f}ms with no UI update "
                f"(stuck showing \"{last_msg}\") before \"{message}\" "
                f"at +{(now - start):.1f}s into sync",
                flush=True,
            )
    trace['last_t'] = now
    trace['last_msg'] = message


# Weighted global progress model for cloud sync.
#
# The UI shows a single progress bar; each sync phase maps onto a fixed
# percentage range. Per-phase (done, total) counters are turned into a global
# 0–100 value by _sync_progress_percent so the bar advances monotonically at
# roughly the pace of real work — never jumping to 99% while phases like
# "Loading cloud measurements" or "Checking cloud observation N/M" are still
# running.
#
# Ordered by execution — the tuple is (name, start_percent, end_percent).
_SYNC_PROGRESS_PHASES: tuple[tuple[str, int, int], ...] = (
    ('auth', 0, 5),
    ('calibration_push', 5, 12),
    ('observation_preflight', 12, 18),
    ('push_observations', 18, 45),
    ('refresh_remote', 45, 50),
    ('pull_preflight', 50, 65),
    ('pull_measurements', 65, 75),
    ('pull_observations', 75, 92),
    ('calibration_pull', 92, 97),
    ('finalize', 97, 100),
)


_SYNC_PROGRESS_PHASE_RANGES: dict[str, tuple[int, int]] = {
    name: (start, end) for name, start, end in _SYNC_PROGRESS_PHASES
}


_SYNC_PROGRESS_TOTAL_UNITS = 100


def _sync_progress_percent(phase: str | None, done: int, total: int) -> int:
    """Map per-phase (done, total) onto the global 0–100 progress scale.

    Unknown or missing phases resolve to 0 so a bug in phase wiring can never
    silently pin the bar to 99%.
    """
    start, end = _SYNC_PROGRESS_PHASE_RANGES.get(str(phase or ''), (0, 0))
    try:
        done_int = max(0, int(done))
        total_int = max(0, int(total))
    except Exception:
        done_int, total_int = 0, 0
    if total_int <= 0:
        return int(start)
    frac = min(1.0, done_int / total_int)
    return int(round(start + frac * (end - start)))


def _set_progress_phase(
    progress_state: dict | None,
    phase_name: str,
    phase_total: int = 0,
) -> None:
    """Enter a named sync phase and reset the per-phase (done, total) counter.

    Progress is tracked *per phase*, not globally: each phase gets a fresh
    ``done``/``total`` pair which the ``_emit_progress`` mapper then squeezes
    into that phase's slice of the global 0–100 range. The finalize phase is
    the only one allowed to reach 100%; other phases cap at their configured
    end percentage even if more work than expected turns out to be needed.
    """
    if not isinstance(progress_state, dict):
        return
    if phase_name not in _SYNC_PROGRESS_PHASE_RANGES:
        # Silently ignore unknown phases so tests that seed a raw
        # {done, total} dict without wiring phases keep working.
        return
    progress_state['phase'] = phase_name
    progress_state['done'] = 0
    try:
        progress_state['total'] = max(0, int(phase_total or 0))
    except Exception:
        progress_state['total'] = 0


def _current_progress_phase(progress_state: dict | None) -> str | None:
    if not isinstance(progress_state, dict):
        return None
    phase = progress_state.get('phase')
    if isinstance(phase, str) and phase in _SYNC_PROGRESS_PHASE_RANGES:
        return phase
    return None


def _emit_progress(
    progress_cb: ProgressCallback | None,
    message: str,
    progress_state: dict | None,
) -> None:
    _trace_progress_gap(message)
    if not callable(progress_cb):
        return
    phase = _current_progress_phase(progress_state)
    if phase is not None:
        phase_done = _progress_done(progress_state)
        phase_total = _progress_total(progress_state)
        percent = _sync_progress_percent(phase, phase_done, phase_total)
        progress_cb(message, percent, _SYNC_PROGRESS_TOTAL_UNITS)
    else:
        progress_cb(message, _progress_done(progress_state), max(1, _progress_total(progress_state)))


def _advance_progress(
    progress_state: dict | None,
    amount: int = 1,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state) + increment
    state['total'] = _progress_total(state)
    return _progress_done(state), _progress_total(state)


def _extend_progress_total(
    progress_state: dict | None,
    amount: int,
) -> tuple[int, int]:
    state = progress_state or {}
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    state['done'] = _progress_done(state)
    state['total'] = _progress_total(state) + increment
    return _progress_done(state), _progress_total(state)


_SYNC_SUMMARY_KEYS = (
    'observations_checked',
    'observations_redirtied_pending_local_images',
    'observations_patched',
    'observations_skipped_noop',
    'observations_deleted_remote',
    'images_checked',
    'images_prepared_local',
    'images_uploaded',
    'images_skipped_already_synced',
    'images_cloud_id_repaired',
    'images_deleted_remote',
    'measurements_checked',
    'measurements_patched',
    'measurements_skipped_noop',
    'calibrations_pushed',
    'calibrations_pulled',
    'calibrations_skipped_noop',
    'calibrations_conflicts',
    'calibration_reference_images_uploaded',
    'calibration_remote_lookups',
    'storage_quota_delta_rpc_calls',
    'remote_media_downloads',
    'remote_media_materializations',
)


def _new_sync_summary() -> dict[str, int]:
    return {key: 0 for key in _SYNC_SUMMARY_KEYS}


def _sync_summary_value(sync_summary: dict | None, key: str) -> int:
    try:
        return max(0, int((sync_summary or {}).get(key, 0) or 0))
    except Exception:
        return 0


def _increment_sync_summary(sync_summary: dict | None, key: str, amount: int = 1) -> None:
    if not isinstance(sync_summary, dict):
        return
    try:
        increment = max(0, int(amount))
    except Exception:
        increment = 0
    if increment <= 0:
        return
    sync_summary[key] = _sync_summary_value(sync_summary, key) + increment


def format_sync_summary(sync_summary: dict | None) -> str | None:
    summary = dict(sync_summary or {})
    if not summary:
        return None

    lines: list[str] = []

    observation_bits = []
    observations_checked = _sync_summary_value(summary, 'observations_checked')
    if observations_checked:
        observation_bits.append(f'{observations_checked} checked')
    observations_redirtied = _sync_summary_value(summary, 'observations_redirtied_pending_local_images')
    if observations_redirtied:
        observation_bits.append(f'{observations_redirtied} re-dirtied due to pending local images')
    observations_patched = _sync_summary_value(summary, 'observations_patched')
    if observations_patched:
        observation_bits.append(f'{observations_patched} patched')
    observations_noop = _sync_summary_value(summary, 'observations_skipped_noop')
    if observations_noop:
        observation_bits.append(f'{observations_noop} skipped as no-op')
    observations_deleted = _sync_summary_value(summary, 'observations_deleted_remote')
    if observations_deleted:
        observation_bits.append(f'{observations_deleted} deleted remotely')
    if observation_bits:
        lines.append(f"Observations: {'; '.join(observation_bits)}.")

    image_bits = []
    images_checked = _sync_summary_value(summary, 'images_checked')
    if images_checked:
        image_bits.append(f'{images_checked} checked')
    images_prepared = _sync_summary_value(summary, 'images_prepared_local')
    if images_prepared:
        image_bits.append(f'{images_prepared} prepared for upload')
    images_uploaded = _sync_summary_value(summary, 'images_uploaded')
    if images_uploaded:
        image_bits.append(f'{images_uploaded} uploaded')
    images_skipped = _sync_summary_value(summary, 'images_skipped_already_synced')
    if images_skipped:
        image_bits.append(f'{images_skipped} skipped as already synced')
    images_repaired = _sync_summary_value(summary, 'images_cloud_id_repaired')
    if images_repaired:
        image_bits.append(f'{images_repaired} cloud_id associations repaired')
    images_deleted = _sync_summary_value(summary, 'images_deleted_remote')
    if images_deleted:
        image_bits.append(f'{images_deleted} deleted remotely')
    if image_bits:
        lines.append(f"Images: {'; '.join(image_bits)}.")

    measurement_bits = []
    measurements_checked = _sync_summary_value(summary, 'measurements_checked')
    if measurements_checked:
        measurement_bits.append(f'{measurements_checked} checked')
    measurements_patched = _sync_summary_value(summary, 'measurements_patched')
    if measurements_patched:
        measurement_bits.append(f'{measurements_patched} patched')
    measurements_noop = _sync_summary_value(summary, 'measurements_skipped_noop')
    if measurements_noop:
        measurement_bits.append(f'{measurements_noop} skipped as no-op')
    if measurement_bits:
        lines.append(f"Measurements: {'; '.join(measurement_bits)}.")

    calibration_bits = []
    calibrations_pushed = _sync_summary_value(summary, 'calibrations_pushed')
    if calibrations_pushed:
        calibration_bits.append(f'{calibrations_pushed} pushed')
    calibrations_pulled = _sync_summary_value(summary, 'calibrations_pulled')
    if calibrations_pulled:
        calibration_bits.append(f'{calibrations_pulled} pulled')
    calibrations_noop = _sync_summary_value(summary, 'calibrations_skipped_noop')
    if calibrations_noop:
        calibration_bits.append(f'{calibrations_noop} skipped as no-op')
    calibrations_conflicts = _sync_summary_value(summary, 'calibrations_conflicts')
    if calibrations_conflicts:
        calibration_bits.append(f'{calibrations_conflicts} conflicts')
    calibration_reference_uploads = _sync_summary_value(summary, 'calibration_reference_images_uploaded')
    if calibration_reference_uploads:
        calibration_bits.append(f'{calibration_reference_uploads} reference image(s) uploaded')
    if calibration_bits:
        lines.append(f"Calibrations: {'; '.join(calibration_bits)}.")

    storage_quota_delta_calls = _sync_summary_value(summary, 'storage_quota_delta_rpc_calls')
    if storage_quota_delta_calls:
        lines.append(f'Storage quota delta RPC calls: {storage_quota_delta_calls}.')

    remote_downloads = _sync_summary_value(summary, 'remote_media_downloads')
    remote_materializations = _sync_summary_value(summary, 'remote_media_materializations')
    if remote_downloads or remote_materializations:
        lines.append(
            'Remote media downloads/materializations: '
            f'{remote_downloads} downloads; {remote_materializations} materializations.'
        )

    return '\n'.join(lines) if lines else None


def summarize_sync_change_activity(result: dict | None) -> dict:
    """Classify a sync result into real changes vs. checked/no-op/local-only work.

    The push phase walks every observation whose row is dirty or has no cloud_id
    and counts each as "pushed" even when the upsert was a no-op (e.g. an
    observation re-dirtied only because a local image row was re-associated to an
    existing cloud image). The user-facing notification must reflect *real*
    remote-facing or local changes, not the raw dirty-scan count, otherwise a
    no-change sync wrongly reports that an observation was synced.

    Returns a dict with explicit counters plus ``any_real_change``:
      - real remote change: observation/measurement metadata written, image bytes
        uploaded or deleted remotely, calibration pushed / reference image uploaded.
      - real local change: observation/calibration pulled, remote media downloaded
        or materialized locally.
    Local-only cloud_id repairs (``images_cloud_id_repaired``) and pure no-op /
    checked counts are reported but excluded from ``any_real_change``.
    """
    data = dict(result or {})
    summary = data.get('sync_summary') or {}

    def _value(key: str) -> int:
        return _sync_summary_value(summary, key)

    def _result_int(key: str) -> int:
        try:
            return max(0, int(data.get(key, 0) or 0))
        except Exception:
            return 0

    observations_metadata_patched = _value('observations_patched')
    observations_checked = _value('observations_checked')
    observations_checked_noop = _value('observations_skipped_noop')
    observations_deleted_remote = _value('observations_deleted_remote')
    images_uploaded = _value('images_uploaded')
    images_deleted_remote = _value('images_deleted_remote')
    images_repaired_local_only = _value('images_cloud_id_repaired')
    measurements_patched = _value('measurements_patched')
    calibrations_pushed = _value('calibrations_pushed')
    calibration_reference_images_uploaded = _value('calibration_reference_images_uploaded')
    calibrations_pulled = _value('calibrations_pulled')
    remote_media_downloads = _value('remote_media_downloads')
    remote_media_materializations = _value('remote_media_materializations')
    # ``pulled`` is the count of observations pulled into the local DB; fall back
    # to the summary count is not tracked separately, so use the result value.
    observations_pulled = _result_int('pulled')
    deleted_remote_rows = len(data.get('deleted_remote') or [])
    reference_sync = data.get('reference_sync')
    if not isinstance(reference_sync, dict):
        reference_sync = {}

    def _reference_int(key: str) -> int:
        try:
            return max(0, int(reference_sync.get(key, 0) or 0))
        except Exception:
            return 0

    reference_pushed = _reference_int('pushed')
    reference_pulled = _reference_int('pulled')

    real_remote_change = (
        observations_metadata_patched
        + images_uploaded
        + images_deleted_remote
        + measurements_patched
        + calibrations_pushed
        + calibration_reference_images_uploaded
        + observations_deleted_remote
        + reference_pushed
    )
    real_local_change = (
        observations_pulled
        + calibrations_pulled
        + remote_media_downloads
        + remote_media_materializations
        + reference_pulled
    )
    # ``deleted_remote_rows`` (cloud observations deleted elsewhere, awaiting local
    # review) is surfaced by its own notification branch, so it is reported here
    # but not folded into ``any_real_change``.
    any_real_change = bool(real_remote_change or real_local_change)

    return {
        'observations_metadata_patched': observations_metadata_patched,
        'observations_checked': observations_checked,
        'observations_checked_noop': observations_checked_noop,
        'observations_images_repaired_local_only': images_repaired_local_only,
        'observations_pulled': observations_pulled,
        'images_uploaded': images_uploaded,
        'images_deleted_remote': images_deleted_remote,
        'measurements_patched': measurements_patched,
        'calibrations_pushed': calibrations_pushed,
        'calibrations_pulled': calibrations_pulled,
        'calibration_reference_images_uploaded': calibration_reference_images_uploaded,
        'remote_media_downloads': remote_media_downloads,
        'remote_media_materializations': remote_media_materializations,
        'deleted_remote_rows': deleted_remote_rows,
        'reference_pushed': reference_pushed,
        'reference_pulled': reference_pulled,
        'real_remote_change': real_remote_change,
        'real_local_change': real_local_change,
        'any_real_change': any_real_change,
    }


_SYNC_SUMMARY_OBSERVATION_REFRESH_KEYS = (
    'observations_redirtied_pending_local_images',
    'observations_patched',
    'observations_deleted_remote',
    'images_uploaded',
    'images_cloud_id_repaired',
    'images_deleted_remote',
    'measurements_patched',
    'calibrations_pushed',
    'calibrations_pulled',
    'calibrations_conflicts',
    'calibration_reference_images_uploaded',
    'remote_media_downloads',
    'remote_media_materializations',
)


def sync_result_requires_observation_refresh(result: dict | None) -> bool:
    """Return False only when a complete sync result proves a UI no-op."""
    data = dict(result or {})
    summary = data.get('sync_summary')
    if not isinstance(summary, dict):
        return True
    if data.get('cancelled') or data.get('skipped'):
        return True
    if data.get('errors') or data.get('deleted_remote'):
        return True

    reference_sync = data.get('reference_sync')
    if isinstance(reference_sync, dict):
        if any(
            reference_sync.get(key)
            for key in (
                'pushed',
                'pulled',
                'errors',
                'retryable_errors',
                'terminal_errors',
                'conflicts',
                'blocked',
            )
        ):
            return True

    for key in ('pushed', 'pulled'):
        try:
            if int(data.get(key, 0) or 0) != 0:
                return True
        except Exception:
            return True

    return any(
        _sync_summary_value(summary, key) > 0
        for key in _SYNC_SUMMARY_OBSERVATION_REFRESH_KEYS
    )
