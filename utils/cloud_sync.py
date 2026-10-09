"""Bidirectional sync between local SQLite and Sporely cloud (Supabase).

Desktop → Cloud  push observations and selected images not yet synced
Cloud → Desktop  pull observations created on mobile/web (no desktop_id)

One-time Supabase SQL to run in the SQL editor for optimal upsert performance:
    ALTER TABLE public.observations
        ADD CONSTRAINT observations_desktop_id_user_unique UNIQUE (desktop_id, user_id);
    ALTER TABLE public.observation_images
        ADD CONSTRAINT observation_images_desktop_id_user_unique UNIQUE (desktop_id, user_id);
"""
from __future__ import annotations

import base64
import logging
import math
import os
import hashlib
import io
import json
import mimetypes
import re
import random
import sqlite3
import shutil
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import quote, urlparse

import requests
from PIL import Image, ImageOps, features
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
import time

from app_identity import app_data_dir, runtime_profile_scope, using_isolated_profile
from database.schema import get_connection, get_app_settings, get_images_dir, update_app_settings
from database.models import (
    CLOUD_IMAGE_STATE_DELETED,
    CLOUD_IMAGE_STATE_DELETE_PENDING,
    CLOUD_IMAGE_STATE_METADATA_ONLY,
    CLOUD_IMAGE_STATE_NONE,
    CLOUD_IMAGE_STATE_UPLOADED,
    ObservationDB,
    ImageDB,
    SettingsDB,
    MeasurementDB,
    CalibrationDB,
    mark_observation_sync_dirty,
    update_observation_sync_state,
    _upsert_image_tombstone,
    get_image_tombstones_by_deleted_cloud_id,
    get_image_tombstones_by_local_image_id,
    list_pending_image_tombstones,
    mark_image_tombstone_synced,
    reconcile_legacy_publish_exclusion_tombstones,
    derive_image_cloud_state,
)
from database.reverse_location_lookup import normalize_country_code
from utils.artsdatabanken_link import concept_link_from_name_id
from utils.heic_converter import guess_local_image_mime_type
from utils.cloud_media_policy import (
    IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE,
    WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE,
    build_cloud_upload_policy,
    build_full_image_webp_quality_attempts,
    normalize_cloud_plan_profile,
    scale_dimensions_to_max_pixels,
)
from utils.original_sync_policy import (
    FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES,
    is_full_resolution_original_sync_enabled,
    is_full_resolution_original_upload_too_large,
    resolve_full_original_upload_source,
    should_download_full_original,
)
from utils.publish_targets import normalize_publish_target
from utils.taxon_text import resolve_observation_taxon_fields
from utils.taxon_identity import (
    IDENTITY_COLUMNS as _TAXON_IDENTITY_COLUMNS,
    STATE_NONE,
    TaxonIdentity,
)
from utils.r2_storage import (
    CloudflareR2Client,
    CloudflareMediaWorkerClient,
    R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE,
    direct_r2_runtime_available,
    media_worker_base_url,
    media_variant_key,
    normalize_media_key,
)
from utils.thumbnail_generator import generate_all_sizes
from utils.spore_summary_sync import (
    STATUS_SKIP_NO_CLOUD_ID as SUMMARY_STATUS_SKIP_NO_CLOUD_ID,
    STATUS_SKIP_TABLE_MISSING as SUMMARY_STATUS_SKIP_TABLE_MISSING,
    STATUS_SYNCED as SUMMARY_STATUS_SYNCED,
    _is_missing_table_error as _is_summary_table_missing_error,
    sync_observation_spore_summaries,
)

from utils.cloud_sync_impl.errors import (  # noqa: F401  (facade re-export)
    AccountMismatchError,
    CloudImageBytesNotDesiredError,
    CloudReauthRequiredError,
    CloudSessionAccountMismatchError,
    CloudSyncError,
    CloudTemporarilyUnavailableError,
    IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE,
    ImageIdentityConflictError,
    ObservationIdentityConflictError,
    PRIVACY_SLOT_LIMIT_USER_MESSAGE,
    PartialConflictPlanError,
    PullOnlyModeError,
    _CLOUD_AUTH_ERROR_HINTS,
    _CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE,
    _IDENTITY_CLEAR_VERIFICATION_FAILED_MARKER,
    _IMAGE_TOO_LARGE_FOR_PLAN_HINTS,
    _IMAGE_TOO_LARGE_FOR_PLAN_REASONS,
    _IMAGE_TOO_LARGE_FOR_PLAN_REASON_LINE_RE,
    _MEASUREMENT_CONFLICT_RE,
    _PRIVACY_SLOT_LIMIT_HINTS,
    _PULL_CONFLICT_RE,
    _PUSH_CONFLICT_RE,
    _REVIEW_CONFLICT_RE,
    _SUPABASE_TRANSIENT_ERROR_HINTS,
    _SUPABASE_TRANSIENT_STATUS_CODES,
    _collect_sync_error_details,
    _extract_label_value,
    _image_too_large_reason_message,
    _image_too_large_summary_message,
    _infer_image_too_large_reason_from_text,
    _normalize_image_too_large_reason,
    _parse_dimension_pair,
    _parse_human_size,
    _parse_int_text,
    format_cloud_sync_error_details,
    format_image_too_large_for_plan_reason,
    infer_image_too_large_for_plan_reason,
    is_cloud_auth_error,
    is_cloud_temporary_unavailable_error,
    is_identity_clear_verification_failed_error,
    is_image_too_large_for_plan_error,
    is_privacy_slot_limit_error,
    is_webp_support_required_for_cloud_media_upload_error,
    privacy_slot_limit_user_message,
    sanitize_image_too_large_for_plan_error_message,
    summarize_image_too_large_for_plan_error,
)
from utils.cloud_sync_impl.common import (  # noqa: F401  (facade re-export)
    _CLOUD_SYNC_IN_BATCH_SIZE,
    _format_size,
    _join_select_columns,
    _normalize_cloud_media_key,
    _normalize_slug,
    _safe_int,
)
from utils.cloud_sync_impl.progress import (  # noqa: F401  (facade re-export)
    CloudSyncProfiler,
    ProgressCallback,
    _CLOUD_SYNC_DEBUG_ENV,
    _CLOUD_SYNC_PROFILE_CONTEXT,
    _CLOUD_SYNC_PROFILE_ENV,
    _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT,
    _CLOUD_SYNC_SLOW_STEP_SECONDS,
    _CLOUD_SYNC_SUMMARY_CONTEXT,
    _SYNC_PROGRESS_PHASES,
    _SYNC_PROGRESS_PHASE_RANGES,
    _SYNC_PROGRESS_TOTAL_UNITS,
    _SYNC_SUMMARY_KEYS,
    _SYNC_SUMMARY_OBSERVATION_REFRESH_KEYS,
    _advance_progress,
    _cloud_sync_current_profiler,
    _cloud_sync_current_summary,
    _cloud_sync_debug_enabled,
    _cloud_sync_perf_counter,
    _cloud_sync_phase_scope,
    _cloud_sync_profile_enabled,
    _cloud_sync_profile_print,
    _cloud_sync_profile_scope,
    _cloud_sync_progress_trace,
    _cloud_sync_summary_scope,
    _current_progress_phase,
    _emit_progress,
    _extend_progress_total,
    _increment_sync_summary,
    _new_sync_summary,
    _progress_done,
    _progress_total,
    _set_progress_phase,
    _sync_progress_percent,
    _sync_summary_value,
    _trace_progress_gap,
    format_sync_summary,
    partition_download_from_cloud_issues,
    summarize_blocked_write_attempts,
    summarize_sync_change_activity,
    summarize_sync_issues,
    sync_result_requires_observation_refresh,
)
from utils.cloud_sync_impl.pull_only import (  # noqa: F401  (facade re-export)
    PullOnlyCloudClient,
    _PULL_ONLY_ALLOWED_READ_METHODS,
    _PULL_ONLY_ALLOWED_RPC_NAMES,
    _PULL_ONLY_BLOCKED_CLIENT_METHODS,
)
from utils.cloud_sync_impl.transport import (  # noqa: F401  (facade re-export)
    CloudSyncTransportMixin,
    _CLOUD_SYNC_MAX_ROWS_PER_PAGE,
)
from utils.cloud_sync_impl.sync_state import (  # noqa: F401  (facade re-export)
    CLOUD_IMAGE_STORAGE_EXCLUDED_SETTING_PREFIX,
    _CLOUD_IMAGE_STORAGE_INTENT_LEDGER_PREFIX,
    _SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX,
    _SETTING_CLOUD_IMAGE_PROMOTION_PENDING_PREFIX,
    _SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX,
    _clear_cloud_image_file_signature,
    _clear_local_cloud_media_signature,
    _clear_pending_image_promotion_key,
    _cloud_image_file_signature_key,
    _cloud_image_promotion_pending_key,
    _cloud_image_storage_excluded_ids_key,
    _cloud_image_storage_intent_ledger_key,
    _cloud_local_media_signature_key,
    _cloud_metadata_only_image_ids,
    _cloud_metadata_only_image_ids_key,
    _explicit_image_restore_source,
    _explicit_image_restore_source_key,
    _load_cloud_image_file_signature,
    _load_local_cloud_media_signature,
    _load_pending_image_promotion_key,
    _set_cloud_image_metadata_only_state,
    _store_cloud_image_file_signature,
    _store_local_cloud_media_signature,
    _store_pending_image_promotion_key,
    mark_observation_dirty,
    mark_observation_media_dirty,
    remember_explicit_image_restore_source,
)
from utils.cloud_sync_impl.image_policy import (  # noqa: F401  (facade re-export)
    _add_cloud_image_storage_excluded_image_id,
    _cloud_explicit_media_upload_selection,
    _cloud_image_storage_excluded_image_ids,
    _cloud_image_storage_initialized,
    _cloud_image_storage_intent_initialized_ids,
    _ensure_cloud_image_storage_intent_initialized,
    _initialize_cloud_image_storage_desired_state_for_observation,
    _is_generated_cloud_image,
    _is_local_metadata_only_microscope_anchor,
    _is_metadata_only_microscope_cloud_image,
    _mark_cloud_image_storage_intent_initialized,
    _microscope_group_key_from_row,
    _remove_cloud_image_storage_excluded_image_id,
    _set_cloud_image_storage_excluded_image_ids,
    _set_cloud_image_storage_intent_initialized_ids,
    cloud_image_bytes_desired,
    cloud_image_storage_intent_initialized,
    measurement_qualifies_for_public_spore_anchor,
    microscope_image_requires_owner_sync_anchor,
    microscope_image_requires_public_spore_anchor,
    set_image_cloud_selected,
    should_pull_cloud_image_to_desktop,
)
from utils.cloud_sync_impl.capabilities import (  # noqa: F401  (facade re-export)
    METADATA_PURPOSE_OWNER_SYNC,
    METADATA_PURPOSE_PUBLIC_MICROSCOPY,
    _owner_sync_parents_supported,
    _remote_metadata_purpose,
)
from utils.cloud_sync_impl.remote_reads import (  # noqa: F401  (facade re-export)
    _group_remote_measurements_by_observation,
    _pull_remote_images_for_sync,
    _pull_remote_measurements_for_images,
)
from utils.cloud_sync_impl.tombstones import (  # noqa: F401  (facade re-export)
    _local_tombstoned_cloud_image_ids,
    _local_tombstoned_local_image_ids,
    _push_pending_image_tombstones,
    _record_remote_image_tombstones,
    _tombstoned_cloud_image_warning,
)
from utils.cloud_sync_impl.logs import (  # noqa: F401  (facade re-export)
    logger,
)
from utils.cloud_sync_impl.reconciliation.values import (  # noqa: F401  (facade re-export)
    _OBSERVATION_FLOAT_ABS_TOL,
    _OBSERVATION_FLOAT_FIELDS,
    _OBSERVATION_FLOAT_REL_TOL,
    _OBSERVATION_GPS_ABS_TOL,
    _OBSERVATION_INT_FIELDS,
    _OBSERVATION_TIMESTAMP_FIELDS,
    _SNAPSHOT_IMG_FIELDS,
    _SNAPSHOT_IMG_PASSIVE_FIELDS,
    _SNAPSHOT_MEAS_FIELDS,
    _SNAPSHOT_OBS_FIELDS,
    _normalize_image_captured_at_for_cloud,
    _normalize_observation_bool_value,
    _normalize_observation_float_value,
    _normalize_observation_int_value,
    _normalize_observation_json_value,
    _normalize_observation_timestamp_value,
    _normalize_sharing_scope,
    _normalize_snapshot_value,
    _observation_field_values_match,
    _parse_sync_timestamp,
    _sharing_scope_to_cloud_visibility,
)
from utils.cloud_sync_impl.reconciliation.location_precision import (  # noqa: F401  (facade re-export)
    _LOCATION_PRECISION_RANK,
    _location_precision_rank,
)
from utils.cloud_sync_impl.reconciliation.identity import (  # noqa: F401  (facade re-export)
    TAXON_IDENTITY_SYNC_FIELD,
    _IDENTITY_BASELINE_UNKNOWN,
    _OBSERVATION_IDENTITY_SELECT_COLUMNS,
    _RemoteIdentityClaim,
    _baseline_identity_key,
    _classify_identity_sync_change,
    _identification_contradicts_remote,
    _identification_key,
    _identity_sync_key,
    _local_identity_is_claim,
    _local_identity_sync_key,
    _remote_identity_changed_since,
    _remote_identity_claim,
    _remote_name_snapshot,
    _remote_row_without_identity,
    _withhold_identity_from_push,
)
from utils.cloud_sync_impl.reconciliation.images import (  # noqa: F401  (facade re-export)
    _analyze_image_changes,
    _image_compare_key,
    _image_identity_keys,
    _image_metadata_payload,
)
from utils.cloud_sync_impl.reconciliation.measurements import (  # noqa: F401  (facade re-export)
    _MEASUREMENT_FLOAT_ABS_TOL,
    _MEASUREMENT_FLOAT_FIELDS,
    _MEASUREMENT_FLOAT_REL_TOL,
    _MEASUREMENT_SYNC_FIELDS,
    _MEASUREMENT_SYNC_MEDIA_FIELDS,
    _analyze_measurement_changes,
    _baseline_measurement_compare_payload,
    _local_measurement_snapshot_payload,
    _measurement_compare_key,
    _measurement_compare_payload,
    _measurement_field_values_match,
    _measurement_payloads_match,
    _measurement_push_diff_fields,
    _measurement_sync_payload,
    _normalize_measurement_float_value,
    _normalize_measurement_identity_value,
    _normalize_measurement_int_value,
    _normalize_measurement_timestamp_value,
    _normalize_measurement_type_value,
    _remote_measurement_snapshot_payload,
)
from utils.cloud_sync_impl.sample_source import (  # noqa: F401  (facade re-export)
    _CLOUD_SAMPLE_SOURCE_VALUES,
    _LEGACY_SAMPLE_SOURCE_ON_SAMPLE_TYPE,
    _apply_image_sample_fields_to_push_payload,
    _cloud_to_desktop_sample_source,
    _desktop_to_cloud_sample_source,
    _split_legacy_sample_type_into_source,
)
from utils.cloud_sync_impl.image_payloads import (  # noqa: F401  (facade re-export)
    _deleted_remote_image_identity_keys,
    _remote_image_payload,
)
from utils.cloud_sync_impl.reconciliation.calibrations import (  # noqa: F401  (facade re-export)
    _CALIBRATION_CONFLICT_IGNORED_FIELDS,
    _CALIBRATION_FLOAT_ABS_TOL,
    _CALIBRATION_FLOAT_FIELDS,
    _CALIBRATION_FLOAT_REL_TOL,
    _CALIBRATION_SYNC_COLS,
    _calibration_diff_fields,
    _calibration_display_name,
    _calibration_field_changes,
    _calibration_field_values_match,
    _calibration_insert_kwargs,
    _calibration_local_wins_patch_payload,
    _calibration_payloads_match,
    _calibration_sync_payload,
    _calibration_sync_warning,
    _normalize_calibration_bool,
    _normalize_calibration_date,
    _normalize_calibration_float,
    _normalize_calibration_int,
    _normalize_calibration_measurements_json,
    _normalize_calibration_text,
    _normalize_calibration_uuid,
    _serialize_calibration_measurements_json,
)
from utils.cloud_sync_impl.reconciliation.report import (  # noqa: F401  (facade re-export)
    ObservationPushConflictReport,
    _format_observation_metadata_field_label,
    _format_push_conflict_review_reasons,
)
from utils.cloud_sync_impl.reconciliation.asymmetry import (  # noqa: F401  (facade re-export)
    _ASYMMETRY_MATERIAL_IMAGE_FIELDS,
    _accepted_asymmetry_key,
    _asymmetry_fingerprint_local_image,
    _asymmetry_fingerprint_local_measurement,
    _asymmetry_fingerprint_remote_image,
    _asymmetry_fingerprint_remote_measurement,
    _filter_accepted_one_sided_images,
    _filter_accepted_one_sided_measurements,
    _identities_referenced_by_plan,
    _merge_accepted_asymmetry,
    _reconcile_accepted_asymmetry,
)
from utils.cloud_sync_impl.push_payloads import (  # noqa: F401  (facade re-export)
    _OBS_PUSH_COLS,
    _analyze_observation_field_changes,
    _baseline_observation_compare_payload,
    _normalize_observation_field_value,
    _observation_compare_payload,
    _observation_push_payload,
)
from utils.cloud_sync_impl.local_files import (  # noqa: F401  (facade re-export)
    _is_readable_local_file,
    _resolve_existing_local_image_asset_path,
)
from utils.cloud_sync_impl.baseline import (  # noqa: F401  (facade re-export)
    _CLOUD_OBSERVATION_SNAPSHOT_SCHEMA_VERSION,
    _SETTING_CLOUD_OBS_SNAPSHOT_PREFIX,
    _clear_cloud_observation_snapshot,
    _cloud_observation_snapshot,
    _cloud_observation_snapshot_key,
    _load_cloud_observation_snapshot,
    _local_observation_id_by_cloud_id,
    _normalize_accepted_asymmetry_entry,
    _normalize_accepted_asymmetry_for_snapshot,
    _parse_cloud_observation_snapshot,
    _store_cloud_observation_snapshot,
    _store_remote_snapshot,
)
from utils.cloud_sync_impl.identity_state import (  # noqa: F401  (facade re-export)
    IDENTITY_APPLY_APPLIED,
    IDENTITY_APPLY_CONFLICT,
    IDENTITY_APPLY_UNCHANGED,
    _apply_remote_identity_to_local,
    _installed_taxon_concept,
    _local_identity_columns_for_remote_claim,
)
from utils.cloud_sync_impl.location_precision import (  # noqa: F401  (facade re-export)
    _LOCATION_PRECISION_REPAIR_DONE_KEY,
    _confirmed_location_precision,
    _confirmed_location_precision_key,
    _guard_local_location_precision,
    _precision_local_id,
    _snapshot_baseline_for_cloud_id,
    consume_confirmed_location_precision,
    record_confirmed_location_precision,
    repair_legacy_location_precision,
)
from utils.cloud_sync_impl.media_signature import (  # noqa: F401  (facade re-export)
    _CLOUD_LOCAL_MEDIA_RENDER_VERSION,
    _LOCAL_MEDIA_SIGNATURE_OPTIONAL_IMAGE_KEYS,
    _local_cloud_image_media_signature,
    _local_cloud_media_signature,
    _local_media_signatures_match,
    _normalized_local_media_signature_payload,
    _parsed_local_media_signature,
    _path_stat_signature,
    _refresh_local_cloud_media_signature,
    _store_local_media_signature_if_equivalent,
)
from utils.cloud_sync_impl.preflight import (  # noqa: F401  (facade re-export)
    _analyze_observation_push_conflicts,
    _image_calibration_uuid,
    _is_spore_measurement_source_image,
    _load_local_measurement_lookup,
    _local_has_real_changes_since_snapshot,
    _local_image_snapshot_payload,
    _locally_tombstoned_snapshot_image_identity_keys,
    _observation_push_diff_fields,
)
from utils.cloud_sync_impl.calibrations import (  # noqa: F401  (facade re-export)
    _load_local_calibration_by_uuid,
    _load_local_calibration_rows,
    _local_calibration_id_for_image,
    _local_calibration_lookup,
    _reconcile_local_image_calibration_links,
    list_calibration_conflicts,
    pull_calibrations,
    push_calibrations,
    repair_calibrations_local_wins,
)
from utils.cloud_sync_impl.identity_push import (  # noqa: F401  (facade re-export)
    CloudSyncTaxonIdentityMixin,
)
from utils.cloud_sync_impl.image_identity import (  # noqa: F401  (facade re-export)
    _finalize_portable_cloud_identity_guard,
    _portable_cloud_identity_pending_for_observation,
    _reconcile_local_image_cloud_id,
    CloudSyncPushIdentityMixin,
)
from utils.cloud_sync_impl.anchors import (  # noqa: F401  (facade re-export)
    _cancel_microscope_anchor_tombstones,
    _ensure_metadata_anchors_for_public_spore_observation,
    _ensure_metadata_only_microscope_image_for_public_spores,
    _ensure_metadata_only_microscope_images_for_observation,
    _METADATA_ONLY_IMG_FIELDS,
    _metadata_only_microscope_image_payload,
    _observation_has_owner_sync_candidates,
    _remote_image_row_matches_anchor_payload,
    _reserve_anchor_promotion_key,
    _retire_unneeded_owner_sync_parent,
    _rollback_anchor_promotion,
)
from utils.cloud_sync_impl.exif import (  # noqa: F401  (facade re-export)
    _backfill_missing_exif_on_cloud_images,
    _exif_datetime_from_text,
    _exif_file_signature,
    _inject_obs_exif_into_field_image,
    _load_exif_backfill_state,
    _load_obs_exif_fallback,
    _save_exif_backfill_state,
    _SETTING_CLOUD_EXIF_BACKFILL_STATE,
)
from utils.cloud_sync_impl.mosaic_signature import (  # noqa: F401  (facade re-export)
    _canonical_signature_value,
    _clear_local_mosaic_signature,
    _current_local_mosaic_signature,
    _file_stat_fingerprint,
    _load_local_mosaic_signature,
    _load_spore_mosaic_eligible_rows,
    _local_mosaic_signature_is_current,
    _local_spore_mosaic_signature,
    _resolve_local_image_path,
    _store_local_mosaic_signature,
)
from utils.cloud_sync_impl.image_files import (  # noqa: F401  (facade re-export)
    _build_worker_storage_path,
    _detected_image_extension,
    _file_content_signature,
    _new_image_storage_suffix,
    _rename_to_detected_image_extension,
)
from utils.cloud_sync_impl.status_messages import (  # noqa: F401  (facade re-export)
    _format_cloud_sync_observation_status,
    _observation_sync_species_label,
)
from utils.cloud_sync_impl.pending_images import (  # noqa: F401  (facade re-export)
    _CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING,
    _CLOUD_PENDING_IMAGE_REPAIR_INTERVAL_HOURS,
    _cloud_pending_image_repair_scan_due,
    _CLOUD_PENDING_IMAGE_REPAIR_VERSION,
    _CLOUD_PENDING_IMAGE_REPAIR_VERSION_SETTING,
    _cloud_publish_path_key,
    _mark_cloud_observations_dirty_for_pending_local_images,
    _measurement_counts_for_observation_images,
    _pending_cloud_pushable_image_ids,
    _record_cloud_pending_image_repair_scan_complete,
    explain_pending_cloud_image_decision,
    PENDING_REASON_ALREADY_SYNCED,
    PENDING_REASON_CACHE_ROW,
    PENDING_REASON_DUPLICATE,
    PENDING_REASON_EXCLUDED,
    PENDING_REASON_GENERATED,
    PENDING_REASON_INTENT_UNINITIALIZED,
    PENDING_REASON_MICROSCOPE_NO_MEASUREMENTS,
    PENDING_REASON_MISSING_FILE,
    PENDING_REASON_NOT_DESIRED_BY_USER,
    PENDING_REASON_PENDING_UPLOAD,
    PENDING_REASON_WRONG_TYPE,
    PendingImageDecision,
)
from utils.cloud_sync_impl.original_recovery import (  # noqa: F401  (facade re-export)
    _find_remote_original_for_local_image,
    _load_remote_original_recovery_image_rows,
    _local_image_existing_original_path,
    _normalize_original_recovery_cache_extension,
    _original_recovery_cache_path,
    _original_recovery_cache_root,
    _original_recovery_sidecar_path,
    _remote_image_matches_local_image,
    _sanitize_original_recovery_cache_component,
    _write_original_recovery_sidecar,
    recover_full_original_for_image,
)
from utils.cloud_sync_impl.image_pull import (  # noqa: F401  (facade re-export)
    _apply_remote_image_metadata_only_to_local,
    _apply_remote_images_to_local,
    _carry_forward_local_mosaic_signature,
    _cloud_image_captured_at_to_local,
    _cloud_missing_image_warning,
    _cloud_pulled_image_order_key,
    _ensure_local_metadata_only_microscope_anchor,
    _import_remote_images,
    _is_missing_cloud_image_error,
    _load_local_image_lookup,
    _normalize_cloud_pulled_image_order,
    _profile_generate_all_sizes,
    _promote_temp_imported_image_if_needed,
    _remote_ai_crop_box,
    _remote_ai_crop_is_custom,
    _remote_ai_crop_source_size,
    _remote_image_bytes_match_local,
    _remote_image_desktop_id_current,
    _remote_images_missing_locally,
    _sync_existing_remote_image_to_local,
    _update_image_columns_without_touching_observation,
    cloud_media_materialization_state_for_observation,
)
from utils.cloud_sync_impl.image_push import (  # noqa: F401  (facade re-export)
    _associate_persisted_cloud_images,
    _local_image_source_bytes_unchanged,
    _lookup_unlisted_remote_image_storage_path,
    _prepared_item_remote_payload,
    _push_images_for_observation,
    _reconcile_metadata_only_linked_images,
    _select_remote_image_identity_candidate,
    _stored_local_media_signature_image_stats,
    CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY,
    should_push_local_image_to_cloud,
)
from utils.cloud_sync_impl.measurements import (  # noqa: F401  (facade re-export)
    _build_remote_measurement_identity_cache,
    _CLOUD_MEASUREMENT_RECONCILE_VERSION,
    _CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING,
    _cloud_measurement_remote_verification_due,
    _measurement_push_lookup_keys,
    _push_measurements_for_observation,
    _SPORE_MEASUREMENT_SELECT_COLUMNS,
    fetch_remote_measurement_identity_cache,
)
from utils.cloud_sync_impl.spore_mosaic import (  # noqa: F401  (facade re-export)
    _classify_mosaic_build_skips,
    _push_spore_mosaic_for_observation,
    _remote_mosaic_row_exists,
    backfill_public_spore_mosaics,
    diagnose_public_spore_mosaic_gates,
    MOSAIC_STATUS_FAIL_BUILD,
    MOSAIC_STATUS_FAIL_MOSAIC_LOOKUP,
    MOSAIC_STATUS_FAIL_MOSAIC_UPSERT,
    MOSAIC_STATUS_FAIL_NO_MOSAIC_ID,
    MOSAIC_STATUS_FAIL_TILE_CLEANUP,
    MOSAIC_STATUS_FAIL_TILE_INSERT,
    MOSAIC_STATUS_FAIL_UPLOAD,
    MOSAIC_STATUS_GENERATED,
    MOSAIC_STATUS_SKIP_INVALID_GEOMETRY,
    MOSAIC_STATUS_SKIP_MISSING_CALIBRATION,
    MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES,
    MOSAIC_STATUS_SKIP_NO_ELIGIBLE_MEASUREMENTS,
    MOSAIC_STATUS_SKIP_NO_OBSERVATION,
    MOSAIC_STATUS_SKIP_NO_PUBLIC_SPORE_DATA,
    MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES,
    MOSAIC_STATUS_SKIP_RENDER_FAILURE,
    MOSAIC_STATUS_SKIP_UNCHANGED,
)
from utils.cloud_sync_impl.measurement_reconcile import (  # noqa: F401  (facade re-export)
    _reconcile_missing_spore_measurements,
)
from utils.cloud_sync_impl.conflict_plan import (  # noqa: F401  (facade re-export)
    _build_plan_from_automatic_decisions,
    _conflict_plan_local_observation_fingerprint,
    _conflict_plan_remote_observation_fingerprint,
    _conflict_plan_local_image_fingerprint,
    _conflict_plan_remote_image_fingerprint,
    _conflict_plan_local_measurement_fingerprint,
    _conflict_plan_remote_measurement_fingerprint,
    build_conflict_plan_baseline,
    _plan_drift_message,
    _find_local_image_fingerprint,
    _find_remote_image_fingerprint,
    _find_local_measurement_fingerprint,
    _find_remote_measurement_fingerprint,
    _CONFLICT_PLAN_BASELINE_SCHEMA_VERSION,
    _CONFLICT_PLAN_BASELINE_REQUIRED_KEYS,
    _validate_plan_baseline_shape,
    _verify_plan_baseline,
    _validate_plan_identity_state,
    _build_plan_operations,
    _malformed_retry_state,
    _reconcile_verification_pending_op,
    _validate_prior_op_expected_after,
    _require_full_material_expected,
    _EXPECTED_AFTER_MEASUREMENT_MATERIAL_FIELDS,
    _material_image_expected_state,
    _material_measurement_expected_state,
    _intended_after_image_from_local,
    _intended_after_image_from_remote,
    _intended_after_measurement_from_local,
    _intended_after_measurement_from_remote,
    _material_image_current_state,
    _material_measurement_current_state,
    _normalize_observation_field_for_baseline,
    _verify_completed_ops_and_rebase,
    _stable_op_key,
    _plan_dispatch_keys,
    _plan_item_matches_completed_op,
    _build_accepted_asymmetry_from_plan,
    _iso_timestamp_now,
    _mosaic_render_state_unverified,
)
from utils.cloud_sync_impl.conflict_detail import (  # noqa: F401  (facade re-export)
    _CONFLICT_COMPARE_FIELDS,
    _CONFLICT_FIELD_LABELS,
    _image_label,
    _image_kind_noun,
    _pluralize_image_count,
    _IMAGE_METADATA_FIELD_GROUPS,
    _image_metadata_group_for_field,
    _format_image_metadata_field_label,
    _summarize_image_changes,
    _observation_display_name,
    _MEASUREMENT_PRESENTATION_FIELDS,
    get_conflict_detail,
)


# Sporely-py's source_app_version for public.observation_spore_summaries
# rows (Stage D). Set once at app startup via
# ``set_cloud_sync_source_app_version(main.APP_VERSION)`` — kept as a
# module-level slot rather than a direct ``from main import APP_VERSION``
# because ``main`` drags in PySide6 at import time. Consumers should not
# read this directly; call ``_current_source_app_version()``.
_CLOUD_SYNC_SOURCE_APP_VERSION: str | None = None


def set_cloud_sync_source_app_version(version: str | None) -> None:
    """Record the running app version for the summary sync layer.

    Safe to call multiple times; only the last non-empty value sticks.
    """
    global _CLOUD_SYNC_SOURCE_APP_VERSION
    text = str(version or "").strip()
    _CLOUD_SYNC_SOURCE_APP_VERSION = text or None


def _current_source_app_version() -> str | None:
    return _CLOUD_SYNC_SOURCE_APP_VERSION


# Stage M (reference measurement-content v2): the owner feed page size and the
# capability declaration. Imported lazily: the capability module reads the
# reference readers, which must not load at cloud-sync import time.
_REFERENCE_FEED_PAGE_SIZE = 500


def _clear_session_scoped_caches() -> None:
    """Drop account-scoped display caches on sign-in, sign-out or switch."""
    try:
        from database.curated_reference_forks import clear_contributor_label_cache

        clear_contributor_label_cache()
    except Exception:  # never block a credential change
        pass


def _reference_client_capabilities() -> dict:
    from utils.reference_client_capabilities import reference_client_capabilities

    return reference_client_capabilities()


def _reference_accept_snapshot_versions() -> list[int]:
    from utils.reference_client_capabilities import declared_snapshot_versions

    return list(declared_snapshot_versions())


_CLOUD_DEBUG_TIMING = str(os.environ.get('SPORELY_DEBUG_RAW_TIMING') or '').strip().lower() in {'1', 'true', 'yes', 'on'}


def _cloud_timing_log(stage: str, start: float | None, *, detail: str = '') -> None:
    if not _CLOUD_DEBUG_TIMING or start is None:
        return
    elapsed_ms = max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0)
    if detail:
        print(f"[raw-timing] cloud delete {stage}: {elapsed_ms:.1f} ms | {detail}")
    else:
        print(f"[raw-timing] cloud delete {stage}: {elapsed_ms:.1f} ms")

SUPABASE_URL = 'https://zkpjklzfwzefhjluvhfw.supabase.co'
SUPABASE_KEY = 'sb_publishable_nZrERVFN3WR4Aqn2yggc7Q_siAG1TCV'
_UNSET = object()
_SUPABASE_AUTH_TIMEOUT = 30
_SUPABASE_REST_TIMEOUT = 60
_SUPABASE_PROFILE_UPLOAD_TIMEOUT = 60
_SUPABASE_REQUEST_MAX_ATTEMPTS = 4
_SUPABASE_REQUEST_BACKOFF_BASE_SECONDS = 0.5
_SUPABASE_REQUEST_BACKOFF_MAX_SECONDS = 8.0
_CLOUD_LAST_CHILD_SAFETY_PULL_AT_SETTING = 'cloud_last_child_safety_pull_at'
_CLOUD_CHILD_SAFETY_PULL_INTERVAL_HOURS = 24
_CLOUD_MEASUREMENT_RECONCILE_AT_SETTING = 'cloud_measurement_reconcile_at'
_CLOUD_CHILD_CHANGE_CURSOR_SETTING = 'cloud_child_change_cursor'
# Version must be bumped whenever the image cursor semantics change.
# v1 (implicit, no 'v' key): used max(created_at, deleted_at) – no updated_at.
# v2: uses updated_at as the single authoritative timestamp.
_CHILD_CHANGE_CURSOR_VERSION = 2
_PULL_NON_BLOCKING_LOCAL_ONLY_FIELDS = frozenset({
    'ai_selected_service',
    'ai_selected_taxon_id',
    'ai_selected_scientific_name',
    'ai_selected_probability',
    'ai_selected_at',
})
_CLOUD_KEYRING_SERVICE = 'Sporely.Cloud'
_CLOUD_LEGACY_KEYRING_SERVICE = 'MycoLog.Cloud'
_profile_suffix = runtime_profile_scope()
_CLOUD_KEYRING_ACCOUNT = f'password:{_profile_suffix}' if _profile_suffix else 'password'

# Never push: private_comment, ai_state_json, folder_path, cloud_id, sync_status, synced_at
# Stage 3B.2/3B.3: local-only taxonomy-v2 fields — not pushed to Supabase
# until a separate cloud-schema migration is authored. Regression test
# `tests/test_stage_3b_3_cloud_isolation.py` asserts these stay out.
#
# Taxonomy-v2 closeout Stage 2 adds the identity-provenance columns to the
# same set. The cloud learns a desktop identity only through the guarded
# ``set_observation_selected_taxon_v2`` RPC, and that RPC accepts a proven
# Sporely ID only — so pushing provenance columns as ordinary observation
# fields would create a second, ungated identity channel.
_STAGE_3B_LOCAL_ONLY_OBS_FIELDS = frozenset(
    {"sporely_taxon_id", "scientific_name_snapshot", "taxon_rank_snapshot"}
    | set(_TAXON_IDENTITY_COLUMNS)
)
assert not (set(_OBS_PUSH_COLS) & _STAGE_3B_LOCAL_ONLY_OBS_FIELDS), (
    "Stage 3B taxonomy fields must never appear in _OBS_PUSH_COLS. "
    "Cloud schema change is a separate migration."
)


def _cloud_visibility_to_sharing_scope(value: str | None, fallback: str = 'private') -> str:
    """Map Phase 7 cloud visibility back to the local desktop sharing scope."""
    return _normalize_sharing_scope(value, fallback=fallback)


#: Restrictiveness order for the visibility enum, least to most exposed.
#: A "narrower" change moves to a strictly LOWER rank (more restrictive,
#: e.g. public -> private); a "wider" change moves to a strictly HIGHER
#: rank. Used only by the blocked-observation privacy exception below —
#: never a general merge/precedence rule for the field.
_VISIBILITY_RESTRICTIVENESS_RANK = {'private': 0, 'friends': 1, 'public': 2}


def _is_strictly_narrower_visibility(new_value: str, current_value: str) -> bool:
    new_rank = _VISIBILITY_RESTRICTIVENESS_RANK.get(new_value)
    current_rank = _VISIBILITY_RESTRICTIVENESS_RANK.get(current_value)
    if new_rank is None or current_rank is None:
        return False
    return new_rank < current_rank


def _push_narrower_visibility_while_blocked(
    client: 'SporelyCloudClient',
    cloud_id: str,
    local_obs: dict,
    remote: dict | None,
) -> str | None:
    """Let a strictly more restrictive visibility change through a blocked
    observation, and NOTHING else.

    Used only from the Case F no-baseline-identity-contradiction block: the
    rest of the observation (identification, names, any other field) stays
    blocked, un-synced and un-snapshotted, exactly as before. This function
    never marks the observation synced, never clears the conflict-review
    marker, and never stores a baseline — it is a single scoped PATCH of the
    ``visibility`` column alone, so the desktop's own narrowing choice
    cannot be silently lost while the rest of the row awaits review.

    Returns ``None`` when no narrowing change was needed (nothing to do),
    or one of:

    - ``'patched'`` — the narrowing reached the cloud.
    - ``'lost_race'`` — Stage C review round 3: ``remote`` is a snapshot
      fetched early in this sync cycle (``push_all``'s bulk
      ``remote_lookup``), not immediately before this write. The PATCH
      carries an equality precondition on the SAME column
      (``visibility=eq.<remote_visibility>``); a concurrent write that
      changed the column since that earlier fetch makes the precondition
      match zero rows, which PostgREST reports as an ordinary success with
      an empty body, never an error. Zero rows means nothing was
      overwritten — the concurrent value stands.
    - ``'failed'`` — the PATCH itself was rejected (e.g. a privacy-slot
      quota check).

    The caller MUST treat anything other than ``'patched'`` as "the
    narrowing did not reach the cloud" and must not advance sync/baseline
    state as if it had.
    """
    local_visibility = _sharing_scope_to_cloud_visibility(local_obs.get('sharing_scope'))
    remote_visibility = _cloud_visibility_to_sharing_scope(
        (remote or {}).get('visibility') or (remote or {}).get('sharing_scope')
    )
    if not _is_strictly_narrower_visibility(local_visibility, remote_visibility):
        return None
    try:
        updated_rows = client._patch_with_precondition(
            f'observations?id=eq.{cloud_id}&visibility=eq.{remote_visibility}',
            {'visibility': local_visibility},
        )
    except Exception as exc:
        # The scoped narrowing PATCH itself was rejected (e.g. a
        # privacy-slot-limit quota check). This must not be mistaken for —
        # or interfere with — the identity conflict-review flow already
        # recorded for this observation; the caller records the failure
        # through the error-detail-only path so it stays visible instead
        # of only a log line.
        logger.warning(
            "cloud sync: blocked-observation privacy exception failed for cloud %s: %s",
            cloud_id, exc,
        )
        return 'failed'
    if not updated_rows:
        logger.warning(
            "cloud sync: blocked-observation privacy exception lost a race for "
            "cloud %s — visibility changed since this sync cycle fetched it "
            "(expected %r); not overwriting the concurrent value",
            cloud_id, remote_visibility,
        )
        return 'lost_race'
    print(
        f"[cloud_sync] blocked-observation privacy exception: cloud {cloud_id} "
        f"visibility {remote_visibility!r} -> {local_visibility!r} (narrowing only; "
        f"identification stays under review)",
        flush=True,
    )
    return 'patched'


_OBSERVATION_BOOL_FIELDS = {
    'location_public',
    'uncertain',
    'unspontaneous',
    'interesting_comment',
    'is_draft',
}


def _encode_postgrest_filter_value(value: str | None) -> str:
    """Encode filter values for PostgREST query strings.

    Timestamps may contain '+' in timezone offsets, which must be percent-encoded
    inside a URL query or they can be parsed incorrectly.
    """
    return quote(str(value or '').strip(), safe='')


def _stable_storage_key_digest(*parts: object) -> str:
    """32-hex digest of stable, non-time identities for deterministic keys."""
    material = '/'.join(str(part or '').strip() for part in parts)
    return hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]


def _stable_storage_key_extension(source_path: str | Path) -> str:
    suffix = Path(str(source_path or '').strip()).suffix.lower()
    if re.fullmatch(r'\.[a-z0-9]{1,10}', suffix or ''):
        return suffix
    return '.jpg'


def _sanitize_original_storage_filename(source_path: str | Path) -> str:
    path = Path(str(source_path or '').strip())
    raw_name = path.name or 'original'
    stem = re.sub(r'[^A-Za-z0-9._-]+', '_', path.stem).strip('._') or 'original'
    suffix = path.suffix.lower()
    if not re.fullmatch(r'\.[A-Za-z0-9]{1,10}', suffix or ''):
        suffix = ''
    sanitized = f'{stem}{suffix}'
    if sanitized:
        return sanitized
    cleaned_name = re.sub(r'[^A-Za-z0-9._-]+', '_', raw_name).strip('._')
    return cleaned_name or 'original'


_CALIBRATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'created_at',
    'image_storage_path',
    *_CALIBRATION_SYNC_COLS,
)


def _remap_known_local_calibration_path(path: Path) -> Path:
    if path.exists():
        return path
    try:
        from app_identity import app_data_dir, legacy_app_data_dir

        legacy_root = legacy_app_data_dir().resolve()
        current_root = app_data_dir().resolve()
        rel = path.resolve(strict=False).relative_to(legacy_root)
    except Exception:
        return path
    return current_root / rel


def _resolve_local_calibration_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
        remapped = _remap_known_local_calibration_path(raw_path)
        if remapped != raw_path:
            candidates.append(remapped)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_readable_local_file(candidate):
            return candidate
    return None


def _resolve_existing_local_calibration_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
        remapped = _remap_known_local_calibration_path(raw_path)
        if remapped != raw_path:
            candidates.append(remapped)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


def _select_representative_calibration_image_path(calibration: dict | None) -> Path | None:
    record = dict(calibration or {})
    local_image = _resolve_local_calibration_asset_path(record.get('image_filepath'))
    if local_image is not None:
        return local_image

    measurements_json = record.get('measurements_json')
    if isinstance(measurements_json, (dict, list, tuple)):
        loaded = measurements_json
    elif measurements_json:
        try:
            loaded = json.loads(str(measurements_json))
        except Exception:
            loaded = None
    else:
        loaded = None

    if not isinstance(loaded, dict):
        return None

    for entry in loaded.get('images', []):
        if not isinstance(entry, dict):
            continue
        path = _resolve_local_calibration_asset_path(entry.get('path'))
        if path is not None:
            return path
    return None


def _calibration_reference_save_format() -> tuple[str, str, dict, str]:
    if features.check('webp'):
        return 'WEBP', 'image/webp', {'quality': 82, 'method': 4}, '.webp'
    return 'JPEG', 'image/jpeg', {'quality': 88, 'optimize': True}, '.jpg'


def _calibration_reference_image_bytes(path: Path) -> tuple[bytes, str, str]:
    with Image.open(path) as image:
        image = ImageOps.exif_transpose(image)
        if image.mode in {'RGBA', 'LA'} or 'transparency' in image.info:
            rgba = image.convert('RGBA')
            background = Image.new('RGB', rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel('A'))
            image = background
        elif image.mode != 'RGB':
            image = image.convert('RGB')
        image.thumbnail((_CALIBRATION_REFERENCE_MAX_EDGE, _CALIBRATION_REFERENCE_MAX_EDGE), Image.Resampling.LANCZOS)

        format_name, mime_type, save_options, extension = _calibration_reference_save_format()
        buffer = io.BytesIO()
        image.save(buffer, format=format_name, **save_options)
        return buffer.getvalue(), mime_type, extension


def _calibration_reference_storage_key(user_id: str, calibration_uuid: str, extension: str) -> str:
    key = f"{str(user_id).strip()}/{str(calibration_uuid).strip()}/reference{extension}"
    return _normalize_cloud_media_key(key)


def _sanitize_calibration_cache_component(value: str | None, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(text).name).strip("._")
    return cleaned or fallback


def _normalize_calibration_cache_extension(value: str | None, fallback: str = ".jpg") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    if not text.startswith("."):
        text = f".{text}"
    cleaned = re.sub(r"[^a-z0-9.]+", "", text)
    if not cleaned:
        return fallback
    if not cleaned.startswith("."):
        cleaned = f".{cleaned.lstrip('.')}"
    return cleaned if cleaned != "." else fallback


def _calibration_recovery_cache_root() -> Path:
    return app_data_dir() / "cloud_cache" / "calibrations"


def _calibration_recovery_cache_path(
    calibration_uuid: str | None,
    cloud_key: str | None = None,
    desired_extension: str | None = None,
) -> Path:
    uuid_value = _normalize_calibration_uuid(calibration_uuid)
    folder_name = uuid_value or _sanitize_calibration_cache_component(calibration_uuid, "unknown_calibration")
    normalized_key = _normalize_cloud_media_key(cloud_key)
    inferred_extension = Path(normalized_key).suffix if normalized_key else ""
    extension = _normalize_calibration_cache_extension(desired_extension or inferred_extension, ".jpg")
    return _calibration_recovery_cache_root() / folder_name / f"reference{extension}"


def _calibration_reference_recovery_state(calibration: dict | None) -> dict[str, object]:
    record = dict(calibration or {})
    image_filepath = str(record.get("image_filepath") or "").strip() or None
    image_storage_path = _normalize_cloud_media_key(record.get("image_storage_path")) or None
    calibration_uuid = _normalize_calibration_uuid(record.get("calibration_uuid"))
    local_original_path = _resolve_existing_local_calibration_asset_path(image_filepath)
    local_original_exists = local_original_path is not None
    return {
        "calibration_uuid": calibration_uuid,
        "image_filepath": image_filepath,
        "image_storage_path": image_storage_path,
        "local_original_exists": local_original_exists,
        "local_original_missing": bool(image_filepath and not local_original_exists),
        "local_original_path": local_original_path,
        "recovery_available": bool(calibration_uuid and image_storage_path and not local_original_exists),
    }


def download_calibration_reference_to_cache(
    client: "SporelyCloudClient",
    calibration: dict | None,
) -> dict[str, object]:
    """Download a cloud calibration reference image into the local cache.

    This helper is intentionally soft-failing and does not mutate the input
    calibration row or any database records.
    """
    state = _calibration_reference_recovery_state(calibration)
    result = dict(state)
    result.update(
        {
            "status": "skipped",
            "warning": None,
            "cache_path": None,
            "downloaded": False,
        }
    )

    calibration_uuid = state.get("calibration_uuid")
    image_storage_path = state.get("image_storage_path")
    if not calibration_uuid:
        result["status"] = "skipped_invalid_calibration_uuid"
        result["warning"] = "calibration ?: skipped recovery because calibration_uuid is missing or invalid"
        return result

    if state.get("local_original_exists"):
        result["status"] = "skipped_local_original_exists"
        return result

    if not image_storage_path:
        result["status"] = "unavailable_missing_storage_path"
        result["warning"] = (
            f"calibration {calibration_uuid}: skipped recovery because image_storage_path is missing"
        )
        return result

    cache_root = _calibration_recovery_cache_root() / str(calibration_uuid)
    cache_root.mkdir(parents=True, exist_ok=True)
    initial_extension = _normalize_calibration_cache_extension(Path(str(image_storage_path)).suffix or ".jpg")
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=cache_root,
            prefix="reference_",
            suffix=initial_extension,
            delete=False,
        ) as tmp:
            temp_path = Path(tmp.name)

        downloaded_path = Path(client.download_image_file(str(image_storage_path), temp_path))
        if not downloaded_path.exists():
            raise RuntimeError("downloaded calibration reference image was not written")

        detected_extension = _detected_image_extension(downloaded_path)
        final_path = _calibration_recovery_cache_path(
            calibration_uuid,
            str(image_storage_path),
            detected_extension,
        )
        if downloaded_path != final_path:
            downloaded_path.replace(final_path)
            downloaded_path = final_path

        result["status"] = "downloaded_to_cache"
        result["downloaded"] = True
        result["cache_path"] = downloaded_path
        return result
    except Exception as exc:
        detail = str(exc or "").strip() or exc.__class__.__name__
        result["status"] = "download_failed"
        result["warning"] = f"calibration {calibration_uuid}: skipped recovery download ({detail})"
        return result
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


def _new_original_upload_summary(enabled: bool) -> dict[str, int | bool]:
    return {
        "enabled": bool(enabled),
        "uploaded": 0,
        "skipped_disabled": 0,
        "skipped_ineligible": 0,
        "skipped_too_large": 0,
        "failed_uploads": 0,
    }


def _format_original_count(count: int, label: str) -> str:
    total = max(0, int(count))
    return f"{total} {label}"


def format_original_upload_summary(original_summary: dict | None) -> str | None:
    summary = dict(original_summary or {})
    if not bool(summary.get("enabled")):
        return None

    uploaded = max(0, int(summary.get("uploaded") or 0))
    skipped_disabled = max(0, int(summary.get("skipped_disabled") or 0))
    skipped_ineligible = max(0, int(summary.get("skipped_ineligible") or 0))
    skipped_too_large = max(0, int(summary.get("skipped_too_large") or 0))
    failed_uploads = max(0, int(summary.get("failed_uploads") or 0))

    skipped_total = skipped_disabled + skipped_ineligible + skipped_too_large
    if not any((uploaded, skipped_total, failed_uploads)):
        return None

    parts: list[str] = []
    if uploaded:
        parts.append(_format_original_count(uploaded, "uploaded"))
    if skipped_total:
        parts.append(_format_original_count(skipped_total, "skipped"))
    if failed_uploads:
        parts.append(_format_original_count(failed_uploads, "failed"))
    return f"Original uploads: {', '.join(parts)}."


def format_original_recovery_summary(recovery_result: dict | None) -> str | None:
    result = dict(recovery_result or {})
    status = str(result.get("status") or "").strip()
    if not status or status == "skipped_disabled":
        return None
    if status == "downloaded_to_cache":
        return "Original recovery: 1 downloaded."
    if status == "download_failed":
        return "Original recovery: 1 failed."
    if status.startswith("skipped_"):
        return "Original recovery: 1 skipped."
    return None


_IMG_PUSH_COLS = [
    'sort_order', 'image_type', 'micro_category', 'objective_name',
    'calibration_uuid',
    'captured_at',
    'scale_microns_per_pixel', 'resample_scale_factor',
    'mount_medium', 'stain',
    # `sample_type` is now specimen condition only (Not_set / Fresh / Dried).
    # `sample_source` records where the material came from (Spore_print,
    # Hymenium, Stipe, Pileus, Context, Other). Legacy sample_type='Spore_print'
    # rows are backfilled into sample_source at push time — see
    # `_normalize_image_push_sample_fields`.
    'sample_type', 'sample_source',
    'contrast', 'measure_color',
    'crop_mode', 'notes',
    'gps_source', 'storage_path',
    'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
    'ai_crop_source_w', 'ai_crop_source_h', 'ai_crop_is_custom',
]
_IMG_UPLOAD_META_COLS = [
    'upload_mode',
    'source_width',
    'source_height',
    'stored_width',
    'stored_height',
    'stored_bytes',
]

_MEAS_PUSH_COLS = [
    'length_um', 'width_um', 'measurement_type',
    'gallery_rotation',
    'p1_x', 'p1_y', 'p2_x', 'p2_y',
    'p3_x', 'p3_y', 'p4_x', 'p4_y',
    'measured_at',
]

_SETTING_CLOUD_MEDIA_SIGNATURE = "sporely_cloud_media_signature_v1"
_SETTING_LINKED_CLOUD_USER_ID = "linked_cloud_user_id"
_CLEAN_CLOUD_IMAGE_CONVERTER_VERSION = "1"
_REMOTE_SYNC_TIMESTAMP_GRACE_SECONDS = 5.0
_CLOUD_THUMB_MAX_EDGE = 400
_CALIBRATION_REFERENCE_MAX_EDGE = 2048

# Fields whose change forces re-encoding of the uploaded WebP. AI crop is
# stored as descriptive metadata alongside the image — _prepare_cloud_image_upload_file
# does NOT apply it to the uploaded bytes — so a change to any AI crop field
# only patches the cloud row and must NOT force prepare_images_cb / WebP prep.
# Everything in this set must actually change the encoded bytes.
_LOCAL_MEDIA_PREP_RENDER_AFFECTING_IMAGE_FIELDS = frozenset()

# Descriptive image-row fields we may sync via a metadata-only patch (no WebP
# encoded, no bytes uploaded). Used both to classify sync changes and to
# document what the "metadata-only image sync" decision covers.
_IMAGE_METADATA_ONLY_FIELDS = frozenset({
    'captured_at',
    'ai_crop_x1',
    'ai_crop_y1',
    'ai_crop_x2',
    'ai_crop_y2',
    'ai_crop_source_w',
    'ai_crop_source_h',
    'ai_crop_is_custom',
    'crop_mode',
    'notes',
    'calibration_uuid',
    'sort_order',
    'image_type',
    'micro_category',
    'objective_name',
    'scale_microns_per_pixel',
    'mount_medium',
    'stain',
    'sample_type',
    'sample_source',
    'contrast',
    'measure_color',
    'gps_source',
})

_LOCAL_MEDIA_PREP_RENDER_AFFECTING_TOP_LEVEL_FIELDS = frozenset({
    'render_version',
    'cloud_image_size_mode',
})


PreparedImagesCallback = Callable[[dict, ProgressCallback | None], tuple[list[dict], object | None, list[str]]]


ACCOUNT_MISMATCH_MESSAGE = (
    "This local database is permanently linked to another Sporely Cloud account. "
    "Please switch to the correct OS user profile, or use the 'Reset Cloud Sync' "
    "tool in Settings to migrate your data to a new account."
)
FREE_TIER_PRIVACY_SLOT_LIMIT = 20
_IMAGE_TOO_LARGE_FOR_PLAN_FIRST_LINE_RE = re.compile(
    r"(?i)^\s*(?:Image(?:\s+is)?\s+too\s+large(?:\s+for\s+your\s+plan)?(?:\.\s*Make it smaller or upgrade to Pro\.)?|Image\s+too\s+large\s+for\s+plan)\s*$"
)


def cloud_observation_uses_privacy_slot(observation: dict | None) -> bool:
    """Mirror of the server privacy-slot trigger (sporely-web
    20260626110000): a non-draft observation that is not public, or whose
    location precision is fuzzed/region/hidden. Missing values coalesce to
    not-draft, 'public' and 'exact' as on the server."""
    record = dict(observation or {})
    if _normalize_observation_bool_value(record.get('is_draft'), default=False):
        return False
    sharing_scope = _normalize_sharing_scope(
        record.get('visibility') or record.get('sharing_scope'),
        fallback='public',
    )
    location_precision = ObservationDB._normalize_location_precision(record.get('location_precision'))
    return sharing_scope != 'public' or location_precision in {'fuzzed', 'region', 'hidden'}


def count_cloud_privacy_slots(remote_observations: list[dict] | None) -> int:
    return sum(1 for row in list(remote_observations or []) if cloud_observation_uses_privacy_slot(row))


def _parse_postgrest_content_range_total(content_range: str | None) -> int | None:
    text = str(content_range or '').strip()
    if not text or '/' not in text:
        return None
    total_text = text.rsplit('/', 1)[-1].strip()
    if total_text == '*':
        return None
    try:
        return int(total_text)
    except (TypeError, ValueError):
        return None


def fetch_cloud_usage_summary(client) -> dict:
    profile = normalize_cloud_plan_profile({})
    profile_loaded = False
    privacy_count_loaded = False
    privacy_loaded = False
    privacy_slots_used: int | None = None
    profile_error = ''
    privacy_error = ''
    if client is not None:
        try:
            profile = normalize_cloud_plan_profile(client.fetch_cloud_plan_profile())
            profile_loaded = True
        except Exception as exc:
            profile = normalize_cloud_plan_profile({})
            profile_error = format_cloud_sync_error_details(exc)
            if profile_error:
                logger.warning(
                    "Cloud plan profile lookup failed for user_id=%s: %s",
                    getattr(client, 'user_id', ''),
                    profile_error,
                )
        try:
            count_method = getattr(client, 'count_remote_privacy_slots', None)
            if callable(count_method):
                privacy_slots_used = int(count_method())
                privacy_count_loaded = True
        except Exception as exc:
            privacy_slots_used = None
            privacy_error = format_cloud_sync_error_details(exc)
            if privacy_error:
                logger.warning(
                    "Cloud privacy slot count failed for user_id=%s: %s",
                    getattr(client, 'user_id', ''),
                    privacy_error,
                )
    has_pro_access = bool(profile.get('has_pro_access') or str(profile.get('cloud_plan') or '').strip().lower() == 'pro')
    privacy_slots_limit = None if has_pro_access else FREE_TIER_PRIVACY_SLOT_LIMIT
    privacy_loaded = bool(profile_loaded and privacy_count_loaded)
    if not privacy_loaded:
        privacy_slots_used = None
    privacy_slots_available = None if privacy_slots_limit is None or not privacy_loaded else max(
        0,
        int(privacy_slots_limit) - int(privacy_slots_used),
    )
    error_messages = []
    if profile_error:
        error_messages.append(f"Profile lookup: {profile_error}")
    if privacy_error:
        error_messages.append(f"Private slot count: {privacy_error}")
    summary = dict(profile)
    summary.update({
        'privacy_slots_used': privacy_slots_used,
        'privacySlotsUsed': privacy_slots_used,
        'privacy_slot_count': privacy_slots_used,
        'privacySlotCount': privacy_slots_used,
        'privacy_slots_limit': privacy_slots_limit,
        'privacySlotsLimit': privacy_slots_limit,
        'privacy_slots_available': privacy_slots_available,
        'privacySlotsAvailable': privacy_slots_available,
        'cloud_profile_loaded': profile_loaded,
        'cloudProfileLoaded': profile_loaded,
        'cloud_privacy_usage_loaded': privacy_loaded,
        'cloudPrivacyUsageLoaded': privacy_loaded,
        'cloud_usage_loaded': bool(profile_loaded and privacy_loaded),
        'cloudUsageLoaded': bool(profile_loaded and privacy_loaded),
        'cloud_profile_error': profile_error or None,
        'cloudProfileError': profile_error or None,
        'cloud_privacy_usage_error': privacy_error or None,
        'cloudPrivacyUsageError': privacy_error or None,
        'cloud_usage_error': "\n".join(error_messages),
        'cloudUsageError': "\n".join(error_messages),
        'cloud_usage_error_messages': error_messages,
        'cloudUsageErrorMessages': error_messages,
    })
    return summary


# Hints that identify a *terminal* refresh-token invalidation coming from
# Supabase's refresh endpoint.  Anything matching this list means the
# session cannot be resumed and the user must sign in again.  A plain
# 401 or an expired-JWT hint is NOT enough — those are recoverable by
# refreshing.
_CLOUD_REAUTH_REQUIRED_HINTS = (
    'invalid_grant',
    'invalid refresh token',
    'refresh token not found',
    'refresh_token_not_found',
    'refresh_token_already_used',
)


def is_cloud_reauth_required_error(error) -> bool:
    """Strict classification: is this error terminal for the current session?

    Returns True only when we can prove the stored refresh token itself
    is dead — e.g. the refresh endpoint returned ``invalid_grant`` — so
    the UI can prompt the user to sign in again.  A wrapper such as
    ``CloudTemporarilyUnavailableError`` chained from a generic ``"auth
    refresh failed"`` string is NOT sufficient: that shape can result
    from a rotation race or a transient Supabase blip, and treating it
    as terminal would wipe a still-valid refresh token on next restart.
    """
    if isinstance(error, CloudReauthRequiredError):
        return True
    seen: set[int] = set()
    value = error
    while value is not None:
        if isinstance(value, CloudReauthRequiredError):
            return True
        try:
            marker = id(value)
        except Exception:
            marker = None
        if marker is not None:
            if marker in seen:
                break
            seen.add(marker)
        value = getattr(value, '__cause__', None) or getattr(value, '__context__', None)
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    return any(hint in haystack for hint in _CLOUD_REAUTH_REQUIRED_HINTS)


def _sleep_supabase_backoff(attempt: int) -> None:
    delay = min(
        _SUPABASE_REQUEST_BACKOFF_MAX_SECONDS,
        _SUPABASE_REQUEST_BACKOFF_BASE_SECONDS * (2 ** max(0, int(attempt))),
    )
    if delay <= 0:
        return
    time.sleep(random.uniform(0.0, delay))


def _sleep_supabase_retry(response: requests.Response, attempt: int) -> None:
    """Honor bounded Retry-After seconds, otherwise use jittered backoff."""
    try:
        retry_after = float(response.headers.get('Retry-After', ''))
    except (AttributeError, TypeError, ValueError):
        retry_after = 0.0
    if retry_after > 0:
        time.sleep(min(60.0, retry_after))
        return
    _sleep_supabase_backoff(attempt)


def _request_exception_is_transient(error: Exception) -> bool:
    if isinstance(error, (requests.Timeout, requests.ConnectionError)):
        return True
    _, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    return any(hint in haystack for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)


def _response_indicates_transient_supabase_error(response: requests.Response) -> bool:
    try:
        status_code = int(getattr(response, 'status_code', 0) or 0)
    except Exception:
        status_code = 0
    if status_code in _SUPABASE_TRANSIENT_STATUS_CODES:
        return True
    try:
        text = str(getattr(response, 'text', '') or '').strip().lower()
    except Exception:
        text = ''
    if not text:
        return False
    return any(hint in text for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)


def _response_indicates_auth_error(response: requests.Response) -> bool:
    try:
        status_code = int(getattr(response, 'status_code', 0) or 0)
    except Exception:
        status_code = 0
    if status_code == 401:
        return True
    # 403 is intentionally excluded: PostgREST/Supabase return 403 for
    # RLS denials, which are authorization failures and must not trigger
    # a token refresh or session wipe.
    try:
        return is_cloud_auth_error(getattr(response, 'text', ''))
    except Exception:
        return False


def _request_with_transient_retry(
    request_callable,
    method: str,
    url: str,
    *,
    refresh_on_auth_error: bool = False,
    refresh_callback: Callable[[], bool] | None = None,
    **kwargs,
):
    last_response: requests.Response | None = None
    refreshed = False
    for attempt in range(_SUPABASE_REQUEST_MAX_ATTEMPTS):
        try:
            response = request_callable(method, url, **kwargs)
        except Exception as exc:
            if isinstance(exc, requests.RequestException) and _request_exception_is_transient(exc):
                if attempt < _SUPABASE_REQUEST_MAX_ATTEMPTS - 1:
                    _sleep_supabase_backoff(attempt)
                    continue
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            raise

        last_response = response
        if getattr(response, 'ok', False):
            return response

        if refresh_on_auth_error and not refreshed and _response_indicates_auth_error(response):
            refreshed = True
            try:
                refreshed_ok = bool(refresh_callback()) if callable(refresh_callback) else False
            except CloudTemporarilyUnavailableError:
                raise
            except CloudReauthRequiredError:
                # Propagate untouched — this is the only way callers can
                # tell that the refresh token itself is dead.
                raise
            except Exception as exc:
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            if refreshed_ok:
                continue
            raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from CloudSyncError(
                f'{method} {url} status={getattr(response, "status_code", "")}: refresh unavailable'
            )

        if _response_indicates_transient_supabase_error(response):
            if attempt < _SUPABASE_REQUEST_MAX_ATTEMPTS - 1:
                _sleep_supabase_retry(response, attempt)
                continue
            raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from CloudSyncError(
                f'{method} {url} status={getattr(response, "status_code", "")}: {getattr(response, "text", "")}'
            )
        return response

    if last_response is not None:
        return last_response
    raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE)


def _normalize_cloud_user_id(value: str | None) -> str:
    return str(value or '').strip()


def _decode_jwt_subject(access_token: str | None) -> str:
    token = str(access_token or '').strip()
    parts = token.split('.')
    if len(parts) < 2:
        return ''
    payload = parts[1]
    padding = '=' * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode((payload + padding).encode('ascii')).decode('utf-8'))
    except Exception:
        return ''
    return _normalize_cloud_user_id(data.get('sub') if isinstance(data, dict) else None)


def _decode_jwt_expiry(access_token: str | None) -> int | None:
    """Return the ``exp`` claim (unix seconds) of *access_token* or None.

    None signals the token is either missing or not decodable — callers
    should treat that as "unknown expiry" rather than "expired".
    """
    token = str(access_token or '').strip()
    parts = token.split('.')
    if len(parts) < 2:
        return None
    payload = parts[1]
    padding = '=' * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode((payload + padding).encode('ascii')).decode('utf-8'))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    exp = data.get('exp')
    try:
        return int(exp) if exp is not None else None
    except Exception:
        return None


# Number of seconds before ``exp`` at which we treat a JWT as "about to
# expire" and refresh proactively.  Supabase access tokens are typically
# 60 minutes, so 5 minutes gives comfortable headroom without spamming
# the refresh endpoint on healthy sessions.
_CLOUD_JWT_EXPIRY_LEEWAY_SECONDS = 5 * 60


def _jwt_expires_soon(
    access_token: str | None,
    *,
    leeway_seconds: int = _CLOUD_JWT_EXPIRY_LEEWAY_SECONDS,
    now: float | None = None,
) -> bool:
    """True when *access_token* has expired or is within *leeway_seconds*.

    An undecodable token returns False here — callers rely on a normal
    401 to detect death for tokens whose expiry we can't inspect.
    """
    exp = _decode_jwt_expiry(access_token)
    if exp is None:
        return False
    current = float(now) if now is not None else time.time()
    return exp <= current + max(0, int(leeway_seconds))


def _read_current_cloud_session_settings() -> tuple[str | None, str | None, str | None]:
    """Snapshot the current on-disk session tokens.

    Returns a triple ``(access_token, user_id, refresh_token)`` where
    each element is either a non-empty string or None.  Reading through
    a single helper keeps every call site symmetric with respect to
    ``get_app_settings()`` monkeypatches in tests.
    """
    settings = get_app_settings()
    access_raw = str(settings.get('cloud_access_token') or '').strip()
    user_raw = _normalize_cloud_user_id(settings.get('cloud_user_id'))
    refresh_raw = str(settings.get('cloud_refresh_token') or '').strip()
    return (
        access_raw or None,
        user_raw or None,
        refresh_raw or None,
    )


# Serializes refresh attempts across every :class:`SporelyCloudClient`
# instance in this process.  Supabase refresh tokens are single-use with
# reuse detection, so two workers racing to refresh with the same token
# is a common cause of spurious "invalid_grant" errors.  Holding this
# lock while re-reading settings, choosing the newest refresh token,
# calling the refresh endpoint, and persisting the result turns that
# race into a wait — the second thread arrives after the first has
# already saved rotated tokens and simply adopts them.
_CLOUD_REFRESH_LOCK = threading.RLock()


def _settings_session_is_compatible(
    client_user_id: str | None,
    settings_user_id: str | None,
    settings_access_token: str | None,
) -> bool:
    """Return True when on-disk session tokens are safe to adopt.

    The rules mirror the Stage-4 spec:

    * A client with no identity yet (``client_user_id`` empty/None) can
      adopt anything — this is the fresh-startup / just-signed-in shape.
    * If both the client and settings carry an explicit ``user_id``,
      they must match.
    * If settings does not carry a ``user_id`` but does carry an
      access token, we accept adoption iff the token's ``sub`` claim
      decodes to the client's user id.
    * If settings carries neither a matching ``user_id`` nor a
      decodable access-token subject, and the client already has an
      identity, we refuse — we cannot prove ownership.
    * If settings is entirely empty (no user id and no access token),
      there is nothing to adopt and nothing to conflict with; the
      caller may proceed with its own tokens.
    """
    client_id = _normalize_cloud_user_id(client_user_id) or None
    if not client_id:
        return True
    settings_id = _normalize_cloud_user_id(settings_user_id) or None
    if settings_id:
        return settings_id == client_id
    # No settings user id.  If settings holds no access token either,
    # there is no on-disk session to conflict with — treat as
    # compatible so the caller can proceed with its own refresh token.
    token_text = str(settings_access_token or '').strip()
    if not token_text:
        return True
    subject = _decode_jwt_subject(token_text)
    if not subject:
        # Access token present but its subject is undecodable — refuse
        # to adopt because we cannot verify ownership.
        return False
    return subject == client_id


def _load_linked_cloud_user_id() -> str:
    return _normalize_cloud_user_id(get_app_settings().get(_SETTING_LINKED_CLOUD_USER_ID))


def _save_linked_cloud_user_id(user_id: str) -> None:
    normalized = _normalize_cloud_user_id(user_id)
    if normalized:
        update_app_settings({_SETTING_LINKED_CLOUD_USER_ID: normalized})


def ensure_database_linked_to_cloud_user(client: "SporelyCloudClient") -> str:
    """Bind this local DB to the active cloud account, or reject a mismatch."""
    current_user_id = ""
    token_user_id = ""
    try:
        token_user_id = _normalize_cloud_user_id(_decode_jwt_subject(getattr(client, 'access_token', None)))
    except Exception:
        token_user_id = ""
    if token_user_id:
        current_user_id = token_user_id
        try:
            if getattr(client, 'user_id', '') != token_user_id:
                client.user_id = token_user_id
        except Exception:
            pass
    if not current_user_id:
        current_user_id = _normalize_cloud_user_id(getattr(client, 'user_id', ''))
    if not current_user_id and hasattr(client, 'fetch_current_user_id'):
        current_user_id = _normalize_cloud_user_id(client.fetch_current_user_id())
    if not current_user_id:
        raise CloudSyncError("Could not verify the active Sporely Cloud account before syncing.")
    linked_user_id = _load_linked_cloud_user_id()
    if not linked_user_id:
        _save_linked_cloud_user_id(current_user_id)
        return current_user_id
    if linked_user_id != current_user_id:
        raise AccountMismatchError(ACCOUNT_MISMATCH_MESSAGE)
    return current_user_id


def _coords_match(
    local_lat, local_lon, baseline_lat, baseline_lon,
) -> bool:
    """Return True when local coords equal the sync baseline within float tolerance.

    Uses the same tolerances as observation float diff detection so harmless
    JSON serialization drift does not spuriously flag a coordinate change.
    """
    left_lat = _normalize_observation_float_value(local_lat)
    left_lon = _normalize_observation_float_value(local_lon)
    right_lat = _normalize_observation_float_value(baseline_lat)
    right_lon = _normalize_observation_float_value(baseline_lon)
    if left_lat is None and right_lat is None and left_lon is None and right_lon is None:
        return True
    if (left_lat is None) != (right_lat is None) or (left_lon is None) != (right_lon is None):
        return False
    return math.isclose(
        float(left_lat), float(right_lat),
        rel_tol=_OBSERVATION_FLOAT_REL_TOL,
        abs_tol=_OBSERVATION_GPS_ABS_TOL,
    ) and math.isclose(
        float(left_lon), float(right_lon),
        rel_tol=_OBSERVATION_FLOAT_REL_TOL,
        abs_tol=_OBSERVATION_GPS_ABS_TOL,
    )


def _shape_geography_patch_payload(
    payload: dict,
    obs: dict | None,
    cloud_id: str | None,
) -> None:
    """Enforce the coord-change / preserve-only rules for country_code + region_id.

    Semantics (see spec §4):
    - field absent  → cloud value is preserved
    - field = null  → cloud value is explicitly cleared
    - field = code  → cloud value is replaced

    Rules applied here for an existing (PATCH) observation:

    * region_id is preserve-only on desktop sync. When coordinates are
      unchanged we never send it. When coordinates changed we send NULL so
      the stale cloud region is dropped.
    * country_code with unchanged coords + no local value → omit key
      (do not clobber the cloud value; may be a stale offline sync).
    * country_code with unchanged coords + valid local code → send code.
    * country_code with changed coords + valid geocode → send new code.
    * country_code with changed coords + no valid code → send NULL.

    The baseline for coord comparison is the stored cloud snapshot for the
    linked cloud observation. When no snapshot exists yet (first push after
    linking) we fall back to omitting the geography keys — the cloud row
    is authoritative and will keep whatever it already holds.
    """
    local_row = dict(obs or {})
    normalized_country = normalize_country_code(local_row.get('country_code'))

    cloud_key = str(cloud_id or '').strip()
    snapshot = None
    baseline_lat = None
    baseline_lon = None
    if cloud_key:
        raw_snapshot = _load_cloud_observation_snapshot(cloud_key)
        snapshot = _parse_cloud_observation_snapshot(raw_snapshot) if raw_snapshot else None
    if snapshot:
        baseline_obs = snapshot.get('observation') or {}
        baseline_lat = baseline_obs.get('gps_latitude')
        baseline_lon = baseline_obs.get('gps_longitude')
    else:
        # No baseline available: treat as coords unchanged to preserve cloud
        # geography (safer than clobbering with a stale local value).
        baseline_lat = local_row.get('gps_latitude')
        baseline_lon = local_row.get('gps_longitude')

    coords_unchanged = _coords_match(
        local_row.get('gps_latitude'),
        local_row.get('gps_longitude'),
        baseline_lat,
        baseline_lon,
    )

    if coords_unchanged:
        # Preserve cloud region_id (never sent) and skip country if local is empty.
        payload.pop('region_id', None)
        if normalized_country is None:
            payload.pop('country_code', None)
        else:
            payload['country_code'] = normalized_country
    else:
        # Coordinates changed. Region tied to old coords is stale; clear it.
        payload['region_id'] = None
        payload['country_code'] = normalized_country  # may be None


def _normalize_observation_sync_field(field: str) -> str:
    normalized = str(field or '').strip()
    if normalized in {'visibility', 'sharing_scope'}:
        return 'sharing_scope'
    return normalized


def _non_blocking_local_only_fields(field_changes: dict) -> frozenset[str]:
    if 'local_payload' not in field_changes:
        return _PULL_NON_BLOCKING_LOCAL_ONLY_FIELDS
    local_payload = dict(field_changes.get('local_payload') or {})
    identification_is_empty = not any(
        str(local_payload.get(field) or '').strip()
        for field in ('genus', 'species', 'common_name', 'species_guess')
    )
    if identification_is_empty:
        return frozenset()
    return _PULL_NON_BLOCKING_LOCAL_ONLY_FIELDS


def _remaining_local_changes_after_remote_merge(
    field_changes: dict,
    *,
    local_media_changed: bool,
) -> bool:
    non_blocking_fields = _non_blocking_local_only_fields(field_changes)
    blocking_local_only_fields = {
        str(field or '').strip()
        for field in (field_changes.get('local_only_fields') or [])
        if str(field or '').strip() not in non_blocking_fields
    }
    return bool(blocking_local_only_fields or field_changes.get('conflict_fields') or local_media_changed)


def _format_review_needed_error(local_id: int, cloud_id: str, reasons: list[str] | None = None) -> str:
    reason_text = ', '.join(str(reason or '').strip() for reason in (reasons or []) if str(reason or '').strip())
    base = (
        f"cloud {str(cloud_id or '').strip() or '?'}: needs review before applying remaining "
        f"cloud changes to local observation {int(local_id)}"
    )
    return f'{base} ({reason_text})' if reason_text else base


# Marker persisted on `observations.sync_blocked_reason` when push_all's
# preflight detects an unresolved three-way conflict. The observation stays
# dirty; a subsequent sync re-evaluates and clears the marker automatically
# once the divergence is gone (either the user resolved it or an incoming pull
# reconciled the state).
CONFLICT_REVIEW_PENDING_MARKER = 'conflict_review_pending'


def _set_observation_conflict_review_pending(local_id: int) -> None:
    """Mark an observation blocked on user review while keeping it dirty."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='dirty',
            sync_blocked_reason=CONFLICT_REVIEW_PENDING_MARKER,
            sync_blocked_at=datetime.now(timezone.utc).isoformat(),
        )
        conn.commit()
    finally:
        conn.close()


def _clear_observation_conflict_review_pending(local_id: int) -> None:
    """Clear the conflict-review marker without disturbing other sync error state."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        columns = _table_columns_for_conflict_marker(cursor)
        if 'sync_blocked_reason' not in columns:
            return
        # Only clear when the current marker is our review flag so we do not
        # step on privacy/plan blocked reasons written by other paths.
        cursor.execute(
            "UPDATE observations SET sync_blocked_reason = NULL, sync_blocked_at = NULL "
            "WHERE id = ? AND sync_blocked_reason = ?",
            (int(local_id), CONFLICT_REVIEW_PENDING_MARKER),
        )
        conn.commit()
    finally:
        conn.close()


def _table_columns_for_conflict_marker(cursor) -> set[str]:
    try:
        cursor.execute("PRAGMA table_info(observations)")
        return {str(row[1] or '') for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return set()


def _clear_observation_dirty_if_no_real_changes(local_id: int, cloud_id: str) -> bool:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        return False
    if _local_has_real_changes_since_snapshot(local_obs, cloud_id):
        return False
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='synced',
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()
    return True


def _cloud_media_signature() -> str:
    """Return clean cloud converter identity, excluding external publish state."""
    snapshot = {
        'converter_version': _CLEAN_CLOUD_IMAGE_CONVERTER_VERSION,
        'format': 'webp',
        'upload_mode': 'full',
    }
    return json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(',', ':'))


# RETIRED (2026-08-19 mass microscope upload): the observation-level
# initialization sentinel. Once set, images imported later never received a
# default and — being absent from the excluded set — looked explicitly
# checked. No code reads or writes this key anymore; existing rows in user
# databases are inert. Storage intent initialization is now recorded per
# image in the ledger key below.
_CLOUD_IMAGE_STORAGE_LEGACY_SENTINEL_PREFIX = "sporely_cloud_image_storage_initialized_"


def _cloud_child_safety_pull_due(now: datetime | None = None) -> tuple[bool, str | None]:
    """Return whether the periodic child-reconciliation backstop is due.

    The 24h TTL safety pull is defense-in-depth: it catches anything that falls
    outside the updated_at cursor's coverage — e.g. hard-deleted rows that leave
    no updated_at trace, measurement edge-cases where image_id mapping may differ,
    or any future schema event the cursor does not track.  It is NOT the primary
    detection mechanism; the cursor probe handles the common case cheaply.
    """
    raw_last = get_app_settings().get(_CLOUD_LAST_CHILD_SAFETY_PULL_AT_SETTING)
    last_text = str(raw_last or '').strip() or None
    if last_text is None:
        return True, None

    last_at = _parse_sync_timestamp(last_text)
    if last_at is None:
        return True, last_text

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    stale_before = current.astimezone(timezone.utc) - timedelta(
        hours=_CLOUD_CHILD_SAFETY_PULL_INTERVAL_HOURS
    )
    return last_at <= stale_before, last_text


def _load_child_change_cursor() -> dict | None:
    """Return stored child-change cursor or None if missing/old-format (bootstrap needed).

    Returns None for:
    - missing key
    - parse failures
    - cursors without a matching 'v' version marker (format/semantic change —
      treat conservatively to avoid missing metadata updates that the old
      created_at/deleted_at cursor never tracked).
    """
    raw = get_app_settings().get(_CLOUD_CHILD_CHANGE_CURSOR_SETTING)
    if not raw:
        return None
    try:
        data = json.loads(str(raw)) if isinstance(raw, str) else raw
        if (
            isinstance(data, dict)
            and data.get('v') == _CHILD_CHANGE_CURSOR_VERSION
            and isinstance(data.get('images'), dict)
            and isinstance(data.get('measurements'), dict)
            and 'ts' in data['images']
            and 'id' in data['images']
            and 'ts' in data['measurements']
            and 'id' in data['measurements']
        ):
            return data
    except Exception:
        pass
    return None


def _store_child_change_cursor(cursor: dict) -> None:
    """Persist child-change cursor to app_settings (always written with version marker)."""
    payload = dict(cursor)
    payload['v'] = _CHILD_CHANGE_CURSOR_VERSION
    update_app_settings({_CLOUD_CHILD_CHANGE_CURSOR_SETTING: json.dumps(payload)})


def _child_change_cursor_id_key(rid) -> tuple:
    """Ordering key for child-change cursor row ids.

    Cloud row ids are numeric (bigint), but the cursor stores them as strings.
    Plain string comparison misorders across digit-length boundaries
    ('10000' < '9999'), which would silently drop genuinely new rows from the
    strict tuple filter and pick the wrong maximum when advancing the cursor.
    Empty (no position yet) sorts before every id; non-numeric ids (defensive)
    sort after all numeric ids, lexicographically — matching nothing the server
    currently produces but keeping the ordering total.
    """
    s = str(rid or '').strip()
    if not s:
        return (-1, 0, '')
    if s.isdigit():
        return (0, len(s), s)
    return (1, 0, s)


_PUSH_SUMMARY_REMOTE_LIST_REUSE_SAFE_POSITIVE_KEYS = frozenset({
    'calibrations_skipped_noop',
    'calibration_remote_lookups',
})


def _push_phase_requires_remote_observation_refresh(
    calibration_push_result: dict | None,
    push_result: dict | None,
) -> bool:
    """Return False only when push results prove no cloud mutation occurred."""
    calibration_data = dict(calibration_push_result or {})
    push_data = dict(push_result or {})
    if calibration_data.get('errors') or push_data.get('errors'):
        return True

    required_zero_values = (
        calibration_data.get('pushed'),
        push_data.get('pushed'),
        push_data.get('total'),
    )
    try:
        if any(int(value) != 0 for value in required_zero_values):
            return True
    except (TypeError, ValueError):
        return True

    for key in ('spore_measurement_reconcile', 'spore_summary_reconcile'):
        reconciliation = push_data.get(key)
        if not isinstance(reconciliation, dict):
            return True
        try:
            if int(reconciliation.get('attempted')) != 0:
                return True
        except (TypeError, ValueError):
            return True

    summary = push_data.get('sync_summary')
    if not isinstance(summary, dict):
        return True
    for key, value in summary.items():
        if key in _PUSH_SUMMARY_REMOTE_LIST_REUSE_SAFE_POSITIVE_KEYS:
            continue
        if _sync_summary_value(summary, key) > 0:
            return True
    return False


def sync_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    materialize_remote_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
    full_pull: bool = True,
    child_safety_pull: bool = False,
    pull_only: bool = False,
) -> dict:
    """Run a full bidirectional sync: push local changes then pull remote ones.

    ``full_pull=False`` enables the "no-op fast path" appropriate for background
    / Refresh sync: the pull step only touches observations whose remote
    ``updated_at`` is newer than the local ``synced_at``, skipping the bulk
    image-metadata + measurement prefetches that dominate no-op sync time.
    Spore-summary reconciliation across all synced observations is also
    skipped — per-observation summaries still sync inside push for locally
    dirty rows. ``child_safety_pull=True`` periodically upgrades only the pull
    phase to a metadata-only deep reconciliation so child changes cannot stay
    hidden indefinitely when a parent timestamp was not bumped.
    """
    profiler = CloudSyncProfiler() if _cloud_sync_profile_enabled() else None
    profile_token = None
    if profiler is not None:
        profile_token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
    sync_summary = _new_sync_summary()
    summary_token = _CLOUD_SYNC_SUMMARY_CONTEXT.set(sync_summary)
    progress_trace = {'start': _cloud_sync_perf_counter(), 'last_t': None, 'last_msg': None}
    progress_trace_token = _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.set(progress_trace)

    sync_error: Exception | None = None
    result: dict | None = None
    try:
        # Safety check: ensure this DB belongs to the current user
        with _cloud_sync_phase_scope(profiler, 'ensure_database_linked_to_cloud_user'):
            ensure_database_linked_to_cloud_user(client)
        try:
            repair_legacy_location_precision()
        except Exception:
            logger.exception('Location precision repair failed')

        if pull_only:
            # Download from Cloud: strict cloud → desktop. Skip both push
            # phases entirely and wrap the client so any accidental write
            # fails closed. This branch shares the pull implementation with
            # normal sync — no parallel engine.
            pull_only_client = (
                client
                if isinstance(client, PullOnlyCloudClient)
                else PullOnlyCloudClient(client)
            )
            progress_state: dict = {'done': 0, 'total': 0}
            _set_progress_phase(progress_state, 'auth')
            _emit_progress(progress_cb, "Connecting to Sporely Cloud...", progress_state)
            _emit_progress(progress_cb, "Loading cloud observations…", progress_state)
            remote_obs = pull_only_client.list_remote_observations()
            with _cloud_sync_phase_scope(profiler, 'pull_all'):
                pull_result = pull_all(
                    pull_only_client,
                    progress_cb=progress_cb,
                    progress_state=progress_state,
                    remote_obs=remote_obs,
                    sync_calibrations=True,
                    materialize_remote_images=materialize_remote_images,
                    sync_images=sync_images,
                    full_pull=True,
                    pull_only=True,
                )
            # Import lazily: the typed adapter depends on cloud-sync's public
            # error classes, so a module-level import would create a cycle.
            from utils.reference_cloud_sync import (
                merge_reference_sync_result,
                sync_reference_library,
            )

            with _cloud_sync_phase_scope(profiler, 'reference_sync'):
                reference_result = sync_reference_library(
                    pull_only_client, pull_only=True
                )
            _set_progress_phase(progress_state, 'finalize', phase_total=1)
            _advance_progress(progress_state, 1)
            _emit_progress(progress_cb, "Finalizing cloud sync…", progress_state)
            summary_snapshot = dict(sync_summary)
            result = {
                'pushed': 0,
                'pulled': pull_result.get('pulled', 0),
                'calibrations_pushed': 0,
                'calibrations_pulled': pull_result.get('calibrations_pulled', 0),
                'errors': list(pull_result.get('errors') or []),
                'deleted_remote': pull_result.get('deleted_remote', []),
                'sync_summary': summary_snapshot,
                'pull_only': True,
                'images_downloaded': int(summary_snapshot.get('remote_media_materializations', 0) or 0),
                'observations_updated': pull_result.get('pulled', 0),
                # Writes that actually reached the network. Under pull-only
                # this is always 0 — the wrapper fails closed on every writer
                # and source-level gates prevent even the wrapper from being
                # reached in the normal flow.
                'cloud_writes_completed': 0,
                # Writes the wrapper blocked (defence in depth). Should also
                # be empty once source-level suppression is in place; any
                # entry here indicates a new pull-side write path that needs
                # to be gated at its source.
                'blocked_write_attempts': list(pull_only_client.write_attempts),
            }
            merge_reference_sync_result(result, reference_result)
            return result

        # Progress state feeds a weighted global bar: each phase maps onto a
        # fixed percentage range so the bar never sits at 99% while real work
        # is still running. Sub-functions update the per-phase (done, total)
        # counters; _emit_progress projects that onto the 0–100 range.
        progress_state: dict = {'done': 0, 'total': 0}
        _set_progress_phase(progress_state, 'auth')
        _emit_progress(progress_cb, "Connecting to Sporely Cloud...", progress_state)

        # Pre-fetch remote metadata once to reuse in both phases
        _emit_progress(progress_cb, "Loading cloud observations…", progress_state)
        with _cloud_sync_phase_scope(profiler, 'list_remote_observations'):
            remote_obs = client.list_remote_observations()
        _emit_progress(progress_cb, "Loading cloud calibrations…", progress_state)
        with _cloud_sync_phase_scope(profiler, 'list_remote_calibrations'):
            remote_calibrations = client.list_remote_calibrations()

        _set_progress_phase(progress_state, 'calibration_push')
        with _cloud_sync_phase_scope(profiler, 'push_calibrations'):
            calibration_push_result = push_calibrations(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_calibrations=remote_calibrations,
            )

        safety_pull_due = False
        safety_pull_last = None
        if child_safety_pull and not full_pull:
            safety_pull_due, safety_pull_last = _cloud_child_safety_pull_due()
        verify_stamped_measurements, measurement_verify_reason = (
            _cloud_measurement_remote_verification_due(
                full_pull=full_pull,
                child_safety_pull=child_safety_pull,
                safety_pull_due=safety_pull_due,
            )
        )
        print(
            f'[cloud_sync] spore measurement remote verification: '
            f'{"enabled" if verify_stamped_measurements else "skipped"} '
            f'reason={measurement_verify_reason}',
            flush=True,
        )

        # Phase 1: Push local edits to the cloud
        with _cloud_sync_phase_scope(profiler, 'push_all'):
            push_result = push_all(
                client,
                progress_cb=progress_cb,
                sync_images=sync_images,
                prepare_images_cb=prepare_images_cb,
                progress_state=progress_state,
                remote_obs=remote_obs,
                sync_calibrations=False,
                full_pull=full_pull,
                verify_stamped_measurements=verify_stamped_measurements,
            )

        # Refresh after any push-side cloud mutation so pull comparisons see
        # the resulting parent timestamps. A fully-described zero-candidate,
        # zero-reconciliation push can safely reuse the initial list.
        _set_progress_phase(progress_state, 'refresh_remote')
        if _push_phase_requires_remote_observation_refresh(
            calibration_push_result,
            push_result,
        ):
            _emit_progress(progress_cb, "Loading cloud observations…", progress_state)
            refresh_start = _cloud_sync_perf_counter()
            with _cloud_sync_phase_scope(profiler, 'refresh_remote_observations_after_push'):
                remote_obs = client.list_remote_observations()
            refresh_elapsed = _cloud_sync_perf_counter() - refresh_start
            print(
                f"[cloud_sync] observation preflight: remote observations refreshed "
                f"count={len(remote_obs or [])} duration={refresh_elapsed * 1000:.0f}ms",
                flush=True,
            )
        else:
            _emit_progress(
                progress_cb,
                "Cloud observations unchanged; reusing loaded list…",
                progress_state,
            )
            print(
                f"[cloud_sync] observation preflight: remote observations reused "
                f"count={len(remote_obs or [])} reason=push_phase_proven_no_cloud_mutation",
                flush=True,
            )

        if child_safety_pull and not full_pull:
            if safety_pull_due:
                print(
                    f"[cloud_sync] child-change safety pull: "
                    f"reason=stale_child_watermark last={safety_pull_last or 'missing'} "
                    f"interval_hours={_CLOUD_CHILD_SAFETY_PULL_INTERVAL_HOURS}",
                    flush=True,
                )
            else:
                print(
                    f"[cloud_sync] child-change safety pull skipped: "
                    f"fresh watermark last={safety_pull_last}",
                    flush=True,
                )

        forced_pull_cloud_ids: frozenset[str] = frozenset()
        child_probe_ok = False
        child_probe_rows: dict[str, list] = {'images': [], 'measurements': []}
        # Pre-scan seed captured before the authoritative bootstrap scan so that
        # nothing written between scan start and cursor seed is skipped on the
        # next probe. None means bootstrap did not run or pre-capture failed.
        _pre_scan_img_seed: dict | None = None
        _pre_scan_meas_seed: dict | None = None
        _bootstrap_needed = False

        if child_safety_pull and not full_pull:
            cursor = _load_child_change_cursor()
            if cursor is None:
                # No valid cursor (missing or old format): force the authoritative
                # child safety scan immediately, regardless of the 24h TTL.
                # Capture remote max(updated_at, id) BEFORE the scan so that rows
                # written between scan start and cursor seed are not skipped.
                _bootstrap_needed = True
                print('[cloud_sync] child-change cursor missing/old: forcing authoritative bootstrap scan', flush=True)
                try:
                    img_seed_rows = client._get(
                        f'observation_images?user_id=eq.{client.user_id}'
                        f'&select=id,updated_at'
                        f'&order=updated_at.desc,id.desc&limit=1'
                    )
                    meas_seed_rows = client._get(
                        f'spore_measurements?user_id=eq.{client.user_id}'
                        f'&select=id,measured_at'
                        f'&order=measured_at.desc&limit=1'
                    )
                    _pre_scan_img_seed = img_seed_rows[0] if img_seed_rows else {}
                    _pre_scan_meas_seed = meas_seed_rows[0] if meas_seed_rows else {}
                except Exception as e:
                    print(f'[cloud_sync] child-change pre-scan seed capture failed (non-fatal): {e}', flush=True)
            else:
                try:
                    img_rows = client.list_image_changes_since(cursor['images']['ts'], cursor['images']['id'])
                    meas_rows = client.list_measurement_changes_since(cursor['measurements']['ts'], cursor['measurements']['id'])
                    child_probe_ok = True
                    child_probe_rows['images'] = img_rows
                    child_probe_rows['measurements'] = meas_rows
                    _forced: set[str] = set()
                    for r in img_rows:
                        _forced.add(str(r['observation_id']))
                    for r in meas_rows:
                        if r.get('observation_id') is not None:
                            _forced.add(str(r['observation_id']))
                    forced_pull_cloud_ids = frozenset(_forced)
                    changed = 'yes' if forced_pull_cloud_ids else 'no'
                    print(
                        f"[cloud_sync] child-change watermark check: "
                        f"local_images=({cursor['images']['ts']}, {cursor['images']['id'] or '<none>'}) "
                        f"local_measurements=({cursor['measurements']['ts']}, {cursor['measurements']['id'] or '<none>'}) "
                        f"remote_rows={len(img_rows)+len(meas_rows)} changed={changed}",
                        flush=True,
                    )
                    if forced_pull_cloud_ids:
                        print(
                            f'[cloud_sync] child-change pull candidates: '
                            f'rows={len(img_rows)+len(meas_rows)} '
                            f'observations={len(forced_pull_cloud_ids)}',
                            flush=True,
                        )
                except Exception as e:
                    print(f'[cloud_sync] child-change probe failed (non-fatal): {e}', flush=True)

        # Phase 2: Pull cloud edits to the desktop. A due safety pass changes
        # only candidate breadth; media upload and materialization remain
        # controlled by their existing independent flags.
        with _cloud_sync_phase_scope(profiler, 'pull_all'):
            pull_result = pull_all(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_obs=remote_obs,
                sync_calibrations=False,
                materialize_remote_images=materialize_remote_images,
                sync_images=sync_images,
                full_pull=full_pull or safety_pull_due or _bootstrap_needed,
                forced_pull_cloud_ids=forced_pull_cloud_ids,
            )
        # Row-level review issues are a completed reconciliation outcome, not a
        # failed safety pass. Exceptions from auth, transport, or bulk child
        # fetches escape pull_all and therefore never reach this write.
        completed_settings: dict[str, object] = {}
        completed_at = datetime.now(timezone.utc).isoformat()
        if safety_pull_due:
            completed_settings[_CLOUD_LAST_CHILD_SAFETY_PULL_AT_SETTING] = completed_at
        measurement_verification_completed = bool(
            verify_stamped_measurements
            and push_result.get('spore_measurement_reconcile') is not None
        )
        if measurement_verification_completed:
            completed_settings.update({
                _CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: _CLOUD_MEASUREMENT_RECONCILE_VERSION,
                _CLOUD_MEASUREMENT_RECONCILE_AT_SETTING: completed_at,
            })
        if completed_settings:
            update_app_settings(completed_settings)

        # Advance child-change cursor after successful pull
        if child_safety_pull and not full_pull and child_probe_ok:
            # Advance images cursor (uses updated_at as authoritative timestamp).
            # Ids compare numerically via _child_change_cursor_id_key so the
            # committed position is the true MAX(updated_at, id) over every
            # inspected row, matching the strict filter's ordering exactly.
            new_img_ts = ''
            new_img_id = ''
            for r in child_probe_rows['images']:
                ts = str(r.get('updated_at') or '')
                rid = str(r.get('id', ''))
                if (ts, _child_change_cursor_id_key(rid)) > (
                    new_img_ts, _child_change_cursor_id_key(new_img_id)
                ):
                    new_img_ts, new_img_id = ts, rid
            # Advance measurements cursor
            new_meas_ts = ''
            new_meas_id = ''
            for r in child_probe_rows['measurements']:
                ts = str(r.get('measured_at') or '')
                rid = str(r.get('id', ''))
                if (ts, _child_change_cursor_id_key(rid)) > (
                    new_meas_ts, _child_change_cursor_id_key(new_meas_id)
                ):
                    new_meas_ts, new_meas_id = ts, rid
            existing_cursor = _load_child_change_cursor()
            if existing_cursor is not None:
                if not new_img_ts:
                    new_img_ts = existing_cursor['images']['ts']
                    new_img_id = existing_cursor['images']['id']
                if not new_meas_ts:
                    new_meas_ts = existing_cursor['measurements']['ts']
                    new_meas_id = existing_cursor['measurements']['id']
            # Only persist when position strictly advanced (avoids a no-op write
            # when the probe returned no rows and existing cursor is unchanged).
            if new_img_ts or new_meas_ts:
                old_img_ts = (existing_cursor or {}).get('images', {}).get('ts', '')
                old_img_id = (existing_cursor or {}).get('images', {}).get('id', '')
                old_meas_ts = (existing_cursor or {}).get('measurements', {}).get('ts', '')
                old_meas_id = (existing_cursor or {}).get('measurements', {}).get('id', '')
                position_changed = (
                    (new_img_ts, new_img_id) != (old_img_ts, old_img_id)
                    or (new_meas_ts, new_meas_id) != (old_meas_ts, old_meas_id)
                )
                if position_changed:
                    _store_child_change_cursor({
                        'images': {'ts': new_img_ts, 'id': new_img_id},
                        'measurements': {'ts': new_meas_ts, 'id': new_meas_id},
                    })

        # Bootstrap seeding: when _bootstrap_needed the full-scan ran above;
        # seed the cursor with the pre-scan max so that rows written between
        # scan start and seed are not skipped on the next probe.
        # If pull_all raised we never reach this block, so a failed scan does not
        # seed — the next explicit sync retries bootstrap (repeated full-scan cost
        # while persistently failing is correct behaviour). If only the pre-scan
        # max capture failed, seeding at EPOCH is the safe fallback: the next
        # probe re-reads everything rather than skipping anything.
        if child_safety_pull and not full_pull and _bootstrap_needed and _load_child_change_cursor() is None:
            try:
                _EPOCH = '1970-01-01T00:00:00+00:00'
                # Use the pre-scan snapshot captured before pull_all ran.
                if _pre_scan_img_seed is not None:
                    r = _pre_scan_img_seed
                    img_ts = str(r.get('updated_at') or '') or _EPOCH
                    img_id = str(r.get('id', ''))
                else:
                    img_ts, img_id = _EPOCH, ''
                if _pre_scan_meas_seed is not None:
                    r = _pre_scan_meas_seed
                    meas_ts = str(r.get('measured_at') or '') or _EPOCH
                    meas_id = str(r.get('id', ''))
                else:
                    meas_ts, meas_id = _EPOCH, ''
                _store_child_change_cursor({
                    'images': {'ts': img_ts, 'id': img_id},
                    'measurements': {'ts': meas_ts, 'id': meas_id},
                })
                print('[cloud_sync] child-change cursor bootstrapped after authoritative scan', flush=True)
            except Exception as e:
                print(
                    f'[cloud_sync] child-change cursor bootstrap seed failed — will retry on next explicit sync: {e}',
                    flush=True,
                )

        _set_progress_phase(progress_state, 'calibration_pull')
        with _cloud_sync_phase_scope(profiler, 'pull_calibrations'):
            calibration_pull_result = pull_calibrations(
                client,
                progress_cb=progress_cb,
                progress_state=progress_state,
                remote_calibrations=remote_calibrations,
            )

        # The observation pull has established any cloud identities required
        # by reference-use reconciliation. Keep this as a sibling executor and
        # import lazily to avoid a cloud-sync/adapter import cycle.
        from utils.reference_cloud_sync import (
            merge_reference_sync_result,
            sync_reference_library,
        )

        with _cloud_sync_phase_scope(profiler, 'reference_sync'):
            reference_result = sync_reference_library(client)

        # Leave the UI on a neutral phase rather than the last per-calibration
        # message while the worker finishes and the table refreshes. The
        # finalize phase is the only slot that may reach 100%.
        _set_progress_phase(progress_state, 'finalize', phase_total=1)
        _advance_progress(progress_state, 1)
        _emit_progress(progress_cb, "Finalizing cloud sync…", progress_state)
        print(
            f"[cloud_sync] sync progress complete "
            f"duration={(_cloud_sync_perf_counter() - progress_trace['start']) * 1000:.0f}ms",
            flush=True,
        )

        # Combine results for the UI summary
        result = {
            'pushed': push_result.get('pushed', 0),
            'pulled': pull_result.get('pulled', 0),
            'calibrations_pushed': calibration_push_result.get('pushed', 0),
            'calibrations_pulled': calibration_pull_result.get('pulled', 0),
            'errors': (
                calibration_push_result.get('errors', [])
                + push_result.get('errors', [])
                + pull_result.get('errors', [])
                + calibration_pull_result.get('errors', [])
            ),
            'deleted_remote': pull_result.get('deleted_remote', []),
        }
        original_sync = push_result.get('original_sync')
        if original_sync is not None:
            result['original_sync'] = original_sync
        result['sync_summary'] = dict(sync_summary)
        merge_reference_sync_result(result, reference_result)
        return result
    except Exception as exc:
        sync_error = exc
        raise
    finally:
        if profiler is not None:
            try:
                profiler.finish(result=result, error=sync_error)
            except Exception:
                pass
            if profile_token is not None:
                try:
                    _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
                except Exception:
                    pass
        try:
            _CLOUD_SYNC_SUMMARY_CONTEXT.reset(summary_token)
        except Exception:
            pass
        try:
            _CLOUD_SYNC_PROGRESS_TRACE_CONTEXT.reset(progress_trace_token)
        except Exception:
            pass


def _detect_deleted_remote_observations(remote_obs: list[dict] | None) -> list[dict]:
    remote_ids = {
        str(row.get('id') or '').strip()
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT *
            FROM observations
            WHERE cloud_id IS NOT NULL
              AND TRIM(COALESCE(cloud_id, '')) != ''
            ORDER BY date DESC, id DESC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    deleted: list[dict] = []
    for local_obs in rows:
        cloud_id = str(local_obs.get('cloud_id') or '').strip()
        if not cloud_id or cloud_id in remote_ids:
            continue
        deleted.append(
            {
                'local_id': int(local_obs.get('id') or 0),
                'cloud_id': cloud_id,
                'title': _observation_display_name(local_obs),
                'date': local_obs.get('date'),
                'location': local_obs.get('location'),
                'sync_status': str(local_obs.get('sync_status') or '').strip().lower(),
                'observation': dict(local_obs),
            }
        )
    return deleted


def unlink_local_observation_from_cloud(local_id: int) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    image_rows = list(ImageDB.get_images_for_observation(int(local_id)) or [])

    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE observations
            SET cloud_id = NULL,
                sync_status = NULL,
                synced_at = NULL
            WHERE id = ?
            """,
            (int(local_id),),
        )
        conn.execute(
            """
            UPDATE images
            SET cloud_id = NULL,
                synced_at = NULL
            WHERE observation_id = ?
            """,
            (int(local_id),),
        )
        conn.commit()
    finally:
        conn.close()

    if cloud_id:
        _clear_cloud_observation_snapshot(cloud_id)
    _clear_local_cloud_media_signature(int(local_id))
    for image_row in image_rows:
        image_id = _safe_int(image_row.get('id'))
        cloud_image_id = str(image_row.get('cloud_id') or '').strip()
        if image_id > 0:
            _clear_cloud_image_file_signature(int(local_id), image_id)
        if cloud_image_id:
            _clear_cloud_image_file_signature(int(local_id), cloud_image_id)
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def _clear_explicit_image_restore_source(image_id: int | str) -> None:
    image_id = _safe_int(image_id)
    if image_id > 0:
        SettingsDB.set_setting(_explicit_image_restore_source_key(image_id), "")


def _local_media_signatures_match_ignoring_tombstoned_images(
    stored_signature: str | None,
    current_signature: str | None,
) -> bool:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    if not stored_text or not current_text:
        return stored_text == current_text
    if stored_text == current_text:
        return True

    tombstoned_local_ids = _local_tombstoned_local_image_ids()
    if not tombstoned_local_ids:
        return False

    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    if not stored_payload or not current_payload:
        return False

    def _filtered_payload(payload: dict) -> dict:
        filtered = dict(payload or {})
        images: list[dict] = []
        for row in list(filtered.get('images') or []):
            if not isinstance(row, dict):
                continue
            if _safe_int(row.get('id')) in tombstoned_local_ids:
                continue
            images.append(dict(row))
        filtered['images'] = images
        return filtered

    return _normalized_local_media_signature_payload(
        _filtered_payload(stored_payload),
        include_measurements=False,
    ) == _normalized_local_media_signature_payload(
        _filtered_payload(current_payload),
        include_measurements=False,
    )


def _local_media_prep_diagnostics(
    observation_id: int | str,
    stored_signature: str | None,
    current_signature: str | None,
) -> dict:
    stored_text = str(stored_signature or '').strip()
    current_text = str(current_signature or '').strip()
    stored_payload = _parsed_local_media_signature(stored_text)
    current_payload = _parsed_local_media_signature(current_text)
    image_render_signature_matched = bool(
        stored_text
        and current_text
        and _local_media_signatures_match(
            stored_text,
            current_text,
            include_measurements=False,
        )
    )
    tombstone_aware_signature_matched = bool(
        stored_text
        and current_text
        and _local_media_signatures_match_ignoring_tombstoned_images(
            stored_text,
            current_text,
        )
    )
    measurement_only_matched = bool(
        stored_text
        and current_text
        and image_render_signature_matched
        and not _local_media_signatures_match(stored_text, current_text)
    )

    local_image_rows: list[dict] = []
    obs_id = _safe_int(observation_id)
    if obs_id > 0:
        try:
            local_image_rows = [dict(row or {}) for row in ImageDB.get_images_for_observation(obs_id) or []]
        except Exception:
            local_image_rows = []
    has_local_image_cloud_id_null = any(not str(row.get('cloud_id') or '').strip() for row in local_image_rows)

    changed_keys: list[str] = []
    any_image_file_signature_changed = False
    any_render_affecting_field_changed = False
    only_metadata_fields_changed = False

    def _normalized_path_signature(path_value: object) -> dict:
        if not isinstance(path_value, dict):
            return {'path': '', 'exists': False}
        normalized = dict(path_value)
        normalized.pop('mtime_ns', None)
        return normalized

    if stored_payload and current_payload:
        stored_images = [dict(row or {}) for row in (stored_payload.get('images') or [])]
        current_images = [dict(row or {}) for row in (current_payload.get('images') or [])]
        image_changes = _analyze_image_changes(current_images, stored_images)
        changed_keys.extend(f'+{key}' for key in (image_changes.get('added_keys') or []))
        changed_keys.extend(f'-{key}' for key in (image_changes.get('removed_keys') or []))

        current_map = {_image_compare_key(row): row for row in current_images}
        stored_map = {_image_compare_key(row): row for row in stored_images}
        shared_keys = [key for key in current_map if key in stored_map]
        metadata_changed_field_count = 0

        for key in shared_keys:
            current_row = current_map[key]
            stored_row = stored_map[key]

            for path_key in ('filepath', 'original_filepath'):
                if _normalized_path_signature(current_row.get(path_key)) != _normalized_path_signature(stored_row.get(path_key)):
                    any_image_file_signature_changed = True
                    changed_keys.append(f'{key}:{path_key}')

            current_meta = _image_metadata_payload(current_row)
            stored_meta = _image_metadata_payload(stored_row)
            for field, current_value in current_meta.items():
                if current_value == stored_meta.get(field):
                    continue
                metadata_changed_field_count += 1
                changed_keys.append(f'{key}:{field}')
                if field in _LOCAL_MEDIA_PREP_RENDER_AFFECTING_IMAGE_FIELDS:
                    any_render_affecting_field_changed = True

        for field in _LOCAL_MEDIA_PREP_RENDER_AFFECTING_TOP_LEVEL_FIELDS:
            if current_payload.get(field) != stored_payload.get(field):
                any_render_affecting_field_changed = True
                changed_keys.append(field)

        if image_changes.get('added_keys') or image_changes.get('removed_keys'):
            any_image_file_signature_changed = True

        only_metadata_fields_changed = bool(
            metadata_changed_field_count
            and not any_image_file_signature_changed
            and not any_render_affecting_field_changed
            and not image_changes.get('added_keys')
            and not image_changes.get('removed_keys')
        )

    changed_keys = list(dict.fromkeys(str(key).strip() for key in changed_keys if str(key).strip()))

    return {
        'image_render_signature_matched': image_render_signature_matched,
        'tombstone_aware_signature_matched': tombstone_aware_signature_matched,
        'measurement_only_matched': measurement_only_matched,
        'has_local_image_cloud_id_null': has_local_image_cloud_id_null,
        'any_image_file_signature_changed': any_image_file_signature_changed,
        'any_render_affecting_field_changed': any_render_affecting_field_changed,
        'only_metadata_fields_changed': only_metadata_fields_changed,
        'changed_keys': changed_keys,
    }


def _format_local_media_prep_diagnostic_keys(keys: list[str], *, limit: int = 8) -> str:
    normalized_keys = [str(key).strip() for key in (keys or []) if str(key or '').strip()]
    if not normalized_keys:
        return '[]'
    display_keys = normalized_keys[: max(1, int(limit))]
    suffix = ''
    if len(normalized_keys) > len(display_keys):
        suffix = f', … (+{len(normalized_keys) - len(display_keys)} more)'
    return f"[{', '.join(display_keys)}{suffix}]"


_OBSERVATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'selected_sporely_taxon_id',
    'captured_at',
    'created_at',
    'updated_at',
    *_SNAPSHOT_OBS_FIELDS,
)

_OBSERVATION_IMAGE_SELECT_COLUMNS = _join_select_columns(
    'id',
    'desktop_id',
    'observation_id',
    'created_at',
    'deleted_at',
    *_SNAPSHOT_IMG_FIELDS,
    *_SNAPSHOT_IMG_PASSIVE_FIELDS,
)

_OBSERVATION_IDENTIFICATION_SELECT_COLUMNS = _join_select_columns(
    'id',
    'service',
    'created_at',
    'results',
    'top_scientific_name',
    'top_vernacular_name',
    'top_taxon_id',
    'top_species_url',
    'top_probability',
)


def _mark_cloud_observations_dirty_for_media_changes() -> None:
    current_signature = _cloud_media_signature()
    previous_signature = str(SettingsDB.get_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, '') or '').strip()
    if previous_signature == current_signature:
        return
    # Background cloud sync currently uploads source images/metadata, not the
    # optional rendered overlays/gallery/plate outputs tied to these settings.
    # Persist the new signature so future comparisons are stable, but don't mark
    # every linked observation dirty just because a global render preference changed.
    SettingsDB.set_setting(_SETTING_CLOUD_MEDIA_SIGNATURE, current_signature)


def _mark_cloud_observations_dirty_for_image_capture_time_changes() -> int:
    """Schedule linked images whose capture time is absent from the baseline.

    Older sync signatures predate ``images.captured_at``. Merely adding the
    field to the normal snapshot contract cannot revisit observations already
    stamped as synced, so explicit media sync performs this narrow comparison.
    Once a successful sync refreshes the local signature, the scan is a no-op.
    """
    conn = get_connection()
    try:
        image_columns = {
            str(row[1] or '').strip()
            for row in conn.execute("PRAGMA table_info(images)").fetchall()
        }
        if 'captured_at' not in image_columns:
            return 0
        rows = conn.execute(
            """
            SELECT DISTINCT o.id
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            WHERE o.cloud_id IS NOT NULL
              AND COALESCE(o.sync_status, '') != 'dirty'
              AND i.cloud_id IS NOT NULL
              AND i.captured_at IS NOT NULL
              AND TRIM(CAST(i.captured_at AS TEXT)) != ''
            """
        ).fetchall()
    finally:
        conn.close()

    marked = 0
    for row in rows:
        observation_id = _safe_int(row[0])
        if observation_id <= 0:
            continue
        stored = _parsed_local_media_signature(
            _load_local_cloud_media_signature(observation_id)
        )
        current = _parsed_local_media_signature(
            _local_cloud_image_media_signature(observation_id)
        )
        stored_by_id = {
            _safe_int(image.get('id')): dict(image)
            for image in (stored.get('images') or [])
            if isinstance(image, dict) and _safe_int(image.get('id')) > 0
        }
        needs_sync = any(
            image.get('captured_at') is not None
            and image.get('captured_at')
            != stored_by_id.get(_safe_int(image.get('id')), {}).get('captured_at')
            for image in (current.get('images') or [])
            if isinstance(image, dict)
        )
        if not needs_sync:
            continue
        mark_observation_dirty(observation_id)
        marked += 1
    return marked


def _load_local_observation_lookup() -> tuple[dict[str, dict], dict[int, dict]]:
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM observations')
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    by_cloud_id: dict[str, dict] = {}
    by_local_id: dict[int, dict] = {}
    for row in rows:
        local_id = _safe_int(row.get('id'))
        cloud_id = str(row.get('cloud_id') or '').strip()
        if local_id > 0:
            by_local_id[local_id] = row
        if cloud_id:
            by_cloud_id[cloud_id] = row
    return by_cloud_id, by_local_id


def _find_local_observation_for_remote_cached(
    remote: dict,
    by_cloud_id: dict[str, dict],
    by_local_id: dict[int, dict],
) -> dict | None:
    cloud_id = str((remote or {}).get('id') or '').strip()
    if cloud_id and cloud_id in by_cloud_id:
        return dict(by_cloud_id[cloud_id])
    local_id = _safe_int((remote or {}).get('desktop_id'))
    if local_id > 0 and local_id in by_local_id:
        candidate = dict(by_local_id[local_id])
        if bool(candidate.get('portable_cloud_identity_pending')):
            return None
        return candidate
    return None


def _remote_snapshot_has_meaningful_changes(
    remote: dict | None,
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None,
    stored_snapshot: str | None,
) -> bool:
    snapshot = _parse_cloud_observation_snapshot(stored_snapshot)
    if not snapshot:
        return True
    baseline_obs = _baseline_observation_compare_payload(snapshot.get('observation') or {})
    if not baseline_obs:
        return True
    remote_payload = _observation_compare_payload(remote, local=False)
    for field in _SNAPSHOT_OBS_FIELDS:
        if field in {'id', 'desktop_id'}:
            continue
        if not _observation_field_values_match(field, remote_payload.get(field), baseline_obs.get(field)):
            return True
    if _remote_identity_changed_since(remote, baseline_obs):
        return True
    baseline_images = [dict(row or {}) for row in (snapshot.get('images') or [])]
    remote_image_payloads = [_remote_image_payload(img) for img in (remote_images or [])]
    remote_image_changes = _analyze_image_changes(remote_image_payloads, baseline_images)
    baseline_measurements = [dict(row or {}) for row in (snapshot.get('measurements') or [])]
    remote_measurement_payloads = [_remote_measurement_snapshot_payload(row) for row in (remote_measurements or [])]
    remote_measurement_changes = _analyze_measurement_changes(remote_measurement_payloads, baseline_measurements)
    return bool(
        remote_image_changes.get('added_keys')
        or remote_image_changes.get('removed_keys')
        or remote_image_changes.get('metadata_changed_keys')
        or remote_measurement_changes.get('changed')
    )


def _stamp_observation_synced(local_id: int, cloud_id: str) -> None:
    _set_observation_sync_state(int(local_id), str(cloud_id or '').strip(), dirty=False)


def _set_observation_sync_state(
    local_id: int,
    cloud_id: str,
    *,
    dirty: bool,
    synced_at: str | None | object = _UNSET,
) -> None:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        if synced_at is _UNSET:
            synced_at_value: str | None = datetime.now(timezone.utc).isoformat()
        else:
            synced_at_value = synced_at if synced_at is None else str(synced_at)
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=str(cloud_id or '').strip() or None,
            sync_status='dirty' if dirty else 'synced',
            synced_at=synced_at_value,
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()


def _set_observation_sync_blocked(local_id: int, raw_error: str, blocked_reason: str, *, error_code: str | None = None) -> str:
    code, _ = _collect_sync_error_details(raw_error)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='blocked',
            sync_error_code=error_code or code or None,
            sync_error_message=str(raw_error or '').strip() or None,
            sync_blocked_reason=blocked_reason,
            sync_blocked_at=datetime.now(timezone.utc).isoformat(),
        )
        conn.commit()
    finally:
        conn.close()
    return blocked_reason


def _set_observation_sync_error_detail_only(local_id: int, message: str, *, error_code: str) -> None:
    """Record an error code/message without disturbing sync_status or the
    blocked-reason marker.

    Used when a secondary problem occurs on an observation some other,
    more specific mechanism already blocked for a different reason (e.g.
    the Case F conflict-review marker) — this must never overwrite or
    clear that primary marker, only make the secondary failure visible
    instead of a log line alone (Stage C review round 3, finding 1).
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_error_code=error_code,
            sync_error_message=message,
        )
        conn.commit()
    finally:
        conn.close()


def _set_observation_privacy_blocked(local_id: int, raw_error: str) -> str:
    return _set_observation_sync_blocked(
        local_id,
        raw_error,
        privacy_slot_limit_user_message(),
        error_code='privacy_slot_limit',
    )


def _set_observation_plan_image_retryable(local_id: int, raw_error: str) -> str:
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            sync_status='dirty',
            sync_error_code='image_too_large_for_plan',
            sync_error_message=str(raw_error or '').strip() or None,
            sync_blocked_reason=None,
            sync_blocked_at=None,
        )
        conn.commit()
    finally:
        conn.close()
    return summarize_image_too_large_for_plan_error(raw_error)


def _remote_observation_update_kwargs(remote: dict) -> dict:
    raw_location_public = remote.get('location_public')
    location_public = _normalize_observation_bool_value(raw_location_public, default=None)
    raw_publish_target = str(remote.get('publish_target') or '').strip()
    genus, species, species_guess = resolve_observation_taxon_fields(
        remote.get('genus'),
        remote.get('species'),
        remote.get('species_guess'),
        remote.get('ai_selected_scientific_name'),
    )
    return {
        'date': remote.get('date'),
        'genus': genus,
        'species': species,
        'common_name': remote.get('common_name'),
        'species_guess': species_guess,
        'location': remote.get('location'),
        'habitat': remote.get('habitat'),
        'notes': remote.get('notes'),
        'open_comment': remote.get('open_comment'),
        'interesting_comment': _normalize_observation_bool_value(remote.get('interesting_comment'), default=False),
        'sharing_scope': _cloud_visibility_to_sharing_scope(
            remote.get('visibility') or remote.get('sharing_scope'),
            fallback='friends' if location_public else 'private',
        ),
        'location_public': location_public,
        'is_draft': _normalize_observation_bool_value(remote.get('is_draft'), default=True),
        'location_precision': ObservationDB._normalize_location_precision(
            remote.get('location_precision')
        ),
        'ai_selected_service': remote.get('ai_selected_service'),
        'ai_selected_taxon_id': remote.get('ai_selected_taxon_id'),
        'ai_selected_scientific_name': remote.get('ai_selected_scientific_name'),
        'ai_selected_probability': _normalize_observation_float_value(remote.get('ai_selected_probability')),
        'ai_selected_at': remote.get('ai_selected_at'),
        'spore_data_visibility': (lambda v: v if v in {'private', 'friends', 'public'} else 'public')(
            str(remote.get('spore_data_visibility') or 'public').strip().lower()
        ),
        'uncertain': _normalize_observation_bool_value(remote.get('uncertain'), default=False),
        'unspontaneous': _normalize_observation_bool_value(remote.get('unspontaneous'), default=False),
        'gps_latitude': _normalize_observation_float_value(remote.get('gps_latitude')),
        'gps_longitude': _normalize_observation_float_value(remote.get('gps_longitude')),
        'artsdata_id': _normalize_observation_int_value(remote.get('artsdata_id')),
        'artportalen_id': _normalize_observation_int_value(remote.get('artportalen_id')),
        'publish_target': normalize_publish_target(raw_publish_target) if raw_publish_target else None,
        'determination_method': _normalize_observation_int_value(remote.get('determination_method')),
        'habitat_nin2_path': remote.get('habitat_nin2_path'),
        'habitat_substrate_path': remote.get('habitat_substrate_path'),
        'habitat_host_genus': remote.get('habitat_host_genus'),
        'habitat_host_species': remote.get('habitat_host_species'),
        'habitat_host_common_name': remote.get('habitat_host_common_name'),
        'habitat_nin2_note': remote.get('habitat_nin2_note'),
        'habitat_substrate_note': remote.get('habitat_substrate_note'),
        'habitat_grows_on_note': remote.get('habitat_grows_on_note'),
        'country_code': normalize_country_code(remote.get('country_code')),
        'region_id': _normalize_observation_field_value('region_id', remote.get('region_id')),
        'allow_nulls': True,
    }


def _remote_observation_extra_values(remote: dict) -> dict:
    raw_spore_stats = remote.get('spore_statistics')
    serialized_spore_stats = _normalize_observation_json_value(raw_spore_stats)
    if serialized_spore_stats is not None and not isinstance(serialized_spore_stats, str):
        serialized_spore_stats = json.dumps(serialized_spore_stats, ensure_ascii=False, sort_keys=True)
    raw_auto_threshold = _normalize_observation_float_value(remote.get('auto_threshold'))
    # The cloud stores red_list_categories_json as JSONB; the local column is
    # TEXT. Serialize back to a JSON string so the local writer stores the
    # exact same payload sporely-web shows in Taxonomy → Red list.
    raw_red_categories = remote.get('red_list_categories_json')
    serialized_red_categories = _normalize_observation_json_value(raw_red_categories)
    if serialized_red_categories is not None and not isinstance(serialized_red_categories, str):
        serialized_red_categories = json.dumps(
            serialized_red_categories, ensure_ascii=False, sort_keys=True
        )
    return {
        'inaturalist_id': _normalize_observation_int_value(remote.get('inaturalist_id')),
        'mushroomobserver_id': _normalize_observation_int_value(remote.get('mushroomobserver_id')),
        'source_type': remote.get('source_type'),
        'citation': remote.get('citation'),
        'data_provider': remote.get('data_provider'),
        'author': remote.get('author'),
        'spore_statistics': serialized_spore_stats,
        'auto_threshold': raw_auto_threshold,
        'red_list_category': remote.get('red_list_category'),
        'red_list_categories_json': serialized_red_categories,
    }


_MERGE_PROTECTED_AI_FIELDS = (
    'ai_selected_service',
    'ai_selected_taxon_id',
    'ai_selected_scientific_name',
    'ai_selected_probability',
    'ai_selected_at',
    'red_list_category',
    'red_list_categories_json',
)


_RED_LIST_FIELDS = frozenset({'red_list_category', 'red_list_categories_json'})


def _merge_cloud_selected_ai_fields(local_obs: dict | None, remote_obs: dict | None) -> dict:
    """Preserve cloud-side selected AI values for an existing identification.

    Existing desktop observations may have `NULL` in the newly added fields until
    they are re-pulled from cloud. When we push an unrelated desktop edit, we
    don't want those missing local values to wipe the cloud selection. A fully
    empty local identification is different: its nulls are an explicit clear
    and must reach the cloud.
    """
    merged = dict(local_obs or {})
    remote = dict(remote_obs or {})
    identification_fields = (
        'genus',
        'species',
        'common_name',
        'species_guess',
    )
    # Missing keys mean this may be a partial update payload. Only treat the
    # identification as explicitly cleared when all identity fields are
    # present and every one is blank/null.
    identification_is_empty = (
        all(field in merged for field in identification_fields)
        and not any(
            str(merged.get(field) or '').strip()
            for field in identification_fields
        )
    )
    # A cloud Red List is an assessment of the cloud's identification. After a
    # desktop re-identification the dialog clears the Red List, so a local
    # NULL means "the new taxon has no stored assessment" — filling it from
    # the cloud would copy the previous taxon's category onto the new one
    # (taxonomy-v2 closeout, observation 917). Skip the gap-fill only when
    # both sides carry genus/species and they differ; a partial row without
    # them is no evidence of a different taxon and keeps the original
    # gap-filling behaviour.
    red_list_describes_other_taxon = (
        all(field in row for row in (merged, remote) for field in ('genus', 'species'))
        and _identification_key(merged) != _identification_key(remote)
    )
    for field in _MERGE_PROTECTED_AI_FIELDS:
        # An empty local identification is an explicit tombstone when the row
        # is pushed. Preserve raw observation_identifications separately, but
        # do not resurrect the previously selected AI taxon or red-list data.
        if identification_is_empty:
            continue
        if field in _RED_LIST_FIELDS and red_list_describes_other_taxon:
            continue
        local_value = merged.get(field)
        if local_value not in (None, ''):
            continue
        remote_value = remote.get(field)
        if remote_value not in (None, ''):
            merged[field] = remote_value
    return merged


def _adopt_merge_filled_ai_fields_locally(local_id, local_obs: dict | None, merged: dict | None) -> None:
    """Persist merge-protected fields that the push payload filled from cloud.

    ``_merge_cloud_selected_ai_fields`` fills local NULL/'' AI-selection and
    red-list fields from the remote row so an unrelated push cannot wipe
    them. Without also adopting those values into the LOCAL row, the cloud
    row and the stored snapshot carry the value while the local column stays
    NULL — a manufactured local-only diff that re-marks the observation
    dirty on every subsequent pull (perpetual push/pull loop, live obs
    673/742 red_list_category). Writes only the filled columns; never
    touches sync status or dirty flags.
    """
    try:
        obs_id = int(local_id)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return
    updates: dict[str, object] = {}
    local_row = dict(local_obs or {})
    merged_row = dict(merged or {})
    for field in _MERGE_PROTECTED_AI_FIELDS:
        if local_row.get(field) not in (None, ''):
            continue
        merged_value = merged_row.get(field)
        if merged_value in (None, ''):
            continue
        if isinstance(merged_value, (dict, list)):
            merged_value = json.dumps(merged_value)
        updates[field] = merged_value
    if not updates:
        return
    conn = get_connection()
    try:
        assignments = ', '.join(f'{column} = ?' for column in updates)
        conn.execute(
            f'UPDATE observations SET {assignments} WHERE id = ?',
            (*updates.values(), obs_id),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_cloud_identification_service(value: object) -> str | None:
    raw = str(value or '').strip().lower()
    if raw in {'artsorakel', 'arts'}:
        return 'artsorakel'
    if raw in {'inat', 'inaturalist'}:
        return 'inat'
    return None


def _cloud_identification_prediction_taxon(prediction: dict, service: str | None = None) -> dict | None:
    pred = dict(prediction or {})
    taxon = dict(pred.get('taxon') or {})
    scientific_name = str(
        pred.get('scientificName')
        or pred.get('scientific_name')
        or pred.get('name')
        or ''
    ).strip()
    vernacular_name = str(
        pred.get('vernacularName')
        or pred.get('vernacular_name')
        or pred.get('commonName')
        or pred.get('common_name')
        or ''
    ).strip()
    taxon_id = pred.get('taxonId') or pred.get('taxon_id')

    if scientific_name:
        taxon.setdefault('scientificName', scientific_name)
        taxon.setdefault('scientific_name', scientific_name)
        taxon.setdefault('name', scientific_name)
    if vernacular_name:
        taxon.setdefault('vernacularName', vernacular_name)
        taxon.setdefault('vernacular_name', vernacular_name)
        taxon.setdefault('preferred_common_name', vernacular_name)
        taxon.setdefault('common_name', vernacular_name)
    if taxon_id not in (None, ''):
        taxon.setdefault('id', taxon_id)
        taxon.setdefault('taxonId', taxon_id)
        taxon.setdefault('taxon_id', taxon_id)
    if service == 'inat' and vernacular_name and not taxon.get('preferred_common_name'):
        taxon['preferred_common_name'] = vernacular_name

    # Preserve Artsorakel redlist metadata so the desktop's Species AI panel
    # can render the badge for predictions produced remotely. sporely-web
    # emits `redlistCategory` / `redlist_category` (lowercase l) plus the
    # rich `redlist_categories` payload; the raw Artsorakel API uses
    # `redListCategory` (capital L). Copy whichever form we find and mirror
    # it under all three aliases so the reader in observations_tab.py can
    # pick it up regardless of casing.
    for key in ('redListCategory', 'redlistCategory', 'redlist_category'):
        value = pred.get(key) or taxon.get(key)
        if value:
            for alias in ('redListCategory', 'redlistCategory', 'redlist_category'):
                taxon.setdefault(alias, value)
            break
    for key in ('redListCategories', 'redlistCategories', 'redlist_categories'):
        value = pred.get(key)
        if value is None:
            value = taxon.get(key)
        if isinstance(value, dict) and value:
            for alias in ('redListCategories', 'redlistCategories', 'redlist_categories'):
                taxon.setdefault(alias, value)
            break

    return taxon or None


def _cloud_identification_prediction_display_name(prediction: dict) -> str:
    scientific_name = str(
        prediction.get('scientificName')
        or prediction.get('scientific_name')
        or prediction.get('name')
        or ''
    ).strip()
    vernacular_name = str(
        prediction.get('vernacularName')
        or prediction.get('vernacular_name')
        or prediction.get('commonName')
        or prediction.get('common_name')
        or ''
    ).strip()
    display_name = str(prediction.get('displayName') or prediction.get('display_name') or '').strip()

    if display_name:
        return display_name
    if vernacular_name and scientific_name and vernacular_name.casefold() != scientific_name.casefold():
        return f'{vernacular_name} ({scientific_name})'
    return vernacular_name or scientific_name


def _cloud_identification_prediction_species_url(prediction: dict, service: str | None = None) -> str | None:
    pred = dict(prediction or {})
    taxon = dict(pred.get('taxon') or {})
    if service == 'inat':
        taxon_id = str(
            pred.get('taxonId')
            or pred.get('taxon_id')
            or taxon.get('id')
            or taxon.get('taxonId')
            or taxon.get('taxon_id')
            or ''
        ).strip()
        if taxon_id:
            return f'https://www.inaturalist.org/taxa/{taxon_id}'
        return None

    for source in (pred, taxon):
        for key in (
            'species_url',
            'speciesUrl',
            'adbUrl',
            'url',
            'link',
            'href',
            'uri',
            'infoUrl',
            'infoURL',
            'info_url',
        ):
            value = source.get(key)
            if isinstance(value, str) and value.strip().startswith('http'):
                return value.strip()

    taxon_id = str(
        pred.get('taxonId')
        or pred.get('taxon_id')
        or taxon.get('taxonId')
        or taxon.get('taxon_id')
        or taxon.get('id')
        or ''
    ).strip()
    if taxon_id and taxon_id.isdigit():
        # Stage 3B.5: this helper runs on the GUI thread inside a
        # per-prediction loop when the observation-detail dialog opens.
        # A 5 s network resolve × ~10 predictions would freeze the
        # editor for tens of seconds on a bad link. Use cache-only mode:
        # cache hits return the true concept URL, misses fall to the
        # NorTaxa fallback with no network I/O.
        return concept_link_from_name_id(taxon_id, network=False)
    return None


def _cloud_identification_prediction_matches_observation(prediction: dict, observation: dict | None) -> bool:
    obs = dict(observation or {})
    obs_scientific_name = str(
        obs.get('genus')
        or ''
    ).strip()
    obs_species = str(obs.get('species') or '').strip()
    if obs_scientific_name and obs_species:
        obs_scientific_name = f'{obs_scientific_name} {obs_species}'.strip()
    else:
        obs_scientific_name = str(obs.get('species_guess') or obs_scientific_name or '').strip()
    obs_common_name = str(obs.get('common_name') or '').strip()

    prediction_scientific_name = str(
        prediction.get('scientificName')
        or prediction.get('scientific_name')
        or prediction.get('name')
        or ''
    ).strip()
    taxon = dict(prediction.get('taxon') or {})
    if not prediction_scientific_name:
        prediction_scientific_name = str(
            taxon.get('scientificName')
            or taxon.get('scientific_name')
            or taxon.get('name')
            or ''
        ).strip()
    prediction_common_name = str(
        prediction.get('vernacularName')
        or prediction.get('vernacular_name')
        or prediction.get('commonName')
        or prediction.get('common_name')
        or ''
    ).strip()
    if not prediction_common_name:
        prediction_common_name = str(
            taxon.get('vernacularName')
            or taxon.get('vernacular_name')
            or taxon.get('preferred_common_name')
            or taxon.get('common_name')
            or ''
        ).strip()
    prediction_taxon_id = str(
        prediction.get('taxonId')
        or prediction.get('taxon_id')
        or ''
    ).strip()
    if not prediction_taxon_id:
        prediction_taxon_id = str(
            taxon.get('id')
            or taxon.get('taxonId')
            or taxon.get('taxon_id')
            or ''
        ).strip()
    selected_taxon_id = str(obs.get('ai_selected_taxon_id') or '').strip()
    selected_scientific_name = str(obs.get('ai_selected_scientific_name') or '').strip()

    if selected_taxon_id and prediction_taxon_id and selected_taxon_id == prediction_taxon_id:
        return True
    if selected_scientific_name and prediction_scientific_name and selected_scientific_name == prediction_scientific_name:
        return True
    if obs_scientific_name and prediction_scientific_name and obs_scientific_name == prediction_scientific_name:
        return True
    if obs_common_name and prediction_common_name and obs_common_name == prediction_common_name:
        return True
    return False


def build_cloud_ai_state_from_observation_identifications(
    observation: dict | None,
    identification_rows: list[dict] | None,
    local_images: list[dict] | None = None,
) -> dict | None:
    """Build the desktop AI-state cache from cloud observation_identifications rows.

    The cloud table stays authoritative; the desktop only keeps a derived cache
    so the observation detail dialog can render the same suggestions without a
    separate local AI run.
    """
    obs = dict(observation or {})
    rows = [dict(row or {}) for row in (identification_rows or []) if row]
    if not rows:
        return None

    local_image_rows = [dict(row or {}) for row in (local_images or []) if row]
    index_count = len(local_image_rows) or 1
    indices = list(range(index_count))
    paths = [str(row.get('filepath') or '').strip() for row in local_image_rows]
    image_ids = [_safe_int(row.get('id')) or None for row in local_image_rows]
    selected_service = _normalize_cloud_identification_service(obs.get('ai_selected_service'))

    service_rows: dict[str, dict] = {}
    for row in sorted(
        rows,
        key=lambda item: (
            _parse_sync_timestamp(item.get('created_at')) or datetime.min.replace(tzinfo=timezone.utc),
            _safe_int(item.get('id')) or 0,
        ),
        reverse=True,
    ):
        service = _normalize_cloud_identification_service(row.get('service'))
        if not service or service in service_rows:
            continue
        service_rows[service] = row

    predictions_by_service: dict[str, list[dict]] = {'artsorakel': [], 'inat': []}
    selected_by_service: dict[str, dict] = {}
    for service, row in service_rows.items():
        raw_predictions = [dict(pred or {}) for pred in (row.get('results') or []) if isinstance(pred, dict)]
        normalized_predictions: list[dict] = []
        for prediction in raw_predictions:
            taxon = _cloud_identification_prediction_taxon(prediction, service=service)
            if taxon:
                prediction['taxon'] = taxon
            if not str(prediction.get('scientificName') or '').strip():
                scientific_name = str(
                    prediction.get('scientific_name')
                    or taxon.get('scientificName')
                    or taxon.get('scientific_name')
                    or taxon.get('name')
                    or ''
                ).strip()
                if scientific_name:
                    prediction['scientificName'] = scientific_name
                    prediction['scientific_name'] = scientific_name
            if not str(prediction.get('vernacularName') or '').strip():
                vernacular_name = str(
                    prediction.get('vernacular_name')
                    or taxon.get('vernacularName')
                    or taxon.get('vernacular_name')
                    or taxon.get('preferred_common_name')
                    or taxon.get('common_name')
                    or ''
                ).strip()
                if vernacular_name:
                    prediction['vernacularName'] = vernacular_name
                    prediction['vernacular_name'] = vernacular_name
            if not str(prediction.get('displayName') or '').strip():
                display_name = _cloud_identification_prediction_display_name(prediction)
                if display_name:
                    prediction['displayName'] = display_name
                    prediction['display_name'] = display_name
            species_url = _cloud_identification_prediction_species_url(prediction, service=service)
            if species_url:
                prediction['species_url'] = species_url
                prediction['speciesUrl'] = species_url
                prediction['adbUrl'] = species_url
            normalized_predictions.append(prediction)

        if not normalized_predictions:
            top_scientific_name = str(row.get('top_scientific_name') or '').strip()
            top_vernacular_name = str(row.get('top_vernacular_name') or '').strip()
            top_taxon_id = str(row.get('top_taxon_id') or '').strip()
            top_species_url = str(
                row.get('top_species_url')
                or row.get('top_speciesUrl')
                or row.get('top_adbUrl')
                or ''
            ).strip()
            if top_scientific_name or top_vernacular_name or top_taxon_id:
                synth_prediction = {
                    'service': service,
                    'rank': 1,
                    'scientificName': top_scientific_name or None,
                    'scientific_name': top_scientific_name or None,
                    'vernacularName': top_vernacular_name or None,
                    'vernacular_name': top_vernacular_name or None,
                    'displayName': top_vernacular_name or top_scientific_name or top_taxon_id or 'Unknown',
                    'taxonId': top_taxon_id or None,
                    'taxon_id': top_taxon_id or None,
                    'probability': row.get('top_probability'),
                }
                if top_species_url:
                    synth_prediction['species_url'] = top_species_url
                    synth_prediction['speciesUrl'] = top_species_url
                    synth_prediction['adbUrl'] = top_species_url
                taxon = _cloud_identification_prediction_taxon(synth_prediction, service=service)
                if taxon:
                    synth_prediction['taxon'] = taxon
                normalized_predictions = [synth_prediction]

        if not normalized_predictions:
            continue

        selected_prediction = None
        if selected_service == service:
            selected_prediction = next(
                (
                    prediction
                    for prediction in normalized_predictions
                    if _cloud_identification_prediction_matches_observation(prediction, obs)
                ),
                None,
            )

        predictions_by_service[service] = normalized_predictions
        if selected_prediction:
            selected_by_service[service] = selected_prediction

    if not any(predictions_by_service.values()) and not selected_by_service:
        selected_service = _normalize_cloud_identification_service(obs.get('ai_selected_service'))
        selected_scientific_name = str(obs.get('ai_selected_scientific_name') or '').strip()
        selected_taxon_id = str(obs.get('ai_selected_taxon_id') or '').strip()
        if selected_service and (selected_scientific_name or selected_taxon_id):
            synth_prediction = {
                'service': selected_service,
                'rank': 1,
                'scientificName': selected_scientific_name or None,
                'scientific_name': selected_scientific_name or None,
                'vernacularName': str(obs.get('common_name') or '').strip() or None,
                'vernacular_name': str(obs.get('common_name') or '').strip() or None,
                'displayName': selected_scientific_name or str(obs.get('common_name') or '').strip() or selected_taxon_id or 'Unknown',
                'taxonId': selected_taxon_id or None,
                'taxon_id': selected_taxon_id or None,
                'probability': obs.get('ai_selected_probability'),
            }
            taxon = _cloud_identification_prediction_taxon(synth_prediction, service=selected_service)
            if taxon:
                synth_prediction['taxon'] = taxon
            predictions_by_service[selected_service] = [synth_prediction]
            selected_by_service[selected_service] = synth_prediction

    if not any(predictions_by_service.values()) and not selected_by_service:
        return None

    state: dict = {
        'predictions': {},
        'selected': {},
        'inat_predictions': {},
        'inat_selected': {},
        'selected_index': indices[0] if indices else None,
        'paths': paths,
        'image_ids': image_ids,
    }

    for index in indices:
        if predictions_by_service.get('artsorakel'):
            state['predictions'][index] = [dict(pred or {}) for pred in predictions_by_service['artsorakel']]
            if selected_by_service.get('artsorakel'):
                state['selected'][index] = dict(selected_by_service['artsorakel'])
        if predictions_by_service.get('inat'):
            state['inat_predictions'][index] = [dict(pred or {}) for pred in predictions_by_service['inat']]
            if selected_by_service.get('inat'):
                state['inat_selected'][index] = dict(selected_by_service['inat'])

    return state


# ── Cloud → desktop taxonomy identity ────────────────────────────────────────
#
# The cloud describes an observation's identity with `selected_sporely_taxon_id`
# (writable only through the guarded RPCs) plus, once the provenance migration
# is deployed, `taxon_identity_state` and the preserved external tuple. A
# desktop never trusts that as proof: a Sporely ID is kept only when the
# installed artifact contains the concept, and then only as
# `cloud_selected_unverified` (see utils/taxon_identity.py). Contract:
# docs/supabase-sync-contract.md §28.


#: PostgREST's "column observations.taxon_identity_state does not exist" (42703)
#: and "Could not find the 'taxon_identity_state' column" (PGRST204).
_MISSING_IDENTITY_COLUMN_PATTERN = re.compile(
    r"column \S*taxon_identity_\w+ does not exist"
    r"|could not find the '?taxon_identity_\w+'? column"
)


def _apply_remote_observation_fields(
    local_id: int,
    remote: dict,
    *,
    fields: set[str] | None = None,
    identity_fail_closed: bool = False,
) -> str:
    """Apply cloud observation fields locally; returns the identity outcome.

    ``identity_fail_closed`` — see `_apply_remote_identity_to_local`.
    """
    identity_outcome = IDENTITY_APPLY_UNCHANGED
    requested_fields = {
        str(field or '').strip()
        for field in (fields or set(_SNAPSHOT_OBS_FIELDS))
        if str(field or '').strip()
    }
    if not requested_fields:
        return identity_outcome

    normalized_fields = {
        'sharing_scope' if field in {'visibility', 'sharing_scope'} else field
        for field in requested_fields
    }
    applies_identity = fields is None or TAXON_IDENTITY_SYNC_FIELD in normalized_fields
    local_before = ObservationDB.get_observation(int(local_id)) if applies_identity else None
    update_kwargs = _remote_observation_update_kwargs(remote)
    partial_kwargs = {
        key: value
        for key, value in update_kwargs.items()
        if key == 'allow_nulls' or key in normalized_fields
    }
    if len(partial_kwargs) > 1:
        ObservationDB.update_observation(int(local_id), **partial_kwargs)

    # Identity travels as one virtual field: a full apply ("cloud wins") or an
    # explicit request for it. Three-way reconciliation decides when to ask.
    if applies_identity:
        identity_outcome = _apply_remote_identity_to_local(
            int(local_id), remote,
            local_before=local_before,
            fail_closed_on_local_claim=identity_fail_closed,
            # An explicit request for the identity field comes from a
            # three-way decision (or the owner's choice) that the cloud's
            # current identity — including "none" — is the one to take.
            absence_is_evidence=fields is not None,
        )

    extra_values = _remote_observation_extra_values(remote)
    extra_updates = {
        key: value
        for key, value in extra_values.items()
        if key in normalized_fields
    }
    if not extra_updates:
        return identity_outcome

    conn = get_connection()
    try:
        assignments = [f'{column} = ?' for column in extra_updates]
        values = list(extra_updates.values())
        values.append(int(local_id))
        conn.execute(
            f"UPDATE observations SET {', '.join(assignments)} WHERE id = ?",
            tuple(values),
        )
        conn.commit()
    finally:
        conn.close()
    return identity_outcome


def _cloud_thumb_save_format(path: Path) -> tuple[str, str, dict]:
    if features.check('webp'):
        return 'WEBP', 'image/webp', {'quality': 65, 'method': 4}
    return 'JPEG', 'image/jpeg', {'quality': 72}


def _cloud_upload_policy_from_meta(upload_meta: dict | None) -> dict[str, object]:
    meta = dict(upload_meta or {})
    upload_mode = str(meta.get('upload_mode') or meta.get('uploadMode') or 'full').strip().lower() or 'full'
    cloud_plan = str(meta.get('cloud_plan') or meta.get('cloudPlan') or '').strip().lower()
    quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    profile = {}
    if cloud_plan:
        profile['cloud_plan'] = cloud_plan
    elif quality_profile == 'high':
        profile['is_pro'] = True
    return build_cloud_upload_policy(normalize_cloud_plan_profile(profile), upload_mode=upload_mode)


def _prepare_cloud_image_upload_file(
    source_path: str,
    temp_dir: Path,
    image_id: int,
    upload_meta: dict | None,
) -> tuple[Path, int, int, int, int, str, int | float | None]:
    source = Path(str(source_path or '').strip())
    if not source.exists():
        raise FileNotFoundError(source)

    policy = _cloud_upload_policy_from_meta(upload_meta)
    resize_max_pixels = max(
        1,
        int(
            policy.get('resizeMaxPixels')
            or policy.get('resize_max_pixels')
            or policy.get('maxPixels')
            or 0
        ) or 20_000_000,
    )
    resize_max_edge = policy.get('resizeMaxEdge') or policy.get('resize_max_edge')
    quality_profile = str(policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
    byte_cap = int(policy.get('fullImageByteCap') or 0)
    webp_qualities = list(build_full_image_webp_quality_attempts(quality_profile))

    if not features.check('webp'):
        raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE)

    with Image.open(source) as img:
        img = ImageOps.exif_transpose(img)
        source_width = int(img.width or 0)
        source_height = int(img.height or 0)
        if not source_width or not source_height:
            raise RuntimeError('Could not determine image dimensions')

        if img.mode in ('RGBA', 'LA') or 'transparency' in img.info:
            rgba = img.convert('RGBA')
            background = Image.new('RGB', rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel('A'))
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')

        target = scale_dimensions_to_max_pixels(source_width, source_height, resize_max_pixels, resize_max_edge)
        if target['resized']:
            img = img.resize((int(target['width']), int(target['height'])), Image.Resampling.LANCZOS)

        for attempt_quality in webp_qualities:
            buffer = io.BytesIO()
            img.save(buffer, 'WEBP', quality=attempt_quality, method=4)
            data = buffer.getvalue()
            if byte_cap and len(data) > byte_cap:
                continue

            out_path = temp_dir / f'cloud_{int(image_id):04d}.webp'
            out_path.write_bytes(data)
            try:
                source_stat = source.stat()
                os.utime(out_path, (source_stat.st_atime, source_stat.st_mtime))
            except Exception:
                pass

            return (
                out_path,
                source_width,
                source_height,
                int(img.width or 0),
                int(img.height or 0),
                'image/webp',
                attempt_quality,
            )

    raise CloudSyncError(IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE)


def _content_type_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == '.webp':
        return 'image/webp'
    return mimetypes.guess_type(path.name)[0] or 'image/jpeg'


def finalize_sync_candidates(
    client: "SporelyCloudClient",
    candidates: list[dict],
    *,
    prepare_images_cb=None,
) -> tuple[list[dict], list[dict]]:
    """The shared final gate used by both caller sites before the dialog opens.

    For each preflight-flagged candidate:

    1. Load read-only conflict detail.
    2. If ``automatic_decisions`` contains any operations, execute them
       through :func:`resolve_conflict_plan` (same executor used for manual
       plans — same drift/identity/finalization guarantees).
    3. Reread and reclassify.
    4. Keep only candidates whose FINAL detail has ``has_manual_conflicts=True``.

    Automatic-execution failures are returned separately as sync errors —
    they are never silently hidden.  A candidate that fails an automatic
    step and still has manual work is passed through so the dialog can
    receive it; a candidate that fails an automatic step with no remaining
    manual work becomes a plain sync error.
    """
    manual_candidates: list[dict] = []
    automatic_errors: list[dict] = []
    for candidate in candidates or []:
        try:
            local_id = int(candidate.get('local_id') or 0)
        except (TypeError, ValueError):
            local_id = 0
        cloud_id = str(candidate.get('cloud_id') or '').strip()
        if not local_id:
            # Malformed candidate — do not silently drop; treat as error.
            automatic_errors.append({
                'local_id': 0, 'cloud_id': cloud_id,
                'phase': 'read', 'error': 'candidate missing local_id',
            })
            continue
        try:
            detail = get_conflict_detail(client, local_id, cloud_id or None)
        except Exception as exc:
            automatic_errors.append({
                'local_id': local_id, 'cloud_id': cloud_id,
                'phase': 'read', 'error': str(exc),
            })
            continue
        auto_ops = detail.get('automatic_decisions') or {}
        has_automatic = bool(auto_ops.get('fields') or auto_ops.get('media'))
        has_manual = bool(detail.get('has_manual_conflicts'))
        if has_automatic:
            plan = _build_plan_from_automatic_decisions(detail)
            try:
                resolve_conflict_plan(
                    client, local_id,
                    cloud_id=cloud_id or None,
                    plan=plan,
                    prepare_images_cb=prepare_images_cb,
                )
            except Exception as exc:
                automatic_errors.append({
                    'local_id': local_id, 'cloud_id': cloud_id,
                    'phase': 'execute', 'error': str(exc),
                })
                if has_manual:
                    manual_candidates.append({**candidate, 'detail': detail})
                continue
        # Reread and reclassify.
        try:
            final_detail = get_conflict_detail(client, local_id, cloud_id or None)
        except Exception as exc:
            automatic_errors.append({
                'local_id': local_id, 'cloud_id': cloud_id,
                'phase': 'verify', 'error': str(exc),
            })
            continue
        if final_detail.get('has_manual_conflicts'):
            manual_candidates.append({**candidate, 'detail': final_detail})
        elif has_automatic:
            _clear_observation_conflict_review_pending(local_id)
    return manual_candidates, automatic_errors


def resolve_conflict_keep_local(
    client: "SporelyCloudClient",
    local_id: int,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')

    remote_obs = None
    cloud_value = str(local_obs.get('cloud_id') or '').strip()
    if cloud_value:
        try:
            remote_obs = client.get_observation(cloud_value)
        except Exception:
            remote_obs = None

    merged_obs = _merge_cloud_selected_ai_fields(local_obs, remote_obs)
    try:
        cloud_id = client.push_observation(
            merged_obs,
            remote_obs=remote_obs,
        )
    except Exception as exc:
        if not is_privacy_slot_limit_error(exc):
            raise
        blocked_reason = _set_observation_privacy_blocked(int(local_id), str(exc))
        return {
            'local_id': int(local_id),
            'cloud_id': None,
            'blocked': True,
            'blocked_reason': blocked_reason,
            'raw_error': str(exc),
        }
    _adopt_merge_filled_ai_fields_locally(local_id, local_obs, merged_obs)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=cloud_id,
            sync_status='synced',
            synced_at=datetime.now(timezone.utc).isoformat(),
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()

    should_push_images = prepare_images_cb is not None
    stored_local_media_signature = _load_local_cloud_media_signature(int(local_id))
    current_local_media_signature = _local_cloud_media_signature(int(local_id))
    local_media_changed = bool(
        stored_local_media_signature
        and current_local_media_signature
        and not _local_media_signatures_match(
            stored_local_media_signature,
            current_local_media_signature,
        )
    )
    if not local_media_changed:
        _store_local_media_signature_if_equivalent(
            int(local_id),
            stored_local_media_signature,
            current_local_media_signature,
        )

    remote_images_raw = _pull_remote_images_for_sync(client, cloud_id) if cloud_id else []
    if remote_images_raw:
        _record_remote_image_tombstones(
            remote_images_raw,
            local_observation_id=int(local_id),
            cloud_observation_id=cloud_id,
        )
        tombstoned_remote_image_keys = _deleted_remote_image_identity_keys(remote_images_raw)

    if should_push_images and cloud_id:
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id)
        if stored_snapshot:
            baseline_images = [dict(row or {}) for row in (_parse_cloud_observation_snapshot(stored_snapshot).get('images') or [])]
            remote_images = [
                dict(row or {})
                for row in remote_images_raw
                if should_pull_cloud_image_to_desktop(row)
                and not str(row.get('deleted_at') or '').strip()
            ]
            remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
            remote_image_changes = _analyze_image_changes(
                remote_image_payloads,
                baseline_images,
                ignored_keys=tombstoned_remote_image_keys,
            )
            should_push_images = bool(
                local_media_changed
                or remote_image_changes.get('added_keys')
                or remote_image_changes.get('removed_keys')
                or remote_image_changes.get('metadata_changed_keys')
            )

    if should_push_images:
        images_ok = _push_images_for_observation(
            client,
            local_obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            progress_cb=progress_cb,
            progress_state={'done': 0, 'total': 0},
            observation_index=1,
            observation_total=1,
        )
        if not images_ok:
            mark_observation_dirty(int(local_id))
            raise CloudSyncError(f'Could not fully upload images for observation {local_id}')

    _push_measurements_for_observation(client, int(local_id))
    try:
        _push_spore_mosaic_for_observation(client, int(local_id), cloud_id)
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        print(
            f'[cloud_sync] Mosaic push errored while keeping local observation '
            f'{int(local_id)}: {exc}',
            flush=True,
        )

    _store_remote_snapshot(client, cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    return {'local_id': int(local_id), 'cloud_id': cloud_id}


def resolve_conflict_keep_cloud(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
    allow_delete: bool = False,
) -> dict:
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    remote_obs = client.get_observation(resolved_cloud_id)
    if not remote_obs:
        raise CloudSyncError(f'Cloud observation {resolved_cloud_id} not found')
    remote_images_raw = _pull_remote_images_for_sync(client, resolved_cloud_id)
    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=int(local_id),
        cloud_observation_id=resolved_cloud_id,
    )
    remote_images = [
        dict(row or {})
        for row in remote_images_raw
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
    ]

    _apply_remote_observation_fields(int(local_id), remote_obs)
    warnings = _apply_remote_images_to_local(client, int(local_id), remote_images, allow_delete=allow_delete)
    remote_measurements = _pull_remote_measurements_for_images(
        client,
        [str(row.get('id') or '').strip() for row in remote_images if str(row.get('id') or '').strip()],
    )
    measurement_result = _import_remote_measurements_for_observation(
        client,
        int(local_id),
        resolved_cloud_id,
        remote_images=remote_images,
        remote_measurements=remote_measurements,
        materialize_remote_images=True,
        overwrite_conflicts=True,
    )
    warnings.extend(str(item) for item in (measurement_result.get('warnings') or []))
    if measurement_result.get('failed'):
        raise CloudSyncError(
            f'Could not apply all Sporely Cloud measurements for observation {local_id}'
        )
    _stamp_observation_synced(int(local_id), resolved_cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    _store_remote_snapshot(
        client,
        resolved_cloud_id,
        remote_obs,
        remote_images,
        remote_measurements=remote_measurements,
    )
    return {'local_id': int(local_id), 'cloud_id': resolved_cloud_id, 'warnings': warnings}


def resolve_conflict_merge(
    client: "SporelyCloudClient",
    local_id: int,
    cloud_id: str | None = None,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    # For merge, keep local observation but add any new images from cloud
    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    # First, pull any new images from cloud and add to local
    remote_obs = client.get_observation(resolved_cloud_id)
    if remote_obs:
        remote_images_raw = _pull_remote_images_for_sync(client, resolved_cloud_id)
        _record_remote_image_tombstones(
            remote_images_raw,
            local_observation_id=int(local_id),
            cloud_observation_id=resolved_cloud_id,
        )
        remote_images = [
            dict(row or {})
            for row in remote_images_raw
            if should_pull_cloud_image_to_desktop(row)
            and not str(row.get('deleted_at') or '').strip()
        ]
        warnings = _apply_remote_images_to_local(client, int(local_id), remote_images, allow_delete=False)
    else:
        warnings = []

    # Then push the local observation (which now includes merged images)
    merged_obs = _merge_cloud_selected_ai_fields(local_obs, remote_obs)
    try:
        cloud_id = client.push_observation(
            merged_obs,
            remote_obs=remote_obs,
        )
    except Exception as exc:
        if not is_privacy_slot_limit_error(exc):
            raise
        blocked_reason = _set_observation_privacy_blocked(int(local_id), str(exc))
        return {
            'local_id': int(local_id),
            'cloud_id': None,
            'blocked': True,
            'blocked_reason': blocked_reason,
            'raw_error': str(exc),
            'warnings': warnings,
        }
    _adopt_merge_filled_ai_fields_locally(local_id, local_obs, merged_obs)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        update_observation_sync_state(
            cursor,
            int(local_id),
            cloud_id=cloud_id,
            sync_status='synced',
            synced_at=datetime.now(timezone.utc).isoformat(),
            clear_sync_error_state=True,
        )
        conn.commit()
    finally:
        conn.close()

    # Push images if needed
    should_push_images = prepare_images_cb is not None
    if should_push_images:
        images_ok = _push_images_for_observation(
            client,
            local_obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            progress_cb=progress_cb,
            progress_state={'done': 0, 'total': 0},
            observation_index=1,
            observation_total=1,
        )
        if not images_ok:
            mark_observation_dirty(int(local_id))
            raise CloudSyncError(f'Could not fully upload images for observation {local_id}')

    _store_remote_snapshot(client, cloud_id)
    _refresh_local_cloud_media_signature(int(local_id))
    return {'local_id': int(local_id), 'cloud_id': cloud_id, 'warnings': warnings}


def _format_recomputed_spore_statistics(observation_id: int) -> str | None:
    stats = MeasurementDB.get_statistics_for_observation(
        int(observation_id), measurement_category='spores'
    )
    if not stats:
        return None
    text = (
        f"Spores: ({stats['length_min']:.1f}-){stats['length_p5']:.1f}-"
        f"{stats['length_p95']:.1f}(-{stats['length_max']:.1f}) um"
    )
    if stats.get('width_mean', 0) > 0:
        text += (
            f" x ({stats['width_min']:.1f}-){stats['width_p5']:.1f}-"
            f"{stats['width_p95']:.1f}(-{stats['width_max']:.1f}) um"
            f", Q = ({stats['ratio_min']:.1f}-){stats['ratio_p5']:.1f}-"
            f"{stats['ratio_p95']:.1f}(-{stats['ratio_max']:.1f})"
            f", Qm = {stats['ratio_mean']:.1f}"
        )
    return text + f", n = {stats['count']}"


# ── Per-item conflict plan — helpers ──────────────────────────────────────────


def _capture_local_presentation(rows: list[dict]) -> dict[int, dict]:
    """Snapshot ``gallery_rotation`` and ``sort_order`` per local image id."""
    capture: dict[int, dict] = {}
    for row in rows or []:
        rid = _safe_int(row.get('id'))
        if rid <= 0:
            continue
        capture[rid] = {
            'gallery_rotation': row.get('gallery_rotation'),
            'sort_order': row.get('sort_order'),
        }
    return capture


def _restore_local_presentation(
    captured: dict[int, dict],
    *,
    matched_local_ids: set[int],
) -> list[dict]:
    """Restore captured rotation/sort_order for matched images and verify.

    A3: presentation is no longer silently swallowed.  Each attempted write
    is followed by a read-back that verifies the row now has the intended
    ``gallery_rotation`` and ``sort_order``.  Per-row status is returned so
    the resolver can surface it in the operation log.
    """
    statuses: list[dict] = []
    for local_id in matched_local_ids:
        snapshot = captured.get(int(local_id))
        if not snapshot:
            continue
        expected_rotation = snapshot.get('gallery_rotation')
        expected_order = snapshot.get('sort_order')
        error_message: str | None = None
        try:
            ImageDB.update_image(
                int(local_id),
                gallery_rotation=expected_rotation,
                sort_order=expected_order,
            )
        except Exception as exc:
            error_message = f'update_image failed: {exc}'
        try:
            current_rows = ImageDB.get_images_for_observation(int(local_id))
        except Exception as exc:
            current_rows = []
            error_message = error_message or f'read-back failed: {exc}'
        current = next(
            (row for row in current_rows if _safe_int(row.get('id')) == int(local_id)),
            None,
        )
        status = 'failed'
        if error_message is None and current is not None:
            actual_rotation = current.get('gallery_rotation')
            actual_order = current.get('sort_order')
            already = (
                actual_rotation == expected_rotation and actual_order == expected_order
            )
            status = 'completed' if already else 'failed'
            if status == 'failed':
                error_message = (
                    f'presentation drift after restore: expected rotation={expected_rotation!r} '
                    f'order={expected_order!r}, got rotation={actual_rotation!r} '
                    f'order={actual_order!r}'
                )
        statuses.append({
            'op': 'restore_presentation',
            'local_id': int(local_id),
            'status': status,
            'expected': {'gallery_rotation': expected_rotation, 'sort_order': expected_order},
            'error': error_message,
        })
    return statuses


def _assign_downloaded_image_order(
    *,
    local_id: int,
    downloaded_cloud_ids: set[str],
    captured_before: dict[int, dict],
) -> list[dict]:
    """Give newly-downloaded cloud-only images deterministic non-colliding order.

    Returns per-row status entries so the resolver can surface completed vs
    failed presentation policy in the plan operation log.
    """
    statuses: list[dict] = []
    if not downloaded_cloud_ids:
        return statuses
    try:
        current_rows = ImageDB.get_images_for_observation(int(local_id))
    except Exception as exc:
        return [{
            'op': 'assign_downloaded_order',
            'status': 'failed',
            'cloud_ids': sorted(downloaded_cloud_ids),
            'error': f'read images failed: {exc}',
        }]
    new_rows = [
        row for row in current_rows
        if str(row.get('cloud_id') or '').strip() in downloaded_cloud_ids
        and _safe_int(row.get('id')) not in captured_before
    ]
    if not new_rows:
        return statuses
    existing_orders = [_safe_int(v.get('sort_order')) for v in captured_before.values()]
    base = (max(existing_orders) + 1) if existing_orders else 1
    for offset, row in enumerate(
        sorted(new_rows, key=lambda r: str(r.get('cloud_id') or ''))
    ):
        expected_order = base + offset
        row_local_id = _safe_int(row.get('id'))
        error_message: str | None = None
        try:
            ImageDB.update_image(int(row_local_id), sort_order=expected_order)
        except Exception as exc:
            error_message = f'update_image failed: {exc}'
        try:
            after_rows = ImageDB.get_images_for_observation(int(local_id))
        except Exception as exc:
            after_rows = []
            error_message = error_message or f'read-back failed: {exc}'
        current = next(
            (r for r in after_rows if _safe_int(r.get('id')) == row_local_id),
            None,
        )
        status = 'failed'
        if error_message is None and current is not None:
            status = 'completed' if _safe_int(current.get('sort_order')) == expected_order else 'failed'
            if status == 'failed':
                error_message = (
                    f'assigned order drift: expected {expected_order}, '
                    f'got {current.get("sort_order")!r}'
                )
        statuses.append({
            'op': 'assign_downloaded_order',
            'local_id': row_local_id,
            'cloud_id': str(row.get('cloud_id') or '').strip(),
            'expected_sort_order': expected_order,
            'status': status,
            'error': error_message,
        })
    return statuses


def resolve_conflict_plan(
    client: "SporelyCloudClient",
    local_id: int,
    *,
    cloud_id: str | None = None,
    plan: dict | None = None,
    prepare_images_cb: PreparedImagesCallback | None = None,
    prior_result: dict | None = None,
) -> dict:
    """Apply one explicit, non-destructive per-item conflict plan.

    Turn-A execution order:

    1. Refetch local and cloud state.
    2. Validate identity (belt-and-braces of UI-side detection).
    3. Verify the reviewed plan against the fresh state (drift check).
    4. Build an explicit per-(kind, side, choice) operation list.
    5. Capture local presentation state so cloud metadata cannot silently
       overwrite ``gallery_rotation`` / ``sort_order`` on matched images.
    6. Execute every operation exactly once.
    7. Restore presentation for matched images; assign deterministic order to
       downloaded cloud-only images.
    8. Recompute spore statistics from the *final* measurement set, preserving
       cloud value when recomputation legitimately yields nothing.
    9. Finalize: store snapshot → refresh media signature → stamp synced.
    """
    selected_plan = dict(plan or {})
    if selected_plan.get('allow_media_deletion'):
        raise CloudSyncError('Conflict plans cannot authorize media deletion')

    local_obs = ObservationDB.get_observation(int(local_id))
    if not local_obs:
        raise CloudSyncError(f'Local observation {local_id} not found')
    resolved_cloud_id = str(cloud_id or local_obs.get('cloud_id') or '').strip()
    if not resolved_cloud_id:
        raise CloudSyncError(f'Observation {local_id} is not linked to Sporely Cloud')

    items = [dict(item or {}) for item in (selected_plan.get('items') or [])]
    baseline = selected_plan.get('baseline')
    # A1: item-level plans MUST carry a reviewed baseline.  Old whole-observation
    # resolvers (resolve_conflict_keep_local / _keep_cloud / _merge) are separate
    # APIs and are unaffected.
    _validate_plan_baseline_shape(baseline)
    baseline = dict(baseline)

    # ── B2: partial-retry — carry forward completed operations ─────────────
    completed_prior_ops: list[dict] = []
    completed_prior_keys: set = set()
    if isinstance(prior_result, dict):
        for op in prior_result.get('operations') or []:
            if isinstance(op, dict) and op.get('status') in {
                'completed', 'verification_pending'
            }:
                completed_prior_ops.append(dict(op))
                completed_prior_keys |= _plan_dispatch_keys(op)

    # ── Step 1: refetch current state ─────────────────────────────────────────
    remote_obs = client.get_observation(resolved_cloud_id)
    remote_images = [
        dict(row or {})
        for row in (_pull_remote_images_for_sync(client, resolved_cloud_id) or [])
        if not str((row or {}).get('deleted_at') or '').strip()
        and should_pull_cloud_image_to_desktop(row)
    ]
    remote_measurements = _pull_remote_measurements_for_images(
        client,
        [
            str(row.get('id') or '').strip()
            for row in remote_images
            if str(row.get('id') or '').strip()
        ],
    )
    current_local_images = ImageDB.get_images_for_observation(int(local_id))
    current_local_measurements = MeasurementDB.get_measurements_for_observation(int(local_id))

    # ── Step 2: identity validation ───────────────────────────────────────────
    _validate_plan_identity_state(
        local_images=current_local_images,
        remote_images=remote_images,
        local_measurements=current_local_measurements,
        remote_measurements=remote_measurements,
        items=items,
    )

    # ── Step 2b: retry rebasing ──────────────────────────────────────────────
    # Turn-B fix 1: expected-effect-aware rebasing.  If we were handed a
    # ``prior_result`` from an earlier PartialConflictPlanError, verify each
    # completed op's ``expected_after`` against the current state, ensure that
    # every UNRELATED record still matches the original baseline, then replace
    # the baseline with a fresh one so the drift check for the remaining
    # (pending/failed) ops runs against the rebased state.
    verified_completed_ops: list[dict] = []
    if isinstance(prior_result, dict) and prior_result.get('operations'):
        baseline, rebased_keys, verified_completed_ops = _verify_completed_ops_and_rebase(
            prior_result, baseline,
            local_obs=local_obs, remote_obs=remote_obs,
            local_images=current_local_images, remote_images=remote_images,
            local_measurements=current_local_measurements,
            remote_measurements=remote_measurements,
        )
        # Expand the completed keys to include plan-dispatch variants.
        for op in prior_result.get('operations') or []:
            if isinstance(op, dict) and op.get('status') in {
                'completed', 'verification_pending'
            }:
                completed_prior_keys |= _plan_dispatch_keys(op)
        completed_prior_keys |= rebased_keys

    # ── Step 3: drift validation against (possibly rebased) baseline ─────────
    _verify_plan_baseline(
        baseline,
        items,
        local_obs=local_obs,
        remote_obs=remote_obs,
        local_images=current_local_images,
        remote_images=remote_images,
        local_measurements=current_local_measurements,
        remote_measurements=remote_measurements,
    )

    # ── Step 4: build the explicit operation list ─────────────────────────────
    ops = [
        op for op in _build_plan_operations(items)
        if _stable_op_key(op) not in completed_prior_keys
    ]

    # ── Step 5: capture local presentation state ──────────────────────────────
    presentation_before = _capture_local_presentation(current_local_images)

    # ── Step 6: execute operations, in a stable order ─────────────────────────
    # Retry carries verified completed prior ops forward as ``already_complete``
    # entries so the return value reports the full history.
    executed: list[dict] = list(verified_completed_ops) if verified_completed_ops else list(completed_prior_ops)
    matched_image_local_ids: set[int] = set()

    def _partial_error(message: str, failing: dict | None = None,
                       cause: Exception | None = None) -> PartialConflictPlanError:
        pending = list(executed)
        if failing is not None:
            pending.append({
                **failing,
                'status': 'failed',
                'error': str(cause) if cause else message,
            })
        err = PartialConflictPlanError(
            message,
            partial_result={
                'local_id': int(local_id),
                'cloud_id': resolved_cloud_id,
                'plan_applied': False,
                'operations': pending,
            },
        )
        if cause is not None:
            err.__cause__ = cause
        return err

    # Field: pull from cloud
    cloud_field_names = {
        str(op.get('field') or '')
        for op in ops
        if op.get('op') == 'pull_field' and op.get('field')
    }
    if cloud_field_names:
        try:
            _apply_remote_observation_fields(int(local_id), remote_obs, fields=cloud_field_names)
        except Exception as exc:
            raise _partial_error(
                f'Could not apply cloud fields to local: {exc}',
                failing={'op': 'pull_field', 'fields': sorted(cloud_field_names)},
                cause=exc,
            )
        # Record the expected local value that pull_field wrote — retry
        # verifies the current local field still matches.
        refreshed_after_pull = ObservationDB.get_observation(int(local_id)) or local_obs
        for f in sorted(cloud_field_names):
            executed.append({
                'op': 'pull_field', 'field': f, 'status': 'completed',
                'stable_identity': {'field': f, 'side': 'local'},
                'expected_after': {
                    'side': 'local',
                    'field': f,
                    'value': _normalize_observation_field_for_baseline(
                        refreshed_after_pull, f, local=True,
                    ),
                },
            })

    # Field: push to cloud
    local_field_names = {
        str(op.get('field') or '')
        for op in ops
        if op.get('op') == 'push_field' and op.get('field')
    }
    if local_field_names:
        try:
            refreshed_local = ObservationDB.get_observation(int(local_id)) or local_obs
            local_payload = _observation_push_payload(refreshed_local, local=True)
            patch_payload = {
                ('visibility' if field == 'sharing_scope' else field): local_payload.get(
                    'visibility' if field in {'visibility', 'sharing_scope'} else field
                )
                for field in local_field_names
                if field != TAXON_IDENTITY_SYNC_FIELD
            }
            if patch_payload:
                client._patch(f'observations?id=eq.{resolved_cloud_id}', patch_payload)
            # "Keep this device's identity": the guarded RPC is the only
            # desktop → cloud identity channel, and it accepts only a proven
            # identity (anything else is a skip, never a clear).
            if TAXON_IDENTITY_SYNC_FIELD in local_field_names:
                client._sync_observation_selected_taxon(
                    resolved_cloud_id, refreshed_local, remote_obs=remote_obs,
                )
        except Exception as exc:
            raise _partial_error(
                f'Could not push local fields to cloud: {exc}',
                failing={'op': 'push_field', 'fields': sorted(local_field_names)},
                cause=exc,
            )
        for f in sorted(local_field_names):
            expected_value = _normalize_observation_field_for_baseline(
                refreshed_local, f, local=True,
            )
            if (
                f == TAXON_IDENTITY_SYNC_FIELD
                and not TaxonIdentity.from_row(refreshed_local).is_proven_sporely
            ):
                # The RPC gate skipped an unproven identity: the cloud keeps
                # what it had, and that is the verified effect.
                expected_value = _normalize_observation_field_for_baseline(
                    remote_obs, f, local=False,
                )
            executed.append({
                'op': 'push_field', 'field': f, 'status': 'completed',
                'stable_identity': {'field': f, 'side': 'cloud'},
                'expected_after': {
                    'side': 'cloud',
                    'field': f,
                    'value': expected_value,
                },
            })

    # Image: import from cloud (materialize) + apply metadata from cloud
    remote_ids_to_import = {
        str(op.get('cloud_id') or '')
        for op in ops
        if op.get('op') == 'import_image' and str(op.get('cloud_id') or '')
    }
    remote_ids_metadata_only = {
        str(op.get('cloud_id') or '')
        for op in ops
        if op.get('op') == 'apply_image_metadata' and str(op.get('cloud_id') or '')
    }
    combined_remote_ids = remote_ids_to_import | remote_ids_metadata_only
    if combined_remote_ids:
        try:
            _apply_remote_images_to_local(
                client,
                int(local_id),
                [row for row in remote_images if str(row.get('id') or '') in combined_remote_ids],
                allow_delete=False,
                materialize_remote_images=True,
            )
        except Exception as exc:
            raise _partial_error(
                f'Could not apply cloud images to local: {exc}',
                failing={'op': 'apply_or_import_image',
                         'cloud_ids': sorted(combined_remote_ids)},
                cause=exc,
            )
        try:
            local_after = ImageDB.get_images_for_observation(int(local_id))
            local_by_cloud = {
                str(r.get('cloud_id') or '').strip(): r for r in local_after
                if str(r.get('cloud_id') or '').strip()
            }
            remote_by_cloud_after = {
                str(row.get('id') or '').strip(): row for row in remote_images
                if str(row.get('id') or '').strip()
            }
            for cid in sorted(combined_remote_ids):
                local_link = local_by_cloud.get(cid) or {}
                remote_row = remote_by_cloud_after.get(cid) or {}
                op_kind = ('import_image' if cid in remote_ids_to_import
                           else 'apply_image_metadata')
                executed.append({
                    'op': op_kind,
                    'cloud_id': cid,
                    'local_id': _safe_int(local_link.get('id')) or None,
                    'status': 'completed',
                    'stable_identity': {'cloud_id': cid,
                                        'local_id': _safe_int(local_link.get('id')) or None},
                    'expected_after': {
                        'kind': 'image',
                        'material_local': _material_image_expected_state(
                            local_link, side='local'
                        ),
                        'material_remote': _material_image_expected_state(
                            remote_row, side='remote'
                        ),
                    },
                })
        except Exception as exc:
            remote_by_cloud_after = {
                str(row.get('id') or '').strip(): row for row in remote_images
                if str(row.get('id') or '').strip()
            }
            for cid in sorted(combined_remote_ids):
                remote_row = remote_by_cloud_after.get(cid) or {}
                op_kind = ('import_image' if cid in remote_ids_to_import
                           else 'apply_image_metadata')
                executed.append({
                    'op': op_kind,
                    'cloud_id': cid,
                    'status': 'verification_pending',
                    'write_attempted': True,
                    'stable_identity': {'cloud_id': cid},
                    'intended_after': _intended_after_image_from_remote(remote_row),
                })
            raise _partial_error(
                f'Cloud→local image apply succeeded but verification read failed: {exc}',
                failing=None, cause=exc,
            )

    # Track matched image local ids for later presentation restore.
    # Matched = the local id appears in an image_metadata op (this attempt) OR
    # was touched by an already-verified image op from a prior attempt.  Even
    # if the underlying apply is not re-run this attempt, presentation restore
    # is a nonblocking write that MUST still be re-attempted so a failed
    # presentation from the prior attempt is retried.
    for op in ops:
        if op.get('op') in {'apply_image_metadata', 'push_image_metadata'}:
            lid = _safe_int(op.get('local_id'))
            if lid:
                matched_image_local_ids.add(lid)
    for op in verified_completed_ops or []:
        if op.get('op') in {'apply_image_metadata', 'push_image_metadata'}:
            lid = _safe_int(op.get('local_id'))
            if lid:
                matched_image_local_ids.add(lid)

    # Image: upload
    local_image_ids_to_push = {
        _safe_int(op.get('local_id'))
        for op in ops
        if op.get('op') == 'push_image' and _safe_int(op.get('local_id')) > 0
    }
    if local_image_ids_to_push and prepare_images_cb is None:
        raise CloudSyncError('Selected local images require the desktop image preparer')
    if local_image_ids_to_push:
        try:
            refreshed_local = ObservationDB.get_observation(int(local_id)) or local_obs
            pushed_ok = _push_images_for_observation(
                client,
                refreshed_local,
                resolved_cloud_id,
                prepare_images_cb=prepare_images_cb,
                include_image_ids=local_image_ids_to_push,
            )
        except Exception as exc:
            raise _partial_error(
                f'Could not upload all selected local images: {exc}',
                failing={'op': 'push_image', 'local_ids': sorted(local_image_ids_to_push)},
                cause=exc,
            )
        if not pushed_ok:
            raise _partial_error(
                'Could not upload all selected local images',
                failing={'op': 'push_image', 'local_ids': sorted(local_image_ids_to_push)},
            )
        # Reread both sides.  If the verification read-back fails after the
        # write succeeded, emit ``verification_pending`` ops so retry can
        # discover the created rows via stable identity without re-dispatch.
        try:
            local_after_push = ImageDB.get_images_for_observation(int(local_id))
            by_local_id = {_safe_int(r.get('id')): r for r in local_after_push
                           if _safe_int(r.get('id'))}
            remote_after_push = [
                dict(row or {})
                for row in (_pull_remote_images_for_sync(client, resolved_cloud_id) or [])
                if not str((row or {}).get('deleted_at') or '').strip()
                and should_pull_cloud_image_to_desktop(row)
            ]
            remote_by_desktop = {_safe_int(r.get('desktop_id')): r for r in remote_after_push
                                 if _safe_int(r.get('desktop_id'))}
            for lid in sorted(local_image_ids_to_push):
                local_row = by_local_id.get(int(lid)) or {}
                cid_after = str(local_row.get('cloud_id') or '').strip() or None
                remote_row = remote_by_desktop.get(int(lid)) or {}
                executed.append({
                    'op': 'push_image', 'local_id': lid,
                    'cloud_id': cid_after,
                    'status': 'completed',
                    'stable_identity': {'local_id': lid, 'cloud_id': cid_after},
                    'expected_after': {
                        'kind': 'image',
                        'material_local': _material_image_expected_state(
                            local_row, side='local'
                        ),
                        'material_remote': _material_image_expected_state(
                            remote_row, side='remote'
                        ),
                    },
                })
        except Exception as exc:
            # Cloud write succeeded but verification fetch failed.  Capture
            # intended_after from the local rows we tried to push, then raise
            # a partial error so the caller can retry safely.
            best_local_rows = {_safe_int(r.get('id')): r
                               for r in current_local_images
                               if _safe_int(r.get('id'))}
            for lid in sorted(local_image_ids_to_push):
                source = best_local_rows.get(int(lid)) or {}
                cid = str(source.get('cloud_id') or '').strip() or None
                executed.append({
                    'op': 'push_image', 'local_id': lid,
                    'cloud_id': cid,
                    'status': 'verification_pending',
                    'write_attempted': True,
                    'stable_identity': {'local_id': lid, 'cloud_id': cid},
                    'intended_after': _intended_after_image_from_local(source),
                })
            raise _partial_error(
                f'Push succeeded but verification fetch failed: {exc}',
                failing=None, cause=exc,
            )

    # Image metadata: local → cloud.  Only when the user chose 'local'.
    local_metadata_image_ids_to_push = {
        _safe_int(op.get('local_id'))
        for op in ops
        if op.get('op') == 'push_image_metadata' and _safe_int(op.get('local_id')) > 0
    }
    if local_metadata_image_ids_to_push and prepare_images_cb is None:
        raise CloudSyncError('Selected local images require the desktop image preparer')
    if local_metadata_image_ids_to_push:
        try:
            refreshed_local = ObservationDB.get_observation(int(local_id)) or local_obs
            pushed_ok = _push_images_for_observation(
                client,
                refreshed_local,
                resolved_cloud_id,
                prepare_images_cb=prepare_images_cb,
                include_image_ids=local_metadata_image_ids_to_push,
            )
        except Exception as exc:
            raise _partial_error(
                f'Could not push local image metadata: {exc}',
                failing={'op': 'push_image_metadata',
                         'local_ids': sorted(local_metadata_image_ids_to_push)},
                cause=exc,
            )
        if not pushed_ok:
            raise _partial_error(
                'Could not push local image metadata',
                failing={'op': 'push_image_metadata',
                         'local_ids': sorted(local_metadata_image_ids_to_push)},
            )
        try:
            local_after_meta = ImageDB.get_images_for_observation(int(local_id))
            by_local_id_meta = {_safe_int(r.get('id')): r for r in local_after_meta
                                if _safe_int(r.get('id'))}
            remote_after_meta = [
                dict(row or {})
                for row in (_pull_remote_images_for_sync(client, resolved_cloud_id) or [])
                if not str((row or {}).get('deleted_at') or '').strip()
                and should_pull_cloud_image_to_desktop(row)
            ]
            remote_meta_by_desktop = {_safe_int(r.get('desktop_id')): r
                                      for r in remote_after_meta
                                      if _safe_int(r.get('desktop_id'))}
            for lid in sorted(local_metadata_image_ids_to_push):
                local_row = by_local_id_meta.get(int(lid)) or {}
                remote_row = remote_meta_by_desktop.get(int(lid)) or {}
                cid_after = str(local_row.get('cloud_id') or '').strip() or None
                executed.append({
                    'op': 'push_image_metadata', 'local_id': lid,
                    'cloud_id': cid_after,
                    'status': 'completed',
                    'stable_identity': {'local_id': lid, 'cloud_id': cid_after},
                    'expected_after': {
                        'kind': 'image',
                        'material_local': _material_image_expected_state(
                            local_row, side='local'
                        ),
                        'material_remote': _material_image_expected_state(
                            remote_row, side='remote'
                        ),
                    },
                })
        except Exception as exc:
            best_local_rows = {_safe_int(r.get('id')): r for r in current_local_images
                               if _safe_int(r.get('id'))}
            for lid in sorted(local_metadata_image_ids_to_push):
                source = best_local_rows.get(int(lid)) or {}
                cid = str(source.get('cloud_id') or '').strip() or None
                executed.append({
                    'op': 'push_image_metadata', 'local_id': lid,
                    'cloud_id': cid,
                    'status': 'verification_pending',
                    'write_attempted': True,
                    'stable_identity': {'local_id': lid, 'cloud_id': cid},
                    'intended_after': _intended_after_image_from_local(source),
                })
            raise _partial_error(
                f'Metadata push succeeded but verification fetch failed: {exc}',
                failing=None, cause=exc,
            )

    # Measurement: cloud → local (matched with choice=cloud, or cloud_only+download)
    remote_measurement_ids_to_apply = {
        str(op.get('cloud_id') or '')
        for op in ops
        if op.get('op') == 'import_measurement' and str(op.get('cloud_id') or '')
    }
    if remote_measurement_ids_to_apply:
        try:
            result = _import_remote_measurements_for_observation(
                client,
                int(local_id),
                resolved_cloud_id,
                remote_images=remote_images,
                remote_measurements=[
                    row for row in remote_measurements
                    if str(row.get('id') or '') in remote_measurement_ids_to_apply
                ],
                materialize_remote_images=True,
                overwrite_conflicts=True,
            )
        except Exception as exc:
            raise _partial_error(
                f'Could not apply selected cloud measurements: {exc}',
                failing={'op': 'import_measurement',
                         'cloud_ids': sorted(remote_measurement_ids_to_apply)},
                cause=exc,
            )
        if result.get('failed'):
            raise _partial_error(
                'Could not apply selected cloud measurements',
                failing={'op': 'import_measurement',
                         'cloud_ids': sorted(remote_measurement_ids_to_apply)},
            )
        try:
            local_after_import = MeasurementDB.get_measurements_for_observation(int(local_id))
            by_cloud_id_import = {
                str(r.get('cloud_id') or '').strip(): r for r in local_after_import
                if str(r.get('cloud_id') or '').strip()
            }
            remote_by_cloud_import = {
                str(r.get('id') or '').strip(): r for r in remote_measurements
                if str(r.get('id') or '').strip()
            }
            for cid in sorted(remote_measurement_ids_to_apply):
                local_row = by_cloud_id_import.get(cid) or {}
                remote_row = remote_by_cloud_import.get(cid) or {}
                executed.append({
                    'op': 'import_measurement', 'cloud_id': cid,
                    'local_id': _safe_int(local_row.get('id')) or None,
                    'status': 'completed',
                    'stable_identity': {'cloud_id': cid,
                                        'local_id': _safe_int(local_row.get('id')) or None},
                    'expected_after': {
                        'kind': 'measurement',
                        'material_local': _material_measurement_expected_state(
                            local_row, side='local'
                        ),
                        'material_remote': _material_measurement_expected_state(
                            remote_row, side='remote'
                        ),
                    },
                })
        except Exception as exc:
            remote_by_cloud_import = {
                str(r.get('id') or '').strip(): r for r in remote_measurements
                if str(r.get('id') or '').strip()
            }
            for cid in sorted(remote_measurement_ids_to_apply):
                remote_row = remote_by_cloud_import.get(cid) or {}
                executed.append({
                    'op': 'import_measurement', 'cloud_id': cid,
                    'status': 'verification_pending',
                    'write_attempted': True,
                    'stable_identity': {'cloud_id': cid},
                    'intended_after': _intended_after_measurement_from_remote(remote_row),
                })
            raise _partial_error(
                f'Measurement import succeeded but verification read failed: {exc}',
                failing=None, cause=exc,
            )

    # Measurement: local → cloud.  Only ids explicitly marked as push_measurement.
    local_measurement_ids_to_push = {
        _safe_int(op.get('local_id'))
        for op in ops
        if op.get('op') == 'push_measurement' and _safe_int(op.get('local_id')) > 0
    }
    if local_measurement_ids_to_push:
        try:
            _push_measurements_for_observation(
                client,
                int(local_id),
                measurement_ids=local_measurement_ids_to_push,
            )
        except Exception as exc:
            raise _partial_error(
                f'Could not push local measurements: {exc}',
                failing={'op': 'push_measurement',
                         'local_ids': sorted(local_measurement_ids_to_push)},
                cause=exc,
            )
        try:
            local_after_meas_push = MeasurementDB.get_measurements_for_observation(int(local_id))
            by_local_id_mp = {_safe_int(r.get('id')): r for r in local_after_meas_push
                              if _safe_int(r.get('id'))}
            remote_after_meas_push = _pull_remote_measurements_for_images(
                client,
                [str(row.get('id') or '').strip() for row in remote_images
                 if str(row.get('id') or '').strip()],
            )
            remote_by_desktop_mp = {_safe_int(r.get('desktop_id')): r
                                    for r in remote_after_meas_push
                                    if _safe_int(r.get('desktop_id'))}
            for lid in sorted(local_measurement_ids_to_push):
                local_row = by_local_id_mp.get(int(lid)) or {}
                remote_row = remote_by_desktop_mp.get(int(lid)) or {}
                cid_after = str(local_row.get('cloud_id') or '').strip() or None
                executed.append({
                    'op': 'push_measurement', 'local_id': lid,
                    'cloud_id': cid_after,
                    'status': 'completed',
                    'stable_identity': {'local_id': lid, 'cloud_id': cid_after},
                    'expected_after': {
                        'kind': 'measurement',
                        'material_local': _material_measurement_expected_state(
                            local_row, side='local'
                        ),
                        'material_remote': _material_measurement_expected_state(
                            remote_row, side='remote'
                        ),
                    },
                })
        except Exception as exc:
            best_local_rows = {_safe_int(r.get('id')): r
                               for r in current_local_measurements
                               if _safe_int(r.get('id'))}
            for lid in sorted(local_measurement_ids_to_push):
                source = best_local_rows.get(int(lid)) or {}
                cid = str(source.get('cloud_id') or '').strip() or None
                executed.append({
                    'op': 'push_measurement', 'local_id': lid,
                    'cloud_id': cid,
                    'status': 'verification_pending',
                    'write_attempted': True,
                    'stable_identity': {'local_id': lid, 'cloud_id': cid},
                    'intended_after': _intended_after_measurement_from_local(source),
                })
            raise _partial_error(
                f'Measurement push succeeded but verification fetch failed: {exc}',
                failing=None, cause=exc,
            )

    # keep-asymmetric ops are recorded here; B4 turns them into a durable
    # accepted-asymmetry baseline that _store_remote_snapshot preserves.
    for op in ops:
        if op.get('op') in {'keep_asymmetric_image', 'keep_asymmetric_measurement'}:
            recorded = dict(op)
            recorded['status'] = 'completed'
            executed.append(recorded)

    # ── Step 7: restore presentation on matched images ────────────────────────
    presentation_statuses = _restore_local_presentation(
        presentation_before, matched_local_ids=matched_image_local_ids
    )
    presentation_statuses.extend(_assign_downloaded_image_order(
        local_id=int(local_id),
        downloaded_cloud_ids=remote_ids_to_import,
        captured_before=presentation_before,
    ))
    executed.extend(presentation_statuses)
    presentation_warnings = [
        status for status in presentation_statuses if status.get('status') != 'completed'
    ]

    # ── Step 8: derived statistics ────────────────────────────────────────────
    split_measurement_sets = any(
        op.get('op') in {'keep_asymmetric_measurement'} for op in ops
    )
    statistics_expected = bool(
        MeasurementDB.get_measurements_for_observation(int(local_id))
    )
    if (
        selected_plan.get('derived_statistics') == 'recompute_from_measurements'
        and not split_measurement_sets
    ):
        try:
            recomputed = _format_recomputed_spore_statistics(int(local_id))
        except Exception as exc:
            raise _partial_error(
                f'Could not recompute spore statistics: {exc}',
                failing={'op': 'recompute_spore_statistics'}, cause=exc,
            )
        if recomputed is None or not str(recomputed).strip():
            if statistics_expected:
                raise _partial_error(
                    'Could not recompute spore statistics from the selected measurements',
                    failing={'op': 'recompute_spore_statistics'},
                )
            # No measurements: preserve the current cloud value.
            executed.append({'op': 'preserve_spore_statistics_no_measurements',
                             'status': 'completed'})
        else:
            try:
                ObservationDB.update_spore_statistics(int(local_id), recomputed)
                client._patch(
                    f'observations?id=eq.{resolved_cloud_id}',
                    {'spore_statistics': recomputed},
                )
            except Exception as exc:
                raise _partial_error(
                    f'Could not persist recomputed spore statistics: {exc}',
                    failing={'op': 'recompute_spore_statistics'}, cause=exc,
                )
            executed.append({
                'op': 'recompute_spore_statistics', 'status': 'completed',
                'stable_identity': {'field': 'spore_statistics'},
                'expected_after': {
                    'side': 'both',
                    'field': 'spore_statistics',
                    'value': _normalize_observation_json_value(recomputed),
                },
            })

    # ── Step 9: build accepted-asymmetry entries from the plan ────────────────
    # Reread current local rows so freshly-created cloud IDs (from upload) or
    # freshly-linked local rows (from download) are visible for reconciliation.
    reconciled_local_images = ImageDB.get_images_for_observation(int(local_id))
    reconciled_local_measurements = MeasurementDB.get_measurements_for_observation(int(local_id))
    reconciled_remote_images = [
        dict(row or {})
        for row in (_pull_remote_images_for_sync(client, resolved_cloud_id) or [])
        if not str((row or {}).get('deleted_at') or '').strip()
        and should_pull_cloud_image_to_desktop(row)
    ]
    reconciled_remote_measurements = _pull_remote_measurements_for_images(
        client,
        [str(row.get('id') or '').strip()
         for row in reconciled_remote_images if str(row.get('id') or '').strip()],
    )
    matched_cloud_image_ids: set[str] = set(
        str(row.get('cloud_id') or '').strip()
        for row in reconciled_local_images
        if str(row.get('cloud_id') or '').strip()
    )
    matched_local_image_ids_now = matched_image_local_ids | {
        _safe_int(row.get('desktop_id'))
        for row in reconciled_remote_images
        if _safe_int(row.get('desktop_id'))
    }
    matched_cloud_measurement_ids: set[str] = set(
        str(row.get('cloud_id') or '').strip()
        for row in reconciled_local_measurements
        if str(row.get('cloud_id') or '').strip()
    )
    matched_local_measurement_ids_now = {
        _safe_int(row.get('desktop_id'))
        for row in reconciled_remote_measurements
        if _safe_int(row.get('desktop_id'))
    }
    new_asymmetry = _build_accepted_asymmetry_from_plan(
        items,
        local_images=reconciled_local_images,
        remote_images=reconciled_remote_images,
        local_measurements=reconciled_local_measurements,
        remote_measurements=reconciled_remote_measurements,
    )
    previous_snapshot = _parse_cloud_observation_snapshot(
        _load_cloud_observation_snapshot(resolved_cloud_id)
    )
    prev_asymmetry = previous_snapshot.get('accepted_asymmetry')
    merged_asymmetry = _reconcile_accepted_asymmetry(
        prev_asymmetry, new_asymmetry,
        plan_items=items,
        current_local_images=reconciled_local_images,
        current_remote_images=reconciled_remote_images,
        current_local_measurements=reconciled_local_measurements,
        current_remote_measurements=reconciled_remote_measurements,
        matched_local_image_ids=matched_local_image_ids_now,
        matched_cloud_image_ids=matched_cloud_image_ids,
        matched_local_measurement_ids=matched_local_measurement_ids_now,
        matched_cloud_measurement_ids=matched_cloud_measurement_ids,
    )

    # ── Step 9b: public spore mosaic for a reviewed measurement upload ────────
    # Restore the observation-wide mosaic step that normal push_all (19303)
    # and legacy resolve_conflict_keep_local (11825) already run after a
    # measurement push. ``executed`` also carries forward verified prior
    # push_measurement completions (status 'already_complete') from a partial
    # retry, so a retry that only had mosaic work outstanding still runs it —
    # checking just this call's ``ops`` would lose that upload on retry.
    pushed_measurement_this_plan = any(
        op.get('op') == 'push_measurement' and op.get('status') in {
            'completed', 'already_complete',
        }
        for op in executed
    )
    # Conservative mixed-asymmetry guard: the mosaic helper selects an
    # observation-wide set of cloud-linked eligible measurements
    # (_push_spore_mosaic_for_observation, ~23044-23078), so any retained
    # measurement OR image asymmetry — new, carried from a prior plan, or
    # inherited from a previous snapshot — means the local render-relevant
    # state cannot be proven equal to the cloud-approved set. This skips
    # mosaic work for every kind of retained asymmetry (including image-only
    # asymmetry unrelated to spore geometry, e.g. a kept-local field photo);
    # distinguishing microscope-relevant asymmetry from harmless asymmetry is
    # deferred to a later stage.
    mosaic_asymmetry_present = bool(
        merged_asymmetry.get('local_only_images')
        or merged_asymmetry.get('cloud_only_images')
        or merged_asymmetry.get('local_only_measurements')
        or merged_asymmetry.get('cloud_only_measurements')
    )
    # ``merged_asymmetry`` only records *accepted* keep-local/keep-cloud
    # entries. A genuine two-sided ("both changed") matched measurement or
    # image-render difference that this plan's automatic items left for
    # manual review never becomes an accepted-asymmetry entry, yet still
    # means the local render-relevant state is not proven to match the
    # cloud-approved set. Guard against that separately.
    if not mosaic_asymmetry_present:
        mosaic_asymmetry_present = _mosaic_render_state_unverified(
            local_images=reconciled_local_images,
            remote_images=reconciled_remote_images,
            local_measurements=reconciled_local_measurements,
            remote_measurements=reconciled_remote_measurements,
        )
    if pushed_measurement_this_plan and mosaic_asymmetry_present:
        executed.append({'op': 'push_spore_mosaic', 'status': 'skipped_asymmetry'})
    elif pushed_measurement_this_plan:
        try:
            mosaic_status = _push_spore_mosaic_for_observation(
                client, int(local_id), resolved_cloud_id,
            )
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise _partial_error(
                    f'Could not push spore mosaic: {exc}',
                    failing={'op': 'push_spore_mosaic'}, cause=exc,
                )
            print(
                f'[cloud_sync] Mosaic push errored while resolving conflict for '
                f'observation {int(local_id)}: {exc}',
                flush=True,
            )
            # Non-'completed'/'verification_pending' status: this is a
            # best-effort side effect, not part of the retry-verified op
            # vocabulary that _verify_completed_ops_and_rebase understands.
            executed.append({'op': 'push_spore_mosaic', 'status': 'best_effort_failed',
                             'error': str(exc)})
        else:
            executed.append({'op': 'push_spore_mosaic', 'status': 'best_effort_completed',
                             'mosaic_status': mosaic_status})

    # ── Step 10: finalize in order — snapshot → signature → stamp ─────────────
    try:
        _store_remote_snapshot(
            client, resolved_cloud_id,
            accepted_asymmetry=merged_asymmetry,
        )
    except Exception as exc:
        raise _partial_error(
            f'Could not store fresh cloud snapshot after applying plan: {exc}',
            failing={'op': 'store_snapshot'}, cause=exc,
        )
    _refresh_local_cloud_media_signature(int(local_id))
    _stamp_observation_synced(int(local_id), resolved_cloud_id)

    return {
        'local_id': int(local_id),
        'cloud_id': resolved_cloud_id,
        'plan_applied': True,
        'media_deleted': False,
        'operations': executed,
        'presentation_warnings': presentation_warnings,
        'accepted_asymmetry': merged_asymmetry,
    }


def _get_keyring_module():
    try:
        import keyring  # type: ignore

        return keyring
    except Exception:
        return None


def load_saved_cloud_password() -> tuple[str, str | None, bool]:
    settings = get_app_settings()
    email = str(settings.get('cloud_user_email') or '').strip()
    keyring = _get_keyring_module()
    if keyring is None:
        return email, None, False
    try:
        password = keyring.get_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
        if password is None and not using_isolated_profile():
            password = keyring.get_password(_CLOUD_LEGACY_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT)
    except Exception:
        return email, None, False
    return email, password, True


def save_cloud_password(email: str, password: str) -> None:
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure password storage is unavailable on this system.")
    try:
        keyring.set_password(_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT, password)
    except Exception as exc:
        raise RuntimeError(f"Could not securely save password: {exc}") from exc
    update_app_settings({'cloud_user_email': str(email or '').strip()})


def clear_saved_cloud_password() -> list[str]:
    keyring = _get_keyring_module()
    if keyring is None:
        return []
    from utils.keyring_cleanup import delete_password_entries

    return delete_password_entries(
        keyring,
        (
            (_CLOUD_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT),
            (_CLOUD_LEGACY_KEYRING_SERVICE, _CLOUD_KEYRING_ACCOUNT),
        ),
    )


def has_saved_cloud_password() -> bool:
    email, password, _ = load_saved_cloud_password()
    return bool(email and password)


class SporelyCloudClient(
    CloudSyncTransportMixin,
    CloudSyncTaxonIdentityMixin,
    CloudSyncPushIdentityMixin,
):
    """Thin wrapper around Supabase REST API."""

    def __init__(self, access_token: str, user_id: str, refresh_token: str | None = None):
        self.access_token = access_token
        self.user_id = user_id
        self.refresh_token = str(refresh_token or '').strip() or None
        self._s = requests.Session()
        self._r2: CloudflareR2Client | None = None
        self._media_worker: CloudflareMediaWorkerClient | None = None
        self._column_support_cache: dict[tuple[str, str], bool] = {}
        self._cloud_image_storage_key_cache: dict[str, str] = {}
        self._s.headers.update({
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        })

    def _get_r2(self) -> CloudflareR2Client:
        if self._r2 is None:
            self._r2 = CloudflareR2Client.from_env()
        return self._r2

    def _get_media_worker(self) -> CloudflareMediaWorkerClient:
        if self._media_worker is None:
            self._media_worker = CloudflareMediaWorkerClient.from_access_token(self.access_token)
        return self._media_worker

    def _using_default_r2_loader(self) -> bool:
        if "_get_r2" in self.__dict__:
            return False
        return type(self)._get_r2 is SporelyCloudClient._get_r2

    def _response_indicates_auth_error(self, response: requests.Response) -> bool:
        return _response_indicates_auth_error(response)

    def _adopt_session_from_values(
        self,
        access_token: str,
        user_id: str | None,
        refresh_token: str | None,
    ) -> None:
        """Copy a freshly-observed session onto this client in-memory.

        Kept small so both the "adopt from settings" and "adopt from
        refresh response" branches of :meth:`_refresh_session_if_possible`
        stay symmetric.
        """
        self.access_token = access_token
        self.user_id = (
            _decode_jwt_subject(access_token)
            or _normalize_cloud_user_id(user_id)
            or self.user_id
        )
        if refresh_token:
            self.refresh_token = refresh_token
        self._s.headers.update({'Authorization': f'Bearer {self.access_token}'})
        self._media_worker = None

    def _refresh_session_if_possible(self) -> bool:
        # Serialize every refresh attempt so concurrent clients cannot
        # race Supabase into rotating the same refresh token twice.
        with _CLOUD_REFRESH_LOCK:
            settings_access, settings_user_id, settings_refresh = (
                _read_current_cloud_session_settings()
            )
            self_access = str(self.access_token or '').strip() or None

            # Account-safety guard: if the on-disk session belongs to a
            # *different* Sporely Cloud user than this client is bound
            # to, refuse to adopt or refresh with any of it.  A stale
            # worker must not authenticate as another account just
            # because settings rotated under it.
            if not _settings_session_is_compatible(
                self.user_id, settings_user_id, settings_access
            ):
                raise CloudSessionAccountMismatchError(
                    "Stored cloud session belongs to a different account "
                    "than this client instance; refusing to adopt or refresh."
                )

            # Fast path: another thread already rotated tokens while we
            # were waiting for the lock.  Adopt the newer access token
            # and skip the network round-trip entirely.
            if (
                settings_access
                and settings_access != self_access
                and not _jwt_expires_soon(settings_access)
            ):
                self._adopt_session_from_values(
                    settings_access, settings_user_id, settings_refresh
                )
                return True

            # Refresh path: prefer whichever refresh token settings has
            # right now over our in-memory copy.  The account-safety
            # guard above has already confirmed settings belongs to the
            # same user, so preferring the settings refresh token is
            # safe.
            candidate_refresh = settings_refresh or (
                str(self.refresh_token or '').strip() or None
            )
            if not candidate_refresh:
                return False

            try:
                refreshed = type(self).refresh_login(candidate_refresh)
            except CloudTemporarilyUnavailableError:
                raise
            except CloudReauthRequiredError:
                # Before treating the session as dead, re-read settings
                # one more time — another thread may have just rotated
                # the refresh token so ``candidate_refresh`` looks dead
                # to Supabase (reuse detection) even though the session
                # is fine.
                after_access, after_user_id, after_refresh = (
                    _read_current_cloud_session_settings()
                )
                # Re-apply the account guard to the fresh snapshot.  If
                # the user switched accounts while we were mid-refresh
                # we must not adopt or retry with the new account's
                # tokens.  Propagate the original reauth-required
                # signal so the caller can prompt sign-in.
                if not _settings_session_is_compatible(
                    self.user_id, after_user_id, after_access
                ):
                    raise
                if (
                    after_access
                    and after_access != settings_access
                    and not _jwt_expires_soon(after_access)
                ):
                    self._adopt_session_from_values(
                        after_access, after_user_id, after_refresh
                    )
                    return True
                if after_refresh and after_refresh != candidate_refresh:
                    # A newer refresh token appeared on disk after we
                    # snapshotted.  Retry once with it before giving up.
                    try:
                        refreshed = type(self).refresh_login(after_refresh)
                    except CloudTemporarilyUnavailableError:
                        raise
                    except CloudReauthRequiredError:
                        # The newer token is also dead — this session
                        # really does need sign-in.  Do NOT clear tokens
                        # here; the UI decides how to prompt the user.
                        raise
                    except CloudSyncError:
                        return False
                else:
                    raise
            except CloudSyncError:
                return False

            self._adopt_session_from_values(
                refreshed.access_token, refreshed.user_id, refreshed.refresh_token
            )
            try:
                self.save_credentials()
            except Exception:
                pass
            return True

    def _request_with_refresh(self, method: str, url: str, *, refresh_on_auth_error: bool = True, **kwargs):
        return _request_with_transient_retry(
            self._s.request,
            method,
            url,
            refresh_on_auth_error=refresh_on_auth_error,
            refresh_callback=self._refresh_session_if_possible if refresh_on_auth_error else None,
            **kwargs,
        )

    def _has_column(self, table_name: str, column_name: str) -> bool:
        cache_key = (str(table_name or '').strip(), str(column_name or '').strip())
        if not all(cache_key):
            return False
        if cache_key in self._column_support_cache:
            return self._column_support_cache[cache_key]
        try:
            self._get(f'{cache_key[0]}?select={cache_key[1]}&limit=1')
            supported = True
        except CloudSyncError as exc:
            text = str(exc or '').lower()
            if (
                ('column' in text and cache_key[1].lower() in text and 'does not exist' in text)
                or 'could not find the' in text
            ):
                supported = False
            else:
                raise
        self._column_support_cache[cache_key] = supported
        return supported

    def _observation_select_columns(self) -> str:
        """The observation read columns, with identity provenance unless absent.

        The cloud's `taxon_identity_*` columns come from sporely-web migration
        20260922120000. Selecting a column the server lacks fails the whole
        read, so `_read_observation_rows` drops them after the first such
        failure; without them `selected_sporely_taxon_id` alone still
        describes a bound identity.
        """
        if getattr(self, '_observation_identity_columns_unsupported', False):
            return _OBSERVATION_SELECT_COLUMNS
        return _join_select_columns(
            _OBSERVATION_SELECT_COLUMNS, *_OBSERVATION_IDENTITY_SELECT_COLUMNS,
        )

    def _read_observation_rows(self, read, path_for):
        """``read(path_for(select))``, retried once without identity columns.

        No extra probe request: the normal read carries the columns, and only
        a server that rejects them costs one retry, once per client.
        """
        try:
            return read(path_for(self._observation_select_columns()))
        except CloudSyncError as exc:
            # The error text echoes the request path (and so the select list),
            # so match the server's own phrasing about one of THESE columns,
            # not any "does not exist" that happens to share the message.
            text = str(exc or '').lower()
            if (
                getattr(self, '_observation_identity_columns_unsupported', False)
                or not _MISSING_IDENTITY_COLUMN_PATTERN.search(text)
            ):
                raise
            self._observation_identity_columns_unsupported = True
            return read(path_for(_OBSERVATION_SELECT_COLUMNS))

    def _observation_supports_media_keys(self) -> bool:
        return self._has_column('observations', 'image_key') or self._has_column('observations', 'thumb_key')

    def _observation_images_support_ai_crop(self) -> bool:
        return self._has_column('observation_images', 'ai_crop_x1') or self._has_column('observation_images', 'ai_crop_source_w')

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return self._has_column('observation_images', 'ai_crop_is_custom')

    def _observation_images_support_metadata_purpose(self) -> bool:
        """Whether the server has the owner-sync metadata-parent capability.

        sporely-web migration 20260925120000 adds
        `observation_images.metadata_purpose` together with the public-RPC
        predicates that keep owner-sync parents out of every public surface,
        so the column's presence is the signal that owner-sync parents are
        safe to create. See `_owner_sync_parents_supported`.
        """
        cached = getattr(self, '_metadata_purpose_supported', None)
        if cached is not None:
            return cached
        try:
            self._get(
                f'observation_images?user_id=eq.{self.user_id}'
                f'&select=metadata_purpose&limit=1'
            )
            supported = True
        except CloudSyncError as exc:
            text = str(exc or '').lower()
            if not (
                re.search(r"column \S*metadata_purpose does not exist", text)
                or re.search(r"could not find the '?metadata_purpose'? column", text)
            ):
                raise
            supported = False
        self._metadata_purpose_supported = supported
        return supported

    def fetch_image_metadata_purpose(self, cloud_image_id: str) -> str | None:
        """The stored `metadata_purpose` of one owned image row, or None."""
        rows = self._get(
            f'observation_images?id=eq.{cloud_image_id}&user_id=eq.{self.user_id}'
            f'&select=id,metadata_purpose'
        )
        return str((rows or [{}])[0].get('metadata_purpose') or '') or None if rows else None

    def _observation_images_support_upload_metadata(self) -> bool:
        return self._has_column('observation_images', 'upload_mode') or self._has_column('observation_images', 'stored_bytes')

    def _observation_images_support_storage_exif_safe(self) -> bool:
        return self._has_column('observation_images', 'storage_exif_safe')

    def _observation_images_support_original_storage_path(self) -> bool:
        return self._has_column('observation_images', 'original_storage_path')

    def _observation_images_support_sample_source(self) -> bool:
        """`sample_source` is added by the sporely-web Stage 2A migration.

        Older cloud deployments only have `sample_type`. When missing here,
        push drops `sample_source` from the payload so PostgREST doesn't
        return a schema-cache error, and legacy `sample_type='Spore_print'`
        stays on the payload so the value doesn't get silently dropped
        during the transition.
        """
        return self._has_column('observation_images', 'sample_source')

    def _measurement_supports_media_keys(self) -> bool:
        return self._has_column('spore_measurements', 'image_key') or self._has_column('spore_measurements', 'thumb_key')

    def _set_observation_media_keys(self, obs_cloud_id: str, storage_key: str, sort_order) -> None:
        if not obs_cloud_id:
            return
        if sort_order not in (None, 0, '0'):
            return
        if not self._observation_supports_media_keys():
            return
        normalized_key = _normalize_cloud_media_key(storage_key)
        if not normalized_key:
            return
        payload: dict[str, str] = {}
        if self._has_column('observations', 'image_key'):
            payload['image_key'] = normalized_key
        if self._has_column('observations', 'thumb_key'):
            payload['thumb_key'] = media_variant_key(normalized_key, 'thumb')
        if payload:
            self._patch(f'observations?id=eq.{obs_cloud_id}', payload)

    def _cloud_image_storage_key(self, cloud_image_id: str) -> str:
        normalized_id = str(cloud_image_id or '').strip()
        if not normalized_id:
            return ''
        if normalized_id in self._cloud_image_storage_key_cache:
            return self._cloud_image_storage_key_cache[normalized_id]
        rows = self._get(f'observation_images?id=eq.{normalized_id}&select=storage_path&limit=1')
        storage_key = _normalize_cloud_media_key((rows[0] or {}).get('storage_path') if rows else '')
        self._cloud_image_storage_key_cache[normalized_id] = storage_key
        return storage_key

    # ── Auth ────────────────────────────────────────────────────────────

    @classmethod
    def login(cls, email: str, password: str) -> 'SporelyCloudClient':
        resp = _request_with_transient_retry(
            requests.request,
            'POST',
            f'{SUPABASE_URL}/auth/v1/token?grant_type=password',
            json={'email': email, 'password': password},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=_SUPABASE_AUTH_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'Login failed (status={resp.status_code}): {resp.text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token'),
        )

    @classmethod
    def refresh_login(cls, refresh_token: str) -> 'SporelyCloudClient':
        token = str(refresh_token or '').strip()
        if not token:
            raise CloudSyncError('Missing refresh token')
        resp = _request_with_transient_retry(
            requests.request,
            'POST',
            f'{SUPABASE_URL}/auth/v1/token?grant_type=refresh_token',
            json={'refresh_token': token},
            headers={'apikey': SUPABASE_KEY, 'Content-Type': 'application/json'},
            timeout=_SUPABASE_AUTH_TIMEOUT,
        )
        if not resp.ok:
            body_text = str(getattr(resp, 'text', '') or '')
            body_lower = body_text.lower()
            status_code = int(getattr(resp, 'status_code', 0) or 0)
            # Supabase returns 400 with an ``invalid_grant`` (or similar)
            # body only when it can prove the refresh token itself is dead:
            # revoked, rotated, expired, or reused past the detection
            # window.  Any *other* non-ok status is treated as a generic
            # CloudSyncError so a rotation race or transient blip does not
            # look terminal to callers.
            if status_code == 400 and any(
                hint in body_lower for hint in _CLOUD_REAUTH_REQUIRED_HINTS
            ):
                raise CloudReauthRequiredError(
                    f'Refresh failed (status={status_code}): {body_text}'
                )
            raise CloudSyncError(f'Refresh failed (status={status_code}): {body_text}')
        d = resp.json()
        return cls(
            access_token=d['access_token'],
            user_id=d['user']['id'],
            refresh_token=d.get('refresh_token') or token,
        )

    @classmethod
    def from_stored_credentials(cls) -> 'SporelyCloudClient | None':
        settings = get_app_settings()
        # Delegate to OAuthSporelyCloudClient when the stored session uses OAuth.
        if cls is SporelyCloudClient and str(settings.get('cloud_auth_method') or '').strip() == 'oauth':
            return OAuthSporelyCloudClient.from_stored_credentials()
        token = settings.get('cloud_access_token')
        user_id = settings.get('cloud_user_id')
        refresh_token = settings.get('cloud_refresh_token')
        token_text = str(token or '').strip()
        user_id_text = _normalize_cloud_user_id(user_id)
        refresh_text = str(refresh_token or '').strip() or None
        if token_text and user_id_text:
            token_user_id = _decode_jwt_subject(token_text)
            client = cls(
                access_token=token_text,
                user_id=token_user_id or user_id_text,
                refresh_token=refresh_text,
            )
            expiry_seconds = _decode_jwt_expiry(token_text)
            # Only refresh proactively when we can *prove* the token is
            # near expiry AND we have a refresh token to spend.  If the
            # JWT is undecodable, keep the historical fast path and rely
            # on a first-request 401 to trigger the locked refresh.
            if (
                expiry_seconds is not None
                and _jwt_expires_soon(token_text)
                and refresh_text
            ):
                try:
                    client._refresh_session_if_possible()
                    return client
                except CloudTemporarilyUnavailableError:
                    # Transient — return the client anyway.  A first API
                    # call will retry the refresh through the same lock.
                    return client
                except CloudReauthRequiredError:
                    # Session is genuinely dead — return None so the caller
                    # surfaces reauth_required.  Password fallback removed.
                    return None
            else:
                return client
        if refresh_text:
            try:
                client = cls.refresh_login(str(refresh_text))
                client.save_credentials()
                return client
            except CloudTemporarilyUnavailableError:
                raise
            except CloudSyncError:
                pass
        return None

    def fetch_current_user_id(self) -> str:
        """Return the authenticated Supabase user id for the current session."""
        user_info = self.fetch_current_user_info()
        user_id = _normalize_cloud_user_id(user_info.get('id') if isinstance(user_info, dict) else None)
        if user_id:
            if user_id != self.user_id:
                self.user_id = user_id
            return user_id
        raise CloudSyncError('Could not fetch current cloud user.')

    def fetch_current_user_info(self) -> dict:
        """Return the authenticated Supabase user record."""
        resp = self._request_with_refresh('GET', f'{SUPABASE_URL}/auth/v1/user', timeout=_SUPABASE_AUTH_TIMEOUT)
        if resp.ok:
            try:
                data = resp.json()
            except Exception:
                data = {}
            user_id = _normalize_cloud_user_id(data.get('id') if isinstance(data, dict) else None)
            if user_id:
                if user_id != self.user_id:
                    self.user_id = user_id
                return data if isinstance(data, dict) else {'id': user_id}
        if not self._response_indicates_auth_error(resp):
            token_user_id = _decode_jwt_subject(self.access_token)
            if token_user_id:
                if token_user_id != self.user_id:
                    self.user_id = token_user_id
                return {'id': token_user_id}
        raise CloudSyncError(f'Could not fetch current cloud user: {resp.text if resp is not None else ""}')

    def fetch_profile(self) -> dict:
        rows = self._get(
            f'profiles?id=eq.{self.user_id}&select=id,username,display_name,bio,avatar_url&limit=1'
        )
        return dict(rows[0] or {}) if rows else {}

    def fetch_cloud_plan_profile(self) -> dict:
        rows = self._get(
            f'profiles?id=eq.{self.user_id}&select='
            'id,cloud_plan,is_pro,full_res_storage_enabled,storage_quota_bytes,'
            'total_storage_bytes,storage_used_bytes,image_count,is_banned&limit=1'
        )
        return normalize_cloud_plan_profile(rows[0] if rows else {})

    def update_profile(
        self,
        *,
        username: str | None = None,
        display_name: str | None = None,
        bio: str | None = None,
        avatar_url: str | None = None,
    ) -> None:
        payload: dict[str, object] = {}
        if username is not None:
            normalized = str(username or '').strip().lstrip('@')
            payload['username'] = normalized or None
        if display_name is not None:
            normalized = str(display_name or '').strip()
            payload['display_name'] = normalized or None
        if bio is not None:
            normalized = str(bio or '').strip()
            payload['bio'] = normalized or None
        if avatar_url is not None:
            normalized = str(avatar_url or '').strip()
            payload['avatar_url'] = normalized or None
        if not payload:
            return
        self._patch(f'profiles?id=eq.{self.user_id}', payload)

    def upload_profile_avatar(self, jpeg_bytes: bytes) -> str:
        content = bytes(jpeg_bytes or b'')
        if not content:
            raise CloudSyncError('Missing avatar image data.')
        path = f'{self.user_id}/avatar.jpg'
        url = f'{SUPABASE_URL}/storage/v1/object/avatars/{path}'
        headers = {
            'Content-Type': 'image/jpeg',
            'x-upsert': 'true',
        }
        resp = self._request_with_refresh('POST', url, data=content, headers=headers, timeout=_SUPABASE_PROFILE_UPLOAD_TIMEOUT)
        if not resp.ok:
            resp = self._request_with_refresh('PUT', url, data=content, headers=headers, timeout=_SUPABASE_PROFILE_UPLOAD_TIMEOUT)
        if not resp.ok:
            raise CloudSyncError(f'Avatar upload failed: {resp.text}')
        public_url = f'{SUPABASE_URL}/storage/v1/object/public/avatars/{path}'
        self.update_profile(avatar_url=public_url)
        return public_url

    def save_credentials(
        self,
        email: str | None = None,
        password: str | None = None,
        remember_password: bool | None = None,
    ) -> None:
        updates = {
            'cloud_access_token': self.access_token,
            'cloud_user_id': self.user_id,
            'cloud_refresh_token': self.refresh_token,
        }
        _clear_session_scoped_caches()
        if email is not None:
            updates['cloud_user_email'] = str(email or '').strip()
        update_app_settings(updates)
        # Only change the saved password when the caller explicitly asked us to.
        # Auto-login/refresh paths should preserve whatever the user chose earlier.
        if remember_password is True and email and password:
            save_cloud_password(str(email or '').strip(), password)
        elif remember_password is False:
            clear_saved_cloud_password()

    @staticmethod
    def clear_session() -> None:
        """Forget only the cloud tokens so the saved password can survive re-login."""
        _clear_session_scoped_caches()
        update_app_settings({
            'cloud_access_token': None,
            'cloud_user_id': None,
            'cloud_refresh_token': None,
            'cloud_auth_method': None,
        })

    @staticmethod
    def clear_credentials() -> None:
        _clear_session_scoped_caches()
        clear_saved_cloud_password()
        update_app_settings({
            'cloud_access_token': None,
            'cloud_user_id': None,
            'cloud_refresh_token': None,
            'cloud_user_email': None,
            'cloud_auth_method': None,
        })

    # ── REST helpers ─────────────────────────────────────────────────────

    def _get(self, path: str) -> list:
        resp = self._request_with_refresh('GET', f'{SUPABASE_URL}/rest/v1/{path}', timeout=_SUPABASE_REST_TIMEOUT)
        if not resp.ok:
            raise CloudSyncError(f'GET {path}: {resp.text}')
        return resp.json()


    def get_read_only(self, path: str) -> list:
        """Perform one REST GET without token refresh or credential writes."""
        resp = self._request_with_refresh(
            'GET',
            f'{SUPABASE_URL}/rest/v1/{path}',
            timeout=_SUPABASE_REST_TIMEOUT,
            refresh_on_auth_error=False,
        )
        if not resp.ok:
            status = int(getattr(resp, 'status_code', 0) or 0)
            if status in {401, 403} or self._response_indicates_auth_error(resp):
                raise CloudReauthRequiredError(
                    'Read-only cloud audit authentication expired; sign in again before retrying.'
                )
            raise CloudSyncError(f'GET {path}: {resp.text}')
        return resp.json()

    def _post(self, path: str, payload: dict) -> list:
        resp = self._request_with_refresh(
            'POST',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=representation'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'POST {path}: {resp.text}')
        return resp.json()

    def _rpc(self, function_name: str, payload: dict | None = None):
        rpc_name = str(function_name or '').strip()
        if not rpc_name:
            raise CloudSyncError('Missing RPC function name')
        resp = self._request_with_refresh(
            'POST',
            f'{SUPABASE_URL}/rest/v1/rpc/{rpc_name}',
            json=dict(payload or {}),
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'RPC {rpc_name}: {resp.text}')
        if not resp.content:
            return None
        return resp.json()

    def sync_reference_work(self, payload: dict, expected_row_version: int):
        return self._rpc('sync_reference_work', {
            'p_payload': payload,
            'p_expected_row_version': expected_row_version,
        })

    def sync_reference_taxon_treatment(self, payload: dict, expected_row_version: int):
        return self._rpc('sync_reference_taxon_treatment', {
            'p_payload': payload,
            'p_expected_row_version': expected_row_version,
        })

    def sync_reference_measurement_set(self, payload: dict, expected_row_version: int):
        return self._rpc('sync_reference_measurement_set', {
            'p_payload': payload,
            'p_expected_row_version': expected_row_version,
            'p_client_capabilities': _reference_client_capabilities(),
        })

    def sync_observation_reference_use(
        self, payload: dict, expected_row_version: int, snapshot_mode: str = 'current'
    ):
        return self._rpc('sync_observation_reference_use', {
            'p_payload': payload,
            'p_expected_row_version': expected_row_version,
            'p_snapshot_mode': snapshot_mode,
            'p_client_capabilities': _reference_client_capabilities(),
        })

    def record_reference_client_capabilities(self, capabilities: dict) -> object:
        """Report this device's reference capability (Stage M)."""
        return self._rpc('record_reference_client_capabilities', {
            'p_client_capabilities': capabilities,
        })

    def list_reference_client_devices(self) -> list[dict]:
        """The owner's device capability records (owner SELECT only)."""
        return self._get_paginated(
            f'reference_client_devices?user_id=eq.{self.user_id}'
            '&select=device_id,reference_snapshot_versions,last_seen_at'
            '&order=device_id.asc'
        )

    def _list_reference_library_feed(
        self,
        entity: str,
        fields: str,
        *,
        page_size: int = _REFERENCE_FEED_PAGE_SIZE,
        max_rows: int | None = None,
        max_response_bytes: int | None = None,
    ) -> list[dict]:
        """One complete owner pull through ``list_reference_library_feed``.

        Stage M (sporely-web 20261002120000): capable clients read the owner
        feed through this RPC instead of the v1-only table GETs. Every pull
        starts without a cursor; ``next_cursor`` is used only to page within
        this pull and is never persisted or seeded from a stored cursor. Rows
        are projected to ``fields`` so reconciliation sees exactly what the
        former table select returned. Any page failure raises: a partial feed
        is never returned.
        """
        capabilities = _reference_client_capabilities()
        wanted = tuple(field.strip() for field in fields.split(',') if field.strip())
        rows: list[dict] = []
        seen_ids: set[str] = set()
        response_bytes = 0
        cursor: tuple[str, str] | None = None
        while True:
            response = self._rpc('list_reference_library_feed', {
                'p_entity': entity,
                'p_client_capabilities': capabilities,
                'p_after_updated_at': cursor[0] if cursor else None,
                'p_after_id': cursor[1] if cursor else None,
                'p_limit': page_size,
            })
            if not isinstance(response, dict):
                raise CloudSyncError(f'reference feed {entity}: malformed response')
            status = response.get('status')
            if status == 'rate_limited':
                raise CloudTemporarilyUnavailableError(
                    f'reference feed {entity}: rate limited'
                )
            if status != 'ok' or response.get('entity') != entity:
                raise CloudSyncError(f'reference feed {entity}: unexpected status {status!r}')
            page = response.get('rows')
            if not isinstance(page, list) or len(page) > page_size:
                raise CloudSyncError(f'reference feed {entity}: malformed rows')
            if cursor is None:
                withheld = response.get('withheld_count')
                self.__dict__.setdefault('reference_feed_withheld_counts', {})[entity] = (
                    withheld if isinstance(withheld, int) and not isinstance(withheld, bool) else 0
                )
            response_bytes += len(json.dumps(
                page, ensure_ascii=False, separators=(',', ':'),
            ).encode('utf-8'))
            if max_response_bytes is not None and response_bytes > max_response_bytes:
                raise CloudSyncError(
                    f'reference feed {entity}: response exceeds {max_response_bytes} bytes'
                )
            for row in page:
                if not isinstance(row, dict):
                    raise CloudSyncError(f'reference feed {entity}: row is not an object')
                missing = [field for field in wanted if field not in row]
                if missing:
                    raise CloudSyncError(
                        f'reference feed {entity}: row lacks {", ".join(missing)}'
                    )
                row_id = str(row.get('id') or '')
                if row_id in seen_ids:
                    raise CloudSyncError(f'reference feed {entity}: duplicate row {row_id}')
                seen_ids.add(row_id)
                rows.append({field: row[field] for field in wanted})
            if max_rows is not None and len(rows) > max_rows:
                raise CloudSyncError(f'reference feed {entity}: response exceeds {max_rows} rows')
            next_cursor = response.get('next_cursor')
            if next_cursor is None:
                return rows
            if (
                not isinstance(next_cursor, dict)
                or not str(next_cursor.get('updated_at') or '').strip()
                or not str(next_cursor.get('id') or '').strip()
                or not page
            ):
                raise CloudSyncError(f'reference feed {entity}: malformed cursor')
            advanced = (str(next_cursor['updated_at']), str(next_cursor['id']))
            if advanced == cursor:
                raise CloudSyncError(f'reference feed {entity}: cursor did not advance')
            cursor = advanced

    def list_reference_works(self) -> list[dict]:
        fields = (
            'user_id,id,type,citation_key,authors_json,editors_json,title,'
            'container_title,year,edition,publisher,place,volume,issue,pages,doi,'
            'isbn,url,language,short_label,citation_override,revision,row_version,'
            'created_at,updated_at,deleted_at'
        )
        return self._get_paginated(
            f'reference_works?user_id=eq.{self.user_id}&select={fields}'
            '&order=updated_at.asc,id.asc'
        )

    def list_reference_taxon_treatments(self) -> list[dict]:
        fields = (
            'user_id,id,reference_work_id,taxon_id,name_as_published,page_from,'
            'page_to,locator_text,treatment_notes,revision,row_version,created_at,'
            'updated_at,deleted_at'
        )
        return self._get_paginated(
            f'reference_taxon_treatments?user_id=eq.{self.user_id}&select={fields}'
            '&order=updated_at.asc,id.asc'
        )

    def list_reference_measurement_sets(self) -> list[dict]:
        fields = (
            'user_id,id,taxon_treatment_id,character,raw_text,data_kind,length_min,'
            'length_core_min,length_core_max,length_max,width_min,width_core_min,'
            'width_core_max,width_max,q_min,q_max,q_mean,length_mean,width_mean,'
            'sample_size,specimen_count,mount_medium,stain,preparation,'
            'measurement_method,notes,raw_points_json,supersedes_id,revision,'
            'measurement_details_json,q_core_min,q_core_max,'
            'row_version,created_at,updated_at,deleted_at'
        )
        return self._list_reference_library_feed('measurement_set', fields)

    def list_observation_reference_uses(self) -> list[dict]:
        fields = (
            'user_id,id,observation_id,reference_measurement_set_id,role,note,'
            'selected_at,reference_revision,snapshot_json,row_version,created_at,'
            'updated_at,deleted_at'
        )
        return self._list_reference_library_feed('observation_use', fields)

    def search_public_curated_reference_sets(
        self,
        sporely_taxon_id: int,
        limit: int = 20,
        after_published_at: str | None = None,
        after_id: str | None = None,
    ) -> list[dict]:
        rows = self._rpc('search_public_curated_reference_sets', {
            'p_sporely_taxon_id': sporely_taxon_id,
            'p_limit': limit,
            'p_after_published_at': after_published_at,
            'p_after_id': after_id,
        })
        return rows if isinstance(rows, list) else []

    def search_public_reference_contributions_v2(
        self,
        sporely_taxon_id: int,
        limit: int = 25,
        after_shared_at: str | None = None,
        after_id: str | None = None,
    ) -> list[dict]:
        rows = self._rpc('search_public_reference_contributions_v2', {
            'p_sporely_taxon_id': sporely_taxon_id,
            'p_limit': limit,
            'p_after_shared_at': after_shared_at,
            'p_after_id': after_id,
            'p_accept_snapshot_versions': _reference_accept_snapshot_versions(),
        })
        return rows if isinstance(rows, list) else []

    def get_public_reference_contribution_v2(
        self, contribution_id: str, revision: int | None = None,
    ) -> list[dict]:
        # _v2 (sporely-web 20261001091940): served shared rows carry
        # relationship_roles; today's unversioned read serves tombstones only.
        rows = self._rpc('get_public_reference_contribution_v2', {
            'p_contribution_id': contribution_id,
            'p_revision': revision,
            'p_accept_snapshot_versions': _reference_accept_snapshot_versions(),
        })
        return rows if isinstance(rows, list) else []

    def share_reference_contribution(
        self, source_measurement_set_id: str, sporely_taxon_id: int,
        expected_work_revision: int, expected_treatment_revision: int,
        expected_measurement_set_revision: int,
    ) -> object:
        return self._rpc('share_reference_contribution', {
            'p_source_measurement_set_id': source_measurement_set_id,
            'p_sporely_taxon_id': sporely_taxon_id,
            'p_expected_work_revision': expected_work_revision,
            'p_expected_treatment_revision': expected_treatment_revision,
            'p_expected_measurement_set_revision': expected_measurement_set_revision,
        })

    def list_my_reference_sharing(self) -> object:
        """The owner's reference sets with their sharing status.

        ``{status: 'ok', sets: [...]}`` or a rate-limited result
        (sporely-web 20261001113007_share_references_by_default).
        """
        return self._rpc('list_my_reference_sharing', {})

    def stop_sharing_reference_set(self, source_measurement_set_id: str) -> object:
        """Owner stop: the set is no longer shown publicly anywhere."""
        return self._rpc('stop_sharing_reference_set', {
            'p_source_measurement_set_id': source_measurement_set_id,
        })

    def share_reference_set_again(self, source_measurement_set_id: str) -> object:
        """Owner undo of a stop. Never lifts a moderation hide."""
        return self._rpc('share_reference_set_again', {
            'p_source_measurement_set_id': source_measurement_set_id,
        })

    def withdraw_reference_contribution(self, contribution_id: str) -> object:
        return self._rpc('withdraw_reference_contribution', {
            'p_contribution_id': contribution_id,
        })

    def get_public_curated_reference_set(
        self, curated_measurement_set_id: str, bundle_revision: int,
    ) -> list[dict]:
        rows = self._rpc('get_public_curated_reference_set', {
            'p_curated_measurement_set_id': curated_measurement_set_id,
            'p_bundle_revision': bundle_revision,
        })
        return rows if isinstance(rows, list) else []

    def submit_private_reference_for_curation(
        self,
        source_measurement_set_id: str,
        expected_work_revision: int,
        expected_treatment_revision: int,
        expected_measurement_set_revision: int,
        attestation_version: str,
        rights_confirmed: bool,
        curation_consent_confirmed: bool,
    ) -> object:
        return self._rpc('submit_private_reference_for_curation', {
            'p_source_measurement_set_id': source_measurement_set_id,
            'p_expected_work_revision': expected_work_revision,
            'p_expected_treatment_revision': expected_treatment_revision,
            'p_expected_measurement_set_revision': expected_measurement_set_revision,
            'p_attestation_version': attestation_version,
            'p_rights_confirmed': rights_confirmed,
            'p_curation_consent_confirmed': curation_consent_confirmed,
        })

    def sync_reference_curated_fork(self, payload: dict, expected_row_version: int):
        return self._rpc('sync_reference_curated_fork', {
            'p_payload': payload,
            'p_expected_row_version': expected_row_version,
        })

    def list_reference_curated_forks(self) -> list[dict]:
        fields = (
            'id,user_id,curated_measurement_set_id,bundle_revision,sporely_taxon_id,'
            'reference_work_id,taxon_treatment_id,reference_measurement_set_id,'
            'source_sha256,source_envelope_json,row_version,created_at,updated_at'
        )
        return self._list_reference_library_feed(
            'curated_fork',
            fields,
            page_size=10,
            max_rows=10_000,
            max_response_bytes=64 * 1024 * 1024,
        )

    def _patch(self, path: str, payload: dict) -> None:
        resp = self._request_with_refresh(
            'PATCH',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=minimal'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')

    def _patch_with_precondition(self, path: str, payload: dict) -> list[dict]:
        """Like ``_patch``, but for a PATCH whose ``path`` already carries an
        equality-filter precondition (e.g. ``...&visibility=eq.<expected>``)
        on a column the caller's decision was based on.

        Requests ``Prefer: return=representation`` and returns the rows
        PostgREST reports as updated. PostgREST reports a precondition that
        matched zero rows as an ordinary 200 response with an empty array —
        never an error — so a caller basing a write on a value it fetched
        earlier in the sync cycle (Stage C review round 3: a bulk
        ``remote_lookup`` built once before the per-observation push loop)
        MUST inspect the returned rows to tell a lost race (the column
        changed concurrently since that earlier fetch) apart from a normal
        successful update; treating an empty result as success would
        silently overwrite or ignore a concurrent write to the same column.
        """
        resp = self._request_with_refresh(
            'PATCH',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json=payload,
            headers={'Prefer': 'return=representation'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')
        try:
            rows = resp.json()
        except Exception:
            rows = []
        return rows if isinstance(rows, list) else []

    def _delete(self, path: str) -> None:
        resp = self._request_with_refresh(
            'DELETE',
            f'{SUPABASE_URL}/rest/v1/{path}',
            headers={'Prefer': 'return=minimal'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'DELETE {path}: {resp.text}')

    def _storage_remove(self, storage_paths: list[str]) -> None:
        cleaned = []
        for path in (storage_paths or []):
            path_str = _normalize_cloud_media_key(path)
            if not path_str:
                continue
            cleaned.append(path_str)

        if not cleaned:
            return
        try:
            # The authenticated Worker owns dual-bucket targeting and logical
            # quota accounting. Direct S3 deletion is legacy-bucket-only and
            # must not be used for lifecycle cleanup, even in an explicitly
            # enabled local admin runtime.
            self._get_media_worker().delete_objects(cleaned)
            _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
        except Exception as exc:
            raise CloudSyncError(f'Media delete failed: {exc}') from exc

    # ── Observation push ─────────────────────────────────────────────────

    def _find_cloud_observation(self, desktop_id: int) -> str | None:
        """Reverse-link (``desktop_id``) recovery lookup for one observation.

        Returns the single same-owner cloud observation id carrying this
        ``desktop_id``, or ``None``. Multiple matches mean cloud-side
        duplicates; silently picking one could route a push at the wrong
        row, so that case raises instead of choosing.
        """
        rows = self._get(
            f'observations?desktop_id=eq.{desktop_id}&user_id=eq.{self.user_id}'
            '&select=id&order=id.asc'
        )
        if len(rows) > 1:
            raise ObservationIdentityConflictError(
                f'observation desktop_id={desktop_id}: {len(rows)} cloud observations '
                f'carry this desktop_id '
                f'({", ".join(str(row.get("id")) for row in rows)}); refusing to '
                f'choose among duplicates'
            )
        return rows[0]['id'] if rows else None

    def get_observation(self, cloud_id: str) -> dict | None:
        cloud_value = str(cloud_id or '').strip()
        if not cloud_value:
            return None
        rows = self._read_observation_rows(
            self._get,
            lambda select: f'observations?id=eq.{cloud_value}&user_id=eq.{self.user_id}&select={select}',
        )
        return rows[0] if rows else None

    def list_remote_observations(self) -> list[dict]:
        return self._read_observation_rows(
            self._get_paginated,
            lambda select: (
                f'observations?user_id=eq.{self.user_id}'
                f'&order=created_at.asc,id.asc&select={select}'
            ),
        )

    def count_remote_privacy_slots(self) -> int:
        resp = self._request_with_refresh(
            'GET',
            (
                f'{SUPABASE_URL}/rest/v1/observations?user_id=eq.{self.user_id}'
                # Server trigger 20260626110000: non-draft, and not public or
                # fuzzed/region/hidden (NULL visibility counts as public).
                '&and=(or(is_draft.is.null,is_draft.eq.false),'
                'or(visibility.neq.public,location_precision.in.(fuzzed,region,hidden)))'
                '&select=id&limit=1'
            ),
            headers={'Prefer': 'count=exact'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'GET observations count failed: {resp.text}')
        total = _parse_postgrest_content_range_total(getattr(resp, 'headers', {}).get('Content-Range'))
        if total is None:
            raise CloudSyncError('Could not determine privacy slot count from cloud response.')
        return total

    def find_remote_calibration(self, calibration_uuid: str) -> dict | None:
        calibration_id = _normalize_calibration_uuid(calibration_uuid)
        if not calibration_id:
            return None
        rows = self._get(
            f'calibrations?user_id=eq.{self.user_id}&calibration_uuid=eq.{calibration_id}&select={_CALIBRATION_SELECT_COLUMNS}'
        )
        return rows[0] if rows else None

    def list_remote_calibrations(self) -> list[dict]:
        return self._get_paginated(
            f'calibrations?user_id=eq.{self.user_id}'
            f'&order=created_at.asc,id.asc&select={_CALIBRATION_SELECT_COLUMNS}'
        )

    def push_calibration_reference_image(
        self,
        calibration: dict,
        *,
        cloud_row_id: str | None = None,
        remote_row: dict | None = None,
    ) -> str | None:
        """Upload a derivative calibration reference image and patch the cloud row.

        Returns a warning string when the image is missing or could not be uploaded.
        """
        record = dict(calibration or {})
        calibration_uuid = _normalize_calibration_uuid(record.get('calibration_uuid'))
        label = _calibration_display_name(record)
        ref_image_start = _cloud_sync_perf_counter()

        def _log_slow_reference_image(*, had_storage_path: bool, upload_attempted: bool, outcome: str) -> None:
            elapsed = _cloud_sync_perf_counter() - ref_image_start
            if elapsed < _CLOUD_SYNC_SLOW_STEP_SECONDS:
                return
            print(
                f"[cloud_sync] calibration reference image: slow "
                f"calibration {calibration_uuid or '?'} ({label}) "
                f"had_storage_path={had_storage_path} "
                f"upload_attempted={upload_attempted} outcome={outcome} "
                f"took {elapsed * 1000:.0f}ms",
                flush=True,
            )

        remote = dict(remote_row or {})
        if not remote and calibration_uuid:
            remote = dict(self.find_remote_calibration(calibration_uuid) or {})

        existing_storage_path = _normalize_cloud_media_key(remote.get('image_storage_path'))
        if existing_storage_path:
            # Fast no-op once the cloud row already references an image.
            return None

        target_cloud_row_id = str(cloud_row_id or remote.get('id') or '').strip()
        if not target_cloud_row_id:
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because the cloud row id is unavailable'
            )

        local_path = _select_representative_calibration_image_path(record)
        if local_path is None:
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because no readable local calibration image was found'
            )

        try:
            image_bytes, content_type, extension = _calibration_reference_image_bytes(local_path)
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=False, outcome='prepare_failed')
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because the image could not be prepared ({exc})'
            )

        storage_key = _calibration_reference_storage_key(
            self.user_id,
            calibration_uuid or str(remote.get('calibration_uuid') or '').strip() or target_cloud_row_id,
            extension,
        )
        cache_control = 'public, max-age=31536000, immutable'
        try:
            if direct_r2_runtime_available():
                self._get_r2().put_bytes(
                    image_bytes,
                    storage_key,
                    content_type=content_type,
                    cache_control=cache_control,
                    timeout=120,
                )
            else:
                cloud_plan = 'free'
                try:
                    cloud_plan = str(self.fetch_cloud_plan_profile().get('cloud_plan') or cloud_plan).strip() or cloud_plan
                except Exception:
                    pass
                self._get_media_worker().put_bytes(
                    image_bytes,
                    storage_key,
                    content_type=content_type,
                    cache_control=cache_control,
                    upload_meta={
                        'upload_mode': 'full',
                        'quality_profile': 'standard',
                        'encoding_quality': '',
                        'encoding_format': content_type,
                        'source_width': '',
                        'source_height': '',
                        'stored_width': '',
                        'stored_height': '',
                        'stored_bytes': str(len(image_bytes)),
                    },
                    options={
                        'uploadMode': 'full',
                        'uploadVariant': 'full',
                        'cloudPlan': cloud_plan,
                        'qualityProfile': 'standard',
                        'encodingFormat': content_type,
                    },
                    timeout=120,
                )
                _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='r2_upload_failed')
            return (
                f'calibration {calibration_uuid or "?"}: skipped reference image upload for {label} '
                f'because R2 upload failed ({exc})'
            )

        try:
            self._patch(
                f'calibrations?user_id=eq.{self.user_id}&id=eq.{target_cloud_row_id}',
                {'image_storage_path': _normalize_cloud_media_key(storage_key)},
            )
        except Exception as exc:
            _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='patch_failed')
            return (
                f'calibration {calibration_uuid or "?"}: uploaded reference image for {label} '
                f'but could not update the cloud row ({exc})'
            )

        _increment_sync_summary(_cloud_sync_current_summary(), 'calibration_reference_images_uploaded')
        _log_slow_reference_image(had_storage_path=False, upload_attempted=True, outcome='uploaded')
        return None

    def push_calibration_metadata(self, calibration: dict) -> str:
        """Upsert calibration metadata row. Returns cloud row id."""
        payload = _calibration_sync_payload(calibration)
        calibration_uuid = str(payload.get('calibration_uuid') or '').strip()
        if not calibration_uuid:
            raise CloudSyncError('Missing calibration UUID')
        if not payload.get('objective_key'):
            raise CloudSyncError('Missing objective key')
        if payload.get('calibration_date') is None:
            raise CloudSyncError('Missing calibration date')
        if payload.get('microns_per_pixel') is None:
            raise CloudSyncError('Missing microns per pixel')

        payload['user_id'] = self.user_id
        payload['calibration_uuid'] = calibration_uuid

        existing_row = self.find_remote_calibration(calibration_uuid)
        if existing_row:
            if _calibration_payloads_match(payload, existing_row):
                return str(existing_row.get('id') or '').strip() or calibration_uuid
            raise CloudSyncError(
                'Skipped cloud update because the same UUID has different metadata'
            )

        try:
            rows = self._post('calibrations', payload)
        except CloudSyncError:
            existing_row = self.find_remote_calibration(calibration_uuid)
            if existing_row and _calibration_payloads_match(payload, existing_row):
                return str(existing_row.get('id') or '').strip() or calibration_uuid
            raise

        return str(rows[0]['id'])

    def push_observation(
        self,
        obs: dict,
        remote_obs: dict | None = None,
        *,
        sync_summary: dict[str, int] | None = None,
        baseline_obs: dict | None = None,
    ) -> str:
        """Upsert observation to cloud. Returns cloud UUID.

        ``baseline_obs`` is the stored sync baseline (the same shape
        ``_baseline_observation_compare_payload`` produces), when the caller
        has one. It is used only by the explicit-clear check in
        ``_sync_observation_selected_taxon`` — an unproven local identity
        never re-asserts the old identity, but it may explicitly clear a
        stale one when the baseline proves the desktop deliberately changed
        the identification away from it. ``None`` means "no baseline
        available", which keeps the prior skip-only behaviour.
        """
        summary = sync_summary or _cloud_sync_current_summary()
        # Never widen location precision beyond the cloud/baseline value
        # without an explicit, confirmed local choice (full-row pushes too).
        guard_refs = [remote_obs, baseline_obs]
        if remote_obs is None and baseline_obs is None:
            guard_refs.append(_snapshot_baseline_for_cloud_id(obs.get('cloud_id')))
        obs = _guard_local_location_precision(obs, *guard_refs)
        payload = _observation_push_payload(obs, local=True)
        payload['user_id'] = self.user_id
        if bool(obs.get('portable_cloud_identity_pending')):
            payload.pop('desktop_id', None)
        else:
            payload['desktop_id'] = obs['id']

        existing_id = self._resolve_existing_observation_for_push(obs, remote_obs=remote_obs)
        if existing_id:
            if remote_obs is not None and str(remote_obs.get('id') or '').strip() == str(existing_id):
                diff_fields = _observation_push_diff_fields(dict(obs or {}), remote_obs)
                if not diff_fields:
                    # The cloud already holds the (guarded) local value.
                    consume_confirmed_location_precision(obs.get('id'))
                    _increment_sync_summary(summary, 'observations_skipped_noop')
                    self._sync_observation_selected_taxon(
                        existing_id,
                        obs,
                        remote_obs=remote_obs,
                        baseline_obs=baseline_obs,
                    )
                    return existing_id
            # Coord-change / preserve-only geography rules — see §4 in the spec.
            _shape_geography_patch_payload(payload, obs, existing_id)
            self._patch(f'observations?id=eq.{existing_id}', payload)
            consume_confirmed_location_precision(obs.get('id'))
            _increment_sync_summary(summary, 'observations_patched')
            self._sync_observation_selected_taxon(
                existing_id,
                obs,
                remote_obs=remote_obs,
                baseline_obs=baseline_obs,
            )
            return existing_id
        # New observation: never invent a region_id.
        payload.pop('region_id', None)
        rows = self._post('observations', payload)
        consume_confirmed_location_precision(obs.get('id'))
        _increment_sync_summary(summary, 'observations_patched')
        cloud_id = rows[0]['id']
        self._sync_observation_selected_taxon(cloud_id, obs, remote_obs=None)
        return cloud_id


    def clear_observation_selected_taxon(
        self,
        cloud_id: str,
        *,
        genus: str | None,
        species: str | None,
        common_name: str | None,
    ) -> None:
        """Atomically clear a stale cloud identity and commit the new name.

        Uses the same atomic RPC the web uses for a coupled identity+name
        change (``set_observation_identification_v2``,
        sporely-web migration 20260922140000): identity and its dependent
        server state (shared-reference contributions) change together, never
        a bare PATCH of taxonomy columns from the desktop.
        """
        self._rpc('set_observation_identification_v2', {
            'p_observation_id': int(cloud_id),
            'p_sporely_taxon_id': None,
            'p_identity_state': None,
            'p_source_system': None,
            'p_namespace': None,
            'p_external_id': None,
            'p_raw_external_id': None,
            'p_write_name': True,
            'p_genus': genus,
            'p_species': species,
            'p_common_name': common_name,
        })
        self._verify_identity_clear_landed(cloud_id)


    def set_observation_selected_taxon(
        self,
        cloud_id: str,
        sporely_taxon_id: int,
    ) -> None:
        """Persist an owner-selected exact taxonomy identity on the cloud row."""
        self._rpc('set_observation_selected_taxon_v2', {
            'p_observation_id': int(cloud_id),
            'p_sporely_taxon_id': int(sporely_taxon_id),
        })

    # ── Image push ───────────────────────────────────────────────────────

    def _build_storage_path(self, obs_cloud_id: str, img_cloud_id: str, local_path: str) -> str:
        # Fallback key for uploads that name an existing cloud row. Never embed
        # the local filename (camera names such as IMG_20260915_134721 carry
        # capture time). The suffix is a hash of stable non-time identities so
        # a retry for the same row rewrites the same object.
        extension = _stable_storage_key_extension(local_path)
        digest = _stable_storage_key_digest(self.user_id, obs_cloud_id, img_cloud_id)
        return f'{self.user_id}/{obs_cloud_id}/{img_cloud_id}_{digest}{extension}'

    def _build_original_storage_path(self, obs_cloud_id: str, img_cloud_id: str, local_path: str) -> str:
        source_path = Path(str(local_path or '').strip())
        safe_name = _sanitize_original_storage_filename(source_path)
        normalized_image_id = str(img_cloud_id or '').strip()
        if not normalized_image_id:
            digest_source = f'{self.user_id}/{str(obs_cloud_id or "").strip()}/{safe_name}'
            normalized_image_id = hashlib.sha1(digest_source.encode('utf-8')).hexdigest()[:16]
        return _normalize_cloud_media_key(
            f'{self.user_id}/{str(obs_cloud_id or "").strip()}/originals/{normalized_image_id}/{safe_name}'
        )

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str, *, remote_row: dict | None = None) -> str:
        """Upsert image metadata row. Returns cloud UUID."""
        payload = {col: img.get(col) for col in _IMG_PUSH_COLS}
        captured_at = _normalize_image_captured_at_for_cloud(
            img.get('captured_at'), local=True
        )
        if captured_at is None:
            # A local NULL must not erase an authoritative cloud capture
            # instant. New inserts still naturally receive cloud NULL.
            payload.pop('captured_at', None)
        else:
            payload['captured_at'] = captured_at
        calibration_uuid = _image_calibration_uuid(img)
        if calibration_uuid:
            payload['calibration_uuid'] = calibration_uuid
        else:
            payload.pop('calibration_uuid', None)
        payload['observation_id']    = obs_cloud_id
        payload['user_id']           = self.user_id
        if bool(img.get('portable_cloud_identity_pending')):
            payload.pop('desktop_id', None)
        else:
            payload['desktop_id'] = img['id']
        # A local image reaching the push path is active (local tombstones are
        # filtered by the caller). Clear a stale remote soft-delete so its
        # measurements become visible again through public RPCs.
        payload['deleted_at']        = None
        payload['original_filename'] = (
            str(img.get('original_filename') or '').strip()
            or Path(img.get('filepath') or '').name
            or None
        )
        # storage_path handling.
        #
        # `_normalize_cloud_media_key('')` returns `''`, not `None`. The
        # cloud RLS WITH CHECK is:
        #   (storage_path LIKE '<uid>/%') OR (storage_path IS NULL AND image_type = 'microscope')
        #
        # Two failure modes to avoid:
        #   1. Sending storage_path='' — neither NULL nor uid-prefixed, so
        #      the policy rejects every PATCH with 42501.
        #   2. Sending storage_path=NULL for a non-microscope row —
        #      violates the NULL-only-if-microscope leg, also 42501.
        #
        # When the caller has no key (metadata-only PATCH), omit the field
        # entirely so PostgREST leaves the existing cloud value untouched:
        # NULL for a metadata-only microscope anchor, or the real R2 key
        # for an uploaded row. Only include storage_path when the caller
        # explicitly needs to write one.
        normalized_storage_path = _normalize_cloud_media_key(storage_path)
        if normalized_storage_path:
            payload['storage_path'] = normalized_storage_path
        else:
            payload.pop('storage_path', None)
        if payload.get('gps_source') is not None:
            payload['gps_source'] = bool(payload['gps_source'])
        # Sample condition + source normalization for the push payload.
        # Handles the legacy row shape where `sample_type='Spore_print'`
        # from a pre-split desktop still exists locally, and applies the
        # null-safe merge against the current cloud row so an unrelated
        # metadata push does not wipe `sample_source` when the local column
        # is empty.
        _apply_image_sample_fields_to_push_payload(
            payload,
            img,
            client=self,
            obs_cloud_id=obs_cloud_id,
        )
        if not self._observation_images_support_ai_crop():
            for key in (
                'ai_crop_x1', 'ai_crop_y1', 'ai_crop_x2', 'ai_crop_y2',
                'ai_crop_source_w', 'ai_crop_source_h',
            ):
                payload.pop(key, None)
        if not self._observation_images_support_ai_crop_custom():
            payload.pop('ai_crop_is_custom', None)
        if self._observation_images_support_upload_metadata():
            for key in _IMG_UPLOAD_META_COLS:
                payload[key] = img.get(key)
        if self._observation_images_support_storage_exif_safe():
            payload['storage_exif_safe'] = True

        existing_id = self._resolve_existing_image_for_push(img, obs_cloud_id, remote_row=remote_row)
        restore_source_id = _explicit_image_restore_source(img['id'])
        if existing_id:
            self._patch(f'observation_images?id=eq.{existing_id}&user_id=eq.{self.user_id}', payload)
            cloud_id = existing_id
        else:
            try:
                rows = self._post('observation_images', payload)
                cloud_id = rows[0]['id']
            except CloudSyncError as e:
                if '23505' in str(e) or 'unique' in str(e).lower():
                    raise ImageIdentityConflictError(
                        f"POST to observation_images hit unique constraint for image {img['id']}; "
                        "a concurrent race or stale global lookup; image left dirty"
                    ) from e
                raise
        if cloud_id and restore_source_id:
            _clear_explicit_image_restore_source(img['id'])
        normalized_key = _normalize_cloud_media_key(payload.get('storage_path'))
        if cloud_id and normalized_key:
            self._cloud_image_storage_key_cache[str(cloud_id)] = normalized_key
            self._set_observation_media_keys(obs_cloud_id, normalized_key, img.get('sort_order'))
        return cloud_id

    def set_image_original_storage_path(self, cloud_image_id: str, original_storage_path: str) -> None:
        normalized_id = str(cloud_image_id or '').strip()
        normalized_key = _normalize_cloud_media_key(original_storage_path)
        if not normalized_id or not normalized_key:
            return
        if not self._observation_images_support_original_storage_path():
            return
        self._patch(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}',
            {'original_storage_path': normalized_key},
        )

    def set_image_storage_path(
        self,
        cloud_image_id: str,
        storage_path: str,
        *,
        upload_meta: dict | None = None,
    ) -> None:
        """Attach a confirmed derivative object key to one owned image row.

        This is the post-upload confirmation write: the bytes behind
        ``storage_path`` must already exist. For binding a key BEFORE bytes
        are sent (the metadata-anchor → byte-backed promotion), use
        :meth:`reserve_image_storage_path_for_promotion` /
        :meth:`release_image_storage_path_reservation` instead — those are
        conditional and rollback-safe; this method is not.
        """
        normalized_id = str(cloud_image_id or '').strip()
        normalized_key = _normalize_cloud_media_key(storage_path)
        if not normalized_id or not normalized_key:
            raise CloudSyncError('Missing cloud image id or storage path')
        payload = {'storage_path': normalized_key}
        if self._observation_images_support_upload_metadata():
            for key in _IMG_UPLOAD_META_COLS:
                if key in (upload_meta or {}):
                    payload[key] = (upload_meta or {}).get(key)
        self._patch(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}',
            payload,
        )

    def reserve_image_storage_path_for_promotion(
        self,
        cloud_image_id: str,
        storage_path: str,
    ) -> bool:
        """Pre-upload key reservation for the anchor → byte-backed promotion.

        Binds the intended Worker key to one EXISTING owned
        ``observation_images`` row so the Worker's storage_path check can
        pass for the byte upload that follows. Owner-scoped and conditional
        on ``storage_path IS NULL``: a row that is already byte-backed (or
        was reserved by a concurrent writer) is never overwritten. Returns
        True when exactly this bare-anchor row was reserved, False when no
        row matched the conditions.

        This is NOT the post-upload confirmation — bytes do not exist yet
        when this runs. See :meth:`set_image_storage_path` for attaching a
        confirmed key, and :meth:`release_image_storage_path_reservation`
        for the failure rollback.
        """
        normalized_id = str(cloud_image_id or '').strip()
        normalized_key = _normalize_cloud_media_key(storage_path)
        if not normalized_id or not normalized_key:
            raise CloudSyncError('Missing cloud image id or storage path for reservation')
        path = (
            f'observation_images?id=eq.{normalized_id}'
            f'&user_id=eq.{self.user_id}'
            f'&storage_path=is.null'
        )
        resp = self._request_with_refresh(
            'PATCH',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json={'storage_path': normalized_key},
            headers={'Prefer': 'return=representation'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')
        rows = resp.json() or []
        return bool(rows)

    def release_image_storage_path_reservation(
        self,
        cloud_image_id: str,
        reserved_key: str,
    ) -> bool:
        """Rollback of :meth:`reserve_image_storage_path_for_promotion`.

        Restores ``storage_path`` to NULL only when the row still carries
        the exact key reserved by this attempt, so a concurrently confirmed
        upload (or another writer's key) is never wiped back to a bare
        anchor. Never deletes the row. Returns True when the reservation
        was released, False when the row no longer carried the key.
        """
        normalized_id = str(cloud_image_id or '').strip()
        normalized_key = _normalize_cloud_media_key(reserved_key)
        if not normalized_id or not normalized_key:
            raise CloudSyncError('Missing cloud image id or reserved key for release')
        path = (
            f'observation_images?id=eq.{normalized_id}'
            f'&user_id=eq.{self.user_id}'
            f'&storage_path=eq.{_encode_postgrest_filter_value(normalized_key)}'
        )
        resp = self._request_with_refresh(
            'PATCH',
            f'{SUPABASE_URL}/rest/v1/{path}',
            json={'storage_path': None},
            headers={'Prefer': 'return=representation'},
            timeout=_SUPABASE_REST_TIMEOUT,
        )
        if not resp.ok:
            raise CloudSyncError(f'PATCH {path}: {resp.text}')
        rows = resp.json() or []
        return bool(rows)

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
        result_meta: dict | None = None,
        *,
        observation_id: int | None = None,
        image_id: int | None = None,
        recovery_authorized: bool = False,
    ) -> str | None:
        """Upload file to Cloudflare R2. Returns the relative media key or None if missing."""
        path = Path(local_path)
        if not path.exists():
            return None

        # Stage 1: byte-storage gate at the client boundary. Fails closed on
        # missing identity — callers on the normal path MUST pass both
        # ``observation_id`` and ``image_id`` so the gate can consult
        # ``cloud_image_bytes_desired``. A caller that omits either identity
        # kwarg is treated as a silent-bypass attempt and refused with the
        # same error the desired-state check would raise. Recovery flows
        # opt in explicitly via ``recovery_authorized=True`` and are the
        # sole intentional exception; when authorized, missing ids are
        # tolerated for auditability of the recovery adapter (which does
        # pass ids in practice).
        gate_obs = _safe_int(observation_id)
        gate_image = _safe_int(image_id)
        if not recovery_authorized:
            if gate_obs <= 0 or gate_image <= 0:
                raise CloudImageBytesNotDesiredError(
                    "cloud image bytes require observation_id and image_id "
                    "on the normal upload path; recovery_authorized=True is "
                    f"the only intentional exception (obs={observation_id!r} "
                    f"image={image_id!r})"
                )
            if not cloud_image_bytes_desired(gate_obs, gate_image):
                raise CloudImageBytesNotDesiredError(
                    f"cloud image bytes not desired: obs={gate_obs} image={gate_image}"
                )

        storage_path = _normalize_cloud_media_key(
            storage_path or self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)
        )
        cache_control = 'public, max-age=31536000, immutable'
        meta = dict(upload_meta or {})
        print(
            '[cloud_sync] Uploading cloud image request '
            f'obs={_safe_int(meta.get("observation_id")) or obs_cloud_id or "?"} '
            f'image={_safe_int(meta.get("image_id")) or img_cloud_id or "?"} '
            f'storage_path={storage_path}'
        )

        with tempfile.TemporaryDirectory(prefix='sporely_cloud_upload_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            worker_base_url = media_worker_base_url()
            try:
                prepared_path, source_width, source_height, stored_width, stored_height, encoding_format, encoding_quality = _prepare_cloud_image_upload_file(
                    str(path),
                    temp_dir,
                    _safe_int(img_cloud_id, default=0) or 0,
                    meta,
                )
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='full',
                            storage_key=storage_path,
                            content_type='image/webp',
                            prepared_path_suffix='.webp',
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                raise
            mime = _content_type_for_path(prepared_path)
            prepared_path_suffix = prepared_path.suffix.lower() or '.webp'
            upload_policy = _cloud_upload_policy_from_meta(meta)
            quality_profile = str(upload_policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
            upload_mode = str(upload_policy.get('uploadMode') or 'full').strip().lower() or 'full'
            cloud_plan = str(upload_policy.get('cloudPlan') or ('pro' if quality_profile == 'high' else 'free'))
            stored_bytes = prepared_path.stat().st_size
            common_metadata = {
                'user_id': self.user_id,
                'uploaded_at': datetime.now(timezone.utc).isoformat(),
                'uploaded_by': self.user_id,
                'upload_mode': upload_mode,
                'upload_variant': 'full',
                'cloud_plan': cloud_plan,
                'quality_profile': quality_profile,
                'encoding_quality': '' if encoding_quality is None else str(encoding_quality),
                'encoding_format': encoding_format,
                'source_width': str(source_width),
                'source_height': str(source_height),
                'stored_width': str(stored_width),
                'stored_height': str(stored_height),
                'stored_bytes': str(stored_bytes),
            }
            if result_meta is not None:
                result_meta.update({
                    'upload_mode': upload_mode,
                    'source_width': source_width,
                    'source_height': source_height,
                    'stored_width': stored_width,
                    'stored_height': stored_height,
                    'stored_bytes': stored_bytes,
                })

            try:
                worker = self._get_media_worker()
                worker_base_url = str(getattr(worker, 'base_url', worker_base_url) or worker_base_url).strip().rstrip('/')
                upload_response = worker.put_file(
                    prepared_path,
                    storage_path,
                    content_type=mime,
                    cache_control=cache_control,
                    timeout=120,
                    upload_meta=common_metadata,
                    options={
                        'imageId': img_cloud_id,
                        'uploadMode': upload_mode,
                        'uploadVariant': 'full',
                        'cloudPlan': cloud_plan,
                        'qualityProfile': quality_profile,
                        'encodingQuality': encoding_quality,
                        'encodingFormat': encoding_format,
                        'sourceWidth': source_width,
                        'sourceHeight': source_height,
                        'storedWidth': stored_width,
                        'storedHeight': stored_height,
                    },
                )
                _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                confirmed_key = _normalize_cloud_media_key(str((upload_response or {}).get('key') or storage_path))
                if not confirmed_key:
                    raise CloudSyncError('Worker upload did not return a storage key')
                storage_path = confirmed_key
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='full',
                            storage_key=storage_path,
                            content_type=mime,
                            prepared_path_suffix=prepared_path_suffix,
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise CloudSyncError(f'Media upload failed: {exc}') from exc

            # Generate the single cloud thumbnail variant used by web and desktop.
            try:
                with Image.open(prepared_path) as img:
                    img = ImageOps.exif_transpose(img)
                    if img.mode in ('RGBA', 'LA'):
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'RGBA':
                            background.paste(img, mask=img.split()[3])
                        else:
                            background.paste(img, mask=img.split()[1])
                        img = background
                    elif img.mode != 'RGB':
                        img = img.convert('RGB')

                    orig_w, orig_h = img.size
                    scale = min(1.0, _CLOUD_THUMB_MAX_EDGE / max(orig_w, orig_h))
                    target_w = max(1, int(orig_w * scale))
                    target_h = max(1, int(orig_h * scale))

                    variant_path = media_variant_key(storage_path, 'thumb')
                    thumb_worker_base_url = media_worker_base_url()
                    thumb_prepared_suffix = Path(variant_path).suffix.lower() or '.webp'
                    thumb_mime = mime
                    img_resized = img.resize((target_w, target_h), Image.Resampling.LANCZOS)
                    buffer = io.BytesIO()
                    thumb_format, thumb_mime, thumb_options = _cloud_thumb_save_format(prepared_path)
                    img_resized.save(buffer, format=thumb_format, **thumb_options)
                    thumb_quality = thumb_options.get('quality')
                    thumb_metadata = {
                        'user_id': self.user_id,
                        'uploaded_at': common_metadata['uploaded_at'],
                        'uploaded_by': self.user_id,
                        'upload_mode': upload_mode,
                        'upload_variant': 'thumb',
                        'cloud_plan': cloud_plan,
                        'quality_profile': quality_profile,
                        'encoding_quality': '' if thumb_quality is None else str(thumb_quality),
                        'encoding_format': thumb_mime,
                        'source_width': str(source_width),
                        'source_height': str(source_height),
                        'stored_width': str(target_w),
                        'stored_height': str(target_h),
                        'stored_bytes': str(len(buffer.getvalue())),
                    }
                    thumb_worker_base_url = media_worker_base_url()
                    thumb_prepared_suffix = Path(variant_path).suffix.lower() or '.webp'
                    worker = self._get_media_worker()
                    thumb_worker_base_url = str(getattr(worker, 'base_url', media_worker_base_url()) or media_worker_base_url()).strip().rstrip('/')
                    thumb_response = worker.put_bytes(
                        buffer.getvalue(),
                        variant_path,
                        content_type=thumb_mime,
                        cache_control=cache_control,
                        timeout=60,
                        upload_meta=thumb_metadata,
                        options={
                            'imageId': img_cloud_id,
                            'uploadMode': upload_mode,
                            'uploadVariant': 'thumb',
                            'cloudPlan': cloud_plan,
                            'qualityProfile': quality_profile,
                            'encodingQuality': thumb_quality,
                            'encodingFormat': thumb_mime,
                            'sourceWidth': source_width,
                            'sourceHeight': source_height,
                            'storedWidth': target_w,
                            'storedHeight': target_h,
                        },
                    )
                    _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                    confirmed_thumb_key = _normalize_cloud_media_key(str((thumb_response or {}).get('key') or variant_path))
                    if confirmed_thumb_key != _normalize_cloud_media_key(variant_path):
                        raise CloudSyncError('Worker thumbnail upload returned an unexpected storage key')
            except Exception as e:
                if is_image_too_large_for_plan_error(e):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            e,
                            upload_variant='thumb',
                            storage_key=variant_path,
                            content_type=thumb_mime,
                            prepared_path_suffix=thumb_prepared_suffix,
                            worker_base_url=thumb_worker_base_url,
                        )
                    ) from e
                raise CloudSyncError(f'Media thumbnail upload failed: {e}') from e

        _increment_sync_summary(_cloud_sync_current_summary(), 'images_uploaded')
        return storage_path

    def upload_original_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
        *,
        observation_id: int | None = None,
        image_id: int | None = None,
        recovery_authorized: bool = False,
    ) -> str | None:
        """Upload a full-resolution original file to cloud storage as WebP."""
        path = Path(local_path)
        if not path.exists():
            return None

        # Stage 1: byte-storage gate at the client boundary. Fails closed on
        # missing identity — identical rules to ``upload_image_file``.
        # Identity in ``upload_meta`` alone is diagnostic labelling and
        # does not satisfy the identity requirement. Recovery flows opt in
        # via ``recovery_authorized=True``.
        gate_obs = _safe_int(observation_id)
        gate_image = _safe_int(image_id)
        if not recovery_authorized:
            if gate_obs <= 0 or gate_image <= 0:
                raise CloudImageBytesNotDesiredError(
                    "cloud original bytes require observation_id and image_id "
                    "on the normal upload path; recovery_authorized=True is "
                    f"the only intentional exception (obs={observation_id!r} "
                    f"image={image_id!r})"
                )
            if not cloud_image_bytes_desired(gate_obs, gate_image):
                raise CloudImageBytesNotDesiredError(
                    f"cloud original bytes not desired: obs={gate_obs} image={gate_image}"
                )

        storage_path = _normalize_cloud_media_key(
            storage_path or self._build_original_storage_path(obs_cloud_id, img_cloud_id, local_path)
        )
        if not storage_path:
            return None

        cache_control = 'public, max-age=31536000, immutable'
        meta = dict(upload_meta or {})
        print(
            '[cloud_sync] Uploading cloud original image request '
            f'obs={_safe_int(meta.get("observation_id")) or obs_cloud_id or "?"} '
            f'image={_safe_int(meta.get("image_id")) or img_cloud_id or "?"} '
            f'storage_path={storage_path}'
        )
        with tempfile.TemporaryDirectory(prefix='sporely_cloud_upload_') as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            worker_base_url = media_worker_base_url()
            try:
                prepared_path, source_width, source_height, stored_width, stored_height, encoding_format, encoding_quality = _prepare_cloud_image_upload_file(
                    str(path),
                    temp_dir,
                    _safe_int(img_cloud_id, default=0) or 0,
                    meta,
                )
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='original',
                            storage_key=storage_path,
                            content_type='image/webp',
                            prepared_path_suffix='.webp',
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise

            content_type = _content_type_for_path(prepared_path)
            prepared_path_suffix = prepared_path.suffix.lower() or '.webp'
            upload_policy = _cloud_upload_policy_from_meta(meta)
            quality_profile = str(upload_policy.get('qualityProfile') or 'standard').strip().lower() or 'standard'
            cloud_plan = str(upload_policy.get('cloudPlan') or ('pro' if quality_profile == 'high' else 'free'))
            stored_bytes = prepared_path.stat().st_size
            common_metadata = {
                'user_id': self.user_id,
                'uploaded_at': datetime.now(timezone.utc).isoformat(),
                'uploaded_by': self.user_id,
                'upload_mode': 'full',
                'upload_variant': 'original',
                'cloud_plan': cloud_plan,
                'quality_profile': quality_profile,
                'encoding_quality': '' if encoding_quality is None else str(encoding_quality),
                'encoding_format': encoding_format,
                'source_width': str(source_width),
                'source_height': str(source_height),
                'stored_width': str(stored_width),
                'stored_height': str(stored_height),
                'stored_bytes': str(stored_bytes),
                'source_role': str(meta.get('source_role') or '').strip(),
                'source_kind': str(meta.get('source_kind') or '').strip(),
            }

            try:
                worker = self._get_media_worker()
                worker_base_url = str(getattr(worker, 'base_url', worker_base_url) or worker_base_url).strip().rstrip('/')
                upload_response = worker.put_file(
                    prepared_path,
                    storage_path,
                    content_type=content_type,
                    cache_control=cache_control,
                    timeout=120,
                    upload_meta=common_metadata,
                    options={
                        'imageId': img_cloud_id,
                        'uploadMode': 'full',
                        'uploadVariant': 'original',
                        'cloudPlan': cloud_plan,
                        'qualityProfile': quality_profile,
                        'encodingQuality': encoding_quality,
                        'encodingFormat': encoding_format,
                        'sourceWidth': source_width,
                        'sourceHeight': source_height,
                        'storedWidth': stored_width,
                        'storedHeight': stored_height,
                    },
                )
                _increment_sync_summary(_cloud_sync_current_summary(), 'storage_quota_delta_rpc_calls')
                confirmed_key = _normalize_cloud_media_key(str((upload_response or {}).get('key') or storage_path))
                if not confirmed_key:
                    raise CloudSyncError('Worker upload did not return a storage key')
                storage_path = confirmed_key
            except Exception as exc:
                if is_image_too_large_for_plan_error(exc):
                    raise CloudSyncError(
                        _format_cloud_image_too_large_error(
                            path,
                            meta,
                            exc,
                            upload_variant='original',
                            storage_key=storage_path,
                            content_type=content_type,
                            prepared_path_suffix=prepared_path_suffix,
                            worker_base_url=worker_base_url,
                        )
                    ) from exc
                if is_webp_support_required_for_cloud_media_upload_error(exc):
                    raise CloudSyncError(WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE) from exc
                raise CloudSyncError(f'Original media upload failed: {exc}') from exc

        return storage_path

    def _download_public_media_file(self, storage_path: str, dest_path: str | Path, *, timeout: int = 120) -> Path:
        storage_key = _normalize_cloud_media_key(storage_path)
        if not storage_key:
            raise CloudSyncError('Missing storage path')
        public_url = self._get_media_worker().public_url(storage_key)
        if not public_url:
            raise CloudSyncError('Missing public media URL')
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        session = requests.Session()
        response = session.get(public_url, stream=True, timeout=timeout)
        try:
            if not response.ok:
                raise CloudSyncError(f'Public media download failed ({response.status_code})')
            content_type = str(response.headers.get('content-type') or '').strip().lower()
            if content_type and not content_type.startswith('image/') and content_type != 'application/octet-stream':
                raise CloudSyncError(f'Public media download returned non-image content ({content_type})')
            with destination.open('wb') as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
            return destination
        finally:
            try:
                response.close()
            except Exception:
                pass

    # ── Pull new web observations ─────────────────────────────────────────

    def pull_web_observations(self, after_iso: str | None = None) -> list[dict]:
        """Fetch observations created on mobile/web (desktop_id IS NULL)."""
        def _path(select: str) -> str:
            qs = (
                f'observations?desktop_id=is.null&user_id=eq.{self.user_id}'
                f'&order=created_at.asc,id.asc&select={select}'
            )
            if after_iso:
                qs += f'&created_at=gt.{_encode_postgrest_filter_value(after_iso)}'
            return qs

        return self._read_observation_rows(self._get_paginated, _path)

    def set_desktop_id(self, cloud_id: str, desktop_id: int) -> None:
        """Write the local SQLite ID back to the cloud row for future dedup."""
        self._patch(f'observations?id=eq.{cloud_id}', {'desktop_id': desktop_id})

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        cloud_value = str(obs_cloud_id or '').strip()
        if not cloud_value:
            return []

        path = f'observation_images?observation_id=eq.{cloud_value}&user_id=eq.{self.user_id}'
        if not include_deleted_for_sync:
            path += '&deleted_at=is.null'
        path += f'&select={_OBSERVATION_IMAGE_SELECT_COLUMNS}'

        rows = self._get(path)
        image_rows = [dict(row or {}) for row in (rows or [])]
        if include_deleted_for_sync:
            return image_rows
        return [
            row
            for row in image_rows
            if not str(row.get('deleted_at') or '').strip()
        ]

    def pull_observation_identifications(self, obs_cloud_id: str) -> list[dict]:
        cloud_value = str(obs_cloud_id or '').strip()
        if not cloud_value:
            return []
        try:
            return [
                dict(row or {})
                for row in self._get(
                    f'observation_identifications?observation_id=eq.{cloud_value}&user_id=eq.{self.user_id}&order=created_at.desc,id.desc&select={_OBSERVATION_IDENTIFICATION_SELECT_COLUMNS}'
                )
            ]
        except Exception as exc:
            message = str(exc or '').lower()
            if 'observation_identifications' in message and (
                'could not find the table' in message
                or 'does not exist' in message
            ):
                return []
            if 'schema cache' in message or 'pgrst002' in message or 'pgrst003' in message:
                raise CloudTemporarilyUnavailableError(_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE) from exc
            raise

    def set_measurement_desktop_id(self, cloud_measurement_id: str, desktop_id: int) -> None:
        """Write the local SQLite measurement ID back to the cloud row for future dedup."""
        self._patch(
            f'spore_measurements?id=eq.{cloud_measurement_id}&user_id=eq.{self.user_id}',
            {'desktop_id': desktop_id},
        )

    def pull_measurements_for_images(self, image_cloud_ids: list[str]) -> list[dict]:
        profiler = _cloud_sync_current_profiler()
        image_ids = [str(image_id or '').strip() for image_id in (image_cloud_ids or []) if str(image_id or '').strip()]
        all_rows: list[dict] = []
        success = False
        batch_size = _CLOUD_SYNC_IN_BATCH_SIZE
        request_count = 0
        fetch_start = _cloud_sync_perf_counter()
        try:
            if not image_ids:
                success = True
                return []
            batch_total = (len(image_ids) + batch_size - 1) // batch_size
            print(
                f'[cloud_sync] measurement fetch: start image_ids={len(image_ids)} '
                f'batch_size={batch_size} batches={batch_total}',
                flush=True,
            )
            for batch_index, i in enumerate(range(0, len(image_ids), batch_size)):
                chunk = image_ids[i:i + batch_size]
                ids_str = ','.join(chunk)
                batch_start = _cloud_sync_perf_counter()
                rows = self._get_paginated(
                    f'spore_measurements?image_id=in.({ids_str})'
                    f'&user_id=eq.{self.user_id}'
                    f'&order=measured_at.asc,id.asc'
                    f'&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
                )
                request_count += 1
                all_rows.extend(rows)
                batch_elapsed = _cloud_sync_perf_counter() - batch_start
                if batch_elapsed >= _CLOUD_SYNC_SLOW_STEP_SECONDS:
                    print(
                        f'[cloud_sync] measurement fetch: slow batch '
                        f'{batch_index + 1}/{batch_total} ids={len(chunk)} '
                        f'rows={len(rows)} duration={batch_elapsed * 1000:.0f}ms',
                        flush=True,
                    )
            success = True
            return all_rows
        finally:
            print(
                f'[cloud_sync] measurement fetch: complete requests={request_count} '
                f'rows={len(all_rows)} '
                f'duration={(_cloud_sync_perf_counter() - fetch_start) * 1000:.0f}ms',
                flush=True,
            )
            if profiler is not None:
                try:
                    profiler.record_pull_measurements_for_images(len(all_rows) if success else 0)
                except Exception:
                    pass

    def pull_bulk_image_metadata(self, obs_cloud_ids: list[str]) -> list[dict]:
        profiler = _cloud_sync_current_profiler()
        all_images: list[dict] = []
        success = False
        try:
            if not obs_cloud_ids:
                success = True
                return []
            for i in range(0, len(obs_cloud_ids), _CLOUD_SYNC_IN_BATCH_SIZE):
                chunk = obs_cloud_ids[i:i + _CLOUD_SYNC_IN_BATCH_SIZE]
                ids_str = ','.join(chunk)
                # Deterministic id.asc order + real pagination — without paging
                # the response is silently truncated at db-max-rows=1000, which
                # drops entire observations at the tail of a large batch and
                # leaves them with an empty images list.
                rows = self._get_paginated(
                    f'observation_images?observation_id=in.({ids_str})'
                    f'&user_id=eq.{self.user_id}'
                    f'&order=id.asc'
                    f'&select={_OBSERVATION_IMAGE_SELECT_COLUMNS}'
                )
                all_images.extend(rows)
            success = True
            return all_images
        finally:
            if profiler is not None:
                try:
                    profiler.record_pull_bulk_image_metadata(len(all_images) if success else 0)
                except Exception:
                    pass

    def list_image_changes_since(self, cursor_ts: str, cursor_id: str) -> list[dict]:
        """Cheap probe: observation_images rows updated since cursor (updated_at-based).

        Uses the server-maintained updated_at column (added via migration:
        every INSERT, metadata UPDATE, soft-delete, and restore advances it;
        clients cannot spoof it). Ordered by (updated_at, id) to allow exact
        tie-breaking with no timestamp loss.
        """
        if not cursor_ts:
            return []
        rows = self._get_paginated(
            f'observation_images?user_id=eq.{self.user_id}'
            f'&updated_at=gte.{_encode_postgrest_filter_value(cursor_ts)}'
            f'&select=id,observation_id,updated_at'
            f'&order=updated_at.asc,id.asc'
        )
        result = []
        for row in rows:
            ts = str(row.get('updated_at') or '')
            rid = str(row.get('id', ''))
            # Strict client-side filter: keep rows strictly after the cursor
            # position, handling identical-ts tie-breaking by numeric id.
            if ts > cursor_ts or (
                ts == cursor_ts
                and _child_change_cursor_id_key(rid) > _child_change_cursor_id_key(cursor_id)
            ):
                result.append(row)
        return result

    def list_measurement_changes_since(self, cursor_ts: str, cursor_id: str) -> list[dict]:
        """Cheap probe: spore_measurements rows updated since cursor."""
        if not cursor_ts:
            return []
        rows = self._get_paginated(
            f'spore_measurements?user_id=eq.{self.user_id}'
            f'&measured_at=gte.{_encode_postgrest_filter_value(cursor_ts)}'
            f'&select=id,image_id,measured_at'
            f'&order=measured_at.asc,id.asc'
        )
        result = []
        for row in rows:
            ts = str(row.get('measured_at') or '')
            if (ts, _child_change_cursor_id_key(row.get('id', ''))) > (
                cursor_ts, _child_change_cursor_id_key(cursor_id)
            ):
                result.append(row)
        if not result:
            return result
        # Map image_id -> observation_id
        image_ids = list({str(r['image_id']) for r in result if r.get('image_id')})
        obs_map: dict[str, str] = {}
        batch_size = 100
        for i in range(0, len(image_ids), batch_size):
            chunk = image_ids[i:i + batch_size]
            ids_str = ','.join(chunk)
            img_rows = self._get(
                f'observation_images?id=in.({ids_str})'
                f'&user_id=eq.{self.user_id}'
                f'&select=id,observation_id'
            )
            for img in (img_rows or []):
                obs_map[str(img.get('id', ''))] = str(img.get('observation_id', ''))
        for row in result:
            row['observation_id'] = obs_map.get(str(row.get('image_id', '')))
        return result

    def search_community_spore_datasets(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_community_spore_datasets', payload)
        return rows if isinstance(rows, list) else []

    def get_community_spore_dataset(self, observation_id: int) -> dict | None:
        rows = self._rpc(
            'get_community_spore_dataset',
            {'p_observation_id': int(observation_id)},
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def community_spore_taxon_summary(self, genus: str, species: str) -> dict | None:
        rows = self._rpc(
            'community_spore_taxon_summary',
            {
                'p_genus': str(genus or '').strip(),
                'p_species': str(species or '').strip(),
            },
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def search_public_reference_values(
        self,
        genus: str,
        species: str,
        limit: int = 50,
    ) -> list[dict]:
        payload = {
            'p_genus': str(genus or '').strip(),
            'p_species': str(species or '').strip(),
            'p_limit': int(limit or 50),
        }
        rows = self._rpc('search_public_reference_values', payload)
        return rows if isinstance(rows, list) else []

    def set_image_desktop_id(self, cloud_image_id: str, desktop_id: int) -> None:
        """Write the local SQLite image ID back to the cloud image row."""
        self._patch(
            f'observation_images?id=eq.{cloud_image_id}&user_id=eq.{self.user_id}',
            {'desktop_id': desktop_id},
        )

    def soft_delete_image(self, cloud_image_id: str, deleted_at: str | None) -> None:
        """Mark one cloud image row as deleted without removing storage objects."""
        normalized_id = str(cloud_image_id or '').strip()
        if not normalized_id:
            raise CloudSyncError('Missing cloud image id')
        deleted_at_text = str(deleted_at or '').strip() or datetime.now(timezone.utc).isoformat()
        rows = self._get(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}&select=id,deleted_at&limit=1'
        )
        if not rows:
            raise CloudSyncError(f'Cloud image {normalized_id} not found')
        self._patch(
            f'observation_images?id=eq.{normalized_id}&user_id=eq.{self.user_id}',
            {'deleted_at': deleted_at_text},
        )

    def push_measurement(
        self,
        meas: dict,
        cloud_image_id: str,
        *,
        remote_measurement_cache: dict[str, dict] | None = None,
        sync_summary: dict[str, int] | None = None,
    ) -> str:
        """Upsert one spore measurement row. Returns cloud UUID."""
        summary = sync_summary or _cloud_sync_current_summary()
        storage_key = ''
        include_media_keys = False
        if self._measurement_supports_media_keys():
            storage_key = self._cloud_image_storage_key(cloud_image_id)
            include_media_keys = bool(storage_key)

        payload = _measurement_sync_payload(
            meas,
            local=True,
            cloud_image_id=cloud_image_id,
            image_storage_key=storage_key,
            include_media_keys=include_media_keys,
        )
        payload['user_id'] = self.user_id

        portable_identity_pending = bool(meas.get('portable_cloud_identity_pending'))
        if portable_identity_pending:
            payload.pop('desktop_id', None)

        if remote_measurement_cache is not None:
            lookup_keys = _measurement_push_lookup_keys(meas)
            if portable_identity_pending:
                cloud_id = str(meas.get('cloud_id') or '').strip()
                lookup_keys = [f'cloud:{cloud_id}'] if cloud_id else []
            for lookup_key in lookup_keys:
                cached_row = remote_measurement_cache.get(lookup_key)
                if cached_row is None:
                    continue
                existing_id = str(cached_row.get('id') or '').strip()
                if existing_id:
                    if _measurement_payloads_match(
                        meas,
                        cached_row,
                        cloud_image_id=cloud_image_id,
                        image_storage_key=storage_key,
                        include_media_keys=include_media_keys,
                    ):
                        _increment_sync_summary(summary, 'measurements_skipped_noop')
                        return existing_id
                    diff_fields = _measurement_push_diff_fields(
                        meas,
                        cached_row,
                        cloud_image_id=cloud_image_id,
                        image_storage_key=storage_key,
                        include_media_keys=include_media_keys,
                    )
                    if diff_fields:
                        print(
                            f'[cloud_sync] Measurement {int(meas.get("id") or 0)} '
                            f'push diff fields: {", ".join(diff_fields)}'
                        )
                    self._patch(f'spore_measurements?id=eq.{existing_id}', payload)
                    _increment_sync_summary(summary, 'measurements_patched')
                    return existing_id
            rows = self._post('spore_measurements', payload)
            _increment_sync_summary(summary, 'measurements_patched')
            return rows[0]['id']

        if portable_identity_pending:
            local_cloud_id = str(meas.get('cloud_id') or '').strip()
            rows = self._get(
                f'spore_measurements?id=eq.{local_cloud_id}&user_id=eq.{self.user_id}'
                f'&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
            ) if local_cloud_id else []
        else:
            rows = self._get(
            f'spore_measurements?desktop_id=eq.{payload["desktop_id"]}&user_id=eq.{self.user_id}&select={_SPORE_MEASUREMENT_SELECT_COLUMNS}'
            )
        if rows:
            remote_row = dict(rows[0] or {})
            existing_id = str(remote_row.get('id') or '').strip()
            if existing_id:
                if _measurement_payloads_match(
                    meas,
                    remote_row,
                    cloud_image_id=cloud_image_id,
                    image_storage_key=storage_key,
                    include_media_keys=include_media_keys,
                ):
                    _increment_sync_summary(summary, 'measurements_skipped_noop')
                    return existing_id
                diff_fields = _measurement_push_diff_fields(
                    meas,
                    remote_row,
                    cloud_image_id=cloud_image_id,
                    image_storage_key=storage_key,
                    include_media_keys=include_media_keys,
                )
                if diff_fields:
                    print(
                        f'[cloud_sync] Measurement {int(meas.get("id") or 0)} '
                        f'push diff fields: {", ".join(diff_fields)}'
                    )
                self._patch(f'spore_measurements?id=eq.{existing_id}', payload)
                _increment_sync_summary(summary, 'measurements_patched')
                return existing_id
        rows = self._post('spore_measurements', payload)
        _increment_sync_summary(summary, 'measurements_patched')
        return rows[0]['id']

    def delete_cloud_measurements_for_image(self, cloud_image_id: str) -> None:
        """Delete all cloud spore_measurements rows for one image."""
        self._delete(f'spore_measurements?image_id=eq.{cloud_image_id}&user_id=eq.{self.user_id}')

    def delete_cloud_observation(self, obs_cloud_id: str) -> None:
        """Delete one cloud observation and its associated cloud image rows/files."""
        cloud_id = str(obs_cloud_id or '').strip()
        if not cloud_id:
            raise CloudSyncError('Missing cloud observation id')
        total_start = _cloud_sync_perf_counter() if _CLOUD_DEBUG_TIMING else None
        meta_start = _cloud_sync_perf_counter() if _CLOUD_DEBUG_TIMING else None
        image_rows = self.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or []
        mosaic_rows = self._get(
            f'spore_measurement_mosaics?observation_id=eq.{cloud_id}'
            f'&user_id=eq.{self.user_id}&select=storage_key'
        ) or []
        _cloud_timing_log(
            'pull image metadata',
            meta_start,
            detail=f'cloud_id={cloud_id} rows={len(image_rows)}',
        )
        storage_paths: set[str] = set()
        for row in image_rows:
            full_path = _normalize_cloud_media_key((row or {}).get('storage_path'))
            if full_path:
                storage_paths.add(full_path)
                for variant in ('thumb', 'small', 'medium'):
                    storage_paths.add(media_variant_key(full_path, variant))
            original_path = _normalize_cloud_media_key((row or {}).get('original_storage_path'))
            if original_path:
                storage_paths.add(original_path)
        for row in mosaic_rows:
            mosaic_path = _normalize_cloud_media_key((row or {}).get('storage_key'))
            if mosaic_path:
                storage_paths.add(mosaic_path)

        if storage_paths:
            storage_start = _cloud_sync_perf_counter() if _CLOUD_DEBUG_TIMING else None
            # Preserve the canonical identities until every configured bucket
            # has accepted the idempotent delete. A partial Worker failure
            # aborts before DB rows are removed so the whole operation remains
            # discoverable and retryable.
            self._storage_remove(sorted(storage_paths))
            _cloud_timing_log(
                'storage remove',
                storage_start,
                detail=f'cloud_id={cloud_id} paths={len(storage_paths)}',
            )
        images_delete_start = _cloud_sync_perf_counter() if _CLOUD_DEBUG_TIMING else None
        self._delete(f'observation_images?observation_id=eq.{cloud_id}')
        _cloud_timing_log(
            'delete observation_images rows',
            images_delete_start,
            detail=f'cloud_id={cloud_id}',
        )
        observation_delete_start = _cloud_sync_perf_counter() if _CLOUD_DEBUG_TIMING else None
        self._delete(f'observations?id=eq.{cloud_id}')
        _cloud_timing_log(
            'delete observation row',
            observation_delete_start,
            detail=f'cloud_id={cloud_id}',
        )
        _cloud_timing_log('TOTAL delete_cloud_observation', total_start, detail=f'cloud_id={cloud_id}')

    def download_image_file(self, storage_path: str, dest_path: str | Path) -> Path:
        """Download one cloud image from Cloudflare R2 into a local path."""
        profiler = _cloud_sync_current_profiler()
        start = _cloud_sync_perf_counter()
        downloaded_path: Path | None = None
        try:
            storage_key = _normalize_cloud_media_key(storage_path)
            if not storage_key:
                raise CloudSyncError('Missing storage path')
            try:
                # Desktop supplies only the canonical object key and bearer
                # token. The Worker owns legacy/private bucket discovery.
                downloaded_path = Path(
                    self._get_media_worker().download_to_file(
                        storage_key, dest_path, timeout=120)
                )
                return downloaded_path
            except Exception as worker_exc:
                detail = str(worker_exc or '').strip() or worker_exc.__class__.__name__
                if 'nosuchkey' in detail.lower():
                    raise CloudSyncError(
                        f'Cloud image file is missing from storage ({storage_key})'
                    ) from worker_exc
                raise CloudSyncError(f'Download failed: {detail}') from worker_exc
        except CloudSyncError:
            raise
        except Exception as exc:
            detail = str(exc or '').strip()
            if 'nosuchkey' in detail.lower():
                raise CloudSyncError(
                    f'Cloud image file is missing from storage ({storage_key})'
                ) from exc
            raise CloudSyncError(f'Download failed: {detail or exc.__class__.__name__}') from exc
        finally:
            if profiler is not None:
                try:
                    bytes_downloaded = 0
                    if downloaded_path is not None:
                        try:
                            bytes_downloaded = int(Path(downloaded_path).stat().st_size)
                        except Exception:
                            bytes_downloaded = 0
                    profiler.record_download_image_file(
                        max(0.0, (_cloud_sync_perf_counter() - start) * 1000.0),
                        bytes_downloaded,
                    )
                except Exception:
                    pass
            try:
                if downloaded_path is not None and Path(downloaded_path).exists():
                    _increment_sync_summary(_cloud_sync_current_summary(), 'remote_media_downloads')
            except Exception:
                pass


class OAuthSporelyCloudClient(SporelyCloudClient):
    """SporelyCloudClient that refreshes via the Supabase OAuth endpoint.

    Constructed from an OAuthTokenResult (Stage 4). Uses
    SporelyDesktopOAuthClient.refresh() instead of the password-session
    /auth/v1/token?grant_type=refresh_token path.
    Never reads or writes desktop passwords.
    """

    def __init__(
        self,
        access_token: str,
        user_id: str,
        refresh_token: str | None = None,
        user_email: str | None = None,
    ):
        super().__init__(access_token, user_id, refresh_token)
        self.user_email = str(user_email or '').strip() or None

    @classmethod
    def from_oauth_session(cls, result) -> 'OAuthSporelyCloudClient':
        """Construct from a freshly-obtained OAuthTokenResult.

        result.user_id / result.user_email are trusted when present;
        JWT sub is used as authoritative fallback.
        Never persists passwords, authorization codes, or PKCE state.
        Call save_credentials() on the returned client to persist the session.
        """
        access_token = str(result.access_token or '').strip()
        if not access_token:
            raise CloudSyncError("OAuth session is missing an access token.")
        user_id = ''
        # JWT sub is authoritative; fall back to server-provided user_id.
        jwt_user_id = _decode_jwt_subject(access_token)
        if jwt_user_id:
            user_id = jwt_user_id
        elif result.user_id:
            user_id = _normalize_cloud_user_id(result.user_id)
        if not user_id:
            raise CloudSyncError("OAuth session is missing user identity.")
        # An empty refresh_token means the server did not rotate it.
        # A freshly-obtained session with no refresh token simply stores None.
        refresh_token = str(result.refresh_token or '').strip() or None
        return cls(
            access_token=access_token,
            user_id=user_id,
            refresh_token=refresh_token,
            user_email=getattr(result, 'user_email', None),
        )

    @classmethod
    def refresh_login(cls, refresh_token: str) -> 'OAuthSporelyCloudClient':
        """Refresh via the Supabase OAuth endpoint (not the password path).

        OAuthError (revoked/invalid token) -> CloudReauthRequiredError.
        RuntimeError (network/infra failure) -> CloudTemporarilyUnavailableError.
        Empty returned refresh_token -> preserve the supplied token (non-rotating server).
        """
        from utils.sporely_cloud_auth import SporelyDesktopOAuthClient, OAuthError
        token = str(refresh_token or '').strip()
        if not token:
            raise CloudSyncError('Missing refresh token')
        try:
            result = SporelyDesktopOAuthClient().refresh(token)
        except OAuthError as exc:
            raise CloudReauthRequiredError(
                "OAuth refresh token is invalid or has been revoked. (invalid_grant)"
            ) from exc
        except RuntimeError as exc:
            raise CloudTemporarilyUnavailableError(
                f"OAuth token refresh temporarily failed: {exc}"
            ) from exc
        user_id = _decode_jwt_subject(result.access_token) or _normalize_cloud_user_id(result.user_id)
        # Preserve the existing refresh token when the server does not rotate it.
        new_refresh = str(result.refresh_token or '').strip() or token
        return cls(
            access_token=result.access_token,
            user_id=user_id,
            refresh_token=new_refresh,
            user_email=getattr(result, 'user_email', None),
        )

    def save_credentials(
        self,
        email: str | None = None,
        password: str | None = None,
        remember_password: bool | None = None,
    ) -> None:
        """Persist OAuth session tokens; never reads or writes passwords."""
        updates: dict[str, object] = {
            'cloud_access_token': self.access_token,
            'cloud_user_id': self.user_id,
            'cloud_refresh_token': self.refresh_token,
            'cloud_auth_method': 'oauth',
        }
        if email is not None:
            updates['cloud_user_email'] = str(email or '').strip()
        update_app_settings(updates)

    @classmethod
    def from_stored_credentials(cls) -> 'OAuthSporelyCloudClient | None':
        """Load an OAuth session from settings.

        Unlike the base class, does NOT fall through to password login.
        CloudReauthRequiredError propagates so callers can surface the
        reauth_required status instead of silently returning None.
        """
        settings = get_app_settings()
        token = settings.get('cloud_access_token')
        user_id = settings.get('cloud_user_id')
        refresh_token = settings.get('cloud_refresh_token')
        user_email = settings.get('cloud_user_email')
        token_text = str(token or '').strip()
        user_id_text = _normalize_cloud_user_id(user_id)
        refresh_text = str(refresh_token or '').strip() or None
        if token_text and user_id_text:
            token_user_id = _decode_jwt_subject(token_text)
            client = cls(
                access_token=token_text,
                user_id=token_user_id or user_id_text,
                refresh_token=refresh_text,
                user_email=user_email,
            )
            expiry_seconds = _decode_jwt_expiry(token_text)
            if (
                expiry_seconds is not None
                and _jwt_expires_soon(token_text)
                and refresh_text
            ):
                try:
                    client._refresh_session_if_possible()
                    return client
                except CloudTemporarilyUnavailableError:
                    return client  # transient — caller retries on first request
                except CloudReauthRequiredError:
                    raise  # OAuth: propagate; do not fall through to password
            else:
                return client
        if refresh_text:
            try:
                client = cls.refresh_login(refresh_text)
                client.save_credentials()
                return client
            except CloudTemporarilyUnavailableError:
                raise
            except CloudReauthRequiredError:
                raise  # propagate; caller surfaces reauth_required status
            except CloudSyncError:
                return None
        return None

    @classmethod
    def login(cls, *args, **kwargs) -> 'OAuthSporelyCloudClient':
        raise CloudSyncError(
            "OAuth sessions do not support password login. "
            "Use SporelyDesktopOAuthClient.authorize() to obtain a new session."
        )


class SporelyReadOnlyCloudClient(SporelyCloudClient):
    """Fixed-token conflict-review client.

    Every network call this subclass makes is read-only.  It cannot refresh a
    session, persist credentials, log in, log out, or write anything back to
    Sporely Cloud.  On authentication failure it raises
    :class:`CloudReauthRequiredError` (or ``CloudSyncError`` from a specific
    caller path) and the caller decides how to prompt the user — the client
    itself never touches settings.

    Prefer this class over monkey-patching a plain ``SporelyCloudClient``:
    the ``_request_with_refresh`` override guarantees ``refresh_on_auth_error``
    is always ``False`` even for future call sites (e.g. new REST helpers),
    and ``download_image_file_read_only`` gives conflict-comparison callers a
    named, obvious entry point.
    """

    def _refresh_session_if_possible(self) -> bool:  # type: ignore[override]
        return False

    def save_credentials(self) -> None:  # type: ignore[override]
        return None

    def clear_session(self) -> None:  # type: ignore[override]
        raise CloudSyncError('Read-only conflict-review client cannot clear session')

    def clear_credentials(self) -> None:  # type: ignore[override]
        raise CloudSyncError('Read-only conflict-review client cannot clear credentials')

    def login(self, *args, **kwargs):  # type: ignore[override]
        raise CloudSyncError('Read-only conflict-review client cannot log in')

    @classmethod
    def refresh_login(cls, *args, **kwargs):  # type: ignore[override]
        raise CloudSyncError('Read-only conflict-review client cannot refresh a session')

    def _request_with_refresh(
        self, method: str, url: str, *, refresh_on_auth_error: bool = True, **kwargs
    ):  # type: ignore[override]
        # Force refresh_on_auth_error=False for every request originating from
        # this client, regardless of what the caller asked.  Also strip the
        # refresh callback so an errant refactor cannot re-introduce a refresh
        # path through this instance.
        return _request_with_transient_retry(
            self._s.request,
            method,
            url,
            refresh_on_auth_error=False,
            refresh_callback=None,
            **kwargs,
        )

    def _get(self, path):  # type: ignore[override]
        return self.get_read_only(path)

    def download_image_file_read_only(self, storage_path: str, dest_path):
        """Named read-only entry point for the conflict thumbnail worker.

        Uses the same authenticated Worker primitive as
        ``download_image_file`` without touching session state. Because this
        instance's ``_request_with_refresh``
        is refresh-disabled, any future addition to the download path that
        routes through it will remain read-only.
        """
        return self.download_image_file(storage_path, dest_path)


def _worker_base_url_from_request_url(request_url: str | None) -> str:
    text = str(request_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _format_cloud_image_too_large_error(
    local_path: str | Path,
    upload_meta: dict | None = None,
    inner_error: Exception | str | None = None,
    *,
    upload_variant: str | None = None,
    storage_key: str | None = None,
    content_type: str | None = None,
    prepared_path_suffix: str | None = None,
    worker_base_url: str | None = None,
) -> str:
    meta = dict(upload_meta or {})
    path = Path(str(local_path or "").strip())
    upload_policy = _cloud_upload_policy_from_meta(meta)

    details: list[str] = []
    worker_payload: dict[str, object] = {}
    worker_request_url = ""
    worker_request_method = ""
    worker_response_status = 0
    worker_response_text = ""
    worker_request_headers: dict[str, str] = {}
    if isinstance(inner_error, dict):
        worker_payload = dict(inner_error)
    elif inner_error is not None:
        for attr in ('payload', 'response_payload', 'response', 'body'):
            try:
                candidate = getattr(inner_error, attr)
            except Exception:
                candidate = None
            if isinstance(candidate, dict):
                worker_payload = dict(candidate)
                break
        try:
            worker_request_url = str(getattr(inner_error, 'request_url', '') or '').strip()
        except Exception:
            worker_request_url = ""
        try:
            worker_request_method = str(getattr(inner_error, 'request_method', '') or '').strip().upper()
        except Exception:
            worker_request_method = ""
        try:
            worker_response_status = _safe_int(
                getattr(inner_error, 'response_status', None)
                or getattr(inner_error, 'response_status_code', None)
                or getattr(inner_error, 'status_code', None)
            )
        except Exception:
            worker_response_status = 0
        try:
            worker_response_text = str(getattr(inner_error, 'response_text', '') or getattr(inner_error, 'text', '') or '').strip()
        except Exception:
            worker_response_text = ""
        try:
            headers = getattr(inner_error, 'request_headers', None)
        except Exception:
            headers = None
        if isinstance(headers, dict):
            worker_request_headers = {
                str(name): str(value).strip()
                for name, value in headers.items()
                if str(name or "").strip() and str(value or "").strip()
            }

    worker_details = worker_payload.get('details')
    worker_detail_map = worker_details if isinstance(worker_details, dict) else {}
    worker_code = str(
        worker_payload.get('error')
        or worker_payload.get('code')
        or worker_detail_map.get('error')
        or worker_detail_map.get('code')
        or ''
    ).strip()
    worker_message = str(
        worker_payload.get('message')
        or worker_detail_map.get('message')
        or worker_detail_map.get('detail')
        or ''
    ).strip()
    worker_reason = str(worker_payload.get('reason') or worker_detail_map.get('reason') or '').strip()
    if worker_payload and not worker_detail_map:
        details.append("Worker details: missing")

    observation_label = str(meta.get('observation_label') or '').strip()
    observation_id = str(meta.get('observation_id') or '').strip()
    if observation_label and observation_id:
        details.append(f"Observation: {observation_label} (ID {observation_id})")
    elif observation_label:
        details.append(f"Observation: {observation_label}")
    elif observation_id:
        details.append(f"Observation ID: {observation_id}")

    image_label = str(meta.get('image_label') or '').strip()
    image_id = str(meta.get('image_id') or '').strip()
    if image_label and image_id:
        details.append(f"Image: {image_label} (ID {image_id})")
    elif image_label:
        details.append(f"Image: {image_label}")
    elif image_id:
        details.append(f"Image ID: {image_id}")

    source_path = str(meta.get('source_path') or '').strip()
    source_filename = str(meta.get('source_filename') or '').strip()
    if source_path:
        details.append(f"Original file: {source_path}")
    elif source_filename:
        details.append(f"Original file: {source_filename}")

    source_bytes = _safe_int(meta.get('source_bytes'))
    if source_bytes <= 0 and source_path:
        try:
            source_bytes = int(Path(source_path).stat().st_size)
        except Exception:
            source_bytes = 0
    if source_bytes <= 0:
        try:
            source_bytes = int(path.stat().st_size)
        except Exception:
            source_bytes = 0
    if source_bytes > 0:
        details.append(f"Original size: {_format_size(source_bytes)}")

    source_width = _safe_int(meta.get('source_width'))
    source_height = _safe_int(meta.get('source_height'))
    if source_width <= 0 or source_height <= 0:
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
                source_width = int(img.width or 0)
                source_height = int(img.height or 0)
        except Exception:
            source_width = source_width or 0
            source_height = source_height or 0
    if source_width > 0 and source_height > 0:
        details.append(f"Original dimensions: {source_width} × {source_height} px")

    prepared_bytes = _safe_int(meta.get('stored_bytes'))
    if prepared_bytes <= 0:
        prepared_bytes = _safe_int(worker_detail_map.get('bodyBytes') or worker_detail_map.get('body_bytes'))
    if prepared_bytes <= 0:
        prepared_bytes = _safe_int(worker_detail_map.get('storedBytes') or worker_detail_map.get('stored_bytes'))
    if prepared_bytes <= 0:
        try:
            prepared_bytes = int(path.stat().st_size)
        except Exception:
            prepared_bytes = 0
    if prepared_bytes > 0:
        details.append(f"Prepared upload size: {_format_size(prepared_bytes)}")

    prepared_width = _safe_int(meta.get('stored_width'))
    prepared_height = _safe_int(meta.get('stored_height'))
    if (prepared_width <= 0 or prepared_height <= 0) and worker_detail_map:
        prepared_width = prepared_width or _safe_int(
            worker_detail_map.get('storedWidth')
            or worker_detail_map.get('stored_width')
        )
        prepared_height = prepared_height or _safe_int(
            worker_detail_map.get('storedHeight')
            or worker_detail_map.get('stored_height')
        )
    if prepared_width > 0 and prepared_height > 0:
        details.append(f"Prepared dimensions: {prepared_width} × {prepared_height} px")

    prepared_suffix = str(
        prepared_path_suffix
        or meta.get('prepared_path_suffix')
        or meta.get('preparedPathSuffix')
        or ''
    ).strip().lower()
    if not prepared_suffix:
        prepared_suffix = path.suffix.lower() or ''
    if not prepared_suffix and content_type:
        lowered_content_type = str(content_type or '').strip().lower()
        if lowered_content_type == 'image/webp':
            prepared_suffix = '.webp'
        elif lowered_content_type in {'image/jpeg', 'image/jpg'}:
            prepared_suffix = '.jpg'
        elif lowered_content_type == 'image/png':
            prepared_suffix = '.png'
        elif lowered_content_type == 'image/avif':
            prepared_suffix = '.avif'
        elif lowered_content_type == 'image/tiff':
            prepared_suffix = '.tif'
    if prepared_suffix:
        details.append(f"Prepared path suffix: {prepared_suffix}")

    encoding_format = str(meta.get('encoding_format') or meta.get('encodingFormat') or '').strip().lower()
    if not encoding_format and worker_detail_map:
        encoding_format = str(
            worker_detail_map.get('encodingFormat')
            or worker_detail_map.get('encoding_format')
            or ''
        ).strip().lower()
    if encoding_format:
        details.append(f"Encoding format: {encoding_format}")

    byte_cap = _safe_int(
        meta.get('full_image_byte_cap')
        or meta.get('fullImageByteCap')
        or upload_policy.get('fullImageByteCap')
        or upload_policy.get('full_image_byte_cap')
    )
    if byte_cap > 0:
        details.append(f"Plan cap: {_format_size(byte_cap)}")

    local_upload_variant = str(
        upload_variant
        or meta.get('upload_variant')
        or meta.get('uploadVariant')
        or worker_request_headers.get('X-Sporely-Upload-Variant')
        or worker_detail_map.get('uploadVariant')
        or worker_detail_map.get('upload_variant')
        or ''
    ).strip().lower()
    if local_upload_variant:
        details.append(f"Local upload variant: {local_upload_variant}")

    upload_mode = str(
        meta.get('upload_mode')
        or meta.get('uploadMode')
        or worker_request_headers.get('X-Sporely-Upload-Mode')
        or worker_detail_map.get('uploadMode')
        or worker_detail_map.get('upload_mode')
        or ''
    ).strip().lower()
    quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    if not quality_profile and worker_detail_map:
        quality_profile = str(
            worker_detail_map.get('qualityProfile')
            or worker_detail_map.get('quality_profile')
            or ''
        ).strip().lower()
    if upload_mode or quality_profile:
        details.append(
            f"Local upload mode: {upload_mode or 'unknown'} / {quality_profile or 'standard'}"
        )

    local_content_type = str(
        content_type
        or meta.get('content_type')
        or meta.get('contentType')
        or worker_request_headers.get('Content-Type')
        or worker_detail_map.get('contentType')
        or worker_detail_map.get('content_type')
        or ''
    ).strip().lower()
    if local_content_type:
        details.append(f"Content type: {local_content_type}")

    if worker_base_url:
        worker_base = str(worker_base_url or '').strip().rstrip('/')
    else:
        worker_base = _worker_base_url_from_request_url(worker_request_url)
        if not worker_base and worker_payload:
            worker_base = media_worker_base_url()
    if worker_base:
        details.append(f"Worker base URL: {worker_base}")

    storage_key_text = str(storage_key or meta.get('storage_path') or meta.get('storageKey') or '').strip()
    if storage_key_text:
        details.append(f"Storage key: {storage_key_text}")

    desktop_cloud_plan = str(meta.get('cloud_plan') or meta.get('cloudPlan') or '').strip().lower()
    desktop_quality_profile = str(meta.get('quality_profile') or meta.get('qualityProfile') or '').strip().lower()
    if desktop_cloud_plan or desktop_quality_profile:
        details.append(
            f"Desktop cloud plan: {desktop_cloud_plan or 'unknown'} / {desktop_quality_profile or 'standard'}"
        )

    worker_cloud_plan = ""
    worker_quality_profile = ""
    worker_plan_cap = 0
    worker_body_bytes = 0
    worker_stored_width = 0
    worker_stored_height = 0
    worker_stored_pixels = 0
    worker_stored_pixel_cap = 0
    worker_resize_max_edge = 0
    worker_upload_mode = ""
    worker_upload_variant = ""
    worker_encoding_format = ""
    worker_content_type = ""
    reason = ""

    if worker_payload:
        worker_cloud_plan = str(
            worker_detail_map.get('cloudPlan')
            or worker_detail_map.get('cloud_plan')
            or worker_payload.get('cloudPlan')
            or worker_payload.get('cloud_plan')
            or ''
        ).strip().lower()
        worker_quality_profile = str(
            worker_detail_map.get('qualityProfile')
            or worker_detail_map.get('quality_profile')
            or worker_payload.get('qualityProfile')
            or worker_payload.get('quality_profile')
            or ''
        ).strip().lower()
        worker_plan_cap = _safe_int(
            worker_detail_map.get('planByteCap')
            or worker_detail_map.get('plan_byte_cap')
            or worker_payload.get('planByteCap')
            or worker_payload.get('plan_byte_cap')
        )
        worker_body_bytes = _safe_int(
            worker_detail_map.get('bodyBytes')
            or worker_detail_map.get('body_bytes')
            or worker_payload.get('bodyBytes')
            or worker_payload.get('body_bytes')
        )
        worker_stored_width = _safe_int(
            worker_detail_map.get('storedWidth')
            or worker_detail_map.get('stored_width')
        )
        worker_stored_height = _safe_int(
            worker_detail_map.get('storedHeight')
            or worker_detail_map.get('stored_height')
        )
        worker_stored_pixels = _safe_int(
            worker_detail_map.get('storedPixels')
            or worker_detail_map.get('stored_pixels')
        )
        worker_stored_pixel_cap = _safe_int(
            worker_detail_map.get('storedPixelCap')
            or worker_detail_map.get('stored_pixel_cap')
        )
        worker_resize_max_edge = _safe_int(
            worker_detail_map.get('resizeMaxEdge')
            or worker_detail_map.get('resize_max_edge')
        )
        worker_upload_mode = str(
            worker_detail_map.get('uploadMode')
            or worker_detail_map.get('upload_mode')
            or worker_request_headers.get('X-Sporely-Upload-Mode')
            or ''
        ).strip().lower()
        worker_upload_variant = str(
            worker_detail_map.get('uploadVariant')
            or worker_detail_map.get('upload_variant')
            or worker_request_headers.get('X-Sporely-Upload-Variant')
            or ''
        ).strip().lower()
        worker_encoding_format = str(
            worker_detail_map.get('encodingFormat')
            or worker_detail_map.get('encoding_format')
            or worker_request_headers.get('X-Sporely-Encoding-Format')
            or ''
        ).strip().lower()
        worker_content_type = str(
            worker_detail_map.get('contentType')
            or worker_detail_map.get('content_type')
            or worker_request_headers.get('Content-Type')
            or ''
        ).strip().lower()

        if worker_code:
            details.append(f"Worker error code: {worker_code}")
        if worker_message:
            details.append(f"Worker message: {worker_message}")
        if worker_reason:
            details.append(f"Worker reason: {worker_reason}")
        if worker_cloud_plan or worker_quality_profile:
            details.append(
                f"Worker cloud plan: {worker_cloud_plan or 'unknown'} / {worker_quality_profile or 'standard'}"
            )
        if worker_plan_cap > 0:
            details.append(f"Worker plan cap: {_format_size(worker_plan_cap)}")
        if worker_body_bytes > 0:
            details.append(f"Worker body size: {_format_size(worker_body_bytes)}")
        if worker_stored_width > 0 and worker_stored_height > 0:
            details.append(f"Worker stored dimensions: {worker_stored_width} × {worker_stored_height} px")
        if worker_stored_pixels > 0:
            details.append(f"Worker stored pixels: {worker_stored_pixels:,}")
        if worker_stored_pixel_cap > 0:
            details.append(f"Worker stored pixel cap: {worker_stored_pixel_cap:,}")
        if worker_resize_max_edge > 0:
            details.append(f"Worker resize max edge: {worker_resize_max_edge}px")
        if worker_upload_mode or worker_upload_variant:
            details.append(
                f"Worker upload mode: {worker_upload_mode or 'unknown'} / {worker_upload_variant or 'unknown'}"
            )
            if worker_encoding_format or worker_content_type:
                details.append(
                    f"Worker encoding/content type: {worker_encoding_format or 'unknown'} / {worker_content_type or 'unknown'}"
                )

    if not reason:
        reason = infer_image_too_large_for_plan_reason(
            {
                'error': worker_code or 'image_too_large_for_plan',
                'message': worker_message,
                'reason': worker_reason,
                'details': {
                    'bodyBytes': worker_body_bytes,
                    'planByteCap': worker_plan_cap,
                    'storedWidth': worker_stored_width,
                    'storedHeight': worker_stored_height,
                    'storedPixels': worker_stored_pixels,
                    'storedPixelCap': worker_stored_pixel_cap,
                    'resizeMaxEdge': worker_resize_max_edge,
                    'preparedBytes': prepared_bytes,
                    'planCap': byte_cap,
                    'preparedWidth': prepared_width,
                    'preparedHeight': prepared_height,
                },
            }
        )

    if inner_error is not None:
        extra = str(inner_error or '').strip()
        if extra and not is_image_too_large_for_plan_error(extra):
            details.append(f"Details: {extra}")

    first_line = format_image_too_large_for_plan_reason(reason)
    if not details:
        return first_line
    return "\n".join([first_line, "", *details])


# ── High-level sync entry points ──────────────────────────────────────────────

def push_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    sync_images: bool = True,
    prepare_images_cb: PreparedImagesCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
    sync_calibrations: bool = True,
    full_pull: bool = True,
    verify_stamped_measurements: bool = True,
) -> dict:
    """Push all unsynced / dirty observations (and optionally images) to cloud.

    Returns a summary dict with counts.
    """
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()

    # Observation preflight: the dirty scans below run before any per-observation
    # progress update, so on a no-change sync (0 dirty rows) they were a silent
    # gap that left the UI stuck on the last calibration label. Emit a neutral
    # message first and time each sub-step so a slow scan is named in the log.
    _set_progress_phase(progress_state, 'observation_preflight', phase_total=1)
    preflight_start = _cloud_sync_perf_counter()
    print("[cloud_sync] observation preflight: start", flush=True)
    _emit_progress(progress_cb, "Checking local observation changes…", progress_state)

    media_scan_start = _cloud_sync_perf_counter()
    _mark_cloud_observations_dirty_for_media_changes()
    media_scan_elapsed = _cloud_sync_perf_counter() - media_scan_start
    print(
        f"[cloud_sync] observation preflight: media dirty scan complete "
        f"duration={media_scan_elapsed * 1000:.0f}ms",
        flush=True,
    )

    if sync_images:
        capture_time_dirty_start = _cloud_sync_perf_counter()
        capture_time_redirtied = _mark_cloud_observations_dirty_for_image_capture_time_changes()
        print(
            f"[cloud_sync] observation preflight: image capture-time dirty scan complete "
            f"re_dirtied={capture_time_redirtied} "
            f"duration={(_cloud_sync_perf_counter() - capture_time_dirty_start) * 1000:.0f}ms",
            flush=True,
        )
        pending_scan_start = _cloud_sync_perf_counter()
        pending_scan_due, pending_scan_reason = _cloud_pending_image_repair_scan_due()
        pending_scan_completed = False
        if pending_scan_due:
            pending_scan_completed = _mark_cloud_observations_dirty_for_pending_local_images(
                include_pending_local_media_uploads=True,
            )
            if pending_scan_completed:
                pending_scan_completed = _record_cloud_pending_image_repair_scan_complete()
        pending_scan_elapsed = _cloud_sync_perf_counter() - pending_scan_start
        redirtied = _sync_summary_value(
            _cloud_sync_current_summary(), 'observations_redirtied_pending_local_images'
        )
        if pending_scan_due:
            print(
                f"[cloud_sync] observation preflight: pending image dirty scan complete "
                f"re_dirtied={redirtied} completed={pending_scan_completed} "
                f"reason={pending_scan_reason} duration={pending_scan_elapsed * 1000:.0f}ms",
                flush=True,
            )
        else:
            print(
                f"[cloud_sync] observation preflight: pending image dirty scan skipped "
                f"reason={pending_scan_reason} duration={pending_scan_elapsed * 1000:.0f}ms",
                flush=True,
            )

    # Image deletion is metadata work, not image-byte preparation. Flush the
    # global queue before pruning to dirty observations so an unchecked image
    # is removed from cloud views even when its observation otherwise takes
    # the no-op fast path.
    tombstone_warnings = _push_pending_image_tombstones(client)

    calibration_result = {'pushed': 0, 'total': 0, 'errors': []}
    if sync_calibrations:
        calibration_result = push_calibrations(
            client,
            progress_cb=progress_cb,
            progress_state=progress_state,
        )

    dirty_scan_start = _cloud_sync_perf_counter()
    cursor.execute(
        "SELECT * FROM observations WHERE cloud_id IS NULL OR sync_status = 'dirty' ORDER BY date DESC"
    )
    observations = [dict(r) for r in cursor.fetchall()]
    conn.close()
    dirty_scan_elapsed = _cloud_sync_perf_counter() - dirty_scan_start
    print(
        f"[cloud_sync] observation preflight: local dirty scan complete "
        f"count={len(observations)} duration={dirty_scan_elapsed * 1000:.0f}ms",
        flush=True,
    )
    print(
        f"[cloud_sync] observation preflight: complete "
        f"candidates={len(observations)} duration={(_cloud_sync_perf_counter() - preflight_start) * 1000:.0f}ms",
        flush=True,
    )

    total = len(observations)
    pushed = 0
    errors = list(calibration_result.get('errors') or []) + list(tombstone_warnings or [])
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    # Mark the preflight complete before switching to the per-observation
    # phase so the bar sits at the push_observations start value.
    _advance_progress(progress_state, 1)
    _set_progress_phase(progress_state, 'push_observations', phase_total=total)
    original_upload_summary = _new_original_upload_summary(is_full_resolution_original_sync_enabled())
    remote_lookup = {
        str(row.get('id') or '').strip(): row
        for row in (remote_obs or [])
        if str(row.get('id') or '').strip()
    }

    for i, obs in enumerate(observations):
        _increment_sync_summary(_cloud_sync_current_summary(), 'observations_checked')
        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                obs,
                f"Syncing observation {i + 1}/{max(1, total)}…",
            ),
            progress_state,
        )
        try:
            cloud_id = str(obs.get('cloud_id') or '').strip()
            had_existing_cloud = bool(cloud_id)
            stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
            # The stored sync baseline, independent of whether the cloud has
            # diverged from it. Needed even on the no-cloud-change fast path:
            # an explicit identity clear (sync-integrity follow-up 4) is
            # triggered by a LOCAL identification change against this
            # baseline, not by any remote divergence.
            identity_baseline_obs = None
            if stored_snapshot:
                try:
                    identity_baseline_obs = _baseline_observation_compare_payload(
                        _parse_cloud_observation_snapshot(stored_snapshot).get('observation') or {}
                    )
                except Exception:
                    identity_baseline_obs = None
            remote = remote_lookup.get(cloud_id) if cloud_id else None
            if cloud_id and remote is None:
                _advance_progress(progress_state, 1)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Cloud copy was deleted for observation {i + 1}/{max(1, total)}",
                    ),
                    progress_state,
                )
                continue
            push_payload = dict(obs)
            if cloud_id and stored_snapshot and remote:
                if sync_images:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Checking cloud media for observation {i + 1}/{max(1, total)}…",
                        ),
                        progress_state,
                    )
                remote_images = client.pull_image_metadata(cloud_id) or []
                remote_measurements = _pull_remote_measurements_for_images(
                    client,
                    [str(row.get('id') or '').strip() for row in remote_images if str(row.get('id') or '').strip()],
                )
                remote_snapshot = _cloud_observation_snapshot(remote, remote_images, remote_measurements)
                if remote_snapshot != stored_snapshot and _clear_observation_dirty_if_no_real_changes(int(obs['id']), cloud_id):
                    _advance_progress(progress_state, 1)
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Skipped stale local change for observation {i + 1}/{max(1, total)}",
                        ),
                        progress_state,
                    )
                    continue
                if remote_snapshot != stored_snapshot:
                    snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                    baseline_obs = _baseline_observation_compare_payload(snapshot_data.get('observation') or {})
                    field_changes = _analyze_observation_field_changes(obs, remote, baseline_obs)
                    # A cloud-only identity change is adopted locally now, exactly
                    # as the pull would. Merely withholding it from this push is
                    # not enough: the post-push snapshot records the cloud value
                    # as baseline, and a stale local identity would then read as
                    # a local change and be re-asserted by the next push.
                    if TAXON_IDENTITY_SYNC_FIELD in (field_changes.get('remote_only_fields') or []):
                        _apply_remote_observation_fields(
                            int(obs['id']), remote, fields={TAXON_IDENTITY_SYNC_FIELD},
                        )
                        adopted = ObservationDB.get_observation(int(obs['id'])) or {}
                        for column in ('sporely_taxon_id', 'scientific_name_snapshot',
                                       'taxon_rank_snapshot', *_TAXON_IDENTITY_COLUMNS):
                            push_payload[column] = adopted.get(column)
                        if TaxonIdentity.from_row(push_payload).is_proven_sporely:
                            # Cannot happen (adoption never yields proof); never
                            # let a remote-only change become an RPC write.
                            _withhold_identity_from_push(push_payload)

                    # Preflight: mirror pull_all's review-needed contract. If
                    # metadata, images, or measurements diverged on both
                    # sides, block the entire per-observation push pipeline so
                    # we never silently overwrite a remote change.
                    obs_local_id = _safe_int(obs.get('id'))
                    local_images_for_preflight = (
                        ImageDB.get_images_for_observation(obs_local_id)
                        if obs_local_id > 0
                        else []
                    )
                    local_measurements_by_cloud_id: dict[str, dict] = {}
                    if obs_local_id > 0:
                        try:
                            local_measurements_by_cloud_id, _ = _load_local_measurement_lookup(obs_local_id)
                        except Exception:
                            local_measurements_by_cloud_id = {}
                    conflict_report = _analyze_observation_push_conflicts(
                        local_obs=dict(obs),
                        local_images=[dict(row or {}) for row in (local_images_for_preflight or [])],
                        local_measurements_by_cloud_id=local_measurements_by_cloud_id,
                        remote_obs=dict(remote or {}),
                        remote_images=[dict(row or {}) for row in (remote_images or [])],
                        remote_measurements=[dict(row or {}) for row in (remote_measurements or [])],
                        baseline_snapshot=snapshot_data,
                    )
                    if conflict_report.has_conflict:
                        review_reasons = _format_push_conflict_review_reasons(conflict_report)
                        errors.append(
                            _format_review_needed_error(
                                obs_local_id if obs_local_id > 0 else 0,
                                cloud_id,
                                ['push_blocked', *(review_reasons or [])],
                            )
                        )
                        if obs_local_id > 0:
                            _set_observation_conflict_review_pending(obs_local_id)
                        print(
                            f"[cloud_sync] conflict push blocked: obs={obs_local_id} "
                            f"categories={list(conflict_report.categories)} "
                            f"action=review_required",
                            flush=True,
                        )
                        _advance_progress(progress_state, 1)
                        _emit_progress(
                            progress_cb,
                            _format_cloud_sync_observation_status(
                                obs,
                                (
                                    f"Observation {i + 1}/{max(1, total)} needs review "
                                    f"before syncing"
                                ),
                            ),
                            progress_state,
                        )
                        continue

                    # Cloud-only ordinary field edits are adopted locally, through
                    # the pull's field-apply path, before the push — for the same
                    # reason as the identity above: the post-push snapshot records
                    # the cloud value as baseline, so a stale local value would
                    # later read as a local change and overwrite the cloud edit.
                    # Adopting before the cloud write means any later failure
                    # leaves local == cloud (a shared value), never the reverse.
                    # The payload takes the adopted local values, so what is
                    # pushed and what is stored locally cannot diverge.
                    ordinary_remote_only_fields = {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('remote_only_fields') or [])
                    } - {TAXON_IDENTITY_SYNC_FIELD}
                    if ordinary_remote_only_fields:
                        _apply_remote_observation_fields(
                            int(obs['id']), remote, fields=ordinary_remote_only_fields,
                        )
                        adopted = ObservationDB.get_observation(int(obs['id'])) or {}
                        for field in ordinary_remote_only_fields:
                            if field in adopted:
                                push_payload[field] = adopted[field]

            # If the preflight ran and reported no conflict (or the fast path
            # short-circuited above), the observation is safe to push. The
            # `update_observation_sync_state(..., clear_sync_error_state=True)`
            # below clears any prior review-pending marker as part of the
            # normal `dirty→synced` transition.

            # Sync-integrity follow-up 4, Case F: with no usable identity
            # baseline (no stored snapshot, or a stored snapshot that predates
            # identity joining change detection), a committed local
            # identification that CONTRADICTS the cloud's currently bound
            # identity — different genus/species, and the local row makes no
            # claim to any identity at all — must never partially push. The
            # ordinary field-level preflight above only runs when a baseline
            # exists, so without one nothing stops the local names from
            # PATCHing over the cloud row while its bound identity stays
            # untouched, leaving the exact stale/contradictory row Stage C
            # fixes on the other side (new names, old identity). This blocks
            # the WHOLE observation push before any cloud mutation, through
            # the same conflict-review mechanism the known-baseline preflight
            # uses (`_format_review_needed_error`,
            # `_set_observation_conflict_review_pending`), rather than the
            # narrower "withhold identity only" handling below — that one
            # covers a local row that already CLAIMS an identity (proven or a
            # preserved external tuple) which disagrees with the cloud's, and
            # deliberately still lets independent ordinary fields (e.g. notes)
            # go out while the identity itself stays under review (see
            # tests/test_cloud_identity_fail_closed.py::
            # test_no_baseline_disagreement_stays_under_review_and_blocks_the_next_push).
            # A non-claiming local with contradicting names is a different,
            # narrower case: the contradiction IS the identification itself,
            # so nothing about this observation is safe to push.
            baseline_identity_unknown = (
                identity_baseline_obs is None
                or _baseline_identity_key(identity_baseline_obs) is _IDENTITY_BASELINE_UNKNOWN
            )
            if baseline_identity_unknown and cloud_id and remote:
                remote_claim_for_contradiction = _remote_identity_claim(remote)
                no_baseline_no_claim_vs_remote_identity = bool(
                    remote_claim_for_contradiction is not None
                    and remote_claim_for_contradiction.key
                    and not _local_identity_is_claim(push_payload)
                )
                if no_baseline_no_claim_vs_remote_identity and _identification_contradicts_remote(
                    push_payload, dict(remote or {}),
                ):
                    obs_local_id_for_block = _safe_int(obs.get('id'))
                    errors.append(_format_review_needed_error(
                        obs_local_id_for_block, cloud_id,
                        ['push_blocked', _format_observation_metadata_field_label(TAXON_IDENTITY_SYNC_FIELD)],
                    ))
                    if obs_local_id_for_block > 0:
                        _set_observation_conflict_review_pending(obs_local_id_for_block)
                    # Privacy exception (Stage C review round 2, item 2): a
                    # strictly MORE restrictive visibility change (narrowing,
                    # e.g. public -> private) may still reach the cloud while
                    # the observation is blocked for review — otherwise the
                    # owner's own narrowing choice silently never propagates,
                    # which is a privacy release blocker, not a sync nicety.
                    # This is the ONLY field the exception ever touches: it
                    # never carries identification/name/any other field, and
                    # never marks the observation synced, clears the
                    # conflict marker, or stores a baseline snapshot — the
                    # conflict-review-pending write above already ran and
                    # this does not repeat or undo it. A WIDENING change
                    # (e.g. private -> public) stays blocked like everything
                    # else on the observation; publishing more broadly is
                    # never done implicitly.
                    narrowing_outcome = _push_narrower_visibility_while_blocked(
                        client, cloud_id, push_payload, remote,
                    )
                    if narrowing_outcome in ('failed', 'lost_race'):
                        # Stage C review round 3, findings 1 & 2: neither a
                        # rejected narrowing PATCH nor a lost precondition
                        # race may be silently absorbed by a log line —
                        # record it via the SAME sync-error machinery every
                        # other push failure uses, without touching
                        # sync_status/sync_blocked_reason (the
                        # conflict-review-pending write above already set
                        # those and must stay authoritative). Cloud
                        # visibility is left exactly where it was; no
                        # baseline is stored recording the narrowing as if
                        # it had landed.
                        narrowing_detail = (
                            f"obs {obs_local_id_for_block}: privacy narrowing while "
                            f"blocked did not reach the cloud ({narrowing_outcome}); "
                            f"cloud visibility left unchanged, identification stays "
                            f"under review"
                        )
                        errors.append(narrowing_detail)
                        if obs_local_id_for_block > 0:
                            _set_observation_sync_error_detail_only(
                                obs_local_id_for_block, narrowing_detail,
                                error_code=f'privacy_narrowing_{narrowing_outcome}',
                            )
                    print(
                        f"[cloud_sync] conflict push blocked: obs={obs_local_id_for_block} "
                        f"categories=['taxon_identity_no_baseline_contradiction'] "
                        f"action=review_required",
                        flush=True,
                    )
                    _advance_progress(progress_state, 1)
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Observation {i + 1}/{max(1, total)} needs review before syncing",
                        ),
                        progress_state,
                    )
                    continue
                if no_baseline_no_claim_vs_remote_identity:
                    # Compatible partial information (blank, or matching):
                    # not a contradiction, so adopt the cloud's fields and
                    # identity onto local FIRST, through the existing
                    # adoption path (identity_fail_closed=True is safe here —
                    # local makes no claim of its own), exactly as the pull
                    # side already does for this same "no stored snapshot"
                    # shape. Without this, pushing the still-blank/partial
                    # local payload would PATCH the cloud's canonical name
                    # down to blank/partial, which is the known production
                    # shape (a bound concept with no local genus/species)
                    # going the wrong direction.
                    _apply_remote_observation_fields(
                        int(obs['id']), remote, identity_fail_closed=True,
                    )
                    adopted_obs = ObservationDB.get_observation(int(obs['id'])) or {}
                    push_payload.update(adopted_obs)

            # No baseline: an identity disagreement is a review, never an
            # RPC overwrite (the pull side applies the same rule).
            identity_review_pending = bool(
                cloud_id and remote and not stored_snapshot and _classify_identity_sync_change(
                    push_payload, remote, {}, identification_locally_owned=False,
                ) == 'conflict'
            )
            if identity_review_pending:
                _withhold_identity_from_push(push_payload)
                errors.append(_format_review_needed_error(
                    _safe_int(obs.get('id')), cloud_id,
                    [_format_observation_metadata_field_label(TAXON_IDENTITY_SYNC_FIELD)],
                ))
            merged_payload = _merge_cloud_selected_ai_fields(push_payload, remote)
            cloud_id = client.push_observation(
                merged_payload,
                remote_obs=remote,
                baseline_obs=identity_baseline_obs,
            )
            _adopt_merge_filled_ai_fields_locally(obs.get('id'), push_payload, merged_payload)

            # Update local record with cloud_id and sync_status
            previous_status = str(obs.get('sync_status') or '').strip().lower()
            conn2 = get_connection()
            cursor2 = conn2.cursor()
            update_observation_sync_state(
                cursor2,
                int(obs['id']),
                cloud_id=cloud_id,
                sync_status='synced',
                synced_at=datetime.now(timezone.utc).isoformat(),
                clear_sync_error_state=True,
            )
            conn2.commit()
            conn2.close()
            if identity_review_pending:
                # The other fields went out; the identity disagreement did
                # not, and must stay visible until the owner resolves it.
                _set_observation_sync_state(int(obs['id']), cloud_id, dirty=True, synced_at=None)
                _set_observation_conflict_review_pending(int(obs['id']))
            if previous_status == 'dirty':
                print(
                    f"[cloud_sync] sync_status transition obs {obs['id']}: dirty→synced "
                    f"caller=push_all",
                    flush=True,
                )

            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    obs,
                    f"Observation {i + 1}/{max(1, total)} synced",
                ),
                progress_state,
            )

            # Local observation id is used both inside the image-sync
            # branch and by the (independent) spore-summary sync below.
            # Hoist it so the summary path runs even when `sync_images`
            # is False.
            local_obs_id = _safe_int(obs.get('id'))
            if local_obs_id > 0 and bool(obs.get('portable_cloud_identity_pending')):
                _finalize_portable_cloud_identity_guard(client, local_obs_id)

            # (The metadata-only stored-signature refresh used to happen HERE,
            # right after stamping the observation synced. That was too eager:
            # it wiped the image-signature drift BEFORE the metadata-only
            # image PATCH branch below could detect it, so image tag edits
            # never reached cloud on a Refresh. Moved to run AFTER the
            # metadata-only PATCH branch — see the trailing refresh block
            # below the `if not sync_images ...` guard.)

            images_synced = True
            if sync_images:

                def _push_measurements_for_current_observation() -> None:
                    if local_obs_id <= 0:
                        return
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            f"Syncing measurements for observation {i + 1}/{max(1, total)}…",
                        ),
                        progress_state,
                    )
                    try:
                        _push_measurements_for_observation(client, local_obs_id)
                    except Exception as e:
                        if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                            raise
                        failure_msg = (
                            f'obs {obs["id"]}: measurement push failed: '
                            f'{type(e).__name__}: {e}'
                        )
                        print(f'[cloud_sync] {failure_msg}')
                        errors.append(failure_msg)
                        mark_observation_dirty(local_obs_id)
                    else:
                        print(
                            f'[cloud_sync] Observation {obs["id"]}: measurements pushed '
                            f'(local_id={local_obs_id})'
                        )
                    # Public spore mosaic (atlas + tile manifest).
                    #
                    # Runs unconditionally after the measurement attempt so a
                    # first-time mosaic still generates when measurements
                    # already exist in the cloud and this sync is a pure
                    # noop for measurements (all cache-hit). The mosaic
                    # pusher is best-effort: it queries the local DB itself,
                    # reads whichever measurement cloud_ids are already
                    # populated, and no-ops (with a `Mosaic skip …` log) if
                    # there aren't enough of them. A mosaic failure never
                    # aborts the observation sync — the public RPC falls
                    # back to per-spore `cropUrl` when no mosaic row exists.
                    try:
                        _push_spore_mosaic_for_observation(
                            client, local_obs_id, cloud_id,
                        )
                    except Exception as mosaic_exc:
                        if is_cloud_auth_error(mosaic_exc) or is_cloud_temporary_unavailable_error(mosaic_exc):
                            raise
                        print(
                            f'[cloud_sync] Mosaic push errored obs {local_obs_id}: '
                            f'{mosaic_exc}',
                            flush=True,
                        )


                # Upload completeness is NOT render-signature equality.
                #
                # The local media signature describes whether local *render
                # inputs* changed. It deliberately carries no cloud_id and no
                # storage intent, so it cannot prove that the cloud identity
                # or the bytes for the user's selected media actually exist.
                # An observation whose render inputs never changed but whose
                # selected images were never uploaded would take an image-prep
                # fast path forever (the stranded-media state). Establish
                # upload completeness separately, from the canonical per-image
                # storage-intent ledger plus cloud-link state, and let it veto
                # every image-prep fast path.
                #
                # Only for `sync_images=True` against an observation that
                # already exists in cloud. Refresh/background sync
                # (`sync_images=False`) never reaches this block.
                pending_cloud_image_ids: list[int] = []
                pending_image_uploads_complete = True
                if had_existing_cloud and local_obs_id > 0:
                    try:
                        # Intent seeding must run BEFORE desiredness is read:
                        # an unseeded ledger makes every row "uninitialized"
                        # (never pending), and an unseeded excluded set makes
                        # every row look desired. The initializer is
                        # incremental, idempotent and local-only; it is run
                        # again inside `_push_images_for_observation`.
                        # Seeding here is strictly conservative relative to
                        # that later call: the only rows it can decide
                        # differently are cloud-identified rows carrying an
                        # active tombstone, which are never pending anyway.
                        _ensure_cloud_image_storage_intent_initialized(local_obs_id)
                        pending_cloud_image_ids = _pending_cloud_pushable_image_ids(
                            local_obs_id
                        )
                    except Exception as exc:
                        # Fail closed. An unknown pending state must not buy a
                        # fast path; full image preparation is the pre-existing
                        # default whenever completeness cannot be established.
                        print(
                            f'[cloud_sync] Observation {obs["id"]}: could not establish '
                            f'cloud image upload completeness '
                            f'({type(exc).__name__}: {exc}); running full image prep',
                            flush=True,
                        )
                        pending_image_uploads_complete = False
                    else:
                        pending_image_uploads_complete = not pending_cloud_image_ids
                        if pending_cloud_image_ids:
                            print(
                                f'[cloud_sync] Observation {obs["id"]}: '
                                f'{len(pending_cloud_image_ids)} selected cloud image(s) '
                                f'still pending upload '
                                f'(image_ids={sorted(pending_cloud_image_ids)}); '
                                f'image-prep fast paths disabled',
                                flush=True,
                            )

                stored_local_media_signature = (
                    _load_local_cloud_media_signature(local_obs_id)
                    if had_existing_cloud and local_obs_id > 0
                    else ''
                )
                current_local_image_signature = ''
                if had_existing_cloud and local_obs_id > 0 and stored_local_media_signature:
                    current_local_image_signature = _local_cloud_image_media_signature(local_obs_id)
                image_render_unchanged = (
                    pending_image_uploads_complete
                    and had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_image_signature
                    and _local_media_signatures_match(
                        stored_local_media_signature,
                        current_local_image_signature,
                        include_measurements=False,
                    )
                )
                tombstone_cleanup_only = (
                    pending_image_uploads_complete
                    and not image_render_unchanged
                    and had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_image_signature
                    and _local_media_signatures_match_ignoring_tombstoned_images(
                        stored_local_media_signature,
                        current_local_image_signature,
                    )
                )
                prep_diagnostics = (
                    _local_media_prep_diagnostics(
                        local_obs_id,
                        stored_local_media_signature,
                        current_local_image_signature,
                    )
                    if had_existing_cloud
                    and local_obs_id > 0
                    and stored_local_media_signature
                    and current_local_image_signature
                    else None
                )
                metadata_only_image_sync = bool(
                    pending_image_uploads_complete
                    and prep_diagnostics
                    and not image_render_unchanged
                    and not tombstone_cleanup_only
                    and prep_diagnostics.get('only_metadata_fields_changed')
                    and not prep_diagnostics.get('has_local_image_cloud_id_null')
                )
                prep_decision = (
                    'skip prep'
                    if image_render_unchanged
                    else 'metadata-only image sync (tombstone_cleanup)'
                    if tombstone_cleanup_only
                    else 'metadata-only image sync'
                    if metadata_only_image_sync
                    else 'full image prep'
                )
                if prep_diagnostics is not None and (
                    _cloud_sync_debug_enabled() or metadata_only_image_sync
                ):
                    print(
                        (
                            f'[cloud_sync] Observation {obs["id"]}: image prep diagnostics '
                            f'image_render_signature_matched={prep_diagnostics["image_render_signature_matched"]} '
                            f'tombstone_aware_signature_matched={prep_diagnostics["tombstone_aware_signature_matched"]} '
                            f'measurement_only_matched={prep_diagnostics["measurement_only_matched"]} '
                            f'has_local_image_cloud_id_null={prep_diagnostics["has_local_image_cloud_id_null"]} '
                            f'image_file_signature_changed={prep_diagnostics["any_image_file_signature_changed"]} '
                            f'render_affecting_field_changed={prep_diagnostics["any_render_affecting_field_changed"]} '
                            f'only_metadata_fields_changed={prep_diagnostics["only_metadata_fields_changed"]} '
                            f'pending_cloud_image_uploads={len(pending_cloud_image_ids)} '
                            f'image_uploads_complete={pending_image_uploads_complete} '
                            f'decision={prep_decision} '
                            f'changed_keys={_format_local_media_prep_diagnostic_keys(prep_diagnostics["changed_keys"])}'
                        ),
                        flush=True,
                    )
                if image_render_unchanged:
                    measurement_only = bool(
                        stored_local_media_signature
                        and current_local_image_signature
                        and not _local_media_signatures_match(
                            stored_local_media_signature,
                            current_local_image_signature,
                        )
                    )
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Image/render media unchanged; skipping image prep for "
                                f"observation {i + 1}/{max(1, total)} "
                                f"(reason={'measurement_only' if measurement_only else 'unchanged'}, "
                                f"no prepared upload candidates)"
                            ),
                        ),
                        progress_state,
                    )
                    _push_measurements_for_current_observation()
                elif tombstone_cleanup_only:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Image/render media unchanged after tombstone cleanup; "
                                f"skipping image prep for observation {i + 1}/{max(1, total)} "
                                f"(reason=tombstone_only, metadata-only image sync)"
                            ),
                        ),
                        progress_state,
                    )
                    original_upload_warnings = []
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=None,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                        summary_warnings=original_upload_warnings,
                        original_summary=original_upload_summary,
                    )
                    if images_synced and local_obs_id > 0:
                        _push_measurements_for_current_observation()
                elif metadata_only_image_sync:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Image bytes unchanged; syncing image metadata only for "
                                f"observation {i + 1}/{max(1, total)} "
                                f"(reason=metadata_only, no prepared upload candidates)"
                            ),
                        ),
                        progress_state,
                    )
                    original_upload_warnings = []
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=None,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                        summary_warnings=original_upload_warnings,
                        original_summary=original_upload_summary,
                    )
                    if images_synced and local_obs_id > 0:
                        _push_measurements_for_current_observation()
                else:
                    original_upload_warnings: list[str] = []
                    images_synced = _push_images_for_observation(
                        client,
                        obs,
                        cloud_id,
                        prepare_images_cb=prepare_images_cb,
                        progress_cb=progress_cb,
                        progress_state=progress_state,
                        observation_index=i + 1,
                        observation_total=total,
                        summary_warnings=original_upload_warnings,
                        original_summary=original_upload_summary,
                    )
                    if images_synced and local_obs_id > 0:
                        _push_measurements_for_current_observation()
                if local_obs_id > 0:
                    if images_synced:
                        _refresh_local_cloud_media_signature(local_obs_id)
                    else:
                        mark_observation_dirty(local_obs_id)
                        if original_upload_warnings:
                            errors.append(
                                f"obs {obs['id']}: image push failures: "
                                + '; '.join(original_upload_warnings)
                            )

            # Metadata-only image PATCH pass under the fast Refresh mode
            # (`sync_images=False`). The `if sync_images:` block above gates
            # the byte-upload path AND the metadata PATCH path together —
            # but pure metadata edits on already-linked cloud images (e.g.
            # setting sample_source / mount_medium on a metadata-only
            # microscope anchor) should still reach cloud on a normal
            # Refresh, because they only require a PATCH, not a byte upload.
            #
            # Trigger policy: any observation that survived the dirty scan
            # AND has a cloud_id is a candidate. We PATCH each of its images
            # that already has a cloud_id. The push is idempotent — cloud
            # rows whose values already match get a no-op PATCH — so we
            # don't try to be clever about pre-filtering by signature. The
            # earlier signature-only gate silently skipped legitimate edits
            # whenever the stored signature was missing (e.g. after a manual
            # recovery step that cleared it).
            if not sync_images and had_existing_cloud and local_obs_id > 0:
                try:
                    local_images_for_patch = ImageDB.get_images_for_observation(local_obs_id)
                except Exception as fetch_exc:
                    print(
                        f"[cloud_sync] Metadata-only patch: could not read local "
                        f"images for obs {local_obs_id}: {fetch_exc}",
                        flush=True,
                    )
                    local_images_for_patch = []
                # Only metadata-only microscope anchors are safe to PATCH
                # under Refresh (`sync_images=False`). Field images require
                # byte-upload gating: PATCHing their metadata with an empty
                # local `storage_path` would either wipe the cloud key or,
                # for non-microscope rows, trip the RLS WITH CHECK
                # (`storage_path IS NULL AND image_type = 'microscope'`)
                # with a 42501 rejection. Media/storage edits on field
                # images remain gated behind `sync_images=True`.
                images_to_patch = [
                    {
                        **dict(img),
                        'portable_cloud_identity_pending': bool(
                            obs.get('portable_cloud_identity_pending')
                        ),
                    }
                    for img in local_images_for_patch
                    if _is_local_metadata_only_microscope_anchor(img)
                ]
                if images_to_patch:
                    print(
                        f'[cloud_sync] Observation {obs["id"]}: metadata-only '
                        f'image PATCH under Refresh (sync_images=False) '
                        f'image_ids={[img.get("id") for img in images_to_patch]}',
                        flush=True,
                    )
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            obs,
                            (
                                f"Patching image metadata for observation "
                                f"{i + 1}/{max(1, total)} "
                                f"(no bytes uploaded)"
                            ),
                        ),
                        progress_state,
                    )
                    images_metadata_synced = True
                    for local_img in images_to_patch:
                        try:
                            client.push_image_metadata(
                                local_img,
                                cloud_id,
                                str(local_img.get('storage_path') or ''),
                            )
                        except Exception as patch_exc:
                            if is_cloud_auth_error(patch_exc) or is_cloud_temporary_unavailable_error(patch_exc):
                                raise
                            images_metadata_synced = False
                            print(
                                f"[cloud_sync] Metadata-only PATCH failed for image "
                                f"{local_img.get('id')} (cloud "
                                f"{local_img.get('cloud_id')}): {patch_exc}",
                                flush=True,
                            )
                    if images_metadata_synced:
                        _refresh_local_cloud_media_signature(local_obs_id)
                    else:
                        mark_observation_dirty(local_obs_id)

            # Metadata-only stored-signature refresh: runs AFTER the PATCH
            # branch above (which needs to detect drift). Prevents the
            # dirty-loop from obs 368 by acknowledging any drift the fast
            # sync couldn't reconcile (e.g. sample_type canonicalization).
            if not sync_images and local_obs_id > 0:
                try:
                    _refresh_local_cloud_media_signature(local_obs_id)
                except Exception as sig_exc:
                    print(
                        f"[cloud_sync] Could not refresh local media signature for obs "
                        f"{local_obs_id}: {sig_exc}",
                        flush=True,
                    )

            # Structured observation-level spore summaries (Stage D).
            # Runs once per observation, outside the `sync_images` block,
            # because the summary is derived entirely from local
            # spore_measurements + local image context — it does not
            # require cloud image or cloud measurement rows to exist. In
            # particular this means summaries still sync when
            # `sync_images=False`, when image byte upload failed
            # (`images_synced=False`), or when only preparation-context
            # fields (mount_medium / stain / contrast / sample_type)
            # changed. Missing-table errors are treated as a
            # compatibility skip; unexpected errors surface via `errors`
            # so the user sees them in the sync result.
            if local_obs_id > 0 and cloud_id:
                _push_summary_for_current_observation(
                    client,
                    obs=obs,
                    local_obs_id=local_obs_id,
                    cloud_id=cloud_id,
                    errors=errors,
                )

            if identity_review_pending:
                # Baseline without identity: it stays "unknown", so the next
                # push preflight and pull classify the disagreement as a
                # conflict instead of a local change for the RPC to push.
                refreshed_remote = client.get_observation(cloud_id)
                if refreshed_remote:
                    _store_remote_snapshot(
                        client, cloud_id,
                        remote=_remote_row_without_identity(refreshed_remote),
                    )
            else:
                _store_remote_snapshot(client, cloud_id)

            pushed += 1
        except CloudSyncError as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            raw_error = f"obs {obs['id']}: {e}"
            if is_privacy_slot_limit_error(raw_error):
                _set_observation_privacy_blocked(int(obs['id']), raw_error)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} blocked: "
                            f"{privacy_slot_limit_user_message()}"
                        ),
                    ),
                    progress_state,
                )
            elif is_image_too_large_for_plan_error(raw_error):
                _set_observation_plan_image_retryable(int(obs['id']), raw_error)
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} needs retry: "
                            f"{summarize_image_too_large_for_plan_error(raw_error)}"
                        ),
                    ),
                    progress_state,
                )
            elif is_webp_support_required_for_cloud_media_upload_error(raw_error):
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        (
                            f"Observation {i + 1}/{max(1, total)} failed: "
                            f"{WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE}"
                        ),
                    ),
                    progress_state,
                )
            elif is_identity_clear_verification_failed_error(raw_error):
                # Stage C review round 2, item 3: the read-back after
                # set_observation_identification_v2 found the identity still
                # attached — a rate-limit row-suppression trigger (or
                # anything else that can silently cancel the row's own
                # UPDATE) may have dropped the clear while the RPC call
                # itself did not raise. Plain `mark_observation_dirty` is not
                # enough here: the ordinary genus/species half of the clear
                # already reached the cloud, so a same-cycle pull would
                # otherwise see local and remote agreeing on the NEW name and
                # silently reconverge/re-stamp this observation synced —
                # exactly the stale identity beside a new name Stage C
                # exists to prevent, reintroduced through a failed retry
                # instead of a rename. The conflict-review marker is the
                # existing signal pull_all's own convergence check honours
                # (see its `elif remote_changed:` branch) to leave this
                # observation blocked until a later push actually succeeds.
                _set_observation_conflict_review_pending(int(obs['id']))
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Observation {i + 1}/{max(1, total)} needs review before syncing",
                    ),
                    progress_state,
                )
            else:
                _emit_progress(
                    progress_cb,
                    _format_cloud_sync_observation_status(
                        obs,
                        f"Observation {i + 1}/{max(1, total)} failed",
                    ),
                    progress_state,
                )
                mark_observation_dirty(int(obs['id']))
            errors.append(raw_error)
            _advance_progress(progress_state, 1)

    # Backfill passes for observations skipped by the main dirty loop.
    #
    # Measurement reconciliation MUST run before summary reconciliation:
    # summaries count local measurements, and if the writer publishes
    # `n_paired = 29` for a species whose cloud raw table only has 20
    # measurements, the public spore RPC and the public summary RPC
    # disagree until the raw table catches up. Filling raw measurements
    # first keeps the two invariants consistent for the same species.
    #
    # Both repairs also run on the normal Refresh path. Measurement
    # reconciliation is a local candidate scan, while summary reconciliation
    # performs one bulk coverage read and only invokes the per-observation
    # writer for missing/stale context hashes. This keeps no-op Refresh cheap
    # without making historical sync gaps depend on a full pull.
    measurement_reconcile = None
    summary_reconcile = None
    profiler = _cloud_sync_current_profiler()
    try:
        with _cloud_sync_phase_scope(profiler, 'reconcile_missing_spore_measurements'):
            measurement_reconcile = _reconcile_missing_spore_measurements(
                client,
                errors,
                verify_stamped_remote=verify_stamped_measurements,
            )
    except Exception as reconcile_exc:
        if is_cloud_auth_error(reconcile_exc) or is_cloud_temporary_unavailable_error(reconcile_exc):
            raise
        errors.append(
            f'spore measurement reconciliation: unexpected error: {reconcile_exc}'
        )
        measurement_reconcile = None

    try:
        with _cloud_sync_phase_scope(profiler, 'reconcile_missing_spore_summaries'):
            summary_reconcile = _reconcile_missing_spore_summaries(client, errors)
    except Exception as reconcile_exc:
        if is_cloud_auth_error(reconcile_exc) or is_cloud_temporary_unavailable_error(reconcile_exc):
            raise
        errors.append(f'spore summary reconciliation: unexpected error: {reconcile_exc}')
        summary_reconcile = None

    result = {
        'pushed': pushed,
        'total': total,
        'calibrations_pushed': calibration_result.get('pushed', 0),
        'calibrations_total': calibration_result.get('total', 0),
        'errors': errors,
    }
    if summary_reconcile is not None:
        result['spore_summary_reconcile'] = summary_reconcile
    if measurement_reconcile is not None:
        result['spore_measurement_reconcile'] = measurement_reconcile
    if format_original_upload_summary(original_upload_summary):
        result['original_sync'] = original_upload_summary
    sync_summary = _cloud_sync_current_summary()
    if sync_summary is not None:
        result['sync_summary'] = dict(sync_summary)
    return result


def _reconcile_missing_spore_summaries(
    client: SporelyCloudClient,
    errors: list[str],
) -> dict[str, int]:
    """Backfill / reconcile `public.observation_spore_summaries` for
    every observation that has been synced at least once and has local
    spore measurements.

    The pass compares the complete set of local and remote context hashes.
    This catches both wholly missing rows and partial coverage (for example,
    KOH present but water missing) without routing every fully-covered
    observation through another network request.

    Metadata-only microscope anchors (`observation_images.storage_path`
    NULL) are covered because the local join key is `image_id`, not
    `storage_path`. Summaries are computed from local measurements plus
    local image context; raw microscope image bytes are irrelevant.

    The main ``push_all`` loop scans
    ``WHERE cloud_id IS NULL OR sync_status = 'dirty'`` and only that
    subset runs the writer inline. This pass exists specifically to
    catch observations that were synced before Stage D landed (or
    otherwise did not go dirty this session). Steady-state network cost is
    one bulk GET, independent of the number of measured observations.

    Returns a small counter dict for the caller's summary/log stanza.
    """
    reconcile_start = _cloud_sync_perf_counter()
    counters = {'candidates': 0, 'attempted': 0}
    print('[cloud_sync] spore summary reconciliation: start', flush=True)

    # Fetch all remote coverage in one request. A missing-table response
    # means this deployment predates Stage B; other errors follow the
    # surrounding sync error policy.
    remote_fetch_start = _cloud_sync_perf_counter()
    try:
        remote_rows = client._get(
            f'observation_spore_summaries'
            f'?user_id=eq.{client.user_id}'
            f'&select=observation_id,context_hash'
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        if _is_summary_table_missing_error(exc):
            print(
                '[cloud_sync] Spore summary reconciliation: cloud table missing '
                '(older deployment); skipping backfill pass.',
                flush=True,
            )
            print(
                f'[cloud_sync] spore summary reconciliation: complete '
                f'candidates=0 attempted=0 duration='
                f'{(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
                flush=True,
            )
            return counters
        # Unknown non-auth error — record it and stop. A per-observation
        # retry via the main dirty loop still works for the individual
        # case.
        errors.append(f'spore summary reconciliation: probe failed: {exc}')
        print(
            f'[cloud_sync] spore summary reconciliation: complete '
            f'candidates=0 attempted=0 probe_error=True duration='
            f'{(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
            flush=True,
        )
        return counters
    print(
        f'[cloud_sync] spore summary reconciliation: remote coverage fetch complete '
        f'rows={len(remote_rows or [])} '
        f'duration={(_cloud_sync_perf_counter() - remote_fetch_start) * 1000:.0f}ms',
        flush=True,
    )

    local_query_start = _cloud_sync_perf_counter()
    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT DISTINCT o.id AS local_id, o.cloud_id AS cloud_id
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            JOIN spore_measurements m ON m.image_id = i.id
            WHERE o.cloud_id IS NOT NULL AND trim(o.cloud_id) != ''
            ORDER BY o.id
            '''
        )
        candidates = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
    print(
        f'[cloud_sync] spore summary reconciliation: local candidate scan complete '
        f'observations={len(candidates)} '
        f'duration={(_cloud_sync_perf_counter() - local_query_start) * 1000:.0f}ms',
        flush=True,
    )

    counters['candidates'] = len(candidates)
    if not candidates:
        print(
            f'[cloud_sync] spore summary reconciliation: complete '
            f'candidates=0 attempted=0 duration='
            f'{(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
            flush=True,
        )
        return counters

    remote_hashes_by_observation: dict[str, set[str]] = {}
    for remote_row in remote_rows or []:
        remote_observation_id = str(remote_row.get('observation_id') or '').strip()
        context_hash = str(remote_row.get('context_hash') or '').strip()
        if remote_observation_id and context_hash:
            remote_hashes_by_observation.setdefault(remote_observation_id, set()).add(context_hash)

    valid_candidates: list[tuple[int, str]] = []
    from utils import spore_summary_sync as spore_summary_sync_module
    from utils.spore_summary import compute_observation_spore_summaries

    hash_build_start = _cloud_sync_perf_counter()
    for row in candidates:
        cloud_id = str(row.get('cloud_id') or '').strip()
        if not cloud_id:
            continue
        try:
            local_id = int(row.get('local_id') or 0)
        except (TypeError, ValueError):
            local_id = 0
        if local_id <= 0:
            continue
        measurements = spore_summary_sync_module.load_measurements_with_context(local_id)
        local_hashes = {
            str(summary.get('context_hash') or '').strip()
            for summary in compute_observation_spore_summaries(
                observation_id=local_id,
                measurements=measurements,
            )
            if str(summary.get('context_hash') or '').strip()
        }
        if local_hashes != remote_hashes_by_observation.get(cloud_id, set()):
            valid_candidates.append((local_id, cloud_id))
    print(
        f'[cloud_sync] spore summary reconciliation: local hash comparison complete '
        f'observations={len(candidates)} mismatched={len(valid_candidates)} '
        f'duration={(_cloud_sync_perf_counter() - hash_build_start) * 1000:.0f}ms',
        flush=True,
    )

    if not valid_candidates:
        print(
            f'[cloud_sync] spore summary reconciliation: complete '
            f'candidates={counters["candidates"]} attempted=0 duration='
            f'{(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
            flush=True,
        )
        return counters

    print(
        f'[cloud_sync] Spore summary reconciliation: attempting '
        f'{len(valid_candidates)} synced observations '
        f'with missing or stale summary contexts.',
        flush=True,
    )

    for local_id, cloud_id in valid_candidates:
        counters['attempted'] += 1
        _push_summary_for_current_observation(
            client,
            obs={'id': local_id},
            local_obs_id=local_id,
            cloud_id=cloud_id,
            errors=errors,
            mark_dirty_on_error=False,
        )
    print(
        f'[cloud_sync] spore summary reconciliation: complete '
        f'candidates={counters["candidates"]} attempted={counters["attempted"]} '
        f'duration={(_cloud_sync_perf_counter() - reconcile_start) * 1000:.0f}ms',
        flush=True,
    )
    return counters


def _push_summary_for_current_observation(
    client: SporelyCloudClient,
    *,
    obs: dict,
    local_obs_id: int,
    cloud_id: str,
    errors: list[str],
    mark_dirty_on_error: bool = True,
) -> dict | None:
    """Sync structured spore summaries for one observation (Stage D).

    ``mark_dirty_on_error`` is True for the per-observation push loop (a
    failed summary must leave the observation dirty for retry) and False for
    the summary backfill pass (D5: backfill failures are reported via
    ``errors`` and the log, not by re-dirtying the observation).

    Returns the sync helper's result dict, or ``None`` if the summary
    call raised. Missing-table errors (older cloud deployments without
    ``public.observation_spore_summaries``) are logged and treated as a
    soft skip — they do not add to ``errors``. Auth / temporary-
    unavailable errors re-raise so the outer sync loop can abort. Any
    other unexpected error is appended to ``errors`` using the same
    ``"obs <local_id>: ..."`` format as the surrounding
    ``CloudSyncError`` reporting, so the caller and the returned
    ``sync_all`` result surface the failure instead of hiding it in a
    print statement.
    """
    try:
        summary_result = sync_observation_spore_summaries(
            client,
            local_observation_id=local_obs_id,
            remote_observation_id=cloud_id,
            user_id=client.user_id,
            source_app_version=_current_source_app_version(),
        )
    except Exception as summary_exc:
        if is_cloud_auth_error(summary_exc) or is_cloud_temporary_unavailable_error(summary_exc):
            raise
        errors.append(
            f"obs {obs.get('id')}: spore summary sync failed: {summary_exc}"
        )
        if mark_dirty_on_error:
            try:
                mark_observation_dirty(int(obs.get('id') or 0))
            except Exception as dirty_exc:
                print(
                    f'[cloud_sync] Could not mark obs {local_obs_id} dirty '
                    f'after spore summary failure: {dirty_exc}',
                    flush=True,
                )
        print(
            f'[cloud_sync] Spore summary push errored obs '
            f'{local_obs_id}: {summary_exc}',
            flush=True,
        )
        return None

    status = summary_result.get('status')
    if status == SUMMARY_STATUS_SKIP_TABLE_MISSING:
        # Compatibility skip: older cloud deployment; recorded but not
        # a user-visible error.
        print(
            f'[cloud_sync] Spore summary push skipped obs '
            f'{local_obs_id}: cloud table missing '
            f'(older deployment); continuing sync.',
            flush=True,
        )
    elif status == SUMMARY_STATUS_SKIP_NO_CLOUD_ID:
        # Defensive; the caller only invokes this helper when cloud_id
        # is set, so this branch is unusual — flag it to `errors` so it
        # is visible if it ever happens.
        errors.append(
            f"obs {obs.get('id')}: spore summary sync skipped (no cloud id available)"
        )
        print(
            f'[cloud_sync] Spore summary push skipped obs '
            f'{local_obs_id}: no cloud id available yet.',
            flush=True,
        )
    elif status == SUMMARY_STATUS_SYNCED and (
        summary_result.get('inserted')
        or summary_result.get('updated')
        or summary_result.get('deleted')
    ):
        # Only log when something actually changed. Fully-idempotent
        # syncs (unchanged > 0 with zero writes) stay quiet so the log
        # is a signal of real cloud mutation, not sync heartbeat.
        print(
            f'[cloud_sync] Spore summaries synced obs '
            f'{local_obs_id}: '
            f'inserted={summary_result.get("inserted", 0)} '
            f'updated={summary_result.get("updated", 0)} '
            f'unchanged={summary_result.get("unchanged", 0)} '
            f'deleted={summary_result.get("deleted", 0)} '
            f'total_local={summary_result.get("total_local", 0)}',
            flush=True,
        )
    return summary_result


# ── Local-only mosaic signature ─────────────────────────────────────────────
#
# The signature is a stable SHA-1 hex string over the tuple of local inputs
# that determine both the rendered mosaic bytes AND the remote tile manifest.
# Normal sync compares it to `observations.mosaic_signature` (local column,
# never synced to cloud) and skips the expensive render/upload/tile-rewrite
# when nothing has changed and a remote mosaic row still exists.
#
# Included in the signature:
#   * observation:  spore_data_visibility, MOSAIC_PIPELINE_VERSION
#   * per measurement (sorted by local id):
#       id, cloud_id, image_id, image_cloud_id,
#       p1..p4 (x,y), length_um, width_um, measurement_type, gallery_rotation
#   * per source image referenced by those measurements:
#       image_id, image_cloud_id, resolved source path (str),
#       file mtime_ns, file size_bytes,
#       scale_microns_per_pixel, resample_scale_factor
#
# Design notes / anti-footguns:
#   * (mtime_ns, size_bytes) alone is not enough — a different file with the
#     same size and mtime would look identical. The signature always pairs
#     the file fingerprint with the resolved path and local/cloud image id.
#   * Numbers, empty strings and Nones are normalised so trivially different
#     text (`""` vs `None`) doesn't produce a different digest.
#   * SHA-1 is used because we're not authenticating anything — this is a
#     cheap change-detector for a local cache; collisions here would just
#     mean "one skipped rebuild we should have done", which is bounded by
#     the remote-mosaic-row presence check.


def pull_all(
    client: SporelyCloudClient,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_obs: list[dict] | None = None,
    sync_calibrations: bool = True,
    materialize_remote_images: bool = True,
    sync_images: bool = True,
    full_pull: bool = True,
    pull_only: bool = False,
    forced_pull_cloud_ids: frozenset[str] = frozenset(),
) -> dict:
    """Pull new cloud observations and apply remote updates to clean local rows.

    ``full_pull=False`` enables the fast path: candidates are pruned to just
    the observations whose remote ``updated_at`` is newer than local
    ``synced_at`` (or whose local row / snapshot is missing). Bulk image and
    measurement fetches only run when there is at least one active candidate,
    and they only fetch the active cloud IDs — not all 1500+ images in the
    account. If no observation actually changed remotely, the pull returns
    early after logging a `no-op fast path` line.
    """
    # Pull preflight: EXIF backfill, candidate build, and the bulk image /
    # measurement fetches all run before the first per-observation progress
    # update. On a no-change sync these were part of the silent gap that left the
    # UI on the last calibration label, so emit messages and time each sub-step.
    pull_preflight_start = _cloud_sync_perf_counter()
    progress_state = progress_state if isinstance(progress_state, dict) else {}
    # A PullOnlyCloudClient wrapper carries its own is_pull_only marker;
    # honour it even if the caller forgot to pass pull_only=True.
    pull_only = bool(pull_only or getattr(client, 'is_pull_only', False))
    _set_progress_phase(progress_state, 'pull_preflight', phase_total=3)
    _emit_progress(progress_cb, "Preparing cloud observations…", progress_state)

    _emit_progress(progress_cb, "Checking local metadata cache…", progress_state)
    exif_start = _cloud_sync_perf_counter()
    # EXIF backfill PATCHes cloud rows to fill missing camera metadata; it is a
    # pull-side cloud write and must not run in Download-from-Cloud mode.
    exif_counts = {} if pull_only else (_backfill_missing_exif_on_cloud_images() or {})
    exif_elapsed = _cloud_sync_perf_counter() - exif_start
    print(
        f"[cloud_sync] pull preflight: exif backfill complete "
        f"scanned={exif_counts.get('scanned', 0)} "
        f"skipped_cached={exif_counts.get('skipped_cached', 0)} "
        f"opened={exif_counts.get('opened', 0)} "
        f"updated={exif_counts.get('updated', 0)} "
        f"duration={exif_elapsed * 1000:.0f}ms",
        flush=True,
    )

    # EXIF backfill step complete.
    _advance_progress(progress_state, 1)

    remote_obs = list(remote_obs or client.list_remote_observations())
    calibration_result = {'pulled': 0, 'total': 0, 'errors': []}
    if sync_calibrations:
        # A standalone pull_all(sync_calibrations=True) uses the calibration_pull
        # phase for the calibration work, then returns here to keep going with
        # the pull-side observation flow.
        _set_progress_phase(progress_state, 'calibration_pull')
        calibration_result = pull_calibrations(
            client,
            progress_cb=progress_cb,
            progress_state=progress_state,
        )
        _set_progress_phase(progress_state, 'pull_preflight', phase_total=3)
        _advance_progress(progress_state, 1)
    pulled = 0
    errors = list(calibration_result.get('errors') or [])
    imported_local_ids: list[int] = []
    candidate_start = _cloud_sync_perf_counter()
    local_by_cloud_id, local_by_id = _load_local_observation_lookup()
    candidates: list[tuple[dict, dict | None, str]] = []
    candidate_cloud_ids: list[str] = []
    for remote in remote_obs:
        cloud_id = str(remote.get('id') or '').strip()
        local_obs = _find_local_observation_for_remote_cached(remote, local_by_cloud_id, local_by_id)
        stored_snapshot = _load_cloud_observation_snapshot(cloud_id) if cloud_id else ''
        candidates.append((remote, local_obs, stored_snapshot))
        if cloud_id:
            candidate_cloud_ids.append(cloud_id)

    total = len(candidates)

    # Fast-path pruning + convergence.
    #
    # A no-op Refresh with 220 synced observations was still fetching bulk
    # image + measurement metadata every run because 119 of them had a remote
    # `updated_at` newer than local `synced_at`. My earlier prune passed them
    # through to the reconciliation loop, but the reconciliation loop found
    # nothing to do (snapshot still matched) and did NOT bump `synced_at`, so
    # the same 119 kept surviving prune forever.
    #
    # Two changes below:
    #  1. Reason-annotated prune: for each candidate, record exactly WHY it
    #     survived (missing_snapshot / remote_newer / no_local / missing_ts /
    #     parse_error).
    #  2. Cheap observation-only equality check for the "remote_newer" and
    #     "missing_snapshot" reasons: if the observation fields already match
    #     the stored snapshot, stamp synced (converging next run) and refresh
    #     the snapshot with the fresh remote row — no image / measurement
    #     bulk fetch required.
    fast_path_skipped_ids: list[str] = []
    fast_path_reason_counts: dict[str, int] = {}
    fast_path_reason_samples: list[str] = []
    fast_path_converged_ids: list[str] = []

    def _record_reason(reason: str, cloud_id: str, remote_updated, local_synced, local_id) -> None:
        fast_path_reason_counts[reason] = fast_path_reason_counts.get(reason, 0) + 1
        if len(fast_path_reason_samples) < 20:
            fast_path_reason_samples.append(
                f"cloud={cloud_id or '?'} local={local_id or '?'} reason={reason} "
                f"remote_updated_at={remote_updated} local_synced_at={local_synced}"
            )

    if not full_pull:
        pruned: list[tuple[dict, dict | None, str]] = []
        pruned_cloud_ids: list[str] = []
        for remote, local_obs, stored_snapshot in candidates:
            cloud_id = str(remote.get('id') or '').strip()
            local_id = _safe_int((local_obs or {}).get('id')) if local_obs else None
            remote_updated_raw = remote.get('updated_at')
            local_synced_raw = (local_obs or {}).get('synced_at')

            if cloud_id and cloud_id in forced_pull_cloud_ids:
                _record_reason('child_changed', cloud_id, remote_updated_raw, local_synced_raw, local_id)
                pruned.append((remote, local_obs, stored_snapshot))
                if cloud_id:
                    pruned_cloud_ids.append(cloud_id)
                continue

            if local_obs is None:
                _record_reason('no_local', cloud_id, remote_updated_raw, local_synced_raw, local_id)
                pruned.append((remote, local_obs, stored_snapshot))
                if cloud_id:
                    pruned_cloud_ids.append(cloud_id)
                continue

            remote_updated = _parse_sync_timestamp(remote_updated_raw)
            local_synced = _parse_sync_timestamp(local_synced_raw)

            if not stored_snapshot:
                # Cheap convergence: if the remote observation fields already
                # match a synthesized empty baseline, we still need to seed a
                # snapshot but we can skip the bulk image/measurement fetch
                # when the local row has no images we haven't already pulled.
                # For safety this branch remains a candidate — first-time
                # snapshot capture is a real event.
                _record_reason('missing_snapshot', cloud_id, remote_updated_raw, local_synced_raw, local_id)
                pruned.append((remote, local_obs, stored_snapshot))
                if cloud_id:
                    pruned_cloud_ids.append(cloud_id)
                continue

            if remote_updated is None or local_synced is None:
                _record_reason('missing_ts', cloud_id, remote_updated_raw, local_synced_raw, local_id)
                pruned.append((remote, local_obs, stored_snapshot))
                if cloud_id:
                    pruned_cloud_ids.append(cloud_id)
                continue

            if remote_updated > local_synced:
                # Cheap convergence: server bumped updated_at but the
                # observation-level fields may still match the stored
                # snapshot. If they do, stamp synced_at + refresh the
                # snapshot right here so the next fast pull skips this row
                # without a bulk image/measurement fetch.
                snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                snapshot_obs = snapshot_data.get('observation') or {}
                baseline_payload = _baseline_observation_compare_payload(snapshot_obs)
                remote_payload = _observation_compare_payload(remote, local=False)
                observation_fields_match = all(
                    _observation_field_values_match(
                        field,
                        remote_payload.get(field),
                        baseline_payload.get(field),
                    )
                    for field in _SNAPSHOT_OBS_FIELDS
                    if field not in {'id', 'desktop_id'}
                ) and not _remote_identity_changed_since(remote, snapshot_obs)
                local_status = str((local_obs or {}).get('sync_status') or '').strip().lower()
                if observation_fields_match and local_status != 'dirty':
                    # Converge without deep fetch: stamp synced + refresh
                    # snapshot (preserving previous images/measurements).
                    if cloud_id and local_id and local_id > 0:
                        _stamp_observation_synced(local_id, cloud_id)
                        try:
                            snapshot_images = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
                            snapshot_measurements = [dict(row or {}) for row in (snapshot_data.get('measurements') or [])]
                            _store_remote_snapshot(
                                client,
                                cloud_id,
                                remote=remote,
                                remote_images=snapshot_images,
                                remote_measurements=snapshot_measurements,
                            )
                        except Exception as snap_exc:
                            print(
                                f"[cloud_sync] fast pull: could not refresh snapshot for obs "
                                f"{local_id}: {snap_exc}",
                                flush=True,
                            )
                    fast_path_converged_ids.append(cloud_id or '?')
                    continue

                _record_reason('remote_newer', cloud_id, remote_updated_raw, local_synced_raw, local_id)
                pruned.append((remote, local_obs, stored_snapshot))
                if cloud_id:
                    pruned_cloud_ids.append(cloud_id)
                continue

            fast_path_skipped_ids.append(cloud_id or '?')
        candidates = pruned
        candidate_cloud_ids = pruned_cloud_ids
        total = len(candidates)

    candidate_elapsed = _cloud_sync_perf_counter() - candidate_start
    print(
        f"[cloud_sync] pull preflight: candidate build complete "
        f"count={total} duration={candidate_elapsed * 1000:.0f}ms "
        f"full_pull={full_pull} skipped_unchanged={len(fast_path_skipped_ids)} "
        f"fast_path_converged={len(fast_path_converged_ids)}",
        flush=True,
    )
    if not full_pull and fast_path_reason_counts:
        reasons_str = ", ".join(f"{k}={v}" for k, v in sorted(fast_path_reason_counts.items()))
        print(
            f"[cloud_sync] fast pull candidate reasons: {reasons_str}",
            flush=True,
        )
        if _cloud_sync_debug_enabled():
            for sample in fast_path_reason_samples:
                print(f"[cloud_sync]   {sample}", flush=True)
    # Candidate-build step complete.
    _advance_progress(progress_state, 1)

    # Fast-path: if nothing needs pulling, short-circuit the bulk fetches.
    if not full_pull and total == 0:
        print(
            f"[cloud_sync] no-op fast path: local_dirty=(see push) "
            f"remote_changed=0 full_pull=False pull_candidates=0 "
            f"skipped_unchanged={len(fast_path_skipped_ids)}",
            flush=True,
        )
        return {
            'pulled': 0,
            'imported_local_ids': [],
            'errors': errors,
            'calibrations_pulled': calibration_result.get('pulled', 0),
            'calibrations_total': calibration_result.get('total', 0),
            'fast_path_used': True,
            'skipped_unchanged': len(fast_path_skipped_ids),
        }

    if total:
        _emit_progress(progress_cb, "Loading cloud image metadata…", progress_state)
    bulk_start = _cloud_sync_perf_counter()
    bulk_fetcher = getattr(client, 'pull_bulk_image_metadata', None)
    if callable(bulk_fetcher):
        bulk_images = [dict(row or {}) for row in (bulk_fetcher(candidate_cloud_ids) or [])]
    else:
        bulk_images = []
        for cloud_id in candidate_cloud_ids:
            bulk_images.extend(
                dict(row or {})
                for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])
            )
    bulk_elapsed = _cloud_sync_perf_counter() - bulk_start
    print(
        f"[cloud_sync] pull preflight: cloud image metadata fetched "
        f"images={len(bulk_images)} duration={bulk_elapsed * 1000:.0f}ms",
        flush=True,
    )
    remote_images_by_obs = {}
    for img in bulk_images:
        obs_id = str(img.get('observation_id') or '').strip()
        if obs_id:
            remote_images_by_obs.setdefault(obs_id, []).append(img)
    # Bulk image metadata fetch complete.
    _advance_progress(progress_state, 1)
    _set_progress_phase(progress_state, 'pull_measurements', phase_total=1)
    if total:
        _emit_progress(progress_cb, "Loading cloud measurements…", progress_state)
    measurements_start = _cloud_sync_perf_counter()
    remote_measurements = _pull_remote_measurements_for_images(
        client,
        [str(row.get('id') or '').strip() for row in bulk_images if str(row.get('id') or '').strip()],
    )
    remote_measurements_by_obs = _group_remote_measurements_by_observation(
        bulk_images,
        remote_measurements,
    )
    measurements_elapsed = _cloud_sync_perf_counter() - measurements_start
    print(
        f"[cloud_sync] pull preflight: remote measurements fetched "
        f"count={len(remote_measurements or [])} duration={measurements_elapsed * 1000:.0f}ms",
        flush=True,
    )
    _advance_progress(progress_state, 1)
    print(
        f"[cloud_sync] pull preflight: complete candidates={total} "
        f"duration={(_cloud_sync_perf_counter() - pull_preflight_start) * 1000:.0f}ms",
        flush=True,
    )

    _set_progress_phase(progress_state, 'pull_observations', phase_total=total)
    for i, (remote, local_obs, stored_snapshot) in enumerate(candidates):
        cloud_id = str(remote.get('id') or '').strip()
        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                remote,
                f"Checking cloud observation {i + 1}/{max(1, total)}…",
            ),
            progress_state,
        )

        try:
            # DO NOT filter by should_pull_cloud_image_to_desktop here, otherwise the 
            # conflict logic falsely thinks microscope images were deleted by the cloud!
            remote_images = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])]
            remote_measurements = [
                dict(row or {})
                for row in remote_measurements_by_obs.get(cloud_id, [])
            ]

            if local_obs is None:
                local_id = _create_local_from_remote(
                    remote,
                    progress_cb=progress_cb,
                    progress_state=progress_state,
                    remote_index=i + 1,
                    remote_total=total,
                    remote_images=remote_images,
                    client=client,
                    remote_measurements=remote_measurements,
                    materialize_remote_images=materialize_remote_images,
                )
                if cloud_id and not pull_only:
                    client.set_desktop_id(cloud_id, local_id)
                _refresh_local_cloud_media_signature(local_id)
                pulled += 1
                imported_local_ids.append(int(local_id))
            else:
                local_id = int(local_obs['id'])
                portable_identity_pending = bool(
                    local_obs.get('portable_cloud_identity_pending')
                )
                if (
                    cloud_id
                    and int(remote.get('desktop_id') or 0) != local_id
                    and not pull_only
                    and not portable_identity_pending
                ):
                    try:
                        client.set_desktop_id(cloud_id, local_id)
                    except Exception as exc:
                        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                            raise
                local_dirty = str(local_obs.get('sync_status') or '').strip().lower() == 'dirty'
                if local_dirty and cloud_id and _clear_observation_dirty_if_no_real_changes(local_id, cloud_id):
                    local_obs = ObservationDB.get_observation(local_id) or local_obs
                    local_dirty = False
                remote_images_raw = [dict(row or {}) for row in remote_images_by_obs.get(cloud_id, [])] if cloud_id else []
                _record_remote_image_tombstones(
                    remote_images_raw,
                    local_observation_id=local_id,
                    cloud_observation_id=cloud_id,
                )
                snapshot_data = _parse_cloud_observation_snapshot(stored_snapshot)
                baseline_images = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
                tombstoned_remote_image_keys = (
                    _deleted_remote_image_identity_keys(remote_images_raw)
                    | _locally_tombstoned_snapshot_image_identity_keys(baseline_images)
                )
                remote_images = [
                    dict(row or {})
                    for row in remote_images_raw
                    if not str(row.get('deleted_at') or '').strip() and should_pull_cloud_image_to_desktop(row)
                ]
                remote_changed = (not stored_snapshot) or _remote_snapshot_has_meaningful_changes(
                    remote,
                    remote_images,
                    remote_measurements,
                    stored_snapshot,
                )
                should_store_snapshot = True
                store_full_snapshot = True
                local_media_changed = False
                # The row the snapshot is taken from. An identity disagreement
                # with no baseline stores it WITHOUT the identity, so the
                # baseline stays "unknown" and every later pull and push
                # classifies the same disagreement as a conflict.
                snapshot_remote = remote
                # Sync-integrity follow-up 4, Case F (pull side): with no
                # stored baseline, `_apply_remote_observation_fields` below
                # applies ALL ordinary fields unconditionally — its
                # `identity_fail_closed` guard only withholds the identity
                # columns, for a local row that already CLAIMS its own
                # identity. A local row with NO identity claim at all, whose
                # own committed genus/species already contradict a cloud row
                # that DOES carry a bound identity, is a different case: the
                # contradiction is the identification itself, so silently
                # overwriting it (as "cloud wins, nothing local to protect")
                # would adopt the cloud's bound concept next to names that
                # never agreed with it. Detected the same way as the mirrored
                # push-side guard: block this observation's whole pull apply
                # (fields, images, measurements) and surface it through the
                # existing conflict-review mechanism instead.
                remote_claim_for_pull_contradiction = (
                    _remote_identity_claim(remote) if remote_changed and not stored_snapshot else None
                )
                pull_contradicts_bound_identity = bool(
                    remote_claim_for_pull_contradiction is not None
                    and remote_claim_for_pull_contradiction.key
                    and not _local_identity_is_claim(local_obs)
                    and _identification_contradicts_remote(local_obs, dict(remote or {}))
                )
                if pull_contradicts_bound_identity:
                    errors.append(_format_review_needed_error(
                        local_id, cloud_id,
                        ['pull_blocked', _format_observation_metadata_field_label(TAXON_IDENTITY_SYNC_FIELD)],
                    ))
                    # `_set_observation_conflict_review_pending` already marks
                    # the row dirty; a separate `_set_observation_sync_state`
                    # call here would clear the marker right back out via its
                    # `clear_sync_error_state=True`.
                    _set_observation_conflict_review_pending(local_id)
                    should_store_snapshot = False
                elif remote_changed and not stored_snapshot:
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Applying cloud changes to local observation {local_id}…",
                        ),
                        progress_state,
                    )
                    # No baseline: nothing says which side changed, so a local
                    # identity claim that differs from the cloud is a review,
                    # never an overwrite (identity-in-change-detection rule 2).
                    identity_outcome = _apply_remote_observation_fields(
                        local_id, remote, identity_fail_closed=True,
                    )
                    identity_conflict = identity_outcome == IDENTITY_APPLY_CONFLICT
                    if identity_conflict:
                        errors.append(_format_review_needed_error(
                            local_id, cloud_id,
                            [_format_observation_metadata_field_label(TAXON_IDENTITY_SYNC_FIELD)],
                        ))
                        snapshot_remote = _remote_row_without_identity(remote)
                    warnings = _apply_remote_images_to_local(
                        client,
                        local_id,
                        remote_images,
                        allow_delete=False,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(warnings)
                    measurement_result = _import_remote_measurements_for_observation(
                        client,
                        local_id,
                        cloud_id,
                        remote_images,
                        remote_measurements,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(measurement_result.get('warnings') or [])
                    remote_media_pending = bool(
                        _remote_images_missing_locally(local_id, remote_images)
                        or measurement_result.get('skipped_materialization')
                        or measurement_result.get('failed')
                    )
                    materialization_failed = bool(materialize_remote_images and remote_media_pending)
                    if measurement_result.get('conflict') or materialization_failed or identity_conflict:
                        _set_observation_sync_state(local_id, cloud_id, dirty=True, synced_at=None)
                    else:
                        _stamp_observation_synced(local_id, cloud_id)
                    if not remote_media_pending:
                        _refresh_local_cloud_media_signature(local_id)
                    store_full_snapshot = not remote_media_pending and not bool(measurement_result.get('conflict'))
                    pulled += 1
                elif remote_changed:
                    baseline_obs = _baseline_observation_compare_payload(
                        snapshot_data.get('observation') or {}
                    )
                    field_changes = _analyze_observation_field_changes(local_obs, remote, baseline_obs)
                    remote_image_payloads = [_remote_image_payload(img) for img in remote_images]
                    remote_image_changes = _analyze_image_changes(
                        remote_image_payloads,
                        baseline_images,
                        ignored_keys=tombstoned_remote_image_keys,
                    )
                    remote_raw_map = {_image_compare_key(row): row for row in remote_images}
                    stored_local_media_signature = _load_local_cloud_media_signature(local_id)
                    current_local_media_signature = _local_cloud_media_signature(local_id)
                    local_media_changed = bool(
                        stored_local_media_signature
                        and current_local_media_signature
                        and not _local_media_signatures_match(
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    )
                    if not local_media_changed:
                        _store_local_media_signature_if_equivalent(
                            local_id,
                            stored_local_media_signature,
                            current_local_media_signature,
                        )
                    if remote_image_changes.get('removed_keys'):
                        errors.append(
                            _format_review_needed_error(
                                local_id,
                                cloud_id,
                                ['cloud removed local image files'],
                            )
                        )
                        should_store_snapshot = False
                        continue
                    _emit_progress(
                        progress_cb,
                        _format_cloud_sync_observation_status(
                            remote,
                            f"Applying cloud changes to local observation {local_id}…",
                        ),
                        progress_state,
                    )
                    remote_only_fields = {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('remote_only_fields') or [])
                    }
                    conflict_fields = {
                        _normalize_observation_sync_field(field)
                        for field in (field_changes.get('conflict_fields') or [])
                    }
                    if remote_only_fields:
                        _apply_remote_observation_fields(
                            local_id,
                            remote,
                            fields=remote_only_fields,
                        )
                    if conflict_fields:
                        errors.append(
                            _format_review_needed_error(
                                local_id,
                                cloud_id,
                                [
                                    ', '.join(
                                        _format_observation_metadata_field_label(field)
                                        for field in sorted(conflict_fields)
                                    )
                                ],
                            )
                        )

                    if remote_image_changes.get('changed'):
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            remote_images,
                            allow_delete=False,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(warnings)
                    elif remote_image_changes.get('added_keys'):
                        added_remote_images = [
                            remote_raw_map[key]
                            for key in remote_image_changes.get('added_keys') or []
                            if key in remote_raw_map
                        ]
                        if added_remote_images:
                            warnings = _apply_remote_images_to_local(
                                client,
                                local_id,
                                added_remote_images,
                                allow_delete=False,
                                materialize_remote_images=materialize_remote_images,
                            )
                            errors.extend(warnings)
                    measurement_result = _import_remote_measurements_for_observation(
                        client,
                        local_id,
                        cloud_id,
                        remote_images,
                        remote_measurements,
                        materialize_remote_images=materialize_remote_images,
                    )
                    errors.extend(measurement_result.get('warnings') or [])
                    remote_media_pending = bool(
                        (not materialize_remote_images and _remote_images_missing_locally(local_id, remote_images))
                        or measurement_result.get('skipped_materialization')
                        or measurement_result.get('failed')
                    )
                    effective_media_changed = local_media_changed if sync_images else False
                    # Stage C review round 2, item 3: a push-side identity
                    # clear whose read-back found the identity still
                    # attached (e.g. a rate-limited row-suppression trigger)
                    # marks this exact conflict-review reason. Its ordinary
                    # genus/species half may already have reached the cloud
                    # by then, which makes local and remote agree at the
                    # ORDINARY field level — that must never read as
                    # "nothing left to do" and silently re-stamp this
                    # observation synced; only a later push that actually
                    # succeeds resolves it. Remote-only field/image/
                    # measurement merging above is unaffected — this only
                    # forces the final dirty/synced decision, the same
                    # narrow point Stage B's "both sides changed" conflict
                    # already uses to stay blocked without disabling merge.
                    identity_clear_verification_pending = (
                        local_obs.get('sync_blocked_reason') == CONFLICT_REVIEW_PENDING_MARKER
                    )
                    remaining_local_changes = _remaining_local_changes_after_remote_merge(
                        field_changes,
                        local_media_changed=effective_media_changed,
                    ) or bool(measurement_result.get('conflict')) or bool(
                        materialize_remote_images and remote_media_pending
                    ) or identity_clear_verification_pending
                    should_store_snapshot = (
                        should_store_snapshot
                        and not bool(conflict_fields)
                        and not identity_clear_verification_pending
                    )
                    if remaining_local_changes:
                        _set_observation_sync_state(local_id, cloud_id, dirty=True, synced_at=None)
                    else:
                        _stamp_observation_synced(local_id, cloud_id)
                    if remaining_local_changes:
                        # Explain why the pull left this observation dirty so a
                        # repeat sync can be diagnosed without adding ad-hoc prints.
                        reasons: list[str] = []
                        blocking_local_only_fields = sorted(
                            set(field_changes.get('local_only_fields') or [])
                            - _non_blocking_local_only_fields(field_changes)
                        )
                        if blocking_local_only_fields:
                            reasons.append(f"local_only_fields={blocking_local_only_fields}")
                        if field_changes.get('conflict_fields'):
                            reasons.append(f"conflict_fields={sorted(field_changes['conflict_fields'])}")
                        if effective_media_changed:
                            reasons.append("local_media_changed")
                        if measurement_result.get('conflict'):
                            reasons.append("measurement_conflict")
                        print(
                            f"[cloud_sync] sync_status transition obs {local_id}: →dirty "
                            f"caller=pull_all reasons={reasons or ['unknown']} "
                            f"sync_images={sync_images}",
                            flush=True,
                        )
                    # Metadata-only mode: even if bytes drifted, refresh the
                    # stored signature to acknowledge current DB state so the
                    # next sync doesn't re-flag the same drift.
                    if (not local_media_changed or not sync_images) and not remote_media_pending:
                        _refresh_local_cloud_media_signature(local_id)
                    store_full_snapshot = not remaining_local_changes and not bool(conflict_fields)
                    pulled += 1
                elif not local_dirty:
                    # Neither branch fired: the observation survived prune
                    # (remote updated_at is newer than local synced_at, or the
                    # timestamps couldn't be parsed) but the reconcile loop
                    # found remote fully matched local snapshot. Stamp synced
                    # so the next fast pull skips this row instead of paying
                    # the bulk image + measurement fetch cost forever.
                    if cloud_id:
                        _stamp_observation_synced(local_id, cloud_id)
                # Metadata-only pulls intentionally skip the retry pass because
                # that branch exists solely to re-materialize missing cloud media.
                if stored_snapshot and materialize_remote_images:
                    retry_remote_images = _remote_images_missing_locally(local_id, remote_images)
                    if retry_remote_images:
                        profiler = _cloud_sync_current_profiler()
                        if profiler is not None:
                            try:
                                profiler.record_retry_missing_cloud_media_branch()
                            except Exception:
                                pass
                        _emit_progress(
                            progress_cb,
                            _format_cloud_sync_observation_status(
                                remote,
                                f"Retrying missing cloud media for local observation {local_id}…",
                            ),
                            progress_state,
                        )
                        warnings = _apply_remote_images_to_local(
                            client,
                            local_id,
                            retry_remote_images,
                            allow_delete=False,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(warnings)
                        measurement_result = _import_remote_measurements_for_observation(
                            client,
                            local_id,
                            cloud_id,
                            remote_images,
                            remote_measurements,
                            materialize_remote_images=materialize_remote_images,
                        )
                        errors.extend(measurement_result.get('warnings') or [])
                        retry_remote_media_pending = bool(
                            _remote_images_missing_locally(local_id, retry_remote_images)
                            or measurement_result.get('skipped_materialization')
                            or measurement_result.get('failed')
                        )
                        if measurement_result.get('conflict') or retry_remote_media_pending:
                            _set_observation_sync_state(local_id, cloud_id, dirty=True, synced_at=None)
                        else:
                            _stamp_observation_synced(local_id, cloud_id)
                        if not local_media_changed and not retry_remote_media_pending:
                            _refresh_local_cloud_media_signature(local_id)
                        if not remote_changed:
                            pulled += 1
                if cloud_id and should_store_snapshot:
                    _store_remote_snapshot(
                        client,
                        cloud_id,
                        remote=snapshot_remote,
                        remote_images=remote_images,
                        remote_measurements=remote_measurements,
                        include_images=store_full_snapshot,
                        include_measurements=store_full_snapshot,
                    )
        except Exception as e:
            if is_cloud_auth_error(e) or is_cloud_temporary_unavailable_error(e):
                raise
            errors.append(f"cloud {remote.get('id')}: {e}")
        finally:
            _advance_progress(progress_state, 1)
            _emit_progress(
                progress_cb,
                _format_cloud_sync_observation_status(
                    remote,
                    f"Processed cloud observation {i + 1}/{max(1, total)}",
                ),
                progress_state,
            )

    updates = {'cloud_last_pull_at': datetime.now(timezone.utc).isoformat()}
    if imported_local_ids:
        updates['cloud_recent_import_local_ids'] = json.dumps(imported_local_ids)
    update_app_settings(updates)
    deleted_remote = _detect_deleted_remote_observations(remote_obs)
    _increment_sync_summary(_cloud_sync_current_summary(), 'observations_deleted_remote', len(deleted_remote))
    return {
        'pulled': pulled,
        'total': total,
        'calibrations_pulled': calibration_result.get('pulled', 0),
        'calibrations_total': calibration_result.get('total', 0),
        'errors': errors,
        'deleted_remote': deleted_remote,
        'sync_summary': dict(_cloud_sync_current_summary() or {}),
    }


def _create_local_from_remote(
    remote: dict,
    progress_cb: ProgressCallback | None = None,
    progress_state: dict | None = None,
    remote_index: int | None = None,
    remote_total: int | None = None,
    remote_images: list[dict] | None = None,
    client: SporelyCloudClient | None = None,
    remote_measurements: list[dict] | None = None,
    materialize_remote_images: bool = True,
) -> int:
    """Insert a cloud observation into local SQLite. Returns new local ID."""
    raw_location_public = remote.get('location_public')
    location_public = _normalize_observation_bool_value(raw_location_public, default=None)
    sharing_scope = _cloud_visibility_to_sharing_scope(
        remote.get('visibility') or remote.get('sharing_scope'),
        fallback='friends' if location_public else 'private',
    )
    raw_spore_vis = str(remote.get('spore_data_visibility') or 'public').strip().lower()
    spore_data_visibility = raw_spore_vis if raw_spore_vis in {'private', 'friends', 'public'} else 'public'
    raw_publish_target = str(remote.get('publish_target') or '').strip()

    # Serialize the JSONB red-list payload back to TEXT so the local column
    # matches what a fresh save from the desktop would store. Missing / empty
    # values map to None so the badge falls back to "Not set" cleanly.
    raw_red_categories = remote.get('red_list_categories_json')
    normalized_red_categories = _normalize_observation_json_value(raw_red_categories)
    if normalized_red_categories is not None and not isinstance(normalized_red_categories, str):
        red_list_categories_json_text = json.dumps(
            normalized_red_categories, ensure_ascii=False, sort_keys=True
        )
    elif isinstance(normalized_red_categories, str):
        red_list_categories_json_text = normalized_red_categories
    else:
        red_list_categories_json_text = None
    red_list_category_text = str(remote.get('red_list_category') or '').strip() or None

    # Map cloud columns to create_observation kwargs
    remote_captured_at = str(remote.get('captured_at') or '').strip()
    genus, species, species_guess = resolve_observation_taxon_fields(
        remote.get('genus'),
        remote.get('species'),
        remote.get('species_guess'),
        remote.get('ai_selected_scientific_name'),
    )
    kwargs = dict(
        date=remote_captured_at or remote.get('date') or datetime.now().strftime('%Y-%m-%d'),
        genus=genus,
        species=species,
        common_name=remote.get('common_name'),
        species_guess=species_guess,
        location=remote.get('location'),
        habitat=remote.get('habitat'),
        notes=remote.get('notes'),
        open_comment=remote.get('open_comment'),
        sharing_scope=sharing_scope,
        location_public=location_public,
        spore_data_visibility=spore_data_visibility,
        uncertain=_normalize_observation_bool_value(remote.get('uncertain'), default=False),
        unspontaneous=_normalize_observation_bool_value(remote.get('unspontaneous'), default=False),
        gps_latitude=_normalize_observation_float_value(remote.get('gps_latitude')),
        gps_longitude=_normalize_observation_float_value(remote.get('gps_longitude')),
        is_draft=_normalize_observation_bool_value(remote.get('is_draft'), default=True),
        location_precision=ObservationDB._normalize_location_precision(remote.get('location_precision')),
        ai_selected_service=remote.get('ai_selected_service'),
        ai_selected_taxon_id=remote.get('ai_selected_taxon_id'),
        ai_selected_scientific_name=remote.get('ai_selected_scientific_name'),
        ai_selected_probability=_normalize_observation_float_value(remote.get('ai_selected_probability')),
        ai_selected_at=remote.get('ai_selected_at'),
        red_list_category=red_list_category_text,
        red_list_categories_json=red_list_categories_json_text,
        source_type=remote.get('source_type') or 'personal',
        author=remote.get('author'),
        habitat_nin2_path=remote.get('habitat_nin2_path'),
        habitat_substrate_path=remote.get('habitat_substrate_path'),
        habitat_host_genus=remote.get('habitat_host_genus'),
        habitat_host_species=remote.get('habitat_host_species'),
        habitat_host_common_name=remote.get('habitat_host_common_name'),
        habitat_nin2_note=remote.get('habitat_nin2_note'),
        habitat_substrate_note=remote.get('habitat_substrate_note'),
        habitat_grows_on_note=remote.get('habitat_grows_on_note'),
        publish_target=normalize_publish_target(raw_publish_target) if raw_publish_target else None,
        interesting_comment=_normalize_observation_bool_value(remote.get('interesting_comment'), default=False),
        country_code=normalize_country_code(remote.get('country_code')),
        region_id=_normalize_observation_field_value('region_id', remote.get('region_id')),
    )
    # The cloud identity arrives with the row, conservatively: see
    # `_local_identity_columns_for_remote_claim`.
    identity_claim = _remote_identity_claim(remote)
    if identity_claim is not None:
        kwargs.update(_local_identity_columns_for_remote_claim(identity_claim, remote))
    local_id = ObservationDB.create_observation(**kwargs)

    # Bind the cloud row immediately, but keep the observation pending until
    # the child image / measurement import outcome is known.
    conn = get_connection()
    cursor = conn.cursor()
    update_observation_sync_state(
        cursor,
        int(local_id),
        cloud_id=remote['id'],
        sync_status='dirty',
        synced_at=None,
        clear_sync_error_state=True,
    )
    conn.commit()
    conn.close()

    cloud_id = str(remote.get('id') or '').strip()
    image_result = {
        'imported': 0,
        'metadata_applied': 0,
        'skipped_materialization': 0,
        'failed': 0,
        'warnings': [],
        'errors': [],
        'complete': True,
    }
    if cloud_id:
        image_result = _import_remote_images(
            client,
            remote,
            local_id,
            cloud_id,
            progress_cb=progress_cb,
            progress_state=progress_state,
            remote_index=remote_index,
            remote_total=remote_total,
            remote_images=remote_images,
            materialize_remote_images=materialize_remote_images,
        )
        measurement_result = _import_remote_measurements_for_observation(
            client,
            local_id,
            cloud_id,
            remote_images=remote_images,
            remote_measurements=remote_measurements,
            materialize_remote_images=materialize_remote_images,
        )
        if image_result.get('warnings'):
            for warning in image_result['warnings']:
                print(f'[cloud_sync] Observation {local_id}: {warning}')
        if measurement_result.get('warnings'):
            for warning in measurement_result['warnings']:
                print(f'[cloud_sync] Observation {local_id}: {warning}')
        complete = bool(image_result.get('complete')) and bool(measurement_result.get('complete'))
        if complete and not image_result.get('errors') and not measurement_result.get('errors') and not measurement_result.get('conflict'):
            _stamp_observation_synced(local_id, cloud_id)
        else:
            _set_observation_sync_state(local_id, cloud_id, dirty=True, synced_at=None)
        try:
            _store_remote_snapshot(
                client,
                cloud_id,
                remote=remote,
                remote_images=remote_images,
                remote_measurements=remote_measurements,
                include_images=complete,
                include_measurements=complete,
            )
        except Exception:
            pass

    return local_id


def _import_remote_measurements_for_observation(
    client: SporelyCloudClient | None,
    local_id: int,
    cloud_id: str,
    remote_images: list[dict] | None = None,
    remote_measurements: list[dict] | None = None,
    materialize_remote_images: bool = True,
    overwrite_conflicts: bool = False,
) -> dict:
    warnings: list[str] = []
    if not str(cloud_id or '').strip():
        return {
            'warnings': warnings,
            'errors': [],
            'conflict': False,
            'imported': 0,
            'skipped_materialization': 0,
            'failed': 0,
            'complete': True,
        }
    if client is None:
        client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        return {
            'warnings': warnings,
            'errors': ['Could not load Sporely Cloud credentials.'],
            'conflict': False,
            'imported': 0,
            'skipped_materialization': 0,
            'failed': 1,
            'complete': False,
        }

    remote_images_raw = (
        [dict(row or {}) for row in remote_images]
        if remote_images is not None
        else [dict(row or {}) for row in (client.pull_image_metadata(cloud_id) or [])]
    )
    remote_image_lookup = {
        str(row.get('id') or '').strip(): row
        for row in remote_images_raw
        if str(row.get('id') or '').strip()
    }
    tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(remote_image_lookup.keys())
    measurement_rows_source = (
        [dict(row or {}) for row in (remote_measurements or [])]
        if remote_measurements is not None
        else _pull_remote_measurements_for_images(client, list(remote_image_lookup.keys()))
    )
    remote_measurements_by_obs = _group_remote_measurements_by_observation(remote_images_raw, measurement_rows_source)
    measurement_rows = [dict(row or {}) for row in remote_measurements_by_obs.get(str(cloud_id), [])]
    if not measurement_rows:
        return {
            'warnings': warnings,
            'errors': [],
            'conflict': False,
            'imported': 0,
            'skipped_materialization': 0,
            'failed': 0,
            'complete': True,
        }

    def _load_local_images() -> tuple[dict[str, dict], dict[int, dict]]:
        local_images = ImageDB.get_images_for_observation(int(local_id))
        by_cloud_id: dict[str, dict] = {}
        by_local_id: dict[int, dict] = {}
        for image_row in local_images or []:
            local_image_id = _safe_int(image_row.get('id'))
            if local_image_id > 0:
                by_local_id[local_image_id] = dict(image_row or {})
            cloud_image_id = str(image_row.get('cloud_id') or '').strip()
            if cloud_image_id:
                by_cloud_id[cloud_image_id] = dict(image_row or {})
        return by_cloud_id, by_local_id

    def _measurement_write_values(remote_row: dict, local_image_id: int) -> dict:
        return {
            'image_id': int(local_image_id),
            'length_um': remote_row.get('length_um'),
            'width_um': remote_row.get('width_um'),
            'measurement_type': _normalize_measurement_type_value(remote_row.get('measurement_type')),
            'gallery_rotation': _safe_int(remote_row.get('gallery_rotation')),
            'p1_x': remote_row.get('p1_x'),
            'p1_y': remote_row.get('p1_y'),
            'p2_x': remote_row.get('p2_x'),
            'p2_y': remote_row.get('p2_y'),
            'p3_x': remote_row.get('p3_x'),
            'p3_y': remote_row.get('p3_y'),
            'p4_x': remote_row.get('p4_x'),
            'p4_y': remote_row.get('p4_y'),
            'measured_at': (
                str(remote_row.get('measured_at') or '').strip()
                or datetime.now(timezone.utc).isoformat()
            ),
        }

    local_images_by_cloud_id, local_images_by_id = _load_local_images()
    local_measurements_by_cloud_id, local_measurements_by_id = _load_local_measurement_lookup(int(local_id))
    # Under Download-from-Cloud the wrapper marks the client is_pull_only; skip
    # the identity write-back to keep the zero-cloud-writes contract without
    # relying on the wrapper to raise mid-flow.
    _pull_only = bool(getattr(client, 'is_pull_only', False))
    _suppress_reverse_identity = (
        _pull_only
        or _portable_cloud_identity_pending_for_observation(int(local_id))
    )
    set_measurement_desktop_id = (
        None
        if _suppress_reverse_identity
        else getattr(client, 'set_measurement_desktop_id', None)
    )
    imported = 0
    conflict = False
    skipped_materialization = 0
    failed = 0
    skip_groups: dict[str, dict[str, object]] = {}

    def _record_skip(key: str, remote_image_id: str | None) -> None:
        bucket = skip_groups.setdefault(key, {'count': 0, 'image_ids': set()})
        bucket['count'] = int(bucket['count'] or 0) + 1
        if remote_image_id:
            bucket['image_ids'].add(remote_image_id)

    conn = get_connection()
    conn.row_factory = __import__('sqlite3').Row
    cursor = conn.cursor()
    try:
        for remote_row in measurement_rows:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue

            remote_image_id = str(remote_row.get('image_id') or '').strip()
            remote_image = remote_image_lookup.get(remote_image_id)
            if not remote_image:
                _record_skip('missing_remote_image', remote_image_id or None)
                continue
            if not _is_spore_measurement_source_image(remote_image):
                _record_skip('excluded_image', remote_image_id or None)
                continue
            if remote_image_id in tombstoned_remote_image_ids:
                _record_skip('tombstoned_image', remote_image_id or None)
                continue

            local_image = local_images_by_cloud_id.get(remote_image_id)
            if local_image is None:
                if not materialize_remote_images:
                    skipped_materialization += 1
                    warnings.append(
                        f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} because cloud image {remote_image_id} has not been materialized locally"
                    )
                    continue
                warnings.extend(
                    _apply_remote_images_to_local(
                        client,
                        int(local_id),
                        [remote_image],
                        allow_delete=False,
                        materialize_remote_images=materialize_remote_images,
                    )
                )
                local_images_by_cloud_id, local_images_by_id = _load_local_images()
                local_image = local_images_by_cloud_id.get(remote_image_id)
            if local_image is None:
                _record_skip('missing_local_anchor', remote_image_id or None)
                continue

            if not _is_spore_measurement_source_image(local_image):
                _record_skip('excluded_image', remote_image_id or None)
                continue

            local_image_id = _safe_int(local_image.get('id'))
            if local_image_id <= 0:
                _record_skip('missing_local_anchor', remote_image_id or None)
                continue

            local_measurement = local_measurements_by_cloud_id.get(remote_measurement_id)
            if local_measurement is None and not _suppress_reverse_identity:
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                if remote_desktop_measurement_id > 0:
                    local_measurement = local_measurements_by_id.get(remote_desktop_measurement_id)

            if local_measurement is None:
                write_values = _measurement_write_values(remote_row, local_image_id)
                cursor.execute(
                    '''
                    INSERT INTO spore_measurements (
                        image_id, length_um, width_um, measurement_type, gallery_rotation,
                        p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y,
                        measured_at, cloud_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        write_values['image_id'],
                        write_values['length_um'],
                        write_values['width_um'],
                        write_values['measurement_type'],
                        write_values['gallery_rotation'],
                        write_values['p1_x'],
                        write_values['p1_y'],
                        write_values['p2_x'],
                        write_values['p2_y'],
                        write_values['p3_x'],
                        write_values['p3_y'],
                        write_values['p4_x'],
                        write_values['p4_y'],
                        write_values['measured_at'],
                        remote_measurement_id,
                    ),
                )
                new_local_measurement_id = _safe_int(cursor.lastrowid)
                imported += 1
                if callable(set_measurement_desktop_id):
                    remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                    if remote_desktop_measurement_id != new_local_measurement_id:
                        try:
                            set_measurement_desktop_id(remote_measurement_id, new_local_measurement_id)
                            remote_row['desktop_id'] = new_local_measurement_id
                        except Exception as exc:
                            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                                raise
                local_measurements_by_cloud_id[remote_measurement_id] = {
                    'id': new_local_measurement_id,
                    'cloud_id': remote_measurement_id,
                    'image_id': local_image_id,
                    'image_cloud_id': str(local_image.get('cloud_id') or '').strip() or None,
                    **write_values,
                }
                if new_local_measurement_id > 0:
                    local_measurements_by_id[new_local_measurement_id] = dict(local_measurements_by_cloud_id[remote_measurement_id])
                continue

            # The local image the measurement should live under has already
            # been resolved (via local_images_by_cloud_id above), so pin both
            # sides to the same cloud image id — image_id is identity, not
            # content, and this stops a stale/missing local images.cloud_id
            # from making every measurement on that image look "locally
            # modified". Routing through the shared comparator also picks up
            # float tolerance and sqlite/pgrest type normalisation.
            if not _measurement_payloads_match(
                local_measurement,
                remote_row,
                cloud_image_id=remote_image_id,
            ) and not overwrite_conflicts:
                conflict = True
                warnings.append(
                    f"obs {int(local_id)}: skipped cloud measurement {remote_measurement_id} "
                    f"because the local copy changed"
                )
                continue

            write_values = _measurement_write_values(remote_row, local_image_id)
            # Gallery rotation is presentation state. For an existing
            # desktop-originated measurement, keep the desktop value even
            # while scientific cloud values are selected or refreshed.
            write_values['gallery_rotation'] = _safe_int(
                local_measurement.get('gallery_rotation')
            )
            cursor.execute(
                '''
                UPDATE spore_measurements
                SET image_id = ?,
                    length_um = ?,
                    width_um = ?,
                    measurement_type = ?,
                    gallery_rotation = ?,
                    p1_x = ?,
                    p1_y = ?,
                    p2_x = ?,
                    p2_y = ?,
                    p3_x = ?,
                    p3_y = ?,
                    p4_x = ?,
                    p4_y = ?,
                    measured_at = ?,
                    cloud_id = ?
                WHERE id = ?
                ''',
                (
                    write_values['image_id'],
                    write_values['length_um'],
                    write_values['width_um'],
                    write_values['measurement_type'],
                    write_values['gallery_rotation'],
                    write_values['p1_x'],
                    write_values['p1_y'],
                    write_values['p2_x'],
                    write_values['p2_y'],
                    write_values['p3_x'],
                    write_values['p3_y'],
                    write_values['p4_x'],
                    write_values['p4_y'],
                    write_values['measured_at'],
                    remote_measurement_id,
                    _safe_int(local_measurement.get('id')),
                ),
            )
            imported += 1
            if callable(set_measurement_desktop_id):
                remote_desktop_measurement_id = _safe_int(remote_row.get('desktop_id'))
                local_measurement_id = _safe_int(local_measurement.get('id'))
                if remote_desktop_measurement_id != local_measurement_id:
                    try:
                        set_measurement_desktop_id(remote_measurement_id, local_measurement_id)
                        remote_row['desktop_id'] = local_measurement_id
                    except Exception as exc:
                        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                            raise
    finally:
        conn.commit()
        conn.close()

    for key, template in (
        (
            'missing_remote_image',
            'obs {local_id}: skipped {count} cloud measurement(s) because cloud images {image_ids} are unavailable',
        ),
        (
            'excluded_image',
            'obs {local_id}: skipped {count} cloud measurement(s) on {image_count} excluded image(s): {image_ids}',
        ),
        (
            'tombstoned_image',
            'obs {local_id}: skipped {count} cloud measurement(s) because cloud images {image_ids} have a local tombstone',
        ),
        (
            'missing_local_anchor',
            'obs {local_id}: skipped {count} cloud measurement(s) because {image_count} image anchor(s) could not be materialized: {image_ids}',
        ),
    ):
        bucket = skip_groups.get(key)
        if not bucket:
            continue
        count = int(bucket.get('count') or 0)
        if count <= 0:
            continue
        image_ids = sorted(
            {str(value) for value in bucket.get('image_ids') or set()},
            key=lambda value: (len(value), value),
        )
        image_ids_text = ', '.join(image_ids) if image_ids else '?'
        image_count = len(image_ids) if image_ids else 1
        warnings.append(
            template.format(
                local_id=int(local_id),
                count=count,
                image_ids=image_ids_text,
                image_count=image_count,
            )
        )

    return {
        'warnings': warnings,
        'errors': [],
        'conflict': conflict,
        'imported': imported,
        'skipped_materialization': skipped_materialization,
        'failed': failed,
        'complete': not conflict and skipped_materialization == 0 and failed == 0,
    }


def materialize_cloud_media_for_observation(
    client: SporelyCloudClient | None,
    local_observation_id: int | str,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    summary = {
        'status': 'skipped',
        'reason': None,
        'local_observation_id': _safe_int(local_observation_id),
        'cloud_observation_id': None,
        'remote_images_considered': 0,
        'skipped_already_materialized': 0,
        'downloaded': 0,
        'failed': 0,
        'measurements_imported': 0,
        'warnings': [],
        'errors': [],
        'used_snapshot_data': False,
        'used_live_fallback': False,
    }

    profiler = _cloud_sync_current_profiler()
    owns_profiler = False
    profile_token = None
    if profiler is None and _cloud_sync_profile_enabled():
        profiler = CloudSyncProfiler()
        owns_profiler = True
        try:
            profile_token = _CLOUD_SYNC_PROFILE_CONTEXT.set(profiler)
        except Exception:
            profile_token = None

    def _finish(result_summary: dict) -> dict:
        if owns_profiler and profiler is not None:
            try:
                profiler.finish(result=result_summary)
            except Exception:
                pass
        return result_summary

    local_id = _safe_int(local_observation_id)
    if local_id <= 0:
        summary['reason'] = 'invalid_local_observation_id'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    local_obs = ObservationDB.get_observation(local_id)
    if not local_obs:
        summary['reason'] = 'local_observation_not_found'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)
    suppress_reverse_identity = bool(
        local_obs.get('portable_cloud_identity_pending')
    )

    cloud_id = str(local_obs.get('cloud_id') or '').strip()
    summary['cloud_observation_id'] = cloud_id or None
    if not cloud_id:
        summary['reason'] = 'no_cloud_snapshot'
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    if client is None:
        client = SporelyCloudClient.from_stored_credentials()
    if client is None:
        summary['status'] = 'error'
        summary['errors'].append('Could not load Sporely Cloud credentials.')
        if profile_token is not None:
            try:
                _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
            except Exception:
                pass
        return _finish(summary)

    snapshot_data = _parse_cloud_observation_snapshot(_load_cloud_observation_snapshot(cloud_id))
    snapshot_has_media = 'images' in snapshot_data and 'measurements' in snapshot_data

    remote = dict(snapshot_data.get('observation') or local_obs or {'id': cloud_id})
    remote_images_raw: list[dict] = []
    remote_measurements_source: list[dict] = []

    if snapshot_has_media:
        summary['used_snapshot_data'] = True
        remote_images_raw = [dict(row or {}) for row in (snapshot_data.get('images') or [])]
        remote_measurements_source = [dict(row or {}) for row in (snapshot_data.get('measurements') or [])]
    else:
        summary['used_live_fallback'] = True
        get_observation = getattr(client, 'get_observation', None)
        if callable(get_observation):
            try:
                live_remote = get_observation(cloud_id)
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['warnings'].append(
                    f'obs {local_id}: could not fetch live cloud observation metadata: {exc}'
                )
            else:
                if isinstance(live_remote, dict) and live_remote:
                    remote = dict(live_remote)
        try:
            remote_images_raw = [
                dict(row or {})
                for row in (client.pull_image_metadata(cloud_id, include_deleted_for_sync=True) or [])
            ]
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['errors'].append(f'obs {local_id}: could not fetch cloud image metadata: {exc}')
            remote_images_raw = []
        image_cloud_ids = [
            str(row.get('id') or '').strip()
            for row in remote_images_raw
            if str(row.get('id') or '').strip()
        ]
        try:
            remote_measurements_source = _pull_remote_measurements_for_images(client, image_cloud_ids)
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['errors'].append(f'obs {local_id}: could not fetch cloud measurements: {exc}')
            remote_measurements_source = []

    _record_remote_image_tombstones(
        remote_images_raw,
        local_observation_id=local_id,
        cloud_observation_id=cloud_id,
    )

    active_remote_images = [
        row
        for row in remote_images_raw
        if should_pull_cloud_image_to_desktop(row)
        and not str(row.get('deleted_at') or '').strip()
        and str(row.get('id') or '').strip()
    ]
    active_remote_images.sort(
        key=lambda row: (int(row.get('sort_order') or 0), str(row.get('id') or ''))
    )
    summary['remote_images_considered'] = len(active_remote_images)

    remote_measurements_by_obs = _group_remote_measurements_by_observation(
        remote_images_raw,
        remote_measurements_source,
    )
    remote_measurements = [dict(row or {}) for row in remote_measurements_by_obs.get(cloud_id, [])]

    local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
    tombstoned_remote_image_ids = _local_tombstoned_cloud_image_ids(
        [str(row.get('id') or '').strip() for row in active_remote_images]
    )

    progress_state = {'done': 0, 'total': 0}
    if active_remote_images:
        _extend_progress_total(progress_state, len(active_remote_images))

    observation_folder = str(local_obs.get('folder_path') or '').strip()
    if observation_folder:
        base_folder = Path(observation_folder)
    else:
        try:
            base_folder = ObservationDB._build_observation_folder_path(
                local_obs.get('genus'),
                local_obs.get('species'),
                local_obs.get('date'),
            )
        except Exception:
            base_folder = get_images_dir() / f'observation_{local_id}'

    def _local_image_asset_path(local_image: dict | None) -> Path | None:
        if not local_image:
            return None
        return _resolve_existing_local_image_asset_path(local_image.get('filepath'))

    def _fallback_local_image_path(remote_image: dict) -> Path:
        filename = Path(str(remote_image.get('original_filename') or '')).name
        if not filename:
            filename = f"{str(remote_image.get('id') or local_id).strip() or local_id}.jpg"
        return base_folder / filename

    def _ensure_local_cloud_link(local_image: dict, remote_image: dict) -> None:
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id:
            return
        local_image_id = _safe_int(local_image.get('id'))
        if local_image_id <= 0:
            return

        current_cloud_id = str(local_image.get('cloud_id') or '').strip()
        current_remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
        if current_cloud_id != cloud_image_id:
            conn = get_connection()
            try:
                conn.execute(
                    'UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?',
                    (cloud_image_id, datetime.now(timezone.utc).isoformat(), local_image_id),
                )
                conn.commit()
                local_image['cloud_id'] = cloud_image_id
            finally:
                conn.close()

        if not suppress_reverse_identity and current_remote_desktop_id != local_image_id:
            set_image_desktop_id = getattr(client, 'set_image_desktop_id', None)
            if callable(set_image_desktop_id):
                try:
                    set_image_desktop_id(cloud_image_id, local_image_id)
                    remote_image['desktop_id'] = local_image_id
                except Exception as exc:
                    if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                        raise
                    summary['warnings'].append(
                        f'obs {local_id}: could not link cloud image {cloud_image_id} to local image {local_image_id}: {exc}'
                    )

    for idx, remote_image in enumerate(active_remote_images, start=1):
        cloud_image_id = str(remote_image.get('id') or '').strip()
        if not cloud_image_id:
            continue
        if cloud_image_id in tombstoned_remote_image_ids:
            warning = _tombstoned_cloud_image_warning(local_id, cloud_image_id)
            summary['warnings'].append(warning)
            print(f'[cloud_sync] Warning: {warning}')
            continue

        _emit_progress(
            progress_cb,
            _format_cloud_sync_observation_status(
                remote,
                f"Materializing cloud image {idx}/{len(active_remote_images)}: {cloud_image_id}…",
            ),
            progress_state,
        )

        local_image = local_images_by_cloud_id.get(cloud_image_id)
        if local_image is None and not suppress_reverse_identity:
            remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
            if remote_desktop_id > 0:
                local_image = local_images_by_id.get(remote_desktop_id)

        if local_image is not None:
            if _is_metadata_only_microscope_cloud_image(remote_image):
                _ensure_local_metadata_only_microscope_anchor(
                    client,
                    local_id,
                    remote_image,
                    local_image=local_image,
                )
                summary['skipped_already_materialized'] += 1
                _ensure_local_cloud_link(local_image, remote_image)
                _advance_progress(progress_state, 1)
                continue

            existing_asset_path = _local_image_asset_path(local_image)
            if existing_asset_path is not None:
                summary['skipped_already_materialized'] += 1
                _ensure_local_cloud_link(local_image, remote_image)
                _advance_progress(progress_state, 1)
                continue

            repair_local_image = dict(local_image)
            if not str(repair_local_image.get('filepath') or '').strip():
                repair_local_image['filepath'] = str(_fallback_local_image_path(remote_image))
            try:
                _sync_existing_remote_image_to_local(
                    client,
                    repair_local_image,
                    remote_image,
                    materialize_remote_images=True,
                )
            except CloudSyncError as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['failed'] += 1
                summary['errors'].append(
                    f'obs {local_id}: could not repair cloud image {cloud_image_id}: {exc}'
                )
                _advance_progress(progress_state, 1)
                continue
            except Exception as exc:
                if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                    raise
                summary['failed'] += 1
                summary['errors'].append(
                    f'obs {local_id}: could not repair cloud image {cloud_image_id}: {exc}'
                )
                _advance_progress(progress_state, 1)
                continue

            summary['downloaded'] += 1
            _ensure_local_cloud_link(repair_local_image, remote_image)
            local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
            _advance_progress(progress_state, 1)
            continue

        if _is_metadata_only_microscope_cloud_image(remote_image):
            local_image_id = _ensure_local_metadata_only_microscope_anchor(
                client,
                local_id,
                remote_image,
            )
            if local_image_id is None:
                summary['failed'] += 1
                summary['errors'].append(
                    f'obs {local_id}: failed to materialize cloud image {cloud_image_id}'
                )
            else:
                summary['skipped_already_materialized'] += 1
            _advance_progress(progress_state, 1)
            continue

        try:
            warnings = _apply_remote_images_to_local(
                client,
                local_id,
                [remote_image],
                allow_delete=False,
                materialize_remote_images=True,
            )
            if warnings:
                summary['warnings'].extend(warnings)
        except CloudSyncError as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: could not materialize cloud image {cloud_image_id}: {exc}'
            )
            _advance_progress(progress_state, 1)
            continue
        except Exception as exc:
            if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
                raise
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: could not materialize cloud image {cloud_image_id}: {exc}'
            )
            _advance_progress(progress_state, 1)
            continue

        local_images_by_cloud_id, local_images_by_id = _load_local_image_lookup(local_id)
        created_local_image = local_images_by_cloud_id.get(cloud_image_id)
        if created_local_image is None:
            remote_desktop_id = _safe_int(remote_image.get('desktop_id'))
            if remote_desktop_id > 0:
                created_local_image = local_images_by_id.get(remote_desktop_id)
        if created_local_image is None or _local_image_asset_path(created_local_image) is None:
            summary['failed'] += 1
            summary['errors'].append(
                f'obs {local_id}: failed to materialize cloud image {cloud_image_id}'
            )
            _advance_progress(progress_state, 1)
            continue

        summary['downloaded'] += 1
        _ensure_local_cloud_link(created_local_image, remote_image)
        _advance_progress(progress_state, 1)

    measurement_result = _import_remote_measurements_for_observation(
        client,
        local_id,
        cloud_id,
        remote_images=remote_images_raw,
        remote_measurements=remote_measurements_source,
        materialize_remote_images=False,
    )
    summary['measurements_imported'] = int(measurement_result.get('imported') or 0)
    try:
        latest_local_measurements_by_cloud_id, _ = _load_local_measurement_lookup(local_id)
        for remote_row in remote_measurements_source:
            remote_measurement_id = str(remote_row.get('id') or '').strip()
            if not remote_measurement_id:
                continue
            local_measurement = latest_local_measurements_by_cloud_id.get(remote_measurement_id)
            if not local_measurement:
                continue
            local_measurement_id = _safe_int(local_measurement.get('id'))
            if local_measurement_id > 0:
                remote_row['desktop_id'] = local_measurement_id
    except Exception:
        pass
    if measurement_result.get('warnings'):
        summary['warnings'].extend(str(warning) for warning in measurement_result.get('warnings') or [])
    if measurement_result.get('conflict'):
        summary['warnings'].append(
            f'obs {local_id}: some cloud measurements need review before they can be linked locally'
        )

    try:
        _store_remote_snapshot(
            client,
            cloud_id,
            remote=remote,
            remote_images=remote_images_raw,
            remote_measurements=remote_measurements_source,
        )
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        summary['warnings'].append(f'obs {local_id}: could not refresh cloud snapshot: {exc}')

    try:
        _refresh_local_cloud_media_signature(local_id)
    except Exception as exc:
        if is_cloud_auth_error(exc) or is_cloud_temporary_unavailable_error(exc):
            raise
        summary['warnings'].append(f'obs {local_id}: could not refresh local media signature: {exc}')

    if summary['failed'] > 0 or summary['errors']:
        summary['status'] = 'partial'
    else:
        summary['status'] = 'ok'
    if profile_token is not None:
        try:
            _CLOUD_SYNC_PROFILE_CONTEXT.reset(profile_token)
        except Exception:
            pass
    return _finish(summary)


