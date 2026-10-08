"""Local cloud-sync state: settings keys, per-image state stores and the dirty markers.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import json

from database.models import (
    SettingsDB,
    mark_observation_sync_dirty,
)
from database.schema import get_connection

from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)


_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX = "sporely_cloud_image_file_sig_"


_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX = "sporely_cloud_local_media_sig_obs_"


# Anchor-promotion pending marker: written when a byte-upload key is reserved
# on an existing metadata-only cloud image row, cleared only after the byte
# upload is confirmed (or the reservation is released). While present, a
# non-NULL remote storage_path equal to the marker must be treated as
# unconfirmed — bytes may never have reached storage.
_SETTING_CLOUD_IMAGE_PROMOTION_PENDING_PREFIX = "sporely_cloud_image_promotion_pending_"


def _cloud_image_file_signature_key(observation_id: int | str, image_id: int | str) -> str:
    return (
        f"{_SETTING_CLOUD_IMAGE_FILE_SIG_PREFIX}"
        f"{str(observation_id or '').strip()}_{str(image_id or '').strip()}"
    )


def _cloud_metadata_only_image_ids_key(observation_id: int | str) -> str:
    return f"sporely_cloud_metadata_only_image_ids_{str(observation_id or '').strip()}"


def _cloud_image_promotion_pending_key(observation_id: int | str, image_id: int | str) -> str:
    return (
        f"{_SETTING_CLOUD_IMAGE_PROMOTION_PENDING_PREFIX}"
        f"{str(observation_id or '').strip()}_{str(image_id or '').strip()}"
    )


# Stage 1: cloud image-storage desired state lives under its own setting so it
# is fully independent of the Artsobs/iNaturalist publication exclusion set.
# The old ``artsobs_publish_excluded_image_ids_<obs>`` key remains a
# publication-only concern and must not be read to infer cloud deletion intent.
CLOUD_IMAGE_STORAGE_EXCLUDED_SETTING_PREFIX = "sporely_cloud_image_storage_excluded_ids_"


# Per-image storage-intent ledger: JSON list of local image ids for which a
# default (or explicit) cloud byte-storage decision has been recorded. An
# image id absent from this ledger has NO storage intent yet — its absence
# from the excluded set proves nothing.
_CLOUD_IMAGE_STORAGE_INTENT_LEDGER_PREFIX = "sporely_cloud_image_storage_intent_ids_"


def _cloud_image_storage_excluded_ids_key(observation_id: int | str) -> str:
    return f"{CLOUD_IMAGE_STORAGE_EXCLUDED_SETTING_PREFIX}{str(observation_id or '').strip()}"


def _cloud_image_storage_intent_ledger_key(observation_id: int | str) -> str:
    return f"{_CLOUD_IMAGE_STORAGE_INTENT_LEDGER_PREFIX}{str(observation_id or '').strip()}"


def _cloud_local_media_signature_key(observation_id: int | str) -> str:
    return f"{_SETTING_CLOUD_LOCAL_MEDIA_SIG_PREFIX}{str(observation_id or '').strip()}"


def _clear_cloud_image_file_signature(observation_id: int | str, image_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_image_file_signature_key(observation_id, image_id), '')


def _load_pending_image_promotion_key(observation_id: int | str, image_id: int | str) -> str:
    return _normalize_cloud_media_key(
        SettingsDB.get_setting(
            _cloud_image_promotion_pending_key(observation_id, image_id), ''
        ) or ''
    )


def _store_pending_image_promotion_key(
    observation_id: int | str,
    image_id: int | str,
    storage_path: str,
) -> None:
    SettingsDB.set_setting(
        _cloud_image_promotion_pending_key(observation_id, image_id),
        _normalize_cloud_media_key(storage_path),
    )


def _clear_pending_image_promotion_key(observation_id: int | str, image_id: int | str) -> None:
    SettingsDB.set_setting(_cloud_image_promotion_pending_key(observation_id, image_id), '')


def _cloud_metadata_only_image_ids(observation_id: int | str) -> set[int]:
    raw = SettingsDB.get_setting(_cloud_metadata_only_image_ids_key(observation_id), '[]')
    try:
        values = json.loads(raw or '[]')
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(values, list):
        return set()
    return {_safe_int(value) for value in values if _safe_int(value) > 0}


def _set_cloud_image_metadata_only_state(
    observation_id: int | str,
    image_id: int | str,
    metadata_only: bool,
) -> None:
    obs_id = _safe_int(observation_id)
    local_image_id = _safe_int(image_id)
    if obs_id <= 0 or local_image_id <= 0:
        return
    image_ids = _cloud_metadata_only_image_ids(obs_id)
    if metadata_only:
        image_ids.add(local_image_id)
    else:
        image_ids.discard(local_image_id)
    SettingsDB.set_setting(
        _cloud_metadata_only_image_ids_key(obs_id),
        json.dumps(sorted(image_ids)),
    )


def _load_local_cloud_media_signature(observation_id: int | str) -> str:
    return str(SettingsDB.get_setting(_cloud_local_media_signature_key(observation_id), '') or '').strip()


def _store_local_cloud_media_signature(observation_id: int | str, signature: str) -> None:
    SettingsDB.set_setting(
        _cloud_local_media_signature_key(observation_id),
        str(signature or '').strip(),
    )


def mark_observation_dirty(local_id: int) -> None:
    try:
        obs_id = int(local_id or 0)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return
    conn = get_connection()
    try:
        cursor = conn.cursor()
        mark_observation_sync_dirty(cursor, obs_id)
        conn.commit()
    finally:
        conn.close()


def mark_observation_media_dirty(local_id: int) -> None:
    """Schedule a user-selected media change for the next cloud sync.

    Checkbox changes are event-driven sync work, so they must not depend on
    the periodic pending-image repair scan. The image-specific cloud-id detach
    and explicit restore marker identify the pending upload; retaining the
    observation media signature lets the pre-encode pass prove that unrelated
    linked images are unchanged.
    """
    try:
        obs_id = int(local_id or 0)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return
    mark_observation_dirty(obs_id)


def _explicit_image_restore_source_key(image_id: int | str) -> str:
    return f"sporely_cloud_explicit_image_restore_source_{int(image_id)}"


def remember_explicit_image_restore_source(image_id: int, deleted_cloud_id: str) -> None:
    """Remember the tombstoned cloud row that an explicit restore must not reuse."""
    image_id = _safe_int(image_id)
    cloud_id = str(deleted_cloud_id or "").strip()
    if image_id > 0 and cloud_id:
        SettingsDB.set_setting(_explicit_image_restore_source_key(image_id), cloud_id)


def _explicit_image_restore_source(image_id: int | str) -> str:
    image_id = _safe_int(image_id)
    if image_id <= 0:
        return ""
    return str(
        SettingsDB.get_setting(_explicit_image_restore_source_key(image_id), "") or ""
    ).strip()
