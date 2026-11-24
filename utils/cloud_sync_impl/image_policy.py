"""Image byte policy: desired bytes, the storage-intent ledger, explicit selection and anchor predicates.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import sqlite3
import threading

from database.models import (
    CLOUD_IMAGE_STATE_DELETED,
    CLOUD_IMAGE_STATE_DELETE_PENDING,
    CLOUD_IMAGE_STATE_METADATA_ONLY,
    CLOUD_IMAGE_STATE_NONE,
    CLOUD_IMAGE_STATE_UPLOADED,
    ImageDB,
    SettingsDB,
    derive_image_cloud_state,
)
from database.schema import get_connection

from utils.cloud_sync_impl.common import (
    _normalize_cloud_media_key,
    _safe_int,
)
from utils.cloud_sync_impl.sync_state import (
    _cloud_image_storage_excluded_ids_key,
    _cloud_image_storage_intent_ledger_key,
    _cloud_metadata_only_image_ids,
    mark_observation_media_dirty,
    remember_explicit_image_restore_source,
)


# Serializes every read-modify-write of the per-observation storage settings
# (the excluded set and the intent ledger). The GUI thread (checkboxes,
# moves, publishing) and the sync thread (default seeding) both write them;
# without this a stale whole-set write could undo a just-made check.
# Re-entrant: a writer may call another writer. In-process only.
_STORAGE_INTENT_LOCK = threading.RLock()


def _is_generated_cloud_image(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    notes = str(row.get('notes') or '').strip().lower()
    filename = str(row.get('original_filename') or '').strip().lower()
    if notes.startswith('generated media'):
        return True
    if filename.startswith('cloud_extra_'):
        return True
    return False


def should_pull_cloud_image_to_desktop(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if _is_generated_cloud_image(row):
        return False
    if str(row.get('deleted_at') or '').strip():
        return False
    if str(row.get('purged_at') or '').strip():
        return False
    return True


def _is_metadata_only_microscope_cloud_image(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if not should_pull_cloud_image_to_desktop(row):
        return False
    if str(row.get('image_type') or '').strip().lower() != 'microscope':
        return False
    return not _normalize_cloud_media_key(row.get('storage_path'))


def _is_local_metadata_only_microscope_anchor(image_row: dict | None) -> bool:
    row = dict(image_row or {})
    if not should_pull_cloud_image_to_desktop(row):
        return False
    if str(row.get('image_type') or '').strip().lower() != 'microscope':
        return False
    return bool(str(row.get('cloud_id') or '').strip())


def _cloud_image_storage_excluded_image_ids(observation_id: int | str) -> set[int]:
    """Return the local image ids the user has excluded from cloud image storage.

    Stage 1 canonical persistence for the gallery "Keep image in Sporely Cloud"
    checkbox. Separate from the Artsobs/iNat publication-exclusion set.
    """
    raw = SettingsDB.get_setting(
        _cloud_image_storage_excluded_ids_key(observation_id), '[]'
    )
    try:
        values = json.loads(raw or '[]')
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(values, list):
        return set()
    return {_safe_int(value) for value in values if _safe_int(value) > 0}


def _set_cloud_image_storage_excluded_image_ids(
    observation_id: int | str,
    excluded_ids: set[int] | list[int] | tuple[int, ...] | None,
) -> None:
    """Persist the full cloud image-storage excluded id set for one observation."""
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return
    normalized = sorted({
        _safe_int(value) for value in (excluded_ids or set()) if _safe_int(value) > 0
    })
    with _STORAGE_INTENT_LOCK:
        SettingsDB.set_setting(
            _cloud_image_storage_excluded_ids_key(obs_id),
            json.dumps(normalized),
        )


def _add_cloud_image_storage_excluded_image_id(
    observation_id: int | str,
    image_id: int | str,
) -> None:
    obs_id = _safe_int(observation_id)
    local_image_id = _safe_int(image_id)
    if obs_id <= 0 or local_image_id <= 0:
        return
    with _STORAGE_INTENT_LOCK:
        excluded = _cloud_image_storage_excluded_image_ids(obs_id)
        if local_image_id in excluded:
            return
        excluded.add(local_image_id)
        _set_cloud_image_storage_excluded_image_ids(obs_id, excluded)


def _remove_cloud_image_storage_excluded_image_id(
    observation_id: int | str,
    image_id: int | str,
) -> None:
    obs_id = _safe_int(observation_id)
    local_image_id = _safe_int(image_id)
    if obs_id <= 0 or local_image_id <= 0:
        return
    with _STORAGE_INTENT_LOCK:
        excluded = _cloud_image_storage_excluded_image_ids(obs_id)
        if local_image_id not in excluded:
            return
        excluded.discard(local_image_id)
        _set_cloud_image_storage_excluded_image_ids(obs_id, excluded)


def _cloud_image_storage_intent_initialized_ids(observation_id: int | str) -> set[int]:
    """Local image ids whose cloud byte-storage intent has been recorded.

    Canonical answer to "has default cloud-storage intent already been
    assigned to this local image?". Membership here — never mere absence
    from the excluded set — is what proves a decision exists.
    """
    raw = SettingsDB.get_setting(
        _cloud_image_storage_intent_ledger_key(observation_id), '[]'
    )
    try:
        values = json.loads(raw or '[]')
    except (TypeError, ValueError, json.JSONDecodeError):
        return set()
    if not isinstance(values, list):
        return set()
    return {_safe_int(value) for value in values if _safe_int(value) > 0}


def _set_cloud_image_storage_intent_initialized_ids(
    observation_id: int | str,
    image_ids: set[int] | list[int] | tuple[int, ...] | None,
) -> None:
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return
    normalized = sorted({
        _safe_int(value) for value in (image_ids or set()) if _safe_int(value) > 0
    })
    with _STORAGE_INTENT_LOCK:
        SettingsDB.set_setting(
            _cloud_image_storage_intent_ledger_key(obs_id),
            json.dumps(normalized),
        )


def _mark_cloud_image_storage_intent_initialized(
    observation_id: int | str,
    image_ids: set[int] | list[int] | tuple[int, ...],
) -> None:
    """Record that storage intent now exists for these images.

    Called for explicit checkbox interactions and by the initializer itself.
    Once an image is in the ledger, default seeding never touches it again.
    """
    obs_id = _safe_int(observation_id)
    normalized = {_safe_int(value) for value in (image_ids or ()) if _safe_int(value) > 0}
    if obs_id <= 0 or not normalized:
        return
    with _STORAGE_INTENT_LOCK:
        ledger = _cloud_image_storage_intent_initialized_ids(obs_id)
        if normalized <= ledger:
            return
        _set_cloud_image_storage_intent_initialized_ids(obs_id, ledger | normalized)


def carry_cloud_image_storage_intent(
    source_observation_id: int | str,
    target_observation_id: int | str,
    image_ids,
) -> None:
    """Move images' recorded storage decisions with the images.

    The decisions are kept per observation, so an image moved to another
    observation (``ImageDB.move_images_to_observation``) would otherwise
    arrive undecided there and take the default — unchecked for a
    microscope photo. A recorded decision (checked or unchecked) moves to
    the target; an image with none stays undecided (its target state is
    left alone). Pass only images that actually moved. Local settings only.
    """
    source_id = _safe_int(source_observation_id)
    target_id = _safe_int(target_observation_id)
    ids = {_safe_int(value) for value in (image_ids or ()) if _safe_int(value) > 0}
    if source_id <= 0 or target_id <= 0 or source_id == target_id or not ids:
        return
    with _STORAGE_INTENT_LOCK:
        _carry_cloud_image_storage_intent_locked(source_id, target_id, ids)


def _carry_cloud_image_storage_intent_locked(source_id: int, target_id: int, ids: set[int]) -> None:
    source_ledger = _cloud_image_storage_intent_initialized_ids(source_id)
    source_excluded = _cloud_image_storage_excluded_image_ids(source_id)
    target_ledger = _cloud_image_storage_intent_initialized_ids(target_id)
    target_excluded = _cloud_image_storage_excluded_image_ids(target_id)
    new_target_ledger = set(target_ledger)
    new_target_excluded = set(target_excluded)
    for image_id in ids:
        if image_id in source_ledger:
            new_target_ledger.add(image_id)
        if image_id in source_excluded:
            new_target_excluded.add(image_id)
        elif image_id in source_ledger:
            new_target_excluded.discard(image_id)
    if new_target_excluded != target_excluded:
        _set_cloud_image_storage_excluded_image_ids(target_id, new_target_excluded)
    if new_target_ledger != target_ledger:
        _set_cloud_image_storage_intent_initialized_ids(target_id, new_target_ledger)
    if source_excluded & ids:
        _set_cloud_image_storage_excluded_image_ids(source_id, source_excluded - ids)
    if source_ledger & ids:
        _set_cloud_image_storage_intent_initialized_ids(source_id, source_ledger - ids)


def cloud_image_storage_intent_initialized(
    observation_id: int | str,
    image_id: int | str,
) -> bool:
    """Pure read: has a storage-intent decision been recorded for this image?"""
    obs_id = _safe_int(observation_id)
    local_image_id = _safe_int(image_id)
    if obs_id <= 0 or local_image_id <= 0:
        return False
    return local_image_id in _cloud_image_storage_intent_initialized_ids(obs_id)


def cloud_image_bytes_desired(
    observation_id: int | str,
    image_id: int | str,
    image_row: dict | None = None,
) -> bool:
    """Return whether cloud image *bytes* are desired for this local image.

    Byte-storage intent only. Anchors are separate — metadata-only microscope
    anchor lifecycle is managed by
    ``_ensure_metadata_only_microscope_image_for_public_spores`` and is not
    affected by this predicate.
    """
    obs_id = _safe_int(observation_id)
    local_image_id = _safe_int(image_id)
    if obs_id <= 0 or local_image_id <= 0:
        return False
    excluded = _cloud_image_storage_excluded_image_ids(obs_id)
    return local_image_id not in excluded


def _cloud_image_storage_initialized(observation_id: int | str) -> bool:
    """Derived: every current image row of this observation is in the ledger.

    Kept as a convenience predicate for tests and diagnostics. The legacy
    observation-level sentinel setting is retired and no longer consulted —
    an observation only counts as initialized when each of its images has a
    per-image intent record.
    """
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return False
    conn = get_connection()
    try:
        image_ids = {
            _safe_int(row[0])
            for row in conn.execute(
                "SELECT id FROM images WHERE observation_id = ?", (obs_id,)
            ).fetchall()
        }
    finally:
        conn.close()
    image_ids = {image_id for image_id in image_ids if image_id > 0}
    if not image_ids:
        return True
    return image_ids <= _cloud_image_storage_intent_initialized_ids(obs_id)


def _ensure_cloud_image_storage_intent_initialized(
    observation_id: int | str,
) -> dict:
    """Canonical per-image cloud-storage intent initializer (incremental).

    Records, in the persistent per-image ledger
    (``sporely_cloud_image_storage_intent_ids_<obs>``), that a default (or
    explicit) byte-storage decision exists for each local image. Only ledger
    membership proves a decision — absence from the excluded set proves
    nothing. This replaces the retired observation-level sentinel, which let
    images imported after first initialization masquerade as explicitly
    checked (2026-08-19 mass microscope upload incident).

    Rules applied to images NOT yet in the ledger (images already in the
    ledger are never rewritten):

    * Active tombstone (DELETE_PENDING / DELETED) → excluded + initialized.
      Tombstone lifecycle itself is untouched.
    * Field (and any non-microscope) image → desired by default (left out of
      the excluded set) + initialized. A pre-existing explicit exclusion is
      preserved.
    * Microscope image → excluded + initialized: the user checks the ones
      to keep (and publish). No image is picked per magnification group
      (the retired one-per-objective keeper, owner 2026-11-15).
    * Legacy inference (explicit "checked" history was never stored before
      the ledger existed): a microscope image that is cloud-identified, NOT
      a registered metadata-only anchor, and NOT tombstoned is treated as
      byte-backed / previously selected — it stays desired. Its bytes (or
      its explicit selection) already exist; only an explicit user action
      may exclude it. Registered metadata-only anchors are never inferred as
      byte-backed; they default local-only, and their anchor
      identity/measurements are untouched either way.

    Idempotent, mutates local settings only (never cloud I/O, never image
    rows, never tombstones). Returns a small summary dict for diagnostics:
    ``{"seeded_desired": int, "seeded_excluded": int}``.
    """
    summary = {"seeded_desired": 0, "seeded_excluded": 0}
    obs_id = _safe_int(observation_id)
    if obs_id <= 0:
        return summary

    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        existing_cols = {
            str(info["name"])
            for info in conn.execute("PRAGMA table_info(images)").fetchall()
        }
        if "id" not in existing_cols:
            return summary
        wanted = [
            col
            for col in (
                "id", "image_type", "cloud_id", "synced_at",
                "objective_name",
                "sort_order",
            )
            if col in existing_cols
        ]
        order_bits: list[str] = []
        if "sort_order" in existing_cols:
            order_bits.append("CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END")
            order_bits.append("sort_order")
        order_bits.append("id")
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT {', '.join(wanted)} FROM images WHERE observation_id = ? "
                f"ORDER BY {', '.join(order_bits)}",
                (obs_id,),
            ).fetchall()
        ]
    finally:
        conn.close()

    if not rows:
        return summary

    ledger = _cloud_image_storage_intent_initialized_ids(obs_id)
    uninitialized_ids = {
        _safe_int(row.get("id"))
        for row in rows
        if _safe_int(row.get("id")) > 0 and _safe_int(row.get("id")) not in ledger
    }
    if not uninitialized_ids:
        return summary

    excluded = _cloud_image_storage_excluded_image_ids(obs_id)
    anchor_ids = _cloud_metadata_only_image_ids(obs_id)
    new_excluded: set[int] = set(excluded)
    new_ledger: set[int] = set(ledger)

    tombstone_cache: dict[str, bool] = {}

    def _has_active_tombstone(row: dict) -> bool:
        cloud_id = str(row.get("cloud_id") or '').strip()
        if not cloud_id:
            return False
        if cloud_id not in tombstone_cache:
            tombstone_cache[cloud_id] = bool(
                ImageDB.get_image_tombstone_by_deleted_cloud_id(cloud_id)
            )
        return tombstone_cache[cloud_id]

    def _inferred_byte_backed(row: dict) -> bool:
        # Legacy inference documented in the docstring: before the ledger,
        # cloud identity (minus registered anchors and tombstones) is the
        # only durable evidence of a prior byte upload / explicit selection.
        image_id = _safe_int(row.get("id"))
        return (
            bool(str(row.get("cloud_id") or '').strip())
            and image_id not in anchor_ids
            and not _has_active_tombstone(row)
        )

    for row in rows:
        image_id = _safe_int(row.get("id"))
        if image_id <= 0 or image_id not in uninitialized_ids:
            continue
        new_ledger.add(image_id)
        if _has_active_tombstone(row):
            new_excluded.add(image_id)
            continue
        image_type = str(row.get("image_type") or '').strip().lower()
        if image_type != "microscope":
            continue
        if _inferred_byte_backed(row):
            # Bytes/selection already exist on cloud — never default an
            # uploaded image into the excluded set.
            continue
        new_excluded.add(image_id)

    # Apply only the defaults, and only to images still undecided now: a
    # checkbox (or another seeding) may have recorded a decision since the
    # reads above, and a whole-set write would undo it.
    default_excluded = new_excluded - excluded
    with _STORAGE_INTENT_LOCK:
        current_ledger = _cloud_image_storage_intent_initialized_ids(obs_id)
        current_excluded = _cloud_image_storage_excluded_image_ids(obs_id)
        seeded = (new_ledger - ledger) - current_ledger
        seeded_excluded = seeded & default_excluded
        if seeded_excluded - current_excluded:
            _set_cloud_image_storage_excluded_image_ids(
                obs_id, current_excluded | seeded_excluded
            )
        if seeded:
            _set_cloud_image_storage_intent_initialized_ids(
                obs_id, current_ledger | seeded
            )
        # Classified by the resulting set: an image already excluded before
        # its first decision counts as seeded excluded.
        seeded_excluded = seeded & (current_excluded | seeded_excluded)

    summary["seeded_excluded"] = len(seeded_excluded)
    summary["seeded_desired"] = len(seeded) - summary["seeded_excluded"]
    return summary


def _initialize_cloud_image_storage_desired_state_for_observation(
    observation_id: int | str,
) -> None:
    """Back-compat alias for the canonical per-image intent initializer.

    Existing call sites (gallery load, measure gallery load, sync-time
    prerequisite) keep this name; all semantics live in
    :func:`_ensure_cloud_image_storage_intent_initialized`.
    """
    _ensure_cloud_image_storage_intent_initialized(observation_id)


def set_image_cloud_selected(image_id: int, selected: bool) -> dict | None:
    """Apply one checkbox change to the image's canonical cloud lifecycle.

    Stage 1: also mirrors the checkbox into the cloud-storage-desired
    excluded set (``sporely_cloud_image_storage_excluded_ids_<obs>``) so the
    boundary byte gate sees a consistent view without depending on any UI
    layer. Tombstone / delete-pending / restore transitions continue to run
    as before.
    """
    image_id = _safe_int(image_id)
    if image_id <= 0:
        return None
    image = ImageDB.get_image(image_id)
    if not image:
        return None

    cloud_id = str(image.get("cloud_id") or "").strip()
    tombstone = (
        ImageDB.get_image_tombstone_by_deleted_cloud_id(cloud_id)
        if cloud_id
        else None
    )
    previous_state = derive_image_cloud_state(
        cloud_id,
        tombstone,
        metadata_only=(
            image_id in _cloud_metadata_only_image_ids(image.get("observation_id"))
        ),
    )
    next_state = previous_state
    next_cloud_id = cloud_id or None
    action = "none"
    observation_id = _safe_int(image.get("observation_id"))

    if not selected and previous_state == CLOUD_IMAGE_STATE_UPLOADED:
        queued_cloud_id = ImageDB.queue_image_tombstone_for_local_image(image_id)
        if queued_cloud_id:
            next_state = CLOUD_IMAGE_STATE_DELETE_PENDING
            next_cloud_id = str(queued_cloud_id)
            action = "delete_queued"
    elif selected and previous_state == CLOUD_IMAGE_STATE_DELETE_PENDING:
        if ImageDB.clear_image_tombstone_by_deleted_cloud_id(cloud_id):
            next_state = CLOUD_IMAGE_STATE_UPLOADED
            action = "delete_cancelled"
    elif selected and previous_state == CLOUD_IMAGE_STATE_DELETED:
        remember_explicit_image_restore_source(image_id, cloud_id)
        if ImageDB.clear_image_cloud_sync_state(image_id):
            next_state = CLOUD_IMAGE_STATE_NONE
            next_cloud_id = None
            action = "restore_queued"
            mark_observation_media_dirty(observation_id)
    elif selected and previous_state == CLOUD_IMAGE_STATE_NONE:
        mark_observation_media_dirty(observation_id)
        action = "upload_queued"
    elif selected and previous_state == CLOUD_IMAGE_STATE_METADATA_ONLY:
        mark_observation_media_dirty(observation_id)
        action = "upload_queued"

    # Stage 1: keep the cloud-storage-desired excluded set aligned with the
    # checkbox. Only mutate when the observation is known; a checkbox without
    # an observation cannot affect the byte gate anyway.
    if observation_id > 0:
        # One lock for both steps, so default seeding cannot slip in between
        # and re-exclude a photo the user just checked.
        with _STORAGE_INTENT_LOCK:
            if selected:
                _remove_cloud_image_storage_excluded_image_id(observation_id, image_id)
            else:
                _add_cloud_image_storage_excluded_image_id(observation_id, image_id)
            # An explicit user decision IS storage intent — record it in the
            # per-image ledger so default seeding can never override it.
            _mark_cloud_image_storage_intent_initialized(observation_id, [image_id])

    return {
        "image_id": image_id,
        "observation_id": observation_id or None,
        "selected": bool(selected),
        "previous_state": previous_state,
        "cloud_state": next_state,
        "cloud_id": next_cloud_id,
        "action": action,
    }


def microscope_image_requires_public_spore_anchor(image_id: int | None) -> bool:
    """Return whether one local image must retain a public metadata anchor.

    This is the shared local eligibility predicate used by checkbox/tombstone
    handling, normal sync, and the explicit mosaic backfill. It intentionally
    mirrors the established public mosaic measurement gate: the observation's
    spore data is public, the parent is a microscope image, both dimensions are
    present, and the measurement category is a spore category.
    """
    local_image_id = _safe_int(image_id)
    if local_image_id <= 0:
        return False
    conn = get_connection()
    try:
        cursor = conn.execute(
            """
            SELECT i.image_type, o.spore_data_visibility,
                   m.length_um, m.width_um, m.measurement_type
            FROM images i
            JOIN observations o ON o.id = i.observation_id
            LEFT JOIN spore_measurements m ON m.image_id = i.id
            WHERE i.id = ?
            """,
            (local_image_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            return False
        if str(rows[0][0] or '') != 'microscope':
            return False
        if str(rows[0][1] or 'public').lower() != 'public':
            return False
        return any(
            measurement_qualifies_for_public_spore_anchor({
                'length_um': row[2],
                'width_um': row[3],
                'measurement_type': row[4],
            })
            for row in rows
        )
    finally:
        conn.close()


def microscope_image_requires_owner_sync_anchor(image_id: int | None) -> bool:
    """Whether one local microscope image must have a cloud parent for its
    owner's own measurements to sync between the owner's devices.

    Deliberately independent of `microscope_image_requires_public_spore_anchor`:
    it ignores observation visibility and measurement type, because
    cross-device sync of the owner's data is not publication. It mirrors the
    measurement pusher's own eligibility — every measurement row on a
    microscope image with a cloud parent is pushed — so no measurement is
    left without a parent. Whether such a parent may ever be public is decided
    by the server (`metadata_purpose` + verified public child data), never by
    this predicate.
    """
    local_image_id = _safe_int(image_id)
    if local_image_id <= 0:
        return False
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT i.image_type,
                   EXISTS (SELECT 1 FROM spore_measurements m WHERE m.image_id = i.id)
            FROM images i
            WHERE i.id = ?
            """,
            (local_image_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    return bool(row) and str(row[0] or '') == 'microscope' and bool(row[1])


def measurement_qualifies_for_public_spore_anchor(measurement: dict | None) -> bool:
    """Pure eligibility predicate shared by sync and read-only incident audit."""
    row = dict(measurement or {})
    if row.get('length_um') is None or row.get('width_um') is None:
        return False
    measurement_type = str(row.get('measurement_type') or '').lower()
    return measurement_type in {'', 'manual', 'spore', 'spores'}


def _cloud_explicit_media_upload_selection(observation_id: int | None) -> set[int]:
    """Return image ids the user has kept in Sporely Cloud image storage.

    Stage 1: this is now the cloud-storage desired selection, read from the
    dedicated ``sporely_cloud_image_storage_excluded_ids_<obs>`` setting.
    Legacy ``artsobs_publish_excluded_image_ids_<obs>`` values are ignored
    here — that key is publication-only.
    """
    local_observation_id = _safe_int(observation_id)
    if local_observation_id <= 0:
        return set()
    conn = get_connection()
    try:
        all_ids = {
            _safe_int(row[0])
            for row in conn.execute(
                "SELECT id FROM images WHERE observation_id = ?",
                (local_observation_id,),
            ).fetchall()
            if _safe_int(row[0]) > 0
        }
        setting_key = _cloud_image_storage_excluded_ids_key(local_observation_id)
        try:
            setting_row = conn.execute(
                "SELECT value FROM settings WHERE key = ?",
                (setting_key,),
            ).fetchone()
        except sqlite3.OperationalError:
            setting_row = None
        excluded: set[int] = set()
        if setting_row:
            try:
                values = json.loads(setting_row[0] or "[]")
                if isinstance(values, list):
                    excluded = {_safe_int(value) for value in values}
            except (TypeError, ValueError, json.JSONDecodeError):
                excluded = set()
        return all_ids - excluded
    finally:
        conn.close()


def _local_image_is_metadata_only_anchor_source(img: dict | None) -> bool:
    """A local microscope image whose bytes are deliberately not sent: its
    cloud row is the metadata-only anchor this desktop creates (without a
    capture time)."""
    row = dict(img or {})
    if str(row.get('image_type') or '').strip().lower() != 'microscope':
        return False
    obs_id, image_id = row.get('observation_id'), row.get('id')
    if not obs_id or not image_id:
        return False
    try:
        return not cloud_image_bytes_desired(obs_id, image_id, row)
    except Exception:
        return False
