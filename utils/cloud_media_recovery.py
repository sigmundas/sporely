"""Narrow Phase 2 recovery for the audited Mycena cloud-media incident."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from utils.cloud_media_audit import (
    AuditFilters, CloudInventoryIncompleteError, ReadOnlyCloudAuditReader,
    build_audit_report, load_local_inventory,
)
from utils.cloud_sync import (
    _file_content_signature, _normalize_cloud_media_key,
    _sanitize_original_storage_filename, _store_cloud_image_file_signature,
)
from utils.original_sync_policy import (
    is_full_resolution_original_sync_enabled, resolve_full_original_upload_source,
)


AUDITED_OBSERVATION_ID = 704
AUDITED_TARGET_IMAGE_IDS = tuple(range(4987, 4999))
MISSING_STATE = "cloud_row_missing_local_file_available"


class RecoveryError(RuntimeError):
    """Raised when the narrow recovery cannot proceed safely."""


@dataclass(frozen=True)
class RecoveryItem:
    local_image_id: int
    image_type: str
    sort_order: int
    measurement_count: int
    status: str
    cloud_image_id: str | None = None


@dataclass(frozen=True)
class RecoveryPlan:
    observation_id: int
    cloud_observation_id: str
    inventory_complete: bool
    local_image_count: int
    healthy_count: int
    unmatched_cloud_count: int
    items: tuple[RecoveryItem, ...]
    inventory: dict[str, Any]
    report: dict[str, Any]


def _readable_source(row: dict[str, Any]) -> str | None:
    for value in (row.get("filepath"), row.get("original_filepath")):
        text = str(value or "").strip()
        if not text:
            continue
        path = Path(text)
        try:
            if path.is_file() and os.access(path, os.R_OK):
                with path.open("rb") as handle:
                    handle.read(1)
                return text
        except OSError:
            pass
    return None


def recovery_storage_key(
    user_id: str,
    cloud_observation_id: str,
    local_image_id: int,
    source: str,
) -> str:
    """Return the stable, filename-safe key used by row-less recovery uploads."""
    safe_name = _sanitize_original_storage_filename(source)
    key = _normalize_cloud_media_key(
        f"{str(user_id).strip()}/{str(cloud_observation_id).strip()}"
        f"/recovery/{int(local_image_id)}_{safe_name}"
    )
    if not key:
        raise RecoveryError("Could not construct deterministic recovery storage key")
    return key


def build_recovery_plan(
    db_path: Path,
    reader: ReadOnlyCloudAuditReader,
    observation_id: int,
    *,
    audited_observation_id: int = AUDITED_OBSERVATION_ID,
    audited_target_ids: Iterable[int] = AUDITED_TARGET_IMAGE_IDS,
) -> RecoveryPlan:
    """Build a fully read-only plan and reject any drift or ambiguity."""
    if int(observation_id) != int(audited_observation_id):
        raise RecoveryError(
            f"This targeted tool only permits audited local observation {audited_observation_id}"
        )
    inventory = load_local_inventory(Path(db_path), AuditFilters(observation_id=int(observation_id)))
    observations = list(inventory.get("observations") or [])
    if len(observations) != 1:
        raise RecoveryError("The exact local observation was not found")
    cloud_observation_id = str(observations[0].get("cloud_id") or "").strip()
    if not cloud_observation_id:
        raise RecoveryError("The local observation has no cloud link")
    report = build_audit_report(inventory, reader, check_storage=True)
    if not report.get("cloud_inventory_complete"):
        raise CloudInventoryIncompleteError("Cloud inventory is incomplete")

    expected = tuple(sorted({int(value) for value in audited_target_ids}))
    records = list(report.get("images") or [])
    by_local = {int(row["local_image_id"]): row for row in records if row.get("local_image_id")}
    local_rows = {int(row["id"]): row for row in inventory.get("images") or []}
    items: list[RecoveryItem] = []
    errors: list[str] = []
    for image_id in expected:
        record = by_local.get(image_id)
        local = local_rows.get(image_id)
        if not record or not local or int(local.get("observation_id") or 0) != int(observation_id):
            errors.append(f"image {image_id}: audited local row is absent")
            continue
        flags = set(record.get("flags") or [])
        if record.get("has_local_tombstone"):
            errors.append(f"image {image_id}: intentional tombstone is pending")
        if flags & {"duplicate_desktop_id", "duplicate_observation_type_order", "multiple_fallback_candidates"}:
            errors.append(f"image {image_id}: duplicate or ambiguous cloud identity")
        if record.get("possible_match_ids") or record.get("duplicate_cloud_ids"):
            errors.append(f"image {image_id}: fallback or duplicate candidates exist")
        if not _readable_source(local):
            errors.append(f"image {image_id}: local source is unreadable")
        cloud_id = str(record.get("cloud_image_id") or "").strip() or None
        state = str(record.get("primary_state") or "")
        if state == MISSING_STATE and cloud_id is None:
            status = "ready_create"
        elif cloud_id and record.get("match_method") in {"desktop_id", "cloud_id"}:
            if record.get("derivative_status") == "exists":
                status = "already_complete"
            elif not str((record.get("cloud") or {}).get("storage_path") or "").strip():
                status = "resume_existing_row"
            else:
                errors.append(f"image {image_id}: existing derivative could not be confirmed")
                continue
        else:
            errors.append(f"image {image_id}: unexpected state {state or 'unknown'}")
            continue
        items.append(RecoveryItem(
            image_id, str(local.get("image_type") or ""), int(local.get("sort_order") or 0),
            int(record.get("local_measurement_count") or 0), status, cloud_id,
        ))
    unexpected = sorted(
        int(row["local_image_id"]) for row in records
        if row.get("primary_state") == MISSING_STATE and row.get("local_image_id")
        and int(row["local_image_id"]) not in expected
    )
    if unexpected:
        errors.append(f"unexpected missing image rows: {','.join(map(str, unexpected))}")
    if errors:
        raise RecoveryError("Recovery preflight failed: " + "; ".join(errors))
    items.sort(key=lambda item: (item.sort_order, item.local_image_id))
    return RecoveryPlan(
        int(observation_id), cloud_observation_id, True, len(local_rows),
        sum(row.get("primary_state") == "healthy_cloud_image" for row in records),
        sum(row.get("primary_state") == "cloud_row_without_local_match" for row in records),
        tuple(items), inventory, report,
    )


class SQLiteRecoveryWriter:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def link_completed(self, observation_id: int, image_id: int, cloud_image_id: str, signature: str) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT observation_id, cloud_id FROM images WHERE id = ?", (int(image_id),),
            ).fetchone()
            if row is None or int(row[0]) != int(observation_id):
                raise RecoveryError(f"Local image {image_id} changed during recovery")
            existing = str(row[1] or "").strip()
            if existing and existing != str(cloud_image_id):
                raise RecoveryError(f"Local image {image_id} acquired a conflicting cloud link")
            conn.execute(
                "UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?",
                (str(cloud_image_id), datetime.now(timezone.utc).isoformat(), int(image_id)),
            )
            conn.commit()
        finally:
            conn.close()
        _store_cloud_image_file_signature(observation_id, image_id, signature)


class CloudRecoveryAdapter:
    def __init__(self, client: Any):
        self.client = client
        self.reader = ReadOnlyCloudAuditReader(client)
        self._upload_results: dict[str, dict[str, Any]] = {}
        profile = dict(client.fetch_cloud_plan_profile() or {})
        self._upload_meta = {
            "upload_mode": "full",
            "cloud_plan": str(profile.get("cloud_plan") or "free"),
        }

    def recovery_storage_key(
        self, source: str, image: dict[str, Any], cloud_observation_id: str,
    ) -> str:
        return recovery_storage_key(
            self.client.user_id, cloud_observation_id, int(image["id"]), source,
        )

    def create_row_with_storage(
        self,
        image: dict[str, Any],
        cloud_observation_id: str,
        storage_path: str,
        upload_ref: str,
    ) -> str:
        payload = dict(image)
        payload.update(self._upload_results.get(str(upload_ref), {}))
        return str(self.client.push_image_metadata(payload, cloud_observation_id, storage_path))

    def upload_derivative(
        self,
        source: str,
        image: dict[str, Any],
        cloud_observation_id: str,
        upload_ref: str,
        *,
        storage_path: str | None = None,
    ) -> str:
        result_meta: dict[str, Any] = {}
        result = self.client.upload_image_file(
            source, cloud_observation_id, upload_ref, storage_path=storage_path,
            upload_meta={
                **self._upload_meta,
                "observation_id": image.get("observation_id"), "image_id": image.get("id"),
            },
            result_meta=result_meta,
            observation_id=image.get("observation_id"),
            image_id=image.get("id"),
            recovery_authorized=True,
        )
        if not result:
            raise RecoveryError(f"Image {image.get('id')}: derivative upload returned no key")
        self._upload_results[str(upload_ref)] = result_meta
        return str(result)

    def attach_derivative(self, cloud_image_id: str, key: str) -> None:
        self.client.set_image_storage_path(
            cloud_image_id, key,
            upload_meta=self._upload_results.get(str(cloud_image_id), {}),
        )

    def derivative_exists(self, key: str) -> bool:
        return self.reader.check_storage(key) == "exists"

    def verify_row(
        self,
        cloud_image_id: str,
        cloud_observation_id: str,
        local_image_id: int,
        storage_path: str,
    ) -> None:
        rows = self.client.get_read_only(
            f"observation_images?id=eq.{cloud_image_id}&select="
            "id,observation_id,desktop_id,storage_path&limit=2"
        )
        if len(rows) != 1:
            raise RecoveryError("Created cloud image row could not be uniquely confirmed")
        row = dict(rows[0] or {})
        if (
            str(row.get("observation_id") or "") != str(cloud_observation_id)
            or int(row.get("desktop_id") or 0) != int(local_image_id)
            or _normalize_cloud_media_key(row.get("storage_path"))
            != _normalize_cloud_media_key(storage_path)
        ):
            raise RecoveryError("Created cloud image row did not match the recovery identity")

    def upload_original_if_enabled(self, image: dict[str, Any], cloud_observation_id: str, cloud_image_id: str) -> None:
        if not is_full_resolution_original_sync_enabled():
            return
        source = resolve_full_original_upload_source(image)
        if not source:
            return
        key = self.client.upload_original_image_file(
            source["source_path"], cloud_observation_id, cloud_image_id,
            upload_meta={"observation_id": image.get("observation_id"), "image_id": image.get("id")},
            observation_id=image.get("observation_id"),
            image_id=image.get("id"),
            recovery_authorized=True,
        )
        if key:
            self.client.set_image_original_storage_path(cloud_image_id, key)

    def upload_measurement(self, measurement: dict[str, Any], cloud_image_id: str) -> str:
        return str(self.client.push_measurement(measurement, cloud_image_id))

    def measurement_count(self, cloud_image_id: str) -> int:
        collection = self.reader.fetch_measurements([str(cloud_image_id)])
        if not collection.complete:
            raise CloudInventoryIncompleteError("Measurement verification inventory is incomplete")
        return sum(str(row.get("image_id") or "") == str(cloud_image_id) for row in collection.rows)


def apply_recovery(plan: RecoveryPlan, cloud: Any, local_writer: Any) -> list[dict[str, Any]]:
    """Recover one image at a time; the caller must perform a fresh preflight first."""
    local_rows = {int(row["id"]): dict(row) for row in plan.inventory.get("images") or []}
    measurements: dict[int, list[dict[str, Any]]] = {}
    for row in plan.inventory.get("measurements") or []:
        measurements.setdefault(int(row.get("image_id") or 0), []).append(dict(row))
    results: list[dict[str, Any]] = []
    for item in plan.items:
        image = local_rows[item.local_image_id]
        source = _readable_source(image)
        if not source:
            results.append({"local_image_id": item.local_image_id, "status": "failed", "error": "source unreadable"})
            continue
        cloud_id = item.cloud_image_id
        uploaded_key: str | None = None
        try:
            if item.status == "already_complete":
                if not cloud_id:
                    raise RecoveryError("completed cloud row has no identity")
                signature = _file_content_signature(source)
                if not signature:
                    raise RecoveryError("source signature could not be calculated")
                local_writer.link_completed(plan.observation_id, item.local_image_id, cloud_id, signature)
                cloud.upload_original_if_enabled(image, plan.cloud_observation_id, cloud_id)
                for measurement in sorted(measurements.get(item.local_image_id, []), key=lambda row: int(row.get("id") or 0)):
                    cloud.upload_measurement(measurement, cloud_id)
                expected_measurements = len(measurements.get(item.local_image_id, []))
                if cloud.measurement_count(cloud_id) != expected_measurements:
                    raise RecoveryError("cloud measurement count mismatch after upload")
                results.append({"local_image_id": item.local_image_id, "cloud_image_id": cloud_id, "status": "already_complete"})
                continue
            if item.status == "ready_create":
                upload_ref = str(item.local_image_id)
                intended_key = cloud.recovery_storage_key(source, image, plan.cloud_observation_id)
                uploaded_key = cloud.upload_derivative(
                    source, image, plan.cloud_observation_id, upload_ref,
                    storage_path=intended_key,
                )
                if _normalize_cloud_media_key(uploaded_key) != _normalize_cloud_media_key(intended_key):
                    raise RecoveryError("Derivative upload returned an unexpected recovery key")
                if not cloud.derivative_exists(uploaded_key):
                    raise RecoveryError("uploaded derivative could not be verified")
                cloud_id = cloud.create_row_with_storage(
                    image, plan.cloud_observation_id, uploaded_key, upload_ref,
                )
                cloud.verify_row(
                    cloud_id, plan.cloud_observation_id, item.local_image_id, uploaded_key,
                )
            else:
                if not cloud_id:
                    raise RecoveryError("existing recovery row has no identity")
                uploaded_key = cloud.upload_derivative(
                    source, image, plan.cloud_observation_id, cloud_id,
                )
                cloud.attach_derivative(cloud_id, uploaded_key)
                if not cloud.derivative_exists(uploaded_key):
                    raise RecoveryError("uploaded derivative could not be verified")
            signature = _file_content_signature(source)
            if not signature:
                raise RecoveryError("source signature could not be calculated")
            local_writer.link_completed(plan.observation_id, item.local_image_id, cloud_id, signature)
            cloud.upload_original_if_enabled(image, plan.cloud_observation_id, cloud_id)
            for measurement in sorted(measurements.get(item.local_image_id, []), key=lambda row: int(row.get("id") or 0)):
                cloud.upload_measurement(measurement, cloud_id)
            expected_measurements = len(measurements.get(item.local_image_id, []))
            if cloud.measurement_count(cloud_id) != expected_measurements:
                raise RecoveryError("cloud measurement count mismatch after upload")
            results.append({"local_image_id": item.local_image_id, "cloud_image_id": cloud_id, "status": "recovered"})
        except Exception as exc:
            result = {"local_image_id": item.local_image_id, "cloud_image_id": cloud_id, "status": "failed", "error": str(exc)}
            if uploaded_key and not cloud_id:
                result["partial_state"] = "derivative_uploaded_row_unconfirmed"
            results.append(result)
    return results


def verify_recovery(
    db_path: Path,
    reader: ReadOnlyCloudAuditReader,
    observation_id: int,
    *,
    baseline_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inventory = load_local_inventory(Path(db_path), AuditFilters(observation_id=int(observation_id)))
    report = build_audit_report(inventory, reader, check_storage=True)
    failures = []
    for row in report["images"]:
        local_id = row.get("local_image_id")
        if local_id and (
            row.get("primary_state") != "healthy_cloud_image"
            or row.get("derivative_status") != "exists"
            or row.get("duplicate_cloud_ids")
            or "duplicate_desktop_id" in set(row.get("flags") or [])
        ):
            failures.append(row)
        elif local_id in AUDITED_TARGET_IMAGE_IDS and (
            row.get("cloud_measurement_count") != row.get("local_measurement_count")
        ):
            failures.append(row)
        elif local_id is None and (
            row.get("derivative_status") != "exists" or row.get("duplicate_cloud_ids")
        ):
            failures.append(row)
    if baseline_report:
        before = {
            str(row.get("cloud_image_id")): row.get("cloud")
            for row in baseline_report.get("images") or []
            if row.get("cloud_image_id") and row.get("local_image_id") not in AUDITED_TARGET_IMAGE_IDS
        }
        after = {
            str(row.get("cloud_image_id")): row.get("cloud")
            for row in report.get("images") or [] if row.get("cloud_image_id")
        }
        for cloud_id, cloud_row in before.items():
            if after.get(cloud_id) != cloud_row:
                failures.append({"cloud_image_id": cloud_id, "reason": "non-target cloud row changed"})
    return {"ok": not failures, "failure_count": len(failures), "report": report}
