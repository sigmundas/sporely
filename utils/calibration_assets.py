"""Calibration asset provenance helpers.

This module keeps the calibration asset vocabulary and extraction rules in one
place so the calibration dialog, database layer, and backfill logic can share
the same normalization rules.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

from utils.heic_converter import build_local_image_provenance, guess_local_image_mime_type

_ASSET_NAMESPACE = uuid.UUID("8f4b7f68-c8d7-4e84-9010-7c7a9f6e1d20")

_VALID_ROLES = {
    "source_photo",
    "working_photo",
    "calibration_crop",
    "reference_cache",
    "overlay",
    "debug_artifact",
    "plot",
    "thumbnail",
    "spore_crop",
}

_VALID_FILE_PURPOSES = {
    "field",
    "microscope",
    "calibration",
    "reference",
    "plot",
    "thumbnail",
    "spore_crop",
    "cache",
}

_VALID_SOURCE_ROLES = {
    "import_source",
    "local_canonical",
    "converted_local",
    "cloud_derivative",
    "cloud_recovery_cache",
    "generated_artifact",
}

_ROLE_DEFAULT_PURPOSE = {
    "source_photo": "calibration",
    "working_photo": "calibration",
    "calibration_crop": "calibration",
    "reference_cache": "cache",
    "overlay": "calibration",
    "debug_artifact": "calibration",
    "plot": "plot",
    "thumbnail": "thumbnail",
    "spore_crop": "spore_crop",
}

_ROLE_DEFAULT_SOURCE_ROLE = {
    "source_photo": "import_source",
    "working_photo": None,
    "calibration_crop": "generated_artifact",
    "reference_cache": "cloud_recovery_cache",
    "overlay": "generated_artifact",
    "debug_artifact": "generated_artifact",
    "plot": "generated_artifact",
    "thumbnail": "generated_artifact",
    "spore_crop": "generated_artifact",
}


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("Expected an integer or None")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Expected an integer or None") from exc


def normalize_calibration_asset_role(role: Any) -> str:
    normalized = _normalize_slug(role)
    if normalized not in _VALID_ROLES:
        raise ValueError(f"Unknown calibration asset role: {role!r}")
    return normalized


def normalize_calibration_asset_source_role(source_role: Any) -> str | None:
    normalized = _normalize_slug(source_role)
    if not normalized:
        return None
    if normalized not in _VALID_SOURCE_ROLES:
        raise ValueError(f"Unknown calibration asset source_role: {source_role!r}")
    return normalized


def normalize_calibration_asset_file_purpose(file_purpose: Any) -> str | None:
    normalized = _normalize_slug(file_purpose)
    if not normalized:
        return None
    if normalized not in _VALID_FILE_PURPOSES:
        raise ValueError(f"Unknown calibration asset file_purpose: {file_purpose!r}")
    return normalized


def _stable_metadata_digest(metadata: dict[str, Any] | None) -> str | None:
    if not metadata:
        return None
    payload = json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_asset_uuid(
    *,
    calibration_uuid: str | None,
    calibration_id: int | None,
    role: str,
    local_path: str | None,
    original_path: str | None,
    asset_index: int | None,
) -> str:
    key = {
        "calibration_uuid": _normalize_text(calibration_uuid) or "",
        "calibration_id": int(calibration_id or 0),
        "role": normalize_calibration_asset_role(role),
        "local_path": _normalize_text(local_path) or "",
        "original_path": _normalize_text(original_path) or "",
        "asset_index": int(asset_index) if asset_index is not None else None,
    }
    payload = json.dumps(key, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(_ASSET_NAMESPACE, payload))


def _describe_local_file(path_value: str | Path | None) -> dict[str, Any]:
    path_text = _normalize_text(path_value)
    if not path_text:
        return {
            "mime_type": None,
            "width": None,
            "height": None,
            "bytes": None,
            "sha256": None,
        }

    path = Path(path_text)
    info = {
        "mime_type": guess_local_image_mime_type(path),
        "width": None,
        "height": None,
        "bytes": None,
        "sha256": None,
    }
    try:
        if path.exists() and path.is_file():
            info["bytes"] = int(path.stat().st_size)
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8192), b""):
                    digest.update(chunk)
            info["sha256"] = digest.hexdigest()
            try:
                from PIL import Image

                with Image.open(path) as image:
                    info["width"] = int(image.width)
                    info["height"] = int(image.height)
            except Exception:
                pass
    except Exception:
        pass
    return info


def _build_local_provenance_fields(
    *,
    source_path: str | Path | None,
    local_path: str | Path | None,
    file_purpose: str | None,
) -> dict[str, Any]:
    source_text = _normalize_text(source_path)
    local_text = _normalize_text(local_path) or source_text
    provenance = build_local_image_provenance(
        source_text or local_text,
        local_text or source_text,
        image_type=file_purpose,
    )
    return {
        "source_role": provenance.get("source_role"),
        "file_purpose": provenance.get("file_purpose"),
        "original_mime_type": provenance.get("original_mime_type"),
        "working_mime_type": provenance.get("working_mime_type"),
    }


def build_calibration_asset_payload(
    *,
    role: Any,
    calibration_id: Any = None,
    calibration_uuid: Any = None,
    local_path: str | Path | None = None,
    source_path: str | Path | None = None,
    original_path: str | Path | None = None,
    cloud_storage_path: str | Path | None = None,
    file_purpose: Any = None,
    source_role: Any = None,
    asset_index: Any = None,
    metadata: dict[str, Any] | None = None,
    asset_uuid: Any = None,
) -> dict[str, Any]:
    """Build a normalized calibration asset payload."""
    normalized_role = normalize_calibration_asset_role(role)
    normalized_file_purpose = normalize_calibration_asset_file_purpose(
        file_purpose if file_purpose is not None else _ROLE_DEFAULT_PURPOSE[normalized_role]
    )
    normalized_source_role = normalize_calibration_asset_source_role(
        source_role if source_role is not None else _ROLE_DEFAULT_SOURCE_ROLE[normalized_role]
    )
    normalized_metadata = dict(metadata or {})

    source_text = _normalize_text(source_path) or _normalize_text(original_path)
    local_text = _normalize_text(local_path)
    original_text = _normalize_text(original_path)
    cloud_text = _normalize_text(cloud_storage_path)
    index_value = _coerce_optional_int(asset_index)
    calibration_id_value = _coerce_optional_int(calibration_id)
    calibration_uuid_text = _normalize_text(calibration_uuid)

    if asset_uuid is not None:
        normalized_asset_uuid = _normalize_text(asset_uuid)
        if not normalized_asset_uuid:
            raise ValueError("asset_uuid must be a non-empty string when provided")
    else:
        normalized_asset_uuid = _build_asset_uuid(
            calibration_uuid=calibration_uuid_text,
            calibration_id=calibration_id_value,
            role=normalized_role,
            local_path=local_text,
            original_path=original_text or source_text,
            asset_index=index_value,
        )

    provenance = _build_local_provenance_fields(
        source_path=source_text,
        local_path=local_text,
        file_purpose=normalized_file_purpose,
    )

    file_info = _describe_local_file(local_text or source_text or original_text)
    if normalized_role in {"calibration_crop", "overlay", "debug_artifact", "plot", "thumbnail", "spore_crop"} and not local_text:
        file_info = {
            "mime_type": None,
            "width": None,
            "height": None,
            "bytes": None,
            "sha256": None,
        }

    if normalized_role == "source_photo":
        provenance["source_role"] = "import_source"

    if normalized_role == "reference_cache":
        provenance["source_role"] = "cloud_recovery_cache"

    if normalized_role not in {"source_photo", "reference_cache"} and normalized_source_role:
        provenance["source_role"] = normalized_source_role

    mime_type = file_info["mime_type"] or provenance.get("working_mime_type") or provenance.get("original_mime_type")
    if normalized_role in {"calibration_crop", "overlay", "debug_artifact", "plot", "thumbnail", "spore_crop"} and not local_text:
        mime_type = None
    original_path_value = original_text if normalized_role == "source_photo" else (original_text or source_text)
    if normalized_role == "reference_cache" and not original_path_value:
        original_path_value = source_text

    metadata_json = None
    if normalized_metadata or provenance.get("original_mime_type") or provenance.get("working_mime_type") or source_text or local_text:
        metadata_json = {
            **normalized_metadata,
            "source_path": source_text,
            "working_path": local_text,
            "original_mime_type": provenance.get("original_mime_type"),
            "working_mime_type": provenance.get("working_mime_type"),
        }
        if index_value is not None and "asset_index" not in metadata_json:
            metadata_json["asset_index"] = index_value
        metadata_json.pop("metadata_sha256", None)
        metadata_json["metadata_sha256"] = _stable_metadata_digest(metadata_json)

    payload = {
        "asset_uuid": normalized_asset_uuid,
        "calibration_id": calibration_id_value,
        "calibration_uuid": calibration_uuid_text,
        "role": normalized_role,
        "source_role": provenance.get("source_role"),
        "file_purpose": normalized_file_purpose,
        "local_path": local_text,
        "original_path": original_path_value,
        "cloud_storage_path": cloud_text,
        "mime_type": mime_type,
        "width": file_info["width"],
        "height": file_info["height"],
        "bytes": file_info["bytes"],
        "sha256": file_info["sha256"],
        "metadata_json": metadata_json,
    }

    if normalized_role == "source_photo" and not payload["original_path"]:
        payload["original_path"] = source_text
    if normalized_role == "reference_cache" and not payload["original_path"]:
        payload["original_path"] = source_text

    return payload


def build_reference_cache_asset_payload(
    *,
    calibration_id: Any = None,
    calibration_uuid: Any = None,
    cache_path: str | Path,
    image_storage_path: str | Path | None = None,
    original_path: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
    asset_uuid: Any = None,
) -> dict[str, Any]:
    """Build a payload for a downloaded calibration reference cache file."""
    merged_metadata = dict(metadata or {})
    if image_storage_path:
        merged_metadata.setdefault("image_storage_path", _normalize_text(image_storage_path))
    return build_calibration_asset_payload(
        role="reference_cache",
        calibration_id=calibration_id,
        calibration_uuid=calibration_uuid,
        local_path=cache_path,
        source_path=original_path,
        original_path=original_path,
        cloud_storage_path=image_storage_path,
        file_purpose="cache",
        source_role="cloud_recovery_cache",
        metadata=merged_metadata,
        asset_uuid=asset_uuid,
    )


def extract_calibration_asset_payloads(calibration: dict | None) -> list[dict[str, Any]]:
    """Extract calibration asset payloads from a calibration row or payload."""
    record = dict(calibration or {})
    calibration_id = record.get("id")
    calibration_uuid = record.get("calibration_uuid")
    payloads: list[dict[str, Any]] = []

    measurements_json = record.get("measurements_json")
    loaded: dict[str, Any] | list[Any] | None = None
    if measurements_json:
        try:
            loaded = json.loads(measurements_json) if isinstance(measurements_json, str) else measurements_json
        except Exception:
            loaded = None

    def _maybe_add(
        *,
        role: str,
        source_path: str | Path | None,
        local_path: str | Path | None,
        original_path: str | Path | None,
        asset_index: int | None,
        file_purpose: str | None = None,
        source_role: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not source_path and not local_path and not original_path:
            return
        payloads.append(
            build_calibration_asset_payload(
                role=role,
                calibration_id=calibration_id,
                calibration_uuid=calibration_uuid,
                local_path=local_path,
                source_path=source_path,
                original_path=original_path,
                file_purpose=file_purpose,
                source_role=source_role,
                asset_index=asset_index,
                metadata=metadata,
            )
        )

    if isinstance(loaded, dict):
        for idx, image_info in enumerate(loaded.get("images") or []):
            if not isinstance(image_info, dict):
                continue
            source_path = image_info.get("source_path") or image_info.get("source_filepath") or image_info.get("original_path")
            working_path = image_info.get("path") or image_info.get("working_path") or record.get("image_filepath")
            crop_box = image_info.get("crop_box")
            crop_source_size = image_info.get("crop_source_size")
            image_metadata = {
                "image_index": image_info.get("index", idx),
                "division_distance_mm": image_info.get("division_distance_mm"),
                "measurements": image_info.get("measurements") or [],
                "crop_box": crop_box,
                "crop_source_size": crop_source_size,
                "source_path": _normalize_text(source_path),
                "working_path": _normalize_text(working_path),
            }
            _maybe_add(
                role="source_photo",
                source_path=source_path,
                local_path=source_path,
                original_path=None,
                asset_index=image_info.get("index", idx),
                file_purpose="calibration",
                source_role="import_source",
                metadata=image_metadata,
            )
            _maybe_add(
                role="working_photo",
                source_path=source_path or working_path,
                local_path=working_path,
                original_path=source_path,
                asset_index=image_info.get("index", idx),
                file_purpose="calibration",
                metadata=image_metadata,
            )
            if crop_box or crop_source_size:
                _maybe_add(
                    role="calibration_crop",
                    source_path=working_path,
                    local_path=None,
                    original_path=working_path,
                    asset_index=image_info.get("index", idx),
                    file_purpose="calibration",
                    source_role="generated_artifact",
                    metadata={
                        **image_metadata,
                        "crop_box": crop_box,
                        "crop_source_size": crop_source_size,
                    },
                )

        for idx, auto_info in enumerate(loaded.get("auto_images") or []):
            if not isinstance(auto_info, dict):
                continue
            working_path = auto_info.get("path") or record.get("image_filepath")
            auto_metadata = {
                "image_index": auto_info.get("index", idx),
                "spacing_um": auto_info.get("spacing_um"),
                "division_distance_mm": auto_info.get("division_distance_mm"),
                "result": auto_info.get("result") or {},
                "overlay_parabola": auto_info.get("overlay_parabola") or [],
                "overlay_edges": auto_info.get("overlay_edges") or [],
                "overlay_edges_50": auto_info.get("overlay_edges_50") or [],
                "working_path": _normalize_text(working_path),
            }
            _maybe_add(
                role="overlay",
                source_path=working_path,
                local_path=None,
                original_path=working_path,
                asset_index=auto_info.get("index", idx),
                file_purpose="calibration",
                source_role="generated_artifact",
                metadata=auto_metadata,
            )
            _maybe_add(
                role="debug_artifact",
                source_path=working_path,
                local_path=None,
                original_path=working_path,
                asset_index=auto_info.get("index", idx),
                file_purpose="calibration",
                source_role="generated_artifact",
                metadata={
                    **auto_metadata,
                    "debug_artifact": True,
                },
            )
    else:
        image_path = record.get("image_filepath")
        if image_path:
            _maybe_add(
                role="working_photo",
                source_path=image_path,
                local_path=image_path,
                original_path=None,
                asset_index=0,
                file_purpose="calibration",
                metadata={
                    "image_index": 0,
                    "working_path": _normalize_text(image_path),
                    "legacy_image_filepath": _normalize_text(image_path),
                },
            )

    return payloads


__all__ = [
    "build_calibration_asset_payload",
    "build_reference_cache_asset_payload",
    "extract_calibration_asset_payloads",
    "normalize_calibration_asset_file_purpose",
    "normalize_calibration_asset_role",
    "normalize_calibration_asset_source_role",
]
