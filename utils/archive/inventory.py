"""Authoritative Phase 1 archive inventory and settings policies."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable


class BackupPolicy(str, Enum):
    EXACT = "exact"
    REGENERABLE = "regenerable"
    CACHE = "cache"
    SECRET = "secret"
    DOWNLOADABLE = "downloadable"


class PortablePolicy(str, Enum):
    ROOT = "root"
    DEPENDENCY = "dependency"
    EXCLUDE = "exclude"
    SPECIAL = "special"


class SettingPolicy(str, Enum):
    EXACT = "exact"
    REGENERABLE = "regenerable"
    MACHINE_SPECIFIC = "machine_specific"
    SECRET = "secret"
    EXCLUDE = "exclude"


@dataclass(frozen=True)
class InventoryPolicy:
    backup: BackupPolicy
    portable: PortablePolicy


@dataclass(frozen=True)
class SettingInventoryPolicy:
    setting: SettingPolicy
    backup: BackupPolicy
    portable: PortablePolicy = PortablePolicy.EXCLUDE


@dataclass(frozen=True)
class ResourceSpec:
    name: str
    archive_path: str
    policy: InventoryPolicy
    source: Callable[[], Path] | None

    def resolve_source(self) -> Path | None:
        return self.source() if self.source is not None else None


MAIN_DATABASE_TABLES: dict[str, InventoryPolicy] = {
    "calibration_assets": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "calibrations": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "image_tombstones": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE),
    "images": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "observation_reference_uses": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "observation_reference_use_cloud_sync_state": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE),
    "observation_reference_use_cloud_tombstones": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE),
    "observations": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.ROOT),
    "portable_import_provenance": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE),
    "session_logs": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "settings": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE),
    "spore_annotations": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "spore_measurements": InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY),
    "thumbnails": InventoryPolicy(BackupPolicy.REGENERABLE, PortablePolicy.EXCLUDE),
}

REFERENCE_DATABASE_TABLES: dict[str, InventoryPolicy] = {
    name: InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY)
    for name in (
        "reference_measurement_sets",
        "reference_measurement_set_preferences",
        "reference_taxon_treatments",
        "reference_values",
        "reference_works",
    )
}
REFERENCE_DATABASE_TABLES.update({
    "reference_measurement_set_preferences": InventoryPolicy(
        BackupPolicy.EXACT, PortablePolicy.EXCLUDE
    ),
    "reference_cloud_sync_state": InventoryPolicy(
        BackupPolicy.EXACT, PortablePolicy.EXCLUDE
    ),
    "reference_cloud_tombstones": InventoryPolicy(
        BackupPolicy.EXACT, PortablePolicy.EXCLUDE
    ),
})


def _schema_path(name: str) -> Callable[[], Path]:
    def resolve() -> Path:
        from database import schema

        return Path(getattr(schema, name)())

    return resolve


def _app_data_child(*parts: str) -> Callable[[], Path]:
    def resolve() -> Path:
        from app_identity import app_data_dir

        return app_data_dir().joinpath(*parts)

    return resolve


def _app_cache() -> Path:
    from app_identity import app_cache_dir

    return app_cache_dir()


def _thumbnail_root() -> Path:
    from database.schema import get_database_path

    return get_database_path().parent / "thumbnails"


def _plate_layout_root() -> Path:
    from database.schema import get_database_path

    return get_database_path().parent / "plate_layouts"


def _taxonomy_root() -> Path:
    from app_identity import app_data_dir

    return app_data_dir() / "taxonomy_v2"


RESOURCE_INVENTORY: tuple[ResourceSpec, ...] = tuple(sorted((
    ResourceSpec("main_database", "databases/mushrooms.db", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.SPECIAL), _schema_path("get_database_path")),
    ResourceSpec("reference_database", "databases/reference_values.db", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.SPECIAL), _schema_path("get_reference_database_path")),
    ResourceSpec("objectives", "data/objectives.json", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY), _schema_path("get_objectives_path")),
    ResourceSpec("last_objective", "data/last_objective.json", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE), _schema_path("get_last_objective_path")),
    ResourceSpec("managed_images", "assets/images", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY), _schema_path("get_images_dir")),
    ResourceSpec("retained_originals", "assets/originals", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY), None),
    ResourceSpec("calibration_assets", "assets/calibrations", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.DEPENDENCY), _schema_path("get_calibrations_dir")),
    ResourceSpec("plate_layouts", "data/plate_layouts", InventoryPolicy(BackupPolicy.EXACT, PortablePolicy.EXCLUDE), _plate_layout_root),
    ResourceSpec("thumbnails", "generated/thumbnails", InventoryPolicy(BackupPolicy.REGENERABLE, PortablePolicy.EXCLUDE), _thumbnail_root),
    ResourceSpec("generated_calibration_artifacts", "generated/calibrations", InventoryPolicy(BackupPolicy.REGENERABLE, PortablePolicy.EXCLUDE), None),
    ResourceSpec("cloud_cache_observations", "cache/cloud/observations", InventoryPolicy(BackupPolicy.CACHE, PortablePolicy.EXCLUDE), _app_data_child("cloud_cache", "observations")),
    ResourceSpec("cloud_cache_calibrations", "cache/cloud/calibrations", InventoryPolicy(BackupPolicy.CACHE, PortablePolicy.EXCLUDE), _app_data_child("cloud_cache", "calibrations")),
    ResourceSpec("cloud_cache_originals", "cache/cloud/originals", InventoryPolicy(BackupPolicy.CACHE, PortablePolicy.EXCLUDE), _app_data_child("cloud_cache", "originals")),
    ResourceSpec("application_cache", "cache/application", InventoryPolicy(BackupPolicy.CACHE, PortablePolicy.EXCLUDE), _app_cache),
    ResourceSpec("taxonomy_v2", "downloadable/taxonomy_v2", InventoryPolicy(BackupPolicy.DOWNLOADABLE, PortablePolicy.EXCLUDE), _taxonomy_root),
    ResourceSpec("artportalen_cookies", "secrets/artportalen_cookies.json", InventoryPolicy(BackupPolicy.SECRET, PortablePolicy.EXCLUDE), _app_data_child("artportalen_cookies.json")),
    ResourceSpec("artsobservasjoner_cookies", "secrets/artsobservasjoner_cookies.json", InventoryPolicy(BackupPolicy.SECRET, PortablePolicy.EXCLUDE), _app_data_child("artsobservasjoner_cookies.json")),
    ResourceSpec("inaturalist_oauth_tokens", "secrets/inaturalist_oauth_tokens.json", InventoryPolicy(BackupPolicy.SECRET, PortablePolicy.EXCLUDE), _app_data_child("inaturalist_oauth_tokens.json")),
    ResourceSpec("keyring_credentials", "secrets/keyring", InventoryPolicy(BackupPolicy.SECRET, PortablePolicy.EXCLUDE), None),
), key=lambda item: item.name))


APP_SETTING_POLICIES: dict[str, SettingPolicy] = {
    "cloud_access_token": SettingPolicy.SECRET,
    "cloud_refresh_token": SettingPolicy.SECRET,
    "database_folder": SettingPolicy.MACHINE_SPECIFIC,
    "database_path": SettingPolicy.MACHINE_SPECIFIC,
    "reference_database_path": SettingPolicy.MACHINE_SPECIFIC,
    "images_dir": SettingPolicy.MACHINE_SPECIFIC,
    "last_export_dir": SettingPolicy.MACHINE_SPECIFIC,
    "last_import_dir": SettingPolicy.MACHINE_SPECIFIC,
    "linked_cloud_user_id": SettingPolicy.EXACT,
    "cloud_user_id": SettingPolicy.EXACT,
    "cloud_user_email": SettingPolicy.EXACT,
    "cloud_child_change_cursor": SettingPolicy.EXACT,
    "cloud_recent_import_local_ids": SettingPolicy.EXACT,
    "ui_language": SettingPolicy.EXACT,
    "ui_theme": SettingPolicy.EXACT,
    "vernacular_language": SettingPolicy.EXACT,
    "cloud_last_pull_at": SettingPolicy.REGENERABLE,
    "cloud_last_sync_at": SettingPolicy.REGENERABLE,
    "cloud_last_sync_status": SettingPolicy.REGENERABLE,
    "cloud_last_sync_summary": SettingPolicy.REGENERABLE,
    "cloud_last_sync_error_count": SettingPolicy.REGENERABLE,
    "cloud_last_sync_errors_json": SettingPolicy.REGENERABLE,
    "cloud_last_child_safety_pull_at": SettingPolicy.REGENERABLE,
    "cloud_measurement_reconcile_version": SettingPolicy.REGENERABLE,
    "cloud_measurement_reconcile_at": SettingPolicy.REGENERABLE,
    "taxonomy_v2_activation": SettingPolicy.REGENERABLE,
}


def _setting_inventory_policy(policy: SettingPolicy) -> SettingInventoryPolicy:
    backup = {
        SettingPolicy.EXACT: BackupPolicy.EXACT,
        SettingPolicy.REGENERABLE: BackupPolicy.REGENERABLE,
        SettingPolicy.MACHINE_SPECIFIC: BackupPolicy.REGENERABLE,
        SettingPolicy.SECRET: BackupPolicy.SECRET,
        SettingPolicy.EXCLUDE: BackupPolicy.REGENERABLE,
    }[policy]
    return SettingInventoryPolicy(policy, backup)


APP_SETTING_INVENTORY: dict[str, SettingInventoryPolicy] = {
    key: _setting_inventory_policy(policy) for key, policy in APP_SETTING_POLICIES.items()
}


def app_setting_policy(key: str) -> SettingPolicy:
    """Return a classified app-setting key, failing closed for unknown keys."""
    try:
        return APP_SETTING_POLICIES[key]
    except KeyError as exc:
        raise KeyError(f"unclassified app setting: {key}") from exc


_DB_SECRET_KEYS = {
    "inat_client_secret", "mushroomobserver_app_api_key", "mushroomobserver_user_api_key",
}
_DB_MACHINE_KEYS = {"originals_dir", "live_lab_watch_dir", "ingestion_hub_scan_dir"}
_DB_REGENERABLE_SUFFIXES = ("_splitter_sizes",)
_DB_EXACT_KEYS = {
    "active_reporting_target", "artportalen_username",
    "inat_client_id", "inat_redirect_uri", "original_storage_mode", "store_original_images",
    "resize_jpeg_quality", "resize_to_optimal_sampling", "target_sampling_pct", "remember_last_used",
    "ingestion_hub_field_match_tolerance_seconds", "ingestion_hub_offset_seconds",
    "sync_full_resolution_originals",
    "profile_name", "profile_email", "profile_bio", "profile_username", "profile_avatar_url",
    "ui_language", "ui_theme", "vernacular_language", "measure_categories",
    "contrast_default",
    "contrast_options", "mount_options", "stain_options", "sample_options",
    "sample_source_options",
    "sporely_debug_cloud_plan_override", "sporely_show_debug_cloud_plan_override",
    "sporely_cloud_media_signature_v1", "cloud_pending_image_repair_version",
    "cloud_pending_image_repair_at", "cloud_exif_backfill_checked",
}
_DB_EXACT_PREFIXES = (
    "artsobs_", "sporely_cloud_", "profile_", "last_used_", "gallery_settings_",
    "live_lab_", "observations_", "measure_view_", "measure_image_view_settings_",
    "measure_observation_scale_bar_value_", "raw_", "default_",
)
_CREDENTIAL_MARKERS = ("token", "password", "secret", "api_key")


def database_setting_policy(key: str) -> SettingPolicy:
    """Classify a DB setting; unknown keys fail closed."""
    normalized = str(key or "").strip().lower()
    if not normalized:
        raise KeyError("empty database setting key")
    if normalized in _DB_SECRET_KEYS or any(marker in normalized for marker in _CREDENTIAL_MARKERS):
        return SettingPolicy.SECRET
    if normalized in _DB_MACHINE_KEYS or normalized.endswith("_dir") or normalized.endswith("_path"):
        return SettingPolicy.MACHINE_SPECIFIC
    if normalized.endswith(_DB_REGENERABLE_SUFFIXES):
        return SettingPolicy.REGENERABLE
    if normalized in _DB_EXACT_KEYS or normalized.startswith(_DB_EXACT_PREFIXES):
        return SettingPolicy.EXACT
    raise KeyError(f"unclassified database setting: {key}")


def database_setting_inventory(key: str) -> SettingInventoryPolicy:
    return _setting_inventory_policy(database_setting_policy(key))


_SPECIES_PLATE_GLOBAL_KEYS = {
    "ins_r", "text_scale", "se_step", "grad_opacity", "grad_pos", "bg_layout",
    "inset_position", "ins_margin", "plate_bg_color", "inset_layout", "bg_border",
    "inset_border", "show_tech", "show_sample", "show_measures", "equal_scale",
}


def qsettings_policy(namespace: tuple[str, str], key: str) -> SettingPolicy:
    """Classify QSettings state; unknown namespaces and keys are excluded."""
    organization, application = namespace
    sporely_organization = organization == "Sporely" or organization.startswith("Sporely.")
    if sporely_organization and application == organization:
        if key.startswith("geometry/") or key.startswith("splitter/"):
            return SettingPolicy.REGENERABLE
        return SettingPolicy.EXCLUDE
    if not sporely_organization or application != "SpeciesPlate":
        return SettingPolicy.EXCLUDE
    if key in _SPECIES_PLATE_GLOBAL_KEYS:
        return SettingPolicy.EXACT
    parts = key.split("/", 1)
    if len(parts) == 2 and parts[0].startswith("obs_") and parts[0][4:].isdigit() and parts[1]:
        return SettingPolicy.EXACT
    return SettingPolicy.EXCLUDE


def qsettings_inventory(namespace: tuple[str, str], key: str) -> SettingInventoryPolicy:
    return _setting_inventory_policy(qsettings_policy(namespace, key))


def inventory_resource_names() -> tuple[str, ...]:
    return tuple(item.name for item in RESOURCE_INVENTORY)
