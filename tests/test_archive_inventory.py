import ast
import sqlite3
from pathlib import Path

import pytest

from database import schema
from utils.archive.inventory import (
    APP_SETTING_POLICIES,
    APP_SETTING_INVENTORY,
    MAIN_DATABASE_TABLES,
    REFERENCE_DATABASE_TABLES,
    RESOURCE_INVENTORY,
    BackupPolicy,
    PortablePolicy,
    SettingPolicy,
    app_setting_policy,
    database_setting_policy,
    database_setting_inventory,
    inventory_resource_names,
    qsettings_policy,
    qsettings_inventory,
)


def _tables(path):
    with sqlite3.connect(path) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }


def test_database_inventory_covers_production_schema(monkeypatch, tmp_path):
    monkeypatch.setattr(schema, "_app_dir", tmp_path)
    monkeypatch.setattr(schema, "DATABASE_PATH", tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", tmp_path / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", tmp_path / "app_settings.json")
    schema.init_database()
    assert _tables(schema.get_database_path()) == set(MAIN_DATABASE_TABLES)
    assert _tables(schema.get_reference_database_path()) == set(REFERENCE_DATABASE_TABLES)
    with sqlite3.connect(schema.get_database_path()) as connection:
        for (key,) in connection.execute("SELECT key FROM settings"):
            database_setting_policy(key)
    assert MAIN_DATABASE_TABLES["observations"].portable is PortablePolicy.ROOT
    assert MAIN_DATABASE_TABLES["thumbnails"].backup is BackupPolicy.REGENERABLE


def test_resource_inventory_is_deterministic_and_complete(monkeypatch, tmp_path):
    monkeypatch.setattr(schema, "_app_dir", tmp_path)
    monkeypatch.setattr(schema, "DATABASE_PATH", tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", tmp_path / "app_settings.json")
    names = inventory_resource_names()
    assert names == tuple(sorted(names))
    assert len(names) == len(set(names))
    assert {"main_database", "reference_database", "objectives", "last_objective", "managed_images", "retained_originals", "calibration_assets", "thumbnails", "application_cache", "taxonomy_v2", "inaturalist_oauth_tokens", "keyring_credentials"} <= set(names)
    main = next(item for item in RESOURCE_INVENTORY if item.name == "main_database")
    assert main.resolve_source() == tmp_path / "mushrooms.db"


@pytest.mark.parametrize("key", sorted(APP_SETTING_POLICIES))
def test_every_registered_app_setting_has_a_policy(key):
    assert app_setting_policy(key) is APP_SETTING_POLICIES[key]
    assert APP_SETTING_INVENTORY[key].portable is PortablePolicy.EXCLUDE


def test_app_settings_fail_closed_and_secrets_and_paths_are_excluded():
    assert app_setting_policy("cloud_access_token") is SettingPolicy.SECRET
    assert app_setting_policy("database_path") is SettingPolicy.MACHINE_SPECIFIC
    assert app_setting_policy("linked_cloud_user_id") is SettingPolicy.EXACT
    assert app_setting_policy("cloud_last_sync_status") is SettingPolicy.REGENERABLE
    with pytest.raises(KeyError):
        app_setting_policy("future_setting")


def test_database_setting_policy_covers_secrets_paths_state_and_unknowns():
    assert database_setting_policy("inat_client_secret") is SettingPolicy.SECRET
    assert database_setting_policy("future_access_token") is SettingPolicy.SECRET
    assert database_setting_policy("originals_dir") is SettingPolicy.MACHINE_SPECIFIC
    assert database_setting_policy("sporely_cloud_snapshot_obs_abc") is SettingPolicy.EXACT
    assert database_setting_policy("live_lab_main_splitter_sizes") is SettingPolicy.REGENERABLE
    assert database_setting_inventory("inat_client_secret").backup is BackupPolicy.SECRET
    with pytest.raises(KeyError):
        database_setting_policy("future_unclassified_preference")


@pytest.mark.parametrize(
    "key",
    (
        "active_reporting_target",
        "artportalen_username",
        "ingestion_hub_offset_seconds",
        "measure_observation_scale_bar_value_micro_um_19",
        "measure_observation_scale_bar_value_micro_um_5",
        "resize_jpeg_quality",
        "sporely_debug_cloud_plan_override",
        "sync_full_resolution_originals",
    ),
)
def test_audited_production_database_settings_restore_exactly(key):
    assert database_setting_policy(key) is SettingPolicy.EXACT


def _resolved_setting_accesses(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    constants: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
        if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
            continue
        for target in targets:
            name = (
                target.id
                if isinstance(target, ast.Name)
                else target.attr
                if isinstance(target, ast.Attribute)
                else None
            )
            if name:
                constants[name] = node.value.value

    keys: set[str] = set()
    for node in ast.walk(tree):
        if (
            not isinstance(node, ast.Call)
            or not node.args
            or not isinstance(node.func, ast.Attribute)
            or node.func.attr not in {"get_setting", "set_setting"}
        ):
            continue
        key = node.args[0]
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.add(key.value)
        elif isinstance(key, ast.Name) and key.id in constants:
            keys.add(constants[key.id])
        elif isinstance(key, ast.Attribute) and key.attr in constants:
            keys.add(constants[key.attr])
    return keys


def test_database_setting_policy_covers_resolvable_production_accesses():
    root = Path(__file__).resolve().parents[1]
    production_files = (
        root / "main.py",
        *(root / "database").glob("*.py"),
        *(root / "ui").glob("*.py"),
        *(root / "utils").glob("*.py"),
    )
    keys = set().union(
        *(
            _resolved_setting_accesses(path)
            for path in production_files
            if path.name != "db_share.py"
        )
    )
    assert keys
    unclassified = []
    for key in sorted(keys):
        try:
            database_setting_policy(key)
        except KeyError:
            unclassified.append(key)
    assert unclassified == []


@pytest.mark.parametrize(
    "key",
    (
        "gallery_settings_42",
        "last_used_stain",
        "live_lab_raw_processing_preset_camera-1",
        "measure_image_view_settings_42",
        "measure_observation_scale_bar_value_field_mm_42",
        "measure_observation_scale_bar_value_micro_um_42",
        "sporely_cloud_snapshot_obs_cloud-id",
    ),
)
def test_database_setting_policy_covers_production_dynamic_families(key):
    assert database_setting_policy(key) is SettingPolicy.EXACT


def test_qsettings_policy_preserves_plate_authorship_only():
    assert qsettings_policy(("Sporely", "Sporely"), "geometry/MainWindow") is SettingPolicy.REGENERABLE
    assert qsettings_policy(("Sporely", "Sporely"), "unknown") is SettingPolicy.EXCLUDE
    assert qsettings_policy(("Sporely", "SpeciesPlate"), "ins_r") is SettingPolicy.EXACT
    assert qsettings_policy(("Sporely", "SpeciesPlate"), "obs_42/crop_top_zoom") is SettingPolicy.EXACT
    assert qsettings_inventory(("Sporely", "SpeciesPlate"), "ins_r").portable is PortablePolicy.EXCLUDE
    assert qsettings_policy(("Other", "Other"), "ins_r") is SettingPolicy.EXCLUDE
