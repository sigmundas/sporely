"""Shared application identity and migration helpers for Sporely."""
from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import shutil
from pathlib import Path

from platformdirs import user_data_dir
from PySide6.QtCore import QSettings

APP_NAME = "Sporely"
APP_DISPLAY_NAME = "Sporely"
APP_FULL_NAME = "Sporely - Mushroom Log and Spore Analyzer"
APP_LOWER_NAME = "sporely"
APP_REPOSITORY_URL = "https://github.com/sigmundas/sporely"
APP_DOCS_BASE_URL = f"{APP_REPOSITORY_URL}/blob/main/docs"

LEGACY_APP_NAME = "MycoLog"
LEGACY_APP_DISPLAY_NAME = "MycoLog"
LEGACY_APP_LOWER_NAME = "mycolog"

SETTINGS_ORG = APP_NAME
SETTINGS_APP = APP_NAME
LEGACY_SETTINGS_ORG = LEGACY_APP_NAME
LEGACY_SETTINGS_APP = LEGACY_APP_NAME
APP_DATA_DIR_ENV = "SPORELY_APP_DATA_DIR"
PROFILE_ENV = "SPORELY_PROFILE"


def _sanitize_profile_name(value: str | None) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
    text = text.strip(".-_")
    return text


def current_profile_name() -> str | None:
    profile = _sanitize_profile_name(os.environ.get(PROFILE_ENV))
    return profile or None


def runtime_profile_scope() -> str | None:
    profile = current_profile_name()
    if profile:
        return profile
    override = str(os.environ.get(APP_DATA_DIR_ENV) or "").strip()
    if not override:
        return None
    resolved = str(Path(override).expanduser().resolve())
    base = _sanitize_profile_name(Path(resolved).name) or "custom"
    digest = hashlib.sha1(resolved.encode("utf-8")).hexdigest()[:8]
    return f"{base}-{digest}"


def using_isolated_profile() -> bool:
    return bool(runtime_profile_scope())


def settings_namespace() -> tuple[str, str]:
    scope = runtime_profile_scope()
    if not scope:
        return APP_NAME, APP_NAME
    suffix = f".{scope}"
    return f"{APP_NAME}{suffix}", f"{APP_NAME}{suffix}"


SETTINGS_ORG, SETTINGS_APP = settings_namespace()


def app_data_dir() -> Path:
    override = str(os.environ.get(APP_DATA_DIR_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    base_dir = Path(user_data_dir(APP_NAME, appauthor=False, roaming=True))
    profile = current_profile_name()
    if not profile:
        return base_dir
    return base_dir / "profiles" / profile


def legacy_app_data_dir() -> Path:
    return Path(user_data_dir(LEGACY_APP_NAME, appauthor=False, roaming=True))


def _merge_directory_missing_only(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    destination.mkdir(parents=True, exist_ok=True)
    for item in source.iterdir():
        dest_item = destination / item.name
        if item.is_dir():
            _merge_directory_missing_only(item, dest_item)
            continue
        if not dest_item.exists():
            dest_item.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_item)


def migrate_app_data_dir() -> Path:
    """Move or merge legacy MycoLog app data into the Sporely folder."""
    new_dir = app_data_dir()
    if using_isolated_profile():
        new_dir.mkdir(parents=True, exist_ok=True)
        return new_dir
    old_dir = legacy_app_data_dir()
    if not old_dir.exists():
        new_dir.mkdir(parents=True, exist_ok=True)
        return new_dir
    if not new_dir.exists():
        new_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_dir), str(new_dir))
        return new_dir
    _merge_directory_missing_only(old_dir, new_dir)
    return new_dir


def _copy_qsettings_missing_only(
    source_org: str,
    source_app: str,
    target_org: str,
    target_app: str,
) -> None:
    source = QSettings(source_org, source_app)
    target = QSettings(target_org, target_app)
    for key in source.allKeys():
        if target.contains(key):
            continue
        target.setValue(key, source.value(key))
    target.sync()


def migrate_qsettings() -> None:
    """Copy legacy QSettings namespaces into the new Sporely namespaces."""
    if using_isolated_profile():
        return
    _copy_qsettings_missing_only(
        LEGACY_SETTINGS_ORG,
        LEGACY_SETTINGS_APP,
        SETTINGS_ORG,
        SETTINGS_APP,
    )
    _copy_qsettings_missing_only(
        LEGACY_SETTINGS_ORG,
        "SpeciesPlate",
        SETTINGS_ORG,
        "SpeciesPlate",
    )


def _rewrite_legacy_path_value(value: object, old_dir: Path, new_dir: Path) -> object:
    try:
        text = str(value)
    except Exception:
        return value
    if not text:
        return value
    old_text = str(old_dir)
    if text == old_text:
        return str(new_dir)
    prefix = old_text.rstrip("/\\") + "/"
    if text.startswith(prefix):
        return str(new_dir / text[len(prefix):])
    mac_prefix = old_text.rstrip("/\\") + "\\"
    if text.startswith(mac_prefix):
        return str(new_dir / text[len(mac_prefix):])
    return value


def migrate_app_settings_file() -> None:
    """Rewrite legacy MycoLog storage paths inside Sporely app_settings.json."""
    if using_isolated_profile():
        return
    new_dir = app_data_dir()
    old_dir = legacy_app_data_dir()
    settings_path = new_dir / "app_settings.json"
    if not settings_path.exists():
        return
    try:
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(settings, dict):
        return
    updated = False
    for key in ("database_folder", "database_path", "reference_database_path", "images_dir"):
        if key not in settings:
            continue
        new_value = _rewrite_legacy_path_value(settings.get(key), old_dir, new_dir)
        if new_value != settings.get(key):
            settings[key] = new_value
            updated = True
    if updated:
        settings_path.write_text(json.dumps(settings, indent=2), encoding="utf-8")


def migrate_database_paths() -> None:
    """Rewrite legacy MycoLog absolute paths stored inside the main database."""
    if using_isolated_profile():
        return
    new_dir = app_data_dir()
    old_dir = legacy_app_data_dir()
    db_path = new_dir / "mushrooms.db"
    if not db_path.exists():
        return
    text_updates = {
        "observations": ("folder_path",),
        "images": ("filepath", "original_filepath"),
        "thumbnails": ("filepath",),
        "calibrations": ("image_filepath",),
    }
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return
    try:
        cur = conn.cursor()
        changed = False
        for table, columns in text_updates.items():
            try:
                cur.execute(f"PRAGMA table_info({table})")
                available = {str(row[1]) for row in cur.fetchall()}
            except Exception:
                continue
            for column in columns:
                if column not in available:
                    continue
                try:
                    cur.execute(f"SELECT rowid, {column} FROM {table}")
                    rows = cur.fetchall()
                except Exception:
                    continue
                for rowid, value in rows:
                    new_value = _rewrite_legacy_path_value(value, old_dir, new_dir)
                    if new_value == value:
                        continue
                    try:
                        cur.execute(
                            f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                            (new_value, rowid),
                        )
                        changed = True
                    except Exception:
                        continue
        if changed:
            conn.commit()
    finally:
        conn.close()


def migrate_legacy_storage() -> Path:
    """Migrate all supported legacy local state to Sporely namespaces."""
    data_dir = migrate_app_data_dir()
    migrate_app_settings_file()
    migrate_database_paths()
    migrate_qsettings()
    return data_dir
