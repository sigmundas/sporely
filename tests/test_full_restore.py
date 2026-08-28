from __future__ import annotations

import json
import hashlib
import os
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.full_backup import create_full_backup
from utils.archive.full_restore import (
    FullRestoreError,
    _local_swap_copy,
    apply_prepared_restore,
    execute_prepared_restore_swap,
    prepare_full_restore,
    restore_full_backup,
)
from utils.archive.manifest import ArchiveManifest
from utils.archive.validation import ArchiveValidationError, validate_full_backup


@pytest.fixture
def installation(monkeypatch, tmp_path):
    root = tmp_path / "installation"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    return root


def _observation_names() -> list[str]:
    with sqlite3.connect(schema.get_database_path()) as connection:
        return [str(row[0]) for row in connection.execute("SELECT genus FROM observations ORDER BY id")]


def _make_backup(tmp_path: Path, *, image: bool = False) -> Path:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Archived')")
        if image:
            image_path = schema.get_images_dir() / "source.jpg"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(b"archived-image")
            oid = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            connection.execute("INSERT INTO images (observation_id, filepath) VALUES (?, ?)", (oid, str(image_path)))
        connection.commit()
    archive = tmp_path / "source.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})
    return archive


def _restore_full_backup(*args, **kwargs):
    kwargs.setdefault("close_live", lambda: None)
    kwargs.setdefault("reopen_live", lambda: None)
    return restore_full_backup(*args, **kwargs)


def test_restore_replaces_installation_rebases_paths_and_keeps_machine_settings(installation, tmp_path):
    external_images = tmp_path / "machine-images"
    external_images.mkdir()
    sentinel = external_images / "unrelated-user-file.txt"
    sentinel.write_text("keep", encoding="utf-8")
    schema.save_app_settings({"images_dir": str(external_images), "ui_theme": "dark"})
    _make_backup(tmp_path, image=True)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "UPDATE observations SET folder_path=?",
            (str(tmp_path / "source-machine-observation-folder"),),
        )
        connection.commit()
    archive = tmp_path / "source.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM images")
        connection.execute("DELETE FROM observations")
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')")
        connection.commit()

    safety = tmp_path / "safety.sporely"
    result = _restore_full_backup(archive, app_version="test", safety_backup_path=safety)

    assert result.safety_backup == safety
    assert safety.is_file()
    assert _observation_names() == ["Archived"]
    assert schema.get_app_settings()["images_dir"] == str(external_images)
    with sqlite3.connect(schema.get_database_path()) as connection:
        restored_path = Path(connection.execute("SELECT filepath FROM images").fetchone()[0])
        restored_folder = Path(connection.execute(
            "SELECT folder_path FROM observations"
        ).fetchone()[0])
    assert restored_path.is_relative_to(external_images)
    assert restored_path.read_bytes() == b"archived-image"
    assert restored_folder.is_relative_to(external_images)
    assert restored_folder != tmp_path / "source-machine-observation-folder"
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_restore_replaces_exact_plate_layout_collection_and_json_files(
    installation, tmp_path
):
    layouts = installation / "plate_layouts"
    layouts.mkdir()
    (layouts / "archived.mplate").write_text("archived-layout", encoding="utf-8")
    objectives = schema.get_objectives_path()
    objectives.write_text('{"source": "archived"}', encoding="utf-8")
    last_objective = schema.get_last_objective_path()
    last_objective.write_text('{"objective": "100X"}', encoding="utf-8")
    archive = _make_backup(tmp_path)

    (layouts / "archived.mplate").write_text("destination-version", encoding="utf-8")
    (layouts / "destination-only.mplate").write_text("remove", encoding="utf-8")
    objectives.write_text('{"source": "destination"}', encoding="utf-8")
    last_objective.write_text('{"objective": "40X"}', encoding="utf-8")

    _restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    assert sorted(path.name for path in layouts.glob("*.mplate")) == ["archived.mplate"]
    assert (layouts / "archived.mplate").read_text(encoding="utf-8") == "archived-layout"
    assert json.loads(objectives.read_text(encoding="utf-8")) == {"source": "archived"}
    assert json.loads(last_objective.read_text(encoding="utf-8")) == {"objective": "100X"}


def test_restore_removes_exact_resources_missing_at_source(installation, tmp_path):
    objectives = schema.get_objectives_path()
    last_objective = schema.get_last_objective_path()
    objectives.unlink(missing_ok=True)
    last_objective.unlink(missing_ok=True)
    archive = _make_backup(tmp_path)

    layouts = installation / "plate_layouts"
    layouts.mkdir(exist_ok=True)
    (layouts / "destination-only.mplate").write_text("remove", encoding="utf-8")
    objectives.write_text('{"source": "destination"}', encoding="utf-8")
    last_objective.write_text('{"objective": "40X"}', encoding="utf-8")

    _restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    assert not layouts.exists()
    assert not objectives.exists()
    assert not last_objective.exists()


def test_restore_preserves_exact_resources_excluded_by_policy(installation, tmp_path):
    objectives = schema.get_objectives_path()
    last_objective = schema.get_last_objective_path()
    objectives.unlink(missing_ok=True)
    last_objective.unlink(missing_ok=True)
    archive = _make_backup(tmp_path)
    excluded = tmp_path / "excluded-exact-resources.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] in {
            "data/plate_layouts",
            "data/objectives.json",
            "data/last_objective.json",
        }:
            entry["status"] = "excluded_by_policy"
    with ZipFile(excluded, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)

    layouts = installation / "plate_layouts"
    layouts.mkdir(exist_ok=True)
    destination_layout = layouts / "destination-only.mplate"
    destination_layout.write_text("keep", encoding="utf-8")
    objectives.write_text('{"source": "destination"}', encoding="utf-8")
    last_objective.write_text('{"objective": "40X"}', encoding="utf-8")

    _restore_full_backup(
        excluded,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    assert destination_layout.read_text(encoding="utf-8") == "keep"
    assert json.loads(objectives.read_text(encoding="utf-8")) == {
        "source": "destination"
    }
    assert json.loads(last_objective.read_text(encoding="utf-8")) == {
        "objective": "40X"
    }


def test_restore_legacy_v1_without_plate_layout_state_preserves_destination_layouts(
    installation, tmp_path
):
    archive = _make_backup(tmp_path)
    legacy = tmp_path / "legacy-unmarked-plate-layouts.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    value = manifest.to_dict()
    value["files"] = [
        entry for entry in value["files"] if entry["path"] != "data/plate_layouts"
    ]
    with ZipFile(legacy, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)
    validate_full_backup(legacy)

    layouts = installation / "plate_layouts"
    layouts.mkdir(exist_ok=True)
    destination_layout = layouts / "destination-only.mplate"
    destination_layout.write_text("keep", encoding="utf-8")

    _restore_full_backup(
        legacy,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    assert destination_layout.read_text(encoding="utf-8") == "keep"


def test_exact_resource_deletions_roll_back_after_later_failure(installation, tmp_path):
    objectives = schema.get_objectives_path()
    last_objective = schema.get_last_objective_path()
    objectives.unlink(missing_ok=True)
    last_objective.write_text('{"objective": "100X"}', encoding="utf-8")
    archive = _make_backup(tmp_path)

    layouts = installation / "plate_layouts"
    layouts.mkdir(exist_ok=True)
    destination_layout = layouts / "destination-only.mplate"
    destination_layout.write_text("destination-layout", encoding="utf-8")
    objectives.write_text('{"source": "destination"}', encoding="utf-8")
    last_objective.unlink()

    def fail_after_swap() -> None:
        assert not layouts.exists()
        assert not objectives.exists()
        assert json.loads(last_objective.read_text(encoding="utf-8")) == {
            "objective": "100X"
        }
        raise RuntimeError("later restore step failed")

    with pytest.raises(FullRestoreError, match="later restore step failed"):
        _restore_full_backup(
            archive,
            app_version="test",
            safety_backup_path=tmp_path / "safety.sporely",
            sanity_check=fail_after_swap,
        )

    assert destination_layout.read_text(encoding="utf-8") == "destination-layout"
    assert json.loads(objectives.read_text(encoding="utf-8")) == {
        "source": "destination"
    }
    assert not last_objective.exists()


def test_included_plate_layout_replacement_rolls_back_after_later_failure(
    installation, tmp_path
):
    layouts = installation / "plate_layouts"
    layouts.mkdir()
    archived_layout = layouts / "archived.mplate"
    archived_layout.write_text("archived-layout", encoding="utf-8")
    archive = _make_backup(tmp_path)

    archived_layout.unlink()
    destination_layout = layouts / "destination-only.mplate"
    destination_layout.write_text("destination-layout", encoding="utf-8")

    def fail_after_swap() -> None:
        assert archived_layout.read_text(encoding="utf-8") == "archived-layout"
        assert not destination_layout.exists()
        raise RuntimeError("later restore step failed")

    with pytest.raises(FullRestoreError, match="later restore step failed"):
        _restore_full_backup(
            archive,
            app_version="test",
            safety_backup_path=tmp_path / "safety.sporely",
            sanity_check=fail_after_swap,
        )

    assert destination_layout.read_text(encoding="utf-8") == "destination-layout"
    assert not archived_layout.exists()


def test_staging_failure_leaves_current_installation_untouched(installation, tmp_path, monkeypatch):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')")
        connection.commit()
    monkeypatch.setattr("utils.archive.full_restore._rebase_and_validate_assets", lambda *args: (_ for _ in ()).throw(RuntimeError("staging failed")))

    safety = tmp_path / "safety.sporely"
    with pytest.raises(FullRestoreError, match="staging failed"):
        _restore_full_backup(archive, app_version="test", safety_backup_path=safety)
    assert _observation_names() == ["Current"]
    assert not safety.exists()


def test_restore_accepts_cache_owned_calibration_assets_excluded_by_policy(
    installation, tmp_path
):
    cache_asset = tmp_path / "cloud-cache-calibration.tif"
    cache_asset.write_bytes(b"remote-owned-cache")
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            """
            INSERT INTO calibration_assets (
                asset_uuid, role, source_role, file_purpose, local_path,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "cache-asset-uuid",
                "source",
                "cloud_recovery_cache",
                "cache",
                str(cache_asset),
                json.dumps({
                    "source_role": "cloud_recovery_cache",
                    "file_purpose": "cache",
                    "source_path": str(cache_asset),
                }),
            ),
        )
        connection.commit()
    archive = tmp_path / "cache-owned.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})

    result = _restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    assert result.manifest.mode == "full_backup"
    with sqlite3.connect(schema.get_database_path()) as connection:
        local_path, metadata = connection.execute(
            "SELECT local_path, metadata_json FROM calibration_assets "
            "WHERE asset_uuid='cache-asset-uuid'"
        ).fetchone()
    assert local_path is None
    assert json.loads(metadata)["source_path"] is None


def test_restore_rebases_authoritative_calibration_auto_image_paths(
    installation, tmp_path
):
    source = tmp_path / "calibration-source.tif"
    source.write_bytes(b"calibration-source")
    image_record = {"path": str(source), "source_path": str(source)}
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date,
                microns_per_pixel, measurements_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "auto-images-calibration",
                "100X",
                "2026-08-27",
                0.1,
                json.dumps({"images": [image_record], "auto_images": [image_record]}),
            ),
        )
        connection.commit()
    archive = tmp_path / "auto-images.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})

    _restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    with sqlite3.connect(schema.get_database_path()) as connection:
        metadata = json.loads(connection.execute(
            "SELECT measurements_json FROM calibrations "
            "WHERE calibration_uuid='auto-images-calibration'"
        ).fetchone()[0])
    for collection in ("images", "auto_images"):
        for key in ("path", "source_path"):
            restored = Path(metadata[collection][0][key])
            assert restored != source
            assert restored.read_bytes() == b"calibration-source"


def test_safety_backup_failure_prevents_staging_and_replacement(installation, tmp_path):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')")
        connection.commit()

    def fail_backup(*args, **kwargs):
        raise RuntimeError("safety backup failed")

    with pytest.raises(FullRestoreError, match="safety backup failed"):
        _restore_full_backup(archive, app_version="test", backup_creator=fail_backup)
    assert _observation_names() == ["Current"]


def test_prepare_does_not_create_safety_backup_before_live_quiescence(
    installation, tmp_path
):
    archive = _make_backup(tmp_path)
    safety = tmp_path / "safety.sporely"

    prepared = prepare_full_restore(
        archive,
        app_version="test",
        safety_backup_path=safety,
    )
    try:
        assert not safety.exists()
    finally:
        prepared.cleanup()


def test_swap_requires_quiescence_and_creates_fresh_safety_backup_first(
    installation, tmp_path, monkeypatch
):
    archive = _make_backup(tmp_path)
    safety = tmp_path / "safety.sporely"
    prepared = prepare_full_restore(
        archive,
        app_version="test",
        safety_backup_path=safety,
    )
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO observations (date, genus) VALUES ('2026-08-28', 'LateEdit')"
        )
        connection.commit()

    events: list[tuple[str, list[str] | None]] = []

    def record_backup(destination, **_kwargs):
        events.append(("backup", _observation_names()))
        Path(destination).write_bytes(b"safety")

    real_replace = os.replace

    def record_replace(source, target):
        events.append(("replace", None))
        return real_replace(source, target)

    monkeypatch.setattr("utils.archive.full_restore.os.replace", record_replace)
    with pytest.raises(FullRestoreError, match="quiescent"):
        execute_prepared_restore_swap(
            prepared,
            app_version="test",
            live_quiesced=False,
            backup_creator=record_backup,
        )
    assert events == []

    swap = execute_prepared_restore_swap(
        prepared,
        app_version="test",
        live_quiesced=True,
        backup_creator=record_backup,
    )
    try:
        assert events[0] == ("backup", ["Archived", "LateEdit"])
        assert events[1][0] == "replace"
        assert safety.read_bytes() == b"safety"
    finally:
        swap.rollback()


def test_failed_replacement_rolls_back_every_replaced_target(installation, tmp_path, monkeypatch):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')")
        connection.commit()
    real_replace = os.replace
    live_main = schema.get_database_path().resolve()
    failed = False

    def fail_after_main(source, target):
        nonlocal failed
        target_path = Path(target).resolve()
        if target_path == schema.get_reference_database_path().resolve() and not failed:
            failed = True
            raise OSError("injected replacement failure")
        return real_replace(source, target)

    monkeypatch.setattr("utils.archive.full_restore.os.replace", fail_after_main)
    safety = tmp_path / "safety.sporely"
    with pytest.raises(FullRestoreError, match="injected replacement failure"):
        _restore_full_backup(archive, app_version="test", safety_backup_path=safety)
    assert live_main.is_file()
    assert _observation_names() == ["Current"]
    assert safety.is_file()


def test_failed_install_and_rollback_rename_still_restores_live_database(
    installation, tmp_path, monkeypatch
):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute(
            "INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')"
        )
        connection.commit()
    real_replace = os.replace
    live_main = schema.get_database_path().resolve()
    install_failed = False
    rollback_failed = False

    def fail_install_and_first_rollback(source, target):
        nonlocal install_failed, rollback_failed
        source_path = Path(source)
        target_path = Path(target).resolve()
        if target_path == live_main and source_path.name == "incoming" and not install_failed:
            install_failed = True
            raise OSError("injected install failure")
        if target_path == live_main and source_path.name == "previous" and not rollback_failed:
            rollback_failed = True
            raise OSError("injected rollback rename failure")
        return real_replace(source, target)

    monkeypatch.setattr(
        "utils.archive.full_restore.os.replace", fail_install_and_first_rollback
    )

    with pytest.raises(FullRestoreError, match="injected"):
        _restore_full_backup(
            archive,
            app_version="test",
            safety_backup_path=tmp_path / "safety.sporely",
        )

    assert live_main.is_file()
    assert _observation_names() == ["Current"]


def test_interrupted_local_swap_copy_removes_partial_swap_directory(
    tmp_path, monkeypatch
):
    source = tmp_path / "source.db"
    source.write_bytes(b"new")
    target = tmp_path / "profile" / "mushrooms.db"
    target.parent.mkdir()

    def fail_copy(_source, destination):
        Path(destination).write_bytes(b"partial")
        raise OSError("injected copy failure")

    monkeypatch.setattr("utils.archive.full_restore.shutil.copy2", fail_copy)

    with pytest.raises(OSError, match="injected copy failure"):
        _local_swap_copy(source, target)

    assert not list(target.parent.glob(".sporely-restore-*"))


def test_failed_post_swap_sanity_restores_databases_and_settings(installation, tmp_path):
    archive = _make_backup(tmp_path)
    schema.save_app_settings({"ui_theme": "light"})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')")
        connection.commit()
    main_before = schema.get_database_path().read_bytes()
    reference_before = schema.get_reference_database_path().read_bytes()
    calls = []

    with pytest.raises(FullRestoreError, match="sanity failed"):
        _restore_full_backup(
            archive,
            app_version="test",
            safety_backup_path=tmp_path / "safety.sporely",
            close_live=lambda: calls.append("close"),
            reopen_live=lambda: calls.append("reopen"),
            sanity_check=lambda: (_ for _ in ()).throw(RuntimeError("sanity failed")),
        )
    assert schema.get_database_path().read_bytes() == main_before
    assert schema.get_reference_database_path().read_bytes() == reference_before
    assert schema.get_app_settings() == {"ui_theme": "light"}
    assert calls == ["close", "reopen", "close", "reopen"]


def test_compatibility_restore_requires_explicit_live_lifecycle_callbacks(
    installation, tmp_path
):
    archive = _make_backup(tmp_path)

    with pytest.raises(TypeError):
        restore_full_backup(archive, app_version="test")


def test_failed_post_swap_close_preserves_swapped_state_and_recovery_copies(
    installation, tmp_path
):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute(
            "INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')"
        )
        connection.commit()
    prepared = prepare_full_restore(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )
    swap_roots = [item[0][2] for item in prepared.swap_targets]
    calls: list[str] = []

    def close_live():
        calls.append("close")
        if calls.count("close") == 2:
            raise RuntimeError("still busy")

    def reopen_live():
        calls.append("reopen")

    with pytest.raises(FullRestoreError, match="could not re-quiesce"):
        apply_prepared_restore(
            prepared,
            close_live=close_live,
            reopen_live=reopen_live,
            sanity_check=lambda: (_ for _ in ()).throw(RuntimeError("sanity failed")),
        )

    assert _observation_names() == ["Archived"]
    assert calls == ["close", "reopen", "close"]
    assert all(path.exists() for path in swap_roots)
    assert (tmp_path / "safety.sporely").is_file()


def test_partial_reopen_must_requiesce_before_file_rollback(installation, tmp_path):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
        connection.execute(
            "INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Current')"
        )
        connection.commit()
    prepared = prepare_full_restore(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )
    swap_roots = [item[0][2] for item in prepared.swap_targets]
    calls: list[str] = []

    def close_live():
        calls.append("close")
        if calls.count("close") == 2:
            raise RuntimeError("partially reopened resource still busy")

    def reopen_live():
        calls.append("reopen")
        raise RuntimeError("reopen failed after partial work")

    with pytest.raises(FullRestoreError, match="could not re-quiesce"):
        apply_prepared_restore(
            prepared,
            close_live=close_live,
            reopen_live=reopen_live,
            sanity_check=lambda: None,
        )

    assert calls == ["close", "reopen", "close"]
    assert _observation_names() == ["Archived"]
    assert all(path.exists() for path in swap_roots)


@pytest.mark.parametrize("mutation", ["missing", "extra", "bad_hash"])
def test_validator_rejects_manifest_member_disagreement_and_bad_hash(installation, tmp_path, mutation):
    archive = _make_backup(tmp_path)
    broken = tmp_path / f"{mutation}.sporely"
    with ZipFile(archive) as source, ZipFile(broken, "w") as target:
        for info in source.infolist():
            if mutation == "missing" and info.filename == "data/app_settings.json":
                continue
            payload = source.read(info.filename)
            if mutation == "bad_hash" and info.filename == "data/app_settings.json":
                payload += b"x"
            target.writestr(info, payload)
        if mutation == "extra":
            target.writestr("data/extra.json", b"{}")
    with pytest.raises(ArchiveValidationError):
        validate_full_backup(broken)


def test_malformed_database_is_rejected_before_live_state_changes(installation, tmp_path):
    archive = _make_backup(tmp_path)
    broken = tmp_path / "malformed.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        files = {info.filename: source.read(info.filename) for info in source.infolist() if info.filename != "manifest.json"}
    files["databases/mushrooms.db"] = b"not sqlite"
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] == "databases/mushrooms.db":
            import hashlib
            entry["size"] = len(files["databases/mushrooms.db"])
            entry["sha256"] = hashlib.sha256(files["databases/mushrooms.db"]).hexdigest()
    with ZipFile(broken, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode())
        for name, payload in files.items():
            target.writestr(name, payload)
    before = schema.get_database_path().read_bytes()
    with pytest.raises(FullRestoreError):
        _restore_full_backup(broken, app_version="test", safety_backup_path=tmp_path / "safety.sporely")
    assert schema.get_database_path().read_bytes() == before


def test_restore_rejects_database_missing_required_current_column(
    installation, tmp_path
):
    archive = _make_backup(tmp_path)
    broken = tmp_path / "missing-column.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    staged = tmp_path / "missing-column.db"
    staged.write_bytes(members["databases/mushrooms.db"])
    with sqlite3.connect(staged) as connection:
        connection.execute("ALTER TABLE observations DROP COLUMN date")
    members["databases/mushrooms.db"] = staged.read_bytes()
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] == "databases/mushrooms.db":
            entry["size"] = len(members["databases/mushrooms.db"])
            entry["sha256"] = hashlib.sha256(
                members["databases/mushrooms.db"]
            ).hexdigest()
    with ZipFile(broken, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)

    before = schema.get_database_path().read_bytes()
    with pytest.raises(FullRestoreError, match="required columns missing.*date"):
        _restore_full_backup(
            broken,
            app_version="test",
            safety_backup_path=tmp_path / "safety.sporely",
        )
    assert schema.get_database_path().read_bytes() == before


def test_restore_migrates_supported_older_staged_schema(installation, tmp_path):
    archive = _make_backup(tmp_path)
    older = tmp_path / "older-supported.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    staged = tmp_path / "older-main.db"
    staged.write_bytes(members["databases/mushrooms.db"])
    with sqlite3.connect(staged) as connection:
        connection.execute("DROP TABLE portable_import_provenance")
        connection.execute(
            "ALTER TABLE observations DROP COLUMN portable_cloud_identity_pending"
        )
    members["databases/mushrooms.db"] = staged.read_bytes()
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] == "databases/mushrooms.db":
            entry["size"] = len(members["databases/mushrooms.db"])
            entry["sha256"] = hashlib.sha256(members["databases/mushrooms.db"]).hexdigest()
    with ZipFile(older, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)

    _restore_full_backup(
        older,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
    )

    with sqlite3.connect(schema.get_database_path()) as connection:
        assert "portable_cloud_identity_pending" in {
            row[1] for row in connection.execute("PRAGMA table_info(observations)")
        }
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='portable_import_provenance'"
        ).fetchone()


def test_validator_rejects_duplicate_qsettings_namespaces(installation, tmp_path):
    archive = tmp_path / "source.sporely"
    create_full_backup(
        archive,
        app_version="test",
        qsettings_values={
            ("Sporely", "SpeciesPlate"): {"ins_r": 99},
        },
    )
    broken = tmp_path / "duplicate-qsettings.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    payload = json.loads(members["data/qsettings.json"])
    payload["namespaces"].append(dict(payload["namespaces"][0]))
    members["data/qsettings.json"] = json.dumps(payload).encode("utf-8")
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] == "data/qsettings.json":
            entry["size"] = len(members["data/qsettings.json"])
            entry["sha256"] = hashlib.sha256(
                members["data/qsettings.json"]
            ).hexdigest()
    with ZipFile(broken, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, member in members.items():
            target.writestr(name, member)

    with pytest.raises(ArchiveValidationError, match="duplicate QSettings namespace"):
        validate_full_backup(broken)
