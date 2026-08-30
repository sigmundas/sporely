from __future__ import annotations

import json
import hashlib
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialogButtonBox

from database import schema
from ui.portable_import_dialog import PortableImportDialog
from utils.archive.portable_export import export_observations
from utils.archive.checksums import sha256_file
from utils.archive.manifest import ArchiveManifest
from utils.archive.portable_import import (
    PortableImportError,
    import_portable_archive,
    preview_portable_archive,
)
from tests.test_curated_reference_forks import bundle_row


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


def _database_pair(monkeypatch, root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True)
    main = root / "mushrooms.db"
    reference = root / "reference_values.db"
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", main)
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", reference)
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    return main, reference


def _snapshot(work_id: str, treatment_id: str, set_id: str) -> str:
    return json.dumps({
        "schema_version": 1,
        "reference_work_id": work_id,
        "reference_treatment_id": treatment_id,
        "reference_measurement_set_id": set_id,
        "reference_revision": 1,
        "short_label": work_id,
        "full_citation": work_id,
        "work_type": "book",
        "year": None,
        "doi": None,
        "isbn": None,
        "taxon_id": None,
        "name_as_published": work_id,
        "locator_text": None,
        "page_from": None,
        "page_to": None,
        "character": "spore_size",
        "data_kind": "range",
        "raw_text": None,
        "measurements": {},
        "method": {},
        "raw_points": None,
    }, sort_keys=True)


def _archive(monkeypatch, tmp_path: Path) -> Path:
    main, reference = _database_pair(monkeypatch, tmp_path / "source")
    schema.save_app_settings({"images_dir": str(tmp_path / "source-images")})
    schema.get_objectives_path().write_text("{}", encoding="utf-8")
    with sqlite3.connect(main) as connection:
        connection.executemany(
            "INSERT INTO observations (id, date, genus, species) VALUES (?, ?, ?, ?)",
            [(1, "2026-08-01", "Amanita", "muscaria"), (2, "2026-08-02", "Russula", "emetica")],
        )
        connection.executemany(
            "INSERT INTO calibrations (id, calibration_uuid, objective_key, calibration_date, microns_per_pixel) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (10, "11111111-1111-4111-8111-111111111111", "100X", "2026-08-01", 0.1),
                (20, "22222222-2222-4222-8222-222222222222", "40X", "2026-08-02", 0.2),
            ],
        )
        connection.executemany(
            "INSERT INTO images (id, observation_id, filepath, calibration_id) VALUES (?, ?, '', ?)",
            [(101, 1, 10), (102, 1, 10), (201, 2, 20)],
        )
        connection.executemany(
            "INSERT INTO spore_measurements (id, image_id, length_um) VALUES (?, ?, ?)",
            [(1001, 101, 10.0), (1002, 102, 11.0), (2001, 201, 12.0)],
        )
        connection.executemany(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, reference_revision, snapshot_json) "
            "VALUES (?, ?, ?, 'compared', 1, ?)",
            [("use-a", 1, "set-a", _snapshot("work-a", "treatment-a", "set-a")),
             ("use-b", 2, "set-b", _snapshot("work-b", "treatment-b", "set-b"))],
        )
        connection.commit()
    with sqlite3.connect(reference) as connection:
        connection.executemany(
            "INSERT INTO reference_works (id, type, title, short_label, revision) VALUES (?, 'book', ?, ?, 1)",
            [("work-a", "Work A", "A"), ("work-b", "Work B", "B")],
        )
        connection.executemany(
            "INSERT INTO reference_taxon_treatments (id, reference_work_id, name_as_published, revision) "
            "VALUES (?, ?, ?, 1)",
            [("treatment-a", "work-a", "Amanita muscaria"), ("treatment-b", "work-b", "Russula emetica")],
        )
        connection.executemany(
            "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, revision) "
            "VALUES (?, ?, 'spore_size', 'range', 1)",
            [("set-a", "treatment-a"), ("set-b", "treatment-b")],
        )
        envelope = json.dumps(bundle_row(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        connection.execute(
            "INSERT INTO curated_reference_forks "
            "(curated_measurement_set_id,bundle_revision,sporely_taxon_id,reference_work_id,"
            "taxon_treatment_id,reference_measurement_set_id,source_envelope_json,source_sha256) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (bundle_row()["curated_measurement_set_id"], 2, 2_100_000_081,
             "work-a", "treatment-a", "set-a", envelope,
             hashlib.sha256(envelope.encode("utf-8")).hexdigest()),
        )
        connection.commit()
    archive = tmp_path / "observations.sporely"
    export_observations(
        {1, 2}, archive, app_version="test-version",
        archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        created_at="2026-08-27T12:00:00+00:00", source_platform="test-platform",
    )
    return archive


def test_preview_contents_and_changing_checkbox_closure(monkeypatch, tmp_path, qapp):
    archive = _archive(monkeypatch, tmp_path)
    preview = preview_portable_archive(archive)
    assert [(item.observation_id, item.name, item.image_count) for item in preview.observations] == [
        (2, "Russula emetica", 1), (1, "Amanita muscaria", 2)
    ]
    assert preview.closure_counts({1, 2}).__dict__ == {
        "observations": 2, "images": 3, "measurements": 3,
        "calibrations": 2, "references": 2,
    }
    dialog = PortableImportDialog(preview)
    assert dialog.selected_observation_ids() == {1, 2}
    dialog.observation_table.item(0, 0).setCheckState(Qt.Unchecked)
    assert dialog.selected_observation_ids() == {1}
    assert "1 observations, 2 images, 2 measurements, 1 calibration records, 1 references" in dialog.selection_summary.text()
    dialog.observation_table.item(1, 0).setCheckState(Qt.Unchecked)
    assert not dialog.button_box.button(QDialogButtonBox.Ok).isEnabled()
    dialog.reject()


def test_preview_and_cancel_do_not_touch_destinations(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    assets.mkdir()
    marker = assets / "marker"
    marker.write_bytes(b"unchanged")
    before = (main.read_bytes(), reference.read_bytes(), marker.read_bytes())
    preview = preview_portable_archive(archive)
    preview.closure_counts({1})
    assert (main.read_bytes(), reference.read_bytes(), marker.read_bytes()) == before
    assert not (assets / ".portable-import-journals").exists()


def test_corrupt_archive_fails_before_preview(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    corrupt = tmp_path / "corrupt.sporely"
    with ZipFile(archive) as source, ZipFile(corrupt, "w", ZIP_DEFLATED) as target:
        for name in source.namelist():
            payload = source.read(name)
            target.writestr(name, payload + b"corrupt" if name == "portable/objectives.json" else payload)
    with pytest.raises(PortableImportError, match="checksum"):
        preview_portable_archive(corrupt)


def test_semantically_dangling_archive_fails_before_preview(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    staging = tmp_path / "dangling-staging"
    staging.mkdir()
    with ZipFile(archive) as source:
        source.extractall(staging)
    main_database = staging / "portable/mushrooms.db"
    with sqlite3.connect(main_database) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("UPDATE images SET observation_id=999 WHERE id=201")
        connection.commit()
    manifest = ArchiveManifest.from_json((staging / "manifest.json").read_bytes())
    files = tuple(
        replace(entry, size=main_database.stat().st_size, sha256=sha256_file(main_database))
        if entry.path == "portable/mushrooms.db" else entry
        for entry in manifest.files
    )
    manifest = replace(manifest, files=files)
    dangling = tmp_path / "dangling.sporely"
    with ZipFile(dangling, "w", ZIP_DEFLATED) as target:
        target.writestr("manifest.json", manifest.to_json_bytes())
        for entry in files:
            if entry.status == "included":
                target.write(staging / entry.path, entry.path)
    with pytest.raises(PortableImportError, match="dependency closure"):
        preview_portable_archive(dangling)


def test_confirm_rejects_archive_changed_after_preview(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    preview = preview_portable_archive(archive)
    archive.write_bytes(archive.read_bytes() + b"changed")
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    with pytest.raises(PortableImportError, match="changed after it was previewed"):
        import_portable_archive(
            archive, destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=tmp_path / "assets", observation_ids={1},
            expected_archive_sha256=preview.archive_sha256,
        )


def test_subset_import_then_overlap_and_replay_preserve_identity(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    first = import_portable_archive(
        archive, destination_main_database=main, destination_reference_database=reference,
        destination_assets_root=assets, observation_ids={1},
    )
    overlap = import_portable_archive(
        archive, destination_main_database=main, destination_reference_database=reference,
        destination_assets_root=assets, observation_ids={1, 2},
    )
    replay = import_portable_archive(
        archive, destination_main_database=main, destination_reference_database=reference,
        destination_assets_root=assets, observation_ids={1},
    )
    assert overlap.observation_id_map[1] == first.observation_id_map[1]
    assert replay.observation_id_map == first.observation_id_map
    assert replay.new_item_counts == {}
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0] == 3
        assert set(connection.execute("SELECT cloud_id FROM images").fetchall()) == {(None,)}
        assert set(connection.execute("SELECT portable_cloud_identity_pending FROM observations").fetchall()) == {(1,)}
    with sqlite3.connect(reference) as connection:
        assert connection.execute("SELECT COUNT(*) FROM curated_reference_forks").fetchone()[0] == 1


def test_overlapping_subset_imports_reuse_shared_session_identity(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    with ZipFile(archive) as source:
        staging = tmp_path / "shared-session-staging"
        source.extractall(staging)
    staged_main = staging / "portable/mushrooms.db"
    with sqlite3.connect(staged_main) as connection:
        connection.executemany(
            "INSERT INTO session_logs "
            "(id, observation_id, session_id, event_type, metadata_json) "
            "VALUES (?, ?, 'shared-session', 'capture', '{}')",
            [(10, 1), (20, 2)],
        )
        connection.commit()
    manifest = ArchiveManifest.from_json((staging / "manifest.json").read_bytes())
    files = tuple(
        replace(entry, size=staged_main.stat().st_size, sha256=sha256_file(staged_main))
        if entry.path == "portable/mushrooms.db" else entry
        for entry in manifest.files
    )
    contents = dict(manifest.contents)
    contents["session_logs"] = contents.get("session_logs", 0) + 2
    shared_archive = tmp_path / "shared-session.sporely"
    with ZipFile(shared_archive, "w", ZIP_DEFLATED) as target:
        target.writestr(
            "manifest.json",
            replace(manifest, files=files, contents=contents).to_json_bytes(),
        )
        for entry in files:
            if entry.status == "included":
                target.write(staging / entry.path, entry.path)

    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    first = import_portable_archive(
        shared_archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=tmp_path / "assets",
        observation_ids={1},
    )
    second = import_portable_archive(
        shared_archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=tmp_path / "assets",
        observation_ids={2},
    )

    assert first.session_id_map["shared-session"] == second.session_id_map["shared-session"]
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM session_logs").fetchone()[0] == 2


def test_import_merges_required_objective_profile_into_destination(monkeypatch, tmp_path):
    archive = _archive(monkeypatch, tmp_path)
    staging = tmp_path / "objective-staging"
    with ZipFile(archive) as source:
        source.extractall(staging)
    main_payload = staging / "portable/mushrooms.db"
    objectives_payload = staging / "portable/objectives.json"
    with sqlite3.connect(main_payload) as connection:
        connection.execute("UPDATE images SET objective_name='CUSTOM' WHERE id=101")
        connection.commit()
    objective = {"name": "Custom 60X", "magnification": 60}
    objectives_payload.write_text(json.dumps({"CUSTOM": objective}), encoding="utf-8")
    manifest = ArchiveManifest.from_json((staging / "manifest.json").read_bytes())
    files = tuple(
        replace(entry, size=(staging / entry.path).stat().st_size,
                sha256=sha256_file(staging / entry.path))
        if entry.path in {"portable/mushrooms.db", "portable/objectives.json"}
        else entry
        for entry in manifest.files
    )
    custom_archive = tmp_path / "custom-objective.sporely"
    with ZipFile(custom_archive, "w", ZIP_DEFLATED) as target:
        target.writestr("manifest.json", replace(manifest, files=files).to_json_bytes())
        for entry in files:
            if entry.status == "included":
                target.write(staging / entry.path, entry.path)

    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    objectives_destination = tmp_path / "profile" / "objectives.json"
    import_portable_archive(
        custom_archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=tmp_path / "destination-assets",
        destination_objectives_path=objectives_destination,
        observation_ids={1},
    )

    assert json.loads(objectives_destination.read_text(encoding="utf-8"))[
        "CUSTOM"
    ] == objective
    assert not (main.parent / "objectives.json").exists()
