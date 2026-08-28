from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile
from dataclasses import replace

import pytest

from database import schema
from utils.archive import portable_import
from utils.archive.portable_export import export_observations
from utils.archive.portable_import import (
    PortableIdentityConflictError,
    PortableImportError,
    import_portable_archive,
)
from utils.archive.manifest import ArchiveManifest


CALIBRATION_UUID = "11111111-1111-4111-8111-111111111111"
ASSET_UUID = "22222222-2222-4222-8222-222222222222"


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


def _make_archive(
    monkeypatch,
    root: Path,
    *,
    archive_id: str,
    working_bytes: bytes = b"working",
    include_original: bool = True,
    missing_working: bool = False,
    cache_shares_working: bool = False,
    malformed_calibration_metadata: bool = False,
    null_calibration_metadata: bool = False,
    calibration_metadata_cache: bool = False,
) -> Path:
    main, _reference = _database_pair(monkeypatch, root / "source")
    managed = root / "source-managed"
    managed.mkdir()
    working = managed / "same-name.jpg"
    if not missing_working:
        working.write_bytes(working_bytes)
    original = root / "external" / "same-name.raw"
    if include_original:
        original.parent.mkdir()
        original.write_bytes(b"authoritative-original")
    calibration = root / "external-calibration" / "same-name.tif"
    calibration.parent.mkdir()
    calibration.write_bytes(b"calibration-working")
    calibration_original = root / "external-calibration" / "same-name.raw"
    calibration_original.write_bytes(b"calibration-original")
    companion = root / "external-calibration" / "companion.dat"
    companion.write_bytes(b"calibration-companion")
    cache = root / "cache" / "same-name.webp"
    cache.parent.mkdir()
    cache.write_bytes(b"must-not-import")
    schema.save_app_settings({"images_dir": str(managed)})

    with sqlite3.connect(main) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-27')")
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            "image_filepath, measurements_json) VALUES (10, ?, '100X', '2026-08-27', "
            "0.1, ?, ?)",
            (
                CALIBRATION_UUID,
                str(calibration),
                (None if null_calibration_metadata else
                    "{/source/invalid"
                    if malformed_calibration_metadata
                    else json.dumps({"images": [{
                        "source_path": str(calibration),
                        "companion_paths": [str(companion)],
                        **({"source_role": "cloud_recovery_cache"} if calibration_metadata_cache else {}),
                    }]})
                ),
            ),
        )
        connection.execute(
            "INSERT INTO images "
            "(id, observation_id, filepath, original_filepath, calibration_id, "
            "source_role, file_purpose) VALUES (101, 1, ?, ?, 10, "
            "'local_original', 'authoritative')",
            (str(working), str(original) if include_original else str(root / "missing.raw")),
        )
        connection.execute(
            "INSERT INTO images "
            "(id, observation_id, filepath, source_role, file_purpose) "
            "VALUES (102, 1, ?, 'cloud_recovery_cache', 'cache')",
            (str(working) if cache_shares_working else str(cache),),
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(id, asset_uuid, calibration_id, calibration_uuid, role, source_role, "
            "file_purpose, local_path, original_path, metadata_json) "
            "VALUES (201, ?, 10, ?, 'source', 'local_original', 'authoritative', ?, ?, ?)",
            (
                ASSET_UUID,
                CALIBRATION_UUID,
                str(calibration),
                str(calibration_original),
                None if null_calibration_metadata else json.dumps({
                    "source_path": str(calibration),
                    "original_path": str(calibration_original),
                    "companion_paths": [str(companion)],
                }),
            ),
        )
        connection.execute(
            "INSERT INTO session_logs "
            "(id, observation_id, session_id, event_type, metadata_json) "
            "VALUES (301, 1, 'asset-session', 'image_imported', ?)",
            (json.dumps({"image_id": 101, "filepath": str(working)}),),
        )
        connection.commit()

    archive = root / f"{archive_id}.sporely"
    export_observations({1}, archive, app_version="test", archive_id=archive_id)
    return archive


def _import(monkeypatch, root: Path, archive: Path):
    main, reference = _database_pair(monkeypatch, root / "destination")
    assets = root / "destination-assets"
    result = import_portable_archive(
        archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=assets,
    )
    return main, assets, result


def _inside(path: str | None, root: Path) -> bool:
    return bool(path) and root.resolve() in Path(path).resolve().parents


def test_colliding_source_filenames_receive_distinct_managed_names(monkeypatch, tmp_path):
    first_archive = _make_archive(
        monkeypatch, tmp_path / "first", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        working_bytes=b"first",
    )
    second_archive = _make_archive(
        monkeypatch, tmp_path / "second", archive_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        working_bytes=b"second",
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    first = import_portable_archive(
        first_archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )
    second = import_portable_archive(
        second_archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )

    with sqlite3.connect(main) as connection:
        paths = [Path(row[0]) for row in connection.execute(
            "SELECT filepath FROM images WHERE id IN (?, ?) ORDER BY id",
            (first.image_id_map[101], second.image_id_map[101]),
        )]
    assert len(set(paths)) == 2
    assert {path.read_bytes() for path in paths} == {b"first", b"second"}
    assert all(_inside(str(path), assets) and path.name != "same-name.jpg" for path in paths)


def test_repeated_import_reuses_rows_and_assets(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    first = import_portable_archive(
        archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )
    before = {path.relative_to(assets): path.read_bytes() for path in assets.rglob("*") if path.is_file()}
    second = import_portable_archive(
        archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )
    after = {path.relative_to(assets): path.read_bytes() for path in assets.rglob("*") if path.is_file()}

    assert second.image_id_map == first.image_id_map
    assert second.new_item_counts == {}
    assert after == before


def test_repeated_import_rejects_corrupt_managed_asset_without_overwrite(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    result = import_portable_archive(
        archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )
    with sqlite3.connect(main) as connection:
        managed = Path(connection.execute(
            "SELECT filepath FROM images WHERE id=?", (result.image_id_map[101],)
        ).fetchone()[0])
    managed.write_bytes(b"corrupt-destination")

    with pytest.raises(PortableImportError, match="destination conflict"):
        import_portable_archive(
            archive, destination_main_database=main,
            destination_reference_database=reference, destination_assets_root=assets,
        )
    assert managed.read_bytes() == b"corrupt-destination"
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_reused_stable_calibration_preserves_authoritative_destination_paths(
    monkeypatch, tmp_path
):
    first_archive = _make_archive(
        monkeypatch, tmp_path / "first", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    second_archive = _make_archive(
        monkeypatch, tmp_path / "second", archive_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    first = import_portable_archive(
        first_archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )
    preserved = tmp_path / "destination-authoritative" / "preserved.raw"
    preserved.parent.mkdir()
    preserved.write_bytes(b"calibration-original")
    with sqlite3.connect(main) as connection:
        connection.execute(
            "UPDATE calibration_assets SET original_path=? WHERE id=?",
            (str(preserved), first.calibration_asset_id_map[201]),
        )
        connection.commit()
    calibration_files_before = {
        path for path in (assets / "calibrations").rglob("*") if path.is_file()
    }

    second = import_portable_archive(
        second_archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )

    assert second.calibration_id_map[10] == first.calibration_id_map[10]
    assert second.calibration_asset_id_map[201] == first.calibration_asset_id_map[201]
    with sqlite3.connect(main) as connection:
        assert connection.execute(
            "SELECT original_path FROM calibration_assets WHERE id=?",
            (first.calibration_asset_id_map[201],),
        ).fetchone()[0] == str(preserved)
    assert {path for path in (assets / "calibrations").rglob("*") if path.is_file()} == calibration_files_before


def test_external_originals_and_calibration_assets_are_materialized_and_rewritten(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, assets, result = _import(monkeypatch, tmp_path, archive)

    with sqlite3.connect(main) as connection:
        image = connection.execute(
            "SELECT filepath, original_filepath FROM images WHERE id=?",
            (result.image_id_map[101],),
        ).fetchone()
        calibration = connection.execute(
            "SELECT image_filepath, measurements_json FROM calibrations WHERE id=?",
            (result.calibration_id_map[10],),
        ).fetchone()
        asset = connection.execute(
            "SELECT local_path, original_path, metadata_json FROM calibration_assets WHERE id=?",
            (result.calibration_asset_id_map[201],),
        ).fetchone()
        session_metadata = connection.execute(
            "SELECT metadata_json FROM session_logs WHERE observation_id=?",
            (result.observation_id_map[1],),
        ).fetchone()[0]
    paths = [image[0], image[1], calibration[0], asset[0], asset[1]]
    assert all(_inside(path, assets) for path in paths)
    assert Path(image[1]).read_bytes() == b"authoritative-original"
    assert Path(asset[1]).read_bytes() == b"calibration-original"
    embedded = json.dumps([json.loads(calibration[1]), json.loads(asset[2])])
    assert str(tmp_path / "archive") not in embedded
    assert "same-name" not in " ".join(Path(path).name for path in paths)
    assert json.loads(session_metadata)["filepath"] == image[0]
    assert all(str(assets.resolve()) in value for value in (
        json.loads(calibration[1])["images"][0]["source_path"],
        json.loads(calibration[1])["images"][0]["companion_paths"][0],
        json.loads(asset[2])["source_path"],
        json.loads(asset[2])["original_path"],
        json.loads(asset[2])["companion_paths"][0],
    ))


def test_missing_and_cache_assets_leave_no_source_paths_or_files(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        include_original=False, missing_working=True,
    )
    main, assets, result = _import(monkeypatch, tmp_path, archive)

    with sqlite3.connect(main) as connection:
        authoritative = connection.execute(
            "SELECT filepath, original_filepath FROM images WHERE id=?",
            (result.image_id_map[101],),
        ).fetchone()
        cache = connection.execute(
            "SELECT filepath FROM images WHERE id=?", (result.image_id_map[102],)
        ).fetchone()[0]
    assert authoritative == ("", None)
    assert cache == ""
    assert b"must-not-import" not in b"".join(
        path.read_bytes() for path in assets.rglob("*") if path.is_file()
    )


def test_cache_row_sharing_source_path_cannot_erase_authoritative_session_path(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        cache_shares_working=True,
    )
    main, assets, result = _import(monkeypatch, tmp_path, archive)
    with sqlite3.connect(main) as connection:
        authoritative = connection.execute(
            "SELECT filepath FROM images WHERE id=?", (result.image_id_map[101],)
        ).fetchone()[0]
        cache = connection.execute(
            "SELECT filepath FROM images WHERE id=?", (result.image_id_map[102],)
        ).fetchone()[0]
        metadata = json.loads(connection.execute(
            "SELECT metadata_json FROM session_logs WHERE observation_id=?",
            (result.observation_id_map[1],),
        ).fetchone()[0])
    assert _inside(authoritative, assets)
    assert cache == ""
    assert metadata["filepath"] == authoritative


def test_authoritative_excluded_status_is_rejected_before_mutation(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    tampered = tmp_path / "tampered.sporely"
    target_member = "portable/assets/images/101/working.jpg"
    with ZipFile(archive) as source:
        manifest = json.loads(source.read("manifest.json"))
        for entry in manifest["files"]:
            if entry["path"] == target_member:
                entry.clear()
                entry.update({"path": target_member, "status": "excluded_by_policy"})
                break
        with ZipFile(tampered, "w", ZIP_DEFLATED) as target:
            target.writestr(
                "manifest.json",
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
            )
            for info in source.infolist()[1:]:
                if info.filename != target_member:
                    target.writestr(info.filename, source.read(info.filename))
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")

    with pytest.raises(PortableImportError, match="authoritative asset was excluded"):
        import_portable_archive(
            tampered, destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=tmp_path / "destination-assets",
        )
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0


def test_malformed_calibration_path_metadata_is_rejected_before_mutation(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        malformed_calibration_metadata=True,
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")

    with pytest.raises(PortableImportError, match="not valid JSON"):
        import_portable_archive(
            archive, destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=tmp_path / "destination-assets",
        )
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0


def test_null_calibration_metadata_round_trips(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        null_calibration_metadata=True,
    )
    main, _assets, result = _import(monkeypatch, tmp_path, archive)
    with sqlite3.connect(main) as connection:
        assert connection.execute(
            "SELECT measurements_json FROM calibrations WHERE id=?",
            (result.calibration_id_map[10],),
        ).fetchone()[0] is None
        assert connection.execute(
            "SELECT metadata_json FROM calibration_assets WHERE id=?",
            (result.calibration_asset_id_map[201],),
        ).fetchone()[0] is None


def test_calibration_metadata_cache_provenance_is_excluded(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        calibration_metadata_cache=True,
    )
    main, _assets, result = _import(monkeypatch, tmp_path, archive)
    with sqlite3.connect(main) as connection:
        metadata = json.loads(connection.execute(
            "SELECT measurements_json FROM calibrations WHERE id=?",
            (result.calibration_id_map[10],),
        ).fetchone()[0])
    assert metadata["images"][0]["source_path"] is None
    assert metadata["images"][0]["companion_paths"] == [None]


def test_replay_with_different_asset_root_fails_explicitly(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    import_portable_archive(
        archive, destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=tmp_path / "assets-a",
    )
    with pytest.raises(PortableImportError, match="asset root does not match"):
        import_portable_archive(
            archive, destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=tmp_path / "assets-b",
        )


def test_next_import_cleans_crash_orphan_journal(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = (tmp_path / "destination-assets").resolve()
    orphan = assets / "images" / "orphan.jpg"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"orphan")
    journal_root = assets / ".portable-import-journals"
    journal_root.mkdir()
    (journal_root / "interrupted.json").write_text(
        json.dumps([str(orphan)]) + "\n", encoding="utf-8"
    )

    import_portable_archive(
        archive, destination_main_database=main,
        destination_reference_database=reference, destination_assets_root=assets,
    )

    assert not orphan.exists()
    assert not journal_root.exists()


def test_malicious_asset_member_is_rejected_before_live_mutation(monkeypatch, tmp_path):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    malicious = tmp_path / "malicious.sporely"
    with ZipFile(archive) as source, ZipFile(malicious, "w", ZIP_DEFLATED) as target:
        for info in source.infolist():
            target.writestr(info.filename, source.read(info.filename))
        target.writestr("../escape.jpg", b"escape")
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"

    with pytest.raises(PortableImportError, match="archive|path|ZIP"):
        import_portable_archive(
            malicious, destination_main_database=main,
            destination_reference_database=reference, destination_assets_root=assets,
        )
    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
    assert not assets.exists()
    assert not (tmp_path / "escape.jpg").exists()


def test_partial_asset_promotion_cleans_files_and_rolls_back_database(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive", archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    real_promote = portable_import._promote_staged_asset
    calls = 0

    def fail_second(source: Path, destination: Path) -> bool:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected partial-copy failure")
        return real_promote(source, destination)

    monkeypatch.setattr(portable_import, "_promote_staged_asset", fail_second)
    with pytest.raises(PortableImportError, match="partial-copy failure"):
        import_portable_archive(
            archive, destination_main_database=main,
            destination_reference_database=reference, destination_assets_root=assets,
        )

    with sqlite3.connect(main) as connection:
        assert connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM portable_import_provenance"
        ).fetchone()[0] == 0
    assert not any(path.is_file() for path in assets.rglob("*")) if assets.exists() else True


def test_replay_rejects_changed_asset_bytes_for_same_archive_identity(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive",
        archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )
    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    first = import_portable_archive(
        archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=assets,
    )
    with sqlite3.connect(main) as connection:
        destination_image = Path(connection.execute(
            "SELECT filepath FROM images WHERE id=?",
            (first.image_id_map[101],),
        ).fetchone()[0])
    original_bytes = destination_image.read_bytes()

    changed = tmp_path / "changed-assets.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    asset_name = "portable/assets/images/101/working.jpg"
    members[asset_name] = b"changed-working-bytes"
    files = tuple(
        replace(
            entry,
            size=len(members[asset_name]),
            sha256=hashlib.sha256(members[asset_name]).hexdigest(),
        )
        if entry.path == asset_name else entry
        for entry in manifest.files
    )
    with ZipFile(changed, "w", ZIP_DEFLATED) as target:
        target.writestr("manifest.json", replace(manifest, files=files).to_json_bytes())
        for name, payload in members.items():
            target.writestr(name, payload)

    with pytest.raises(PortableImportError, match="conflicting source content inventory"):
        import_portable_archive(
            changed,
            destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=assets,
        )
    assert destination_image.read_bytes() == original_bytes


def test_stable_calibration_reuse_rejects_different_authoritative_bytes(
    monkeypatch, tmp_path
):
    archive = _make_archive(
        monkeypatch, tmp_path / "archive",
        archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )
    changed = tmp_path / "different-calibration.sporely"
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    member = "portable/assets/calibrations/records/10/working.tif"
    members[member] = b"different-calibration-bytes"
    files = tuple(
        replace(
            entry,
            size=len(members[member]),
            sha256=hashlib.sha256(members[member]).hexdigest(),
        )
        if entry.path == member else entry
        for entry in manifest.files
    )
    changed_manifest = replace(
        manifest,
        archive_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        files=files,
    )
    with ZipFile(changed, "w", ZIP_DEFLATED) as target:
        target.writestr("manifest.json", changed_manifest.to_json_bytes())
        for name, payload in members.items():
            target.writestr(name, payload)

    main, reference = _database_pair(monkeypatch, tmp_path / "destination")
    assets = tmp_path / "destination-assets"
    import_portable_archive(
        archive,
        destination_main_database=main,
        destination_reference_database=reference,
        destination_assets_root=assets,
    )

    with pytest.raises(PortableIdentityConflictError, match="asset bytes"):
        import_portable_archive(
            changed,
            destination_main_database=main,
            destination_reference_database=reference,
            destination_assets_root=assets,
        )
