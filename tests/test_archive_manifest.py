import json

import pytest

from utils.archive.manifest import (
    ArchiveManifest,
    ManifestError,
    ManifestFile,
    build_manifest,
)


def _manifest():
    return build_manifest(
        mode="full_backup",
        archive_id="fixed-id",
        created_at="2026-08-27T12:00:00Z",
        app_version="1.2.3",
        source_platform="test-platform",
        contents={"measurements": 3, "observations": 1, "images": 2},
        files=[
            ManifestFile("data/objectives.json", "missing_at_source"),
            ManifestFile("databases/mushrooms.db", "included", 7, "a" * 64),
        ],
    )


def test_manifest_serialization_is_deterministic_and_round_trips():
    manifest = _manifest()
    payload = manifest.to_json_bytes()
    assert payload == manifest.to_json_bytes()
    assert payload.endswith(b"\n")
    assert ArchiveManifest.from_json(payload) == manifest
    decoded = json.loads(payload)
    assert decoded["schema_version"] is None
    assert [item["path"] for item in decoded["files"]] == [
        "data/objectives.json", "databases/mushrooms.db",
    ]


@pytest.mark.parametrize(
    ("mode", "identity"),
    [("full_backup", "portable"), ("portable_observations", "preserve"), ("other", "preserve")],
)
def test_manifest_rejects_invalid_mode_identity_pairs(mode, identity):
    with pytest.raises(ManifestError):
        ArchiveManifest(
            mode=mode, identity_policy=identity, archive_id="id", created_at="now",
            app_version="1", source_platform="test", contents={}, files=(),
        )


@pytest.mark.parametrize(
    "entry",
    [
        lambda: ManifestFile("manifest.json", "included", 1, "a" * 64),
        lambda: ManifestFile("../escape", "included", 1, "a" * 64),
        lambda: ManifestFile("file", "included", -1, "a" * 64),
        lambda: ManifestFile("file", "included", 1, "A" * 64),
        lambda: ManifestFile("file", "missing_at_source", 0, None),
        lambda: ManifestFile("file", "unknown"),
    ],
)
def test_manifest_rejects_invalid_file_entries(entry):
    with pytest.raises(ManifestError):
        entry()


def test_manifest_parser_rejects_unknown_or_missing_fields():
    value = _manifest().to_dict()
    value["extra"] = True
    with pytest.raises(ManifestError):
        ArchiveManifest.from_json(json.dumps(value))
    del value["extra"]
    del value["format"]
    with pytest.raises(ManifestError):
        ArchiveManifest.from_json(json.dumps(value))


def test_manifest_rejects_duplicate_and_case_fold_colliding_paths():
    common = dict(
        mode="full_backup", identity_policy="preserve", archive_id="id",
        created_at="now", app_version="1", source_platform="test", contents={},
    )
    with pytest.raises(ManifestError):
        ArchiveManifest(files=(ManifestFile("a", "missing_at_source"),) * 2, **common)
    with pytest.raises(ManifestError):
        ArchiveManifest(
            files=(ManifestFile("A", "missing_at_source"), ManifestFile("a", "missing_at_source")),
            **common,
        )
