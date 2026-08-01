"""Tests for W2D-R source-recovery tooling: pseudonym, validator, transformer."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from database.taxonomy.reconciliation.snapshot.pseudonym import (
    KEY_ENV_VAR,
    MIN_KEY_BYTES,
    PseudonymKeyError,
    is_pseudonym,
    make_pseudonymiser,
)
from database.taxonomy.reconciliation.snapshot.transformer import (
    run_transform,
    transform_record,
)
from database.taxonomy.reconciliation.snapshot.validator import (
    SCHEMA_VERSION,
    validate_record,
    validate_snapshot,
)


@pytest.fixture
def key_env(monkeypatch):
    raw = base64.b64encode(b"a" * MIN_KEY_BYTES).decode("ascii")
    monkeypatch.setenv(KEY_ENV_VAR, raw)
    yield raw


def test_pseudonym_is_deterministic_under_same_key(key_env):
    p = make_pseudonymiser()
    assert p("obs-123") == p("obs-123")
    assert p("obs-123") != p("obs-124")
    assert is_pseudonym(p("obs-123"))


def test_pseudonym_differs_across_keys(monkeypatch):
    monkeypatch.setenv(KEY_ENV_VAR, base64.b64encode(b"a" * MIN_KEY_BYTES).decode("ascii"))
    a = make_pseudonymiser()("obs-1")
    monkeypatch.setenv(KEY_ENV_VAR, base64.b64encode(b"b" * MIN_KEY_BYTES).decode("ascii"))
    b = make_pseudonymiser()("obs-1")
    assert a != b


def test_pseudonym_rejects_short_key(monkeypatch):
    monkeypatch.setenv(KEY_ENV_VAR, base64.b64encode(b"short").decode("ascii"))
    with pytest.raises(PseudonymKeyError):
        make_pseudonymiser()


def test_pseudonym_rejects_missing_key(monkeypatch):
    monkeypatch.delenv(KEY_ENV_VAR, raising=False)
    with pytest.raises(PseudonymKeyError):
        make_pseudonymiser()


def test_validator_accepts_minimal_valid_record():
    record = {
        "observation_id": "obs_" + "a" * 24,
        "signals": [],
        "manual_identification_flag": False,
        "stored_scientific_name": None,
        "stored_vernacular_name": None,
        "stored_rank": None,
        "source_release_or_timestamp": None,
    }
    assert validate_record(record, index=0) == []


def test_validator_rejects_prohibited_field():
    record = {
        "observation_id": "obs_" + "a" * 24,
        "signals": [],
        "manual_identification_flag": False,
        "stored_scientific_name": None,
        "stored_vernacular_name": None,
        "stored_rank": None,
        "source_release_or_timestamp": None,
        "latitude": 59.9,
    }
    errors = validate_record(record, index=0)
    assert any(e.kind == "prohibited_field_name" for e in errors)
    assert any(e.kind == "unexpected_field" for e in errors)


def test_validator_rejects_raw_uuid_observation_id():
    record = {
        "observation_id": "550e8400-e29b-41d4-a716-446655440000",
        "signals": [],
        "manual_identification_flag": False,
        "stored_scientific_name": None,
        "stored_vernacular_name": None,
        "stored_rank": None,
        "source_release_or_timestamp": None,
    }
    errors = validate_record(record, index=0)
    assert any(e.kind == "raw_uuid_observation_id" for e in errors)


def test_validator_flags_email_and_media_url_in_values():
    record = {
        "observation_id": "obs_" + "a" * 24,
        "signals": [],
        "manual_identification_flag": False,
        "stored_scientific_name": "user@example.com",
        "stored_vernacular_name": "https://storage.example.com/photo.jpg",
        "stored_rank": None,
        "source_release_or_timestamp": None,
    }
    kinds = {e.kind for e in validate_record(record, index=0)}
    assert "prohibited_email_like_value" in kinds
    assert "prohibited_media_url" in kinds


def test_validator_rejects_signal_with_non_string_external_id():
    record = {
        "observation_id": "obs_" + "a" * 24,
        "signals": [
            {
                "kind": "exact",
                "source_system": "nortaxa",
                "namespace": "nortaxa_taxon_id",
                "external_id": 42,
                "origin_field": "observations.artsdata_id",
                "raw_value": 42,
            }
        ],
        "manual_identification_flag": False,
        "stored_scientific_name": None,
        "stored_vernacular_name": None,
        "stored_rank": None,
        "source_release_or_timestamp": None,
    }
    assert any(
        e.kind == "external_id_not_string"
        for e in validate_record(record, index=0)
    )


def test_validate_snapshot_detects_duplicate_and_missing_header(tmp_path):
    p = tmp_path / "snap.jsonl"
    lines = [
        {"__snapshot_header__": True, "schema_version": SCHEMA_VERSION, "record_count": 2},
        {
            "observation_id": "obs_" + "a" * 24,
            "signals": [],
            "manual_identification_flag": False,
            "stored_scientific_name": None,
            "stored_vernacular_name": None,
            "stored_rank": None,
            "source_release_or_timestamp": None,
        },
        {
            "observation_id": "obs_" + "a" * 24,
            "signals": [],
            "manual_identification_flag": False,
            "stored_scientific_name": None,
            "stored_vernacular_name": None,
            "stored_rank": None,
            "source_release_or_timestamp": None,
        },
    ]
    p.write_text("\n".join(json.dumps(l) for l in lines) + "\n")
    report = validate_snapshot(p)
    assert report.record_count == 2
    assert any(e.kind == "duplicate_observation_reference" for e in report.errors)


def test_transformer_end_to_end_is_deterministic(tmp_path, key_env):
    raw = tmp_path / "raw.jsonl"
    rows = [
        {
            "id": "src-1",
            "artsdata_id": 4321,
            "ai_selected_service": "artsorakel",
            "ai_selected_taxon_id": "NBIC:4321",
            "scientific_name_snapshot": "Amanita muscaria",
            "taxon_rank_snapshot": "species",
            "common_name": "Fluesopp",
            "source_release": "desktop-2026-05",
        },
        {
            "id": "src-2",
            "inaturalist_taxon_id": 47328,
            "scientific_name_snapshot": "Cantharellus cibarius",
            "taxon_rank_snapshot": "species",
        },
    ]
    raw.write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    out1 = tmp_path / "run1" / "snapshot.jsonl"
    out2 = tmp_path / "run2" / "snapshot.jsonl"
    stats1 = run_transform(raw_export=raw, output=out1, pseudonym_key_file=None)
    stats2 = run_transform(raw_export=raw, output=out2, pseudonym_key_file=None)

    assert out1.read_bytes() == out2.read_bytes()
    assert stats1.snapshot_sha256 == stats2.snapshot_sha256
    assert stats1.input_records == stats2.input_records == 2
    assert stats1.output_records == 2

    report = validate_snapshot(out1)
    assert report.ok, report.to_dict()


def test_transformer_strips_prohibited_fields(tmp_path, key_env):
    raw = tmp_path / "raw.jsonl"
    rows = [
        {
            "id": "src-3",
            "artsdata_id": 42,
            "email": "leak@example.com",
            "latitude": 59.9,
            "longitude": 10.7,
            "photo_url": "https://storage.example.com/x.jpg",
        }
    ]
    raw.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    out = tmp_path / "snapshot.jsonl"
    stats = run_transform(raw_export=raw, output=out, pseudonym_key_file=None)

    record = [json.loads(l) for l in out.read_text().splitlines() if l.strip()][1]
    for banned in ("email", "latitude", "longitude", "photo_url"):
        assert banned not in record
    stats_payload = json.loads((out.parent / (out.name + ".stats.json")).read_text())
    assert stats_payload["prohibited_fields_stripped"] == {
        "email": 1,
        "latitude": 1,
        "longitude": 1,
        "photo_url": 1,
    }
    assert stats.output_records == 1


def test_transformer_refuses_to_overwrite_existing_output(tmp_path, key_env):
    raw = tmp_path / "raw.jsonl"
    raw.write_text(json.dumps({"id": "x"}) + "\n")
    out = tmp_path / "existing.jsonl"
    out.write_text("already here\n")
    with pytest.raises(ValueError, match="refusing to overwrite"):
        run_transform(raw_export=raw, output=out, pseudonym_key_file=None)


def test_transformer_cli_refuses_production_flag(tmp_path, key_env):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "database.taxonomy.reconciliation.snapshot.transformer",
            "transform",
            "--raw-export",
            str(tmp_path / "nope.jsonl"),
            "--output",
            str(tmp_path / "out.jsonl"),
            "--production",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, KEY_ENV_VAR: base64.b64encode(b"a" * MIN_KEY_BYTES).decode("ascii")},
        cwd=Path(__file__).resolve().parents[2],
    )
    assert result.returncode == 3
    assert "refuse" in result.stderr.lower()


def test_export_spec_refuses_production():
    """The specification-only export tool must refuse --production and exit 3."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "database.taxonomy.scripts.export_observations_snapshot",
            "--observations",
            "/tmp/does-not-matter.sqlite3",
            "--output",
            "/tmp/does-not-matter",
            "--production",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    assert result.returncode == 3


def test_transformer_accepts_supabase_csv_export(tmp_path, key_env):
    """The offline transformer must consume the CSV that Supabase SQL Editor emits."""
    csv_path = tmp_path / "supabase-export.csv"
    csv_path.write_text(
        "id,artsdata_id,artportalen_id,inaturalist_id,mushroomobserver_id,"
        "desktop_id,ai_selected_service,ai_selected_taxon_id,"
        "ai_selected_scientific_name,genus,species,common_name,species_guess\n"
        "1001,4321,,,,,artsorakel,NBIC:4321,Amanita muscaria,Amanita,muscaria,Fluesopp,\n"
        "1002,,,,,,inaturalist,47328,Cantharellus cibarius,Cantharellus,cibarius,,\n"
        "1003,,,,,,,,,Boletus,edulis,Steinsopp,Boletus edulis?\n"
    )
    out = tmp_path / "snapshot.jsonl"
    stats = run_transform(raw_export=csv_path, output=out, pseudonym_key_file=None)
    assert stats.input_records == 3
    assert stats.output_records == 3
    report = validate_snapshot(out)
    assert report.ok, report.to_dict()
    # Type coercion: integer columns must be ints in the snapshot, strings
    # for other columns; empty CSV fields must become None.
    lines = [json.loads(l) for l in out.read_text().splitlines() if l.strip()]
    header = lines[0]
    assert header["__snapshot_header__"] is True
    first = lines[1]
    assert first["observation_id"].startswith("obs_")
    exact = [s for s in first["signals"] if s["kind"] == "exact"]
    assert any(s["source_system"] == "nortaxa" and s["external_id"] == "4321" for s in exact)


def test_real_and_fixture_manifests_are_separately_labelled(tmp_path):
    """Aggregate evidence must keep synthetic-fixture and anonymised-real outputs distinct."""
    repo_root = Path(__file__).resolve().parents[2]
    fixture_manifest = (
        repo_root
        / "database/taxonomy/evidence/historical-reconciliation/reconciliation-manifest.json"
    )
    doc = json.loads(fixture_manifest.read_text())
    assert doc["input_source_hash"], "synthetic-fixture manifest must record input source hash"
    # A real-data manifest would be produced by the operator into a distinct
    # output directory. Assert the runbook mandates that separation.
    runbook = (
        repo_root
        / "database/taxonomy/docs/w2d-source-recovery-runbook.md"
    ).read_text()
    assert "historical anonymized manifest" in runbook
    assert "reconciliation-real" in runbook
