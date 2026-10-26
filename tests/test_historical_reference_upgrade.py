"""Lossless historical reuse on temporary databases only."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from database.reference_library import MeasurementSetRepository, TaxonTreatmentRepository
from tools.upgrade_historical_reference_library import prepare, apply
from tests.test_legacy_reference_migration import libs, _insert_legacy


def _backup(database: Path, folder: Path) -> None:
    folder.mkdir()
    target = folder / "reference_values.db"
    with sqlite3.connect(database) as source, sqlite3.connect(target) as destination:
        source.backup(destination)
    (folder / "manifest.json").write_text(json.dumps({"reference_values.db": {
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}}))


def test_reuses_actual_flammulina_values_without_inventing_a_mean(libs, tmp_path):
    _, database = libs
    legacy_id = _insert_legacy(database, genus="Flammulina", species="finlandica",
        source="Wang et al, 2018", length_min=7, length_p05=8, length_p50=9.88,
        length_p95=12, length_max=13, width_min=3, width_p05=3.5, width_p50=4.05,
        width_p95=4.5, width_max=5, q_min=1.75, q_p50=2.46, q_max=3.44)
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        original = dict(connection.execute("SELECT * FROM reference_values WHERE id=?", (legacy_id,)).fetchone())
    manifest = prepare(database)
    backup = tmp_path / "backup"
    _backup(database, backup)
    assert apply(database, manifest, backup)["created"] == 1
    assert apply(database, manifest, backup)["reused"] == 1
    stored = MeasurementSetRepository.find_by_legacy_reference_value_id(legacy_id)
    assert (stored.length_min, stored.length_core_min, stored.length_core_max, stored.length_max) == (7, 8, 12, 13)
    assert (stored.q_min, stored.q_max) == (1.75, 3.44)
    assert stored.length_mean is None and stored.width_mean is None and stored.q_mean is None
    preserved = json.loads(stored.notes.split("original row JSON: ")[1])
    assert preserved == original
    treatment = TaxonTreatmentRepository.get(stored.taxon_treatment_id)
    assert treatment.name_as_published == "Flammulina finlandica" and treatment.taxon_id is None
    assert len(MeasurementSetRepository.list_attachment_candidates()) == 1


def test_keeps_personal_observations_separate_and_prepare_readonly(libs):
    _, database = libs
    personal = _insert_legacy(database, genus="Flammulina", species="velutipes",
        source="Cloud: personal observation", metadata_json='{"cloud_dataset_kind":"observation"}')
    unnamed = _insert_legacy(database, genus="Flammulina", species="velutipes", source="")
    checksum = hashlib.sha256(database.read_bytes()).hexdigest()
    manifest = prepare(database)
    assert manifest["rows"] == [] and manifest["excluded_ids"] == [personal, unnamed]
    assert hashlib.sha256(database.read_bytes()).hexdigest() == checksum


def test_changed_source_refuses_whole_upgrade(libs, tmp_path):
    _, database = libs
    legacy_id = _insert_legacy(database, genus="Flammulina", species="velutipes",
                               source="Ripkova et al, 2010", length_min=5.9, length_max=8.1)
    manifest = prepare(database)
    backup = tmp_path / "backup"
    _backup(database, backup)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE reference_values SET length_min=6 WHERE id=?", (legacy_id,))
    with pytest.raises(ValueError, match="source rows changed"):
        apply(database, manifest, backup)
    assert MeasurementSetRepository.list_attachment_candidates() == []


def test_q_core_avg_and_original_publication_metadata_survive(libs, tmp_path):
    _, database = libs
    legacy_id = _insert_legacy(database, genus="Flammulina", species="velutipes",
        source="Original publication, 2010", q_min=1.5, q_max=3.5, q_avg=2.4,
        metadata_json='{"doi":"original DOI","notes":"original publication notes"}')
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE reference_values SET q_p05=2,q_p95=3 WHERE id=?", (legacy_id,))
    manifest = prepare(database)
    backup = tmp_path / "backup"
    _backup(database, backup)
    apply(database, manifest, backup)
    stored = MeasurementSetRepository.find_by_legacy_reference_value_id(legacy_id)
    assert (stored.q_core_min, stored.q_core_max, stored.q_min, stored.q_max, stored.q_mean) == (2, 3, 1.5, 3.5, 2.4)
    assert 'original DOI' in stored.notes and 'original publication notes' in stored.notes
