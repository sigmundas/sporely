"""Full measurement support through entry, reopen, reuse, plotting and transport."""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import replace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QWidget

from database.reference_library import (
    MeasurementSetRepository, ObservationReferenceUseRepository,
    ReferenceWork, ReferenceWorkRepository, TaxonTreatment, TaxonTreatmentRepository,
)
from database.reference_sync_state import load_library_payload
from references.reference_comparison import source_series, MetricDomain
from references.reference_display import display_from_row
from references.reference_plotting import translate_observation_reference_use
from ui.add_reference_dialog import AddReferenceDialog, filter_library_candidates
from ui.reference_entry_editor import ReferenceEntryEditor
from ui.reference_preview_pane import ReferencePreviewPane
from utils.reference_cloud_sync import sync_reference_library
from tests.test_reference_editor_reported_statistics import libs, qapp, HEBELOMA_TABLE
from tests.test_reference_library_push_executor import RecordingReferenceClient
from tests.test_reference_measurement_content_cloud_transport import (
    _point_at, _remote_rows_of, PullClient, pull_reference_library,
)


@pytest.mark.parametrize("expression,core,extremes", [
    ("6-10 x 2-5 um, Q = 2.00-3.00", (2.0, 3.0), (None, None)),
    ("(5-)6-10(-11) x (1.5-)2-5(-5.5) um, Q = (1.50-)2.00-3.00(-3.50)",
     (2.0, 3.0), (1.5, 3.5)),
    (HEBELOMA_TABLE, (1.42, 1.96), (1.3, 2.07)),
])
def test_enter_save_reopen_reuse_plot_and_sync(qapp, libs, monkeypatch, tmp_path,
                                             expression, core, extremes):
    db_path, _ = libs
    parent = QWidget()
    pane = ReferencePreviewPane(parent)
    editor = ReferenceEntryEditor(parent, "Flammulina", "velutipes",
                                  observation_id=1, sporely_taxon_id=625621, preview_pane=pane)
    editor.measurement_paste_input.setText(expression)
    editor._on_parse_measurement_clicked()
    work = ReferenceWorkRepository.create(ReferenceWork(id="", type="article", title="Original source",
                                                        short_label="Original source"))
    treatment = TaxonTreatmentRepository.create(TaxonTreatment(
        id="", reference_work_id=work.id, name_as_published="Flammulina velutipes", taxon_id=None))
    payload = editor.normalized_measurement_set_payload()
    stored = MeasurementSetRepository.create(replace(payload, taxon_treatment_id=treatment.id))
    assert (stored.q_core_min, stored.q_core_max) == core
    assert (stored.q_min, stored.q_max) == extremes
    with sqlite3.connect(db_path) as connection:
        ids = [connection.execute("INSERT INTO observations (date, genus, species) VALUES "
                                  "('2026-10-26','Flammulina','velutipes')").lastrowid for _ in range(2)]
    uses = [ObservationReferenceUseRepository.attach(observation_id=obs,
                reference_measurement_set_id=stored.id, role="compared") for obs in ids]
    assert len(MeasurementSetRepository.list_for_treatment(treatment.id)) == 1
    reopened = MeasurementSetRepository.get(stored.id)
    assert reopened == stored
    candidates = MeasurementSetRepository.list_attachment_candidates()
    assert [c.measurement_set_id for c in filter_library_candidates(candidates,
        taxon_id="625621", taxon_text="Flammulina velutipes", only_this_taxon=True)] == [stored.id]
    picker = AddReferenceDialog(parent, genus="Flammulina", species="velutipes",
                               taxon_id=625621, candidates=candidates, my_observations=[], community_results=[], ai_candidates=[])
    assert [c.measurement_set_id for c in picker._filtered_candidates()] == [stored.id]
    for use in uses:
        snapshot = json.loads(use.snapshot_json)
        assert snapshot["schema_version"] == 2
        plotted = translate_observation_reference_use(use)
        assert (plotted["data"]["q_p05"], plotted["data"]["q_p95"]) == core
        assert (plotted["data"]["q_min"], plotted["data"]["q_max"]) == extremes
        assert json.loads(plotted["data"]["measurement_details_json"]) == snapshot["measurement_details"]
        q = source_series(display_from_row(plotted["data"]), "q", domain=MetricDomain(metric="q", low=1.0, high=4.0))
        assert (q.core.low, q.core.high) == core
        if expression == HEBELOMA_TABLE:
            assert q.centre("mean").interval == (1.55, 1.78)
            assert q.centre("median").interval == (1.54, 1.79)
            assert stored.q_mean is None
    pushed = load_library_payload("measurement_set", stored.id)
    client = RecordingReferenceClient()
    result = sync_reference_library(client)
    assert not result.errors and not result.conflicts
    assert client.remote_rows[("measurement_set", stored.id)]["q_core_min"] == core[0]
    _point_at(monkeypatch, tmp_path / "pulled")
    puller = PullClient(works=_remote_rows_of(client, "work"),
                        treatments=_remote_rows_of(client, "treatment"),
                        sets=_remote_rows_of(client, "measurement_set"))
    result = pull_reference_library(puller)
    assert not result.errors and not result.conflicts
    assert load_library_payload("measurement_set", stored.id) == pushed
    picker.close()
    editor.close()
    parent.close()


def test_manual_q_typical_pair_is_stored_in_its_own_columns(qapp, libs):
    parent = QWidget()
    editor = ReferenceEntryEditor(parent, "Flammulina", "velutipes", observation_id=1)
    for row, low, high in ((0, "6", "10"), (1, "2", "5"), (2, "2.00", "3.00")):
        editor.set_measurement_cell_text(row, 1, low)
        editor.set_measurement_cell_text(row, 3, high)
    payload = editor.normalized_measurement_set_payload()
    assert (payload.q_core_min, payload.q_core_max) == (2.0, 3.0)
    assert payload.q_min is None and payload.q_max is None and payload.q_mean is None
    editor.close()
    parent.close()
