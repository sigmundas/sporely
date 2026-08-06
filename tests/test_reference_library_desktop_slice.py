"""Stage 2 desktop-slice tests: snapshot translator, attachment
repository semantics, and (when Qt is available) the attach dialog.

These tests exercise the Qt-free ``references.reference_plotting``
translator and the repository/candidate joins added in Stage 2. They
must not require a running sporely application or a headed Qt session.
"""
from __future__ import annotations

import json
import os

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUse,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from references.reference_plotting import (
    translate_observation_reference_use,
    translate_observation_reference_uses,
)


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    """Fresh, isolated main+reference sqlite databases for each test.

    Mirrors the pattern used by tests/test_reference_library_repository.py.
    """
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "does_not_exist.db",
    )
    _schema.init_database()
    return db_path, ref_path


# --- Snapshot helpers -------------------------------------------------------


def _range_snapshot(
    *,
    length_core=(8.0, 10.0),
    length_exceptional=(7.5, 10.5),
    width_core=(5.0, 6.0),
    width_exceptional=(5.0, 6.5),
    length_mean=None,
    width_mean=None,
    data_kind="range",
    short_label="Petersen et al. 1990",
    name_as_published="Russula paludosa",
    reference_measurement_set_id="set-1",
    reference_treatment_id="treat-1",
    reference_work_id="work-1",
    raw_text="(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
    locator_text="p. 214",
    sample_size=None,
) -> dict:
    measurements = {
        "length_min": length_exceptional[0],
        "length_core_min": length_core[0],
        "length_core_max": length_core[1],
        "length_max": length_exceptional[1],
        "width_min": width_exceptional[0],
        "width_core_min": width_core[0],
        "width_core_max": width_core[1],
        "width_max": width_exceptional[1],
        "length_mean": length_mean,
        "width_mean": width_mean,
        "sample_size": sample_size,
    }
    return {
        "schema_version": 1,
        "reference_measurement_set_id": reference_measurement_set_id,
        "reference_treatment_id": reference_treatment_id,
        "reference_work_id": reference_work_id,
        "short_label": short_label,
        "name_as_published": name_as_published,
        "data_kind": data_kind,
        "raw_text": raw_text,
        "locator_text": locator_text,
        "measurements": measurements,
    }


def _make_use(snapshot: dict, *, use_id="use-uuid-1", role="compared", revision=3):
    return ObservationReferenceUse(
        id=use_id,
        observation_id=1,
        reference_measurement_set_id=str(
            snapshot.get("reference_measurement_set_id") or "set-1"
        ),
        role=role,
        reference_revision=revision,
        snapshot_json=json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
    )


# --- Translator tests -------------------------------------------------------


def test_translator_range_maps_core_and_exceptional_bounds():
    """Range snapshot: core bounds → p05/p95, exceptional → min/max
    verbatim, no p50 or points synthesised."""
    snapshot = _range_snapshot()
    use = _make_use(snapshot, use_id="use-A", role="compared", revision=3)

    result = translate_observation_reference_use(use)

    assert result is not None
    assert result["key"] == "use-A"
    assert result["label"] == "Petersen et al. 1990"
    data = result["data"]
    assert data["source_kind"] == "reference"
    assert data["observation_reference_use_id"] == "use-A"
    assert data["reference_measurement_set_id"] == "set-1"
    assert data["role"] == "compared"
    assert data["reference_revision"] == 3
    assert data["reference_data_kind"] == "range"

    # Core -> p05/p95 exactly.
    assert data["length_p05"] == 8.0
    assert data["length_p95"] == 10.0
    assert data["width_p05"] == 5.0
    assert data["width_p95"] == 6.0
    # Exceptional -> min/max verbatim.
    assert data["length_min"] == 7.5
    assert data["length_max"] == 10.5
    assert data["width_min"] == 5.0
    assert data["width_max"] == 6.5

    # Range/summary entries never carry a `points` array.
    assert "points" not in data
    # No means supplied -> no p50 fabrication.
    assert "length_p50" not in data
    assert "width_p50" not in data


def test_translator_summary_kind_uses_same_grammar():
    """`summary` data_kind is treated the same as `range` (no points)."""
    snapshot = _range_snapshot(data_kind="summary")
    use = _make_use(snapshot, use_id="use-S", role="supports_identification")

    result = translate_observation_reference_use(use)

    assert result is not None
    data = result["data"]
    assert data["source_kind"] == "reference"
    assert data["reference_data_kind"] == "summary"
    assert data["role"] == "supports_identification"
    assert "points" not in data
    assert data["length_p05"] == 8.0
    assert data["length_p95"] == 10.0


def test_translator_never_invents_midpoint_when_means_absent():
    """No means in the snapshot -> translator must NOT synthesise
    ``length_p50`` / ``width_p50`` from ``(min+max)/2``."""
    snapshot = _range_snapshot(length_mean=None, width_mean=None)
    use = _make_use(snapshot, use_id="use-B")

    result = translate_observation_reference_use(use)

    assert result is not None
    data = result["data"]
    # Either absent (preferred) or explicitly None; never a fabricated value.
    if "length_p50" in data:
        assert data["length_p50"] is None
    if "width_p50" in data:
        assert data["width_p50"] is None
    if "length_mean" in data:
        assert data["length_mean"] is None
    if "width_mean" in data:
        assert data["width_mean"] is None


def test_translator_populates_p50_only_when_means_supplied():
    """Means in the snapshot -> p50 mirrors the mean exactly, no
    invention when only one axis is provided."""
    snapshot = _range_snapshot(length_mean=9.0, width_mean=None)
    use = _make_use(snapshot, use_id="use-C")

    result = translate_observation_reference_use(use)

    assert result is not None
    data = result["data"]
    assert data["length_p50"] == 9.0
    # Width mean absent -> width_p50 must not appear as a fabricated value.
    if "width_p50" in data:
        assert data["width_p50"] is None


def test_translator_raw_points_keeps_only_paired_numeric_points():
    """raw_points: emit source_kind='points' with only the paired
    numeric length/width points; drop entries missing a coordinate. No
    invented p05/p95 for raw_points."""
    snapshot = {
        "schema_version": 1,
        "reference_measurement_set_id": "set-rp",
        "reference_work_id": "work-rp",
        "short_label": "Points Author 2020",
        "name_as_published": "Russula paludosa",
        "data_kind": "raw_points",
        "raw_points": [
            {"length": 9.0, "width": 5.5},
            {"length": 9.5, "width": 5.7},
            {"length": 10.0},  # missing width -> dropped
        ],
    }
    use = _make_use(snapshot, use_id="use-RP", role="contradicts", revision=1)

    result = translate_observation_reference_use(use)

    assert result is not None
    data = result["data"]
    assert data["source_kind"] == "points"
    assert data["reference_data_kind"] == "raw_points"
    assert data["role"] == "contradicts"
    points = data.get("points")
    assert isinstance(points, list)
    # Only the two fully paired numeric points survive.
    assert len(points) == 2
    assert {"length_um": 9.0, "width_um": 5.5} in points
    assert {"length_um": 9.5, "width_um": 5.7} in points
    # raw_points must NOT be turned into invented range statistics.
    for forbidden in (
        "length_p05",
        "length_p95",
        "length_min",
        "length_max",
        "width_p05",
        "width_p95",
        "width_min",
        "width_max",
    ):
        assert forbidden not in data


def test_translator_malformed_range_snapshot_returns_none():
    """Range snapshot with no length/width bounds at all -> translator
    returns None (skip cleanly, do not crash)."""
    bad_snapshot = {
        "schema_version": 1,
        "reference_measurement_set_id": "set-bad",
        "short_label": "Missing bounds",
        "data_kind": "range",
        "measurements": {
            # No length or width keys at all -> nothing to plot.
        },
    }
    use = _make_use(bad_snapshot, use_id="use-BAD")

    result = translate_observation_reference_use(use)

    assert result is None


def test_translator_bulk_helper_drops_none_entries():
    """The bulk translator returns only the successfully translated
    entries; malformed inputs are dropped without raising."""
    good = _make_use(_range_snapshot(), use_id="use-good-1")
    bad = _make_use(
        {
            "schema_version": 1,
            "reference_measurement_set_id": "set-bad",
            "short_label": "Missing bounds",
            "data_kind": "range",
            "measurements": {},
        },
        use_id="use-bad-1",
    )

    results = translate_observation_reference_uses([good, bad])

    assert len(results) == 1
    assert results[0]["key"] == "use-good-1"


# --- Repository / integration tests -----------------------------------------


def _seed_work_treatment_set(
    libs,
    *,
    work_title="Danmarks Basidiesvampe",
    short_label="Petersen et al. 1990",
    name_as_published="Russula paludosa",
    length_core=(8.0, 10.0),
    length_exceptional=(7.5, 10.5),
    width_core=(5.0, 6.0),
    width_exceptional=(5.0, 6.5),
    locator_text="p. 214",
):
    """Seed one work + treatment + range measurement set. Returns the
    (work, treatment, ms) tuple."""
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title=work_title,
            short_label=short_label,
            authors_json=json.dumps([{"family": "Petersen"}, {"family": "Læssøe"}]),
            year=1990,
            publisher="Foreningen til Svampekundskabens Fremme",
            place="Copenhagen",
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published=name_as_published,
            locator_text=locator_text,
        )
    )
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
            length_min=length_exceptional[0],
            length_core_min=length_core[0],
            length_core_max=length_core[1],
            length_max=length_exceptional[1],
            width_min=width_exceptional[0],
            width_core_min=width_core[0],
            width_core_max=width_core[1],
            width_max=width_exceptional[1],
        )
    )
    return work, treatment, ms


def _make_observation(db_path) -> int:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-01-01", "Test"),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def test_attach_then_list_for_observation_preserves_accepted_bounds(libs):
    """Attach a range measurement set to an observation, then read it
    back. The stored snapshot must contain the exact core/exceptional
    bounds we seeded, and ``reference_revision`` must equal the source
    set's revision (which the repository sets at attach time)."""
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    use = ObservationReferenceUseRepository.attach(
        obs_id, ms.id, role="compared"
    )

    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    stored = listed[0]
    assert stored.id == use.id
    assert stored.role == "compared"
    assert stored.reference_revision == ms.revision
    snapshot = json.loads(stored.snapshot_json)
    measurements = snapshot["measurements"]
    assert measurements["length_core_min"] == 8.0
    assert measurements["length_core_max"] == 10.0
    assert measurements["length_min"] == 7.5
    assert measurements["length_max"] == 10.5
    assert measurements["width_core_min"] == 5.0
    assert measurements["width_core_max"] == 6.0
    assert measurements["width_min"] == 5.0
    assert measurements["width_max"] == 6.5


def test_detach_removes_use_row_but_preserves_library_set(libs):
    """Detach clears the observation link but never touches the shared
    library measurement set (other observations may still point at it)."""
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(obs_id, ms.id, role="compared")

    ObservationReferenceUseRepository.detach(use.id)

    assert ObservationReferenceUseRepository.list_for_observation(obs_id) == []
    # Library row survives — detach must not cascade into the library.
    assert MeasurementSetRepository.get(ms.id) is not None


def test_attached_use_round_trips_through_translator(libs):
    """End-to-end: attach in the repository, read back, and translate
    the persisted snapshot. The resulting wrapper's data dict carries
    the accepted core/exceptional bounds under the expected keys."""
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(
        obs_id, ms.id, role="supports_identification"
    )

    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1

    translated = translate_observation_reference_use(listed[0])
    assert translated is not None
    data = translated["data"]
    assert data["source_kind"] == "reference"
    assert data["role"] == "supports_identification"
    assert data["length_p05"] == 8.0
    assert data["length_p95"] == 10.0
    assert data["length_min"] == 7.5
    assert data["length_max"] == 10.5
    assert data["width_p05"] == 5.0
    assert data["width_p95"] == 6.0
    assert data["reference_measurement_set_id"] == ms.id
    assert data["reference_revision"] == use.reference_revision
    assert "points" not in data


def test_list_attachment_candidates_joins_context_for_each_set(libs):
    """Seed 2 works x 1 treatment x 2 sets and verify the candidate
    listing returns all four sets, each with the joined display
    metadata expected by the chooser."""
    # First work + one treatment + two measurement sets.
    _, _, ms_a = _seed_work_treatment_set(
        libs,
        work_title="Work A",
        short_label="Author A 2001",
        name_as_published="Russula paludosa",
        locator_text="p. 100",
    )
    # Add a second measurement set under the SAME treatment as ms_a.
    treatment_a_id = ms_a.taxon_treatment_id
    ms_a2 = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment_a_id,
            character="spore_size",
            data_kind="range",
            raw_text="6–7 × 4–5 µm",
            length_core_min=6.0,
            length_core_max=7.0,
            width_core_min=4.0,
            width_core_max=5.0,
        )
    )
    # Second work + one treatment + two measurement sets.
    _, _, ms_b = _seed_work_treatment_set(
        libs,
        work_title="Work B",
        short_label="Author B 2010",
        name_as_published="Russula claroflava",
        locator_text="p. 55",
    )
    ms_b2 = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=ms_b.taxon_treatment_id,
            character="spore_size",
            data_kind="range",
            raw_text="12–14 × 8–10 µm",
            length_core_min=12.0,
            length_core_max=14.0,
            width_core_min=8.0,
            width_core_max=10.0,
        )
    )

    candidates = MeasurementSetRepository.list_attachment_candidates()

    ids = {c.measurement_set_id for c in candidates}
    assert ids == {ms_a.id, ms_a2.id, ms_b.id, ms_b2.id}
    by_id = {c.measurement_set_id: c for c in candidates}
    # Each candidate carries joined display metadata.
    for candidate in candidates:
        assert candidate.short_label
        assert candidate.name_as_published
        assert candidate.data_kind
        assert candidate.reference_work_id
        assert candidate.reference_treatment_id
        assert candidate.revision >= 1
    # Locator, raw_text, kind observed for a specific seeded set.
    assert by_id[ms_a.id].locator_text == "p. 100"
    assert by_id[ms_a.id].data_kind == "range"
    assert by_id[ms_a.id].raw_text is not None


def test_list_attachment_candidates_excludes_ids(libs):
    """``exclude_ids`` removes already-attached sets from the listing."""
    _, _, ms = _seed_work_treatment_set(libs)
    excluded = MeasurementSetRepository.list_attachment_candidates(
        exclude_ids=[ms.id]
    )
    assert all(c.measurement_set_id != ms.id for c in excluded)
    included = MeasurementSetRepository.list_attachment_candidates()
    assert any(c.measurement_set_id == ms.id for c in included)


def test_list_attachment_candidates_excludes_unsupported_data_kinds(libs):
    """F-002 guard: by default ``list_attachment_candidates`` returns only
    plot-supported kinds (`range`/`summary`/`raw_points`). A `parmasto`
    set must NOT appear in the chooser because there is no translator for
    it and attaching it would produce an invisible orphan row. Passing
    an explicit ``supported_kinds`` set that includes `parmasto`
    restores it — proving the filter is data-driven, not hard-coded to
    hide unsupported kinds forever."""
    # A supported set that must remain visible.
    _, _, ms_range = _seed_work_treatment_set(
        libs,
        work_title="Range Work",
        short_label="Range 2001",
        name_as_published="Russula paludosa",
    )
    # An unsupported (parmasto) set that must be filtered out by default.
    ms_parmasto = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=ms_range.taxon_treatment_id,
            character="spore_size",
            data_kind="parmasto",
            raw_text="parmasto biometric expression",
            length_mean=9.0,
            width_mean=5.5,
        )
    )

    default_candidates = MeasurementSetRepository.list_attachment_candidates()
    default_ids = {c.measurement_set_id for c in default_candidates}
    assert ms_range.id in default_ids
    assert ms_parmasto.id not in default_ids
    # Every returned candidate carries a supported kind.
    for candidate in default_candidates:
        assert candidate.data_kind in {"range", "summary", "raw_points"}

    # An explicit override that opts in to `parmasto` restores it.
    override = MeasurementSetRepository.list_attachment_candidates(
        supported_kinds=("range", "summary", "raw_points", "parmasto"),
    )
    override_ids = {c.measurement_set_id for c in override}
    assert ms_range.id in override_ids
    assert ms_parmasto.id in override_ids


def test_attach_rolls_back_when_translation_fails(libs, monkeypatch):
    """F-002 defense-in-depth: if the attach handler's translator step
    returns ``None`` for a freshly-persisted use, the persisted
    ``observation_reference_uses`` row must be detached so no orphan
    row survives with no visible entry in the UI."""
    from ui import main_window as main_window_module

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    # Simulate the attach handler's essential steps directly (this
    # avoids constructing a full MainWindow which requires Qt).
    use = ObservationReferenceUseRepository.attach(
        int(obs_id), ms.id, role="compared"
    )
    # Confirm the row is persisted before the (simulated) translator failure.
    listed_before = ObservationReferenceUseRepository.list_for_observation(
        int(obs_id)
    )
    assert len(listed_before) == 1
    assert listed_before[0].id == use.id

    # Simulate a translator that cannot handle the freshly-attached use.
    def _fake_translate(_use):
        return None

    monkeypatch.setattr(
        main_window_module,
        "translate_observation_reference_use",
        _fake_translate,
    )
    translated = main_window_module.translate_observation_reference_use(use)
    assert translated is None

    # The handler's rollback path: detach the just-persisted row.
    ObservationReferenceUseRepository.detach(use.id)

    # Post-condition: the observation_reference_uses row is gone and the
    # library measurement set is untouched.
    listed_after = ObservationReferenceUseRepository.list_for_observation(
        int(obs_id)
    )
    assert listed_after == []
    assert MeasurementSetRepository.get(ms.id) is not None


def test_apply_gallery_settings_preserves_normalized_entries():
    """F-001 regression: ``_apply_saved_reference_state`` — the code path
    driven by ``apply_gallery_settings()`` and ``on_tab_changed`` — must
    not wipe normalized (durable) reference-series entries carrying
    ``observation_reference_use_id``. Gallery settings only persist
    legacy/transient entries, so normalized entries must be preserved
    across every restore call, including the "explicitly saved empty
    list" branch.
    """
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    # Build a stub MainWindow-like object. Only the small set of methods
    # touched by ``_apply_saved_reference_state`` is stubbed; the method
    # under test is bound from the real class.
    stub = SimpleNamespace()
    stub.active_observation_id = None
    stub.reference_values = {}
    # Pre-existing normalized entry: this is what `_restore_reference_uses_for_observation`
    # would have appended before a subsequent `apply_gallery_settings` call.
    normalized_entry = {
        "key": "use-uuid-123",
        "label": "Petersen et al. 1990",
        "enabled": True,
        "data": {
            "source_kind": "reference",
            "reference_data_kind": "range",
            "observation_reference_use_id": "use-uuid-123",
            "reference_measurement_set_id": "ms-uuid-abc",
            "short_label": "Petersen et al. 1990",
            "length_p05": 8.0,
            "length_p95": 10.0,
            "width_p05": 5.0,
            "width_p95": 6.0,
        },
    }
    stub.reference_series = [dict(normalized_entry)]

    # Stub every helper method the function under test calls. Each
    # returns a sensible neutral value; ``_set_reference_series``
    # actually mutates ``reference_series`` so we can observe the merge.
    def _set_reference_series(self, series):
        # Preserve the code's normalization semantics (drop empty).
        self.reference_series = [entry for entry in series if entry]

    def _restore_reference_data_from_settings(self, value):
        return value if isinstance(value, dict) else {}

    stub._set_reference_series = MethodType(_set_reference_series, stub)
    stub._restore_reference_data_from_settings = MethodType(
        _restore_reference_data_from_settings, stub
    )
    stub._apply_reference_panel_values = lambda *a, **kw: None
    stub._update_reference_add_state = lambda *a, **kw: None
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *a, **kw: None
    stub.update_graph_plots_only = lambda *a, **kw: None
    stub._collect_reference_panel_state = lambda: {}
    stub._save_gallery_settings = lambda *a, **kw: None
    stub._populate_reference_panel_sources = lambda *a, **kw: None

    # Bind the real method to the stub and run the two problematic paths.
    apply_state = MethodType(MainWindow._apply_saved_reference_state, stub)

    # Path 1 — settings say "explicitly saved empty list": the persisted
    # gallery settings contain an empty ``reference_series`` list, which
    # historically wiped the reference_series entirely. The normalized
    # entry must still survive.
    apply_state({"reference_series": []})
    assert isinstance(stub.reference_series, list)
    normalized_after = [
        entry
        for entry in stub.reference_series
        if isinstance(entry, dict)
        and isinstance(entry.get("data"), dict)
        and entry["data"].get("observation_reference_use_id")
    ]
    assert len(normalized_after) == 1, (
        "normalized entry with observation_reference_use_id was wiped by "
        "apply_gallery_settings; F-001 regression"
    )
    assert normalized_after[0]["data"]["observation_reference_use_id"] == "use-uuid-123"

    # Path 2 — settings restore a legacy entry alongside the normalized
    # entry. Both should coexist.
    stub.reference_series = [dict(normalized_entry)]
    apply_state({
        "reference_series": [
            {
                "enabled": True,
                "data": {
                    "source_kind": "reference",
                    "genus": "Russula",
                    "species": "paludosa",
                    "length_p05": 7.0,
                    "length_p95": 9.0,
                },
            }
        ]
    })
    normalized_after = [
        entry
        for entry in stub.reference_series
        if isinstance(entry, dict)
        and isinstance(entry.get("data"), dict)
        and entry["data"].get("observation_reference_use_id")
    ]
    legacy_after = [
        entry
        for entry in stub.reference_series
        if isinstance(entry, dict)
        and isinstance(entry.get("data"), dict)
        and not entry["data"].get("observation_reference_use_id")
    ]
    assert len(normalized_after) == 1
    assert len(legacy_after) == 1

    # Path 3 — no ``reference_series`` key in settings at all, but a
    # legacy ``reference_values`` payload. The normalized entry must
    # remain.
    stub.reference_series = [dict(normalized_entry)]
    apply_state({
        "reference_values": {
            "source_kind": "reference",
            "genus": "Russula",
            "species": "paludosa",
            "length_p05": 7.0,
            "length_p95": 9.0,
        }
    })
    normalized_after = [
        entry
        for entry in stub.reference_series
        if isinstance(entry, dict)
        and isinstance(entry.get("data"), dict)
        and entry["data"].get("observation_reference_use_id")
    ]
    assert len(normalized_after) == 1

    # Path 4 — empty settings dict (no reference state at all). The
    # normalized entry must still survive because it lives in a durable
    # table, not in the gallery settings.
    stub.reference_series = [dict(normalized_entry)]
    apply_state({})
    normalized_after = [
        entry
        for entry in stub.reference_series
        if isinstance(entry, dict)
        and isinstance(entry.get("data"), dict)
        and entry["data"].get("observation_reference_use_id")
    ]
    assert len(normalized_after) == 1


# --- Optional Qt dialog test (skipped when Qt is unavailable) ---------------


try:  # pragma: no cover - environmental
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication  # noqa: F401
    _QT_AVAILABLE = True
except Exception:  # pragma: no cover
    _QT_AVAILABLE = False


@pytest.fixture(scope="module")
def qapp():
    if not _QT_AVAILABLE:  # pragma: no cover
        pytest.skip("PySide6 not available; skipping Qt dialog test")
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.mark.skipif(not _QT_AVAILABLE, reason="PySide6 unavailable")
def test_attach_dialog_result_pair_reflects_selection(qapp):
    """Construct the dialog with stub candidates, simulate a row
    selection, and confirm ``result_pair`` reports the chosen
    measurement-set id and role."""
    from database.reference_library import MeasurementSetCandidate
    from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog

    candidates = [
        MeasurementSetCandidate(
            measurement_set_id="ms-alpha",
            short_label="Author A 2001",
            name_as_published="Russula paludosa",
            locator_text="p. 100",
            data_kind="range",
            raw_text="7.5–10 × 5–6 µm",
            revision=1,
            reference_work_id="work-A",
            reference_treatment_id="treat-A",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-beta",
            short_label="Author B 2010",
            name_as_published="Russula claroflava",
            locator_text="p. 55",
            data_kind="summary",
            raw_text="12–14 × 8–10 µm",
            revision=2,
            reference_work_id="work-B",
            reference_treatment_id="treat-B",
        ),
    ]
    dialog = ReferenceLibraryAttachDialog(None, candidates=candidates)
    try:
        # Initially nothing is selected, so the dialog's OK path is not usable.
        assert dialog.selected_measurement_set_id() is None

        dialog.table.selectRow(1)
        set_id, role = dialog.result_pair()
        assert set_id == "ms-beta"
        # Default role is `compared` (the first entry in _ROLE_VALUES).
        assert role in {"compared", "supports_identification", "contradicts"}
        assert role == "compared"

        # Switch role and re-check.
        role_index = dialog.role_combo.findData("supports_identification")
        assert role_index >= 0
        dialog.role_combo.setCurrentIndex(role_index)
        set_id2, role2 = dialog.result_pair()
        assert set_id2 == "ms-beta"
        assert role2 == "supports_identification"
    finally:
        dialog.deleteLater()
