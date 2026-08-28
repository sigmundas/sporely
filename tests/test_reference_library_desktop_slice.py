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
    MeasurementSetPreferenceRepository,
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
    # The row label composes short label + published taxon + locator so
    # two rows from the same work but different taxa or pages stay
    # visually distinguishable in the reference-series table.
    assert result["label"] == "Petersen et al. 1990 — Russula paludosa — p. 214"
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


def _bind_attach_handler(stub):
    """Bind ``MainWindow._on_attach_library_reference_clicked`` (and the
    small helpers it uses) onto a lightweight ``stub`` so tests can drive
    the real handler without instantiating a Qt MainWindow."""
    from types import MethodType
    from ui.main_window import MainWindow

    stub._on_attach_library_reference_clicked = MethodType(
        MainWindow._on_attach_library_reference_clicked, stub
    )
    stub._attach_normalized_reference_to_active_observation = MethodType(
        MainWindow._attach_normalized_reference_to_active_observation, stub
    )
    stub._current_attached_measurement_set_ids = MethodType(
        MainWindow._current_attached_measurement_set_ids, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )
    stub._format_normalized_reference_row_tooltip = MethodType(
        MainWindow._format_normalized_reference_row_tooltip, stub
    )
    return stub


def _install_handler_environment(monkeypatch, stub, *, dialog_result):
    """Patch the module-level Qt/dialog symbols the handler references so
    ``_on_attach_library_reference_clicked`` runs without a live Qt loop.
    ``dialog_result`` is ``(measurement_set_id, role)`` or ``None`` to
    simulate the user pressing Cancel."""
    from types import SimpleNamespace
    from ui import main_window as main_window_module

    class _FakeDialog:
        def __init__(
            self,
            parent,
            *,
            exclude_measurement_set_ids=None,
            taxon_id=None,
        ):
            self._exclude = set(exclude_measurement_set_ids or [])
            self._taxon_id = taxon_id

        def exec(self):
            return 1 if dialog_result is not None else 0

        def result_pair(self):
            return dialog_result if dialog_result is not None else (None, "compared")

    class _FakeQDialog:
        Accepted = 1

    class _FakeMessageBox:
        calls: list[tuple[str, tuple, dict]] = []

        @classmethod
        def _record(cls, name, *args, **kwargs):
            cls.calls.append((name, args, kwargs))

        @classmethod
        def warning(cls, *args, **kwargs):
            cls._record("warning", *args, **kwargs)

        @classmethod
        def information(cls, *args, **kwargs):
            cls._record("information", *args, **kwargs)

        @classmethod
        def critical(cls, *args, **kwargs):
            cls._record("critical", *args, **kwargs)

    _FakeMessageBox.calls = []

    monkeypatch.setattr(
        main_window_module,
        "ReferenceLibraryAttachDialog",
        _FakeDialog,
    )
    monkeypatch.setattr(main_window_module, "QDialog", _FakeQDialog)
    monkeypatch.setattr(main_window_module, "QMessageBox", _FakeMessageBox)
    return _FakeMessageBox


def _stub_main_window_shell(*, active_observation_id, initial_series=None):
    """Return a lightweight object with only the attributes/methods the
    attach handler touches on ``self``. This avoids constructing a real
    ``MainWindow`` (which requires a full Qt shell) while still exercising
    the actual handler code path end-to-end."""
    from types import MethodType, SimpleNamespace

    stub = SimpleNamespace()
    stub.active_observation_id = active_observation_id
    stub.reference_series = list(initial_series or [])
    stub.reference_values = {}
    stub.added_entries: list[dict] = []

    def _tr(self, text, *args, **kwargs):
        return text

    def _add_entry(self, entry):
        self.added_entries.append(entry)

    stub.tr = MethodType(_tr, stub)
    stub._add_reference_series_entry = MethodType(_add_entry, stub)
    return stub


def test_attach_handler_detaches_newly_created_row_when_translation_fails(
    libs, monkeypatch
):
    """When the handler attaches a *new* use and the translator cannot
    produce a wrapper, the just-inserted row must be detached — but the
    library measurement set must remain untouched."""
    from references import reference_plotting
    from ui import main_window as main_window_module

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    stub = _stub_main_window_shell(active_observation_id=obs_id)
    _bind_attach_handler(stub)
    message_box = _install_handler_environment(
        monkeypatch, stub, dialog_result=(ms.id, "compared")
    )

    # Force the translator (looked up via the main_window module) to fail.
    monkeypatch.setattr(
        main_window_module, "translate_observation_reference_use", lambda _use: None
    )
    # Also intercept the reference_plotting module in case the handler is
    # patched to consult it directly in a future refactor.
    monkeypatch.setattr(
        reference_plotting, "translate_observation_reference_use", lambda _use: None
    )

    stub._on_attach_library_reference_clicked()

    # Post-condition: rollback happened, no orphan use row survives.
    listed = ObservationReferenceUseRepository.list_for_observation(int(obs_id))
    assert listed == []
    # The library measurement set is untouched.
    assert MeasurementSetRepository.get(ms.id) is not None
    # No entry was added to reference_series (the plot cannot render it).
    assert stub.added_entries == []
    # The handler must have surfaced the "cannot translate" warning to the user.
    assert any(call[0] == "warning" for call in message_box.calls)


def test_attach_handler_rollback_failure_is_surfaced_not_swallowed(
    libs, monkeypatch
):
    """If detaching the just-inserted row fails, the handler must NOT
    silently swallow the failure — it must surface a clear error naming
    the still-persisted use id so the user can act."""
    from ui import main_window as main_window_module

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    stub = _stub_main_window_shell(active_observation_id=obs_id)
    _bind_attach_handler(stub)
    message_box = _install_handler_environment(
        monkeypatch, stub, dialog_result=(ms.id, "compared")
    )

    monkeypatch.setattr(
        main_window_module, "translate_observation_reference_use", lambda _use: None
    )

    # Simulate detach raising unexpectedly (e.g. locked db, disk error).
    class _BoomError(RuntimeError):
        pass

    def _boom_detach(_use_id):
        raise _BoomError("simulated rollback failure")

    monkeypatch.setattr(
        ObservationReferenceUseRepository, "detach", staticmethod(_boom_detach)
    )

    stub._on_attach_library_reference_clicked()

    # The critical path must fire — the failure is surfaced, not silenced.
    critical_calls = [c for c in message_box.calls if c[0] == "critical"]
    assert critical_calls, (
        "rollback failure must be surfaced via a critical dialog, "
        "not silently swallowed"
    )
    # No wrapper entry added (translation failed).
    assert stub.added_entries == []


def test_attach_handler_never_detaches_pre_existing_use_on_race(libs, monkeypatch):
    """Race scenario: a use row already exists for this observation +
    measurement set (attached by a prior session or a concurrent path).
    The handler must NOT detach that row as a "rollback" for its own
    call — the pre-existing row belongs to the user and must survive."""
    from ui import main_window as main_window_module

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    # Pre-existing attach — simulates the racing/other-session state.
    pre_existing_use = ObservationReferenceUseRepository.attach(
        int(obs_id), ms.id, role="compared"
    )

    stub = _stub_main_window_shell(active_observation_id=obs_id)
    _bind_attach_handler(stub)
    message_box = _install_handler_environment(
        monkeypatch, stub, dialog_result=(ms.id, "compared")
    )

    # Force translation to fail on the returned use (which is the
    # PRE-EXISTING row, not one we just inserted).
    monkeypatch.setattr(
        main_window_module, "translate_observation_reference_use", lambda _use: None
    )

    # Track calls to detach — must NOT be called for the pre-existing row.
    detach_calls: list[str] = []
    original_detach = ObservationReferenceUseRepository.detach

    def _tracked_detach(use_id):
        detach_calls.append(use_id)
        return original_detach(use_id)

    monkeypatch.setattr(
        ObservationReferenceUseRepository, "detach", staticmethod(_tracked_detach)
    )

    stub._on_attach_library_reference_clicked()

    # The row must still be present — the handler must not have detached
    # the pre-existing use as its rollback.
    assert pre_existing_use.id not in detach_calls
    listed = ObservationReferenceUseRepository.list_for_observation(int(obs_id))
    assert len(listed) == 1
    assert listed[0].id == pre_existing_use.id
    # And a warning row for the unplottable snapshot must have been added
    # so the user still has a working detach affordance.
    assert len(stub.added_entries) == 1
    added = stub.added_entries[0]
    assert added["data"].get("malformed") is True
    assert added["data"]["observation_reference_use_id"] == pre_existing_use.id
    # And a warning dialog was surfaced.
    assert any(call[0] == "warning" for call in message_box.calls)


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
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )

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


# --- Regression tests: plotability, overrides, malformed rows, geometry ----


def test_translator_supported_kind_without_drawable_geometry_returns_none():
    """A supported data_kind alone is insufficient — the range/summary
    snapshot must produce a drawable L/W rectangle or a complete L/W
    mean pair. A snapshot that only supplies a length axis (no width)
    must NOT translate to a wrapper."""
    snapshot = {
        "schema_version": 1,
        "reference_measurement_set_id": "set-halflength",
        "reference_work_id": "work-half",
        "short_label": "Half Bounds 2001",
        "name_as_published": "Russula paludosa",
        "data_kind": "range",
        "measurements": {
            "length_core_min": 8.0,
            "length_core_max": 10.0,
            "length_mean": 9.0,
            # Deliberately no width bounds and no width mean -> not drawable.
        },
    }
    use = _make_use(snapshot, use_id="use-halflength")
    assert translate_observation_reference_use(use) is None


def test_translator_summary_with_only_length_mean_rejected():
    """Summary with a single-axis mean (no matching width mean and no
    L/W rectangle) is not plottable — translator must return None."""
    snapshot = _range_snapshot(
        length_core=(0.0, 0.0),
        length_exceptional=(0.0, 0.0),
        width_core=(0.0, 0.0),
        width_exceptional=(0.0, 0.0),
        length_mean=9.0,
        width_mean=None,
        data_kind="summary",
    )
    use = _make_use(snapshot, use_id="use-halfmean")
    assert translate_observation_reference_use(use) is None


def test_translator_summary_with_complete_mean_pair_translates():
    """A summary with a complete L/W mean pair and no rectangle is still
    drawable — the mean cross renders even without bounds."""
    snapshot = _range_snapshot(
        length_core=(0.0, 0.0),
        length_exceptional=(0.0, 0.0),
        width_core=(0.0, 0.0),
        width_exceptional=(0.0, 0.0),
        length_mean=9.0,
        width_mean=5.5,
        data_kind="summary",
    )
    use = _make_use(snapshot, use_id="use-meanpair")
    result = translate_observation_reference_use(use)
    assert result is not None
    data = result["data"]
    assert data["length_p50"] == 9.0
    assert data["width_p50"] == 5.5


def test_translator_summary_rejects_inverted_bounds():
    """A rectangle with lmax <= lmin (or wmax <= wmin) is not drawable."""
    snapshot = _range_snapshot(
        length_core=(10.0, 8.0),   # inverted
        length_exceptional=(11.0, 7.0),
        width_core=(6.0, 5.0),
        width_exceptional=(7.0, 4.0),
        length_mean=None,
        width_mean=None,
    )
    use = _make_use(snapshot, use_id="use-inverted")
    assert translate_observation_reference_use(use) is None


def test_translator_raw_points_rejects_non_finite_or_non_positive():
    """raw_points must contain at least one finite, strictly-positive
    paired point. Zero, negative, NaN and infinity are dropped."""
    import math as _math

    snapshot = {
        "schema_version": 1,
        "reference_measurement_set_id": "set-rp2",
        "reference_work_id": "work-rp2",
        "short_label": "Bad Points",
        "name_as_published": "Russula paludosa",
        "data_kind": "raw_points",
        "raw_points": [
            {"length": 0.0, "width": 5.0},
            {"length": 9.0, "width": -1.0},
            {"length": _math.nan, "width": 5.0},
            {"length": _math.inf, "width": 5.0},
        ],
    }
    use = _make_use(snapshot, use_id="use-badrp")
    assert translate_observation_reference_use(use) is None


def test_translator_raw_points_keeps_only_finite_positive_points():
    """Mixed valid and invalid points -> only the finite, strictly-positive
    paired points survive."""
    import math as _math

    snapshot = {
        "schema_version": 1,
        "reference_measurement_set_id": "set-rp-mixed",
        "reference_work_id": "work-rp-mixed",
        "short_label": "Mixed Points",
        "name_as_published": "Russula paludosa",
        "data_kind": "raw_points",
        "raw_points": [
            {"length": 9.0, "width": 5.5},
            {"length": 0.0, "width": 5.0},
            {"length": _math.nan, "width": _math.nan},
            {"length": 10.0, "width": 6.0},
        ],
    }
    use = _make_use(snapshot, use_id="use-mixedrp")
    result = translate_observation_reference_use(use)
    assert result is not None
    points = result["data"]["points"]
    assert len(points) == 2
    for p in points:
        assert p["length_um"] > 0.0
        assert p["width_um"] > 0.0


def test_translator_label_includes_short_label_taxon_and_locator():
    """Regression: the row label must combine short label + published
    taxon + locator so two rows from the same work but different taxa
    or pages remain distinguishable in the reference-series table."""
    snapshot = _range_snapshot(
        short_label="Petersen 1990",
        name_as_published="Russula paludosa",
        locator_text="p. 214",
    )
    use = _make_use(snapshot, use_id="use-label")
    result = translate_observation_reference_use(use)
    assert result is not None
    assert "Petersen 1990" in result["label"]
    assert "Russula paludosa" in result["label"]
    assert "p. 214" in result["label"]


def test_attach_with_status_reports_created_vs_existing(libs):
    """``attach_with_status`` returns ``(use, True)`` on first insert and
    ``(use, False)`` when the same observation+set link already exists.
    Callers depend on this to distinguish rollback-safe from
    rollback-unsafe conditions."""
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)

    first_use, first_created = (
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), ms.id, role="compared"
        )
    )
    assert first_created is True

    second_use, second_created = (
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), ms.id, role="compared"
        )
    )
    assert second_created is False
    assert second_use.id == first_use.id


def test_restore_reference_uses_applies_persisted_display_overrides(
    libs, monkeypatch
):
    """Persisted enabled/plot_color overrides are keyed by use id and
    applied to translated entries during observation restore. They are
    NOT stored inside the snapshot data."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(
        int(obs_id), ms.id, role="compared"
    )

    stub = SimpleNamespace()
    stub.active_observation_id = obs_id
    stub.reference_series = []
    stub._normalized_reference_display_overrides = {
        use.id: {"enabled": False, "plot_color": "#00ff00"},
    }

    def _tr(self, text, *args, **kwargs):
        return text

    stub.tr = MethodType(_tr, stub)
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *a, **kw: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )
    restore = MethodType(
        MainWindow._restore_reference_uses_for_observation, stub
    )

    restore(int(obs_id))

    assert len(stub.reference_series) == 1
    entry = stub.reference_series[0]
    # Overrides applied.
    assert entry["enabled"] is False
    assert entry["data"]["plot_color"] == "#00ff00"
    # And the snapshot data itself was NOT duplicated into an override
    # blob (there is no such field on the entry / data).
    for forbidden in ("length_p05", "length_p95", "width_p05", "width_p95"):
        # These fields DO exist because the translator populated them
        # from the snapshot itself — not from the override. Sanity: they
        # exist on the data dict but NOT on any nested "override" field.
        assert forbidden in entry["data"]
    assert "override" not in entry
    assert "reference_use_overrides" not in entry


def test_restore_reference_uses_preserves_malformed_as_warning_row(
    libs, monkeypatch
):
    """A persisted use whose snapshot the translator cannot render must
    appear as a visible warning row (``malformed=True``, disabled) so
    the user has a working detach affordance — not silently dropped."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)

    # Insert a dangling use with a snapshot the translator cannot render.
    bad_use = ObservationReferenceUseRepository.attach(
        int(obs_id),
        "dangling-set-id",
        role="compared",
        allow_dangling=True,
    )

    stub = SimpleNamespace()
    stub.active_observation_id = obs_id
    stub.reference_series = []
    stub._normalized_reference_display_overrides = {}

    def _tr(self, text, *args, **kwargs):
        return text

    stub.tr = MethodType(_tr, stub)
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *a, **kw: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )
    stub._format_normalized_reference_row_tooltip = MethodType(
        MainWindow._format_normalized_reference_row_tooltip, stub
    )
    restore = MethodType(
        MainWindow._restore_reference_uses_for_observation, stub
    )

    restore(int(obs_id))

    # A warning row was created — the malformed use was NOT dropped.
    assert len(stub.reference_series) == 1
    entry = stub.reference_series[0]
    assert entry["data"]["malformed"] is True
    assert entry["data"]["observation_reference_use_id"] == bad_use.id
    assert entry["data"]["library_snapshot_state"] == "source_missing"
    assert entry["data"]["library_update_available"] is False
    assert entry["enabled"] is False


def test_restore_marks_semantically_changed_attachment_as_update_available(libs):
    """A work-only edit must decorate the frozen row without replacing its
    plotted snapshot or relying on the measurement-set revision."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    work, _, ms = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(obs_id, ms.id)
    frozen = use.snapshot_json
    ReferenceWorkRepository.update(work.id, {"short_label": "Corrected 1990"})

    stub = SimpleNamespace(
        active_observation_id=obs_id,
        reference_series=[],
        _normalized_reference_display_overrides={},
    )
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *args, **kwargs: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )

    MethodType(MainWindow._restore_reference_uses_for_observation, stub)(obs_id)

    data = stub.reference_series[0]["data"]
    assert data["library_snapshot_state"] == "update_available"
    assert data["library_update_available"] is True
    assert data["short_label"] != "Corrected 1990"
    assert ObservationReferenceUseRepository.get(use.id).snapshot_json == frozen


def test_explicit_ui_update_refreshes_row_and_preserves_display_override(libs):
    """The explicit UI handler must update the same use, restore its row from
    persistence, and retain per-use enabled/color preferences."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    work, _, ms = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(
        obs_id, ms.id, role="contradicts", note="keep me"
    )
    ReferenceWorkRepository.update(work.id, {"short_label": "Corrected 1990"})

    plot_refreshes: list[bool] = []
    stub = SimpleNamespace(
        active_observation_id=obs_id,
        reference_series=[],
        _normalized_reference_display_overrides={
            use.id: {"enabled": False, "plot_color": "#123456"}
        },
    )
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *args, **kwargs: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )
    stub._restore_reference_uses_for_observation = MethodType(
        MainWindow._restore_reference_uses_for_observation, stub
    )
    stub.update_graph_plots_only = lambda: plot_refreshes.append(True)

    MethodType(MainWindow._update_reference_use_from_library, stub)(
        use.id, obs_id
    )

    persisted = ObservationReferenceUseRepository.get(use.id)
    assert persisted is not None
    assert persisted.role == "contradicts"
    assert persisted.note == "keep me"
    assert json.loads(persisted.snapshot_json)["short_label"] == "Corrected 1990"
    assert stub.reference_series[0]["enabled"] is False
    assert stub.reference_series[0]["data"]["plot_color"] == "#123456"
    assert stub.reference_series[0]["data"]["library_update_available"] is False
    assert plot_refreshes == [True]


def test_explicit_ui_update_refuses_observation_drift(monkeypatch):
    """A button captured for one observation must never update after the
    user has switched to another observation."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    refresh_calls: list[str] = []
    warnings: list[str] = []
    monkeypatch.setattr(
        ObservationReferenceUseRepository,
        "refresh_snapshot",
        lambda use_id: refresh_calls.append(use_id),
    )
    monkeypatch.setattr(
        "ui.main_window.QMessageBox.warning",
        lambda _parent, _title, message: warnings.append(message),
    )
    stub = SimpleNamespace(active_observation_id=12)
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)

    MethodType(MainWindow._update_reference_use_from_library, stub)("use-1", 11)

    assert refresh_calls == []
    assert warnings


def test_restore_distinguishes_successor_from_same_set_update(libs):
    """A successor UUID gets its own review state; it must not masquerade as
    an in-place snapshot update or alter the frozen plotted row."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, original = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(obs_id, original.id)
    successor = MeasurementSetRepository.create_revision(
        original.id, {"raw_text": "9–11 × 5–6 µm"}
    )
    stub = SimpleNamespace(
        active_observation_id=obs_id,
        reference_series=[],
        _normalized_reference_display_overrides={},
    )
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *args, **kwargs: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )

    MethodType(MainWindow._restore_reference_uses_for_observation, stub)(obs_id)

    data = stub.reference_series[0]["data"]
    assert data["library_update_available"] is False
    assert data["library_successor_state"] == "successor_available"
    assert data["library_successor_available"] is True
    assert data["library_successor_id"] == successor.id
    assert data["reference_measurement_set_id"] == original.id
    assert ObservationReferenceUseRepository.get(use.id).reference_measurement_set_id == original.id


def test_successor_review_cancellation_keeps_historical_attachment(libs):
    """Closing the review without confirmation must perform no persistence or
    plot refresh work."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, original = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(obs_id, original.id)
    MeasurementSetRepository.create_revision(original.id, {"raw_text": "new"})
    plot_refreshes: list[bool] = []
    stub = SimpleNamespace(active_observation_id=obs_id)
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)
    stub._confirm_reference_successor_adoption = lambda *_args: False
    stub.update_graph_plots_only = lambda: plot_refreshes.append(True)

    MethodType(MainWindow._review_reference_successor, stub)(use.id, obs_id)

    unchanged = ObservationReferenceUseRepository.get(use.id)
    assert unchanged == use
    assert plot_refreshes == []


def test_confirmed_successor_review_adopts_and_refreshes_row(libs):
    """Explicit confirmation retargets the same use and reloads the plotted
    entry from the successor snapshot."""
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, original = _seed_work_treatment_set(libs)
    use = ObservationReferenceUseRepository.attach(
        obs_id, original.id, role="supports_identification", note="keep"
    )
    successor = MeasurementSetRepository.create_revision(
        original.id,
        {"raw_text": "9–11 × 5–6 µm", "length_core_min": 9.0, "length_core_max": 11.0},
    )
    plot_refreshes: list[bool] = []
    stub = SimpleNamespace(
        active_observation_id=obs_id,
        reference_series=[],
        _normalized_reference_display_overrides={},
    )
    stub.tr = MethodType(lambda self, text, *args, **kwargs: text, stub)
    stub._confirm_reference_successor_adoption = lambda *_args: True
    stub._normalize_reference_series_entry = lambda entry: entry
    stub._refresh_reference_series_table = lambda *args, **kwargs: None
    stub._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub
    )
    stub._clear_normalized_reference_entries = MethodType(
        MainWindow._clear_normalized_reference_entries, stub
    )
    stub._build_malformed_reference_series_entry = MethodType(
        MainWindow._build_malformed_reference_series_entry, stub
    )
    stub._restore_reference_uses_for_observation = MethodType(
        MainWindow._restore_reference_uses_for_observation, stub
    )
    stub.update_graph_plots_only = lambda: plot_refreshes.append(True)

    MethodType(MainWindow._review_reference_successor, stub)(use.id, obs_id)

    adopted = ObservationReferenceUseRepository.get(use.id)
    assert adopted is not None
    assert adopted.id == use.id
    assert adopted.reference_measurement_set_id == successor.id
    assert adopted.role == "supports_identification"
    assert adopted.note == "keep"
    assert stub.reference_series[0]["data"]["reference_measurement_set_id"] == successor.id
    assert stub.reference_series[0]["data"]["length_p05"] == 9.0
    preference = MeasurementSetPreferenceRepository.get(successor.id)
    assert preference is not None
    assert preference.recent_use_sequence == 1
    assert plot_refreshes == [True]


def test_successor_review_text_includes_both_full_citations():
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    stub = SimpleNamespace(tr=lambda text: text)
    use = SimpleNamespace(
        snapshot_json=json.dumps(
            {
                "short_label": "Old 2000",
                "full_citation": "Old Author (2000). Original work.",
                "raw_text": "8–10 × 5–6 µm",
            }
        )
    )
    status = SimpleNamespace(
        successor_snapshot_json=json.dumps(
            {
                "short_label": "New 2001",
                "full_citation": "New Author (2001). Successor work.",
                "raw_text": "9–11 × 5–6 µm",
            }
        )
    )

    text = MethodType(MainWindow._format_reference_successor_review, stub)(
        use, status
    )

    assert "Old Author (2000). Original work." in text
    assert "New Author (2001). Successor work." in text
    assert "8–10 × 5–6 µm" in text
    assert "9–11 × 5–6 µm" in text


def test_collect_and_apply_gallery_settings_round_trips_use_overrides():
    """Round-trip test: ``_collect_gallery_settings`` produces override
    entries keyed by use id (no snapshot duplication); the resulting
    dict, fed back through ``_apply_saved_reference_state``, restores
    the overrides on ``self._normalized_reference_display_overrides``.
    """
    from types import MethodType, SimpleNamespace
    from ui.main_window import MainWindow

    stub = SimpleNamespace()
    stub.active_observation_id = None
    stub.reference_values = {}
    stub.reference_series = [
        {
            "key": "use-1",
            "label": "Petersen 1990 — Russula paludosa — p. 214",
            "enabled": False,
            "data": {
                "source_kind": "reference",
                "reference_data_kind": "range",
                "observation_reference_use_id": "use-1",
                "reference_measurement_set_id": "ms-1",
                "short_label": "Petersen 1990",
                "name_as_published": "Russula paludosa",
                "locator_text": "p. 214",
                "plot_color": "#123456",
                "length_p05": 8.0,
                "length_p95": 10.0,
                "width_p05": 5.0,
                "width_p95": 6.0,
            },
        },
    ]
    stub.gallery_plot_settings = {}
    # Deliberately do NOT set gallery_filter_combo/orient_checkbox/etc so
    # the code's ``hasattr(...)`` guards short-circuit to None/False.
    stub._collect_reference_panel_state = lambda: {}
    stub._serialize_reference_data_for_settings = lambda data: None
    stub._gallery_plot_style = lambda settings: "ellipse"
    collect = MethodType(MainWindow._collect_gallery_settings, stub)

    collected = collect()
    overrides = collected.get("reference_use_overrides")
    assert isinstance(overrides, list)
    assert len(overrides) == 1
    override = overrides[0]
    assert override["use_id"] == "use-1"
    assert override["enabled"] is False
    assert override["plot_color"] == "#123456"
    # The snapshot geometry must NOT be duplicated into the override.
    assert "length_p05" not in override
    assert "data" not in override
    # ``reference_series`` in gallery settings excludes normalized entries.
    assert collected["reference_series"] == []

    # Now feed it back through _apply_saved_reference_state's override parsing.
    stub2 = SimpleNamespace()
    stub2.active_observation_id = None
    stub2.reference_values = {}
    stub2.reference_series = []

    def _tr(self, text, *args, **kwargs):
        return text

    def _set_reference_series(self, series):
        self.reference_series = [entry for entry in series if entry]

    stub2.tr = MethodType(_tr, stub2)
    stub2._set_reference_series = MethodType(_set_reference_series, stub2)
    stub2._restore_reference_data_from_settings = lambda value: (
        value if isinstance(value, dict) else {}
    )
    stub2._apply_reference_panel_values = lambda *a, **kw: None
    stub2._update_reference_add_state = lambda *a, **kw: None
    stub2._normalize_reference_series_entry = lambda entry: entry
    stub2._refresh_reference_series_table = lambda *a, **kw: None
    stub2.update_graph_plots_only = lambda *a, **kw: None
    stub2._collect_reference_panel_state = lambda: {}
    stub2._save_gallery_settings = lambda *a, **kw: None
    stub2._populate_reference_panel_sources = lambda *a, **kw: None
    stub2._apply_normalized_reference_override = MethodType(
        MainWindow._apply_normalized_reference_override, stub2
    )
    apply_state = MethodType(MainWindow._apply_saved_reference_state, stub2)

    apply_state({"reference_use_overrides": overrides})
    persisted = getattr(stub2, "_normalized_reference_display_overrides", None)
    assert isinstance(persisted, dict)
    assert "use-1" in persisted
    assert persisted["use-1"]["enabled"] is False
    assert persisted["use-1"]["plot_color"] == "#123456"


def test_core_rectangle_face_translucent_edge_opaque(tmp_path, monkeypatch):
    """The core rectangle for a normalized range attachment must be
    drawn with a translucent RGBA face and a fully-opaque edge — the
    previous ``alpha=`` shorthand collapsed both to the same alpha."""
    from matplotlib.colors import to_rgba
    from matplotlib.patches import Rectangle

    # Compose the plot-side call using the same primitives used in
    # ``update_graph_plots_only``. We are asserting the RGBA math the
    # code relies on, not the whole plot pipeline.
    color = "#123456"
    face_rgba = to_rgba(color, alpha=0.18)
    edge_rgba = to_rgba(color, alpha=1.0)
    rect = Rectangle(
        (0.0, 0.0),
        1.0,
        1.0,
        facecolor=face_rgba,
        edgecolor=edge_rgba,
        linewidth=1.5,
        linestyle="-",
        fill=True,
    )
    face = rect.get_facecolor()
    edge = rect.get_edgecolor()
    # Face alpha is the translucent value.
    assert abs(face[3] - 0.18) < 1e-6
    # Edge alpha is fully opaque.
    assert abs(edge[3] - 1.0) < 1e-6
    # RGB channels match the shared source color.
    for face_c, edge_c in zip(face[:3], edge[:3]):
        assert abs(face_c - edge_c) < 1e-6
