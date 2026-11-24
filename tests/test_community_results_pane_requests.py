"""Deterministic worker-race regressions for
``ui.cloud_reference_dialog.CommunityResultsPane`` (stage-4b-fix2 Part 1).

Reproduces the reviewed defect at ui/cloud_reference_dialog.py:1288-1359: a
target change used to skip starting a new search while an old search worker
was still outstanding, and a stale search/detail worker's completion or
error was applied unconditionally -- letting an earlier target's result
attach under a later target. Uses fake, manually-driven workers (no real
network/threads/sleeps) so completion order is fully controlled.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

import ui.cloud_reference_dialog as mod


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeSearchWorker(QObject):
    """Stand-in for ``_CloudSearchWorker``: created synchronously, does
    nothing on ``start()``, and only reports completion when the test
    explicitly emits ``search_done``/``error``."""

    search_done = Signal(list, dict)
    error = Signal(str)
    finished = Signal()

    def __init__(self, genus: str, species: str) -> None:
        super().__init__()
        self.genus = genus
        self.species = species
        self.waited = False

    def start(self) -> None:
        pass

    def wait(self, *_args, **_kwargs) -> bool:
        self.waited = True
        return True

    def deleteLater(self) -> None:  # pragma: no cover - Qt cleanup no-op
        pass


class _FakeDetailWorker(QObject):
    """Stand-in for ``_CloudDetailWorker``, same manual-completion contract."""

    detail_done = Signal(dict)
    error = Signal(str)
    finished = Signal()

    def __init__(self, row: dict) -> None:
        super().__init__()
        self.row = row
        self.waited = False

    def start(self) -> None:
        pass

    def wait(self, *_args, **_kwargs) -> bool:
        self.waited = True
        return True

    def deleteLater(self) -> None:  # pragma: no cover - Qt cleanup no-op
        pass


def _patch_workers(monkeypatch):
    search_workers: list[_FakeSearchWorker] = []
    detail_workers: list[_FakeDetailWorker] = []

    def make_search_worker(genus, species):
        worker = _FakeSearchWorker(genus, species)
        search_workers.append(worker)
        return worker

    def make_detail_worker(row):
        worker = _FakeDetailWorker(row)
        detail_workers.append(worker)
        return worker

    monkeypatch.setattr(mod, "_CloudSearchWorker", make_search_worker)
    monkeypatch.setattr(mod, "_CloudDetailWorker", make_detail_worker)
    return search_workers, detail_workers


def _make_pane(monkeypatch, genus="Amanita", species="fulva"):
    _app()
    search_workers, detail_workers = _patch_workers(monkeypatch)
    pane = mod.CommunityResultsPane(genus=genus, species=species)
    return pane, search_workers, detail_workers


def _select_row(pane: "mod.CommunityResultsPane", row: int) -> None:
    pane.results_list.setCurrentRow(row)


# ---------------------------------------------------------------------
# Search races: target changes while a search is outstanding
# ---------------------------------------------------------------------


def test_target_change_while_search_pending_still_searches_latest_target(monkeypatch):
    """Search A pending -> target B -> target C: the latest target actually
    gets a search (the old code returned early and skipped it)."""
    pane, search_workers, _ = _make_pane(monkeypatch, genus="Amanita", species="fulva")
    assert len(search_workers) == 1  # search A started on construction

    pane.set_taxon("Cortinarius", "limonius")  # target B
    pane.set_taxon("Cortinarius", "rubellus")  # target C

    # The regression: a new search must actually be issued for both B and C,
    # not skipped because A (and then B) were still "in flight".
    assert len(search_workers) == 3
    assert pane._genus == "Cortinarius"
    assert pane._species == "rubellus"


def test_stale_search_success_and_error_cannot_replace_latest_target(monkeypatch):
    pane, search_workers, _ = _make_pane(monkeypatch, genus="Amanita", species="fulva")
    worker_a = search_workers[0]

    pane.set_taxon("Cortinarius", "limonius")
    worker_b = search_workers[1]

    pane.set_taxon("Cortinarius", "rubellus")
    worker_c = search_workers[2]

    # Stale completions for A and B must be ignored entirely: no results
    # applied, and C's worker tracking must survive untouched.
    worker_a.search_done.emit(
        [{"contributor_label": "wrong species A"}], {}
    )
    assert pane._results == []
    assert pane._search_worker is worker_c

    worker_b.error.emit("stale B error")
    assert pane._search_worker is worker_c
    assert pane.status_label.text() == pane.tr("Searching community spore data...")

    # Only C's completion may actually apply.
    worker_c.search_done.emit([{"contributor_label": "correct C result"}], {})
    assert pane._search_worker is None
    assert len(pane._results) == 1
    assert pane._results[0]["contributor_label"] == "correct C result"
    status = pane.status_label.text()
    for worker in (worker_a, worker_b):
        worker.search_done.emit([{"contributor_label": "obsolete"}], {})
        worker.error.emit("obsolete error")
        worker.finished.emit()
        assert worker not in pane._worker_refs
    assert pane._results[0]["contributor_label"] == "correct C result"
    assert pane.status_label.text() == status
    assert worker_c in pane._worker_refs


# ---------------------------------------------------------------------
# Detail races: target/selection changes while a detail fetch is outstanding
# ---------------------------------------------------------------------


def test_stale_detail_success_cannot_overwrite_current_selection(monkeypatch):
    pane, search_workers, detail_workers = _make_pane(monkeypatch)
    search_workers[0].search_done.emit(
        [
            {"contributor_label": "row 0", "observation_id": 1},
            {"contributor_label": "row 1", "observation_id": 2},
        ],
        {},
    )
    assert len(pane._results) == 2

    _select_row(pane, 0)
    assert len(detail_workers) == 1
    stale_detail_worker = detail_workers[0]

    _select_row(pane, 1)
    assert len(detail_workers) == 2
    current_detail_worker = detail_workers[1]

    # The superseded row-0 detail request must not be able to attach its
    # payload as the current selection/preview once row 1 is selected.
    stale_detail_worker.detail_done.emit({"contributor_label": "stale detail for row 0"})
    assert pane._selected_detail is None

    current_detail_worker.detail_done.emit({"contributor_label": "row 1 detail"})
    assert pane._selected_detail == {"contributor_label": "row 1 detail"}
    stale_detail_worker.detail_done.emit({"contributor_label": "obsolete"})
    stale_detail_worker.error.emit("obsolete error")
    assert pane._selected_detail == {"contributor_label": "row 1 detail"}
    assert pane._selected_error is None


def test_stale_detail_error_cannot_mark_the_current_selection_failed(monkeypatch):
    pane, search_workers, detail_workers = _make_pane(monkeypatch)
    search_workers[0].search_done.emit(
        [
            {"contributor_label": "row 0", "observation_id": 1},
            {"contributor_label": "row 1", "observation_id": 2},
        ],
        {},
    )
    _select_row(pane, 0)
    stale_detail_worker = detail_workers[0]
    _select_row(pane, 1)

    stale_detail_worker.error.emit("stale detail failure for row 0")
    # Row 1 is still loading; the stale row-0 error must not mark it failed.
    assert pane._selected_detail is None
    assert pane._selected_error is None


def test_the_checked_mode_decides_what_the_drawer_adds(monkeypatch):
    """Range summary adds the aggregate; Raw points adds the contributors' points.

    Raw points is only offered when the detail carries measurements, and a
    detail without them falls back to the summary.
    """
    pane, search_workers, detail_workers = _make_pane(monkeypatch)
    search_workers[0].search_done.emit([{"contributor_label": "row 0", "observation_id": 1}], {})
    _select_row(pane, 0)
    assert pane.current_mode_payload() is None  # nothing loaded yet

    detail = {
        "genus": "Amanita", "species": "fulva", "contributor_label": "row 0",
        "length_min": 9.0, "length_max": 12.0, "width_min": 8.0, "width_max": 11.0,
        "measurements_json": [{"length_um": 10.0, "width_um": 9.0}, {"length_um": 11.0, "width_um": 9.5}],
    }
    detail_workers[0].detail_done.emit(detail)
    assert pane.raw_points_radio.isEnabled() is True
    assert pane.raw_points_radio.text().endswith("(n=2)")
    assert pane.current_mode_payload()["source_kind"] == "reference"

    pane.raw_points_radio.setChecked(True)
    payload = pane.current_mode_payload()
    assert payload["source_kind"] == "points"
    assert payload["points"] == [{"length_um": 10.0, "width_um": 9.0}, {"length_um": 11.0, "width_um": 9.5}]

    # A later detail without measurements takes Raw points away again.
    _select_row(pane, -1)
    _select_row(pane, 0)
    detail_workers[-1].detail_done.emit(dict(detail, measurements_json=[]))
    assert pane.raw_points_radio.isEnabled() is False
    assert pane.range_summary_radio.isChecked() is True
    assert pane.current_mode_payload()["source_kind"] == "reference"


def test_detail_pending_then_target_cleared_invalidates_stale_detail(monkeypatch):
    """Changing/clearing the target while a detail fetch is outstanding must
    invalidate it: its later completion cannot produce an attachable stale
    payload under the new (possibly empty) target."""
    pane, search_workers, detail_workers = _make_pane(monkeypatch, genus="Amanita", species="fulva")
    search_workers[0].search_done.emit(
        [{"contributor_label": "row 0", "observation_id": 1}], {}
    )
    _select_row(pane, 0)
    stale_detail_worker = detail_workers[0]
    assert pane._selected_result is not None

    pane.set_taxon("", "")  # target cleared
    assert pane._selected_result is None
    assert pane._results == []

    stale_detail_worker.detail_done.emit({"contributor_label": "must not attach"})
    assert pane._selected_detail is None
    assert pane._results == []
    stale_detail_worker.error.emit("obsolete error")
    assert not pane.has_selection()
    assert pane.current_mode_payload() is None


# ---------------------------------------------------------------------
# Cleanup: close while requests are outstanding
# ---------------------------------------------------------------------


def test_close_with_outstanding_search_and_detail_waits_on_every_tracked_worker(monkeypatch):
    pane, search_workers, detail_workers = _make_pane(monkeypatch)
    search_workers[0].search_done.emit(
        [{"contributor_label": "row 0", "observation_id": 1}], {}
    )
    _select_row(pane, 0)

    pane.set_taxon("Cortinarius", "limonius")  # leaves search[0]/detail[0] superseded but tracked
    assert len(search_workers) == 2

    from PySide6.QtGui import QCloseEvent

    pane.closeEvent(QCloseEvent())

    assert all(worker.waited for worker in search_workers)
    assert all(worker.waited for worker in detail_workers)
    assert pane._search_worker is None
    assert pane._detail_worker is None


def test_callbacks_after_close_cannot_restore_selection(monkeypatch):
    pane, searches, details = _make_pane(monkeypatch)
    searches[0].search_done.emit([{"observation_id": 1}], {})
    _select_row(pane, 0)
    from PySide6.QtGui import QCloseEvent
    pane.closeEvent(QCloseEvent())
    searches[0].search_done.emit([{"observation_id": 2}], {})
    details[0].detail_done.emit({"contributor_label": "obsolete"})
    details[0].error.emit("obsolete")
    assert not pane.has_selection()
    assert pane.current_mode_payload() is None




# ---------------------------------------------------------------------
# The active observation's own community dataset is never offered to
# compare with itself (moved from the retired Add-reference dialog's tests,
# stage 10a2; the drawer builds the pane this way).
# ---------------------------------------------------------------------


def _own_and_published_rows() -> list[dict]:
    return [
        {
            "_kind": "observation",
            "observation_id": 4242,
            "genus": "Cortinarius",
            "species": "limonius",
            "contributor_label": "sporely_community_user_7",
            "measurement_count": 3,
        },
        {
            "_kind": "reference",
            "genus": "Cortinarius",
            "species": "limonius",
            "source": "mycena.no",
            "contributor_label": "mycena.no",
        },
    ]


def _drawer_pane(rows, exclude_observation_cloud_id=None):
    _app()
    return mod.CommunityResultsPane(
        genus="Cortinarius",
        species="limonius",
        results=rows,
        exclude_observation_cloud_id=exclude_observation_cloud_id,
        checkable=True,
    )


def test_the_active_observations_own_dataset_is_excluded_by_cloud_id():
    pane = _drawer_pane(_own_and_published_rows(), exclude_observation_cloud_id="4242")
    assert pane.results_list.count() == 1
    assert [row["_kind"] for row in pane._results] == ["reference"]


def test_another_observation_of_the_same_taxon_is_kept():
    pane = _drawer_pane(_own_and_published_rows(), exclude_observation_cloud_id="9999")
    assert pane.results_list.count() == 2
    assert [row["_kind"] for row in pane._results] == ["observation", "reference"]


def test_rows_of_other_taxa_pass_the_exclusion_untouched():
    rows = _own_and_published_rows() + [
        {"_kind": "observation", "observation_id": 7777, "genus": "Amanita", "species": "muscaria",
         "contributor_label": "sporely_community_user_9", "measurement_count": 5},
    ]
    pane = _drawer_pane(rows, exclude_observation_cloud_id="4242")
    assert [row.get("observation_id") for row in pane._results if row["_kind"] == "observation"] == [7777]


def test_without_an_exclusion_id_every_row_stays():
    pane = _drawer_pane(_own_and_published_rows())
    assert pane.results_list.count() == 2
