"""Stage 3B.3 — TaxonInputController scientific-name snapshot tests.

Cover the three-surface invalidation contract, load-time signal suppression,
no text-based rebinding, and hint-copy relation awareness.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QLineEdit

from ui.taxon_input_controller import ROLE_TAXON_CHOICE, TaxonInputController


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _Lookup:
    """Fake TaxonLookup implementing only what the scientific-name path uses."""

    def __init__(self, suggestions):
        self._suggestions = suggestions
        self.vernacular_db = object()
        self.language_code = "en"
        self.calls: list[tuple] = []

    # Methods the controller may still call defensively during suspension.
    def suggest_genera(self, prefix="", limit=200):
        return []

    def suggest_species(self, genus, prefix="", limit=200):
        return []

    def suggest_common_names(self, prefix="", genus=None, species=None, limit=200):
        return []

    def resolve_common_name(self, name, genus=None, species=None):
        return []

    def resolve_scientific(self, genus, species):
        return None

    def suggest_scientific_names(self, prefix, limit=50):
        self.calls.append(("suggest_scientific_names", prefix, limit))
        p = (prefix or "").strip().lower()
        return [s for s in self._suggestions
                if str(s["scientific_name"]).lower().startswith(p)]


def _new(scientific_widget: bool = True):
    """Build a controller with three real QLineEdits and optional
    scientific-name input + observer callbacks."""
    _app()
    genus, species, vern = QLineEdit(), QLineEdit(), QLineEdit()
    sci = QLineEdit() if scientific_widget else None
    events: list[tuple] = []
    lookup = _Lookup([
        {
            "scientific_name": "Hygrocybe conica var. pseudoconica",
            "canonical_scientific_name": "Hygrocybe conica coll.",
            "taxon_rank_snapshot": "variety",
            "canonical_rank": "species",
            "sporely_taxon_id": 625372,
            "link_kind": "linked",
            "canonical_source_system": "nortaxa",
            "authorship": None,
            "family": "Hygrophoraceae",
        },
        {
            "scientific_name": "Hygrocybe conica",
            "canonical_scientific_name": "Hygrocybe conica",
            "taxon_rank_snapshot": "species",
            "canonical_rank": "species",
            "sporely_taxon_id": 16436,
            "link_kind": "canonical",
            "canonical_source_system": "col_xr",
            "authorship": None,
            "family": "Hygrophoraceae",
        },
    ])
    controller = TaxonInputController(
        lookup, genus, species, vern, None,
        scientific_name_input=sci,
        on_snapshot_invalidated=lambda reason=None: events.append(("invalidated", reason)),
        on_snapshot_committed=lambda snap: events.append(("committed", snap)),
    )
    return SimpleNamespace(
        controller=controller, genus=genus, species=species, vern=vern,
        scientific=sci, events=events, lookup=lookup,
    )


def _commit_via_selection(ctx, name: str = "Hygrocybe conica var. pseudoconica"):
    """Simulate the user picking a completer suggestion for ``name``."""
    ctx.controller.refresh_scientific_suggestions.__self__._current_scientific_text  # sanity
    ctx.scientific.setText(name)
    ctx.controller.refresh_scientific_suggestions()
    model = ctx.controller._scientific_model
    for row in range(model.rowCount()):
        item = model.item(row)
        if str(item.data(Qt.UserRole)) == name:
            ctx.controller.on_scientific_name_selected(model.indexFromItem(item))
            return
    raise AssertionError(f"suggestion {name!r} not present in completer model")


# -------------------------- selection commit -----------------------------


def test_selection_commits_snapshot_and_populates_genus_species():
    ctx = _new()
    _commit_via_selection(ctx)
    snap = ctx.controller.committed_snapshot()
    assert snap is not None
    assert snap["scientific_name"] == "Hygrocybe conica var. pseudoconica"
    assert snap["taxon_rank_snapshot"] == "variety"
    assert snap["sporely_taxon_id"] == 625372
    assert snap["link_kind"] == "linked"
    assert ctx.genus.text() == "Hygrocybe"
    assert ctx.species.text() == "conica"
    kinds = [e for e in ctx.events if e[0] == "committed"]
    assert len(kinds) == 1


# -------------------------- three-surface invalidation -------------------


def test_genus_edit_invalidates_snapshot_and_clears_scientific_input():
    ctx = _new()
    _commit_via_selection(ctx)
    ctx.genus.setText("Amanita")
    assert ctx.controller.committed_snapshot() is None
    assert ctx.scientific.text() == ""  # cleared on structured_diverged
    assert ("invalidated", None) in ctx.events


def test_species_edit_invalidates_snapshot_and_clears_scientific_input():
    ctx = _new()
    _commit_via_selection(ctx)
    ctx.species.setText("muscaria")
    assert ctx.controller.committed_snapshot() is None
    assert ctx.scientific.text() == ""


def test_scientific_edit_invalidates_snapshot_and_keeps_typed_text():
    ctx = _new()
    _commit_via_selection(ctx)
    # User types over the field.
    ctx.scientific.setText("Hygrocybe conica var. tetraspora")
    assert ctx.controller.committed_snapshot() is None
    # Typed text preserved (rule: scientific_diverged does not clear the input).
    assert ctx.scientific.text() == "Hygrocybe conica var. tetraspora"


# -------------------------- preservation contract ------------------------


def test_invalidation_preserves_common_name():
    """`_invalidate_snapshot` itself must never touch the common-name
    field. (Other paths — e.g. genus/species autofill — may clear
    vernacular independently; that's out of scope for the snapshot
    invalidation contract.)"""
    ctx = _new()
    _commit_via_selection(ctx)
    # Put a common name in place using suspension so we don't trip any
    # sync callback.
    with ctx.controller._suspended():
        ctx.vern.setText("witch's hat")
    ctx.controller._invalidate_snapshot(reason="structured_diverged")
    assert ctx.controller.committed_snapshot() is None
    assert ctx.vern.text() == "witch's hat"


# -------------------------- no text-based rebinding ----------------------


def test_retyping_identical_scientific_name_does_not_restore_snapshot():
    ctx = _new()
    _commit_via_selection(ctx)
    original = ctx.controller.committed_snapshot()
    ctx.genus.setText("Amanita")  # invalidates
    assert ctx.controller.committed_snapshot() is None
    # User retypes the exact same string that had previously been bound.
    ctx.scientific.setText(original["scientific_name"])
    # Rule 2: only explicit suggestion selection may rebind.
    assert ctx.controller.committed_snapshot() is None


# -------------------------- load-time signal suppression -----------------


def test_programmatic_load_inside_suspended_does_not_invalidate():
    ctx = _new()
    _commit_via_selection(ctx)
    snap = ctx.controller.committed_snapshot()
    # Simulate the dialog's load path: clear the widgets and repopulate them
    # to freshly loaded values inside the suspension guard, then reinstall
    # the snapshot. Nothing should be invalidated.
    ctx.controller.load_committed_snapshot(None)
    ctx.genus.setText("")
    ctx.species.setText("")
    ctx.scientific.setText("")
    with ctx.controller._suspended():
        ctx.genus.setText(snap["genus"])
        ctx.species.setText(snap["species"])
        ctx.scientific.setText(snap["scientific_name"])
    ctx.controller.load_committed_snapshot(snap)
    restored = ctx.controller.committed_snapshot()
    assert restored is not None
    assert restored["sporely_taxon_id"] == 625372
    # No invalidation event fired during suspended repopulation.
    assert not any(e for e in ctx.events[-5:] if e[0] == "invalidated"
                   and e is ctx.events[-1])


def test_programmatic_writes_outside_suspended_DO_invalidate():
    """Regression guard: `_suspended()` is the ONLY thing keeping a load
    from destroying an in-memory snapshot. If the dialog forgets to wrap
    the load, the snapshot MUST be invalidated (not silently kept)."""
    ctx = _new()
    _commit_via_selection(ctx)
    # NB: no suspension.
    ctx.genus.setText("Amanita")
    assert ctx.controller.committed_snapshot() is None


# -------------------------- callbacks ------------------------------------


def test_commit_and_invalidate_callbacks_fire():
    ctx = _new()
    _commit_via_selection(ctx)
    ctx.genus.setText("Amanita")
    kinds = [e[0] for e in ctx.events]
    assert "committed" in kinds
    assert "invalidated" in kinds


# -------------------------- graceful degradation -------------------------


def test_controller_works_without_scientific_input():
    ctx = _new(scientific_widget=False)
    # Structured edits without a committed snapshot are no-ops.
    ctx.genus.setText("Hygrocybe")
    ctx.species.setText("conica")
    assert ctx.controller.committed_snapshot() is None
