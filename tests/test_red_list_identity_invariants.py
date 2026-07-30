"""Stage 3B.3 — Red-list snapshot invariants.

Invariants enforced:

1. Editing common_name alone NEVER clears red-list information.
2. Any manual genus/species/scientific-name edit that invalidates identity
   also clears every red-list snapshot field.
3. Unknown species clears the red-list fields.
4. Explicitly selecting a different taxonomy suggestion clears the previous
   red-list snapshot before any new status is resolved.
5. Never retain a red-list category copied from Artsorakel after the
   scientific identity has changed (whether or not a Stage 3B.3 snapshot
   had been committed).
6. Load-time programmatic writes (inside `_taxon_controller._suspended()`)
   MUST NOT clear the red-list a `_load_observation_values` call restored.
7. Manual genus/species entry that unambiguously resolves to a single
   canonical concept must commit the taxonomy snapshot immediately (so
   the Red List badge refreshes without a Save + Reopen round trip).
   Ambiguous / unresolved edits stay unbound.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QLineEdit

from database.taxon_lookup import ManualScientificResolution
from ui.taxon_input_controller import TaxonInputController


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _Lookup:
    def __init__(self, suggestions=None, manual_resolutions=None):
        self._suggestions = suggestions or []
        # ``manual_resolutions`` maps (genus.casefold(), species.casefold())
        # to a ManualScientificResolution — same contract as the real
        # ``TaxonLookupService.resolve_manual_scientific``.
        self._manual_resolutions = dict(manual_resolutions or {})
        self.vernacular_db = object()
        self.language_code = "en"

    def suggest_genera(self, prefix="", limit=200): return []
    def suggest_species(self, genus, prefix="", limit=200): return []
    def suggest_common_names(self, prefix="", genus=None, species=None, limit=200): return []
    def resolve_common_name(self, name, genus=None, species=None): return []
    def resolve_scientific(self, genus, species): return None
    def suggest_scientific_names(self, prefix, limit=50):
        p = (prefix or "").strip().lower()
        return [s for s in self._suggestions
                if str(s["scientific_name"]).lower().startswith(p)]

    def resolve_manual_scientific(self, genus, species):
        key = (str(genus or "").strip().casefold(),
               str(species or "").strip().casefold())
        return self._manual_resolutions.get(key)


class _Host:
    """Minimal stand-in for ObservationTab that mirrors the two red-list
    handlers under test, plus the shared `_clear_red_list_for_identity_change`
    helper.

    Uses the exact same signal wiring as the real widget:
    - controller callbacks (invalidated / committed) → helper
    - genus/species/scientific textChanged (gated by controller suspension)
      → helper (unless red-list is already empty).
    - genus/species editingFinished → manual (genus, species) resolve
      when no snapshot is committed (Stage 3B.5 badge-refresh follow-up).
    """

    def __init__(self, controller: TaxonInputController,
                 genus: QLineEdit, species: QLineEdit,
                 vern: QLineEdit, scientific: QLineEdit,
                 lookup=None) -> None:
        self._taxon_controller = controller
        self._taxon_lookup = lookup
        self._red_list_category = ""
        self._red_list_categories: dict | None = None
        self.clear_calls: list[str] = []
        self.badge_refresh_calls: list[dict] = []
        self.genus_input = genus
        self.species_input = species
        self.vernacular_input = vern
        self.scientific_name_input = scientific
        # Wire signals in the same order as the real tab.
        genus.textChanged.connect(self._on_taxon_identity_field_edited)
        species.textChanged.connect(self._on_taxon_identity_field_edited)
        scientific.textChanged.connect(self._on_taxon_identity_field_edited)
        genus.editingFinished.connect(self._on_taxon_manual_editing_finished)
        species.editingFinished.connect(self._on_taxon_manual_editing_finished)

    def _set_red_list_category(self, code: str | None, categories: dict | None) -> None:
        self._red_list_category = str(code or "").strip().upper()
        self._red_list_categories = (
            dict(categories) if isinstance(categories, dict) else None
        )

    def _clear_red_list_for_identity_change(self) -> None:
        self.clear_calls.append("cleared")
        self._set_red_list_category(None, None)

    def _schedule_final_redlist_resolution(self) -> None:
        # Mirror of the real dialog's method — records the identity that
        # would be resolved so tests can assert the badge would refresh.
        snap = self._taxon_controller.committed_snapshot() or {}
        self.badge_refresh_calls.append(dict(snap))

    def on_snapshot_invalidated(self, reason: str | None = None) -> None:
        self._clear_red_list_for_identity_change()

    def on_snapshot_committed(self, snapshot: dict) -> None:
        # Same order as the real _on_scientific_snapshot_committed: clear
        # any prior status, then trigger a deferred local red-list apply.
        self._clear_red_list_for_identity_change()
        self._schedule_final_redlist_resolution()

    def _on_taxon_identity_field_edited(self, _text: str) -> None:
        if self._taxon_controller._is_suspended():
            return
        if not self._red_list_category and not self._red_list_categories:
            return
        self._clear_red_list_for_identity_change()

    def _on_taxon_manual_editing_finished(self) -> None:
        controller = self._taxon_controller
        if controller._is_suspended():
            return
        if controller.committed_snapshot() is not None:
            return
        genus = str(self.genus_input.text() or "").strip()
        species = str(self.species_input.text() or "").strip()
        if not genus or not species:
            return
        lookup = self._taxon_lookup
        if lookup is None:
            return
        resolver = getattr(lookup, "resolve_manual_scientific", None)
        if not callable(resolver):
            return
        resolution = resolver(genus, species)
        if resolution is None:
            return
        controller.commit_manual_resolution(
            sporely_taxon_id=resolution.sporely_taxon_id,
            scientific_name=resolution.scientific_name,
            taxon_rank_snapshot=resolution.taxon_rank_snapshot,
            genus=resolution.genus,
            species=resolution.species,
            link_kind=resolution.link_kind,
            canonical_scientific_name=resolution.canonical_scientific_name,
            canonical_rank=resolution.canonical_rank,
        )


@pytest.fixture
def env():
    _app()
    genus, species, vern, sci = (QLineEdit() for _ in range(4))
    suggestions = [
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
            "scientific_name": "Amanita muscaria",
            "canonical_scientific_name": "Amanita muscaria",
            "taxon_rank_snapshot": "species",
            "canonical_rank": "species",
            "sporely_taxon_id": 111111,
            "link_kind": "canonical",
            "canonical_source_system": "col_xr",
            "authorship": None,
            "family": "Amanitaceae",
        },
    ]
    manual_resolutions = {
        # Unique manual resolution — Cortinarius limonius binds a single
        # canonical concept.
        ("cortinarius", "limonius"): ManualScientificResolution(
            sporely_taxon_id=624905,
            genus="Cortinarius",
            species="limonius",
            scientific_name="Cortinarius limonius",
            taxon_rank_snapshot="species",
            canonical_scientific_name="Cortinarius limonius",
            canonical_rank="species",
            link_kind="canonical",
        ),
    }
    lookup = _Lookup(suggestions, manual_resolutions=manual_resolutions)
    controller = TaxonInputController(
        lookup, genus, species, vern, None,
        scientific_name_input=sci,
    )
    host = _Host(controller, genus, species, vern, sci, lookup=lookup)
    controller._on_snapshot_invalidated = lambda reason=None: host.on_snapshot_invalidated(reason)
    controller._on_snapshot_committed = lambda snap: host.on_snapshot_committed(snap)
    return controller, host, (genus, species, vern, sci), suggestions


def _select_suggestion(controller, sci_input, suggestions, name: str) -> None:
    sci_input.setText(name)
    controller.refresh_scientific_suggestions()
    from PySide6.QtCore import Qt
    model = controller._scientific_model
    for row in range(model.rowCount()):
        item = model.item(row)
        if str(item.data(Qt.UserRole)) == name:
            controller.on_scientific_name_selected(model.indexFromItem(item))
            return
    raise AssertionError(f"suggestion {name!r} missing from model")


# ---------------------------------------------------------------- invariant 1
def test_common_name_edit_never_clears_red_list(env):
    controller, host, widgets, _sugs = env
    _genus, _species, vern, _sci = widgets
    host._set_red_list_category("VU", {"no": "VU"})
    vern.setText("witch's hat")
    assert host._red_list_category == "VU"
    assert host._red_list_categories == {"no": "VU"}
    assert host.clear_calls == []


# ---------------------------------------------------------------- invariant 2
def test_genus_edit_clears_red_list_when_snapshot_present(env):
    controller, host, widgets, sugs = env
    genus, _s, _v, sci = widgets
    _select_suggestion(controller, sci, sugs, "Amanita muscaria")
    host._set_red_list_category("VU", {"no": "VU"})
    host.clear_calls.clear()
    genus.setText("Fomes")
    assert host._red_list_category == ""
    assert host._red_list_categories is None
    assert "cleared" in host.clear_calls


def test_species_edit_clears_red_list_when_snapshot_present(env):
    controller, host, widgets, sugs = env
    _g, species, _v, sci = widgets
    _select_suggestion(controller, sci, sugs, "Amanita muscaria")
    host._set_red_list_category("EN", None)
    host.clear_calls.clear()
    species.setText("phalloides")
    assert host._red_list_category == ""


def test_scientific_edit_clears_red_list_when_snapshot_present(env):
    controller, host, widgets, sugs = env
    _g, _s, _v, sci = widgets
    _select_suggestion(controller, sci, sugs, "Amanita muscaria")
    host._set_red_list_category("NT", None)
    host.clear_calls.clear()
    sci.setText("Amanita phalloides")
    assert host._red_list_category == ""


# ---------------------------------------------------------------- invariant 3
def test_unknown_species_clears_red_list(env):
    controller, host, widgets, _sugs = env
    _g, species, _v, _sci = widgets
    # Simulate Artsorakel populating red-list without a committed snapshot.
    host._set_red_list_category("VU", None)
    species.setText("xyzzy-not-a-species")
    assert host._red_list_category == ""


# ---------------------------------------------------------------- invariant 4
def test_new_taxonomy_selection_clears_previous_red_list(env):
    controller, host, widgets, sugs = env
    _g, _s, _v, sci = widgets
    # Prior state: Artsorakel-derived red-list, no snapshot yet.
    host._set_red_list_category("EN", {"no": "EN"})
    host.clear_calls.clear()
    _select_suggestion(controller, sci, sugs, "Hygrocybe conica var. pseudoconica")
    # Red-list cleared as part of the commit (BEFORE any Artsorakel resolve).
    assert host._red_list_category == ""
    assert host._red_list_categories is None
    assert "cleared" in host.clear_calls


# ---------------------------------------------------------------- invariant 5
def test_artsorakel_category_never_survives_identity_change(env):
    controller, host, widgets, _sugs = env
    genus, species, _v, _sci = widgets
    # Reproduce Artsorakel path: no Stage 3B.3 snapshot, red-list set directly.
    with controller._suspended():
        genus.setText("Amanita")
        species.setText("muscaria")
    host._set_red_list_category("LC", {"no": "LC"})
    # Now the observer manually corrects the taxon.
    species.setText("phalloides")
    assert host._red_list_category == ""
    assert host._red_list_categories is None


# ---------------------------------------------------------------- invariant 6
def test_load_time_suspended_writes_do_not_clear_red_list(env):
    controller, host, widgets, _sugs = env
    genus, species, _v, sci = widgets
    host._set_red_list_category("VU", {"no": "VU"})
    with controller._suspended():
        genus.setText("Hygrocybe")
        species.setText("conica")
        sci.setText("Hygrocybe conica var. pseudoconica")
    assert host._red_list_category == "VU"
    assert host._red_list_categories == {"no": "VU"}
    assert host.clear_calls == []


# ---------------------------------------------------------------- idempotence
def test_edit_with_no_red_list_is_a_noop(env):
    controller, host, widgets, _sugs = env
    genus, *_ = widgets
    host._set_red_list_category(None, None)
    genus.setText("Amanita")
    # No spurious clear calls (avoids badge repaint churn).
    assert host.clear_calls == []


# ---------------------------------------------------------------- invariant 7
def test_manual_genus_species_edit_unique_resolution_refreshes_badge(env):
    """When the observer manually types a (genus, species) pair that the
    taxonomy service pins to a single canonical concept, the controller
    must commit the snapshot AND fire the on_snapshot_committed callback
    so the Red List badge can refresh (via _schedule_final_redlist_resolution)
    without a Save + Reopen cycle."""
    controller, host, widgets, _sugs = env
    genus, species, _v, _sci = widgets
    # Preload widgets as the user would after typing both fields.
    genus.setText("Cortinarius")
    species.setText("limonius")
    # Then the user tabs away → editingFinished fires on the species
    # widget (its editingFinished handler is what triggers the manual
    # resolve). Emit it explicitly, matching Qt's real signal path.
    species.editingFinished.emit()
    snap = controller.committed_snapshot()
    assert snap is not None
    assert snap["sporely_taxon_id"] == 624905
    assert snap["scientific_name"] == "Cortinarius limonius"
    assert snap["taxon_rank_snapshot"] == "species"
    assert snap["link_kind"] == "canonical"
    # The badge-refresh scheduler must have been invoked with the
    # committed identity (that's how the badge repopulates without
    # Save + Reopen).
    assert host.badge_refresh_calls, "expected badge refresh to fire"
    refreshed = host.badge_refresh_calls[-1]
    assert refreshed["sporely_taxon_id"] == 624905


def test_manual_genus_species_edit_ambiguous_does_not_bind_badge(env):
    """Ambiguous (genus, species) pairs (multiple canonical concepts) must
    NOT commit a snapshot — the picker is the only path that can bind
    identity for ambiguous inputs."""
    controller, host, widgets, _sugs = env
    genus, species, _v, _sci = widgets
    # `Amanita muscaria` is NOT in the manual_resolutions map — mirrors
    # the real service returning None for ambiguous pairs.
    genus.setText("Amanita")
    species.setText("muscaria")
    species.editingFinished.emit()
    assert controller.committed_snapshot() is None
    assert host.badge_refresh_calls == []


def test_manual_genus_species_edit_unresolved_leaves_badge_clear(env):
    """Unknown (genus, species) pairs must not commit a snapshot either.
    (Same code path as ambiguous — the resolver returns None.)"""
    controller, host, widgets, _sugs = env
    genus, species, _v, _sci = widgets
    genus.setText("Xyzus")
    species.setText("unknownia")
    species.editingFinished.emit()
    assert controller.committed_snapshot() is None
    assert host.badge_refresh_calls == []


def test_manual_editing_finished_does_not_overwrite_prior_snapshot(env):
    """Regression guard: when the observer has already committed a
    specific snapshot via the picker (e.g. a synonym_of_accepted choice),
    a subsequent editingFinished for the same visible genus/species must
    NOT overwrite the prior identity with the canonical resolution."""
    controller, host, widgets, sugs = env
    genus, species, _v, sci = widgets
    # First, use the picker to commit a specific taxon_id.
    _select_suggestion(controller, sci, sugs, "Amanita muscaria")
    committed_before = controller.committed_snapshot()
    assert committed_before is not None
    prior_id = committed_before["sporely_taxon_id"]
    # Even if the lookup would return a different manual resolution for
    # (Amanita, muscaria), the snapshot-present guard must skip it.
    species.editingFinished.emit()
    committed_after = controller.committed_snapshot()
    assert committed_after is not None
    assert committed_after["sporely_taxon_id"] == prior_id


def test_manual_editing_finished_ignored_during_suspend(env):
    """Load-time programmatic writes inside `_suspended()` must not
    trigger a manual resolution (otherwise a reload would immediately
    re-commit the manual snapshot and refresh the badge unnecessarily)."""
    controller, host, widgets, _sugs = env
    genus, species, _v, _sci = widgets
    with controller._suspended():
        genus.setText("Cortinarius")
        species.setText("limonius")
        # Emit editingFinished during suspension — the guard must skip.
        species.editingFinished.emit()
    assert controller.committed_snapshot() is None
    assert host.badge_refresh_calls == []
