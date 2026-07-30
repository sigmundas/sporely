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
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QLineEdit

from ui.taxon_input_controller import TaxonInputController


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _Lookup:
    def __init__(self, suggestions=None):
        self._suggestions = suggestions or []
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


class _Host:
    """Minimal stand-in for ObservationTab that mirrors the two red-list
    handlers under test, plus the shared `_clear_red_list_for_identity_change`
    helper.

    Uses the exact same signal wiring as the real widget:
    - controller callbacks (invalidated / committed) → helper
    - genus/species/scientific textChanged (gated by controller suspension)
      → helper (unless red-list is already empty).
    """

    def __init__(self, controller: TaxonInputController,
                 genus: QLineEdit, species: QLineEdit,
                 vern: QLineEdit, scientific: QLineEdit) -> None:
        self._taxon_controller = controller
        self._red_list_category = ""
        self._red_list_categories: dict | None = None
        self.clear_calls: list[str] = []
        self.genus_input = genus
        self.species_input = species
        self.vernacular_input = vern
        self.scientific_name_input = scientific
        # Wire signals in the same order as the real tab.
        genus.textChanged.connect(self._on_taxon_identity_field_edited)
        species.textChanged.connect(self._on_taxon_identity_field_edited)
        scientific.textChanged.connect(self._on_taxon_identity_field_edited)

    def _set_red_list_category(self, code: str | None, categories: dict | None) -> None:
        self._red_list_category = str(code or "").strip().upper()
        self._red_list_categories = (
            dict(categories) if isinstance(categories, dict) else None
        )

    def _clear_red_list_for_identity_change(self) -> None:
        self.clear_calls.append("cleared")
        self._set_red_list_category(None, None)

    def on_snapshot_invalidated(self, reason: str | None = None) -> None:
        self._clear_red_list_for_identity_change()

    def on_snapshot_committed(self, snapshot: dict) -> None:
        self._clear_red_list_for_identity_change()

    def _on_taxon_identity_field_edited(self, _text: str) -> None:
        if self._taxon_controller._is_suspended():
            return
        if not self._red_list_category and not self._red_list_categories:
            return
        self._clear_red_list_for_identity_change()


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
    lookup = _Lookup(suggestions)
    controller = TaxonInputController(
        lookup, genus, species, vern, None,
        scientific_name_input=sci,
    )
    host = _Host(controller, genus, species, vern, sci)
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
