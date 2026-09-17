"""Search-field behavior for ``ui.cloud_reference_dialog.CommunityResultsPane``.

Covers the three defects reported against the picker's Community tab:

* a genus alone was discarded client-side (``if len(parts) < 2: return``),
  even though both community RPCs treat an empty species as "any species";
* the field only acted on Return, so typing refreshed nothing;
* the placeholder advertised a name search that was never implemented.

Search workers are faked, so no test here touches the network or a thread.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QObject, Signal
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication

import ui.cloud_reference_dialog as mod
from ui.reference_preview_pane import ReferencePreviewPane


def _app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


_LIVE_PANES: list = []


@pytest.fixture(autouse=True)
def _close_panes():
    """Close every pane a test built, before the next test runs.

    A pane left alive with an armed debounce timer keeps firing into whatever
    ``_CloudSearchWorker`` the *next* test has monkeypatched in, which both
    corrupts that test's worker count and would reach the real worker in a
    test that patches nothing.
    """
    yield
    while _LIVE_PANES:
        _LIVE_PANES.pop().close()


class _FakeSearchWorker(QObject):
    """Records the (genus, species) it was asked for and never completes."""

    search_done = Signal(list, dict)
    error = Signal(str)
    finished = Signal()

    def __init__(self, genus: str, species: str) -> None:
        super().__init__()
        self.genus = genus
        self.species = species

    def start(self) -> None:
        pass

    def wait(self, *_args, **_kwargs) -> bool:
        return True

    def deleteLater(self) -> None:  # pragma: no cover - Qt cleanup no-op
        pass


def _make_pane(monkeypatch, genus="Amanita", species="muscaria"):
    _app()
    searches: list[_FakeSearchWorker] = []

    def make_search_worker(g, s):
        worker = _FakeSearchWorker(g, s)
        searches.append(worker)
        return worker

    monkeypatch.setattr(mod, "_CloudSearchWorker", make_search_worker)
    pane = mod.CommunityResultsPane(
        genus=genus, species=species, preview_pane=ReferencePreviewPane()
    )
    _LIVE_PANES.append(pane)
    return pane, searches


def _type(pane, text: str) -> None:
    """Set the field's text as typing would, without firing the debounce."""
    pane.search_input.setText(text)


# ---------------------------------------------------------------------
# Genus-only search
# ---------------------------------------------------------------------


def test_a_genus_on_its_own_is_searched_with_an_empty_species(monkeypatch):
    """The reported defect: "Amanita" used to be discarded silently."""
    pane, searches = _make_pane(monkeypatch)
    assert len(searches) == 1  # the host taxon, searched on construction

    _type(pane, "Amanita")
    pane._apply_search_text()

    assert len(searches) == 2
    assert searches[-1].genus == "Amanita"
    assert searches[-1].species == ""


def test_a_genus_and_species_still_narrow_the_search(monkeypatch):
    pane, searches = _make_pane(monkeypatch)

    _type(pane, "Hebeloma mesophaeum")
    pane._apply_search_text()

    assert searches[-1].genus == "Hebeloma"
    assert searches[-1].species == "mesophaeum"


def test_a_genus_shorter_than_the_minimum_is_not_sent_to_the_server(monkeypatch):
    """The server matches the genus exactly, so a fragment cannot match."""
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    _type(pane, "Am")
    pane._apply_search_text()

    assert len(searches) == before  # no round trip spent on it
    assert "Keep typing" in pane.status_label.text()


# ---------------------------------------------------------------------
# Per-keystroke refresh, debounced
# ---------------------------------------------------------------------


def test_typing_arms_the_debounce_rather_than_searching_immediately(monkeypatch):
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    _type(pane, "Cortinarius")

    assert pane._search_debounce.isActive()
    assert len(searches) == before


def test_a_burst_of_keystrokes_collapses_into_one_search(monkeypatch):
    """Each keystroke restarts the window, so only the final text is searched."""
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    for text in ("C", "Co", "Cor", "Cort", "Corti", "Cortinarius"):
        _type(pane, text)
        assert len(searches) == before  # nothing issued mid-burst

    pane._apply_search_text()

    assert len(searches) == before + 1
    assert searches[-1].genus == "Cortinarius"


def test_the_debounce_actually_fires_a_search_on_its_own(monkeypatch):
    """End-to-end wiring: no test-only call, just the timer elapsing."""
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    _type(pane, "Cortinarius")
    QTest.qWait(mod._COMMUNITY_SEARCH_DEBOUNCE_MS + 250)

    assert len(searches) == before + 1
    assert searches[-1].genus == "Cortinarius"
    assert not pane._search_debounce.isActive()


def test_return_searches_without_waiting_out_the_debounce(monkeypatch):
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    _type(pane, "Cortinarius")
    pane._on_search_input_submitted()

    assert len(searches) == before + 1
    assert not pane._search_debounce.isActive()


def test_retyping_the_same_taxon_does_not_re_search(monkeypatch):
    """A pointless refresh would throw away the selection and its preview."""
    pane, searches = _make_pane(monkeypatch)

    _type(pane, "Amanita muscaria")
    pane._apply_search_text()
    after_first = len(searches)

    _type(pane, "Amanita muscaria  ")
    pane._apply_search_text()

    assert len(searches) == after_first


# ---------------------------------------------------------------------
# Falling back to the picker's own taxon
# ---------------------------------------------------------------------


def test_clearing_the_field_returns_to_the_hosts_taxon(monkeypatch):
    pane, searches = _make_pane(monkeypatch, genus="Amanita", species="muscaria")

    _type(pane, "Hebeloma")
    pane._apply_search_text()
    assert searches[-1].genus == "Hebeloma"

    _type(pane, "")
    pane._apply_search_text()

    assert searches[-1].genus == "Amanita"
    assert searches[-1].species == "muscaria"


def test_clearing_the_field_follows_a_taxon_changed_while_searching_elsewhere(monkeypatch):
    """The host taxon is tracked separately from the typed override."""
    pane, searches = _make_pane(monkeypatch, genus="Amanita", species="muscaria")

    _type(pane, "Hebeloma")
    pane._apply_search_text()
    pane.set_taxon("Cortinarius", "limonius")

    _type(pane, "")
    pane._apply_search_text()

    assert searches[-1].genus == "Cortinarius"
    assert searches[-1].species == "limonius"


def test_set_taxon_overrides_a_stale_typed_query(monkeypatch):
    """Changing the picker's taxon re-searches for it, typed text or not."""
    pane, searches = _make_pane(monkeypatch, genus="Amanita", species="muscaria")

    _type(pane, "Hebeloma")
    pane._apply_search_text()
    pane.set_taxon("Cortinarius", "limonius")

    assert searches[-1].genus == "Cortinarius"
    assert searches[-1].species == "limonius"


# ---------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------


def test_closing_the_pane_cancels_a_pending_keystroke(monkeypatch):
    """A queued search must not start a thread after shutdown has begun."""
    pane, searches = _make_pane(monkeypatch)
    before = len(searches)

    _type(pane, "Cortinarius")
    assert pane._search_debounce.isActive()
    pane.close()

    assert not pane._search_debounce.isActive()
    QTest.qWait(mod._COMMUNITY_SEARCH_DEBOUNCE_MS + 250)
    assert len(searches) == before


# ---------------------------------------------------------------------
# Placeholder honesty
# ---------------------------------------------------------------------


def test_the_placeholder_does_not_promise_a_name_search(monkeypatch):
    """There is no name/source search path: the first word is always a genus."""
    pane, _ = _make_pane(monkeypatch)

    placeholder = pane.search_input.placeholderText()

    assert "name" not in placeholder.lower()
    assert "genus" in placeholder.lower()
