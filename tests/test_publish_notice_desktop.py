"""Stage 2c desktop publish notice: transitions, text, Cancel, and both paths."""
from __future__ import annotations

import copy
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QDialog

import ui.cloud_conflict_dialog as conflict_ui
import ui.observations_tab as observations_tab
import ui.publish_notice as pn
from ui.publish_notice import (
    PublishFacts,
    build_publish_notice_text,
    load_local_facts,
    needs_publish_notice,
    resolve_already_shared,
)


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


PUB = {"sharing_scope": "public", "is_draft": False}


# --- Transitions ---------------------------------------------------------------

@pytest.mark.parametrize("previous,new,expected", [
    (None, PUB, True),                                         # new, published at once
    ({"sharing_scope": "public", "is_draft": True}, PUB, True),   # draft finished while public
    ({"sharing_scope": "friends", "is_draft": False}, PUB, True), # visibility to public
    ({"sharing_scope": "private", "is_draft": True}, PUB, True),
    ({"visibility": "public", "is_draft": 1}, PUB, True),         # cloud row key / int flag
    (PUB, PUB, False),                                          # already public
    ({"visibility": "public", "is_draft": False}, {"sharing_scope": "public", "is_draft": 0}, False),
    (None, {"sharing_scope": "public", "is_draft": True}, False),
    ({"sharing_scope": "friends", "is_draft": False}, {"sharing_scope": "friends", "is_draft": False}, False),
    (PUB, {"sharing_scope": "private", "is_draft": False}, False),
    (None, {"sharing_scope": "public"}, False),                 # missing draft flag = draft
])
def test_shown_exactly_on_publishing_transitions(previous, new, expected):
    assert needs_publish_notice(previous, new) is expected


# --- Already shared -----------------------------------------------------------------

LISTING = [
    {"contribution_id": "c1", "status": "shared", "sporely_taxon_id": 617026,
     "source_measurement_set_id": "set-a", "canonical_scientific_name": "Mycena galopus",
     "source_short_label": "Funga 2020"},
    {"contribution_id": "c2", "status": "withdrawn", "sporely_taxon_id": 617026,
     "source_measurement_set_id": "set-b"},
]


def _facts(**kw):
    base = dict(attached_uses=(("set-a", "contradicts"),), taxon_id=617026, taxon_known=True,
                spore_data_visibility="public", has_photos=True, contributions=LISTING)
    base.update(kw)
    return PublishFacts(**base)


def test_already_shared_line_only_when_it_applies():
    kind, matches = resolve_already_shared(_facts())
    assert kind == "yes" and matches[0][1] == "contradicts"
    text = build_publish_notice_text("exact", _facts())
    assert ("You have already shared the reference Mycena galopus · Funga 2020. It will appear "
            "on this observation as \"contradicts the identification\".") in text
    assert "may appear" not in text
    for facts in (_facts(attached_uses=()), _facts(attached_uses=(("set-b", "compared"),)),
                  _facts(spore_data_visibility="private"), _facts(taxon_id=None)):
        text = build_publish_notice_text("exact", facts)
        assert "already shared" not in text and "may appear" not in text


def test_species_change_in_the_same_save_is_matched():
    assert resolve_already_shared(_facts(taxon_id=999))[0] == "no"
    listing = [dict(LISTING[0], sporely_taxon_id=999)]
    assert resolve_already_shared(_facts(taxon_id=999, contributions=listing))[0] == "yes"


@pytest.mark.parametrize("facts", [
    _facts(contributions=None),                       # failed / rate-limited lookup
    _facts(attached_uses=None),                       # uses unknown
    _facts(taxon_known=False),
    _facts(spore_data_visibility=None),
    _facts(contributions=[{k: v for k, v in LISTING[0].items() if k != "source_measurement_set_id"}]),
])
def test_unknown_lookup_shows_cautious_line(facts):
    text = build_publish_notice_text("exact", facts)
    assert "References you have shared may appear on this observation." in text
    assert "already shared" not in text


class _Client:
    def __init__(self, result):
        self.result = result
        self.calls = 0

    def list_my_shared_reference_contributions(self):
        self.calls += 1
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.parametrize("result", [RuntimeError("offline"), {"status": "rate_limited"},
                                    {"status": "ok", "contributions": "x"}])
def test_load_owner_contributions_failures_are_unknown(result):
    assert pn.load_owner_contributions(_Client(result)) is None
    assert pn.load_owner_contributions(None) is None


def test_load_local_facts_new_observation_uses_state_after_save():
    client = _Client({"status": "ok", "contributions": LISTING})
    facts = load_local_facts(None, {"sporely_taxon_id": 617026, "has_photos": False},
                             client_getter=lambda: client)
    # A new observation has no attached references, so no lookup is needed.
    assert facts.attached_uses == () and client.calls == 0
    assert resolve_already_shared(facts)[0] == "no"


# --- Text matches the verified exposure ------------------------------------------------

def test_text_exact_location_and_public_spore_data():
    text = build_publish_notice_text("exact", _facts())
    for needle in (
        "Anyone, including people who are not signed in, will be able to see:",
        "The exact location: precise coordinates and the location name you entered",
        "Species, date and time of day, the date you created it, habitat, notes, the uncertain "
        "flag, red-list status and your name",
        "The AI identification you selected and its probability",
        "Your photos, as thumbnails and at full size",
        "Microscope photos (including scale bars) and preparation details",
        "Spore measurements, statistics, measurement points and the spore mosaic",
        "Signed-in users can read and write comments on it.",
        "stay private unless you share them one by one with \"Share publicly…\".",
        "The change takes effect after the next sync.",
    ):
        assert needle in text, needle
    assert "file data" not in text and "approximate" not in text.lower()


def test_text_approximate_location_photo_caveat_and_hidden_spores():
    text = build_publish_notice_text("fuzzed", _facts(spore_data_visibility="friends"))
    assert "coordinates rounded to about 1 km and only the region or country name" in text
    assert "Some photos may still contain the exact position in their file data." in text
    assert "stay hidden. Microscope photos and preparation details are still public." in text
    assert "• Spore measurements" not in text
    assert "file data" not in build_publish_notice_text("fuzzed", _facts(has_photos=False))
    assert "file data" in build_publish_notice_text("fuzzed", _facts(has_photos=None))


# --- Observation details dialog --------------------------------------------------------

def _details_dialog(monkeypatch, qapp, baseline):
    from tests.test_red_list_ai_history_not_promoted import _observation_917, _patch_dialog_collaborators

    _patch_dialog_collaborators(monkeypatch)
    observation = _observation_917(**baseline)
    monkeypatch.setattr(observations_tab.ObservationDB, "get_observation",
                        lambda _id: dict(observation))
    monkeypatch.setattr(pn, "load_local_facts", lambda *_a, **_k: _facts(contributions=None))
    dialog = observations_tab.ObservationDetailsDialog(parent=None, observation=observation)
    qapp.processEvents()
    return dialog


@pytest.mark.parametrize("answer", [True, False])
def test_unchecking_draft_while_public_asks_and_cancel_keeps_draft(monkeypatch, qapp, answer):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 1, "sharing_scope": "public"})
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or answer)
    dialog.is_draft_checkbox.setChecked(False)
    dialog.accept()
    assert len(shown) == 1 and "may appear" in shown[0]
    assert dialog.result() == (QDialog.Accepted if answer else 0)
    assert dialog.is_draft_checkbox.isChecked() is (not answer)
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_visibility_to_public_asks_and_cancel_restores_scope(monkeypatch, qapp):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "friends"})
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    dialog._set_sharing_scope("public")
    dialog.accept()
    assert len(shown) == 1
    assert dialog._selected_sharing_scope() == "friends"
    assert dialog.result() == 0
    # Shown every time: a second attempt asks again.
    dialog._set_sharing_scope("public")
    dialog.accept()
    assert len(shown) == 2
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_no_notice_when_already_public(monkeypatch, qapp):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "public"})
    monkeypatch.setattr(pn, "show_publish_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    dialog.accept()
    assert dialog.result() == QDialog.Accepted
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


# --- Cloud conflict dialog --------------------------------------------------------------

@pytest.fixture
def conflict_dialog(qapp, monkeypatch):
    from tests.test_cloud_conflict_dialog import _detail, _fixed_token

    detail = _detail(measurement=False)
    detail["image_pairs"] = []
    detail["remote_observation"].update({"visibility": "public", "is_draft": True})
    detail["local_observation"].update({"sharing_scope": "public", "is_draft": False})
    detail["field_rows"] = [{"field": "is_draft", "label": "Draft state", "baseline": True,
                             "local": False, "remote": True, "local_changed": True,
                             "remote_changed": False}]
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1"})
    monkeypatch.setattr(conflict_ui, "get_conflict_detail", lambda *a, **k: copy.deepcopy(detail))
    monkeypatch.setattr(conflict_ui.ConflictDetailWorker, "start", lambda self: self.run())
    monkeypatch.setattr(pn, "load_local_facts", lambda *_a, **_k: _facts(contributions=None))
    started = []
    monkeypatch.setattr(conflict_ui.ConflictPlanApplyWorker, "start", lambda self: started.append(self))
    instance = conflict_ui.CloudConflictDialog(conflicts=[{"local_id": 593, "cloud_id": "902"}])
    instance._populate_detail(copy.deepcopy(detail))
    qapp.processEvents()
    yield instance, started
    instance.close()
    qapp.processEvents()


def test_conflict_resolving_draft_toward_public_asks_and_cancel_applies_nothing(conflict_dialog, monkeypatch):
    dialog, started = conflict_dialog
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    dialog._set_choice("field:is_draft", "local")
    dialog._update_apply_enabled()
    dialog._apply_selected_changes()
    assert len(shown) == 1 and started == []
    assert dialog._selected_choice("field:is_draft") == "local"
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or True)
    dialog._apply_selected_changes()
    assert len(shown) == 2 and len(started) == 1


def test_conflict_keeping_cloud_draft_needs_no_notice(conflict_dialog, monkeypatch):
    dialog, started = conflict_dialog
    monkeypatch.setattr(pn, "show_publish_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    dialog._set_choice("field:is_draft", "cloud")
    dialog._update_apply_enabled()
    dialog._apply_selected_changes()
    assert len(started) == 1
