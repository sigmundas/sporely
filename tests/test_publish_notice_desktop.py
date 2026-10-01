"""Stage 2c desktop publish notice: transitions, text, Cancel, and both paths."""
from __future__ import annotations

import copy
import os
from pathlib import Path

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
                  _facts(spore_data_visibility="private")):
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
    _facts(taxon_id=None),                            # no species after the save
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
    import utils.cloud_sync as cloud_sync
    recorded = []
    monkeypatch.setattr(cloud_sync, "record_confirmed_location_precision",
                        lambda local_id, value: recorded.append((local_id, value)))
    dialog = observations_tab.ObservationDetailsDialog(parent=None, observation=observation)
    dialog._recorded_precision = recorded
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
    # Realistic shape: both sides changed visibility (a manual field row),
    # the observation is published on both sides (is_draft converged).
    detail["remote_observation"].update({"visibility": "friends", "is_draft": False,
                                         "location_precision": "exact"})
    detail["local_observation"].update({"sharing_scope": "public", "is_draft": False,
                                        "location_precision": "exact"})
    detail["field_rows"] = [{"field": "visibility", "label": "Visibility", "baseline": "private",
                             "local": "public", "remote": "friends", "local_changed": True,
                             "remote_changed": True}]
    detail["automatic_decisions"] = {"fields": [], "media": []}
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1"})
    monkeypatch.setattr(conflict_ui, "get_conflict_detail", lambda *a, **k: copy.deepcopy(detail))
    monkeypatch.setattr(conflict_ui.ConflictDetailWorker, "start", lambda self: self.run())
    monkeypatch.setattr(pn, "load_local_facts", lambda *_a, **_k: _facts(contributions=None))
    import utils.cloud_sync as cloud_sync
    recorded = []
    monkeypatch.setattr(cloud_sync, "record_confirmed_location_precision",
                        lambda local_id, value: recorded.append((local_id, value)))
    started = []
    monkeypatch.setattr(conflict_ui.ConflictPlanApplyWorker, "start", lambda self: started.append(self))
    instance = conflict_ui.CloudConflictDialog(conflicts=[{"local_id": 593, "cloud_id": "902"}])
    instance._populate_detail(copy.deepcopy(detail))
    qapp.processEvents()
    instance._recorded_precision = recorded
    yield instance, started
    instance.close()
    qapp.processEvents()


def test_conflict_resolving_visibility_toward_public_asks_and_cancel_applies_nothing(conflict_dialog, monkeypatch):
    dialog, started = conflict_dialog
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    dialog._set_choice("field:visibility", "local")
    dialog._update_apply_enabled()
    dialog._apply_selected_changes()
    assert len(shown) == 1 and started == []
    assert dialog._selected_choice("field:visibility") == "local"
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or True)
    dialog._apply_selected_changes()
    assert len(shown) == 2 and len(started) == 1


def test_conflict_keeping_cloud_visibility_needs_no_notice(conflict_dialog, monkeypatch):
    dialog, started = conflict_dialog
    monkeypatch.setattr(pn, "show_publish_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    dialog._set_choice("field:visibility", "cloud")
    dialog._update_apply_enabled()
    dialog._apply_selected_changes()
    assert len(started) == 1


# --- Parity: precision increases, preserved levels, lookup timeout ---------------------

@pytest.mark.parametrize("before,after,expected", [
    ("fuzzed", "exact", True), ("hidden", "region", True), ("region", "fuzzed", True),
    ("hidden", None, True),            # missing counts as exact
    ("exact", "fuzzed", False), ("fuzzed", "fuzzed", False), (None, "exact", False),
    ("region", "hidden", False),
])
def test_precision_increase_on_public_observation(before, after, expected):
    assert needs_publish_notice(dict(PUB, location_precision=before),
                                dict(PUB, location_precision=after)) is expected
    # Not public after the change: never a notice.
    assert not needs_publish_notice(
        {"sharing_scope": "public", "is_draft": False, "location_precision": before},
        {"sharing_scope": "friends", "is_draft": False, "location_precision": after})


def test_region_and_hidden_use_the_approximate_text():
    for level in ("region", "hidden"):
        text = build_publish_notice_text(level, _facts())
        assert "An approximate location" in text and "file data" in text


def test_precision_increase_in_details_dialog_asks_and_cancel_restores(monkeypatch, qapp):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "public",
                                                 "location_precision": "fuzzed"})
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    dialog._set_location_precision("exact")
    dialog.accept()
    assert len(shown) == 1 and "The exact location" in shown[0]
    assert dialog._selected_location_precision() == "fuzzed"
    assert dialog.result() == 0
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


@pytest.mark.parametrize("level", ["hidden", "region"])
def test_stored_hidden_or_region_precision_is_kept_on_save(monkeypatch, qapp, level):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "public",
                                                 "location_precision": level})
    monkeypatch.setattr(pn, "show_publish_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    assert dialog.get_data()["location_precision"] == level
    dialog.accept()
    assert dialog.result() == QDialog.Accepted
    # Picking a level in the dialog replaces the stored one.
    dialog.location_precision_exact_radio.click()
    assert dialog.get_data()["location_precision"] == "exact"
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_model_keeps_hidden_and_region_precision():
    from database.models import ObservationDB

    for level in ("hidden", "region", "fuzzed", "exact"):
        assert ObservationDB._normalize_location_precision(level) == level
    assert ObservationDB._normalize_location_precision("bogus") == "exact"


def test_conflict_precision_increase_on_public_asks(conflict_dialog, monkeypatch):
    dialog, started = conflict_dialog
    dialog._current_detail["remote_observation"].update({"visibility": "public", "is_draft": False,
                                                         "location_precision": "region"})
    dialog._current_detail["field_rows"] = [{"field": "location_precision", "label": "Precision",
                                             "local": "exact", "remote": "region"}]
    dialog._choice_specs["field:location_precision"] = {"kind": "field", "field": "location_precision"}
    shown = []
    monkeypatch.setattr(dialog, "_selected_choice", lambda key: "local")
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    prev, resolved = dialog.resolved_observation_state()
    assert resolved["location_precision"] == "exact"
    assert not dialog._confirm_publish_for_plan({"local_id": 593})
    assert len(shown) == 1 and "The exact location" in shown[0]
    assert dialog._recorded_precision == []  # Cancel records nothing
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or True)
    assert dialog._confirm_publish_for_plan({"local_id": 593})
    assert dialog._recorded_precision == [(593, "exact")]
    monkeypatch.setattr(dialog, "_selected_choice", lambda key: "cloud")
    assert dialog._confirm_publish_for_plan({"local_id": 593})
    assert len(shown) == 2


def test_owner_list_lookup_times_out_to_cautious_line():
    import threading
    import time as _time

    release = threading.Event()

    class Slow:
        def list_my_shared_reference_contributions(self):
            release.wait(5)
            return {"status": "ok", "contributions": LISTING}

    start = _time.monotonic()
    assert pn.load_owner_contributions(Slow(), timeout=0.2) is None
    assert _time.monotonic() - start < 2
    release.set()
    text = build_publish_notice_text("exact", _facts(contributions=None))
    assert "References you have shared may appear on this observation." in text
    assert pn.OWNER_LIST_TIMEOUT_S <= 3
    assert pn.load_owner_contributions(_Client({"status": "ok", "contributions": LISTING})) == LISTING


def test_conflict_models_automatic_push_local_decisions(conflict_dialog, monkeypatch):
    """A one-sided local visibility change is pushed automatically; the
    resolved state must include it, so publishing still asks."""
    dialog, started = conflict_dialog
    dialog._current_detail["field_rows"] = []
    dialog._current_detail["automatic_decisions"] = {"fields": [
        {"field": "visibility", "action": "push_local", "local": "public",
         "remote": "friends", "baseline": "friends"},
    ], "media": []}
    _prev, resolved = dialog.resolved_observation_state()
    assert resolved["visibility"] == resolved["sharing_scope"] == "public"
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    assert not dialog._confirm_publish_for_plan({"local_id": 593})
    assert len(shown) == 1
    dialog._current_detail["automatic_decisions"]["fields"][0]["action"] = "pull_cloud"
    assert dialog.resolved_observation_state()[1]["visibility"] == "friends"
    assert dialog._confirm_publish_for_plan({"local_id": 593})


def test_details_dialog_records_explicit_confirmed_precision(monkeypatch, qapp):
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "public",
                                                 "location_precision": "hidden"})
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: True)
    dialog.accept()  # unchanged hidden: nothing recorded
    assert dialog._recorded_precision == []
    dialog.location_precision_exact_radio.click()
    dialog.accept()
    assert dialog._recorded_precision == [(917, "exact")]
    assert dialog.confirmed_location_precision == "exact"
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_client_creation_runs_inside_the_lookup_bound():
    import threading
    import time as _time

    release = threading.Event()
    main = threading.get_ident()
    seen = {}

    def slow_getter():
        seen["thread"] = threading.get_ident()
        release.wait(5)  # e.g. a token refresh that hangs
        return _Client({"status": "ok", "contributions": LISTING})

    start = _time.monotonic()
    assert pn.load_owner_contributions(slow_getter, timeout=0.2) is None
    assert _time.monotonic() - start < 2
    release.set()
    assert seen["thread"] != main
    facts = load_local_facts(None, {"sporely_taxon_id": 1}, client_getter=lambda: None)
    assert facts.contributions is None


def test_notice_compares_against_cloud_precision_not_stale_local(monkeypatch, qapp):
    """Stale local 'exact' while the cloud serves 'hidden': picking Fuzzed
    widens the cloud and must ask, even though it narrows the local value."""
    import utils.cloud_sync as cloud_sync

    monkeypatch.setattr(cloud_sync, "_snapshot_baseline_for_cloud_id",
                        lambda cid: {"location_precision": "hidden"} if cid else {})
    dialog = _details_dialog(monkeypatch, qapp, {"is_draft": 0, "sharing_scope": "public",
                                                 "location_precision": "exact",
                                                 "cloud_id": "cloud-917"})
    shown = []
    monkeypatch.setattr(pn, "show_publish_notice", lambda _p, text: shown.append(text) or False)
    dialog.location_precision_fuzzed_radio.click()
    dialog.accept()
    assert len(shown) == 1 and "An approximate location" in shown[0]
    assert dialog.result() == 0
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_app_start_runs_precision_repair_after_database_init():
    source = (Path(__file__).resolve().parents[1] / "main.py").read_text()
    init = source.index("    init_database()\n")
    repair = source.index("repair_legacy_location_precision()", init)
    assert repair - init < 400
