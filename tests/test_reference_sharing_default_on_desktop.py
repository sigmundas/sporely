"""Default-on reference sharing on desktop: My shared references (set list,
Stop sharing / Share again), the publish-notice suppression switch, and the
notices for attaching to / making spore data public on a public observation."""
from __future__ import annotations

import os
from pathlib import Path
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.publish_notice as pn
from ui import reference_sharing_dialogs as rsd
from ui.reference_sharing_dialogs import MySharedReferencesDialog
from utils import cloud_sync

ROOT = Path(__file__).resolve().parents[1]
RPCS = ("list_my_reference_sharing", "stop_sharing_reference_set", "share_reference_set_again")
PUB = {"sharing_scope": "public", "is_draft": 0, "spore_data_visibility": "public"}


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


@pytest.fixture
def boxes(monkeypatch):
    seen: list[str] = []
    monkeypatch.setattr(rsd, "show_message", lambda _p, _i, _t, text: seen.append(text))
    return seen


def _set(set_id, status, **kw):
    row = {
        "source_measurement_set_id": set_id, "status": status,
        "stopped_at": "2026-10-01T10:00:00Z" if status in ("stopped",) else None,
        "hidden_by_moderation": status == "hidden", "public_observation_count": 2,
        "species_page_contributions": [{"contribution_id": "c", "sporely_taxon_id": 1,
                                        "canonical_scientific_name": "Mycena galopus",
                                        "current_revision": 3, "shared_at": None,
                                        "hidden_at": None}],
        "source_short_label": f"Work {set_id}", "source_raw_text": "8-10 µm",
    }
    row.update(kw)
    return row


class FakeClient:
    def __init__(self, sets, action_result=None, list_result=None):
        self.sets = sets
        self.action_result = action_result
        self.list_result = list_result
        self.calls: list[tuple] = []

    def list_my_reference_sharing(self):
        self.calls.append(("list",))
        if self.list_result is not None:
            if isinstance(self.list_result, Exception):
                raise self.list_result
            return self.list_result
        return {"status": "ok", "sets": [dict(s) for s in self.sets]}

    def _apply(self, set_id, status):
        for row in self.sets:
            if row["source_measurement_set_id"] == set_id and row["status"] != "hidden":
                row["status"] = status
                row["stopped_at"] = "2026-10-01" if status == "stopped" else None

    def stop_sharing_reference_set(self, set_id):
        self.calls.append(("stop", set_id))
        if isinstance(self.action_result, Exception):
            raise self.action_result
        self._apply(set_id, "stopped")
        return self.action_result or {"status": "updated", "row": None}

    def share_reference_set_again(self, set_id):
        self.calls.append(("share_again", set_id))
        if isinstance(self.action_result, Exception):
            raise self.action_result
        self._apply(set_id, "shared")
        return self.action_result or {"status": "updated", "row": None}


# --- Client wrappers / pull-only block ----------------------------------------

def test_wrappers_send_exact_rpc_payloads(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user")
    sent = []
    monkeypatch.setattr(client, "_rpc", lambda name, payload=None: sent.append((name, payload)) or {})
    client.list_my_reference_sharing()
    client.stop_sharing_reference_set("s1")
    client.share_reference_set_again("s2")
    assert sent == [
        ("list_my_reference_sharing", {}),
        ("stop_sharing_reference_set", {"p_source_measurement_set_id": "s1"}),
        ("share_reference_set_again", {"p_source_measurement_set_id": "s2"}),
    ]


def test_owner_rpcs_are_blocked_during_download_from_cloud():
    for name in RPCS:
        assert name in cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
        assert name not in cloud_sync._PULL_ONLY_ALLOWED_READ_METHODS
    wrapper = cloud_sync.PullOnlyCloudClient(cloud_sync.SporelyCloudClient("t", "u"))
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper.stop_sharing_reference_set("s")


def test_explicit_consent_flow_is_gone():
    for name in ("share_reference_contribution_with_consent", "get_reference_share_consent_text",
                 "list_my_shared_reference_contributions"):
        assert not hasattr(cloud_sync.SporelyCloudClient, name)
    assert not hasattr(rsd, "ReferenceShareConsentDialog")
    assert "Share publicly" not in (ROOT / "ui/comparison_panel.py").read_text(encoding="utf-8")
    assert "_on_comparison_share_publicly_requested" not in (
        ROOT / "ui/main_window.py").read_text(encoding="utf-8")


# --- My shared references --------------------------------------------------------

def test_renders_shared_stopped_and_hidden(qapp):
    client = FakeClient([_set("a", "shared"), _set("b", "stopped"), _set("c", "hidden")])
    dialog = MySharedReferencesDialog(client)
    assert [dialog.table.item(i, 0).text() for i in range(3)] == ["Shared", "Stopped", "Hidden"]
    assert dialog.table.item(0, 1).text() == "Work a — 8-10 µm"
    assert dialog.table.item(0, 2).text() == "2"
    assert dialog.table.item(0, 3).text() == "Mycena galopus"
    dialog.select_row(0)
    assert dialog.stop_btn.isEnabled() and not dialog.share_again_btn.isEnabled()
    dialog.select_row(1)
    assert dialog.share_again_btn.isEnabled() and not dialog.stop_btn.isEnabled()
    dialog.select_row(2)
    assert not dialog.stop_btn.isEnabled() and not dialog.share_again_btn.isEnabled()
    assert "moderation" in dialog.hint_label.text() and "cannot" in dialog.hint_label.text()
    dialog.deleteLater()


def test_hidden_and_stopped_is_hidden_with_no_action(qapp):
    row = _set("c", "hidden", stopped_at="2026-09-01")
    assert rsd.set_status(row) == "hidden" and rsd.set_action("hidden") is None


def test_stop_sharing_calls_rpc_and_reloads(qapp, monkeypatch, boxes):
    client = FakeClient([_set("a", "shared")])
    dialog = MySharedReferencesDialog(client)
    monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: True)
    dialog.select_row(0)
    dialog.stop_btn.click()
    assert ("stop", "a") in client.calls
    assert client.calls[-1] == ("list",)
    assert dialog.table.item(0, 0).text() == "Stopped"
    assert boxes == []
    dialog.deleteLater()


def test_stop_cancelled_calls_nothing(qapp, monkeypatch):
    client = FakeClient([_set("a", "shared")])
    dialog = MySharedReferencesDialog(client)
    monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: False)
    dialog.select_row(0)
    dialog.stop_btn.click()
    assert [c for c in client.calls if c[0] != "list"] == []
    dialog.deleteLater()


def test_share_again_calls_rpc_and_reloads(qapp, boxes):
    client = FakeClient([_set("b", "stopped")])
    dialog = MySharedReferencesDialog(client)
    dialog.select_row(0)
    dialog.share_again_btn.click()
    assert ("share_again", "b") in client.calls and client.calls[-1] == ("list",)
    assert dialog.table.item(0, 0).text() == "Shared"
    assert boxes == []
    dialog.deleteLater()


def test_share_again_that_stays_hidden_never_reports_success(qapp, boxes):
    client = FakeClient([_set("b", "stopped")])
    dialog = MySharedReferencesDialog(client)
    dialog.select_row(0)

    def again(set_id):
        client.calls.append(("share_again", set_id))
        client.sets[0].update(status="hidden", hidden_by_moderation=True)
        return {"status": "updated", "row": None}

    client.share_reference_set_again = again
    dialog.share_again_btn.click()
    assert dialog.table.item(0, 0).text() == "Hidden"
    assert len(boxes) == 1 and "hidden by moderation" in boxes[0]
    dialog.deleteLater()


@pytest.mark.parametrize("result,needle", [
    ({"status": "rate_limited", "retry_after_seconds": 30}, "30 seconds"),
    ({"status": "not_found", "row": None}, "not found"),
    ({"status": "weird"}, "weird"),
    (cloud_sync.CloudSyncError("HTTP 429 rate_limited"), "Too many requests"),
    (cloud_sync.CloudSyncError("offline"), "Could not reach Sporely Cloud"),
])
def test_action_failures_are_reported_truthfully(qapp, monkeypatch, boxes, result, needle):
    client = FakeClient([_set("a", "shared")], action_result=result)
    dialog = MySharedReferencesDialog(client)
    monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: True)
    dialog.select_row(0)
    dialog.stop_btn.click()
    assert len(boxes) == 1 and needle in boxes[0]
    assert client.calls[-1] == ("list",)
    dialog.deleteLater()


@pytest.mark.parametrize("result,needle", [
    ({"status": "rate_limited", "retry_after_seconds": 12}, "12 seconds"),
    ({"status": "error"}, "Could not load"),
    (cloud_sync.CloudSyncError("offline"), "Could not reach Sporely Cloud"),
])
def test_list_failures_are_reported(qapp, result, needle):
    dialog = MySharedReferencesDialog(FakeClient([], list_result=result))
    assert needle in dialog.status_label.text()
    assert dialog.table.rowCount() == 0
    assert not dialog.stop_btn.isEnabled() and not dialog.share_again_btn.isEnabled()
    dialog.deleteLater()


# --- Publish notice suppression ----------------------------------------------------

@pytest.fixture
def settings(monkeypatch):
    store: dict[str, str] = {}
    from database.models import SettingsDB

    monkeypatch.setattr(SettingsDB, "get_setting", staticmethod(lambda k, d=None: store.get(k, d)))
    monkeypatch.setattr(SettingsDB, "set_setting", staticmethod(lambda k, v: store.__setitem__(k, v)))
    return store


def test_suppressed_notice_returns_true_without_showing_and_restore(settings):
    def fail(*_a):
        raise AssertionError("no notice")

    assert pn.publish_notice_enabled() is True
    pn.set_publish_notice_enabled(False)
    assert pn.confirm_publish_if_needed(None, None, dict(PUB), lambda: pn.PublishFacts(), show=fail)
    assert pn.confirm_public_attach_if_needed(None, PUB, ["compared"], show=fail)
    assert pn.confirm_spore_public_if_needed(
        None, dict(PUB, spore_data_visibility="private"), "public", show=fail)
    pn.set_publish_notice_enabled(True)  # Preferences -> show the warning again
    shown = []
    assert not pn.confirm_publish_if_needed(None, None, dict(PUB), lambda: pn.PublishFacts(),
                                            show=lambda _p, t: shown.append(t) or False)
    assert len(shown) == 1


def test_dont_show_again_saved_only_on_accept(qapp, settings, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    def fake_exec(box, accept_button: bool):
        box.checkBox().setChecked(True)
        for button in box.buttons():
            if (box.buttonRole(button) == QMessageBox.AcceptRole) == accept_button:
                button.click()
                return

    monkeypatch.setattr(QMessageBox, "exec", lambda box: fake_exec(box, False))
    assert pn.show_publish_notice(None, "text") is False
    assert pn.publish_notice_enabled() is True
    monkeypatch.setattr(QMessageBox, "exec", lambda box: fake_exec(box, True))
    assert pn.show_publish_notice(None, "text") is True
    assert pn.publish_notice_enabled() is False


def test_preferences_checkbox_restores_the_notice(qapp, settings):
    import ui.main_window as mw

    hub = SimpleNamespace(tr=lambda t: t, _artsobs_dialog=None)
    from PySide6.QtWidgets import QWidget

    hub._artsobs_dialog = QWidget()
    pn.set_publish_notice_enabled(False)
    page = mw.SettingsHubDialog._build_publishing_page(hub)
    assert hub._show_publish_notice_check.isChecked() is False
    hub._show_publish_notice_check.setChecked(True)
    assert pn.publish_notice_enabled() is True
    page.deleteLater()


# --- Attach to an already public observation -----------------------------------------

@pytest.mark.parametrize("obs,expected", [
    (PUB, True),
    (dict(PUB, is_draft=1), False),
    (dict(PUB, sharing_scope="friends"), False),
    (dict(PUB, sharing_scope="private"), False),
    (dict(PUB, spore_data_visibility="private"), False),
    (dict(PUB, spore_data_visibility="friends"), False),
    (dict(PUB, spore_data_visibility=None), True),  # column default is public
    (None, False),
])
def test_attach_notice_decision(obs, expected):
    assert pn.needs_public_attach_notice(obs) is expected


def test_attach_notice_text_names_relationships():
    one = pn.build_attach_notice_text(["contradicts"])
    assert "contradicts the identification" in one and "My shared references" in one
    many = pn.build_attach_notice_text(["compared", "contradicts", "compared"])
    assert "3 reference sets" in many
    assert "2 × compared" in many and "1 × contradicts the identification" in many


def _attach_host(monkeypatch, obs):
    import ui.main_window as mw

    monkeypatch.setattr(pn, "publish_notice_enabled", lambda: True)
    monkeypatch.setattr(mw.ObservationDB, "get_observation", staticmethod(lambda _id: dict(obs)))
    attached = []
    host = SimpleNamespace(tr=lambda t: t, active_observation_id=5)
    host._confirm_public_reference_attach = MethodType(
        mw.MainWindow._confirm_public_reference_attach, host)
    host._attach_normalized_reference_outcome = (
        lambda obs_id, ms_id, role: attached.append((obs_id, ms_id, role)) or ("attached", None, None))
    host._attach_normalized_reference_to_active_observation = MethodType(
        mw.MainWindow._attach_normalized_reference_to_active_observation, host)
    return host, attached


@pytest.mark.parametrize("answer", [True, False])
def test_single_attach_to_public_observation_asks_and_cancel_attaches_nothing(monkeypatch, answer):
    host, attached = _attach_host(monkeypatch, PUB)
    shown = []
    monkeypatch.setattr(pn, "show_attach_notice", lambda _p, t: shown.append(t) or answer)
    host._attach_normalized_reference_to_active_observation("ms-1", "contradicts")
    assert len(shown) == 1 and "contradicts the identification" in shown[0]
    assert attached == ([(5, "ms-1", "contradicts")] if answer else [])


@pytest.mark.parametrize("obs", [
    dict(PUB, is_draft=1), dict(PUB, sharing_scope="private"), dict(PUB, spore_data_visibility="private"),
])
def test_no_attach_notice_for_private_draft_or_non_public_spores(monkeypatch, obs):
    host, attached = _attach_host(monkeypatch, obs)
    monkeypatch.setattr(pn, "show_attach_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    host._attach_normalized_reference_to_active_observation("ms-1", "compared")
    assert attached == [(5, "ms-1", "compared")]


# --- Spore data visibility -> public on a public observation ----------------------------

@pytest.mark.parametrize("obs,new,expected", [
    (dict(PUB, spore_data_visibility="private"), "public", True),
    (dict(PUB, spore_data_visibility="friends"), "public", True),
    (dict(PUB, spore_data_visibility="public"), "public", False),
    (dict(PUB, spore_data_visibility="private"), "friends", False),
    (dict(PUB, spore_data_visibility="private", is_draft=1), "public", False),
    (dict(PUB, spore_data_visibility="private", sharing_scope="friends"), "public", False),
])
def test_spore_public_notice_decision(obs, new, expected):
    assert pn.needs_spore_public_notice(obs, new) is expected


def _spore_host(monkeypatch, stored):
    import ui.main_window as mw
    import utils.cloud_sync as cs
    from database import reference_library

    state = {"stored": dict(stored), "updates": [], "ui_refresh": 0}
    monkeypatch.setattr(pn, "publish_notice_enabled", lambda: True)
    monkeypatch.setattr(mw.ObservationDB, "get_observation",
                        staticmethod(lambda _id: dict(state["stored"])))
    monkeypatch.setattr(mw.ObservationDB, "update_observation",
                        staticmethod(lambda obs_id, **kw: state["updates"].append((obs_id, kw))))
    monkeypatch.setattr(cs, "mark_observation_dirty", lambda _id: None)
    monkeypatch.setattr(
        reference_library.ObservationReferenceUseRepository, "list_for_observation",
        staticmethod(lambda _id: [SimpleNamespace(role="contradicts")]))

    def radio(checked):
        return SimpleNamespace(isChecked=lambda: checked)

    host = SimpleNamespace(
        active_observation_id=7,
        spore_sharing_private_radio=radio(False),
        spore_sharing_friends_radio=radio(False),
        observations_tab=None,
    )
    host._update_spore_sharing_ui = lambda _id: state.__setitem__("ui_refresh", state["ui_refresh"] + 1)
    host._on_spore_sharing_changed = MethodType(mw.MainWindow._on_spore_sharing_changed, host)
    return host, state


@pytest.mark.parametrize("answer", [True, False])
def test_spore_visibility_to_public_on_public_observation_asks(monkeypatch, answer):
    host, state = _spore_host(monkeypatch, dict(PUB, spore_data_visibility="private"))
    shown = []
    monkeypatch.setattr(pn, "show_spore_public_notice", lambda _p, t: shown.append(t) or answer)
    host._on_spore_sharing_changed()
    assert len(shown) == 1
    assert "Spore measurements" in shown[0] and "contradicts the identification" in shown[0]
    if answer:
        assert state["updates"] == [(7, {"spore_data_visibility": "public"})]
    else:
        assert state["updates"] == [] and state["ui_refresh"] == 1


def test_spore_visibility_on_draft_needs_no_notice(monkeypatch):
    host, state = _spore_host(monkeypatch, dict(PUB, spore_data_visibility="private", is_draft=1))
    monkeypatch.setattr(pn, "show_spore_public_notice",
                        lambda *_a: (_ for _ in ()).throw(AssertionError("no notice")))
    host._on_spore_sharing_changed()
    assert state["updates"] == [(7, {"spore_data_visibility": "public"})]


def test_batch_confirm_skips_notice_when_observation_drifted(monkeypatch):
    import ui.main_window as mw

    host, _attached = _attach_host(monkeypatch, PUB)
    host._confirm_picker_batch_attach = MethodType(mw.MainWindow._confirm_picker_batch_attach, host)
    shown = []
    monkeypatch.setattr(pn, "show_attach_notice", lambda _p, t: shown.append(t) or False)
    host.active_observation_id = 6  # picker opened on 5
    assert host._confirm_picker_batch_attach(5, ["compared"]) is True
    assert shown == []
    host.active_observation_id = 5
    assert host._confirm_picker_batch_attach(5, ["compared"]) is False
    assert len(shown) == 1
