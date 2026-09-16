"""Stage 3: the iNaturalist publish UI must describe only what Sporely knows.

Stages 1 and 2 made the behaviour right - a stored ``inaturalist_id`` is
verified when the user invokes an action, and a live link gains media instead of
being duplicated. The UI was still create-shaped. These tests pin the wording
and the state contract:

* without a stored link the action is create-oriented;
* with a stored link the action promises a check, not a reachable remote;
* the ordinary selection/enablement refresh issues no iNaturalist request;
* the manual "Clear iNaturalist link…" escape hatch is local-only.

All iNaturalist traffic is faked; nothing here touches the network.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from types import SimpleNamespace

import pytest
from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QApplication, QLabel, QMenu, QTableWidget

from ui import observations_tab
from ui.observations_tab import ObservationsTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


_OBSERVATION_BASE = {
    "id": 7,
    "genus": "Atheniella",
    "species": "flavoalba",
    "publish_target": "inat",
}


def _forbid_network(monkeypatch):
    """Any HTTP verb the uploader module could reach for becomes a test failure."""

    def _boom(*args, **kwargs):
        raise AssertionError("the UI refresh path must not talk to iNaturalist")

    for verb in ("get", "post"):
        monkeypatch.setattr(f"utils.artsobs_uploaders.requests.{verb}", _boom)


class _ExplodingUploader:
    """An uploader whose remote check is a tripwire."""

    key = "inat"
    label = "iNaturalist"

    def check_observation_link(self, observation_id, cookies=None, timeout=None):
        raise AssertionError("check_observation_link must be user-action driven")


def _fake_tab(monkeypatch, *, observations, selected_ids, enabled_keys=("web", "inat")):
    """A minimal stand-in for ObservationsTab carrying only publish-UI state.

    ``observations`` maps local observation id to its row dict, so the real
    local-state helpers (``_observation_has_existing_upload``,
    ``_selection_has_existing_upload_for_uploader``) do the deciding.
    """
    recorded: dict[str, list] = {
        "status_messages": [],
        "set_inat_calls": [],
        "prompts": [],
        "cloud_sync": [],
        "rendered": [],
        "publish_controls_refreshed": [],
    }

    menu = QMenu()
    actions = {}
    base_labels = {}
    for key in enabled_keys:
        label = {"web": "Artsobservasjoner", "inat": "iNaturalist"}.get(key, key)
        actions[key] = menu.addAction(label)
        base_labels[key] = label
    both_action = menu.addAction("Both") if {"web", "inat"}.issubset(set(enabled_keys)) else None

    publish_btn = SimpleNamespace(
        _props={},
        _enabled=None,
        setEnabled=lambda value: publish_btn.__setattr__("_enabled", bool(value)),
        setProperty=lambda name, value: publish_btn._props.__setitem__(name, value),
        setMenu=lambda menu_or_none: None,
        property=lambda name: publish_btn._props.get(name),
    )

    tab = SimpleNamespace(
        tr=lambda text: text,
        publish_menu=menu,
        publish_btn=publish_btn,
        plate_btn=SimpleNamespace(setEnabled=lambda value: None),
        _publish_actions=actions,
        _publish_action_base_labels=base_labels,
        _publish_both_action=both_action,
        _publish_enabled_keys=list(enabled_keys),
        _delete_in_progress=False,
        _artsobs_dead_by_observation_id={},
        _artsobs_public_published_by_observation_id={},
        _refresh_publish_targets_if_needed=lambda: None,
        _disconnect_publish_click_if_needed=lambda: None,
        _selected_observation_ids=lambda: list(selected_ids),
        _selection_matches_uploader_target=lambda key: True,
        _find_table_row_for_observation=lambda observation_id: -1,
        _render_publish_cell=lambda row, obs: recorded["rendered"].append((row, dict(obs or {}))),
        schedule_metadata_cloud_sync=lambda observation_id: recorded["cloud_sync"].append(
            observation_id
        ),
        set_status_message=lambda message, level="info", auto_clear_ms=8000: recorded[
            "status_messages"
        ].append((str(message), str(level))),
    )

    # Bind the real logic under test.
    for name in (
        "_observation_has_existing_upload",
        "_existing_upload_blocks_publish",
        "_uploader_label",
        "_enabled_uploader_labels",
        "_selection_has_existing_upload_for_uploader",
        "_selection_blocks_publish_for_uploader",
        "_inaturalist_action_label",
        "_inaturalist_action_hint",
        "_sync_inaturalist_action_wording",
        "_update_publish_controls",
        "_selected_inaturalist_links",
        "_confirm_clear_inaturalist_link",
        "_clear_inaturalist_link_for_selection",
    ):
        unbound = getattr(ObservationsTab, name)
        setattr(tab, name, (lambda fn: lambda *args, **kwargs: fn(tab, *args, **kwargs))(unbound))

    monkeypatch.setattr(
        observations_tab,
        "ObservationDB",
        SimpleNamespace(
            get_observation=lambda observation_id: (
                dict(observations[observation_id]) if observation_id in observations else None
            ),
            set_inaturalist_id=lambda observation_id, value: (
                recorded["set_inat_calls"].append((observation_id, value)),
                observations[observation_id].__setitem__("inaturalist_id", value),
            )[0],
        ),
    )
    monkeypatch.setattr("utils.artsobs_uploaders.get_uploader", lambda key: _ExplodingUploader())
    _forbid_network(monkeypatch)

    def _ask(parent, title, message, default_yes=False, **kwargs):
        recorded["prompts"].append((str(title), str(message)))
        return recorded.get("confirm", True)

    monkeypatch.setattr(observations_tab, "ask_wrapped_yes_no", _ask)
    return tab, recorded


# --------------------------------------------------------------------------
# A. no stored link -> create-oriented action
# --------------------------------------------------------------------------


def test_action_without_a_stored_link_is_create_oriented(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=None)},
        selected_ids=[7],
    )

    tab._update_publish_controls()

    action = tab._publish_actions["inat"]
    assert action.isEnabled() is True
    assert action.text() == "Publish to iNaturalist"
    assert "new iNaturalist observation" in action.toolTip()


# --------------------------------------------------------------------------
# B. stored link, remote state unknown -> check-oriented action
# --------------------------------------------------------------------------


def test_action_with_a_stored_link_promises_a_check_not_a_live_remote(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )

    tab._update_publish_controls()

    action = tab._publish_actions["inat"]
    # Stage 1 relaxed the stored-id block for iNaturalist; that must hold.
    assert action.isEnabled() is True
    label = action.text()
    hint = action.toolTip()
    # The label must not claim the remote observation is definitely there...
    assert label == "Update iNaturalist…"
    assert "Add images" not in label
    # ...and the hint must name all three things that can happen.
    assert "Check the linked iNaturalist observation" in hint
    assert "add the selected images" in hint
    assert "republish if the link is stale" in hint


def test_stored_link_hint_replaces_publish_directly_wording_for_inat_only(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
        enabled_keys=("inat",),
    )

    tab._update_publish_controls()

    hint = tab.publish_btn.property("_hint_text")
    assert "Publish directly to" not in hint
    assert "Check the linked iNaturalist observation" in hint


def test_status_messages_still_name_the_plain_service_label(qapp, monkeypatch):
    """The state-aware action text must not leak into sentences about the target."""
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )

    tab._update_publish_controls()

    assert tab._publish_actions["inat"].text() == "Update iNaturalist…"
    assert tab._uploader_label("inat") == "iNaturalist"


# --------------------------------------------------------------------------
# C. ordinary refresh performs no remote verification
# --------------------------------------------------------------------------


def test_ordinary_refresh_never_verifies_the_remote_link(qapp, monkeypatch):
    """_ExplodingUploader and the blocked requests verbs are the assertions here."""
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )

    tab._update_publish_controls()
    tab._sync_inaturalist_action_wording()
    tab._selected_inaturalist_links()
    tab._update_publish_controls()


def test_empty_selection_resets_the_action_to_create_wording(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )
    tab._update_publish_controls()
    assert tab._publish_actions["inat"].text() == "Update iNaturalist…"

    tab._selected_observation_ids = lambda: []
    tab._update_publish_controls()

    assert tab._publish_actions["inat"].text() == "Publish to iNaturalist"


# --------------------------------------------------------------------------
# D/E. the invoked action still resolves to append / republish / refusal
# --------------------------------------------------------------------------


def _link_status(state: str, detail: str = ""):
    return SimpleNamespace(
        state=state,
        detail=detail,
        status_code=None,
        is_live=state == "live",
        is_missing=state == "missing",
        is_unverified=state == "unverified",
    )


class _StatusUploader:
    key = "inat"
    label = "iNaturalist"

    def __init__(self, status):
        self._status = status
        self.link_checks: list[int] = []

    def check_observation_link(self, observation_id, cookies=None, timeout=None):
        self.link_checks.append(int(observation_id))
        return self._status


def _decision_tab(monkeypatch, confirm: bool):
    recorded: dict[str, list] = {"prompts": []}

    def _ask(parent, title, message, default_yes=False, **kwargs):
        recorded["prompts"].append((str(title), str(message)))
        return confirm

    monkeypatch.setattr(observations_tab, "ask_wrapped_yes_no", _ask)
    tab = SimpleNamespace(tr=lambda text: text)
    tab._resolve_inaturalist_link_before_publish = (
        lambda observation_id, obs, uploader, cookies: (
            ObservationsTab._resolve_inaturalist_link_before_publish(
                tab, observation_id, obs, uploader, cookies
            )
        )
    )
    tab._confirm_inaturalist_media_append = (
        lambda existing_observation_id, upload_image_paths: (
            ObservationsTab._confirm_inaturalist_media_append(
                tab, existing_observation_id, upload_image_paths
            )
        )
    )
    return tab, recorded


def test_live_link_resolves_to_append_mode(monkeypatch):
    tab, recorded = _decision_tab(monkeypatch, confirm=True)
    uploader = _StatusUploader(_link_status("live"))

    decision = tab._resolve_inaturalist_link_before_publish(
        7, dict(_OBSERVATION_BASE, inaturalist_id=4242), uploader, {"access_token": "tok"}
    )

    assert uploader.link_checks == [4242]
    assert decision.proceed is True
    assert decision.mode == observations_tab.INAT_PUBLISH_MODE_APPEND
    assert decision.existing_observation_id == 4242
    # The append prompt, not the link check, is where the user is asked.
    assert recorded["prompts"] == []


def test_append_confirmation_states_the_count_the_id_and_the_duplicate_risk(monkeypatch):
    tab, recorded = _decision_tab(monkeypatch, confirm=True)

    assert tab._confirm_inaturalist_media_append(4242, ["/tmp/a.jpg", "/tmp/b.jpg"]) is True

    title, message = recorded["prompts"][0]
    assert title == "Add images to iNaturalist"
    assert "2 selected image(s)" in message
    assert "4242" in message
    assert "duplicate" in message
    assert "Nothing else on the iNaturalist observation is changed." in message


def test_missing_link_resolves_to_a_republish_confirmation_naming_the_id(monkeypatch):
    tab, recorded = _decision_tab(monkeypatch, confirm=True)
    uploader = _StatusUploader(_link_status("missing", "404"))

    decision = tab._resolve_inaturalist_link_before_publish(
        7, dict(_OBSERVATION_BASE, inaturalist_id=4242), uploader, {"access_token": "tok"}
    )

    assert decision.proceed is True
    assert decision.mode == observations_tab.INAT_PUBLISH_MODE_CREATE
    title, message = recorded["prompts"][0]
    assert title == "Republish to iNaturalist"
    assert "4242" in message
    assert "no longer exists" in message
    assert "new iNaturalist observation" in message
    # Republish and append must not read as the same operation.
    assert "duplicate" not in message


def test_declined_republish_is_a_skip_with_no_message(monkeypatch):
    tab, _recorded = _decision_tab(monkeypatch, confirm=False)
    uploader = _StatusUploader(_link_status("missing"))

    decision = tab._resolve_inaturalist_link_before_publish(
        7, dict(_OBSERVATION_BASE, inaturalist_id=4242), uploader, {"access_token": "tok"}
    )

    assert decision.proceed is False
    assert decision.failure_message is None


@pytest.mark.parametrize("detail", ["401 Unauthorized", "connection timed out"])
def test_unverified_link_warns_and_is_not_treated_as_stale(monkeypatch, detail):
    tab, recorded = _decision_tab(monkeypatch, confirm=True)
    uploader = _StatusUploader(_link_status("unverified", detail))

    decision = tab._resolve_inaturalist_link_before_publish(
        7, dict(_OBSERVATION_BASE, inaturalist_id=4242), uploader, {"access_token": "tok"}
    )

    assert decision.proceed is False
    assert decision.failure_level == "warning"
    assert "could not check the linked" in decision.failure_message
    assert "existing link was kept" in decision.failure_message
    assert detail in decision.failure_message
    # "unverified" must never be converted into "stale": no republish offer.
    assert recorded["prompts"] == []


# --------------------------------------------------------------------------
# Clear iNaturalist link…
# --------------------------------------------------------------------------


def test_clear_link_confirmed_clears_the_id_without_touching_inaturalist(qapp, monkeypatch):
    observations = {7: dict(_OBSERVATION_BASE, inaturalist_id=4242)}
    tab, recorded = _fake_tab(monkeypatch, observations=observations, selected_ids=[7])
    recorded["confirm"] = True
    refreshed: list[bool] = []
    tab._update_publish_controls = lambda: refreshed.append(True)

    tab._clear_inaturalist_link_for_selection()

    # Local id cleared through the ordinary setter, which marks the row dirty.
    assert recorded["set_inat_calls"] == [(7, None)]
    assert observations[7]["inaturalist_id"] is None
    # Metadata cloud-sync bookkeeping, as for any other publication-id change.
    assert recorded["cloud_sync"] == [7]
    # UI refreshed.
    assert refreshed == [True]
    message, level = recorded["status_messages"][0]
    assert level == "info"
    assert "Nothing on iNaturalist was changed." in message
    # _ExplodingUploader and the blocked requests verbs prove no API call happened.


def test_clear_link_declined_changes_nothing(qapp, monkeypatch):
    observations = {7: dict(_OBSERVATION_BASE, inaturalist_id=4242)}
    tab, recorded = _fake_tab(monkeypatch, observations=observations, selected_ids=[7])
    recorded["confirm"] = False

    tab._clear_inaturalist_link_for_selection()

    assert recorded["prompts"]  # the user really was asked
    assert recorded["set_inat_calls"] == []
    assert observations[7]["inaturalist_id"] == 4242
    assert recorded["cloud_sync"] == []
    assert recorded["status_messages"] == []


def test_clear_link_confirmation_says_iNaturalist_is_not_modified(qapp, monkeypatch):
    tab, recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )
    recorded["confirm"] = False

    tab._clear_inaturalist_link_for_selection()

    title, message = recorded["prompts"][0]
    assert title == "Clear iNaturalist link"
    # The existing remote id is named so the user can check it first.
    assert "4242" in message
    assert "from Sporely only" in message
    assert "does not delete or modify anything on iNaturalist" in message
    assert "photos stay exactly as they are" in message


def test_clear_link_covers_every_linked_row_in_a_multi_selection(qapp, monkeypatch):
    observations = {
        7: dict(_OBSERVATION_BASE, id=7, inaturalist_id=4242),
        8: dict(_OBSERVATION_BASE, id=8, inaturalist_id=None),
        9: dict(_OBSERVATION_BASE, id=9, inaturalist_id=5150),
    }
    tab, recorded = _fake_tab(monkeypatch, observations=observations, selected_ids=[7, 8, 9])
    recorded["confirm"] = True

    tab._clear_inaturalist_link_for_selection()

    assert recorded["set_inat_calls"] == [(7, None), (9, None)]
    assert recorded["cloud_sync"] == [7, 9]
    _title, message = recorded["prompts"][0]
    assert "4242, 5150" in message


def test_clear_link_without_a_stored_link_is_a_no_op(qapp, monkeypatch):
    observations = {7: dict(_OBSERVATION_BASE, inaturalist_id=None)}
    tab, recorded = _fake_tab(monkeypatch, observations=observations, selected_ids=[7])

    tab._clear_inaturalist_link_for_selection()

    assert recorded["prompts"] == []
    assert recorded["set_inat_calls"] == []
    message, level = recorded["status_messages"][0]
    assert level == "warning"
    assert "stored iNaturalist link" in message


def _context_menu_tab(monkeypatch, observations, selected_ids):
    """A fake tab with a real table, for exercising the row context menu.

    ``QMenu.exec`` is modal, so it is replaced by a recorder; the menu object it
    is called on is what the assertions inspect.
    """
    tab, recorded = _fake_tab(monkeypatch, observations=observations, selected_ids=selected_ids)
    table = QTableWidget(len(observations) or 1, 10)
    tab.table = table
    tab._show_observation_context_menu = lambda pos: ObservationsTab._show_observation_context_menu(
        tab, pos
    )
    shown: list[QMenu] = []

    class _NonModalMenu(QMenu):
        """``exec`` is modal, so record the call instead of opening the menu."""

        def __init__(self, *args, **kwargs):
            # The real code parents the menu to the tab widget; the fake tab is
            # not one, so the parent is dropped.
            super().__init__()

        def exec(self, *args, **kwargs):
            shown.append(self)
            return None

    monkeypatch.setattr(observations_tab, "QMenu", _NonModalMenu)
    recorded["shown_menus"] = shown
    return tab, recorded


def test_context_menu_offers_clear_link_when_a_link_is_stored(qapp, monkeypatch):
    tab, recorded = _context_menu_tab(
        monkeypatch,
        {7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )

    tab._show_observation_context_menu(QPoint(-1, -1))

    menu = recorded["shown_menus"][0]
    actions = menu.actions()
    assert [action.text() for action in actions] == ["Clear iNaturalist link…"]
    assert actions[0].isEnabled() is True
    assert "Nothing on iNaturalist is changed" in actions[0].toolTip()


def test_context_menu_disables_clear_link_without_a_stored_link(qapp, monkeypatch):
    tab, recorded = _context_menu_tab(
        monkeypatch,
        {7: dict(_OBSERVATION_BASE, inaturalist_id=None)},
        selected_ids=[7],
    )

    tab._show_observation_context_menu(QPoint(-1, -1))

    action = recorded["shown_menus"][0].actions()[0]
    assert action.isEnabled() is False
    assert "none of the selected observations has a stored iNaturalist link" in action.toolTip()


# --------------------------------------------------------------------------
# The existing publication link must survive
# --------------------------------------------------------------------------


def test_publication_cell_still_offers_the_inaturalist_link(qapp, monkeypatch):
    table = QTableWidget(1, 10)
    tab = SimpleNamespace(
        tr=lambda text: text,
        table=table,
        _artsobs_dead_by_observation_id={},
        _artsobs_public_published_by_observation_id={},
        _status_hint_controller=SimpleNamespace(register_widget=lambda widget, tooltip: None),
    )

    ObservationsTab._render_publish_cell(tab, 0, {"id": 7, "inaturalist_id": 4242})

    widget = table.cellWidget(0, 9)
    assert isinstance(widget, QLabel)
    assert "https://www.inaturalist.org/observations/4242" in widget.text()
    assert ">iNat<" in widget.text()


def test_publication_cell_drops_the_link_after_the_link_is_cleared(qapp, monkeypatch):
    table = QTableWidget(1, 10)
    tab = SimpleNamespace(
        tr=lambda text: text,
        table=table,
        _artsobs_dead_by_observation_id={},
        _artsobs_public_published_by_observation_id={},
        _status_hint_controller=SimpleNamespace(register_widget=lambda widget, tooltip: None),
    )

    ObservationsTab._render_publish_cell(tab, 0, {"id": 7, "inaturalist_id": 4242})
    ObservationsTab._render_publish_cell(tab, 0, {"id": 7, "inaturalist_id": None})

    assert table.cellWidget(0, 9) is None
    assert table.item(0, 9).text() == "-"


# --------------------------------------------------------------------------
# "Both" protection is unchanged by the UX work
# --------------------------------------------------------------------------


def test_both_stays_disabled_for_an_observation_with_a_stored_inat_id(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=4242)},
        selected_ids=[7],
    )

    tab._update_publish_controls()

    assert tab._publish_both_action.isEnabled() is False
    assert "Use the individual iNaturalist action" in tab._publish_both_action.toolTip()
    # ...while the individual action stays the route for a linked observation.
    assert tab._publish_actions["inat"].isEnabled() is True


def test_both_is_available_when_nothing_is_published_yet(qapp, monkeypatch):
    tab, _recorded = _fake_tab(
        monkeypatch,
        observations={7: dict(_OBSERVATION_BASE, inaturalist_id=None)},
        selected_ids=[7],
    )

    tab._update_publish_controls()

    assert tab._publish_both_action.isEnabled() is True
