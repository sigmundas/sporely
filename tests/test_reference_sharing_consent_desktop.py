"""Stage 2b desktop: consent-gated reference sharing and My shared references."""
from __future__ import annotations

import os
import re
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QMessageBox

from utils import cloud_sync
from ui import reference_sharing_dialogs as rsd
from ui.reference_sharing_dialogs import (
    MySharedReferencesDialog,
    ReferenceShareConsentDialog,
    ReferenceShareRequest,
    consent_locale_for_ui,
    share_status_message,
)

ROOT = Path(__file__).resolve().parents[1]
NEW_RPCS = (
    "share_reference_contribution_with_consent",
    "list_my_shared_reference_contributions",
    "get_reference_share_consent_text",
)


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


@pytest.fixture
def boxes(monkeypatch):
    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(QMessageBox, "information", lambda _p, _t, m, *a, **k: seen.append(("info", m)))
    monkeypatch.setattr(QMessageBox, "warning", lambda _p, _t, m, *a, **k: seen.append(("warn", m)))
    return seen


class FakeClient:
    def __init__(self, consent=None, share_results=None, contributions=None):
        self.consent = consent
        self.share_results = list(share_results or [])
        self.contributions = contributions or []
        self.calls: list[tuple] = []

    def get_reference_share_consent_text(self, locale):
        self.calls.append(("text", locale))
        if isinstance(self.consent, Exception):
            raise self.consent
        return self.consent

    def share_reference_contribution_with_consent(self, *args):
        self.calls.append(("share", *args))
        result = self.share_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def list_my_shared_reference_contributions(self):
        self.calls.append(("list",))
        return {"status": "ok", "contributions": list(self.contributions)}

    def withdraw_reference_contribution(self, contribution_id):
        self.calls.append(("withdraw", contribution_id))
        for row in self.contributions:
            if row["contribution_id"] == contribution_id:
                row["status"] = "withdrawn"
                row["withdrawal_reason"] = "owner"
        return {"status": "withdrawn"}


TEXT = {"status": "ok", "version": 3, "locale": "nb", "text": "Vilkår\n\nLinje to  med  mellomrom.",
        "text_sha256": "x", "scope": {}}
REQUEST = ReferenceShareRequest("set-uuid", 617026, 2, 4, 7, "Work — 8-10", "Mycena")


def _share_calls(client):
    return [c for c in client.calls if c[0] == "share"]


# --- RPC wrappers / allowlist ------------------------------------------------

def test_wrappers_send_exact_rpc_payloads(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user")
    sent = []
    monkeypatch.setattr(client, "_rpc", lambda name, payload=None: sent.append((name, payload)) or {"status": "ok"})
    client.share_reference_contribution_with_consent("s", 1, 2, 3, 4, 5, "nb", "desktop")
    client.list_my_shared_reference_contributions()
    client.get_reference_share_consent_text("en")
    assert sent == [
        ("share_reference_contribution_with_consent", {
            "p_source_measurement_set_id": "s", "p_sporely_taxon_id": 1,
            "p_expected_work_revision": 2, "p_expected_treatment_revision": 3,
            "p_expected_measurement_set_revision": 4, "p_consent_version": 5,
            "p_locale": "nb", "p_consent_client": "desktop",
        }),
        ("list_my_shared_reference_contributions", {}),
        ("get_reference_share_consent_text", {"p_locale": "en"}),
    ]


def test_new_rpcs_are_blocked_during_download_from_cloud():
    for name in NEW_RPCS:
        assert name in cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
        assert name not in cloud_sync._PULL_ONLY_ALLOWED_READ_METHODS
        assert name not in cloud_sync._PULL_ONLY_ALLOWED_RPC_NAMES
    wrapper = cloud_sync.PullOnlyCloudClient(cloud_sync.SporelyCloudClient("t", "u"))
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper.share_reference_contribution_with_consent("s", 1, 1, 1, 1, 1, "en")
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper._rpc("share_reference_contribution_with_consent", {})


def test_sync_never_calls_share():
    """Only the consent dialog may call a share RPC; no sync/database code does."""
    pattern = re.compile(r"\.share_reference_contribution(_with_consent)?\(|_rpc\(\s*['\"]share_reference")
    offenders = []
    for path in [*ROOT.joinpath("utils").rglob("*.py"), *ROOT.joinpath("database").rglob("*.py"),
                 *ROOT.joinpath("ui").rglob("*.py")]:
        rel = path.relative_to(ROOT).as_posix()
        for match in pattern.finditer(path.read_text(encoding="utf-8")):
            line = path.read_text(encoding="utf-8")[: match.start()].count("\n") + 1
            offenders.append(f"{rel}:{line}")
    # The two wrapper definitions in cloud_sync.py and the one dialog call.
    allowed_files = {"utils/cloud_sync.py", "ui/reference_sharing_dialogs.py"}
    assert {o.split(":")[0] for o in offenders} <= allowed_files
    cloud_calls = [o for o in offenders if o.startswith("utils/cloud_sync.py")]
    assert len(cloud_calls) == 2  # the self._rpc(...) bodies of the two wrappers
    assert len([o for o in offenders if o.startswith("ui/")]) == 1


def test_consent_locale_follows_ui_language():
    assert consent_locale_for_ui("nb_NO") == "nb"
    assert consent_locale_for_ui("sv_SE") == "en"
    assert consent_locale_for_ui(None) == "en"


# --- Consent dialog ------------------------------------------------------------

def test_dialog_shows_server_text_verbatim_and_sends_displayed_values(qapp, boxes):
    client = FakeClient(TEXT, [{"status": "created", "row": {}}])
    dialog = ReferenceShareConsentDialog(client, REQUEST, locale="nb")
    assert client.calls[0] == ("text", "nb")
    assert dialog.text_view.toPlainText() == TEXT["text"]
    assert dialog.text_view.isReadOnly()
    assert dialog.share_btn.text() == "Share publicly"
    assert _share_calls(client) == []  # nothing before explicit confirm
    dialog.share_btn.click()
    assert _share_calls(client) == [("share", "set-uuid", 617026, 2, 4, 7, 3, "nb", "desktop")]
    assert dialog.result_status == "created"
    assert boxes[-1][0] == "info"


def test_no_active_text_disables_sharing(qapp, boxes):
    client = FakeClient({"status": "not_found"})
    dialog = ReferenceShareConsentDialog(client, REQUEST, locale="en")
    assert not dialog.share_btn.isEnabled()
    assert dialog.status_label.text() == "Public sharing isn't available yet."
    dialog._on_share_clicked()
    assert _share_calls(client) == []


def test_revision_mismatch_reloads_and_requires_new_confirm(qapp, boxes):
    fresh = ReferenceShareRequest("set-uuid", 617026, 2, 5, 8)
    client = FakeClient(TEXT, [{"status": "revision_mismatch"}, {"status": "updated"}])
    dialog = ReferenceShareConsentDialog(client, REQUEST, locale="nb", reload_request=lambda: fresh)
    dialog.share_btn.click()
    assert dialog.request == fresh
    assert len(_share_calls(client)) == 1  # not retried automatically
    assert [c for c in client.calls if c[0] == "text"] == [("text", "nb"), ("text", "nb")]
    dialog.share_btn.click()
    assert _share_calls(client)[-1][1:6] == ("set-uuid", 617026, 2, 5, 8)


@pytest.mark.parametrize("status,needle", [
    ("consent_required", "sharing terms"),
    ("consent_text_unavailable", "sharing terms"),
    ("qualifying_use_required", "public (not a draft)"),
    ("invalid_taxon", "Sporely taxonomy"),
    ("rate_limited", "Too many requests"),
])
def test_each_failure_status_shows_message(qapp, boxes, status, needle):
    client = FakeClient(TEXT, [{"status": status}])
    dialog = ReferenceShareConsentDialog(client, REQUEST, locale="en")
    dialog.share_btn.click()
    assert dialog.result_status == status
    assert boxes[-1][0] == "warn" and needle in boxes[-1][1]
    assert share_status_message(status)[0] is False


def test_network_error_and_rate_limit_exception(qapp, boxes):
    client = FakeClient(TEXT, [cloud_sync.CloudSyncError("boom"),
                               cloud_sync.CloudSyncError('RPC x: {"status":"rate_limited"}')])
    dialog = ReferenceShareConsentDialog(client, REQUEST, locale="en")
    dialog.share_btn.click()
    assert dialog.result_status == "network_error" and "boom" in boxes[-1][1]
    dialog.share_btn.click()
    assert dialog.result_status == "rate_limited"


def test_consent_text_fetch_network_error(qapp, boxes):
    dialog = ReferenceShareConsentDialog(FakeClient(cloud_sync.CloudSyncError("offline")), REQUEST, locale="en")
    assert not dialog.share_btn.isEnabled()
    assert "offline" in dialog.status_label.text()


# --- My shared references -----------------------------------------------------

def _rows():
    return [
        {"contribution_id": "c1", "status": "shared", "canonical_scientific_name": "Mycena galopus",
         "current_revision": 2, "shared_at": "2026-10-01", "withdrawn_at": None, "hidden_at": None,
         "withdrawal_reason": None, "source_short_label": "Funga", "source_raw_text": "8-10"},
        {"contribution_id": "c2", "status": "withdrawn", "canonical_scientific_name": "Mycena pura",
         "current_revision": 1, "shared_at": "2026-09-01", "withdrawn_at": "2026-10-01",
         "hidden_at": "2026-10-01", "withdrawal_reason": "use_detached",
         "source_short_label": "Other", "source_raw_text": None},
    ]


def test_list_shows_status_species_label_and_moderation(qapp):
    dialog = MySharedReferencesDialog(FakeClient(contributions=_rows()))
    assert dialog.table.rowCount() == 2
    assert dialog.table.item(0, 1).text() == "Mycena galopus"
    assert dialog.table.item(0, 2).text() == "Funga — 8-10"
    assert dialog.table.item(1, 6).text() == "Hidden by moderation"
    assert dialog.table.item(1, 7).text() == "Reference detached from the observation"


def test_stop_sharing_confirms_withdraws_and_refreshes(qapp, monkeypatch):
    client = FakeClient(contributions=_rows())
    dialog = MySharedReferencesDialog(client)
    dialog.table.selectRow(1)
    assert not dialog.stop_btn.isEnabled()  # already withdrawn
    dialog.table.selectRow(0)
    assert dialog.stop_btn.isEnabled()
    monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: False)
    dialog.stop_btn.click()
    assert ("withdraw", "c1") not in client.calls
    monkeypatch.setattr(dialog, "confirm_stop_sharing", lambda: True)
    dialog.stop_btn.click()
    assert ("withdraw", "c1") in client.calls
    assert client.calls.count(("list",)) == 2
    assert dialog.rows[0]["status"] == "withdrawn"


def test_stop_sharing_confirmation_explains_what_it_cannot_undo(qapp, monkeypatch):
    captured = {}

    def fake_question(_parent, _title, text, *a, **k):
        captured["text"] = text
        return QMessageBox.Cancel

    monkeypatch.setattr(QMessageBox, "question", fake_question)
    dialog = MySharedReferencesDialog(FakeClient(contributions=_rows()))
    assert dialog.confirm_stop_sharing() is False
    for needle in ("copies other users", "privately", "cached by third parties"):
        assert needle in captured["text"]


def test_comparison_row_offers_share_for_attached_use(qapp):
    from ui import comparison_panel
    assert hasattr(comparison_panel.ComparisonListWidget, "share_publicly_requested")
    assert "Share publicly…" in Path(comparison_panel.__file__).read_text(encoding="utf-8")
    assert rsd.CONSENT_CLIENT == "desktop"


def test_build_share_request_uses_local_revisions_and_requires_clean_sync(monkeypatch):
    from types import SimpleNamespace as NS
    import database.models as models
    import database.reference_library as lib
    import database.reference_sync_state as state

    use = NS(id="u", observation_id=5, reference_measurement_set_id="ms", reference_revision=7)
    ms = NS(id="ms", taxon_treatment_id="t", revision=7, raw_text="8-10")
    tr_ = NS(id="t", reference_work_id="w", revision=4, name_as_published="Mycena galopus")
    work = NS(id="w", revision=2, short_label="Funga")
    monkeypatch.setattr(lib.ObservationReferenceUseRepository, "get", staticmethod(lambda _i: use))
    monkeypatch.setattr(lib.MeasurementSetRepository, "get", staticmethod(lambda _i: ms))
    monkeypatch.setattr(lib.TaxonTreatmentRepository, "get", staticmethod(lambda _i: tr_))
    monkeypatch.setattr(lib.ReferenceWorkRepository, "get", staticmethod(lambda _i: work))
    monkeypatch.setattr(models.ObservationDB, "get_observation", staticmethod(lambda _i: {"sporely_taxon_id": 617026}))
    clean = NS(remote_identity_state="acknowledged", sync_status="clean")
    states = {"work": clean, "treatment": clean, "measurement_set": clean}
    monkeypatch.setattr(state.ReferenceCloudSyncStateRepository, "get_library",
                        staticmethod(lambda kind, _i: states[kind]))

    request = rsd.build_share_request_for_use("u", 5)
    assert (request.source_measurement_set_id, request.sporely_taxon_id, request.work_revision,
            request.treatment_revision, request.measurement_set_revision) == ("ms", 617026, 2, 4, 7)

    states["treatment"] = NS(remote_identity_state="acknowledged", sync_status="dirty")
    with pytest.raises(ValueError, match="Sync"):
        rsd.build_share_request_for_use("u", 5)
    states["treatment"] = clean
    use.reference_revision = 6
    with pytest.raises(ValueError, match="Update from library"):
        rsd.build_share_request_for_use("u", 5)
