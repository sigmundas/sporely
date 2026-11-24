"""Stage 10d2: the login and publishing pages' messages as notices.

The Sporely Cloud card's messages (sign-in, sign-out, the account link
reset) show in the Profile page's bar, where the card is; the publishing
services' logins, sign-outs and the saved passwords in Online publisering's
bar. The credential prompts say what is missing in red under their form and
put the cursor in the field; they are translated. Only the two
confirmations (reset the account link, remove saved passwords) stay
dialogs. Sign-in and session handling is unchanged.
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QCoreApplication, QTimer, QTranslator
from PySide6.QtWidgets import QApplication, QCheckBox, QDialogButtonBox, QLabel, QLineEdit, QMessageBox, QWidget

import ui.main_window as main_window
from tests.test_preferences_cloud_sync_controls import _build_settings_hub_dialog, qapp  # noqa: F401
from utils import artportalen_auth, artsobservasjoner_auto_login, cloud_sync

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _no_information_or_warning_boxes(monkeypatch):
    def _fail(*args, **_kwargs):
        raise AssertionError(f"a message box was shown: {args[1:3]}")

    monkeypatch.setattr(QMessageBox, "information", _fail)
    monkeypatch.setattr(QMessageBox, "warning", _fail)


def _hub(monkeypatch, qapp, uploaders=None):  # noqa: F811
    parent, hub = _build_settings_hub_dialog(monkeypatch, qapp, uploaders=uploaders)
    return parent, hub, hub._artsobs_dialog


@pytest.fixture
def hub(monkeypatch, qapp):  # noqa: F811
    parent, hub, _publishing = _hub(monkeypatch, qapp)
    yield hub
    hub.deleteLater()
    parent.deleteLater()


def _profile_notice(hub):
    return hub._profile_hint_controller._notice


def _publishing_notice(publishing):
    return publishing._hint_controller._notice


# -- Sporely Cloud: the Profile page's bar ----------------------------------------------

def test_a_failed_cloud_sign_in_says_so_on_the_profile_page(hub, monkeypatch, qapp):  # noqa: F811
    monkeypatch.setattr(main_window, "update_app_settings", lambda _payload: None)
    hub._nav.setCurrentRow(hub.PAGE_PUBLISHING)
    hub._artsobs_dialog._on_cloud_login_failure("invalid_grant: code expired")
    qapp.processEvents()
    assert hub._nav.currentRow() == hub.PAGE_PROFILE
    notice = _profile_notice(hub)
    assert (notice.tone, notice.text, notice.details) == (
        "error", "Sporely Cloud login failed.", "invalid_grant: code expired",
    )
    assert hub._profile_hint_bar.isVisible()


def test_a_sign_in_to_another_accounts_database_says_why(hub, monkeypatch):
    def refuse(_client):
        raise RuntimeError(cloud_sync.ACCOUNT_MISMATCH_MESSAGE)

    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", refuse)
    hub._artsobs_dialog._on_cloud_login_success(SimpleNamespace(), "other@example.com")
    notice = _profile_notice(hub)
    assert (notice.tone, notice.text) == ("error", "This database belongs to another Sporely Cloud account.")
    assert cloud_sync.ACCOUNT_MISMATCH_MESSAGE in notice.details
    assert hub._artsobs_dialog._cloud_client is None


def test_cloud_sign_out_and_account_reset_failures_are_notices(hub, monkeypatch):
    def fail():
        raise OSError("keychain locked")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "clear_credentials", staticmethod(fail))
    hub._artsobs_dialog._logout_cloud()
    notice = _profile_notice(hub)
    assert (notice.tone, notice.text, notice.details) == (
        "error", "Could not sign out of Sporely Cloud.", "keychain locked",
    )

    monkeypatch.setattr(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Ok)
    monkeypatch.setattr("database.models.reset_cloud_sync_state", lambda: None)
    hub._artsobs_dialog._on_reset_cloud_account()
    notice = _profile_notice(hub)
    assert (notice.tone, notice.text) == ("error", "Could not reset the cloud account link.")


def test_a_reset_account_link_says_so_and_the_question_stays(hub, monkeypatch):
    asked = []
    monkeypatch.setattr(QMessageBox, "question", lambda *args, **_k: asked.append(args[1]) or QMessageBox.Ok)
    monkeypatch.setattr("database.models.reset_cloud_sync_state", lambda: None)
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "clear_credentials", staticmethod(lambda: None))
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])
    monkeypatch.setattr(main_window, "update_app_settings", lambda _payload: None)
    hub._artsobs_dialog._on_reset_cloud_account()
    assert asked == ["Reset Cloud Account Link?"]
    notice = _profile_notice(hub)
    assert notice.tone == "success" and notice.text.startswith("Cloud account link has been cleared.")


def test_outside_preferences_cloud_notices_use_the_dialogs_own_bar(monkeypatch, qtbot):
    """“Manage online publishing accounts” opens the dialog on its own."""
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_status", lambda self: None)
    monkeypatch.setattr(main_window, "update_app_settings", lambda _payload: None)
    monkeypatch.setattr(main_window, "get_app_settings", lambda: {})
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr("utils.artsobs_uploaders.list_uploaders", lambda: [])
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: None)
    host = QWidget()
    qtbot.addWidget(host)
    dialog = main_window.ArtsobservasjonerSettingsDialog(host)  # the host owns it
    dialog.show()
    qtbot.waitExposed(dialog)
    dialog._on_cloud_login_failure("Connection closed")
    notice = _publishing_notice(dialog)
    assert (notice.text, notice.details) == ("Sporely Cloud login failed.", "Connection closed")
    assert dialog.hint_bar.isVisible()


def test_a_sign_in_failing_after_preferences_closed_is_still_shown(hub, monkeypatch, qapp):  # noqa: F811
    """A sign-in can end after Preferanser closed (Esc while the code is
    exchanged): the closed window's bar would hide it, so a box shows it."""
    monkeypatch.setattr(main_window, "update_app_settings", lambda _payload: None)
    shown = []
    monkeypatch.setattr(QMessageBox, "warning", lambda _parent, title, text: shown.append((title, text)))
    hub.hide()
    qapp.processEvents()
    hub._artsobs_dialog._on_cloud_login_failure("invalid_grant: code expired")
    assert shown == [("Sporely", "Sporely Cloud login failed.\n\ninvalid_grant: code expired")]
    assert _profile_notice(hub) is None


def test_a_service_login_failing_after_preferences_closed_is_still_shown(publishing, monkeypatch, qapp):  # noqa: F811
    shown = []
    monkeypatch.setattr(QMessageBox, "warning", lambda _parent, title, text: shown.append((title, text)))
    publishing.window().hide()
    qapp.processEvents()
    publishing._show_notice("iNaturalist login failed.", "error", details="timed out")
    assert shown == [("Sporely", "iNaturalist login failed.\n\ntimed out")]


# -- the publishing services: Online publisering's bar ----------------------------------

@pytest.fixture
def publishing(monkeypatch, qapp):  # noqa: F811
    uploaders = [SimpleNamespace(key="artportalen", label="Artportalen")]
    parent, hub, publishing = _hub(monkeypatch, qapp, uploaders=uploaders)
    hub._nav.setCurrentRow(hub.PAGE_PUBLISHING)
    qapp.processEvents()
    yield publishing
    hub.deleteLater()
    parent.deleteLater()


def test_a_failed_service_login_is_a_notice_with_the_error_in_details(publishing, monkeypatch):
    def fail(self, parent=None):
        raise RuntimeError("Login failed. Please check your email and password.")

    monkeypatch.setattr(artportalen_auth.ArtportalenAuth, "login_with_gui", fail)
    publishing._open_login()
    notice = _publishing_notice(publishing)
    assert (notice.tone, notice.text, notice.details) == (
        "error", "Artportalen login failed.", "Login failed. Please check your email and password.",
    )
    assert publishing.hint_bar.isVisible()


def test_a_failed_sign_out_names_the_service(publishing, monkeypatch):
    def fail(self):
        raise PermissionError("cookies file is read-only")

    monkeypatch.setattr(artportalen_auth.ArtportalenAuth, "clear_cookies", fail)
    publishing._logout()
    notice = _publishing_notice(publishing)
    assert (notice.tone, notice.text, notice.details) == (
        "error", "Could not sign out of Artportalen.", "cookies file is read-only",
    )


def test_saved_passwords_removed_or_not_says_so_in_the_bar(publishing, monkeypatch):
    monkeypatch.setattr(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Yes)
    monkeypatch.setattr(artsobservasjoner_auto_login, "clear_saved_web_credentials", lambda: [])
    monkeypatch.setattr(artportalen_auth, "clear_saved_credentials", lambda: ["Artportalen: access denied"])
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])
    publishing._remove_saved_passwords()
    notice = _publishing_notice(publishing)
    assert notice.tone == "warning" and notice.details == "Artportalen: access denied"
    assert notice.text.startswith("Some saved passwords could not be removed.")

    monkeypatch.setattr(artportalen_auth, "clear_saved_credentials", lambda: [])
    publishing._remove_saved_passwords()
    notice = _publishing_notice(publishing)
    assert notice.tone == "success" and notice.text.startswith("Saved passwords were removed.")


# -- the credential prompts: what is missing, inline -------------------------------------

_STEP_ERRORS: list[BaseException] = []


def _drive_modal(steps):
    """Run ``steps(dialog)`` on the modal dialog once it shows.

    A failing step closes the dialog (so the test cannot hang) and is raised
    by ``_raise_step_errors`` once ``exec`` has returned.
    """

    def run():
        dialog = QApplication.activeModalWidget()
        if dialog is None:
            QTimer.singleShot(10, run)
            return
        try:
            steps(dialog)
        except BaseException as exc:  # noqa: BLE001 - re-raised in the test
            _STEP_ERRORS.append(exc)
            dialog.reject()

    QTimer.singleShot(0, run)


def _raise_step_errors():
    if _STEP_ERRORS:
        raise _STEP_ERRORS.pop()


def _ok(dialog):
    box = dialog.findChild(QDialogButtonBox)
    return next(button for button in box.buttons() if box.buttonRole(button) == QDialogButtonBox.AcceptRole)


def _message(dialog):
    return dialog.findChild(QLabel, "credentialValidationMessage")


@pytest.mark.parametrize(
    "prompt, loader",
    [
        (artportalen_auth._prompt_credentials, "_load_saved_credentials"),
        (artsobservasjoner_auto_login._prompt_web_credentials, "_load_saved_web_credentials"),
    ],
    ids=["artportalen", "artsobservasjoner"],
)
def test_a_credential_prompt_says_what_is_missing_under_the_form(prompt, loader, monkeypatch, qapp):  # noqa: F811
    module = artportalen_auth if loader == "_load_saved_credentials" else artsobservasjoner_auto_login
    monkeypatch.setattr(module, loader, lambda: ("", None, True))
    seen = {}

    def steps(dialog):
        email, password = dialog.findChildren(QLineEdit)
        _ok(dialog).click()
        seen["empty"] = (_message(dialog).isVisible(), _message(dialog).text(), dialog.focusWidget() is email)
        email.setFocus()
        email.setText("me@example.com")
        email.textEdited.emit("me@example.com")  # typing clears the message
        seen["typed"] = _message(dialog).isVisible()
        _ok(dialog).click()
        seen["no password"] = (_message(dialog).isVisible(), dialog.focusWidget() is password)
        password.setText("secret")
        _ok(dialog).click()

    _drive_modal(steps)
    username, password, remember = prompt(parent=None)
    _raise_step_errors()
    assert seen["empty"] == (True, "Please enter both email and password.", True)
    assert seen["typed"] is False
    assert seen["no password"] == (True, True)
    assert (username, password, remember) == ("me@example.com", "secret", False)


@pytest.mark.parametrize("retype", [False, True], ids=["kept", "retyped"])
def test_a_saved_password_is_used_unless_retyped(retype, monkeypatch, qapp):  # noqa: F811
    from PySide6.QtTest import QTest

    monkeypatch.setattr(artportalen_auth, "_load_saved_credentials", lambda: ("me@example.com", "saved-secret", True))

    def steps(dialog):
        _email, password = dialog.findChildren(QLineEdit)
        if retype:
            password.selectAll()
            QTest.keyClicks(password, "new-secret")  # real typing: textEdited
        _ok(dialog).click()

    _drive_modal(steps)
    result = artportalen_auth._prompt_credentials(parent=None)
    _raise_step_errors()
    assert result == ("me@example.com", "new-secret" if retype else "saved-secret", True)


def test_mushroom_observers_key_prompt_says_what_is_missing(publishing, qapp):  # noqa: F811
    seen = {}

    def steps(dialog):
        app_key, user_key = dialog.findChildren(QLineEdit)
        _ok(dialog).click()
        seen["no app key"] = (_message(dialog).isVisible(), _message(dialog).text(), dialog.focusWidget() is app_key)
        app_key.setText("app")
        _ok(dialog).click()
        seen["no user key"] = (_message(dialog).isVisible(), _message(dialog).text(), dialog.focusWidget() is user_key)
        dialog.reject()

    _drive_modal(steps)
    assert publishing._prompt_mushroomobserver_login(require_app_key=True) is False
    _raise_step_errors()
    assert seen["no app key"] == (True, "Please enter the app API key and your user API key.", True)
    assert seen["no user key"] == (True, "Please enter your user API key.", True)


def test_the_credential_prompt_is_translated(monkeypatch, qapp):  # noqa: F811
    monkeypatch.setattr(artportalen_auth, "_load_saved_credentials", lambda: ("", None, True))
    translator = QTranslator()
    assert translator.load(str(REPO / "i18n" / "Sporely_nb_NO.qm"))
    QCoreApplication.installTranslator(translator)
    seen = {}

    def steps(dialog):
        seen["title"] = dialog.windowTitle()
        seen["remember"] = dialog.findChild(QCheckBox).text()
        box = dialog.findChild(QDialogButtonBox)
        seen["cancel"] = next(b.text() for b in box.buttons() if box.buttonRole(b) == QDialogButtonBox.RejectRole)
        _ok(dialog).click()
        seen["missing"] = _message(dialog).text()
        dialog.reject()

    try:
        _drive_modal(steps)
        assert artportalen_auth._prompt_credentials(parent=None) == (None, None, False)
        _raise_step_errors()
    finally:
        QCoreApplication.removeTranslator(translator)
    assert seen["title"] == "Logg inn på Artportalen"
    assert seen["cancel"] == "Avbryt"  # the app's text, not Qt's untranslated standard button
    assert "Artportalen" not in seen["remember"] and seen["remember"] != "Save password on this device"
    assert seen["missing"] != "Please enter both email and password."
