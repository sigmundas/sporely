"""Tests for Stage 6: CloudSyncDialog OAuth browser login UI.

Covers the Sign-in-in-browser flow, error handling, cancel, reauth, signout,
and _summarize_sync_error for cloud auth errors.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


@pytest.fixture(scope="session")
def qapp():
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication(sys.argv)
    return app


def _make_mock_client(email="user@example.com", user_id="user-uuid"):
    client = MagicMock()
    client.user_email = email
    client.user_id = user_id
    return client


def _open_dialog(qapp, from_stored=None, reauth=False):
    """Open a CloudSyncDialog with full init, mocking credential store."""
    from utils.cloud_sync import CloudReauthRequiredError
    import ui.cloud_sync_dialog as mod

    if reauth:
        side_effect = CloudReauthRequiredError("reauth required")
        ret = None
    else:
        side_effect = None
        ret = from_stored

    with patch.object(mod.SporelyCloudClient, "from_stored_credentials", side_effect=side_effect, return_value=ret):
        with patch("ui.cloud_sync_dialog.get_app_settings", return_value={}):
            from ui.cloud_sync_dialog import CloudSyncDialog
            dlg = CloudSyncDialog()
    dlg.show()
    return dlg


# ---------------------------------------------------------------------------
# Test 1: Logged-out UI shows Sign in in browser — no email/password controls
# ---------------------------------------------------------------------------

def test_logged_out_shows_sign_in_button(qapp):
    dlg = _open_dialog(qapp)
    assert dlg._login_frame.isVisible()
    assert not dlg._sync_frame.isVisible()
    assert dlg._signin_btn.isVisible()
    # No email or password input widgets
    assert not hasattr(dlg, "_email_input"), "email_input must not exist"
    assert not hasattr(dlg, "_pw_input"), "pw_input must not exist"
    assert not hasattr(dlg, "_remember_pw_check"), "remember_pw_check must not exist"
    dlg.close()


# ---------------------------------------------------------------------------
# Test 2: Button starts OAuth worker
# ---------------------------------------------------------------------------

def test_sign_in_button_starts_worker(qapp):
    import ui.cloud_sync_dialog as mod

    dlg = _open_dialog(qapp)

    started = []

    class FakeWorker(mod._OAuthLoginWorker):
        def start(self):
            started.append(True)

    with patch.object(mod, "_OAuthLoginWorker", FakeWorker):
        dlg._signin_btn.click()

    assert started, "Worker must be started when Sign in button is clicked"
    # Clean up
    if dlg._oauth_worker is not None:
        dlg._oauth_worker.cancel()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 3: Successful OAuth session opens sync panel
# ---------------------------------------------------------------------------

def test_successful_oauth_opens_sync_panel(qapp):
    import ui.cloud_sync_dialog as mod

    dlg = _open_dialog(qapp)
    client = _make_mock_client()

    with patch("ui.cloud_sync_dialog.get_app_settings", return_value={}):
        with patch("database.schema.update_app_settings"):
            dlg._on_oauth_success(client)

    assert not dlg._login_frame.isVisible()
    assert dlg._sync_frame.isVisible()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 4: OAuth session is persisted (save_credentials called)
# ---------------------------------------------------------------------------

def test_successful_oauth_saves_credentials(qapp):
    import ui.cloud_sync_dialog as mod

    dlg = _open_dialog(qapp)
    client = _make_mock_client(email="me@test.com")

    with patch("ui.cloud_sync_dialog.get_app_settings", return_value={}):
        with patch("database.schema.update_app_settings"):
            dlg._on_oauth_success(client)

    client.save_credentials.assert_called_once()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 5: OAuthError -> error label shown, button re-enabled
# ---------------------------------------------------------------------------

def test_oauth_error_shows_error_label(qapp):
    dlg = _open_dialog(qapp)
    # Simulate flow state: button hidden, waiting shown
    dlg._signin_btn.setEnabled(False)
    dlg._signin_btn.hide()
    dlg._waiting_label.show()

    dlg._on_oauth_failure("Authorization denied by server.", "oauth_error")

    assert dlg._login_error.isVisible()
    assert "Authorization denied" in dlg._login_error.text()
    assert dlg._signin_btn.isEnabled()
    assert dlg._signin_btn.isVisible()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 6: Timeout OAuthError shows error
# ---------------------------------------------------------------------------

def test_oauth_timeout_shows_error(qapp):
    dlg = _open_dialog(qapp)
    dlg._on_oauth_failure("Timed out waiting for browser sign-in. Please try again.", "oauth_error")
    assert dlg._login_error.isVisible()
    assert "Timed out" in dlg._login_error.text()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 7: Browser-open failure (RuntimeError) shows error
# ---------------------------------------------------------------------------

def test_browser_open_failure_shows_error(qapp):
    dlg = _open_dialog(qapp)
    dlg._on_oauth_failure("No browser available (webbrowser.open returned False).", "runtime_error")
    assert dlg._login_error.isVisible()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 8: Callback-port failure shows error
# ---------------------------------------------------------------------------

def test_callback_port_failure_shows_error(qapp):
    dlg = _open_dialog(qapp)
    dlg._on_oauth_failure("OAuth callback port 8765 is already in use.", "runtime_error")
    assert dlg._login_error.isVisible()
    assert "8765" in dlg._login_error.text()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 9: Cancel -> idle signed-out state, no error shown
# ---------------------------------------------------------------------------

def test_cancel_returns_to_signed_out_no_error(qapp):
    dlg = _open_dialog(qapp)
    # Simulate in-progress state
    dlg._signin_btn.setEnabled(False)
    dlg._signin_btn.hide()
    dlg._waiting_label.show()
    dlg._cancel_btn.show()

    dlg._on_oauth_failure("Sign-in cancelled.", "cancelled")

    assert not dlg._login_error.isVisible(), "Cancel must not show error label"
    assert dlg._signin_btn.isVisible()
    assert dlg._signin_btn.isEnabled()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 10: Closing dialog during auth calls cancel() on worker
# ---------------------------------------------------------------------------

def test_close_during_auth_cancels_worker(qapp):
    import ui.cloud_sync_dialog as mod

    dlg = _open_dialog(qapp)
    mock_worker = MagicMock()
    dlg._oauth_worker = mock_worker

    dlg.close()

    mock_worker.cancel.assert_called_once()


# ---------------------------------------------------------------------------
# Test 11: reauth_required shows "Cloud sign-in is required."
# ---------------------------------------------------------------------------

def test_reauth_required_shows_reauth_message(qapp):
    dlg = _open_dialog(qapp, reauth=True)
    assert dlg._login_frame.isVisible()
    assert "required" in dlg._login_desc_label.text().lower()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 12: Stale cached client (CloudReauthRequiredError) -> login panel
# ---------------------------------------------------------------------------

def test_stale_session_shows_login_panel(qapp):
    dlg = _open_dialog(qapp, reauth=True)
    assert dlg._client is None
    assert dlg._login_frame.isVisible()
    assert not dlg._sync_frame.isVisible()
    dlg.close()


# ---------------------------------------------------------------------------
# Test 13: Sign out -> returns to logged-out panel
# ---------------------------------------------------------------------------

def test_signout_returns_to_login_panel(qapp):
    import ui.cloud_sync_dialog as mod

    client = _make_mock_client()
    dlg = _open_dialog(qapp, from_stored=client)

    # Should be in sync panel
    assert dlg._sync_frame.isVisible()

    with patch.object(mod.SporelyCloudClient, "clear_credentials"):
        dlg._do_signout()

    assert dlg._login_frame.isVisible()
    assert not dlg._sync_frame.isVisible()
    assert dlg._client is None
    dlg.close()


# ---------------------------------------------------------------------------
# Test 14: _summarize_sync_error returns "sign in again" for auth errors
# ---------------------------------------------------------------------------

def test_summarize_sync_error_says_sign_in_again(qapp):
    dlg = _open_dialog(qapp)
    # is_cloud_auth_error detects 401 patterns
    with patch("ui.cloud_sync_dialog.is_cloud_auth_error", return_value=True):
        result = dlg._summarize_sync_error("Invalid JWT")
    assert "sign in again" in result.lower()
    assert "password" not in result.lower()
    dlg.close()
