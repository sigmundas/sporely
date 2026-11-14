"""Review scenarios for Preferanser's notices (stages 10d1 and 10d2).

The Profile page has a bottom bar of its own; a profile that did not reach
Sporely Cloud says so there, with Details.
Runs on the isolated review profile (``quick_fixes._isolate``): no stored
login state or credentials are read.
"""
from __future__ import annotations

from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry
from .measure_tab import _settle
from .quick_fixes import _isolate


def _profile(context: ReviewContext, *, notice: bool) -> QWidget:
    _isolate(context)
    from ui.main_window import SettingsHubDialog

    hub = SettingsHubDialog(context.host, start_page=SettingsHubDialog.PAGE_PROFILE)
    hub.show()
    _settle(0.3)
    if notice:
        hub._show_profile_notice(
            hub.tr("The local profile was saved, but Sporely Cloud was not updated."),
            "error",
            details="HTTP 503 from api.sporely.no: the service is temporarily unavailable.",
        )
        _settle(0.1)
        hub._profile_hint_bar._details_btn.click()
        _settle(0.2)
    return hub


def _publishing_login_failed(context: ReviewContext) -> QWidget:
    """Online publisering after a failed Artportalen login (stage 10d2), Details open."""
    _isolate(context)
    from ui.main_window import SettingsHubDialog

    hub = SettingsHubDialog(context.host, start_page=SettingsHubDialog.PAGE_PUBLISHING)
    hub.show()
    _settle(0.3)
    publishing = hub._artsobs_dialog
    publishing._show_notice(
        publishing.tr("Artportalen login failed."),
        "error",
        details="Login failed. Please check your email and password.",
    )
    _settle(0.1)
    publishing.hint_bar._details_btn.click()
    _settle(0.2)
    return hub


def _cloud_login_failed(context: ReviewContext) -> QWidget:
    """A failed Sporely Cloud sign-in, said on the Profile page (stage 10d2)."""
    _isolate(context)
    from ui.main_window import SettingsHubDialog

    hub = SettingsHubDialog(context.host, start_page=SettingsHubDialog.PAGE_PUBLISHING)
    hub.show()
    _settle(0.3)
    publishing = hub._artsobs_dialog
    publishing._show_cloud_notice(
        publishing.tr("Sporely Cloud login failed."), "error", details="invalid_grant: authorization code expired",
    )
    _settle(0.1)
    hub._profile_hint_bar._details_btn.click()
    _settle(0.2)
    return hub


def _credential_prompt_missing(context: ReviewContext) -> QWidget:
    """The Artportalen credential prompt after OK with nothing filled in (stage 10d2).

    No keyring is read (the saved-credentials loader is stubbed); ``exec`` is
    replaced while the prompt is built so it shows without blocking.
    """
    from unittest.mock import patch

    from PySide6.QtWidgets import QDialog, QDialogButtonBox

    import utils.artportalen_auth as artportalen_auth

    shown: list[QDialog] = []

    def show_and_press_ok(dialog) -> int:
        dialog.show()
        _settle(0.1)
        box = dialog.findChild(QDialogButtonBox)
        next(b for b in box.buttons() if box.buttonRole(b) == QDialogButtonBox.AcceptRole).click()
        _settle(0.1)
        shown.append(dialog)
        return QDialog.Rejected

    with patch.object(artportalen_auth, "_load_saved_credentials", lambda: ("", None, True)), \
            patch.object(QDialog, "exec", show_and_press_ok):
        artportalen_auth._prompt_credentials(parent=context.host)
    dialog = shown[0]
    context.state.setdefault("preferences.prompts", []).append(dialog)  # kept alive for the capture
    return dialog


def register_preferences_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"preferences.publishing-login-failed{suffix}",
                group="preferences",
                title=f"Preferanser › Online publishing: a failed login, Details open (nb{', dark' if theme == 'dark' else ''})",
                description="A failed service login is a notice in the page's bar (stage 10d2).",
                build=_publishing_login_failed,
                viewport=(960, 680),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="preferences.cloud-login-failed",
            group="preferences",
            title="Preferanser › Profile: a failed Sporely Cloud sign-in (nb)",
            description="Said in the Profile page's bar, where the Sporely Cloud card is (stage 10d2).",
            build=_cloud_login_failed,
            viewport=(960, 680),
            locale="nb_NO",
        )
    )
    for locale in ("nb_NO", "de_DE"):
        registry.register(
            ReviewScenario(
                id=f"preferences.credential-prompt-missing-{locale[:2]}",
                group="preferences",
                title=f"Artportalen login prompt: email and password missing ({locale[:2]})",
                description="What is missing is said in red under the form (stage 10d2); the prompt is translated.",
                build=_credential_prompt_missing,
                viewport=(520, 300),
                locale=locale,
            )
        )
    registry.register(
        ReviewScenario(
            id="preferences.profile",
            group="preferences",
            title="Preferanser › Profile with its bottom bar (nb)",
            description="The Profile page has its own bottom bar for notices (stage 10d1).",
            build=lambda context: _profile(context, notice=False),
            viewport=(960, 680),
            locale="nb_NO",
        )
    )
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"preferences.profile-not-synced{suffix}",
                group="preferences",
                title=f"Preferanser › Profile: not synced, Details open (nb{', dark' if theme == 'dark' else ''})",
                description="Save kept Preferanser open on the Profile page; the error in its bar.",
                build=lambda context: _profile(context, notice=True),
                viewport=(960, 680),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="preferences.profile-not-synced-de",
            group="preferences",
            title="Preferanser › Profile: not synced, Details open (de)",
            description="German strings at the hub's default size.",
            build=lambda context: _profile(context, notice=True),
            viewport=(960, 680),
            locale="de_DE",
        )
    )
