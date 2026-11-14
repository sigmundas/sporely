"""Review scenarios for Preferanser's notices (stage 10d1).

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


def register_preferences_scenarios(registry: ScenarioRegistry) -> None:
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
