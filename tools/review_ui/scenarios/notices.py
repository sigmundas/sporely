"""Review scenarios for the shared non-blocking notice (stage 10b).

A notice lives in each screen's bottom (hint) bar: shown at once, with
optional Details and Undo, and ✕ to dismiss; errors stay until dismissed.
These show it on Rediger's bottom bar and on the Analysis gallery's bar
(a ``HintProgressStack``, which makes room while Details is open).
"""
from __future__ import annotations

from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry
from .analysis_tab import _analysis_tab
from .measure_tab import _settle
from .mode_bar import _edit_mode

TEXT = {
    "nb_NO": {
        "error": "Kunne ikke laste opp bildene til Sporely Cloud. Endringene er lagret på denne maskinen.",
        "details": "HTTP 503 fra storage.sporely.no\nNytt forsøk ved neste synkronisering.\n"
                   "Bilder som venter: IMG_4213.JPG, IMG_4214.JPG, IMG_4220.ORF",
        "undo": "Bildet ble fjernet fra observasjonen.",
        "export": "Kunne ikke eksportere galleriet.",
        "export_details": "PermissionError: [Errno 13] Permission denied: '/Volumes/Felt/galleri.png'",
    },
    "de_DE": {
        "error": "Die Bilder konnten nicht zu Sporely Cloud hochgeladen werden. Die Änderungen sind auf diesem Computer gespeichert.",
        "details": "HTTP 503 von storage.sporely.no\nNeuer Versuch bei der nächsten Synchronisierung.\n"
                   "Wartende Bilder: IMG_4213.JPG, IMG_4214.JPG, IMG_4220.ORF",
        "undo": "Das Bild wurde aus der Beobachtung entfernt.",
        "export": "Die Galerie konnte nicht exportiert werden.",
        "export_details": "PermissionError: [Errno 13] Permission denied: '/Volumes/Felt/galleri.png'",
    },
}


def _rediger_notice(
    context: ReviewContext, kind: str, *, lang: str = "nb_NO", size=(1500, 950), details_open=False,
) -> QWidget:
    host = _edit_mode(context, size=size)
    editor = host.window_under_review.edit_page.editor()
    text = TEXT[lang]
    controller = editor._hint_controller
    if kind == "undo":
        controller.show_notice(text["undo"], "info", undo=lambda: None, timeout_ms=0)
    else:
        controller.show_notice(text["error"], "error", details=text["details"])
    _settle(0.2)
    if details_open:
        editor.hint_bar._details_btn.click()
        _settle(0.2)
    return host


def _analysis_notice(context: ReviewContext) -> QWidget:
    def after(window) -> None:
        _settle(2.0)  # the gallery's own thumbnail status first
        text = TEXT["nb_NO"]
        window._gallery_hint_controller.show_notice(
            text["export"], "error", details=text["export_details"]
        )
        _settle(0.2)
        window.gallery_hint_bar._details_btn.click()

    return _analysis_tab(context, after=after)


def register_notice_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"notice.error{suffix}",
                group="notices",
                title=f"Notice: an error in Rediger's bottom bar (nb{', dark' if theme == 'dark' else ''})",
                description="An error stays until ✕; Details opens the text in the bar (stage 10b).",
                build=lambda context: _rediger_notice(context, "error"),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
        registry.register(
            ReviewScenario(
                id=f"notice.error-details{suffix}",
                group="notices",
                title=f"Notice: an error with Details open (nb{', dark' if theme == 'dark' else ''})",
                description="Details opens below the message, in the bar; the page above gives up the room.",
                build=lambda context: _rediger_notice(context, "error", details_open=True),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="notice.undo",
            group="notices",
            title="Notice: a change with Undo (nb)",
            description="An info notice with Undo; it clears itself after a while.",
            build=lambda context: _rediger_notice(context, "undo"),
            viewport=(1500, 950),
            locale="nb_NO",
        )
    )
    registry.register(
        ReviewScenario(
            id="notice.error-details-small-de",
            group="notices",
            title="Notice: an error with Details open, small window (de)",
            description="German strings in a 1100 × 720 window: the message wraps beside the buttons.",
            build=lambda context: _rediger_notice(
                context, "error", lang="de_DE", size=(1100, 720), details_open=True,
            ),
            viewport=(1100, 720),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="notice.analysis-details",
            group="notices",
            title="Notice: an error in the Analysis gallery's bar, Details open (nb)",
            description="The gallery's hint and progress slot makes room while a notice shows (HintProgressStack).",
            build=_analysis_notice,
            viewport=(1500, 950),
            locale="nb_NO",
        )
    )
