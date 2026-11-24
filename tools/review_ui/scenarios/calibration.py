"""Review scenarios for Kalibrer objektiv without message boxes (stage 10c).

Validation is inline (New objective) or a disabled action whose bottom-bar
hint says why; failures and results are notices in the bottom bar. Runs on
the isolated review profile (``quick_fixes._isolate``): no stored login,
settings or objectives of the user's are read or written.
"""
from __future__ import annotations

from PySide6.QtCore import QEvent
from PySide6.QtWidgets import QApplication, QWidget

from ..context import ReviewContext
from ..live_lab_harness import use_database
from ..registry import ReviewScenario, ScenarioRegistry
from .measure_tab import _settle
from .quick_fixes import _isolate


def _fresh(context: ReviewContext) -> None:
    _isolate(context)
    count = int(context.state.get("calibration.builds", 0)) + 1
    context.state["calibration.builds"] = count
    use_database(context.state["quick-fixes.profile"], f"calibration-{count}")


def _calibration(context: ReviewContext, state: str, *, size=(1200, 860)) -> QWidget:
    from ui.calibration_dialog import CalibrationDialog

    _fresh(context)
    dialog = CalibrationDialog()
    dialog.resize(*size)
    dialog.show()
    _settle(0.4)
    if state == "no-image":
        # The pointer on the disabled Calibrate: its hint says why.
        QApplication.sendEvent(dialog.auto_run_btn, QEvent(QEvent.Enter))
    elif state == "export-failed":
        dialog._on_export_failed(
            "PermissionError: [Errno 13] Permission denied: '/Volumes/Felt/kalibrering.png'"
        )
        _settle(0.1)
        dialog.hint_bar._details_btn.click()
    _settle(0.3)
    return dialog


def _new_objective(context: ReviewContext) -> QWidget:
    from ui.calibration_dialog import NewObjectiveDialog

    _fresh(context)
    dialog = NewObjectiveDialog()
    dialog.show()
    _settle(0.2)
    dialog.objective_name_input.setText("Plan achro")
    dialog.ok_btn.click()  # no magnification yet
    _settle(0.2)
    return dialog


def register_calibration_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"calibration.no-image{suffix}",
                group="calibration",
                title=f"Kalibrer objektiv with no image (nb{', dark' if theme == 'dark' else ''})",
                description="Calibrate, Export and Add measurement wait for an image; the hint says why (stage 10c).",
                build=lambda context: _calibration(context, "no-image"),
                viewport=(1200, 860),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="calibration.export-failed",
            group="calibration",
            title="Kalibrer objektiv: an export failure as a notice (nb)",
            description="The error in the bottom bar with Details open, instead of a message box.",
            build=lambda context: _calibration(context, "export-failed"),
            viewport=(1200, 860),
            locale="nb_NO",
        )
    )
    registry.register(
        ReviewScenario(
            id="calibration.export-failed-de",
            group="calibration",
            title="Kalibrer objektiv: an export failure (de)",
            description="German strings; the dialog's minimum size is about 1300 × 960.",
            build=lambda context: _calibration(context, "export-failed"),
            viewport=(1200, 860),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="calibration.new-objective-missing",
            group="calibration",
            title="New objective: what is missing, said under the form (nb)",
            description="Create without a magnification: the message under the form, the cursor in the field.",
            build=_new_objective,
            viewport=(460, 340),
            locale="nb_NO",
        )
    )
