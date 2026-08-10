"""Generic Qt capture and autonomous-development manifest plumbing."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from PySide6.QtWidgets import QApplication, QWidget

from .context import ReviewContext
from .registry import ReviewScenario


def _target_for(output_dir: Path, scenario: ReviewScenario) -> Path:
    target = (output_dir / scenario.filename).resolve()
    if target.parent != output_dir:
        raise RuntimeError(f"{scenario.id}: screenshot path escapes output directory")
    return target


def _capture(
    scenario: ReviewScenario,
    context: ReviewContext,
    output_dir: Path,
) -> dict[str, str]:
    try:
        context.set_theme(scenario.theme)
        with context.locale(scenario.locale):
            try:
                widget = scenario.build(context)
            except Exception as error:
                raise RuntimeError(
                    f"{scenario.id}: failed to construct widget: {error}"
                ) from error
            if not isinstance(widget, QWidget):
                raise RuntimeError(
                    f"{scenario.id}: builder returned {type(widget).__name__}, not QWidget"
                )

            width, height = scenario.viewport
            widget.resize(width, height)
            widget.show()
            context.app.processEvents()
            context.app.sendPostedEvents()
            context.app.processEvents()

            target = _target_for(output_dir, scenario)
            try:
                if not widget.grab().save(str(target), "PNG"):
                    raise RuntimeError(f"could not save screenshot: {target}")
            except Exception as error:
                raise RuntimeError(f"{scenario.id}: failed to capture widget: {error}") from error
            finally:
                widget.close()
                widget.deleteLater()
                context.app.processEvents()
    except RuntimeError:
        raise
    except Exception as error:
        raise RuntimeError(f"{scenario.id}: scenario setup failed: {error}") from error

    print(f"saved {target}")
    return {
        "id": scenario.id,
        "path": scenario.filename,
        "title": scenario.title,
        "description": scenario.description,
        "viewport": f"{scenario.viewport[0]}x{scenario.viewport[1]}",
    }


def render_scenarios(
    scenarios: Iterable[ReviewScenario],
    output_dir: Path,
) -> dict:
    """Render a selected sequence atomically at the manifest level."""
    selected = tuple(scenarios)
    if not selected:
        raise ValueError("no UI review scenarios selected")

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    # A failed rerender must not leave an older manifest looking authoritative.
    manifest_path.unlink(missing_ok=True)
    app = QApplication.instance() or QApplication([])
    app.setStyle("Fusion")

    screens: list[dict[str, str]] = []
    with ReviewContext(app) as context:
        for scenario in selected:
            screens.append(_capture(scenario, context, output_dir))

    manifest = {"version": 1, "screens": screens}
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest
