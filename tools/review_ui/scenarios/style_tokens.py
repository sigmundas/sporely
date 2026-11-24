"""Review scenarios for the shared style tokens (UI redesign stage P3).

Shows every ISO 8578 objective ring colour as it appears on gallery thumbnail
badges, plus objective pills with a black and a white ring selected, in both
themes. Thumbnails alternate a bright and a dark frame so the outline on the
black and white rings can be judged against both.
"""
from __future__ import annotations

import numpy as np
from PIL import Image
from PySide6.QtWidgets import QHBoxLayout, QLabel, QVBoxLayout, QWidget

from ..context import ReviewContext
from ..live_lab_harness import synthetic_frame
from ..registry import ReviewScenario, ScenarioRegistry

# One magnification per ISO 8578 band, plus a macro profile and an unknown one,
# each with the frame it sits on. The black (1x) and white (100x) rings appear
# on a bright and on a dark frame, so their outline can be judged on both.
_RINGS = (
    ("1x", {"magnification": 1.0}, False),
    ("1x", {"magnification": 1.0}, True),
    ("2x", {"magnification": 2.0}, False),
    ("2.5x", {"magnification": 2.5}, True),
    ("4x", {"magnification": 4.0}, False),
    ("6.3x", {"magnification": 6.3}, True),
    ("10x", {"magnification": 10.0}, False),
    ("20x", {"magnification": 20.0}, True),
    ("25x", {"magnification": 25.0}, False),
    ("40x", {"magnification": 40.0}, True),
    ("63x", {"magnification": 63.0}, False),
    ("100x", {"magnification": 100.0}, False),
    ("100x", {"magnification": 100.0}, True),
    ("1:2", {"optics_type": "macro", "magnification": 2.0}, False),
    ("?", {"name": "Plan achro"}, True),
)
_SELECTOR_RINGS = tuple(
    (f"{mag:g}x", {"magnification": float(mag)}) for mag in (1, 2, 2.5, 4, 6.3, 10, 20, 25, 40, 63, 100)
)


def _frame_path(context: ReviewContext, index: int, dark: bool):
    root = context.temporary_root / "style-tokens"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"ring-{index:02d}{'-dark' if dark else ''}.jpg"
    if not path.exists():
        frame = synthetic_frame(320, 240, seed=11 + index).astype(np.float32) / 255.0
        if dark:
            frame = 1.0 - frame * 0.85  # dark-field-like: dark background, light shapes
        rgb = np.clip(frame[..., :3] * 255.0, 0, 255).astype(np.uint8)
        Image.fromarray(rgb).save(path, quality=92)
    return path


def _gallery(context: ReviewContext, parent: QWidget, *, compact: bool) -> QWidget:
    from ui.adaptive_choice_selector import objective_color
    from ui.image_gallery_widget import ImageGalleryWidget

    gallery = ImageGalleryWidget(
        "Objective rings (compact)" if compact else "Objective rings",
        parent,
        thumbnail_size=84 if compact else 140,
    )
    gallery.set_compact_overlay(compact)
    items = []
    for index, (label, objective, dark) in enumerate(_RINGS):
        path = _frame_path(context, index, dark)
        items.append(
            {
                "id": f"ring-{index}",
                "filepath": str(path),
                "preview_path": str(path),
                "image_number": str(index + 1),
                "microscope_tag_text": f"{label} BF" if compact else label,
                "microscope_tag_color": objective_color(objective, label),
            }
        )
    gallery.set_items(items)
    return gallery


def _viewer(context: ReviewContext, parent: QWidget, label: str, objective: dict, dark: bool) -> QWidget:
    from PySide6.QtGui import QPixmap
    from ui.adaptive_choice_selector import objective_color
    from ui.zoomable_image_widget import ZoomableImageLabel

    viewer = ZoomableImageLabel(parent)
    viewer.setMinimumSize(300, 170)
    viewer.set_image(QPixmap(str(_frame_path(context, 90 + int(dark), dark))))
    viewer.set_objective_text(label)
    viewer.set_objective_color(objective_color(objective, label))
    return viewer


def _objective_rings(context: ReviewContext) -> QWidget:
    from ui.adaptive_choice_selector import AdaptiveChoiceSelector, objective_color

    page = QWidget(context.host)
    layout = QVBoxLayout(page)
    layout.setContentsMargins(16, 16, 16, 16)
    layout.setSpacing(12)
    layout.addWidget(_gallery(context, page, compact=False))
    layout.addWidget(_gallery(context, page, compact=True))

    viewers = QHBoxLayout()
    for label, objective, dark in (
        ("100x", {"magnification": 100.0}, False),
        ("100x", {"magnification": 100.0}, True),
        ("1x", {"magnification": 1.0}, False),
        ("1x", {"magnification": 1.0}, True),
    ):
        viewers.addWidget(_viewer(context, page, label, objective, dark))
    layout.addLayout(viewers)

    for selected in ("1x", "100x"):
        row = QHBoxLayout()
        row.addWidget(QLabel(selected, page))
        selector = AdaptiveChoiceSelector(page, compact=True)
        for label, objective in _SELECTOR_RINGS:
            selector.addItem(label, label, color=objective_color(objective, label))
        selector.set_selected_value(selected)
        row.addWidget(selector, 1)
        layout.addLayout(row)
    return page


def register_style_token_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"style-tokens.objective-rings{suffix}",
                group="style-tokens",
                title=f"Objective ring colours (ISO 8578){', dark' if theme == 'dark' else ''}",
                description=(
                    "Gallery badges (normal and compact) for every ISO 8578 ring band, a macro "
                    "profile and an unknown objective; the black (1x) and white (100x) rings on "
                    "bright and dark frames, in the image viewer too; objective pills with the "
                    "black and white rings selected."
                ),
                build=_objective_rings,
                viewport=(2300, 720),
                theme=theme,
            )
        )
