"""Shared RAW processing controls used by Prepare Images and Live Lab."""
from __future__ import annotations

import math
from dataclasses import replace

from PySide6.QtCore import Signal, QSignalBlocker, Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSlider,
    QFormLayout,
    QSizePolicy,
    QStyle,
    QStyleOptionSlider,
    QVBoxLayout,
    QWidget,
)

from utils.raw_render import RawRenderSettings, apply_auto_level_bounds_to_settings

from .segmented_selector import SegmentedSelector


_EV_SLIDER_SCALE = 1000
_EV_SLIDER_MAX = 2000
_TRAILING_WIDTH = 48


class MixedStateSlider(QSlider):
    """QSlider that can paint two ghost handles at a min/max range.

    Used to indicate a multi-selection whose values disagree. While the
    slider is in mixed mode the real handle is hidden and the two ghost
    thumbs show the spread. Any mouse press exits mixed mode so the click
    lands on a real value.
    """

    def __init__(self, orientation, parent=None) -> None:
        super().__init__(orientation, parent)
        self._mixed_min: int | None = None
        self._mixed_max: int | None = None

    def is_mixed(self) -> bool:
        return self._mixed_min is not None and self._mixed_max is not None

    def set_mixed_range(self, min_value: int | None, max_value: int | None) -> None:
        if min_value is None or max_value is None or int(min_value) == int(max_value):
            if self.is_mixed():
                self._mixed_min = None
                self._mixed_max = None
                self.update()
            return
        lo = int(min(min_value, max_value))
        hi = int(max(min_value, max_value))
        self._mixed_min = lo
        self._mixed_max = hi
        self.update()

    def clear_mixed_range(self) -> None:
        if self.is_mixed():
            self._mixed_min = None
            self._mixed_max = None
            self.update()

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if self.is_mixed():
            self.clear_mixed_range()
        super().mousePressEvent(event)

    def keyPressEvent(self, event) -> None:  # type: ignore[override]
        if self.is_mixed():
            self.clear_mixed_range()
        super().keyPressEvent(event)

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        if self.is_mixed():
            self.clear_mixed_range()
        super().wheelEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        if not self.is_mixed():
            super().paintEvent(event)
            return
        opt = QStyleOptionSlider()
        self.initStyleOption(opt)
        # Paint groove without an active handle by removing the SC_SliderHandle
        # sub-control before painting.
        painter = QPainter(self)
        opt.subControls = QStyle.SC_SliderGroove | QStyle.SC_SliderTickmarks
        self.style().drawComplexControl(QStyle.CC_Slider, opt, painter, self)
        groove_rect = self.style().subControlRect(
            QStyle.CC_Slider, opt, QStyle.SC_SliderGroove, self
        )
        handle_rect = self.style().subControlRect(
            QStyle.CC_Slider, opt, QStyle.SC_SliderHandle, self
        )
        span = groove_rect.width() - handle_rect.width()
        if span <= 0:
            painter.end()
            return
        value_span = max(1, self.maximum() - self.minimum())
        def _pos_for(value: int) -> int:
            return QStyle.sliderPositionFromValue(
                self.minimum(), self.maximum(), int(value), span, opt.upsideDown
            )
        lo_x = groove_rect.x() + _pos_for(int(self._mixed_min or 0))
        hi_x = groove_rect.x() + _pos_for(int(self._mixed_max or 0))
        # Range fill between the two ghost handles.
        palette = self.palette()
        accent = palette.color(palette.ColorRole.Highlight)
        fill_color = QColor(accent)
        fill_color.setAlpha(60)
        painter.setPen(Qt.NoPen)
        painter.setBrush(fill_color)
        fill_top = groove_rect.center().y() - 2
        painter.drawRect(min(lo_x, hi_x), fill_top, abs(hi_x - lo_x) + handle_rect.width(), 4)
        # Two ghost handle circles.
        ghost_color = QColor(accent)
        ghost_color.setAlpha(160)
        border_color = QColor(accent).darker(140)
        painter.setBrush(ghost_color)
        painter.setPen(QPen(border_color, 1))
        diameter = handle_rect.height() - 4
        if diameter < 8:
            diameter = 8
        cy = groove_rect.center().y() - diameter // 2
        for x in (lo_x, hi_x):
            painter.drawEllipse(x, cy, diameter, diameter)
        painter.end()


class AutoLevelsToggle(QWidget):
    """Segmented On/Off pill for the Auto levels control.

    Exposes a small subset of the QCheckBox API (isChecked / setChecked /
    toggled / setEnabled) so callers that used to hold a checkable
    QPushButton keep working. `setProperty("mixed", True)` visually deselects
    both buttons to indicate a disagreeing multi-selection; the property is
    still readable via `.property("mixed")` for `mixed_fields()`.
    """

    toggled = Signal(bool)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._checked = False
        self._selector = SegmentedSelector(self, compact=True, button_height=28, container_height=32)
        self._on_button = self._selector.add_option(self.tr("On"), True)
        self._off_button = self._selector.add_option(self.tr("Off"), False, checked=True)
        self._selector.selectionChanged.connect(self._on_selection_changed)

        heading = QLabel(self.tr("Auto levels"), self)
        heading.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)

        row_layout = QVBoxLayout(self)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(2)
        row_layout.addWidget(heading)
        row_layout.addWidget(self._selector, 0, Qt.AlignLeft)

    def isChecked(self) -> bool:  # noqa: N802 (Qt API name)
        return bool(self._checked)

    def setChecked(self, checked: bool) -> None:  # noqa: N802 (Qt API name)
        target = bool(checked)
        if target == self._checked:
            self._sync_selector(target)
            return
        self._checked = target
        self._sync_selector(target)
        self.setProperty("mixed", False)
        self.toggled.emit(target)

    def text(self) -> str:
        return self.tr("Auto levels")

    def _sync_selector(self, checked: bool) -> None:
        # Programmatically flip the underlying selection without re-emitting
        # selectionChanged (SegmentedSelector only emits on user click, but
        # the button toggle can still trigger unwanted focus behavior — a
        # blocker keeps things quiet).
        button = self._on_button if checked else self._off_button
        with QSignalBlocker(self._selector):
            button.setChecked(True)

    def _on_selection_changed(self, value) -> None:
        target = bool(value)
        if target == self._checked:
            return
        self._checked = target
        self.setProperty("mixed", False)
        self.toggled.emit(target)

    def setProperty(self, name, value) -> bool:  # noqa: N802 (Qt API name)
        # Reflect "mixed" state visually by deselecting both buttons via a
        # temporary non-exclusive group. QButtonGroup requires exclusive=False
        # to allow zero-checked state.
        if name == "mixed":
            mixed = bool(value)
            group = self._selector.button_group
            if mixed:
                group.setExclusive(False)
                with QSignalBlocker(self._selector):
                    self._on_button.setChecked(False)
                    self._off_button.setChecked(False)
            else:
                group.setExclusive(True)
                self._sync_selector(self._checked)
        return super().setProperty(name, value)


class RawProcessingControls(QWidget):
    """Compact RAW controls backed by :class:`RawRenderSettings`."""

    settingsChanged = Signal(object)
    pickWhiteBalanceToggled = Signal(bool)

    def __init__(
        self,
        parent=None,
        *,
        show_shadow_lift: bool = False,
        show_tone_controls_when_disabled: bool = False,
    ) -> None:
        super().__init__(parent)
        self._settings = RawRenderSettings.default()
        self._auto_level_settings: RawRenderSettings | None = None
        self._loading = False
        self._slider_change_pending = False
        self._mixed_wb_mode = False
        self._mixed_auto_levels_method = False
        self._mixed_auto_levels_clipping = False
        self._multi_selection_mode = False
        self._pick_context_enabled = True
        self._show_shadow_lift = bool(show_shadow_lift)
        self._show_tone_controls_when_disabled = bool(show_tone_controls_when_disabled)
        # Percentile cutoffs pushed in from Preferences. Kept as fractions
        # (0.0005 == 0.05%) so we can hand them straight to the pipeline as
        # black_percentile / (1 - white_percentile).
        self._dark_cutoff: float = 0.0
        self._bright_cutoff: float = 0.0

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self.white_balance_selector = SegmentedSelector(self, compact=True, button_height=32, container_height=40)
        self.white_balance_selector.add_option(self.tr("Camera WB"), "camera", checked=True)
        self.white_balance_selector.add_option(self.tr("Auto WB"), "auto")
        self.white_balance_selector.add_option(self.tr("Custom WB"), "custom")
        self.white_balance_selector.selectionChanged.connect(self._on_control_changed)

        white_balance_row = QWidget(self)
        white_balance_row_layout = QHBoxLayout(white_balance_row)
        white_balance_row_layout.setContentsMargins(0, 4, 0, 4)
        white_balance_row_layout.setSpacing(8)
        white_balance_row_layout.addWidget(self.white_balance_selector, 0, Qt.AlignLeft | Qt.AlignVCenter)

        self.pick_button = QPushButton(self.tr("Pick"), white_balance_row)
        self.pick_button.setCheckable(True)
        self.pick_button.setMinimumHeight(32)
        self.pick_button.toggled.connect(self._on_pick_toggled)
        white_balance_row_layout.addWidget(self.pick_button, 0, Qt.AlignLeft | Qt.AlignVCenter)
        white_balance_row_layout.addStretch(1)
        white_balance_row.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        white_balance_row.setMinimumHeight(44)
        layout.addWidget(white_balance_row)

        self.auto_levels_toggle = AutoLevelsToggle(self)
        self.auto_levels_toggle.toggled.connect(self._on_control_changed)
        # Historical aliases used across the codebase and tests.
        self.auto_levels_btn = self.auto_levels_toggle
        self.auto_levels_checkbox = self.auto_levels_toggle

        self.auto_levels_method_selector = SegmentedSelector(
            self, compact=True, button_height=28, container_height=32
        )
        self.auto_levels_method_selector.add_option(self.tr("A"), "a", checked=True)
        self.auto_levels_method_selector.add_option(self.tr("B"), "b")
        self.auto_levels_method_selector.selectionChanged.connect(self._on_control_changed)
        method_heading = QLabel(self.tr("Method"), self)
        method_heading.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        method_control = QWidget(self)
        method_layout = QVBoxLayout(method_control)
        method_layout.setContentsMargins(0, 0, 0, 0)
        method_layout.setSpacing(2)
        method_layout.addWidget(method_heading)
        method_layout.addWidget(self.auto_levels_method_selector, 0, Qt.AlignLeft)

        self.auto_levels_clipping_selector = SegmentedSelector(
            self, compact=True, button_height=28, container_height=32
        )
        self.auto_levels_clipping_selector.add_option(self.tr("On"), True, checked=True)
        self.auto_levels_clipping_selector.add_option(self.tr("Off"), False)
        self.auto_levels_clipping_selector.selectionChanged.connect(self._on_control_changed)
        clipping_heading = QLabel(self.tr("Clipping"), self)
        clipping_heading.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        clipping_control = QWidget(self)
        clipping_layout = QVBoxLayout(clipping_control)
        clipping_layout.setContentsMargins(0, 0, 0, 0)
        clipping_layout.setSpacing(2)
        clipping_layout.addWidget(clipping_heading)
        clipping_layout.addWidget(self.auto_levels_clipping_selector, 0, Qt.AlignLeft)

        auto_levels_row = QWidget(self)
        auto_levels_layout = QHBoxLayout(auto_levels_row)
        auto_levels_layout.setContentsMargins(0, 0, 0, 0)
        auto_levels_layout.setSpacing(12)
        auto_levels_layout.addWidget(self.auto_levels_toggle, 0, Qt.AlignTop)
        auto_levels_layout.addWidget(method_control, 0, Qt.AlignTop)
        auto_levels_layout.addWidget(clipping_control, 0, Qt.AlignTop)
        auto_levels_layout.addStretch(1)
        layout.addWidget(auto_levels_row)

        slider_form = QFormLayout()
        slider_form.setContentsMargins(0, 0, 0, 0)
        slider_form.setHorizontalSpacing(8)
        slider_form.setVerticalSpacing(8)
        slider_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        def _build_slider_row(slider: QSlider, trailing: QWidget) -> QWidget:
            row = QWidget(self)
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(8)
            row_layout.addWidget(slider, 1)
            row_layout.addWidget(trailing, 0)
            return row

        self.light_label = QLabel(self.tr("Light:"), self)
        self.light_slider = MixedStateSlider(Qt.Horizontal, self)
        self.light_slider.setRange(0, _EV_SLIDER_MAX)
        self.light_slider.setSingleStep(1)
        self.light_slider.setPageStep(25)
        self.light_slider.setMaximumWidth(240)
        self.light_slider.valueChanged.connect(self._on_control_changed)
        self.light_slider.sliderReleased.connect(self._on_slider_released)
        light_trailing = QWidget(self)
        light_trailing.setFixedWidth(_TRAILING_WIDTH)
        light_trailing.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        self.light_value_label = QLabel("", self)
        self.light_value_label.setVisible(False)
        self.light_row = _build_slider_row(self.light_slider, light_trailing)
        slider_form.addRow(self.light_label, self.light_row)
        self.exposure_row = self.light_row
        self.exposure_slider = self.light_slider
        self.exposure_value_label = self.light_value_label

        self.dark_label = QLabel(self.tr("Dark:"), self)
        self.dark_slider = MixedStateSlider(Qt.Horizontal, self)
        self.dark_slider.setRange(0, _EV_SLIDER_MAX)
        self.dark_slider.setSingleStep(1)
        self.dark_slider.setPageStep(25)
        self.dark_slider.setMaximumWidth(240)
        self.dark_slider.valueChanged.connect(self._on_control_changed)
        self.dark_slider.sliderReleased.connect(self._on_slider_released)
        dark_trailing = QWidget(self)
        dark_trailing.setFixedWidth(_TRAILING_WIDTH)
        dark_trailing.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        self.dark_value_label = QLabel("", self)
        self.dark_value_label.setVisible(False)
        self.dark_row = _build_slider_row(self.dark_slider, dark_trailing)
        slider_form.addRow(self.dark_label, self.dark_row)
        self.dark_exposure_slider = self.dark_slider
        self.dark_exposure_value_label = self.dark_value_label

        self.contrast_label = QLabel(self.tr("Contrast:"), self)
        self.contrast_slider = MixedStateSlider(Qt.Horizontal, self)
        self.contrast_slider.setRange(-100, 100)
        self.contrast_slider.setSingleStep(1)
        self.contrast_slider.setPageStep(5)
        self.contrast_slider.setMaximumWidth(240)
        self.contrast_slider.valueChanged.connect(self._on_control_changed)
        self.contrast_slider.sliderReleased.connect(self._on_slider_released)
        self.contrast_value_label = QLabel("", self)
        self.contrast_value_label.setFixedWidth(_TRAILING_WIDTH)
        self.contrast_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.contrast_row = _build_slider_row(self.contrast_slider, self.contrast_value_label)
        slider_form.addRow(self.contrast_label, self.contrast_row)

        self.tone_curve_checkbox = QCheckBox(self.tr("Tone curve"), self)
        self.tone_curve_checkbox.toggled.connect(self._on_control_changed)
        slider_form.addRow(QLabel("", self), self.tone_curve_checkbox)

        self.curve_strength_label = QLabel(self.tr("Strength:"), self)
        self.curve_strength_slider = MixedStateSlider(Qt.Horizontal, self)
        self.curve_strength_slider.setRange(0, 100)
        self.curve_strength_slider.setSingleStep(1)
        self.curve_strength_slider.setPageStep(5)
        self.curve_strength_slider.valueChanged.connect(self._on_control_changed)
        self.curve_strength_slider.sliderReleased.connect(self._on_slider_released)
        self.curve_strength_value_label = QLabel("", self)
        self.curve_strength_value_label.setFixedWidth(_TRAILING_WIDTH)
        self.curve_strength_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.curve_strength_row = _build_slider_row(self.curve_strength_slider, self.curve_strength_value_label)
        slider_form.addRow(self.curve_strength_label, self.curve_strength_row)
        self.strength_label = self.curve_strength_label
        self.strength_slider = self.curve_strength_slider
        self.strength_value_label = self.curve_strength_value_label

        self.curve_midpoint_label = QLabel(self.tr("Midpoint:"), self)
        self.curve_midpoint_slider = MixedStateSlider(Qt.Horizontal, self)
        self.curve_midpoint_slider.setRange(0, 100)
        self.curve_midpoint_slider.setSingleStep(1)
        self.curve_midpoint_slider.setPageStep(5)
        self.curve_midpoint_slider.valueChanged.connect(self._on_control_changed)
        self.curve_midpoint_slider.sliderReleased.connect(self._on_slider_released)
        self.curve_midpoint_value_label = QLabel("", self)
        self.curve_midpoint_value_label.setFixedWidth(_TRAILING_WIDTH)
        self.curve_midpoint_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.curve_midpoint_row = _build_slider_row(self.curve_midpoint_slider, self.curve_midpoint_value_label)
        slider_form.addRow(self.curve_midpoint_label, self.curve_midpoint_row)
        self.midpoint_slider = self.curve_midpoint_slider
        self.midpoint_value_label = self.curve_midpoint_value_label

        self.shadows_label = QLabel(self.tr("Shadows:"), self)
        self.shadows_slider = MixedStateSlider(Qt.Horizontal, self)
        self.shadows_slider.setRange(-100, 100)
        self.shadows_slider.setSingleStep(1)
        self.shadows_slider.setPageStep(5)
        self.shadows_slider.valueChanged.connect(self._on_control_changed)
        self.shadows_slider.sliderReleased.connect(self._on_slider_released)
        self.shadows_value_label = QLabel("", self)
        self.shadows_value_label.setFixedWidth(_TRAILING_WIDTH)
        self.shadows_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.shadows_row = _build_slider_row(self.shadows_slider, self.shadows_value_label)
        slider_form.addRow(self.shadows_label, self.shadows_row)
        self.shadow_lift_label = self.shadows_label
        self.shadow_lift_row = self.shadows_row
        self.shadow_lift_slider = self.shadows_slider
        self.shadow_lift_value_label = self.shadows_value_label

        self.highlights_label = QLabel(self.tr("Highlights:"), self)
        self.highlights_slider = MixedStateSlider(Qt.Horizontal, self)
        self.highlights_slider.setRange(-100, 100)
        self.highlights_slider.setSingleStep(1)
        self.highlights_slider.setPageStep(5)
        self.highlights_slider.valueChanged.connect(self._on_control_changed)
        self.highlights_slider.sliderReleased.connect(self._on_slider_released)
        self.highlights_value_label = QLabel("", self)
        self.highlights_value_label.setFixedWidth(_TRAILING_WIDTH)
        self.highlights_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.highlights_row = _build_slider_row(self.highlights_slider, self.highlights_value_label)
        slider_form.addRow(self.highlights_label, self.highlights_row)

        layout.addLayout(slider_form)

        self._sync_controls_from_settings(self._settings)

    def settings(self) -> RawRenderSettings:
        self._settings = self._settings_from_controls()
        return self._settings

    def set_settings(self, settings: RawRenderSettings | dict | None) -> None:
        self._clear_mixed_state()
        self._settings = RawRenderSettings.from_dict(settings)
        if bool(self._settings.auto_levels) and self._auto_level_settings is None:
            self._auto_level_settings = RawRenderSettings.from_dict(self._settings)
        self._sync_controls_from_settings(self._settings)
        if bool(self.auto_levels_checkbox.isChecked()) and self._auto_level_settings is not None:
            self._apply_auto_level_settings()

    def set_mixed_settings(self, settings_list) -> None:
        """Populate controls with ghost min/max handles for fields that
        disagree across the supplied per-image settings.

        Fields that agree across every element are set to that common
        value using the normal path; disagreeing fields put their slider
        into mixed mode (dual ghost handles) or, for boolean/segmented
        controls, into a no-selection / tri-state visual.
        """
        resolved = [RawRenderSettings.from_dict(s) for s in (settings_list or [])]
        if not resolved:
            self.set_settings(None)
            return
        if len(resolved) == 1:
            self.set_settings(resolved[0])
            return
        self._loading = True
        self._slider_change_pending = False
        try:
            base = resolved[0]
            self._settings = base
            slider_specs = list(self._mixed_slider_specs())
            for slider, extractor in slider_specs:
                values = [int(round(extractor(s))) for s in resolved]
                lo, hi = min(values), max(values)
                with QSignalBlocker(slider):
                    if lo == hi:
                        slider.clear_mixed_range()
                        slider.setValue(lo)
                    else:
                        slider.setValue(lo)
                        slider.set_mixed_range(lo, hi)
            wb_modes = {str(s.white_balance_mode or "camera").strip().lower() or "camera" for s in resolved}
            with QSignalBlocker(self.white_balance_selector):
                if len(wb_modes) == 1:
                    self._set_white_balance_visual_selection(next(iter(wb_modes)))
                else:
                    self._set_white_balance_visual_selection(None)
            self._mixed_wb_mode = len(wb_modes) > 1
            auto_levels_values = {bool(s.auto_levels) for s in resolved}
            with QSignalBlocker(self.auto_levels_checkbox):
                if len(auto_levels_values) == 1:
                    self.auto_levels_checkbox.setChecked(next(iter(auto_levels_values)))
                    self.auto_levels_checkbox.setProperty("mixed", False)
                else:
                    self.auto_levels_checkbox.setChecked(False)
                    self.auto_levels_checkbox.setProperty("mixed", True)
            method_values = {str(s.auto_levels_method) for s in resolved}
            with QSignalBlocker(self.auto_levels_method_selector):
                if len(method_values) == 1:
                    self.auto_levels_method_selector.set_selected_value(next(iter(method_values)))
                    self._mixed_auto_levels_method = False
                else:
                    group = self.auto_levels_method_selector.button_group
                    group.setExclusive(False)
                    for button in self.auto_levels_method_selector.buttons():
                        button.setChecked(False)
                    group.setExclusive(True)
                    self._mixed_auto_levels_method = True
            clipping_values = {bool(s.auto_levels_clipping) for s in resolved}
            with QSignalBlocker(self.auto_levels_clipping_selector):
                if len(clipping_values) == 1:
                    self.auto_levels_clipping_selector.set_selected_value(next(iter(clipping_values)))
                    self._mixed_auto_levels_clipping = False
                else:
                    group = self.auto_levels_clipping_selector.button_group
                    group.setExclusive(False)
                    for button in self.auto_levels_clipping_selector.buttons():
                        button.setChecked(False)
                    group.setExclusive(True)
                    self._mixed_auto_levels_clipping = True
            tone_enabled = {bool(s.tone_curve_enabled) for s in resolved}
            with QSignalBlocker(self.tone_curve_checkbox):
                if len(tone_enabled) == 1:
                    self.tone_curve_checkbox.setChecked(next(iter(tone_enabled)))
                    self.tone_curve_checkbox.setProperty("mixed", False)
                else:
                    self.tone_curve_checkbox.setChecked(False)
                    self.tone_curve_checkbox.setProperty("mixed", True)
            self._refresh_mixed_value_labels()
            self._set_tone_controls_enabled(bool(self.tone_curve_checkbox.isChecked()) or len(tone_enabled) > 1)
            self._refresh_pick_button_text()
        finally:
            self._loading = False

    def mixed_fields(self) -> set:
        """Return the set of field names still in mixed state."""
        mixed: set = set()
        for name, slider, _ in self._named_mixed_slider_specs():
            if slider.is_mixed():
                mixed.add(name)
        if getattr(self, "_mixed_wb_mode", False):
            mixed.add("white_balance_mode")
        if self.auto_levels_checkbox.property("mixed"):
            mixed.add("auto_levels")
        if self._mixed_auto_levels_method:
            mixed.add("auto_levels_method")
        if self._mixed_auto_levels_clipping:
            mixed.add("auto_levels_clipping")
            mixed.add("black_percentile")
            mixed.add("white_percentile")
        if self.tone_curve_checkbox.property("mixed"):
            mixed.add("tone_curve_enabled")
        return mixed

    def _clear_mixed_state(self) -> None:
        for _, slider, _ in self._named_mixed_slider_specs():
            slider.clear_mixed_range()
        if self._mixed_wb_mode:
            mode = str(self._settings.white_balance_mode or "camera").strip().lower() or "camera"
            with QSignalBlocker(self.white_balance_selector):
                self._set_white_balance_visual_selection(mode)
        self._mixed_wb_mode = False
        self._mixed_auto_levels_method = False
        self._mixed_auto_levels_clipping = False
        self.auto_levels_checkbox.setProperty("mixed", False)
        self.tone_curve_checkbox.setProperty("mixed", False)

    def _set_white_balance_visual_selection(self, value: str | None) -> None:
        """Select one WB pill, or leave all pills unselected for mixed values."""
        group = self.white_balance_selector.button_group
        group.setExclusive(False)
        for button in self.white_balance_selector.buttons():
            button.setChecked(False)
        group.setExclusive(True)
        if value is not None:
            if not self.white_balance_selector.set_selected_value(value):
                self.white_balance_selector.set_selected_value("camera")

    def _mixed_slider_specs(self):
        for _, slider, extractor in self._named_mixed_slider_specs():
            yield slider, extractor

    def _named_mixed_slider_specs(self):
        return (
            ("light_ev", self.light_slider, lambda s: float(s.light_ev) * _EV_SLIDER_SCALE),
            ("dark_ev", self.dark_slider, lambda s: abs(float(s.dark_ev)) * _EV_SLIDER_SCALE),
            ("tone_contrast", self.contrast_slider, lambda s: float(s.tone_contrast) * 100.0),
            ("tone_curve_strength", self.curve_strength_slider, lambda s: float(s.tone_curve_strength) * 100.0),
            ("tone_curve_midpoint", self.curve_midpoint_slider, lambda s: float(s.tone_curve_midpoint) * 100.0),
            ("tone_shadows", self.shadows_slider, lambda s: float(s.tone_shadows) * 100.0),
            ("tone_highlights", self.highlights_slider, lambda s: float(s.tone_highlights) * 100.0),
        )

    def _refresh_mixed_value_labels(self) -> None:
        # Blank the trailing value label for sliders still in mixed mode so
        # the number doesn't lie about which image the user is looking at.
        def _text(slider, formatter):
            return "" if slider.is_mixed() else formatter(slider.value())
        self.light_value_label.setText(_text(self.light_slider, self._ev_value_text))
        self.dark_value_label.setText(_text(self.dark_slider, self._dark_ev_value_text))
        self.curve_strength_value_label.setText(
            "" if self.curve_strength_slider.is_mixed() else f"{float(self.curve_strength_slider.value()) / 100.0:.2f}"
        )
        self.curve_midpoint_value_label.setText(
            "" if self.curve_midpoint_slider.is_mixed() else f"{float(self.curve_midpoint_slider.value()) / 100.0:.2f}"
        )
        self.contrast_value_label.setText(_text(self.contrast_slider, self._signed_percent_value_text))
        self.shadows_value_label.setText(_text(self.shadows_slider, self._signed_percent_value_text))
        self.highlights_value_label.setText(_text(self.highlights_slider, self._signed_percent_value_text))

    def set_auto_level_settings(self, settings: RawRenderSettings | dict | None) -> None:
        if settings is None:
            self._auto_level_settings = None
            return
        resolved = RawRenderSettings.from_dict(settings)
        self._auto_level_settings = resolved
        if self._loading or not self.auto_levels_checkbox.isChecked():
            return
        self._apply_auto_level_settings()

    def set_auto_level_cutoffs(self, dark_cutoff: float, bright_cutoff: float) -> None:
        """Push the Preferences dark/bright cutoff fractions into the widget.

        The values become the ``black_percentile`` / ``1 - white_percentile``
        used by the auto-levels stage. Called by containers that own the
        widget so the shared pill (which knows nothing about SettingsDB)
        renders with the user's configured cutoffs.
        """
        try:
            dark = float(dark_cutoff)
        except Exception:
            dark = 0.0
        try:
            bright = float(bright_cutoff)
        except Exception:
            bright = 0.0
        self._dark_cutoff = max(0.0, min(1.0, dark))
        self._bright_cutoff = max(0.0, min(1.0, bright))
        # Refresh the cached settings snapshot so callers reading
        # ``settings()`` back before touching any control see the new
        # percentiles.
        self._settings = replace(
            RawRenderSettings.from_dict(self._settings),
            black_percentile=float(self._dark_cutoff) if self._settings.auto_levels_clipping else 0.0,
            white_percentile=float(1.0 - self._bright_cutoff) if self._settings.auto_levels_clipping else 1.0,
        )

    def sync_from_live_bounds(self, black_level: float | None, white_level: float | None) -> None:
        """Reflect the bounds the pipeline actually used on the Light/Dark sliders.

        Called with the auto-levels stage's per-render ``debug.black_level`` /
        ``debug.white_level``. Values get converted to the same ``light_ev`` /
        ``dark_ev`` scale that ``apply_light_dark_levels`` uses, so toggling
        Auto Levels off freezes the image at these bounds (the pipeline then
        reads the sliders instead of recomputing).
        """
        if self._loading:
            return
        if black_level is None or white_level is None:
            return
        try:
            black_point = float(black_level)
            white_point = float(white_level)
        except (TypeError, ValueError):
            return
        if not (math.isfinite(black_point) and math.isfinite(white_point)):
            return
        if white_point <= black_point:
            return
        black_point = max(0.0, min(1.0, black_point))
        white_point = max(1e-6, min(1.0, white_point))
        light_ev = float(max(0.0, min(2.0, -math.log2(max(1e-6, white_point)))))
        dark_ev = float(max(-2.0, min(0.0, math.log2(max(1e-6, 1.0 - black_point)))))
        base = RawRenderSettings.from_dict(self._auto_level_settings or self._settings)
        updated = apply_auto_level_bounds_to_settings(base, black_point, white_point)
        self._auto_level_settings = updated
        if not self.auto_levels_checkbox.isChecked():
            return
        # Only push into the sliders; do NOT re-emit settingsChanged. The
        # pipeline is still doing live recompute, so a re-render was already
        # triggered by whatever slider event fired.
        with QSignalBlocker(self.light_slider):
            self.light_slider.setValue(int(round(light_ev * _EV_SLIDER_SCALE)))
        with QSignalBlocker(self.dark_slider):
            self.dark_slider.setValue(int(round(abs(dark_ev) * _EV_SLIDER_SCALE)))
        self._refresh_value_labels()

    def set_pick_checked(self, checked: bool) -> None:
        with QSignalBlocker(self.pick_button):
            self.pick_button.setChecked(bool(checked))
        self._refresh_pick_button_text()

    def set_pick_enabled(self, enabled: bool) -> None:
        self._pick_context_enabled = bool(enabled)
        self.pick_button.setEnabled(
            bool(self._pick_context_enabled and not self._multi_selection_mode)
        )

    def set_multi_selection_mode(self, enabled: bool) -> None:
        """Limit WB actions that cannot be applied safely to several sources."""
        self._multi_selection_mode = bool(enabled)
        for value in ("auto", "custom"):
            button = self.white_balance_selector.button_for_value(value)
            if button is not None:
                button.setEnabled(not self._multi_selection_mode)
        if self._multi_selection_mode and self.pick_button.isChecked():
            self.set_pick_checked(False)
        self.pick_button.setEnabled(
            bool(self._pick_context_enabled and not self._multi_selection_mode)
        )

    def set_controls_enabled(self, enabled: bool) -> None:
        self.setEnabled(bool(enabled))

    def set_tone_controls_enabled(self, enabled: bool) -> None:
        self._set_tone_controls_enabled(bool(enabled))

    def _sync_controls_from_settings(self, settings: RawRenderSettings) -> None:
        self._loading = True
        self._slider_change_pending = False
        try:
            mode = str(settings.white_balance_mode or "camera").strip().lower() or "camera"
            if mode not in {"camera", "auto", "custom"}:
                mode = "camera"
            with QSignalBlocker(self.white_balance_selector):
                self._set_white_balance_visual_selection(mode)
            with QSignalBlocker(self.auto_levels_checkbox):
                self.auto_levels_checkbox.setChecked(bool(settings.auto_levels))
            with QSignalBlocker(self.auto_levels_method_selector):
                self.auto_levels_method_selector.set_selected_value(settings.auto_levels_method)
            with QSignalBlocker(self.auto_levels_clipping_selector):
                self.auto_levels_clipping_selector.set_selected_value(settings.auto_levels_clipping)
            with QSignalBlocker(self.light_slider):
                self.light_slider.setValue(int(round(float(settings.light_ev) * _EV_SLIDER_SCALE)))
            with QSignalBlocker(self.dark_slider):
                self.dark_slider.setValue(int(round(abs(float(settings.dark_ev)) * _EV_SLIDER_SCALE)))
            with QSignalBlocker(self.tone_curve_checkbox):
                self.tone_curve_checkbox.setChecked(bool(settings.tone_curve_enabled))
            with QSignalBlocker(self.curve_strength_slider):
                self.curve_strength_slider.setValue(int(round(float(settings.tone_curve_strength) * 100.0)))
            with QSignalBlocker(self.curve_midpoint_slider):
                self.curve_midpoint_slider.setValue(int(round(float(settings.tone_curve_midpoint) * 100.0)))
            with QSignalBlocker(self.contrast_slider):
                self.contrast_slider.setValue(int(round(float(settings.tone_contrast) * 100.0)))
            with QSignalBlocker(self.shadows_slider):
                self.shadows_slider.setValue(int(round(float(settings.tone_shadows) * 100.0)))
            with QSignalBlocker(self.highlights_slider):
                self.highlights_slider.setValue(int(round(float(settings.tone_highlights) * 100.0)))
            self._refresh_value_labels()
            self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._refresh_pick_button_text()
        finally:
            self._loading = False

    def _apply_auto_level_settings(self) -> None:
        if self._auto_level_settings is None:
            return
        with QSignalBlocker(self.light_slider):
            self.light_slider.setValue(int(round(float(self._auto_level_settings.light_ev) * _EV_SLIDER_SCALE)))
        with QSignalBlocker(self.dark_slider):
            self.dark_slider.setValue(int(round(abs(float(self._auto_level_settings.dark_ev)) * _EV_SLIDER_SCALE)))
        self._refresh_value_labels()

    def _refresh_value_labels(self) -> None:
        def _mixed(slider) -> bool:
            return isinstance(slider, MixedStateSlider) and slider.is_mixed()
        self.light_value_label.setText("" if _mixed(self.light_slider) else self._ev_value_text(self.light_slider.value()))
        self.dark_value_label.setText("" if _mixed(self.dark_slider) else self._dark_ev_value_text(self.dark_slider.value()))
        self.curve_strength_value_label.setText(
            "" if _mixed(self.curve_strength_slider) else f"{float(self.curve_strength_slider.value()) / 100.0:.2f}"
        )
        self.curve_midpoint_value_label.setText(
            "" if _mixed(self.curve_midpoint_slider) else f"{float(self.curve_midpoint_slider.value()) / 100.0:.2f}"
        )
        self.contrast_value_label.setText(
            "" if _mixed(self.contrast_slider) else self._signed_percent_value_text(self.contrast_slider.value())
        )
        self.shadows_value_label.setText(
            "" if _mixed(self.shadows_slider) else self._signed_percent_value_text(self.shadows_slider.value())
        )
        self.highlights_value_label.setText(
            "" if _mixed(self.highlights_slider) else self._signed_percent_value_text(self.highlights_slider.value())
        )

    def _settings_from_controls(self) -> RawRenderSettings:
        auto_levels_enabled = bool(self.auto_levels_checkbox.isChecked())
        base_settings = RawRenderSettings.from_dict(
            self._auto_level_settings if auto_levels_enabled and self._auto_level_settings is not None else self._settings
        )
        white_balance_mode = str(self.white_balance_selector.selected_value("camera") or "camera").strip().lower() or "camera"
        light_ev = max(0.0, min(2.0, float(self.light_slider.value()) / _EV_SLIDER_SCALE))
        dark_ev = -max(0.0, min(2.0, float(self.dark_slider.value()) / _EV_SLIDER_SCALE))
        tone_curve_strength = max(0.0, min(1.0, float(self.curve_strength_slider.value()) / 100.0))
        tone_curve_midpoint = max(0.0, min(1.0, float(self.curve_midpoint_slider.value()) / 100.0))
        clipping_enabled = bool(self.auto_levels_clipping_selector.selected_value(True))
        settings = replace(
            base_settings,
            white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
            wb_multipliers=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_multipliers,
            wb_selection=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_selection,
            wb_multiplier_space=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_multiplier_space,
            wb_sample_point=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_sample_point,
            wb_selection_space=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_selection_space,
            exposure_ev=light_ev + dark_ev,
            light_ev=light_ev,
            dark_ev=dark_ev,
            auto_levels=auto_levels_enabled,
            auto_levels_method=str(self.auto_levels_method_selector.selected_value("a") or "a"),
            auto_levels_clipping=clipping_enabled,
            black_percentile=float(max(0.0, min(1.0, self._dark_cutoff))) if clipping_enabled else 0.0,
            white_percentile=float(max(0.0, min(1.0, 1.0 - self._bright_cutoff))) if clipping_enabled else 1.0,
            auto_levels_strength=1.0,
            auto_levels_soft_tails=False,
            auto_levels_tail_size=0.03,
            auto_levels_shadow_lift=float(base_settings.auto_levels_shadow_lift),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=tone_curve_strength,
            tone_curve_midpoint=tone_curve_midpoint,
            tone_contrast=float(self.contrast_slider.value()) / 100.0,
            tone_shadows=float(self.shadows_slider.value()) / 100.0,
            tone_highlights=float(self.highlights_slider.value()) / 100.0,
        )
        if white_balance_mode in {"camera", "auto"}:
            settings = replace(
                settings,
                white_balance_mode=white_balance_mode,
                wb_multipliers=None,
                wb_selection=None,
                wb_multiplier_space=None,
                wb_sample_point=None,
                wb_selection_space=None,
            )
        else:
            settings = replace(settings, white_balance_mode="custom")
        self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
        return settings

    def _set_tone_controls_enabled(self, enabled: bool) -> None:
        visible = bool(enabled) or self._show_tone_controls_when_disabled
        for widget in (
            self.curve_strength_label,
            self.curve_strength_slider,
            self.curve_midpoint_label,
            self.curve_midpoint_slider,
            self.curve_strength_value_label,
            self.curve_midpoint_value_label,
            self.shadows_label,
            self.shadows_slider,
            self.shadows_value_label,
            self.highlights_label,
            self.highlights_slider,
            self.highlights_value_label,
        ):
            widget.setVisible(visible)
            widget.setEnabled(bool(enabled))

    def is_auto_levels_enabled(self) -> bool:
        return bool(self.auto_levels_btn.isChecked())

    @staticmethod
    def _ev_value_text(value: int) -> str:
        return f"{float(value) / _EV_SLIDER_SCALE:.3f}"

    @staticmethod
    def _dark_ev_value_text(value: int) -> str:
        return f"{float(value) / _EV_SLIDER_SCALE:.3f}"

    @staticmethod
    def _signed_percent_value_text(value: int) -> str:
        return f"{int(value):+d}"

    def _refresh_pick_button_text(self) -> None:
        self.pick_button.setText(self.tr("Cancel") if self.pick_button.isChecked() else self.tr("Pick"))

    def _on_pick_toggled(self, checked: bool) -> None:
        self._refresh_pick_button_text()
        if not self._loading:
            self.pickWhiteBalanceToggled.emit(bool(checked))

    def _on_control_changed(self, *_args) -> None:
        if self._loading:
            return
        sender = self.sender()
        if sender is self.white_balance_selector:
            self._mixed_wb_mode = False
        if sender is self.auto_levels_checkbox:
            self.auto_levels_checkbox.setProperty("mixed", False)
            if self.auto_levels_checkbox.isChecked():
                self._apply_auto_level_settings()
        if sender is self.auto_levels_method_selector:
            self._mixed_auto_levels_method = False
            self._auto_level_settings = None
            self._settings = replace(
                RawRenderSettings.from_dict(self._settings),
                auto_levels_method=str(self.auto_levels_method_selector.selected_value("a") or "a"),
                auto_black_level=None,
                auto_white_level=None,
            )
        if sender is self.auto_levels_clipping_selector:
            self._mixed_auto_levels_clipping = False
            self._auto_level_settings = None
            self._settings = replace(
                RawRenderSettings.from_dict(self._settings),
                auto_levels_clipping=bool(self.auto_levels_clipping_selector.selected_value(True)),
                auto_black_level=None,
                auto_white_level=None,
            )
        if sender is self.tone_curve_checkbox:
            self.tone_curve_checkbox.setProperty("mixed", False)
        if isinstance(sender, QSlider):
            if isinstance(sender, MixedStateSlider) and sender.is_mixed():
                sender.clear_mixed_range()
            if sender in {self.light_slider, self.dark_slider} and self.auto_levels_checkbox.isChecked():
                with QSignalBlocker(self.auto_levels_checkbox):
                    self.auto_levels_checkbox.setChecked(False)
            self._slider_change_pending = bool(sender.isSliderDown())
            self._refresh_value_labels()
        self._settings = self._settings_from_controls()
        self._slider_change_pending = False
        self._refresh_value_labels()
        self.settingsChanged.emit(self._settings)

    def _on_slider_released(self, *_args) -> None:
        if self._loading:
            return
        self._slider_change_pending = False
