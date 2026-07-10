"""Shared RAW processing controls used by Prepare Images and Live Lab."""
from __future__ import annotations

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

from utils.raw_render import RawRenderSettings

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
        self._show_shadow_lift = bool(show_shadow_lift)
        self._show_tone_controls_when_disabled = bool(show_tone_controls_when_disabled)

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
        self.light_slider.valueChanged.connect(self._on_control_changed)
        self.light_slider.sliderReleased.connect(self._on_slider_released)
        self.auto_levels_btn = QPushButton(self.tr("Auto"), self)
        self.auto_levels_btn.setCheckable(True)
        self.auto_levels_btn.setFixedWidth(_TRAILING_WIDTH)
        self.auto_levels_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        self.auto_levels_btn.toggled.connect(self._on_control_changed)
        self.auto_levels_checkbox = self.auto_levels_btn  # backwards-compat alias
        self.light_value_label = QLabel("", self)
        self.light_value_label.setVisible(False)
        self.light_row = _build_slider_row(self.light_slider, self.auto_levels_btn)
        slider_form.addRow(self.light_label, self.light_row)
        self.exposure_row = self.light_row
        self.exposure_slider = self.light_slider
        self.exposure_value_label = self.light_value_label

        self.dark_label = QLabel(self.tr("Dark:"), self)
        self.dark_slider = MixedStateSlider(Qt.Horizontal, self)
        self.dark_slider.setRange(0, _EV_SLIDER_MAX)
        self.dark_slider.setSingleStep(1)
        self.dark_slider.setPageStep(25)
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
                    self.white_balance_selector.set_selected_value(next(iter(wb_modes)))
                else:
                    self.white_balance_selector.set_selected_value("camera")
            self._mixed_wb_mode = len(wb_modes) > 1
            auto_levels_values = {bool(s.auto_levels) for s in resolved}
            with QSignalBlocker(self.auto_levels_checkbox):
                if len(auto_levels_values) == 1:
                    self.auto_levels_checkbox.setChecked(next(iter(auto_levels_values)))
                    self.auto_levels_checkbox.setProperty("mixed", False)
                else:
                    self.auto_levels_checkbox.setChecked(False)
                    self.auto_levels_checkbox.setProperty("mixed", True)
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
        if self.tone_curve_checkbox.property("mixed"):
            mixed.add("tone_curve_enabled")
        return mixed

    def _clear_mixed_state(self) -> None:
        for _, slider, _ in self._named_mixed_slider_specs():
            slider.clear_mixed_range()
        self._mixed_wb_mode = False
        self.auto_levels_checkbox.setProperty("mixed", False)
        self.tone_curve_checkbox.setProperty("mixed", False)

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
        resolved = RawRenderSettings.from_dict(settings)
        self._auto_level_settings = resolved
        if self._loading or not self.auto_levels_checkbox.isChecked():
            return
        self._apply_auto_level_settings()

    def set_pick_checked(self, checked: bool) -> None:
        with QSignalBlocker(self.pick_button):
            self.pick_button.setChecked(bool(checked))
        self._refresh_pick_button_text()

    def set_pick_enabled(self, enabled: bool) -> None:
        self.pick_button.setEnabled(bool(enabled))

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
                if not self.white_balance_selector.set_selected_value(mode):
                    self.white_balance_selector.set_selected_value("camera")
            with QSignalBlocker(self.auto_levels_checkbox):
                self.auto_levels_checkbox.setChecked(bool(settings.auto_levels))
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
        base_settings = RawRenderSettings.from_dict(self._settings)
        white_balance_mode = str(self.white_balance_selector.selected_value("camera") or "camera").strip().lower() or "camera"
        light_ev = max(0.0, min(2.0, float(self.light_slider.value()) / _EV_SLIDER_SCALE))
        dark_ev = -max(0.0, min(2.0, float(self.dark_slider.value()) / _EV_SLIDER_SCALE))
        tone_curve_strength = max(0.0, min(1.0, float(self.curve_strength_slider.value()) / 100.0))
        tone_curve_midpoint = max(0.0, min(1.0, float(self.curve_midpoint_slider.value()) / 100.0))
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
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            black_percentile=0.0,
            white_percentile=1.0,
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
