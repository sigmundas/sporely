"""Microscope, Slide / prep and Notes cards for the Live Lab tab (split out of ``ui/live_lab_tab.py``)."""
from __future__ import annotations

from PySide6.QtWidgets import (
    QFormLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QWidget,
)

from database.database_tags import DatabaseTerms
from database.models import SettingsDB
from database.schema import load_objectives, objective_display_name, objective_sort_value

from .combo_alerts import combo_is_unset, lab_state_combo_alert_stylesheet, update_combo_alert, update_combo_alerts
from .adaptive_choice_selector import (
    AdaptiveChoiceSelector,
    objective_color,
    objective_short_label,
    objective_is_macro_profile,
    stain_color,
)
from .section_card import create_section_card
from .live_lab_raw import PendingRawCapture


class LiveLabPrepPanelMixin:
    """Microscope, Slide / prep and Notes cards of ``LiveLabTab`` and their combo helpers.

    Mixed into ``LiveLabTab``; the methods use the tab's widgets and state through ``self``.
    """

    def _build_microscope_card(self) -> None:
        """Build the Microscope card (objective, contrast) as ``self.microscope_group``."""
        self.microscope_group, micro_form = create_section_card(
            self.tr("Microscope"),
            QFormLayout,
            body_margins=(10, 12, 10, 10),
        )
        micro_form.setSpacing(8)
        self.objective_combo = self._make_combo()
        self.objective_combo.set_unselected_border_visible(False)
        self.objective_combo.currentIndexChanged.connect(self._save_objective_selection)
        self.contrast_combo = self._build_term_combo("contrast")
        micro_form.addRow(self.tr("Objective:"), self.objective_combo)
        micro_form.addRow(self.tr("Contrast:"), self.contrast_combo)

    def _build_slide_prep_card(self) -> None:
        """Build the Slide / prep card (mount, stain, condition, source) as ``self.slide_prep_group``."""
        self.slide_prep_group, prep_form = create_section_card(
            self.tr("Slide / prep"),
            QFormLayout,
            body_margins=(10, 12, 10, 10),
        )
        prep_form.setSpacing(8)
        self.mount_combo = self._build_term_combo("mount")
        self.stain_combo = self._build_term_combo("stain")
        self.sample_combo = self._build_term_combo("sample")
        self.sample_source_combo = self._build_term_combo(
            "sample_source",
            pill_labels=DatabaseTerms.sample_source_compact_pills(),
            tooltips=DatabaseTerms.sample_source_compact_tooltips(),
        )
        self.sample_combo.setToolTip(self.tr("Specimen condition"))
        self.sample_source_combo.setToolTip(self.tr("Sample source"))
        prep_form.addRow(self.tr("Mount:"), self.mount_combo)
        prep_form.addRow(self.tr("Stain:"), self.stain_combo)
        prep_form.addRow(self.tr("Condition:"), self.sample_combo)
        prep_form.addRow(self.tr("Source:"), self.sample_source_combo)

    def _build_notes_card(self) -> None:
        """Build the Notes card (session note, image note) as ``self.notes_group``."""
        self.notes_group, notes_form = create_section_card(
            self.tr("Notes"),
            QFormLayout,
            body_margins=(10, 12, 10, 10),
        )
        notes_form.setSpacing(8)
        note_row = QWidget()
        note_row_layout = QHBoxLayout(note_row)
        note_row_layout.setContentsMargins(0, 0, 0, 0)
        note_row_layout.setSpacing(6)
        self.session_note_input = QLineEdit()
        self.session_note_input.setPlaceholderText(self.tr("Add a timestamped session note"))
        self.session_note_input.returnPressed.connect(self._add_session_note)
        note_row_layout.addWidget(self.session_note_input, 1)
        self.add_note_btn = QPushButton(self.tr("Add"))
        self.add_note_btn.clicked.connect(self._add_session_note)
        note_row_layout.addWidget(self.add_note_btn, 0)
        notes_form.addRow(self.tr("Note:"), note_row)
        try:
            from .image_import_dialog import AutoSizingPlainTextEdit
        except Exception:
            AutoSizingPlainTextEdit = None
        if AutoSizingPlainTextEdit is not None:
            self.image_note_input = AutoSizingPlainTextEdit(self, min_lines=2, max_lines=6)
            self.image_note_input.setObjectName("labImageNoteInput")
            self.image_note_input.setPlaceholderText(self.tr("Optional note for the selected image"))
            self.image_note_input.textChanged.connect(self._on_image_note_text_changed)
            notes_form.addRow(self.tr("Image note:"), self.image_note_input)
            self._image_note_mixed = False
            self._image_note_loading = False

    def _load_image_note_from_metadata(self, metadata: dict | None) -> None:
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        note = ""
        if isinstance(metadata, dict):
            note = str(metadata.get("notes") or metadata.get("image_note") or "")
        self._image_note_mixed = False
        self._image_note_loading = True
        try:
            widget.blockSignals(True)
            widget.setPlainText(note)
            widget.setPlaceholderText(self.tr("Optional note for the selected image"))
        finally:
            widget.blockSignals(False)
            self._image_note_loading = False

    def _load_image_note_from_metadatas(self, metadatas: list) -> None:
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        values = []
        for m in metadatas or []:
            if isinstance(m, dict):
                values.append(str(m.get("notes") or m.get("image_note") or ""))
            else:
                values.append("")
        distinct = []
        for v in values:
            if v not in distinct:
                distinct.append(v)
        self._image_note_loading = True
        try:
            widget.blockSignals(True)
            if len(distinct) <= 1:
                widget.setPlainText(distinct[0] if distinct else "")
                widget.setPlaceholderText(self.tr("Optional note for the selected image"))
                self._image_note_mixed = False
            else:
                widget.setPlainText("")
                widget.setPlaceholderText(self.tr("(Multiple notes)"))
                self._image_note_mixed = True
        finally:
            widget.blockSignals(False)
            self._image_note_loading = False

    def _on_image_note_text_changed(self) -> None:
        if getattr(self, "_image_note_loading", False):
            return
        widget = getattr(self, "image_note_input", None)
        if widget is None:
            return
        self._image_note_mixed = False
        text = str(widget.toPlainText() or "")
        captures = getattr(self, "_pending_raw_captures", []) or []
        selected_keys = set(self._session_gallery_selected_keys() or [])
        targets: list = []
        current_pending = self._current_pending_raw_capture()
        if not selected_keys and current_pending is not None:
            targets.append(current_pending)
        for capture in captures:
            if not isinstance(capture, PendingRawCapture):
                continue
            key = self._pending_raw_gallery_key(capture)
            if key in selected_keys or capture is current_pending:
                if capture not in targets:
                    targets.append(capture)
        for capture in targets:
            meta = dict(capture.lab_metadata or {})
            meta["notes"] = text
            capture.lab_metadata = meta

    def _make_combo(self):
        return AdaptiveChoiceSelector(self, compact=True)

    def _add_choice_item(
        self,
        combo,
        text: str,
        value,
        *,
        pill_text: str | None = None,
        tooltip: str | None = None,
        color: str | None = None,
    ) -> None:
        if isinstance(combo, AdaptiveChoiceSelector):
            combo.addItem(text, value, pillText=pill_text, tooltip=tooltip, color=color)
        else:
            combo.addItem(text, value)

    def _build_term_combo(
        self,
        category: str,
        *,
        pill_labels: dict[str, str] | None = None,
        tooltips: dict[str, str] | None = None,
    ):
        combo = self._make_combo()
        if category == "stain" and hasattr(combo, "set_unselected_border_visible"):
            combo.set_unselected_border_visible(False)
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            pill_override = (pill_labels or {}).get(value)
            if value == "Not_set":
                pill_text = "—"
            elif pill_override:
                pill_text = pill_override
            else:
                pill_text = display
            tooltip_override = (tooltips or {}).get(value)
            tooltip = tooltip_override or display
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text=pill_text,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=tooltip,
                )
            else:
                combo.addItem(display, value)
        combo.currentIndexChanged.connect(
            lambda _idx, cat=category, c=combo: self._remember_last_used_term(cat, c.currentData())
        )
        return combo

    def _refresh_term_combo(self, combo, category: str) -> None:
        if combo is None:
            return
        current_value = combo.currentData()
        combo.blockSignals(True)
        combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text="—" if value == "Not_set" else display,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=display,
                )
            else:
                combo.addItem(display, value)
        index = combo.findData(current_value) if current_value is not None else -1
        if index < 0:
            index = 0 if combo.count() else -1
        if index >= 0:
            combo.setCurrentIndex(index)
        combo.blockSignals(False)

    def refresh_microscope_tag_preferences(self) -> None:
        for category, combo in (
            ("contrast", getattr(self, "contrast_combo", None)),
            ("mount", getattr(self, "mount_combo", None)),
            ("stain", getattr(self, "stain_combo", None)),
            ("sample", getattr(self, "sample_combo", None)),
            ("sample_source", getattr(self, "sample_source_combo", None)),
        ):
            self._refresh_term_combo(combo, category)
        self._update_lab_state_combo_alerts()

    def _restore_term_selection(self, combo, category: str) -> None:
        saved = DatabaseTerms.canonicalize(category, SettingsDB.get_setting(DatabaseTerms.last_used_key(category), ""))
        if not saved:
            return
        index = combo.findData(saved)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _remember_last_used_term(self, category: str, value: str | None) -> None:
        if value:
            SettingsDB.set_setting(DatabaseTerms.last_used_key(category), str(value))

    def _lab_state_combo_alert_stylesheet(self) -> str:
        return lab_state_combo_alert_stylesheet(True)

    @staticmethod
    def _combo_is_unset(combo) -> bool:
        return combo_is_unset(combo)

    def _set_lab_state_combo_alert(self, combo, alert: bool) -> None:
        update_combo_alert(combo, alert)

    def _update_lab_state_combo_alerts(self, *_args) -> None:
        update_combo_alerts(
            (
                getattr(self, "objective_combo", None),
                getattr(self, "contrast_combo", None),
                getattr(self, "mount_combo", None),
                getattr(self, "stain_combo", None),
                getattr(self, "sample_combo", None),
            )
        )

    def _populate_objective_combo(self) -> None:
        self.objective_combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        objectives = load_objectives()
        rows = []
        for key, obj in objectives.items():
            if objective_is_macro_profile(obj, key):
                continue
            rows.append((key, obj))
        rows.sort(key=lambda item: objective_sort_value(item[1], item[0]))
        for key, obj in rows:
            label = objective_display_name(obj, key) or str(key)
            if callable(adder):
                adder(
                    self.objective_combo,
                    label,
                    key,
                    pill_text=objective_short_label(obj, key) or label,
                    color=objective_color(obj, key),
                    tooltip=label,
                )
            else:
                self.objective_combo.addItem(label, key)
        saved = str(SettingsDB.get_setting(self.SETTING_LAST_OBJECTIVE, "") or "").strip()
        if saved:
            index = self.objective_combo.findData(saved)
            if index >= 0:
                self.objective_combo.setCurrentIndex(index)
        self._update_lab_state_combo_alerts()

    def _save_objective_selection(self) -> None:
        objective_value = self._selected_combo_value(self.objective_combo)
        SettingsDB.set_setting(self.SETTING_LAST_OBJECTIVE, "" if objective_value is None else str(objective_value))
        refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh_viewer_objective_tag):
            refresh_viewer_objective_tag()

    def _add_session_note(self) -> None:
        note_text = str(self.session_note_input.text() or "").strip()
        if not note_text:
            return
        if not self.is_session_running():
            self._show_status(
                self.tr("Start a Live Lab session before adding notes."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        self._log_session_event("manual_note", value=note_text, metadata={"note_length": len(note_text)})
        self.session_note_input.clear()
        self._show_status(
            self.tr("Added session note."),
            tone="success",
            timeout_ms=2500,
        )
