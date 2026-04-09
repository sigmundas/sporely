"""Dialog for reviewing and importing community spore data."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from PySide6.QtCore import QEvent, QModelIndex, QStringListModel, QThread, Qt, Signal
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCompleter,
    QDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.models import ReferenceDB, SettingsDB, SpeciesDataAvailability
from utils.cloud_sync import CloudSyncError, SporelyCloudClient
from utils.vernacular_utils import (
    common_name_display_label,
    normalize_vernacular_language,
    resolve_vernacular_db_path,
)

from .delegates import SpeciesItemDelegate
from .dialog_helpers import make_github_help_button
from .observations_tab import VernacularDB


class _CloudSearchWorker(QThread):
    # Use a distinct name to avoid shadowing QThread.finished, which fires after run() returns
    # and is needed for safe lifecycle management.
    search_done = Signal(list, dict)
    error = Signal(str)

    def __init__(self, genus: str, species: str):
        super().__init__()
        self._genus = str(genus or "").strip()
        self._species = str(species or "").strip()

    def run(self) -> None:
        try:
            client = SporelyCloudClient.from_stored_credentials()
            if not client:
                raise CloudSyncError("Sign in to Sporely Cloud to search community spore data.")
            observation_rows = client.search_community_spore_datasets(self._genus, self._species, limit=50)
            reference_rows = client.search_public_reference_values(self._genus, self._species, limit=50)
            summary = client.community_spore_taxon_summary(self._genus, self._species) or {}
            combined: list[dict[str, Any]] = []
            for row in observation_rows or []:
                item = dict(row or {})
                item["_kind"] = "observation"
                combined.append(item)
            for row in reference_rows or []:
                item = dict(row or {})
                item["_kind"] = "reference"
                combined.append(item)
            combined.sort(
                key=lambda row: (
                    0 if row.get("_kind") == "observation" else 1,
                    -(int(row.get("measurement_count") or 0)),
                    str(row.get("updated_at") or ""),
                )
            )
            self.search_done.emit(combined, dict(summary or {}))
        except Exception as exc:
            self.error.emit(str(exc))


class _CloudDetailWorker(QThread):
    # Use a distinct name to avoid shadowing QThread.finished.
    detail_done = Signal(dict)
    error = Signal(str)

    def __init__(self, result_row: dict[str, Any]):
        super().__init__()
        self._row = dict(result_row or {})

    def run(self) -> None:
        try:
            kind = str(self._row.get("_kind") or "").strip()
            if kind == "reference":
                self.detail_done.emit(self._row)
                return
            obs_id = int(self._row.get("observation_id") or 0)
            if obs_id <= 0:
                raise CloudSyncError("Missing observation id for community dataset.")
            client = SporelyCloudClient.from_stored_credentials()
            if not client:
                raise CloudSyncError("Sign in to Sporely Cloud to load dataset details.")
            detail = client.get_community_spore_dataset(obs_id)
            if not isinstance(detail, dict) or not detail:
                raise CloudSyncError("No dataset details were returned.")
            detail["_kind"] = "observation"
            self.detail_done.emit(detail)
        except Exception as exc:
            self.error.emit(str(exc))


class CloudReferenceDialog(QDialog):
    """Search, review, and import community spore data."""

    def __init__(
        self,
        parent=None,
        *,
        genus: str = "",
        species: str = "",
        vernacular: str = "",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Search Community Spore Data"))
        self.setModal(True)
        self.setMinimumSize(980, 640)
        self.resize(1080, 700)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)

        self._vernacular_db = None
        self._species_availability = getattr(parent, "species_availability", None)
        if not isinstance(self._species_availability, SpeciesDataAvailability):
            self._species_availability = SpeciesDataAvailability()
        self._search_worker: _CloudSearchWorker | None = None
        self._detail_worker: _CloudDetailWorker | None = None
        # Strong Python refs to workers that have finished emitting but whose
        # QThread.finished hasn't fired yet (i.e. run() hasn't returned).
        # Prevents PySide6 GC from collecting the wrapper while the OS thread is live.
        self._worker_refs: list[QThread] = []
        self._results: list[dict[str, Any]] = []
        self._community_summary: dict[str, Any] = {}
        self._selected_result: dict[str, Any] | None = None
        self._selected_detail: dict[str, Any] | None = None
        self._accepted_action: str | None = None
        self._accepted_data: dict[str, Any] | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        intro = QLabel(
            self.tr(
                "Search public and friend-visible community spore datasets, review measurement quality, "
                "and import only after checking the method and calibration context."
            )
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        search_row = QHBoxLayout()
        search_row.setSpacing(8)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.vernacular_label = QLabel(self._vernacular_label())
        self.vernacular_input = QLineEdit()
        self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.genus_input = QLineEdit(genus or "")
        self.genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.species_input = QLineEdit(species or "")
        self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))

        form.addRow(self.vernacular_label, self.vernacular_input)
        form.addRow(self.tr("Genus:"), self.genus_input)
        form.addRow(self.tr("Species:"), self.species_input)
        search_row.addLayout(form, 1)

        actions_col = QVBoxLayout()
        actions_col.setContentsMargins(0, 0, 0, 0)
        actions_col.setSpacing(8)
        self.search_button = QPushButton(self.tr("Search"))
        self.search_button.clicked.connect(self._on_search_clicked)
        self.search_button.setDefault(True)
        actions_col.addWidget(self.search_button)
        actions_col.addWidget(
            make_github_help_button(self, "community-spore-data-plan.md"),
            0,
            Qt.AlignRight | Qt.AlignTop,
        )
        actions_col.addStretch(1)
        search_row.addLayout(actions_col)
        root.addLayout(search_row)

        splitter = QSplitter(Qt.Horizontal)
        splitter.setChildrenCollapsible(False)
        root.addWidget(splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)

        left_title = QLabel(self.tr("Results"))
        left_title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(left_title)

        self.results_table = QTableWidget(0, 4)
        self.results_table.setHorizontalHeaderLabels(
            [self.tr("Source"), self.tr("n"), self.tr("Q / L-W"), self.tr("Contributor")]
        )
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.verticalHeader().setVisible(False)
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.results_table.itemSelectionChanged.connect(self._on_result_selection_changed)
        left_layout.addWidget(self.results_table, 1)

        self.search_status_label = QLabel("")
        self.search_status_label.setWordWrap(True)
        self.search_status_label.setStyleSheet("color: #7f8c8d;")
        left_layout.addWidget(self.search_status_label)
        splitter.addWidget(left_panel)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        preview_title = QLabel(self.tr("Review"))
        preview_title.setStyleSheet("font-weight: 600;")
        right_layout.addWidget(preview_title)

        self.review_tabs = QTabWidget()
        right_layout.addWidget(self.review_tabs, 1)

        self.summary_tab = QWidget()
        self.summary_layout = QVBoxLayout(self.summary_tab)
        self.summary_layout.setContentsMargins(8, 8, 8, 8)
        self.summary_layout.setSpacing(8)
        self.summary_title_label = QLabel(self.tr("No dataset selected"))
        self.summary_title_label.setStyleSheet("font-weight: 600;")
        self.summary_layout.addWidget(self.summary_title_label)
        self.summary_meta_label = QLabel("")
        self.summary_meta_label.setWordWrap(True)
        self.summary_layout.addWidget(self.summary_meta_label)
        self.summary_table = QTableWidget(3, 4)
        self.summary_table.setHorizontalHeaderLabels(
            [self.tr("Metric"), self.tr("Min"), self.tr("Median / Mean"), self.tr("Max")]
        )
        self.summary_table.verticalHeader().setVisible(False)
        self.summary_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.summary_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.summary_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.summary_layout.addWidget(self.summary_table)
        self.summary_note_label = QLabel()
        self.summary_note_label.setWordWrap(True)
        self.summary_note_label.setFrameShape(QFrame.StyledPanel)
        self.summary_note_label.setStyleSheet("color: #7f8c8d; padding: 8px;")
        self.summary_layout.addWidget(self.summary_note_label)
        self.review_tabs.addTab(self.summary_tab, self.tr("Summary"))

        self.raw_spores_text = QPlainTextEdit()
        self.raw_spores_text.setReadOnly(True)
        self.review_tabs.addTab(self.raw_spores_text, self.tr("Raw spores"))

        self.method_tab = QWidget()
        self.method_form = QFormLayout(self.method_tab)
        self.method_form.setContentsMargins(8, 8, 8, 8)
        self.method_form.setSpacing(8)
        self._method_labels: dict[str, QLabel] = {}
        for key, label in (
            ("mount", self.tr("Mount medium:")),
            ("stain", self.tr("Stain:")),
            ("sample_type", self.tr("Sample type:")),
            ("contrast", self.tr("Contrast:")),
            ("objective", self.tr("Objective / profile:")),
            ("scale", self.tr("Scale / calibration:")),
        ):
            value_label = QLabel("—")
            value_label.setWordWrap(True)
            self._method_labels[key] = value_label
            self.method_form.addRow(label, value_label)
        self.review_tabs.addTab(self.method_tab, self.tr("Method"))

        self.calibration_text = QPlainTextEdit()
        self.calibration_text.setReadOnly(True)
        self.review_tabs.addTab(self.calibration_text, self.tr("Calibration"))

        self.provenance_text = QPlainTextEdit()
        self.provenance_text.setReadOnly(True)
        self.review_tabs.addTab(self.provenance_text, self.tr("Provenance"))

        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)

        button_row = QHBoxLayout()
        button_row.setSpacing(8)
        self.footer_hint = QLabel("")
        self.footer_hint.setWordWrap(True)
        self.footer_hint.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.footer_hint.setStyleSheet("color: #7f8c8d;")
        button_row.addWidget(self.footer_hint, 1)

        self.import_summary_button = QPushButton(self.tr("Import summary as reference"))
        self.import_summary_button.setEnabled(False)
        self.import_summary_button.clicked.connect(self._on_import_summary_clicked)
        button_row.addWidget(self.import_summary_button)

        self.plot_points_button = QPushButton(self.tr("Use raw points for plot"))
        self.plot_points_button.setEnabled(False)
        self.plot_points_button.clicked.connect(self._on_plot_points_clicked)
        button_row.addWidget(self.plot_points_button)

        close_button = QPushButton(self.tr("Close"))
        close_button.clicked.connect(self.reject)
        button_row.addWidget(close_button)
        root.addLayout(button_row)

        self._init_completers()
        if vernacular and not self.vernacular_input.text().strip():
            self.vernacular_input.setText(vernacular)
        self._maybe_set_vernacular_from_taxon()
        self._reset_preview()

    def _track_worker(self, worker: QThread) -> None:
        """Keep a strong Python ref to *worker* until after QThread.finished fires."""
        self._worker_refs.append(worker)
        worker.finished.connect(lambda: self._release_worker(worker))

    def _release_worker(self, worker: QThread) -> None:
        try:
            self._worker_refs.remove(worker)
        except ValueError:
            pass
        worker.deleteLater()

    def closeEvent(self, event) -> None:
        if self._search_worker is not None:
            self._search_worker.wait()
            self._search_worker = None
        if self._detail_worker is not None:
            self._detail_worker.wait()
            self._detail_worker = None
        super().closeEvent(event)

    def accepted_action(self) -> str | None:
        return self._accepted_action

    def accepted_data(self) -> dict[str, Any] | None:
        return dict(self._accepted_data or {}) if isinstance(self._accepted_data, dict) else None

    def _vernacular_label(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        base = self.tr("Common name")
        return f"{common_name_display_label(lang, base)}:"

    def _vernacular_placeholder(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        examples = {
            "no": "Kantarell",
            "de": "Pfifferling",
            "fr": "Girolle",
            "es": "Rebozuelo",
            "da": "Kantarel",
            "sv": "Kantarell",
            "fi": "Kantarelli",
            "pl": "Kurka",
            "pt": "Cantarelo",
            "it": "Gallinaccio",
        }
        return f"e.g., {examples.get(lang, 'Chanterelle')}"

    def _clean_species_text(self, text: str | None) -> str:
        return str(text or "").strip()

    def _clean_genus_text(self, text: str | None) -> str:
        token = str(text or "").strip().split()
        return token[0].strip() if token else ""

    def _set_species_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
            return
        self.species_input.setPlaceholderText(f"e.g., {'; '.join(cleaned[:4])}")

    def _set_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        self.vernacular_input.setPlaceholderText(f"e.g., {'; '.join(cleaned[:4])}")

    def _init_completers(self) -> None:
        popup_styler = getattr(self.parent(), "_style_dropdown_popup_readability", None)
        self._genus_model = QStringListModel()
        self._species_model = QStandardItemModel()
        self._vernacular_model = QStandardItemModel()

        self._genus_completer = QCompleter(self._genus_model, self)
        self._genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.genus_input.setCompleter(self._genus_completer)
        if callable(popup_styler):
            popup_styler(self._genus_completer.popup(), self.genus_input)
        self._genus_completer.activated[str].connect(self._on_genus_selected)

        self._species_completer = QCompleter(self._species_model, self)
        self._species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._species_completer.setCompletionRole(Qt.UserRole)
        self._species_completer.setFilterMode(Qt.MatchContains)
        self.species_input.setCompleter(self._species_completer)
        if callable(popup_styler):
            popup_styler(self._species_completer.popup(), self.species_input)
        self._species_completer.popup().setItemDelegate(
            SpeciesItemDelegate(
                self._species_availability,
                self._species_completer.popup(),
                genus_provider=lambda: self._clean_genus_text(self.genus_input.text()),
            )
        )
        self._species_completer.activated[QModelIndex].connect(self._on_species_selected)

        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            self._vernacular_db = VernacularDB(db_path, language_code=lang)
        self._vernacular_completer = QCompleter(self._vernacular_model, self)
        self._vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._vernacular_completer.setCompletionRole(Qt.UserRole)
        self.vernacular_input.setCompleter(self._vernacular_completer)
        if callable(popup_styler):
            popup_styler(self._vernacular_completer.popup(), self.vernacular_input)
        self._vernacular_completer.activated[QModelIndex].connect(self._on_vernacular_selected)

        self.genus_input.textChanged.connect(self._on_genus_text_changed)
        self.species_input.textChanged.connect(self._on_species_text_changed)
        self.vernacular_input.textChanged.connect(self._on_vernacular_text_changed)
        self.genus_input.editingFinished.connect(self._on_taxon_editing_finished)
        self.species_input.editingFinished.connect(self._on_taxon_editing_finished)
        self.vernacular_input.editingFinished.connect(self._on_vernacular_editing_finished)
        self.genus_input.installEventFilter(self)
        self.species_input.installEventFilter(self)
        self.vernacular_input.installEventFilter(self)

        self._update_genus_suggestions(self.genus_input.text())
        if self.genus_input.text().strip():
            suggestions = self._update_species_suggestions(self.genus_input.text(), self.species_input.text())
            self._set_species_placeholder_from_suggestions(suggestions)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.FocusIn:
            if obj == self.vernacular_input and not self.vernacular_input.text().strip():
                self._update_vernacular_suggestions_for_taxon()
                if self._vernacular_model.rowCount() > 0:
                    self._vernacular_completer.complete()
            elif obj == self.genus_input:
                self._update_genus_suggestions(self.genus_input.text())
                if self._genus_model.stringList():
                    self._genus_completer.complete()
            elif obj == self.species_input:
                genus = self._clean_genus_text(self.genus_input.text())
                if genus:
                    self._update_species_suggestions(genus, self.species_input.text())
                    if self._species_model.rowCount() > 0:
                        self._species_completer.setCompletionPrefix(self._clean_species_text(self.species_input.text()))
                        self._species_completer.complete()
        return super().eventFilter(obj, event)

    def _update_genus_suggestions(self, text: str, hide_on_exact: bool = False) -> list[str]:
        values = ReferenceDB.list_genera(text or "")
        if self._vernacular_db:
            values.extend(self._vernacular_db.suggest_genus(text or ""))
        values = sorted({value for value in values if value})
        if hide_on_exact and text.strip():
            text_lower = text.strip().lower()
            if any(value.lower() == text_lower for value in values):
                self._genus_model.setStringList([])
                self._genus_completer.popup().hide()
                return values
        self._genus_model.setStringList(values)
        return values

    def _update_species_suggestions(self, genus: str, text: str, hide_on_exact: bool = False) -> list[str]:
        genus = self._clean_genus_text(genus)
        prefix = self._clean_species_text(text)
        values = ReferenceDB.list_species(genus, prefix)
        if self._vernacular_db:
            values.extend(self._vernacular_db.suggest_species(genus, prefix))
        values = sorted({value for value in values if value})
        if hide_on_exact and prefix:
            prefix_lower = prefix.lower()
            if any(value.lower() == prefix_lower for value in values):
                self._species_model.clear()
                self._species_completer.popup().hide()
                return values
        self._species_model.clear()
        for species in values:
            item = QStandardItem(species)
            item.setData(species, Qt.UserRole)
            item.setData(genus, Qt.UserRole + 1)
            item.setData(species, Qt.UserRole + 2)
            self._species_model.appendRow(item)
        return values

    def _populate_vernacular_model(self, suggestions: list[str]) -> None:
        self._vernacular_model.clear()
        for name in suggestions:
            item = QStandardItem(name)
            item.setData(name, Qt.UserRole)
            self._vernacular_model.appendRow(item)

    def _update_vernacular_suggestions_for_taxon(self) -> None:
        if not self._vernacular_db:
            self._vernacular_model.clear()
            self._set_vernacular_placeholder_from_suggestions([])
            return
        genus = self._clean_genus_text(self.genus_input.text()) or None
        species = self._clean_species_text(self.species_input.text()) or None
        suggestions = self._vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        self._populate_vernacular_model(suggestions)
        self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _maybe_set_vernacular_from_taxon(self) -> None:
        if not self._vernacular_db or self.vernacular_input.text().strip():
            return
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if not genus or not species:
            return
        suggestions = self._vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if len(suggestions) == 1:
            self.vernacular_input.setText(suggestions[0])
        else:
            self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _on_genus_text_changed(self, text: str) -> None:
        self._update_genus_suggestions(text, hide_on_exact=True)
        genus = self._clean_genus_text(text)
        if self.genus_input.hasFocus():
            if self.vernacular_input.text().strip():
                self.vernacular_input.blockSignals(True)
                self.vernacular_input.clear()
                self.vernacular_input.blockSignals(False)
            if self.species_input.text().strip():
                self.species_input.blockSignals(True)
                self.species_input.clear()
                self.species_input.blockSignals(False)
                self._species_model.clear()
        if genus and not self.species_input.text().strip():
            suggestions = self._update_species_suggestions(genus, "")
            self._set_species_placeholder_from_suggestions(suggestions)
        elif not genus:
            self._species_model.clear()
            self._set_species_placeholder_from_suggestions([])
        self._update_vernacular_suggestions_for_taxon()

    def _on_species_text_changed(self, text: str) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        if not genus:
            self._species_model.clear()
            self._set_species_placeholder_from_suggestions([])
            return
        suggestions = self._update_species_suggestions(genus, text, hide_on_exact=True)
        if not self._clean_species_text(text):
            self._set_species_placeholder_from_suggestions(suggestions)

    def _on_vernacular_text_changed(self, text: str) -> None:
        if not self._vernacular_db:
            return
        genus = self._clean_genus_text(self.genus_input.text()) or None
        species = self._clean_species_text(self.species_input.text()) or None
        if not text.strip():
            self._update_vernacular_suggestions_for_taxon()
            return
        suggestions = self._vernacular_db.suggest_vernacular(text, genus=genus, species=species)
        self._populate_vernacular_model(suggestions)

    def _on_taxon_editing_finished(self) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if genus != self.genus_input.text().strip():
            self.genus_input.blockSignals(True)
            self.genus_input.setText(genus)
            self.genus_input.blockSignals(False)
        if species != self.species_input.text().strip():
            self.species_input.blockSignals(True)
            self.species_input.setText(species)
            self.species_input.blockSignals(False)
        self._maybe_set_vernacular_from_taxon()

    def _on_vernacular_selected(self, index: QModelIndex) -> None:
        if not self._vernacular_db or not index.isValid():
            return
        name = (index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
        self.vernacular_input.blockSignals(True)
        self.vernacular_input.setText(name)
        self.vernacular_input.blockSignals(False)
        taxon = self._vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            self.genus_input.setText(genus)
            self.species_input.setText(species)

    def _on_vernacular_editing_finished(self) -> None:
        if not self._vernacular_db:
            return
        name = self.vernacular_input.text().strip()
        if not name:
            return
        taxon = self._vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            self.genus_input.setText(genus)
            self.species_input.setText(species)

    def _on_genus_selected(self, genus: str) -> None:
        cleaned = self._clean_genus_text(genus)
        if cleaned:
            self.genus_input.setText(cleaned)

    def _on_species_selected(self, index: QModelIndex) -> None:
        if not index.isValid():
            return
        species = (index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
        if species:
            self.species_input.setText(species)
        self._maybe_set_vernacular_from_taxon()

    def _set_busy(self, busy: bool, status_text: str | None = None) -> None:
        self.search_button.setEnabled(not busy)
        self.genus_input.setEnabled(not busy)
        self.species_input.setEnabled(not busy)
        self.vernacular_input.setEnabled(not busy)
        if status_text is not None:
            self.search_status_label.setText(status_text)

    def _result_source_label(self, row: dict[str, Any]) -> str:
        kind = str(row.get("_kind") or "").strip()
        species_tag = str(row.get("species") or "").strip()
        if species_tag:
            genus_tag = str(row.get("genus") or "").strip()
            species_tag = f"{genus_tag} {species_tag}".strip() if genus_tag else species_tag
        if kind == "reference":
            source = str(row.get("source") or "").strip() or self.tr("Reference values")
            mount = str(row.get("mount_medium") or "").strip()
            stain = str(row.get("stain") or "").strip()
            prep = ", ".join(part for part in (mount, stain) if part)
            label = f"{source} [{prep}]".strip() if prep else source
            return f"{species_tag} – {label}" if species_tag else label
        observed_on = str(row.get("observed_on") or "").strip()
        if observed_on:
            base = self.tr("Community observation {date}").format(date=observed_on)
        else:
            base = self.tr("Community observation")
        return f"{species_tag} – {base}" if species_tag else base

    def _result_q_range_label(self, row: dict[str, Any]) -> str:
        q_min = row.get("q_min")
        q_p50 = row.get("q_p50")
        q_max = row.get("q_max")
        if q_min is None and q_p50 is None and q_max is None:
            length = row.get("length_p05")
            width = row.get("width_p50")
            if length is None and width is None:
                return "—"
        parts = []
        if q_min is not None:
            parts.append(f"{float(q_min):.2f}")
        if q_p50 is not None:
            parts.append(f"{float(q_p50):.2f}")
        if q_max is not None and (not parts or parts[-1] != f"{float(q_max):.2f}"):
            parts.append(f"{float(q_max):.2f}")
        return " / ".join(parts) if parts else "—"

    def _populate_results_table(self) -> None:
        self.results_table.setRowCount(0)
        for row_data in self._results:
            row = self.results_table.rowCount()
            self.results_table.insertRow(row)
            values = [
                self._result_source_label(row_data),
                str(int(row_data.get("measurement_count") or 0) or "—"),
                self._result_q_range_label(row_data),
                str(row_data.get("contributor_label") or "—"),
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self.results_table.setItem(row, col, item)
        self.results_table.resizeRowsToContents()

    def _reset_preview(self) -> None:
        self.summary_title_label.setText(self.tr("No dataset selected"))
        self.summary_meta_label.setText(
            self.tr("Search by genus (or genus + species) and choose a result to review stats, method, calibration, and provenance.")
        )
        self.summary_note_label.setText(
            self.tr("Import actions stay disabled until a search result is selected and loaded.")
        )
        for row, metric in enumerate((self.tr("Length"), self.tr("Width"), self.tr("Q"))):
            self.summary_table.setItem(row, 0, QTableWidgetItem(metric))
            self.summary_table.setItem(row, 1, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 2, QTableWidgetItem("—"))
            self.summary_table.setItem(row, 3, QTableWidgetItem("—"))
        self.raw_spores_text.setPlainText(self.tr("Select a community result to review raw spore points."))
        for label in self._method_labels.values():
            label.setText("—")
        self.calibration_text.setPlainText(self.tr("Select a community result to review calibration details."))
        self.provenance_text.setPlainText(self.tr("Select a community result to review contributor and source provenance."))
        self.footer_hint.setText("")
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)
        self._selected_result = None
        self._selected_detail = None

    def _on_search_clicked(self) -> None:
        genus = self._clean_genus_text(self.genus_input.text())
        species = self._clean_species_text(self.species_input.text())
        if not genus:
            QMessageBox.warning(
                self,
                self.tr("Missing Genus"),
                self.tr("Enter at least a genus to search community spore data."),
            )
            return
        if self._search_worker is not None:
            return
        self._results = []
        self._community_summary = {}
        self.results_table.clearSelection()
        self.results_table.setRowCount(0)
        self._reset_preview()
        self._set_busy(True, self.tr("Searching community spore data..."))
        worker = _CloudSearchWorker(genus, species)
        worker.search_done.connect(self._on_search_finished)
        worker.error.connect(self._on_search_error)
        self._search_worker = worker
        self._track_worker(worker)
        worker.start()

    def _on_search_finished(self, results: list, summary: dict) -> None:
        self._search_worker = None
        self._set_busy(False)
        self._results = [dict(row or {}) for row in (results or [])]
        self._community_summary = dict(summary or {})
        self._populate_results_table()
        if self._results:
            text = self.tr("Found {count} community source(s). Select one to review before importing.").format(
                count=len(self._results)
            )
            dataset_count = int(self._community_summary.get("dataset_count") or 0)
            measurement_count = int(self._community_summary.get("measurement_count") or 0)
            if dataset_count or measurement_count:
                text += " " + self.tr("Community aggregate: {datasets} dataset(s), n={count}.").format(
                    datasets=dataset_count,
                    count=measurement_count,
                )
            self.search_status_label.setText(text)
            self.footer_hint.setText(
                self.tr("Use Import summary to save a local reference, or Use raw points for a temporary comparison plot.")
            )
        else:
            genus_q = self._clean_genus_text(self.genus_input.text())
            species_q = self._clean_species_text(self.species_input.text())
            taxon_q = f"{genus_q} {species_q}".strip() if species_q else genus_q
            self.search_status_label.setText(
                self.tr("No community spore results found for {taxon}.").format(taxon=taxon_q)
            )

    def _on_search_error(self, message: str) -> None:
        self._search_worker = None
        self._set_busy(False)
        self.search_status_label.setText(str(message or "").strip() or self.tr("Community search failed."))

    def _on_result_selection_changed(self) -> None:
        if self._detail_worker is not None:
            return
        row = self.results_table.currentRow()
        if row < 0 or row >= len(self._results):
            self._reset_preview()
            return
        self._selected_result = dict(self._results[row])
        self._selected_detail = None
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)
        self.summary_title_label.setText(self.tr("Loading review details..."))
        self.summary_meta_label.setText(self._result_source_label(self._selected_result))
        self.summary_note_label.setText(self.tr("Loading dataset details..."))
        worker = _CloudDetailWorker(self._selected_result)
        worker.detail_done.connect(self._on_detail_finished)
        worker.error.connect(self._on_detail_error)
        self._detail_worker = worker
        self._track_worker(worker)
        worker.start()

    def _on_detail_finished(self, detail: dict) -> None:
        self._detail_worker = None
        self._selected_detail = dict(detail or {})
        self._populate_detail_preview()

    def _on_detail_error(self, message: str) -> None:
        self._detail_worker = None
        self._selected_detail = None
        self.summary_title_label.setText(self.tr("Could not load dataset"))
        self.summary_meta_label.setText(str(message or "").strip())
        self.summary_note_label.setText(self.tr("This result could not be reviewed."))
        self.raw_spores_text.setPlainText(str(message or "").strip())
        self.import_summary_button.setEnabled(False)
        self.plot_points_button.setEnabled(False)

    def _format_stat(self, value: Any, decimals: int = 2) -> str:
        if value is None:
            return "—"
        try:
            return f"{float(value):.{decimals}f}"
        except Exception:
            return str(value)

    def _populate_detail_preview(self) -> None:
        detail = self._selected_detail or {}
        kind = str(detail.get("_kind") or "").strip()
        genus = str(detail.get("genus") or "").strip()
        species = str(detail.get("species") or "").strip()
        contributor = str(detail.get("contributor_label") or "—")
        observed_on = str(detail.get("observed_on") or "").strip()
        measurement_count = int(detail.get("measurement_count") or 0)

        title = f"{genus} {species}".strip() or self.tr("Community dataset")
        if kind == "reference":
            title += f" ({self.tr('Reference')})"
        else:
            title += f" ({self.tr('Observation dataset')})"
        self.summary_title_label.setText(title)
        self.summary_meta_label.setText(
            self.tr("Contributor: {contributor}  •  Date: {date}  •  n={count}").format(
                contributor=contributor,
                date=observed_on or "—",
                count=measurement_count,
            )
        )
        for row, key in enumerate(("length", "width", "q")):
            self.summary_table.setItem(row, 0, QTableWidgetItem(self.tr(key.capitalize())))
            self.summary_table.setItem(row, 1, QTableWidgetItem(self._format_stat(detail.get(f"{key}_min"))))
            median_value = detail.get(f"{key}_p50")
            if median_value is None:
                median_value = detail.get(f"{key}_avg")
            self.summary_table.setItem(row, 2, QTableWidgetItem(self._format_stat(median_value)))
            max_value = detail.get(f"{key}_max")
            self.summary_table.setItem(row, 3, QTableWidgetItem(self._format_stat(max_value)))

        qc_flags = detail.get("qc_flags") or {}
        qc_lines = []
        if isinstance(qc_flags, dict):
            for key, label in (
                ("has_mount", self.tr("Mount recorded")),
                ("has_stain", self.tr("Stain recorded")),
                ("has_sample_type", self.tr("Sample type recorded")),
                ("has_contrast", self.tr("Contrast recorded")),
                ("has_objective", self.tr("Objective recorded")),
                ("has_scale", self.tr("Scale recorded")),
                ("has_point_geometry", self.tr("Measurement points recorded")),
            ):
                if qc_flags.get(key):
                    qc_lines.append(label)
        self.summary_note_label.setText(
            self.tr("QC signals: {signals}").format(signals=", ".join(qc_lines) if qc_lines else self.tr("No extra QC metadata"))
        )

        measurements = detail.get("measurements_json") or []
        raw_lines = []
        for row in measurements[:200]:
            if not isinstance(row, dict):
                continue
            raw_lines.append(
                f"L={self._format_stat(row.get('length_um'))}  "
                f"W={self._format_stat(row.get('width_um'))}  "
                f"Q={self._format_stat((float(row.get('length_um')) / float(row.get('width_um'))) if row.get('length_um') is not None and row.get('width_um') not in (None, 0) else None)}"
            )
        self.raw_spores_text.setPlainText("\n".join(raw_lines) if raw_lines else self.tr("No raw point data returned."))

        def _join_list(value: Any) -> str:
            if isinstance(value, list):
                cleaned = [str(item).strip() for item in value if str(item).strip()]
                return ", ".join(cleaned) if cleaned else "—"
            text = str(value or "").strip()
            return text or "—"

        self._method_labels["mount"].setText(_join_list(detail.get("mount_media") or detail.get("mount_medium")))
        self._method_labels["stain"].setText(_join_list(detail.get("stains") or detail.get("stain")))
        self._method_labels["sample_type"].setText(_join_list(detail.get("sample_types") or detail.get("sample_type")))
        self._method_labels["contrast"].setText(_join_list(detail.get("contrasts") or detail.get("contrast")))
        self._method_labels["objective"].setText(_join_list(detail.get("objectives") or detail.get("objective_name")))
        scale_min = detail.get("scale_min")
        scale_max = detail.get("scale_max")
        if scale_min is not None or scale_max is not None:
            if scale_min == scale_max or scale_max is None:
                scale_text = f"{self._format_stat(scale_min)} µm/px"
            else:
                scale_text = f"{self._format_stat(scale_min)}-{self._format_stat(scale_max)} µm/px"
        else:
            scale_text = self._format_stat(detail.get("scale_microns_per_pixel"))
            if scale_text != "—":
                scale_text += " µm/px"
        self._method_labels["scale"].setText(scale_text)

        calibration_lines = []
        if scale_text != "—":
            calibration_lines.append(f"{self.tr('Scale')}: {scale_text}")
        if kind == "observation":
            calibration_lines.append(
                self.tr("Calibration details come from image/objective metadata in the synced observation dataset.")
            )
        else:
            calibration_lines.append(self.tr("Reference rows currently expose summary values only."))
        self.calibration_text.setPlainText("\n".join(calibration_lines))

        provenance_lines = [
            f"{self.tr('Kind')}: {kind or '—'}",
            f"{self.tr('Contributor')}: {contributor}",
            f"{self.tr('Date')}: {observed_on or '—'}",
        ]
        if kind == "reference":
            provenance_lines.append(f"{self.tr('Source')}: {detail.get('source') or '—'}")
            provenance_lines.append(self.tr("Imported reference values are currently treated as shared reference material."))
        else:
            provenance_lines.append(f"{self.tr('Observation id')}: {detail.get('observation_id') or '—'}")
            provenance_lines.append(self.tr("Location and private observation content are intentionally excluded from this review flow."))
        self.provenance_text.setPlainText("\n".join(provenance_lines))

        self.import_summary_button.setEnabled(True)
        self.plot_points_button.setEnabled(bool(measurements))
        self.footer_hint.setText(
            self.tr("Review complete. Import summary saves a local reference; Use raw points adds a temporary comparison plot.")
        )

    def _default_import_source(self) -> str:
        detail = self._selected_detail or {}
        contributor = str(detail.get("contributor_label") or "").strip()
        observed_on = str(detail.get("observed_on") or "").strip()
        if detail.get("_kind") == "reference":
            source = str(detail.get("source") or "").strip()
            return f"Cloud: {source}".strip() if source else self.tr("Cloud reference")
        parts = [part for part in (contributor, observed_on) if part]
        suffix = " - ".join(parts)
        return f"Cloud: {suffix}".strip() if suffix else self.tr("Cloud observation")

    def _single_detail_value(self, *keys: str) -> str | None:
        detail = self._selected_detail or {}
        for key in keys:
            value = detail.get(key)
            if isinstance(value, list):
                cleaned = [str(item).strip() for item in value if str(item).strip()]
                if len(cleaned) == 1:
                    return cleaned[0]
                if cleaned:
                    return None
            text = str(value or "").strip()
            if text:
                return text
        return None

    def _summary_metadata_payload(self) -> dict[str, Any]:
        detail = self._selected_detail or {}
        return {
            "source_type": "cloud",
            "cloud_dataset_kind": str(detail.get("_kind") or "").strip() or "observation",
            "cloud_observation_id": detail.get("observation_id"),
            "cloud_reference_id": detail.get("reference_id"),
            "cloud_source_label": self._default_import_source(),
            "contributor_label": str(detail.get("contributor_label") or "").strip() or None,
            "observed_on": str(detail.get("observed_on") or "").strip() or None,
            "license": detail.get("license"),
            "qc_flags": detail.get("qc_flags") or {},
            "imported_via": "cloud_reference_dialog",
            "imported_at": datetime.now().isoformat(timespec="seconds"),
        }

    def _summary_reference_payload(self) -> dict[str, Any] | None:
        detail = self._selected_detail or {}
        genus = str(detail.get("genus") or "").strip()
        species = str(detail.get("species") or "").strip()
        if not genus or not species:
            return None
        return {
            "genus": genus,
            "species": species,
            "source": self._default_import_source(),
            "mount_medium": self._single_detail_value("mount_media", "mount_medium"),
            "stain": self._single_detail_value("stains", "stain"),
            "length_min": detail.get("length_min"),
            "length_p05": detail.get("length_p05"),
            "length_p50": detail.get("length_p50"),
            "length_p95": detail.get("length_p95"),
            "length_max": detail.get("length_max"),
            "length_avg": detail.get("length_avg"),
            "width_min": detail.get("width_min"),
            "width_p05": detail.get("width_p05"),
            "width_p50": detail.get("width_p50"),
            "width_p95": detail.get("width_p95"),
            "width_max": detail.get("width_max"),
            "width_avg": detail.get("width_avg"),
            "q_min": detail.get("q_min"),
            "q_p50": detail.get("q_p50"),
            "q_max": detail.get("q_max"),
            "q_avg": detail.get("q_avg"),
            "source_kind": "reference",
            "source_type": "cloud",
            "metadata_json": self._summary_metadata_payload(),
        }

    def _points_payload(self) -> dict[str, Any] | None:
        detail = self._selected_detail or {}
        genus = str(detail.get("genus") or "").strip()
        species = str(detail.get("species") or "").strip()
        measurements = detail.get("measurements_json") or []
        if not genus or not species or not isinstance(measurements, list):
            return None
        points = []
        for row in measurements:
            if not isinstance(row, dict):
                continue
            length = row.get("length_um")
            width = row.get("width_um")
            if length is None or width in (None, 0):
                continue
            try:
                points.append({"length_um": float(length), "width_um": float(width)})
            except Exception:
                continue
        if not points:
            return None
        return {
            "genus": genus,
            "species": species,
            "points": points,
            "points_label": self._default_import_source(),
            "source_kind": "points",
            "source_type": "cloud",
            "length_min": detail.get("length_min"),
            "length_p05": detail.get("length_p05"),
            "length_p50": detail.get("length_p50"),
            "length_p95": detail.get("length_p95"),
            "length_max": detail.get("length_max"),
            "length_avg": detail.get("length_avg"),
            "width_min": detail.get("width_min"),
            "width_p05": detail.get("width_p05"),
            "width_p50": detail.get("width_p50"),
            "width_p95": detail.get("width_p95"),
            "width_max": detail.get("width_max"),
            "width_avg": detail.get("width_avg"),
            "q_min": detail.get("q_min"),
            "q_p50": detail.get("q_p50"),
            "q_max": detail.get("q_max"),
            "q_avg": detail.get("q_avg"),
        }

    def _on_import_summary_clicked(self) -> None:
        payload = self._summary_reference_payload()
        if not payload:
            QMessageBox.warning(self, self.tr("Missing Data"), self.tr("No summary data is loaded for import."))
            return
        self._accepted_action = "import_summary"
        self._accepted_data = payload
        self.accept()

    def _on_plot_points_clicked(self) -> None:
        payload = self._points_payload()
        if not payload:
            QMessageBox.warning(self, self.tr("Missing Data"), self.tr("No raw spore points are available for plotting."))
            return
        self._accepted_action = "plot_points"
        self._accepted_data = payload
        self.accept()
