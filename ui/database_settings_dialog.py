"""Database settings dialog."""

from pathlib import Path

from PySide6.QtCore import Qt, QEvent, QT_TRANSLATE_NOOP
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QFormLayout,
    QLineEdit,
    QPushButton,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QTabWidget,
    QListWidget,
    QListWidgetItem,
    QWidget,
)

from database.models import SettingsDB
from database.schema import (
    get_app_settings,
    save_app_settings,
    get_database_path,
    get_images_dir,
    init_database,
)
from database.database_tags import DatabaseTerms
from .hint_status import HintBar, HintStatusController


class DatabaseSettingsDialog(QDialog):
    """Dialog for database and image folder settings."""

    TAG_CATEGORIES = (
        ("contrast", QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Contrast methods")),
        ("mount", QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Mount media")),
        ("stain", QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Stains")),
        ("sample", QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Sample types")),
        ("measure", QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Measure categories")),
    )
    CONTRAST_HINTS = {
        "bf": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "BF - Brightfield"),
        "df": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "DF - Darkfield"),
        "dic": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "DIC - Differential interference contrast"),
        "oblique": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Oblique - side illumination / oblique lighting"),
        "phase": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Phase - Phase contrast"),
        "hmc": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "HMC - Hoffman modulation contrast"),
    }
    MOUNT_HINTS = {
        "water": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Water - neutral reference measurements"),
        "koh": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "KOH - clearing tissue, pigment reactions"),
        "nh3": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "NH₃ - pigment reactions"),
        "glycerine": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Glycerine - semi-permanent mounts"),
        "l4": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "L4 - alkaline glycerol mount with KOH, NaCl, and glycerol for clearer, slower-drying fungal mounts. Clémençon (1972), Zeitschrift für Pilzkunde 38: 49–53."),
    }
    STAIN_HINTS = {
        "melzer": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Melzer - iodine reagent for amyloid and dextrinoid reactions in spores and other fungal structures."),
        "congored": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Congo Red - wall staining / contrast"),
        "cottonblue": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Cotton Blue - chitin staining"),
        "lactofuchsin": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Lactofuchsin - strong general fungal stain"),
        "cresylblue": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Cresyl Blue - basic oxazine stain that can show metachromatic shifts and improve contrast in walls and cell contents."),
        "trypanblue": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Trypan Blue - blue diazo counterstain used to increase contrast in fungal tissues and damaged cells."),
        "chlorazolblacke": QT_TRANSLATE_NOOP("DatabaseSettingsDialog", "Chlorazol Black E - dark direct dye with strong affinity for chitin-rich fungal walls, septa, and fine outlines."),
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Database Settings"))
        self.setModal(True)
        self.setMinimumWidth(620)
        self.setMinimumHeight(580)
        self._tag_lists: dict[str, QListWidget] = {}
        self._category_order: list[str] = [category for category, _ in self.TAG_CATEGORIES]
        self._default_hint_text = self.tr(
            "Predefined tags can be toggled on/off. Add custom tags as you like"
        )
        self._hint_controller: HintStatusController | None = None
        self._loading_settings = False
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()

        # Database folder
        self.db_path_input = QLineEdit()
        self.db_path_input.editingFinished.connect(self._save_settings)
        db_browse = QPushButton(self.tr("Browse"))
        db_browse.clicked.connect(self._browse_db_folder)
        db_row = QHBoxLayout()
        db_row.addWidget(self.db_path_input)
        db_row.addWidget(db_browse)
        form.addRow(self.tr("Database folder:"), db_row)

        # Images folder
        self.images_dir_input = QLineEdit()
        self.images_dir_input.editingFinished.connect(self._save_settings)
        img_browse = QPushButton(self.tr("Browse"))
        img_browse.clicked.connect(self._browse_images_dir)
        img_row = QHBoxLayout()
        img_row.addWidget(self.images_dir_input)
        img_row.addWidget(img_browse)
        form.addRow(self.tr("Images folder:"), img_row)

        layout.addLayout(form)

        tags_label = QLabel(self.tr("Microscope tags"))
        tags_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(tags_label)

        self.tag_tabs = QTabWidget()
        for category, label in self.TAG_CATEGORIES:
            page = self._build_tag_page(category)
            self.tag_tabs.addTab(page, self.tr(label))
        layout.addWidget(self.tag_tabs, 1)

        custom_row = QHBoxLayout()
        self.add_custom_btn = QPushButton(self.tr("Add custom tag"))
        self.add_custom_btn.clicked.connect(self._add_custom_tag)
        self.remove_custom_btn = QPushButton(self.tr("Remove selected"))
        self.remove_custom_btn.clicked.connect(self._remove_selected_custom_tag)
        custom_row.addWidget(self.add_custom_btn)
        custom_row.addWidget(self.remove_custom_btn)
        custom_row.addStretch()
        layout.addLayout(custom_row)

        self._bottom_widget = QWidget(self)
        bottom_row = QHBoxLayout(self._bottom_widget)
        bottom_row.setContentsMargins(0, 0, 0, 0)
        self.hint_bar = HintBar(self)
        bottom_row.addWidget(self.hint_bar, 1)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        self.set_hint(self._default_hint_text)

        close_btn = QPushButton(self.tr("Close"))
        close_btn.setDefault(True)
        close_btn.clicked.connect(self.accept)
        bottom_row.addWidget(close_btn)
        layout.addWidget(self._bottom_widget)

        self._load_settings()

    def _build_tag_page(self, category: str):
        page = QWidget(self)
        page_layout = QVBoxLayout(page)
        page_layout.setContentsMargins(8, 8, 8, 8)
        page_layout.setSpacing(6)

        tag_list = QListWidget()
        tag_list.setAlternatingRowColors(True)
        tag_list.setSpacing(3)
        tag_list.setMouseTracking(True)
        tag_list.viewport().setMouseTracking(True)
        tag_list.itemEntered.connect(self._on_tag_item_hovered)
        tag_list.itemChanged.connect(lambda _item: self._save_tag_settings())
        tag_list.viewport().installEventFilter(self)
        tag_list.setStyleSheet(
            "QListWidget::item { padding: 4px 6px; min-height: 22px; }"
            "QListWidget::item:selected { background: #d9e9f8; color: #1f2d3d; }"
            "QListWidget::item:selected:!active { background: #d9e9f8; color: #1f2d3d; }"
            "QListWidget::item:hover { background: #edf5fd; color: #1f2d3d; }"
        )
        page_layout.addWidget(tag_list, 1)
        self._tag_lists[category] = tag_list
        return page

    def _active_category(self) -> str:
        idx = self.tag_tabs.currentIndex()
        if idx < 0 or idx >= len(self._category_order):
            return self._category_order[0]
        return self._category_order[idx]

    def _active_tag_list(self) -> QListWidget:
        return self._tag_lists[self._active_category()]

    def _populate_category_list(self, category: str, current_tags: list[str]) -> None:
        tag_list = self._tag_lists[category]
        tag_list.clear()

        predefined = DatabaseTerms.default_values(category)
        enabled = set(current_tags)

        for canonical in predefined:
            item = QListWidgetItem(DatabaseTerms.translate(category, canonical))
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if canonical in enabled else Qt.Unchecked)
            item.setData(Qt.UserRole, canonical)
            item.setData(Qt.UserRole + 1, "predefined")
            if category == "contrast":
                contrast_hint = self.CONTRAST_HINTS.get(str(canonical).strip().lower())
                if contrast_hint:
                    translated_hint = self.tr(contrast_hint)
                    item.setData(Qt.UserRole + 2, translated_hint)
            elif category == "mount":
                mount_hint = self.MOUNT_HINTS.get(
                    DatabaseTerms._normalize_token(canonical)
                )
                if mount_hint:
                    translated_hint = self.tr(mount_hint)
                    item.setData(Qt.UserRole + 2, translated_hint)
            elif category == "stain":
                stain_hint = self.STAIN_HINTS.get(
                    DatabaseTerms._normalize_token(canonical)
                )
                if stain_hint:
                    translated_hint = self.tr(stain_hint)
                    item.setData(Qt.UserRole + 2, translated_hint)
            tag_list.addItem(item)

        for canonical in current_tags:
            if canonical in predefined:
                continue
            item = QListWidgetItem(DatabaseTerms.translate(category, canonical))
            item.setFlags(item.flags() | Qt.ItemIsEditable)
            item.setData(Qt.UserRole, canonical)
            item.setData(Qt.UserRole + 1, "custom")
            tag_list.addItem(item)

    def _collect_category_tags(self, category: str) -> list[str]:
        tag_list = self._tag_lists[category]
        values: list[str] = []
        seen: set[str] = set()

        for row in range(tag_list.count()):
            item = tag_list.item(row)
            source = item.data(Qt.UserRole + 1)
            if source == "predefined":
                if item.checkState() != Qt.Checked:
                    continue
                canonical = DatabaseTerms.canonicalize(category, item.data(Qt.UserRole))
            else:
                canonical = DatabaseTerms.custom_to_canonical(item.text())

            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            values.append(canonical)

        return DatabaseTerms.canonicalize_list(category, values)

    def _add_custom_tag(self) -> None:
        tag_list = self._active_tag_list()
        item = QListWidgetItem(self.tr("New tag"))
        item.setFlags(item.flags() | Qt.ItemIsEditable)
        item.setData(Qt.UserRole, DatabaseTerms.custom_to_canonical(item.text()))
        item.setData(Qt.UserRole + 1, "custom")
        tag_list.addItem(item)
        tag_list.setCurrentItem(item)
        tag_list.editItem(item)
        self._save_tag_settings()

    def _remove_selected_custom_tag(self) -> None:
        tag_list = self._active_tag_list()
        item = tag_list.currentItem()
        if not item:
            return
        if item.data(Qt.UserRole + 1) != "custom":
            return
        tag_list.takeItem(tag_list.row(item))
        self._save_tag_settings()

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _on_tag_item_hovered(self, item: QListWidgetItem) -> None:
        hint_text = item.data(Qt.UserRole + 2) if item is not None else None
        if isinstance(hint_text, str) and hint_text.strip():
            self.set_hint(hint_text)
        else:
            self.set_hint(self._default_hint_text)

    def eventFilter(self, watched, event):
        if event.type() == QEvent.Leave:
            for tag_list in self._tag_lists.values():
                if watched is tag_list.viewport():
                    self.set_hint(self._default_hint_text)
                    break
        return super().eventFilter(watched, event)

    def _load_settings(self):
        self._loading_settings = True
        settings = get_app_settings()
        db_folder = settings.get("database_folder")
        if not db_folder and settings.get("database_path"):
            db_folder = str(Path(settings.get("database_path")).parent)
        if not db_folder:
            db_folder = str(get_database_path().parent)
        self.db_path_input.setText(db_folder)
        self.images_dir_input.setText(str(settings.get("images_dir") or get_images_dir()))

        for category, _label in self.TAG_CATEGORIES:
            setting_key = DatabaseTerms.setting_key(category)
            defaults = DatabaseTerms.default_values(category)
            current_tags = SettingsDB.get_list_setting(setting_key, defaults)
            current_tags = DatabaseTerms.canonicalize_list(category, current_tags)
            self._populate_category_list(category, current_tags)

        self._loading_settings = False

    def _browse_db_folder(self):
        path = QFileDialog.getExistingDirectory(
            self, self.tr("Select Database Folder"), self.db_path_input.text()
        )
        if path:
            self.db_path_input.setText(path)
            self._save_settings()

    def _browse_images_dir(self):
        path = QFileDialog.getExistingDirectory(
            self, self.tr("Select Images Folder"), self.images_dir_input.text()
        )
        if path:
            self.images_dir_input.setText(path)
            self._save_settings()

    def _save_tag_settings(self):
        if self._loading_settings:
            return
        for category, _label in self.TAG_CATEGORIES:
            setting_key = DatabaseTerms.setting_key(category)
            SettingsDB.set_list_setting(setting_key, self._collect_category_tags(category))

        # Always remember last used values.
        SettingsDB.set_setting("remember_last_used", True)
        SettingsDB.set_setting("original_storage_mode", "none")
        SettingsDB.set_setting("store_original_images", False)

    def _save_settings(self):
        if self._loading_settings:
            return
        settings = get_app_settings()
        db_folder = self.db_path_input.text().strip()
        images_dir = self.images_dir_input.text().strip()

        old_db_path = settings.get("database_path")
        old_ref_path = settings.get("reference_database_path")
        if db_folder:
            settings["database_folder"] = db_folder
            settings.pop("database_path", None)
            settings.pop("reference_database_path", None)
        else:
            settings.pop("database_folder", None)

        if images_dir:
            settings["images_dir"] = images_dir
        else:
            settings.pop("images_dir", None)

        save_app_settings(settings)

        if db_folder:
            try:
                target_dir = Path(db_folder)
                target_dir.mkdir(parents=True, exist_ok=True)
                new_db = target_dir / "mushrooms.db"
                new_ref = target_dir / "reference_values.db"
                if old_db_path and Path(old_db_path).exists() and Path(old_db_path) != new_db:
                    Path(old_db_path).replace(new_db)
                if old_ref_path and Path(old_ref_path).exists() and Path(old_ref_path) != new_ref:
                    Path(old_ref_path).replace(new_ref)
            except Exception as exc:
                QMessageBox.warning(self, self.tr("Database Move Failed"), str(exc))

        init_database()

        self._save_tag_settings()
