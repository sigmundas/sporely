"""Rediger observasjon: the observation details dialog.

Moved unchanged from ``ui/observations_tab.py`` (desktop redesign stage 9g1),
so the Rediger mode can build on it. ``ui.observations_tab`` re-exports
``ObservationDetailsDialog``; the helpers it shares with the Observations tab
stay there and are imported from it.
"""
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QDialog,
    QFormLayout,
    QLineEdit,
    QTextEdit,
    QDateTimeEdit,
    QFileDialog,
    QLabel,
    QSplitter,
    QRadioButton,
    QButtonGroup,
    QComboBox,
    QCheckBox,
    QDoubleSpinBox,
    QTabWidget,
    QCompleter,
    QSizePolicy,
    QAbstractItemView,
    QFrame,
    QApplication,
    QMenu,
    QToolButton,
    QScrollArea,
    QGridLayout,
)
from PySide6.QtCore import (
    Qt,
    Signal,
    QDateTime,
    QSize,
    QStringListModel,
    QEvent,
    QTimer,
    QThread,
    QStandardPaths,
    QSettings,
    QModelIndex,
)
from PySide6.QtGui import (
    QAction,
    QActionGroup,
    QIcon,
    QPixmap,
    QImage,
    QImageReader,
    QDesktopServices,
    QColor,
    QPainter,
    QShortcut,
    QKeySequence,
)
from PIL import Image
from PySide6.QtCore import QUrl
from dataclasses import replace as dataclass_replace
from pathlib import Path
import io
import tempfile
import time
import os
import sys
import json
import unicodedata
from database.models import (
    ObservationDB,
    ImageDB,
    MeasurementDB,
    SettingsDB,
    CLOUD_IMAGE_STATE_DELETED,
    CLOUD_IMAGE_STATE_NONE,
    CLOUD_IMAGE_STATE_UPLOADED,
)
from database.vernacular_db import VernacularDB
from database.taxon_lookup import TAXON_COMPLETER_LIMIT, TaxonChoice, TaxonLookupService, determine_redlist_area
from database.taxon_suggestions import (
    KIND_NAME_CONFIRMED,
    ORIGIN_OBSERVATION,
    ai_prediction_external_id,
    evidence_concept_ids,
    observation_identity_offer,
)
from .taxon_input_controller import TaxonInputController
from .taxon_picker import TaxonPicker
from database.reverse_location_lookup import LocationLookupResult, normalize_country_code
from database.database_tags import DatabaseTerms
from database.schema import (
    get_images_dir,
    get_app_settings,
    load_objectives,
    objective_display_name,
    objective_sort_value,
    resolve_objective_key,
    update_app_settings,
)
from utils.thumbnail_generator import get_thumbnail_path
from utils.exif_reader import get_image_metadata
from utils.cloud_sync_impl.reconciliation.location_provenance import (
    IMAGE_EXIF_LOCATION_PAIR,
    LOCATION_PROVENANCE_FIELDS,
    MANUAL_LOCATION_PAIR,
    attribute_coordinate_edit,
    coordinates_match,
    location_pair_is_writable,
    provenance_pair,
)
import utils.ai_image_prep as ai_image_prep
from utils.artsdatabanken_link import concept_link_from_name_id
from utils.taxon_identity import IDENTITY_COLUMNS, TaxonIdentity, proven_sporely_taxon_id
import re
import requests
from utils.vernacular_utils import (
    normalize_vernacular_language,
    resolve_available_vernacular_language,
    resolve_vernacular_db_path,
    vernacular_language_label,
    list_available_vernacular_languages,
)
from utils.publish_targets import (
    PUBLISH_TARGET_ARTPORTALEN_SE,
    PUBLISH_TARGET_ARTSOBS_NO,
    normalize_publish_target,
    publish_target_from_country_code,
)
from utils.taxon_text import format_probability_percent, split_scientific_name_text
from utils.cloud_sync import (
    SporelyCloudClient,
    cloud_observation_uses_privacy_slot,
    cloud_media_materialization_state_for_observation,
    fetch_cloud_usage_summary,
    _format_cloud_sync_observation_status,
)
from .image_gallery_widget import ImageGalleryWidget, _microscope_tag_from_image
from .image_import_dialog import ImageImportDialog, ImageImportResult, AIGuessWorker
from .hint_status import HintBar, HintStatusController
from .dialog_helpers import ask_measurements_exist_delete, ask_wrapped_yes_no, make_github_help_button
from .section_card import create_section_card
from .segmented_selector import SegmentedSelector
from .styles import RED_LIST_BADGE_COLORS, pt, get_design_tokens
from .window_state import GeometryMixin
from app_identity import SETTINGS_APP, SETTINGS_ORG, app_data_dir

from . import observations_tab as _observations_tab
from .observations_tab import (
    INatAIGuessWorker,
    LOCATION_PRECISION_EXACT,
    LOCATION_PRECISION_FUZZED,
    LocationLookupWorker,
    MapServiceHelper,
    ObservationsTab,
    SETTING_CLOUD_DEFAULT_SHARING_SCOPE,
    SHARING_SCOPE_FRIENDS,
    SHARING_SCOPE_PRIVATE,
    SHARING_SCOPE_PUBLIC,
    _CloudMediaMaterializationWorker,
    _NullSuspend,
    _VALID_SHARING_SCOPES,
    _cloud_media_materialization_should_launch,
    _current_utc_timestamp_text,
    _debug_import_flow,
    _default_publish_selected_for_new_image,
    _extract_coords_from_osm_url,
    _extract_location_name_from_map_url,
    _image_microscope_publish_group_key,
    _normalize_ai_prediction_taxon,
    _normalize_ai_selected_service,
    _parse_observation_datetime,
    normalize_location_precision,
    normalize_sharing_scope,
    resolve_observation_publish_target,
    sharing_scope_location_public,
)


class ObservationDetailsDialog(GeometryMixin, QDialog):
    # In place (Rediger, stage 9g2): "Edit images" and an image's Edit or
    # double-click ask the page to open Bilder › Files and SD card (path, or "").
    editImagesRequested = Signal(str)

    """Dialog for creating or editing an observation after image import."""

    _geometry_key = "ObservationDetailsDialog"
    _gallery_splitter_key = "splitter/ObservationDetailsDialogBottom"
    _taxonomy_splitter_key = "splitter/ObservationDetailsDialogTaxonomy"
    _ROLE_TAXON_CHOICE = Qt.UserRole + 4

    def _create_dialog_box(
        self,
        title: str,
        layout_type=QVBoxLayout,
        *,
        body_margins: tuple[int, int, int, int] = (12, 12, 12, 12),
        body_spacing: int = 8,
    ) -> tuple[QFrame, object]:
        return create_section_card(
            title,
            layout_type,
            body_margins=body_margins,
            body_spacing=body_spacing,
        )

    def _build_stacked_selector_field(
        self,
        label_text: str,
        widget: QWidget,
        *,
        parent: QWidget | None = None,
    ) -> QWidget:
        """Build a compact stacked label + selector field for cloud settings."""
        field = QWidget(parent or self)
        field.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        field_layout = QVBoxLayout(field)
        field_layout.setContentsMargins(0, 0, 0, 0)
        field_layout.setSpacing(4)

        label = QLabel(str(label_text), field)
        label.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        field_layout.addWidget(label, 0, Qt.AlignLeft)
        field_layout.addWidget(widget, 0, Qt.AlignLeft)
        return field

    def _window_cloud_client(self) -> SporelyCloudClient | None:
        try:
            window = self.window()
        except Exception:
            window = None
        if window is not None and hasattr(window, "__dict__"):
            if "_cloud_client" in window.__dict__:
                return window.__dict__.get("_cloud_client")
            getter = getattr(window, "_get_cloud_client", None)
            if callable(getter):
                try:
                    client = getter()
                except Exception:
                    client = None
                window.__dict__["_cloud_client"] = client
                return client
        try:
            client = SporelyCloudClient.from_stored_credentials()
        except Exception:
            client = None
        if window is not None and hasattr(window, "__dict__"):
            window.__dict__["_cloud_client"] = client
        return client

    def _cloud_privacy_usage_client(self) -> SporelyCloudClient | None:
        return self._window_cloud_client()

    def _refresh_cloud_privacy_slots_summary(self, force: bool = False) -> None:
        client = self._cloud_privacy_usage_client()
        if client is None:
            self._cloud_privacy_usage_summary = {}
            self._cloud_privacy_usage_summary_user_id = ""
            self._update_cloud_privacy_slots_label()
            return

        user_id = str(getattr(client, "user_id", "") or "").strip()
        if not force and user_id and self._cloud_privacy_usage_summary_user_id == user_id:
            self._update_cloud_privacy_slots_label()
            return

        try:
            summary = fetch_cloud_usage_summary(client)
            self._cloud_privacy_usage_summary = summary
        except Exception:
            summary = {}
            self._cloud_privacy_usage_summary = summary
        self._cloud_privacy_usage_summary_user_id = user_id if summary.get("cloud_usage_loaded") else ""
        self._update_cloud_privacy_slots_label()

    def _current_cloud_privacy_slot_consumption(self) -> int:
        record = {
            "sharing_scope": self._selected_sharing_scope(),
            "location_precision": self._selected_location_precision(),
        }
        return 1 if cloud_observation_uses_privacy_slot(record) else 0

    def _update_cloud_privacy_slots_label(self) -> None:
        label = getattr(self, "cloud_privacy_slots_label", None)
        if label is None:
            return

        summary = dict(getattr(self, "_cloud_privacy_usage_summary", {}) or {})
        profile_loaded = bool(
            summary.get("cloud_profile_loaded")
            or summary.get("cloudProfileLoaded")
            or summary.get("cloud_usage_loaded")
            or summary.get("cloudUsageLoaded")
        )
        privacy_loaded = bool(
            summary.get("cloud_privacy_usage_loaded")
            or summary.get("cloudPrivacyUsageLoaded")
            or summary.get("cloud_usage_loaded")
            or summary.get("cloudUsageLoaded")
        )
        profile_error = str(summary.get("cloud_profile_error") or summary.get("cloudProfileError") or "").strip()
        privacy_error = str(summary.get("cloud_privacy_usage_error") or summary.get("cloudPrivacyUsageError") or "").strip()
        summary_error = str(summary.get("cloud_usage_error") or summary.get("cloudUsageError") or "").strip()
        if not profile_loaded:
            if profile_error or summary_error:
                label.setText(self.tr("Private slot availability: unavailable"))
                label.setToolTip(profile_error or summary_error)
            elif self._cloud_privacy_usage_summary_user_id:
                label.setText(self.tr("Private slot availability: loading…"))
                label.setToolTip("")
            else:
                label.setText(self.tr("Sign in to Sporely Cloud to see private slot availability."))
                label.setToolTip("")
            label.setVisible(True)
            return

        base_used = max(0, int(summary.get("privacy_slots_used") or summary.get("privacy_slot_count") or 0))
        original_consumption = max(0, int(getattr(self, "_cloud_privacy_original_slot_consumption", 0) or 0))
        projected_consumption = self._current_cloud_privacy_slot_consumption()
        projected_used = max(0, base_used - original_consumption + projected_consumption)
        privacy_limit = summary.get("privacy_slots_limit")
        if privacy_limit is None:
            label.setText("")
            label.setToolTip("")
            label.setVisible(False)
            return

        try:
            limit_value = max(0, int(privacy_limit))
        except Exception:
            limit_value = 0
        if not privacy_loaded:
            if privacy_error:
                label.setText(self.tr("Private slot availability: unavailable"))
                label.setToolTip(privacy_error)
            else:
                label.setText(self.tr("Private slot availability: loading…"))
                label.setToolTip("")
        else:
            available = max(0, limit_value - projected_used)
            label.setText(
                self.tr("Available private slots: {available} of {limit}").format(
                    available=available,
                    limit=limit_value,
                )
            )
            label.setToolTip("")
        label.setVisible(True)

    def _ai_selected_fields(self, obs: dict | None) -> dict:
        row = dict(obs or {})
        return {
            "ai_selected_service": str(row.get("ai_selected_service") or "").strip() or None,
            "ai_selected_taxon_id": str(row.get("ai_selected_taxon_id") or "").strip() or None,
            "ai_selected_scientific_name": str(row.get("ai_selected_scientific_name") or "").strip() or None,
            "ai_selected_probability": (
                None
                if row.get("ai_selected_probability") in {"", None}
                else row.get("ai_selected_probability")
            ),
            "ai_selected_at": str(row.get("ai_selected_at") or "").strip() or None,
        }

    def _format_selected_ai_summary(self, obs: dict | None) -> str | None:
        row = dict(obs or {})
        service = str(row.get("ai_selected_service") or "").strip()
        taxon_id = str(row.get("ai_selected_taxon_id") or "").strip()
        scientific_name = str(row.get("ai_selected_scientific_name") or "").strip()
        selected_at = str(row.get("ai_selected_at") or "").strip()
        probability = row.get("ai_selected_probability")
        parts: list[str] = []
        if service:
            parts.append(service)
        if scientific_name:
            parts.append(scientific_name)
        if taxon_id:
            parts.append(self.tr("taxon {taxon_id}").format(taxon_id=taxon_id))
        if probability is not None and str(probability).strip():
            probability_text = format_probability_percent(probability)
            if probability_text:
                parts.append(f"p={probability_text}")
        if selected_at:
            parts.append(selected_at)
        if not parts:
            return None
        return self.tr("Selected AI: {summary}").format(summary=" · ".join(parts))

    def _update_selected_ai_summary_label(self, obs: dict | None) -> None:
        label = getattr(self, "ai_selected_summary_label", None)
        if label is None:
            return
        summary = self._format_selected_ai_summary(obs)
        if summary:
            label.setText(summary)
            label.setVisible(True)
        else:
            label.setText("")
            label.setVisible(False)

    def __init__(
        self,
        parent=None,
        observation=None,
        draft_data: dict | None = None,
        image_results: list[ImageImportResult] | None = None,
        primary_index: int | None = None,
        allow_edit_images: bool = False,
        suggested_taxon: dict | None = None,
        ai_state: dict | None = None,
    ):
        super().__init__(parent)
        self.observation = observation
        self.draft_data = dict(draft_data) if isinstance(draft_data, dict) else None
        self.edit_mode = observation is not None
        self.image_results = image_results or []
        self.primary_index = primary_index
        self.allow_edit_images = allow_edit_images
        self.request_edit_images = False
        self.request_edit_images_path: str | None = None
        # When True, the caller should skip re-opening this details dialog
        # after Prepare Images closes — set by the thumbnail right-click
        # "Edit photo" action so it behaves like a direct edit command.
        self.request_edit_images_skip_return = False
        self.suggested_taxon = suggested_taxon
        self.map_helper = MapServiceHelper(self)
        self._hint_controller: HintStatusController | None = None
        self.setWindowTitle(
            self.tr("Edit Observation") if self.edit_mode else self.tr("New Observation")
        )
        self.setModal(True)
        self.setMinimumSize(1100, 900)
        self.resize(1680, 1000)
        self._observation_datetime = _parse_observation_datetime(
            observation.get("date") if observation else None
        )
        self.image_files = []
        self.image_metadata = []
        self.image_settings = []
        self.selected_image_index = -1
        # Last coordinates applied from a photo's EXIF in this dialog, so a
        # save can attribute them as image_exif rather than manual entry.
        self._image_exif_coordinates: tuple[float, float] | None = None
        self._opened_coordinates: tuple = (None, None)
        self._opened_location_pair: tuple[str, str] | None = None
        self.objectives = self._load_objectives()
        self.default_objective = self._get_default_objective()
        self.contrast_options = self._load_tag_options("contrast")
        self.mount_options = self._load_tag_options("mount")
        self.stain_options = self._load_tag_options("stain")
        self.sample_options = self._load_tag_options("sample")
        self.contrast_default = self._preferred_tag_value(
            "contrast",
            self.contrast_options,
            DatabaseTerms.CONTRAST_METHODS[0],
        )
        self.mount_default = self._preferred_tag_value(
            "mount",
            self.mount_options,
            DatabaseTerms.MOUNT_MEDIA[0],
        )
        self.stain_default = self._preferred_tag_value(
            "stain",
            self.stain_options,
            DatabaseTerms.STAIN_TYPES[0],
        )
        self.sample_default = self._preferred_tag_value(
            "sample",
            self.sample_options,
            DatabaseTerms.SAMPLE_TYPES[0],
        )
        self.vernacular_db = None
        self._taxon_lookup = None
        self._vernacular_model = None
        self._vernacular_completer = None
        self._genus_model = None
        self._genus_completer = None
        self._species_model = None
        self._species_completer = None
        self._suppress_taxon_autofill = False
        self._host_suppress_taxon_autofill = False
        self._last_genus = ""
        self._last_species = ""
        self._ai_predictions_by_index: dict[int, list[dict]] = {}
        self._ai_selected_by_index: dict[int, dict] = {}
        self._inat_predictions_by_index: dict[int, list[dict]] = {}
        self._inat_selected_by_index: dict[int, dict] = {}
        self._ai_selected_taxon: dict | None = None
        self._selected_ai_source = "arts"
        self._current_ai_selected_fields = self._ai_selected_fields(
            self.observation if isinstance(self.observation, dict) and self.observation else self.draft_data
        )
        self._ai_thread = None
        self._inat_ai_thread = None
        self._artsobs_check_thread = None
        self._close_cleanup_done = False
        self._dialog_temp_preview_paths: set[str] = set()
        self._dialog_preview_path_cache: dict[str, str] = {}
        self._location_lookup_workers: set[QThread] = set()
        self._cloud_media_worker: QThread | None = None
        # Guard against duplicate launches for the current detail view.
        self._cloud_media_auto_start_attempted = False
        self._cloud_media_state: dict = {}
        self._cloud_privacy_usage_summary: dict = {}
        self._cloud_privacy_usage_summary_user_id = ""
        self._cloud_privacy_original_slot_consumption = 0
        self._ai_selected_index: int | None = None
        self._publish_target_manual_override = False
        self._publish_target_sync_in_progress = False
        self._location_lookup_name = ""
        self._location_lookup_suggestions: list[str] = []
        # ``_location_country_code`` holds the normalized (uppercase 2-letter)
        # country code — either an empty string when unknown or a validated
        # ISO 3166-1 alpha-2 code. ``_location_region_id`` is preserve-only
        # from cloud data; the desktop never invents it.
        self._location_country_code = ""
        self._location_country_name = ""
        self._location_region_id: str | None = None
        # Tracks the latitude/longitude the current in-memory country/region
        # were resolved from. Used to keep stale async lookup results from
        # overwriting geography associated with newly entered coordinates.
        self._location_country_coords: tuple[float, float] | None = None
        self._inaturalist_taxon_id: int | None = None
        self._red_list_category = ""
        self._red_list_categories: dict | None = None
        # Stage 3B.5: deferred red-list resolution guards.
        # ``_redlist_generation`` is the primary invalidation nonce — every
        # schedule increments it, and any observation load / identity
        # clear / dialog close also bumps it so in-flight callbacks whose
        # captured generation no longer matches are dropped. The identity
        # token remains as a defensive second check.
        self._pending_redlist_token: tuple | None = None
        self._last_applied_redlist_key: tuple | None = None
        self._redlist_generation: int = 0
        self._last_applied_location_lookup_name = ""
        self._force_apply_next_location_lookup_name = False
        self._sharing_scope_value = self._default_sharing_scope()
        self._debug_dialog_created_at = time.perf_counter()
        self._loading_form = True
        self._initial_gallery_refresh_pending = True
        self._deferred_location_lookup_pending = False
        self._dialog_gallery_splitter_syncing = False
        _debug_import_flow(
            f"ObservationDetailsDialog init start; edit_mode={self.edit_mode}; images={len(self.image_results)}"
        )
        self._apply_ai_state(ai_state)
        self.init_ui()
        if self.edit_mode:
            self._load_existing_observation()
        elif self.draft_data:
            self._load_observation_values(self.draft_data)
        else:
            self._set_publish_target_combo(self._active_reporting_target(), manual_override=False)
            self._set_sharing_scope(None)
            self._apply_primary_metadata()
            self._cloud_privacy_original_slot_consumption = 0
            self._update_cloud_privacy_slots_label()
        self._loading_form = False
        self._refresh_identity_offer()
        self._sync_taxon_picker(force=True)
        # Coordinates as opened: what get_data() compares against to decide
        # whether the user changed the location in this dialog.
        if self.edit_mode:
            self._opened_coordinates = (
                (observation or {}).get("gps_latitude"),
                (observation or {}).get("gps_longitude"),
            )
        else:
            self._opened_coordinates = self._current_coordinate_inputs()
        # The pair the opened record holds for those coordinates: a stored
        # attribution, or one made before an Edit Images detour (drafts).
        opened_record = (observation if self.edit_mode else self.draft_data) or {}
        opened_pair = provenance_pair(opened_record)
        self._opened_location_pair = (
            opened_pair
            if location_pair_is_writable(*opened_pair, *self._opened_coordinates)
            else None
        )
        # A new observation opens attributed to where Prepare Images got its
        # coordinates: an imported photo's own EXIF, or values typed there.
        # The photo's EXIF stays the reference for later edits in this dialog.
        # (Edits of existing observations keep the contract's rule: a Prepare
        # Images replacement is of unknown origin.)
        if not self.edit_mode:
            lat, lon = self._current_coordinate_inputs()
            exif_coordinates = self._imported_photo_exif_coordinates(lat, lon)
            if exif_coordinates is not None:
                self._image_exif_coordinates = exif_coordinates
            if self._opened_location_pair is None and lat is not None and lon is not None:
                if exif_coordinates is not None and coordinates_match(*exif_coordinates, lat, lon):
                    self._opened_location_pair = IMAGE_EXIF_LOCATION_PAIR
                elif self._typed_in_prepare_images(lat, lon):
                    self._opened_location_pair = MANUAL_LOCATION_PAIR
        QTimer.singleShot(0, self._complete_deferred_dialog_setup)
        self._apply_suggested_taxon()
        self._sync_taxon_cache()
        self._restore_geometry()
        self.finished.connect(self._save_geometry)

    def _dialog_gallery_default_height(self) -> int:
        gallery = getattr(self, "image_gallery", None)
        if gallery is None:
            return 120
        return max(gallery.minimumHeight(), 120)

    def _dialog_gallery_max_height(self) -> int:
        gallery = getattr(self, "image_gallery", None)
        if gallery is None:
            return 240
        return max(self._dialog_gallery_default_height(), gallery.maximum_useful_height() + 24)

    def _apply_dialog_gallery_splitter_height(self, gallery_height: int | None = None) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        gallery = getattr(self, "image_gallery", None)
        if splitter is None or gallery is None:
            return
        target = self._dialog_gallery_default_height() if gallery_height is None else int(gallery_height)
        target = max(gallery.minimumHeight(), min(self._dialog_gallery_max_height(), target))
        sizes = splitter.sizes()
        total = sum(sizes) if sizes else 0
        if total <= 0:
            total = splitter.height()
        if total <= 0:
            total = max(self.height(), target + 600)
        if self._dialog_gallery_splitter_syncing:
            return
        self._dialog_gallery_splitter_syncing = True
        try:
            splitter.setSizes([max(0, total - target), target])
        finally:
            self._dialog_gallery_splitter_syncing = False

    def _restore_dialog_gallery_splitter(self) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        raw_value = settings.value(self._gallery_splitter_key)
        parsed: list[int] = []
        if isinstance(raw_value, (list, tuple)):
            for value in raw_value[:2]:
                try:
                    parsed.append(max(0, int(value)))
                except Exception:
                    parsed.append(0)
            if len(parsed) >= 2 and sum(parsed[:2]) > 0:
                self._apply_dialog_gallery_splitter_height(parsed[1])
                return
        else:
            try:
                gallery_height = max(0, int(raw_value))
            except Exception:
                gallery_height = None
            self._apply_dialog_gallery_splitter_height(gallery_height)
            return
        self._apply_dialog_gallery_splitter_height(None)

    def _save_dialog_gallery_splitter(self) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is None:
            return
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue(self._gallery_splitter_key, splitter.sizes())

    def _restore_taxonomy_splitter(self) -> None:
        splitter = getattr(self, "taxonomy_row_splitter", None)
        if splitter is None:
            return
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        raw_value = settings.value(self._taxonomy_splitter_key)
        if not isinstance(raw_value, (list, tuple)):
            self._apply_taxonomy_splitter_sizes()
            return
        parsed: list[int] = []
        for value in raw_value[:2]:
            try:
                parsed.append(max(0, int(value)))
            except Exception:
                parsed.append(0)
        if len(parsed) >= 2 and sum(parsed[:2]) > 0:
            self._apply_taxonomy_splitter_sizes(parsed[0], parsed[1])
        else:
            self._apply_taxonomy_splitter_sizes()

    def _apply_taxonomy_splitter_sizes(
        self,
        taxonomy_width: int | None = None,
        ai_width: int | None = None,
    ) -> None:
        splitter = getattr(self, "taxonomy_row_splitter", None)
        if splitter is None:
            return
        sizes = splitter.sizes()
        total = sum(sizes) if sizes else 0
        if total <= 0:
            total = splitter.width()
        if total <= 0:
            total = max(900, self.width() - 80)

        taxonomy_min = 360
        ai_min = 300
        if total <= taxonomy_min + ai_min:
            splitter.setSizes([max(1, int(total * 0.6)), max(1, int(total * 0.4))])
            return

        if taxonomy_width is None or ai_width is None or taxonomy_width + ai_width <= 0:
            taxonomy_width = int(total * 0.62)
            ai_width = total - taxonomy_width

        requested_total = taxonomy_width + ai_width
        if requested_total > 0 and requested_total != total:
            taxonomy_width = int(total * (taxonomy_width / requested_total))
            ai_width = total - taxonomy_width

        ai_width = max(ai_min, min(total - taxonomy_min, int(ai_width)))
        taxonomy_width = total - ai_width
        splitter.setSizes([taxonomy_width, ai_width])

    def _save_taxonomy_splitter(self) -> None:
        splitter = getattr(self, "taxonomy_row_splitter", None)
        if splitter is None:
            return
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue(self._taxonomy_splitter_key, splitter.sizes())

    def _on_dialog_gallery_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is None or self._dialog_gallery_splitter_syncing:
            return
        QTimer.singleShot(0, self._save_dialog_gallery_splitter)

    def _on_taxonomy_splitter_moved(self, _pos: int, _index: int) -> None:
        QTimer.singleShot(0, self._save_taxonomy_splitter)

    def _restore_geometry(self) -> None:
        super()._restore_geometry()
        QTimer.singleShot(0, self._restore_dialog_gallery_splitter)
        QTimer.singleShot(0, self._restore_taxonomy_splitter)

    def _save_geometry(self) -> None:
        self._save_dialog_gallery_splitter()
        self._save_taxonomy_splitter()
        super()._save_geometry()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)

        top_content = QWidget(self)
        top_content_layout = QVBoxLayout(top_content)
        top_content_layout.setContentsMargins(0, 0, 0, 0)
        top_content_layout.setSpacing(10)

        # ===== OBSERVATION DETAILS SECTION =====
        details_group, details_layout = self._create_dialog_box(
            self.tr("Observation Details"),
            QHBoxLayout,
            body_margins=(12, 12, 12, 12),
            body_spacing=12,
        )
        details_group.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        details_layout.setSpacing(12)
        details_layout.setAlignment(Qt.AlignTop)

        left_panel = QWidget()
        left_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        left_layout = QGridLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setHorizontalSpacing(8)
        left_layout.setVerticalSpacing(8)

        right_panel = QWidget()
        right_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        self.is_draft_checkbox = QCheckBox(self.tr("Draft / WIP"))
        self.is_draft_checkbox.setToolTip(self.tr("Draft observations are visible as work in progress until you mark them finished."))
        self.is_draft_checkbox.setChecked(True)
        _details_row = 0

        # Date and time
        datetime_container = QWidget()
        datetime_layout = QHBoxLayout(datetime_container)
        datetime_layout.setContentsMargins(0, 0, 0, 0)
        self.datetime_input = QDateTimeEdit()
        self.datetime_input.setDateTime(QDateTime.currentDateTime())
        self.datetime_input.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.datetime_input.setCalendarPopup(True)
        _cal = self.datetime_input.calendarWidget()
        _cal.setHorizontalHeaderFormat(_cal.HorizontalHeaderFormat.SingleLetterDayNames)
        _cal.setMinimumSize(300, 240)
        self.datetime_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        datetime_layout.addWidget(self.datetime_input, 1)
        left_layout.addWidget(QLabel(self.tr("Date & time:")), _details_row, 0)
        left_layout.addWidget(datetime_container, _details_row, 1, 1, 2)
        _details_row += 1

        # GPS fields split across two rows with the Map button spanning both rows.
        self.lat_input = QDoubleSpinBox()
        self.lat_input.setRange(-90.0, 90.0)
        self.lat_input.setDecimals(6)
        self.lat_input.setSpecialValueText("--")
        self.lat_input.setValue(self.lat_input.minimum())
        self.lat_label = QLabel(self.tr("Latitude:"))

        self.lon_input = QDoubleSpinBox()
        self.lon_input.setRange(-180.0, 180.0)
        self.lon_input.setDecimals(6)
        self.lon_input.setSpecialValueText("--")
        self.lon_input.setValue(self.lon_input.minimum())
        self.lon_label = QLabel(self.tr("Longitude:"))

        # Map button - opens location in browser
        self.map_btn = QPushButton(self.tr("Map"))
        self.map_btn.setMinimumWidth(64)
        self.map_btn.clicked.connect(self.open_map)
        self.map_btn.setEnabled(False)

        left_layout.addWidget(self.lat_label, _details_row, 0)
        left_layout.addWidget(self.lat_input, _details_row, 1)
        left_layout.addWidget(self.map_btn, _details_row, 2, 2, 1, Qt.AlignVCenter)
        _details_row += 1
        left_layout.addWidget(self.lon_label, _details_row, 0)
        left_layout.addWidget(self.lon_input, _details_row, 1)
        _details_row += 1

        maplink_container = QWidget()
        maplink_layout = QHBoxLayout(maplink_container)
        maplink_layout.setContentsMargins(0, 0, 0, 0)
        self.maplink_input = QLineEdit()
        self.maplink_input.setPlaceholderText(self.tr("Paste OpenStreetMap link"))
        self.maplink_input.setClearButtonEnabled(True)
        self.maplink_input.textChanged.connect(self._on_map_link_changed)
        self.maplink_open_btn = QPushButton(self.tr("Get map link"))
        self.maplink_open_btn.clicked.connect(self._open_map_url)
        maplink_layout.addWidget(self.maplink_input, 1)
        maplink_layout.addWidget(self.maplink_open_btn)
        self.paste_link_label = QLabel(self.tr("Paste link:"))
        left_layout.addWidget(self.paste_link_label, _details_row, 0)
        left_layout.addWidget(maplink_container, _details_row, 1, 1, 2)
        _details_row += 1

        # Enable map button when coordinates are manually changed
        self.lat_input.valueChanged.connect(self._update_map_button)
        self.lon_input.valueChanged.connect(self._update_map_button)
        self.lat_input.valueChanged.connect(self._maybe_autoselect_publish_target_from_coords)
        self.lon_input.valueChanged.connect(self._maybe_autoselect_publish_target_from_coords)

        # Location lookup from coordinates (debounced)
        self._location_lookup_timer = QTimer(self)
        self._location_lookup_timer.setSingleShot(True)
        self._location_lookup_timer.setInterval(600)
        self._location_lookup_timer.timeout.connect(self._do_location_lookup)
        self._location_lookup_worker = None
        self.lat_input.valueChanged.connect(self._schedule_location_lookup)
        self.lon_input.valueChanged.connect(self._schedule_location_lookup)

        # Location (text)
        location_container = QWidget()
        location_layout = QHBoxLayout(location_container)
        location_layout.setContentsMargins(0, 0, 0, 0)
        location_layout.setSpacing(6)
        self.location_input = QLineEdit()
        self.location_input.setPlaceholderText(self.tr("e.g., Bymarka, Trondheim"))
        self.location_input.installEventFilter(self)
        self._location_suggestions_model = QStringListModel(self)
        self._location_suggestions_completer = QCompleter(self._location_suggestions_model, self)
        self._location_suggestions_completer.setCompletionMode(QCompleter.UnfilteredPopupCompletion)
        self._location_suggestions_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._location_suggestions_completer.activated.connect(self._on_location_suggestion_selected)
        self.location_input.setCompleter(self._location_suggestions_completer)
        location_layout.addWidget(self.location_input, 1)
        left_layout.addWidget(QLabel(self.tr("Location:")), _details_row, 0)
        left_layout.addWidget(location_container, _details_row, 1, 1, 2)
        _details_row += 1

        self.country_summary_label = QLabel("")
        self.country_summary_label.setStyleSheet("color: #2c3e50;")
        self.country_summary_label.setVisible(False)
        left_layout.addWidget(self.country_summary_label, _details_row, 1, 1, 2)
        _details_row += 1

        cloud_divider = QFrame()
        cloud_divider.setFrameShape(QFrame.HLine)
        cloud_divider.setFixedHeight(1)
        data_brd = get_design_tokens()["data_brd"]
        cloud_divider.setStyleSheet(
            f"QFrame {{ height: 1px; max-height: 1px; color: {data_brd}; background-color: {data_brd}; border: none; }}"
        )
        left_layout.addWidget(cloud_divider, _details_row, 0, 1, 3)
        _details_row += 1

        cloud_controls = QWidget()
        cloud_controls.setObjectName("observationCloudControls")
        self._cloud_controls = cloud_controls
        cloud_layout = QVBoxLayout(cloud_controls)
        cloud_layout.setContentsMargins(0, 0, 0, 0)
        cloud_layout.setSpacing(6)

        cloud_title = QLabel(self.tr("Sporely Cloud").upper())
        cloud_title.setObjectName("metaLabel")
        cloud_layout.addWidget(cloud_title)

        self.sharing_scope_selector = SegmentedSelector(self, compact=True)
        self.sharing_scope_private_radio = self.sharing_scope_selector.add_option(
            self.tr("Private"),
            SHARING_SCOPE_PRIVATE,
            checked=True,
            tooltip=self.tr("Private observations are visible only to you."),
        )
        self.sharing_scope_friends_radio = self.sharing_scope_selector.add_option(
            self.tr("Friends"),
            SHARING_SCOPE_FRIENDS,
            tooltip=self.tr("Friends-only observations consume one privacy slot."),
        )
        self.sharing_scope_public_radio = self.sharing_scope_selector.add_option(
            self.tr("Public"),
            SHARING_SCOPE_PUBLIC,
            tooltip=self.tr("Public observations do not use a privacy slot."),
        )
        self.sharing_scope_selector.selectionChanged.connect(
            lambda value: setattr(self, "_sharing_scope_value", normalize_sharing_scope(value))
        )
        self.sharing_scope_selector.selectionChanged.connect(
            lambda _value: self._update_cloud_privacy_slots_label()
        )

        self.location_precision_selector = SegmentedSelector(self, compact=True)
        self.location_precision_exact_radio = self.location_precision_selector.add_option(
            self.tr("Exact"),
            LOCATION_PRECISION_EXACT,
            checked=True,
        )
        self.location_precision_fuzzed_radio = self.location_precision_selector.add_option(
            self.tr("Fuzzed"),
            LOCATION_PRECISION_FUZZED,
            tooltip=self.tr("Fuzzed locations are rounded in public and follow feeds."),
        )
        self.location_precision_selector.selectionChanged.connect(
            self._on_location_precision_selected
        )
        self.location_precision_selector.selectionChanged.connect(
            lambda _value: self._update_cloud_privacy_slots_label()
        )

        cloud_grid = QGridLayout()
        cloud_grid.setContentsMargins(0, 0, 0, 0)
        cloud_grid.setHorizontalSpacing(18)
        cloud_grid.setVerticalSpacing(4)

        share_label = QLabel(self.tr("Share with.."), cloud_controls)
        precision_label = QLabel(self.tr("Location precision:"), cloud_controls)
        cloud_grid.addWidget(share_label, 0, 0, Qt.AlignLeft)
        cloud_grid.addWidget(precision_label, 0, 1, Qt.AlignLeft)
        cloud_grid.addWidget(self.sharing_scope_selector, 1, 0, Qt.AlignLeft | Qt.AlignTop)
        cloud_grid.addWidget(self.location_precision_selector, 1, 1, Qt.AlignLeft | Qt.AlignTop)

        draft_container = QWidget(cloud_controls)
        draft_container.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        draft_container_layout = QVBoxLayout(draft_container)
        draft_container_layout.setContentsMargins(0, 0, 0, 0)
        draft_container_layout.setSpacing(0)
        self.is_draft_checkbox.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        draft_container_layout.addWidget(self.is_draft_checkbox, 0, Qt.AlignLeft | Qt.AlignVCenter)
        cloud_grid.addWidget(draft_container, 1, 2, Qt.AlignLeft | Qt.AlignVCenter)

        cloud_layout.addLayout(cloud_grid)
        self.cloud_privacy_slots_label = QLabel(self.tr("Sign in to Sporely Cloud to see private slot availability."))
        self.cloud_privacy_slots_label.setWordWrap(True)
        self.cloud_privacy_slots_label.setStyleSheet("color: #6b7280;")
        cloud_layout.addWidget(self.cloud_privacy_slots_label)
        self._update_cloud_privacy_slots_label()

        left_layout.addWidget(cloud_controls, _details_row, 0, 1, 3)
        _details_row += 1

        comments_container = QWidget()
        comments_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        comments_layout = QVBoxLayout(comments_container)
        comments_layout.setContentsMargins(0, 0, 0, 0)
        comments_layout.setSpacing(8)

        open_comment_container = QWidget()
        open_comment_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        open_comment_layout = QVBoxLayout(open_comment_container)
        open_comment_layout.setContentsMargins(0, 0, 0, 0)
        open_comment_layout.setSpacing(6)

        open_comment_label = QLabel(self.tr("Open comment:"))
        open_comment_layout.addWidget(open_comment_label)

        self.open_comment_input = QTextEdit()
        self.open_comment_input.setPlaceholderText(self.tr("Open comment..."))
        self.open_comment_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        open_comment_layout.addWidget(self.open_comment_input, 1)
        comments_layout.addWidget(open_comment_container, 1)

        private_comment_container = QWidget()
        private_comment_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        private_comment_layout = QVBoxLayout(private_comment_container)
        private_comment_layout.setContentsMargins(0, 0, 0, 0)
        private_comment_layout.setSpacing(6)

        private_comment_label = QLabel(self.tr("Private comment:"))
        private_comment_layout.addWidget(private_comment_label)

        self.private_comment_input = QTextEdit()
        self.private_comment_input.setPlaceholderText(self.tr("Private comment..."))
        self.private_comment_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        private_comment_layout.addWidget(self.private_comment_input, 1)
        comments_layout.addWidget(private_comment_container, 1)
        right_layout.addWidget(comments_container, 1)

        # GPS info label (shows source of coordinates)
        self.gps_info_label = QLabel("")
        self.gps_info_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        left_layout.addWidget(self.gps_info_label, _details_row, 1, 1, 2)
        left_layout.setColumnStretch(1, 1)

        # Put location/date/GPS on the left, and comments on the right (about 40/60 split).
        details_layout.addWidget(left_panel, 4)
        details_layout.addWidget(right_panel, 6)
        top_content_layout.addWidget(details_group, 1)

        # ===== TAXONOMY SECTION =====
        taxonomy_group, taxonomy_layout = self._create_dialog_box(
            self.tr("Taxonomy"),
            QVBoxLayout,
            body_margins=(0, 0, 0, 0),
            body_spacing=0,
        )
        taxonomy_group.setMinimumWidth(360)
        taxonomy_group.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)

        left_container = QWidget()
        left_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        left_container.setMinimumWidth(0)
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(0)

        # Taxonomy tab widget (Species + Biotope + Grows on)
        self.taxonomy_tabs = QTabWidget()
        self.taxonomy_tabs.setObjectName("taxonomyTabs")
        self.taxonomy_tabs.setDocumentMode(True)
        self.taxonomy_tabs.setMinimumHeight(120)
        self.taxonomy_tabs.setMinimumWidth(0)
        try:
            self.taxonomy_tabs.tabBar().setDrawBase(False)
        except Exception:
            pass
        self.taxonomy_tabs.currentChanged.connect(self.on_taxonomy_tab_changed)

        # Tab 1: Identified (vernacular + genus/species)
        identified_tab = QWidget()
        identified_tab.setObjectName("taxonomyPage")
        identified_layout = QVBoxLayout(identified_tab)
        identified_layout.setContentsMargins(8, 8, 8, 8)
        identified_layout.setSpacing(4)
        identified_layout.setAlignment(Qt.AlignTop)
        self._taxonomy_label_width = 96
        taxonomy_label_width = self._taxonomy_label_width

        # The unified taxon picker is what the user sees. The vernacular,
        # genus, species and scientific inputs stay as hidden backing state:
        # the taxon controller, the save path and the Artsorakel copy read
        # and write them as before.
        self._taxon_backing = QWidget(identified_tab)
        self._taxon_backing.setVisible(False)
        backing = self._taxon_backing
        self.vernacular_label = QLabel(self._vernacular_label(), backing)
        self.vernacular_input = QLineEdit(backing)
        self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.vernacular_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.genus_input = QLineEdit(backing)
        self.genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.genus_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.species_input = QLineEdit(backing)
        self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
        self.species_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.scientific_name_input = QLineEdit(backing)
        self.scientific_name_input.setPlaceholderText(
            self.tr("Optional: e.g., Hygrocybe conica var. pseudoconica")
        )
        # The card names a chosen synonym's stored entry itself.
        self.scientific_hint_label = QLabel("", backing)
        self.scientific_hint_label.setVisible(False)

        self._taxon_picker_sync_timer = QTimer(self)
        self._taxon_picker_sync_timer.setSingleShot(True)
        self._taxon_picker_sync_timer.timeout.connect(self._sync_taxon_picker)
        for widget in (self.vernacular_input, self.genus_input, self.species_input, self.scientific_name_input):
            widget.textChanged.connect(self._schedule_taxon_picker_sync)

        taxon_row = QHBoxLayout()
        taxon_row.setContentsMargins(0, 0, 0, 0)
        taxon_row.setSpacing(4)
        taxon_label = QLabel(self.tr("Species:"))
        taxon_label.setFixedWidth(taxonomy_label_width)
        taxon_row.addWidget(taxon_label, 0, Qt.AlignTop)
        self.taxon_picker = TaxonPicker()
        self.taxon_picker.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        taxon_label.setMinimumHeight(self.taxon_picker.search_input.sizeHint().height())
        self.taxon_picker.concept_picked.connect(self._on_taxon_picker_concept)
        self.taxon_picker.free_text_committed.connect(self._on_taxon_picker_free_name)
        self.taxon_picker.cleared.connect(self._on_taxon_picker_cleared)
        taxon_row.addWidget(self.taxon_picker, 1)
        # The language of common names, for the search and the stored name.
        self.vernacular_language_btn = QToolButton()
        self.vernacular_language_btn.setText("🌐")
        self.vernacular_language_btn.setPopupMode(QToolButton.InstantPopup)
        self.vernacular_language_menu = QMenu(self.vernacular_language_btn)
        self.vernacular_language_btn.setMenu(self.vernacular_language_menu)
        input_height = max(18, int(self.taxon_picker.search_input.sizeHint().height()))
        self.vernacular_language_btn.setFixedSize(input_height, input_height)
        self._populate_vernacular_language_menu()
        taxon_row.addWidget(self.vernacular_language_btn, 0, Qt.AlignTop)
        identified_layout.addLayout(taxon_row)

        # Offered, never applied: a provider identifier that resolves to one
        # Sporely taxon matching the entered name, for an observation without
        # a proven identity. A one-line hint with Link; only the button
        # binds it.
        offer_row = QHBoxLayout()
        offer_row.setContentsMargins(0, 0, 0, 0)
        offer_row.setSpacing(4)
        offer_row.addSpacing(taxonomy_label_width + 4)
        self.identity_offer_row = QLabel("")
        self.identity_offer_row.setWordWrap(True)
        self.identity_offer_row.setStyleSheet("color: #6b7280; font-size: 11px;")
        self.identity_offer_row.setVisible(False)
        self.identity_offer_label = self.identity_offer_row
        offer_row.addWidget(self.identity_offer_row, 1)
        self.identity_offer_button = QPushButton(self.tr("Link"))
        self.identity_offer_button.clicked.connect(self._on_identity_offer_accepted)
        self.identity_offer_button.setVisible(False)
        offer_row.addWidget(self.identity_offer_button, 0, Qt.AlignVCenter)
        self._identity_offer = None
        identified_layout.addLayout(offer_row)

        red_list_row = QHBoxLayout()
        red_list_label = QLabel(self.tr("Red list:"))
        red_list_label.setFixedWidth(taxonomy_label_width)
        red_list_row.addWidget(red_list_label)
        from ui.delegates import RedListCircleWidget
        self.red_list_badge_widget = RedListCircleWidget()
        badge_size = max(24, int(self.taxon_picker.search_input.sizeHint().height()))
        self.red_list_badge_widget.setFixedSize(48, badge_size)
        self.red_list_badge_widget.setVisible(False)
        red_list_row.addWidget(self.red_list_badge_widget, 0, Qt.AlignVCenter)
        self.red_list_status_label = QLabel("")
        self.red_list_status_label.setWordWrap(False)
        self.red_list_status_label.setStyleSheet("color: #6b7280; font-size: 11px;")
        self.red_list_status_label.setMinimumHeight(badge_size)
        red_list_row.addWidget(self.red_list_status_label, 1, Qt.AlignVCenter)
        identified_layout.addLayout(red_list_row)

        determination_row = QHBoxLayout()
        determination_label = QLabel(self.tr("Determination:"))
        determination_label.setFixedWidth(taxonomy_label_width)
        determination_row.addWidget(determination_label)
        self.determination_method_combo = QComboBox()
        self.determination_method_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.determination_method_combo.addItem("", None)
        self.determination_method_combo.addItem(self.tr("Microscopy"), 1)
        self.determination_method_combo.addItem(self.tr("Sequencing"), 2)
        self.determination_method_combo.addItem(self.tr("eDNA"), 3)
        self.determination_method_combo.setToolTip(self.tr("Optional method used for determination."))
        self._style_dropdown_popup_readability(self.determination_method_combo.view(), self.determination_method_combo)
        self.determination_method_combo.currentIndexChanged.connect(self._update_taxonomy_tab_indicators)
        determination_row.addWidget(self.determination_method_combo, 1)
        identified_layout.addLayout(determination_row)

        self.ai_selected_summary_label = QLabel("")
        self.ai_selected_summary_label.setWordWrap(True)
        self.ai_selected_summary_label.setStyleSheet("color: #6b7280; font-size: 11px;")
        self.ai_selected_summary_label.setVisible(False)
        identified_layout.addWidget(self.ai_selected_summary_label)

        self.publish_target_combo = QComboBox()
        self.publish_target_combo.addItem(self.tr("Artsobservasjoner (Norway)"), PUBLISH_TARGET_ARTSOBS_NO)
        self.publish_target_combo.addItem(self.tr("Artportalen (Sweden)"), PUBLISH_TARGET_ARTPORTALEN_SE)
        self.publish_target_combo.currentIndexChanged.connect(self._on_publish_target_changed)

        self.publish_target_hint = QLabel()
        self.publish_target_hint.setWordWrap(True)
        self.publish_target_hint.setStyleSheet("color: #6b7280; font-size: 11px;")
        self.publish_target_hint.setVisible(False)

        unidentified_row = QHBoxLayout()
        unidentified_row.setContentsMargins(0, 0, 0, 0)
        unidentified_row.setSpacing(4)
        unidentified_row.addSpacing(taxonomy_label_width)
        self.unidentified_checkbox = QCheckBox(self.tr("Unidentified"))
        self.unidentified_checkbox.toggled.connect(self._on_unidentified_toggled)
        unidentified_row.addWidget(self.unidentified_checkbox)
        unidentified_row.addStretch()
        identified_layout.addLayout(unidentified_row)

        uncertain_row = QHBoxLayout()
        uncertain_row.setContentsMargins(0, 0, 0, 0)
        uncertain_row.setSpacing(4)
        uncertain_row.addSpacing(taxonomy_label_width)
        self.uncertain_checkbox = QCheckBox(self.tr("Uncertain"))
        self.uncertain_checkbox.toggled.connect(self._update_taxonomy_tab_indicators)
        self.uncertain_checkbox.toggled.connect(self.taxon_picker.set_uncertain)
        uncertain_row.addWidget(self.uncertain_checkbox)
        uncertain_row.addStretch()
        identified_layout.addLayout(uncertain_row)

        unspontaneous_row = QHBoxLayout()
        unspontaneous_row.setContentsMargins(0, 0, 0, 0)
        unspontaneous_row.setSpacing(4)
        unspontaneous_row.addSpacing(taxonomy_label_width)
        self.unspontaneous_checkbox = QCheckBox(self.tr("Alien or cultivated"))
        self.unspontaneous_checkbox.toggled.connect(self._update_taxonomy_tab_indicators)
        unspontaneous_row.addWidget(self.unspontaneous_checkbox)
        unspontaneous_row.addStretch()
        identified_layout.addLayout(unspontaneous_row)

        identified_layout.addStretch(1)

        # The page scrolls rather than squashing the identification card when
        # the taxonomy pane is short.
        species_page = QScrollArea()
        species_page.setObjectName("taxonomyPage")
        species_page.setFrameShape(QFrame.NoFrame)
        species_page.setWidgetResizable(True)
        species_page.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        species_page.setWidget(identified_tab)
        self.species_tab = species_page
        self.taxonomy_tabs.addTab(species_page, self.tr("Species"))

        # Tab 2: Biotope metadata
        self._habitat_tree_states: dict[str, dict] = {}
        nin2_nodes = self._load_habitat_tree("nin2_biotopes_tree.json")
        nin2_tab = QWidget()
        nin2_tab.setObjectName("taxonomyPage")
        self.nin2_tab = nin2_tab
        nin2_layout = QVBoxLayout(nin2_tab)
        nin2_layout.setContentsMargins(8, 8, 8, 8)
        nin2_layout.setSpacing(8)
        nin2_layout.setAlignment(Qt.AlignTop)
        self._create_habitat_tree_controls(
            nin2_layout,
            self.tr("NIN2 biotope"),
            "nin2",
            nin2_nodes,
            expand_fields=True,
        )
        self.nin2_target_note = QLabel("")
        self.nin2_target_note.setWordWrap(True)
        self.nin2_target_note.setStyleSheet("color: #6b7280; font-size: 11px;")
        self._habitat_tree_states["nin2"]["note_label"] = self.nin2_target_note
        nin2_layout.addWidget(self.nin2_target_note)
        self.nin2_note_input = self._make_note_input()
        self.nin2_note_input.setPlaceholderText(self.tr("Biotope note..."))
        self.nin2_note_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        nin2_layout.addWidget(QLabel(self.tr("Biotope note:")))
        nin2_layout.addWidget(self.nin2_note_input)
        nin2_layout.addStretch(1)
        self.taxonomy_tabs.addTab(nin2_tab, self.tr("Biotope"))

        # Tab 3: Substrate metadata
        substrate_nodes = self._load_habitat_tree("substrate_tree.json")
        substrate_tab = QWidget()
        substrate_tab.setObjectName("taxonomyPage")
        self.substrate_tab = substrate_tab
        substrate_layout = QVBoxLayout(substrate_tab)
        substrate_layout.setContentsMargins(8, 8, 8, 8)
        substrate_layout.setSpacing(8)
        substrate_layout.setAlignment(Qt.AlignTop)
        self._create_habitat_tree_controls(
            substrate_layout,
            self.tr("Substrate"),
            "substrate",
            substrate_nodes,
        )
        self.substrate_target_note = QLabel("")
        self.substrate_target_note.setWordWrap(True)
        self.substrate_target_note.setStyleSheet("color: #6b7280; font-size: 11px;")
        self._habitat_tree_states["substrate"]["note_label"] = self.substrate_target_note
        substrate_layout.addWidget(self.substrate_target_note)
        self.substrate_note_input = self._make_note_input()
        self.substrate_note_input.setPlaceholderText(self.tr("Substrate note..."))
        self.substrate_note_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        substrate_layout.addWidget(QLabel(self.tr("Substrate note:")))
        substrate_layout.addWidget(self.substrate_note_input)
        substrate_layout.addStretch(1)
        self.taxonomy_tabs.addTab(substrate_tab, self.tr("Substrate"))

        # Tab 4: Grows-on species metadata
        grows_tab = QWidget()
        grows_tab.setObjectName("taxonomyPage")
        self.grows_tab = grows_tab
        grows_tab_layout = QVBoxLayout(grows_tab)
        grows_tab_layout.setContentsMargins(8, 8, 8, 8)
        grows_tab_layout.setSpacing(8)
        grows_tab_layout.setAlignment(Qt.AlignTop)
        grows_group, grows_layout = self._create_dialog_box(
            self._grows_on_tab_title(),
            QVBoxLayout,
            body_margins=(8, 8, 8, 8),
            body_spacing=6,
        )
        self.host_genus_input = QLineEdit()
        self.host_genus_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.host_genus_input.setPlaceholderText(self.tr("e.g., Betula"))
        self.host_genus_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_species_input = QLineEdit()
        self.host_species_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.host_species_input.setPlaceholderText(self.tr("e.g., pendula"))
        self.host_species_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_vernacular_input = QLineEdit()
        self.host_vernacular_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.host_vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.host_vernacular_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_vernacular_label = QLabel(self._vernacular_label())

        def _add_grows_row(label_text: str, widget: QWidget) -> None:
            row = QHBoxLayout()
            row.setContentsMargins(0, 0, 0, 0)
            row.setSpacing(4)
            label = QLabel(label_text)
            label.setFixedWidth(taxonomy_label_width)
            row.addWidget(label)
            row.addWidget(widget, 1)
            grows_layout.addLayout(row)

        _add_grows_row(self.tr("Genus:"), self.host_genus_input)
        _add_grows_row(self.tr("Species:"), self.host_species_input)
        _add_grows_row(self.host_vernacular_label.text(), self.host_vernacular_input)
        grows_tab_layout.addWidget(grows_group)
        self.grows_on_note_input = self._make_note_input()
        self.grows_on_note_input.setPlaceholderText(self.tr("Grows-on note..."))
        self.grows_on_note_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        grows_tab_layout.addWidget(QLabel(self.tr("Grows-on note:")))
        grows_tab_layout.addWidget(self.grows_on_note_input)
        grows_tab_layout.addStretch(1)
        self.taxonomy_tabs.addTab(grows_tab, self._grows_on_tab_title())

        left_layout.addWidget(self.taxonomy_tabs)
        taxonomy_layout.addWidget(left_container)

        self.ai_group = self._build_ai_suggestions_group()
        self.taxonomy_row_splitter = QSplitter(Qt.Horizontal)
        self.taxonomy_row_splitter.setChildrenCollapsible(False)
        self.taxonomy_row_splitter.setHandleWidth(8)
        self.taxonomy_row_splitter.addWidget(taxonomy_group)
        self.taxonomy_row_splitter.addWidget(self.ai_group)
        self.taxonomy_row_splitter.setStretchFactor(0, 3)
        self.taxonomy_row_splitter.setStretchFactor(1, 2)
        self._apply_taxonomy_splitter_sizes()
        self.taxonomy_row_splitter.splitterMoved.connect(self._on_taxonomy_splitter_moved)
        top_content_layout.addWidget(self.taxonomy_row_splitter)

        # ===== IMAGES SUMMARY (BOTTOM) =====
        self.image_gallery = ImageGalleryWidget(
            "",
            self,
            show_delete=True,
            show_badges=True,
            show_edit=True,
            show_delete_cloud_copy=True,
            thumbnail_size=140,
            min_height=90,
            default_height=120,
            thumbnail_tooltip=self.tr("Double-click to open"),
        )
        self.image_gallery.set_plain_container(True)
        self.image_gallery.set_compact_overlay(True)
        self.image_gallery.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.image_gallery.set_reorderable(True)
        self.image_gallery.set_multi_select(True)
        self._gps_source_index = self._resolve_gps_source_index()
        self.image_gallery.imageClicked.connect(self._on_gallery_image_clicked)
        self.image_gallery.imageSelected.connect(self._on_gallery_image_clicked)
        self.image_gallery.deleteImagesRequested.connect(self._on_details_dialog_gallery_delete_images_requested)
        self.image_gallery.deleteCloudCopiesRequested.connect(
            lambda ids, g=self.image_gallery: self._on_gallery_delete_cloud_copies_requested(g, ids)
        )
        self.image_gallery.editRequested.connect(self._on_gallery_edit_requested)
        self.image_gallery.imageDoubleClicked.connect(self._on_image_double_clicked)
        self.image_gallery.itemsReordered.connect(self._on_gallery_items_reordered)
        self.image_gallery.selectionChanged.connect(self._on_details_dialog_gallery_selection_changed)

        self.dialog_gallery_splitter = QSplitter(Qt.Vertical)
        self.dialog_gallery_splitter.setChildrenCollapsible(False)
        self.dialog_gallery_splitter.addWidget(top_content)
        self.dialog_gallery_splitter.addWidget(self.image_gallery)
        self.dialog_gallery_splitter.setStretchFactor(0, 1)
        self.dialog_gallery_splitter.setStretchFactor(1, 0)
        self.dialog_gallery_splitter.setSizes([760, self._dialog_gallery_default_height()])
        self.dialog_gallery_splitter.splitterMoved.connect(self._on_dialog_gallery_splitter_moved)
        main_layout.addWidget(self.dialog_gallery_splitter, 1)

        # ===== BOTTOM BUTTONS =====
        bottom_buttons = QHBoxLayout()
        self.hint_bar = HintBar(self)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        bottom_buttons.addWidget(self.hint_bar, 1)
        self.download_cloud_media_btn = QPushButton(self.tr("Download media"))
        self.download_cloud_media_btn.setFixedHeight(35)
        self.download_cloud_media_btn.setMinimumWidth(120)
        self.download_cloud_media_btn.setVisible(False)
        self.download_cloud_media_btn.clicked.connect(self._download_cloud_media_clicked)
        bottom_buttons.addWidget(self.download_cloud_media_btn, 0)
        bottom_buttons.addWidget(make_github_help_button(self, "observation-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
        _edit_key = "⌘E" if sys.platform == "darwin" else "Alt-E"
        self._edit_key_label = _edit_key
        if self.allow_edit_images:
            self.edit_images_btn = QPushButton(
                self.tr("Edit images ({key})").format(key=_edit_key)
            )
            self.edit_images_btn.setFixedHeight(35)
            self.edit_images_btn.setMinimumWidth(120)
            self.edit_images_btn.clicked.connect(self._on_edit_images_clicked)
            bottom_buttons.addWidget(self.edit_images_btn)
        cancel_btn = QPushButton(self.tr("Cancel"))
        cancel_btn.setFixedHeight(35)
        cancel_btn.setStyleSheet("background-color: #e74c3c;")
        cancel_btn.clicked.connect(self.reject)
        bottom_buttons.addWidget(cancel_btn)
        self.cancel_btn = cancel_btn
        self.submit_observation_btn = QPushButton(
            self.tr("Save Observation") if self.edit_mode else self.tr("Create Observation")
        )
        self.submit_observation_btn.setObjectName("dialogPrimaryButton")
        self.submit_observation_btn.setFixedHeight(35)
        self.submit_observation_btn.setAutoDefault(True)
        self.submit_observation_btn.setDefault(True)
        self.submit_observation_btn.clicked.connect(self.accept)
        bottom_buttons.addWidget(self.submit_observation_btn)
        self._bottom_buttons = bottom_buttons
        # The form and its images (the splitter, stretch 1) take any spare
        # height; the bottom row keeps its own. (Stretches left from an older
        # three-row layout gave the bottom row most of it: a tall empty band.)
        main_layout.addLayout(bottom_buttons, 0)

        self._setup_vernacular_autocomplete()
        self._setup_host_autocomplete()

        self.on_taxonomy_tab_changed(self.taxonomy_tabs.currentIndex())
        self._select_initial_ai_image()
        self._update_ai_controls_state()
        self._update_ai_table()
        self._update_datetime_width()
        self._register_dialog_hints()
        self._init_submit_shortcuts()
        self._update_taxonomy_tab_indicators()
        self._update_publish_target_specific_controls()

    def _init_submit_shortcuts(self) -> None:
        self._submit_shortcut_return = QShortcut(QKeySequence(Qt.Key_Return), self)
        self._submit_shortcut_return.setContext(Qt.WidgetWithChildrenShortcut)
        self._submit_shortcut_return.activated.connect(self._submit_dialog_from_enter)

        self._submit_shortcut_enter = QShortcut(QKeySequence(Qt.Key_Enter), self)
        self._submit_shortcut_enter.setContext(Qt.WidgetWithChildrenShortcut)
        self._submit_shortcut_enter.activated.connect(self._submit_dialog_from_enter)

        if getattr(self, "allow_edit_images", False):
            _seq = "Ctrl+E" if sys.platform == "darwin" else "Alt+E"
            self._edit_images_shortcut = QShortcut(QKeySequence(_seq), self)
            self._edit_images_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            self._edit_images_shortcut.activated.connect(self._edit_images_from_modified_shortcut)

    def _dialog_shortcut_blocked_by_text_input(self) -> bool:
        focus = QApplication.focusWidget()
        if focus is None:
            return False
        if isinstance(focus, (QLineEdit, QTextEdit)):
            return True
        if isinstance(focus, QComboBox) and focus.isEditable():
            return True
        parent = focus.parentWidget()
        while parent is not None:
            if isinstance(parent, (QLineEdit, QTextEdit)):
                return True
            if isinstance(parent, QComboBox) and parent.isEditable():
                return True
            parent = parent.parentWidget()
        return False

    def _submit_dialog_from_enter(self) -> None:
        focus = QApplication.focusWidget()
        if isinstance(focus, QTextEdit):
            return
        submit_btn = getattr(self, "submit_observation_btn", None)
        if submit_btn is None or not submit_btn.isEnabled():
            return
        submit_btn.click()

    def _edit_images_from_shortcut(self) -> None:
        if self._dialog_shortcut_blocked_by_text_input():
            return
        edit_btn = getattr(self, "edit_images_btn", None)
        if edit_btn is None or not edit_btn.isEnabled():
            return
        edit_btn.click()

    def _edit_images_from_modified_shortcut(self) -> None:
        edit_btn = getattr(self, "edit_images_btn", None)
        if edit_btn is None or not edit_btn.isEnabled():
            return
        edit_btn.click()

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _set_widget_hint(
        self,
        widget: QWidget | None,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
        disabled_hint: str | None = None,
    ) -> None:
        if widget is None:
            return
        hint = (hint_text or "").strip()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", (tone or "info").strip().lower())
        widget.setProperty("_hint_allow_disabled", bool(allow_when_disabled))
        if disabled_hint is not None:
            widget.setProperty("_hint_disabled_text", (disabled_hint or "").strip())
        widget.setToolTip("")

    def _register_hint_widget(
        self,
        widget: QWidget | None,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
        disabled_hint: str | None = None,
    ) -> None:
        if widget is None:
            return
        hint = (hint_text or "").strip()
        if self._hint_controller is None:
            self._set_widget_hint(
                widget,
                hint,
                tone=tone,
                allow_when_disabled=allow_when_disabled,
                disabled_hint=disabled_hint,
            )
            return
        self._hint_controller.register_widget(
            widget,
            hint,
            tone=tone,
            allow_when_disabled=allow_when_disabled,
            disabled_hint=disabled_hint,
        )
        self._set_widget_hint(
            widget,
            hint,
            tone=tone,
            allow_when_disabled=allow_when_disabled,
        )

    def _register_dialog_hints(self) -> None:
        coord_hint = self.tr("Coordinates in WGS84 decimal degrees.")
        maplink_hint = self.tr("Get map link first then paste the link in the text field.")
        self._register_hint_widget(self.lat_label, coord_hint)
        self._register_hint_widget(self.lat_input, coord_hint)
        self._register_hint_widget(self.lon_label, coord_hint)
        self._register_hint_widget(self.lon_input, coord_hint)
        self._register_hint_widget(self.paste_link_label, maplink_hint)
        self._register_hint_widget(self.maplink_input, maplink_hint)
        self._register_hint_widget(self.maplink_open_btn, maplink_hint)
        self._register_hint_widget(
            self.location_input,
            self.tr("Place name for the observation. Click the field to choose from lookup suggestions."),
        )
        self._register_hint_widget(
            self.map_btn,
            self.tr("Open location in Google Maps"),
            allow_when_disabled=True,
            disabled_hint=self.tr("Enter coordinates to enable the map"),
        )
        self._register_hint_widget(
            self.unidentified_checkbox,
            self.tr("Clear the current identification while keeping AI candidates."),
        )
        self._register_hint_widget(
            self.uncertain_checkbox,
            self.tr("Uncertain identification"),
        )
        self._register_hint_widget(
            self.unspontaneous_checkbox,
            self.tr("Introduced species, escaped from cultivation, not native (ikke spontant)."),
        )
        self._register_hint_widget(
            getattr(self, "vernacular_language_btn", None),
            self.tr("Choose common-name language (applies to the whole app)."),
        )
        for button in getattr(self, "ai_guess_buttons", {}).values():
            self._register_hint_widget(
                button,
                "",
                allow_when_disabled=True,
            )
        for button in getattr(self, "ai_copy_buttons", {}).values():
            self._register_hint_widget(
                button,
                "",
                allow_when_disabled=True,
            )
        self._register_hint_widget(
            self.submit_observation_btn,
            self.tr("Save observation (Enter)") if self.edit_mode else self.tr("Create observation (Enter)"),
        )
        if getattr(self, "edit_images_btn", None):
            _key = getattr(self, "_edit_key_label", "Alt-E")
            _hint = self.tr("Add or remove images for this observation (E)").replace("(E)", f"({_key})")
            self._register_hint_widget(self.edit_images_btn, _hint)
        if getattr(self, "download_cloud_media_btn", None):
            self._register_hint_widget(
                self.download_cloud_media_btn,
                self.tr("Download cloud images and measurements for this observation."),
                allow_when_disabled=True,
            )
        self._update_ai_button_hints()

    def _update_ai_button_hints(self) -> None:
        for _source, guess_btn in getattr(self, "ai_guess_buttons", {}).items():
            selected_indices = self._selected_gallery_indices()
            if guess_btn.isEnabled():
                if selected_indices:
                    guess_hint = self.tr("Guess species using AI - select one or more thumbnails (shift/ctrl + click)")
                elif self._field_gallery_indices():
                    guess_hint = self.tr("Guess species using AI - all field images are used when nothing is selected")
                else:
                    guess_hint = self.tr("Select a field image to use AI recognition")
            else:
                guess_hint = self.tr("Select a field image to use AI recognition")
            self._set_widget_hint(
                guess_btn,
                guess_hint,
                allow_when_disabled=True,
            )
        for _source, copy_btn in getattr(self, "ai_copy_buttons", {}).items():
            if copy_btn.isEnabled():
                copy_hint = (
                    self.tr("Transfer selected species to grows-on")
                    if hasattr(self, "taxonomy_tabs") and self.taxonomy_tabs.currentWidget() == getattr(self, "grows_tab", None)
                    else self.tr("Transfer selected species to taxonomy")
                )
            else:
                if not self._can_copy_ai_to_current_tab():
                    copy_hint = self.tr("Select the Species tab or the Grows-on tab to use AI recognition")
                elif self._ai_selection_has_non_field_image():
                    copy_hint = self.tr("Select a field image to use AI recognition")
                else:
                    copy_hint = self.tr("Select an AI suggestion to copy")
            self._set_widget_hint(
                copy_btn,
                copy_hint,
                allow_when_disabled=True,
            )

    def _ai_selection_has_non_field_image(self) -> bool:
        indices = self._ai_guess_indices()
        indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
        if not indices:
            return False
        return any(
            (self.image_results[idx].image_type or "field").strip().lower() != "field"
            for idx in indices
        )

    def _build_ai_suggestions_group(self) -> QFrame:
        ai_group, ai_layout = self._create_dialog_box(
            self.tr("AI suggestions"),
            QVBoxLayout,
            body_margins=(0, 0, 0, 0),
            body_spacing=0,
        )
        ai_group.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        ai_group.setMinimumWidth(300)

        self.ai_source_tabs = QTabWidget()
        self.ai_source_tabs.setObjectName("taxonomyTabs")
        self.ai_source_tabs.setDocumentMode(True)
        try:
            self.ai_source_tabs.tabBar().setDrawBase(False)
        except Exception:
            pass
        self.ai_source_tabs.currentChanged.connect(self._on_ai_source_tab_changed)
        self.ai_tables: dict[str, QTableWidget] = {}
        self.ai_guess_buttons: dict[str, QPushButton] = {}
        self.ai_copy_buttons: dict[str, QPushButton] = {}
        self.ai_status_labels: dict[str, QLabel] = {}
        self.ai_tabs: dict[str, QWidget] = {}
        self.ai_tab_titles: dict[str, str] = {}

        self.ai_table = self._build_ai_provider_tab(
            source="arts",
            title=self.tr("Artsorakel"),
            headers=[self.tr("Suggested species"), "Match", self.tr("Red List"), "Link"],
        )
        self.inat_ai_table = self._build_ai_provider_tab(
            source="inat",
            title=self.tr("iNaturalist"),
            headers=[self.tr("Suggested species"), "Match", "Link"],
        )
        self.ai_guess_btn = self.ai_guess_buttons["arts"]
        self.ai_copy_btn = self.ai_copy_buttons["arts"]
        self.ai_status_label = self.ai_status_labels["arts"]
        ai_layout.addWidget(self.ai_source_tabs)

        return ai_group

    def _build_ai_provider_tab(self, source: str, title: str, headers: list[str]) -> QTableWidget:
        tab = QWidget()
        tab.setObjectName("taxonomyPage")
        tab_layout = QVBoxLayout(tab)
        tab_layout.setContentsMargins(8, 8, 8, 8)
        tab_layout.setSpacing(8)

        controls = QHBoxLayout()
        guess_btn = QPushButton(self.tr("Guess"))
        guess_btn.clicked.connect(lambda _checked=False, src=source: self._on_ai_guess_clicked(src))
        copy_btn = QPushButton(self.tr("Copy"))
        copy_btn.clicked.connect(lambda _checked=False, src=source: self._on_ai_copy_to_taxonomy(src))
        copy_btn.setEnabled(False)
        guess_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        copy_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        controls.addWidget(guess_btn)
        controls.addWidget(copy_btn)
        controls.setStretch(0, 1)
        controls.setStretch(1, 1)
        tab_layout.addLayout(controls)

        table = QTableWidget(0, len(headers))
        table.setFocusPolicy(Qt.NoFocus)
        table.setObjectName("aiTable")
        table.setHorizontalHeaderLabels(headers)
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        for col in range(1, len(headers)):
            table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeToContents)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setWordWrap(False)
        table.setTextElideMode(Qt.ElideRight)
        table.setMinimumHeight(140)
        table.setMinimumWidth(260)
        table.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)
        table.setShowGrid(False)
        table.setAttribute(Qt.WA_MacShowFocusRect, False)
        if source == "arts":
            from ui.delegates import RedListCircleDelegate
            table.setItemDelegateForColumn(2, RedListCircleDelegate(table))
        table.setStyleSheet(
            "#aiTable::item:selected { background-color: #f0f2f4; color: #2c3e50; font-weight: bold; border: none; }"
            "#aiTable::item:selected:!active { background-color: #f0f2f4; color: #2c3e50; font-weight: bold; border: none; }"
            "#aiTable::item:focus, #aiTable::item:selected:focus, #aiTable::item:selected:!active:focus { outline: none; border: none; }"
            "#aiTable { gridline-color: transparent; }"
        )
        table.itemSelectionChanged.connect(lambda src=source: self._on_ai_selection_changed(src))
        table.cellClicked.connect(lambda row, col, src=source: self._on_ai_table_cell_clicked(src, row, col))
        table.doubleClicked.connect(lambda index, src=source: self._on_ai_table_double_clicked(src, index))
        tab_layout.addWidget(table)

        status_label = QLabel("")
        status_label.setWordWrap(True)
        status_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        tab_layout.addWidget(status_label)

        self.ai_tables[source] = table
        self.ai_guess_buttons[source] = guess_btn
        self.ai_copy_buttons[source] = copy_btn
        self.ai_status_labels[source] = status_label
        self.ai_tabs[source] = tab
        self.ai_tab_titles[source] = title
        self.ai_source_tabs.addTab(tab, self._tab_title_with_state(title, False))
        return table

    def _apply_ai_state(self, ai_state: dict | None) -> None:
        if not ai_state:
            return
        predictions = ai_state.get("predictions")
        selected = ai_state.get("selected")
        inat_predictions = ai_state.get("inat_predictions")
        inat_selected = ai_state.get("inat_selected")
        selected_index = ai_state.get("selected_index")
        if isinstance(predictions, dict):
            remapped: dict[int, list] = {}
            for key, value in predictions.items():
                try:
                    remapped[int(key)] = value
                except (TypeError, ValueError):
                    continue
            self._ai_predictions_by_index = remapped
        if isinstance(selected, dict):
            remapped_selected: dict[int, dict] = {}
            for key, value in selected.items():
                try:
                    remapped_selected[int(key)] = value
                except (TypeError, ValueError):
                    continue
            self._ai_selected_by_index = remapped_selected
        if isinstance(inat_predictions, dict):
            remapped_inat: dict[int, list] = {}
            for key, value in inat_predictions.items():
                try:
                    remapped_inat[int(key)] = value
                except (TypeError, ValueError):
                    continue
            self._inat_predictions_by_index = remapped_inat
        if isinstance(inat_selected, dict):
            remapped_inat_selected: dict[int, dict] = {}
            for key, value in inat_selected.items():
                try:
                    remapped_inat_selected[int(key)] = value
                except (TypeError, ValueError):
                    continue
            self._inat_selected_by_index = remapped_inat_selected
        if isinstance(selected_index, int):
            self._ai_selected_index = selected_index

    def get_ai_state(self) -> dict:
        return {
            "predictions": dict(self._ai_predictions_by_index),
            "selected": dict(self._ai_selected_by_index),
            "inat_predictions": dict(self._inat_predictions_by_index),
            "inat_selected": dict(self._inat_selected_by_index),
            "selected_index": self._ai_selected_index,
            "paths": [item.filepath for item in self.image_results],
            "image_ids": [item.image_id for item in self.image_results],
        }

    # Rediger (stage 9g2) pauses this while the form opens and until the user edits.
    _persist_ai_paused = False

    def _persist_ai_state_now(self, raise_errors: bool = False) -> None:
        """Persist AI lookup state for an existing observation without waiting for Save.

        ``raise_errors``: Rediger's autosave reports a failed write (the dialog ignores it).
        """
        if self._persist_ai_paused:
            return
        obs = self.observation if isinstance(self.observation, dict) else {}
        obs_id = obs.get("id")
        try:
            obs_id = int(obs_id)
        except (TypeError, ValueError):
            return
        if obs_id <= 0:
            return
        try:
            state = self.get_ai_state()
            raw_state = json.dumps(state, ensure_ascii=False)
            stored = self.observation.get("ai_state_json")
            if stored:
                try:
                    if json.loads(stored) == json.loads(raw_state):
                        return  # unchanged (restoring the saved selection on opening): nothing to write
                except (TypeError, ValueError):
                    pass
            ObservationDB.update_observation(
                obs_id,
                ai_state_json=raw_state,
                allow_nulls=True,
            )
            self.observation["ai_state_json"] = raw_state
        except Exception:
            if raise_errors:
                raise

    def _select_initial_ai_image(self) -> None:
        index = self._current_ai_index()
        if index is None:
            return
        self._ai_selected_index = index
        if 0 <= index < len(self.image_results):
            path = self.image_results[index].filepath
            if path:
                self.image_gallery.select_paths([path])
        self._update_ai_controls_state()
        self._update_ai_table()

    def _current_ai_index(self) -> int | None:
        if self._ai_selected_index is not None:
            if 0 <= self._ai_selected_index < len(self.image_results):
                return self._ai_selected_index
            self._ai_selected_index = None
        if self.primary_index is not None and 0 <= self.primary_index < len(self.image_results):
            return self.primary_index
        if self.image_results:
            return 0
        return None

    def _selected_gallery_indices(self) -> list[int]:
        if not hasattr(self, "image_gallery"):
            return []
        paths = self.image_gallery.selected_paths()
        if not paths:
            return []
        indices = []
        for path in paths:
            for idx, item in enumerate(self.image_results):
                if item.filepath == path:
                    indices.append(idx)
                    break
        return sorted(set(indices))

    def _field_gallery_indices(self) -> list[int]:
        indices: list[int] = []
        for idx, item in enumerate(self.image_results):
            if (item.image_type or "field").strip().lower() == "field":
                indices.append(idx)
        return indices

    def _ai_guess_indices(self) -> list[int]:
        indices = self._selected_gallery_indices()
        if indices:
            return indices
        return self._field_gallery_indices()

    def _on_gallery_image_clicked(self, _image_id, path: str) -> None:
        if not path:
            return
        image_index = None
        for idx, meta in enumerate(getattr(self, "image_metadata", []) or []):
            if str(meta.get("filepath") or "") == path:
                image_index = idx
                break
        if image_index is not None and hasattr(self, "image_table"):
            try:
                self.image_table.selectRow(image_index)
            except Exception:
                pass
            try:
                self.on_image_selected()
            except Exception:
                pass
        for idx, item in enumerate(self.image_results):
            if item.filepath == path:
                self._ai_selected_index = idx
                self._update_ai_controls_state()
                self._update_ai_table()
                return

    def _ai_source_key(self) -> str:
        tabs = getattr(self, "ai_source_tabs", None)
        if tabs is not None:
            current = tabs.currentIndex()
            if current == 1:
                return "inat"
        return "arts"

    def _on_ai_source_tab_changed(self, _index: int) -> None:
        self._selected_ai_source = self._ai_source_key()
        self._update_ai_table()
        self._update_ai_controls_state()

    def _update_ai_controls_state(self) -> None:
        if not hasattr(self, "ai_guess_buttons"):
            return
        indices = self._ai_guess_indices()
        enable = False
        if indices:
            indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
            if indices:
                enable = all(
                    (self.image_results[idx].image_type or "field").strip().lower() == "field"
                    for idx in indices
                )
        arts_enable = enable and self._ai_thread is None
        inat_enable = enable and self._inat_ai_thread is None
        if "arts" in self.ai_guess_buttons:
            self.ai_guess_buttons["arts"].setEnabled(arts_enable)
        if "inat" in self.ai_guess_buttons:
            self.ai_guess_buttons["inat"].setEnabled(inat_enable)
        self._update_ai_button_hints()
        for source, table in getattr(self, "ai_tables", {}).items():
            self._set_ai_copy_enabled(table.currentRow() >= 0, source=source)

    def _update_ai_tab_indicator(self, source: str, has_data: bool) -> None:
        if not hasattr(self, "ai_source_tabs") or not hasattr(self, "ai_tabs") or not hasattr(self, "ai_tab_titles"):
            return
        tab = self.ai_tabs.get(source)
        title = self.ai_tab_titles.get(source)
        if tab is None or title is None:
            return
        idx = self.ai_source_tabs.indexOf(tab)
        if idx >= 0:
            self.ai_source_tabs.setTabText(idx, self._tab_title_with_state(title, has_data))

    def _update_ai_table(self) -> None:
        if not hasattr(self, "ai_tables"):
            return
        index = self._current_ai_index()
        for source, table in self.ai_tables.items():
            table.setRowCount(0)
            if index is None:
                self._set_ai_copy_enabled(False, source=source)
                self._update_ai_tab_indicator(source, False)
                continue
            predictions = (
                self._inat_predictions_by_index.get(index, [])
                if source == "inat"
                else self._ai_predictions_by_index.get(index, [])
            )
            self._update_ai_tab_indicator(source, bool(predictions))
            for row, pred in enumerate(predictions):
                taxon = _normalize_ai_prediction_taxon(pred, source=source)
                display_name = self._format_ai_taxon_name(taxon, source=source)
                confidence = self._ai_prediction_score(pred)
                name_item = QTableWidgetItem(display_name)
                name_item.setData(Qt.UserRole, pred)
                conf_item = QTableWidgetItem(self._format_ai_prediction_score(pred, source=source))
                table.insertRow(row)
                table.setItem(row, 0, name_item)
                table.setItem(row, 1, conf_item)
                if source == "arts":
                    red_item = self._build_red_list_table_item(
                        self._red_list_display_from_prediction(pred, taxon, short=True),
                        self._red_list_display_from_prediction(pred, taxon, short=False),
                    )
                    if red_item:
                        table.setItem(row, 2, red_item)
                    link_item = self._build_taxon_link_item(self._ai_prediction_links(pred, taxon, source=source))
                    if link_item:
                        table.setItem(row, 3, link_item)
                else:
                    link_item = self._build_taxon_link_item(self._ai_prediction_links(pred, taxon, source=source))
                    if link_item:
                        table.setItem(row, 2, link_item)
                table.setRowHeight(row, max(table.verticalHeader().defaultSectionSize(), 28))
            if predictions:
                selected = (
                    self._inat_selected_by_index.get(index)
                    if source == "inat"
                    else self._ai_selected_by_index.get(index)
                )
                if selected:
                    for row in range(table.rowCount()):
                        item = table.item(row, 0)
                        if item and item.data(Qt.UserRole) == selected:
                            table.selectRow(row)
                            break
                else:
                    table.selectRow(0)
                self._set_ai_copy_enabled(table.currentRow() >= 0, source=source)
            else:
                if source == self._ai_source_key():
                    self._ai_selected_taxon = None
                self._set_ai_copy_enabled(False, source=source)

    @staticmethod
    def _ai_prediction_score(pred: dict) -> float:
        for key in ("probability", "combined_score", "vision_score", "score", "frequency_score"):
            value = pred.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return 0.0

    def _format_ai_prediction_score(self, pred: dict, source: str = "arts") -> str:
        score = self._ai_prediction_score(pred)
        return format_probability_percent(score)

    def _format_ai_taxon_name(self, taxon: dict, source: str = "arts") -> str:
        scientific = self._scientific_name_from_taxon(taxon)
        vernacular = ""
        if source == "inat":
            vernacular = self._inat_preferred_common_name(taxon) or ""
        else:
            vernacular = self._preferred_vernacular_from_taxon(taxon) or ""
            
        if vernacular and scientific:
            vernacular_norm = str(vernacular).strip()
            scientific_norm = str(scientific).strip()
            if vernacular_norm and scientific_norm and vernacular_norm.casefold() != scientific_norm.casefold():
                return f"{vernacular_norm} ({scientific_norm})"
        return vernacular or scientific or self.tr("Unknown")

    RED_LIST_LABELS = {
        "RE": "regionalt utdødd",
        "CR": "kritisk truet",
        "EN": "sterkt truet",
        "VU": "sårbar",
        "NT": "nær truet",
        "DD": "datamangel",
        "LC": "livskraftig",
        "NA": "ikke egnet",
        "NE": "ikke vurdert",
    }

    def _red_list_label(self, code: str | None, *, include_code: bool = True) -> str:
        clean = str(code or "").strip().upper()
        if not clean:
            return ""
        label = self.RED_LIST_LABELS.get(clean)
        if not label:
            return clean
        return f"{clean} - {label}" if include_code else label

    def _set_red_list_category_raw(
        self,
        raw_code: str | None,
        categories: dict | None = None,
        *,
        hint: str | None = None,
    ) -> None:
        """Store the display value verbatim (preserving degree marks) and
        refresh the badge + label.

        Stage 3B.5: ``raw_code`` may include a trailing ``"°"`` (degree
        marker). The badge text renders it as-is. Colour and label lookups
        use the base code (``raw_code`` with any trailing ``"°"`` removed),
        because ``RED_LIST_BADGE_COLORS`` and ``_red_list_label`` are keyed
        on the plain IUCN code.
        """
        text = str(raw_code or "").strip()
        # Keep the degree marker verbatim; uppercase only the leading
        # alphabetic part so "vu°" still renders as "VU°".
        if text.endswith("°"):
            base_alpha = text[:-1].upper()
            display = f"{base_alpha}°"
            base_code = base_alpha
        else:
            display = text.upper()
            base_code = display
        self._red_list_category = display
        self._red_list_categories = dict(categories) if isinstance(categories, dict) else None
        badge = getattr(self, "red_list_badge_widget", None)
        if badge is not None:
            if display:
                hex_color = RED_LIST_BADGE_COLORS.get(base_code, "#63666A")
                badge.set_color_and_text(hex_color, display)
                badge.setVisible(True)
            else:
                badge.setVisible(False)
        label = getattr(self, "red_list_status_label", None)
        if label is not None:
            if hint:
                label.setText(hint)
            else:
                label.setText(
                    self._red_list_label(base_code, include_code=False) or self.tr("Not set")
                )

    def _set_red_list_category(self, code: str | None, categories: dict | None = None) -> None:
        """Backwards-compatible wrapper over :meth:`_set_red_list_category_raw`.

        Existing call sites use this signature. The raw variant accepts a
        hint string; callers that need to display a non-standard status
        message (e.g. "Multiple assessments") should call the raw form
        directly.
        """
        self._set_red_list_category_raw(code, categories, hint=None)

    def _set_inaturalist_taxon_id(self, value) -> None:
        try:
            taxon_id = int(value)
        except (TypeError, ValueError):
            taxon_id = None
        self._inaturalist_taxon_id = taxon_id if taxon_id and taxon_id > 0 else None

    # Artsorakel's raw API returns camelCase `redListCategory` (capital L).
    # sporely-web reformats predictions to `redlistCategory` (lowercase l) and
    # also emits a snake_case `redlist_category`. Cloud-synced observations
    # therefore reach us with the lowercase variant, so accept all three.
    _RED_LIST_CATEGORY_KEYS = ("redListCategory", "redlistCategory", "redlist_category")
    _RED_LIST_CATEGORIES_KEYS = ("redListCategories", "redlistCategories", "redlist_categories")

    @classmethod
    def _read_red_list_code(cls, source: dict | None) -> str:
        if not isinstance(source, dict):
            return ""
        for key in cls._RED_LIST_CATEGORY_KEYS:
            value = source.get(key)
            if value:
                text = str(value).strip()
                if text:
                    return text
        return ""

    @classmethod
    def _read_red_list_categories(cls, source: dict | None) -> dict | None:
        if not isinstance(source, dict):
            return None
        for key in cls._RED_LIST_CATEGORIES_KEYS:
            value = source.get(key)
            if isinstance(value, dict) and value:
                return value
        return None

    def _red_list_display_from_prediction(self, pred: dict, taxon: dict, *, short: bool) -> str:
        code = self._read_red_list_code(taxon) or self._read_red_list_code(pred)
        return code.upper() if short and code else self._red_list_label(code)

    def _build_red_list_table_item(self, code: str | None, tooltip: str | None = None) -> QTableWidgetItem | None:
        clean = str(code or "").strip().upper()
        if not clean:
            return None
        item = QTableWidgetItem(clean)
        item.setToolTip(str(tooltip or self._red_list_label(clean)).strip())
        hex_color = RED_LIST_BADGE_COLORS.get(clean, "#63666A")
        item.setForeground(QColor(hex_color))
        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
        return item

    def _red_list_categories_from_prediction(self, pred: dict, taxon: dict) -> dict | None:
        return self._read_red_list_categories(taxon) or self._read_red_list_categories(pred)

    def _ai_prediction_link(self, pred: dict, taxon: dict, source: str = "arts") -> str | None:
        source = str(source or "").strip().lower()
        pred_obj = pred if isinstance(pred, dict) else {}
        taxon_obj = taxon if isinstance(taxon, dict) else {}

        if source == "inat":
            taxon_id = str(
                taxon_obj.get("id")
                or taxon_obj.get("taxonId")
                or taxon_obj.get("taxon_id")
                or pred_obj.get("taxonId")
                or pred_obj.get("taxon_id")
                or ""
            ).strip()
            if taxon_id:
                return f"https://www.inaturalist.org/taxa/{taxon_id}"
            return None

        for obj in (pred_obj, taxon_obj):
            for key in (
                "species_url",
                "speciesUrl",
                "adbUrl",
                "url",
                "link",
                "href",
                "uri",
                "infoUrl",
                "infoURL",
                "info_url",
            ):
                value = obj.get(key)
                if isinstance(value, str) and value.strip().startswith("http"):
                    return value.strip()

        taxon_id = str(
            taxon_obj.get("taxonId")
            or taxon_obj.get("taxon_id")
            or taxon_obj.get("TaxonId")
            or taxon_obj.get("id")
            or pred_obj.get("taxonId")
            or pred_obj.get("taxon_id")
            or ""
        ).strip()
        if taxon_id and taxon_id.isdigit():
            return concept_link_from_name_id(taxon_id)
        return None

    def _ai_prediction_links(self, pred: dict, taxon: dict, source: str = "arts") -> list[tuple[str, str]]:
        links: list[tuple[str, str]] = []
        url = self._ai_prediction_link(pred, taxon, source=source)
        if url:
            links.append((self.tr("Open link"), url))
        return links

    def _build_taxon_link_item(self, links: list[tuple[str, str]]) -> QTableWidgetItem | None:
        valid_links = [
            (str(label or "").strip(), str(url or "").strip())
            for label, url in (links or [])
            if str(label or "").strip() and str(url or "").strip()
        ]
        if not valid_links:
            return None
        label, url = valid_links[0]
        
        color = get_design_tokens().get("text", "#2c3e50")
        svg_data = f"""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
          <polyline points="15 3 21 3 21 9"></polyline>
          <line x1="10" y1="14" x2="21" y2="3"></line>
        </svg>""".encode("utf-8")
        
        from PySide6.QtSvg import QSvgRenderer
        renderer = QSvgRenderer(svg_data)
        pixmap = QPixmap(48, 48)  # Render cleanly at 2x for high-DPI displays
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        renderer.render(painter)
        painter.end()
        
        item = QTableWidgetItem(QIcon(pixmap), "")
        item.setTextAlignment(Qt.AlignCenter)
        item.setToolTip(label)
        item.setData(Qt.UserRole, url)
        return item

    def _on_ai_table_cell_clicked(self, source: str, row: int, column: int) -> None:
        table = self.ai_tables.get(source) if hasattr(self, "ai_tables") else None
        if table is None:
            return
        link_column = 3 if source == "arts" else 2
        if column != link_column:
            return
        item = table.item(row, column)
        url = str(item.data(Qt.UserRole) or "").strip() if item else ""
        if url:
            QDesktopServices.openUrl(QUrl(url))

    def _on_ai_table_double_clicked(self, source: str, index: QModelIndex) -> None:
        link_column = 3 if source == "arts" else 2
        if index.isValid() and index.column() == link_column:
            self._on_ai_table_cell_clicked(source, index.row(), index.column())
            return
        self._on_ai_copy_to_taxonomy(source)

    def _on_ai_selection_changed(self, source: str | None = None) -> None:
        source = source or self._ai_source_key()
        index = self._current_ai_index()
        if index is None:
            return
        table = self.ai_tables.get(source) if hasattr(self, "ai_tables") else getattr(self, "ai_table", None)
        if table is None:
            return
        selected_items = table.selectedItems()
        if not selected_items:
            self._ai_selected_taxon = None
            self._set_ai_status(None, source=source)
            self._set_ai_copy_enabled(False, source=source)
            return
        row_item = table.item(table.currentRow(), 0)
        if not row_item:
            return
        pred = row_item.data(Qt.UserRole) or {}
        if source == "inat":
            self._inat_selected_by_index[index] = pred
        else:
            self._ai_selected_by_index[index] = pred
        self._ai_selected_taxon = _normalize_ai_prediction_taxon(pred, source=source)
        self._set_ai_status(None, source=source)
        self._set_ai_copy_enabled(True, source=source)
        self._persist_ai_state_now()

    def _set_ai_status(self, text: str | None, color: str = "#7f8c8d", source: str | None = None) -> None:
        source = source or self._ai_source_key()
        label = None
        if hasattr(self, "ai_status_labels"):
            label = self.ai_status_labels.get(source)
        if label is None:
            label = getattr(self, "ai_status_label", None)
        if label is None:
            return
        if not text:
            label.setText("")
            return
        label.setText(text)
        label.setStyleSheet(f"color: {color}; font-size: {pt(9)}pt;")

    def _set_ai_copy_enabled(self, enabled: bool, source: str | None = None) -> None:
        source = source or self._ai_source_key()
        button = None
        if hasattr(self, "ai_copy_buttons"):
            button = self.ai_copy_buttons.get(source)
        if button is None:
            button = getattr(self, "ai_copy_btn", None)
        if button is not None:
            button.setEnabled(bool(enabled) and self._can_copy_ai_to_current_tab())
            self._update_ai_button_hints()

    def _can_copy_ai_to_current_tab(self) -> bool:
        tabs = getattr(self, "taxonomy_tabs", None)
        if tabs is None:
            return True
        current = tabs.currentWidget()
        return current in {getattr(self, "species_tab", None), getattr(self, "grows_tab", None)}

    def _inat_credentials(self) -> tuple[str, str, str]:
        from utils.inat_oauth import INatOAuthClient

        client_id = (SettingsDB.get_setting("inat_client_id", "") or "").strip()
        client_secret = (SettingsDB.get_setting("inat_client_secret", "") or "").strip()
        redirect_uri = (
            SettingsDB.get_setting("inat_redirect_uri", INatOAuthClient.DEFAULT_REDIRECT_URI)
            or INatOAuthClient.DEFAULT_REDIRECT_URI
        )
        if not client_id:
            client_id = (os.getenv("INAT_CLIENT_ID", "") or "").strip()
        if not client_id:
            client_id = INatOAuthClient.DEFAULT_CLIENT_ID
        if not client_secret:
            client_secret = (os.getenv("INAT_CLIENT_SECRET", "") or "").strip()
        return client_id, client_secret, redirect_uri

    def _inat_token_file(self) -> Path:
        return app_data_dir() / "inaturalist_oauth_tokens.json"

    def _inat_locale(self) -> str:
        raw = str(SettingsDB.get_setting("vernacular_language", "") or get_app_settings().get("vernacular_language") or "no")
        prefix = raw.replace("-", "_").split("_", 1)[0].lower()
        if prefix in {"nb", "nn", "no"}:
            return "no"
        if prefix in {"sv", "de", "en", "da", "fi", "fr", "es", "it", "pt", "pl"}:
            return prefix
        return "en"

    def _first_common_name(self, value) -> str:
        if isinstance(value, (list, tuple)):
            for item in value:
                name = self._first_common_name(item)
                if name:
                    return name
            return ""
        if isinstance(value, dict):
            locale = self._inat_locale()
            for key in (locale, normalize_vernacular_language(locale), "nb", "no", "en"):
                if key in value:
                    name = self._first_common_name(value.get(key))
                    if name:
                        return name
            for item in value.values():
                name = self._first_common_name(item)
                if name:
                    return name
            return ""
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"^\s*['\"]|['\"]\s*$", "", text)
        parts = re.split(
            r"\s*(?:\r?\n|[,;/|•·・]|\s+-\s+|\s+\bor\b\s+|\s+\band\b\s+)\s*",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        return (parts[0] if parts else text).strip()

    def _inat_preferred_common_name(self, taxon: dict) -> str | None:
        if not isinstance(taxon, dict):
            return None
        for key in ("preferred_common_name", "preferred_common_names", "common_name", "common_names"):
            value = self._first_common_name(taxon.get(key))
            if value:
                return value
        return None

    @staticmethod
    def _looks_like_scientific_name(text: str | None) -> bool:
        value = str(text or "").strip()
        parts = value.split()
        if len(parts) < 2:
            return False
        return bool(re.match(r"^[A-Z][A-Za-z-]+$", parts[0]) and re.match(r"^[a-z][A-Za-z-]+$", parts[1]))

    @staticmethod
    def _split_scientific_name_text(text: str | None) -> tuple[str | None, str | None]:
        return split_scientific_name_text(text)

    def _scientific_name_from_taxon(self, taxon: dict) -> str:
        for key in ("scientificName", "scientific_name"):
            value = str(taxon.get(key) or "").strip()
            if value:
                return value
        value = str(taxon.get("name") or "").strip()
        if self._looks_like_scientific_name(value):
            return value
        return ""

    def _extract_genus_species_from_taxon(self, taxon: dict) -> tuple[str | None, str | None]:
        """Return ``(genus, species)`` for an AI prediction taxon dict.

        Accepts genus-only predictions (e.g. the Artsorakel row
        ``trevlesopper (Inocybe)``): when a structured genus field is
        present without a species, or when the scientific-name text is a
        single capitalised token, returns ``(genus, None)`` so the caller
        can still populate the Genus input while leaving Species empty.
        The AI Copy handler treats the pair as valid whenever ``genus``
        is set, regardless of ``species``.
        """
        if not isinstance(taxon, dict):
            return None, None
        genus = taxon.get("genus") or taxon.get("genusName") or taxon.get("genus_name")
        species = (
            taxon.get("species")
            or taxon.get("specificEpithet")
            or taxon.get("specific_epithet")
        )
        genus_text = str(genus).strip() if genus else ""
        species_text = str(species).strip() if species else ""
        if genus_text and species_text:
            return genus_text, species_text
        sci = self._scientific_name_from_taxon(taxon)
        if sci and isinstance(sci, str):
            split = self._split_scientific_name_text(sci)
            # ``split_scientific_name_text`` returns ``(None, None)`` for
            # single-token input (a bare genus). Truthiness of a tuple is
            # not enough — check the parsed genus explicitly before we
            # accept the split.
            split_genus = split[0] if split else None
            split_species = split[1] if split else None
            if split_genus:
                return (
                    genus_text or split_genus,
                    species_text or split_species,
                )
            # Scientific-name text is a single capitalised token — treat
            # it as a genus-only identification when no structured genus
            # is present. Matches the shape Artsorakel returns for
            # genus-level predictions (e.g. ``"Inocybe"``).
            cleaned = " ".join(str(sci).split())
            if (
                cleaned
                and " " not in cleaned
                and cleaned[:1].isupper()
                and cleaned.replace("-", "").isalpha()
            ):
                return (genus_text or cleaned, species_text or None)
        # iNaturalist genus-rank predictions store the genus in
        # ``taxon.name`` as a bare single token. ``_scientific_name_from_taxon``
        # rejects it because it fails the binomial regex, but the field
        # is still an identity signal. Recognise a single capitalised
        # token in ``name`` as a genus when nothing else pinned it.
        raw_name = str(taxon.get("name") or "").strip()
        if raw_name:
            cleaned_name = " ".join(raw_name.split())
            if (
                " " not in cleaned_name
                and cleaned_name[:1].isupper()
                and cleaned_name.replace("-", "").isalpha()
            ):
                return (genus_text or cleaned_name, species_text or None)
        return (genus_text or None, species_text or None)

    def _selected_ai_prediction(self, source: str) -> dict:
        table = self.ai_tables.get(source) if hasattr(self, "ai_tables") else None
        if table is None:
            return {}
        row = table.currentRow()
        if row < 0 and table.rowCount() > 0:
            row = 0
        item = table.item(row, 0) if row >= 0 else None
        pred = item.data(Qt.UserRole) if item else None
        return pred if isinstance(pred, dict) else {}

    def _on_ai_copy_to_taxonomy(self, source: str | None = None, prediction: dict | None = None) -> None:
        source = source or self._ai_source_key()
        concept_before = self._committed_sporely_taxon_id()
        index = self._current_ai_index()
        # A prediction chosen elsewhere (an Artsorakel chip in the species
        # picker), else the row selected in the AI table.
        selected_pred = prediction or self._selected_ai_prediction(source)
        if index is not None:
            if selected_pred:
                if source == "inat":
                    self._inat_selected_by_index[index] = selected_pred
                else:
                    self._ai_selected_by_index[index] = selected_pred
            else:
                selected_pred = (
                    self._inat_selected_by_index.get(index)
                    if source == "inat"
                    else self._ai_selected_by_index.get(index)
                ) or {}
        taxon = _normalize_ai_prediction_taxon(selected_pred or {}, source=source) or self._ai_selected_taxon or {}
        genus, species = self._extract_genus_species_from_taxon(taxon)
        if not genus or not species:
            fallback_scientific_name = str((self._current_ai_selected_fields or {}).get("ai_selected_scientific_name") or "").strip()
            if not fallback_scientific_name and isinstance(self.observation, dict):
                fallback_scientific_name = str(self.observation.get("species_guess") or "").strip()
            if not fallback_scientific_name and isinstance(self.draft_data, dict):
                fallback_scientific_name = str(self.draft_data.get("species_guess") or "").strip()
            fallback_genus, fallback_species = self._split_scientific_name_text(fallback_scientific_name)
            if not genus and fallback_genus:
                genus = fallback_genus
            if not species and fallback_species:
                species = fallback_species
        # A genus-only Artsorakel/iNat row is a valid identification level
        # (e.g. ``trevlesopper (Inocybe)`` with no species). Require only a
        # genus; leave species empty when the AI row has none — do NOT
        # invent one via string-matching. Refuse only when the payload has
        # no scientific-level identity at all.
        if not genus:
            self._set_ai_status(self.tr("Could not parse genus/species from AI suggestion."), "#e67e22", source=source)
            return
        # ``ai_selected_scientific_name`` mirrors what the user picked.
        # For a full binomial keep the "Genus species" form; for genus-only
        # keep the bare genus so the "Selected AI: … · Inocybe · …" status
        # footer still renders and Save persists the same string.
        if genus and species:
            scientific_name = f"{genus} {species}".strip()
        else:
            scientific_name = genus.strip() if genus else None
        current_tab = self.taxonomy_tabs.currentWidget() if hasattr(self, "taxonomy_tabs") else None
        target_grows = current_tab == getattr(self, "grows_tab", None)
        if target_grows:
            self._host_suppress_taxon_autofill = True
            self.host_genus_input.setText(genus)
            self.host_species_input.setText(species or "")
            self._host_suppress_taxon_autofill = False
            if self.vernacular_db:
                self.host_vernacular_input.clear()
                self._maybe_set_host_vernacular_from_taxon()
            self._set_ai_status(self.tr("Copied to grows-on species."), "#27ae60", source=source)
        else:
            if hasattr(self, "taxonomy_tabs"):
                self.taxonomy_tabs.setCurrentIndex(self.taxonomy_tabs.indexOf(self.species_tab))
            if self.is_unidentified():
                self.unidentified_checkbox.setChecked(False)
            self._suppress_taxon_autofill = True
            if hasattr(self, "genus_input"):
                self.genus_input.setText(genus)
            if hasattr(self, "species_input"):
                # An empty species must clear the widget so a prior
                # species-level pick doesn't linger under a genus-only
                # copy.
                self.species_input.setText(species or "")
            vernacular = (
                self._inat_preferred_common_name(taxon)
                if source == "inat"
                else self._preferred_vernacular_from_taxon(taxon)
            )
            if hasattr(self, "vernacular_input"):
                self.vernacular_input.setText(vernacular or "")
            # Commit the provider identity before applying the provider's
            # Red List snapshot: the commit callback clears the Red List for
            # the identity change, and must not clear the snapshot that
            # belongs to this very identity.
            self._preserve_ai_external_taxon_identity(taxon, genus, species)
            if source == "inat":
                self._set_inaturalist_taxon_id(taxon.get("id"))
                self._set_red_list_category(None, None)
            else:
                # The publishing taxon ID stays only with the concept it was
                # stored for (see _committed_sporely_taxon_id).
                if concept_before is None or self._committed_sporely_taxon_id() != concept_before:
                    self._set_inaturalist_taxon_id(None)
                if species:
                    self._set_red_list_category(
                        self._read_red_list_code(taxon) or self._read_red_list_code(selected_pred),
                        self._red_list_categories_from_prediction(selected_pred or {}, taxon),
                    )
                    # Stage 3B.5: after applying the Artsorakel source snapshot,
                    # schedule a single deferred local-lookup resolve so a
                    # matching taxonomy pick refines the category into its
                    # final display form (respects degree marker, area, and
                    # conflict semantics).
                    self._schedule_final_redlist_resolution()
                else:
                    # Genus-only Artsorakel row: no unambiguous species-
                    # level identity means no local red-list lookup can
                    # refine a category. Clear any stale badge from a
                    # prior species-level copy and skip the deferred
                    # resolve (its guard would drop the run anyway
                    # because no snapshot binds).
                    self._set_red_list_category(None, None)
            self._suppress_taxon_autofill = False
            if self.vernacular_db:
                self._update_vernacular_suggestions_for_taxon()
                if not vernacular:
                    self._maybe_set_vernacular_from_taxon()
            self._current_ai_selected_fields = {
                "ai_selected_service": _normalize_ai_selected_service(source),
                "ai_selected_taxon_id": str(taxon.get("id") or "").strip() or None,
                "ai_selected_scientific_name": scientific_name,
                "ai_selected_probability": self._ai_prediction_score(selected_pred or {}),
                "ai_selected_at": _current_utc_timestamp_text(),
            }
            self._update_selected_ai_summary_label(self._current_ai_selected_fields)
            self._set_ai_status(self.tr("Copied to taxonomy."), "#27ae60", source=source)
        self._update_taxonomy_tab_indicators()
        self._persist_ai_state_now()

    def _preserve_ai_external_taxon_identity(
        self, taxon: dict, genus: str, species: str | None,
    ) -> bool:
        """Keep a provider's namespaced identifier when its row is copied in.

        Taxonomy-v2 closeout Stage 2 Part A. Copying an AI suggestion into the
        identification writes genus/species text, which invalidates any
        committed snapshot and leaves the observation with no identity record
        at all — even though the provider handed us a perfectly good
        namespaced identifier (Artsorakel returns ``NBIC:53482``). That
        identifier was only kept as ``ai_selected_taxon_id``, which is
        AI-identification *history* and must not be mistaken for an accepted
        observation identity.

        The identifier is preserved as ``(source_system, namespace,
        external_id)`` plus the verbatim provider value. When the installed
        taxonomy-v2 artifact holds an exact namespaced bridge for it, it is
        committed as that Sporely concept with ``external_id_resolution``
        proof (0.9.23); otherwise it stays an explicitly unresolved external
        identity with no ``sporely_taxon_id``, and the cloud gate refuses to
        emit anything for it.

        Returns whether an external identity was committed. iNaturalist and
        other sources whose ids are bare integers are deliberately not
        preserved here: a namespace-lost integer is legacy/audit evidence
        only, and inventing a namespace for it is the defect this stage
        closes.
        """
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return False
        raw_identifier = str(taxon.get("id") or "").strip()
        parts = [str(genus or "").strip(), str(species or "").strip()]
        identity = TaxonIdentity.from_prefixed_external_id(
            raw_identifier,
            scientific_name=" ".join(p for p in parts if p) or None,
            rank="species" if species else "genus",
        )
        if not identity.has_external_evidence:
            return False
        # Resolved through the installed taxonomy-v2 artifact when an exact
        # namespaced bridge exists; committed unresolved otherwise.
        return controller.commit_provider_identity(
            identity,
            genus=parts[0],
            species=parts[1],
        )

    def _on_ai_crop_clicked(self) -> None:
        return

    def _observation_ai_lat_lon(self) -> tuple[float | None, float | None]:
        """Return the observation/draft's canonical lat/lon (if both known).

        Prefers the persisted ``gps_latitude``/``gps_longitude`` on the
        current observation dict; falls back to the editable
        ``lat_input``/``lon_input`` values for fresh drafts.
        """
        lat: float | None = None
        lon: float | None = None
        obs = getattr(self, "observation", None) or {}
        try:
            raw_lat = obs.get("gps_latitude")
            raw_lon = obs.get("gps_longitude")
            if raw_lat is not None:
                lat = float(raw_lat)
            if raw_lon is not None:
                lon = float(raw_lon)
        except Exception:
            lat = None
            lon = None
        if (lat is None or lon is None) and hasattr(self, "lat_input") and hasattr(self, "lon_input"):
            try:
                lat_value = self.lat_input.value()
                lon_value = self.lon_input.value()
                if lat_value > self.lat_input.minimum():
                    lat = float(lat_value)
                if lon_value > self.lon_input.minimum():
                    lon = float(lon_value)
            except Exception:
                pass
        return lat, lon

    def _on_ai_guess_clicked(self, source: str | None = None) -> None:
        source_key = "all"
        try:
            source_key = str(source or "all").strip().lower() or "all"
            if source_key not in {"arts", "inat", "all"}:
                source_key = "all"
            run_arts = source_key in {"arts", "all"}
            run_inat = source_key in {"inat", "all"}
            indices = self._selected_gallery_indices()
            if not indices:
                indices = self._field_gallery_indices()
                if indices and hasattr(self, "image_gallery"):
                    field_paths = [
                        self.image_results[idx].filepath
                        for idx in indices
                        if 0 <= idx < len(self.image_results) and self.image_results[idx].filepath
                    ]
                    if field_paths:
                        self.image_gallery.select_paths(field_paths)
                        self._update_ai_controls_state()
            indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
            if not indices:
                return
            if any(
                (self.image_results[idx].image_type or "field").strip().lower() != "field"
                for idx in indices
            ):
                if run_arts:
                    self._set_ai_status(self.tr("AI guess only works for field photos"), "#e74c3c", source="arts")
                if run_inat:
                    self._set_ai_status(self.tr("AI guess only works for field photos"), "#e74c3c", source="inat")
                return
            requests = []
            for idx in indices:
                result = self.image_results[idx]
                image_path = result.filepath
                if not image_path:
                    continue
                requests.append(
                    {
                        "index": idx,
                        "image_path": image_path,
                        "crop_box": getattr(result, "ai_crop_box", None),
                    }
                )
            if not requests:
                return

            count = len(requests)
            temp_dir = get_images_dir() / "imports"

            if run_arts and self._ai_thread is None:
                arts_access_token = ""
                try:
                    arts_client = self._window_cloud_client()
                    arts_access_token = str(getattr(arts_client, "access_token", "") or "").strip()
                except Exception:
                    arts_access_token = ""
                if not arts_access_token:
                    self._set_ai_status(
                        self.tr("Sign in to Sporely Cloud to use Artsorakel AI guess."),
                        "#e67e22",
                        source="arts",
                    )
                else:
                    arts_guess_btn = self.ai_guess_buttons.get("arts")
                    if arts_guess_btn:
                        arts_guess_btn.setEnabled(False)
                        arts_guess_btn.setText(self.tr("AI guessing..."))
                    self._set_ai_status(
                        self.tr("Sending {count} image(s) to Artsdatabanken AI...").format(count=count),
                        "#3498db",
                        source="arts",
                    )
                    arts_lat, arts_lon = self._observation_ai_lat_lon()
                    self._ai_thread = AIGuessWorker(
                        requests,
                        temp_dir,
                        max_dim=ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
                        parent=self,
                        access_token=arts_access_token,
                        latitude=arts_lat,
                        longitude=arts_lon,
                    )
                    self._ai_thread.resultReady.connect(self._on_ai_guess_finished)
                    self._ai_thread.error.connect(self._on_ai_guess_error)
                    self._ai_thread.finished.connect(self._ai_thread.deleteLater)
                    self._ai_thread.finished.connect(self._on_ai_thread_finished)
                    self._ai_thread.start()

            if run_inat and self._inat_ai_thread is None:
                from utils.inat_oauth import INatOAuthClient
                client_id, client_secret, redirect_uri = self._inat_credentials()
                oauth = INatOAuthClient(
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    token_file=self._inat_token_file(),
                )
                if not oauth.is_logged_in():
                    self._set_ai_status(
                        self.tr("Not logged in to iNaturalist. Log in via Settings -> Online publishing."),
                        "#e67e22",
                        source="inat"
                    )
                else:
                    inat_guess_btn = self.ai_guess_buttons.get("inat")
                    if inat_guess_btn:
                        inat_guess_btn.setEnabled(False)
                        inat_guess_btn.setText(self.tr("AI guessing..."))
                    observed_on = self.datetime_input.date().toString("yyyy-MM-dd") if hasattr(self, "datetime_input") else ""
                    lat = self.lat_input.value() if hasattr(self, "lat_input") and self.lat_input.value() > self.lat_input.minimum() else None
                    lon = self.lon_input.value() if hasattr(self, "lon_input") and self.lon_input.value() > self.lon_input.minimum() else None
                    self._set_ai_status(
                        self.tr("Sending {count} image(s) to iNaturalist AI...").format(count=count),
                        "#3498db",
                        source="inat",
                    )
                    self._inat_ai_thread = INatAIGuessWorker(
                        requests,
                        temp_dir,
                        client_id=client_id,
                        client_secret=client_secret,
                        redirect_uri=redirect_uri,
                        token_file=self._inat_token_file(),
                        locale=self._inat_locale(),
                        lat=lat,
                        lon=lon,
                        observed_on=observed_on,
                        max_dim=1600,
                        parent=self,
                    )
                    self._inat_ai_thread.resultReady.connect(self._on_inat_ai_guess_finished)
                    self._inat_ai_thread.error.connect(self._on_inat_ai_guess_error)
                    self._inat_ai_thread.finished.connect(self._inat_ai_thread.deleteLater)
                    self._inat_ai_thread.finished.connect(self._on_inat_ai_thread_finished)
                    self._inat_ai_thread.start()

            self._update_ai_button_hints()
        except Exception as exc:
            message = self.tr("AI guess failed: {message}").format(message=str(exc))
            if source_key in {"arts", "all"}:
                self._set_ai_status(message, "#e74c3c", source="arts")
            if source_key in {"inat", "all"}:
                self._set_ai_status(message, "#e74c3c", source="inat")
            self._on_ai_thread_finished()
            self._on_inat_ai_thread_finished()


    def _on_ai_thread_finished(self) -> None:
        self._ai_thread = None
        if hasattr(self, "ai_guess_buttons") and "arts" in self.ai_guess_buttons:
            self.ai_guess_buttons["arts"].setText(self.tr("Guess"))
        self._update_ai_controls_state()

    def _on_inat_ai_thread_finished(self) -> None:
        self._inat_ai_thread = None
        if hasattr(self, "ai_guess_buttons") and "inat" in self.ai_guess_buttons:
            self.ai_guess_buttons["inat"].setText(self.tr("Guess"))
        self._update_ai_controls_state()

    def _park_thread_until_finished(self, thread: QThread | None) -> None:
        """Reparent a still-running thread away from the dialog so close won't destroy it."""
        if thread is None:
            return
        app = QApplication.instance()
        if app is None:
            try:
                if thread.parent() is self:
                    thread.setParent(None)
            except Exception:
                pass
            return
        try:
            if thread.parent() is self or thread.parent() is None:
                thread.setParent(app)
        except Exception:
            pass
        parked = getattr(app, "_sporely_parked_threads", None)
        if parked is None:
            parked = set()
            setattr(app, "_sporely_parked_threads", parked)
        try:
            parked.add(thread)
        except Exception:
            pass

        def _release_thread(t=thread, a=app):
            def _delayed_release():
                try:
                    parked_threads = getattr(a, "_sporely_parked_threads", None)
                    if parked_threads is not None:
                        parked_threads.discard(t)
                except Exception:
                    pass
            QTimer.singleShot(2000, _delayed_release)

        try:
            thread.finished.connect(thread.deleteLater)
        except Exception:
            pass
        try:
            thread.finished.connect(_release_thread)
        except Exception:
            pass

    def _cleanup_dialog_threads(self) -> None:
        if getattr(self, "_close_cleanup_done", False):
            return
        self._close_cleanup_done = True
        # Stage 3B.5: invalidate any deferred red-list resolve queued for
        # this dialog. The `_close_cleanup_done` check in the callback is
        # already sufficient; bumping the generation makes the invariant
        # symmetric with the load / clear paths.
        self._redlist_generation = getattr(self, "_redlist_generation", 0) + 1
        self._cleanup_location_lookup()
        for preview_path in list(self._dialog_temp_preview_paths):
            try:
                Path(preview_path).unlink(missing_ok=True)
            except Exception:
                pass
        self._dialog_temp_preview_paths.clear()
        self._dialog_preview_path_cache.clear()
        if self._artsobs_check_thread is not None:
            try:
                self._artsobs_check_thread.requestInterruption()
                self._artsobs_check_thread.wait(1000)
                if self._artsobs_check_thread.isRunning():
                    self._park_thread_until_finished(self._artsobs_check_thread)
            except Exception:
                pass
        if self._ai_thread is not None:
            try:
                self._ai_thread.quit()
                self._ai_thread.wait(1000)
                if self._ai_thread.isRunning():
                    self._park_thread_until_finished(self._ai_thread)
            except Exception:
                pass
        if self._inat_ai_thread is not None:
            try:
                self._inat_ai_thread.requestInterruption()
            except Exception:
                pass
            try:
                self._inat_ai_thread.wait(1000)
                if self._inat_ai_thread.isRunning():
                    self._park_thread_until_finished(self._inat_ai_thread)
            except Exception:
                pass
        if self._cloud_media_worker is not None:
            try:
                self._cloud_media_worker.requestInterruption()
            except Exception:
                pass
            try:
                self._cloud_media_worker.wait(1000)
                if self._cloud_media_worker.isRunning():
                    self._park_thread_until_finished(self._cloud_media_worker)
            except Exception:
                pass

    def done(self, result: int) -> None:  # noqa: N802 - Qt API
        self._cleanup_dialog_threads()
        super().done(result)

    def _publish_baseline_state(self) -> dict | None:
        """Sharing state before this save: the persisted row when editing,
        the cloud row for a cloud draft, nothing for a new observation."""
        baseline = getattr(self, "publish_baseline", None)
        if baseline is not None:
            return dict(baseline)
        if self.edit_mode and isinstance(self.observation, dict):
            obs_id = self.observation.get("id")
            if obs_id:
                try:
                    stored = ObservationDB.get_observation(int(obs_id))
                except Exception:
                    stored = None
                if stored:
                    return self._with_cloud_precision(dict(stored))
            return self._with_cloud_precision(dict(self.observation))
        return None

    @staticmethod
    def _with_cloud_precision(baseline: dict) -> dict:
        """The notice compares against what the cloud serves: a synced row's
        snapshot precision wins over a stale local value (an older build
        stored cloud 'hidden'/'region' as 'exact')."""
        cloud_id = str(baseline.get("cloud_id") or "").strip()
        if not cloud_id:
            return baseline
        try:
            from utils.cloud_sync import _snapshot_baseline_for_cloud_id

            cloud_precision = _snapshot_baseline_for_cloud_id(cloud_id).get("location_precision")
        except Exception:
            cloud_precision = None
        if cloud_precision:
            baseline["location_precision"] = cloud_precision
        return baseline

    # -- in place: the Rediger mode (stage 9g2) ------------------------------

    def set_in_place(self) -> None:
        """Show as Rediger's page rather than a window (owner 2026-11-11).

        The page saves as you go, so Save and Cancel go, and Esc keeps the
        page. Images are changed in Bilder, Vis and Mål: here the strip only
        chooses which images AI looks at, and Edit images opens Bilder ›
        Files and SD card.
        """
        self._in_place = True
        self.setWindowFlags(Qt.Widget)
        self.setMinimumSize(0, 0)  # the dialog's window minimum; the page's scroll area handles small windows
        if self.layout() is not None:
            self.layout().setContentsMargins(0, 0, 0, 0)  # the page has its own margins; a window's are not needed
            self.layout().setSpacing(6)
        try:
            self.finished.disconnect(self._save_geometry)  # a page has no window geometry to remember
        except (RuntimeError, TypeError):
            pass
        for button in (getattr(self, "submit_observation_btn", None), getattr(self, "cancel_btn", None)):
            if button is not None:
                button.hide()
                button.setEnabled(False)  # Enter in a field must not press the hidden Save
        gallery = getattr(self, "image_gallery", None)
        if gallery is not None:
            gallery.set_reorderable(False)
            gallery.set_context_actions(delete=False, delete_cloud_copy=False)

    def add_in_place_status(self, widget) -> None:
        """The page's save status, where Save was (bottom right)."""
        row = getattr(self, "_bottom_buttons", None)
        if row is not None:
            row.addWidget(widget, 0, Qt.AlignRight | Qt.AlignVCenter)

    def take_bottom_bar(self) -> QWidget | None:
        """The bottom row (hints, Edit images, status) as a widget of its own.

        In place the page pins it below the scrolling form, so it shows
        whatever the window's height. Its widgets stay this form's (the hint
        bar, Edit images); the page deletes the bar with the form.
        """
        row = getattr(self, "_bottom_buttons", None)
        layout = self.layout()
        if row is None or layout is None:
            return None
        layout.removeItem(row)
        row.setParent(None)
        bar = QWidget()
        bar.setObjectName("editBottomBar")
        bar.setLayout(row)  # moves the row's widgets into the bar
        row.setContentsMargins(0, 6, 0, 0)
        if getattr(self, "_edit_images_shortcut", None) is not None:
            # The form's Edit images shortcut covers its own children only:
            # the bar's buttons are no longer among them.
            shortcut = QShortcut(self._edit_images_shortcut.key(), bar)
            shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            shortcut.activated.connect(self._edit_images_from_modified_shortcut)
            self._bar_edit_images_shortcut = shortcut
        return bar

    def is_in_place(self) -> bool:
        return bool(getattr(self, "_in_place", False))

    def unfinished_taxon_text(self) -> str:
        """Species search text typed but not picked (never saved in place)."""
        picker = getattr(self, "taxon_picker", None)
        if picker is None or not picker.isEnabled():
            return ""
        try:
            return str(picker.unfinished_text() or "")
        except Exception:
            return ""

    def reject(self) -> None:  # noqa: D401 - Qt API
        if self.is_in_place():
            return  # Esc keeps the page; what is typed is saved as you go
        super().reject()

    def accept(self) -> None:  # noqa: D401 - Qt API
        """Stage 2c publish notice: confirm before a save that makes the
        observation public and not a draft. Cancel keeps the previous state."""
        if self.is_in_place():
            return  # Rediger saves as you go; nothing closes the page
        if not self._confirm_unfinished_taxon_search():
            return
        if not self.confirm_publish_change():
            return
        super().accept()

    def confirm_publish_change(self, changed_keys=None) -> bool:
        """The publish notice and the precision record before a save.

        Extracted from ``accept`` (stage 9g2) so Rediger's autosave asks the
        same way: False when the user cancels (the draft and visibility
        controls are put back). ``changed_keys``: Rediger writes only these
        columns, so the notice judges the stored row with them applied (a
        value changed elsewhere meanwhile is what will be published).
        """
        from ui.publish_notice import confirm_publish_if_needed, load_local_facts

        previous = self._publish_baseline_state()
        try:
            new_state = dict(self.get_data())
        except Exception:
            new_state = {}
        if changed_keys is not None and new_state:
            # The stored row as it is (not the cloud-adjusted baseline), with
            # only the changed columns applied: exactly what will be saved.
            obs_id = (self.observation or {}).get("id") if self.edit_mode else None
            stored = ObservationDB.get_observation(int(obs_id)) if obs_id else None
            merged = dict(stored or previous or {})
            merged.update({key: value for key, value in new_state.items() if key in changed_keys})
            new_state = merged
        if new_state and "has_photos" not in new_state:
            new_state["has_photos"] = bool(self.image_results)
        obs_id = (self.observation or {}).get("id") if self.edit_mode else None
        if new_state and not confirm_publish_if_needed(
            self, previous, new_state,
            lambda: load_local_facts(obs_id, new_state),
        ):
            self._restore_publish_controls(previous)
            return False
        # An explicit precision choice on this device (confirmed above when it
        # publishes or widens) is what lets sync push a more precise level.
        self.confirmed_location_precision = None
        chosen = new_state.get("location_precision")
        before = (previous or {}).get("location_precision")
        if (
            chosen
            and getattr(self, "_preserved_location_precision", None) is None
            and str(chosen).strip().lower() != str(before or "exact").strip().lower()
        ):
            self.confirmed_location_precision = str(chosen)
            if obs_id:
                from utils.cloud_sync import record_confirmed_location_precision

                record_confirmed_location_precision(obs_id, chosen)
        return True

    def _restore_publish_controls(self, previous: dict | None) -> None:
        """Put the draft and visibility controls back to the previous state."""
        previous = previous or {}
        if hasattr(self, "is_draft_checkbox"):
            self.is_draft_checkbox.setChecked(bool(previous.get("is_draft", True)) if previous else True)
        if previous:
            self._set_sharing_scope(
                previous.get("sharing_scope") or previous.get("visibility"),
                location_public=previous.get("location_public"),
            )
            self._set_location_precision(previous.get("location_precision"))

    def closeEvent(self, event):
        self._cleanup_dialog_threads()
        super().closeEvent(event)

    def _on_ai_guess_finished(
        self,
        indices: list,
        predictions: list,
        _box: object,
        _warnings: object,
        temp_paths: list,
    ) -> None:
        for temp_path in temp_paths or []:
            if not temp_path:
                continue
            if ai_image_prep.should_keep_ai_id_temp():
                continue
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass
        for index in indices or []:
            self._ai_predictions_by_index[index] = predictions or []
        self._update_ai_table()
        if predictions:
            self._set_ai_status(self.tr("AI suggestion updated"), "#27ae60", source="arts")
        else:
            self._set_ai_status(self.tr("No AI suggestions found"), "#7f8c8d", source="arts")
        self._update_ai_controls_state()
        self._persist_ai_state_now()
        # New Artsorakel identifiers may settle same-name taxa or prove the
        # entered name's taxon.
        self._refresh_identity_offer()

    def _on_ai_guess_error(self, _indices: list, message: str) -> None:
        if "500" in message:
            hint = self.tr("AI guess failed: server error (500). Try again later.")
        else:
            hint = self.tr("AI guess failed: {message}").format(message=message)
        self._set_ai_status(hint, "#e74c3c", source="arts")
        self._update_ai_controls_state()

    def _on_inat_ai_guess_finished(
        self,
        indices: list,
        predictions: list,
        _box: object,
        _warnings: object,
        temp_paths: list,
    ) -> None:
        for temp_path in temp_paths or []:
            if not temp_path:
                continue
            if ai_image_prep.should_keep_ai_id_temp():
                continue
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass
        for index in indices or []:
            self._inat_predictions_by_index[index] = predictions or []
        self._update_ai_table()
        if predictions:
            self._set_ai_status(self.tr("AI suggestion updated"), "#27ae60", source="inat")
        else:
            self._set_ai_status(self.tr("No AI suggestions found"), "#7f8c8d", source="inat")
        self._update_ai_controls_state()
        self._persist_ai_state_now()

    def _on_inat_ai_guess_error(self, _indices: list, message: str) -> None:
        self._set_ai_status(self.tr("AI guess failed: {message}").format(message=message), "#e74c3c", source="inat")
        self._update_ai_controls_state()

    def _on_edit_images_clicked(self):
        if self.is_in_place():
            self.editImagesRequested.emit("")
            return
        # Leaving for Prepare Images keeps the identification; unfinished
        # species search text needs the same decision as on Save.
        if not self._confirm_unfinished_taxon_search():
            return
        self.request_edit_images = True
        self.reject()

    def _on_image_double_clicked(self, _img_id, path: str) -> None:
        if self.is_in_place():
            self.editImagesRequested.emit(path or "")
            return
        # Leaving for Prepare Images keeps the identification; unfinished
        # species search text needs the same decision as on Save.
        if not self._confirm_unfinished_taxon_search():
            return
        self.request_edit_images = True
        self.request_edit_images_path = path or None
        self.reject()

    def _on_gallery_edit_requested(self, _image_id, filepath: str) -> None:
        if self.is_in_place():
            self.editImagesRequested.emit((filepath or "").strip())
            return
        # Right-click "Edit photo" routes through the same reject+
        # request_edit_images flag as the "Edit images" button and the
        # double-click handler — the parent tab picks it up and opens
        # Prepare Images for the requested path. The extra
        # request_edit_images_skip_return flag tells the caller not to
        # re-open the details dialog after Prepare Images closes; a
        # thumbnail right-click is a direct "just edit this photo" action.
        # Unfinished species search text needs the same decision as on Save.
        if not self._confirm_unfinished_taxon_search():
            return
        self.request_edit_images = True
        self.request_edit_images_path = (filepath or "").strip() or None
        self.request_edit_images_skip_return = True
        self.reject()

    def _apply_primary_metadata(self):
        if not self.image_results:
            return
        result = self._primary_result()
        if not result:
            return
        if result.captured_at:
            self.datetime_input.setDateTime(result.captured_at)
        if result.gps_latitude is not None:
            self.lat_input.setValue(result.gps_latitude)
        if result.gps_longitude is not None:
            self.lon_input.setValue(result.gps_longitude)
        source_name = ""
        if getattr(self, "_gps_source_index", None) is not None:
            idx = self._gps_source_index
            if idx is not None and 0 <= idx < len(self.image_results):
                source_name = Path(self.image_results[idx].filepath).name if self.image_results[idx].filepath else ""
        if not source_name:
            source_name = Path(result.filepath).name if result.filepath else ""
        if result.gps_latitude is not None or result.gps_longitude is not None:
            self.gps_info_label.setText(
                self.tr("From: {source}").format(source=source_name) if source_name else ""
            )
        else:
            self.gps_info_label.setText("")
        self._update_map_button()

    def _apply_suggested_taxon(self):
        if not self.suggested_taxon:
            return
        if self.is_unidentified():
            return
        if not hasattr(self, "genus_input") or not hasattr(self, "species_input"):
            return
        if self.genus_input.text().strip() or self.species_input.text().strip():
            return
        genus = self.suggested_taxon.get("genus")
        species = self.suggested_taxon.get("species")
        if not genus or not species:
            return
        self._suppress_taxon_autofill = True
        self.genus_input.setText(genus)
        self.species_input.setText(species)
        self._suppress_taxon_autofill = False
        if hasattr(self, "vernacular_input") and not self.vernacular_input.text().strip():
            vernacular = self._preferred_vernacular_from_taxon(self.suggested_taxon.get("taxon") or {})
            if vernacular:
                self._suppress_taxon_autofill = True
                self.vernacular_input.setText(vernacular)
                self._suppress_taxon_autofill = False
        if self.vernacular_db:
            self._update_vernacular_suggestions_for_taxon()
            self._maybe_set_vernacular_from_taxon()

    def _preferred_vernacular_from_taxon(self, taxon: dict) -> str | None:
        if not isinstance(taxon, dict):
            return None
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        vernacular_names = taxon.get("vernacularNames") or {}
        if isinstance(vernacular_names, dict):
            if lang:
                direct = vernacular_names.get(lang)
                if isinstance(direct, str) and direct.strip():
                    return direct.strip()
                # Some payloads use language variants (for example "de-DE" or "nb_NO").
                for code, value in vernacular_names.items():
                    if normalize_vernacular_language(str(code)) != lang:
                        continue
                    text = str(value).strip()
                    if text:
                        return text

        # If selected-language text is missing in AI payload, resolve from the shared local taxonomy lookup.
        genus, species = self._extract_genus_species_from_taxon(taxon)
        lookup = getattr(self, "_taxon_lookup", None)
        if lookup and genus and species:
            # The AI table is drawn several times while the form opens: look
            # each name up once (a local taxonomy query of tens of ms).
            # Keyed by the lookup and the language it answers in (which can
            # differ from the setting until the lookup is next synced).
            cache = self.__dict__.setdefault("_ai_common_name_cache", {})
            key = (
                id(lookup), getattr(lookup, "language_code", None),
                str(genus).casefold(), str(species).casefold(),
            )
            if key in cache:
                common = cache[key]
            else:
                try:
                    best_choice = lookup.best_common_name_for_taxon(genus, species)
                except Exception:
                    best_choice = None
                common = (best_choice.common_name or "").strip() if best_choice else ""
                cache[key] = common
            if common:
                return common

        name = taxon.get("vernacularName")
        if isinstance(name, str) and name.strip():
            return name.strip()
        if isinstance(vernacular_names, dict):
            for value in vernacular_names.values():
                text = str(value).strip()
                if text:
                    return text
        return None

    def _primary_result(self) -> ImageImportResult | None:
        if self.primary_index is not None and 0 <= self.primary_index < len(self.image_results):
            return self.image_results[self.primary_index]
        for item in self.image_results:
            if item.captured_at or item.gps_latitude is not None or item.gps_longitude is not None:
                return item
        return self.image_results[0] if self.image_results else None

    def select_images(self):
        """Select images and extract EXIF metadata."""
        from utils.exif_reader import get_image_metadata

        settings = get_app_settings()
        last_import_dir = settings.get("last_import_dir")
        if not last_import_dir or not Path(last_import_dir).exists():
            docs = QStandardPaths.writableLocation(QStandardPaths.DocumentsLocation)
            last_import_dir = docs if docs else str(Path.home())

        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Photos", last_import_dir,
            "Images (*.png *.jpg *.jpeg *.tif *.tiff *.heic *.heif);;All Files (*)"
        )
        if not files:
            return
        update_app_settings({"last_import_dir": str(Path(files[0]).parent)})

        # Add to existing files
        for filepath in files:
            if filepath not in self.image_files:
                self.image_files.append(filepath)
                metadata = get_image_metadata(filepath)
                metadata["filepath"] = filepath
                metadata["image_id"] = None
                self.image_metadata.append(metadata)
                # Default settings: field image, default objective
                self.image_settings.append({
                    'image_type': 'field',
                    'objective': self.default_objective,
                    'contrast': self._field_tag_value('contrast'),
                    'mount_medium': self._field_tag_value('mount'),
                    'stain': self._field_tag_value('stain'),
                    'sample_type': self._field_tag_value('sample')
                })

        self._update_image_table()

        # If this is the first batch of images, auto-populate date/GPS from last image
        if len(self.image_metadata) > 0:
            self._apply_metadata_from_index(len(self.image_metadata) - 1)

    def _update_image_table(self):
        """Update the image table with current images."""
        self.image_table.setRowCount(len(self.image_metadata))

        for row, meta in enumerate(self.image_metadata):
            filename = meta['filename']
            dt = meta.get('datetime')
            if dt:
                date_str = dt.strftime("%Y-%m-%d %H:%M")
                display = f"{filename}\n{date_str}"
            else:
                display = filename

            # Column 0: Filename/Date
            name_item = QTableWidgetItem(display)
            name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
            self.image_table.setItem(row, 0, name_item)

            # Column 1: Field radio button
            field_radio = QRadioButton()
            field_radio.setChecked(self.image_settings[row]['image_type'] == 'field')
            field_radio.toggled.connect(lambda checked, r=row: self._on_image_type_changed(r, 'field', checked))
            field_container = QWidget()
            field_layout = QHBoxLayout(field_container)
            field_layout.addWidget(field_radio)
            field_layout.setAlignment(Qt.AlignCenter)
            field_layout.setContentsMargins(0, 0, 0, 0)
            self.image_table.setCellWidget(row, 1, field_container)

            # Column 2: Micro radio button
            micro_radio = QRadioButton()
            micro_radio.setChecked(self.image_settings[row]['image_type'] == 'microscope')
            micro_radio.toggled.connect(lambda checked, r=row: self._on_image_type_changed(r, 'microscope', checked))
            micro_container = QWidget()
            micro_layout = QHBoxLayout(micro_container)
            micro_layout.addWidget(micro_radio)
            micro_layout.setAlignment(Qt.AlignCenter)
            micro_layout.setContentsMargins(0, 0, 0, 0)
            self.image_table.setCellWidget(row, 2, micro_container)

            # Link the radio buttons
            btn_group = QButtonGroup(self.image_table)
            btn_group.addButton(field_radio, 0)
            btn_group.addButton(micro_radio, 1)

            # Column 3: Objective dropdown
            obj_combo = QComboBox()
            obj_combo.setEnabled(self.image_settings[row]['image_type'] == 'microscope')
            obj_combo.setStyleSheet("""
                QComboBox { padding: 2px 4px; min-height: 24px; }
                QComboBox QAbstractItemView { min-height: 24px; }
            """)
            for key, obj in sorted(self.objectives.items(), key=lambda item: objective_sort_value(item[1], item[0])):
                label = objective_display_name(obj, key) or key
                obj_combo.addItem(label, key)
            # Set current objective
            current_obj = self.image_settings[row].get('objective', self.default_objective)
            idx = obj_combo.findData(current_obj)
            if idx >= 0:
                obj_combo.setCurrentIndex(idx)
            obj_combo.currentIndexChanged.connect(lambda idx, r=row, c=obj_combo: self._on_objective_changed(r, c))
            self.image_table.setCellWidget(row, 3, obj_combo)

            # Column 4: Contrast dropdown
            contrast_combo = QComboBox()
            contrast_combo.setEnabled(self.image_settings[row]['image_type'] == 'microscope')
            self._populate_tag_combo(contrast_combo, "contrast", self.contrast_options)
            current_contrast = DatabaseTerms.canonicalize(
                "contrast",
                self.image_settings[row].get('contrast', self.contrast_default),
            )
            idx = contrast_combo.findData(current_contrast)
            if idx >= 0:
                contrast_combo.setCurrentIndex(idx)
            self._set_tag_combo_neutral_display(
                contrast_combo,
                "contrast",
                self.image_settings[row]['image_type'] != 'microscope',
            )
            contrast_combo.currentIndexChanged.connect(lambda idx, r=row, c=contrast_combo: self._on_contrast_changed(r, c))
            self.image_table.setCellWidget(row, 4, contrast_combo)

            # Column 5: Mount medium dropdown
            mount_combo = QComboBox()
            mount_combo.setEnabled(self.image_settings[row]['image_type'] == 'microscope')
            self._populate_tag_combo(mount_combo, "mount", self.mount_options)
            current_mount = DatabaseTerms.canonicalize(
                "mount",
                self.image_settings[row].get('mount_medium', self.mount_default),
            )
            idx = mount_combo.findData(current_mount)
            if idx >= 0:
                mount_combo.setCurrentIndex(idx)
            self._set_tag_combo_neutral_display(
                mount_combo,
                "mount",
                self.image_settings[row]['image_type'] != 'microscope',
            )
            mount_combo.currentIndexChanged.connect(lambda idx, r=row, c=mount_combo: self._on_mount_changed(r, c))
            self.image_table.setCellWidget(row, 5, mount_combo)

            # Column 6: Stain dropdown
            stain_combo = QComboBox()
            stain_combo.setEnabled(self.image_settings[row]['image_type'] == 'microscope')
            self._populate_tag_combo(stain_combo, "stain", self.stain_options)
            current_stain = DatabaseTerms.canonicalize(
                "stain",
                self.image_settings[row].get('stain', self.stain_default),
            )
            idx = stain_combo.findData(current_stain)
            if idx >= 0:
                stain_combo.setCurrentIndex(idx)
            self._set_tag_combo_neutral_display(
                stain_combo,
                "stain",
                self.image_settings[row]['image_type'] != 'microscope',
            )
            stain_combo.currentIndexChanged.connect(lambda idx, r=row, c=stain_combo: self._on_stain_changed(r, c))
            self.image_table.setCellWidget(row, 6, stain_combo)

            # Column 7: Sample type dropdown
            sample_combo = QComboBox()
            sample_combo.setEnabled(self.image_settings[row]['image_type'] == 'microscope')
            self._populate_tag_combo(sample_combo, "sample", self.sample_options)
            current_sample = DatabaseTerms.canonicalize(
                "sample",
                self.image_settings[row].get('sample_type', self.sample_default),
            )
            idx = sample_combo.findData(current_sample)
            if idx >= 0:
                sample_combo.setCurrentIndex(idx)
            self._set_tag_combo_neutral_display(
                sample_combo,
                "sample",
                self.image_settings[row]['image_type'] != 'microscope',
            )
            sample_combo.currentIndexChanged.connect(lambda idx, r=row, c=sample_combo: self._on_sample_changed(r, c))
            self.image_table.setCellWidget(row, 7, sample_combo)

        # Select the last row
        if self.image_table.rowCount() > 0:
            self.image_table.selectRow(self.image_table.rowCount() - 1)

    def _on_image_type_changed(self, row, image_type, checked):
        """Handle image type radio button change."""
        if checked:
            self.image_settings[row]['image_type'] = image_type
            # Enable/disable objective dropdown
            obj_combo = self.image_table.cellWidget(row, 3)
            if obj_combo:
                obj_combo.setEnabled(image_type == 'microscope')
            contrast_combo = self.image_table.cellWidget(row, 4)
            if contrast_combo:
                contrast_combo.setEnabled(image_type == 'microscope')
                self._set_tag_combo_neutral_display(contrast_combo, 'contrast', image_type != 'microscope')
            mount_combo = self.image_table.cellWidget(row, 5)
            if mount_combo:
                mount_combo.setEnabled(image_type == 'microscope')
                self._set_tag_combo_neutral_display(mount_combo, 'mount', image_type != 'microscope')
            stain_combo = self.image_table.cellWidget(row, 6)
            if stain_combo:
                stain_combo.setEnabled(image_type == 'microscope')
                self._set_tag_combo_neutral_display(stain_combo, 'stain', image_type != 'microscope')
            sample_combo = self.image_table.cellWidget(row, 7)
            if sample_combo:
                sample_combo.setEnabled(image_type == 'microscope')
                self._set_tag_combo_neutral_display(sample_combo, 'sample', image_type != 'microscope')
            if image_type != 'microscope':
                self.image_settings[row]['contrast'] = self._field_tag_value('contrast')
                self.image_settings[row]['mount_medium'] = self._field_tag_value('mount')
                self.image_settings[row]['stain'] = self._field_tag_value('stain')
                self.image_settings[row]['sample_type'] = self._field_tag_value('sample')
                if contrast_combo:
                    idx = contrast_combo.findData(self.image_settings[row]['contrast'])
                    if idx >= 0:
                        contrast_combo.setCurrentIndex(idx)
                if mount_combo:
                    idx = mount_combo.findData(self.image_settings[row]['mount_medium'])
                    if idx >= 0:
                        mount_combo.setCurrentIndex(idx)
                if stain_combo:
                    idx = stain_combo.findData(self.image_settings[row]['stain'])
                    if idx >= 0:
                        stain_combo.setCurrentIndex(idx)
                if sample_combo:
                    idx = sample_combo.findData(self.image_settings[row]['sample_type'])
                    if idx >= 0:
                        sample_combo.setCurrentIndex(idx)

    def _on_objective_changed(self, row, combo):
        """Handle objective dropdown change."""
        self.image_settings[row]['objective'] = combo.currentData()

    def _on_mount_changed(self, row, combo):
        """Handle mount medium change."""
        self.image_settings[row]['mount_medium'] = combo.currentData()

    def _on_stain_changed(self, row, combo):
        """Handle stain change."""
        self.image_settings[row]['stain'] = combo.currentData()

    def _on_contrast_changed(self, row, combo):
        """Handle contrast change."""
        self.image_settings[row]['contrast'] = combo.currentData()

    def _on_sample_changed(self, row, combo):
        """Handle sample type change."""
        self.image_settings[row]['sample_type'] = combo.currentData()

    def on_image_selected(self):
        """Handle image selection in the table."""
        selected_rows = self.image_table.selectionModel().selectedRows()
        if not selected_rows or selected_rows[0].row() >= len(self.image_metadata):
            self.thumbnail_label.setText(self.tr("No image selected"))
            if hasattr(self, "delete_image_btn"):
                self.delete_image_btn.setEnabled(False)
            self.selected_image_index = -1
            return

        selected = selected_rows[0].row()
        self.selected_image_index = selected
        if hasattr(self, "delete_image_btn"):
            self.delete_image_btn.setEnabled(True)
        meta = self.image_metadata[selected]
        filepath = meta['filepath']

        # Show thumbnail - handle HEIC files specially
        pixmap = None
        suffix = Path(filepath).suffix.lower()

        if suffix in ('.heic', '.heif'):
            # Convert HEIC to QPixmap via PIL
            try:
                import pillow_heif
                from PIL import Image
                import io

                pillow_heif.register_heif_opener()
                with Image.open(filepath) as img:
                    # Convert to RGB if needed
                    if img.mode in ('RGBA', 'LA'):
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        background.paste(img, mask=img.split()[-1])
                        img = background
                    elif img.mode != 'RGB':
                        img = img.convert('RGB')

                    # Convert PIL image to QPixmap
                    buffer = io.BytesIO()
                    img.save(buffer, format='JPEG', quality=85)
                    buffer.seek(0)
                    qimage = QImage()
                    qimage.loadFromData(buffer.read())
                    pixmap = QPixmap.fromImage(qimage)
            except Exception as e:
                print(f"Error loading HEIC thumbnail: {e}")
                pixmap = None
        else:
            pixmap = QPixmap(filepath)

        if pixmap and not pixmap.isNull():
            scaled = pixmap.scaled(
                self.thumbnail_label.width() - 10,
                self.thumbnail_label.height() - 10,
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )
            self.thumbnail_label.setPixmap(scaled)
        else:
            self.thumbnail_label.setText(self.tr("Preview unavailable"))

        # Apply metadata from selected image
        self._apply_metadata_from_index(selected)

    def delete_selected_image(self):
        """Remove the selected image from the dialog."""
        selected_rows = self.image_table.selectionModel().selectedRows()
        if not selected_rows or selected_rows[0].row() >= len(self.image_metadata):
            return

        row = selected_rows[0].row()
        meta = self.image_metadata[row]
        image_id = meta.get("image_id")

        if image_id:
            measurements = MeasurementDB.get_measurements_for_image(image_id)
            if measurements:
                confirmed = ask_measurements_exist_delete(self, count=1)
            else:
                confirmed = self._question_yes_no(
                    self.tr("Confirm Delete"),
                    self.tr("Delete image?"),
                    default_yes=False
                )
        else:
            confirmed = self._question_yes_no(
                self.tr("Confirm Delete"),
                self.tr("Remove image from this observation?"),
                default_yes=False
            )
        if not confirmed:
            return

        if image_id:
            self.deleted_image_ids.add(image_id)

        # Remove from dialog state
        self.image_metadata.pop(row)
        self.image_settings.pop(row)
        if row < len(self.image_files):
            self.image_files.pop(row)

        self.selected_image_index = -1
        self._update_image_table()
        if self.image_table.rowCount() == 0:
            self.thumbnail_label.setText(self.tr("No image selected"))
            if hasattr(self, "delete_image_btn"):
                self.delete_image_btn.setEnabled(False)

    def _question_yes_no(self, title, text, default_yes=False):
        """Show a localized Yes/No confirmation dialog."""
        return ask_wrapped_yes_no(self, title, text, default_yes=default_yes)

    def _apply_metadata_from_index(self, index):
        """Apply date/time and GPS from the image at the given index."""
        if index < 0 or index >= len(self.image_metadata):
            return

        meta = self.image_metadata[index]

        # Set date/time from image EXIF
        if meta.get('datetime'):
            dt = meta['datetime']
            qdt = QDateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
            self.datetime_input.setDateTime(qdt)

        # Set GPS coordinates if available
        lat = meta.get('latitude')
        lon = meta.get('longitude')

        if lat is not None and lon is not None:
            self.lat_input.setValue(lat)
            self.lon_input.setValue(lon)
            self.gps_latitude = lat
            self.gps_longitude = lon
            self._image_exif_coordinates = (lat, lon)
            self.gps_info_label.setText(f"GPS from: {meta['filename']}")
            self.map_btn.setEnabled(True)
        else:
            self.gps_info_label.setText(self.tr("No GPS data in selected image"))
            self.map_btn.setEnabled(False)

    def _update_map_button(self):
        """Enable/disable the Map button based on whether valid coordinates are entered."""
        lat = self.lat_input.value()
        lon = self.lon_input.value()
        has_coords = lat > self.lat_input.minimum() and lon > self.lon_input.minimum()
        self.map_btn.setEnabled(has_coords)

    def _schedule_location_lookup(self):
        """Restart the debounce timer for location lookup."""
        if getattr(self, "_loading_form", False):
            self._deferred_location_lookup_pending = True
            return
        # Clear cached geography only when the coordinates really moved away
        # from the point it belongs to. The dialog also runs this deferred on
        # open, after `_load_observation_values` has restored the stored
        # country for the stored coordinates; treating that load as a change
        # erased a valid country whenever the refresh geocode could not
        # complete (offline), and an unrelated edit then saved NULL.
        cached_coords = getattr(self, "_location_country_coords", None)
        coords_moved = cached_coords is None or ObservationsTab._coords_meaningfully_changed(
            cached_coords[0], cached_coords[1],
            self.lat_input.value(), self.lon_input.value(),
        )
        if coords_moved:
            # Coordinates changed — any previously-cached country/region are
            # associated with the OLD point. Drop region_id (never invented)
            # and clear the country_code until the new geocode returns. This
            # is what makes the save path emit an explicit clear when a
            # lookup ultimately fails after a coordinate change.
            self._location_country_code = ""
            self._location_country_name = ""
            self._location_region_id = None
            self._location_country_coords = None
            self._refresh_location_reporting_summary()
        self._location_lookup_timer.start()

    def _do_location_lookup(self):
        """Fire off a background request to resolve coordinates to a place name."""
        lat = self.lat_input.value()
        lon = self.lon_input.value()
        if lat <= self.lat_input.minimum() or lon <= self.lon_input.minimum():
            return
        # Cancel any in-flight worker
        if self._location_lookup_worker is not None:
            try:
                self._location_lookup_worker.resultReady.disconnect(self._on_location_lookup_result)
            except Exception:
                pass
            try:
                self._location_lookup_worker.requestInterruption()
            except Exception:
                pass
            self._location_lookup_worker = None
        worker = LocationLookupWorker(lat, lon, parent=self)
        worker.resultReady.connect(self._on_location_lookup_result)
        worker.finished.connect(worker.deleteLater)
        worker.finished.connect(self._on_location_lookup_worker_finished)
        self._location_lookup_workers.add(worker)
        self._location_lookup_worker = worker
        worker.start()

    def _set_location_lookup_suggestions(self, suggestions: list[str]) -> None:
        """Store location lookup suggestions and refresh the Location popup."""
        clean: list[str] = []
        seen: set[str] = set()
        for value in suggestions or []:
            text = str(value or "").strip()
            if not text:
                continue
            key = " ".join(text.casefold().split())
            if key in seen:
                continue
            seen.add(key)
            clean.append(text)
        self._location_lookup_suggestions = clean
        if hasattr(self, "_location_suggestions_model"):
            self._location_suggestions_model.setStringList(clean)

    def _show_location_suggestions_dropdown(self) -> None:
        if not getattr(self, "_location_lookup_suggestions", None):
            return
        if getattr(self, "_close_cleanup_done", False) or not self.isVisible():
            return
        completer = getattr(self, "_location_suggestions_completer", None)
        if completer is None:
            return
        completer.setCompletionPrefix("")
        completer.complete()

    def _on_location_suggestion_selected(self, value: str) -> None:
        text = str(value or "").strip()
        if not text or not hasattr(self, "location_input"):
            return
        self.location_input.setText(text)
        self._last_applied_location_lookup_name = text

    def _on_location_lookup_result(self, result):
        """Store place-name suggestions from the API and apply the best one when appropriate."""
        if isinstance(result, LocationLookupResult):
            if not self._location_lookup_result_matches_current_coords(result):
                # Stale async result for an old coordinate — discard it so it
                # cannot overwrite geography associated with newer coords.
                return
            suggestions = list(result.suggestions)
            normalized_code = normalize_country_code(result.country_code)
            self._location_country_code = normalized_code or ""
            self._location_country_name = str(result.country_name or "").strip()
            try:
                self._location_country_coords = (
                    float(result.latitude),
                    float(result.longitude),
                )
            except (TypeError, ValueError):
                self._location_country_coords = None
        elif isinstance(result, (list, tuple)):
            suggestions = [str(value or "").strip() for value in result]
        else:
            suggestions = [str(result or "").strip()]
        self._set_location_lookup_suggestions(suggestions)
        resolved_name = self._location_lookup_suggestions[0] if self._location_lookup_suggestions else ""
        self._location_lookup_name = resolved_name
        current_name = (self.location_input.text() or "").strip() if hasattr(self, "location_input") else ""
        should_apply = (
            bool(getattr(self, "_force_apply_next_location_lookup_name", False))
            or not current_name
            or current_name == (self._last_applied_location_lookup_name or "").strip()
        )
        self._force_apply_next_location_lookup_name = False
        if resolved_name and should_apply:
            self.location_input.setText(resolved_name)
            self._last_applied_location_lookup_name = resolved_name
        self._apply_publish_target_from_lookup_country()
        # Stage 3B.5: recompute the red-list category for the new area
        # (e.g. NO → SJ or vice versa). If the country goes to something
        # else (or unknown), the deferred resolver clears the derived
        # category and preserves the Artsorakel snapshot JSON.
        self._schedule_final_redlist_resolution()
        self._location_lookup_worker = None

    def _location_lookup_result_matches_current_coords(self, result: LocationLookupResult) -> bool:
        try:
            result_lat = float(result.latitude)
            result_lon = float(result.longitude)
            current_lat = float(self.lat_input.value())
            current_lon = float(self.lon_input.value())
        except (TypeError, ValueError, AttributeError):
            return True
        return abs(result_lat - current_lat) < 0.000001 and abs(result_lon - current_lon) < 0.000001

    def _on_location_lookup_worker_finished(self) -> None:
        worker = self.sender()
        if isinstance(worker, QThread):
            def _discard(w=worker):
                self._location_lookup_workers.discard(w)
            QTimer.singleShot(2000, _discard)
            if self._location_lookup_worker is worker:
                self._location_lookup_worker = None

    def _cleanup_location_lookup(self) -> None:
        if getattr(self, "_location_lookup_timer", None) is not None:
            try:
                self._location_lookup_timer.stop()
            except Exception:
                pass
        worker = getattr(self, "_location_lookup_worker", None)
        self._location_lookup_worker = None
        workers = set(getattr(self, "_location_lookup_workers", set()) or set())
        if worker is not None:
            workers.add(worker)
        for w in list(workers):
            try:
                w.resultReady.disconnect(self._on_location_lookup_result)
            except Exception:
                pass
            try:
                w.requestInterruption()
            except Exception:
                pass
            try:
                if w.isRunning():
                    w.wait(200)
            except Exception:
                pass
            try:
                if w.isRunning():
                    self._park_thread_until_finished(w)
            except Exception:
                pass
        self._location_lookup_workers.clear()

    def _open_map_url(self):
        lat = self.lat_input.value()
        lon = self.lon_input.value()
        if lat > self.lat_input.minimum() and lon > self.lon_input.minimum():
            url = f"https://www.openstreetmap.org/#map=18/{lat:.6f}/{lon:.6f}"
        else:
            url = "https://www.openstreetmap.org"
        QDesktopServices.openUrl(QUrl(url))

    def _on_map_link_changed(self, text: str):
        coords = _extract_coords_from_osm_url(text)
        if not coords:
            return
        lat, lon = coords
        link_name = _extract_location_name_from_map_url(text)
        if link_name and hasattr(self, "location_input"):
            self.location_input.setText(link_name)
            self._location_lookup_name = link_name
            self._set_location_lookup_suggestions([link_name])
            self._last_applied_location_lookup_name = link_name
            self._force_apply_next_location_lookup_name = False
        else:
            self._force_apply_next_location_lookup_name = True
        self.lat_input.setValue(lat)
        self.lon_input.setValue(lon)
        self._update_map_button()
        self._schedule_location_lookup()

    def open_map(self):
        """Open the GPS coordinates in a map service."""
        lat = self.lat_input.value()
        lon = self.lon_input.value()

        # Check if we have valid coordinates (not at minimum/special value)
        if lat <= self.lat_input.minimum() or lon <= self.lon_input.minimum():
            return

        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        species_name = f"{genus} {species}".strip() if genus and species else None
        self.map_helper.show_map_service_dialog(lat, lon, species_name)

    def _resolved_country_code_for_current_coords(
        self,
        lat: float | None,
        lon: float | None,
    ) -> str | None:
        """Return the country code to save with the observation.

        Only propagate a country code when the value in memory was resolved
        from coordinates that match what is currently in the form. This keeps
        a stale async lookup from an older point out of the save path.
        """
        code = normalize_country_code(getattr(self, "_location_country_code", None))
        if not code:
            return None
        cached = getattr(self, "_location_country_coords", None)
        if lat is None or lon is None or not cached:
            return None
        try:
            cached_lat = float(cached[0])
            cached_lon = float(cached[1])
        except (TypeError, ValueError, IndexError):
            return None
        if abs(cached_lat - float(lat)) > 1e-6 or abs(cached_lon - float(lon)) > 1e-6:
            return None
        return code

    def _imported_photo_exif_coordinates(self, lat, lon) -> tuple[float, float] | None:
        """The EXIF GPS of a photo imported in this session, or ``None``.

        That is ``(lat, lon)`` when they equal one; otherwise the GPS-source
        photo's, else the first such photo's (rounded to the coordinate
        inputs' precision), so returning to it later is recognised.

        Uses the GPS each file's EXIF held at import (``exif_gps_*``), never
        ``gps_latitude``/``gps_longitude``, which Prepare Images overwrites
        with typed values. Only photos without a stored image row count:
        stored and cloud-materialised images can carry GPS that Sporely
        itself wrote, which is not evidence of where the photo was taken.
        """
        photos = []
        for result in self.image_results:
            if getattr(result, "image_id", None) is not None:
                continue
            exif_lat = getattr(result, "exif_gps_latitude", None)
            exif_lon = getattr(result, "exif_gps_longitude", None)
            if exif_lat is None or exif_lon is None:
                continue
            if coordinates_match(exif_lat, exif_lon, lat, lon):
                return (lat, lon)
            photos.append((bool(getattr(result, "gps_source", False)), exif_lat, exif_lon))
        if not photos:
            return None
        photos.sort(key=lambda photo: not photo[0])
        _gps_source, exif_lat, exif_lon = photos[0]
        decimals = self.lat_input.decimals()
        return (round(exif_lat, decimals), round(exif_lon, decimals))

    def _typed_in_prepare_images(self, lat, lon) -> bool:
        """Whether ``(lat, lon)`` were typed for a photo in Prepare Images."""
        return any(
            coordinates_match(
                getattr(result, "typed_gps_latitude", None),
                getattr(result, "typed_gps_longitude", None),
                lat,
                lon,
            )
            for result in self.image_results
        )

    def _current_coordinate_inputs(self) -> tuple[float | None, float | None]:
        lat = self.lat_input.value() if self.lat_input.value() > self.lat_input.minimum() else None
        lon = self.lon_input.value() if self.lon_input.value() > self.lon_input.minimum() else None
        return lat, lon

    def get_data(self):
        """Return observation data as dict."""
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        common_name = self.vernacular_input.text().strip() or None
        species_guess = f"{genus} {species}".strip() if genus and species else None
        unidentified = self.is_unidentified()

        # Stage 3B.3: scientific-name snapshot + rank come STRICTLY from
        # the taxonomy controller's committed_snapshot. Any manual edit to
        # genus / species / scientific-name field since the last picker
        # selection has already invalidated the snapshot inside the
        # controller. If invalidated, all identity snapshot fields save NULL.
        snapshot = None
        try:
            controller = getattr(self, "_taxon_controller", None)
            snapshot = controller.committed_snapshot() if controller else None
        except Exception:
            snapshot = None

        scientific_name_snapshot = (
            (snapshot or {}).get("scientific_name") if snapshot else None
        )
        taxon_rank_snapshot = (
            (snapshot or {}).get("taxon_rank_snapshot") if snapshot else None
        )

        # Stage 3B.5: persist the exact taxonomy-v2 concept alongside
        # the scientific-name and rank snapshots.
        #
        # Taxonomy-v2 closeout Stage 2: the integer is no longer read out of
        # the snapshot with a bare ``int()``. It comes from the typed identity,
        # which populates it only for a proven Sporely concept (or a legacy
        # value awaiting re-verification) and otherwise keeps the external
        # ``(source_system, namespace, external_id)`` evidence instead. All of
        # it round-trips through the ``taxon_identity_*`` columns so a restart
        # cannot silently downgrade an unresolved external identity to "no
        # identification".
        identity = TaxonIdentity.from_row(snapshot)
        identity_row = identity.to_row()
        sporely_taxon_id = identity_row.get("sporely_taxon_id")
        sharing_scope = self._selected_sharing_scope()
        location_precision = self._selected_location_precision()

        # Get GPS values (None if at minimum/special value)
        lat = None
        lon = None
        if self.lat_input.value() > self.lat_input.minimum():
            lat = self.lat_input.value()
        if self.lon_input.value() > self.lon_input.minimum():
            lon = self.lon_input.value()
        country_code = self._resolved_country_code_for_current_coords(lat, lon)
        location_pair = attribute_coordinate_edit(
            before_lat=self._opened_coordinates[0],
            before_lon=self._opened_coordinates[1],
            after_lat=lat,
            after_lon=lon,
            image_exif_coords=self._image_exif_coordinates,
            # A new observation without coordinates never observed a location
            # acquisition: it stays unattributed rather than "skipped".
            allow_clear=self.edit_mode,
        )
        if location_pair is None and coordinates_match(
            self._opened_coordinates[0], self._opened_coordinates[1], lat, lon,
        ):
            # Still the opened coordinates: keep their attribution (a pending
            # one from before an Edit Images detour must not be lost).
            location_pair = self._opened_location_pair
        # ``region_id`` is preserve-only from the cloud. We surface whatever was
        # loaded so the standard save path can pass it back into
        # ``update_observation`` unchanged; if coordinates changed we do NOT
        # invent one, and the sync layer will explicitly clear it when the
        # cloud row is patched.
        region_id = self._location_region_id if self._location_country_coords else None

        publish_target = normalize_publish_target(self.publish_target_combo.currentData())
        include_regional_habitat = self._regional_habitat_tabs_enabled()
        nin2_path = self._selected_habitat_tree_path("nin2") if include_regional_habitat else []
        substrate_path = self._selected_habitat_tree_path("substrate") if include_regional_habitat else []
        nin2_labels = [str(node.get("name") or "").strip() for node in nin2_path if isinstance(node, dict)]
        substrate_labels = [str(node.get("name") or "").strip() for node in substrate_path if isinstance(node, dict)]
        nin2_ids = [int(node.get("id")) for node in nin2_path if isinstance(node, dict) and node.get("id") is not None]
        substrate_ids = [int(node.get("id")) for node in substrate_path if isinstance(node, dict) and node.get("id") is not None]
        host_genus = self.host_genus_input.text().strip() if hasattr(self, "host_genus_input") else ""
        host_species = self.host_species_input.text().strip() if hasattr(self, "host_species_input") else ""
        host_vernacular = self.host_vernacular_input.text().strip() if hasattr(self, "host_vernacular_input") else ""
        host_scientific = f"{host_genus} {host_species}".strip()
        # An edit that leaves an empty host field empty keeps the stored empty
        # value (NULL or ''), so an unchanged save produces no sync change.
        loaded = self.observation if self.edit_mode and isinstance(self.observation, dict) else {}
        if not host_genus and loaded.get("habitat_host_genus") is None and "habitat_host_genus" in loaded:
            host_genus = None
        if not host_species and loaded.get("habitat_host_species") is None and "habitat_host_species" in loaded:
            host_species = None
        ai_selected = dict(
            getattr(self, "_current_ai_selected_fields", None)
            or self._ai_selected_fields(
                self.observation if isinstance(self.observation, dict) and self.observation else self.draft_data
            )
        )
        if unidentified:
            genus = None
            species = None
            common_name = None
            species_guess = None
            ai_selected = {
                "ai_selected_service": None,
                "ai_selected_taxon_id": None,
                "ai_selected_scientific_name": None,
                "ai_selected_probability": None,
                "ai_selected_at": None,
            }
        habitat_parts: list[str] = []
        if nin2_labels:
            habitat_parts.append(f"{self._nin2_tab_title()}: {' > '.join(nin2_labels)}")
        if substrate_labels:
            habitat_parts.append(f"{self._substrate_tab_title()}: {' > '.join(substrate_labels)}")
        if host_scientific or host_vernacular:
            if host_scientific and host_vernacular:
                habitat_parts.append(f"Grows on: {host_vernacular} ({host_scientific})")
            else:
                habitat_parts.append(f"Grows on: {host_scientific or host_vernacular}")

        return {
            'genus': genus,
            'species': species,
            'common_name': common_name,
            'inaturalist_taxon_id': None if unidentified else self._inaturalist_taxon_id,
            'red_list_category': None if unidentified else self._red_list_category or None,
            'red_list_categories_json': (
                json.dumps(self._red_list_categories, ensure_ascii=False)
                if not unidentified and isinstance(self._red_list_categories, dict) and self._red_list_categories
                else None
            ),
            'publish_target': publish_target,
            'is_draft': self.is_draft_checkbox.isChecked() if hasattr(self, "is_draft_checkbox") else True,
            'sharing_scope': sharing_scope,
            'location_public': sharing_scope_location_public(sharing_scope),
            'location_precision': location_precision,
            'species_guess': species_guess,
            **ai_selected,
            'uncertain': False if unidentified else self.uncertain_checkbox.isChecked(),
            'unspontaneous': self.unspontaneous_checkbox.isChecked(),
            'determination_method': None if unidentified else self.determination_method_combo.currentData(),
            'date': self.datetime_input.dateTime().toString("yyyy-MM-dd HH:mm:ss"),
            'location': self.location_input.text().strip() or None,
            'habitat': " | ".join(habitat_parts) if habitat_parts else None,
            'habitat_nin2_path': json.dumps(nin2_ids) if nin2_ids else None,
            'habitat_substrate_path': json.dumps(substrate_ids) if substrate_ids else None,
            'habitat_host_genus': host_genus,
            'habitat_host_species': host_species,
            'habitat_host_common_name': host_vernacular or None,
            'open_comment': self.open_comment_input.toPlainText().strip() or None,
            'private_comment': self.private_comment_input.toPlainText().strip() or None,
            'interesting_comment': False,
            'habitat_nin2_note': (
                self.nin2_note_input.toPlainText().strip()
                if include_regional_habitat and hasattr(self, "nin2_note_input")
                else ""
            ) or None,
            'habitat_substrate_note': (
                self.substrate_note_input.toPlainText().strip()
                if include_regional_habitat and hasattr(self, "substrate_note_input")
                else ""
            ) or None,
            'habitat_grows_on_note': (self.grows_on_note_input.toPlainText().strip() if hasattr(self, "grows_on_note_input") else "") or None,
            'gps_latitude': lat,
            'gps_longitude': lon,
            'country_code': country_code,
            'region_id': region_id,
            # Present only for a coordinate edit whose origin this dialog
            # knows (manual entry, a photo's EXIF, or a deliberate clear).
            **(
                dict(zip(LOCATION_PROVENANCE_FIELDS, location_pair))
                if location_pair is not None else {}
            ),
            'scientific_name_snapshot': scientific_name_snapshot,
            'taxon_rank_snapshot': taxon_rank_snapshot,
            'sporely_taxon_id': sporely_taxon_id,
            **{key: identity_row.get(key) for key in IDENTITY_COLUMNS},
        }

    def on_taxonomy_tab_changed(self, index):
        """Handle taxonomy tab changes."""
        _ = index
        if hasattr(self, "vernacular_input"):
            self.vernacular_input.setEnabled(True)
        self._update_taxonomy_tab_indicators()
        self._update_ai_controls_state()

    def _set_publish_target_combo(self, publish_target: str, manual_override: bool = False) -> None:
        if not hasattr(self, "publish_target_combo"):
            return
        self._publish_target_sync_in_progress = True
        try:
            normalized = normalize_publish_target(publish_target)
            idx = self.publish_target_combo.findData(normalized)
            if idx < 0:
                idx = 0
            self.publish_target_combo.setCurrentIndex(idx)
            self._publish_target_manual_override = bool(manual_override)
        finally:
            self._publish_target_sync_in_progress = False
        self._refresh_publish_target_hint()
        self._update_publish_target_specific_controls()

    def _active_reporting_target(self) -> str:
        return PUBLISH_TARGET_ARTSOBS_NO

    def _country_name_from_coords(self) -> str:
        return str(getattr(self, "_location_country_name", "") or "").strip()

    def _apply_publish_target_from_lookup_country(self) -> None:
        target = publish_target_from_country_code(getattr(self, "_location_country_code", ""))
        if target:
            self._set_publish_target_combo(target, manual_override=False)
            return
        self._refresh_location_reporting_summary()

    def _reporting_system_name(self) -> str:
        if not hasattr(self, "publish_target_combo"):
            return ""
        target = normalize_publish_target(self.publish_target_combo.currentData())
        if target == PUBLISH_TARGET_ARTPORTALEN_SE:
            return self.tr("Sweden")
        return self.tr("Norway")

    def _refresh_location_reporting_summary(self) -> None:
        country_name = self._country_name_from_coords()
        if hasattr(self, "country_summary_label"):
            self.country_summary_label.setText(
                self.tr("Country: {country}").format(country=country_name)
                if country_name
                else ""
            )
            self.country_summary_label.setVisible(bool(country_name))
        if hasattr(self, "reporting_system_summary_label"):
            system_name = self._reporting_system_name()
            self.reporting_system_summary_label.setText(
                self.tr("Reporting system: {target}").format(target=system_name)
                if system_name
                else ""
            )
            self.reporting_system_summary_label.setVisible(bool(system_name))
        self._apply_regional_habitat_tab_visibility()

    def _regional_habitat_tabs_enabled(self) -> bool:
        country_code = str(getattr(self, "_location_country_code", "") or "").strip().lower()
        if country_code in {"no", "se"}:
            return True
        # Until a country has been resolved, keep existing habitat data reachable.
        if not country_code and not self._country_name_from_coords():
            return True
        return False

    def _apply_regional_habitat_tab_visibility(self) -> None:
        tabs = getattr(self, "taxonomy_tabs", None)
        if tabs is None:
            return
        visible = self._regional_habitat_tabs_enabled()
        regional_tabs = [
            getattr(self, "nin2_tab", None),
            getattr(self, "substrate_tab", None),
        ]
        current_widget = tabs.currentWidget()
        for widget in regional_tabs:
            if widget is None:
                continue
            index = tabs.indexOf(widget)
            if index < 0:
                continue
            if hasattr(tabs, "setTabVisible"):
                tabs.setTabVisible(index, visible)
            else:
                widget.setVisible(visible)
        if not visible and current_widget in regional_tabs:
            fallback = getattr(self, "species_tab", None) or getattr(self, "grows_tab", None)
            fallback_index = tabs.indexOf(fallback)
            if fallback_index >= 0:
                tabs.setCurrentIndex(fallback_index)

    def _norwegian_habitat_tree_ids_enabled(self) -> bool:
        if not hasattr(self, "publish_target_combo"):
            return True
        target = normalize_publish_target(self.publish_target_combo.currentData())
        return target == PUBLISH_TARGET_ARTSOBS_NO

    def _habitat_tree_filename(self, key: str, target: str | None = None) -> str:
        normalized_target = normalize_publish_target(target or self.publish_target_combo.currentData())
        if key == "nin2":
            if normalized_target == PUBLISH_TARGET_ARTPORTALEN_SE:
                return "artportalen_biotopes_tree.json"
            return "nin2_biotopes_tree.json"
        if key == "substrate":
            if normalized_target == PUBLISH_TARGET_ARTPORTALEN_SE:
                return "artportalen_substrate_tree.json"
            return "substrate_tree.json"
        return ""

    def _habitat_group_title(self, key: str, target: str | None = None) -> str:
        normalized_target = normalize_publish_target(target or self.publish_target_combo.currentData())
        if key == "nin2":
            return self.tr("Biotope") if normalized_target == PUBLISH_TARGET_ARTPORTALEN_SE else self.tr("NIN2 biotope")
        if key == "substrate":
            return self.tr("Substrate")
        return ""

    def _habitat_target_note_text(self, key: str, target: str | None = None) -> str:
        return ""

    def _apply_habitat_tree_source(self, key: str, preserve_ids: list[int] | None = None) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        if not combos:
            return
        filename = self._habitat_tree_filename(key)
        roots = self._load_habitat_tree(filename)
        state["roots"] = roots
        state["index"] = self._build_habitat_tree_index(roots)
        state["source_file"] = filename
        group = state.get("group")
        header = state.get("group_header")
        if header is not None and hasattr(header, "set_title"):
            header.set_title(self._habitat_group_title(key))
        elif group is not None and hasattr(group, "setTitle"):
            group.setTitle(self._habitat_group_title(key))
        note_label = state.get("note_label")
        if note_label is not None:
            note_text = self._habitat_target_note_text(key)
            note_label.setText(note_text)
            note_label.setVisible(bool(note_text))
        ids = [int(v) for v in (preserve_ids or [])]
        self._populate_habitat_tree_level(key, 0, roots, ids[0] if ids else None)
        for level in range(1, len(combos)):
            prev_node = combos[level - 1].currentData()
            children = prev_node.get("children") if isinstance(prev_node, dict) else None
            self._populate_habitat_tree_level(
                key,
                level,
                children if isinstance(children, list) else [],
                ids[level] if level < len(ids) else None,
            )

    def _refresh_publish_target_hint(self) -> None:
        self._refresh_location_reporting_summary()
        if not hasattr(self, "publish_target_hint"):
            return
        self.publish_target_hint.clear()
        self.publish_target_hint.setVisible(False)

    def _update_publish_target_specific_controls(self) -> None:
        current_nin2_ids = [
            int(node.get("id"))
            for node in self._selected_habitat_tree_path("nin2")
            if isinstance(node, dict) and node.get("id") is not None
        ]
        current_substrate_ids = [
            int(node.get("id"))
            for node in self._selected_habitat_tree_path("substrate")
            if isinstance(node, dict) and node.get("id") is not None
        ]
        self._apply_habitat_tree_source("nin2", preserve_ids=current_nin2_ids)
        self._apply_habitat_tree_source("substrate", preserve_ids=current_substrate_ids)
        self._update_taxonomy_tab_indicators()

    def _on_publish_target_changed(self, _index: int) -> None:
        if not getattr(self, "_publish_target_sync_in_progress", False):
            self._publish_target_manual_override = True
        self._refresh_publish_target_hint()
        self._update_publish_target_specific_controls()
        self._update_taxonomy_tab_indicators()

    def _maybe_autoselect_publish_target_from_coords(self, _value=None) -> None:
        self._location_country_code = ""
        self._location_country_name = ""
        self._location_region_id = None
        self._location_country_coords = None
        self._refresh_location_reporting_summary()

    def _tab_title_with_state(self, title: str, filled: bool) -> str:
        marker = "●" if filled else "○"
        return f"{marker} {title}"

    def _nin2_tab_title(self) -> str:
        if self._norwegian_habitat_tree_ids_enabled():
            return self.tr("NIN2 biotope")
        return self.tr("Biotope")

    def _substrate_tab_title(self) -> str:
        return self.tr("Substrate")

    def _tab_has_data(self, key: str) -> bool:
        if key == "species":
            return any(
                [
                    bool(self.unidentified_checkbox.isChecked()),
                    (self.genus_input.text() or "").strip(),
                    (self.species_input.text() or "").strip(),
                    (self.vernacular_input.text() or "").strip(),
                    bool(self.determination_method_combo.currentData()),
                    bool(self.uncertain_checkbox.isChecked()),
                    bool(self.unspontaneous_checkbox.isChecked()),
                ]
            )
        if key == "nin2":
            return bool(self._selected_habitat_tree_path("nin2")) or bool(
                (self.nin2_note_input.toPlainText() or "").strip()
            )
        if key == "substrate":
            return bool(self._selected_habitat_tree_path("substrate")) or bool(
                (self.substrate_note_input.toPlainText() or "").strip()
            )
        if key == "grows":
            return any(
                [
                    (self.host_genus_input.text() or "").strip(),
                    (self.host_species_input.text() or "").strip(),
                    (self.host_vernacular_input.text() or "").strip(),
                    (self.grows_on_note_input.toPlainText() or "").strip(),
                ]
            )
        return False

    def is_unidentified(self) -> bool:
        checkbox = getattr(self, "unidentified_checkbox", None)
        return bool(checkbox is not None and checkbox.isChecked())

    def _on_unidentified_toggled(self, checked: bool) -> None:
        checked = bool(checked)
        for widget in (
            getattr(self, "vernacular_input", None),
            getattr(self, "vernacular_language_btn", None),
            getattr(self, "genus_input", None),
            getattr(self, "species_input", None),
            getattr(self, "uncertain_checkbox", None),
            getattr(self, "determination_method_combo", None),
            getattr(self, "taxon_picker", None),
        ):
            if widget is not None:
                widget.setEnabled(not checked)

        if checked:
            self._suppress_taxon_autofill = True
            self.vernacular_input.clear()
            self.genus_input.clear()
            self.species_input.clear()
            self.uncertain_checkbox.setChecked(False)
            self.determination_method_combo.setCurrentIndex(0)
            self._set_inaturalist_taxon_id(None)
            self._set_red_list_category(None, None)
            self._current_ai_selected_fields = self._ai_selected_fields(None)
            self._ai_selected_by_index.clear()
            self._inat_selected_by_index.clear()
            self._ai_selected_taxon = None
            self._update_selected_ai_summary_label(None)
            self._suppress_taxon_autofill = False
            picker = getattr(self, "taxon_picker", None)
            if picker is not None:
                picker.discard_unfinished()
        self._sync_taxon_picker(force=True)
        self._update_taxonomy_tab_indicators()

    def _update_taxonomy_tab_indicators(self) -> None:
        if not hasattr(self, "taxonomy_tabs"):
            return
        species_index = self.taxonomy_tabs.indexOf(getattr(self, "species_tab", None))
        nin2_index = self.taxonomy_tabs.indexOf(getattr(self, "nin2_tab", None))
        substrate_index = self.taxonomy_tabs.indexOf(getattr(self, "substrate_tab", None))
        grows_index = self.taxonomy_tabs.indexOf(getattr(self, "grows_tab", None))

        if species_index >= 0:
            self.taxonomy_tabs.setTabText(
                species_index, self._tab_title_with_state(self.tr("Species"), self._tab_has_data("species"))
            )
        if nin2_index >= 0:
            self.taxonomy_tabs.setTabText(
                nin2_index,
                self._tab_title_with_state(self._nin2_tab_title(), self._tab_has_data("nin2")),
            )
        if substrate_index >= 0:
            self.taxonomy_tabs.setTabText(
                substrate_index,
                self._tab_title_with_state(self._substrate_tab_title(), self._tab_has_data("substrate")),
            )
        if grows_index >= 0:
            self.taxonomy_tabs.setTabText(
                grows_index,
                self._tab_title_with_state(self._grows_on_tab_title(), self._tab_has_data("grows")),
            )

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_datetime_width()

    def showEvent(self, event):
        super().showEvent(event)
        if (
            getattr(self, "_initial_gallery_refresh_pending", False)
            or getattr(self, "_deferred_location_lookup_pending", False)
        ):
            QTimer.singleShot(0, self._complete_deferred_dialog_setup)

    def _update_datetime_width(self):
        """Keep Date & Time responsive with the details grid width."""
        if hasattr(self, "datetime_input"):
            self.datetime_input.setMinimumWidth(200)

    def _complete_deferred_dialog_setup(self) -> None:
        if getattr(self, "_initial_gallery_refresh_pending", False):
            self._initial_gallery_refresh_pending = False
            gallery_start = time.perf_counter()
            self._refresh_image_gallery_summary()
            self._refresh_cloud_media_materialization_controls(show_prompt=True)
            self._refresh_cloud_privacy_slots_summary(force=True)
            QTimer.singleShot(0, self._restore_dialog_gallery_splitter)
            _debug_import_flow(
                "deferred gallery refresh complete; "
                f"images={len(self.image_results)}; "
                f"elapsed={time.perf_counter() - gallery_start:.3f}s"
            )
        if getattr(self, "_deferred_location_lookup_pending", False):
            self._deferred_location_lookup_pending = False
            _debug_import_flow("starting deferred location lookup")
            self._schedule_location_lookup()
        created_at = getattr(self, "_debug_dialog_created_at", None)
        if created_at is not None:
            _debug_import_flow(
                f"ObservationDetailsDialog ready after {time.perf_counter() - created_at:.3f}s"
            )
            self._debug_dialog_created_at = None

    def _cloud_media_materialization_observation_id(self) -> int | None:
        obs = self.observation if isinstance(self.observation, dict) else None
        if not obs:
            return None
        try:
            obs_id = int(obs.get("id"))
        except (TypeError, ValueError):
            return None
        return obs_id if obs_id > 0 else None

    def _cloud_media_materialization_state(self) -> dict:
        obs_id = self._cloud_media_materialization_observation_id()
        if obs_id is None:
            state = {
                'status': 'skipped',
                'reason': 'no_observation_id',
                'local_observation_id': None,
                'cloud_observation_id': None,
                'snapshot_available': False,
                'snapshot_has_media': False,
                'remote_images_considered': 0,
                'remote_measurements_considered': 0,
                'local_images_total': 0,
                'local_images_ready': 0,
                'local_images_missing_files': 0,
                'local_measurements_total': 0,
                'local_measurements_linked': 0,
                'local_measurements_missing': 0,
                'needs_materialization': False,
                'can_auto_start': False,
                'warnings': [],
                'worker_running': bool(self._cloud_media_worker and self._cloud_media_worker.isRunning()),
            }
        else:
            state = dict(cloud_media_materialization_state_for_observation(obs_id) or {})
            state['worker_running'] = bool(self._cloud_media_worker and self._cloud_media_worker.isRunning())
        self._cloud_media_state = dict(state)
        return state

    def _set_cloud_media_status(self, message: str, *, level: str = "info", auto_clear_ms: int = 0) -> None:
        text = str(message or "").strip()
        controller = getattr(self, "_hint_controller", None)
        if controller is not None:
            try:
                controller.set_status(text, timeout_ms=auto_clear_ms, tone=level)
                return
            except Exception:
                pass
        self._set_hint(text, tone=level)

    def _refresh_cloud_media_materialization_controls(self, show_prompt: bool = False) -> dict:
        state = self._cloud_media_materialization_state()
        worker_running = bool(state.get('worker_running'))
        if _cloud_media_materialization_should_launch(state, worker_running) and not self._cloud_media_auto_start_attempted:
            self._cloud_media_auto_start_attempted = True
            self.download_cloud_media_btn.setVisible(False)
            self.download_cloud_media_btn.setEnabled(False)
            self._set_cloud_media_status(
                self.tr("Cloud media available. Downloading…"),
                level="info",
                auto_clear_ms=8000,
            )
            self._start_cloud_media_materialization(auto_start=True)
            return state

        if worker_running:
            self.download_cloud_media_btn.setVisible(False)
            self.download_cloud_media_btn.setEnabled(False)
            if show_prompt:
                self._set_cloud_media_status(
                    self.tr("Cloud media available. Downloading…"),
                    level="info",
                    auto_clear_ms=8000,
                )
            return state

        needs_materialization = bool(state.get('needs_materialization'))
        if needs_materialization:
            self.download_cloud_media_btn.setVisible(True)
            self.download_cloud_media_btn.setEnabled(True)
            self.download_cloud_media_btn.setText(self.tr("Download media"))
            if show_prompt and not bool(state.get('can_auto_start')):
                self._set_cloud_media_status(
                    self.tr("Cloud media available on this observation."),
                    level="info",
                    auto_clear_ms=8000,
                )
        else:
            self.download_cloud_media_btn.setVisible(False)
            self.download_cloud_media_btn.setEnabled(False)
        return state

    def _download_cloud_media_clicked(self) -> None:
        state = self._cloud_media_materialization_state()
        if not bool(state.get('needs_materialization')):
            self.download_cloud_media_btn.setVisible(False)
            self.download_cloud_media_btn.setEnabled(False)
            return
        self._start_cloud_media_materialization(auto_start=False)

    def _start_cloud_media_materialization(self, *, auto_start: bool) -> None:
        obs_id = self._cloud_media_materialization_observation_id()
        if obs_id is None:
            self._set_cloud_media_status(
                self.tr("Cloud media could not be loaded for this observation."),
                level="warning",
                auto_clear_ms=12000,
            )
            return
        worker = self._cloud_media_worker
        if worker is not None and worker.isRunning():
            return
        worker = _CloudMediaMaterializationWorker(obs_id, parent=self)
        self._cloud_media_worker = worker
        worker.progress.connect(self._on_cloud_media_materialization_progress)
        worker.materialization_finished.connect(self._on_cloud_media_materialization_finished)
        worker.error.connect(self._on_cloud_media_materialization_error)
        worker.finished.connect(self._on_cloud_media_materialization_worker_finished)
        worker.finished.connect(worker.deleteLater)
        self.download_cloud_media_btn.setVisible(False)
        self.download_cloud_media_btn.setEnabled(False)
        self._cloud_media_auto_start_attempted = True
        self._set_cloud_media_status(
            self.tr("Cloud media available. Downloading…"),
            level="info",
            auto_clear_ms=8000,
        )
        worker.start()

    def _on_cloud_media_materialization_progress(self, message: str, current: int, total: int) -> None:
        total_i = max(1, int(total or 0))
        current_i = max(1, min(total_i, int(current or 0) + 1))
        progress_message = str(message or "").strip() or self.tr("Downloading cloud media…")
        if progress_message.startswith("Observation "):
            display_message = progress_message
        else:
            display_message = _format_cloud_sync_observation_status(
                self.observation if isinstance(self.observation, dict) else None,
                self.tr("Downloading cloud media {current}/{total}: {message}").format(
                    current=current_i,
                    total=total_i,
                    message=progress_message,
                ),
            )
        self._set_cloud_media_status(
            display_message,
            level="info",
            auto_clear_ms=8000,
        )

    def _on_cloud_media_materialization_finished(self, result: dict) -> None:
        summary = dict(result or {})
        status = str(summary.get('status') or '').strip().lower()
        self._reload_observation_images_after_cloud_media_materialization()
        self._refresh_cloud_media_materialization_controls(show_prompt=False)
        if status == 'ok':
            self._set_cloud_media_status(
                self.tr("Cloud media downloaded."),
                level="success",
                auto_clear_ms=6000,
            )
        elif status == 'partial':
            warnings = [str(item or '').strip() for item in (summary.get('warnings') or []) if str(item or '').strip()]
            warning_text = (
                warnings[0]
                if warnings
                else self.tr("Cloud media downloaded with warnings.")
            )
            self._set_cloud_media_status(
                warning_text,
                level="warning",
                auto_clear_ms=12000,
            )
        elif status == 'error':
            errors = [str(item or '').strip() for item in (summary.get('errors') or []) if str(item or '').strip()]
            error_text = (
                errors[0]
                if errors
                else self.tr("Cloud media download failed.")
            )
            self._set_cloud_media_status(
                error_text,
                level="warning",
                auto_clear_ms=12000,
            )

    def _on_cloud_media_materialization_error(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        self._set_cloud_media_status(
            self.tr("Cloud media download failed: {message}").format(message=text),
            level="warning",
            auto_clear_ms=12000,
        )
        self._refresh_cloud_media_materialization_controls(show_prompt=False)

    def _on_cloud_media_materialization_worker_finished(self) -> None:
        worker = self._cloud_media_worker
        if worker is not None and worker.isRunning():
            return
        self._cloud_media_worker = None
        self.download_cloud_media_btn.setEnabled(True)
        self._refresh_cloud_media_materialization_controls(show_prompt=False)

    def _build_materialized_image_results_from_images(self, images: list[dict]) -> list[ImageImportResult]:
        objectives = load_objectives()
        resize_preference = bool(SettingsDB.get_setting("resize_to_optimal_sampling", False))
        store_original = bool(SettingsDB.get_setting("store_original_images", False))
        results: list[ImageImportResult] = []
        for img in images:
            if not img:
                continue
            filepath = str(img.get("filepath") or "").strip()
            if not filepath:
                continue
            meta = get_image_metadata(filepath)
            dt_value = meta.get("datetime")
            captured_at = QDateTime(dt_value) if dt_value else None
            exif_has_gps = meta.get("latitude") is not None or meta.get("longitude") is not None
            image_type = (img.get("image_type") or "field").strip().lower()
            objective_name = img.get("objective_name")
            resolved_key = resolve_objective_key(objective_name, objectives)
            scale_value = img.get("scale_microns_per_pixel")
            custom_scale = None
            if scale_value is not None and (objective_name == "Custom" or not resolved_key):
                try:
                    custom_scale = float(scale_value)
                except (TypeError, ValueError):
                    custom_scale = None
            resize_to_optimal = resize_preference
            resample_factor = img.get("resample_scale_factor")
            if image_type == "microscope":
                try:
                    parsed_resample = float(resample_factor) if resample_factor is not None else None
                except (TypeError, ValueError):
                    parsed_resample = None
                if parsed_resample is not None:
                    resize_to_optimal = 0.0 < parsed_resample < 0.999
                else:
                    original_path = str(img.get("original_filepath") or "").strip()
                    if original_path and original_path != filepath:
                        resize_to_optimal = True
            objective_value = resolved_key if resolved_key else (None if objective_name == "Custom" else objective_name)
            results.append(
                ImageImportResult(
                    filepath=filepath,
                    preview_path=filepath,
                    image_id=img.get("id"),
                    image_type=image_type,
                    objective=objective_value,
                    custom_scale=custom_scale,
                    contrast=img.get("contrast"),
                    mount_medium=img.get("mount_medium"),
                    stain=img.get("stain"),
                    sample_type=img.get("sample_type"),
                    sample_source=img.get("sample_source"),
                    notes=img.get("notes"),
                    captured_at=captured_at,
                    exif_has_gps=exif_has_gps,
                    crop_mode=img.get("crop_mode"),
                    gps_source=bool(img.get("gps_source")) if img.get("gps_source") is not None else False,
                    resample_scale_factor=resample_factor,
                    resize_to_optimal=resize_to_optimal,
                    store_original=store_original,
                    original_filepath=str(img.get("original_filepath") or filepath),
                    lab_metadata=dict(img.get("lab_metadata") or {}) or None,
                )
            )
        return results

    def _reload_observation_images_after_cloud_media_materialization(self) -> None:
        obs_id = self._cloud_media_materialization_observation_id()
        if obs_id is None:
            return
        try:
            images = ImageDB.get_images_for_observation(obs_id)
        except Exception:
            images = []
        selected_paths = list(self.image_gallery.selected_paths()) if hasattr(self, "image_gallery") else []
        self.image_results = self._build_materialized_image_results_from_images(images)
        self.primary_index = None if not self.image_results else min(
            max(0, self.primary_index or 0),
            len(self.image_results) - 1,
        )
        self._gps_source_index = self._resolve_gps_source_index()
        self._refresh_image_gallery_summary()
        if selected_paths and hasattr(self, "image_gallery"):
            existing_paths = {item.filepath for item in self.image_results if item.filepath}
            self.image_gallery.select_paths([path for path in selected_paths if path in existing_paths])
        if hasattr(self, "image_gallery") and not self.image_gallery.selected_paths() and self.image_results:
            self._select_initial_ai_image()
        self._update_ai_controls_state()
        self._update_ai_table()

    def _resolve_gps_source_index(self) -> int | None:
        source_idx = None
        for idx, item in enumerate(self.image_results):
            if getattr(item, "gps_source", False):
                source_idx = idx
                break
        if source_idx is not None:
            for i, item in enumerate(self.image_results):
                item.gps_source = i == source_idx
            return source_idx
        if not self._observation_datetime:
            return None
        for idx, item in enumerate(self.image_results):
            if item.exif_has_gps and self._matches_observation_datetime(item.captured_at):
                return idx
        return None

    def _build_dialog_preview_path(self, source_path: str) -> str | None:
        source = str(source_path or "").strip()
        if not source:
            return None
        source_file = Path(source)
        if not source_file.exists() or not source_file.is_file():
            return None
        cached = self._dialog_preview_path_cache.get(source)
        if cached and Path(cached).exists():
            return cached

        reader = QImageReader(str(source_file))
        reader.setAutoTransform(True)
        target_dim = 240
        size = reader.size()
        if size.isValid():
            if max(size.width(), size.height()) <= target_dim:
                self._dialog_preview_path_cache[source] = str(source_file)
                return str(source_file)
            scaled_size = QSize(size)
            scaled_size.scale(target_dim, target_dim, Qt.KeepAspectRatio)
            if scaled_size.isValid() and scaled_size.width() > 0 and scaled_size.height() > 0:
                reader.setScaledSize(scaled_size)

        image = reader.read()
        if image.isNull():
            return None

        preview_dir = Path(tempfile.gettempdir()) / "sporely_dialog_previews"
        preview_dir.mkdir(parents=True, exist_ok=True)
        preview_name = f"{source_file.stem}_{abs(hash(source)) & 0xffffffff:08x}.jpg"
        preview_path = preview_dir / preview_name
        if not image.save(str(preview_path), "JPEG", 88):
            return None

        preview_text = str(preview_path)
        self._dialog_temp_preview_paths.add(preview_text)
        self._dialog_preview_path_cache[source] = preview_text
        return preview_text

    def _image_gallery_preview_path(self, item: ImageImportResult) -> str:
        if item.image_id:
            thumb_preview = get_thumbnail_path(item.image_id, "small")
            if thumb_preview and Path(thumb_preview).exists():
                return str(thumb_preview)
        for candidate in (item.preview_path, item.filepath):
            preview_path = self._build_dialog_preview_path(candidate or "")
            if preview_path:
                return preview_path
        return item.filepath or ""

    @staticmethod
    def _get_image_size(path: str | None) -> tuple[int, int] | None:
        if not path:
            return None
        try:
            with Image.open(path) as img:
                return img.width, img.height
        except Exception:
            return None

    def _image_gallery_ai_crop_preview(
        self,
        item: ImageImportResult,
    ) -> tuple[tuple[float, float, float, float] | None, tuple[int, int] | None]:
        crop_box = ImageImportDialog._normalize_crop_box(getattr(item, "ai_crop_box", None))
        crop_source_size = getattr(item, "ai_crop_source_size", None)
        if crop_box:
            if not (crop_source_size and len(crop_source_size) == 2):
                crop_source_size = self._get_image_size(item.filepath or item.preview_path or item.original_filepath)
            if crop_source_size and len(crop_source_size) == 2:
                try:
                    crop_source_size = (
                        max(1, int(crop_source_size[0])),
                        max(1, int(crop_source_size[1])),
                    )
                except (TypeError, ValueError):
                    crop_source_size = None
            return crop_box, crop_source_size
        image_type = str(getattr(item, "image_type", "") or "").strip().lower()
        crop_mode = str(getattr(item, "crop_mode", "") or "").strip().lower()
        if image_type != "field" or crop_mode == "image":
            return None, None
        crop_source_size = self._get_image_size(item.filepath or item.preview_path or item.original_filepath)
        if not (crop_source_size and len(crop_source_size) == 2):
            return None, None
        try:
            crop_source_size = (
                max(1, int(crop_source_size[0])),
                max(1, int(crop_source_size[1])),
            )
        except (TypeError, ValueError):
            return None, None
        crop_box = ai_image_prep.get_default_ai_crop_rect(crop_source_size[0], crop_source_size[1])
        if not crop_box:
            return None, None
        return crop_box, crop_source_size

    @staticmethod
    def _image_result_key(item: ImageImportResult):
        if item.image_id is not None:
            return item.image_id
        return str(item.filepath or "")

    def _remap_index_dict_after_reorder(
        self,
        source: dict[int, object],
        old_results: list[ImageImportResult],
        new_index_by_key: dict[object, int],
    ) -> dict[int, object]:
        remapped: dict[int, object] = {}
        for old_index, value in source.items():
            if old_index < 0 or old_index >= len(old_results):
                continue
            key = self._image_result_key(old_results[old_index])
            new_index = new_index_by_key.get(key)
            if new_index is None:
                continue
            remapped[int(new_index)] = value
        return remapped

    def _refresh_image_gallery_summary(self) -> None:
        if not hasattr(self, "image_gallery"):
            return
        start_time = time.perf_counter()
        items = []
        selected_microscope_group_keys: set[str] = set()
        cloud_state_cache: dict[int, tuple[str | None, bool, bool, str]] = {}

        def _cloud_state_for_image_id(image_id: int | None) -> tuple[str | None, bool, bool, str]:
            try:
                parsed_id = int(image_id)
            except (TypeError, ValueError):
                return None, False, False, CLOUD_IMAGE_STATE_NONE
            if parsed_id in cloud_state_cache:
                return cloud_state_cache[parsed_id]
            cloud_id: str | None = None
            tombstone = None
            try:
                image_row = ImageDB.get_image(parsed_id)
            except Exception:
                image_row = None
            if image_row:
                cloud_id = str(image_row.get("cloud_id") or "").strip() or None
                if cloud_id:
                    tombstone = ImageDB.get_image_tombstone_by_deleted_cloud_id(cloud_id)
            cloud_state = ImageGalleryWidget._derive_cloud_state(cloud_id, tombstone)
            cloud_uploaded = cloud_state == CLOUD_IMAGE_STATE_UPLOADED
            cloud_tombstone_synced = cloud_state == CLOUD_IMAGE_STATE_DELETED
            cloud_state_cache[parsed_id] = (cloud_id, cloud_uploaded, cloud_tombstone_synced, cloud_state)
            return cloud_state_cache[parsed_id]

        for idx, item in enumerate(self.image_results):
            gps_match = idx == self._gps_source_index and item.exif_has_gps
            needs_scale = bool(item.needs_scale)
            if not needs_scale:
                needs_scale = (
                    (item.image_type or "field").strip().lower() == "microscope"
                    and not item.objective
                    and not item.custom_scale
                )
            objective_label = item.objective
            if item.objective and item.objective in self.objectives:
                objective_label = objective_display_name(
                    self.objectives[item.objective],
                    item.objective,
                ) or item.objective
            objective_short = ImageGalleryWidget._short_objective_label(objective_label, self.tr) or objective_label
            publish_selected = None
            if item.image_id is None:
                publish_selected, selected_microscope_group_keys = _default_publish_selected_for_new_image(
                    item.image_type,
                    _image_microscope_publish_group_key(
                        {
                            "objective_name": item.objective,
                            "lab_metadata": item.lab_metadata,
                        },
                        getattr(self, "objectives", None),
                    ),
                    selected_microscope_group_keys,
                )
            badges = ImageGalleryWidget.build_gallery_badges(
                image_type=item.image_type,
                objective_name=objective_short,
                contrast=item.contrast,
                custom_scale=bool(item.custom_scale),
                needs_scale=needs_scale,
                lab_metadata=item.lab_metadata,
                translate=self.tr,
            )
            # Colored bottom-left microscope tag so this gallery matches the
            # Observations and Prepare Images galleries. Also drop the first
            # (objective/contrast) badge to avoid duplicating the info shown
            # in the colored tag itself.
            microscope_tag_text, microscope_tag_color = _microscope_tag_from_image(
                {
                    "objective_name": item.objective,
                    "contrast": item.contrast,
                    "lab_metadata": item.lab_metadata,
                },
                translate=self.tr,
            )
            if microscope_tag_text and badges and (item.image_type or "").strip().lower() == "microscope":
                badges = badges[1:]
            has_measurements = False
            if item.image_id:
                has_measurements = bool(MeasurementDB.get_measurements_for_image(item.image_id))
            cloud_id = None
            cloud_uploaded = None
            cloud_tombstone_synced = None
            cloud_state = None
            if item.image_id:
                cloud_id, cloud_uploaded, cloud_tombstone_synced, cloud_state = _cloud_state_for_image_id(item.image_id)
            crop_box, crop_source_size = self._image_gallery_ai_crop_preview(item)
            items.append(
                {
                    "id": item.image_id,
                    "filepath": item.filepath,
                    "preview_path": self._image_gallery_preview_path(item),
                    "image_number": idx + 1,
                    "crop_box": crop_box,
                    "crop_source_size": crop_source_size,
                    "gps_tag_text": self.tr("GPS") if gps_match else None,
                    "gps_tag_highlight": gps_match,
                    "badges": badges,
                    "microscope_tag_text": microscope_tag_text,
                    "microscope_tag_color": microscope_tag_color,
                    "publish_selected": publish_selected,
                    "publish_selected_default": (item.image_type or "field").strip().lower() != "microscope",
                    "has_measurements": has_measurements,
                    "cloud_id": cloud_id,
                    "cloud_uploaded": cloud_uploaded,
                    "cloud_tombstone_synced": cloud_tombstone_synced,
                    "cloud_state": cloud_state,
                }
            )
        self.image_gallery.set_items(items)
        _debug_import_flow(
            f"_refresh_image_gallery_summary built {len(items)} items in {time.perf_counter() - start_time:.3f}s"
        )

    def _on_gallery_items_reordered(self, ordered_keys: list[object]) -> None:
        old_results = list(self.image_results)
        if len(old_results) < 2:
            return

        ordered_results: list[ImageImportResult] = []
        seen: set[object] = set()
        for key in ordered_keys or []:
            for item in old_results:
                item_key = self._image_result_key(item)
                if item_key != key or item_key in seen:
                    continue
                ordered_results.append(item)
                seen.add(item_key)
                break
        for item in old_results:
            item_key = self._image_result_key(item)
            if item_key in seen:
                continue
            ordered_results.append(item)
            seen.add(item_key)
        if ordered_results == old_results:
            return

        selected_keys = []
        for path in self.image_gallery.selected_paths():
            for item in old_results:
                if item.filepath == path:
                    selected_keys.append(self._image_result_key(item))
                    break
        primary_key = None
        if self.primary_index is not None and 0 <= self.primary_index < len(old_results):
            primary_key = self._image_result_key(old_results[self.primary_index])
        ai_key = None
        if self._ai_selected_index is not None and 0 <= self._ai_selected_index < len(old_results):
            ai_key = self._image_result_key(old_results[self._ai_selected_index])

        self.image_results[:] = ordered_results
        new_index_by_key = {
            self._image_result_key(item): idx
            for idx, item in enumerate(self.image_results)
        }
        self._ai_predictions_by_index = self._remap_index_dict_after_reorder(
            self._ai_predictions_by_index,
            old_results,
            new_index_by_key,
        )
        self._ai_selected_by_index = self._remap_index_dict_after_reorder(
            self._ai_selected_by_index,
            old_results,
            new_index_by_key,
        )
        self.primary_index = new_index_by_key.get(primary_key)
        self._ai_selected_index = new_index_by_key.get(ai_key)
        self._gps_source_index = self._resolve_gps_source_index()
        self._refresh_image_gallery_summary()

        selected_paths = [
            self.image_results[new_index_by_key[key]].filepath
            for key in selected_keys
            if key in new_index_by_key
        ]
        if selected_paths:
            self.image_gallery.select_paths(selected_paths)
        if len(selected_paths) <= 1:
            self._select_initial_ai_image()
        else:
            self._update_ai_controls_state()
            self._update_ai_table()

    def _remap_ai_indices(self, removed_indices: list[int]) -> None:
        def new_index(old_index: int) -> int:
            shift = 0
            for removed in removed_indices:
                if removed < old_index:
                    shift += 1
            return old_index - shift

        def remap_dict(source: dict[int, object]) -> dict[int, object]:
            remapped = {}
            for old_index, value in source.items():
                if old_index in removed_indices:
                    continue
                remapped[new_index(old_index)] = value
            return remapped

        self._ai_predictions_by_index = remap_dict(self._ai_predictions_by_index)
        self._ai_selected_by_index = remap_dict(self._ai_selected_by_index)
        self._ai_selected_taxon = None

    def _on_details_dialog_gallery_selection_changed(self, selected_paths) -> None:
        try:
            count = len([p for p in (selected_paths or []) if p])
        except Exception:
            count = 0
        active = count > 1
        previous = bool(getattr(self, "_gallery_multi_hint_active", False))
        if active == previous:
            return
        self._gallery_multi_hint_active = active
        controller = getattr(self, "_hint_controller", None)
        if controller is None:
            return
        if active:
            controller.set_hint(self.tr("Selected images will be edited"), tone="info")
        else:
            controller.set_hint("")

    def _on_details_dialog_gallery_delete_images_requested(self, keys) -> None:
        keys_list = list(keys or [])
        if not keys_list:
            return
        if len(keys_list) == 1:
            self._on_gallery_delete_requested(keys_list[0])
        else:
            self._on_gallery_delete_selection_requested(keys_list)

    def _on_gallery_delete_cloud_copies_requested(self, gallery, image_ids) -> None:
        """Confirm and tombstone cloud copies for the details-dialog gallery.

        Mirrors ``ObservationsTab._on_gallery_delete_cloud_copies_requested``
        but uses the dialog's hint bar for feedback instead of the tab's
        status bar. ``image_ids`` is already filtered to CLOUD_UPLOADED items
        by the gallery widget.
        """
        eligible: list[int] = []
        seen: set[int] = set()
        for raw in image_ids or []:
            try:
                parsed = int(raw)
            except (TypeError, ValueError):
                continue
            if parsed <= 0 or parsed in seen:
                continue
            seen.add(parsed)
            eligible.append(parsed)
        if not eligible:
            return

        if len(eligible) == 1:
            prompt = self.tr(
                "Delete the cloud copy of this image?\n\n"
                "The local file stays on this computer. The cloud copy "
                "will be removed from Cloud on your next sync."
            )
        else:
            prompt = self.tr(
                "Delete the cloud copies of {count} images?\n\n"
                "The local files stay on this computer. The cloud copies "
                "will be removed from Cloud on your next sync."
            ).format(count=len(eligible))
        confirmed = self._question_yes_no(
            self.tr("Delete cloud copies"),
            prompt,
            default_yes=False,
        )
        if not confirmed:
            return

        try:
            from utils.cloud_sync import set_image_cloud_selected
            transitions = [
                set_image_cloud_selected(image_id, False)
                for image_id in eligible
            ]
        except Exception:
            self._set_hint(self.tr("Failed to queue cloud image deletion."), tone="error")
            return

        queued_ids = [
            int(transition["image_id"])
            for transition in transitions
            if transition and transition.get("action") == "delete_queued"
        ]
        if not queued_ids:
            self._set_hint(self.tr("No cloud copies to delete."), tone="info")
            return

        try:
            gallery.mark_cloud_delete_pending(queued_ids)
        except Exception:
            pass

        # Queued only — no automatic sync from a thumbnail action. The
        # DELETE_PENDING badge advertises the state; the user's normal or
        # manual cloud sync will push it to Sporely Cloud.
        if len(queued_ids) == 1:
            message = self.tr(
                "Cloud copy marked for deletion. Sync to apply the change to Cloud."
            )
        else:
            message = self.tr(
                "{count} cloud copies marked for deletion. Sync to apply the change to Cloud."
            ).format(count=len(queued_ids))
        self._set_hint(message, tone="info")

    def _on_gallery_delete_requested(self, image_key) -> None:
        if isinstance(image_key, (list, tuple, set)):
            self._on_gallery_delete_selection_requested(list(image_key))
            return
        if image_key is None:
            return
        index = None
        for idx, item in enumerate(self.image_results):
            if image_key == item.image_id or image_key == item.filepath:
                index = idx
                break
        if index is None:
            return

        result = self.image_results[index]
        if result.image_id:
            measurements = MeasurementDB.get_measurements_for_image(result.image_id)
            if measurements:
                confirmed = ask_measurements_exist_delete(self, count=1)
            else:
                confirmed = self._question_yes_no(
                    self.tr("Confirm Delete"),
                    self.tr("Delete image?"),
                    default_yes=False
                )
        else:
            confirmed = self._question_yes_no(
                self.tr("Confirm Delete"),
                self.tr("Remove image from this observation?"),
                default_yes=False
            )
        if not confirmed:
            return

        self.image_results.pop(index)
        self._remap_ai_indices([index])

        if self.primary_index is not None:
            if self.primary_index == index:
                self.primary_index = None
            elif self.primary_index > index:
                self.primary_index -= 1

        if self._ai_selected_index is not None:
            if self._ai_selected_index == index:
                self._ai_selected_index = None
            elif self._ai_selected_index > index:
                self._ai_selected_index -= 1

        self._gps_source_index = self._resolve_gps_source_index()
        self._refresh_image_gallery_summary()
        self._select_initial_ai_image()
        self._update_ai_controls_state()
        self._update_ai_table()

    def _on_gallery_delete_selection_requested(self, image_keys) -> None:
        if not image_keys:
            return

        selected_indices: list[int] = []
        seen_indices: set[int] = set()
        measured_count = 0
        for key in image_keys or []:
            index = None
            for idx, item in enumerate(self.image_results):
                if key == item.image_id or key == item.filepath:
                    index = idx
                    break
            if index is None or index in seen_indices:
                continue
            seen_indices.add(index)
            selected_indices.append(index)
            result = self.image_results[index]
            if result.image_id and MeasurementDB.get_measurements_for_image(result.image_id):
                measured_count += 1

        if not selected_indices:
            return

        if measured_count:
            confirmed = ask_measurements_exist_delete(self, count=measured_count)
        else:
            confirmed = self._question_yes_no(
                self.tr("Confirm Delete"),
                self.tr("Delete {count} selected image(s)?").format(count=len(selected_indices)),
                default_yes=False,
            )
        if not confirmed:
            return

        selected_paths = set(self.image_gallery.selected_paths()) if hasattr(self, "image_gallery") else set()
        removed_indices = sorted(selected_indices)
        for index in sorted(selected_indices, reverse=True):
            if 0 <= index < len(self.image_results):
                self.image_results.pop(index)

        self._remap_ai_indices(removed_indices)

        def _shift_index(old_index: int | None) -> int | None:
            if old_index is None:
                return None
            if old_index in removed_indices:
                return None
            return old_index - sum(1 for removed in removed_indices if removed < old_index)

        self.primary_index = _shift_index(self.primary_index)
        self._ai_selected_index = _shift_index(self._ai_selected_index)
        self._gps_source_index = self._resolve_gps_source_index()
        self._refresh_image_gallery_summary()

        if selected_paths:
            remaining_paths = [
                item.filepath
                for item in self.image_results
                if item.filepath in selected_paths
            ]
            if remaining_paths:
                self.image_gallery.select_paths(remaining_paths)

        self._select_initial_ai_image()
        self._update_ai_controls_state()
        self._update_ai_table()

    def _matches_observation_datetime(self, dt: QDateTime | None) -> bool:
        if not dt or not self._observation_datetime:
            return False
        if not dt.isValid() or not self._observation_datetime.isValid():
            return False
        obs_minutes = int(self._observation_datetime.toSecsSinceEpoch() / 60)
        img_minutes = int(dt.toSecsSinceEpoch() / 60)
        return obs_minutes == img_minutes

    def _make_note_input(self) -> QTextEdit:
        note_input = QTextEdit()
        note_input.setMaximumHeight(60)
        note_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        note_input.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)
        note_input.setStyleSheet("QTextEdit { border: 1px solid #bdc3c7; border-radius: 3px; }")
        return note_input

    def _grows_on_tab_title(self) -> str:
        ui_lang = str(SettingsDB.get_setting("ui_language", "en") or "en").lower()
        return self.tr("Livsmedium") if ui_lang.startswith("no") else self.tr("Grows on")

    def _vernacular_label(self) -> str:
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        if lang == "no":
            return self.tr("Namn:")
        if lang in {"en", "de"}:
            return self.tr("Name:")
        return self.tr("Name:")

    def _vernacular_placeholder(self) -> str:
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        if lang == "no":
            return self.tr("e.g., Kantarell")
        if lang == "de":
            return self.tr("e.g., Pfifferling")
        if lang == "fr":
            return self.tr("e.g., Girolle")
        if lang == "es":
            return self.tr("e.g., Rebozuelo")
        if lang == "da":
            return self.tr("e.g., Kantarel")
        if lang == "sv":
            return self.tr("e.g., Kantarell")
        if lang == "fi":
            return self.tr("e.g., Kantarelli")
        if lang == "pl":
            return self.tr("e.g., Kurka")
        if lang == "pt":
            return self.tr("e.g., Cantarelo")
        if lang == "it":
            return self.tr("e.g., Gallinaccio")
        return self.tr("e.g., Chanterelle")

    def _ensure_taxon_lookup(self) -> TaxonLookupService:
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        vernacular_db = getattr(self, "vernacular_db", None)
        lookup = getattr(self, "_taxon_lookup", None)
        if lookup is not None and getattr(lookup, "vernacular_db", None) is vernacular_db:
            try:
                lookup.language_code = lang
            except Exception:
                pass
            if vernacular_db is not None:
                try:
                    vernacular_db.language_code = lang
                except Exception:
                    pass
            return lookup
        lookup = TaxonLookupService(
            vernacular_db=vernacular_db,
            language_code=lang,
            include_reference_data=True,
        )
        self._taxon_lookup = lookup
        return lookup

    def apply_vernacular_language_change(self) -> None:
        if hasattr(self, "vernacular_label"):
            self.vernacular_label.setText(self._vernacular_label())
        if hasattr(self, "vernacular_input"):
            self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        if hasattr(self, "host_vernacular_label"):
            self.host_vernacular_label.setText(self._vernacular_label())
        if hasattr(self, "host_vernacular_input"):
            self.host_vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self._populate_vernacular_language_menu()
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            current_db = getattr(self, "vernacular_db", None)
            if current_db and current_db.db_path == db_path:
                current_db.language_code = lang
            else:
                self.vernacular_db = VernacularDB(db_path, language_code=lang)
        else:
            self.vernacular_db = None
        lookup = self._ensure_taxon_lookup()
        if hasattr(self, "taxon_picker"):
            self.taxon_picker.set_lookup(lookup)
            # The card's common name follows the new language. Search text
            # still being typed is kept and searched again in that language.
            unfinished = self.taxon_picker.unfinished_text()
            self._sync_taxon_picker(force=True)
            if unfinished and not self.taxon_picker.unfinished_text():
                self.taxon_picker.search_for(unfinished)
        if not db_path:
            return
        self._refresh_vernacular_for_current_taxon()
        self._refresh_host_vernacular_for_current_taxon()
        self._update_taxonomy_tab_indicators()

    def _populate_vernacular_language_menu(self) -> None:
        button = getattr(self, "vernacular_language_btn", None)
        menu = getattr(self, "vernacular_language_menu", None)
        if button is None or menu is None:
            return
        menu.clear()
        stored = SettingsDB.get_setting("vernacular_language", "no")
        current = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        language_group = QActionGroup(menu)
        language_group.setExclusive(True)
        for code in list_available_vernacular_languages():
            norm_code = normalize_vernacular_language(code)
            label = vernacular_language_label(norm_code) or norm_code
            action = QAction(self.tr(label), menu)
            action.setData(norm_code)
            action.setCheckable(True)
            action.setChecked(norm_code == current)
            action.triggered.connect(
                lambda checked=False, selected_code=norm_code: self._on_vernacular_language_selected(
                    selected_code,
                    checked,
                )
            )
            language_group.addAction(action)
            menu.addAction(action)
        current_label = vernacular_language_label(current) or current
        button.setToolTip(
            self.tr("Common-name language: {lang}").format(lang=self.tr(current_label))
        )

    def _on_vernacular_language_selected(self, code: str, checked: bool = True) -> None:
        if not checked:
            return
        selected = normalize_vernacular_language(code)
        stored = SettingsDB.get_setting("vernacular_language", "no")
        current = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        if not selected or selected == current:
            return
        SettingsDB.set_setting("vernacular_language", selected)
        update_app_settings({"vernacular_language": selected})
        for widget in QApplication.topLevelWidgets():
            if hasattr(widget, "apply_vernacular_language_change"):
                try:
                    widget.apply_vernacular_language_change()
                except Exception:
                    pass

    def _setup_vernacular_autocomplete(self):
        """Wire vernacular lookup/completion if taxonomy DB is available."""
        if not hasattr(self, "vernacular_input"):
            return
        stored = SettingsDB.get_setting("vernacular_language", "no")
        lang = resolve_available_vernacular_language(stored) or normalize_vernacular_language(stored)
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            self.vernacular_db = VernacularDB(db_path, language_code=lang)
        else:
            self.vernacular_db = None
        self._ensure_taxon_lookup()
        self._taxon_controller = TaxonInputController(
            self._taxon_lookup,
            self.genus_input,
            self.species_input,
            self.vernacular_input,
            self,
            scientific_name_input=getattr(self, "scientific_name_input", None),
            on_snapshot_invalidated=self._on_scientific_snapshot_invalidated,
            on_snapshot_committed=self._on_scientific_snapshot_committed,
            # Species picks bind the picked concept, like Scientific picks.
            concept_species_suggestions=True,
        )
        if hasattr(self, "taxon_picker"):
            self.taxon_picker.set_lookup(self._taxon_lookup)
        self._genus_model = self._taxon_controller.genus_model
        self._species_model = self._taxon_controller.species_model
        self._vernacular_model = self._taxon_controller.vernacular_model
        self._genus_completer = self._taxon_controller.genus_completer
        self._species_completer = self._taxon_controller.species_completer
        self._vernacular_completer = self._taxon_controller.vernacular_completer

        vernacular_popup = self._vernacular_completer.popup() if self._vernacular_completer else None
        if vernacular_popup:
            self._style_dropdown_popup_readability(vernacular_popup, self.vernacular_input)
        genus_popup = self._genus_completer.popup() if self._genus_completer else None
        if genus_popup:
            self._style_dropdown_popup_readability(genus_popup, self.genus_input)
        species_popup = self._species_completer.popup() if self._species_completer else None
        if species_popup:
            self._style_dropdown_popup_readability(species_popup, self.species_input)

        # Stage 3B.3 red-list invariant: manual taxonomic-identity edits
        # (genus, species, or scientific-name) must clear the red-list
        # snapshot even when NO taxonomy suggestion had been committed
        # (Artsorakel-populated status attaches without a committed
        # scientific snapshot). Vernacular text is deliberately excluded.
        for widget in (self.genus_input, self.species_input,
                       getattr(self, "scientific_name_input", None)):
            if widget is not None:
                widget.textChanged.connect(self._on_taxon_identity_field_edited)

        # Stage 3B.5 follow-up: manual genus/species entry that
        # unambiguously resolves to a single canonical concept should
        # commit the taxonomy snapshot immediately (so the Red List
        # badge refreshes without waiting for Save + Reopen). The
        # controller's editingFinished handlers do their own
        # "resolve-to-accepted" pass; wire an extra hook AFTER them so
        # the manual resolution runs on the already-normalised pair.
        for widget in (self.genus_input, self.species_input):
            if widget is not None:
                widget.editingFinished.connect(
                    self._on_taxon_manual_editing_finished
                )
        # The identity offer follows the entered name: hidden while it is
        # edited, re-evaluated once editing finishes.
        for widget in (self.genus_input, self.species_input, getattr(self, "scientific_name_input", None)):
            if widget is not None:
                widget.textEdited.connect(lambda _text: self._hide_identity_offer())
                widget.editingFinished.connect(self._refresh_identity_offer)

        # Vernacular -> genus autofill: when the observer finishes
        # typing a common name that resolves unambiguously to a single
        # genus (across all matching taxa), auto-populate the Genus
        # field. Species stays empty and Red List stays untouched — a
        # bare vernacular can rarely be pinned to one species-level
        # concept and MUST NOT clear an already-shown badge (Stage 3B.5
        # vernacular-only invariant). The controller's own
        # ``on_vernacular_editing_finished`` handler already covers the
        # exact species-level unique-match case; this hook fires for
        # the genus-only case it deliberately skips.
        if self.vernacular_input is not None:
            self.vernacular_input.editingFinished.connect(
                self._on_vernacular_editing_finished_populate_genus
            )

    def _on_taxon_manual_editing_finished(self) -> None:
        """Resolve typed genus/species text for DISPLAY only.

        Taxonomy-v2 closeout Stage 2 changed what this handler is allowed to
        do. It previously called ``commit_manual_resolution``, which asserted
        a ``sporely_taxon_id`` — so typed text created identity implicitly,
        and because an edit invalidates the committed snapshot first, the very
        next ``editingFinished`` re-bound identity from name text. Both are
        forbidden: identity binding must stay explicit, identity must not be
        inferred from genus/species equality alone, and a committed selection
        must not be rebound from text after the observer edits it.

        What survives is the *reason* the hook exists: refreshing the Red List
        badge without making the observer open the completer. The resolved id
        is recorded through
        :meth:`TaxonInputController.set_display_only_name_match`, which the
        save path never reads, so it can no longer reach
        ``observations.sporely_taxon_id`` or
        ``set_observation_selected_taxon_v2``.

        Only an unambiguous single-candidate resolution is used, and only when
        no real selection is held — a prior explicit picker choice (e.g. a
        ``synonym_of_accepted`` selection) is never shadowed.
        """
        controller = getattr(self, "_taxon_controller", None)
        if controller is None or controller._is_suspended():
            return
        if controller.committed_snapshot() is not None:
            return

        # Every path out of this handler that does NOT establish a fresh match
        # must clear the previous one. A display-only match is derived from the
        # text, so once the text no longer resolves — empty, unknown or
        # ambiguous — the old taxon's evidence is stale and must not remain
        # available to the deferred Red List resolve.
        # The badge itself is already cleared by
        # ``_on_taxon_identity_field_edited`` on the text change that got us
        # here; what must also go is the resolved id behind it, so the
        # deferred resolve cannot re-apply the previous taxon's assessment.
        def _give_up() -> None:
            controller.clear_display_only_name_match()

        genus_widget = getattr(self, "genus_input", None)
        species_widget = getattr(self, "species_input", None)
        if genus_widget is None or species_widget is None:
            _give_up()
            return
        genus = str(genus_widget.text() or "").strip()
        species = str(species_widget.text() or "").strip()
        if not genus or not species:
            _give_up()
            return
        lookup = self._ensure_taxon_lookup()
        if lookup is None:
            _give_up()
            return
        resolver = getattr(lookup, "resolve_manual_scientific", None)
        if not callable(resolver):
            _give_up()
            return
        try:
            resolution = resolver(genus, species)
        except Exception:
            _give_up()
            return
        if resolution is None:
            # Unknown or ambiguous pair — see the note above.
            _give_up()
            return
        # Guard against a stale queued resolve: the widgets must still hold
        # the pair that was resolved. ``commit_manual_resolution`` used to do
        # this check itself.
        if str(genus_widget.text() or "").strip().casefold() != genus.casefold() \
                or str(species_widget.text() or "").strip().casefold() != species.casefold():
            _give_up()
            return
        # DISPLAY ONLY — deliberately not ``commit_manual_resolution``. See the
        # docstring: typed text may refresh the badge, never bind identity.
        if not controller.set_display_only_name_match(resolution.sporely_taxon_id):
            _give_up()
            return
        self._schedule_final_redlist_resolution()

    def _on_vernacular_editing_finished_populate_genus(self) -> None:
        """Auto-populate Genus when a typed vernacular resolves to a
        single genus across all matching taxa.

        Vernacular-only edit invariant (Stage 3B.5): this handler MUST
        NOT clear the Red List badge, MUST NOT overwrite user-typed
        text in Genus, and MUST NOT bind a taxonomy snapshot. Species
        stays empty — a Norwegian common name almost always maps to a
        genus rather than a species, and the exact species-level case
        is already handled by :class:`TaxonInputController`'s
        ``on_vernacular_editing_finished`` (which also fires on this
        signal and executes before this handler due to connection
        order).

        Skip conditions:
          - No taxon lookup available.
          - Vernacular field is empty.
          - Genus field already has text (do not overwrite manual input).
          - A taxonomy snapshot is committed (do not override an
            explicit picker/manual identity).
          - The controller is suspended (load-time / programmatic write).
          - The vernacular resolves to zero, or to multiple distinct
            genera (ambiguity — user must disambiguate).
        """
        controller = getattr(self, "_taxon_controller", None)
        if controller is None or controller._is_suspended():
            return
        try:
            if controller.committed_snapshot() is not None:
                return
        except Exception:
            return
        vernacular_widget = getattr(self, "vernacular_input", None)
        genus_widget = getattr(self, "genus_input", None)
        if vernacular_widget is None or genus_widget is None:
            return
        name = str(vernacular_widget.text() or "").strip()
        if not name:
            return
        current_genus = str(genus_widget.text() or "").strip()
        if current_genus:
            # Do not overwrite what the observer has already typed.
            return
        lookup = self._ensure_taxon_lookup()
        if lookup is None:
            return
        try:
            matches = lookup.resolve_common_name(name)
        except Exception:
            return
        if not matches:
            return
        genera = set()
        first_genus = ""
        for choice in matches:
            genus_text = str(getattr(choice, "genus", "") or "").strip()
            if not genus_text:
                continue
            key = genus_text.casefold()
            if not first_genus:
                first_genus = genus_text
            genera.add(key)
            if len(genera) > 1:
                # Ambiguous across genera — do nothing.
                return
        if not first_genus or len(genera) != 1:
            return
        # Populate genus without triggering the identity-clear handler
        # (Stage 3B.5 vernacular-only invariant) or the controller's
        # sync-vernacular-from-taxon logic (which would overwrite the
        # freshly typed vernacular). ``_taxon_controller._suspended()``
        # suppresses both.
        with controller._suspended():
            genus_widget.setText(first_genus)
        # Refresh downstream indicators (tab dots, etc.) without going
        # through the suppressed textChanged path.
        try:
            self._update_taxonomy_tab_indicators()
        except Exception:
            pass

    def _on_taxon_identity_field_edited(self, _text: str) -> None:
        controller = getattr(self, "_taxon_controller", None)
        if controller is not None and controller._is_suspended():
            return
        # Skip if red-list is already empty — avoids badge repaint churn.
        if not getattr(self, "_red_list_category", ""):
            if not getattr(self, "_red_list_categories", None):
                return
        self._clear_red_list_for_identity_change()

    def _identity_offer_ai_entries(self) -> list[dict]:
        """Artsorakel predictions in this dialog, as identity-offer evidence."""
        entries: list[dict] = []
        for predictions in (getattr(self, "_ai_predictions_by_index", None) or {}).values():
            for pred in predictions or []:
                if not isinstance(pred, dict):
                    continue
                taxon = _normalize_ai_prediction_taxon(pred, source="arts")
                genus, species = self._extract_genus_species_from_taxon(taxon)
                entries.append({
                    "source": "arts",
                    "genus": genus,
                    "species": species,
                    "scientific_name": " ".join(part for part in (genus, species) if part),
                    "external_id": ai_prediction_external_id(pred, taxon),
                })
        return entries

    def _hide_identity_offer(self) -> None:
        self._identity_offer = None
        for widget in (getattr(self, "identity_offer_row", None), getattr(self, "identity_offer_button", None)):
            if widget is not None:
                widget.setVisible(False)

    def _identity_evidence_entries(self) -> list[dict]:
        """Provider identifiers on this observation: its stored AI selection
        and the Artsorakel predictions in the dialog."""
        entries = list(self._identity_offer_ai_entries())
        selected = self._current_ai_selected_fields or {}
        if not selected and isinstance(self.observation or self.draft_data, dict):
            selected = self._ai_selected_fields(self.observation or self.draft_data)
        selected_id = str(selected.get("ai_selected_taxon_id") or "").strip()
        if selected_id:
            entries.append({
                "scientific_name": selected.get("ai_selected_scientific_name") or "",
                "external_id": selected_id,
            })
        return entries

    def _refresh_identity_offer(self) -> None:
        """Use the observation's provider identifiers in the background.

        They settle which taxon a name means when several database taxa
        share it (the Species and Scientific suggestions then list one), and
        they let the editor offer a one-click link for an entered name that
        has no database taxon yet. Only identifiers count; an AI's
        probability does not, and nothing is bound until the user picks a
        row or presses Link (see
        :func:`database.taxon_suggestions.observation_identity_offer`).
        """
        row = getattr(self, "identity_offer_row", None)
        controller = getattr(self, "_taxon_controller", None)
        if row is None or controller is None or not hasattr(self, "genus_input"):
            return
        self._hide_identity_offer()
        lookup = self._ensure_taxon_lookup()
        entries = self._identity_evidence_entries()
        try:
            evidence_ids = evidence_concept_ids(lookup, entries)
        except Exception:
            evidence_ids = ()
        controller.set_evidence_taxon_ids(evidence_ids)
        picker = getattr(self, "taxon_picker", None)
        if picker is not None:
            picker.set_evidence_taxon_ids(evidence_ids)
        if self.is_unidentified():
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        # The Scientific field may hold a different name typed there (an
        # infraspecific name, say); the offer then does not apply to what
        # the user sees.
        scientific_input = getattr(self, "scientific_name_input", None)
        scientific = scientific_input.text().strip() if scientific_input is not None else ""
        if scientific and " ".join(scientific.split()).casefold() != f"{genus} {species}".casefold():
            return
        record = dict(self.observation or self.draft_data or {})
        record.update(controller.committed_identity().to_row())
        record.update(self._current_ai_selected_fields or {})
        try:
            offer = observation_identity_offer(
                lookup,
                record,
                genus=genus,
                species=species,
                ai_entries=self._identity_offer_ai_entries(),
            )
        except Exception:
            offer = None
        if offer is None:
            return
        name = offer.concept_name or offer.scientific_name
        self.identity_offer_label.setText(
            self.tr("{name} is in the species database.").format(name=name)
        )
        identifier = offer.raw_external_id or self.tr("the stored identifier")
        tooltip = self.tr(
            "Matched by {identifier}. Link binds the observation to this database "
            "taxon; the name you entered is kept."
        ).format(identifier=identifier)
        row.setToolTip(tooltip)
        self.identity_offer_button.setToolTip(tooltip)
        self._identity_offer = offer
        row.setVisible(True)
        self.identity_offer_button.setVisible(True)

    def _on_identity_offer_accepted(self) -> None:
        offer = getattr(self, "_identity_offer", None)
        controller = getattr(self, "_taxon_controller", None)
        if offer is None or controller is None:
            return
        # The name may have changed since the offer was shown (programmatic
        # fills do not hide it). Bind only what still applies to the name
        # in the fields now.
        shown_id = offer.sporely_taxon_id
        self._refresh_identity_offer()
        offer = getattr(self, "_identity_offer", None)
        if offer is None or offer.sporely_taxon_id != shown_id:
            return
        # The offer matched the entered name ignoring case, against either
        # the suggested name or the concept's own name. Bind and show that
        # name as spelled in the taxonomy, so the fields, the stored name and
        # the identity agree.
        entered = " ".join(f"{self.genus_input.text()} {self.species_input.text()}".split()).casefold()
        name = next(
            (n for n in (offer.scientific_name, offer.concept_name) if n and n.casefold() == entered),
            " ".join(f"{self.genus_input.text()} {self.species_input.text()}".split()),
        )
        genus, _, species = name.partition(" ")
        if offer.kind == KIND_NAME_CONFIRMED:
            # The identifier reaches this concept only by the release's
            # automatic name match: Link is the user's pick of the concept by
            # name, bound exactly as picking it in the search.
            found = self._taxon_picker_row_for([offer.concept_name or name], int(offer.sporely_taxon_id), None)
            if found is not None:
                self._on_taxon_picker_concept(*found)
                self._sync_taxon_picker(force=True)
            self._hide_identity_offer()
            return
        if offer.origin != ORIGIN_OBSERVATION and offer.raw_external_id:
            identity = TaxonIdentity.from_prefixed_external_id(
                offer.raw_external_id,
                scientific_name=name or None,
                rank="species" if species else "genus",
            )
        else:
            # The observation's own preserved provider tuple, as stored: its
            # raw value may be a bare id that does not parse on its own.
            identity = dataclass_replace(controller.committed_identity(), scientific_name=name or None)
        # The same resolver and commit path as copying an AI suggestion: the
        # identifier is resolved again and bound with its provider evidence.
        if controller.commit_provider_identity(identity, genus=genus, species=species):
            with controller._suspended():
                for widget, value in (
                    (self.genus_input, genus),
                    (self.species_input, species),
                    (getattr(self, "scientific_name_input", None), name),
                ):
                    if widget is not None and widget.text() != value:
                        widget.setText(value)
        self._hide_identity_offer()

    # ------------------------------------------------------------------
    # Unified taxon picker
    # ------------------------------------------------------------------

    def _on_taxon_picker_concept(self, _row, concept) -> None:
        """A concept the user picked: the same commit as a Scientific pick."""
        controller = getattr(self, "_taxon_controller", None)
        if controller is None or self.is_unidentified():
            self._schedule_taxon_picker_sync()
            return
        before = self._committed_sporely_taxon_id()
        if self._pick_confirms_stored_identity(before, concept):
            # The species the observation already is: its source concept,
            # proof and provenance stay as stored.
            self._sync_taxon_picker(force=True)
            self._update_taxonomy_tab_indicators()
            return
        if not controller.commit_picked_concept(concept.as_scientific_suggestion()):
            # Not committed (a name the snapshot parser refuses): show what
            # the observation still holds.
            self._sync_taxon_picker(force=True)
        elif self._committed_sporely_taxon_id() != before:
            self._set_inaturalist_taxon_id(None)
        self._update_taxonomy_tab_indicators()

    def _pick_confirms_stored_identity(self, before: int | None, concept) -> bool:
        """Whether a pick is the identification the observation already has,
        under the name it stores: the same proven concept (its provider id and
        provenance are kept), or the entry a member-bound observation is shown
        in, the concept a release name association presents ``before`` as.
        Another name of that species is a new identification and binds as
        usual."""
        picked = getattr(concept, "sporely_taxon_id", None)
        if before is None or picked is None:
            return False
        controller = getattr(self, "_taxon_controller", None)
        snapshot = (controller.committed_snapshot() if controller else None) or {}
        stored = str(snapshot.get("scientific_name") or "")
        if " ".join(stored.split()).casefold() != " ".join(str(concept.scientific_name or "").split()).casefold():
            return False
        if int(picked) == int(before):
            return TaxonIdentity.from_row(snapshot).is_proven_sporely
        try:
            primary = self._ensure_taxon_lookup().association_primary_id(before)
        except Exception:
            return False
        return primary is not None and int(primary) == int(picked)

    def _on_taxon_picker_free_name(self, name: str) -> None:
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return
        controller.set_unlinked_name(name)
        self._set_inaturalist_taxon_id(None)
        self._update_taxonomy_tab_indicators()
        self._refresh_identity_offer()

    def _on_taxon_picker_cleared(self) -> None:
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return
        controller.clear_identification()
        self._set_inaturalist_taxon_id(None)
        # The AI determination that was chosen goes with the identification,
        # or loading would refill the empty name from it. The predictions
        # themselves stay.
        self._current_ai_selected_fields = self._ai_selected_fields(None)
        self._ai_selected_by_index.clear()
        self._inat_selected_by_index.clear()
        self._update_selected_ai_summary_label(None)
        self._update_taxonomy_tab_indicators()
        self._refresh_identity_offer()

    def _committed_sporely_taxon_id(self) -> int | None:
        """The concept the observation is bound to now, or None.

        The stored iNaturalist taxon ID is the publishing ID for that
        concept: a pick of another concept, a name without a taxon and ✕
        drop it (publishing then resolves the new name as for an
        observation that never had one). Picking the same concept again
        keeps it."""
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return None
        identity = controller.committed_identity()
        if identity.is_proven_sporely and identity.sporely_taxon_id is not None:
            return int(identity.sporely_taxon_id)
        return None

    def _schedule_taxon_picker_sync(self, *_args) -> None:
        timer = getattr(self, "_taxon_picker_sync_timer", None)
        if timer is not None:
            timer.start(0)

    def _taxon_picker_row_for(self, names, taxon_id: int, link_kind: str | None):
        """The search row and concept that show a stored concept: a row with
        the stored name if there is one, else any row holding the concept."""
        lookup = self._ensure_taxon_lookup()
        searcher = getattr(lookup, "search_taxa", None) if lookup is not None else None
        if not callable(searcher):
            return None
        # A concept a release name association presents as another one has
        # no row of its own: it shows in its primary's row, still carrying
        # the stored id, so nothing changes unless the user picks again.
        try:
            shown_as = lookup.current_taxon_id(taxon_id) or taxon_id
        except Exception:
            shown_as = taxon_id
        fallback = None
        for name in names:
            if not name:
                continue
            try:
                page = searcher(name, limit=40, current_taxon_id=taxon_id)
            except Exception:
                continue
            exact = []
            for row in page.rows:
                concept = next((c for c in row.concepts if c.sporely_taxon_id in (taxon_id, shown_as)), None)
                if concept is None:
                    continue
                if concept.sporely_taxon_id != taxon_id:
                    concept = dataclass_replace(concept, sporely_taxon_id=taxon_id)
                if row.name.casefold() == name.casefold():
                    exact.append((row, concept))
                elif fallback is None:
                    fallback = (row, concept)
            if exact:
                return next((pair for pair in exact if pair[1].link_kind == link_kind), exact[0])
        return fallback

    def _sync_taxon_picker(self, force: bool = False) -> None:
        """Show what the backing fields and the committed identity hold.

        Display only: it reads the identification and never commits one, so
        loading, the Artsorakel copy and Link appear in the picker without
        any pick being made. A state the picker already shows is left alone.
        """
        picker = getattr(self, "taxon_picker", None)
        controller = getattr(self, "_taxon_controller", None)
        if picker is None or controller is None:
            return
        unidentified = self.is_unidentified()
        picker.setEnabled(not unidentified)
        picker.set_uncertain(self.uncertain_checkbox.isChecked())
        snapshot = controller.committed_snapshot() or {}
        identity = controller.committed_identity()
        proven_id = (
            int(identity.sporely_taxon_id)
            if identity.is_proven_sporely and identity.sporely_taxon_id is not None
            else None
        )
        # The committed concept, after a source switch too, is its row's
        # default: Endre + Enter on the unchanged name keeps it.
        picker.set_current_taxon_id(proven_id)
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        typed_name = " ".join(part for part in (genus, species) if part)
        stored_name = str(snapshot.get("scientific_name") or "").strip()
        if identity.is_proven_sporely and identity.sporely_taxon_id is not None:
            taxon_id = int(identity.sporely_taxon_id)
            current = picker.selection()
            if not force and current is not None and current[1].sporely_taxon_id == taxon_id \
                    and (not stored_name or current[1].scientific_name == stored_name):
                return
            try:
                display = self._ensure_taxon_lookup().display_scientific_name(taxon_id)
            except Exception:
                display = None
            found = self._taxon_picker_row_for(
                (stored_name, snapshot.get("canonical_scientific_name"), display, typed_name),
                taxon_id,
                snapshot.get("link_kind"),
            )
            if found is not None:
                picker.show_selection(*found)
            else:
                picker.show_missing_concept(stored_name or display or typed_name or str(taxon_id))
            return
        name = stored_name or typed_name or self.vernacular_input.text().strip()
        if not name:
            if force or picker.selection() is not None or picker.unlinked() is not None:
                picker.clear_display()
            return
        # A name kept without a taxon is stored whole in Species; anything
        # else unlinked is an older or provider-supplied name to review.
        free = not genus and bool(species) and not snapshot
        wanted = (" ".join(name.split()), "free" if free else "legacy")
        if not force and picker.unlinked() == wanted:
            return
        picker.show_unlinked_name(name, legacy=not free)

    def _confirm_unfinished_taxon_search(self) -> bool:
        """Typed species search text that was never picked needs a decision
        before saving: keep it as a name, drop it, or go back. Returns
        whether saving may go on."""
        picker = getattr(self, "taxon_picker", None)
        text = picker.unfinished_text() if picker is not None and picker.isEnabled() else ""
        if not text:
            if picker is not None:
                # Endre without typing: the card comes back as it was.
                picker.discard_unfinished()
            return True
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle(self.tr("Unfinished species search"))
        box.setText(self.tr("“{text}” is typed in the species search, but no entry is chosen.").format(text=text))
        box.setInformativeText(self.tr(
            "Choose an entry from the results, keep the text as a name without a taxon, "
            "or discard it and keep the identification as it was."
        ))
        keep = box.addButton(self.tr("Keep as name without taxon"), QMessageBox.AcceptRole)
        discard = box.addButton(self.tr("Discard search text"), QMessageBox.DestructiveRole)
        back = box.addButton(self.tr("Choose an entry"), QMessageBox.RejectRole)
        box.setDefaultButton(back)
        box.setEscapeButton(back)
        box.exec()
        clicked = box.clickedButton()
        if clicked is keep:
            return picker.keep_unfinished_as_name()
        if clicked is discard:
            picker.discard_unfinished()
            return True
        if hasattr(self, "taxonomy_tabs"):
            self.taxonomy_tabs.setCurrentIndex(self.taxonomy_tabs.indexOf(self.species_tab))
        picker.search_input.setFocus(Qt.OtherFocusReason)
        return False

    def _on_scientific_snapshot_committed(self, snapshot: dict) -> None:
        self._schedule_taxon_picker_sync()
        self._hide_identity_offer()
        # Stage 3B.3 red-list invariant: an explicit new taxonomy
        # selection clears the previous red-list snapshot BEFORE any
        # subsequent Artsorakel resolve populates a new one. Never carry
        # a status from the old identity into the new.
        self._clear_red_list_for_identity_change()
        # Stage 3B.5: an explicit taxonomy pick (without an Artsorakel
        # prior) should still resolve the local red-list lookup.
        self._schedule_final_redlist_resolution()
        label = getattr(self, "scientific_hint_label", None)
        if label is None:
            return
        link_kind = (snapshot or {}).get("link_kind") or ""
        canonical = (snapshot or {}).get("canonical_scientific_name") or ""
        picked = str((snapshot or {}).get("scientific_name") or "").strip()
        # Name the concept as it is displayed in the UI language: its
        # national preferred scientific name where one exists (taxonomy-v3
        # Stage 3P), else the canonical name. Display only.
        concept = canonical
        try:
            display = self._ensure_taxon_lookup().display_scientific_name(
                (snapshot or {}).get("sporely_taxon_id"))
        except Exception:
            display = None
        if display:
            concept = display
        if link_kind in ("synonym_of_accepted", "canonical") and concept and concept != picked:
            label.setText(self.tr("Accepted concept: {name}").format(name=concept))
            label.setVisible(True)
        elif link_kind == "linked" and concept and concept != picked:
            label.setText(self.tr("Linked concept: {name}").format(name=concept))
            label.setVisible(True)
        else:
            label.setText("")
            label.setVisible(False)

    def _on_scientific_snapshot_invalidated(self, reason: str | None = None) -> None:
        self._schedule_taxon_picker_sync()
        # Stage 3B.3 red-list invariant: any manual genus/species/
        # scientific-name edit that invalidates identity also clears the
        # red-list snapshot. Category, categories map, and label all go
        # to their empty state.
        self._clear_red_list_for_identity_change()
        label = getattr(self, "scientific_hint_label", None)
        if label is not None:
            label.setText("")
            label.setVisible(False)

    def _clear_red_list_for_identity_change(self) -> None:
        """Reset every red-list snapshot field to its empty state.

        Called on any event that would leave the observation's stored
        red-list category inconsistent with the taxonomic identity
        currently entered:

        - taxonomy suggestion committed (new identity, old status stale);
        - snapshot invalidation via manual genus/species/scientific edit;
        - scientific-name field wiped while a snapshot was pending;
        - Artsorakel-populated status when the source identity changed.

        Common-name (vernacular) edits MUST NOT reach this method.
        """
        setter = getattr(self, "_set_red_list_category", None)
        if setter is not None:
            setter(None, None)
        # Stage 3B.5: also drop the idempotence guard so a return to the
        # same identity later still triggers a fresh apply. Bump the
        # generation nonce so any in-flight deferred resolve tied to the
        # previous identity is invalidated even if the four identity
        # signals happen to coincide with the new one.
        self._last_applied_redlist_key = None
        self._redlist_generation = getattr(self, "_redlist_generation", 0) + 1

    # ------------------------------------------------------------------
    # Stage 3B.5 — deferred post-Artsorakel red-list resolution.
    # ------------------------------------------------------------------

    def _current_identity_token(self) -> tuple:
        """Snapshot of identity signals used to reject stale deferred callbacks."""
        controller = getattr(self, "_taxon_controller", None)
        snap = None
        try:
            snap = controller.committed_snapshot() if controller else None
        except Exception:
            snap = None
        sporely_id = proven_sporely_taxon_id(snap) if snap else None
        try:
            sporely_int = int(sporely_id) if sporely_id else 0
        except (TypeError, ValueError):
            sporely_int = 0
        # Taxonomy-v2 closeout Stage 2: the display-only name match also drives
        # the badge, so it is an identity *signal* for staleness purposes even
        # though it is never persisted identity.
        try:
            display_match = int(controller.display_only_name_match() or 0) if controller else 0
        except Exception:
            display_match = 0
        return (
            id(self),
            sporely_int,
            (snap or {}).get("scientific_name") if snap else None,
            self._location_country_code,
            display_match,
        )

    def _schedule_final_redlist_resolution(self) -> None:
        """Enqueue a single deferred red-list resolve on the next tick.

        The 0-ms QTimer defers execution until the current call stack
        finishes (e.g. an Artsorakel apply that just wrote a source
        snapshot). Only the most recent scheduling wins — an earlier
        deferred callback whose captured ``_redlist_generation`` no
        longer matches, or whose identity token no longer matches, is
        dropped silently.
        """
        self._redlist_generation += 1
        gen = self._redlist_generation
        token = self._current_identity_token()
        self._pending_redlist_token = token
        QTimer.singleShot(
            0,
            lambda g=gen, t=token: self._resolve_and_apply_redlist(g, t),
        )

    @staticmethod
    def _derive_redlist_apply(result, current_categories):
        """Return the pure ``(raw_code, categories, hint)`` triple for a
        :class:`RedlistLookupResult` and the currently held categories map.

        Pure decision logic — no widget access. Extracted so tests can
        exercise every branch without spinning up Qt.

        Rules:
          - ``unique`` / ``multiple_same_category``: expose the raw
            category (with degree mark) and preserve ``current_categories``.
            No hint.
          - ``conflict``: clear derived category; preserve
            ``current_categories`` (Artsorakel snapshot); set a hint.
          - ``none``: clear derived category; preserve
            ``current_categories``; no hint.
        """
        status = getattr(result, "status", None)
        if status in ("unique", "multiple_same_category"):
            assessment = getattr(result, "assessment", None)
            raw = str(getattr(assessment, "category_raw", "") or "").strip()
            return (raw or None, current_categories, None)
        if status == "conflict":
            return (None, current_categories, "Multiple assessments")
        # "none" or anything unexpected
        return (None, current_categories, None)

    def _resolve_and_apply_redlist(
        self,
        expected_gen: int,
        expected_token: tuple,
    ) -> None:
        # Reject stale execution. The generation nonce is the primary
        # guard — every reload / clear / close bumps it, so a deferred
        # callback from a previous observation (or a previous identity
        # in the same dialog) is dropped even if the coincident identity
        # signals happen to match. The identity-token check is a
        # defensive second guard for the case where the counter alone
        # would still allow a stale write (e.g. a signal handler that
        # forgot to bump the generation).
        if getattr(self, "_close_cleanup_done", False):
            return
        if getattr(self, "_redlist_generation", 0) != expected_gen:
            return
        if self._current_identity_token() != expected_token:
            return
        if getattr(self, "_pending_redlist_token", None) != expected_token:
            return
        controller = getattr(self, "_taxon_controller", None)
        try:
            snap = controller.committed_snapshot() if controller else None
        except Exception:
            snap = None
        # Taxonomy-v2 closeout Stage 2: only a PROVEN identity may drive the
        # Red List lookup. A reloaded legacy-unverified row restores its raw
        # integer into the committed snapshot, so reading the column directly
        # let a numeric collision resolve somebody else's assessment. The name
        # still displays; only the identity-bearing lookup is gated.
        sporely_id = proven_sporely_taxon_id(snap) if snap else None
        if not sporely_id and controller is not None:
            # Taxonomy-v2 closeout Stage 2: fall back to the display-only name
            # match. Typed genus/species text can still refresh the badge, but
            # that resolution is not identity and is never persisted — see
            # ``_on_taxon_manual_editing_finished``.
            try:
                sporely_id = controller.display_only_name_match()
            except Exception:
                sporely_id = None
        if not sporely_id:
            return
        area = determine_redlist_area(self._location_country_code)
        if area is None:
            # No reliable assessment area → clear any locally derived
            # category but preserve _red_list_categories (Artsorakel
            # source snapshot).
            if self._red_list_category:
                self._set_red_list_category_raw(
                    None, self._red_list_categories, hint=None
                )
            self._last_applied_redlist_key = None
            return
        lookup = self._ensure_taxon_lookup()
        if lookup is None:
            return
        try:
            # Use the overlay-aware entrypoint so a bound COL identity
            # can still surface its assessed Norwegian counterpart's
            # Red List category. See
            # ``TaxonLookupService.get_redlist_lookup_with_overlay``:
            # identity is not modified — only the surfaced assessment.
            overlay_getter = getattr(
                lookup, "get_redlist_lookup_with_overlay", None,
            )
            if callable(overlay_getter):
                result = overlay_getter(
                    int(sporely_id), area=area, source_release="2021"
                )
            else:
                result = lookup.get_redlist_lookup(
                    int(sporely_id), area=area, source_release="2021"
                )
        except Exception:
            return
        raw_key = (
            int(sporely_id),
            area,
            "2021",
            getattr(result, "status", None),
            getattr(getattr(result, "assessment", None), "category_raw", None),
            getattr(result, "overlay_taxon_id", None),
        )
        if getattr(self, "_last_applied_redlist_key", None) == raw_key:
            return
        self._last_applied_redlist_key = raw_key
        raw, categories, hint = self._derive_redlist_apply(
            result, self._red_list_categories
        )
        hint_text = self.tr("Multiple assessments") if hint == "Multiple assessments" else hint
        self._set_red_list_category_raw(raw, categories, hint=hint_text)

    def _setup_host_autocomplete(self):
        """Autocomplete for Habitat -> Grows on genus/species/vernacular fields."""
        lookup = self._ensure_taxon_lookup()
        if (
            not hasattr(self, "host_genus_input")
            or not hasattr(self, "host_species_input")
            or not hasattr(self, "host_vernacular_input")
        ):
            return
        self._host_taxon_controller = TaxonInputController(
            lookup,
            self.host_genus_input,
            self.host_species_input,
            self.host_vernacular_input,
            self,
        )
        self._host_genus_model = self._host_taxon_controller.genus_model
        self._host_species_model = self._host_taxon_controller.species_model
        self._host_vernacular_model = self._host_taxon_controller.vernacular_model
        self._host_genus_completer = self._host_taxon_controller.genus_completer
        self._host_species_completer = self._host_taxon_controller.species_completer
        self._host_vernacular_completer = self._host_taxon_controller.vernacular_completer

        host_genus_popup = self._host_genus_completer.popup() if self._host_genus_completer else None
        if host_genus_popup:
            self._style_dropdown_popup_readability(host_genus_popup, self.host_genus_input)
        host_species_popup = self._host_species_completer.popup() if self._host_species_completer else None
        if host_species_popup:
            self._style_dropdown_popup_readability(host_species_popup, self.host_species_input)
        vern_popup = self._host_vernacular_completer.popup() if self._host_vernacular_completer else None
        if vern_popup:
            self._style_dropdown_popup_readability(vern_popup, self.host_vernacular_input)

    def _on_host_genus_text_changed(self, text: str) -> None:
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup or self._host_suppress_taxon_autofill:
            return
        value = (text or "").strip()
        suggestions = lookup.suggest_genera(value, limit=TAXON_COMPLETER_LIMIT)
        if len(suggestions) == 1 and suggestions[0].casefold() == value.casefold():
            self._host_genus_model.setStringList([])
            if self._host_genus_completer:
                self._host_genus_completer.popup().hide()
        else:
            self._host_genus_model.setStringList(suggestions[:TAXON_COMPLETER_LIMIT])
        if not value:
            self._host_suppress_taxon_autofill = True
            try:
                self.host_species_input.clear()
                self.host_vernacular_input.clear()
            finally:
                self._host_suppress_taxon_autofill = False
            self._host_species_model.setStringList([])
            if self._host_species_completer:
                self._host_species_completer.setCompletionPrefix("")
                self._host_species_completer.popup().hide()
            self._set_host_species_placeholder_from_suggestions([])
            self._set_vernacular_entries(self._host_vernacular_model, [], host=True)
            if self._host_vernacular_completer:
                self._host_vernacular_completer.setCompletionPrefix("")
                self._host_vernacular_completer.popup().hide()
            self._set_host_vernacular_placeholder_from_suggestions([])
            return
        if self._host_species_completer and not self.host_species_input.hasFocus():
            self._host_species_completer.setCompletionPrefix("")
        if not self.host_species_input.text().strip():
            species_suggestions = lookup.suggest_species(value, "", limit=TAXON_COMPLETER_LIMIT)
            self._set_host_species_placeholder_from_suggestions(
                [choice.species for choice in species_suggestions if choice.species]
            )
        if not self.host_vernacular_input.text().strip():
            self._update_host_vernacular_suggestions_for_taxon()

    def _on_host_genus_selected(self, genus: str) -> None:
        if self._host_genus_completer:
            self._host_genus_completer.popup().hide()
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        if self.host_species_input.text().strip():
            return
        suggestions = lookup.suggest_species(str(genus).strip(), "", limit=TAXON_COMPLETER_LIMIT)
        self._host_species_model.setStringList([choice.species for choice in suggestions if choice.species][:TAXON_COMPLETER_LIMIT])
        self._set_host_species_placeholder_from_suggestions(
            [choice.species for choice in suggestions if choice.species]
        )
        if not self.host_vernacular_input.text().strip():
            self._update_host_vernacular_suggestions_for_taxon()

    def _on_host_genus_editing_finished(self) -> None:
        if self._host_genus_completer and self._host_genus_completer.popup().isVisible():
            return
        if self._host_suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted(host=True)
        self._refresh_host_vernacular_for_current_taxon()

    def _on_host_species_text_changed(self, text: str) -> None:
        if self._host_suppress_taxon_autofill:
            return
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        genus = self.host_genus_input.text().strip()
        if not genus:
            self._host_species_model.setStringList([])
            self._set_vernacular_entries(self._host_vernacular_model, [], host=True)
            if self._host_vernacular_completer:
                self._host_vernacular_completer.popup().hide()
            self._set_host_vernacular_placeholder_from_suggestions([])
            return
        prefix = (text or "").strip()
        suggestions = lookup.suggest_species(genus, prefix, limit=TAXON_COMPLETER_LIMIT)
        species_values = [choice.species for choice in suggestions if choice.species]
        if prefix and any(s.lower() == prefix.lower() for s in species_values):
            self._host_species_model.setStringList([])
            if self._host_species_completer:
                self._host_species_completer.popup().hide()
        else:
            self._host_species_model.setStringList(species_values[:TAXON_COMPLETER_LIMIT])
            if self._host_species_model.stringList() and self._host_species_completer:
                self._host_species_completer.setCompletionPrefix(prefix)
                self._host_species_completer.complete()
        self._update_host_vernacular_suggestions_for_taxon()
        if prefix:
            self._maybe_set_host_vernacular_from_taxon()

    def _on_host_species_selected(self, species: str) -> None:
        if self._host_species_completer:
            self._host_species_completer.popup().hide()
        _ = species
        self._resolve_current_taxon_to_accepted(host=True)
        self._refresh_host_vernacular_for_current_taxon()

    def _on_host_species_editing_finished(self) -> None:
        if self._host_species_completer and self._host_species_completer.popup().isVisible():
            return
        if self._host_suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted(host=True)
        self._refresh_host_vernacular_for_current_taxon()

    def _on_host_vernacular_text_changed(self, text: str) -> None:
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup or not lookup.vernacular_db or not hasattr(self, "_host_vernacular_model"):
            return
        if self._host_suppress_taxon_autofill:
            return
        value = (text or "").strip()
        if not value:
            self._update_host_vernacular_suggestions_for_taxon()
            return
        genus = self.host_genus_input.text().strip() or None
        species = self.host_species_input.text().strip() or None
        entries = lookup.suggest_common_names(prefix=value, genus=genus, species=species, limit=TAXON_COMPLETER_LIMIT)
        text_label = self._format_vernacular_suggestion_label(
            {"vernacular_name": value, "genus": genus, "species": species}
        )
        if any(self._format_vernacular_suggestion_label(entry).casefold() == text_label.casefold() for entry in entries):
            self._set_vernacular_entries(self._host_vernacular_model, [], host=True)
            if self._host_vernacular_completer:
                self._host_vernacular_completer.popup().hide()
        else:
            self._set_vernacular_entries(self._host_vernacular_model, entries, host=True, limit=TAXON_COMPLETER_LIMIT)

    def _on_host_vernacular_selected(self, text: str) -> None:
        self._set_host_taxon_from_vernacular(text)

    def _on_host_vernacular_editing_finished(self) -> None:
        self._set_host_taxon_from_vernacular(self.host_vernacular_input.text())

    def _set_host_taxon_from_vernacular(self, name: str) -> None:
        controller = getattr(self, "_host_taxon_controller", None)
        if controller is None or not hasattr(self, "host_vernacular_input"):
            return
        value = (name or "").strip()
        if value and value != self.host_vernacular_input.text().strip():
            self.host_vernacular_input.setText(value)
        controller.on_vernacular_editing_finished()

    def _maybe_set_host_vernacular_from_taxon(self) -> None:
        controller = getattr(self, "_host_taxon_controller", None)
        if controller is None:
            return
        controller.sync_vernacular_after_taxon_change()

    def _update_host_vernacular_suggestions_for_taxon(self) -> None:
        controller = getattr(self, "_host_taxon_controller", None)
        if controller is None:
            return
        controller.refresh_vernacular_suggestions()

    def _set_host_vernacular_placeholder_from_suggestions(self, suggestions: list[TaxonChoice | dict | str]) -> None:
        if not hasattr(self, "host_vernacular_input"):
            return
        preview_items: list[str] = []
        for suggestion in suggestions:
            if isinstance(suggestion, TaxonChoice):
                label = str(suggestion.common_name or "").strip()
                if not label:
                    label = self._format_vernacular_suggestion_label(suggestion)
            elif isinstance(suggestion, dict):
                label = str(suggestion.get("vernacular_name") or suggestion.get("common_name") or "").strip()
                if not label:
                    label = self._format_vernacular_suggestion_label(suggestion)
            else:
                label = str(suggestion or "").strip()
            if label:
                preview_items.append(label)
        if not preview_items:
            self.host_vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        preview = "; ".join(preview_items[:4])
        self.host_vernacular_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

    def _set_host_species_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "host_species_input"):
            return
        if not suggestions:
            self.host_species_input.setPlaceholderText(self.tr("e.g., pendula"))
            return
        preview = "; ".join(suggestions[:4])
        self.host_species_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

    def _style_dropdown_popup_readability(self, popup, font_source=None):
        """Make popup rows slightly looser while matching the input font."""
        if popup is None:
            return
        if font_source is not None and hasattr(font_source, "font"):
            try:
                popup.setFont(font_source.font())
            except Exception:
                pass
        if hasattr(popup, "setSpacing"):
            try:
                popup.setSpacing(1)
            except Exception:
                pass
        popup.setStyleSheet(
            "QListView::item { padding: 2px 6px; }"
            "QAbstractItemView::item { padding: 2px 6px; }"
        )

    @staticmethod
    def _vernacular_entry_to_choice(entry: TaxonChoice | dict | None) -> TaxonChoice | None:
        if isinstance(entry, TaxonChoice):
            return entry
        if not isinstance(entry, dict):
            return None
        genus = str(entry.get("genus") or "").strip()
        if not genus:
            return None
        species = str(entry.get("species") or "").strip() or None
        common_name = str(entry.get("vernacular_name") or entry.get("common_name") or "").strip() or None
        family = str(entry.get("family") or "").strip() or None
        return TaxonChoice(
            genus=genus,
            species=species,
            common_name=common_name,
            family=family,
        )

    @staticmethod
    def _format_vernacular_suggestion_label(entry: TaxonChoice | dict) -> str:
        if isinstance(entry, TaxonChoice):
            vernacular_name = str(entry.common_name or "").strip()
        else:
            vernacular_name = str(entry.get("vernacular_name") or entry.get("common_name") or "").strip()
        return vernacular_name

    def _set_vernacular_entries(
        self,
        model: QStringListModel | None,
        entries: list[TaxonChoice | dict],
        *,
        host: bool = False,
        limit: int = TAXON_COMPLETER_LIMIT,
    ) -> None:
        if model is None:
            return
        labels: list[str] = []
        entry_map: dict[str, dict] = {}
        row_entries: list[TaxonChoice | dict] = []
        for entry in entries[:limit]:
            label = self._format_vernacular_suggestion_label(entry)
            if not label:
                continue
            labels.append(label)
            entry_map[label.casefold()] = entry
            row_entries.append(entry)
        model.setStringList(labels)
        if hasattr(model, "item"):
            for row, entry in enumerate(row_entries):
                item = model.item(row, 0)
                if item is None:
                    continue
                choice = self._vernacular_entry_to_choice(entry)
                item.setData(labels[row], Qt.UserRole)
                item.setData(choice or entry, self._ROLE_TAXON_CHOICE)
                if isinstance(entry, TaxonChoice):
                    item.setData(str(entry.genus or "").strip(), Qt.UserRole + 1)
                    item.setData(str(entry.species or "").strip(), Qt.UserRole + 2)
                elif isinstance(entry, dict):
                    item.setData(str(entry.get("genus") or "").strip(), Qt.UserRole + 1)
                    item.setData(str(entry.get("species") or "").strip(), Qt.UserRole + 2)
        if host:
            self._host_vernacular_entry_map = entry_map
        else:
            self._vernacular_entry_map = entry_map

    def _lookup_vernacular_entry(self, text: str, *, host: bool = False) -> dict | None:
        mapping = getattr(self, "_host_vernacular_entry_map", {}) if host else getattr(self, "_vernacular_entry_map", {})
        return mapping.get(str(text or "").strip().casefold())

    def _resolve_current_taxon_to_accepted(self, *, host: bool = False) -> bool:
        if host:
            lookup = getattr(self, "_taxon_lookup", None)
            if not lookup:
                return False
            genus_widget = getattr(self, "host_genus_input", None)
            species_widget = getattr(self, "host_species_input", None)
            suppress_attr = "_host_suppress_taxon_autofill"
        else:
            lookup = getattr(self, "_taxon_lookup", None)
            if not lookup:
                return False
            genus_widget = getattr(self, "genus_input", None)
            species_widget = getattr(self, "species_input", None)
            suppress_attr = "_suppress_taxon_autofill"
        if genus_widget is None or species_widget is None:
            return False
        genus = genus_widget.text().strip()
        species = species_widget.text().strip()
        if not genus or not species:
            return False
        if host:
            resolved_choice = lookup.resolve_scientific(genus, species) if lookup else None
            if not resolved_choice:
                return False
            accepted_genus = resolved_choice.genus
            accepted_species = resolved_choice.species or species
        else:
            resolved_choice = lookup.resolve_scientific(genus, species) if lookup else None
            if not resolved_choice:
                return False
            accepted_genus = resolved_choice.genus
            accepted_species = resolved_choice.species or species
        if not accepted_genus or not accepted_species:
            return False
        if accepted_genus.casefold() == genus.casefold() and accepted_species.casefold() == species.casefold():
            return False
        setattr(self, suppress_attr, True)
        try:
            genus_widget.setText(accepted_genus)
            species_widget.setText(accepted_species)
        finally:
            setattr(self, suppress_attr, False)
        return True

    def _on_vernacular_text_changed(self, text):
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        if self._suppress_taxon_autofill:
            return
        if not text.strip():
            self._update_vernacular_suggestions_for_taxon()
            return
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        entries = lookup.suggest_common_names(prefix=text, genus=genus, species=species, limit=TAXON_COMPLETER_LIMIT)
        text_label = self._format_vernacular_suggestion_label(
            TaxonChoice(genus=genus or "", species=species, common_name=text.strip())
        )
        if any(self._format_vernacular_suggestion_label(entry).casefold() == text_label.casefold() for entry in entries):
            self._set_vernacular_entries(self._vernacular_model, [], host=False)
            if self._vernacular_completer:
                self._vernacular_completer.popup().hide()
        else:
            self._set_vernacular_entries(self._vernacular_model, entries, host=False, limit=TAXON_COMPLETER_LIMIT)

    def _update_vernacular_suggestions_for_taxon(self):
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return
        controller.refresh_vernacular_suggestions()

    def _set_vernacular_placeholder_from_suggestions(self, suggestions: list[TaxonChoice | dict | str]) -> None:
        if not hasattr(self, "vernacular_input"):
            return
        preview_items: list[str] = []
        for suggestion in suggestions:
            if isinstance(suggestion, TaxonChoice):
                label = str(suggestion.common_name or "").strip()
                if not label:
                    label = self._format_vernacular_suggestion_label(suggestion)
            elif isinstance(suggestion, dict):
                label = str(suggestion.get("vernacular_name") or suggestion.get("common_name") or "").strip()
                if not label:
                    label = self._format_vernacular_suggestion_label(suggestion)
            else:
                label = str(suggestion or "").strip()
            if label:
                preview_items.append(label)
        if not preview_items:
            self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        preview = "; ".join(preview_items[:4])
        self.vernacular_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

    def _set_species_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "species_input"):
            return
        if not suggestions:
            self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
            return
        preview = "; ".join(suggestions[:4])
        self.species_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

    def _on_vernacular_selected(self, name):
        # Hide the popup after selection
        if self._vernacular_completer:
            self._vernacular_completer.popup().hide()
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        index = name if isinstance(name, QModelIndex) else None
        raw_name = ""
        if index is not None and index.isValid():
            raw_name = str(index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
            choice = index.data(self._ROLE_TAXON_CHOICE)
            if isinstance(choice, TaxonChoice):
                entry = choice
            else:
                entry = self._lookup_vernacular_entry(raw_name, host=False)
        else:
            raw_name = str(name or "").strip()
            entry = self._lookup_vernacular_entry(name, host=False)
        choice = self._vernacular_entry_to_choice(entry)
        if choice is None and index is not None and index.isValid():
            payload = index.data(self._ROLE_TAXON_CHOICE)
            if isinstance(payload, TaxonChoice):
                choice = payload
        if choice is None and raw_name:
            matches = lookup.resolve_common_name(raw_name)
            choice = matches[0] if matches else None
        if choice is None:
            return
        current_genus = self.genus_input.text().strip()
        current_species = self.species_input.text().strip()
        self._suppress_taxon_autofill = True
        try:
            vernacular_text = str(choice.common_name or name or "").strip()
            if vernacular_text:
                self.vernacular_input.setText(vernacular_text)
            if choice.genus and not current_genus:
                self.genus_input.setText(choice.genus)
            if choice.species and not current_species:
                self.species_input.setText(choice.species)
        finally:
            self._suppress_taxon_autofill = False
        if choice.species:
            self._handle_taxon_change()
        else:
            self._sync_taxon_cache()
        self._update_vernacular_suggestions_for_taxon()

    def _on_vernacular_editing_finished(self):
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        name = self.vernacular_input.text().strip()
        if not name:
            return
        entry = self._lookup_vernacular_entry(name, host=False)
        choice = self._vernacular_entry_to_choice(entry)
        if choice is None:
            current_genus = self.genus_input.text().strip() or None
            current_species = self.species_input.text().strip() or None
            matches = lookup.resolve_common_name(name, genus=current_genus, species=current_species)
            if len(matches) != 1:
                return
            choice = matches[0]
        if choice is None:
            return
        current_genus = self.genus_input.text().strip()
        current_species = self.species_input.text().strip()
        self._suppress_taxon_autofill = True
        try:
            vernacular_text = str(choice.common_name or name or "").strip()
            if vernacular_text:
                self.vernacular_input.setText(vernacular_text)
            if choice.genus and not current_genus:
                self.genus_input.setText(choice.genus)
            if choice.species and not current_species:
                self.species_input.setText(choice.species)
        finally:
            self._suppress_taxon_autofill = False
        if choice.species:
            self._handle_taxon_change()
        else:
            self._sync_taxon_cache()
        self._update_vernacular_suggestions_for_taxon()

    def _on_genus_text_changed(self, text):
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        if self._suppress_taxon_autofill:
            return
        text = text.strip()
        suggestions = lookup.suggest_genera(text, limit=TAXON_COMPLETER_LIMIT)

        # If text exactly matches a single suggestion, clear the model to prevent popup
        if len(suggestions) == 1 and suggestions[0].lower() == text.lower():
            self._genus_model.setStringList([])
            if self._genus_completer:
                self._genus_completer.popup().hide()
        else:
            self._genus_model.setStringList(suggestions)
        
        if not text:
            self._suppress_taxon_autofill = True
            self.species_input.clear()
            if hasattr(self, "vernacular_input"):
                self.vernacular_input.clear()
            self._suppress_taxon_autofill = False
            self._species_model.setStringList([])
            self._set_vernacular_entries(self._vernacular_model, [], host=False)
            self._set_vernacular_placeholder_from_suggestions([])
            # Reset species completer filtering
            if self._species_completer:
                self._species_completer.setCompletionPrefix("")
            self._set_species_placeholder_from_suggestions([])
            return

        # Reset species completer filtering when genus changes
        if self._species_completer and not self.species_input.hasFocus():
            self._species_completer.setCompletionPrefix("")

        if not self.species_input.text().strip():
            species_suggestions = lookup.suggest_species(text, "", limit=TAXON_COMPLETER_LIMIT)
            self._set_species_placeholder_from_suggestions(
                [choice.species for choice in species_suggestions if choice.species]
            )
        if hasattr(self, "vernacular_input") and not self.vernacular_input.text().strip():
            self._update_vernacular_suggestions_for_taxon()

    def _on_genus_editing_finished(self):
        if self._genus_completer and self._genus_completer.popup().isVisible():
            return
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup or self._suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted()
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()
        genus = self.genus_input.text().strip()
        if genus and not self.species_input.text().strip():
            species_suggestions = lookup.suggest_species(genus, "", limit=TAXON_COMPLETER_LIMIT)
            self._set_species_placeholder_from_suggestions(
                [choice.species for choice in species_suggestions if choice.species]
            )

    def _on_genus_selected(self, genus):
        # Hide the popup after selection
        if self._genus_completer:
            self._genus_completer.popup().hide()

        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        if self.species_input.text().strip():
            return
        species_suggestions = lookup.suggest_species(str(genus).strip(), "", limit=TAXON_COMPLETER_LIMIT)
        self._set_species_placeholder_from_suggestions(
            [choice.species for choice in species_suggestions if choice.species]
        )

    def _on_species_selected(self, species):
        """Handle species selection from completer."""
        # Hide the popup after selection
        if self._species_completer:
            self._species_completer.popup().hide()

        lookup = getattr(self, "_taxon_lookup", None)
        if lookup:
            self._resolve_current_taxon_to_accepted()
            self._handle_taxon_change()
            self._maybe_set_vernacular_from_taxon()
            self._update_vernacular_suggestions_for_taxon()

    def _on_species_editing_finished(self):
        if self._species_completer and self._species_completer.popup().isVisible():
            return
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup or self._suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted()
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()
        self._update_vernacular_suggestions_for_taxon()

    def _on_species_text_changed(self, text):
        if self._suppress_taxon_autofill:
            return
        lookup = getattr(self, "_taxon_lookup", None)
        if not lookup:
            return
        genus = self.genus_input.text().strip()
        if not genus:
            self._species_model.setStringList([])
            self._set_vernacular_entries(self._vernacular_model, [], host=False)
            self._set_vernacular_placeholder_from_suggestions([])
            return
        text_stripped = (text or "").strip()
        suggestions = lookup.suggest_species(genus, text_stripped, limit=TAXON_COMPLETER_LIMIT)
        species_values = [choice.species for choice in suggestions if choice.species]

        # Hide popup when text exactly matches any suggestion (covers multi-result cases)
        if text_stripped and any(s.lower() == text_stripped.lower() for s in species_values):
            self._species_model.setStringList([])
            if self._species_completer:
                self._species_completer.popup().hide()
        else:
            self._species_model.setStringList(species_values)
            if self._species_model.stringList() and self._species_completer:
                self._species_completer.setCompletionPrefix(text_stripped)
                self._species_completer.complete()

        if not text_stripped:
            self._set_species_placeholder_from_suggestions(species_values)
        self._update_vernacular_suggestions_for_taxon()
        if text_stripped:
            self._maybe_set_vernacular_from_taxon()

    def _handle_taxon_change(self):
        if not hasattr(self, "_last_genus"):
            self._sync_taxon_cache()
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        if genus != self._last_genus or species != self._last_species:
            current_common = self.vernacular_input.text().strip()
            lookup = getattr(self, "_taxon_lookup", None)
            if current_common and lookup and lookup.vernacular_db and genus and species:
                matches = lookup.resolve_common_name(current_common, genus=genus, species=species)
                if not matches:
                    self._suppress_taxon_autofill = True
                    self.vernacular_input.clear()
                    self._suppress_taxon_autofill = False
                    # Reset vernacular completer filtering after clearing
                    if self._vernacular_completer:
                        self._vernacular_completer.setCompletionPrefix("")
                    self._set_vernacular_entries(self._vernacular_model, [], host=False)
                    self._set_vernacular_placeholder_from_suggestions([])
        self._last_genus = genus
        self._last_species = species

    def _sync_taxon_cache(self):
        self._last_genus = self.genus_input.text().strip()
        self._last_species = self.species_input.text().strip()

    def eventFilter(self, obj, event):
        if obj == getattr(self, "location_input", None) and event.type() == QEvent.MouseButtonRelease:
            if not self.location_input.hasSelectedText():
                self._show_location_suggestions_dropdown()
        controller = getattr(self, "_taxon_controller", None)
        host_controller = getattr(self, "_host_taxon_controller", None)
        if controller is not None:
            controller.eventFilter(obj, event)
        if host_controller is not None:
            host_controller.eventFilter(obj, event)
        return super().eventFilter(obj, event)

    def _maybe_set_vernacular_from_taxon(self):
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return
        controller.sync_vernacular_after_taxon_change()

    def _refresh_vernacular_for_current_taxon(self) -> None:
        controller = getattr(self, "_taxon_controller", None)
        if controller is None:
            return
        controller.sync_vernacular_after_taxon_change()

    def _refresh_host_vernacular_for_current_taxon(self) -> None:
        controller = getattr(self, "_host_taxon_controller", None)
        if controller is None:
            return
        controller.sync_vernacular_after_taxon_change()

    def get_files(self):
        """Return selected image files."""
        return [item.filepath for item in self.image_results]

    def get_image_settings(self):
        """Return image settings (type and objective for each image)."""
        settings = []
        for item in self.image_results:
            settings.append({
                "image_type": item.image_type,
                "objective": item.objective,
                "contrast": item.contrast,
                "mount_medium": item.mount_medium,
                "stain": item.stain,
                "sample_type": item.sample_type,
                "sample_source": getattr(item, "sample_source", None),
                "notes": item.notes,
            })
        return settings

    def get_image_entries(self):
        """Return images with settings for saving."""
        entries = []
        for item in self.image_results:
            entries.append({
                "image_id": item.image_id,
                "filepath": item.filepath,
                "image_type": item.image_type or "field",
                "objective": item.objective,
                "contrast": item.contrast,
                "mount_medium": item.mount_medium,
                "stain": item.stain,
                "sample_type": item.sample_type,
                "sample_source": getattr(item, "sample_source", None),
                "notes": item.notes,
            })
        return entries

    def _load_habitat_tree(self, filename: str) -> list[dict]:
        base_dir = Path(__file__).resolve().parents[1]
        candidates = [
            base_dir / "database" / "reference_data" / "generated" / filename,
            base_dir / "database" / filename,
            Path(filename),
        ]
        path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return []
        if isinstance(data, list):
            return [node for node in data if isinstance(node, dict)]
        if isinstance(data, dict):
            roots = data.get("roots")
            if isinstance(roots, list):
                return [node for node in roots if isinstance(node, dict)]
        return []

    def _tree_depth(self, nodes: list[dict]) -> int:
        if not nodes:
            return 0
        max_child = 0
        for node in nodes:
            children = node.get("children")
            if isinstance(children, list) and children:
                max_child = max(max_child, self._tree_depth(children))
        return 1 + max_child

    @staticmethod
    def _normalize_tree_search_text(value: str) -> str:
        text = unicodedata.normalize("NFKD", str(value or ""))
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text.casefold().strip()

    def _build_habitat_tree_index(self, roots: list[dict]) -> dict:
        by_level: dict[int, list[dict]] = {}
        id_to_path: dict[int, list[int]] = {}
        node_to_path: dict[int, list[int]] = {}

        def visit(nodes: list[dict], level: int, path_nodes: list[dict]) -> None:
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                current_path_nodes = [*path_nodes, node]
                node_name = str(node.get("name") or "").strip()
                try:
                    node_id = int(node.get("id")) if node.get("id") is not None else None
                except Exception:
                    node_id = None
                path_ids = []
                for n in current_path_nodes:
                    try:
                        if n.get("id") is not None:
                            path_ids.append(int(n.get("id")))
                    except Exception:
                        continue
                entry = {
                    "node": node,
                    "node_id": node_id,
                    "name": node_name,
                    "path_ids": path_ids,
                    "path_names": [str(n.get("name") or "").strip() for n in current_path_nodes],
                    "search_text": self._normalize_tree_search_text(node_name),
                }
                by_level.setdefault(level, []).append(entry)
                node_to_path[id(node)] = path_ids
                if node_id is not None and path_ids:
                    id_to_path[node_id] = path_ids
                children = node.get("children")
                if isinstance(children, list) and children:
                    visit(children, level + 1, current_path_nodes)

        visit(roots, 0, [])
        return {"by_level": by_level, "id_to_path": id_to_path, "node_to_path": node_to_path}

    def _set_habitat_tree_level_options(
        self,
        key: str,
        level: int,
        entries: list[dict],
        selected_id: int | None = None,
        auto_popup: bool = False,
    ) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        if level < 0 or level >= len(combos):
            return
        combo = combos[level]
        sorted_entries = sorted(
            entries or [],
            key=lambda entry: self._normalize_tree_search_text(str(entry.get("name") or "")),
        )
        combo.blockSignals(True)
        combo.clear()
        combo.addItem(self.tr("Select..."), None)
        for entry in sorted_entries:
            node = entry.get("node")
            if not isinstance(node, dict):
                continue
            label = str(entry.get("name") or "")
            combo.addItem(label, node)
            idx = combo.count() - 1
            combo.setItemData(idx, entry.get("path_ids") or [], Qt.UserRole + 1)
        # Keep editable combos enabled even when a filter has no matches,
        # so the user can continue typing without re-clicking.
        combo.setEnabled(True)
        if selected_id is not None:
            for idx in range(1, combo.count()):
                node = combo.itemData(idx)
                try:
                    node_id = int(node.get("id")) if isinstance(node, dict) and node.get("id") is not None else None
                except Exception:
                    node_id = None
                if node_id == int(selected_id):
                    combo.setCurrentIndex(idx)
                    break
        else:
            combo.setCurrentIndex(0)
            # Keep "Select..." as placeholder text, not as editable content.
            if combo.isEditable():
                line_edit = combo.lineEdit()
                if line_edit is not None:
                    line_edit.clear()
                    line_edit.setPlaceholderText(self.tr("Select..."))
        combo.blockSignals(False)
        if auto_popup and combo.isEditable() and combo.count() > 1 and not combo.view().isVisible():
            combo.showPopup()

    def _clear_habitat_tree_children(self, key: str, from_level: int) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        for child_level in range(from_level + 1, len(combos)):
            all_level_entries = self._all_habitat_tree_level_entries(key, child_level)
            self._set_habitat_tree_level_options(
                key,
                child_level,
                all_level_entries,
                selected_id=None,
                auto_popup=False,
            )

    def _selected_habitat_tree_ancestor_ids(self, key: str, level: int) -> dict[int, int]:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        selected: dict[int, int] = {}
        for ancestor_level in range(min(level, len(combos))):
            node = combos[ancestor_level].currentData()
            if not isinstance(node, dict):
                continue
            try:
                node_id = int(node.get("id")) if node.get("id") is not None else None
            except Exception:
                node_id = None
            if node_id is not None:
                selected[ancestor_level] = node_id
        return selected

    @staticmethod
    def _habitat_tree_entry_matches_ancestors(entry: dict, selected_ancestors: dict[int, int]) -> bool:
        if not selected_ancestors:
            return True
        path_ids = entry.get("path_ids") or []
        for ancestor_level, ancestor_id in selected_ancestors.items():
            if ancestor_level >= len(path_ids):
                return False
            if path_ids[ancestor_level] != ancestor_id:
                return False
        return True

    def _filter_habitat_tree_entries_for_context(self, key: str, level: int, entries: list[dict]) -> list[dict]:
        selected_ancestors = self._selected_habitat_tree_ancestor_ids(key, level)
        return [
            entry
            for entry in entries
            if self._habitat_tree_entry_matches_ancestors(entry, selected_ancestors)
        ]

    def _all_habitat_tree_level_entries(self, key: str, level: int) -> list[dict]:
        state = self._habitat_tree_states.get(key) or {}
        tree_index = state.get("index") or {}
        level_entries = list((tree_index.get("by_level") or {}).get(level) or [])
        level_entries = self._filter_habitat_tree_entries_for_context(key, level, level_entries)
        return [
            {
                "node": entry.get("node"),
                "name": entry.get("name"),
                "path_ids": entry.get("path_ids") or [],
            }
            for entry in level_entries
            if isinstance(entry.get("node"), dict)
        ]

    def _apply_habitat_tree_selected_path(self, key: str, path_ids: list[int]) -> None:
        if not path_ids:
            return
        state = self._habitat_tree_states.get(key) or {}
        roots: list[dict] = state.get("roots") or []
        combos: list[QComboBox] = state.get("combos") or []
        if not combos:
            return
        self._populate_habitat_tree_level(key, 0, roots, path_ids[0] if path_ids else None)
        for level in range(1, len(combos)):
            prev_node = combos[level - 1].currentData()
            children = prev_node.get("children") if isinstance(prev_node, dict) else None
            self._populate_habitat_tree_level(
                key,
                level,
                children if isinstance(children, list) else [],
                path_ids[level] if level < len(path_ids) else None,
            )

    def _on_habitat_tree_level_text_edited(self, key: str, level: int, text: str) -> None:
        state = self._habitat_tree_states.get(key) or {}
        if state.get("suppress_text_filter"):
            return
        query = self._normalize_tree_search_text(text)
        if not query:
            # Restore full list for this level when filter is cleared.
            self._set_habitat_tree_level_options(
                key,
                level,
                self._all_habitat_tree_level_entries(key, level),
                selected_id=None,
                auto_popup=False,
            )
            return
        tree_index = state.get("index") or {}
        level_entries = list((tree_index.get("by_level") or {}).get(level) or [])
        level_entries = self._filter_habitat_tree_entries_for_context(key, level, level_entries)
        if not level_entries:
            return
        filtered = [entry for entry in level_entries if query in str(entry.get("search_text") or "")]
        state["suppress_text_filter"] = True
        self._set_habitat_tree_level_options(key, level, filtered, selected_id=None, auto_popup=False)
        combos: list[QComboBox] = state.get("combos") or []
        if 0 <= level < len(combos):
            line_edit = combos[level].lineEdit()
            if line_edit is not None:
                line_edit.setText(text)
                line_edit.setCursorPosition(len(text))
        state["suppress_text_filter"] = False

    def _on_habitat_tree_level_activated(self, key: str, level: int, _index: int) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        if level < 0 or level >= len(combos):
            return
        combo = combos[level]
        path_ids = combo.currentData(Qt.UserRole + 1)
        if not path_ids:
            return
        state["suppress_text_filter"] = True
        try:
            self._apply_habitat_tree_selected_path(key, [int(v) for v in path_ids])
        finally:
            state["suppress_text_filter"] = False
        self._update_taxonomy_tab_indicators()

    def _clear_habitat_tree_level_selection(self, key: str, level: int) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        if level < 0 or level >= len(combos):
            return
        roots: list[dict] = state.get("roots") or []
        state["suppress_text_filter"] = True
        try:
            if level == 0:
                self._populate_habitat_tree_level(key, 0, roots, selected_id=None)
            else:
                parent_node = combos[level - 1].currentData()
                children = parent_node.get("children") if isinstance(parent_node, dict) else None
                self._populate_habitat_tree_level(
                    key,
                    level,
                    children if isinstance(children, list) else None,
                    selected_id=None,
                )
        finally:
            state["suppress_text_filter"] = False
        self._update_taxonomy_tab_indicators()

    def _create_habitat_tree_controls(
        self,
        parent_layout: QVBoxLayout,
        title: str,
        key: str,
        roots: list[dict],
        *,
        expand_fields: bool = False,
    ) -> None:
        label_width = int(getattr(self, "_taxonomy_label_width", 96) or 96)
        group, group_layout = self._create_dialog_box(
            title,
            QFormLayout,
            body_margins=(8, 8, 8, 8),
            body_spacing=6,
        )
        group_layout.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        group_layout.setFormAlignment(Qt.AlignLeft | Qt.AlignTop)
        group_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        level_count = max(1, min(6, self._tree_depth(roots)))
        combos: list[QComboBox] = []
        for level in range(level_count):
            combo = QComboBox()
            combo.setEditable(True)
            combo.setInsertPolicy(QComboBox.NoInsert)
            combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            combo.addItem(self.tr("Select..."), None)
            combo.currentIndexChanged.connect(
                lambda _idx, tree_key=key, tree_level=level: self._on_habitat_tree_level_changed(tree_key, tree_level)
            )
            combo.activated.connect(
                lambda idx, tree_key=key, tree_level=level: self._on_habitat_tree_level_activated(tree_key, tree_level, idx)
            )
            line_edit = combo.lineEdit()
            if line_edit is not None:
                line_edit.textEdited.connect(
                    lambda text, tree_key=key, tree_level=level: self._on_habitat_tree_level_text_edited(tree_key, tree_level, text)
                )
            clear_btn = QToolButton()
            clear_btn.setText("X")
            clear_btn.setAutoRaise(True)
            clear_btn.setFixedWidth(20)
            clear_btn.setToolTip(self.tr("Clear selection"))
            clear_btn.clicked.connect(
                lambda _checked=False, tree_key=key, tree_level=level: self._clear_habitat_tree_level_selection(
                    tree_key, tree_level
                )
            )
            row_widget = QWidget()
            row_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            row_layout = QHBoxLayout(row_widget)
            row_layout.setContentsMargins(0, 0, 0, 0)
            row_layout.setSpacing(4)
            row_layout.addWidget(combo, 1)
            row_layout.addWidget(clear_btn, 0)
            level_label = QLabel(self.tr("Level {n}:").format(n=level + 1))
            level_label.setFixedWidth(label_width)
            group_layout.addRow(level_label, row_widget)
            combos.append(combo)
        tree_index = self._build_habitat_tree_index(roots)
        self._habitat_tree_states[key] = {
            "roots": roots,
            "combos": combos,
            "index": tree_index,
            "group": group,
            "group_header": getattr(group, "_box_header", None),
            "note_label": getattr(self, f"{key}_target_note", None),
            "suppress_text_filter": False,
        }
        self._populate_habitat_tree_level(key, 0, roots)
        parent_layout.addWidget(group)

    def _populate_habitat_tree_level(
        self,
        key: str,
        level: int,
        nodes: list[dict] | None,
        selected_id: int | None = None,
    ) -> None:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        if level < 0 or level >= len(combos):
            return
        combo = combos[level]

        valid_nodes = [node for node in (nodes or []) if isinstance(node, dict)]
        if not valid_nodes:
            # Allow direct search/selection on deeper levels even without selecting parents first.
            valid_nodes = [
                entry.get("node")
                for entry in self._all_habitat_tree_level_entries(key, level)
                if isinstance(entry.get("node"), dict)
            ]
        entries = []
        for node in valid_nodes:
            node_name = str(node.get("name") or "").strip()
            path_ids = []
            try:
                node_id = int(node.get("id")) if node.get("id") is not None else None
            except Exception:
                node_id = None
            idx = state.get("index") or {}
            node_map = idx.get("node_to_path") or {}
            path_ids = list(node_map.get(id(node)) or [])
            if not path_ids and node_id is not None:
                idx_map = idx.get("id_to_path") or {}
                path_ids = idx_map.get(node_id) or [node_id]
            entries.append(
                {
                    "node": node,
                    "name": node_name,
                    "path_ids": path_ids,
                }
            )
        self._set_habitat_tree_level_options(key, level, entries, selected_id=selected_id)

        self._clear_habitat_tree_children(key, level)

        node = combo.currentData()
        children = node.get("children") if isinstance(node, dict) else None
        if level + 1 < len(combos) and isinstance(children, list) and children:
            self._populate_habitat_tree_level(key, level + 1, children)

    def _on_habitat_tree_level_changed(self, key: str, level: int) -> None:
        state = self._habitat_tree_states.get(key) or {}
        if state.get("suppress_text_filter"):
            return
        combos: list[QComboBox] = state.get("combos") or []
        if level < 0 or level >= len(combos):
            return
        node = combos[level].currentData()
        children = node.get("children") if isinstance(node, dict) else None
        if level + 1 < len(combos):
            self._populate_habitat_tree_level(key, level + 1, children if isinstance(children, list) else None)
        self._update_taxonomy_tab_indicators()

    def _selected_habitat_tree_path(self, key: str) -> list[dict]:
        state = self._habitat_tree_states.get(key) or {}
        combos: list[QComboBox] = state.get("combos") or []
        selected: list[dict] = []
        for combo in combos:
            node = combo.currentData()
            if not isinstance(node, dict):
                break
            selected.append(node)
        return selected

    def _apply_habitat_tree_path(self, key: str, raw_path: str | None) -> None:
        state = self._habitat_tree_states.get(key) or {}
        roots: list[dict] = state.get("roots") or []
        combos: list[QComboBox] = state.get("combos") or []
        if not combos:
            return
        ids: list[int] = []
        try:
            parsed = json.loads(raw_path or "[]")
            if isinstance(parsed, list):
                ids = [int(v) for v in parsed]
        except Exception:
            ids = []
        # Backward compatibility: older saved paths may use shortcut levels.
        # Resolve to the canonical tree path for the deepest selected node id.
        if ids:
            try:
                deepest_id = int(ids[-1])
            except Exception:
                deepest_id = None
            if deepest_id is not None:
                canonical = ((state.get("index") or {}).get("id_to_path") or {}).get(deepest_id) or []
                if canonical:
                    ids = [int(v) for v in canonical]
        self._populate_habitat_tree_level(key, 0, roots, ids[0] if ids else None)
        for level in range(1, len(combos)):
            prev_node = combos[level - 1].currentData()
            children = prev_node.get("children") if isinstance(prev_node, dict) else None
            self._populate_habitat_tree_level(
                key,
                level,
                children if isinstance(children, list) else [],
                ids[level] if level < len(ids) else None,
            )
        self._update_taxonomy_tab_indicators()

    def _load_tag_options(self, category: str) -> list[str]:
        setting_key = DatabaseTerms.setting_key(category)
        defaults = DatabaseTerms.default_values(category)
        options = SettingsDB.get_list_setting(setting_key, defaults)
        return DatabaseTerms.canonicalize_list(category, options)

    def _preferred_tag_value(self, category: str, options: list[str], fallback: str) -> str:
        options = options or [fallback]
        legacy_default_key = {
            "contrast": "contrast_default",
            "mount": "mount_default",
            "stain": "stain_default",
            "sample": "sample_default",
        }.get(category, "")
        preferred = SettingsDB.get_setting(DatabaseTerms.last_used_key(category), None)
        if not preferred and legacy_default_key:
            preferred = SettingsDB.get_setting(legacy_default_key, None)
        preferred = DatabaseTerms.canonicalize(category, preferred)
        if preferred and preferred in options:
            return preferred
        if preferred and preferred not in options:
            options.insert(0, preferred)
        return options[0] if options else fallback

    def _field_tag_value(self, category: str) -> str:
        mapping = {
            "contrast": DatabaseTerms.CONTRAST_METHODS[0],
            "mount": DatabaseTerms.MOUNT_MEDIA[0],
            "stain": DatabaseTerms.STAIN_TYPES[0],
            "sample": DatabaseTerms.SAMPLE_TYPES[0],
        }
        return mapping[category]

    def _populate_tag_combo(self, combo: QComboBox, category: str, options: list[str]) -> None:
        combo.clear()
        for canonical in options:
            combo.addItem(DatabaseTerms.translate(category, canonical), canonical)

    def _set_tag_combo_neutral_display(self, combo: QComboBox, category: str, blank: bool) -> None:
        neutral_value = self._field_tag_value(category)
        idx = combo.findData(neutral_value)
        if idx < 0:
            return
        combo.setItemText(idx, "" if blank else DatabaseTerms.translate(category, neutral_value))

    def _load_objectives(self):
        """Load objectives from JSON file."""
        return load_objectives()

    def _get_default_objective(self):
        """Get the default objective key."""
        # Check already-loaded objectives for default
        for key, obj in self.objectives.items():
            if obj.get('is_default'):
                return key
        # Return first objective if no default set
        if self.objectives:
            return sorted(self.objectives.keys())[0]
        return None

    def _load_observation_values(self, obs: dict | None) -> None:
        """Preload observation details from an existing observation or in-memory draft."""
        # Stage 3B.5: loading a fresh observation into an already-live
        # editor must invalidate any in-flight deferred red-list resolve
        # scheduled for the previous observation. The generation nonce
        # is the reliable guard; the identity token cannot distinguish
        # two observations whose (sporely_taxon_id, scientific_name,
        # country_code) coincide.
        self._redlist_generation = getattr(self, "_redlist_generation", 0) + 1
        obs = obs or {}
        date_str = obs.get("date")
        if date_str:
            dt = _parse_observation_datetime(date_str)
            if dt and dt.isValid():
                self.datetime_input.setDateTime(dt)

        genus = obs.get("genus") or ""
        species = obs.get("species") or ""
        has_identification = any(
            str(obs.get(field) or "").strip()
            for field in (
                "genus",
                "species",
                "common_name",
                "species_guess",
                "ai_selected_scientific_name",
            )
        )
        if not genus and not species and has_identification:
            for candidate in (obs.get("species_guess"), obs.get("ai_selected_scientific_name")):
                fallback_genus, fallback_species = self._split_scientific_name_text(candidate)
                if not genus and fallback_genus:
                    genus = fallback_genus
                if not species and fallback_species:
                    species = fallback_species
                if genus and species:
                    break
        self.taxonomy_tabs.setCurrentIndex(0)
        # Stage 3B.3: suppress controller invalidation while persisted
        # identity values are copied into the editor widgets.
        _controller = getattr(self, "_taxon_controller", None)
        _load_ctx = (
            _controller._suspended()
            if _controller is not None
            else _NullSuspend()
        )
        with _load_ctx:
            self.genus_input.setText(genus)
            self.species_input.setText(species)
            self.uncertain_checkbox.setChecked(bool(obs.get("uncertain", 0)))
            # UX: never auto-check "Unidentified" merely because the loaded
            # observation lacks identification fields. That state is a
            # UI-only convenience for the user to say "I don't want to
            # identify this now"; it disables the taxonomy widgets. Empty
            # identification on load is the normal state of a fresh
            # import or of a previously-saved not-yet-identified
            # observation — the user MUST be able to start typing without
            # first discovering and unchecking the box. The identity
            # fields themselves round-trip via ``get_data`` regardless.
            self.unidentified_checkbox.setChecked(False)
            if hasattr(self, "vernacular_input"):
                self.vernacular_input.setText(obs.get("common_name") or "")
            if hasattr(self, "scientific_name_input"):
                self.scientific_name_input.setText(
                    (obs.get("scientific_name_snapshot") or "").strip()
                )

        # Restore the committed snapshot after all text widgets have settled.
        # Missing identity fields mean the observation remains unresolved.
        snapshot_name = (
            obs.get("scientific_name_snapshot") or ""
        ).strip()
        rank_snapshot = (
            obs.get("taxon_rank_snapshot") or ""
        ).strip()

        # Taxonomy-v2 closeout Stage 2: restore the identity from its own
        # provenance columns, not from the presence of a scientific-name
        # snapshot plus an integer. An unresolved external identity has a
        # source tuple and NO Sporely ID, so the old
        # ``sporely_id is not None`` condition would have discarded exactly
        # the state this stage exists to preserve.
        restored_identity = TaxonIdentity.from_row(obs)
        if _controller is not None:
            has_identity = (
                restored_identity.is_proven_sporely
                or restored_identity.is_legacy_unverified
                or restored_identity.is_cloud_selected_unverified
                or restored_identity.has_external_evidence
            )
            # A proven Sporely identity stored without name/rank snapshots
            # (the 2026-10-07 stage 4 identity repair bound rows that way) is
            # still the committed identity. Restore it as stored, without
            # deriving a name from text or the release, so an unchanged save
            # writes back the same columns; editing genus/species still
            # invalidates it through the controller.
            if has_identity and (
                (snapshot_name and rank_snapshot) or restored_identity.is_proven_sporely
            ):
                _controller.load_committed_snapshot({
                    "genus": genus,
                    "species": species,
                    "scientific_name": snapshot_name,
                    "taxon_rank_snapshot": rank_snapshot or None,
                    # Canonical fields are not re-fetched on load.
                    **restored_identity.to_row(),
                })
            else:
                _controller.load_committed_snapshot(None)
        if hasattr(self, "taxon_picker"):
            # The concept already on the observation stays its row's default:
            # neither Artsorakel evidence nor the source order replaces it.
            self.taxon_picker.set_current_taxon_id(
                restored_identity.sporely_taxon_id if restored_identity.is_proven_sporely else None
            )
        self._set_inaturalist_taxon_id(obs.get("inaturalist_taxon_id"))
        red_categories = None
        raw_red_categories = obs.get("red_list_categories_json")
        if raw_red_categories:
            try:
                parsed_red_categories = json.loads(raw_red_categories)
                if isinstance(parsed_red_categories, dict):
                    red_categories = parsed_red_categories
            except Exception:
                red_categories = None
        red_code = obs.get("red_list_category")

        # The Red List restored on load comes ONLY from the persisted
        # columns. ``ai_state_json`` is provider/UI history — the per-image
        # prediction a user highlighted — not the committed identity, so its
        # assessment may belong to a different taxon. Borrowing it here let
        # an unrelated save persist that taxon's category onto the
        # observation. No assessment beats someone else's assessment; the
        # identity-gated lookup (`_resolve_and_apply_redlist`) is what
        # derives a category for the committed concept.

        # Preserve raw degree-marked values such as VU°.
        self._set_red_list_category_raw(
            red_code,
            red_categories,
            hint=None,
        )

        self.unspontaneous_checkbox.setChecked(bool(obs.get("unspontaneous", 0)))
        self._set_sharing_scope(
            obs.get("sharing_scope"),
            location_public=obs.get("location_public"),
        )
        self._set_location_precision(obs.get("location_precision"))
        if hasattr(self, "is_draft_checkbox"):
            self.is_draft_checkbox.setChecked(bool(obs.get("is_draft", 1)))
        cloud_id = str(obs.get("cloud_id") or "").strip()
        self._cloud_privacy_original_slot_consumption = (
            1 if cloud_id and cloud_observation_uses_privacy_slot(obs) else 0
        )
        self._update_cloud_privacy_slots_label()
        method = obs.get("determination_method")
        idx = self.determination_method_combo.findData(method)
        if idx >= 0:
            self.determination_method_combo.setCurrentIndex(idx)
        else:
            self.determination_method_combo.setCurrentIndex(0)

        self.location_input.setText(obs.get("location") or "")
        legacy_notes = (obs.get("notes") or "").strip()
        self.open_comment_input.setPlainText((obs.get("open_comment") or legacy_notes).strip())
        self.private_comment_input.setPlainText((obs.get("private_comment") or "").strip())
        if hasattr(self, "host_genus_input"):
            self.host_genus_input.setText((obs.get("habitat_host_genus") or "").strip())
        if hasattr(self, "host_species_input"):
            self.host_species_input.setText((obs.get("habitat_host_species") or "").strip())
        if hasattr(self, "host_vernacular_input"):
            self.host_vernacular_input.setText((obs.get("habitat_host_common_name") or "").strip())
        lat = obs.get("gps_latitude")
        lon = obs.get("gps_longitude")
        if lat is not None:
            self.lat_input.setValue(lat)
        if lon is not None:
            self.lon_input.setValue(lon)
        # Restore any previously persisted geography so a subsequent save does
        # not accidentally clear cloud values. ``normalize_country_code`` is
        # forgiving of legacy lowercase storage while still filtering junk.
        stored_country = normalize_country_code(obs.get("country_code"))
        self._location_country_code = stored_country or ""
        raw_region = obs.get("region_id")
        if isinstance(raw_region, str):
            region_text = raw_region.strip()
            self._location_region_id = region_text or None
        elif raw_region is None:
            self._location_region_id = None
        else:
            self._location_region_id = str(raw_region).strip() or None
        try:
            if lat is not None and lon is not None:
                self._location_country_coords = (float(lat), float(lon))
            else:
                self._location_country_coords = None
        except (TypeError, ValueError):
            self._location_country_coords = None
        saved_target = resolve_observation_publish_target(
            obs,
            default_target=self._active_reporting_target(),
        )
        self._set_publish_target_combo(saved_target, manual_override=True)
        self._apply_habitat_tree_path("nin2", obs.get("habitat_nin2_path"))
        self._apply_habitat_tree_path("substrate", obs.get("habitat_substrate_path"))
        if hasattr(self, "nin2_note_input"):
            self.nin2_note_input.setPlainText(obs.get("habitat_nin2_note") or "")
        if hasattr(self, "substrate_note_input"):
            self.substrate_note_input.setPlainText(obs.get("habitat_substrate_note") or "")
        if hasattr(self, "grows_on_note_input"):
            self.grows_on_note_input.setPlainText(obs.get("habitat_grows_on_note") or "")
        self._update_map_button()
        self._update_selected_ai_summary_label(obs)
        self._maybe_set_vernacular_from_taxon()
        self._update_taxonomy_tab_indicators()

    def _default_sharing_scope(self) -> str:
        return normalize_sharing_scope(
            SettingsDB.get_setting(
                SETTING_CLOUD_DEFAULT_SHARING_SCOPE,
                SHARING_SCOPE_PUBLIC,
            ),
            fallback=SHARING_SCOPE_PUBLIC,
        )

    def _selected_sharing_scope(self) -> str:
        selector = getattr(self, "sharing_scope_selector", None)
        if selector is not None:
            return normalize_sharing_scope(selector.selected_value(self._default_sharing_scope()), fallback=self._default_sharing_scope())
        return normalize_sharing_scope(getattr(self, "_sharing_scope_value", None), fallback=self._default_sharing_scope())

    def _set_sharing_scope(self, scope: str | None, location_public=None) -> None:
        raw = str(scope or "").strip().lower()
        normalized = raw if raw in _VALID_SHARING_SCOPES else ""
        if not normalized:
            normalized = (
                SHARING_SCOPE_FRIENDS
                if bool(location_public)
                else self._default_sharing_scope()
            )
        self._sharing_scope_value = normalized
        selector = getattr(self, "sharing_scope_selector", None)
        if selector is not None:
            selector.set_selected_value(normalized)

    def _selected_location_precision(self) -> str:
        # A stored 'hidden'/'region' precision (set elsewhere, e.g. on the
        # web) is kept unless the user picks a level here; it is never
        # silently rewritten to 'exact'.
        preserved = getattr(self, "_preserved_location_precision", None)
        if preserved:
            return preserved
        selector = getattr(self, "location_precision_selector", None)
        if selector is not None:
            return normalize_location_precision(selector.selected_value(LOCATION_PRECISION_EXACT))
        return LOCATION_PRECISION_EXACT

    def _set_location_precision(self, value: str | None) -> None:
        raw = str(value or "").strip().lower()
        preserved = raw if raw in {"hidden", "region"} else None
        normalized = normalize_location_precision(
            LOCATION_PRECISION_FUZZED if preserved else value
        )
        selector = getattr(self, "location_precision_selector", None)
        self._setting_location_precision = True
        try:
            if selector is not None:
                selector.set_selected_value(normalized)
        finally:
            self._setting_location_precision = False
        self._preserved_location_precision = preserved

    def _on_location_precision_selected(self, _value) -> None:
        if not getattr(self, "_setting_location_precision", False):
            self._preserved_location_precision = None

    def _load_existing_observation(self):
        """Preload observation details and images for editing."""
        self._load_observation_values(self.observation)


# Imported first (not through ui.observations_tab): that module could not
# re-export the class while this one was still loading, so hand it over now.
if not hasattr(_observations_tab, "ObservationDetailsDialog"):
    _observations_tab.ObservationDetailsDialog = ObservationDetailsDialog
