# ui/observations_tab.py
"""Observations tab for managing mushroom observations and photos."""
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                                QTableWidget, QTableWidgetItem, QHeaderView,
                                QDialog, QFormLayout, QLineEdit, QTextEdit,
                                QDateTimeEdit, QFileDialog, QLabel, QMessageBox,
                                QSplitter, QRadioButton, QButtonGroup,
                                QComboBox,
                                QListWidget, QListWidgetItem, QGroupBox, QCheckBox,
                                QDoubleSpinBox, QTabWidget, QDialogButtonBox, QCompleter,
                                QSizePolicy, QAbstractItemView, QFrame, QProgressDialog,
                                QApplication, QMenu, QProgressBar, QToolButton, QScrollArea,
                                QGridLayout)
from PySide6.QtCore import Signal, Qt, QDateTime, QSize, QStringListModel, QEvent, QTimer, QThread, QPointF, QStandardPaths, QCoreApplication, QSettings
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
    QPainterPath,
    QPen,
)
from PIL import Image
from PySide6.QtCore import QUrl
from pathlib import Path
import sqlite3
import csv
import shutil
import tempfile
import threading
import time
import os
import sys
import html
import json
import unicodedata
from queue import SimpleQueue, Empty
from database.models import ObservationDB, ImageDB, MeasurementDB, SettingsDB, CalibrationDB
from database.database_tags import DatabaseTerms
from database.schema import (
    get_connection,
    get_database_path,
    get_images_dir,
    get_app_settings,
    load_objectives,
    objective_display_name,
    objective_sort_value,
    resolve_objective_key,
    update_app_settings,
)
from utils.thumbnail_generator import get_thumbnail_path, generate_all_sizes
from utils.image_utils import cleanup_import_temp_file
from utils.exif_reader import get_image_metadata
from utils.heic_converter import maybe_convert_heic
from utils.ml_export import export_coco_format, get_export_summary
from datetime import datetime
import re
import requests
from urllib.parse import urlparse, parse_qs
from utils.vernacular_utils import (
    normalize_vernacular_language,
    resolve_vernacular_db_path,
    vernacular_language_label,
    list_available_vernacular_languages,
)
from utils.publish_targets import (
    PUBLISH_TARGET_ARTPORTALEN_SE,
    PUBLISH_TARGET_ARTSOBS_NO,
    SETTING_ACTIVE_REPORTING_TARGET,
    infer_publish_target_from_coords,
    normalize_publish_target,
    nonregional_uploader_keys,
    publish_target_label,
    uploader_key_for_publish_target,
)
from .image_gallery_widget import ImageGalleryWidget
from .image_import_dialog import (
    ImageImportDialog,
    ImageImportResult,
    AIGuessWorker,
    dropped_image_paths_from_mime_data,
)
from .calibration_dialog import get_resolution_status
from .hint_status import HintBar, HintStatusController, style_progress_widgets
from .zoomable_image_widget import ZoomableImageLabel
from .dialog_helpers import ask_measurements_exist_delete, ask_wrapped_yes_no, make_github_help_button
from .styles import pt
from .window_state import GeometryMixin
from matplotlib.ticker import MaxNLocator
from app_identity import APP_NAME, SETTINGS_APP, SETTINGS_ORG, app_data_dir


def _parse_observation_datetime(value: str | None) -> QDateTime | None:
    if not value:
        return None
    for fmt in ("yyyy-MM-dd HH:mm", "yyyy-MM-dd HH:mm:ss"):
        dt_value = QDateTime.fromString(value, fmt)
        if dt_value.isValid():
            return dt_value
    dt_value = QDateTime.fromString(value, Qt.ISODate)
    return dt_value if dt_value.isValid() else None


def _normalized_observation_datetime_minute(value: str | None) -> str | None:
    dt_value = _parse_observation_datetime(value)
    if not dt_value or not dt_value.isValid():
        return None
    return dt_value.toString("yyyy-MM-dd HH:mm")


def _debug_import_flow_enabled() -> bool:
    value = (
        os.environ.get("SPORELY_DEBUG_IMPORT_FLOW", "")
        or os.environ.get("MYCOLOG_DEBUG_IMPORT_FLOW", "")
    )
    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _debug_import_flow(message: str) -> None:
    if _debug_import_flow_enabled():
        print(f"[{APP_NAME} debug][obs-edit] {message}", flush=True)


def _extract_coords_from_osm_url(text: str) -> tuple[float, float] | None:
    if not text:
        return None
    match = re.search(r"#map=\d+/(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)", text)
    if match:
        lat = float(match.group(1))
        lon = float(match.group(2))
        if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
            return lat, lon
    parsed = urlparse(text)
    query = parse_qs(parsed.query)
    if "mlat" in query and "mlon" in query:
        try:
            lat = float(query["mlat"][0])
            lon = float(query["mlon"][0])
            if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                return lat, lon
        except (TypeError, ValueError, IndexError):
            return None
    return None


class UploadCancelledError(Exception):
    """Raised when user cancels an upload task from the progress dialog."""


class LocationLookupWorker(QThread):
    """Background worker to look up place name from coordinates."""
    resultReady = Signal(str)

    def __init__(self, lat: float, lon: float, parent=None):
        super().__init__(parent)
        self.lat = lat
        self.lon = lon

    def run(self):
        try:
            resp = requests.get(
                "https://stedsnavn.artsdatabanken.no/v1/punkt",
                params={"lat": self.lat, "lng": self.lon, "zoom": 55},
                headers={"Accept": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                name = data.get("navn", "")
                if name:
                    self.resultReady.emit(name)
        except Exception:
            pass


def resolve_observation_publish_target(
    observation: dict | None,
    default_target: str = PUBLISH_TARGET_ARTSOBS_NO,
) -> str:
    """Preserve an observation's own reporting system before using app defaults."""
    obs = observation or {}
    raw_target = str(obs.get("publish_target") or "").strip()
    if raw_target:
        normalized = normalize_publish_target(raw_target, fallback="")
        if normalized:
            return normalized
    if obs.get("artportalen_id"):
        return PUBLISH_TARGET_ARTPORTALEN_SE
    if obs.get("artsdata_id"):
        return PUBLISH_TARGET_ARTSOBS_NO
    if obs.get("habitat_nin2_path") or obs.get("habitat_substrate_path"):
        return PUBLISH_TARGET_ARTSOBS_NO
    inferred = infer_publish_target_from_coords(
        obs.get("gps_latitude"),
        obs.get("gps_longitude"),
    )
    return inferred or normalize_publish_target(default_target, fallback=PUBLISH_TARGET_ARTSOBS_NO)


class ArtsobsMobileLinkCheckWorker(QThread):
    """Background worker that checks public Artsobs sighting links."""

    linkChecked = Signal(int, bool, bool)  # observation_id, is_dead, is_publicly_published
    checkFinished = Signal(int, int, int)  # checked, alive, dead
    checkFailed = Signal(str)

    def __init__(self, checks: list[tuple[int, int]], parent=None):
        super().__init__(parent)
        self._checks = checks

    def run(self):
        if not self._checks:
            self.checkFinished.emit(0, 0, 0)
            return

        session = requests.Session()

        checked = 0
        alive = 0
        dead = 0
        for observation_id, arts_id in self._checks:
            if self.isInterruptionRequested():
                break
            try:
                # Public web endpoint works without mobile session cookies.
                url = f"https://www.artsobservasjoner.no/Sighting/{arts_id}"
                response = session.get(url, timeout=12)
                final_url = str(getattr(response, "url", "") or "")
                final_url_l = final_url.lower()
                is_dead = response.status_code == 404
                is_publicly_published = False
                if response.status_code == 200:
                    if "/viewsighting/sightingnotpublished" in final_url_l:
                        is_publicly_published = False
                    elif f"/sighting/{arts_id}" in final_url_l:
                        is_publicly_published = True
                if response.status_code in (200, 404):
                    checked += 1
                    if is_dead:
                        dead += 1
                    else:
                        alive += 1
                    self.linkChecked.emit(observation_id, is_dead, is_publicly_published)
                elif response.status_code in (401, 403):
                    self.checkFailed.emit("Artsobs link check unauthorized.")
                    break
            except Exception:
                continue

        self.checkFinished.emit(checked, alive, dead)


class SortableTableWidgetItem(QTableWidgetItem):
    """Table item that prefers UserRole for sorting when available."""

    def __lt__(self, other):
        self_data = self.data(Qt.UserRole)
        other_data = other.data(Qt.UserRole)
        if self_data is None or other_data is None:
            return super().__lt__(other)
        try:
            return self_data < other_data
        except TypeError:
            return str(self_data) < str(other_data)


class MapServiceHelper:
    """Shared map service helpers for observation dialogs."""

    def __init__(self, parent):
        self.parent = parent
        self._nbic_index: dict[str, int] | None = None

    def _set_status(self, message: str, level: str = "warning") -> None:
        if self.parent and hasattr(self.parent, "set_status_message"):
            self.parent.set_status_message(message, level=level)

    def _utm_from_latlon(self, lat, lon):
        """Convert WGS84 lat/lon to EUREF89 / UTM 33N."""
        try:
            from pyproj import Transformer
        except Exception as exc:
            self._set_status(
                "pyproj is required for UTM conversions. Install it and try again.",
                level="error",
            )
            raise exc
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:25833", always_xy=True)
        easting, northing = transformer.transform(lon, lat)
        return easting, northing

    def _normalize_species_key(self, text: str | None) -> str:
        if not text:
            return ""
        return " ".join(text.strip().lower().split())

    def _load_nbic_index(self) -> dict[str, int]:
        if self._nbic_index is not None:
            return self._nbic_index
        self._nbic_index = {}
        try:
            try:
                csv.field_size_limit(1024 * 1024 * 10)
            except OverflowError:
                csv.field_size_limit(2147483647)
            base_dir = Path(__file__).resolve().parents[1]
            taxon_path = base_dir / "database" / "taxon.txt"
            if not taxon_path.exists():
                return self._nbic_index
            with taxon_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                for row in reader:
                    if (row.get("taxonRank") or "").strip().lower() != "species":
                        continue
                    if (row.get("taxonomicStatus") or "").strip().lower() != "valid":
                        continue
                    taxon_id = (row.get("id") or row.get("taxonID") or "").strip()
                    if not taxon_id:
                        continue
                    sci = (row.get("scientificName") or "").strip()
                    genus = (row.get("genus") or "").strip()
                    species = (row.get("specificEpithet") or "").strip()
                    if sci:
                        self._nbic_index[self._normalize_species_key(sci)] = int(taxon_id)
                    if genus and species:
                        combined = f"{genus} {species}"
                        self._nbic_index[self._normalize_species_key(combined)] = int(taxon_id)
        except Exception:
            return self._nbic_index
        return self._nbic_index

    def _nbic_id_from_local(self, scientific_name: str) -> int | None:
        key = self._normalize_species_key(scientific_name)
        if not key:
            return None
        index = self._load_nbic_index()
        return index.get(key)

    def _taxon_id_from_nbic(self, nbic_id: int) -> int | None:
        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for Artsdatabanken lookups.") from exc
        url = f"https://artsdatabanken.no/Api/Taxon/ScientificName/{nbic_id}"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        taxon_id = data.get("taxonID")
        return int(taxon_id) if taxon_id else None

    def _inat_taxon_id(self, species_name):
        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for iNaturalist lookups.") from exc

        url = "https://api.inaturalist.org/v1/taxa"
        params = {"q": species_name, "rank": "species", "per_page": 1}
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        if not data.get("results"):
            raise ValueError("No taxon found")
        return data["results"][0]["id"]

    def _inat_map_link(self, species_name, lat, lon, radius_km):
        from urllib.parse import urlencode

        params = {"lat": lat, "lng": lon, "radius": radius_km}
        if species_name:
            taxon_id = self._inat_taxon_id(species_name)
            params["taxon_id"] = taxon_id
        return "https://www.inaturalist.org/observations?" + urlencode(params)

    def _inat_species_link(self, species_name):
        taxon_id = self._inat_taxon_id(species_name)
        slug = species_name.strip().replace(" ", "-")
        return f"https://www.inaturalist.org/taxa/{taxon_id}-{slug}"

    def open_inaturalist_map(self, lat, lon, species_name):
        """Open iNaturalist observations map for the selected species."""
        import webbrowser
        try:
            url = self._inat_map_link(species_name, lat, lon, 50.0)
        except Exception as exc:
            self._set_status(f"iNaturalist lookup failed: {exc}", level="warning")
            return
        webbrowser.open(url)

    def open_inaturalist_species(self, species_name):
        import webbrowser
        try:
            url = self._inat_species_link(species_name)
        except Exception as exc:
            self._set_status(f"iNaturalist lookup failed: {exc}", level="warning")
            return
        webbrowser.open(url)

    def _gbif_taxon_id(self, species_name):
        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for GBIF lookups.") from exc

        url = "https://api.gbif.org/v1/species/match"
        response = requests.get(
            url,
            params={"name": species_name, "strict": "false", "kingdom": "Fungi"},
            timeout=20
        )
        response.raise_for_status()
        data = response.json()
        taxon_id = data.get("usageKey")
        if not taxon_id:
            raise ValueError("No GBIF taxon found")
        return taxon_id

    def open_gbif_species(self, species_name):
        import webbrowser
        try:
            taxon_id = self._gbif_taxon_id(species_name)
            url = f"https://www.gbif.org/species/{taxon_id}"
        except Exception as exc:
            self._set_status(f"GBIF lookup failed: {exc}", level="warning")
            return
        webbrowser.open(url)

    def _artskart_taxon_id(self, scientific_name):
        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for Artskart lookups.") from exc

        nbic_id = self._nbic_id_from_local(scientific_name)
        if nbic_id:
            try:
                taxon_id = self._taxon_id_from_nbic(nbic_id)
                if taxon_id:
                    return taxon_id
            except Exception:
                pass

        candidates = [
            ("https://artskart.artsdatabanken.no/publicapi/api/taxon/search", {"searchString": scientific_name}),
            ("https://artskart.artsdatabanken.no/publicapi/api/taxon", {"searchString": scientific_name}),
            ("https://artskart.artsdatabanken.no/publicapi/api/taxon/search", {"q": scientific_name}),
            ("https://artskart.artsdatabanken.no/publicapi/api/taxon", {"q": scientific_name}),
        ]

        last_error = None
        for url, params in candidates:
            try:
                response = requests.get(
                    url,
                    params={**params, "pageSize": 1, "page": 1},
                    timeout=20
                )
                if response.status_code == 404:
                    continue
                if response.status_code == 405:
                    continue
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict):
                    for key in ("data", "results", "items", "taxa"):
                        if key in data:
                            data = data[key]
                            break
                if not data:
                    last_error = ValueError("No taxon found")
                    continue
                first = data[0] if isinstance(data, list) else data
                taxon_id = None
                if isinstance(first, dict):
                    taxon_id = (
                        first.get("taxonId")
                        or first.get("taxon_id")
                        or first.get("id")
                        or first.get("TaxonId")
                    )
                if taxon_id:
                    return taxon_id
            except Exception as exc:
                last_error = exc

        if last_error:
            raise last_error
        raise ValueError("No taxon found")

    def _artskart_link(self, taxon_id, lat, lon, zoom=12, bg="nibwmts"):
        from urllib.parse import quote
        import json

        easting, northing = self._utm_from_latlon(lat, lon)
        filt = {
            "TaxonIds": [taxon_id],
            "IncludeSubTaxonIds": True,
            "Found": [2],
            "NotRecovered": [2],
            "Blocked": [2],
            "Style": 1
        }
        filt_s = json.dumps(filt, separators=(",", ":"))
        return (
            f"https://artskart.artsdatabanken.no/app/#map/"
            f"{easting:.0f},{northing:.0f}/{zoom}/background/{bg}/filter/{quote(filt_s)}"
        )

    def _artskart_base_link(self, lat, lon, zoom=12, bg="nibwmts"):
        easting, northing = self._utm_from_latlon(lat, lon)
        return (
            f"https://artskart.artsdatabanken.no/app/#map/"
            f"{easting:.0f},{northing:.0f}/{zoom}/background/{bg}"
        )

    def show_map_service_dialog(self, lat, lon, species_name=None):
        """Show a dialog to choose a map service."""
        tr = lambda text: QCoreApplication.translate("ObservationsTab", text)
        dialog = QDialog(self.parent)
        dialog.setWindowTitle(tr("Open Map"))
        dialog.setModal(True)
        dialog.setMinimumWidth(300)

        layout = QVBoxLayout(dialog)
        layout.setSpacing(4)
        layout.setContentsMargins(16, 16, 16, 12)

        header = QLabel(tr("Choose a map service:"))
        header.setStyleSheet("font-weight: bold; margin-bottom: 4px;")
        layout.addWidget(header)

        species_complete = bool(species_name and len(species_name.split()) >= 2)

        btn_style = (
            "QPushButton#mapLink { text-align: left; padding: 7px 12px;"
            " border: 1px solid #d0d0d0; border-radius: 4px;"
            " background-color: white; color: #2c3e50;"
            f" font-size: {pt(10)}pt; font-weight: normal; }}"
            "QPushButton#mapLink:hover { background-color: #e8f0fe;"
            " border-color: #4a90d9; color: #2c3e50; }"
        )

        def add_link(label_text, description, handler):
            btn = QPushButton(label_text)
            btn.setObjectName("mapLink")
            btn.setStyleSheet(btn_style)
            btn.setCursor(Qt.PointingHandCursor)
            btn.setToolTip(description)
            btn.clicked.connect(handler)
            layout.addWidget(btn)

        def open_url(url):
            import webbrowser
            webbrowser.open(url)
            dialog.accept()

        def open_google_maps():
            open_url(f"https://www.google.com/maps?q={lat},{lon}")

        def open_kilden():
            easting, northing = self._utm_from_latlon(lat, lon)
            url = (
                "https://kilden.nibio.no/?topic=arealinformasjon"
                f"&zoom=14&x={easting:.2f}&y={northing:.2f}&bgLayer=graatone"
            )
            open_url(url)

        def open_norge_i_bilder():
            easting, northing = self._utm_from_latlon(lat, lon)
            url = (
                "https://www.norgeibilder.no/"
                f"?x={easting:.0f}&y={northing:.0f}&level=17&utm=33"
                "&projects=&layers=&plannedOmlop=0&plannedGeovekst=0"
            )
            open_url(url)

        def open_artskart():
            try:
                if species_complete:
                    taxon_id = self._artskart_taxon_id(species_name)
                    url = self._artskart_link(taxon_id, lat, lon, zoom=12, bg="nibwmts")
                else:
                    url = self._artskart_base_link(lat, lon, zoom=12, bg="nibwmts")
            except Exception as exc:
                self._set_status(
                    f"Artskart lookup failed: {exc}. Opening map without species filter.",
                    level="warning",
                )
                url = self._artskart_base_link(lat, lon, zoom=12, bg="nibwmts")
            open_url(url)

        def open_inat_local():
            try:
                self.open_inaturalist_map(lat, lon, species_name)
            finally:
                dialog.accept()

        def open_inat_species():
            try:
                self.open_inaturalist_species(species_name)
            finally:
                dialog.accept()

        def open_gbif_species():
            try:
                self.open_gbif_species(species_name)
            finally:
                dialog.accept()

        add_link("Google Maps", tr("Open location in Google Maps"), open_google_maps)
        add_link("Kilden (NIBIO)", tr("Agricultural & land-use maps"), open_kilden)
        add_link("Artskart", tr("Species occurrence map (Artsdatabanken)"), open_artskart)
        add_link("Norge i Bilder", tr("Aerial imagery of Norway"), open_norge_i_bilder)
        add_link("iNaturalist — nearby observations", tr("Observations near this location"), open_inat_local)
        if species_complete:
            add_link(f"iNaturalist — {species_name}", tr("Species page on iNaturalist"), open_inat_species)
            add_link(f"GBIF — {species_name}", tr("Species page on GBIF"), open_gbif_species)

        layout.addStretch()

        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        dialog.exec()


class ObservationsTab(QWidget):
    """Tab for viewing and managing observations."""

    SETTING_SHOW_TABLE_THUMBNAILS = "observations_table_show_thumbnails"
    SETTING_INCLUDE_ANNOTATIONS = "artsobs_publish_include_annotations"
    SETTING_SHOW_SCALE_BAR = "artsobs_publish_show_scale_bar"
    SETTING_INCLUDE_SPORE_STATS = "artsobs_publish_include_spore_stats"
    SETTING_INCLUDE_MEASURE_PLOTS = "artsobs_publish_include_measure_plots"
    SETTING_INCLUDE_THUMBNAIL_GALLERY = "artsobs_publish_include_thumbnail_gallery"
    SETTING_INCLUDE_COPYRIGHT = "artsobs_publish_include_copyright"
    SETTING_IMAGE_LICENSE = "artsobs_publish_image_license"
    SETTING_MO_APP_API_KEY = "mushroomobserver_app_api_key"
    SETTING_MO_USER_API_KEY = "mushroomobserver_user_api_key"
    ARTSOBS_MEDIA_LICENSE_CODES = {
        "10": "CC BY 4.0",
        "20": "CC BY-SA 4.0",
        "30": "CC BY-NC-SA 4.0",
        "60": "No reuse without permission",
    }

    # Signal emitted when observation is selected (id, display_name, switch_tab)
    observation_selected = Signal(int, str, bool)
    # Signal emitted when a single row is highlighted (id only) — for lightweight header refresh
    observation_highlighted = Signal(int)
    # Signal emitted when an observation is deleted
    observation_deleted = Signal(int)
    # Signal emitted when an image is selected to open in Measure tab
    image_selected = Signal(int, int, str)  # image_id, observation_id, display_name

    def __init__(self, parent=None):
        super().__init__(parent)
        self.selected_observation_id = None
        self._observations_splitter_syncing = False
        self._publish_actions: dict[str, object] = {}
        self._artsobs_dead_by_observation_id: dict[int, bool] = {}
        self._artsobs_public_published_by_observation_id: dict[int, bool] = {}
        self._artsobs_check_thread: ArtsobsMobileLinkCheckWorker | None = None
        self._artsobs_check_failed = False
        self.map_helper = MapServiceHelper(self)
        self._ai_suggestions_cache: dict[int, dict] = {}
        self._observation_edit_draft_cache: dict[int, dict] = {}
        self._accepted_taxon_pair_cache: dict[tuple[str, str], tuple[str, str]] | None = None
        self._publish_login_status_cache: dict[str, bool] | None = None
        self._publish_login_status_cache_ts: float = 0.0
        self._publish_saved_login_status_cache: dict[str, bool] | None = None
        self._publish_saved_login_status_cache_ts: float = 0.0
        self._observation_table_rows_cache: list[dict] = []
        self._observation_thumb_icon_cache: dict[str, QIcon] = {}
        self._search_refresh_timer = QTimer(self)
        self._search_refresh_timer.setSingleShot(True)
        self._search_refresh_timer.setInterval(180)
        self._search_refresh_timer.timeout.connect(self._apply_search_refresh)
        self.setAcceptDrops(True)
        self.init_ui()
        self.refresh_observations()
        # Keyboard shortcut to open Prepare Images directly from the table.
        # Ctrl+E = Cmd+E on macOS; Alt+E works on all platforms.
        for _seq in ("Ctrl+E", "Alt+E"):
            sc = QShortcut(QKeySequence(_seq), self)
            sc.setContext(Qt.WidgetWithChildrenShortcut)
            sc.activated.connect(self.open_edit_images_direct)

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        content_layout = QHBoxLayout()
        content_layout.setSpacing(10)

        left_panel = QWidget()
        left_panel.setMaximumWidth(285)
        left_panel.setMinimumWidth(285)
        left_panel_layout = QVBoxLayout(left_panel)
        left_panel_layout.setContentsMargins(0, 0, 0, 0)
        left_panel_layout.setSpacing(8)

        _icons = Path(__file__).parent.parent / "assets" / "icons"
        _iz = QSize(16, 16)

        def _btn_icon(name: str) -> QIcon:
            return QIcon(str(_icons / name))

        # ── New — full-width primary action ───────────────────────────────
        self.new_btn = QPushButton(self.tr("+ New (N)"))
        self.new_btn.setObjectName("primaryButton")
        self.new_btn.setToolTip(self.tr("Create a new observation"))
        self.new_btn.clicked.connect(self.create_new_observation)
        left_panel_layout.addWidget(self.new_btn)

        # ── Search ────────────────────────────────────────────────────────
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(self.tr("Search observations..."))
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self._on_search_text_changed)
        left_panel_layout.addWidget(self.search_input)

        self.show_table_thumbnails_checkbox = QCheckBox(self.tr("Show thumbnail"))
        self.show_table_thumbnails_checkbox.setChecked(
            bool(SettingsDB.get_setting(self.SETTING_SHOW_TABLE_THUMBNAILS, False))
        )
        self.show_table_thumbnails_checkbox.toggled.connect(
            self._on_show_table_thumbnails_toggled
        )
        left_panel_layout.addWidget(self.show_table_thumbnails_checkbox)

        # ── Edit — frequent, single-selection ─────────────────────────────
        self.rename_btn = QPushButton(self.tr("Edit"))
        self.rename_btn.setObjectName("outlineButton")
        self.rename_btn.setEnabled(False)
        self.rename_btn.setToolTip(self.tr("Edit selected observation (⌘E / double-click)"))
        self.rename_btn.clicked.connect(self.edit_observation)
        left_panel_layout.addWidget(self.rename_btn)

        # ── Plate | Publish — sharing pair ────────────────────────────────
        share_row = QHBoxLayout()
        share_row.setSpacing(5)

        self.plate_btn = QPushButton(self.tr("Plate"))
        self.plate_btn.setObjectName("outlineButton")
        self.plate_btn.setToolTip(self.tr("Generate a species plate for selected observation"))
        self.plate_btn.setEnabled(False)
        self.plate_btn.clicked.connect(self._on_plate_clicked)
        share_row.addWidget(self.plate_btn)

        self.publish_menu = QMenu(self)
        self.publish_btn = QPushButton(self.tr("Publish"))
        self.publish_btn.setObjectName("outlineButton")
        self.publish_btn.setMenu(self.publish_menu)
        self.publish_btn.setEnabled(False)
        self._publish_direct_click_connected = False
        self._build_publish_menu()
        share_row.addWidget(self.publish_btn)

        left_panel_layout.addLayout(share_row)

        # ── Quiet row: Delete + data management ───────────────────────────
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Plain)
        sep.setFixedHeight(1)
        left_panel_layout.addWidget(sep)

        data_row = QHBoxLayout()
        data_row.setSpacing(5)

        self.delete_btn = QPushButton(self.tr("Delete"))
        self.delete_btn.setObjectName("destructiveButton")
        self.delete_btn.setEnabled(False)
        self.delete_btn.setToolTip(self.tr("Delete selected observation(s)"))
        self.delete_btn.clicked.connect(self.delete_selected_observation)
        data_row.addWidget(self.delete_btn)

        data_row.addStretch()

        self.import_btn = QPushButton(self.tr("Import"))
        self.import_btn.setObjectName("dataButton")
        self.import_btn.setToolTip(self.tr("Import observations from zip archive"))
        self.import_btn.clicked.connect(self._on_import_db_clicked)
        data_row.addWidget(self.import_btn)

        self.export_btn = QPushButton(self.tr("Export"))
        self.export_btn.setObjectName("dataButton")
        self.export_btn.setEnabled(False)
        self.export_btn.setToolTip(self.tr("Export selected observations (Ctrl-A for all) to zip archive"))
        self.export_btn.clicked.connect(self._on_export_db_clicked)
        data_row.addWidget(self.export_btn)

        self.refresh_btn = QPushButton(self.tr("Refresh"))
        self.refresh_btn.setObjectName("dataButton")
        self.refresh_btn.setToolTip(self.tr("Refresh database (R)"))
        self.refresh_btn.clicked.connect(self._on_refresh_clicked)
        data_row.addWidget(self.refresh_btn)

        left_panel_layout.addLayout(data_row)

        content_layout.addWidget(left_panel)

        # Splitter for table and detail view
        splitter = QSplitter(Qt.Vertical)
        self.observations_splitter = splitter

        # Observations table
        self.table = QTableWidget()
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            self._observation_first_column_title(),
            self._common_name_column_title(),
            self.tr("Genus"),
            self.tr("Species"),
            self._spore_stats_column_title(),
            self.tr("Date"),
            self.tr("Location"),
            self.tr("Map"),
            self.tr("Publish")
        ])

        # Set column properties
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.Stretch)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeToContents)
        header.setStretchLastSection(False)
        self.table.setColumnWidth(0, 56)   # ID
        self.table.setColumnWidth(1, 115)  # Name
        self.table.setColumnWidth(2, 78)   # Genus
        self.table.setColumnWidth(3, 95)   # Species
        self.table.setColumnWidth(4, 290)  # Spores
        self.table.setColumnWidth(5, 132)  # Date
        self.table.setColumnWidth(6, 170)  # Location
        self.table.setColumnWidth(7, 56)   # Map
        self.table.setColumnWidth(8, 90)   # Publish

        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        # Light selection highlight keeps all text readable.
        self.table.setStyleSheet("""
            QTableWidget::item:selected {
                background-color: #d9e9f8;
                color: #1f2d3d;
            }
            QTableWidget::item:selected:!active {
                background-color: #eaf3ff;
                color: #1f2d3d;
            }
        """)
        self.table.itemSelectionChanged.connect(self.on_selection_changed)
        self.table.itemDoubleClicked.connect(self.on_row_double_clicked)
        self.table.setSortingEnabled(True)
        self._observations_table_default_row_height = self.table.verticalHeader().defaultSectionSize()
        splitter.addWidget(self.table)

        # Detail view (shows selected observation info and images)
        self.detail_widget = QWidget()
        detail_layout = QVBoxLayout(self.detail_widget)
        detail_layout.setContentsMargins(5, 5, 5, 5)
        detail_layout.setSpacing(5)

        # Image gallery (collapsible) in a resizable splitter.
        self.gallery_widget = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=50,
            default_height=180,
            show_publish_checkbox=True,
            publish_checkbox_hint=self.tr("Select image for online publishing"),
        )
        self.gallery_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.gallery_widget.setFixedHeight(190)
        self.gallery_widget.set_multi_select(True)
        self.gallery_widget.imageClicked.connect(self._on_gallery_image_clicked)
        self.gallery_widget.imageDoubleClicked.connect(self._on_gallery_image_double_clicked)
        self.gallery_widget.measureBadgeClicked.connect(self._on_gallery_measure_badge_clicked)
        self.gallery_widget.deleteRequested.connect(self._confirm_delete_image)
        self.gallery_widget.publishSelectionChanged.connect(self._on_gallery_publish_selection_changed)

        detail_layout.addWidget(self.gallery_widget)
        self.detail_widget.setMaximumHeight(self._observations_detail_max_height())

        splitter.addWidget(self.detail_widget)

        splitter.setStretchFactor(0, 2)
        drop_targets = [
            self,
            left_panel,
            splitter,
            self.table,
            self.table.viewport(),
            self.detail_widget,
            self.gallery_widget,
        ]
        drop_targets.extend(self.findChildren(QWidget))
        seen_drop_targets: set[int] = set()
        for drop_target in drop_targets:
            if drop_target is None:
                continue
            marker = id(drop_target)
            if marker in seen_drop_targets:
                continue
            seen_drop_targets.add(marker)
            drop_target.setAcceptDrops(True)
            drop_target.installEventFilter(self)
        splitter.setStretchFactor(1, 3)
        splitter.setSizes([600, 180])
        splitter.splitterMoved.connect(self._on_observations_splitter_moved)

        content_layout.addWidget(splitter, 1)
        layout.addLayout(content_layout, 1)

        self.status_label = HintBar(self)
        layout.addWidget(self.status_label)

        self.status_progress_widget = QWidget(self)
        status_progress_layout = QVBoxLayout(self.status_progress_widget)
        status_progress_layout.setContentsMargins(0, 0, 0, 0)
        status_progress_layout.setSpacing(4)
        status_progress_bar_row = QHBoxLayout()
        status_progress_bar_row.setContentsMargins(0, 0, 0, 0)
        status_progress_bar_row.setSpacing(6)
        self.status_progress_bar = QProgressBar(self.status_progress_widget)
        self.status_progress_bar.setRange(0, 100)
        self.status_progress_bar.setValue(0)
        self.status_progress_bar.setTextVisible(False)
        try:
            self.status_progress_bar.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        except Exception:
            pass
        self.status_progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.status_progress_bar.setFixedHeight(18)
        self.status_progress_pct = QLabel("0%")
        self.status_progress_pct.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.status_progress_pct.setFixedWidth(34)
        self.status_progress_text = QLabel("")
        self.status_progress_text.setWordWrap(True)
        self.status_progress_text.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        style_progress_widgets(
            self.status_progress_bar,
            self.status_progress_text,
            self.status_progress_pct,
        )
        status_progress_bar_row.addWidget(self.status_progress_bar, 1)
        status_progress_bar_row.addWidget(self.status_progress_pct, 0, Qt.AlignRight)
        status_progress_layout.addLayout(status_progress_bar_row)
        status_progress_layout.addWidget(self.status_progress_text)
        self.status_progress_widget.setVisible(False)
        layout.addWidget(self.status_progress_widget)

        left_panel_layout.addStretch()

        self._status_hint_controller = HintStatusController(self.status_label, self)
        self._status_hint_controller.set_hint(self.tr("Ready."))
        for widget, hint, disabled in (
            (getattr(self, "new_btn", None), self.tr("Create a new observation"), None),
            (getattr(self, "rename_btn", None), self.tr("Edit selected observation"), self.tr("Select an observation to edit")),
            (getattr(self, "delete_btn", None), self.tr("Delete selected observation(s)"), self.tr("Select one or more observations to delete")),
            (getattr(self, "refresh_btn", None), self.tr("Refresh database"), None),
            (getattr(self, "export_btn", None), self.tr("Export selected observations (Ctrl-A for all) to zip archive"), self.tr("Select observations to export (Ctrl-A selects all)")),
            (getattr(self, "import_btn", None), self.tr("Import observations from zip archive"), None),
        ):
            if widget is not None:
                self._status_hint_controller.register_widget(widget, hint, disabled_hint=disabled)
        if getattr(self, "publish_btn", None):
            self._status_hint_controller.register_widget(
                self.publish_btn,
                self.tr("Choose a publish target."),
                allow_when_disabled=True,
            )
        self._register_gallery_publish_hint_widgets()
        self._init_shortcuts()

    def _init_shortcuts(self) -> None:
        self._shortcut_refresh = QShortcut(QKeySequence("R"), self)
        self._shortcut_refresh.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_refresh.activated.connect(self._on_refresh_shortcut)

        self._shortcut_new = QShortcut(QKeySequence("N"), self)
        self._shortcut_new.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_new.activated.connect(self._on_new_shortcut)

        self._shortcut_delete = QShortcut(QKeySequence(Qt.Key_Delete), self)
        self._shortcut_delete.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_delete.activated.connect(self._on_delete_shortcut)

        self._shortcut_delete_alt = QShortcut(QKeySequence(Qt.ALT | Qt.Key_D), self)
        self._shortcut_delete_alt.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_delete_alt.activated.connect(self._on_delete_shortcut)

        self._shortcut_delete_cmd = QShortcut(QKeySequence(Qt.CTRL | Qt.Key_D), self)
        self._shortcut_delete_cmd.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_delete_cmd.activated.connect(self._on_delete_shortcut)

        self._shortcut_edit_return = QShortcut(QKeySequence(Qt.Key_Return), self)
        self._shortcut_edit_return.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_edit_return.activated.connect(self._on_edit_shortcut)

        self._shortcut_edit_enter = QShortcut(QKeySequence(Qt.Key_Enter), self)
        self._shortcut_edit_enter.setContext(Qt.WidgetWithChildrenShortcut)
        self._shortcut_edit_enter.activated.connect(self._on_edit_shortcut)

    def _shortcut_blocked_by_text_input(self) -> bool:
        widget = QApplication.focusWidget()
        if widget is None:
            return False
        if isinstance(widget, (QLineEdit, QTextEdit)):
            return True
        if isinstance(widget, QComboBox) and widget.isEditable():
            return True
        parent = widget.parentWidget()
        while parent is not None:
            if isinstance(parent, (QLineEdit, QTextEdit)):
                return True
            if isinstance(parent, QComboBox) and parent.isEditable():
                return True
            parent = parent.parentWidget()
        return False

    def _on_refresh_shortcut(self) -> None:
        if self._shortcut_blocked_by_text_input():
            return
        self._on_refresh_clicked()

    def _on_new_shortcut(self) -> None:
        if self._shortcut_blocked_by_text_input():
            return
        self.create_new_observation()

    def _accept_image_drag(self, event) -> bool:
        if not dropped_image_paths_from_mime_data(event.mimeData()):
            return False
        event.acceptProposedAction()
        return True

    def _handle_new_observation_drop(self, event) -> bool:
        paths = dropped_image_paths_from_mime_data(event.mimeData())
        if not paths:
            return False
        event.acceptProposedAction()
        QTimer.singleShot(
            0,
            lambda dropped_paths=list(paths): self.create_new_observation(
                initial_image_paths=dropped_paths
            ),
        )
        return True

    def dragEnterEvent(self, event) -> None:
        if self._accept_image_drag(event):
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event) -> None:
        if self._accept_image_drag(event):
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event) -> None:
        if self._handle_new_observation_drop(event):
            return
        super().dropEvent(event)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.DragEnter and self._accept_image_drag(event):
            return True
        if event.type() == QEvent.DragMove and self._accept_image_drag(event):
            return True
        if event.type() == QEvent.Drop and self._handle_new_observation_drop(event):
            return True
        return super().eventFilter(obj, event)

    def _on_delete_shortcut(self) -> None:
        if self._shortcut_blocked_by_text_input():
            return
        self.delete_selected_observation()

    def _on_edit_shortcut(self) -> None:
        if self._shortcut_blocked_by_text_input():
            return
        self.edit_observation()

    def _on_search_text_changed(self, _text: str) -> None:
        # Debounce typing so we don't rebuild the table (and reload details) on every keystroke.
        if hasattr(self, "_search_refresh_timer"):
            self._search_refresh_timer.start()
        else:
            self._apply_search_refresh()

    def _apply_search_refresh(self) -> None:
        # Keep search responsive: do not auto-restore selection/detail panes while typing.
        if hasattr(self, "_observation_table_rows_cache"):
            self._render_observations_table(
                self._observation_table_rows_cache,
                query=self.search_input.text().strip().lower() if hasattr(self, "search_input") else "",
                restore_selection=False,
                show_status=False,
                status_message=None,
            )
            return
        self.refresh_observations(show_status=False, restore_selection=False)

    def set_status_message(
        self,
        message: str,
        level: str = "info",
        auto_clear_ms: int = 8000,
    ) -> None:
        text = (message or "").strip()
        if hasattr(self, "_status_hint_controller") and self._status_hint_controller is not None:
            self._status_hint_controller.set_status(
                text,
                timeout_ms=auto_clear_ms,
                tone=level,
            )
            return
        self.status_label.setText(text)

    def _set_status_progress_visible(self, visible: bool) -> None:
        visible = bool(visible)
        if hasattr(self, "status_label"):
            self.status_label.setVisible(not visible)
        if hasattr(self, "status_progress_widget"):
            self.status_progress_widget.setVisible(visible)

    def _set_status_progress(
        self,
        status_text: str | None,
        current: int | None = None,
        total: int | None = None,
    ) -> None:
        if hasattr(self, "status_progress_text"):
            self.status_progress_text.setText((status_text or "").strip())
        if hasattr(self, "status_progress_bar"):
            if total is not None and total > 0:
                try:
                    total_i = max(1, int(total))
                    current_i = 0 if current is None else max(0, min(int(current), total_i))
                    pct = int(round((current_i / total_i) * 100.0))
                    self.status_progress_bar.setRange(0, 100)
                    pct = max(0, min(100, pct))
                    self.status_progress_bar.setValue(pct)
                    if hasattr(self, "status_progress_pct"):
                        self.status_progress_pct.setText(f"{pct}%")
                except Exception:
                    pass
            elif current is not None:
                try:
                    self.status_progress_bar.setRange(0, 100)
                    pct = max(0, min(100, int(current)))
                    self.status_progress_bar.setValue(pct)
                    if hasattr(self, "status_progress_pct"):
                        self.status_progress_pct.setText(f"{pct}%")
                except Exception:
                    pass

    def _call_main_window_db_action(self, method_name: str) -> None:
        main_window = self.window()
        action = getattr(main_window, method_name, None)
        if callable(action):
            action()
            return
        self.set_status_message(
            self.tr("Database action unavailable."),
            level="warning",
            auto_clear_ms=8000,
        )

    def _on_export_db_clicked(self) -> None:
        self._call_main_window_db_action("export_database_bundle")

    def _on_import_db_clicked(self) -> None:
        self._call_main_window_db_action("import_database_bundle")

    def _on_refresh_clicked(self) -> None:
        self._invalidate_publish_login_status_cache()
        self.refresh_observations(show_status=False)
        pending_status = self._upload_pending_artsobs_web_images()
        # Public web link checks do not require Artsobs mobile login.
        # Keep any pending-upload warning/success message unless there was nothing pending.
        if pending_status == "none":
            self.set_status_message(self.tr("Checking links."), level="info", auto_clear_ms=0)
        self._start_artsobs_link_check()

    def _upload_pending_artsobs_web_images(self) -> str:
        try:
            pending_rows = ImageDB.get_pending_artsobs_web_uploads()
        except Exception as exc:
            self.set_status_message(
                self.tr("Could not check pending image uploads: {error}").format(error=exc),
                level="warning",
                auto_clear_ms=12000,
            )
            return "failed"

        if not pending_rows:
            return "none"

        total_pending = len(pending_rows)
        if total_pending == 1:
            uploading_msg = self.tr("1 image added to a published observation. Uploading...")
            login_msg = self.tr(
                "1 image added to a published observation. Log in to Artsobservasjoner (web), then click Refresh db."
            )
        else:
            uploading_msg = self.tr(
                "{count} images added to published observations. Uploading..."
            ).format(count=total_pending)
            login_msg = self.tr(
                "{count} images added to published observations. Log in to Artsobservasjoner (web), then click Refresh db."
            ).format(count=total_pending)

        try:
            from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth
            from utils.artsobservasjoner_submit import ArtsObservasjonerWebClient
        except Exception as exc:
            self.set_status_message(
                self.tr("Upload unavailable: {error}").format(error=exc),
                level="warning",
                auto_clear_ms=12000,
            )
            return "failed"

        auth = ArtsObservasjonerAuth()
        web_cookies = auth.get_valid_cookies(target="web")
        if not web_cookies:
            self.set_status_message(login_msg, level="warning", auto_clear_ms=15000)
            return "no_cookie"

        grouped: dict[int, dict] = {}
        for row in pending_rows:
            try:
                obs_id = int(row.get("observation_id"))
                sighting_id = int(row.get("artsdata_id"))
                image_id = int(row.get("image_id"))
            except (TypeError, ValueError):
                continue
            path = row.get("filepath") or row.get("original_filepath")
            entry = grouped.setdefault(
                obs_id,
                {"sighting_id": sighting_id, "paths": [], "image_ids": []},
            )
            if path and Path(path).exists():
                entry["paths"].append(path)
                entry["image_ids"].append(image_id)

        if not grouped:
            return "none"

        self.set_status_message(uploading_msg, level="info", auto_clear_ms=0)
        QApplication.processEvents()

        client = ArtsObservasjonerWebClient()
        client.set_cookies_from_browser(web_cookies)

        uploaded_image_ids: list[int] = []
        errors: list[str] = []
        attempted = 0
        for data in grouped.values():
            sighting_id = data.get("sighting_id")
            paths = data.get("paths") or []
            image_ids = data.get("image_ids") or []
            if not sighting_id or not paths:
                continue
            attempted += len(paths)
            try:
                client.upload_images_web(
                    sighting_id=int(sighting_id),
                    image_paths=paths,
                    progress_cb=None,
                )
                uploaded_image_ids.extend(image_ids)
            except Exception as exc:
                errors.append(str(exc))

        if uploaded_image_ids:
            try:
                ImageDB.mark_images_artsobs_web_uploaded(uploaded_image_ids)
            except Exception:
                pass

        uploaded_count = len(uploaded_image_ids)
        if uploaded_count > 0 and not errors:
            self.set_status_message(
                self.tr("Uploaded {count} pending image(s) to Artsobservasjoner (web).").format(
                    count=uploaded_count
                ),
                level="success",
                auto_clear_ms=10000,
            )
            return "uploaded"

        if uploaded_count > 0 and errors:
            self.set_status_message(
                self.tr("Uploaded {ok}/{total} pending image(s). Some uploads failed: {error}").format(
                    ok=uploaded_count,
                    total=max(total_pending, attempted),
                    error=errors[0],
                ),
                level="warning",
                auto_clear_ms=15000,
            )
            return "partial"

        if errors:
            self.set_status_message(
                self.tr("Pending image upload failed: {error}").format(error=errors[0]),
                level="warning",
                auto_clear_ms=15000,
            )
        return "failed"

    def _find_table_row_for_observation(self, observation_id: int) -> int:
        for row in range(self.table.rowCount()):
            row_id = self._observation_id_for_row(row)
            if row_id is None:
                continue
            if row_id == observation_id:
                return row
        return -1

    def _pending_published_image_upload_notice_for_observation(self, observation_id: int) -> tuple[str, str] | None:
        """Return a reminder message for pending Artsobservasjoner web image uploads."""
        try:
            pending_count = int(ImageDB.get_pending_artsobs_web_upload_count_for_observation(observation_id))
        except Exception:
            pending_count = 0
        if pending_count <= 0:
            return None

        if pending_count == 1:
            return (
                self.tr(
                    "Image added to published observation. Click Refresh db to upload the new image. If needed, log in to Artsobservasjoner (web) first."
                ),
                "warning",
            )

        return (
            self.tr(
                "{count} images added to published observations. Click Refresh db to upload the new images. If needed, log in to Artsobservasjoner (web) first."
            ).format(
                count=pending_count
            ),
            "warning",
        )

    def _render_publish_cell(
        self,
        row: int,
        observation_id: int,
        publish_target: str | None,
        arts_id: int | None,
        artportalen_id: int | None,
    ) -> None:
        self.table.removeCellWidget(row, 8)
        normalized_target = normalize_publish_target(publish_target)
        service_id = artportalen_id if normalized_target == PUBLISH_TARGET_ARTPORTALEN_SE else arts_id
        has_id = bool(service_id)
        arts_item = SortableTableWidgetItem("" if has_id else "-")
        arts_item.setData(Qt.UserRole, int(service_id or 0))
        arts_item.setFlags(arts_item.flags() & ~Qt.ItemIsEditable)
        self.table.setItem(row, 8, arts_item)
        if not has_id:
            return

        if normalized_target == PUBLISH_TARGET_ARTPORTALEN_SE:
            artportalen_url = f"https://www.artportalen.se/Sighting/{int(service_id)}"
            label_html = f'<a href="{artportalen_url}">AP</a>'
            link_tooltip = self.tr("AP: Artportalen web")
            arts_label = QLabel(label_html)
            arts_label.setTextFormat(Qt.RichText)
            arts_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
            arts_label.setOpenExternalLinks(False)
            arts_label.setAlignment(Qt.AlignCenter)
            arts_label.linkActivated.connect(lambda url: QDesktopServices.openUrl(QUrl(url)))
            self._status_hint_controller.register_widget(arts_label, link_tooltip)
            arts_label.setToolTip(link_tooltip)
            self.table.setCellWidget(row, 8, arts_label)
            return

        if self._artsobs_dead_by_observation_id.get(observation_id):
            warn_label = QLabel("\u25B2")
            warn_label.setAlignment(Qt.AlignCenter)
            warn_label.setStyleSheet("color: #c0392b; font-weight: bold;")
            warn_label.setToolTip(self.tr("Can't find this observation"))
            self.table.setCellWidget(row, 8, warn_label)
            return

        mao_url = f"https://mobil.artsobservasjoner.no/sighting/{int(service_id)}"
        wao_url = f"https://www.artsobservasjoner.no/Sighting/{int(service_id)}"
        is_publicly_published = self._artsobs_public_published_by_observation_id.get(observation_id)
        if is_publicly_published is True:
            label_html = f'<a href="{mao_url}">MAo</a> | <a href="{wao_url}">Ao</a>'
            link_tooltip = self.tr("MAo: Artsobservasjoner mobile app · Ao: Artsobservasjoner web")
        else:
            label_html = f'<a href="{mao_url}">MAo</a>'
            link_tooltip = (
                self.tr("Ao link shown only after the observation is publicly published.")
                if is_publicly_published is False
                else self.tr("MAo: Artsobservasjoner mobile app. Ao link appears after link check.")
            )
        arts_label = QLabel(label_html)
        arts_label.setTextFormat(Qt.RichText)
        arts_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        arts_label.setOpenExternalLinks(False)
        arts_label.setAlignment(Qt.AlignCenter)
        arts_label.linkActivated.connect(
            lambda url: QDesktopServices.openUrl(QUrl(url))
        )
        self._status_hint_controller.register_widget(arts_label, link_tooltip)
        arts_label.setToolTip(link_tooltip)
        self.table.setCellWidget(row, 8, arts_label)

    def _start_artsobs_link_check(self) -> None:
        try:
            observations = ObservationDB.get_all_observations()
            checks: list[tuple[int, int]] = []
            for obs in observations:
                obs_id = obs.get("id")
                arts_id = obs.get("artsdata_id")
                try:
                    obs_id_int = int(obs_id)
                    arts_id_int = int(arts_id or 0)
                except (TypeError, ValueError):
                    continue
                if arts_id_int > 0:
                    checks.append((obs_id_int, arts_id_int))
        except Exception as exc:
            self.set_status_message(
                self.tr("Checking links failed: {error}").format(error=exc),
                level="warning",
                auto_clear_ms=12000,
            )
            return

        if not checks:
            self.set_status_message(self.tr("No Artsobs links to check."), level="info", auto_clear_ms=6000)
            return

        if self._artsobs_check_thread and self._artsobs_check_thread.isRunning():
            self._artsobs_check_thread.requestInterruption()

        worker = ArtsobsMobileLinkCheckWorker(checks, parent=self)
        self._artsobs_check_thread = worker
        self._artsobs_check_failed = False
        worker.linkChecked.connect(self._on_artsobs_link_checked)
        worker.checkFailed.connect(self._on_artsobs_check_failed)
        worker.checkFinished.connect(self._on_artsobs_check_finished)
        worker.finished.connect(lambda: self._on_artsobs_check_thread_finished(worker))
        worker.start()

    def _on_artsobs_check_thread_finished(self, worker: ArtsobsMobileLinkCheckWorker) -> None:
        if self._artsobs_check_thread is worker:
            self._artsobs_check_thread = None
        worker.deleteLater()

    def _on_artsobs_link_checked(self, observation_id: int, is_dead: bool, is_publicly_published: bool) -> None:
        self._artsobs_dead_by_observation_id[observation_id] = bool(is_dead)
        self._artsobs_public_published_by_observation_id[observation_id] = bool(is_publicly_published) and not bool(is_dead)
        row = self._find_table_row_for_observation(observation_id)
        if row < 0:
            return
        arts_item = self.table.item(row, 8)
        if not arts_item:
            return
        value = arts_item.data(Qt.UserRole)
        try:
            arts_id = int(value or 0)
        except (TypeError, ValueError):
            arts_id = 0
        if arts_id <= 0:
            return
        obs = ObservationDB.get_observation(observation_id)
        self._render_publish_cell(
            row,
            observation_id,
            obs.get("publish_target") if obs else None,
            obs.get("artsdata_id") if obs else arts_id,
            obs.get("artportalen_id") if obs else None,
        )
        self._update_publish_controls()

    def _on_artsobs_check_failed(self, message: str) -> None:
        self._artsobs_check_failed = True
        self.set_status_message(self.tr(message), level="warning", auto_clear_ms=12000)

    def _on_artsobs_check_finished(self, checked: int, alive: int, dead: int) -> None:
        self._update_publish_controls()
        if self._artsobs_check_failed:
            return
        if checked <= 0:
            self.set_status_message(self.tr("Checking links finished."), level="info", auto_clear_ms=6000)
            return
        if dead > 0:
            cleared_count = self._prompt_clear_dead_artsobs_links()
            if cleared_count > 0:
                self.set_status_message(
                    self.tr(
                        "Checking links finished. Cleared Artsobs ID for {count} dead link(s)."
                    ).format(count=cleared_count),
                    level="warning",
                    auto_clear_ms=12000,
                )
            else:
                self.set_status_message(
                    self.tr("Checking links finished. Missing: {dead} of {total}.").format(
                        dead=dead,
                        total=checked,
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            return
        self.set_status_message(
            self.tr("Checking links finished. All {total} links found.").format(total=checked),
            level="success",
            auto_clear_ms=10000,
        )

    def _prompt_clear_dead_artsobs_links(self) -> int:
        dead_ids = self._collect_dead_artsobs_observation_ids()
        if not dead_ids:
            return 0
        if not ask_wrapped_yes_no(
            self,
            self.tr("Dead Artsobs links"),
            (
                f"{self.tr('Delete dead links?')}\n\n"
                f"{self.tr('OK will clear the Artsobs ID for dead links.')}"
            ),
            default_yes=True,
            yes_text=self.tr("OK"),
            no_text=self.tr("Cancel"),
        ):
            return 0

        cleared = 0
        for observation_id in dead_ids:
            try:
                ObservationDB.clear_artsdata_id(observation_id)
                self._artsobs_dead_by_observation_id[observation_id] = False
                self._artsobs_public_published_by_observation_id.pop(observation_id, None)
                cleared += 1
            except Exception:
                continue

        if cleared > 0:
            self.refresh_observations(show_status=False)
            self._update_publish_controls()
        return cleared

    def _collect_dead_artsobs_observation_ids(self) -> list[int]:
        dead_ids: list[int] = []
        try:
            observations = ObservationDB.get_all_observations()
        except Exception:
            return dead_ids
        for obs in observations:
            try:
                obs_id = int(obs.get("id"))
                arts_id = int(obs.get("artsdata_id") or 0)
            except (TypeError, ValueError):
                continue
            if arts_id > 0 and self._artsobs_dead_by_observation_id.get(obs_id):
                dead_ids.append(obs_id)
        return dead_ids

    def _build_publish_menu(self) -> None:
        self.publish_menu.clear()
        self._publish_actions = {}
        self._publish_direct_target_key = None
        self._disconnect_publish_click_if_needed()
        try:
            from utils.artsobs_uploaders import list_uploaders
            uploaders = list_uploaders()
        except Exception as exc:
            action = self.publish_menu.addAction(self.tr("Upload unavailable"))
            action.setEnabled(False)
            self.publish_btn.setProperty("_hint_text", self.tr("Upload helpers unavailable: {error}").format(error=exc))
            return

        if not uploaders:
            action = self.publish_menu.addAction(self.tr("No publish targets configured"))
            action.setEnabled(False)
            return

        enabled_keys = self._enabled_publish_uploader_keys(uploaders)
        enabled_uploaders = [uploader for uploader in uploaders if uploader.key in enabled_keys]
        self._publish_enabled_keys = [uploader.key for uploader in enabled_uploaders]

        if not enabled_uploaders:
            action = self.publish_menu.addAction(self.tr("No publish targets enabled"))
            action.setEnabled(False)
            self.publish_btn.setMenu(self.publish_menu)
            return

        if len(enabled_uploaders) == 1:
            uploader = enabled_uploaders[0]
            self._publish_direct_target_key = uploader.key
            self.publish_btn.setMenu(None)
            self.publish_btn.clicked.connect(
                lambda _checked=False, key=uploader.key: self._publish_selected_observations(key)
            )
            self._publish_direct_click_connected = True
            self.publish_btn.setText(self.tr("Publish"))
            self.publish_btn.setProperty(
                "_hint_text",
                self.tr("Publish directly to {target}.").format(target=self.tr(uploader.label)),
            )
            return

        self.publish_btn.setMenu(self.publish_menu)
        for uploader in enabled_uploaders:
            action = self.publish_menu.addAction(self.tr(uploader.label))
            action.triggered.connect(
                lambda _checked=False, key=uploader.key: self._publish_selected_observations(key)
            )
            self._publish_actions[uploader.key] = action

    def _disconnect_publish_click_if_needed(self) -> None:
        if not getattr(self, "_publish_direct_click_connected", False):
            return
        try:
            self.publish_btn.clicked.disconnect()
        except (RuntimeError, TypeError):
            pass
        self._publish_direct_click_connected = False

    def _enabled_publish_uploader_keys(self, uploaders: list | None = None) -> list[str]:
        try:
            from ui.main_window import ArtsobservasjonerSettingsDialog

            return ArtsobservasjonerSettingsDialog.enabled_upload_target_keys(uploaders)
        except Exception:
            if uploaders is None:
                return []
            return [str(getattr(uploader, "key", "")).strip().lower() for uploader in uploaders if getattr(uploader, "key", None)]

    def _uploader_label(self, uploader_key: str) -> str:
        key = str(uploader_key or "").strip().lower()
        if not key:
            return self.tr("selected service")
        action = self._publish_actions.get(key)
        if action is not None and action.text():
            return action.text()
        try:
            from utils.artsobs_uploaders import get_uploader

            uploader = get_uploader(key)
            if uploader is not None:
                return self.tr(uploader.label)
        except Exception:
            pass
        return key

    def _enabled_uploader_labels(self, enabled_keys: list[str]) -> list[str]:
        labels: list[str] = []
        for key in enabled_keys:
            label = self._uploader_label(key)
            if label not in labels:
                labels.append(label)
        return labels

    def _active_reporting_target(self) -> str:
        return normalize_publish_target(
            SettingsDB.get_setting(SETTING_ACTIVE_REPORTING_TARGET, PUBLISH_TARGET_ARTSOBS_NO),
            fallback=PUBLISH_TARGET_ARTSOBS_NO,
        )

    def _refresh_publish_targets_if_needed(self) -> None:
        try:
            from utils.artsobs_uploaders import list_uploaders

            uploaders = list_uploaders()
        except Exception:
            uploaders = []
        enabled_keys = tuple(self._enabled_publish_uploader_keys(uploaders))
        current_keys = tuple(getattr(self, "_publish_enabled_keys", []) or [])
        if enabled_keys != current_keys:
            self._build_publish_menu()

    def _selected_observation_ids(self) -> list[int]:
        selection_model = self.table.selectionModel()
        if not selection_model:
            return []
        rows = sorted(selection_model.selectedRows(), key=lambda index: index.row())
        observation_ids: list[int] = []
        for index in rows:
            obs_id = self._observation_id_for_row(index.row())
            if obs_id is None:
                continue
            observation_ids.append(obs_id)
        return observation_ids

    @staticmethod
    def _observation_id_from_item(item: QTableWidgetItem | None) -> int | None:
        if item is None:
            return None
        value = item.data(Qt.UserRole)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass
        text = str(item.text() or "").strip()
        if not text:
            return None
        try:
            return int(text)
        except (TypeError, ValueError):
            return None

    def _observation_id_for_row(self, row: int) -> int | None:
        if row < 0 or not hasattr(self, "table"):
            return None
        return self._observation_id_from_item(self.table.item(row, 0))

    def _observation_publish_target(self, obs: dict | None) -> str:
        return resolve_observation_publish_target(obs, default_target=PUBLISH_TARGET_ARTSOBS_NO)

    def _uploader_matches_publish_target(self, uploader_key: str, publish_target: str) -> bool:
        key = (uploader_key or "").strip().lower()
        target = normalize_publish_target(publish_target)
        if key == "artportalen":
            return target == PUBLISH_TARGET_ARTPORTALEN_SE
        if key in {"mobile", "web"}:
            return target == PUBLISH_TARGET_ARTSOBS_NO
        return True

    def _observation_has_existing_upload(self, obs: dict | None, uploader_key: str) -> bool:
        observation = obs or {}
        key = (uploader_key or "").strip().lower()
        try:
            if key == "artportalen":
                return int(observation.get("artportalen_id") or 0) > 0
            if key in {"mobile", "web"}:
                obs_id = int(observation.get("id") or 0)
                arts_id = int(observation.get("artsdata_id") or 0)
                if arts_id <= 0:
                    return False
                if obs_id > 0 and self._artsobs_dead_by_observation_id.get(obs_id):
                    return False
                return True
        except (TypeError, ValueError):
            return False
        return False

    def _selection_has_uploaded_artsobs(self) -> bool:
        selection_model = self.table.selectionModel()
        if not selection_model:
            return False
        for index in selection_model.selectedRows():
            observation_id = self._observation_id_for_row(index.row())
            arts_item = self.table.item(index.row(), 8)
            if not arts_item:
                continue
            value = arts_item.data(Qt.UserRole)
            try:
                arts_id = int(value or 0)
                if arts_id <= 0:
                    continue
                # Dead links are treated as re-publishable.
                if observation_id is not None and self._artsobs_dead_by_observation_id.get(observation_id):
                    continue
                if arts_id > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def _target_for_uploader_key(self, uploader_key: str | None) -> str | None:
        key = (uploader_key or "").strip().lower()
        if key == "artportalen":
            return PUBLISH_TARGET_ARTPORTALEN_SE
        if key in {"mobile", "web"}:
            return PUBLISH_TARGET_ARTSOBS_NO
        return None

    def _ensure_selection_publish_target(self, uploader_key: str, observation_ids: list[int]) -> bool:
        target = self._target_for_uploader_key(uploader_key)
        if target is None or not observation_ids:
            return True

        mismatched_ids: list[int] = []
        for observation_id in observation_ids:
            obs = ObservationDB.get_observation(observation_id)
            if not obs:
                continue
            current_target = self._observation_publish_target(obs)
            if normalize_publish_target(current_target) != normalize_publish_target(target):
                mismatched_ids.append(observation_id)

        if not mismatched_ids:
            return True

        target_label = self.tr(publish_target_label(target))
        confirmed = ask_wrapped_yes_no(
            self,
            self.tr("Switch Reporting System"),
            self.tr(
                "Publishing to {target} will switch {count} selected observation(s) to that reporting system.\n\n"
                "This will affect biotope/substrate choices when you edit them.\n\n"
                "Continue?"
            ).format(target=target_label, count=len(mismatched_ids)),
            default_yes=False,
        )
        if not confirmed:
            return False

        for observation_id in mismatched_ids:
            try:
                ObservationDB.update_observation(
                    observation_id,
                    publish_target=target,
                )
            except Exception:
                return False

        return True

    def _selection_matches_uploader_target(self, uploader_key: str) -> bool:
        observation_ids = self._selected_observation_ids()
        if not observation_ids:
            return False
        for observation_id in observation_ids:
            obs = ObservationDB.get_observation(observation_id)
            if not obs:
                return False
            if not self._uploader_matches_publish_target(uploader_key, self._observation_publish_target(obs)):
                return False
        return True

    def _selection_has_existing_upload_for_uploader(self, uploader_key: str) -> bool:
        observation_ids = self._selected_observation_ids()
        if not observation_ids:
            return False
        for observation_id in observation_ids:
            obs = ObservationDB.get_observation(observation_id)
            if obs and self._observation_has_existing_upload(obs, uploader_key):
                return True
        return False

    def _publish_target_logged_in(self, uploader_key: str) -> bool:
        key = (uploader_key or "").strip().lower()
        if key in {"mobile", "web"}:
            try:
                from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth

                cookies = ArtsObservasjonerAuth().load_cookies(target="web") or {}
            except Exception:
                return False
            if not cookies:
                return False
            return bool(str(cookies.get(".ASPXAUTHNO") or "").strip())

        if key == "artportalen":
            try:
                from utils.artportalen_auth import ArtportalenAuth

                cookies = ArtportalenAuth().load_cookies() or {}
            except Exception:
                return False
            if not cookies:
                return False
            return bool(str(cookies.get(".ASPXAUTH") or "").strip())

        if key == "inat":
            client_id = (SettingsDB.get_setting("inat_client_id", "") or "").strip() or (
                os.getenv("INAT_CLIENT_ID", "") or ""
            ).strip()
            client_secret = (SettingsDB.get_setting("inat_client_secret", "") or "").strip() or (
                os.getenv("INAT_CLIENT_SECRET", "") or ""
            ).strip()
            redirect_uri = (
                SettingsDB.get_setting("inat_redirect_uri", "http://localhost:8000/callback")
                or "http://localhost:8000/callback"
            )
            if not client_id or not client_secret:
                return False
            try:
                from utils.inat_oauth import INatOAuthClient

                token_file = (
                    app_data_dir() / "inaturalist_oauth_tokens.json"
                )
                oauth = INatOAuthClient(
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    token_file=token_file,
                )
                return bool(oauth.is_logged_in())
            except Exception:
                return False

        if key == "mo":
            app_key = (SettingsDB.get_setting(self.SETTING_MO_APP_API_KEY, "") or "").strip() or (
                os.getenv("MO_APP_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_APP_API_KEY", "") or ""
            ).strip()
            user_key = (SettingsDB.get_setting(self.SETTING_MO_USER_API_KEY, "") or "").strip() or (
                os.getenv("MO_USER_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_USER_API_KEY", "") or ""
            ).strip()
            return bool(app_key and user_key)

        return True

    def _invalidate_publish_login_status_cache(self) -> None:
        self._publish_login_status_cache = None
        self._publish_login_status_cache_ts = 0.0
        self._publish_saved_login_status_cache = None
        self._publish_saved_login_status_cache_ts = 0.0

    def _publish_target_login_status(self, force_refresh: bool = False) -> dict[str, bool]:
        # Avoid disk reads (cookie JSON) and token checks on every row click.
        now = time.perf_counter()
        ttl_seconds = 10.0
        cache_valid = (
            not force_refresh
            and isinstance(self._publish_login_status_cache, dict)
            and (now - float(self._publish_login_status_cache_ts)) < ttl_seconds
        )
        if cache_valid:
            return dict(self._publish_login_status_cache or {})

        status = {
            key: self._publish_target_logged_in(key)
            for key in (getattr(self, "_publish_enabled_keys", []) or self._publish_actions.keys())
        }
        self._publish_login_status_cache = dict(status)
        self._publish_login_status_cache_ts = now
        return status

    def _publish_target_has_saved_login(self, uploader_key: str) -> bool:
        key = (uploader_key or "").strip().lower()
        if key in {"mobile", "web"}:
            try:
                from utils.artsobservasjoner_auto_login import has_saved_web_login

                return bool(has_saved_web_login())
            except Exception:
                return False

        if key == "artportalen":
            try:
                from utils.artportalen_auth import has_saved_login

                return bool(has_saved_login())
            except Exception:
                return False

        if key == "inat":
            client_id = (SettingsDB.get_setting("inat_client_id", "") or "").strip() or (
                os.getenv("INAT_CLIENT_ID", "") or ""
            ).strip()
            client_secret = (SettingsDB.get_setting("inat_client_secret", "") or "").strip() or (
                os.getenv("INAT_CLIENT_SECRET", "") or ""
            ).strip()
            redirect_uri = (
                SettingsDB.get_setting("inat_redirect_uri", "http://localhost:8000/callback")
                or "http://localhost:8000/callback"
            )
            if not client_id or not client_secret:
                return False
            try:
                from utils.inat_oauth import INatOAuthClient

                token_file = (
                    app_data_dir() / "inaturalist_oauth_tokens.json"
                )
                oauth = INatOAuthClient(
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    token_file=token_file,
                )
                return bool(oauth.is_logged_in())
            except Exception:
                return False

        if key == "mo":
            app_key = (SettingsDB.get_setting(self.SETTING_MO_APP_API_KEY, "") or "").strip() or (
                os.getenv("MO_APP_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_APP_API_KEY", "") or ""
            ).strip()
            user_key = (SettingsDB.get_setting(self.SETTING_MO_USER_API_KEY, "") or "").strip() or (
                os.getenv("MO_USER_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_USER_API_KEY", "") or ""
            ).strip()
            return bool(app_key and user_key)

        return False

    def _publish_target_saved_login_status(self, force_refresh: bool = False) -> dict[str, bool]:
        now = time.perf_counter()
        ttl_seconds = 10.0
        cache_valid = (
            not force_refresh
            and isinstance(self._publish_saved_login_status_cache, dict)
            and (now - float(self._publish_saved_login_status_cache_ts)) < ttl_seconds
        )
        if cache_valid:
            return dict(self._publish_saved_login_status_cache or {})

        status = {
            key: self._publish_target_has_saved_login(key)
            for key in (getattr(self, "_publish_enabled_keys", []) or self._publish_actions.keys())
        }
        self._publish_saved_login_status_cache = dict(status)
        self._publish_saved_login_status_cache_ts = now
        return status

    def _open_online_publishing_settings(self) -> bool:
        window = self.window()
        if window is not None and hasattr(window, "open_artsobservasjoner_settings_dialog"):
            try:
                window.open_artsobservasjoner_settings_dialog()
                return True
            except Exception:
                pass
        return False

    def _update_publish_controls(self) -> None:
        if not hasattr(self, "publish_btn"):
            return
        self._refresh_publish_targets_if_needed()

        observation_ids = self._selected_observation_ids()
        has_selection = bool(observation_ids)
        if not has_selection:
            for action in self._publish_actions.values():
                action.setEnabled(False)
            self.publish_btn.setEnabled(False)
            self.publish_btn.setProperty("_hint_text", self.tr("Select one or more observations to publish."))
            if hasattr(self, "plate_btn"):
                self.plate_btn.setEnabled(False)
            return

        enabled_keys = list(getattr(self, "_publish_enabled_keys", []) or [])
        any_target_enabled = False
        for key in enabled_keys:
            has_existing_upload = self._selection_has_existing_upload_for_uploader(key)
            enabled = has_selection and not has_existing_upload
            action = self._publish_actions.get(key)
            if action is not None:
                action.setEnabled(enabled)
            if enabled:
                any_target_enabled = True

        self.publish_btn.setEnabled(has_selection and any_target_enabled)
        if len(enabled_keys) > 1:
            self._disconnect_publish_click_if_needed()
            self.publish_btn.setMenu(self.publish_menu)

        # Plate button: enabled whenever exactly one observation is selected
        if hasattr(self, "plate_btn"):
            self.plate_btn.setEnabled(len(observation_ids) == 1)

        if not has_selection:
            self.publish_btn.setProperty("_hint_text", self.tr("Select one or more observations to publish."))
        elif any(self._selection_has_existing_upload_for_uploader(key) for key in enabled_keys):
            enabled_labels = ", ".join(self._enabled_uploader_labels(enabled_keys))
            self.publish_btn.setProperty(
                "_hint_text",
                self.tr("Publishing to {targets} is disabled for observations that already have an ID in that service.").format(
                    targets=enabled_labels or self.tr("the selected service")
                ),
            )
        elif not any(self._selection_matches_uploader_target(key) for key in enabled_keys):
            enabled_labels = ", ".join(self._enabled_uploader_labels(enabled_keys))
            self.publish_btn.setProperty(
                "_hint_text",
                self.tr("Publishing to {targets} will switch the selected observations to that reporting system.").format(
                    targets=enabled_labels or self.tr("the selected service")
                ),
            )
        elif len(enabled_keys) == 1:
            target_key = enabled_keys[0]
            target_label = self._uploader_label(target_key)
            self.publish_btn.setProperty(
                "_hint_text",
                self.tr(
                    "Publish directly to {target}. Saved login will be used automatically if available; otherwise Publish opens Online publishing."
                ).format(target=target_label),
            )
        else:
            enabled_labels = ", ".join(self._enabled_uploader_labels(enabled_keys))
            self.publish_btn.setProperty(
                "_hint_text",
                self.tr(
                    "Choose where to publish: {targets}. Saved logins will be used automatically when available."
                ).format(
                    targets=enabled_labels or self.tr("available services")
                ),
            )

    def _on_plate_clicked(self) -> None:
        obs_ids = self._selected_observation_ids()
        if len(obs_ids) != 1:
            self.set_status_message(self.tr("Select a single observation to generate a plate."), level="warning")
            return
        obs_id = obs_ids[0]
        obs = ObservationDB.get_observation(obs_id)
        if obs is None:
            self.set_status_message(self.tr("Could not load observation."), level="error")
            return
        # Inject displayed vernacular name (may come from lookup table, not just DB column)
        if not obs.get("common_name"):
            name_map = self._build_common_name_map([obs])
            vernacular = self._lookup_common_name(obs, name_map)
            if vernacular:
                obs = dict(obs)
                obs["common_name"] = vernacular
        excluded = self._publish_excluded_image_ids(obs_id)
        from ui.species_plate_dialog import SpeciesPlateDialog
        dlg = SpeciesPlateDialog(obs, excluded_image_ids=excluded, parent=self)
        dlg.exec()

    def _publish_selected_observations(self, uploader_key: str) -> None:
        self._invalidate_publish_login_status_cache()
        observation_ids = self._selected_observation_ids()
        if not observation_ids:
            self.set_status_message(
                self.tr("Select one or more observations to publish."),
                level="warning",
            )
            return
        login_status = self._publish_target_login_status(force_refresh=True)
        saved_login_status = self._publish_target_saved_login_status(force_refresh=True)
        if not login_status.get(uploader_key, False) and not saved_login_status.get(uploader_key, False):
            opened = self._open_online_publishing_settings()
            if not opened:
                self.set_status_message(
                    self.tr("Open Online publishing and log in before publishing."),
                    level="warning",
                    auto_clear_ms=12000,
                )
            self._invalidate_publish_login_status_cache()
            self._update_publish_controls()
            return
        if not self._ensure_selection_publish_target(uploader_key, observation_ids):
            self._update_publish_controls()
            return
        if not self._selection_matches_uploader_target(uploader_key):
            self.set_status_message(
                self.tr("Publishing disabled: the selection does not match this reporting system."),
                level="warning",
                auto_clear_ms=12000,
            )
            self._update_publish_controls()
            return
        if self._selection_has_existing_upload_for_uploader(uploader_key):
            self.set_status_message(
                self.tr("Publishing disabled: selection contains an observation already uploaded to this service."),
                level="warning",
                auto_clear_ms=12000,
            )
            self._update_publish_controls()
            return

        action = self._publish_actions.get(uploader_key)
        target_label = action.text() if action else uploader_key

        total = len(observation_ids)
        success_count = 0
        failed: list[tuple[int, str | None]] = []
        for idx, observation_id in enumerate(observation_ids, start=1):
            self.set_status_message(
                self.tr("Publishing {current}/{total}...").format(current=idx, total=total),
                level="info",
                auto_clear_ms=0,
            )
            ok, _uploaded_id, error = self.upload_observation_to_artsobs(
                observation_id,
                uploader_key=uploader_key,
                show_status=False,
                refresh_table=False,
            )
            if ok:
                success_count += 1
            else:
                failed.append((observation_id, error))

        self.refresh_observations()
        self._invalidate_publish_login_status_cache()
        if not failed:
            self.set_status_message(
                self.tr("Published {count} observations to {target}.").format(
                    count=success_count,
                    target=target_label,
                ),
                level="success",
                auto_clear_ms=12000,
            )
            return

        if success_count:
            summary = self.tr(
                "Published {ok}/{total} observations to {target}. Failed: {failed_count}."
            ).format(
                ok=success_count,
                total=total,
                target=target_label,
                failed_count=len(failed),
            )
            level = "warning"
        else:
            summary = self.tr("Publishing to {target} failed for all selected observations.").format(
                target=target_label
            )
            level = "error"
        first_error = failed[0][1] if failed and failed[0][1] else None
        if first_error:
            summary = f"{summary} {first_error}"
        self.set_status_message(summary, level=level, auto_clear_ms=15000)

    def _build_observation_table_rows_cache(self, observations: list[dict]) -> list[dict]:
        common_name_map = self._build_common_name_map(observations)
        observation_ids: list[int] = []
        for obs in observations:
            try:
                observation_ids.append(int(obs.get("id")))
            except (TypeError, ValueError):
                continue
        thumbnail_map = self._build_observation_thumbnail_map(observation_ids)
        rows: list[dict] = []
        for obs in observations:
            try:
                obs_id = int(obs.get("id"))
            except (TypeError, ValueError):
                continue

            genus_raw = (obs.get("genus") or "").strip()
            species_raw = (obs.get("species") or "").strip()
            genus_display = genus_raw or "-"
            if obs.get("uncertain", 0):
                genus_display = f"? {genus_display}"
            species_display = (obs.get("species") or obs.get("species_guess") or "sp.")

            common_name = self._lookup_common_name(obs, common_name_map)
            common_name_display = common_name
            if not common_name_display:
                if genus_raw and species_raw:
                    common_name_display = f"- ({genus_raw} {species_raw})"
                else:
                    common_name_display = "-"

            spore_short = self._spore_stats_for_observation_row(obs) or "-"
            date_text = obs.get("date") or "-"
            location_text = obs.get("location") or "-"
            lat = obs.get("gps_latitude")
            lon = obs.get("gps_longitude")
            has_coords = lat is not None and lon is not None
            arts_id = obs.get("artsdata_id")
            artportalen_id = obs.get("artportalen_id")
            publish_target = self._observation_publish_target(obs)
            species_name = self._build_species_name(obs)

            search_parts = [str(v) for v in obs.values() if v is not None]
            if common_name_display and common_name_display != "-":
                search_parts.append(common_name_display)
            if spore_short and spore_short != "-":
                search_parts.append(spore_short)
            search_text = " ".join(search_parts).lower()

            rows.append(
                {
                    "id": obs_id,
                    "thumbnail_path": thumbnail_map.get(obs_id),
                    "genus": genus_display,
                    "species": species_display,
                    "common_name": common_name_display,
                    "spore_short": spore_short,
                    "date": date_text,
                    "location": location_text,
                    "lat": lat,
                    "lon": lon,
                    "has_coords": bool(has_coords),
                    "species_name": species_name,
                    "arts_id": arts_id,
                    "artportalen_id": artportalen_id,
                    "publish_target": publish_target,
                    "search_text": search_text,
                }
            )
        return rows

    def _build_observation_thumbnail_map(self, observation_ids: list[int]) -> dict[int, str]:
        ids: list[int] = []
        for observation_id in observation_ids or []:
            try:
                ids.append(int(observation_id))
            except (TypeError, ValueError):
                continue
        if not ids:
            return {}

        conn = None
        try:
            conn = get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            placeholders = ",".join("?" for _ in ids)
            cursor.execute(
                f"""
                SELECT observation_id, id, image_type
                FROM images
                WHERE image_type IN ('field', 'microscope')
                  AND observation_id IN ({placeholders})
                ORDER BY
                    observation_id,
                    CASE
                        WHEN image_type = 'field' THEN 0
                        WHEN image_type = 'microscope' THEN 1
                        ELSE 2
                    END,
                    CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                    sort_order,
                    created_at,
                    id
                """,
                tuple(ids),
            )
            thumbnail_map: dict[int, str] = {}
            for row in cursor.fetchall():
                try:
                    observation_id = int(row["observation_id"])
                    image_id = int(row["id"])
                except (TypeError, ValueError, KeyError):
                    continue
                if observation_id in thumbnail_map:
                    continue
                thumb_path = get_thumbnail_path(image_id, "224x224")
                if thumb_path and Path(thumb_path).exists():
                    thumbnail_map[observation_id] = str(thumb_path)
            return thumbnail_map
        except Exception:
            return {}
        finally:
            if conn is not None:
                conn.close()

    def _observation_thumbnail_icon(self, thumbnail_path: str | None) -> QIcon | None:
        if not thumbnail_path:
            return None
        path = str(thumbnail_path).strip()
        if not path:
            return None
        cached = self._observation_thumb_icon_cache.get(path)
        if cached is not None:
            return cached
        pixmap = QPixmap(path)
        if pixmap.isNull():
            return None
        size = self._observation_table_thumbnail_size()
        pixmap = pixmap.scaled(
            size,
            size,
            Qt.KeepAspectRatioByExpanding,
            Qt.SmoothTransformation,
        )
        if pixmap.width() != size or pixmap.height() != size:
            x = max(0, (pixmap.width() - size) // 2)
            y = max(0, (pixmap.height() - size) // 2)
            pixmap = pixmap.copy(x, y, size, size)
        icon = QIcon(pixmap)
        self._observation_thumb_icon_cache[path] = icon
        return icon

    def _render_observations_table(
        self,
        row_cache: list[dict],
        query: str,
        restore_selection: bool,
        show_status: bool,
        status_message: str | None,
    ) -> None:
        previous_id = self.selected_observation_id if restore_selection else None
        query_text = (query or "").strip().lower()
        if query_text:
            visible_rows = [row for row in (row_cache or []) if query_text in (row.get("search_text") or "")]
        else:
            visible_rows = list(row_cache or [])

        table = self.table
        header = table.horizontalHeader()
        sorting_enabled = bool(table.isSortingEnabled())
        sort_col = header.sortIndicatorSection() if header else -1
        sort_order = header.sortIndicatorOrder() if header else Qt.AscendingOrder
        show_thumbnails = self._show_observation_table_thumbnails()
        thumb_size = self._observation_table_thumbnail_size()

        restored_selection = False
        table.setUpdatesEnabled(False)
        table.blockSignals(True)
        try:
            if sorting_enabled:
                table.setSortingEnabled(False)
            table.setRowCount(len(visible_rows))
            self._update_observations_table_geometry()

            for row_index, row_data in enumerate(visible_rows):
                obs_id = int(row_data["id"])

                id_item = SortableTableWidgetItem("" if show_thumbnails else str(obs_id))
                id_item.setData(Qt.UserRole, obs_id)
                id_item.setFlags(id_item.flags() & ~Qt.ItemIsEditable)
                if show_thumbnails:
                    icon = self._observation_thumbnail_icon(row_data.get("thumbnail_path"))
                    if icon is not None:
                        id_item.setIcon(icon)
                    id_item.setToolTip(self.tr("Observation {id}").format(id=obs_id))
                    id_item.setTextAlignment(Qt.AlignCenter)
                    id_item.setSizeHint(QSize(thumb_size + 20, thumb_size + 20))
                table.setItem(row_index, 0, id_item)

                table.setItem(row_index, 1, QTableWidgetItem(str(row_data.get("common_name") or "-")))
                table.setItem(row_index, 2, QTableWidgetItem(str(row_data.get("genus") or "-")))
                table.setItem(row_index, 3, QTableWidgetItem(str(row_data.get("species") or "sp.")))
                table.setItem(row_index, 4, QTableWidgetItem(str(row_data.get("spore_short") or "-")))

                table.setItem(row_index, 5, QTableWidgetItem(str(row_data.get("date") or "-")))
                table.setItem(row_index, 6, QTableWidgetItem(str(row_data.get("location") or "-")))

                has_coords = bool(row_data.get("has_coords"))
                table.removeCellWidget(row_index, 7)
                map_item = SortableTableWidgetItem("" if has_coords else "-")
                map_item.setData(Qt.UserRole, 1 if has_coords else 0)
                map_item.setFlags(map_item.flags() & ~Qt.ItemIsEditable)
                table.setItem(row_index, 7, map_item)
                if has_coords:
                    lat = row_data.get("lat")
                    lon = row_data.get("lon")
                    species_name = row_data.get("species_name")
                    map_label = QLabel('<a href="#">Map</a>')
                    map_label.setTextFormat(Qt.RichText)
                    map_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
                    map_label.setOpenExternalLinks(False)
                    map_label.setAlignment(Qt.AlignCenter)
                    map_label.linkActivated.connect(
                        lambda _=None, la=lat, lo=lon, sn=species_name: self.show_map_service_dialog(la, lo, sn)
                    )
                    _map_hint = self.tr("Open map service")
                    self._status_hint_controller.register_widget(map_label, _map_hint)
                    map_label.setToolTip(_map_hint)
                    table.setCellWidget(row_index, 7, map_label)

                self._render_publish_cell(
                    row_index,
                    obs_id,
                    row_data.get("publish_target"),
                    row_data.get("arts_id"),
                    row_data.get("artportalen_id"),
                )

            self.rename_btn.setEnabled(False)
            self.delete_btn.setEnabled(False)
            if hasattr(self, "export_btn"):
                self.export_btn.setEnabled(False)
            self._update_publish_controls()
            self.gallery_widget.clear()
            self.selected_observation_id = None

            if sorting_enabled:
                table.setSortingEnabled(True)
                if sort_col >= 0:
                    table.sortItems(sort_col, sort_order)

            if previous_id:
                for row_index in range(table.rowCount()):
                    row_obs_id = self._observation_id_for_row(row_index)
                    if row_obs_id is None:
                        continue
                    if row_obs_id == previous_id:
                        table.selectRow(row_index)
                        restored_selection = True
                        break
            elif not restore_selection:
                table.clearSelection()
        finally:
            table.blockSignals(False)
            table.setUpdatesEnabled(True)

        if restored_selection:
            self.on_selection_changed()
        if status_message:
            self.set_status_message(status_message, level="success")
        elif show_status:
            self.set_status_message(self.tr("Refreshed db."), level="success")

    def _observations_detail_default_height(self) -> int:
        gallery = getattr(self, "gallery_widget", None)
        detail_widget = getattr(self, "detail_widget", None)
        if gallery is None:
            return 190
        gallery_height = max(gallery.minimumHeight(), 190)
        if detail_widget is None or detail_widget.layout() is None:
            return gallery_height
        margins = detail_widget.layout().contentsMargins()
        return gallery_height + margins.top() + margins.bottom()

    def _observations_detail_max_height(self) -> int:
        gallery = getattr(self, "gallery_widget", None)
        detail_widget = getattr(self, "detail_widget", None)
        if gallery is None:
            return 220
        gallery_max = max(gallery.minimumHeight(), gallery.maximum_useful_height())
        if detail_widget is None or detail_widget.layout() is None:
            return gallery_max
        margins = detail_widget.layout().contentsMargins()
        return gallery_max + margins.top() + margins.bottom()

    def _apply_observations_splitter_height(self, detail_height: int | None = None) -> None:
        splitter = getattr(self, "observations_splitter", None)
        detail_widget = getattr(self, "detail_widget", None)
        if splitter is None or detail_widget is None:
            return
        target = self._observations_detail_default_height() if detail_height is None else int(detail_height)
        target = max(detail_widget.minimumHeight(), min(self._observations_detail_max_height(), target))
        detail_widget.setMaximumHeight(self._observations_detail_max_height())
        sizes = splitter.sizes()
        total = sum(sizes) if sizes else 0
        if total <= 0:
            total = splitter.height()
        if total <= 0:
            total = max(self.height(), target + 400)
        if self._observations_splitter_syncing:
            return
        self._observations_splitter_syncing = True
        try:
            splitter.setSizes([max(0, total - target), target])
        finally:
            self._observations_splitter_syncing = False

    def _on_observations_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "observations_splitter", None)
        if splitter is None or self._observations_splitter_syncing:
            return
        sizes = splitter.sizes()
        detail_height = sizes[1] if len(sizes) >= 2 else None
        QTimer.singleShot(0, lambda h=detail_height: self._apply_observations_splitter_height(h))

    def resizeEvent(self, event):
        super().resizeEvent(event)
        splitter = getattr(self, "observations_splitter", None)
        if splitter is None or self._observations_splitter_syncing:
            return
        sizes = splitter.sizes()
        detail_height = sizes[1] if len(sizes) >= 2 else None
        QTimer.singleShot(0, lambda h=detail_height: self._apply_observations_splitter_height(h))

    def refresh_observations(
        self,
        show_status: bool = False,
        status_message: str | None = None,
        restore_selection: bool = True,
    ):
        """Load all observations from database."""
        observations = ObservationDB.get_all_observations()
        self._vernacular_cache = {}
        self._table_vernacular_db = self._get_vernacular_db_for_active_language()
        self._update_table_headers()
        self._observation_table_rows_cache = self._build_observation_table_rows_cache(observations)
        self._render_observations_table(
            self._observation_table_rows_cache,
            query=self.search_input.text().strip().lower() if hasattr(self, "search_input") else "",
            restore_selection=restore_selection,
            show_status=show_status,
            status_message=status_message,
        )

    def _get_vernacular_db_for_active_language(self):
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if not db_path:
            return None
        return VernacularDB(db_path, language_code=lang)

    def _build_common_name_map(self, observations: list[dict]) -> dict[tuple[str, str], str | None]:
        """Pre-build a cache of all common names for the observations."""
        if not self._table_vernacular_db:
            return {}
        
        # Collect all unique genus+species combinations from observations
        taxa = set()
        for obs in observations:
            genus = self._normalize_taxon_text(obs.get("genus"))
            species = self._normalize_taxon_text(obs.get("species"))
            if not genus or not species:
                guess = self._normalize_taxon_text(obs.get("species_guess"))
                parts = guess.split() if guess else []
                if len(parts) >= 2:
                    genus, species = parts[0], parts[1]
            if genus and species:
                taxa.add((genus, species))
        
        if not taxa:
            return {}
        
        # Fetch all common names in one database session
        name_map: dict[tuple[str, str], str | None] = {}
        for genus, species in taxa:
            try:
                name_map[(genus, species)] = self._table_vernacular_db.vernacular_from_taxon(genus, species)
            except Exception:
                name_map[(genus, species)] = None
        
        return name_map

    def get_ai_suggestions_for_observation(self, obs_id: int) -> dict | None:
        """Return cached AI suggestion state for the given observation id."""
        return self._ai_suggestions_cache.get(obs_id)

    def _merge_observation_edit_draft(
        self,
        observation: dict | None,
        draft: dict | None,
    ) -> dict | None:
        if not observation and not draft:
            return None
        merged = dict(observation or {})
        if draft:
            merged.update(draft)
        return merged

    def _remap_ai_state_to_images(
        self,
        ai_state: dict | None,
        image_results: list[ImageImportResult],
    ) -> dict | None:
        if not ai_state:
            return None
        predictions = ai_state.get("predictions") or {}
        selected = ai_state.get("selected") or {}
        prev_paths = ai_state.get("paths") or []
        prev_image_ids = ai_state.get("image_ids") or []
        if not isinstance(predictions, dict) or not isinstance(selected, dict):
            return None
        new_paths = [item.filepath for item in image_results]
        new_image_ids = [item.image_id for item in image_results]
        new_index_by_path = {path: idx for idx, path in enumerate(new_paths) if path}
        new_index_by_image_id = {
            int(image_id): idx
            for idx, image_id in enumerate(new_image_ids)
            if isinstance(image_id, int) and image_id > 0
        }
        new_predictions: dict[int, list] = {}
        new_selected: dict[int, dict] = {}
        for old_idx, preds in predictions.items():
            try:
                old_index = int(old_idx)
            except (TypeError, ValueError):
                continue
            old_image_id = prev_image_ids[old_index] if 0 <= old_index < len(prev_image_ids) else None
            new_index = (
                new_index_by_image_id.get(int(old_image_id))
                if isinstance(old_image_id, int) and old_image_id > 0
                else None
            )
            if new_index is None:
                old_path = prev_paths[old_index] if 0 <= old_index < len(prev_paths) else None
                new_index = new_index_by_path.get(old_path)
            if new_index is not None:
                new_predictions[new_index] = preds
        for old_idx, sel in selected.items():
            try:
                old_index = int(old_idx)
            except (TypeError, ValueError):
                continue
            old_image_id = prev_image_ids[old_index] if 0 <= old_index < len(prev_image_ids) else None
            new_index = (
                new_index_by_image_id.get(int(old_image_id))
                if isinstance(old_image_id, int) and old_image_id > 0
                else None
            )
            if new_index is None:
                old_path = prev_paths[old_index] if 0 <= old_index < len(prev_paths) else None
                new_index = new_index_by_path.get(old_path)
            if new_index is not None:
                new_selected[new_index] = sel
        selected_index = ai_state.get("selected_index")
        new_selected_index = None
        if selected_index is not None:
            try:
                old_index = int(selected_index)
            except (TypeError, ValueError):
                old_index = None
            if old_index is not None:
                old_image_id = prev_image_ids[old_index] if 0 <= old_index < len(prev_image_ids) else None
                if isinstance(old_image_id, int) and old_image_id > 0:
                    new_selected_index = new_index_by_image_id.get(int(old_image_id))
                if new_selected_index is None and 0 <= old_index < len(prev_paths):
                    old_path = prev_paths[old_index]
                    new_selected_index = new_index_by_path.get(old_path)
        if new_selected_index is None and new_predictions:
            new_selected_index = sorted(new_predictions.keys())[0]
        return {
            "predictions": new_predictions,
            "selected": new_selected,
            "selected_index": new_selected_index,
            "paths": new_paths,
            "image_ids": new_image_ids,
        }

    def _serialize_ai_state(self, ai_state: dict | None) -> str | None:
        if not ai_state:
            return None
        try:
            return json.dumps(ai_state, ensure_ascii=False)
        except (TypeError, ValueError):
            return None

    def _deserialize_ai_state(self, raw_value: str | None) -> dict | None:
        text = str(raw_value or "").strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None

    def _load_observation_ai_state(self, observation: dict | None) -> dict | None:
        if not observation:
            return None
        obs_id = observation.get("id")
        if obs_id is not None and obs_id in self._ai_suggestions_cache:
            return self._ai_suggestions_cache.get(obs_id)
        ai_state = self._deserialize_ai_state(observation.get("ai_state_json"))
        if obs_id is not None and ai_state:
            self._ai_suggestions_cache[obs_id] = ai_state
        return ai_state

    def _lookup_common_name(self, obs: dict, name_map: dict[tuple[str, str], str | None]) -> str | None:
        """Look up common name from the pre-built cache."""
        stored_name = self._normalize_taxon_text(obs.get("common_name"))
        if stored_name:
            return stored_name
        genus = self._normalize_taxon_text(obs.get("genus"))
        species = self._normalize_taxon_text(obs.get("species"))
        
        if not genus or not species:
            guess = self._normalize_taxon_text(obs.get("species_guess"))
            parts = guess.split() if guess else []
            if len(parts) >= 2:
                genus, species = parts[0], parts[1]
        
        if not genus or not species:
            return None
        return name_map.get((genus, species))

    @staticmethod
    def _normalize_duplicate_compare_text(value: str | None) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip()).casefold()

    @staticmethod
    def _observation_image_name_set(images: list[dict] | list[ImageImportResult]) -> set[str]:
        names: set[str] = set()
        for image in images or []:
            for key in ("filepath", "original_filepath"):
                value = None
                if isinstance(image, dict):
                    value = image.get(key)
                else:
                    value = getattr(image, key, None)
                if not value:
                    continue
                name = Path(str(value)).name.strip()
                if name:
                    names.add(name.casefold())
        return names

    def _build_duplicate_candidate_thumbnails(self, observation_id: int, limit: int = 4) -> list[str]:
        images = ImageDB.get_images_for_observation(observation_id)
        paths: list[str] = []
        fallback_paths: list[str] = []
        for image in images:
            image_id = image.get("id")
            if not image_id:
                continue
            thumb_path = get_thumbnail_path(int(image_id), "224x224")
            if not thumb_path or not Path(thumb_path).exists():
                continue
            image_type = str(image.get("image_type") or "").strip().lower()
            if image_type == "field":
                paths.append(str(thumb_path))
            else:
                fallback_paths.append(str(thumb_path))
            if len(paths) >= limit:
                break
        if len(paths) < limit:
            for thumb_path in fallback_paths:
                if thumb_path in paths:
                    continue
                paths.append(thumb_path)
                if len(paths) >= limit:
                    break
        return paths

    def _find_duplicate_observation_candidates(
        self,
        obs_data: dict,
        image_results: list[ImageImportResult],
    ) -> list[dict]:
        target_minute = _normalized_observation_datetime_minute(obs_data.get("date"))
        target_taxon = (
            self._normalize_duplicate_compare_text(obs_data.get("genus")),
            self._normalize_duplicate_compare_text(obs_data.get("species")),
        )
        target_location = self._normalize_duplicate_compare_text(obs_data.get("location"))
        target_names = self._observation_image_name_set(image_results)
        candidates: list[dict] = []
        for obs in ObservationDB.get_all_observations():
            reasons: list[str] = []
            score = 0
            existing_minute = _normalized_observation_datetime_minute(obs.get("date"))
            same_minute = bool(target_minute and existing_minute and target_minute == existing_minute)
            existing_taxon = (
                self._normalize_duplicate_compare_text(obs.get("genus")),
                self._normalize_duplicate_compare_text(obs.get("species")),
            )
            same_taxon = bool(target_taxon[0] and target_taxon[1] and target_taxon == existing_taxon)
            existing_location = self._normalize_duplicate_compare_text(obs.get("location"))
            same_location = bool(target_location and existing_location and target_location == existing_location)

            existing_images = ImageDB.get_images_for_observation(int(obs.get("id")))
            shared_names = sorted(
                target_names.intersection(self._observation_image_name_set(existing_images))
            )

            if same_minute:
                reasons.append(self.tr("Same date/time"))
                score += 3
            if same_taxon:
                reasons.append(self.tr("Same taxon"))
                score += 3
            if same_location:
                reasons.append(self.tr("Same location"))
                score += 2
            if shared_names:
                label = self.tr("Shared image filename") if len(shared_names) == 1 else self.tr("Shared image filenames")
                reasons.append(f"{label}: {', '.join(shared_names[:3])}")
                score += 4 + min(3, len(shared_names))

            likely_duplicate = False
            if shared_names and (same_minute or same_location or same_taxon):
                likely_duplicate = True
            elif same_minute and same_taxon:
                likely_duplicate = True
            elif same_minute and same_location and len(shared_names) >= 1:
                likely_duplicate = True

            if not likely_duplicate:
                continue

            candidates.append(
                {
                    "observation": obs,
                    "reasons": reasons,
                    "shared_names": shared_names,
                    "score": score,
                    "thumbnail_paths": self._build_duplicate_candidate_thumbnails(int(obs.get("id"))),
                }
            )

        candidates.sort(
            key=lambda item: (
                -int(item.get("score", 0)),
                str(item.get("observation", {}).get("date") or ""),
            )
        )
        return candidates[:4]

    def _confirm_duplicate_observation_creation(
        self,
        obs_data: dict,
        image_results: list[ImageImportResult],
    ) -> bool:
        candidates = self._find_duplicate_observation_candidates(obs_data, image_results)
        if not candidates:
            return True

        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Possible Duplicate Observation"))
        dialog.setModal(True)
        dialog.setMinimumWidth(760)

        layout = QVBoxLayout(dialog)
        layout.setSpacing(12)

        title = QLabel(
            self.tr("A similar observation already exists. Do you still want to create a new observation?")
        )
        title.setWordWrap(True)
        title.setStyleSheet(f"font-size: {pt(12)}pt; font-weight: 600;")
        layout.addWidget(title)

        subtitle_parts: list[str] = []
        if obs_data.get("date"):
            subtitle_parts.append(self.tr("Date: {value}").format(value=obs_data.get("date")))
        if obs_data.get("location"):
            subtitle_parts.append(self.tr("Location: {value}").format(value=obs_data.get("location")))
        genus = (obs_data.get("genus") or "").strip()
        species = (obs_data.get("species") or "").strip()
        if genus or species:
            subtitle_parts.append(self.tr("Taxon: {value}").format(value=f"{genus} {species}".strip()))
        if subtitle_parts:
            summary = QLabel(" | ".join(subtitle_parts))
            summary.setWordWrap(True)
            summary.setStyleSheet("color: #5f6b7a;")
            layout.addWidget(summary)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(10)

        for candidate in candidates:
            obs = candidate["observation"]
            card = QFrame()
            card.setStyleSheet(
                "QFrame { border: 1px solid #d9dde3; border-radius: 10px; background: #fbfcfd; }"
            )
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(12, 12, 12, 12)
            card_layout.setSpacing(8)

            heading = QLabel(
                self.tr("#{id}  {date}  {label}").format(
                    id=obs.get("id"),
                    date=obs.get("date") or self.tr("Unknown date"),
                    label=((f"{obs.get('genus') or ''} {obs.get('species') or ''}").strip() or (obs.get("location") or self.tr("Unnamed observation"))),
                )
            )
            heading.setStyleSheet("font-weight: 600;")
            heading.setWordWrap(True)
            card_layout.addWidget(heading)

            meta_parts: list[str] = []
            if obs.get("location"):
                meta_parts.append(self.tr("Location: {value}").format(value=obs.get("location")))
            image_count = len(ImageDB.get_images_for_observation(int(obs.get("id"))))
            meta_parts.append(self.tr("Images: {count}").format(count=image_count))
            if candidate.get("reasons"):
                meta_parts.append(self.tr("Match: {value}").format(value="; ".join(candidate["reasons"])))
            meta = QLabel(" | ".join(meta_parts))
            meta.setWordWrap(True)
            meta.setStyleSheet("color: #5f6b7a;")
            card_layout.addWidget(meta)

            thumbs = candidate.get("thumbnail_paths") or []
            if thumbs:
                thumb_row = QHBoxLayout()
                thumb_row.setSpacing(8)
                for thumb_path in thumbs:
                    label = QLabel()
                    pixmap = QPixmap(str(thumb_path))
                    if pixmap and not pixmap.isNull():
                        pixmap = pixmap.scaled(88, 88, Qt.KeepAspectRatioByExpanding, Qt.SmoothTransformation)
                        if pixmap.width() != 88 or pixmap.height() != 88:
                            x = max(0, (pixmap.width() - 88) // 2)
                            y = max(0, (pixmap.height() - 88) // 2)
                            pixmap = pixmap.copy(x, y, 88, 88)
                        label.setPixmap(pixmap)
                    label.setFixedSize(88, 88)
                    label.setStyleSheet("border: 1px solid #d9dde3; border-radius: 6px; background: white;")
                    thumb_row.addWidget(label)
                thumb_row.addStretch(1)
                card_layout.addLayout(thumb_row)

            container_layout.addWidget(card)

        container_layout.addStretch(1)
        scroll.setWidget(container)
        layout.addWidget(scroll, 1)

        buttons = QDialogButtonBox(dialog)
        create_btn = buttons.addButton(self.tr("Create Anyway"), QDialogButtonBox.AcceptRole)
        create_btn.setStyleSheet("font-weight: bold;")
        buttons.addButton(self.tr("Go Back"), QDialogButtonBox.RejectRole)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        return dialog.exec() == QDialog.Accepted
        
        # Use the pre-built cache - no database access needed here!
        return name_map.get((genus, species))

    def _normalize_taxon_text(self, value: str | None) -> str:
        if not value:
            return ""
        try:
            import unicodedata
            text = unicodedata.normalize("NFKC", str(value))
        except Exception:
            text = str(value)
        text = text.replace("\u00a0", " ")
        text = text.strip()
        if text.startswith("?"):
            text = text.lstrip("?").strip()
        return " ".join(text.split())

    def _common_name_column_title(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        if lang in {"no", "nb", "nn"}:
            return self.tr("Navn")
        return self.tr("Name")

    def _observation_first_column_title(self) -> str:
        if self._show_observation_table_thumbnails():
            return self.tr("Photo")
        return self.tr("ID")

    def _spore_stats_column_title(self) -> str:
        lang = (SettingsDB.get_setting("ui_language", "en") or "en").lower()
        return self.tr("Sporer") if lang.startswith("nb") or lang.startswith("no") else self.tr("Spores")

    def _publish_prefers_norwegian_labels(self) -> bool:
        ui_lang = str(SettingsDB.get_setting("ui_language", "en") or "en").lower()
        if ui_lang.startswith("nb") or ui_lang.startswith("no"):
            return True
        vern_lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        return bool(vern_lang.startswith("nb") or vern_lang.startswith("no"))

    def _localize_spore_stats_for_publish(self, stats_text: str | None) -> str:
        text = str(stats_text or "").strip()
        if not text:
            return ""
        if not self._publish_prefers_norwegian_labels():
            return text
        # Only rewrite the leading label; keep the actual numeric content as stored.
        return re.sub(r"^\s*(?:Spores?|Sporer)\s*:", "Sporer:", text, count=1, flags=re.IGNORECASE)

    def _update_table_headers(self) -> None:
        if not hasattr(self, "table"):
            return
        id_item = self.table.horizontalHeaderItem(0)
        if id_item:
            id_item.setText(self._observation_first_column_title())
        item = self.table.horizontalHeaderItem(1)
        if item:
            item.setText(self._common_name_column_title())
        spore_item = self.table.horizontalHeaderItem(4)
        if spore_item:
            spore_item.setText(self._spore_stats_column_title())

    def _show_observation_table_thumbnails(self) -> bool:
        checkbox = getattr(self, "show_table_thumbnails_checkbox", None)
        if checkbox is not None:
            return bool(checkbox.isChecked())
        return bool(SettingsDB.get_setting(self.SETTING_SHOW_TABLE_THUMBNAILS, False))

    def _observation_table_thumbnail_size(self) -> int:
        return 96

    def _update_observations_table_geometry(self) -> None:
        if not hasattr(self, "table"):
            return
        if self._show_observation_table_thumbnails():
            thumb_size = self._observation_table_thumbnail_size()
            self.table.setIconSize(QSize(thumb_size, thumb_size))
            self.table.setColumnWidth(0, thumb_size + 28)
            self.table.verticalHeader().setDefaultSectionSize(
                max(self._observations_table_default_row_height, thumb_size + 18)
            )
        else:
            self.table.setIconSize(QSize(16, 16))
            self.table.setColumnWidth(0, 56)
            self.table.verticalHeader().setDefaultSectionSize(
                self._observations_table_default_row_height
            )

    def _on_show_table_thumbnails_toggled(self, checked: bool) -> None:
        SettingsDB.set_setting(self.SETTING_SHOW_TABLE_THUMBNAILS, bool(checked))
        self._update_table_headers()
        self._update_observations_table_geometry()
        self._render_observations_table(
            self._observation_table_rows_cache,
            query=self.search_input.text().strip().lower() if hasattr(self, "search_input") else "",
            restore_selection=True,
            show_status=False,
            status_message=None,
        )

    def _format_spore_stats_short(self, stats: str | None) -> str | None:
        if not stats:
            return None
        text = str(stats)
        length_seg = None
        width_seg = None
        match_len = re.search(r"Spores?:\\s*([^,]+?)\\s*um\\s*x", text, re.IGNORECASE)
        match_wid = re.search(r"\\s*x\\s*([^,]+?)\\s*um", text, re.IGNORECASE)
        if match_len:
            length_seg = match_len.group(1)
        if match_wid:
            width_seg = match_wid.group(1)

        n_match = re.search(r"\bn\s*=\s*(\d+)\b", text, re.IGNORECASE)
        count = n_match.group(1) if n_match else None
        q_match = re.search(r"\bQ\s*=\s*(.+?)(?:\s{2,}\bn\s*=|,\s*\bn\s*=|$)", text, re.IGNORECASE)
        q_seg = q_match.group(1).strip() if q_match else None

        def _extract_p05_p95(segment: str | None) -> tuple[str | None, str | None]:
            if not segment:
                return None, None
            nums = re.findall(r"[0-9]+(?:\\.[0-9]+)?", segment)
            if len(nums) >= 3:
                return nums[1], nums[2]
            if len(nums) == 2:
                return nums[0], nums[1]
            return None, None

        l5, l95 = _extract_p05_p95(length_seg)
        w5, w95 = _extract_p05_p95(width_seg)
        q5, q95 = _extract_p05_p95(q_seg)
        if not l5 or not l95 or not w5 or not w95:
            return None

        base = f"{l5}-{l95} x {w5}-{w95}"
        if q5 and q95:
            base += f"  Q = {q5}-{q95}"
        if count:
            return f"{base}  n = {count}"
        return base

    @staticmethod
    def _format_spore_stats_short_from_values(stats: dict | None) -> str | None:
        if not isinstance(stats, dict) or not stats:
            return None
        length_p5 = stats.get("length_p5")
        length_p95 = stats.get("length_p95")
        width_p5 = stats.get("width_p5")
        width_p95 = stats.get("width_p95")
        ratio_p5 = stats.get("ratio_p5")
        ratio_p95 = stats.get("ratio_p95")
        count = stats.get("count")
        if None in (length_p5, length_p95, width_p5, width_p95):
            return None
        try:
            text = f"{float(length_p5):.1f}-{float(length_p95):.1f} x {float(width_p5):.1f}-{float(width_p95):.1f}"
            if ratio_p5 is not None and ratio_p95 is not None:
                text += f"  Q = {float(ratio_p5):.1f}-{float(ratio_p95):.1f}"
            if count is not None:
                text += f"  n = {int(count)}"
            return text
        except Exception:
            return None

    def _spore_stats_for_observation_row(self, observation: dict | None) -> str | None:
        if not isinstance(observation, dict):
            return None
        from_string = self._format_spore_stats_short(observation.get("spore_statistics"))
        if from_string:
            return from_string
        try:
            obs_id = int(observation.get("id"))
        except (TypeError, ValueError):
            return None
        stats = MeasurementDB.get_statistics_for_observation(obs_id, measurement_category="spores")
        return self._format_spore_stats_short_from_values(stats)

    def apply_vernacular_language_change(self) -> None:
        self._table_vernacular_db = self._get_vernacular_db_for_active_language()
        self._vernacular_cache = {}
        self._update_table_headers()
        self.refresh_observations()

    def _question_yes_no(self, title, text, default_yes=False):
        """Show a localized Yes/No confirmation dialog."""
        return ask_wrapped_yes_no(self, title, text, default_yes=default_yes)

    def _warn_delete_failures(self, failures: list[str]) -> int:
        if not failures:
            return 0
        paths = [p for p in failures if p]
        if not paths:
            return 0
        names = [Path(p).name for p in paths]
        return len(names)

    def _get_measurements_for_image(self, image_id):
        """Get measurements for a specific image."""
        return MeasurementDB.get_measurements_for_image(image_id)

    def _build_species_name(self, obs):
        """Return a scientific name when genus/species are known."""
        genus = (obs.get('genus') or '').strip()
        species = (obs.get('species') or '').strip()
        if genus and species:
            return f"{genus} {species}".strip()
        guess = (obs.get('species_guess') or '').strip()
        if guess:
            parts = guess.split()
            if len(parts) >= 2:
                return f"{parts[0]} {parts[1]}".strip()
        return None

    def show_map_service_dialog(self, lat, lon, species_name):
        """Show a dialog to choose a map service."""
        self.map_helper.show_map_service_dialog(lat, lon, species_name)

    def _confirm_delete_image(self, image_id):
        """Confirm and delete an image (and measurements if present)."""
        measurements = self._get_measurements_for_image(image_id)
        if measurements:
            confirmed = ask_measurements_exist_delete(self, count=1)
        else:
            confirmed = self._question_yes_no(
                self.tr("Confirm Delete"),
                self.tr("Delete image?"),
                default_yes=False
            )
        if confirmed:
            ImageDB.delete_image(image_id)
            self.refresh_observations()
            self.set_status_message(self.tr("Image deleted."), level="success")

    def _on_gallery_image_clicked(self, _image_id, _filepath):
        """Keep selection in Observations tab; do not jump to Measure tab."""
        return

    def _on_gallery_measure_badge_clicked(self, image_id, _filepath):
        """Open the clicked gallery image in the Measure tab."""
        if not image_id or not self.selected_observation_id:
            return
        selected = self.get_selected_observation()
        if not selected:
            return
        observation_id, display_name = selected
        QTimer.singleShot(
            75,
            lambda img_id=int(image_id), obs_id=int(observation_id), name=display_name:
                self.image_selected.emit(img_id, obs_id, name),
        )

    def _register_gallery_publish_hint_widgets(self) -> None:
        if not hasattr(self, "_status_hint_controller") or self._status_hint_controller is None:
            return
        if not hasattr(self, "gallery_widget"):
            return
        hint = self.tr("Select image for online publishing")
        for checkbox in self.gallery_widget.publish_checkbox_widgets():
            self._status_hint_controller.register_widget(checkbox, hint)

    def _apply_gallery_publish_selection_for_observation(self, observation_id: int | None) -> None:
        if not observation_id or not hasattr(self, "gallery_widget"):
            return
        images = ImageDB.get_images_for_observation(int(observation_id))
        all_ids = {
            int(img.get("id"))
            for img in images
            if img.get("id") is not None
        }
        excluded = self._publish_excluded_image_ids(observation_id)
        excluded = {img_id for img_id in excluded if img_id in all_ids}
        selected_ids = all_ids - excluded
        self.gallery_widget.set_publish_selected_ids(selected_ids, emit_signal=False)
        self._register_gallery_publish_hint_widgets()

    def _on_gallery_publish_selection_changed(self, selected_ids) -> None:
        if not self.selected_observation_id:
            return
        images = ImageDB.get_images_for_observation(int(self.selected_observation_id))
        all_ids = {
            int(img.get("id"))
            for img in images
            if img.get("id") is not None
        }
        try:
            selected_set = {int(v) for v in (selected_ids or set())}
        except Exception:
            selected_set = set()
        excluded = all_ids - selected_set
        obs_id = int(self.selected_observation_id)
        self._set_publish_excluded_image_ids(obs_id, excluded)
        host = self.window()
        if host is None:
            host = self.parent()
        if host is not None and hasattr(host, "_set_publish_excluded_image_ids_for_observation"):
            try:
                host._set_publish_excluded_image_ids_for_observation(obs_id, excluded)
            except Exception:
                pass
        if (
            host is not None
            and hasattr(host, "active_observation_id")
            and int(getattr(host, "active_observation_id") or 0) == obs_id
            and hasattr(host, "_apply_measure_gallery_publish_selection")
        ):
            try:
                host._apply_measure_gallery_publish_selection()
            except Exception:
                pass

    def refresh_publish_checkbox_state(self, observation_id: int | None = None) -> None:
        """Refresh publish checkbox selection from persisted settings."""
        if not hasattr(self, "gallery_widget"):
            return
        target_obs_id = observation_id or self.selected_observation_id
        if not target_obs_id:
            selected = self.get_selected_observation()
            if selected:
                target_obs_id = selected[0]
        if not target_obs_id:
            host = self.window()
            if host is None:
                host = self.parent()
            if host is not None and hasattr(host, "active_observation_id"):
                target_obs_id = int(getattr(host, "active_observation_id") or 0) or None
        if not target_obs_id:
            return
        self._apply_gallery_publish_selection_for_observation(int(target_obs_id))

    def on_selection_changed(self):
        """Update detail view when selection changes."""
        # Reset action buttons first to avoid stale enabled state during edge-case transitions.
        self.rename_btn.setEnabled(False)
        self.delete_btn.setEnabled(False)
        if hasattr(self, "export_btn"):
            self.export_btn.setEnabled(False)
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            self.gallery_widget.clear()
            self.selected_observation_id = None
            self._update_publish_controls()
            return
        if len(selected_rows) > 1:
            self.delete_btn.setEnabled(True)
            if hasattr(self, "export_btn"):
                self.export_btn.setEnabled(True)
            self.gallery_widget.clear()
            self.selected_observation_id = None
            self._update_publish_controls()
            return

        row = selected_rows[0].row()
        id_item = self.table.item(row, 0)
        if id_item is None:
            self.gallery_widget.clear()
            self.selected_observation_id = None
            self._update_publish_controls()
            return
        obs_id = self._observation_id_from_item(id_item)
        if obs_id is None:
            self.gallery_widget.clear()
            self.selected_observation_id = None
            self._update_publish_controls()
            return
        self.selected_observation_id = obs_id
        self.observation_highlighted.emit(obs_id)

        self.rename_btn.setEnabled(True)
        self.delete_btn.setEnabled(True)
        if hasattr(self, "export_btn"):
            self.export_btn.setEnabled(True)

        # Populate image browser for the selected row only (no extra full-table DB fetch).
        self.gallery_widget.set_observation_id(obs_id)
        self._apply_gallery_publish_selection_for_observation(obs_id)
        # Do not force a full Measure-tab reload on every table click.
        # MainWindow.on_tab_changed() synchronizes the selected observation when the
        # user switches to Measure/Analysis, and explicit actions can still call
        # set_selected_as_active() directly.
        self._update_publish_controls()
        pending_notice = self._pending_published_image_upload_notice_for_observation(obs_id)
        if pending_notice:
            msg, level = pending_notice
            self.set_status_message(msg, level=level, auto_clear_ms=0)

    def on_row_double_clicked(self, item):
        """Double-click to open edit dialog for the observation."""
        if len(self.table.selectionModel().selectedRows()) != 1:
            return
        self.edit_observation()

    def set_selected_as_active(self, switch_tab=True):
        """Set the selected observation as active, optionally switching to Measure tab."""
        selected_rows = self.table.selectionModel().selectedRows()
        if len(selected_rows) != 1:
            return

        row = selected_rows[0].row()
        obs_id = self._observation_id_for_row(row)
        if obs_id is None:
            return
        genus = self.table.item(row, 2).text()
        species = self.table.item(row, 3).text()
        date = self.table.item(row, 5).text()
        display_name = f"{genus} {species} {date}"

        # Emit signal to set as active observation
        self.observation_selected.emit(obs_id, display_name, switch_tab)

    def get_selected_observation(self):
        """Return (observation_id, display_name) for current selection."""
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            return None
        row = selected_rows[0].row()
        obs_id = self._observation_id_for_row(row)
        if obs_id is None:
            return None
        genus = self.table.item(row, 2).text()
        species = self.table.item(row, 3).text()
        date = self.table.item(row, 5).text()
        display_name = f"{genus} {species} {date}"
        return obs_id, display_name

    def _collect_artsobs_image_paths(self, observation_id: int) -> list[str]:
        images = ImageDB.get_images_for_observation(observation_id)
        excluded_ids = self._publish_excluded_image_ids(observation_id)
        ordered = []
        for image in images:
            image_id = image.get("id")
            if image_id is not None:
                try:
                    if int(image_id) in excluded_ids:
                        continue
                except Exception:
                    pass
            image_type = (image.get("image_type") or "").strip().lower()
            if image_type not in {"field", "microscope"}:
                continue
            filepath = image.get("filepath") or image.get("original_filepath")
            if not filepath or not Path(filepath).exists():
                continue
            if filepath not in ordered:
                ordered.append(filepath)
        return ordered

    @staticmethod
    def _publish_excluded_images_setting_key(observation_id: int | None) -> str:
        return f"artsobs_publish_excluded_image_ids_{int(observation_id or 0)}"

    @classmethod
    def _publish_excluded_image_ids(cls, observation_id: int | None) -> set[int]:
        if not observation_id:
            return set()
        key = cls._publish_excluded_images_setting_key(observation_id)
        raw = SettingsDB.get_setting(key, "[]")
        try:
            loaded = json.loads(raw or "[]")
            if isinstance(loaded, list):
                return {int(v) for v in loaded}
        except Exception:
            pass
        return set()

    @classmethod
    def _set_publish_excluded_image_ids(cls, observation_id: int | None, excluded_ids: set[int]) -> None:
        if not observation_id:
            return
        normalized = sorted({int(v) for v in (excluded_ids or set())})
        key = cls._publish_excluded_images_setting_key(observation_id)
        try:
            SettingsDB.set_setting(
                key,
                json.dumps(normalized),
            )
        except Exception:
            pass

    @staticmethod
    def _publish_option_enabled(key: str, default: bool = False) -> bool:
        fallback = "1" if default else "0"
        raw = SettingsDB.get_setting(key, fallback)
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _publish_path_key(path: str | None) -> str:
        if not path:
            return ""
        try:
            return str(Path(path).resolve()).lower()
        except Exception:
            return str(Path(path)).lower()

    @staticmethod
    def _cleanup_publish_temp_dir(temp_dir: Path | None) -> None:
        if not temp_dir:
            return
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass

    @staticmethod
    def _publish_observation_year(observation_date: str | None) -> int:
        parsed = _parse_observation_datetime(observation_date)
        if parsed and parsed.isValid():
            return int(parsed.date().year())
        text = str(observation_date or "").strip()
        match = re.match(r"^(\d{4})", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return int(datetime.now().year)

    def _publish_copyright_text(self, obs: dict) -> str | None:
        if not isinstance(obs, dict):
            return None
        profile = SettingsDB.get_profile() if hasattr(SettingsDB, "get_profile") else {}
        name = str((profile or {}).get("name") or "").strip()
        if not name:
            name = str(obs.get("author") or "").strip()
        if not name:
            return None
        return f"{name} • {self._publish_image_license_watermark_text()}"

    def _publish_image_license_code(self) -> str:
        raw = str(SettingsDB.get_setting(self.SETTING_IMAGE_LICENSE, "60") or "60").strip()
        return raw if raw in self.ARTSOBS_MEDIA_LICENSE_CODES else "60"

    def _publish_image_license_watermark_text(self) -> str:
        return self.ARTSOBS_MEDIA_LICENSE_CODES.get(
            self._publish_image_license_code(),
            "No reuse without permission",
        )

    @staticmethod
    def _draw_publish_copyright(pixmap: QPixmap, text: str) -> bool:
        if not pixmap or pixmap.isNull() or not text:
            return False
        painter = QPainter(pixmap)
        if not painter.isActive():
            return False
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setRenderHint(QPainter.TextAntialiasing, True)
        font = painter.font()
        target_font_px = max(10, int(round(float(pixmap.height()) * 0.012)))
        font.setPixelSize(target_font_px)
        font.setBold(False)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        margin = max(8, int(round(target_font_px * 0.6)))
        baseline = max(metrics.ascent() + margin, pixmap.height() - margin)
        x = margin

        # Inkscape-like stroked underlay: dark outline @ 40% opacity under white text.
        stroke_w = max(1.0, float(metrics.height()) * 0.4)
        path = QPainterPath()
        path.addText(float(x), float(baseline), painter.font(), text)
        pen = QPen(QColor(0, 0, 0, 102), stroke_w)
        pen.setJoinStyle(Qt.RoundJoin)
        pen.setCapStyle(Qt.RoundCap)
        painter.save()
        painter.setBrush(Qt.NoBrush)
        painter.setPen(pen)
        painter.drawPath(path)
        painter.restore()
        painter.setPen(QColor(255, 255, 255, 225))
        painter.drawText(int(x), int(baseline), text)
        painter.end()
        return True

    def _generate_publish_copyright_images(
        self,
        image_paths: list[str],
        temp_dir: Path,
        copyright_text: str,
        progress_cb=None,
        cancel_cb=None,
    ) -> list[str]:
        if not image_paths or not copyright_text:
            return list(image_paths or [])

        generated: list[str] = []
        total = len(image_paths)
        for idx, source_path in enumerate(image_paths, start=1):
            if cancel_cb:
                cancel_cb()
            if progress_cb:
                progress_cb(
                    self.tr("Adding watermark {current}/{total}...").format(
                        current=idx,
                        total=total,
                    ),
                    idx,
                    max(1, total),
                )
            pixmap = QPixmap(source_path)
            if pixmap.isNull():
                generated.append(source_path)
                continue
            if not self._draw_publish_copyright(pixmap, copyright_text):
                generated.append(source_path)
                continue
            suffix = Path(source_path).suffix.lower()
            image_format = "PNG" if suffix == ".png" else "JPEG"
            out_suffix = ".png" if image_format == "PNG" else ".jpg"
            out_path = temp_dir / f"copyright_{idx:03d}{out_suffix}"
            saved = pixmap.save(str(out_path), image_format, 92 if image_format == "JPEG" else -1)
            generated.append(str(out_path) if saved else source_path)
        return generated

    def _publish_render_preferences(self) -> dict:
        parent = self.window()
        show_overlays = self._publish_option_enabled(self.SETTING_INCLUDE_ANNOTATIONS, default=False)
        show_labels = show_overlays
        show_scale_bar = False
        scale_bar_um = 10.0

        scale_toggle = getattr(parent, "show_scale_bar_checkbox", None)
        scale_default = False
        if scale_toggle is not None and hasattr(scale_toggle, "isChecked"):
            scale_default = bool(scale_toggle.isChecked())
        show_scale_bar = show_overlays and self._publish_option_enabled(
            self.SETTING_SHOW_SCALE_BAR,
            default=scale_default,
        )

        scale_spin = getattr(parent, "scale_size_spin", None)
        if scale_spin is not None and hasattr(scale_spin, "value"):
            try:
                scale_bar_um = float(scale_spin.value())
            except Exception:
                scale_bar_um = 10.0

        if scale_bar_um <= 0:
            scale_bar_um = 10.0

        return {
            "show_overlays": show_overlays,
            "show_labels": show_labels,
            "show_scale_bar": show_scale_bar,
            "scale_bar_um": scale_bar_um,
        }

    @staticmethod
    def _snap_publish_scale_bar_um(value_um: float) -> float:
        """Snap publish scale-bar length to human-friendly increments."""
        value_um = max(0.1, float(value_um))
        multipliers = (1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0)
        candidates: list[float] = []
        for exp in range(-1, 7):  # 0.1 up to 6,000,000
            base = 10.0 ** exp
            for mul in multipliers:
                candidate = mul * base
                if 0.1 <= candidate <= 6_000_000.0:
                    candidates.append(candidate)
        if not candidates:
            return value_um
        return min(candidates, key=lambda candidate: abs(candidate - value_um))

    def _publish_scale_bar_for_image(
        self,
        image_row: dict,
        pixmap: QPixmap,
        parent,
        fallback_um: float,
    ) -> tuple[float | None, str]:
        """Return publish scale-bar length in µm and unit label for one image."""
        try:
            mpp_value = float(image_row.get("scale_microns_per_pixel") or 0.0)
        except Exception:
            mpp_value = 0.0
        if mpp_value <= 0 or pixmap is None or pixmap.isNull():
            return None, "\u03bcm"

        image_type = (image_row.get("image_type") or "").strip().lower()
        objective_name = (image_row.get("objective_name") or "").strip()
        objective_key = resolve_objective_key(objective_name, load_objectives()) or objective_name
        has_micro_override = False
        override_um = None

        # Preserve Measure-tab user override/basis logic when available.
        if (
            image_type == "microscope"
            and parent is not None
            and hasattr(parent, "_scale_bar_micro_manual_by_objective")
            and hasattr(parent, "_suggest_microscope_scale_bar_um_for_objective")
        ):
            manual_map = getattr(parent, "_scale_bar_micro_manual_by_objective", {}) or {}
            basis_key = str(getattr(parent, "_scale_bar_micro_basis_objective", "") or "").strip()
            has_micro_override = bool(manual_map) or bool(basis_key)
            if has_micro_override:
                try:
                    override_um = float(parent._suggest_microscope_scale_bar_um_for_objective(objective_key))
                except Exception:
                    override_um = None

        if (
            image_type == "field"
            and parent is not None
            and hasattr(parent, "_scale_bar_overlay_field_mm")
        ):
            try:
                is_manual = bool(getattr(parent, "_scale_bar_overlay_field_is_manual", False))
                if is_manual:
                    mm_value = float(getattr(parent, "_scale_bar_overlay_field_mm"))
                    if mm_value > 0:
                        return mm_value * 1000.0, "mm"
            except Exception:
                pass

        if override_um and override_um > 0:
            return float(self._snap_publish_scale_bar_um(override_um)), "\u03bcm"

        # Auto default: target roughly 10% of image width in physical units.
        auto_um = float(pixmap.width()) * float(mpp_value) * 0.10
        if auto_um <= 0:
            auto_um = float(fallback_um or 10.0)
        return float(self._snap_publish_scale_bar_um(auto_um)), ("\u03bcm" if image_type != "field" else "mm")

    @staticmethod
    def _measurement_rectangle_from_lines(line1: list[float], line2: list[float]) -> list[QPointF] | None:
        p1 = QPointF(float(line1[0]), float(line1[1]))
        p2 = QPointF(float(line1[2]), float(line1[3]))
        p3 = QPointF(float(line2[0]), float(line2[1]))
        p4 = QPointF(float(line2[2]), float(line2[3]))

        length_vec = p2 - p1
        length_len = ((length_vec.x() ** 2) + (length_vec.y() ** 2)) ** 0.5
        width_vec = p4 - p3
        width_len = ((width_vec.x() ** 2) + (width_vec.y() ** 2)) ** 0.5
        if length_len < 0.001 or width_len < 0.001:
            return None

        length_dir = QPointF(length_vec.x() / length_len, length_vec.y() / length_len)
        width_dir = QPointF(-length_dir.y(), length_dir.x())

        line1_mid = QPointF((p1.x() + p2.x()) / 2.0, (p1.y() + p2.y()) / 2.0)
        line2_mid = QPointF((p3.x() + p4.x()) / 2.0, (p3.y() + p4.y()) / 2.0)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2.0, (line1_mid.y() + line2_mid.y()) / 2.0)

        half_length = length_len / 2.0
        half_width = width_len / 2.0

        return [
            QPointF(
                center.x() - width_dir.x() * half_width - length_dir.x() * half_length,
                center.y() - width_dir.y() * half_width - length_dir.y() * half_length,
            ),
            QPointF(
                center.x() + width_dir.x() * half_width - length_dir.x() * half_length,
                center.y() + width_dir.y() * half_width - length_dir.y() * half_length,
            ),
            QPointF(
                center.x() + width_dir.x() * half_width + length_dir.x() * half_length,
                center.y() + width_dir.y() * half_width + length_dir.y() * half_length,
            ),
            QPointF(
                center.x() - width_dir.x() * half_width + length_dir.x() * half_length,
                center.y() - width_dir.y() * half_width + length_dir.y() * half_length,
            ),
        ]

    @staticmethod
    def _measurement_unit_for_image(image_type: str | None) -> tuple[str, float]:
        if (image_type or "").strip().lower() == "field":
            return "mm", 1000.0
        return "\u03bcm", 1.0

    def _build_publish_overlays_for_image(
        self,
        image_row: dict,
    ) -> tuple[list[list[float]], list[list[QPointF]], list[dict]]:
        image_id = image_row.get("id")
        if not image_id:
            return [], [], []

        try:
            mpp = float(image_row.get("scale_microns_per_pixel") or 0.0)
        except Exception:
            mpp = 0.0
        if mpp <= 0:
            mpp = 0.5

        unit, divisor = self._measurement_unit_for_image(image_row.get("image_type"))
        measurements = MeasurementDB.get_measurements_for_image(int(image_id))
        single_lines: list[list[float]] = []
        rectangles: list[list[QPointF]] = []
        labels: list[dict] = []

        for measurement in measurements:
            if not all(
                measurement.get(f"p{i}_{axis}") is not None
                for i in range(1, 3)
                for axis in ("x", "y")
            ):
                continue
            line1 = [
                float(measurement["p1_x"]),
                float(measurement["p1_y"]),
                float(measurement["p2_x"]),
                float(measurement["p2_y"]),
            ]
            has_line2 = all(
                measurement.get(f"p{i}_{axis}") is not None
                for i in range(3, 5)
                for axis in ("x", "y")
            )
            line2 = None
            if has_line2:
                line2 = [
                    float(measurement["p3_x"]),
                    float(measurement["p3_y"]),
                    float(measurement["p4_x"]),
                    float(measurement["p4_y"]),
                ]

            length_um = measurement.get("length_um")
            width_um = measurement.get("width_um")
            if has_line2 and line2 and (length_um is None or width_um is None):
                len1 = (((line1[2] - line1[0]) ** 2) + ((line1[3] - line1[1]) ** 2)) ** 0.5
                len2 = (((line2[2] - line2[0]) ** 2) + ((line2[3] - line2[1]) ** 2)) ** 0.5
                length_um = max(len1, len2) * mpp
                width_um = min(len1, len2) * mpp
            elif not has_line2 and length_um is None:
                len1 = (((line1[2] - line1[0]) ** 2) + ((line1[3] - line1[1]) ** 2)) ** 0.5
                length_um = len1 * mpp

            if has_line2 and line2:
                corners = self._measurement_rectangle_from_lines(line1, line2)
                if corners:
                    rectangles.append(corners)
                if length_um is not None and width_um is not None:
                    center = QPointF(
                        (line1[0] + line1[2] + line2[0] + line2[2]) / 4.0,
                        (line1[1] + line1[3] + line2[1] + line2[3]) / 4.0,
                    )
                    labels.append(
                        {
                            "id": measurement.get("id"),
                            "center": center,
                            "length_um": float(length_um),
                            "width_um": float(width_um),
                            "length_value": float(length_um) / divisor,
                            "width_value": float(width_um) / divisor,
                            "unit": unit,
                            "line1": line1,
                            "line2": line2,
                        }
                    )
            else:
                single_lines.append(line1)
                if length_um is not None:
                    labels.append(
                        {
                            "id": measurement.get("id"),
                            "kind": "line",
                            "center": QPointF((line1[0] + line1[2]) / 2.0, (line1[1] + line1[3]) / 2.0),
                            "length_um": float(length_um),
                            "length_value": float(length_um) / divisor,
                            "unit": unit,
                            "line": line1,
                        }
                    )

        return single_lines, rectangles, labels

    def _generate_publish_annotated_images(
        self,
        observation_id: int,
        base_image_paths: list[str],
        temp_dir: Path,
        progress_cb=None,
        cancel_cb=None,
    ) -> list[str]:
        if not base_image_paths:
            return []

        parent = self.window()
        preferences = self._publish_render_preferences()
        images = ImageDB.get_images_for_observation(observation_id)
        by_path_key: dict[str, dict] = {}
        for image_row in images:
            for key in ("filepath", "original_filepath"):
                row_path = image_row.get(key)
                path_key = self._publish_path_key(row_path)
                if path_key:
                    by_path_key[path_key] = image_row

        generated: list[str] = []
        total_images = len(base_image_paths)
        for idx, source_path in enumerate(base_image_paths, start=1):
            if cancel_cb:
                cancel_cb()
            if progress_cb:
                progress_cb(
                    self.tr("Preparing annotated image {current}/{total}...").format(
                        current=idx,
                        total=total_images,
                    ),
                    idx,
                    max(1, total_images),
                )
            source_key = self._publish_path_key(source_path)
            image_row = by_path_key.get(source_key)
            if not image_row:
                continue
            pixmap = QPixmap(source_path)
            if pixmap.isNull():
                continue

            widget = ZoomableImageLabel()
            widget.set_image(pixmap)
            orig_pixels = max(1, pixmap.width() * pixmap.height())
            # Artsobs commonly downsizes uploads to around 2 MP. Increase exported
            # measurement label text so it remains readable after that resize.
            # Slightly reduce publish-only measurement label scaling after recent tuning.
            publish_measure_label_scale = max(
                1.0,
                min(4.0, ((orig_pixels / 2_000_000.0) ** 0.5) * 0.85),
            )
            widget.set_export_measure_label_scale_multiplier(publish_measure_label_scale)
            measure_color = (image_row.get("measure_color") or "").strip()
            if measure_color:
                widget.set_measurement_color(QColor(measure_color))

            lines, rectangles, labels = self._build_publish_overlays_for_image(image_row)
            show_overlays = bool(preferences["show_overlays"]) and bool(lines or rectangles)
            show_labels = show_overlays and bool(preferences["show_labels"]) and bool(labels)
            widget.set_show_measure_overlays(show_overlays)
            widget.set_show_measure_labels(show_labels)
            widget.set_measurement_lines(lines if show_overlays else [])
            widget.set_measurement_rectangles(rectangles if show_overlays else [])
            widget.set_measurement_labels(labels if show_labels else [])

            has_scale_bar = False
            if preferences["show_scale_bar"]:
                try:
                    mpp_value = float(image_row.get("scale_microns_per_pixel") or 0.0)
                except Exception:
                    mpp_value = 0.0
                if mpp_value > 0:
                    scale_bar_um, unit = self._publish_scale_bar_for_image(
                        image_row=image_row,
                        pixmap=pixmap,
                        parent=parent,
                        fallback_um=float(preferences["scale_bar_um"]),
                    )
                    if scale_bar_um and scale_bar_um > 0:
                        widget.set_microns_per_pixel(mpp_value)
                        widget.set_scale_bar(True, float(scale_bar_um), unit=unit)
                    has_scale_bar = True

            if not show_overlays and not has_scale_bar:
                continue

            exported = widget.export_annotated_pixmap()
            if not exported or exported.isNull():
                continue
            out_path = temp_dir / f"annotated_{idx:03d}.jpg"
            if exported.save(str(out_path), "JPEG", 92):
                generated.append(str(out_path))

        return generated

    @staticmethod
    def _normalize_publish_measurement_category(category: str | None) -> str:
        text = (category or "").strip().lower()
        if not text:
            return "spores"
        if text in {"spore", "spores", "manual"}:
            return "spores"
        return text

    def _filter_publish_measurements(self, measurements: list[dict], category: str | None) -> list[dict]:
        filtered = [
            m
            for m in (measurements or [])
            if self._normalize_publish_measurement_category(m.get("measurement_type")) != "calibration"
        ]
        normalized = (category or "all").strip().lower()
        if not normalized or normalized == "all":
            return filtered
        if normalized in {"spore", "spores"}:
            return [
                m
                for m in filtered
                if self._normalize_publish_measurement_category(m.get("measurement_type")) == "spores"
            ]
        return [
            m
            for m in filtered
            if self._normalize_publish_measurement_category(m.get("measurement_type")) == normalized
        ]

    @staticmethod
    def _publish_measurement_has_overlay(measurement: dict | None) -> bool:
        if not isinstance(measurement, dict):
            return False
        return all(
            measurement.get(f"p{i}_{axis}") is not None
            for i in range(1, 3)
            for axis in ("x", "y")
        )

    @staticmethod
    def _publish_measurement_has_plot_values(measurement: dict | None) -> bool:
        if not isinstance(measurement, dict):
            return False
        try:
            length = float(measurement.get("length_um"))
            width = float(measurement.get("width_um"))
        except (TypeError, ValueError):
            return False
        return length > 0 and width > 0

    @staticmethod
    def _publish_measurement_has_gallery_data(measurement: dict | None) -> bool:
        if not isinstance(measurement, dict):
            return False
        if not all(
            measurement.get(f"p{i}_{axis}") is not None
            for i in range(1, 5)
            for axis in ("x", "y")
        ):
            return False
        image_path = str(measurement.get("image_filepath") or "").strip()
        return bool(image_path) and Path(image_path).exists()

    def _publish_measurement_availability(
        self,
        observation_id: int,
        base_image_paths: list[str],
    ) -> dict[str, object]:
        all_measurements = self._filter_publish_measurements(
            MeasurementDB.get_measurements_for_observation(observation_id),
            "all",
        )
        settings = self._load_gallery_settings_for_observation(observation_id)
        category = settings.get("measurement_type", "all")
        category_measurements = self._filter_publish_measurements(all_measurements, category)

        selected_image_ids: set[int] = set()
        base_keys = {
            self._publish_path_key(path)
            for path in (base_image_paths or [])
            if path
        }
        if base_keys:
            for image_row in ImageDB.get_images_for_observation(observation_id):
                image_id = image_row.get("id")
                if image_id is None:
                    continue
                row_keys = {
                    self._publish_path_key(image_row.get("filepath")),
                    self._publish_path_key(image_row.get("original_filepath")),
                }
                row_keys.discard("")
                if row_keys & base_keys:
                    try:
                        selected_image_ids.add(int(image_id))
                    except Exception:
                        pass

        has_overlay_measurements = any(
            self._publish_measurement_has_overlay(measurement)
            and measurement.get("image_id") is not None
            and str(measurement.get("image_id")).isdigit()
            and int(measurement.get("image_id")) in selected_image_ids
            for measurement in all_measurements
        )
        plot_measurements = [
            measurement
            for measurement in category_measurements
            if self._publish_measurement_has_plot_values(measurement)
        ]
        has_gallery_measurements = any(
            self._publish_measurement_has_gallery_data(measurement)
            for measurement in category_measurements
        )
        spore_stats = MeasurementDB.get_statistics_for_observation(
            observation_id,
            measurement_category="spores",
        )
        return {
            "has_any_measurements": bool(all_measurements),
            "has_overlay_measurements": has_overlay_measurements,
            "has_plot_measurements": bool(plot_measurements),
            "has_gallery_measurements": has_gallery_measurements,
            "spore_stats": spore_stats,
        }

    def _publish_spore_stats_text(self, observation_id: int, obs: dict, spore_stats: dict | None = None) -> str:
        stats_text = self._localize_spore_stats_for_publish(obs.get("spore_statistics"))
        if stats_text:
            return stats_text
        stats = spore_stats if isinstance(spore_stats, dict) else MeasurementDB.get_statistics_for_observation(
            observation_id,
            measurement_category="spores",
        )
        if not stats:
            return ""
        parent = self.window()
        formatter = getattr(parent, "format_literature_string", None)
        if callable(formatter):
            try:
                return self._localize_spore_stats_for_publish(formatter(stats))
            except Exception:
                pass
        return ""

    @staticmethod
    def _load_gallery_settings_for_observation(observation_id: int) -> dict:
        import json

        raw = SettingsDB.get_setting(f"gallery_settings_{observation_id}")
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _publish_confidence_ellipse_points(x, y, confidence: float = 0.95, n_points: int = 300):
        import numpy as np
        import math

        if x is None or y is None or len(x) < 3 or len(y) < 3:
            return None

        mean = np.array([np.mean(x), np.mean(y)])
        if confidence == 0.95:
            chi2_val = 5.991464547107979
        else:
            chi2_val = -2.0 * math.log(max(1e-6, 1.0 - float(confidence)))
        cov = np.cov(x, y, ddof=1)
        eigvals, eigvecs = np.linalg.eigh(cov)
        order = np.argsort(eigvals)[::-1]
        eigvals = eigvals[order]
        eigvecs = eigvecs[:, order]
        axis_lengths = np.sqrt(np.maximum(eigvals, 0) * chi2_val)
        t = np.linspace(0, 2 * math.pi, n_points)
        circle = np.vstack((np.cos(t), np.sin(t)))
        ellipse = (eigvecs @ (axis_lengths[:, None] * circle)) + mean[:, None]
        return ellipse[0, :], ellipse[1, :]

    @staticmethod
    def _quantize_png8(path: Path) -> None:
        try:
            with Image.open(path) as img:
                img8 = img.convert("P", palette=Image.ADAPTIVE, colors=256)
                img8.save(path, format="PNG", optimize=True)
        except Exception:
            return

    def _generate_publish_measure_plot_image(
        self,
        observation_id: int,
        temp_dir: Path,
        progress_cb=None,
        cancel_cb=None,
    ) -> str | None:
        import numpy as np
        from matplotlib.figure import Figure

        if cancel_cb:
            cancel_cb()
        if progress_cb:
            progress_cb(self.tr("Preparing measure plot image..."), 1, 3)

        out_path = temp_dir / "measure_plot.png"
        parent = self.window()
        export_plot = getattr(parent, "export_publish_measure_plot_png", None)
        if callable(export_plot):
            if cancel_cb:
                cancel_cb()
            if progress_cb:
                progress_cb(self.tr("Rendering measure plot image..."), 3, 3)
            try:
                if bool(export_plot(observation_id, out_path)):
                    self._quantize_png8(out_path)
                    return str(out_path)
            except Exception:
                pass

        settings = self._load_gallery_settings_for_observation(observation_id)
        category = settings.get("measurement_type", "all")
        try:
            bins_value = int(settings.get("bins", 8))
        except (TypeError, ValueError):
            bins_value = 8
        plot_settings = {
            "bins": bins_value,
            "histogram": bool(settings.get("histogram", True)),
            "ci": bool(settings.get("ci", True)),
            "avg_q": bool(settings.get("avg_q", False)),
            "q_minmax": bool(settings.get("q_minmax", False)),
            "axis_equal": bool(settings.get("axis_equal", False)),
        }

        measurements = MeasurementDB.get_measurements_for_observation(observation_id)
        measurements = self._filter_publish_measurements(measurements, category)
        lengths: list[float] = []
        widths: list[float] = []
        if progress_cb:
            progress_cb(self.tr("Collecting measurement data..."), 2, 3)
        for measurement in measurements:
            if cancel_cb:
                cancel_cb()
            length = measurement.get("length_um")
            width = measurement.get("width_um")
            try:
                length_f = float(length)
                width_f = float(width)
            except (TypeError, ValueError):
                continue
            if width_f <= 0:
                continue
            lengths.append(length_f)
            widths.append(width_f)
        if not lengths:
            return None

        L = np.asarray(lengths)
        W = np.asarray(widths)
        Q = L / W
        show_hist = plot_settings["histogram"]
        show_ci = plot_settings["ci"]
        show_avg_q = plot_settings["avg_q"]
        show_q_minmax = plot_settings["q_minmax"]
        axis_equal = plot_settings["axis_equal"]
        bins = max(3, plot_settings["bins"])

        # Wider export canvas so the scatter panel and histogram labels do not collide.
        fig = Figure(figsize=(10.5, 6.0), dpi=140)
        if show_hist:
            gs = fig.add_gridspec(3, 2, width_ratios=[3.8, 1.2], hspace=0.7, wspace=0.30)
            ax_scatter = fig.add_subplot(gs[:, 0])
            ax_len = fig.add_subplot(gs[0, 1])
            ax_wid = fig.add_subplot(gs[1, 1])
            ax_q = fig.add_subplot(gs[2, 1])
        else:
            gs = fig.add_gridspec(1, 1)
            ax_scatter = fig.add_subplot(gs[0, 0])
            ax_len = None
            ax_wid = None
            ax_q = None
        fig.subplots_adjust(left=0.08, right=0.98, top=0.97, bottom=0.11)

        hist_color = "#3498db"
        ax_scatter.scatter(L, W, s=20, alpha=0.85, color=hist_color)
        ax_scatter.set_xlabel(self.tr("Length (\u03bcm)"))
        ax_scatter.set_ylabel(self.tr("Width (\u03bcm)"))

        min_len = float(np.min(L))
        max_len = float(np.max(L))
        min_w = float(np.min(W))
        max_w = float(np.max(W))

        if show_avg_q:
            avg_q = float(np.mean(Q))
            line_w = np.array([min_w, max_w], dtype=float)
            line_l = avg_q * line_w
            ax_scatter.plot(line_l, line_w, linestyle="--", color="#7f8c8d", linewidth=1.2)

        if show_q_minmax:
            q_min = float(np.min(Q))
            q_max = float(np.max(Q))
            if q_min > 0:
                start_x = max(min_len, min_w * q_min)
                if start_x < max_len:
                    end_y = min(max_w, max_len / q_min)
                    end_x = min(max_len, end_y * q_min)
                    ax_scatter.plot([start_x, end_x], [start_x / q_min, end_x / q_min], color="black", linewidth=1.0)
            if q_max > 0:
                start_x = max(min_len, min_w * q_max)
                if start_x < max_len:
                    end_y = min(max_w, max_len / q_max)
                    end_x = min(max_len, end_y * q_max)
                    ax_scatter.plot([start_x, end_x], [start_x / q_max, end_x / q_max], color="black", linewidth=1.0)

        if show_ci and len(L) >= 3:
            ellipse = self._publish_confidence_ellipse_points(L, W, confidence=0.95)
            if ellipse is not None:
                ex, ey = ellipse
                ax_scatter.plot(ex, ey, color=hist_color, linewidth=1.5)
        if axis_equal:
            ax_scatter.set_aspect("equal", adjustable="box")
        else:
            ax_scatter.set_aspect("auto")

        if show_hist and ax_len and ax_wid and ax_q:
            ax_len.hist(L, bins=np.histogram_bin_edges(L, bins=bins), color=hist_color)
            ax_wid.hist(W, bins=np.histogram_bin_edges(W, bins=bins), color=hist_color)
            ax_q.hist(Q, bins=np.histogram_bin_edges(Q, bins=bins), color=hist_color)
            ax_len.set_ylabel("Count")
            ax_wid.set_ylabel("Count")
            ax_q.set_ylabel("Count")
            ax_len.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax_wid.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax_q.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax_q.set_xlabel("Q")
            ax_len.set_title(self.tr("Length"))
            ax_wid.set_title(self.tr("Width"))
            ax_q.set_title("Q")

        if cancel_cb:
            cancel_cb()
        if progress_cb:
            progress_cb(self.tr("Rendering measure plot image..."), 3, 3)
        fig.savefig(out_path, format="png", dpi=140)
        fig.clear()
        self._quantize_png8(out_path)
        return str(out_path)

    def _generate_publish_gallery_mosaic_image(
        self,
        observation_id: int,
        temp_dir: Path,
        progress_cb=None,
        cancel_cb=None,
    ) -> str | None:
        parent = self.window()
        create_thumbnail = getattr(parent, "create_spore_thumbnail", None)
        if not callable(create_thumbnail):
            return None

        if cancel_cb:
            cancel_cb()
        if progress_cb:
            progress_cb(self.tr("Preparing thumbnail gallery image..."), 1, 3)

        settings = self._load_gallery_settings_for_observation(observation_id)
        category = settings.get("measurement_type", "all")
        orient = bool(settings.get("orient", True))
        uniform_scale = bool(settings.get("uniform_scale", False))
        measurements = MeasurementDB.get_measurements_for_observation(observation_id)
        measurements = self._filter_publish_measurements(measurements, category)
        valid_measurements = [
            m
            for m in measurements
            if all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y"))
            and m.get("image_filepath")
            and Path(m.get("image_filepath")).exists()
        ]
        if not valid_measurements:
            return None

        thumbnail_size = 220
        size_fn = getattr(parent, "_gallery_thumbnail_size", None)
        if callable(size_fn):
            try:
                thumbnail_size = max(120, int(size_fn()))
            except Exception:
                thumbnail_size = 220

        image_rows = {int(row["id"]): row for row in ImageDB.get_images_for_observation(observation_id)}
        default_color = getattr(parent, "default_measure_color", QColor(52, 152, 219))

        uniform_length_um = None
        if uniform_scale:
            for measurement in valid_measurements:
                length_um = measurement.get("length_um")
                try:
                    length_f = float(length_um)
                except (TypeError, ValueError):
                    continue
                if uniform_length_um is None or length_f > uniform_length_um:
                    uniform_length_um = length_f

        thumbnails: list[QPixmap] = []
        total_items = len(valid_measurements)
        if progress_cb:
            progress_cb(
                self.tr("Rendering thumbnail gallery {current}/{total}...").format(
                    current=0,
                    total=total_items,
                ),
                2,
                3,
            )
        for measurement in valid_measurements:
            if cancel_cb:
                cancel_cb()
            image_path = measurement.get("image_filepath")
            pixmap = QPixmap(image_path)
            if pixmap.isNull():
                continue

            points = [
                QPointF(float(measurement["p1_x"]), float(measurement["p1_y"])),
                QPointF(float(measurement["p2_x"]), float(measurement["p2_y"])),
                QPointF(float(measurement["p3_x"]), float(measurement["p3_y"])),
                QPointF(float(measurement["p4_x"]), float(measurement["p4_y"])),
            ]
            image_row = image_rows.get(int(measurement.get("image_id") or 0), {})
            mpp = image_row.get("scale_microns_per_pixel")
            uniform_length_px = None
            if uniform_scale and uniform_length_um:
                try:
                    mpp_value = float(mpp or 0.0)
                except Exception:
                    mpp_value = 0.0
                if mpp_value > 0:
                    uniform_length_px = float(uniform_length_um) / mpp_value

            measure_color = default_color
            custom_color = (image_row.get("measure_color") or "").strip()
            if custom_color:
                measure_color = QColor(custom_color)

            thumb = create_thumbnail(
                pixmap,
                points,
                measurement.get("length_um") or 0,
                measurement.get("width_um") or 0,
                thumbnail_size,
                0,
                orient=orient,
                extra_rotation=int(measurement.get("gallery_rotation") or 0),
                uniform_length_px=uniform_length_px,
                color=measure_color,
            )
            if thumb and not thumb.isNull():
                thumbnails.append(thumb)
            if progress_cb:
                progress_cb(
                    self.tr("Rendering thumbnail gallery {current}/{total}...").format(
                        current=len(thumbnails),
                        total=total_items,
                    ),
                    2,
                    3,
                )

        if not thumbnails:
            return None

        import math

        cols = max(1, int(math.ceil(math.sqrt(len(thumbnails)))))
        rows = int(math.ceil(len(thumbnails) / cols))
        spacing = 12
        canvas_w = cols * thumbnail_size + (cols + 1) * spacing
        canvas_h = rows * thumbnail_size + (rows + 1) * spacing

        canvas = QPixmap(canvas_w, canvas_h)
        canvas.fill(QColor("white"))
        if progress_cb:
            progress_cb(self.tr("Composing thumbnail gallery image..."), 3, 3)
        painter = QPainter(canvas)
        for idx, thumb in enumerate(thumbnails):
            if cancel_cb:
                cancel_cb()
            row = idx // cols
            col = idx % cols
            x = spacing + col * (thumbnail_size + spacing)
            y = spacing + row * (thumbnail_size + spacing)
            painter.drawPixmap(x, y, thumb)
        painter.end()

        out_path = temp_dir / "gallery_mosaic.png"
        if not canvas.save(str(out_path), "PNG"):
            return None
        return str(out_path)

    def _prepare_publish_media_assets(
        self,
        observation_id: int,
        base_image_paths: list[str],
        include_annotations: bool,
        include_measure_plots: bool,
        include_thumbnail_gallery: bool,
        include_copyright: bool = False,
        copyright_text: str | None = None,
        progress_cb=None,
        cancel_cb=None,
    ) -> tuple[list[str], Path | None, list[str]]:
        upload_paths = list(base_image_paths)
        warnings: list[str] = []
        if not (
            include_annotations
            or include_measure_plots
            or include_thumbnail_gallery
            or include_copyright
        ):
            return upload_paths, None, warnings

        if cancel_cb:
            cancel_cb()
        temp_dir = Path(tempfile.mkdtemp(prefix=f"sporely_publish_{observation_id}_"))
        generated_any = False
        if include_copyright and not copyright_text:
            warnings.append(
                self.tr("Watermark was skipped because profile name is missing.")
            )

        if include_annotations:
            try:
                if progress_cb:
                    progress_cb(self.tr("Preparing annotated images..."), 1, 3)
                annotated_paths = self._generate_publish_annotated_images(
                    observation_id=observation_id,
                    base_image_paths=base_image_paths,
                    temp_dir=temp_dir,
                    progress_cb=progress_cb,
                    cancel_cb=cancel_cb,
                )
            except UploadCancelledError:
                raise
            except Exception:
                annotated_paths = []
            if annotated_paths:
                upload_paths = annotated_paths
                generated_any = True
            else:
                warnings.append(
                    self.tr("No annotated images were generated; original images were used.")
                )

        if include_copyright and copyright_text:
            try:
                if progress_cb:
                    progress_cb(self.tr("Adding watermark..."), 1, 1)
                copyright_paths = self._generate_publish_copyright_images(
                    upload_paths,
                    temp_dir,
                    copyright_text,
                    progress_cb=progress_cb,
                    cancel_cb=cancel_cb,
                )
            except UploadCancelledError:
                raise
            except Exception:
                copyright_paths = []
            if copyright_paths:
                upload_paths = copyright_paths
                generated_any = True
            else:
                warnings.append(self.tr("Could not add watermark to images."))

        if include_measure_plots:
            try:
                if progress_cb:
                    progress_cb(self.tr("Preparing measure plot..."), 2, 3)
                plot_path = self._generate_publish_measure_plot_image(
                    observation_id,
                    temp_dir,
                    progress_cb=progress_cb,
                    cancel_cb=cancel_cb,
                )
            except UploadCancelledError:
                raise
            except Exception:
                plot_path = None
            if plot_path:
                upload_paths.append(plot_path)
                generated_any = True
            else:
                warnings.append(self.tr("Could not generate measure plot image."))

        if include_thumbnail_gallery:
            try:
                if progress_cb:
                    progress_cb(self.tr("Preparing thumbnail gallery..."), 3, 3)
                mosaic_path = self._generate_publish_gallery_mosaic_image(
                    observation_id,
                    temp_dir,
                    progress_cb=progress_cb,
                    cancel_cb=cancel_cb,
                )
            except UploadCancelledError:
                raise
            except Exception:
                mosaic_path = None
            if mosaic_path:
                upload_paths.append(mosaic_path)
                generated_any = True
            else:
                warnings.append(self.tr("Could not generate thumbnail gallery image."))

        if not generated_any:
            self._cleanup_publish_temp_dir(temp_dir)
            temp_dir = None
        elif progress_cb:
            progress_cb(self.tr("Media files prepared."), 1, 1)

        return upload_paths, temp_dir, warnings

    def _resolve_artsobs_taxon_id(self, obs: dict) -> int | None:
        taxon_pair = self._extract_artsobs_taxon_pair(obs)
        if not taxon_pair:
            return None
        genus, species = taxon_pair
        adb_taxon_id = ObservationDB.resolve_adb_taxon_id(genus, species)
        if not adb_taxon_id:
            accepted_pair = self._resolve_accepted_taxon_pair(genus, species)
            if accepted_pair:
                adb_taxon_id = ObservationDB.resolve_adb_taxon_id(*accepted_pair)
        return adb_taxon_id

    def _resolve_artportalen_taxon_id(self, obs: dict) -> int | None:
        taxon_pair = self._extract_artsobs_taxon_pair(obs)
        if not taxon_pair:
            return None
        genus, species = taxon_pair
        taxon_id = ObservationDB.resolve_external_taxon_id(genus, species, "artportalen")
        if not taxon_id:
            accepted_pair = self._resolve_accepted_taxon_pair(genus, species)
            if accepted_pair:
                taxon_id = ObservationDB.resolve_external_taxon_id(*accepted_pair, source_system="artportalen")
        return taxon_id

    def _preferred_publish_uploader_key(self, obs: dict, requested_uploader_key: str | None = None) -> str:
        requested_key = (requested_uploader_key or "").strip().lower()
        if requested_key in {"artportalen", "inat", "mo"}:
            return requested_key
        if requested_key in {"mobile", "web"}:
            return "web"
        return uploader_key_for_publish_target(self._observation_publish_target(obs))

    def _extract_artsobs_taxon_pair(self, obs: dict) -> tuple[str, str] | None:
        def _split_name(text: str | None) -> tuple[str, str] | None:
            normalized = self._normalize_taxon_text(text)
            if not normalized:
                return None
            parts = normalized.split()
            if len(parts) < 2:
                return None
            genus_part = parts[0]
            species_part = parts[1]
            if species_part.lower() in {"cf", "cf.", "aff", "aff.", "sp", "sp.", "spp", "spp."}:
                if len(parts) < 3:
                    return None
                species_part = parts[2]
            return genus_part, species_part

        genus = self._normalize_taxon_text(obs.get("genus"))
        species = self._normalize_taxon_text(obs.get("species"))

        if genus and species:
            species_parts = species.split()
            if len(species_parts) >= 2 and species_parts[0].lower() == genus.lower():
                species = species_parts[1]
            return genus, species

        if genus and not species:
            split = _split_name(genus)
            if split:
                return split

        if species and not genus:
            split = _split_name(species)
            if split:
                return split

        return _split_name(obs.get("species_guess"))

    def _resolve_accepted_taxon_pair(self, genus: str, species: str) -> tuple[str, str] | None:
        if not genus or not species:
            return None
        mapping = self._load_accepted_taxon_pair_cache()
        key = (genus.strip().lower(), species.strip().lower())
        resolved = mapping.get(key)
        if not resolved:
            return None
        if resolved[0].lower() == key[0] and resolved[1].lower() == key[1]:
            return None
        return resolved

    def _load_accepted_taxon_pair_cache(self) -> dict[tuple[str, str], tuple[str, str]]:
        if self._accepted_taxon_pair_cache is not None:
            return self._accepted_taxon_pair_cache

        mapping: dict[tuple[str, str], tuple[str, str]] = {}
        rows: list[tuple[str, str, str, str, str]] = []
        try:
            taxon_path = Path(__file__).resolve().parents[1] / "database" / "taxon.txt"
            if not taxon_path.exists():
                self._accepted_taxon_pair_cache = mapping
                return mapping

            with taxon_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                for row in reader:
                    if (row.get("taxonRank") or "").strip().lower() != "species":
                        continue
                    genus = self._normalize_taxon_text(row.get("genus"))
                    species = self._normalize_taxon_text(row.get("specificEpithet"))
                    if not genus or not species:
                        continue
                    taxon_id = (row.get("id") or "").strip()
                    accepted_id = (row.get("acceptedNameUsageID") or "").strip()
                    status = (row.get("taxonomicStatus") or "").strip().lower()
                    if not taxon_id:
                        continue
                    rows.append((taxon_id, accepted_id or taxon_id, genus, species, status))

            valid_by_id: dict[str, tuple[str, str]] = {}
            for taxon_id, _accepted_id, genus, species, status in rows:
                if status == "valid":
                    valid_by_id[taxon_id] = (genus, species)

            for taxon_id, accepted_id, genus, species, status in rows:
                key = (genus.lower(), species.lower())
                accepted_pair = valid_by_id.get(accepted_id)
                if accepted_pair:
                    mapping[key] = accepted_pair
                elif status == "valid":
                    mapping[key] = (genus, species)
        except Exception:
            mapping = {}

        self._accepted_taxon_pair_cache = mapping
        return mapping

    def upload_observation_to_artsobs(
        self,
        observation_id: int,
        uploader_key: str | None = None,
        show_status: bool = True,
        refresh_table: bool = True,
    ) -> tuple[bool, int | None, str | None]:
        def _fail(message: str, level: str = "error", auto_clear_ms: int = 12000):
            if show_status:
                self.set_status_message(message, level=level, auto_clear_ms=auto_clear_ms)
            return False, None, message

        try:
            from utils.artsobs_uploaders import get_uploader
        except Exception as exc:
            return _fail(self.tr("Upload unavailable: {error}").format(error=exc))

        obs = ObservationDB.get_observation(observation_id)
        if not obs:
            return _fail(self.tr("Upload failed: observation not found."))
        publish_target = self._observation_publish_target(obs)

        lat = obs.get("gps_latitude")
        lon = obs.get("gps_longitude")
        if lat is None or lon is None:
            return _fail(
                self.tr("Upload failed: this observation is missing GPS coordinates."),
                level="warning",
                auto_clear_ms=12000,
            )

        image_paths = self._collect_artsobs_image_paths(observation_id)
        include_spore_stats = self._publish_option_enabled(
            self.SETTING_INCLUDE_SPORE_STATS,
            default=True,
        )
        include_annotations = self._publish_option_enabled(
            self.SETTING_INCLUDE_ANNOTATIONS,
            default=False,
        )
        include_measure_plots = self._publish_option_enabled(
            self.SETTING_INCLUDE_MEASURE_PLOTS,
            default=False,
        )
        include_thumbnail_gallery = self._publish_option_enabled(
            self.SETTING_INCLUDE_THUMBNAIL_GALLERY,
            default=False,
        )
        include_copyright = self._publish_option_enabled(
            self.SETTING_INCLUDE_COPYRIGHT,
            default=False,
        )
        measurement_availability = self._publish_measurement_availability(
            observation_id,
            image_paths,
        )
        include_annotations = bool(include_annotations and measurement_availability["has_overlay_measurements"])
        include_spore_stats = bool(include_spore_stats and measurement_availability["spore_stats"])
        include_measure_plots = bool(include_measure_plots and measurement_availability["has_plot_measurements"])
        include_thumbnail_gallery = bool(
            include_thumbnail_gallery and measurement_availability["has_gallery_measurements"]
        )

        observed_datetime = obs.get("date")
        if not observed_datetime:
            return _fail(
                self.tr("Upload failed: observation date is missing."),
                level="warning",
                auto_clear_ms=12000,
            )
        image_license_code = self._publish_image_license_code()
        copyright_text = (
            self._publish_copyright_text(obs)
            if include_copyright
            else None
        )

        target_key = self._preferred_publish_uploader_key(obs, uploader_key)
        uploader = get_uploader(target_key)
        if not uploader:
            return _fail(
                self.tr("Upload failed: no uploader is configured for the selected target."),
                level="error",
            )
        if not self._uploader_matches_publish_target(uploader.key, publish_target):
            return _fail(
                self.tr(
                    "Upload failed: this observation is set to {target}, not {service}."
                ).format(
                    target=self.tr(publish_target_label(publish_target)),
                    service=self.tr(uploader.label),
                ),
                level="warning",
                auto_clear_ms=12000,
            )
        if self._observation_has_existing_upload(obs, uploader.key):
            return _fail(
                self.tr("Upload failed: this observation already has an ID in {service}.").format(
                    service=self.tr(uploader.label)
                ),
                level="warning",
                auto_clear_ms=12000,
            )
        if uploader.key in {"mobile", "web"}:
            base_image_count = len(image_paths)
            extra_image_count = int(bool(include_measure_plots)) + int(bool(include_thumbnail_gallery))
            total_image_count = base_image_count + extra_image_count
            if base_image_count > 10 or total_image_count > 10:
                if extra_image_count > 0:
                    warning_text = self.tr(
                        "Artsobservasjoner allows up to 10 images per observation. "
                        "You have {count} images, including plots and gallery images "
                        "(Settings - Online publishing)."
                    ).format(count=total_image_count)
                else:
                    warning_text = self.tr(
                        "Artsobservasjoner allows up to 10 images per observation. "
                        "You have {count} images."
                    ).format(count=base_image_count)
                return _fail(
                    warning_text,
                    level="warning",
                    auto_clear_ms=15000,
                )

        taxon_id = None
        cookies: dict = {}
        if uploader.key in {"mobile", "web"}:
            taxon_id = self._resolve_artsobs_taxon_id(obs)
            if not taxon_id:
                return _fail(
                    self.tr(
                        "Upload failed: could not resolve Artsobservasjoner taxon id from genus/species."
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            try:
                from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth
            except Exception as exc:
                return _fail(
                    self.tr("Upload failed: could not load Artsobservasjoner login helper ({error}).").format(error=exc),
                    level="error",
                    auto_clear_ms=12000,
                )
            auth = ArtsObservasjonerAuth()
            cookies = auth.ensure_valid_cookies(target="web") or {}
            if not cookies:
                return _fail(
                    self.tr("Not logged in to Artsobservasjoner (session expired and no saved credentials). Log in via Settings -> Online publishing."),
                    level="warning",
                    auto_clear_ms=12000,
                )
        elif uploader.key == "artportalen":
            taxon_id = self._resolve_artportalen_taxon_id(obs)
            if not taxon_id:
                return _fail(
                    self.tr(
                        "Upload failed: could not resolve an Artportalen taxon id from genus/species."
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            try:
                from utils.artportalen_auth import ArtportalenAuth
            except Exception as exc:
                return _fail(
                    self.tr("Upload failed: could not load Artportalen login helper ({error}).").format(error=exc),
                    level="error",
                    auto_clear_ms=12000,
                )
            cookies = ArtportalenAuth().ensure_valid_cookies() or {}
            if not cookies:
                return _fail(
                    self.tr("Not logged in to Artportalen. Log in via Settings -> Online publishing."),
                    level="warning",
                    auto_clear_ms=12000,
                )
        elif uploader.key == "inat":
            client_id = (SettingsDB.get_setting("inat_client_id", "") or "").strip() or (
                os.getenv("INAT_CLIENT_ID", "") or ""
            ).strip()
            client_secret = (SettingsDB.get_setting("inat_client_secret", "") or "").strip() or (
                os.getenv("INAT_CLIENT_SECRET", "") or ""
            ).strip()
            redirect_uri = (
                SettingsDB.get_setting("inat_redirect_uri", "http://localhost:8000/callback")
                or "http://localhost:8000/callback"
            )
            if not client_id or not client_secret:
                return _fail(
                    self.tr(
                        "Upload failed: missing iNaturalist CLIENT_ID/CLIENT_SECRET."
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            try:
                from utils.inat_oauth import INatOAuthClient

                token_file = (
                    app_data_dir() / "inaturalist_oauth_tokens.json"
                )
                oauth = INatOAuthClient(
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri,
                    token_file=token_file,
                )
                access_token = oauth.get_valid_access_token()
            except Exception as exc:
                return _fail(
                    self.tr("Upload failed: could not load iNaturalist OAuth helper ({error}).").format(error=exc),
                    level="error",
                    auto_clear_ms=12000,
                )
            if not access_token:
                return _fail(
                    self.tr("Not logged in to iNaturalist. Log in via Settings -> Online publishing."),
                    level="warning",
                    auto_clear_ms=12000,
                )
            cookies = {"access_token": access_token}
        elif uploader.key == "mo":
            app_key = (SettingsDB.get_setting(self.SETTING_MO_APP_API_KEY, "") or "").strip() or (
                os.getenv("MO_APP_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_APP_API_KEY", "") or ""
            ).strip()
            user_key = (SettingsDB.get_setting(self.SETTING_MO_USER_API_KEY, "") or "").strip() or (
                os.getenv("MO_USER_API_KEY", "") or ""
            ).strip() or (
                os.getenv("MUSHROOMOBSERVER_USER_API_KEY", "") or ""
            ).strip()
            if not app_key or not user_key:
                return _fail(
                    self.tr(
                        "Not logged in to Mushroom Observer. Log in via Settings -> Online publishing."
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            cookies = {
                "app_key": app_key,
                "user_key": user_key,
            }

        self._set_status_progress_visible(True)
        QApplication.processEvents()

        prepare_media_requested = bool(
            include_annotations
            or include_measure_plots
            or include_thumbnail_gallery
            or include_copyright
        )
        prepare_phase_max = 35 if prepare_media_requested else 0
        connect_phase_pct = prepare_phase_max + (5 if prepare_media_requested else 0)
        upload_phase_start = connect_phase_pct
        upload_phase_end = 97

        progress_tracker: dict[str, object] = {
            "overall_pct": 0,
            "prepare_pct": 5.0 if prepare_media_requested else 0.0,
            "last_prepare_text": "",
        }

        def _set_overall_publish_progress(status_text: str, pct: int) -> None:
            try:
                pct_i = int(pct)
            except Exception:
                pct_i = 0
            pct_i = max(0, min(100, pct_i))
            try:
                last_pct = int(progress_tracker.get("overall_pct", 0))
            except Exception:
                last_pct = 0
            pct_i = max(last_pct, pct_i)
            progress_tracker["overall_pct"] = pct_i
            self._set_status_progress(status_text, current=pct_i, total=100)

        def _update_prepare_phase_progress(
            status_text: str,
            current: int | None = None,
            total: int | None = None,
        ) -> None:
            text_key = (status_text or "").strip()
            if prepare_phase_max <= 0:
                progress_tracker["prepare_pct"] = 0.0
                _set_overall_publish_progress(status_text, 0)
                return
            try:
                prepare_pct = float(progress_tracker.get("prepare_pct", 5.0))
            except Exception:
                prepare_pct = 5.0
            if text_key and text_key != progress_tracker.get("last_prepare_text"):
                progress_tracker["last_prepare_text"] = text_key
                prepare_pct = min(float(prepare_phase_max), prepare_pct + 2.0)
            if total is not None and total > 0 and current is not None:
                try:
                    ratio = max(0.0, min(1.0, float(current) / float(max(1, total))))
                    prepare_pct = min(
                        float(prepare_phase_max),
                        max(prepare_pct, prepare_pct + (ratio * 1.2)),
                    )
                except Exception:
                    pass
            progress_tracker["prepare_pct"] = prepare_pct
            _set_overall_publish_progress(status_text, int(round(prepare_pct)))

        def ensure_not_cancelled() -> None:
            QApplication.processEvents()

        def update_progress(
            text: str,
            current: int | None = None,
            total: int | None = None,
            phase: str | None = None,
        ) -> None:
            ensure_not_cancelled()
            if phase == "prepare":
                _update_prepare_phase_progress(text, current=current, total=total)
            elif phase == "connect":
                _set_overall_publish_progress(text, connect_phase_pct)
            elif phase == "upload":
                if total is not None and total > 0 and current is not None:
                    try:
                        ratio = max(0.0, min(1.0, float(current) / float(max(1, total))))
                    except Exception:
                        ratio = 0.0
                    _set_overall_publish_progress(
                        text,
                        int(round(upload_phase_start + (ratio * (upload_phase_end - upload_phase_start)))),
                    )
                else:
                    _set_overall_publish_progress(text, max(upload_phase_start, 5))
            elif phase == "finalize":
                _set_overall_publish_progress(text, 99)
            else:
                # Fallback path: never move backwards.
                try:
                    current_pct = int(progress_tracker.get("overall_pct", 0))
                except Exception:
                    current_pct = 0
                _set_overall_publish_progress(text, current_pct)
            QApplication.processEvents()
            ensure_not_cancelled()

        update_progress(self.tr("Preparing upload..."), 0, 1, phase="prepare")

        def prepare_progress_cb(text: str, current: int | None = None, total: int | None = None) -> None:
            update_progress(text, current=current, total=total, phase="prepare")

        upload_image_paths = list(image_paths)
        publish_temp_dir: Path | None = None
        publish_warnings: list[str] = []
        try:
            update_progress(self.tr("Preparing media for upload..."), 0, 1, phase="prepare")

            (
                upload_image_paths,
                publish_temp_dir,
                publish_warnings,
            ) = self._prepare_publish_media_assets(
                observation_id=observation_id,
                base_image_paths=image_paths,
                include_annotations=include_annotations,
                include_measure_plots=include_measure_plots,
                include_thumbnail_gallery=include_thumbnail_gallery,
                include_copyright=include_copyright,
                copyright_text=copyright_text,
                progress_cb=prepare_progress_cb,
                cancel_cb=ensure_not_cancelled,
            )
            if uploader.key == "mobile" and not upload_image_paths:
                return _fail(
                    self.tr("Upload failed: no images are available for this observation."),
                    level="warning",
                    auto_clear_ms=12000,
                )

            spore_stats = self._publish_spore_stats_text(
                observation_id,
                obs,
                spore_stats=measurement_availability.get("spore_stats"),
            )
            legacy_notes = (obs.get("notes") or "").strip()
            open_comment = (obs.get("open_comment") or "").strip()
            private_comment = (obs.get("private_comment") or "").strip()
            interesting_comment = bool(obs.get("interesting_comment", 0))
            open_comment_parts = [part for part in [open_comment or legacy_notes] if part]
            if include_spore_stats and spore_stats:
                open_comment_parts.append(spore_stats)
            open_comment_text = "\n".join(open_comment_parts) if open_comment_parts else None
            observation_payload = {
                "taxon_id": taxon_id,
                "latitude": float(lat),
                "longitude": float(lon),
                "observed_datetime": observed_datetime,
                "count": 1,
                "comment": open_comment_text,
                "open_comment": open_comment_text,
                "private_comment": private_comment or None,
                "interesting_comment": interesting_comment,
                "accuracy_meters": obs.get("gps_accuracy") or 25,
                "site_name": (obs.get("location") or "").strip(),
                "habitat": (obs.get("habitat") or "").strip() or None,
                "notes": None,
                "uncertain": bool(obs.get("uncertain", 0)),
                "unspontaneous": bool(obs.get("unspontaneous", 0)),
                "determination_method": obs.get("determination_method"),
                "include_annotations_on_images": include_annotations,
                "include_spore_stats_in_comment": include_spore_stats,
                "include_measure_plots": include_measure_plots,
                "include_thumbnail_gallery": include_thumbnail_gallery,
                "include_copyright": include_copyright,
                "image_license_code": image_license_code,
                "genus": (obs.get("genus") or "").strip(),
                "species": (obs.get("species") or "").strip(),
                "species_guess": (obs.get("species_guess") or "").strip(),
                "inaturalist_taxon_id": obs.get("inaturalist_id"),
                "publish_target": publish_target,
                "habitat_nin2_path": obs.get("habitat_nin2_path"),
                "habitat_substrate_path": obs.get("habitat_substrate_path"),
                "habitat_nin2_note": (obs.get("habitat_nin2_note") or "").strip() or None,
                "habitat_substrate_note": (obs.get("habitat_substrate_note") or "").strip() or None,
                "habitat_grows_on_note": (obs.get("habitat_grows_on_note") or "").strip() or None,
                "habitat_host_scientific": " ".join(
                    [
                        (obs.get("habitat_host_genus") or "").strip(),
                        (obs.get("habitat_host_species") or "").strip(),
                    ]
                ).strip()
                or None,
                "habitat_host_common_name": (obs.get("habitat_host_common_name") or "").strip() or None,
                "habitat_host_taxon_id": ObservationDB.resolve_adb_taxon_id(
                    (obs.get("habitat_host_genus") or "").strip() or None,
                    (obs.get("habitat_host_species") or "").strip() or None,
                ),
            }
            update_progress(
                self.tr("Connecting to {target}...").format(target=self.tr(uploader.label)),
                0,
                1,
                phase="connect",
            )
            upload_state: dict[str, object] = {"result": None, "error": None}
            upload_finished = threading.Event()
            progress_events: SimpleQueue[tuple[str, int, int]] = SimpleQueue()

            def worker_progress_cb(text: str, current: int, total: int) -> None:
                try:
                    c = int(current)
                except Exception:
                    c = 0
                try:
                    t = int(total)
                except Exception:
                    t = 1
                progress_events.put((str(text), c, max(1, t)))

            def _upload_worker() -> None:
                try:
                    upload_state["result"] = uploader.upload(
                        observation_payload,
                        upload_image_paths,
                        cookies,
                        progress_cb=worker_progress_cb,
                    )
                except Exception as exc:
                    upload_state["error"] = exc
                finally:
                    upload_finished.set()

            worker = threading.Thread(
                target=_upload_worker,
                name=f"artsobs-upload-{observation_id}",
                daemon=True,
            )
            worker.start()

            while not upload_finished.is_set():
                QApplication.processEvents()
                while True:
                    try:
                        step_text, current_step, total_steps = progress_events.get_nowait()
                    except Empty:
                        break
                    update_progress(
                        self.tr("Uploading: {step}").format(step=self.tr(step_text)),
                        current_step,
                        total_steps,
                        phase="upload",
                    )
                time.sleep(0.05)

            while True:
                try:
                    step_text, current_step, total_steps = progress_events.get_nowait()
                except Empty:
                    break
                update_progress(
                    self.tr("Uploading: {step}").format(step=self.tr(step_text)),
                    current_step,
                    total_steps,
                    phase="upload",
                )
            final_progress_text = ""
            if hasattr(self, "status_progress_text"):
                try:
                    final_progress_text = self.status_progress_text.text()
                except Exception:
                    final_progress_text = ""
            update_progress(final_progress_text or self.tr("Preparing upload..."), 1, 1, phase="finalize")
            if upload_state["error"] is not None:
                raise upload_state["error"]  # type: ignore[misc]
            result = upload_state["result"]
        except UploadCancelledError:
            return _fail(
                self.tr("Upload cancelled."),
                level="warning",
                auto_clear_ms=8000,
            )
        except Exception as exc:
            return _fail(self.tr("Upload failed: {error}").format(error=exc))
        finally:
            self._set_status_progress_visible(False)
            self._set_status_progress("", 0, 1)
            self._cleanup_publish_temp_dir(publish_temp_dir)

        obs_id = None
        image_upload_error = None
        if result and getattr(result, "sighting_id", None):
            obs_id = result.sighting_id
        if result and getattr(result, "raw", None):
            image_upload_error = result.raw.get("image_upload_error")
        publish_warning_text = publish_warnings[0] if publish_warnings else None
        if obs_id:
            if uploader.key in {"mobile", "web"}:
                ObservationDB.update_observation(observation_id, artsdata_id=int(obs_id))
                if uploader.key == "web":
                    ImageDB.mark_observation_images_artsobs_web_uploaded(observation_id)
                self._artsobs_dead_by_observation_id[observation_id] = False
                # Newly sent observations are often still in review/preview on Artsobs.
                self._artsobs_public_published_by_observation_id[observation_id] = False
            elif uploader.key == "artportalen":
                ObservationDB.set_artportalen_id(observation_id, int(obs_id))
            elif uploader.key == "inat":
                ObservationDB.set_inaturalist_id(observation_id, int(obs_id))
            elif uploader.key == "mo":
                ObservationDB.set_mushroomobserver_id(observation_id, int(obs_id))
        if refresh_table:
            self.refresh_observations()
        elif obs_id:
            row = self._find_table_row_for_observation(observation_id)
            if row >= 0:
                if uploader.key in {"mobile", "web"}:
                    updated_obs = ObservationDB.get_observation(observation_id)
                    self._render_publish_cell(
                        row,
                        observation_id,
                        updated_obs.get("publish_target") if updated_obs else None,
                        updated_obs.get("artsdata_id") if updated_obs else int(obs_id),
                        updated_obs.get("artportalen_id") if updated_obs else None,
                    )
                elif uploader.key == "artportalen":
                    updated_obs = ObservationDB.get_observation(observation_id)
                    self._render_publish_cell(
                        row,
                        observation_id,
                        updated_obs.get("publish_target") if updated_obs else None,
                        updated_obs.get("artsdata_id") if updated_obs else None,
                        updated_obs.get("artportalen_id") if updated_obs else int(obs_id),
                    )
                self._update_publish_controls()
        if show_status:
            if obs_id:
                if image_upload_error or publish_warning_text:
                    details = []
                    if image_upload_error:
                        details.append(str(image_upload_error))
                    if publish_warning_text:
                        details.append(str(publish_warning_text))
                    self.set_status_message(
                        self.tr(
                            "Observation uploaded (ID {id}), but some publish assets had issues: {error}"
                        ).format(id=obs_id, error="; ".join(details)),
                        level="warning",
                        auto_clear_ms=15000,
                    )
                else:
                    self.set_status_message(
                        self.tr("Uploaded to {target} (ID {id}).").format(
                            target=self.tr(uploader.label),
                            id=obs_id,
                        ),
                        level="success",
                    )
            else:
                if publish_warning_text:
                    self.set_status_message(
                        self.tr("Upload completed with warnings: {warning}").format(
                            warning=publish_warning_text
                        ),
                        level="warning",
                        auto_clear_ms=15000,
                    )
                else:
                    self.set_status_message(self.tr("Upload completed."), level="success")
        return True, obs_id, None

    def edit_observation(self):
        """Edit the selected observation."""
        selected_rows = self.table.selectionModel().selectedRows()
        if len(selected_rows) != 1:
            return

        row = selected_rows[0].row()
        obs_id = self._observation_id_for_row(row)
        if obs_id is None:
            return
        observation = ObservationDB.get_observation(obs_id)
        if not observation:
            return

        obs_dt = _parse_observation_datetime(observation.get("date"))
        obs_lat = observation.get("gps_latitude")
        obs_lon = observation.get("gps_longitude")

        existing_images = ImageDB.get_images_for_observation(obs_id)
        image_results = self._build_import_results_from_images(existing_images)
        draft_observation = self._merge_observation_edit_draft(
            observation,
            self._observation_edit_draft_cache.get(obs_id),
        )

        ai_taxon = None
        ai_state = self._remap_ai_state_to_images(
            self._load_observation_ai_state(observation),
            image_results,
        )
        while True:
            dialog = ObservationDetailsDialog(
                self,
                observation=draft_observation,
                image_results=image_results,
                allow_edit_images=True,
                suggested_taxon=ai_taxon,
                ai_state=ai_state,
            )
            if dialog.exec():
                image_results = list(dialog.image_results)
                ai_state = dialog.get_ai_state()
                self._ai_suggestions_cache[obs_id] = ai_state
                data = dialog.get_data()
                self._observation_edit_draft_cache.pop(obs_id, None)
                ObservationDB.update_observation(
                    obs_id,
                    genus=data.get('genus'),
                    species=data.get('species'),
                    common_name=data.get('common_name'),
                    publish_target=data.get('publish_target'),
                    species_guess=data.get('species_guess'),
                    uncertain=1 if data.get('uncertain') else 0,
                    unspontaneous=1 if data.get('unspontaneous') else 0,
                    determination_method=data.get('determination_method'),
                    date=data.get('date'),
                    location=data.get('location'),
                    habitat=data.get('habitat'),
                    habitat_nin2_path=data.get('habitat_nin2_path'),
                    habitat_substrate_path=data.get('habitat_substrate_path'),
                    habitat_host_genus=data.get('habitat_host_genus'),
                    habitat_host_species=data.get('habitat_host_species'),
                    habitat_host_common_name=data.get('habitat_host_common_name'),
                    habitat_nin2_note=data.get('habitat_nin2_note'),
                    habitat_substrate_note=data.get('habitat_substrate_note'),
                    habitat_grows_on_note=data.get('habitat_grows_on_note'),
                    notes=None,
                    open_comment=data.get('open_comment'),
                    private_comment=data.get('private_comment'),
                    interesting_comment=1 if data.get('interesting_comment') else 0,
                    ai_state_json=self._serialize_ai_state(ai_state),
                    gps_latitude=data.get('gps_latitude'),
                    gps_longitude=data.get('gps_longitude'),
                    allow_nulls=True
                )

                self._apply_import_results_to_observation(
                    obs_id,
                    image_results,
                    existing_images=existing_images
                )

                self.refresh_observations()
                for row, obs in enumerate(ObservationDB.get_all_observations()):
                    if obs['id'] == obs_id:
                        self.table.selectRow(row)
                        self.selected_observation_id = obs_id
                        self.on_selection_changed()
                        break
                pending_status = self._upload_pending_artsobs_web_images()
                if pending_status == "none":
                    self.set_status_message(self.tr("Observation updated."), level="success")
                return

            if dialog.request_edit_images:
                data = dialog.get_data()
                self._observation_edit_draft_cache[obs_id] = dict(data)
                draft_observation = self._merge_observation_edit_draft(observation, data)
                draft_dt = _parse_observation_datetime(data.get("date"))
                if draft_dt and draft_dt.isValid():
                    obs_dt = draft_dt
                obs_lat = data.get("gps_latitude")
                obs_lon = data.get("gps_longitude")
                ai_state = dialog.get_ai_state()
                self._ai_suggestions_cache[obs_id] = ai_state
                _debug_import_flow(
                    f"edit observation {obs_id}: opening Prepare Images with {len(image_results)} images"
                )
                image_dialog = ImageImportDialog(
                    self,
                    import_results=image_results,
                    observation_datetime=obs_dt,
                    observation_lat=obs_lat,
                    observation_lon=obs_lon,
                    continue_to_observation_details=False,
                )
                if dialog.request_edit_images_path:
                    image_dialog.select_image_by_path(dialog.request_edit_images_path)
                if image_dialog.exec():
                    _debug_import_flow(
                        f"edit observation {obs_id}: Prepare Images accepted with {len(image_dialog.import_results)} images"
                    )
                    image_results = image_dialog.import_results
                    ai_taxon = image_dialog.get_ai_selected_taxon()
                    ai_state = self._remap_ai_state_to_images(ai_state, image_results)
                    obs_lat, obs_lon = image_dialog.get_observation_gps()
                    _debug_import_flow(
                        f"edit observation {obs_id}: metadata from Prepare Images gps=({obs_lat}, {obs_lon})"
                    )
                    ObservationDB.update_observation(
                        obs_id,
                        gps_latitude=obs_lat,
                        gps_longitude=obs_lon,
                        allow_nulls=True,
                    )
                    if observation is not None:
                        observation["gps_latitude"] = obs_lat
                        observation["gps_longitude"] = obs_lon
                    if draft_observation is not None:
                        draft_observation["gps_latitude"] = obs_lat
                        draft_observation["gps_longitude"] = obs_lon
                    _debug_import_flow(
                        f"edit observation {obs_id}: reopening observation dialog with {len(image_results)} images"
                    )
                continue
            ai_state = dialog.get_ai_state()
            self._ai_suggestions_cache[obs_id] = ai_state
            self._observation_edit_draft_cache.pop(obs_id, None)
            return

    def open_edit_images_direct(self, selected_image_path: str | None = None):
        """Open Prepare Images dialog directly for the selected observation.

        Skips ObservationDetailsDialog — useful when triggered via keyboard
        shortcut (Ctrl/Cmd+E or Alt+E) from the observations table.
        """
        selected_rows = self.table.selectionModel().selectedRows()
        if len(selected_rows) != 1:
            return
        row = selected_rows[0].row()
        obs_id = self._observation_id_for_row(row)
        if obs_id is None:
            return
        observation = ObservationDB.get_observation(obs_id)
        if not observation:
            return

        obs_dt  = _parse_observation_datetime(observation.get("date"))
        obs_lat = observation.get("gps_latitude")
        obs_lon = observation.get("gps_longitude")

        existing_images = ImageDB.get_images_for_observation(obs_id)
        image_results   = self._build_import_results_from_images(existing_images)

        image_dialog = ImageImportDialog(
            self,
            import_results=image_results,
            observation_datetime=obs_dt,
            observation_lat=obs_lat,
            observation_lon=obs_lon,
            continue_to_observation_details=False,
        )
        # Jump to requested image when provided, otherwise first image.
        if selected_image_path:
            image_dialog.select_image_by_path(selected_image_path)
        elif existing_images:
            first_path = existing_images[0].get("filepath")
            if first_path:
                image_dialog.select_image_by_path(first_path)

        if not image_dialog.exec():
            return

        image_results       = image_dialog.import_results
        obs_lat, obs_lon    = image_dialog.get_observation_gps()
        ObservationDB.update_observation(
            obs_id,
            gps_latitude=obs_lat,
            gps_longitude=obs_lon,
            allow_nulls=True,
        )
        self._apply_import_results_to_observation(obs_id, image_results, existing_images=existing_images)
        self.refresh_observations()
        for r, obs in enumerate(ObservationDB.get_all_observations()):
            if obs["id"] == obs_id:
                self.table.selectRow(r)
                self.selected_observation_id = obs_id
                self.on_selection_changed()
                break
        # Force Measure tab to reload updated scale/measurements without switching tabs
        self.set_selected_as_active(switch_tab=False)
        self.set_status_message(self.tr("Images updated."), level="success")

    def _on_gallery_image_double_clicked(self, _image_id, filepath: str) -> None:
        """Open Prepare Images from Observations-gallery double-click."""
        target_path = (filepath or "").strip() or None
        self.open_edit_images_direct(selected_image_path=target_path)

    def create_new_observation(self, initial_image_paths: list[str] | None = None):
        """Show dialog to create new observation."""
        image_results: list[ImageImportResult] = []
        primary_index = None
        draft_observation: dict | None = None
        ai_state: dict | None = None
        ai_taxon: dict | None = None
        while True:
            image_dialog = ImageImportDialog(
                self,
                image_paths=initial_image_paths if not image_results else None,
                import_results=image_results or None,
                continue_to_observation_details=True,
            )
            if not image_dialog.exec():
                return
            image_results = image_dialog.import_results
            primary_index = image_dialog.primary_index
            if ai_state:
                ai_state = self._remap_ai_state_to_images(ai_state, image_results)
            ai_taxon = image_dialog.get_ai_selected_taxon() or ai_taxon
            dialog = ObservationDetailsDialog(
                self,
                image_results=image_results,
                primary_index=primary_index,
                allow_edit_images=True,
                suggested_taxon=ai_taxon,
                ai_state=ai_state,
                draft_data=draft_observation,
            )
            if dialog.exec():
                obs_data = dialog.get_data()
                image_results = list(dialog.image_results)
                primary_index = dialog.primary_index
                ai_state = dialog.get_ai_state()
                draft_observation = dict(obs_data)
                profile = SettingsDB.get_profile()
                author = profile.get("name")
                if author:
                    obs_data["author"] = author

                if not self._confirm_duplicate_observation_creation(obs_data, image_results):
                    continue

                obs_id = ObservationDB.create_observation(**obs_data)
                if ai_state:
                    ObservationDB.update_observation(
                        obs_id,
                        ai_state_json=self._serialize_ai_state(ai_state),
                        allow_nulls=True,
                    )
                progress = None
                progress_cb = None
                total_images = len(image_results)
                if total_images:
                    progress = QProgressDialog(
                        self.tr("Processing images..."),
                        None,
                        0,
                        total_images,
                        self,
                    )
                    progress.setWindowTitle(self.tr("Processing Images"))
                    progress.setWindowModality(Qt.WindowModal)
                    progress.setAutoClose(True)
                    progress.setAutoReset(True)
                    progress.setCancelButton(None)
                    progress.setMinimumDuration(300)

                    def progress_cb(index, total, _result):
                        if total <= 0:
                            return
                        progress.setMaximum(total)
                        progress.setValue(index)
                        progress.setLabelText(
                            self.tr("Processing image {current}/{total}").format(
                                current=index,
                                total=total,
                            )
                        )
                        QApplication.processEvents()

                try:
                    self._apply_import_results_to_observation(
                        obs_id,
                        image_results,
                        progress_cb=progress_cb,
                    )
                finally:
                    if progress is not None:
                        progress.setValue(total_images)
                        progress.close()

                self.refresh_observations()
                for row, obs in enumerate(ObservationDB.get_all_observations()):
                    if obs['id'] == obs_id:
                        self.table.selectRow(row)
                        self.selected_observation_id = obs_id
                        self.on_selection_changed()
                        break
                self.set_status_message(self.tr("Observation created."), level="success")
                return

            if dialog.request_edit_images:
                draft_observation = dict(dialog.get_data())
                ai_state = dialog.get_ai_state()
                image_results = list(dialog.image_results)
                primary_index = dialog.primary_index
                continue
            return

    def export_for_ml(self):
        """Export annotations in COCO format for ML training."""
        def _get_default_export_dir() -> str:
            settings = get_app_settings()
            last_dir = settings.get("last_export_dir")
            if last_dir and Path(last_dir).exists():
                return last_dir
            docs = QStandardPaths.writableLocation(QStandardPaths.DocumentsLocation)
            if docs:
                return docs
            return str(Path.home())

        def _remember_export_dir(folderpath: str | None) -> None:
            if not folderpath:
                return
            update_app_settings({"last_export_dir": str(folderpath)})

        # Get export summary first
        summary = get_export_summary()

        if summary['total_annotations'] == 0:
            self.set_status_message(
                self.tr("No spore annotations to export. Measure spores first to create training data."),
                level="warning",
            )
            return

        self.set_status_message(self.tr("Select an output directory for ML export."), level="info")

        # Select output directory
        output_dir = QFileDialog.getExistingDirectory(
            self, "Select Output Directory for ML Dataset", _get_default_export_dir()
        )

        if not output_dir:
            self.set_status_message(self.tr("ML export cancelled."), level="info")
            return
        _remember_export_dir(output_dir)

        # Perform export
        try:
            stats = export_coco_format(output_dir)

            status_msg = self.tr(
                "Export complete. Images: {images}, annotations: {annotations}, skipped: {skipped}."
            ).format(
                images=stats['images_exported'],
                annotations=stats['annotations_exported'],
                skipped=stats['images_skipped'],
            )
            if stats['errors']:
                status_msg += " " + self.tr("Warnings: {count}.").format(count=len(stats['errors']))
            self.set_status_message(status_msg, level="success", auto_clear_ms=12000)

        except Exception as e:
            self.set_status_message(
                self.tr("Export failed: {error}").format(error=e),
                level="error",
                auto_clear_ms=12000,
            )

    def delete_selected_observation(self):
        """Delete the selected observation after confirmation."""
        selected_rows = self.table.selectionModel().selectedRows()
        if not selected_rows:
            return

        if len(selected_rows) == 1:
            row = selected_rows[0].row()
            obs_id = self._observation_id_for_row(row)
            species = self.table.item(row, 1).text()
            prompt = self.tr(
                "Delete observation '{species}'?\n\n"
                "This will also delete all associated images and measurements."
            ).format(species=species)
        else:
            obs_id = None
            prompt = self.tr(
                "Delete {count} observations?\n\n"
                "This will also delete all associated images and measurements."
            ).format(count=len(selected_rows))

        confirmed = self._question_yes_no(self.tr("Confirm Delete"), prompt, default_yes=False)
        if confirmed:
            failures: list[str] = []
            if obs_id is not None:
                failures.extend(ObservationDB.delete_observation(obs_id))
                self.observation_deleted.emit(obs_id)
            else:
                rows = [row.row() for row in selected_rows]
                obs_ids = [
                    obs_id
                    for r in rows
                    for obs_id in [self._observation_id_for_row(r)]
                    if obs_id is not None
                ]
                for obs_id in obs_ids:
                    failures.extend(ObservationDB.delete_observation(obs_id))
                    self.observation_deleted.emit(obs_id)
            failure_count = self._warn_delete_failures(failures)
            self.refresh_observations()
            if len(selected_rows) == 1 and failure_count:
                self.set_status_message(
                    self.tr("Observation deleted with {count} cleanup issue(s).").format(count=failure_count),
                    level="warning",
                    auto_clear_ms=12000,
                )
            elif len(selected_rows) == 1:
                self.set_status_message(self.tr("Observation deleted."), level="success")
            elif failure_count:
                self.set_status_message(
                    self.tr("Deleted {count} observations with {issues} cleanup issue(s).").format(
                        count=len(selected_rows),
                        issues=failure_count,
                    ),
                    level="warning",
                    auto_clear_ms=12000,
                )
            else:
                self.set_status_message(
                    self.tr("Deleted {count} observations.").format(count=len(selected_rows)),
                    level="success",
                )

    def _build_import_results_from_images(self, images: list[dict]) -> list[ImageImportResult]:
        objectives = load_objectives()
        results: list[ImageImportResult] = []
        missing_paths: list[str] = []
        for img in images:
            if not img:
                continue
            meta = {}
            filepath = img.get("filepath")
            if filepath:
                meta = get_image_metadata(filepath)
                if meta.get("missing"):
                    missing_paths.append(filepath)
            dt = meta.get("datetime")
            captured_at = QDateTime(dt) if dt else None
            exif_has_gps = meta.get("latitude") is not None or meta.get("longitude") is not None
            crop_x1 = img.get("ai_crop_x1")
            crop_y1 = img.get("ai_crop_y1")
            crop_x2 = img.get("ai_crop_x2")
            crop_y2 = img.get("ai_crop_y2")
            ai_crop_box = None
            if all(v is not None for v in (crop_x1, crop_y1, crop_x2, crop_y2)):
                ai_crop_box = (float(crop_x1), float(crop_y1), float(crop_x2), float(crop_y2))
            crop_w = img.get("ai_crop_source_w")
            crop_h = img.get("ai_crop_source_h")
            ai_crop_source_size = None
            if crop_w is not None and crop_h is not None:
                ai_crop_source_size = (int(crop_w), int(crop_h))
            gps_source = bool(img.get("gps_source")) if img.get("gps_source") is not None else False
            scale_value = img.get("scale_microns_per_pixel")
            objective_name = img.get("objective_name")
            resolved_key = resolve_objective_key(objective_name, objectives)
            custom_scale = None
            if scale_value is not None and (objective_name == "Custom" or not resolved_key):
                try:
                    custom_scale = float(scale_value)
                except (TypeError, ValueError):
                    custom_scale = None
            objective_value = resolved_key if resolved_key else (None if objective_name == "Custom" else objective_name)
            image_type = (img.get("image_type") or "field").strip().lower()
            resize_to_optimal = bool(SettingsDB.get_setting("resize_to_optimal_sampling", False))
            storage_mode = self._get_original_storage_mode()
            store_original = storage_mode != "none"
            resample_factor = img.get("resample_scale_factor")
            if image_type == "microscope":
                parsed_resample = None
                try:
                    if resample_factor is not None:
                        parsed_resample = float(resample_factor)
                except (TypeError, ValueError):
                    parsed_resample = None
                if parsed_resample is not None:
                    # Persist per-image resize intent: explicit non-resized microscope images
                    # store 1.0, while resized images store a factor below 1.0.
                    resize_to_optimal = 0.0 < parsed_resample < 0.999
                else:
                    original_path = (img.get("original_filepath") or "").strip()
                    current_path = (img.get("filepath") or "").strip()
                    if original_path and current_path and original_path != current_path:
                        resize_to_optimal = True
            if (
                resample_factor is None
                and objective_value
                and objective_value in objectives
                and isinstance(scale_value, (int, float))
            ):
                base_scale = objectives[objective_value].get("microns_per_pixel")
                if isinstance(base_scale, (int, float)) and base_scale > 0 and scale_value > 0:
                    factor_guess = float(base_scale) / float(scale_value)
                    if 0 < factor_guess < 0.999:
                        resample_factor = factor_guess
            sb_x1 = img.get("scale_bar_x1")
            sb_y1 = img.get("scale_bar_y1")
            sb_x2 = img.get("scale_bar_x2")
            sb_y2 = img.get("scale_bar_y2")
            scale_bar_sel = None
            if all(v is not None for v in (sb_x1, sb_y1, sb_x2, sb_y2)):
                scale_bar_sel = ((float(sb_x1), float(sb_y1)), (float(sb_x2), float(sb_y2)))
            calibration_length_um = self._get_calibration_measurement_length(img.get("id"))
            if scale_bar_sel is None or calibration_length_um is None:
                fallback_sel, fallback_len = self._get_calibration_overlay_data(img.get("id"))
                if scale_bar_sel is None and fallback_sel is not None:
                    scale_bar_sel = fallback_sel
                if calibration_length_um is None and fallback_len is not None:
                    calibration_length_um = fallback_len
            results.append(
                ImageImportResult(
                    filepath=filepath,
                    image_id=img.get("id"),
                    image_type=image_type,
                    objective=objective_value,
                    custom_scale=custom_scale,
                    contrast=img.get("contrast"),
                    mount_medium=img.get("mount_medium"),
                    stain=img.get("stain"),
                    sample_type=img.get("sample_type"),
                    captured_at=captured_at,
                    exif_has_gps=exif_has_gps,
                    ai_crop_box=ai_crop_box,
                    ai_crop_source_size=ai_crop_source_size,
                    crop_mode=img.get("crop_mode"),
                    gps_source=gps_source,
                    resample_scale_factor=resample_factor,
                    original_filepath=img.get("original_filepath") or filepath,
                    resize_to_optimal=resize_to_optimal,
                    store_original=store_original,
                    scale_bar_selection=scale_bar_sel,
                    scale_bar_length_um=calibration_length_um,
                )
            )
        if missing_paths:
            names = [Path(p).name for p in missing_paths if p]
            self.set_status_message(
                self.tr("Missing image files detected ({count}). Relink or remove them.").format(
                    count=len(names)
                ),
                level="warning",
                auto_clear_ms=12000,
            )
        return results

    def _compute_resample_scale_factor(
        self,
        result: ImageImportResult,
        scale_mpp: float | None,
        objective_entry: dict | None,
    ) -> float:
        if not result or result.image_type != "microscope":
            return 1.0
        if not getattr(result, "resize_to_optimal", True):
            return 1.0
        if not scale_mpp or scale_mpp <= 0:
            return 1.0
        na_value = objective_entry.get("na") if objective_entry else None
        if not na_value:
            return 1.0
        if objective_entry and objective_entry.get("target_sampling_pct") is not None:
            target_pct = float(objective_entry.get("target_sampling_pct"))
        else:
            target_pct = float(SettingsDB.get_setting("target_sampling_pct", 120.0))
        pixels_per_micron = 1.0 / float(scale_mpp)
        info = get_resolution_status(pixels_per_micron, float(na_value))
        ideal_pixels_per_micron = float(info.get("ideal_pixels_per_micron", 0.0))
        if not ideal_pixels_per_micron or ideal_pixels_per_micron <= 0:
            return 1.0
        target_pixels_per_micron = ideal_pixels_per_micron * (target_pct / 100.0)
        factor = target_pixels_per_micron / pixels_per_micron
        if factor > 1.0:
            factor = 1.0
        # Skip resize when ideal area is close to current (>= 90% of current MP).
        if (float(factor) * float(factor)) >= 0.90:
            return 1.0
        return max(0.01, float(factor))

    def _resample_import_image(
        self,
        source_path: str,
        scale_factor: float,
        output_dir: Path,
    ) -> str | None:
        if not source_path or scale_factor >= 0.999:
            return source_path
        try:
            with Image.open(source_path) as img:
                exif_bytes = None
                try:
                    exif = img.getexif()
                    if exif:
                        exif_bytes = exif.tobytes()
                except Exception:
                    exif_bytes = None
                new_w = max(1, int(round(img.width * scale_factor)))
                new_h = max(1, int(round(img.height * scale_factor)))
                resized = img.resize((new_w, new_h), Image.LANCZOS)
                src_path = Path(source_path)
                suffix = src_path.suffix or ".jpg"
                temp_path = output_dir / f"{src_path.stem}_resized{suffix}"
                counter = 1
                while temp_path.exists():
                    temp_path = output_dir / f"{src_path.stem}_resized_{counter}{suffix}"
                    counter += 1
                save_kwargs = {}
                fmt = img.format or None
                if suffix.lower() in {".jpg", ".jpeg"}:
                    resized = resized.convert("RGB")
                    quality = SettingsDB.get_setting("resize_jpeg_quality", 80)
                    try:
                        quality = int(quality)
                    except (TypeError, ValueError):
                        quality = 80
                    quality = max(1, min(100, quality))
                    save_kwargs["quality"] = quality
                    fmt = "JPEG"
                    if exif_bytes:
                        save_kwargs["exif"] = exif_bytes
                resized.save(temp_path, format=fmt, **save_kwargs)
                return str(temp_path)
        except Exception as exc:
            print(f"Warning: Could not resize image {source_path}: {exc}")
            return source_path

    def _get_image_size(self, path: str | None) -> tuple[int, int] | None:
        if not path:
            return None
        try:
            with Image.open(path) as img:
                return img.width, img.height
        except Exception:
            return None

    def _scale_measurement_points(self, image_id: int, scale_factor: float) -> None:
        if not image_id or not scale_factor or scale_factor <= 0:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE spore_measurements
            SET p1_x = p1_x * ?, p1_y = p1_y * ?,
                p2_x = p2_x * ?, p2_y = p2_y * ?,
                p3_x = CASE WHEN p3_x IS NOT NULL THEN p3_x * ? ELSE NULL END,
                p3_y = CASE WHEN p3_y IS NOT NULL THEN p3_y * ? ELSE NULL END,
                p4_x = CASE WHEN p4_x IS NOT NULL THEN p4_x * ? ELSE NULL END,
                p4_y = CASE WHEN p4_y IS NOT NULL THEN p4_y * ? ELSE NULL END
            WHERE image_id = ?
            ''',
            [
                scale_factor, scale_factor,
                scale_factor, scale_factor,
                scale_factor, scale_factor,
                scale_factor, scale_factor,
                image_id,
            ]
        )
        conn.commit()
        conn.close()

    def _translate_measurement_points(self, image_id: int, offset_x: float, offset_y: float) -> None:
        if not image_id:
            return
        if not offset_x and not offset_y:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE spore_measurements
            SET p1_x = p1_x - ?, p1_y = p1_y - ?,
                p2_x = p2_x - ?, p2_y = p2_y - ?,
                p3_x = CASE WHEN p3_x IS NOT NULL THEN p3_x - ? ELSE NULL END,
                p3_y = CASE WHEN p3_y IS NOT NULL THEN p3_y - ? ELSE NULL END,
                p4_x = CASE WHEN p4_x IS NOT NULL THEN p4_x - ? ELSE NULL END,
                p4_y = CASE WHEN p4_y IS NOT NULL THEN p4_y - ? ELSE NULL END
            WHERE image_id = ?
            ''',
            [
                offset_x, offset_y,
                offset_x, offset_y,
                offset_x, offset_y,
                offset_x, offset_y,
                image_id,
            ],
        )
        conn.commit()
        conn.close()

    def _replace_observation_image_file(
        self,
        source_path: str | None,
        target_path: str | None,
        output_dir: Path,
    ) -> str | None:
        if not source_path:
            return target_path
        try:
            source = Path(source_path).resolve()
        except Exception:
            return target_path
        if not source.exists():
            return target_path
        target = None
        if target_path:
            try:
                target = Path(target_path).resolve()
            except Exception:
                target = None
        if target and source == target:
            return str(target)
        if target is None:
            target_dir = output_dir
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / source.name
            counter = 1
            while target.exists():
                target = target_dir / f"{source.stem}_{counter}{source.suffix}"
                counter += 1
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
        tmp_target = target.with_name(f"{target.stem}_replacing{target.suffix}")
        try:
            shutil.copy2(source, tmp_target)
            tmp_target.replace(target)
            return str(target)
        except Exception as exc:
            print(f"Warning: Could not replace observation image {target}: {exc}")
            try:
                tmp_target.unlink(missing_ok=True)
            except Exception:
                pass
            return target_path

    def _get_calibration_measurement_length(self, image_id: int) -> float | None:
        if not image_id:
            return None
        conn = get_connection()
        row = conn.execute(
            """
            SELECT length_um
            FROM spore_measurements
            WHERE image_id = ?
              AND COALESCE(LOWER(measurement_type), '') = 'calibration'
            ORDER BY measured_at DESC, id DESC
            LIMIT 1
            """,
            (image_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        value = row[0]
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _get_calibration_overlay_data(
        self,
        image_id: int,
    ) -> tuple[tuple[tuple[float, float], tuple[float, float]] | None, float | None]:
        if not image_id:
            return None, None
        conn = get_connection()
        row = conn.execute(
            """
            SELECT p1_x, p1_y, p2_x, p2_y, length_um
            FROM spore_measurements
            WHERE image_id = ?
              AND COALESCE(LOWER(measurement_type), '') = 'calibration'
            ORDER BY measured_at DESC, id DESC
            LIMIT 1
            """,
            (image_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None, None
        p1_x, p1_y, p2_x, p2_y, length_um = row
        selection = None
        if all(v is not None for v in (p1_x, p1_y, p2_x, p2_y)):
            selection = (
                (float(p1_x), float(p1_y)),
                (float(p2_x), float(p2_y)),
            )
        length_value = None
        try:
            length_value = float(length_um) if length_um is not None else None
        except (TypeError, ValueError):
            length_value = None
        return selection, length_value

    def _upsert_calibration_measurement_for_image(
        self,
        image_id: int,
        scale_bar_selection: tuple | None,
        total_um: float | None,
        coordinate_scale: float = 1.0,
    ) -> None:
        if not image_id or not scale_bar_selection or len(scale_bar_selection) != 2:
            return
        if total_um is None or total_um <= 0:
            return
        (x1, y1), (x2, y2) = scale_bar_selection
        factor = float(coordinate_scale) if coordinate_scale and coordinate_scale > 0 else 1.0
        x1 *= factor
        y1 *= factor
        x2 *= factor
        y2 *= factor
        dx = x2 - x1
        dy = y2 - y1
        length_px = (dx * dx + dy * dy) ** 0.5
        if length_px <= 0:
            return
        perp_x = -dy / length_px
        perp_y = dx / length_px
        half_width_px = length_px / 20.0  # 1:10 ratio
        mx = (x1 + x2) / 2
        my = (y1 + y2) / 2
        p1 = QPointF(x1, y1)
        p2 = QPointF(x2, y2)
        p3 = QPointF(mx - perp_x * half_width_px, my - perp_y * half_width_px)
        p4 = QPointF(mx + perp_x * half_width_px, my + perp_y * half_width_px)
        width_um = float(total_um) / 10.0

        conn = get_connection()
        conn.execute(
            "DELETE FROM spore_measurements WHERE image_id = ? AND COALESCE(LOWER(measurement_type), '') = 'calibration'",
            (image_id,),
        )
        conn.commit()
        conn.close()

        MeasurementDB.add_measurement(
            image_id=image_id,
            length=float(total_um),
            width=width_um,
            measurement_type="calibration",
            notes=f"Scale bar: {float(total_um):.1f} µm",
            points=[p1, p2, p3, p4],
        )

    def _scale_scale_bar_selection(
        self,
        selection: tuple | None,
        factor: float,
    ) -> tuple | None:
        if not selection or len(selection) != 2:
            return selection
        if not isinstance(factor, (int, float)) or factor <= 0 or abs(float(factor) - 1.0) < 1e-9:
            return selection
        (x1, y1), (x2, y2) = selection
        f = float(factor)
        return ((float(x1) * f, float(y1) * f), (float(x2) * f, float(y2) * f))

    def _rescale_measurement_lengths(
        self,
        image_id: int,
        old_scale: float | None,
        new_scale: float | None,
    ) -> None:
        if (
            not image_id
            or not old_scale
            or not new_scale
            or old_scale <= 0
            or new_scale <= 0
        ):
            return
        ratio = float(new_scale) / float(old_scale)
        if abs(ratio - 1.0) < 1e-6:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE spore_measurements
            SET length_um = length_um * ?,
                width_um = CASE WHEN width_um IS NOT NULL THEN width_um * ? ELSE NULL END
            WHERE image_id = ?
              AND COALESCE(LOWER(measurement_type), '') != 'calibration'
            ''',
            (ratio, ratio, image_id)
        )
        conn.commit()
        conn.close()

    def _maybe_remove_image_file(
        self,
        old_path: str | None,
        new_path: str | None,
        keep_original: bool,
        images_root: Path,
    ) -> None:
        if keep_original or not old_path or not new_path:
            return
        if old_path == new_path:
            return
        try:
            old = Path(old_path).resolve()
            root = images_root.resolve()
            old.relative_to(root)
        except Exception:
            return
        try:
            old.unlink()
        except Exception as exc:
            print(f"Warning: Could not remove replaced image {old_path}: {exc}")

    def _get_original_storage_mode(self) -> str:
        mode = SettingsDB.get_setting("original_storage_mode")
        if not mode:
            mode = "observation" if SettingsDB.get_setting("store_original_images", False) else "none"
        return str(mode)

    def _get_originals_base_dir(self) -> Path:
        base = SettingsDB.get_setting("originals_dir")
        if base:
            return Path(base)
        return get_database_path().parent / "images" / "originals"

    def _store_original_for_observation(
        self,
        observation_id: int,
        source_path: str | None,
        storage_mode: str,
        images_root: Path,
        obs_folder: Path | None,
    ) -> tuple[str | None, bool]:
        if storage_mode == "none" or not source_path:
            return None, False
        try:
            source = Path(source_path).resolve()
        except Exception:
            return None, False
        if not source.exists():
            return None, False
        target_dir = None
        if storage_mode == "global":
            base = self._get_originals_base_dir()
            if obs_folder:
                try:
                    rel = obs_folder.resolve().relative_to(images_root.resolve())
                    target_dir = base / rel
                except Exception:
                    target_dir = base / obs_folder.name
            else:
                target_dir = base / f"observation_{observation_id}"
        else:
            if obs_folder:
                target_dir = obs_folder / "originals"
        if not target_dir:
            return None, False
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            return None, False
        try:
            if source.is_relative_to(target_dir.resolve()):
                return str(source), True
        except Exception:
            pass
        dest = target_dir / source.name
        counter = 1
        while dest.exists():
            dest = target_dir / f"{source.stem}_{counter}{source.suffix}"
            counter += 1
        try:
            shutil.copy2(source, dest)
        except Exception as exc:
            print(f"Warning: Could not copy original image: {exc}")
            return None, False
        return str(dest), True

    def _apply_import_results_to_observation(
        self,
        obs_id: int,
        results: list[ImageImportResult],
        existing_images: list[dict] | None = None,
        progress_cb=None,
    ) -> None:
        objectives = load_objectives()
        images_root = Path(get_images_dir())
        output_dir = images_root / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        existing_by_id = {
            img.get("id"): img for img in (existing_images or []) if img.get("id")
        }
        storage_mode = self._get_original_storage_mode()
        obs_folder = None
        try:
            obs = ObservationDB.get_observation(obs_id)
            if obs and obs.get("folder_path"):
                obs_folder = Path(obs.get("folder_path"))
        except Exception:
            obs_folder = None
        if obs_folder:
            obs_folder.mkdir(parents=True, exist_ok=True)

        existing_ids = {img.get("id") for img in (existing_images or []) if img.get("id")}
        result_ids = {res.image_id for res in results if res.image_id}
        removed_ids = existing_ids - result_ids
        for image_id in removed_ids:
            ImageDB.delete_image(image_id)

        total = len(results)
        for index, result in enumerate(results, start=1):
            if progress_cb:
                progress_cb(index, total, result)
            image_type = result.image_type or "field"
            objective_key = result.objective
            if objective_key and objective_key not in objectives:
                resolved_key = resolve_objective_key(objective_key, objectives)
                if resolved_key:
                    objective_key = resolved_key
            objective_entry = objectives.get(objective_key) if objective_key in objectives else None
            contrast = result.contrast
            mount_medium = result.mount_medium
            stain = result.stain
            sample_type = result.sample_type

            scale = None
            objective_name = None
            scale_from_existing = False
            scale_is_custom = False
            if result.custom_scale:
                scale = float(result.custom_scale)
                scale_is_custom = True
                if image_type == "microscope":
                    objective_name = "Custom"
            if image_type == "microscope":
                if (not scale_is_custom) and objective_key and objective_key in objectives:
                    objective_name = objective_key
                    if result.image_id:
                        existing = existing_by_id.get(result.image_id)
                        existing_scale = existing.get("scale_microns_per_pixel") if existing else None
                        existing_obj = existing.get("objective_name") if existing else None
                        existing_key = resolve_objective_key(existing_obj, objectives) or existing_obj
                        if (
                            existing_scale is not None
                            and existing_key
                            and existing_key == objective_key
                        ):
                            scale = float(existing_scale)
                            scale_from_existing = True
                    if scale is None:
                        scale = float(objectives[objective_key]["microns_per_pixel"])
            else:
                # Field images can have a custom scale from scale-bar calibration.
                # Keep objective empty so stale microscope objective values are cleared.
                objective_name = ""

            calibration_id = None
            if objective_name and objective_name != "Custom":
                calibration_id = CalibrationDB.get_active_calibration_id(objective_name)

            if result.image_id:
                existing = existing_by_id.get(result.image_id)
                existing_path = existing.get("filepath") if existing else result.filepath
                working_path = result.filepath or existing_path
                existing_scale = existing.get("scale_microns_per_pixel") if existing else None
                existing_resample = existing.get("resample_scale_factor") if existing else None
                already_resized = (
                    isinstance(existing_resample, (int, float))
                    and existing_resample > 0
                    and existing_resample < 0.999
                )
                resample_factor = self._compute_resample_scale_factor(result, scale, objective_entry)
                if already_resized and isinstance(existing_resample, (int, float)):
                    resample_factor = float(existing_resample)
                    if scale is not None and not scale_from_existing and not scale_is_custom:
                        scale = float(scale) / float(existing_resample)

                update_kwargs = dict(
                    image_type=image_type,
                    objective_name=objective_name,
                    scale=scale,
                    sort_order=index - 1,
                    contrast=contrast,
                    mount_medium=mount_medium,
                    stain=stain,
                    sample_type=sample_type,
                    ai_crop_box=result.ai_crop_box,
                    ai_crop_source_size=result.ai_crop_source_size,
                    crop_mode=result.crop_mode,
                    gps_source=result.gps_source,
                    calibration_id=calibration_id,
                    scale_bar_selection=getattr(result, "scale_bar_selection", None),
                )
                pending_edit = False
                if working_path and existing_path:
                    try:
                        pending_edit = Path(working_path).resolve() != Path(existing_path).resolve()
                    except Exception:
                        pending_edit = str(working_path) != str(existing_path)
                pending_crop_offset = getattr(result, "pending_image_crop_offset", None)
                if (
                    pending_crop_offset
                    and len(pending_crop_offset) == 2
                    and (pending_crop_offset[0] or pending_crop_offset[1])
                ):
                    self._translate_measurement_points(
                        result.image_id,
                        float(pending_crop_offset[0]),
                        float(pending_crop_offset[1]),
                    )
                    result.pending_image_crop_offset = None

                apply_resample = (
                    image_type == "microscope"
                    and getattr(result, "resize_to_optimal", True)
                    and resample_factor < 0.999
                    and not already_resized
                )
                if pending_edit and working_path:
                    source_for_update = working_path
                    final_path = working_path
                    if apply_resample:
                        resample_dir = None
                        try:
                            resample_dir = Path(existing_path).parent if existing_path else None
                        except Exception:
                            resample_dir = None
                        if resample_dir is None and obs_folder is not None:
                            resample_dir = obs_folder
                        if resample_dir is None:
                            resample_dir = output_dir
                        resample_dir.mkdir(parents=True, exist_ok=True)
                        final_path = self._resample_import_image(
                            source_for_update,
                            resample_factor,
                            resample_dir,
                        ) or source_for_update
                        if scale is not None and resample_factor > 0:
                            scale = float(scale) / float(resample_factor)
                            update_kwargs["scale"] = scale
                        update_kwargs["resample_scale_factor"] = resample_factor

                        crop_box = result.ai_crop_box
                        if crop_box:
                            update_kwargs["ai_crop_box"] = tuple(v * resample_factor for v in crop_box)
                        source_size = result.ai_crop_source_size
                        if source_size:
                            update_kwargs["ai_crop_source_size"] = (
                                int(round(source_size[0] * resample_factor)),
                                int(round(source_size[1] * resample_factor)),
                            )
                        else:
                            size = self._get_image_size(source_for_update)
                            if size:
                                update_kwargs["ai_crop_source_size"] = (
                                    int(round(size[0] * resample_factor)),
                                    int(round(size[1] * resample_factor)),
                                )

                        self._scale_measurement_points(result.image_id, resample_factor)
                        if result.scale_bar_selection:
                            scaled_sel = self._scale_scale_bar_selection(
                                result.scale_bar_selection, resample_factor
                            )
                            result.scale_bar_selection = scaled_sel
                            update_kwargs["scale_bar_selection"] = scaled_sel
                    else:
                        current_image = ImageDB.get_image(result.image_id)
                        current_scale = (
                            current_image.get("scale_microns_per_pixel")
                            if current_image
                            else existing_scale
                        )
                        self._rescale_measurement_lengths(
                            result.image_id,
                            current_scale,
                            scale,
                        )

                    if storage_mode != "none":
                        original_source = existing.get("original_filepath") if existing else None
                        if not original_source:
                            original_source = existing_path
                        dest_original, _ = self._store_original_for_observation(
                            obs_id,
                            original_source,
                            storage_mode,
                            images_root,
                            obs_folder,
                        )
                        update_kwargs["original_filepath"] = dest_original or original_source
                    else:
                        update_kwargs["original_filepath"] = (
                            existing.get("original_filepath")
                            if existing and existing.get("original_filepath")
                            else existing_path
                        )

                    stored_path = self._replace_observation_image_file(
                        final_path,
                        existing_path,
                        obs_folder or output_dir,
                    )
                    if stored_path:
                        update_kwargs["filepath"] = stored_path
                        try:
                            generate_all_sizes(stored_path, result.image_id)
                        except Exception as e:
                            print(f"Warning: Could not regenerate thumbnails for {stored_path}: {e}")
                        cleanup_import_temp_file(existing_path or source_for_update, source_for_update, stored_path, output_dir)
                        if final_path and final_path != source_for_update:
                            cleanup_import_temp_file(source_for_update, final_path, stored_path, output_dir)
                        result.filepath = stored_path
                        result.preview_path = stored_path
                    ImageDB.update_image(result.image_id, **update_kwargs)
                    continue
                if apply_resample and existing_path:
                    resample_dir = None
                    try:
                        resample_dir = Path(existing_path).parent
                    except Exception:
                        resample_dir = None
                    if resample_dir is None and obs_folder is not None:
                        resample_dir = obs_folder
                    if resample_dir is None:
                        resample_dir = output_dir
                    resample_dir.mkdir(parents=True, exist_ok=True)
                    resampled_path = self._resample_import_image(
                        existing_path,
                        resample_factor,
                        resample_dir,
                    ) or existing_path
                    if resampled_path != existing_path:
                        if scale is not None and resample_factor > 0:
                            scale = float(scale) / float(resample_factor)
                            update_kwargs["scale"] = scale
                        update_kwargs["filepath"] = resampled_path
                        update_kwargs["resample_scale_factor"] = resample_factor

                        crop_box = result.ai_crop_box
                        if crop_box:
                            update_kwargs["ai_crop_box"] = tuple(v * resample_factor for v in crop_box)
                        source_size = result.ai_crop_source_size
                        if source_size:
                            update_kwargs["ai_crop_source_size"] = (
                                int(round(source_size[0] * resample_factor)),
                                int(round(source_size[1] * resample_factor)),
                            )
                        else:
                            size = self._get_image_size(existing_path)
                            if size:
                                update_kwargs["ai_crop_source_size"] = (
                                    int(round(size[0] * resample_factor)),
                                    int(round(size[1] * resample_factor)),
                                )

                        self._scale_measurement_points(result.image_id, resample_factor)
                        if result.scale_bar_selection:
                            scaled_sel = self._scale_scale_bar_selection(
                                result.scale_bar_selection, resample_factor
                            )
                            result.scale_bar_selection = scaled_sel
                            update_kwargs["scale_bar_selection"] = scaled_sel

                        copied_original = False
                        if storage_mode != "none":
                            original_source = None
                            if existing:
                                original_source = existing.get("original_filepath")
                            if not original_source:
                                original_source = existing_path
                            dest_original, copied_original = self._store_original_for_observation(
                                obs_id,
                                original_source,
                                storage_mode,
                                images_root,
                                obs_folder,
                            )
                            update_kwargs["original_filepath"] = dest_original or original_source
                        else:
                            update_kwargs["original_filepath"] = None

                        try:
                            generate_all_sizes(resampled_path, result.image_id)
                        except Exception as e:
                            print(f"Warning: Could not regenerate thumbnails for {resampled_path}: {e}")
                        self._maybe_remove_image_file(
                            existing_path,
                            resampled_path,
                            not (storage_mode == "none" or copied_original),
                            images_root,
                        )

                if not apply_resample:
                    current_image = ImageDB.get_image(result.image_id)
                    current_scale = (
                        current_image.get("scale_microns_per_pixel")
                        if current_image
                        else existing_scale
                    )
                    self._rescale_measurement_lengths(
                        result.image_id,
                        current_scale,
                        scale,
                    )

                ImageDB.update_image(result.image_id, **update_kwargs)
                continue

            filepath = result.filepath
            if not filepath:
                continue
            final_path = maybe_convert_heic(filepath, output_dir)
            if final_path is None:
                continue
            if objective_name:
                calibration_id = CalibrationDB.get_active_calibration_id(objective_name)
            resample_factor = self._compute_resample_scale_factor(result, scale, objective_entry)
            result.resample_scale_factor = resample_factor
            resampled_path = final_path
            if (
                image_type == "microscope"
                and getattr(result, "resize_to_optimal", True)
                and resample_factor < 0.999
            ):
                resampled_path = self._resample_import_image(final_path, resample_factor, output_dir) or final_path
                if scale is not None and resample_factor > 0:
                    scale = float(scale) / float(resample_factor)

            original_to_store = None
            if (
                image_type == "microscope"
                and getattr(result, "store_original", False)
                and resample_factor < 0.999
            ):
                original_to_store = result.original_filepath or final_path
            image_id = ImageDB.add_image(
                observation_id=obs_id,
                filepath=resampled_path,
                image_type=image_type,
                scale=scale,
                objective_name=objective_name,
                contrast=contrast,
                mount_medium=mount_medium,
                stain=stain,
                sample_type=sample_type,
                sort_order=index - 1,
                calibration_id=calibration_id,
                ai_crop_box=result.ai_crop_box,
                ai_crop_source_size=result.ai_crop_source_size,
                crop_mode=result.crop_mode,
                gps_source=result.gps_source,
                resample_scale_factor=resample_factor,
                original_filepath=original_to_store,
            )
            stored_scale_bar_selection = self._scale_scale_bar_selection(
                getattr(result, "scale_bar_selection", None),
                resample_factor
                if image_type == "microscope"
                and getattr(result, "resize_to_optimal", True)
                and resample_factor < 0.999
                else 1.0,
            )
            if stored_scale_bar_selection:
                ImageDB.update_image(
                    image_id,
                    scale_bar_selection=stored_scale_bar_selection,
                )
                result.scale_bar_selection = stored_scale_bar_selection
            scale_bar_length = getattr(result, "scale_bar_length_um", None)
            self._upsert_calibration_measurement_for_image(
                image_id=image_id,
                scale_bar_selection=stored_scale_bar_selection,
                total_um=scale_bar_length,
                coordinate_scale=1.0,
            )

            stored_path = resampled_path
            try:
                image_data = ImageDB.get_image(image_id)
                stored_path = image_data.get("filepath") if image_data else resampled_path
                generate_all_sizes(stored_path, image_id)
            except Exception as e:
                print(f"Warning: Could not generate thumbnails for {resampled_path}: {e}")
            cleanup_import_temp_file(filepath, final_path, stored_path, output_dir)
            if resampled_path and resampled_path != final_path:
                cleanup_import_temp_file(filepath, resampled_path, stored_path, output_dir)


class ObservationDetailsDialog(GeometryMixin, QDialog):
    """Dialog for creating or editing an observation after image import."""

    _geometry_key = "ObservationDetailsDialog"
    _gallery_splitter_key = "splitter/ObservationDetailsDialogBottom"

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
        self.suggested_taxon = suggested_taxon
        self.map_helper = MapServiceHelper(self)
        self._hint_controller: HintStatusController | None = None
        self.setWindowTitle(
            self.tr("Edit Observation") if self.edit_mode else self.tr("New Observation")
        )
        self.setModal(True)
        self.setMinimumSize(900, 820)
        self._observation_datetime = _parse_observation_datetime(
            observation.get("date") if observation else None
        )
        self.image_files = []
        self.image_metadata = []
        self.image_settings = []
        self.selected_image_index = -1
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
        self._ai_selected_taxon: dict | None = None
        self._ai_thread = None
        self._artsobs_check_thread = None
        self._close_cleanup_done = False
        self._dialog_temp_preview_paths: set[str] = set()
        self._dialog_preview_path_cache: dict[str, str] = {}
        self._location_lookup_workers: set[QThread] = set()
        self._ai_selected_index: int | None = None
        self._publish_target_manual_override = False
        self._publish_target_sync_in_progress = False
        self._location_lookup_name = ""
        self._last_applied_location_lookup_name = ""
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
            inferred_target = infer_publish_target_from_coords(
                self.lat_input.value() if hasattr(self, "lat_input") else None,
                self.lon_input.value() if hasattr(self, "lon_input") else None,
            )
            self._set_publish_target_combo(
                inferred_target or self._active_reporting_target(),
                manual_override=False,
            )
            self._apply_primary_metadata()
        self._loading_form = False
        QTimer.singleShot(0, self._complete_deferred_dialog_setup)
        self._apply_suggested_taxon()
        self._sync_taxon_cache()
        self._restore_geometry()
        self.finished.connect(self._save_geometry)

    def _dialog_gallery_default_height(self) -> int:
        gallery = getattr(self, "image_gallery", None)
        if gallery is None:
            return 160
        return max(gallery.minimumHeight(), gallery.preferred_single_row_height() - 8)

    def _dialog_gallery_max_height(self) -> int:
        gallery = getattr(self, "image_gallery", None)
        if gallery is None:
            return 300
        return max(gallery.minimumHeight(), gallery.maximum_useful_height())

    def _apply_dialog_gallery_splitter_height(self, gallery_height: int | None = None) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        gallery = getattr(self, "image_gallery", None)
        if splitter is None or gallery is None:
            return
        target = self._dialog_gallery_default_height() if gallery_height is None else int(gallery_height)
        target = max(gallery.minimumHeight(), min(self._dialog_gallery_max_height(), target))
        gallery.setMaximumHeight(self._dialog_gallery_max_height())
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
        gallery_height = None
        if isinstance(raw_value, (list, tuple)):
            parsed: list[int] = []
            for value in raw_value[:2]:
                try:
                    parsed.append(max(0, int(value)))
                except Exception:
                    parsed.append(0)
            if len(parsed) >= 2:
                gallery_height = parsed[1]
        else:
            try:
                gallery_height = max(0, int(raw_value))
            except Exception:
                gallery_height = None
        self._apply_dialog_gallery_splitter_height(gallery_height)

    def _save_dialog_gallery_splitter(self) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        gallery = getattr(self, "image_gallery", None)
        if splitter is None or gallery is None:
            return
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        height = max(gallery.minimumHeight(), int(gallery.height() or 0))
        if height <= 0:
            sizes = splitter.sizes()
            if len(sizes) >= 2:
                height = max(gallery.minimumHeight(), int(sizes[1]))
        settings.setValue(self._gallery_splitter_key, height)

    def _on_dialog_gallery_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is None or self._dialog_gallery_splitter_syncing:
            return
        sizes = splitter.sizes()
        gallery_height = sizes[1] if len(sizes) >= 2 else None
        QTimer.singleShot(0, lambda h=gallery_height: self._apply_dialog_gallery_splitter_height(h))
        QTimer.singleShot(0, self._save_dialog_gallery_splitter)

    def _restore_geometry(self) -> None:
        super()._restore_geometry()
        QTimer.singleShot(0, self._restore_dialog_gallery_splitter)

    def _save_geometry(self) -> None:
        self._save_dialog_gallery_splitter()
        super()._save_geometry()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)

        top_content = QWidget(self)
        top_content_layout = QVBoxLayout(top_content)
        top_content_layout.setContentsMargins(0, 0, 0, 0)
        top_content_layout.setSpacing(10)

        # ===== OBSERVATION DETAILS SECTION =====
        details_group = QGroupBox(self.tr("Observation Details"))
        details_layout = QHBoxLayout(details_group)
        details_layout.setSpacing(12)

        left_panel = QWidget()
        left_layout = QGridLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setHorizontalSpacing(8)
        left_layout.setVerticalSpacing(8)

        right_panel = QWidget()
        right_layout = QFormLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)
        right_layout.setLabelAlignment(Qt.AlignLeft | Qt.AlignTop)
        right_layout.setFormAlignment(Qt.AlignTop)

        # Date and time
        datetime_container = QWidget()
        datetime_layout = QHBoxLayout(datetime_container)
        datetime_layout.setContentsMargins(0, 0, 0, 0)
        self.datetime_input = QDateTimeEdit()
        self.datetime_input.setDateTime(QDateTime.currentDateTime())
        self.datetime_input.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.datetime_input.setCalendarPopup(True)
        self.datetime_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        datetime_layout.addWidget(self.datetime_input, 1)
        _details_row = 0
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
        self.location_input.textEdited.connect(self._on_location_name_edited)
        location_layout.addWidget(self.location_input, 1)
        self.location_lookup_apply_btn = QPushButton(self.tr("Get name"))
        self.location_lookup_apply_btn.setMinimumWidth(self.maplink_open_btn.sizeHint().width())
        self.location_lookup_apply_btn.setEnabled(False)
        self.location_lookup_apply_btn.clicked.connect(self._apply_lookup_location_name)
        location_layout.addWidget(self.location_lookup_apply_btn)
        left_layout.addWidget(QLabel(self.tr("Location:")), _details_row, 0)
        left_layout.addWidget(location_container, _details_row, 1, 1, 2)
        _details_row += 1

        self.country_summary_label = QLabel("")
        self.country_summary_label.setStyleSheet("color: #2c3e50;")
        self.country_summary_label.setVisible(False)
        left_layout.addWidget(self.country_summary_label, _details_row, 1, 1, 2)
        _details_row += 1

        open_comment_container = QWidget()
        open_comment_layout = QVBoxLayout(open_comment_container)
        open_comment_layout.setContentsMargins(0, 0, 0, 0)
        open_comment_layout.setSpacing(6)

        open_comment_label = QLabel(self.tr("Open comment:"))
        open_comment_layout.addWidget(open_comment_label)

        self.open_comment_input = QTextEdit()
        self.open_comment_input.setPlaceholderText(self.tr("Open comment..."))
        comment_h = (self.open_comment_input.fontMetrics().lineSpacing() * 3) + 14
        self.open_comment_input.setFixedHeight(comment_h)
        self.open_comment_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        open_comment_layout.addWidget(self.open_comment_input)
        right_layout.addRow(open_comment_container)

        private_comment_container = QWidget()
        private_comment_layout = QVBoxLayout(private_comment_container)
        private_comment_layout.setContentsMargins(0, 0, 0, 0)
        private_comment_layout.setSpacing(6)

        private_comment_label = QLabel(self.tr("Private comment:"))
        private_comment_layout.addWidget(private_comment_label)

        self.private_comment_input = QTextEdit()
        self.private_comment_input.setPlaceholderText(self.tr("Private comment..."))
        self.private_comment_input.setFixedHeight(comment_h)
        self.private_comment_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        private_comment_layout.addWidget(self.private_comment_input)
        right_layout.addRow(private_comment_container)

        # GPS info label (shows source of coordinates)
        self.gps_info_label = QLabel("")
        self.gps_info_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        left_layout.addWidget(self.gps_info_label, _details_row, 1, 1, 2)
        left_layout.setColumnStretch(1, 1)

        # Put location/date/GPS on the left, and comments on the right (about 40/60 split).
        details_layout.addWidget(left_panel, 4)
        details_layout.addWidget(right_panel, 6)
        top_content_layout.addWidget(details_group)

        # ===== TAXONOMY SECTION =====
        taxonomy_group = QGroupBox(self.tr("Taxonomy"))
        taxonomy_layout = QHBoxLayout(taxonomy_group)
        taxonomy_layout.setContentsMargins(8, 8, 8, 8)
        taxonomy_layout.setSpacing(8)

        taxonomy_split = QSplitter(Qt.Horizontal)
        taxonomy_split.setChildrenCollapsible(False)

        left_container = QWidget()
        left_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        left_container.setMinimumWidth(0)
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(6)

        # Taxonomy tab widget (Species + Biotope + Grows on)
        self.taxonomy_tabs = QTabWidget()
        self.taxonomy_tabs.setMinimumHeight(120)
        self.taxonomy_tabs.setMinimumWidth(0)
        self.taxonomy_tabs.currentChanged.connect(self.on_taxonomy_tab_changed)

        # Tab 1: Identified (vernacular + genus/species)
        identified_tab = QWidget()
        self.species_tab = identified_tab
        identified_layout = QVBoxLayout(identified_tab)
        identified_layout.setContentsMargins(8, 8, 8, 8)
        identified_layout.setSpacing(4)
        identified_layout.setAlignment(Qt.AlignTop)
        taxonomy_label_width = 96
        taxonomy_field_width = 520

        vern_row = QHBoxLayout()
        vern_row.setContentsMargins(0, 0, 0, 0)
        vern_row.setSpacing(4)
        self.vernacular_label = QLabel(self._vernacular_label())
        self.vernacular_label.setMinimumWidth(taxonomy_label_width)
        self.vernacular_label.setMaximumWidth(taxonomy_label_width)
        vern_row.addWidget(self.vernacular_label)
        vern_field_container = QWidget()
        vern_field_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        vern_field_container.setMaximumWidth(taxonomy_field_width)
        vern_field_layout = QHBoxLayout(vern_field_container)
        vern_field_layout.setContentsMargins(0, 0, 0, 0)
        vern_field_layout.setSpacing(4)
        self.vernacular_input = QLineEdit()
        self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.vernacular_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        vern_field_layout.addWidget(self.vernacular_input, 1)
        self.vernacular_language_btn = QToolButton()
        self.vernacular_language_btn.setText("🌐")
        self.vernacular_language_btn.setPopupMode(QToolButton.InstantPopup)
        self.vernacular_language_menu = QMenu(self.vernacular_language_btn)
        self.vernacular_language_btn.setMenu(self.vernacular_language_menu)
        input_height = max(18, int(self.vernacular_input.sizeHint().height()))
        self.vernacular_language_btn.setFixedSize(input_height, input_height)
        self._populate_vernacular_language_menu()
        vern_field_layout.addWidget(self.vernacular_language_btn, 0, Qt.AlignVCenter)
        vern_row.addWidget(vern_field_container, 1)
        vern_row.addStretch(1)
        identified_layout.addLayout(vern_row)

        genus_row = QHBoxLayout()
        genus_label = QLabel(self.tr("Genus:"))
        genus_label.setFixedWidth(taxonomy_label_width)
        genus_row.addWidget(genus_label)
        self.genus_input = QLineEdit()
        self.genus_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.genus_input.setMaximumWidth(taxonomy_field_width)
        self.genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.genus_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        genus_row.addWidget(self.genus_input, 1)
        genus_row.addStretch(1)
        identified_layout.addLayout(genus_row)

        species_row = QHBoxLayout()
        species_label = QLabel(self.tr("Species:"))
        species_label.setFixedWidth(taxonomy_label_width)
        species_row.addWidget(species_label)
        self.species_input = QLineEdit()
        self.species_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.species_input.setMaximumWidth(taxonomy_field_width)
        self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
        self.species_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        species_row.addWidget(self.species_input, 1)
        species_row.addStretch(1)
        identified_layout.addLayout(species_row)

        determination_row = QHBoxLayout()
        determination_label = QLabel(self.tr("Determination:"))
        determination_label.setFixedWidth(taxonomy_label_width)
        determination_row.addWidget(determination_label)
        self.determination_method_combo = QComboBox()
        self.determination_method_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.determination_method_combo.setMaximumWidth(taxonomy_field_width)
        self.determination_method_combo.addItem("", None)
        self.determination_method_combo.addItem(self.tr("Microscopy"), 1)
        self.determination_method_combo.addItem(self.tr("Sequencing"), 2)
        self.determination_method_combo.addItem(self.tr("eDNA"), 3)
        self.determination_method_combo.setToolTip(self.tr("Optional method used for determination."))
        self._style_dropdown_popup_readability(self.determination_method_combo.view(), self.determination_method_combo)
        self.determination_method_combo.currentIndexChanged.connect(self._update_taxonomy_tab_indicators)
        determination_row.addWidget(self.determination_method_combo, 1)
        determination_row.addStretch(1)
        identified_layout.addLayout(determination_row)

        self.publish_target_combo = QComboBox()
        self.publish_target_combo.addItem(self.tr("Artsobservasjoner (Norway)"), PUBLISH_TARGET_ARTSOBS_NO)
        self.publish_target_combo.addItem(self.tr("Artportalen (Sweden)"), PUBLISH_TARGET_ARTPORTALEN_SE)
        self.publish_target_combo.currentIndexChanged.connect(self._on_publish_target_changed)

        self.publish_target_hint = QLabel()
        self.publish_target_hint.setWordWrap(True)
        self.publish_target_hint.setStyleSheet("color: #6b7280; font-size: 11px;")
        self.publish_target_hint.setVisible(False)

        uncertain_row = QHBoxLayout()
        uncertain_row.setContentsMargins(0, 0, 0, 0)
        uncertain_row.setSpacing(4)
        uncertain_row.addSpacing(taxonomy_label_width)
        self.uncertain_checkbox = QCheckBox(self.tr("Uncertain"))
        self.uncertain_checkbox.toggled.connect(self._update_taxonomy_tab_indicators)
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

        self.taxonomy_tabs.addTab(identified_tab, self.tr("Species"))

        # Tab 2: Biotope metadata
        self._habitat_tree_states: dict[str, dict] = {}
        nin2_nodes = self._load_habitat_tree("nin2_biotopes_tree.json")
        nin2_tab = QWidget()
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
        self.grows_tab = grows_tab
        grows_tab_layout = QVBoxLayout(grows_tab)
        grows_tab_layout.setContentsMargins(8, 8, 8, 8)
        grows_tab_layout.setSpacing(8)
        grows_tab_layout.setAlignment(Qt.AlignTop)
        grows_group = QGroupBox(self._grows_on_tab_title())
        grows_layout = QFormLayout(grows_group)
        grows_layout.setSpacing(6)
        grows_layout.setFieldGrowthPolicy(QFormLayout.FieldsStayAtSizeHint)
        self.host_genus_input = QLineEdit()
        self.host_genus_input.setMaximumWidth(taxonomy_field_width)
        self.host_genus_input.setPlaceholderText(self.tr("e.g., Betula"))
        self.host_genus_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_species_input = QLineEdit()
        self.host_species_input.setMaximumWidth(taxonomy_field_width)
        self.host_species_input.setPlaceholderText(self.tr("e.g., pendula"))
        self.host_species_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_vernacular_input = QLineEdit()
        self.host_vernacular_input.setMaximumWidth(taxonomy_field_width)
        self.host_vernacular_input.setPlaceholderText(self._vernacular_placeholder())
        self.host_vernacular_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        self.host_vernacular_label = QLabel(self._vernacular_label())
        grows_layout.addRow(self.tr("Genus:"), self.host_genus_input)
        grows_layout.addRow(self.tr("Species:"), self.host_species_input)
        grows_layout.addRow(self.host_vernacular_label, self.host_vernacular_input)
        grows_tab_layout.addWidget(grows_group)
        self.grows_on_note_input = self._make_note_input()
        self.grows_on_note_input.setPlaceholderText(self.tr("Grows-on note..."))
        self.grows_on_note_input.textChanged.connect(self._update_taxonomy_tab_indicators)
        grows_tab_layout.addWidget(QLabel(self.tr("Grows-on note:")))
        grows_tab_layout.addWidget(self.grows_on_note_input)
        grows_tab_layout.addStretch(1)
        self.taxonomy_tabs.addTab(grows_tab, self._grows_on_tab_title())

        left_layout.addWidget(self.taxonomy_tabs)
        taxonomy_split.addWidget(left_container)

        self.ai_group = self._build_ai_suggestions_group()
        taxonomy_split.addWidget(self.ai_group)
        taxonomy_split.setStretchFactor(0, 1)
        taxonomy_split.setStretchFactor(1, 1)
        taxonomy_split.setSizes([500, 500])

        taxonomy_layout.addWidget(taxonomy_split)
        top_content_layout.addWidget(taxonomy_group)

        # ===== IMAGES SUMMARY (BOTTOM) =====
        self.image_gallery = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=60,
            default_height=160,
            thumbnail_tooltip=self.tr("Double-click to edit"),
        )
        self.image_gallery.set_compact_overlay(True)
        self.image_gallery.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.image_gallery.set_reorderable(True)
        self.image_gallery.set_multi_select(True)
        self.image_gallery.setMaximumHeight(self._dialog_gallery_max_height())
        self._gps_source_index = self._resolve_gps_source_index()
        self.image_gallery.imageClicked.connect(self._on_gallery_image_clicked)
        self.image_gallery.imageSelected.connect(self._on_gallery_image_clicked)
        self.image_gallery.deleteRequested.connect(self._on_gallery_delete_requested)
        self.image_gallery.imageDoubleClicked.connect(self._on_image_double_clicked)
        self.image_gallery.itemsReordered.connect(self._on_gallery_items_reordered)

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
        bottom_buttons.addWidget(make_github_help_button(self, "observation-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
        _edit_key = "⌘E" if sys.platform == "darwin" else "Alt-E"
        self._edit_key_label = _edit_key
        if self.allow_edit_images:
            self.edit_images_btn = QPushButton(
                self.tr("Edit images ({key})").format(key=_edit_key)
            )
            self.edit_images_btn.setMinimumHeight(35)
            self.edit_images_btn.setMinimumWidth(120)
            self.edit_images_btn.clicked.connect(self._on_edit_images_clicked)
            bottom_buttons.addWidget(self.edit_images_btn)
        cancel_btn = QPushButton(self.tr("Cancel"))
        cancel_btn.setMinimumHeight(35)
        cancel_btn.setStyleSheet("background-color: #e74c3c;")
        cancel_btn.clicked.connect(self.reject)
        bottom_buttons.addWidget(cancel_btn)
        self.submit_observation_btn = QPushButton(
            self.tr("Save Observation") if self.edit_mode else self.tr("Create Observation")
        )
        self.submit_observation_btn.setObjectName("primaryButton")
        self.submit_observation_btn.setMinimumHeight(35)
        self.submit_observation_btn.setAutoDefault(True)
        self.submit_observation_btn.setDefault(True)
        self.submit_observation_btn.clicked.connect(self.accept)
        bottom_buttons.addWidget(self.submit_observation_btn)
        main_layout.addLayout(bottom_buttons)
        main_layout.setStretch(0, 2)  # Observation details
        main_layout.setStretch(1, 3)  # Taxonomy
        main_layout.setStretch(2, 0)  # Images gallery stays content-sized

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
            self.tr("Place name for the observation. Manual edits are kept until you explicitly fetch the API name."),
        )
        self._register_hint_widget(
            self.location_lookup_apply_btn,
            self.tr("Replace the current place name with the latest API lookup result."),
            allow_when_disabled=True,
            disabled_hint=self.tr("The current place name already matches the latest API lookup."),
        )
        self._register_hint_widget(
            self.map_btn,
            self.tr("Open location in Google Maps"),
            allow_when_disabled=True,
            disabled_hint=self.tr("Enter coordinates to enable the map"),
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
        self._register_hint_widget(
            self.ai_guess_btn,
            "",
            allow_when_disabled=True,
        )
        self._register_hint_widget(
            self.ai_copy_btn,
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
        self._update_ai_button_hints()

    def _update_ai_button_hints(self) -> None:
        if hasattr(self, "ai_guess_btn"):
            guess_hint = (
                self.tr("Guess species using AI - select one or more thumbnails (shift/ctrl + click)")
                if self.ai_guess_btn.isEnabled()
                else self.tr("Select a field image to use AI recognition")
            )
            self._set_widget_hint(
                self.ai_guess_btn,
                guess_hint,
                allow_when_disabled=True,
            )
        if hasattr(self, "ai_copy_btn"):
            if self.ai_copy_btn.isEnabled():
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
                self.ai_copy_btn,
                copy_hint,
                allow_when_disabled=True,
            )

    def _ai_selection_has_non_field_image(self) -> bool:
        indices = self._selected_gallery_indices()
        if not indices:
            current = self._current_ai_index()
            if current is not None:
                indices = [current]
        indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
        if not indices:
            return False
        return any(
            (self.image_results[idx].image_type or "field").strip().lower() != "field"
            for idx in indices
        )

    def _build_ai_suggestions_group(self) -> QGroupBox:
        ai_group = QGroupBox(self.tr("AI suggestions"))
        ai_layout = QVBoxLayout(ai_group)
        ai_layout.setContentsMargins(6, 6, 6, 6)
        ai_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        ai_group.setMinimumWidth(0)

        ai_controls = QHBoxLayout()
        self.ai_guess_btn = QPushButton(self.tr("Guess"))
        self.ai_guess_btn.clicked.connect(self._on_ai_guess_clicked)
        self.ai_copy_btn = QPushButton(self.tr("Copy"))
        self.ai_copy_btn.clicked.connect(self._on_ai_copy_to_taxonomy)
        self.ai_copy_btn.setEnabled(False)
        self.ai_guess_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.ai_copy_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        ai_controls.addWidget(self.ai_guess_btn)
        ai_controls.addWidget(self.ai_copy_btn)
        ai_controls.setStretch(0, 1)
        ai_controls.setStretch(1, 1)
        ai_layout.addLayout(ai_controls)

        self.ai_table = QTableWidget(0, 3)
        self.ai_table.setHorizontalHeaderLabels([self.tr("Suggested species"), "Match", "Link"])
        self.ai_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.ai_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.ai_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.ai_table.verticalHeader().setVisible(False)
        self.ai_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.ai_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.ai_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.ai_table.setMinimumHeight(140)
        self.ai_table.setStyleSheet(
            "QTableWidget::item:selected { background-color: #f0f2f4; color: #2c3e50; font-weight: bold; }"
            "QTableWidget::item:selected:!active { background-color: #f0f2f4; color: #2c3e50; font-weight: bold; }"
        )
        self.ai_table.itemSelectionChanged.connect(self._on_ai_selection_changed)
        ai_layout.addWidget(self.ai_table)

        self.ai_status_label = QLabel("")
        self.ai_status_label.setWordWrap(True)
        self.ai_status_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        ai_layout.addWidget(self.ai_status_label)

        return ai_group

    def _apply_ai_state(self, ai_state: dict | None) -> None:
        if not ai_state:
            return
        predictions = ai_state.get("predictions")
        selected = ai_state.get("selected")
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
        if isinstance(selected_index, int):
            self._ai_selected_index = selected_index

    def get_ai_state(self) -> dict:
        return {
            "predictions": dict(self._ai_predictions_by_index),
            "selected": dict(self._ai_selected_by_index),
            "selected_index": self._ai_selected_index,
            "paths": [item.filepath for item in self.image_results],
            "image_ids": [item.image_id for item in self.image_results],
        }

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

    def _on_gallery_image_clicked(self, _image_id, path: str) -> None:
        if not path:
            return
        for idx, item in enumerate(self.image_results):
            if item.filepath == path:
                self._ai_selected_index = idx
                self._update_ai_controls_state()
                self._update_ai_table()
                return

    def _update_ai_controls_state(self) -> None:
        if not hasattr(self, "ai_guess_btn"):
            return
        indices = self._selected_gallery_indices()
        if not indices:
            index = self._current_ai_index()
            if index is not None:
                indices = [index]
        enable = False
        if indices:
            indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
            if indices:
                enable = all(
                    (self.image_results[idx].image_type or "field").strip().lower() == "field"
                    for idx in indices
                )
        if self._ai_thread is not None:
            enable = False
        self.ai_guess_btn.setEnabled(enable)
        self._update_ai_button_hints()
        if hasattr(self, "ai_table"):
            self._set_ai_copy_enabled(self.ai_table.currentRow() >= 0)
        else:
            self._set_ai_copy_enabled(False)

    def _update_ai_table(self) -> None:
        if not hasattr(self, "ai_table"):
            return
        index = self._current_ai_index()
        self.ai_table.setRowCount(0)
        if index is None:
            self._set_ai_copy_enabled(False)
            return
        predictions = self._ai_predictions_by_index.get(index, [])
        for row, pred in enumerate(predictions):
            taxon = pred.get("taxon", {})
            display_name = self._format_ai_taxon_name(taxon)
            confidence = pred.get("probability", 0.0)
            name_item = QTableWidgetItem(display_name)
            name_item.setData(Qt.UserRole, pred)
            conf_item = QTableWidgetItem(f"{confidence:.1%}")
            link_widget = self._build_taxon_link_widget(self._ai_prediction_links(pred, taxon))
            self.ai_table.insertRow(row)
            self.ai_table.setItem(row, 0, name_item)
            self.ai_table.setItem(row, 1, conf_item)
            if link_widget:
                self.ai_table.setCellWidget(row, 2, link_widget)
        if predictions:
            selected = self._ai_selected_by_index.get(index)
            if selected:
                for row in range(self.ai_table.rowCount()):
                    item = self.ai_table.item(row, 0)
                    if item and item.data(Qt.UserRole) == selected:
                        self.ai_table.selectRow(row)
                        break
            else:
                self.ai_table.selectRow(0)
            self._set_ai_copy_enabled(self.ai_table.currentRow() >= 0)
        else:
            self._ai_selected_taxon = None
            self._set_ai_copy_enabled(False)

    def _format_ai_taxon_name(self, taxon: dict) -> str:
        scientific = taxon.get("scientificName") or taxon.get("scientific_name") or taxon.get("name") or ""
        vernacular = self._preferred_vernacular_from_taxon(taxon) or ""
        if vernacular and scientific:
            vernacular_norm = str(vernacular).strip()
            scientific_norm = str(scientific).strip()
            if vernacular_norm and scientific_norm and vernacular_norm.casefold() != scientific_norm.casefold():
                return f"{vernacular_norm} ({scientific_norm})"
        return vernacular or scientific or self.tr("Unknown")

    def _ai_prediction_link(self, pred: dict, taxon: dict) -> str | None:
        if isinstance(pred, dict):
            for key in ("infoURL", "infoUrl", "info_url"):
                value = pred.get(key)
                if isinstance(value, str) and value.startswith("http"):
                    return value
        if not isinstance(taxon, dict):
            return None
        for key in ("infoURL", "infoUrl", "info_url"):
            value = taxon.get(key)
            if isinstance(value, str) and value.startswith("http"):
                return value
        for key in ("url", "link", "href", "uri"):
            value = taxon.get(key)
            if isinstance(value, str) and value.startswith("http"):
                return value
        taxon_id = (
            taxon.get("taxonId")
            or taxon.get("taxon_id")
            or taxon.get("TaxonId")
            or taxon.get("id")
        )
        if taxon_id:
            return f"https://artsdatabanken.no/arter/takson/{taxon_id}"
        return "https://artsdatabanken.no"

    def _ai_prediction_links(self, pred: dict, taxon: dict) -> list[tuple[str, str]]:
        links: list[tuple[str, str]] = []
        adb_url = self._ai_prediction_link(pred, taxon)
        if adb_url:
            links.append(("AdB.no", adb_url))
        genus, species = self._extract_genus_species_from_taxon(taxon)
        artportalen_taxon_id = ObservationDB.resolve_external_taxon_id(genus, species, "artportalen")
        if artportalen_taxon_id:
            links.append(("AP.se", f"https://dyntaxa.se/taxon/info/{int(artportalen_taxon_id)}"))
        return links

    def _build_taxon_link_widget(self, links: list[tuple[str, str]]) -> QLabel | None:
        valid_links = [
            (str(label or "").strip(), str(url or "").strip())
            for label, url in (links or [])
            if str(label or "").strip() and str(url or "").strip()
        ]
        if not valid_links:
            return None
        link_html = " | ".join(
            f'<a href="{html.escape(url, quote=True)}">{html.escape(label)}</a>'
            for label, url in valid_links
        )
        label = QLabel(link_html)
        label.setTextFormat(Qt.RichText)
        label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        label.setOpenExternalLinks(True)
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("QLabel { padding: 2px 6px; }")
        return label

    def _on_ai_selection_changed(self) -> None:
        index = self._current_ai_index()
        if index is None:
            return
        selected_items = self.ai_table.selectedItems()
        if not selected_items:
            self._ai_selected_taxon = None
            self._set_ai_status(None)
            self._set_ai_copy_enabled(False)
            return
        row_item = self.ai_table.item(self.ai_table.currentRow(), 0)
        if not row_item:
            return
        pred = row_item.data(Qt.UserRole) or {}
        self._ai_selected_by_index[index] = pred
        self._ai_selected_taxon = pred.get("taxon") or {}
        self._set_ai_status(None)
        self._set_ai_copy_enabled(True)

    def _set_ai_status(self, text: str | None, color: str = "#7f8c8d") -> None:
        if not hasattr(self, "ai_status_label"):
            return
        if not text:
            self.ai_status_label.setText("")
            return
        self.ai_status_label.setText(text)
        self.ai_status_label.setStyleSheet(f"color: {color}; font-size: {pt(9)}pt;")

    def _set_ai_copy_enabled(self, enabled: bool) -> None:
        if hasattr(self, "ai_copy_btn"):
            self.ai_copy_btn.setEnabled(bool(enabled) and self._can_copy_ai_to_current_tab())
            self._update_ai_button_hints()

    def _can_copy_ai_to_current_tab(self) -> bool:
        tabs = getattr(self, "taxonomy_tabs", None)
        if tabs is None:
            return True
        current = tabs.currentWidget()
        return current in {getattr(self, "species_tab", None), getattr(self, "grows_tab", None)}

    def _extract_genus_species_from_taxon(self, taxon: dict) -> tuple[str | None, str | None]:
        if not isinstance(taxon, dict):
            return None, None
        genus = taxon.get("genus") or taxon.get("genusName") or taxon.get("genus_name")
        species = (
            taxon.get("species")
            or taxon.get("specificEpithet")
            or taxon.get("specific_epithet")
        )
        if genus and species:
            return str(genus).strip(), str(species).strip()
        sci = taxon.get("scientificName") or taxon.get("scientific_name") or taxon.get("name")
        if sci and isinstance(sci, str):
            parts = sci.strip().split()
            if len(parts) >= 2:
                return parts[0], parts[1]
        return None, None

    def _on_ai_copy_to_taxonomy(self) -> None:
        taxon = self._ai_selected_taxon or {}
        genus, species = self._extract_genus_species_from_taxon(taxon)
        if not genus or not species:
            self._set_ai_status(self.tr("Could not parse genus/species from AI suggestion."), "#e67e22")
            return
        current_tab = self.taxonomy_tabs.currentWidget() if hasattr(self, "taxonomy_tabs") else None
        target_grows = current_tab == getattr(self, "grows_tab", None)
        if target_grows:
            self._host_suppress_taxon_autofill = True
            self.host_genus_input.setText(genus)
            self.host_species_input.setText(species)
            self._host_suppress_taxon_autofill = False
            if self.vernacular_db:
                self.host_vernacular_input.clear()
                self._maybe_set_host_vernacular_from_taxon()
            self._set_ai_status(self.tr("Copied to grows-on species."), "#27ae60")
        else:
            if hasattr(self, "taxonomy_tabs"):
                self.taxonomy_tabs.setCurrentIndex(self.taxonomy_tabs.indexOf(self.species_tab))
            if hasattr(self, "unknown_checkbox") and self.unknown_checkbox.isChecked():
                self.unknown_checkbox.setChecked(False)
            self._suppress_taxon_autofill = True
            if hasattr(self, "genus_input"):
                self.genus_input.setText(genus)
            if hasattr(self, "species_input"):
                self.species_input.setText(species)
            vernacular = self._preferred_vernacular_from_taxon(taxon)
            if hasattr(self, "vernacular_input"):
                self.vernacular_input.setText(vernacular or "")
            self._suppress_taxon_autofill = False
            if self.vernacular_db:
                self._update_vernacular_suggestions_for_taxon()
                if not vernacular:
                    self._maybe_set_vernacular_from_taxon()
            self._set_ai_status(self.tr("Copied to taxonomy."), "#27ae60")
        self._update_taxonomy_tab_indicators()

    def _on_ai_crop_clicked(self) -> None:
        return

    def _on_ai_guess_clicked(self) -> None:
        try:
            indices = self._selected_gallery_indices()
            if not indices:
                index = self._current_ai_index()
                if index is None or index < 0 or index >= len(self.image_results):
                    return
                indices = [index]
            indices = [idx for idx in indices if 0 <= idx < len(self.image_results)]
            if not indices:
                return
            if any(
                (self.image_results[idx].image_type or "field").strip().lower() != "field"
                for idx in indices
            ):
                self._set_ai_status(self.tr("AI guess only works for field photos"), "#e74c3c")
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
            if self._ai_thread is not None:
                return
            self.ai_guess_btn.setEnabled(False)
            self.ai_guess_btn.setText(self.tr("AI guessing..."))
            self._update_ai_button_hints()
            count = len(requests)
            self._set_ai_status(
                self.tr("Sending {count} image(s) to Artsdatabanken AI...").format(count=count),
                "#3498db",
            )
            temp_dir = get_images_dir() / "imports"
            self._ai_thread = AIGuessWorker(requests, temp_dir, max_dim=1600, parent=self)
            self._ai_thread.resultReady.connect(self._on_ai_guess_finished)
            self._ai_thread.error.connect(self._on_ai_guess_error)
            self._ai_thread.finished.connect(self._ai_thread.deleteLater)
            self._ai_thread.finished.connect(self._on_ai_thread_finished)
            self._ai_thread.start()
        except Exception as exc:
            self._set_ai_status(self.tr("AI guess failed: {message}").format(message=str(exc)), "#e74c3c")
            if hasattr(self, "ai_guess_btn"):
                self.ai_guess_btn.setEnabled(True)
                self.ai_guess_btn.setText(self.tr("Guess"))
                self._update_ai_button_hints()

    def _on_ai_thread_finished(self) -> None:
        self._ai_thread = None
        if hasattr(self, "ai_guess_btn"):
            self.ai_guess_btn.setText(self.tr("Guess"))
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
            try:
                parked_threads = getattr(a, "_sporely_parked_threads", None)
                if parked_threads is not None:
                    parked_threads.discard(t)
            except Exception:
                pass

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

    def done(self, result: int) -> None:  # noqa: N802 - Qt API
        self._cleanup_dialog_threads()
        super().done(result)

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
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass
        for index in indices or []:
            self._ai_predictions_by_index[index] = predictions or []
        self._update_ai_table()
        if predictions:
            self._set_ai_status(self.tr("AI suggestion updated"), "#27ae60")
        else:
            self._set_ai_status(self.tr("No AI suggestions found"), "#7f8c8d")
        self._update_ai_controls_state()

    def _on_ai_guess_error(self, _indices: list, message: str) -> None:
        if "500" in message:
            hint = self.tr("AI guess failed: server error (500). Try again later.")
        else:
            hint = self.tr("AI guess failed: {message}").format(message=message)
        self._set_ai_status(hint, "#e74c3c")
        self._update_ai_controls_state()

    def _on_edit_images_clicked(self):
        self.request_edit_images = True
        self.reject()

    def _on_image_double_clicked(self, _img_id, path: str) -> None:
        self.request_edit_images = True
        self.request_edit_images_path = path or None
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
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
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

        # If selected-language text is missing in AI payload, resolve from local taxonomy DB.
        genus, species = self._extract_genus_species_from_taxon(taxon)
        if self.vernacular_db and genus and species:
            try:
                db_name = self.vernacular_db.vernacular_from_taxon(genus, species)
            except Exception:
                db_name = None
            if isinstance(db_name, str) and db_name.strip():
                return db_name.strip()

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
            "Images (*.png *.jpg *.jpeg *.tif *.tiff *.orf *.nef *.heic *.heif);;All Files (*)"
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

    def _on_location_lookup_result(self, name: str):
        """Store the place name from the API and apply it only when appropriate."""
        resolved_name = str(name or "").strip()
        self._location_lookup_name = resolved_name
        current_name = (self.location_input.text() or "").strip() if hasattr(self, "location_input") else ""
        should_apply = not current_name or current_name == (self._last_applied_location_lookup_name or "").strip()
        if resolved_name and should_apply:
            self.location_input.setText(resolved_name)
            self._last_applied_location_lookup_name = resolved_name
        self._update_location_lookup_button_state()
        self._location_lookup_worker = None

    def _on_location_name_edited(self, _text: str) -> None:
        self._update_location_lookup_button_state()

    def _apply_lookup_location_name(self) -> None:
        resolved_name = (self._location_lookup_name or "").strip()
        if not resolved_name or not hasattr(self, "location_input"):
            return
        self.location_input.setText(resolved_name)
        self._last_applied_location_lookup_name = resolved_name
        self._update_location_lookup_button_state()

    def _update_location_lookup_button_state(self) -> None:
        button = getattr(self, "location_lookup_apply_btn", None)
        if button is None or not hasattr(self, "location_input"):
            return
        current_name = (self.location_input.text() or "").strip()
        resolved_name = (self._location_lookup_name or "").strip()
        button.setEnabled(bool(resolved_name) and current_name != resolved_name)

    def _on_location_lookup_worker_finished(self) -> None:
        worker = self.sender()
        if isinstance(worker, QThread):
            self._location_lookup_workers.discard(worker)
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
        self.lat_input.setValue(lat)
        self.lon_input.setValue(lon)
        self._update_map_button()

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

    def get_data(self):
        """Return observation data as dict."""
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        common_name = self.vernacular_input.text().strip() or None
        working_title = None

        # Get GPS values (None if at minimum/special value)
        lat = None
        lon = None
        if self.lat_input.value() > self.lat_input.minimum():
            lat = self.lat_input.value()
        if self.lon_input.value() > self.lon_input.minimum():
            lon = self.lon_input.value()

        publish_target = normalize_publish_target(self.publish_target_combo.currentData())
        nin2_path = self._selected_habitat_tree_path("nin2")
        substrate_path = self._selected_habitat_tree_path("substrate")
        nin2_labels = [str(node.get("name") or "").strip() for node in nin2_path if isinstance(node, dict)]
        substrate_labels = [str(node.get("name") or "").strip() for node in substrate_path if isinstance(node, dict)]
        nin2_ids = [int(node.get("id")) for node in nin2_path if isinstance(node, dict) and node.get("id") is not None]
        substrate_ids = [int(node.get("id")) for node in substrate_path if isinstance(node, dict) and node.get("id") is not None]
        host_genus = self.host_genus_input.text().strip() if hasattr(self, "host_genus_input") else ""
        host_species = self.host_species_input.text().strip() if hasattr(self, "host_species_input") else ""
        host_vernacular = self.host_vernacular_input.text().strip() if hasattr(self, "host_vernacular_input") else ""
        host_scientific = f"{host_genus} {host_species}".strip()
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
            'publish_target': publish_target,
            'species_guess': working_title,
            'uncertain': self.uncertain_checkbox.isChecked(),
            'unspontaneous': self.unspontaneous_checkbox.isChecked(),
            'determination_method': self.determination_method_combo.currentData(),
            'date': self.datetime_input.dateTime().toString("yyyy-MM-dd HH:mm"),
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
            'habitat_nin2_note': (self.nin2_note_input.toPlainText().strip() if hasattr(self, "nin2_note_input") else "") or None,
            'habitat_substrate_note': (self.substrate_note_input.toPlainText().strip() if hasattr(self, "substrate_note_input") else "") or None,
            'habitat_grows_on_note': (self.grows_on_note_input.toPlainText().strip() if hasattr(self, "grows_on_note_input") else "") or None,
            'gps_latitude': lat,
            'gps_longitude': lon
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
        return normalize_publish_target(
            SettingsDB.get_setting(SETTING_ACTIVE_REPORTING_TARGET, PUBLISH_TARGET_ARTSOBS_NO),
            fallback=PUBLISH_TARGET_ARTSOBS_NO,
        )

    def _country_name_from_coords(self) -> str:
        lat = self.lat_input.value() if hasattr(self, "lat_input") else None
        lon = self.lon_input.value() if hasattr(self, "lon_input") else None
        if lat is not None and hasattr(self, "lat_input") and lat <= self.lat_input.minimum():
            lat = None
        if lon is not None and hasattr(self, "lon_input") and lon <= self.lon_input.minimum():
            lon = None
        inferred = infer_publish_target_from_coords(lat, lon)
        if inferred == PUBLISH_TARGET_ARTPORTALEN_SE:
            return self.tr("Sweden")
        if inferred == PUBLISH_TARGET_ARTSOBS_NO:
            return self.tr("Norway")
        if lat is not None and lon is not None:
            return self._reporting_system_name()
        return ""

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
        if group is not None:
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
        if getattr(self, "_publish_target_manual_override", False):
            self._refresh_location_reporting_summary()
            return
        lat = self.lat_input.value() if hasattr(self, "lat_input") else None
        lon = self.lon_input.value() if hasattr(self, "lon_input") else None
        if lat is not None and hasattr(self, "lat_input") and lat <= self.lat_input.minimum():
            lat = None
        if lon is not None and hasattr(self, "lon_input") and lon <= self.lon_input.minimum():
            lon = None
        inferred = infer_publish_target_from_coords(lat, lon)
        if inferred:
            self._set_publish_target_combo(inferred, manual_override=False)
            return
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
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is not None and not self._dialog_gallery_splitter_syncing:
            sizes = splitter.sizes()
            gallery_height = sizes[1] if len(sizes) >= 2 else None
            QTimer.singleShot(0, lambda h=gallery_height: self._apply_dialog_gallery_splitter_height(h))

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
        target_dim = 224
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
            thumb_preview = get_thumbnail_path(item.image_id, "224x224")
            if thumb_preview and Path(thumb_preview).exists():
                return str(thumb_preview)
        for candidate in (item.preview_path, item.filepath):
            preview_path = self._build_dialog_preview_path(candidate or "")
            if preview_path:
                return preview_path
        return item.filepath or ""

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
            badges = ImageGalleryWidget.build_image_type_badges(
                image_type=item.image_type,
                objective_name=objective_short,
                contrast=item.contrast,
                custom_scale=bool(item.custom_scale),
                needs_scale=needs_scale,
                translate=self.tr,
            )
            has_measurements = False
            if item.image_id:
                has_measurements = bool(MeasurementDB.get_measurements_for_image(item.image_id))
            items.append(
                {
                    "id": item.image_id,
                    "filepath": item.filepath,
                    "preview_path": self._image_gallery_preview_path(item),
                    "image_number": idx + 1,
                    "crop_box": item.ai_crop_box,
                    "crop_source_size": item.ai_crop_source_size,
                    "gps_tag_text": self.tr("GPS") if gps_match else None,
                    "gps_tag_highlight": gps_match,
                    "badges": badges,
                    "has_measurements": has_measurements,
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

    def _on_gallery_delete_requested(self, image_key) -> None:
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
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        if lang == "no":
            return self.tr("Namn:")
        if lang in {"en", "de"}:
            return self.tr("Name:")
        return self.tr("Name:")

    def _vernacular_placeholder(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
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
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if not db_path:
            return
        if self.vernacular_db and self.vernacular_db.db_path == db_path:
            self.vernacular_db.language_code = lang
        else:
            self.vernacular_db = VernacularDB(db_path, language_code=lang)
        self._refresh_vernacular_for_current_taxon()
        self._refresh_host_vernacular_for_current_taxon()
        self._update_taxonomy_tab_indicators()

    def _populate_vernacular_language_menu(self) -> None:
        button = getattr(self, "vernacular_language_btn", None)
        menu = getattr(self, "vernacular_language_menu", None)
        if button is None or menu is None:
            return
        menu.clear()
        current = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
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
        current = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
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
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if not db_path:
            return
        self.vernacular_db = VernacularDB(db_path, language_code=lang)
        self._vernacular_model = QStringListModel()
        self._vernacular_completer = QCompleter(self._vernacular_model, self)
        self._vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.vernacular_input.setCompleter(self._vernacular_completer)
        vernacular_popup = self._vernacular_completer.popup()
        if vernacular_popup:
            self._style_dropdown_popup_readability(vernacular_popup, self.vernacular_input)
        self._vernacular_completer.activated.connect(self._on_vernacular_selected)
        self.vernacular_input.textChanged.connect(self._on_vernacular_text_changed)
        self.vernacular_input.editingFinished.connect(self._on_vernacular_editing_finished)
        self.vernacular_input.installEventFilter(self)

        self._genus_model = QStringListModel()
        self._genus_completer = QCompleter(self._genus_model, self)
        self._genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.genus_input.setCompleter(self._genus_completer)
        genus_popup = self._genus_completer.popup()
        if genus_popup:
            self._style_dropdown_popup_readability(genus_popup, self.genus_input)
        self._genus_completer.activated.connect(self._on_genus_selected)
        self.genus_input.textChanged.connect(self._on_genus_text_changed)
        self.genus_input.editingFinished.connect(self._on_genus_editing_finished)

        self._species_model = QStringListModel()
        self._species_completer = QCompleter(self._species_model, self)
        self._species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.species_input.setCompleter(self._species_completer)
        species_popup = self._species_completer.popup()
        if species_popup:
            self._style_dropdown_popup_readability(species_popup, self.species_input)
        self._species_completer.activated.connect(self._on_species_selected)
        self.species_input.textChanged.connect(self._on_species_text_changed)
        self.species_input.editingFinished.connect(self._on_species_editing_finished)

        self.genus_input.installEventFilter(self)
        self.species_input.installEventFilter(self)

    def _setup_host_autocomplete(self):
        """Autocomplete for Habitat -> Grows on genus/species/vernacular fields."""
        if not self.vernacular_db:
            return
        if (
            not hasattr(self, "host_genus_input")
            or not hasattr(self, "host_species_input")
            or not hasattr(self, "host_vernacular_input")
        ):
            return

        self._host_genus_model = QStringListModel()
        self._host_genus_completer = QCompleter(self._host_genus_model, self)
        self._host_genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._host_genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.host_genus_input.setCompleter(self._host_genus_completer)
        host_genus_popup = self._host_genus_completer.popup()
        if host_genus_popup:
            self._style_dropdown_popup_readability(host_genus_popup, self.host_genus_input)
        self._host_genus_completer.activated.connect(self._on_host_genus_selected)
        self.host_genus_input.textChanged.connect(self._on_host_genus_text_changed)
        self.host_genus_input.editingFinished.connect(self._on_host_genus_editing_finished)
        self.host_genus_input.installEventFilter(self)

        self._host_species_model = QStringListModel()
        self._host_species_completer = QCompleter(self._host_species_model, self)
        self._host_species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._host_species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.host_species_input.setCompleter(self._host_species_completer)
        host_species_popup = self._host_species_completer.popup()
        if host_species_popup:
            self._style_dropdown_popup_readability(host_species_popup, self.host_species_input)
        self._host_species_completer.activated.connect(self._on_host_species_selected)
        self.host_species_input.textChanged.connect(self._on_host_species_text_changed)
        self.host_species_input.editingFinished.connect(self._on_host_species_editing_finished)
        self.host_species_input.installEventFilter(self)

        self._host_vernacular_model = QStringListModel()
        self._host_vernacular_completer = QCompleter(self._host_vernacular_model, self)
        self._host_vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._host_vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.host_vernacular_input.setCompleter(self._host_vernacular_completer)
        vern_popup = self._host_vernacular_completer.popup()
        if vern_popup:
            self._style_dropdown_popup_readability(vern_popup, self.host_vernacular_input)
        self._host_vernacular_completer.activated.connect(self._on_host_vernacular_selected)
        self.host_vernacular_input.textChanged.connect(self._on_host_vernacular_text_changed)
        self.host_vernacular_input.editingFinished.connect(self._on_host_vernacular_editing_finished)
        self.host_vernacular_input.installEventFilter(self)

    def _on_host_genus_text_changed(self, text: str) -> None:
        if not self.vernacular_db or self._host_suppress_taxon_autofill:
            return
        value = (text or "").strip()
        suggestions = self.vernacular_db.suggest_genus(value)
        self._host_genus_model.setStringList(suggestions[:30])
        if not value:
            self._host_suppress_taxon_autofill = True
            self.host_species_input.clear()
            self.host_vernacular_input.clear()
            self._host_suppress_taxon_autofill = False

    def _on_host_genus_selected(self, genus: str) -> None:
        if self._host_genus_completer:
            self._host_genus_completer.popup().hide()
        if not self.vernacular_db:
            return
        if self.host_species_input.text().strip():
            return
        suggestions = self.vernacular_db.suggest_species(str(genus).strip(), "")
        self._host_species_model.setStringList(suggestions[:30])

    def _on_host_genus_editing_finished(self) -> None:
        if self._host_genus_completer and self._host_genus_completer.popup().isVisible():
            return
        if self._host_suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted(host=True)
        self._update_host_vernacular_suggestions_for_taxon()
        self._maybe_set_host_vernacular_from_taxon()

    def _on_host_species_text_changed(self, text: str) -> None:
        if self._host_suppress_taxon_autofill:
            return
        genus = self.host_genus_input.text().strip()
        if not genus:
            self._host_species_model.setStringList([])
            self._set_host_vernacular_placeholder_from_suggestions([])
            return
        prefix = (text or "").strip()
        suggestions = self.vernacular_db.suggest_species(genus, prefix) if self.vernacular_db else []
        if prefix and any(s.lower() == prefix.lower() for s in suggestions):
            self._host_species_model.setStringList([])
            if self._host_species_completer:
                self._host_species_completer.popup().hide()
        else:
            self._host_species_model.setStringList(suggestions[:30])
            if self._host_species_model.stringList() and self._host_species_completer:
                self._host_species_completer.setCompletionPrefix(prefix)
                self._host_species_completer.complete()
        if prefix:
            self._update_host_vernacular_suggestions_for_taxon()
            self._maybe_set_host_vernacular_from_taxon()

    def _on_host_species_selected(self, species: str) -> None:
        if self._host_species_completer:
            self._host_species_completer.popup().hide()
        _ = species
        self._resolve_current_taxon_to_accepted(host=True)
        self._update_host_vernacular_suggestions_for_taxon()
        self._maybe_set_host_vernacular_from_taxon()

    def _on_host_species_editing_finished(self) -> None:
        if self._host_species_completer and self._host_species_completer.popup().isVisible():
            return
        if self._host_suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted(host=True)
        self._update_host_vernacular_suggestions_for_taxon()
        self._maybe_set_host_vernacular_from_taxon()

    def _on_host_vernacular_text_changed(self, text: str) -> None:
        if not self.vernacular_db or not hasattr(self, "_host_vernacular_model"):
            return
        if self._host_suppress_taxon_autofill:
            return
        value = (text or "").strip()
        if not value:
            self._update_host_vernacular_suggestions_for_taxon()
            return
        genus = self.host_genus_input.text().strip() or None
        species = self.host_species_input.text().strip() or None
        entries = self.vernacular_db.suggest_vernacular_entries(value, genus=genus, species=species)
        text_label = self._format_vernacular_suggestion_label(
            {"vernacular_name": value, "genus": genus, "species": species}
        )
        if any(self._format_vernacular_suggestion_label(entry).casefold() == text_label.casefold() for entry in entries):
            self._set_vernacular_entries(self._host_vernacular_model, [], host=True)
            if self._host_vernacular_completer:
                self._host_vernacular_completer.popup().hide()
        else:
            self._set_vernacular_entries(self._host_vernacular_model, entries, host=True, limit=20)

    def _on_host_vernacular_selected(self, text: str) -> None:
        self._set_host_taxon_from_vernacular(text)

    def _on_host_vernacular_editing_finished(self) -> None:
        self._set_host_taxon_from_vernacular(self.host_vernacular_input.text())

    def _set_host_taxon_from_vernacular(self, name: str) -> None:
        if not self.vernacular_db:
            return
        value = (name or "").strip()
        if not value:
            return
        entry = self._lookup_vernacular_entry(value, host=True)
        if entry:
            self._host_suppress_taxon_autofill = True
            try:
                self.host_vernacular_input.setText(str(entry.get("vernacular_name") or "").strip())
            finally:
                self._host_suppress_taxon_autofill = False
            taxon = (entry.get("genus"), entry.get("species"), entry.get("family"))
        else:
            taxon = self.vernacular_db.taxon_from_vernacular(value)
        if not taxon:
            return
        genus, species, _family = taxon
        current_genus = self.host_genus_input.text().strip()
        current_species = self.host_species_input.text().strip()
        if current_genus and current_species:
            return
        self._host_suppress_taxon_autofill = True
        if not current_genus:
            self.host_genus_input.setText(genus)
        if not current_species:
            self.host_species_input.setText(species)
        self._host_suppress_taxon_autofill = False

    def _maybe_set_host_vernacular_from_taxon(self) -> None:
        if not self.vernacular_db:
            return
        if self.host_vernacular_input.text().strip():
            return
        genus = self.host_genus_input.text().strip()
        species = self.host_species_input.text().strip()
        if not genus or not species:
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            self._set_host_vernacular_placeholder_from_suggestions([])
            return
        if len(suggestions) == 1:
            self._host_suppress_taxon_autofill = True
            self.host_vernacular_input.setText(suggestions[0])
            self._host_suppress_taxon_autofill = False
            self._set_host_vernacular_placeholder_from_suggestions([])
        else:
            self._set_host_vernacular_placeholder_from_suggestions(suggestions)

    def _update_host_vernacular_suggestions_for_taxon(self) -> None:
        if not self.vernacular_db or not hasattr(self, "_host_vernacular_model"):
            return
        genus = self.host_genus_input.text().strip() or None
        species = self.host_species_input.text().strip() or None
        if not genus and not species:
            self._set_vernacular_entries(self._host_vernacular_model, [], host=True)
            self._set_host_vernacular_placeholder_from_suggestions([])
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        entries = [{"vernacular_name": suggestion, "genus": genus, "species": species} for suggestion in suggestions]
        self._set_vernacular_entries(self._host_vernacular_model, entries, host=True, limit=20)
        self._set_host_vernacular_placeholder_from_suggestions(suggestions)

    def _set_host_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "host_vernacular_input"):
            return
        if not suggestions:
            self.host_vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        preview = "; ".join(suggestions[:4])
        self.host_vernacular_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

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
    def _format_vernacular_suggestion_label(entry: dict) -> str:
        vernacular_name = str(entry.get("vernacular_name") or "").strip()
        genus = str(entry.get("genus") or "").strip()
        species = str(entry.get("species") or "").strip()
        scientific_name = " ".join(part for part in (genus, species) if part).strip()
        if vernacular_name and scientific_name:
            return f"{vernacular_name} ({scientific_name})"
        return vernacular_name or scientific_name

    def _set_vernacular_entries(
        self,
        model: QStringListModel | None,
        entries: list[dict],
        *,
        host: bool = False,
        limit: int = 30,
    ) -> None:
        if model is None:
            return
        labels: list[str] = []
        entry_map: dict[str, dict] = {}
        for entry in entries[:limit]:
            label = self._format_vernacular_suggestion_label(entry)
            if not label:
                continue
            labels.append(label)
            entry_map[label.casefold()] = entry
        model.setStringList(labels)
        if host:
            self._host_vernacular_entry_map = entry_map
        else:
            self._vernacular_entry_map = entry_map

    def _lookup_vernacular_entry(self, text: str, *, host: bool = False) -> dict | None:
        mapping = getattr(self, "_host_vernacular_entry_map", {}) if host else getattr(self, "_vernacular_entry_map", {})
        return mapping.get(str(text or "").strip().casefold())

    def _resolve_current_taxon_to_accepted(self, *, host: bool = False) -> bool:
        if not self.vernacular_db:
            return False
        if host:
            genus_widget = getattr(self, "host_genus_input", None)
            species_widget = getattr(self, "host_species_input", None)
            suppress_attr = "_host_suppress_taxon_autofill"
        else:
            genus_widget = getattr(self, "genus_input", None)
            species_widget = getattr(self, "species_input", None)
            suppress_attr = "_suppress_taxon_autofill"
        if genus_widget is None or species_widget is None:
            return False
        genus = genus_widget.text().strip()
        species = species_widget.text().strip()
        if not genus or not species:
            return False
        resolved = self.vernacular_db.taxon_from_scientific(genus, species)
        if not resolved:
            return False
        accepted_genus, accepted_species, _family = resolved
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
        if not self.vernacular_db:
            return
        if self._suppress_taxon_autofill:
            return
        if not text.strip():
            self._update_vernacular_suggestions_for_taxon()
            return
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        entries = self.vernacular_db.suggest_vernacular_entries(text, genus=genus, species=species)
        text_label = self._format_vernacular_suggestion_label(
            {"vernacular_name": text.strip(), "genus": genus, "species": species}
        )
        if any(self._format_vernacular_suggestion_label(entry).casefold() == text_label.casefold() for entry in entries):
            self._set_vernacular_entries(self._vernacular_model, [], host=False)
            if self._vernacular_completer:
                self._vernacular_completer.popup().hide()
        else:
            self._set_vernacular_entries(self._vernacular_model, entries, host=False, limit=30)

    def _update_vernacular_suggestions_for_taxon(self):
        if not self.vernacular_db:
            return
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        if not genus and not species:
            self._set_vernacular_entries(self._vernacular_model, [], host=False)
            self._set_vernacular_placeholder_from_suggestions([])
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        entries = [{"vernacular_name": suggestion, "genus": genus, "species": species} for suggestion in suggestions]
        self._set_vernacular_entries(self._vernacular_model, entries, host=False, limit=30)
        self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _set_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "vernacular_input"):
            return
        if not suggestions:
            self.vernacular_input.setPlaceholderText(self._vernacular_placeholder())
            return
        preview = "; ".join(suggestions[:4])
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
        
        if not self.vernacular_db:
            return
        entry = self._lookup_vernacular_entry(name, host=False)
        if entry:
            self._suppress_taxon_autofill = True
            try:
                self.vernacular_input.setText(str(entry.get("vernacular_name") or "").strip())
            finally:
                self._suppress_taxon_autofill = False
            taxon = (entry.get("genus"), entry.get("species"), entry.get("family"))
        else:
            taxon = self.vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            current_genus = self.genus_input.text().strip()
            current_species = self.species_input.text().strip()
            if current_genus and current_species:
                return
            self._suppress_taxon_autofill = True
            if not current_genus:
                self.genus_input.setText(genus)
            if not current_species:
                self.species_input.setText(species)
            self._suppress_taxon_autofill = False
            self._sync_taxon_cache()

    def _on_vernacular_editing_finished(self):
        if not self.vernacular_db:
            return
        name = self.vernacular_input.text().strip()
        if not name:
            return
        entry = self._lookup_vernacular_entry(name, host=False)
        if entry:
            self._suppress_taxon_autofill = True
            try:
                self.vernacular_input.setText(str(entry.get("vernacular_name") or "").strip())
            finally:
                self._suppress_taxon_autofill = False
            taxon = (entry.get("genus"), entry.get("species"), entry.get("family"))
        else:
            taxon = self.vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            current_genus = self.genus_input.text().strip()
            current_species = self.species_input.text().strip()
            if current_genus and current_species:
                return
            self._suppress_taxon_autofill = True
            if not current_genus:
                self.genus_input.setText(genus)
            if not current_species:
                self.species_input.setText(species)
            self._suppress_taxon_autofill = False
            self._sync_taxon_cache()

    def _on_genus_text_changed(self, text):
        if not self.vernacular_db:
            return
        if self._suppress_taxon_autofill:
            return
        text = text.strip()
        suggestions = self.vernacular_db.suggest_genus(text)
        
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
            # Reset species completer filtering
            if self._species_completer:
                self._species_completer.setCompletionPrefix("")
            self._set_species_placeholder_from_suggestions([])
            return

        # Reset species completer filtering when genus changes
        if self._species_completer and not self.species_input.hasFocus():
            self._species_completer.setCompletionPrefix("")
        
        if not self.species_input.text().strip():
            species_suggestions = self.vernacular_db.suggest_species(text, "")
            self._set_species_placeholder_from_suggestions(species_suggestions)

    def _on_genus_editing_finished(self):
        if self._genus_completer and self._genus_completer.popup().isVisible():
            return
        if not self.vernacular_db or self._suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted()
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()
        genus = self.genus_input.text().strip()
        if genus and not self.species_input.text().strip():
            species_suggestions = self.vernacular_db.suggest_species(genus, "")
            self._set_species_placeholder_from_suggestions(species_suggestions)

    def _on_genus_selected(self, genus):
        # Hide the popup after selection
        if self._genus_completer:
            self._genus_completer.popup().hide()
        
        if not self.vernacular_db:
            return
        if self.species_input.text().strip():
            return
        species_suggestions = self.vernacular_db.suggest_species(str(genus).strip(), "")
        self._set_species_placeholder_from_suggestions(species_suggestions)

    def _on_species_selected(self, species):
        """Handle species selection from completer."""
        # Hide the popup after selection
        if self._species_completer:
            self._species_completer.popup().hide()
        
        # Update vernacular name suggestions
        if self.vernacular_db:
            self._resolve_current_taxon_to_accepted()
            self._maybe_set_vernacular_from_taxon()

    def _on_species_editing_finished(self):
        if self._species_completer and self._species_completer.popup().isVisible():
            return
        if not self.vernacular_db or self._suppress_taxon_autofill:
            return
        self._resolve_current_taxon_to_accepted()
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()

    def _on_species_text_changed(self, text):
        if self._suppress_taxon_autofill:
            return
        genus = self.genus_input.text().strip()
        if not genus:
            self._species_model.setStringList([])
            return
        text_stripped = (text or "").strip()
        if self.vernacular_db:
            suggestions = self.vernacular_db.suggest_species(genus, text_stripped)
        else:
            from database.reference_db import ReferenceDB
            suggestions = ReferenceDB.list_species(genus, text_stripped)

        # Hide popup when text exactly matches any suggestion (covers multi-result cases)
        if text_stripped and any(s.lower() == text_stripped.lower() for s in suggestions):
            self._species_model.setStringList([])
            if self._species_completer:
                self._species_completer.popup().hide()
        else:
            self._species_model.setStringList(suggestions)
            if self._species_model.stringList() and self._species_completer:
                self._species_completer.setCompletionPrefix(text_stripped)
                self._species_completer.complete()

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
            if current_common and self.vernacular_db and genus and species:
                suggestions = self.vernacular_db.suggest_vernacular_for_taxon(
                    genus=genus,
                    species=species
                )
                matches = any(
                    name.strip().lower() == current_common.lower()
                    for name in suggestions
                )
                if not matches:
                    self._suppress_taxon_autofill = True
                    self.vernacular_input.clear()
                    self._suppress_taxon_autofill = False
                    # Reset vernacular completer filtering after clearing
                    if self._vernacular_completer:
                        self._vernacular_completer.setCompletionPrefix("")
        self._last_genus = genus
        self._last_species = species

    def _sync_taxon_cache(self):
        self._last_genus = self.genus_input.text().strip()
        self._last_species = self.species_input.text().strip()

    def eventFilter(self, obj, event):
        if event.type() == QEvent.FocusIn and self.vernacular_db:
            if obj == self.vernacular_input:
                if not self.vernacular_input.text().strip():
                    # Reset completer filtering when focusing empty field
                    if self._vernacular_completer:
                        self._vernacular_completer.setCompletionPrefix("")
                    self._update_vernacular_suggestions_for_taxon()
                    if self._vernacular_model.stringList():
                        self._vernacular_completer.complete()
            elif obj == self.genus_input:
                text = self.genus_input.text().strip()
                suggestions = self.vernacular_db.suggest_genus(text)
                self._genus_model.setStringList(suggestions)
                if suggestions:
                    self._genus_completer.complete()
            elif obj == self.species_input:
                genus = self.genus_input.text().strip()
                if genus:
                    text = self.species_input.text().strip()
                    suggestions = self.vernacular_db.suggest_species(genus, text)
                    self._species_model.setStringList(suggestions)
                    if suggestions:
                        self._species_completer.complete()
            elif obj == self.host_genus_input:
                text = self.host_genus_input.text().strip()
                suggestions = self.vernacular_db.suggest_genus(text)
                self._host_genus_model.setStringList(suggestions[:30])
                if suggestions:
                    self._host_genus_completer.complete()
            elif obj == self.host_species_input:
                genus = self.host_genus_input.text().strip()
                if genus:
                    text = self.host_species_input.text().strip()
                    suggestions = self.vernacular_db.suggest_species(genus, text)
                    self._host_species_model.setStringList(suggestions[:30])
                    if suggestions:
                        self._host_species_completer.complete()
            elif obj == self.host_vernacular_input:
                if not self.host_vernacular_input.text().strip():
                    if self._host_vernacular_completer:
                        self._host_vernacular_completer.setCompletionPrefix("")
                    self._update_host_vernacular_suggestions_for_taxon()
                    if self._host_vernacular_model.stringList():
                        self._host_vernacular_completer.complete()
        return super().eventFilter(obj, event)

    def _maybe_set_vernacular_from_taxon(self):
        if not self.vernacular_db:
            return
        if not hasattr(self, "vernacular_input"):
            return
        if self.vernacular_input.text().strip():
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        if not genus or not species:
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            self._set_vernacular_placeholder_from_suggestions([])
            return
        if len(suggestions) == 1:
            self._suppress_taxon_autofill = True
            self.vernacular_input.setText(suggestions[0])
            self._suppress_taxon_autofill = False
            self._set_vernacular_placeholder_from_suggestions([])
        else:
            self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _refresh_vernacular_for_current_taxon(self) -> None:
        if not self.vernacular_db or not hasattr(self, "vernacular_input"):
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        if not genus or not species:
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            return
        old_block = self.vernacular_input.blockSignals(True)
        try:
            self._suppress_taxon_autofill = True
            self.vernacular_input.setText(suggestions[0])
        finally:
            self._suppress_taxon_autofill = False
            self.vernacular_input.blockSignals(old_block)
        self._set_vernacular_placeholder_from_suggestions([])

    def _refresh_host_vernacular_for_current_taxon(self) -> None:
        if not self.vernacular_db or not hasattr(self, "host_genus_input") or not hasattr(self, "host_species_input") or not hasattr(self, "host_vernacular_input"):
            return
        genus = self.host_genus_input.text().strip()
        species = self.host_species_input.text().strip()
        if not genus or not species:
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            return
        old_block = self.host_vernacular_input.blockSignals(True)
        try:
            self.host_vernacular_input.setText(suggestions[0])
        finally:
            self.host_vernacular_input.blockSignals(old_block)

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
                "sample_type": item.sample_type
            })
        return entries

    def _load_habitat_tree(self, filename: str) -> list[dict]:
        path = Path(__file__).resolve().parents[1] / "database" / filename
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
        field_width = 360
        group = QGroupBox(title)
        group_layout = QFormLayout(group)
        group_layout.setSpacing(6)
        level_count = max(1, min(6, self._tree_depth(roots)))
        combos: list[QComboBox] = []
        for level in range(level_count):
            combo = QComboBox()
            combo.setEditable(True)
            combo.setInsertPolicy(QComboBox.NoInsert)
            if expand_fields:
                combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            else:
                combo.setFixedWidth(field_width)
                combo.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
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
            group_layout.addRow(self.tr("Level {n}:").format(n=level + 1), row_widget)
            combos.append(combo)
        tree_index = self._build_habitat_tree_index(roots)
        self._habitat_tree_states[key] = {
            "roots": roots,
            "combos": combos,
            "index": tree_index,
            "group": group,
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
        obs = obs or {}
        date_str = obs.get("date")
        if date_str:
            dt = _parse_observation_datetime(date_str)
            if dt and dt.isValid():
                self.datetime_input.setDateTime(dt)

        genus = obs.get("genus") or ""
        species = obs.get("species") or ""
        self.taxonomy_tabs.setCurrentIndex(0)
        self.genus_input.setText(genus)
        self.species_input.setText(species)
        self.uncertain_checkbox.setChecked(bool(obs.get("uncertain", 0)))
        if hasattr(self, "vernacular_input"):
            self.vernacular_input.setText(obs.get("common_name") or "")

        self.unspontaneous_checkbox.setChecked(bool(obs.get("unspontaneous", 0)))
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
        self._maybe_set_vernacular_from_taxon()
        self._update_taxonomy_tab_indicators()

    def _load_existing_observation(self):
        """Preload observation details and images for editing."""
        self._load_observation_values(self.observation)


class RenameObservationDialog(QDialog):
    """Dialog for renaming an observation."""

    def __init__(self, observation, parent=None):
        super().__init__(parent)
        self.observation = observation
        self.setWindowTitle(self.tr("Rename Observation"))
        self.setModal(True)
        self.setMinimumWidth(400)
        self.init_ui()

    def init_ui(self):
        layout = QFormLayout(self)
        layout.setSpacing(10)

        self.unknown_checkbox = QCheckBox(self.tr("Unknown"))
        self.unknown_checkbox.toggled.connect(self.on_unknown_toggled)

        self.title_input = QLineEdit()
        self.title_input.setPlaceholderText(self.tr("Working title (e.g., Unknown 1)"))
        self.title_input.setText(self.observation.get('species_guess') or "")

        unknown_row = QHBoxLayout()
        unknown_row.addWidget(self.unknown_checkbox)
        self.working_title_container = QWidget()
        working_title_layout = QHBoxLayout(self.working_title_container)
        working_title_layout.setContentsMargins(0, 0, 0, 0)
        working_title_layout.setSpacing(6)
        working_title_layout.addWidget(QLabel(self.tr("Working title:")))
        working_title_layout.addWidget(self.title_input)
        unknown_row.addWidget(self.working_title_container)
        layout.addRow("", unknown_row)

        self.genus_input = QLineEdit()
        self.genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.genus_input.setText(self.observation.get('genus') or "")
        layout.addRow("Genus:", self.genus_input)

        self.species_input = QLineEdit()
        self.species_input.setPlaceholderText(self.tr("e.g., velutipes"))
        self.species_input.setText(self.observation.get('species') or "")
        layout.addRow("Species:", self.species_input)

        self.uncertain_checkbox = QCheckBox(self.tr("Uncertain identification"))
        self.uncertain_checkbox.setChecked(bool(self.observation.get('uncertain', 0)))
        layout.addRow("", self.uncertain_checkbox)

        button_layout = QHBoxLayout()
        save_btn = QPushButton(self.tr("Save"))
        save_btn.setObjectName("primaryButton")
        save_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton(self.tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(save_btn)
        button_layout.addWidget(cancel_btn)
        layout.addRow(button_layout)

        genus = self.observation.get('genus')
        species = self.observation.get('species')
        guess = self.observation.get('species_guess')
        unknown_checked = bool(guess) and not (genus or species)
        self.unknown_checkbox.setChecked(unknown_checked)
        self.on_unknown_toggled(unknown_checked)

    def get_data(self):
        """Return updated observation data."""
        working_title = self.title_input.text().strip() or None
        if not self.unknown_checkbox.isChecked():
            working_title = None
        return {
            'species_guess': working_title,
            'genus': self.genus_input.text().strip() or None,
            'species': self.species_input.text().strip() or None,
            'uncertain': self.uncertain_checkbox.isChecked()
        }

    def on_unknown_toggled(self, checked):
        """Show working title and disable genus/species when unknown."""
        self.working_title_container.setVisible(checked)
        self.title_input.setEnabled(checked)
        self.genus_input.setEnabled(not checked)
        self.species_input.setEnabled(not checked)
        if checked:
            self.genus_input.clear()
            self.species_input.clear()


# Helpers for vernacular language lookup are in utils.vernacular_utils.


def _normalize_taxon_text_impl(self, value: str | None) -> str:
    if not value:
        return ""
    try:
        import unicodedata
        text = unicodedata.normalize("NFKC", str(value))
    except Exception:
        text = str(value)
    text = text.replace("\u00a0", " ")
    text = text.strip()
    if text.startswith("?"):
        text = text.lstrip("?").strip()
    return " ".join(text.split())


class VernacularDB:
    """Simple helper for vernacular name lookup."""

    def __init__(self, db_path: Path, language_code: str | None = None):
        self.db_path = db_path
        self.language_code = normalize_vernacular_language(language_code) if language_code else None
        self._has_language_column = None
        self._tables: set[str] | None = None

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _table_names(self) -> set[str]:
        if self._tables is None:
            with self._connect() as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                self._tables = {str(row[0] or "") for row in cur.fetchall()}
        return self._tables

    def _has_scientific_name_table(self) -> bool:
        return "scientific_name_min" in self._table_names()

    def _has_language(self) -> bool:
        if self._has_language_column is None:
            with self._connect() as conn:
                cur = conn.execute("PRAGMA table_info(vernacular_min)")
                self._has_language_column = any(row[1] == "language_code" for row in cur.fetchall())
        return bool(self._has_language_column)

    def _language_clause(self, language_code: str | None) -> tuple[str, list[str]]:
        if not self._has_language():
            return "", []
        raw = language_code or self.language_code
        if not raw:
            return "", []
        lang = normalize_vernacular_language(raw)
        if not lang:
            return "", []
        return " AND v.language_code = ? ", [lang]

    def list_languages(self) -> list[str]:
        if not self._has_language():
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT language_code
                FROM vernacular_min
                WHERE language_code IS NOT NULL AND language_code != ''
                ORDER BY language_code
                """
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular(self, prefix: str, genus: str | None = None, species: str | None = None) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.vernacular_name
                LIMIT 200
                """,
                (prefix, genus, genus, species, species, *lang_params),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular_entries(
        self,
        prefix: str,
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        prefix = prefix.strip()
        if not prefix:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name, t.genus, t.specific_epithet, t.family, v.is_preferred_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ? COLLATE NOCASE)
                  AND (? IS NULL OR t.specific_epithet = ? COLLATE NOCASE)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name, t.genus, t.specific_epithet
                LIMIT ?
                """,
                (prefix, genus, genus, species, species, *lang_params, int(limit)),
            )
            return [
                {
                    "vernacular_name": row[0],
                    "genus": row[1],
                    "species": row[2],
                    "family": row[3],
                    "is_preferred_name": bool(row[4]),
                }
                for row in cur.fetchall()
                if row and row[0] and row[1] and row[2]
            ]

    def suggest_vernacular_for_taxon(
        self, genus: str | None = None, species: str | None = None, limit: int = 200
    ) -> list[str]:
        genus = genus.strip() if genus else None
        species = species.strip() if species else None
        if not genus and not species:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT ?
                """,
                (genus, genus, species, species, *lang_params, limit),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_genus(self, prefix: str) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT genus
                FROM taxon_min
                WHERE genus LIKE ? || '%'
                ORDER BY genus
                LIMIT 200
                """,
                (prefix,),
            )
            for row in cur.fetchall():
                genus = str(row[0] or "").strip()
                lowered = genus.casefold()
                if genus and lowered not in seen:
                    seen.add(lowered)
                    values.append(genus)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' %'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (prefix,),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    genus = scientific_name.split(" ", 1)[0].strip() if scientific_name else ""
                    lowered = genus.casefold()
                    if genus and lowered not in seen:
                        seen.add(lowered)
                        values.append(genus)
            return values[:200]

    def suggest_species(self, genus: str, prefix: str) -> list[str]:
        genus = genus.strip()
        prefix = prefix.strip()
        if not genus:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT specific_epithet
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet LIKE ? || '%'
                ORDER BY specific_epithet
                LIMIT 200
                """,
                (genus, prefix),
            )
            for row in cur.fetchall():
                species = str(row[0] or "").strip()
                lowered = species.casefold()
                if species and lowered not in seen:
                    seen.add(lowered)
                    values.append(species)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' ' || ? || '%'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (genus, prefix),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    parts = scientific_name.split()
                    if len(parts) < 2 or parts[0].casefold() != genus.casefold():
                        continue
                    species = parts[1].strip()
                    lowered = species.casefold()
                    if species and lowered not in seen:
                        seen.add(lowered)
                        values.append(species)
            return values[:200]

    def taxon_from_scientific(self, genus: str, species: str) -> tuple[str, str, str | None] | None:
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        scientific_name = f"{genus} {species}".strip()
        with self._connect() as conn:
            cur = conn.cursor()
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT t.genus, t.specific_epithet, t.family
                    FROM taxon_min t
                    LEFT JOIN scientific_name_min s ON s.taxon_id = t.taxon_id
                    WHERE (
                            t.genus = ? COLLATE NOCASE
                        AND t.specific_epithet = ? COLLATE NOCASE
                    )
                       OR (
                            t.canonical_scientific_name = ? COLLATE NOCASE
                    )
                       OR (
                            s.scientific_name = ? COLLATE NOCASE
                    )
                    ORDER BY
                        CASE
                            WHEN t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE THEN 0
                            WHEN s.is_preferred_name = 1 THEN 1
                            ELSE 2
                        END,
                        t.genus,
                        t.specific_epithet
                    LIMIT 1
                    """,
                    (genus, species, scientific_name, scientific_name, genus, species),
                )
            else:
                cur.execute(
                    """
                    SELECT genus, specific_epithet, family
                    FROM taxon_min
                    WHERE genus = ? COLLATE NOCASE
                      AND specific_epithet = ? COLLATE NOCASE
                    ORDER BY genus, specific_epithet
                    LIMIT 1
                    """,
                    (genus, species),
                )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_from_vernacular(self, name: str) -> tuple[str, str, str | None] | None:
        name = name.strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.genus, t.specific_epithet, t.family
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name = ?
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (name, *lang_params),
            )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def vernacular_from_taxon(self, genus: str, species: str) -> str | None:
        if not genus or not species:
            return None
        resolved = self.taxon_from_scientific(genus, species)
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE t.genus = ? COLLATE NOCASE
                  AND t.specific_epithet = ? COLLATE NOCASE
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (genus, species, *lang_params),
            )
            row = cur.fetchone()
            return row[0] if row else None
