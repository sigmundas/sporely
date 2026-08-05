"""Review and resolve Sporely Cloud sync conflicts."""
from __future__ import annotations

import json
import base64
from datetime import datetime
from pathlib import Path
import re
import tempfile
import time

from PySide6.QtCore import QBuffer, QByteArray, QIODevice, QSize, Qt, QThread, Signal
from PySide6.QtGui import QColor, QImage, QImageReader, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFrame,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QCheckBox,
    QButtonGroup,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QSplitter,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from utils.cloud_sync import (
    CloudSyncError,
    SporelyCloudClient,
    _resolve_existing_local_image_asset_path,
    get_conflict_detail,
    resolve_conflict_keep_cloud,
    resolve_conflict_keep_local,
    resolve_conflict_merge,
    resolve_conflict_plan,
)
from database.models import ObservationDB
from database.schema import get_app_settings


class ConflictResolutionWorker(QThread):
    progress = Signal(str, int, int)
    resolution_finished = Signal(bool)
    error = Signal(str)

    def __init__(self, decisions, prepare_images_cb=None):
        super().__init__()
        self.setObjectName("Cloud conflict resolution")
        self.decisions = decisions
        self.prepare_images_cb = prepare_images_cb

    def run(self):
        try:
            client = SporelyCloudClient.from_stored_credentials()
            if not client:
                raise CloudSyncError('Not logged in to Sporely Cloud')
            total = len(self.decisions)
            resolved_any = False
            for i, dec in enumerate(self.decisions):
                if self.isInterruptionRequested():
                    return
                local_id = dec['local_id']
                cloud_id = dec['cloud_id']
                action = dec['action']
                self.progress.emit(f"Resolving conflict {i+1} of {total}...", i, total)
                
                result = None
                if action == 'keep_local':
                    result = resolve_conflict_keep_local(client, local_id, prepare_images_cb=self.prepare_images_cb)
                elif action == 'keep_cloud':
                    allow_delete = dec.get('allow_delete', False)
                    def _retryable_cloud_error(exc: Exception) -> bool:
                        message = str(exc or '').lower()
                        return any(
                            token in message
                            for token in (
                                'connection aborted',
                                'connection reset',
                                'remote disconnected',
                                'connection broken',
                                'timed out',
                                'read timed out',
                                'broken pipe',
                            )
                        )
                    try:
                        result = resolve_conflict_keep_cloud(client, local_id, cloud_id=cloud_id or None, allow_delete=allow_delete)
                    except Exception as exc:
                        if not _retryable_cloud_error(exc):
                            raise
                        client_retry = SporelyCloudClient.from_stored_credentials()
                        if client_retry is None:
                            raise CloudSyncError('Not logged in to Sporely Cloud')
                        result = resolve_conflict_keep_cloud(client_retry, local_id, cloud_id=cloud_id or None, allow_delete=allow_delete)
                elif action == 'merge':
                    result = resolve_conflict_merge(client, local_id, cloud_id=cloud_id or None, prepare_images_cb=self.prepare_images_cb)
                elif action == 'plan':
                    result = resolve_conflict_plan(
                        client,
                        local_id,
                        cloud_id=cloud_id or None,
                        plan=dict(dec.get('plan') or {}),
                        prepare_images_cb=self.prepare_images_cb,
                    )
                if isinstance(result, dict) and result.get('blocked'):
                    blocked_reason = str(result.get('blocked_reason') or '').strip()
                    if blocked_reason:
                        self.progress.emit(f"Conflict {i+1} blocked: {blocked_reason}", i + 1, total)
                resolved_any = True
            
            self.progress.emit("Conflict resolution finished.", total, total)
            self.resolution_finished.emit(resolved_any)
        except Exception as e:
            self.error.emit(str(e))


def _format_timestamp(value) -> str:
    text = str(value or '').strip()
    if not text:
        return 'Unknown'
    normalized = text.replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return text.replace('T', ' ')


def _format_compare_value(field: str, value) -> str:
    if value is None or value == '':
        return '—'
    if field in {'location_public', 'is_draft'}:
        return 'Yes' if bool(value) else 'No'
    if field in {'visibility', 'sharing_scope'}:
        return str(value).capitalize()
    if field in {'gps_latitude', 'gps_longitude'}:
        try:
            return f'{float(value):.7f}'
        except Exception:
            return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _first_nonempty(*values) -> str:
    for value in values:
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _observation_species(obs: dict | None) -> str:
    record = dict(obs or {})
    genus = str(record.get('genus') or '').strip()
    species = str(record.get('species') or '').strip()
    if genus and species:
        return f'{genus} {species}'
    return _first_nonempty(
        record.get('species_guess'),
        record.get('common_name'),
    ) or 'Unknown species'


def _split_observation_date_time(value) -> tuple[str, str]:
    text = str(value or '').strip()
    if not text:
        return '—', '—'
    normalized = text.replace('T', ' ').replace('Z', '+00:00')
    time_match = re.search(r'(\d{1,2}:\d{2})(?::\d{2})?', normalized)
    time_text = time_match.group(1) if time_match else '—'
    try:
        dt = datetime.fromisoformat(normalized)
        date_text = dt.strftime('%Y-%m-%d')
        return date_text, time_text
    except Exception:
        pass
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', normalized)
    date_text = date_match.group(1) if date_match else text
    return date_text, time_text


def _conflict_identity(detail: dict) -> dict:
    local_obs = dict(detail.get('local_observation') or {})
    remote_obs = dict(detail.get('remote_observation') or {})
    species = _first_nonempty(
        _observation_species(local_obs),
        _observation_species(remote_obs),
        detail.get('title'),
    ) or 'Observation'
    local_date, local_time = _split_observation_date_time(local_obs.get('date'))
    remote_date, remote_time = _split_observation_date_time(remote_obs.get('date'))
    date_text = local_date if local_date != '—' else remote_date
    time_text = local_time if local_time != '—' else remote_time
    location_text = _first_nonempty(local_obs.get('location'), remote_obs.get('location')) or '—'
    return {
        'species': species,
        'date': date_text,
        'time': time_text,
        'location': location_text,
    }
def _conflict_headline(identity: dict) -> str:
    """One-line human summary used everywhere the observation is named."""
    species = str(identity.get('species') or 'Unknown species').strip() or 'Unknown species'
    date_text = str(identity.get('date') or '').strip()
    if date_text and date_text != '—':
        return f'{species} — {date_text}'
    return species


def _conflict_subtitle(identity: dict, detail: dict) -> str:
    parts: list[str] = []
    location_text = str(identity.get('location') or '').strip()
    if location_text and location_text != '—':
        parts.append(location_text)
    time_text = str(identity.get('time') or '').strip()
    if time_text and time_text != '—':
        parts.append(f'at {time_text}')
    local_id = int(detail.get('local_id') or 0)
    cloud_id = str(detail.get('cloud_id') or '').strip()
    id_bits = []
    if local_id:
        id_bits.append(f'local #{local_id}')
    if cloud_id:
        id_bits.append(f'cloud {cloud_id}')
    if id_bits:
        parts.append('(' + ' · '.join(id_bits) + ')')
    return ' · '.join(parts)


def _conflict_list_label(detail: dict) -> str:
    identity = _conflict_identity(detail)
    headline = _conflict_headline(identity)
    subtitle = _conflict_subtitle(identity, detail)
    return f'{headline}\n{subtitle}' if subtitle else headline


def _join_english(items: list[str]) -> str:
    values = [str(item or '').strip() for item in (items or []) if str(item or '').strip()]
    if not values:
        return ''
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f'{values[0]} and {values[1]}'
    return ', '.join(values[:-1]) + f', and {values[-1]}'


def _readable_text_color(background: QColor) -> QColor:
    luminance = (
        0.2126 * background.redF()
        + 0.7152 * background.greenF()
        + 0.0722 * background.blueF()
    )
    return QColor('#111827') if luminance > 0.56 else QColor('#f9fafb')


def _changed_cell_colors(widget: QWidget, side: str) -> tuple[QColor, QColor]:
    window = widget.palette().color(QPalette.Window)
    dark = window.lightnessF() < 0.5
    if side == 'local':
        background = QColor('#b7e4c7') if not dark else QColor('#285943')
    else:
        background = QColor('#f4c7ab') if not dark else QColor('#70412f')
    return background, _readable_text_color(background)


def _read_only_cloud_client() -> SporelyCloudClient | None:
    """Build a fixed-token client without invoking any login or refresh path."""
    settings = get_app_settings()
    access_token = str(settings.get('cloud_access_token') or '').strip()
    user_id = str(settings.get('cloud_user_id') or '').strip()
    if not access_token or not user_id:
        return None
    try:
        payload_part = access_token.split('.')[1]
        payload_part += '=' * (-len(payload_part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_part.encode('ascii')))
        if float(payload.get('exp') or 0) <= time.time():
            raise CloudSyncError('Authentication expired; sign in again to review cloud differences.')
    except CloudSyncError:
        raise
    except Exception as exc:
        raise CloudSyncError('Stored cloud authentication is invalid; sign in again.') from exc
    client = SporelyCloudClient(access_token=access_token, user_id=user_id, refresh_token=None)
    # Every PostgREST read made by get_conflict_detail now has refresh disabled.
    client._get = client.get_read_only  # type: ignore[method-assign]
    return client


class ConflictDetailWorker(QThread):
    """Load one comparison without blocking Qt's event loop."""

    loaded = Signal(int, str, object)
    failed = Signal(int, str, str)

    def __init__(self, generation: int, key: str, conflict: dict, parent=None):
        super().__init__(parent)
        self.generation = generation
        self.key = key
        self.conflict = dict(conflict)

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            client = _read_only_cloud_client()
            if client is None:
                raise CloudSyncError('Authentication expired')
            detail = get_conflict_detail(
                client,
                int(self.conflict.get('local_id') or 0),
                str(self.conflict.get('cloud_id') or '').strip() or None,
            )
            if self.isInterruptionRequested():
                return
            self.loaded.emit(self.generation, self.key, detail)
        except Exception as exc:
            self.failed.emit(self.generation, self.key, str(exc))


class ConflictThumbnailWorker(QThread):
    """Read a local image or download a cloud image; never mutates sync state."""

    loaded = Signal(int, str, QByteArray)
    failed = Signal(int, str, str)

    def __init__(self, generation: int, cache_key: str, side: str, source: str, parent=None):
        super().__init__(parent)
        self.generation = generation
        self.cache_key = cache_key
        self.side = side
        self.source = source

    def run(self) -> None:
        try:
            if self.isInterruptionRequested():
                return
            if not self.source:
                raise CloudSyncError('Image location is unavailable')
            if self.side == 'local':
                path = _resolve_existing_local_image_asset_path(self.source)
                if path is None:
                    raise CloudSyncError('Local file unavailable')
                reader = QImageReader(str(path))
                reader.setAutoTransform(True)
                source_size = reader.size()
                if source_size.isValid():
                    source_size.scale(QSize(480, 300), Qt.KeepAspectRatio)
                    reader.setScaledSize(source_size)
                preview = reader.read()
                if preview.isNull():
                    raise CloudSyncError(reader.errorString() or 'Local file unavailable')
                encoded = QByteArray()
                buffer = QBuffer(encoded)
                buffer.open(QIODevice.WriteOnly)
                if not preview.save(buffer, 'PNG'):
                    raise CloudSyncError('Could not create local thumbnail')
                data = bytes(encoded)
            else:
                client = _read_only_cloud_client()
                if client is None:
                    raise CloudSyncError('Authentication expired')
                with tempfile.TemporaryDirectory(prefix='sporely_conflict_thumb_') as directory:
                    destination = Path(directory) / 'thumbnail'
                    downloaded = client.download_image_file(self.source, destination)
                    data = Path(downloaded).read_bytes()
            if not data:
                raise CloudSyncError('Image is unavailable')
            if self.isInterruptionRequested():
                return
            self.loaded.emit(self.generation, self.cache_key, QByteArray(data))
        except Exception as exc:
            self.failed.emit(self.generation, self.cache_key, str(exc))


_ACTIVE_COMPARISON_WORKERS: set[QThread] = set()


def _retain_worker(worker: QThread) -> None:
    """Keep a parentless network worker alive until its blocking read returns."""
    _ACTIVE_COMPARISON_WORKERS.add(worker)

    def _release() -> None:
        _ACTIVE_COMPARISON_WORKERS.discard(worker)
        worker.deleteLater()

    worker.finished.connect(_release)


class PhotoCard(QFrame):
    def __init__(self, side: str, image: dict | None, status: str, parent=None):
        super().__init__(parent)
        self.side = side
        self.image = dict(image or {})
        self.setFrameShape(QFrame.StyledPanel)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        self.thumbnail = QLabel(self.tr('Unavailable'), self)
        self.thumbnail.setAlignment(Qt.AlignCenter)
        self.thumbnail.setMinimumSize(180, 120)
        self.thumbnail.setStyleSheet('background: #25282d; color: #9ca3af; border-radius: 5px;')
        layout.addWidget(self.thumbnail)
        if not self.image:
            self.thumbnail.setText(self.tr('No counterpart'))
            self.setEnabled(False)
            return
        image_type = str(self.image.get('image_type') or 'field').strip().lower()
        type_text = self.tr('Microscope') if image_type == 'microscope' else self.tr('Field')
        position = self.image.get('sort_order')
        layout.addWidget(QLabel(self.tr('{type} image {position}').format(
            type=type_text, position=position if position is not None else self.tr('unknown')
        )))
        id_bits = []
        if self.image.get('local_id'):
            id_bits.append(self.tr('local image #{id}').format(id=self.image['local_id']))
        if self.image.get('cloud_id'):
            id_bits.append(self.tr('cloud image {id}').format(id=self.image['cloud_id']))
        diagnostic = QLabel(' · '.join(id_bits), self)
        diagnostic_palette = diagnostic.palette()
        diagnostic_palette.setColor(
            QPalette.WindowText, self.palette().color(QPalette.PlaceholderText)
        )
        diagnostic.setPalette(diagnostic_palette)
        diagnostic.setStyleSheet('font-size: 11px;')
        diagnostic.setWordWrap(True)
        layout.addWidget(diagnostic)
        layout.addWidget(QLabel(self.tr('Measurements: {count}').format(
            count=int(self.image.get('measurement_count') or 0)
        )))
        status_label = QLabel(_status_display(status, self), self)
        status_palette = status_label.palette()
        status_palette.setColor(QPalette.WindowText, self.palette().color(QPalette.Link))
        status_label.setPalette(status_palette)
        status_label.setStyleSheet('font-weight: 600;')
        layout.addWidget(status_label)
        if status == 'local_only' and side == 'local':
            consequence = QLabel(self.tr(
                'If you choose “Use Sporely Cloud”: this photo remains in the local database and '
                'its source file is not deleted. The observation is marked synced; the image link '
                'is left unchanged, and eligible pending media may be offered by a later image-enabled sync.'
            ), self)
            consequence.setWordWrap(True)
            consequence.setStyleSheet('font-size: 11px;')
            layout.addWidget(consequence)
        elif status == 'cloud_only' and side == 'cloud':
            consequence = QLabel(self.tr(
                'If you choose “Use this device”: this cloud photo remains in Sporely Cloud. Its '
                'absence on this device is not treated as deletion consent.'
            ), self)
            consequence.setWordWrap(True)
            consequence.setStyleSheet('font-size: 11px;')
            layout.addWidget(consequence)

    def show_loading(self) -> None:
        self.thumbnail.setText(self.tr('Loading thumbnail…'))

    def show_error(self, message: str) -> None:
        lowered = str(message or '').lower()
        if 'auth' in lowered or 'login' in lowered:
            text = self.tr('Authentication expired')
        elif self.side == 'local':
            text = self.tr('Local file unavailable')
        else:
            text = self.tr('Cloud thumbnail unavailable')
        self.thumbnail.setText(text)
        self.thumbnail.setToolTip(str(message or text))

    def show_image(self, data: QByteArray) -> None:
        image = QImage.fromData(data)
        if image.isNull():
            self.show_error(self.tr('Unsupported image data'))
            return
        pixmap = QPixmap.fromImage(image).scaled(
            240, 150, Qt.KeepAspectRatio, Qt.SmoothTransformation
        )
        self.thumbnail.setPixmap(pixmap)
        self.thumbnail.setText('')


def _status_display(status: str, widget: QWidget) -> str:
    labels = {
        'same': widget.tr('Same'),
        'local_only': widget.tr('Only on this device'),
        'cloud_only': widget.tr('Only on Sporely Cloud'),
        'metadata_differs': widget.tr('Metadata differs'),
        'measurements_differ': widget.tr('Measurements differ'),
        'values_differ': widget.tr('Values differ'),
        'possible_match': widget.tr('Possible match — review required'),
        'identity_conflict': widget.tr('Identity conflict — automatic resolution unavailable'),
        'unavailable': widget.tr('Unavailable'),
    }
    return labels.get(str(status or ''), widget.tr('Unavailable'))


class CloudConflictDialog(QDialog):
    def __init__(self, parent=None, *, conflicts: list[dict] | None = None, prepare_images_cb=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr('Review cloud sync conflicts'))
        self.setModal(True)
        self.resize(1240, 820)
        self._prepare_images_cb = prepare_images_cb
        self._conflicts = [dict(row or {}) for row in (conflicts or [])]
        self._detail_cache: dict[str, dict] = {}
        self._detail_workers: set[ConflictDetailWorker] = set()
        self._thumbnail_workers: set[ConflictThumbnailWorker] = set()
        self._thumbnail_cache: dict[str, QByteArray | str] = {}
        self._thumbnail_cards: dict[str, list[PhotoCard]] = {}
        self._selection_generation = 0
        self._closing = False
        self._current_detail: dict | None = None
        self._choice_groups: dict[str, QButtonGroup] = {}
        self._choice_specs: dict[str, dict] = {}
        self.decisions: list[dict] = []
        self.resolved_any = False

        root = QVBoxLayout(self)
        intro = QLabel(self.tr(
            'Safe automatic changes have already synced. This screen shows only unresolved '
            'differences. Nothing in this review is changed until you confirm a resolution action.'
        ), self)
        intro.setWordWrap(True)
        root.addWidget(intro)
        note = QLabel(self.tr(
            'Review later closes without applying any conflict decision. The observations remain '
            'pending and will appear again on a later sync.'
        ), self)
        note.setWordWrap(True)
        note_palette = note.palette()
        note_palette.setColor(QPalette.WindowText, self.palette().color(QPalette.PlaceholderText))
        note.setPalette(note_palette)
        root.addWidget(note)

        self._status_label = QLabel('', self)
        self._status_label.setWordWrap(True)
        self._status_label.hide()
        root.addWidget(self._status_label)

        splitter = QSplitter(Qt.Horizontal, self)
        root.addWidget(splitter, 1)
        left = QWidget(splitter)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addWidget(QLabel(self.tr('Conflicted observations'), left))
        self._list = QListWidget(left)
        self._list.setWordWrap(True)
        self._list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._list.currentRowChanged.connect(self._on_selection_changed)
        left_layout.addWidget(self._list)
        splitter.addWidget(left)

        self._right_scroll = QScrollArea(splitter)
        self._right_scroll.setWidgetResizable(True)
        right = QWidget(self._right_scroll)
        self._right_layout = QVBoxLayout(right)
        self._title_label = QLabel(self.tr('Select a conflicted observation'), right)
        self._title_label.setStyleSheet('font-size: 17px; font-weight: 700;')
        self._title_label.setWordWrap(True)
        self._right_layout.addWidget(self._title_label)
        self._identity_label = QLabel('', right)
        identity_palette = self._identity_label.palette()
        identity_palette.setColor(
            QPalette.WindowText, self.palette().color(QPalette.PlaceholderText)
        )
        self._identity_label.setPalette(identity_palette)
        self._identity_label.setWordWrap(True)
        self._right_layout.addWidget(self._identity_label)
        self._overview_label = QLabel('', right)
        self._overview_label.setWordWrap(True)
        overview_palette = self._overview_label.palette()
        overview_palette.setColor(QPalette.Window, self.palette().color(QPalette.AlternateBase))
        overview_palette.setColor(QPalette.WindowText, self.palette().color(QPalette.Text))
        self._overview_label.setPalette(overview_palette)
        self._overview_label.setAutoFillBackground(True)
        self._overview_label.setStyleSheet('border-radius: 6px; padding: 8px;')
        self._right_layout.addWidget(self._overview_label)
        self._baseline_label = QLabel('', right)
        self._baseline_label.setWordWrap(True)
        baseline_palette = self._baseline_label.palette()
        baseline_palette.setColor(
            QPalette.WindowText, self.palette().color(QPalette.PlaceholderText)
        )
        self._baseline_label.setPalette(baseline_palette)
        self._right_layout.addWidget(self._baseline_label)
        self._field_status = QLabel(self.tr('Loading comparison…'), right)
        self._field_status.setStyleSheet('font-weight: 600;')
        self._right_layout.addWidget(self._field_status)
        self._compare_table = QTableWidget(0, 4, right)
        self._compare_table.setHorizontalHeaderLabels([
            self.tr('Field'), self.tr('Last synced'), self.tr('On this device'),
            self.tr('On Sporely Cloud'),
        ])
        self._compare_table.verticalHeader().setVisible(False)
        self._compare_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._compare_table.setSelectionMode(QTableWidget.NoSelection)
        self._compare_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._right_layout.addWidget(self._compare_table)
        self._field_choices_container = QWidget(right)
        self._field_choices_layout = QVBoxLayout(self._field_choices_container)
        self._field_choices_layout.setContentsMargins(0, 0, 0, 0)
        self._right_layout.addWidget(self._field_choices_container)
        self._show_matching_check = QCheckBox(
            self.tr('Show matching photos and measurements'), right
        )
        self._show_matching_check.setChecked(False)
        self._show_matching_check.toggled.connect(self._matching_visibility_changed)
        self._right_layout.addWidget(self._show_matching_check)
        self._photos_heading = QLabel(self.tr('Photo and measurement comparison'), right)
        self._photos_heading.setStyleSheet('font-size: 15px; font-weight: 700;')
        self._right_layout.addWidget(self._photos_heading)
        self._photo_headings = QWidget(right)
        headings_layout = QHBoxLayout(self._photo_headings)
        headings_layout.setContentsMargins(0, 0, 0, 0)
        headings_layout.addWidget(QLabel(self.tr('On this device'), self._photo_headings), 1)
        headings_layout.addWidget(QLabel(self.tr('On Sporely Cloud'), self._photo_headings), 1)
        self._right_layout.addWidget(self._photo_headings)
        self._photos_container = QWidget(right)
        self._photos_layout = QVBoxLayout(self._photos_container)
        self._photos_layout.setContentsMargins(0, 0, 0, 0)
        self._right_layout.addWidget(self._photos_container)
        self._right_layout.addStretch(1)
        self._right_scroll.setWidget(right)
        splitter.addWidget(self._right_scroll)
        splitter.setSizes([310, 930])

        self._apply_all_check = QCheckBox(self.tr('Apply this choice to all remaining conflicts'), self)
        root.addWidget(self._apply_all_check)
        actions = QHBoxLayout()
        self._refresh_btn = QPushButton(self.tr('Refresh comparison'), self)
        self._refresh_btn.clicked.connect(self._refresh_current_detail)
        actions.addWidget(self._refresh_btn)
        actions.addStretch(1)
        self._keep_local_btn = QPushButton(self.tr('Prefer this device'), self)
        self._keep_local_btn.clicked.connect(lambda: self._apply_preset('local'))
        actions.addWidget(self._keep_local_btn)
        self._keep_remote_btn = QPushButton(self.tr('Prefer Sporely Cloud'), self)
        self._keep_remote_btn.clicked.connect(lambda: self._apply_preset('cloud'))
        actions.addWidget(self._keep_remote_btn)
        self._merge_btn = QPushButton(self.tr('Keep safe additions'), self)
        self._merge_btn.clicked.connect(lambda: self._apply_preset('safe'))
        actions.addWidget(self._merge_btn)
        self._apply_btn = QPushButton(self.tr('Apply selected changes'), self)
        self._apply_btn.clicked.connect(self._apply_selected_changes)
        actions.addWidget(self._apply_btn)
        self._review_later_btn = QPushButton(self.tr('Review later'), self)
        self._review_later_btn.setToolTip(self.tr(
            'Close without applying any conflict decision. The observation remains pending and '
            'will be shown again on a later sync.'
        ))
        self._review_later_btn.clicked.connect(self.reject)
        self._review_later_btn.setDefault(True)
        actions.addWidget(self._review_later_btn)
        root.addLayout(actions)

        self._reload_list()
        self._set_resolution_enabled(False)
        if self._conflicts:
            self._list.setCurrentRow(0)
        else:
            self._show_status(self.tr('No unresolved Sporely Cloud conflicts.'), 'success')

    def _key(self, conflict: dict) -> str:
        return f"{int(conflict.get('local_id') or 0)}::{str(conflict.get('cloud_id') or '').strip()}"

    def _reload_list(self) -> None:
        current_key = self._key(self._current_conflict()) if self._current_conflict() else ''
        self._list.blockSignals(True)
        self._list.clear()
        selected_row = -1
        for index, conflict in enumerate(self._conflicts):
            key = self._key(conflict)
            detail = self._detail_cache.get(key) or conflict.get('detail')
            if detail:
                label = self._list_label(detail)
            else:
                local_id = int(conflict.get('local_id') or 0)
                try:
                    local_observation = ObservationDB.get_observation(local_id) or {}
                except Exception:
                    local_observation = {}
                if local_observation:
                    identity_detail = {
                        'local_id': local_id,
                        'cloud_id': str(conflict.get('cloud_id') or ''),
                        'local_observation': local_observation,
                        'remote_observation': {},
                    }
                    label = _conflict_list_label(identity_detail)
                else:
                    label = self.tr('Observation local #{local_id} · cloud {cloud_id}').format(
                        local_id=local_id, cloud_id=str(conflict.get('cloud_id') or '?')
                    )
            item = QListWidgetItem(label)
            item.setToolTip(label)
            item.setData(Qt.UserRole, dict(conflict))
            self._list.addItem(item)
            if key == current_key:
                selected_row = index
        self._list.blockSignals(False)
        if selected_row >= 0:
            self._list.setCurrentRow(selected_row)

    def _list_label(self, detail: dict) -> str:
        base = _conflict_list_label(detail)
        fields = len(detail.get('field_rows') or [])
        pairs = detail.get('image_pairs') or []
        local_only = sum(1 for pair in pairs if pair.get('status') in {'local_only', 'possible_match'} and pair.get('local'))
        cloud_only = sum(1 for pair in pairs if pair.get('status') in {'cloud_only', 'possible_match'} and pair.get('remote'))
        measurements = len(detail.get('measurement_conflicts') or [])
        badges = self.tr('{fields} fields · {local} local-only · {cloud} cloud-only · {measurements} measurements').format(
            fields=fields, local=local_only, cloud=cloud_only, measurements=measurements
        )
        return f'{base}\n{badges}'

    def _current_conflict(self) -> dict | None:
        item = self._list.currentItem()
        data = item.data(Qt.UserRole) if item else None
        return dict(data) if isinstance(data, dict) else None

    def _start_detail_load(self, row: int, *, force: bool = False) -> None:
        if not 0 <= row < len(self._conflicts):
            return
        conflict = self._conflicts[row]
        key = self._key(conflict)
        if not force and key in self._detail_cache:
            if row == self._list.currentRow():
                self._populate_detail(self._detail_cache[key])
            return
        generation = self._selection_generation
        worker = ConflictDetailWorker(generation, key, conflict)
        _retain_worker(worker)
        self._detail_workers.add(worker)
        worker.loaded.connect(self._detail_loaded)
        worker.failed.connect(self._detail_failed)
        worker.finished.connect(lambda worker=worker: self._detail_workers.discard(worker))
        worker.start()
        if row == self._list.currentRow():
            self._show_loading()

    def _on_selection_changed(self, row: int) -> None:
        self._selection_generation += 1
        self._current_detail = None
        self._show_loading()
        if row < 0:
            return
        conflict = self._current_conflict()
        if conflict and self._key(conflict) in self._detail_cache:
            self._populate_detail(self._detail_cache[self._key(conflict)])
        else:
            self._start_detail_load(row)

    def _detail_loaded(self, generation: int, key: str, detail: object) -> None:
        if self._closing or generation != self._selection_generation or not isinstance(detail, dict):
            return
        self._detail_cache[key] = dict(detail)
        for conflict in self._conflicts:
            if self._key(conflict) == key:
                conflict['detail'] = dict(detail)
                break
        current = self._current_conflict()
        if current and self._key(current) == key:
            self._populate_detail(detail)
        self._update_list_item(key, detail)

    def _detail_failed(self, generation: int, key: str, message: str) -> None:
        if self._closing or generation != self._selection_generation:
            return
        current = self._current_conflict()
        if not current or self._key(current) != key:
            return
        self._show_loading()
        lowered = message.lower()
        friendly = self.tr('Authentication expired') if 'auth' in lowered or 'login' in lowered else self.tr('Could not load cloud details')
        self._show_status(f'{friendly}: {message}', 'error')

    def _update_list_item(self, key: str, detail: dict) -> None:
        for row in range(self._list.count()):
            item = self._list.item(row)
            data = item.data(Qt.UserRole)
            if isinstance(data, dict) and self._key(data) == key:
                item.setText(self._list_label(detail))
                break

    def _show_loading(self) -> None:
        self._current_detail = None
        self._title_label.setText(self.tr('Loading comparison…'))
        self._identity_label.clear()
        self._overview_label.setText(self.tr('Loading comparison…'))
        self._baseline_label.clear()
        self._field_status.setText(self.tr('Loading comparison…'))
        self._compare_table.hide()
        self._clear_photo_rows()
        self._set_resolution_enabled(False)
        self._refresh_btn.setEnabled(False)

    def _populate_detail(self, detail: dict) -> None:
        self._current_detail = dict(detail)
        self._choice_groups.clear()
        self._choice_specs.clear()
        identity = _conflict_identity(detail)
        self._title_label.setText(_conflict_headline(identity))
        self._identity_label.setText(_conflict_subtitle(identity, detail))
        self._overview_label.setText(self._overview(detail))
        if detail.get('baseline_available'):
            self._baseline_label.setText(self.tr(
                'Change labels use the stored last-sync snapshot. “Changed on both sides” means '
                'both current versions differ from that snapshot.'
            ))
        else:
            self._baseline_label.setText(self.tr(
                'Previous synchronized state is unavailable; current versions differ.'
            ))
        self._populate_fields(detail.get('field_rows') or [])
        self._populate_photos(detail.get('image_pairs') or [])
        derived = detail.get('derived_statistics') or {}
        if derived.get('status') == 'recompute_from_measurements':
            self._show_status(
                self.tr('Spore statistics will be recomputed automatically from the selected scientific measurements.'),
                'info',
            )
        elif derived.get('status') == 'diagnostic_without_measurements':
            self._show_status(
                self.tr('Spore statistics differ, but reconstructable measurements are unavailable. Review later is required.'),
                'error',
            )
        self._configure_actions(detail)
        self._refresh_btn.setEnabled(True)
        if not derived:
            self._show_status('', 'info')

    def _overview(self, detail: dict) -> str:
        sentences = []
        fields = len(detail.get('field_rows') or [])
        if not fields:
            sentences.append(self.tr('No ordinary observation fields differ.'))
        else:
            sentences.append(self.tr('{count} observation field(s) differ.').format(count=fields))
        pairs = detail.get('image_pairs') or []
        local_only = [pair for pair in pairs if pair.get('local') and not pair.get('remote')]
        cloud_only = [pair for pair in pairs if pair.get('remote') and not pair.get('local')]
        if local_only:
            sentences.append(self.tr('{count} photo(s) exist only on this device.').format(count=len(local_only)))
        if cloud_only:
            sentences.append(self.tr('{count} photo(s) exist only on Sporely Cloud.').format(count=len(cloud_only)))
        for pair in pairs:
            count = len(pair.get('measurement_conflicts') or [])
            if count:
                image = pair.get('local') or pair.get('remote') or {}
                position = image.get('sort_order') if image.get('sort_order') is not None else self.tr('unknown')
                sentences.append(self.tr(
                    '{count} measurement(s) on microscope image {position} differ from the cloud copy.'
                ).format(count=count, position=position))
        possible = sum(1 for pair in pairs if pair.get('status') == 'possible_match')
        if possible:
            sentences.append(self.tr(
                'Possible counterparts are shown separately because their identity is not confirmed.'
            ))
        return ' '.join(sentences)

    def _clear_layout(self, layout: QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def _choice_widget(
        self,
        key: str,
        spec: dict,
        options: list[tuple[str, str]],
        parent: QWidget,
    ) -> QWidget:
        widget = QWidget(parent)
        row = QHBoxLayout(widget)
        row.setContentsMargins(0, 0, 0, 0)
        group = QButtonGroup(widget)
        group.setExclusive(True)
        for value, label in options:
            button = QRadioButton(label, widget)
            button.setProperty('choice_value', value)
            group.addButton(button)
            row.addWidget(button)
        row.addStretch(1)
        group.buttonToggled.connect(lambda _button, checked: self._update_apply_enabled() if checked else None)
        self._choice_groups[key] = group
        self._choice_specs[key] = dict(spec)
        return widget

    def _selected_choice(self, key: str) -> str | None:
        group = self._choice_groups.get(key)
        button = group.checkedButton() if group else None
        return str(button.property('choice_value')) if button else None

    def _set_choice(self, key: str, value: str) -> None:
        group = self._choice_groups.get(key)
        if group is None:
            return
        for button in group.buttons():
            if str(button.property('choice_value')) == value:
                button.setChecked(True)
                return

    def _populate_fields(self, rows: list[dict]) -> None:
        self._clear_layout(self._field_choices_layout)
        self._compare_table.setRowCount(len(rows))
        self._compare_table.setVisible(bool(rows))
        self._field_choices_container.setVisible(bool(rows))
        if not rows:
            self._field_status.setText(self.tr('No observation fields differ'))
            return
        self._field_status.setText(self.tr('Observation field differences'))
        for row_index, row in enumerate(rows):
            values = [row.get('label'), row.get('baseline'), row.get('local'), row.get('remote')]
            for column, value in enumerate(values):
                item = QTableWidgetItem(_format_compare_value(str(row.get('field') or ''), value))
                if column == 2 and row.get('local_changed'):
                    background, foreground = _changed_cell_colors(self._compare_table, 'local')
                    item.setBackground(background)
                    item.setForeground(foreground)
                if column == 3 and row.get('remote_changed'):
                    background, foreground = _changed_cell_colors(self._compare_table, 'cloud')
                    item.setBackground(background)
                    item.setForeground(foreground)
                self._compare_table.setItem(row_index, column, item)
            field = str(row.get('field') or '')
            chooser = QGroupBox(str(row.get('label') or field), self._field_choices_container)
            chooser_layout = QVBoxLayout(chooser)
            chooser_layout.addWidget(QLabel(self.tr('Needs your choice'), chooser))
            chooser_layout.addWidget(self._choice_widget(
                f'field:{field}',
                {'kind': 'field', 'field': field, 'required': True},
                [
                    ('local', self.tr('This device — {value}').format(
                        value=_format_compare_value(field, row.get('local'))
                    )),
                    ('cloud', self.tr('Sporely Cloud — {value}').format(
                        value=_format_compare_value(field, row.get('remote'))
                    )),
                ],
                chooser,
            ))
            self._field_choices_layout.addWidget(chooser)
        self._compare_table.resizeRowsToContents()
        content_height = self._compare_table.horizontalHeader().height() + sum(
            self._compare_table.rowHeight(row)
            for row in range(self._compare_table.rowCount())
        ) + self._compare_table.frameWidth() * 2 + 6
        self._compare_table.setFixedHeight(max(58, content_height))

    def _clear_photo_rows(self) -> None:
        self._thumbnail_cards.clear()
        while self._photos_layout.count():
            item = self._photos_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def _matching_visibility_changed(self, _checked: bool) -> None:
        if self._current_detail is not None:
            self._populate_photos(self._current_detail.get('image_pairs') or [])

    def _populate_photos(self, pairs: list[dict]) -> None:
        self._clear_photo_rows()
        show_matching = self._show_matching_check.isChecked()
        visible_pairs = [
            pair for pair in pairs
            if show_matching or str(pair.get('status') or '') != 'same'
        ]
        has_any_pairs = bool(pairs)
        has_visible_pairs = bool(visible_pairs)
        self._show_matching_check.setVisible(has_any_pairs)
        self._photos_heading.setVisible(has_visible_pairs)
        self._photo_headings.setVisible(has_visible_pairs)
        self._photos_container.setVisible(has_visible_pairs)
        if not visible_pairs:
            return
        generation = self._selection_generation
        for pair in visible_pairs:
            group = QGroupBox(self._photos_container)
            layout = QVBoxLayout(group)
            cards = QWidget(group)
            card_layout = QHBoxLayout(cards)
            card_layout.setContentsMargins(0, 0, 0, 0)
            status = str(pair.get('status') or 'unavailable')
            local_card = PhotoCard('local', pair.get('local'), status, cards)
            remote_card = PhotoCard('cloud', pair.get('remote'), status, cards)
            card_layout.addWidget(local_card, 1)
            card_layout.addWidget(remote_card, 1)
            layout.addWidget(cards)
            if pair.get('possible_counterpart'):
                warning = QLabel(self.tr('Possible counterpart — identity is not confirmed'), group)
                warning.setWordWrap(True)
                layout.addWidget(warning)
            if pair.get('identity_conflict_reasons'):
                warning = QLabel(
                    self.tr('Identity conflict — automatic resolution unavailable') + '\n' +
                    '\n'.join(f"• {reason}" for reason in pair.get('identity_conflict_reasons') or []),
                    group,
                )
                warning.setWordWrap(True)
                warning.setStyleSheet('font-weight: 600;')
                layout.addWidget(warning)
            if pair.get('metadata_diff_details'):
                layout.addWidget(self._metadata_widget(pair.get('metadata_diff_details') or [], group))
                identity = str((pair.get('local') or {}).get('local_id') or (pair.get('remote') or {}).get('cloud_id') or '')
                layout.addWidget(self._choice_widget(
                    f'image_metadata:{identity}',
                    {'kind': 'image_metadata', 'identity': identity,
                     'local_id': (pair.get('local') or {}).get('local_id'),
                     'cloud_id': (pair.get('remote') or {}).get('cloud_id'),
                     'required': True},
                    [('local', self.tr('Use this device metadata')),
                     ('cloud', self.tr('Use Sporely Cloud metadata'))],
                    group,
                ))
            if pair.get('presentation_differences'):
                order = pair.get('presentation_differences')[0]
                info = QLabel(self.tr(
                    'Informational only — image order differs. Desktop order {local} will be used automatically; cloud order is {remote}.'
                ).format(local=order.get('local'), remote=order.get('remote')), group)
                info.setWordWrap(True)
                layout.addWidget(info)
            local_image = pair.get('local')
            remote_image = pair.get('remote')
            if local_image and not remote_image:
                identity = str(local_image.get('local_id') or '')
                layout.addWidget(self._choice_widget(
                    f'image:{identity}',
                    {'kind': 'image', 'side': 'local_only', 'identity': identity,
                     'local_id': local_image.get('local_id'), 'cloud_id': local_image.get('cloud_id'),
                     'required': True},
                    [('upload', self.tr('Upload to Sporely Cloud')),
                     ('keep_local', self.tr('Keep only on this device'))],
                    group,
                ))
            elif remote_image and not local_image:
                identity = str(remote_image.get('cloud_id') or '')
                layout.addWidget(self._choice_widget(
                    f'image:{identity}',
                    {'kind': 'image', 'side': 'cloud_only', 'identity': identity,
                     'local_id': remote_image.get('local_id'), 'cloud_id': remote_image.get('cloud_id'),
                     'required': True},
                    [('download', self.tr('Download to this device')),
                     ('keep_cloud', self.tr('Keep only in Sporely Cloud'))],
                    group,
                ))
            measurement_pairs = (
                pair.get('measurement_pairs') or []
                if show_matching
                else pair.get('measurement_conflicts') or []
            )
            if measurement_pairs:
                layout.addWidget(self._measurement_widget(measurement_pairs, group))
            self._photos_layout.addWidget(group)
            self._queue_thumbnail(generation, local_card)
            self._queue_thumbnail(generation, remote_card)

    def _metadata_widget(self, details: list[dict], parent: QWidget) -> QWidget:
        box = QGroupBox(self.tr('Photo metadata differences'), parent)
        layout = QVBoxLayout(box)
        labels = {
            'sort_order': self.tr('Image order'),
            'image_type': self.tr('Image type'),
            'micro_category': self.tr('Microscope category'),
            'objective_name': self.tr('Microscope objective'),
            'scale_microns_per_pixel': self.tr('Scale'),
            'mount_medium': self.tr('Mount medium'),
            'stain': self.tr('Stain'),
            'sample_type': self.tr('Sample type'),
            'sample_source': self.tr('Sample source'),
            'contrast': self.tr('Contrast method'),
            'notes': self.tr('Notes'),
            'gps_source': self.tr('GPS source'),
            'crop_mode': self.tr('Crop mode'),
        }
        origins = {
            'local': self.tr('Changed only on this device'),
            'cloud': self.tr('Changed only on Sporely Cloud'),
            'both': self.tr('Changed on both sides'),
            'baseline_unavailable': self.tr('Previous baseline unavailable'),
        }
        for detail in details:
            field = str(detail.get('field') or '')
            line = QLabel(
                self.tr('• {field}: this device {local} · cloud {remote}\n  {origin}').format(
                    field=labels.get(field, field.replace('_', ' ').title()),
                    local=_format_compare_value(field, detail.get('local')),
                    remote=_format_compare_value(field, detail.get('remote')),
                    origin=origins.get(detail.get('change_origin'), self.tr('Current versions differ')),
                ),
                box,
            )
            line.setWordWrap(True)
            layout.addWidget(line)
        return box

    def _measurement_widget(self, comparisons: list[dict], parent: QWidget) -> QWidget:
        box = QGroupBox(self.tr('Measurement comparison'), parent)
        layout = QVBoxLayout(box)
        geometry_fields = {f'p{point}_{axis}' for point in range(1, 5) for axis in ('x', 'y')}
        for comparison in comparisons:
            status = str(comparison.get('status') or 'values_differ')
            local_id = comparison.get('local_id')
            cloud_id = comparison.get('cloud_id')
            identity = QLabel(self.tr(
                'Measurement · local {local_id} · cloud {cloud_id}'
            ).format(
                local_id=f'#{local_id}' if local_id else self.tr('missing'),
                cloud_id=cloud_id or self.tr('missing'),
            ), box)
            identity.setStyleSheet('font-weight: 600;')
            layout.addWidget(identity)
            status_label = QLabel(_status_display(status, box), box)
            status_label.setStyleSheet('font-weight: 600;')
            layout.addWidget(status_label)
            if comparison.get('identity_conflict_reasons'):
                reasons = QLabel(
                    '\n'.join(f"• {reason}" for reason in comparison.get('identity_conflict_reasons') or []),
                    box,
                )
                reasons.setWordWrap(True)
                layout.addWidget(reasons)
            origin = {
                'local': self.tr('Changed only on this device'),
                'cloud': self.tr('Changed only on Sporely Cloud'),
                'both': self.tr('Changed on both sides'),
                'added_local': self.tr('Added only on this device'),
                'added_cloud': self.tr('Added only on Sporely Cloud'),
                'baseline_unavailable': self.tr('Previous synchronized state is unavailable; current versions differ.'),
            }.get(comparison.get('change_origin'), self.tr('Current versions differ'))
            layout.addWidget(QLabel(origin, box))
            measurement_identity = str(local_id or cloud_id or '')
            if status == 'values_differ':
                layout.addWidget(self._choice_widget(
                    f'measurement:{measurement_identity}',
                    {'kind': 'measurement', 'side': 'matched', 'identity': measurement_identity,
                     'local_id': local_id, 'cloud_id': cloud_id, 'required': True},
                    [('local', self.tr('Use this device')),
                     ('cloud', self.tr('Use Sporely Cloud'))],
                    box,
                ))
            elif status == 'local_only':
                layout.addWidget(self._choice_widget(
                    f'measurement:{measurement_identity}',
                    {'kind': 'measurement', 'side': 'local_only', 'identity': measurement_identity,
                     'local_id': local_id, 'cloud_id': cloud_id, 'required': True},
                    [('upload', self.tr('Upload to Sporely Cloud')),
                     ('keep_local', self.tr('Keep only on this device'))],
                    box,
                ))
            elif status == 'cloud_only':
                layout.addWidget(self._choice_widget(
                    f'measurement:{measurement_identity}',
                    {'kind': 'measurement', 'side': 'cloud_only', 'identity': measurement_identity,
                     'local_id': local_id, 'cloud_id': cloud_id, 'required': True},
                    [('download', self.tr('Download to this device')),
                     ('keep_cloud', self.tr('Keep only in Sporely Cloud'))],
                    box,
                ))
            for presentation in comparison.get('presentation_differences') or []:
                layout.addWidget(QLabel(self.tr(
                    'Informational only — gallery rotation differs. This device value {local}° will be used automatically; cloud value is {remote}°.'
                ).format(local=presentation.get('local'), remote=presentation.get('remote')), box))

            fields = list(comparison.get('fields') or [])
            if status in {'local_only', 'cloud_only', 'same', 'identity_conflict'}:
                fields = ['desktop_id', 'length_um', 'width_um', 'measurement_type']
            display_fields = [field for field in fields if field not in geometry_fields]
            if (
                any(field in geometry_fields for field in fields)
                or status in {'local_only', 'cloud_only', 'same', 'identity_conflict'}
            ):
                display_fields.append('__geometry__')
            table = QTableWidget(len(display_fields), 4, box)
            table.setHorizontalHeaderLabels([
                self.tr('Property'), self.tr('Last synced'), self.tr('On this device'),
                self.tr('On Sporely Cloud')
            ])
            table.verticalHeader().setVisible(False)
            table.setEditTriggers(QTableWidget.NoEditTriggers)
            table.setSelectionMode(QTableWidget.NoSelection)
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            local_values = comparison.get('local_values') or {}
            cloud_values = comparison.get('remote_values') or {}
            baseline_values = comparison.get('baseline_values') or {}
            for row_index, field in enumerate(display_fields):
                raw = ''
                if field == '__geometry__':
                    values = [
                        self.tr('Measurement geometry'),
                        comparison.get('geometry_baseline'),
                        comparison.get('geometry_local'),
                        comparison.get('geometry_cloud'),
                    ]
                    raw_fields = sorted(geometry_fields)
                    raw = (
                        self.tr('Baseline:') + '\n' +
                        '\n'.join(f'{name}: {baseline_values.get(name)!r}' for name in raw_fields) +
                        '\n\n' + self.tr('On this device:') + '\n' +
                        '\n'.join(f'{name}: {local_values.get(name)!r}' for name in raw_fields) +
                        '\n\n' + self.tr('On Sporely Cloud:') + '\n' +
                        '\n'.join(f'{name}: {cloud_values.get(name)!r}' for name in raw_fields)
                    )
                else:
                    labels = {
                        'length_um': self.tr('Length'), 'width_um': self.tr('Width'),
                        'gallery_rotation': self.tr('Gallery rotation'),
                        'measurement_type': self.tr('Measurement type'),
                        'measured_at': self.tr('Measured at'),
                        'image_id': self.tr('Owning image'),
                        'desktop_id': self.tr('Measurement identity'),
                    }
                    values = [
                        labels.get(field, field.replace('_', ' ').title()),
                        baseline_values.get(field),
                        local_values.get(field),
                        cloud_values.get(field),
                    ]
                for column, value in enumerate(values):
                    item = QTableWidgetItem(_format_measurement_value(field, value))
                    if raw:
                        item.setToolTip(raw)
                    table.setItem(row_index, column, item)
            table.resizeRowsToContents()
            content_height = table.horizontalHeader().height() + sum(
                table.rowHeight(row) for row in range(table.rowCount())
            ) + table.frameWidth() * 2 + 6
            table.setFixedHeight(max(58, content_height))
            layout.addWidget(table)
        return box

    def _queue_thumbnail(self, generation: int, card: PhotoCard) -> None:
        if not card.image:
            return
        side = card.side
        source = str(card.image.get('thumbnail_source') or '')
        identity = card.image.get('local_id') if side == 'local' else card.image.get('cloud_id')
        key = f'{side}:{identity}'
        self._thumbnail_cards.setdefault(key, []).append(card)
        cached = self._thumbnail_cache.get(key)
        if isinstance(cached, QByteArray):
            card.show_image(cached)
            return
        if isinstance(cached, str):
            card.show_error(cached)
            return
        card.show_loading()
        worker = ConflictThumbnailWorker(generation, key, side, source)
        _retain_worker(worker)
        self._thumbnail_workers.add(worker)
        worker.loaded.connect(self._thumbnail_loaded)
        worker.failed.connect(self._thumbnail_failed)
        worker.finished.connect(lambda worker=worker: self._thumbnail_workers.discard(worker))
        worker.start()

    def _thumbnail_loaded(self, generation: int, key: str, data: QByteArray) -> None:
        self._thumbnail_cache[key] = data
        if self._closing or generation != self._selection_generation:
            return
        for card in self._thumbnail_cards.get(key, []):
            card.show_image(data)

    def _thumbnail_failed(self, generation: int, key: str, message: str) -> None:
        self._thumbnail_cache[key] = message
        if self._closing or generation != self._selection_generation:
            return
        for card in self._thumbnail_cards.get(key, []):
            card.show_error(message)

    def _configure_actions(self, detail: dict) -> None:
        identity_conflict = (
            bool(detail.get('identity_conflicts'))
            or any(
                comparison.get('status') == 'identity_conflict'
                for comparison in detail.get('measurement_pairs') or []
            )
        )
        unreconstructable_statistics = (
            (detail.get('derived_statistics') or {}).get('status')
            == 'diagnostic_without_measurements'
        )
        presets_enabled = not identity_conflict
        self._keep_local_btn.setEnabled(presets_enabled)
        self._keep_remote_btn.setEnabled(presets_enabled)
        self._merge_btn.setEnabled(presets_enabled)
        reason = ''
        if identity_conflict:
            reason = self.tr(
                'Cannot resolve automatically because image or measurement identities contradict each other. Review later and repair the links first.'
            )
        elif unreconstructable_statistics:
            reason = self.tr(
                'Statistics differ without reconstructable measurements. Review the source data before applying.'
            )
        for button in (self._keep_local_btn, self._keep_remote_btn, self._merge_btn):
            button.setToolTip(reason)
        self._apply_all_check.setChecked(False)
        self._apply_all_check.setEnabled(False)
        self._apply_all_check.setToolTip(self.tr(
            'Per-item conflict plans cannot be applied blindly to other observations.'
        ))
        self._update_apply_enabled()

    def _set_resolution_enabled(self, enabled: bool) -> None:
        self._keep_local_btn.setEnabled(enabled)
        self._keep_remote_btn.setEnabled(enabled)
        self._merge_btn.setEnabled(enabled)
        self._apply_btn.setEnabled(False)
        if not enabled:
            self._apply_all_check.setEnabled(False)

    def _update_apply_enabled(self) -> None:
        if self._current_detail is None:
            self._apply_btn.setEnabled(False)
            return
        blocked = (
            bool(self._current_detail.get('identity_conflicts'))
            or any(
                comparison.get('status') == 'identity_conflict'
                for comparison in self._current_detail.get('measurement_pairs') or []
            )
            or (self._current_detail.get('derived_statistics') or {}).get('status')
            == 'diagnostic_without_measurements'
        )
        complete = all(
            not spec.get('required') or self._selected_choice(key) is not None
            for key, spec in self._choice_specs.items()
        )
        self._apply_btn.setEnabled(bool(self._choice_specs) and complete and not blocked)

    def _apply_preset(self, preset: str) -> None:
        for key, spec in self._choice_specs.items():
            kind = spec.get('kind')
            side = spec.get('side')
            choice = None
            if preset == 'local':
                if kind in {'field', 'image_metadata'} or side == 'matched':
                    choice = 'local'
                elif side == 'local_only':
                    choice = 'upload'
                elif side == 'cloud_only':
                    choice = 'keep_cloud'
            elif preset == 'cloud':
                if kind in {'field', 'image_metadata'} or side == 'matched':
                    choice = 'cloud'
                elif side == 'local_only':
                    choice = 'keep_local'
                elif side == 'cloud_only':
                    choice = 'download'
            elif preset == 'safe':
                if side == 'local_only':
                    choice = 'upload'
                elif side == 'cloud_only':
                    choice = 'download'
            if choice is not None:
                self._set_choice(key, choice)
            elif preset == 'safe':
                group = self._choice_groups.get(key)
                if group is not None:
                    group.setExclusive(False)
                    for button in group.buttons():
                        button.setChecked(False)
                    group.setExclusive(True)
        self._update_apply_enabled()

    def _build_selected_plan(self) -> dict:
        items = []
        for key, spec in self._choice_specs.items():
            choice = self._selected_choice(key)
            if choice is None:
                continue
            items.append({**spec, 'key': key, 'choice': choice})
        split_measurement_sets = any(
            item.get('kind') == 'measurement'
            and item.get('choice') in {'keep_local', 'keep_cloud'}
            for item in items
        )
        return {
            'items': items,
            'derived_statistics': (
                'recompute_from_measurements'
                if (self._current_detail or {}).get('derived_statistics')
                and not split_measurement_sets
                else 'unchanged'
            ),
            'presentation_policy': {
                'gallery_rotation': 'local_desktop',
                'image_order': 'local_desktop',
            },
            'allow_media_deletion': False,
        }

    def _plan_summary(self, plan: dict) -> str:
        items = list(plan.get('items') or [])
        counts = {
            'cloud_fields': sum(1 for item in items if item.get('kind') == 'field' and item.get('choice') == 'cloud'),
            'local_fields': sum(1 for item in items if item.get('kind') == 'field' and item.get('choice') == 'local'),
            'local_measurements': sum(1 for item in items if item.get('kind') == 'measurement' and item.get('choice') in {'local', 'upload'}),
            'cloud_measurements': sum(1 for item in items if item.get('kind') == 'measurement' and item.get('choice') in {'cloud', 'download'}),
            'upload_images': sum(1 for item in items if item.get('kind') == 'image' and item.get('choice') == 'upload'),
            'download_images': sum(1 for item in items if item.get('kind') == 'image' and item.get('choice') == 'download'),
        }
        lines = [
            self.tr('Use cloud values for {count} observation field(s)').format(count=counts['cloud_fields']),
            self.tr('Use device values for {count} observation field(s)').format(count=counts['local_fields']),
            self.tr('Use or upload {count} device measurement(s)').format(count=counts['local_measurements']),
            self.tr('Use or download {count} cloud measurement(s)').format(count=counts['cloud_measurements']),
            self.tr('Upload {count} local-only image(s)').format(count=counts['upload_images']),
            self.tr('Download {count} cloud-only image(s)').format(count=counts['download_images']),
        ]
        if plan.get('derived_statistics') == 'recompute_from_measurements':
            lines.append(self.tr('Recompute spore statistics'))
        lines.append(self.tr('No media will be deleted'))
        return '\n'.join(f'• {line}' for line in lines)

    def _apply_selected_changes(self) -> None:
        conflict = self._current_conflict()
        if conflict is None or not self._apply_btn.isEnabled():
            return
        plan = self._build_selected_plan()
        reply = QMessageBox.question(
            self,
            self.tr('Apply selected conflict changes?'),
            self._plan_summary(plan),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        self.decisions.append({
            'local_id': int(conflict.get('local_id') or 0),
            'cloud_id': str(conflict.get('cloud_id') or '').strip(),
            'action': 'plan',
            'plan': plan,
        })
        self.resolved_any = True
        self._remove_current_conflict()

    def _remove_current_conflict(self) -> None:
        row = self._list.currentRow()
        if row < 0:
            return
        del self._conflicts[row]
        self._reload_list()
        if self._conflicts:
            self._list.setCurrentRow(min(row, len(self._conflicts) - 1))
        else:
            self.accept()

    def _refresh_current_detail(self) -> None:
        row = self._list.currentRow()
        if row < 0:
            return
        self._detail_cache.pop(self._key(self._conflicts[row]), None)
        self._selection_generation += 1
        self._show_loading()
        self._start_detail_load(row, force=True)

    def _show_status(self, message: str, tone: str) -> None:
        if not message:
            self._status_label.clear()
            self._status_label.hide()
            return
        colors = {'info': '#7aa2ff', 'success': '#4fa66b', 'error': '#c05848'}
        self._status_label.setStyleSheet(f"color: {colors.get(tone, colors['info'])};")
        self._status_label.setText(message)
        self._status_label.show()

    def reject(self) -> None:
        # Decisions are merely accumulated until the dialog is accepted by the
        # caller.  Every rejection path discards them atomically.
        self.decisions.clear()
        self.resolved_any = False
        self._begin_close()
        super().reject()

    def closeEvent(self, event) -> None:
        self.decisions.clear()
        self.resolved_any = False
        self._begin_close()
        super().closeEvent(event)

    def _begin_close(self) -> None:
        self._closing = True
        self._selection_generation += 1
        for worker in tuple(self._detail_workers | self._thumbnail_workers):
            worker.requestInterruption()
        self._detail_workers.clear()
        self._thumbnail_workers.clear()


def _format_measurement_value(field: str, value) -> str:
    if value is None or value == '':
        return '—'
    if field in {'length_um', 'width_um'}:
        try:
            return f'{float(value):.2f} µm'
        except (TypeError, ValueError):
            pass
    if field == 'gallery_rotation':
        try:
            return f'{float(value):g}°'
        except (TypeError, ValueError):
            pass
    return str(value)
