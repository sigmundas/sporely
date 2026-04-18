"""Review and resolve Sporely Cloud sync conflicts."""
from __future__ import annotations

import json
from datetime import datetime
import re

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from utils.cloud_sync import (
    CloudSyncError,
    SporelyCloudClient,
    get_conflict_detail,
    resolve_conflict_keep_cloud,
    resolve_conflict_keep_local,
)
from PySide6.QtCore import QThread, Signal


class ConflictResolutionWorker(QThread):
    progress = Signal(str, int, int)
    finished = Signal(bool)
    error = Signal(str)

    def __init__(self, decisions, prepare_images_cb=None):
        super().__init__()
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
                local_id = dec['local_id']
                cloud_id = dec['cloud_id']
                action = dec['action']
                self.progress.emit(f"Resolving conflict {i+1} of {total}...", i, total)
                
                if action == 'keep_local':
                    resolve_conflict_keep_local(client, local_id, prepare_images_cb=self.prepare_images_cb)
                elif action == 'keep_cloud':
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
                        resolve_conflict_keep_cloud(client, local_id, cloud_id=cloud_id or None)
                    except Exception as exc:
                        if not _retryable_cloud_error(exc):
                            raise
                        client_retry = SporelyCloudClient.from_stored_credentials()
                        if client_retry is None:
                            raise CloudSyncError('Not logged in to Sporely Cloud')
                        resolve_conflict_keep_cloud(client_retry, local_id, cloud_id=cloud_id or None)
                resolved_any = True
            
            self.progress.emit("Conflict resolution finished.", total, total)
            self.finished.emit(resolved_any)
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
    if field in {'location_public'}:
        return 'Yes' if bool(value) else 'No'
    if field in {'visibility', 'sharing_scope'}:
        return str(value).capitalize()
    if field in {'gps_latitude', 'gps_longitude'}:
        try:
            return f'{float(value):.6f}'
        except Exception:
            return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _summary_text(lines: list[str], empty_text: str) -> str:
    cleaned = [str(line or '').strip() for line in (lines or []) if str(line or '').strip()]
    if not cleaned:
        return empty_text
    return '\n'.join(f'- {line}' for line in cleaned)


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


def _conflict_list_label(detail: dict) -> str:
    identity = _conflict_identity(detail)
    local_id = int(detail.get('local_id') or 0)
    cloud_id = str(detail.get('cloud_id') or '').strip() or '?'
    second_line = f"{identity['date']}"
    if identity['time'] != '—':
        second_line += f" {identity['time']}"
    if identity['location'] != '—':
        second_line += f" · {identity['location']}"
    second_line += f"  (desktop #{local_id} ↔ cloud {cloud_id})"
    return f"{identity['species']}\n{second_line}"


class CloudConflictDialog(QDialog):
    def __init__(
        self,
        parent=None,
        *,
        conflicts: list[dict] | None = None,
        prepare_images_cb=None,
    ):
        super().__init__(parent)
        self.setWindowTitle('Sporely Cloud review')
        self.setModal(True)
        self.resize(1080, 720)

        self._prepare_images_cb = prepare_images_cb
        self._client = SporelyCloudClient.from_stored_credentials()
        self._conflicts = [dict(row or {}) for row in (conflicts or [])]
        self._detail_cache: dict[str, dict] = {}
        self.resolved_any = False
        self.decisions = []

        root = QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        intro = QLabel(
            'Most cloud changes were synced automatically. '
            'These observations still need review because both sides changed the same data, '
            'or a cloud change would remove local media. '
            'The table compares observation fields such as species, date, notes, and GPS. '
            'The panels below summarize image and other media-related changes. '
            'Choose which version to keep.'
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        self._status_label = QLabel('')
        self._status_label.setWordWrap(True)
        self._status_label.hide()
        root.addWidget(self._status_label)

        splitter = QSplitter(Qt.Horizontal, self)
        root.addWidget(splitter, 1)

        left = QWidget(self)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)
        left_layout.addWidget(QLabel('Conflicted observations'))
        self._list = QListWidget(left)
        self._list.currentRowChanged.connect(self._on_selection_changed)
        left_layout.addWidget(self._list, 1)
        splitter.addWidget(left)

        right = QWidget(self)
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(10)

        self._title_label = QLabel('Select a conflicted observation')
        self._title_label.setStyleSheet('font-size: 16px; font-weight: 700;')
        right_layout.addWidget(self._title_label)

        self._identity_label = QLabel('Species: —    Date: —    Time: —    Location: —')
        self._identity_label.setWordWrap(True)
        right_layout.addWidget(self._identity_label)

        meta_row = QHBoxLayout()
        meta_row.setSpacing(18)
        self._synced_label = QLabel('Last synced: —')
        self._local_time_label = QLabel('Desktop changed: —')
        self._remote_time_label = QLabel('Cloud changed: —')
        meta_row.addWidget(self._synced_label)
        meta_row.addWidget(self._local_time_label)
        meta_row.addWidget(self._remote_time_label)
        meta_row.addStretch(1)
        right_layout.addLayout(meta_row)

        self._compare_table = QTableWidget(0, 4, self)
        self._compare_table.setHorizontalHeaderLabels(['Field', 'Last synced', 'Desktop now', 'Cloud now'])
        self._compare_table.verticalHeader().setVisible(False)
        self._compare_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._compare_table.setSelectionMode(QTableWidget.NoSelection)
        self._compare_table.setAlternatingRowColors(True)
        header = self._compare_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        right_layout.addWidget(self._compare_table, 2)

        summary_splitter = QSplitter(Qt.Horizontal, self)
        self._desktop_box = self._make_summary_box('Desktop media changes')
        self._cloud_box = self._make_summary_box('Cloud media changes')
        summary_splitter.addWidget(self._desktop_box)
        summary_splitter.addWidget(self._cloud_box)
        right_layout.addWidget(summary_splitter, 1)

        splitter.addWidget(right)
        splitter.setSizes([280, 780])

        action_row = QHBoxLayout()
        self._refresh_btn = QPushButton('Refresh comparison')
        self._refresh_btn.clicked.connect(self._refresh_current_detail)
        action_row.addWidget(self._refresh_btn)

        action_row.addStretch(1)

        self._keep_local_btn = QPushButton('Keep desktop')
        self._keep_local_btn.clicked.connect(self._resolve_keep_local)
        action_row.addWidget(self._keep_local_btn)

        self._keep_remote_btn = QPushButton('Keep cloud')
        self._keep_remote_btn.clicked.connect(self._resolve_keep_cloud)
        action_row.addWidget(self._keep_remote_btn)

        self._cancel_btn = QPushButton('Cancel')
        self._cancel_btn.clicked.connect(self.reject)
        action_row.addWidget(self._cancel_btn)

        root.addLayout(action_row)

        self._reload_list()
        if self._conflicts:
            self._list.setCurrentRow(0)
        else:
            self._set_detail_enabled(False)
            self._show_status('No unresolved Sporely Cloud conflicts.', tone='success')

    def _make_summary_box(self, title: str) -> QGroupBox:
        box = QGroupBox(title, self)
        layout = QVBoxLayout(box)
        layout.setContentsMargins(10, 12, 10, 10)
        text = QTextEdit(box)
        text.setReadOnly(True)
        text.setFrameShape(QFrame.NoFrame)
        text.setMinimumHeight(180)
        layout.addWidget(text, 1)
        box._summary_text = text  # type: ignore[attr-defined]
        return box

    def _summary_widget(self, box: QGroupBox) -> QTextEdit:
        return getattr(box, '_summary_text')

    def _reload_list(self) -> None:
        self._list.clear()
        for conflict in self._conflicts:
            label = None
            detail = dict(conflict.get('detail') or {})
            if detail:
                try:
                    label = _conflict_list_label(detail)
                except Exception:
                    label = None
            if not label:
                local_id = int(conflict.get('local_id') or 0)
                cloud_id = str(conflict.get('cloud_id') or '').strip() or '?'
                label = f'Observation #{local_id}\n desktop #{local_id} ↔ cloud {cloud_id}'
            item = QListWidgetItem(label)
            item.setToolTip(label.replace('\n', '\n'))
            item.setData(Qt.UserRole, dict(conflict))
            self._list.addItem(item)

    def _set_detail_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        self._compare_table.setEnabled(enabled)
        self._refresh_btn.setEnabled(enabled)
        self._keep_local_btn.setEnabled(enabled)
        self._keep_remote_btn.setEnabled(enabled)

    def _show_status(self, message: str, *, tone: str = 'info') -> None:
        text = str(message or '').strip()
        if not text:
            self._status_label.hide()
            self._status_label.clear()
            return
        color = '#7aa2ff'
        if tone == 'success':
            color = '#4fa66b'
        elif tone == 'warning':
            color = '#c98c2a'
        elif tone == 'error':
            color = '#c05848'
        self._status_label.setStyleSheet(f'color: {color};')
        self._status_label.setText(text)
        self._status_label.show()

    def _current_conflict(self) -> dict | None:
        item = self._list.currentItem()
        if item is None:
            return None
        data = item.data(Qt.UserRole)
        return dict(data or {}) if isinstance(data, dict) else None

    def _detail_cache_key(self, conflict: dict) -> str:
        return f"{int(conflict.get('local_id') or 0)}::{str(conflict.get('cloud_id') or '').strip()}"

    def _load_detail(self, conflict: dict, *, force: bool = False) -> dict:
        key = self._detail_cache_key(conflict)
        if not force and key in self._detail_cache:
            return dict(self._detail_cache[key])
        client = self._fresh_client(force=force)
        if client is None:
            raise CloudSyncError('Not logged in to Sporely Cloud')
        detail = get_conflict_detail(
            client,
            int(conflict.get('local_id') or 0),
            str(conflict.get('cloud_id') or '').strip() or None,
        )
        self._detail_cache[key] = dict(detail)
        conflict['detail'] = dict(detail)
        return detail

    def _fresh_client(self, *, force: bool = False) -> SporelyCloudClient | None:
        if not force and self._client is not None:
            return self._client
        self._client = SporelyCloudClient.from_stored_credentials()
        return self._client

    def _retryable_cloud_error(self, exc: Exception) -> bool:
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

    def _on_selection_changed(self, row: int) -> None:
        if row < 0:
            self._clear_detail()
            return
        conflict = self._current_conflict()
        if not conflict:
            self._clear_detail()
            return
        try:
            detail = self._load_detail(conflict)
        except Exception as exc:
            self._clear_detail()
            self._show_status(f'Could not load conflict details: {exc}', tone='error')
            return
        self._populate_detail(detail)
        self._show_status('')

    def _clear_detail(self) -> None:
        self._title_label.setText('Select a conflicted observation')
        self._identity_label.setText('Species: —    Date: —    Time: —    Location: —')
        self._synced_label.setText('Last synced: —')
        self._local_time_label.setText('Desktop changed: —')
        self._remote_time_label.setText('Cloud changed: —')
        self._compare_table.setRowCount(0)
        self._summary_widget(self._desktop_box).setPlainText('No desktop image or media changes detected.')
        self._summary_widget(self._cloud_box).setPlainText('No cloud image changes detected.')
        self._set_detail_enabled(False)

    def _populate_detail(self, detail: dict) -> None:
        self._set_detail_enabled(True)
        identity = _conflict_identity(detail)
        current_item = self._list.currentItem()
        if current_item is not None:
            current_label = _conflict_list_label(detail)
            current_item.setText(current_label)
            current_item.setToolTip(current_label)
        self._title_label.setText(
            f"{identity.get('species') or detail.get('title') or 'Observation'}  "
            f"(desktop #{detail.get('local_id')} ↔ cloud {detail.get('cloud_id')})"
        )
        self._identity_label.setText(
            f"Species: {identity.get('species') or '—'}    "
            f"Date: {identity.get('date') or '—'}    "
            f"Time: {identity.get('time') or '—'}    "
            f"Location: {identity.get('location') or '—'}"
        )
        self._synced_label.setText(f"Last synced: {_format_timestamp(detail.get('last_synced_at'))}")
        self._local_time_label.setText(f"Desktop changed: {_format_timestamp(detail.get('local_updated_at'))}")
        self._remote_time_label.setText(f"Cloud changed: {_format_timestamp(detail.get('remote_updated_at'))}")

        field_rows = list(detail.get('field_rows') or [])
        desktop_lines = list(detail.get('local_image_changes') or [])
        desktop_lines.append(f"Desktop images now: {int(detail.get('local_image_count') or 0)}")
        desktop_lines.append(f"Desktop measurements now: {int(detail.get('local_measurement_count') or 0)}")
        cloud_lines = list(detail.get('remote_image_changes') or [])
        cloud_lines.append(f"Cloud images now: {int(detail.get('remote_image_count') or 0)}")

        self._compare_table.setRowCount(len(field_rows))
        for row_index, row in enumerate(field_rows):
            values = [
                row.get('label'),
                _format_compare_value(str(row.get('field') or ''), row.get('baseline')),
                _format_compare_value(str(row.get('field') or ''), row.get('local')),
                _format_compare_value(str(row.get('field') or ''), row.get('remote')),
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled)
                if col == 2 and row.get('local_changed'):
                    item.setBackground(QColor(57, 83, 62))
                elif col == 3 and row.get('remote_changed'):
                    item.setBackground(QColor(83, 64, 57))
                self._compare_table.setItem(row_index, col, item)
        if not field_rows:
            self._compare_table.setRowCount(1)
            for col, value in enumerate([
                'Observation fields',
                '—',
                'No observation-field conflict shown here. Review media changes below.',
                'No observation-field conflict shown here. Review media changes below.',
            ]):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled)
                self._compare_table.setItem(0, col, item)
        self._compare_table.resizeRowsToContents()

        self._summary_widget(self._desktop_box).setPlainText(
            _summary_text(desktop_lines, 'No desktop image or media changes detected.')
        )
        self._summary_widget(self._cloud_box).setPlainText(
            _summary_text(cloud_lines, 'No cloud image changes detected.')
        )

    def _refresh_current_detail(self) -> None:
        conflict = self._current_conflict()
        if not conflict:
            return
        try:
            detail = self._load_detail(conflict, force=True)
        except Exception as exc:
            self._show_status(f'Could not refresh conflict details: {exc}', tone='error')
            return
        self._populate_detail(detail)
        self._show_status('Conflict comparison refreshed.', tone='success')

    def _set_busy(self, busy: bool, message: str | None = None) -> None:
        self._list.setEnabled(not busy)
        self._refresh_btn.setEnabled(not busy)
        self._keep_local_btn.setEnabled(not busy)
        self._keep_remote_btn.setEnabled(not busy)
        if busy:
            QApplication.setOverrideCursor(Qt.WaitCursor)
            if message:
                self._show_status(message, tone='info')
        else:
            QApplication.restoreOverrideCursor()

    def _remove_current_conflict(self) -> None:
        row = self._list.currentRow()
        if row < 0:
            return
        del self._conflicts[row]
        self._reload_list()
        if self._conflicts:
            self._list.setCurrentRow(min(row, len(self._conflicts) - 1))
        else:
            self._clear_detail()
            self.accept()

    def _resolve_keep_local(self) -> None:
        conflict = self._current_conflict()
        if not conflict:
            return
        local_id = int(conflict.get('local_id') or 0)
        cloud_id = str(conflict.get('cloud_id') or '').strip()
        self.decisions.append({
            'local_id': local_id,
            'cloud_id': cloud_id,
            'action': 'keep_local'
        })
        self._remove_current_conflict()

    def _resolve_keep_cloud(self) -> None:
        conflict = self._current_conflict()
        if not conflict:
            return
        local_id = int(conflict.get('local_id') or 0)
        cloud_id = str(conflict.get('cloud_id') or '').strip()
        self.decisions.append({
            'local_id': local_id,
            'cloud_id': cloud_id,
            'action': 'keep_cloud'
        })
        self._remove_current_conflict()
