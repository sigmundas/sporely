"""Cloud sync dialog — login and bidirectional sync with Sporely cloud."""
from __future__ import annotations

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QProgressBar, QFrame, QMessageBox, QCheckBox,
)

from database.models import ObservationDB
from database.schema import get_app_settings
from utils.cloud_sync import (
    SporelyCloudClient,
    CloudSyncError,
    sync_all,
    load_saved_cloud_password,
    summarize_sync_issues,
    unlink_local_observation_from_cloud,
)
from .cloud_conflict_dialog import CloudConflictDialog


class _SyncWorker(QThread):
    progress  = Signal(str, int, int)   # message, current, total
    finished  = Signal(dict)            # summary dict
    error     = Signal(str)

    def __init__(
        self,
        client: SporelyCloudClient,
        sync_images: bool,
        prepare_images_cb=None,
    ):
        super().__init__()
        self._client      = client
        self._sync_images = sync_images
        self._prepare_images_cb = prepare_images_cb

    def run(self) -> None:
        try:
            result = sync_all(
                self._client,
                progress_cb=lambda msg, cur, tot: self.progress.emit(msg, cur, tot),
                sync_images=self._sync_images,
                prepare_images_cb=self._prepare_images_cb,
            )
            self.finished.emit(result)
        except CloudSyncError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f'Unexpected error: {e}')


class CloudSyncDialog(QDialog):
    """Settings → Sporely Cloud Sync dialog.

    Shows a login form when not authenticated, or a sync status panel
    with a "Sync Now" button when signed in.
    """

    def __init__(self, parent=None, prepare_images_cb=None):
        super().__init__(parent)
        self.setWindowTitle('Sporely Cloud Sync')
        self.setMinimumWidth(420)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)

        self._client: SporelyCloudClient | None = None
        self._worker: _SyncWorker | None = None
        self._prepare_images_cb = prepare_images_cb

        root = QVBoxLayout(self)
        root.setSpacing(16)
        root.setContentsMargins(24, 24, 24, 24)

        # Title
        title = QLabel('Sporely Cloud Sync')
        title.setStyleSheet('font-size: 16px; font-weight: 700;')
        root.addWidget(title)

        # ── Login panel ─────────────────────────────────────────────────
        self._login_frame = QFrame()
        lf = QVBoxLayout(self._login_frame)
        lf.setSpacing(10)
        lf.setContentsMargins(0, 0, 0, 0)

        lf.addWidget(QLabel('Sign in with your Sporely account to enable cloud sync.'))

        self._email_input = QLineEdit()
        self._email_input.setPlaceholderText('Email')
        self._email_input.returnPressed.connect(self._do_login)
        lf.addWidget(self._email_input)

        self._pw_input = QLineEdit()
        self._pw_input.setPlaceholderText('Password')
        self._pw_input.setEchoMode(QLineEdit.Password)
        self._pw_input.returnPressed.connect(self._do_login)
        lf.addWidget(self._pw_input)

        saved_email, saved_password, can_store_password = load_saved_cloud_password()
        self._saved_cloud_password = saved_password
        self._saved_cloud_password_loaded = bool(saved_password)
        self._cloud_password_edited = False
        self._remember_cloud_password = False
        if saved_email:
            self._email_input.setText(saved_email)
        if self._saved_cloud_password_loaded:
            self._pw_input.setText('********')
        self._pw_input.textEdited.connect(self._on_password_edited)

        self._remember_pw_check = QCheckBox('Save password on this device')
        self._remember_pw_check.setChecked(bool(saved_email or saved_password))
        if not can_store_password:
            self._remember_pw_check.setChecked(False)
            self._remember_pw_check.setEnabled(False)
            self._remember_pw_check.setToolTip('Install keyring to enable encrypted password storage.')
        lf.addWidget(self._remember_pw_check)

        self._login_error = QLabel('')
        self._login_error.setWordWrap(True)
        self._login_error.setStyleSheet('color: #c05848;')
        self._login_error.hide()
        lf.addWidget(self._login_error)

        self._login_btn = QPushButton('Sign in')
        self._login_btn.setDefault(True)
        self._login_btn.clicked.connect(self._do_login)
        lf.addWidget(self._login_btn)

        root.addWidget(self._login_frame)

        # ── Sync panel ───────────────────────────────────────────────────
        self._sync_frame = QFrame()
        sf = QVBoxLayout(self._sync_frame)
        sf.setSpacing(12)
        sf.setContentsMargins(0, 0, 0, 0)

        self._account_label = QLabel('Signed in as: …')
        sf.addWidget(self._account_label)

        self._img_check = QCheckBox('Sync selected images (may be slow on first run)')
        self._img_check.setChecked(True)
        sf.addWidget(self._img_check)

        self._status_label = QLabel('Ready to sync.')
        self._status_label.setWordWrap(True)
        sf.addWidget(self._status_label)

        self._progress = QProgressBar()
        self._progress.hide()
        sf.addWidget(self._progress)

        btn_row = QHBoxLayout()
        self._sync_btn = QPushButton('Sync Now')
        self._sync_btn.setDefault(True)
        self._sync_btn.clicked.connect(self._do_sync)
        btn_row.addWidget(self._sync_btn)

        self._signout_btn = QPushButton('Sign out')
        self._signout_btn.clicked.connect(self._do_signout)
        btn_row.addWidget(self._signout_btn)
        sf.addLayout(btn_row)

        root.addWidget(self._sync_frame)

        # Close button
        self._close_btn = QPushButton('Close')
        self._close_btn.clicked.connect(self.accept)
        root.addWidget(self._close_btn)

        # Check for stored credentials and decide which panel to show
        self._client = SporelyCloudClient.from_stored_credentials()
        if self._client:
            self._show_sync_panel()
        else:
            self._show_login_panel()

    # ── Panel switching ──────────────────────────────────────────────────

    def _show_login_panel(self) -> None:
        self._login_frame.show()
        self._sync_frame.hide()
        self.adjustSize()

    def _show_sync_panel(self) -> None:
        settings = get_app_settings()
        email = settings.get('cloud_user_email', self._client.user_id[:8] + '…')
        self._account_label.setText(f'Signed in as: {email}')
        last = settings.get('cloud_last_pull_at')
        if last:
            self._status_label.setText(f'Last sync: {last[:19].replace("T", " ")} UTC')
        self._login_frame.hide()
        self._sync_frame.show()
        self.adjustSize()

    # ── Login ────────────────────────────────────────────────────────────

    def _do_login(self) -> None:
        email = self._email_input.text().strip()
        pw    = self._pw_input.text()
        if not email or not pw:
            self._show_login_error('Please enter your email and password.')
            return

        if self._saved_cloud_password_loaded and not self._cloud_password_edited:
            pw = self._saved_cloud_password or ''
        self._remember_cloud_password = bool(self._remember_pw_check.isChecked())

        self._login_btn.setEnabled(False)
        self._login_btn.setText('Signing in…')
        self._login_error.hide()

        # Run in thread so UI stays responsive
        class _LoginWorker(QThread):
            ok    = Signal(object)
            fail  = Signal(str)
            def __init__(self, email, pw):
                super().__init__()
                self._email, self._pw = email, pw
            def run(self):
                try:
                    client = SporelyCloudClient.login(self._email, self._pw)
                    self.ok.emit(client)
                except CloudSyncError as e:
                    self.fail.emit(str(e))

        self._login_worker = _LoginWorker(email, pw)
        self._login_worker.ok.connect(self._on_login_ok)
        self._login_worker.fail.connect(self._on_login_fail)
        self._login_worker.start()

    def _on_login_ok(self, client: SporelyCloudClient) -> None:
        self._client = client
        self._client.save_credentials(
            email=self._email_input.text().strip(),
            password=(self._saved_cloud_password if self._saved_cloud_password_loaded and not self._cloud_password_edited else self._pw_input.text()),
            remember_password=self._remember_cloud_password,
        )
        from database.schema import update_app_settings
        update_app_settings({'cloud_user_email': self._email_input.text().strip()})
        self._login_btn.setEnabled(True)
        self._login_btn.setText('Sign in')
        self._show_sync_panel()

    def _on_login_fail(self, msg: str) -> None:
        self._login_btn.setEnabled(True)
        self._login_btn.setText('Sign in')
        self._show_login_error(msg)

    def _show_login_error(self, msg: str) -> None:
        self._login_error.setText(msg)
        self._login_error.show()

    def _on_password_edited(self, _text: str) -> None:
        self._cloud_password_edited = True

    # ── Sync ─────────────────────────────────────────────────────────────

    def _do_sync(self) -> None:
        if not self._client:
            return
        self._sync_btn.setEnabled(False)
        self._signout_btn.setEnabled(False)
        self._status_label.setText('Syncing…')
        self._progress.setValue(0)
        self._progress.setRange(0, 0)  # indeterminate
        self._progress.show()

        self._worker = _SyncWorker(
            self._client,
            self._img_check.isChecked(),
            prepare_images_cb=self._prepare_images_cb,
        )
        self._worker.progress.connect(self._on_sync_progress)
        self._worker.finished.connect(self._on_sync_done)
        self._worker.error.connect(self._on_sync_error)
        self._worker.start()

    def _on_sync_progress(self, msg: str, cur: int, total: int) -> None:
        if total > 0:
            display_total = int(total)
            display_cur = int(cur)
            if display_cur >= display_total:
                display_cur = max(0, display_total - 1)
            self._progress.setRange(0, display_total)
            self._progress.setValue(display_cur)
        self._status_label.setText(msg)

    def _format_deleted_cloud_observation_label(self, entry: dict) -> str:
        observation = dict(entry.get('observation') or {})
        genus = str(observation.get('genus') or '').strip()
        species = str(observation.get('species') or '').strip()
        species_guess = str(observation.get('species_guess') or '').strip()
        species_text = f'{genus} {species}'.strip() or species_guess or 'Unknown species'
        date_text = str(entry.get('date') or observation.get('date') or '—').strip() or '—'
        location_text = str(entry.get('location') or observation.get('location') or '—').strip() or '—'
        return f'{species_text}\nDate: {date_text}\nLocation: {location_text}'

    def _prompt_for_deleted_cloud_observations(self, deleted_remote: list[dict]) -> bool:
        entries = [dict(row or {}) for row in (deleted_remote or []) if row]
        if not entries:
            return False
        changed = False
        for entry in entries:
            local_id = int(entry.get('local_id') or 0)
            cloud_id = str(entry.get('cloud_id') or '').strip() or '?'
            if local_id <= 0:
                continue
            box = QMessageBox(self)
            box.setIcon(QMessageBox.Warning)
            box.setWindowTitle('Cloud Observation Deleted')
            box.setText(
                f'Cloud observation {cloud_id} was deleted.\n\n'
                'Delete the desktop observation too?'
            )
            box.setInformativeText(
                self._format_deleted_cloud_observation_label(entry)
                + '\n\nChoose Keep desktop only to keep it locally and remove the cloud link.'
            )
            delete_btn = box.addButton('Delete desktop', QMessageBox.YesRole)
            keep_btn = box.addButton('Keep desktop only', QMessageBox.NoRole)
            box.setDefaultButton(keep_btn)
            box.exec()
            clicked = box.clickedButton()
            if clicked is delete_btn:
                ObservationDB.delete_observation(local_id)
            else:
                unlink_local_observation_from_cloud(local_id)
            changed = True
        if changed:
            parent = self.parent()
            refresh = getattr(parent, 'refresh_observations', None)
            if callable(refresh):
                try:
                    refresh(show_status=False)
                except Exception:
                    try:
                        refresh()
                    except Exception:
                        pass
        return changed

    def _on_sync_done(self, result: dict) -> None:
        self._progress.setRange(0, 1)
        self._progress.setValue(1)
        pushed = result.get('pushed', 0)
        pulled = result.get('pulled', 0)
        errors = result.get('errors', [])
        deleted_remote = [dict(row or {}) for row in (result.get('deleted_remote') or []) if row]
        issue_summary = summarize_sync_issues(errors)
        conflicts = list(issue_summary.get('conflicts', []) or [])
        conflict_count = int(issue_summary.get('conflict_count', 0) or 0)
        other_count = int(issue_summary.get('other_count', 0) or 0)
        deleted_count = len(deleted_remote)
        parts = []
        if pushed:
            parts.append(f'{pushed} observation{"s" if pushed != 1 else ""} pushed')
        if pulled:
            parts.append(f'{pulled} observation{"s" if pulled != 1 else ""} pulled')
        if not parts:
            parts.append('Everything up to date')
        summary = ', '.join(parts) + '.'
        if errors:
            issue_parts = []
            if conflict_count:
                issue_parts.append(f'{conflict_count} conflict{"s" if conflict_count != 1 else ""}')
            if other_count:
                issue_parts.append(f'{other_count} error{"s" if other_count != 1 else ""}')
            summary += f"\n{', '.join(issue_parts)} — check console or Details for raw messages."
            for e in errors:
                print(f'[cloud_sync] {e}')
        elif deleted_count:
            summary += f"\n{deleted_count} deleted cloud observation{'s' if deleted_count != 1 else ''} need review."

        self._status_label.setText(summary)
        self._progress.hide()
        self._sync_btn.setEnabled(True)
        self._signout_btn.setEnabled(True)

        if errors:
            box = QMessageBox(self)
            box.setIcon(QMessageBox.Warning)
            box.setWindowTitle('Sporely Cloud Sync')
            if conflict_count and not other_count:
                box.setText('Most cloud changes synced automatically, but a few observations still need review.')
            else:
                box.setText('Cloud sync completed, but some observations or images failed.')
            box.setInformativeText(
                f'Pushed: {pushed}\nPulled: {pulled}\nNeeds review: {conflict_count}\nOther errors: {other_count}\n\nOpen Details to copy the full error list.'
            )
            box.setDetailedText('\n'.join(str(err) for err in errors))
            box.exec()
        if conflicts:
            dialog = CloudConflictDialog(
                self,
                conflicts=conflicts,
                prepare_images_cb=self._prepare_images_cb,
            )
            dialog.exec()
        if deleted_remote:
            self._prompt_for_deleted_cloud_observations(deleted_remote)

    def _on_sync_error(self, msg: str) -> None:
        summary = self._summarize_sync_error(msg)
        self._status_label.setText(summary)
        self._progress.hide()
        self._sync_btn.setEnabled(True)
        self._signout_btn.setEnabled(True)
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle('Sporely Cloud Sync')
        box.setText(summary)
        box.setInformativeText('Open Details to copy the raw server/message text.')
        box.setDetailedText(str(msg))
        box.exec()

    def _summarize_sync_error(self, msg: str) -> str:
        text = str(msg or '').strip()
        if text.startswith('Push phase failed'):
            return 'Cloud sync failed while pushing local observations to Sporely Cloud.'
        if text.startswith('Pull phase failed'):
            return 'Cloud sync failed while pulling observations from Sporely Cloud.'
        return 'Cloud sync failed.'

    # ── Sign out ──────────────────────────────────────────────────────────

    def _do_signout(self) -> None:
        SporelyCloudClient.clear_credentials()
        self._client = None
        self._email_input.clear()
        self._pw_input.clear()
        self._show_login_panel()
