"""Cloud sync dialog — login and bidirectional sync with Sporely cloud."""
from __future__ import annotations

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QProgressBar, QFrame, QMessageBox, QCheckBox,
)

_running_cloud_sync_workers: list[QThread] = []

def _track_worker(worker: QThread) -> None:
    _running_cloud_sync_workers.append(worker)
    def _on_finished() -> None:
        def _remove():
            try:
                _running_cloud_sync_workers.remove(worker)
            except ValueError:
                pass
            try:
                worker.deleteLater()
            except Exception:
                pass
        from PySide6.QtCore import QTimer
        QTimer.singleShot(2000, _remove)
    worker.finished.connect(_on_finished)

from database.models import ObservationDB
from database.schema import get_app_settings
from .dialog_helpers import ask_wrapped_yes_no_with_checkbox
from utils.cloud_sync import (
    SporelyCloudClient,
    ACCOUNT_MISMATCH_MESSAGE,
    AccountMismatchError,
    CloudSyncError,
    is_cloud_auth_error,
    is_image_too_large_for_plan_error,
    format_original_upload_summary,
    sanitize_image_too_large_for_plan_error_message,
    summarize_image_too_large_for_plan_error,
    sync_all,
    load_saved_cloud_password,
    summarize_sync_issues,
    unlink_local_observation_from_cloud,
)
from utils.cloud_media_policy import WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE
from .cloud_conflict_dialog import CloudConflictDialog


class _SyncWorker(QThread):
    progress  = Signal(str, int, int)   # message, current, total
    sync_finished  = Signal(dict)            # summary dict
    error     = Signal(str)

    def __init__(
        self,
        client: SporelyCloudClient,
        push_images: bool,
        materialize_remote_images: bool,
        prepare_images_cb=None,
    ):
        super().__init__()
        self.setObjectName("Cloud sync (dialog)")
        self._client = client
        self._push_images = push_images
        self._materialize_remote_images = materialize_remote_images
        self._prepare_images_cb = prepare_images_cb

    def run(self) -> None:
        try:
            result = sync_all(
                self._client,
                progress_cb=lambda msg, cur, tot: self.progress.emit(msg, cur, tot),
                sync_images=self._push_images,
                materialize_remote_images=self._materialize_remote_images,
                prepare_images_cb=self._prepare_images_cb,
            )
            self.sync_finished.emit(result)
        except AccountMismatchError:
            self.error.emit(ACCOUNT_MISMATCH_MESSAGE)
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

        self._push_images_check = QCheckBox('Upload desktop images to cloud')
        self._push_images_check.setChecked(True)
        sf.addWidget(self._push_images_check)

        self._pull_images_check = QCheckBox('Download cloud images to this device')
        self._pull_images_check.setChecked(True)
        sf.addWidget(self._pull_images_check)

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
                self.setObjectName("Cloud login (dialog)")
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
        _track_worker(self._login_worker)
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
            self._push_images_check.isChecked(),
            self._pull_images_check.isChecked(),
            prepare_images_cb=self._prepare_images_cb,
        )
        self._worker.progress.connect(self._on_sync_progress)
        self._worker.sync_finished.connect(self._on_sync_done)
        self._worker.error.connect(self._on_sync_error)
        _track_worker(self._worker)
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
        """Refined to ensure local files aren't deleted without explicit user choice."""
        entries = [dict(row or {}) for row in (deleted_remote or []) if row]
        if not entries:
            return False
        changed = False
        bulk_choice: str | None = None
        total = len(entries)
        for index, entry in enumerate(entries):
            local_id = int(entry.get('local_id') or 0)
            if local_id <= 0:
                continue

            if bulk_choice == 'delete':
                ObservationDB.delete_observation(local_id)
                changed = True
                continue
            if bulk_choice == 'keep':
                unlink_local_observation_from_cloud(local_id)
                changed = True
                continue

            remaining = total - index
            prompt = self.tr(
                "Cloud observation {cloud_id} was deleted.\n\n"
                "{details}\n\n"
                "Delete the desktop observation too?\n\n"
                "Choose No to keep it locally only and remove the cloud link."
            ).format(
                cloud_id=str(entry.get('cloud_id') or '?').strip() or '?',
                details=self._format_deleted_cloud_observation_label(entry),
            )
            if remaining > 1:
                prompt += "\n\n" + self.tr("{count} observations remain in this review.").format(
                    count=remaining,
                )
            delete_local, apply_to_all = ask_wrapped_yes_no_with_checkbox(
                self,
                self.tr("Cloud Observation Deleted"),
                prompt,
                checkbox_text=self.tr("Apply this choice to all remaining deleted cloud observations"),
                default_yes=False,
                yes_text=self.tr("Delete local copy"),
                no_text=self.tr("Keep local only (Unlink)"),
            )

            if delete_local:
                confirm = QMessageBox.warning(
                    self,
                    self.tr("Confirm Delete"),
                    self.tr(
                        "This will permanently delete the observation record and associated local image references. Continue?"
                    ),
                    QMessageBox.Yes | QMessageBox.No,
                )
                if confirm == QMessageBox.Yes:
                    ObservationDB.delete_observation(local_id)
                    changed = True
                    if apply_to_all:
                        bulk_choice = 'delete'
            else:
                unlink_local_observation_from_cloud(local_id)
                changed = True
                if apply_to_all:
                    bulk_choice = 'keep'

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
        blocked_count = int(issue_summary.get('blocked_count', 0) or 0)
        retryable_count = int(issue_summary.get('retryable_count', 0) or 0)
        other_count = int(issue_summary.get('other_count', 0) or 0)
        deleted_count = len(deleted_remote)
        parts = []
        if pushed:
            parts.append(f'{pushed} observation{"s" if pushed != 1 else ""} pushed')
        if pulled:
            parts.append(f'{pulled} observation{"s" if pulled != 1 else ""} pulled')
        if not parts:
            if blocked_count:
                parts.append('Cloud sync blocked')
            elif retryable_count:
                parts.append('Cloud sync needs retry')
            else:
                parts.append('Everything up to date')
        summary = ', '.join(parts) + '.'
        original_summary = format_original_upload_summary(result.get('original_sync'))
        if original_summary:
            summary += f"\n{original_summary}"
        if errors:
            issue_parts = []
            if conflict_count:
                issue_parts.append(f'{conflict_count} conflict{"s" if conflict_count != 1 else ""}')
            if blocked_count:
                issue_parts.append(f'{blocked_count} blocked')
            if retryable_count:
                issue_parts.append(f'{retryable_count} will retry')
            if other_count:
                issue_parts.append(f'{other_count} error{"s" if other_count != 1 else ""}')
            summary += f"\n{', '.join(issue_parts)} — check console or Details for raw messages."
            if blocked_count:
                blocked_messages = []
                for entry in issue_summary.get('blocked_errors', []) or []:
                    message = str(entry.get('message') or '').strip()
                    if message and message not in blocked_messages:
                        blocked_messages.append(message)
                if blocked_messages:
                    summary += '\n' + '\n'.join(blocked_messages)
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
            if blocked_count and not conflict_count and not other_count and not retryable_count:
                box.setText('Cloud sync blocked by the privacy cap.')
            elif retryable_count and not conflict_count and not blocked_count and not other_count:
                box.setText('Cloud sync completed, but some images will retry.')
            elif conflict_count and not other_count and not blocked_count:
                box.setText('Most cloud changes synced automatically, but a few observations still need review.')
            else:
                box.setText('Cloud sync completed, but some observations or images failed.')
            box.setInformativeText(
                f'Pushed: {pushed}\nPulled: {pulled}\nNeeds review: {conflict_count}\nBlocked: {blocked_count}\nWill retry: {retryable_count}\nOther errors: {other_count}\n\nOpen Details to copy the full error list.'
            )
            box.setDetailedText('\n'.join(str(err) for err in errors))
            box.exec()
        if conflicts:
            dialog = CloudConflictDialog(
                self,
                conflicts=conflicts,
                prepare_images_cb=self._prepare_images_cb,
            )
            result = dialog.exec()

            if result == QDialog.Accepted and dialog.decisions:
                from .cloud_conflict_dialog import ConflictResolutionWorker
                self._resolution_worker = ConflictResolutionWorker(dialog.decisions, prepare_images_cb=self._prepare_images_cb)
                _track_worker(self._resolution_worker)

                def _on_progress(msg, current, total):
                    self._progress.show()
                    self._progress.setMaximum(total)
                    self._progress.setValue(current)
                    self._status_label.setText(msg)

                def _on_finished(resolved_any):
                    self._progress.hide()
                    self._status_label.setText('Conflict resolution finished.')
                    if deleted_remote:
                        self._prompt_for_deleted_cloud_observations(deleted_remote)

                def _on_error(err):
                    self._progress.hide()
                    self._status_label.setText(f'Conflict resolution error: {err}')
                    if deleted_remote:
                        self._prompt_for_deleted_cloud_observations(deleted_remote)

                self._resolution_worker.resolution_finished.connect(_on_finished)
                self._resolution_worker.error.connect(_on_error)
                self._resolution_worker.start()
                return # Deleted remote handled in callback
            else:
                self._status_label.setText(
                    'Conflict review canceled. Unresolved conflicts remain and no decisions were applied.'
                )

        if deleted_remote:
            self._prompt_for_deleted_cloud_observations(deleted_remote)

    def _on_sync_error(self, msg: str) -> None:
        summary = self._summarize_sync_error(msg)
        self._status_label.setText(summary)
        self._progress.hide()
        self._sync_btn.setEnabled(True)
        self._signout_btn.setEnabled(True)
        box = QMessageBox(self)
        is_account_mismatch = str(msg or '').strip() == ACCOUNT_MISMATCH_MESSAGE
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle('Sporely Cloud Sync')
        if is_account_mismatch:
            box.setText(ACCOUNT_MISMATCH_MESSAGE)
        else:
            box.setText(summary)
            if is_image_too_large_for_plan_error(msg):
                box.setInformativeText('Open Details to view the observation, image, and cap details.')
            else:
                box.setInformativeText('Open Details to copy the raw server/message text.')
            box.setDetailedText(
                sanitize_image_too_large_for_plan_error_message(msg)
                if is_image_too_large_for_plan_error(msg)
                else str(msg)
            )
        box.exec()

    def _summarize_sync_error(self, msg: str) -> str:
        text = str(msg or '').strip()
        if text == ACCOUNT_MISMATCH_MESSAGE:
            return 'Cloud sync blocked: this database is linked to another account.'
        if is_cloud_auth_error(text):
            return 'Cloud sync sign-in failed. Please check your email and password.'
        if WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE.lower() in text.lower():
            return 'Cloud sync failed because WebP support is required for cloud media uploads.'
        if is_image_too_large_for_plan_error(text):
            return summarize_image_too_large_for_plan_error(text)
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
