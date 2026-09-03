"""Cloud sync dialog — login and bidirectional sync with Sporely cloud."""
from __future__ import annotations

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel,
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
    CloudReauthRequiredError,
    CloudSyncError,
    is_cloud_auth_error,
    is_image_too_large_for_plan_error,
    format_original_upload_summary,
    partition_download_from_cloud_issues,
    sanitize_image_too_large_for_plan_error_message,
    summarize_blocked_write_attempts,
    summarize_image_too_large_for_plan_error,
    sync_all,
    summarize_sync_issues,
    unlink_local_observation_from_cloud,
)
from utils.cloud_media_policy import WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE


class _OAuthLoginWorker(QThread):
    ok    = Signal(object)       # OAuthSporelyCloudClient
    fail  = Signal(str, str)     # message, kind: "cancelled"|"oauth_error"|"runtime_error"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("Cloud OAuth login")
        self._stop_requested = False

    def cancel(self) -> None:
        self._stop_requested = True

    def run(self) -> None:
        from utils.sporely_cloud_auth import SporelyDesktopOAuthClient, OAuthError
        from utils.cloud_sync import OAuthSporelyCloudClient

        def _tick():
            if self._stop_requested:
                raise InterruptedError("Cancelled.")

        try:
            result = SporelyDesktopOAuthClient().authorize(tick_callback=_tick)
            client = OAuthSporelyCloudClient.from_oauth_session(result)
            self.ok.emit(client)
        except InterruptedError:
            self.fail.emit("Sign-in cancelled.", "cancelled")
        except OAuthError as e:
            self.fail.emit(str(e), "oauth_error")
        except RuntimeError as e:
            self.fail.emit(str(e), "runtime_error")
        except Exception as e:
            self.fail.emit(f"Unexpected error: {e}", "runtime_error")
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
        pull_only: bool = False,
    ):
        super().__init__()
        self.setObjectName("Cloud download (dialog)" if pull_only else "Cloud sync (dialog)")
        self._client = client
        self._push_images = push_images
        self._materialize_remote_images = materialize_remote_images
        self._prepare_images_cb = prepare_images_cb
        self._pull_only = bool(pull_only)

    def run(self) -> None:
        try:
            result = sync_all(
                self._client,
                progress_cb=lambda msg, cur, tot: self.progress.emit(msg, cur, tot),
                sync_images=self._push_images,
                materialize_remote_images=self._materialize_remote_images,
                prepare_images_cb=self._prepare_images_cb,
                pull_only=self._pull_only,
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
        self._oauth_worker: _OAuthLoginWorker | None = None
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

        self._login_desc_label = QLabel('Sign in to your Sporely account to enable cloud sync.')
        self._login_desc_label.setWordWrap(True)
        lf.addWidget(self._login_desc_label)

        self._signin_btn = QPushButton('Sign in in browser')
        self._signin_btn.setDefault(True)
        self._signin_btn.clicked.connect(self._start_oauth_login)
        lf.addWidget(self._signin_btn)

        self._waiting_label = QLabel('Waiting for browser sign-in…')
        self._waiting_label.hide()
        lf.addWidget(self._waiting_label)

        self._cancel_btn = QPushButton('Cancel')
        self._cancel_btn.clicked.connect(self._cancel_oauth_login)
        self._cancel_btn.hide()
        lf.addWidget(self._cancel_btn)

        self._login_error = QLabel('')
        self._login_error.setWordWrap(True)
        self._login_error.setStyleSheet('color: #c05848;')
        self._login_error.hide()
        lf.addWidget(self._login_error)

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

        # Download from Cloud is a strictly cloud → desktop pull. It never
        # pushes observation metadata, images, mosaics, measurements, or
        # tombstones. Kept as a separate button from Sync Now so users can
        # deliberately choose the read-only path.
        self._download_btn = QPushButton('Download from Cloud')
        self._download_btn.setToolTip(
            'Pull cloud observations and images to this device without '
            'uploading any local changes.'
        )
        self._download_btn.clicked.connect(self._do_download_from_cloud)
        btn_row.addWidget(self._download_btn)

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
        try:
            self._client = SporelyCloudClient.from_stored_credentials()
        except CloudReauthRequiredError:
            self._client = None
            self._show_login_panel(reauth=True)
        else:
            if self._client:
                self._show_sync_panel()
            else:
                self._show_login_panel()

    # ── Panel switching ──────────────────────────────────────────────────

    def _show_login_panel(self, *, reauth: bool = False) -> None:
        if reauth:
            self._login_desc_label.setText('Cloud sign-in is required.')
        else:
            self._login_desc_label.setText('Sign in to your Sporely account to enable cloud sync.')
        self._signin_btn.show()
        self._signin_btn.setEnabled(True)
        self._waiting_label.hide()
        self._cancel_btn.hide()
        self._login_error.hide()
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

    # ── Login (OAuth) ───────────────────────────────────────────────────────

    def _start_oauth_login(self) -> None:
        if self._oauth_worker is not None:
            return
        self._signin_btn.setEnabled(False)
        self._signin_btn.hide()
        self._waiting_label.show()
        self._cancel_btn.show()
        self._login_error.hide()

        self._oauth_worker = _OAuthLoginWorker(self)
        self._oauth_worker.ok.connect(self._on_oauth_success)
        self._oauth_worker.fail.connect(self._on_oauth_failure)
        self._oauth_worker.finished.connect(self._on_oauth_worker_done)
        _track_worker(self._oauth_worker)
        self._oauth_worker.start()

    def _cancel_oauth_login(self) -> None:
        if self._oauth_worker is not None:
            self._oauth_worker.cancel()

    def _on_oauth_success(self, client) -> None:
        from database.schema import update_app_settings
        self._client = client
        email = client.user_email or ''
        client.save_credentials(email=email or None)
        if email:
            update_app_settings({'cloud_user_email': email})
        self._show_sync_panel()

    def _on_oauth_failure(self, message: str, kind: str) -> None:
        if kind == 'cancelled':
            self._login_error.hide()
        else:
            self._login_error.setText(message)
            self._login_error.show()
        self._waiting_label.hide()
        self._cancel_btn.hide()
        self._signin_btn.show()
        self._signin_btn.setEnabled(True)

    def _on_oauth_worker_done(self) -> None:
        self._oauth_worker = None

    def closeEvent(self, event) -> None:
        if self._oauth_worker is not None:
            self._oauth_worker.cancel()
        super().closeEvent(event)

    # ── Sync ─────────────────────────────────────────────────────────────

    def _do_sync(self) -> None:
        if not self._client:
            return
        self._sync_btn.setEnabled(False)
        self._download_btn.setEnabled(False)
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

    def _do_download_from_cloud(self) -> None:
        """Cloud → desktop pull only. Zero cloud writes."""
        if not self._client:
            return
        self._sync_btn.setEnabled(False)
        self._download_btn.setEnabled(False)
        self._signout_btn.setEnabled(False)
        self._status_label.setText('Downloading from Cloud…')
        self._progress.setValue(0)
        self._progress.setRange(0, 0)
        self._progress.show()

        # push_images is irrelevant in pull-only mode; the pull-only
        # branch skips all push work regardless. Materialization follows
        # the same checkbox the normal Sync uses.
        self._worker = _SyncWorker(
            self._client,
            push_images=False,
            materialize_remote_images=self._pull_images_check.isChecked(),
            prepare_images_cb=None,
            pull_only=True,
        )
        self._worker.progress.connect(self._on_sync_progress)
        self._worker.sync_finished.connect(self._on_sync_done)
        self._worker.error.connect(self._on_sync_error)
        _track_worker(self._worker)
        self._worker.start()

    def _on_sync_progress(self, msg: str, cur: int, total: int) -> None:
        if total > 0:
            display_total = int(total)
            display_cur = max(0, min(int(cur), display_total))
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
        if result.get('pull_only'):
            self._on_download_from_cloud_done(result)
            return
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
        self._download_btn.setEnabled(True)
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
            # Shared final gate: apply the automatic decisions and only pass
            # candidates still containing genuine manual conflicts to the
            # dialog.  This prevents the "empty dialog" case where a
            # preflight-flagged observation has already-automatic changes
            # (additive image, one-sided scalar edit, Draft transition) that
            # the dialog itself has no work to do on.
            from utils.cloud_sync import finalize_sync_candidates, SporelyCloudClient
            gate_client = SporelyCloudClient.from_stored_credentials()
            if gate_client is not None:
                manual_conflicts, gate_errors = finalize_sync_candidates(
                    gate_client, conflicts,
                    prepare_images_cb=self._prepare_images_cb,
                )
                if gate_errors:
                    # Surface automatic-execution failures as sync errors —
                    # never hide.
                    self._status_label.setText(
                        f'Cloud sync applied automatic changes with {len(gate_errors)} error(s).'
                    )
                    print(
                        '[cloud_sync] finalize gate errors: '
                        + '; '.join(str(e) for e in gate_errors),
                        flush=True,
                    )
                conflicts = manual_conflicts
            if not conflicts:
                # All flagged candidates resolved automatically.  Do not open
                # a dialog with nothing to review.
                self._status_label.setText(
                    'Cloud sync applied automatic changes; no manual review needed.'
                )
                if deleted_remote:
                    self._prompt_for_deleted_cloud_observations(deleted_remote)
                return
            dialog = CloudConflictDialog(
                self,
                conflicts=conflicts,
                prepare_images_cb=self._prepare_images_cb,
            )
            # Turn B: the dialog itself now runs the per-conflict resolution
            # worker (see ConflictPlanApplyWorker in cloud_conflict_dialog).
            # dialog.decisions[] is the log of already-committed applies once
            # exec() returns.  We no longer launch a second worker here.
            result = dialog.exec()

            resolved_count = len([d for d in (dialog.decisions or []) if d.get('action') == 'plan'])
            deferred_conflicts = [
                c for c in conflicts
                if c not in [d.get('_source_conflict') for d in (dialog.decisions or [])]
            ]
            if resolved_count:
                self._status_label.setText(
                    f'Conflict resolution finished. Applied {resolved_count} plan(s).'
                )
            if resolved_count == 0 or result != QDialog.Accepted:
                # "Review later" (or dialog dismissed): the still-pending
                # conflicts remain dirty; the next sync will re-open the
                # dialog if the divergence is still present.  Do NOT clear
                # snapshots or stamp anything synced.
                for conflict in deferred_conflicts or conflicts:
                    try:
                        deferred_local_id = int(conflict.get('local_id') or 0)
                    except Exception:
                        deferred_local_id = 0
                    print(
                        f"[cloud_sync] conflict review deferred: obs={deferred_local_id}",
                        flush=True,
                    )
                if resolved_count == 0:
                    self._status_label.setText(
                        'Conflict review canceled. Unresolved conflicts remain and no decisions were applied.'
                    )

        if deleted_remote:
            self._prompt_for_deleted_cloud_observations(deleted_remote)

    def _on_download_from_cloud_done(self, result: dict) -> None:
        """Feedback specific to Download from Cloud: images + updates + zero writes."""
        images_downloaded = int(result.get('images_downloaded') or 0)
        observations_updated = int(result.get('observations_updated') or 0)
        writes_completed = int(result.get('cloud_writes_completed') or 0)
        blocked_writes = list(result.get('blocked_write_attempts') or [])
        errors_raw = list(result.get('errors') or [])
        review_items, real_errors = partition_download_from_cloud_issues(errors_raw)

        images_word = 'image' if images_downloaded == 1 else 'images'
        observations_word = 'observation' if observations_updated == 1 else 'observations'
        parts = [
            f"Downloaded {images_downloaded} {images_word}; "
            f"updated {observations_updated} {observations_word}.",
            "No cloud changes made." if writes_completed == 0 else
            f"{writes_completed} cloud change(s) made.",
        ]
        if review_items:
            n = len(review_items)
            parts.append(f"{n} observation{'s' if n != 1 else ''} need review.")
        if real_errors:
            n = len(real_errors)
            parts.append(f"{n} error{'s' if n != 1 else ''}.")
        summary = " ".join(parts)
        self._status_label.setText(summary)

        # Details are only shown behind an information box the user can
        # open on demand. Blocked writes are deduplicated so a single leaky
        # source (e.g. 595 identical calls) reads as ``name ×595`` not a
        # 595-line dump.
        detail_sections: list[str] = []
        if blocked_writes:
            detail_sections.append(
                "Blocked cloud write attempts (defence in depth — nothing "
                "reached the network):\n"
                + summarize_blocked_write_attempts(blocked_writes)
            )
        if review_items:
            detail_sections.append(
                "Observations that need review (cloud/local differences kept "
                "as-is):\n" + "\n".join(f"  • {line}" for line in review_items)
            )
        if real_errors:
            detail_sections.append(
                "Errors:\n" + "\n".join(f"  • {line}" for line in real_errors)
            )

        if detail_sections:
            box = QMessageBox(self)
            box.setIcon(
                QMessageBox.Warning if (real_errors or blocked_writes) else QMessageBox.Information
            )
            box.setWindowTitle('Download from Cloud')
            box.setText(summary)
            box.setStandardButtons(QMessageBox.Ok)
            box.setDetailedText("\n\n".join(detail_sections))
            box.exec()

        for err in real_errors:
            print(f'[cloud_sync] {err}')
        self._progress.hide()
        self._sync_btn.setEnabled(True)
        self._download_btn.setEnabled(True)
        self._signout_btn.setEnabled(True)

    def _on_sync_error(self, msg: str) -> None:
        summary = self._summarize_sync_error(msg)
        self._status_label.setText(summary)
        self._progress.hide()
        self._sync_btn.setEnabled(True)
        self._download_btn.setEnabled(True)
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
            return 'Cloud sync sign-in failed. Please sign in again.'
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
        self._show_login_panel()
