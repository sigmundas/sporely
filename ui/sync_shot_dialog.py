"""Modal dialog for creating a clock-calibration Sync Shot."""
from __future__ import annotations

from uuid import uuid4

from PIL.ImageQt import ImageQt

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QPixmap
from PySide6.QtWidgets import QDialog, QDialogButtonBox, QLabel, QVBoxLayout, QWidget

from utils.sync_shot_qr import (
    SYNC_SHOT_QR_INTERVAL_MS,
    SYNC_SHOT_QR_BLANK_MS,
    SYNC_SHOT_QR_VISIBLE_MS,
    build_sync_shot_payload,
    current_sync_shot_utc,
    format_sync_shot_utc,
    render_sync_shot_qr,
)


class SyncShotDialog(QDialog):
    """Display a live QR code that can later calibrate a camera batch."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Sync Shot"))
        self.setModal(True)
        self.resize(620, 620)
        self.shot_record: dict | None = None
        self._blank_frame_active = False
        self._frame_timer = QTimer(self)
        self._frame_timer.setSingleShot(True)
        self._frame_timer.timeout.connect(self._advance_frame)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        intro_label = QLabel(
            self.tr(
                "Photograph this screen with your camera, then close this dialog. When you "
                "scan an import folder, Sporely will auto-check the first and last image from "
                "each folder for this QR and use it to calibrate the batch clock offset. "
                "You can still use 'Use image...' if needed."
            )
        )
        intro_label.setWordWrap(True)
        layout.addWidget(intro_label)

        self.qr_label = QLabel("")
        self.qr_label.setAlignment(Qt.AlignCenter)
        self.qr_label.setMinimumSize(360, 360)
        self.qr_label.setStyleSheet(
            "padding: 18px; border: 1px solid #cbd5e1; border-radius: 10px; background-color: #ffffff;"
        )
        layout.addWidget(self.qr_label, 1)

        self.token_label = QLabel("")
        self.token_label.setAlignment(Qt.AlignCenter)
        token_font = QFont("Menlo")
        if not token_font.exactMatch():
            token_font.setStyleHint(QFont.Monospace)
        token_font.setPointSize(16)
        token_font.setBold(True)
        self.token_label.setFont(token_font)
        self.token_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(self.token_label)

        times_panel = QWidget()
        times_layout = QVBoxLayout(times_panel)
        times_layout.setContentsMargins(0, 0, 0, 0)
        times_layout.setSpacing(4)
        self.cadence_label = QLabel(
            self.tr("QR updates every 2 seconds with a 0.1 second blank frame between codes.")
        )
        self.cadence_label.setWordWrap(True)
        self.cadence_label.setStyleSheet("color: #6b7280;")
        times_layout.addWidget(self.cadence_label)
        self.local_time_label = QLabel("")
        self.local_time_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.utc_time_label = QLabel("")
        self.utc_time_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        times_layout.addWidget(self.local_time_label)
        times_layout.addWidget(self.utc_time_label)
        layout.addWidget(times_panel)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok, Qt.Horizontal, self)
        ok_button = buttons.button(QDialogButtonBox.Ok)
        if ok_button is not None:
            ok_button.setText(self.tr("Done"))
        buttons.accepted.connect(self.accept)
        layout.addWidget(buttons)

        self._start_new_session()

    def _start_new_session(self) -> None:
        self._frame_timer.stop()
        session_id = uuid4().hex
        created_utc = current_sync_shot_utc()
        self.shot_record = {
            "shot_id": uuid4().hex,
            "mode": "qr",
            "session_id": session_id,
            "created_utc_text": format_sync_shot_utc(created_utc),
            "qr_interval_ms": SYNC_SHOT_QR_INTERVAL_MS,
            "blank_interval_ms": SYNC_SHOT_QR_BLANK_MS,
        }
        self._blank_frame_active = False
        self._show_current_qr()

    def _advance_frame(self) -> None:
        if self._blank_frame_active:
            self._blank_frame_active = False
            self._show_current_qr()
            return
        self._blank_frame_active = True
        self.qr_label.clear()
        self.qr_label.setText("")
        self._frame_timer.start(SYNC_SHOT_QR_BLANK_MS)

    def _show_current_qr(self) -> None:
        if not self.shot_record:
            return
        utc_dt = current_sync_shot_utc()
        payload = build_sync_shot_payload(utc_dt, str(self.shot_record.get("session_id") or ""))
        self.shot_record["current_payload"] = payload
        self.shot_record["current_utc_text"] = format_sync_shot_utc(utc_dt)
        self.shot_record["current_local_text"] = utc_dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")

        try:
            qr_image = render_sync_shot_qr(payload)
        except ImportError:
            self.qr_label.setPixmap(QPixmap())
            self.qr_label.setText(self.tr("Install Sync Shot QR dependencies to use this tool."))
            self.token_label.setText(self.tr("QR dependencies missing"))
            self.local_time_label.setText("")
            self.utc_time_label.setText("")
            return
        pixmap = QPixmap.fromImage(ImageQt(qr_image))
        pixmap = pixmap.scaled(
            360,
            360,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        self.qr_label.setPixmap(pixmap)
        self.token_label.setText(self.tr("SYNC-SHOT {value}").format(value=self.shot_record["current_utc_text"]))
        self.local_time_label.setText(
            self.tr("Local clock: {value}").format(value=self.shot_record["current_local_text"])
        )
        self.utc_time_label.setText(
            self.tr("UTC clock: {value} UTC").format(
                value=str(self.shot_record["current_utc_text"]).replace("T", " ").replace("Z", "")
            )
        )
        self._frame_timer.start(SYNC_SHOT_QR_VISIBLE_MS)

    def done(self, result: int) -> None:
        self._frame_timer.stop()
        super().done(result)
