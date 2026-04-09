"""Modal dialog for creating a clock-calibration Sync Shot."""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class SyncShotDialog(QDialog):
    """Freeze a precise shot timestamp that can later calibrate a camera batch."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Sync Shot"))
        self.setModal(True)
        self.resize(560, 320)
        self.shot_record: dict | None = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        intro_label = QLabel(
            self.tr(
                "Photograph this screen with your camera. Later, choose that photo in the "
                "Ingestion Hub to calibrate the camera clock offset for the batch."
            )
        )
        intro_label.setWordWrap(True)
        layout.addWidget(intro_label)

        self.token_label = QLabel("")
        self.token_label.setAlignment(Qt.AlignCenter)
        token_font = QFont("Menlo")
        if not token_font.exactMatch():
            token_font.setStyleHint(QFont.Monospace)
        token_font.setPointSize(18)
        token_font.setBold(True)
        self.token_label.setFont(token_font)
        self.token_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.token_label.setStyleSheet(
            "padding: 16px; border: 1px solid #cbd5e1; border-radius: 10px; background-color: #f8fafc;"
        )
        layout.addWidget(self.token_label)

        times_panel = QWidget()
        times_layout = QVBoxLayout(times_panel)
        times_layout.setContentsMargins(0, 0, 0, 0)
        times_layout.setSpacing(4)
        self.local_time_label = QLabel("")
        self.local_time_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.utc_time_label = QLabel("")
        self.utc_time_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        times_layout.addWidget(self.local_time_label)
        times_layout.addWidget(self.utc_time_label)
        layout.addWidget(times_panel)

        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(8)
        self.new_shot_btn = QPushButton(self.tr("New shot timestamp"))
        self.new_shot_btn.clicked.connect(self._capture_shot)
        button_row.addWidget(self.new_shot_btn)
        button_row.addStretch(1)
        layout.addLayout(button_row)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel,
            Qt.Horizontal,
            self,
        )
        ok_button = buttons.button(QDialogButtonBox.Ok)
        if ok_button is not None:
            ok_button.setText(self.tr("Use this shot"))
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._capture_shot()

    def _capture_shot(self) -> None:
        local_dt = datetime.now().replace(microsecond=0)
        utc_dt = datetime.utcnow().replace(microsecond=0)
        token = f"SYNC-SHOT {utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        self.shot_record = {
            "shot_id": uuid4().hex,
            "token": token,
            "local_dt": local_dt,
            "utc_dt": utc_dt,
            "local_text": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "utc_text": utc_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        self.token_label.setText(token)
        self.local_time_label.setText(
            self.tr("Local clock: {value}").format(value=self.shot_record["local_text"])
        )
        self.utc_time_label.setText(
            self.tr("UTC clock: {value}").format(value=self.shot_record["utc_text"])
        )
