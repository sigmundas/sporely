"""Shared dialog helpers for consistent cross-platform sizing."""
from __future__ import annotations

from PySide6.QtCore import QCoreApplication, Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLayout,
    QSizePolicy,
    QStyle,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app_identity import APP_DOCS_BASE_URL


def github_doc_url(doc_filename: str) -> str:
    """Return the GitHub URL for a help page stored under docs/."""
    return f"{APP_DOCS_BASE_URL}/{str(doc_filename).lstrip('/')}"


def open_github_doc(doc_filename: str) -> None:
    """Open a docs page on GitHub in the system browser."""
    QDesktopServices.openUrl(QUrl(github_doc_url(doc_filename)))


def make_github_help_button(parent: QWidget | None, doc_filename: str) -> QToolButton:
    """Create a compact '?' help button that opens a GitHub docs page."""
    button = QToolButton(parent)
    button.setText("?")
    button.setCursor(Qt.PointingHandCursor)
    button.setToolTip("Open help on GitHub")
    button.setAutoRaise(False)
    button.setFixedSize(28, 28)
    button.clicked.connect(lambda: open_github_doc(doc_filename))
    return button


def ask_wrapped_yes_no(
    parent: QWidget | None,
    title: str,
    text: str,
    *,
    default_yes: bool = False,
    yes_text: str | None = None,
    no_text: str | None = None,
) -> bool:
    """Show a compact wrapped Yes/No dialog with reliable Linux sizing."""
    def _helper_tr(value: str) -> str:
        return QCoreApplication.translate("DialogHelpers", value)

    host = parent
    tr = getattr(parent, "tr", None)
    if not callable(tr):
        tr = _helper_tr

    dialog = QDialog(host)
    dialog.setWindowTitle(str(title))
    dialog.setModal(True)
    dialog.setWindowFlags(
        Qt.Dialog
        | Qt.CustomizeWindowHint
        | Qt.WindowTitleHint
        | Qt.WindowCloseButtonHint
    )
    dialog.setStyleSheet(
        "QDialogButtonBox QPushButton { min-width: 90px; max-width: 200px; padding: 6px 10px; }"
    )

    outer = QVBoxLayout(dialog)
    outer.setContentsMargins(16, 14, 16, 12)
    outer.setSpacing(12)
    outer.setSizeConstraint(QLayout.SetMinimumSize)

    row = QHBoxLayout()
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(12)

    icon_label = QLabel(dialog)
    icon = dialog.style().standardIcon(QStyle.SP_MessageBoxQuestion)
    icon_label.setPixmap(icon.pixmap(48, 48))
    icon_label.setAlignment(Qt.AlignHCenter | Qt.AlignTop)
    icon_label.setFixedWidth(56)
    row.addWidget(icon_label, 0, Qt.AlignTop)

    text_label = QLabel(str(text), dialog)
    text_label.setWordWrap(True)
    text_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
    text_label.setMinimumWidth(440)
    text_label.setMaximumWidth(640)
    text_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
    row.addWidget(text_label, 1)
    outer.addLayout(row)

    buttons = QDialogButtonBox(dialog)
    no_btn = buttons.addButton(no_text or _helper_tr("No"), QDialogButtonBox.RejectRole)
    yes_btn = buttons.addButton(yes_text or _helper_tr("Yes"), QDialogButtonBox.AcceptRole)
    if default_yes:
        yes_btn.setDefault(True)
        yes_btn.setAutoDefault(True)
    else:
        no_btn.setDefault(True)
        no_btn.setAutoDefault(True)
    outer.addWidget(buttons)

    accepted = {"value": False}

    def _accept() -> None:
        accepted["value"] = True
        dialog.accept()

    yes_btn.clicked.connect(_accept)
    no_btn.clicked.connect(dialog.reject)
    buttons.rejected.connect(dialog.reject)

    dialog.setMinimumWidth(620)
    dialog.setMaximumWidth(760)
    dialog.setMinimumHeight(185)
    dialog.adjustSize()
    hint = dialog.sizeHint()
    dialog.resize(max(620, min(760, hint.width() + 12)), max(190, hint.height() + 8))
    dialog.exec()
    return bool(accepted["value"])


def ask_measurements_exist_delete(parent: QWidget | None, count: int = 1) -> bool:
    """Ask whether to delete image(s) that already have measurements."""
    host = parent
    tr = getattr(parent, "tr", None)
    if not callable(tr):
        tr = lambda s: s

    dialog = QDialog(host)
    dialog.setWindowTitle(tr("Measurements exist"))
    dialog.setModal(True)
    dialog.setWindowFlags(
        Qt.Dialog
        | Qt.CustomizeWindowHint
        | Qt.WindowTitleHint
        | Qt.WindowCloseButtonHint
    )
    dialog.setStyleSheet(
        "QDialogButtonBox QPushButton { min-width: 90px; max-width: 200px; padding: 6px 10px; }"
    )

    outer = QVBoxLayout(dialog)
    outer.setContentsMargins(16, 14, 16, 12)
    outer.setSpacing(12)
    outer.setSizeConstraint(QLayout.SetMinimumSize)

    row = QHBoxLayout()
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(12)

    icon_label = QLabel(dialog)
    icon = dialog.style().standardIcon(QStyle.SP_MessageBoxWarning)
    icon_label.setPixmap(icon.pixmap(48, 48))
    icon_label.setAlignment(Qt.AlignHCenter | Qt.AlignTop)
    icon_label.setFixedWidth(56)
    row.addWidget(icon_label, 0, Qt.AlignTop)

    text_col = QVBoxLayout()
    text_col.setContentsMargins(0, 0, 0, 0)
    text_col.setSpacing(4)

    if int(count or 1) == 1:
        main_text = tr("Measurements exist for this image.")
    else:
        main_text = tr("Measurements exist for {count} images.").format(count=int(count))
    detail_text = tr("Delete anyway?")

    main_label = QLabel(main_text, dialog)
    main_label.setWordWrap(True)
    main_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
    main_label.setMinimumWidth(440)
    main_label.setMaximumWidth(640)
    main_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
    text_col.addWidget(main_label)

    detail_label = QLabel(detail_text, dialog)
    detail_label.setWordWrap(True)
    detail_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
    detail_label.setMinimumWidth(440)
    detail_label.setMaximumWidth(640)
    detail_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
    text_col.addWidget(detail_label)

    row.addLayout(text_col, 1)
    outer.addLayout(row)

    buttons = QDialogButtonBox(dialog)
    cancel_btn = buttons.addButton(tr("Cancel"), QDialogButtonBox.RejectRole)
    delete_btn = buttons.addButton(tr("Delete"), QDialogButtonBox.DestructiveRole)
    delete_btn.setDefault(False)
    cancel_btn.setDefault(True)
    cancel_btn.setAutoDefault(True)
    delete_btn.setStyleSheet(
        "QPushButton { background-color: #e74c3c; color: white; border: none; border-radius: 6px; }"
        "QPushButton:hover { background-color: #c0392b; }"
        "QPushButton:pressed { background-color: #a93226; }"
    )
    outer.addWidget(buttons)

    accepted = {"value": False}

    def _accept() -> None:
        accepted["value"] = True
        dialog.accept()

    delete_btn.clicked.connect(_accept)
    cancel_btn.clicked.connect(dialog.reject)
    buttons.rejected.connect(dialog.reject)

    dialog.setMinimumWidth(620)
    dialog.setMaximumWidth(760)
    dialog.setMinimumHeight(185)
    dialog.adjustSize()
    hint = dialog.sizeHint()
    dialog.resize(max(620, min(760, hint.width() + 12)), max(190, hint.height() + 8))
    dialog.exec()
    return bool(accepted["value"])
