"""Artsobservasjoner login and cookie capture for MycoLog."""

import json
import os
import re
import sys
from pathlib import Path
import requests
from platformdirs import user_data_dir
from typing import Dict, Optional, Callable

_ARTSOBS_WEB_USERNAME_KEY = "artsobs_web_username"
_ARTSOBS_WEB_KEYRING_SERVICE = "MycoLog.Artsobservasjoner"
_ARTSOBS_WEB_KEYRING_ACCOUNT = "web_password"


def _get_keyring_module():
    try:
        import keyring  # type: ignore
        return keyring
    except Exception:
        return None


def _get_saved_web_username() -> str:
    try:
        from database.models import SettingsDB
        return (SettingsDB.get_setting(_ARTSOBS_WEB_USERNAME_KEY, "") or "").strip()
    except Exception:
        return ""


def _set_saved_web_username(username: str) -> None:
    try:
        from database.models import SettingsDB
        SettingsDB.set_setting(_ARTSOBS_WEB_USERNAME_KEY, username or "")
    except Exception:
        return


def _load_saved_web_credentials() -> tuple[str, Optional[str], bool]:
    username = _get_saved_web_username()
    keyring = _get_keyring_module()
    if keyring is None:
        return username, None, False
    try:
        password = keyring.get_password(
            _ARTSOBS_WEB_KEYRING_SERVICE,
            _ARTSOBS_WEB_KEYRING_ACCOUNT,
        )
    except Exception:
        return username, None, False
    return username, password, True


def _save_web_credentials(username: str, password: str) -> None:
    _set_saved_web_username(username)
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure password storage is unavailable on this system.")
    try:
        keyring.set_password(
            _ARTSOBS_WEB_KEYRING_SERVICE,
            _ARTSOBS_WEB_KEYRING_ACCOUNT,
            password,
        )
    except Exception as exc:
        raise RuntimeError(f"Could not securely save password: {exc}") from exc


def _clear_saved_web_credentials() -> None:
    _set_saved_web_username("")
    keyring = _get_keyring_module()
    if keyring is None:
        return
    try:
        keyring.delete_password(
            _ARTSOBS_WEB_KEYRING_SERVICE,
            _ARTSOBS_WEB_KEYRING_ACCOUNT,
        )
    except Exception:
        return


def _prompt_web_credentials(parent=None) -> tuple[Optional[str], Optional[str], bool]:
    """Show a Qt dialog for Artsobservasjoner web credentials."""
    from PySide6.QtWidgets import (
        QDialog,
        QDialogButtonBox,
        QFormLayout,
        QCheckBox,
        QLabel,
        QLineEdit,
        QMessageBox,
        QVBoxLayout,
    )

    saved_username, saved_password, can_store_password = _load_saved_web_credentials()
    has_saved_password = bool(saved_password)
    password_edited = False
    submitted_password: Optional[str] = None
    remember_login = False

    dialog = QDialog(parent)
    dialog.setWindowTitle("Log in to Artsobservasjoner (web)")
    dialog.setModal(True)
    dialog.setMinimumWidth(420)

    layout = QVBoxLayout(dialog)
    layout.addWidget(QLabel("Enter your Artsobservasjoner email and password:"))

    form = QFormLayout()
    username_edit = QLineEdit()
    username_edit.setPlaceholderText("Email")
    username_edit.setText(saved_username)
    password_edit = QLineEdit()
    password_edit.setPlaceholderText("Password")
    password_edit.setEchoMode(QLineEdit.Password)
    if has_saved_password:
        password_edit.setText("********")
    form.addRow("Email:", username_edit)
    form.addRow("Password:", password_edit)
    layout.addLayout(form)

    remember_checkbox = QCheckBox("Save password on this device")
    remember_checkbox.setChecked(bool(saved_username or has_saved_password))
    if not can_store_password:
        remember_checkbox.setChecked(False)
        remember_checkbox.setEnabled(False)
        remember_checkbox.setToolTip("Install keyring to enable encrypted password storage.")
    layout.addWidget(remember_checkbox)

    if has_saved_password:
        layout.addWidget(QLabel("Saved password loaded (shown masked)."))
    if not can_store_password:
        warning = QLabel("Secure password storage unavailable: password will not be saved.")
        warning.setStyleSheet("color: #b35c00;")
        layout.addWidget(warning)

    buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
    layout.addWidget(buttons)

    def _on_password_edited(_text: str):
        nonlocal password_edited
        password_edited = True

    password_edit.textEdited.connect(_on_password_edited)

    def _accept_if_valid():
        nonlocal submitted_password, remember_login
        if not username_edit.text().strip() or not password_edit.text():
            QMessageBox.warning(
                dialog,
                "Missing Information",
                "Please enter both email and password.",
            )
            return
        if has_saved_password and not password_edited:
            submitted_password = saved_password
        else:
            submitted_password = password_edit.text()
        remember_login = bool(remember_checkbox.isChecked())
        if remember_login and not can_store_password:
            QMessageBox.warning(
                dialog,
                "Secure Storage Unavailable",
                "Password saving requires secure keyring support on this system.",
            )
            return
        dialog.accept()

    buttons.accepted.connect(_accept_if_valid)
    buttons.rejected.connect(dialog.reject)
    username_edit.returnPressed.connect(_accept_if_valid)
    password_edit.returnPressed.connect(_accept_if_valid)
    username_edit.setFocus()

    if dialog.exec() != QDialog.Accepted:
        return None, None, False

    return username_edit.text().strip(), submitted_password, remember_login


class ArtsObservasjonerWebLogin:
    """Handles programmatic login to www.artsobservasjoner.no."""

    BASE_URL = "https://www.artsobservasjoner.no"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) "
                    "Gecko/20100101 Firefox/147.0"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def get_csrf_token(self) -> Optional[str]:
        """Fetch /Logon and extract __RequestVerificationToken."""
        response = self.session.get(f"{self.BASE_URL}/Logon", timeout=20)
        response.raise_for_status()

        match = re.search(
            r'name="__RequestVerificationToken"[^>]+value="([^"]+)"',
            response.text,
        )
        if match:
            return match.group(1)

        return self.session.cookies.get("__RequestVerificationToken")

    def login(self, username: str, password: str, remember_me: bool = False) -> bool:
        """Authenticate with username/password."""
        csrf_token = self.get_csrf_token()
        if not csrf_token:
            raise RuntimeError("Failed to get CSRF token from Artsobservasjoner.")

        login_data: list[tuple[str, str]] = [
            ("__RequestVerificationToken", csrf_token),
            ("AuthenticationViewModel.UserName", username),
            ("AuthenticationViewModel.ReturnUrl", ""),
            ("AuthenticationViewModel.Password", password),
        ]
        # Mirror standard ASP.NET checkbox post pattern:
        # hidden false value first, then checkbox true when checked.
        login_data.append(("AuthenticationViewModel.RememberMe", "false"))
        if remember_me:
            login_data.append(("AuthenticationViewModel.RememberMe", "true"))
        login_data.append(("Shared_LogOn", "Logg inn"))

        response = self.session.post(
            f"{self.BASE_URL}/LogOn",
            data=login_data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{self.BASE_URL}/Logon",
            },
            allow_redirects=True,
            timeout=20,
        )
        response.raise_for_status()

        if ".ASPXAUTHNO" not in self.session.cookies:
            return False
        return self.check_auth()

    def check_auth(self) -> bool:
        """Check if session can access MyPages without being redirected to login."""
        response = self.session.get(
            f"{self.BASE_URL}/User/MyPages",
            allow_redirects=True,
            timeout=10,
        )
        if response.status_code != 200:
            return False
        url = (response.url or "").lower()
        return "/logon" not in url and "/account/login" not in url

    def get_cookies_dict(self) -> Dict[str, str]:
        return requests.utils.dict_from_cookiejar(self.session.cookies)


class ArtsObservasjonerAuthWidget:
    """
    PySide6 widget for Artsobservasjoner login
    
    Usage in MycoLog:
    1. Show this widget in a dialog when user needs to authenticate
    2. User logs in through the embedded browser
    3. Widget automatically captures cookies
    4. Save cookies for future use
    """
    
    def __init__(
        self,
        on_login_success: Optional[Callable] = None,
        parent=None,
        login_url: Optional[str] = None,
        required_cookies: Optional[list[str]] = None,
    ):
        """
        Args:
            on_login_success: Callback function called with cookies dict when login succeeds
        """
        os.environ.setdefault("QTWEBENGINE_DISABLE_GPU", "1")
        os.environ.setdefault("QT_QUICK_BACKEND", "software")
        os.environ.setdefault("LIBGL_ALWAYS_SOFTWARE", "1")
        os.environ.setdefault(
            "QTWEBENGINE_CHROMIUM_FLAGS",
            "--disable-gpu --log-level=3"
        )
        if sys.platform.startswith("linux"):
            # Avoid loading libproxy-based GIO module in mixed snap/system setups.
            os.environ.setdefault("GIO_USE_PROXY_RESOLVER", "0")
        from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton, QSizePolicy
        from PySide6.QtWebEngineWidgets import QWebEngineView
        from PySide6.QtWebEngineCore import QWebEngineProfile
        from PySide6.QtCore import QUrl
        
        self.widget = QWidget(parent)
        self.on_login_success = on_login_success
        
        # Create layout
        layout = QVBoxLayout()
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)
        
        # Instructions
        label = QLabel("Log in to Artsobservasjoner to continue:")
        layout.addWidget(label)
        
        # Embedded browser
        self.web_view = QWebEngineView()
        self.web_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.profile = QWebEngineProfile.defaultProfile()
        self.cookie_store = self.profile.cookieStore()
        
        # Monitor cookies
        self.cookies = {}
        if required_cookies is None:
            if login_url and "www.artsobservasjoner.no" in login_url:
                required_cookies = [".ASPXAUTHNO"]
            else:
                required_cookies = ["__Host-bff", "__Host-bffC1", "__Host-bffC2"]
        self.required_cookies = required_cookies
        self._login_saved = False
        self.cookie_store.cookieAdded.connect(self._on_cookie_added)
        
        # Load the Artsobservasjoner login entry point.
        if not login_url:
            login_url = "https://mobil.artsobservasjoner.no/bff/login?returnUrl=/my-page"
        self.web_view.setUrl(QUrl(login_url))
        layout.addWidget(self.web_view)
        
        # Done button
        self.done_button = QPushButton("Done - Save Login")
        self.done_button.clicked.connect(self._on_done)
        self.done_button.setEnabled(False)  # Enable once we have cookies
        layout.addWidget(self.done_button)
        
        self.widget.setLayout(layout)
        self.widget.setWindowTitle("Log in to Artsobservasjoner")
        self.widget.setMinimumSize(700, 540)
        self.widget.resize(860, 640)
    
    def _on_cookie_added(self, cookie):
        """Called when browser receives a cookie"""
        name = bytes(cookie.name()).decode('utf-8')
        value = bytes(cookie.value()).decode('utf-8')
        domain = cookie.domain()
        
        # Store cookies from artsobservasjoner.no
        if 'artsobservasjoner.no' in domain:
            self.cookies[name] = value
            
            # Check if we have all required cookies
            if all(k in self.cookies for k in self.required_cookies):
                self.done_button.setEnabled(True)
                self.done_button.setText(f"Logged in - Click to Save ({len(self.cookies)} cookies)")
                if not self._login_saved and self.on_login_success:
                    self._login_saved = True
                    self.on_login_success(self.cookies)
                    self.widget.close()
    
    def _on_done(self):
        """User clicked done - save cookies and close"""
        if self.on_login_success:
            self.on_login_success(self.cookies)
        self.widget.close()
    
    def show(self):
        """Show the login widget"""
        self.widget.show()
        return self.widget
    
    def get_cookies(self) -> Dict[str, str]:
        """Get captured cookies"""
        return self.cookies


class ArtsObservasjonerAuth:
    """
    Unified authentication manager
    Tries multiple approaches and caches cookies
    """

    def __init__(self, cookies_file: Optional[Path] = None):
        """
        Args:
            cookies_file: Where to cache cookies (default: ~/.myco_log/artsobservasjoner_cookies.json)
        """
        if cookies_file is None:
            cookies_file = (
                Path(user_data_dir("MycoLog", appauthor=False, roaming=True))
                / "artsobservasjoner_cookies.json"
            )
        self.cookies_file = Path(cookies_file)
        stem = self.cookies_file.stem
        suffix = self.cookies_file.suffix or ".json"
        self._cookies_files = {
            "mobile": self.cookies_file.with_name(f"{stem}_mobile{suffix}"),
            "web": self.cookies_file.with_name(f"{stem}_web{suffix}"),
        }
        self._migrate_legacy_cookies()
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalize_target(target: str | None) -> str:
        key = (target or "mobile").strip().lower()
        return "web" if key == "web" else "mobile"

    def _cookies_path(self, target: str | None = None) -> Path:
        return self._cookies_files[self._normalize_target(target)]

    @staticmethod
    def _is_mobile_cookie_payload(cookies: Dict[str, str]) -> bool:
        return any(
            name in cookies for name in ("__Host-bff", "__Host-bffC1", "__Host-bffC2")
        )

    @staticmethod
    def _is_web_cookie_payload(cookies: Dict[str, str]) -> bool:
        return ".ASPXAUTHNO" in cookies

    def _load_json_cookies(self, path: Path) -> Optional[Dict[str, str]]:
        if not path.exists():
            return None
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            return None
        return data if isinstance(data, dict) else None

    def _write_json_cookies(self, path: Path, cookies: Dict[str, str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(cookies, f, indent=2)

    def _migrate_legacy_cookies(self) -> None:
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_file = Path.home() / ".myco_log" / "artsobservasjoner_cookies.json"

        for source in (legacy_file, self.cookies_file):
            cookies = self._load_json_cookies(source)
            if not cookies:
                continue

            wrote_any = False
            if self._is_mobile_cookie_payload(cookies):
                mobile_path = self._cookies_path("mobile")
                if not mobile_path.exists():
                    self._write_json_cookies(mobile_path, cookies)
                    wrote_any = True
            if self._is_web_cookie_payload(cookies):
                web_path = self._cookies_path("web")
                if not web_path.exists():
                    self._write_json_cookies(web_path, cookies)
                    wrote_any = True

            if not wrote_any:
                # Unknown legacy payload; keep compatibility by storing under mobile.
                mobile_path = self._cookies_path("mobile")
                if not mobile_path.exists():
                    self._write_json_cookies(mobile_path, cookies)

    def load_cookies(self, target: str = "mobile") -> Optional[Dict[str, str]]:
        """Load cached cookies if they exist"""
        target_path = self._cookies_path(target)
        cookies = self._load_json_cookies(target_path)
        if cookies:
            print(f"Loaded {len(cookies)} cached cookies from {target_path}")
            return cookies

        # Backwards compatibility for the old shared file.
        if self.cookies_file != target_path:
            cookies = self._load_json_cookies(self.cookies_file)
            if cookies:
                print(f"Loaded {len(cookies)} cached cookies from {self.cookies_file}")
                return cookies
        return None

    def save_cookies(self, cookies: Dict[str, str], target: str = "mobile"):
        """Save cookies to cache"""
        target_path = self._cookies_path(target)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "w") as f:
            json.dump(cookies, f, indent=2)
        print(f"Saved {len(cookies)} cookies to {target_path}")

    def clear_cookies(self, target: Optional[str] = None) -> None:
        if target:
            paths = [self._cookies_path(target)]
        else:
            paths = [self._cookies_path("mobile"), self._cookies_path("web"), self.cookies_file]
        for path in paths:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                continue

    def login_with_gui(self, callback: Optional[Callable] = None) -> Dict[str, str]:
        """
        Show PyQt login dialog (best for MycoLog)

        Args:
            callback: Function to call when login succeeds
        """

        def on_success(cookies):
            self.save_cookies(cookies, target="mobile")
            if callback:
                callback(cookies)

        auth_widget = ArtsObservasjonerAuthWidget(on_login_success=on_success)
        auth_widget.show()
        return auth_widget.get_cookies()

    def login_web_with_gui(self, parent=None, callback: Optional[Callable] = None) -> Optional[Dict[str, str]]:
        """
        Prompt for web credentials and authenticate against www.artsobservasjoner.no.

        Returns:
            Cookie dict on success, None if user cancelled.
        """
        username, password, remember_login = _prompt_web_credentials(parent=parent)
        if username is None:
            return None
        if not password:
            raise RuntimeError("Missing password.")

        web_auth = ArtsObservasjonerWebLogin()
        success = web_auth.login(
            username=username,
            password=password,
            # Always request a persistent cookie from the server so the session
            # survives days/weeks instead of the ~24 h non-persistent timeout.
            # Whether the user's credentials are saved locally is a separate
            # concern controlled by the checkbox below.
            remember_me=True,
        )
        if not success:
            raise RuntimeError("Login failed. Please check your email and password.")

        cookies = web_auth.get_cookies_dict()
        if not cookies:
            raise RuntimeError("Login succeeded but no cookies were returned.")

        if remember_login:
            try:
                _save_web_credentials(username, password)
            except Exception as exc:
                print(f"Warning: could not save web credentials securely: {exc}")
        else:
            _clear_saved_web_credentials()

        if callback:
            callback(cookies)
        else:
            self.save_cookies(cookies, target="web")
        return cookies

    def get_valid_cookies(self, target: str = "mobile") -> Optional[Dict[str, str]]:
        """
        Get valid cookies, using cache if available

        Returns None if no valid cookies found
        """
        cookies = self.load_cookies(target=target)

        if cookies and self._validate_cookies(cookies, target=target):
            target_path = self._cookies_path(target)
            if not target_path.exists():
                try:
                    self.save_cookies(cookies, target=target)
                except Exception:
                    pass
            return cookies

        return None

    def _validate_cookies(self, cookies: Dict[str, str], target: str = "mobile") -> bool:
        """
        Test if cookies are still valid
        """
        target_key = (target or "mobile").lower()
        if target_key == "web":
            return self._validate_web_cookies(cookies)
        return self._validate_mobile_cookies(cookies)

    def _validate_mobile_cookies(self, cookies: Dict[str, str]) -> bool:
        session = requests.Session()
        for name, value in cookies.items():
            session.cookies.set(name, value, domain='mobil.artsobservasjoner.no')

        try:
            # Try a simple authenticated endpoint
            response = session.get(
                'https://mobil.artsobservasjoner.no/core/Sites/ByUser/LastUsed?top=1',
                headers={'X-Csrf': '1'},
                timeout=5
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def _validate_web_cookies(self, cookies: Dict[str, str]) -> bool:
        if ".ASPXAUTHNO" not in cookies:
            return False

        session = requests.Session()
        for name, value in cookies.items():
            session.cookies.set(name, value, domain=".artsobservasjoner.no")

        try:
            response = session.get(
                "https://www.artsobservasjoner.no/User/MyPages",
                allow_redirects=True,
                timeout=8,
            )
        except requests.RequestException:
            return False

        if response.status_code != 200:
            return False
        url = (response.url or "").lower()
        return "/logon" not in url and "/account/login" not in url
