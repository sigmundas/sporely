"""Artportalen login and cookie capture for Sporely."""
from __future__ import annotations

import json
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from app_identity import APP_NAME, app_data_dir

_ARTPORTALEN_USERNAME_KEY = "artportalen_username"
_ARTPORTALEN_KEYRING_SERVICE = "Sporely.Artportalen"
_ARTPORTALEN_LEGACY_KEYRING_SERVICE = "MycoLog.Artportalen"
_ARTPORTALEN_KEYRING_ACCOUNT = "password"


def _get_keyring_module():
    try:
        import keyring  # type: ignore

        return keyring
    except Exception:
        return None


def _get_saved_username() -> str:
    try:
        from database.models import SettingsDB

        return (SettingsDB.get_setting(_ARTPORTALEN_USERNAME_KEY, "") or "").strip()
    except Exception:
        return ""


def _set_saved_username(username: str) -> None:
    try:
        from database.models import SettingsDB

        SettingsDB.set_setting(_ARTPORTALEN_USERNAME_KEY, username or "")
    except Exception:
        return


def _load_saved_credentials() -> tuple[str, Optional[str], bool]:
    username = _get_saved_username()
    keyring = _get_keyring_module()
    if keyring is None:
        return username, None, False
    try:
        password = keyring.get_password(_ARTPORTALEN_KEYRING_SERVICE, _ARTPORTALEN_KEYRING_ACCOUNT)
        if password is None:
            password = keyring.get_password(
                _ARTPORTALEN_LEGACY_KEYRING_SERVICE,
                _ARTPORTALEN_KEYRING_ACCOUNT,
            )
    except Exception:
        return username, None, False
    return username, password, True


def has_saved_login() -> bool:
    """Return True when saved Artportalen credentials are available."""
    username, password, _ = _load_saved_credentials()
    return bool(username and password)


def _save_credentials(username: str, password: str) -> None:
    _set_saved_username(username)
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure password storage is unavailable on this system.")
    try:
        keyring.set_password(
            _ARTPORTALEN_KEYRING_SERVICE,
            _ARTPORTALEN_KEYRING_ACCOUNT,
            password,
        )
    except Exception as exc:
        raise RuntimeError(f"Could not securely save password: {exc}") from exc


def _clear_saved_credentials() -> None:
    _set_saved_username("")
    keyring = _get_keyring_module()
    if keyring is None:
        return
    for service_name in (_ARTPORTALEN_KEYRING_SERVICE, _ARTPORTALEN_LEGACY_KEYRING_SERVICE):
        try:
            keyring.delete_password(service_name, _ARTPORTALEN_KEYRING_ACCOUNT)
        except Exception:
            continue


def _prompt_credentials(parent=None, title: str = "Log in to Artportalen") -> tuple[Optional[str], Optional[str], bool]:
    from PySide6.QtWidgets import (
        QCheckBox,
        QDialog,
        QDialogButtonBox,
        QFormLayout,
        QLabel,
        QLineEdit,
        QMessageBox,
        QVBoxLayout,
    )

    saved_username, saved_password, can_store_password = _load_saved_credentials()
    has_saved_password = bool(saved_password)
    password_edited = False
    submitted_password: Optional[str] = None
    remember_login = False

    dialog = QDialog(parent)
    dialog.setWindowTitle(title)
    dialog.setModal(True)
    dialog.setMinimumWidth(420)

    layout = QVBoxLayout(dialog)
    layout.addWidget(QLabel("Enter your Artportalen email and password:"))

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


def _parse_login_form(html_text: str, base_url: str) -> Optional[dict]:
    """Parse an HTML login form and return action, hidden fields, and user/pass fields."""

    class _FormParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.forms: list[dict] = []
            self._current: Optional[dict] = None

        def handle_starttag(self, tag, attrs):
            attrs_d = dict(attrs)
            if tag == "form":
                self._current = {"action": attrs_d.get("action", ""), "hidden": {}, "fields": []}
                self.forms.append(self._current)
            elif tag == "input" and self._current is not None:
                input_type = attrs_d.get("type", "text").lower()
                name = attrs_d.get("name", "")
                value = attrs_d.get("value", "")
                if input_type == "hidden" and name:
                    self._current["hidden"][name] = value
                elif input_type not in {"submit", "button", "reset", "image", "checkbox", "radio"} and name:
                    self._current["fields"].append(
                        {"name": name, "type": input_type, "id": attrs_d.get("id", "")}
                    )

        def handle_endtag(self, tag):
            if tag == "form":
                self._current = None

    parser = _FormParser()
    try:
        parser.feed(html_text)
    except Exception:
        return None

    login_form = next(
        (form for form in parser.forms if any(field["type"] == "password" for field in form["fields"])),
        None,
    )
    if login_form is None:
        return None

    username_field: Optional[str] = None
    password_field: Optional[str] = None
    username_hints = ("user", "email", "login", "name", "identifier")

    for field in login_form["fields"]:
        if field["type"] == "password":
            password_field = field["name"]
        elif field["type"] in {"text", "email"}:
            combined = (field["name"] + " " + field["id"]).lower()
            if any(hint in combined for hint in username_hints):
                username_field = field["name"]
            elif username_field is None:
                username_field = field["name"]

    if not username_field or not password_field:
        return None

    action = str(login_form["action"] or "").strip()
    resolved_action = urljoin(base_url, action) if action else base_url
    parsed_base = urlparse(base_url)
    base_origin = f"{parsed_base.scheme}://{parsed_base.netloc}" if parsed_base.scheme and parsed_base.netloc else ""
    if (
        base_origin
        and "useradmin-auth.slu.se" in parsed_base.netloc.lower()
        and action
        and not action.startswith(("http://", "https://", "/"))
    ):
        lowered_action = action.lower()
        if "usercredentials" in lowered_action:
            resolved_action = urljoin(base_origin + "/", action)
        elif parsed_base.path.lower().startswith("/account/login"):
            resolved_action = urljoin(base_origin + "/", action)
    return {
        "action": resolved_action,
        "hidden": login_form["hidden"],
        "username_field": username_field,
        "password_field": password_field,
    }


def _parse_hidden_form(html_text: str, base_url: str) -> Optional[dict]:
    """Parse a hidden-input form, used for the OIDC form_post callback step."""

    class _HiddenFormParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.forms: list[dict] = []
            self._current: Optional[dict] = None

        def handle_starttag(self, tag, attrs):
            attrs_d = dict(attrs)
            if tag == "form":
                self._current = {"action": attrs_d.get("action", ""), "hidden": {}}
                self.forms.append(self._current)
            elif tag == "input" and self._current is not None:
                input_type = attrs_d.get("type", "text").lower()
                name = attrs_d.get("name", "")
                value = attrs_d.get("value", "")
                if name and input_type == "hidden":
                    self._current["hidden"][name] = value

        def handle_endtag(self, tag):
            if tag == "form":
                self._current = None

    parser = _HiddenFormParser()
    try:
        parser.feed(html_text)
    except Exception:
        return None

    for form in parser.forms:
        hidden = form.get("hidden") or {}
        action = urljoin(base_url, form.get("action", "") or base_url)
        if {"access_token", "id_token", "state"} <= set(hidden.keys()):
            return {"action": action, "hidden": hidden}
    return None


def _find_login_link(html_text: str, base_url: str) -> Optional[str]:
    """Find a likely login link on a public Artportalen page."""

    class _LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links: list[dict] = []
            self._current_href: Optional[str] = None
            self._current_text_parts: list[str] = []

        def handle_starttag(self, tag, attrs):
            if tag != "a":
                return
            attrs_d = dict(attrs)
            self._current_href = attrs_d.get("href")
            self._current_text_parts = []

        def handle_data(self, data):
            if self._current_href is not None:
                self._current_text_parts.append(data or "")

        def handle_endtag(self, tag):
            if tag != "a" or self._current_href is None:
                return
            text = " ".join(part.strip() for part in self._current_text_parts if part.strip()).strip()
            self.links.append({"href": self._current_href, "text": text})
            self._current_href = None
            self._current_text_parts = []

    parser = _LinkParser()
    try:
        parser.feed(html_text)
    except Exception:
        return None

    preferred_texts = {"logga in", "log in", "login"}
    for link in parser.links:
        href = str(link.get("href") or "").strip()
        text = str(link.get("text") or "").strip().lower()
        if not href:
            continue
        href_lower = href.lower()
        if text in preferred_texts or "useradmin" in href_lower or "/logon" in href_lower or "/account/login" in href_lower:
            return urljoin(base_url, href)
    return None


class ArtportalenLogin:
    """Handles programmatic login to Artportalen via SLU's auth service."""

    BASE_URL = "https://www.artportalen.se"
    START_URL = f"{BASE_URL}/"
    REPORT_URL = f"{BASE_URL}/SubmitSighting/Report"
    MY_PAGES_URL = f"{BASE_URL}/MinaSidor"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) "
                    "Gecko/20100101 Firefox/148.0"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def _request(self, method: str, url: str, **kwargs):
        kwargs.setdefault("timeout", 20)
        kwargs.setdefault("allow_redirects", True)
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def login(self, username: str, password: str) -> bool:
        login_page = self._request("GET", self.REPORT_URL)
        if self.check_auth():
            return True

        form = _parse_login_form(login_page.text or "", login_page.url or self.START_URL)
        if form is None:
            login_link = _find_login_link(login_page.text or "", login_page.url or self.START_URL)
            if login_link:
                login_page = self._request("GET", login_link)
                form = _parse_login_form(login_page.text or "", login_page.url or login_link)
        if form is None:
            raise RuntimeError(
                "Could not find the Artportalen login form. The SLU login page may have changed."
            )

        login_data = dict(form["hidden"])
        login_data[form["username_field"]] = username
        login_data[form["password_field"]] = password

        post_login = self._request(
            "POST",
            form["action"],
            data=login_data,
            headers={
                "Referer": login_page.url or self.START_URL,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        if self.check_auth():
            return True

        callback_form = _parse_hidden_form(post_login.text or "", post_login.url or form["action"])
        if callback_form is None:
            lowered = (post_login.text or "").lower()
            if "invalid" in lowered or "incorrect" in lowered or "wrong password" in lowered:
                return False
            if "/account/login" in (post_login.url or "").lower():
                return False
            raise RuntimeError(
                "Artportalen login did not return the expected authentication callback form."
            )

        parsed_referer = urlparse(post_login.url or form["action"])
        origin = f"{parsed_referer.scheme}://{parsed_referer.netloc}" if parsed_referer.scheme and parsed_referer.netloc else "https://useradmin-auth.slu.se"
        self._request(
            "POST",
            callback_form["action"],
            data=callback_form["hidden"],
            headers={
                "Referer": post_login.url or form["action"],
                "Origin": origin,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        return self.check_auth()

    def check_auth(self) -> bool:
        if ".ASPXAUTH" not in requests.utils.dict_from_cookiejar(self.session.cookies):
            return False
        try:
            response = self._request("GET", self.REPORT_URL)
        except requests.RequestException:
            return False
        final_url = (response.url or "").lower()
        if response.status_code != 200:
            return False
        if "useradmin-auth.slu.se" in final_url or "/account/login" in final_url:
            return False
        return "submitsighting" in final_url or "requestverificationtoken" in (response.text or "").lower()

    def get_cookies_dict(self) -> Dict[str, str]:
        return requests.utils.dict_from_cookiejar(self.session.cookies)


class ArtportalenAuth:
    BASE_URL = "https://www.artportalen.se"
    LOGIN_URL = f"{BASE_URL}/"
    REPORT_URL = f"{BASE_URL}/SubmitSighting/Report"

    def __init__(self, cookies_file: Optional[Path] = None):
        if cookies_file is None:
            cookies_file = app_data_dir() / "artportalen_cookies.json"
        self.cookies_file = Path(cookies_file)
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)

    def load_cookies(self) -> Optional[Dict[str, str]]:
        if not self.cookies_file.exists():
            return None
        try:
            with self.cookies_file.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return None
        return data if isinstance(data, dict) else None

    def save_cookies(self, cookies: Dict[str, str]) -> None:
        with self.cookies_file.open("w", encoding="utf-8") as handle:
            json.dump(cookies, handle, indent=2)

    def clear_cookies(self) -> None:
        try:
            if self.cookies_file.exists():
                self.cookies_file.unlink()
        except Exception:
            return

    def _session_from_cookies(self, cookies: Dict[str, str]) -> requests.Session:
        session = requests.Session()
        for name, value in (cookies or {}).items():
            if not str(name).strip() or value is None:
                continue
            session.cookies.set(str(name), str(value), domain=".artportalen.se")
        return session

    def validate_cookies(self, cookies: Dict[str, str]) -> bool:
        if not cookies or ".ASPXAUTH" not in cookies:
            return False
        session = self._session_from_cookies(cookies)
        try:
            response = session.get(self.REPORT_URL, allow_redirects=True, timeout=12)
        except requests.RequestException:
            return False
        if response.status_code != 200:
            return False
        final_url = str(getattr(response, "url", "") or "").lower()
        if "useradmin-auth.slu.se" in final_url or "/account/login" in final_url:
            return False
        return "submitsighting" in final_url or "requestverificationtoken" in (response.text or "").lower()

    def get_valid_cookies(self) -> Optional[Dict[str, str]]:
        cookies = self.load_cookies()
        if cookies and self.validate_cookies(cookies):
            return cookies
        return None

    def ensure_valid_cookies(self) -> Optional[Dict[str, str]]:
        cookies = self.get_valid_cookies()
        if cookies:
            return cookies

        username, password, _ = _load_saved_credentials()
        if not username or not password:
            return None

        try:
            auth_obj = ArtportalenLogin()
            ok = auth_obj.login(username=username, password=password)
        except Exception:
            return None
        if not ok:
            return None

        new_cookies = auth_obj.get_cookies_dict()
        if not new_cookies:
            return None
        self.save_cookies(new_cookies)
        return new_cookies

    def login_with_gui(self, parent=None, callback: Optional[Callable] = None) -> Optional[Dict[str, str]]:
        username, password, remember_login = _prompt_credentials(parent=parent)
        if username is None:
            return None
        if not password:
            raise RuntimeError("Missing password.")

        auth_obj = ArtportalenLogin()
        success = auth_obj.login(username=username, password=password)
        if not success:
            raise RuntimeError("Login failed. Please check your email and password.")

        cookies = auth_obj.get_cookies_dict()
        if not cookies:
            raise RuntimeError("Login succeeded but no cookies were returned.")

        if remember_login:
            try:
                _save_credentials(username, password)
            except Exception as exc:
                print(f"Warning: could not save Artportalen credentials securely: {exc}")
        else:
            _clear_saved_credentials()

        if callback:
            callback(cookies)
        else:
            self.save_cookies(cookies)
        return cookies

    @staticmethod
    def _parse_cookie_text(raw_text: str) -> Dict[str, str]:
        text = (raw_text or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            return {
                str(key).strip(): str(value).strip()
                for key, value in parsed.items()
                if str(key).strip() and str(value).strip()
            }

        cookies: Dict[str, str] = {}
        for part in text.split(";"):
            chunk = part.strip()
            if not chunk or "=" not in chunk:
                continue
            name, value = chunk.split("=", 1)
            name = name.strip()
            value = value.strip()
            if name and value:
                cookies[name] = value
        return cookies

    def import_cookies_with_gui(self, parent=None) -> Optional[Dict[str, str]]:
        from PySide6.QtCore import QUrl
        from PySide6.QtGui import QDesktopServices
        from PySide6.QtWidgets import (
            QDialog,
            QDialogButtonBox,
            QLabel,
            QMessageBox,
            QPushButton,
            QTextEdit,
            QVBoxLayout,
        )

        dialog = QDialog(parent)
        dialog.setWindowTitle("Import Artportalen session cookies")
        dialog.setModal(True)
        dialog.setMinimumWidth(520)

        layout = QVBoxLayout(dialog)
        layout.addWidget(
            QLabel(
                "1. Open Artportalen in your browser and log in.\n"
                "2. Copy the Cookie request header from a logged-in request to artportalen.se\n"
                "   or paste a JSON object with cookie names and values.\n"
                f"3. Paste it below so {APP_NAME} can reuse the session."
            )
        )

        open_btn = QPushButton("Open Artportalen login page")
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(self.LOGIN_URL)))
        layout.addWidget(open_btn)

        text_edit = QTextEdit()
        text_edit.setPlaceholderText('Cookie header or JSON, e.g. {"CookieName": "value"}')
        layout.addWidget(text_edit, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        layout.addWidget(buttons)

        def _accept() -> None:
            cookies = self._parse_cookie_text(text_edit.toPlainText())
            if not cookies:
                QMessageBox.warning(
                    dialog,
                    "Missing Cookies",
                    "Paste a Cookie header value or a JSON cookie object.",
                )
                return
            if not self.validate_cookies(cookies):
                QMessageBox.warning(
                    dialog,
                    "Login Failed",
                    "Those cookies did not validate against Artportalen. "
                    "Log in in your browser first, then copy a fresh session cookie header.",
                )
                return
            self.save_cookies(cookies)
            dialog.accept()

        buttons.accepted.connect(_accept)
        buttons.rejected.connect(dialog.reject)

        if dialog.exec() != QDialog.Accepted:
            return None
        return self.load_cookies()
