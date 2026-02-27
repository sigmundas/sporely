"""Artsobservasjoner login and cookie capture for MycoLog."""

import json
import re
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


def _prompt_web_credentials(
    parent=None,
    title: str = "Log in to Artsobservasjoner (web)",
) -> tuple[Optional[str], Optional[str], bool]:
    """Show a Qt dialog for Artsobservasjoner credentials."""
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
    dialog.setWindowTitle(title)
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


def _parse_login_form(html_text: str, base_url: str) -> Optional[dict]:
    """Parse an HTML page for a login form.

    Returns a dict with keys ``action``, ``hidden``, ``username_field``,
    ``password_field``, or *None* if no suitable form is found.
    """
    from html.parser import HTMLParser
    from urllib.parse import urljoin

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
                t = attrs_d.get("type", "text").lower()
                name = attrs_d.get("name", "")
                value = attrs_d.get("value", "")
                if t == "hidden" and name:
                    self._current["hidden"][name] = value
                elif t not in ("submit", "button", "reset", "image", "checkbox", "radio") and name:
                    self._current["fields"].append(
                        {"name": name, "type": t, "id": attrs_d.get("id", "")}
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
        (f for f in parser.forms if any(field["type"] == "password" for field in f["fields"])),
        None,
    )
    if login_form is None:
        return None

    _USERNAME_HINTS = ("user", "email", "login", "name", "identifier")
    username_field: Optional[str] = None
    password_field: Optional[str] = None
    for f in login_form["fields"]:
        if f["type"] == "password":
            password_field = f["name"]
        elif f["type"] in ("text", "email"):
            combined = (f["name"] + " " + f["id"]).lower()
            if any(hint in combined for hint in _USERNAME_HINTS):
                username_field = f["name"]
            elif username_field is None:
                username_field = f["name"]

    if not username_field or not password_field:
        return None

    action = login_form["action"]
    action = urljoin(base_url, action) if action else base_url
    return {
        "action": action,
        "hidden": login_form["hidden"],
        "username_field": username_field,
        "password_field": password_field,
    }


class ArtsObservasjonerMobileBffLogin:
    """Programmatic login to ``mobil.artsobservasjoner.no`` via BFF/OIDC.

    Follows the same redirect chain a browser would, but using
    :mod:`requests` — no embedded browser (QtWebEngine) required.
    The :class:`requests.Session` accumulates all cookies, including
    the ``__Host-bff*`` BFF session cookies set after a successful login.
    """

    MOBILE_BASE = "https://mobil.artsobservasjoner.no"
    LOGIN_URL = f"{MOBILE_BASE}/bff/login?returnUrl=%2Fmy-page"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.7",
            }
        )

    def login(self, username: str, password: str) -> bool:
        """Follow the BFF OIDC redirect chain and submit credentials.

        Raises :class:`RuntimeError` if no login form is found.
        Returns *True* if BFF session cookies were obtained.
        """
        # Step 1: follow BFF login redirect to the identity provider
        resp = self.session.get(self.LOGIN_URL, timeout=20, allow_redirects=True)
        resp.raise_for_status()

        # Step 2: parse the identity provider's login form
        form = _parse_login_form(resp.text, resp.url)
        if form is None:
            raise RuntimeError(
                "Could not find a login form on the identity provider page "
                f"({resp.url}). The page may require JavaScript or have changed."
            )

        # Step 3: build form data (CSRF / anti-forgery tokens + credentials)
        data = dict(form["hidden"])
        data[form["username_field"]] = username
        data[form["password_field"]] = password

        # Step 4: submit and follow all redirects back to the BFF
        resp2 = self.session.post(
            form["action"],
            data=data,
            headers={"Referer": resp.url},
            timeout=20,
            allow_redirects=True,
        )
        resp2.raise_for_status()

        return self.check_auth()

    def check_auth(self) -> bool:
        """Return *True* if the session holds valid BFF session cookies."""
        cookie_names = {c.name for c in self.session.cookies}
        if "__Host-bff" in cookie_names or "__Host-bffC1" in cookie_names:
            return True
        # Fallback: hit a lightweight authenticated endpoint
        try:
            r = self.session.get(
                f"{self.MOBILE_BASE}/core/Sites/ByUser/LastUsed?top=1",
                headers={"X-Csrf": "1"},
                timeout=8,
            )
            return r.status_code == 200
        except requests.RequestException:
            return False

    def get_cookies_dict(self) -> Dict[str, str]:
        return {c.name: c.value for c in self.session.cookies}


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

    def login_mobile_with_gui(
        self, parent=None, callback: Optional[Callable] = None
    ) -> Optional[Dict[str, str]]:
        """Prompt for credentials and authenticate against mobil.artsobservasjoner.no.

        Follows the BFF/OIDC redirect chain programmatically — no browser required.

        Returns:
            Cookie dict on success, None if user cancelled.
        Raises:
            RuntimeError: on login failure.
        """
        username, password, remember_login = _prompt_web_credentials(
            parent=parent,
            title="Log in to Artsobservasjoner (mobile)",
        )
        if username is None:
            return None
        if not password:
            raise RuntimeError("Missing password.")

        mobile_auth = ArtsObservasjonerMobileBffLogin()
        success = mobile_auth.login(username=username, password=password)
        if not success:
            raise RuntimeError("Login failed. Please check your email and password.")

        cookies = mobile_auth.get_cookies_dict()
        if not cookies:
            raise RuntimeError("Login succeeded but no session cookies were returned.")

        if remember_login:
            try:
                _save_web_credentials(username, password)
            except Exception as exc:
                print(f"Warning: could not save credentials: {exc}")
        else:
            _clear_saved_web_credentials()

        if callback:
            callback(cookies)
        else:
            self.save_cookies(cookies, target="mobile")
        return cookies

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
        print("[artsobs] mobile: validating cached session...")
        session = requests.Session()
        for name, value in cookies.items():
            session.cookies.set(name, value, domain='mobil.artsobservasjoner.no')

        try:
            response = session.get(
                'https://mobil.artsobservasjoner.no/core/Sites/ByUser/LastUsed?top=1',
                headers={'X-Csrf': '1'},
                timeout=5
            )
            if response.status_code == 200:
                print("[artsobs] mobile: session still valid")
                return True
            print(f"[artsobs] mobile: session invalid (HTTP {response.status_code})")
            return False
        except requests.RequestException as exc:
            print(f"[artsobs] mobile: session check failed ({exc})")
            return False

    def _validate_web_cookies(self, cookies: Dict[str, str]) -> bool:
        if ".ASPXAUTHNO" not in cookies:
            print("[artsobs] web: no .ASPXAUTHNO cookie — session missing")
            return False

        print("[artsobs] web: validating cached session...")
        session = requests.Session()
        for name, value in cookies.items():
            session.cookies.set(name, value, domain=".artsobservasjoner.no")

        try:
            response = session.get(
                "https://www.artsobservasjoner.no/User/MyPages",
                allow_redirects=True,
                timeout=8,
            )
        except requests.RequestException as exc:
            print(f"[artsobs] web: session check failed ({exc})")
            return False

        if response.status_code != 200:
            print(f"[artsobs] web: session invalid (HTTP {response.status_code})")
            return False
        url = (response.url or "").lower()
        if "/logon" in url or "/account/login" in url:
            print(f"[artsobs] web: session expired (redirected to login page)")
            return False
        print("[artsobs] web: session still valid")
        return True

    def ensure_valid_cookies(self, target: str = "mobile") -> Optional[Dict[str, str]]:
        """Get valid cookies, silently re-authenticating from saved credentials if needed.

        Returns cookies dict on success, None if login is required manually.
        """
        target_key = self._normalize_target(target)
        cookies = self.get_valid_cookies(target=target_key)
        if cookies:
            return cookies

        print(f"[artsobs] {target_key}: session expired — attempting silent re-auth")
        username, password, _ = _load_saved_web_credentials()
        if not username or not password:
            print(f"[artsobs] {target_key}: no saved credentials — manual login required")
            return None

        print(f"[artsobs] {target_key}: re-authenticating as {username}...")
        try:
            if target_key == "mobile":
                auth_obj = ArtsObservasjonerMobileBffLogin()
                ok = auth_obj.login(username=username, password=password)
            else:
                auth_obj = ArtsObservasjonerWebLogin()
                ok = auth_obj.login(username=username, password=password, remember_me=True)

            if not ok:
                print(f"[artsobs] {target_key}: silent re-auth — login rejected")
                return None

            new_cookies = auth_obj.get_cookies_dict()
            if not new_cookies:
                print(f"[artsobs] {target_key}: silent re-auth succeeded but no cookies returned")
                return None

            self.save_cookies(new_cookies, target=target_key)
            print(f"[artsobs] {target_key}: silent re-auth OK ({len(new_cookies)} cookies saved)")
            return new_cookies
        except Exception as exc:
            print(f"[artsobs] {target_key}: silent re-auth error: {exc}")
            return None

    def login_both_with_gui(self, parent=None) -> dict:
        """Single credential dialog -> authenticate both mobile and web endpoints.

        Returns {
            "mobile": cookies_dict_or_None,
            "web":    cookies_dict_or_None,
            "cancelled": bool,
        }
        """
        username, password, remember_login = _prompt_web_credentials(
            parent=parent,
            title="Log in to Artsobservasjoner",
        )
        if username is None:
            print("[artsobs] login cancelled by user")
            return {"mobile": None, "web": None, "cancelled": True}
        if not password:
            raise RuntimeError("Missing password.")

        results: dict = {"mobile": None, "web": None, "cancelled": False}

        # --- Mobile BFF ---
        print("[artsobs] unified login: authenticating mobile endpoint...")
        try:
            mobile_auth = ArtsObservasjonerMobileBffLogin()
            ok = mobile_auth.login(username=username, password=password)
            if ok:
                mobile_cookies = mobile_auth.get_cookies_dict()
                if mobile_cookies:
                    self.save_cookies(mobile_cookies, target="mobile")
                    results["mobile"] = mobile_cookies
                    print(f"[artsobs] mobile login OK ({len(mobile_cookies)} cookies saved)")
                else:
                    print("[artsobs] mobile login: no cookies returned")
            else:
                print("[artsobs] mobile login: rejected by server")
        except Exception as exc:
            print(f"[artsobs] mobile login error: {exc}")

        # --- Web ---
        print("[artsobs] unified login: authenticating web endpoint...")
        try:
            web_auth = ArtsObservasjonerWebLogin()
            ok = web_auth.login(username=username, password=password, remember_me=True)
            if ok:
                web_cookies = web_auth.get_cookies_dict()
                if web_cookies:
                    self.save_cookies(web_cookies, target="web")
                    results["web"] = web_cookies
                    print(f"[artsobs] web login OK ({len(web_cookies)} cookies saved)")
                else:
                    print("[artsobs] web login: no cookies returned")
            else:
                print("[artsobs] web login: rejected by server")
        except Exception as exc:
            print(f"[artsobs] web login error: {exc}")

        # --- Persist credentials ---
        if remember_login:
            try:
                _save_web_credentials(username, password)
                print("[artsobs] credentials saved to keyring for silent re-auth")
            except Exception as exc:
                print(f"[artsobs] warning: could not save credentials: {exc}")
        else:
            _clear_saved_web_credentials()
            print("[artsobs] credentials cleared (save-password unchecked)")

        return results
