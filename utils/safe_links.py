"""Validation of links that come from remote services before they are opened or shown."""
from __future__ import annotations

from urllib.parse import urlsplit

from app_identity import APP_REPOSITORY_URL

# Hosts that already appear in the code: utils/artsdatabanken_link.py (artsdatabanken.no,
# nortaxa.artsdatabanken.no), ui/observation_details_dialog.py (www.inaturalist.org).
AI_LINK_HOSTS = frozenset(
    {
        "artsdatabanken.no",
        "www.artsdatabanken.no",
        "nortaxa.artsdatabanken.no",
        "artskart.artsdatabanken.no",
        "inaturalist.org",
        "www.inaturalist.org",
    }
)

ARTSDATABANKEN_FALLBACK_URL = "https://artsdatabanken.no"


def _checked(url, hosts, path_prefix: str = "") -> str | None:
    if not isinstance(url, str):
        return None
    value = url.strip()
    if not value or any(ch.isspace() or ch in "\"'<>\\" or ord(ch) < 32 for ch in value):
        return None
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError:
        return None
    if parts.scheme.lower() != "https" or parts.username or parts.password:
        return None
    if port not in (None, 443):
        return None
    if (parts.hostname or "").lower() not in hosts:
        return None
    # Dot segments and encoded separators could climb out of the approved path
    # once the server or Qt normalises the URL.
    lowered = parts.path.lower()
    if any(seg in (".", "..") for seg in parts.path.split("/")):
        return None
    if any(token in lowered for token in ("%2e", "%2f", "%5c", "%00")):
        return None
    if path_prefix:
        path = parts.path
        if path != path_prefix and not path.startswith(path_prefix + "/"):
            return None
    return value


def safe_ai_link(url) -> str | None:
    """Return ``url`` if it is an https link on an Artsdatabanken or iNaturalist host."""
    return _checked(url, AI_LINK_HOSTS)


def safe_release_link(url, fallback: str | None = None) -> str:
    """Return ``url`` if it points into the repository's releases, else the fixed releases page."""
    default = fallback or f"{APP_REPOSITORY_URL}/releases/latest"
    repo = urlsplit(APP_REPOSITORY_URL)
    checked = _checked(url, {(repo.hostname or "").lower()}, f"{repo.path}/releases")
    return checked or default


MAX_AVATAR_BYTES = 5 * 1024 * 1024


def safe_avatar_url(url) -> str | None:
    """Return ``url`` if it is an https storage URL on the Supabase host.

    Avatars are uploaded by the desktop (``upload_profile_avatar``) and by the
    web app (``getPublicUrl`` on the ``avatars`` bucket); both land on the
    Supabase project host under ``/storage/v1/``.
    """
    from utils.cloud_sync import SUPABASE_URL

    host = (urlsplit(SUPABASE_URL).hostname or "").lower()
    if not host:
        return None
    return _checked(url, {host}, "/storage/v1")
