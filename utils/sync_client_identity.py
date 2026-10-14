"""Desktop sync-identity request headers and server refusal answers.

The desktop sends ``x-sporely-sync-identity: 2`` and
``x-sporely-client-version: <app version>`` on every Data API and
media-worker request. The server's write stop keys only on these headers
(sporely-web migration ``20261014120000_sync_client_write_block.sql``), so a
request without them is never affected.

Kept free of cloud-sync and Qt imports: both the Supabase client and the
media-worker client read it.
"""
from __future__ import annotations

from typing import Mapping

SYNC_IDENTITY_HEADER = "x-sporely-sync-identity"
SYNC_IDENTITY_VERSION = "2"
CLIENT_VERSION_HEADER = "x-sporely-client-version"
UNKNOWN_CLIENT_VERSION = "unknown"

# Refusal hints. Both arrive as P0001 (HTTP 400), never 42501/403.
UPGRADE_REQUIRED_HINT = "sporely_upgrade_required"
SYNC_IDENTITY_REFUSED_HINT = "sporely_sync_identity_refused"
UPLOAD_REFUSAL_HINTS = frozenset({UPGRADE_REQUIRED_HINT, SYNC_IDENTITY_REFUSED_HINT})

# Media-worker refusal: HTTP 426 with this header naming the hint.
MEDIA_ERROR_CODE_HEADER = "X-Sporely-Error-Code"
MEDIA_UPGRADE_REQUIRED_STATUS = 426

# Upload-block reasons recorded on a sync result and in app settings.
UPLOAD_BLOCK_UPGRADE_REQUIRED = "upgrade_required"
UPLOAD_BLOCK_CLOUD_NOT_READY = "cloud_not_ready"
UPLOAD_BLOCK_REASONS = frozenset({UPLOAD_BLOCK_UPGRADE_REQUIRED, UPLOAD_BLOCK_CLOUD_NOT_READY})
# App setting holding the reason of the last normal sync's upload block ("" =
# uploads ran). It keeps the banner up across restarts until uploads run again.
UPLOAD_BLOCK_SETTING = "cloud_upload_blocked_reason"

UPGRADE_REQUIRED_MESSAGE = (
    "This version of Sporely can no longer upload. Please install the latest version."
)
CLOUD_NOT_READY_MESSAGE = (
    "Sporely Cloud is not ready for this version yet. Uploads are paused; "
    "downloads still work."
)

_client_version: str | None = None


def set_client_version(version: str | None) -> None:
    """Record the running app version sent in ``x-sporely-client-version``."""
    global _client_version
    text = str(version or "").strip()
    _client_version = text or None


def client_version() -> str | None:
    return _client_version


def sync_identity_headers() -> dict[str, str]:
    """The two headers every desktop Data API and media-worker request carries."""
    return {
        SYNC_IDENTITY_HEADER: SYNC_IDENTITY_VERSION,
        CLIENT_VERSION_HEADER: _client_version or UNKNOWN_CLIENT_VERSION,
    }


def media_response_requires_upgrade(status_code: object, headers: Mapping[str, str] | None) -> bool:
    """True for the media worker's ``426`` + ``sporely_upgrade_required`` answer."""
    try:
        status = int(status_code or 0)
    except (TypeError, ValueError):
        return False
    if status != MEDIA_UPGRADE_REQUIRED_STATUS:
        return False
    try:
        code = (headers or {}).get(MEDIA_ERROR_CODE_HEADER)
    except Exception:
        code = None
    return str(code or "").strip() == UPGRADE_REQUIRED_HINT


def upload_block_message(reason: str | None) -> str:
    if reason == UPLOAD_BLOCK_CLOUD_NOT_READY:
        return CLOUD_NOT_READY_MESSAGE
    if reason == UPLOAD_BLOCK_UPGRADE_REQUIRED:
        return UPGRADE_REQUIRED_MESSAGE
    return ""
