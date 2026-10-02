"""Reference measurement-content client capability (Stage M, desktop part).

sporely-web ``20261002120000_reference_client_capability_minimum.sql`` makes
the reference sync RPCs and the owner library feed capability-aware. This
module owns the desktop's declaration:

* a stable device id (uuid4, generated once, stored in the active profile's
  ``app_settings.json``, the app's existing settings mechanism);
* the declared ``reference_snapshot_versions``, derived from the reader code
  this build actually ships rather than from a constant (see
  :func:`declared_snapshot_versions`);
* the capability-hold fingerprint used to stop retrying a write the server
  refused with ``requires_newer_client`` / ``older_client_active`` until
  something that could change the answer has changed.

Device id scope: one per installed app data directory, i.e. per Sporely
profile (``app_identity.app_data_dir``), not per database file or per
account. The capability describes the installed binary's readers, which are
the same for every database or account a profile opens; the server keys
device records by ``(user_id, device_id)``, so one profile used with two
accounts is two records. A per-database id would travel with a copied
database and let two machines claim one device (the creation guard ignores
the caller's own device id). Two profiles on one machine report as two
devices; both run the same binary and declare the same versions, so this
never blocks anything.

The two measurement-content gates in ``references.measurement_content_gates``
are unrelated to this declaration and stay closed: declaring version 2 says
this desktop can *read* version-2 content, which it can with the gates closed.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

logger = logging.getLogger(__name__)

CLIENT_NAME = "desktop"
DEVICE_ID_SETTING = "reference_client_device_id"
NIL_UUID = "00000000-0000-0000-0000-000000000000"

#: Write statuses that refuse a write because of client capability. The local
#: change stays pending and is not retried until the hold fingerprint changes.
CAPABILITY_HOLD_STATUSES = frozenset({"requires_newer_client", "older_client_active"})
CAPABILITY_HOLD_PREFIX = "capability_hold"

#: The server's creation-guard window (``reference_older_client_active``).
_OLDER_CLIENT_WINDOW = timedelta(days=30)

_device_lock = threading.Lock()


def _valid_device_id(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    try:
        parsed = uuid.UUID(text)
    except (ValueError, AttributeError, TypeError):
        return None
    if str(parsed) == NIL_UUID or str(parsed) != text:
        return None
    return text


def get_reference_device_id() -> str:
    """The install's device id; generated and persisted on first use."""
    from database.schema import get_app_settings, update_app_settings

    with _device_lock:
        existing = _valid_device_id(get_app_settings().get(DEVICE_ID_SETTING))
        if existing:
            return existing
        device_id = str(uuid.uuid4())
        update_app_settings({DEVICE_ID_SETTING: device_id})
        return device_id


def declared_snapshot_versions() -> tuple[int, ...]:
    """Snapshot versions every owner-feed and public reader of this build reads.

    Version 2 is declared only when all three readers that receive v2 content
    from the capability-aware server ship it: the use-feed stager
    (``SUPPORTED_SNAPSHOT_VERSIONS``), the measurement-set feed stager (the
    extension columns) and the public/curated envelope normalizer.
    """
    from database.curated_reference_forks import _SNAPSHOT_KEYS_BY_VERSION
    from database.reference_citation import SUPPORTED_SNAPSHOT_VERSIONS
    from database.reference_sync_reconciliation import _PAYLOAD_COLUMNS

    set_fields = set(_PAYLOAD_COLUMNS.get("measurement_set", ()))
    if (
        2 in SUPPORTED_SNAPSHOT_VERSIONS
        and 2 in _SNAPSHOT_KEYS_BY_VERSION
        and {"measurement_details_json", "q_core_min", "q_core_max"} <= set_fields
    ):
        return (1, 2)
    return (1,)


def _app_version() -> str:
    from utils.cloud_sync import _current_source_app_version

    return str(_current_source_app_version() or "")[:32]


def reference_client_capabilities() -> dict[str, Any]:
    """The ``p_client_capabilities`` object sent on every reference call."""
    return {
        "reference_snapshot_versions": list(declared_snapshot_versions()),
        "device_id": get_reference_device_id(),
        "client": CLIENT_NAME,
        "app_version": _app_version(),
    }


# Capability holds -----------------------------------------------------------------

def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def capability_fingerprint(
    capabilities: dict[str, Any],
    devices: Iterable[dict[str, Any]] | None,
    *,
    now: datetime | None = None,
) -> str:
    """Digest of everything that can change a capability refusal.

    Own declaration (versions, app version, device id) plus the owner's other
    devices that currently trigger the creation guard: seen within 30 days and
    without version 2. An older device upgrading, or ageing out of the window,
    changes the digest. ``devices=None`` (read failed) yields a distinct
    ``unknown`` digest so a hold is kept rather than retried blindly.
    """
    own_device = str(capabilities.get("device_id") or "")
    if devices is None:
        blocking: object = "unknown"
    else:
        cutoff = (now or datetime.now(timezone.utc)) - _OLDER_CLIENT_WINDOW
        rows = []
        for row in devices:
            if not isinstance(row, dict):
                continue
            device_id = str(row.get("device_id") or "")
            versions = row.get("reference_snapshot_versions") or []
            seen = _parse_timestamp(row.get("last_seen_at"))
            if device_id == own_device or 2 in versions:
                continue
            if seen is None or seen <= cutoff:
                continue
            rows.append(device_id)
        blocking = sorted(rows)
    material = {
        "versions": list(capabilities.get("reference_snapshot_versions") or []),
        "app_version": capabilities.get("app_version") or "",
        "device_id": own_device,
        "blocking_devices": blocking,
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def payload_digest(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def hold_marker(status: str, fingerprint: str, payload: object) -> str:
    """``last_error`` text recording a capability hold of one local row."""
    return f"{CAPABILITY_HOLD_PREFIX}:{status}:{fingerprint}:{payload_digest(payload)}"


def parse_hold_marker(last_error: object) -> tuple[str, str, str] | None:
    parts = str(last_error or "").split(":")
    if len(parts) != 4 or parts[0] != CAPABILITY_HOLD_PREFIX:
        return None
    if parts[1] not in CAPABILITY_HOLD_STATUSES:
        return None
    return parts[1], parts[2], parts[3]


# Device report --------------------------------------------------------------------

_reported: set[tuple[str, str]] = set()
_reported_lock = threading.Lock()


def report_reference_client_capabilities(client: object) -> bool:
    """Report this device once per app session and account; never raises.

    Cheap and non-blocking for the sync: any failure (transport, rate limit,
    missing RPC) is logged and retried at the next sync.
    """
    user_id = str(getattr(client, "user_id", "") or "").strip()
    record = getattr(client, "record_reference_client_capabilities", None)
    if not user_id or not callable(record):
        return False
    try:
        capabilities = reference_client_capabilities()
        key = (user_id, json.dumps(capabilities, sort_keys=True))
        with _reported_lock:
            if key in _reported:
                return True
        response = record(capabilities)
        if not isinstance(response, dict) or response.get("status") != "recorded":
            logger.info("reference device report not recorded: %r", response)
            return False
        with _reported_lock:
            _reported.add(key)
        return True
    except Exception as exc:  # non-fatal by contract
        logger.warning("reference device report failed: %s", exc)
        return False


def _reset_reported_for_tests() -> None:
    with _reported_lock:
        _reported.clear()


def summarize_capability_holds(sync_result: object) -> dict[str, int]:
    """Count held reference changes per status in a ``sync_all`` result."""
    reference = sync_result.get("reference_sync") if isinstance(sync_result, dict) else None
    holds = reference.get("capability_holds") if isinstance(reference, dict) else None
    counts = {status: 0 for status in sorted(CAPABILITY_HOLD_STATUSES)}
    for item in holds or ():
        status = str(item).rsplit(":", 1)[-1]
        if status in counts:
            counts[status] += 1
    return counts
