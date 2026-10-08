"""Cloud-sync error classes and sync-issue classification and formatting.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
import re

from utils.cloud_media_policy import WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE

from utils.cloud_sync_impl.common import _safe_int


_PUSH_CONFLICT_RE = re.compile(
    r"^obs\s+(?P<local_id>\d+):\s+skipped desktop push because the linked cloud observation changed on the web$"
)


_PULL_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+skipped remote update because local observation\s+(?P<local_id>\d+)\s+has unsynced desktop edits$"
)


_REVIEW_CONFLICT_RE = re.compile(
    r"^cloud\s+(?P<cloud_id>[^:]+):\s+needs review before applying remaining cloud changes to local observation\s+(?P<local_id>\d+)(?:\s+\((?P<reason>.*)\))?$"
)


_MEASUREMENT_CONFLICT_RE = re.compile(
    r"^obs\s+(?P<local_id>\d+):\s+skipped cloud measurement\s+"
    r"(?P<measurement_id>\S+)\s+because the local copy changed$"
)


class CloudSyncError(Exception):
    pass


class AccountMismatchError(CloudSyncError):
    pass


class CloudTemporarilyUnavailableError(CloudSyncError):
    pass


class CloudReauthRequiredError(CloudSyncError):
    """Raised when the refresh endpoint proves the refresh token is dead.

    Distinct from CloudTemporarilyUnavailableError (Supabase glitch, retry
    likely fine) and from generic CloudSyncError (transport-level noise).
    Reaching this state means the current session cannot be resumed and the
    user must sign in again — but callers still must not wipe stored tokens
    unless the user explicitly signs out.
    """


class PullOnlyModeError(CloudSyncError):
    """Raised when a cloud-write is attempted during a Download-from-Cloud run.

    Download from Cloud is strictly cloud → desktop. Any code path that
    reaches an upload, PATCH/POST/DELETE, storage removal, or write-back
    identity call while the pull-only client is active raises this error.
    The wrapper counts every attempt on ``write_attempts`` so tests can
    prove zero cloud writes reached the network.
    """


class PartialConflictPlanError(CloudSyncError):
    """Raised when a conflict plan fails mid-execution.

    Carries the partial operation log so the caller (typically the in-dialog
    apply worker) can present per-item statuses, keep the conflict visible,
    and offer a safe retry using ``prior_result`` on the next call.
    """

    def __init__(self, message: str, *, partial_result: dict):
        super().__init__(message)
        self.partial_result = dict(partial_result or {})


class ObservationIdentityConflictError(CloudSyncError):
    """Raised when observation push identity cannot be resolved safely.

    Two links tie a local observation to a cloud row: the direct link
    (local ``observations.cloud_id``) and the reverse link (remote
    ``observations.desktop_id``). This error is raised when they resolve to
    different cloud rows, or when the reverse-link recovery lookup matches
    more than one cloud row. PATCHing either candidate could overwrite the
    wrong row and POSTing would create a duplicate, so the push must fail
    and leave the observation dirty/retryable for review.
    """


class ImageIdentityConflictError(CloudSyncError):
    """Raised when image push identity is ambiguous or contradictory.

    The caller must not PATCH or POST. Leave the image dirty/retryable.
    """


class CloudSessionAccountMismatchError(AccountMismatchError):
    """Raised when a stale SporelyCloudClient sees on-disk session tokens
    that belong to a different Sporely Cloud user than the one this
    client is bound to.

    Usually happens when the user signed out and signed back in as a
    different account while an old worker/thread was still alive: the
    worker's in-memory ``user_id`` still points at the previous
    account, but the on-disk tokens now belong to the new one.  We
    refuse to adopt the new account's tokens or call the refresh
    endpoint with them — the worker should surface this and stop.
    Inherits from :class:`AccountMismatchError` so existing handlers
    that only catch that base class keep working; catch this subclass
    to distinguish "stale worker" from "database linked to a different
    account".
    """


PRIVACY_SLOT_LIMIT_USER_MESSAGE = (
    "Free accounts can have up to 20 private or fuzzed-location cloud observations. "
    "Make one public, delete one, or upgrade to Pro."
)


IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE = (
    "Image upload was rejected by the worker."
)


_PRIVACY_SLOT_LIMIT_HINTS = (
    "Free Sporely accounts",
    "20 privacy slot",
    "privacy slot observations",
)


_IMAGE_TOO_LARGE_FOR_PLAN_HINTS = (
    "image too large for plan",
    "too large for your plan",
)


_IMAGE_TOO_LARGE_FOR_PLAN_REASONS = {"byte_cap", "pixel_cap", "edge_cap", "unknown"}


_IMAGE_TOO_LARGE_FOR_PLAN_REASON_LINE_RE = re.compile(
    r"(?im)^\s*(?:Worker\s+)?Reason:\s*(?P<reason>[A-Za-z_]+)\s*$"
)


def _normalize_image_too_large_reason(reason: str | None) -> str:
    text = str(reason or "").strip().lower().replace("-", "_")
    return text if text in _IMAGE_TOO_LARGE_FOR_PLAN_REASONS else ""


def _parse_human_size(text: str | None) -> int | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    match = re.search(r"(?i)(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)\b", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2).upper()
    scale = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
    }.get(unit, 1)
    return int(value * scale)


def _parse_dimension_pair(text: str | None) -> tuple[int, int] | None:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None
    match = re.search(r"(?i)(\d+)\s*[×x]\s*(\d+)\s*px\b", cleaned)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _parse_int_text(text: str | None) -> int | None:
    cleaned = str(text or "").strip().replace(",", "")
    if not cleaned:
        return None
    match = re.search(r"(-?\d+)", cleaned)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _extract_label_value(text: str, label: str) -> str:
    pattern = re.compile(rf"(?im)^\s*{re.escape(label)}\s*:\s*(?P<value>.+?)\s*$")
    match = pattern.search(str(text or ""))
    return str(match.group("value") or "").strip() if match else ""


def _image_too_large_reason_message(reason: str | None) -> str:
    normalized = _normalize_image_too_large_reason(reason)
    if normalized == "byte_cap":
        return "Image exceeds the byte cap for this upload policy."
    if normalized == "pixel_cap":
        return "Image exceeds the pixel cap for this upload policy."
    if normalized == "edge_cap":
        return "Image exceeds the longest-edge cap for this upload policy."
    return IMAGE_TOO_LARGE_FOR_PLAN_USER_MESSAGE


def _image_too_large_summary_message(reason: str | None) -> str:
    normalized = _normalize_image_too_large_reason(reason)
    if normalized == "byte_cap":
        return "Cloud sync failed while uploading an image that exceeded the byte cap."
    if normalized == "pixel_cap":
        return "Cloud sync failed while uploading an image that exceeded the pixel cap."
    if normalized == "edge_cap":
        return "Cloud sync failed while uploading an image that exceeded the longest-edge cap."
    return "Cloud sync failed while uploading an image that was rejected by the worker."


def _infer_image_too_large_reason_from_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""

    reason_match = _IMAGE_TOO_LARGE_FOR_PLAN_REASON_LINE_RE.search(cleaned)
    if reason_match:
        normalized = _normalize_image_too_large_reason(reason_match.group("reason"))
        if normalized:
            return normalized

    worker_body_bytes = _parse_human_size(_extract_label_value(cleaned, "Worker body size"))
    worker_plan_cap = _parse_human_size(_extract_label_value(cleaned, "Worker plan cap"))
    prepared_bytes = _parse_human_size(_extract_label_value(cleaned, "Prepared upload size"))
    plan_cap = _parse_human_size(_extract_label_value(cleaned, "Plan cap"))
    if worker_body_bytes and worker_plan_cap and worker_body_bytes > worker_plan_cap:
        return "byte_cap"
    if prepared_bytes and plan_cap and prepared_bytes > plan_cap:
        return "byte_cap"

    worker_stored_pixels = _parse_int_text(_extract_label_value(cleaned, "Worker stored pixels"))
    worker_stored_pixel_cap = _parse_int_text(_extract_label_value(cleaned, "Worker stored pixel cap"))
    if worker_stored_pixels and worker_stored_pixel_cap and worker_stored_pixels > worker_stored_pixel_cap:
        return "pixel_cap"

    worker_resize_max_edge = _parse_int_text(_extract_label_value(cleaned, "Worker resize max edge"))
    stored_dimensions = _parse_dimension_pair(
        _extract_label_value(cleaned, "Worker stored dimensions") or _extract_label_value(cleaned, "Prepared dimensions")
    )
    if stored_dimensions and worker_resize_max_edge and max(stored_dimensions) > worker_resize_max_edge:
        return "edge_cap"

    return "unknown"


def infer_image_too_large_for_plan_reason(error) -> str:
    code, texts = _collect_sync_error_details(error)
    haystack = " ".join(dict.fromkeys(texts)).lower()
    if not (
        code.strip().lower() == "image_too_large_for_plan"
        or "image_too_large_for_plan" in haystack
        or "too large for your plan" in haystack
    ):
        return ""

    payload = {}
    if isinstance(error, dict):
        payload = dict(error)
    else:
        for attr in ("payload", "response_payload", "response", "body"):
            try:
                candidate = getattr(error, attr)
            except Exception:
                candidate = None
            if isinstance(candidate, dict):
                payload = dict(candidate)
                break

    details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
    reason = _normalize_image_too_large_reason(
        details.get("reason")
        or payload.get("reason")
        or details.get("errorReason")
        or payload.get("errorReason")
    )
    if reason:
        return reason

    worker_body_bytes = _safe_int(
        details.get("bodyBytes")
        or details.get("body_bytes")
        or payload.get("bodyBytes")
        or payload.get("body_bytes")
    )
    worker_plan_cap = _safe_int(
        details.get("planByteCap")
        or details.get("plan_byte_cap")
        or details.get("planCap")
        or details.get("plan_cap")
        or payload.get("planByteCap")
        or payload.get("plan_byte_cap")
        or payload.get("planCap")
        or payload.get("plan_cap")
    )
    if worker_body_bytes > 0 and worker_plan_cap > 0 and worker_body_bytes > worker_plan_cap:
        return "byte_cap"

    prepared_bytes = _safe_int(
        details.get("preparedBytes")
        or details.get("prepared_bytes")
        or payload.get("preparedBytes")
        or payload.get("prepared_bytes")
    )
    prepared_plan_cap = _safe_int(
        details.get("planCap")
        or details.get("plan_cap")
        or payload.get("planCap")
        or payload.get("plan_cap")
        or worker_plan_cap
    )
    if prepared_bytes > 0 and prepared_plan_cap > 0 and prepared_bytes > prepared_plan_cap:
        return "byte_cap"

    worker_stored_pixels = _safe_int(
        details.get("storedPixels")
        or details.get("stored_pixels")
        or payload.get("storedPixels")
        or payload.get("stored_pixels")
    )
    worker_stored_pixel_cap = _safe_int(
        details.get("storedPixelCap")
        or details.get("stored_pixel_cap")
        or payload.get("storedPixelCap")
        or payload.get("stored_pixel_cap")
    )
    if worker_stored_pixels > 0 and worker_stored_pixel_cap > 0 and worker_stored_pixels > worker_stored_pixel_cap:
        return "pixel_cap"

    worker_stored_width = _safe_int(
        details.get("storedWidth")
        or details.get("stored_width")
        or payload.get("storedWidth")
        or payload.get("stored_width")
        or details.get("preparedWidth")
        or details.get("prepared_width")
        or payload.get("preparedWidth")
        or payload.get("prepared_width")
    )
    worker_stored_height = _safe_int(
        details.get("storedHeight")
        or details.get("stored_height")
        or payload.get("storedHeight")
        or payload.get("stored_height")
        or details.get("preparedHeight")
        or details.get("prepared_height")
        or payload.get("preparedHeight")
        or payload.get("prepared_height")
    )
    worker_resize_max_edge = _safe_int(
        details.get("resizeMaxEdge")
        or details.get("resize_max_edge")
        or payload.get("resizeMaxEdge")
        or payload.get("resize_max_edge")
    )
    if worker_stored_width > 0 and worker_stored_height > 0 and worker_resize_max_edge > 0 and max(worker_stored_width, worker_stored_height) > worker_resize_max_edge:
        return "edge_cap"

    return _infer_image_too_large_reason_from_text("\n".join(dict.fromkeys(texts)))


def format_image_too_large_for_plan_reason(reason_or_error) -> str:
    if isinstance(reason_or_error, str):
        reason = _normalize_image_too_large_reason(reason_or_error)
        if not reason:
            reason = infer_image_too_large_for_plan_reason(reason_or_error)
    else:
        reason = infer_image_too_large_for_plan_reason(reason_or_error)
    return _image_too_large_reason_message(reason)


def summarize_image_too_large_for_plan_error(reason_or_error) -> str:
    if isinstance(reason_or_error, str):
        reason = _normalize_image_too_large_reason(reason_or_error)
        if not reason:
            reason = infer_image_too_large_for_plan_reason(reason_or_error)
    else:
        reason = infer_image_too_large_for_plan_reason(reason_or_error)
    return _image_too_large_summary_message(reason)


def sanitize_image_too_large_for_plan_error_message(error) -> str:
    text = str(error or "").strip()
    if not text or not is_image_too_large_for_plan_error(text):
        return text
    lines = [line.rstrip() for line in text.splitlines()]
    reason_message = format_image_too_large_for_plan_reason(text)
    if not lines:
        return reason_message
    lines[0] = reason_message
    return "\n".join(lines)


def privacy_slot_limit_user_message() -> str:
    return PRIVACY_SLOT_LIMIT_USER_MESSAGE


def _collect_sync_error_details(value, seen: set[int] | None = None) -> tuple[str, list[str]]:
    if seen is None:
        seen = set()
    try:
        marker = id(value)
    except Exception:
        marker = None
    if marker is not None and marker in seen:
        return '', []
    if marker is not None:
        seen.add(marker)

    code = ''
    texts: list[str] = []
    if value is None:
        return code, texts

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return code, texts
        texts.append(text)
        if text[:1] in {'{', '['}:
            try:
                parsed = json.loads(text)
            except Exception:
                return code, texts
            parsed_code, parsed_texts = _collect_sync_error_details(parsed, seen)
            if parsed_code and not code:
                code = parsed_code
            texts.extend(parsed_texts)
        if not code:
            lowered = text.lower()
            if '23514' in text:
                code = '23514'
            elif 'check_violation' in lowered:
                code = 'check_violation'
        return code, texts

    if isinstance(value, dict):
        for key in ('code', 'sqlstate', 'status_code', 'statusCode', 'status'):
            raw_code = value.get(key)
            if raw_code not in (None, ''):
                candidate = str(raw_code).strip()
                if candidate and not code:
                    code = candidate
        for key in ('message', 'details', 'hint', 'error', 'body', 'text', 'reason', 'response'):
            if key not in value:
                continue
            sub_code, sub_texts = _collect_sync_error_details(value.get(key), seen)
            if sub_code and not code:
                code = sub_code
            texts.extend(sub_texts)
        return code, texts

    for attr in ('code', 'sqlstate', 'status_code', 'statusCode', 'status'):
        try:
            raw_code = getattr(value, attr)
        except Exception:
            raw_code = None
        if raw_code not in (None, ''):
            candidate = str(raw_code).strip()
            if candidate and not code:
                code = candidate
    for attr in ('message', 'details', 'hint', 'error', 'body', 'text', 'reason', 'response', 'payload', 'response_payload'):
        try:
            raw_value = getattr(value, attr)
        except Exception:
            raw_value = None
        if raw_value is None:
            continue
        sub_code, sub_texts = _collect_sync_error_details(raw_value, seen)
        if sub_code and not code:
            code = sub_code
        texts.extend(sub_texts)
    for attr in ('__cause__', '__context__'):
        try:
            chained_value = getattr(value, attr)
        except Exception:
            chained_value = None
        if chained_value is None:
            continue
        sub_code, sub_texts = _collect_sync_error_details(chained_value, seen)
        if sub_code and not code:
            code = sub_code
        texts.extend(sub_texts)
    text = str(value).strip()
    if text:
        texts.append(text)
        if not code:
            lowered = text.lower()
            if '23514' in text:
                code = '23514'
            elif 'check_violation' in lowered:
                code = 'check_violation'
    return code, texts


def format_cloud_sync_error_details(error) -> str:
    code, texts = _collect_sync_error_details(error)
    parts: list[str] = []
    code_text = str(code or '').strip()
    if code_text:
        parts.append(f"code={code_text}")
    for text in dict.fromkeys(texts):
        cleaned = str(text or '').strip()
        if cleaned and cleaned not in parts:
            parts.append(cleaned)
    if not parts:
        fallback = str(error or '').strip()
        if fallback:
            parts.append(fallback)
    return " | ".join(parts)


#: Distinguishing marker in the CloudSyncError message raised by
#: `_verify_identity_clear_landed` — see `is_identity_clear_verification_failed_error`.
_IDENTITY_CLEAR_VERIFICATION_FAILED_MARKER = 'identity clear did not take effect'


def is_identity_clear_verification_failed_error(error) -> bool:
    """Whether *error* is the read-back verification failure raised when a
    rate-limit (or similar) row-suppression trigger cancelled an identity
    clear while the RPC call itself did not raise (Stage C review round 2,
    item 3)."""
    _code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    return _IDENTITY_CLEAR_VERIFICATION_FAILED_MARKER in haystack


def is_privacy_slot_limit_error(error) -> bool:
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    has_privacy_phrase = any(hint.lower() in haystack for hint in _PRIVACY_SLOT_LIMIT_HINTS)
    has_constraint_code = (
        code.strip() == '23514'
        or code.strip().lower() == 'check_violation'
        or '23514' in haystack
        or 'check_violation' in haystack
    )
    return has_privacy_phrase and has_constraint_code


def is_image_too_large_for_plan_error(error) -> bool:
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    has_phrase = any(hint in haystack for hint in _IMAGE_TOO_LARGE_FOR_PLAN_HINTS)
    has_code = (
        code.strip().lower() == 'image_too_large_for_plan'
        or 'image_too_large_for_plan' in haystack
        or 'payload_too_large' in haystack
    )
    return has_phrase or has_code


def is_webp_support_required_for_cloud_media_upload_error(error) -> bool:
    return WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE.lower() in str(error or '').lower()


class CloudImageBytesNotDesiredError(CloudSyncError):
    """Raised when a byte upload is attempted for an image the user unchecked.

    The cloud-storage-desired predicate rejects the upload at the client
    boundary. Recovery flows may opt in explicitly by passing
    ``recovery_authorized=True`` to the upload method.
    """


_SUPABASE_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


_SUPABASE_TRANSIENT_ERROR_HINTS = (
    'bad gateway',
    'connection aborted',
    'connection refused',
    'connection reset',
    'could not connect to server',
    'gateway timeout',
    'postgrest unavailable',
    'schema cache',
    'service unavailable',
    'temporarily unavailable',
    'timed out',
    'timeout',
)


_CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE = (
    'Supabase/cloud sync is temporarily unavailable; local data was not overwritten.'
)


_CLOUD_AUTH_ERROR_HINTS = (
    'jwt expired',
    'invalid jwt',
    'expired access token',
    'access token expired',
    'token expired',
    'session expired',
    'authentication failed',
    'invalid_grant',
    'not logged in',
    'unauthorized',
    'pgrst301',
    'pgrst303',
    # Supabase returns this when password login is attempted without a captcha
    # token — the user must sign in interactively (e.g. via browser).
    'captcha_failed',
)


def is_cloud_auth_error(error) -> bool:
    """Broad classification: does *error* smell like an auth/token issue?

    Used by the request layer to decide whether to try a refresh and by
    the sync loops to decide whether to abort early.  Deliberately does
    not match a raw ``403`` — PostgREST returns 403 for RLS denials,
    which are authorization (not authentication) failures and must not
    be conflated with an expired session.
    """
    if isinstance(error, CloudReauthRequiredError):
        return True
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    code_text = str(code or '').strip().lower()
    if code_text == '401':
        return True
    return any(hint in haystack for hint in _CLOUD_AUTH_ERROR_HINTS)


def is_cloud_temporary_unavailable_error(error) -> bool:
    if isinstance(error, CloudTemporarilyUnavailableError):
        return True
    code, texts = _collect_sync_error_details(error)
    haystack = ' '.join(dict.fromkeys(texts)).lower()
    code_text = str(code or '').strip().lower()
    if code_text in {'pgrst000', 'pgrst001', 'pgrst002', 'pgrst003'}:
        return True
    if code_text in {str(status) for status in _SUPABASE_TRANSIENT_STATUS_CODES}:
        return True
    if _CLOUD_TEMPORARILY_UNAVAILABLE_MESSAGE.lower() in haystack:
        return True
    return any(hint in haystack for hint in _SUPABASE_TRANSIENT_ERROR_HINTS)
