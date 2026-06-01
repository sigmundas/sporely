"""Cloudflare R2 helpers for media uploads, downloads, and key handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import logging
import os
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import quote, urlparse
from xml.sax.saxutils import escape as xml_escape
import warnings

import requests


R2_BUCKET_NAME = "sporely-media"
R2_PUBLIC_BASE_URL = "https://media.sporely.no"
MEDIA_WORKER_DEFAULT_URL = "https://upload.sporely.no"
R2_REGION = "auto"
R2_SERVICE = "s3"
R2_DIRECT_ACCESS_UNAVAILABLE_MESSAGE = "Cloud media storage is not configured for direct desktop R2 access"
SPORELY_MEDIA_WORKER_URL_ENV_VAR = "SPORELY_MEDIA_WORKER_URL"
SPORELY_ENABLE_DIRECT_R2_ENV_VAR = "SPORELY_ENABLE_DIRECT_R2"
_LEGACY_ADMIN_ENV_DEPRECATION_MESSAGE = (
    "python.env is deprecated; rename it to sporely-admin.env for local admin secrets."
)

_ENV_FILE_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "sporely-admin.env",
    Path.cwd() / "sporely-admin.env",
)
_LEGACY_ENV_FILE_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "python.env",
    Path.cwd() / "python.env",
)
_SUPABASE_STORAGE_PREFIXES = (
    "storage/v1/object/authenticated/observation-images/",
    "storage/v1/object/public/observation-images/",
    "storage/v1/object/observation-images/",
    "observation-images/",
)


class R2ConfigError(RuntimeError):
    """Raised when R2 configuration is missing or invalid."""


def _load_simple_env_file(env_path: Path) -> bool:
    """Load simple KEY=VALUE pairs from one local env file if variables are unset."""
    if not env_path.exists():
        return False
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return False
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue
        os.environ[env_key] = value.strip()
    return True


_LEGACY_ADMIN_ENV_WARNING_EMITTED = False


def _warn_legacy_admin_env_file() -> None:
    global _LEGACY_ADMIN_ENV_WARNING_EMITTED
    if _LEGACY_ADMIN_ENV_WARNING_EMITTED:
        return
    _LEGACY_ADMIN_ENV_WARNING_EMITTED = True
    warnings.warn(_LEGACY_ADMIN_ENV_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=3)
    logging.getLogger(__name__).warning(_LEGACY_ADMIN_ENV_DEPRECATION_MESSAGE)


def load_admin_env_file(env_file: str | Path | None = None) -> bool:
    """Load local admin secrets from sporely-admin.env, with python.env fallback.

    Normal application runtime should not rely on this helper unless direct R2
    access has been explicitly enabled.
    """
    if env_file is not None:
        env_path = Path(env_file).expanduser()
        loaded = _load_simple_env_file(env_path)
        if loaded and env_path.name.lower() == "python.env":
            _warn_legacy_admin_env_file()
        return loaded

    for env_path in _ENV_FILE_CANDIDATES:
        if _load_simple_env_file(env_path):
            return True

    for env_path in _LEGACY_ENV_FILE_CANDIDATES:
        if _load_simple_env_file(env_path):
            _warn_legacy_admin_env_file()
            return True
    return False


def _read_r2_env_values() -> tuple[str, str, str]:
    load_admin_env_file()
    access_key_id = str(os.environ.get("R2_ACCESS_KEY_ID") or "").strip()
    secret_access_key = str(os.environ.get("R2_SECRET_ACCESS_KEY") or "").strip()
    s3_endpoint = str(os.environ.get("R2_S3_ENDPOINT") or "").strip()
    return access_key_id, secret_access_key, s3_endpoint


def r2_config_available() -> bool:
    access_key_id, secret_access_key, s3_endpoint = _read_r2_env_values()
    return bool(access_key_id and secret_access_key and s3_endpoint)


def direct_r2_runtime_enabled() -> bool:
    value = str(os.environ.get(SPORELY_ENABLE_DIRECT_R2_ENV_VAR) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def direct_r2_runtime_available() -> bool:
    if not direct_r2_runtime_enabled():
        return False
    return r2_config_available()


def media_worker_base_url() -> str:
    value = str(os.environ.get(SPORELY_MEDIA_WORKER_URL_ENV_VAR) or "").strip().rstrip("/")
    return value or MEDIA_WORKER_DEFAULT_URL


def _first_text(*values: object, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def normalize_media_key(value: str | None) -> str:
    """Return a relative media key, stripping known public/storage prefixes."""
    text = str(value or "").strip()
    if not text:
        return ""

    lowered = text.lower()
    public_base = R2_PUBLIC_BASE_URL.rstrip("/")
    if lowered.startswith(public_base.lower() + "/"):
        return text[len(public_base) + 1 :].lstrip("/")

    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc:
        path = parsed.path.lstrip("/")
        bucket_prefix = f"{R2_BUCKET_NAME}/"
        if path.startswith(bucket_prefix):
            return path[len(bucket_prefix) :].lstrip("/")
        for prefix in _SUPABASE_STORAGE_PREFIXES:
            if path.startswith(prefix):
                return path[len(prefix) :].lstrip("/")
        return path

    bucket_prefix = f"{R2_BUCKET_NAME}/"
    if text.startswith(bucket_prefix):
        return text[len(bucket_prefix) :].lstrip("/")
    for prefix in _SUPABASE_STORAGE_PREFIXES:
        if text.startswith(prefix):
            return text[len(prefix) :].lstrip("/")
    return text.lstrip("/")


def media_variant_key(key: str | None, variant: str = "original") -> str:
    """Return the media key for a thumbnail variant, preserving directory layout."""
    normalized = normalize_media_key(key)
    if not normalized or variant == "original":
        return normalized
    parts = normalized.split("/")
    file_name = parts.pop() if parts else normalized
    dir_path = "/".join(parts)
    variant_name = f"thumb_{file_name}" if variant == "thumb" else f"thumb_{variant}_{file_name}"
    return f"{dir_path}/{variant_name}" if dir_path else variant_name


@dataclass(frozen=True)
class R2Config:
    access_key_id: str
    secret_access_key: str
    s3_endpoint: str
    bucket_name: str = R2_BUCKET_NAME
    public_base_url: str = R2_PUBLIC_BASE_URL
    region: str = R2_REGION

    @classmethod
    def from_env(cls) -> "R2Config":
        access_key_id, secret_access_key, s3_endpoint = _read_r2_env_values()
        if not access_key_id or not secret_access_key or not s3_endpoint:
            raise R2ConfigError(
                "Missing R2 configuration. Expected R2_ACCESS_KEY_ID, "
                "R2_SECRET_ACCESS_KEY, and R2_S3_ENDPOINT."
            )
        return cls(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            s3_endpoint=s3_endpoint.rstrip("/"),
        )


class CloudflareR2Client:
    """Minimal S3-compatible client for Cloudflare R2 using SigV4 signing."""

    def __init__(self, config: R2Config, session: requests.Session | None = None):
        self.config = config
        self._session = session or requests.Session()

    @classmethod
    def from_env(cls) -> "CloudflareR2Client":
        return cls(R2Config.from_env())

    def public_url(self, key: str | None) -> str:
        normalized = normalize_media_key(key)
        if not normalized:
            return ""
        return f"{self.config.public_base_url.rstrip('/')}/{normalized}"

    def put_file(
        self,
        file_path: str | Path,
        key: str,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
        custom_metadata: Mapping[str, object] | None = None,
        timeout: int = 120,
    ) -> None:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)
        payload_hash = self._sha256_file(path)
        extra_headers = self._extra_headers(
            content_type=content_type,
            cache_control=cache_control,
            custom_metadata=custom_metadata,
        )
        with path.open("rb") as handle:
            response = self._request(
                "PUT",
                key=key,
                data=handle,
                payload_hash=payload_hash,
                extra_headers=extra_headers,
                timeout=timeout,
            )
        self._raise_for_status(response, "R2 upload failed")

    def put_bytes(
        self,
        data: bytes,
        key: str,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
        custom_metadata: Mapping[str, object] | None = None,
        timeout: int = 120,
    ) -> None:
        payload = bytes(data)
        payload_hash = hashlib.sha256(payload).hexdigest()
        extra_headers = self._extra_headers(
            content_type=content_type,
            cache_control=cache_control,
            custom_metadata=custom_metadata,
        )
        response = self._request(
            "PUT",
            key=key,
            data=payload,
            payload_hash=payload_hash,
            extra_headers=extra_headers,
            timeout=timeout,
        )
        self._raise_for_status(response, "R2 upload failed")

    def download_to_file(self, key: str, dest_path: str | Path, *, timeout: int = 120) -> Path:
        normalized = normalize_media_key(key)
        if not normalized:
            raise ValueError("Missing R2 media key")
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        response = self._request("GET", key=normalized, stream=True, timeout=timeout)
        self._raise_for_status(response, "R2 download failed")
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        return destination

    def delete_objects(self, keys: Iterable[str]) -> None:
        cleaned = []
        for key in keys:
            normalized = normalize_media_key(key)
            if normalized:
                cleaned.append(normalized)
        unique_keys = sorted(set(cleaned))
        if not unique_keys:
            return

        xml_body = self._delete_objects_xml(unique_keys).encode("utf-8")
        payload_hash = hashlib.sha256(xml_body).hexdigest()
        response = self._request(
            "POST",
            key="",
            query={"delete": ""},
            data=xml_body,
            payload_hash=payload_hash,
            extra_headers={"Content-Type": "application/xml"},
            timeout=120,
        )
        self._raise_for_status(response, "R2 delete failed")

    def _extra_headers(
        self,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
        custom_metadata: Mapping[str, object] | None = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = str(content_type)
        if cache_control:
            headers["Cache-Control"] = str(cache_control)
        for key, value in dict(custom_metadata or {}).items():
            meta_key = str(key or "").strip().lower().replace("_", "-")
            if not meta_key:
                continue
            headers[f"x-amz-meta-{meta_key}"] = "" if value is None else str(value)
        return headers

    def _request(
        self,
        method: str,
        *,
        key: str,
        query: Mapping[str, str] | None = None,
        data=None,
        payload_hash: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        stream: bool = False,
        timeout: int = 120,
    ) -> requests.Response:
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        canonical_uri = self._canonical_uri(key)
        canonical_query = self._canonical_query(query or {})
        payload_digest = str(payload_hash or hashlib.sha256(b"").hexdigest())

        host = urlparse(self.config.s3_endpoint).netloc
        headers = {
            "host": host,
            "x-amz-content-sha256": payload_digest,
            "x-amz-date": amz_date,
        }
        for name, value in dict(extra_headers or {}).items():
            headers[name.lower()] = str(value)

        signed_header_names = sorted(headers)
        canonical_headers = "".join(f"{name}:{self._normalize_header_value(headers[name])}\n" for name in signed_header_names)
        signed_headers = ";".join(signed_header_names)

        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers,
                payload_digest,
            ]
        )

        credential_scope = f"{date_stamp}/{self.config.region}/{R2_SERVICE}/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = self._signature(date_stamp, string_to_sign)

        request_headers = {
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={self.config.access_key_id}/{credential_scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
            "x-amz-content-sha256": payload_digest,
            "x-amz-date": amz_date,
        }
        for name, value in headers.items():
            if name == "host":
                continue
            request_headers[name] = value

        url = f"{self.config.s3_endpoint}{canonical_uri}"
        if canonical_query:
            url = f"{url}?{canonical_query}"
        return self._session.request(
            method=method.upper(),
            url=url,
            data=data,
            headers=request_headers,
            timeout=timeout,
            stream=stream,
        )

    def _canonical_uri(self, key: str) -> str:
        normalized = normalize_media_key(key)
        if normalized:
            segments = [self.config.bucket_name] + [segment for segment in normalized.split("/") if segment]
        else:
            segments = [self.config.bucket_name]
        return "/" + "/".join(quote(segment, safe="-_.~") for segment in segments)

    @staticmethod
    def _canonical_query(query: Mapping[str, str]) -> str:
        if not query:
            return ""
        items = []
        for name, value in sorted(query.items()):
            encoded_name = quote(str(name), safe="-_.~")
            encoded_value = quote(str(value), safe="-_.~")
            items.append(f"{encoded_name}={encoded_value}")
        return "&".join(items)

    @staticmethod
    def _normalize_header_value(value: str) -> str:
        return " ".join(str(value or "").strip().split())

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if chunk:
                    digest.update(chunk)
        return digest.hexdigest()

    def _signature(self, date_stamp: str, string_to_sign: str) -> str:
        def _sign(key_bytes: bytes, message: str) -> bytes:
            return hmac.new(key_bytes, message.encode("utf-8"), hashlib.sha256).digest()

        k_date = _sign(("AWS4" + self.config.secret_access_key).encode("utf-8"), date_stamp)
        k_region = _sign(k_date, self.config.region)
        k_service = _sign(k_region, R2_SERVICE)
        k_signing = _sign(k_service, "aws4_request")
        return hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    @staticmethod
    def _delete_objects_xml(keys: Iterable[str]) -> str:
        xml_keys = "".join(
            f"<Object><Key>{xml_escape(normalize_media_key(key))}</Key></Object>"
            for key in keys
            if normalize_media_key(key)
        )
        return f'<?xml version="1.0" encoding="UTF-8"?><Delete>{xml_keys}</Delete>'

    @staticmethod
    def _raise_for_status(response: requests.Response, message: str) -> None:
        if response.ok:
            return
        try:
            payload = response.text
        except Exception:
            payload = "<no response body>"
        raise RuntimeError(f"{message}: {payload}")


def _build_worker_upload_headers(
    *,
    access_token: str,
    blob_type: str | None = None,
    content_type: str | None = None,
    cache_control: str | None = None,
    upload_meta: Mapping[str, object] | None = None,
    options: Mapping[str, object] | None = None,
) -> dict[str, str]:
    meta = dict(upload_meta or {})
    opts = dict(options or {})
    normalized_blob_type = _first_text(blob_type, default="image/jpeg")
    normalized_content_type = _first_text(content_type, normalized_blob_type, default="image/jpeg")

    headers: dict[str, str] = {
        "Authorization": f"Bearer {str(access_token or '').strip()}",
        "Content-Type": normalized_content_type,
        "Cache-Control": str(cache_control or "public, max-age=31536000, immutable").strip(),
    }

    upload_variant = _first_text(
        opts.get("uploadVariant"),
        opts.get("upload_variant"),
        meta.get("upload_variant"),
        default="full",
    ).lower() or "full"
    upload_mode = _first_text(
        opts.get("uploadMode"),
        opts.get("upload_mode"),
        meta.get("upload_mode"),
        default="reduced",
    ).lower() or "reduced"
    cloud_plan = _first_text(
        opts.get("cloudPlan"),
        opts.get("cloud_plan"),
        meta.get("cloud_plan"),
        default="free",
    )
    quality_profile = _first_text(
        opts.get("qualityProfile"),
        opts.get("quality_profile"),
        meta.get("quality_profile"),
        default="standard",
    )
    encoding_quality = _first_text(
        opts.get("encodingQuality"),
        opts.get("encoding_quality"),
        meta.get("encoding_quality"),
    )
    encoding_format = _first_text(
        opts.get("encodingFormat"),
        opts.get("encoding_format"),
        meta.get("encoding_format"),
        normalized_content_type,
    )
    source_width = _first_text(
        opts.get("sourceWidth"),
        opts.get("source_width"),
        meta.get("source_width"),
    )
    source_height = _first_text(
        opts.get("sourceHeight"),
        opts.get("source_height"),
        meta.get("source_height"),
    )
    stored_width = _first_text(
        opts.get("storedWidth"),
        opts.get("stored_width"),
        meta.get("stored_width"),
    )
    stored_height = _first_text(
        opts.get("storedHeight"),
        opts.get("stored_height"),
        meta.get("stored_height"),
    )

    headers["X-Sporely-Upload-Mode"] = upload_mode
    headers["X-Sporely-Upload-Variant"] = upload_variant
    headers["X-Sporely-Cloud-Plan"] = cloud_plan
    headers["X-Sporely-Quality-Profile"] = quality_profile
    if encoding_quality:
        headers["X-Sporely-Encoding-Quality"] = encoding_quality
    if encoding_format:
        headers["X-Sporely-Encoding-Format"] = encoding_format
    if source_width:
        headers["X-Sporely-Source-Width"] = source_width
    if source_height:
        headers["X-Sporely-Source-Height"] = source_height
    if stored_width:
        headers["X-Sporely-Stored-Width"] = stored_width
    if stored_height:
        headers["X-Sporely-Stored-Height"] = stored_height
    return headers


class CloudflareMediaWorkerClient:
    """Authenticated Cloudflare Worker client for media upload/download/delete."""

    def __init__(
        self,
        access_token: str,
        *,
        base_url: str | None = None,
        public_base_url: str = R2_PUBLIC_BASE_URL,
        session: requests.Session | None = None,
    ):
        token = str(access_token or "").strip()
        if not token:
            raise ValueError("Missing access token for media worker client")
        self.access_token = token
        self.base_url = str(base_url or media_worker_base_url()).strip().rstrip("/")
        self.public_base_url = str(public_base_url or R2_PUBLIC_BASE_URL).strip().rstrip("/")
        self._session = session or requests.Session()

    @classmethod
    def from_access_token(
        cls,
        access_token: str,
        *,
        base_url: str | None = None,
        public_base_url: str = R2_PUBLIC_BASE_URL,
    ) -> "CloudflareMediaWorkerClient":
        return cls(access_token, base_url=base_url, public_base_url=public_base_url)

    def public_url(self, key: str | None) -> str:
        normalized = normalize_media_key(key)
        if not normalized:
            return ""
        return f"{self.public_base_url.rstrip('/')}/{normalized}"

    def put_file(
        self,
        file_path: str | Path,
        key: str,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
        upload_meta: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
        timeout: int = 120,
    ) -> dict:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open("rb") as handle:
            response = self._request(
                "PUT",
                key=key,
                data=handle,
                content_type=content_type or _content_type_for_path(path),
                cache_control=cache_control,
                upload_meta=upload_meta,
                options=options,
                timeout=timeout,
            )
        return self._json_response(response, "Worker upload failed")

    def put_bytes(
        self,
        data: bytes,
        key: str,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
        upload_meta: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
        timeout: int = 120,
    ) -> dict:
        payload = bytes(data)
        response = self._request(
            "PUT",
            key=key,
            data=payload,
            content_type=content_type,
            cache_control=cache_control,
            upload_meta=upload_meta,
            options=options,
            timeout=timeout,
        )
        return self._json_response(response, "Worker upload failed")

    def download_to_file(self, key: str, dest_path: str | Path, *, timeout: int = 120) -> Path:
        normalized = normalize_media_key(key)
        if not normalized:
            raise ValueError("Missing media key")
        destination = Path(dest_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        response = self._request("GET", key=normalized, timeout=timeout, stream=True)
        self._raise_for_status(response, "Worker download failed")
        content_type = str(response.headers.get("content-type") or "").strip().lower()
        if content_type and not content_type.startswith("image/") and content_type != "application/octet-stream":
            raise RuntimeError(f"Worker download returned non-image content ({content_type})")
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        return destination

    def delete_object(self, key: str, *, timeout: int = 120, ignore_missing: bool = True) -> dict | None:
        normalized = normalize_media_key(key)
        if not normalized:
            return None
        response = self._request("DELETE", key=normalized, timeout=timeout)
        if response.status_code == 404 and ignore_missing:
            return None
        return self._json_response(response, "Worker delete failed")

    def delete_objects(self, keys: Iterable[str], *, timeout: int = 120) -> None:
        for key in keys:
            try:
                self.delete_object(key, timeout=timeout, ignore_missing=True)
            except Exception as exc:
                # Missing objects are fine; only surface genuine failures.
                if "404" in str(exc):
                    continue
                raise

    def _request(
        self,
        method: str,
        *,
        key: str,
        data=None,
        content_type: str | None = None,
        cache_control: str | None = None,
        upload_meta: Mapping[str, object] | None = None,
        options: Mapping[str, object] | None = None,
        timeout: int = 120,
        stream: bool = False,
    ) -> requests.Response:
        normalized = normalize_media_key(key)
        if not normalized:
            raise ValueError("Missing media key")
        url = f"{self.base_url}/upload/{self._encode_object_key(normalized)}"
        headers = None
        if method.upper() in {"PUT", "POST"}:
            headers = _build_worker_upload_headers(
                access_token=self.access_token,
                blob_type=content_type,
                content_type=content_type,
                cache_control=cache_control,
                upload_meta=upload_meta,
                options=options,
            )
        else:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        return self._session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            data=data,
            timeout=timeout,
            stream=stream,
        )

    @staticmethod
    def _encode_object_key(storage_path: str) -> str:
        return "/".join(
            quote(segment, safe="-_.~")
            for segment in normalize_media_key(storage_path).split("/")
            if segment
        )

    @staticmethod
    def _json_response(response: requests.Response, message: str) -> dict:
        if response.ok:
            try:
                payload = response.json()
            except Exception:
                return {}
            return payload if isinstance(payload, dict) else {}
        detail = message
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = str(payload.get("message") or payload.get("error") or detail)
        except Exception:
            try:
                payload_text = response.text
            except Exception:
                payload_text = ""
            if payload_text:
                detail = payload_text
        raise RuntimeError(f"{message}: {detail}")

    @staticmethod
    def _raise_for_status(response: requests.Response, message: str) -> None:
        if response.ok:
            return
        try:
            payload = response.text
        except Exception:
            payload = "<no response body>"
        raise RuntimeError(f"{message}: {payload}")
