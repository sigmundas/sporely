"""Cloudflare R2 helpers for media uploads, downloads, and key handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import os
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import quote, urlparse
from xml.sax.saxutils import escape as xml_escape

import requests


R2_BUCKET_NAME = "sporely-media"
R2_PUBLIC_BASE_URL = "https://media.sporely.no"
R2_REGION = "auto"
R2_SERVICE = "s3"

_ENV_FILE_CANDIDATES = (
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


def _load_python_env_file() -> None:
    """Load simple KEY=VALUE pairs from python.env if the variables are unset."""
    for env_path in _ENV_FILE_CANDIDATES:
        if not env_path.exists():
            continue
        try:
            lines = env_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for raw_line in lines:
            line = str(raw_line or "").strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_key = key.strip()
            if not env_key or env_key in os.environ:
                continue
            os.environ[env_key] = value.strip()
        return


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
        _load_python_env_file()
        access_key_id = str(os.environ.get("R2_ACCESS_KEY_ID") or "").strip()
        secret_access_key = str(os.environ.get("R2_SECRET_ACCESS_KEY") or "").strip()
        s3_endpoint = str(os.environ.get("R2_S3_ENDPOINT") or "").strip()
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
        timeout: int = 120,
    ) -> None:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)
        payload_hash = self._sha256_file(path)
        extra_headers = self._extra_headers(content_type=content_type, cache_control=cache_control)
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
        timeout: int = 120,
    ) -> None:
        payload = bytes(data)
        payload_hash = hashlib.sha256(payload).hexdigest()
        extra_headers = self._extra_headers(content_type=content_type, cache_control=cache_control)
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
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = str(content_type)
        if cache_control:
            headers["Cache-Control"] = str(cache_control)
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
