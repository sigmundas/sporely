#!/usr/bin/env python3
"""One-GET acquisition of the Dyntaxa Darwin Core Archive (refresh route).

Dyntaxa is an unversioned rolling export; the version label of an
acquisition is its fetch date (``YYYY-MM-DD``) and the pin is the archive
SHA-256. The archive endpoint SLU registered with GBIF (dataset
de8934f4-a136-481c-a87a-b0b202b80a31) embeds an API subscription key. The key
is public in the GBIF registry record but it is SLU's credential, so it is
read from the environment variable ``SPORELY_DYNTAXA_SUBSCRIPTION_KEY`` and
never written: every record and message carries the redacted endpoint
``ENDPOINT_REDACTED``, and any error text is scrubbed of the key before it is
raised or recorded.

Requires a committed approval record ``sources/dyntaxa/<version>/
acquisition-approval.json`` (approved, one GET, no redirects/retries, ceiling
at most ``MAXIMUM_BYTES``). ``--execute`` is the per-invocation authorization;
there is one HTTP GET, no redirects, no retries, and an existing archive or
manifest is never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

SOURCE_CODE = "dyntaxa"
PROFILE_CODE = "dyntaxa_dwca"
KEY_ENV = "SPORELY_DYNTAXA_SUBSCRIPTION_KEY"
BASE_ENDPOINT = "https://api.artdatabanken.se/taxonservice/v1/DarwinCore/Download"
ENDPOINT_REDACTED = BASE_ENDPOINT + "?subscription-key=<redacted; from " + KEY_ENV + ">"
MAXIMUM_BYTES = 64 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024
TIMEOUT_SECONDS = 180
USER_AGENT = "Sporely-Taxonomy-Acquisition/1 (Dyntaxa refresh)"
ZIP_TYPES = {"application/zip", "application/octet-stream", "application/x-zip-compressed"}
REQUIRED_MEMBERS = ("Taxon.csv", "VernacularName.csv", "meta.xml", "eml.xml")
VERSION_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
APPROVAL_FILENAME = "acquisition-approval.json"
REQUIRED_APPROVAL = {
    "approval_status": "approved",
    "download_authorized": True,
    "source_code": SOURCE_CODE,
    "approved_canonical_endpoint": ENDPOINT_REDACTED,
    "permitted_get_attempts": 1,
    "redirects_authorized": False,
    "retries_authorized": False,
}


class DyntaxaAcquisitionError(RuntimeError):
    pass


@dataclass
class Response:
    status: int
    headers: Mapping[str, str]
    stream: BinaryIO
    redirected: bool = False


class Transport(Protocol):
    def open(self, url: str) -> Response: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTPTransport:
    """urllib adapter: one GET, no redirects, no ambient proxies."""

    def __init__(self) -> None:
        self._used = False

    def open(self, url: str) -> Response:
        if self._used:
            raise DyntaxaAcquisitionError("transport permits exactly one GET")
        self._used = True
        request = Request(url, method="GET", headers={
            "Accept": "application/zip, application/octet-stream",
            "Accept-Encoding": "identity", "User-Agent": USER_AGENT})
        try:
            response = build_opener(ProxyHandler({}), _NoRedirect()).open(request, timeout=TIMEOUT_SECONDS)
        except HTTPError as exc:
            raise DyntaxaAcquisitionError(f"archive GET returned HTTP {exc.code}") from None
        except (URLError, OSError) as exc:
            raise DyntaxaAcquisitionError(f"archive GET failed: {type(exc).__name__}") from None
        return Response(status=response.status, headers=dict(response.headers.items()), stream=response,
                        redirected=response.geturl() != url)


def redact(text: str, key: str) -> str:
    return text.replace(key, "<redacted>") if key else text


def endpoint_with_key(key: str) -> str:
    return BASE_ENDPOINT + "?" + urlencode({"subscription-key": key})


def require_approval(release_dir: Path, version: str) -> dict[str, Any]:
    path = release_dir / APPROVAL_FILENAME
    if not path.is_file():
        raise DyntaxaAcquisitionError(f"missing acquisition approval {path}")
    approval = json.loads(path.read_text(encoding="utf-8"))
    for key, value in {**REQUIRED_APPROVAL, "version": version}.items():
        if approval.get(key) != value:
            raise DyntaxaAcquisitionError(f"acquisition approval {key} must be {value!r}")
    maximum = approval.get("approved_maximum_bytes")
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 0 < maximum <= MAXIMUM_BYTES:
        raise DyntaxaAcquisitionError("acquisition approval maximum bytes is invalid")
    for key in ("approved_by", "approved_at", "scope"):
        if not str(approval.get(key, "")).strip():
            raise DyntaxaAcquisitionError(f"acquisition approval {key} is required")
    return approval


def inspect_archive(path: Path) -> dict[str, Any]:
    """Non-extracting check: required members present, member hashes, EML pubDate."""
    with zipfile.ZipFile(path) as archive:
        names = {info.filename: info for info in archive.infolist()}
        missing = [name for name in REQUIRED_MEMBERS if name not in names]
        if missing:
            raise DyntaxaAcquisitionError(f"archive lacks required members {missing}")
        members = []
        for name in sorted(names):
            digest = hashlib.sha256()
            with archive.open(name) as handle:
                for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
                    digest.update(chunk)
            members.append({"name": name, "bytes": names[name].file_size, "sha256": digest.hexdigest()})
        eml = archive.read("eml.xml").decode("utf-8", errors="replace")
    match = re.search(r"<pubDate>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", eml)
    return {"members": members, "eml_pub_date": match.group(1) if match else None}


def acquire(release_dir: Path, archive_path: Path, version: str, key: str,
            transport: Transport | None = None,
            clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    if not VERSION_RE.match(version):
        raise DyntaxaAcquisitionError("version must be the fetch date YYYY-MM-DD")
    if not key or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", key):
        raise DyntaxaAcquisitionError(f"{KEY_ENV} is unset or not a plain token")
    approval = require_approval(release_dir, version)
    manifest_path = release_dir / "manifest.json"
    if manifest_path.exists() or archive_path.exists():
        raise DyntaxaAcquisitionError("manifest or archive already exists; acquisitions are immutable")
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    part = archive_path.with_name(archive_path.name + ".part")
    if part.exists():
        raise DyntaxaAcquisitionError(f"stale partial download {part}; inspect it manually")
    started = clock()
    try:
        response = (transport or HTTPTransport()).open(endpoint_with_key(key))
        if response.redirected:
            raise DyntaxaAcquisitionError("archive GET was redirected")
        if response.status != 200:
            raise DyntaxaAcquisitionError(f"archive GET requires HTTP 200, observed {response.status}")
        headers = {k.casefold(): v for k, v in response.headers.items()}
        media = headers.get("content-type", "").split(";", 1)[0].strip().casefold()
        if media not in ZIP_TYPES:
            raise DyntaxaAcquisitionError(f"archive GET Content-Type is unsupported: {media!r}")
        declared = headers.get("content-length")
        if declared is not None and int(declared) > approval["approved_maximum_bytes"]:
            raise DyntaxaAcquisitionError("declared Content-Length exceeds the approved ceiling")
        digest, received = hashlib.sha256(), 0
        with part.open("xb") as out:
            for chunk in iter(lambda: response.stream.read(CHUNK_BYTES), b""):
                received += len(chunk)
                if received > approval["approved_maximum_bytes"]:
                    raise DyntaxaAcquisitionError("archive stream exceeds the approved ceiling")
                out.write(chunk)
                digest.update(chunk)
        if declared is not None and int(declared) != received:
            raise DyntaxaAcquisitionError("received bytes differ from Content-Length")
        inspection = inspect_archive(part)
    except BaseException as exc:
        if part.exists():
            part.unlink()
        message = redact(str(exc), key)
        raise DyntaxaAcquisitionError(message) from None
    os.link(part, archive_path)
    part.unlink()
    sha = digest.hexdigest()
    manifest = {
        "manifest_schema_version": 1,
        "source_code": SOURCE_CODE,
        "profile_code": PROFILE_CODE,
        "state": "acquired",
        "approval_file": APPROVAL_FILENAME,
        "release": {"version": version, "version_basis": "fetch date (unversioned rolling export)",
                    "issued_date": inspection["eml_pub_date"] or version,
                    "issued_date_basis": "eml.xml dataset/pubDate of the fetched archive"
                    if inspection["eml_pub_date"] else "fetch date; eml.xml has no pubDate"},
        "download": {
            "endpoint": ENDPOINT_REDACTED,
            "fetched_at": started.isoformat().replace("+00:00", "Z"),
            "get_attempts": 1,
            "http_status": 200,
            "bytes": received,
            "sha256": sha,
            "archive_sha256": sha,
            "content_type": media,
            "members": inspection["members"],
            "local_path_note": "gitignored; the SHA-256 above is the pin, wherever the bytes live",
        },
        "licence": {"identifier": "CC0 1.0",
                    "uri": "http://creativecommons.org/publicdomain/zero/1.0/legalcode"},
    }
    text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if key in text:  # defence in depth: never write the credential
        raise DyntaxaAcquisitionError("refusing to write a manifest containing the subscription key")
    manifest_path.write_text(text, encoding="utf-8")
    return manifest


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="authorize one live GET")
    parser.add_argument("--version", required=True, help="fetch date YYYY-MM-DD")
    parser.add_argument("--taxonomy-root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="taxonomy tree holding sources/dyntaxa/<version>/ (a scratch mirror keeps "
                             "the archive out of the repository)")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.execute:
        print("Refusing to run without --execute (one GET per invocation).", file=sys.stderr)
        return 2
    release_dir = args.taxonomy_root / "sources" / SOURCE_CODE / args.version
    key = os.environ.get(KEY_ENV, "")
    try:
        manifest = acquire(release_dir, release_dir / "archive.zip", args.version, key)
    except DyntaxaAcquisitionError as exc:
        print(f"error: {redact(str(exc), key)}", file=sys.stderr)
        return 2
    print(json.dumps({"state": manifest["state"], "sha256": manifest["download"]["sha256"],
                      "bytes": manifest["download"]["bytes"], "release": manifest["release"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
