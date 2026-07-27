#!/usr/bin/env python3
"""Offline, fixture-first NorTaxa DwC-A request and structural validation."""

from __future__ import annotations

import argparse
import csv
import json
import re
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import parse_qsl, urlparse
from xml.etree import ElementTree

from acquisition_profiles import get_profile
from refresh_col_xr import (
    AcquisitionError,
    ImmutableReleaseError,
    _reject_secrets,
    _safe_zip_members,
    _write_json_atomic,
    git_commit,
    sha256_file,
    sha256_json,
)

SOURCE_CODE = "nortaxa"
PROFILE_CODE = "nortaxa_dwca"
PROFILE = get_profile(PROFILE_CODE)
REQUEST_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
OFFICIAL_HOST = "ipt.artsdatabanken.no"
DEFAULT_SOURCES_ROOT = Path(__file__).resolve().parents[1] / "sources" / SOURCE_CODE
DEFAULT_PROPOSAL = Path(__file__).resolve().parents[1] / "nortaxa-source-selection.proposal.json"
TAXON_ROW_TYPE = "http://rs.tdwg.org/dwc/terms/Taxon"
VERNACULAR_ROW_TYPE = "http://rs.gbif.org/terms/1.0/VernacularName"
DISTRIBUTION_ROW_TYPE = "http://rs.tdwg.org/dwc/terms/Distribution"
# The current pinned NorTaxa DwC-A declares its Distribution extension using
# the GBIF namespace below. Both are Distribution-extension identities: they
# are structurally validated (safe path, ZIP member present, no traversal) and
# then ignored — Distribution data is not imported or interpreted here.
DISTRIBUTION_ROW_TYPE_GBIF = "http://rs.gbif.org/terms/1.0/Distribution"
DISTRIBUTION_ROW_TYPES = frozenset({DISTRIBUTION_ROW_TYPE, DISTRIBUTION_ROW_TYPE_GBIF})
SUPPORTED_ENCODINGS = {"utf-8": "utf-8", "utf8": "utf-8"}
SUPPORTED_DELIMITERS = {"\\t": "\t", "\\,": ",", ",": ",", "\t": "\t"}
SUPPORTED_LINE_TERMINATORS = {"\\n": b"\n", "\\r\\n": b"\r\n", "\\r": b"\r"}
MAX_PROPOSED_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_MEMBERS = 100
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 128 * 1024 * 1024
MAX_COMPRESSION_RATIO = 200
MAX_META_BYTES = 512 * 1024
MAX_LINE_BYTES = 1024 * 1024
MAX_FIELD_BYTES = 256 * 1024

COLUMN_REQUIRED = {
    "taxonID", "acceptedNameUsageID", "parentNameUsageID", "scientificName",
    "taxonRank", "taxonomicStatus", "kingdom", "family", "genus", "specificEpithet",
}
VERNACULAR_REQUIRED = {"vernacularName", "language"}
ALWAYS_REQUIRED_VALUES = {"taxonID", "scientificName", "taxonRank", "taxonomicStatus", "kingdom"}


def _local_name(term: str) -> str:
    return term.rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def _safe_location(value: str) -> str:
    if not value or "\\" in value:
        raise AcquisitionError("DwC-A location is empty or uses backslashes")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or any(part in {"", "."} for part in path.parts):
        raise AcquisitionError(f"unsafe DwC-A location: {value!r}")
    return value


def _endpoint(value: str, suffix: str, *, host: str, resource_key: str, version: str) -> str:
    parsed = urlparse(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise AcquisitionError("endpoint port is invalid") from exc
    if (
        parsed.scheme != "https" or parsed.hostname != host
        or parsed.username or parsed.password or port not in {None, 443}
    ):
        raise AcquisitionError("endpoint must be credential-free HTTPS on the allowed IPT host")
    if parsed.fragment:
        raise AcquisitionError("endpoint fragments are forbidden")
    if parsed.path != suffix:
        raise AcquisitionError(f"endpoint must use {suffix}")
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise AcquisitionError("duplicate endpoint query keys are forbidden")
    query = dict(pairs)
    if set(query) != {"r", "v"} or query["r"] != resource_key:
        raise AcquisitionError("endpoint must pin resource and exact version")
    if not query["v"] or query["v"].casefold() == "latest" or query["v"] != version:
        raise AcquisitionError("floating or missing IPT version is forbidden")
    return value


@dataclass(frozen=True)
class SourceProposal:
    raw: dict[str, Any]
    canonical_sha256: str
    source_code: str
    profile_code: str
    resource_key: str
    title: str
    version: str
    issued_date: str
    release_type: str
    archive_format: str
    archive_endpoint: str
    eml_endpoint: str
    resource_page: str
    dataset_uuid: str
    expected_license: str
    proposed_maximum_bytes: int


def validate_proposal(raw: dict[str, Any]) -> SourceProposal:
    if not isinstance(raw, dict):
        raise AcquisitionError("source-selection proposal must be an object")
    _reject_secrets(raw)
    if raw.get("approval_status") != "proposed" or raw.get("download_authorized") is not False:
        raise AcquisitionError("source-selection artifact must remain proposed and unauthorized")
    if raw.get("source_code") != SOURCE_CODE or raw.get("profile_code") != PROFILE_CODE:
        raise AcquisitionError("source-selection profile mismatch")
    if raw.get("network_values_status") != "proposed_unverified":
        raise AcquisitionError("network-derived proposal values must remain proposed/unverified")
    strings = {
        key: str(raw.get(key, "")).strip()
        for key in (
            "resource_key", "title", "version", "issued_date", "release_type",
            "archive_format", "dataset_uuid", "expected_license",
        )
    }
    if not all(strings.values()):
        raise AcquisitionError("source-selection proposal has empty required values")
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", strings["version"]):
        raise AcquisitionError("IPT version must be explicit and filesystem-safe")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", strings["issued_date"]):
        raise AcquisitionError("issued date must be YYYY-MM-DD")
    delivery = raw.get("delivery")
    if not isinstance(delivery, dict) or delivery.get("allowed_hosts") != [OFFICIAL_HOST]:
        raise AcquisitionError("proposal must declare the single allowed IPT host")
    archive_policy = raw.get("archive_policy")
    maximum = archive_policy.get("proposed_maximum_bytes") if isinstance(archive_policy, dict) else None
    if isinstance(maximum, bool) or not isinstance(maximum, int) or not 0 < maximum <= MAX_PROPOSED_ARCHIVE_BYTES:
        raise AcquisitionError("proposed archive ceiling is invalid")
    counts = raw.get("published_counts")
    if (
        not isinstance(counts, dict) or set(counts) != {"Taxon", "VernacularName"}
        or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in counts.values())
        or not str(raw.get("published_update_frequency", "")).strip()
    ):
        raise AcquisitionError("published proposal metadata is invalid")
    endpoints = {
        "archive_endpoint": _endpoint(
            str(delivery.get("archive_endpoint", "")), "/archive.do",
            host=OFFICIAL_HOST, resource_key=strings["resource_key"], version=strings["version"],
        ),
        "eml_endpoint": _endpoint(
            str(delivery.get("eml_endpoint", "")), "/eml.do",
            host=OFFICIAL_HOST, resource_key=strings["resource_key"], version=strings["version"],
        ),
        "resource_page": _endpoint(
            str(delivery.get("resource_page", "")), "/resource",
            host=OFFICIAL_HOST, resource_key=strings["resource_key"], version=strings["version"],
        ),
    }
    return SourceProposal(
        raw=raw, canonical_sha256=sha256_json(raw), source_code=SOURCE_CODE,
        profile_code=PROFILE_CODE, proposed_maximum_bytes=maximum, **strings, **endpoints,
    )


def load_proposal(path: Path = DEFAULT_PROPOSAL) -> SourceProposal:
    try:
        return validate_proposal(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"invalid source-selection proposal {path}: {exc}") from exc


@dataclass(frozen=True)
class NorTaxaRequest:
    source_selection_proposal_sha256: str
    resource_key: str
    title: str
    version: str
    issued_date: str
    release_type: str
    archive_format: str
    archive_endpoint: str
    eml_endpoint: str
    resource_page: str
    dataset_uuid: str
    expected_license: str
    required_row_types: tuple[str, ...]
    compiler_consumed_terms: tuple[str, ...]
    proposed_maximum_bytes: int
    source_code: str = SOURCE_CODE
    profile_code: str = PROFILE_CODE
    request_schema_version: int = REQUEST_SCHEMA_VERSION

    @classmethod
    def from_dict(cls, raw: dict[str, Any], proposal: SourceProposal) -> "NorTaxaRequest":
        if not isinstance(raw, dict):
            raise AcquisitionError("request must be an object")
        _reject_secrets(raw)
        if raw.get("approval_status") != "proposed" or raw.get("download_authorized") is not False:
            raise AcquisitionError("NorTaxa fixture request must remain explicitly proposed and unauthorized")
        expected = {
            "request_schema_version": REQUEST_SCHEMA_VERSION,
            "source_selection_proposal_sha256": proposal.canonical_sha256,
            "source_code": proposal.source_code, "profile_code": proposal.profile_code,
            "resource_key": proposal.resource_key, "version": proposal.version,
            "issued_date": proposal.issued_date, "release_type": proposal.release_type,
            "archive_format": proposal.archive_format, "title": proposal.title,
            "dataset_uuid": proposal.dataset_uuid, "expected_license": proposal.expected_license,
            "archive_endpoint": proposal.archive_endpoint, "eml_endpoint": proposal.eml_endpoint,
            "resource_page": proposal.resource_page,
            "proposed_maximum_bytes": proposal.proposed_maximum_bytes,
        }
        for key, value in expected.items():
            if raw.get(key) != value:
                raise AcquisitionError(f"request {key} must be {value!r}")
        version = str(raw["version"])
        archive_endpoint = proposal.archive_endpoint
        eml_endpoint = proposal.eml_endpoint
        resource_page = proposal.resource_page
        maximum = raw.get("proposed_maximum_bytes")
        rows = raw.get("required_row_types")
        terms = raw.get("compiler_consumed_terms")
        if not isinstance(rows, list) or set(rows) != {"Taxon", "VernacularName"}:
            raise AcquisitionError("required row types must be Taxon and VernacularName")
        if not isinstance(terms, list) or not terms or not all(isinstance(v, str) and v for v in terms):
            raise AcquisitionError("compiler-consumed terms must be a non-empty string list")
        return cls(
            source_selection_proposal_sha256=proposal.canonical_sha256,
            resource_key=raw["resource_key"], title=str(raw.get("title", "")).strip(),
            version=version, issued_date=raw["issued_date"], release_type=raw["release_type"],
            archive_format=raw["archive_format"], archive_endpoint=archive_endpoint,
            eml_endpoint=eml_endpoint, resource_page=resource_page,
            dataset_uuid=str(raw.get("dataset_uuid", "")).strip(),
            expected_license=str(raw.get("expected_license", "")).strip(),
            required_row_types=tuple(sorted(rows)), compiler_consumed_terms=tuple(sorted(set(terms))),
            proposed_maximum_bytes=maximum,
        )

    def identity(self) -> dict[str, Any]:
        return {
            "request_schema_version": self.request_schema_version,
            "source_selection_proposal_sha256": self.source_selection_proposal_sha256,
            "source_code": self.source_code,
            "profile_code": self.profile_code,
            "resource_key": self.resource_key,
            "title": self.title,
            "version": self.version,
            "issued_date": self.issued_date,
            "release_type": self.release_type,
            "archive_format": self.archive_format,
            "archive_endpoint": self.archive_endpoint,
            "eml_endpoint": self.eml_endpoint,
            "resource_page": self.resource_page,
            "dataset_uuid": self.dataset_uuid,
            "expected_license": self.expected_license,
            "required_row_types": list(self.required_row_types),
            "compiler_consumed_terms": list(self.compiler_consumed_terms),
            "proposed_maximum_bytes": self.proposed_maximum_bytes,
        }

    @property
    def request_sha256(self) -> str:
        return sha256_json(self.identity())


def load_request(path: Path, proposal_path: Path = DEFAULT_PROPOSAL) -> NorTaxaRequest:
    try:
        return NorTaxaRequest.from_dict(
            json.loads(path.read_text(encoding="utf-8")),
            load_proposal(proposal_path),
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"invalid request file {path}: {exc}") from exc


def release_dir(root: Path, request: NorTaxaRequest) -> Path:
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", request.version):
        raise AcquisitionError("unsafe version path")
    resolved = root.resolve()
    candidate = (resolved / request.version).resolve()
    if candidate.parent != resolved:
        raise AcquisitionError("release path escapes source root")
    return candidate


def planned_manifest(request: NorTaxaRequest) -> dict[str, Any]:
    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "source_code": SOURCE_CODE,
        "profile_code": PROFILE_CODE,
        "state": "planned",
        "approval_status": "proposed",
        "download_authorized": False,
        "source_selection_proposal_sha256": request.source_selection_proposal_sha256,
        "request_definition_sha256": request.request_sha256,
        "release": {"version": request.version, "issued_date": request.issued_date},
        "download": None,
        "validation": None,
        "execution_attempts": [],
    }


def plan(request: NorTaxaRequest, root: Path = DEFAULT_SOURCES_ROOT) -> tuple[Path, bool]:
    target = release_dir(root, request)
    request_path, manifest_path = target / "request.json", target / "manifest.json"
    if target.exists():
        if not request_path.is_file() or not manifest_path.is_file():
            raise ImmutableReleaseError("release directory exists without complete managed state")
        existing = json.loads(request_path.read_text(encoding="utf-8"))
        if (
            existing.get("canonical_request_sha256") != request.request_sha256
            or existing.get("source_selection_proposal_sha256")
            != request.source_selection_proposal_sha256
        ):
            raise ImmutableReleaseError("release directory belongs to a different immutable request")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("request_definition_sha256") != request.request_sha256
            or manifest.get("source_selection_proposal_sha256")
            != request.source_selection_proposal_sha256
        ):
            raise ImmutableReleaseError("manifest proposal/request identity mismatch")
        return target, True
    target.mkdir(parents=True)
    _write_json_atomic(request_path, {
        **request.identity(), "approval_status": "proposed", "download_authorized": False,
        "canonical_request_sha256": request.request_sha256,
        "execution": {"tool_git_commit": git_commit()},
    })
    _write_json_atomic(manifest_path, planned_manifest(request))
    return target, False


@dataclass(frozen=True)
class Table:
    kind: str
    row_type: str
    location: str
    encoding: str
    delimiter: str
    line_terminator: bytes
    line_terminator_declared: str
    quotechar: str | None
    ignore_header_lines: int
    link_index: int
    terms: dict[str, int]
    term_uris: dict[str, str]


def _int_attr(element: ElementTree.Element, name: str) -> int:
    try:
        value = int(element.attrib[name])
    except (KeyError, ValueError) as exc:
        raise AcquisitionError(f"missing or invalid {name} index") from exc
    if value < 0:
        raise AcquisitionError(f"{name} index must be non-negative")
    return value


def _table(element: ElementTree.Element, kind: str) -> Table:
    row_type = element.attrib.get("rowType", "")
    files = [child for child in element if child.tag.rsplit("}", 1)[-1] == "files"]
    locations = [
        node.text.strip() for files_node in files for node in files_node
        if node.tag.rsplit("}", 1)[-1] == "location" and node.text and node.text.strip()
    ]
    if len(locations) != 1:
        raise AcquisitionError("each core/extension must declare exactly one location")
    encoding_raw = element.attrib.get("encoding", "UTF-8").casefold()
    if encoding_raw not in SUPPORTED_ENCODINGS:
        raise AcquisitionError(f"unsupported encoding: {encoding_raw}")
    delimiter_raw = element.attrib.get("fieldsTerminatedBy", "\\t")
    if delimiter_raw not in SUPPORTED_DELIMITERS:
        raise AcquisitionError(f"unsupported delimiter: {delimiter_raw!r}")
    line_raw = element.attrib.get("linesTerminatedBy", "\\n")
    if line_raw not in SUPPORTED_LINE_TERMINATORS:
        raise AcquisitionError(f"unsupported line terminator: {line_raw!r}")
    quote_raw = element.attrib.get("fieldsEnclosedBy", "")
    quotechar = None if quote_raw == "" else quote_raw
    if quotechar is not None and len(quotechar) != 1:
        raise AcquisitionError("quote character must be empty or one character")
    try:
        ignored = int(element.attrib.get("ignoreHeaderLines", "0"))
    except ValueError as exc:
        raise AcquisitionError("ignoreHeaderLines must be an integer") from exc
    if ignored < 0 or ignored > 10:
        raise AcquisitionError("ignoreHeaderLines is outside the fixture policy")
    link_name = "id" if kind == "core" else "coreid"
    links = [node for node in element if node.tag.rsplit("}", 1)[-1] == link_name]
    if len(links) != 1:
        raise AcquisitionError(f"{kind} must contain exactly one <{link_name}>")
    link_index = _int_attr(links[0], "index")
    terms: dict[str, int] = {}
    uris: dict[str, str] = {}
    used_indexes = {link_index}
    for field in (node for node in element if node.tag.rsplit("}", 1)[-1] == "field"):
        index = _int_attr(field, "index")
        if index in used_indexes:
            raise AcquisitionError("duplicate field/link index")
        used_indexes.add(index)
        uri = field.attrib.get("term", "").strip()
        if not uri:
            raise AcquisitionError("field term URI is required")
        local = _local_name(uri)
        if local in terms:
            raise AcquisitionError(f"ambiguous duplicate term mapping: {local}")
        terms[local], uris[local] = index, uri
    return Table(
        kind, row_type, _safe_location(locations[0]), SUPPORTED_ENCODINGS[encoding_raw],
        SUPPORTED_DELIMITERS[delimiter_raw], SUPPORTED_LINE_TERMINATORS[line_raw], line_raw,
        quotechar, ignored, link_index, terms, uris,
    )


def parse_meta(raw: bytes) -> tuple[Table, list[Table]]:
    if len(raw) > MAX_META_BYTES:
        raise AcquisitionError("meta.xml exceeds bounded size")
    head = raw.upper()
    if any(token in head for token in (b"<!DOCTYPE", b"<!ENTITY", b"SYSTEM", b"PUBLIC")):
        raise AcquisitionError("DOCTYPE, ENTITY, and external-resource declarations are forbidden")
    try:
        root = ElementTree.fromstring(raw)
    except ElementTree.ParseError as exc:
        raise AcquisitionError(f"malformed meta.xml: {exc}") from exc
    if root.tag.rsplit("}", 1)[-1] != "archive":
        raise AcquisitionError("meta.xml root must be archive")
    cores = [node for node in root if node.tag.rsplit("}", 1)[-1] == "core"]
    if len(cores) != 1:
        raise AcquisitionError("meta.xml must declare exactly one core")
    core = _table(cores[0], "core")
    extensions = [_table(node, "extension") for node in root if node.tag.rsplit("}", 1)[-1] == "extension"]
    if core.row_type != TAXON_ROW_TYPE:
        raise AcquisitionError("core row type must be Taxon")
    vernacular = [table for table in extensions if table.row_type == VERNACULAR_ROW_TYPE]
    if len(vernacular) != 1:
        raise AcquisitionError("exactly one VernacularName extension is required")
    allowed_extension_types = {VERNACULAR_ROW_TYPE, *DISTRIBUTION_ROW_TYPES}
    unknown = [table.row_type for table in extensions if table.row_type not in allowed_extension_types]
    if unknown:
        raise AcquisitionError(f"unsupported extension row type: {unknown[0]}")
    # Policy: at most one Distribution extension. Multiple Distribution extensions
    # (in either namespace, in any combination) are rejected rather than silently
    # aggregated, so the archive's Distribution provenance stays unambiguous.
    distribution = [table for table in extensions if table.row_type in DISTRIBUTION_ROW_TYPES]
    if len(distribution) > 1:
        raise AcquisitionError(
            "at most one Distribution extension is permitted; the meta.xml declared "
            f"{len(distribution)} Distribution extensions"
        )
    missing_core = COLUMN_REQUIRED - set(core.terms)
    missing_vernacular = VERNACULAR_REQUIRED - set(vernacular[0].terms)
    if missing_core:
        raise AcquisitionError(f"Taxon core lacks required terms: {sorted(missing_core)}")
    if missing_vernacular:
        raise AcquisitionError(f"VernacularName lacks required terms: {sorted(missing_vernacular)}")
    return core, extensions


def _bounded_records(stream: Any, terminator: bytes):
    buffer = bytearray()
    while True:
        chunk = stream.read(64 * 1024)
        if not chunk:
            break
        buffer.extend(chunk)
        if len(buffer) > MAX_LINE_BYTES and terminator not in buffer:
            raise AcquisitionError("row exceeds maximum size")
        while True:
            position = buffer.find(terminator)
            if position < 0:
                break
            record = bytes(buffer[:position])
            del buffer[:position + len(terminator)]
            if b"\r" in record or b"\n" in record:
                raise AcquisitionError("table bytes disagree with declared linesTerminatedBy")
            if len(record) > MAX_LINE_BYTES:
                raise AcquisitionError("row exceeds maximum size")
            yield record
    if buffer:
        if b"\r" in buffer or b"\n" in buffer:
            raise AcquisitionError("table bytes disagree with declared linesTerminatedBy")
        if len(buffer) > MAX_LINE_BYTES:
            raise AcquisitionError("row exceeds maximum size")
        yield bytes(buffer)


def _rows(archive: zipfile.ZipFile, table: Table):
    try:
        stream = archive.open(table.location)
    except KeyError as exc:
        raise AcquisitionError(f"declared DwC-A file is missing: {table.location}") from exc
    declared_width = max([table.link_index, *table.terms.values()]) + 1
    physical_width: int | None = None
    record_number = 0
    try:
        with stream:
            for raw in _bounded_records(stream, table.line_terminator):
                record_number += 1
                if record_number <= table.ignore_header_lines:
                    continue
                try:
                    text = raw.decode(table.encoding, errors="strict")
                except UnicodeDecodeError as exc:
                    raise AcquisitionError(f"invalid {table.encoding} in {table.location}") from exc
                try:
                    row = next(csv.reader(
                        [text], delimiter=table.delimiter, quotechar=table.quotechar or '"',
                        quoting=csv.QUOTE_MINIMAL if table.quotechar else csv.QUOTE_NONE,
                        strict=True,
                    ))
                except csv.Error as exc:
                    raise AcquisitionError(f"malformed delimited row in {table.location}: {exc}") from exc
                if physical_width is None:
                    physical_width = len(row)
                    if physical_width < declared_width:
                        raise AcquisitionError(f"declared index is absent in {table.location}")
                elif len(row) != physical_width:
                    raise AcquisitionError(f"inconsistent physical row width in {table.location}")
                if any(len(value.encode(table.encoding)) > MAX_FIELD_BYTES for value in row):
                    raise AcquisitionError("field exceeds maximum size")
                yield row
    except RuntimeError as exc:
        raise AcquisitionError(f"failed streaming {table.location}: {exc}") from exc
    if record_number < table.ignore_header_lines:
        raise AcquisitionError("declared ignored header count exceeds table rows")


def _taxon_semantics(row: list[str], table: Table) -> None:
    value = lambda term: row[table.terms[term]].strip()
    for term in ALWAYS_REQUIRED_VALUES:
        if not value(term):
            raise AcquisitionError(f"required Taxon value is empty: {term}")
    rank = value("taxonRank").casefold()
    status = value("taxonomicStatus").casefold()
    taxon_id = value("taxonID")
    accepted_id = value("acceptedNameUsageID")
    parent_id = value("parentNameUsageID")
    if rank in {"species", "subspecies", "variety", "form"}:
        for term in ("genus", "specificEpithet"):
            if not value(term):
                raise AcquisitionError(f"{rank} Taxon row requires {term}")
    elif rank == "genus" and not value("genus"):
        raise AcquisitionError("genus Taxon row requires genus")
    is_synonym = "synonym" in status or status in {"misapplied", "misapplied name"}
    if is_synonym and not accepted_id:
        raise AcquisitionError("synonym Taxon row requires acceptedNameUsageID")
    if is_synonym and accepted_id == taxon_id:
        raise AcquisitionError("synonym acceptedNameUsageID cannot equal its taxonID")
    if parent_id and parent_id == taxon_id:
        raise AcquisitionError("Taxon parentNameUsageID cannot equal its taxonID")


def validate_fixture(path: Path, request: NorTaxaRequest) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size == 0 or path.stat().st_size > request.proposed_maximum_bytes:
        raise AcquisitionError("fixture archive is missing, empty, or exceeds the proposed ceiling")
    try:
        with zipfile.ZipFile(path) as archive:
            members = _safe_zip_members(archive)
            if len(members) > MAX_MEMBERS:
                raise AcquisitionError("ZIP member count exceeds policy")
            total = 0
            names: dict[str, str] = {}
            for member in members:
                if member.flag_bits & 1 or member.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise AcquisitionError("encrypted or unsupported ZIP member")
                if member.file_size > MAX_MEMBER_BYTES:
                    raise AcquisitionError("ZIP member exceeds size ceiling")
                total += member.file_size
                if member.file_size and member.compress_size == 0:
                    raise AcquisitionError("invalid ZIP compression size")
                if member.compress_size and member.file_size / member.compress_size > MAX_COMPRESSION_RATIO:
                    raise AcquisitionError("ZIP compression ratio exceeds policy")
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise AcquisitionError("ZIP symlinks are forbidden")
                folded = member.filename.casefold()
                if folded in names:
                    raise AcquisitionError("case-insensitive duplicate ZIP member")
                names[folded] = member.filename
            if total > MAX_TOTAL_BYTES:
                raise AcquisitionError("ZIP total expanded size exceeds policy")
            meta_names = [name for folded, name in names.items() if PurePosixPath(folded).name == "meta.xml"]
            if len(meta_names) != 1 or meta_names[0] != "meta.xml":
                raise AcquisitionError("archive requires exactly one safe root meta.xml")
            bad = archive.testzip()
            if bad:
                raise AcquisitionError(f"ZIP CRC failed at {bad}")
            meta_info = archive.getinfo("meta.xml")
            if meta_info.file_size > MAX_META_BYTES:
                raise AcquisitionError("meta.xml exceeds bounded size")
            core, extensions = parse_meta(archive.read("meta.xml"))
            declared = [core, *extensions]
            if len({table.location.casefold() for table in declared}) != len(declared):
                raise AcquisitionError("multiple tables resolve to the same location")
            for table in declared:
                if table.location.casefold() not in names:
                    raise AcquisitionError(f"declared DwC-A file is missing: {table.location}")
            core_ids: set[str] = set()
            taxon_ids: set[str] = set()
            usage_references: set[tuple[str, str]] = set()
            identifier_samples: list[dict[str, str]] = []
            core_count = 0
            for row in _rows(archive, core):
                core_count += 1
                row_id = row[core.link_index].strip()
                if not row_id or row_id in core_ids:
                    raise AcquisitionError("missing or duplicate core row ID")
                core_ids.add(row_id)
                _taxon_semantics(row, core)
                taxon_id = row[core.terms["taxonID"]].strip()
                if taxon_id in taxon_ids:
                    raise AcquisitionError("duplicate dwc:taxonID")
                taxon_ids.add(taxon_id)
                for term in ("acceptedNameUsageID", "parentNameUsageID"):
                    reference = row[core.terms[term]].strip()
                    if reference:
                        usage_references.add((term, reference))
                if len(identifier_samples) < 10:
                    sample = {
                        "core_row_id": row_id,
                        "dwc:taxonID": row[core.terms["taxonID"]],
                        "dwc:acceptedNameUsageID": row[core.terms["acceptedNameUsageID"]],
                        "dwc:parentNameUsageID": row[core.terms["parentNameUsageID"]],
                    }
                    if "scientificNameID" in core.terms:
                        sample["dwc:scientificNameID"] = row[core.terms["scientificNameID"]]
                    identifier_samples.append(sample)
            if not core_count:
                raise AcquisitionError("Taxon core contains no data rows")
            for term, reference in usage_references:
                if reference not in taxon_ids:
                    raise AcquisitionError(f"{term} references an unknown dwc:taxonID")
            counts = {"Taxon": core_count}
            links: dict[str, int] = {}
            vernacular = next(table for table in extensions if table.row_type == VERNACULAR_ROW_TYPE)
            vernacular_count = 0
            for row in _rows(archive, vernacular):
                vernacular_count += 1
                core_id = row[vernacular.link_index]
                if not core_id or core_id not in core_ids:
                    raise AcquisitionError("orphan VernacularName core ID")
                for term in VERNACULAR_REQUIRED:
                    if not row[vernacular.terms[term]]:
                        raise AcquisitionError(f"required VernacularName value is empty: {term}")
                links[core_id] = links.get(core_id, 0) + 1
            counts["VernacularName"] = vernacular_count
            for extension in extensions:
                if extension.row_type in DISTRIBUTION_ROW_TYPES:
                    # Distribution rows are structurally checked (bounded stream,
                    # core-id linkage) and then ignored. No Distribution field
                    # is imported or interpreted here.
                    distribution_count = 0
                    for row in _rows(archive, extension):
                        distribution_count += 1
                        if not row[extension.link_index].strip() or row[extension.link_index].strip() not in core_ids:
                            raise AcquisitionError("orphan Distribution core ID")
                    counts["Distribution"] = distribution_count
    except zipfile.BadZipFile as exc:
        raise AcquisitionError(f"malformed ZIP: {exc}") from exc
    return {
        "report_schema_version": 1, "profile_code": PROFILE_CODE, "result": "passed",
        "request_definition_sha256": request.request_sha256,
        "archive": {
            "bytes": path.stat().st_size,
            "member_count": len(members),
            "sha256": sha256_file(path),
        },
        "meta_xml": {
            "core_location": core.location,
            "core_lines_terminated_by": core.line_terminator_declared,
            "extensions": [{"row_type": item.row_type, "location": item.location} for item in extensions],
            "original_term_uris": {"Taxon": core.term_uris, "VernacularName": vernacular.term_uris},
        },
        "record_counts": counts,
        "linkage": {"distinct_core_row_ids": len(core_ids), "vernacular_rows_by_core_id": links},
        "identifier_contract": {
            "core_row_id": {"index": core.link_index, "role": "archive-local core row key"},
            "dwc:taxonID": {"index": core.terms["taxonID"], "role": "source-defined taxon identifier"},
            "dwc:acceptedNameUsageID": {"index": core.terms["acceptedNameUsageID"], "role": "accepted usage reference"},
            "dwc:parentNameUsageID": {"index": core.terms["parentNameUsageID"], "role": "parent usage reference"},
            "extension_coreid": {"index": vernacular.link_index, "role": "archive-local link to core row ID"},
            "NBIC_scientific_name_id": {"role": "namespaced scientific-name identifier; never numerically collapsed"},
            "raw_samples": identifier_samples,
        },
        "network_calls": 0,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("validate-request", "normalize-request", "plan"):
        command = commands.add_parser(name)
        command.add_argument("request", type=Path)
        command.add_argument("--proposal", type=Path, default=DEFAULT_PROPOSAL)
        if name == "plan":
            command.add_argument("--sources-root", type=Path, default=DEFAULT_SOURCES_ROOT)
    validate = commands.add_parser("validate-fixture")
    validate.add_argument("request", type=Path)
    validate.add_argument("archive", type=Path)
    validate.add_argument("--proposal", type=Path, default=DEFAULT_PROPOSAL)
    status = commands.add_parser("status")
    status.add_argument("release_dir", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "status":
            print((args.release_dir / "manifest.json").read_text(encoding="utf-8"), end="")
            return 0
        request = load_request(args.request, args.proposal)
        if args.command == "validate-request":
            print(request.request_sha256)
        elif args.command == "normalize-request":
            print(json.dumps({**request.identity(), "canonical_request_sha256": request.request_sha256},
                             ensure_ascii=False, sort_keys=True, indent=2))
        elif args.command == "plan":
            target, idempotent = plan(request, args.sources_root)
            print(json.dumps({"created": not idempotent, "idempotent": idempotent, "network_calls": 0,
                              "release_dir": str(target), "request_sha256": request.request_sha256,
                              "source_selection_proposal_sha256": request.source_selection_proposal_sha256},
                             sort_keys=True))
        elif args.command == "validate-fixture":
            print(json.dumps(validate_fixture(args.archive, request), ensure_ascii=False,
                             sort_keys=True, indent=2))
        return 0
    except (AcquisitionError, OSError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=__import__("sys").stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
