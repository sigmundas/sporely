#!/usr/bin/env python3
"""National taxonomy source-adapter kit.

A small, offline toolkit that lets a maintainer add a national Darwin Core
Archive source (Norway, Sweden, later Denmark, …) as a source-profile plus
optional pinned archive, then normalize it into the shared compiler input
format. No source has its own compiler; every national source is an adapter
feeding the single Sporely compiler downstream.

This CLI intentionally supports ONLY:

    * Darwin Core Archives with a top-level ``meta.xml``.
    * Configurable Taxon-core and VernacularName-extension term mappings.
    * Optional Distribution validation, without importing distribution data.

It is not a plugin framework. There is no dynamic Python-hook loading, no
web UI, no download authorization. A JSON profile plus this reusable DwC-A
adapter is enough for the currently intended countries.

Subcommands:
    init       Create a starter profile skeleton for a new source.
    inspect    Read an archive's meta.xml and suggest a term mapping.
    validate   Verify an archive against a profile without emitting output.
    normalize  Emit normalized JSONL for the shared compiler.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping
from xml.etree import ElementTree as ET


DWCA_NS = "http://rs.tdwg.org/dwc/text/"
DWC_TAXON = "http://rs.tdwg.org/dwc/terms/Taxon"
GBIF_VERNACULAR = "http://rs.gbif.org/terms/1.0/VernacularName"
GBIF_DISTRIBUTION = "http://rs.gbif.org/terms/1.0/Distribution"
DWC_DISTRIBUTION = "http://rs.tdwg.org/dwc/terms/Distribution"
DISTRIBUTION_ROW_TYPES = frozenset({DWC_DISTRIBUTION, GBIF_DISTRIBUTION})

MAX_MEMBERS = 200
MAX_MEMBER_BYTES = 128 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_FIELD_BYTES = 256 * 1024
MAX_LINE_BYTES = 1024 * 1024
CHUNK_BYTES = 512 * 1024
# Raise the CSV reader's built-in field-size cap enough that oversize
# fields reach the adapter's MAX_FIELD_BYTES / MAX_LINE_BYTES gates and
# fail with a NationalSourceError rather than a bare csv.Error.
csv.field_size_limit(max(csv.field_size_limit(), MAX_LINE_BYTES + MAX_FIELD_BYTES))
SUPPORTED_ENCODINGS = {"utf-8": "utf-8", "utf8": "utf-8", "UTF-8": "utf-8"}
SUPPORTED_DELIMITERS = {"\\t": "\t", "\t": "\t", ",": ",", "\\,": ","}
SUPPORTED_LINE_TERMINATORS = {"\\n": "\n", "\n": "\n", "\\r\\n": "\r\n", "\r\n": "\r\n"}

CORE_REQUIRED_TERMS = (
    "taxonID", "scientificName", "taxonRank", "taxonomicStatus",
)
CORE_OPTIONAL_TERMS = (
    "acceptedNameUsageID", "parentNameUsageID", "scientificNameAuthorship",
    "kingdom", "family", "genus", "specificEpithet",
)
VERNACULAR_REQUIRED_TERMS = ("vernacularName", "language")
VERNACULAR_OPTIONAL_TERMS = ("countryCode", "isPreferredName", "source")


class NationalSourceError(Exception):
    """Raised on any adapter-profile or archive validation problem."""


# ----- Safe path / bounded ZIP helpers -----


def _safe_location(value: str) -> str:
    if not value or "\\" in value:
        raise NationalSourceError(f"unsafe DwC-A location: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or any(p in {"", "."} for p in path.parts):
        raise NationalSourceError(f"unsafe DwC-A location: {value!r}")
    return value


def _open_archive(path: Path) -> zipfile.ZipFile:
    if not path.exists() or not path.is_file():
        raise NationalSourceError(f"archive does not exist: {path}")
    try:
        archive = zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise NationalSourceError(f"not a ZIP archive: {path}") from exc
    infos = archive.infolist()
    if len(infos) > MAX_MEMBERS:
        archive.close()
        raise NationalSourceError(f"archive has {len(infos)} members; limit is {MAX_MEMBERS}")
    total = 0
    seen_names: set[str] = set()
    for info in infos:
        if info.filename in seen_names:
            archive.close()
            raise NationalSourceError(
                f"duplicate ZIP member: {info.filename!r}"
            )
        seen_names.add(info.filename)
        if info.file_size > MAX_MEMBER_BYTES:
            archive.close()
            raise NationalSourceError(
                f"member {info.filename!r} is {info.file_size} bytes; per-member limit is {MAX_MEMBER_BYTES}"
            )
        total += info.file_size
        _safe_location(info.filename)
        if info.filename.endswith("/"):
            continue
        # No symlinks / special files (create_system 3 = unix; mode in external_attr).
        mode = info.external_attr >> 16
        if mode and (mode & 0o170000) not in (0o100000, 0):
            archive.close()
            raise NationalSourceError(f"member {info.filename!r} is not a regular file")
    if total > MAX_TOTAL_BYTES:
        archive.close()
        raise NationalSourceError(
            f"archive uncompressed total {total} exceeds limit {MAX_TOTAL_BYTES}"
        )
    return archive


def _archive_sha256(path: Path) -> str:
    """Bounded incremental SHA-256 of an archive file (never whole-file read)."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


# ----- meta.xml parsing -----


@dataclass(frozen=True)
class DwcaTable:
    kind: str  # "core" or "extension"
    row_type: str
    location: str
    encoding: str
    delimiter: str
    line_terminator: str
    ignore_header_lines: int
    id_index: int | None
    coreid_index: int | None
    term_index: dict[str, int]  # term URI → 0-based column index
    quote_char: str | None


def _local_name(term: str) -> str:
    return term.rsplit("/", 1)[-1].rsplit("#", 1)[-1]


def _decode_meta_attr(name: str, value: str, translation: dict[str, str]) -> str:
    if value not in translation:
        raise NationalSourceError(f"meta.xml {name} value not supported: {value!r}")
    return translation[value]


def _parse_table(element: ET.Element, kind: str) -> DwcaTable:
    row_type = element.attrib.get("rowType", "")
    if not row_type:
        raise NationalSourceError(f"meta.xml {kind} lacks rowType")
    encoding = _decode_meta_attr("encoding", element.attrib.get("encoding", "UTF-8"), SUPPORTED_ENCODINGS)
    delimiter = _decode_meta_attr(
        "fieldsTerminatedBy", element.attrib.get("fieldsTerminatedBy", "\\t"), SUPPORTED_DELIMITERS,
    )
    lines = _decode_meta_attr(
        "linesTerminatedBy", element.attrib.get("linesTerminatedBy", "\\n"), SUPPORTED_LINE_TERMINATORS,
    )
    try:
        ignore_header_lines = int(element.attrib.get("ignoreHeaderLines", "0"))
    except ValueError as exc:
        raise NationalSourceError(f"meta.xml {kind} ignoreHeaderLines is not an integer") from exc
    quote_char = element.attrib.get("fieldsEnclosedBy") or None
    locations = [f.text or "" for f in element.findall(f"{{{DWCA_NS}}}files/{{{DWCA_NS}}}location")]
    if len(locations) != 1:
        raise NationalSourceError(f"meta.xml {kind} must declare exactly one location")
    location = _safe_location(locations[0].strip())
    id_node = element.find(f"{{{DWCA_NS}}}id")
    coreid_node = element.find(f"{{{DWCA_NS}}}coreid")
    try:
        id_index = int(id_node.attrib["index"]) if (kind == "core" and id_node is not None) else None
        coreid_index = int(coreid_node.attrib["index"]) if (kind == "extension" and coreid_node is not None) else None
    except (KeyError, ValueError) as exc:
        raise NationalSourceError(f"meta.xml {kind} has malformed id/coreid index") from exc
    term_index: dict[str, int] = {}
    for field in element.findall(f"{{{DWCA_NS}}}field"):
        term = field.attrib.get("term", "")
        try:
            index = int(field.attrib.get("index", "-1"))
        except ValueError as exc:
            raise NationalSourceError(f"meta.xml {kind} field index is not an integer") from exc
        if not term or index < 0:
            raise NationalSourceError("meta.xml field lacks term/index")
        if term in term_index:
            raise NationalSourceError(f"meta.xml duplicate term: {term}")
        term_index[term] = index
    return DwcaTable(
        kind=kind, row_type=row_type, location=location,
        encoding=encoding, delimiter=delimiter, line_terminator=lines,
        ignore_header_lines=ignore_header_lines,
        id_index=id_index, coreid_index=coreid_index,
        term_index=term_index, quote_char=quote_char,
    )


def _require_member_exists(archive: zipfile.ZipFile, location: str) -> None:
    if location not in archive.namelist():
        raise NationalSourceError(
            f"meta.xml references ZIP member {location!r} which the archive does not contain"
        )


def parse_meta(raw: bytes) -> tuple[DwcaTable, list[DwcaTable]]:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise NationalSourceError(f"malformed meta.xml: {exc}") from exc
    if root.tag.rsplit("}", 1)[-1] != "archive":
        raise NationalSourceError("meta.xml root must be <archive>")
    cores = [n for n in root if n.tag.rsplit("}", 1)[-1] == "core"]
    exts = [n for n in root if n.tag.rsplit("}", 1)[-1] == "extension"]
    if len(cores) != 1:
        raise NationalSourceError("meta.xml must declare exactly one <core>")
    core = _parse_table(cores[0], "core")
    if core.row_type != DWC_TAXON:
        raise NationalSourceError(
            f"core rowType must be {DWC_TAXON}, got {core.row_type}"
        )
    extensions = [_parse_table(node, "extension") for node in exts]
    return core, extensions


# ----- Profile schema -----


@dataclass(frozen=True)
class NationalProfile:
    source_code: str
    source_release: dict[str, str]
    identifier_namespace: str
    core_row_type: str
    core_location: str
    core_terms: dict[str, str]  # profile-field → DwC term URI
    vernacular_row_type: str
    vernacular_location: str
    vernacular_terms: dict[str, str]
    distribution_row_type: str | None
    distribution_location: str | None
    distribution_validation_only: bool
    optional_external_id_terms: tuple[str, ...]


PROFILE_SCHEMA_VERSION = 1
_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")


def load_profile(path: Path) -> NationalProfile:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NationalSourceError(f"cannot read profile {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise NationalSourceError(f"profile must be a JSON object: {path}")
    if raw.get("profile_schema_version") != PROFILE_SCHEMA_VERSION:
        raise NationalSourceError(
            f"profile_schema_version must be {PROFILE_SCHEMA_VERSION}"
        )
    source_code = raw.get("source_code", "")
    if not (isinstance(source_code, str) and _ID_RE.match(source_code)):
        raise NationalSourceError("source_code must be a short lowercase identifier")
    release = raw.get("source_release") or {}
    if not isinstance(release, dict) or "version" not in release or "issued_date" not in release:
        raise NationalSourceError("source_release must contain version + issued_date")
    identifier_namespace = raw.get("identifier_namespace", "")
    if not isinstance(identifier_namespace, str) or not identifier_namespace.strip():
        raise NationalSourceError("identifier_namespace must be a non-empty string")
    core = raw.get("core") or {}
    vern = raw.get("vernacular") or {}
    if not isinstance(core, dict) or not isinstance(vern, dict):
        raise NationalSourceError("core and vernacular blocks must be objects")
    for section, label in ((core, "core"), (vern, "vernacular")):
        if not isinstance(section.get("row_type"), str) or not section["row_type"]:
            raise NationalSourceError(f"{label}.row_type must be a URI string")
        _safe_location(section.get("location", ""))
        terms = section.get("term_mapping") or {}
        if not isinstance(terms, dict) or not terms:
            raise NationalSourceError(f"{label}.term_mapping must be a non-empty object")
    core_terms = {k: v for k, v in core["term_mapping"].items()}
    vern_terms = {k: v for k, v in vern["term_mapping"].items()}
    dist = raw.get("distribution") or None
    dist_row_type = dist_location = None
    dist_validate_only = True
    if dist is not None:
        if not isinstance(dist, dict):
            raise NationalSourceError("distribution must be an object")
        dist_row_type = dist.get("row_type")
        if dist_row_type not in DISTRIBUTION_ROW_TYPES:
            raise NationalSourceError(
                f"distribution.row_type must be one of {sorted(DISTRIBUTION_ROW_TYPES)}"
            )
        dist_location = _safe_location(dist.get("location", ""))
        dist_validate_only = bool(dist.get("validation_only", True))
        if not dist_validate_only:
            raise NationalSourceError(
                "distribution.validation_only must be true; distribution data is not imported"
            )
    optional_ids = tuple(raw.get("optional_external_id_terms") or ())
    if not all(isinstance(t, str) and t for t in optional_ids):
        raise NationalSourceError("optional_external_id_terms must be a list of URIs")
    return NationalProfile(
        source_code=source_code,
        source_release={"version": str(release["version"]),
                        "issued_date": str(release["issued_date"])},
        identifier_namespace=identifier_namespace,
        core_row_type=core["row_type"],
        core_location=core["location"],
        core_terms=core_terms,
        vernacular_row_type=vern["row_type"],
        vernacular_location=vern["location"],
        vernacular_terms=vern_terms,
        distribution_row_type=dist_row_type,
        distribution_location=dist_location,
        distribution_validation_only=dist_validate_only,
        optional_external_id_terms=optional_ids,
    )


def profile_skeleton(source_code: str) -> dict[str, Any]:
    return {
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "source_code": source_code,
        "source_release": {"version": "0.0.0", "issued_date": "1970-01-01"},
        "identifier_namespace": f"{source_code.upper()}:",
        "core": {
            "row_type": DWC_TAXON,
            "location": "taxa.tsv",
            "term_mapping": {
                "taxonID":                 "http://rs.tdwg.org/dwc/terms/taxonID",
                "acceptedNameUsageID":     "http://rs.tdwg.org/dwc/terms/acceptedNameUsageID",
                "parentNameUsageID":       "http://rs.tdwg.org/dwc/terms/parentNameUsageID",
                "scientificName":          "http://rs.tdwg.org/dwc/terms/scientificName",
                "scientificNameAuthorship":"http://rs.tdwg.org/dwc/terms/scientificNameAuthorship",
                "taxonRank":               "http://rs.tdwg.org/dwc/terms/taxonRank",
                "taxonomicStatus":         "http://rs.tdwg.org/dwc/terms/taxonomicStatus",
            },
        },
        "vernacular": {
            "row_type": GBIF_VERNACULAR,
            "location": "vernacular.tsv",
            "term_mapping": {
                "vernacularName": "http://rs.gbif.org/terms/1.0/vernacularName",
                "language":       "http://rs.tdwg.org/dwc/terms/language",
                "isPreferredName":"http://rs.gbif.org/terms/1.0/isPreferredName",
            },
        },
        "distribution": {
            "row_type": GBIF_DISTRIBUTION,
            "location": "distribution.tsv",
            "validation_only": True,
        },
        "optional_external_id_terms": [],
    }


# ----- Bounded row iterator -----


def _iter_rows(archive: zipfile.ZipFile, table: DwcaTable) -> Iterator[list[str]]:
    try:
        member = archive.open(table.location, "r")
    except KeyError as exc:
        raise NationalSourceError(
            f"meta.xml references ZIP member {table.location!r} which the archive does not contain"
        ) from exc
    with member as raw:
        text = io.TextIOWrapper(raw, encoding=table.encoding, newline="")
        reader = csv.reader(
            text, delimiter=table.delimiter,
            quotechar=(table.quote_char or '"'),
            quoting=csv.QUOTE_MINIMAL if table.quote_char else csv.QUOTE_NONE,
        )
        for skip in range(table.ignore_header_lines):
            try:
                next(reader)
            except StopIteration:
                return
        for row in reader:
            row_bytes = 0
            for value in row:
                encoded = len(value.encode("utf-8"))
                if encoded > MAX_FIELD_BYTES:
                    raise NationalSourceError("row field exceeds maximum size")
                row_bytes += encoded
            if row_bytes > MAX_LINE_BYTES:
                raise NationalSourceError(
                    f"row exceeds maximum record size ({row_bytes} > {MAX_LINE_BYTES})"
                )
            yield row


_DWC_CLASSIFICATION_TERMS = (
    ("kingdom", "http://rs.tdwg.org/dwc/terms/kingdom"),
    ("phylum", "http://rs.tdwg.org/dwc/terms/phylum"),
    ("class", "http://rs.tdwg.org/dwc/terms/class"),
    ("order", "http://rs.tdwg.org/dwc/terms/order"),
    ("family", "http://rs.tdwg.org/dwc/terms/family"),
    ("genus", "http://rs.tdwg.org/dwc/terms/genus"),
    ("specific_epithet", "http://rs.tdwg.org/dwc/terms/specificEpithet"),
    ("infraspecific_epithet",
     "http://rs.tdwg.org/dwc/terms/infraspecificEpithet"),
)


def _classification_from_row(core: "DwcaTable", row: list[str]) -> dict[str, str]:
    """Return declared Darwin Core classification terms, if any.

    Reads directly from the archive's ``meta.xml`` term index so that we
    preserve exactly what the source publishes without depending on the
    per-source profile enumerating each classification term. Unmapped terms
    return an empty string; nothing is invented from display text.
    """
    out: dict[str, str] = {}
    for key, term_uri in _DWC_CLASSIFICATION_TERMS:
        idx = core.term_index.get(term_uri)
        if idx is None or idx >= len(row):
            out[key] = ""
        else:
            out[key] = row[idx]
    return out


_PREFERRED_TRUE = frozenset({"true", "1"})
_PREFERRED_FALSE = frozenset({"false", "0", ""})


def _parse_preferred_boolean(raw: str) -> bool:
    value = raw.strip().casefold()
    if value in _PREFERRED_TRUE:
        return True
    if value in _PREFERRED_FALSE:
        return False
    raise NationalSourceError(f"malformed preferred-name boolean: {raw!r}")


# ----- Public operations -----


def inspect_archive(archive_path: Path) -> dict[str, Any]:
    archive = _open_archive(archive_path)
    try:
        if "meta.xml" not in archive.namelist():
            raise NationalSourceError("archive lacks meta.xml at root")
        core, extensions = parse_meta(archive.read("meta.xml"))
    finally:
        archive.close()
    def summarize(table: DwcaTable) -> dict[str, Any]:
        return {
            "kind": table.kind,
            "row_type": table.row_type,
            "location": table.location,
            "encoding": table.encoding,
            "delimiter": table.delimiter.replace("\t", "\\t"),
            "line_terminator": table.line_terminator.replace("\n", "\\n").replace("\r", "\\r"),
            "ignore_header_lines": table.ignore_header_lines,
            "columns": sorted(table.term_index),
        }
    return {
        "archive": str(archive_path),
        "core": summarize(core),
        "extensions": [summarize(ext) for ext in extensions],
        "suggested_core_term_mapping": {
            _local_name(term): term for term in core.term_index if _local_name(term) in
            set(CORE_REQUIRED_TERMS) | set(CORE_OPTIONAL_TERMS)
        },
    }


def _validate_against_profile(
    archive: zipfile.ZipFile, profile: NationalProfile,
) -> tuple[DwcaTable, DwcaTable, DwcaTable | None]:
    if "meta.xml" not in archive.namelist():
        raise NationalSourceError("archive lacks meta.xml at root")
    core, extensions = parse_meta(archive.read("meta.xml"))
    if core.row_type != profile.core_row_type:
        raise NationalSourceError(
            f"core row_type mismatch: profile={profile.core_row_type} archive={core.row_type}"
        )
    if core.location != profile.core_location:
        raise NationalSourceError(
            f"core location mismatch: profile={profile.core_location} archive={core.location}"
        )
    # Every declared table location must actually exist as a ZIP member.
    _require_member_exists(archive, core.location)
    for ext in extensions:
        _require_member_exists(archive, ext.location)
    vernaculars = [t for t in extensions if t.row_type == profile.vernacular_row_type]
    if len(vernaculars) == 0:
        raise NationalSourceError(
            f"vernacular extension {profile.vernacular_row_type} not present in archive"
        )
    if len(vernaculars) > 1:
        raise NationalSourceError(
            f"exactly one VernacularName extension is required; the archive declares {len(vernaculars)}"
        )
    vernacular = vernaculars[0]
    if vernacular.location != profile.vernacular_location:
        raise NationalSourceError(
            f"vernacular location mismatch: profile={profile.vernacular_location} "
            f"archive={vernacular.location}"
        )
    # Required terms must be resolvable through the profile's term mapping.
    def resolve(term_uri: str, table: DwcaTable, field_label: str) -> int:
        if term_uri not in table.term_index:
            raise NationalSourceError(
                f"{field_label} maps to term {term_uri} which the archive does not declare"
            )
        return table.term_index[term_uri]

    for field in CORE_REQUIRED_TERMS:
        term = profile.core_terms.get(field)
        if not term:
            raise NationalSourceError(f"profile.core.term_mapping is missing required field {field!r}")
        resolve(term, core, f"core.{field}")
    for field in VERNACULAR_REQUIRED_TERMS:
        term = profile.vernacular_terms.get(field)
        if not term:
            raise NationalSourceError(f"profile.vernacular.term_mapping is missing required field {field!r}")
        resolve(term, vernacular, f"vernacular.{field}")
    if core.id_index is None:
        raise NationalSourceError("core must declare an <id> element in meta.xml")
    if vernacular.coreid_index is None:
        raise NationalSourceError("vernacular extension must declare a <coreid> element in meta.xml")

    dist = None
    if profile.distribution_row_type is not None:
        distributions = [t for t in extensions if t.row_type in DISTRIBUTION_ROW_TYPES]
        if len(distributions) > 1:
            raise NationalSourceError("at most one Distribution extension is permitted")
        if distributions:
            dist = distributions[0]
            if dist.row_type != profile.distribution_row_type:
                raise NationalSourceError(
                    f"distribution row_type mismatch: profile={profile.distribution_row_type} archive={dist.row_type}"
                )
            if dist.location != profile.distribution_location:
                raise NationalSourceError(
                    f"distribution location mismatch: profile={profile.distribution_location} archive={dist.location}"
                )
    # Unknown extensions (not vernacular, not distribution) are refused.
    allowed = {profile.vernacular_row_type} | (DISTRIBUTION_ROW_TYPES if profile.distribution_row_type else set())
    unknown = [t.row_type for t in extensions if t.row_type not in allowed]
    if unknown:
        raise NationalSourceError(f"unsupported extension row type: {unknown[0]}")
    return core, vernacular, dist


def validate_archive(profile: NationalProfile, archive_path: Path) -> dict[str, Any]:
    archive = _open_archive(archive_path)
    try:
        core, vernacular, distribution = _validate_against_profile(archive, profile)
        core_id_index = core.id_index
        assert core_id_index is not None
        core_ids: set[str] = set()
        taxon_count = 0
        for row in _iter_rows(archive, core):
            if len(row) <= core_id_index:
                raise NationalSourceError("core row width is inconsistent with meta.xml")
            core_id = row[core_id_index]
            if not core_id.strip():
                raise NationalSourceError("core row_id must be non-empty")
            if core_id in core_ids:
                raise NationalSourceError(f"duplicate core row id: {core_id!r}")
            core_ids.add(core_id)
            taxon_count += 1
        vernacular_count = 0
        vern_link_index = vernacular.coreid_index
        assert vern_link_index is not None
        for row in _iter_rows(archive, vernacular):
            if len(row) <= vern_link_index:
                raise NationalSourceError("vernacular row width is inconsistent with meta.xml")
            link = row[vern_link_index]
            if not link.strip() or link not in core_ids:
                raise NationalSourceError(f"orphan vernacular link: {link!r}")
            vernacular_count += 1
        distribution_count: int | None = None
        if distribution is not None:
            distribution_count = 0
            link_index = distribution.coreid_index
            if link_index is None:
                raise NationalSourceError("distribution extension must declare <coreid>")
            for row in _iter_rows(archive, distribution):
                if len(row) <= link_index:
                    raise NationalSourceError("distribution row width is inconsistent with meta.xml")
                link = row[link_index]
                if not link.strip() or link not in core_ids:
                    raise NationalSourceError(f"orphan distribution link: {link!r}")
                distribution_count += 1
    finally:
        archive.close()
    return {
        "result": "passed",
        "profile_source_code": profile.source_code,
        "profile_source_release": profile.source_release,
        "record_counts": {
            "Taxon": taxon_count,
            "VernacularName": vernacular_count,
            **({"Distribution": distribution_count} if distribution_count is not None else {}),
        },
        "distribution_imported": False,
    }


def normalize_archive(
    profile: NationalProfile, archive_path: Path, output_dir: Path,
) -> dict[str, Any]:
    """Transactional normalize.

    Writes every artifact to a unique temporary sibling directory. On any
    handled failure the temporary directory is removed and no output appears
    at ``output_dir``. Only after every Taxon, VernacularName, Distribution,
    report, and hash step succeeds is the temporary directory atomically
    renamed to ``output_dir``.
    """
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".{output_dir.name}.", suffix=".tmp",
        dir=str(output_dir.parent),
    ))
    committed = False
    try:
        report = _normalize_into(profile, archive_path, staging)
        # Atomic rename: after this, output_dir contains the complete evidence.
        os.replace(staging, output_dir)
        committed = True
        return report
    finally:
        if not committed:
            shutil.rmtree(staging, ignore_errors=True)


def _normalize_into(
    profile: NationalProfile, archive_path: Path, staging: Path,
) -> dict[str, Any]:
    archive = _open_archive(archive_path)
    taxa_out = staging / "taxa.jsonl"
    vern_out = staging / "vernacular.jsonl"
    report_out = staging / "report.json"
    try:
        core, vernacular, distribution = _validate_against_profile(archive, profile)
        core_id_index = core.id_index
        vern_link_index = vernacular.coreid_index
        assert core_id_index is not None and vern_link_index is not None

        def _resolve(term_key: str, mapping: dict[str, str], table: DwcaTable, row: list[str]) -> str:
            term = mapping.get(term_key)
            if not term:
                return ""
            idx = table.term_index.get(term)
            if idx is None or idx >= len(row):
                return ""
            return row[idx]

        namespaces = profile_identifier_namespaces(profile)
        provenance_base = {
            "source_code": profile.source_code,
            "source_release": profile.source_release,
            "identifier_namespace": profile.identifier_namespace,
        }
        # Two-pass over core: first pass collects core row IDs AND taxonIDs so
        # the second pass can validate accepted/parent identifier references
        # against the known taxonID set (DwC-A references are by taxonID).
        core_rows_cache: list[tuple[int, list[str]]] = []
        core_row_ids: set[str] = set()
        core_taxon_ids: set[str] = set()
        for row_index, row in enumerate(_iter_rows(archive, core)):
            if len(row) <= core_id_index:
                raise NationalSourceError("core row width is inconsistent")
            core_row_id = row[core_id_index]
            if not core_row_id.strip():
                raise NationalSourceError("core row ID (DwC-A <id>) must be non-empty")
            if core_row_id in core_row_ids:
                raise NationalSourceError(f"duplicate core row ID: {core_row_id!r}")
            core_row_ids.add(core_row_id)
            taxon_id = _resolve("taxonID", profile.core_terms, core, row)
            if not taxon_id.strip():
                raise NationalSourceError("dwc:taxonID must be non-empty")
            if taxon_id in core_taxon_ids:
                raise NationalSourceError(f"duplicate dwc:taxonID: {taxon_id!r}")
            core_taxon_ids.add(taxon_id)
            for required in ("scientificName", "taxonRank", "taxonomicStatus"):
                if not _resolve(required, profile.core_terms, core, row).strip():
                    raise NationalSourceError(
                        f"required core term {required!r} is empty at row_index={row_index}"
                    )
            core_rows_cache.append((row_index, row))

        orphan_parent_pairs: set[tuple[str, str]] = set()
        with taxa_out.open("w", encoding="utf-8") as tf:
            for row_index, row in core_rows_cache:
                core_row_id = row[core_id_index]
                taxon_id = _resolve("taxonID", profile.core_terms, core, row)
                accepted = _resolve("acceptedNameUsageID", profile.core_terms, core, row)
                parent = _resolve("parentNameUsageID", profile.core_terms, core, row)
                # Compilation-blocker: a NON-EMPTY acceptedNameUsageID must
                # resolve. Empty accepted on a synonym is caught downstream
                # once synonym-rank semantics are checked (not this stage).
                if accepted and accepted not in core_taxon_ids:
                    raise NationalSourceError(
                        f"acceptedNameUsageID references unknown taxonID: {accepted!r}"
                    )
                # Non-blocking: a NON-EMPTY parentNameUsageID that does not
                # resolve is preserved verbatim as an unresolved hierarchy
                # edge. No inferred parent relationship is created.
                if not parent:
                    parent_resolution = "absent"
                elif parent in core_taxon_ids:
                    parent_resolution = "resolved"
                else:
                    parent_resolution = "unresolved"
                    orphan_parent_pairs.add((str(taxon_id), str(parent)))
                # Optional external IDs are preserved as-is under their FULL
                # term URI, so two terms with the same DwC local name never
                # collide.
                external_ids: dict[str, str] = {}
                for term_uri in profile.optional_external_id_terms:
                    if term_uri in core.term_index:
                        idx = core.term_index[term_uri]
                        if idx < len(row) and row[idx].strip():
                            external_ids[term_uri] = row[idx]
                record = {
                    "source_code": profile.source_code,
                    "source_release": profile.source_release,
                    "core_row_id": {
                        "value": str(core_row_id),
                        "namespace": namespaces["core_row_id"],
                    },
                    "taxon_id": {
                        "value": str(taxon_id),
                        "namespace": namespaces["taxon_id"],
                    },
                    "accepted_name_usage_id": (
                        {"value": str(accepted),
                         "namespace": namespaces["accepted_name_usage_id"]}
                        if accepted else None
                    ),
                    # `parent_name_usage_id` object is preserved unchanged:
                    # the raw namespaced identifier lives there whether the
                    # target resolves or not. The distinct resolution state
                    # is exposed under `parent_reference_resolution`.
                    "parent_name_usage_id": (
                        {"value": str(parent),
                         "namespace": namespaces["parent_name_usage_id"]}
                        if parent else None
                    ),
                    "parent_reference_resolution": parent_resolution,
                    "identifier_namespace": profile.identifier_namespace,
                    "scientific_name": _resolve("scientificName", profile.core_terms, core, row),
                    "authorship": _resolve("scientificNameAuthorship", profile.core_terms, core, row),
                    "rank": _resolve("taxonRank", profile.core_terms, core, row),
                    "taxonomic_status": _resolve("taxonomicStatus", profile.core_terms, core, row),
                    "external_ids": external_ids,
                    # Preserve Darwin Core higher-classification fields
                    # declared by the archive so downstream scope, mapping,
                    # and display logic can consult them without a source-
                    # specific parser hack. Terms absent from the archive
                    # remain empty strings; nothing is invented.
                    "classification": _classification_from_row(core, row),
                    "provenance": {
                        **provenance_base,
                        "member": core.location,
                        "row_index": row_index,
                    },
                }
                tf.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        vern_count = 0
        with vern_out.open("w", encoding="utf-8") as vf:
            for row_index, row in enumerate(_iter_rows(archive, vernacular)):
                if len(row) <= vern_link_index:
                    raise NationalSourceError("vernacular row width is inconsistent")
                link = row[vern_link_index]
                if not link.strip() or link not in core_row_ids:
                    raise NationalSourceError(f"orphan vernacular link: {link!r}")
                vernacular_name = _resolve("vernacularName", profile.vernacular_terms, vernacular, row)
                language = _resolve("language", profile.vernacular_terms, vernacular, row)
                if not vernacular_name.strip():
                    raise NationalSourceError(f"vernacularName is empty at row_index={row_index}")
                if not language.strip():
                    raise NationalSourceError(f"language is empty at row_index={row_index}")
                is_preferred_raw = _resolve("isPreferredName", profile.vernacular_terms, vernacular, row)
                record = {
                    "source_code": profile.source_code,
                    "source_release": profile.source_release,
                    "core_row_id": {
                        "value": str(link),
                        "namespace": namespaces["core_row_id"],
                    },
                    "vernacular_name": vernacular_name,
                    "language": language,
                    "is_preferred": _parse_preferred_boolean(is_preferred_raw),
                    "provenance": {
                        **provenance_base,
                        "member": vernacular.location,
                        "row_index": row_index,
                    },
                }
                vf.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                vern_count += 1
        distribution_count = 0
        if distribution is not None:
            link_index = distribution.coreid_index
            if link_index is None:
                raise NationalSourceError("distribution extension must declare <coreid>")
            for row in _iter_rows(archive, distribution):
                if len(row) <= link_index:
                    raise NationalSourceError("distribution row width is inconsistent")
                link = row[link_index]
                if not link.strip() or link not in core_row_ids:
                    raise NationalSourceError(f"orphan distribution link: {link!r}")
                distribution_count += 1
        MAX_UNRESOLVED_PARENT_SAMPLES = 25
        unresolved_parent_reference_samples = [
            {"source_taxon_id": s, "raw_reference": r}
            for s, r in sorted(orphan_parent_pairs)[:MAX_UNRESOLVED_PARENT_SAMPLES]
        ]
        report = {
            "result": "passed",
            "profile_source_code": profile.source_code,
            "profile_source_release": profile.source_release,
            "record_counts": {
                "Taxon": len(core_row_ids),
                "VernacularName": vern_count,
                **({"Distribution": distribution_count} if distribution is not None else {}),
            },
            "outputs": {
                "taxa": str(taxa_out.name),
                "vernacular": str(vern_out.name),
            },
            "distribution_imported": False,
            "identifier_namespaces": namespaces,
            "archive_sha256": _archive_sha256(archive_path),
            "reference_gaps": {
                "orphan_parent_reference_count": len(orphan_parent_pairs),
                "orphan_accepted_reference_count": 0,
                "orphan_parent_reference_samples": unresolved_parent_reference_samples,
                "orphan_accepted_reference_samples": [],
                "sample_bound": MAX_UNRESOLVED_PARENT_SAMPLES,
            },
            # Hierarchy is complete only when every non-empty parent resolves.
            "hierarchy_complete": len(orphan_parent_pairs) == 0,
            # compiler_ready is True when no COMPILATION-BLOCKING check fails.
            # An unresolved accepted reference is a blocker (a synonym without
            # a placed target cannot be compiled); it raises before reaching
            # this point in normalize_archive. A parent-only unresolved
            # hierarchy edge is a warning, not a blocker.
            "compiler_ready": True,
        }
        report_out.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        return report
    finally:
        archive.close()


def profile_identifier_namespaces(profile: NationalProfile) -> dict[str, str]:
    """Resolve identifier-namespace names per identity-contract.md.

    Every emitted external identifier carries a source-scoped namespace name
    of the form ``<source_code>_<identifier>``. NorTaxa examples in the
    identity contract: ``nortaxa_dwc_id``, ``nortaxa_taxon_id``,
    ``nortaxa_accepted_name_usage_id``, ``nortaxa_parent_name_usage_id``.
    """
    return {
        "core_row_id":               f"{profile.source_code}_dwc_id",
        "taxon_id":                  f"{profile.source_code}_taxon_id",
        "accepted_name_usage_id":    f"{profile.source_code}_accepted_name_usage_id",
        "parent_name_usage_id":      f"{profile.source_code}_parent_name_usage_id",
    }


# ----- CLI -----


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="create a starter profile for a national source")
    init.add_argument("source_code")
    init.add_argument("--output", type=Path,
                      help="destination path (default: national_sources/<code>/source.json)")

    inspect = sub.add_parser("inspect", help="inspect a Darwin Core Archive and suggest a term mapping")
    inspect.add_argument("--archive", type=Path, required=True)

    validate = sub.add_parser("validate", help="validate an archive against a profile")
    validate.add_argument("--profile", type=Path, required=True)
    validate.add_argument("--archive", type=Path, required=True)

    normalize = sub.add_parser("normalize", help="emit normalized compiler input")
    normalize.add_argument("--profile", type=Path, required=True)
    normalize.add_argument("--archive", type=Path, required=True)
    normalize.add_argument("--output", type=Path, required=True)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "init":
            skel = profile_skeleton(args.source_code)
            out = args.output or (
                Path(__file__).resolve().parents[1] / "national_sources"
                / args.source_code / "source.json"
            )
            if out.exists():
                print(f"error: profile already exists: {out}", file=sys.stderr)
                return 2
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(skel, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            print(json.dumps({"created": str(out)}, indent=2))
        elif args.command == "inspect":
            print(json.dumps(inspect_archive(args.archive), indent=2, sort_keys=True))
        elif args.command == "validate":
            profile = load_profile(args.profile)
            print(json.dumps(validate_archive(profile, args.archive), indent=2, sort_keys=True))
        elif args.command == "normalize":
            profile = load_profile(args.profile)
            print(json.dumps(normalize_archive(profile, args.archive, args.output),
                             indent=2, sort_keys=True))
    except NationalSourceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
