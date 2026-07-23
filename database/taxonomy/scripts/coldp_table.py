#!/usr/bin/env python3
"""Bounded record parsing for ColDP tabular deliveries."""

from __future__ import annotations

import csv
import unicodedata
from dataclasses import dataclass

from refresh_col_xr import AcquisitionError


@dataclass(frozen=True)
class LiteralTsvRecord:
    raw_fields: tuple[str, ...]
    semantic_fields: tuple[str, ...]
    terminator: str


def decode_coldp_escapes(value: str) -> str:
    output: list[str] = []
    index = 0
    recognized = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            following = value[index + 1]
            if following in recognized:
                output.append(recognized[following])
                index += 2
                continue
        output.append(value[index])
        index += 1
    return "".join(output)


def parse_literal_tsv_record(
    raw: bytes,
    *,
    line_number: int,
    header: bool = False,
    max_line_bytes: int,
    max_field_bytes: int,
) -> LiteralTsvRecord:
    if len(raw) > max_line_bytes:
        raise AcquisitionError(f"ColDP TSV line {line_number} exceeds the safety ceiling")
    if raw.endswith(b"\n"):
        body = raw[:-1]
        if body.endswith(b"\r"):
            body = body[:-1]
            terminator = "CRLF"
        else:
            terminator = "LF"
    else:
        body = raw
        terminator = "EOF"
    fields = body.split(b"\t")
    if any(len(field) > max_field_bytes for field in fields):
        raise AcquisitionError(f"ColDP TSV field at line {line_number} exceeds the safety ceiling")
    decoded: list[str] = []
    for index, field in enumerate(fields):
        try:
            value = field.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AcquisitionError(
                f"ColDP TSV is not strict UTF-8 at line {line_number}, field {index + 1}"
            ) from exc
        if value.startswith("\ufeff"):
            if not (header and line_number == 1 and index == 0):
                raise AcquisitionError("UTF-8 BOM is allowed only on the first header token")
            value = value[1:]
        if "\ufeff" in value:
            raise AcquisitionError("UTF-8 BOM is allowed only on the first header token")
        decoded.append(value)
    return LiteralTsvRecord(
        raw_fields=tuple(decoded),
        semantic_fields=tuple(decode_coldp_escapes(value) for value in decoded),
        terminator=terminator,
    )


def parse_rfc4180_csv_record(text: str) -> list[str]:
    """Keep actual CSV handling distinct from literal ColDP TSV handling."""
    try:
        return next(csv.reader([text], strict=True))
    except csv.Error as exc:
        raise AcquisitionError(f"malformed RFC 4180 CSV record: {exc}") from exc


def former_strict_csv_failure(raw_fields_record: bytes) -> str | None:
    if b'"' not in raw_fields_record:
        return None
    try:
        text = raw_fields_record.decode("utf-8")
        next(csv.reader([text.rstrip("\r\n")], delimiter="\t", strict=True))
    except (UnicodeDecodeError, csv.Error) as exc:
        return str(exc)
    return None


def bounded_quote_context(raw: bytes, *, radius: int = 80) -> str:
    location = raw.find(b'"')
    if location < 0:
        location = 0
    start = max(0, location - radius)
    end = min(len(raw), location + radius)
    return raw[start:end].decode("utf-8", errors="backslashreplace").encode(
        "unicode_escape"
    ).decode("ascii")
