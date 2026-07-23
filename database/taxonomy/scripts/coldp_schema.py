#!/usr/bin/env python3
"""Exact, entity-specific header profiles for bounded ColDP validation."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

from refresh_col_xr import AcquisitionError


CHECKLISTBANK_COL_XR_PROFILE = "checklistbank-col-xr-2026-07-17"
CANONICAL_PROFILE = "canonical-coldp"
CONTROL_OR_FORMAT = {"Cc", "Cf"}
PERCENT_DELIMITER = re.compile(r"%3[aA]")

NAME_USAGE_FIELDS = (
    "ID", "alternativeID", "nameAlternativeID", "sourceID", "parentID",
    "basionymID", "status", "scientificName", "authorship", "rank", "notho",
    "originalSpelling", "uninomial", "genericName", "infragenericEpithet",
    "specificEpithet", "infraspecificEpithet", "cultivarEpithet",
    "combinationAuthorship", "combinationAuthorshipID",
    "combinationExAuthorship", "combinationExAuthorshipID",
    "combinationAuthorshipYear", "basionymAuthorship", "basionymAuthorshipID",
    "basionymExAuthorship", "basionymExAuthorshipID", "basionymAuthorshipYear",
    "namePhrase", "nameReferenceID", "namePublishedInYear",
    "namePublishedInPage", "namePublishedInPageLink", "gender",
    "genderAgreement", "etymology", "code", "nameStatus", "accordingToID",
    "accordingToPage", "accordingToPageLink", "referenceID", "scrutinizer",
    "scrutinizerID", "scrutinizerDate", "extinct", "temporalRangeStart",
    "temporalRangeEnd", "environment", "species", "section", "subgenus",
    "genus", "subtribe", "tribe", "subfamily", "family", "superfamily",
    "suborder", "order", "subclass", "class", "subphylum", "phylum",
    "kingdom", "ordinal", "branchLength", "link", "nameRemarks", "remarks",
    "modified", "modifiedBy",
)


@dataclass(frozen=True)
class EntityHeaderProfile:
    entity: str
    canonical_fields: frozenset[str]
    required_fields: frozenset[str]
    optional_known_fields: frozenset[str]
    allowed_namespace_profiles: frozenset[str]
    known_opaque_extensions: frozenset[str] = frozenset()
    allow_unknown_unprefixed: bool = True


@dataclass(frozen=True)
class ResolvedHeader:
    entity: str
    source_profile: str
    original_tokens: tuple[str, ...]
    normalized_tokens: tuple[str, ...]
    original_to_normalized: tuple[tuple[str, str], ...]
    unknown_columns: tuple[str, ...]


NAME_USAGE_PROFILE = EntityHeaderProfile(
    entity="NameUsage",
    canonical_fields=frozenset(NAME_USAGE_FIELDS),
    required_fields=frozenset({
        "ID", "parentID", "status", "scientificName", "authorship", "rank",
    }),
    optional_known_fields=frozenset(NAME_USAGE_FIELDS) - {
        "ID", "parentID", "status", "scientificName", "authorship", "rank",
    },
    allowed_namespace_profiles=frozenset({
        CANONICAL_PROFILE, CHECKLISTBANK_COL_XR_PROFILE,
    }),
    known_opaque_extensions=frozenset({"clb:merged"}),
)

ENTITY_PROFILES = {"NameUsage": NAME_USAGE_PROFILE}


def _validate_token_shape(token: str) -> None:
    if not token or token != token.strip():
        raise AcquisitionError(f"unsafe header token whitespace or empty name: {token!r}")
    if unicodedata.normalize("NFC", token) != token:
        raise AcquisitionError(f"non-canonical Unicode header token: {token!r}")
    if any(unicodedata.category(char) in CONTROL_OR_FORMAT for char in token):
        raise AcquisitionError(f"control or invisible character in header token: {token!r}")
    if PERCENT_DELIMITER.search(token):
        raise AcquisitionError(f"percent-encoded header delimiter is forbidden: {token!r}")
    if "\uff1a" in token or "\ua789" in token or "\ufe55" in token:
        raise AcquisitionError(f"confusable Unicode colon in header token: {token!r}")


def resolve_entity_header(
    entity: str,
    tokens: Iterable[str],
    *,
    source_profile: str,
) -> ResolvedHeader:
    try:
        profile = ENTITY_PROFILES[entity]
    except KeyError as exc:
        raise AcquisitionError(f"no ColDP header profile for entity: {entity}") from exc
    if source_profile not in profile.allowed_namespace_profiles:
        raise AcquisitionError(f"unapproved source header profile: {source_profile}")
    originals = tuple(tokens)
    normalized: list[str] = []
    unknown: list[str] = []
    mappings: list[tuple[str, str]] = []
    seen_original: dict[str, str] = {}
    seen_normalized: dict[str, str] = {}
    for token in originals:
        _validate_token_shape(token)
        original_key = unicodedata.normalize("NFC", token).casefold()
        if original_key in seen_original:
            raise AcquisitionError(
                f"duplicate or case-fold-colliding header tokens: "
                f"{seen_original[original_key]!r}, {token!r}"
            )
        seen_original[original_key] = token
        if token in profile.canonical_fields:
            value = token
        elif token.startswith("col:"):
            if source_profile != CHECKLISTBANK_COL_XR_PROFILE:
                raise AcquisitionError("col: headers require the pinned ChecklistBank profile")
            local = token[4:]
            if not local or ":" in local or local not in profile.canonical_fields:
                raise AcquisitionError(f"unknown or nested col: header term: {token!r}")
            value = local
        elif token in profile.known_opaque_extensions:
            value = token
            unknown.append(token)
        elif ":" in token:
            raise AcquisitionError(f"unapproved header namespace or deceptive prefix: {token!r}")
        elif token in profile.required_fields:
            value = token
        elif profile.allow_unknown_unprefixed:
            value = token
            unknown.append(token)
        else:
            raise AcquisitionError(f"unknown header field: {token!r}")
        normalized_key = unicodedata.normalize("NFC", value).casefold()
        if normalized_key in seen_normalized:
            raise AcquisitionError(
                f"header normalization collision: {seen_normalized[normalized_key]!r}, "
                f"{token!r} -> {value!r}"
            )
        seen_normalized[normalized_key] = token
        normalized.append(value)
        mappings.append((token, value))
    missing = profile.required_fields - set(normalized)
    if missing:
        raise AcquisitionError(f"{entity} lacks exact required fields: {sorted(missing)}")
    return ResolvedHeader(
        entity=entity,
        source_profile=source_profile,
        original_tokens=originals,
        normalized_tokens=tuple(normalized),
        original_to_normalized=tuple(mappings),
        unknown_columns=tuple(unknown),
    )
