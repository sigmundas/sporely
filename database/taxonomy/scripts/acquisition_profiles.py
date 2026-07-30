"""Source-specific validation profiles sharing the Stage 2 acquisition boundary."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AcquisitionProfile:
    code: str
    source_code: str
    archive_format: str
    metadata_contract: str


PROFILES = {
    "col_xr": AcquisitionProfile(
        code="col_xr",
        source_code="col_xr",
        archive_format="ColDP",
        metadata_contract="COL-specific metadata.yaml and ColDP validation",
    ),
    "nortaxa_dwca": AcquisitionProfile(
        code="nortaxa_dwca",
        source_code="nortaxa",
        archive_format="Darwin Core Archive",
        metadata_contract="DwC-A meta.xml-driven Taxon and VernacularName validation",
    ),
}


def get_profile(code: str) -> AcquisitionProfile:
    try:
        return PROFILES[code]
    except KeyError as exc:
        raise ValueError(f"unknown acquisition profile: {code}") from exc
