import io
import json
import socket
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from refresh_nortaxa import (  # noqa: E402
    AcquisitionError,
    NorTaxaRequest,
    load_request,
    load_proposal,
    parse_meta,
    plan,
    validate_fixture,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nortaxa"
NS = "http://rs.tdwg.org/dwc/text/"
DWC = "http://rs.tdwg.org/dwc/terms/"
GBIF = "http://rs.gbif.org/terms/1.0/"


def request_raw() -> dict:
    return json.loads((FIXTURES / "valid-request.json").read_text(encoding="utf-8"))


def request() -> NorTaxaRequest:
    return load_request(FIXTURES / "valid-request.json")


def proposal():
    return load_proposal()


CORE_TERMS = [
    "taxonID", "acceptedNameUsageID", "parentNameUsageID", "scientificName",
    "scientificNameAuthorship", "taxonRank", "taxonomicStatus", "kingdom",
    "family", "genus", "specificEpithet", "scientificNameID",
]
VERN_TERMS = ["vernacularName", "language", "countryCode", "isPreferredName", "source"]


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("NorTaxa fixture tests attempted a real network operation")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


def meta_xml(
    *,
    core_file: str = "data/nonstandard-core.csv",
    vern_file: str = "names/localized.data",
    delimiter: str = "\\,",
    lines: str = "\\n",
    core_row_type: str = DWC + "Taxon",
    include_id: bool = True,
    include_coreid: bool = True,
    core_terms: list[str] | None = None,
    vern_terms: list[str] | None = None,
    malformed_index: str | None = None,
    include_distribution: bool = True,
    distribution_row_type: str | None = None,
    extra_extension: tuple[str, str] | None = None,
) -> bytes:
    core_terms = CORE_TERMS if core_terms is None else core_terms
    vern_terms = VERN_TERMS if vern_terms is None else vern_terms
    core_fields = []
    for offset, term in enumerate(core_terms, 1):
        index = malformed_index if offset == 1 and malformed_index is not None else str(offset)
        core_fields.append(f'<field index="{index}" term="{DWC}{term}"/>')
    vern_fields = "".join(
        f'<field index="{offset}" term="{GBIF if term in {"vernacularName", "isPreferredName"} else DWC}{term}"/>'
        for offset, term in enumerate(vern_terms, 1)
    )
    distribution_row_type = distribution_row_type or (DWC + "Distribution")
    distribution = (
        f'<extension rowType="{distribution_row_type}" encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="{lines}" '
        'ignoreHeaderLines="1"><files><location>extra/distribution.tsv</location></files>'
        f'<coreid index="0"/><field index="1" term="{DWC}countryCode"/></extension>'
        if include_distribution else ""
    )
    extra_extension_xml = ""
    if extra_extension is not None:
        extra_row_type, extra_location = extra_extension
        extra_extension_xml = (
            f'<extension rowType="{extra_row_type}" encoding="UTF-8" '
            f'fieldsTerminatedBy="\\t" linesTerminatedBy="{lines}" ignoreHeaderLines="1">'
            f'<files><location>{extra_location}</location></files>'
            f'<coreid index="0"/><field index="1" term="{DWC}countryCode"/></extension>'
        )
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<archive xmlns="{NS}">'
        f'<core rowType="{core_row_type}" encoding="UTF-8" fieldsTerminatedBy="{delimiter}" linesTerminatedBy="{lines}" '
        f'fieldsEnclosedBy="&quot;" ignoreHeaderLines="2"><files><location>{core_file}</location></files>'
        f'{"<id index=\"0\"/>" if include_id else ""}{"".join(core_fields)}</core>'
        f'<extension rowType="{GBIF}VernacularName" encoding="UTF-8" fieldsTerminatedBy="{delimiter}" linesTerminatedBy="{lines}" '
        f'fieldsEnclosedBy="&quot;" ignoreHeaderLines="1"><files><location>{vern_file}</location></files>'
        f'{"<coreid index=\"0\"/>" if include_coreid else ""}{vern_fields}</extension>'
        f'{distribution}{extra_extension_xml}</archive>'
    ).encode("utf-8")


def archive_bytes(
    *,
    meta: bytes | None = None,
    core_file: str = "data/nonstandard-core.csv",
    vern_file: str = "names/localized.data",
    core_rows: list[list[str]] | None = None,
    vern_rows: list[list[str]] | None = None,
    core_final_newline: bool = False,
    vern_final_newline: bool = True,
    line_ending: str = "\n",
    extra: dict[str, bytes] | None = None,
) -> bytes:
    core_rows = core_rows or [
        ["row-R", "taxon:root", "", "", "Fungi", "", "kingdom",
         "accepted", "Fungi", "", "", "", "NBIC:root"],
        ["row-G", "taxon:genus", "", "taxon:root", "Candolleomyces", "", "genus",
         "accepted", "Fungi", "Psathyrellaceae", "Candolleomyces", "", "NBIC:genus"],
        ["row-A", "taxon:accepted", "", "taxon:genus",
         "Candolleomyces candolleanus", "(Fr.) D. Wächt. & A. Melzer", "species",
         "accepted", "Fungi", "Psathyrellaceae", "Candolleomyces", "candolleanus", "NBIC:54995"],
        ["row-S", "taxon:synonym", "taxon:accepted", "taxon:genus",
         "Psathyrella candolleana", "(Fr.) Maire", "species", "synonym",
         "Fungi", "Psathyrellaceae", "Psathyrella", "candolleana", "NBIC:old-54995"],
    ]
    vern_rows = vern_rows or [
        ["row-A", "hvit sprøsopp", "nb", "NO", "true", "Artsnavnebasen"],
        ["row-A", "kvit sprøsopp", "nn", "NO", "false", "Artsnavnebasen"],
        ["row-A", "vilges čuovkkusgussaguoppar", "se", "NO", "true", "synthetic Sámi"],
    ]

    def csv_text(headers: list[str], rows: list[list[str]], ignored: int, final: bool) -> bytes:
        stream = io.StringIO(newline="")
        writer = __import__("csv").writer(stream, delimiter=",", quotechar='"', lineterminator=line_ending)
        for number in range(ignored):
            writer.writerow([f"header-{number}", *headers])
        writer.writerows(rows)
        value = stream.getvalue()
        if not final:
            value = value.removesuffix(line_ending)
        return value.encode("utf-8")

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        def write(name: str, payload: bytes | str) -> None:
            info = zipfile.ZipInfo(name, date_time=(2026, 7, 17, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(info, payload)

        if meta is not None:
            write("meta.xml", meta)
        core_headers = ["id", *CORE_TERMS]
        vern_headers = ["coreid", *VERN_TERMS]
        write(core_file, csv_text(core_headers, core_rows, 2, core_final_newline))
        write(vern_file, csv_text(vern_headers, vern_rows, 1, vern_final_newline))
        write("extra/distribution.tsv", f"header{line_ending}row-A\tNO")
        write("eml.xml", '<?xml version="1.0" encoding="UTF-8"?><eml>synthetic &amp; escaped</eml>')
        for name, payload in (extra or {}).items():
            write(name, payload)
    return output.getvalue()


def write_archive(tmp_path: Path, **kwargs) -> Path:
    path = tmp_path / "fixture.zip"
    path.write_bytes(archive_bytes(**kwargs))
    return path


def test_valid_fixture_is_meta_driven_and_preserves_identifier_roles(tmp_path: Path) -> None:
    report = validate_fixture(write_archive(tmp_path, meta=meta_xml()), request())
    assert report["result"] == "passed"
    assert report["record_counts"] == {"Distribution": 1, "Taxon": 4, "VernacularName": 3}
    assert report["meta_xml"]["core_location"] == "data/nonstandard-core.csv"
    assert report["identifier_contract"]["core_row_id"]["role"] != report["identifier_contract"]["dwc:taxonID"]["role"]
    assert report["identifier_contract"]["NBIC_scientific_name_id"]["role"].startswith("namespaced")
    assert report["network_calls"] == 0


def test_tracked_fixture_and_machine_evidence_match() -> None:
    report = validate_fixture(FIXTURES / "valid-dwca.zip", request())
    evidence = json.loads((FIXTURES / "valid-dwca.validation.json").read_text(encoding="utf-8"))
    for key in ("archive", "linkage", "record_counts", "request_definition_sha256", "result"):
        assert report[key] == evidence[key]


def test_tab_delimited_reordered_columns_and_final_newline_variant(tmp_path: Path) -> None:
    terms = list(reversed(CORE_TERMS))
    vern_terms = list(reversed(VERN_TERMS))
    original_core = archive_bytes  # prove fixture builder remains local/offline
    assert original_core
    core_by_term = dict(zip(CORE_TERMS, archive_rows()[0][1:]))
    vern_by_term = dict(zip(VERN_TERMS, vernacular_rows()[0][1:]))
    meta = meta_xml(delimiter="\\t", core_terms=terms, vern_terms=vern_terms, include_distribution=False)
    rows = [["row-A", *(core_by_term[term] for term in terms)]]
    vrows = [["row-A", *(vern_by_term[term] for term in vern_terms)]]
    payload = tab_archive(meta, rows, vrows)
    path = tmp_path / "tab.zip"
    path.write_bytes(payload)
    assert validate_fixture(path, request())["record_counts"]["Taxon"] == 1


def archive_rows() -> list[list[str]]:
    return [["row-A", "taxon:accepted", "", "",
             "Candolleomyces candolleanus", "(Fr.) D. Wächt. & A. Melzer", "species",
             "accepted", "Fungi", "Psathyrellaceae", "Candolleomyces", "candolleanus", "NBIC:54995"]]


def vernacular_rows() -> list[list[str]]:
    return [["row-A", "hvit sprøsopp", "nb", "NO", "true", "synthetic"]]


def tab_archive(meta: bytes, core: list[list[str]], vern: list[list[str]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("meta.xml", meta)
        archive.writestr("data/nonstandard-core.csv",
                         "ignored\tcolumns\nignored\tcolumns\n" + "\n".join("\t".join(row) for row in core))
        archive.writestr("names/localized.data",
                         "ignored\tcolumns\n" + "\n".join("\t".join(row) for row in vern) + "\n")
    return output.getvalue()


def test_request_is_deterministic_unapproved_and_immutable(tmp_path: Path) -> None:
    first = NorTaxaRequest.from_dict(request_raw(), proposal())
    reordered = NorTaxaRequest.from_dict(dict(reversed(list(request_raw().items()))), proposal())
    assert first.request_sha256 == reordered.request_sha256
    target, idempotent = plan(first, tmp_path)
    assert not idempotent and not (target / "archive.zip").exists()
    persisted_request = json.loads((target / "request.json").read_text())
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["state"] == "planned"
    assert persisted_request["source_selection_proposal_sha256"] == proposal().canonical_sha256
    assert manifest["source_selection_proposal_sha256"] == proposal().canonical_sha256
    assert manifest["request_definition_sha256"] == first.request_sha256
    assert plan(first, tmp_path)[1]


@pytest.mark.parametrize("endpoint", [
    "https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase",
    "https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=latest",
    "http://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.284",
    "https://example.org/archive.do?r=artsnavnebase&v=1.284",
])
def test_unversioned_latest_or_unofficial_endpoint_rejected(endpoint: str) -> None:
    raw = request_raw()
    raw["archive_endpoint"] = endpoint
    with pytest.raises(AcquisitionError):
        NorTaxaRequest.from_dict(raw, proposal())


@pytest.mark.parametrize("field", ["token", "authorization", "client_secret"])
def test_secret_bearing_request_rejected(field: str) -> None:
    raw = request_raw()
    raw[field] = "fixture-secret"
    with pytest.raises(AcquisitionError, match="secret"):
        NorTaxaRequest.from_dict(raw, proposal())


def test_proposal_cannot_be_marked_approved() -> None:
    raw = request_raw()
    raw["approval_status"] = "approved"
    raw["download_authorized"] = True
    with pytest.raises(AcquisitionError, match="proposed"):
        NorTaxaRequest.from_dict(raw, proposal())


@pytest.mark.parametrize(
    ("meta", "message"),
    [
        (b"<archive>", "malformed"),
        (b'<!DOCTYPE archive SYSTEM "https://example.org/x"><archive/>', "DOCTYPE"),
        (meta_xml(core_file="../taxon.txt"), "unsafe"),
        (meta_xml(core_file="/taxon.txt"), "unsafe"),
        (meta_xml(core_row_type=DWC + "Occurrence"), "Taxon"),
        (meta_xml(include_id=False), "<id>"),
        (meta_xml(include_coreid=False), "<coreid>"),
        (meta_xml(malformed_index="x"), "invalid"),
        (meta_xml(malformed_index="0"), "duplicate"),
        (meta_xml(core_terms=[term for term in CORE_TERMS if term != "taxonID"]), "required"),
        (meta_xml().replace(b'<core rowType="' + (DWC + "Taxon").encode() + b'" encoding="UTF-8"',
                            b'<core rowType="' + (DWC + "Taxon").encode() + b'" encoding="ISO-8859-1"', 1),
         "encoding"),
        (meta_xml().replace(b'fieldsTerminatedBy="\\,"', b'fieldsTerminatedBy="|"', 1), "delimiter"),
    ],
)
def test_invalid_meta_contracts_rejected(meta: bytes, message: str) -> None:
    with pytest.raises(AcquisitionError, match=message):
        parse_meta(meta)


def test_missing_and_nonroot_or_duplicate_meta_rejected(tmp_path: Path) -> None:
    with pytest.raises(AcquisitionError, match="meta.xml"):
        validate_fixture(write_archive(tmp_path, meta=None), request())
    path = tmp_path / "nested.zip"
    path.write_bytes(archive_bytes(meta=None, extra={"nested/meta.xml": meta_xml()}))
    with pytest.raises(AcquisitionError, match="root meta.xml"):
        validate_fixture(path, request())


def test_missing_declared_table_rejected(tmp_path: Path) -> None:
    path = write_archive(tmp_path, meta=meta_xml(core_file="absent.csv"))
    with pytest.raises(AcquisitionError, match="missing"):
        validate_fixture(path, request())


def test_malformed_width_duplicate_core_and_orphan_links_rejected(tmp_path: Path) -> None:
    malformed = archive_rows()[0][:-1]
    with pytest.raises(AcquisitionError, match="width|declared index"):
        validate_fixture(write_archive(tmp_path, meta=meta_xml(), core_rows=[malformed]), request())
    duplicate = archive_rows() * 2
    with pytest.raises(AcquisitionError, match="duplicate core"):
        validate_fixture(write_archive(tmp_path, meta=meta_xml(), core_rows=duplicate), request())
    with pytest.raises(AcquisitionError, match="orphan"):
        validate_fixture(write_archive(tmp_path, meta=meta_xml(), core_rows=archive_rows(),
                                       vern_rows=[["missing", "name", "nb", "NO", "true", "fixture"]]), request())


def test_invalid_utf8_unsafe_member_and_symlink_rejected(tmp_path: Path) -> None:
    bad_utf8 = archive_bytes(meta=meta_xml(), extra={})
    source = io.BytesIO(bad_utf8)
    output = io.BytesIO()
    with zipfile.ZipFile(source) as incoming, zipfile.ZipFile(output, "w") as outgoing:
        for member in incoming.infolist():
            payload = incoming.read(member)
            if member.filename == "names/localized.data":
                payload += b"\xff"
            outgoing.writestr(member, payload)
    path = tmp_path / "utf8.zip"
    path.write_bytes(output.getvalue())
    with pytest.raises(AcquisitionError, match="invalid utf-8"):
        validate_fixture(path, request())

    path.write_bytes(archive_bytes(meta=meta_xml(), extra={"../escape": b"x"}))
    with pytest.raises(AcquisitionError, match="unsafe ZIP"):
        validate_fixture(path, request())

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("meta.xml", meta_xml())
        link = zipfile.ZipInfo("data/nonstandard-core.csv")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, "target")
    path.write_bytes(output.getvalue())
    with pytest.raises(AcquisitionError, match="symlink"):
        validate_fixture(path, request())


def test_member_count_and_compression_ratio_limits(tmp_path: Path) -> None:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for index in range(101):
            archive.writestr(f"f{index}", b"x")
    path = tmp_path / "many.zip"
    path.write_bytes(output.getvalue())
    with pytest.raises(AcquisitionError, match="member count"):
        validate_fixture(path, request())

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("bomb", b"A" * 1_000_000)
    path.write_bytes(output.getvalue())
    with pytest.raises(AcquisitionError, match="compression ratio"):
        validate_fixture(path, request())


def test_no_network_api_or_live_download_command_exists() -> None:
    source = (SCRIPTS / "refresh_nortaxa.py").read_text(encoding="utf-8")
    assert "urlopen" not in source
    assert "requests." not in source
    assert "download" not in {action for action in ("validate-request", "normalize-request", "plan",
                                                     "validate-fixture", "status")}


def test_higher_taxa_roots_and_synonym_semantics(tmp_path: Path) -> None:
    report = validate_fixture(write_archive(tmp_path, meta=meta_xml()), request())
    samples = report["identifier_contract"]["raw_samples"]
    root = next(item for item in samples if item["core_row_id"] == "row-R")
    accepted = next(item for item in samples if item["core_row_id"] == "row-A")
    synonym = next(item for item in samples if item["core_row_id"] == "row-S")
    assert root["dwc:acceptedNameUsageID"] == root["dwc:parentNameUsageID"] == ""
    assert accepted["dwc:acceptedNameUsageID"] == ""
    assert synonym["dwc:acceptedNameUsageID"] == "taxon:accepted"


def test_rank_and_status_aware_required_values(tmp_path: Path) -> None:
    species = archive_rows()[0]
    species[10] = ""
    with pytest.raises(AcquisitionError, match="species Taxon row requires genus"):
        validate_fixture(write_archive(tmp_path, meta=meta_xml(), core_rows=[species]), request())
    synonym = archive_rows()[0]
    synonym[1] = "taxon:synonym"
    synonym[7] = "synonym"
    with pytest.raises(AcquisitionError, match="requires acceptedNameUsageID"):
        validate_fixture(write_archive(tmp_path, meta=meta_xml(), core_rows=[synonym]), request())


def test_streaming_parser_does_not_read_or_materialize_complete_tables() -> None:
    source = (SCRIPTS / "refresh_nortaxa.py").read_text(encoding="utf-8")
    assert "archive.read(table.location)" not in source
    assert "list(reader)" not in source
    assert "stream.read(64 * 1024)" in source


def test_future_explicit_version_is_data_driven(tmp_path: Path) -> None:
    proposal_raw = json.loads(
        (Path(__file__).resolve().parents[1] / "nortaxa-source-selection.proposal.json").read_text()
    )
    proposal_raw["version"] = "2.7"
    proposal_raw["issued_date"] = "2027-01-02"
    for key in ("archive_endpoint", "eml_endpoint", "resource_page"):
        proposal_raw["delivery"][key] = proposal_raw["delivery"][key].replace("v=1.284", "v=2.7")
    proposal_path = tmp_path / "proposal.json"
    proposal_path.write_text(json.dumps(proposal_raw), encoding="utf-8")
    selected = load_proposal(proposal_path)
    raw = request_raw()
    for key in (
        "version", "issued_date", "archive_endpoint", "eml_endpoint", "resource_page",
    ):
        raw[key] = getattr(selected, key)
    raw["source_selection_proposal_sha256"] = selected.canonical_sha256
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps(raw), encoding="utf-8")
    assert load_request(request_path, proposal_path).version == "2.7"
    source = (SCRIPTS / "refresh_nortaxa.py").read_text(encoding="utf-8")
    assert "1.284" not in source and "2026-07-17" not in source


def test_proposal_hash_and_selection_mismatches_are_rejected(tmp_path: Path) -> None:
    raw = request_raw()
    raw["source_selection_proposal_sha256"] = "0" * 64
    with pytest.raises(AcquisitionError, match="proposal_sha256"):
        NorTaxaRequest.from_dict(raw, proposal())
    raw = request_raw()
    raw["title"] = "Different selection"
    with pytest.raises(AcquisitionError, match="title"):
        NorTaxaRequest.from_dict(raw, proposal())


def test_declared_crlf_line_terminator_and_mismatch(tmp_path: Path) -> None:
    path = write_archive(tmp_path, meta=meta_xml(lines="\\r\\n"), line_ending="\r\n")
    assert validate_fixture(path, request())["record_counts"]["Taxon"] == 4
    path = write_archive(tmp_path, meta=meta_xml(lines="\\r\\n"), line_ending="\n")
    with pytest.raises(AcquisitionError, match="linesTerminatedBy"):
        validate_fixture(path, request())


def test_unmapped_physical_columns_are_allowed_but_width_must_be_consistent(tmp_path: Path) -> None:
    row = [*archive_rows()[0], "unmapped-value"]
    report = validate_fixture(
        write_archive(tmp_path, meta=meta_xml(), core_rows=[row]),
        request(),
    )
    assert report["record_counts"]["Taxon"] == 1
    with pytest.raises(AcquisitionError, match="physical row width"):
        validate_fixture(
            write_archive(tmp_path, meta=meta_xml(), core_rows=[row, archive_rows()[0]]),
            request(),
        )


@pytest.mark.parametrize("suffix", ["&v=1.284", "#fragment", ":8443"])
def test_duplicate_query_fragment_and_nonstandard_port_rejected(tmp_path: Path, suffix: str) -> None:
    raw = json.loads(
        (Path(__file__).resolve().parents[1] / "nortaxa-source-selection.proposal.json").read_text()
    )
    endpoint = raw["delivery"]["archive_endpoint"]
    if suffix == ":8443":
        endpoint = endpoint.replace("ipt.artsdatabanken.no", "ipt.artsdatabanken.no:8443")
    else:
        endpoint += suffix
    raw["delivery"]["archive_endpoint"] = endpoint
    proposal_path = tmp_path / "proposal.json"
    proposal_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(AcquisitionError):
        load_proposal(proposal_path)


def test_plan_cli_reports_created_then_idempotent_not_dry_run(tmp_path: Path) -> None:
    command = [
        sys.executable, str(SCRIPTS / "refresh_nortaxa.py"), "plan",
        str(FIXTURES / "valid-request.json"), "--sources-root", str(tmp_path),
    ]
    created = json.loads(subprocess.run(command, check=True, capture_output=True, text=True).stdout)
    repeated = json.loads(subprocess.run(command, check=True, capture_output=True, text=True).stdout)
    assert created["created"] is True and created["idempotent"] is False
    assert repeated["created"] is False and repeated["idempotent"] is True
    assert "dry_run" not in created


# ----- Distribution extension namespace handling (nortaxa_dwca) -----


def test_gbif_namespace_distribution_extension_is_accepted(tmp_path: Path) -> None:
    """The pinned NorTaxa 1.284 archive uses the GBIF Distribution row-type
    namespace. The profile allowlist recognizes both DwC and GBIF namespaces."""
    report = validate_fixture(
        write_archive(tmp_path, meta=meta_xml(distribution_row_type=GBIF + "Distribution")),
        request(),
    )
    assert report["result"] == "passed"
    assert report["record_counts"] == {"Distribution": 1, "Taxon": 4, "VernacularName": 3}
    # The Distribution row-type identity is preserved in the meta_xml evidence.
    exts = report["meta_xml"]["extensions"]
    assert any(e["row_type"] == GBIF + "Distribution" for e in exts)


def test_dwc_namespace_distribution_extension_still_accepted(tmp_path: Path) -> None:
    report = validate_fixture(
        write_archive(tmp_path, meta=meta_xml(distribution_row_type=DWC + "Distribution")),
        request(),
    )
    assert report["result"] == "passed"
    assert report["record_counts"]["Distribution"] == 1


def test_missing_required_row_types_fails(tmp_path: Path) -> None:
    """Removing VernacularName is fatal even when Distribution is present.

    Uses real XML parsing to REMOVE the VernacularName extension node (not
    rename it), so parse_meta observes a genuinely absent extension and the
    'exactly one VernacularName extension is required' gate fires.
    """
    from xml.etree import ElementTree as ET
    meta = meta_xml(include_distribution=True)
    ns = "http://rs.tdwg.org/dwc/text/"
    ET.register_namespace("", ns)
    root = ET.fromstring(meta)
    removed = 0
    for ext in list(root.findall(f"{{{ns}}}extension")):
        if ext.attrib.get("rowType") == "http://rs.gbif.org/terms/1.0/VernacularName":
            root.remove(ext)
            removed += 1
    assert removed == 1, "test fixture must contain exactly one VernacularName extension"
    # The VernacularName extension is now truly absent from the parse tree.
    stripped = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    with pytest.raises(AcquisitionError, match="exactly one VernacularName"):
        parse_meta(stripped)


def test_multiple_distribution_extensions_rejected(tmp_path: Path) -> None:
    """Policy: exactly one Distribution extension may be present. A meta.xml
    that declares two Distribution extensions (either namespace, in any
    combination) is rejected as a structural error, not silently aggregated.
    """
    from xml.etree import ElementTree as ET
    meta = meta_xml(distribution_row_type=DWC + "Distribution")
    ns = "http://rs.tdwg.org/dwc/text/"
    ET.register_namespace("", ns)
    root = ET.fromstring(meta)
    original = next(
        ext for ext in root.findall(f"{{{ns}}}extension")
        if ext.attrib.get("rowType") == DWC + "Distribution"
    )
    duplicate = ET.SubElement(root, f"{{{ns}}}extension", {
        "rowType": GBIF + "Distribution", "encoding": "UTF-8",
        "fieldsTerminatedBy": "\\t", "linesTerminatedBy": "\\n",
        "ignoreHeaderLines": "1",
    })
    files = ET.SubElement(duplicate, f"{{{ns}}}files")
    ET.SubElement(files, f"{{{ns}}}location").text = "extra/distribution.tsv"
    ET.SubElement(duplicate, f"{{{ns}}}coreid", {"index": "0"})
    ET.SubElement(duplicate, f"{{{ns}}}field", {"index": "1", "term": DWC + "countryCode"})
    payload = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    with pytest.raises(AcquisitionError):
        parse_meta(payload)


def test_unknown_extension_still_rejected(tmp_path: Path) -> None:
    """An unrelated extension row type (not Vernacular, not Distribution in
    either allowlisted namespace) must fail parse_meta."""
    with pytest.raises(AcquisitionError, match="unsupported extension row type"):
        parse_meta(meta_xml(extra_extension=(GBIF + "Reference", "extra/references.tsv")))


def test_distribution_referencing_missing_file_fails(tmp_path: Path) -> None:
    """If meta.xml declares Distribution at a location that does not exist as a
    ZIP member, validation fails at the ZIP-member existence check."""
    payload = archive_bytes(
        meta=meta_xml(distribution_row_type=GBIF + "Distribution"),
    )
    # Rebuild the ZIP without the Distribution table.
    import io as _io, zipfile as _zip
    src = _zip.ZipFile(_io.BytesIO(payload), "r")
    out = _io.BytesIO()
    with _zip.ZipFile(out, "w", compression=_zip.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            if info.filename == "extra/distribution.tsv":
                continue
            dst.writestr(info, src.read(info.filename))
    path = tmp_path / "no-distribution-file.zip"
    path.write_bytes(out.getvalue())
    with pytest.raises(AcquisitionError):
        validate_fixture(path, request())


def test_distribution_unsafe_location_rejected(tmp_path: Path) -> None:
    """A Distribution `<location>` outside the archive root fails the safe-path check."""
    unsafe = meta_xml(distribution_row_type=GBIF + "Distribution").replace(
        b"<location>extra/distribution.tsv</location>",
        b"<location>../evil.tsv</location>",
    )
    with pytest.raises(AcquisitionError, match="unsafe DwC-A location"):
        parse_meta(unsafe)


def test_distribution_data_is_not_extracted_to_disk(tmp_path: Path) -> None:
    """The validator streams Distribution rows in memory only; no file is
    written or extracted beside the archive."""
    archive_path = write_archive(tmp_path, meta=meta_xml(distribution_row_type=GBIF + "Distribution"))
    report = validate_fixture(archive_path, request())
    assert report["result"] == "passed"
    # Only the archive fixture itself exists in tmp_path — no extraction.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fixture.zip"]


def test_distribution_orphan_core_id_still_fails(tmp_path: Path) -> None:
    """Distribution row-linkage check still fires under the GBIF namespace."""
    payload = archive_bytes(
        meta=meta_xml(distribution_row_type=GBIF + "Distribution"),
    )
    # Rewrite the distribution table with an orphan core id.
    import io as _io, zipfile as _zip
    src = _zip.ZipFile(_io.BytesIO(payload), "r")
    out = _io.BytesIO()
    with _zip.ZipFile(out, "w", compression=_zip.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            if info.filename == "extra/distribution.tsv":
                dst.writestr(info, "header\nrow-DOES-NOT-EXIST\tNO")
            else:
                dst.writestr(info, src.read(info.filename))
    path = tmp_path / "orphan-distribution.zip"
    path.write_bytes(out.getvalue())
    with pytest.raises(AcquisitionError, match="orphan Distribution core ID"):
        validate_fixture(path, request())
