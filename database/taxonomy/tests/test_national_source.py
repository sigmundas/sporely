"""Focused offline tests for the national-source adapter CLI."""
from __future__ import annotations

import hashlib
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

import national_source as subject  # noqa: E402
from national_source import NationalSourceError  # noqa: E402


TAX = Path(__file__).resolve().parents[1]
EXAMPLE_DIR = TAX / "national_sources" / "example"
EXAMPLE_PROFILE = EXAMPLE_DIR / "source.json"
EXAMPLE_FIXTURE = EXAMPLE_DIR / "fixture.zip"


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def blocked(*a, **kw):
        raise AssertionError("national-source adapter test attempted network access")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


# ----- profile schema -----


def test_profile_skeleton_is_valid_and_reloadable(tmp_path: Path) -> None:
    skel = subject.profile_skeleton("denmark")
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    prof = subject.load_profile(p)
    assert prof.source_code == "denmark"
    assert prof.core_row_type == subject.DWC_TAXON
    assert prof.identifier_namespace == "DENMARK:"
    assert prof.distribution_row_type is not None
    assert prof.distribution_validation_only is True


@pytest.mark.parametrize("bad_code", ["Denmark", "d", "", "with space", "1abc"])
def test_source_code_must_be_lowercase_short_identifier(tmp_path: Path, bad_code: str) -> None:
    skel = subject.profile_skeleton("denmark")
    skel["source_code"] = bad_code
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    with pytest.raises(NationalSourceError, match="source_code"):
        subject.load_profile(p)


def test_distribution_import_flag_must_be_true(tmp_path: Path) -> None:
    skel = subject.profile_skeleton("denmark")
    skel["distribution"]["validation_only"] = False
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    with pytest.raises(NationalSourceError, match="validation_only"):
        subject.load_profile(p)


def test_unknown_distribution_row_type_rejected(tmp_path: Path) -> None:
    skel = subject.profile_skeleton("denmark")
    skel["distribution"]["row_type"] = "http://example.com/Distribution"
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    with pytest.raises(NationalSourceError, match="distribution.row_type"):
        subject.load_profile(p)


def test_profile_rejects_wrong_schema_version(tmp_path: Path) -> None:
    skel = subject.profile_skeleton("denmark")
    skel["profile_schema_version"] = 99
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    with pytest.raises(NationalSourceError, match="profile_schema_version"):
        subject.load_profile(p)


# ----- inspect -----


def test_inspect_example_archive_reports_declared_tables() -> None:
    report = subject.inspect_archive(EXAMPLE_FIXTURE)
    assert report["core"]["row_type"] == subject.DWC_TAXON
    assert report["core"]["location"] == "taxa.tsv"
    ext_row_types = {e["row_type"] for e in report["extensions"]}
    assert subject.GBIF_VERNACULAR in ext_row_types
    assert subject.GBIF_DISTRIBUTION in ext_row_types
    # Suggested mapping should include core required terms.
    for required in ("taxonID", "scientificName", "taxonRank", "taxonomicStatus"):
        assert required in report["suggested_core_term_mapping"]


def test_inspect_rejects_non_zip(tmp_path: Path) -> None:
    bogus = tmp_path / "not-a-zip.zip"
    bogus.write_bytes(b"not-a-zip")
    with pytest.raises(NationalSourceError, match="ZIP"):
        subject.inspect_archive(bogus)


def test_inspect_rejects_archive_without_meta_xml(tmp_path: Path) -> None:
    p = tmp_path / "no-meta.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("only.tsv", "id\n1\n")
    with pytest.raises(NationalSourceError, match="meta.xml"):
        subject.inspect_archive(p)


# ----- validate -----


def test_validate_example_passes() -> None:
    profile = subject.load_profile(EXAMPLE_PROFILE)
    report = subject.validate_archive(profile, EXAMPLE_FIXTURE)
    assert report["result"] == "passed"
    assert report["record_counts"]["Taxon"] == 4
    assert report["record_counts"]["VernacularName"] == 2
    assert report["record_counts"]["Distribution"] == 1
    assert report["distribution_imported"] is False


def test_validate_missing_vernacular_extension_fails(tmp_path: Path) -> None:
    profile = subject.load_profile(EXAMPLE_PROFILE)
    # Rebuild the archive without vernacular extension in meta.xml.
    with zipfile.ZipFile(EXAMPLE_FIXTURE, "r") as src:
        infos = {i.filename: src.read(i.filename) for i in src.infolist()}
    meta = infos["meta.xml"].decode("utf-8")
    meta = meta.replace(
        '<extension rowType="http://rs.gbif.org/terms/1.0/VernacularName"',
        '<!-- vernacular-removed --><extension rowType="http://example.com/removed"',
    )
    infos["meta.xml"] = meta.encode("utf-8")
    tampered = tmp_path / "no-vernacular.zip"
    with zipfile.ZipFile(tampered, "w") as dst:
        for name, payload in infos.items():
            dst.writestr(name, payload)
    with pytest.raises(NationalSourceError):
        subject.validate_archive(profile, tampered)


def test_validate_unknown_extension_fails(tmp_path: Path) -> None:
    """Adding an unrelated extension row type fails validate_archive."""
    from xml.etree import ElementTree as ET
    with zipfile.ZipFile(EXAMPLE_FIXTURE, "r") as src:
        infos = {i.filename: src.read(i.filename) for i in src.infolist()}
    ns = "http://rs.tdwg.org/dwc/text/"
    ET.register_namespace("", ns)
    root = ET.fromstring(infos["meta.xml"])
    extra = ET.SubElement(root, f"{{{ns}}}extension", {
        "rowType": "http://example.com/UnknownRowType",
        "encoding": "UTF-8", "fieldsTerminatedBy": "\\t",
        "linesTerminatedBy": "\\n", "ignoreHeaderLines": "1",
    })
    files = ET.SubElement(extra, f"{{{ns}}}files")
    ET.SubElement(files, f"{{{ns}}}location").text = "unknown.tsv"
    ET.SubElement(extra, f"{{{ns}}}coreid", {"index": "0"})
    ET.SubElement(extra, f"{{{ns}}}field", {"index": "1", "term": "http://example.com/x"})
    infos["meta.xml"] = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    infos["unknown.tsv"] = b"coreid\tx\nsp1\ty\n"
    tampered = tmp_path / "unknown-extension.zip"
    with zipfile.ZipFile(tampered, "w") as dst:
        for name, payload in infos.items():
            dst.writestr(name, payload)
    profile = subject.load_profile(EXAMPLE_PROFILE)
    with pytest.raises(NationalSourceError, match="unsupported extension"):
        subject.validate_archive(profile, tampered)


def test_validate_missing_required_core_term_fails(tmp_path: Path) -> None:
    profile_dict = json.loads(EXAMPLE_PROFILE.read_text())
    del profile_dict["core"]["term_mapping"]["scientificName"]
    p = tmp_path / "source.json"
    p.write_text(json.dumps(profile_dict))
    with pytest.raises(NationalSourceError, match="scientificName"):
        subject.validate_archive(subject.load_profile(p), EXAMPLE_FIXTURE)


# ----- normalize -----


def test_normalize_emits_expected_records(tmp_path: Path) -> None:
    profile = subject.load_profile(EXAMPLE_PROFILE)
    out = tmp_path / "example"
    report = subject.normalize_archive(profile, EXAMPLE_FIXTURE, out)
    assert report["result"] == "passed"
    assert report["record_counts"]["Taxon"] == 4
    assert report["record_counts"]["VernacularName"] == 2
    assert report["record_counts"]["Distribution"] == 1
    assert report["distribution_imported"] is False
    assert (out / "taxa.jsonl").exists()
    assert (out / "vernacular.jsonl").exists()
    assert (out / "report.json").exists()
    assert not (out / "distribution.jsonl").exists()
    # Namespaces are per-source and per-identity-role.
    ns = report["identifier_namespaces"]
    assert ns["core_row_id"]            == "example_dwc_id"
    assert ns["taxon_id"]               == "example_taxon_id"
    assert ns["accepted_name_usage_id"] == "example_accepted_name_usage_id"
    assert ns["parent_name_usage_id"]   == "example_parent_name_usage_id"

    taxa = [json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()]
    assert {t["core_row_id"]["value"] for t in taxa} == {"root", "gen", "sp1", "sp2"}
    assert {t["taxon_id"]["value"] for t in taxa} == {"XX:root", "XX:gen", "XX:sp1", "XX:sp2"}
    for t in taxa:
        assert t["source_code"] == "example"
        # Every emitted identifier is a text value under an explicit namespace.
        assert isinstance(t["core_row_id"]["value"], str)
        assert t["core_row_id"]["namespace"] == "example_dwc_id"
        assert isinstance(t["taxon_id"]["value"], str)
        assert t["taxon_id"]["namespace"] == "example_taxon_id"
        assert t["provenance"]["member"] == "taxa.tsv"
        assert isinstance(t["provenance"]["row_index"], int)

    sp1 = next(t for t in taxa if t["core_row_id"]["value"] == "sp1")
    assert sp1["scientific_name"] == "Examplaria minor"
    assert sp1["rank"] == "species"
    # DwC-A parent/accepted reference other rows' taxonID, not their <id>.
    assert sp1["parent_name_usage_id"] == {
        "value": "XX:gen", "namespace": "example_parent_name_usage_id",
    }
    assert sp1["accepted_name_usage_id"] is None
    sp2 = next(t for t in taxa if t["core_row_id"]["value"] == "sp2")
    assert sp2["taxonomic_status"] == "synonym"
    assert sp2["accepted_name_usage_id"] == {
        "value": "XX:sp1", "namespace": "example_accepted_name_usage_id",
    }

    verns = [json.loads(line) for line in (out / "vernacular.jsonl").read_text().splitlines()]
    assert {v["vernacular_name"] for v in verns} == {"small example", "lite eksempel"}
    preferred = next(v for v in verns if v["vernacular_name"] == "small example")
    assert preferred["is_preferred"] is True
    assert preferred["language"] == "en"
    # Vernacular <coreid> links the core row ID; it is NOT an accepted usage.
    assert preferred["core_row_id"] == {"value": "sp1", "namespace": "example_dwc_id"}
    assert "accepted_usage_id" not in preferred


def test_normalize_refuses_to_overwrite_output(tmp_path: Path) -> None:
    profile = subject.load_profile(EXAMPLE_PROFILE)
    out = tmp_path / "example"
    out.mkdir()
    with pytest.raises(FileExistsError):
        subject.normalize_archive(profile, EXAMPLE_FIXTURE, out)


def test_normalize_does_not_extract_archive_members(tmp_path: Path) -> None:
    """Normalized output contains only JSONL + report.json; no archive member
    is extracted to disk."""
    profile = subject.load_profile(EXAMPLE_PROFILE)
    out = tmp_path / "example"
    subject.normalize_archive(profile, EXAMPLE_FIXTURE, out)
    got = sorted(p.name for p in out.iterdir())
    assert got == ["report.json", "taxa.jsonl", "vernacular.jsonl"]


def test_normalize_preserves_id_prefixes_and_optional_external_ids(tmp_path: Path) -> None:
    """The XX: prefix must be preserved. External IDs listed in
    optional_external_id_terms round-trip under their FULL term URI so two
    URIs with the same DwC local name never collide."""
    profile_dict = json.loads(EXAMPLE_PROFILE.read_text())
    profile_dict["optional_external_id_terms"] = ["http://rs.tdwg.org/dwc/terms/taxonID"]
    p = tmp_path / "profile.json"
    p.write_text(json.dumps(profile_dict))
    out = tmp_path / "example"
    subject.normalize_archive(subject.load_profile(p), EXAMPLE_FIXTURE, out)
    sp1 = next(
        json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()
        if json.loads(line)["core_row_id"]["value"] == "sp1"
    )
    assert sp1["external_ids"] == {
        "http://rs.tdwg.org/dwc/terms/taxonID": "XX:sp1",
    }


# ----- id-identity rules -----


def test_ids_are_never_coerced_to_integers(tmp_path: Path) -> None:
    """Even fully-numeric IDs must round-trip as strings under their namespace.
    parent/accepted references point at taxonID, per DwC-A cross-referencing.
    """
    with zipfile.ZipFile(EXAMPLE_FIXTURE, "r") as src:
        infos = {i.filename: src.read(i.filename) for i in src.infolist()}
    infos["taxa.tsv"] = (
        b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
        b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
        b"root\tXX:root\t\t\tFungi\t\tkingdom\taccepted\n"
        b"12345\t12345\t\tXX:root\tNumericus specimen\tAuct.\tspecies\taccepted\n"
    )
    infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
    infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    p = tmp_path / "numeric-ids.zip"
    with zipfile.ZipFile(p, "w") as dst:
        for name, payload in infos.items():
            dst.writestr(name, payload)
    profile = subject.load_profile(EXAMPLE_PROFILE)
    out = tmp_path / "example"
    subject.normalize_archive(profile, p, out)
    numeric = next(
        json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()
        if json.loads(line)["scientific_name"] == "Numericus specimen"
    )
    assert numeric["core_row_id"] == {"value": "12345", "namespace": "example_dwc_id"}
    assert numeric["taxon_id"] == {"value": "12345", "namespace": "example_taxon_id"}
    assert isinstance(numeric["core_row_id"]["value"], str)


# ----- CLI end-to-end -----


def test_cli_init_creates_starter_profile(tmp_path: Path) -> None:
    out = tmp_path / "nowhere" / "source.json"
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "national_source.py"),
         "init", "denmark", "--output", str(out)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert out.exists()
    prof = subject.load_profile(out)
    assert prof.source_code == "denmark"


def test_cli_init_refuses_to_overwrite(tmp_path: Path) -> None:
    out = tmp_path / "source.json"
    out.write_text("{}")
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "national_source.py"),
         "init", "denmark", "--output", str(out)],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "already exists" in result.stderr


def test_cli_validate_example_passes() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "national_source.py"), "validate",
         "--profile", str(EXAMPLE_PROFILE), "--archive", str(EXAMPLE_FIXTURE)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout)
    assert out["result"] == "passed"


# ----- Negative tests introduced by the identity/robustness correction -----


def test_empty_identifier_namespace_rejected(tmp_path: Path) -> None:
    skel = subject.profile_skeleton("denmark")
    skel["identifier_namespace"] = ""
    p = tmp_path / "source.json"
    p.write_text(json.dumps(skel))
    with pytest.raises(NationalSourceError, match="identifier_namespace"):
        subject.load_profile(p)


def _tamper_archive(fixture: Path, output: Path, mutate) -> None:
    with zipfile.ZipFile(fixture, "r") as src:
        infos = {i.filename: src.read(i.filename) for i in src.infolist()}
    mutate(infos)
    with zipfile.ZipFile(output, "w") as dst:
        for name, payload in infos.items():
            dst.writestr(name, payload)


def test_duplicate_zip_member_rejected(tmp_path: Path) -> None:
    """A ZIP with two entries for the same filename fails _open_archive."""
    tampered = tmp_path / "dup-members.zip"
    with zipfile.ZipFile(EXAMPLE_FIXTURE, "r") as src, zipfile.ZipFile(tampered, "w") as dst:
        for info in src.infolist():
            dst.writestr(info, src.read(info.filename))
        dst.writestr("taxa.tsv", b"duplicated\n")  # inject a duplicate
    with pytest.raises(NationalSourceError, match="duplicate ZIP member"):
        subject.inspect_archive(tampered)


def test_referenced_member_missing_rejected(tmp_path: Path) -> None:
    """meta.xml declaring a table whose ZIP member is missing fails validate."""
    def drop_taxa(infos):
        infos.pop("taxa.tsv")
    tampered = tmp_path / "missing-taxa.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, drop_taxa)
    with pytest.raises(NationalSourceError, match="does not contain"):
        subject.validate_archive(subject.load_profile(EXAMPLE_PROFILE), tampered)


def test_malformed_meta_index_wrapped_as_national_source_error(tmp_path: Path) -> None:
    """A non-integer field index in meta.xml raises NationalSourceError (not
    ValueError propagated from int())."""
    def corrupt_meta(infos):
        meta = infos["meta.xml"].decode()
        infos["meta.xml"] = meta.replace('index="1" term', 'index="not-a-number" term', 1).encode()
    tampered = tmp_path / "bad-index.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, corrupt_meta)
    with pytest.raises(NationalSourceError, match="index"):
        subject.inspect_archive(tampered)


def test_multiple_vernacular_extensions_rejected(tmp_path: Path) -> None:
    """meta.xml declaring two VernacularName extensions is rejected."""
    def add_vern(infos):
        from xml.etree import ElementTree as ET
        ns = "http://rs.tdwg.org/dwc/text/"
        ET.register_namespace("", ns)
        root = ET.fromstring(infos["meta.xml"])
        # Duplicate the existing vernacular extension pointing at a new file.
        dup = ET.SubElement(root, f"{{{ns}}}extension", {
            "rowType": subject.GBIF_VERNACULAR, "encoding": "UTF-8",
            "fieldsTerminatedBy": "\\t", "linesTerminatedBy": "\\n",
            "ignoreHeaderLines": "1",
        })
        files = ET.SubElement(dup, f"{{{ns}}}files")
        ET.SubElement(files, f"{{{ns}}}location").text = "vernacular-extra.tsv"
        ET.SubElement(dup, f"{{{ns}}}coreid", {"index": "0"})
        ET.SubElement(dup, f"{{{ns}}}field",
                      {"index": "1", "term": "http://rs.gbif.org/terms/1.0/vernacularName"})
        ET.SubElement(dup, f"{{{ns}}}field",
                      {"index": "2", "term": "http://rs.tdwg.org/dwc/terms/language"})
        infos["meta.xml"] = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        infos["vernacular-extra.tsv"] = b"coreid\tvernacularName\tlanguage\n"
    tampered = tmp_path / "two-vernacular.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, add_vern)
    with pytest.raises(NationalSourceError, match="exactly one VernacularName"):
        subject.validate_archive(subject.load_profile(EXAMPLE_PROFILE), tampered)


def test_accepted_name_usage_id_referencing_unknown_taxon_id_rejected(tmp_path: Path) -> None:
    """acceptedNameUsageID must reference a known dwc:taxonID."""
    def bad_ref(infos):
        infos["taxa.tsv"] = (
            b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
            b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
            b"root\tXX:root\t\t\tFungi\t\tkingdom\taccepted\n"
            b"sp1\tXX:sp1\tXX:does-not-exist\tXX:root\tFake sp\tAuct.\tspecies\tsynonym\n"
        )
        infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
        infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    tampered = tmp_path / "bad-ref.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, bad_ref)
    out = tmp_path / "out"
    with pytest.raises(NationalSourceError, match="acceptedNameUsageID references unknown"):
        subject.normalize_archive(subject.load_profile(EXAMPLE_PROFILE), tampered, out)


def test_malformed_preferred_name_boolean_rejected(tmp_path: Path) -> None:
    """isPreferredName must be true/false/1/0/empty. Any other value fails."""
    def bad_bool(infos):
        infos["vernacular.tsv"] = (
            b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
            b"sp1\tsmall example\ten\tmaybe\n"
        )
    tampered = tmp_path / "bad-bool.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, bad_bool)
    out = tmp_path / "out"
    with pytest.raises(NationalSourceError, match="malformed preferred-name boolean"):
        subject.normalize_archive(subject.load_profile(EXAMPLE_PROFILE), tampered, out)


def test_empty_required_core_value_rejected(tmp_path: Path) -> None:
    """A row where a required core term value is empty fails normalize."""
    def empty_status(infos):
        infos["taxa.tsv"] = (
            b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
            b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
            b"root\tXX:root\t\t\tFungi\t\tkingdom\t\n"
        )
        infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
        infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    tampered = tmp_path / "empty-status.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, empty_status)
    out = tmp_path / "out"
    with pytest.raises(NationalSourceError, match="taxonomicStatus"):
        subject.normalize_archive(subject.load_profile(EXAMPLE_PROFILE), tampered, out)


def test_empty_taxon_id_rejected(tmp_path: Path) -> None:
    """dwc:taxonID must be non-empty on every core row."""
    def empty_taxon_id(infos):
        infos["taxa.tsv"] = (
            b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
            b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
            b"root\t\t\t\tFungi\t\tkingdom\taccepted\n"
        )
        infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
        infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    tampered = tmp_path / "empty-taxon-id.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, empty_taxon_id)
    with pytest.raises(NationalSourceError, match="taxonID"):
        subject.normalize_archive(subject.load_profile(EXAMPLE_PROFILE),
                                  tampered, tmp_path / "out")


def test_row_exceeding_max_line_bytes_rejected(tmp_path: Path) -> None:
    """A record whose total encoded field bytes exceed MAX_LINE_BYTES is rejected."""
    big = "x" * (subject.MAX_LINE_BYTES + 1)
    def huge_row(infos):
        infos["taxa.tsv"] = (
            b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
            b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
            + f"root\tXX:root\t\t\t{big}\t\tkingdom\taccepted\n".encode()
        )
        infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
        infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    tampered = tmp_path / "huge-row.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, huge_row)
    # The huge value itself trips the MAX_FIELD_BYTES gate first.
    with pytest.raises(NationalSourceError, match="row field exceeds maximum size|maximum record size"):
        subject.validate_archive(subject.load_profile(EXAMPLE_PROFILE), tampered)


def test_normalize_is_transactional_on_failure(tmp_path: Path) -> None:
    """On any handled failure the output path must not exist at all — no
    partial taxa.jsonl / vernacular.jsonl / report.json is left behind."""
    def orphan_link(infos):
        infos["vernacular.tsv"] = (
            b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
            b"ghost\tsmall\ten\ttrue\n"
        )
    tampered = tmp_path / "orphan.zip"
    _tamper_archive(EXAMPLE_FIXTURE, tampered, orphan_link)
    out = tmp_path / "example"
    with pytest.raises(NationalSourceError, match="orphan vernacular link"):
        subject.normalize_archive(subject.load_profile(EXAMPLE_PROFILE), tampered, out)
    assert not out.exists(), "normalize must not leave a partial output directory"
    # The parent directory should also be free of stray .*.tmp staging dirs.
    stray = [
        p for p in out.parent.iterdir()
        if p.name.startswith(".example.") and p.name.endswith(".tmp")
    ]
    assert stray == [], f"transactional staging directory not cleaned up: {stray}"


def test_normalize_does_not_read_archive_whole_into_memory(tmp_path: Path) -> None:
    """Archive SHA-256 is computed via bounded incremental reads.

    We monkey-patch Path.read_bytes to fail; if normalize relied on whole-file
    reads it would raise from that patch. The bounded implementation uses
    open('rb') + chunked read, so the test passes.
    """
    profile = subject.load_profile(EXAMPLE_PROFILE)
    out = tmp_path / "example"
    original = Path.read_bytes
    calls = {"count": 0}
    def failing_read_bytes(self):
        calls["count"] += 1
        raise AssertionError("normalize_archive must not use Path.read_bytes on the archive")
    Path.read_bytes = failing_read_bytes  # type: ignore[assignment]
    try:
        report = subject.normalize_archive(profile, EXAMPLE_FIXTURE, out)
    finally:
        Path.read_bytes = original  # type: ignore[assignment]
    assert report["archive_sha256"] == hashlib.sha256(EXAMPLE_FIXTURE.read_bytes()).hexdigest()
    assert calls["count"] == 0


# ----- Determinism -----


_FORBIDDEN_REPORT_SUBSTRINGS = (
    # ISO 8601 hour markers — normalize must not stamp a run time.
    *[f"T{h:02d}:" for h in range(24)],
    # Explicit clock/temporal fields that should never appear in a normalize report.
    "\"retrieved_at\"", "\"generated_at\"", "\"timestamp\"", "\"now\"",
    # Temporary directory / random-staging leakage.
    "/tmp/", "/private/var/folders/", ".tmp\"", "tmpdir", "tempfile",
    "staging_name", "staging_device", "staging_inode",
)


def test_normalize_output_is_byte_identical_across_runs(tmp_path: Path) -> None:
    """Two normalize invocations against the same profile + archive must
    produce byte-identical taxa.jsonl, vernacular.jsonl, and report.json.
    The report must contain no timestamps, temp paths, or random staging
    names."""
    profile = subject.load_profile(EXAMPLE_PROFILE)
    a = tmp_path / "a"
    b = tmp_path / "b"
    subject.normalize_archive(profile, EXAMPLE_FIXTURE, a)
    subject.normalize_archive(profile, EXAMPLE_FIXTURE, b)
    for name in ("taxa.jsonl", "vernacular.jsonl", "report.json"):
        left = (a / name).read_bytes()
        right = (b / name).read_bytes()
        assert left == right, f"{name} is not byte-identical across runs"
    report = (a / "report.json").read_text(encoding="utf-8")
    hits = [s for s in _FORBIDDEN_REPORT_SUBSTRINGS if s in report]
    assert hits == [], (
        "report.json contains nondeterministic content: "
        + ", ".join(hits) + f"\nreport:\n{report}"
    )


# ----- Parent-reference resolution states + unresolved-parent counts -----


def _example_profile(tmp_path: Path) -> Path:
    """Copy the example profile into tmp_path so tests can share it."""
    p = tmp_path / "source.json"
    p.write_text(EXAMPLE_PROFILE.read_text())
    return p


def _fixture_with_core_rows(tmp_path: Path, core_rows: list[list[str]]) -> Path:
    """Build a fresh DwC-A whose taxa.tsv contains exactly the given rows."""
    with zipfile.ZipFile(EXAMPLE_FIXTURE, "r") as src:
        infos = {i.filename: src.read(i.filename) for i in src.infolist()}
    header = (
        b"id\ttaxonID\tacceptedNameUsageID\tparentNameUsageID\tscientificName\t"
        b"scientificNameAuthorship\ttaxonRank\ttaxonomicStatus\n"
    )
    body = "".join(
        "\t".join(fields) + "\n" for fields in core_rows
    ).encode("utf-8")
    infos["taxa.tsv"] = header + body
    # No vernacular rows so link-integrity checks stay narrow.
    infos["vernacular.tsv"] = b"coreid\tvernacularName\tlanguage\tisPreferredName\n"
    infos["distribution.tsv"] = b"coreid\tcountryCode\n"
    path = tmp_path / "custom.zip"
    with zipfile.ZipFile(path, "w") as dst:
        for name, payload in infos.items():
            dst.writestr(name, payload)
    return path


def test_normalize_resolved_absent_and_unresolved_parent_states(tmp_path: Path) -> None:
    """Each of the three parent_reference_resolution states appears at least once,
    and the raw parent_name_usage_id object is preserved unchanged."""
    core_rows = [
        ["root",  "XX:root",   "", "",              "Fungi",             "", "kingdom", "accepted"],
        ["child", "XX:child",  "", "XX:root",       "Fungi child",       "", "species", "accepted"],
        ["orph",  "XX:orph",   "", "XX:does-not-exist", "Fungi lost",   "", "species", "accepted"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    out = tmp_path / "normalized"
    report = subject.normalize_archive(profile, archive, out)
    taxa = [json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()]
    by_id = {t["core_row_id"]["value"]: t for t in taxa}
    assert by_id["root"]["parent_reference_resolution"] == "absent"
    assert by_id["root"]["parent_name_usage_id"] is None
    assert by_id["child"]["parent_reference_resolution"] == "resolved"
    assert by_id["child"]["parent_name_usage_id"] == {
        "value": "XX:root", "namespace": "example_parent_name_usage_id",
    }
    # Unresolved: raw identifier preserved verbatim; no resolved edge created.
    assert by_id["orph"]["parent_reference_resolution"] == "unresolved"
    assert by_id["orph"]["parent_name_usage_id"] == {
        "value": "XX:does-not-exist", "namespace": "example_parent_name_usage_id",
    }
    # Report signals: parent-only unresolved edges are hierarchy warnings,
    # not compilation blockers. compiler_ready stays True.
    assert report["hierarchy_complete"] is False
    assert report["compiler_ready"] is True
    assert report["reference_gaps"]["orphan_parent_reference_count"] == 1
    assert report["reference_gaps"]["orphan_accepted_reference_count"] == 0
    samples = report["reference_gaps"]["orphan_parent_reference_samples"]
    assert samples == [{"source_taxon_id": "XX:orph", "raw_reference": "XX:does-not-exist"}]


def test_normalize_hierarchy_complete_true_when_every_parent_resolves(tmp_path: Path) -> None:
    core_rows = [
        ["root",  "XX:root",  "", "",         "Fungi",       "", "kingdom", "accepted"],
        ["child", "XX:child", "", "XX:root",  "Fungi child", "", "species", "accepted"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    out = tmp_path / "normalized"
    report = subject.normalize_archive(profile, archive, out)
    assert report["hierarchy_complete"] is True
    assert report["compiler_ready"] is True
    assert report["reference_gaps"]["orphan_parent_reference_count"] == 0
    assert report["reference_gaps"]["orphan_parent_reference_samples"] == []


def test_orphan_parent_identifier_is_byte_for_byte_preserved(tmp_path: Path) -> None:
    """The exact raw string, including any prefix or whitespace tolerated by
    the source, round-trips into parent_name_usage_id.value without
    modification. No inference from names is permitted."""
    raw = "NBIC:oddly-named-lineage-42"
    core_rows = [
        ["root", "XX:root",  "", "",  "Fungi",       "", "kingdom", "accepted"],
        ["orph", "XX:orph",  "", raw, "Genus specie", "", "species", "accepted"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    out = tmp_path / "normalized"
    subject.normalize_archive(profile, archive, out)
    orph = next(
        json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()
        if json.loads(line)["core_row_id"]["value"] == "orph"
    )
    assert orph["parent_name_usage_id"]["value"] == raw
    assert orph["parent_reference_resolution"] == "unresolved"


def test_normalize_creates_no_resolved_edge_for_orphan_parent(tmp_path: Path) -> None:
    """A record's parent_reference_resolution stays 'unresolved' and no other
    field mutates when the target doesn't exist. The compiler decides what
    to do with the orphan; normalization never invents a substitute."""
    core_rows = [
        ["root", "XX:root", "", "",         "Fungi",           "", "kingdom", "accepted"],
        ["gen",  "XX:gen",  "", "XX:root",  "SomeGenus",        "", "genus",   "accepted"],
        ["orph", "XX:orph", "", "XX:not-a-real-parent", "Genus species", "", "species", "accepted"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    out = tmp_path / "normalized"
    subject.normalize_archive(profile, archive, out)
    orph = next(
        json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()
        if json.loads(line)["core_row_id"]["value"] == "orph"
    )
    assert orph["parent_reference_resolution"] == "unresolved"
    # Every other identifier field is unchanged; nothing was rewritten to
    # match "gen" or "root" merely because they exist.
    assert orph["parent_name_usage_id"]["value"] == "XX:not-a-real-parent"
    assert orph["accepted_name_usage_id"] is None


def test_orphan_accepted_reference_still_blocks_normalization(tmp_path: Path) -> None:
    """Compilation-blocker unchanged: a NON-EMPTY acceptedNameUsageID whose
    target is missing raises NationalSourceError."""
    core_rows = [
        ["root", "XX:root", "",             "",         "Fungi", "", "kingdom", "accepted"],
        ["syn",  "XX:syn",  "XX:no-target", "",         "Fungi", "", "species", "synonym"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    with pytest.raises(NationalSourceError, match="acceptedNameUsageID references unknown"):
        subject.normalize_archive(profile, archive, tmp_path / "normalized")
    assert not (tmp_path / "normalized").exists()


def test_normalize_output_with_orphan_parents_is_deterministic(tmp_path: Path) -> None:
    """Two runs against the same archive with unresolved parents produce
    byte-identical taxa.jsonl / vernacular.jsonl / report.json."""
    core_rows = [
        ["root", "XX:root", "", "",         "Fungi", "", "kingdom", "accepted"],
        ["b",    "XX:b",    "", "XX:zzz",   "Genus b species", "", "species", "accepted"],
        ["c",    "XX:c",    "", "XX:aaa",   "Genus c species", "", "species", "accepted"],
    ]
    archive = _fixture_with_core_rows(tmp_path, core_rows)
    profile = subject.load_profile(_example_profile(tmp_path))
    a = tmp_path / "a"
    b = tmp_path / "b"
    subject.normalize_archive(profile, archive, a)
    subject.normalize_archive(profile, archive, b)
    for name in ("taxa.jsonl", "vernacular.jsonl", "report.json"):
        assert (a / name).read_bytes() == (b / name).read_bytes(), name
    report = json.loads((a / "report.json").read_text())
    # Sample ordering is by (source_taxon_id, raw_reference).
    assert [s["source_taxon_id"] for s in report["reference_gaps"]["orphan_parent_reference_samples"]] == ["XX:b", "XX:c"]
