import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from coldp_table import parse_literal_tsv_record, parse_rfc4180_csv_record
from refresh_col_xr import AcquisitionError


def parse(raw: bytes, *, line=2, header=False, line_limit=1024, field_limit=512):
    return parse_literal_tsv_record(
        raw,
        line_number=line,
        header=header,
        max_line_bytes=line_limit,
        max_field_bytes=field_limit,
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b'a\tb\n', ("a", "b")),
        (b'a\t"one"\n', ("a", '"one"')),
        (b'a\t"""many"""\n', ("a", '"""many"""')),
        (b'a\t"unmatched\n', ("a", '"unmatched')),
        (b'a\tleading"\n', ("a", 'leading"')),
        (b'a\t""\n', ("a", '""')),
        (b"a\tit's\n", ("a", "it's")),
        (b"a\t\tb\t\n", ("a", "", "b", "")),
        (b"a\tb\r\n", ("a", "b")),
        (b"a\tb", ("a", "b")),
        ("a\tBánki\tčáhci\n".encode(), ("a", "Bánki", "čáhci")),
        (b'a\t"former csv error"suffix\n', ("a", '"former csv error"suffix')),
        (b'a\t"unexpected end\n', ("a", '"unexpected end')),
    ],
)
def test_literal_tsv_rows_preserve_quotes_empty_fields_and_utf8(raw, expected) -> None:
    assert parse(raw).raw_fields == expected


def test_recognized_escapes_have_separate_semantic_view() -> None:
    result = parse(b"a\t" + br"one\ttwo\nthree\rfour\\five\q" + b"\n")
    assert result.raw_fields == (r"a", r"one\ttwo\nthree\rfour\\five\q")
    assert result.semantic_fields == ("a", "one\ttwo\nthree\rfour\\five\\q")


def test_colon_qualified_value_is_unchanged() -> None:
    result = parse(b"col:F\taccepted\n")
    assert result.raw_fields[0] == "col:F"
    assert result.semantic_fields[0] == "col:F"


def test_bom_is_accepted_only_on_first_header_token() -> None:
    assert parse(b"\xef\xbb\xbfID\tstatus\n", line=1, header=True).raw_fields[0] == "ID"
    with pytest.raises(AcquisitionError, match="BOM"):
        parse(b"ID\t\xef\xbb\xbfstatus\n", line=1, header=True)
    with pytest.raises(AcquisitionError, match="BOM"):
        parse(b"\xef\xbb\xbfvalue\taccepted\n")


def test_physical_tabs_are_always_delimiters() -> None:
    assert len(parse(b"a\tb\tc\n").raw_fields) == 3


def test_line_field_and_utf8_limits_are_enforced_before_unbounded_decode() -> None:
    with pytest.raises(AcquisitionError, match="line"):
        parse(b"12345\n", line_limit=4)
    with pytest.raises(AcquisitionError, match="field"):
        parse(b"12345\tb\n", field_limit=4)
    with pytest.raises(AcquisitionError, match="strict UTF-8"):
        parse(b"a\t\xff\n")


def test_csv_parser_remains_structurally_separate() -> None:
    assert parse_rfc4180_csv_record('"a,b",c') == ["a,b", "c"]
    assert parse(b'"a,b"\tc\n').raw_fields == ('"a,b"', "c")
