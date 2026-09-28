"""Cross-reference evidence grading from the sources' own synonymy.

Source rows are written into tiny archives shaped like the pinned NorTaxa
(``taxon.txt``) and COL (``NameUsage.tsv``) exports and read back through the
module's own readers.
"""
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import cross_reference_evidence as xref  # noqa: E402

_NORTAXA_COLUMNS = ["taxonID", "acceptedNameUsageID", "scientificName",
                    "scientificNameAuthorship", "taxonomicStatus", "taxonRank"]
_COL_COLUMNS = ["col:ID", "col:parentID", "col:status", "col:rank",
                "col:scientificName", "col:authorship"]


def _archive(path: Path, member: str, columns: list[str], rows) -> Path:
    lines = ["\t".join(columns)] + ["\t".join(row) for row in rows]
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr(member, "\n".join(lines) + "\n")
    return path


def _grade(tmp_path, nortaxa_rows, col_rows, pair):
    nortaxa = xref.read_nortaxa(
        _archive(tmp_path / "nortaxa.zip", "taxon.txt", _NORTAXA_COLUMNS,
                 nortaxa_rows), {pair[0]})
    col = xref.read_col(
        _archive(tmp_path / "col.zip", "NameUsage.tsv", _COL_COLUMNS,
                 col_rows), {pair[1]})
    return xref.grade(nortaxa[pair[0]], col[pair[1]])


# NorTaxa 1.284 and COL XR 2026-07-17 rows for Didymocyrtis peltigerae, the
# Group-A association NorTaxa 227128 / COL 35YQ2 on Sporely 3841. NorTaxa
# publishes a duplicate synonym usage, 226877, spelled exactly like its own
# accepted usage.
_DIDYMOCYRTIS_NORTAXA = [
    ["227128", "227128", "Didymocyrtis peltigerae", "(Fuckel) Hafellner",
     "valid", "species"],
    ["226877", "227128", "Didymocyrtis peltigerae", "(Fuckel) Hafellner",
     "synonym", "species"],
]
_DIDYMOCYRTIS_COL = [
    ["35YQ2", "8HHKX", "accepted", "species", "Didymocyrtis peltigerae",
     "(Fuckel) Hafellner"],
    ["6RK3N", "35YQ2", "synonym", "species", "Microthelia peltigerae",
     "(Fuckel) Kuntze"],
    ["35ZFZ", "35YQ2", "synonym", "species", "Didymosphaeria peltigerae",
     "Fuckel"],
    ["4L4NV", "35YQ2", "synonym", "species", "Polycoccum peltigerae",
     "(Fuckel) Vězda"],
    ["35Z8Y", "35YQ2", "synonym", "species", "Didymosphaerella peltigerae",
     "(Fuckel) Cooke"],
]


def test_self_synonym_is_not_an_accepted_synonymy_reference(tmp_path):
    result = _grade(tmp_path, _DIDYMOCYRTIS_NORTAXA, _DIDYMOCYRTIS_COL,
                    ("227128", "35YQ2"))
    assert result["accepted_names_agree"] is True
    assert result["bridge_lists_backbone_accepted_name_as_synonym"] is False
    assert result["backbone_lists_bridge_accepted_name_as_synonym"] is False
    assert result["evidence_class"] == xref.EVIDENCE_NONE
    assert (result["bridge_synonym_count"],
            result["backbone_synonym_count"]) == (1, 4)


def test_self_synonyms_on_both_sides_are_not_reciprocal(tmp_path):
    col = _DIDYMOCYRTIS_COL + [
        ["X1", "35YQ2", "synonym", "species", "Didymocyrtis peltigerae",
         "(Fuckel) Hafellner"]]
    result = _grade(tmp_path, _DIDYMOCYRTIS_NORTAXA, col, ("227128", "35YQ2"))
    assert result["evidence_class"] == xref.EVIDENCE_NONE


def test_divergent_accepted_names_still_grade_as_accepted_synonymy(tmp_path):
    nortaxa = [
        ["58766", "58766", "Pholiotina vexans", "(P.D. Orton) Bon", "valid",
         "species"],
        ["60001", "58766", "Conocybe vexans", "P.D. Orton", "synonym",
         "species"],
    ]
    col = [["XQZ6", "P", "accepted", "species", "Conocybe vexans",
            "P.D. Orton"]]
    one_way = _grade(tmp_path, nortaxa, col, ("58766", "XQZ6"))
    assert one_way["evidence_class"] == xref.EVIDENCE_ONE_DIRECTIONAL

    col.append(["S1", "XQZ6", "synonym", "species", "Pholiotina vexans",
                "(P.D. Orton) Bon"])
    both = _grade(tmp_path, nortaxa, col, ("58766", "XQZ6"))
    assert both["evidence_class"] == xref.EVIDENCE_RECIPROCAL


def test_readers_record_the_accepted_usage_rank(tmp_path):
    nortaxa = xref.read_nortaxa(
        _archive(tmp_path / "n.zip", "taxon.txt", _NORTAXA_COLUMNS,
                 _DIDYMOCYRTIS_NORTAXA), {"227128"})
    col = xref.read_col(
        _archive(tmp_path / "c.zip", "NameUsage.tsv", _COL_COLUMNS,
                 _DIDYMOCYRTIS_COL), {"35YQ2"})
    assert nortaxa["227128"]["rank"] == col["35YQ2"]["rank"] == "species"


def test_readers_record_structured_synonym_status(tmp_path):
    # NorTaxa 1.284 / COL XR 2026-07-17 rows for Cortinarius diosmus, NorTaxa
    # 59001 / COL YKPL: its only shared synonym is not validly published.
    nortaxa = xref.read_nortaxa(
        _archive(tmp_path / "n.zip", "taxon.txt",
                 _NORTAXA_COLUMNS + ["nomenclaturalStatus"], [
                     ["59001", "59001", "Cortinarius diosmus", "Kühner",
                      "valid", "species", ""],
                     ["N1", "59001", "Cortinarius argillaceosericeus",
                      "Kytöv., Niskanen & Liimat.", "synonym", "species",
                      "notvalidlypublished"]]), {"59001"})
    col = xref.read_col(
        _archive(tmp_path / "c.zip", "NameUsage.tsv",
                 _COL_COLUMNS + ["col:nameStatus"], [
                     ["YKPL", "P", "accepted", "species",
                      "Cortinarius diosmus", "Kühner", ""],
                     ["C1", "YKPL", "synonym", "species",
                      "Cortinarius argillaceosericeus",
                      "Kytöv., Niskanen & Liimat.", "not established"]]),
        {"YKPL"})
    key = ("cortinarius argillaceosericeus", "Kytöv., Niskanen & Liimat.")
    assert nortaxa["59001"]["synonym_status"][key] == {
        ("taxonomicStatus", "synonym"),
        ("nomenclaturalStatus", "notvalidlypublished")}
    assert col["YKPL"]["synonym_status"][key] == {
        ("col:status", "synonym"), ("col:nameStatus", "not established")}
    # Recording status changes no grade.
    assert xref.grade(nortaxa["59001"], col["YKPL"])["evidence_class"] \
        == xref.EVIDENCE_SHARED_SYNONYMY
