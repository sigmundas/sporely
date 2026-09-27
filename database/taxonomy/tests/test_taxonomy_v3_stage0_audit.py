"""Taxonomy v3 Stage 0 audit: population queries and manifest determinism.

The full audit runs against the tracked release and the gitignored source
archives; these tests pin the pieces that decide membership and fingerprints on
a tiny in-memory release.
"""
import importlib.util
import sqlite3
from pathlib import Path

_SCRIPT = (Path(__file__).resolve().parents[1] / "evidence" / "taxonomy-v3"
           / "audit_stage0_coverage.py")
_spec = importlib.util.spec_from_file_location("audit_stage0_coverage", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)


def _release() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
            canonical_scientific_name TEXT, canonical_source_system TEXT,
            canonical_external_id TEXT);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER,
            source_system TEXT, external_id INTEGER, note TEXT);
        INSERT INTO taxon_min VALUES
            (1, 'Craterellus tubaeformis', 'col_xr', 'Z8TV'),
            (2, 'Cantharellus cibarius', 'col_xr', 'QMKY'),
            (3, 'Cantharellus cibarius', 'nortaxa', '56210'),
            (4, 'Pholiotina vexans', 'nortaxa', '58766'),
            (5, 'Entoloma conferendum', 'col_xr', '39ZCL');
        INSERT INTO taxon_external_id_min VALUES
            (1, 'artsdatabanken', 56227, 'cross_source_automatic_exact'),
            (1, 'artsdatabanken', 62407, 'synonym_of_accepted'),
            (5, 'artsdatabanken', 53482, 'manual_approved_exact');
    """)
    return conn


def test_populations_are_separated():
    conn = _release()
    assert audit.group_a(conn) == [("56227", "Z8TV", 1)]
    # Name equality sizes Group B; a synonym-level duplicate is not in it.
    assert audit.group_b(conn) == [("56210", 3, "QMKY", 2)]
    assert audit.reviewed_bridges(conn) == [
        {"nortaxa_taxon_id": "53482", "sporely_taxon_id": 5,
         "note": "manual_approved_exact"}]


def _row(nortaxa_id, evidence_class, shared):
    row = {column: None for column in audit.MANIFEST_COLUMNS}
    row.update(nortaxa_taxon_id=nortaxa_id, col_usage_id="C" + nortaxa_id,
               sporely_taxon_id=int(nortaxa_id), evidence_class=evidence_class,
               shared_synonym_count=shared, in_cloud_scope=True)
    return row


def test_manifest_is_order_independent_and_class_scoped():
    rows = [_row("2", audit.EVIDENCE_SHARED_SYNONYMY, 3),
            _row("1", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("9", audit.EVIDENCE_NONE, 0)]
    pins = {"release": {"sqlite_sha256": "x"}}
    first = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY, rows, pins)
    second = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY,
                                  list(reversed(rows)), pins)
    assert first == second
    assert first["member_count"] == 2
    assert [m[0] for m in first["members"]] == ["1", "2"]
    assert first["review_status"] == "needs_review"


def test_summary_distributions():
    rows = [_row("1", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("2", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("3", audit.EVIDENCE_NONE, 0)]
    summary = audit.summarise(rows, taxon_key="sporely_taxon_id")
    assert summary["reviewable_total"] == 2
    assert summary["shared_synonym_count_distribution"][
        audit.EVIDENCE_SHARED_SYNONYMY] == {"1": 2}
