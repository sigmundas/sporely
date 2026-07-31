"""Tests for the Stage W1 cloud-taxonomy exporter.

Covers:
* canonical JSON + hash primitives;
* scope construction on a fabricated fixture SQLite;
* full end-to-end export on a fabricated fixture (fast unit tests);
* determinism (two clean runs → byte-identical outputs);
* required regression cases: multi-source same-name concepts, genus-only,
  synonym-of-accepted aliases, language preservation, null vs empty string,
  namespace collision, `NBIC:` prefix retention;
* pinned-release regression counts against the installed compiled SQLite,
  when it exists on this machine (skipped otherwise).
"""
from __future__ import annotations

import hashlib
import gzip
import json
import os
import sqlite3
import sys
import textwrap
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from database.taxonomy import cloud_export as ce  # noqa: E402


# ---------- canonical primitives ---------------------------------------


def test_canonical_dumps_sorts_keys_and_escapes():
    assert ce.canonical_dumps({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert ce.canonical_dumps({"x": None}) == '{"x":null}'
    assert ce.canonical_dumps({"x": ""}) == '{"x":""}'
    # UTF-8 characters preserved, not escaped.
    assert ce.canonical_dumps({"n": "Cåhppesguoppar"}) == '{"n":"Cåhppesguoppar"}'


def test_canonical_dumps_rejects_nan():
    with pytest.raises(ValueError):
        ce.canonical_dumps({"x": float("nan")})


def test_whole_export_hash_is_length_prefixed(tmp_path):
    a = tmp_path / "a.jsonl"
    b = tmp_path / "b.jsonl"
    a.write_bytes(b'{"a":1}\n')
    b.write_bytes(b'{"b":2}\n')
    h = ce.whole_export_sha256([("a.jsonl", a), ("b.jsonl", b)])

    # Manually reconstruct: len(name):name:len(bytes):bytes\n per file.
    expected = hashlib.sha256()
    for name, data in [("a.jsonl", b'{"a":1}\n'), ("b.jsonl", b'{"b":2}\n')]:
        nb = name.encode("utf-8")
        expected.update(f"{len(nb)}".encode("ascii") + b":" + nb + b":")
        expected.update(f"{len(data)}".encode("ascii") + b":" + data + b"\n")
    assert h == expected.hexdigest()


def test_whole_export_hash_distinguishes_filename_swap(tmp_path):
    a = tmp_path / "a.jsonl"
    b = tmp_path / "b.jsonl"
    a.write_bytes(b"xx")
    b.write_bytes(b"xx")
    h1 = ce.whole_export_sha256([("a.jsonl", a), ("b.jsonl", b)])
    h2 = ce.whole_export_sha256([("b.jsonl", b), ("a.jsonl", a)])
    assert h1 != h2, "swapping name order must change the whole-export hash"


# ---------- fabricated fixture -----------------------------------------


def _make_fixture_sqlite(path: Path, release_id: str = "tax-2099.01.01-01") -> None:
    """Build a small compiler-shaped SQLite for tests.

    The graph:
        1 Fungi (kingdom, col_xr)               root
          10 Basidiomycota (phylum, col_xr)
            100 Cortinariaceae (family, col_xr)
              1000 Cortinarius (genus, col_xr)
              1001 Aureonarius (genus, col_xr)
                10010 Aureonarius limonius (species, col_xr)
              10000 Inocybe (genus, col_xr)        -- same name as Nortaxa 20003
              10001 Cantharellus cibarius (species, col_xr) -- same name as Nortaxa 20004
              10002 Candolleomyces candolleanus (species, col_xr) -- alias tests
        2 Plantae (kingdom, col_xr)              NOT in export (only Fungi + nortaxa)
          20 Rosales (order, col_xr)
        20000 Cortinarius limonius (species, nortaxa)   -- forced-in via nortaxa
        20001 Some Nortaxa Order (order, nortaxa)
        20002 Some Nortaxa Class (class, nortaxa)
        20003 Inocybe (genus, nortaxa)
        20004 Cantharellus cibarius (species, nortaxa)
    """
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxon_min (
          taxon_id INTEGER PRIMARY KEY,
          parent_taxon_id INTEGER,
          genus TEXT NOT NULL DEFAULT '',
          specific_epithet TEXT NOT NULL DEFAULT '',
          family TEXT,
          norwegian_taxon_id INTEGER,
          swedish_taxon_id INTEGER,
          inaturalist_taxon_id INTEGER,
          canonical_scientific_name TEXT,
          taxon_rank TEXT,
          taxonomic_status TEXT,
          source_system TEXT,
          preferred_scientific_name_no TEXT,
          preferred_scientific_name_sv TEXT,
          sporely_content_release_id TEXT,
          canonical_source_system TEXT NOT NULL,
          canonical_external_id TEXT NOT NULL
        );
        CREATE TABLE scientific_name_min (
          scientific_name_id INTEGER PRIMARY KEY,
          taxon_id INTEGER NOT NULL,
          language_code TEXT NOT NULL,
          scientific_name TEXT NOT NULL,
          is_preferred_name INTEGER NOT NULL DEFAULT 0,
          source TEXT,
          note TEXT
        );
        CREATE TABLE vernacular_min (
          vernacular_id INTEGER PRIMARY KEY,
          taxon_id INTEGER NOT NULL,
          language_code TEXT NOT NULL,
          vernacular_name TEXT NOT NULL,
          is_preferred_name INTEGER NOT NULL DEFAULT 0,
          source TEXT
        );
        CREATE TABLE taxon_external_id_min (
          external_id_row_id INTEGER PRIMARY KEY,
          taxon_id INTEGER NOT NULL,
          source_system TEXT NOT NULL,
          external_id INTEGER NOT NULL,
          id_role TEXT NOT NULL,
          is_preferred INTEGER NOT NULL DEFAULT 0,
          external_name TEXT,
          note TEXT
        );
        CREATE TABLE taxon_external_id_text_min (
          external_id_row_id INTEGER PRIMARY KEY,
          taxon_id INTEGER NOT NULL,
          source_system TEXT NOT NULL,
          namespace TEXT NOT NULL,
          external_id TEXT NOT NULL,
          id_role TEXT NOT NULL,
          is_preferred INTEGER NOT NULL DEFAULT 0,
          external_name TEXT,
          note TEXT
        );
        CREATE TABLE taxon_redlist_min (
          redlist_row_id INTEGER PRIMARY KEY,
          taxon_id INTEGER,
          source_system TEXT NOT NULL,
          source_release TEXT NOT NULL,
          assessment_id TEXT NOT NULL,
          assessment_area TEXT NOT NULL,
          assessed_name_source TEXT NOT NULL,
          assessed_name_namespace TEXT NOT NULL,
          assessed_name_id TEXT NOT NULL,
          scientific_name_snapshot TEXT NOT NULL,
          authorship_snapshot TEXT,
          taxon_rank_snapshot TEXT,
          category_raw TEXT NOT NULL,
          category_code TEXT NOT NULL,
          category_is_downgraded INTEGER NOT NULL DEFAULT 0,
          criteria TEXT,
          expert_group TEXT,
          assessment_url TEXT
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    concepts = [
        # id,parent,genus,epi,family,rank,cansrc,canext,canonical_name
        (1, None, "", "", None, "kingdom", "col_xr", "K:FUN", "Fungi"),
        (10, 1, "", "", None, "phylum", "col_xr", "P:BAS", "Basidiomycota"),
        (100, 10, "", "", "Cortinariaceae", "family", "col_xr", "F:COR", "Cortinariaceae"),
        (1000, 100, "Cortinarius", "", "Cortinariaceae", "genus", "col_xr", "G:CORT", "Cortinarius"),
        (1001, 100, "Aureonarius", "", "Cortinariaceae", "genus", "col_xr", "G:AURE", "Aureonarius"),
        (10010, 1001, "Aureonarius", "limonius", "Cortinariaceae", "species", "col_xr", "S:AURELIM", "Aureonarius limonius"),
        (10000, 100, "Inocybe", "", "Cortinariaceae", "genus", "col_xr", "G:INO", "Inocybe"),
        (10001, 100, "Cantharellus", "cibarius", "Cantharellaceae", "species", "col_xr", "S:CANCIB", "Cantharellus cibarius"),
        (10002, 100, "Candolleomyces", "candolleanus", "Psathyrellaceae", "species", "col_xr", "S:CANDOLL", "Candolleomyces candolleanus"),
        # Plantae side is NOT nortaxa and NOT Fungi — should be excluded.
        (2, None, "", "", None, "kingdom", "col_xr", "K:PLA", "Plantae"),
        (20, 2, "", "", None, "order", "col_xr", "O:ROS", "Rosales"),
        # NorTaxa side
        (20000, None, "Cortinarius", "limonius", "Cortinariaceae", "species", "nortaxa", "52796", "Cortinarius limonius"),
        (20001, None, "", "", None, "order", "nortaxa", "N:ORD", "SomeNortaxaOrder"),
        (20002, None, "", "", None, "class", "nortaxa", "N:CLS", "SomeNortaxaClass"),
        (20003, None, "Inocybe", "", "Cortinariaceae", "genus", "nortaxa", "53077", "Inocybe"),
        (20004, None, "Cantharellus", "cibarius", "Cantharellaceae", "species", "nortaxa", "56210", "Cantharellus cibarius"),
    ]
    for (
        tid, parent, genus, epi, family, rank, cansrc, canext, canonical_name
    ) in concepts:
        conn.execute(
            "INSERT INTO taxon_min("
            "taxon_id, parent_taxon_id, genus, specific_epithet, family, "
            "canonical_scientific_name, taxon_rank, taxonomic_status, "
            "source_system, sporely_content_release_id, canonical_source_system, "
            "canonical_external_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                tid, parent, genus, epi, family, canonical_name,
                rank, "accepted", cansrc, release_id, cansrc, canext,
            ),
        )
    # Mirror the compiler's unique-preferred NorTaxa shortcut. Each of these
    # rows is the sole preferred nortaxa_taxon_id source usage for its
    # Sporely concept, so the compiler is entitled to populate
    # taxon_min.norwegian_taxon_id from it.
    for tid, no_id in [(20000, 52796), (20003, 53077), (20004, 56210)]:
        conn.execute(
            "UPDATE taxon_min SET norwegian_taxon_id=? WHERE taxon_id=?",
            (no_id, tid),
        )
    # Canonical scientific names + one synonym alias to test aliasing.
    sn_rows = [
        (1, "sci", "Fungi", 1, "col_xr", None),
        (10, "sci", "Basidiomycota", 1, "col_xr", None),
        (100, "sci", "Cortinariaceae", 1, "col_xr", None),
        (1000, "sci", "Cortinarius", 1, "col_xr", None),
        (1001, "sci", "Aureonarius", 1, "col_xr", None),
        (10010, "sci", "Aureonarius limonius", 1, "col_xr", None),
        (10000, "sci", "Inocybe", 1, "col_xr", None),
        (10001, "sci", "Cantharellus cibarius", 1, "col_xr", None),
        (10002, "sci", "Candolleomyces candolleanus", 1, "col_xr", None),
        (10002, "sci", "Psathyrella candolleana", 0, "nortaxa", "synonym_of_accepted"),
        (20000, "sci", "Cortinarius limonius", 1, "nortaxa", None),
        (20003, "sci", "Inocybe", 1, "nortaxa", None),
        (20004, "sci", "Cantharellus cibarius", 1, "nortaxa", None),
    ]
    for t, lang, name, pref, src, note in sn_rows:
        conn.execute(
            "INSERT INTO scientific_name_min("
            "taxon_id, language_code, scientific_name, is_preferred_name, source, note"
            ") VALUES (?,?,?,?,?,?)",
            (t, lang, name, pref, src, note),
        )
    vern = [
        # Preserve nb/nn/se distinctly plus one empty-string / null contrast row.
        (10001, "nb", "kantarell", 1, "nortaxa"),
        (10001, "nn", "kantarell", 1, "nortaxa"),
        (10001, "se", "šálti-guoppar", 0, "nortaxa"),
        (10002, "nb", "hvit sprøsopp", 1, "nortaxa"),
        # Null vs empty string vernacular_name is not allowed by schema (NOT NULL);
        # instead we test null-vs-empty on optional column `source`:
        (20003, "nb", "trevlesopp", 1, None),   # source is null
        (20003, "nn", "trevlesopp", 0, ""),     # source is empty string (distinct from null)
    ]
    for t, lang, name, pref, src in vern:
        conn.execute(
            "INSERT INTO vernacular_min("
            "taxon_id, language_code, vernacular_name, is_preferred_name, source"
            ") VALUES (?,?,?,?,?)",
            (t, lang, name, pref, src),
        )
    # Integer external IDs — nortaxa taxonIDs (source_system='artsdatabanken' per compiler).
    int_ext = [
        (20000, "artsdatabanken", 52796, "accepted", 1, "Cortinarius limonius", None),
        (20003, "artsdatabanken", 53077, "accepted", 1, "Inocybe", None),
        (20004, "artsdatabanken", 56210, "accepted", 1, "Cantharellus cibarius", None),
        # Namespace-collision fixture: same numeric value under integer source.
        (10000, "artsdatabanken", 53077, "synonym", 0, None, None),
    ]
    for t, ss, eid, role, pref, ename, note in int_ext:
        conn.execute(
            "INSERT INTO taxon_external_id_min(taxon_id, source_system, external_id, id_role, is_preferred, external_name, note) "
            "VALUES (?,?,?,?,?,?,?)",
            (t, ss, eid, role, pref, ename, note),
        )
    # Text external IDs — COL usage IDs + NBIC prefixed IDs on artsorakel.
    text_ext = [
        (10001, "col_xr", "col_usage_id", "QMKY", "accepted", 1, "Cantharellus cibarius", None),
        (10000, "col_xr", "col_usage_id", "54HL", "accepted", 1, "Inocybe", None),
        (10002, "col_xr", "col_usage_id", "C2ND", "accepted", 1, "Candolleomyces candolleanus", None),
        # NBIC:12345 — prefix must be preserved. Same numeric suffix collides with an
        # unrelated NorTaxa integer id 12345 (which we do not fabricate here) — proves
        # the exporter never conflates them via numeric equality.
        (10001, "artsorakel", "nbic_scientific_name_id", "NBIC:12345", "accepted", 0, None, None),
        # Namespace collision within the same source: same textual value, different namespace.
        (10001, "col_xr", "gbif_taxon_key", "QMKY", "accepted", 0, None, None),
        # Same textual value across sources — must survive.
        (20004, "col_xr", "col_usage_id", "QMKY_X", "accepted", 0, None, None),
    ]
    for t, ss, ns, eid, role, pref, ename, note in text_ext:
        conn.execute(
            "INSERT INTO taxon_external_id_text_min(taxon_id, source_system, namespace, external_id, id_role, is_preferred, external_name, note) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (t, ss, ns, eid, role, pref, ename, note),
        )
    # Red-list — one resolved Norge row, one Svalbard row, one unresolved (taxon_id NULL).
    rl = [
        (20004, "artsdatabanken_redlist", "2021", "A001", "Norge", "artsdatabanken",
         "artsnavnebase_scientific_name_id", "56210", "Cantharellus cibarius",
         None, "species", "LC", "LC", 0, None, None, None),
        (20000, "artsdatabanken_redlist", "2021", "A002", "Svalbard", "artsdatabanken",
         "artsnavnebase_scientific_name_id", "52796", "Cortinarius limonius",
         "Fr.", "species", "VU", "VU", 0, "B2ab", "cort-committee", None),
        (None, "artsdatabanken_redlist", "2021", "A003", "Norge", "artsdatabanken",
         "artsnavnebase_scientific_name_id", "999999", "Nonexistent species",
         None, "species", "DD", "DD", 0, None, None, None),
    ]
    for row in rl:
        conn.execute(
            "INSERT INTO taxon_redlist_min("
            "taxon_id, source_system, source_release, assessment_id, assessment_area, "
            "assessed_name_source, assessed_name_namespace, assessed_name_id, "
            "scientific_name_snapshot, authorship_snapshot, taxon_rank_snapshot, "
            "category_raw, category_code, category_is_downgraded, criteria, "
            "expert_group, assessment_url) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row,
        )
    conn.executemany(
        "INSERT INTO taxonomy_meta(key, value) VALUES(?,?)",
        [
            ("taxonomy_schema_version", "2"),
            ("content_release_id", release_id),
            ("state", "candidate"),
            ("publication", "none"),
            ("compiler_manifest_sha256", "deadbeef" * 8),
            ("registry_sha256", "ba" * 32),
            ("source_release[col_xr].id", "col_xr:test:2099-01-01"),
            ("source_release[col_xr].archive_sha256", "aa" * 32),
            ("source_release[nortaxa].id", "nortaxa:test:2099-01-01"),
            ("source_release[nortaxa].archive_sha256", "bb" * 32),
            ("source_release[artsdatabanken_redlist].id", "2021"),
            ("source_release[artsdatabanken_redlist].archive_sha256", "cc" * 32),
        ],
    )
    conn.commit()
    conn.close()


def _wrap_sqlite_as_release(tmp_path: Path, release_id: str = "tax-2099.01.01-01"):
    """Build fixture SQLite, gz it, and write a matching outer manifest.

    Returns (artifact_gz_path, manifest_path, artifact_dir).
    """
    art_dir = tmp_path / "artifact"
    art_dir.mkdir(exist_ok=True)
    sqlite_path = art_dir / f"{release_id}.sqlite3"
    _make_fixture_sqlite(sqlite_path, release_id=release_id)
    gz_path = art_dir / f"{release_id}.sqlite3.gz"
    # Deterministic gzip: mtime=0.
    with sqlite_path.open("rb") as src, gzip.GzipFile(
        filename="", mode="wb", fileobj=gz_path.open("wb"), mtime=0
    ) as gz:
        while True:
            chunk = src.read(1 << 16)
            if not chunk:
                break
            gz.write(chunk)
    sq_sha = ce.sha256_file(sqlite_path)
    gz_sha = ce.sha256_file(gz_path)
    manifest = art_dir / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                # Deliberately omit canonical_authority / doi /
                # checklistbank_dataset_id / nortaxa_release — the pinned
                # production manifest also lacks them, and the exporter must
                # emit `null` rather than fabricate a literal.
                "manifest_schema_version": 1,
                "taxonomy_schema_version": 2,
                "content_release_id": release_id,
                "state": "candidate",
                "publication": "none",
                "gz_artifact": gz_path.name,
                "gz_sha256": gz_sha,
                "gz_bytes": gz_path.stat().st_size,
                "sqlite_sha256": sq_sha,
                "sqlite_bytes": sqlite_path.stat().st_size,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    sqlite_path.unlink()  # remove decompressed copy; verify_source will re-produce
    return gz_path, manifest, art_dir


def _write_fixture_policies(tmp_path: Path) -> Path:
    policies = tmp_path / "policies"
    policies.mkdir(exist_ok=True)
    for name in ce.POLICY_HASH_TARGETS[:2]:
        (policies / name).write_text(f"# stub {name}\n", encoding="utf-8")
    return policies


# ---------- scope + emission on fixture --------------------------------


def _run_fixture_export(tmp_path: Path, *, replace: bool = False):
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    policies = _write_fixture_policies(tmp_path)
    out = tmp_path / "cloud_export"
    return ce.run_export(
        artifact_gz=gz,
        manifest=manifest,
        output_dir=out,
        policy_dir=policies,
        replace=replace,
        generated_at="2099-01-01T00:00:00Z",
    )


def test_scope_excludes_plantae(tmp_path):
    result = _run_fixture_export(tmp_path)
    ids = set(result.scope.concept_ids)
    # Fungi kingdom + descendants + all nortaxa
    assert 1 in ids and 10 in ids and 10001 in ids and 10002 in ids
    assert 20000 in ids and 20003 in ids and 20004 in ids
    # Plantae kingdom + descendants NOT included
    assert 2 not in ids and 20 not in ids
    assert result.scope.excluded_count == 2


def test_datasets_row_counts_and_shape(tmp_path):
    result = _run_fixture_export(tmp_path)
    ds = result.datasets
    assert ds["taxon.jsonl"].row_count == len(result.scope.concept_ids)

    # Verify manifest is canonical JSON.
    manifest = json.loads(result.manifest_path.read_text())
    assert manifest["scope_predicate_id"] == "fungi_closure_union_nortaxa_v1"
    assert manifest["excluded_concept_count"] == result.scope.excluded_count
    assert manifest["taxonomy_schema_version"] == 2


def test_multi_source_same_name_kept(tmp_path):
    result = _run_fixture_export(tmp_path)
    lines = (result.output_dir / "taxon.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines]
    cantharellus = [r for r in rows if r["canonical_scientific_name"] == "Cantharellus cibarius"]
    assert len(cantharellus) == 2
    inocybe = [r for r in rows if r["canonical_scientific_name"] == "Inocybe"]
    assert len(inocybe) == 2  # col_xr genus + nortaxa genus
    aur = [r for r in rows if r["canonical_scientific_name"] == "Aureonarius limonius"]
    cort_lim = [r for r in rows if r["canonical_scientific_name"] == "Cortinarius limonius"]
    assert len(aur) == 1 and len(cort_lim) == 1
    # Different canonical_source_system — never collapsed.
    assert {aur[0]["canonical_source_system"], cort_lim[0]["canonical_source_system"]} == {
        "col_xr", "nortaxa"
    }


def test_scientific_name_alias_preserved(tmp_path):
    result = _run_fixture_export(tmp_path)
    lines = (result.output_dir / "scientific_name.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines]
    on_10002 = [r for r in rows if r["taxon_id"] == 10002]
    names = {r["scientific_name"] for r in on_10002}
    assert names == {"Candolleomyces candolleanus", "Psathyrella candolleana"}
    synonym = [r for r in on_10002 if r["scientific_name"] == "Psathyrella candolleana"][0]
    assert synonym["is_preferred_name"] is False
    assert synonym["note"] == "synonym_of_accepted"


def test_languages_preserved_verbatim(tmp_path):
    result = _run_fixture_export(tmp_path)
    lines = (result.output_dir / "vernacular.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines]
    langs = {r["language_code"] for r in rows}
    assert langs == {"nb", "nn", "se"}


def test_serializer_preserves_future_lang_codes():
    # Prove that sma/smj/no would pass through unchanged.
    obj = {"language_code": "sma", "vernacular_name": "Ex."}
    assert '"sma"' in ce.canonical_dumps(obj)
    obj["language_code"] = "smj"
    assert '"smj"' in ce.canonical_dumps(obj)
    obj["language_code"] = "no"
    assert '"no"' in ce.canonical_dumps(obj)


def test_null_vs_empty_string_distinguished(tmp_path):
    result = _run_fixture_export(tmp_path)
    lines = (result.output_dir / "vernacular.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines if json.loads(x)["taxon_id"] == 20003]
    src_values = {r["vernacular_name"]: r["source"] for r in rows}
    # The two fixture rows should distinguish null vs empty string.
    # Fixture used the SAME vernacular_name; the differing column is `source`.
    all_srcs = [r["source"] for r in rows]
    assert None in all_srcs
    assert "" in all_srcs
    # And the raw JSONL bytes must differ.
    raw = (result.output_dir / "vernacular.jsonl").read_bytes()
    assert b'"source":null' in raw
    assert b'"source":""' in raw


def test_authoritative_file_has_namespace_and_no_source_table(tmp_path):
    result = _run_fixture_export(tmp_path)
    rows = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    assert rows, "authoritative file should not be empty"
    for r in rows:
        assert r["namespace"] is not None, "authoritative rows must have declared namespace"
        assert "source_table" not in r, "authoritative rows must not carry legacy discriminator"


def test_legacy_integer_file_has_no_namespace_field(tmp_path):
    result = _run_fixture_export(tmp_path)
    rows = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id_legacy_integer.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    assert rows, "legacy integer file should not be empty"
    for r in rows:
        assert "namespace" not in r, (
            "legacy integer rows must not fake a namespace field — the compiler's "
            "integer table does not preserve namespace"
        )
        assert "source_table" not in r


def test_nbic_prefix_and_namespace_collisions_preserved(tmp_path):
    result = _run_fixture_export(tmp_path)
    auth = [
        json.loads(x) for x in
        (result.output_dir / "taxon_external_id.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    legacy = [
        json.loads(x) for x in
        (result.output_dir / "taxon_external_id_legacy_integer.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    # NBIC prefix retained in the authoritative file (text namespace declared).
    nbic = [r for r in auth if r["external_id"] == "NBIC:12345"]
    assert nbic and nbic[0]["namespace"] == "nbic_scientific_name_id"
    assert nbic[0]["source_system"] == "artsorakel"

    # Namespace-scoped collision — same textual `QMKY` under two namespaces.
    qmky = [r for r in auth if r["external_id"] == "QMKY"]
    assert len(qmky) == 2
    assert {r["namespace"] for r in qmky} == {"col_usage_id", "gbif_taxon_key"}

    # Integer id 53077 (from the legacy integer table) attaches to two taxa
    # (nortaxa accepted + col_xr synonym). Both survive; both live in the
    # legacy file with NO namespace field at all.
    both = [r for r in legacy if r["external_id"] == "53077"]
    assert len(both) == 2
    for r in both:
        assert "namespace" not in r

    # No merging across files by numeric equality: 20004 has 56210 (legacy int)
    # AND QMKY_X (authoritative text) under source_system col_xr — both must survive.
    nortaxa_int = [
        r for r in legacy
        if r["taxon_id"] == 20004 and r["source_system"] == "artsdatabanken"
    ]
    qmkyx = [r for r in auth if r["external_id"] == "QMKY_X"]
    assert nortaxa_int and qmkyx


def test_legacy_integer_id_emitted_as_string(tmp_path):
    result = _run_fixture_export(tmp_path)
    raw = (result.output_dir / "taxon_external_id_legacy_integer.jsonl").read_bytes()
    assert b'"external_id":"52796"' in raw
    assert b'"external_id":52796' not in raw  # integer form must not leak


def test_redlist_resolved_only_and_areas(tmp_path):
    result = _run_fixture_export(tmp_path)
    lines = (result.output_dir / "taxon_redlist.jsonl").read_text(encoding="utf-8").splitlines()
    rows = [json.loads(x) for x in lines]
    assert all(r["taxon_id"] is not None for r in rows)
    areas = {r["assessment_area"] for r in rows}
    assert areas == {"Norge", "Svalbard"}
    # Assessment name snapshot preserved as-is.
    cort_lim = [r for r in rows if r["scientific_name_snapshot"] == "Cortinarius limonius"][0]
    assert cort_lim["assessment_area"] == "Svalbard"
    assert cort_lim["assessed_name_namespace"] == "artsnavnebase_scientific_name_id"


def test_determinism_two_runs_byte_identical(tmp_path):
    """Two runs must produce byte-identical dataset files."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()

    # Two independent fixture roots (isolated tmp dirs).
    gz1, m1, _ = _wrap_sqlite_as_release(a)
    gz2, m2, _ = _wrap_sqlite_as_release(b)
    pol1 = _write_fixture_policies(a)
    pol2 = _write_fixture_policies(b)

    r1 = ce.run_export(
        artifact_gz=gz1, manifest=m1, output_dir=a / "out", policy_dir=pol1,
        generated_at="2099-01-01T00:00:00Z",
    )
    r2 = ce.run_export(
        artifact_gz=gz2, manifest=m2, output_dir=b / "out", policy_dir=pol2,
        generated_at="2099-01-01T00:00:00Z",
    )

    for name in ce.DATASET_FILES:
        assert r1.datasets[name].sha256 == r2.datasets[name].sha256, (
            f"non-deterministic dataset: {name}"
        )
    assert r1.whole_export_sha256 == r2.whole_export_sha256

    # Manifest bytes should also match (generated_at pinned via kwarg).
    assert r1.manifest_sha256 == r2.manifest_sha256


def test_manifest_records_namespace_counts(tmp_path):
    result = _run_fixture_export(tmp_path)
    manifest = json.loads(result.manifest_path.read_text())
    auth = manifest["external_id_authoritative_namespace_counts"]
    legacy = manifest["external_id_legacy_integer_source_counts"]
    # Authoritative keys are "source_system/namespace"; namespace never empty.
    assert "artsorakel/nbic_scientific_name_id" in auth
    assert "col_xr/col_usage_id" in auth
    for key in auth:
        assert not key.endswith("/"), (
            f"authoritative namespace count key {key!r} has empty namespace suffix"
        )
    # Legacy keys are just the source_system (namespace unavailable).
    assert "artsdatabanken" in legacy


def test_dangling_parent_references_reported(tmp_path):
    """Parent points outside scope → preserved in taxon.jsonl, listed in manifest."""
    result = _run_fixture_export(tmp_path)
    manifest = json.loads(result.manifest_path.read_text())
    dp = manifest["dangling_parent_references"]
    # Fixture: taxon_id 20 has parent_taxon_id=2 (Plantae kingdom) which is
    # excluded from scope. But 20 is Plantae's descendant, also excluded.
    # So the dangling case in the fixture is different — none exist by default.
    # Add a synthetic dangling row via a follow-up patch fixture.
    #
    # Just assert the manifest block exists and is well-formed here.
    assert set(dp.keys()) == {"count", "total_with_parent", "sample"}
    assert dp["count"] >= 0
    assert isinstance(dp["sample"], list)


def test_dangling_parent_preserved_verbatim(tmp_path):
    """A concept with a parent outside scope must keep parent_taxon_id verbatim."""
    # Build the fixture, then poke a dangling parent into the SQLite BEFORE
    # gzipping. Rebuild the artifact + manifest around the mutated content.
    art_dir = tmp_path / "artifact"
    art_dir.mkdir(exist_ok=True)
    sqlite_path = art_dir / "tax-2099.01.01-01.sqlite3"
    _make_fixture_sqlite(sqlite_path, release_id="tax-2099.01.01-01")

    # Inject: taxon_id 99001 is a NorTaxa concept whose parent 99999 does NOT
    # exist in taxon_min at all (an obviously dangling reference).
    conn = sqlite3.connect(sqlite_path)
    conn.execute(
        "INSERT INTO taxon_min("
        "taxon_id, parent_taxon_id, genus, specific_epithet, family, "
        "canonical_scientific_name, taxon_rank, taxonomic_status, "
        "source_system, sporely_content_release_id, canonical_source_system, "
        "canonical_external_id) VALUES "
        "(99001, 99999, 'Danglingus', '', NULL, 'Danglingus', 'genus', "
        "'accepted', 'nortaxa', 'tax-2099.01.01-01', 'nortaxa', 'N:DANG')"
    )
    conn.commit()
    conn.close()

    # Rebuild gz + manifest around the mutated SQLite.
    gz_path = art_dir / "tax-2099.01.01-01.sqlite3.gz"
    with sqlite_path.open("rb") as src, gzip.GzipFile(
        filename="", mode="wb", fileobj=gz_path.open("wb"), mtime=0
    ) as gz:
        while True:
            chunk = src.read(1 << 16)
            if not chunk:
                break
            gz.write(chunk)
    sq_sha = ce.sha256_file(sqlite_path)
    gz_sha = ce.sha256_file(gz_path)
    manifest = art_dir / "manifest.json"
    manifest.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": "tax-2099.01.01-01",
        "state": "candidate",
        "publication": "none",
        "gz_artifact": gz_path.name,
        "gz_sha256": gz_sha,
        "gz_bytes": gz_path.stat().st_size,
        "sqlite_sha256": sq_sha,
        "sqlite_bytes": sqlite_path.stat().st_size,
    }))
    sqlite_path.unlink()

    pol = _write_fixture_policies(tmp_path)
    result = ce.run_export(
        artifact_gz=gz_path, manifest=manifest, output_dir=tmp_path / "out",
        policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
    )

    # Manifest reports the dangling reference.
    m = json.loads(result.manifest_path.read_text())
    dp = m["dangling_parent_references"]
    assert dp["count"] >= 1
    dangling_ids = {s["taxon_id"] for s in dp["sample"]}
    assert 99001 in dangling_ids
    danglingus = next(s for s in dp["sample"] if s["taxon_id"] == 99001)
    assert danglingus["parent_taxon_id"] == 99999

    # And taxon.jsonl preserves the parent_taxon_id verbatim (never nulled).
    taxon_rows = [
        json.loads(x) for x in (result.output_dir / "taxon.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    row = next(r for r in taxon_rows if r["taxon_id"] == 99001)
    assert row["parent_taxon_id"] == 99999


def test_noop_returned_paths_point_to_existing_final(tmp_path):
    """A byte-identical second run must return paths that actually exist."""
    r1 = _run_fixture_export(tmp_path)
    r2 = _run_fixture_export(tmp_path)  # no --replace; should short-circuit

    assert r2.output_dir == r1.output_dir
    assert r2.output_dir.is_dir()
    assert r2.manifest_path.is_file()
    for name, ds in r2.datasets.items():
        assert ds.path.is_file(), f"{name}: {ds.path} does not exist"
        assert ds.path.parent == r2.output_dir
        assert ce.sha256_file(ds.path) == ds.sha256
    # Whole-export hash recomputed from existing files should match staged.
    assert r2.whole_export_sha256 == r1.whole_export_sha256


def test_manifest_missing_required_key_fails_closed(tmp_path):
    """Manifest without content_release_id must not silently fabricate one."""
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    m = json.loads(manifest.read_text())
    del m["content_release_id"]
    manifest.write_text(json.dumps(m))
    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="content_release_id"):
        ce.run_export(
            artifact_gz=gz, manifest=manifest, output_dir=tmp_path / "x",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


def test_taxonomy_release_has_no_fabricated_provenance(tmp_path):
    """Fixture manifest lacks canonical_authority/doi/etc → JSON emits null."""
    result = _run_fixture_export(tmp_path)
    line = (result.output_dir / "taxonomy_release.jsonl").read_text(encoding="utf-8").strip()
    obj = json.loads(line)
    # Fixture manifest never sets these; must be null, not a fabricated literal.
    for key in ("canonical_authority", "checklistbank_dataset_id", "doi"):
        assert obj[key] is None, (
            f"{key} was fabricated: {obj[key]!r}; must be null when source lacks it"
        )
    # nortaxa_release is derived from source_release[nortaxa].id = "nortaxa:test:2099-01-01"
    assert obj["nortaxa_release"] == "test"


def test_symlink_output_parent_rejected(tmp_path):
    """If output_dir passes through a symlink component, exporter refuses."""
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="symlink"):
        ce.run_export(
            artifact_gz=gz, manifest=manifest,
            output_dir=link / "child" / "cloud_export",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


def test_derived_nortaxa_authoritative_row(tmp_path):
    """taxon_min.norwegian_taxon_id → one derived authoritative row per concept."""
    result = _run_fixture_export(tmp_path)
    rows = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    derived = [
        r for r in rows if r.get("note") == "derived_from_taxon_min.norwegian_taxon_id"
    ]
    # Fixture sets norwegian_taxon_id on three concepts.
    assert len(derived) == 3
    for r in derived:
        assert isinstance(r["taxon_id"], int)
        assert isinstance(r["external_id"], str)
        assert r["source_system"] == "nortaxa"
        assert r["namespace"] == "nortaxa_taxon_id"
        assert r["id_role"] == "accepted"
        assert r["is_preferred"] is True
    # Values and external_name match canonical_scientific_name.
    by_taxon = {r["taxon_id"]: r for r in derived}
    assert by_taxon[20000]["external_id"] == "52796"
    assert by_taxon[20000]["external_name"] == "Cortinarius limonius"
    assert by_taxon[20004]["external_id"] == "56210"
    assert by_taxon[20004]["external_name"] == "Cantharellus cibarius"


def test_no_norwegian_taxon_id_no_derived_row(tmp_path):
    """Concepts without norwegian_taxon_id produce no derived NorTaxa row."""
    result = _run_fixture_export(tmp_path)
    rows = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    # Fixture concept 20001 ("SomeNortaxaOrder") has no norwegian_taxon_id.
    derived_for_20001 = [
        r for r in rows
        if r.get("taxon_id") == 20001
        and r.get("note") == "derived_from_taxon_min.norwegian_taxon_id"
    ]
    assert derived_for_20001 == []


def test_legacy_row_survives_alongside_derived(tmp_path):
    """The same numeric value may exist in both files without deduplication."""
    result = _run_fixture_export(tmp_path)
    auth = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    legacy = [
        json.loads(x)
        for x in (result.output_dir / "taxon_external_id_legacy_integer.jsonl")
        .read_text(encoding="utf-8").splitlines()
    ]
    # Concept 20000: derived authoritative row (nortaxa/nortaxa_taxon_id/52796)
    # AND legacy integer row (artsdatabanken source, external_id=52796). Both
    # must survive independently.
    derived = [
        r for r in auth
        if r["taxon_id"] == 20000
        and r["namespace"] == "nortaxa_taxon_id"
        and r["external_id"] == "52796"
    ]
    legacy_row = [
        r for r in legacy
        if r["taxon_id"] == 20000 and r["external_id"] == "52796"
    ]
    assert len(derived) == 1
    assert len(legacy_row) == 1


def test_duplicate_authoritative_key_fails(tmp_path):
    """Two rows with the same (source, namespace, external_id, taxon_id) fail."""
    art_dir = tmp_path / "artifact"
    art_dir.mkdir(exist_ok=True)
    sqlite_path = art_dir / "tax-2099.01.01-01.sqlite3"
    _make_fixture_sqlite(sqlite_path, release_id="tax-2099.01.01-01")

    conn = sqlite3.connect(sqlite_path)
    # Add a text-table row that duplicates the derived NorTaxa row for 20000:
    # source_system='nortaxa', namespace='nortaxa_taxon_id', external_id='52796'.
    conn.execute(
        "INSERT INTO taxon_external_id_text_min("
        "taxon_id, source_system, namespace, external_id, id_role, "
        "is_preferred, external_name, note) VALUES "
        "(20000, 'nortaxa', 'nortaxa_taxon_id', '52796', 'accepted', 1, "
        "'Cortinarius limonius', NULL)"
    )
    conn.commit()
    conn.close()

    gz_path = art_dir / "tax-2099.01.01-01.sqlite3.gz"
    with sqlite_path.open("rb") as src, gzip.GzipFile(
        filename="", mode="wb", fileobj=gz_path.open("wb"), mtime=0
    ) as gz:
        while True:
            chunk = src.read(1 << 16)
            if not chunk:
                break
            gz.write(chunk)
    sq_sha = ce.sha256_file(sqlite_path)
    gz_sha = ce.sha256_file(gz_path)
    manifest = art_dir / "manifest.json"
    manifest.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": "tax-2099.01.01-01",
        "state": "candidate",
        "publication": "none",
        "gz_artifact": gz_path.name,
        "gz_sha256": gz_sha,
        "gz_bytes": gz_path.stat().st_size,
        "sqlite_sha256": sq_sha,
        "sqlite_bytes": sqlite_path.stat().st_size,
    }))
    sqlite_path.unlink()

    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="duplicate authoritative"):
        ce.run_export(
            artifact_gz=gz_path, manifest=manifest, output_dir=tmp_path / "out",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


# ---------- post-emission taxon_id validator --------------------------


def test_post_emission_out_of_scope_taxon_id_fails(tmp_path):
    """A child JSONL row referencing an unknown taxon_id fails validation."""
    from database.taxonomy.cloud_export import _validate_emitted_taxon_id_references
    result = _run_fixture_export(tmp_path)
    # Append a bogus row with an out-of-scope taxon_id.
    bad = result.output_dir / "vernacular.jsonl"
    with bad.open("ab") as fh:
        fh.write(b'{"is_preferred_name":true,"language_code":"nb","source":null,"taxon_id":999999999,"vernacular_name":"impostor"}\n')
    with pytest.raises(ce.ExportError, match="taxon_id is not in the exported concept set"):
        _validate_emitted_taxon_id_references(
            result.output_dir, frozenset(result.scope.concept_ids)
        )


def test_post_emission_null_taxon_id_fails(tmp_path):
    from database.taxonomy.cloud_export import _validate_emitted_taxon_id_references
    result = _run_fixture_export(tmp_path)
    bad = result.output_dir / "vernacular.jsonl"
    with bad.open("ab") as fh:
        fh.write(b'{"taxon_id":null,"language_code":"nb","vernacular_name":"x","is_preferred_name":false,"source":null}\n')
    with pytest.raises(ce.ExportError, match="taxon_id must not be null"):
        _validate_emitted_taxon_id_references(
            result.output_dir, frozenset(result.scope.concept_ids)
        )


def test_post_emission_boolean_taxon_id_fails(tmp_path):
    from database.taxonomy.cloud_export import _validate_emitted_taxon_id_references
    result = _run_fixture_export(tmp_path)
    bad = result.output_dir / "vernacular.jsonl"
    with bad.open("ab") as fh:
        fh.write(b'{"taxon_id":true,"language_code":"nb","vernacular_name":"x","is_preferred_name":false,"source":null}\n')
    with pytest.raises(ce.ExportError, match="not boolean"):
        _validate_emitted_taxon_id_references(
            result.output_dir, frozenset(result.scope.concept_ids)
        )


def test_post_emission_string_taxon_id_fails(tmp_path):
    from database.taxonomy.cloud_export import _validate_emitted_taxon_id_references
    result = _run_fixture_export(tmp_path)
    bad = result.output_dir / "vernacular.jsonl"
    with bad.open("ab") as fh:
        fh.write(b'{"taxon_id":"10001","language_code":"nb","vernacular_name":"x","is_preferred_name":false,"source":null}\n')
    with pytest.raises(ce.ExportError, match="not a string"):
        _validate_emitted_taxon_id_references(
            result.output_dir, frozenset(result.scope.concept_ids)
        )


def test_post_emission_missing_taxon_id_fails(tmp_path):
    from database.taxonomy.cloud_export import _validate_emitted_taxon_id_references
    result = _run_fixture_export(tmp_path)
    bad = result.output_dir / "vernacular.jsonl"
    with bad.open("ab") as fh:
        fh.write(b'{"language_code":"nb","vernacular_name":"x","is_preferred_name":false,"source":null}\n')
    with pytest.raises(ce.ExportError, match="missing taxon_id field"):
        _validate_emitted_taxon_id_references(
            result.output_dir, frozenset(result.scope.concept_ids)
        )


# ---------- existing-manifest validation -----------------------------


def test_idempotent_rerun_returns_existing_generated_at(tmp_path):
    """A byte-identical rerun returns the FIRST run's generated_at."""
    a = _run_fixture_export(tmp_path)
    first_generated = json.loads(a.manifest_path.read_text())["generated_at"]

    # Second run supplies a different generated_at intentionally.
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    pol = _write_fixture_policies(tmp_path)
    b = ce.run_export(
        artifact_gz=gz,
        manifest=manifest,
        output_dir=tmp_path / "cloud_export",
        policy_dir=pol,
        generated_at="2100-06-15T12:00:00Z",  # deliberately different
    )
    assert b.generated_at == first_generated
    # And the persisted manifest still carries the ORIGINAL generated_at.
    assert json.loads(b.manifest_path.read_text())["generated_at"] == first_generated


def test_idempotent_rerun_leaves_no_staging_dir(tmp_path):
    """After a no-op rerun, no `.<name>.staging.*` or `.replaced.*` dir remains."""
    _run_fixture_export(tmp_path)
    _run_fixture_export(tmp_path)
    parent = (tmp_path / "cloud_export").parent
    leftovers = [
        p for p in parent.iterdir()
        if p.name.startswith(".cloud_export.")
    ]
    assert not leftovers, f"unexpected staging leftovers: {leftovers}"


def test_manifest_hash_tampering_forces_replacement(tmp_path):
    """Mutating a recorded per-file hash in the manifest breaks validation."""
    _run_fixture_export(tmp_path)
    mpath = tmp_path / "cloud_export" / ce.MANIFEST_FILENAME
    m = json.loads(mpath.read_text())
    m["files"][1]["sha256"] = "00" * 32   # taxon.jsonl
    mpath.write_text(json.dumps(m))
    # A rerun without --replace should refuse.
    with pytest.raises(ce.ExportError, match="differs"):
        _run_fixture_export(tmp_path, replace=False)


def test_manifest_scope_metadata_tampering_forces_replacement(tmp_path):
    """Mutating scope/release metadata in the manifest breaks validation."""
    _run_fixture_export(tmp_path)
    mpath = tmp_path / "cloud_export" / ce.MANIFEST_FILENAME
    m = json.loads(mpath.read_text())
    m["content_release_id"] = "tax-1900.01.01-01"  # stale/forged
    mpath.write_text(json.dumps(m))
    with pytest.raises(ce.ExportError, match="differs"):
        _run_fixture_export(tmp_path, replace=False)


# ---------- verify_only ---------------------------------------------


def test_verify_only_reports_dangling_parents(tmp_path):
    """--verify-only must run the real dangling audit, not return zeros."""
    art_dir = tmp_path / "artifact"
    art_dir.mkdir(exist_ok=True)
    sqlite_path = art_dir / "tax-2099.01.01-01.sqlite3"
    _make_fixture_sqlite(sqlite_path, release_id="tax-2099.01.01-01")

    conn = sqlite3.connect(sqlite_path)
    conn.execute(
        "INSERT INTO taxon_min("
        "taxon_id, parent_taxon_id, genus, specific_epithet, family, "
        "canonical_scientific_name, taxon_rank, taxonomic_status, "
        "source_system, sporely_content_release_id, canonical_source_system, "
        "canonical_external_id) VALUES "
        "(99001, 99999, 'Danglingus', '', NULL, 'Danglingus', 'genus', "
        "'accepted', 'nortaxa', 'tax-2099.01.01-01', 'nortaxa', 'N:DANG')"
    )
    conn.commit()
    conn.close()

    gz_path = art_dir / "tax-2099.01.01-01.sqlite3.gz"
    with sqlite_path.open("rb") as src, gzip.GzipFile(
        filename="", mode="wb", fileobj=gz_path.open("wb"), mtime=0
    ) as gz:
        while True:
            chunk = src.read(1 << 16)
            if not chunk:
                break
            gz.write(chunk)
    sq_sha = ce.sha256_file(sqlite_path)
    gz_sha = ce.sha256_file(gz_path)
    manifest = art_dir / "manifest.json"
    manifest.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": "tax-2099.01.01-01",
        "state": "candidate",
        "publication": "none",
        "gz_artifact": gz_path.name,
        "gz_sha256": gz_sha,
        "gz_bytes": gz_path.stat().st_size,
        "sqlite_sha256": sq_sha,
        "sqlite_bytes": sqlite_path.stat().st_size,
    }))
    sqlite_path.unlink()

    pol = _write_fixture_policies(tmp_path)
    result = ce.run_export(
        artifact_gz=gz_path, manifest=manifest,
        output_dir=tmp_path / "would_not_write",
        policy_dir=pol, verify_only=True,
        generated_at="2099-01-01T00:00:00Z",
    )
    assert result.dangling_parents.count >= 1
    ids = {s["taxon_id"] for s in result.dangling_parents.sample}
    assert 99001 in ids
    # verify-only writes nothing.
    assert not (tmp_path / "would_not_write").exists()


def test_traversal_token_rejected(tmp_path):
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="traversal"):
        ce.run_export(
            artifact_gz=gz, manifest=manifest,
            output_dir=Path(str(tmp_path)) / "..",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


def test_atomic_replace_flag_required(tmp_path):
    _run_fixture_export(tmp_path)
    # Mutate the existing output so a second run detects a difference.
    (tmp_path / "cloud_export" / "taxon.jsonl").write_bytes(b"{}\n")
    with pytest.raises(ce.ExportError, match="differs"):
        _run_fixture_export(tmp_path, replace=False)
    # With replace=True the run must succeed.
    r2 = _run_fixture_export(tmp_path, replace=True)
    assert r2.output_dir.is_dir()


def test_source_hash_mismatch_fails(tmp_path):
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path)
    # Corrupt the outer manifest gz_sha256.
    m = json.loads(manifest.read_text())
    m["gz_sha256"] = "00" * 32
    manifest.write_text(json.dumps(m))
    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="gzip SHA-256 mismatch"):
        ce.run_export(
            artifact_gz=gz, manifest=manifest, output_dir=tmp_path / "x",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


def test_release_id_regex_guard(tmp_path):
    gz, manifest, _ = _wrap_sqlite_as_release(tmp_path, release_id="tax-2099.01.01-01")
    m = json.loads(manifest.read_text())
    m["content_release_id"] = "../../etc/passwd"
    manifest.write_text(json.dumps(m))
    pol = _write_fixture_policies(tmp_path)
    with pytest.raises(ce.ExportError, match="unsafe"):
        ce.run_export(
            artifact_gz=gz, manifest=manifest, output_dir=tmp_path / "x",
            policy_dir=pol, generated_at="2099-01-01T00:00:00Z",
        )


# ---------- pinned-release regression ----------------------------------


_PINNED_GZ = Path(
    "database/reference_data/generated/taxonomy_v2/tax-2026.07.30-02.sqlite3.gz"
)
_PINNED_MANIFEST = Path(
    "database/reference_data/generated/taxonomy_v2/manifest.json"
)


@pytest.mark.skipif(
    not (_REPO / _PINNED_GZ).is_file() or not (_REPO / _PINNED_MANIFEST).is_file(),
    reason="pinned taxonomy-v2 artifact not present in this checkout",
)
def test_pinned_release_regression_counts(tmp_path):
    result = ce.run_export(
        artifact_gz=_REPO / _PINNED_GZ,
        manifest=_REPO / _PINNED_MANIFEST,
        output_dir=tmp_path / "cloud_export_tax-2026.07.30-02",
        policy_dir=_REPO / "database" / "taxonomy" / "policies",
        generated_at="2026-07-31T00:00:00Z",
    )
    exp = ce.PINNED_RELEASE_EXPECTATIONS
    assert len(result.scope.concept_ids) == exp["concepts_included"]
    assert result.scope.excluded_count == exp["concepts_excluded"]
    ds = result.datasets
    assert ds["taxon.jsonl"].row_count == exp["concepts_included"]
    assert ds["scientific_name.jsonl"].row_count == exp["scientific_name_rows"]
    assert ds["vernacular.jsonl"].row_count == exp["vernacular_rows"]
    assert ds["taxon_external_id.jsonl"].row_count == exp["external_authoritative_total_rows"]
    assert ds["taxon_external_id_legacy_integer.jsonl"].row_count == exp["external_legacy_int_rows"]
    assert ds["taxon_redlist.jsonl"].row_count == exp["redlist_rows"]


@pytest.mark.skipif(
    not (_REPO / _PINNED_GZ).is_file() or not (_REPO / _PINNED_MANIFEST).is_file(),
    reason="pinned taxonomy-v2 artifact not present",
)
def test_pinned_release_determinism(tmp_path):
    kwargs = dict(
        artifact_gz=_REPO / _PINNED_GZ,
        manifest=_REPO / _PINNED_MANIFEST,
        policy_dir=_REPO / "database" / "taxonomy" / "policies",
        generated_at="2026-07-31T00:00:00Z",
    )
    a = ce.run_export(output_dir=tmp_path / "a", **kwargs)
    b = ce.run_export(output_dir=tmp_path / "b", **kwargs)
    assert a.whole_export_sha256 == b.whole_export_sha256
    assert a.manifest_sha256 == b.manifest_sha256
    for name in ce.DATASET_FILES:
        assert a.datasets[name].sha256 == b.datasets[name].sha256
