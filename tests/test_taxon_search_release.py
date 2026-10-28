"""Unified taxon search against the bundled taxonomy release.

Representative results and broad-query timing on the real artifact
(`database/reference_data/generated/taxonomy_v2`, or the database named by
``SPORELY_TAXONOMY_PROBE_DB``). The ids below are those of
``tax-2026.10.23-11``; a later release may renumber them, so the module skips
when the bundle is another release and no override is given.
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sqlite3
import statistics
import time
from pathlib import Path

import pytest

from database.taxon_search import (
    KIND_SYNONYM,
    MATCH_EPITHET,
    MATCH_VERNACULAR_PART,
    TIER_EXACT,
    search,
)

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "database/reference_data/generated/taxonomy_v2"
RELEASE = "tax-2026.10.23-11"

# Warm median per query. The searches measured 2-30 ms on a 2024 laptop;
# the ceiling leaves room for slower CI runners while still catching an
# accidental full scan per row or a widening loop that never ends.
WARM_MEDIAN_BUDGET_MS = 150.0


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
    override = os.environ.get("SPORELY_TAXONOMY_PROBE_DB")
    if override:
        path = Path(override)
    else:
        manifest = json.loads((BUNDLE / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("content_release_id") != RELEASE:
            pytest.skip(f"bundled release is {manifest.get('content_release_id')}, ids are for {RELEASE}")
        path = tmp_path_factory.mktemp("release") / "release.sqlite3"
        with gzip.open(BUNDLE / manifest["gz_artifact"], "rb") as src, path.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    yield connection
    connection.close()


def run(conn, query, **kwargs):
    kwargs.setdefault("language_codes", ("nb", "no", "nn"))
    return search(conn, query, **kwargs)


def ids(row) -> list[int]:
    return [c.sporely_taxon_id for c in row.concepts]


def find(page, name, kind="taxon"):
    rows = [r for r in page.rows if r.name == name and r.kind == kind]
    assert rows, [(r.name, r.kind) for r in page.rows]
    return rows[0]


def test_genus_query_offers_both_flammulina_concepts_first(conn):
    page = run(conn, "Flammulina", limit=10)
    genus = page.rows[0]
    assert (genus.name, genus.rank, genus.tier) == ("Flammulina", "genus", TIER_EXACT)
    assert ids(genus) == [625619, 50109]  # NorTaxa (Artsdatabanken 54695), then COL
    assert genus.vernacular_name == "vintersopper"
    assert page.has_more
    assert all(r.name.startswith("Flammulina ") for r in page.rows[1:])


def test_epithet_query_spans_genera_with_pairs_and_synonyms(conn):
    page = run(conn, "velutipes")
    assert all(r.match_kind == MATCH_EPITHET for r in page.rows)
    assert ids(find(page, "Flammulina velutipes")) == [625621, 98288]
    assert {"Arrhenia velutipes", "Aureoboletus velutipes", "Conocybe velutipes"} <= {r.name for r in page.rows}
    synonym = find(page, "Collybia velutipes", KIND_SYNONYM)
    assert ids(synonym) == [625621] and synonym.synonym_of == "Flammulina velutipes"


def test_artsorakel_evidence_reorders_without_removing(conn):
    plain = run(conn, "velutipes")
    proven = run(conn, "velutipes", evidence_taxon_ids=[98288])
    assert {tuple(sorted(ids(r))) for r in proven.rows} == {tuple(sorted(ids(r))) for r in plain.rows}
    assert proven.rows[0].name == "Flammulina velutipes"
    assert ids(proven.rows[0]) == [98288, 625621]


def test_vernacular_exact_and_group_queries(conn):
    assert ids(run(conn, "rød fluesopp").rows[0]) == [78915]
    kremler = run(conn, "kremler")
    assert ids(kremler.rows[0]) == [123322] and kremler.rows[0].rank == "genus"
    green = run(conn, "grønnkremle").rows[0]
    assert ids(green) == [626822, 61673]
    assert all(r.match_kind == MATCH_VERNACULAR_PART for r in kremler.rows[1:])
    assert all(r.default_concept.canonical_scientific_name.startswith("Russula") for r in kremler.rows[1:])


def test_hyphenated_epithet(conn):
    assert ids(run(conn, "Cortinarius albo-ochraceus").rows[0]) == [92473]


def test_same_source_homonyms_stay_apart(conn):
    rows = [r for r in run(conn, "Aschersonia").rows if r.name == "Aschersonia"]
    assert sorted(ids(r)[0] for r in rows) == [980, 131914]
    assert all(r.shares_name and not r.has_source_choice for r in rows)


def test_aggregate_and_species_are_distinct_rows(conn):
    page = run(conn, "Russula queletii")
    assert ids(find(page, "Russula queletii")) == [62433]
    assert ids(find(page, "Russula queletii coll.")) == [626865]


def test_large_genus_pages_never_repeat_or_split_a_name(conn):
    seen: list[tuple] = []
    offset = 0
    for _ in range(8):
        page = run(conn, "Russula", offset=offset, limit=40)
        seen.extend((r.kind, r.name, r.rank, tuple(ids(r))) for r in page.rows)
        offset += 40
        if not page.has_more:
            break
    assert len(seen) == len(set(seen)) == 320
    # After the genus row the species follow in name order across pages.
    names = [s[1] for s in seen[1:] if s[0] == "taxon"]
    assert names == sorted(names)
    # A concept may appear once under its own name and again under a synonym,
    # but never twice in one row or under one name twice.
    assert all(len(s[3]) == len(set(s[3])) for s in seen)
    by_name = [(s[0], s[1], i) for s in seen for i in s[3]]
    assert len(by_name) == len(set(by_name))


BROAD_QUERIES = [
    "ru", "ab", "co",  # two letters: many prefix hits
    "Russula", "Cortinarius", "Inocybe",  # large genera
    "sopp", "kremle", "slørsopp", "rød",  # broad Norwegian word parts
    "velutipes", "alba", "rufescens",  # epithets in many genera
    "Flammulina vel", "Amanita mus",  # narrowing scientific names
    "xqzt",  # no match at all
]


@pytest.mark.parametrize("query", BROAD_QUERIES)
def test_broad_query_timing(conn, query):
    run(conn, query)  # warm the page cache
    times = []
    for _ in range(3):
        start = time.perf_counter()
        page = run(conn, query, offset=0, limit=40)
        times.append((time.perf_counter() - start) * 1000)
    median = statistics.median(times)
    print(f"{query!r}: {median:.1f} ms, {len(page.rows)} rows, has_more={page.has_more}")
    assert median < WARM_MEDIAN_BUDGET_MS
    assert len(page.rows) <= 40
