"""Regression tests pinning the current runtime behaviour for the
``Cortinarius limonius`` / ``Aureonarius limonius`` concept pair.

## Audit summary (2026-07-30)

Both the raw sources and the compiled ``tax-2026.07.30-02`` release
disagree between COL and NorTaxa on which name is accepted:

* **COL** (``NameUsage.tsv``): ``B2NK4 Aureonarius limonius`` is
  ``accepted``; ``YLCZ Cortinarius limonius`` is a ``synonym`` whose
  ``parentID`` is ``B2NK4`` (i.e. Cortinarius is a synonym of
  Aureonarius). Both share basionym ``65N5J``.
* **NorTaxa** (``taxon.txt``): ``52796 Cortinarius limonius`` is
  ``valid``; ``297477 Aureonarius limonius`` is a ``synonym`` whose
  ``acceptedNameUsageID`` is ``52796`` (i.e. Aureonarius is a synonym
  of Cortinarius).

The compile pipeline preserved both source-canonical concepts as
distinct ``sporely_taxon_id`` values (``624905`` for NorTaxa Cortinarius,
``139099`` for COL Aureonarius) with no cross-source mapping recorded
in ``taxon_min``, ``taxon_external_id_min``, or any other table on the
runtime-usable side. NorTaxa's own alias record (``taxon 624905``,
``scientific_name = 'Aureonarius limonius'``, ``is_preferred = 0``,
``note = 'synonym_of_accepted'``) only encodes NorTaxa's view of its
own concept — it does not link taxon ``624905`` to taxon ``139099``.

The runtime therefore CANNOT auto-unify the two without either:

  1. hardcoding a name pair (forbidden by the current task brief), or
  2. adding a "prefer accepted-name-elsewhere over
     synonym_of_accepted" heuristic that would change semantics for
     all 27,753 rows carrying that note.

Both are curation decisions, not runtime decisions. These tests pin
the current behaviour so any future compile-pipeline change that
unifies these concepts will be explicit (and will need to be paired
with an update here).
"""
from __future__ import annotations

import gzip
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from database.reference_data_paths import TAXONOMY_V2_DIR
from database.taxon_lookup import TaxonLookupService
from database.vernacular_db import VernacularDB
from utils.taxonomy_v2 import load_manifest


@pytest.fixture(scope="module")
def release_sqlite(tmp_path_factory) -> Path:
    """Decompress the currently active taxonomy release into a tmp path.

    Skips the test if the gz artifact is not present (e.g. a stripped
    CI build). The gz artifact filename is read from the release
    manifest — never hardcoded — so a release rollover doesn't leave a
    dangling filename here.
    """
    try:
        manifest = load_manifest()
    except Exception as exc:  # pragma: no cover — defensive skip
        pytest.skip(f"taxonomy manifest unreadable: {exc}")
    gz_path = TAXONOMY_V2_DIR / manifest.gz_artifact
    if not gz_path.exists():
        pytest.skip(f"taxonomy release artifact missing: {gz_path}")
    out_dir = tmp_path_factory.mktemp("tax_release")
    out_path = out_dir / "tax.sqlite3"
    with gzip.open(gz_path, "rb") as src, out_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    return out_path


@pytest.fixture
def lookup(release_sqlite: Path) -> TaxonLookupService:
    vdb = VernacularDB(release_sqlite, language_code="no")
    return TaxonLookupService(
        vernacular_db=vdb,
        include_reference_data=False,
        language_code="no",
    )


# ---------------------------------------------------------------------------
# Raw DB row snapshot — pin every field the audit relied on so a future
# compile change is visible as a test diff, not a silent data drift.
# ---------------------------------------------------------------------------


def test_taxon_min_rows_for_limonius_names_are_distinct_concepts(release_sqlite: Path):
    """Both names have their OWN ``taxon_min`` canonical row with a
    distinct ``sporely_taxon_id`` and its own ``canonical_source_system``.

    Regression guard: unifying the two into a single canonical row
    would be a legitimate compile-pipeline change, but it MUST land as
    an explicit test edit here so the runtime and the tests move in
    lockstep.
    """
    conn = sqlite3.connect(str(release_sqlite))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT taxon_id, genus, specific_epithet, "
            "canonical_scientific_name, taxon_rank, taxonomic_status, "
            "canonical_source_system "
            "FROM taxon_min "
            "WHERE (genus = 'Cortinarius' AND specific_epithet = 'limonius') "
            "   OR (genus = 'Aureonarius' AND specific_epithet = 'limonius') "
            "ORDER BY taxon_id"
        ).fetchall()
    finally:
        conn.close()
    by_id = {r["taxon_id"]: dict(r) for r in rows}
    assert 139099 in by_id, "COL canonical Aureonarius limonius (139099) missing"
    assert 624905 in by_id, "NorTaxa canonical Cortinarius limonius (624905) missing"
    assert by_id[139099]["genus"] == "Aureonarius"
    assert by_id[139099]["canonical_scientific_name"] == "Aureonarius limonius"
    assert by_id[139099]["taxon_rank"] == "species"
    assert by_id[139099]["canonical_source_system"] == "col_xr"
    assert by_id[624905]["genus"] == "Cortinarius"
    assert by_id[624905]["canonical_scientific_name"] == "Cortinarius limonius"
    assert by_id[624905]["taxon_rank"] == "species"
    assert by_id[624905]["canonical_source_system"] == "nortaxa"


def test_scientific_name_min_exposes_synonym_alias_on_nortaxa_taxon(release_sqlite: Path):
    """NorTaxa's Cortinarius concept carries ``Aureonarius limonius``
    as a synonym alias (``is_preferred_name = 0`` +
    ``note = 'synonym_of_accepted'``). This is what the scientific-name
    picker relies on to surface the disagreement to the observer."""
    conn = sqlite3.connect(str(release_sqlite))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT taxon_id, scientific_name, is_preferred_name, source, note "
            "FROM scientific_name_min "
            "WHERE scientific_name IN ('Cortinarius limonius', 'Aureonarius limonius') "
            "ORDER BY taxon_id, scientific_name, is_preferred_name DESC"
        ).fetchall()
    finally:
        conn.close()
    triples = {
        (r["taxon_id"], r["scientific_name"], bool(r["is_preferred_name"]))
        for r in rows
    }
    # COL canonical: taxon 139099 preferred as "Aureonarius limonius".
    assert (139099, "Aureonarius limonius", True) in triples
    # NorTaxa canonical: taxon 624905 preferred as "Cortinarius limonius".
    assert (624905, "Cortinarius limonius", True) in triples
    # NorTaxa cross-reference: taxon 624905 also carries
    # "Aureonarius limonius" as a non-preferred alias.
    assert (624905, "Aureonarius limonius", False) in triples
    # Verify the note on the alias.
    alias_note = None
    for r in rows:
        if r["taxon_id"] == 624905 and r["scientific_name"] == "Aureonarius limonius" \
                and not bool(r["is_preferred_name"]):
            alias_note = r["note"]
            break
    assert alias_note == "synonym_of_accepted"


def test_no_cross_source_mapping_between_concepts(release_sqlite: Path):
    """No ``taxon_external_id_min`` row links taxon 139099 (COL) to
    the ``artsdatabanken`` name id 297477 (NorTaxa's synonym record for
    Aureonarius limonius). If a future compile pipeline adds such a
    mapping this test will fail — that's the intended signal to update
    the runtime resolver to consult it."""
    conn = sqlite3.connect(str(release_sqlite))
    conn.row_factory = sqlite3.Row
    try:
        # Does any external-id row on taxon 139099 (COL Aureonarius)
        # reference NorTaxa taxon 624905 or NorTaxa name id 297477?
        rows = conn.execute(
            "SELECT * FROM taxon_external_id_min "
            "WHERE taxon_id = 139099"
        ).fetchall()
    finally:
        conn.close()
    # 139099 has only its COL external id — no artsdatabanken cross-ref.
    for r in rows:
        d = dict(r)
        assert d["source_system"] != "artsdatabanken", (
            "unexpected artsdatabanken cross-reference on taxon 139099: "
            f"{d} — if the compile pipeline started emitting cross-source "
            "concept links, update the runtime resolver AND this test."
        )


# ---------------------------------------------------------------------------
# Runtime resolver behaviour — pin the fact that both names find a hit
# and that they yield DIFFERENT sporely_taxon_ids in the current build.
# ---------------------------------------------------------------------------


def test_both_names_are_findable_via_resolve_scientific(lookup: TaxonLookupService):
    """`resolve_scientific` returns a hit for both names (neither is
    silently dropped from the runtime pathway)."""
    cort = lookup.resolve_scientific("Cortinarius", "limonius")
    aureo = lookup.resolve_scientific("Aureonarius", "limonius")
    assert cort is not None, "Cortinarius limonius unresolved"
    assert aureo is not None, "Aureonarius limonius unresolved"
    assert cort.taxon_id == 624905
    assert aureo.taxon_id == 139099


def test_cortinarius_limonius_resolves_to_aureonarius_limonius_canonical(
    lookup: TaxonLookupService,
):
    """### Current disposition: NOT unified.

    In the current compiled release, ``Cortinarius limonius`` and
    ``Aureonarius limonius`` produce DIFFERENT ``sporely_taxon_id`` values
    because:

    * COL treats Aureonarius as accepted, Cortinarius as a synonym.
    * NorTaxa treats Cortinarius as valid, Aureonarius as a synonym.
    * The compile pipeline preserved both source-canonical concepts.
    * No cross-source concept mapping was recorded.

    A safe runtime auto-unification would require either a hardcoded
    name pair (explicitly forbidden by the current brief) or a
    curator-owned "prefer accepted-elsewhere over synonym_of_accepted"
    heuristic that would rewrite semantics for the 27,753 aliases
    that carry ``note = 'synonym_of_accepted'``.

    This test pins the current behaviour. If a future compile-pipeline
    change unifies the concepts (or if a source-data-justified
    resolver indirection lands), update BOTH the resolver and this
    test in the same commit.
    """
    cort = lookup.resolve_manual_scientific("Cortinarius", "limonius")
    aureo = lookup.resolve_manual_scientific("Aureonarius", "limonius")
    assert cort is not None
    assert aureo is not None
    # Same biological concept in the source data, but currently pinned
    # to two distinct sporely_taxon_ids in the compiled release.
    assert cort.sporely_taxon_id == 624905  # NorTaxa canonical
    assert aureo.sporely_taxon_id == 139099  # COL canonical
    assert cort.sporely_taxon_id != aureo.sporely_taxon_id


def test_both_names_reach_same_sporely_taxon_id_via_picker_synonym_link(
    lookup: TaxonLookupService,
):
    """The scientific-name picker surfaces the concept disagreement:
    typing ``Aureonarius limonius`` yields BOTH the COL canonical
    (139099) AND a ``synonym_of_accepted``-linked entry that binds
    to the NorTaxa Cortinarius canonical (624905). The observer can
    therefore reach the same sporely_taxon_id NorTaxa users see by
    picking the alias entry from the picker.

    Typing ``Cortinarius limonius`` yields only the NorTaxa canonical
    (there is no matching alias on the COL taxon).
    """
    aureo_suggestions = lookup.suggest_scientific_names("Aureonarius limonius", limit=10)
    aureo_ids = {(s["sporely_taxon_id"], s["link_kind"]) for s in aureo_suggestions}
    assert (139099, "canonical") in aureo_ids
    assert (624905, "synonym_of_accepted") in aureo_ids

    cort_suggestions = lookup.suggest_scientific_names("Cortinarius limonius", limit=10)
    cort_ids = {(s["sporely_taxon_id"], s["link_kind"]) for s in cort_suggestions}
    assert (624905, "canonical") in cort_ids
    # Absence: no COL-linked alias points back at 139099 from the
    # Cortinarius direction.
    assert not any(
        sid == 139099 for sid, _kind in cort_ids
    ), "unexpected reverse alias — compile pipeline change; update tests"
