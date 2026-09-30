"""Permanent regression probes for the bundled taxonomy-v2 release.

Each probe runs the desktop's own lookup against the release that ships in
``database/reference_data/generated/taxonomy_v2``. They pin known cases:
publishing ids, contradictory legacy iNaturalist ids, provider identity
bridges and vernacular coverage.

Set ``SPORELY_TAXONOMY_PROBE_DB`` to a candidate SQLite to run the same probes
against a release before it is promoted.
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sqlite3
from pathlib import Path

import pytest

import utils.vernacular_utils as vernacular_utils
from database.models import ObservationDB
from database.taxon_lookup import resolve_installed_external_identity

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / "database/reference_data/generated/taxonomy_v2"
OVERLAY = ROOT / "database/taxonomy/overlays/artportalen-publishing.json"
PROPOSALS = ROOT / "database/taxonomy/evidence/artportalen-overlay/artportalen-proposals.json"
RECIPE = ROOT / "database/taxonomy/release-recipe.json"


@pytest.fixture(scope="module")
def release_db(tmp_path_factory) -> Path:
    override = os.environ.get("SPORELY_TAXONOMY_PROBE_DB")
    if override:
        return Path(override)
    manifest = json.loads((BUNDLE / "manifest.json").read_text(encoding="utf-8"))
    target = tmp_path_factory.mktemp("release") / "release.sqlite3"
    with gzip.open(BUNDLE / manifest["gz_artifact"], "rb") as src, target.open("wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    return target


@pytest.fixture
def resolve(release_db, monkeypatch):
    monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda lang_code=None: release_db)
    return ObservationDB.resolve_external_taxon_id


def _refresh_document() -> dict:
    recipe = json.loads(RECIPE.read_text(encoding="utf-8"))
    return json.loads((ROOT / recipe["inaturalist_refresh"]["path"]).read_text(encoding="utf-8"))


def _refresh() -> dict:
    return {e["scientific_name"]: e for e in _refresh_document()["entries"]}


# --------------------------------------------------------------- publishing

def test_cantharellus_cibarius_publishing_ids(resolve):
    assert resolve("Cantharellus", "cibarius", "artportalen") == 3213
    assert resolve("Cantharellus", "cibarius", "inaturalist") == 47347


def test_amanita_muscaria_artportalen_follows_the_reviewed_overlay(resolve):
    """Artportalen splits A. muscaria (s.lat. 2976 / s.str. 236537); only a
    reviewed overlay decision may give it an Artportalen id. The release has
    two A. muscaria concepts (COL and an unmapped NorTaxa concept); both are
    split proposals."""
    proposals = [e for e in json.loads(PROPOSALS.read_text(encoding="utf-8"))["entries"]
                 if e["scientific_name"] == "Amanita muscaria"]
    assert proposals and all(e["classification"] == "split" for e in proposals)
    decided = [e for e in json.loads(OVERLAY.read_text(encoding="utf-8"))["entries"]
               if e["scientific_name"] == "Amanita muscaria"]
    assert all(e["decision"] == "accepted_after_review:split" for e in decided)
    assert {e["sporely_taxon_id"] for e in decided} <= {e["sporely_taxon_id"] for e in proposals}
    expected = decided[0]["artportalen_taxon_id"] if decided else None
    assert len(decided) <= 1
    assert resolve("Amanita", "muscaria", "artportalen") == expected


def test_amanita_muscaria_inaturalist_id_comes_from_fresh_validation(resolve, release_db):
    entry = _refresh()["Amanita muscaria"]
    assert entry["legacy_inaturalist_ids"] == [48715]  # legacy id shared with A. gemmata
    assert entry["status"] == "resolved" and entry["inaturalist"]["taxon_id"] == 48715
    assert entry["checks"]["kingdom_fungi"] and entry["checks"]["genus_matches"]
    assert resolve("Amanita", "muscaria", "inaturalist") == 48715
    notes = [note for (note,) in sqlite3.connect(release_db).execute(
        "SELECT e.note FROM taxon_external_id_min e JOIN taxon_min t ON t.taxon_id = e.taxon_id "
        "WHERE t.canonical_scientific_name = 'Amanita muscaria' AND e.source_system = 'inaturalist'")]
    # Since Stage 3R the entry, reviewed on a concept a supersession retired,
    # is re-keyed onto the surviving concept, and the note keeps the retired
    # id and the supersession as provenance.
    refresh_note = f"inaturalist_refresh:{_refresh_document()['acquired_on']}"
    assert len(notes) == 1
    assert notes[0] == refresh_note or notes[0].startswith(refresh_note + ";superseded_from:")
    # The other holder of the legacy id did not validate and gets nothing.
    assert _refresh()["Amanita gemmata"]["status"] == "unresolved"
    assert resolve("Amanita", "gemmata", "inaturalist") is None


def test_boletus_edulis_contradictory_legacy_inaturalist_id(resolve):
    refresh = _refresh()
    assert refresh["Boletus edulis"]["legacy_inaturalist_ids"] == [48701]
    assert refresh["Boletus pinetorum"]["legacy_inaturalist_ids"] == [48701]
    assert resolve("Boletus", "edulis", "inaturalist") == 48701     # freshly validated
    assert resolve("Boletus", "pinetorum", "inaturalist") is None   # a synonym on iNaturalist


def test_no_inaturalist_lookup_id_is_shared_by_two_concepts(release_db):
    conn = sqlite3.connect(release_db)
    shared = conn.execute(
        "SELECT inaturalist_taxon_id, COUNT(*) FROM taxon_min WHERE inaturalist_taxon_id IS NOT NULL "
        "GROUP BY 1 HAVING COUNT(*) > 1").fetchall()
    assert shared == []
    # Every lookup id is either a one-to-one legacy pairing or a fresh validation.
    legacy_duplicates = conn.execute(
        "SELECT t.taxon_id FROM taxon_min t WHERE t.inaturalist_taxon_id IN ("
        "  SELECT external_id FROM taxon_external_id_min WHERE source_system = 'inaturalist' "
        "  AND note LIKE 'legacy_compat:%' GROUP BY external_id HAVING COUNT(DISTINCT taxon_id) > 1)").fetchall()
    assert legacy_duplicates == []


# --------------------------------------------------------------- vernaculars

def test_swedish_and_english_common_names_are_present(release_db):
    conn = sqlite3.connect(release_db)
    names = set(conn.execute(
        "SELECT v.language_code, v.vernacular_name FROM vernacular_min v JOIN taxon_min t USING (taxon_id) "
        "WHERE t.canonical_scientific_name = 'Cantharellus cibarius'"))
    assert ("sv", "Kantarell") in names and ("en", "Chanterelle") in names and ("nb", "kantarell") in names
    counts = dict(conn.execute("SELECT language_code, COUNT(*) FROM vernacular_min GROUP BY 1"))
    assert counts.get("sv", 0) > 4000 and counts.get("en", 0) > 3000


# --------------------------------------------------------------- identity

def _provider(release_db, source: str, external_id: str):
    concept = resolve_installed_external_identity(release_db, source_system=source,
                                                  namespace=f"{source}_taxon_id", external_id=external_id)
    return concept.sporely_taxon_id if concept is not None else None


# The taxonomy-v3 regression table (docs/plans/active/2026-09-27-taxonomy-v3.md,
# "Regression species"). 53482 and 52369/58722 are the original reviewed
# bridges; 58766, 56210 and 56449 resolve through approved Stage 2
# supersessions of their NorTaxa concepts onto the COL survivor.
@pytest.mark.parametrize("nortaxa_id, sporely_id", [
    ("53482", 7821), ("52369", 83668), ("58722", 83668),
    ("58766", 617026), ("56210", 168873), ("56449", 11307),
])
def test_nortaxa_bridges(release_db, nortaxa_id, sporely_id):
    assert _provider(release_db, "nortaxa", nortaxa_id) == sporely_id


def test_craterellus_tubaeformis_nortaxa_id_stays_unresolved(release_db):
    """NorTaxa 56227 is ``single_shared_synonym_low_overlap``, which the owner
    did not approve. Only the automatic legacy integer row carries it."""
    assert _provider(release_db, "nortaxa", "56227") is None
    notes = sqlite3.connect(release_db).execute(
        "SELECT taxon_id, note FROM taxon_external_id_min "
        "WHERE source_system = 'artsdatabanken' AND external_id = 56227").fetchall()
    assert notes == [(620306, "cross_source_automatic_exact")]


@pytest.mark.parametrize("dyntaxa_taxon, sporely_id", [
    ("3423", 83668),    # reciprocal_accepted_synonymy, approved
    ("3957", 7821),     # shared_synonymy ordinary, approved
    ("236654", None),   # 617026's pair is one-directional: individual review, not approved
])
def test_dyntaxa_bridges(release_db, dyntaxa_taxon, sporely_id):
    assert _provider(release_db, "dyntaxa", f"urn:lsid:dyntaxa.se:Taxon:{dyntaxa_taxon}") == sporely_id


@pytest.mark.parametrize("retired", [624680, 627000, 626243, 626327])
def test_superseded_concepts_are_not_emitted(release_db, retired):
    assert sqlite3.connect(release_db).execute(
        "SELECT COUNT(*) FROM taxon_min WHERE taxon_id = ?", (retired,)).fetchone() == (0,)


@pytest.mark.parametrize("sporely_id, canonical", [
    (7821, "Entoloma conferendum"), (83668, "Conocybe rugosa"), (617026, "Conocybe vexans"),
    (620306, "Craterellus tubaeformis"), (168873, "Cantharellus cibarius"),
])
def test_canonical_names_are_unchanged(release_db, sporely_id, canonical):
    assert sqlite3.connect(release_db).execute(
        "SELECT canonical_scientific_name FROM taxon_min WHERE taxon_id = ?",
        (sporely_id,)).fetchone() == (canonical,)


@pytest.mark.parametrize("sporely_id, expected", [
    (7821, {("nb", "stjernesporet rødspore"), ("sv", "Stjärnrödhätting"), ("en", "star pinkgill")}),
    (620306, {("nb", "traktkantarell"), ("nn", "trektkantarell"), ("sv", "Trattkantarell"),
              ("en", "Funnel Chanterelle")}),
    (617026, {("nb", "vrang ringerlehatt")}),
    (83668, {("nb", "slank ringkjeglesopp")}),
])
def test_regression_vernaculars(release_db, sporely_id, expected):
    names = set(sqlite3.connect(release_db).execute(
        "SELECT language_code, vernacular_name FROM vernacular_min WHERE taxon_id = ?", (sporely_id,)))
    assert expected <= names


def test_kantarell_reaches_168873_only_from_the_superseded_nortaxa_concept(release_db):
    rows = sqlite3.connect(release_db).execute(
        "SELECT language_code, source FROM vernacular_min "
        "WHERE taxon_id = 168873 AND vernacular_name = 'kantarell'").fetchall()
    assert sorted(rows) == [("nb", "nortaxa"), ("nn", "nortaxa")]
    assert _provider(release_db, "nortaxa", "56210") == 168873


# --------------------------------------------------------------- national display

def _lookup(release_db, ui_language: str, vernacular_language: str):
    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB

    return TaxonLookupService(
        vernacular_db=VernacularDB(release_db, language_code=vernacular_language,
                                   display_language_code=ui_language),
        language_code=vernacular_language, include_reference_data=False,
    )


def _list_label(release_db, sporely_id: int, ui_language: str, vernacular_language: str) -> str:
    """The observation table's name cell for an observation bound to the concept."""
    from database.vernacular_db import VernacularDB
    from ui.observations_tab import _format_observation_display_label, _national_display_scientific_name

    db = VernacularDB(release_db, language_code=vernacular_language, display_language_code=ui_language)
    names = db.display_scientific_names([sporely_id])
    genus, species = names[sporely_id][0].split(" ", 1)
    observation = {"genus": genus, "species": species, "sporely_taxon_id": sporely_id,
                   "scientific_name_snapshot": names[sporely_id][0],
                   "taxon_identity_state": "sporely_v2", "taxon_identity_proof": "taxonomy_v2_artifact"}
    scientific = _national_display_scientific_name(observation, genus, species, names) \
        or observation["scientific_name_snapshot"]
    return _format_observation_display_label(db.vernacular_from_taxon(genus, species), genus, species,
                                             scientific_name_snapshot=scientific)


@pytest.mark.parametrize("sporely_id, ui_language, vernacular_language, label", [
    (83668, "nb_NO", "nb", "Slank ringkjeglesopp\nPholiotina rugosa"),
    # Stage 4P's approved Dyntaxa bridge (Taxon:3423) gives 83668 a Swedish name.
    (83668, "sv_SE", "sv", "Pholiotina rugosa"),
    (617026, "nb_NO", "nb", "Vrang ringerlehatt\nPholiotina vexans"),
    # No approved Dyntaxa identity: the canonical COL name.
    (617026, "sv_SE", "sv", "Kraghätting\nConocybe vexans"),
    # No reviewed national bridge in either language.
    (620306, "nb_NO", "nb", "Traktkantarell\nCraterellus tubaeformis"),
    (620306, "sv_SE", "sv", "Trattkantarell\nCraterellus tubaeformis"),
])
def test_observation_list_label(release_db, sporely_id, ui_language, vernacular_language, label):
    assert _list_label(release_db, sporely_id, ui_language, vernacular_language) == label


@pytest.mark.parametrize("ui_language", ["nb_NO", "sv_SE"])
@pytest.mark.parametrize("prefix", ["Pholiotina rug", "Conocybe rug"])
def test_scientific_name_search_reaches_83668_by_either_name(release_db, ui_language, prefix):
    suggestions = _lookup(release_db, ui_language, "nb").suggest_scientific_names(prefix)
    assert 83668 in {s["sporely_taxon_id"] for s in suggestions}
