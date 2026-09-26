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
    assert notes == [f"inaturalist_refresh:{_refresh_document()['acquired_on']}"]
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

@pytest.mark.parametrize("nortaxa_id, sporely_id", [("53482", 7821), ("52369", 83668)])
def test_nortaxa_bridges(release_db, nortaxa_id, sporely_id):
    concept = resolve_installed_external_identity(release_db, source_system="nortaxa",
                                                  namespace="nortaxa_taxon_id", external_id=nortaxa_id)
    assert concept is not None and concept.sporely_taxon_id == sporely_id
