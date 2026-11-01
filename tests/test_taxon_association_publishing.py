"""Publishing ids on a release with name associations: an association may
supply a candidate, but never displaces the id the release already gave a
name, and never one a destination knows under another name."""
from __future__ import annotations

import sqlite3

from tests.test_taxon_name_associations import build

def test_publishing_ids_copied_across_a_differently_named_association_are_not_used(tmp_path, monkeypatch):
    """iNaturalist: an id the overlay copied from a member under another name
    is not sent for the primary's name; a same-name copy is. Artportalen: the
    name another concept lists only through an association answers last."""
    from database import models
    from utils import vernacular_utils

    def prepare(conn):
        conn.execute("UPDATE taxon_min SET inaturalist_taxon_id = 9021 WHERE taxon_id = 21")
        conn.execute("UPDATE taxon_min SET inaturalist_taxon_id = 9020 WHERE taxon_id = 20")
        # Artportalen: Calocybe carnea's own id, and another concept that
        # lists the name only because an association put it there.
        conn.execute("INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, "
                     "is_preferred_name, source, note) VALUES (12, 'sci', 'Calocybe carnea', 0, 'nortaxa', "
                     "'name_association:unique_synonym_bridge;association=x')")
        conn.executemany(
            "INSERT INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, is_preferred, "
            "external_name, note) VALUES (?, 'artportalen', ?, ?, 0, ?, NULL)",
            [(11, 7001, "accepted", "Calocybe carnea"), (12, 5, "accepted", "Flammulina fennae")])

    release = build(tmp_path / "tax-2026.10.31-01.sqlite3", prepare=prepare)
    with sqlite3.connect(release) as conn:
        assert conn.execute("SELECT inaturalist_taxon_id FROM taxon_min WHERE taxon_id = 11").fetchone() == (9021,)
    monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda *_a, **_k: release)

    assert models._resolve_external_taxon_id("Calocybe", "carnea", "inaturalist") is None
    assert models._resolve_external_taxon_id("Rugosomyces", "carneus", "inaturalist") == 9021
    assert models._resolve_external_taxon_id("Russula", "aeruginea", "inaturalist") == 9020
    assert models._resolve_external_taxon_id("Calocybe", "carnea", "artportalen") == 7001


def test_ids_reached_only_through_an_association_need_the_destination_name(tmp_path, monkeypatch):
    """Artportalen and Artsobservasjoner: an association-projected id answers
    only under the destination's own name for it."""
    from database import models
    from utils import vernacular_utils

    def prepare(conn):
        conn.execute(
            "INSERT INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, is_preferred, "
            "external_name, note) VALUES (11, 'artportalen', 7001, 'accepted', 0, 'Calocybe carnea', NULL)")

    release = build(tmp_path / "tax-2026.10.31-01.sqlite3", prepare=prepare)
    monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda *_a, **_k: release)

    # Rugosomyces carneus is listed on Calocybe carnea only by the association.
    assert models._resolve_external_taxon_id("Rugosomyces", "carneus", "artportalen") is None
    assert models._resolve_external_taxon_id("Calocybe", "carnea", "artportalen") == 7001
    # NorTaxa's 501 is its id for Rugosomyces carneus, not for Calocybe carnea.
    assert models._lookup_adb_taxon_id_for_concept(11, "Calocybe", "carnea") == ("unresolved", None)
    assert models._resolve_adb_taxon_id("Calocybe", "carnea") is None
    # Same name: the NorTaxa id is the one for this name.
    assert models._lookup_adb_taxon_id_for_concept(10, "Russula", "aeruginea") == ("found", 500)
    assert models._resolve_adb_taxon_id("Russula", "aeruginea") == 500
