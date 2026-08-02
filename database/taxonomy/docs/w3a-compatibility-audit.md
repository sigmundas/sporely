# W3-A client compatibility audit

Enumerates every taxonomy read/write path in **sporely-web** and
**sporely-py** and classifies it against the W3-A rollout: how it
behaves during and after production install, and what (if anything)
it requires.

All findings assume the additive rollout described in
[`w3a-operator-runbook.md`](w3a-operator-runbook.md): a new nullable
`public.observations.resolved_sporely_taxon_id`; the existing
taxonomy/name columns retained as authoritative historical snapshots;
`taxonomy_v3` schema is service-role-write, public-read.

Classifications used:

* **already compatible** — the path only reads legacy columns or writes
  only to legacy columns; no code change required at cutover time.
* **requires dual-read** — the path benefits from also consulting the
  canonical link once available; can ship AFTER install without
  breaking anything if omitted.
* **requires dual-write** — the path writes taxonomy data (identification
  flow); must be updated to also populate `resolution_link` at write
  time in a later stage; NOT part of W3-A.
* **requires cutover** — the path must switch from legacy to canonical
  in a coordinated release; also NOT part of W3-A.
* **legacy-only and removable later** — the path is a compatibility
  shim that can be removed after cutover.

## sporely-web

| path | file(s) | classification | notes |
|---|---|---|---|
| Observation list — species/genus display | `src/screens/finds.js`, `src/screens/home.js` | already compatible | reads `observation.genus`, `observation.species`, `observation.common_name` directly. Continues to work with the historical snapshot columns unchanged. |
| Observation detail — species header | `src/screens/find_detail.js` | requires dual-read (deferred) | during the first production phase, keep reading legacy columns. Later: prefer `observations.resolved_sporely_taxon_id`-joined canonical name when non-null; fall back to legacy. |
| Map markers — species labels | `src/screens/map.js` | already compatible | same legacy-column read as list screens. |
| AI identification apply — writes to observation | `src/ai-identification.js` | requires dual-write (deferred) | at cutover it must also write the picked provider ID into `taxonomy_v3.resolution_link` via a service-role RPC or edge function. Not W3-A. |
| Artsorakel/Artsdatabanken provider client | `src/artsorakel.js`, `src/artsorakel.test.js` | already compatible | fetches NBIC/nortaxa taxon IDs; no schema dependence. |
| Taxonomy-v2 sparse-registry experiment | `scripts/taxonomy-v2/run-sparse-registry-experiment.mjs`, `sparse-registry-prototype.{sql,test.mjs}` | already compatible | disposable, never applied to production. |
| W2D disposable simulation | `scripts/taxonomy-v2/w2d-migration-simulation.test.mjs`, `experiments/w2d-migration-simulation.sql` | already compatible | disposable schema `w2d_migration_simulation`, isolated. |
| W3-A rehearsal (this stage) | `scripts/taxonomy-v3/*`, `supabase/drafts/*.sql` | already compatible | draft-only, never touched by `supabase db reset` or `supabase migration up`. |
| Legacy `public.taxa` / `public.search_taxa` reads | (unchanged) | legacy-only and removable later | left in place during dual-read phase; retire after cutover. |

## sporely-py

| path | file(s) | classification | notes |
|---|---|---|---|
| Local observations SQLite schema | `database/schema.py`, `database/migrate.py`, `database/rebuild_taxonomy_db.py` | already compatible | desktop-local storage; adding taxonomy-v3 later happens through a separate sync step; W3-A does not activate anything on desktop. |
| Legacy backfill from artsdata/artportalen | `database/migrate_observations_sporely_id.py` | already compatible | reads legacy IDs, writes desktop-local `sporely_taxon_id`. Independent of the cloud registry. |
| Taxonomy build pipeline (COL XR + NorTaxa) | `database/taxonomy/reconciliation/**`, `database/taxonomy/scripts/**`, `database/import_taxa_to_supabase.py`, `database/vernacular_db.py`, `database/build_unified_multilang_taxonomy_db.py`, `database/taxon_lookup.py`, `database/models.py`, `database/update_inat_common_names.py` | already compatible | never runs against production Supabase; produces release artefacts + the reconciliation manifest that the W3-A installer consumes. |
| Historical reconciliation engine (this branch series) | `database/taxonomy/reconciliation/**`, `database/taxonomy/scripts/allocate_*.py` | already compatible | offline pipeline. Its output is the W3-A input, not the reverse. |
| iNat vernaculars refresh | `database/update_inat_common_names.py` | already compatible | reads iNat CSV, writes local SQLite; no cloud dependency. |
| Cloud taxonomy export | `database/taxonomy/cloud_export.py` | already compatible | generates the release directory the W3-A installer reads; no runtime dependency on the cloud schema. |
| Desktop taxonomy activation | (not present) | (not applicable) | W3-A does not activate anything on desktop. |

## Summary

* No sporely-web or sporely-py path breaks under W3-A. All writes to
  `public.observations.resolved_sporely_taxon_id` come from
  `taxonomy_v3.link_observations_to_resolution()` (service-role only)
  — clients cannot self-assign the link.
* The rollout is strictly additive during the first production phase:
  legacy taxonomy/name columns remain the authoritative reads for every
  client screen; the canonical link is populated but not consulted.
* Dual-read and dual-write changes are deferred to later stages. The
  compatibility of every listed path was verified by static inspection;
  no dead-column or ambiguous-source reference was found.

Cutover-time work (NOT in W3-A):

1. Add a `taxonomy_v3.resolve(observation_id)` view or edge function
   that returns `{canonical_name, rank, sporely_taxon_id, cache_state}`
   with legacy-fallback semantics.
2. Migrate every observation-detail read path (`find_detail.js` and
   any downstream) to consult the canonical link and prefer it when
   non-null.
3. Add a service-role RPC for AI-identification writes to keep
   `resolution_link` current.
4. Only after those three land, plan any legacy-column deprecation.
