# Local cloud import rehearsal — tax-2026.10.31-01 (2026-10-31)

Preparation only: `release_preparation_approved` decision bound to SQLite
`ec19acac…cfe` (`<releases>/tax-2026.10.31-01/release-prep/`), export
`--local-rehearsal` (`export_use: local_rehearsal`, `importable: false`).
The production preparation path refuses this export ("release_approval is not
importable"). Disposable local containers only
(`public.ecr.aws/supabase/postgres:17.6.1.084`); the payload carries the
production system-identifier and marker-nonce guards. No production access.

Pipeline: `export_cloud_taxonomy.py --local-rehearsal` → `macrofungi_scope.py`
(53,166 taxa; included and ancestor sets identical to `-11`) →
sporely-web `prepare-production-release-import.mjs --local-rehearsal` →
schema `prod-schema-2026-10-25.sql` + migration `20261031120000`.

- **Rehearsal 1** — unmodified `local-rehearsal.sh`, baseline `tax-2026.10.07-01`
  (the `-11` rehearsal's inputs): 13/13 PASS (guard, races A/B, import,
  search, COL resolution, stale-id refusal, replay, rollback).
  `rehearsal-1-summary.txt`.
- **Rehearsal 2** — production-equivalent baseline `tax-2026.10.23-11` (replayed
  from the `-11` rehearsal and dumped): import refused without the migration;
  after it, imported, validates, no national-name errors; every `-11` concept
  and synthetic observation selection kept; 300/300 sampled projected
  NorTaxa/Dyntaxa ids resolve to their primary; Norwegian/Swedish display and
  vernacular search; Red List 2,984 → 3,601 rows (614 concepts gain, none
  lose); unchanged web/desktop RPC write paths accepted; desktop-only member
  refused as before; rollback to `-11`. `rehearsal-2-summary.txt`.

Found and fixed by the rehearsal: `macrofungi_scope.py` accepted only
`authoritative_bridge` national-name provenance and kept national names on
classification ancestors (`26c1c5b`).
