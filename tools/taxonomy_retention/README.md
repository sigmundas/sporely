# Retired taxonomy payload cleanup — stage 1 candidate

Status: **isolated cleanup/recovery rehearsal passed; production maintenance capacity gate remains open**.

## Scope and dependency findings

Remove payloads for `tax-2026.08.01-01`, `tax-2026.09.26-02`, and
`tax-2026.09.30-01`. Keep `tax-2026.10.07-01` active and intact. The six
payload tables are taxa, scientific names, vernacular names, external IDs,
legacy external IDs, and red-list assessments under `public.taxonomy_v2_*`.

Keep all release headers, hashes, manifests, original publication counts,
timestamps, import audits, and `taxonomy_v2_concepts` unchanged. Original
manifest counts describe the published dataset, not its current residency.
Keep taxonomy-v3 registry/mappings/snapshots/resolution history unchanged.
Historical identifiers must never be reassigned or recycled.

Production FK inspection on 2026-10-11 found only the five payload child
tables referencing release-specific taxa. Observations reference stable
concepts. Concepts' first-seen FK and import runs reference release headers.
No exception requiring an old full payload has been identified.

Client paths inspected:

- web/Android `src/taxonomy-v2.js`: active search and external-resolution RPCs
  take no release parameter; missing-v2 fallback uses legacy taxonomy, which
  this cleanup leaves intact.
- web/Android `src/sync-queue.js::_persistQueuedTaxonomyIdentity`: resolves
  namespaced evidence through the current resolver, preserves unresolved
  evidence, and uses atomic identification writes.
- Android Java/Kotlin source search found no pinned taxonomy-release consumer.
- landing `src/lib/publicApi.ts` and public-reference snapshots use concept
  identity and frozen observation/reference evidence, not retired v2 payloads.
- Python `utils/vernacular_utils.py::resolve_vernacular_db_path` uses local
  artifacts; cloud selection references stable identities.
- web `supabase/taxonomy-v2-client-cutover-runbook.md` states old clients stay
  readable/writable; the production import runbook states older clients
  tolerate trailing national-name response columns.

This is current-source evidence, not proof of every supported Android binary.
Before approval, identify the oldest supported Android build and verify its
sync/search contract against a cleaned isolated backend. Play approval alone
is not the criterion. Historical one-off publication probes that compare old
payloads will no longer be runnable without restoring those payloads; they
are not normal client dependencies.

## Candidate cleanup

`cleanup.sql` uses an exact release allowlist, checks that all targets are
retired and that the expected active release is sole active, serializes with
the activation advisory lock, locks the payload tables against concurrent
writes, deletes children in bulk before parents, verifies absence and active
integrity, and commits atomically. Repeating after success deletes zero rows.
It deliberately does not perform physical maintenance.

**Not yet tested:** runtime, idempotence, fingerprints, client/RPC results,
rollback, publication after cleanup, and physical reclamation. Do not infer
success from SQL inspection.

## Rehearsal and recovery requirements

1. Obtain a consistent private production snapshot with PostgreSQL 17 tools.
   Restore it into a new disposable database, never reset an existing local
   database. Keep the snapshot outside Git with mode 0600 and a SHA-256.
   Snapshot output location for this attempt:
   `/Users/sigmundas/sporely-scratch/taxonomy-retention-2026-10-11/`.
2. Capture exact payload counts, active payload fingerprints, permanent-table
   fingerprints, RPC definitions, FK/trigger inventory, table/index sizes,
   search/resolution probe results, and observation identity integrity.
3. Create a target-payload-only recovery dump. Restore it after rehearsal and
   prove exact original row fingerprints/counts and unchanged metadata.
   Recovery restores original rows under original IDs; it never activates or
   renumbers a release. Keep this short-lived safety backup through execution
   verification; indefinite taxonomy archives are not required.
4. Run `psql -X -v ON_ERROR_STOP=1 -f cleanup.sql` twice on the isolated copy.
   Compare the captured fingerprints/probes and check all FK relationships.
5. Measure database/table/index sizes immediately after deletion, after plain
   vacuum, and after separately tested physical rebuilding. Record each
   table's duration, lock behavior and maximum additional allocation.
6. Rehearse importing a genuine new release using the existing importer;
   never reuse a historical release ID to simulate publication. Retired
   headers must continue preventing ID/hash reinterpretation.

Ordinary DELETE/vacuum is not a physical-shrink guarantee. Sequential
`VACUUM FULL` is a possible **local experiment only**, not a production
recommendation yet: it needs exclusive locks, replacement heap/index space,
and WAL headroom. Index rebuilding has similar temporary-allocation needs.
Production disk/WAL free space and provider enforcement must be established
before any maintenance proposal. Prefer a temporary upgrade if a verified
safe peak cannot fit; do not gamble on the Free quota during rebuilding.

## Evidence and stage handoff

- Production size read-only measurement: 450,137,235 bytes (~429.3 MiB).
- Existing local PostgreSQL 17.6 database: ~20 MiB, no taxonomy releases;
  left untouched. Local Docker filesystem had ~37 GiB available.
- Direct host `pg_dump` and containerized `pg_dump` attempts stalled before
  producing a usable archive. Containerized `psql` through the existing
  private connection file successfully returned the database size.
  Stalled export processes were stopped. Zero-byte dump files are **not
  backups** and no accepted/frozen artifact exists.
- No production writes or DDL; no local database reset or restoration;
  no production approval requested because evidence is incomplete.
- No commit: verification has not passed.
- Next action: resolve the read-only snapshot path, then complete the
  production-sized rehearsal and recovery verification above.

Future separate stages: enforce active-only retention with an explicit
supported-client overlap exception and publication capacity gate in the
existing importer; independently correct `admin_database_health()` in web
and its UI. Keep legacy-table retirement and general index changes excluded.

Expected ~214 MiB payload savings and ~215 MiB final production size remain
unverified proportional estimates. No measured reclaimed-storage result or
Free-tier execution recommendation is available yet.


## Completed rehearsal (2026-10-11)

A PostgreSQL 17.6 `pg_dump -Fc --verbose --lock-wait-timeout=10s
--no-owner --no-acl` through the existing session pooler on port 5432 exited
0. Transaction pooling was not used. `pg_restore -f /dev/null` read the entire
archive successfully. Backup SHA-256:
`d04127187af967a664735676a09ce20237d53f3f6f7d0f46e90c5aa10eb47c8b`.
Private archive: `/Users/sigmundas/sporely-scratch/taxonomy-retention-2026-10-11/verified.dump`.
Verbose export log and full numerical evidence are alongside it.

The initial host invocation mistakenly put a URI in PGDATABASE and attempted
an unavailable local socket. Later attempts were stopped prematurely without
verbose logs. The session pooler reports backend application names as
Supavisor, so filtering pg_stat_activity for pg_dump falsely suggested no
export was running. The verbose retry showed catalog discovery and COPY
progress and completed. No specific network fault or startup-option fault
was demonstrated; those earlier diagnoses were conjecture.

The complete archive was restored with PostgreSQL 17 into the separate
`taxonomy_retention_final` database in the existing local Supabase container,
using local supabase_admin. Restore exited 0 with --exit-on-error. Only cron
extension/schema objects were excluded by TOC because pg_cron permits
installation only in the database named postgres. All application, taxonomy,
observation, auth, storage and v3 objects in the archive were restored. This
is an application-complete rehearsal, not a cluster disaster-recovery test;
--no-acl/--no-owner means production ownership/grants were not reproduced.
Existing local postgres databases were not reset.

Measured database bytes:

| Phase | Bytes | MiB |
| --- | ---: | ---: |
| Compact full restore | 337456275 | 321.82 |
| After DELETE | 337472659 | 321.84 |
| After ordinary VACUUM/ANALYZE | 336669843 | 321.07 |
| After six sequential VACUUM FULL/ANALYZE | 167341203 | 159.59 |

Cleanup took 2.26 seconds locally; idempotent retry 0.32 seconds. Individual
rebuilds took 0.06–0.32 seconds locally; these are not production timing
promises. Local available filesystem capacity was ~32 GiB, vastly greater
than required. Exact peak temporary allocation was not sampled; do not claim
it was measured. WAL settings are not actual available production disk.

Preserved fingerprints: all six active payloads; release records, concepts,
import runs, observations (744), identification history (1271), and v3
registry/mappings/snapshots/resolution links. RPC definition fingerprints,
scientific/vernacular search and external resolution probes were unchanged.
Active validation passed; orphaned observation concept links were zero.
Historical release headers and their original hashes/counts remained intact.

Recovery: exporting retired-only CSV rows from the untouched restored copy
and loading taxa before children in one transaction restored every original
full payload count and fingerprint. Replaying full-table data against existing
active rows failed on duplicate keys and rolled back; do not use that method.
Selective recovery counts: taxa 158751, scientific names 176236, vernacular
names 28328, external IDs 162724, legacy external IDs 0, redlist 7124.
Cleanup passed again after recovery. Local reproduction scripts are
`rehearse_local.py` and `recover_local.py`; they hard-code isolated local
container/database targets and must not be pointed at production.

Using production's six current relation sizes (309.35 MiB) and measured
compacted active relation sizes (~70.04 MiB), estimated final production size
is ~183.7 MiB from ~429.3 MiB, saving ~245.6 MiB. This estimate includes
packing improvements within those six tables, but no legacy-table or general
index optimization. The restored database's lower baseline must not be
mistaken for cleanup savings. DELETE alone does not achieve this reduction.

## Proposed production procedure — approval required

1. Independently confirm project, sole active release and exact retired
   targets. Take a fresh session-pooler PostgreSQL 17 archive using the tested
   dump command; capture exit status, log, SHA-256 and archive readability.
   Restore it locally. Export the exact retired rows for selective recovery,
   retaining column order and schema fingerprints. Keep files private.
2. Capture pre-change permanent/active fingerprints, counts and RPC probes.
   Verify supported-client compatibility and schedule a maintenance window.
3. Verify actual production disk allocation, WAL usage and available disk in
   Supabase's compute/disk dashboard. Do not execute rewrites if unknown.
   Reserve space for replacement heap, all new indexes, sort spill and WAL;
   local surviving largest relation is ~20 MiB, but that is not a peak bound.
   A conservative 150 MiB maintenance reserve must be checked against actual
   free disk, with additional WAL/replication margin. If not demonstrably
   sufficient, temporarily upgrade first. Provider quota byte interpretation
   remains unresolved; use conservative 500,000,000 bytes for planning.
4. Run cleanup.sql exactly, with stop-on-error. Recheck fingerprints and
   active validator. No metadata/status edits or ID reassignment.
5. In the approved outage, run VACUUM (FULL, ANALYZE) one table at a time:
   taxonomy_v2_external_ids, taxonomy_v2_scientific_names, taxonomy_v2_taxa,
   taxonomy_v2_vernacular_names, taxonomy_v2_redlist, taxonomy_v2_legacy_external_ids.
   Use a short lock timeout; abort on contention, not wait indefinitely.
   Check database and disk/WAL capacity after every table before continuing.
   Rebuilds include indexes; no separate general REINDEX is proposed.
6. Verify all invariants/probes again and measure final actual bytes. Resume
   service only after checks pass. Keep recovery files through verification,
   then remove according to normal private-backup retention.

Rollback before cleanup COMMIT is transaction rollback. After COMMIT,
selectively COPY retired taxa then child rows in a transaction using the
fresh tested backup; no active rows or permanent records need replacing.
After maintenance, restored old payloads require space again: capacity must
be reserved or upgrade before recovery. VACUUM FULL is individually atomic;
interruption leaves earlier completed table rebuilds in place.

Free is viable at the estimated ~184 MiB final size, subject to the physical
maintenance capacity gate. No production approval requested until actual
available disk/WAL and supported Android checks are recorded. No actual
Android-device/cross-client sync or new-release publication test was performed
in this SQL-only rehearsal. Those limitations remain explicit.


Rebuild order correction: use Red List first, taxa second, then external IDs,
scientific names, vernacular names and legacy external IDs. This supersedes
the earlier order above. Exact sequential peaks and unchanged execution gates
are recorded in READINESS.md under “Approved candidate order revision”.
