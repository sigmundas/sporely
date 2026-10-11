# Production readiness — 2026-10-11

**NO-GO for production execution until actual disk/WAL capacity is visible.**
No production changes made. No Android device/emulator attached (`adb devices`).
No secret API key available for the documented customer metrics endpoint.
Native browser dashboard accessibility exposed only the sidebar; disk metrics
could not be read. Do not mistake unavailable metrics for zero usage.

Read-only production facts: database 450170003 bytes (429.32 MiB); zero
replication slots; pg_stat_wal reports 1192894526 cumulative bytes since
2026-06-17. This is generated WAL, NOT current WAL disk usage. Current free
disk, WAL-directory bytes, archive backlog and allocated disk were not measured.
No server filesystem functions/OS commands were used through SQL.

## Sequential space estimate

Measured local PostgreSQL 17 WAL INSERT LSN deltas and compact replacement
relation sizes on the restored, cleaned database (including indexes):

| Table | Replacement MiB | WAL generated MiB |
| --- | ---: | ---: |
| external_ids | 19.45 | 18.42 |
| scientific_names | 16.84 | 15.78 |
| taxa | 15.91 | 15.28 |
| vernacular_names | 16.70 | 15.62 |
| redlist | 1.12 | 1.09 |
| legacy_external_ids | 0.02 | 0.03 |

Total rewrite WAL: 66.21 MiB. LSN differences include any concurrent local
cluster activity, and are not guaranteed production bounds. Sort spill and
peak live files were not sampled. Each replacement coexists with its old
relation until the rewrite commits; old allocations are already in baseline.
The largest replacement is ~19.5 MiB. The first operation's replacement +
measured WAL is ~37.9 MiB before sort/segment/background margins. Later
operations release old relation space; required new disk is not the sum of
all replacement relations. WAL can accumulate if archiving/checkpointing lags.
DELETE also produces WAL, which was not measured independently.

Use a conservative **150 MiB additional free-disk reserve**, plus normal
background/archival margin, as a planning gate, not a proven hard peak bound.
Verify disk/WAL before DELETE and before each rewrite. Allow 16 MiB WAL
segment rounding and potentially different production checkpoint/compression
behavior. If free space cannot be demonstrated, do not execute on Free.

The earlier 183.7 MiB final estimate was an arithmetic error. Correct formula:
429.32 - 309.35 + 70.04 = **190.00 MiB**, estimated savings **239.31 MiB**.
Production final size is not guaranteed. Database quota is distinct from disk
capacity; conservatively plan against 500000000 bytes until quota units are
confirmed. Largest replacement alone keeps the estimated database allocation
below that conservative quota, but this does not prove physical disk safety.

## Earlier execution sequence — order superseded below

1. Verify project ref, active `tax-2026.10.07-01`, three retired targets and
   current schema/FK/trigger fingerprints. Confirm actual allocated/free disk,
   current WAL bytes and archive health through Supabase metrics/dashboard.
2. PostgreSQL 17 session-pooler port 5432: fresh `pg_dump --verbose
   --lock-wait-timeout=10s -Fc --no-owner --no-acl`, exit 0, SHA-256,
   complete `pg_restore -f /dev/null`, isolated restore and selective retired
   row export. Existing rehearsed archive SHA and path are in README.md.
   Record explicit column lists for recovery COPY; do not replay active rows.
3. Capture permanent/active counts and fingerprints, RPC definitions and
   representative resolver/search outputs. Pause application writes and
   taxonomy publication during the approved window. Wait out active users'
   long transactions; do not terminate them automatically.
4. Execute cleanup.sql exactly: activation advisory lock, 5-second lock
   timeout, 120-second statement timeout, six tables locked against writes;
   reads remain possible. Exact retired allowlist only; child-first deletion;
   active validation before COMMIT. Abort on any error or changed active state.
5. In separate autocommit operations run:
   `SET lock_timeout='5s'; SET statement_timeout='120s';`
   then `VACUUM (FULL, ANALYZE) public.<table>;` one at a time in the order
   shown above. Each takes ACCESS EXCLUSIVE, blocking reads and writes of that
   table. Do not wrap all VACUUM operations in one transaction. Recheck disk,
   WAL/archive health and database size after each; stop before the next if
   capacity or checks fail. No separate REINDEX or unrelated optimization.
6. Verify no retired payload rows, unchanged release headers/checksums and
   concepts, unchanged observation/v3 identities, zero orphan links, active
   validator pass, unchanged RPC definitions/results and expected counts.
   Measure actual final size. Resume writes after checks pass.

Before cleanup commit: transaction rollback. After commit: transactionally
COPY retired taxa then children from verified selective backup. Stable IDs and
headers stay unchanged. Ensure recovery capacity; old data needs space again.
A failed/interrupted individual VACUUM FULL is atomic; completed earlier
rewrites remain valid. Keep the backup through post-execution verification.

## Recommendation and outage

Free is likely sufficient after cleanup (~190 MiB). Temporary Pro is **not
proven necessary**, but it is the safer execution route if actual capacity
cannot be observed or is below the reserve. Do not upgrade automatically.
A measured capacity pass can turn this into GO; unknown capacity stays NO-GO.

Local cleanup ~2.3 seconds and rewrites <0.5 seconds each are not production
performance evidence. Reserve a **10–15 minute maintenance window** for
backup-independent execution/checks, with expected SQL work of seconds to a
few minutes if uncontended. Lock acquisition fails after 5 seconds and each
statement is capped at 120 seconds. This bounds individual attempts, not the
whole outage. Backups/restores can run before the window.

Android smoke test not performed: no attached device, and the isolated SQL
database is not an app-configured Auth/PostgREST backend. Current source and
RPC rehearsal support active-only compatibility; they do not constitute an
Android synchronization pass. No new backend was installed or existing local
API configuration redirected.

## Sequential quota calculation — awaiting dashboard measurements

The decision is whether the complete operation fits Free during execution.
No upgrade is recommended merely because the current database is large.

Using the recorded 450170003-byte baseline and measured replacement sizes,
and conservatively assuming DELETE returns no relation space:

| Rebuild order | Database peak MiB (old + replacement coexist) | After commit MiB |
| --- | ---: | ---: |
| external_ids | 448.77 | 342.99 |
| scientific_names | 359.82 | 276.15 |
| taxa | 292.07 | 215.52 |
| vernacular_names | 232.22 | 193.53 |
| redlist | 194.65 | 190.00 |
| legacy_external_ids | 190.02 | 190.00 |

Peak estimated database allocation is 470568083 bytes, below conservative
500000000-byte quota by 29431917 bytes (~28.07 MiB). This is not a guarantee:
fresh baseline, other databases if counted by the provider, replacement-size
variance, and concurrent growth must be included. WAL is physical disk usage,
not an addition to pg_database_size; temporary sort files need separate disk
accounting. No transaction may span the six VACUUM FULL operations.

Physical disk calculation must start with dashboard allocated and used disk,
which already include existing database/WAL/system files. Add deletion WAL,
current operation's replacement/sort spill, WAL retained from completed
operations, segment rounding and operating margin; subtract old relation
files only after each successful rewrite. Do not double-count baseline WAL.
If WAL is not recycled, conservatively retain the entire measured 66.21 MiB
rewrite WAL budget through the sequence. Deletion WAL remains unmeasured.

The previously cited 1 GB is a generic Free disk allowance; 8 GB is a generic
paid-plan initial disk size. Neither establishes this project's current
allocation or enforced quota. An actual 8 GB dashboard allocation would not,
by itself, change Free's database-size quota. Resolve using the exact labels,
actual project plan, allocated disk and used/WAL measurements, not generic
plan figures. The user's newly available dashboard numbers were not included
in the message or found in these files; they must be supplied before replacing
this section with measured physical peaks or changing the readiness verdict.

Backup gate remains mandatory: fresh compatible session-pooler pg_dump,
exit 0, checksum, full archive readability, isolated restore and selective
retired-only recovery verification. Execute cleanup once, then rebuild one
table per autocommit operation with checks between operations.

## Final decision with dashboard capacity — GO, approval still required

User-reported dashboard: provisioned Disk **8 GB**, ~0.73 GB used; database
443.7 MB, WAL 128 MB, system 171.5 MB. WAL measurement timestamp:
2026-10-11 14:33:57.707753 UTC. The 1 GB gp3 text describes the plan inclusion,
not this project's displayed provisioned capacity. Treat 8 GB as provisioned
capacity, with approximately 7.27 GB free in displayed units. The database
quota remains independently 500 MB. No temporary Pro upgrade is necessary
on this evidence. Refresh these metrics immediately before execution.

Fresh read-only SQL measured application database 450243731 bytes and all
cluster databases combined 465359025 bytes; zero replication slots. The
cluster total explains the dashboard's larger 443.7 MB database figure.
For maximum caution, apply the 500000000-byte quota to the **cluster sum**,
not just the application database, until enforcement scope is confirmed.

Earlier external_ids-first peak (superseded by the safer order below):
- Application database: 470641811 bytes (~448.84 MiB).
- Conservative cluster total: 485757105 bytes (~463.25 MiB), leaving
  14242895 bytes (~13.58 MiB) below 500000000.
- With a 25% replacement-size allowance (25497600 bytes rather than
  20398080), cluster peak is 490856625 bytes, leaving 9143375 bytes.

This tight but adequate database margin is the limiting factor, not disk.
Each successful rebuild then removes its much larger old allocation, so the
first rebuild is the critical quota step. Expected final application database
~190.07 MiB. Existing disk/WAL is already included in 0.73 GB used; do not
add it again. Even retaining all measured rewrite WAL (66.21 MiB), a 150 MiB
additional maintenance reserve is tiny versus displayed free disk. Exact
DELETE WAL/sort peak is not guaranteed, so keep the live capacity checks.

### Mandatory execution gates and abort thresholds (updated safer order)

1. Production changes still require explicit user approval. Pause normal
   application writes and publication; the pause must be real, not assumed.
2. Verify current backup readability and SHA-256, then take a fresh backup
   and verify it through the already tested archive/isolated selective-recovery
   procedure. Current rehearsed SHA remains
   d04127187af967a664735676a09ce20237d53f3f6f7d0f46e90c5aa10eb47c8b.
   cleanup.sql SHA is
   3dd22fbe5467931cba9e077ca3094a8701a702e6b0ed3dc4cc578e983744393f.
3. Before DELETE, verify active/retired statuses, schema/FKs/triggers, backup
   and permanent/active baselines. Abort if targets/statuses differ, archive
   verification fails, or active/permanent evidence differs unexpectedly.
4. Refresh cluster database sum and disk/WAL immediately before and after
   DELETE. Do not start the first rebuild unless
   `cluster_bytes + 1464320 < 495000000` for Red List first.
   This reserves 5 MB below conservative quota after a 25% replacement margin.
   At the recorded baseline this first gate passes by ~28.18 MB; unexplained growth or inability
   to observe cluster bytes means STOP. Disk free must remain >=150 MiB,
   archiving healthy, and no unexplained WAL backlog. If checks fail, retain
   logically cleaned data and defer physical maintenance; do not auto-upgrade.
5. Run redlist first, taxa second, then external_ids, scientific_names,
   vernacular_names, legacy_external_ids. One VACUUM FULL per autocommit operation;
   no transaction spanning rebuilds. lock_timeout=5s, statement_timeout=120s.
   Abort the current statement on contention/timeout/error; do not auto-retry.
6. Before each subsequent operation, recompute
   `cluster_bytes + ceil(1.25 * measured_replacement_bytes) < 495000000`.
   Recheck >=150 MiB free disk, current WAL/archive health, no unexpected
   writers, active identity integrity and last operation's actual size.
   Unexpected size growth above the 25% replacement allowance, failed integrity,
   or unavailable metrics means STOP before the next operation. A second
   read-only monitoring connection should watch quota during rewrites; cancel
   the statement if cluster size reaches 495000000 bytes, free disk falls
   below 150 MiB, or archival health deteriorates. Cancellation may require
   authorized operator intervention and does not guarantee instant rollback.
7. Post-cleanup/rebuild checks and selective recovery are as specified above.
   Preserve release headers/concepts, check zero retired payloads, active
   validator and RPC probes, observation/v3 fingerprints and measured final
   size. Do not resume normal writes until checks pass.

GO for the exact bounded procedure on Free, contingent on these refreshed
prechecks and explicit approval. No production modification has occurred.
Reserve 10–15 minutes; actual production locks/I/O remain unmeasured. Device
smoke test remains unavailable (no attached Android device), but no retired
payload dependency was identified in supported source/RPC behavior.


## Approved candidate order revision — no production execution

The user correctly identified a safer order. There is no technical dependency
requiring external IDs first. Child-first DELETE order is required by the
cleanup design; VACUUM FULL order is independent. Rewriting taxa preserves
its table identity, constraints and referenced keys, so retained child rows
remain valid. Each rewrite includes its own indexes. No standalone REINDEX,
second copy of all six tables, or transaction spanning rebuilds is required.

**Use Red List, taxa, external IDs, scientific names, vernacular names,
legacy external IDs.** This supersedes every earlier external-IDs-first order
in this report and README.md.

Exact calculations using the recorded conservative cluster baseline
465359025 bytes; no space credit for DELETE or ordinary VACUUM:

| Operation | Cluster before (bytes) | Replacement allowance (125%, bytes) | Estimated peak (bytes) | After successful commit (bytes) |
| --- | ---: | ---: | ---: | ---: |
| redlist | 465359025 | 1464320 | 466823345 | 461656241 |
| taxa | 461656241 | 20858880 | **482515121** | 398078129 |
| external_ids | 398078129 | 25497600 | 423575729 | 307556529 |
| scientific_names | 307556529 | 22067200 | 329623729 | 237473969 |
| vernacular_names | 237473969 | 21893120 | 259367089 | 214421681 |
| legacy_external_ids | 214421681 | 20480 | 214442161 | 214421681 |

Maximum estimated quota peak **482.515 MB (460.162 MiB)** occurs during taxa,
leaving **17.485 MB** below 500000000 bytes and **12.485 MB** below the
495000000-byte abort ceiling. Compared with external IDs first, this lowers
the allowance-adjusted peak by **8.342 MB**. Red List releases 3702784 bytes;
taxa then releases 63578112 bytes (~60.63 MiB).

The intermediate assumptions are explicit: all six DELETEs commit first;
old relation files remain allocated; the current replacement heap/TOAST and
indexes coexist with that table's old files; only after successful commit is
the old total subtracted. Allowance is 25% on the measured replacement total.
Temp cleanup_targets is tiny and ON COMMIT DROP, but cleanup/catalog/ANALYZE
allocation changes are not mathematically zero. Therefore the table is a
planning estimate, not an upper bound: remeasure the actual cluster baseline
AFTER cleanup and BEFORE every rewrite. Carry no assumed release credit if
the observed size does not fall as expected.

WAL/sort files affect physical disk, not the ordinary database-size calculation.
Measured rewrite WAL in this order is 1.09, 15.28, 18.42, 15.78, 15.62 and
0.03 MiB respectively (~66.21 MiB total). Retain the whole WAL allowance for
physical planning if not recycled. DELETE WAL, sort spill, segment rounding
and background writes remain covered by capacity monitoring and the existing
150 MiB free-disk gate; no exact physical peak bound was measured. Displayed
~7.27 GB free disk remains ample; neither old database nor current 128 MiB
WAL should be added twice to dashboard used disk.

All backup/recovery, integrity, write-pause, lock-timeout, statement-timeout,
monitoring and abort requirements remain unchanged. Each next operation must
pass actual `cluster_bytes + allowance_for_that_table < 495000000`; stop on
unexpected allocation/growth, unavailable metrics, integrity failure or timeout.
An operator must cancel the current statement if the monitored ceiling is
reached; monitoring is a safeguard, not permission to start above the gate.

**GO on Free with this safer order**, contingent on fresh backup verification,
refreshed per-operation capacity checks and explicit user approval. No
production data/configuration changes have been made.
