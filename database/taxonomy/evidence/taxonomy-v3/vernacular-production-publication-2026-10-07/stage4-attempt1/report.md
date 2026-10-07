# Stage 4 production publication — STOP_PRODUCTION_MISMATCH

Production project: zkpjklzfwzefhjluvhfw. Attempt: 2026-10-07 14:08:28–14:11:11 Europe/Oslo (12:08:28–12:11:11 UTC).

Freeze: a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e. verify_frozen passed every frozen file, evidence member and decompressed SQLite fingerprint. R2-bound evidence archive hash/size matches committed metadata; no evidence or import artifacts regenerated.

Exact Stage 3 SQL: ~/sporely-scratch/vernacular-2026-10-07-f1/run1/import.sql, 76,193,662 bytes, SHA-256 cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e. run2 has identical SQL. All preserved Stage 3 import/export hashes pass. run1 analysis frozen.sqlite3 was previously removed; run2 retains the matching file. Original frozen gzip remains verified.

Read-only preflight passed: expected/observed active tax-2026.09.30-01, exactly one active release, 52,917 stable concepts, 13,760 active vernaculars; target release/run absent. Schema objects, constraints and one-active index present. Active release validator and national-name validator pass. Remote migration versions match local except the documented deferred 20260914090000 snapshot-v2 migration. Full row comparison to frozen cloud candidate gives zero differences for 52,917 taxon rows, 60,697 scientific names, 56,959 external IDs and 2,600 red-list rows. Registry and taxonomy-v3 mapping/identity/audit baseline recorded. Expected candidate vernacular count: 59,884.

Executed approved containerized psql command against independently verified production env target; exact command/start/end are in import-execution.json. Frozen SQL unchanged before/after. The artifact loads, validates and activates in a single transaction. Its count checks passed and it reached status ready inside the uncommitted transaction.

Divergence: Phase C, /payload/import.sql:233209, DO $validate$, taxonomy_v2_validate_release(text) line 68. Expected successful parent-reference validation. Observed ERROR: canceling statement due to statement timeout; production timeout 2min. The failing SQL counts dangling parents with NOT EXISTS over taxonomy_v2_taxa. psql exit 3. Activation and COMMIT were not reached.

Writes occurred only inside the failed transaction. Automatic rollback verified independently: no target release/import-run/taxon/vernacular rows remain; exactly one active release tax-2026.09.30-01; concepts 52,917 before/after; vernaculars 13,760 before/after. All baseline taxon/scientific-name/external-ID/red-list and all seven taxonomy-v3 table fingerprints unchanged. Import connection ended. No manual rollback or production repair is required. The import-run identity sequence may have consumed a value despite transactional rollback; no audit row persists.

Phase D activation and Phase E new-release search probes were not run; Phase F performed as rollback invariant verification. No retries or further production writes. No merge, cleanup, commit or push. Canonical active plan updated uncommitted with stop handoff.

Safest next action: retain the current active release; review a separate timeout/execution-plan mitigation for this existing validator, then explicitly authorize a new bounded publication attempt with the same immutable SQL digest. Do not rebuild/rewrite the artifact, repair identities or alter migration history.

Evidence: preflight.json, import-execution.json, import.stdout.log, import.stderr.log, rollback-state.json, rollback-verification.json in this directory. Scripts produce only Stage 4 operational checks/logs, not taxonomy/evidence/import artifacts.
