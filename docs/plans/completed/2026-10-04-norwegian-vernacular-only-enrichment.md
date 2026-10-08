# Norwegian vernacular-only enrichment

Status: **COMPLETED 2026-10-07.** `tax-2026.10.07-01` is the sole active production taxonomy release (previous `tax-2026.09.30-01`, now retired); verdict PRODUCTION_RELEASE_VERIFIED_WITH_NONBLOCKING_FINDINGS. See "Stage 4C" below.

User architecture: keep identity reconciliation strict; exact name/rank may associate vernacular metadata only. Full canonical-release uniqueness, current status, fungal compatibility, no qualifiers/nomenclatural warnings/reviewed contradictions/explicit splits; species classification differences below kingdom are provenance diagnostics. Existing preferred names survive.

## Current bounded pass — normalization and revised simulation

Completed 2026-10-04. Raw `nomenclaturalStatus` is preserved in national-source normalized records. Revised audit restores 1,619 unique Norwegian name rows on 756 concepts (633 species). Exceptions: 62 source usages (16 species, 46 genera). Full-release uniqueness and Gerronema/grønnkremle/lys høstmorkel/rognerust/gråfiolett køllesopp regressions pass.

Evidence: `database/taxonomy/evidence/taxonomy-v3/norwegian-vernacular-2026-10-04-policy-v2/report.md`, `summary.json`, `automatic-associations.json`, `exception-source-usages.json`, `policy-change.json`.

Validation: 46 national-source tests + 21 NorTaxa/compiler tests pass; syntax and diff checks pass. Commit status: normalization/tests and audit evidence are currently uncommitted. No release or production work performed.

## Compiler vernacular projection — implemented and verified

Implement metadata-only associations in `compile_release.py` after existing linkage, preserving original source record and detailed association provenance. Consume owner-reviewed vernacular selections separately from the unchanged automatic rule, validating pinned target reconstruction. Preserve the Entoloma collective-source constraint; never flatten it to the strict species. Do not mutate source_usages, aliases, registry allocations, supersessions, bridge eligibility or national scientific-name authority. Full-release uniqueness before filtering scope/classification; preserve preferred names. Add focused behavior/invariant fixtures and report exact results against this audit.

Completed stage 1 in the latest implementation pass. `vernacular_projection.py` is called only from compiler vernacular projection; normalized national/COL records retain concept and nomenclatural annotations. `policies/vernacular_associations.json` contains ten fingerprinted concept approvals, revalidated against current sources and exact stable/canonical target usage. Qualified source-native metadata remains valid at its own concept. Historical bundle names do not supply future rows. Compiler emits hashed evidence, review validation and change-report artifacts. Recipe supplies approved ledger and existing macrofungi scope, with full-release uniqueness first.

Verified two compiler sets and two SQLite candidates byte-identical: 1,619 automatic additions + 17 reviewed = 1,636 new rows on 766 concepts. Full automatic set matches audit. Identity/source-usage/registry/external-ID artifacts unchanged; mapping records differ only by release stamp. Five open species withheld; Entoloma strict/collective separation passes. 169 focused tests, syntax/diff checks pass. Candidate outputs `/tmp/sporely-vernacular-stage1/frozenA` and `frozenA.sqlite3`; report `database/taxonomy/evidence/taxonomy-v3/vernacular-compiler-stage1-2026-10-04/report.md` with fingerprinted verification. Changes remain uncommitted under the earlier explicit instruction. No promotion, bundled release replacement or production change.

Material publication delta: 19,505 non-Norwegian historical names removed by current-source derivation, zero Norwegian removals; 19 Swedish preferred-name flags now follow current source metadata. Retaining historical names requires pinned current source evidence or concept decisions. Review this delta before stage 3; exact rows recorded in stage verification. Stage 1 stops at compiler/publication subsystem boundary under AGENTS.md.

## Following bounded pass — release evidence retention

Before `promote_desktop_bundle.py:promote` / `build_release.py --promote`, produce and validate the complete frozen compiler/SQLite/evidence artifact set and deterministic archive. Promotion must publish exactly that set, generating or mutating no evidence. Bind archive hash in publication manifest and verify every output fingerprint/compiler digest. Ensure publication retains archive and previous evidence lineage. Stage 3 builds/validates cloud export and search, asserts all pinned counts and zero identity changes before publication, and reviews the historical-name removal delta. No production operation has occurred.

The pre-implementation report checkpoint is complete. Separate compiler and publication-lifecycle stages; preserve unrelated workspace work.

## Owner species-review update — 2026-10-04

Owner selected lys høstmorkel → 14541 / 3KJX6 (Helvella crispa (Scop.) Fr.) and dunpipe → 14834 / 3KV7Z. Recorded as vernacular-only evidence in `norwegian-vernacular-2026-10-04-policy-v2/owner-reviewed-vernacular-associations.json`; readable queue `problem-species.md`. Full-release automatic uniqueness stays unchanged. Original simulation has 16 species exceptions; 2 owner-reviewed, 14 species remain open (plus 46 genera). Decisions are not applied to compiler, identities or production. Future compiler stage must consume reviewed vernacular selection explicitly without editing identity manual mappings or supersessions.

## Latest bounded pass — eight owner approvals and collective distinction

Completed 2026-10-04. Extended the existing owner-reviewed evidence ledger with the eight requested approvals. The simulation applies those only after calculating the unchanged automatic result: 1,619 unique automatic rows on 756 concepts; 13 latest reviewed rows on 8 concepts; 17 total reviewed rows on 10 concepts; 1,636 combined rows on 766 concepts. All reviewed targets reconstruct uniquely. Remaining exceptions: 104 source name rows on 52 usages, including 6 species and 46 genera. Of the six species, five need further owner review; Entoloma is a deliberate collective-concept hold.

Strict Entoloma sericellum / 96276 retains silkerødspore; collective NorTaxa record 53666 / existing source-bound 625268 retains silkerødspore-gruppen and all other metadata from that aggregate usage. The simulation records a prohibition on projecting this collective usage to the strict target. No collective canonical identity is created.

Evidence: `database/taxonomy/evidence/taxonomy-v3/norwegian-vernacular-2026-10-04-owner-reviewed/report.md`, `reviewed-associations.json`, `collective-concept-preservation.json`, `exception-source-usages.json`, `verification.json`. The existing `policy-v2/problem-species.md` and machine species queue now show all ten reviews and six withheld species.

Validation: 6 focused offline review safety tests pass; full simulation/regression assertions pass; automatic manifest is byte-identical to policy v2; repeat-run generated evidence is byte-identical; syntax/diff checks and identity/bridge fingerprint comparison pass. Changes are uncommitted as explicitly requested. No compiler implementation, identity mutation, release build or production operation. Next stage remains compiler projection, followed separately by publication evidence retention.

Repeated owner instructions verified idempotently, including the latest simulation rerun: each of the eight approvals exists exactly once. Fresh simulation runs match one another and all 14 saved generated evidence files byte-for-byte; the original automatic manifest and all 13 protected identity/bridge file hashes are unchanged. Six safety tests and read-only strict/collective Entoloma assertions pass again. Counts and decisions remain unchanged; no duplicate reviews or release/production operations were introduced.

## Removal evidence audit — 2026-10-04

Completed the read-only census of all 19,505 stage-1 exact-string removals. The earlier characterization as unsupported cleanup is superseded: 10,331 have current pinned COL usage-linked vernacular evidence omitted by normalization/projection; 721 Swedish names are already recovered under current case/spacing spelling; 8,453 have no established current pinned association to their old target. Of those 8,453, 5,616 have current matching names on scientifically matching but unassociated usages, 1,116 only on other scientific concepts, and 1,721 have no matching name in the three pinned archives. All have local unpinned snapshot candidates, which were not accepted as current evidence. Every row carries separate current-name and current-target-association indicators and detailed provenance.

Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-removal-audit-2026-10-04/report.md`, complete compressed row manifest, source/language/reason summary, candidate queues and fingerprinted verification. Audit script: `database/taxonomy/evidence/taxonomy-v3/audit_removed_vernacular_evidence.py`. Exhaustive/disjoint classification and byte-identical repeat run verified; compiler/policy/identity/bridge/bundle/candidate fingerprints unchanged. No restoration, compiler run, release or production action. Changes remain uncommitted.

Next bounded implementation should address current COL source-native vernacular normalization/projection, retaining current provider provenance and preferred policy, before freezing stage 2. Separately acquire/pin/revalidate provider evidence for the remaining unresolved associations. Do not use historic presence or scientific-name equality to create identity bindings. Norwegian automatic/reviewed counts and five unresolved species/Entoloma separation remain unchanged. Stop at this audit boundary.

## Stage 1B — current COL source-native vernacular recovery

Implementation in progress in this bounded pass: additive COL `VernacularName.tsv` normalization with usage ID, ISO language code, unmodified source name, preferred/raw fields, source release/member/row provenance and explicit dangling-usage rejection artifact. Compiler accepts only the same existing canonical COL usage binding. Existing national-source association/preferred evidence wins over duplicate COL copies; SQLite exact-key deduplication remains the projection boundary. No historical-input restoration or scientific-name association. Compiler/candidate repeat builds, removal audit, Norwegian regressions and preferred delta are being verified before stopping. No promotion or production action; changes remain uncommitted.

Stage 1B completed and verified: 10,331 audited recoveries / 3,579 concepts; 89,926 current COL source-native rows emitted; candidate 96,144 unique rows. Versus old bundle, 19,418 exact rows retained and 11,722 exact removals remain (3,269 current spelling replacements + 8,453 unsupported associations); zero compiler recovery gaps and zero Stage 1 row losses. Existing Stage 1 preferred flags unchanged; old-bundle preferred delta separately records 6,878 recovered names with no current COL preferred flag plus the prior 19 Swedish changes. Norwegian 1,619 automatic + 17 reviewed association evidence unchanged; five open species and strict/collective distinction unchanged.

187 focused tests, syntax/diff checks, exhaustive current canonical COL projection, SQLite integrity/FK and identity tables pass. Independent COL normalized artifacts, compiler sets and SQLite candidates are byte-identical; compiler identity/bridge outputs and registry byte-identical to Stage 1. Evidence/report/verification and recovery manifest: `database/taxonomy/evidence/taxonomy-v3/vernacular-compiler-stage1b-2026-10-04/`; scratch candidates `/tmp/sporely-vernacular-stage1b/frozenA` and `frozenB`. No commit/promotion/release replacement/production action. Stop at Stage 1B; next remains frozen evidence retention/promotion design.

## Stage 2 — freeze and archive

Implementation in progress after owner acceptance of Stage 1B. Freeze validates the complete compiler inventory, accepted receipt, SQLite/registry/red-list provenance and pinned enrichment counts before preparing deterministic gzip, full compiler/evidence archive, publication manifest, compatibility bytes and fingerprinted descriptor. Promotion requires the expected descriptor hash, verifies full archive/file/SQLite fingerprints and baseline/registry, then copies only frozen bytes. Previous evidence archives and per-release freeze descriptors remain immutable. Build-release integration freezes before optional promotion; no evidence generation in promotion. Real accepted candidate independent freezes and scratch publication verification are pending completion. No repository bundle replacement, production change or commit.

Stage 2 completed and verified. Two independent frozen publication sets are byte-identical, with full compiler/evidence inventory, accepted receipt and previous provenance. Real archive/member/SQLite fingerprints pass. Scratch-only promotion copies frozen bytes, retains prior evidence and repeats idempotently; frozen inputs and protected repository bundle/compatibility/identity/bridge/SQLite files unchanged. 81 focused tests, syntax and diff checks pass. Freeze descriptor hash: `d0e5de92cd2e4e2222f77818e5b0425360d37596df2cc9b3d3fbd9567d6870ad`. Prepared local set `/tmp/sporely-vernacular-stage2/finalA`; report/fingerprints `database/taxonomy/evidence/taxonomy-v3/vernacular-freeze-stage2-2026-10-04/`. No commit, repository bundle replacement or production publication. Stop at Stage 2. Next Stage 3 must validate cloud export/search and publish exactly the reviewed frozen set when authorized; no new evidence during promotion.

## Stage 3 — frozen cloud validation and publication checks

Completed bounded export/import/search validation of accepted finalA, without recompiling or changing builder/client logic. W1: 633,541 taxa / 96,144 names; scoped cloud: 52,917 taxa, 60,697 scientific names, 59,892 vernaculars, 56,959 authoritative IDs, zero legacy IDs, 2,600 red-list rows. All 1,619 automatic + 17 reviewed additions / 766 concepts exported. Existing scoped Stage 1 names (6,948) and old Norwegian rows (4,616) retained. Actual local approved import validation and ten authenticated RPC search probes pass; transaction rolled back and local state unchanged. Seven owner-requested queries return exact canonical targets; grønn navlesopp and strict silkerødspore pass; scientific Russula aeruginea returns grønnkremle. Web client contract 29 tests pass.

Publication caveat: existing macrofungi scope only loads canonical COL concepts, excluding NorTaxa-native collective Entoloma 625268. It remains intact in frozen SQLite, never projected onto strict 96276, but silkerødspore-gruppen is unavailable in cloud. No scope change or identity collapse. Resolve separately before publication if group availability is required. Accepted finalA/freeze hash unchanged; all frozen file/member/SQLite fingerprints and protected repository/identity/bridge bytes verified. Report `database/taxonomy/evidence/taxonomy-v3/vernacular-cloud-stage3-2026-10-04/report.md`. Changes uncommitted; no production connection/write, migration, bundle replacement or publication. Stop at this validation boundary.

### Stage 3 rerun (2026-10-06)

Owner-authorized hash-gated re-materialization after `/tmp` loss. `build_release.py` with current code is deterministic but diverges at compiler `vernacular.jsonl` (accepted Stage 1B used Dyntaxa normalized by the committed HEAD `national_source.py`); reproducing that exactly plus the reconstructed Stage 2 `freeze-final.py` matched every accepted fingerprint (freeze `d0e5de92…`, 42 archive members, SQLite `69a34da8…`). Frozen set at `~/sporely-scratch/vernacular-2026-10-06/stage2/finalA` (read-only). Cloud export/import (local container, ROLLBACK) and all 12 `search_taxa_v2` probes pass, deterministic across two runs and identical to 2026-10-04; zero identity/external-ID changes; scratch promotion exact and guarded. Verdict NOT_READY pending owner decision: four withheld qualified names (taigarødtuppsopp, knolltrevlesopp nb, sørlig/sørleg rødtuppsopp) reach COL targets via Stage 1B COL-native rows. Publish only via `promote_desktop_bundle.py --expect-freeze-sha256`, never `build_release.py --promote`. Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-cloud-stage3-2026-10-06/`. No commit, no production action.

Handoff (2026-10-06): the D2 divergence is evidence-only. 1,341 sv Dyntaxa rows differ only in `association_evidence_sha256`; without that field the outputs hash to the same value. D1 rows are absent from bundled `2026.09.30-01`, so this is new exposure. Next decisions for the owner: (1) D1 — accept COL-native rows on 58814/58815/19080 as-is, or withhold them (that changes Stage 1B policy and requires a new freeze and review). (2) D2 — before committing, either keep the working-tree `national_source.py` change and re-freeze with the current code (new freeze, same name content), or defer that change so the committed code reproduces `d0e5de92…`. Commit plan deferred until the verdict is READY*. `database/AGENTS.md` (new, untracked, not created by this stage) still needs owner attribution.

### Stage 3B refreeze (2026-10-06)

- **Owner decisions implemented.**
  - D1: added an automatic COL echo guard in `vernacular_projection.py`. A `col_xr` row is withheld from a target where a NorTaxa association is withheld as `qualified_or_aggregate_source`, and the withholding is recorded as evidence. An owner review lifts the guard for the languages it lists.
  - Fixed the qualifier regex so author initials such as "S. L." and "P.P." no longer read as qualifiers. This affects NorTaxa 71888 and 227151 only.
  - Added owner reviews: taigarødtuppsopp → 58814 and rosenekornnøtt → 60590, nb only.
  - D2: `build_release --promote` now requires `--expect-freeze-sha256`. Recipe pins are now 1,619 automatic + 18 reviewed on 767 concepts.
- **New candidate**: `tax-2026.10.06-01`, freeze `fda89f3744975dfb296336b73256c33df1b278efe092eca336557d067d07148e`.
  - Produced only by `build_release.py`. Two independent builds are byte-identical.
  - Durable read-only copy at `~/sporely-scratch/vernacular-2026-10-06-r2/final`.
- **Content delta against d0e5de92**: exactly the 3 withheld COL rows (58815 nb/nn, 19080 nb), plus taigarødtuppsopp on 58814 becoming the NorTaxa preferred name. The rest is evidence-hash churn.
- **Stage 3 rerun passes twice and deterministically**: local ROLLBACK, all probes, invariants and promotion guards. The furustokklav probe fails only because the lichen is out of cloud scope.
- Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-refreeze-stage3-2026-10-06/`. Verdict READY_WITH_RECOMMENDATIONS.
- Nothing committed or published. Publish only with `promote_desktop_bundle.py --frozen-dir <final> --expect-freeze-sha256 fda89f37…`.

Handoff (2026-10-06, after Stage 3B): independently re-verified the frozen hashes (freeze `fda89f37…`, decompressed SQLite `3f9037c0…`), the D1 split rows, the regex fix, and that lichen 103256 was already out of cloud scope. Verdict READY_WITH_RECOMMENDATIONS. The reviewed set is `~/sporely-scratch/vernacular-2026-10-06-r2/final` (durable, read-only). The d0e5de92 set and the two earlier Stage 3 folders are superseded historical records. Next: commit in the planned groups (run the focused suite at each code commit), then a separate owner-authorized promotion of the reviewed set with `promote_desktop_bundle.py --expect-freeze-sha256 fda89f3744975dfb296336b73256c33df1b278efe092eca336557d067d07148e`, then the cloud import. No commit, promotion or production action has occurred.

## Stage 3C validation (2026-10-07)

- Validated frozen set `~/sporely-scratch/vernacular-2026-10-06-r2/final`, freeze `fda89f37…7148e`. The owner prompt's `/tmp/…/finalA` (d0e5de92) was wiped and is superseded by D1/D2.
- Ran the production export path and the local Supabase import (ROLLBACK proven) twice from scratch. Results were byte-identical apart from the `desktop.json` `build_seconds` timing, and equal to the 2026-10-06 hashes (`import.sql` `4465a8ba…`).
- All 1,637 enrichments survive. All 7,176 selectable Stage 1B COL recoveries survive, and the 3,155 others are explained by scope rules. The 8,453 unsupported historical rows are absent.
- 36,252 vernacular rows are dropped by scoping, 0 unexplained. All 22 positive and 7 negative `search_taxa_v2` probes pass. There are no qualified/collective leaks, and all identity invariants show 0 diffs.
- Verdict READY_WITH_NONBLOCKING_FINDINGS. MEDIUM, owner decision: six nb COL names carry `[GAMMELT]`/`[UTGÅTT]` markers, five of them in the cloud export, one of them on *Ramaria aurea* 58651.
- Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-cloud-stage3c-2026-10-07/` (the report text was delivered in the agent reply; report.md was not written). Nothing committed, promoted or published.

## Stage 3D F1 refreeze (2026-10-07)

- **Owner decision F1 implemented**: `normalize_col_xr.py` rejects a COL vernacular whose name contains `[GAMMELT]` or `[UTGÅTT]` (case-insensitive, whitespace-tolerant, bracketed token only). Rejection reason `source_marked_obsolete_vernacular`, with the raw row. The row is not stripped and kept. Test: `test_col_vernacular_source_marked_obsolete_is_rejected`. A README paragraph lists the COL rejection reasons.
- **Full scan of pinned COL VernacularName**: exactly 5 hits, all `nob`/NO (1041, 58651, 58722, 60670, 65750). There are none in other languages or outside the projection.
- **New candidate**: `tax-2026.10.07-01`, freeze `a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e`, SQLite `e349e8b1…`. Three independent builds are byte-identical. Durable read-only copy at `~/sporely-scratch/vernacular-2026-10-07-f1/final`.
- **Content delta against fda89f37**: exactly −5 `vernacular_min` rows, with no preferred-flag change. The rest is evidence/compiler-manifest hash churn. The recipe is unchanged, because it pins only the projection (1,619/18/1,637/767), which still holds.
- **Stage 3C checks A–G rerun twice, deterministically**: all 29 probes plus 4 F1 report probes pass. There are no marked names in SQLite, W1, the scoped export or the imported DB. Promotion guards hold. Taxonomy tests: 1076 passed, 1 skipped.
- Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-f1-refreeze-2026-10-07/`. Nothing committed, promoted or published. The fda89f37 set is superseded. Next: commit, then owner-authorized `promote_desktop_bundle.py --frozen-dir <final> --expect-freeze-sha256 a86e3585…`, then the cloud import.


## Stage 4 production import stopped (2026-10-07)

> The Stage 4, timeout-investigation and Stage 4B handoffs below are kept as
> written at the time. Their "uncommitted", "not deployed", "no merge" and
> "Next boundary" statements are superseded: the validator fix was deployed
> and merged to web `main`, the publication completed in Stage 4C, and the
> evidence was committed as recorded in *Post-merge review and closeout*.

- **Verdict: STOP_PRODUCTION_MISMATCH.** Freeze `a86e3585…fd14e` and exact preserved Stage 3 SQL `cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e` verified; no artifacts regenerated.
- Read-only production preflight passed on `zkpjklzfwzefhjluvhfw`: sole active `tax-2026.09.30-01`, 52,917 concepts / 13,760 vernacular rows; target absent; schema/history compatible including the documented deferred migration. Full frozen-candidate comparison found zero taxon/scientific-name/external-ID/red-list deltas.
- Exact approved containerized psql import attempted 14:08:28–14:11:11 Europe/Oslo. Load/count checks passed inside its transaction; pre-activation `taxonomy_v2_validate_release` dangling-parent check hit production `statement_timeout=2min` (`import.sql:233209`, function line 68). psql exited 3; activation/COMMIT not reached.
- Automatic rollback independently verified: target release/run/data absent; previous sole active and counts unchanged; all checked identity/registry/mapping/scientific-name/external-ID/red-list and taxonomy-v3 audit fingerprints unchanged. No further production writes or retry. Sequence allocation may leave an ordinary import-run ID gap.
- Operational report/logs: committed in `database/taxonomy/evidence/taxonomy-v3/vernacular-production-publication-2026-10-07/stage4-attempt1/` (report, preflight, import execution, rollback state and verification, scripts). The raw per-table baseline JSON is too large for git; its digests are in the committed preflight, and the taxonomy reference baselines are archived in R2 (`stage4-attempt1/baselines.evidence-location.json`). The two observation-linked baselines (`identification_snapshot`, `resolution_link`) are withheld pending an owner decision on personal data. The attempt-1 `verify_production.py` and `investigate_readonly.py` are committed alongside. New-release search probes deferred because activation did not occur.
- Safest next action: keep previous active release; separately review validator timeout/execution-plan mitigation and authorize a new bounded attempt using the same frozen SQL. No data repair, artifact regeneration, migration-history repair or merge. Handoff remains uncommitted under the live-production verification tier; no commit/push of a failed partial stage.


### Stage 4 timeout investigation (2026-10-07)

- Verdict **VALIDATOR_FIX_REQUIRED**; no retry or production data/schema change. Exact parent-reference query identified in deployed `taxonomy_v2_validate_release`, frozen `import.sql:233204`/`:233209`.
- Active-release parent check measured 351 ms (generic warm plan 22 ms), whole validator 2.142 s. Absent-target custom plan estimates one row per input and chooses nested-loop anti join with release-only inner index scan / parent equality join filter. Frozen 52,917-row distribution would imply ~1.31 billion comparisons under that plan. Failed-session nested plan was not captured; cold target statistics + observed plans strongly support pathology rather than ordinary >2min workload.
- Read-only alternative (`OFFSET 0` within correlated NOT EXISTS) preserves missing-parent semantics, gives both-key indexed parent lookup even for absent target; active 52,917-probe execution measured 190 ms. Proposed only: requires separate reviewed validator/validation-execution fix and fresh-release cold-statistics testing, not an applied-migration edit or frozen SQL rewrite.
- Production 2min timeout is server configuration-file default; no applicable role/database/function timeout override. Read-only SET LOCAL scope proof reverts on rollback. No justified timeout-only retry value/procedure.
- Reconfirmed sole active `tax-2026.09.30-01`, concepts 52,917 / vernaculars 13,760, no target rows in any release table or import runs. Exact SQL `cc9a1ddf…` and freeze `a86e3585…` unchanged; verify_frozen passes. No regeneration required.
- Detailed read-only plans/settings/measured results: committed report `.../vernacular-production-publication-2026-10-07/stage4-attempt1/timeout-investigation-report.md`. The raw read-only queries, plans and outputs it summarizes are committed in `stage4-attempt1/timeout-investigation/`. Report-only handoff; stop before validator edits or another publication attempt.

### Stage 4B validator fix prepared (2026-10-07)

- Verdict **VALIDATOR_FIX_READY**, not deployed. Forward-only web migration `20261007123912_fix_taxonomy_v2_parent_reference_plan.sql` replaces only `taxonomy_v2_validate_release(text)`, adding `OFFSET 0` to the original correlated same-release parent lookup. All other validator code/security/ACL/OID retained. Suggested constant-release-only rewrite still generated the pathological plan; constant-release plus OFFSET also timed out in the complete fresh-release PL/pgSQL test, so original correlation is preserved.
- Independent reviewer found no defects and reran focused integration: 2 pass / 0 fail or skip, 4.535s. Cases cover valid/null/one/multiple dangling, cross-release-only parent, same ID elsewhere, empty/absent/populated/fresh release, result equality and both-key indexed plans.
- Entire exact frozen import locally with proposed function, original checks and ROLLBACK: 4.506s, unchanged 2min timeout; fixed parent query 41.346ms and full validator 230.157ms. Candidate counts match all frozen expectations; no frozen artifact regenerated or modified.
- Node taxonomy suite 52 pass / 23 optional skips / 0 fail; 5 taxonomy SQL suites pass. Security suite deferred: local PG17.6 signal-11 crash reproduces baseline denied activation call without migration; reviewer classifies it as an existing engine limitation. Catalog/ACL checks pass. No global/session production timeout changes.
- Report in web `docs/deployments/2026-10-07-taxonomy-validator-parent-reference.md`; operational evidence committed as `.../vernacular-production-publication-2026-10-07/stage4b-prepare/` (byte-identical copy of the former scratch folder). Migration/test/report remain uncommitted for stage review; no deployment, main merge or publication retry. Production remains sole active 2026.09.30-01, 52,917 concepts / 13,760 vernaculars, target release/run absent, old function unchanged. Freeze/import digests and verify_frozen pass.
- Next boundary: review/commit minimal validator migration, then separately authorize guarded deploy-tree deployment and read-only verification; publication retry remains separate.

### Stage 4B production validator deployed (2026-10-07)

- **VALIDATOR_FIX_DEPLOYED_AND_VERIFIED.** Reviewed migration/tests/report committed and pushed in web feature branch `feature/taxonomy-validator-parent-reference`: checkpoint `3f57439`; verified deployment report `d10e5c0`. No merge.
- Guarded deploy-tree check/dry-run allowed exactly `20261007123912`; production push applied only that forward validator-function migration. Post-verify confirms remote history/deferred snapshot exception correct. Temporary deploy tree removed after all verification.
- Exact reviewed function body read back; owner/ACL/OID/security/search_path preserved. Active 2026.09.30-01 validator JSON exactly matches baseline (ok=true/errors=[]). Production correlated parent lookup uses both key dimensions, no release-only join filter: 511.612ms; full validator 2375.4ms.
- All taxonomy-v2/taxonomy-v3 registry/mapping/identity/audit content fingerprints, counts, release states and indexes unchanged. Sole active 2026.09.30-01; concepts 52,917, vernaculars 13,760, target release/run absent; timeout still 2min. Frozen SQL and freeze digests unchanged; no import or activation attempted.
- Evidence committed as `.../vernacular-production-publication-2026-10-07/stage4b-deploy/` (before/after state, validator timing, production check, verdict; plus the raw deploy plan, dry-run, migration-list and parent-plan outputs); committed web report `docs/deployments/2026-10-07-taxonomy-validator-parent-reference.md`. This cross-repository active-plan note remains uncommitted alongside earlier handoffs. Next boundary is separately authorized exact frozen publication retry.

### Stage 4C production publication (2026-10-07) — COMPLETED

- **Verdict: PRODUCTION_RELEASE_VERIFIED_WITH_NONBLOCKING_FINDINGS.** Previous active release `tax-2026.09.30-01` (now retired); new sole active release `tax-2026.10.07-01`. Freeze `a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e`; exact Stage 3 import SQL `cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e`, unchanged and not regenerated. No timeout, index or mapping change.
- Read-only preflight passed: the production fingerprint equals Stage 4B post-deploy; the validator equals migration `20261007123912`; previous-release rows equal the frozen candidate; taxonomy_v3 is unchanged.
- The single-transaction import (load → validate → activate) committed 13:34:31–13:35:34 UTC.
- Post-commit verification:
  - Concepts 52,917 (unchanged); vernacular rows 13,760 → 59,884 (frozen metadata).
  - The new release equals the frozen set row for row.
  - Pre-existing rows: 14 of the 16 fingerprinted tables are unchanged outside the new release (`taxonomy_v2_taxa`, `_concepts`, `_scientific_names`, `_external_ids`, `_legacy_external_ids`, `_redlist`, `_vernacular_names`, and taxonomy_v3 `registry_concept`, `external_mapping`, `identification_snapshot`, `resolution_link`, `release_installation`, `supplement_installation`, `reconciliation_manifest_audit`). `taxonomy_v2_releases` and `taxonomy_v2_import_runs` were allowed to change and were checked only for +1 row each; their old rows were recorded but not compared.
  - So taxonomy identity (taxa, concepts, taxonomy_v3 registry, mappings, identification snapshots and resolution links), external IDs, scientific names and red list are unchanged.
  - Validator ok in 840 ms (the retained, warm rerun); parent check about 170 ms; timeout 2min. A first-run timing of 2,207 ms was reported, but that run's output was overwritten and is not retained.
- Search and data checks: no `[GAMMELT]`/`[UTGÅTT]`; 357 spelling replacements correct; the 96276/19080/58651/58832/58815/58814 isolation holds; all 33 Stage 3 search probes identical.
- Nonblocking, owner-accepted finding: the declared `dangling_parent_count` = 1, solely Fungi 152331 → parent 150361 outside the release scope, identical in all prior releases. The verification asserts exactly that row.
- Evidence: `database/taxonomy/evidence/taxonomy-v3/vernacular-production-publication-2026-10-07/` (report, preflight, import execution, production verification, scripts, logs, the first-attempt rollback and the Stage 4B deploy records). The validator fix (`3f57439`) and its deployment report (`d10e5c0`) are on web `main`.

### Post-merge review and closeout (2026-10-08)

- **Retrospective review of `c9d655f`: APPROVED_WITH_NOTES, nothing blocking.** Hashes, sizes, release ids, row counts, timestamps and the verdict agree across the evidence, the commit message and this plan. The verification scripts run read-only (`BEGIN READ ONLY … ROLLBACK`, production project ref checked first) and cannot pass on empty results. No credentials are committed.
- The committed `report.md` and `production-verification.json` are historical evidence and stay as written. Read them with these corrections:
  - `report.md` line 20 ("every pre-existing row is unchanged") holds for the 14 fully compared tables listed under Stage 4C; `taxonomy_v2_releases` and `taxonomy_v2_import_runs` were checked only for +1 row each.
  - `report.md` line 19's first-run timing of 2,207 ms has no retained record; the retained figure is the 840 ms warm rerun.
  - `writes_occurred: true` in `production-verification.json` describes the stage, which included the import; the verification script itself only reads. (`preflight.json`, written before the import, records `false`.)
- Main-branch workflows: nothing failed or outstanding. sporely-py `main` triggered no workflow (`build-release` runs on tags only). sporely-web `main` (`d10e5c0`) deployed to Cloudflare Pages successfully; the scheduled Supabase heartbeat kept succeeding.
- Retained and reverified: the frozen release `~/sporely-scratch/vernacular-2026-10-07-f1/final/` against `final.sha256` (freeze `a86e3585…`), the exact import SQL `vernacular-2026-10-07-f1/run1/import.sql` (`cc9a1ddf…`, 76,193,662 bytes), the Stage 4C working folder, and the R2 evidence archive (fresh read-only download matched `e2d1dfca…`, 53,372,637 bytes).
- The Stage 4B preparation evidence was committed as `stage4b-prepare/` before its scratch folder was removed. The other approved scratch folders (superseded frozen sets, rebuilds, Stage 3C, the earlier R2 retrieval copy, and the F1 rebuild/promotion/run2 intermediates) were deleted on 2026-10-08.
- The remaining raw Stage 4 and Stage 4B deploy files were preserved before their scratch folders were removed: timeout-investigation and deploy outputs committed as above, and nine taxonomy reference baselines archived in R2 in two content-addressed parts (`4af0ffcf…`, `3979b148…`), each verified by a fresh download.
