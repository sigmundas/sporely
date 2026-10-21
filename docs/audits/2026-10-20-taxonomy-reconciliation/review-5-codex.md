# Review of revision 5 (S1 scope) by Codex (gpt-6.1-sol, high effort), 2026-10-21

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

**Verdict: (b) S1 is safe after the corrections below.** The pinned taxonomy set is separable from the deferred partitions; the release package is not yet executable or sufficiently verified.

Paths: `audit/` = `docs/audits/2026-10-20-taxonomy-reconciliation/`; `build/` = `database/taxonomy/scripts/`; `Web/` = the supplied sporely-web repository.

1. **S1-BLOCKER — the cloud inventory SQL cannot execute against the checked-in schema.**
   `audit/scripts/cloud_reference_check.sql:6452` uses `original_namespace`; `Web/supabase/migrations/20260802120000_add_taxonomy_v3_core.sql:78` defines **`original_source_namespace`**. No rename was found. This invalidates the entire 41-check UNION and aborts its transaction. Correct the generator (`references.py:248`) and generated SQL, then run them against the verified production schema.

2. **S1-BLOCKER — P10 withholds bridges but does not preserve the promised evidence in the released artifact.**
   `build/build_sqlite_candidate.py:621–636` stores only `alias_reason` as `note`; it discards the proposed inheritance, nomenclatural status and evidence-class fields. Immutable inspection reproduced **20→4 bridges and 20 names** in the sample, but unchanged-builder synonym notes remain merely `synonym_of_accepted`. Only the separately patched builder preserves status/inheritance.
   Therefore `policy-amendment-proposal.md:138–147` cannot call provenance optional while promising its preservation. Make artifact/export preservation mandatory and extend H16 through SQLite and cloud export, including scientific-name deduplication. No schema change is necessarily required.

3. **S1-BLOCKER — baseline and post-release cloud expectations are conflated.**
   The independently recomputed cloud scope is **52,917 concepts: 0 S1 retirees, 499 survivors**. Scoped export filters external identifiers (`database/taxonomy/macrofungi_scope.py:565–569`), so the cloud gets **499 S1 anchor bridges**, while desktop gets 988. Runbook `staged-release-plan.md:37` does not distinguish them; SQL `cloud_reference_check.sql:6505–6506` still expects zero S1 NorTaxa mappings.
   Define separate baseline/candidate/post-activation gates. Replace “unverified” expectations on live-dependency checks with explicit acceptance criteria; `staged-release-plan.md:22` currently cannot determine whether those checks pass.

4. **S1-CORRECTION — S1 changes external-id resolution, despite changing no stored retiree selection.**
   NorTaxa **55146**, currently preserved unresolved, belongs to **625825→163349**. After activation its reviewed bridge resolves to 163349. The resolver reads active release identifiers (`Web/.../20260724130000_add_taxonomy_v2_schema_and_search.sql:623–636`); NBIC selections use that namespace bridge (`Web/src/taxonomy-v2.js:158–174`).
   Activation itself does **not** rewrite stored `resolved_sporely_taxon_id` (`Web/.../202608021210_integrate_taxonomy_v3_observations.sql:46`). Later selection/retry can resolve differently. Gate the before/after resolution of affected accepted/provider tuples, including ambiguity and synonym non-resolution. The inspected queue retains earlier write targets (`Web/src/sync-queue.js:1468–1476`); I found no necessary general remapping dependency.

5. **S1-CORRECTION — CG5b protects selections, but not the complete remote identity.**
   Both dirty candidates are **624659 / NorTaxa 52330**, outside S1. If fallback executes, `identity_push.py:137–148` either skips an identical external claim or atomically sets selection NULL and overwrites the source tuple, leaving names untouched.
   A mocked call with NULL selection and another external tuple overwrote that tuple with 52330. Thus “selection IS NULL” alone is insufficient. Verify the current remote claim is identical or explicitly empty, and check resolved identity/baseline; do not assume both candidates necessarily issue a fallback write.

6. **S1-CORRECTION — rollback and backup instructions need concrete executable steps.**
   `staged-release-plan.md:25` applies SQLite backup/integrity procedures to **`install_receipt.json`**, which requires a byte-for-byte file backup instead.
   “Reactivate the previous release” (`:45`) fails directly: activation requires `ready` and retires the previous release (`Web/.../20260724130000_add_taxonomy_v2_schema_and_search.sql:420–430`). Specify and authorize the guarded rollback transaction, validation and restored receipt/artifact pair. Explicitly distinguish restoring pre-window state from retaining valid post-activation selections; restoring desktop backups alone cannot undo cloud writes after sync resumes.

7. **S1-CORRECTION — two claimed baseline facts need narrower wording.**
   G6 counts reference tombstones only (`s1_desktop_gates.py:244–245`): today **13 image tombstones remain pending**, although G6 reports zero. Classify these separately or the runbook’s blanket tombstone stop condition is false.
   Re-derivation found **0 shipped-parent child links and 4 corrected-parent links**, all internal—not four under both maps (`report.md:31`). Neither issue establishes a deferred-taxonomy dependency.

**Approval decisions affected:** S1-1’s pinned 988-pair scope is supported. Amend S1-2/S1-3 for mandatory P10 preservation and complete candidate/export checks. Correct S1-4’s SQL. Withhold S1-5 until corrected implementation, cloud gates and rollback verification pass. S1-6 may preserve unresolved rows, provided it acknowledges changed resolver answers and does not authorize later repairs.

**Ran:** project Python `-B`; immutable SQLite queries; in-memory `s1_derive.main` with output captured; both S1 validator acceptance checks, missing-member rejection and 66 comparator cases. Reproduced **943+45**, all stated hashes, no ledger chains/duplicates/deferred survivor dependencies, 1,175 synonyms/457 retirees, 1,016 blocked/38 qualified/7 doubtful, desktop **0/44**, scope **0/499**, no merged accepted/publishing-id multiplicity and no red-list collisions. Existing dry-run copies matched **27/27 table hashes**, zero migration-log rows. Sampled **46 archive pairs**, including Hygrocybe conica **624178→16436**: no eligibility discrepancies under the stated incertae-sedis treatment.

**Inferred/unverified:** post-implementation artifact/export behavior, live cloud contents, CG5b and behavioral rollback. The 13 S1 assessments using synonym name ids retain assessment snapshots but still inherit identity through `compile_release.py:1733–1749`; define that permitted exception explicitly. Historical 7-backup/16-profile zeros were not independently rerun. No network, credentials, writes or commits; Git status remained `?? docs/audits/`.