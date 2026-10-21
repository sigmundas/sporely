# Review of revision 4 by Codex (gpt-6.1-sol, high effort), 2026-10-21

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

**Verdict: unsound as a basis for owner decisions.** Revision 4 repairs much of revision 3, but still overstates approval scope and migration safety.

Paths: `audit/` = `docs/audits/2026-10-20-taxonomy-reconciliation/`; `Web/` = the supplied sporely-web repository.

1. **CORRECTNESS — supersession still bypasses the proposed synonym approval boundary.**
   `compile_release.py:1077–1092` promotes **every usage** allocated to a retiree to `reviewed_supersession`; it does not check that usage’s membership or source-release pin.
   Executing this block in memory for a synonym on Hygrocybe conica **624178→16436** produced `bridge_evidence_class=reviewed_supersession`; `build_sqlite_candidate.py:687–699` then admits its bridge.
   SQL against the workdb found **1,175 S1 synonym usages**, including **38 qualified citations, 7 unpublished/doubtful citations**, and **1,016 blocked nomenclatural statuses**.
   Synonym illegitimacy need not invalidate the accepted concept, but P9/H13 must explicitly define and enforce inherited-usage approval—including this re-key path. Current approval language does neither. **Blocks D1/D2/D11 implementation approval.**

2. **CORRECTNESS — the offered server remap does not cover web/Android writes or queue recovery.**
   `audit/staged-release-plan.md:108–111` offers a remap in `set_observation_selected_taxon_v2`.
   The normal atomic writer instead calls **`set_observation_identification_v2`** (`Web/src/taxonomy-v2.js:449`), whose membership check rejects retired ids (`Web/supabase/migrations/20260922140000_atomic_observation_identification.sql:76`).
   Furthermore, queued recovery compares literal identities before writing (`Web/src/sync-queue.js:1046`, `:1122`): a remotely repaired survivor differs from the queued retiree and throws a conflict before either RPC.
   Cover both RPCs and canonicalize queued selections/recovery targets; pausing the owner’s devices cannot drain every multi-user offline queue. **Blocks D5 and D8’s S4 rollout.**

3. **CORRECTNESS — maintenance-window consistency gates cannot pass literally on existing data.**
   `audit/staged-release-plan.md:94–97` requires release membership everywhere and no NULL identity push.
   Read-only local queries found **45 stored desktop identities outside cloud scope**, including **43 proven NorTaxa identities with external evidence**; S1 leaves all 43 unchanged.
   These legitimately reach the fallback at `utils/cloud_sync_impl/identity_push.py:130–144`. The cloud snapshot also contains **20 resolved ids absent from the desktop release**, within **634896–634916**.
   Define separate desktop/cloud membership checks, resolve those 20 ids, and test specifically that no fallback clears a different remote survivor. Pause→migrate→activate→install→verify→resume is otherwise sensible. **Blocks D8/D9 clearance.**

4. **RISK — “every desktop table/JSON path” and “every dry-run check” are unsupported.**
   Schema inspection found omitted reference-use tombstones/remote markers, reference tombstones/remote markers, fork sync-state payloads, and `conflict_json`.
   `audit/scripts/desktop_migration.py:135–146` leaves reference baselines unchanged; cloud re-keying also requires coordinated payload/version handling to avoid compare-and-set conflicts (`utils/reference_cloud_sync.py:344–359`).
   Verification at `desktop_migration.py:212–229` checks retirees only in `observations.sporely_taxon_id`, counts only three desktop tables, and never logs fork changes (`:207–210`).
   Current omitted tables/conflicts are empty; the actual copied databases reproduce **0 S1 / 17 ALL observation changes**. This supports today’s narrow sample, not a complete migration contract. **Blocks D8’s script approval.**

5. **CORRECTNESS — comparator/status evaluation remains fail-open in generalization.**
   Actual probes: `compare("Smith; Jones","Smith Jones") → typography`; identical `"Smith sens. str." → identical`; `"A. Smith"` versus `"A. B. Smith" → abbreviation`.
   Causes: semicolons disappear (`audit/scripts/authorship.py:86`), unrecognized qualification survives, and `zip()` ignores excess initials (`:188`).
   Current A1 contains **38 species author slots and 2 genus slots** with unequal populated initial counts; `et al.` also accepts unknown remaining authors (`:224`). These require author-reference evidence, not merely the label “abbreviation.”
   `rules.status_blockers("invalid","rejected") → []`; COL actually contains **29 rejected names**, although none enters current A.
   No current A0 semicolon example found. **Blocks D1’s fail-closed claim; strengthens the A1 expert gate.**

6. **RISK — cloud inventory is read-only but still incomplete.**
   Static inspection confirms `BEGIN READ ONLY`, aggregate SELECTs and `ROLLBACK` (`audit/scripts/cloud_reference_check.sql:9`, `:3227`).
   Migration-schema enumeration found unclassified **`public.taxa.taxon_id`, `public.taxa_vernacular.taxon_id`**, provider-id columns, and **`taxonomy_v3.identification_snapshot.original_legacy_taxon_id`**.
   The reference-use check follows observation identity only (`:3155`); it misses `snapshot_json.taxon_id` when that frozen evidence differs from the observation’s current taxon.
   Classify legacy/provider/historical paths explicitly; historical hits need reporting, not mandatory mutation. **Blocks treating D9 results as complete dependency clearance, not permission to run the SELECTs.**

7. **RISK — freezing rejected identity does not quarantine its publishing metadata.**
   P8 withholds bridges but retains bindings; the builder still carries scientific names and integer external ids (`build_sqlite_candidate.py:623–656`, `:754–803`).
   Reading compiler evidence found **572 accepted iNaturalist** and **608 accepted Artportalen rows** derived through failing legacy hosts.
   D12 must state whether these remain searchable/publishable and how their provenance is exposed; “unratified” is otherwise invisible to these consumers. **Blocks unconditional D10/D12 approval.**

8. **CLEANUP — several supplied counts are stale.**
   In-memory classification reproduced **A species/genus/infra/higher = 1,795/578/55/190**, all stated group counts, and **10,309/1,993/4,122** legacy outcomes.
   Parent reconstruction matched the manifest operation set: **1,768 + 8 = 1,776**, not 1,784. Author-filtered exclusions contain **19**, not 12 (`manifests/index.json:60`); SQL has **28** checks, not 27.
   Coverage reproduces **81.2/83.8/80.8%** and **84.6/85.1/84.6%**; 84.1% is obsolete. **Correct decision materials; no separate taxonomic blocker.**

**A0 false-positive estimate:** stratified seed **20261021**, independently read both archives for **60 species and 27 genera**: zero name/rank/status/comparator discrepancies or demonstrated false equivalences. Examples include Ramaria flava **56501/4RBQ5** and Volvariella bombycina **54815/7FSNQ**. Biological error rate remains unknown; matching nomenclature does not establish genus circumscription. A hypothetical fully detectable random sample would give roughly 5% species/11% genus upper bounds, not evidence of near-zero error.

**Earlier findings:** revision-3 #1/#2/#4 are resolved for current populations; #5/#6/#7 remain partly open; #3’s maintenance ordering is directionally fixed but its gate is defective; #8’s metric labeling is fixed; #9 remains incomplete for multi-user queues and irreversible reference changes.

**Ran vs inferred:** ran 66 comparator cases plus probes, 18 validator checks using the unmodified validator and in-memory file bytes, all partition hash/disjointness checks, source sampling, count/parent/coverage reproduction, local inventories and copied-database comparisons. No current supersession chains found. Queue failure and future-emission consequences follow verified code paths; no live cloud, IndexedDB, behavioral rollback or biological verification. No network, credentials, writes or commits; Git status unchanged.