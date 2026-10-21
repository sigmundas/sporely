# Review of revision 3 by Codex (gpt-6.1-sol, high effort), 2026-10-20

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

**Verdict: unsound as a basis for owner decisions.** The arithmetic largely reproduces, but revision 3 still overstates eligibility and migration safety.

Paths below: `audit/` = `docs/audits/2026-10-20-taxonomy-reconciliation/`; `WebM/` = sporely-web’s `supabase/migrations/`.

1. **CORRECTNESS — A0 ignores explicit nomenclatural statuses.**
   `audit/scripts/classify.py:222` compares authorship but never checks `nt_raw.nom_status` or `col_raw.name_status`.
   Independently reading NorTaxa `taxon.txt` and COL `NameUsage.tsv` confirmed **24 A0 members marked illegitimate**: 19 species, two varieties, two genera, one family.
   Examples: Mycena alcalina **627030→34119**, Morchella semilibera **634851→33606**, and Galerina pumila var. pumila **627063→164467**, also COL `unacceptable`.
   These contradict R4’s exclusion of `nom. illeg.` when that information resides outside authorship. Illegitimacy does **not** itself prove different concepts; it requires an explicit policy treatment. **Blocks D1, D2, D4, D11.**

2. **CORRECTNESS — staged A0 dependencies are calculated against eventual retirements.**
   `audit/scripts/groups.py:172` excludes every child in the combined A/A1/A2/B-placement retirement map.
   Reconstructing repaired parents found **145 omitted A0 child links**. **51 connect A0 parents to children deferred to A1/A2**.
   Example: A0 Didymellaceae **623821** parents A2 Stagonosporopsis **623822**. Retiring the family earlier requires an operation absent from `reparent-ops.jsonl`.
   The initial species+infraspecific stage avoids this particular defect: its four affected children retire together. Later rank batches require dependency calculation for each actual release. **Blocks genus/higher D2, D4, D6.**

3. **CORRECTNESS — “older desktops handle rejection” conceals a selection-clearing path.**
   `utils/cloud_sync_impl/identity_push.py:142` sends `p_sporely_taxon_id=None` after rejecting an older local id; it does not preserve a different remote survivor selection.
   Executing the extracted method in memory with local Hygrocybe conica **624178**, remote selected **16436**, produced `set_observation_identification_v2(None, external_unresolved)`.
   `WebM/20260922140000_atomic_observation_identification.sql:102` sets the selected id to that NULL. The observation survives, but its cloud selection is cleared.
   Compatibility needs a remap or protective guard before referenced retirements activate. **Blocks D8, D9 and referenced A2 rollout.**

4. **CORRECTNESS — 12,481 is a weaker evaluator’s pass count, not R1–R8 compliance.**
   `audit/scripts/existing_bindings.py:74–101` omits explicit name/rank/status checks, the full uniqueness test and external-id conflict checks.
   Reproduction returned exactly **12,481/3,943**, but “passing” includes **2,031 missing-authorship bindings**, **99 illegitimate names**, and **37 missing-authorship bindings with fewer than two agreeing higher ranks**.
   Separately, 11 cross-source A members violate written R4’s two-rank requirement, including Fungi **634865** and Ascomycota **634868**.
   Ratification needs the same complete evaluator and expert gate used for new candidates. **Blocks D1, D11; changes D12’s population.**

5. **CORRECTNESS — stopping `automatic_exact` leaves a future-release loophole.**
   `database/taxonomy/scripts/compile_release.py:932–965` binds synonym usages onto their accepted usage’s registry id, including a COL alias.
   A future, previously unseen NorTaxa synonym of a frozen legacy binding can therefore acquire COL identity without appearing in the finite legacy list or a pinned manifest. Existing data already contains **1,325 synonym usages on failing legacy hosts**.
   H11 only checks `cross_source_automatic_exact_*`; this path is `synonym_of_accepted`. Scope the transitive exception explicitly and test it.
   Also, supersession loading verifies manifest-file pins but carries no compiled-input pin checks onward (`compile_release.py:388–450`); R1 must implement those checks, not rely on today’s validator. **Blocks D1, D11, D12.**

6. **CORRECTNESS — the comparator is still not fail-closed.**
   Actual probes: `compare("Smith: Jones","Smith") → sanctioning`; `compare("Smith, Jones: Brown","Smith") → citation_stripped`; `compare("Smith, Brown","Smith Brown") → typography`.
   `audit/scripts/authorship.py:85` accepts any colon suffix as sanctioning; `:112` discards comma tails containing any digit/colon; `:82` removes author-list separators.
   Identical qualified forms such as `Smith nom illeg` and `Smith auctorum` also pass. A1 accepts malformed Mycosphaerella lineolata parentheses.
   **Current A0 check:** all sanctioning suffixes were Fr./Pers.; every citation-stripped A0 change was only a trailing year. These counterexamples concern rule generalization. **Blocks D1’s comparator adoption.**

7. **RISK — the cloud inventory and “zero everywhere” gate remain incomplete.**
   `audit/scripts/cloud_reference_check.sql:3188–3229` is genuinely read-only: `BEGIN READ ONLY`, aggregate SELECTs, `ROLLBACK`.
   Migration inventory additionally found omitted `taxonomy_v2_concepts`, taxa—including parent ids—scientific/vernacular names, external/legacy ids, and repair-history taxon columns.
   Historical registry/repair rows require classification, not a blanket zero requirement. NorTaxa external-unresolved references are also invisible when selected/resolved ids are NULL.
   Distinguish live dependencies, historical evidence and generated release rows; check source tuples whose resolution changes. **Blocks D9 and dependency clearance for D2/D5.**

8. **RISK — the coverage metric verifies name/rank, not each observation’s concept.**
   `audit/scripts/artsobs_lookup.py:125` accepts any species-rank holder when `target=None`; desktop coverage uses exactly that mode at `:161`.
   **465/573 = 81.2%**, **482/573 = 84.1%**, then **465/573 = 81.2%** reproduce exactly. They measure name-based lookup success.
   Comparing stored ids found three observations whose returned species id belonged elsewhere; stored-id provenance remains unverified. The script does not establish concept correctness for them. **Blocks D10’s “concept-verified” claim.**

9. **RISK — rollback and rejected-legacy splits lack an executable contract.**
   No rollback procedure appears in the staged plan. P8 promises a “split record,” but the current compiler refuses rebinding (`compile_release.py:775`) and supersession chains (`:1013`).
   Rolling back taxonomy bytes alone cannot undo repaired cloud pointers, shared-reference keys or queued selections. Define reversible operations and release compatibility before publishing. **Blocks D5/D8/D9 rollout and D12 implementation approval.**

**A0 false-positive estimate:** independently checked 40 species and 20 genera against both archives; all 60 manifest records matched, with **zero demonstrated biological false equivalences**. This sample cannot establish a low population error rate or genus circumscription equivalence. Confirmed **policy-ineligible rates are 24/1,439 = 1.67% overall and 19/976 = 1.95% for species**; those are eligibility failures, not proven false merges.

**Earlier findings still open:** review-2’s policy loophole, parent-dependency handling, cloud inventory, comparator fail-closure and coverage semantics remain partly open. Its validator-format/author-filtered finding is fixed; Ramaria ammophila and the cited structural exclusions are fixed. Its older-client safety conclusion is contradicted by finding 3.

**Ran:** in-memory audit/validator logic (**18 checks passed**, unmodified validator); 57 comparator cases plus adversarial probes; source-archive sampling; pair/group recounts; exact parent-operation reconstruction (**1,784**); red-list counts **478/240/78/205**; full coverage/binding reproduction; release hash and historical/proposed chain checks. Groups are disjoint; no current chains found.

**Not established:** live cloud counts, actual IndexedDB queues, restart behavior, biological circumscription, external-provider acceptance or rollback behavior. No network, credentials, production queries, file writes or commits; Git status remained unchanged.