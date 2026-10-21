# Second review of the revision 2 audit (2026-10-20)

A separate reviewer agent with fresh context ran this read-only review. It
modified no files and ran no production queries. None of these findings has
been corrected yet; see [review.md](review.md) for the first review.

**Verdict: sound with corrections.** The counts and manifests reproduce, and
A-species is close to clean. Five items need fixing before owner decisions
D1, D2, D6, D9 and D10.

## Findings

1. **CORRECTNESS: the amendment does not settle the policy/compiler
   conflict.**
   - `mapping_policy.yml` line 51 (`only_reviewed_relationships_are_eligible`:
     "per-association human review, and nothing else") and the `rejected`
     entries for `cross_source_automatic_exact_strict` and
     `_missing_authorship` still stand. The new `batch_reviewed_identity`
     contradicts them without amending them.
   - The proposal's §2 says only manifest members and ratified aliases apply.
     §4.2 says the 3,942 failing bindings "stay as registry identity". Both
     cannot hold.
   - **Loophole:** `staged-release-plan.md` R1 has no task that stops
     `compile_release.py` from binding by name. The next NorTaxa source update
     would still merge automatically.
2. **CORRECTNESS: a provisional name is in A.**
   - Ramaria ammophila 626337 → 603885 is A-species, class `typography`. Both
     sides read `nom. prov.` / `nom.prov.`.
   - Ascocoryne inflata 623714 has the same problem in B-placement.
   - `authorship.py` (`_UNPUBLISHED` / `_QUALIFIER`) misses `nom. prov.`.
     Treat it like `ined.` and add a negative self-test case.
3. **RISK: the "no children" guard (R9 / H9) checks nothing today.**
   - The parent-link defect means no concept has a NorTaxa parent, so the
     guard passes for every retiree.
   - In the source, retirees are parents of valid usages: 61 in A-species,
     508 in A-genus and 196 in A-higher (`nt_raw.parent_id`).
   - After D6 option (a) or (b), R9 plus fail-closed would refuse 61 applied
     A-species supersessions.
   - The defect doc says the compiler re-parents through re-keyed usages,
     which contradicts R9's rationale. Settle D6 before or with D2, not only
     with D4.
4. **CORRECTNESS: the validator does not accept the manifests as claimed.**
   - `bridge_emission.py` (~380–385) requires `one_to_one is True` on every
     member.
   - 13 A pairs are one-to-one only after authorship filtering. Four are
     species: Stereum durum, Helvella crispa, Uromyces trifolii and Alectoria
     asiatica. The other 9 are genus or higher.
   - The audit's JSONL files also lack the required `approval_mode` /
     `columns` / `members` layout.
5. **RISK: `cloud_reference_check.sql` misses data.**
   - It misses `public.reference_taxon_treatments.taxon_id` (text,
     `20260828143513`).
   - It misses observations whose `taxon_identity_*` fields hold a Sporely
     namespace id.
   - The query is verified read-only.
6. **RISK: the §9 Artsobservasjoner coverage counts wrong-taxon ids.**
   - When the species concept has no `norwegian_taxon_id`, the name lookup
     returns an infraspecific concept's id. Example: Cuphophyllus virgineus
     gets var. virgineus 186443.
   - This affects 126 name keys and 28 desktop observations before A, and
     122 keys and 14 observations after A.
   - The 86.0% baseline used in R2 and H10 is therefore inflated.
7. **RISK: the comparator still rests on a block-list.**
   - Any abbreviation still matches any longer surname not listed in
     `KNOWN_DISTINCT`. These all pass as `abbreviation`: Fr./Frank,
     Fr./Fritsch, Bres./Bresinsky, Arn./Arnaud, Henn./Hennebert.
   - None of these occur in A today. All 102 prefix pairs used by A-species
     were checked and found correct.
   - Elsewhere the comparator is too strict, which is safe: it rejects
     Pers./Persoon, Karst./Karsten, `Fr. in Berk.`/`Fr.`, and
     `(Bull.) Fr.`/`(Bull. ex Fr.) Fr.`.
   - Typography ignores structure: `Scop.: Fr.` equals `(Scop.) Fr.`.
   - Before the rule is pinned for future releases, replace the block-list
     with a standard author-abbreviation list.
8. **CLEANUP.**
   - The table of failing existing bindings mixes sole-blocker counts with
     any-blocker counts, so its rows sum to 4,075, not 3,942. 554 bindings
     have a class or phylum disagreement.
   - The direct insert from web is validated after all.
     `_guard_selected_sporely_taxon_id_v2` (`20260802130000`) blocks non-RPC
     writes of `selected_sporely_taxon_id`. Older desktops handle the
     rejection (`identity_push.py`,
     `_preserve_identity_missing_from_cloud_release`), so the stage order is
     safe for them.

## Status of the first review's findings

- **Fixed:** findings 1, 3, 5 and 7, and finding 9's parent counts and its
  `stale_sporely_identity` point.
- **Partly fixed:**
  - 2: the prefix rule is still loose (finding 7 above).
  - 4: `nom. prov.` is still missed (finding 2).
  - 6: the amendment is incomplete (finding 1).
  - 8: the cloud query has gaps (finding 5).
  - 9: the validator's one-to-one requirement is not met (finding 4).

## A-species false positives

- **Stratified sample of 30** (12 abbreviation, 7 sanctioning, 6 typography,
  5 missing; seed 20261020): 0 false merges.
- **Full scan of the 1,824 members:** 1 policy violation, Ramaria ammophila.
- **Estimated rate:** about 0.1% definite. Pseudolachnea hispidula is weak:
  it agrees only at phylum level.

## What was run and what was inferred

**Ran:**
- the self-test (46 cases, both orders), plus about 80 adversarial cases;
- all six `members_sha256` values and the release pin, which match;
- A/B/C/D per rank;
- the transitions 110 = 73/29/7/1;
- 16,424 = 10,451 + 3,840 + 2,031 + 102, including 2,777 placement-only;
- the 14 proven desktop observations;
- the Artsobservasjoner script, which is unchanged under the fix in 38b33cf;
- the parent-link root cause (`build_sqlite_candidate.py:334–366`), which
  holds.

**Inferred, not run:**
- that production holds only COL concepts in the cloud scope;
- whether `reference_taxon_treatments.taxon_id` always holds a Sporely id.

## Still needs a person

- A mycologist sample of the 796 abbreviation members.
- An owner decision on whether `nom. prov.` names are ever batchable.
