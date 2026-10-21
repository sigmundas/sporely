# S1 approval sheet (revision 5, 2026-10-21)

Nothing is approved. The count is **provisional** until the Codex review of
revision 5 independently verifies it. Evidence: `data/s1-candidate.json`,
`data/s1-synonyms.json`, `data/s1-desktop-gates.json`.

**Candidate.** 988 NorTaxa concepts would retire onto their COL survivors:
943 species and 45 infraspecific (34 varieties, 10 forms, 1 subspecies). An
independent re-derivation (`s1_derive.py`) equals the manifest member set and
`id-mapping-S1.json` exactly. No member was removed.

**Pins (sha256):**
- `A0-species` manifest: file `62790e59…`, members `7fd5ef6b…`.
- `A0-infraspecific` manifest: file `6706b6a6…`, members `5b0c9080…`.
- S1 pairs `f5975af7…`; `id-mapping-S1.json` `86f4726a…`.
- Release `e349e8b1…`, NorTaxa `29c11c54…`, COL `397d701c…`, comparator `23f75f37…`.

**Independence.** S1 needs 0 re-parenting operations: its 4 child links all
stay inside S1. S1 forms no chain and no duplicate with the 1,353 shipped
supersessions. No survivor is retired in another group. No survivor hosts a
legacy name binding. Every nomenclatural status is empty or `acceptable`.
Hygrocybe conica 624178→16436 is in S1.

**Reference impact** (S1 retirees only):
- **Desktop, checked on copies.**
  - 0 live or mirror references to retiree Sporely ids.
  - 44 references in the NorTaxa id space, kept verbatim: 21 Artsorakel `NBIC:` suggestions, 22 snapshot mirrors of them, and 1 `external_unresolved` tuple (55146 → 163349).
  - Generated release rows: 988 taxa, 2,160 names, 779 vernaculars, 3,149 external ids and 467 red-list rows (0 collisions).
  - Historical copies: 0 hits in 7 backups and 16 profile databases.
- **Cloud.** In the 2026-10-20 snapshot, 0 rows select or resolve a retiree, and 0 retirees are in cloud scope. Everything else is **unverified**: 41 S1 checks and 6 gates in `scripts/cloud_reference_check.sql`.

**Synonym treatment.** 1,175 synonyms sit on 457 retirees: 1,016 with a
blocked status, 38 qualified and 7 unpublished or doubtful. Today's compiler
would publish all of them as `reviewed_supersession` bridges.

The proposal (**P10**): only each member's own accepted usage becomes a
bridge, so 988 bridges in all. Synonyms move to the survivor as names. Each
keeps its evidence class (`intra_source_synonym`, never emitted as a bridge),
its taxonomic and nomenclatural status, and `inherited_via_supersession`
provenance.

This needs no schema change, so it is **not a blocker**. The real builder
showed it on 4 retirees: 20 bridges became 4, and the names were unchanged.

**Required changes:**
1. **Policy:** the batch-manifest clauses (P1, P4, P5, P7) and P10, scoped to new records.
2. **Compiler:**
   - `compile_release.py:1083–1092`: promote only the anchor;
   - ledger field `inherited_usages: names_only`;
   - tests H14 and H16.
3. **Ledger:** 988 records and 2 `approved_manifests` entries, with the manifests committed.
4. **Desktop:** no row migration. Install the release and run the gates.
5. **Cloud,** with separate authorisation: run the S1 SQL read-only, then import and activate the release. No row repairs.

**Gates on today's baseline** (desktop gates were run on copies):

| Gate | Expected |
|---|---|
| G1a: references to retiree Sporely ids | 0 |
| G1b: NorTaxa-space references | 44 (unchanged) |
| G2: ids absent from the release, today / after S1 | 0 / 0 |
| G3: identities outside cloud scope | 45 = 43 NorTaxa with a tuple + 2 COL; set `e1dc3761…`; 0 of them are S1 ids |
| G4: fallback exposure | 2 |
| G5: NorTaxa tuples on S1 ids | 1 |
| G6: pending state | 8 dirty observations, 0 conflicts, 0 tombstones |
| G7: survivors absent from the release | 0 |
| Dry run | 0 rows changed; 27/27 table hashes identical; integrity ok |

**Remaining S1 blockers:**
- **B1.** The cloud checks have not been run.
- **B2.** The policy and compiler change is not implemented.
- **B3.** The CG5b precondition is unverified: the remote rows of the 2 fallback observations must hold no selection.

**Owner decisions:**
- **S1-1.** Approve exactly the two pinned manifests (988 pairs).
- **S1-2.** Adopt the S1 policy subset and P10 for new records. The 1,353 shipped supersessions (4,779 synonym bridges) stay as they are; that is a separate decision.
- **S1-3.** Authorise the implementation stage, which does not include a release.
- **S1-4.** Authorise a read-only run of the S1 SQL.
- **S1-5.** After the gates pass, authorise the build, import and activation.
- **S1-6.** Accept that S1 rewrites no `external_unresolved` cloud row.
