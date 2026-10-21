# Taxonomy reconciliation audit — `tax-2026.10.07-01` (revision 5, S1 scope)

Date: 2026-10-21. **Nothing is approved.** This revision is read-only.
- No production or cloud queries were made; cloud figures are from the 2026-10-20 snapshot.
- Desktop data was read only from immutable read-only opens and backup-API copies.
- The repository is unmodified.

The owner has narrowed scope to **S1**: retiring the A0 species and
infraspecific NorTaxa concepts onto their COL survivors. Decision sheet:
[s1-approval-sheet.md](s1-approval-sheet.md). Earlier reviews are preserved
unchanged: [review.md](review.md), [review-2.md](review-2.md),
[review-3-codex.md](review-3-codex.md) and
[review-4-codex.md](review-4-codex.md).

## Revision 5: S1

**1. Frozen candidate: 988, provisional.** `scripts/s1_derive.py` re-derives
S1 independently of `classify.py` and `groups.py`. It shares only the rule
definitions (`authorship.py`, `rules.py`) and runs against a work DB rebuilt
from scratch. The result is 943 species and 45 infraspecific concepts. It
equals both manifests and `data/id-mapping-S1.json`, with 0 field mismatches.
Re-running the revision-4 pipeline on the rebuilt work DB reproduces every
manifest byte for byte.

**Pins:**
- `A0-species`: file `62790e59…`, members `7fd5ef6b…`.
- `A0-infraspecific`: file `6706b6a6…`, members `5b0c9080…`.
- S1 pairs `f5975af7…`; S1 members `382cbefd…`.

**Dependency closure:**
- Re-parenting operations: 0. There are 4 child links, under both the shipped and the corrected parent maps, and all of them stay inside S1.
- 0 chains or duplicates with the 1,353 shipped supersessions.
- 0 survivors retired elsewhere, and 0 legacy bindings on survivors.
- 42 pairs left S1 because of live references, and 33 because of children; both groups are already in A2.
- Nomenclatural status on members: NorTaxa always empty; COL empty or `acceptable`.
- The review-4 comparator probes (`;`, sense qualifiers, `et al.`) match 0 S1 strings.

S1 does not depend on any decision about A1, A2, genera, the parent fix or legacy bindings.

**2. Synonym inheritance** (`scripts/s1_synonyms.py`). This reproduces
review-4: the retirees carry 1,175 synonym usages (on 457 retirees). Of
these, 1,016 have a blocked nomenclatural status, 38 are qualified and 7 are
unpublished or doubtful. `compile_release.py:1083–1092`, executed verbatim,
gives all 1,175 the class `reviewed_supersession`, so 1,175 extra bridges
would be emitted.

Proposal P10 (`policy-amendment-proposal.md` §5): only the member's own
accepted usage is promoted. Every other usage moves to the survivor as a
name, keeping:
- its own class, `intra_source_synonym`, which the policy rejects for bridge emission;
- its status;
- `superseded_from` and `inherited_via_supersession` provenance.

With the proposal, all S1 usages emit 988 bridges and 0 synonym bridges.
`build_sqlite_candidate._build_into` ran unchanged on 4 retirees
(Hygrocybe conica plus 3 with blocked or qualified synonyms):
- **Today's compiler:** 20 bridges.
- **Proposed compiler:** 4 bridges.
- **Both:** 20 scientific names and 20 NorTaxa integer ids preserved.

**Representable without a schema change:**
- `source_usages.jsonl` gains two optional fields.
- `note` can carry the provenance. That is optional: it would break the exact `note` match at `audit_stage0_coverage.py:89`.
- The cloud export carries `note` verbatim.

**Not an S1 blocker.**

Context, not in S1: the shipped release already publishes 4,779 synonym
bridges through the 1,353 earlier supersessions. P10 is scoped to new ledger
records, so S1 does not withdraw them.

**3. Reference inventory** (`scripts/s1_desktop_gates.py`; S1 checks in
`scripts/cloud_reference_check.sql`):

| Location | Class | S1 retirees | Status |
|---|---|---|---|
| Desktop: every column and JSON leaf of the 27 tables (tombstones, remote markers, fork sync, `conflict_json`, settings snapshots, reference uses) | live / mirror | 0 Sporely-id hits | verified (copies) |
| Desktop: NorTaxa-space provider ids (`NBIC:` AI suggestions, their snapshot mirrors, 1 `external_unresolved` tuple) | live, kept verbatim | 44 | verified |
| Desktop: frozen `snapshot_json`, session logs, AI payloads | historical | 0 structured hits | verified |
| Desktop: 7 backups and 16 profile databases | historical | 0 | verified (immutable read) |
| Installed release and profile copies | generated | 988 taxa, 2,160 names, 779 vernaculars, 3,149 external ids, 467 red-list rows, 0 parents | verified |
| Cloud: `selected`/`resolved` | live | 0 | snapshot 2026-10-20 |
| Cloud: everything else (41 checks: identity tuples, AI ids, reference tables, JSON leaves, `identification_snapshot`, legacy `taxa`/`taxa_vernacular`, registry, active release rows, repair history) | classified per check | – | **unverified** |
| Web and Android IndexedDB | – | – | **unverified**: cannot be inspected |

Retirees are outside the cloud scope, so they are absent from the active
release. As a result:
- `search_taxa_v2` cannot return them;
- both writing RPCs reject them through the active-release guard.

**4. Gates.** The desktop gates ran on copies; the cloud gates are SQL with
the expected value written beside each.
- **G1a:** 0 references to retiree Sporely ids.
- **G1b:** 44 references in the NorTaxa space, unchanged.
- **G2:** 0 ids absent from the release today, and 0 after S1.
- **G3:** 45 identities outside cloud scope: 43 NorTaxa with external evidence and 2 COL. The set is pinned as `e1dc3761…`, and 0 of them are S1 ids.
- **G4:** 2 dirty observations take the fallback at `identity_push.py:130–144`.
- **G5:** 1 NorTaxa tuple on an S1 id.
- **G6:** 0 conflicts and 0 tombstones.
- **G7:** 0 survivors missing.
- **Cloud CG0–CG5:** active release; selected ids outside the release = 0; resolved ids outside the registry = 0; drift = 87; the 20 ids 634896–634916, none of them in S1; no inconsistent `external_unresolved` selection.
- **CG5b:** the remote rows of the G4 observations must hold no selection.

**5. Runbook.** See `staged-release-plan.md`, under "S1 runbook". The dry
run changed 0 rows; all 27 table hashes are identical; integrity and foreign
keys are clean.

**Validation.** `scripts/test_manifests_validator.py` passes 18 checks with
the unmodified repository validator.

## Changes since revision 4

| Review-4 finding | Revision 5 |
|---|---|
| 1. Synonyms upgraded by supersession | Counts reproduced. P10 proposed, with the compiler change. Real-builder proof on a sample. Not a blocker. |
| 3. Gates fail on today's data | Desktop and cloud membership are checked separately. The 43 + 2 identities outside cloud scope are pinned, the 20 cloud ids are allow-listed and checked (CG4), and fallback exposure is measured (G4 plus CG5b). |
| 4. Desktop inventory incomplete | The scan now covers every column and JSON leaf, plus backups and profiles. The dry run compares hashes for all 27 tables. |
| 6. Cloud inventory incomplete | The S1 section adds `taxa`/`taxa_vernacular`, `original_legacy_taxon_id`, `snapshot_json` and envelope leaves, provider ids and `external_mapping`, each classified. |
| 8. Stale counts | Re-parenting is 1,776 (1,768 + 8), not 1,784. Author-filtered pairs are 19, not 12. The main SQL has 28 checks, not 27, plus 41 S1 checks and 6 gates. The 84.1% figure is obsolete: the correct figures are 84.6% and 85.1%. The 988 count is provisional. |
| 2. Web/Android remap | Out of S1 scope. No S1 retiree can be selected, queued as a Sporely id or resolved in the cloud (snapshot: 0). S1 rewrites no cloud row, so no queued literal comparison changes. |
| 5. Comparator fail-open | Out of S1 scope. A0 excludes `abbreviation`, the probes match 0 S1 strings, and no S1 member has a `rejected` or `invalid` status. It still matters for A1. |
| 7. Publishing metadata of frozen legacy bindings | Out of S1 scope. S1 neither ratifies nor freezes any legacy binding. |

## Deferred (not part of S1)

The revision-4 material below is kept for later stages. It is not part of
the S1 decision.

Relationship-based pass over the 2,805 species pairs not in S1 (read-only,
2026-10-21): [relationship-pass/README.md](relationship-pass/README.md).

### 1. Results

**Cross-source pairs** (`data/summary.json`):

| Rank | A | B | C | D |
|---|---|---|---|---|
| Species | 1,795 | 1,948 | 3 | 2 |
| Genus | 578 | 1,273 | 25 | – |
| Infraspecific | 55 | 29 | – | – |
| Higher | 190 | 136 | – | 6 |

- Different-rank pairs: C 32.
- COL-internal groups: A 4, B 120, C 30.
- A2-author-filtered: 19 pairs (not 12).
- NorTaxa-internal groups: A 4, B 7, C 18.
- B-placement: 508 pairs.

**Disjoint groups**, precedence A2 > A1 > A0, with dependencies computed per
release stage (`data/groups_summary.json`):

| Group | Species | Infraspecific | Genus | Higher | COL-internal |
|---|---|---|---|---|---|
| A0 | 943 | 45 | 301 | 66 | – |
| A1 | 766 | 10 | 54 | 12 | – |
| A2 | 86 | – | 223 | 112 | 4 |

**Stages:**

| Stage | Members | Re-parenting operations |
|---|---|---|
| S1 (A0 species + infraspecific) | 988 | **0, verified** |
| S2 | 367 | 0 |
| S3 | 842 | 0 |
| S4 (A2) | 425 | 1,768 |
| S5 (B-placement) | 508 | 8 |
| Total | | 1,776 (not 1,784) |

**Legacy bindings (16,424)**, re-evaluated with the same `rules.py` evaluator:

| Status | Count |
|---|---|
| Pass, A0-equivalent | 10,309 |
| Pass through missing authorship only (A1-equivalent) | 1,993 |
| Fail | 4,122 |

1,424 synonym usages are bound through failing hosts.

**Desktop data.** The S1 mapping touches 0 desktop rows. The mapping for all
groups touches 17 `observations.sporely_taxon_id` rows. Every dry-run check
passes (`data/desktop_migration.json`).

**Artsobservasjoner, desktop observations:**

| Metric | Before | After S1 | After all A |
|---|---|---|---|
| Name-lookup success | 81.2% | 83.8% | 80.8% |
| Concept-verified (195 observations with a stored id) | 84.6% | 85.1% | 84.6% |

3 observations get an id that belongs to another concept: 55311→55368,
56092→626199 and 61673→626822.

**Red list:** 467 rows on A0 retirees, 240 on A1, 78 on A2 and 203 on
B-placement.

**Duplicate rows** (identical labels): 5,606 today, 3,250 after all A groups.

**Hygrocybe conica** is A0 (S1).

### 2. Comparator and rules (`scripts/authorship.py` rev 4, `scripts/rules.py`)

The comparator is fail-closed, because no standard author-abbreviation list
exists locally. Its rules:
- **Sanctioning:** only `: Fr.` and `: Pers.` are stripped, the sanctioning
  authors seen in the data.
- **Citation stripping:** only a trailing year is removed.
- **Separators** are significant.
- **Qualifiers:** identical qualified strings still count as qualified.
- **Malformed parentheses** are never batchable.

The new `rules.py` adds nomenclatural-status blocking on either side:
- NorTaxa: illegitimate, dubious, notvalidlypublished, orthographic,
  misapplied;
- COL: unacceptable, doubtful, manuscript, not established.

The two-rank rule for missing authorship now applies at every rank.

### 3. Section 2 populations

Unchanged; see `data/section2_counts.json`.

### 4. Migration

The owner is the only user of desktop sync. The desktop is migrated in a
maintenance window, using scripts reviewed beforehand
(`staged-release-plan.md` R3):
1. pause sync;
2. back up and verify;
3. apply the pinned old → survivor mapping;
4. inventory;
5. dry run;
6. migrate;
7. make the cloud changes, under separate authorisation;
8. install the release and check consistency;
9. resume sync.

There is no compatibility layer for older desktops. Web and Android remain in
scope. Rollback and reversibility are set out in the plan.

### 5. Limitations

- **Cloud counts beyond the snapshot are unverified**; see
  `scripts/cloud_reference_check.sql`: 28 classified checks (revision 4 said 27), plus the S1 section.
- **Not inspected:** web and Android IndexedDB contents.
- **Not assessed:** provider acceptance and biological circumscription.
- **A1 needs** an expert sample and a standard author list.

### Changes since revision 3

| Finding / item | Change |
|---|---|
| Codex 1: nomenclatural status | `rules.status_blockers`; new B sub-class `nomenclatural_status`. Moved out of A: 24 species, 2 infraspecific, 3 genus, 1 higher (`data/transitions_v3_v4.json`). |
| Codex 2: per-stage dependencies | Per-stage closure in `groups.py`. S1 has 0 operations; 33 + 284 + 41 pairs moved to A2. `reparent-ops.jsonl` now records the stage. H15 added. |
| Codex 3: selection clearing | Handled by maintenance-window ordering (owner scope) and checked in the consistency step. |
| Codex 4: legacy evaluator | `existing_bindings.py` uses the full R1–R9 evaluator: 10,309 / 1,993 / 4,122. The 11 two-rank violations are fixed (6 → B, 5 → D). |
| Codex 5: synonym loophole and pins | Policy P9, build test H13, manifest pin verification (H14), all in R1. |
| Codex 6: comparator | Fail-closed changes and 9 new negative tests (66 cases in total). |
| Codex 7: cloud inventory | 27 checks: concepts, taxa and parent ids, names, external and legacy ids, repair history, NorTaxa tuples, each classified. |
| Codex 8: coverage semantics | Relabelled as name-lookup success. A new concept-verified metric covers observations with a stored id; the 3 mismatches are reported. |
| Codex 9: rollback and split | A reversibility table is in the plan. Split is declared non-executable (P8). |
| Owner B: migration scope | Maintenance-window runbook, desktop inventory and dry run on copies. `taxon_supersession_min` is now optional. |
