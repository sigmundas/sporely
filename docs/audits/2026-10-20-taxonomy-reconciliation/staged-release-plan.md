# Staged release plan (proposal; revision 5)

Nothing is approved. Production steps need explicit operator approval.

**Scope (owner, 2026-10-21).** The owner is the only user of desktop sync. His
desktop data is migrated directly, by reviewed scripts, in a maintenance
window. There is no compatibility layer for older desktop installs, and
`taxon_supersession_min` is optional, not a blocker. Web and Android
compatibility stays in scope.

## S1 runbook (revision 5)

S1 changes no desktop row. Its maintenance window is therefore an install
and verification window, not a data migration. Every step has a stop
condition, and any deviation stops the window. Cloud steps need their own
explicit authorisation.

1. **Preconditions.**
   - Owner decisions S1-1 to S1-4 (`s1-approval-sheet.md`) are made.
   - The implementation stage has passed: P10 in the compiler, ledger records, H14 and H16.
   - An operator has run `scripts/cloud_reference_check.sql` (S1 section) read-only.
   - *Stop if* any `live_dependency` check differs from its stated expectation, or CG1, CG2 or CG5 is non-zero.
2. **Pause sync** on the owner's desktop and on his web and Android sessions. Run `s1_desktop_gates.py` and record G6. *Stop if* there are conflicts, pending tombstones, or new dirty rows on S1 ids.
3. **Back up and verify.**
   - Desktop: back up `mushrooms.db`, `reference_values.db`, the installed release file and `install_receipt.json` with the SQLite backup API. Record the sha256 of each, run `PRAGMA integrity_check`, and compare per-table hashes between the original and the copy, using the routine in `s1_desktop_gates.py`.
   - Cloud: the operator saves the S1 SQL output and a read-only export of the rows it counts. This needs authorisation.
   - *Stop if* any hash or integrity check fails.
4. **Pinned mapping:** `data/id-mapping-S1.json`, sha256 `86f4726a…`; canonical mapping `50fb7e9c…` (988 pairs).
5. **Dry run on copies** with `s1_desktop_gates.py`.
   - Expected: 0 rows changed; 27/27 table hashes identical; 0 S1 Sporely-id hits after the run; integrity ok; 0 foreign-key rows.
   - *Stop if* any row would change. S1's scope would have moved, and it must be re-audited.
6. **Cloud,** with separate authorisation: import and activate the S1 release. No observation, reference or resolution row is repaired. `external_unresolved` tuples on S1 NorTaxa ids (at least 1) stay as they are.
7. **Install** the S1 release on the desktop, then re-run the gates against it.
   - Desktop: G1a = 0; G1b = 44; G2 = 0; G3 = 45 with the same set hash `e1dc3761…`; G5 = 1; G7 = 0.
   - Cloud: CG0 names the S1 release; CG1 = 0; CG2 = 0.
   - S1 survivors: 499 present in the active taxa; retirees: 0 present.
   - Bridges: 988 for S1 anchors; 0 for synonyms whose `superseded_from` is an S1 retiree.
   - *Stop if* any value differs.
8. **Resume sync,** desktop first.
   - Before resuming, check CG5b: the remote rows of the 2 G4 observations hold no selection.
   - *Stop if* either does: the fallback would clear it.

**Rollback.** Survivors exist in both releases, and S1 rewrites no stored id.
- **Desktop:** reinstall `tax-2026.10.07-01` (sha256 `e349e8b1…`). Restore the step-3 backups only if a gate shows that data changed.
- **Cloud:** reactivate the previous release. Only generated release rows differ.
- **Registry:** allocations are append-only and harmless.
- **What stays after rollback:** a selection a user made after activation names a survivor, which remains valid under the old release.

**Web and Android.** Nothing is required for S1:
- retirees are outside the cloud scope, so `search_taxa_v2` cannot return them;
- the active-release guard rejects them;
- the snapshot shows 0 selected or resolved retirees;
- S1 rewrites no cloud row, so queued literal comparisons are unaffected.

IndexedDB contents remain unverified. The legacy `search_taxa` fallback reads
`public.taxa`, whose id space is unverified; that is S1 check
`public.taxa.taxon_id`.

## Deferred (not part of S1)

Revision-4 plan for all stages, kept for later; R2 and R3 are superseded for S1 by the runbook above.

### Release stages

The stages are disjoint (`data/groups_summary.json` `stage_dependency_closure`).

| Stage | Members | Re-parenting operations in stage |
|---|---|---|
| **S1** A0 species + infraspecific | 988 (943 + 45) | **0, verified** (33 species moved to A2 because their children retire later) |
| S2 A0 genus + higher | 367 | 0 (284 moved to A2) |
| S3 A1 (after the expert gate) | 842 | 0 (41 moved to A2) |
| S4 A2 | 425 | 1,768, listed per stage in `manifests/reparent-ops.jsonl` |
| S5 B-placement (only if D3) | 508 | 8 |

Dependencies are computed per stage, against what is retired by the end of
that stage, never against the union of all stages. Each stage is its own
immutable release, and its dependency closure is recomputed and gated (H15)
when its release is built.

### R1 — Tooling, no release

- **Policy:** P1–P9.
- **Compiler** (`compile_release.py`):
  - stop applying `automatic_exact` aliases outside manifests, ratified legacy
    bindings and manual mappings;
  - limit `synonym_of_accepted` inheritance as P9 specifies;
  - verify each manifest's pins against the compiled inputs.
- **Manifest and supersession generators**, plus the parent-link fix (per D6).
- **Gates:** `pytest database/taxonomy/tests`, the audit's
  `scripts/test_manifests_validator.py` (18 pass), and H1–H15.

### R2 — Candidate release S1

- Build twice.
- Stage 6P accounting, including the 467 red-list rows on A0 retirees.
- **Concept-verified Artsobservasjoner baseline** for desktop observations
  with a stored id: 84.6% before, 85.1% after S1. It must not drop.

### R3 — Maintenance window: desktop and cloud

Run every step on scripts reviewed beforehand. Each step has a check, and a
failed check stops the window.

1. **Pause sync** on desktop, web and Android. Record the pending state:
   `observations.sync_status`, `sync_blocked_reason`, the reference sync-state
   tables, and the web/Android IndexedDB queues and drafts. Drain or export
   pending items first.
2. **Back up and verify.**
   - Copy `mushrooms.db` and `reference_values.db` with the SQLite backup API.
     Record their sha256, and run `PRAGMA integrity_check` on both.
   - Take a cloud backup or snapshot, which needs operator authorisation.
3. **Mapping.** Use the pinned old → survivor mapping for the stage
   (`data/id-mapping-S1.json`, with its sha256 in
   `data/desktop_migration.json`). It is generated deterministically from
   `manifests/index.json`.
4. **Inventory.** `scripts/desktop_migration.py` covers every desktop
   table, column and JSON path holding a taxon id or identity tuple
   (`data/desktop_migration.json`):
   - **Remapped:** `observations.sporely_taxon_id` and Sporely-namespace
     `taxon_identity_*`; the cloud snapshot `taxon_identity` in `settings`
     (`sporely_cloud_snapshot_obs_*`), only together with its cloud row;
     `reference_taxon_treatments.taxon_id`; `curated_reference_forks`.
   - **Kept verbatim:** provider tuples (they re-resolve through the bridge),
     iNaturalist and AI provider ids, and historical reference snapshots.
5. **Dry run on copies.** This was done in the audit, against scratchpad
   copies only:

   | Scope | Rows changed | Retirees left | Integrity check | Row counts, `sync_key`, `cloud_id` |
   |---|---|---|---|---|
   | S1 | 0 | 0 | ok | unchanged |
   | All groups | 17 `observations.sporely_taxon_id` | 0 | ok | unchanged |

   Every change is logged in `taxonomy_migration_log`.
6. **Migrate the desktop**, with the same script in apply mode (a reviewed
   change to the script). Repeat the verification.
7. **Cloud changes**, planned separately and needing explicit authorisation:
   - run `scripts/cloud_reference_check.sql` read-only. Every count it returns
     is unverified until it is run. It classifies each hit as
     `live_dependency`, `registry_append_only`, `generated_release_row` or
     `historical_evidence`.
   - repair the `live_dependency` rows (resolution links, selections,
     Sporely-namespace identities, references and contributions);
   - import and activate the candidate release.
8. **Install** the new taxonomy release on the desktop. Verify desktop/cloud
   consistency:
   - every desktop `sporely_taxon_id` and every cloud selected or resolved id
     exists in the active release;
   - the cloud snapshot settings agree with the cloud rows;
   - no observation would push `p_sporely_taxon_id=None`.
9. **Resume sync**, desktop first, then web and Android.

**Hazard handled by ordering.** `identity_push.py:142` pushes
`p_sporely_taxon_id=None` for a local id the cloud rejects, and that clears the
cloud selection. Steps 1, 6, 8 and 9 make sure no unmigrated desktop pushes
against a release that lacks its ids.

**Web and Android.**
- IndexedDB drafts and queues can hold only cloud-scope COL ids. For S1–S3
  these are survivors, never retirees.
- A2-col-internal retirees (4) can sit in drafts. Before S4 they need a
  server-side retired → survivor remap in `set_observation_selected_taxon_v2`,
  or a client remap. Otherwise the RPC and
  `_guard_selected_sporely_taxon_id_v2` reject them.

### Rollback and reversibility

| Change | Reversible? | How |
|---|---|---|
| Desktop data | Yes | Restore the step-2 backups, after checking their sha256 |
| Taxonomy release | Yes | Reactivate the previous release (cloud); reinstall the previous bundle (desktop) |
| Cloud pointer repairs (resolution links, selections) | Only if logged | Log every changed row (old → new) in the repair audit run, and run the inverse from the log. The 09.30 pattern records evidence; an inverse script must be written and reviewed before S4. |
| Registry allocations and retired anchors | No, but harmless | The registry is append-only |
| Shared-reference keys, and selections already submitted from web/Android | No | Hence the pause in step 1, and the remap before S4 |
| Rejecting a legacy binding (a split) | No mechanism exists | The compiler refuses re-binding (`:775`) and chains (`:1013`). Not proposed. |

### Separate tracks

- The Artsobservasjoner Sporely-id fallthrough fix (its own branch).
- Validation of COL `inat:` ids and the Dyntaxa → Artportalen mapping.
- Design of the legacy split mechanism.
