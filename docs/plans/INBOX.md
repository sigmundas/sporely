# Development inbox

Quick observations captured while using Sporely.
Items here are not approved implementation plans.
Add a note under the nearest heading, or use “Unsorted / needs review” when its home is unclear.

## Cloud sync / recovery
- Value-identical image metadata PATCHes: syncs of the identity backfill logged "metadata patch … source bytes unchanged" on observations whose stored image baselines did not change (456, 460, 462, 502, 505, 442), followed by a child-change pull. The contract forbids no-op cloud writes (they advance the child-change cursor). Find which field the PATCH sends and skip it when the cloud already matches.
- The two copies of `docs/supabase-sync-contract.md` (sporely-py and sporely-web `main`) differ by about 600 lines that predate 2026-10-19 (for example the personal-copy provenance paragraph and the pulled-identity paragraph). Reconcile them in one docs change.
- Sync summary counters mislead: "touched N dirty observation(s) with no real change" ignores identity RPCs, and `measurement push … pushed=N` counts unchanged rows.
- Old v1/v2 spore-mosaic rows (and storage objects) remain next to the current v4 row per observation. Decide whether to clean them up.
- NorTaxa-fallback identity comparison: an observation bound locally to a NorTaxa-only concept (`sporely_v2`, proof `external_id_resolution`) whose cloud copy holds the same NorTaxa tuple as `external_unresolved` is re-marked dirty (`local_only_fields=['taxon_identity']`) whenever a pull reconciles it after a cloud change. The next push is refused by the cloud release (expected), finds the tuple already in the cloud, writes nothing and marks it synced, so it converges (observation 701, 2026-10-19). Treat a local NorTaxa-bound identity and the matching cloud `external_unresolved` tuple as in sync in `_classify_identity_sync_change` to avoid the extra push.

- Run live cloud-lock QA with two disposable Sporely Cloud accounts; verify account-mismatch blocking and the Reset Cloud Link flow.
- Verify Profile parity between desktop and web for `username`, `display_name`, `bio`, `avatar_url`, and `profile_email`.
- Review E2 anchor-promotion residual risks before changing that pipeline: cross-device reservation adoption, dangling reserved keys, and URL encoding assumptions.
- Decide whether to repair the known `G_conflicting_intent` row and historical duplicate observation/image records outside the extraction plan.
- Harden standalone migration tooling if it becomes necessary; it is explicitly outside the cloud-sync extraction plan.
- Plan the cloud-sync orchestration implementation (one owner for observation completion; `synced` and the baseline written only after required work succeeds) from the decisions in `docs/cloud-sync-orchestration-design.md`. The extraction plan is complete (`docs/plans/completed/2026-10-07-cloud-sync-extraction-and-orchestration.md`); an unmerged draft is on `origin/plan/cloud-sync-orchestration`.
- Design a safe fix for the observation-deletion ordering and original-media coverage noted in `docs/cloud-media-incident-audit.md`.
- Reference attach/detach should mark the observation as having unsynced changes via a lightweight row-level pending state (not full `dirty`: no image re-upload, prep, mosaic rebuild or measurement re-push). Clears only when the reference-use outbox work succeeds. Also consider telling the user that edits saved mid-sync upload next sync. Removed from the cloud-sync refactor plan (old 6.5k, 2026-10-03); plan it after the refactor's Stage 4 coordinator lands.
- Spore mosaic after conflict resolution: the mosaic is skipped when local and cloud differ in any way the user accepted. Narrow that so only microscope-relevant differences block it.
- Spore mosaic after conflict resolution: build the mosaic even when local and cloud differ, using only measurements whose images match on both sides.
- Spore mosaic after conflict resolution: generate a mosaic for observations that only came in by import (cloud pull), not just ones pushed from this device.
- Spore mosaic size: the mosaic is encoded once as WebP (quality 82) with every spore as a tile, with no size loop and no tile cap. Large mosaics exceed the plan cap: on 2026-10-17, obs 11 (302 tiles of 262×320) failed with "Image too large for plan", and the failure was not listed among the sync issues. Make the mosaic fit the plan cap the way the web app does: step down tile resolution and/or quality until it fits, and consider capping the tile count. Also report the failure as a sync issue. The alternative, raising the worker cap for mosaic keys, costs storage, and every worker deploy now needs the brake checks. Seen again on 2026-11-02 (obs 11, 302 tiles, 303 measurements); the failure was still not listed among the sync issues.
- Local images linked to cloud images that were deleted in the cloud (seen 2026-11-02, desktop on the PR #53 branch): every sync reports `image push failures: anchor: local_image=N: ChildAdoptionConflictError: image N: linked cloud image M was deleted in the cloud; never resurrected` for obs 421 (image 1128 → 1985), obs 393 (983 → 1757, 987 → 1761) and obs 389 (871, 872, 876, 877, 881 → 4367, 4368, 4371, 4372, 4375). The log also warns that the cloud images are skipped because of local tombstones. These observations never converge: 393 and 421 stay dirty, and every pull re-marks 389 dirty (`local_only_fields=['taxon_identity']`, `local_media_changed`), even after its identity push. For obs 389 the five local images also have no source file (`missing_source_file`), and image prep logs "Kunne ikke klargjøre bilde 2/3/6/7/11 for skyopplasting". Decide what the desktop should do with a local image row whose cloud copy was deleted and whose sync key is tombstoned: tombstone or unlink it locally, or route it to conflict review (compare the `ChildAdoptionConflictError` review item above). Then check that 389's identity baseline is stored once its image push no longer fails.
- Obs 704: the first sync on 2026-11-02 reported "skipped cloud image 4850 because it has a local tombstone" as a sync issue. A local tombstone for a cloud image that still exists should be resolved, either by deleting the cloud row through the normal path or by dropping the tombstone, not reported again on every full pull.
- Follow-ups from the first 0.9.26 syncs (2026-10-17):
  - Obs 604, image 3258: the cloud row 7752 (created by legacy-recovery as a "possible duplicate") carries 3258's sync key, but differs in `scale_microns_per_pixel`, which gives a stable `ChildAdoptionConflictError`. Decide the correct scale in conflict review, then clean up the duplicate cloud row. Related: when a local image matches a soft-deleted cloud image (obs 604, local 3258, cloud 3694), decide when recovery restores the existing cloud identity versus preserving the deletion and reporting a conflict; never silently create a duplicate.
  - Obs 372 (image PATCHes interrupted by the legacy `0520afd` sync on 2026-10-16): make a trivial edit to mark it dirty, sync, and check whether image prep reports `changed_keys` (the backfill edits are then pushed) or none (the cloud is already complete).
  - Storage clean-up from the pre-upgrade reconciliation report: C = 10 images marked not uploaded that still hold cloud bytes, and F = 84 soft-deleted cloud rows whose bytes remain. Separate decision; nothing is deleted automatically.

Source: former `PLAN.md`, the active cloud-sync extraction plan, the cloud-media incident audit, and the deferred items from `completed/2026-09-07-conflict-resolution-spore-mosaic.md`.

## Images / galleries / publishing

- Fix missing scale bars on microscope images published to iNaturalist/Artsobservasjoner.
- Define HEIC behavior: HEIC as import source, JPEG/PNG as local working form, and cloud derivatives from the best decoded pixels when practical.
- Replace remaining generated-media heuristics with explicit provenance tags in a dedicated artifact-model plan.

Source: former `PLAN.md`.

- Owner images folder (2026-10-25): 4,240 JPEGs (16.8 GB) are not referenced by any `images` row. They were mostly written July–September 2026, and full backup skips them. Produce a read-only list before any cleanup. Working JPEGs are full 20 MP (about 8.4 MB each; `resize_to_optimal_sampling` is off), so a full backup is about 39 GB. RAW originals (82 GB of ORF) are already excluded.

## UI / usability

UI items now live in the screen backlog of `docs/plans/active/2026-11-08-desktop-ui-redesign.md` (moved 2026-11-08). Add new UI observations here; they are folded into that plan per screen.

## Taxonomy
- Swedish vernacular names are missing from the shipped bundle `tax-2026.10.07-01` (desktop since 0.9.26), so the Swedish UI shows only the scientific name for those species. `tests/test_taxonomy_release_probes.py` fails 4 probes: Kantarell, Trattkantarell and Stjärnrödhätting are absent, and *Conocybe vexans* lists no Kraghätting. The probes were written for `tax-2026.09.30-01`, which had these names. Restore them in the next taxonomy release; not a blocker for 0.9.28 (owner, 2026-10-19).
  - Status in `tax-2026.10.23-11` (bundled 2026-10-25): *Cantharellus cibarius* now has Swedish `kantarell` (COL), and `trattkantarell` and `stjärnrödhätting` are present. All are stored in lower case; the observation list capitalises them. The probes now compare stored names case-insensitively (owner, 2026-10-25). Remaining: *Conocybe vexans* (617026) has no Swedish name; its Kraghätting label probe is a strict xfail until a later release adds Kraghätting from a verified source.
- Identity backfill follow-ups (plan closed 2026-10-19, see `completed/2026-10-18-identity-backfill.md`):
  - Observation 701 (*Russula queletii* coll.): bind its NorTaxa identity (group `nortaxa_self_unique_name`) once the image-create/link fix is in the build used for syncing. Its unlinked microscope image 5042 carries 3 `other` measurements; the sync will create a metadata-only owner-sync anchor and push them. Keep both; fresh dry run, owner GO, preview and authorized sync as for the other batches.
  - Deferred by owner decision: 419 (copied Artsorakel selection unconfirmed) and 183 `same_name_ai_history_only` rows. Unresolved without new evidence: 147 no identity evidence, 87 unresolved external IDs, 24 cloud-selected unverified, 16 AI suggestions not adopted, 5 identifier/name mismatches, 3 numeric collisions.
  - Observations 382, 495 and 580 have no name locally or in the cloud; the only candidate is an unaccepted low-probability AI pick (`restore_lost_names` in the audit). Left unchanged by owner rule.

- Owner observations checked read-only against `tax-2026.10.23-11` (2026-10-25):
  - 997 (*Russula aeruginea*) is bound to COL 61673, which has no NorTaxa or Artsobservasjoner id. NorTaxa concept 626822 carries them. Re-pick it in the Scientific dropdown.
  - 742, 775 and 917 hold a `legacy_unverified` `sporely_taxon_id` that points at an unrelated concept in -11: 56092 *Puccinia caricis-rhizopodae*, 55311 *Psilocybe mairei* and 52369 *Prospodium peruvianum*. The cloud gate never pushes these numbers. Check that nothing displays them (Red List badge, links), then re-pick to replace them.
  - 17 of 18 `external_unresolved` NorTaxa tuples now resolve to exactly one concept, for example 1036 → 625761, 1017 and 1035 → 153280, and 1019, 1021, 1024 and 1034 → 158335. They bind only on an explicit pick or an offline repair run. 989 (NorTaxa 48041, *Ursus maritimus*) correctly stays unresolved.
  - The Scientific picker lists the 2,618 same-name species pairs in -11 as identical rows: a COL concept without a NorTaxa link next to a NorTaxa concept with Artsdatabanken, Artportalen and iNaturalist ids. Examples are *Panaeolina foenisecii* 40673/624659 and *Russula aeruginea* 61673/626822. Picking the COL row loses the NorTaxa id. Check whether the row labels tell them apart. Related to the Group B item below.
  - `TaxonLookupService.resolve_scientific`, which is display-only (vernacular and family), has no rank filter: "Cantharellus cibarius" resolves to var. *monstrosus* (150931). Identity binding is unaffected.

- Reconcile NorTaxa/COL duplicate candidates (Taxonomy v3 Group B): 7,338 NorTaxa concepts in 7,423 same-name pairs with a separate COL concept in `tax-2026.09.26-02`; the COL side is in the cloud scope for 2,132 pairs, the NorTaxa side never. Names and ids sit on the NorTaxa concept, so web users find no common name for species such as Cantharellus cibarius. Name equality only sizes this; identity needs reviewed evidence. Counts: `database/taxonomy/evidence/taxonomy-v3/stage0/coverage-report.json`.
- Group-A bridge coverage (Taxonomy v3): 19,807 NorTaxa ids on COL concepts only through automatic exact matches, so production cannot resolve them (7,099 in the cloud scope: 1,888 `shared_synonymy`, 5,211 without a published cross-reference). Review and emit approved bridges from the Stage 0 candidate manifests. Desktop mitigation since 0.9.29 (2026-10-30): when such an id and the entered name agree on one concept, Edit Observation offers Link, which binds as a name pick (`taxonomy_v2_artifact` proof), never as provider evidence; Copy to taxonomy still stores the id unresolved. Example: *Flammulina fennae*, NBIC:54697 → COL 98271 in `tax-2026.10.23-11`.
- National scientific names (Taxonomy v3): show the approved NorTaxa (later Dyntaxa) preferred scientific name alongside canonical COL; search accepts both; identity never changes.
- Historical unresolved-observation repair (Taxonomy v3): after new Group-A bridges are active, idempotently re-resolve cloud `external_unresolved` observations from preserved source + namespace + external id.
- Review the 448 Artportalen overlay cases left out of `tax-2026.09.26-02` (`database/taxonomy/evidence/artportalen-overlay/`).
- Decide whether the cloud should carry Artportalen/iNaturalist publishing ids as a namespaced channel; today the scoped export suppresses the legacy integer channel.
- Consider Dyntaxa as a national source so Swedish names and Artportalen ids stop depending on the frozen legacy database.
- Decide whether `TaxonChoice` should expose external IDs directly or through a richer match object.
- Add list-returning iNaturalist and Artportalen ID lookup APIs; both identifier types can map to multiple local concepts.
- Define accepted-backbone versus Artportalen-only tie-breaking for duplicate scientific names.
- Decide normalization rules for exact vernacular matching.
- Add an on-demand Artsdatabanken red-list resolver and caching policy.
- Verify AI Photo ID uses a local iNaturalist ID before name matching and that desktop/web apply compatible lookup rules.
- Local taxonomy review workbench for the quarterly refresh (owner idea, 2026-10-23; not approved for implementation). Workflow:
  1. Run `prepare_refresh.py`.
  2. Open a simple local HTML page built from `change-report.json`. It lists taxonomic changes and unresolved identity relationships grouped by taxonomic decision, not compiler error: one accepted-name change can cover several synonym IDs.
  3. Pick a case, run an external Claude/Codex research agent on it, and import its result. The agent compares old and new NorTaxa/COL/Dyntaxa records and says which treatment each source follows and since when. It also finds relevant publications, with reference, DOI/URL, year and evidence, and marks literature evidence separately from inference, plus any unresolved disagreement.
  4. The owner chooses: accept the new relationship, retain the existing one, or hold it. The agent never approves anything.
  5. Decisions go into versioned files that the existing compiler and approval mechanisms read, extending `policies/source_continuity_holds.json` and the reviewed mapping/supersession records.
  6. A later refresh recognises reviewed cases and reopens one only when its underlying evidence changes.

  No web service, new database or autonomous taxonomy authority; direct agent integration can come later. First real cases: the 16 NorTaxa 1.292 identity conflicts (renames, lumps, species-to-"coll.") held in the disposable R2 build.
- `tax-2026.10.31-01` derivation evidence archive (`08e7ecac…`, in the frozen set under `<releases>/tax-2026.10.31-01/frozen/`) has no durable R2 copy or `evidence-location.json` yet (as for `-11`, see `733ffb5`). The destination, checks and steps are in `database/taxonomy/evidence/name-associations/tax-2026.10.31-01/evidence-upload-plan.md` (prechecks passed 2026-11-05). The upload is a production write and needs its own approval.
- Derived-release freeze verification hardening (Codex review of PR #53, 2026-11-04; low severity, deferred by owner decision). `promote()` only accepts the owner-reviewed `freeze.json` hash, so none of these lets a tampered set be promoted. They matter only if every fingerprint in a rewritten frozen set is regenerated: `verify_frozen` alone would then accept it. Fix only if a release-safety problem is shown, because any change means re-freezing and a new descriptor hash. In `database/taxonomy/scripts/promote_desktop_bundle.py`:
  - `_verify_derivation_evidence` checks `derivation/associations.jsonl` only by line count. Compare its association ids with the SQLite association table.
  - It never reads the archived `derivation/manifest.json`. Require the published manifest to equal it, apart from the keys the freeze adds (`freeze_artifact`, `derivation_evidence`, the extra `name_association` hashes), so a changed `redlist_no` count is caught.
  - `freeze_derived` checks the base's manifest and gzip against the base `freeze.json`, but not the base evidence archive. Check that archive's hash against the base freeze entry too.
  - Not a defect: a derived release cannot be rebuilt from its published files alone. The normalized inputs and the base gzip are pinned by hash, not archived, the same as compiler-built releases.

Source: former `PLAN.md` and the legacy-database lookup audit `docs/taxonomy-lookup-status.md` (removed 2026-09-26; see Git history).

## AI identification / crop

- Verify the current Supabase AI-crop fields and desktop/web crop sync.
- Verify Artsorakel/iNaturalist result persistence and dropdown behavior.
- Verify Review, Import Review, and Find Detail share one AI Photo ID state model.
- Confirm AI crop is used only for AI requests, not gallery display or R2 originals.
- Prevent replay of stale or tombstoned-image AI runs as current suggestions.
- Define retention before production: remove stale runs after 30 days or retain at most 2–3 stale rows per observation/service; long term, prefer one current row plus short-lived debug history.

Do not crop R2 originals, make gallery display depend on AI crop, or add a separate crop table unless the current model fails.

Source: former `PLAN.md`.

## Reference data / community data

- Return QC metadata in community-data RPC responses.
- Complete the public reference dataset model before broadly publishing comparison plots.

The reference-library plan is complete (`docs/plans/completed/2026-08-05-reference-library-and-public-plots.md`); revisit its deferred items before planning new reference work.

## Testing / infrastructure
- `tests/test_render_review_screenshots.py::test_default_renderer_emits_central_manifest_and_all_established_states` times out at its 120 s limit (the full renderer takes several minutes) and the file is excluded from CI. Tracked as redesign prep P3. The other failures noted 2026-10-19 (registry tests, the Live Lab keyboard test, taxonomy probes) pass as of 2026-11-08; one probe is an expected failure.
- `pytest-qt` (4.5.0) is pinned in CI but in no requirements file; a fresh `.venv` errors on the missing `qapp` fixture. Tracked as redesign prep P6.

- Add export coverage for observations, images, measurements, calibrations, reference data, and image files; verify `app_settings.json` and full profile state are excluded.
- Introduce Ruff.
- Consider mypy after the codebase has stable, useful annotations.
- Broaden focused coverage for cloud conflicts, local media signatures, image crop math, `utils/r2_storage.py`, SQLite migrations, and `database/models.py`.
- Test metadata auto-merge and true conflict-dialog triggers.
- Reconcile old “cloud deletion conflict” tests with tombstone behavior.
- Add direct coverage for deterministic pagination ordering, metadata-only anchor byte-fetch prevention, broader pull-only zero-write surfaces, affirmative measurement/calibration identity repair, cross-restart retryability, and snapshot schema compatibility.

- Full-backup settings classification (fixed in #35 and #37): the database-key AST guard only scans literal keys in top-level `utils/*.py`. Dynamic per-id keys such as `cloud_location_precision_confirmed:<id>` are invisible to it, and so is any old key left in a profile. The gaps were found by a key-names-only audit of the owner's profile. Consider a supported audit command that reports unclassified setting names, so the next gap is not found by a failed backup.

Source: former `PLAN.md` and the active cloud-sync extraction plan.

## Hardware / ingestion

- Design the field-device temporal anchor for DSLR/phone batches.
- Add Artsobservasjoner support for per-image note upload.

Source: `docs/hardware-sync.md`.

## Web / infrastructure

- Add an offline queue for upload failures in field conditions.
- Consider a cloud summary RPC/view for observation/image change summaries.
- Future web-native analysis ideas: responsive Plotly L × W plots, thumbnail-linked outlier review, mobile/desktop layouts, a public dataset explorer, taxon summaries, literature reference entry, browser measurement, and possible Pyodide sharing of Python/Numpy logic.
- Privacy/social follow-ups: verify live owner/friend/stranger/blocked/banned RLS and feed behavior, strip GPS EXIF from public media, add an iNaturalist export deep link, and generate Bluesky share cards.

Source: former `PLAN.md`.

## Unsorted / needs review

- Confirm whether Worker secrets/routes and `SUPABASE_URL`, JWT overrides, `MEDIA_PUBLIC_BASE_URL`, and the `sporely-media` R2 binding are already deployed.
- Re-check whether old R2 migration notes are obsolete after the Supabase baseline reset.
- Re-check whether old Phase 7 SQL notes are obsolete after the Supabase baseline reset.
- Verify whether the earlier AI crop backlog has already been completed before promoting any item.

- Desktop 0.9.30 published 2026-11-05: the draft was rebuilt from `main` after PR #57 (Sporely app icon; release PR #55, compatibility review #56, name associations #53) and published with the CHANGELOG notes as the latest release. The `v0.9.30` tag points at the PR #57 merge, `260d826`. All four installers built, and the published checksums equal the tested draft's.
  - Verification of the macOS Apple Silicon installer, run in a test profile copied from real data with cloud sign-in blocked:
    - the app reports 0.9.30, bundles `tax-2026.10.31-01` and installs it byte-identical (`sqlite_sha256` `ec19acac…`);
    - all 645 observation identities are unchanged;
    - associated pairs show as one species (*Flammulina velutipes*/vintersopp → 98288) in Edit Observation, the main-window fields and the reference picker; *Mycena pearsoniana* stays two rows.
  - `tax-2026.10.31-01` is the active release in production (read-only check, 2026-11-05).
  - Keep until the owner confirms 0.9.30 in their normal profile:
    - the rollback artifacts (the `releases/` frozen sets, the `-11` bundle in `0c54c47`);
    - the `sporely-worktrees/py-name-assoc-smoke` worktree;
    - the test profiles `name-assoc-smoke`, `release-0930-smoke` and `release-0930-smoke2`.
- Housekeeping: merged branches `fix/backup-dynamic-setting-keys`, `fix/backup-cloud-auth-method` and `ci/release-draft-retarget` (local and remote) can be deleted. The `sporely-worktrees/py-taxonomy-audit` worktree holds uncommitted audit outputs (`relationship-pass/author-standards/`, `decision-package-e/`, `v3/NorTaxa-COL-avvik.xlsx` with an Excel lock file); commit or move them before removing it.
- sporely-landing PR #9 (the public "why COL XR" explainer) is still unmerged. R2 has shipped, so it only needs the owner's go-ahead.
- Free disk space on the development Mac dropped from 53 GB to 27 GB during 2026-10-25. No leftover backup or build files were found; cause unknown.

Source: former `PLAN.md`; status could not be established safely from this repository alone.
