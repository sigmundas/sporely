# Sync-key N1, Stage 6 (slice P1d): two-desktop collision, brake regressions, ownership, recovery and GUI verification

Agent verification on the disposable target, 2026-10-15. Plan `2026-10-14-sync-key-identity`, Stage 6, slice P1d (run `2026-10-14-sync-key-identity-ff39f23d-7f95e23f`). This file is the stage's only change after the tested commit.

## Tested commit, target and harness

- **Tested commit `T` = `ea2bd2e65e21d3e66e748790965f3a66ad23cdc9`** (`feature/sync-key-desktop-n1`).
  - Every new-desktop run cited below records this `head` in `g4-evidence/runs/<run>/manifest.json`. Each was admitted through the lineage rule: clean worktree, ancestors `ee6f2af` and `be511bf`, reachable from `feature/sync-key-desktop-n1`.
  - Released-client runs record their own released heads (`17a74e2` v0.8.13, `640282c` v0.9.1, `769338e` v0.9.15, `58934c2` v0.9.18, `6c764bc` v0.9.25, `0520afd`, `ee6f2af`), as those cases require.
  - The appendix lists every final run with its build, status and write counts (`g4-evidence/p1f/analysis/runtable.json`).
- **Changes in this stage, base `79f5a35` → `T`:** five commits reused by cherry-pick from the P1c attempt (`backup/sync-key-p1c-attempt-1`), and two new ones:
  - `7f4c130`: fault points `during_upload` (the process ends while a media PUT is in flight, after its first body chunk) and `between_deletes` (after an observation's image-row DELETE, before the observation DELETE). Like the existing points, they act only when `SPORELY_SYNC_FAULT` names them and are never read from settings or the UI. `tests/test_sync_fault_points.py` (5 tests) shows they are inert when unset; the `during_upload` test uses a real local HTTP server.
  - `3e2eabb`: image adoption compares the sample fields the create actually sent (`'Not_set'` → NULL; a legacy `Spore_print` → `sample_source`). This is the first attempt's fix `6934c2e`, with its unit test.
  - `8d97b00`: the brake's marked block copied into `docs/supabase-sync-contract.md`. The block between the `obsolete-desktop-brake` markers is byte-identical to sporely-web `0bb2782` (both sha256 `d0308d6b…1fba`).
  - `0ce21a3`, `c50c627`: repository scenarios in `tools/sync_harness/scenarios.py` (sha256 `d629df13…8644` at `T`, the value every `repo:` run records). They cover microscope observation, measurement, portable export/import, legacy bundle import, image move, sync with an injected request fault, state probe, and (new in `c50c627`) an observation without photos. Native file dialogs return the scenario's path; the fault wrapper never alters a request or response.
  - `4dfef20`: facade re-export of the new fault names.
  - **`ea2bd2e`: defect found in this stage's G tier and fixed.**
    - A kill right after an image INSERT left the parent observation without `image_key`/`thumb_key` for good. The retry adopted the image without a write, and the parent's media-key PATCH never ran (exploratory runs `p1d-a65-kill`/`p1d-a67-retry` on `c50c627`: #184 kept no cover). The web already repairs the same gap in its queue recovery (`src/sync-queue.js`).
    - The adoption branch now sets the parent's cover keys only when they are missing; an existing cover is never replaced. The new writer method is in `_PULL_ONLY_BLOCKED_CLIENT_METHODS`.
    - Tests: `test_kill_after_the_child_insert_adopts_it_on_retry[observation_images]` now asserts the restored cover (it fails without the fix, checked by disabling the call), and `test_image_adoption_keeps_a_cover_the_observation_already_has` is new.
    - Verified on `T` in p1f-a67-retry. The shared contract says nothing about cover keys, so no contract change.
- **Tests on `T`:** focused sync families, 1226 passed and 1 skipped: fault points, facade, download-only, tombstones, bytes-desired, fast path, dirty-loop, image push identity, portable identity guard, metadata sync, all `test_sync_key_*`, `test_cloud_sync_image_*`, anchor promotion, measurement sync, dirty pending images, contract, `r2_storage`, `cloud_media`, pending-image repair, calibration linkage and sample source. `git diff --check` is clean.
  - Full suite: 5704 passed, 24 failed, 54 errors, 27 skipped. Every failure is in files this stage does not touch (taxonomy, gallery/live-lab widgets, screenshot renderer, AI-ID parity), with environment causes: no `qapp` fixture (pytest-qt not installed), generated taxonomy data and source fixtures missing from this worktree, no worktree-local `.venv`, a newer taxonomy release than the probes expect.
  - No sync test fails.
- **Web pin:** sporely-web `0bb2782` (W1b final candidate).
  - Brake migration `20261015090000_obsolete_desktop_sync_brake.sql`: sha256 `bd256809…a989`, identical to `0bb2782`.
  - Media guard `obsolete-desktop-guard.js`: sha256 `c313551d…c15b`, identical to `0bb2782`. The harness stub worker (`identity-impl/stub-w1b/stub-worker-w1b.mjs`) imports the guard unchanged and enables it exactly when the database brake is installed.
- **Target:** local stack `sporely-g3-verify` (API `127.0.0.1:55421`, stub worker `127.0.0.1:55425`).
  - Catalog snapshot, no row data: `g4-evidence/p1d/target-fingerprint-start.txt`. It shows the three `sync_key` columns and `*_user_sync_key_uidx` indexes, the identity guard (`prosecdef=f`), the write stop (`prosecdef=t`, `search_path=""`, 7 tables, block table empty), 7 `trg_000_sync_brake` triggers, and audit triggers on 40 tables.
  - `g4-evidence/p1f/target-fingerprint-brake-on.txt` is identical to it.
- **Brake-off windows (released builds only, never `T`):** the brake (database half, and therefore the stub's media half) was removed with the coordinator's `identity-impl/stub-w1b/remove-brake.sql` and re-applied from the exact migration file twice:
  - 10:59:24Z–11:10:09Z: exploratory set-up;
  - 15:12:27Z–15:20:21Z (`g4-evidence/p1f/logs-setup-window.txt`): the final campaign's set-up. It produced:
    - the row 14c wrong-pairing state (two v0.9.15 installations, `p1f-14c@g3.test`);
    - the row 22 crashed-old-desktop state (v0.9.25, `p1f-leg@g3.test`);
    - the keyless rows that the brake regressions collide with (v0.9.15, `p1f-old@g3.test`).

  Every case below ran with the brake applied.
- **Accounts** (local sign-up; profile rows via `p1f/setup-users.sql`): `owner@g3.test` (rows 1–13d, 24, G4, web, ab reproducers), `p1f-two` (row 1 with exact local-id overlap), `p1f-old` (rows 14, 14b, 14d, brake regressions, row 15), `p1f-leg` (row 22), `p1f-14c` (row 14c).
- **Harness:** `g4-evidence/tools/run.sh` unchanged, wrapped by `g4-evidence/p1f/case.sh`.
  - The wrapper takes before/after snapshots.
  - An observation-only capture shim (`p1f/capture/sitecustomize.py`, via `PYTHONPATH`) records every request's headers (credentials redacted), status and response body to `p1f/capture/runs/<run>.jsonl`.
  - Read-only analysis: `p1f/{audit,cloudstate,orphans,ownership,oldclients,runtable}.py`.
  - Isolation: `p1f-p00-netprobe` on `T`. Outbound 1.1.1.1:443 was blocked; auth and REST answered 200, the worker 404.
- **Web:** the sporely-web main checkout at `1029975`, whose `src/`, `index.html`, `vite.config.js` and `package.json` equal `0bb2782`. It was served by Vite with headless Chromium, both inside the kernel network sandbox, signed in as `owner@g3.test`.
- **Evidence per run `<run>`:**
  - `g4-evidence/runs/<run>/` (manifest, steps, results, screenshots);
  - `g4-evidence/p1f/logs/<run>.log` (audited DB writes, Kong non-GET requests, stub mutations, identity flags);
  - `g4-evidence/p1f/analysis/<run>.json`;
  - `g4-evidence/p1f/capture/runs/<run>.jsonl`.
- **Exploratory pass:** the `p1d-*` runs on the earlier commit `c50c627` found the defect fixed in `ea2bd2e`. They are not evidence here.

## Matrix rows on `T`

All rows below ran with the brake applied. "Writes" are audited database writes in the run's window (`p1f/logs/<run>.log`). Runs on `owner@g3.test` also record the device heartbeat (`reference_client_devices.last_seen_at`). While the web tab was open, they also record the web's own `record_client_activity` heartbeat (`client_activity_daily`, `profiles.last_client_seen_at`). The desktop's capture shows no such call.

| Row | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 1 | p1f-a04-new, p1f-b03-new; p1f-t01-c1new, p1f-t02-c2new | B inserts; A untouched; no `desktop_id` writes | **A/B:** both created **local 122**. A's became INSERT #218 and B's INSERT #219; neither touched the other's row. **Fresh account `p1f-two`** (C1/C2 never downloaded): both created **local 1**. C1's became INSERT #226 and C2's INSERT #227; #226 was untouched. In every run there were 0 identity flags, and no request body carried `desktop_id`. Every Data API and media request carried `x-sporely-sync-identity: 2` and `x-sporely-client-version: 0.9.25` (capture `p1f/capture/runs/p1f-t02-c2new.jsonl`); only the auth login did not |
| 2 | p1f-a05…a07-pull, p1f-b04…b06-pull; p1f-t03…t08 | no identity writes, no `updated_at` echo | each of the 12 alternating pulls wrote only the device heartbeat |
| 3 | p1f-a08-edit, p1f-b07-pull; p1f-t09-c1edit, p1f-t10-c2pull | only B's linked row changes | A's edit wrote #218 and its image #202 only; B's pull wrote nothing but the heartbeat. Same for C1 (#226) and C2 |
| 4 | p1f-a12-kedit, p1f-b08-cedit, p1f-a13-keep, p1f-b09-pull | keep-device: only the pair changes | the conflict dialog showed #219 (`gps_latitude`); choosing "local" wrote `set_observation_location` on #219 only (62.3333/9.3333, `manual/acquired`) |
| 5 | p1f-a14-tedit, p1f-b10-cedit, p1f-a15-take, p1f-b11-pull | take-cloud: only the pair changes | conflict on #16; choosing "cloud" made no cloud write (heartbeat only); A's local row took the cloud value |
| 6 obs | p1f-a60-new, p1f-a61-kill (exit 86), p1f-a62-restart, p1f-a63-retry | 23505 → adopt; one row | killed after INSERT #243 (`SPORELY_SYNC_FAULT=after_insert:observations`); restart: 0 writes; retry: POST 409 → "obs 140: create already landed; adopted cloud 243 by sync key (no PATCH)"; one row |
| 6 image | p1f-a64-new, p1f-a65-kill, p1f-a66-restart, p1f-a67-retry | same | killed after image INSERT #231; retry: POST 409 → adopted 231 (no PATCH of the image). The bytes went to the reserved key `…/245/0_044898fc…`. The parent's missing cover was restored with one PATCH of #245 `image_key`/`thumb_key` (the defect fixed in this stage) |
| 6 measurement | p1f-a68-micro, p1f-a69/a69b-measure, p1f-a70-kill, p1f-a71-restart, p1f-a72-retry | same | killed after INSERT #78; retry: adopted 78; one row |
| 7 | p1f-a73-micro, p1f-a74-measure, p1f-a75-timeout, p1f-a75b-restart, p1f-a75c-sync | adopt, no duplicate | the three 201 answers (observation, image, measurement) were discarded as read timeouts. The same sync's retries adopted #247, #234 and #80: one row each. Restart and next sync: heartbeat only |
| 8 | p1f-a76-new, p1f-a77-kill, p1f-a78-restart, p1f-a79-retry | single insert on retry | killed before the request (`before_request:observations`): 0 writes; retry: INSERT #249 once |
| 9 in flight | p1f-a80-new, p1f-a81-kill, p1f-a82-restart, p1f-a83-retry | reserved key reused, one object | `during_upload:observation_images` ended the process while the PUT of `…/250/0_7c5e6e6c…` was in flight. The stub logged the request (18:55:54Z) but stored nothing. Retry: adopted #237 and stored that same key once, plus its thumb |
| 9 after upload | p1f-a84-new, p1f-a85-kill, p1f-a86-restart, p1f-a87-retry | same | `after_upload:observation_images`: bytes stored at `…/251/0_61e8b7d3…`, then killed. Retry: adopted #239 and re-PUT the same key: one object |
| 10 | p1f-a88-new, p1f-a89-connerr, p1f-a90-restart, p1f-a91-retry | deferred; parent first | 20 connection errors on the observation POST: 0 writes and no image POST; retry: INSERT #252, then image #241 |
| 11 | p1f-a62, a66, a71, a75b, a78, a82, a86, a90 (restart probes) | same as 6–10 | each crash was followed by a restart (0 writes), then the retry above |
| 12 portable | p1f-b12-micro, p1f-b13-measure, p1f-a33-pull, p1f-a34-export, p1f-b14-import | fresh keys, new rows, no shared `cloud_id` | B's measured observation #229 (image #214, measurements #67/#68) was exported by A and imported into B: new #230, image #215, measurements #69/#70. Fresh keys, `desktop_id` NULL |
| 12 bundle | p1f-e01-micro, p1f-e02-measure, `p1f/archives/p1f-e.zip`, p1f-b15-bundle, p1f-e03-sync, p1f-e03b-catchup | same | E's measured observation was packaged with the app's exporter (there is no GUI export) and imported into B: new #231/#216/#71. E's own rows became #232/#217/#72; E's local 1 left the legacy `desktop_id` 1 row #2 untouched. All keys distinct |
| 13 | p1f-a40-new, copy `p1f-A13`, p1f-a41-sync, p1f-c13-adopt | 23505 → adopt silently | A uploaded #233/#218; the unchanged copy's POSTs got 409 → "adopted cloud 233 / 218 by sync key (no PATCH)"; heartbeat only |
| 13b | copy `p1f-A13b`, p1f-a42-edit, p1f-c13b-conflict | adoption raises a conflict; no PATCH | A changed #233's open comment; the copy's adoption found different content: "conflict push blocked: obs=132 categories=['sync_key_adoption'] action=review_required"; heartbeat only |
| 13c | p1f-a45-new (two photos), copy `p1f-A13c`, p1f-a46-sync, p1f-a47-move, p1f-c13c-tomb, p1f-c13c-again | deletion/restore flow; no resurrection | the move tombstoned the second cloud image #221 (`deleted_at`). The copy adopted #237 and #220. Its second image: "its cloud copy 221 was deleted in the cloud; never recreated", with no INSERT. On its second sync the copy re-sent #221's unchanged `deleted_at` and refreshed metadata of its own adopted #220 by verified id. #221 is still deleted |
| 13c-p | copy `p1f-A13cp`, p1f-a43-delete, p1f-a44-sync, p1f-c13cp-deleted, p1f-c13cp-again | accepted limitation: re-uploaded once as a new row | after A hard-deleted #233, the unlinked copy re-uploaded it once as #236. No other row was written, and its next sync wrote nothing |
| 13d | fixture `p1f/fault-constraint.sql`; p1f-a51-obs-fk, a52-obs-uq, a53-img-fk, a54-img-uq, a57-meas-fk, a58-meas-uq, a59-after-fixture | plain error, not adoption | A disposable trigger answered INSERTs exactly as the constraints would; no client can reach these answers on this schema. Each answer was a plain error and the row stayed dirty: `23503` on `observations_user_id_fkey`, `observation_images_observation_id_fkey` and `spore_measurements_image_id_fkey`; `23505` on `observations_desktop_id_user_unique`, `observation_images_desktop_id_user_unique` and `idx_spore_measurements_desktop_user`. After the fixture was dropped, the pending rows uploaded (measurement #76) |

**Orphan objects (stub store, `owner@g3.test`):** 0 of 260 at the start, 0 of 301 before the crash cases, 0 of 321 after them (`p1f/analysis/orphans-p1f-{start,before-crash,after-crash}.json`).

## Outstanding desktop GUI scenarios from the local-key check (on `T`)

- **Measurement creation** (p1f-a30-micro, p1f-a31-measure; also b12/b13, e01/e02, a55/a56, a68/a69, a73/a74). Rectangle clicks in the Measure tab on a 100X microscope image created two keyed measurements, uploaded as #65/#66 under image #212 and observation #228.
- **Bundle import** (p1f-b15-bundle) and **portable import** (p1f-b14-import): see row 12.
- **Image move** (p1f-a32-move, gallery context menu "Move to observation"). The r03 observation's image moved to the r04b observation (local 124 → 128). The old cloud image #204 was tombstoned (`deleted_at`). A new row #213 with a fresh key was created under #224.
- **Observation without photos** (p1f-d03-nophoto, new scenario `new_observation_without_images`). Created through "+ New" with an empty Prepare Images, and uploaded as a keyed row; it is the photo-less target of the brake regressions.

## Legacy recovery, inherited pairing and the exact reproducers (on `T`)

| Row / case | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 22 | set-up (brake off, released v0.9.25): p1f-l01…l05, fixtures `p1f-l04-fixture`, `p1f-l06-fixture`; upgrade: p1f-l10-upgrade, p1f-l11-noop | equal content → linked, no cloud write, no duplicate; different content → new row plus "possible duplicate" report; nothing PATCHed | L uploaded #215/#214/#213 (`desktop_id` 1–3) with images #199/#198/#197 and measurement #64. `legacy_fixture.py` then unlinked observations 1 and 2 (with images) and the measurement, and changed observation 2's notes. On `T`: "obs 1 linked to cloud 215 (no cloud write)", "image 1 linked to cloud 199", "measurement 1 linked to cloud 64". "obs 2 not linked (matches=1); uploading as a new row, possible duplicate of an earlier upload": new #255/#244. #213–#215 received no row write. **One media write on a linked row:** the upgrade re-PUT image 1's bytes to #199's own existing storage key `…/215/0_ae05e73e…` (see "Findings", F4). No-op sync: heartbeat only |
| 22, another device's colliding rows | p1f-l07/l08 (v0.9.25 L2, never downloaded, locals 1 and 2), p1f-l20-upgrade, p1f-l21-noop | new rows plus report; nothing PATCHed | "obs 1 / obs 2 not linked (matches=1) … possible duplicate": new #256/#257 with images #245/#246. L's #213–#215 were untouched. The no-op recomputed one spore summary (#66 → #84, non-identity) |
| 14c | set-up (brake off): p1f-c01…c07 (two v0.9.15 installations). P2's pull paired its locals 1/2 with P1's #210/#211 and overwrote them (the pre-brake defect, p1f-c06-p2sync); P2 then kept unsynced local 4. Upgrade of a copy to `T`: p1f-x14c-1-sync, p1f-x14c-3-del, p1f-x14c-4-sync, p1f-x14c-5-restart, p1f-x14c-6-edit, p1f-x14c-7-restart | accepted limitation for inherited keyless links; still required: no unsynced local work lost, and nothing written to any row other than the paired observation and its own children | **First sync:** uploaded the unsynced local 4 as new keyed #258/#247, with no write to #210–#212. **Delete of local 2** (paired with P1's #211): deleted #211, its image #195 and its 4 media objects. **Edit of local 1** (paired with P1's #210; p1f-x14c-6-edit): PATCHed #210 and its image #194, and re-PUT P2's photo to #194's existing key `…/210/0_1792077161000.webp` (the wrong record's media). P2's own #212 was never written. Locals 1, 3 and 4 were kept after sync and restart. This matches the limitation recorded after the P1c failure (gate `56491780ae7348acabd6b9dbf4e709f2`; runs `p6-x14c-2-edit`, `p6-x14c-3-del`, `p6-c06-p2sync`). p1f-x14c-2-edit is invalid: the script named the exploratory pair (cloud 149), the run failed with "no local observation for cloud id 149" and wrote nothing. The edit was re-run as p1f-x14c-6-edit, after the delete |
| ab15 | copy of profile `cand` (the original ab15 profile), p1f-r15-ab15 | zero overwrites, zero `desktop_id` writes | new local 27; cloud #28 holds `desktop_id` 27. INSERT #259/#248, keyed; #28 untouched (last change 2026-10-14) |
| ab19 | copy of `candAB2`, p1f-r19-ab19 | same | new local 27 → INSERT #260/#249; #28 untouched |
| ab17 | copy of `legacy` (the original ab17 profile, last used by released `0520afd`), run with the new desktop at `T`: p1f-r17-ab17 | same | new local 28; cloud #31 holds `desktop_id` 28. INSERT #261/#250; #31 untouched |

The three reproducer runs record "FAILED" only because the driver's 600 s wait for the sync ended while the profile was still pulling its older backlog. The upload had finished first. The released `0520afd` baseline of the defect is the original ab17 run of 2026-10-13 (`g4-evidence/runs/ab17-legacy-new-photo`); with the brake applied, a released build can no longer reproduce it.

**Inherited legacy links without a sync key are trusted as they are. If an old version paired a local observation with another installation's cloud record, edits and deletes on the upgraded desktop can still affect that wrong cloud record until the link is corrected.**

## Old clients under the brake, brake regressions, row 14d (on `T` and released builds)

Account `p1f-old@g3.test` holds:
- the keyless rows of released v0.9.15 installation S (#216/#217, `desktop_id` 1/2, made in the brake-off set-up window);
- the new desktop D's keyed rows, made on `T`: #262 and #263 with images, and the photo-less #264 (p1f-d01…d04).

Each released build then ran, on its own fresh profile:
1. two local observations, unsynced (`1new`, `2new`; local ids 1 and 2 collide with S's `desktop_id` 1/2);
2. a sync (`3sync`);
3. an edit of D's keyed #262 (`4edit`);
4. a delete of the local row its pull had paired with S's #216 (`5delpair`, the earlier wrong-target delete);
5. a delete of D's keyed #263 (`6delkeyed`, the earlier ghost row);
6. a delete of the photo-less keyed #264 (`7delnophoto`);
7. a sync (`8sync`) and a restart without a new login (`9restart`).

Every response is in `p1f/capture/runs/p1f-o-<build>-<step>.jsonl` (headers, status, body); the per-run summary is `p1f/analysis/oldclients.json`.

| Case | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 14 | p1f-o-{v0.8.13, v0.9.1, v0.9.15, v0.9.18, v0.9.25, 0520afd, ee6f2af}-{1new…9restart} (63 runs) | every row write refused `P0001`/`sporely_upgrade_required`, every media PUT/DELETE 426; zero cloud changes; downloads work; no sign-in prompt; credentials and unsynced local work kept | **Cloud:** unchanged. The account's digest stayed `c32d258164a5` from before the first client to after the last (`p1f/analysis/cloudstate-old-*.json`); the only audited writes were the released builds' own device records (`reference_client_devices`). **Refusals:** every row write answered 400 `sporely_upgrade_required` (POST, PATCH and DELETE on observations and images, plus `set_observation_location`), and every media PUT (8) and DELETE (52) answered 426 with `X-Sporely-Error-Code: sporely_upgrade_required`. **Client side:** 0 responses 401/403 and no sign-in dialog; the restart without `--login` still synced (credentials kept); pulls kept working. **Unsynced local work:** the local rows and their photos were kept, but see finding F1: each build's own pull overwrote the fields of its unsynced local 2 with S's colliding row |
| 14b | same; `oldclients.json` | an old client's link repair onto a new-desktop row is refused; the row keeps a NULL link | 0 `desktop_id` changes in any run; D's keyed #262–#264 still have `desktop_id` NULL |
| wrong-target delete (v0.9.15, `0520afd`; also v0.9.18, v0.9.25, `ee6f2af`, v0.8.13, v0.9.1) | `…-5delpair` | zero cloud changes | each build paired its local 1 with S's #216 and deleted it. Media DELETE → 426, row DELETEs → 400; #216 and its media are intact. Accepted local effect: the row reappears locally on the next sync |
| ghost row (old-desktop delete of a keyed row) | `…-6delkeyed` | zero cloud changes | D's #263 intact for every build; it reappears locally on the next sync |
| photo-less delete (database DELETE reached) | `…-7delnophoto` | refused | `DELETE observation_images?observation_id=eq.264` matched no rows (204), then `DELETE observations?id=eq.264` was refused 400 `sporely_upgrade_required`; #264 intact |
| collision, pull and delete on v0.9.18, v0.9.25, `ee6f2af` | as above | zero cloud changes | as above |
| new desktop beside released `ee6f2af` and `0520afd` | p1f-d05-pull, p1f-d06-edit, p1f-d07-pull | released builds refused; the new desktop's rows untouched | D's rows were unchanged by every released build. D's pull wrote only its heartbeat, and its edit wrote only #262 and its image |
| 14d | p1f-d01…d07 (new desktop: create, photo-less create, edit, media PUT); new-desktop deletes with media DELETE on `owner@g3.test` (p1f-a43-delete, p1f-a94-del-kill); `p1f/api14d.py` → `p1f/analysis/api14d-p1f-old.json`; row 23 for the write stop | all allowed under the brake; the write stop still refuses a listed new-desktop version | new desktop: every create/edit/delete and media PUT/DELETE succeeded under the brake. API probe on #263 with a browser User-Agent and an Android-WebView User-Agent (`…; wv) … Mobile Safari`), no identity header: PATCH 204, media PUT 200, DELETE 200. Released-desktop User-Agent `python-requests/2.32.3`: PATCH 400 `sporely_upgrade_required`, PUT 426, DELETE 426. Web app writes under the brake: p1f-w01-web-set (`set_observation_location` 200). The write stop: row 23 |

## Rows 15, 23 and 24 (on `T`)

| Row | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 15 | `sync_key` renamed on the three tables (disposable, `p1f/logs/p1f-k-hide.log`; catalog `p1f/target-fingerprint-row15-hidden.txt`), p1f-k01-dl, p1f-k02-new, p1f-k03-sync; restored (`p1f-k-restore.log`), p1f-k04-after-restore | uploads blocked "cloud not ready"; pull works | Download worked. Create and sync: banner "Sporely Cloud is not ready for this version yet. Uploads are paused; downloads still work.", 0 writes. After the restore, the catalog fingerprint was identical to the brake-on fingerprint, and the pending row uploaded (#275/#262) |
| 23 | p1f-a99-base; `insert … sync_client_write_block ('0.9.25')` (`p1f/logs/p1f-block.log`); p1f-a100-blocked, p1f-a101-blocked-again, p1f-a101b-new-blocked, p1f-w03-web-blocked; `delete …`; p1f-a102-unblocked, p1f-a103-noop | empty table: syncs normally; blocked: every row write from the blocked build refused, banner shown, local data kept, web unaffected; after unblock: pending work uploads | Empty table: normal sync (a99). **Blocked** (`T` sends `x-sporely-client-version: 0.9.25`): PATCH of #218 and POST of a new observation → 400 `P0001` `sporely_upgrade_required`, "This version of Sporely Desktop is blocked from uploading. Install the latest version from sporely.no. Nothing was changed in the cloud." 0 cloud writes in all three runs. The banner "This version of Sporely can no longer upload. Please install the latest version." was shown again after the restart, and the edit and the new row stayed local. The web's `set_observation_location` on #16 → 200. Released desktops are refused by the brake, not the write stop (row 14). **Unblocked:** the pending edit (#218 `open_comment`) and the new row (#274/#261) uploaded; the next no-op wrote only the heartbeat |
| 24 | `p1f/analysis/ownership-p1f-{owner,old,leg,14c}.json`, audit since the first final run (id 2886) | every image under its own observation and every measurement under its own image; storage paths in the owner's prefix; no child re-parented | 14 profiles (A, B, E, the four copies, the three reproducers, D, L, L2, the 14c copy). Every linked image sits under its local parent's cloud row, and every linked measurement under its local parent image's cloud row. All storage paths are in the owner's prefix; 0 re-parenting UPDATEs. The single dangling link (copy A13's image → #218) points at the row A deliberately hard-deleted in the 13c-p case |

**Orphan objects at the end:** 0 of 329 (`owner`), 0 of 8 (`p1f-old`), 0 of 13 (`p1f-leg`), 0 of 6 (`p1f-14c`).

## Photo-location re-check (G4) on `T`

| Case | Runs | Observed |
| --- | --- | --- |
| Download on a fresh profile, then the first write-enabled sync | p1f-a01-dl, p1f-a02-sync, p1f-a02b-catchup, p1f-b01-dl, p1f-b02-sync, p1f-b02b-catchup | Download: the desktop sent only its login (capture); the audited writes in that window were the open web tab's activity heartbeat. First syncs: 0 identity writes. Non-identity echoes: each fresh profile re-sent the unchanged `deleted_at` of the account's four image tombstones (#44, #54, #145, #162: `updated_at` only), and A recomputed four spore summaries once |
| No-op sync | p1f-a03-noop, p1f-a05…a07, p1f-a103-noop | heartbeat only |
| New observation from a geotagged photo | p1f-a04-new | INSERT #218 `image_exif/acquired` |
| Web → desktop → web retention | p1f-w01-web-set, p1f-a23-pull, p1f-a24-unrelated, p1f-w02-web-read | the web set #16 to `manual/acquired` (63.2218…/10.2234…); the desktop pulled it and made an unrelated `open_comment` edit with no location keys in the PATCH; the web read #16 back with the pair retained: `runs/p1f-w02-web-read/web-result.json` shows 63.22182/10.22350, `manual/acquired`, with the desktop's `p1f-a24` comment |
| Unrelated edit | p1f-a08-edit, p1f-a24-unrelated | no location keys written; the pair stays |
| Attributed edit, restart before sync | p1f-a09-attr, p1f-a10-restart, p1f-a11-sync | 0 writes until sync, then `set_observation_location` → #218 `manual/acquired` 61.4444/9.4444 |
| Conflict keep-device / take-cloud | p1f-a13-keep / p1f-a15-take | as rows 4/5 |
| Restart persistence | p1f-a10-restart, p1f-a25-restart | 0 writes |
| Edit Images detour | p1f-a16-detour | Details → Edit Images → Prepare Images → Save. The latitude entered equals #13's stored value, so the observation PATCH carried no location keys. No RPC; #13 stays NULL/NULL |
| Prepare Images typing (re-run, not carried forward) | p1f-a17-r03, a18-r04, a19-r05, a20-r06, a21-r04b, a22-r05b | r03 `manual/acquired` 61.1/9.1; r04 `image_exif/acquired` (restored in Prepare Images); r05 `image_exif/acquired` (restored in the details dialog); r06 (no GPS) `manual/acquired` 59.5/10.5; r04b `image_exif/acquired`; r05b `image_exif/acquired`. All six match `ee6f2af`. r04 and r05 have the same steps as r04b/r05b, each with its own photo (the original r04/r05 were invalid duplicate-file runs) |

`git diff ee6f2af..T` touches `ui/image_import_dialog.py` (+5) and `ui/observations_tab.py`, so the typing set was re-run in full rather than carried forward.

## Findings (not fixed in this stage)

- **F1, released builds overwrite unsynced local work locally when local ids collide** (all seven builds; `p1f-o-<build>-3sync`).
  - Each released build's own pull paired its unsynced local observation 2 with another installation's keyless cloud row #217 (`desktop_id` 2), through its built-in `desktop_id` reverse lookup. It replaced the local fields (latitude 61.671 → 61.143). The local photo was kept, next to the cloud row's photo.
  - The cloud was not changed: the brake refused every write.
  - This is the released clients' own logic and cannot be changed by the new desktop or the brake. The plan's row 14 asks that unsynced local work be kept, and its accepted local effects do not list this one. It needs a decision (an upgrade-notice item, or an accepted limitation) before release.
- **F2, an interrupted new-desktop delete leaves an image-less cloud observation that comes back** (p1f-a92…a97, fault `between_deletes`).
  - The local row and the cloud image row and media were deleted. Observation #253 stayed in the cloud with no images and a cover key that points at deleted media.
  - After restart, the next sync pulled it back as a new local row 150. The unsynced local work (#254) was kept and uploaded.
  - The desktop keeps no durable record of an observation delete in progress, so this predates the plan. A fix needs a pending-delete record, which is outside this stage.
- **F3, row 14c limitation re-confirmed** (see above). Edits and deletes through an inherited keyless wrong pairing reach the other installation's row, its image row and its media. Release preconditions 1 and 3 apply.
- **F4, legacy recovery re-uploads an image's bytes to the matched row's own key** (p1f-l10-upgrade). After linking image 1 to #199 with no row write, the upgrade PUT the photo to #199's existing `storage_path`.
  - This completes the bytes if the old desktop crashed before uploading them; the key and the row are the matched row's own.
  - It is a media write, while the plan's legacy-recovery text says "no cloud write". The reviewer should judge whether "no cloud write" covers media bytes.
- **F5, pre-existing environment and harness notes.**
  - Fresh owner profiles need more than the driver's fixed 600 s wait to pull the account's backlog (p1f-e03-sync and the three reproducers record FAILED for that reason after their uploads). Catch-up pulls were added.
  - The media of cloud rows 3 and 21 is missing from the stub store, so every owner sync reports "2 other issue(s)" (download failed).
  - The partial Download from Cloud on a fresh profile is tracked separately.

## Corrections to the paused candidate's evidence (sparrer findings, 2026-10-14)

1. Row 9 has a fault while the PUT is in flight (`during_upload`, p1f-a81) and one after upload before confirmation (`after_upload`, p1f-a85). `before_request` (p1f-a77) is row 8 only.
2. The full typing set r03, r04, r05, r06, r04b and r05b was re-run on `T` (table above).
3. Row 13d has the constraint-specific `23503` and other-constraint `23505` G cases for all three tables (via the disposable fixture), not `42501`.
4. ab17 ran with the new desktop at `T` (p1f-r17-ab17). Released `0520afd` serves only as baseline (original ab17, 2026-10-13).
5. The Stage 6 preflight timeout of the first attempt (2026-10-14) was a host maintenance DarkWake (19:03:55–19:08:23 local), not a proxy. The harness `run.sh` was restored to its original form (`identity-impl/STATUS.md`, 17:14:01Z correction) and is the unmodified launcher used here.

## Invalid or interrupted runs (kept)

- p1f-x14c-2-edit: the script named the exploratory pair (cloud 149) and the run wrote nothing; re-run as p1f-x14c-6-edit.
- p1f-e03-sync, p1f-r15-ab15, p1f-r19-ab19, p1f-r17-ab17: harness status FAILED because the driver's 600 s sync wait ended during the backlog pull. Their uploads had completed, and the writes cited above are from their audit windows.
- The `p1d-*` runs (exploratory pass on `c50c627`, accounts `p1d-*@g3.test`) are not evidence.

## Appendix: every final-campaign log (`g4-evidence/p1f/analysis/runtable.json`)

Build `T` = `ea2bd2e`; released heads by name; "-" = a fixture, SQL, web or API log without a desktop manifest. Status "EXIT 86" is the product fault hook ending the process as intended. DB writes and stub mutations are counted in each run's audit window.

| Run | Build | Scenario | Status | DB writes | Stub mutations | Identity flags |
| --- | --- | --- | --- | --- | --- | --- |
| p1f-c01-p1new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1f-c02-p1new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1f-c03-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-c04-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-c05-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-c06-p2sync | v0.9.15 | sync_now | OK | 15 | 12 | 4 |
| p1f-c07-p2unsynced | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-l01-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-l02-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-l03-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-l04-fixture | - | - |  | None | None | 0 |
| p1f-l05-sync | v0.9.25 | sync_now | OK | 27 | 14 | 14 |
| p1f-l06-fixture | - | - |  | None | None | 0 |
| p1f-l07-l2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-l08-l2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-s01-new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1f-s02-new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1f-p00-netprobe | T |  |  | None | None | 0 |
| p1f-a01-dl | T | download_from_cloud | OK | 4 | 0 | 0 |
| p1f-a02-sync | T | repo:sync_and_record | OK | 5 | 0 | 0 |
| p1f-a02b-catchup | T | repo:sync_and_record | OK | 13 | 0 | 0 |
| p1f-a03-noop | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-b01-dl | T | download_from_cloud | OK | 0 | 0 | 0 |
| p1f-b02-sync | T | repo:sync_and_record | OK | 5 | 0 | 0 |
| p1f-b02b-catchup | T | repo:sync_and_record | OK | 5 | 0 | 0 |
| p1f-a04-new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-b03-new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-b04-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a06-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-b05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-b06-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a08-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1f-b07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a09-attr | T | edit_observation | OK | 0 | 0 | 0 |
| p1f-a10-restart | T | idle | OK | 0 | 0 | 0 |
| p1f-a11-sync | T | repo:sync_and_record | OK | 4 | 0 | 0 |
| p1f-a12-kedit | T | edit_observation | OK | 0 | 0 | 0 |
| p1f-b08-cedit | T | edit_observation | OK | 7 | 4 | 0 |
| p1f-a13-keep | T | sync_with_conflicts | OK | 3 | 0 | 0 |
| p1f-b09-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a14-tedit | T | edit_observation | OK | 0 | 0 | 0 |
| p1f-b10-cedit | T | edit_observation | OK | 4 | 0 | 0 |
| p1f-a15-take | T | sync_with_conflicts | OK | 1 | 0 | 0 |
| p1f-b11-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a16-detour | T | edit_via_prepare_images | OK | 4 | 0 | 0 |
| p1f-a17-r03 | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a18-r04 | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a19-r05 | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a20-r06 | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a21-r04b | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a22-r05b | T | new_observation | OK | 8 | 4 | 0 |
| p1f-w01-web-set | - | web |  | 4 | 0 | 0 |
| p1f-a23-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a24-unrelated | T | edit_observation | OK | 2 | 0 | 0 |
| p1f-w02-web-read | - | web |  | 2 | 0 | 0 |
| p1f-a25-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-t01-c1new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-t02-c2new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-t03-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t04-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t05-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t06-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t07-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t08-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-t09-c1edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1f-t10-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a30-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-a31-measure | T | repo:measure_new | OK | 17 | 6 | 0 |
| p1f-a32-move | T | repo:move_image | OK | 10 | 8 | 0 |
| p1f-b12-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-b13-measure | T | repo:measure_new | OK | 17 | 6 | 0 |
| p1f-a33-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a34-export | T | repo:export_portable | OK | 0 | 0 | 0 |
| p1f-b14-import | T | repo:import_archive | OK | 13 | 0 | 0 |
| p1f-e01-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-e02-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1f-e-bundle | - | - |  | None | None | 0 |
| p1f-b15-bundle | T | repo:import_archive | OK | 14 | 6 | 0 |
| p1f-e03-sync | T | repo:sync_and_record | FAILED | 13 | 6 | 0 |
| p1f-e03b-catchup | T | repo:sync_and_record | OK | 5 | 0 | 0 |
| p1f-a40-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a41-sync | T | repo:sync_and_record | OK | 12 | 4 | 0 |
| p1f-c13-adopt | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1f-a42-edit | T | edit_observation | OK | 9 | 4 | 0 |
| p1f-c13b-conflict | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a43-delete | T | delete_observation | OK | 3 | 4 | 2 |
| p1f-a44-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-c13cp-deleted | T | repo:sync_and_record | OK | 2 | 0 | 0 |
| p1f-c13cp-again | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a45-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a46-sync | T | repo:sync_and_record | OK | 12 | 8 | 0 |
| p1f-a47-move | T | repo:move_image | OK | 13 | 8 | 0 |
| p1f-c13c-tomb | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1f-c13c-again | T | repo:sync_and_record | OK | 6 | 0 | 0 |
| p1f-a50-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a51-obs-fk | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a52-obs-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a53-img-fk | T | repo:sync_and_record | OK | 2 | 0 | 0 |
| p1f-a54-img-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a55-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-a56-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1f-a57-meas-fk | T | repo:sync_and_record | OK | 16 | 8 | 0 |
| p1f-a58-meas-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-13d-fixture | - | - |  | None | None | 0 |
| p1f-a59-after-fixture | T | repo:sync_and_record | OK | 5 | 2 | 0 |
| p1f-a60-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a61-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 1 | 0 | 0 |
| p1f-a62-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a63-retry | T | repo:sync_and_record | OK | 7 | 4 | 0 |
| p1f-a64-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a65-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 3 | 0 | 0 |
| p1f-a66-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a67-retry | T | repo:sync_and_record | OK | 2 | 4 | 0 |
| p1f-a68-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-a69-measure | T | repo:measure_new | OK | 14 | 6 | 0 |
| p1f-a69b-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1f-a70-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 3 | 0 | 0 |
| p1f-a71-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a72-retry | T | repo:sync_and_record | OK | 5 | 3 | 0 |
| p1f-a73-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1f-a74-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1f-a75-timeout | T | repo:sync_with_fault | OK | 15 | 6 | 0 |
| p1f-a75b-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a75c-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a76-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a77-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 0 | 0 | 0 |
| p1f-a78-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a79-retry | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1f-a80-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a81-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 4 | 1 | 0 |
| p1f-a82-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a83-retry | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1f-a84-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a85-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 4 | 2 | 0 |
| p1f-a86-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a87-retry | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1f-a88-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a89-connerr | T | repo:sync_with_fault | OK | 0 | 0 | 0 |
| p1f-a90-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a91-retry | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1f-a92-new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-a93-unsynced | T | new_observation | OK | 0 | 0 | 0 |
| p1f-a94-del-kill | T | delete_observation | EXIT 86 (product fault hook) | 2 | 4 | 1 |
| p1f-a95-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a96-sync | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1f-a97-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-l10-upgrade | T | repo:sync_and_record | OK | 8 | 8 | 0 |
| p1f-l11-noop | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-l20-upgrade | T | repo:sync_and_record | OK | 15 | 8 | 0 |
| p1f-l21-noop | T | repo:sync_and_record | OK | 3 | 0 | 0 |
| p1f-x14c-1-sync | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1f-x14c-2-edit | T | edit_observation | FAILED | 0 | 0 | 0 |
| p1f-x14c-3-del | T | delete_observation | OK | 3 | 4 | 2 |
| p1f-x14c-4-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-x14c-5-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-r15-ab15 | T | new_observation | FAILED | 7 | 4 | 0 |
| p1f-r19-ab19 | T | new_observation | FAILED | 7 | 4 | 0 |
| p1f-r17-ab17 | T | new_observation | FAILED | 7 | 4 | 0 |
| p1f-d01-new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-d02-new | T | new_observation | OK | 8 | 4 | 0 |
| p1f-d03-nophoto | T | repo:new_observation_without_images | OK | 2 | 0 | 0 |
| p1f-d04-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-o-v0.8.13-1new | v0.8.13 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.8.13-2new | v0.8.13 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.8.13-3sync | v0.8.13 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.8.13-4edit | v0.8.13 | edit_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.8.13-5delpair | v0.8.13 | delete_observation | OK | 0 | 2 | 0 |
| p1f-o-v0.8.13-6delkeyed | v0.8.13 | delete_observation | OK | 0 | 2 | 0 |
| p1f-o-v0.8.13-7delnophoto | v0.8.13 | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.8.13-8sync | v0.8.13 | sync_now | OK | 0 | 4 | 0 |
| p1f-o-v0.8.13-9restart | v0.8.13 | sync_now | OK | 0 | 4 | 0 |
| p1f-o-v0.9.1-1new | v0.9.1 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.1-2new | v0.9.1 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.1-3sync | v0.9.1 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.1-4edit | v0.9.1 | edit_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.1-5delpair | v0.9.1 | delete_observation | OK | 0 | 2 | 0 |
| p1f-o-v0.9.1-6delkeyed | v0.9.1 | delete_observation | OK | 0 | 2 | 0 |
| p1f-o-v0.9.1-7delnophoto | v0.9.1 | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.1-8sync | v0.9.1 | sync_now | OK | 0 | 4 | 0 |
| p1f-o-v0.9.1-9restart | v0.9.1 | sync_now | OK | 0 | 4 | 0 |
| p1f-o-v0.9.15-1new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-3sync | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-4edit | v0.9.15 | edit_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-5delpair | v0.9.15 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.15-6delkeyed | v0.9.15 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.15-7delnophoto | v0.9.15 | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-8sync | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.15-9restart | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-1new | v0.9.18 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-2new | v0.9.18 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-3sync | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-4edit | v0.9.18 | edit_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-5delpair | v0.9.18 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.18-6delkeyed | v0.9.18 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.18-7delnophoto | v0.9.18 | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-8sync | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.18-9restart | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-1new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-3sync | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-4edit | v0.9.25 | edit_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-5delpair | v0.9.25 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.25-6delkeyed | v0.9.25 | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-v0.9.25-7delnophoto | v0.9.25 | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-8sync | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-v0.9.25-9restart | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1f-o-0520afd-1new | 0520afd | new_observation | OK | 0 | 0 | 0 |
| p1f-o-0520afd-2new | 0520afd | new_observation | OK | 0 | 0 | 0 |
| p1f-o-0520afd-3sync | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1f-o-0520afd-4edit | 0520afd | edit_observation | OK | 1 | 0 | 0 |
| p1f-o-0520afd-5delpair | 0520afd | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-0520afd-6delkeyed | 0520afd | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-0520afd-7delnophoto | 0520afd | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-0520afd-8sync | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1f-o-0520afd-9restart | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1f-o-ee6f2af-1new | ee6f2af | new_observation | OK | 0 | 0 | 0 |
| p1f-o-ee6f2af-2new | ee6f2af | new_observation | OK | 0 | 0 | 0 |
| p1f-o-ee6f2af-3sync | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1f-o-ee6f2af-4edit | ee6f2af | edit_observation | OK | 1 | 0 | 0 |
| p1f-o-ee6f2af-5delpair | ee6f2af | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-ee6f2af-6delkeyed | ee6f2af | delete_observation | OK | 0 | 8 | 0 |
| p1f-o-ee6f2af-7delnophoto | ee6f2af | delete_observation | OK | 0 | 0 | 0 |
| p1f-o-ee6f2af-8sync | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1f-o-ee6f2af-9restart | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1f-d05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-d06-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1f-api14d | - | - |  | None | None | 0 |
| p1f-d07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-oldclients-summary | - | - |  | None | None | 0 |
| p1f-x14c-6-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1f-x14c-7-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1f-a99-base | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-a100-blocked | T | edit_observation | OK | 0 | 0 | 0 |
| p1f-a101-blocked-again | T | repo:sync_and_record | OK | 0 | 0 | 0 |
| p1f-a101b-new-blocked | T | new_observation | OK | 0 | 0 | 0 |
| p1f-w03-web-blocked | - | web |  | 4 | 0 | 0 |
| p1f-block | - | - |  | None | None | 0 |
| p1f-a102-unblocked | T | repo:sync_and_record | OK | 9 | 4 | 0 |
| p1f-a103-noop | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1f-k-hide | - | - |  | None | None | 0 |
| p1f-k01-dl | T | download_from_cloud | OK | 0 | 0 | 0 |
| p1f-k02-new | T | new_observation | OK | 0 | 0 | 0 |
| p1f-k03-sync | T | repo:sync_and_record | OK | 0 | 0 | 0 |
| p1f-k-restore | - | - |  | None | None | 0 |
| p1f-k04-after-restore | T | repo:sync_and_record | OK | 8 | 4 | 0 |
