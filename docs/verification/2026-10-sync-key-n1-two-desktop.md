# Sync-key N1, Stage 6 (slice P1d): two-desktop collision, brake regressions, ownership, recovery and GUI verification

Agent verification on the disposable target, 2026-10-15/16. Plan `2026-10-14-sync-key-identity`, Stage 6, slice P1d (run `2026-10-14-sync-key-identity-ff39f23d-7f95e23f`). This file is the stage's only change after the tested commit.

**Status: one required result is not met.** Matrix row 14 fails its "unsynced local work kept" condition for the two-installation collision on every released build (finding F1). The released clients' own pull causes it, and neither the new desktop nor the brake can change it. It needs the plan owner's disposition. Every other required case passes on `T`.

## Tested commit, target and harness

- **Tested commit `T` = `ee6ab4ec840a4f37679210bc4c1895bbb121c94f`** (`feature/sync-key-desktop-n1`).
  - Every new-desktop run cited below records this `head` in `g4-evidence/runs/<run>/manifest.json`. All 156 were admitted through the lineage rule (role `n1-lineage`, reachable from `feature/sync-key-desktop-n1`, clean worktree), with outbound traffic blocked.
  - Released-client runs record their own heads (`17a74e2` v0.8.13, `640282c` v0.9.1, `769338e` v0.9.15, `58934c2` v0.9.18, `6c764bc` v0.9.25, `0520afd`, `ee6f2af`).
  - The appendix lists every run of this campaign (`g4-evidence/p1g/analysis/runtable.json`).
- **Changes in this stage, base `79f5a35` → `T`:**
  - Reused by cherry-pick from the P1c attempt (`backup/sync-key-p1c-attempt-1`):
    - `7f4c130`: fault points `during_upload` (the process ends while a media PUT is in flight, after its first body chunk) and `between_deletes` (after an observation's image-row DELETE, before the observation DELETE). They act only when `SPORELY_SYNC_FAULT` names them and are never read from settings or the UI. `tests/test_sync_fault_points.py` (5 tests) shows them inert when unset; the `during_upload` test uses a real local HTTP server.
    - `3e2eabb`: image adoption compares the sample fields the create actually sent (the first attempt's `6934c2e`).
    - `8d97b00`: the brake's marked block in `docs/supabase-sync-contract.md`, byte-identical to sporely-web `0bb2782` (both sha256 `d0308d6b…1fba`).
    - `0ce21a3`: repository scenarios in `tools/sync_harness/scenarios.py`.
    - `4dfef20`: facade re-exports for the fault points.
  - `c50c627`: scenario `new_observation_without_images` ("+ New" with an empty Prepare Images), the photo-less target of the brake regressions. The scenario file has sha256 `d629df13…8644` at `T`, the value every `repo:` run records.
  - `ea2bd2e`: **defect found in this stage's G tier.** A kill right after an image INSERT left the parent observation without `image_key`/`thumb_key`, because adoption links the image with no write and the parent's media-key PATCH never ran (first campaign, run `p1d-a67-retry`). The adoption now restores a missing cover.
  - `5145af1`: **corrections after review (2026-10-16).**
    1. **Cover repair.** It writes only when the parent has no cover keys at all, with a PATCH conditioned on both columns still being NULL. A partial cover and a cover set concurrently are never overwritten. New tests: a partial cover is kept; a cover set between the read and the PATCH is not overwritten. Both fail on `ea2bd2e`.
    2. **Legacy recovery writes nothing to the matched row, media included** (contract: "no cloud write"; rule 12: identity repair does not upload bytes). `create_child_row` marks a legacy link (`is_legacy_link`). Such a row gets no confirming byte upload (image push and anchor paths) and no parent cover repair; sync-key adoption keeps its confirming upload. The accepted Stage 4 test that expected the bytes on the legacy row's key now requires zero media writes, now and on the next sync. A new field-image test covers the image-push path. Both fail on `ea2bd2e`.
    3. **Durable intent for a local observation's cloud delete** (contract rules 6 and 8; new owner `utils/cloud_sync_impl/observation_delete.py`).
       - The UI records the intent before the local delete and clears it after the cloud delete.
       - Every write-enabled push first finishes the bound account's pending deletes.
       - A re-linked local row voids the intent; a refusal (`CloudUploadRefusedError`) stops the retry and keeps the intent.
       - Pull, Download from Cloud and the cloud inbox never import a pending delete, nor one completed earlier in the same sync (the pull may use a listing read before the push).
       - `tests/test_observation_delete_intent.py` has 6 tests. The architecture doc lists the owner and rules; the shared contract text is unchanged, because the change implements its existing rules 6 and 8.
  - `ee6ab4e`: the new owner declared in the import layer map (`tests/test_cloud_sync_impl_import_direction.py`).
- **Tests on `T`:**
  - Focused sync families: 1409 passed, 1 skipped. They cover fault points, facade, download-only, tombstones, bytes-desired, fast path, dirty-loop, image push identity, portable guard, metadata sync, child-change probe, direct R2 guard, conflict plan execution, reference-use sync, every `test_sync_key_*`, `test_cloud_sync_image_*`, anchors, measurement sync, mosaics, contract, `r2_storage`, `cloud_media`, pending images, calibration linkage, sample source and the new delete-intent tests. The layering and facade tests pass too.
  - Full suite (without `tests/test_render_review_screenshots.py`, which crashed once in Qt rendering and fails 4 tests for its registry when run alone): 5714 passed, 21 failed, 54 errors.
  - Every remaining failure is environmental and outside sync: no `qapp` fixture (pytest-qt not installed), missing generated taxonomy data and source fixtures, no worktree-local `.venv`, a newer taxonomy release than the probes expect.
  - `git diff --check` is clean.
- **Web pin:** sporely-web `0bb2782`.
  - Brake migration `20261015090000_obsolete_desktop_sync_brake.sql`: sha256 `bd256809…a989`, identical to `0bb2782`.
  - Media guard `obsolete-desktop-guard.js`: sha256 `c313551d…c15b`, identical to `0bb2782`. The harness stub (`identity-impl/stub-w1b/stub-worker-w1b.mjs`) imports it unchanged and enables it exactly when the database brake is installed.
- **Target:** local stack `sporely-g3-verify` (API `127.0.0.1:55421`, stub worker `127.0.0.1:55425`).
  - Catalog snapshot `g4-evidence/p1d/target-fingerprint-start.txt`: three `sync_key` columns, the `*_user_sync_key_uidx` indexes, identity guard (`prosecdef=f`), write stop (`prosecdef=t`, `search_path=""`, 7 tables, block table empty), 7 `trg_000_sync_brake` triggers, audit triggers on 40 tables.
  - `g4-evidence/p1g/target-fingerprint-brake-on.txt` and the end-of-campaign fingerprint are identical to it.
- **Brake-off set-up windows (released builds only, never `T`):** the brake (database half, and therefore the stub's media half) was removed with the coordinator's `identity-impl/stub-w1b/remove-brake.sql` and re-applied from the exact migration file. Windows:
  - 10:59:24Z–11:10:09Z and 15:12:27Z–15:20:21Z on 2026-10-15: earlier campaigns, not cited;
  - **21:17:10Z–21:25:11Z on 2026-10-15** (`g4-evidence/p1g/logs-setup-window.txt`): this campaign's set-up.
    - Row 14c wrong-pairing state: two v0.9.15 installations, `p1g-14c@g3.test`.
    - Row 22 crashed-old-desktop state: v0.9.25, `p1g-leg@g3.test`.
    - Keyless collision rows for the brake regressions: v0.9.15, `p1g-old@g3.test`.

  Every case below ran with the brake applied.
- **Accounts** (local sign-up; profile rows `p1g/setup-users.sql`): `owner@g3.test` (rows 2–13d, 24, G4, web, ab reproducers), `p1g-two` (row 1 with exact overlap), `p1g-old` (rows 14, 14b, 14d, brake regressions, row 15), `p1g-leg` (row 22), `p1g-14c` (row 14c).
- **Harness:** `g4-evidence/tools/run.sh` unchanged, wrapped by `g4-evidence/p1g/case.sh` (before/after snapshots).
  - An observation-only capture shim (`p1g/capture/sitecustomize.py`) records every request's headers (credentials redacted), status and response body to `p1g/capture/runs/<run>.jsonl`.
  - Read-only analysis: `p1g/{audit,cloudstate,orphans,ownership,oldclients,runtable}.py`.
  - Isolation: `p1g-p00-netprobe` on `T`. Outbound 1.1.1.1:443 blocked; auth and REST 200; worker 404.
- **Web:** the sporely-web main checkout at `1029975`, whose `src/`, `index.html`, `vite.config.js` and `package.json` equal `0bb2782`. It was served by Vite with headless Chromium, both inside the kernel network sandbox, signed in as `owner@g3.test`.
- **Evidence per run `<run>`:**
  - `g4-evidence/runs/<run>/` (manifest, steps, results, screenshots);
  - `g4-evidence/p1g/logs/<run>.log` (audited DB writes, Kong non-GET requests, stub mutations, identity flags);
  - `g4-evidence/p1g/analysis/<run>.json`;
  - `g4-evidence/p1g/capture/runs/<run>.jsonl`.
- **Earlier campaigns** (`p1d-*` on `c50c627`, `p1f-*` on `ea2bd2e`) are not evidence here. Their findings led to the corrections above.

## Matrix rows on `T`

All rows ran with the brake applied. "Writes" are audited database writes in the run's window (`p1g/logs/<run>.log`). Owner runs also record the device heartbeat (`reference_client_devices.last_seen_at`), and, while the web tab was open, the web's own `record_client_activity` heartbeat; the desktop's capture shows no such call.

| Row | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 1 | p1g-t01-c1new, p1g-t02-c2new (fresh account `p1g-two`; C1, C2 never downloaded); p1g-a04-new, p1g-b03-new | B inserts; A untouched; no `desktop_id` writes | C1's **local 1** → INSERT #292/#279; C2's **local 1** → INSERT #293/#280; #292 untouched. On `owner`: A's local 152 → #284, B's local 153 → #285. 0 identity flags; no request body carries `desktop_id`. Every Data API and media request carries `x-sporely-sync-identity: 2` and `x-sporely-client-version: 0.9.25` (capture); only the auth login does not |
| 2 | p1g-a05…a07-pull, p1g-b04…b06-pull; p1g-t03…t08 | no identity writes, no `updated_at` echo | each of the 12 alternating pulls wrote only the device heartbeat |
| 3 | p1g-a08-edit, p1g-b07-pull; p1g-t09-c1edit, p1g-t10-c2pull | only B's linked row changes | A's edit wrote #284 and its image #271 only; B's pull wrote only the heartbeat. Same for C1 (#292) and C2 |
| 4 | p1g-a12-kedit, p1g-b08-cedit, p1g-a13-keep, p1g-b09-pull | keep-device; only the pair changes | conflict on #285 (`gps_latitude`); "local" wrote `set_observation_location` on #285 only (62.3333/9.3333, `manual/acquired`) |
| 5 | p1g-a14-tedit, p1g-b10-cedit, p1g-a15-take, p1g-b11-pull | take-cloud; only the pair changes | conflict on #16; "cloud" made no cloud write (heartbeat only) |
| 6 obs | p1g-a60-new, p1g-a61-kill (exit 86), p1g-a62-restart, p1g-a63-retry | 23505 → adopt; one row | killed after INSERT #309; restart: 0 writes; retry: POST 409 → "adopted cloud 309 by sync key (no PATCH)"; one row |
| 6 image | p1g-a64-new, p1g-a65-kill, p1g-a66-restart, p1g-a67-retry | same | killed after image INSERT #300; retry: POST 409 → adopted 300, image not written. Bytes to the reserved key `…/311/0_e78194ed…`. The parent's missing cover was restored by one conditional PATCH, `observations?id=eq.311&…&image_key=is.null&thumb_key=is.null` |
| 6 measurement | p1g-a68-micro, p1g-a69/a69b-measure, p1g-a70-kill, p1g-a71-restart, p1g-a72-retry | same | killed after INSERT #96; retry: adopted 96 |
| 7 | p1g-a73-micro, p1g-a74-measure, p1g-a75-timeout, p1g-a75b-restart, p1g-a75c-sync | adopt, no duplicate | three 201 answers discarded as read timeouts; the same sync adopted #313, #303 and #98; one row each |
| 8 | p1g-a76-new, p1g-a77-kill, p1g-a78-restart, p1g-a79-retry | single insert on retry | `before_request:observations`: 0 writes; retry: INSERT #315 once |
| 9 in flight | p1g-a80-new, p1g-a81-kill, p1g-a82-restart, p1g-a83-retry | reserved key reused, one object | `during_upload`: the stub logged the PUT of `…/316/0_d31259ff…` (01:47:08Z) and stored nothing. Retry: adopted #306 and stored that key once, plus its thumb |
| 9 after upload | p1g-a84-new, p1g-a85-kill, p1g-a86-restart, p1g-a87-retry | same | `after_upload`: `…/317/0_1652019b…` stored, then killed; retry: adopted #308, same key re-PUT; one object |
| 10 | p1g-a88-new, p1g-a89-connerr, p1g-a90-restart, p1g-a91-retry | deferred; parent first | 20 connection errors on the observation POST: 0 writes, no image POST; retry: #318, then image #310 |
| 11 | restart probes p1g-a62, a66, a71, a75b, a78, a82, a86, a90 | same as 6–10 | each crash was followed by a restart (0 writes), then the retry above. All parents have their cover keys afterwards (#309, #311, #313, #315–#318) |
| 12 portable | p1g-b12-micro, p1g-b13-measure, p1g-a33-pull, p1g-a34-export, p1g-b14-import | fresh keys, new rows, no shared `cloud_id` | B's #295/#283/#85–86, exported by A and imported into B → new #296/#284/#87–88, fresh keys, `desktop_id` NULL |
| 12 bundle | p1g-e01-micro, p1g-e02-measure, `p1g/archives/p1g-e.zip`, p1g-b15-bundle, p1g-e03-sync | same | E's measured observation packaged with the app's exporter (no GUI export) and imported into B → new #297/#285/#89. E's own rows → #298/#286/#90; E's local 1 left the `desktop_id` 1 row #2 untouched |
| 13 | p1g-a40-new, copy `p1g-A13`, p1g-a41-sync, p1g-c13-adopt | adopt silently | A uploaded #299/#287; the copy's POSTs → 409 → "adopted cloud 299 / 287 (no PATCH)"; heartbeat only |
| 13b | copy `p1g-A13b`, p1g-a42-edit, p1g-c13b-conflict | conflict; no PATCH | "conflict push blocked … categories=['sync_key_adoption'] action=review_required"; heartbeat only |
| 13c | p1g-a45-new (two photos), copy `p1g-A13c`, p1g-a46-sync, p1g-a47-move, p1g-c13c-tomb, p1g-c13c-again | deletion/restore flow; no resurrection | the move tombstoned the second image #290 (new #291 under #284). The copy adopted #303/#289. Its second image: "its cloud copy 290 was deleted in the cloud; never recreated", with no INSERT; #290 stays deleted |
| 13c-p | copy `p1g-A13cp`, p1g-a43-delete, p1g-a44-sync, p1g-c13cp-deleted, p1g-c13cp-again | accepted limitation: re-uploaded once | after A hard-deleted #299, the unlinked copy re-uploaded it once as #302; nothing else written; next sync wrote nothing |
| 13d | fixture `p1g/fault-constraint.sql`; p1g-a51-obs-fk, a52-obs-uq, a53-img-fk, a54-img-uq, a57-meas-fk, a58-meas-uq, a59-after-fixture | plain error, not adoption | A disposable trigger answers INSERTs exactly as the constraints would; no client can reach these answers on this schema. Each answer was a plain error and the row stayed dirty: `23503` (observation, image and measurement foreign keys); `23505` on `observations_desktop_id_user_unique`, `observation_images_desktop_id_user_unique` and `idx_spore_measurements_desktop_user`. After the fixture was dropped, the rows uploaded (measurement #94) |
| interrupted delete (brake regression, new desktop) | p1g-a92-new, p1g-a93-unsynced, p1g-a94-del-kill (`between_deletes`, exit 86), p1g-a95-restart, p1g-a96-sync, p1g-a97-restart | resumes; unsynced work kept | **Fixed in this stage.** After the kill, cloud #319 had no image rows. Restart: 0 writes. The next sync: "pending observation deletes: completed=['319']" → `DELETE observations#319`; no local row re-imported, no intent left. The unsynced observation was kept and uploaded (#320). Restart: 0 writes |

**Orphan objects (stub store, `owner@g3.test`):** 0 of 329 at the start, 0 of 370 before the crash cases, 0 of 390 after them (`p1g/analysis/orphans-p1g-*.json`).

## Outstanding desktop GUI scenarios from the local-key check (on `T`)

- **Measurement creation** (p1g-a30-micro, p1g-a31-measure; also b12/b13, e01/e02, a55/a56, a68/a69, a73/a74): rectangle clicks in the Measure tab on a 100X microscope image; keyed measurements #83/#84 under image #281, observation #294.
- **Bundle import** (p1g-b15-bundle) and **portable import** (p1g-b14-import): row 12.
- **Image move** (p1g-a32-move, gallery "Move to observation"): local 155 → 159. Old cloud image #273 tombstoned (`deleted_at`); new keyed #282 under #290.
- **Observation without photos** (p1g-d03-nophoto, new scenario): keyed #330.

## Legacy recovery, inherited pairing and the exact reproducers (on `T`)

| Row / case | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 22 | set-up (brake off, released v0.9.25): p1g-l01…l05, fixtures `p1g-l04-fixture`, `p1g-l06-fixture`; upgrade: p1g-l10-upgrade, p1g-l11-noop | equal content → linked, no cloud write, no duplicate; different content → new row plus report; nothing PATCHed | L uploaded #279–#281 with images #266–#268 and measurement #82. The fixture then unlinked observations 1 and 2 (with images) and the measurement, and changed observation 2's notes. On `T`: "obs 1 linked to cloud 281", "image 1 linked to cloud 268", "measurement 1 linked to cloud 82", each "(no cloud write)". **No row and no media write to #281/#268/#82**: the upgrade's only writes were the new duplicate (capture `p1g-l10-upgrade.jsonl`). "obs 2 not linked (matches=1) … possible duplicate": new #321/#313. No-op: heartbeat only, no stub mutation |
| 22, another device's colliding rows | p1g-l07/l08 (v0.9.25 L2, never downloaded), p1g-l20-upgrade, p1g-l21-noop | new rows plus report; nothing PATCHed | new #322/#323 (images #314/#315) with two reports; L's rows untouched |
| 14c | set-up (brake off): p1g-c01…c07. P2's pull paired its locals 1 and 2 with P1's #276/#277 and overwrote them (the pre-brake defect, p1g-c06-p2sync); P2 then kept unsynced local 4. Upgrade of a copy to `T`: p1g-x14c-1-sync, p1g-x14c-2-edit, p1g-x14c-3-del, p1g-x14c-4-sync, p1g-x14c-5-restart | accepted limitation for inherited keyless links; still required: no unsynced local work lost; nothing written to any row other than the paired observation and its own children | **First sync:** unsynced local 4 → new keyed #324/#316, no write to #276–#278. **Edit of local 1** (paired with P1's #276): PATCHed #276 and its image #263, re-PUT P2's photo to #263's key. **Delete of local 2** (paired with P1's #277): deleted #277, its image #264 and its media. P2's own #278 never written. Locals 1, 3 and 4 kept after sync and restart. This matches the limitation recorded after the P1c failure (gate `56491780ae7348acabd6b9dbf4e709f2`; runs `p6-x14c-2-edit`, `p6-x14c-3-del`, `p6-c06-p2sync`) |
| ab15 | copy of `cand` (the original ab15 profile), p1g-r15-ab15 | zero overwrites, zero `desktop_id` writes | new local 27 (cloud #28 holds `desktop_id` 27) → INSERT #325/#317; #28 untouched (last change 2026-10-14) |
| ab19 | copy of `candAB2`, p1g-r19-ab19 | same | new local 27 → INSERT #326/#318; #28 untouched |
| ab17 | copy of `legacy` (the original ab17 profile, last used by released `0520afd`), on the new desktop at `T`: p1g-r17-ab17 | same | new local 28 (cloud #31 holds `desktop_id` 28) → INSERT #327/#319; #31 untouched |

Statuses "FAILED" on p1g-r19, p1g-r17, p1g-e03, p1g-e03b, p1g-a02, p1g-a03 and p1g-b02 come only from the driver's fixed 600 s wait during a long backlog pull; uploads had completed and there are no identity flags. The released `0520afd` baseline of the defect is the original ab17 run of 2026-10-13 (`g4-evidence/runs/ab17-legacy-new-photo`).

**Inherited legacy links without a sync key are trusted as they are. If an old version paired a local observation with another installation's cloud record, edits and deletes on the upgraded desktop can still affect that wrong cloud record until the link is corrected.**

## Old clients under the brake, brake regressions, row 14d

Account `p1g-old@g3.test` holds:
- released v0.9.15 installation S's keyless rows #282/#283 (`desktop_id` 1/2, from the set-up window);
- the new desktop D's keyed rows #328 and #329 with images, and the photo-less #330 (p1g-d01…d04).

Each released build then ran, on a fresh profile:
1. two unsynced local observations (local ids 1 and 2 collide with S's `desktop_id` values);
2. a sync;
3. an edit of D's keyed #328;
4. a delete of the local row its pull paired with S's #282 (the earlier wrong-target delete);
5. a delete of D's keyed #329 (the earlier ghost row);
6. a delete of the photo-less #330;
7. a sync, and a restart without a new login.

Responses: `p1g/capture/runs/p1g-o-<build>-<step>.jsonl`. Summary: `p1g/analysis/oldclients.json`.

| Case | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 14 | p1g-o-{v0.8.13, v0.9.1, v0.9.15, v0.9.18, v0.9.25, 0520afd, ee6f2af}-{1new…9restart} (63 runs) | every row write refused `P0001`/`sporely_upgrade_required`, every media PUT/DELETE 426; zero cloud changes; downloads work; no sign-in prompt; credentials and unsynced local work kept | **Not met: unsynced local work (F1).** Every other condition held: **cloud unchanged**: the account digest stayed `97182d88826c` from before the first client to after the last (`p1g/analysis/cloudstate-old-*.json`); the only audited writes were the builds' own device records. Every row write → 400 `sporely_upgrade_required`; every media PUT and DELETE → 426 with `X-Sporely-Error-Code: sporely_upgrade_required`. 0 responses 401/403, no sign-in dialog; the restart without `--login` synced (credentials kept); pulls worked. **F1:** each build's own pull paired its unsynced local 2 with S's #283 and overwrote its fields: latitude 62.671, from its photo, became S's 62.143 (`p1g/logs-phase4.txt`). The local row and photo remain, next to S's photo |
| 14b | same; `oldclients.json` | link repair refused; NULL link | 0 `desktop_id` changes in any run; D's #328–#330 keep `desktop_id` NULL |
| wrong-target delete (v0.9.15, `0520afd`, and every other build) | `…-5delpair` | zero cloud changes | media DELETE → 426, row DELETEs → 400; #282 and its media intact. Accepted local effect: the row reappears locally on the next sync |
| ghost row (old-desktop delete of a keyed row) | `…-6delkeyed` | zero cloud changes | #329 intact for every build; reappears locally on the next sync |
| photo-less delete (database DELETE reached) | `…-7delnophoto` | refused | `DELETE observation_images?observation_id=eq.330` matched no rows (204); `DELETE observations?id=eq.330` → 400 `sporely_upgrade_required`; #330 intact |
| collision, pull and delete on v0.9.18, v0.9.25, `ee6f2af` | as above | zero cloud changes | as above |
| new desktop beside released `ee6f2af` and `0520afd` | p1g-d05-pull, p1g-d06-edit, p1g-d07-pull | released builds refused; new desktop's rows untouched | D's rows unchanged by every released build; D's pull wrote only its heartbeat; its edit wrote only #328 and its image |
| 14d | new desktop: p1g-d01…d07 (create, photo-less create, edit, media PUT); deletes with media DELETE on `owner` (p1g-a43-delete, p1g-a96-sync); `p1g/api14d.py` → `p1g/analysis/api14d-p1g-old.json`; web p1g-w01-web-set; write stop: row 23 | all allowed; the write stop still refuses a listed new-desktop version | new desktop: every write succeeded under the brake. API probe on #329, no identity header: browser and Android-WebView (`…; wv) … Mobile Safari`) User-Agents → PATCH 204, PUT 200, DELETE 200; released-desktop `python-requests/2.32.3` → PATCH 400 `sporely_upgrade_required`, PUT 426, DELETE 426. Web `set_observation_location` → 200 |

## Rows 15, 23 and 24 (on `T`)

| Row | Runs | Expected | Observed |
| --- | --- | --- | --- |
| 15 | `sync_key` renamed on the three tables (disposable, `p1g/logs/p1g-k-hide.log`, catalog `p1g/target-fingerprint-row15-hidden.txt`), p1g-k01-dl, p1g-k02-new, p1g-k03-sync; restored (`p1g-k-restore.log`), p1g-k04-after-restore | uploads blocked "cloud not ready"; pull works | download worked. Banner "Sporely Cloud is not ready for this version yet. Uploads are paused; downloads still work.", 0 writes. After the restore the fingerprint was identical, and the pending row uploaded (#341/#331) |
| 23 | p1g-a99-base; block `0.9.25` (`p1g/logs/p1g-block.log`); p1g-a100-blocked, p1g-a101-blocked-again, p1g-a101b-new-blocked, p1g-w03-web-blocked; unblock; p1g-a102-unblocked, p1g-a103-noop | as the matrix | empty table: normal. **Blocked** (`T` sends `x-sporely-client-version: 0.9.25`): PATCH of #284 and POST of a new observation → 400 `P0001` `sporely_upgrade_required`, "This version of Sporely Desktop is blocked from uploading. Install the latest version from sporely.no. Nothing was changed in the cloud."; 0 cloud writes in all three runs. The banner "This version of Sporely can no longer upload. Please install the latest version." was shown again after the restart. Edit and new row kept locally. Web `set_observation_location` on #16 → 200. **Unblocked:** the edit (#284 `open_comment`) and the new row (#340/#330) uploaded; the next no-op wrote only the heartbeat |
| 24 | `p1g/analysis/ownership-p1g-{owner,old,leg,14c}.json` (audit since id 3535, the first run of this campaign) | every child under its own parent; storage paths in the owner's prefix; no re-parenting | 14 profiles. Every linked image is under its local parent's cloud row, and every linked measurement under its local parent image's cloud row. All storage paths are in the owner's prefix; 0 re-parenting UPDATEs. The single dangling link (copy A13's image → #287) points at the row A deliberately hard-deleted in 13c-p |

**Orphan objects at the end:** 0 of 398 (`owner`), 0 of 8 (`p1g-old`), 0 of 13 (`p1g-leg`), 0 of 6 (`p1g-14c`).

## Photo-location re-check (G4) on `T`

| Case | Runs | Observed |
| --- | --- | --- |
| Download on a fresh profile, then the first write-enabled sync | p1g-a01-dl, p1g-a02-sync, p1g-a02b-catchup, p1g-b01-dl, p1g-b02-sync, p1g-b02b-catchup | download: the desktop sent only its login. First syncs: 0 identity writes; non-identity echoes are re-sends of unchanged image tombstones (`updated_at` only, with the parent's trigger-bumped `updated_at`), and A's catch-up recomputed a few spore summaries once |
| No-op sync | p1g-a05…a07, p1g-b04…b06, p1g-a103-noop | heartbeat only. The first post-download sync p1g-a03-noop also recomputed another device's spore statistics and mosaic for #228 once (non-identity) |
| New observation from a geotagged photo | p1g-a04-new | INSERT #284 `image_exif/acquired` |
| Web → desktop → web retention | p1g-w01-web-set, p1g-a23-pull, p1g-a24-unrelated, p1g-w02-web-read | the web set #16 to `manual/acquired`. The desktop pulled it and made an `open_comment` edit without location keys. `runs/p1g-w02-web-read/web-result.json`: 63.2218…/10.2234…, `manual/acquired`, comment `p1g-a24` |
| Unrelated edit | p1g-a08-edit, p1g-a24-unrelated | no location keys written |
| Attributed edit, restart before sync | p1g-a09-attr, p1g-a10-restart, p1g-a11-sync | 0 writes until sync, then `set_observation_location` → #284 `manual/acquired` 61.4444/9.4444 |
| Conflict keep-device / take-cloud | p1g-a13-keep / p1g-a15-take | rows 4/5 |
| Restart persistence | p1g-a10-restart, p1g-a25-restart | 0 writes |
| Edit Images detour | p1g-a16-detour | the entered latitude equals #13's stored value; the PATCH carried no location keys; no RPC; #13 stays NULL/NULL |
| Prepare Images typing (re-run, not carried forward) | p1g-a17-r03, a18-r04, a19-r05, a20-r06, a21-r04b, a22-r05b | r03 `manual/acquired` 61.1/9.1; r04 `image_exif/acquired`; r05 `image_exif/acquired` (restored in the details dialog); r06 (no GPS) `manual/acquired` 59.5/10.5; r04b `image_exif/acquired`; r05b `image_exif/acquired`. All match `ee6f2af`. r04/r05 have the steps of r04b/r05b, each with its own photo (the original r04/r05 were invalid duplicate-file runs) |

`git diff ee6f2af..T` touches `ui/image_import_dialog.py` and `ui/observations_tab.py`, so the typing set was re-run in full rather than carried forward.

## Findings

- **F1, open: released builds overwrite unsynced local work when local ids collide** (all seven builds, `p1g-o-<build>-3sync`). Each released build's own pull pairs its unsynced local observation with another installation's keyless cloud row through its built-in `desktop_id` reverse lookup, and overwrites the local fields. The local photo is kept. The cloud is not changed: the brake refuses every write. Row 14 requires unsynced local work to be kept, and the plan's accepted local effects do not include this, so **row 14 fails on this condition**. It cannot be changed by the new desktop or the brake. It needs the plan owner's disposition (an accepted limitation with an upgrade-notice item, or another measure) before the stage can be accepted.
- **F2, fixed in this stage** (`5145af1`): an interrupted new-desktop delete no longer leaves a partial cloud observation that comes back (interrupted-delete row above).
- **F3, row 14c limitation re-confirmed:** edits and deletes through an inherited keyless wrong pairing reach the other installation's row, its image row and its media. Release preconditions 1 and 3 apply.
- **F4, fixed in this stage** (`5145af1`): legacy recovery no longer uploads bytes to the matched row's key (row 22 above).
- **F5, environment and harness notes.**
  - Fresh owner profiles need longer than the driver's fixed 600 s wait to pull the account's backlog; catch-up pulls were added.
  - The media of cloud rows 3 and 21 is missing from the stub store, so owner syncs report "2 other issue(s)".
  - The partial Download from Cloud on a fresh profile is tracked separately.

## Corrections

From the sparrer findings of 2026-10-14:
1. Row 9 has an in-flight fault (`during_upload`, p1g-a81) and an after-upload fault (`after_upload`, p1g-a85); `before_request` (p1g-a77) is row 8 only.
2. The full typing set r03, r04, r05, r06, r04b and r05b ran on `T`.
3. Row 13d has the constraint-specific `23503` and other-constraint `23505` G cases for all three tables, not `42501`.
4. ab17 ran with the new desktop at `T` (p1g-r17-ab17).
5. The first attempt's Stage 6 preflight timeout (2026-10-14) was a host maintenance DarkWake (19:03:55–19:08:23 local), not a proxy. The harness `run.sh` was restored to its original form (`identity-impl/STATUS.md`, 17:14:01Z correction) and is the unmodified launcher used here.

From the review of 2026-10-16:
1. Cover repair preserves existing and concurrently set covers (`5145af1`; row 6 image).
2. Legacy recovery writes no row and no media to the match (`5145af1`; row 22).
3. Row 14 is recorded as failing on F1, pending the plan owner's disposition.
4. The interrupted delete resumes instead of resurrecting (`5145af1`; interrupted-delete row).

## Appendix: every log of this campaign (`g4-evidence/p1g/analysis/runtable.json`)

Build `T` = `ee6ab4e`; released heads by name; "-" = a fixture, SQL, web or API log without a desktop manifest. "EXIT 86" is the product fault hook ending the process as intended. DB writes and stub mutations are counted in each run's audit window.

| Run | Build | Scenario | Status | DB writes | Stub mutations | Identity flags |
| --- | --- | --- | --- | --- | --- | --- |
| p1g-c01-p1new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1g-c02-p1new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1g-c03-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-c04-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-c05-p2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-c06-p2sync | v0.9.15 | sync_now | OK | 15 | 12 | 4 |
| p1g-c07-p2unsynced | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-l01-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-l02-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-l03-new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-l04-fixture | - | - |  | None | None | 0 |
| p1g-l05-sync | v0.9.25 | sync_now | OK | 27 | 14 | 14 |
| p1g-l06-fixture | - | - |  | None | None | 0 |
| p1g-l07-l2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-l08-l2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-s01-new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1g-s02-new | v0.9.15 | new_observation | OK | 7 | 4 | 4 |
| p1g-p00-netprobe | T |  |  | None | None | 0 |
| p1g-a01-dl | T | download_from_cloud | OK | 0 | 0 | 0 |
| p1g-a02-sync | T | repo:sync_and_record | FAILED | 4 | 0 | 0 |
| p1g-a02b-catchup | T | repo:sync_and_record | OK | 12 | 0 | 0 |
| p1g-a03-noop | T | repo:sync_and_record | FAILED | 59 | 31 | 0 |
| p1g-b01-dl | T | download_from_cloud | OK | 0 | 0 | 0 |
| p1g-b02-sync | T | repo:sync_and_record | FAILED | 4 | 0 | 0 |
| p1g-b02b-catchup | T | repo:sync_and_record | OK | 9 | 0 | 0 |
| p1g-a04-new | T | new_observation | OK | 18 | 9 | 0 |
| p1g-b03-new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-b04-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a06-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-b05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-b06-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a08-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1g-b07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a09-attr | T | edit_observation | OK | 0 | 0 | 0 |
| p1g-a10-restart | T | idle | OK | 0 | 0 | 0 |
| p1g-a11-sync | T | repo:sync_and_record | OK | 4 | 0 | 0 |
| p1g-a12-kedit | T | edit_observation | OK | 0 | 0 | 0 |
| p1g-b08-cedit | T | edit_observation | OK | 7 | 4 | 0 |
| p1g-a13-keep | T | sync_with_conflicts | OK | 3 | 0 | 0 |
| p1g-b09-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a14-tedit | T | edit_observation | OK | 0 | 0 | 0 |
| p1g-b10-cedit | T | edit_observation | OK | 4 | 0 | 0 |
| p1g-a15-take | T | sync_with_conflicts | OK | 1 | 0 | 0 |
| p1g-b11-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a16-detour | T | edit_via_prepare_images | OK | 4 | 0 | 0 |
| p1g-a17-r03 | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a18-r04 | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a19-r05 | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a20-r06 | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a21-r04b | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a22-r05b | T | new_observation | OK | 8 | 4 | 0 |
| p1g-w01-web-set | - | web |  | 4 | 0 | 0 |
| p1g-a23-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a24-unrelated | T | edit_observation | OK | 2 | 0 | 0 |
| p1g-w02-web-read | - | web |  | 2 | 0 | 0 |
| p1g-a25-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-t01-c1new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-t02-c2new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-t03-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t04-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t05-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t06-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t07-c1pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t08-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-t09-c1edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1g-t10-c2pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a30-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-a31-measure | T | repo:measure_new | OK | 17 | 6 | 0 |
| p1g-a32-move | T | repo:move_image | OK | 10 | 8 | 0 |
| p1g-b12-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-b13-measure | T | repo:measure_new | OK | 17 | 6 | 0 |
| p1g-a33-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a34-export | T | repo:export_portable | OK | 0 | 0 | 0 |
| p1g-b14-import | T | repo:import_archive | OK | 13 | 0 | 0 |
| p1g-e01-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-e02-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1g-e-bundle | - | - |  | None | None | 0 |
| p1g-b15-bundle | T | repo:import_archive | OK | 14 | 6 | 0 |
| p1g-e03-sync | T | repo:sync_and_record | FAILED | 13 | 6 | 0 |
| p1g-e03b-catchup | T | repo:sync_and_record | FAILED | 4 | 0 | 0 |
| p1g-a40-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a41-sync | T | repo:sync_and_record | OK | 12 | 4 | 0 |
| p1g-c13-adopt | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1g-a42-edit | T | edit_observation | OK | 9 | 4 | 0 |
| p1g-c13b-conflict | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a43-delete | T | delete_observation | OK | 3 | 4 | 2 |
| p1g-a44-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-c13cp-deleted | T | repo:sync_and_record | OK | 2 | 0 | 0 |
| p1g-c13cp-again | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a45-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a46-sync | T | repo:sync_and_record | OK | 12 | 8 | 0 |
| p1g-a47-move | T | repo:move_image | OK | 13 | 8 | 0 |
| p1g-c13c-tomb | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1g-c13c-again | T | repo:sync_and_record | OK | 6 | 0 | 0 |
| p1g-a50-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a51-obs-fk | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a52-obs-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a53-img-fk | T | repo:sync_and_record | OK | 2 | 0 | 0 |
| p1g-a54-img-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a55-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-a56-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1g-a57-meas-fk | T | repo:sync_and_record | OK | 16 | 8 | 0 |
| p1g-a58-meas-uq | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-13d-fixture | - | - |  | None | None | 0 |
| p1g-a59-after-fixture | T | repo:sync_and_record | OK | 5 | 2 | 0 |
| p1g-a60-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a61-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 1 | 0 | 0 |
| p1g-a62-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a63-retry | T | repo:sync_and_record | OK | 7 | 4 | 0 |
| p1g-a64-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a65-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 3 | 0 | 0 |
| p1g-a66-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a67-retry | T | repo:sync_and_record | OK | 2 | 4 | 0 |
| p1g-a68-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-a69-measure | T | repo:measure_new | OK | 14 | 6 | 0 |
| p1g-a69b-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1g-a70-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 3 | 0 | 0 |
| p1g-a71-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a72-retry | T | repo:sync_and_record | OK | 5 | 3 | 0 |
| p1g-a73-micro | T | repo:new_microscope_observation | OK | 0 | 0 | 0 |
| p1g-a74-measure | T | repo:measure_new | OK | 0 | 0 | 0 |
| p1g-a75-timeout | T | repo:sync_with_fault | OK | 15 | 6 | 0 |
| p1g-a75b-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a75c-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a76-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a77-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 0 | 0 | 0 |
| p1g-a78-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a79-retry | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1g-a80-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a81-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 4 | 1 | 0 |
| p1g-a82-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a83-retry | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1g-a84-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a85-kill | T | repo:sync_and_record | EXIT 86 (product fault hook) | 4 | 2 | 0 |
| p1g-a86-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a87-retry | T | repo:sync_and_record | OK | 1 | 4 | 0 |
| p1g-a88-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a89-connerr | T | repo:sync_with_fault | OK | 0 | 0 | 0 |
| p1g-a90-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a91-retry | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1g-a92-new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-a93-unsynced | T | new_observation | OK | 0 | 0 | 0 |
| p1g-a94-del-kill | T | delete_observation | EXIT 86 (product fault hook) | 2 | 4 | 1 |
| p1g-a95-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-a96-sync | T | repo:sync_and_record | OK | 9 | 4 | 1 |
| p1g-a97-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-l10-upgrade | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1g-l11-noop | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-l20-upgrade | T | repo:sync_and_record | OK | 15 | 8 | 0 |
| p1g-l21-noop | T | repo:sync_and_record | OK | 3 | 0 | 0 |
| p1g-x14c-1-sync | T | repo:sync_and_record | OK | 8 | 4 | 0 |
| p1g-x14c-2-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1g-x14c-3-del | T | delete_observation | OK | 3 | 4 | 2 |
| p1g-x14c-4-sync | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-x14c-5-restart | T | repo:probe | OK | 0 | 0 | 0 |
| p1g-r15-ab15 | T | new_observation | OK | 7 | 4 | 0 |
| p1g-r19-ab19 | T | new_observation | FAILED | 7 | 4 | 0 |
| p1g-r17-ab17 | T | new_observation | FAILED | 7 | 4 | 0 |
| p1g-d01-new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-d02-new | T | new_observation | OK | 8 | 4 | 0 |
| p1g-d03-nophoto | T | repo:new_observation_without_images | OK | 2 | 0 | 0 |
| p1g-d04-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-o-v0.8.13-1new | v0.8.13 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.8.13-2new | v0.8.13 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.8.13-3sync | v0.8.13 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.8.13-4edit | v0.8.13 | edit_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.8.13-5delpair | v0.8.13 | delete_observation | OK | 0 | 2 | 0 |
| p1g-o-v0.8.13-6delkeyed | v0.8.13 | delete_observation | OK | 0 | 2 | 0 |
| p1g-o-v0.8.13-7delnophoto | v0.8.13 | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.8.13-8sync | v0.8.13 | sync_now | OK | 0 | 4 | 0 |
| p1g-o-v0.8.13-9restart | v0.8.13 | sync_now | OK | 0 | 4 | 0 |
| p1g-o-v0.9.1-1new | v0.9.1 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.1-2new | v0.9.1 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.1-3sync | v0.9.1 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.1-4edit | v0.9.1 | edit_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.1-5delpair | v0.9.1 | delete_observation | OK | 0 | 2 | 0 |
| p1g-o-v0.9.1-6delkeyed | v0.9.1 | delete_observation | OK | 0 | 2 | 0 |
| p1g-o-v0.9.1-7delnophoto | v0.9.1 | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.1-8sync | v0.9.1 | sync_now | OK | 0 | 4 | 0 |
| p1g-o-v0.9.1-9restart | v0.9.1 | sync_now | OK | 0 | 4 | 0 |
| p1g-o-v0.9.15-1new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-2new | v0.9.15 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-3sync | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-4edit | v0.9.15 | edit_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-5delpair | v0.9.15 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.15-6delkeyed | v0.9.15 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.15-7delnophoto | v0.9.15 | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-8sync | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.15-9restart | v0.9.15 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-1new | v0.9.18 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-2new | v0.9.18 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-3sync | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-4edit | v0.9.18 | edit_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-5delpair | v0.9.18 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.18-6delkeyed | v0.9.18 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.18-7delnophoto | v0.9.18 | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-8sync | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.18-9restart | v0.9.18 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-1new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-2new | v0.9.25 | new_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-3sync | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-4edit | v0.9.25 | edit_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-5delpair | v0.9.25 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.25-6delkeyed | v0.9.25 | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-v0.9.25-7delnophoto | v0.9.25 | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-8sync | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-v0.9.25-9restart | v0.9.25 | sync_now | OK | 0 | 0 | 0 |
| p1g-o-0520afd-1new | 0520afd | new_observation | OK | 0 | 0 | 0 |
| p1g-o-0520afd-2new | 0520afd | new_observation | OK | 0 | 0 | 0 |
| p1g-o-0520afd-3sync | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1g-o-0520afd-4edit | 0520afd | edit_observation | OK | 1 | 0 | 0 |
| p1g-o-0520afd-5delpair | 0520afd | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-0520afd-6delkeyed | 0520afd | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-0520afd-7delnophoto | 0520afd | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-0520afd-8sync | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1g-o-0520afd-9restart | 0520afd | sync_now | OK | 1 | 0 | 0 |
| p1g-o-ee6f2af-1new | ee6f2af | new_observation | OK | 0 | 0 | 0 |
| p1g-o-ee6f2af-2new | ee6f2af | new_observation | OK | 0 | 0 | 0 |
| p1g-o-ee6f2af-3sync | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1g-o-ee6f2af-4edit | ee6f2af | edit_observation | OK | 1 | 0 | 0 |
| p1g-o-ee6f2af-5delpair | ee6f2af | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-ee6f2af-6delkeyed | ee6f2af | delete_observation | OK | 0 | 8 | 0 |
| p1g-o-ee6f2af-7delnophoto | ee6f2af | delete_observation | OK | 0 | 0 | 0 |
| p1g-o-ee6f2af-8sync | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1g-o-ee6f2af-9restart | ee6f2af | sync_now | OK | 1 | 0 | 0 |
| p1g-d05-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-d06-edit | T | edit_observation | OK | 4 | 4 | 0 |
| p1g-api14d | - | - |  | None | None | 0 |
| p1g-d07-pull | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-oldclients-summary | - | - |  | None | None | 0 |
| p1g-a99-base | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-a100-blocked | T | edit_observation | OK | 0 | 0 | 0 |
| p1g-a101-blocked-again | T | repo:sync_and_record | OK | 0 | 0 | 0 |
| p1g-a101b-new-blocked | T | new_observation | OK | 0 | 0 | 0 |
| p1g-w03-web-blocked | - | web |  | 4 | 0 | 0 |
| p1g-block | - | - |  | None | None | 0 |
| p1g-a102-unblocked | T | repo:sync_and_record | OK | 9 | 4 | 0 |
| p1g-a103-noop | T | repo:sync_and_record | OK | 1 | 0 | 0 |
| p1g-k-hide | - | - |  | None | None | 0 |
| p1g-k01-dl | T | download_from_cloud | OK | 0 | 0 | 0 |
| p1g-k02-new | T | new_observation | OK | 0 | 0 | 0 |
| p1g-k03-sync | T | repo:sync_and_record | OK | 0 | 0 | 0 |
| p1g-k-restore | - | - |  | None | None | 0 |
| p1g-k04-after-restore | T | repo:sync_and_record | OK | 8 | 4 | 0 |
