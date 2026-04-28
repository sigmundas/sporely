# Sporely Development Plan (Merged Desktop & Web)

## Code Review & Refactoring
*Review this code with a strict refactor/audit mindset. Do not praise. Look for concrete problems only.*

For each issue you find, return:
- severity: low / medium / high
- category
- file(s)
- exact problem
- why it is a problem
- minimal fix
- whether fix is safe or risky

Check specifically for these categories:

1. Duplicate logic
- Same calculation, formatting, parsing, filtering, or validation implemented in multiple places
- Similar helper functions with slightly different behavior
- Repeated UI text mapping / label mapping / enum mapping
- Repeated SQL fragments or repeated DB row-to-object conversion
- Repeated code paths for create vs update that should share one function

2. Conflicting source of truth
- Same concept stored in multiple variables/fields with possible drift
- UI state duplicated in local widget state and global/app state
- Derived values stored instead of computed
- Flags with overlapping meaning (example: uncertain vs needs_id style drift)
- Cached values that are not invalidated reliably

3. Database consistency
- Field names inconsistent with app terminology
- Same field interpreted differently in different files
- Reads/writes missing defaults or null handling
- Manual SQL repeated across files instead of centralized helpers
- Migration risk: assumptions about columns existing without checks
- Boolean/integer/string inconsistencies for flags
- Unused columns, legacy columns, or dead migration paths

4. State flow problems
- State mutated from too many places
- Hidden side effects in setter/update functions
- UI refresh depends on call order
- State changed without emitting update/refresh signals
- Async operations that can race or overwrite newer state
- Screen-level state that should be owned centrally, or central state that should stay local

5. UI consistency problems
- Same concept displayed with different labels in different screens
- Different rules for formatting names, dates, units, uncertain markers, etc.
- Button behavior differs between screens without reason
- Same filter/sort option implemented differently in different views
- Translation keys duplicated, stale, or inconsistent
- UI conditions duplicated instead of shared helper formatting

6. Dead code / stale code
- Unused functions, classes, imports, constants, styles, translation keys
- Old code paths kept after refactor
- Commented-out code that should be deleted
- Feature flags or branches that are no longer reachable
- Files that appear obsolete or superseded

7. Overgrown files / bad boundaries
- Files doing too many unrelated things
- UI files containing DB logic or business rules
- State files containing presentation formatting
- Large functions that mix query, transformation, and rendering
- Helpers that know too much about callers

8. Naming problems
- Names that hide real meaning
- Same thing called different names in different files
- Old legacy names still used after concept changed
- Booleans with misleading names
- Function names that sound pure but mutate state

9. Error handling / edge cases
- Missing guard clauses
- None/null/undefined handling inconsistencies
- Empty list / empty selection / missing record edge cases
- Silent failure paths
- User-visible state not reset after failure
- Potential crash when DB field or UI element is absent

10. Refactor opportunities worth doing now
- Extract shared helper
- Centralize formatting rules
- Centralize DB access for one entity
- Replace copy-paste conditionals with enum/config mapping
- Split giant file into modules
- Remove legacy aliasing and adopt one canonical term

Important:
- Prefer specific findings over style opinions
- Ignore superficial formatting unless it hides a real problem
- Do not suggest huge rewrites unless necessary
- Flag places where behavior may drift across desktop/web/mobile versions
- Distinguish “must fix” from “cleanup”

### Existing Refactor & Audit Tasks
- [ ] **Make room for text on measure type radio buttons** — Currently, Multi-line text is cut off. Same with Square, choice for Reference shape on Analysis tab. Perhaps rename to Shape instead of Reference shape.
- [ ] **Table highlight*** — There are some lines appearing inside the table cells when selected. Like the AI suggestions table: clicking it make a light-gray ine appear over the text. Possibly some cell frame that has collapsed into a line. Observations table shows a grey rectangle in the cell that is clicked - it appears to have a grey gradient fill. No need for this. The highlight in Measurements on Measure tab appears good. Use the same style for highlight on other tables: define that color in css. Apply same in all other tables.

## Cloud vs local db

### Phase 1
I need to implement a "Database Lock" feature in my Sporely desktop app (`sporely-py`) to prevent users from accidentally syncing a single local SQLite database to multiple Supabase accounts.

Currently, `app_settings.json` stores `cloud_last_pull_at`. I want to expand this to also store a `linked_cloud_user_id`.

Please write the Python code for `sporely/utils/cloud_sync.py` (and any related settings managers) to do the following:
1. **Fetch Current User:** Before starting the push/pull sync loop, decode the current session's JWT or make a quick API call to Supabase to get the active user's `user_id`.
2. **Check Lock:** Compare this `user_id` against `linked_cloud_user_id` in `app_settings.json`.
3. **Bind on First Sync:** If `linked_cloud_user_id` is null/empty, save the current `user_id` to the settings file and proceed with the sync.
4. **Abort on Mismatch:** If the `user_id` does not match the stored `linked_cloud_user_id`, immediately abort the sync and raise a custom exception (e.g., `AccountMismatchError`).
5. Provide the PySide6 UI code snippet to catch this `AccountMismatchError` and show a `QMessageBox.critical` stating: "This local database is permanently linked to another Sporely Cloud account. Please switch to the correct OS user profile, or use the 'Reset Cloud Sync' tool in Settings to migrate your data to a new account."

### Phase 2
I need to add a "Reset Cloud Sync State" feature to the settings tab of my Sporely desktop app (`sporely-py`). Because I am on a free hosting tier, I need to be very careful about users abandoning old accounts and eating up my Supabase/R2 quotas with duplicate uploads.

Please write the SQLite database queries, Python logic, and PySide6 UI code to handle this reset.

**1. Database Reset Logic (`sporely/database/models.py` or similar):**
Write a function that strictly wipes all cloud references from the local SQLite database so it treats all local data as unpublished/new. It needs to:
* Set `cloud_id` to NULL and `sync_status` to 'dirty' for all observations.
* Nullify any cloud-specific media paths (e.g., stored R2 keys) in the local image records, forcing the next sync to re-upload them.
* Clear `cloud_last_pull_at` and the new `linked_cloud_user_id` from `app_settings.json`.

**2. PySide6 UI Implementation:**
Create a "Reset Cloud Link..." button in the Settings UI. When clicked, it must show a high-friction `QMessageBox.critical` to ensure the user understands the old cloud data will NOT be automatically deleted. Use the following workflow:

* **Title:** CRITICAL: Reset Cloud Link
* **Text:** "Resetting the cloud link will sever the connection to your current Sporely Cloud account. Your local data will remain safe, but the next time you sync, ALL local images and observations will be uploaded as brand new files.\n\nIMPORTANT: This action DOES NOT delete your old cloud data. To prevent duplicate storage, you MUST do the following first:\n1. Open the Sporely Web App.\n2. Log into your CURRENT account.\n3. Go to Profile -> Delete Account to erase your old data."
* **Buttons:** * "Cancel" (Default, safe option)
  * "I have already deleted my old account, proceed with reset" (Executes the SQLite wipe)

**3. Execution:**
If the user confirms, execute the database reset logic, log the user out of their current desktop session, and show a success message prompting them to log in with their new account.

### Manual test plan: cloud account lock and reset

#### Phase 0: Export and backup coverage check
- [ ] **Do not use the app database export as the golden backup for this test.** `utils/db_share.py` exports a share/import bundle: selected observation/image/measurement/calibration tables, copied image files, objective profiles, and reference values. It does not currently export `app_settings.json`, the full SQLite `settings` table, keyring credentials, thumbnail caches, or every user preference.
- [ ] **Use a raw profile backup instead.** Locate the active Sporely app data directory and copy the full set needed to recreate the local state: `mushrooms.db`, `mushrooms.db-wal`, `mushrooms.db-shm` if present, `reference_values.db` if needed, `app_settings.json`, `objectives.json`, and the complete local images directory.
- [ ] **Confirm image path coverage.** Before backing up, open the SQLite `images` table and verify the files referenced by `images.filepath` and `images.original_filepath` live under the configured local images directory, or add those external files to the backup manually.
- [ ] **Use the app export only for its intended workflow.** It is suitable for moving selected observations/images/measurements/calibrations/reference values into another existing profile, but not for restoring an exact cloud-lock test state.

#### Phase 1: Preparation and golden backup
- [ ] **Create test accounts:** Register two disposable Sporely Cloud users, for example `testA+sporely@example.com` and `testB+sporely@example.com`. Use real inboxes or plus aliases that can receive confirmation email unless email confirmation is disabled in the test environment.
- [ ] **Stage local data:** Open the desktop app. Create 2-3 local observations and attach a mix of field and microscope images. Add at least one measurement so measurement cloud IDs are covered. Do not sync yet.
- [ ] **Record local paths:** Note the active database path, images directory, and `app_settings.json` path from the app settings or `database/schema.py` resolution.
- [ ] **Create the golden backup:** Close the app. Copy the raw profile files listed in Phase 0 into a ZIP or separate folder. Restore from this backup whenever a test run needs to start over.
- [ ] **Preflight cloud state:** Confirm both test accounts have no existing observations/media, or delete them through the web Profile deletion flow before starting.

#### Phase 2: Database lock happy path
- [ ] **Log in as Test A:** Open the desktop app and log into the Test A account.
- [ ] **First sync:** Start Sporely Cloud sync and let it finish.
- [ ] **Verify settings lock:** Open `app_settings.json`. Confirm `linked_cloud_user_id` is populated with Test A's Supabase `auth.users.id` UUID, not an email address.
- [ ] **Verify local DB:** Open `mushrooms.db`. Confirm staged observations now have non-empty `observations.cloud_id`, `sync_status = 'synced'`, and `synced_at` populated. Confirm synced local images have `images.cloud_id` and `images.synced_at` populated.
- [ ] **Verify cloud rows:** In Supabase, confirm Test A owns the uploaded `observations`, `observation_images`, and any synced `spore_measurements`.
- [ ] **Verify R2 media:** Confirm uploaded media exists under an R2 key prefixed by Test A's user UUID.

#### Phase 3: Mismatch guardrail
- [ ] **Switch accounts:** Log out of Test A in the desktop app. Log into Test B.
- [ ] **Attempt sync:** Start Sporely Cloud sync.
- [ ] **Verify rejection UI:** Sync should halt before push/pull work and show the critical message: "This local database is permanently linked to another Sporely Cloud account..."
- [ ] **Verify no local relink:** Confirm `app_settings.json` still contains Test A's `linked_cloud_user_id`.
- [ ] **Verify cloud integrity:** Check Supabase and R2 for Test B. There must be no new observations, images, measurements, or media objects from this local database.

#### Phase 4: Reset Cloud Link tool
- [ ] **Trigger reset:** Go to Settings -> Sporely Cloud and click **Reset Cloud Link...**.
- [ ] **Verify warning UI:** Confirm the high-friction critical dialog appears, warns that old cloud data is not deleted automatically, and instructs the user to delete the old account through the web Profile page first.
- [ ] **Cancel path:** Click **Cancel** once. Confirm no DB/settings changes occurred and Test A's `linked_cloud_user_id` remains.
- [ ] **Execute reset:** Open the reset dialog again and click **I have already deleted my old account, proceed with reset**.
- [ ] **Verify logout:** Confirm the desktop is no longer logged into Sporely Cloud.
- [ ] **Verify settings wipe:** Open `app_settings.json`. Confirm `linked_cloud_user_id`, `cloud_last_pull_at`, and `cloud_recent_import_local_ids` are absent or null.
- [ ] **Verify DB wipe:** Open `mushrooms.db`. Confirm all `observations.cloud_id`, `images.cloud_id`, and `spore_measurements.cloud_id` values are `NULL`; confirm observations have `sync_status = 'dirty'` and `synced_at` cleared. In the current desktop schema, local image rows do not store R2 `storage_path`, `image_key`, or `thumb_key`; if such local columns are added later, include them in this verification and reset logic.
- [ ] **Verify sync baselines cleared:** Confirm cloud snapshot/media-signature keys such as `sporely_cloud_snapshot_obs_%`, `sporely_cloud_image_file_sig_%`, and `sporely_cloud_local_media_sig_obs_%` are removed from the local SQLite `settings` table.

#### Phase 5: Migration to the new account
- [ ] **Log in as Test B:** If reset did not already leave the desktop logged out, explicitly log out and log into Test B.
- [ ] **Migration sync:** Start Sporely Cloud sync and let it finish.
- [ ] **Verify new settings lock:** Check `app_settings.json`. `linked_cloud_user_id` should now equal Test B's Supabase UUID.
- [ ] **Verify local DB:** Confirm observations/images/measurements have fresh cloud IDs and `sync_status = 'synced'`.
- [ ] **Verify Test B cloud data:** Check Supabase and R2. Test B should now have a fresh copy of all staged database rows and media.
- [ ] **Verify R2 paths:** Confirm new media keys are under Test B's user UUID path, not Test A's.
- [ ] **Verify Test A unchanged:** Unless already deleted, Test A's cloud rows/media should be unchanged by Test B migration.

#### Phase 6: Restore and cleanup
- [ ] **Restore golden backup:** Close the desktop app. Replace the current raw profile files with the golden backup from Phase 1. Reopen the app and verify the original unsynced local state is back.
- [ ] **Test web account deletion:** Log into Test A in the web app and use Profile -> Delete Account. Confirm the Supabase Edge Function deletes the auth user, owned DB rows, and R2 storage if R2 deletion is part of that function. Repeat for Test B.
- [ ] **Post-cleanup audit:** In Supabase and R2, search both user UUIDs. Confirm no test rows or media objects remain unless intentionally retained for debugging.

#### Phase 7: Automated regression coverage
- [x] **Unit test account lock:** `tests/test_cloud_account_lock.py` covers first-bind, same-account sync, and mismatch rejection without network access.
- [x] **Unit test reset wipe:** `tests/test_cloud_sync_reset.py` covers local cloud reference clearing, app-setting clearing, and preservation of unrelated settings.
- [ ] **Add export coverage test:** Add a focused test that documents the current app export contract: observation/image/measurement/calibration/reference data and image files are included, but `app_settings.json` and full profile state are intentionally not part of the share/import bundle.


## New Shared Priority: AI Crop Sync Between Web, Supabase, and Desktop
*Goal: implement a single AI crop model for Artsorakel across `sporely-web`, Supabase, and `sporely-py`, using the desktop crop schema as the canonical shape.*

### Recommended rollout order
1. **Supabase migration**
   add AI crop fields to `public.observation_images`
2. **Web crop utility**
   extract the reusable crop math/gesture code
3. **Import review crop editor**
   add per-image editing for imported photos
4. **Artsorakel uses cropped blobs**
   apply crop data only for AI requests
5. **Capture overlay**
   add the live default crop rectangle on the web camera screen
6. **Normal review crop editor**
   allow captured photos to be adjusted after shooting
7. **Desktop sync**
   push/pull crop data between Supabase and local SQLite
8. **Cross-platform QA**
   verify the same image’s crop can be edited on web and seen on desktop, and vice versa

### Explicit non-goals for the first pass
- [ ] **Do not crop R2 originals**
- [ ] **Do not make gallery display depend on AI crop**
- [ ] **Do not add a separate AI-crop table in Supabase**
- [ ] **Do not rely on browser camera preview geometry as the only crop source**

### R2 Migration Status
- [ ] **Deploy Upload Worker** — Cloudflare Worker code is in the repo, but still needs deployment, route setup, and R2 binding.
- [ ] **Run R2 SQL Migration in Supabase** — Apply `database/supabase_r2_media_migration.sql` against the live project.
- [ ] **Run Initial Local Media Migration** — Execute the one-time bulk uploader against the full local image library.
- [ ] **Switch Desktop to Worker Uploads** — Optional follow-up if you want desktop uploads to go through the same authenticated Worker instead of direct R2 credentials.

---

## Active Tasks (TODO) - Web & Infrastructure
- [ ] **Deploy Worker Secrets and Route** — Configure `SUPABASE_URL`, optional JWT issuer/audience overrides, `MEDIA_PUBLIC_BASE_URL`, and bind `sporely-media` as the Worker bucket.
- [ ] **Offline Queue** — Wrap R2-bound upload failures in IndexedDB so photos aren't lost when in the field.
- [ ] **Unique Constraints** — Run `database/supabase_unique_constraints.sql` to support high-performance upserts during desktop-to-cloud sync.
- [ ] **Optional cloud summary RPC/view** — Add a Supabase-side per-observation change summary for `observations` + `observation_images` so desktop sync can skip most client-side deep comparison work entirely.


## Active Tasks (TODO) - Automated Testing & Auditing
*Goal: Build an automated safety net to replace the manual 10-point audit checklist and prevent sync regressions.*
- [ ] **Static Analysis** — Introduce `Ruff` and `mypy` for automated linting, formatting, and type-checking. Configure them to fail on dead code, missing variables, and unused imports.
- [ ] **Unit Testing Framework** — Introduce `pytest` for the desktop application.
- [ ] **Core Logic Tests** — Write tests for `cloud_sync.py` (conflict resolution, local media signatures), `image_crop` math, and `utils/r2_storage.py`.
- [ ] **Database Tests** — Create automated tests for local SQLite migrations and CRUD operations in `database/models.py`.

## Active Tasks (TODO) - UI
- [ ] **Implement fine-tue for multi-line segments** — Currently, multi-line does not appear in the preview window. Implement feature to drag each segment node. Show nodes as small dots that highlight with mouse over.
- [ ] **Add hint bar at the bottom of the Measure tab** — Current messages that appear below the Start measuring button should go in the hint bar, same as other tabs. Hint bar should span the whole width of the window.

## Active Tasks (TODO) - image handling
- [ ] **Image rotation** — Fix image import of jpg from the android app: thumbnails in sporely-py shows up rotated 90 deg. counter-clockwise when photo is in portrait mode. Image is rotated correctly when viewed in Prepare images dialog. Rotated 90 dg. cc in Measure tab.
---

## Phase 2: Web-Native Analysis (app.sporely.no)
*Goal: Replicate core analysis insights in a responsive browser environment.*

### A. Data Visualization & QC
- [ ] **Responsive Plotting** — Integrate **Plotly.js** for L × W scatter plots.
- [ ] **Outlier Verification UI** — Link Plotly "click" events to display the R2-hosted 200px thumbnail instantly for QC.
- [ ] **Device Layouts** — Use CSS breakpoints to toggle between "Mobile Gallery" and "Desktop Analysis" views.

### C. Community & Reference Data
- [ ] **Public Dataset Explorer** — Build search interface for public measurements using existing Supabase RPCs.
- [ ] **Taxon Summaries** — Display aggregated statistics (min/max/mean/n) across all public datasets.
- [ ] **Reference Entry** — UI for entering Parmasto-type statistics from literature to overlay on user plots.

---

## Analysis Tab — Desktop Interaction logic
*Status: In Progress*

- [ ] **Multi-select Logic** — Implement Cmd/Ctrl + click for additive selection in `main_window.py` (see internal implementation notes for matplotlib pick_event).
- [ ] **Histogram Additive Selection** — Resolve IDs in bins to allow compound filtering.

---

## Community Spore Data (Supabase/Desktop Sync)
*Status: Active*

- [x] Use dedicated cloud-review dialog.
- [x] Support genus-only search (species optional).
- [x] Separate `spore_data_visibility` from observation visibility.
- [ ] **Remaining:** Return QC metadata in RPC responses.
- [ ] **Remaining:** Add stronger visual distinction for cloud-origin imported sources in the reference panel.

---

## Desktop Ingestion Hub
*Status: Active*

- [ ] **Remaining:** Persist original capture time through more cloud import/export paths if we later want exact cross-device time-window matching without local file reads.
- [ ] **Remaining:** Add richer manual reassignment tools for unmatched images across multiple candidate observations.

---

## Design System Migration — "Slate Lab / Clinical Nocturne"
*Goal: Replace the generic blue-accent Material-adjacent UI with an editorial, scientific design system using organic slate-green tones, editorial typography (Inter + Manrope), tonal surface hierarchy, and no hard borders.*

### Remaining
- [ ] **Phase 6 — Remaining tabs and dialogs** — Apply surface/typography/component patterns to `ui/live_lab_tab.py`, `ui/ingestion_hub_tab.py`, `ui/calibration_dialog.py`, and all other dialogs. Consolidate remaining inline `setStyleSheet()` calls into `styles.py`.

---

---

## Long-Term Goals (Phase 3)
- [ ] **In-Browser Measurement** — Replicate manual spore clicking and calibration using HTML5 Canvas.
- [ ] **Pyodide Integration** — Run existing Python/Numpy measurement logic in-browser to ensure 1:1 math consistency between desktop and web.
