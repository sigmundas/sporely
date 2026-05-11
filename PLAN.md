# Sporely Development Plan (Merged Desktop & Web)

## Refactor & Audit Tasks
- [ ] **Make room for text on measure type radio buttons** — Currently, Multi-line text is cut off. Same with Square, choice for Reference shape on Analysis tab. Perhaps rename to Shape instead of Reference shape.
- [ ] **Table highlight*** — There are some lines appearing inside the table cells when selected. Like the AI suggestions table: clicking it make a light-gray ine appear over the text. Possibly some cell frame that has collapsed into a line. Observations table shows a grey rectangle in the cell that is clicked - it appears to have a grey gradient fill. No need for this. The highlight in Measurements on Measure tab appears good. Use the same style for highlight on other tables: define that color in css. Apply same in all other tables.

## Cloud Sync Follow-Up
- [ ] **Run live cloud-lock QA:** Use two disposable Sporely Cloud accounts to verify the implemented account lock, login-time mismatch block, mismatch dialog, Reset Cloud Link flow, R2 ownership paths, and web Profile deletion cleanup against the real Supabase/R2 environment.
- [ ] **Account migration guardrail:** Design a safer user-facing migration flow before encouraging users to delete/recreate accounts. Resetting the local cloud link must remain explicit, should not delete remote data, and should explain duplicate-risk versus data-loss-risk clearly.
- [ ] **Profile parity QA:** Verify desktop Profile & Cloud saves `username`, `display_name`, `bio`, and `avatar_url` to Supabase `profiles`, and that the web Profile shows the same values. Confirm local `profile_email` follows the cloud auth email when signed in.
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
- [ ] **Broaden pytest coverage** — Keep expanding the existing pytest suite around cloud sync conflict resolution, local media signatures, image crop math, and `utils/r2_storage.py`.
- [ ] **Database Tests** — Create automated tests for local SQLite migrations and CRUD operations in `database/models.py`.
- [ ] **Test metadata auto-merge:** Verify that concurrent image metadata changes (e.g., desktop adds measurements, cloud updates image sizing) auto-merge smoothly in favor of the desktop without showing the conflict dialog.
- [ ] **Test true conflict dialog triggers:** Verify that overlapping edits to the same observation text fields (e.g., Notes, Species) correctly trigger the conflict dialog.
- [ ] **Test cloud deletion conflict:** Verify that if an image is deleted on Sporely Cloud but still exists locally, the sync engine pauses and prompts the user for review.

## Active Tasks (TODO) - UI
- Intestion tab: change name to Camera import
- Sync shot: rename to Camera time offset
- Microscope sessions: rename to Live lab sessions
- Change the order of groups in the left tab on Camera import tab: Import folder at the top, Camera time offset, Live lab sessions, then Actions.
- Hint text for buttons:
   * Browse: Select folder with camera images 
   * Scan folder: Find images that match Live Lab sessions or observation time stamps
   * New Sync shot: Calculate camera time offset to match images by date stamp
   * Use image: Manually select Sync Shot
   * Clear: Clear current Sync Shot settings and image data
   * Refresh matches: Refresh against sync data
   * Add selected images: Add to matched observaiont. Select a thumbnail or multi-select (shift+click for range, ctrl+click for single pick)
   * Add all images: Add all images to matched observation(s)
- [ ] **Implement fine-tue for multi-line segments** — Currently, multi-line does not appear in the preview window. Implement feature to drag each segment node. Show nodes as small dots that highlight with mouse over.
- [ ] **Add hint bar at the bottom of the Measure tab** — Current messages that appear below the Start measuring button should go in the hint bar, same as other tabs. Hint bar should span the whole width of the window.
- [ ] **Height of thumbnail image galleries** — These change sometimes when code changes, and I don't know why. They should all be user adjustable, and the thumbnails should reduce in size down to a reasonable small view, say 100px. Currently, the Prepare Images dialog has a gallery that is too small verticaly, and the thumbnails are cropped so 60% is hidden. User cannot resize.

## Active Tasks (TODO) - image handling
- [ ] **Image rotation** — Fix image import of jpg from the android app: thumbnails in sporely-py shows up rotated 90 deg. counter-clockwise when photo is in portrait mode. Image is rotated correctly when viewed in Prepare images dialog. Rotated 90 dg. cc in Measure tab.

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

---
# Phase 7: Privacy, Social Feeds, and Costs

## Phase 7: Privacy, Social Feeds, and Costs

### Active Implementation Tasks
- [ ] **Apply current Supabase delta in staging/live:** Run the latest `database/supabase_phase7_transparency_social_trails.sql` after the already-applied base SQL, then verify it against current production tables and policies.
- [ ] **Verify live RLS/feed behavior:** Test owner, accepted friend, stranger, blocked user, banned profile, and non-public limit paths with disposable accounts.
- [ ] **Strip GPS EXIF from public media serving path:** Ensure the Cloudflare public image path cannot leak embedded GPS metadata.

### Growth Features
- [ ] Implement "Export to iNaturalist" with the `sporely.no` deep link.
- [ ] Implement Bluesky "Share Card" generator.
