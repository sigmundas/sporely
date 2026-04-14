# Sporely Development Plan (Merged Desktop & Web)

## 🚨 Immediate Priority: Cloudflare R2 Storage Migration
*Goal: Move all image hosting from Supabase Storage to Cloudflare R2 to secure a 10GB free tier and zero egress fees before scaling to more users.*

---

## New Shared Priority: AI Crop Sync Between Web, Supabase, and Desktop
*Goal: implement a single AI crop model for Artsorakel across `sporely-web`, Supabase, and `sporely-py`, using the desktop crop schema as the canonical shape.*

### Existing desktop foundation
- [x] **Desktop already stores AI crop data locally** — `sporely-py` `images` rows already have `ai_crop_x1`, `ai_crop_y1`, `ai_crop_x2`, `ai_crop_y2`, `ai_crop_source_w`, `ai_crop_source_h`.
- [x] **Desktop already has an AI crop UI** — `ui/image_import_dialog.py` supports drawing and editing an `AI crop` region for Artsorakelet.
- [x] **Desktop already uses normalized coordinates** — crop rectangles are stored image-relative rather than in screen pixels.
- [x] **Desktop already passes crop geometry into AI preparation** — `AIGuessWorker` crops before sending to Artsdatabanken AI when a crop exists.

### Product decision
- [x] **Use an editable AI crop rectangle, not "capture only visible area"** — original images remain intact; crop metadata is stored separately.
- [x] **Default live framing overlay on web capture** — show a centered default crop rectangle on the browser camera preview, but do not destructively crop the saved asset.
- [x] **Same crop workflow for imported images** — imported device photos need the same AI crop editing flow as live captures.
- [x] **One crop model everywhere** — web and cloud now align to the desktop field names and semantics.

### Canonical data model
- [x] **Per-image crop fields**
  `ai_crop_x1`, `ai_crop_y1`, `ai_crop_x2`, `ai_crop_y2`
- [x] **Per-image source dimensions**
  `ai_crop_source_w`, `ai_crop_source_h`
- [x] **Semantics**
  normalized `0..1` coordinates relative to the original image orientation currently stored for that row
- [x] **Null behavior**
  null means no explicit AI crop has been set

### Supabase schema plan
- [x] **Add AI crop columns to `public.observation_images`** — match the desktop local schema exactly.
- [x] **Author a dedicated migration SQL file** — place it under `sporely-py/database/` with the other shared Supabase migrations.
- [x] **Backfill strategy**
  leave existing rows null; no destructive inference of crop rectangles for old images
- [x] **Update docs**
  add these fields to `sporely-web/SUPABASE_DB.md` and `sporely-web/ARCHITECTURE.md`

### Web app implementation plan
- [x] **Create a reusable crop editor module**
  base it on the existing avatar crop gesture handling in `sporely-web/src/screens/profile.js`
- [x] **Store crop metadata per image in client state**
  captures and imported files need per-image AI crop state, not just per-observation state
- [x] **Import review first**
  add a full-screen image review/editor with crop rectangle support to `src/screens/import_review.js`
- [x] **Normal review second**
  add the same editor to `src/screens/review.js`
- [x] **Capture overlay guidance**
  add a default crop rectangle to `src/screens/capture.js` and pre-seed new captures with that normalized crop
- [x] **Crop before AI requests**
  update `src/artsorakel.js` so the browser sends a cropped blob to Artsorakel when crop metadata exists
- [x] **Do not store a separate cropped file**
  crop is a transient AI input, not a replacement for the original upload

### Desktop cloud sync plan
- [x] **Push local AI crop metadata to Supabase**
  when desktop images are uploaded/synced, include the AI crop fields in `observation_images`
- [x] **Pull cloud AI crop metadata into local desktop SQLite**
  when pulling field observations from the cloud, hydrate local `images.ai_crop_*` columns
- [x] **Preserve crop metadata during desktop image transforms**
  rotations/crops already adjust local AI crop data; synced values must reflect the post-transform geometry
- [x] **Ensure conflict behavior is sane**
  decide whether last-write-wins is acceptable for AI crop metadata or whether cloud/local conflicts need explicit resolution
- [x] **Reduce false-positive media conflicts**
  order-only image changes, mtime-only file churn, local gallery layout, and older pre-AI-crop media signatures no longer force cloud conflicts
- [x] **Reduce startup re-check churn**
  desktop sync now prefilters cloud observations using local lookup caches plus a small `updated_at` vs `synced_at` grace window so same-sync timestamp skew does not re-trigger every observation on restart
- [x] **Speed up Keep desktop**
  resolving a conflict skips image re-upload work when there are no meaningful media changes left to push

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
- [x] **Configure R2 Bucket** — `sporely-media` exists in Cloudflare.
- [x] **Desktop Sync Engine Rewritten** — `sporely-py` now uploads, downloads, and deletes media in R2 while keeping relative keys in Supabase metadata.
- [x] **Local Thumbnail Logic** — Desktop sync still generates local `small` and `medium` thumbnails before upload.
- [x] **Local-to-R2 Initial Sync Script Added** — `tools/migrate_images_to_r2.py` can bulk-upload the local library using the cloud sync key layout.
- [x] **Database Migration Authored** — `database/supabase_r2_media_migration.sql` adds `image_key` and `thumb_key` columns and normalizes old storage paths.
- [x] **Community RPC Payloads Updated** — Community dataset SQL now exposes `image_key` and `thumb_key` for web QC and plotting work.
- [x] **Domain Roles Clarified** — `media.sporely.no` serves public media reads from R2, while authenticated uploads belong on the Worker endpoint. The deployed `workers.dev` URL can be used before a custom `upload.sporely.no` route exists.
- [ ] **Deploy Upload Worker** — Cloudflare Worker code is in the repo, but still needs deployment, route setup, and R2 binding.
- [ ] **Run R2 SQL Migration in Supabase** — Apply `database/supabase_r2_media_migration.sql` against the live project.
- [ ] **Run Initial Local Media Migration** — Execute the one-time bulk uploader against the full local image library.
- [ ] **Switch Desktop to Worker Uploads** — Optional follow-up if you want desktop uploads to go through the same authenticated Worker instead of direct R2 credentials.

---

## Active Tasks (TODO) - Web & Infrastructure
- [ ] **Deploy Worker Secrets and Route** — Configure `SUPABASE_URL`, optional JWT issuer/audience overrides, `MEDIA_PUBLIC_BASE_URL`, and bind `sporely-media` as the Worker bucket.
- [ ] **Supabase Heartbeat** — Set up a GitHub Action to ping the database every 4 days to prevent the 1-week auto-pause on the Free Tier.
- [ ] **Offline Queue** — Wrap R2-bound upload failures in IndexedDB so photos aren't lost when in the field.
- [ ] **Unique Constraints** — Run `database/supabase_unique_constraints.sql` to support high-performance upserts during desktop-to-cloud sync.
- [ ] **Optional cloud summary RPC/view** — Add a Supabase-side per-observation change summary for `observations` + `observation_images` so desktop sync can skip most client-side deep comparison work entirely.

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

- [x] **QR-based Sync Shot** — desktop ingestion now uses a live QR timestamp with 1-second precision, 2-second cadence, and a 0.1 second blank frame between codes.
- [x] **Sync Shot auto-detect** — folder scans now auto-check the first and last image from each folder for the active Sync Shot QR, while keeping `Use image...` as a manual fallback.
- [x] **Mixed field + microscope matching** — one scanned folder can now match field images by nearby observation times and microscope images by retrospective Live Lab session logs.
- [x] **Per-image local capture time** — desktop now stores `images.captured_at` locally and lazily backfills older rows when time-window matching needs them.
- [x] **Visible same-tab tolerances** — field and microscope match tolerances are now adjustable directly inside the Ingestion Hub instead of being buried in global settings.
- [x] **Lighter scan pass** — batch scan now reads image datetime first instead of full metadata, reducing unnecessary import latency.
- [ ] **Remaining:** Persist original capture time through more cloud import/export paths if we later want exact cross-device time-window matching without local file reads.
- [ ] **Remaining:** Add richer manual reassignment tools for unmatched images across multiple candidate observations.

---

## Design System Migration — "Slate Lab / Clinical Nocturne"
*Goal: Replace the generic blue-accent Material-adjacent UI with an editorial, scientific design system using organic slate-green tones, editorial typography (Inter + Manrope), tonal surface hierarchy, and no hard borders.*

### Completed
- [x] **Phase 1 — Color tokens** — `ui/styles.py` `get_style()` updated to Slate Lab (light) and Clinical Nocturne (dark) palettes. `apply_palette()` updated with matching `QPalette` values. `ui/hint_status.py` progress bar and state colors updated.
- [x] **Phase 2 — Typography** — Manrope (headlines/section headers) and Inter (body/data) loaded via `QFontDatabase` in `main.py`. Font families registered as `'Manrope'` and `'Inter 18pt'`. All QSS font references updated.
- [x] **Phase 3 — Surfaces/borders** — `QGroupBox` border removed, rounded 8px. Inputs use Soft Box style (tonal bg, no border, focus underline). Tab navigation seamless (selected tab merges with pane background). `QSplitter` handle hidden. No 1px separator lines.
- [x] **Phase 4 — Core components** — Buttons 8px border-radius with gradient. `QPushButton:disabled` uses `dis_bg`/`dis_fg` tokens. SpinBox arrows hidden. `QDateEdit`/`QDateTimeEdit` Soft Box style with calendar popup. `QGroupBox#dialogSection` for dialog shell sections. Category toggle buttons segmented. `QPushButton[sourceActive]` property for EXIF source highlight.
- [x] **Phase 5 — Observations tab** — Table grid lines removed, alternating rows off, selection uses `sel_bg` (mint). Side panel `#sidePanel` tonal bg. Calendar popup: single-letter day headers (`setHorizontalHeaderFormat(SingleLetterDayNames)`), minimum size 300×240. Table item padding override prevents calendar cell clipping.
- [x] **Prepare Images dialog** — Outer "Image settings" and "Import details" `QGroupBox` wrappers replaced with `QLabel#sectionHeader` plain headers. Inner groups retained. Inline stylesheets removed from "Set from current image" button.
- [x] **`ui/delegates.py`** — `SpeciesItemDelegate` highlight color updated from blue to `primary_container` green.

### Remaining
- [ ] **Phase 6 — Remaining tabs and dialogs** — Apply surface/typography/component patterns to `ui/live_lab_tab.py`, `ui/ingestion_hub_tab.py`, `ui/calibration_dialog.py`, and all other dialogs. Consolidate remaining inline `setStyleSheet()` calls into `styles.py`.

---

## Cloud Sync — EXIF & File Integrity Fixes
*Completed 2026-04-14*

- [x] **EXIF stripping by web 2MP conversion** — Web app (free tier) strips all EXIF when converting images to 2MP before uploading to R2. The "Set from current image" button in the Prepare Images dialog was always disabled for cloud-synced images because `_current_exif_datetime`, `_current_exif_lat`, and `_current_exif_lon` were all `None`.
  - **Fix (desktop side):** `_inject_obs_exif_into_field_image()` in `cloud_sync.py` writes observation GPS/date back into downloaded JPEG EXIF.
  - `_backfill_missing_exif_on_cloud_images()` retroactively patches existing cloud images on next sync.
  - **Fix still needed (web side):** Extract GPS/datetime from native EXIF *before* the Canvas 2MP resize, store in the database row, and/or re-inject into the saved JPEG. See `sporely-web PLAN.md` Phase 4 Metadata Preservation task.
- [x] **Local full-res overwrite** — `_sync_existing_remote_image_to_local()` was unconditionally replacing local full-res desktop-imported images with smaller cloud 2MP versions on every sync.
  - **Fix:** File size comparison — if local file is larger than downloaded cloud copy, keep local and only update DB metadata. The local full-resolution original is preserved.

---

## Long-Term Goals (Phase 3)
- [ ] **In-Browser Measurement** — Replicate manual spore clicking and calibration using HTML5 Canvas.
- [ ] **Pyodide Integration** — Run existing Python/Numpy measurement logic in-browser to ensure 1:1 math consistency between desktop and web.
