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

## 🚨 Immediate Priority: Cloudflare R2 Storage Migration
*Goal: Move all image hosting from Supabase Storage to Cloudflare R2 to secure a 10GB free tier and zero egress fees before scaling to more users.*

---

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
