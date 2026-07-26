# Changelog

All notable changes to Sporely are documented here.

## 2026-07-12

### Added
- **Structured observation-level spore summaries.** Sporely-py now computes deterministic per-observation summary rows (min / p05 / mean / median / p95 / max / sample SD for length, width, Q, plus paired/length/width/spore counts) directly from the raw `spore_measurements` table joined with each image's preparation context (`mount_medium` / `stain` / `sample_type` / `contrast`). Multiple contexts on one observation produce multiple summary rows keyed by a deterministic SHA-256 `context_hash`. Canonical Lm/Wm/Qm are always mean-of-paired; Qm is the mean of individual length_i/width_i ratios, never `Lm/Wm`.
- **Cloud table `public.observation_spore_summaries`** in the shared Supabase project. Structured summaries are upserted from `sporely-py` on each cloud sync, keyed by `(observation_id, context_hash)`. Existing observations backfill automatically on the next sync. Owner-only RLS on the table; public reads go exclusively through the new RPC below.
- **Public RPC `get_public_observation_spore_summaries(p_observation_ids, [p_sample_type, p_mount_reagent, p_stain_reagent, p_contrast_method])`** returns structured measured summary rows for public/community-visible observations, respecting `is_draft`, `can_read_observation`, and `can_access_spore_data`. Optional context filters narrow rows to a single preparation. Every non-empty filter must match the SAME summary row — cross-row satisfaction is not possible.
- **Observation-balanced species profiles on the landing site.** `ExploreSporePanel` now fetches structured measured summaries for the observations on screen, aggregates them with unweighted arithmetic means (Parmasto-style), and shows canonical Lm/Wm/Qm plus between-observation SD, contributor count, and profile status (`Insufficient measured data` / `Provisional` / `Community-supported` / `Strong`). Rendered `—` for Mean cells when no eligible measured summaries exist.
- **Legacy fallback preserved** for older observations whose only spore data lives in `observations.spore_statistics` text. The landing parser wraps them as `meanSource='legacy_text'` rows and never fabricates Lm/Wm from p05/p95 midpoints. Only `meanSource='measured'` rows contribute to the canonical species profile.

### Changed
- **Removed spore-count-weighted canonical means.** `poolSporeSummaries` in the landing bundle no longer emits `length_mean_um` / `width_mean_um` / `q_mean` — those were spore-count weighted and violated the Parmasto-style rule that a specimen with 300 spores must not dominate a specimen with 20. Canonical means now come exclusively from `poolObservationSporeSummaries` (unweighted arithmetic mean across eligible observation/context means). The pool function's remaining role is envelope ranges only.
- **`observations.spore_statistics` literature text is untouched** — the writer still emits it and existing displays continue to render it verbatim. It is no longer parsed to invent means for canonical profiles.

### Fixed
- **Metadata-only microscope image leak.** `search_public_observation_images` and `get_public_observation_images` no longer return metadata-only microscope anchors (`storage_path IS NULL`) as gallery images. Those anchors continue to contribute measurements, summaries, and mosaic tiles — they just do not surface as displayable images. `storage_exif_safe` / `fullUrl` gate preserved verbatim.

### Migrations (Supabase, in this order)
1. `20260712120000_add_observation_spore_summaries.sql`
2. `20260713120000_add_public_spore_summary_rpc.sql`
3. `20260714120000_add_context_filters_to_public_spore_summary_rpc.sql`
4. `20260714130000_fix_search_public_observation_images_hide_metadata_only.sql`

All four are forward-only and backward-compatible: older desktop clients keep syncing without writing to the new table; older landing bundles keep working via `DEFAULT NULL` on the new filter args.

### Not shipping in this release
- Parmasto-style matcher (planned as a separate stage; no z-score / Mahalanobis / distance scoring yet).
- Removal of the legacy `observations.spore_statistics` string.
- Historical backfill scripts — backfill happens automatically as users upgrade sporely-py.

Full design and stage-by-stage progress notes live in [docs/spore-statistics-species-profiles.md](docs/spore-statistics-species-profiles.md).

## 2026-06-22

### Performance
- **Cloud sync preflight**: instrumented and sped up the no-change sync pause. EXIF backfill now uses a per-file signature cache (skips unchanged files without opening them); remote measurement/image fetches batch by 100 IDs instead of 50 (halves request count); added scoped timing logs and truthful UI progress text for each preflight sub-step.

### Fixed
- **QThread shutdown**: `ObservationsTab.shutdown()` now interrupts, waits for, and parks the Artsobs mobile-link-check worker, fixing the "QThread: Destroyed while thread is still running" warning on close.

## 2026-04-21 (v0.7.6)

### Removed
- **Legacy Mobile Uploads**: Removed the dead `ArtsobsMobileUploader` and `ArtsObservasjonerClient` classes. Sporely now exclusively uses the web-based form submission strategy for Artsobservasjoner.
- **Dead Code**: Cleaned up unused EXIF helpers (`get_camera_settings`), unused image format checks (`is_raw_format`), and empty UI stubs left over from the `v0.7.5` refactoring pass.

## 2026-04-06

### Added
- **Community spore search**: search by genus only (species now optional) — results list includes species name so you can distinguish entries across species.
- **Spore data visibility per observation**: new "Spore data sharing" collapsible section in the Analysis tab sidebar. Each observation can be set to Public (default), Friends only, or Private. The setting is synced to and from Sporely Cloud.
- **Spore measurement sync**: spore measurements are now pushed to Sporely Cloud during observation sync. Measurements are upserted by desktop ID so repeated syncs are safe. Requires running `../sporely-web/supabase/migrations/supabase_spore_measurements_sync.sql` in the Supabase SQL editor once.
- `spore_data_visibility` column added to local SQLite `observations` table; migrated automatically on first launch.
- `cloud_id` column added to local SQLite `spore_measurements` table to track which rows have been synced; migrated automatically on first launch.

### Fixed
- Crash when closing the community search dialog while a search or detail fetch was in progress. Root cause: custom `finished` signals on `_CloudSearchWorker` / `_CloudDetailWorker` shadowed `QThread.finished`, causing the PySide6 wrapper to be garbage-collected while the OS thread was still running. Signals renamed to `search_done` / `detail_done`; workers are now kept alive until `QThread.finished` fires after `run()` returns.
- False conflict on first sync after pulling a new observation from the cloud. The snapshot stored after pull was built from pre-pull image metadata (without `desktop_id` values), so the next sync saw a key shift from `cloud:<id>` to `desktop:<id>` as a conflict. The snapshot is now refreshed from the cloud after all `set_image_desktop_id` calls complete.
- Same snapshot staleness fixed for the existing-observation update path during pull.

## 2026-04-02

### Added
- Automatic Sporely Cloud sync on startup and from **Refresh** in the Observations tab.
- Shared publish-content support for cloud uploads, including checked gallery images, measure plot, thumbnail gallery, and species plate export.
- Persistent cloud-import star marking on imported local observations until a later pull imports newer cloud observations.

### Changed
- Sporely Cloud sharing is now controlled from the global **Online publishing** settings instead of per observation.
- Cloud pulls now import observation images into the local database and generate local thumbnails immediately.
- The Observations table now shows cloud imports as normal local observations instead of temporary cloud-only rows.

### Fixed
- Prevented cloud observations from appearing briefly as one row and then reappearing elsewhere after startup sync.
- Synced local observation deletion now also removes the linked cloud copy when possible.

## 2026-03-30

### Added
- Full app rename from **MycoLog** to **Sporely**, including renamed app assets, build outputs, installer metadata, and translation file names.
- Legacy storage migration for app data, window/settings state, secure login entries, and saved path references so existing installs can move from `MycoLog` storage to `Sporely`.
- Unified taxonomy lookup improvements with merged Norwegian and Swedish vernacular names, scientific-name aliases/synonyms, and mixed-name lookup in the observation editor.
- Swedish Artportalen support in the observation workflow, including publish-target handling and `AP.se` links alongside `AdB.no` in AI suggestions.
- Persistent AI lookup state for observations, so saved suggestions reopen with the observation instead of requiring a new lookup each time.

### Changed
- Measure overlays can now use configurable rectangle appearance styles in the Measure tab, with the same styling shared in Fine tune and the Analysis gallery.
- Measure/Analysis galleries gained stronger keyboard and resize behavior, including `Tab` / `Shift+Tab` image navigation and splitter-based gallery resizing in observation dialogs.
- Observation taxonomy entry now supports searching scientific and vernacular names together while still keeping the internal scientific/common-name fields needed for uploads.
- Analysis defaults now favor `Spores` as the active category, while the old `All` view is now `All except spores`.
- The Analysis sidebar now uses Qt accordion-style sections for plot/reference controls, with gallery settings kept separate at the bottom.

### Fixed
- Migrated legacy database path settings and stored absolute file paths so observations, thumbnails, calibration images, and image folders continue to resolve after the rename.
- Corrected several measurement overlay issues, including text halo alignment, rectangle color matching for dark palette colors, and consistent thick/thin rectangle switching.
- Resetting analysis filters now clears the selected scatter-point highlight as expected.
- Manual location names in Edit Observation are no longer overwritten by reverse-geocode lookups; the API-provided name can be reapplied explicitly with `Get name`.

## 2026-02-08

### Added
- Field measurement category and a 2-click Line tool for length-only measurements.
- Sampling assessment for microscope images based on objective NA.
- Calibration history metadata: camera, megapixels, and overlay export.
- Multiple reference datasets in Analysis with a plot table and clear labels.

### Changed
- Objective definitions now use Magnification + NA + Objective name.
- Gallery tags show magnification and contrast (e.g. `63X DIC`).
- Scale mismatch warnings appear when image resolution differs from calibration.

### Fixed
- Updated calibration megapixels to reflect full image size instead of crop area.
- Various UI and workflow refinements in Prepare Images and Analysis.

## 2026-02-05

- Auto calibration workflow improvements.

## 2026-02-02

- AI species lookup via Artsorakelet (initial support).
