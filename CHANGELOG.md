# Changelog

All notable changes to Sporely are documented here.

## 2026-04-06

### Added
- **Community spore search**: search by genus only (species now optional) — results list includes species name so you can distinguish entries across species.
- **Spore data visibility per observation**: new "Spore data sharing" collapsible section in the Analysis tab sidebar. Each observation can be set to Public (default), Friends only, or Private. The setting is synced to and from Sporely Cloud.
- **Spore measurement sync**: spore measurements are now pushed to Sporely Cloud during observation sync. Measurements are upserted by desktop ID so repeated syncs are safe. Requires running `database/supabase_spore_measurements_sync.sql` in the Supabase SQL editor once.
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
