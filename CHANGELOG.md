# Changelog

All notable changes to Sporely are documented here.

## 2026-08-24

### Added
- **Child-change detection via a server-maintained `updated_at` cursor.** Cloud-side edits to `observation_images` / `spore_measurements` whose parent observation is unchanged are now detected by a per-sync keyset probe (`updated_at >= ts`, ordered `updated_at.asc,id.asc`, strict client-side `(updated_at, id) > (cursor_ts, cursor_id)` tuple filter). Parents of changed child rows are pulled via `forced_pull_cloud_ids` — never a blanket `full_pull`. Server side: sporely-web migration `20260824120000` adds `observation_images.updated_at` (historical backfill from server timestamps; unconditional every-role `BEFORE INSERT OR UPDATE` trigger; `(user_id, updated_at, id)` index). The per-leg `(ts, id)` cursor (v2) advances to the maximum inspected tuple only after `pull_all` succeeds; a missing/old cursor bootstraps with one full child reconciliation. (`034703b`, `f546541`, `c0a02ae`)

### Fixed
- **Pull-phase `desktop_id` echo loop.** Every pull PATCHed `set_image_desktop_id` for every already-linked image; the new unconditional `updated_at` trigger turned those ~2 500 no-op writes per sync into next sync's "child changes", forcing a permanent full child re-pull of every observation (confirmed in live testing 2026-08-24). All pull-path relink sites are now guarded by `_remote_image_desktop_id_current` and skip the PATCH when the remote link already matches; stale links still repair. New contract rule: no no-op cloud writes on sync paths. (`592cf1e`)
- **Cursor ids compared as strings.** `'10000' < '9999'` lexicographically, so new same-timestamp rows could be silently dropped once ids crossed a digit-length boundary. Probe filters and cursor advancement now share one numeric total order (`_child_change_cursor_id_key`); the watermark log prints the full `(ts, id)` tuple per leg. Regression: a real 3-page / 2 501-row same-timestamp cohort through the real paginated probe converges in one sync. (`592cf1e`)
- **Child-probe cursor timestamps were not percent-encoded** in PostgREST filter values (`+00:00` decoded as a space, breaking the `gte` comparison). (`2208926`)
- **Local-only-field dirty loop.** Two observations re-dirtied after every pull despite successful pushes: `ai_selected_at` stored locally with a `Z` suffix never string-matched the cloud's `+00:00` form (now compared as instants), and merge-filled AI-selection/red-list values (`_merge_cloud_selected_ai_fields`) protected the cloud row but were never adopted into the local row (now persisted locally after a successful push, so local, cloud, and snapshot converge). (`6c753e7`)

### Notes
- Docs updated: `docs/supabase-sync-contract.md` (child-change detection section; safety rules 24–25; both repository copies), `docs/cloud-sync-architecture.md` (probe in the sync flow, guarded `set_image_desktop_id`, lessons 23–24, test-map rows), `AGENTS.md` cloud-sync invariants.
- Test-fake cleanup: cloud fakes updated for the `remote_row=` keyword on `push_image_metadata` and `recovery_authorized=` on `upload_image_file` — signature-only, resolved ~30 pre-existing failures. (`a7863d1`, `eee56ff`)

## 2026-07-30

### Added
- **Stage 3B.5 desktop Red List runtime integration.** The observation editor now resolves the Norwegian Red List through `TaxonLookupService.get_redlist_lookup_with_overlay` and applies it after every identity commit (Artsorakel apply, explicit picker selection, manual genus/species editing-finished, or a country-change from the reverse-location lookup). Statuses `unique`, `multiple_same_category`, `conflict`, `none` are handled distinctly per the [redlist-overlay](database/taxonomy/docs/redlist-overlay.md) contract; degree-marked categories (`VU°`, `NT°`, `LC°`) are stored and displayed verbatim and never collapse with their undegraded counterparts. Assessment area is derived from ISO-3166-1 (`no` → `Norge`, `sj` → `Svalbard`, else `None`); `None` clears the derived category and preserves the Artsorakel source-snapshot JSON.
- **Manifest-driven taxonomy-v2 artifact resolution.** `utils.taxonomy_v2.ensure_installed` derives the gzip artifact path from `manifest.gz_artifact` (with an unsafe-path guard rejecting absolute paths, `..`, directory separators, and non-`.sqlite3.gz` suffixes). Explicit `gz_path` override remains for tests. The release-specific `TAXONOMY_V2_GZ_PATH` constant has been removed so future release rollovers cannot desynchronise from the manifest.
- **NorTaxa exact-name overlay on the Red List path.** When a bound `sporely_taxon_id` has no assessment for the requested `(area, source_release)`, the runtime consults NorTaxa canonical rows by *verbatim* `canonical_scientific_name` string. A unique matching NorTaxa counterpart with an assessment surfaces the assessment annotated with `overlay_source="nortaxa_name"` and `overlay_taxon_id`. Multiple counterparts, an unassessed counterpart, or a non-`none` primary status disable the overlay. Overlay never alters primary identity.
- **Vernacular Name → Genus autofill.** Typing an unambiguous Norwegian common name into the Name field auto-populates the Genus when the name resolves to exactly one distinct genus across all species. Multi-genus vernaculars (e.g. `trevlesopper`) leave Genus empty. Existing user-typed Genus is never overwritten; a committed picker snapshot is never overwritten; prior Red List badges are preserved (vernacular-only edit invariant).
- **Shared Artsdatabanken concept-link resolver** (`utils/artsdatabanken_link.py`). All three public `/arter/takson/{...}` link builders now resolve the Artsnavnebase scientific-name id to the taxon-concept id via `/Api/Taxon/ScientificName/{name-id}` with a 256-entry LRU cache (900 s negative TTL, 5 s network timeout). On any failure the URL falls back to the NorTaxa name-info page `https://nortaxa.artsdatabanken.no/name-info/{name-id}` — never to `/arter/takson/{name-id}`. Cloud-sync payload assembly passes `network=False` to prevent per-observation network stalls.

### Changed
- **AI Copy accepts genus-only Artsorakel/iNat suggestions.** Rows like `trevlesopper (Inocybe)` now populate Name and Genus and leave Species empty (previously they were refused with "Could not parse genus/species"). Genus-only copies clear any stale species-level Red List badge and skip the deferred red-list resolution (no species → no unambiguous local lookup).
- **"Unidentified" checkbox is no longer auto-checked** merely because identity fields are empty. Fresh observations and existing observations without an identification now open with Name/Genus/Species/Determination fields active. Explicit user-checked Unidentified still disables the fields; the checkbox state is not persisted (empty identity fields already express the saved state on reload).
- **Manual genus/species edit binds identity and refreshes the badge immediately** (previously required Save + Reopen). Uses `VernacularDB.taxon_id_from_scientific` (which returns `None` on multi-canonical). When multi-canonical resolution is required, `_resolve_manual_via_source_system_preference` prefers COL as the canonical authority (source-system preference, not name-based hardcoding); if only NorTaxa candidates exist, one NorTaxa row still binds. Genuinely ambiguous cases require the picker.
- **Same-category collapse in `get_redlist_lookup` keys on `(category_code, category_is_downgraded)`.** Differing rank / assessed name / criteria / expert group / explanation no longer promote an agreed category to `conflict` — they only mean the representative's metadata is not authoritative for those fields. `[VU, VU°]` still returns `conflict`; `[VU, VU]` differing rank returns `multiple_same_category`.
- **`create_observation` and `update_observation` persist `sporely_taxon_id`** interactively (not just via the offline backfill). Save/reopen restores the same primary identity so the Red List refresh is stable across sessions.

### Fixed
- **Red List badge did not refresh on manual `Cantharellus cibarius` entry.** Two layers: identity resolution refused multi-canonical, and the primary COL id has no Norwegian Red List assessment. Fix uses COL source-system preference for identity and consults the exact-name NorTaxa counterpart only to surface the assessment. Verified against `tax-2026.07.30-02`: COL `168873` is bound; overlay finds NorTaxa `626243` with the LC-Norge assessment.
- **Deferred Red List resolution rejects stale callbacks** using a monotonic `_redlist_generation` counter bumped on every schedule, identity clear, observation load, and dialog close. Prevents an in-flight resolve from writing into a different observation loaded into the same dialog even when the four identity signals happen to coincide.
- **Order-dependent RAW discard test.** A stray focused `QPushButton` leaked from a prior test broke `LiveLabTab._raw_review_shortcut_allowed`. The Python-side text-input focus guard now scopes to widgets inside the tab, matching Qt's `WidgetWithChildrenShortcut` context — behaviour-preserving in production.
- **Cloud partial-update Red List preservation** (`5eeb94e`) and **cloud media deletion client identity** (`d5f8e9b`). See commit bodies.

### Notes
- **Cortinarius vs Aureonarius `limonius` audit.** COL treats `Aureonarius limonius` (B2NK4) as accepted with `Cortinarius limonius` (YLCZ) as its synonym; NorTaxa keeps the reverse (52796 accepted, 297477 synonym). Both are separately compiled into `taxon_min` with distinct `sporely_taxon_id`s (139099 for COL, 624905 for NorTaxa) and no cross-source mapping exists in the release. Runtime does not silently unify them; the picker exposes the disagreement via `link_kind: synonym_of_accepted`. Regression tests in `tests/test_taxonomy_v2_cortinarius_limonius_audit.py` pin this disposition. Proper unification is a compile-pipeline / cross-source mapping concern.

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

Full design and stage-by-stage progress notes live in the
[completed spore-statistics plan](docs/plans/completed/2026-07-13-spore-statistics-species-profiles.md).

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
