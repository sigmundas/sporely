# Supabase Sync Contract

Status: draft

This document defines the shared-domain contract between `sporely-py` and `sporely-web`.
It is intentionally narrower than the full Supabase schema.

The goal is to keep the desktop app local-first for ingestion and microscopy, while syncing
only the domain data that matters across devices, the web app, and future shared workflows.
No schema or sync code changes are proposed in this document.

## Baseline Sources

- Local desktop schema source of truth:
  - `database/schema.py`
  - `database/models.py`
- Cloud schema source of truth:
  - `sporely-web/supabase/migrations/20260521120000_baseline_live_public_schema.sql`
  - `sporely-web/supabase/migrations/20260521150000_add_observation_identification_ai_metadata.sql`
- The post-baseline AI metadata migration is harmless history redundancy if the baseline export
  already contains those columns. Keep it in the migration history anyway.
- Old migration history is not authoritative.

## Classification Legend

- `sync-required`: should travel desktop <-> cloud as part of the shared domain model.
- `cloud-only`: belongs to the web/cloud layer and should not be mirrored blindly into SQLite.
- `desktop-only`: local workflow, file system, or ingestion state.
- `generated/reference-only`: derived or bundled lookup data, not observation state.
- `future cloud feature`: should likely exist in cloud later, but is not part of the current contract.
- `shared-but-currently-ignored`: exists on both sides or is visible in the data model, but is not
  yet part of the active sync contract.
- `decision point`: intentionally not settled yet; keep the field in the contract and document the
  preferred direction.
- `near-term sync-required schema gap`: missing today, but needed soon for the shared model.
- `future schema gap`: missing from one side even though the product model needs it.

## Device-Local vs Cloud-Synced Boundary

Sporely-py may run on several computers for the same user. The cloud should work as a bridge for shared domain data, not as a mirror of every desktop setting.

### Device-local

These stay local to each computer:

- window and layout settings
- hardware preferences
- local file paths
- import/watch-folder state
- live lab state
- cache paths
- UI preferences
- temporary session state

### Cloud-synced

These should sync because they are shared domain data needed to recreate or continue work across devices:

- observations
- image metadata
- measurement geometry
- calibration records
- calibration photos or calibration image references
- objective/calibration mapping
- visibility/privacy fields
- AI crop parameters
- selected AI result

Cloud sync should not overwrite higher-quality local originals or device-local workflow state. Local originals and local paths remain desktop-owned, while cloud media is used for web display, recovery, and cross-device continuity.

## Contract Principles

- `sporely-web` is the cloud, social, sharing, and publishing layer.
- `sporely-py` is the desktop, local microscopy, ingestion, and analysis app.
- Sync should not mirror the entire Supabase schema.
- Local SQLite primary keys are the desktop identity.
- Cloud row IDs are the cloud identity.
- `desktop_id` and `user_id` are the durable cloud-side sync anchors.
- `cloud_id` is the local link back to the cloud row.
- Date and time can help humans group or recognize observations, but they are not the sync identity.
- If both a local original file and a cloud derivative exist, the local original wins for analysis,
  measurement, export, and re-upload decisions.
- Cloud media is authoritative for cross-device and web display, but should be treated as a derivative
  or cache when a better local original exists.
- Cloud should never silently downsample the desktop source of truth.

## Domain Crosswalk

| Domain | Contract stance | Notes |
| --- | --- | --- |
| Observation identity and grouping | `sync-required` | Use local IDs, `cloud_id`, `desktop_id`, and `user_id`. `date` and capture time are grouping metadata only. |
| Location and privacy | `sync-required` | Latitude, longitude, placename, visibility, `location_precision`, and `spore_data_visibility` all matter. |
| Country and locality | `near-term sync-required schema gap` | Canonical country should be persisted, not only inferred from placename or coordinates. It affects redlist interpretation, biotope/habitat vocabulary, substrate choices, national ecological systems, common names, and external service/source selection. |
| Taxonomy and determination | `sync-required` | Genus, species, common name, species guess, determination method, and uncertainty flags should sync. |
| Comments | mixed | `open_comment` should sync. `private_comment` is a decision point, not something to discard. |
| AI suggestions | mixed | Crop/input parameters are sync-required. Selected result is near-term sync-required. Candidate lists are sync-capable/future-candidate. Raw/debug payloads stay cloud-only/cache unless needed for reproducibility. |
| Images and originals | mixed | Desktop originals stay local. Cloud stores derivatives and cloud media keys. |
| Microscope images | `sync-required` for metadata | Geometry, scale, objective, stain, mount medium, sample type, and contrast matter for analysis. |
| Measurements | `sync-required` | Raw geometry and calibration context must be reconstructable. |
| Calibration | `sync-required, staged implementation` | Calibration records and calibration photos are shared domain data because cloud bridges multiple desktop installs. |
| Analysis/reference comparison data | `future cloud feature` | Should become a shared dataset model, not per-observation duplication. |
| Social and moderation | `cloud-only` | Comments, follows, friendships, shares, reports, and blocks stay in the cloud layer. |
| Ingestion and lab workflow | `desktop-only` | Session logs, transient import state, and file system paths remain local. |
| Taxonomy/reference generation | `generated/reference-only` | Bundled lookup DBs and generated taxonomy assets are not observation sync state. |

## Table Crosswalk

| Local table or asset | Cloud table or asset | Contract stance | Notes |
| --- | --- | --- | --- |
| `observations` | `public.observations` | `sync-required` | Core observation record. Local `id` maps to cloud `desktop_id`. |
| `images` | `public.observation_images` | `sync-required` for metadata | Desktop paths are local-only; cloud stores `storage_path` and derivative bookkeeping. |
| `spore_measurements` | `public.spore_measurements` | `sync-required` | Measurement points and values must round-trip. |
| `calibrations` | `public.calibrations` | `sync-required, staged implementation` | Fields already mostly exist on both sides, but stable sync identity and implementation wiring are not done yet. |
| `calibration_assets` | none | `desktop-only` | Local multi-asset calibration provenance for source photos, working photos, crops, overlays, debug outputs, and reference caches. |
| `spore_annotations` | `public.spore_annotations` | `future cloud feature` | Useful for overlays and ML, but not part of the current sync path. |
| `reference_values.db` / `reference_values` | `public.reference_values` | `generated/reference-only` now, future shared model later | Desktop reference data is local cache / bundled data today. Cloud reference stats are lookup data, not yet a full shared dataset model. |
| `taxon_min`, `vernacular_min`, `scientific_name_min`, `taxon_external_id_min` | `public.taxa`, `public.taxa_vernacular` | `generated/reference-only` | Lookup taxonomy mirrors, not user content. |
| `settings` | none direct | `desktop-only` except sync state | App settings and sync caches stay local. |
| `session_logs` | none | `desktop-only` | Live lab and retrospective ingestion logs stay local. |
| `observation_identifications` | `public.observation_identifications` | `cloud-owned/cloud-only` | Keep cloud-only for now. Do not mirror the full table into SQLite. |
| `comments`, `follows`, `friendships`, `observation_shares`, `reports`, `user_blocks` | same-named cloud tables | `cloud-only` | Desktop should only care about the privacy/visibility fields needed for sync or display. |
| `profiles` | `public.profiles` | `cloud-only` | Desktop may keep linked-account state, but not the whole profile model. |

## Field Ownership Rules

### Observations

- `sync-required`: `date`, `genus`, `species`, `common_name`, `species_guess`, `location`,
  `gps_latitude`, `gps_longitude`, `location_public`, `location_precision`, `visibility`,
  `sharing_scope`, `spore_data_visibility`, `is_draft`, `publish_target`, `uncertain`,
  `unspontaneous`, `determination_method`, habitat fields, `notes`, `open_comment`,
  `interesting_comment`, `source_type`, `citation`, `data_provider`, `author`, `artsdata_id`,
  `artportalen_id`, `inaturalist_id`, `mushroomobserver_id`, `spore_statistics`, `auto_threshold`.
- `decision point`: `private_comment`.
  - Preferred stance: keep it for Artsobservasjoner upload, local private comments, and eventual
    web support.
  - Do not treat it as ignored or disposable data.
- `near-term sync-required schema gap`: `country_code` or canonical country.
  - Placename alone is not enough.
  - Country affects redlist interpretation, biotope and habitat vocabulary, substrate choices,
    national ecological systems, common-name choices, and preferred external sources.
- `future schema gap`: observation-level `captured_at` is present in the cloud baseline but not in the
  local observation table today.
  - Local desktops already use image capture time for grouping and EXIF backfill.
  - If we want observation-level capture time on the desktop, add it deliberately.
- `future schema gap`: `gps_altitude` and `gps_accuracy` are present in the cloud baseline but are
  not currently modeled as local observation columns.
- `future schema gap`: `inaturalist_taxon_id` exists locally but not in the current cloud baseline.
  - Keep this as a deliberate compatibility choice until we decide whether the web needs it.
- `shared-but-currently-ignored`: `ai_state_json` is local and cloud-visible in the schema, but the
  desktop should not mirror the whole raw AI state by default.

### Comments

- `open_comment`: `sync-required`.
- `private_comment`: `decision point`.
- `interesting_comment`: `sync-required` as a lightweight flag, not a separate social comment.
- Cloud social comments are not the same thing as observation comments.
- The desktop should only sync the privacy/display fields it needs for sync or review.

### AI Suggestions and Identifications

AI should not be classified as cloud-only by default.

Separate the AI model into four layers:

| Layer | Contract stance | Notes |
| --- | --- | --- |
| Crop and input parameters | `sync-required` | `ai_crop_x1`, `ai_crop_y1`, `ai_crop_x2`, `ai_crop_y2`, `ai_crop_source_w`, `ai_crop_source_h`, and custom-crop flags should travel with the image metadata. |
| Selected AI result | `near-term sync-required` | Selected service, selected taxon, scientific name, probability, and selection timestamp should be visible across desktop and web. |
| Candidate suggestion list | `sync-capable/future-candidate` | iNaturalist and Artsorakel suggestion lists may be synced later, but they are not required as the primary contract today. |
| Raw/debug response | `cloud-only` or cache | Full service response payloads should stay cloud-side or in a cache unless needed for reproducibility. |

- `public.observation_identifications` stays cloud-owned/cloud-only for now.
- Its AI metadata fields are cloud-only AI metadata:
  - `top_species_url`
  - `top_redlist_category`
  - `top_redlist_status`
  - `top_redlist_source`
- The desktop should be able to display or use the same selected result later, but it should not
  blindly mirror the full identification history table into SQLite.

### Images and Originals

This section defines the local-only provenance vocabulary for image files. It is additive: it
does not replace `image_type`, `filepath`, `original_filepath`, or cloud upload bookkeeping.

- `filepath` is the local working file path.
- `original_filepath` is the preserved source/import path when the source file is kept.
- `source_role`, `file_purpose`, `original_filepath`, `original_mime_type`, and
  `working_mime_type` are local-only provenance fields on `images`; they are not part of the
  current cloud contract.
- `storage_path`, `upload_mode`, `source_width`, `source_height`, `stored_width`, `stored_height`,
  and `stored_bytes` stay cloud bookkeeping, not provenance.
- `notes` should not be used as a hidden file-role flag.

#### `source_role`

`source_role` describes where the file came from and whether it is the durable working copy.

| Role | Meaning | Analysis-authoritative? | Safe to regenerate/delete? | Should sync? | Browser/public display? |
| --- | --- | --- | --- | --- | --- |
| `import_source` | Raw file selected or ingested before conversion | No | Yes, once a durable working copy exists | No | No |
| `local_canonical` | Durable local original/working copy used for analysis | Yes | No | Metadata only | Yes, locally |
| `converted_local` | Local decoded/conversion result from an import source | Yes, when it is the durable working copy | Conditional | Metadata only | Yes, locally |
| `cloud_derivative` | Web-friendly derivative created from decoded pixels or a cloud asset | No | Yes | Yes, cloud-side | Yes, public/browser |
| `cloud_recovery_cache` | Local cache downloaded from cloud to recover a missing file | No | Yes | No | Owner-only |
| `generated_artifact` | Derived output such as a plot, thumbnail, spore crop, or reference derivative | No | Yes | Deferred / optional publish-only | Only if intentionally published |

- `converted_local` is intentionally not a disposable-only label. If it is the durable working copy,
  it can still be authoritative for analysis.
- `cloud_derivative` and `cloud_recovery_cache` are both derived, but only the derivative is a cloud
  sync asset. The recovery cache is a local-only fallback copy.
- `generated_artifact` covers assets that should not be mistaken for canonical scientific originals.
  If they need persistence, they should move to a later artifact table/model rather than the main
  `images` table.

#### `file_purpose`

`file_purpose` describes what the file is for. It does not by itself decide whether the bytes are
authoritative; that comes from `source_role`.

| Purpose | Meaning | Analysis-authoritative? | Safe to regenerate/delete? | Should sync? | Browser/public display? |
| --- | --- | --- | --- | --- | --- |
| `field` | Field photo used as observation evidence | Yes, when paired with `local_canonical` or durable `converted_local` | No for canonical copies | Metadata yes; bytes later no | Yes |
| `microscope` | Microscope image used for measurement and analysis | Yes, when paired with `local_canonical` or durable `converted_local` | No for canonical copies | Metadata yes; bytes later no | Yes |
| `calibration` | Original calibration capture | Yes, when paired with `local_canonical` or durable `converted_local` | No for canonical copies | Metadata yes; bytes later no | Usually local-only |
| `reference` | Calibration reference derivative or other compact reference asset | No | Yes | Yes, as a derivative asset | Yes |
| `plot` | Generated comparison or measurement plot | No | Yes | Publish-only, later if needed | Yes, if published |
| `thumbnail` | UI preview or gallery thumbnail | No | Yes | Usually no | Yes |
| `spore_crop` | Generated crop around a measured spore or evidence point | No | Yes | Deferred / optional publish-only | Only if intentionally published |
| `cache` | Recovery or temporary cache file | No | Yes | No | Owner-only |

- `field`, `microscope`, and `calibration` are the only purposes that should normally carry analysis
  authority, and only when the source role is durable.
- `reference` is explicitly a derivative classification for calibration-side images, not a scientific
  original.

HEIC/import behavior:

- Treat HEIC as an import source, not as a durable working format.
- If a HEIC is decoded to JPEG/PNG for local work, that converted file can become `converted_local`
  and may still be the authoritative working copy.
- Preserve the original source path in `original_filepath` when a converted working copy is
  available.
- If the original HEIC is preserved, remember it separately through `original_filepath` instead of
  overloading `filepath`.
- Cloud uploads should prefer decoded pixels when available so we avoid HEIC -> JPEG -> WebP double
  compression.
- WebP should not become the default durable desktop working format unless it is explicitly tested.

Local canonical vs converted local:

- `local_canonical` is the durable local source of truth.
- `converted_local` is the local decoded/converted file that may become the source of truth when it is
  the only durable working copy.
- Neither label should be inferred from `notes`, cloud `storage_path`, or upload metadata.

Cloud derivative vs cloud recovery cache:

- `cloud_derivative` is a cloud-side display/recovery asset, typically WebP or JPEG.
- `cloud_recovery_cache` is a local-only file restored from cloud storage when the local source is
  missing.
- When the desktop stores a local row for a cloud recovery cache, tag it with
  `file_purpose=cache`.
- A cloud recovery cache must never replace a higher-quality local canonical source.
- If both exist, the local canonical or durable converted local copy wins for analysis, measurement,
  export, and re-upload decisions.

Tombstone interaction:

- Tombstones are deletion state.
- `source_role` and `file_purpose` are provenance state.
- A tombstone should not be treated as a provenance label.
- Cloud recovery/cache files should not create tombstones; they are disposable fallback copies, not
  user-deleted canonical files.
- Tombstones may optionally snapshot provenance later, but that is additive and not required by this
  contract.

Deferred items:

- Cloud provenance fields on `public.observation_images`.
- Full-resolution original sync.
- A dedicated `measurement_artifacts` / `spore_measurement_artifacts` table/model for plots,
  spore crops, thumbnails, and reference derivatives. Keep image thumbnails in `thumbnails`.
- Calibration multi-asset provenance beyond the representative derivative path.

Full-resolution original sync note:

- Current cloud image rows only expose derivative/recovery media fields such as `storage_path`,
  `image_key`, and `thumb_key`.
- A future original-object field such as `original_storage_path` is still needed before the desktop
  can upload or restore full-resolution originals safely.
- Until that contract exists, the desktop policy helper keeps original sync disabled by default and
  only treats canonical local originals as eligible candidates.

- `sync-required`: `sort_order`, `image_type`, `micro_category`, `objective_name`,
  `scale_microns_per_pixel`, `resample_scale_factor`, `mount_medium`, `stain`, `sample_type`,
  `contrast`, `measure_color`, `crop_mode`, `notes`, `gps_source`, `ai_crop_*`.
- `desktop-only`: local file system paths and local source metadata.
  - `filepath`
  - `original_filepath`
  - local import temp paths
  - live lab capture state
  - other ingestion-only file management state
- `cloud-only`: cloud object references and upload bookkeeping.
  - `storage_path`
  - `image_key`
  - `thumb_key`
  - `upload_mode`
  - `source_width`
  - `source_height`
  - `stored_width`
  - `stored_height`
  - `stored_bytes`
- `storage_path` and cloud media keys are cloud media references. The desktop may read them for
  recovery or download, but they are not local source-of-truth.
- `shared-but-currently-ignored`: `scale_bar_x1`, `scale_bar_y1`, `scale_bar_x2`, `scale_bar_y2`.
  - Keep this as a future contract item if the web needs to reproduce the desktop scale-bar overlay
    exactly.

Cloud upload bookkeeping fields are cloud-only unless the desktop actually needs them for a current
feature. At present they function as cloud metadata, not desktop source data.

Conflict rule for images:

- Metadata may sync both ways.
- Cloud image derivatives may be downloaded when local files are missing.
- A lower-quality cloud image must not replace a higher-quality local original.
- If a local file is missing but the cloud image exists, mark the recovered file as cloud-derived,
  recovery, or cache data, not as the canonical original.
- Sync from cloud must never overwrite local `filepath` or `original_filepath` values when those
  point to higher-quality local sources.
- If both exist, the local original wins for analysis, measurement, export, and re-upload decisions.
- Sync should never silently downsample the desktop source of truth.

Cloud derivative rule:

- Cloud media should normally be compressed and web-friendly, typically WebP or JPEG.
- Local users may work with large JPEGs, TIFFs, or uncompressed microscope originals.
- A downloaded cloud copy on a desktop without the original should be treated as a cache or recovery
  file, not as the canonical original.

### Microscope Images and Measurements

- Measurement geometry must sync independently from rendered images.
- Overlays and measurement rectangles must be reconstructable from source geometry plus calibration
  data.
- Spore thumbnails are generated artifacts, not the only source of truth.
- Large microscope originals may remain local-only unless the user explicitly opts into full-resolution
  cloud storage later.
- Full measurement reproducibility is incomplete until calibration sync implementation lands.
  Measurement geometry can sync now, but calibration data are part of the shared contract even
  though the implementation is staged.

`sync-required` measurement fields:

- `length_um`
- `width_um`
- `measurement_type`
- `gallery_rotation`
- `p1_x`, `p1_y`
- `p2_x`, `p2_y`
- `p3_x`, `p3_y`
- `p4_x`, `p4_y`
- `measured_at`

`cloud-only` measurement bookkeeping today:

- `image_key`
- `thumb_key`

`sync-required, staged calibration fields`:

- `objective_key`
- `calibration_date`
- `calibration_image_date`
- `microns_per_pixel`
- `microns_per_pixel_std`
- `confidence_interval_low`
- `confidence_interval_high`
- `num_measurements`
- `measurements_json`
- `image_filepath`
- `camera`
- `megapixels`
- `target_sampling_pct`
- `resample_scale_factor`
- `calibration_image_width`
- `calibration_image_height`
- `notes`
- `is_active`
- These calibration fields are part of the shared contract, but sync implementation is staged.

### Generated Artifacts and Spore Evidence Crops

Generated artifacts are derived render outputs or evidence views. They are useful, but they are not
canonical source images.

Artifact categories:

- `thumbnail`: image-level preview for gallery and browsing
- `spore_crop`: evidence crop around a measured spore
- `plot`: measurement or comparison plot
- `reference`: compact calibration/reference derivative

Current generated-artifact `file_purpose` values are `thumbnail`, `spore_crop`, `plot`, and
`reference`.

Spore evidence crop rules:

- They are derived evidence, not replacements for the original microscope image.
- They are generated from source image pixels, measurement geometry, and calibration context.
- When available, they should reference the source image id/cloud id and measurement id/cloud id.
- If persisted later, they should preserve crop rectangle, measurement geometry, scale/calibration,
  orientation, generation version, MIME type, pixel dimensions, and `generated_at`.
- They should be safe to regenerate whenever the source image still exists.

Model decision:

- Keep `thumbnails` for image-level previews.
- Keep `images` for source, working, and recovery image files.
- Do not store spore crops in `images`.
- If persistent artifacts are needed, add a dedicated `measurement_artifacts` /
  `spore_measurement_artifacts` table later in Stage H.

Source image missing:

- Evidence crops may still preserve useful audit context.
- They do not replace the original microscope image.
- UI/web should mark them as "derived evidence only" when the source image is missing.
- Measurements may remain publishable if geometry and calibration context are intact, but
  reproducibility is reduced.

Deletion behavior:

- Current image deletion cascades to measurements, annotations, and thumbnails for that image.
- Future measured-image deletion should warn before destroying source-linked data.
- A later workflow may offer preserve-vs-purge derived evidence.
- Artifact tombstones and artifact lifecycle should stay separate from source image tombstones.

### Calibration Identity

- Do not use `objective_key + date` as identity.
- Do not use plain `desktop_id` as the cross-machine identity.
- Preferred direction: a stable calibration UUID or sync key scoped by `user_id`.
- `objective_key` is grouping and display metadata, not identity.
- Two similar calibrations for the same objective should remain separate unless they share the
  same stable identity.
- Use `calibration_uuid` as the portable cross-device link for images.
- `calibration_id` stays local-only and can be reconciled from `calibration_uuid` after
  calibration sync.

### Calibration Photos

- Numeric calibration can sync without the original photo.
- Visual inspection or reproduction needs the calibration photo or reference image and
  `measurements_json`.
- Local calibration photos remain authoritative when present.
- Cloud-hydrated calibration photos should be marked derived, cache, or recovery data.
- Do not overwrite higher-quality local calibration photos.
- Calibration-side assets now live in the desktop-only `calibration_assets` table. The current cloud
  contract does not include those rows yet.

### Calibration Implementation Stages

- Define stable calibration identity and payload shape.
- Metadata-only calibration sync.
- Calibration photo or reference sync.
- Image-calibration linkage and reconciliation.
- Only later decide whether hidden `measurement_type = "calibration"` rows need cloud
  representation.

### Country, Redlist, and Locality

- `country_code` or canonical country should be treated as `future schema gap` and future
  `sync-required`, not as a display-only detail.
- Placename plus latitude/longitude is not enough for the product model.
- Country affects:
  - redlist interpretation
  - biotope and habitat vocabulary
  - substrate choices
  - local and national ecological classification systems
  - local common-name choices
  - preferred external services and reference sources
- If a schema only stores placename and coordinates today, document canonical country as missing
  product data, not as a cosmetic enhancement.
- Local `red_list_category` and `red_list_categories_json` should be treated as desktop-side helpers
  until we have a country-aware shared model.
- Cloud redlist data in `observation_identifications` is AI metadata, not the same thing as a canonical
  observation-level redlist field.

### Social and Moderation

Keep these tables `cloud-only`:

- `comments`
- `follows`
- `friendships`
- `observation_shares`
- `reports`
- `user_blocks`

The desktop should only care about the visibility and privacy fields needed for sync or display:

- `visibility`
- `location_public`
- `location_precision`
- `spore_data_visibility`
- `is_draft`

### Ingestion and Local Workflow

These stay `desktop-only`:

- `session_logs`
- local watch-folder state
- live lab session state
- local import matching state
- `folder_path`
- `lab_metadata`
- local temporary file paths
- `artsobs_web_unpublished`

### Taxonomy and Reference Data

- Bundled taxonomy sources and generated lookup DBs are `generated/reference-only`.
- `taxa` and `taxa_vernacular` are cloud lookup mirrors, not user-content sync rows.
- `reference_values` is not yet a shared dataset model. It is a local reference cache / bundled
  dataset plus a cloud lookup table.
- Local `reference_values` currently carries provenance-style metadata that the cloud flat table does
  not fully model.

## Sync Identity Model

- Desktop SQLite primary keys are the local identity.
- Cloud row IDs are the cloud identity.
- `desktop_id` is the cloud-side link back to the local row.
- `cloud_id` is the local-side link back to the cloud row.
- `user_id` scopes cloud rows and prevents cross-account collisions.
- Observation-level sync should be keyed by local `observations.id` and cloud `observations.desktop_id`.
- Image sync should be keyed by local `images.id` and cloud `observation_images.desktop_id`.
- Measurement sync should be keyed by local `spore_measurements.id` and cloud `spore_measurements.desktop_id`.
- Relationships should be resolved by foreign keys and IDs, not by file path or by date.
- Date and capture time may help group imported photos and help humans recognize observations, but they
  are never the primary identity.

## Gaps and Drift

- `private_comment` is a decision point, not ignored data.
- `upload_mode`, `source_width`, `source_height`, `stored_width`, `stored_height`, and `stored_bytes`
  are cloud upload bookkeeping fields. Treat them as cloud-only unless the desktop gains a real feature
  that needs them.
- `observation_identifications` stays cloud-only for now, including the four AI redlist/species URL
  metadata fields.
- `country_code` / canonical country is a near-term sync-required schema gap even though the product
  model needs it.
- Cloud baseline verification:
  - `captured_at`, `gps_altitude`, and `gps_accuracy` are present in the authoritative cloud
    observations table.
  - `camera_model`, `iso`, `exposure_time`, and `f_number` were not found in the authoritative
    baseline migration, even though desktop EXIF helpers still reference them.
  - Treat that as either stale-script risk or missing-cloud-schema risk until it is resolved.
- Local `inaturalist_taxon_id` does not have a matching field in the current cloud baseline.
- Local `red_list_category` and `red_list_categories_json` do not have a matching canonical cloud
  observation field today.
- Local `calibration_id` on images is reconciled from portable `calibration_uuid`; do not use the
  numeric id as cloud identity.
- `spore_annotations` exists on both sides, but the contract should treat it as a future shared
  annotation feature, not as current sync state.
- Cloud image tombstones should be recorded locally and used to block reupload or recreation, but
  the desktop active image row stays visible for now. Files, measurements, and annotations remain
  intact, and any UI hiding or explicit delete confirmation is deferred.
- `scale_bar_*` exists on both sides but is currently shared-but-ignored.
- Current cloud `reference_values` is too flat for shared provenance and usage tracking.

## Analysis and Reference Data Proposal

This is a future shared dataset model proposal only. Do not implement it yet.

Model reference and comparison data as its own versioned asset family, not as duplicated per-observation
blobs.

Proposed cloud-side shape:

- `reference_datasets`
  - dataset metadata
  - uploader / owner `user_id`
  - citation
  - provenance
  - visibility
  - taxon scope
  - mount medium and stain context
  - country scope if relevant
- `reference_dataset_entries` or `reference_dataset_points`
  - raw spore measurements or Parmasto-style inputs
  - summary statistics
  - source references
- `reference_dataset_links`
  - which observations use which shared dataset
  - role or usage context
- `reference_dataset_artifacts`
  - derived plots
  - thumbnails
  - SVG / PNG / JPEG render outputs
  - render parameter hash or cache key

Proposed desktop-side shape:

- Keep `reference_values` as the local cache and bundled reference store.
- Add a local dataset link table only if the desktop needs to remember which cloud dataset was used.
- Keep per-observation analysis settings separate from the shared dataset itself.

Rules for this model:

- Observations should link to a shared reference dataset instead of duplicating the whole dataset.
- Raw reference points and provenance are source data.
- Summary statistics and rendered plots are derived.
- Derived plots are cache or render outputs, not primary source-of-truth data.
- The cloud should eventually be able to show comparison plots, histograms, thumbnails, overlays, and
  selectable spore size ranges without duplicating the whole dataset into every observation row.

## Source-of-Truth Rules

| Area | Source of truth | Notes |
| --- | --- | --- |
| Shared observation, image, and measurement metadata | Cloud schema + sync contract | Cloud is the shared canonical store; the contract defines ownership and transforms. |
| Desktop file paths, ingestion state, and local workflow | Desktop SQLite schema | Local originals and ingestion state belong to the desktop. |
| Field ownership and ID mapping | Sync contract | This document is the contract for what syncs and how. |
| Taxonomy and lookup generation | Taxonomy/reference generation scripts | Bundled lookup DBs and import scripts remain generated artifacts. |
| Shared reference datasets | Future cloud reference-dataset tables | Until then, local `reference_values` remains the working store. |
| Plot artifacts, evidence crops, and thumbnails | Generated artifacts | Rebuild from source data and render parameters whenever possible; keep image thumbnails separate from source images. |

## First Implementation Step

No schema change is required to start.

The safe first step is to keep this contract document under version control and use it as the checklist
for the next sync/schema pass.

If this contract is accepted, the next code change should be the smallest possible field-mapping update
that closes one real mismatch without expanding scope.
