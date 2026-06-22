# Sporely Desktop (sporely-py) — Technical Spec

## Architecture Overview
A Python-based desktop application (PySide6) for field observations, microscopy calibration, spore measurements, and cloud synchronization. Acts as the local "source of truth" and offline workstation, while optionally syncing with Sporely Cloud (`sporely-web`).

## Directory Structure
- `ui/`: PySide6 interface. Driven by `main_window.py` containing tabbed widgets (`observations_tab`, `measure_tab`, `analysis_tab`, `live_lab_tab`).
- `database/`: SQLite schemas and access models (`schema.py`, `models.py`). Handles local CRUD.
- `utils/`: Integrations. Includes `cloud_sync.py` (REST sync) and `artsobs_uploaders.py` (external publishing).

## RAW Processing
- RAW detection is suffix-based (`utils/raw_detection.py`), while decoding is lazy-imported through `rawpy`/LibRaw (`utils/rawpy_import.py`).
- `utils/raw_render.py` uses `rawpy`, `numpy`, and `Pillow` to decode RAW data, then applies the shared post-decode pipeline in `utils/image_processing_pipeline.py`.
- RAW sources render to a local JPEG derivative: `.jpg`, quality `95`, subsampling `0`, with `optimize=True`. Preview renders use the same pipeline with `half_size=True`.
- Capture timestamps are pulled from `rawpy` when available and written into EXIF on the JPEG derivative. `RawRenderSettings` stores the render snapshot, including white balance, exposure, auto-levels, tone-curve settings, and output bit depth.
- The source path and render snapshot are persisted with the image record. `images.original_filepath` keeps the original/source location or copied original, and `images.lab_metadata.raw_processing` stores the source metadata, derivative metadata, and settings JSON.
- Live Lab also stores per-context RAW presets in the local SQLite `settings` table under `live_lab_raw_processing_preset::<context>`.
- HEIC/HEIF import uses `pillow_heif` and is converted to JPEG (`quality=90`). Optional microscope resampling writes WebP derivatives (`quality=65`, `method=4`) when the optimal-size path is enabled.
- RAW-backed edits reopen the source RAW file and replace the compressed working derivative. They do not edit the JPEG derivative in isolation, and if the RAW source file is missing the RAW edit path is unavailable.

## Data Flow & Sync Engine
- **Local-First Database:** All data is initially written to the local SQLite database.
- **Cloud Syncing:** `cloud_sync.py` manages bidirectional REST sync with the Supabase PostgreSQL database.
- **Metadata-First Desktop Sync:** The desktop `Sync now` path refreshes observations, image metadata, measurements, and snapshots without downloading missing cloud media. When a user explicitly wants cloud media on the local device, there is a separate offline-media download action.
- **Conflict Resolution:** 
  - Sync engine stores a last-seen snapshot for cloud observations.
  - Cloud is the source of truth for linked observation metadata; manual review is reserved for destructive cases such as image removal.
  - Reduced cloud image copies and harmless metadata drift are merged without a modal.
  - Missing R2 media objects are gracefully skipped, allowing the rest of the sync to continue.
- **Important Upload Failures:** Plan-limit image upload failures surface a detailed dialog naming the observation, image, and file sizes so users can tell which upload needs attention.
- **Account Lock:** `linked_cloud_user_id` is stored in local settings after the first sync. Any attempt to sync with a different account without explicitly resetting the local cloud link throws an `AccountMismatchError`.

## Privacy & Visibility Model
- **Workflow vs Privacy:** 
  - `is_draft`: Indicates a WIP observation and stays separate from visibility.
  - `sharing_scope`: Maps to Supabase `visibility` (`private`, `friends`, `public`).
  - `location_precision`: Maps to Supabase `location_precision` (`exact`, `fuzzed`).
- **Privacy Slots:** When an observation syncs as non-public (`visibility != 'public'` or `location_precision = 'fuzzed'`), it consumes 1 of 20 available free-tier privacy slots in Supabase.
- The observation editor shows the current available private slots for free-tier accounts and updates that count live when sharing or location precision changes; Pro accounts hide the slot counter.

## Location Lookup Engine
- `database/reverse_location_lookup.py` manages asynchronous reverse geocoding.
- **Nominatim:** First pass fallback using a 1 req/sec throttle and parsed `display_name` + `address` values. Returns short local suggestions (`amenity`/`road` and `suburb`) as primary display options.
- **Norway (Artsdatabanken):** High-precision lookup queried when coordinates fall in Norway. Used if `dist <= 0.006`.
- **Denmark (DAWA):** High-precision DAWA lookup prepended to suggestions for Danish coordinates.
- **Behavior:** Lookup aborts if the observation form coordinates shift before the async request resolves.

## Desktop Identity & Profiles
- Mirrored from Supabase `public.profiles` (`username`, `display_name`, `bio`, `avatar_url`).
- `username` is editable as a profile handle, but cloud saves must respect the server-side uniqueness constraint and surface a clear conflict if the name is taken.
- Local `profile_email` follows the signed-in Sporely Cloud email to prevent orphaned/disjoint metadata while signed in.
- The Preferences profile avatar should render `avatar_url` when available and fall back to initials when no image can be loaded.
- The Preferences Sporely Cloud card shows the signed-in email, current plan, free-tier privacy-slot usage, and a free-tier upgrade link to `sporely.no` when appropriate.
- The Preferences UI no longer exposes the old full-resolution original sync opt-in or its explanatory copy; the main cloud surface is intentionally metadata-first with a separate offline-media path.
- Copyright and watermarks remain in Desktop "Online publishing" settings and are intentionally omitted from Sporely Cloud image syncs.

## External Integrations
- **Authentication:** Email/password for Sporely Cloud. Custom PKCE OAuth2 flow over `http://localhost:8000` for iNaturalist.
- **Publishing:** Artsobservasjoner and Artportalen run on invisible web session cookies. Successfully published observations persist their remote ID (`artsdata_id`, `inaturalist_id`, etc.) locally to prevent duplicate uploads.
- **AI Species Suggestion:** The observation editor provides AI-powered species suggestions from Artsdatabanken (Artsorakel) and iNaturalist.
  - Pressing "Guess" sends the selected image to both services simultaneously.
  - Results are displayed in separate tabs.
  - iNaturalist suggestions require the user to be logged in via the OAuth2 flow. If not logged in, a message is displayed.
