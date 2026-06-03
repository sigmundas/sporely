# Sporely Desktop (sporely-py) — Technical Spec

## Architecture Overview
A Python-based desktop application (PySide6) for field observations, microscopy calibration, spore measurements, and cloud synchronization. Acts as the local "source of truth" and offline workstation, while optionally syncing with Sporely Cloud (`sporely-web`).

## Directory Structure
- `ui/`: PySide6 interface. Driven by `main_window.py` containing tabbed widgets (`observations_tab`, `measure_tab`, `analysis_tab`, `live_lab_tab`).
- `database/`: SQLite schemas and access models (`schema.py`, `models.py`). Handles local CRUD.
- `utils/`: Integrations. Includes `cloud_sync.py` (REST sync) and `artsobs_uploaders.py` (external publishing).

## Data Flow & Sync Engine
- **Local-First Database:** All data is initially written to the local SQLite database.
- **Cloud Syncing:** `cloud_sync.py` manages bidirectional REST sync with the Supabase PostgreSQL database.
- **Metadata-First Desktop Sync:** The desktop `Sync now` path refreshes observations, image metadata, measurements, and snapshots without downloading missing cloud media. When a user explicitly wants cloud media on the local device, there is a separate offline-media download action.
- **Conflict Resolution:** 
  - Sync engine stores a last-seen snapshot for cloud observations.
  - Cloud is the source of truth for linked observation metadata; manual review is reserved for destructive cases such as image removal.
  - Reduced cloud image copies and harmless metadata drift are merged without a modal.
  - Missing R2 media objects are gracefully skipped, allowing the rest of the sync to continue.
- **Account Lock:** `linked_cloud_user_id` is stored in local settings after the first sync. Any attempt to sync with a different account without explicitly resetting the local cloud link throws an `AccountMismatchError`.

## Privacy & Visibility Model
- **Workflow vs Privacy:** 
  - `is_draft`: Indicates a WIP observation and stays separate from visibility.
  - `sharing_scope`: Maps to Supabase `visibility` (`private`, `friends`, `public`).
  - `location_precision`: Maps to Supabase `location_precision` (`exact`, `fuzzed`).
- **Privacy Slots:** When an observation syncs as non-public (`visibility != 'public'` or `location_precision = 'fuzzed'`), it consumes 1 of 20 available free-tier privacy slots in Supabase.

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
- The Preferences UI no longer exposes the old full-resolution original sync opt-in or its explanatory copy; the main cloud surface is intentionally metadata-first with a separate offline-media path.
- Copyright and watermarks remain in Desktop "Online publishing" settings and are intentionally omitted from Sporely Cloud image syncs.

## External Integrations
- **Authentication:** Email/password for Sporely Cloud. Custom PKCE OAuth2 flow over `http://localhost:8000` for iNaturalist.
- **Publishing:** Artsobservasjoner and Artportalen run on invisible web session cookies. Successfully published observations persist their remote ID (`artsdata_id`, `inaturalist_id`, etc.) locally to prevent duplicate uploads.
- **AI Species Suggestion:** The observation editor provides AI-powered species suggestions from Artsdatabanken (Artsorakel) and iNaturalist.
  - Pressing "Guess" sends the selected image to both services simultaneously.
  - Results are displayed in separate tabs.
  - iNaturalist suggestions require the user to be logged in via the OAuth2 flow. If not logged in, a message is displayed.
