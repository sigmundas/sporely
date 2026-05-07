# Sporely Desktop (sporely-py) — Architecture

## Overview
A Python-based desktop application for field observations, microscopy calibration, spore measurements, and cloud synchronization. This app acts as the local "source of truth" and offline workstation, while syncing selectively with Sporely Cloud (`sporely-web`).

---

## Tech Stack
| Component | Choice |
|---|---|
| **Language** | Python 3.10+ |
| **UI Framework** | PySide6 (Qt for Python) |
| **Local Database** | SQLite3 (`sqlite3` module) |
| **Analysis & Plots** | Matplotlib |
| **Image Processing** | Pillow (PIL), OpenCV |
| **Networking** | `requests` |

---

## Testing & Auditing
| Component | Choice |
|---|---|
| **Static Analysis** | Ruff (linting/formatting), mypy (type-checking) (planned) |
| **Testing Framework**| pytest for sync safety, EXIF handling, reverse location lookup, map-link parsing, stats helpers, and other core logic |

---

## Directory Structure
- **`ui/`**: PySide6 user interface code. Driven by `main_window.py` containing tabbed widgets (`observations_tab.py`, `measure_tab`, `analysis_tab`, `live_lab_tab.py`).
- **`database/`**: SQLite schemas and access models (`schema.py`, `models.py`). Handles local CRUD operations for observations, images, measurements, and calibrations.
- **`utils/`**: Integrations and helper utilities.
  - `cloud_sync.py`: Synchronizes the local SQLite database to Supabase via REST APIs.
  - `inat_oauth.py`: Custom PKCE OAuth2 flow for iNaturalist integration.
  - `artsobs_uploaders.py`: Adapters for publishing to external services (Artsobservasjoner, Artportalen, iNaturalist, Mushroom Observer).
- **`assets/`**: Static assets, SVG icons, and themes.
- **`docs/`**: Markdown documentation for app workflows.

---

## Data Flow & Integrations
1. **Local-First Database**: All data is initially written to the local SQLite database.
2. **Cloud Syncing**: `cloud_sync.py` manages an optional bidirectional sync with the Supabase PostgreSQL database. The local SQLite database remains usable without cloud login or cloud storage. Conflict resolution defaults to showing the user a diff for manual review.
   - Cloud observation workflow state is stored separately from privacy: `is_draft` means the find is still WIP, `sharing_scope` maps to Supabase `visibility` (`private`, `friends`, `public`), and `location_precision` maps to Supabase `location_precision` (`exact`, `fuzzed`).
   - New observations default to open-science mode: public, draft, and exact location. Users can choose private/friends or fuzzed location when a find should consume a privacy slot.
3. **Authentication**: 
   - **Sporely Cloud**: Standard JWT-based email/password authentication.
   - **iNaturalist**: Uses a PKCE-first OAuth2 flow for desktop login. Sporely can operate as a public client without storing a `client_secret`, using the open-source desktop Client ID plus a temporary local callback server (`http://localhost:8000/callback`) to complete authorization securely.
   - **Artsobservasjoner / Artportalen**: Web session cookies managed invisibly.
4. **External Publishing State**: Successful uploads store the external observation IDs directly on the local `observations` row (`artsdata_id`, `artportalen_id`, `inaturalist_id`, `mushroomobserver_id`). The desktop Publish column renders service links from those persisted IDs and uses them as the local "already uploaded" markers to prevent duplicate publishing.
5. **Cloud Media Fault Tolerance**: Desktop cloud pull now tolerates missing R2 objects for individual cloud images by skipping the broken image and continuing the rest of the sync, while still surfacing meaningful review items when the remote image set changed.

---

## Location Lookup and Regional Metadata
- `database/reverse_location_lookup.py` is the desktop source of truth for reverse geocoding. It returns a `LocationLookupResult` containing ordered user-facing suggestions, coordinates, `country_code`, `country_name`, Nominatim `display_name`, and the winning source.
- Nominatim is queried first for every coordinate with an app-specific `User-Agent`, a global 1 request/second throttle, and parsing of `display_name`, `address.country_code`, and `address.country`.
- Nominatim suggestions shown in the Location field are intentionally local and separate: first `address.amenity` or `address.road`, then `address.neighbourhood` or `address.suburb`. The full `display_name` is kept as fallback/reference, not as the normal dropdown label.
- Norway gets a second high-precision lookup through Artsdatabanken. A result is used only when `dist <= 0.006`; otherwise Sporely falls back to the Nominatim suggestions to avoid snapped offshore/boundary anomalies.
- Denmark gets a second local lookup through DAWA. DAWA results are placed before Nominatim suggestions for Danish coordinates.
- The edit-observation dialog auto-fills the first suggestion, exposes all suggestions in a dropdown, and ignores stale async lookup results whose coordinates no longer match the current form.
- Resolved country drives regional UI behavior: `no` selects Artsobservasjoner-oriented publishing, `se` selects Artportalen-oriented publishing, and other countries keep their actual country label without pretending to be Norway or Sweden.
- NIN2/Biotope and Substrate tabs are visible only when the resolved country is Norway or Sweden, or while the country is still unknown. When hidden for other countries, their values are omitted from the saved observation payload.

---

## Cloud Sync Safety
- `app_settings.json` stores `linked_cloud_user_id` once the local database successfully syncs with a Sporely Cloud account for the first time.
- Before any push/pull cycle, `utils/cloud_sync.py` verifies the active Supabase user via `/auth/v1/user` with JWT-subject fallback. If the active user differs from `linked_cloud_user_id`, sync aborts with `AccountMismatchError`.
- The settings login flow also calls the same account-link check before saving new credentials. A user cannot silently replace the signed-in cloud account for a local database; they must explicitly reset/migrate the cloud link first.
- The lock prevents accidentally syncing one SQLite database into multiple Supabase accounts. It does not affect local-only use; all SQLite CRUD paths continue to work without cloud credentials.
- Settings → Profile & Cloud → **Reset Cloud Link...** runs `database.models.reset_cloud_sync_state()` after a high-friction confirmation. It clears local cloud IDs, sync snapshots, media signatures, `cloud_last_pull_at`, recent cloud-import markers, and `linked_cloud_user_id`, then logs out of Sporely Cloud.
- Resetting the cloud link does not delete remote Supabase/R2 data. Users must delete the old cloud account in the web Profile first to avoid duplicate cloud storage before re-syncing the same local database to a new account.

## Cloud Privacy Model
- Desktop does not implement social follows or friend-request UI. Those live in `sporely-web`.
- Desktop owns per-observation cloud controls that sync to Supabase:
  - **Draft:** local `observations.is_draft`, Supabase `observations.is_draft`; true means WIP and should be shown with a Draft/WIP badge in community surfaces.
  - **Sharing:** local `observations.sharing_scope`, Supabase `observations.visibility`; values are `private`, `friends`, and `public`.
  - **Location precision:** local/Supabase `location_precision`; values are `exact` and `fuzzed`.
- A privacy slot is consumed in Supabase when `visibility != 'public' OR location_precision = 'fuzzed'`. Free users have 20 slots; pro users are unlimited.
- The legacy Supabase `draft` visibility value is read as local `private` for compatibility only. New desktop pushes send `private`, not `draft`.

## Profile and Account Identity
- Settings now has a merged **Profile & Cloud** page. It owns Sporely Cloud login, default cloud sharing, username, display name, profile email, bio, and avatar.
- The desktop profile mirrors the web profile row in Supabase `public.profiles`: `username`, `display_name`, `bio`, and `avatar_url`.
- `profile_name` remains the local display-name/author/watermark source used by desktop observations and publishing. When signed in, saving the profile pushes the same display name to `profiles.display_name`.
- The local `profile_email` follows the signed-in Sporely Cloud auth email when a cloud account is active. This avoids maintaining a separate author email from the account that owns sync data.
- Profile photos are uploaded to Supabase Storage at `avatars/{user_id}/avatar.jpg`; `profiles.avatar_url` stores the public URL. Observation media remains in Cloudflare R2.
- Image copyright and watermark settings are part of **Online publishing**, not cloud identity. Sporely Cloud sync uploads clean observation images and thumbnails without visible overlays or generated publish media.
