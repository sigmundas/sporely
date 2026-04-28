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
| **Testing Framework**| pytest (planned) for testing sync logic, EXIF injection, and database models |

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
3. **Authentication**: 
   - **Sporely Cloud**: Standard JWT-based email/password authentication.
   - **iNaturalist**: Uses a PKCE-first OAuth2 flow for desktop login. Sporely can operate as a public client without storing a `client_secret`, using the open-source desktop Client ID plus a temporary local callback server (`http://localhost:8000/callback`) to complete authorization securely.
   - **Artsobservasjoner / Artportalen**: Web session cookies managed invisibly.
4. **External Publishing State**: Successful uploads store the external observation IDs directly on the local `observations` row (`artsdata_id`, `artportalen_id`, `inaturalist_id`, `mushroomobserver_id`). The desktop Publish column renders service links from those persisted IDs and uses them as the local "already uploaded" markers to prevent duplicate publishing.
5. **Cloud Media Fault Tolerance**: Desktop cloud pull now tolerates missing R2 objects for individual cloud images by skipping the broken image and continuing the rest of the sync, while still surfacing meaningful review items when the remote image set changed.

---

## Cloud Sync Safety
- `app_settings.json` stores `linked_cloud_user_id` once the local database successfully syncs with a Sporely Cloud account for the first time.
- Before any push/pull cycle, `utils/cloud_sync.py` verifies the active Supabase user via `/auth/v1/user` with JWT-subject fallback. If the active user differs from `linked_cloud_user_id`, sync aborts with `AccountMismatchError`.
- The lock prevents accidentally syncing one SQLite database into multiple Supabase accounts. It does not affect local-only use; all SQLite CRUD paths continue to work without cloud credentials.
- Settings → Sporely Cloud → **Reset Cloud Link...** runs `database.models.reset_cloud_sync_state()` after a high-friction confirmation. It clears local cloud IDs, sync snapshots, media signatures, `cloud_last_pull_at`, recent cloud-import markers, and `linked_cloud_user_id`, then logs out of Sporely Cloud.
- Resetting the cloud link does not delete remote Supabase/R2 data. Users must delete the old cloud account in the web Profile first to avoid duplicate cloud storage before re-syncing the same local database to a new account.
