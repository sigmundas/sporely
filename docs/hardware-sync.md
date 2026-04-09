# Hardware Sync & Ingestion Architecture

This document outlines the architectural blueprint for merging disconnected hardware (Phone, Macro Camera, Microscope) into a unified observation record across the Sporely ecosystem.

## 1. The "Temporal Anchor" System (Macro-to-GPS Link)

**Goal:** Automatically assign GPS coordinates and field metadata to SD-card macro photos by correlating their EXIF timestamps with the phone's geotagged field photos.

### Implementation Strategy (`sporely-py`):
- **Time-Delta Calibration:** 
  - `sporely-web` will host a "Sync Screen" tool displaying a high-contrast QR code encoding the current UTC time.
  - The user captures this screen with their macro camera.
  - During import in `sporely-py`, a scanner reads the QR code, compares it to the image's `DateTimeOriginal` EXIF tag, and calculates the exact clock drift offset (e.g., `-1m 24s`).
- **Matching Algorithm:** 
  - SD-card images imported into the desktop app will have the clock drift applied.
  - `sporely-py` queries the local SQLite `observations` table (which contains synced field observations from the phone).
  - Images falling within a configurable time threshold (e.g., ±15 minutes) of a field observation are automatically grouped under that Observation ID.
- **EXIF Merging:** 
  - Once confirmed, `sporely-py` uses an EXIF library to embed the matched GPS coordinates directly into the macro files on disk, ensuring the source files become permanently geotagged.

---

## 2. Microscope "Live Lab Mode" (Active Sessions)

**Goal:** Eliminate paper notes by automatically tagging and linking microscope images to an observation in real-time as they are captured.

### Implementation Strategy (`sporely-py`):
- **UI Integration:** 
  - A new "Live Microscopy Session" panel will be added to the desktop UI (`observations_tab.py` or similar).
  - The user selects a target observation and clicks **[Start Session]**.
  - Dropdowns reflect the current lab state: *Objective (40x, 100x)*, *Mount (Water, KOH)*, *Stain (Melzer's)*, and *Contrast (Brightfield, DIC)*.
- **Folder Watcher:** 
  - A background thread utilizing the `watchdog` library will monitor a specific local directory (e.g., the tethered capture folder from the microscope camera).
- **Real-time Ingestion:** 
  - When a new image is saved by the camera software, `watchdog` detects it.
  - The app reads the *current* state of the UI dropdowns and instantly attaches those specific tags to the incoming image in the database.
  - The image appears immediately in a rolling "Session Gallery" within the app.
- **Dynamic Tagging:** 
  - Users can change dropdowns (e.g., switching from 40x to 100x oil) without stopping the session. All subsequent photos captured by the watcher are tagged with the new settings.

---

## 3. Hybrid Sync Workflow (Desktop vs. Web)

To maintain performance and keep responsibilities clean:

- **`sporely-web` (Capacitor App):**
  - **Role:** The Field Anchor.
  - **Responsibility:** Fast, reliable capture of GPS, initial ID guesses, field photos, and generating the core `Observation` record in Supabase.
  - **Feature Addition:** A "Requires Microscopy" or "Lab Queue" flag, allowing users to highlight observations in the field that need scoping later.

- **`sporely-py` (Desktop App):**
  - **Role:** The Data Consolidation Hub.
  - **Responsibility:** Handles heavy CPU tasks including EXIF rewriting, batch SD-card imports, `watchdog` file monitoring, and AI/spore measurement bounding boxes.
  - **Sync Behavior:** When the desktop app syncs, it pushes the newly merged macro photos, the tagged microscopy photos, and the spore statistics up to the Supabase cloud, completing the record created by the phone.
