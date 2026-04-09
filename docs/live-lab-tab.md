# Live Lab Tab

The Live Lab tab (Alt+L) turns Sporely into a live microscope-ingestion workspace. It watches a capture folder, imports new microscope images into the current observation, and shows the latest import in a dedicated viewer without leaving the tab.

This implements the "Microscope Live Lab Mode" described in [Hardware Sync & Ingestion Architecture](hardware-sync.md).

---

## Layout

The tab follows the same split layout pattern as the Analysis tab:

- **Left sidebar (fixed width):** Current observation, watched folder, current lab state, and session controls.
- **Right side:** Large image viewer on top, session gallery below, and a hint/status bar at the bottom.

---

## Current Observation

The sidebar always targets the current active observation from the app when you enter the tab.

Shown fields:

- Vernacular name
- Scientific name
- Observation date

The **Change observation** button returns to the Observations tab so you can pick a different record.

---

## Watched Folder

The Watched Folder section defines the directory that Sporely monitors for new microscope captures.

Controls:

- **Browse:** Choose the folder used by the microscope camera software
- **Open folder:** Open the current watched folder in Finder

Sporely only enables session start when the folder exists.

---

## Current Lab State

The Current Lab State panel stores the metadata that should be attached to each newly imported microscope image:

- Objective
- Contrast
- Mount
- Stain
- Sample

These controls stay editable during a running session, so you can switch from one microscope setup to another without stopping the watcher. Each newly detected image uses the state that is active at the moment it is imported.

---

## Session Controls

The Session panel starts and stops the watcher thread.

Behavior:

- **Start Session:** Begins watching the selected folder for new image files
- **Stop Session:** Stops the watcher and keeps the current session gallery visible
- **Imported this session:** Shows how many images were added during the current session

If no observation is selected, or the watched folder is missing, the start button stays disabled.

---

## Main Viewer

The large viewer behaves like the image area in the Measure tab:

- Mouse wheel / trackpad scroll to zoom
- Drag to pan
- **Reset view** fits the image back into view
- Zoom percentage is drawn directly on the image
- A scale bar is shown automatically when the imported image has calibrated microns-per-pixel metadata

The viewer always shows the newest imported image by default, but clicking any thumbnail in the session gallery swaps the main viewer to that image.

---

## Session Gallery

The gallery at the bottom shows all images imported during the current Live Lab session in chronological order.

Features:

- Newest import is automatically selected and highlighted
- Clicking a thumbnail loads that image into the main viewer
- Objective / contrast / scale badges are shown on thumbnails when available

This gives a rolling visual record of the session while keeping the latest microscope frame front and center.

---

## Hint Bar

Instead of a separate session log, Live Lab uses the same bottom hint/status bar pattern as other tabs.

The bar is used for:

- Hover hints for controls
- Session start/stop feedback
- Import success messages
- Warnings such as HEIC conversion or thumbnail-generation problems

---

## Import Flow

When a new file appears in the watched folder:

1. The folder watcher waits for the file to finish writing.
2. HEIC files are converted when needed.
3. The image is inserted into the database as a microscope image for the current observation.
4. Objective, contrast, mount, stain, sample, and scale metadata are stored on the new image.
5. Thumbnails are generated.
6. The image is added to the session gallery and loaded into the main viewer.

If the observation is also active elsewhere in the app, the Observations and Measure views are refreshed so the imported image becomes available immediately across the UI.
