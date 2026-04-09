# Sporely: Hardware Sync & Ingestion

This document describes the current local-only hardware sync workflow in `sporely-py`, plus the next pieces that still remain.

## Scope

There are two related time-based ingestion problems:

1. **Microscope retrospective ingestion**
   Use session logs and EXIF timestamps to match microscope images to the correct observation later.
2. **Field temporal anchor**
   Match phone observations to DSLR or other camera images later by using a shared time anchor. This is still future work.

## What Is Implemented

### Live Lab tab

The Live Lab tab now supports two session modes:

- **Live capture** watches a folder and imports new microscope images directly into the current observation.
- **Retrospective session** logs microscope state changes and timestamped notes without importing files immediately.

During a session, Sporely writes local `session_logs` rows for:

- `session_started`
- `dropdown_change`
- `manual_note`
- `session_stopped`

### Per-image notes

Per-image notes are already part of the local image model through `images.notes`.

They can now be edited in:

- **Prepare Images** dialog
- **Measure** tab sidebar

### Sync Shot

The current Sync Shot flow is implemented as a manual clock-calibration helper:

1. Open the Sync Shot modal in the Ingestion Hub.
2. Sporely freezes a precise local and UTC timestamp.
3. Photograph that screen with the camera used for the batch.
4. After scanning the batch folder, choose that photographed Sync Shot image.
5. Sporely compares the chosen image EXIF capture time to the frozen Sync Shot time and applies the resulting batch offset.

This is currently a manual selection flow. Automatic QR generation/decoding is not implemented yet.

### Ingestion Hub

The Ingestion Hub tab now supports the retrospective microscope workflow:

- scan a folder of offline microscope images
- load retrospective session logs from `session_logs`
- apply a manual or Sync Shot-derived time offset
- match images to logged sessions via `TemporalMatcher`
- review the matched images per observation
- commit selected matches into the target observation

Committed retrospective images write their microscope metadata into the normal image fields and attach the matched note text to `images.notes`.

## Where Microscope Metadata Is Stored

The canonical per-image microscope metadata is stored in the existing image columns:

| Table | Field | Purpose |
| :--- | :--- | :--- |
| `images` | `objective_name` | Objective used for the imported microscope image |
| `images` | `contrast` | Contrast mode |
| `images` | `mount_medium` | Mounting medium |
| `images` | `stain` | Stain |
| `images` | `sample_type` | Sample type |
| `images` | `notes` | Per-image note text |

Session history is stored locally in `session_logs`:

| Table | Field | Purpose |
| :--- | :--- | :--- |
| `session_logs` | `session_id` | Groups log rows into one session |
| `session_logs` | `session_kind` | `live` or `offline` |
| `session_logs` | `event_type` | `session_started`, `dropdown_change`, `manual_note`, `session_stopped` |
| `session_logs` | `attribute_name` | Which microscope field changed |
| `session_logs` | `value` | Changed value or note text |
| `session_logs` | `metadata_json` | Optional display labels and capture context |
| `session_logs` | `recorded_at` | Local event timestamp |

`images.lab_metadata` is optional local snapshot data used mainly for direct live imports. It is not required for retrospective matching, and it is not part of the current cloud/Supabase scope.

## Supabase Scope

No Supabase schema changes are required for the current hardware-sync implementation.

The current system is local-only:

- `session_logs` live in the local desktop database
- retrospective matching is done locally against EXIF timestamps
- per-image notes use the existing local `images.notes` field

## Remaining Work

- automatic QR generation/decoding for Sync Shot
- field-device temporal anchor for DSLR/phone batches
- richer Ingestion Hub review tools for unmatched images and manual reassignment
- Artsobservasjoner support for per-image note upload
