# Database Structure

## Data Location

Sporely stores user data in the OS-specific application data folder:

- Windows: `%APPDATA%\Sporely`
- macOS: `~/Library/Application Support/Sporely`
- Linux: `~/.local/share/Sporely`

## Main Database (mushrooms.db)

Key tables include:

- **observations**: field and taxonomy metadata; includes source tracking fields and `artsdata_id` (Artsobservasjoner sighting id).
- **images**: image paths, image type, objective name, calibration id, and crop metadata.
- **spore_measurements**: length, width, Q, and measurement points.
- **calibrations**: objective calibration history, camera, and megapixels.
- **thumbnails** and **spore_annotations** for UI and ML tooling.

## Reference Database (`reference_data/generated/reference_values.db`)

- **reference_values**: genus, species, source, mount medium, and min/percentile ranges.

## Objectives

Objective definitions are stored as data files in the app data folder (for example `objectives.json`). The active objective selection is stored separately.

## Schema Source of Truth

The authoritative schema definitions live in:

- `database/schema.py`
- `database/models.py`

Use those files for the most up-to-date table and column definitions.

## Backup, Restore, Export, and Import

The **File** menu separates installation recovery from portable observation
transfer:

- **Back Up Sporely…** creates a complete `.sporely` recovery archive while
  preserving installation identity and excluding credentials.
- **Restore Sporely Backup…** validates and restores a complete Sporely backup.
- **Export Selected Observations…** creates a portable `.sporely` archive for
  the observations currently selected in the observations table.
- **Import Observations…** previews and imports observations with new local and
  cloud identities.

Older ZIP data packages remain supported only through the observation import
action as a legacy compatibility path. They are not complete backups and cannot
be used with **Restore Sporely Backup…**.

## See also

- [Database Settings](./database-settings.md)
- [Artsobservasjoner login and upload](./artsobservasjoner.md)
- [Field photography](./field-photography.md)
- [Microscopy workflow](./microscopy-workflow.md)
- [Spore measurements](./spore-measurements.md)
- [Taxonomy integration](./taxonomy-integration.md)
