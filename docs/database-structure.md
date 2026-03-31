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

## Reference Database (reference_values.db)

- **reference_values**: genus, species, source, mount medium, and min/percentile ranges.

## Objectives

Objective definitions are stored as data files in the app data folder (for example `objectives.json`). The active objective selection is stored separately.

## Schema Source of Truth

The authoritative schema definitions live in:

- `database/schema.py`
- `database/models.py`

Use those files for the most up-to-date table and column definitions.

## Export and Import (Backup / Sharing)

Sporely can bundle your data for backup or sharing with others:

- **Export DB**: creates a zip file containing the main database, reference database, and image data.
- **Import DB**: merges a shared bundle into your local data.

These are available in the **File** menu as **Export DB** and **Import DB**.

## See also

- [Database Settings](./database-settings.md)
- [Artsobservasjoner login and upload](./artsobservasjoner.md)
- [Field photography](./field-photography.md)
- [Microscopy workflow](./microscopy-workflow.md)
- [Spore measurements](./spore-measurements.md)
- [Taxonomy integration](./taxonomy-integration.md)
