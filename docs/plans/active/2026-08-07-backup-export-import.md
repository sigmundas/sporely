# Backup, restore, export, and import plan

Status: Active; implementation has not been verified as complete.

## Agent handoff

- Status: Proposed; repository evidence does not show this plan completed.
- Last completed stage: None verified.
- Current/next stage: Phase 1 — define the archive contract.
- Important decisions: Backup/restore preserves installation identity; portable export/import deliberately remaps local and cloud identity while retaining scientific provenance.
- Do not: Extend the current selective exporter into a full backup without first establishing the archive contract and authoritative inventory.
- Remaining acceptance criteria: All phase-specific and round-trip criteria below.

The original planning discussion recommended a deliberately simple UI: **no toolbar/buttons at all**. Treat backup/restore as application-level operations under **File**, while observation transfer is also accessible there and uses the current observation selection when appropriate.

## Proposed backup/export plan

### 1. User-facing model

Keep four distinct operations so there is no ambiguity between “backup” and “sharing data”:

**File**

* **Back Up Sporely…**
* **Restore Sporely Backup…**
* separator
* **Export Selected Observations…**
* **Import Observations…**

The important semantic split is:

| Operation                    | Intended use                                     | Identity             |
| ---------------------------- | ------------------------------------------------ | -------------------- |
| Back Up Sporely              | Disaster recovery / moving your own installation | Preserve everything  |
| Restore Sporely Backup       | Reconstruct that installation                    | Preserve everything  |
| Export Selected Observations | Share/move observations                          | Portable identities  |
| Import Observations          | Bring portable observations into current DB      | New local identities |

I would remove the existing export/import buttons from `observations_tab.py` once the File menu equivalents are working.

For **Export Selected Observations…**, use the existing multi-selection in the observations table. If nothing is selected, disable the menu item rather than interpreting that as “export everything.”

---

# Phase 1 — Define the archive contract

Do this before changing the current exporter.

Introduce one container format, for example:

```text
*.sporely
```

Internally it can remain ZIP.

Every archive starts with:

```text
manifest.json
```

Suggested high-level structure:

```text
manifest.json
databases/
    mushrooms.db
    reference_values.db
data/
    objectives.json
    last_objective.json
    ...
assets/
    images/
    originals/
    calibrations/
    ...
```

For portable exports:

```text
manifest.json
portable/
    observations.json / sqlite snapshot
assets/
    ...
```

### Manifest

Something along these lines:

```json
{
  "format": "sporely-archive",
  "format_version": 1,
  "mode": "full_backup",
  "archive_id": "uuid",
  "created_at": "...",
  "app_version": "...",
  "schema_version": "...",
  "source_platform": "...",
  "contents": {
    "observations": 1243,
    "images": 3811,
    "measurements": 7400
  },
  "files": [
    {
      "path": "databases/mushrooms.db",
      "size": 123456,
      "sha256": "..."
    }
  ],
  "missing_files": []
}
```

Crucially, the manifest records whether each missing asset was:

```text
included
excluded_by_policy
missing_at_source
```

That makes archive corruption distinguishable from an intentionally excluded file.

### Archive classification

Create one authoritative inventory in code:

```text
EXACT_BACKUP
PORTABLE
REGENERABLE
CACHE
SECRET
DOWNLOADABLE
```

Every DB table and application-owned file should belong to one category.

This prevents future features from silently being omitted from backups.

---

# Phase 2 — Full backup

This should be a **new implementation**, not an extension of the current selective `db_share.py` export logic.

I'd put the new functionality behind something like:

```text
utils/archive/
    manifest.py
    inventory.py
    paths.py
    checksums.py
    full_backup.py
    validation.py
```

or, if you want less module proliferation:

```text
utils/backup.py
utils/archive_manifest.py
```

### Database snapshots

Never copy the live SQLite files directly.

Use Python SQLite's backup API:

```python
source.backup(destination)
```

for both:

```text
mushrooms.db
reference_values.db
```

This gives a transactionally consistent snapshot even if WAL mode is active.

After snapshotting:

```text
PRAGMA integrity_check
```

must succeed on both archived databases.

### Full-backup contents

Include:

* entire `mushrooms.db`
* entire `reference_values.db`
* every locally managed image referenced from the DB
* originals that are actually retained locally
* calibration images/assets
* plate layouts
* `objectives.json`
* `last_objective.json`
* other application-owned configuration needed to reproduce the working installation
* `session_logs`
* `settings`
* `image_tombstones`
* cloud identifiers and sync metadata
* observation/reference relationships
* all other persistent application tables

Do **not** include:

* OAuth/auth tokens
* cookies
* keyring contents
* credentials
* transient HTTP caches
* regenerable thumbnails, unless some thumbnail is actually authoritative data
* downloadable taxonomy/reference datasets if they can safely be reconstructed

For recovery-cache files, explicitly decide whether the file is authoritative. Don't use “cache” in the filename as the deciding factor.

### File collection

Do not walk directories wholesale.

Instead generate an inventory from authoritative application state:

```text
DB/file setting
    ↓
resolved source path
    ↓
archive-relative managed path
```

That also solves the current issue where unrelated image-directory files get included.

### Atomic creation

Build:

```text
backup-name.sporely.tmp/
```

or a temporary ZIP in a staging location.

Then:

1. create SQLite snapshots
2. collect files
3. calculate hashes
4. create manifest
5. reopen archive
6. verify manifest
7. verify hashes
8. verify DB integrity
9. atomically rename to final `.sporely`

A failed backup should never leave a file that looks like a valid completed backup.

---

# Phase 3 — Full restore

Restore needs stronger guarantees than import.

### Restore flow

**File → Restore Sporely Backup…**

1. Choose `.sporely`.
2. Read manifest without extracting anything.
3. Verify:

   * supported archive format
   * supported archive version
   * required files
   * hashes
   * archive paths
4. Show a concise summary:

```text
Sporely backup
Created: 7 August 2026 14:32
App version: …
Observations: 1,243
Images: 3,811
Backup size: 4.2 GB
```

5. Create an automatic **pre-restore backup** of the current installation.
6. Extract into staging.
7. Validate SQLite databases.
8. Validate referenced required assets.
9. Perform schema compatibility/migration if necessary.
10. Close/release live DB connections.
11. Swap staged data into place.
12. Re-open databases and run application-level sanity checks.
13. Only then delete temporary staging.

The old profile should never be progressively overwritten.

Conceptually:

```text
current
   ↓
automatic safety backup

archive
   ↓
staging
   ↓
validation
   ↓
atomic replacement
```

If restoration fails, current data stays untouched.

---

# Phase 4 — Portable observation exporter

Here we can reuse more of `db_share.py`, but change the contract substantially.

The request becomes something like:

```python
export_observations(
    observation_ids: set[int],
    destination: Path,
    ...
)
```

No “all observations because the table was selected.”

### Dependency closure

For the selected observations, discover only related data:

```text
observations
│
├── images
│   ├── image files
│   ├── retained originals
│   ├── measurements
│   ├── annotations
│   └── calibration references
│       └── calibration assets
│
├── session logs
│
└── observation reference uses
    └── measurement sets
        └── treatments
            └── works/references
```

The exact graph should be derived from current schema foreign keys and application semantics rather than being maintained as a loose collection of SQL snippets.

### Important distinction

The export should contain enough reference/calibration data to make the selected observations meaningful, but **not everything from those tables**.

Example:

If observation A uses calibration UUID X, include X.

Don't include every calibration the user has ever made.

---

# Phase 5 — Portable identity policy

This is probably the most important behavioral change.

Portable archives should explicitly say:

```json
"identity_policy": "portable"
```

On import:

### Remap

Local integer IDs get new destination IDs:

```text
source observation 123 → destination observation 897
source image 777      → destination image 1602
```

All dependent FKs are rewritten through mapping tables held during import.

### Preserve stable content identities selectively

For things already designed around stable UUID identity, such as calibration/reference entities, use their intended merge semantics.

For example:

```text
same calibration UUID + same content
→ reuse

same UUID + conflicting immutable content
→ conflict/error, not silent overwrite
```

### Strip source-user cloud ownership

Portable import should not preserve cloud identity in a way that can mutate the source record.

Imported observations become roughly:

```text
cloud_id = NULL
sync state = local/dirty/new
cloud deletion state = clear
remote revision = clear
```

where those concepts exist in the current schema.

The import creates genuinely local observations that may subsequently be uploaded as new Sporely cloud records.

---

# Phase 6 — Idempotent portable import

Repeatedly importing the same `.sporely` archive should not blindly generate duplicates.

Give every archive:

```text
archive_id
```

and every exported logical item an archive-local identity.

Maintain import provenance, preferably in the database:

```text
import_history
```

Conceptually:

```text
archive_id
source_item_type
source_item_id
destination_item_id
imported_at
```

Then a second import can say:

```text
17 observations already imported
3 new observations
```

rather than silently creating another 20.

This provenance must **not** imply cloud identity; it is only local import tracking.

---

# Phase 7 — Safe asset handling

Replace filename-based copying with managed destination names.

Never do:

```text
incoming/photo.jpg
       ↓
images/photo.jpg
```

because two archives may contain `photo.jpg`.

Use something deterministic or generated:

```text
images/<new-image-uuid>.webp
originals/<new-image-uuid>.<ext>
```

and update the DB accordingly.

For external originals:

* resolve the actual referenced source;
* copy it if the portable/full-backup policy says it is included;
* record a deliberate omission if unavailable;
* never recreate the original absolute source path on another machine.

### ZIP safety

Every archive member must be validated before extraction:

Reject:

```text
../
/absolute/path
C:\...
symlink escapes
```

Extract only beneath the staging directory.

Also guard against pathological archive expansion with reasonable size/count validation from the manifest.

---

# Phase 8 — Import preview

**File → Import Observations…**

Before modifying anything, show archive metadata and observation contents.

Something like:

```text
Import observations

Archive created: 7 August 2026
Created with Sporely 0.x.x

☑ Amanita muscaria       03 Aug 2026   5 images
☑ Russula emetica        02 Aug 2026   3 images
☐ Lactarius turpis       01 Aug 2026   8 images

Selected:
2 observations
8 images
147 measurements
2 calibration records
4 references
```

Checkboxes here are useful.

No corresponding export-options dialog is really necessary for normal use. **Export Selected Observations…** should generally export the complete required dependency closure automatically.

That is much safer than asking the user whether they remembered to select “measurements” or “calibrations.”

---

# Phase 9 — File-menu integration

Once the engines exist, add actions in `main_window.py`:

```text
File
├── …
├── Back Up Sporely…
├── Restore Sporely Backup…
├──────────────
├── Export Selected Observations…
├── Import Observations…
├──────────────
└── …
```

### Selection integration

Reuse the selection mechanism around:

```text
observations_tab.py:4700
```

but keep the menu action in `MainWindow`.

Something along the lines of:

```python
ids = self.observations_tab.selected_observation_ids()
```

The File action is enabled only when:

```python
bool(ids)
```

That keeps data operations out of the visual observation workspace.

### Remove old UI

After feature parity:

* remove export/import buttons around `observations_tab.py:2341`;
* remove the old generic “select tables to export” user-facing workflow;
* retain useful lower-level functions from `db_share.py`;
* eventually retire incompatible legacy paths.

---

# Phase 10 — Legacy archive compatibility

I would **not** force the new code to make old archives conform to the new backup contract.

Instead:

```text
new manifest present
→ new importer

old archive signature
→ legacy importer
```

The old format should be described in the UI as something like:

> Legacy Sporely data package

rather than “backup.”

That lets existing users import their previous packages without weakening the new format.

Do not allow legacy packages through **Restore Sporely Backup** because they were never complete backups.

---

# Phase 11 — Testing

I'd divide tests into four groups.

### Archive-format tests

Test:

* manifest serialization
* deterministic inventory
* SHA-256 verification
* unsupported future versions
* malformed manifests
* missing required entries
* extra files
* path traversal
* absolute paths
* bad hashes
* truncated ZIPs

### Full-backup tests

Create a fixture installation containing every persistent data type, including:

* observations
* images
* originals
* annotations
* measurements
* calibration assets
* session logs
* tombstones
* settings
* references
* cloud/sync metadata

Run backup.

Then verify the archive contains the complete authoritative inventory.

Explicitly test a WAL-active database to prove the SQLite backup API captures committed WAL data.

### Restore round trip

The strongest test should be:

```text
Fixture A
  ↓
full backup
  ↓
restore into clean profile
  ↓
Fixture B
```

Then compare semantically authoritative contents.

For DB state, compare tables/rows after excluding legitimate machine-specific data.

For assets:

```text
SHA256(source) == SHA256(restored)
```

This is more meaningful than literally comparing the SQLite file bytes, because SQLite snapshots can be semantically identical without being byte-identical.

### Portable round trip

Fixture:

```text
Observation A
Observation B
Observation C
```

Select only A.

Verify that:

* A is present;
* B/C are absent;
* A's images are present;
* A's measurements are present;
* required calibration is present;
* unrelated calibration is absent;
* required references are present;
* unrelated references are absent;
* unrelated image-directory files are absent;
* cloud identity is removed during import;
* newly assigned IDs are internally consistent.

Then import the same archive twice and verify the second import does not silently duplicate A.

---

# Suggested implementation sequence

I would land this in **six PR-sized stages**, rather than the seven broad stages in the audit:

1. **Archive contract + inventory + security utilities**
   Manifest v1, file classification, safe archive paths, checksums, tests. No UI behavior changed.

2. **Full backup + File menu**
   WAL-safe DB snapshots, authoritative asset inventory, staging/validation, **File → Back Up Sporely…**. This gives you a genuinely useful backup early.

3. **Full restore**
   Archive validation, pre-restore safety backup, staged restore, integrity checks, **File → Restore Sporely Backup…**.

4. **Selective export**
   Observation-ID API, dependency closure, portable manifest, **File → Export Selected Observations…**. Remove the observation-tab export button.

5. **Selective import**
   Preview/select observations, ID mapping, cloud neutralization, safe filenames, idempotency and conflict policy, **File → Import Observations…**. Remove the old import button.

6. **Legacy cleanup + destructive/failure testing**
   Legacy package compatibility, interrupted operations, corrupt archives, missing files, WAL tests, external originals, calibration/reference conflicts, and full round-trip recovery tests.

## One change I would make to the audit recommendation

I would **not start by expanding `db_share.py`**.

Use it as a source of proven handling for calibrations, references, provenance, etc., but establish the new backup/archive layer separately. Otherwise the semantics of:

```text
"copy my installation exactly"
```

and:

```text
"import somebody's observations into my installation"
```

will continue to leak into each other.

The key invariant should be:

> **Backup/restore preserves identity. Export/import deliberately breaks installation and cloud identity while preserving scientific provenance.**

That distinction will make the implementation substantially easier to reason about and safer long-term.
