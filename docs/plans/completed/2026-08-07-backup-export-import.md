# Backup, restore, export, and import plan

Status: Completed; Phase 11 completed 2026-08-27.

## Agent handoff

- Status: All Phase 1–11 acceptance criteria pass.
- Last completed stage: Phase 11 — final verification and destructive/failure testing.
- Current/next stage: Complete.
- Important decisions: Backup/restore preserves installation identity; portable export/import deliberately remaps local and cloud identity while retaining scientific provenance.
- Do not: Extend the current selective exporter into a full backup, copy `app_settings.json` wholesale, or place credentials in an archive.
- Remaining acceptance criteria: None.

## Implementation reality audited 2026-08-27

The audit covered the active schema helpers, migration code, path resolvers,
legacy sharing implementation, calibration/image provenance, taxonomy-v2,
thumbnail handling, authentication persistence, and current QSettings users.
The findings below replace assumptions in the original draft.

- `utils/db_share.py` is a selective legacy data-package implementation. It
  copies selected logical tables and currently uses `ZipFile.extractall()` on
  import. It is compatibility/reference material, not the new archive layer,
  and Phase 1 must not modify it.
- Neither desktop database has an authoritative schema version. Fresh
  production initialization yields `PRAGMA user_version = 0` for both files;
  that value is SQLite's default, not a Sporely migration version.
- Database and image locations are configurable and may be outside the app
  data directory. Inventory resolution must call `get_database_path()`,
  `get_reference_database_path()`, and `get_images_dir()` at runtime.
- `reference_values.db` is not a disposable reference cache. It contains the
  normalized reference library and is authoritative in its entirety.
- The main DB `settings` table mixes preferences, sync baselines, paths,
  personal profile data, usernames, and credentials. A raw full snapshot can
  contain secrets, including iNaturalist client secrets and Mushroom Observer
  API keys. Phase 2 must sanitize only the staged snapshot according to the
  Phase 1 key policy; it must never mutate the live DB to prepare a backup.
- Additional persistent state missing from the original draft includes
  `inaturalist_oauth_tokens.json`, cloud recovery caches, the publish-media
  cache, taxonomy-v2 activation/install receipts, database-backed retained-
  original settings, and per-observation Species Plate state in QSettings.
- Thumbnail code has competing roots: `utils/thumbnail_generator.py` uses the
  import-time `database.schema.DATABASE_PATH`, while `database/models.py` uses
  `get_database_path().parent`. Archive code will treat the latter runtime
  derivation as canonical without changing thumbnail production in Phase 1.

The audit initialized both databases in an isolated temporary app-data
directory using production helpers. Persistent non-`sqlite_*` tables are:

| Database | Tables |
| --- | --- |
| Main | `calibration_assets`, `calibrations`, `image_tombstones`, `images`, `observation_reference_uses`, `observations`, `session_logs`, `settings`, `spore_annotations`, `spore_measurements`, `thumbnails` |
| Reference | `reference_measurement_sets`, `reference_taxon_treatments`, `reference_values`, `reference_works` |

`images_new` and `spore_measurements_new` are migration-only names and are not
inventory items.

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

# Phase 1 — Archive contract and authoritative inventory

Do this before changing the current exporter. Build it separately under
`utils/archive/`; do not modify `utils/db_share.py`, add UI, create archives,
snapshot SQLite, or extract/import anything in this phase.

Introduce one ZIP-based `*.sporely` container. Every archive begins with
`manifest.json` and reserves these layouts:

```text
manifest.json
databases/
    mushrooms.db
    reference_values.db
data/
    objectives.json
    last_objective.json
assets/
    images/
    originals/
    calibrations/
```

Portable exports instead use a `portable/` payload plus their required asset
closure.

### Manifest v1

The side-effect-free contract defines:

```text
format = "sporely-archive"
format_version = 1

mode = full_backup | portable_observations
identity_policy = preserve | portable
```

`full_backup` requires `preserve`; `portable_observations` requires
`portable`. Builders accept `archive_id`, `created_at`, and `app_version` so
tests do not depend on clocks, random UUIDs, or importing side-effectful
`main.py`. Serialization is deterministic UTF-8 JSON with stable key and file
ordering.

The v1 shape is:

```json
{
  "format": "sporely-archive",
  "format_version": 1,
  "mode": "full_backup",
  "identity_policy": "preserve",
  "archive_id": "uuid",
  "created_at": "...",
  "app_version": "...",
  "schema_version": null,
  "source_platform": "...",
  "contents": {
    "observations": 1243,
    "images": 3811,
    "measurements": 7400
  },
  "files": [
    {
      "path": "databases/mushrooms.db",
      "status": "included",
      "size": 123456,
      "sha256": "..."
    }
  ]
}
```

`schema_version` is reserved and nullable. Version 1 writes `null`; it must
not serialize `PRAGMA user_version = 0` as though that were an application
schema version. Future schema-version work can populate the field without
changing the archive envelope.

Each logical file has exactly one status:

```text
included
excluded_by_policy
missing_at_source
```

For `included`, `path`, non-negative `size`, and a lowercase SHA-256 digest are
required. For excluded or missing entries, size and hash are absent. There is
no second `missing_files` source of truth. `manifest.json` does not hash
itself. Future validation must enforce a bijection: every non-manifest ZIP
member has exactly one included entry, and every included entry has exactly
one member.

### Orthogonal inventory policies

The original single classification mixed unrelated questions. Phase 1 uses
two dimensions:

```text
BackupPolicy = EXACT | REGENERABLE | CACHE | SECRET | DOWNLOADABLE
PortablePolicy = ROOT | DEPENDENCY | EXCLUDE | SPECIAL
```

`BackupPolicy` decides whether state belongs in installation recovery;
`PortablePolicy` decides whether it can participate in a selected-observation
dependency closure. Every known DB table, application-owned resource, and
relevant settings key must receive both decisions. Inventory output is
deterministically ordered.

### Database inventory decisions

Full backup retains complete table coverage, except that known secret values
in the staged copy of the main `settings` table must be removed. Regenerable
tables are not stripped from the snapshot.

| Database state | Backup | Portable | Notes |
| --- | --- | --- | --- |
| `observations` | EXACT | ROOT | Selected observations are portable roots. |
| `images`, `spore_measurements`, `spore_annotations`, `session_logs`, `observation_reference_uses` | EXACT | DEPENDENCY | Include only the selected closure in portable mode. |
| `calibrations`, `calibration_assets` | EXACT | DEPENDENCY | Inclusion of bytes also follows provenance policy below. |
| `settings` | EXACT with staged secret filtering | EXCLUDE | Key-level policy is mandatory; the live DB is never edited. |
| `image_tombstones` | EXACT | EXCLUDE | Installation/cloud-sync state, not shared observation data. |
| `thumbnails` | REGENERABLE | EXCLUDE | Rows and files can be rebuilt; rows remain in a full DB snapshot. |
| all four reference DB tables | EXACT | DEPENDENCY | The entire DB is authoritative for full backup; portable mode takes the referenced closure. |

Add a coverage test that initializes isolated main/reference databases with
production helpers and compares all non-`sqlite_*` table names with the code
inventory. A newly added production table must fail until it receives policy.

### Persistent files and directories

Do not walk the app-data or image directories wholesale. Each inventory item
resolves a current source and an archive-managed destination. Row-referenced
paths may be absolute and outside standard roots.

| State | Backup | Portable | Decision |
| --- | --- | --- | --- |
| `objectives.json` | EXACT | DEPENDENCY | Full file for backup; only required profiles for portable data. Resolve with `get_objectives_path()`. |
| `last_objective.json` | EXACT | EXCLUDE | Small workflow state; resolve with `get_last_objective_path()`. |
| Managed working images and retained originals | EXACT | DEPENDENCY | Resolve from DB rows and current storage roots, including valid external paths. |
| Calibration source/working assets | EXACT | DEPENDENCY | Resolve from calibration rows and `calibration_assets`, not directory membership alone. |
| Thumbnails and generated calibration artifacts | REGENERABLE | EXCLUDE | Record the policy; do not require bytes for recovery. |
| `cloud_cache/observations`, `cloud_cache/calibrations`, `cloud_cache/originals` | CACHE | EXCLUDE | Remote-owned recovery material; never promote to authoritative originals. |
| `app_cache_dir()` including `publish-media` | CACHE | EXCLUDE | Disposable application cache. |
| Installed taxonomy-v2 DB and `install_receipt.json` | DOWNLOADABLE | EXCLUDE | Reinstallable from the bundled/verified taxonomy release. |
| `artportalen_cookies.json`, `artsobservasjoner_cookies.json` | SECRET | EXCLUDE | Authentication material. |
| `inaturalist_oauth_tokens.json` | SECRET | EXCLUDE | Authentication material discovered by this audit. |
| Keyring-held cloud/Artportalen/Artsobservasjoner credentials | SECRET | EXCLUDE | Never enumerate, read, or archive keyring values. |

Image and calibration bytes are classified by row provenance, not only path.
Any row with `source_role = cloud_recovery_cache` or
`file_purpose = cache` is cache/remote-owned and its bytes are excluded even
when the path exists locally. `images.filepath`, `images.original_filepath`,
`calibrations.image_filepath`, `calibration_assets.local_path`, and
`calibration_assets.original_path` are all candidate sources. Paths embedded
in calibration JSON metadata must also be audited when the Phase 2 collector
is implemented.

For archive inventory purposes, the canonical thumbnail root is
`get_database_path().parent / "thumbnails"`. Fixing the separate import-time
root in `utils/thumbnail_generator.py` is outside Phase 1.

### Settings policy

Never copy raw `app_settings.json`. Phase 1 defines an explicit key policy and
Phase 2 emits a safe configuration snapshot:

| Key family | Policy |
| --- | --- |
| `cloud_access_token`, `cloud_refresh_token` | SECRET; exclude. |
| `database_folder`, `database_path`, `reference_database_path`, `images_dir`, `last_export_dir`, `last_import_dir` | Machine-specific; exclude from restored values and rebuild/rebase later. |
| `linked_cloud_user_id` | EXACT; preserve account binding without credentials. |
| `cloud_user_id`, `cloud_user_email`, child-change cursor, reconciliation markers, recent-import IDs | EXACT installation/sync state; preserve, subject to validation on restore. |
| Last-sync status, summaries, errors, and timestamps | REGENERABLE; exclude. |
| `taxonomy_v2_activation` | REGENERABLE; exclude because the installed dataset is not archived. |
| `ui_language`, `ui_theme`, `vernacular_language` and other non-secret preferences | EXACT. |
| Unknown keys | Fail closed during backup until assigned policy; never copy by default. |

The DB `settings` table also requires key policy despite remaining part of the
full SQLite snapshot. At minimum classify iNaturalist client secrets,
Mushroom Observer API keys, and any future token/password/API-key values as
SECRET and delete them only from the staged snapshot. Usernames, profile
fields, sync snapshots/baselines, workflow preferences, and UI preferences are
EXACT unless separately classified. Machine-specific values such as
`originals_dir`, watched/import directories, and scan directories require
restore remapping or clearing. A coverage-oriented key registry is required so
new credential-bearing settings cannot silently enter an archive.

QSettings receives namespace/key policy rather than copying a platform-native
settings file. Window geometry and splitter state are REGENERABLE. Species
Plate global preferences and per-observation composition/crop/label state are
EXACT because they contain user-authored presentation state tied to preserved
observation IDs. Unknown QSettings namespaces/keys are excluded until
classified.

### Archive path and ZIP-entry rules

All archive paths use canonical relative POSIX form. Reject empty names, NULs,
absolute POSIX paths, Windows drive paths, UNC paths, backslashes, `.`/`..`
components, duplicate normalized paths, case-fold collisions, ZIP symlinks,
and entries whose resolved staging destination would escape the staging root.
Never use `extractall()` in the new implementation.

### Checksums

Provide a local streaming SHA-256 file helper and verifier. It must not load
large assets into memory or depend on taxonomy internals.

### Phase 1 acceptance

Focused tests cover deterministic manifest serialization and parsing, invalid
contract values and file entries, current DB inventory coverage, settings and
secret policy, deterministic resource inventory, safe-path rejection,
duplicate/case-fold collisions, ZIP symlink rejection, and streaming checksum
verification. Tests use isolated temporary app-data roots and production
schema helpers. Do not mark Phase 1 complete until focused tests and
`py_compile` for touched modules pass.

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

Use Python SQLite's backup API to create staging copies:

```python
source.backup(destination)
```

for both:

```text
mushrooms.db
reference_values.db
```

This gives a transactionally consistent snapshot even if WAL mode is active.
The reference snapshot remains complete. In the staged main snapshot only,
remove `settings` rows classified `SECRET`; never modify the live source DB.

After snapshotting:

```text
PRAGMA integrity_check
```

must succeed on both archived databases.

### Full-backup contents

Include:

* entire `mushrooms.db` structure and data, except explicitly classified
  secret settings removed from the staged copy
* entire `reference_values.db`
* every locally managed image referenced from the DB
* originals that are actually retained locally
* calibration images/assets
* plate layouts
* `objectives.json`
* `last_objective.json`
* the Phase 1 allowlisted safe configuration snapshot, not raw
  `app_settings.json`
* classified QSettings state, including Species Plate authoring state
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
* downloadable taxonomy datasets if they can safely be reconstructed

Cloud recovery-cache files are excluded according to `source_role` and
`file_purpose`, not merely because “cache” appears in a filename.

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

Implemented 2026-08-27. The database-layer importer now allocates fresh local
integer identities, rewrites direct and embedded relationships, merges stable
calibration/reference identities with fail-closed conflict checks, and strips
source cloud ownership. A persistent `portable_cloud_identity_pending` guard
prevents observation, image, and measurement `desktop_id` recovery on push and
pull until collision-free destination reverse identities have been established
for the complete graph. User-facing import orchestration, archive import
history/idempotency, preview, and asset materialization remain in later phases.

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

Implemented 2026-08-27. Portable database import now requires the archive's
identity, persists local per-item source-to-destination mappings and canonical
source fingerprints, and binds that identity to the complete logical-item
inventory. Replay preflight distinguishes complete reused roots from genuinely
new roots, rejects changed archives, missing/crossed destinations, incomplete
root provenance, and incompatible stable entities before commit. Domain rows
and provenance commit together under an immediate transaction, so ordinary
failure and retry cannot leave provenance claiming uncommitted rows. Replay
preserves both pending and already-finalized Phase 5 cloud-identity guard state.
Archive extraction, asset materialization, and user-facing import remain in
later phases.

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

Implemented 2026-08-27. Portable archive import now validates and extracts exact
manifest members into staging, rewrites every imported image/original and
calibration asset path to deterministic destination-managed names, preserves
included authoritative originals, and clears missing or policy-excluded slots.
Promotion is checksum-verified and no-overwrite, occurs under the import write
transaction, and uses a durable recovery journal plus compensating cleanup for
partial failures and interrupted imports. Replay reuses the Phase 6 mappings,
verifies existing bytes, and rejects a mismatched destination asset root.

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

Completed 2026-08-27. The File menu opens a validated portable archive preview
with per-observation checkboxes and exact closure counts. Preview and confirmed
subset import share the portable export closure pruning functions. Preview
uses temporary staging only; destination databases, managed assets, and import
settings remain untouched until confirmation. Subset provenance retains the
full archive inventory fingerprint while recording only selected closure
mappings, allowing later overlapping subsets and exact replay without duplicate
rows. Focused UI scenarios cover all-selected and subset states.

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

Completed 2026-08-27. The File menu now contains one action each for full
backup, full restore, selected-observation export, and observation import, with
the intended grouping and selection-dependent export enablement. Import stays
available without a selection. The obsolete observation-tab Import/Export
buttons, forwarding callbacks, hints, busy-state references, and generic
table-selecting export entry point were removed. The lower-level `db_share`
implementation remains unchanged, and `.zip` selections from the File import
action continue to route to the legacy compatibility importer pending the
broader Phase 10 compatibility work.

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

Completed 2026-08-27. Archive routing now uses validated internal signatures,
not filename suffixes. Valid current manifests route only to their declared new
archive mode; legacy root-level database packages route only to the retained
`db_share` compatibility importer. Mixed signatures, unsafe ZIP entries,
malformed manifests, unknown packages, and corrupt legacy SQLite members fail
closed. Restore accepts only current `full_backup` manifests and explicitly
rejects portable observation archives and legacy data packages. The UI labels
legacy packages as legacy data packages rather than backups.

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

Completed 2026-08-27. Final adversarial verification added archive expansion
and truncation coverage, complete persistent-state fixture coverage, staged
secret-byte scrubbing, schema compatibility checks, cache-provenance and
calibration `auto_images` restoration, rollback and interrupted-copy recovery,
portable subset/session replay, objective-profile merging, stable-asset byte
conflicts, source-path scrubbing, preview-worker reentrancy, and archive-content
fingerprinting. The complete archive/backup/restore/portable suite and affected
cloud-sync safety suites pass, together with Python compilation, translation
completeness, and whitespace validation.

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
