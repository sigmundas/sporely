# Findings from the 2026-11-26 desktop security audit

Status: written 2026-11-26. Nothing is implemented. Update this line when a
package lands; move the file to `completed/` when every package is done or
explicitly dropped.

Written for an agent that has not seen the session that produced it.

## Where this comes from

On 2026-11-26 the whole desktop repository was audited read-only. Claude ran
its own pass with two read-only scouts (UI injection and SQL; build, release
and admin tooling). A Codex review ran the same prompt in parallel. The server
side (RLS, RPCs, the upload Worker) was out of scope; it was audited on
2026-11-25 in sporely-web.

The audit ran at `be6c2ec`, which is `main` at `54a67b3` plus one UI commit in
`ui/main_window.py`. Locations below were re-checked against `main` at
`22f2b69`. Between the two, only documentation changed, apart from that one
commit, so line numbers in `ui/main_window.py` after line 7717 shift by 13
once it merges. Find code by the function name given, not by line alone.

Every finding below was checked against the code by reading the cited
function. None was reproduced at runtime. One claim was tested by running
code: the cloud upload encoder (`_prepare_cloud_image_upload_file`) drops EXIF,
including GPS, from a sample JPEG (Pillow 12.3.0).

## How to work on these

- Read `AGENTS.md` first. Its rules win over this file.
- By default the top-level session implements and subagents never write code.
  The packages below are file-disjoint so that the `/security-fix-packages`
  skill can run one subagent per package, each in its own worktree, but only
  after the owner says in the conversation that subagent implementation and
  pull requests are allowed for this run. Without that, work through the
  packages one at a time in the top-level session.
- A package may edit only the files listed for it, plus new test files and new
  modules it creates. It must not edit `tests/conftest.py` or another
  package's files. If a fix needs a file owned by another package, stop and
  report.
- Branch from `origin/main`, one commit per finding, push the branch. Never
  merge into or push to `main`.
- For every finding, write the regression test first and show that it fails on
  the unfixed code.
- Tests: `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest <files>`
  for the package's tests and the tests of the modules it touches; CI runs the
  full suite on push. Syntax check with the same Python and
  `-m py_compile <touched files>`. Do not install or upgrade packages in
  `.venv`.
- Fixtures: no real-looking coordinates, names or emails. Round coordinates to
  two decimals.
- Review before merge: `/codex:challenge` for packages A, B and C;
  `/codex:review` for the rest. Verify each review finding against the code.
- New or changed `tr("…")` strings need the translation update in
  `.claude/rules/localization.md`.
- Packages B and C touch cloud sync: read `.claude/rules/cloud-sync.md` and the
  relevant section of `docs/supabase-sync-contract.md` first.
- The desktop has no deployment. A package is done when it is merged and CI is
  green on `main`. Releases are the owner's step.

## Packages

| Package | Findings | Model | Needs the owner's yes first |
|---|---|---|---|
| A. Legacy bundle import and folder operations | F01, F02, F03, F04a | Opus | no |
| B. Export, backup and restore | F05, F06, F04b, F07, F08 | Opus | F06 (deletion behaviour, sync contract) |
| C. Cloud client: session and account binding | F09, F04c, F10 | Opus | F09 (auth), F10 (account binding) |
| D. UI: links, avatar, keys, previews, watched folder | F11, F12, F13, F14, F15 | Sonnet | no |
| E. Third-party clients, transport and admin tools | F16 to F24 | Sonnet | no |
| F. Local data permissions | F25 | Sonnet | no |
| G. Release pipeline | F26 | Sonnet | no |

Model choice: Sonnet where the brief names the files and the test and the fix
is mechanical; Opus where the fix depends on path containment, restore, sync,
deletion or sign-in logic.

Severity is the auditor's rating for a single-user desktop app. "Crafted
bundle" means a `.sporely` or legacy `.zip` data package the user was given by
someone else and chose to import.

### Package A. Legacy bundle import and folder operations

Files: `database/models.py`, `utils/db_share.py`,
`utils/archive/portable_import.py`, `ui/calibration_dialog.py`.

The legacy importer (`utils/db_share.py::import_database_bundle`) is still
reachable: `ui/main_window.py::import_observations` routes a ZIP holding
`mushrooms.db` without a manifest to it
(`utils/archive/routing.py::classify_archive`).

**F01. Deleting an imported objective can delete an unrelated folder. High.**
- Where: `database/models.py::CalibrationDB.delete_calibrations_for_objective`
  (line 5950) runs `shutil.rmtree(get_calibrations_dir() / objective_key)`.
  The legacy importer saves bundled objective keys unchanged
  (`utils/db_share.py:1235`); the portable importer keeps selected incoming
  keys (`utils/archive/portable_import.py::_merged_objectives_payload`). The
  delete dialog (`ui/calibration_dialog.py:3390`) shows the objective's
  display name, not the key.
- Scenario: a bundle's `objectives.json` has a key that is an absolute path or
  contains `..`. The user later deletes that objective. `Path` joins an
  absolute key to itself, so the tree at that path is removed.
- Fix: accept an objective key only if it is a single path component (no
  separators, not `.` or `..`, not absolute); give an invalid incoming key a
  fresh local key at import. At deletion, resolve the target and require it to
  be a strict descendant of the calibrations folder; otherwise skip the
  `rmtree`.
- Test: import a bundle whose objective key points at a sibling fixture
  folder, delete the objective through `CalibrationDB`, assert the folder and
  its files survive. Also call the delete directly with a bad key already in
  the settings.

**F02. Editing the taxon of an imported observation can move an unrelated folder. Medium-high.**
- Where: `_BUNDLE_OBSERVATION_CLEARED_FIELDS` (`utils/db_share.py:712`) does
  not include `folder_path`, so the bundle's value is inserted (line 861).
  `database/models.py::ObservationDB.update_observation` (line 2087) moves
  `old_folder_path` when it exists, through `_move_observation_folder`
  (line 1790). The portable importer already clears the field.
- Scenario: a bundle row has `folder_path` set to a folder in the user's home.
  The user corrects the genus; the folder is moved into the images folder.
- Fix: clear `folder_path` in the legacy importer. In the move, require the
  source (stored or inferred) and the destination to resolve inside the
  managed images folder; otherwise skip the move and keep the row's path.
- Test: import a legacy row pointing at a sibling fixture folder, change the
  genus through the model, assert the folder has not moved.

**F03. An orphaned bundle image attaches to an existing observation. Medium-low.**
- Where: `utils/db_share.py:952`. When `old_obs_id` is not in `obs_map`, the
  source `observation_id` is kept and the row is inserted against whichever
  local observation has that number.
- Fix: when observations are being imported, skip (with a warning in the
  import report) any image whose parent did not map. Apply the same rule to
  measurements and annotations whose image did not map.
- Test: source image whose parent id is absent from the source but exists in
  the destination; assert no row and no file is added to that observation.

**F04a. Path-bearing calibration metadata survives import. Medium (with F04b and F04c).**
- Where: `utils/db_share.py:1086` rebases only `measurements_json.images[].path`.
  Other keys that later code treats as file paths are kept as supplied:
  `source_path`, `source_filepath`, `original_path`, `selected_path`,
  `working_path`, `companion_paths`, and everything under `auto_images`. The
  key list is in `utils/archive/full_backup.py::_json_image_entries`.
- Scenario: a bundle's calibration metadata names a file on the importing
  computer. A later backup or export packs that file (F04b), or a later sync
  uploads it as a calibration image (F04c).
- Fix: at import, pass every path-bearing key through
  `_restore_archive_asset` (or drop the key when the file is not in the
  bundle), for `images` and `auto_images` alike.
- Test: import a bundle whose metadata names an out-of-library sentinel file
  under each key; assert no imported row still holds that path.

### Package B. Export, backup and restore

Files: `utils/archive/portable_export.py`, `utils/archive/full_backup.py`,
`utils/archive/full_restore.py`, `utils/archive/inventory.py`,
`utils/archive/validation.py`, `utils/archive/routing.py`,
`utils/archive/paths.py`, `utils/archive/manifest.py`,
`utils/cloud_sync_impl/observation_delete.py`,
`docs/supabase-sync-contract.md`.

**F05. A selected-observation export can include photos of no observation. Medium.**
- Where: `utils/archive/portable_export.py::_prune_main_database` (line 89):
  `DELETE FROM images WHERE observation_id NOT IN (SELECT id FROM observations)`.
  `images.observation_id` is nullable (`database/schema.py:1665`), and
  `NULL NOT IN (…)` is not true, so parentless images stay and their files are
  packed. Validation counts the same snapshot, so it passes.
- Fix: delete images whose `observation_id` is null or not selected; assert
  before packing that every remaining image has a selected parent.
- Test: export one observation from a database that also holds a parentless
  image; assert the image row and its files are absent from the archive.
- Note: the owner's database had no parentless images on 2026-11-26.

**F06. A restored backup replays pending cloud deletes. Medium. Needs the owner's yes.**
- Where: `mark_observation_delete_pending` stores
  `sporely_cloud_observation_delete_pending_<cloud id>` in the database
  settings. `_DB_EXACT_PREFIXES` in `utils/archive/inventory.py` (line 274)
  includes `sporely_cloud_`, so a full restore keeps these rows.
  `retry_pending_observation_deletes`
  (`utils/cloud_sync_impl/observation_delete.py:107`) then deletes each cloud
  observation that has no local row. An empty owner value passes the owner
  check (`if owner and owner != user_id`).
- Scenario: a stale or crafted backup holds pending deletes. After the
  restore, the next sync deletes those cloud observations. The restore
  confirmation says nothing about cloud deletions. The server still limits
  this to observations the signed-in account may delete.
- Fix: classify the pending-delete prefix so a restore drops it (a delete
  intent does not cross a restore), and require a non-empty owner equal to the
  signed-in account before retrying. Keep ordinary same-installation retries.
  Update the deletion section of `docs/supabase-sync-contract.md`.
- Test: restore an archive holding a same-owner, an empty-owner and a
  foreign-owner intent, then run the retry against a recording client; assert
  no delete is sent. Separately assert a normal interrupted delete is still
  retried.
- Note: the owner's database had no pending deletes on 2026-11-26.

**F04b. Backup and export follow file paths found in calibration metadata. Medium.**
- Where: `utils/archive/full_backup.py::_collect_database_assets` (line 239)
  resolves every path from `_json_image_entries` and from
  `calibration_assets.metadata_json` with `_resolve_row_path`, which accepts
  any absolute path, and packs the file. `utils/archive/portable_export.py`
  (line 356) does the same for portable exports.
- Scenario: metadata that arrived through a bundle import (F04a) or a cloud
  pull names a file outside the library; the next archive contains that
  file's bytes, and the user shares the archive.
- Fix: for paths that come from metadata JSON (not from the `images`,
  `calibrations` or `calibration_assets` path columns), include the file only
  when it resolves inside the managed images folder or the configured
  originals folder; otherwise record it as missing. Check first which
  locations legitimate metadata uses, so real libraries still back up fully.
- Test: a calibration whose metadata names a sentinel file outside both
  folders; assert the sentinel is in neither a full backup nor a portable
  export, and that an in-library metadata file still is.

**F07. A full restore installs whatever triggers the backup database contains. Low.**
- Where: `utils/archive/validation.py::validate_full_backup` checks integrity
  and required tables; `utils/archive/full_restore.py::_require_tables`
  (line 84) and `_require_current_columns` check names only. Extra or altered
  triggers and views are installed with the database.
- Scenario: a crafted backup carries a trigger that silently alters later
  edits. The user must already have accepted a full replacement of their
  library from an untrusted source, which is why this is low.
- Fix: after migrating the staged databases, compare the names and SQL of
  triggers and views in `sqlite_master` with those of a freshly initialised
  database of the same schema version; reject on any extra or changed object.
- Test: a valid backup plus one benign extra trigger is rejected before the
  swap; an unmodified backup still restores.

**F08. Archive control files are read whole before any size check. Low.**
- Where: `utils/archive/routing.py::classify_archive` (line 47) and
  `utils/archive/validation.py` read `manifest.json`, the settings JSON and
  the objectives JSON with `archive.read(…)`. `utils/archive/paths.py` allows
  an 8 GiB member.
- Fix: cap control documents (a few MiB) by declared size before reading, and
  read through a bounded stream so a wrong declared size cannot bypass it.
- Test: with a small test limit, an oversized manifest is rejected without
  its body being read.

### Package C. Cloud client: session and account binding

Files: `utils/cloud_sync.py`, `utils/sporely_cloud_auth.py`.

**F09. Sign-out does not end the session at Supabase. Low-medium. Needs the owner's yes.**
- Where: `ui/main_window.py::_logout_cloud` calls
  `SporelyCloudClient.clear_credentials` (`utils/cloud_sync.py:7170`), which
  only clears local settings and the saved password. The desktop never calls
  a logout or revocation endpoint.
- Scenario: a refresh token copied earlier from this machine stays usable
  after the user signs out.
- Fix: on explicit sign-out, revoke the current session first, then clear
  local state whatever the outcome; report when the revocation could not be
  sent (offline). For password sessions this is `POST /auth/v1/logout` with
  `scope=local`. For sessions issued through the Supabase OAuth server, check
  the current Supabase documentation for the supported revocation call before
  writing code; do not guess the endpoint. Do this inside `clear_credentials`
  or a new method beside it so `ui/main_window.py` (package D) is not edited;
  check every other caller of `clear_credentials` first, because an account
  mismatch or a forced re-login may not want a remote sign-out. A new client
  method that writes must be added to `_PULL_ONLY_BLOCKED_CLIENT_METHODS`.
- Test: a recording auth client receives the revocation for the current
  session; local state is cleared when the request fails.

**F04c. Calibration push opens any file named in pulled or imported metadata. Medium.**
- Where: `utils/cloud_sync.py::_resolve_local_calibration_asset_path`
  (line 1182) accepts absolute paths;
  `_select_representative_calibration_image_path` (line 1251) falls back to
  `measurements_json.images[].path`; the file is re-encoded and uploaded as
  the calibration reference image.
- Scenario: metadata from a cloud pull or a bundle names a readable image
  elsewhere on the disk; a later push uploads a copy to the user's own cloud
  calibration scope. The cloud query is owner-scoped, so another account has
  no demonstrated way to plant this through the cloud; the bundle route is
  F04a.
- Fix: accept a resolved path only when it lies inside the managed images
  folder or the configured originals folder. Treat anything else as "no local
  reference image".
- Test: a calibration whose metadata names an out-of-library sentinel image;
  assert the push neither opens nor uploads it.

**F10. An unlinked database binds to an account that owns any one sampled observation. Low. Needs the owner's yes.**
- Where: `utils/cloud_sync.py::_verify_unlinked_database_owner` (line 2113)
  samples up to 50 synced cloud ids and accepts the account when
  `list_owned_observation_ids` returns anything.
- Scenario: a database holding rows from two accounts binds to the first that
  owns one row; unsynced rows can then be created under that account.
- Fix: require the account to own every sampled id; on a partial match raise
  `AccountMismatchError`.
- Test: a database with rows attributable to two accounts refuses to bind; a
  single-owner database still binds.

### Package D. UI: links, avatar, keys, previews, watched folder

Files: `ui/main_window.py`, `ui/image_import_dialog.py`,
`ui/observation_details_dialog.py`, `ui/observations_tab.py`,
`ui/live_lab_tab.py`, `utils/lab_watcher.py`, plus new helper modules.

**F11. Links from the AI service and the update feed are opened almost unchecked. Low.**
- Where: `ui/image_import_dialog.py::_ai_prediction_link` (line 3126) accepts
  any value that starts with `http`; `_build_adb_link_widget` (line 3154) puts
  it unescaped into `<a href="…">` with external links enabled.
  `ui/observation_details_dialog.py::_ai_prediction_link` (line 2237) and
  `_on_ai_table_cell_clicked` (line 2324) do the same through
  `QDesktopServices.openUrl`. `ui/main_window.py::_handle_atom_reply`
  (line 6077) passes the feed's `href` to `_show_update_dialog`, which opens
  it.
- Fix: one helper that returns a URL only when it parses as `https` with a
  host on an allow-list (Artsdatabanken and iNaturalist hosts for AI links;
  the repository's GitHub releases path for the update notice) and otherwise
  returns the fixed fallback. Escape the URL when it goes into rich text.
- Test: an off-origin URL, a `"`-bearing URL and a non-https scheme each fall
  back to the fixed link; a valid link is kept.

**F12. The profile avatar URL is fetched from any host with no limits. Low.**
- Where: `ui/main_window.py::_fetch_and_set_profile_avatar` (line 1974)
  builds a `QNetworkRequest` from the profile's `avatar_url` and reads the
  whole reply. The value comes from the user's own profile row.
- Fix: fetch only `https` URLs on the Supabase storage host, which is where
  the desktop uploads avatars (`upload_avatar` in `utils/cloud_sync.py`).
  Check whether the web app stores avatars on another Sporely host before
  narrowing the list. Cap the reply size; apply the same check to redirects.
- Test: a fake network manager; other hosts and oversized replies are refused.

**F13. Mushroom Observer keys are stored in plain SQLite settings. Low-medium.**
- Where: `ui/main_window.py:2848` writes `mushroomobserver_app_api_key` and
  `mushroomobserver_user_api_key` with `SettingsDB.set_setting`;
  `ui/observations_tab.py` reads them. The uploader sends them as URL
  parameters (`utils/artsobs_uploaders.py:580`, package E's file; leave it,
  the Mushroom Observer API takes the key as a parameter).
- Fix: store both keys in the OS keyring (same pattern as
  `utils/artportalen_auth.py`), migrate an existing database value once and
  blank the database row, read from the keyring in both UI files. Keep the
  setting names in `_DB_SECRET_KEYS` (package B's file) unchanged.
- Test: saving keys leaves no value in the settings table; an existing value
  is migrated and blanked; the uploader still receives the keys.
- Note: the owner's database held no such keys on 2026-11-26.

**F14. Previews are written to a fixed folder in the shared temp directory. Low.**
- Where: `ui/observation_details_dialog.py::_build_dialog_preview_path`
  (line 4981) uses `<tmp>/sporely_dialog_previews`;
  `ui/live_lab_tab.py::_prepare_calibration_source_image` (line 1978) uses
  `<tmp>/sporely/calibration_from_livelab`. On a shared `/tmp` another user
  can read the files or pre-create the folder.
- Fix: one owner-only temporary directory per process (`tempfile.mkdtemp`),
  removed on exit.
- Test: the folder is created with mode `0700` under a permissive umask
  (skip on Windows).

**F15. The watched folder follows links to files outside it. Low.**
- Where: `utils/lab_watcher.py::_list_file_names` (line 132) uses
  `entry.is_file()`, which follows symlinks;
  `ui/live_lab_tab.py::rescan_watch_folder` (line 3398) resolves and queues
  the target.
- Fix: ignore symlinks at discovery and at rescan
  (`entry.is_file(follow_symlinks=False)`), and re-check before copying.
- Test: a real image and a link to an outside image in the folder; only the
  first is reported by the watcher and by the rescan.

### Package E. Third-party clients, transport and admin tools

Files: `utils/r2_storage.py`, `utils/oauth_loopback.py`,
`utils/inat_oauth.py`, `utils/artsobservasjoner_auto_login.py`,
`utils/artportalen_auth.py`, `utils/sync_shot_qr.py`,
`tools/repair_supabase_storage_media.py`,
`database/import_taxa_to_supabase.py`.

**F16. The media repair tool sends Supabase credentials to the public media host. Medium.**
- Where: `tools/repair_supabase_storage_media.py`. `AdminServiceRoleClient`
  (line 82) puts the service-role key in its session headers;
  `_probe_media_backends` (line 220) reuses that session for
  `https://media.sporely.no/…` (line 713). This happens in audit mode, before
  any repair flag. In signed-in mode the user's bearer token is sent the same
  way.
- Fix: use a fresh `requests.Session()` with no default headers for public
  probes; pass Supabase headers only to Supabase URLs.
- Test: capture prepared requests with dummy credentials in audit-only mode;
  no `apikey` or `Authorization` header reaches the media host.

**F17. Media downloads write straight to the final path with no size limit. Low.**
- Where: `utils/r2_storage.py::CloudflareMediaWorkerClient.download_to_file`
  (line 795). An interrupted download leaves a partial file that the
  observation gallery then treats as cached
  (`ui/observations_tab.py:3365`, package D's file; do not edit it).
- Fix: stream to a temporary file beside the destination, enforce a byte
  ceiling, then `os.replace` into place; remove the temporary file on any
  failure. The same in `CloudflareR2Client.download_to_file`.
- Test: a fake response that fails midway, and one that exceeds the cap;
  neither leaves a file at the destination or replaces a good one.

**F18. The Worker URL override accepts any scheme and host. Low.**
- Where: `utils/r2_storage.py::media_worker_base_url` (line 183) returns
  `SPORELY_MEDIA_WORKER_URL` as given; every request carries the bearer token.
- Fix: accept the override only when it is `https`, or `http` on a loopback
  host for local development; otherwise use the default and log a warning.
- Test: `http://example.invalid` falls back to the default.

**F19. Reporting-service login posts the password to whatever form action the page returns. Low.**
- Where: `utils/artportalen_auth.py::login` (line 436, posts at 456 and 482)
  and `utils/artsobservasjoner_auto_login.py` (login at 430, post at 447).
  Redirects are followed and the form action is taken from the HTML.
- Fix: before sending credentials or callback fields, require the target to
  be `https` on an allow-list of the service's own hosts and its identity
  provider (`useradmin-auth.slu.se` is already named in the code; take the
  Artsobservasjoner hosts from the existing constants and confirm them from
  the code, not from memory).
- Test: synthetic pages whose form action or redirect leaves the allow-list;
  no password is posted.

**F20. Old Artsobservasjoner cookies come back after logout. Low.**
- Where: `utils/artsobservasjoner_auto_login.py::_migrate_legacy_cookies`
  (line 532) runs in every constructor and copies from
  `~/.myco_log/artsobservasjoner_cookies.json` and the old shared file when a
  target file is missing; `clear_cookies(target)` leaves those sources.
- Fix: migrate once (delete or mark the legacy sources after a successful
  migration), and make logout remove them.
- Test: migrate, log out, construct again; no cookies load.

**F21. A stray request to the sign-in listener ends the wait. Low.**
- Where: `utils/oauth_loopback.py::wait_for_callback` sets the event for any
  request on the callback path (line 129); the caller then rejects the state
  and sign-in fails. Accepted connections have no read timeout.
- Fix: let the caller pass the expected `state`; ignore callbacks that do not
  carry it and keep listening; set a socket timeout on accepted connections.
- Test: a wrong-state request followed by the real one still succeeds; an
  idle connection cannot outlast the deadline.

**F22. The iNaturalist example entry point prints a live access token. Low.**
- Where: `utils/inat_oauth.py::example_login_and_print_access_token`
  (line 275). `authorize` (line 191) also prints the authorization URL.
- Fix: print status only; remove the token print and the URL print.
- Test: run the entry-point logic with a fake token; neither output stream
  contains it.

**F23. Sync Shot QR decoding multiplies image memory. Low.**
- Where: `utils/sync_shot_qr.py::_load_decode_image` and
  `_collect_candidate_arrays` (lines 94 to 117) hold five arrays at once, two
  at four times the pixel count.
- Fix: downscale to a QR-sufficient size before building candidates and try
  candidates one at a time.
- Test: a large synthetic image decodes with every array under a set pixel
  budget; an existing QR fixture still decodes.

**F24. The taxon import script writes to production on plain execution. Low.**
- Where: `database/import_taxa_to_supabase.py`: fixed project URL (line 24),
  service-role key from the environment, upserts under `__main__` with no dry
  run. It is not bundled with the app.
- Fix: default to a dry run that prints counts; write only with an explicit
  `--apply` flag that echoes the target project and row count first.
- Test: without `--apply` no request is sent (recording session).

### Package F. Local data permissions

Files: `utils/private_files.py`, `app_identity.py`, `database/schema.py`,
`main.py`.

**F25. The data folder, databases and older secret files are readable by other accounts. Low on macOS, medium on shared Linux.**
- Where: the app data folder is created with the default umask
  (`app_identity.py::migrate_app_data_dir`); SQLite files likewise
  (`database/schema.py::get_connection`). `write_private_text` tightens a
  secret file only when it is rewritten, so cookie files written by older
  versions stay `0644`. Observed on the owner's Mac: folder `0755`, databases
  and two old cookie files `0644`. macOS protects these through the `0700`
  parent `Application Support`; a Linux home may not.
- Fix: at startup on POSIX, set the app data folder to `0700` and set `0600`
  on the settings file, the cookie and token files and the SQLite files that
  live inside it. Do not change permissions on a database folder the user
  chose outside the app data folder. No change on Windows.
- Test: a temporary profile with `0644` files and a `0755` folder; after the
  startup helper they are `0600` and `0700` (skip on Windows).

### Package G. Release pipeline

Files: `.github/workflows/release.yml`, `.github/workflows/ci.yml`,
`requirements*.txt`, `build_mac.sh`, `build_linux.sh`, `build.ps1`.

**F26. Release builds run unpinned code with a write token. Medium.**
- Where: `.github/workflows/release.yml` sets `contents: write` for the whole
  workflow (line 17); actions are pinned by tag (`actions/checkout@v4`,
  `actions/setup-python@v5`); the build installs `pillow` unpinned and
  `requirements.txt`, where only PySide6 and PyYAML are exact pins. The draft
  check verifies asset names and sizes, not content.
- Fix: default the workflow to `contents: read` and grant `contents: write`
  only to the jobs that create the draft and upload assets; pin actions by
  commit SHA with the tag in a comment; build from a hash-locked requirements
  file (`pip install --require-hashes`), including the bootstrap `pillow`;
  publish a `SHA256SUMS` asset.
- Limits: the lock file must be generated without installing into the
  project `.venv` (`AGENTS.md`); ask the owner before changing any pinned
  version. The workflow cannot be exercised locally. The owner verifies with a
  `workflow_dispatch` run to a draft release.

## Held for the owner

- **Location on third-party publishing. Medium, product decision.**
  `utils/artsobs_uploaders.py` sends exact coordinates to iNaturalist
  (line 420, no `geoprivacy` field) and to Mushroom Observer (line 575, in the
  locality text), and posts the image files unmodified, whatever the
  observation's Sporely `location_precision` is. Options: map hidden or
  obscured precision to the provider's own privacy setting; show the outgoing
  location and ask before publishing; strip GPS from the copies sent for
  observations that are not exact. Not assigned to a package until decided;
  when it is, it touches `ui/observations_tab.py`, `ui/publish_notice.py` and
  `utils/artsobs_uploaders.py`, so it must run after packages D and E.
- **Code signing and notarization.** macOS builds are ad-hoc signed
  (`build_mac.sh:43`), Windows installers are unsigned. Needs developer
  accounts.
- **Service-role key.** If the media repair tool was ever run with
  `--admin-service-role`, the key was sent to `media.sporely.no` (F16). That
  host is the owner's own Cloudflare domain; rotation is optional.
- **Housekeeping on the owner's Mac (not a repository change).** Two
  `app_settings.repair-backup-20260612-*.json` files in the Sporely data
  folder still hold token values from June, and `artportalen_cookies.json`
  and `artsobservasjoner_cookies_mobile.json` are world-readable.

## Rejected

- **Cloud observation delete can leave media behind.**
  `delete_cloud_observation` (`utils/cloud_sync.py:9199`) lists image and
  mosaic rows without paging. It only loses keys when one observation has
  more rows than the server's page cap (1000), which does not happen in
  practice.

## Checked and found sound

- Browser sign-in: PKCE with S256, random state checked before the code is
  used, loopback-only listener, no request logging
  (`utils/sporely_cloud_auth.py`, `utils/oauth_loopback.py`).
- Token and cookie writes are owner-only (`utils/private_files.py`);
  passwords go to the OS keyring.
- Account binding: a linked database refuses a different account
  (`utils/cloud_sync.py::ensure_database_linked_to_cloud_user`).
- Archive member validation: absolute paths, traversal, symlinks, special
  files, case collisions and compression bombs are rejected
  (`utils/archive/paths.py`). Bundled asset paths are confined
  (`utils/db_share.py::_restore_archive_asset`).
- Imports clear cloud identity and mint new sync keys.
- Cloud image uploads are re-encoded to WebP without EXIF (tested, see above).
- No runtime path loads a service-role or R2 key unless
  `SPORELY_ENABLE_DIRECT_R2` is set (`utils/r2_storage.py:177`). No `.env`,
  admin script or `tools/` file is bundled by the build scripts.
- CI for pull requests uses `pull_request`, read-only permissions and no
  secrets. The Windows installer is per-user.
- No `shell=True`, `eval`, `pickle`, unsafe YAML load, `load_extension` or
  disabled TLS verification in runtime code.

## Not examined

- Runtime behaviour: nothing was run except the EXIF test.
- Qt rich-text rendering was sampled, not swept: most `QMessageBox` call
  sites and the large UI modules beyond the search hits were not read.
- SQL built from imported column names in the legacy importer
  (`INSERT INTO … ({', '.join(columns)})` in `utils/db_share.py`), and
  PostgREST filter strings built from local ids.
- Native decoders (JPEG, HEIC, RAW, OpenCV), OS keyring behaviour, Windows
  ACLs and reserved names.
- Maintainer scripts under `database/taxonomy/` and `tools/` beyond F16 and
  F24.
