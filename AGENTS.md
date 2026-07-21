## Working agreements

- Do not run `npm test` after modifying JavaScript files.
- Ask for confirmation before adding new production dependencies.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. If a task touches multiple workflows or large UI files, propose staged patches and stop after the current stage.
- Do not rewrite or refactor unrelated code while fixing a bug. Preserve existing behavior unless the prompt explicitly asks for a behavior change.
- For sporely-py, always use the project virtual environment:
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python
/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest
Ask for confirmation before installing or upgrading packages in .venv.
For syntax checks, use ./.venv/bin/python -m py_compile <touched files>

## Cloud sync invariants

- Treat `sync_images`, `materialize_remote_images`, and `full_pull` as independent controls. Never turn all three on as a generic "full sync" fix.
- Observations-tab refresh uses `sync_images=False`, `materialize_remote_images=True`, and `full_pull=False`: push metadata, fast-pull only new/changed remote observations, and download their missing media without scanning the local media backlog.
- Profile & Cloud "Sync now" uses `sync_images=True`, `materialize_remote_images=True`, and `full_pull=False`: upload genuinely pending selected local media and materialize new/changed remote media, while preserving unchanged-observation pruning.
- `full_pull=True` is a deep reconciliation/recovery control. Pair it with `sync_images=False` unless a separately named, user-confirmed migration explicitly requires scanning and uploading all pending local media.
- `sync_images=True` activates the global pending-image dirty scan. It may re-dirty synced observations containing eligible `cloud_id IS NULL` rows and must never be enabled for background/startup/ordinary refresh paths.
- `materialize_remote_images=True` controls remote byte download; it does not require `sync_images=True` or `full_pull=True`.
- Rows with `source_role=cloud_recovery_cache` or `file_purpose=cache` are remote-owned recovery copies. They may receive metadata/link repairs but their bytes must never be prepared or uploaded back to cloud.
- Preserve the no-op fast-path contract: unchanged observations must not trigger bulk image/measurement fetches, WebP preparation, measurement pushes, or mosaic rebuilds.
- Any sync flag or media-selection wiring change must include focused tests for the exact caller mode and run `tests/test_cloud_sync_fast_path.py`, `tests/test_cloud_sync_dirty_loop_steady_state.py`, and the affected media pull/upload policy tests.
