# SQLite Migrations

This folder is reserved for standalone local SQLite migration SQL.

## Status

- No standalone SQL migrations have been moved here yet.
- The desktop app migrates its database through the Python helpers in `schema.py`.
  `migrate.py` holds an older parallel copy that only tests call; its future is
  tracked in `docs/plans/active/2026-11-18-database-package-layout.md`.

## Guidance

- Add future local-only SQL migrations here if we decide to split them out from the Python helpers.
- Do not place Supabase/Postgres SQL here.

