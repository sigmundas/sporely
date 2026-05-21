# SQLite Migrations

This folder is reserved for standalone local SQLite migration SQL.

## Status

- No standalone SQL migrations have been moved here yet.
- The current desktop database still uses the Python migration helpers in `schema.py`, `migrate.py`, and `add_point_columns.py`.

## Guidance

- Add future local-only SQL migrations here if we decide to split them out from the Python helpers.
- Do not place Supabase/Postgres SQL here.

