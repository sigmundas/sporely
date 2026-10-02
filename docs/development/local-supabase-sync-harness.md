# Local Supabase sync harness

Runs the desktop's real sync code against a local Supabase stack built from
sporely-web `origin/main` without the production-deferred migrations
(`supabase/deploy-exceptions.json`). Never touches production, the sporely-web
checkout, your real Sporely profile or the keychain.

## Run

```bash
tools/run_local_sync_harness.sh               # reset local DB, run scenarios
tools/run_local_sync_harness.sh --skip-reset  # reuse the current local DB
tools/run_local_sync_harness.sh --force-reset # reset although not provably idle
```

Requirements: Docker, the Supabase CLI and a running local stack
(`supabase start` in sporely-web; API at `http://127.0.0.1:54321`).

## Safety rules

- Only `127.0.0.1`/`localhost` URLs are accepted (parsed hostname, no userinfo)
  in the script, the pytest conftest and every device process; a device
  process blocks and fails on any non-local HTTP request.
- One run at a time (`$TMPDIR/sporely-local-sync-harness.lock`).
- The local database is reset only when provably idle (no foreign client
  sessions, PostgREST connections idle for 60 s); otherwise use `--skip-reset`
  or, deliberately, `--force-reset`.
- Each simulated device is a separate process with its own temporary
  `SPORELY_APP_DATA_DIR` and the keyring fail backend.

## Scenarios

`tests/local_supabase/` (marker `local_supabase`; skipped unless
`SPORELY_LOCAL_SUPABASE=1`, which the script sets): device registration,
no-change resync (strict xfail for issue #11), Download from Cloud without
writes, set and treatment deletion across devices, default-shared contribution
through the anon catalogue and copy, undeclared legacy write, and withheld
enhanced sets (table read omits, capable feed serves, v1 write refused).
