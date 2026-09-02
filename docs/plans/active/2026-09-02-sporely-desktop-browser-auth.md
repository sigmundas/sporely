# Sporely Desktop Browser Authentication Plan

> **For agentic workers:** Implement one stage per agent run. Use focused tests first, then the relevant auth/cloud-sync suite. Update this canonical plan after every implementation pass with completed work, deviations, tests, commits, review findings, and the exact next stage.

## Goal

Replace `sporely-py` password login with browser-based Supabase OAuth/PKCE, so Turnstile remains a browser concern. At the same time, fix the current auth-state bug where a rejected cloud login can still leave the UI saying **“Logged in, click Sync now to sync.”**

## Architecture

```text
Sporely Desktop
  -> opens browser
  -> app.sporely.no / Supabase OAuth authorization
  -> normal Sporely login (password + Turnstile or Google)
  -> localhost callback with authorization code
  -> desktop exchanges code + PKCE verifier
  -> access + refresh tokens
  -> existing SporelyCloudClient / cloud sync
```

Use a **public OAuth client** with PKCE. Do not embed a Supabase secret, service-role key, Turnstile secret, or OAuth client secret in the desktop app.

Preserve existing cloud-sync contracts, RLS behavior, account-mismatch protection, and download-only zero-write guarantees.

---

## Stage 1 — Fix auth-failure state and prove OAuth feasibility

### A. Fix the current misleading status

Reproduce the Supabase Turnstile failure:

```text
400 captcha_failed
captcha protection: request disallowed (no captcha_token found)
```

Ensure it is classified as an auth / reauthentication failure.

Required behavior:

```text
Cloud sync pending for observation IDs 839, 700.
Cloud sign-in is required. Click Sync now to sign in again.
```

The UI must never say **“Logged in”** after a failed login or unusable session.

Update the existing cloud status fields consistently:

- `cloud_last_sync_status`
- `cloud_last_sync_summary`
- `cloud_last_sync_errors_json`

Add regression tests for:

- `captcha_failed`
- invalid/expired stored session
- pending observation IDs remaining visible
- background auto-sync not repeatedly retrying known-bad interactive auth

### B. Prove Supabase OAuth for desktop

Configure a Supabase **public OAuth client** with an exact loopback redirect, for example:

```text
http://127.0.0.1:8765/auth/callback
```

Manually prove:

```text
authorize
-> browser login
-> consent
-> localhost callback with code + state
-> token exchange with PKCE verifier
-> access + refresh tokens
-> existing owner-scoped Supabase read succeeds
```

**Stop here if the loopback redirect or token/RLS behavior does not work as expected.**

---

## Stage 2 — Add the browser consent flow in `sporely-web`

**Repo:** `sporely-web`

Add a dedicated route such as:

```text
/oauth/consent?authorization_id=...
```

Behavior:

1. Read `authorization_id`.
2. Load authorization details from Supabase.
3. If not signed in, use the existing Sporely auth flow and return to the consent route.
4. If signed in, show a simple first-party approval screen:
   - **Continue to Sporely Desktop**
   - **Cancel**
5. Approve or deny through Supabase OAuth APIs.
6. Navigate only to the redirect URL returned by Supabase.

Reuse the existing browser auth stack, including Turnstile and Google login. Do not manually construct callback URLs or expose browser session tokens.

Tests:

- missing/expired authorization
- logged-out -> login -> return
- approve
- deny
- Supabase error
- no sensitive values in logs/DOM

---

## Stage 3 — Extract reusable localhost OAuth transport in `sporely`

**Repo:** `sporely`

`utils/inat_oauth.py` already contains a localhost callback server and PKCE flow. Extract the generic transport instead of copying it.

Suggested:

```text
utils/oauth_loopback.py
tests/test_oauth_loopback.py
```

Requirements:

- loopback-only binding
- fixed callback path
- bounded timeout
- exact state handling by caller
- no auth-code logging
- close server after completion
- preserve existing iNaturalist behavior

This stage should have **no Sporely Cloud behavior change**.

---

## Stage 4 — Implement `SporelyDesktopOAuthClient`

**Repo:** `sporely`

Suggested:

```text
utils/sporely_cloud_auth.py
tests/test_sporely_cloud_auth.py
```

Implement:

```python
build_authorization_url(...)
authorize(...)
exchange_code(...)
refresh(...)
```

Requirements:

- random `state`
- PKCE S256 verifier/challenge
- start callback listener before browser launch
- exact redirect URI
- verify returned `state`
- exchange code directly with Supabase
- support refresh-token rotation

Test:

- success
- user denial
- state mismatch
- callback timeout
- occupied callback port
- browser launch failure
- malformed token response
- token refresh failure

No passwords, auth codes, PKCE verifiers, access tokens, or refresh tokens in logs.

---

## Stage 5 — Integrate OAuth sessions with `SporelyCloudClient`

**Repo:** `sporely`

Add a clean session-construction seam, for example:

```python
SporelyCloudClient.from_oauth_session(...)
```

Persist only the normal session state needed by the existing cloud client:

```text
cloud_access_token
cloud_refresh_token
cloud_user_id
cloud_user_email
cloud_auth_method = oauth
```

Do not persist:

```text
password
authorization code
PKCE verifier
state
```

Verify OAuth-issued access tokens behave correctly for:

- profile/user lookup
- observation read/write
- image/media operations
- normal sync
- download-only sync
- account ownership / RLS

Existing valid desktop sessions should continue working until reauthentication is actually required.

---

## Stage 6 — Replace password UI and remove desktop password auth

**Repo:** `sporely`

Replace the current email/password cloud login panel with:

```text
Sign in to your Sporely account to enable cloud sync.

[ Sign in in browser ]
```

While waiting:

```text
Waiting for browser sign-in…
[ Cancel ]
```

Handle:

- success
- cancel
- denial
- timeout
- callback-port error
- browser-open error
- network/OAuth failure

Then remove production dependence on:

- desktop Sporely password entry
- `Save password on this device`
- saved Sporely Cloud passwords
- password fallback in background sync
- `SporelyCloudClient.login(email, password)` for interactive desktop auth

New session rule:

```text
valid access token
  -> use it

expired access token + valid refresh token
  -> refresh silently

refresh revoked/invalid
  -> mark reauth_required
  -> do not auto-open browser
  -> next interactive UI asks user to sign in again
```

Background sync must never launch a browser unexpectedly.

---

## Stage 7 — Re-enable Turnstile and run end-to-end verification

Re-enable Supabase CAPTCHA only after the desktop browser-auth path is complete.

Verify:

```text
app.sporely.no password login
  -> Turnstile -> success

sporely-landing password login
  -> Turnstile -> success

Sporely Desktop
  -> browser
  -> password + Turnstile OR Google
  -> consent
  -> localhost callback
  -> desktop authenticated

existing desktop OAuth session
  -> silent refresh
  -> no Turnstile
```

Also verify:

- restart with persisted OAuth session
- logout/login as another account
- account-mismatch protection
- revoked refresh token -> `reauth_required`
- pending sync hint never says “Logged in” when auth is unusable
- no password remnants in desktop settings/keyring
- no sensitive auth material in logs

---

## Stage summary

| Stage | Scope |
|---|---|
| 1 | Fix misleading auth status + prove Supabase OAuth |
| 2 | Browser consent flow |
| 3 | Reusable localhost OAuth transport |
| 4 | Desktop OAuth client |
| 5 | `SporelyCloudClient` session integration |
| 6 | UI cutover + remove desktop passwords |
| 7 | Re-enable Turnstile + end-to-end verification |
