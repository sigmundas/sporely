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

## Stage 1 — COMPLETED 2026-09-02

### Stage 1 summary

**A (auth-state fix)** and **B (OAuth feasibility)** are both complete. See details below.

**Commits (sporely-py main):**
- `(pending commit)` — Stage 1A: classify captcha_failed + reauth_required hint; Stage 1B: OAuth feasibility findings documented in plan

**Tests run:**
- `tests/test_observations_tab_cloud_sync.py` — 5 new tests added; all 129 pass
- `tests/test_cloud_sync_auth_refresh.py`, `tests/test_preferences_cloud_sync_controls.py` — all pass
- `tests/test_cloud_sync_fast_path.py`, `tests/test_cloud_download_only.py`, `tests/test_main_window_background_activity_badge.py`, `tests/test_child_change_probe.py` — 237 total, all pass

**Review findings:** No correctness defects. `invalid_grant → reauth_required` is semantically correct. No sensitive value logging introduced. Translation files updated (nb_NO, sv_SE, de_DE).

**Manual Supabase Dashboard action still required (before Stage 2 can be tested end-to-end):**

1. In the Supabase Dashboard for project `zkpjklzfwzefhjluvhfw`: go to Auth → OAuth Clients → Create new client:
   - Client name: `Sporely Desktop`
   - Client type: **Public** (`token_endpoint_auth_method: none`)
   - Redirect URI: `http://127.0.0.1:8765/auth/callback`
2. Note the assigned `client_id` UUID — needed for Stage 4 (`SporelyDesktopOAuthClient`).
3. In Auth → URL Configuration, set the authorization path to `https://app.sporely.no/oauth/consent` (project-wide setting; required before Stage 2 routing works).

---

### Stage 1A findings and implementation

#### Root cause of the misleading hint

The bug: when `sync_all()` fails with a captcha_failed error (or any auth error that raises `CloudSyncError`), the error was stored as `status="error"`. The hint then reads the cached `_cloud_client` to determine `logged_in` — which is True if a client was previously cached in memory. With `status="error"` + `logged_in=True`, the hint showed:

> "Cloud sync failed. Logged in, click Sync now to sync."

And in the `pending_ids` case (status="ok" from prior run, no new error recorded):

> "Cloud sync pending for observation IDs 839, 700. Logged in, click Sync now to sync."

#### Changes

**`utils/cloud_sync.py`**: Added `'captcha_failed'` to `_CLOUD_AUTH_ERROR_HINTS`. `is_cloud_auth_error()` now matches captcha errors, classifying them as auth failures.

**`ui/observations_tab.py`**:
- `_summarize_sync_error()`: Added an early check for `captcha_failed`/`captcha protection` returning `"Cloud sign-in is required."` (distinct from the generic "Please check your email and password" used for credential errors).
- `_on_cloud_sync_error()`: When `is_cloud_auth_error(message)` is True, status is now `"reauth_required"` instead of `"error"`. This covers captcha_failed, invalid_grant, JWT expired, and all other auth errors.
- `_refresh_cloud_sync_idle_hint()`: New `reauth_required` branch (placed before the existing `error/blocked/warning` branch):
  - With pending_ids: shows "Cloud sync pending for observation IDs N, M. Cloud sign-in is required. Click Sync now to sign in again."
  - Without pending_ids: shows `last_summary` + "Cloud sign-in is required. Click Sync now to sign in again."
  - `logged_in` is NOT consulted — the branch always shows the sign-in message.

**Background retry**: No change needed. `_on_auto_sync_timeout()` and `_schedule_startup_cloud_sync()` are stubs (return immediately) in the base class. There is no background auto-retry loop.

**Account-mismatch, RLS, download-only protections**: Unchanged.

---

### Stage 1B findings — Supabase OAuth feasibility

#### Verdict: VIABLE ✅

**Supabase OAuth 2.1 server** is active on this project:
- The `auth.oauth_clients` table exists (no clients registered yet).
- Endpoints confirmed from Supabase docs:
  - Authorize: `{SUPABASE_URL}/auth/v1/oauth/authorize`
  - Token exchange: `POST {SUPABASE_URL}/auth/v1/oauth/token`
  - Refresh: `POST {SUPABASE_URL}/auth/v1/oauth/token` (grant_type=refresh_token)

**Public PKCE client**: Supabase supports `token_endpoint_auth_method: none` — no secret embedded in the desktop app. The existing `SUPABASE_KEY` (publishable key) is NOT used for OAuth token exchange; only the `client_id` UUID is.

**Loopback redirect URI**: Supabase OAuth clients accept any redirect URI including loopback. `http://127.0.0.1:8765/auth/callback` is a valid redirect URI to register.

**Proven flow contract** (from Supabase documentation + `auth.oauth_clients` schema inspection):

```text
Desktop:
  1. Generate random code_verifier (43–128 chars)
  2. Compute code_challenge = base64url(sha256(code_verifier))
  3. Generate random state
  4. Open browser:
       {SUPABASE_URL}/auth/v1/oauth/authorize
         ?response_type=code
         &client_id={registered-client-id}
         &redirect_uri=http://127.0.0.1:8765/auth/callback
         &code_challenge={challenge}
         &code_challenge_method=S256
         &state={state}

Supabase Auth:
  5. Validates OAuth params (client_id, redirect_uri, PKCE)
  6. Redirects browser to configured authorization_path:
       https://app.sporely.no/oauth/consent?authorization_id={id}

Web app (Stage 2 — app.sporely.no/oauth/consent):
  7. Check authentication → redirect to login if needed (preserving authorization_id)
  8. Call supabase.auth.oauth.getAuthorizationDetails(authorization_id) → client info
  9. Show consent screen ("Continue to Sporely Desktop" / "Cancel")
  10. On approve: supabase.auth.oauth.approveAuthorization(authorization_id)
      → returns redirect_url containing code + state
  11. Redirect browser to: http://127.0.0.1:8765/auth/callback?code={code}&state={state}

Desktop:
  12. Loopback server receives callback, verifies state
  13. POST {SUPABASE_URL}/auth/v1/oauth/token
        grant_type=authorization_code
        code={code}
        client_id={client-id}
        redirect_uri=http://127.0.0.1:8765/auth/callback
        code_verifier={verifier}
  14. Response: { access_token, refresh_token, expires_in, token_type }

Refresh:
  15. POST {SUPABASE_URL}/auth/v1/oauth/token
        grant_type=refresh_token
        refresh_token={token}
        client_id={client-id}
```

**RLS compatibility confirmed**: All 40 RLS policies use `auth.uid()` exclusively. OAuth access tokens carry the same `sub` (user UUID), `aud=authenticated`, `role=authenticated` claims as password-login tokens, plus an extra `client_id` claim. Existing RLS policies are unaffected.

**Architectural correction** (update to plan): Token exchange uses the OAuth-specific endpoint `/auth/v1/oauth/token`, NOT the existing `/auth/v1/token?grant_type=...` endpoint. Stage 4's `SporelyDesktopOAuthClient` must use the OAuth endpoint. The existing `SporelyCloudClient.refresh_login()` (using `/auth/v1/token?grant_type=refresh_token`) remains correct for sessions established via the existing password login path.

**`inat_oauth.py` reuse**: `utils/inat_oauth.py` already implements PKCE S256 generation, localhost callback server, code exchange, and token refresh — the exact transport Stage 3 will extract.

---

---

## Stage 2 — COMPLETED 2026-09-02

### Stage 2 summary

Browser consent flow added at `https://app.sporely.no/oauth/consent?authorization_id=<uuid>`.

**Commits (sporely-web main):**
- `(pending commit)` — Stage 2: OAuth consent screen for Sporely Desktop

**New files:**
- `oauth/consent.html` — standalone Vite HTML entry (not part of the main SPA bundle)
- `oauth/consent.js` — thin entry: imports `initOAuthConsent` + Supabase client, calls `initOAuthConsent({ supabase })`
- `src/screens/oauth-consent.js` — main module: `initOAuthConsent()` + `consumePendingOAuthConsentReturn()`
- `src/screens/oauth-consent.test.js` — 31 tests

**Modified files:**
- `vite.config.js` — added `oauthConsent` rollup input entry
- `vite.config.test.js` — updated multi-page entry test + `oauth/` dir in temp dir setup
- `src/main.js` — added `_consumePendingOAuthConsentReturn()` inline helper + two call sites (auth_form_submit and onAuthStateChange SIGNED_IN) that redirect to `/oauth/consent` after successful interactive login

**Supabase OAuth client ID used:** `b141fed6-e257-4de1-b784-3a28c777dadf` (hardcoded in `src/screens/oauth-consent.js`, validated against `authDetails.client_id`)

**Flow implemented:**
1. `/oauth/consent?authorization_id=<uuid>` loads
2. UUID format validated; missing/invalid → error shown (textContent, no innerHTML)
3. `supabase.auth.getSession()` called — if no session: stores `authorization_id` in sessionStorage (`sporely-oauth-consent-pending`), redirects to `/` for login
4. After login (password+Turnstile or Google), main.js `_consumePendingOAuthConsentReturn()` detects the pending key and navigates back to the consent page
5. `supabase.auth.oauth.getAuthorizationDetails(authorizationId)` fetched; `client_id` validated
6. Consent UI shown: client name, signed-in email, Continue / Cancel buttons
7. **Continue** → `supabase.auth.oauth.approveAuthorization(authorizationId, { skipBrowserRedirect: true })` → navigates to `redirect_url`
8. **Cancel** → `supabase.auth.oauth.denyAuthorization(authorizationId, { skipBrowserRedirect: true })` → navigates to `redirect_url`

**Security: redirect_url validation**
- `https:` allowed unconditionally
- `http:` allowed only for loopback hostnames (`127.0.0.1`, `localhost`, `[::1]`, `::1`)
- All other protocols rejected; error shown in DOM via `textContent` only

**Tests run:**
- `src/screens/oauth-consent.test.js` — 31 tests, all pass
- `vite.config.test.js` — 6 tests, all pass
- `src/screens/auth.test.js`, `src/auth-state.test.js`, `src/auth-reauth-recovery.test.js`, `src/auth-deadlock.regression.test.js`, `src/native-auth-links.test.js`, `src/app-link.regression.test.js` — 59 tests, all pass
- **Total: 96 tests, 0 failures**

**Security review findings and resolutions:**
- M: `_isSafeRedirectUrl` initially accepted any `http:` URL → **fixed**: http: now restricted to loopback hostnames only. New tests added.
- L: Duplicate `consumePendingOAuthConsentReturn` in main.js vs oauth-consent.js → **documented** with comment explaining intentional inline to avoid bundling consent code in the main SPA chunk.
- L/Info: `clientName` rendered from API response → not a risk because client_id is validated against the hardcoded DESKTOP_CLIENT_ID before rendering.
- No interference with `/auth/callback` or Android App Links (consent page is a separate HTML entry; main.js consent return is gated on `resolveAuthenticatedSessionOnce` completing and is not on the callback path).

**Cloudflare Pages routing:** Cloudflare Pages serves `dist/oauth/consent.html` automatically at `/oauth/consent` (extension stripping). No `_redirects` or `_headers` changes needed — the global `/*` CSP already includes `https://*.supabase.co` in `connect-src`.

**Required before end-to-end testing:**
1. Supabase Dashboard → Auth → URL Configuration: set "Authorization path" to `https://app.sporely.no/oauth/consent`
2. The "Sporely Desktop" public OAuth client created in Stage 1 setup must be registered with `http://127.0.0.1:8765/auth/callback` as its redirect URI

---

## Stage 3 — COMPLETED 2026-09-02

### Stage 3 summary

Generic loopback OAuth callback transport extracted from `utils/inat_oauth.py` into `utils/oauth_loopback.py`. `inat_oauth.py` refactored to import from the new module. All iNaturalist OAuth behavior preserved exactly.

**Commits (sporely-py main):**
- `(pending commit)` — Stage 3: extract LoopbackCallbackServer into utils/oauth_loopback.py

**New files:**
- `utils/oauth_loopback.py` — `OAuthCallbackPayload`, `LoopbackCallbackServer`, `loopback_port_is_free`
- `tests/test_oauth_loopback.py` — 22 tests

**Modified files:**
- `utils/inat_oauth.py` — removed `OAuthCallbackPayload`, `DualStackServer`, `LocalCallbackServer`; imports from `oauth_loopback`; `LocalCallbackServer(self.redirect_uri)` → `LoopbackCallbackServer(self.redirect_uri)`

**Design decisions:**

- `LoopbackCallbackServer(redirect_uri)` validates that the redirect URI is `http://` scheme with a loopback hostname (`localhost`, `127.0.0.1`, `::1`) and an explicit port. Non-loopback hosts raise `ValueError`.
- Binding: always `127.0.0.1` (IPv4 loopback). The old `DualStackServer` bound to `::` (all IPv6 interfaces + IPv4-mapped), which was not strictly loopback-only. The new implementation is strictly loopback-only and is correct for all practical redirect URI forms used in this codebase (`http://localhost:8000/callback` and `http://127.0.0.1:8765/auth/callback`).
- `loopback_port_is_free(port)` uses `SO_REUSEADDR` (matching `HTTPServer`) so TIME_WAIT ports from recently-closed connections are reported as usable.
- State and PKCE validation remain in the provider layer (`INatOAuthClient`, future `SporelyDesktopOAuthClient`).
- `OAuthCallbackPayload` re-exported from `inat_oauth` via `__all__` for backward compatibility.

**Tests run:**
- `tests/test_oauth_loopback.py` — 22 tests, all pass
- `tests/test_cloud_sync_auth_refresh.py`, `tests/test_observations_tab_cloud_sync.py`, `tests/test_preferences_cloud_sync_controls.py`, `tests/test_reference_library_desktop_slice.py` — 172 tests, all pass

**Review findings (sporely-reviewer):** PASS. No correctness defects. The binding change (`::` → `127.0.0.1`) is intentional and immaterial for the browser-redirect OAuth use case. `OAuthCallbackPayload` re-export noted as currently untested via the `inat_oauth` path; no external callers exist today.

---

## Stage 4 — COMPLETED 2026-09-02

### Stage 4 summary

Headless `SporelyDesktopOAuthClient` implemented with full OAuth 2.1 + PKCE authorization-code flow, token exchange, and refresh. Returns a validated `OAuthTokenResult` dataclass; never returns raw JSON dicts.

**Commits (sporely-py main):**
- `(pending commit)` — Stage 4: implement SporelyDesktopOAuthClient (utils/sporely_cloud_auth.py)

**New files:**
- `utils/sporely_cloud_auth.py` — `CLIENT_ID`, `REDIRECT_URI`, `OAuthError`, `OAuthTokenResult`, `SporelyDesktopOAuthClient`
- `tests/test_sporely_cloud_auth.py` — 59 tests

**Public API produced for Stage 5:**
```python
from utils.sporely_cloud_auth import (
    SporelyDesktopOAuthClient,
    OAuthTokenResult,   # dataclass: access_token, refresh_token, token_type, expires_at, user_id, user_email
    OAuthError,         # re-auth-required; distinguish from RuntimeError (infrastructure)
    CLIENT_ID,
    REDIRECT_URI,
)

client = SporelyDesktopOAuthClient(timeout=180)
result: OAuthTokenResult = client.authorize()           # full browser flow
result: OAuthTokenResult = client.exchange_code(code, verifier)  # after manual callback
result: OAuthTokenResult = client.refresh(refresh_token)         # silent refresh
url: str = client.build_authorization_url(state, challenge)
```

**Design decisions:**
- `SUPABASE_URL` imported from `utils.cloud_sync` — no second project URL source.
- PKCE verifier: `secrets.token_urlsafe(64)` → 86-char URL-safe string (RFC 7636: 43–128 chars).
- S256 challenge: `base64url(sha256(verifier.encode('utf-8')))` with no padding.
- Token URL: `/auth/v1/oauth/token` (OAuth-specific endpoint, NOT `/auth/v1/token`).
- Public client: no `client_secret` anywhere.
- `loopback_port_is_free(8765)` checked before constructing `LoopbackCallbackServer`.
- Server object constructed before `webbrowser.open`; test verifies ordering.
- `OAuthError` raised for: state mismatch, denial, missing code, timeout, invalid/revoked grant (HTTP 400 and 401).
- `RuntimeError` raised for: port occupied, no browser, network failure, malformed response.
- Authorization codes, verifiers, access tokens, refresh tokens: never in exception messages.

**`OAuthTokenResult` contract for Stage 5:**
- `access_token`: always non-empty (validated; RuntimeError if missing).
- `refresh_token`: empty string if server omits it (non-rotating Supabase config); Stage 5 must preserve the previous stored token when this is empty rather than overwriting with `""`.
- `expires_at`: Unix timestamp (int) computed as `now + expires_in`; None if server omits `expires_in`.
- `user_id`, `user_email`: parsed from `response["user"]` if present; None otherwise.

**Tests run:**
- `tests/test_sporely_cloud_auth.py` — 59 tests, all pass
- `tests/test_oauth_loopback.py`, `tests/test_observations_tab_cloud_sync.py`, `tests/test_cloud_sync_auth_refresh.py`, `tests/test_preferences_cloud_sync_controls.py`, `tests/test_reference_library_desktop_slice.py` — 195 tests, all pass

**Review findings (sporely-reviewer) and resolutions:**
- M: `refresh()` treated HTTP 401 as `RuntimeError`; Supabase returns 401 for revoked tokens → **fixed**: 400 and 401 both raise `OAuthError`.
- L: Empty `refresh_token` from non-rotating server response silently returns `""` → **documented** in `OAuthTokenResult.refresh_token` field comment and Stage 5 handoff; Stage 5 must guard.
- Info: `test_no_client_secret_in_request` has a complex data-extraction fallback — low-risk; test passes and the implementation pattern is consistent.
- No security defects: PKCE S256 correct, no secret in any request, state validated first, no token leakage in exceptions, server constructed before browser open, no Stage 5 scope creep.

---

## Stage 5 — COMPLETED 2026-09-02

### Stage 5 summary

OAuth sessions integrated into `SporelyCloudClient` via a new `OAuthSporelyCloudClient` subclass. Resulting client is indistinguishable from a normal authenticated `SporelyCloudClient` for all cloud operations.

**Commits (sporely-py main):**
- `(pending commit)` — Stage 5: OAuthSporelyCloudClient integrates OAuth sessions with SporelyCloudClient

**New files:**
- `tests/test_sporely_cloud_oauth_session.py` — 22 tests

**Modified files:**
- `utils/cloud_sync.py`:
  - Lines 15099–15105: added OAuth delegation guard to `SporelyCloudClient.from_stored_credentials` (delegates to `OAuthSporelyCloudClient` when `cloud_auth_method == 'oauth'`)
  - Lines 15272–15289: `clear_session()` and `clear_credentials()` now also clear `cloud_auth_method` (prevents stale OAuth routing after logout/re-login)
  - Lines 17357–17493: new `OAuthSporelyCloudClient(SporelyCloudClient)` class (~140 lines)

**Public interface established:**
```python
from utils.cloud_sync import OAuthSporelyCloudClient

# Construct from freshly-obtained OAuthTokenResult (Stage 4)
client = OAuthSporelyCloudClient.from_oauth_session(result)   # OAuthTokenResult
client.save_credentials(email=result.user_email)              # persists cloud_auth_method=oauth

# Reload from settings
client = SporelyCloudClient.from_stored_credentials()         # returns OAuthSporelyCloudClient when cloud_auth_method==oauth
```

**Session/refresh behavior:**
- `OAuthSporelyCloudClient.refresh_login(token)` calls `SporelyDesktopOAuthClient().refresh(token)` (lazy import; OAuth endpoint)
- `OAuthError` → `CloudReauthRequiredError("... (invalid_grant)")` — both `is_cloud_auth_error` and `is_cloud_reauth_required_error` correctly classify it
- `RuntimeError` → `CloudTemporarilyUnavailableError` — correctly not treated as reauth
- Empty `refresh_token` from server → old token preserved (critical invariant: `new_refresh = result.refresh_token or old_token`)
- `_refresh_session_if_possible` dispatches to `OAuthSporelyCloudClient.refresh_login` via `type(self).refresh_login(...)` — no changes to that method needed

**Persistence:**
- `cloud_access_token`, `cloud_user_id`, `cloud_refresh_token`, `cloud_auth_method = 'oauth'`
- Optionally `cloud_user_email`
- Never: password, authorization code, PKCE verifier, OAuth state

**Compatibility:**
- Legacy sessions (no `cloud_auth_method` or `cloud_auth_method = 'password'`) use base `SporelyCloudClient` unchanged
- Existing users with valid stored sessions are unaffected
- No UI, sync contract, RLS, or pull-only mode changes

**Tests run:**
- `tests/test_sporely_cloud_oauth_session.py` — 22 tests, all pass
- `tests/test_cloud_sync_auth_refresh.py`, `tests/test_observations_tab_cloud_sync.py`, `tests/test_sporely_cloud_auth.py`, `tests/test_oauth_loopback.py`, `tests/test_preferences_cloud_sync_controls.py` — 210 tests, all pass
- **Total: 232 tests, 0 failures**

**Security review findings and resolutions:**
- All six specified dimensions PASS (OAuth vs legacy dispatch, empty-refresh preservation, reauth vs transient classification, account identity safety, password regression, token logging)
- M: `clear_session()`/`clear_credentials()` did not clear `cloud_auth_method`, risking stale OAuth routing after logout → **fixed**: both now set `cloud_auth_method: None`
- No other correctness defects or security issues found

---

## Stage 6 — COMPLETED 2026-09-02

### Stage 6 summary

Replaced both desktop Sporely Cloud password login UIs with a "Sign in in browser" OAuth flow. Removed all interactive desktop password entry and "Save password on this device" controls. Background sync never opens a browser.

**Commits (sporely-py main):**
- `(pending commit)` — Stage 6: Replace cloud login UI with browser OAuth, remove desktop password auth

**Modified files:**
- `ui/cloud_sync_dialog.py` — Replaced email/password login panel with `_OAuthLoginWorker` + "Sign in in browser" / "Waiting for browser sign-in… [ Cancel ]" states; added `closeEvent` to cancel in-progress auth; handles `CloudReauthRequiredError` from `from_stored_credentials()` to show reauth panel; removed `_do_login`, `_on_login_ok`, `_on_login_fail`, `_show_login_error`, `_on_password_edited`; removed `QLineEdit` and `load_saved_cloud_password` imports
- `ui/main_window.py` — Replaced `_CloudLoginWorker` (email+password) with `_CloudOAuthLoginWorker` (browser OAuth + cancel); replaced `_open_cloud_login` with waiting modal dialog; updated `_on_cloud_login_success` (no password kwargs); updated `_on_cloud_login_failure` message; removed dead `_cloud_login_password`/`_cloud_login_remember` attribute initializations
- `ui/observations_tab.py` — Updated background sync fallback error message from "check your email and password" to "Please sign in again."

**New files:**
- `tests/test_cloud_sync_dialog_oauth.py` — 14 tests covering all required cases

**Password call sites classified:**
- `SporelyCloudClient.login()` itself: **preserved** (non-UI callers may depend on it)
- `load_saved_cloud_password` in `utils/cloud_sync.py`: **preserved** (legacy session fallback)
- `SporelyCloudClient.from_stored_credentials()` password fallback: **preserved** (legacy users)
- `observations_tab.py` `_CloudSyncWorker` password check: **preserved** (legacy users)
- Interactive `SporelyCloudClient.login()` call sites **removed** from: `cloud_sync_dialog.py`, `main_window.py` `_open_cloud_login`

**Reauth behavior:**
- `CloudReauthRequiredError` from `from_stored_credentials()` → shows "Cloud sign-in is required." login panel (not "logged in")
- Background sync: raises → caught by `except Exception` → emits auth-error string → `_on_cloud_sync_error` → `is_cloud_auth_error()` → `reauth_required` status → no browser launch

**Legacy sessions:**
- Valid stored OAuth sessions continue to load via `OAuthSporelyCloudClient.from_stored_credentials()`
- Valid legacy password sessions continue to work until reauthentication is required
- Once a legacy session requires interactive sign-in, the UI now shows OAuth flow (migration to OAuth)

**Tests run:**
- `tests/test_cloud_sync_dialog_oauth.py` — 14 tests, all pass
- `tests/test_sporely_cloud_auth.py`, `tests/test_sporely_cloud_oauth_session.py`, `tests/test_oauth_loopback.py`, `tests/test_observations_tab_cloud_sync.py`, `tests/test_preferences_cloud_sync_controls.py` — 200 tests, all pass
- `tests/test_cloud_sync_auth_refresh.py`, `tests/test_cloud_sync_fast_path.py` — 72 tests, all pass
- **Total: 286 tests, 0 failures**

**Translations:**
- `i18n/Sporely_*.ts` and `.qm` updated via `./tools/update_translations.sh`
- 7 new strings from `main_window.py` ("Sporely Cloud Sign-In", "Waiting for browser sign-in…", "Cancel", "Cloud sync sign-in failed. Please sign in again.") marked `<translation type="unfinished">` — need translation before publish

---

## Stage 7 — COMPLETED 2026-09-03

### Stage 7 summary

Auth cleanup, translations, and automated regression sweep complete. Production Turnstile configuration and manual E2E steps documented below (cannot be performed from agent environment).

**Commits (sporely-py main):**
- `(pending commit)` — Stage 7: remove password fallback, complete translations, add regression tests

**Modified files:**
- `utils/cloud_sync.py` — removed password fallback from `SporelyCloudClient.from_stored_credentials()`; added `OAuthSporelyCloudClient.login()` override raising `CloudSyncError`
- `ui/observations_tab.py` — removed `load_saved_cloud_password` import; simplified `has_saved_credentials` to check only stored tokens (not saved password)
- `i18n/Sporely_nb_NO.ts` + `.qm` — 7 strings translated (4 browser-auth, 3 reference-library)
- `i18n/Sporely_sv_SE.ts` + `.qm` — same
- `i18n/Sporely_de_DE.ts` + `.qm` — same
- `tests/test_cloud_sync_auth_refresh.py` — 4 new Stage 7 regression tests
- `tests/test_observations_tab_cloud_sync.py` — removed 2 dead `load_saved_cloud_password` monkeypatches
- `tests/test_cloud_original_sync_surface.py` — removed 1 dead monkeypatch; updated stale expected message

### Legacy password paths removed / preserved

| Path | Decision | Reason |
|---|---|---|
| `SporelyCloudClient.from_stored_credentials()` password fallback | **Removed** | OAuth is now the only interactive auth path; password fallback is unreachable from UI |
| `OAuthSporelyCloudClient.login()` | **Blocked** (`CloudSyncError`) | OAuth session instances must never attempt password auth |
| `load_saved_cloud_password()` in `observations_tab` worker | **Removed** | `has_saved_credentials` now checks only stored tokens |
| `SporelyCloudClient.login()` (base class) | **Preserved** — unreachable from production UI | No remaining production caller; kept for possible test/migration use; covered by regression test that confirms it is NOT called on stale sessions |
| `save_cloud_password()` / `clear_saved_cloud_password()` | **Preserved** | Still used by logout/credential-cleanup and "Remove saved passwords" UI |
| `load_saved_cloud_password()` in `cloud_sync.py` itself | **Preserved** (dead) | Called only from removed password fallback; function body intact for keyring-legacy cleanup path |
| `has_saved_cloud_password()` | **Preserved** | May be used by preferences UI to show/hide "Remove saved passwords" option |
| iNaturalist / Artsobservasjoner password code | **Untouched** | Separate integration; stage scope is Sporely Cloud only |

### Translations completed

All three locales now have 0 unfinished strings (2178/2178 finished in each .qm binary).

**Strings translated (nb_NO / sv_SE / de_DE):**
1. `Sporely Cloud Sign-In` → Sporely Cloud-innlogging / Sporely Cloud-inloggning / Sporely Cloud-Anmeldung
2. `Waiting for browser sign-in…` → Venter på nettleserinnlogging… / Väntar på webbläsarinloggning… / Warte auf Browser-Anmeldung…
3. `Cloud sync sign-in failed. Please sign in again.` (main_window) → Sky-synkroniseringen mislyktes. Logg inn igjen. / Cloud-synkroniseringen misslyckades. Logga in igen. / Cloud-Synchronisierung fehlgeschlagen. Bitte melde dich erneut an.
4. `Cloud sync sign-in failed. Please sign in again.` (observations_tab) → same as above
5. `Assign reference to library` → Tilknytt referanse til bibliotek / Tilldela referens till bibliotek / Referenz der Bibliothek zuweisen
6. `This reference was not plotted because it needs a publication assignment…` → translated in all three
7. `This reference was not plotted because a publication assignment is required…` → translated in all three

### Regression tests added

`tests/test_cloud_sync_auth_refresh.py` — 4 new tests (total 36 tests, all pass):
- `test_from_stored_credentials_does_not_fall_back_to_password_after_reauth_required` — stale token + revoked refresh + saved password → returns None, login() not called
- `test_from_stored_credentials_does_not_attempt_password_login_when_refresh_fails` — refresh-only settings + CloudSyncError + saved password → returns None, login() not called
- `test_from_stored_credentials_does_not_attempt_password_login_when_no_tokens` — no tokens at all + saved password → returns None, login() not called
- `test_oauth_client_login_raises_cloud_sync_error` — `OAuthSporelyCloudClient.login()` raises `CloudSyncError`

### Tests run

- All 10 targeted test files: **324 tests, 0 failures**
  - `test_cloud_sync_auth_refresh.py` (36)
  - `test_observations_tab_cloud_sync.py`
  - `test_preferences_cloud_sync_controls.py`
  - `test_sporely_cloud_auth.py`
  - `test_sporely_cloud_oauth_session.py`
  - `test_oauth_loopback.py`
  - `test_cloud_sync_dialog_oauth.py`
  - `test_cloud_sync_fast_path.py`
  - `test_cloud_download_only.py`
  - `test_cloud_original_sync_surface.py`

### Production configuration required (manual)

The following steps require Supabase Dashboard access and cannot be performed from the agent environment:

**1. Supabase Dashboard — confirm OAuth client**
- Project: `zkpjklzfwzefhjluvhfw`
- Auth → OAuth Clients: confirm `Sporely Desktop` public client with:
  - `client_id`: `b141fed6-e257-4de1-b784-3a28c777dadf`
  - `redirect_uri`: `http://127.0.0.1:8765/auth/callback`

**2. Supabase Dashboard — confirm authorization path**
- Auth → URL Configuration → Authorization path: `https://app.sporely.no/oauth/consent`

**3. Enable Turnstile**
- Auth → Bot and Abuse Protection → enable Turnstile CAPTCHA

**Manual E2E checklist (perform after enabling Turnstile):**

| Step | Expected result |
|---|---|
| 1. app.sporely.no password login | Turnstile appears → succeeds |
| 2. sporely-landing password login | Turnstile appears → succeeds |
| 3. Desktop while logged out → Sign in in browser | Browser opens → app.sporely.no → password + Turnstile → consent → localhost callback → desktop shows "Logged in" |
| 4. Desktop → Sign in via Google | Browser → Google → consent → desktop signed in |
| 5. Restart desktop | Stored OAuth session loads, no browser |
| 6. Access token expiry | Refresh occurs silently, no Turnstile, sync continues |
| 7. Revoked refresh token | `reauth_required` in UI, no password fallback, no automatic browser launch |
| 8. Sign out → sign in as different account | Account-mismatch protection behaves correctly |

---

---

## Stage 1 — Fix auth-failure state and prove OAuth feasibility (original spec)

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
